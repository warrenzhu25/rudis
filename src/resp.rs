use bytes::{Buf, Bytes, BytesMut};
use smallvec::{SmallVec, smallvec};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SetSlotSubcommand {
    Migrating(String),
    Importing(String),
    Stable,
    Node(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ClusterSubcommand {
    KeySlot(Bytes),
    CountKeysInSlot(u16),
    GetKeysInSlot(u16, usize),
    SetSlot(u16, SetSlotSubcommand),
    Slots,
    Shards,
    Links,
    AddSlots(Vec<u16>),
    DelSlots(Vec<u16>),
    AddSlotsRange(Vec<(u16, u16)>),
    DelSlotsRange(Vec<(u16, u16)>),
    Nodes,
    Info,
    Meet {
        ip: String,
        port: u16,
    },
    MyId,
    MigrateSlot {
        slot: u16,
        host: String,
        port: u16,
    },
    Rebalance {
        host: Option<String>,
        port: Option<u16>,
        slots: Option<usize>,
        weights: Vec<(String, f64)>,
        simulate: bool,
        threshold: f64,
        pipeline: usize,
    },
    Check,
    Reshard {
        target_node_id: String,
        source_node_id: String,
        slots: usize,
    },
    Failover {
        force: bool,
    },
    Reset {
        hard: bool,
    },
    Forget(String),
    Replicate(String),
    SaveConfig,
    BumpEpoch,
    SetConfigEpoch(u64),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DflyClusterSubcommand {
    MyId,
    Config(String),
    GetSlotInfo(Vec<u16>),
    FlushSlots(Vec<(u16, u16)>),
    SlotMigrationStatus,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DflyMigrateSubcommand {
    Init {
        source_id: String,
        num_shards: usize,
        slots: Vec<(u16, u16)>,
    },
    Flow {
        source_id: String,
        flow_id: u64,
    },
    Ack {
        flow_id: u64,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SetCondition {
    None,
    Nx,
    Xx,
    Ifeq(Bytes),
    Ifne(Bytes),
    Ifdeq(Bytes),
    Ifdne(Bytes),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MsetexCondition {
    None,
    Nx,
    Xx,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MsetexExpiry {
    None,
    KeepTtl,
    ExpireIn(Duration),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum IncrexIncrement {
    Int(i64),
    Float(f64),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum IncrexBound {
    Int(i64),
    Float(f64),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum IncrexExpire {
    Ex(u64),
    Px(u64),
    Exat(u64),
    Pxat(u64),
    Persist,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MemorySubcommand {
    Usage { key: Bytes },
    Stats,
    Purge,
    Doctor,
    Defrag,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TierSubcommand {
    Spill(Bytes),
    Load(Bytes),
    Info,
    SpillAll,
    Cool(Bytes),
    Decommit(Option<Bytes>),
    Gc,
    Snapshot(std::path::PathBuf),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ClientSubcommand {
    List(Vec<u64>),
    Info,
    SetName(String),
    GetName,
    Id,
    Kill(Vec<Bytes>),
    Tracking {
        enabled: bool,
        redirect: Option<i64>,
        bcast: bool,
        prefixes: Vec<Bytes>,
        optin: bool,
        optout: bool,
        noloop: bool,
    },
    Caching(Option<bool>),
    GetRedir,
    TrackingInfo,
    Unblock {
        client_id: u64,
        unblock_type: crate::block::ClientUnblockType,
    },
    Pause(u64, bool),
    Unpause,
    NoTouch(bool),
    NoEvict(bool),
    SetInfo {
        attr: String,
        val: String,
    },
    Reply(ClientReplyMode),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ClientReplyMode {
    On,
    Off,
    Skip,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum LatencySubcommand {
    Latest,
    History(String),
    Doctor,
    Reset(Vec<String>),
    Graph(String),
    Histogram(Vec<String>),
    Help,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AclSubcommand {
    List,
    Users,
    GetUser(String),
    SetUser {
        username: String,
        rules: Vec<String>,
    },
    DelUser(Vec<String>),
    WhoAmI,
    Cat,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ObjectSubcommand {
    Encoding(Bytes),
    Freq(Bytes),
    Idletime(Bytes),
    Refcount(Bytes),
    Help,
}

#[derive(Debug, PartialEq, Clone)]
pub enum XinfoSubcommand {
    Stream(Bytes),
    StreamFull { key: Bytes, count: Option<usize> },
    Groups(Bytes),
    Consumers { key: Bytes, group: Bytes },
    Help,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum HexpireCondition {
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum HFieldExpireOpt {
    None,
    Persist,
    KeepTtl,
    ExMs(i64),
    ExAtMs(i64),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct ExpireOptions {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
}

pub fn parse_expire_options(args: &[Bytes]) -> Result<ExpireOptions, String> {
    let mut opts = ExpireOptions::default();
    for arg in args {
        let s = std::str::from_utf8(arg).map_err(|_| "ERR syntax error".to_string())?;
        if s.eq_ignore_ascii_case("NX") {
            opts.nx = true;
        } else if s.eq_ignore_ascii_case("XX") {
            opts.xx = true;
        } else if s.eq_ignore_ascii_case("GT") {
            opts.gt = true;
        } else if s.eq_ignore_ascii_case("LT") {
            opts.lt = true;
        } else {
            return Err(format!("ERR Unsupported option {}", s));
        }
    }
    if (opts.nx && opts.xx) || (opts.nx && opts.gt) || (opts.nx && opts.lt) {
        return Err(
            "ERR NX and XX, GT or LT options at the same time are not compatible".to_string(),
        );
    }
    if opts.gt && opts.lt {
        return Err("ERR GT and LT options at the same time are not compatible".to_string());
    }
    Ok(opts)
}

#[inline]
fn parse_integer(b: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(b)
        .map_err(|_| ())?
        .parse::<i64>()
        .map_err(|_| ())
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum HsetexCondition {
    None,
    Fnx,
    Fxx,
}

#[derive(Debug, PartialEq, Clone)]
pub enum VsimTarget {
    Element(Bytes),
    Vector(Vec<f32>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitfieldOpType {
    Get,
    Set(i64),
    Incrby(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitfieldOverflow {
    Wrap,
    Sat,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitfieldSubOp {
    pub op_type: BitfieldOpType,
    pub sign: bool,
    pub bits: usize,
    pub offset: u64,
    pub overflow: BitfieldOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LmovemMode {
    Count,
    Exactly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LmovemOrdering {
    Obo,
    Bulk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XnackMode {
    Silent,
    Fail,
    Fatal,
}

/// The command a [`Command::IncrBy`] was parsed from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrName {
    Incr,
    Decr,
    IncrBy,
    DecrBy,
}

impl IncrName {
    pub fn as_str(self) -> &'static str {
        match self {
            IncrName::Incr => "INCR",
            IncrName::Decr => "DECR",
            IncrName::IncrBy => "INCRBY",
            IncrName::DecrBy => "DECRBY",
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Object(ObjectSubcommand),
    Xinfo(XinfoSubcommand),
    Latency(LatencySubcommand),
    PubsubHelp,
    FunctionStats,
    FunctionKill,
    CommandCount,
    CommandList,
    CommandListFiltered {
        filter_type: String,
        filter_val: String,
    },
    CommandGetkeys(Vec<Bytes>),
    CommandGetkeysAndFlags(Vec<Bytes>),
    CommandInfo(Vec<String>),
    Hexpire {
        key: Bytes,
        expire_ms: i64,
        is_at: bool,
        condition: HexpireCondition,
        fields: Vec<Bytes>,
    },
    Httl {
        key: Bytes,
        is_ms: bool,
        is_expiretime: bool,
        fields: Vec<Bytes>,
    },
    Hpersist {
        key: Bytes,
        fields: Vec<Bytes>,
    },
    Hgetex {
        key: Bytes,
        expire: HFieldExpireOpt,
        fields: Vec<Bytes>,
    },
    Hsetex {
        key: Bytes,
        condition: HsetexCondition,
        expire: HFieldExpireOpt,
        pairs: Vec<(Bytes, Bytes)>,
    },
    Xclaim {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
        min_idle_time: u64,
        ids: Vec<Bytes>,
        idle: Option<u64>,
        time: Option<u64>,
        retrycount: Option<usize>,
        force: bool,
        justid: bool,
    },
    Xautoclaim {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
        min_idle_time: u64,
        start: Bytes,
        count: usize,
        justid: bool,
    },
    Auth {
        username: Option<String>,
        password: String,
    },
    Acl(AclSubcommand),
    Get(Bytes),
    Getex {
        key: Bytes,
        expire_in: Option<Duration>,
        persist: bool,
    },
    Set {
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        condition: SetCondition,
        get: bool,
        keepttl: bool,
        past_expired: bool,
    },
    Mget(Vec<Bytes>),
    Mset(Vec<(Bytes, Bytes)>),
    Msetex {
        pairs: Vec<(Bytes, Bytes)>,
        condition: MsetexCondition,
        expiry: MsetexExpiry,
    },
    Lcs {
        key1: Bytes,
        key2: Bytes,
        len_only: bool,
        idx: bool,
        min_match_len: usize,
        with_match_len: bool,
    },
    Digest(Bytes),
    Del(SmallVec<[Bytes; 1]>),
    Unlink(SmallVec<[Bytes; 1]>),
    Exists(SmallVec<[Bytes; 1]>),
    /// INCR, DECR, INCRBY and DECRBY, all as a signed delta. The name the
    /// client used is kept for ACL checks, stats and MONITOR.
    IncrBy(Bytes, i64, IncrName),
    Expire {
        key: Bytes,
        duration: Duration,
        opts: ExpireOptions,
    },
    Copy {
        source: Bytes,
        destination: Bytes,
        destination_db: Option<u32>,
        replace: bool,
    },
    Persist(Bytes),
    Ttl(Bytes, bool), // true for PTTL (milliseconds), false for TTL (seconds)
    Cluster(ClusterSubcommand),
    Client(ClientSubcommand),
    Asking,
    Readonly,
    Readwrite,
    Wait {
        numreplicas: usize,
        timeout: u64,
    },
    WaitAof {
        numlocal: usize,
        numreplicas: usize,
        timeout: u64,
    },
    Migrate {
        host: String,
        port: u16,
        key: Option<Bytes>,
        keys: Vec<Bytes>,
        destination_db: u32,
        timeout_ms: u64,
        copy: bool,
        replace: bool,
    },
    Hset {
        key: Bytes,
        fields: SmallVec<[(Bytes, Bytes); 1]>,
    },
    Hsetnx {
        key: Bytes,
        field: Bytes,
        value: Bytes,
    },
    Hmset {
        key: Bytes,
        fields: SmallVec<[(Bytes, Bytes); 1]>,
    },
    Hget {
        key: Bytes,
        field: Bytes,
    },
    Hmget {
        key: Bytes,
        fields: Vec<Bytes>,
    },
    Hdel {
        key: Bytes,
        fields: Vec<Bytes>,
    },
    Hexists {
        key: Bytes,
        field: Bytes,
    },
    Hlen(Bytes),
    Hgetall(Bytes),
    Hkeys(Bytes),
    Hvals(Bytes),
    Hstrlen {
        key: Bytes,
        field: Bytes,
    },
    Hgetdel {
        key: Bytes,
        fields: Vec<Bytes>,
    },
    // LIST COMMANDS
    Lpush {
        key: Bytes,
        values: SmallVec<[Bytes; 1]>,
    },
    Rpush {
        key: Bytes,
        values: SmallVec<[Bytes; 1]>,
    },
    Lpushx {
        key: Bytes,
        values: Vec<Bytes>,
    },
    Rpushx {
        key: Bytes,
        values: Vec<Bytes>,
    },
    Lpop {
        key: Bytes,
        count: Option<usize>,
    },
    Rpop {
        key: Bytes,
        count: Option<usize>,
    },
    Lrange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    Llen(Bytes),
    Lindex {
        key: Bytes,
        index: i64,
    },
    Blpop {
        keys: Vec<Bytes>,
        timeout: f64,
    },
    Brpop {
        keys: Vec<Bytes>,
        timeout: f64,
    },
    // SET COMMANDS
    Sadd {
        key: Bytes,
        members: SmallVec<[Bytes; 1]>,
    },
    Srem {
        key: Bytes,
        members: Vec<Bytes>,
    },
    Smembers(Bytes),
    Sismember {
        key: Bytes,
        member: Bytes,
    },
    Scard(Bytes),
    Spop {
        key: Bytes,
        count: Option<usize>,
    },
    Sinter(Vec<Bytes>),
    Sunion(Vec<Bytes>),
    Sdiff(Vec<Bytes>),
    Sinterstore {
        destination: Bytes,
        keys: Vec<Bytes>,
    },
    Sunionstore {
        destination: Bytes,
        keys: Vec<Bytes>,
    },
    Sdiffstore {
        destination: Bytes,
        keys: Vec<Bytes>,
    },
    Sintercard {
        keys: Vec<Bytes>,
        limit: usize,
    },
    Sunioncard {
        keys: Vec<Bytes>,
        limit: usize,
    },
    Sdiffcard {
        keys: Vec<Bytes>,
        limit: usize,
    },
    // ZSET COMMANDS
    Zadd {
        key: Bytes,
        elements: SmallVec<[(f64, Bytes); 1]>,
        flags: crate::table::ZAddFlags,
    },
    Zrem {
        key: Bytes,
        members: Vec<Bytes>,
    },
    Zscore {
        key: Bytes,
        member: Bytes,
    },
    Zcard(Bytes),
    Zrank {
        key: Bytes,
        member: Bytes,
        with_score: bool,
    },
    Zrevrank {
        key: Bytes,
        member: Bytes,
        with_score: bool,
    },
    Zcount {
        key: Bytes,
        min: f64,
        min_inc: bool,
        max: f64,
        max_inc: bool,
    },
    Zincrby {
        key: Bytes,
        delta: f64,
        member: Bytes,
    },
    Zrange {
        key: Bytes,
        opts: crate::table::ZRangeOpts,
    },
    Zrangestore {
        dst: Bytes,
        src: Bytes,
        opts: crate::table::ZRangeOpts,
    },
    Zpopmin {
        key: Bytes,
        count: Option<usize>,
    },
    Zpopmax {
        key: Bytes,
        count: Option<usize>,
    },
    Bzpopmin {
        keys: Vec<Bytes>,
        timeout: f64,
    },
    Bzpopmax {
        keys: Vec<Bytes>,
        timeout: f64,
    },
    Zmpop {
        keys: Vec<Bytes>,
        is_min: bool,
        count: usize,
    },
    Bzmpop {
        timeout: f64,
        keys: Vec<Bytes>,
        is_min: bool,
        count: usize,
    },
    Zunionstore {
        destination: Bytes,
        keys: Vec<Bytes>,
        weights: Vec<f64>,
        aggregate: crate::table::Aggregate,
    },
    Zinterstore {
        destination: Bytes,
        keys: Vec<Bytes>,
        weights: Vec<f64>,
        aggregate: crate::table::Aggregate,
    },
    Zdiffstore {
        destination: Bytes,
        keys: Vec<Bytes>,
    },
    Zdiff {
        keys: Vec<Bytes>,
        with_scores: bool,
    },
    Zinter {
        keys: Vec<Bytes>,
        weights: Vec<f64>,
        aggregate: crate::table::Aggregate,
        with_scores: bool,
    },
    Zunion {
        keys: Vec<Bytes>,
        weights: Vec<f64>,
        aggregate: crate::table::Aggregate,
        with_scores: bool,
    },
    Zintercard {
        keys: Vec<Bytes>,
        limit: usize,
    },
    // GENERIC & DATABASE COMMANDS
    Type(Bytes),
    Dbsize,
    Select(u32),
    Slowlog(Vec<Bytes>),
    Debug(Vec<Bytes>),
    Flushdb,
    Flushall,
    Touch(Vec<Bytes>),
    Rename {
        key: Bytes,
        newkey: Bytes,
        nx: bool,
    },
    // EXTENDED STRING COMMANDS
    Setnx {
        key: Bytes,
        value: Bytes,
    },
    Getset {
        key: Bytes,
        value: Bytes,
    },
    Getdel(Bytes),
    Append {
        key: Bytes,
        value: Bytes,
    },
    Strlen(Bytes),
    Msetnx(Vec<(Bytes, Bytes)>),
    Save,
    Bgsave,
    Bgrewriteaof,
    Lastsave,
    Ping(Option<Bytes>),
    Monitor,
    /// COMMAND DOCS [name ...] (lower-cased names).
    CommandDocs(Vec<String>),
    Info(Option<Bytes>),
    Replicaof {
        host: Bytes,
        port: Bytes,
    },
    Psync {
        replid: Bytes,
        offset: i64,
    },
    Sync,
    Replconf(Vec<Bytes>),
    Role,
    // LUA SCRIPTING COMMANDS
    Eval {
        script: Bytes,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        read_only: bool,
        auth_user: String,
    },
    Evalsha {
        sha: Bytes,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        read_only: bool,
        auth_user: String,
    },
    ScriptLoad(Bytes),
    ScriptExists(Vec<Bytes>),
    ScriptFlush,
    /// SCRIPT KILL. Scripts run to completion on their shard, so there is
    /// never one to kill.
    ScriptKill,
    // TIERED STORAGE COMMANDS
    Tier(TierSubcommand),
    // CONFIG COMMANDS
    ConfigGet(Vec<Bytes>),
    ConfigSet(Vec<(Bytes, Bytes)>),
    Shutdown {
        save: Option<bool>,
    },
    Quit,
    // PUBSUB COMMANDS
    Subscribe(Vec<Bytes>),
    Unsubscribe(Vec<Bytes>),
    Psubscribe(Vec<Bytes>),
    Punsubscribe(Vec<Bytes>),
    Publish {
        channel: Bytes,
        message: Bytes,
    },
    Spublish {
        channel: Bytes,
        message: Bytes,
    },
    Ssubscribe(Vec<Bytes>),
    Sunsubscribe(Vec<Bytes>),
    PubsubChannels(Option<Bytes>),
    PubsubNumsub(Vec<Bytes>),
    PubsubNumpat,
    PubsubShardchannels(Option<Bytes>),
    PubsubShardnumsub(Vec<Bytes>),
    // KEYSPACE INSPECTION
    Keys(Bytes),
    Scan {
        cursor: u64,
        pattern: Option<Bytes>,
        count: Option<usize>,
        key_type: Option<Bytes>,
    },
    Randomkey,
    Expiretime(Bytes, bool),
    // TRANSACTIONS
    Multi,
    Exec,
    Discard,
    Watch(Vec<Bytes>),
    Unwatch,
    // BITMAP COMMANDS
    Setbit {
        key: Bytes,
        offset: usize,
        value: u8,
    },
    Getbit {
        key: Bytes,
        offset: usize,
    },
    Bitcount {
        key: Bytes,
        start: Option<i64>,
        end: Option<i64>,
        is_bit: bool,
    },
    Bitpos {
        key: Bytes,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        is_bit: bool,
    },
    Bitfield {
        key: Bytes,
        ops: Vec<BitfieldSubOp>,
        readonly: bool,
    },
    Bitop {
        op: String,
        destkey: Bytes,
        srckeys: Vec<Bytes>,
    },
    Sort {
        key: Bytes,
        desc: bool,
        alpha: bool,
        store: Option<Bytes>,
        limit: Option<(i64, i64)>,
        by: Option<Bytes>,
        get: Vec<Bytes>,
        readonly: bool,
    },
    // HYPERLOGLOG COMMANDS
    Pfadd {
        key: Bytes,
        elements: Vec<Bytes>,
    },
    Pfcount {
        keys: Vec<Bytes>,
    },
    Pfmerge {
        destkey: Bytes,
        srckeys: Vec<Bytes>,
    },
    PfdebugGetreg(Bytes),
    PfdebugEncoding(Bytes),
    PfdebugTodense(Bytes),
    PfdebugSimd(bool),
    Pfselftest,
    // RDB SERIALIZATION
    Dump(Bytes),
    Restore {
        key: Bytes,
        ttl_ms: u64,
        serialized: Bytes,
        replace: bool,
        absttl: bool,
    },
    // STREAM COMMANDS
    Xadd {
        key: Bytes,
        nomkstream: bool,
        maxlen: Option<usize>,
        minid: Option<crate::table::StreamId>,
        approx: bool,
        trim_strategy: crate::table::StreamTrimStrategy,
        idmp: Option<crate::table::StreamIdmpOption>,
        id: crate::table::StreamAddId,
        fields: Vec<(Bytes, Bytes)>,
        limit: Option<usize>,
    },
    Xlen(Bytes),
    Xrange {
        key: Bytes,
        start: String,
        end: String,
        count: Option<usize>,
    },
    Xrevrange {
        key: Bytes,
        end: String,
        start: String,
        count: Option<usize>,
    },
    Xread {
        count: Option<usize>,
        maxcount: Option<usize>,
        maxsize: Option<usize>,
        block_ms: Option<u64>,
        keys: Vec<Bytes>,
        ids: Vec<String>,
    },
    Xdel {
        key: Bytes,
        ids: Vec<crate::table::StreamId>,
    },
    Xtrim {
        key: Bytes,
        maxlen: Option<usize>,
        minid: Option<crate::table::StreamId>,
        approx: bool,
        trim_strategy: crate::table::StreamTrimStrategy,
        limit: Option<usize>,
    },
    Xcfgset {
        key: Bytes,
        duration: Option<u64>,
        maxsize: Option<usize>,
    },
    Xsetid {
        key: Bytes,
        last_id: crate::table::StreamId,
        entries_added: Option<u64>,
        max_deleted_id: Option<crate::table::StreamId>,
    },
    Xdelex {
        key: Bytes,
        strategy: crate::table::StreamTrimStrategy,
        ids: Vec<crate::table::StreamId>,
    },
    Xackdel {
        key: Bytes,
        group: Bytes,
        strategy: crate::table::StreamTrimStrategy,
        ids: Vec<crate::table::StreamId>,
    },
    Xidmprecord {
        key: Bytes,
        pid: Bytes,
        iid: Bytes,
        id_raw: Bytes,
    },
    XgroupCreate {
        key: Bytes,
        group: Bytes,
        id: String,
        mkstream: bool,
        entries_read: Option<u64>,
    },
    XgroupDestroy {
        key: Bytes,
        group: Bytes,
    },
    XgroupCreateConsumer {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
    },
    XgroupDelConsumer {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
    },
    XgroupSetId {
        key: Bytes,
        group: Bytes,
        id: String,
        entries_read: Option<u64>,
    },
    XgroupHelp,
    Xreadgroup {
        group: Bytes,
        consumer: Bytes,
        count: Option<usize>,
        maxcount: Option<usize>,
        maxsize: Option<usize>,
        block_ms: Option<u64>,
        noack: bool,
        claim: Option<u64>,
        keys: Vec<Bytes>,
        ids: Vec<String>,
    },
    Xack {
        key: Bytes,
        group: Bytes,
        ids: Vec<crate::table::StreamId>,
    },
    Xnack {
        key: Bytes,
        group: Bytes,
        mode: XnackMode,
        ids: Vec<crate::table::StreamId>,
        retrycount: Option<usize>,
        force: bool,
    },
    Xpending {
        key: Bytes,
        group: Bytes,
        range: Option<(
            std::ops::Bound<crate::table::StreamId>,
            std::ops::Bound<crate::table::StreamId>,
            usize,
            Option<Bytes>,
            Option<u64>,
        )>,
    },
    // VALKEY EXTENDED COMMANDS
    Hello {
        proto: Option<i64>,
        auth: Option<(String, String)>,
        setname: Option<String>,
    },
    Reset,
    Time,
    Echo(Bytes),
    Hincrby {
        key: Bytes,
        field: Bytes,
        increment: i64,
    },
    Hincrbyfloat {
        key: Bytes,
        field: Bytes,
        increment: f64,
    },
    Hrandfield {
        key: Bytes,
        count: Option<i64>,
        with_values: bool,
    },
    Hscan {
        key: Bytes,
        cursor: usize,
        pattern: Option<Bytes>,
        count: Option<usize>,
        no_values: bool,
    },
    Smismember {
        key: Bytes,
        members: Vec<Bytes>,
    },
    Srandmember {
        key: Bytes,
        count: Option<i64>,
    },
    Smove {
        source: Bytes,
        destination: Bytes,
        member: Bytes,
    },
    Sscan {
        key: Bytes,
        cursor: u64,
        pattern: Option<Bytes>,
        count: Option<usize>,
    },
    Zmscore {
        key: Bytes,
        members: Vec<Bytes>,
    },
    Zrandmember {
        key: Bytes,
        count: Option<i64>,
        with_scores: bool,
    },
    Zremrangebyrank {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    Zremrangebyscore {
        key: Bytes,
        min_score: f64,
        min_inc: bool,
        max_score: f64,
        max_inc: bool,
    },
    Zremrangebylex {
        key: Bytes,
        min: crate::table::LexBound,
        max: crate::table::LexBound,
    },
    Zlexcount {
        key: Bytes,
        min: crate::table::LexBound,
        max: crate::table::LexBound,
    },
    Zscan {
        key: Bytes,
        cursor: usize,
        pattern: Option<Bytes>,
        count: Option<usize>,
    },
    Ltrim {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    Lset {
        key: Bytes,
        index: i64,
        element: Bytes,
    },
    Lrem {
        key: Bytes,
        count: i64,
        element: Bytes,
    },
    Lpos {
        key: Bytes,
        element: Bytes,
        rank: Option<i64>,
        count: Option<usize>,
        maxlen: Option<usize>,
    },
    Linsert {
        key: Bytes,
        before: bool,
        pivot: Bytes,
        element: Bytes,
    },
    Lmove {
        source: Bytes,
        destination: Bytes,
        where_from: crate::table::ListDirection,
        where_to: crate::table::ListDirection,
    },
    Blmove {
        source: Bytes,
        destination: Bytes,
        where_from: crate::table::ListDirection,
        where_to: crate::table::ListDirection,
        timeout: f64,
    },
    Lmovem {
        source: Bytes,
        destination: Bytes,
        where_from: crate::table::ListDirection,
        where_to: crate::table::ListDirection,
        mode: LmovemMode,
        count: usize,
        ordering: LmovemOrdering,
        raw_tokens: Option<[Bytes; 4]>,
    },
    Blmovem {
        source: Bytes,
        destination: Bytes,
        where_from: crate::table::ListDirection,
        where_to: crate::table::ListDirection,
        timeout: f64,
        mode: LmovemMode,
        count: usize,
        ordering: LmovemOrdering,
    },
    Lmpop {
        keys: Vec<Bytes>,
        where_from: crate::table::ListDirection,
        count: usize,
    },
    Blmpop {
        timeout: f64,
        keys: Vec<Bytes>,
        where_from: crate::table::ListDirection,
        count: usize,
    },
    Incrbyfloat {
        key: Bytes,
        increment: f64,
    },
    Increx {
        key: Bytes,
        increment: IncrexIncrement,
        lbound: Option<IncrexBound>,
        ubound: Option<IncrexBound>,
        saturate: bool,
        expire: Option<IncrexExpire>,
        enx: bool,
    },
    Setrange {
        key: Bytes,
        offset: usize,
        value: Bytes,
    },
    Getrange {
        key: Bytes,
        start: i64,
        end: i64,
    },
    // VECTOR COMMANDS
    Vadd {
        key: Bytes,
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
        cas: bool,
        is_redis_vset: bool,
    },
    Vquery {
        key: Bytes,
        k: usize,
        query: Vec<f32>,
        rerank: bool,
    },
    /// Redis 8 `VSIM key (ELE elem | FP32 blob | VALUES n ...) [WITHSCORES] [WITHATTRIBS] ...`
    Vsim {
        key: Bytes,
        target: VsimTarget,
        with_scores: bool,
        with_attribs: bool,
        count: usize,
        epsilon: Option<f32>,
        ef: Option<usize>,
        filter: Option<String>,
        filter_ef: Option<usize>,
        truth: bool,
        no_thread: bool,
    },
    /// Legacy Rudis pairwise distance `VSIM index k1 k2 [COSINE|L2|IP]`.
    Vdist {
        key: Bytes,
        k1: Bytes,
        k2: Bytes,
        metric: Option<crate::vector::VectorMetric>,
    },
    Vdel {
        key: Bytes,
        element: Bytes,
    },
    Vinfo(Bytes),
    Vcard(Bytes),
    Vdim(Bytes),
    Vemb {
        key: Bytes,
        element: Bytes,
        raw: bool,
    },
    Vlinks {
        key: Bytes,
        element: Bytes,
        with_scores: bool,
    },
    Vrandmember {
        key: Bytes,
        count: Option<i64>,
    },
    Vsetattr {
        key: Bytes,
        element: Bytes,
        attr: String,
    },
    Vgetattr {
        key: Bytes,
        element: Bytes,
    },
    Vismember {
        key: Bytes,
        element: Bytes,
    },
    // AI SEMANTIC CACHE COMMANDS
    SemanticSet {
        namespace: Bytes,
        id: Bytes,
        prompt: Bytes,
        response: Bytes,
        vector: Vec<f32>,
        ttl: Option<Duration>,
        scope: Option<Bytes>,
        quantize: bool,
        tokens: Option<u64>,
    },
    SemanticGet {
        namespace: Bytes,
        query: Vec<f32>,
        threshold: f32,
        scope: Option<Bytes>,
        with_score: bool,
        with_prompt: bool,
        with_id: bool,
    },
    SemanticDel {
        namespace: Bytes,
        ids: Vec<Bytes>,
    },
    SemanticFlush(Bytes),
    SemanticInfo(Bytes),
    // AI AGENT MEMORY COMMANDS
    AgentMemAdd {
        session: Bytes,
        role: Bytes,
        content: Bytes,
        tokens: Option<u64>,
        vector: Option<Vec<f32>>,
        meta: Option<Bytes>,
    },
    AgentMemContext {
        session: Bytes,
        max_tokens: u64,
        query: Option<Vec<f32>>,
        recall_k: usize,
    },
    AgentMemCompact {
        session: Bytes,
        keep_recent: usize,
        summary: Bytes,
        tokens: Option<u64>,
        vector: Option<Vec<f32>>,
    },
    AgentMemInfo(Bytes),
    AgentMemClear(Bytes),
    // LLM QUOTA GOVERNOR COMMANDS
    LlmQuotaReserve {
        key: Bytes,
        rpm: usize,
        tpm: u64,
        est_tokens: u64,
        window_ms: Option<u64>,
    },
    LlmQuotaSettle {
        key: Bytes,
        reservation_id: u64,
        actual_tokens: u64,
    },
    LlmQuotaInfo(Bytes),
    // AGENT CHECKPOINT & IDEMPOTENT TOOL EXECUTION COMMANDS
    AgentCheckpointPut {
        key: Bytes,
        step_id: Bytes,
        parent_id: Option<Bytes>,
        state: Bytes,
        meta: Option<Bytes>,
    },
    AgentCheckpointGet {
        key: Bytes,
        step_id: Option<Bytes>,
    },
    AgentCheckpointHistory {
        key: Bytes,
        from_step: Option<Bytes>,
        limit: usize,
    },
    AgentToolClaim {
        key: Bytes,
        call_id: Bytes,
        ttl_ms: u64,
        input: Option<Bytes>,
    },
    AgentToolComplete {
        key: Bytes,
        call_id: Bytes,
        output: Bytes,
        ttl_ms: Option<u64>,
    },
    // MODEL CONTEXT PROTOCOL (MCP) COMMANDS
    McpTools,
    McpCall {
        tool: String,
        args_json: Bytes,
    },
    McpRpc(Bytes),
    // CRDT MULTI-REGION COMMANDS
    CrdtSet {
        key: Bytes,
        val: Bytes,
    },
    CrdtGet(Bytes),
    CrdtDel(Bytes),
    CrdtIncrby {
        key: Bytes,
        delta: i64,
    },
    CrdtSadd {
        key: Bytes,
        member: Bytes,
    },
    CrdtSmembers(Bytes),
    CrdtSrem {
        key: Bytes,
        member: Bytes,
    },
    CrdtDump,
    /// `CRDT.MERGE payload`. The CRDT write handlers log their effect in this
    /// form (the changed key's state as a payload), since their HLC
    /// timestamps would differ on replay.
    CrdtMerge(Bytes),
    CrdtGc(crate::crdt::GcHorizon),
    // REDIS 7 FUNCTIONS
    FunctionLoad {
        replace: bool,
        code: Bytes,
    },
    Fcall {
        function: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        read_only: bool,
        auth_user: String,
    },
    FunctionList {
        library_name_pattern: Option<String>,
        with_code: bool,
    },
    FunctionDelete(String),
    FunctionFlush,
    FunctionDump,
    FunctionRestore {
        payload: Bytes,
        policy: String,
    },
    // REDISJSON COMMANDS
    JsonSet {
        key: Bytes,
        path: String,
        json_val: String,
        nx: bool,
        xx: bool,
    },
    JsonGet {
        key: Bytes,
        paths: Vec<String>,
    },
    JsonDel {
        key: Bytes,
        path: Option<String>,
    },
    JsonType {
        key: Bytes,
        path: Option<String>,
    },
    JsonNumIncrBy {
        key: Bytes,
        path: String,
        delta: f64,
    },
    JsonNumMultBy {
        key: Bytes,
        path: String,
        factor: f64,
    },
    JsonStrAppend {
        key: Bytes,
        path: Option<String>,
        value: String,
    },
    JsonStrLen {
        key: Bytes,
        path: Option<String>,
    },
    JsonArrAppend {
        key: Bytes,
        path: String,
        values: Vec<String>,
    },
    JsonArrLen {
        key: Bytes,
        path: Option<String>,
    },
    JsonArrPop {
        key: Bytes,
        path: Option<String>,
        index: Option<isize>,
    },
    JsonObjKeys {
        key: Bytes,
        path: Option<String>,
    },
    JsonObjLen {
        key: Bytes,
        path: Option<String>,
    },
    JsonToggle {
        key: Bytes,
        path: String,
    },
    JsonClear {
        key: Bytes,
        path: Option<String>,
    },
    JsonMget {
        keys: Vec<Bytes>,
        path: String,
    },
    // GEOSPATIAL COMMANDS
    Geoadd {
        key: Bytes,
        items: Vec<(f64, f64, Bytes)>,
        nx: bool,
        xx: bool,
        ch: bool,
    },
    Geodist {
        key: Bytes,
        m1: Bytes,
        m2: Bytes,
        unit: Option<crate::geo::GeoUnit>,
    },
    Geopos {
        key: Bytes,
        members: Vec<Bytes>,
    },
    Geohash {
        key: Bytes,
        members: Vec<Bytes>,
    },
    Georadius {
        key: Bytes,
        lon: f64,
        lat: f64,
        radius: f64,
        unit: crate::geo::GeoUnit,
        withcoord: bool,
        withdist: bool,
        withhash: bool,
        count: Option<usize>,
        any: bool,
        asc: Option<bool>,
        store: Option<Bytes>,
        storedist: Option<Bytes>,
    },
    Georadiusbymember {
        key: Bytes,
        member: Bytes,
        radius: f64,
        unit: crate::geo::GeoUnit,
        withcoord: bool,
        withdist: bool,
        withhash: bool,
        count: Option<usize>,
        any: bool,
        asc: Option<bool>,
        store: Option<Bytes>,
        storedist: Option<Bytes>,
    },
    Geosearch {
        key: Bytes,
        from_member: Option<Bytes>,
        from_lonlat: Option<(f64, f64)>,
        by_radius: Option<(f64, crate::geo::GeoUnit)>,
        by_box: Option<(f64, f64, crate::geo::GeoUnit)>,
        asc: Option<bool>,
        count: Option<usize>,
        any: bool,
        withcoord: bool,
        withdist: bool,
        withhash: bool,
    },
    Geosearchstore {
        dest: Bytes,
        key: Bytes,
        from_member: Option<Bytes>,
        from_lonlat: Option<(f64, f64)>,
        by_radius: Option<(f64, crate::geo::GeoUnit)>,
        by_box: Option<(f64, f64, crate::geo::GeoUnit)>,
        asc: Option<bool>,
        count: Option<usize>,
        any: bool,
        storedist: bool,
    },
    // PROBABILISTIC COMMANDS
    BfReserve {
        key: Bytes,
        error_rate: f64,
        capacity: usize,
    },
    BfAdd {
        key: Bytes,
        item: Bytes,
    },
    BfMadd {
        key: Bytes,
        items: Vec<Bytes>,
    },
    BfExists {
        key: Bytes,
        item: Bytes,
    },
    BfMexists {
        key: Bytes,
        items: Vec<Bytes>,
    },
    BfInfo(Bytes),
    CfReserve {
        key: Bytes,
        capacity: usize,
    },
    CfAdd {
        key: Bytes,
        item: Bytes,
    },
    CfAddnx {
        key: Bytes,
        item: Bytes,
    },
    CfExists {
        key: Bytes,
        item: Bytes,
    },
    CfDel {
        key: Bytes,
        item: Bytes,
    },
    CfInfo(Bytes),
    CmsInitbydim {
        key: Bytes,
        width: usize,
        depth: usize,
    },
    CmsInitbyprob {
        key: Bytes,
        error: f64,
        probability: f64,
    },
    CmsIncrby {
        key: Bytes,
        pairs: Vec<(Bytes, u64)>,
    },
    CmsQuery {
        key: Bytes,
        items: Vec<Bytes>,
    },
    CmsInfo(Bytes),
    TopkReserve {
        key: Bytes,
        topk: usize,
    },
    TopkAdd {
        key: Bytes,
        items: Vec<Bytes>,
    },
    TopkQuery {
        key: Bytes,
        items: Vec<Bytes>,
    },
    TopkList(Bytes),
    TopkInfo(Bytes),
    /// `BF.RESTORE`/`CF.RESTORE`/`CMS.RESTORE`/`TOPK.RESTORE key payload`:
    /// replaces `key`'s structure with an encoded state. The AOF rewrite
    /// emits these, since Bloom and Count-Min state can't be rebuilt from
    /// the commands that made it.
    ProbRestore {
        kind: crate::probabilistic::ProbKind,
        key: Bytes,
        payload: Bytes,
    },
    // Full-Text Search (FT.*)
    FtCreate {
        index: String,
        on_type: String,
        prefixes: Vec<String>,
        fields: std::collections::HashMap<String, crate::search::FieldType>,
        schema_fields: Vec<crate::search::SchemaField>,
    },
    FtSearch {
        index: String,
        query: String,
        options: crate::search::SearchOptions,
    },
    FtAggregate {
        index: String,
        query: String,
        options: crate::search::AggregateOptions,
    },
    FtInfo(String),
    FtDropIndex {
        index: String,
        dd: bool,
    },
    FtExplain {
        index: String,
        query: String,
    },
    FtAdd {
        index: String,
        doc_id: String,
        score: f64,
        fields: Vec<(String, String)>,
    },
    FtList,
    FtAlter {
        index: String,
        fields: std::collections::HashMap<String, crate::search::FieldType>,
        schema_fields: Vec<crate::search::SchemaField>,
    },
    FtProfile {
        index: String,
        query: String,
        options: crate::search::SearchOptions,
    },
    // AF_XDP & eBPF (XDP.*)
    XdpInfo,
    XdpRuleAdd {
        action: crate::xdp::XdpAction,
        cidr: String,
    },
    XdpRuleDel(u32),
    XdpRuleList,
    XdpStats,
    XdpPacket(Bytes),
    XdpSocket(u32),
    XdpInject {
        queue_id: u32,
        payload: Bytes,
    },
    // Dragonfly native extensions
    DflyCluster(DflyClusterSubcommand),
    DflyMigrate(DflyMigrateSubcommand),
    DflyFlow {
        master_replid: String,
        sync_id: String,
        shard_id: usize,
        lsn: Option<u64>,
    },
    Stick(Vec<Bytes>),
    Unstick(Vec<Bytes>),
    Sticky(Bytes),
    Delex {
        key: Bytes,
        condition: Option<(String, Bytes)>,
    },
    // Memcached Protocol
    MemcachedSet {
        key: Bytes,
        flags: u32,
        exptime: u32,
        bytes: usize,
        noreply: bool,
        data: Bytes,
    },
    MemcachedAdd {
        key: Bytes,
        flags: u32,
        exptime: u32,
        bytes: usize,
        noreply: bool,
        data: Bytes,
    },
    MemcachedReplace {
        key: Bytes,
        flags: u32,
        exptime: u32,
        bytes: usize,
        noreply: bool,
        data: Bytes,
    },
    MemcachedGet {
        keys: Vec<Bytes>,
    },
    MemcachedDelete {
        key: Bytes,
        noreply: bool,
    },
    MemcachedIncr {
        key: Bytes,
        value: u64,
        noreply: bool,
    },
    MemcachedDecr {
        key: Bytes,
        value: u64,
        noreply: bool,
    },
    MemcachedStats,
    MemcachedVersion,
    MemcachedQuit,
    Memory(MemorySubcommand),
    Unknown(String),
}

impl Command {
    pub fn allows_oom(&self) -> bool {
        match self {
            Command::Eval { script, .. } => {
                let s = script.as_ref();
                if s.starts_with(b"#!") {
                    let s_str = String::from_utf8_lossy(s);
                    let first_line = s_str.lines().next().unwrap_or("");
                    first_line.contains("allow-oom")
                } else {
                    false
                }
            }
            Command::Evalsha { sha, .. } => {
                let sha_str = String::from_utf8_lossy(sha);
                if let Some(cached) = crate::scripting::get_script(&sha_str) {
                    if cached.starts_with("#!") {
                        let first_line = cached.lines().next().unwrap_or("");
                        first_line.contains("allow-oom")
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            // Like Redis, MIGRATE is not denied over maxmemory: it only removes
            // local keys, so it is one way to get back under the limit.
            Command::Migrate { .. } => true,
            _ => false,
        }
    }

    pub fn is_write_command(&self) -> bool {
        match self {
            Command::Eval {
                script, read_only, ..
            } => {
                if *read_only {
                    return false;
                }
                let s = script.as_ref();
                if s.starts_with(b"#!") {
                    let s_str = String::from_utf8_lossy(s);
                    let first_line = s_str.lines().next().unwrap_or("");
                    !first_line.contains("no-writes")
                } else {
                    false
                }
            }
            Command::Evalsha { sha, read_only, .. } => {
                if *read_only {
                    return false;
                }
                let sha_str = String::from_utf8_lossy(sha);
                if let Some(cached) = crate::scripting::get_script(&sha_str) {
                    if cached.starts_with("#!") {
                        let first_line = cached.lines().next().unwrap_or("");
                        !first_line.contains("no-writes")
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Command::Fcall {
                function,
                read_only,
                ..
            } => {
                if *read_only {
                    return false;
                }
                !crate::scripting::is_function_read_only(function)
            }
            _ => {
                matches!(
                    crate::connection::get_cmd_name(self),
                    "SET"
                        | "SETEX"
                        | "PSETEX"
                        | "SETNX"
                        | "MSET"
                        | "MSETNX"
                        | "MSETEX"
                        | "GETSET"
                        | "GETDEL"
                        | "APPEND"
                        | "INCR"
                        | "DECR"
                        | "INCRBY"
                        | "DECRBY"
                        | "INCRBYFLOAT"
                        | "DEL"
                        | "UNLINK"
                        | "EXPIRE"
                        | "PEXPIRE"
                        | "EXPIREAT"
                        | "PEXPIREAT"
                        | "PERSIST"
                        | "HEXPIRE"
                        | "HEXPIREAT"
                        | "HPEXPIRE"
                        | "HPEXPIREAT"
                        | "HPERSIST"
                        | "HSETEX"
                        | "HSET"
                        | "HSETNX"
                        | "HMSET"
                        | "HDEL"
                        | "HINCRBY"
                        | "HINCRBYFLOAT"
                        | "LPUSH"
                        | "RPUSH"
                        | "LPUSHX"
                        | "RPUSHX"
                        | "LPOP"
                        | "RPOP"
                        | "LSET"
                        | "LTRIM"
                        | "LREM"
                        | "LMOVE"
                        | "BLMOVE"
                        | "BLPOP"
                        | "BRPOP"
                        | "BRPOPLPUSH"
                        | "LMPOP"
                        | "BLMPOP"
                        | "SADD"
                        | "SREM"
                        | "SPOP"
                        | "SMOVE"
                        | "ZADD"
                        | "ZINCRBY"
                        | "ZREM"
                        | "ZREMRANGEBYRANK"
                        | "ZREMRANGEBYSCORE"
                        | "ZREMRANGEBYLEX"
                        | "ZPOPMAX"
                        | "ZPOPMIN"
                        | "BZPOPMAX"
                        | "BZPOPMIN"
                        | "ZMPOP"
                        | "BZMPOP"
                        | "XADD"
                        | "XDEL"
                        | "XTRIM"
                        | "XGROUP"
                        | "XACK"
                        | "XCLAIM"
                        | "XAUTOCLAIM"
                        | "FLUSHDB"
                        | "FLUSHALL"
                ) || {
                    let name = crate::connection::acl_cmd_name(self);
                    // rudis-only families (JSON, BF, FT, ...) share one
                    // coarse name tagged @write for ACLs, so exclude their
                    // read-only subcommands.
                    is_write_in_command_table(name) && !self.is_family_read()
                }
            }
        }
    }

    /// Read-only subcommands of the rudis-only command families whose
    /// shared `acl_cmd_name` is tagged @write (see EXTRA in
    /// scripts/gen_acl_categories.py).
    fn is_family_read(&self) -> bool {
        matches!(
            self,
            Command::JsonGet { .. }
                | Command::JsonType { .. }
                | Command::JsonStrLen { .. }
                | Command::JsonArrLen { .. }
                | Command::JsonObjKeys { .. }
                | Command::JsonObjLen { .. }
                | Command::JsonMget { .. }
                | Command::BfExists { .. }
                | Command::BfMexists { .. }
                | Command::BfInfo(_)
                | Command::CfExists { .. }
                | Command::CfInfo(_)
                | Command::CmsQuery { .. }
                | Command::CmsInfo(_)
                | Command::TopkQuery { .. }
                | Command::TopkList(_)
                | Command::TopkInfo(_)
                | Command::FtSearch { .. }
                | Command::FtAggregate { .. }
                | Command::FtInfo(_)
                | Command::FtExplain { .. }
                | Command::FtList
                | Command::FtProfile { .. }
                | Command::SemanticGet { .. }
                | Command::SemanticInfo(_)
                | Command::CrdtGet(_)
                | Command::CrdtSmembers(_)
                | Command::CrdtDump
        )
    }
}

/// Whether `name` (as returned by `acl_cmd_name`) is in the `@write`
/// category of the command table generated from Valkey's, i.e. carries the
/// WRITE flag. Cached per thread by the name's address, since every command
/// goes through here.
fn is_write_in_command_table(name: &'static str) -> bool {
    thread_local! {
        static CACHE: std::cell::RefCell<fxhash::FxHashMap<(usize, usize), bool>> =
            std::cell::RefCell::new(fxhash::FxHashMap::default());
    }
    CACHE.with(|cache| {
        *cache
            .borrow_mut()
            .entry((name.as_ptr() as usize, name.len()))
            .or_insert_with(|| {
                let write_bit = crate::acl_categories::CATEGORIES
                    .iter()
                    .position(|c| *c == "write")
                    .map_or(0, |i| 1u32 << i);
                crate::acl::command_index(name)
                    .is_some_and(|i| crate::acl_categories::COMMANDS[i].cats & write_bit != 0)
            })
    })
}

fn parse_memcached_storage_command(buf: &mut BytesMut) -> Result<Option<Option<Command>>, String> {
    // Only the first line is looked at: searching the whole buffer for a CRLF
    // made a run of bare-LF frames quadratic.
    let newline_pos = match find_newline(buf) {
        Some((line_end, advance_len)) if advance_len == line_end + 2 => line_end,
        _ => return Ok(None),
    };
    let line = &buf[..newline_pos];
    let first_space = match line.iter().position(|&b| b == b' ' || b == b'\t') {
        Some(p) => p,
        None => return Ok(None),
    };
    let first_word = &line[..first_space];
    let is_set = first_word.eq_ignore_ascii_case(b"set");
    let is_add = first_word.eq_ignore_ascii_case(b"add");
    let is_replace = first_word.eq_ignore_ascii_case(b"replace");
    if !is_set && !is_add && !is_replace {
        return Ok(None);
    }
    let parts: Vec<&[u8]> = line
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|p| !p.is_empty())
        .collect();
    if parts.len() < 5 {
        return Ok(None);
    }
    let flags: u32 = match std::str::from_utf8(parts[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(f) => f,
        None => return Ok(None),
    };
    let exptime: u32 = match std::str::from_utf8(parts[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(e) => e,
        None => return Ok(None),
    };
    let bytes_len: usize = match std::str::from_utf8(parts[4])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(b) => b,
        None => return Ok(None),
    };
    let noreply = parts.len() > 5 && parts[5].eq_ignore_ascii_case(b"noreply");

    // Like an oversized RESP bulk, a data block beyond proto-max-bulk-len is a
    // protocol error (closes the connection) rather than buffering it forever;
    // this also keeps the length arithmetic below from overflowing.
    if bytes_len > get_proto_max_bulk_len() {
        return Err("Protocol error: invalid bulk length".to_string());
    }
    let total_len = newline_pos + 2 + bytes_len + 2;
    if buf.len() < total_len {
        return Ok(Some(None));
    }
    if &buf[newline_pos + 2 + bytes_len..total_len] != b"\r\n" {
        // Swallow the malformed block: the caller keeps parsing after a
        // non-protocol error, so leaving it in `buf` would loop forever.
        buf.advance(total_len);
        return Err("CLIENT_ERROR bad data chunk".to_string());
    }

    let key = Bytes::copy_from_slice(parts[1]);
    let data = Bytes::copy_from_slice(&buf[newline_pos + 2..newline_pos + 2 + bytes_len]);
    buf.advance(total_len);

    let cmd = if is_set {
        Command::MemcachedSet {
            key,
            flags,
            exptime,
            bytes: bytes_len,
            noreply,
            data,
        }
    } else if is_add {
        Command::MemcachedAdd {
            key,
            flags,
            exptime,
            bytes: bytes_len,
            noreply,
            data,
        }
    } else {
        Command::MemcachedReplace {
            key,
            flags,
            exptime,
            bytes: bytes_len,
            noreply,
            data,
        }
    };
    Ok(Some(Some(cmd)))
}

fn parse_min_idle_time(s: &str) -> Result<u64, String> {
    if s.is_empty() || s.starts_with('+') || s == "-0" {
        return Err("ERR min-idle-time is not an integer".to_string());
    }
    if s.starts_with('-') {
        if let Ok(val) = s.parse::<i64>()
            && val < 0
        {
            return Err("ERR min-idle-time must be a positive integer".to_string());
        }
        return Err("ERR min-idle-time is not an integer".to_string());
    }
    match s.parse::<u64>() {
        Ok(val) => Ok(val),
        Err(_) => Err("ERR min-idle-time is not an integer".to_string()),
    }
}

/// Parse a single Redis command from the buffer.
/// Supports both RESP arrays (e.g., `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n`)
/// and inline commands (e.g., `GET foo\r\n`).
pub fn parse_command(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    // Frames that carry no command (blank inline lines, `*0`, `*-1`) are
    // consumed here in a loop rather than by recursing: a peer can send
    // millions of them for a byte or two each, which would overflow the stack.
    loop {
        if buf.is_empty() {
            return Ok(None);
        }
        let mut skipped = false;
        let parsed = if buf[0] == b'*' {
            parse_resp_array(buf, &mut skipped)
        } else {
            match parse_memcached_storage_command(buf)? {
                Some(Some(cmd)) => Ok(Some(cmd)),
                Some(None) => Ok(None),
                None => parse_inline_command(buf, &mut skipped),
            }
        };
        if !skipped {
            return parsed;
        }
    }
}

#[inline]
pub fn parse_decimal_bytes(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut val: usize = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(val)
}

#[inline]
pub fn bytes_to_uppercase_ascii<'a>(
    bytes: &'a [u8],
    buf: &'a mut [u8; 64],
    heap: &'a mut String,
) -> &'a str {
    if bytes.len() <= 64 && bytes.is_ascii() {
        for (i, b) in bytes.iter().enumerate() {
            buf[i] = b.to_ascii_uppercase();
        }
        // SAFETY: All input bytes were ASCII, and ASCII uppercase preserves ASCII values (< 128),
        // which are guaranteed to be valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&buf[..bytes.len()]) }
    } else {
        *heap = String::from_utf8_lossy(bytes).to_uppercase();
        heap.as_str()
    }
}

pub static PROTO_MAX_BULK_LEN: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(512 * 1024 * 1024);

#[inline(always)]
pub fn get_proto_max_bulk_len() -> usize {
    PROTO_MAX_BULK_LEN.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn set_proto_max_bulk_len(val: usize) {
    PROTO_MAX_BULK_LEN.store(val, std::sync::atomic::Ordering::Relaxed);
}

/// Sets `skipped` instead of returning when the frame is an empty array.
fn parse_resp_array(buf: &mut BytesMut, skipped: &mut bool) -> Result<Option<Command>, String> {
    let (newline_pos, advance_len) = match find_newline(buf) {
        Some(res) => res,
        None => {
            if buf.len() > 64 * 1024 {
                return Err("Protocol error: too big mbulk count string".to_string());
            }
            return Ok(None);
        }
    };

    let line = &buf[1..newline_pos];
    if line.starts_with(b"-") {
        buf.advance(advance_len);
        *skipped = true;
        return Ok(None);
    }
    let num_args: usize = match parse_decimal_bytes(line) {
        Some(n) => {
            if n > 1024 * 1024 {
                return Err("Protocol error: invalid multibulk length".to_string());
            }
            n
        }
        None => return Err("Protocol error: invalid multibulk length".to_string()),
    };
    if num_args == 0 {
        buf.advance(advance_len);
        *skipped = true;
        return Ok(None);
    }

    // First check if the full frame is present before consuming any bytes from buf
    let mut scan_cursor = advance_len;
    let mut offsets = [(0usize, 0usize); 16];
    let is_small = num_args <= 16;

    #[allow(clippy::needless_range_loop)]
    for i in 0..num_args {
        if scan_cursor >= buf.len() {
            return Ok(None);
        }
        if buf[scan_cursor] != b'$' {
            return Err(format!(
                "Protocol error: expected '$', got '{}'",
                buf[scan_cursor] as char
            ));
        }

        let (next_crlf, next_advance) = match find_newline_at(buf, scan_cursor) {
            Some(res) => res,
            None => {
                if buf.len() - scan_cursor > 64 * 1024 {
                    return Err("Protocol error: too big bulk count string".to_string());
                }
                return Ok(None);
            }
        };

        let len_str = &buf[scan_cursor + 1..next_crlf];
        if len_str.starts_with(b"-") {
            return Err("Protocol error: invalid bulk length".to_string());
        }
        let arg_len: usize = match parse_decimal_bytes(len_str) {
            Some(len) => len,
            None => return Err("Protocol error: invalid bulk length".to_string()),
        };

        if arg_len > get_proto_max_bulk_len() {
            return Err("Protocol error: invalid bulk length".to_string());
        }

        let data_start = scan_cursor + next_advance;
        let data_end = data_start + arg_len;

        if data_end + 2 > buf.len() {
            return Ok(None);
        }

        if &buf[data_end..data_end + 2] != b"\r\n" {
            return Err("Protocol error: invalid CRLF in request".to_string());
        }

        if is_small {
            offsets[i] = (data_start, arg_len);
        }

        scan_cursor = data_end + 2;
    }

    if is_small {
        let frame = buf.split_to(scan_cursor).freeze();
        if num_args > 0 {
            let (cmd_start, cmd_len) = offsets[0];
            let cmd_bytes = &frame[cmd_start..cmd_start + cmd_len];
            match cmd_len {
                3 => {
                    if cmd_bytes.eq_ignore_ascii_case(b"GET") && num_args == 2 {
                        let (k_start, k_len) = offsets[1];
                        return Ok(Some(Command::Get(frame.slice(k_start..k_start + k_len))));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"SET") && num_args == 3 {
                        let (k_start, k_len) = offsets[1];
                        let (v_start, v_len) = offsets[2];
                        return Ok(Some(Command::Set {
                            key: frame.slice(k_start..k_start + k_len),
                            value: frame.slice(v_start..v_start + v_len),
                            expire_in: None,
                            condition: SetCondition::None,
                            get: false,
                            keepttl: false,
                            past_expired: false,
                        }));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"DEL") && num_args >= 2 {
                        let (k_start, k_len) = offsets[1];
                        if num_args == 2 {
                            return Ok(Some(Command::Del(smallvec![
                                frame.slice(k_start..k_start + k_len),
                            ])));
                        }
                        let mut keys = SmallVec::with_capacity(num_args - 1);
                        for &(k_s, k_l) in offsets[1..num_args].iter() {
                            keys.push(frame.slice(k_s..k_s + k_l));
                        }
                        return Ok(Some(Command::Del(keys)));
                    }
                }
                4 => {
                    if cmd_bytes.eq_ignore_ascii_case(b"INCR") && num_args == 2 {
                        let (k_start, k_len) = offsets[1];
                        return Ok(Some(Command::IncrBy(
                            frame.slice(k_start..k_start + k_len),
                            1,
                            IncrName::Incr,
                        )));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"HGET") && num_args == 3 {
                        let (k_start, k_len) = offsets[1];
                        let (f_start, f_len) = offsets[2];
                        return Ok(Some(Command::Hget {
                            key: frame.slice(k_start..k_start + k_len),
                            field: frame.slice(f_start..f_start + f_len),
                        }));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"HSET") && num_args == 4 {
                        let (k_start, k_len) = offsets[1];
                        let (f_start, f_len) = offsets[2];
                        let (v_start, v_len) = offsets[3];
                        return Ok(Some(Command::Hset {
                            key: frame.slice(k_start..k_start + k_len),
                            fields: smallvec![(
                                frame.slice(f_start..f_start + f_len),
                                frame.slice(v_start..v_start + v_len),
                            )],
                        }));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"SADD") && num_args == 3 {
                        let (k_start, k_len) = offsets[1];
                        let (m_start, m_len) = offsets[2];
                        return Ok(Some(Command::Sadd {
                            key: frame.slice(k_start..k_start + k_len),
                            members: smallvec![frame.slice(m_start..m_start + m_len)],
                        }));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"LPOP") && num_args == 2 {
                        let (k_start, k_len) = offsets[1];
                        return Ok(Some(Command::Lpop {
                            key: frame.slice(k_start..k_start + k_len),
                            count: None,
                        }));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"ZADD") && num_args == 4 {
                        let (k_start, k_len) = offsets[1];
                        let (s_start, s_len) = offsets[2];
                        let (m_start, m_len) = offsets[3];
                        if let Ok(score_str) = std::str::from_utf8(&frame[s_start..s_start + s_len])
                            && let Some(score) = parse_redis_f64(score_str)
                        {
                            return Ok(Some(Command::Zadd {
                                key: frame.slice(k_start..k_start + k_len),
                                elements: smallvec![(score, frame.slice(m_start..m_start + m_len))],
                                flags: crate::table::ZAddFlags::default(),
                            }));
                        }
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"PING") && num_args == 1 {
                        return Ok(Some(Command::Ping(None)));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"MGET") && num_args >= 2 {
                        let mut keys = Vec::with_capacity(num_args - 1);
                        for &(k_s, k_l) in offsets[1..num_args].iter() {
                            keys.push(frame.slice(k_s..k_s + k_l));
                        }
                        return Ok(Some(Command::Mget(keys)));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"MSET")
                        && num_args >= 3
                        && (num_args - 1).is_multiple_of(2)
                    {
                        let num_pairs = (num_args - 1) / 2;
                        let mut pairs = Vec::with_capacity(num_pairs);
                        for p in 0..num_pairs {
                            let (k_start, k_len) = offsets[1 + p * 2];
                            let (v_start, v_len) = offsets[2 + p * 2];
                            pairs.push((
                                frame.slice(k_start..k_start + k_len),
                                frame.slice(v_start..v_start + v_len),
                            ));
                        }
                        return Ok(Some(Command::Mset(pairs)));
                    }
                }
                5 => {
                    if cmd_bytes.eq_ignore_ascii_case(b"LPUSH") && num_args == 3 {
                        let (k_start, k_len) = offsets[1];
                        let (v_start, v_len) = offsets[2];
                        return Ok(Some(Command::Lpush {
                            key: frame.slice(k_start..k_start + k_len),
                            values: smallvec![frame.slice(v_start..v_start + v_len)],
                        }));
                    }
                }
                6 => {
                    if cmd_bytes.eq_ignore_ascii_case(b"EXISTS") && num_args >= 2 {
                        let (k_start, k_len) = offsets[1];
                        if num_args == 2 {
                            return Ok(Some(Command::Exists(smallvec![
                                frame.slice(k_start..k_start + k_len),
                            ])));
                        }
                        let mut keys = SmallVec::with_capacity(num_args - 1);
                        for &(k_s, k_l) in offsets[1..num_args].iter() {
                            keys.push(frame.slice(k_s..k_s + k_l));
                        }
                        return Ok(Some(Command::Exists(keys)));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"LRANGE")
                        && num_args == 4
                        && let (Some(start), Some(stop)) = (
                            crate::table::RudisTable::parse_i64_bytes(
                                &frame[offsets[2].0..offsets[2].0 + offsets[2].1],
                            ),
                            crate::table::RudisTable::parse_i64_bytes(
                                &frame[offsets[3].0..offsets[3].0 + offsets[3].1],
                            ),
                        )
                    {
                        let (k_start, k_len) = offsets[1];
                        return Ok(Some(Command::Lrange {
                            key: frame.slice(k_start..k_start + k_len),
                            start,
                            stop,
                        }));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"ZRANGE")
                        && num_args == 4
                        && let (Some(start), Some(stop)) = (
                            crate::table::RudisTable::parse_i64_bytes(
                                &frame[offsets[2].0..offsets[2].0 + offsets[2].1],
                            ),
                            crate::table::RudisTable::parse_i64_bytes(
                                &frame[offsets[3].0..offsets[3].0 + offsets[3].1],
                            ),
                        )
                    {
                        let (k_start, k_len) = offsets[1];
                        return Ok(Some(Command::Zrange {
                            key: frame.slice(k_start..k_start + k_len),
                            opts: crate::table::ZRangeOpts {
                                start,
                                stop,
                                ..Default::default()
                            },
                        }));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"UNLINK") && num_args >= 2 {
                        let (k_start, k_len) = offsets[1];
                        if num_args == 2 {
                            return Ok(Some(Command::Unlink(smallvec![
                                frame.slice(k_start..k_start + k_len),
                            ])));
                        }
                        let mut keys = SmallVec::with_capacity(num_args - 1);
                        for &(k_s, k_l) in offsets[1..num_args].iter() {
                            keys.push(frame.slice(k_s..k_s + k_l));
                        }
                        return Ok(Some(Command::Unlink(keys)));
                    }
                }
                8 => {
                    if cmd_bytes.eq_ignore_ascii_case(b"READONLY") && num_args == 1 {
                        return Ok(Some(Command::Readonly));
                    }
                }
                9 => {
                    if cmd_bytes.eq_ignore_ascii_case(b"READWRITE") && num_args == 1 {
                        return Ok(Some(Command::Readwrite));
                    }
                    if cmd_bytes.eq_ignore_ascii_case(b"SISMEMBER") && num_args == 3 {
                        let (k_start, k_len) = offsets[1];
                        let (m_start, m_len) = offsets[2];
                        return Ok(Some(Command::Sismember {
                            key: frame.slice(k_start..k_start + k_len),
                            member: frame.slice(m_start..m_start + m_len),
                        }));
                    }
                }
                _ => {}
            }
        }
        let mut args = Vec::with_capacity(num_args);
        for &(start, len) in offsets.iter().take(num_args) {
            args.push(frame.slice(start..start + len));
        }
        build_command(args)
    } else {
        buf.advance(advance_len); // Consume "*N\r\n"
        let mut args = Vec::with_capacity(num_args);

        for _ in 0..num_args {
            let (header_crlf, next_advance) = find_newline(buf).unwrap();
            let arg_len: usize = parse_decimal_bytes(&buf[1..header_crlf]).unwrap_or(0);

            buf.advance(next_advance); // Consume "$len\r\n"
            let data = buf.split_to(arg_len).freeze(); // Zero-copy slice!
            buf.advance(2); // Consume "\r\n"
            args.push(data);
        }

        build_command(args)
    }
}

fn split_inline_args(line: &[u8]) -> Result<Vec<Bytes>, String> {
    let mut args = Vec::new();
    let mut p = line;
    while !p.is_empty() {
        while !p.is_empty() && (p[0] == b' ' || p[0] == b'\t' || p[0] == b'\r' || p[0] == b'\n') {
            p = &p[1..];
        }
        if p.is_empty() {
            break;
        }
        let in_quote = p[0] == b'"' || p[0] == b'\'';
        if in_quote {
            let quote_char = p[0];
            p = &p[1..];
            let mut arg = Vec::new();
            let mut closed = false;
            while !p.is_empty() {
                if p[0] == b'\\' {
                    if p.len() > 1 {
                        if quote_char == b'"' {
                            match p[1] {
                                b'n' => arg.push(b'\n'),
                                b'r' => arg.push(b'\r'),
                                b't' => arg.push(b'\t'),
                                b'b' => arg.push(b'\x08'),
                                b'a' => arg.push(b'\x07'),
                                b'x' if p.len() > 3 => {
                                    if let Ok(b) = u8::from_str_radix(
                                        std::str::from_utf8(&p[2..4]).unwrap_or(""),
                                        16,
                                    ) {
                                        arg.push(b);
                                        p = &p[4..];
                                        continue;
                                    }
                                }
                                other => arg.push(other),
                            }
                        } else {
                            arg.push(p[1]);
                        }
                        p = &p[2..];
                        continue;
                    } else {
                        return Err("Protocol error: unbalanced quotes in request".to_string());
                    }
                } else if p[0] == quote_char {
                    p = &p[1..];
                    closed = true;
                    break;
                } else {
                    arg.push(p[0]);
                    p = &p[1..];
                }
            }
            if !closed {
                return Err("Protocol error: unbalanced quotes in request".to_string());
            }
            if !p.is_empty() && p[0] != b' ' && p[0] != b'\t' && p[0] != b'\r' && p[0] != b'\n' {
                return Err("Protocol error: unbalanced quotes in request".to_string());
            }
            args.push(Bytes::from(arg));
        } else {
            let mut arg = Vec::new();
            while !p.is_empty() && p[0] != b' ' && p[0] != b'\t' && p[0] != b'\r' && p[0] != b'\n' {
                if p[0] == b'"' || p[0] == b'\'' {
                    return Err("Protocol error: unbalanced quotes in request".to_string());
                }
                arg.push(p[0]);
                p = &p[1..];
            }
            args.push(Bytes::from(arg));
        }
    }
    Ok(args)
}

/// Sets `skipped` instead of returning when the line is blank.
fn parse_inline_command(buf: &mut BytesMut, skipped: &mut bool) -> Result<Option<Command>, String> {
    let (line_end, advance_len) = match find_newline(buf) {
        Some(res) => res,
        None => {
            if buf.len() > 64 * 1024 {
                return Err("Protocol error: too big inline request".to_string());
            }
            return Ok(None);
        }
    };

    let line = &buf[..line_end];
    let parts = split_inline_args(line)?;
    buf.advance(advance_len);

    if parts.is_empty() {
        *skipped = true;
        return Ok(None);
    }

    build_command(parts)
}

/// Returns the `count` arguments starting at `start` for a RediSearch-style
/// `<KEYWORD> <count> arg...` option. The count is client-supplied, so it is
/// validated against the arguments actually present (never looped over
/// blindly); `start` may already be past the end of `args`.
fn counted_args<'a>(
    args: &'a [Bytes],
    start: usize,
    count: usize,
    keyword: &str,
) -> Result<&'a [Bytes], String> {
    if count > args.len().saturating_sub(start) {
        return Err(format!(
            "Bad arguments for {keyword}: Expected an argument, but none provided"
        ));
    }
    Ok(args.get(start..start + count).unwrap_or_default())
}

/// Field name with an optional leading `@` removed (FT.AGGREGATE syntax).
fn strip_field_sigil(arg: &Bytes) -> String {
    let f = String::from_utf8_lossy(arg);
    f.strip_prefix('@').unwrap_or(&f).to_string()
}

/// Parses the name/value pairs of an FT.* `PARAMS <count> name value ...`
/// option starting at `start` (an odd count ignores the trailing argument).
/// Returns how many arguments were consumed.
fn parse_ft_params(
    args: &[Bytes],
    start: usize,
    count: usize,
    params: &mut std::collections::HashMap<String, Vec<u8>>,
) -> Result<usize, String> {
    let pairs = counted_args(args, start, count / 2 * 2, "PARAMS")?;
    for [k, v] in pairs.as_chunks::<2>().0 {
        params.insert(String::from_utf8_lossy(k).to_string(), v.to_vec());
    }
    Ok(pairs.len())
}

fn parse_lmovem_trailer(args: &[Bytes]) -> Result<(LmovemMode, usize, LmovemOrdering), String> {
    if args.is_empty() {
        return Ok((LmovemMode::Count, 1, LmovemOrdering::Bulk));
    }
    if args.len() != 3 {
        return Err("syntax error".to_string());
    }
    let mode_str = String::from_utf8_lossy(&args[0]).to_uppercase();
    let mode = match mode_str.as_str() {
        "COUNT" => LmovemMode::Count,
        "EXACTLY" => LmovemMode::Exactly,
        _ => return Err("syntax error".to_string()),
    };
    let count_str =
        std::str::from_utf8(&args[1]).map_err(|_| "count should be greater than 0".to_string())?;
    let count_num: i64 = count_str
        .parse()
        .map_err(|_| "count should be greater than 0".to_string())?;
    if count_num <= 0 {
        return Err("count should be greater than 0".to_string());
    }
    let order_str = String::from_utf8_lossy(&args[2]).to_uppercase();
    let ordering = match order_str.as_str() {
        "OBO" => LmovemOrdering::Obo,
        "BULK" => LmovemOrdering::Bulk,
        _ => return Err("syntax error".to_string()),
    };
    Ok((mode, count_num as usize, ordering))
}

// On in this crate's unit tests, which exercise every parser.
static EXPERIMENTAL_COMMANDS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(cfg!(test));

/// Enables the experimental command families (`enable-experimental-commands`).
pub fn set_experimental_commands(enabled: bool) {
    EXPERIMENTAL_COMMANDS.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub fn experimental_commands_enabled() -> bool {
    EXPERIMENTAL_COMMANDS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Command families that are off unless `enable-experimental-commands` is
/// set. They are not Redis/Valkey commands, and apart from JSON.,
/// SEMANTIC., BF., CF., CMS., TOPK. and CRDT. most of their writes are
/// neither written to the AOF nor replicated, so a restart or failover silently
/// loses that data and replicas never see it. MCP./XDP. also expose tool
/// calls and packet-filter control.
const EXPERIMENTAL_PREFIXES: &[&str] = &[
    "JSON.",
    "BF.",
    "CF.",
    "CMS.",
    "TOPK.",
    "FT.",
    "SEMANTIC.",
    "CRDT.",
    "LLM.",
    "MCP.",
    "XDP.",
];

/// `name` must be upper case.
pub fn is_experimental_command_name(name: &str) -> bool {
    EXPERIMENTAL_PREFIXES.iter().any(|p| name.starts_with(p))
}

/// Most elements a negative (repeating) `SRANDMEMBER`/`HRANDFIELD`/`ZRANDMEMBER`/
/// `VRANDMEMBER` count may ask for. Redis streams such replies; rudis builds them in
/// memory first, so a count near -i64::MAX hung the shard or exhausted memory.
pub const MAX_RANDOM_REPEATS: i64 = 1 << 20;

/// Parses the `count` of the `*RANDMEMBER`/`HRANDFIELD` family.
fn parse_random_count(arg: &[u8]) -> Result<i64, String> {
    let c: i64 = std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
    if c < -MAX_RANDOM_REPEATS {
        return Err("value is out of range".to_string());
    }
    Ok(c)
}

pub fn build_command(mut args: Vec<Bytes>) -> Result<Option<Command>, String> {
    if args.is_empty() {
        return Ok(None);
    }

    let mut cmd_buf = [0u8; 64];
    let mut cmd_heap = String::new();
    let cmd_name = bytes_to_uppercase_ascii(&args[0], &mut cmd_buf, &mut cmd_heap);

    // No core command has a '.', so this costs one byte scan on the hot path.
    if cmd_name.as_bytes().contains(&b'.')
        && !experimental_commands_enabled()
        && is_experimental_command_name(cmd_name)
    {
        return Ok(Some(Command::Unknown(
            String::from_utf8_lossy(&args[0]).into_owned(),
        )));
    }

    match cmd_name {
        "AUTH" => {
            if args.len() == 2 {
                let password = String::from_utf8_lossy(&args[1]).to_string();
                Ok(Some(Command::Auth {
                    username: None,
                    password,
                }))
            } else if args.len() == 3 {
                let username = String::from_utf8_lossy(&args[1]).to_string();
                let password = String::from_utf8_lossy(&args[2]).to_string();
                Ok(Some(Command::Auth {
                    username: Some(username),
                    password,
                }))
            } else {
                Err("wrong number of arguments for 'auth' command".to_string())
            }
        }
        "ACL" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'acl' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "LIST" => Ok(Some(Command::Acl(AclSubcommand::List))),
                "USERS" => Ok(Some(Command::Acl(AclSubcommand::Users))),
                "WHOAMI" => Ok(Some(Command::Acl(AclSubcommand::WhoAmI))),
                "CAT" => Ok(Some(Command::Acl(AclSubcommand::Cat))),
                "GETUSER" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'acl|getuser' command".to_string()
                        );
                    }
                    let username = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Acl(AclSubcommand::GetUser(username))))
                }
                "SETUSER" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'acl|setuser' command".to_string()
                        );
                    }
                    let username = String::from_utf8_lossy(&args[2]).to_string();
                    let rules = args[3..]
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).to_string())
                        .collect();
                    Ok(Some(Command::Acl(AclSubcommand::SetUser {
                        username,
                        rules,
                    })))
                }
                "DELUSER" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'acl|deluser' command".to_string()
                        );
                    }
                    let usernames = args[2..]
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).to_string())
                        .collect();
                    Ok(Some(Command::Acl(AclSubcommand::DelUser(usernames))))
                }
                _ => Err(format!("unknown subcommand '{}' for 'acl'", sub)),
            }
        }
        "GET" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'get' command".to_string());
            }
            Ok(Some(Command::Get(args[1].clone())))
        }
        "GETEX" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'getex' command".to_string());
            }
            let key = args[1].clone();
            let mut expire_in = None;
            let mut persist = false;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "EX" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let sec: i64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        if !((i64::MIN / 1000)..=(i64::MAX / 1000)).contains(&sec)
                            || (sec > 0 && (sec as i128 * 1000) > (i64::MAX - now_ms) as i128)
                        {
                            return Err("invalid expire time in 'getex' command".to_string());
                        }
                        expire_in = Some(if sec <= 0 {
                            Duration::ZERO
                        } else {
                            Duration::from_secs(sec as u64)
                        });
                        i += 2;
                    }
                    "PX" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ms: i64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        if ms > 0 && ms > i64::MAX - now_ms {
                            return Err("invalid expire time in 'getex' command".to_string());
                        }
                        expire_in = Some(if ms <= 0 {
                            Duration::ZERO
                        } else {
                            Duration::from_millis(ms as u64)
                        });
                        i += 2;
                    }
                    "EXAT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ts: i64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        if !((i64::MIN / 1000)..=(i64::MAX / 1000)).contains(&ts) {
                            return Err("invalid expire time in 'getex' command".to_string());
                        }
                        let now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        let dur = if ts <= now_unix {
                            Duration::ZERO
                        } else {
                            Duration::from_secs((ts - now_unix) as u64)
                        };
                        expire_in = Some(dur);
                        i += 2;
                    }
                    "PXAT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ts_ms: i64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        let now_unix_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        let dur = if ts_ms <= now_unix_ms {
                            Duration::ZERO
                        } else {
                            Duration::from_millis((ts_ms - now_unix_ms) as u64)
                        };
                        expire_in = Some(dur);
                        i += 2;
                    }
                    "PERSIST" => {
                        persist = true;
                        i += 1;
                    }
                    _ => {
                        return Err("syntax error".to_string());
                    }
                }
            }
            Ok(Some(Command::Getex {
                key,
                expire_in,
                persist,
            }))
        }
        "SET" | "PUT" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'set' command".to_string());
            }
            let mut expire_in = None;
            let mut condition = SetCondition::None;
            let mut get = false;
            let mut keepttl = false;
            let mut past_expired = false;
            let mut has_expiry = false;

            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "NX" => {
                        if condition != SetCondition::None {
                            return Err("syntax error".to_string());
                        }
                        condition = SetCondition::Nx;
                        i += 1;
                    }
                    "XX" => {
                        if condition != SetCondition::None {
                            return Err("syntax error".to_string());
                        }
                        condition = SetCondition::Xx;
                        i += 1;
                    }
                    "IFEQ" => {
                        if condition != SetCondition::None || i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        condition = SetCondition::Ifeq(args[i + 1].clone());
                        i += 2;
                    }
                    "IFNE" => {
                        if condition != SetCondition::None || i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        condition = SetCondition::Ifne(args[i + 1].clone());
                        i += 2;
                    }
                    "IFDEQ" => {
                        if condition != SetCondition::None || i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        condition = SetCondition::Ifdeq(args[i + 1].clone());
                        i += 2;
                    }
                    "IFDNE" => {
                        if condition != SetCondition::None || i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        condition = SetCondition::Ifdne(args[i + 1].clone());
                        i += 2;
                    }
                    "GET" => {
                        get = true;
                        i += 1;
                    }
                    "KEEPTTL" => {
                        if has_expiry || keepttl {
                            return Err("syntax error".to_string());
                        }
                        keepttl = true;
                        i += 1;
                    }
                    "EX" => {
                        if has_expiry || keepttl || i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let secs_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let secs: i64 = secs_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        if secs <= 0
                            || secs > (i64::MAX / 1000)
                            || (secs as i128 * 1000) > (i64::MAX - now_ms) as i128
                        {
                            return Err("invalid expire time in 'set' command".to_string());
                        }
                        expire_in = Some(Duration::from_secs(secs as u64));
                        has_expiry = true;
                        i += 2;
                    }
                    "PX" => {
                        if has_expiry || keepttl || i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ms_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let ms: i64 = ms_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if ms <= 0 {
                            return Err("invalid expire time in 'set' command".to_string());
                        }
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        if ms.checked_add(now_ms).is_none() {
                            return Err("invalid expire time in 'set' command".to_string());
                        }
                        expire_in = Some(Duration::from_millis(ms as u64));
                        has_expiry = true;
                        i += 2;
                    }
                    "EXAT" => {
                        if has_expiry || keepttl || i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ts_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let ts: i64 = ts_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if ts <= 0 || ts > (i64::MAX / 1000) {
                            return Err("invalid expire time in 'set' command".to_string());
                        }
                        let now_sec = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64;
                        if ts > now_sec {
                            expire_in = Some(Duration::from_secs((ts - now_sec) as u64));
                        } else {
                            past_expired = true;
                        }
                        has_expiry = true;
                        i += 2;
                    }
                    "PXAT" => {
                        if has_expiry || keepttl || i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ts_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let ts: i128 = ts_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if ts <= 0 || ts > (i64::MAX as i128) {
                            return Err("invalid expire time in 'set' command".to_string());
                        }
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i128;
                        if ts > now_ms {
                            expire_in = Some(Duration::from_millis((ts - now_ms) as u64));
                        } else {
                            past_expired = true;
                        }
                        has_expiry = true;
                        i += 2;
                    }
                    _ => {
                        return Err("syntax error".to_string());
                    }
                }
            }
            Ok(Some(Command::Set {
                key: args[1].clone(),
                value: args[2].clone(),
                expire_in,
                condition,
                get,
                keepttl,
                past_expired,
            }))
        }
        "MGET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'mget' command".to_string());
            }
            args.remove(0);
            Ok(Some(Command::Mget(args)))
        }
        "MSET" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return Err("wrong number of arguments for 'mset' command".to_string());
            }
            let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
            let mut i = 1;
            while i < args.len() {
                pairs.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            }
            Ok(Some(Command::Mset(pairs)))
        }
        "MSETEX" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'msetex' command".to_string());
            }
            let numkeys_str =
                std::str::from_utf8(&args[1]).map_err(|_| "invalid numkeys value".to_string())?;
            let numkeys: i64 = numkeys_str
                .parse()
                .map_err(|_| "invalid numkeys value".to_string())?;
            if numkeys < 0 || numkeys > i32::MAX as i64 {
                return Err("invalid numkeys value".to_string());
            }
            let numkeys = numkeys as usize;

            let mut pairs = Vec::with_capacity(numkeys.min(args.len() / 2));
            let mut condition = MsetexCondition::None;
            let mut expiry = MsetexExpiry::None;
            let mut idx = 2;
            let mut keys_found = 0;

            while idx < args.len() {
                let opt = std::str::from_utf8(&args[idx]).unwrap_or("").to_uppercase();
                match opt.as_str() {
                    "NX" => {
                        if condition != MsetexCondition::None {
                            return Err("syntax error".to_string());
                        }
                        condition = MsetexCondition::Nx;
                        idx += 1;
                    }
                    "XX" => {
                        if condition != MsetexCondition::None {
                            return Err("syntax error".to_string());
                        }
                        condition = MsetexCondition::Xx;
                        idx += 1;
                    }
                    "KEEPTTL" => {
                        if expiry != MsetexExpiry::None {
                            return Err("syntax error".to_string());
                        }
                        expiry = MsetexExpiry::KeepTtl;
                        idx += 1;
                    }
                    "EX" => {
                        if expiry != MsetexExpiry::None || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let sec_str = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "syntax error".to_string())?;
                        let sec: u64 = sec_str.parse().map_err(|_| "syntax error".to_string())?;
                        expiry = MsetexExpiry::ExpireIn(Duration::from_secs(sec));
                        idx += 2;
                    }
                    "PX" => {
                        if expiry != MsetexExpiry::None || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ms_str = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "syntax error".to_string())?;
                        let ms: u64 = ms_str.parse().map_err(|_| "syntax error".to_string())?;
                        expiry = MsetexExpiry::ExpireIn(Duration::from_millis(ms));
                        idx += 2;
                    }
                    "EXAT" => {
                        if expiry != MsetexExpiry::None || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ts_str = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "syntax error".to_string())?;
                        let ts: u64 = ts_str.parse().map_err(|_| "syntax error".to_string())?;
                        let now_epoch = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let dur = if ts > now_epoch {
                            Duration::from_secs(ts - now_epoch)
                        } else {
                            Duration::from_millis(1)
                        };
                        expiry = MsetexExpiry::ExpireIn(dur);
                        idx += 2;
                    }
                    "PXAT" => {
                        if expiry != MsetexExpiry::None || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ts_str = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "syntax error".to_string())?;
                        let ts: u128 = ts_str.parse().map_err(|_| "syntax error".to_string())?;
                        let now_epoch = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis();
                        let dur = if ts > now_epoch {
                            Duration::from_millis((ts - now_epoch) as u64)
                        } else {
                            Duration::from_millis(1)
                        };
                        expiry = MsetexExpiry::ExpireIn(dur);
                        idx += 2;
                    }
                    _ => {
                        if keys_found < numkeys {
                            if idx + 1 >= args.len() {
                                return Err("wrong number of key-value pairs".to_string());
                            }
                            pairs.push((args[idx].clone(), args[idx + 1].clone()));
                            keys_found += 1;
                            idx += 2;
                        } else {
                            return Err("syntax error".to_string());
                        }
                    }
                }
            }

            if keys_found != numkeys {
                return Err("wrong number of key-value pairs".to_string());
            }

            Ok(Some(Command::Msetex {
                pairs,
                condition,
                expiry,
            }))
        }
        "LCS" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'lcs' command".to_string());
            }
            let key1 = args[1].clone();
            let key2 = args[2].clone();
            let mut len_only = false;
            let mut idx = false;
            let mut min_match_len = 0;
            let mut with_match_len = false;

            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "LEN" => {
                        len_only = true;
                        i += 1;
                    }
                    "IDX" => {
                        idx = true;
                        i += 1;
                    }
                    "WITHMATCHLEN" => {
                        with_match_len = true;
                        i += 1;
                    }
                    "MINMATCHLEN" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let m_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let m: i64 = m_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if m < 0 {
                            return Err("value is not an integer or out of range".to_string());
                        }
                        min_match_len = m as usize;
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }

            if len_only && idx {
                return Err(
                    "If you want both the length and indexes, please just use IDX.".to_string(),
                );
            }

            Ok(Some(Command::Lcs {
                key1,
                key2,
                len_only,
                idx,
                min_match_len,
                with_match_len,
            }))
        }
        "DEL" | "DELETE" | "UNLINK" => {
            if args.len() < 2 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            if cmd_name == "DELETE" {
                let noreply = args.len() > 2 && args[2].eq_ignore_ascii_case(b"noreply");
                Ok(Some(Command::MemcachedDelete {
                    key: args[1].clone(),
                    noreply,
                }))
            } else if cmd_name == "UNLINK" {
                args.remove(0);
                Ok(Some(Command::Unlink(SmallVec::from_vec(args))))
            } else {
                args.remove(0);
                Ok(Some(Command::Del(SmallVec::from_vec(args))))
            }
        }
        "READONLY" => Ok(Some(Command::Readonly)),
        "READWRITE" => Ok(Some(Command::Readwrite)),
        "WAIT" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'wait' command".to_string());
            }
            let numreplicas: usize = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let timeout: u64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Wait {
                numreplicas,
                timeout,
            }))
        }
        "WAITAOF" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'waitaof' command".to_string());
            }
            let numlocal: usize = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let numreplicas: usize = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let timeout: u64 = std::str::from_utf8(&args[3])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::WaitAof {
                numlocal,
                numreplicas,
                timeout,
            }))
        }
        "OBJECT" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'object' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "ENCODING" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'object encoding' command".to_string()
                        );
                    }
                    Ok(Some(Command::Object(ObjectSubcommand::Encoding(
                        args[2].clone(),
                    ))))
                }
                "FREQ" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'object freq' command".to_string()
                        );
                    }
                    Ok(Some(Command::Object(ObjectSubcommand::Freq(
                        args[2].clone(),
                    ))))
                }
                "IDLETIME" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'object idletime' command".to_string()
                        );
                    }
                    Ok(Some(Command::Object(ObjectSubcommand::Idletime(
                        args[2].clone(),
                    ))))
                }
                "REFCOUNT" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'object refcount' command".to_string()
                        );
                    }
                    Ok(Some(Command::Object(ObjectSubcommand::Refcount(
                        args[2].clone(),
                    ))))
                }
                "HELP" => Ok(Some(Command::Object(ObjectSubcommand::Help))),
                _ => Ok(Some(Command::Unknown(format!("OBJECT {}", sub)))),
            }
        }
        "EXISTS" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'exists' command".to_string());
            }
            args.remove(0);
            Ok(Some(Command::Exists(SmallVec::from_vec(args))))
        }
        "INCR" => {
            if args.len() == 2 {
                return Ok(Some(Command::IncrBy(args[1].clone(), 1, IncrName::Incr)));
            }
            if (args.len() == 3 || args.len() == 4)
                && let Some(val) = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
            {
                let noreply = args.len() > 3 && args[3].eq_ignore_ascii_case(b"noreply");
                return Ok(Some(Command::MemcachedIncr {
                    key: args[1].clone(),
                    value: val,
                    noreply,
                }));
            }
            Err("wrong number of arguments for 'incr' command".to_string())
        }
        "DECR" => {
            if args.len() == 2 {
                return Ok(Some(Command::IncrBy(args[1].clone(), -1, IncrName::Decr)));
            }
            if (args.len() == 3 || args.len() == 4)
                && let Some(val) = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
            {
                let noreply = args.len() > 3 && args[3].eq_ignore_ascii_case(b"noreply");
                return Ok(Some(Command::MemcachedDecr {
                    key: args[1].clone(),
                    value: val,
                    noreply,
                }));
            }
            Err("wrong number of arguments for 'decr' command".to_string())
        }
        "INCRBY" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'incrby' command".to_string());
            }
            let delta = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::IncrBy(
                args[1].clone(),
                delta,
                IncrName::IncrBy,
            )))
        }
        "DECRBY" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'decrby' command".to_string());
            }
            let delta = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if delta == i64::MIN {
                return Err("increment or decrement would overflow".to_string());
            }
            Ok(Some(Command::IncrBy(
                args[1].clone(),
                -delta,
                IncrName::DecrBy,
            )))
        }
        "EXPIRE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'expire' command".to_string());
            }
            let secs: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            if !((i64::MIN / 1000)..=(i64::MAX / 1000)).contains(&secs)
                || (secs > 0 && (secs as i128 * 1000) > (i64::MAX - now_ms) as i128)
            {
                return Err("invalid expire time in 'expire' command".to_string());
            }
            let opts = parse_expire_options(&args[3..])?;
            let dur = if secs <= 0 {
                Duration::ZERO
            } else {
                Duration::from_secs(secs as u64)
            };
            Ok(Some(Command::Expire {
                key: args[1].clone(),
                duration: dur,
                opts,
            }))
        }
        "PEXPIRE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'pexpire' command".to_string());
            }
            let ms: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            if ms > 0 && ms > i64::MAX - now_ms {
                return Err("invalid expire time in 'pexpire' command".to_string());
            }
            let opts = parse_expire_options(&args[3..])?;
            let dur = if ms <= 0 {
                Duration::ZERO
            } else {
                Duration::from_millis(ms as u64)
            };
            Ok(Some(Command::Expire {
                key: args[1].clone(),
                duration: dur,
                opts,
            }))
        }
        "PERSIST" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'persist' command".to_string());
            }
            Ok(Some(Command::Persist(args[1].clone())))
        }
        "TTL" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'ttl' command".to_string());
            }
            Ok(Some(Command::Ttl(args[1].clone(), false)))
        }
        "PTTL" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'pttl' command".to_string());
            }
            Ok(Some(Command::Ttl(args[1].clone(), true)))
        }
        "PING" => {
            if args.len() > 2 {
                return Err("wrong number of arguments for 'ping' command".to_string());
            }
            let msg = if args.len() == 2 {
                Some(args[1].clone())
            } else {
                None
            };
            Ok(Some(Command::Ping(msg)))
        }
        "CLUSTER" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'cluster' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "KEYSLOT" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'cluster keyslot' command".to_string()
                        );
                    }
                    Ok(Some(Command::Cluster(ClusterSubcommand::KeySlot(
                        args[2].clone(),
                    ))))
                }
                "COUNTKEYSINSLOT" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'cluster countkeysinslot' command"
                                .to_string(),
                        );
                    }
                    let slot: u16 = std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Cluster(ClusterSubcommand::CountKeysInSlot(
                        slot,
                    ))))
                }
                "GETKEYSINSLOT" => {
                    if args.len() < 4 {
                        return Err(
                            "wrong number of arguments for 'cluster getkeysinslot' command"
                                .to_string(),
                        );
                    }
                    let slot: u16 = std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    let count: usize = std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Cluster(ClusterSubcommand::GetKeysInSlot(
                        slot, count,
                    ))))
                }
                "SETSLOT" => {
                    if args.len() < 4 {
                        return Err(
                            "wrong number of arguments for 'cluster setslot' command".to_string()
                        );
                    }
                    let slot: u16 = std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    let action = String::from_utf8_lossy(&args[3]).to_uppercase();
                    match action.as_str() {
                        "MIGRATING" => {
                            if args.len() < 5 {
                                return Err("wrong number of arguments for 'cluster setslot migrating' command".to_string());
                            }
                            let node = String::from_utf8_lossy(&args[4]).to_string();
                            Ok(Some(Command::Cluster(ClusterSubcommand::SetSlot(
                                slot,
                                SetSlotSubcommand::Migrating(node),
                            ))))
                        }
                        "IMPORTING" => {
                            if args.len() < 5 {
                                return Err("wrong number of arguments for 'cluster setslot importing' command".to_string());
                            }
                            let node = String::from_utf8_lossy(&args[4]).to_string();
                            Ok(Some(Command::Cluster(ClusterSubcommand::SetSlot(
                                slot,
                                SetSlotSubcommand::Importing(node),
                            ))))
                        }
                        "STABLE" => Ok(Some(Command::Cluster(ClusterSubcommand::SetSlot(
                            slot,
                            SetSlotSubcommand::Stable,
                        )))),
                        "NODE" => {
                            if args.len() < 5 {
                                return Err(
                                    "wrong number of arguments for 'cluster setslot node' command"
                                        .to_string(),
                                );
                            }
                            let node = String::from_utf8_lossy(&args[4]).to_string();
                            Ok(Some(Command::Cluster(ClusterSubcommand::SetSlot(
                                slot,
                                SetSlotSubcommand::Node(node),
                            ))))
                        }
                        _ => Ok(Some(Command::Unknown(format!(
                            "CLUSTER SETSLOT {}",
                            action
                        )))),
                    }
                }
                "SLOTS" => Ok(Some(Command::Cluster(ClusterSubcommand::Slots))),
                "SHARDS" => Ok(Some(Command::Cluster(ClusterSubcommand::Shards))),
                "LINKS" => Ok(Some(Command::Cluster(ClusterSubcommand::Links))),
                "ADDSLOTS" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'cluster addslots' command".to_string()
                        );
                    }
                    let mut slots = Vec::with_capacity(args.len() - 2);
                    for a in &args[2..] {
                        let s: u16 = std::str::from_utf8(a)
                            .ok()
                            .and_then(|val| val.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        slots.push(s);
                    }
                    Ok(Some(Command::Cluster(ClusterSubcommand::AddSlots(slots))))
                }
                "DELSLOTS" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'cluster delslots' command".to_string()
                        );
                    }
                    let mut slots = Vec::with_capacity(args.len() - 2);
                    for a in &args[2..] {
                        let s: u16 = std::str::from_utf8(a)
                            .ok()
                            .and_then(|val| val.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        slots.push(s);
                    }
                    Ok(Some(Command::Cluster(ClusterSubcommand::DelSlots(slots))))
                }
                "ADDSLOTSRANGE" | "ADDSLOTS-RANGE" => {
                    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                        return Err(
                            "wrong number of arguments for 'cluster addslotsrange' command"
                                .to_string(),
                        );
                    }
                    let mut ranges = Vec::new();
                    for chunk in args[2..].chunks(2) {
                        let start: u16 = std::str::from_utf8(&chunk[0])
                            .ok()
                            .and_then(|val| val.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        let end: u16 = std::str::from_utf8(&chunk[1])
                            .ok()
                            .and_then(|val| val.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        ranges.push((start, end));
                    }
                    Ok(Some(Command::Cluster(ClusterSubcommand::AddSlotsRange(
                        ranges,
                    ))))
                }
                "DELSLOTSRANGE" | "DELSLOTS-RANGE" => {
                    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                        return Err(
                            "wrong number of arguments for 'cluster delslotsrange' command"
                                .to_string(),
                        );
                    }
                    let mut ranges = Vec::new();
                    for chunk in args[2..].chunks(2) {
                        let start: u16 = std::str::from_utf8(&chunk[0])
                            .ok()
                            .and_then(|val| val.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        let end: u16 = std::str::from_utf8(&chunk[1])
                            .ok()
                            .and_then(|val| val.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        ranges.push((start, end));
                    }
                    Ok(Some(Command::Cluster(ClusterSubcommand::DelSlotsRange(
                        ranges,
                    ))))
                }
                "NODES" => Ok(Some(Command::Cluster(ClusterSubcommand::Nodes))),
                "INFO" => Ok(Some(Command::Cluster(ClusterSubcommand::Info))),
                "MYID" => Ok(Some(Command::Cluster(ClusterSubcommand::MyId))),
                "MEET" => {
                    if args.len() < 4 {
                        return Err(
                            "wrong number of arguments for 'cluster meet' command".to_string()
                        );
                    }
                    let ip = String::from_utf8_lossy(&args[2]).to_string();
                    let port: u16 = std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Cluster(ClusterSubcommand::Meet { ip, port })))
                }
                "MIGRATE-SLOT" | "MIGRATESLOT" => {
                    if args.len() < 5 {
                        return Err(
                            "wrong number of arguments for 'cluster migrate-slot' command"
                                .to_string(),
                        );
                    }
                    let slot: u16 = std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    let host = String::from_utf8_lossy(&args[3]).to_string();
                    let port: u16 = std::str::from_utf8(&args[4])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Cluster(ClusterSubcommand::MigrateSlot {
                        slot,
                        host,
                        port,
                    })))
                }
                "REBALANCE" => {
                    let mut host = None;
                    let mut port = None;
                    let mut slots = None;
                    let mut weights = Vec::new();
                    let mut simulate = false;
                    let mut threshold = 1.25;
                    let mut pipeline = 16;

                    let mut i = 2;
                    while i < args.len() {
                        let token = String::from_utf8_lossy(&args[i]).to_uppercase();
                        if token == "SIMULATE" {
                            simulate = true;
                            i += 1;
                        } else if token == "THRESHOLD" && i + 1 < args.len() {
                            threshold = String::from_utf8_lossy(&args[i + 1])
                                .parse()
                                .unwrap_or(1.25);
                            i += 2;
                        } else if token == "PIPELINE" && i + 1 < args.len() {
                            pipeline = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(16);
                            i += 2;
                        } else if token == "WEIGHTS" {
                            i += 1;
                            while i < args.len() {
                                let w_str = String::from_utf8_lossy(&args[i]);
                                if let Some((node, w_val)) = w_str.split_once('=') {
                                    if let Ok(w) = w_val.parse::<f64>() {
                                        weights.push((node.to_string(), w));
                                    }
                                    i += 1;
                                } else {
                                    break;
                                }
                            }
                        } else if host.is_none() && i + 1 < args.len() && !token.starts_with('-') {
                            host = Some(String::from_utf8_lossy(&args[i]).to_string());
                            port = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                            i += 2;
                            if i < args.len()
                                && let Ok(s) = String::from_utf8_lossy(&args[i]).parse::<usize>()
                            {
                                slots = Some(s);
                                i += 1;
                            }
                        } else {
                            i += 1;
                        }
                    }
                    Ok(Some(Command::Cluster(ClusterSubcommand::Rebalance {
                        host,
                        port,
                        slots,
                        weights,
                        simulate,
                        threshold,
                        pipeline,
                    })))
                }
                "CHECK" => Ok(Some(Command::Cluster(ClusterSubcommand::Check))),
                "RESHARD" => {
                    if args.len() < 5 {
                        return Err(
                            "wrong number of arguments for 'cluster reshard' command".to_string()
                        );
                    }
                    let target_node_id = String::from_utf8_lossy(&args[2]).to_string();
                    let source_node_id = String::from_utf8_lossy(&args[3]).to_string();
                    let slots: usize = std::str::from_utf8(&args[4])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Cluster(ClusterSubcommand::Reshard {
                        target_node_id,
                        source_node_id,
                        slots,
                    })))
                }
                "FAILOVER" => {
                    let force = args.len() > 2 && args[2].eq_ignore_ascii_case(b"force");
                    Ok(Some(Command::Cluster(ClusterSubcommand::Failover {
                        force,
                    })))
                }
                "RESET" => {
                    let hard = args.len() > 2 && args[2].eq_ignore_ascii_case(b"hard");
                    Ok(Some(Command::Cluster(ClusterSubcommand::Reset { hard })))
                }
                "FORGET" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'cluster forget' command".to_string()
                        );
                    }
                    let node_id = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Cluster(ClusterSubcommand::Forget(node_id))))
                }
                "REPLICATE" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'cluster replicate' command".to_string()
                        );
                    }
                    let node_id = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Cluster(ClusterSubcommand::Replicate(
                        node_id,
                    ))))
                }
                "SAVECONFIG" => Ok(Some(Command::Cluster(ClusterSubcommand::SaveConfig))),
                "BUMPEPOCH" => Ok(Some(Command::Cluster(ClusterSubcommand::BumpEpoch))),
                "SET-CONFIG-EPOCH" | "SETCONFIGEPOCH" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'cluster set-config-epoch' command"
                                .to_string(),
                        );
                    }
                    let epoch: u64 = std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Cluster(ClusterSubcommand::SetConfigEpoch(
                        epoch,
                    ))))
                }
                _ => Ok(Some(Command::Unknown(format!("CLUSTER {}", sub)))),
            }
        }
        "ASKING" => Ok(Some(Command::Asking)),
        "MIGRATE" => {
            if args.len() < 6 {
                return Err("wrong number of arguments for 'migrate' command".to_string());
            }
            let host = String::from_utf8_lossy(&args[1]).to_string();
            let port: u16 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let key = if args[3].is_empty() {
                None
            } else {
                Some(args[3].clone())
            };
            let destination_db: u32 = std::str::from_utf8(&args[4])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let timeout_ms: u64 = std::str::from_utf8(&args[5])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;

            let mut copy = false;
            let mut replace = false;
            let mut keys = Vec::new();
            let mut i = 6;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "COPY" => {
                        copy = true;
                        i += 1;
                    }
                    "REPLACE" => {
                        replace = true;
                        i += 1;
                    }
                    "KEYS" => {
                        i += 1;
                        while i < args.len() {
                            keys.push(args[i].clone());
                            i += 1;
                        }
                    }
                    _ => {
                        i += 1;
                    }
                }
            }
            Ok(Some(Command::Migrate {
                host,
                port,
                key,
                keys,
                destination_db,
                timeout_ms,
                copy,
                replace,
            }))
        }
        "CLIENT" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'client' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "LIST" => {
                    let mut ids = Vec::new();
                    let mut i = 2;
                    while i < args.len() {
                        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                        if opt == "ID" {
                            i += 1;
                            while i < args.len() {
                                if let Ok(id) =
                                    std::str::from_utf8(&args[i]).unwrap_or("").parse::<u64>()
                                {
                                    ids.push(id);
                                    i += 1;
                                } else {
                                    break;
                                }
                            }
                        } else if opt == "TYPE" {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    Ok(Some(Command::Client(ClientSubcommand::List(ids))))
                }
                "INFO" => Ok(Some(Command::Client(ClientSubcommand::Info))),
                "SETNAME" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'client|setname' command".to_string()
                        );
                    }
                    let name = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Client(ClientSubcommand::SetName(name))))
                }
                "SETINFO" => {
                    if args.len() != 4 {
                        return Err(
                            "wrong number of arguments for 'client|setinfo' command".to_string()
                        );
                    }
                    let attr = String::from_utf8_lossy(&args[2]).to_string();
                    let val = String::from_utf8_lossy(&args[3]).to_string();
                    Ok(Some(Command::Client(ClientSubcommand::SetInfo {
                        attr,
                        val,
                    })))
                }
                "GETNAME" => Ok(Some(Command::Client(ClientSubcommand::GetName))),
                "ID" => Ok(Some(Command::Client(ClientSubcommand::Id))),
                "KILL" => Ok(Some(Command::Client(ClientSubcommand::Kill(
                    args[2..].to_vec(),
                )))),
                "TRACKING" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'client|tracking' command".to_string()
                        );
                    }
                    let state = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let enabled = match state.as_str() {
                        "ON" => true,
                        "OFF" => false,
                        _ => return Err("syntax error".to_string()),
                    };
                    let mut redirect = None;
                    let mut bcast = false;
                    let mut prefixes = Vec::new();
                    let mut optin = false;
                    let mut optout = false;
                    let mut noloop = false;
                    let mut i = 3;
                    while i < args.len() {
                        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                        match opt.as_str() {
                            "REDIRECT" if i + 1 < args.len() => {
                                let id_str = String::from_utf8_lossy(&args[i + 1]);
                                let id = id_str.parse::<i64>().map_err(|_| {
                                    "value is not an integer or out of range".to_string()
                                })?;
                                redirect = Some(id);
                                i += 2;
                            }
                            "BCAST" => {
                                bcast = true;
                                i += 1;
                            }
                            "PREFIX" if i + 1 < args.len() => {
                                prefixes.push(args[i + 1].clone());
                                i += 2;
                            }
                            "OPTIN" => {
                                optin = true;
                                i += 1;
                            }
                            "OPTOUT" => {
                                optout = true;
                                i += 1;
                            }
                            "NOLOOP" => {
                                noloop = true;
                                i += 1;
                            }
                            _ => {
                                return Err("syntax error".to_string());
                            }
                        }
                    }
                    if !bcast && !prefixes.is_empty() {
                        return Err("PREFIX option requires BCAST mode to be enabled".to_string());
                    }
                    if bcast && (optin || optout) {
                        return Err(
                            "OPTIN and OPTOUT are not compatible with BCAST mode to be enabled"
                                .to_string(),
                        );
                    }
                    if optin && optout {
                        return Err("You can't provide both OPTIN and OPTOUT options".to_string());
                    }
                    Ok(Some(Command::Client(ClientSubcommand::Tracking {
                        enabled,
                        redirect,
                        bcast,
                        prefixes,
                        optin,
                        optout,
                        noloop,
                    })))
                }
                "CACHING" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'client|caching' command".to_string()
                        );
                    }
                    let state = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let flag = match state.as_str() {
                        "YES" => Some(true),
                        "NO" => Some(false),
                        _ => None,
                    };
                    Ok(Some(Command::Client(ClientSubcommand::Caching(flag))))
                }
                "GETREDIR" => {
                    if args.len() != 2 {
                        return Err(
                            "wrong number of arguments for 'client|getredir' command".to_string()
                        );
                    }
                    Ok(Some(Command::Client(ClientSubcommand::GetRedir)))
                }
                "TRACKINGINFO" => {
                    if args.len() != 2 {
                        return Err(
                            "wrong number of arguments for 'client|trackinginfo' command"
                                .to_string(),
                        );
                    }
                    Ok(Some(Command::Client(ClientSubcommand::TrackingInfo)))
                }
                "UNBLOCK" => {
                    if args.len() < 3 || args.len() > 4 {
                        return Err(
                            "wrong number of arguments for 'client unblock' command".to_string()
                        );
                    }
                    let client_id: u64 = std::str::from_utf8(&args[2])
                        .map_err(|_| "value is not an integer or out of range".to_string())?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    let unblock_type = if args.len() == 4 {
                        let opt = String::from_utf8_lossy(&args[3]).to_uppercase();
                        match opt.as_str() {
                            "TIMEOUT" => crate::block::ClientUnblockType::Timeout,
                            "ERROR" => crate::block::ClientUnblockType::Error,
                            _ => return Err("syntax error".to_string()),
                        }
                    } else {
                        crate::block::ClientUnblockType::Timeout
                    };
                    Ok(Some(Command::Client(ClientSubcommand::Unblock {
                        client_id,
                        unblock_type,
                    })))
                }
                "PAUSE" => {
                    if args.len() < 3 || args.len() > 4 {
                        return Err(
                            "wrong number of arguments for 'client|pause' command".to_string()
                        );
                    }
                    let timeout_i64: i64 = std::str::from_utf8(&args[2])
                        .map_err(|_| "timeout is not an integer or out of range".to_string())?
                        .parse()
                        .map_err(|_| "timeout is not an integer or out of range".to_string())?;
                    if timeout_i64 < 0 {
                        return Err("timeout is negative".to_string());
                    }
                    let timeout = timeout_i64 as u64;
                    let write_only = if args.len() == 4 {
                        let mode = String::from_utf8_lossy(&args[3]).to_uppercase();
                        if mode == "WRITE" {
                            true
                        } else if mode == "ALL" {
                            false
                        } else {
                            return Err("CLIENT PAUSE mode must be WRITE or ALL".to_string());
                        }
                    } else {
                        false
                    };
                    Ok(Some(Command::Client(ClientSubcommand::Pause(
                        timeout, write_only,
                    ))))
                }
                "UNPAUSE" => Ok(Some(Command::Client(ClientSubcommand::Unpause))),
                "NO-TOUCH" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'client|no-touch' command".to_string()
                        );
                    }
                    let opt = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let enabled = match opt.as_str() {
                        "ON" | "YES" | "1" => true,
                        "OFF" | "NO" | "0" => false,
                        _ => return Err("syntax error".to_string()),
                    };
                    Ok(Some(Command::Client(ClientSubcommand::NoTouch(enabled))))
                }
                "NO-EVICT" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'client|no-evict' command".to_string()
                        );
                    }
                    let opt = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let enabled = match opt.as_str() {
                        "ON" => true,
                        "OFF" => false,
                        _ => return Err("syntax error".to_string()),
                    };
                    Ok(Some(Command::Client(ClientSubcommand::NoEvict(enabled))))
                }
                "REPLY" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'client reply' command".to_string()
                        );
                    }
                    let mode_str = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let mode = match mode_str.as_str() {
                        "ON" => ClientReplyMode::On,
                        "OFF" => ClientReplyMode::Off,
                        "SKIP" => ClientReplyMode::Skip,
                        _ => return Err("syntax error, try CLIENT REPLY ON|OFF|SKIP".to_string()),
                    };
                    Ok(Some(Command::Client(ClientSubcommand::Reply(mode))))
                }
                _ => Ok(Some(Command::Unknown(format!("CLIENT {}", sub)))),
            }
        }

        "HSET" => {
            if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                return Err("wrong number of arguments for 'hset' command".to_string());
            }
            let key = args[1].clone();
            let mut fields = SmallVec::with_capacity((args.len() - 2) / 2);
            let mut i = 2;
            while i < args.len() {
                fields.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            }
            Ok(Some(Command::Hset { key, fields }))
        }
        "HSETNX" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'hsetnx' command".to_string());
            }
            Ok(Some(Command::Hsetnx {
                key: args[1].clone(),
                field: args[2].clone(),
                value: args[3].clone(),
            }))
        }
        "HMSET" => {
            if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                return Err("wrong number of arguments for 'hmset' command".to_string());
            }
            let key = args[1].clone();
            let mut fields = SmallVec::with_capacity((args.len() - 2) / 2);
            let mut i = 2;
            while i < args.len() {
                fields.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            }
            Ok(Some(Command::Hmset { key, fields }))
        }
        "HGET" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'hget' command".to_string());
            }
            Ok(Some(Command::Hget {
                key: args[1].clone(),
                field: args[2].clone(),
            }))
        }
        "HMGET" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'hmget' command".to_string());
            }
            Ok(Some(Command::Hmget {
                key: args[1].clone(),
                fields: args[2..].to_vec(),
            }))
        }
        "HDEL" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'hdel' command".to_string());
            }
            Ok(Some(Command::Hdel {
                key: args[1].clone(),
                fields: args[2..].to_vec(),
            }))
        }
        "HEXISTS" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'hexists' command".to_string());
            }
            Ok(Some(Command::Hexists {
                key: args[1].clone(),
                field: args[2].clone(),
            }))
        }
        "HLEN" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'hlen' command".to_string());
            }
            Ok(Some(Command::Hlen(args[1].clone())))
        }
        "HGETALL" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'hgetall' command".to_string());
            }
            Ok(Some(Command::Hgetall(args[1].clone())))
        }
        "HKEYS" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'hkeys' command".to_string());
            }
            Ok(Some(Command::Hkeys(args[1].clone())))
        }
        "HVALS" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'hvals' command".to_string());
            }
            Ok(Some(Command::Hvals(args[1].clone())))
        }
        "HSTRLEN" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'hstrlen' command".to_string());
            }
            Ok(Some(Command::Hstrlen {
                key: args[1].clone(),
                field: args[2].clone(),
            }))
        }
        "HGETDEL" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'hgetdel' command".to_string());
            }
            if !args[2].eq_ignore_ascii_case(b"FIELDS") {
                return Err("ERR argument FIELDS is missing or invalid".to_string());
            }
            let s = std::str::from_utf8(&args[3])
                .map_err(|_| "ERR Number of fields must be a positive integer".to_string())?;
            let numfields = s
                .parse::<i64>()
                .map_err(|_| "ERR Number of fields must be a positive integer".to_string())?;
            if numfields <= 0 {
                return Err("ERR Number of fields must be a positive integer".to_string());
            }
            let fields = args[4..].to_vec();
            if fields.len() != numfields as usize {
                return Err(
                    "ERR numfields parameter must match the number of arguments".to_string()
                );
            }
            Ok(Some(Command::Hgetdel {
                key: args[1].clone(),
                fields,
            }))
        }
        "LPUSH" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'lpush' command".to_string());
            }
            Ok(Some(Command::Lpush {
                key: args[1].clone(),
                values: args[2..].iter().cloned().collect(),
            }))
        }
        "RPUSH" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'rpush' command".to_string());
            }
            Ok(Some(Command::Rpush {
                key: args[1].clone(),
                values: args[2..].iter().cloned().collect(),
            }))
        }
        "LPUSHX" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'lpushx' command".to_string());
            }
            Ok(Some(Command::Lpushx {
                key: args[1].clone(),
                values: args[2..].to_vec(),
            }))
        }
        "RPUSHX" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'rpushx' command".to_string());
            }
            Ok(Some(Command::Rpushx {
                key: args[1].clone(),
                values: args[2..].to_vec(),
            }))
        }
        "LPOP" => {
            if args.len() < 2 || args.len() > 3 {
                return Err("wrong number of arguments for 'lpop' command".to_string());
            }
            let count = if args.len() > 2 {
                let c = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                Some(c)
            } else {
                None
            };
            Ok(Some(Command::Lpop {
                key: args[1].clone(),
                count,
            }))
        }
        "RPOP" => {
            if args.len() < 2 || args.len() > 3 {
                return Err("wrong number of arguments for 'rpop' command".to_string());
            }
            let count = if args.len() > 2 {
                let c = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                Some(c)
            } else {
                None
            };
            Ok(Some(Command::Rpop {
                key: args[1].clone(),
                count,
            }))
        }
        "LRANGE" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'lrange' command".to_string());
            }
            let start: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let stop: i64 = std::str::from_utf8(&args[3])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Lrange {
                key: args[1].clone(),
                start,
                stop,
            }))
        }
        "LLEN" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'llen' command".to_string());
            }
            Ok(Some(Command::Llen(args[1].clone())))
        }
        "LINDEX" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'lindex' command".to_string());
            }
            let index: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Lindex {
                key: args[1].clone(),
                index,
            }))
        }
        "BLPOP" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'blpop' command".to_string());
            }
            let s = std::str::from_utf8(args.last().unwrap())
                .map_err(|_| "timeout is not a float or out of range".to_string())?;
            let timeout: f64 =
                if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                    match u128::from_str_radix(hex, 16) {
                        Ok(v) => {
                            if v > (i64::MAX / 1000) as u128 {
                                return Err("timeout is out of range".to_string());
                            }
                            v as f64
                        }
                        Err(_) => return Err("timeout is out of range".to_string()),
                    }
                } else {
                    s.parse::<f64>()
                        .map_err(|_| "timeout is not a float or out of range".to_string())?
                };
            if timeout < 0.0 || timeout.is_nan() {
                return Err("timeout is negative".to_string());
            }
            if timeout > (i64::MAX / 1000) as f64 {
                return Err("timeout is out of range".to_string());
            }
            let keys = args[1..args.len() - 1].to_vec();
            Ok(Some(Command::Blpop { keys, timeout }))
        }
        "BRPOP" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'brpop' command".to_string());
            }
            let s = std::str::from_utf8(args.last().unwrap())
                .map_err(|_| "timeout is not a float or out of range".to_string())?;
            let timeout: f64 =
                if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                    match u128::from_str_radix(hex, 16) {
                        Ok(v) => {
                            if v > (i64::MAX / 1000) as u128 {
                                return Err("timeout is out of range".to_string());
                            }
                            v as f64
                        }
                        Err(_) => return Err("timeout is out of range".to_string()),
                    }
                } else {
                    s.parse::<f64>()
                        .map_err(|_| "timeout is not a float or out of range".to_string())?
                };
            if timeout < 0.0 || timeout.is_nan() {
                return Err("timeout is negative".to_string());
            }
            if timeout > (i64::MAX / 1000) as f64 {
                return Err("timeout is out of range".to_string());
            }
            let keys = args[1..args.len() - 1].to_vec();
            Ok(Some(Command::Brpop { keys, timeout }))
        }
        "LMPOP" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'lmpop' command".to_string());
            }
            let numkeys: i64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "numkeys should be greater than 0".to_string())?;
            if numkeys <= 0 {
                return Err("numkeys should be greater than 0".to_string());
            }
            let numkeys = numkeys as usize;
            if args.len() < 2 + numkeys + 1 {
                return Err("syntax error".to_string());
            }
            let keys = args[2..2 + numkeys].to_vec();
            let dir_str = String::from_utf8_lossy(&args[2 + numkeys]).to_uppercase();
            let where_from = match dir_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let mut count = 1;
            let idx = 2 + numkeys + 1;
            if idx < args.len() {
                if idx + 2 != args.len() {
                    return Err("syntax error".to_string());
                }
                if !String::from_utf8_lossy(&args[idx]).eq_ignore_ascii_case("COUNT") {
                    return Err("syntax error".to_string());
                }
                let c: i64 = std::str::from_utf8(&args[idx + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| "count should be greater than 0".to_string())?;
                if c <= 0 {
                    return Err("count should be greater than 0".to_string());
                }
                count = c as usize;
            }
            Ok(Some(Command::Lmpop {
                keys,
                where_from,
                count,
            }))
        }
        "BLMPOP" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'blmpop' command".to_string());
            }
            let timeout: f64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "timeout is not a float or out of range".to_string())?;
            if timeout < 0.0 || timeout.is_nan() {
                return Err("timeout is negative".to_string());
            }
            if timeout > (i64::MAX / 1000) as f64 {
                return Err("timeout is out of range".to_string());
            }
            let numkeys: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "numkeys should be greater than 0".to_string())?;
            if numkeys <= 0 {
                return Err("numkeys should be greater than 0".to_string());
            }
            let numkeys = numkeys as usize;
            if args.len() < 3 + numkeys + 1 {
                return Err("syntax error".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let dir_str = String::from_utf8_lossy(&args[3 + numkeys]).to_uppercase();
            let where_from = match dir_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let mut count = 1;
            let idx = 3 + numkeys + 1;
            if idx < args.len() {
                if idx + 2 != args.len() {
                    return Err("syntax error".to_string());
                }
                if !String::from_utf8_lossy(&args[idx]).eq_ignore_ascii_case("COUNT") {
                    return Err("syntax error".to_string());
                }
                let c: i64 = std::str::from_utf8(&args[idx + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| "count should be greater than 0".to_string())?;
                if c <= 0 {
                    return Err("count should be greater than 0".to_string());
                }
                count = c as usize;
            }
            Ok(Some(Command::Blmpop {
                timeout,
                keys,
                where_from,
                count,
            }))
        }
        "SADD" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'sadd' command".to_string());
            }
            Ok(Some(Command::Sadd {
                key: args[1].clone(),
                members: SmallVec::from_vec(args[2..].to_vec()),
            }))
        }
        "SREM" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'srem' command".to_string());
            }
            Ok(Some(Command::Srem {
                key: args[1].clone(),
                members: args[2..].to_vec(),
            }))
        }
        "SMEMBERS" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'smembers' command".to_string());
            }
            Ok(Some(Command::Smembers(args[1].clone())))
        }
        "SISMEMBER" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'sismember' command".to_string());
            }
            Ok(Some(Command::Sismember {
                key: args[1].clone(),
                member: args[2].clone(),
            }))
        }
        "SCARD" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'scard' command".to_string());
            }
            Ok(Some(Command::Scard(args[1].clone())))
        }
        "SPOP" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'spop' command".to_string());
            }
            let count = if args.len() > 2 {
                let c = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                Some(c)
            } else {
                None
            };
            Ok(Some(Command::Spop {
                key: args[1].clone(),
                count,
            }))
        }
        "SINTER" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'sinter' command".to_string());
            }
            Ok(Some(Command::Sinter(args[1..].to_vec())))
        }
        "SUNION" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'sunion' command".to_string());
            }
            Ok(Some(Command::Sunion(args[1..].to_vec())))
        }
        "SDIFF" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'sdiff' command".to_string());
            }
            Ok(Some(Command::Sdiff(args[1..].to_vec())))
        }
        "SINTERSTORE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'sinterstore' command".to_string());
            }
            Ok(Some(Command::Sinterstore {
                destination: args[1].clone(),
                keys: args[2..].to_vec(),
            }))
        }
        "SUNIONSTORE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'sunionstore' command".to_string());
            }
            Ok(Some(Command::Sunionstore {
                destination: args[1].clone(),
                keys: args[2..].to_vec(),
            }))
        }
        "SDIFFSTORE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'sdiffstore' command".to_string());
            }
            Ok(Some(Command::Sdiffstore {
                destination: args[1].clone(),
                keys: args[2..].to_vec(),
            }))
        }
        "SINTERCARD" | "SUNIONCARD" | "SDIFFCARD" => {
            if args.len() < 3 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let numkeys: i64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "numkeys must be greater than 0".to_string())?;
            if numkeys <= 0 {
                return Err("numkeys must be greater than 0".to_string());
            }
            let numkeys = numkeys as usize;
            if args.len() < 2 + numkeys {
                return Err("Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[2..2 + numkeys].to_vec();
            let mut limit = 0;
            let mut i = 2 + numkeys;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "LIMIT" {
                    if i + 1 >= args.len() {
                        return Err("syntax error".to_string());
                    }
                    let lim: i64 = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "LIMIT can't be negative".to_string())?;
                    if lim < 0 {
                        return Err("LIMIT can't be negative".to_string());
                    }
                    limit = lim as usize;
                    i += 2;
                } else if cmd_name == "SUNIONCARD" && opt == "APPROX" {
                    i += 1;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            match cmd_name {
                "SINTERCARD" => Ok(Some(Command::Sintercard { keys, limit })),
                "SUNIONCARD" => Ok(Some(Command::Sunioncard { keys, limit })),
                _ => Ok(Some(Command::Sdiffcard { keys, limit })),
            }
        }
        "ZADD" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'zadd' command".to_string());
            }
            let mut i = 2;
            let mut flags = crate::table::ZAddFlags::default();
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "NX" => {
                        flags.nx = true;
                        i += 1;
                    }
                    "XX" => {
                        flags.xx = true;
                        i += 1;
                    }
                    "GT" => {
                        flags.gt = true;
                        i += 1;
                    }
                    "LT" => {
                        flags.lt = true;
                        i += 1;
                    }
                    "CH" => {
                        flags.ch = true;
                        i += 1;
                    }
                    "INCR" => {
                        flags.incr = true;
                        i += 1;
                    }
                    _ => break,
                }
            }
            if flags.nx && flags.xx {
                return Err("XX and NX options at the same time are not compatible".to_string());
            }
            if flags.gt && flags.lt {
                return Err("GT and LT options at the same time are not compatible".to_string());
            }
            if flags.nx && (flags.gt || flags.lt) {
                return Err("NX and GT, LT options are not compatible".to_string());
            }
            let remaining = &args[i..];
            if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
                return Err("syntax error".to_string());
            }
            if flags.incr && remaining.len() != 2 {
                return Err("INCR option supports a single increment-element pair".to_string());
            }
            let mut elements = SmallVec::with_capacity(remaining.len() / 2);
            let mut j = 0;
            while j < remaining.len() {
                let score_str =
                    std::str::from_utf8(&remaining[j]).map_err(|_| "value is not a valid float")?;
                let score = parse_redis_f64(score_str)
                    .ok_or_else(|| "value is not a valid float".to_string())?;
                let member = remaining[j + 1].clone();
                elements.push((score, member));
                j += 2;
            }
            Ok(Some(Command::Zadd {
                key: args[1].clone(),
                elements,
                flags,
            }))
        }
        "ZREM" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'zrem' command".to_string());
            }
            Ok(Some(Command::Zrem {
                key: args[1].clone(),
                members: args[2..].to_vec(),
            }))
        }
        "ZSCORE" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'zscore' command".to_string());
            }
            Ok(Some(Command::Zscore {
                key: args[1].clone(),
                member: args[2].clone(),
            }))
        }
        "ZCARD" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'zcard' command".to_string());
            }
            Ok(Some(Command::Zcard(args[1].clone())))
        }
        "ZRANK" => {
            if args.len() != 3 && args.len() != 4 {
                return Err("wrong number of arguments for 'zrank' command".to_string());
            }
            let with_score = if args.len() == 4 {
                if !args[3].eq_ignore_ascii_case(b"WITHSCORE") {
                    return Err("syntax error".to_string());
                }
                true
            } else {
                false
            };
            Ok(Some(Command::Zrank {
                key: args[1].clone(),
                member: args[2].clone(),
                with_score,
            }))
        }
        "ZREVRANK" => {
            if args.len() != 3 && args.len() != 4 {
                return Err("wrong number of arguments for 'zrevrank' command".to_string());
            }
            let with_score = if args.len() == 4 {
                if !args[3].eq_ignore_ascii_case(b"WITHSCORE") {
                    return Err("syntax error".to_string());
                }
                true
            } else {
                false
            };
            Ok(Some(Command::Zrevrank {
                key: args[1].clone(),
                member: args[2].clone(),
                with_score,
            }))
        }
        "ZCOUNT" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'zcount' command".to_string());
            }
            let (min, min_inc) = parse_score_bound(&args[2])?;
            let (max, max_inc) = parse_score_bound(&args[3])?;
            Ok(Some(Command::Zcount {
                key: args[1].clone(),
                min,
                min_inc,
                max,
                max_inc,
            }))
        }
        "ZINCRBY" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'zincrby' command".to_string());
            }
            let delta_str =
                std::str::from_utf8(&args[2]).map_err(|_| "value is not a valid float")?;
            let delta = parse_redis_f64(delta_str)
                .ok_or_else(|| "value is not a valid float".to_string())?;
            Ok(Some(Command::Zincrby {
                key: args[1].clone(),
                delta,
                member: args[3].clone(),
            }))
        }
        "ZRANGE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'zrange' command".to_string());
            }
            let mut by_score = false;
            let mut by_lex = false;
            let mut rev = false;
            let mut with_scores = false;
            let mut has_limit = false;
            let mut offset = 0;
            let mut count = None;

            let mut i = 4;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "BYSCORE" => {
                        by_score = true;
                        i += 1;
                    }
                    "BYLEX" => {
                        by_lex = true;
                        i += 1;
                    }
                    "REV" => {
                        rev = true;
                        i += 1;
                    }
                    "WITHSCORES" => {
                        with_scores = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        has_limit = true;
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let off_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?;
                        let cnt_str = std::str::from_utf8(&args[i + 2])
                            .map_err(|_| "value is not an integer or out of range")?;
                        let off: i64 = off_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        offset = if off < 0 { usize::MAX } else { off as usize };
                        let c = cnt_str
                            .parse::<i64>()
                            .map_err(|_| "value is not an integer or out of range")?;
                        count = if c < 0 { None } else { Some(c as usize) };
                        i += 3;
                    }
                    _ => {
                        return Err("syntax error".to_string());
                    }
                }
            }

            if by_score && by_lex {
                return Err("syntax error".to_string());
            }
            if has_limit && !by_score && !by_lex {
                return Err("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX".to_string());
            }
            if with_scores && by_lex {
                return Err(
                    "syntax error, WITHSCORES not supported in combination with BYLEX".to_string(),
                );
            }

            let mut opts = crate::table::ZRangeOpts {
                by_score,
                by_lex,
                rev,
                with_scores,
                offset,
                count,
                ..Default::default()
            };

            if by_score {
                let (min_idx, max_idx) = if rev { (3, 2) } else { (2, 3) };
                let (min, min_i) = parse_score_bound(&args[min_idx])?;
                let (max, max_i) = parse_score_bound(&args[max_idx])?;
                opts.min_score = min;
                opts.min_inc = min_i;
                opts.max_score = max;
                opts.max_inc = max_i;
            } else if by_lex {
                let (min_idx, max_idx) = if rev { (3, 2) } else { (2, 3) };
                let min =
                    crate::table::parse_lex_bound(&args[min_idx]).map_err(|e| e.to_string())?;
                let max =
                    crate::table::parse_lex_bound(&args[max_idx]).map_err(|e| e.to_string())?;
                opts.min_lex = min;
                opts.max_lex = max;
            } else {
                let start: i64 = std::str::from_utf8(&args[2])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                let stop: i64 = std::str::from_utf8(&args[3])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                opts.start = start;
                opts.stop = stop;
            }

            Ok(Some(Command::Zrange {
                key: args[1].clone(),
                opts,
            }))
        }
        "ZREVRANGE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'zrevrange' command".to_string());
            }
            let start: i64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            let stop: i64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            let mut with_scores = false;
            if args.len() == 5 {
                if args[4].eq_ignore_ascii_case(b"WITHSCORES") {
                    with_scores = true;
                } else {
                    return Err("syntax error".to_string());
                }
            } else if args.len() > 5 {
                return Err("syntax error".to_string());
            }
            Ok(Some(Command::Zrange {
                key: args[1].clone(),
                opts: crate::table::ZRangeOpts {
                    start,
                    stop,
                    rev: true,
                    with_scores,
                    ..Default::default()
                },
            }))
        }
        "ZRANGEBYSCORE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'zrangebyscore' command".to_string());
            }
            let (min, min_inc) = parse_score_bound(&args[2])?;
            let (max, max_inc) = parse_score_bound(&args[3])?;
            let mut with_scores = false;
            let mut offset = 0;
            let mut count = None;
            let mut i = 4;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "WITHSCORES" => {
                        with_scores = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let off: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        offset = if off < 0 { usize::MAX } else { off as usize };
                        let c: i64 = std::str::from_utf8(&args[i + 2])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        count = if c < 0 { None } else { Some(c as usize) };
                        i += 3;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Zrange {
                key: args[1].clone(),
                opts: crate::table::ZRangeOpts {
                    min_score: min,
                    min_inc,
                    max_score: max,
                    max_inc,
                    by_score: true,
                    rev: false,
                    with_scores,
                    offset,
                    count,
                    ..Default::default()
                },
            }))
        }
        "ZREVRANGEBYSCORE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'zrevrangebyscore' command".to_string());
            }
            let (max, max_inc) = parse_score_bound(&args[2])?;
            let (min, min_inc) = parse_score_bound(&args[3])?;
            let mut with_scores = false;
            let mut offset = 0;
            let mut count = None;
            let mut i = 4;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "WITHSCORES" => {
                        with_scores = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let off: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        offset = if off < 0 { usize::MAX } else { off as usize };
                        let c: i64 = std::str::from_utf8(&args[i + 2])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        count = if c < 0 { None } else { Some(c as usize) };
                        i += 3;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Zrange {
                key: args[1].clone(),
                opts: crate::table::ZRangeOpts {
                    min_score: min,
                    min_inc,
                    max_score: max,
                    max_inc,
                    by_score: true,
                    rev: true,
                    with_scores,
                    offset,
                    count,
                    ..Default::default()
                },
            }))
        }
        "ZRANGEBYLEX" => {
            if args.len() != 4 && args.len() != 7 {
                return Err("wrong number of arguments for 'zrangebylex' command".to_string());
            }
            let min = crate::table::parse_lex_bound(&args[2]).map_err(|e| e.to_string())?;
            let max = crate::table::parse_lex_bound(&args[3]).map_err(|e| e.to_string())?;
            let mut offset = 0;
            let mut count = None;
            if args.len() == 7 {
                let opt = String::from_utf8_lossy(&args[4]).to_uppercase();
                if opt != "LIMIT" {
                    return Err("syntax error".to_string());
                }
                let off: i64 = std::str::from_utf8(&args[5])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                offset = if off < 0 { usize::MAX } else { off as usize };
                let c: i64 = std::str::from_utf8(&args[6])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                count = if c < 0 { None } else { Some(c as usize) };
            }
            Ok(Some(Command::Zrange {
                key: args[1].clone(),
                opts: crate::table::ZRangeOpts {
                    by_lex: true,
                    min_lex: min,
                    max_lex: max,
                    offset,
                    count,
                    ..Default::default()
                },
            }))
        }
        "ZREVRANGEBYLEX" => {
            if args.len() != 4 && args.len() != 7 {
                return Err("wrong number of arguments for 'zrevrangebylex' command".to_string());
            }
            let max = crate::table::parse_lex_bound(&args[2]).map_err(|e| e.to_string())?;
            let min = crate::table::parse_lex_bound(&args[3]).map_err(|e| e.to_string())?;
            let mut offset = 0;
            let mut count = None;
            if args.len() == 7 {
                let opt = String::from_utf8_lossy(&args[4]).to_uppercase();
                if opt != "LIMIT" {
                    return Err("syntax error".to_string());
                }
                let off: i64 = std::str::from_utf8(&args[5])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                offset = if off < 0 { usize::MAX } else { off as usize };
                let c: i64 = std::str::from_utf8(&args[6])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                count = if c < 0 { None } else { Some(c as usize) };
            }
            Ok(Some(Command::Zrange {
                key: args[1].clone(),
                opts: crate::table::ZRangeOpts {
                    by_lex: true,
                    min_lex: min,
                    max_lex: max,
                    rev: true,
                    offset,
                    count,
                    ..Default::default()
                },
            }))
        }
        "ZRANGESTORE" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'zrangestore' command".to_string());
            }
            let mut by_score = false;
            let mut by_lex = false;
            let mut rev = false;
            let mut has_limit = false;
            let mut offset = 0;
            let mut count = None;

            let mut i = 5;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "BYSCORE" => {
                        by_score = true;
                        i += 1;
                    }
                    "BYLEX" => {
                        by_lex = true;
                        i += 1;
                    }
                    "REV" => {
                        rev = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        has_limit = true;
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let off: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        offset = if off < 0 { usize::MAX } else { off as usize };
                        let c: i64 = std::str::from_utf8(&args[i + 2])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        count = if c < 0 { None } else { Some(c as usize) };
                        i += 3;
                    }
                    _ => {
                        return Err("syntax error".to_string());
                    }
                }
            }

            if by_score && by_lex {
                return Err("syntax error".to_string());
            }
            if has_limit && !by_score && !by_lex {
                return Err("syntax error".to_string());
            }

            let mut opts = crate::table::ZRangeOpts {
                by_score,
                by_lex,
                rev,
                with_scores: false,
                offset,
                count,
                ..Default::default()
            };

            if by_score {
                let (min_idx, max_idx) = if rev { (4, 3) } else { (3, 4) };
                let (min, min_i) = parse_score_bound(&args[min_idx])?;
                let (max, max_i) = parse_score_bound(&args[max_idx])?;
                opts.min_score = min;
                opts.min_inc = min_i;
                opts.max_score = max;
                opts.max_inc = max_i;
            } else if by_lex {
                let (min_idx, max_idx) = if rev { (4, 3) } else { (3, 4) };
                let min =
                    crate::table::parse_lex_bound(&args[min_idx]).map_err(|e| e.to_string())?;
                let max =
                    crate::table::parse_lex_bound(&args[max_idx]).map_err(|e| e.to_string())?;
                opts.min_lex = min;
                opts.max_lex = max;
            } else {
                let start: i64 = std::str::from_utf8(&args[3])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                let stop: i64 = std::str::from_utf8(&args[4])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                opts.start = start;
                opts.stop = stop;
            }

            Ok(Some(Command::Zrangestore {
                dst: args[1].clone(),
                src: args[2].clone(),
                opts,
            }))
        }
        "ZPOPMIN" | "ZPOPMAX" => {
            let is_min = cmd_name == "ZPOPMIN";
            if args.len() < 2 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let count = if args.len() > 2 {
                let val = std::str::from_utf8(&args[2])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse::<i64>()
                    .map_err(|_| "value is not an integer or out of range")?;
                if val < 0 {
                    return Err("value is out of range, must be positive".to_string());
                }
                Some(val as usize)
            } else {
                None
            };
            if is_min {
                Ok(Some(Command::Zpopmin {
                    key: args[1].clone(),
                    count,
                }))
            } else {
                Ok(Some(Command::Zpopmax {
                    key: args[1].clone(),
                    count,
                }))
            }
        }
        "BZPOPMIN" | "BZPOPMAX" => {
            let is_min = cmd_name == "BZPOPMIN";
            if args.len() < 3 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let timeout: f64 = std::str::from_utf8(&args[args.len() - 1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "timeout is not a float or out of range".to_string())?;
            if timeout < 0.0 || timeout.is_nan() {
                return Err("timeout is negative".to_string());
            }
            if timeout > (i64::MAX / 1000) as f64 {
                return Err("timeout is out of range".to_string());
            }
            let keys = args[1..args.len() - 1].to_vec();
            if is_min {
                Ok(Some(Command::Bzpopmin { keys, timeout }))
            } else {
                Ok(Some(Command::Bzpopmax { keys, timeout }))
            }
        }
        "ZMPOP" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'zmpop' command".to_string());
            }
            let numkeys: i64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "numkeys should be greater than 0".to_string())?;
            if numkeys <= 0 {
                return Err("numkeys should be greater than 0".to_string());
            }
            let numkeys = numkeys as usize;
            if args.len() < 2 + numkeys + 1 {
                return Err("syntax error".to_string());
            }
            let keys = args[2..2 + numkeys].to_vec();
            let where_str = String::from_utf8_lossy(&args[2 + numkeys]).to_uppercase();
            let is_min = match where_str.as_str() {
                "MIN" => true,
                "MAX" => false,
                _ => return Err("syntax error".to_string()),
            };
            let mut count = 1;
            let idx = 2 + numkeys + 1;
            if idx < args.len() {
                if idx + 2 != args.len() {
                    return Err("syntax error".to_string());
                }
                if !String::from_utf8_lossy(&args[idx]).eq_ignore_ascii_case("COUNT") {
                    return Err("syntax error".to_string());
                }
                let c: i64 = std::str::from_utf8(&args[idx + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| "count should be greater than 0".to_string())?;
                if c <= 0 {
                    return Err("count should be greater than 0".to_string());
                }
                count = c as usize;
            }
            Ok(Some(Command::Zmpop {
                keys,
                is_min,
                count,
            }))
        }
        "BZMPOP" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'bzmpop' command".to_string());
            }
            let timeout: f64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "timeout is not a float or out of range".to_string())?;
            if timeout < 0.0 || timeout.is_nan() {
                return Err("timeout is negative".to_string());
            }
            if timeout > (i64::MAX / 1000) as f64 {
                return Err("timeout is out of range".to_string());
            }
            let numkeys: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "numkeys should be greater than 0".to_string())?;
            if numkeys <= 0 {
                return Err("numkeys should be greater than 0".to_string());
            }
            let numkeys = numkeys as usize;
            if args.len() < 3 + numkeys + 1 {
                return Err("syntax error".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let where_str = String::from_utf8_lossy(&args[3 + numkeys]).to_uppercase();
            let is_min = match where_str.as_str() {
                "MIN" => true,
                "MAX" => false,
                _ => return Err("syntax error".to_string()),
            };
            let mut count = 1;
            let idx = 3 + numkeys + 1;
            if idx < args.len() {
                if idx + 2 != args.len() {
                    return Err("syntax error".to_string());
                }
                if !String::from_utf8_lossy(&args[idx]).eq_ignore_ascii_case("COUNT") {
                    return Err("syntax error".to_string());
                }
                let c: i64 = std::str::from_utf8(&args[idx + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| "count should be greater than 0".to_string())?;
                if c <= 0 {
                    return Err("count should be greater than 0".to_string());
                }
                count = c as usize;
            }
            Ok(Some(Command::Bzmpop {
                timeout,
                keys,
                is_min,
                count,
            }))
        }
        "ZUNIONSTORE" | "ZINTERSTORE" => {
            let is_union = cmd_name == "ZUNIONSTORE";
            if args.len() < 4 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let destination = args[1].clone();
            let numkeys: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys <= 0 {
                return Err(format!(
                    "at least 1 input key is needed for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let numkeys = numkeys as usize;
            if args.len() < 3 + numkeys {
                return Err("syntax error".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let mut weights = Vec::new();
            let mut aggregate = crate::table::Aggregate::Sum;
            let mut i = 3 + numkeys;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "WEIGHTS" {
                    i += 1;
                    for _ in 0..numkeys {
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let w: f64 = std::str::from_utf8(&args[i])
                            .ok()
                            .and_then(parse_redis_f64)
                            .ok_or_else(|| "weight value is not a float".to_string())?;
                        weights.push(w);
                        i += 1;
                    }
                } else if opt == "AGGREGATE" {
                    if i + 1 >= args.len() {
                        return Err("syntax error".to_string());
                    }
                    let agg_str = String::from_utf8_lossy(&args[i + 1]).to_uppercase();
                    aggregate = match agg_str.as_str() {
                        "SUM" => crate::table::Aggregate::Sum,
                        "MIN" => crate::table::Aggregate::Min,
                        "MAX" => crate::table::Aggregate::Max,
                        "COUNT" => crate::table::Aggregate::Count,
                        _ => return Err("syntax error".to_string()),
                    };
                    i += 2;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            if is_union {
                Ok(Some(Command::Zunionstore {
                    destination,
                    keys,
                    weights,
                    aggregate,
                }))
            } else {
                Ok(Some(Command::Zinterstore {
                    destination,
                    keys,
                    weights,
                    aggregate,
                }))
            }
        }
        "ZDIFFSTORE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'zdiffstore' command".to_string());
            }
            let destination = args[1].clone();
            let numkeys: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys <= 0 {
                return Err("at least 1 input key is needed for 'zdiffstore' command".to_string());
            }
            let numkeys = numkeys as usize;
            if args.len() != 3 + numkeys {
                return Err("syntax error".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            Ok(Some(Command::Zdiffstore { destination, keys }))
        }
        "ZDIFF" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'zdiff' command".to_string());
            }
            let numkeys: i64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys <= 0 {
                return Err("at least 1 input key is needed for 'zdiff' command".to_string());
            }
            let numkeys = numkeys as usize;
            if args.len() < 2 + numkeys {
                return Err("syntax error".to_string());
            }
            let keys = args[2..2 + numkeys].to_vec();
            let mut with_scores = false;
            if args.len() > 2 + numkeys {
                let opt = String::from_utf8_lossy(&args[2 + numkeys]).to_uppercase();
                if opt == "WITHSCORES" {
                    with_scores = true;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            Ok(Some(Command::Zdiff { keys, with_scores }))
        }
        "ZUNION" | "ZINTER" => {
            let is_union = cmd_name == "ZUNION";
            if args.len() < 3 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let numkeys: i64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys <= 0 {
                return Err(format!(
                    "at least 1 input key is needed for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let numkeys = numkeys as usize;
            if args.len() < 2 + numkeys {
                return Err("syntax error".to_string());
            }
            let keys = args[2..2 + numkeys].to_vec();
            let mut weights = Vec::new();
            let mut aggregate = crate::table::Aggregate::Sum;
            let mut with_scores = false;
            let mut i = 2 + numkeys;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "WEIGHTS" {
                    i += 1;
                    for _ in 0..numkeys {
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let w: f64 = std::str::from_utf8(&args[i])
                            .ok()
                            .and_then(parse_redis_f64)
                            .ok_or_else(|| "weight value is not a float".to_string())?;
                        weights.push(w);
                        i += 1;
                    }
                } else if opt == "AGGREGATE" {
                    if i + 1 >= args.len() {
                        return Err("syntax error".to_string());
                    }
                    let agg_str = String::from_utf8_lossy(&args[i + 1]).to_uppercase();
                    aggregate = match agg_str.as_str() {
                        "SUM" => crate::table::Aggregate::Sum,
                        "MIN" => crate::table::Aggregate::Min,
                        "MAX" => crate::table::Aggregate::Max,
                        "COUNT" => crate::table::Aggregate::Count,
                        _ => return Err("syntax error".to_string()),
                    };
                    i += 2;
                } else if opt == "WITHSCORES" {
                    with_scores = true;
                    i += 1;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            if is_union {
                Ok(Some(Command::Zunion {
                    keys,
                    weights,
                    aggregate,
                    with_scores,
                }))
            } else {
                Ok(Some(Command::Zinter {
                    keys,
                    weights,
                    aggregate,
                    with_scores,
                }))
            }
        }
        "ZINTERCARD" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'zintercard' command".to_string());
            }
            let numkeys: i64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys <= 0 {
                return Err("at least 1 input key is needed for 'zintercard' command".to_string());
            }
            let numkeys = numkeys as usize;
            if args.len() < 2 + numkeys {
                return Err("Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[2..2 + numkeys].to_vec();
            let mut limit = 0;
            let mut i = 2 + numkeys;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "LIMIT" {
                    if i + 1 >= args.len() {
                        return Err("syntax error".to_string());
                    }
                    let lim: i64 = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "LIMIT can't be negative".to_string())?;
                    if lim < 0 {
                        return Err("LIMIT can't be negative".to_string());
                    }
                    limit = lim as usize;
                    i += 2;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            Ok(Some(Command::Zintercard { keys, limit }))
        }
        "TYPE" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'type' command".to_string());
            }
            Ok(Some(Command::Type(args[1].clone())))
        }
        "DBSIZE" => {
            if args.len() != 1 {
                return Err("wrong number of arguments for 'dbsize' command".to_string());
            }
            Ok(Some(Command::Dbsize))
        }
        "SELECT" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'select' command".to_string());
            }
            let idx: u32 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Select(idx)))
        }
        "SLOWLOG" => {
            let sub_args = if args.len() > 1 {
                args[1..].to_vec()
            } else {
                vec![Bytes::from_static(b"GET")]
            };
            Ok(Some(Command::Slowlog(sub_args)))
        }
        "DEBUG" => Ok(Some(Command::Debug(args[1..].to_vec()))),
        "DIGEST" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'digest' command".to_string());
            }
            Ok(Some(Command::Digest(args[1].clone())))
        }
        "MEMORY" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'memory' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "USAGE" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'memory|usage' command".to_string()
                        );
                    }
                    Ok(Some(Command::Memory(MemorySubcommand::Usage {
                        key: args[2].clone(),
                    })))
                }
                "STATS" => Ok(Some(Command::Memory(MemorySubcommand::Stats))),
                "PURGE" => Ok(Some(Command::Memory(MemorySubcommand::Purge))),
                "DOCTOR" => Ok(Some(Command::Memory(MemorySubcommand::Doctor))),
                "DEFRAG" => Ok(Some(Command::Memory(MemorySubcommand::Defrag))),
                "HELP" => Ok(Some(Command::Unknown("MEMORY HELP".to_string()))),
                _ => Err(format!("unknown subcommand '{}' for 'memory'", sub)),
            }
        }
        "DEFRAG" | "ACTIVE-DEFRAG" => Ok(Some(Command::Memory(MemorySubcommand::Defrag))),
        "MODULE" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'module' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            Ok(Some(Command::Unknown(format!("MODULE {}", sub))))
        }
        "HOTKEYS" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'hotkeys' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            Ok(Some(Command::Unknown(format!("HOTKEYS {}", sub))))
        }
        "MONITOR" => {
            if args.len() != 1 {
                return Err("wrong number of arguments for 'monitor' command".to_string());
            }
            Ok(Some(Command::Monitor))
        }
        "EXPIREAT" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'expireat' command".to_string());
            }
            let ts: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if !((i64::MIN / 1000)..=(i64::MAX / 1000)).contains(&ts) {
                return Err("invalid expire time in 'expireat' command".to_string());
            }
            let opts = parse_expire_options(&args[3..])?;
            let now_unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let dur = if ts <= now_unix {
                Duration::ZERO
            } else {
                Duration::from_secs((ts - now_unix) as u64)
            };
            Ok(Some(Command::Expire {
                key: args[1].clone(),
                duration: dur,
                opts,
            }))
        }
        "PEXPIREAT" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'pexpireat' command".to_string());
            }
            let ts_ms: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let opts = parse_expire_options(&args[3..])?;
            let now_unix_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let dur = if ts_ms <= now_unix_ms {
                Duration::ZERO
            } else {
                Duration::from_millis((ts_ms - now_unix_ms) as u64)
            };
            Ok(Some(Command::Expire {
                key: args[1].clone(),
                duration: dur,
                opts,
            }))
        }
        "TOUCH" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'touch' command".to_string());
            }
            args.remove(0);
            Ok(Some(Command::Touch(args)))
        }
        "RENAME" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'rename' command".to_string());
            }
            Ok(Some(Command::Rename {
                key: args[1].clone(),
                newkey: args[2].clone(),
                nx: false,
            }))
        }
        "RENAMENX" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'renamenx' command".to_string());
            }
            Ok(Some(Command::Rename {
                key: args[1].clone(),
                newkey: args[2].clone(),
                nx: true,
            }))
        }
        "COPY" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'copy' command".to_string());
            }
            let source = args[1].clone();
            let destination = args[2].clone();
            if source == destination {
                return Err("source and destination objects are the same".to_string());
            }
            let mut replace = false;
            let mut destination_db = None;
            let mut i = 3;
            while i < args.len() {
                let opt = std::str::from_utf8(&args[i])
                    .map_err(|_| "syntax error".to_string())?
                    .to_uppercase();
                match opt.as_str() {
                    "REPLACE" => {
                        replace = true;
                        i += 1;
                    }
                    "DB" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let db_id: i64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        if db_id != 0 {
                            return Err("DB index is out of range".to_string());
                        }
                        destination_db = Some(db_id as u32);
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Copy {
                source,
                destination,
                destination_db,
                replace,
            }))
        }
        "FLUSHDB" => Ok(Some(Command::Flushdb)),
        "FLUSHALL" => Ok(Some(Command::Flushall)),
        "SETNX" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'setnx' command".to_string());
            }
            Ok(Some(Command::Setnx {
                key: args[1].clone(),
                value: args[2].clone(),
            }))
        }
        "SETEX" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'setex' command".to_string());
            }
            let secs: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            if secs <= 0
                || secs > (i64::MAX / 1000)
                || (secs as i128 * 1000) > (i64::MAX - now_ms) as i128
            {
                return Err("invalid expire time in 'setex' command".to_string());
            }
            Ok(Some(Command::Set {
                key: args[1].clone(),
                value: args[3].clone(),
                expire_in: Some(Duration::from_secs(secs as u64)),
                condition: SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            }))
        }
        "PSETEX" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'psetex' command".to_string());
            }
            let ms: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            if ms <= 0 || ms > i64::MAX - now_ms {
                return Err("invalid expire time in 'psetex' command".to_string());
            }
            Ok(Some(Command::Set {
                key: args[1].clone(),
                value: args[3].clone(),
                expire_in: Some(Duration::from_millis(ms as u64)),
                condition: SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            }))
        }
        "GETSET" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'getset' command".to_string());
            }
            Ok(Some(Command::Getset {
                key: args[1].clone(),
                value: args[2].clone(),
            }))
        }
        "GETDEL" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'getdel' command".to_string());
            }
            Ok(Some(Command::Getdel(args[1].clone())))
        }
        "APPEND" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'append' command".to_string());
            }
            Ok(Some(Command::Append {
                key: args[1].clone(),
                value: args[2].clone(),
            }))
        }
        "STRLEN" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'strlen' command".to_string());
            }
            Ok(Some(Command::Strlen(args[1].clone())))
        }
        "MSETNX" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return Err("wrong number of arguments for 'msetnx' command".to_string());
            }
            let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
            let mut i = 1;
            while i < args.len() {
                pairs.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            }
            Ok(Some(Command::Msetnx(pairs)))
        }
        "SAVE" => Ok(Some(Command::Save)),
        "BGSAVE" => Ok(Some(Command::Bgsave)),
        "BGREWRITEAOF" => Ok(Some(Command::Bgrewriteaof)),
        "LASTSAVE" => Ok(Some(Command::Lastsave)),
        "LATENCY" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'latency' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "LATEST" => Ok(Some(Command::Latency(LatencySubcommand::Latest))),
                "HISTORY" => {
                    let ev = if args.len() > 2 {
                        String::from_utf8_lossy(&args[2]).to_string()
                    } else {
                        String::new()
                    };
                    Ok(Some(Command::Latency(LatencySubcommand::History(ev))))
                }
                "DOCTOR" => Ok(Some(Command::Latency(LatencySubcommand::Doctor))),
                "RESET" => {
                    let events = args[2..]
                        .iter()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .collect();
                    Ok(Some(Command::Latency(LatencySubcommand::Reset(events))))
                }
                "GRAPH" => {
                    let ev = if args.len() > 2 {
                        String::from_utf8_lossy(&args[2]).to_string()
                    } else {
                        String::new()
                    };
                    Ok(Some(Command::Latency(LatencySubcommand::Graph(ev))))
                }
                "HISTOGRAM" => {
                    let cmds = args[2..]
                        .iter()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .collect();
                    Ok(Some(Command::Latency(LatencySubcommand::Histogram(cmds))))
                }
                "HELP" => {
                    if args.len() > 2 {
                        return Err(
                            "wrong number of arguments for 'latency|help' command".to_string()
                        );
                    }
                    Ok(Some(Command::Latency(LatencySubcommand::Help)))
                }
                _ => Ok(Some(Command::Unknown(format!("LATENCY {}", sub)))),
            }
        }
        "COMMAND" => {
            if args.len() == 1 {
                // COMMAND is COMMAND INFO for every command.
                Ok(Some(Command::CommandInfo(Vec::new())))
            } else {
                let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
                match sub.as_str() {
                    "COUNT" => {
                        if args.len() != 2 {
                            return Err(
                                "wrong number of arguments for 'command|count' command".to_string()
                            );
                        }
                        Ok(Some(Command::CommandCount))
                    }
                    "LIST" => {
                        if args.len() == 2 {
                            Ok(Some(Command::CommandList))
                        } else if args.len() == 5 && args[2].eq_ignore_ascii_case(b"FILTERBY") {
                            let ftype = String::from_utf8_lossy(&args[3]).to_uppercase();
                            if ftype != "MODULE" && ftype != "ACLCAT" && ftype != "PATTERN" {
                                return Err("syntax error".to_string());
                            }
                            let fval = String::from_utf8_lossy(&args[4]).to_string();
                            Ok(Some(Command::CommandListFiltered {
                                filter_type: ftype,
                                filter_val: fval,
                            }))
                        } else {
                            Err("syntax error".to_string())
                        }
                    }
                    "GETKEYS" => {
                        if args.len() < 3 {
                            return Err("wrong number of arguments for 'command|getkeys' command"
                                .to_string());
                        }
                        Ok(Some(Command::CommandGetkeys(args[2..].to_vec())))
                    }
                    "GETKEYSANDFLAGS" => {
                        if args.len() < 3 {
                            return Err(
                                "wrong number of arguments for 'command|getkeysandflags' command"
                                    .to_string(),
                            );
                        }
                        Ok(Some(Command::CommandGetkeysAndFlags(args[2..].to_vec())))
                    }
                    "INFO" => {
                        let cmds = args[2..]
                            .iter()
                            .map(|b| String::from_utf8_lossy(b).to_lowercase())
                            .collect();
                        Ok(Some(Command::CommandInfo(cmds)))
                    }
                    "DOCS" => {
                        let cmds = args[2..]
                            .iter()
                            .map(|b| String::from_utf8_lossy(b).to_lowercase())
                            .collect();
                        Ok(Some(Command::CommandDocs(cmds)))
                    }
                    "HELP" => Ok(Some(Command::Unknown("COMMAND HELP".to_string()))),
                    _ => Err(format!(
                        "unknown subcommand '{}'. Try COMMAND HELP.",
                        String::from_utf8_lossy(&args[1])
                    )),
                }
            }
        }
        "INFO" => {
            let section = if args.len() == 2 {
                Some(args[1].clone())
            } else if args.len() > 2 {
                let mut joined = Vec::new();
                for (i, a) in args[1..].iter().enumerate() {
                    if i > 0 {
                        joined.push(b' ');
                    }
                    joined.extend_from_slice(a);
                }
                Some(Bytes::from(joined))
            } else {
                None
            };
            Ok(Some(Command::Info(section)))
        }
        "REPLICAOF" | "SLAVEOF" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'replicaof' command".to_string());
            }
            Ok(Some(Command::Replicaof {
                host: args[1].clone(),
                port: args[2].clone(),
            }))
        }
        "PSYNC" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'psync' command".to_string());
            }
            let replid = args[1].clone();
            let offset = match std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(o) => o,
                None => return Err("ERR value is not an integer or out of range".to_string()),
            };
            Ok(Some(Command::Psync { replid, offset }))
        }
        "SYNC" => {
            if args.len() != 1 {
                return Err("wrong number of arguments for 'sync' command".to_string());
            }
            Ok(Some(Command::Sync))
        }
        "REPLCONF" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'replconf' command".to_string());
            }
            Ok(Some(Command::Replconf(args[1..].to_vec())))
        }
        "ROLE" => Ok(Some(Command::Role)),
        "EVAL" | "EVAL_RO" => {
            let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
            let read_only = cmd_name == "EVAL_RO";
            if args.len() < 3 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let script = args[1].clone();
            let numkeys_str = std::str::from_utf8(&args[2]).unwrap_or("");
            if numkeys_str.starts_with('-') {
                return Err("ERR Number of keys can't be negative".to_string());
            }
            let numkeys: usize = match numkeys_str.parse::<usize>() {
                Ok(n) => n,
                Err(_) => return Err("ERR value is not an integer or out of range".to_string()),
            };
            if numkeys > args.len() - 3 {
                return Err("ERR Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let script_args = args[3 + numkeys..].to_vec();
            Ok(Some(Command::Eval {
                script,
                keys,
                args: script_args,
                read_only,
                auth_user: String::new(),
            }))
        }
        "EVALSHA" | "EVALSHA_RO" => {
            let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
            let read_only = cmd_name == "EVALSHA_RO";
            if args.len() < 3 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let sha = args[1].clone();
            let numkeys_str = std::str::from_utf8(&args[2]).unwrap_or("");
            if numkeys_str.starts_with('-') {
                return Err("ERR Number of keys can't be negative".to_string());
            }
            let numkeys: usize = match numkeys_str.parse::<usize>() {
                Ok(n) => n,
                Err(_) => return Err("ERR value is not an integer or out of range".to_string()),
            };
            if numkeys > args.len() - 3 {
                return Err("ERR Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let script_args = args[3 + numkeys..].to_vec();
            Ok(Some(Command::Evalsha {
                sha,
                keys,
                args: script_args,
                read_only,
                auth_user: String::new(),
            }))
        }
        "SCRIPT" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'script' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "LOAD" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'script|load' command".to_string()
                        );
                    }
                    Ok(Some(Command::ScriptLoad(args[2].clone())))
                }
                "EXISTS" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'script|exists' command".to_string()
                        );
                    }
                    Ok(Some(Command::ScriptExists(args[2..].to_vec())))
                }
                "FLUSH" => Ok(Some(Command::ScriptFlush)),
                "KILL" if args.len() == 2 => Ok(Some(Command::ScriptKill)),
                _ => Err(format!(
                    "ERR Unknown SCRIPT subcommand or wrong number of arguments for '{}'",
                    sub
                )),
            }
        }
        "TIER" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'tier' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "SPILL" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'tier spill' command".to_string()
                        );
                    }
                    Ok(Some(Command::Tier(TierSubcommand::Spill(args[2].clone()))))
                }
                "LOAD" | "PROMOTE" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'tier load' command".to_string());
                    }
                    Ok(Some(Command::Tier(TierSubcommand::Load(args[2].clone()))))
                }
                "COOL" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'tier cool' command".to_string());
                    }
                    Ok(Some(Command::Tier(TierSubcommand::Cool(args[2].clone()))))
                }
                "DECOMMIT" => {
                    if args.len() == 2 {
                        Ok(Some(Command::Tier(TierSubcommand::Decommit(None))))
                    } else if args.len() == 3 {
                        Ok(Some(Command::Tier(TierSubcommand::Decommit(Some(
                            args[2].clone(),
                        )))))
                    } else {
                        Err("wrong number of arguments for 'tier decommit' command".to_string())
                    }
                }
                "INFO" => Ok(Some(Command::Tier(TierSubcommand::Info))),
                "SPILLALL" => Ok(Some(Command::Tier(TierSubcommand::SpillAll))),
                "GC" => Ok(Some(Command::Tier(TierSubcommand::Gc))),
                "SNAPSHOT" | "BACKUP" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'tier snapshot' command".to_string()
                        );
                    }
                    let dir = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Tier(TierSubcommand::Snapshot(
                        std::path::PathBuf::from(dir),
                    ))))
                }
                _ => Err(format!(
                    "ERR unknown subcommand '{}'. Try TIER SPILL, TIER COOL, TIER DECOMMIT, TIER LOAD, TIER INFO, TIER SPILLALL, TIER GC, TIER SNAPSHOT.",
                    sub
                )),
            }
        }
        "DFLYCLUSTER" => {
            if args.len() < 2 {
                return Err("ERR wrong number of arguments for 'dflycluster' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "MYID" => Ok(Some(Command::DflyCluster(DflyClusterSubcommand::MyId))),
                "CONFIG" => {
                    if args.len() != 3 {
                        return Err(
                            "ERR wrong number of arguments for 'dflycluster config' command"
                                .to_string(),
                        );
                    }
                    let json = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::DflyCluster(DflyClusterSubcommand::Config(
                        json,
                    ))))
                }
                "GETSLOTINFO" => {
                    if args.len() < 4 || !args[2].eq_ignore_ascii_case(b"slots") {
                        return Err("ERR syntax error, expected DFLYCLUSTER GETSLOTINFO SLOTS slot1 [slot2 ...]".to_string());
                    }
                    let mut slots = Vec::new();
                    for a in &args[3..] {
                        let s: u16 = std::str::from_utf8(a)
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "ERR Invalid slot id".to_string())?;
                        slots.push(s);
                    }
                    Ok(Some(Command::DflyCluster(
                        DflyClusterSubcommand::GetSlotInfo(slots),
                    )))
                }
                "FLUSHSLOTS" => {
                    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                        return Err("ERR syntax error, expected DFLYCLUSTER FLUSHSLOTS start end [start end ...]".to_string());
                    }
                    let mut ranges = Vec::new();
                    for chunk in args[2..].chunks(2) {
                        let s: u16 = std::str::from_utf8(&chunk[0])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "ERR Invalid start slot".to_string())?;
                        let e: u16 = std::str::from_utf8(&chunk[1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "ERR Invalid end slot".to_string())?;
                        if s > e || e >= 16384 {
                            return Err("ERR Slot out of range".to_string());
                        }
                        ranges.push((s, e));
                    }
                    Ok(Some(Command::DflyCluster(
                        DflyClusterSubcommand::FlushSlots(ranges),
                    )))
                }
                "SLOT-MIGRATION-STATUS" => Ok(Some(Command::DflyCluster(
                    DflyClusterSubcommand::SlotMigrationStatus,
                ))),
                _ => Err(format!("ERR unknown subcommand '{}' for DFLYCLUSTER", sub)),
            }
        }
        "DFLY" => {
            if args.len() < 2 {
                return Err("ERR wrong number of arguments for 'dfly' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            if sub == "FLOW" {
                if args.len() < 5 {
                    return Err("ERR wrong number of arguments for 'dfly flow' command".to_string());
                }
                let master_replid = String::from_utf8_lossy(&args[2]).to_string();
                let sync_id = String::from_utf8_lossy(&args[3]).to_string();
                let shard_id: usize = String::from_utf8_lossy(&args[4])
                    .parse()
                    .map_err(|_| "ERR invalid shard id".to_string())?;
                let lsn = if args.len() > 5 {
                    String::from_utf8_lossy(&args[5]).parse::<u64>().ok()
                } else {
                    None
                };
                Ok(Some(Command::DflyFlow {
                    master_replid,
                    sync_id,
                    shard_id,
                    lsn,
                }))
            } else {
                Ok(Some(Command::Unknown(format!("DFLY {}", sub))))
            }
        }
        "DFLYMIGRATE" => {
            if args.len() < 2 {
                return Err("ERR wrong number of arguments for 'dflymigrate' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "INIT" => {
                    if args.len() < 5 {
                        return Err(
                            "ERR wrong number of arguments for 'dflymigrate init' command"
                                .to_string(),
                        );
                    }
                    let source_id = String::from_utf8_lossy(&args[2]).to_string();
                    let num_shards = std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "ERR Invalid num_shards".to_string())?;
                    let mut slots = Vec::new();
                    for chunk in args[4..].chunks(2) {
                        if chunk.len() == 2 {
                            let s: u16 = std::str::from_utf8(&chunk[0])
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0);
                            let e: u16 = std::str::from_utf8(&chunk[1])
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0);
                            slots.push((s, e));
                        }
                    }
                    Ok(Some(Command::DflyMigrate(DflyMigrateSubcommand::Init {
                        source_id,
                        num_shards,
                        slots,
                    })))
                }
                "FLOW" => {
                    if args.len() != 4 {
                        return Err(
                            "ERR wrong number of arguments for 'dflymigrate flow' command"
                                .to_string(),
                        );
                    }
                    let source_id = String::from_utf8_lossy(&args[2]).to_string();
                    let flow_id = std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "ERR Invalid flow_id".to_string())?;
                    Ok(Some(Command::DflyMigrate(DflyMigrateSubcommand::Flow {
                        source_id,
                        flow_id,
                    })))
                }
                "ACK" => {
                    if args.len() != 3 {
                        return Err(
                            "ERR wrong number of arguments for 'dflymigrate ack' command"
                                .to_string(),
                        );
                    }
                    let flow_id = std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "ERR Invalid flow_id".to_string())?;
                    Ok(Some(Command::DflyMigrate(DflyMigrateSubcommand::Ack {
                        flow_id,
                    })))
                }
                _ => Err(format!("ERR unknown subcommand '{}' for DFLYMIGRATE", sub)),
            }
        }
        "STICK" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'stick' command".to_string());
            }
            Ok(Some(Command::Stick(args[1..].to_vec())))
        }
        "UNSTICK" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'unstick' command".to_string());
            }
            Ok(Some(Command::Unstick(args[1..].to_vec())))
        }
        "STICKY" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'sticky' command".to_string());
            }
            Ok(Some(Command::Sticky(args[1].clone())))
        }
        "DELEX" => {
            if args.len() != 2 && args.len() != 4 {
                return Err("wrong number of arguments for 'delex' command".to_string());
            }
            let key = args[1].clone();
            let condition = if args.len() == 4 {
                let op = String::from_utf8_lossy(&args[2]).to_uppercase();
                match op.as_str() {
                    "IFEQ" | "IFNE" | "IFDEQ" | "IFDNE" | "IFGT" | "IFLT" => {
                        let expected = args[3].clone();
                        Some((op, expected))
                    }
                    _ => return Err("Invalid condition for 'delex' command".to_string()),
                }
            } else {
                None
            };
            Ok(Some(Command::Delex { key, condition }))
        }
        "STATS" => Ok(Some(Command::MemcachedStats)),
        "VERSION" => Ok(Some(Command::MemcachedVersion)),
        "CONFIG" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'config' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "GET" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'config|get' command".to_string()
                        );
                    }
                    Ok(Some(Command::ConfigGet(args[2..].to_vec())))
                }
                "SET" => {
                    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                        return Err(
                            "wrong number of arguments for 'config|set' command".to_string()
                        );
                    }
                    let mut pairs = Vec::with_capacity((args.len() - 2) / 2);
                    let mut i = 2;
                    while i < args.len() {
                        let mut v = args[i + 1].clone();
                        if v.len() >= 2
                            && ((v.starts_with(b"\"") && v.ends_with(b"\""))
                                || (v.starts_with(b"'") && v.ends_with(b"'")))
                        {
                            v = v.slice(1..v.len() - 1);
                        }
                        pairs.push((args[i].clone(), v));
                        i += 2;
                    }
                    Ok(Some(Command::ConfigSet(pairs)))
                }
                "RESETSTAT" => Ok(Some(Command::ConfigSet(vec![(
                    Bytes::from_static(b"resetstat"),
                    Bytes::new(),
                )]))),
                "REWRITE" => Ok(Some(Command::ConfigSet(vec![(
                    Bytes::from_static(b"rewrite"),
                    Bytes::new(),
                )]))),
                "HELP" => Ok(Some(Command::Unknown("CONFIG HELP".to_string()))),
                _ => Err(format!("ERR unknown subcommand '{}' for CONFIG", sub)),
            }
        }
        "QUIT" => Ok(Some(Command::Quit)),
        "SHUTDOWN" => {
            let save = if args.len() > 1 {
                let arg = String::from_utf8_lossy(&args[1]).to_uppercase();
                if arg == "SAVE" {
                    Some(true)
                } else if arg == "NOSAVE" {
                    Some(false)
                } else {
                    None
                }
            } else {
                None
            };
            Ok(Some(Command::Shutdown { save }))
        }
        "SUBSCRIBE" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'subscribe' command".to_string());
            }
            Ok(Some(Command::Subscribe(args[1..].to_vec())))
        }
        "UNSUBSCRIBE" => Ok(Some(Command::Unsubscribe(args[1..].to_vec()))),
        "PSUBSCRIBE" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'psubscribe' command".to_string());
            }
            Ok(Some(Command::Psubscribe(args[1..].to_vec())))
        }
        "PUNSUBSCRIBE" => Ok(Some(Command::Punsubscribe(args[1..].to_vec()))),
        "PUBLISH" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'publish' command".to_string());
            }
            Ok(Some(Command::Publish {
                channel: args[1].clone(),
                message: args[2].clone(),
            }))
        }
        "SPUBLISH" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'spublish' command".to_string());
            }
            Ok(Some(Command::Spublish {
                channel: args[1].clone(),
                message: args[2].clone(),
            }))
        }
        "SSUBSCRIBE" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'ssubscribe' command".to_string());
            }
            Ok(Some(Command::Ssubscribe(args[1..].to_vec())))
        }
        "SUNSUBSCRIBE" => Ok(Some(Command::Sunsubscribe(args[1..].to_vec()))),
        "PUBSUB" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'pubsub' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "CHANNELS" => {
                    let pat = if args.len() > 2 {
                        Some(args[2].clone())
                    } else {
                        None
                    };
                    Ok(Some(Command::PubsubChannels(pat)))
                }
                "NUMSUB" => {
                    let channels = if args.len() > 2 {
                        args[2..].to_vec()
                    } else {
                        Vec::new()
                    };
                    Ok(Some(Command::PubsubNumsub(channels)))
                }
                "NUMPAT" => Ok(Some(Command::PubsubNumpat)),
                "SHARDCHANNELS" => {
                    let pat = if args.len() > 2 {
                        Some(args[2].clone())
                    } else {
                        None
                    };
                    Ok(Some(Command::PubsubShardchannels(pat)))
                }
                "SHARDNUMSUB" => {
                    let channels = if args.len() > 2 {
                        args[2..].to_vec()
                    } else {
                        Vec::new()
                    };
                    Ok(Some(Command::PubsubShardnumsub(channels)))
                }
                "HELP" => Ok(Some(Command::PubsubHelp)),
                _ => Ok(Some(Command::Unknown(format!("PUBSUB {}", sub)))),
            }
        }
        "KEYS" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'keys' command".to_string());
            }
            Ok(Some(Command::Keys(args[1].clone())))
        }
        "SCAN" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'scan' command".to_string());
            }
            let cursor: u64 = std::str::from_utf8(&args[1])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            let mut pattern = None;
            let mut count = None;
            let mut key_type = None;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "MATCH" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        pattern = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let cnt: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        count = Some(cnt);
                        i += 2;
                    }
                    "TYPE" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        key_type = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Scan {
                cursor,
                pattern,
                count,
                key_type,
            }))
        }
        "RANDOMKEY" => {
            if args.len() != 1 {
                return Err("wrong number of arguments for 'randomkey' command".to_string());
            }
            Ok(Some(Command::Randomkey))
        }
        "EXPIRETIME" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'expiretime' command".to_string());
            }
            Ok(Some(Command::Expiretime(args[1].clone(), false)))
        }
        "PEXPIRETIME" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'pexpiretime' command".to_string());
            }
            Ok(Some(Command::Expiretime(args[1].clone(), true)))
        }
        "MULTI" => {
            if args.len() != 1 {
                return Err("wrong number of arguments for 'multi' command".to_string());
            }
            Ok(Some(Command::Multi))
        }
        "EXEC" => {
            if args.len() != 1 {
                return Err("wrong number of arguments for 'exec' command".to_string());
            }
            Ok(Some(Command::Exec))
        }
        "DISCARD" => {
            if args.len() != 1 {
                return Err("wrong number of arguments for 'discard' command".to_string());
            }
            Ok(Some(Command::Discard))
        }
        "WATCH" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'watch' command".to_string());
            }
            Ok(Some(Command::Watch(args[1..].to_vec())))
        }
        "UNWATCH" => {
            if args.len() != 1 {
                return Err("wrong number of arguments for 'unwatch' command".to_string());
            }
            Ok(Some(Command::Unwatch))
        }
        "SETBIT" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'setbit' command".to_string());
            }
            let offset_raw: u64 = std::str::from_utf8(&args[2])
                .map_err(|_| "bit offset is not an integer or out of range")?
                .parse()
                .map_err(|_| "bit offset is not an integer or out of range")?;
            if (offset_raw >> 3) >= get_proto_max_bulk_len() as u64 {
                return Err("bit offset is not an integer or out of range".to_string());
            }
            let offset = offset_raw as usize;
            let val_str = std::str::from_utf8(&args[3])
                .map_err(|_| "bit is not an integer or out of range")?;
            let val_num: i64 = val_str
                .parse()
                .map_err(|_| "bit is not an integer or out of range")?;
            if val_num != 0 && val_num != 1 {
                return Err("bit is not an integer or out of range".to_string());
            }
            let value = val_num as u8;
            Ok(Some(Command::Setbit {
                key: args[1].clone(),
                offset,
                value,
            }))
        }
        "GETBIT" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'getbit' command".to_string());
            }
            let offset_raw: u64 = std::str::from_utf8(&args[2])
                .map_err(|_| "bit offset is not an integer or out of range")?
                .parse()
                .map_err(|_| "bit offset is not an integer or out of range")?;
            if (offset_raw >> 3) >= get_proto_max_bulk_len() as u64 {
                return Err("bit offset is not an integer or out of range".to_string());
            }
            let offset = offset_raw as usize;
            Ok(Some(Command::Getbit {
                key: args[1].clone(),
                offset,
            }))
        }
        "BITCOUNT" => {
            if args.len() != 2 && args.len() != 4 && args.len() != 5 {
                if args.len() < 2 {
                    return Err("wrong number of arguments for 'bitcount' command".to_string());
                }
                return Err("syntax error".to_string());
            }
            let mut start = None;
            let mut end = None;
            let mut is_bit = false;
            if args.len() >= 4 {
                start = Some(
                    std::str::from_utf8(&args[2])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range")?,
                );
                end = Some(
                    std::str::from_utf8(&args[3])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range")?,
                );
            }
            if args.len() == 5 {
                if args[4].eq_ignore_ascii_case(b"BIT") {
                    is_bit = true;
                } else if args[4].eq_ignore_ascii_case(b"BYTE") {
                    is_bit = false;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            Ok(Some(Command::Bitcount {
                key: args[1].clone(),
                start,
                end,
                is_bit,
            }))
        }
        "BITPOS" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'bitpos' command".to_string());
            }
            if args.len() > 6 {
                return Err("syntax error".to_string());
            }
            let bit_str = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range")?;
            let bit_val: i64 = bit_str
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            if bit_val != 0 && bit_val != 1 {
                return Err("The bit argument must be 1 or 0.".to_string());
            }
            let bit = bit_val as u8;

            let mut is_bit = false;
            let start = if args.len() >= 4 {
                Some(
                    std::str::from_utf8(&args[3])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse::<i64>()
                        .map_err(|_| "value is not an integer or out of range")?,
                )
            } else {
                None
            };
            if args.len() == 6 {
                if args[5].eq_ignore_ascii_case(b"BIT") {
                    is_bit = true;
                } else if args[5].eq_ignore_ascii_case(b"BYTE") {
                    is_bit = false;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            let end = if args.len() >= 5 {
                Some(
                    std::str::from_utf8(&args[4])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse::<i64>()
                        .map_err(|_| "value is not an integer or out of range")?,
                )
            } else {
                None
            };
            Ok(Some(Command::Bitpos {
                key: args[1].clone(),
                bit,
                start,
                end,
                is_bit,
            }))
        }
        "BITFIELD" | "BITFIELD_RO" => {
            let is_ro = cmd_name.eq_ignore_ascii_case("BITFIELD_RO");
            if args.len() < 2 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    if is_ro { "bitfield_ro" } else { "bitfield" }
                ));
            }
            let key = args[1].clone();
            let mut ops = Vec::new();
            let mut cur_overflow = BitfieldOverflow::Wrap;
            let mut j = 2;

            while j < args.len() {
                let subcmd =
                    std::str::from_utf8(&args[j]).map_err(|_| "syntax error".to_string())?;
                if subcmd.eq_ignore_ascii_case("OVERFLOW") {
                    if j + 1 >= args.len() {
                        return Err("syntax error".to_string());
                    }
                    let ow_type = std::str::from_utf8(&args[j + 1])
                        .map_err(|_| "syntax error".to_string())?;
                    if ow_type.eq_ignore_ascii_case("WRAP") {
                        cur_overflow = BitfieldOverflow::Wrap;
                    } else if ow_type.eq_ignore_ascii_case("SAT") {
                        cur_overflow = BitfieldOverflow::Sat;
                    } else if ow_type.eq_ignore_ascii_case("FAIL") {
                        cur_overflow = BitfieldOverflow::Fail;
                    } else {
                        return Err("Invalid OVERFLOW type specified".to_string());
                    }
                    j += 2;
                    continue;
                }

                let (is_get, is_set, _is_incrby) = if subcmd.eq_ignore_ascii_case("GET") {
                    (true, false, false)
                } else if subcmd.eq_ignore_ascii_case("SET") {
                    (false, true, false)
                } else if subcmd.eq_ignore_ascii_case("INCRBY") {
                    (false, false, true)
                } else {
                    return Err("syntax error".to_string());
                };

                if is_ro && !is_get {
                    return Err("BITFIELD_RO only supports the GET subcommand".to_string());
                }

                let required_tokens = if is_get { 3 } else { 4 };
                if j + required_tokens > args.len() {
                    return Err("syntax error".to_string());
                }

                let type_bytes = &args[j + 1];
                if type_bytes.is_empty() {
                    return Err("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".to_string());
                }
                let (sign, bits) = match type_bytes[0] {
                    b'i' | b'I' => {
                        let s = std::str::from_utf8(&type_bytes[1..]).map_err(|_| "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".to_string())?;
                        let b: usize = s.parse().map_err(|_| "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".to_string())?;
                        if !(1..=64).contains(&b) {
                            return Err("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".to_string());
                        }
                        (true, b)
                    }
                    b'u' | b'U' => {
                        let s = std::str::from_utf8(&type_bytes[1..]).map_err(|_| "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".to_string())?;
                        let b: usize = s.parse().map_err(|_| "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".to_string())?;
                        if !(1..=63).contains(&b) {
                            return Err("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".to_string());
                        }
                        (false, b)
                    }
                    _ => return Err("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".to_string()),
                };

                let offset_bytes = &args[j + 2];
                let (use_hash, num_bytes) = if !offset_bytes.is_empty() && offset_bytes[0] == b'#' {
                    (true, &offset_bytes[1..])
                } else {
                    (false, &offset_bytes[..])
                };
                let offset_str = std::str::from_utf8(num_bytes)
                    .map_err(|_| "bit offset is not an integer or out of range".to_string())?;
                let mut loffset: i64 = offset_str
                    .parse()
                    .map_err(|_| "bit offset is not an integer or out of range".to_string())?;
                if loffset < 0 {
                    return Err("bit offset is not an integer or out of range".to_string());
                }
                if use_hash {
                    if loffset > (i64::MAX / (bits as i64)) {
                        return Err("bit offset is not an integer or out of range".to_string());
                    }
                    loffset *= bits as i64;
                }
                if (loffset as u64 >> 3) >= get_proto_max_bulk_len() as u64 {
                    return Err("bit offset is not an integer or out of range".to_string());
                }
                let offset = loffset as u64;

                let op_type = if is_get {
                    j += 3;
                    BitfieldOpType::Get
                } else if is_set {
                    let val_str = std::str::from_utf8(&args[j + 3])
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    let val: i64 = val_str
                        .parse()
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    j += 4;
                    BitfieldOpType::Set(val)
                } else {
                    let incr_str = std::str::from_utf8(&args[j + 3])
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    let incr: i64 = incr_str
                        .parse()
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    j += 4;
                    BitfieldOpType::Incrby(incr)
                };

                ops.push(BitfieldSubOp {
                    op_type,
                    sign,
                    bits,
                    offset,
                    overflow: cur_overflow,
                });
            }

            Ok(Some(Command::Bitfield {
                key,
                ops,
                readonly: is_ro,
            }))
        }
        "BITOP" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'bitop' command".to_string());
            }
            let op = String::from_utf8_lossy(&args[1]).to_uppercase();
            let destkey = args[2].clone();
            let srckeys = args[3..].to_vec();
            if op == "NOT" && srckeys.len() != 1 {
                return Err("BITOP NOT must be called with a single source key.".to_string());
            }
            if (op == "DIFF" || op == "DIFF1" || op == "ANDOR") && srckeys.len() < 2 {
                return Err(format!("BITOP {} requires at least 2 source keys", op));
            }
            match op.as_str() {
                "AND" | "OR" | "XOR" | "NOT" | "DIFF" | "DIFF1" | "ANDOR" | "ONE" => {
                    Ok(Some(Command::Bitop {
                        op,
                        destkey,
                        srckeys,
                    }))
                }
                _ => Err("syntax error".to_string()),
            }
        }
        "PFADD" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'pfadd' command".to_string());
            }
            let key = args[1].clone();
            let elements = args[2..].to_vec();
            Ok(Some(Command::Pfadd { key, elements }))
        }
        "PFCOUNT" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'pfcount' command".to_string());
            }
            let keys = args[1..].to_vec();
            Ok(Some(Command::Pfcount { keys }))
        }
        "PFMERGE" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'pfmerge' command".to_string());
            }
            let destkey = args[1].clone();
            let srckeys = args[2..].to_vec();
            Ok(Some(Command::Pfmerge { destkey, srckeys }))
        }
        "PFSELFTEST" => Ok(Some(Command::Pfselftest)),
        "PFDEBUG" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'pfdebug' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "GETREG" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'pfdebug' command".to_string());
                    }
                    Ok(Some(Command::PfdebugGetreg(args[2].clone())))
                }
                "ENCODING" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'pfdebug' command".to_string());
                    }
                    Ok(Some(Command::PfdebugEncoding(args[2].clone())))
                }
                "TODENSE" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'pfdebug' command".to_string());
                    }
                    Ok(Some(Command::PfdebugTodense(args[2].clone())))
                }
                "SIMD" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'pfdebug' command".to_string());
                    }
                    let on = matches!(
                        args[2].to_ascii_lowercase().as_slice(),
                        b"on" | b"1" | b"yes"
                    );
                    Ok(Some(Command::PfdebugSimd(on)))
                }
                _ => Err("unknown subcommand for 'pfdebug' command".to_string()),
            }
        }
        "DUMP" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'dump' command".to_string());
            }
            Ok(Some(Command::Dump(args[1].clone())))
        }
        "RESTORE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'restore' command".to_string());
            }
            let key = args[1].clone();
            let ttl_ms: u64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            let serialized = args[3].clone();
            let mut replace = false;
            let mut absttl = false;
            let mut i = 4;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "REPLACE" => {
                        replace = true;
                        i += 1;
                    }
                    "ABSTTL" => {
                        absttl = true;
                        i += 1;
                    }
                    "IDLETIME" | "FREQ" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Restore {
                key,
                ttl_ms,
                serialized,
                replace,
                absttl,
            }))
        }
        "XADD" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'xadd' command".to_string());
            }
            let key = args[1].clone();
            let mut nomkstream = false;
            let mut maxlen = None;
            let mut minid = None;
            let mut approx = false;
            let mut limit = None;
            let mut limit_seen = false;
            let mut trim_strategy = crate::table::StreamTrimStrategy::KeepRef;
            let mut trim_strategy_seen = false;
            let mut idmp = None;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "NOMKSTREAM" => {
                        nomkstream = true;
                        i += 1;
                    }
                    "MAXLEN" => {
                        i += 1;
                        if i < args.len() {
                            if args[i].as_ref() == b"=" {
                                i += 1;
                            } else if args[i].as_ref() == b"~" {
                                approx = true;
                                i += 1;
                            }
                        }
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let len: usize = std::str::from_utf8(&args[i])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        maxlen = Some(len);
                        i += 1;
                    }
                    "MINID" => {
                        i += 1;
                        if i < args.len() {
                            if args[i].as_ref() == b"=" {
                                i += 1;
                            } else if args[i].as_ref() == b"~" {
                                approx = true;
                                i += 1;
                            }
                        }
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let id_str = std::str::from_utf8(&args[i]).map_err(
                            |_| "Invalid stream ID specified as stream command argument",
                        )?;
                        let parsed_id = crate::table::StreamId::parse_exact(id_str)
                            .map_err(|e| e.to_string())?;
                        minid = Some(parsed_id);
                        i += 1;
                    }
                    "LIMIT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let l_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?;
                        let l: i64 = l_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if l < 0 {
                            return Err("ERR The LIMIT argument must be >= 0.".to_string());
                        }
                        limit = Some(l as usize);
                        limit_seen = true;
                        i += 2;
                    }
                    "ACKED" => {
                        if trim_strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        trim_strategy = crate::table::StreamTrimStrategy::Acked;
                        trim_strategy_seen = true;
                        i += 1;
                    }
                    "DELREF" => {
                        if trim_strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        trim_strategy = crate::table::StreamTrimStrategy::DelRef;
                        trim_strategy_seen = true;
                        i += 1;
                    }
                    "KEEPREF" => {
                        if trim_strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        trim_strategy = crate::table::StreamTrimStrategy::KeepRef;
                        trim_strategy_seen = true;
                        i += 1;
                    }
                    "IDMP" => {
                        if idmp.is_some() {
                            return Err("ERR IDMP/IDMPAUTO specified multiple times".to_string());
                        }
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let producer = args[i + 1].clone();
                        if producer.is_empty() {
                            return Err("ERR IDMP requires a non-empty producer ID".to_string());
                        }
                        let iid = args[i + 2].clone();
                        if iid.is_empty() {
                            return Err("ERR IDMP requires a non-empty idempotent ID".to_string());
                        }
                        idmp = Some(crate::table::StreamIdmpOption::Manual { producer, iid });
                        i += 3;
                    }
                    "IDMPAUTO" => {
                        if idmp.is_some() {
                            return Err("ERR IDMP/IDMPAUTO specified multiple times".to_string());
                        }
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let producer = args[i + 1].clone();
                        if producer.is_empty() {
                            return Err("ERR IDMPAUTO requires a non-empty producer ID".to_string());
                        }
                        idmp = Some(crate::table::StreamIdmpOption::Auto { producer });
                        i += 2;
                    }
                    _ => break,
                }
            }
            if limit_seen && maxlen.is_none() && minid.is_none() {
                return Err(
                    "syntax error, LIMIT cannot be used without specifying a trimming strategy"
                        .to_string(),
                );
            }
            if limit_seen && !approx {
                return Err(
                    "syntax error, LIMIT cannot be used without the special ~ option".to_string(),
                );
            }
            if i >= args.len() {
                return Err("wrong number of arguments for 'xadd' command".to_string());
            }
            let id_str = std::str::from_utf8(&args[i])
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            let id = crate::table::StreamAddId::parse(id_str).map_err(|e| e.to_string())?;
            if idmp.is_some() && !matches!(id, crate::table::StreamAddId::Auto) {
                return Err(
                    "ERR IDMP/IDMPAUTO can be used only with auto-generated IDs".to_string()
                );
            }
            i += 1;
            let rem = args.len() - i;
            if rem == 0 || !rem.is_multiple_of(2) {
                return Err("wrong number of arguments for 'xadd' command".to_string());
            }
            let mut fields = Vec::with_capacity(rem / 2);
            while i < args.len() {
                fields.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            }
            Ok(Some(Command::Xadd {
                key,
                nomkstream,
                maxlen,
                minid,
                approx,
                trim_strategy,
                idmp,
                id,
                fields,
                limit,
            }))
        }
        "XLEN" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'xlen' command".to_string());
            }
            Ok(Some(Command::Xlen(args[1].clone())))
        }
        "XRANGE" => {
            if args.len() < 4 || args.len() > 6 {
                return Err("wrong number of arguments for 'xrange' command".to_string());
            }
            let key = args[1].clone();
            let start = String::from_utf8_lossy(&args[2]).to_string();
            let end = String::from_utf8_lossy(&args[3]).to_string();
            let mut count = None;
            if args.len() == 6 {
                let opt = String::from_utf8_lossy(&args[4]).to_uppercase();
                if opt != "COUNT" {
                    return Err("syntax error".to_string());
                }
                let cnt: usize = std::str::from_utf8(&args[5])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                count = Some(cnt);
            } else if args.len() == 5 {
                return Err("syntax error".to_string());
            }
            Ok(Some(Command::Xrange {
                key,
                start,
                end,
                count,
            }))
        }
        "XREVRANGE" => {
            if args.len() < 4 || args.len() > 6 {
                return Err("wrong number of arguments for 'xrevrange' command".to_string());
            }
            let key = args[1].clone();
            let end = String::from_utf8_lossy(&args[2]).to_string();
            let start = String::from_utf8_lossy(&args[3]).to_string();
            let mut count = None;
            if args.len() == 6 {
                let opt = String::from_utf8_lossy(&args[4]).to_uppercase();
                if opt != "COUNT" {
                    return Err("syntax error".to_string());
                }
                let cnt: usize = std::str::from_utf8(&args[5])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                count = Some(cnt);
            } else if args.len() == 5 {
                return Err("syntax error".to_string());
            }
            Ok(Some(Command::Xrevrange {
                key,
                end,
                start,
                count,
            }))
        }
        "XREAD" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'xread' command".to_string());
            }
            let mut count = None;
            let mut maxcount = None;
            let mut maxsize = None;
            let mut block_ms = None;
            let mut i = 1;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let cnt: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        count = Some(cnt);
                        i += 2;
                    }
                    "MAXCOUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let mc: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if mc <= 0 {
                            return Err("ERR MAXCOUNT must be a positive integer".to_string());
                        }
                        maxcount = Some(mc as usize);
                        i += 2;
                    }
                    "MAXSIZE" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ms: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if ms <= 0 {
                            return Err("ERR MAXSIZE must be a positive integer".to_string());
                        }
                        maxsize = Some(ms as usize);
                        i += 2;
                    }
                    "BLOCK" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let b: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        block_ms = Some(b);
                        i += 2;
                    }
                    "STREAMS" => {
                        i += 1;
                        break;
                    }
                    "CLAIM" => {
                        return Err(
                            "ERR The CLAIM option is only supported with XREADGROUP".to_string()
                        );
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            if let (Some(c), Some(mc)) = (count, maxcount)
                && mc < c
            {
                return Err("ERR MAXCOUNT must be greater than or equal to COUNT".to_string());
            }
            let rem = args.len() - i;
            if rem < 2 || !rem.is_multiple_of(2) {
                return Err("ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.".to_string());
            }
            let n = rem / 2;
            let keys: Vec<Bytes> = args[i..i + n].to_vec();
            let ids: Vec<String> = args[i + n..]
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect();
            Ok(Some(Command::Xread {
                count,
                maxcount,
                maxsize,
                block_ms,
                keys,
                ids,
            }))
        }
        "XDEL" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'xdel' command".to_string());
            }
            let key = args[1].clone();
            let mut ids = Vec::with_capacity(args.len() - 2);
            for arg in &args[2..] {
                let s = std::str::from_utf8(arg)
                    .map_err(|_| "Invalid stream ID specified as stream command argument")?;
                let id = crate::table::StreamId::parse_exact(s).map_err(|e| e.to_string())?;
                ids.push(id);
            }
            Ok(Some(Command::Xdel { key, ids }))
        }
        "XTRIM" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'xtrim' command".to_string());
            }
            let key = args[1].clone();
            let mut maxlen = None;
            let mut minid = None;
            let mut approx = false;
            let mut limit = None;
            let mut limit_seen = false;
            let mut trim_strategy = crate::table::StreamTrimStrategy::KeepRef;
            let mut trim_strategy_seen = false;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "MAXLEN" => {
                        i += 1;
                        if i < args.len() {
                            if args[i].as_ref() == b"=" {
                                i += 1;
                            } else if args[i].as_ref() == b"~" {
                                approx = true;
                                i += 1;
                            }
                        }
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let len: usize = std::str::from_utf8(&args[i])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        maxlen = Some(len);
                        i += 1;
                    }
                    "MINID" => {
                        i += 1;
                        if i < args.len() {
                            if args[i].as_ref() == b"=" {
                                i += 1;
                            } else if args[i].as_ref() == b"~" {
                                approx = true;
                                i += 1;
                            }
                        }
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let id_str = std::str::from_utf8(&args[i]).map_err(
                            |_| "Invalid stream ID specified as stream command argument",
                        )?;
                        let parsed_id = crate::table::StreamId::parse_exact(id_str)
                            .map_err(|e| e.to_string())?;
                        minid = Some(parsed_id);
                        i += 1;
                    }
                    "LIMIT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let l_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?;
                        let l: i64 = l_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if l < 0 {
                            return Err("ERR The LIMIT argument must be >= 0.".to_string());
                        }
                        limit = Some(l as usize);
                        limit_seen = true;
                        i += 2;
                    }
                    "ACKED" => {
                        if trim_strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        trim_strategy = crate::table::StreamTrimStrategy::Acked;
                        trim_strategy_seen = true;
                        i += 1;
                    }
                    "DELREF" => {
                        if trim_strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        trim_strategy = crate::table::StreamTrimStrategy::DelRef;
                        trim_strategy_seen = true;
                        i += 1;
                    }
                    "KEEPREF" => {
                        if trim_strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        trim_strategy = crate::table::StreamTrimStrategy::KeepRef;
                        trim_strategy_seen = true;
                        i += 1;
                    }
                    _ => {
                        i += 1;
                    }
                }
            }
            if limit_seen && maxlen.is_none() && minid.is_none() {
                return Err(
                    "syntax error, LIMIT cannot be used without specifying a trimming strategy"
                        .to_string(),
                );
            }
            if limit_seen && !approx {
                return Err(
                    "syntax error, LIMIT cannot be used without the special ~ option".to_string(),
                );
            }
            Ok(Some(Command::Xtrim {
                key,
                maxlen,
                minid,
                approx,
                trim_strategy,
                limit,
            }))
        }
        "XCFGSET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'xcfgset' command".to_string());
            }
            if args.len() == 2 {
                return Err("ERR At least one parameter must be specified for XCFGSET".to_string());
            }
            let key = args[1].clone();
            let mut duration = None;
            let mut maxsize = None;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "IDMP-DURATION" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "ERR value is not an integer or out of range")?;
                        if s.starts_with("00") || s.contains('.') {
                            return Err("ERR value is not an integer or out of range".to_string());
                        }
                        let val: i64 = s
                            .parse()
                            .map_err(|_| "ERR value is not an integer or out of range")?;
                        if !(1..=86400).contains(&val) {
                            return Err("ERR IDMP-DURATION must be between 1 and 86400".to_string());
                        }
                        duration = Some(val as u64);
                        i += 2;
                    }
                    "IDMP-MAXSIZE" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "ERR value is not an integer or out of range")?;
                        if s.starts_with("00") || s.contains('.') {
                            return Err("ERR value is not an integer or out of range".to_string());
                        }
                        let val: i64 = s
                            .parse()
                            .map_err(|_| "ERR value is not an integer or out of range")?;
                        if !(1..=10000).contains(&val) {
                            return Err("ERR IDMP-MAXSIZE must be between 1 and 10000".to_string());
                        }
                        maxsize = Some(val as usize);
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Xcfgset {
                key,
                duration,
                maxsize,
            }))
        }
        "XSETID" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'xsetid' command".to_string());
            }
            let key = args[1].clone();
            let last_id_str = std::str::from_utf8(&args[2])
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            let last_id =
                crate::table::StreamId::parse_exact(last_id_str).map_err(|e| e.to_string())?;

            let mut entries_added = None;
            let mut max_deleted_id = None;
            let mut i = 3;
            while i < args.len() {
                let moreargs = (args.len() - 1) - i;
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "ENTRIESADDED" && moreargs > 0 {
                    let ea_str = std::str::from_utf8(&args[i + 1])
                        .map_err(|_| "value is not an integer or out of range")?;
                    let ea_i: i64 = ea_str
                        .parse()
                        .map_err(|_| "value is not an integer or out of range")?;
                    if ea_i < 0 {
                        return Err("ERR entries_added must be positive".to_string());
                    }
                    entries_added = Some(ea_i as u64);
                    i += 2;
                } else if opt == "MAXDELETEDID" && moreargs > 0 {
                    let md_str = std::str::from_utf8(&args[i + 1])
                        .map_err(|_| "Invalid stream ID specified as stream command argument")?;
                    let md =
                        crate::table::StreamId::parse_exact(md_str).map_err(|e| e.to_string())?;
                    if last_id < md {
                        return Err("ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id".to_string());
                    }
                    max_deleted_id = Some(md);
                    i += 2;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            Ok(Some(Command::Xsetid {
                key,
                last_id,
                entries_added,
                max_deleted_id,
            }))
        }
        "XDELEX" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'xdelex' command".to_string());
            }
            let key = args[1].clone();
            let mut strategy = crate::table::StreamTrimStrategy::KeepRef;
            let mut strategy_seen = false;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "KEEPREF" => {
                        if strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        strategy = crate::table::StreamTrimStrategy::KeepRef;
                        strategy_seen = true;
                        i += 1;
                    }
                    "DELREF" => {
                        if strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        strategy = crate::table::StreamTrimStrategy::DelRef;
                        strategy_seen = true;
                        i += 1;
                    }
                    "ACKED" => {
                        if strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        strategy = crate::table::StreamTrimStrategy::Acked;
                        strategy_seen = true;
                        i += 1;
                    }
                    "IDS" => {
                        i += 1;
                        break;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            if i >= args.len() {
                return Err("syntax error".to_string());
            }
            let numids_i: i64 = std::str::from_utf8(&args[i])
                .map_err(|_| "ERR Number of IDs must be a positive integer")?
                .parse()
                .map_err(|_| "ERR Number of IDs must be a positive integer")?;
            if numids_i <= 0 {
                return Err("ERR Number of IDs must be a positive integer".to_string());
            }
            let numids = numids_i as usize;
            i += 1;
            let remaining = args.len() - i;
            if numids > remaining {
                return Err(
                    "ERR The `numids` parameter must match the number of arguments".to_string(),
                );
            }
            if numids < remaining {
                return Err("syntax error".to_string());
            }
            let mut ids = Vec::with_capacity(numids);
            for arg in &args[i..] {
                let s = std::str::from_utf8(arg)
                    .map_err(|_| "Invalid stream ID specified as stream command argument")?;
                let id = crate::table::StreamId::parse_exact(s).map_err(|e| e.to_string())?;
                ids.push(id);
            }
            Ok(Some(Command::Xdelex { key, strategy, ids }))
        }
        "XACKDEL" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'xackdel' command".to_string());
            }
            let key = args[1].clone();
            let group = args[2].clone();
            let mut strategy = crate::table::StreamTrimStrategy::KeepRef;
            let mut strategy_seen = false;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "KEEPREF" => {
                        if strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        strategy = crate::table::StreamTrimStrategy::KeepRef;
                        strategy_seen = true;
                        i += 1;
                    }
                    "DELREF" => {
                        if strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        strategy = crate::table::StreamTrimStrategy::DelRef;
                        strategy_seen = true;
                        i += 1;
                    }
                    "ACKED" => {
                        if strategy_seen {
                            return Err("syntax error".to_string());
                        }
                        strategy = crate::table::StreamTrimStrategy::Acked;
                        strategy_seen = true;
                        i += 1;
                    }
                    "IDS" => {
                        i += 1;
                        break;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            if i >= args.len() {
                return Err("syntax error".to_string());
            }
            let numids_i: i64 = std::str::from_utf8(&args[i])
                .map_err(|_| "ERR Number of IDs must be a positive integer")?
                .parse()
                .map_err(|_| "ERR Number of IDs must be a positive integer")?;
            if numids_i <= 0 {
                return Err("ERR Number of IDs must be a positive integer".to_string());
            }
            let numids = numids_i as usize;
            i += 1;
            let remaining = args.len() - i;
            if numids > remaining {
                return Err(
                    "ERR The `numids` parameter must match the number of arguments".to_string(),
                );
            }
            if numids < remaining {
                return Err("syntax error".to_string());
            }
            let mut ids = Vec::with_capacity(numids);
            for arg in &args[i..] {
                let s = std::str::from_utf8(arg)
                    .map_err(|_| "Invalid stream ID specified as stream command argument")?;
                let id = crate::table::StreamId::parse_exact(s).map_err(|e| e.to_string())?;
                ids.push(id);
            }
            Ok(Some(Command::Xackdel {
                key,
                group,
                strategy,
                ids,
            }))
        }
        "XIDMPRECORD" => {
            if args.len() != 5 {
                return Err("wrong number of arguments for 'xidmprecord' command".to_string());
            }
            let key = args[1].clone();
            let pid = args[2].clone();
            let iid = args[3].clone();
            let id_raw = args[4].clone();
            Ok(Some(Command::Xidmprecord {
                key,
                pid,
                iid,
                id_raw,
            }))
        }
        "XGROUP" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'xgroup' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "CREATE" => {
                    if args.len() < 5 {
                        return Err(
                            "wrong number of arguments for 'xgroup create' command".to_string()
                        );
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    let id = String::from_utf8_lossy(&args[4]).to_string();
                    let mut mkstream = false;
                    let mut entries_read = None;
                    let mut i = 5;
                    while i < args.len() {
                        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                        match opt.as_str() {
                            "MKSTREAM" => {
                                mkstream = true;
                                i += 1;
                            }
                            "ENTRIESREAD" => {
                                if i + 1 >= args.len() {
                                    return Err("syntax error".to_string());
                                }
                                let s = std::str::from_utf8(&args[i + 1]).map_err(|_| {
                                    "value is not an integer or out of range".to_string()
                                })?;
                                let er: i64 = s.parse().map_err(|_| {
                                    "value is not an integer or out of range".to_string()
                                })?;
                                if er < -1 {
                                    return Err("ERR value for ENTRIESREAD must be positive or -1"
                                        .to_string());
                                }
                                if er >= 0 {
                                    entries_read = Some(er as u64);
                                }
                                i += 2;
                            }
                            _ => return Err("syntax error".to_string()),
                        }
                    }
                    Ok(Some(Command::XgroupCreate {
                        key,
                        group,
                        id,
                        mkstream,
                        entries_read,
                    }))
                }
                "DESTROY" => {
                    if args.len() < 4 {
                        return Err(
                            "wrong number of arguments for 'xgroup destroy' command".to_string()
                        );
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    Ok(Some(Command::XgroupDestroy { key, group }))
                }
                "CREATECONSUMER" => {
                    if args.len() < 5 {
                        return Err(
                            "wrong number of arguments for 'xgroup createconsumer' command"
                                .to_string(),
                        );
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    let consumer = args[4].clone();
                    Ok(Some(Command::XgroupCreateConsumer {
                        key,
                        group,
                        consumer,
                    }))
                }
                "DELCONSUMER" => {
                    if args.len() < 5 {
                        return Err("wrong number of arguments for 'xgroup delconsumer' command"
                            .to_string());
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    let consumer = args[4].clone();
                    Ok(Some(Command::XgroupDelConsumer {
                        key,
                        group,
                        consumer,
                    }))
                }
                "SETID" => {
                    if args.len() < 5 {
                        return Err(
                            "wrong number of arguments for 'xgroup setid' command".to_string()
                        );
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    let id = String::from_utf8_lossy(&args[4]).to_string();
                    let mut entries_read = None;
                    let mut i = 5;
                    while i < args.len() {
                        if args[i].eq_ignore_ascii_case(b"ENTRIESREAD") && i + 1 < args.len() {
                            let s = std::str::from_utf8(&args[i + 1]).map_err(|_| {
                                "value is not an integer or out of range".to_string()
                            })?;
                            let er: i64 = s.parse().map_err(|_| {
                                "value is not an integer or out of range".to_string()
                            })?;
                            if er < -1 {
                                return Err(
                                    "ERR value for ENTRIESREAD must be positive or -1".to_string()
                                );
                            }
                            if er >= 0 {
                                entries_read = Some(er as u64);
                            }
                            i += 2;
                        } else {
                            return Err("syntax error".to_string());
                        }
                    }
                    Ok(Some(Command::XgroupSetId {
                        key,
                        group,
                        id,
                        entries_read,
                    }))
                }
                "HELP" => {
                    if args.len() != 2 {
                        return Err(
                            "wrong number of arguments for 'xgroup|help' command".to_string()
                        );
                    }
                    Ok(Some(Command::XgroupHelp))
                }
                _ => Ok(Some(Command::Unknown(format!("XGROUP {}", sub)))),
            }
        }
        "XREADGROUP" => {
            if args.len() < 7 {
                return Err("wrong number of arguments for 'xreadgroup' command".to_string());
            }
            let mut group = None;
            let mut consumer = None;
            let mut count = None;
            let mut maxcount = None;
            let mut maxsize = None;
            let mut block_ms = None;
            let mut noack = false;
            let mut claim = None;
            let mut i = 1;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "GROUP" => {
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        group = Some(args[i + 1].clone());
                        consumer = Some(args[i + 2].clone());
                        i += 3;
                    }
                    "CLAIM" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "ERR min-idle-time is not an integer")?;
                        let min_idle = parse_min_idle_time(s)?;
                        claim = Some(min_idle);
                        i += 2;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let c: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        count = Some(c);
                        i += 2;
                    }
                    "MAXCOUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let mc: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if mc <= 0 {
                            return Err("ERR MAXCOUNT must be a positive integer".to_string());
                        }
                        maxcount = Some(mc as usize);
                        i += 2;
                    }
                    "MAXSIZE" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ms: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if ms <= 0 {
                            return Err("ERR MAXSIZE must be a positive integer".to_string());
                        }
                        maxsize = Some(ms as usize);
                        i += 2;
                    }
                    "BLOCK" => {
                        if i + 1 >= args.len() || args[i + 1].eq_ignore_ascii_case(b"STREAMS") {
                            return Err("ERR timeout is not an integer or out of range".to_string());
                        }
                        let b_i: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| {
                                "ERR timeout is not an integer or out of range".to_string()
                            })?
                            .parse()
                            .map_err(|_| {
                                "ERR timeout is not an integer or out of range".to_string()
                            })?;
                        if b_i < 0 {
                            return Err("ERR timeout is negative".to_string());
                        }
                        block_ms = Some(b_i as u64);
                        i += 2;
                    }
                    "NOACK" => {
                        noack = true;
                        i += 1;
                    }
                    "STREAMS" => {
                        i += 1;
                        break;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            let (group, consumer) = match (group, consumer) {
                (Some(g), Some(c)) => (g, c),
                _ => return Err("syntax error".to_string()),
            };
            if let (Some(c), Some(mc)) = (count, maxcount)
                && mc < c
            {
                return Err("ERR MAXCOUNT must be greater than or equal to COUNT".to_string());
            }
            let rem = args.len() - i;
            if rem < 2 || !rem.is_multiple_of(2) {
                return Err("ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.".to_string());
            }
            let n = rem / 2;
            let keys: Vec<Bytes> = args[i..i + n].to_vec();
            let ids: Vec<String> = args[i + n..]
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect();
            Ok(Some(Command::Xreadgroup {
                group,
                consumer,
                count,
                maxcount,
                maxsize,
                block_ms,
                noack,
                claim,
                keys,
                ids,
            }))
        }
        "XNACK" => {
            if args.len() < 7 {
                return Err("wrong number of arguments for 'xnack' command".to_string());
            }
            let key = args[1].clone();
            let group = args[2].clone();
            let mut mode = None;
            let mut ids = None;
            let mut retrycount = None;
            let mut force = false;

            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "SILENT" => {
                        if mode.is_some() {
                            return Err(format!("ERR Unrecognized XNACK option '{}'", opt));
                        }
                        mode = Some(XnackMode::Silent);
                        i += 1;
                    }
                    "FAIL" => {
                        if mode.is_some() {
                            return Err(format!("ERR Unrecognized XNACK option '{}'", opt));
                        }
                        mode = Some(XnackMode::Fail);
                        i += 1;
                    }
                    "FATAL" => {
                        if mode.is_some() {
                            return Err(format!("ERR Unrecognized XNACK option '{}'", opt));
                        }
                        mode = Some(XnackMode::Fatal);
                        i += 1;
                    }
                    "FORCE" => {
                        force = true;
                        i += 1;
                    }
                    "RETRYCOUNT" => {
                        if i + 1 >= args.len() {
                            if ids.is_none() {
                                return Err(
                                    "wrong number of arguments for 'xnack' command".to_string()
                                );
                            } else {
                                return Err(
                                    "ERR Unrecognized XNACK option 'RETRYCOUNT'".to_string()
                                );
                            }
                        }
                        let s = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "ERR value is not an integer or out of range")?;
                        if s.starts_with('-') {
                            if let Ok(rc_i) = s.parse::<i64>()
                                && rc_i < 0
                            {
                                return Err(
                                    "ERR Invalid RETRYCOUNT value, must be >= 0".to_string()
                                );
                            }
                            return Err("ERR value is not an integer or out of range".to_string());
                        }
                        match s.parse::<usize>() {
                            Ok(rc) => {
                                retrycount = Some(rc);
                                i += 2;
                            }
                            Err(_) => {
                                return Err(
                                    "ERR value is not an integer or out of range".to_string()
                                );
                            }
                        }
                    }
                    "IDS" => {
                        if i + 1 >= args.len() {
                            return Err("wrong number of arguments for 'xnack' command".to_string());
                        }
                        let numids_i: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "ERR numids must be a positive integer")?
                            .parse()
                            .map_err(|_| "ERR numids must be a positive integer")?;
                        if numids_i <= 0 {
                            return Err("ERR numids must be a positive integer".to_string());
                        }
                        let numids = numids_i as usize;
                        i += 2;
                        let remaining = args.len() - i;
                        if remaining < numids {
                            return Err("ERR number of IDs doesn't match numids".to_string());
                        }
                        let mut parsed_ids = Vec::with_capacity(numids);
                        for id_arg in &args[i..i + numids] {
                            let s = std::str::from_utf8(id_arg).map_err(
                                |_| "ERR Invalid stream ID specified as stream command argument",
                            )?;
                            let id = crate::table::StreamId::parse_exact(s).map_err(
                                |_| "ERR Invalid stream ID specified as stream command argument",
                            )?;
                            parsed_ids.push(id);
                        }
                        ids = Some(parsed_ids);
                        i += numids;
                    }
                    _ => {
                        if mode.is_none() && i == 3 {
                            return Err("ERR mode must be SILENT, FAIL, or FATAL".to_string());
                        }
                        return Err(format!(
                            "ERR Unrecognized XNACK option '{}'",
                            String::from_utf8_lossy(&args[i])
                        ));
                    }
                }
            }

            let mode = match mode {
                Some(m) => m,
                None => return Err("ERR mode must be SILENT, FAIL, or FATAL".to_string()),
            };
            let ids = match ids {
                Some(id_list) => id_list,
                None => return Err("ERR syntax error, expected IDS keyword".to_string()),
            };

            Ok(Some(Command::Xnack {
                key,
                group,
                mode,
                ids,
                retrycount,
                force,
            }))
        }
        "XACK" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'xack' command".to_string());
            }
            let key = args[1].clone();
            let group = args[2].clone();
            let mut ids = Vec::with_capacity(args.len() - 3);
            for arg in &args[3..] {
                let s = std::str::from_utf8(arg)
                    .map_err(|_| "Invalid stream ID specified as stream command argument")?;
                let id = crate::table::StreamId::parse_exact(s).map_err(|e| e.to_string())?;
                ids.push(id);
            }
            Ok(Some(Command::Xack { key, group, ids }))
        }
        "XPENDING" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'xpending' command".to_string());
            }
            let key = args[1].clone();
            let group = args[2].clone();
            if args.len() == 3 {
                Ok(Some(Command::Xpending {
                    key,
                    group,
                    range: None,
                }))
            } else {
                let mut start_idx = 3;
                let mut min_idle = None;
                if args[start_idx].eq_ignore_ascii_case(b"IDLE") {
                    if args.len() < start_idx + 2 {
                        return Err("syntax error".to_string());
                    }
                    let idle_ms: u64 = std::str::from_utf8(&args[start_idx + 1])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range")?;
                    min_idle = Some(idle_ms);
                    start_idx += 2;
                }
                if args.len() < start_idx + 3 {
                    return Err("syntax error".to_string());
                }
                let start_s = std::str::from_utf8(&args[start_idx]).map_err(|_| "syntax error")?;
                let start = crate::table::parse_range_bound(start_s, true)?;
                let end_s =
                    std::str::from_utf8(&args[start_idx + 1]).map_err(|_| "syntax error")?;
                let end = crate::table::parse_range_bound(end_s, false)?;
                let count: usize = std::str::from_utf8(&args[start_idx + 2])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                let consumer = if args.len() > start_idx + 3 {
                    Some(args[start_idx + 3].clone())
                } else {
                    None
                };
                Ok(Some(Command::Xpending {
                    key,
                    group,
                    range: Some((start, end, count, consumer, min_idle)),
                }))
            }
        }
        "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" => {
            if args.len() < 6 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let key = args[1].clone();
            let raw_time: i64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            let expire_ms = match cmd_name {
                "HEXPIRE" | "HEXPIREAT" => raw_time.saturating_mul(1000),
                _ => raw_time,
            };
            let is_at = matches!(cmd_name, "HEXPIREAT" | "HPEXPIREAT");

            let mut idx = 3;
            let mut condition = HexpireCondition::None;
            let mut has_cond = false;
            while idx < args.len() {
                if args[idx].eq_ignore_ascii_case(b"FIELDS") {
                    break;
                }
                let tok = String::from_utf8_lossy(&args[idx]).to_uppercase();
                match tok.as_str() {
                    "NX" => {
                        if has_cond {
                            return Err("ERR Multiple condition flags specified".to_string());
                        }
                        condition = HexpireCondition::Nx;
                        has_cond = true;
                        idx += 1;
                    }
                    "XX" => {
                        if has_cond {
                            return Err("ERR Multiple condition flags specified".to_string());
                        }
                        condition = HexpireCondition::Xx;
                        has_cond = true;
                        idx += 1;
                    }
                    "GT" => {
                        if has_cond {
                            return Err("ERR Multiple condition flags specified".to_string());
                        }
                        condition = HexpireCondition::Gt;
                        has_cond = true;
                        idx += 1;
                    }
                    "LT" => {
                        if has_cond {
                            return Err("ERR Multiple condition flags specified".to_string());
                        }
                        condition = HexpireCondition::Lt;
                        has_cond = true;
                        idx += 1;
                    }
                    _ => {
                        return Err("ERR unknown argument".to_string());
                    }
                }
            }
            if idx >= args.len() || !args[idx].eq_ignore_ascii_case(b"FIELDS") {
                return Err("ERR unknown argument".to_string());
            }
            if idx + 1 >= args.len() {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let numfields = parse_integer(&args[idx + 1])
                .map_err(|_| "value is not an integer or out of range")?;
            if numfields <= 0 {
                return Err("ERR Parameter `numFields` should be greater than 0".to_string());
            }
            let numfields = numfields as usize;
            let expected_len = idx + 2 + numfields;
            if args.len() < expected_len {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            } else if args.len() > expected_len {
                let remaining = &args[expected_len..];
                if remaining.iter().any(|a| a.eq_ignore_ascii_case(b"FIELDS")) {
                    return Err("ERR FIELDS keyword specified multiple times".to_string());
                }
                if remaining.iter().any(|a| {
                    let s = String::from_utf8_lossy(a).to_uppercase();
                    matches!(s.as_str(), "NX" | "XX" | "GT" | "LT")
                }) {
                    return Err("ERR Multiple condition flags specified".to_string());
                }
                return Err("ERR unknown argument".to_string());
            }
            let fields = args[idx + 2..expected_len].to_vec();
            Ok(Some(Command::Hexpire {
                key,
                expire_ms,
                is_at,
                condition,
                fields,
            }))
        }
        "HTTL" | "HPTTL" | "HEXPIRETIME" | "HPEXPIRETIME" => {
            if args.len() < 5 || !args[2].eq_ignore_ascii_case(b"FIELDS") {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let key = args[1].clone();
            let numfields =
                parse_integer(&args[3]).map_err(|_| "value is not an integer or out of range")?;
            if numfields <= 0 {
                return Err("ERR Parameter `numFields` should be greater than 0".to_string());
            }
            let numfields = numfields as usize;
            if args.len() != 4 + numfields {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let is_ms = matches!(cmd_name, "HPTTL" | "HPEXPIRETIME");
            let is_expiretime = matches!(cmd_name, "HEXPIRETIME" | "HPEXPIRETIME");
            let fields = args[4..].to_vec();
            Ok(Some(Command::Httl {
                key,
                is_ms,
                is_expiretime,
                fields,
            }))
        }
        "HPERSIST" => {
            if args.len() < 5 || !args[2].eq_ignore_ascii_case(b"FIELDS") {
                return Err("wrong number of arguments for 'hpersist' command".to_string());
            }
            let key = args[1].clone();
            let numfields =
                parse_integer(&args[3]).map_err(|_| "value is not an integer or out of range")?;
            if numfields <= 0 {
                return Err("ERR Parameter `numFields` should be greater than 0".to_string());
            }
            let numfields = numfields as usize;
            if args.len() != 4 + numfields {
                return Err("wrong number of arguments for 'hpersist' command".to_string());
            }
            let fields = args[4..].to_vec();
            Ok(Some(Command::Hpersist { key, fields }))
        }
        "HGETEX" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'hgetex' command".to_string());
            }
            let key = args[1].clone();
            let mut fields_idx = None;
            let mut i = 2;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"FIELDS") {
                    if fields_idx.is_some() {
                        return Err("ERR FIELDS keyword specified multiple times".to_string());
                    }
                    fields_idx = Some(i);
                    if i + 1 < args.len()
                        && let Ok(n) = parse_integer(&args[i + 1])
                        && n > 0
                    {
                        i += 2 + n as usize;
                        continue;
                    }
                    i += 1;
                } else {
                    i += 1;
                }
            }

            let fields_idx = match fields_idx {
                Some(idx) => idx,
                None => return Err("ERR unknown argument".to_string()),
            };

            if fields_idx + 1 >= args.len() {
                return Err("ERR wrong number of arguments for 'hgetex' command".to_string());
            }

            let numfields = parse_integer(&args[fields_idx + 1])
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
            if numfields <= 0 {
                return Err("ERR invalid number of fields".to_string());
            }
            let numfields = numfields as usize;
            let fields_start = fields_idx + 2;
            let fields_end = fields_start + numfields;

            if fields_end > args.len() {
                return Err("ERR wrong number of arguments for 'hgetex' command".to_string());
            }

            let fields = args[fields_start..fields_end].to_vec();

            let mut expire = HFieldExpireOpt::None;
            let mut has_exp_opt = false;

            let mut parse_opt = |idx: &mut usize| -> Result<(), String> {
                let opt = String::from_utf8_lossy(&args[*idx]).to_uppercase();
                match opt.as_str() {
                    "PERSIST" => {
                        if has_exp_opt {
                            return Err("ERR syntax error".to_string());
                        }
                        expire = HFieldExpireOpt::Persist;
                        has_exp_opt = true;
                        *idx += 1;
                    }
                    "EX" | "PX" | "EXAT" | "PXAT" => {
                        if has_exp_opt {
                            return Err("ERR syntax error".to_string());
                        }
                        if *idx + 1 >= args.len() || (*idx + 1 >= fields_idx && *idx < fields_end) {
                            return Err("ERR syntax error".to_string());
                        }
                        let val = parse_integer(&args[*idx + 1]).map_err(|_| {
                            "ERR value is not an integer or out of range".to_string()
                        })?;
                        // Valkey only rejects negative values; an already-expired
                        // time (e.g. PX 0) returns the values and deletes the fields.
                        if val < 0 {
                            return Err("ERR invalid expire time in 'hgetex' command".to_string());
                        }
                        expire = match opt.as_str() {
                            "EX" => HFieldExpireOpt::ExMs(val.saturating_mul(1000)),
                            "PX" => HFieldExpireOpt::ExMs(val),
                            "EXAT" => HFieldExpireOpt::ExAtMs(val.saturating_mul(1000)),
                            "PXAT" => HFieldExpireOpt::ExAtMs(val),
                            _ => unreachable!(),
                        };
                        has_exp_opt = true;
                        *idx += 2;
                    }
                    "FIELDS" => {
                        return Err("ERR FIELDS keyword specified multiple times".to_string());
                    }
                    _ => return Err("ERR unknown argument".to_string()),
                }
                Ok(())
            };

            let mut idx = 2;
            while idx < fields_idx {
                parse_opt(&mut idx)?;
            }
            idx = fields_end;
            while idx < args.len() {
                parse_opt(&mut idx)?;
            }

            Ok(Some(Command::Hgetex {
                key,
                expire,
                fields,
            }))
        }
        "HSETEX" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'hsetex' command".to_string());
            }
            let key = args[1].clone();
            let mut fields_idx = None;
            let mut i = 2;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"FIELDS") {
                    if fields_idx.is_some() {
                        return Err("ERR FIELDS keyword specified multiple times".to_string());
                    }
                    fields_idx = Some(i);
                    if i + 1 < args.len()
                        && let Ok(n) = parse_integer(&args[i + 1])
                        && n > 0
                    {
                        i = i.saturating_add((n as usize).saturating_mul(2).saturating_add(2));
                        continue;
                    }
                    i += 1;
                } else {
                    i += 1;
                }
            }

            let fields_idx = match fields_idx {
                Some(idx) => idx,
                None => return Err("ERR unknown argument".to_string()),
            };

            if fields_idx + 1 >= args.len() {
                return Err("ERR wrong number of arguments for 'hsetex' command".to_string());
            }

            let numfields = parse_integer(&args[fields_idx + 1])
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
            if numfields <= 0 {
                return Err("ERR invalid number of fields".to_string());
            }
            let numfields = numfields as usize;
            let fields_start = fields_idx + 2;
            if numfields > (args.len() - fields_start) / 2 {
                return Err("ERR wrong number of arguments for 'hsetex' command".to_string());
            }
            let fields_end = fields_start + numfields * 2;

            let mut pairs = Vec::with_capacity(numfields);
            for [f, v] in args[fields_start..fields_end].as_chunks::<2>().0 {
                pairs.push((f.clone(), v.clone()));
            }

            let mut condition = HsetexCondition::None;
            let mut has_cond = false;
            let mut expire = HFieldExpireOpt::None;
            let mut has_exp_opt = false;

            let mut parse_opt = |idx: &mut usize| -> Result<(), String> {
                let opt = String::from_utf8_lossy(&args[*idx]).to_uppercase();
                match opt.as_str() {
                    "FNX" => {
                        if has_cond {
                            return Err("ERR syntax error".to_string());
                        }
                        condition = HsetexCondition::Fnx;
                        has_cond = true;
                        *idx += 1;
                    }
                    "FXX" => {
                        if has_cond {
                            return Err("ERR syntax error".to_string());
                        }
                        condition = HsetexCondition::Fxx;
                        has_cond = true;
                        *idx += 1;
                    }
                    "KEEPTTL" => {
                        if has_exp_opt {
                            return Err("ERR syntax error".to_string());
                        }
                        expire = HFieldExpireOpt::KeepTtl;
                        has_exp_opt = true;
                        *idx += 1;
                    }
                    "EX" | "PX" | "EXAT" | "PXAT" => {
                        if has_exp_opt {
                            return Err("ERR syntax error".to_string());
                        }
                        if *idx + 1 >= args.len() || (*idx + 1 >= fields_idx && *idx < fields_end) {
                            return Err("ERR syntax error".to_string());
                        }
                        let val = parse_integer(&args[*idx + 1]).map_err(|_| {
                            "ERR value is not an integer or out of range".to_string()
                        })?;
                        if (opt == "EX" || opt == "PX") && val <= 0 {
                            return Err("ERR invalid expire time in 'hsetex' command".to_string());
                        }
                        expire = match opt.as_str() {
                            "EX" => HFieldExpireOpt::ExMs(val.saturating_mul(1000)),
                            "PX" => HFieldExpireOpt::ExMs(val),
                            "EXAT" => HFieldExpireOpt::ExAtMs(val.saturating_mul(1000)),
                            "PXAT" => HFieldExpireOpt::ExAtMs(val),
                            _ => unreachable!(),
                        };
                        has_exp_opt = true;
                        *idx += 2;
                    }
                    "FIELDS" => {
                        return Err("ERR FIELDS keyword specified multiple times".to_string());
                    }
                    _ => return Err("ERR unknown argument".to_string()),
                }
                Ok(())
            };

            let mut idx = 2;
            while idx < fields_idx {
                parse_opt(&mut idx)?;
            }
            idx = fields_end;
            while idx < args.len() {
                parse_opt(&mut idx)?;
            }

            Ok(Some(Command::Hsetex {
                key,
                condition,
                expire,
                pairs,
            }))
        }
        "XCLAIM" => {
            if args.len() < 6 {
                return Err("wrong number of arguments for 'xclaim' command".to_string());
            }
            let key = args[1].clone();
            let group = args[2].clone();
            let consumer = args[3].clone();
            let min_idle_time: u64 = std::str::from_utf8(&args[4])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;

            let mut ids = Vec::new();
            let mut idle = None;
            let mut time = None;
            let mut retrycount = None;
            let mut force = false;
            let mut justid = false;
            let mut i = 5;
            let mut in_opts = false;

            while i < args.len() {
                let tok = String::from_utf8_lossy(&args[i]).to_uppercase();
                match tok.as_str() {
                    "IDLE" => {
                        in_opts = true;
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idle = Some(
                            std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "value is not an integer or out of range")?
                                .parse()
                                .map_err(|_| "value is not an integer or out of range")?,
                        );
                        i += 2;
                    }
                    "TIME" => {
                        in_opts = true;
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        time = Some(
                            std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "value is not an integer or out of range")?
                                .parse()
                                .map_err(|_| "value is not an integer or out of range")?,
                        );
                        i += 2;
                    }
                    "RETRYCOUNT" => {
                        in_opts = true;
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        retrycount = Some(
                            std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "value is not an integer or out of range")?
                                .parse()
                                .map_err(|_| "value is not an integer or out of range")?,
                        );
                        i += 2;
                    }
                    "FORCE" => {
                        in_opts = true;
                        force = true;
                        i += 1;
                    }
                    "JUSTID" => {
                        in_opts = true;
                        justid = true;
                        i += 1;
                    }
                    "LASTID" => {
                        in_opts = true;
                        i += 2;
                    }
                    _ => {
                        if in_opts {
                            return Err("syntax error".to_string());
                        }
                        ids.push(args[i].clone());
                        i += 1;
                    }
                }
            }
            if ids.is_empty() {
                return Err("wrong number of arguments for 'xclaim' command".to_string());
            }
            Ok(Some(Command::Xclaim {
                key,
                group,
                consumer,
                min_idle_time,
                ids,
                idle,
                time,
                retrycount,
                force,
                justid,
            }))
        }
        "XAUTOCLAIM" => {
            if args.len() < 6 {
                return Err("wrong number of arguments for 'xautoclaim' command".to_string());
            }
            let key = args[1].clone();
            let group = args[2].clone();
            let consumer = args[3].clone();
            let min_idle_time: u64 = std::str::from_utf8(&args[4])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            let start = args[5].clone();
            let mut count: usize = 100;
            let mut justid = false;
            let mut i = 6;
            while i < args.len() {
                let tok = String::from_utf8_lossy(&args[i]).to_uppercase();
                match tok.as_str() {
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "ERR COUNT must be > 0".to_string())?;
                        let val: i64 =
                            s.parse().map_err(|_| "ERR COUNT must be > 0".to_string())?;
                        if val <= 0 || val > i32::MAX as i64 {
                            return Err("ERR COUNT must be > 0".to_string());
                        }
                        count = val as usize;
                        i += 2;
                    }
                    "JUSTID" => {
                        justid = true;
                        i += 1;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Xautoclaim {
                key,
                group,
                consumer,
                min_idle_time,
                start,
                count,
                justid,
            }))
        }
        "XINFO" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'xinfo' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "STREAM" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'xinfo stream' command".to_string()
                        );
                    }
                    let key = args[2].clone();
                    if args.len() == 3 {
                        Ok(Some(Command::Xinfo(XinfoSubcommand::Stream(key))))
                    } else if args.len() == 4 && args[3].eq_ignore_ascii_case(b"FULL") {
                        Ok(Some(Command::Xinfo(XinfoSubcommand::StreamFull {
                            key,
                            count: None,
                        })))
                    } else if args.len() == 6
                        && args[3].eq_ignore_ascii_case(b"FULL")
                        && args[4].eq_ignore_ascii_case(b"COUNT")
                    {
                        let count_str = std::str::from_utf8(&args[5])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let count: i64 = count_str
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let count = if count < 0 { 10 } else { count as usize };
                        Ok(Some(Command::Xinfo(XinfoSubcommand::StreamFull {
                            key,
                            count: Some(count),
                        })))
                    } else {
                        Err("ERR syntax error".to_string())
                    }
                }
                "GROUPS" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'xinfo groups' command".to_string()
                        );
                    }
                    let key = args[2].clone();
                    Ok(Some(Command::Xinfo(XinfoSubcommand::Groups(key))))
                }
                "CONSUMERS" => {
                    if args.len() < 4 {
                        return Err(
                            "wrong number of arguments for 'xinfo consumers' command".to_string()
                        );
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    Ok(Some(Command::Xinfo(XinfoSubcommand::Consumers {
                        key,
                        group,
                    })))
                }
                "HELP" => {
                    if args.len() != 2 {
                        return Err(
                            "wrong number of arguments for 'xinfo|help' command".to_string()
                        );
                    }
                    Ok(Some(Command::Xinfo(XinfoSubcommand::Help)))
                }
                _ => Ok(Some(Command::Unknown(format!("XINFO {}", sub)))),
            }
        }
        "HELLO" => {
            let mut proto = None;
            let mut auth = None;
            let mut setname = None;
            let mut i = 1;
            if i < args.len() {
                let first_arg = String::from_utf8_lossy(&args[i]);
                if let Ok(p) = first_arg.parse::<i64>() {
                    proto = Some(p);
                    i += 1;
                } else if !["AUTH", "SETNAME"].contains(&first_arg.to_uppercase().as_str()) {
                    proto = Some(-1);
                    i += 1;
                }
            }
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "AUTH" => {
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let u = String::from_utf8_lossy(&args[i + 1]).to_string();
                        let p = String::from_utf8_lossy(&args[i + 2]).to_string();
                        auth = Some((u, p));
                        i += 3;
                    }
                    "SETNAME" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let name = String::from_utf8_lossy(&args[i + 1]).to_string();
                        setname = Some(name);
                        i += 2;
                    }
                    _ => {
                        i += 1;
                    }
                }
            }
            Ok(Some(Command::Hello {
                proto,
                auth,
                setname,
            }))
        }
        "RESET" => Ok(Some(Command::Reset)),
        "TIME" => Ok(Some(Command::Time)),
        "ECHO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'echo' command".to_string());
            }
            Ok(Some(Command::Echo(args[1].clone())))
        }
        "HINCRBY" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'hincrby' command".to_string());
            }
            let increment: i64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Hincrby {
                key: args[1].clone(),
                field: args[2].clone(),
                increment,
            }))
        }
        "HINCRBYFLOAT" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'hincrbyfloat' command".to_string());
            }
            let s = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not a valid float".to_string())?;
            let increment: f64 =
                parse_redis_f64(s).ok_or_else(|| "value is not a valid float".to_string())?;
            if increment.is_nan() || increment.is_infinite() {
                return Err("value is NaN or Infinity".to_string());
            }
            Ok(Some(Command::Hincrbyfloat {
                key: args[1].clone(),
                field: args[2].clone(),
                increment,
            }))
        }
        "HRANDFIELD" => {
            if args.len() < 2 || args.len() > 4 {
                return Err("wrong number of arguments for 'hrandfield' command".to_string());
            }
            let count = if args.len() >= 3 {
                Some(parse_random_count(&args[2])?)
            } else {
                None
            };
            let mut with_values = false;
            if args.len() == 4 {
                if String::from_utf8_lossy(&args[3]).eq_ignore_ascii_case("WITHVALUES") {
                    with_values = true;
                    if let Some(c) = count
                        && c.checked_mul(2).is_none()
                    {
                        return Err("value is out of range".to_string());
                    }
                } else {
                    return Err("syntax error".to_string());
                }
            }
            Ok(Some(Command::Hrandfield {
                key: args[1].clone(),
                count,
                with_values,
            }))
        }
        "HSCAN" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'hscan' command".to_string());
            }
            let cursor: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            let mut pattern = None;
            let mut count = None;
            let mut no_values = false;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "MATCH" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        pattern = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let c: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        count = Some(c);
                        i += 2;
                    }
                    "NOVALUES" => {
                        no_values = true;
                        i += 1;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Hscan {
                key: args[1].clone(),
                cursor,
                pattern,
                count,
                no_values,
            }))
        }
        "SMISMEMBER" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'smismember' command".to_string());
            }
            Ok(Some(Command::Smismember {
                key: args[1].clone(),
                members: args[2..].to_vec(),
            }))
        }
        "SRANDMEMBER" => {
            if args.len() < 2 || args.len() > 3 {
                return Err("wrong number of arguments for 'srandmember' command".to_string());
            }
            let count = if args.len() == 3 {
                Some(parse_random_count(&args[2])?)
            } else {
                None
            };
            Ok(Some(Command::Srandmember {
                key: args[1].clone(),
                count,
            }))
        }
        "SMOVE" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'smove' command".to_string());
            }
            Ok(Some(Command::Smove {
                source: args[1].clone(),
                destination: args[2].clone(),
                member: args[3].clone(),
            }))
        }
        "SSCAN" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'sscan' command".to_string());
            }
            let cursor: u64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            let mut pattern = None;
            let mut count = None;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "MATCH" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        pattern = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let c: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        count = Some(c);
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Sscan {
                key: args[1].clone(),
                cursor,
                pattern,
                count,
            }))
        }
        "ZMSCORE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'zmscore' command".to_string());
            }
            Ok(Some(Command::Zmscore {
                key: args[1].clone(),
                members: args[2..].to_vec(),
            }))
        }
        "ZRANDMEMBER" => {
            if args.len() < 2 || args.len() > 4 {
                return Err("wrong number of arguments for 'zrandmember' command".to_string());
            }
            let count = if args.len() >= 3 {
                Some(parse_random_count(&args[2])?)
            } else {
                None
            };
            let mut with_scores = false;
            if args.len() == 4 {
                if String::from_utf8_lossy(&args[3]).eq_ignore_ascii_case("WITHSCORES") {
                    with_scores = true;
                    if let Some(c) = count
                        && c.checked_mul(2).is_none()
                    {
                        return Err("value is out of range".to_string());
                    }
                } else {
                    return Err("syntax error".to_string());
                }
            }
            Ok(Some(Command::Zrandmember {
                key: args[1].clone(),
                count,
                with_scores,
            }))
        }
        "ZREMRANGEBYRANK" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'zremrangebyrank' command".to_string());
            }
            let start: i64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            let stop: i64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Zremrangebyrank {
                key: args[1].clone(),
                start,
                stop,
            }))
        }
        "ZREMRANGEBYSCORE" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'zremrangebyscore' command".to_string());
            }
            let (min_score, min_inc) = parse_score_bound(&args[2])?;
            let (max_score, max_inc) = parse_score_bound(&args[3])?;
            Ok(Some(Command::Zremrangebyscore {
                key: args[1].clone(),
                min_score,
                min_inc,
                max_score,
                max_inc,
            }))
        }
        "ZREMRANGEBYLEX" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'zremrangebylex' command".to_string());
            }
            let min = crate::table::parse_lex_bound(&args[2]).map_err(|e| e.to_string())?;
            let max = crate::table::parse_lex_bound(&args[3]).map_err(|e| e.to_string())?;
            Ok(Some(Command::Zremrangebylex {
                key: args[1].clone(),
                min,
                max,
            }))
        }
        "ZLEXCOUNT" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'zlexcount' command".to_string());
            }
            let min = crate::table::parse_lex_bound(&args[2]).map_err(|e| e.to_string())?;
            let max = crate::table::parse_lex_bound(&args[3]).map_err(|e| e.to_string())?;
            Ok(Some(Command::Zlexcount {
                key: args[1].clone(),
                min,
                max,
            }))
        }
        "ZSCAN" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'zscan' command".to_string());
            }
            let cursor: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            let mut pattern = None;
            let mut count = None;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "MATCH" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        pattern = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let c: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        count = Some(c);
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Zscan {
                key: args[1].clone(),
                cursor,
                pattern,
                count,
            }))
        }
        "LTRIM" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'ltrim' command".to_string());
            }
            let start: i64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            let stop: i64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Ltrim {
                key: args[1].clone(),
                start,
                stop,
            }))
        }
        "LSET" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'lset' command".to_string());
            }
            let index: i64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Lset {
                key: args[1].clone(),
                index,
                element: args[3].clone(),
            }))
        }
        "LREM" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'lrem' command".to_string());
            }
            let count: i64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Lrem {
                key: args[1].clone(),
                count,
                element: args[3].clone(),
            }))
        }
        "LPOS" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'lpos' command".to_string());
            }
            let mut rank = None;
            let mut count = None;
            let mut maxlen = None;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "RANK" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let r: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if r == i64::MIN {
                            return Err("value is out of range".to_string());
                        }
                        if r == 0 {
                            return Err("RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list".to_string());
                        }
                        rank = Some(r);
                        i += 2;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let c: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        count = Some(c);
                        i += 2;
                    }
                    "MAXLEN" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let m: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        maxlen = Some(m);
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Lpos {
                key: args[1].clone(),
                element: args[2].clone(),
                rank,
                count,
                maxlen,
            }))
        }
        "LINSERT" => {
            if args.len() != 5 {
                return Err("wrong number of arguments for 'linsert' command".to_string());
            }
            let dir_str = String::from_utf8_lossy(&args[2]).to_uppercase();
            let before = match dir_str.as_str() {
                "BEFORE" => true,
                "AFTER" => false,
                _ => return Err("syntax error".to_string()),
            };
            Ok(Some(Command::Linsert {
                key: args[1].clone(),
                before,
                pivot: args[3].clone(),
                element: args[4].clone(),
            }))
        }
        "LMOVE" => {
            if args.len() != 5 {
                return Err("wrong number of arguments for 'lmove' command".to_string());
            }
            let from_str = String::from_utf8_lossy(&args[3]).to_uppercase();
            let where_from = match from_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let to_str = String::from_utf8_lossy(&args[4]).to_uppercase();
            let where_to = match to_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            Ok(Some(Command::Lmove {
                source: args[1].clone(),
                destination: args[2].clone(),
                where_from,
                where_to,
            }))
        }
        "BLMOVE" => {
            if args.len() != 6 {
                return Err("wrong number of arguments for 'blmove' command".to_string());
            }
            let from_str = String::from_utf8_lossy(&args[3]).to_uppercase();
            let where_from = match from_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let to_str = String::from_utf8_lossy(&args[4]).to_uppercase();
            let where_to = match to_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let timeout: f64 = std::str::from_utf8(&args[5])
                .map_err(|_| "timeout is not a float or out of range".to_string())?
                .parse()
                .map_err(|_| "timeout is not a float or out of range".to_string())?;
            if timeout < 0.0 || timeout.is_nan() {
                return Err("timeout is negative".to_string());
            }
            if timeout > (i64::MAX / 1000) as f64 {
                return Err("timeout is out of range".to_string());
            }
            Ok(Some(Command::Blmove {
                source: args[1].clone(),
                destination: args[2].clone(),
                where_from,
                where_to,
                timeout,
            }))
        }
        "LMOVEM" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'lmovem' command".to_string());
            }
            if args.len() != 5 && args.len() != 8 {
                return Err("syntax error".to_string());
            }
            let from_str = String::from_utf8_lossy(&args[3]).to_uppercase();
            let where_from = match from_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let to_str = String::from_utf8_lossy(&args[4]).to_uppercase();
            let where_to = match to_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let (mode, count, ordering) = parse_lmovem_trailer(&args[5..])?;
            let raw_tokens = if args.len() == 8 {
                Some([
                    args[3].clone(),
                    args[4].clone(),
                    args[5].clone(),
                    args[7].clone(),
                ])
            } else {
                Some([
                    args[3].clone(),
                    args[4].clone(),
                    Bytes::from_static(b"count"),
                    Bytes::from_static(b"bulk"),
                ])
            };
            Ok(Some(Command::Lmovem {
                source: args[1].clone(),
                destination: args[2].clone(),
                where_from,
                where_to,
                mode,
                count,
                ordering,
                raw_tokens,
            }))
        }
        "BLMOVEM" => {
            if args.len() < 6 {
                return Err("wrong number of arguments for 'blmovem' command".to_string());
            }
            if args.len() != 6 && args.len() != 9 {
                return Err("syntax error".to_string());
            }
            let from_str = String::from_utf8_lossy(&args[3]).to_uppercase();
            let where_from = match from_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let to_str = String::from_utf8_lossy(&args[4]).to_uppercase();
            let where_to = match to_str.as_str() {
                "LEFT" => crate::table::ListDirection::Left,
                "RIGHT" => crate::table::ListDirection::Right,
                _ => return Err("syntax error".to_string()),
            };
            let timeout: f64 = std::str::from_utf8(&args[5])
                .map_err(|_| "timeout is not a float or out of range".to_string())?
                .parse()
                .map_err(|_| "timeout is not a float or out of range".to_string())?;
            if timeout < 0.0 || timeout.is_nan() {
                return Err("timeout is negative".to_string());
            }
            if timeout > (i64::MAX / 1000) as f64 {
                return Err("timeout is out of range".to_string());
            }
            let (mode, count, ordering) = parse_lmovem_trailer(&args[6..])?;
            Ok(Some(Command::Blmovem {
                source: args[1].clone(),
                destination: args[2].clone(),
                where_from,
                where_to,
                timeout,
                mode,
                count,
                ordering,
            }))
        }
        "SORT" | "SORT_RO" => {
            if args.len() < 2 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_ascii_lowercase()
                ));
            }
            let readonly = cmd_name == "SORT_RO";
            let key = args[1].clone();
            let mut desc = false;
            let mut alpha = false;
            let mut store = None;
            let mut limit = None;
            let mut by = None;
            let mut get = Vec::new();
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "ASC" => {
                        desc = false;
                        i += 1;
                    }
                    "DESC" => {
                        desc = true;
                        i += 1;
                    }
                    "ALPHA" => {
                        alpha = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let offset: i64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        let count: i64 = std::str::from_utf8(&args[i + 2])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        limit = Some((offset, count));
                        i += 3;
                    }
                    "STORE" => {
                        if readonly {
                            return Err("syntax error".to_string());
                        }
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        store = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "BY" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        by = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "GET" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        get.push(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        return Err("syntax error".to_string());
                    }
                }
            }
            Ok(Some(Command::Sort {
                key,
                desc,
                alpha,
                store,
                limit,
                by,
                get,
                readonly,
            }))
        }
        "RPOPLPUSH" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'rpoplpush' command".to_string());
            }
            Ok(Some(Command::Lmove {
                source: args[1].clone(),
                destination: args[2].clone(),
                where_from: crate::table::ListDirection::Right,
                where_to: crate::table::ListDirection::Left,
            }))
        }
        "BRPOPLPUSH" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'brpoplpush' command".to_string());
            }
            let timeout: f64 = std::str::from_utf8(&args[3])
                .map_err(|_| "timeout is not a float or out of range".to_string())?
                .parse()
                .map_err(|_| "timeout is not a float or out of range".to_string())?;
            if timeout < 0.0 || timeout.is_nan() {
                return Err("timeout is negative".to_string());
            }
            if timeout > (i64::MAX / 1000) as f64 {
                return Err("timeout is out of range".to_string());
            }
            Ok(Some(Command::Blmove {
                source: args[1].clone(),
                destination: args[2].clone(),
                where_from: crate::table::ListDirection::Right,
                where_to: crate::table::ListDirection::Left,
                timeout,
            }))
        }
        "INCRBYFLOAT" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'incrbyfloat' command".to_string());
            }
            let s = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not a valid float".to_string())?;
            let increment: f64 =
                parse_redis_f64(s).ok_or_else(|| "value is not a valid float".to_string())?;
            if increment.is_nan() || increment.is_infinite() {
                return Err("increment would produce NaN or Infinity".to_string());
            }
            Ok(Some(Command::Incrbyfloat {
                key: args[1].clone(),
                increment,
            }))
        }
        "INCREX" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'increx' command".to_string());
            }
            let key = args[1].clone();
            let mut seen_byint = false;
            let mut seen_byfloat = false;
            let mut seen_lbound = false;
            let mut seen_ubound = false;
            let mut seen_saturate = false;
            let mut seen_expire = false;
            let mut seen_persist = false;
            let mut seen_enx = false;

            let mut byint_val: Option<i64> = None;
            let mut byfloat_val: Option<f64> = None;
            let mut lbound_raw: Option<Bytes> = None;
            let mut ubound_raw: Option<Bytes> = None;
            let mut expire_opt: Option<IncrexExpire> = None;

            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "BYINT" => {
                        if seen_byint || seen_byfloat {
                            return Err("syntax error".to_string());
                        }
                        seen_byint = true;
                        i += 1;
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i]).map_err(|_| {
                            "Increment is not an integer or out of range".to_string()
                        })?;
                        let val = s.parse::<i64>().map_err(|_| {
                            "Increment is not an integer or out of range".to_string()
                        })?;
                        byint_val = Some(val);
                    }
                    "BYFLOAT" => {
                        if seen_byint || seen_byfloat {
                            return Err("syntax error".to_string());
                        }
                        seen_byfloat = true;
                        i += 1;
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i])
                            .map_err(|_| "Increment is not a valid float".to_string())?;
                        if s.eq_ignore_ascii_case("inf")
                            || s.eq_ignore_ascii_case("+inf")
                            || s.eq_ignore_ascii_case("-inf")
                        {
                            return Err("ERR BYFLOAT increment cannot be Infinity".to_string());
                        }
                        let val = parse_redis_f64(s)
                            .ok_or_else(|| "Increment is not a valid float".to_string())?;
                        if val.is_nan() || val.is_infinite() {
                            return Err("Increment is not a valid float".to_string());
                        }
                        byfloat_val = Some(val);
                    }
                    "LBOUND" => {
                        if seen_lbound {
                            return Err("syntax error".to_string());
                        }
                        seen_lbound = true;
                        i += 1;
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        lbound_raw = Some(args[i].clone());
                    }
                    "UBOUND" => {
                        if seen_ubound {
                            return Err("syntax error".to_string());
                        }
                        seen_ubound = true;
                        i += 1;
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        ubound_raw = Some(args[i].clone());
                    }
                    "SATURATE" => {
                        if seen_saturate {
                            return Err("syntax error".to_string());
                        }
                        seen_saturate = true;
                    }
                    "EX" => {
                        if seen_expire || seen_persist {
                            return Err("syntax error".to_string());
                        }
                        seen_expire = true;
                        i += 1;
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let val = s
                            .parse::<i64>()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if val <= 0 {
                            return Err("invalid expire time in 'increx' command".to_string());
                        }
                        expire_opt = Some(IncrexExpire::Ex(val as u64));
                    }
                    "PX" => {
                        if seen_expire || seen_persist {
                            return Err("syntax error".to_string());
                        }
                        seen_expire = true;
                        i += 1;
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let val = s
                            .parse::<i64>()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if val <= 0 {
                            return Err("invalid expire time in 'increx' command".to_string());
                        }
                        expire_opt = Some(IncrexExpire::Px(val as u64));
                    }
                    "EXAT" => {
                        if seen_expire || seen_persist {
                            return Err("syntax error".to_string());
                        }
                        seen_expire = true;
                        i += 1;
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let val = s
                            .parse::<i64>()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if val <= 0 {
                            return Err("invalid expire time in 'increx' command".to_string());
                        }
                        expire_opt = Some(IncrexExpire::Exat(val as u64));
                    }
                    "PXAT" => {
                        if seen_expire || seen_persist {
                            return Err("syntax error".to_string());
                        }
                        seen_expire = true;
                        i += 1;
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let s = std::str::from_utf8(&args[i])
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        let val = s
                            .parse::<i64>()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if val <= 0 {
                            return Err("invalid expire time in 'increx' command".to_string());
                        }
                        expire_opt = Some(IncrexExpire::Pxat(val as u64));
                    }
                    "PERSIST" => {
                        if seen_expire || seen_persist || seen_enx {
                            return Err("syntax error".to_string());
                        }
                        seen_persist = true;
                        expire_opt = Some(IncrexExpire::Persist);
                    }
                    "ENX" => {
                        if seen_enx || seen_persist {
                            return Err("syntax error".to_string());
                        }
                        seen_enx = true;
                    }
                    _ => return Err("syntax error".to_string()),
                }
                i += 1;
            }

            if seen_enx && !seen_expire {
                return Err("ENX flag requires an expiration".to_string());
            }

            if seen_byfloat {
                let increment = IncrexIncrement::Float(byfloat_val.unwrap());
                let lbound = if let Some(raw) = lbound_raw {
                    let s = std::str::from_utf8(&raw)
                        .map_err(|_| "Increment is not a valid float".to_string())?;
                    let val = parse_redis_f64(s)
                        .ok_or_else(|| "Increment is not a valid float".to_string())?;
                    if val.is_nan() || val.is_infinite() {
                        return Err("Increment is not a valid float".to_string());
                    }
                    Some(IncrexBound::Float(val))
                } else {
                    None
                };
                let ubound = if let Some(raw) = ubound_raw {
                    let s = std::str::from_utf8(&raw)
                        .map_err(|_| "Increment is not a valid float".to_string())?;
                    let val = parse_redis_f64(s)
                        .ok_or_else(|| "Increment is not a valid float".to_string())?;
                    if val.is_nan() || val.is_infinite() {
                        return Err("Increment is not a valid float".to_string());
                    }
                    Some(IncrexBound::Float(val))
                } else {
                    None
                };
                if let (Some(IncrexBound::Float(lb)), Some(IncrexBound::Float(ub))) =
                    (lbound, ubound)
                    && lb > ub
                {
                    return Err("LBOUND can't be greater than UBOUND".to_string());
                }
                Ok(Some(Command::Increx {
                    key,
                    increment,
                    lbound,
                    ubound,
                    saturate: seen_saturate,
                    expire: expire_opt,
                    enx: seen_enx,
                }))
            } else {
                let delta = byint_val.unwrap_or(1);
                let increment = IncrexIncrement::Int(delta);
                let lbound = if let Some(raw) = lbound_raw {
                    let s = std::str::from_utf8(&raw)
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    let val = s
                        .parse::<i64>()
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    Some(IncrexBound::Int(val))
                } else {
                    None
                };
                let ubound = if let Some(raw) = ubound_raw {
                    let s = std::str::from_utf8(&raw)
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    let val = s
                        .parse::<i64>()
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    Some(IncrexBound::Int(val))
                } else {
                    None
                };
                if let (Some(IncrexBound::Int(lb)), Some(IncrexBound::Int(ub))) = (lbound, ubound)
                    && lb > ub
                {
                    return Err("LBOUND can't be greater than UBOUND".to_string());
                }
                Ok(Some(Command::Increx {
                    key,
                    increment,
                    lbound,
                    ubound,
                    saturate: seen_saturate,
                    expire: expire_opt,
                    enx: seen_enx,
                }))
            }
        }
        "SETRANGE" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'setrange' command".to_string());
            }
            let offset: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Setrange {
                key: args[1].clone(),
                offset,
                value: args[3].clone(),
            }))
        }
        "GETRANGE" | "SUBSTR" => {
            if args.len() != 4 {
                let name = if cmd_name == "SUBSTR" {
                    "substr"
                } else {
                    "getrange"
                };
                return Err(format!("wrong number of arguments for '{}' command", name));
            }
            let start: i64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            let end: i64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Getrange {
                key: args[1].clone(),
                start,
                end,
            }))
        }
        "VADD" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'vadd' command".to_string());
            }
            let key = args[1].clone();
            let second_upper = String::from_utf8_lossy(&args[2]).to_ascii_uppercase();
            if second_upper == "REDUCE" || second_upper == "FP32" || second_upper == "VALUES" {
                let mut i = 2;
                let mut reduce = None;
                if String::from_utf8_lossy(&args[i]).eq_ignore_ascii_case("REDUCE") {
                    if i + 1 >= args.len() {
                        return Err("syntax error".to_string());
                    }
                    let d: usize = std::str::from_utf8(&args[i + 1])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range")?;
                    if d == 0 {
                        return Err("Projection dimension must be > 0".to_string());
                    }
                    reduce = Some(d);
                    i += 2;
                }
                if i >= args.len() {
                    return Err("syntax error".to_string());
                }
                let fmt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
                let vector = if fmt == "FP32" {
                    if i + 1 >= args.len() {
                        return Err("syntax error".to_string());
                    }
                    let blob = &args[i + 1];
                    if blob.is_empty() || !blob.len().is_multiple_of(4) {
                        return Err("Invalid FP32 vector blob length".to_string());
                    }
                    let mut v = Vec::with_capacity(blob.len() / 4);
                    for chunk in blob.as_chunks::<4>().0 {
                        let f = f32::from_le_bytes(*chunk);
                        if !f.is_finite() {
                            return Err("Vector contains NaN or Inf".to_string());
                        }
                        v.push(f);
                    }
                    i += 2;
                    v
                } else if fmt == "VALUES" {
                    if i + 1 >= args.len() {
                        return Err("syntax error".to_string());
                    }
                    let num: usize = std::str::from_utf8(&args[i + 1])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range")?;
                    if num == 0 || num > args.len() - (i + 2) {
                        return Err("syntax error".to_string());
                    }
                    let mut v = Vec::with_capacity(num);
                    for a in &args[i + 2..i + 2 + num] {
                        let f: f32 = std::str::from_utf8(a)
                            .map_err(|_| "not a valid float")?
                            .parse()
                            .map_err(|_| "not a valid float")?;
                        if !f.is_finite() {
                            return Err("Vector contains NaN or Inf".to_string());
                        }
                        v.push(f);
                    }
                    i += 2 + num;
                    v
                } else {
                    return Err("syntax error".to_string());
                };
                // Same limits as Redis vector sets; REDUCE also bounds the projection
                // matrix, which is input dim x reduced dim floats.
                if vector.len() > crate::vector::MAX_VECTOR_DIM
                    || reduce.is_some_and(|r| r > vector.len())
                {
                    return Err("invalid vector specification".to_string());
                }
                if reduce.is_some_and(|r| r * vector.len() > crate::vector::MAX_PROJECTION_ENTRIES)
                {
                    return Err(format!(
                        "REDUCE projection too large: input dim x reduced dim must be <= {}",
                        crate::vector::MAX_PROJECTION_ENTRIES
                    ));
                }
                if i >= args.len() {
                    return Err("wrong number of arguments for 'vadd' command".to_string());
                }
                let element = args[i].clone();
                i += 1;
                let mut cas = false;
                let mut quant = None;
                let mut ef = None;
                let mut setattr = None;
                let mut m = None;
                let mut tiered = false;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
                    match opt.as_str() {
                        "CAS" => {
                            cas = true;
                            i += 1;
                        }
                        "TIERED" => {
                            tiered = true;
                            i += 1;
                        }
                        "NOQUANT" => {
                            quant = Some(crate::vector::VQuant::NoQuant);
                            i += 1;
                        }
                        "Q8" => {
                            quant = Some(crate::vector::VQuant::Q8);
                            i += 1;
                        }
                        "BIN" => {
                            quant = Some(crate::vector::VQuant::Bin);
                            i += 1;
                        }
                        "EF" => {
                            if i + 1 >= args.len() {
                                return Err("syntax error".to_string());
                            }
                            let val: usize = std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "value is not an integer or out of range")?
                                .parse()
                                .map_err(|_| "value is not an integer or out of range")?;
                            if val > crate::vector::MAX_VSET_EF {
                                return Err("invalid EF".to_string());
                            }
                            ef = Some(val.max(1));
                            i += 2;
                        }
                        "SETATTR" => {
                            if i + 1 >= args.len() {
                                return Err("syntax error".to_string());
                            }
                            let s = std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "Invalid JSON attributes")?
                                .to_string();
                            if !s.trim().is_empty()
                                && serde_json::from_str::<serde_json::Value>(&s).is_err()
                            {
                                return Err("Invalid JSON in SETATTR".to_string());
                            }
                            setattr = Some(s);
                            i += 2;
                        }
                        "M" => {
                            if i + 1 >= args.len() {
                                return Err("syntax error".to_string());
                            }
                            let val: usize = std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "value is not an integer or out of range")?
                                .parse()
                                .map_err(|_| "value is not an integer or out of range")?;
                            if val < 2 {
                                return Err("M must be >= 2".to_string());
                            }
                            if val > crate::vector::MAX_HNSW_M {
                                return Err("invalid M".to_string());
                            }
                            m = Some(val);
                            i += 2;
                        }
                        _ => return Err("syntax error".to_string()),
                    }
                }
                let quantize =
                    quant.unwrap_or(crate::vector::VQuant::Q8) == crate::vector::VQuant::Q8;
                Ok(Some(Command::Vadd {
                    key,
                    element,
                    vector,
                    metric: None,
                    quantize,
                    pq: false,
                    tiered,
                    reduce,
                    quant,
                    ef,
                    setattr,
                    m,
                    cas,
                    is_redis_vset: true,
                }))
            } else {
                let element = args[2].clone();
                let mut vector = Vec::with_capacity(args.len() - 3);
                let mut metric = None;
                let mut quantize = false;
                let mut pq = false;
                let mut tiered = false;
                for a in &args[3..] {
                    let s = String::from_utf8_lossy(a).to_uppercase();
                    if s == "QUANTIZE" || s == "SQ8" {
                        quantize = true;
                    } else if s == "PQ" {
                        pq = true;
                    } else if s == "TIERED" {
                        tiered = true;
                    } else if let Ok(m) = s.parse::<crate::vector::VectorMetric>() {
                        metric = Some(m);
                    } else {
                        let val: f32 = s.parse().map_err(|_| "not a valid float")?;
                        vector.push(val);
                    }
                }
                Ok(Some(Command::Vadd {
                    key,
                    element,
                    vector,
                    metric,
                    quantize,
                    pq,
                    tiered,
                    reduce: None,
                    quant: None,
                    ef: None,
                    setattr: None,
                    m: None,
                    cas: false,
                    is_redis_vset: false,
                }))
            }
        }
        "VQUERY" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'vquery' command".to_string());
            }
            let key = args[1].clone();
            let k: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            let mut query = Vec::with_capacity(args.len() - 3);
            let mut rerank = false;
            for a in &args[3..] {
                let s = String::from_utf8_lossy(a).to_uppercase();
                if s == "RERANK" {
                    rerank = true;
                } else {
                    let val: f32 = s.parse().map_err(|_| "not a valid float")?;
                    query.push(val);
                }
            }
            Ok(Some(Command::Vquery {
                key,
                k,
                query,
                rerank,
            }))
        }
        "CRDT.SET" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'crdt.set' command".to_string());
            }
            Ok(Some(Command::CrdtSet {
                key: args[1].clone(),
                val: args[2].clone(),
            }))
        }
        "CRDT.GET" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'crdt.get' command".to_string());
            }
            Ok(Some(Command::CrdtGet(args[1].clone())))
        }
        "CRDT.DEL" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'crdt.del' command".to_string());
            }
            Ok(Some(Command::CrdtDel(args[1].clone())))
        }
        "CRDT.INCRBY" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'crdt.incrby' command".to_string());
            }
            let delta: i64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            Ok(Some(Command::CrdtIncrby {
                key: args[1].clone(),
                delta,
            }))
        }
        "CRDT.SADD" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'crdt.sadd' command".to_string());
            }
            Ok(Some(Command::CrdtSadd {
                key: args[1].clone(),
                member: args[2].clone(),
            }))
        }
        "CRDT.SMEMBERS" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'crdt.smembers' command".to_string());
            }
            Ok(Some(Command::CrdtSmembers(args[1].clone())))
        }
        "CRDT.SREM" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'crdt.srem' command".to_string());
            }
            Ok(Some(Command::CrdtSrem {
                key: args[1].clone(),
                member: args[2].clone(),
            }))
        }
        "CRDT.DUMP" => Ok(Some(Command::CrdtDump)),
        "CRDT.MERGE" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'crdt.merge' command".to_string());
            }
            Ok(Some(Command::CrdtMerge(args[1].clone())))
        }
        "CRDT.GC" => {
            use crate::crdt::GcHorizon;
            let int = |arg: &Bytes| -> Result<u64, String> {
                std::str::from_utf8(arg)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| "value is not an integer or out of range".to_string())
            };
            let horizon = if args.len() == 3 && args[1].eq_ignore_ascii_case(b"BEFORE") {
                GcHorizon::Before(int(&args[2])?)
            } else if args.len() > 1 {
                GcHorizon::Ttl(Some(int(&args[1])?))
            } else {
                GcHorizon::Ttl(None)
            };
            Ok(Some(Command::CrdtGc(horizon)))
        }
        "VSIM" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'vsim' command".to_string());
            }
            let key = args[1].clone();
            let mode = String::from_utf8_lossy(&args[2]).to_ascii_uppercase();
            if mode == "ELE" || mode == "FP32" || mode == "VALUES" {
                let mut i = 2;
                let target = if mode == "ELE" {
                    let elem = args[3].clone();
                    i += 2;
                    VsimTarget::Element(elem)
                } else if mode == "FP32" {
                    let blob = &args[3];
                    if blob.is_empty() || !blob.len().is_multiple_of(4) {
                        return Err("Invalid FP32 vector blob length".to_string());
                    }
                    let mut v = Vec::with_capacity(blob.len() / 4);
                    for chunk in blob.as_chunks::<4>().0 {
                        let f = f32::from_le_bytes(*chunk);
                        if !f.is_finite() {
                            return Err("Vector contains NaN or Inf".to_string());
                        }
                        v.push(f);
                    }
                    i += 2;
                    VsimTarget::Vector(v)
                } else {
                    let num: usize = std::str::from_utf8(&args[3])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range")?;
                    if num == 0 || num > args.len() - (i + 2) {
                        return Err("syntax error".to_string());
                    }
                    let mut v = Vec::with_capacity(num);
                    for a in &args[i + 2..i + 2 + num] {
                        let f: f32 = std::str::from_utf8(a)
                            .map_err(|_| "not a valid float")?
                            .parse()
                            .map_err(|_| "not a valid float")?;
                        if !f.is_finite() {
                            return Err("Vector contains NaN or Inf".to_string());
                        }
                        v.push(f);
                    }
                    i += 2 + num;
                    VsimTarget::Vector(v)
                };
                let mut with_scores = false;
                let mut with_attribs = false;
                let mut count = 10usize;
                let mut epsilon = None;
                let mut ef = None;
                let mut filter = None;
                let mut filter_ef = None;
                let mut truth = false;
                let mut no_thread = false;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
                    match opt.as_str() {
                        "WITHSCORES" => {
                            with_scores = true;
                            i += 1;
                        }
                        "WITHATTRIBS" => {
                            with_attribs = true;
                            i += 1;
                        }
                        "COUNT" => {
                            if i + 1 >= args.len() {
                                return Err("syntax error".to_string());
                            }
                            let c: usize = std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "value is not an integer or out of range")?
                                .parse()
                                .map_err(|_| "value is not an integer or out of range")?;
                            if c == 0 {
                                return Err("COUNT must be > 0".to_string());
                            }
                            count = c;
                            i += 2;
                        }
                        "EPSILON" => {
                            if i + 1 >= args.len() {
                                return Err("syntax error".to_string());
                            }
                            let ep: f32 = std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "not a valid float")?
                                .parse()
                                .map_err(|_| "not a valid float")?;
                            if !(0.0..=1.0).contains(&ep) {
                                return Err("EPSILON must be between 0 and 1".to_string());
                            }
                            epsilon = Some(ep);
                            i += 2;
                        }
                        "EF" => {
                            if i + 1 >= args.len() {
                                return Err("syntax error".to_string());
                            }
                            let val: usize = std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "value is not an integer or out of range")?
                                .parse()
                                .map_err(|_| "value is not an integer or out of range")?;
                            if val > crate::vector::MAX_VSET_EF {
                                return Err("invalid EF".to_string());
                            }
                            ef = Some(val.max(1));
                            i += 2;
                        }
                        "FILTER" => {
                            if i + 1 >= args.len() {
                                return Err("syntax error".to_string());
                            }
                            let expr = std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "Invalid FILTER expression")?
                                .to_string();
                            crate::vector::validate_vset_filter(&expr)?;
                            filter = Some(expr);
                            i += 2;
                        }
                        "FILTER-EF" => {
                            if i + 1 >= args.len() {
                                return Err("syntax error".to_string());
                            }
                            let val: usize = std::str::from_utf8(&args[i + 1])
                                .map_err(|_| "value is not an integer or out of range")?
                                .parse()
                                .map_err(|_| "value is not an integer or out of range")?;
                            filter_ef = Some(val.max(1));
                            i += 2;
                        }
                        "TRUTH" => {
                            truth = true;
                            i += 1;
                        }
                        "NOTHREAD" => {
                            no_thread = true;
                            i += 1;
                        }
                        _ => return Err("syntax error".to_string()),
                    }
                }
                Ok(Some(Command::Vsim {
                    key,
                    target,
                    with_scores,
                    with_attribs,
                    count,
                    epsilon,
                    ef,
                    filter,
                    filter_ef,
                    truth,
                    no_thread,
                }))
            } else {
                let k1 = args[2].clone();
                let k2 = args[3].clone();
                let metric = if args.len() > 4 {
                    let s = String::from_utf8_lossy(&args[4]);
                    s.parse::<crate::vector::VectorMetric>().ok()
                } else {
                    None
                };
                Ok(Some(Command::Vdist {
                    key,
                    k1,
                    k2,
                    metric,
                }))
            }
        }
        "VDEL" | "VREM" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'vrem' command".to_string());
            }
            Ok(Some(Command::Vdel {
                key: args[1].clone(),
                element: args[2].clone(),
            }))
        }
        "VINFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'vinfo' command".to_string());
            }
            Ok(Some(Command::Vinfo(args[1].clone())))
        }
        "VCARD" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'vcard' command".to_string());
            }
            Ok(Some(Command::Vcard(args[1].clone())))
        }
        "VDIM" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'vdim' command".to_string());
            }
            Ok(Some(Command::Vdim(args[1].clone())))
        }
        "VEMB" => {
            if args.len() != 3 && args.len() != 4 {
                return Err("wrong number of arguments for 'vemb' command".to_string());
            }
            let raw = if args.len() == 4 {
                if !String::from_utf8_lossy(&args[3]).eq_ignore_ascii_case("RAW") {
                    return Err("syntax error".to_string());
                }
                true
            } else {
                false
            };
            Ok(Some(Command::Vemb {
                key: args[1].clone(),
                element: args[2].clone(),
                raw,
            }))
        }
        "VLINKS" => {
            if args.len() != 3 && args.len() != 4 {
                return Err("wrong number of arguments for 'vlinks' command".to_string());
            }
            let with_scores = if args.len() == 4 {
                if !String::from_utf8_lossy(&args[3]).eq_ignore_ascii_case("WITHSCORES") {
                    return Err("syntax error".to_string());
                }
                true
            } else {
                false
            };
            Ok(Some(Command::Vlinks {
                key: args[1].clone(),
                element: args[2].clone(),
                with_scores,
            }))
        }
        "VRANDMEMBER" => {
            if args.len() != 2 && args.len() != 3 {
                return Err("wrong number of arguments for 'vrandmember' command".to_string());
            }
            let count = if args.len() == 3 {
                Some(parse_random_count(&args[2])?)
            } else {
                None
            };
            Ok(Some(Command::Vrandmember {
                key: args[1].clone(),
                count,
            }))
        }
        "VSETATTR" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'vsetattr' command".to_string());
            }
            let attr = std::str::from_utf8(&args[3])
                .map_err(|_| "Invalid JSON attributes")?
                .to_string();
            if !attr.trim().is_empty() && serde_json::from_str::<serde_json::Value>(&attr).is_err()
            {
                return Err("Invalid JSON in VSETATTR".to_string());
            }
            Ok(Some(Command::Vsetattr {
                key: args[1].clone(),
                element: args[2].clone(),
                attr,
            }))
        }
        "VGETATTR" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'vgetattr' command".to_string());
            }
            Ok(Some(Command::Vgetattr {
                key: args[1].clone(),
                element: args[2].clone(),
            }))
        }
        "VISMEMBER" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'vismember' command".to_string());
            }
            Ok(Some(Command::Vismember {
                key: args[1].clone(),
                element: args[2].clone(),
            }))
        }
        "FUNCTION" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'function' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "LOAD" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'function load' command".to_string()
                        );
                    }
                    let (replace, code) = if args.len() == 3 {
                        (false, args[2].clone())
                    } else if args.len() == 4 {
                        let opt = String::from_utf8_lossy(&args[2]).to_uppercase();
                        if opt == "REPLACE" {
                            (true, args[3].clone())
                        } else {
                            return Err(format!(
                                "ERR Unknown option given: '{}'",
                                String::from_utf8_lossy(&args[2])
                            ));
                        }
                    } else {
                        return Err(format!(
                            "ERR Unknown option given: '{}'",
                            String::from_utf8_lossy(&args[2])
                        ));
                    };
                    Ok(Some(Command::FunctionLoad { replace, code }))
                }
                "DUMP" => {
                    if args.len() != 2 {
                        return Err(
                            "wrong number of arguments for 'function dump' command".to_string()
                        );
                    }
                    Ok(Some(Command::FunctionDump))
                }
                "RESTORE" => {
                    if args.len() < 3 || args.len() > 4 {
                        return Err("ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.".to_string());
                    }
                    let payload = args[2].clone();
                    let policy = if args.len() == 4 {
                        let p = String::from_utf8_lossy(&args[3]).to_uppercase();
                        if p != "FLUSH" && p != "APPEND" && p != "REPLACE" {
                            return Err("ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.".to_string());
                        }
                        p
                    } else {
                        "APPEND".to_string()
                    };
                    Ok(Some(Command::FunctionRestore { payload, policy }))
                }
                "LIST" => {
                    let mut library_name_pattern = None;
                    let mut with_code = false;
                    let mut i = 2;
                    while i < args.len() {
                        let arg = String::from_utf8_lossy(&args[i]);
                        if arg.eq_ignore_ascii_case("WITHCODE") && !with_code {
                            with_code = true;
                        } else if arg.eq_ignore_ascii_case("LIBRARYNAME")
                            && library_name_pattern.is_none()
                        {
                            if i + 1 >= args.len() {
                                return Err("ERR library name argument was not given".to_string());
                            }
                            i += 1;
                            library_name_pattern =
                                Some(String::from_utf8_lossy(&args[i]).to_string());
                        } else {
                            let sanitized: String = arg
                                .chars()
                                .map(|c| if c == '\r' || c == '\n' { ' ' } else { c })
                                .collect();
                            return Err(format!("ERR Unknown argument {}", sanitized));
                        }
                        i += 1;
                    }
                    Ok(Some(Command::FunctionList {
                        library_name_pattern,
                        with_code,
                    }))
                }
                "FLUSH" => {
                    if args.len() > 3 {
                        return Err("ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.".to_string());
                    }
                    if args.len() == 3 {
                        let mode = String::from_utf8_lossy(&args[2]).to_uppercase();
                        if mode != "ASYNC" && mode != "SYNC" {
                            return Err(
                                "ERR FUNCTION FLUSH only supports SYNC|ASYNC option".to_string()
                            );
                        }
                    }
                    Ok(Some(Command::FunctionFlush))
                }
                "STATS" => Ok(Some(Command::FunctionStats)),
                "KILL" => Ok(Some(Command::FunctionKill)),
                "HELP" => Ok(Some(Command::Unknown("FUNCTION HELP".to_string()))),
                "DELETE" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'function delete' command".to_string()
                        );
                    }
                    let lib = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::FunctionDelete(lib)))
                }
                _ => Err(
                    "ERR unknown subcommand or wrong number of arguments for 'function' command"
                        .to_string(),
                ),
            }
        }
        "FCALL" | "FCALL_RO" => {
            let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
            let read_only = cmd_name == "FCALL_RO";
            if args.len() < 3 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            let function = String::from_utf8_lossy(&args[1]).to_string();
            let numkeys_str = std::str::from_utf8(&args[2]).unwrap_or("");
            if numkeys_str.starts_with('-') {
                return Err("ERR Number of keys can't be negative".to_string());
            }
            let numkeys: usize = match numkeys_str.parse::<usize>() {
                Ok(n) => n,
                Err(_) => {
                    return Err(
                        "ERR Bad number of keys provided, please give a non negative number"
                            .to_string(),
                    );
                }
            };
            if numkeys > args.len() - 3 {
                return Err("ERR Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let func_args = args[3 + numkeys..].to_vec();
            Ok(Some(Command::Fcall {
                function,
                keys,
                args: func_args,
                read_only,
                auth_user: String::new(),
            }))
        }
        "JSON.SET" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'json.set' command".to_string());
            }
            let key = args[1].clone();
            let path = String::from_utf8_lossy(&args[2]).to_string();
            let json_val = String::from_utf8_lossy(&args[3]).to_string();
            let mut nx = false;
            let mut xx = false;
            for arg in &args[4..] {
                let opt = String::from_utf8_lossy(arg).to_uppercase();
                if opt == "NX" {
                    nx = true;
                } else if opt == "XX" {
                    xx = true;
                }
            }
            Ok(Some(Command::JsonSet {
                key,
                path,
                json_val,
                nx,
                xx,
            }))
        }
        "JSON.GET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.get' command".to_string());
            }
            let key = args[1].clone();
            let paths = if args.len() > 2 {
                args[2..]
                    .iter()
                    .map(|a| String::from_utf8_lossy(a).to_string())
                    .collect()
            } else {
                vec!["$".to_string()]
            };
            Ok(Some(Command::JsonGet { key, paths }))
        }
        "JSON.DEL" | "JSON.FORGET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.del' command".to_string());
            }
            let key = args[1].clone();
            let path = if args.len() > 2 {
                Some(String::from_utf8_lossy(&args[2]).to_string())
            } else {
                None
            };
            Ok(Some(Command::JsonDel { key, path }))
        }
        "JSON.TYPE" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.type' command".to_string());
            }
            let key = args[1].clone();
            let path = if args.len() > 2 {
                Some(String::from_utf8_lossy(&args[2]).to_string())
            } else {
                None
            };
            Ok(Some(Command::JsonType { key, path }))
        }
        "JSON.NUMINCRBY" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'json.numincrby' command".to_string());
            }
            let key = args[1].clone();
            let path = String::from_utf8_lossy(&args[2]).to_string();
            let delta = String::from_utf8_lossy(&args[3])
                .parse::<f64>()
                .map_err(|_| "not a number")?;
            Ok(Some(Command::JsonNumIncrBy { key, path, delta }))
        }
        "JSON.NUMMULTBY" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'json.nummultby' command".to_string());
            }
            let key = args[1].clone();
            let path = String::from_utf8_lossy(&args[2]).to_string();
            let factor = String::from_utf8_lossy(&args[3])
                .parse::<f64>()
                .map_err(|_| "not a number")?;
            Ok(Some(Command::JsonNumMultBy { key, path, factor }))
        }
        "JSON.STRAPPEND" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'json.strappend' command".to_string());
            }
            let key = args[1].clone();
            let (path, value) = if args.len() == 3 {
                (None, String::from_utf8_lossy(&args[2]).to_string())
            } else {
                (
                    Some(String::from_utf8_lossy(&args[2]).to_string()),
                    String::from_utf8_lossy(&args[3]).to_string(),
                )
            };
            Ok(Some(Command::JsonStrAppend { key, path, value }))
        }
        "JSON.STRLEN" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.strlen' command".to_string());
            }
            let key = args[1].clone();
            let path = if args.len() > 2 {
                Some(String::from_utf8_lossy(&args[2]).to_string())
            } else {
                None
            };
            Ok(Some(Command::JsonStrLen { key, path }))
        }
        "JSON.ARRAPPEND" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'json.arrappend' command".to_string());
            }
            let key = args[1].clone();
            let path = String::from_utf8_lossy(&args[2]).to_string();
            let values = args[3..]
                .iter()
                .map(|a| String::from_utf8_lossy(a).to_string())
                .collect();
            Ok(Some(Command::JsonArrAppend { key, path, values }))
        }
        "JSON.ARRLEN" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.arrlen' command".to_string());
            }
            let key = args[1].clone();
            let path = if args.len() > 2 {
                Some(String::from_utf8_lossy(&args[2]).to_string())
            } else {
                None
            };
            Ok(Some(Command::JsonArrLen { key, path }))
        }
        "JSON.ARRPOP" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.arrpop' command".to_string());
            }
            let key = args[1].clone();
            let path = if args.len() > 2 {
                Some(String::from_utf8_lossy(&args[2]).to_string())
            } else {
                None
            };
            let index = if args.len() > 3 {
                String::from_utf8_lossy(&args[3]).parse::<isize>().ok()
            } else {
                None
            };
            Ok(Some(Command::JsonArrPop { key, path, index }))
        }
        "JSON.OBJKEYS" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.objkeys' command".to_string());
            }
            let key = args[1].clone();
            let path = if args.len() > 2 {
                Some(String::from_utf8_lossy(&args[2]).to_string())
            } else {
                None
            };
            Ok(Some(Command::JsonObjKeys { key, path }))
        }
        "JSON.OBJLEN" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.objlen' command".to_string());
            }
            let key = args[1].clone();
            let path = if args.len() > 2 {
                Some(String::from_utf8_lossy(&args[2]).to_string())
            } else {
                None
            };
            Ok(Some(Command::JsonObjLen { key, path }))
        }
        "JSON.TOGGLE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'json.toggle' command".to_string());
            }
            let key = args[1].clone();
            let path = String::from_utf8_lossy(&args[2]).to_string();
            Ok(Some(Command::JsonToggle { key, path }))
        }
        "JSON.CLEAR" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'json.clear' command".to_string());
            }
            let key = args[1].clone();
            let path = if args.len() > 2 {
                Some(String::from_utf8_lossy(&args[2]).to_string())
            } else {
                None
            };
            Ok(Some(Command::JsonClear { key, path }))
        }
        "JSON.MGET" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'json.mget' command".to_string());
            }
            let path = String::from_utf8_lossy(args.last().unwrap()).to_string();
            let keys = args[1..args.len() - 1].to_vec();
            Ok(Some(Command::JsonMget { keys, path }))
        }
        "GEOADD" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'geoadd' command".to_string());
            }
            let key = args[1].clone();
            let mut nx = false;
            let mut xx = false;
            let mut ch = false;
            let mut idx = 2;
            while idx < args.len() {
                let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
                if opt == "NX" {
                    nx = true;
                    idx += 1;
                } else if opt == "XX" {
                    xx = true;
                    idx += 1;
                } else if opt == "CH" {
                    ch = true;
                    idx += 1;
                } else {
                    break;
                }
            }
            if !(args.len() - idx).is_multiple_of(3) || idx == args.len() || (nx && xx) {
                return Err("syntax error".to_string());
            }
            let mut items = Vec::new();
            while idx < args.len() {
                let lon: f64 = std::str::from_utf8(&args[idx])
                    .map_err(|_| "value is not a valid float")?
                    .parse()
                    .map_err(|_| "value is not a valid float")?;
                let lat: f64 = std::str::from_utf8(&args[idx + 1])
                    .map_err(|_| "value is not a valid float")?
                    .parse()
                    .map_err(|_| "value is not a valid float")?;
                if lon < crate::geo::GEO_LON_MIN
                    || lon > crate::geo::GEO_LON_MAX
                    || lat < crate::geo::GEO_LAT_MIN
                    || lat > crate::geo::GEO_LAT_MAX
                {
                    return Err(format!(
                        "invalid longitude,latitude pair {:.6},{:.6}",
                        lon, lat
                    ));
                }
                let member = args[idx + 2].clone();
                items.push((lon, lat, member));
                idx += 3;
            }
            Ok(Some(Command::Geoadd {
                key,
                items,
                nx,
                xx,
                ch,
            }))
        }
        "GEODIST" => {
            if args.len() < 4 || args.len() > 5 {
                return Err("wrong number of arguments for 'geodist' command".to_string());
            }
            let key = args[1].clone();
            let m1 = args[2].clone();
            let m2 = args[3].clone();
            let unit = if args.len() == 5 {
                Some(crate::geo::GeoUnit::parse(&String::from_utf8_lossy(
                    &args[4],
                ))?)
            } else {
                None
            };
            Ok(Some(Command::Geodist { key, m1, m2, unit }))
        }
        "GEOPOS" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'geopos' command".to_string());
            }
            let key = args[1].clone();
            let members = args[2..].to_vec();
            Ok(Some(Command::Geopos { key, members }))
        }
        "GEOHASH" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'geohash' command".to_string());
            }
            let key = args[1].clone();
            let members = args[2..].to_vec();
            Ok(Some(Command::Geohash { key, members }))
        }
        "GEORADIUS" | "GEORADIUS_RO" => {
            let is_ro = args[0].eq_ignore_ascii_case(b"GEORADIUS_RO");
            if args.len() < 6 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    if is_ro { "georadius_ro" } else { "georadius" }
                ));
            }
            let key = args[1].clone();
            let lon: f64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not a valid float")?
                .parse()
                .map_err(|_| "value is not a valid float")?;
            let lat: f64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not a valid float")?
                .parse()
                .map_err(|_| "value is not a valid float")?;
            if lon < crate::geo::GEO_LON_MIN
                || lon > crate::geo::GEO_LON_MAX
                || lat < crate::geo::GEO_LAT_MIN
                || lat > crate::geo::GEO_LAT_MAX
            {
                return Err(format!(
                    "invalid longitude,latitude pair {:.6},{:.6}",
                    lon, lat
                ));
            }
            let radius: f64 = std::str::from_utf8(&args[4])
                .map_err(|_| "value is not a valid float")?
                .parse()
                .map_err(|_| "value is not a valid float")?;
            if radius < 0.0 {
                return Err("radius cannot be negative".to_string());
            }
            let unit = crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[5]))?;
            let mut withcoord = false;
            let mut withdist = false;
            let mut withhash = false;
            let mut any = false;
            let mut count = None;
            let mut asc = None;
            let mut store = None;
            let mut storedist = None;
            let mut idx = 6;
            while idx < args.len() {
                let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
                match opt.as_str() {
                    "WITHCOORD" => withcoord = true,
                    "WITHDIST" => withdist = true,
                    "WITHHASH" => withhash = true,
                    "ANY" => any = true,
                    "ASC" => asc = Some(true),
                    "DESC" => asc = Some(false),
                    "COUNT" => {
                        if idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        let c: usize = std::str::from_utf8(&args[idx])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if c == 0 {
                            return Err("COUNT must be > 0".to_string());
                        }
                        count = Some(c);
                        if idx + 1 < args.len() && args[idx + 1].eq_ignore_ascii_case(b"ANY") {
                            any = true;
                            idx += 1;
                        }
                    }
                    "STORE" => {
                        if is_ro || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        store = Some(args[idx].clone());
                    }
                    "STOREDIST" => {
                        if is_ro || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        storedist = Some(args[idx].clone());
                    }
                    _ => return Err("syntax error".to_string()),
                }
                idx += 1;
            }
            if any && count.is_none() {
                return Err("the ANY argument requires COUNT argument".to_string());
            }
            if (store.is_some() || storedist.is_some()) && (withdist || withhash || withcoord) {
                return Err("STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options".to_string());
            }
            Ok(Some(Command::Georadius {
                key,
                lon,
                lat,
                radius,
                unit,
                withcoord,
                withdist,
                withhash,
                count,
                any,
                asc,
                store,
                storedist,
            }))
        }
        "GEORADIUSBYMEMBER" | "GEORADIUSBYMEMBER_RO" => {
            let is_ro = args[0].eq_ignore_ascii_case(b"GEORADIUSBYMEMBER_RO");
            if args.len() < 5 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    if is_ro {
                        "georadiusbymember_ro"
                    } else {
                        "georadiusbymember"
                    }
                ));
            }
            let key = args[1].clone();
            let member = args[2].clone();
            let radius: f64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not a valid float")?
                .parse()
                .map_err(|_| "value is not a valid float")?;
            if radius < 0.0 {
                return Err("radius cannot be negative".to_string());
            }
            let unit = crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[4]))?;
            let mut withcoord = false;
            let mut withdist = false;
            let mut withhash = false;
            let mut any = false;
            let mut count = None;
            let mut asc = None;
            let mut store = None;
            let mut storedist = None;
            let mut idx = 5;
            while idx < args.len() {
                let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
                match opt.as_str() {
                    "WITHCOORD" => withcoord = true,
                    "WITHDIST" => withdist = true,
                    "WITHHASH" => withhash = true,
                    "ANY" => any = true,
                    "ASC" => asc = Some(true),
                    "DESC" => asc = Some(false),
                    "COUNT" => {
                        if idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        let c: usize = std::str::from_utf8(&args[idx])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if c == 0 {
                            return Err("COUNT must be > 0".to_string());
                        }
                        count = Some(c);
                        if idx + 1 < args.len() && args[idx + 1].eq_ignore_ascii_case(b"ANY") {
                            any = true;
                            idx += 1;
                        }
                    }
                    "STORE" => {
                        if is_ro || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        store = Some(args[idx].clone());
                    }
                    "STOREDIST" => {
                        if is_ro || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        storedist = Some(args[idx].clone());
                    }
                    _ => return Err("syntax error".to_string()),
                }
                idx += 1;
            }
            if any && count.is_none() {
                return Err("the ANY argument requires COUNT argument".to_string());
            }
            if (store.is_some() || storedist.is_some()) && (withdist || withhash || withcoord) {
                return Err("STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options".to_string());
            }
            Ok(Some(Command::Georadiusbymember {
                key,
                member,
                radius,
                unit,
                withcoord,
                withdist,
                withhash,
                count,
                any,
                asc,
                store,
                storedist,
            }))
        }
        "GEOSEARCH" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'geosearch' command".to_string());
            }
            let key = args[1].clone();
            let mut from_member = None;
            let mut from_lonlat = None;
            let mut by_radius = None;
            let mut by_box = None;
            let mut asc = None;
            let mut count = None;
            let mut any = false;
            let mut withcoord = false;
            let mut withdist = false;
            let mut withhash = false;
            let mut idx = 2;
            while idx < args.len() {
                let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
                match opt.as_str() {
                    "FROMMEMBER" => {
                        if from_lonlat.is_some() || from_member.is_some() || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        from_member = Some(args[idx].clone());
                    }
                    "FROMLONLAT" => {
                        if from_member.is_some() || from_lonlat.is_some() || idx + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let lon: f64 = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        let lat: f64 = std::str::from_utf8(&args[idx + 2])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        if lon < crate::geo::GEO_LON_MIN
                            || lon > crate::geo::GEO_LON_MAX
                            || lat < crate::geo::GEO_LAT_MIN
                            || lat > crate::geo::GEO_LAT_MAX
                        {
                            return Err(format!(
                                "invalid longitude,latitude pair {:.6},{:.6}",
                                lon, lat
                            ));
                        }
                        idx += 2;
                        from_lonlat = Some((lon, lat));
                    }
                    "BYRADIUS" => {
                        if by_box.is_some() || by_radius.is_some() || idx + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let rad: f64 = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        if rad < 0.0 {
                            return Err("radius cannot be negative".to_string());
                        }
                        let u =
                            crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[idx + 2]))?;
                        idx += 2;
                        by_radius = Some((rad, u));
                    }
                    "BYBOX" => {
                        if by_radius.is_some() || by_box.is_some() || idx + 3 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let w: f64 = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        let h: f64 = std::str::from_utf8(&args[idx + 2])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        if w < 0.0 || h < 0.0 {
                            return Err("height or width cannot be negative".to_string());
                        }
                        let u =
                            crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[idx + 3]))?;
                        idx += 3;
                        by_box = Some((w, h, u));
                    }
                    "ASC" => asc = Some(true),
                    "DESC" => asc = Some(false),
                    "ANY" => any = true,
                    "COUNT" => {
                        if idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        let c: usize = std::str::from_utf8(&args[idx])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if c == 0 {
                            return Err("COUNT must be > 0".to_string());
                        }
                        count = Some(c);
                        if idx + 1 < args.len() && args[idx + 1].eq_ignore_ascii_case(b"ANY") {
                            any = true;
                            idx += 1;
                        }
                    }
                    "WITHCOORD" => withcoord = true,
                    "WITHDIST" => withdist = true,
                    "WITHHASH" => withhash = true,
                    _ => return Err("syntax error".to_string()),
                }
                idx += 1;
            }
            if from_member.is_none() && from_lonlat.is_none() {
                return Err(
                    "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH"
                        .to_string(),
                );
            }
            if by_radius.is_none() && by_box.is_none() {
                return Err(
                    "exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH".to_string(),
                );
            }
            if any && count.is_none() {
                return Err("the ANY argument requires COUNT argument".to_string());
            }
            Ok(Some(Command::Geosearch {
                key,
                from_member,
                from_lonlat,
                by_radius,
                by_box,
                asc,
                count,
                any,
                withcoord,
                withdist,
                withhash,
            }))
        }
        "GEOSEARCHSTORE" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'geosearchstore' command".to_string());
            }
            let dest = args[1].clone();
            let key = args[2].clone();
            let mut from_member = None;
            let mut from_lonlat = None;
            let mut by_radius = None;
            let mut by_box = None;
            let mut asc = None;
            let mut count = None;
            let mut any = false;
            let mut storedist = false;
            let mut idx = 3;
            while idx < args.len() {
                let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
                match opt.as_str() {
                    "FROMMEMBER" => {
                        if from_lonlat.is_some() || from_member.is_some() || idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        from_member = Some(args[idx].clone());
                    }
                    "FROMLONLAT" => {
                        if from_member.is_some() || from_lonlat.is_some() || idx + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let lon: f64 = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        let lat: f64 = std::str::from_utf8(&args[idx + 2])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        if lon < crate::geo::GEO_LON_MIN
                            || lon > crate::geo::GEO_LON_MAX
                            || lat < crate::geo::GEO_LAT_MIN
                            || lat > crate::geo::GEO_LAT_MAX
                        {
                            return Err(format!(
                                "invalid longitude,latitude pair {:.6},{:.6}",
                                lon, lat
                            ));
                        }
                        idx += 2;
                        from_lonlat = Some((lon, lat));
                    }
                    "BYRADIUS" => {
                        if by_box.is_some() || by_radius.is_some() || idx + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let rad: f64 = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        if rad < 0.0 {
                            return Err("radius cannot be negative".to_string());
                        }
                        let u =
                            crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[idx + 2]))?;
                        idx += 2;
                        by_radius = Some((rad, u));
                    }
                    "BYBOX" => {
                        if by_radius.is_some() || by_box.is_some() || idx + 3 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let w: f64 = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        let h: f64 = std::str::from_utf8(&args[idx + 2])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        if w < 0.0 || h < 0.0 {
                            return Err("height or width cannot be negative".to_string());
                        }
                        let u =
                            crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[idx + 3]))?;
                        idx += 3;
                        by_box = Some((w, h, u));
                    }
                    "ASC" => asc = Some(true),
                    "DESC" => asc = Some(false),
                    "ANY" => any = true,
                    "COUNT" => {
                        if idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        let c: usize = std::str::from_utf8(&args[idx])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
                        if c == 0 {
                            return Err("COUNT must be > 0".to_string());
                        }
                        count = Some(c);
                        if idx + 1 < args.len() && args[idx + 1].eq_ignore_ascii_case(b"ANY") {
                            any = true;
                            idx += 1;
                        }
                    }
                    "STOREDIST" => storedist = true,
                    "WITHCOORD" | "WITHDIST" | "WITHHASH" => {
                        return Err("GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options".to_string());
                    }
                    _ => return Err("syntax error".to_string()),
                }
                idx += 1;
            }
            if from_member.is_none() && from_lonlat.is_none() {
                return Err(
                    "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCHSTORE"
                        .to_string(),
                );
            }
            if by_radius.is_none() && by_box.is_none() {
                return Err(
                    "exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCHSTORE"
                        .to_string(),
                );
            }
            if any && count.is_none() {
                return Err("the ANY argument requires COUNT argument".to_string());
            }
            Ok(Some(Command::Geosearchstore {
                dest,
                key,
                from_member,
                from_lonlat,
                by_radius,
                by_box,
                asc,
                count,
                any,
                storedist,
            }))
        }
        "BF.RESERVE" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'bf.reserve' command".to_string());
            }
            let key = args[1].clone();
            let error_rate: f64 = std::str::from_utf8(&args[2])
                .map_err(|_| "not a valid float")?
                .parse()
                .map_err(|_| "not a valid float")?;
            // Written so NaN is refused too.
            if !(error_rate > 0.0 && error_rate < 1.0) {
                return Err("(0 < error rate range < 1)".to_string());
            }
            let capacity: usize = std::str::from_utf8(&args[3])
                .map_err(|_| "not an integer")?
                .parse()
                .map_err(|_| "not an integer")?;
            Ok(Some(Command::BfReserve {
                key,
                error_rate,
                capacity,
            }))
        }
        "BF.ADD" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'bf.add' command".to_string());
            }
            Ok(Some(Command::BfAdd {
                key: args[1].clone(),
                item: args[2].clone(),
            }))
        }
        "BF.MADD" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'bf.madd' command".to_string());
            }
            Ok(Some(Command::BfMadd {
                key: args[1].clone(),
                items: args[2..].to_vec(),
            }))
        }
        "BF.EXISTS" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'bf.exists' command".to_string());
            }
            Ok(Some(Command::BfExists {
                key: args[1].clone(),
                item: args[2].clone(),
            }))
        }
        "BF.MEXISTS" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'bf.mexists' command".to_string());
            }
            Ok(Some(Command::BfMexists {
                key: args[1].clone(),
                items: args[2..].to_vec(),
            }))
        }
        "BF.INFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'bf.info' command".to_string());
            }
            Ok(Some(Command::BfInfo(args[1].clone())))
        }
        "CF.RESERVE" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'cf.reserve' command".to_string());
            }
            let key = args[1].clone();
            let capacity: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "not an integer")?
                .parse()
                .map_err(|_| "not an integer")?;
            Ok(Some(Command::CfReserve { key, capacity }))
        }
        "CF.ADD" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'cf.add' command".to_string());
            }
            Ok(Some(Command::CfAdd {
                key: args[1].clone(),
                item: args[2].clone(),
            }))
        }
        "CF.ADDNX" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'cf.addnx' command".to_string());
            }
            Ok(Some(Command::CfAddnx {
                key: args[1].clone(),
                item: args[2].clone(),
            }))
        }
        "CF.EXISTS" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'cf.exists' command".to_string());
            }
            Ok(Some(Command::CfExists {
                key: args[1].clone(),
                item: args[2].clone(),
            }))
        }
        "CF.DEL" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'cf.del' command".to_string());
            }
            Ok(Some(Command::CfDel {
                key: args[1].clone(),
                item: args[2].clone(),
            }))
        }
        "CF.INFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'cf.info' command".to_string());
            }
            Ok(Some(Command::CfInfo(args[1].clone())))
        }
        "CMS.INITBYDIM" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'cms.initbydim' command".to_string());
            }
            let key = args[1].clone();
            let width: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "not an integer")?
                .parse()
                .map_err(|_| "not an integer")?;
            let depth: usize = std::str::from_utf8(&args[3])
                .map_err(|_| "not an integer")?
                .parse()
                .map_err(|_| "not an integer")?;
            Ok(Some(Command::CmsInitbydim { key, width, depth }))
        }
        "CMS.INITBYPROB" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'cms.initbyprob' command".to_string());
            }
            let key = args[1].clone();
            let error: f64 = std::str::from_utf8(&args[2])
                .map_err(|_| "not a valid float")?
                .parse()
                .map_err(|_| "not a valid float")?;
            let probability: f64 = std::str::from_utf8(&args[3])
                .map_err(|_| "not a valid float")?
                .parse()
                .map_err(|_| "not a valid float")?;
            Ok(Some(Command::CmsInitbyprob {
                key,
                error,
                probability,
            }))
        }
        "CMS.INCRBY" => {
            if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                return Err("wrong number of arguments for 'cms.incrby' command".to_string());
            }
            let key = args[1].clone();
            let mut pairs = Vec::new();
            let mut i = 2;
            while i < args.len() {
                let item = args[i].clone();
                let inc: u64 = std::str::from_utf8(&args[i + 1])
                    .map_err(|_| "not an integer")?
                    .parse()
                    .map_err(|_| "not an integer")?;
                pairs.push((item, inc));
                i += 2;
            }
            Ok(Some(Command::CmsIncrby { key, pairs }))
        }
        "CMS.QUERY" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'cms.query' command".to_string());
            }
            Ok(Some(Command::CmsQuery {
                key: args[1].clone(),
                items: args[2..].to_vec(),
            }))
        }
        "CMS.INFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'cms.info' command".to_string());
            }
            Ok(Some(Command::CmsInfo(args[1].clone())))
        }
        "TOPK.RESERVE" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'topk.reserve' command".to_string());
            }
            let key = args[1].clone();
            let topk: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "not an integer")?
                .parse()
                .map_err(|_| "not an integer")?;
            Ok(Some(Command::TopkReserve { key, topk }))
        }
        "TOPK.ADD" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'topk.add' command".to_string());
            }
            Ok(Some(Command::TopkAdd {
                key: args[1].clone(),
                items: args[2..].to_vec(),
            }))
        }
        "TOPK.QUERY" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'topk.query' command".to_string());
            }
            Ok(Some(Command::TopkQuery {
                key: args[1].clone(),
                items: args[2..].to_vec(),
            }))
        }
        "TOPK.LIST" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'topk.list' command".to_string());
            }
            Ok(Some(Command::TopkList(args[1].clone())))
        }
        "TOPK.INFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'topk.info' command".to_string());
            }
            Ok(Some(Command::TopkInfo(args[1].clone())))
        }
        "BF.RESTORE" | "CF.RESTORE" | "CMS.RESTORE" | "TOPK.RESTORE" => {
            use crate::probabilistic::ProbKind;
            let kind = match cmd_name {
                "BF.RESTORE" => ProbKind::Bloom,
                "CF.RESTORE" => ProbKind::Cuckoo,
                "CMS.RESTORE" => ProbKind::Cms,
                _ => ProbKind::TopK,
            };
            if args.len() != 3 {
                return Err(format!(
                    "wrong number of arguments for '{}' command",
                    kind.restore_command().to_ascii_lowercase()
                ));
            }
            Ok(Some(Command::ProbRestore {
                kind,
                key: args[1].clone(),
                payload: args[2].clone(),
            }))
        }
        "FT.CREATE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'ft.create' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let mut on_type = "HASH".to_string();
            let mut prefixes = Vec::new();
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "ON" && i + 1 < args.len() {
                    on_type = String::from_utf8_lossy(&args[i + 1]).to_uppercase();
                    i += 2;
                } else if opt == "PREFIX" && i + 1 < args.len() {
                    let count: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(1);
                    i += 2;
                    for arg in counted_args(&args, i, count, "PREFIX")? {
                        prefixes.push(String::from_utf8_lossy(arg).to_string());
                    }
                    i += count;
                } else if opt == "SCHEMA" {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }

            let mut fields = std::collections::HashMap::new();
            let mut schema_fields = Vec::new();
            while i < args.len() {
                let identifier = String::from_utf8_lossy(&args[i]).to_string();
                i += 1;
                if i >= args.len() {
                    break;
                }
                let mut alias = identifier.clone();
                if i < args.len() && String::from_utf8_lossy(&args[i]).to_uppercase() == "AS" {
                    if i + 1 < args.len() {
                        alias = String::from_utf8_lossy(&args[i + 1]).to_string();
                        i += 2;
                    }
                } else if alias.starts_with("$.") {
                    alias = alias
                        .trim_start_matches("$.")
                        .trim_end_matches(".*")
                        .trim_end_matches("[*]")
                        .to_string();
                }

                if i >= args.len() {
                    break;
                }

                let ftype_str = String::from_utf8_lossy(&args[i]).to_uppercase();
                i += 1;
                let field_type = match ftype_str.as_str() {
                    "TEXT" => {
                        let mut weight = 1.0;
                        let mut sortable = false;
                        let mut nostem = false;
                        while i < args.len() {
                            let sub_opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                            if sub_opt == "WEIGHT" && i + 1 < args.len() {
                                weight =
                                    String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(1.0);
                                i += 2;
                            } else if sub_opt == "SORTABLE" {
                                sortable = true;
                                i += 1;
                            } else if sub_opt == "NOSTEM" {
                                nostem = true;
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        Some(crate::search::FieldType::Text {
                            weight,
                            sortable,
                            nostem,
                        })
                    }
                    "NUMERIC" => {
                        let mut sortable = false;
                        if i < args.len()
                            && String::from_utf8_lossy(&args[i]).to_uppercase() == "SORTABLE"
                        {
                            sortable = true;
                            i += 1;
                        }
                        Some(crate::search::FieldType::Numeric { sortable })
                    }
                    "TAG" => {
                        let mut separator = ',';
                        let mut casesensitive = false;
                        while i < args.len() {
                            let sub_opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                            if sub_opt == "SEPARATOR" && i + 1 < args.len() {
                                separator = String::from_utf8_lossy(&args[i + 1])
                                    .chars()
                                    .next()
                                    .unwrap_or(',');
                                i += 2;
                            } else if sub_opt == "CASESENSITIVE" {
                                casesensitive = true;
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        Some(crate::search::FieldType::Tag {
                            separator,
                            casesensitive,
                        })
                    }
                    "VECTOR" => {
                        if i >= args.len() {
                            return Err(
                                "Bad arguments for vector field: missing algorithm".to_string()
                            );
                        }
                        let algorithm = String::from_utf8_lossy(&args[i]).to_uppercase();
                        if algorithm != "HNSW" && algorithm != "FLAT" {
                            return Err(format!(
                                "Bad arguments for vector similarity algorithm: {}",
                                algorithm
                            ));
                        }
                        i += 1;
                        // `<nargs>` is mandatory in RediSearch; accept its absence for
                        // backward compatibility and parse attribute pairs greedily instead.
                        let nargs = if i < args.len() {
                            String::from_utf8_lossy(&args[i]).parse::<usize>().ok()
                        } else {
                            None
                        };
                        let end = match nargs {
                            Some(n) => {
                                i += 1;
                                if n % 2 != 0 || n > args.len() - i {
                                    return Err(format!(
                                        "Bad arguments for vector similarity {} index arguments",
                                        algorithm
                                    ));
                                }
                                i + n
                            }
                            None => args.len(),
                        };
                        let mut dim: Option<usize> = None;
                        let mut distance_metric: Option<String> = None;
                        let mut data_type: Option<crate::search::VectorDataType> = None;
                        let mut attrs = crate::search::VectorFieldAttrs::default();
                        let bad = |attr: &str| {
                            format!(
                                "Bad arguments for vector similarity {} argument {}",
                                algorithm, attr
                            )
                        };
                        while i + 1 < args.len() && (nargs.is_none() || i < end) {
                            let attr = String::from_utf8_lossy(&args[i]).to_uppercase();
                            let val = String::from_utf8_lossy(&args[i + 1]).to_string();
                            let parse_usize = |v: &str| v.parse::<usize>().map_err(|_| bad(&attr));
                            match attr.as_str() {
                                "TYPE" => {
                                    data_type = Some(
                                        crate::search::VectorDataType::parse(&val)
                                            .ok_or_else(|| bad("TYPE"))?,
                                    );
                                }
                                "DIM" => {
                                    let d = parse_usize(&val)?;
                                    if d == 0 || d > crate::vector::MAX_VECTOR_DIM {
                                        return Err(bad("DIM"));
                                    }
                                    dim = Some(d);
                                }
                                "DISTANCE_METRIC" => {
                                    let m = val.to_uppercase();
                                    if !matches!(m.as_str(), "L2" | "IP" | "COSINE") {
                                        return Err(bad("DISTANCE_METRIC"));
                                    }
                                    distance_metric = Some(m);
                                }
                                "INITIAL_CAP" => attrs.initial_cap = parse_usize(&val)?,
                                "BLOCK_SIZE" if algorithm == "FLAT" => {
                                    attrs.block_size = parse_usize(&val)?
                                }
                                "M" if algorithm == "HNSW" => {
                                    attrs.m = parse_usize(&val)?;
                                    if attrs.m > crate::vector::MAX_HNSW_M {
                                        return Err(bad("M"));
                                    }
                                }
                                "EF_CONSTRUCTION" if algorithm == "HNSW" => {
                                    attrs.ef_construction = parse_usize(&val)?
                                }
                                "EF_RUNTIME" if algorithm == "HNSW" => {
                                    attrs.ef_runtime = parse_usize(&val)?
                                }
                                "EPSILON" if algorithm == "HNSW" => {
                                    attrs.epsilon = val
                                        .parse::<f64>()
                                        .ok()
                                        .filter(|e| *e > 0.0)
                                        .ok_or_else(|| bad("EPSILON"))?
                                }
                                _ if nargs.is_none() => break,
                                _ => return Err(bad(&attr)),
                            }
                            i += 2;
                        }
                        if nargs.is_some() {
                            i = end;
                        }
                        let Some(dim) = dim else {
                            return Err(format!(
                                "Missing mandatory parameter: cannot create {} index without specifying DIM",
                                algorithm
                            ));
                        };
                        let Some(distance_metric) = distance_metric else {
                            return Err(format!(
                                "Missing mandatory parameter: cannot create {} index without specifying DISTANCE_METRIC",
                                algorithm
                            ));
                        };
                        let Some(data_type) = data_type else {
                            return Err(format!(
                                "Missing mandatory parameter: cannot create {} index without specifying TYPE",
                                algorithm
                            ));
                        };
                        attrs.data_type = data_type;
                        Some(crate::search::FieldType::Vector {
                            dim,
                            distance_metric,
                            algorithm,
                            attrs,
                        })
                    }
                    _ => None,
                };

                if let Some(ftype) = field_type {
                    fields.insert(alias.clone(), ftype.clone());
                    if alias != identifier {
                        fields.insert(identifier.clone(), ftype.clone());
                    }
                    schema_fields.push(crate::search::SchemaField {
                        identifier,
                        alias,
                        field_type: ftype,
                    });
                }
            }
            Ok(Some(Command::FtCreate {
                index,
                on_type,
                prefixes,
                fields,
                schema_fields,
            }))
        }
        "FT.SEARCH" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'ft.search' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let query = String::from_utf8_lossy(&args[2]).to_string();
            let mut options = crate::search::SearchOptions::default();

            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "NOCONTENT" {
                    options.nocontent = true;
                    i += 1;
                } else if opt == "WITHSCORES" {
                    options.withscores = true;
                    i += 1;
                } else if opt == "RRF" {
                    let mut k_val = 60.0;
                    if i + 1 < args.len()
                        && let Ok(parsed_k) = String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                        && parsed_k > 0.0
                    {
                        k_val = parsed_k;
                        i += 2;
                    } else {
                        i += 1;
                    }
                    options.rrf_k = Some(k_val);
                    options.linear_weights = None;
                } else if opt == "LINEAR" {
                    let mut alpha = 0.5;
                    let mut beta = 0.5;
                    if i + 2 < args.len()
                        && let Ok(a) = String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                        && let Ok(b) = String::from_utf8_lossy(&args[i + 2]).parse::<f64>()
                    {
                        alpha = a;
                        beta = b;
                        i += 3;
                    } else {
                        i += 1;
                    }
                    options.linear_weights = Some((alpha, beta));
                    options.rrf_k = None;
                } else if opt == "SCORER" && i + 1 < args.len() {
                    let scorer = String::from_utf8_lossy(&args[i + 1]).to_uppercase();
                    i += 2;
                    if scorer == "RRF" {
                        let mut k_val = 60.0;
                        if i + 1 < args.len()
                            && String::from_utf8_lossy(&args[i]).eq_ignore_ascii_case("K")
                            && let Ok(parsed_k) =
                                String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                            && parsed_k > 0.0
                        {
                            k_val = parsed_k;
                            i += 2;
                        }
                        options.rrf_k = Some(k_val);
                        options.linear_weights = None;
                    } else if scorer == "LINEAR" {
                        let mut alpha = 0.5;
                        let mut beta = 0.5;
                        while i + 1 < args.len() {
                            let sub = String::from_utf8_lossy(&args[i]).to_uppercase();
                            if sub == "ALPHA"
                                && let Ok(a) = String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                            {
                                alpha = a;
                                i += 2;
                            } else if sub == "BETA"
                                && let Ok(b) = String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                            {
                                beta = b;
                                i += 2;
                            } else {
                                break;
                            }
                        }
                        options.linear_weights = Some((alpha, beta));
                        options.rrf_k = None;
                    }
                } else if opt == "LIMIT" && i + 2 < args.len() {
                    options.offset = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    options.limit = String::from_utf8_lossy(&args[i + 2]).parse().unwrap_or(10);
                    i += 3;
                } else if opt == "SORTBY" && i + 1 < args.len() {
                    let field = String::from_utf8_lossy(&args[i + 1]).to_string();
                    let mut asc = true;
                    i += 2;
                    if i < args.len() {
                        let dir = String::from_utf8_lossy(&args[i]).to_uppercase();
                        if dir == "DESC" {
                            asc = false;
                            i += 1;
                        } else if dir == "ASC" {
                            asc = true;
                            i += 1;
                        }
                    }
                    options.sortby = Some((field, asc));
                } else if opt == "RETURN" && i + 1 < args.len() {
                    let count: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    let r_fields = counted_args(&args, i, count, "RETURN")?
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).to_string())
                        .collect();
                    i += count;
                    options.return_fields = Some(r_fields);
                } else if opt == "PARAMS" && i + 1 < args.len() {
                    let count: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    i += parse_ft_params(&args, i, count, &mut options.params)?;
                } else if opt == "DIALECT" && i + 1 < args.len() {
                    let d: u32 = String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .map_err(|_| "DIALECT requires a non-negative integer".to_string())?;
                    if !(1..=4).contains(&d) {
                        return Err(format!("Unsupported dialect version {}", d));
                    }
                    options.dialect = Some(d);
                    i += 2;
                } else if opt == "TIMEOUT" && i + 1 < args.len() {
                    let ms: u64 = String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .map_err(|_| "TIMEOUT requires a non-negative integer".to_string())?;
                    options.timeout_ms = Some(ms);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            Ok(Some(Command::FtSearch {
                index,
                query,
                options,
            }))
        }
        "FT.AGGREGATE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'ft.aggregate' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let query = String::from_utf8_lossy(&args[2]).to_string();
            let mut options = crate::search::AggregateOptions::default();

            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "LOAD" && i + 1 < args.len() {
                    let nargs: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    for a in counted_args(&args, i, nargs, "LOAD")? {
                        options.load_fields.push(strip_field_sigil(a));
                    }
                    i += nargs;
                } else if opt == "GROUPBY" && i + 1 < args.len() {
                    let num_fields: usize =
                        String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    let group_fields: Vec<String> = counted_args(&args, i, num_fields, "GROUPBY")?
                        .iter()
                        .map(strip_field_sigil)
                        .collect();
                    i += num_fields;
                    let mut reducers = Vec::new();
                    while i < args.len()
                        && String::from_utf8_lossy(&args[i]).to_uppercase() == "REDUCE"
                    {
                        i += 1;
                        if i >= args.len() {
                            break;
                        }
                        let func = String::from_utf8_lossy(&args[i]).to_uppercase();
                        i += 1;
                        let nargs: usize = if i < args.len() {
                            String::from_utf8_lossy(&args[i]).parse().unwrap_or(0)
                        } else {
                            0
                        };
                        i += 1;
                        let reduce_args: Vec<String> = counted_args(&args, i, nargs, "REDUCE")?
                            .iter()
                            .map(strip_field_sigil)
                            .collect();
                        i += nargs;
                        let mut alias = func.to_lowercase();
                        if i + 1 < args.len()
                            && String::from_utf8_lossy(&args[i]).to_uppercase() == "AS"
                        {
                            alias = String::from_utf8_lossy(&args[i + 1]).to_string();
                            i += 2;
                        }
                        match func.as_str() {
                            "COUNT" => reducers.push(crate::search::Reducer::Count { alias }),
                            "SUM" => {
                                let field = reduce_args.into_iter().next().unwrap_or_default();
                                reducers.push(crate::search::Reducer::Sum { field, alias });
                            }
                            "AVG" => {
                                let field = reduce_args.into_iter().next().unwrap_or_default();
                                reducers.push(crate::search::Reducer::Avg { field, alias });
                            }
                            "MIN" => {
                                let field = reduce_args.into_iter().next().unwrap_or_default();
                                reducers.push(crate::search::Reducer::Min { field, alias });
                            }
                            "MAX" => {
                                let field = reduce_args.into_iter().next().unwrap_or_default();
                                reducers.push(crate::search::Reducer::Max { field, alias });
                            }
                            _ => {}
                        }
                    }
                    options.stages.push(crate::search::AggregateStage::Group(
                        crate::search::GroupByStage {
                            fields: group_fields,
                            reducers,
                        },
                    ));
                } else if opt == "APPLY" && i + 1 < args.len() {
                    let expr = String::from_utf8_lossy(&args[i + 1]).to_string();
                    i += 2;
                    let mut alias = expr.clone();
                    if i + 1 < args.len()
                        && String::from_utf8_lossy(&args[i]).to_uppercase() == "AS"
                    {
                        alias = String::from_utf8_lossy(&args[i + 1]).to_string();
                        i += 2;
                    }
                    options.stages.push(crate::search::AggregateStage::Apply(
                        crate::search::ApplyStage { expr, alias },
                    ));
                } else if opt == "SORTBY" && i + 1 < args.len() {
                    let nargs: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    let mut sort_fields = Vec::new();
                    let mut count = 0;
                    while count < nargs && i < args.len() {
                        let mut f = String::from_utf8_lossy(&args[i]).to_string();
                        if let Some(stripped) = f.strip_prefix('@') {
                            f = stripped.to_string();
                        }
                        i += 1;
                        count += 1;
                        let mut asc = true;
                        if count < nargs && i < args.len() {
                            let dir = String::from_utf8_lossy(&args[i]).to_uppercase();
                            if dir == "DESC" {
                                asc = false;
                                i += 1;
                                count += 1;
                            } else if dir == "ASC" {
                                asc = true;
                                i += 1;
                                count += 1;
                            }
                        }
                        sort_fields.push((f, asc));
                    }
                    options.stages.push(crate::search::AggregateStage::Sort(
                        crate::search::SortByStage {
                            fields: sort_fields,
                            max: None,
                        },
                    ));
                } else if opt == "LIMIT" && i + 2 < args.len() {
                    let offset: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    let num: usize = String::from_utf8_lossy(&args[i + 2]).parse().unwrap_or(10);
                    i += 3;
                    options
                        .stages
                        .push(crate::search::AggregateStage::Limit { offset, num });
                } else if opt == "FILTER" && i + 1 < args.len() {
                    let expr = String::from_utf8_lossy(&args[i + 1]).to_string();
                    i += 2;
                    options
                        .stages
                        .push(crate::search::AggregateStage::Filter(expr));
                } else if (opt == "DIALECT" || opt == "TIMEOUT") && i + 1 < args.len() {
                    i += 2;
                } else {
                    i += 1;
                }
            }

            Ok(Some(Command::FtAggregate {
                index,
                query,
                options,
            }))
        }
        "FT.INFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'ft.info' command".to_string());
            }
            Ok(Some(Command::FtInfo(
                String::from_utf8_lossy(&args[1]).to_string(),
            )))
        }
        "FT.DROPINDEX" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'ft.dropindex' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let dd = args.len() >= 3 && String::from_utf8_lossy(&args[2]).to_uppercase() == "DD";
            Ok(Some(Command::FtDropIndex { index, dd }))
        }
        "FT.EXPLAIN" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'ft.explain' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let query = String::from_utf8_lossy(&args[2]).to_string();
            Ok(Some(Command::FtExplain { index, query }))
        }
        "FT.ADD" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'ft.add' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let doc_id = String::from_utf8_lossy(&args[2]).to_string();
            let score: f64 = String::from_utf8_lossy(&args[3]).parse().unwrap_or(1.0);
            let mut fields = Vec::new();
            let mut i = 4;
            while i < args.len() {
                if String::from_utf8_lossy(&args[i]).to_uppercase() == "FIELDS" {
                    i += 1;
                    break;
                }
                i += 1;
            }
            while i + 1 < args.len() {
                let f = String::from_utf8_lossy(&args[i]).to_string();
                let v = String::from_utf8_lossy(&args[i + 1]).to_string();
                fields.push((f, v));
                i += 2;
            }
            Ok(Some(Command::FtAdd {
                index,
                doc_id,
                score,
                fields,
            }))
        }
        "FT._LIST" => Ok(Some(Command::FtList)),
        "FT.ALTER" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'ft.alter' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let mut i = 2;
            while i < args.len() {
                let tok = String::from_utf8_lossy(&args[i]).to_uppercase();
                if tok == "SCHEMA" {
                    i += 1;
                    if i < args.len()
                        && String::from_utf8_lossy(&args[i]).eq_ignore_ascii_case("ADD")
                    {
                        i += 1;
                    }
                    break;
                }
                i += 1;
            }
            if i >= args.len() {
                return Err(
                    "syntax error in 'ft.alter': expected SCHEMA ADD <field> <type>".to_string(),
                );
            }
            let mut fields = std::collections::HashMap::new();
            let mut schema_fields = Vec::new();
            while i < args.len() {
                let identifier = String::from_utf8_lossy(&args[i]).to_string();
                i += 1;
                if i >= args.len() {
                    break;
                }
                let mut alias = identifier.clone();
                if i < args.len() && String::from_utf8_lossy(&args[i]).to_uppercase() == "AS" {
                    if i + 1 < args.len() {
                        alias = String::from_utf8_lossy(&args[i + 1]).to_string();
                        i += 2;
                    }
                } else if alias.starts_with("$.") {
                    alias = alias
                        .trim_start_matches("$.")
                        .trim_end_matches(".*")
                        .trim_end_matches("[*]")
                        .to_string();
                }
                if i >= args.len() {
                    break;
                }
                let ftype_str = String::from_utf8_lossy(&args[i]).to_uppercase();
                i += 1;
                let field_type = match ftype_str.as_str() {
                    "TEXT" => {
                        let mut weight = 1.0;
                        let mut sortable = false;
                        let mut nostem = false;
                        while i < args.len() {
                            let sub_opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                            if sub_opt == "WEIGHT" && i + 1 < args.len() {
                                weight =
                                    String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(1.0);
                                i += 2;
                            } else if sub_opt == "SORTABLE" {
                                sortable = true;
                                i += 1;
                            } else if sub_opt == "NOSTEM" {
                                nostem = true;
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        Some(crate::search::FieldType::Text {
                            weight,
                            sortable,
                            nostem,
                        })
                    }
                    "NUMERIC" => {
                        let mut sortable = false;
                        if i < args.len()
                            && String::from_utf8_lossy(&args[i]).to_uppercase() == "SORTABLE"
                        {
                            sortable = true;
                            i += 1;
                        }
                        Some(crate::search::FieldType::Numeric { sortable })
                    }
                    "TAG" => {
                        let mut separator = ',';
                        let mut casesensitive = false;
                        while i < args.len() {
                            let sub_opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                            if sub_opt == "SEPARATOR" && i + 1 < args.len() {
                                separator = String::from_utf8_lossy(&args[i + 1])
                                    .chars()
                                    .next()
                                    .unwrap_or(',');
                                i += 2;
                            } else if sub_opt == "CASESENSITIVE" {
                                casesensitive = true;
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        Some(crate::search::FieldType::Tag {
                            separator,
                            casesensitive,
                        })
                    }
                    "VECTOR" => {
                        if i >= args.len() {
                            return Err(
                                "Bad arguments for vector field: missing algorithm".to_string()
                            );
                        }
                        let algorithm = String::from_utf8_lossy(&args[i]).to_uppercase();
                        i += 1;
                        let nargs = if i < args.len() {
                            String::from_utf8_lossy(&args[i]).parse::<usize>().ok()
                        } else {
                            None
                        };
                        let end = match nargs {
                            Some(n) => {
                                i += 1;
                                (i + n).min(args.len())
                            }
                            None => args.len(),
                        };
                        let mut dim = 1usize;
                        let mut distance_metric = "COSINE".to_string();
                        let mut attrs = crate::search::VectorFieldAttrs::default();
                        while i + 1 < args.len() && (nargs.is_none() || i < end) {
                            let attr = String::from_utf8_lossy(&args[i]).to_uppercase();
                            let val = String::from_utf8_lossy(&args[i + 1]).to_string();
                            match attr.as_str() {
                                "TYPE" => {
                                    if let Some(dt) = crate::search::VectorDataType::parse(&val) {
                                        attrs.data_type = dt;
                                    }
                                }
                                "DIM" => dim = val.parse().unwrap_or(1),
                                "DISTANCE_METRIC" => distance_metric = val.to_uppercase(),
                                "INITIAL_CAP" => attrs.initial_cap = val.parse().unwrap_or(1024),
                                "M" => attrs.m = val.parse().unwrap_or(16),
                                "EF_CONSTRUCTION" => {
                                    attrs.ef_construction = val.parse().unwrap_or(200)
                                }
                                "EF_RUNTIME" => attrs.ef_runtime = val.parse().unwrap_or(10),
                                "EPSILON" => attrs.epsilon = val.parse().unwrap_or(0.01),
                                _ if nargs.is_none() => break,
                                _ => {}
                            }
                            i += 2;
                        }
                        if nargs.is_some() {
                            i = end;
                        }
                        for (attr, val, max) in [
                            ("DIM", dim, crate::vector::MAX_VECTOR_DIM),
                            ("M", attrs.m, crate::vector::MAX_HNSW_M),
                        ] {
                            if val > max {
                                return Err(format!(
                                    "Bad arguments for vector similarity {} argument {}",
                                    algorithm, attr
                                ));
                            }
                        }
                        Some(crate::search::FieldType::Vector {
                            dim,
                            distance_metric,
                            algorithm,
                            attrs,
                        })
                    }
                    _ => None,
                };
                if let Some(ftype) = field_type {
                    fields.insert(alias.clone(), ftype.clone());
                    if alias != identifier {
                        fields.insert(identifier.clone(), ftype.clone());
                    }
                    schema_fields.push(crate::search::SchemaField {
                        identifier,
                        alias,
                        field_type: ftype,
                    });
                }
            }
            if schema_fields.is_empty() {
                return Err(
                    "syntax error in 'ft.alter': no valid schema fields provided".to_string(),
                );
            }
            Ok(Some(Command::FtAlter {
                index,
                fields,
                schema_fields,
            }))
        }
        "FT.PROFILE" => {
            // FT.PROFILE <index> SEARCH [LIMITED] QUERY <query> [options ...]
            if args.len() < 5 {
                return Err("wrong number of arguments for 'ft.profile' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let mut i = 2;
            let mut query_opt = None;
            while i < args.len() {
                let tok = String::from_utf8_lossy(&args[i]).to_uppercase();
                if tok == "QUERY" && i + 1 < args.len() {
                    query_opt = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                    i += 2;
                    break;
                }
                i += 1;
            }
            let query = query_opt
                .ok_or_else(|| "syntax error in 'ft.profile': missing QUERY".to_string())?;
            let mut options = crate::search::SearchOptions::default();
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "NOCONTENT" {
                    options.nocontent = true;
                    i += 1;
                } else if opt == "WITHSCORES" {
                    options.withscores = true;
                    i += 1;
                } else if opt == "LIMIT" && i + 2 < args.len() {
                    options.offset = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    options.limit = String::from_utf8_lossy(&args[i + 2]).parse().unwrap_or(10);
                    i += 3;
                } else if opt == "PARAMS" && i + 1 < args.len() {
                    let count: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    i += parse_ft_params(&args, i, count, &mut options.params)?;
                } else if (opt == "DIALECT" || opt == "TIMEOUT") && i + 1 < args.len() {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            Ok(Some(Command::FtProfile {
                index,
                query,
                options,
            }))
        }
        "FT.HYBRID" => {
            // FT.HYBRID <index> <text_query> <vector_query> [SCORER RRF [K <k>] | LINEAR [ALPHA <a>] [BETA <b>]] [LIMIT ...] [PARAMS ...] [WITHSCORES] [NOCONTENT]
            if args.len() < 4 {
                return Err("wrong number of arguments for 'ft.hybrid' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let text_query = String::from_utf8_lossy(&args[2]).to_string();
            let vec_query = String::from_utf8_lossy(&args[3]).to_string();
            let trimmed_vec = vec_query.trim();
            let query = if let Some(knn_tail) = trimmed_vec
                .strip_prefix("*=>")
                .or_else(|| trimmed_vec.strip_prefix("=>"))
            {
                format!("({})=>{}", text_query, knn_tail)
            } else if trimmed_vec.starts_with("[KNN") {
                format!("({})=>{}", text_query, trimmed_vec)
            } else {
                format!("{} {}", text_query, trimmed_vec)
            };
            let mut options = crate::search::SearchOptions {
                rrf_k: Some(60.0),
                ..Default::default()
            };
            let mut i = 4;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                if opt == "NOCONTENT" {
                    options.nocontent = true;
                    i += 1;
                } else if opt == "WITHSCORES" {
                    options.withscores = true;
                    i += 1;
                } else if opt == "RRF" {
                    let mut k_val = 60.0;
                    if i + 1 < args.len()
                        && let Ok(parsed_k) = String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                        && parsed_k > 0.0
                    {
                        k_val = parsed_k;
                        i += 2;
                    } else {
                        i += 1;
                    }
                    options.rrf_k = Some(k_val);
                    options.linear_weights = None;
                } else if opt == "LINEAR" {
                    let mut alpha = 0.5;
                    let mut beta = 0.5;
                    if i + 2 < args.len()
                        && let Ok(a) = String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                        && let Ok(b) = String::from_utf8_lossy(&args[i + 2]).parse::<f64>()
                    {
                        alpha = a;
                        beta = b;
                        i += 3;
                    } else {
                        i += 1;
                    }
                    options.linear_weights = Some((alpha, beta));
                    options.rrf_k = None;
                } else if opt == "SCORER" && i + 1 < args.len() {
                    let scorer = String::from_utf8_lossy(&args[i + 1]).to_uppercase();
                    i += 2;
                    if scorer == "RRF" {
                        let mut k_val = 60.0;
                        if i + 1 < args.len()
                            && String::from_utf8_lossy(&args[i]).eq_ignore_ascii_case("K")
                            && let Ok(parsed_k) =
                                String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                            && parsed_k > 0.0
                        {
                            k_val = parsed_k;
                            i += 2;
                        } else if i < args.len()
                            && let Ok(parsed_k) = String::from_utf8_lossy(&args[i]).parse::<f64>()
                            && parsed_k > 0.0
                        {
                            k_val = parsed_k;
                            i += 1;
                        }
                        options.rrf_k = Some(k_val);
                        options.linear_weights = None;
                    } else if scorer == "LINEAR" {
                        let mut alpha = 0.5;
                        let mut beta = 0.5;
                        while i + 1 < args.len() {
                            let sub = String::from_utf8_lossy(&args[i]).to_uppercase();
                            if sub == "ALPHA"
                                && let Ok(a) = String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                            {
                                alpha = a;
                                i += 2;
                            } else if sub == "BETA"
                                && let Ok(b) = String::from_utf8_lossy(&args[i + 1]).parse::<f64>()
                            {
                                beta = b;
                                i += 2;
                            } else {
                                break;
                            }
                        }
                        options.linear_weights = Some((alpha, beta));
                        options.rrf_k = None;
                    }
                } else if opt == "LIMIT" && i + 2 < args.len() {
                    options.offset = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    options.limit = String::from_utf8_lossy(&args[i + 2]).parse().unwrap_or(10);
                    i += 3;
                } else if opt == "RETURN" && i + 1 < args.len() {
                    let count: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    let r_fields = counted_args(&args, i, count, "RETURN")?
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).to_string())
                        .collect();
                    i += count;
                    options.return_fields = Some(r_fields);
                } else if opt == "PARAMS" && i + 1 < args.len() {
                    let count: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    i += parse_ft_params(&args, i, count, &mut options.params)?;
                } else if (opt == "DIALECT" || opt == "TIMEOUT") && i + 1 < args.len() {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            Ok(Some(Command::FtSearch {
                index,
                query,
                options,
            }))
        }
        "SEMANTIC.SET" => {
            if args.len() < 7 {
                return Err("wrong number of arguments for 'semantic.set' command".to_string());
            }
            let namespace = args[1].clone();
            let id = args[2].clone();
            let prompt = args[3].clone();
            let response = args[4].clone();
            let mut vector = Vec::new();
            let mut ttl = None;
            let mut scope = None;
            let mut quantize = false;
            let mut tokens = None;
            let mut i = 5;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "VECTOR" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        if let Some(dim) = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            if dim == 0 || dim > args.len() - (i + 2) {
                                return Err("invalid VECTOR dimension in 'semantic.set' command"
                                    .to_string());
                            }
                            vector.reserve(dim);
                            for k in 0..dim {
                                let v_str = std::str::from_utf8(&args[i + 2 + k])
                                    .map_err(|_| "not a valid float".to_string())?;
                                let v: f32 =
                                    v_str.parse().map_err(|_| "not a valid float".to_string())?;
                                vector.push(v);
                            }
                            i += 2 + dim;
                        } else if let Some(decoded) = crate::search::decode_vector(
                            &args[i + 1],
                            crate::search::VectorDataType::Float32,
                            0,
                        ) {
                            vector = decoded;
                            i += 2;
                        } else {
                            return Err("not a valid float".to_string());
                        }
                    }
                    "EX" if i + 1 < args.len() => {
                        let secs: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if secs == 0 {
                            return Err("invalid expire time in 'semantic.set' command".to_string());
                        }
                        ttl = Some(Duration::from_secs(secs));
                        i += 2;
                    }
                    "PX" if i + 1 < args.len() => {
                        let ms: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        if ms == 0 {
                            return Err("invalid expire time in 'semantic.set' command".to_string());
                        }
                        ttl = Some(Duration::from_millis(ms));
                        i += 2;
                    }
                    "SCOPE" if i + 1 < args.len() => {
                        scope = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "QUANTIZE" | "SQ8" => {
                        quantize = true;
                        i += 1;
                    }
                    "TOKENS" if i + 1 < args.len() => {
                        let n: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        tokens = Some(n);
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            if vector.is_empty() {
                return Err("missing or empty VECTOR in 'semantic.set' command".to_string());
            }
            Ok(Some(Command::SemanticSet {
                namespace,
                id,
                prompt,
                response,
                vector,
                ttl,
                scope,
                quantize,
                tokens,
            }))
        }
        "SEMANTIC.GET" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'semantic.get' command".to_string());
            }
            let namespace = args[1].clone();
            let mut query = Vec::new();
            let mut threshold = 0.90f32;
            let mut scope = None;
            let mut with_score = false;
            let mut with_prompt = false;
            let mut with_id = false;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "VECTOR" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        if let Some(dim) = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            if dim == 0 || dim > args.len() - (i + 2) {
                                return Err("invalid VECTOR dimension in 'semantic.get' command"
                                    .to_string());
                            }
                            query.reserve(dim);
                            for k in 0..dim {
                                let v_str = std::str::from_utf8(&args[i + 2 + k])
                                    .map_err(|_| "not a valid float".to_string())?;
                                let v: f32 =
                                    v_str.parse().map_err(|_| "not a valid float".to_string())?;
                                query.push(v);
                            }
                            i += 2 + dim;
                        } else if let Some(decoded) = crate::search::decode_vector(
                            &args[i + 1],
                            crate::search::VectorDataType::Float32,
                            0,
                        ) {
                            query = decoded;
                            i += 2;
                        } else {
                            return Err("not a valid float".to_string());
                        }
                    }
                    "THRESHOLD" if i + 1 < args.len() => {
                        let t: f32 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "not a valid float".to_string())?
                            .parse()
                            .map_err(|_| "not a valid float".to_string())?;
                        if !(0.0..=1.0).contains(&t) {
                            return Err(
                                "THRESHOLD must be between 0.0 and 1.0 in 'semantic.get' command"
                                    .to_string(),
                            );
                        }
                        threshold = t;
                        i += 2;
                    }
                    "SCOPE" if i + 1 < args.len() => {
                        scope = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "WITHSCORE" | "WITHSCORES" => {
                        with_score = true;
                        i += 1;
                    }
                    "WITHPROMPT" => {
                        with_prompt = true;
                        i += 1;
                    }
                    "WITHID" => {
                        with_id = true;
                        i += 1;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            if query.is_empty() {
                return Err("missing or empty VECTOR in 'semantic.get' command".to_string());
            }
            Ok(Some(Command::SemanticGet {
                namespace,
                query,
                threshold,
                scope,
                with_score,
                with_prompt,
                with_id,
            }))
        }
        "SEMANTIC.DEL" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'semantic.del' command".to_string());
            }
            let namespace = args[1].clone();
            let ids = args[2..].to_vec();
            Ok(Some(Command::SemanticDel { namespace, ids }))
        }
        "SEMANTIC.FLUSH" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'semantic.flush' command".to_string());
            }
            Ok(Some(Command::SemanticFlush(args[1].clone())))
        }
        "SEMANTIC.INFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'semantic.info' command".to_string());
            }
            Ok(Some(Command::SemanticInfo(args[1].clone())))
        }
        "AGENT.MEM.ADD" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'agent.mem.add' command".to_string());
            }
            let session = args[1].clone();
            let role = args[2].clone();
            let content = args[3].clone();
            let mut tokens = None;
            let mut vector = None;
            let mut meta = None;
            let mut i = 4;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "TOKENS" if i + 1 < args.len() => {
                        let n: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        tokens = Some(n);
                        i += 2;
                    }
                    "VEC" | "VECTOR" if i + 1 < args.len() => {
                        if let Some(dim) = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                            && dim > 0
                            && dim <= args.len() - (i + 2)
                        {
                            let mut v = Vec::with_capacity(dim);
                            for k in 0..dim {
                                let s = std::str::from_utf8(&args[i + 2 + k])
                                    .map_err(|_| "not a valid float".to_string())?;
                                let f: f32 =
                                    s.parse().map_err(|_| "not a valid float".to_string())?;
                                v.push(f);
                            }
                            vector = Some(v);
                            i += 2 + dim;
                        } else if let Some(decoded) = crate::search::decode_vector(
                            &args[i + 1],
                            crate::search::VectorDataType::Float32,
                            0,
                        ) {
                            vector = Some(decoded);
                            i += 2;
                        } else {
                            return Err("not a valid vector in 'agent.mem.add' command".to_string());
                        }
                    }
                    "META" if i + 1 < args.len() => {
                        meta = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::AgentMemAdd {
                session,
                role,
                content,
                tokens,
                vector,
                meta,
            }))
        }
        "AGENT.MEM.CONTEXT" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'agent.mem.context' command".to_string());
            }
            let session = args[1].clone();
            let mut max_tokens = 2048u64;
            let mut query = None;
            let mut recall_k = 0usize;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "MAX_TOKENS" | "MAXTOKENS" if i + 1 < args.len() => {
                        max_tokens = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        i += 2;
                    }
                    "QUERY" | "VEC" | "VECTOR" if i + 1 < args.len() => {
                        if let Some(dim) = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                            && dim > 0
                            && dim <= args.len() - (i + 2)
                        {
                            let mut v = Vec::with_capacity(dim);
                            for k in 0..dim {
                                let s = std::str::from_utf8(&args[i + 2 + k])
                                    .map_err(|_| "not a valid float".to_string())?;
                                let f: f32 =
                                    s.parse().map_err(|_| "not a valid float".to_string())?;
                                v.push(f);
                            }
                            query = Some(v);
                            i += 2 + dim;
                        } else if let Some(decoded) = crate::search::decode_vector(
                            &args[i + 1],
                            crate::search::VectorDataType::Float32,
                            0,
                        ) {
                            query = Some(decoded);
                            i += 2;
                        } else {
                            return Err(
                                "not a valid vector in 'agent.mem.context' command".to_string()
                            );
                        }
                        if recall_k == 0 {
                            recall_k = 3;
                        }
                    }
                    "RECALL" | "RECALL_K" if i + 1 < args.len() => {
                        recall_k = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::AgentMemContext {
                session,
                max_tokens,
                query,
                recall_k,
            }))
        }
        "AGENT.MEM.COMPACT" => {
            if args.len() < 6 {
                return Err("wrong number of arguments for 'agent.mem.compact' command".to_string());
            }
            let session = args[1].clone();
            let mut keep_recent = None;
            let mut summary = None;
            let mut tokens = None;
            let mut vector = None;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "KEEP_RECENT" | "KEEP" if i + 1 < args.len() => {
                        let k: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        keep_recent = Some(k);
                        i += 2;
                    }
                    "SUMMARY" if i + 1 < args.len() => {
                        summary = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "TOKENS" if i + 1 < args.len() => {
                        let n: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        tokens = Some(n);
                        i += 2;
                    }
                    "VEC" | "VECTOR" if i + 1 < args.len() => {
                        if let Some(dim) = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                            && dim > 0
                            && dim <= args.len() - (i + 2)
                        {
                            let mut v = Vec::with_capacity(dim);
                            for k in 0..dim {
                                let s = std::str::from_utf8(&args[i + 2 + k])
                                    .map_err(|_| "not a valid float".to_string())?;
                                let f: f32 =
                                    s.parse().map_err(|_| "not a valid float".to_string())?;
                                v.push(f);
                            }
                            vector = Some(v);
                            i += 2 + dim;
                        } else if let Some(decoded) = crate::search::decode_vector(
                            &args[i + 1],
                            crate::search::VectorDataType::Float32,
                            0,
                        ) {
                            vector = Some(decoded);
                            i += 2;
                        } else {
                            return Err(
                                "not a valid vector in 'agent.mem.compact' command".to_string()
                            );
                        }
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            let Some(keep_recent) = keep_recent else {
                return Err("missing KEEP_RECENT in 'agent.mem.compact' command".to_string());
            };
            let Some(summary) = summary else {
                return Err("missing SUMMARY in 'agent.mem.compact' command".to_string());
            };
            Ok(Some(Command::AgentMemCompact {
                session,
                keep_recent,
                summary,
                tokens,
                vector,
            }))
        }
        "AGENT.MEM.INFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'agent.mem.info' command".to_string());
            }
            Ok(Some(Command::AgentMemInfo(args[1].clone())))
        }
        "AGENT.MEM.CLEAR" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'agent.mem.clear' command".to_string());
            }
            Ok(Some(Command::AgentMemClear(args[1].clone())))
        }
        "LLM.QUOTA.RESERVE" => {
            if args.len() < 8 {
                return Err("wrong number of arguments for 'llm.quota.reserve' command".to_string());
            }
            let key = args[1].clone();
            let mut rpm = None;
            let mut tpm = None;
            let mut est_tokens = None;
            let mut window_ms = None;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "RPM" if i + 1 < args.len() => {
                        let v: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        rpm = Some(v);
                        i += 2;
                    }
                    "TPM" if i + 1 < args.len() => {
                        let v: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        tpm = Some(v);
                        i += 2;
                    }
                    "EST_TOKENS" | "TOKENS" if i + 1 < args.len() => {
                        let v: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        est_tokens = Some(v);
                        i += 2;
                    }
                    "WINDOW" | "WINDOW_MS" if i + 1 < args.len() => {
                        let v: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        window_ms = Some(v);
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            let Some(rpm) = rpm else {
                return Err("missing RPM in 'llm.quota.reserve' command".to_string());
            };
            let Some(tpm) = tpm else {
                return Err("missing TPM in 'llm.quota.reserve' command".to_string());
            };
            let Some(est_tokens) = est_tokens else {
                return Err("missing EST_TOKENS in 'llm.quota.reserve' command".to_string());
            };
            Ok(Some(Command::LlmQuotaReserve {
                key,
                rpm,
                tpm,
                est_tokens,
                window_ms,
            }))
        }
        "LLM.QUOTA.SETTLE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'llm.quota.settle' command".to_string());
            }
            let key = args[1].clone();
            let reservation_id: u64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range".to_string())?
                .parse()
                .map_err(|_| "value is not an integer or out of range".to_string())?;
            let actual_tokens: u64 = if args.len() == 4 {
                std::str::from_utf8(&args[3])
                    .map_err(|_| "value is not an integer or out of range".to_string())?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range".to_string())?
            } else if args.len() == 5
                && matches!(
                    String::from_utf8_lossy(&args[3]).to_uppercase().as_str(),
                    "ACTUAL_TOKENS" | "TOKENS"
                )
            {
                std::str::from_utf8(&args[4])
                    .map_err(|_| "value is not an integer or out of range".to_string())?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range".to_string())?
            } else {
                return Err("syntax error".to_string());
            };
            Ok(Some(Command::LlmQuotaSettle {
                key,
                reservation_id,
                actual_tokens,
            }))
        }
        "LLM.QUOTA.INFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'llm.quota.info' command".to_string());
            }
            Ok(Some(Command::LlmQuotaInfo(args[1].clone())))
        }
        "AGENT.CHECKPOINT.PUT" => {
            if args.len() < 4 {
                return Err(
                    "wrong number of arguments for 'agent.checkpoint.put' command".to_string(),
                );
            }
            let key = args[1].clone();
            let step_id = args[2].clone();
            let mut parent_id: Option<Bytes> = None;
            let mut state: Option<Bytes> = None;
            let mut meta: Option<Bytes> = None;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "PARENT" if i + 1 < args.len() => {
                        parent_id = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "STATE" if i + 1 < args.len() => {
                        state = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "META" if i + 1 < args.len() => {
                        meta = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ if state.is_none() && i == 3 => {
                        state = Some(args[i].clone());
                        i += 1;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            let Some(state) = state else {
                return Err("missing STATE in 'agent.checkpoint.put' command".to_string());
            };
            Ok(Some(Command::AgentCheckpointPut {
                key,
                step_id,
                parent_id,
                state,
                meta,
            }))
        }
        "AGENT.CHECKPOINT.GET" => {
            if args.len() < 2 {
                return Err(
                    "wrong number of arguments for 'agent.checkpoint.get' command".to_string(),
                );
            }
            let key = args[1].clone();
            let step_id = if args.len() == 2 {
                None
            } else if args.len() == 3 {
                Some(args[2].clone())
            } else if args.len() == 4
                && String::from_utf8_lossy(&args[2]).eq_ignore_ascii_case("STEP")
            {
                Some(args[3].clone())
            } else {
                return Err("syntax error".to_string());
            };
            Ok(Some(Command::AgentCheckpointGet { key, step_id }))
        }
        "AGENT.CHECKPOINT.HISTORY" => {
            if args.len() < 2 {
                return Err(
                    "wrong number of arguments for 'agent.checkpoint.history' command".to_string(),
                );
            }
            let key = args[1].clone();
            let mut from_step: Option<Bytes> = None;
            let mut limit: usize = 50;
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "FROM" | "STEP" if i + 1 < args.len() => {
                        from_step = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "LIMIT" if i + 1 < args.len() => {
                        let v: usize = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        limit = v;
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::AgentCheckpointHistory {
                key,
                from_step,
                limit,
            }))
        }
        "AGENT.TOOL.CLAIM" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'agent.tool.claim' command".to_string());
            }
            let key = args[1].clone();
            let call_id = args[2].clone();
            let mut ttl_ms: u64 = 30_000;
            let mut input: Option<Bytes> = None;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "TTL" | "PX" if i + 1 < args.len() => {
                        let v: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        ttl_ms = v;
                        i += 2;
                    }
                    "INPUT" if i + 1 < args.len() => {
                        input = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::AgentToolClaim {
                key,
                call_id,
                ttl_ms,
                input,
            }))
        }
        "AGENT.TOOL.COMPLETE" => {
            if args.len() < 4 {
                return Err(
                    "wrong number of arguments for 'agent.tool.complete' command".to_string(),
                );
            }
            let key = args[1].clone();
            let call_id = args[2].clone();
            let mut output: Option<Bytes> = None;
            let mut ttl_ms: Option<u64> = None;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "OUTPUT" if i + 1 < args.len() => {
                        output = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "TTL" | "PX" if i + 1 < args.len() => {
                        let v: u64 = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range".to_string())?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range".to_string())?;
                        ttl_ms = Some(v);
                        i += 2;
                    }
                    _ if output.is_none() && i == 3 => {
                        output = Some(args[i].clone());
                        i += 1;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            let Some(output) = output else {
                return Err("missing OUTPUT in 'agent.tool.complete' command".to_string());
            };
            Ok(Some(Command::AgentToolComplete {
                key,
                call_id,
                output,
                ttl_ms,
            }))
        }
        "MCP.TOOLS" => Ok(Some(Command::McpTools)),
        "MCP.CALL" => {
            if args.len() < 2 || args.len() > 3 {
                return Err("wrong number of arguments for 'mcp.call' command".to_string());
            }
            let tool = String::from_utf8_lossy(&args[1]).to_string();
            let args_json = if args.len() == 3 {
                args[2].clone()
            } else {
                Bytes::from_static(b"{}")
            };
            Ok(Some(Command::McpCall { tool, args_json }))
        }
        "MCP.RPC" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'mcp.rpc' command".to_string());
            }
            Ok(Some(Command::McpRpc(args[1].clone())))
        }
        "XDP.INFO" => Ok(Some(Command::XdpInfo)),
        "XDP.RULE" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'xdp.rule' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "ADD" => {
                    if args.len() < 4 {
                        return Err(
                            "wrong number of arguments for 'xdp.rule add' command".to_string()
                        );
                    }
                    let action_str = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let action = match action_str.as_str() {
                        "DROP" => crate::xdp::XdpAction::Drop,
                        "PASS" => crate::xdp::XdpAction::Pass,
                        "REDIRECT" => crate::xdp::XdpAction::Redirect,
                        "TX" => crate::xdp::XdpAction::Tx,
                        _ => return Err(format!("Unknown XDP action: {}", action_str)),
                    };
                    let cidr = String::from_utf8_lossy(&args[3]).to_string();
                    Ok(Some(Command::XdpRuleAdd { action, cidr }))
                }
                "DEL" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'xdp.rule del' command".to_string()
                        );
                    }
                    let id: u32 = String::from_utf8_lossy(&args[2])
                        .parse()
                        .map_err(|_| "Invalid rule ID".to_string())?;
                    Ok(Some(Command::XdpRuleDel(id)))
                }
                "LIST" => Ok(Some(Command::XdpRuleList)),
                _ => Err(format!("Unknown xdp.rule subcommand: {}", sub)),
            }
        }
        "XDP.STATS" => Ok(Some(Command::XdpStats)),
        "XDP.PACKET" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'xdp.packet' command".to_string());
            }
            Ok(Some(Command::XdpPacket(args[1].clone())))
        }
        "XDP.SOCKET" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'xdp.socket' command".to_string());
            }
            let qid: u32 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
            Ok(Some(Command::XdpSocket(qid)))
        }
        "XDP.INJECT" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'xdp.inject' command".to_string());
            }
            let qid: u32 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
            Ok(Some(Command::XdpInject {
                queue_id: qid,
                payload: args[2].clone(),
            }))
        }
        _ => Ok(Some(Command::Unknown(cmd_name.to_string()))),
    }
}

pub fn parse_redis_f64(s: &str) -> Option<f64> {
    if s.is_empty() || s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return None;
    }
    if s.eq_ignore_ascii_case("nan")
        || s.eq_ignore_ascii_case("+nan")
        || s.eq_ignore_ascii_case("-nan")
    {
        return None;
    }
    let is_literal_inf = s.eq_ignore_ascii_case("inf")
        || s.eq_ignore_ascii_case("+inf")
        || s.eq_ignore_ascii_case("-inf")
        || s.eq_ignore_ascii_case("infinity")
        || s.eq_ignore_ascii_case("+infinity")
        || s.eq_ignore_ascii_case("-infinity");

    if let Ok(v) = s.parse::<f64>() {
        if !v.is_nan() {
            if v.is_infinite() && !is_literal_inf {
                return None;
            }
            return Some(v);
        }
        return None;
    }
    if let Ok(c_str) = std::ffi::CString::new(s) {
        unsafe {
            let mut end: *mut libc::c_char = std::ptr::null_mut();
            let val = libc::strtod(c_str.as_ptr(), &mut end);
            if !end.is_null() && *end == 0 && !std::ptr::eq(end, c_str.as_ptr()) && !val.is_nan() {
                if val.is_infinite() && !is_literal_inf {
                    return None;
                }
                return Some(val);
            }
        }
    }
    None
}

pub fn parse_score_bound(arg: &[u8]) -> Result<(f64, bool), String> {
    if arg.is_empty() {
        return Err("min or max not specified".to_string());
    }
    let (slice, inc) = if arg[0] == b'(' {
        (&arg[1..], false)
    } else {
        (arg, true)
    };
    let s = std::str::from_utf8(slice).map_err(|_| "value is not a valid float".to_string())?;
    if s.eq_ignore_ascii_case("-inf") {
        Ok((f64::NEG_INFINITY, inc))
    } else if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
        Ok((f64::INFINITY, inc))
    } else {
        let val = parse_redis_f64(s).ok_or_else(|| "value is not a valid float".to_string())?;
        Ok((val, inc))
    }
}

fn find_newline(buf: &[u8]) -> Option<(usize, usize)> {
    find_newline_at(buf, 0)
}

fn find_newline_at(buf: &[u8], start: usize) -> Option<(usize, usize)> {
    if buf.len() <= start {
        return None;
    }
    if let Some(pos) = buf[start..].iter().position(|&b| b == b'\n') {
        let abs_pos = start + pos;
        let line_end = if abs_pos > start && buf[abs_pos - 1] == b'\r' {
            abs_pos - 1
        } else {
            abs_pos
        };
        let advance_len = abs_pos + 1 - start;
        Some((line_end, advance_len))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrate_is_allowed_over_maxmemory() {
        let mut buf = BytesMut::from("MIGRATE 127.0.0.1 6380 k 0 5000\r\n");
        let migrate = parse_command(&mut buf).unwrap().unwrap();
        assert!(migrate.allows_oom());
        let mut buf = BytesMut::from("SET k v\r\n");
        assert!(!parse_command(&mut buf).unwrap().unwrap().allows_oom());
    }

    #[test]
    fn test_is_write_command_follows_command_table() {
        let parse = |line: &str| {
            let mut buf = BytesMut::from(format!("{line}\r\n").as_str());
            parse_command(&mut buf).unwrap().unwrap()
        };
        for line in [
            "SET k v",
            "SETBIT k 0 1",
            "BITFIELD k SET i5 0 1",
            "SETRANGE k 0 x",
            "BITOP AND d a b",
            "RENAME a b",
            "COPY a b",
            "PFADD h a",
            "GEOADD g 1 2 m",
            "LINSERT l BEFORE a b",
            "RPOPLPUSH a b",
            "SINTERSTORE d a b",
            "HINCRBY h f 1",
            "SORT l STORE d",
            "GEORADIUS g 0 0 1 km STORE d",
            "JSON.SET j $ 1",
            "BF.ADD b x",
        ] {
            assert!(parse(line).is_write_command(), "{line}");
        }
        for line in [
            "GET k",
            "GETBIT k 0",
            "PFCOUNT h",
            "SMEMBERS s",
            "PING",
            "INFO",
            "SORT_RO l",
            "GEORADIUS_RO g 0 0 1 km",
            "BITFIELD_RO k GET i5 0",
            "JSON.GET j",
            "BF.EXISTS b x",
            "SEMANTIC.GET ns VECTOR 2 0.1 0.2",
            "FT._LIST",
        ] {
            assert!(!parse(line).is_write_command(), "{line}");
        }
    }

    #[test]
    fn test_resp_get() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Get(Bytes::from_static(b"mykey")));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_hgetex_zero_expire_accepted_negative_rejected() {
        let mut buf = BytesMut::from("HGETEX h PX 0 FIELDS 1 f\r\n");
        match parse_command(&mut buf).unwrap().unwrap() {
            Command::Hgetex { expire, .. } => assert_eq!(expire, HFieldExpireOpt::ExMs(0)),
            other => panic!("unexpected {:?}", other),
        }
        let mut buf = BytesMut::from("HGETEX h EX 0 FIELDS 1 f\r\n");
        assert!(parse_command(&mut buf).unwrap().is_some());
        for opt in ["EX", "PX", "EXAT", "PXAT"] {
            let mut buf = BytesMut::from(format!("HGETEX h {} -1 FIELDS 1 f\r\n", opt).as_str());
            assert_eq!(
                parse_command(&mut buf).unwrap_err(),
                "ERR invalid expire time in 'hgetex' command"
            );
        }
    }

    #[test]
    fn test_resp_set_and_put() {
        let mut buf = BytesMut::from("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: Bytes::from_static(b"mykey"),
                value: Bytes::from_static(b"myvalue"),
                expire_in: None,
                condition: SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            }
        );
        assert!(buf.is_empty());

        let mut buf = BytesMut::from("*3\r\n$3\r\nPUT\r\n$1\r\nk\r\n$1\r\nv\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                expire_in: None,
                condition: SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            }
        );
        assert!(buf.is_empty());

        // SET with EX
        let mut buf =
            BytesMut::from("*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                expire_in: Some(Duration::from_secs(10)),
                condition: SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            }
        );
    }

    #[test]
    fn test_inline_commands() {
        let mut buf = BytesMut::from("GET foo\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Get(Bytes::from_static(b"foo")));

        let mut buf = BytesMut::from("SET foo bar\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: Bytes::from_static(b"foo"),
                value: Bytes::from_static(b"bar"),
                expire_in: None,
                condition: SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            }
        );

        let mut buf = BytesMut::from("EXPIRE foo 60\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Expire {
                key: Bytes::from_static(b"foo"),
                duration: Duration::from_secs(60),
                opts: ExpireOptions::default(),
            }
        );

        let mut buf = BytesMut::from("EXPIRE foo 60 NX\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Expire {
                key: Bytes::from_static(b"foo"),
                duration: Duration::from_secs(60),
                opts: ExpireOptions {
                    nx: true,
                    ..Default::default()
                },
            }
        );

        let mut buf = BytesMut::from("EXPIRE foo 60 LT GT\r\n");
        assert!(parse_command(&mut buf).is_err());

        let mut buf = BytesMut::from("TTL foo\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Ttl(Bytes::from_static(b"foo"), false));
    }

    #[test]
    fn test_resp_hash_commands() {
        let mut buf = BytesMut::from("HSET myhash f1 v1 f2 v2\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Hset {
                key: Bytes::from_static(b"myhash"),
                fields: smallvec![
                    (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                ],
            }
        );

        let mut buf = BytesMut::from("HGET myhash f1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Hget {
                key: Bytes::from_static(b"myhash"),
                field: Bytes::from_static(b"f1"),
            }
        );

        let mut buf = BytesMut::from("HMGET myhash f1 f2\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Hmget {
                key: Bytes::from_static(b"myhash"),
                fields: vec![Bytes::from_static(b"f1"), Bytes::from_static(b"f2")],
            }
        );

        let mut buf = BytesMut::from("HDEL myhash f1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Hdel {
                key: Bytes::from_static(b"myhash"),
                fields: vec![Bytes::from_static(b"f1")],
            }
        );

        let mut buf = BytesMut::from("HEXISTS myhash f1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Hexists {
                key: Bytes::from_static(b"myhash"),
                field: Bytes::from_static(b"f1"),
            }
        );

        let mut buf = BytesMut::from("HLEN myhash\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Hlen(Bytes::from_static(b"myhash")));

        let mut buf = BytesMut::from("HGETALL myhash\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Hgetall(Bytes::from_static(b"myhash")));
    }

    #[test]
    fn test_resp_keyspace_and_transactions() {
        // KEYS
        let mut buf = BytesMut::from("KEYS user:*\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Keys(Bytes::from_static(b"user:*"))
        );

        // SCAN
        let mut buf = BytesMut::from("SCAN 123 MATCH pat:* COUNT 50\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Scan {
                cursor: 123,
                pattern: Some(Bytes::from_static(b"pat:*")),
                count: Some(50),
                key_type: None,
            }
        );

        // RANDOMKEY
        let mut buf = BytesMut::from("RANDOMKEY\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Randomkey
        );

        // EXPIRETIME & PEXPIRETIME
        let mut buf = BytesMut::from("EXPIRETIME k1\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Expiretime(Bytes::from_static(b"k1"), false)
        );
        let mut buf = BytesMut::from("PEXPIRETIME k1\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Expiretime(Bytes::from_static(b"k1"), true)
        );

        // MULTI, EXEC, DISCARD
        let mut buf = BytesMut::from("MULTI\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Multi);
        let mut buf = BytesMut::from("EXEC\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Exec);
        let mut buf = BytesMut::from("DISCARD\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Discard);
    }

    #[test]
    fn test_resp_bitmaps_and_hll() {
        // SETBIT
        let mut buf = BytesMut::from("SETBIT mykey 10 1\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Setbit {
                key: Bytes::from_static(b"mykey"),
                offset: 10,
                value: 1,
            }
        );

        // GETBIT
        let mut buf = BytesMut::from("GETBIT mykey 10\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Getbit {
                key: Bytes::from_static(b"mykey"),
                offset: 10,
            }
        );

        // BITCOUNT
        let mut buf = BytesMut::from("BITCOUNT mykey 0 5\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Bitcount {
                key: Bytes::from_static(b"mykey"),
                start: Some(0),
                end: Some(5),
                is_bit: false,
            }
        );

        // BITPOS
        let mut buf = BytesMut::from("BITPOS mykey 1 2 4\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Bitpos {
                key: Bytes::from_static(b"mykey"),
                bit: 1,
                start: Some(2),
                end: Some(4),
                is_bit: false,
            }
        );

        // BITOP
        let mut buf = BytesMut::from("BITOP AND dest k1 k2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Bitop {
                op: "AND".to_string(),
                destkey: Bytes::from_static(b"dest"),
                srckeys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
            }
        );

        // PFADD
        let mut buf = BytesMut::from("PFADD hll elem1 elem2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Pfadd {
                key: Bytes::from_static(b"hll"),
                elements: vec![Bytes::from_static(b"elem1"), Bytes::from_static(b"elem2")],
            }
        );

        // PFCOUNT
        let mut buf = BytesMut::from("PFCOUNT hll1 hll2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Pfcount {
                keys: vec![Bytes::from_static(b"hll1"), Bytes::from_static(b"hll2")],
            }
        );

        // PFMERGE
        let mut buf = BytesMut::from("PFMERGE dest hll1 hll2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Pfmerge {
                destkey: Bytes::from_static(b"dest"),
                srckeys: vec![Bytes::from_static(b"hll1"), Bytes::from_static(b"hll2")],
            }
        );
    }

    #[test]
    fn test_resp_dump_and_restore() {
        // DUMP
        let mut buf = BytesMut::from("DUMP mykey\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Dump(Bytes::from_static(b"mykey"))
        );

        // RESTORE
        let mut buf =
            BytesMut::from("*4\r\n$7\r\nRESTORE\r\n$5\r\nmykey\r\n$1\r\n0\r\n$4\r\ndata\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Restore {
                key: Bytes::from_static(b"mykey"),
                ttl_ms: 0,
                serialized: Bytes::from_static(b"data"),
                replace: false,
                absttl: false,
            }
        );

        // RESTORE with REPLACE and ABSTTL
        let mut buf = BytesMut::from(
            "*6\r\n$7\r\nRESTORE\r\n$5\r\nmykey\r\n$4\r\n1000\r\n$4\r\ndata\r\n$7\r\nREPLACE\r\n$6\r\nABSTTL\r\n",
        );
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Restore {
                key: Bytes::from_static(b"mykey"),
                ttl_ms: 1000,
                serialized: Bytes::from_static(b"data"),
                replace: true,
                absttl: true,
            }
        );
    }

    #[test]
    fn test_resp_streams() {
        // XADD with auto ID
        let mut buf = BytesMut::from("XADD s1 * field1 val1\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Xadd {
                key: Bytes::from_static(b"s1"),
                nomkstream: false,
                maxlen: None,
                minid: None,
                approx: false,
                trim_strategy: crate::table::StreamTrimStrategy::KeepRef,
                idmp: None,
                id: crate::table::StreamAddId::Auto,
                fields: vec![(Bytes::from_static(b"field1"), Bytes::from_static(b"val1"))],
                limit: None,
            }
        );

        // XADD with NOMKSTREAM and MAXLEN
        let mut buf = BytesMut::from("XADD s1 NOMKSTREAM MAXLEN ~ 1000 100-0 f1 v1\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Xadd {
                key: Bytes::from_static(b"s1"),
                nomkstream: true,
                maxlen: Some(1000),
                minid: None,
                approx: true,
                trim_strategy: crate::table::StreamTrimStrategy::KeepRef,
                idmp: None,
                id: crate::table::StreamAddId::Explicit(crate::table::StreamId::new(100, 0)),
                fields: vec![(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))],
                limit: None,
            }
        );

        // XLEN
        let mut buf = BytesMut::from("XLEN s1\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Xlen(Bytes::from_static(b"s1"))
        );

        // XRANGE
        let mut buf = BytesMut::from("XRANGE s1 - + COUNT 10\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Xrange {
                key: Bytes::from_static(b"s1"),
                start: "-".to_string(),
                end: "+".to_string(),
                count: Some(10),
            }
        );

        // XREVRANGE
        let mut buf = BytesMut::from("XREVRANGE s1 + - COUNT 5\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Xrevrange {
                key: Bytes::from_static(b"s1"),
                end: "+".to_string(),
                start: "-".to_string(),
                count: Some(5),
            }
        );

        // XREAD
        let mut buf = BytesMut::from("XREAD COUNT 2 STREAMS s1 s2 0-0 $\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Xread {
                count: Some(2),
                maxcount: None,
                maxsize: None,
                block_ms: None,
                keys: vec![Bytes::from_static(b"s1"), Bytes::from_static(b"s2")],
                ids: vec!["0-0".to_string(), "$".to_string()],
            }
        );

        // XDEL
        let mut buf = BytesMut::from("XDEL s1 100-1 100-2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Xdel {
                key: Bytes::from_static(b"s1"),
                ids: vec![
                    crate::table::StreamId::new(100, 1),
                    crate::table::StreamId::new(100, 2)
                ],
            }
        );

        // XTRIM
        let mut buf = BytesMut::from("XTRIM s1 MAXLEN = 50\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Xtrim {
                key: Bytes::from_static(b"s1"),
                maxlen: Some(50),
                minid: None,
                approx: false,
                trim_strategy: crate::table::StreamTrimStrategy::KeepRef,
                limit: None,
            }
        );
    }

    #[test]
    fn test_valkey_extended_parsers() {
        // HELLO
        let mut buf = BytesMut::from("HELLO 3 AUTH alice secret SETNAME client1\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Hello {
                proto: Some(3),
                auth: Some(("alice".to_string(), "secret".to_string())),
                setname: Some("client1".to_string()),
            }
        );

        // RESET, TIME, ECHO
        let mut buf = BytesMut::from("RESET\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Reset);
        let mut buf = BytesMut::from("TIME\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Time);
        let mut buf = BytesMut::from("ECHO hi\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Echo(Bytes::from_static(b"hi"))
        );

        // HASHES: HINCRBY, HINCRBYFLOAT, HRANDFIELD, HSCAN
        let mut buf = BytesMut::from("HINCRBY h f 5\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Hincrby {
                key: Bytes::from_static(b"h"),
                field: Bytes::from_static(b"f"),
                increment: 5
            }
        );
        let mut buf = BytesMut::from("HINCRBYFLOAT h f 2.5\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Hincrbyfloat {
                key: Bytes::from_static(b"h"),
                field: Bytes::from_static(b"f"),
                increment: 2.5
            }
        );
        let mut buf = BytesMut::from("HRANDFIELD h 3 WITHVALUES\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Hrandfield {
                key: Bytes::from_static(b"h"),
                count: Some(3),
                with_values: true
            }
        );
        let mut buf = BytesMut::from("HSCAN h 0 MATCH pat* COUNT 20\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Hscan {
                key: Bytes::from_static(b"h"),
                cursor: 0,
                pattern: Some(Bytes::from_static(b"pat*")),
                count: Some(20),
                no_values: false,
            }
        );

        // SETS: SMISMEMBER, SRANDMEMBER, SMOVE, SSCAN
        let mut buf = BytesMut::from("SMISMEMBER s m1 m2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Smismember {
                key: Bytes::from_static(b"s"),
                members: vec![Bytes::from_static(b"m1"), Bytes::from_static(b"m2")]
            }
        );
        let mut buf = BytesMut::from("SRANDMEMBER s 2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Srandmember {
                key: Bytes::from_static(b"s"),
                count: Some(2)
            }
        );
        let mut buf = BytesMut::from("SMOVE s1 s2 m\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Smove {
                source: Bytes::from_static(b"s1"),
                destination: Bytes::from_static(b"s2"),
                member: Bytes::from_static(b"m")
            }
        );
        let mut buf = BytesMut::from("SSCAN s 0\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Sscan {
                key: Bytes::from_static(b"s"),
                cursor: 0,
                pattern: None,
                count: None
            }
        );

        // ZSETS: ZMSCORE, ZRANDMEMBER, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZLEXCOUNT, ZSCAN
        let mut buf = BytesMut::from("ZMSCORE z m1 m2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Zmscore {
                key: Bytes::from_static(b"z"),
                members: vec![Bytes::from_static(b"m1"), Bytes::from_static(b"m2")]
            }
        );
        let mut buf = BytesMut::from("ZRANDMEMBER z 2 WITHSCORES\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Zrandmember {
                key: Bytes::from_static(b"z"),
                count: Some(2),
                with_scores: true
            }
        );
        let mut buf = BytesMut::from("ZREMRANGEBYRANK z 0 2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Zremrangebyrank {
                key: Bytes::from_static(b"z"),
                start: 0,
                stop: 2
            }
        );
        let mut buf = BytesMut::from("ZREMRANGEBYSCORE z (1.5 5.0\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Zremrangebyscore {
                key: Bytes::from_static(b"z"),
                min_score: 1.5,
                min_inc: false,
                max_score: 5.0,
                max_inc: true
            }
        );
        let mut buf = BytesMut::from("ZREMRANGEBYLEX z [a (c\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Zremrangebylex {
                key: Bytes::from_static(b"z"),
                min: crate::table::LexBound::Inclusive(Bytes::from_static(b"a")),
                max: crate::table::LexBound::Exclusive(Bytes::from_static(b"c"))
            }
        );
        let mut buf = BytesMut::from("ZLEXCOUNT z - +\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Zlexcount {
                key: Bytes::from_static(b"z"),
                min: crate::table::LexBound::UnboundedMin,
                max: crate::table::LexBound::UnboundedMax
            }
        );
        let mut buf = BytesMut::from("ZSCAN z 0\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Zscan {
                key: Bytes::from_static(b"z"),
                cursor: 0,
                pattern: None,
                count: None
            }
        );

        // LISTS: LTRIM, LSET, LREM, LPOS, LINSERT, LMOVE, BLMOVE
        let mut buf = BytesMut::from("LTRIM l 1 2\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Ltrim {
                key: Bytes::from_static(b"l"),
                start: 1,
                stop: 2
            }
        );
        let mut buf = BytesMut::from("LSET l 0 val\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Lset {
                key: Bytes::from_static(b"l"),
                index: 0,
                element: Bytes::from_static(b"val")
            }
        );
        let mut buf = BytesMut::from("LREM l 2 val\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Lrem {
                key: Bytes::from_static(b"l"),
                count: 2,
                element: Bytes::from_static(b"val")
            }
        );
        let mut buf = BytesMut::from("LPOS l val RANK 2 COUNT 3 MAXLEN 100\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Lpos {
                key: Bytes::from_static(b"l"),
                element: Bytes::from_static(b"val"),
                rank: Some(2),
                count: Some(3),
                maxlen: Some(100)
            }
        );
        let mut buf = BytesMut::from("LINSERT l AFTER p e\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Linsert {
                key: Bytes::from_static(b"l"),
                before: false,
                pivot: Bytes::from_static(b"p"),
                element: Bytes::from_static(b"e")
            }
        );
        let mut buf = BytesMut::from("LMOVE l1 l2 LEFT RIGHT\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Lmove {
                source: Bytes::from_static(b"l1"),
                destination: Bytes::from_static(b"l2"),
                where_from: crate::table::ListDirection::Left,
                where_to: crate::table::ListDirection::Right
            }
        );
        let mut buf = BytesMut::from("BLMOVE l1 l2 RIGHT LEFT 1.5\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Blmove {
                source: Bytes::from_static(b"l1"),
                destination: Bytes::from_static(b"l2"),
                where_from: crate::table::ListDirection::Right,
                where_to: crate::table::ListDirection::Left,
                timeout: 1.5
            }
        );

        // STRINGS: INCRBYFLOAT, SETRANGE, GETRANGE
        let mut buf = BytesMut::from("INCRBYFLOAT num 1.25\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Incrbyfloat {
                key: Bytes::from_static(b"num"),
                increment: 1.25
            }
        );
        let mut buf = BytesMut::from("SETRANGE k 2 world\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Setrange {
                key: Bytes::from_static(b"k"),
                offset: 2,
                value: Bytes::from_static(b"world")
            }
        );
        let mut buf = BytesMut::from("GETRANGE k 0 -1\r\n");
        assert_eq!(
            parse_command(&mut buf).unwrap().unwrap(),
            Command::Getrange {
                key: Bytes::from_static(b"k"),
                start: 0,
                end: -1
            }
        );
    }

    #[test]
    fn test_parse_decimal_bytes_and_ascii_uppercase() {
        // Decimal parsing
        assert_eq!(parse_decimal_bytes(b"0"), Some(0));
        assert_eq!(parse_decimal_bytes(b"42"), Some(42));
        assert_eq!(parse_decimal_bytes(b"1048576"), Some(1048576));
        assert_eq!(parse_decimal_bytes(b""), None);
        assert_eq!(parse_decimal_bytes(b"-1"), None);
        assert_eq!(parse_decimal_bytes(b"abc"), None);

        // ASCII uppercase
        let mut buf = [0u8; 64];
        let mut heap = String::new();
        assert_eq!(bytes_to_uppercase_ascii(b"get", &mut buf, &mut heap), "GET");
        assert_eq!(bytes_to_uppercase_ascii(b"SeT", &mut buf, &mut heap), "SET");
        assert_eq!(
            bytes_to_uppercase_ascii(b"mget", &mut buf, &mut heap),
            "MGET"
        );

        // Command parsing through RESP array
        let mut buf = BytesMut::from("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Get(Bytes::from_static(b"foo")));

        let mut buf = BytesMut::from("*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\nbaz\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Set { key, value, .. } => {
                assert_eq!(key, Bytes::from_static(b"bar"));
                assert_eq!(value, Bytes::from_static(b"baz"));
            }
            _ => panic!("Expected Set command"),
        }

        // Unknown command
        let mut buf = BytesMut::from("*1\r\n$7\r\nunknown\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Unknown("UNKNOWN".to_string()));
    }

    #[test]
    fn test_smallvec_exists_and_sadd_parsing() {
        // Single-key EXISTS fast path
        let mut buf = BytesMut::from("*2\r\n$6\r\nEXISTS\r\n$4\r\nmyk1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Exists(keys) => {
                assert_eq!(keys.len(), 1);
                assert!(!keys.spilled());
                assert_eq!(keys[0], Bytes::from_static(b"myk1"));
            }
            _ => panic!("Expected Exists command"),
        }

        // Single-member SADD fast path
        let mut buf = BytesMut::from("*3\r\n$4\r\nSADD\r\n$4\r\nmyk1\r\n$4\r\nmbr1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Sadd { key, members } => {
                assert_eq!(key, Bytes::from_static(b"myk1"));
                assert_eq!(members.len(), 1);
                assert!(!members.spilled());
                assert_eq!(members[0], Bytes::from_static(b"mbr1"));
            }
            _ => panic!("Expected Sadd command"),
        }

        // Multi-key EXISTS
        let mut buf = BytesMut::from("*3\r\n$6\r\nEXISTS\r\n$2\r\nk1\r\n$2\r\nk2\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Exists(keys) => {
                assert_eq!(keys.len(), 2);
                assert!(keys.spilled());
                assert_eq!(keys[0], Bytes::from_static(b"k1"));
                assert_eq!(keys[1], Bytes::from_static(b"k2"));
            }
            _ => panic!("Expected Exists command"),
        }
    }

    #[test]
    fn test_resp_lrange_zrange_fast_slice_parser() {
        let mut buf = BytesMut::from(
            "*4\r\n$6\r\nLRANGE\r\n$4\r\nmyl1\r\n$1\r\n0\r\n$2\r\n10\r\n*4\r\n$6\r\nZRANGE\r\n$4\r\nmyz1\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        );
        let cmd1 = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd1,
            Command::Lrange {
                key: Bytes::from_static(b"myl1"),
                start: 0,
                stop: 10,
            }
        );
        let cmd2 = parse_command(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty());
        assert_eq!(
            cmd2,
            Command::Zrange {
                key: Bytes::from_static(b"myz1"),
                opts: crate::table::ZRangeOpts {
                    start: 0,
                    stop: -1,
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn test_smallvec_del_and_hset_parsing() {
        // Single-key DEL: not spilled
        let mut buf = BytesMut::from("*2\r\n$3\r\nDEL\r\n$4\r\nmyk1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Del(keys) => {
                assert_eq!(keys.len(), 1);
                assert!(!keys.spilled());
                assert_eq!(keys[0], Bytes::from_static(b"myk1"));
            }
            _ => panic!("Expected Del command"),
        }

        // Single-field HSET: not spilled
        let mut buf = BytesMut::from("*4\r\n$4\r\nHSET\r\n$4\r\nmyh1\r\n$2\r\nf1\r\n$2\r\nv1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Hset { key, fields } => {
                assert_eq!(key, Bytes::from_static(b"myh1"));
                assert_eq!(fields.len(), 1);
                assert!(!fields.spilled());
                assert_eq!(fields[0].0, Bytes::from_static(b"f1"));
                assert_eq!(fields[0].1, Bytes::from_static(b"v1"));
            }
            _ => panic!("Expected Hset command"),
        }

        // Multi-key DEL: spilled
        let mut buf = BytesMut::from("*3\r\n$3\r\nDEL\r\n$2\r\nk1\r\n$2\r\nk2\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Del(keys) => {
                assert_eq!(keys.len(), 2);
                assert!(keys.spilled());
                assert_eq!(keys[0], Bytes::from_static(b"k1"));
                assert_eq!(keys[1], Bytes::from_static(b"k2"));
            }
            _ => panic!("Expected Del command"),
        }

        // Single-element ZADD: not spilled
        let mut buf = BytesMut::from("*4\r\n$4\r\nZADD\r\n$4\r\nmyz1\r\n$2\r\n10\r\n$2\r\nm1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Zadd { key, elements, .. } => {
                assert_eq!(key, Bytes::from_static(b"myz1"));
                assert_eq!(elements.len(), 1);
                assert!(!elements.spilled());
                assert_eq!(elements[0].0, 10.0);
                assert_eq!(elements[0].1, Bytes::from_static(b"m1"));
            }
            _ => panic!("Expected Zadd command"),
        }

        // Single-element LPUSH: not spilled
        let mut buf = BytesMut::from("*3\r\n$5\r\nLPUSH\r\n$4\r\nmyl1\r\n$2\r\nv1\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Lpush { key, values } => {
                assert_eq!(key, Bytes::from_static(b"myl1"));
                assert_eq!(values.len(), 1);
                assert!(!values.spilled());
                assert_eq!(values[0], Bytes::from_static(b"v1"));
            }
            _ => panic!("Expected Lpush command"),
        }

        // Fast-path multi-key EXISTS (3 keys):
        let mut buf = BytesMut::from("*4\r\n$6\r\nEXISTS\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Exists(keys) => {
                assert_eq!(keys.len(), 3);
                assert_eq!(keys[0], Bytes::from_static(b"k1"));
                assert_eq!(keys[1], Bytes::from_static(b"k2"));
                assert_eq!(keys[2], Bytes::from_static(b"k3"));
            }
            _ => panic!("Expected Exists command"),
        }

        // Fast-path MGET (3 keys):
        let mut buf = BytesMut::from("*4\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Mget(keys) => {
                assert_eq!(keys.len(), 3);
                assert_eq!(keys[0], Bytes::from_static(b"k1"));
                assert_eq!(keys[1], Bytes::from_static(b"k2"));
                assert_eq!(keys[2], Bytes::from_static(b"k3"));
            }
            _ => panic!("Expected Mget command"),
        }

        // Fast-path MSET (2 pairs):
        let mut buf =
            BytesMut::from("*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Mset(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0, Bytes::from_static(b"k1"));
                assert_eq!(pairs[0].1, Bytes::from_static(b"v1"));
                assert_eq!(pairs[1].0, Bytes::from_static(b"k2"));
                assert_eq!(pairs[1].1, Bytes::from_static(b"v2"));
            }
            _ => panic!("Expected Mset command"),
        }
    }

    #[test]
    fn test_unlink_readonly_wait_object_commands() {
        // UNLINK
        let mut buf = BytesMut::from("*3\r\n$6\r\nUNLINK\r\n$2\r\nk1\r\n$2\r\nk2\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        match cmd {
            Command::Unlink(keys) => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0], Bytes::from_static(b"k1"));
                assert_eq!(keys[1], Bytes::from_static(b"k2"));
            }
            _ => panic!("Expected Unlink command for UNLINK"),
        }

        // READONLY
        let mut buf = BytesMut::from("*1\r\n$8\r\nREADONLY\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Readonly);

        // READWRITE
        let mut buf = BytesMut::from("*1\r\n$9\r\nREADWRITE\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Readwrite);

        // WAIT
        let mut buf = BytesMut::from("*3\r\n$4\r\nWAIT\r\n$1\r\n2\r\n$4\r\n1000\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Wait {
                numreplicas: 2,
                timeout: 1000,
            }
        );

        // WAITAOF
        let mut buf = BytesMut::from("*4\r\n$7\r\nWAITAOF\r\n$1\r\n1\r\n$1\r\n2\r\n$4\r\n1000\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::WaitAof {
                numlocal: 1,
                numreplicas: 2,
                timeout: 1000,
            }
        );

        // OBJECT ENCODING
        let mut buf = BytesMut::from("*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$5\r\nmykey\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Object(ObjectSubcommand::Encoding(Bytes::from_static(b"mykey")))
        );

        // OBJECT HELP
        let mut buf = BytesMut::from("*2\r\n$6\r\nOBJECT\r\n$4\r\nHELP\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Object(ObjectSubcommand::Help));

        // PROTO MAX BULK LEN
        let old = get_proto_max_bulk_len();
        set_proto_max_bulk_len(5);
        let mut buf = BytesMut::from("*2\r\n$4\r\nECHO\r\n$6\r\n123456\r\n");
        let err = parse_command(&mut buf).unwrap_err();
        assert!(err.contains("invalid bulk length"));
        set_proto_max_bulk_len(old);

        // XINFO STREAM
        let mut buf = BytesMut::from("*3\r\n$5\r\nXINFO\r\n$6\r\nSTREAM\r\n$8\r\nmystream\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Xinfo(XinfoSubcommand::Stream(Bytes::from_static(b"mystream")))
        );

        // XINFO GROUPS
        let mut buf = BytesMut::from("*3\r\n$5\r\nXINFO\r\n$6\r\nGROUPS\r\n$8\r\nmystream\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Xinfo(XinfoSubcommand::Groups(Bytes::from_static(b"mystream")))
        );

        // XINFO CONSUMERS
        let mut buf = BytesMut::from(
            "*4\r\n$5\r\nXINFO\r\n$9\r\nCONSUMERS\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Xinfo(XinfoSubcommand::Consumers {
                key: Bytes::from_static(b"mystream"),
                group: Bytes::from_static(b"mygroup"),
            })
        );

        // XINFO HELP
        let mut buf = BytesMut::from("*2\r\n$5\r\nXINFO\r\n$4\r\nHELP\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Xinfo(XinfoSubcommand::Help));

        // COMMAND COUNT / LIST
        let mut buf = BytesMut::from("*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::CommandCount);

        let mut buf = BytesMut::from("*2\r\n$7\r\nCOMMAND\r\n$4\r\nLIST\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::CommandList);

        // CLIENT SETINFO
        let mut buf = BytesMut::from(
            "*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nlib-name\r\n$8\r\nredis-py\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Client(ClientSubcommand::SetInfo {
                attr: "lib-name".to_string(),
                val: "redis-py".to_string(),
            })
        );

        // LATENCY subcommands
        let mut buf = BytesMut::from("*2\r\n$7\r\nLATENCY\r\n$6\r\nLATEST\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Latency(LatencySubcommand::Latest));

        let mut buf = BytesMut::from("*2\r\n$7\r\nLATENCY\r\n$6\r\nDOCTOR\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Latency(LatencySubcommand::Doctor));

        let mut buf = BytesMut::from("*3\r\n$7\r\nLATENCY\r\n$7\r\nHISTORY\r\n$7\r\ncommand\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Latency(LatencySubcommand::History("command".to_string()))
        );

        let mut buf = BytesMut::from("*2\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Latency(LatencySubcommand::Reset(vec![])));

        let mut buf = BytesMut::from("*3\r\n$7\r\nLATENCY\r\n$5\r\nGRAPH\r\n$7\r\ncommand\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Latency(LatencySubcommand::Graph("command".to_string()))
        );

        let mut buf = BytesMut::from("*2\r\n$7\r\nLATENCY\r\n$4\r\nHELP\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Latency(LatencySubcommand::Help));

        // PUBSUB HELP
        let mut buf = BytesMut::from("*2\r\n$6\r\nPUBSUB\r\n$4\r\nHELP\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::PubsubHelp);

        // FUNCTION STATS & KILL
        let mut buf = BytesMut::from("*2\r\n$8\r\nFUNCTION\r\n$5\r\nSTATS\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::FunctionStats);

        let mut buf = BytesMut::from("*2\r\n$8\r\nFUNCTION\r\n$4\r\nKILL\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::FunctionKill);

        // HEXPIRE / HTTL / HPERSIST
        let mut buf = BytesMut::from(
            "*8\r\n$7\r\nHEXPIRE\r\n$6\r\nmyhash\r\n$2\r\n10\r\n$2\r\nNX\r\n$6\r\nFIELDS\r\n$1\r\n2\r\n$2\r\nf1\r\n$2\r\nf2\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Hexpire {
                key: Bytes::from_static(b"myhash"),
                expire_ms: 10000,
                is_at: false,
                condition: HexpireCondition::Nx,
                fields: vec![Bytes::from_static(b"f1"), Bytes::from_static(b"f2")],
            }
        );

        let mut buf = BytesMut::from(
            "*5\r\n$4\r\nHTTL\r\n$6\r\nmyhash\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$2\r\nf1\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Httl {
                key: Bytes::from_static(b"myhash"),
                is_ms: false,
                is_expiretime: false,
                fields: vec![Bytes::from_static(b"f1")],
            }
        );

        let mut buf = BytesMut::from(
            "*5\r\n$8\r\nHPERSIST\r\n$6\r\nmyhash\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$2\r\nf1\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Hpersist {
                key: Bytes::from_static(b"myhash"),
                fields: vec![Bytes::from_static(b"f1")],
            }
        );

        // XCLAIM & XAUTOCLAIM
        let mut buf = BytesMut::from(
            "*7\r\n$6\r\nXCLAIM\r\n$2\r\ns1\r\n$2\r\ng1\r\n$2\r\nc2\r\n$1\r\n0\r\n$5\r\n100-0\r\n$6\r\nJUSTID\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Xclaim {
                key: Bytes::from_static(b"s1"),
                group: Bytes::from_static(b"g1"),
                consumer: Bytes::from_static(b"c2"),
                min_idle_time: 0,
                ids: vec![Bytes::from_static(b"100-0")],
                idle: None,
                time: None,
                retrycount: None,
                force: false,
                justid: true,
            }
        );

        let mut buf = BytesMut::from(
            "*8\r\n$10\r\nXAUTOCLAIM\r\n$2\r\ns1\r\n$2\r\ng1\r\n$2\r\nc2\r\n$1\r\n0\r\n$3\r\n0-0\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Xautoclaim {
                key: Bytes::from_static(b"s1"),
                group: Bytes::from_static(b"g1"),
                consumer: Bytes::from_static(b"c2"),
                min_idle_time: 0,
                start: Bytes::from_static(b"0-0"),
                count: 10,
                justid: false,
            }
        );
    }

    #[test]
    fn test_resp_semantic_cache_commands() {
        let mut buf = BytesMut::from(
            "*15\r\n$12\r\nSEMANTIC.SET\r\n$7\r\nllm:ns1\r\n$2\r\nq1\r\n$6\r\nprompt\r\n$4\r\nresp\r\n$6\r\nVECTOR\r\n$1\r\n3\r\n$3\r\n1.0\r\n$3\r\n0.0\r\n$3\r\n0.0\r\n$2\r\nEX\r\n$2\r\n60\r\n$5\r\nSCOPE\r\n$2\r\nt1\r\n$8\r\nQUANTIZE\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::SemanticSet {
                namespace: Bytes::from_static(b"llm:ns1"),
                id: Bytes::from_static(b"q1"),
                prompt: Bytes::from_static(b"prompt"),
                response: Bytes::from_static(b"resp"),
                vector: vec![1.0, 0.0, 0.0],
                ttl: Some(Duration::from_secs(60)),
                scope: Some(Bytes::from_static(b"t1")),
                quantize: true,
                tokens: None,
            }
        );

        let mut buf = BytesMut::from(
            "*14\r\n$12\r\nSEMANTIC.GET\r\n$7\r\nllm:ns1\r\n$6\r\nVECTOR\r\n$1\r\n3\r\n$3\r\n1.0\r\n$3\r\n0.0\r\n$3\r\n0.0\r\n$9\r\nTHRESHOLD\r\n$4\r\n0.92\r\n$5\r\nSCOPE\r\n$2\r\nt1\r\n$9\r\nWITHSCORE\r\n$10\r\nWITHPROMPT\r\n$6\r\nWITHID\r\n",
        );
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::SemanticGet {
                namespace: Bytes::from_static(b"llm:ns1"),
                query: vec![1.0, 0.0, 0.0],
                threshold: 0.92,
                scope: Some(Bytes::from_static(b"t1")),
                with_score: true,
                with_prompt: true,
                with_id: true,
            }
        );
    }

    fn cmd_args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|p| Bytes::from(p.to_string())).collect()
    }

    /// Hostile clients must get an error, never a panic, a hang or an
    /// unbounded allocation. Inputs stay short (<= 10 args), so any loop or
    /// capacity driven by a client-supplied count only terminates quickly if
    /// that count is validated against the arguments actually present.
    #[test]
    fn test_build_command_survives_hostile_arguments() {
        #[rustfmt::skip]
        const TOKENS: &[&[u8]] = &[
            b"", b"0", b"-1", b"1", b"2", b"3", b"-9223372036854775808", b"9223372036854775807",
            b"18446744073709551615", b"18446744073709551616", b"2147483647", b"4294967295",
            b"1000000000", b"nan", b"inf", b"1e400", b"0.5", b"\xff\xfe", b"\x00", b"k", b"$",
            b"$.a", b"(1", b"-", b"+", b"*", b"0-0", b"1-", b"18446744073709551615-0", b"LIMIT",
            b"COUNT", b"STORE", b"BY", b"GET", b"WITHSCORES", b"KEYS", b"MATCH", b"FIELDS", b"EX",
            b"PX", b"NX", b"XX", b"STREAMS", b">", b"ID", b"IDS", b"FILTER", b"ARGS", b"AGGREGATE",
            b"WEIGHTS", b"RANK", b"MAXLEN", b"~", b"VALUES", b"FP32", b"ELE", b"DIM", b"TYPE",
            b"SCHEMA", b"ON", b"PREFIX", b"TEXT", b"VECTOR", b"VEC", b"QUERY", b"HNSW", b"FLAT",
            b"RETURN", b"PARAMS", b"LOAD", b"GROUPBY", b"REDUCE", b"NUMKEYS", b"BLOCK",
        ];
        // Commands outside the ACL table (module families and extensions).
        #[rustfmt::skip]
        const EXTRA: &[&str] = &[
            "AGENT.CHECKPOINT.GET", "AGENT.CHECKPOINT.HISTORY", "AGENT.CHECKPOINT.PUT",
            "AGENT.MEM.ADD", "AGENT.MEM.CLEAR", "AGENT.MEM.COMPACT", "AGENT.MEM.CONTEXT",
            "AGENT.MEM.INFO", "AGENT.TOOL.CLAIM", "AGENT.TOOL.COMPLETE", "BF.ADD", "BF.EXISTS",
            "BF.INFO", "BF.MADD", "BF.MEXISTS", "BF.RESERVE", "CF.ADD", "CF.ADDNX", "CF.DEL",
            "CF.EXISTS", "CF.INFO", "CF.RESERVE", "CMS.INCRBY", "CMS.INFO", "CMS.INITBYDIM",
            "CMS.INITBYPROB", "CMS.QUERY", "CRDT.DEL", "CRDT.DUMP", "CRDT.GC", "CRDT.GET",
            "CRDT.INCRBY", "CRDT.MERGE", "CRDT.SADD", "CRDT.SET", "CRDT.SMEMBERS", "CRDT.SREM",
            "FT.ADD", "FT.AGGREGATE", "FT.ALTER", "FT.CREATE", "FT.DROPINDEX", "FT.EXPLAIN",
            "FT.HYBRID", "FT.INFO", "FT._LIST", "FT.PROFILE", "FT.SEARCH", "JSON.ARRAPPEND",
            "JSON.ARRLEN", "JSON.ARRPOP", "JSON.CLEAR", "JSON.DEL", "JSON.FORGET", "JSON.GET",
            "JSON.MGET", "JSON.NUMINCRBY", "JSON.NUMMULTBY", "JSON.OBJKEYS", "JSON.OBJLEN",
            "JSON.SET", "JSON.STRAPPEND", "JSON.STRLEN", "JSON.TOGGLE", "JSON.TYPE",
            "LLM.QUOTA.INFO", "LLM.QUOTA.RESERVE", "LLM.QUOTA.SETTLE", "MCP.CALL", "MCP.RPC",
            "MCP.TOOLS", "SEMANTIC.DEL", "SEMANTIC.FLUSH", "SEMANTIC.GET", "SEMANTIC.INFO",
            "SEMANTIC.SET", "TOPK.ADD", "TOPK.INFO", "TOPK.LIST", "TOPK.QUERY", "TOPK.RESERVE",
            "XDP.INFO", "XDP.INJECT", "XDP.PACKET", "XDP.RULE", "XDP.SOCKET", "XDP.STATS", "VDEL",
            "DFLY",
        ];
        let mut names: Vec<Vec<Bytes>> = crate::acl_categories::COMMANDS
            .iter()
            .map(|c| {
                c.name
                    .split('|')
                    .map(|w| Bytes::from(w.to_ascii_uppercase()))
                    .collect()
            })
            .collect();
        names.extend(EXTRA.iter().map(|n| vec![Bytes::from_static(n.as_bytes())]));

        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for name in &names {
            for extra in 0..=8 {
                for _ in 0..40 {
                    let mut args = name.clone();
                    for _ in 0..extra {
                        let t = TOKENS[(next() % TOKENS.len() as u64) as usize];
                        args.push(Bytes::from_static(t));
                    }
                    let shown = format!("{args:?}");
                    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let _ = build_command(args);
                    }));
                    assert!(res.is_ok(), "build_command panicked on {shown}");
                }
            }
        }
    }

    #[test]
    fn test_untrusted_counts_are_rejected() {
        let huge = "9223372036854775807";
        let max = "18446744073709551615";
        let rejected: &[&[&str]] = &[
            // Used to loop 2^63 times.
            &["FT.CREATE", "i", "PREFIX", huge, "a"],
            &["FT.CREATE", "x", "0x10", "PREFIX", huge, "ARGS", "inf", "*"],
            &["FT.SEARCH", "i", "*", "RETURN", huge, "f"],
            &["FT.SEARCH", "i", "*", "PARAMS", max, "k", "v"],
            &["FT.AGGREGATE", "i", "*", "LOAD", huge, "@f"],
            &["FT.AGGREGATE", "i", "*", "GROUPBY", huge, "@f"],
            &[
                "FT.AGGREGATE",
                "i",
                "*",
                "GROUPBY",
                "1",
                "@f",
                "REDUCE",
                "COUNT",
                huge,
            ],
            &[
                "FT.PROFILE",
                "i",
                "SEARCH",
                "QUERY",
                "*",
                "PARAMS",
                huge,
                "k",
                "v",
            ],
            &["FT.HYBRID", "i", "t", "v", "PARAMS", huge, "k", "v"],
            &["FT.HYBRID", "i", "t", "v", "RETURN", huge, "f"],
            // Used to overflow `start + count` and panic slicing.
            &[
                "FT.CREATE",
                "i",
                "SCHEMA",
                "v",
                "VECTOR",
                "HNSW",
                "18446744073709551614",
                "DIM",
                "2",
            ],
            &["HSETEX", "k", "FIELDS", huge, "f", "v"],
            &["HSETEX", "k", "ELE", "FIELDS", huge, "\0"],
            &["VADD", "k", "VALUES", max, "1", "e"],
            &["VSIM", "k", "VALUES", max, "1"],
            &["EVAL", "s", max, "k"],
            &["EVALSHA", "s", max, "k"],
            &["FCALL", "f", max, "k"],
            &["SEMANTIC.SET", "ns", "id", "p", "r", "VECTOR", max, "1"],
            &["SEMANTIC.GET", "ns", "VECTOR", max, "1"],
            // Used to preallocate numkeys (2^31) pairs and abort.
            &["MSETEX", "2147483647", "k", "v"],
        ];
        for parts in rejected {
            let res = build_command(cmd_args(parts));
            assert!(res.is_err(), "{parts:?} should be rejected, got {res:?}");
        }
        // A huge dim is no longer an overflow; it falls through to blob decoding.
        for cmd in ["AGENT.MEM.ADD", "AGENT.MEM.COMPACT"] {
            let _ = build_command(cmd_args(&[cmd, "s", "r", "c", "VEC", max, "1"]));
        }
        let _ = build_command(cmd_args(&[
            "AGENT.MEM.CONTEXT",
            "s",
            "x",
            "QUERY",
            max,
            "1",
        ]));
    }

    #[test]
    fn test_counted_options_still_parse_valid_input() {
        match build_command(cmd_args(&[
            "FT.CREATE",
            "i",
            "PREFIX",
            "2",
            "a:",
            "b:",
            "SCHEMA",
            "t",
            "TEXT",
        ])) {
            Ok(Some(Command::FtCreate { prefixes, .. })) => assert_eq!(prefixes, ["a:", "b:"]),
            other => panic!("{other:?}"),
        }
        match build_command(cmd_args(&[
            "FT.SEARCH",
            "i",
            "*",
            "RETURN",
            "1",
            "f",
            "PARAMS",
            "3",
            "k",
            "v",
            "DIALECT",
            "2",
        ])) {
            Ok(Some(Command::FtSearch { options, .. })) => {
                assert_eq!(options.return_fields, Some(vec!["f".to_string()]));
                assert_eq!(options.params.get("k").map(Vec::as_slice), Some(&b"v"[..]));
                assert_eq!(options.dialect, Some(2));
            }
            other => panic!("{other:?}"),
        }
        match build_command(cmd_args(&[
            "FT.AGGREGATE",
            "i",
            "*",
            "LOAD",
            "2",
            "@a",
            "b",
        ])) {
            Ok(Some(Command::FtAggregate { options, .. })) => {
                assert_eq!(options.load_fields, ["a", "b"]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn test_framing_rejects_hostile_lengths() {
        // Oversized multibulk count: rejected before anything is allocated.
        let mut buf = BytesMut::from("*1000000000\r\n");
        let err = parse_command(&mut buf).unwrap_err();
        assert!(err.contains("invalid multibulk length"), "{err}");
        // At the limit the parser just waits for the arguments.
        let mut buf = BytesMut::from("*1048576\r\n");
        assert_eq!(parse_command(&mut buf), Ok(None));

        // A memcached block length beyond proto-max-bulk-len closes the
        // connection instead of overflowing or buffering forever.
        let mut buf = BytesMut::from(format!("set k 0 0 {}\r\n", usize::MAX).as_str());
        let err = parse_command(&mut buf).unwrap_err();
        assert!(err.starts_with("Protocol error:"), "{err}");

        // A bad data chunk is consumed, so the connection loop (which keeps
        // parsing after a non-protocol error) makes progress.
        let mut buf = BytesMut::from("set k 0 0 1\r\nab\r\nPING\r\n");
        let err = parse_command(&mut buf).unwrap_err();
        assert!(err.contains("bad data chunk"), "{err}");
        assert_eq!(parse_command(&mut buf), Ok(Some(Command::Ping(None))));
    }

    /// Blank lines, `*0` and `*-1` used to recurse once per frame, so a
    /// buffer of them (a byte or two each) overflowed the stack and aborted
    /// the process. They are skipped in a loop now; a 256 KiB stack is plenty.
    #[test]
    fn test_empty_frames_do_not_recurse() {
        std::thread::Builder::new()
            .stack_size(256 << 10)
            .spawn(|| {
                for frame in [&b"\n"[..], b"\r\n", b" \t\r\n", b"*0\r\n", b"*-1\r\n"] {
                    let mut buf = BytesMut::new();
                    for _ in 0..500_000 {
                        buf.extend_from_slice(frame);
                    }
                    buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
                    assert_eq!(parse_command(&mut buf), Ok(Some(Command::Ping(None))));
                    assert!(buf.is_empty());
                    // Only empty frames: all consumed, then more data is awaited.
                    buf.extend_from_slice(&frame.repeat(1000));
                    assert_eq!(parse_command(&mut buf), Ok(None));
                    assert!(buf.is_empty());
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Like RedisBloom, BF.RESERVE needs 0 < error rate < 1; NaN used to build
    /// a minimum-size filter.
    #[test]
    fn test_bf_reserve_error_rate_range() {
        for rate in ["nan", "NaN", "0", "-0.1", "1", "1.5", "inf", "-inf"] {
            assert_eq!(
                build_command(cmd_args(&["BF.RESERVE", "b", rate, "100"])).unwrap_err(),
                "(0 < error rate range < 1)",
                "{rate}"
            );
        }
        for rate in ["0.000001", "0.01", "0.999"] {
            assert!(build_command(cmd_args(&["BF.RESERVE", "b", rate, "100"])).is_ok());
        }
    }

    /// Client sizes that reached allocations or loops at execution time are
    /// refused by the parser with Redis-compatible errors.
    #[test]
    fn test_vector_and_random_count_limits() {
        let max_dim = crate::vector::MAX_VECTOR_DIM.to_string();
        let over_dim = (crate::vector::MAX_VECTOR_DIM + 1).to_string();
        let over_m = (crate::vector::MAX_HNSW_M + 1).to_string();
        let over_ef = (crate::vector::MAX_VSET_EF + 1).to_string();
        let flat = |dim: &str| {
            cmd_args(&[
                "FT.CREATE",
                "i",
                "SCHEMA",
                "v",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                dim,
                "DISTANCE_METRIC",
                "L2",
            ])
        };
        assert!(build_command(flat(&max_dim)).is_ok());
        assert_eq!(
            build_command(flat(&over_dim)).unwrap_err(),
            "Bad arguments for vector similarity FLAT argument DIM"
        );
        let hnsw_m = |m: &str| {
            cmd_args(&[
                "FT.CREATE",
                "i",
                "SCHEMA",
                "v",
                "VECTOR",
                "HNSW",
                "8",
                "TYPE",
                "FLOAT32",
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
                "M",
                m,
            ])
        };
        assert!(build_command(hnsw_m("4096")).is_ok());
        assert_eq!(
            build_command(hnsw_m(&over_m)).unwrap_err(),
            "Bad arguments for vector similarity HNSW argument M"
        );
        for (attr, val) in [("DIM", over_dim.as_str()), ("M", over_m.as_str())] {
            let err = build_command(cmd_args(&[
                "FT.ALTER", "i", "SCHEMA", "ADD", "w", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
                attr, val,
            ]))
            .unwrap_err();
            assert_eq!(
                err,
                format!("Bad arguments for vector similarity HNSW argument {attr}")
            );
        }

        // VADD: vector dimension, REDUCE, EF and M as in Redis vector sets.
        let blob = vec![0u8; (crate::vector::MAX_VECTOR_DIM + 1) * 4];
        let mut args = cmd_args(&["VADD", "k", "FP32"]);
        args.push(Bytes::from(blob));
        args.push(Bytes::from_static(b"e"));
        assert_eq!(
            build_command(args).unwrap_err(),
            "invalid vector specification"
        );
        let vadd = |extra: &[&str]| {
            let mut a = vec!["VADD", "k"];
            a.extend_from_slice(extra);
            build_command(cmd_args(&a))
        };
        assert_eq!(
            vadd(&["REDUCE", "3", "VALUES", "2", "1", "1", "e"]).unwrap_err(),
            "invalid vector specification"
        );
        assert!(vadd(&["REDUCE", "1", "VALUES", "2", "1", "1", "e"]).is_ok());
        assert_eq!(
            vadd(&["VALUES", "2", "1", "1", "e", "EF", &over_ef]).unwrap_err(),
            "invalid EF"
        );
        assert_eq!(
            vadd(&["VALUES", "2", "1", "1", "e", "M", &over_m]).unwrap_err(),
            "invalid M"
        );
        assert!(vadd(&["VALUES", "2", "1", "1", "e", "EF", "1000000", "M", "4096"]).is_ok());
        // input dim x reduced dim floats: 4096 x 4097 > MAX_PROJECTION_ENTRIES.
        let mut big = cmd_args(&["VADD", "k", "REDUCE", "4097", "FP32"]);
        big.push(Bytes::from(vec![0u8; 8192 * 4]));
        big.push(Bytes::from_static(b"e"));
        assert!(
            build_command(big)
                .unwrap_err()
                .starts_with("REDUCE projection too large")
        );
        assert_eq!(
            build_command(cmd_args(&["VSIM", "k", "ELE", "e", "EF", &over_ef])).unwrap_err(),
            "invalid EF"
        );

        // Repeating random counts are bounded; Redis streams them, rudis buffers.
        let limit = MAX_RANDOM_REPEATS.to_string();
        let over = (-MAX_RANDOM_REPEATS - 1).to_string();
        for cmd in ["SRANDMEMBER", "HRANDFIELD", "ZRANDMEMBER", "VRANDMEMBER"] {
            assert!(build_command(cmd_args(&[cmd, "k", &format!("-{limit}")])).is_ok());
            assert_eq!(
                build_command(cmd_args(&[cmd, "k", &over])).unwrap_err(),
                "value is out of range",
                "{cmd}"
            );
            assert!(build_command(cmd_args(&[cmd, "k", "9223372036854775807"])).is_ok());
        }
    }
}
