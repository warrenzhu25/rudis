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
        bcast: bool,
        prefixes: Vec<Bytes>,
    },
    Caching(bool),
    Unblock {
        client_id: u64,
        unblock_type: crate::block::ClientUnblockType,
    },
    Pause(u64),
    Unpause,
    NoTouch(bool),
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
pub enum Command {
    Object(ObjectSubcommand),
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
    Exists(SmallVec<[Bytes; 1]>),
    IncrBy(Bytes, i64),
    Expire(Bytes, Duration),
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
    CommandDocs,
    Info(Option<Bytes>),
    Replicaof {
        host: Bytes,
        port: Bytes,
    },
    Psync {
        replid: Bytes,
        offset: i64,
    },
    Replconf(Vec<Bytes>),
    Role,
    // LUA SCRIPTING COMMANDS
    Eval {
        script: Bytes,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    Evalsha {
        sha: Bytes,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    ScriptLoad(Bytes),
    ScriptExists(Vec<Bytes>),
    ScriptFlush,
    // TIERED STORAGE COMMANDS
    Tier(TierSubcommand),
    // CONFIG COMMANDS
    ConfigGet(Bytes),
    ConfigSet(Bytes, Bytes),
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
    },
    Bitpos {
        key: Bytes,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
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
        id: crate::table::StreamAddId,
        fields: Vec<(Bytes, Bytes)>,
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
    },
    XgroupCreate {
        key: Bytes,
        group: Bytes,
        id: String,
        mkstream: bool,
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
    Xreadgroup {
        group: Bytes,
        consumer: Bytes,
        count: Option<usize>,
        block_ms: Option<u64>,
        noack: bool,
        keys: Vec<Bytes>,
        ids: Vec<String>,
    },
    Xack {
        key: Bytes,
        group: Bytes,
        ids: Vec<crate::table::StreamId>,
    },
    Xpending {
        key: Bytes,
        group: Bytes,
        range: Option<(
            crate::table::StreamId,
            crate::table::StreamId,
            usize,
            Option<Bytes>,
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
        cursor: usize,
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
        index: String,
        key: Bytes,
        vector: Vec<f32>,
        metric: Option<crate::vector::VectorMetric>,
        quantize: bool,
        pq: bool,
        tiered: bool,
    },
    Vquery {
        index: String,
        k: usize,
        query: Vec<f32>,
        rerank: bool,
    },
    Vsim {
        index: String,
        k1: Bytes,
        k2: Bytes,
        metric: Option<crate::vector::VectorMetric>,
    },
    Vdel {
        index: String,
        key: Bytes,
    },
    Vinfo(String),
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
    CrdtMerge(Bytes),
    CrdtGc(Option<u64>),
    // REDIS 7 FUNCTIONS
    FunctionLoad {
        replace: bool,
        code: Bytes,
    },
    Fcall {
        function: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    FunctionList,
    FunctionDelete(String),
    FunctionFlush,
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
        asc: Option<bool>,
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
        asc: Option<bool>,
    },
    Geosearch {
        key: Bytes,
        from_member: Option<Bytes>,
        from_lonlat: Option<(f64, f64)>,
        by_radius: Option<(f64, crate::geo::GeoUnit)>,
        by_box: Option<(f64, f64, crate::geo::GeoUnit)>,
        asc: Option<bool>,
        count: Option<usize>,
        withcoord: bool,
        withdist: bool,
        withhash: bool,
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

fn parse_memcached_storage_command(buf: &mut BytesMut) -> Result<Option<Option<Command>>, String> {
    let newline_pos = match find_crlf(buf) {
        Some(pos) => pos,
        None => return Ok(None),
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

    let total_len = newline_pos + 2 + bytes_len + 2;
    if buf.len() < total_len {
        return Ok(Some(None));
    }
    if &buf[newline_pos + 2 + bytes_len..total_len] != b"\r\n" {
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

/// Parse a single Redis command from the buffer.
/// Supports both RESP arrays (e.g., `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n`)
/// and inline commands (e.g., `GET foo\r\n`).
pub fn parse_command(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    if buf.is_empty() {
        return Ok(None);
    }

    if buf[0] == b'*' {
        parse_resp_array(buf)
    } else {
        match parse_memcached_storage_command(buf)? {
            Some(Some(cmd)) => Ok(Some(cmd)),
            Some(None) => Ok(None),
            None => parse_inline_command(buf),
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

fn parse_resp_array(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    let newline_pos = match find_crlf(buf) {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let line = &buf[1..newline_pos];
    let num_args: usize = match parse_decimal_bytes(line) {
        Some(n) => n,
        None => return Err("Invalid array length in RESP frame".to_string()),
    };

    // First check if the full frame is present before consuming any bytes from buf
    let mut scan_cursor = newline_pos + 2;
    let mut offsets = [(0usize, 0usize); 16];
    let is_small = num_args <= 16;

    #[allow(clippy::needless_range_loop)]
    for i in 0..num_args {
        if scan_cursor >= buf.len() {
            return Ok(None);
        }
        if buf[scan_cursor] != b'$' {
            return Err("Expected bulk string in command array".to_string());
        }

        let next_crlf = match find_crlf_at(buf, scan_cursor) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = &buf[scan_cursor + 1..next_crlf];
        let arg_len: usize = match parse_decimal_bytes(len_str) {
            Some(len) => len,
            None => return Err("Invalid bulk string length".to_string()),
        };

        if arg_len > get_proto_max_bulk_len() {
            return Err("Protocol error: excessive bulk string length".to_string());
        }

        let data_start = next_crlf + 2;
        let data_end = data_start + arg_len;

        if data_end + 2 > buf.len() {
            return Ok(None);
        }

        if &buf[data_end..data_end + 2] != b"\r\n" {
            return Err("Expected CRLF after bulk string data".to_string());
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
                            && let Ok(score) = score_str.parse::<f64>()
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
        buf.advance(newline_pos + 2); // Consume "*N\r\n"
        let mut args = Vec::with_capacity(num_args);

        for _ in 0..num_args {
            let header_crlf = find_crlf(buf).unwrap();
            let arg_len: usize = match parse_decimal_bytes(&buf[1..header_crlf]) {
                Some(len) => len,
                None => return Err("Invalid bulk string length".to_string()),
            };

            buf.advance(header_crlf + 2); // Consume "$len\r\n"
            let data = buf.split_to(arg_len).freeze(); // Zero-copy slice!
            buf.advance(2); // Consume "\r\n"
            args.push(data);
        }

        build_command(args)
    }
}

fn parse_inline_command(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    let newline_pos = match find_crlf(buf) {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let line = &buf[..newline_pos];
    let parts: Vec<Bytes> = line
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|part| !part.is_empty())
        .map(Bytes::copy_from_slice)
        .collect();

    buf.advance(newline_pos + 2);

    if parts.is_empty() {
        return Ok(None);
    }

    build_command(parts)
}

pub fn build_command(mut args: Vec<Bytes>) -> Result<Option<Command>, String> {
    if args.is_empty() {
        return Ok(None);
    }

    let mut cmd_buf = [0u8; 64];
    let mut cmd_heap = String::new();
    let cmd_name = bytes_to_uppercase_ascii(&args[0], &mut cmd_buf, &mut cmd_heap);

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
            if args.len() < 2 {
                return Err("wrong number of arguments for 'get' command".to_string());
            }
            if args.len() == 2 {
                Ok(Some(Command::Get(args[1].clone())))
            } else {
                Ok(Some(Command::MemcachedGet {
                    keys: args[1..].to_vec(),
                }))
            }
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
                        let sec: u64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        expire_in = Some(Duration::from_secs(sec));
                        i += 2;
                    }
                    "PX" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ms: u64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        expire_in = Some(Duration::from_millis(ms));
                        i += 2;
                    }
                    "EXAT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ts: u64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        let now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let dur = if ts <= now_unix {
                            Duration::from_millis(1)
                        } else {
                            Duration::from_secs(ts - now_unix)
                        };
                        expire_in = Some(dur);
                        i += 2;
                    }
                    "PXAT" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let ts: u64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        let now_unix_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let dur = if ts <= now_unix_ms {
                            Duration::from_millis(1)
                        } else {
                            Duration::from_millis(ts - now_unix_ms)
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
                return Err("wrong number of arguments for 'set'/'put' command".to_string());
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
                        if secs <= 0 || secs > (i64::MAX / 1000) {
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

            let mut pairs = Vec::with_capacity(numkeys);
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
                return Err("wrong number of arguments for 'del' command".to_string());
            }
            if cmd_name == "DELETE" {
                let noreply = args.len() > 2 && args[2].eq_ignore_ascii_case(b"noreply");
                Ok(Some(Command::MemcachedDelete {
                    key: args[1].clone(),
                    noreply,
                }))
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
            if args.len() < 2 {
                return Err("wrong number of arguments for 'incr' command".to_string());
            }
            if args.len() >= 3
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
            Ok(Some(Command::IncrBy(args[1].clone(), 1)))
        }
        "DECR" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'decr' command".to_string());
            }
            if args.len() >= 3
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
            Ok(Some(Command::IncrBy(args[1].clone(), -1)))
        }
        "INCRBY" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'incrby' command".to_string());
            }
            let delta = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::IncrBy(args[1].clone(), delta)))
        }
        "DECRBY" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'decrby' command".to_string());
            }
            let delta = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::IncrBy(args[1].clone(), -delta)))
        }
        "EXPIRE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'expire' command".to_string());
            }
            let secs: u64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Expire(
                args[1].clone(),
                Duration::from_secs(secs),
            )))
        }
        "PEXPIRE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'pexpire' command".to_string());
            }
            let ms: u64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Expire(
                args[1].clone(),
                Duration::from_millis(ms),
            )))
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
            let msg = if args.len() > 1 {
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
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'client setname' command".to_string()
                        );
                    }
                    let name = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Client(ClientSubcommand::SetName(name))))
                }
                "GETNAME" => Ok(Some(Command::Client(ClientSubcommand::GetName))),
                "ID" => Ok(Some(Command::Client(ClientSubcommand::Id))),
                "KILL" => Ok(Some(Command::Client(ClientSubcommand::Kill(
                    args[2..].to_vec(),
                )))),
                "TRACKING" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'client tracking' command".to_string()
                        );
                    }
                    let state = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let enabled = match state.as_str() {
                        "ON" => true,
                        "OFF" => false,
                        _ => return Err("syntax error: expected 'on' or 'off'".to_string()),
                    };
                    let mut bcast = false;
                    let mut prefixes = Vec::new();
                    let mut i = 3;
                    while i < args.len() {
                        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                        match opt.as_str() {
                            "BCAST" => {
                                bcast = true;
                                i += 1;
                            }
                            "PREFIX" if i + 1 < args.len() => {
                                prefixes.push(args[i + 1].clone());
                                i += 2;
                            }
                            _ => {
                                i += 1;
                            }
                        }
                    }
                    Ok(Some(Command::Client(ClientSubcommand::Tracking {
                        enabled,
                        bcast,
                        prefixes,
                    })))
                }
                "CACHING" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'client caching' command".to_string()
                        );
                    }
                    let state = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let flag = state == "YES";
                    Ok(Some(Command::Client(ClientSubcommand::Caching(flag))))
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
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'client pause' command".to_string()
                        );
                    }
                    let timeout: u64 = std::str::from_utf8(&args[2])
                        .map_err(|_| "value is not an integer or out of range".to_string())?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Client(ClientSubcommand::Pause(timeout))))
                }
                "UNPAUSE" => Ok(Some(Command::Client(ClientSubcommand::Unpause))),
                "NO-TOUCH" => {
                    if args.len() < 3 {
                        return Err(
                            "wrong number of arguments for 'client no-touch' command".to_string()
                        );
                    }
                    let opt = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let enabled = opt == "ON" || opt == "YES" || opt == "1";
                    Ok(Some(Command::Client(ClientSubcommand::NoTouch(enabled))))
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
                _ => Err(format!("unknown subcommand '{}' for 'memory'", sub)),
            }
        }
        "DEFRAG" | "ACTIVE-DEFRAG" => Ok(Some(Command::Memory(MemorySubcommand::Defrag))),
        "EXPIREAT" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'expireat' command".to_string());
            }
            let ts: u64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let now_unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let dur = if ts <= now_unix {
                Duration::from_millis(1)
            } else {
                Duration::from_secs(ts - now_unix)
            };
            Ok(Some(Command::Expire(args[1].clone(), dur)))
        }
        "PEXPIREAT" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'pexpireat' command".to_string());
            }
            let ts_ms: u64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            let now_unix_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let dur = if ts_ms <= now_unix_ms {
                Duration::from_millis(1)
            } else {
                Duration::from_millis(ts_ms - now_unix_ms)
            };
            Ok(Some(Command::Expire(args[1].clone(), dur)))
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
            let secs: u64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Set {
                key: args[1].clone(),
                value: args[3].clone(),
                expire_in: Some(Duration::from_secs(secs)),
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
            let ms: u64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Set {
                key: args[1].clone(),
                value: args[3].clone(),
                expire_in: Some(Duration::from_millis(ms)),
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
        "COMMAND" => Ok(Some(Command::CommandDocs)),
        "INFO" => {
            let section = if args.len() > 1 {
                Some(args[1].clone())
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
        "REPLCONF" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'replconf' command".to_string());
            }
            Ok(Some(Command::Replconf(args[1..].to_vec())))
        }
        "ROLE" => Ok(Some(Command::Role)),
        "EVAL" | "EVAL_RO" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'eval' command".to_string());
            }
            let script = args[1].clone();
            let numkeys: usize = match std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(n) => n,
                None => return Err("ERR value is not an integer or out of range".to_string()),
            };
            if 3 + numkeys > args.len() {
                return Err("ERR Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let script_args = args[3 + numkeys..].to_vec();
            Ok(Some(Command::Eval {
                script,
                keys,
                args: script_args,
            }))
        }
        "EVALSHA" | "EVALSHA_RO" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'evalsha' command".to_string());
            }
            let sha = args[1].clone();
            let numkeys: usize = match std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(n) => n,
                None => return Err("ERR value is not an integer or out of range".to_string()),
            };
            if 3 + numkeys > args.len() {
                return Err("ERR Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let script_args = args[3 + numkeys..].to_vec();
            Ok(Some(Command::Evalsha {
                sha,
                keys,
                args: script_args,
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
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'config get' command".to_string()
                        );
                    }
                    Ok(Some(Command::ConfigGet(args[2].clone())))
                }
                "SET" => {
                    if args.len() < 4 {
                        return Err(
                            "wrong number of arguments for 'config set' command".to_string()
                        );
                    }
                    let val = if args.len() == 4 {
                        let mut v = args[3].clone();
                        if (v.starts_with(b"\"") && v.ends_with(b"\""))
                            || (v.starts_with(b"'") && v.ends_with(b"'"))
                        {
                            v = v.slice(1..v.len() - 1);
                        }
                        v
                    } else {
                        let mut combined = Vec::new();
                        for (i, a) in args[3..].iter().enumerate() {
                            if i > 0 {
                                combined.push(b' ');
                            }
                            combined.extend_from_slice(a);
                        }
                        if (combined.starts_with(b"\"") && combined.ends_with(b"\""))
                            || (combined.starts_with(b"'") && combined.ends_with(b"'"))
                        {
                            combined = combined[1..combined.len() - 1].to_vec();
                        }
                        Bytes::from(combined)
                    };
                    Ok(Some(Command::ConfigSet(args[2].clone(), val)))
                }
                "RESETSTAT" => Ok(Some(Command::ConfigSet(
                    Bytes::from_static(b"resetstat"),
                    Bytes::new(),
                ))),
                "REWRITE" => Ok(Some(Command::ConfigSet(
                    Bytes::from_static(b"rewrite"),
                    Bytes::new(),
                ))),
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
                        i += 2;
                    }
                    _ => return Err("syntax error".to_string()),
                }
            }
            Ok(Some(Command::Scan {
                cursor,
                pattern,
                count,
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
            if offset_raw >= (1u64 << 32) {
                return Err("bit offset is not an integer or out of range".to_string());
            }
            let offset = offset_raw as usize;
            let val_str = std::str::from_utf8(&args[3])
                .map_err(|_| "bit is not an integer or out of range")?;
            let value: u8 = val_str
                .parse()
                .map_err(|_| "bit is not an integer or out of range")?;
            if value > 1 {
                return Err("bit is not an integer or out of range".to_string());
            }
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
            let offset: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "bit offset is not an integer or out of range")?
                .parse()
                .map_err(|_| "bit offset is not an integer or out of range")?;
            Ok(Some(Command::Getbit {
                key: args[1].clone(),
                offset,
            }))
        }
        "BITCOUNT" => {
            if args.len() != 2 && args.len() != 4 && args.len() != 5 {
                return Err("wrong number of arguments for 'bitcount' command".to_string());
            }
            let mut start = None;
            let mut end = None;
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
            Ok(Some(Command::Bitcount {
                key: args[1].clone(),
                start,
                end,
            }))
        }
        "BITPOS" => {
            if args.len() < 3 || args.len() > 5 {
                return Err("wrong number of arguments for 'bitpos' command".to_string());
            }
            let bit_str =
                std::str::from_utf8(&args[2]).map_err(|_| "The bit argument must be 1 or 0.")?;
            let bit: u8 = bit_str
                .parse()
                .map_err(|_| "The bit argument must be 1 or 0.")?;
            if bit > 1 {
                return Err("The bit argument must be 1 or 0.".to_string());
            }
            let start = if args.len() >= 4 {
                Some(
                    std::str::from_utf8(&args[3])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse()
                        .map_err(|_| "value is not an integer or out of range")?,
                )
            } else {
                None
            };
            let end = if args.len() == 5 {
                Some(
                    std::str::from_utf8(&args[4])
                        .map_err(|_| "value is not an integer or out of range")?
                        .parse()
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
                return Err("BITOP NOT takes only one source key".to_string());
            }
            Ok(Some(Command::Bitop {
                op,
                destkey,
                srckeys,
            }))
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
                        if i < args.len() && (args[i].as_ref() == b"=" || args[i].as_ref() == b"~")
                        {
                            i += 1;
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
                        if i < args.len() && (args[i].as_ref() == b"=" || args[i].as_ref() == b"~")
                        {
                            i += 1;
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
                        i += 2;
                    }
                    _ => break,
                }
            }
            if i >= args.len() {
                return Err("wrong number of arguments for 'xadd' command".to_string());
            }
            let id_str = std::str::from_utf8(&args[i])
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            let id = crate::table::StreamAddId::parse(id_str).map_err(|e| e.to_string())?;
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
                id,
                fields,
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
                    _ => return Err("syntax error".to_string()),
                }
            }
            let rem = args.len() - i;
            if rem < 2 || !rem.is_multiple_of(2) {
                return Err("ERR Unbalanced XREAD list of streams and IDs".to_string());
            }
            let n = rem / 2;
            let keys: Vec<Bytes> = args[i..i + n].to_vec();
            let ids: Vec<String> = args[i + n..]
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect();
            Ok(Some(Command::Xread {
                count,
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
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "MAXLEN" => {
                        i += 1;
                        if i < args.len() && (args[i].as_ref() == b"=" || args[i].as_ref() == b"~")
                        {
                            i += 1;
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
                        if i < args.len() && (args[i].as_ref() == b"=" || args[i].as_ref() == b"~")
                        {
                            i += 1;
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
                        i += 2;
                    }
                    _ => {
                        i += 1;
                    }
                }
            }
            Ok(Some(Command::Xtrim { key, maxlen, minid }))
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
                    for arg in &args[5..] {
                        if arg.eq_ignore_ascii_case(b"MKSTREAM") {
                            mkstream = true;
                        }
                    }
                    Ok(Some(Command::XgroupCreate {
                        key,
                        group,
                        id,
                        mkstream,
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
                _ => Ok(Some(Command::Unknown(format!("XGROUP {}", sub)))),
            }
        }
        "XREADGROUP" => {
            if args.len() < 6 {
                return Err("wrong number of arguments for 'xreadgroup' command".to_string());
            }
            if !args[1].eq_ignore_ascii_case(b"GROUP") {
                return Err("syntax error".to_string());
            }
            let group = args[2].clone();
            let consumer = args[3].clone();
            let mut count = None;
            let mut block_ms = None;
            let mut noack = false;
            let mut i = 4;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
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
            let rem = args.len() - i;
            if rem < 2 || !rem.is_multiple_of(2) {
                return Err("ERR Unbalanced XREADGROUP list of streams and IDs".to_string());
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
                block_ms,
                noack,
                keys,
                ids,
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
                if args[start_idx].eq_ignore_ascii_case(b"IDLE") {
                    start_idx += 2;
                }
                if args.len() < start_idx + 3 {
                    return Err("syntax error".to_string());
                }
                let start_s = std::str::from_utf8(&args[start_idx]).map_err(|_| "syntax error")?;
                let start = if start_s == "-" {
                    crate::table::StreamId::default()
                } else {
                    crate::table::StreamId::parse(start_s)?
                };
                let end_s =
                    std::str::from_utf8(&args[start_idx + 1]).map_err(|_| "syntax error")?;
                let end = if end_s == "+" {
                    crate::table::StreamId::new(u64::MAX, u64::MAX)
                } else {
                    crate::table::StreamId::parse(end_s)?
                };
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
                    range: Some((start, end, count, consumer)),
                }))
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
                let c: i64 = std::str::from_utf8(&args[2])
                    .map_err(|_| "value is not an integer or out of range".to_string())?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range".to_string())?;
                if c == i64::MIN {
                    return Err("value is out of range".to_string());
                }
                Some(c)
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
            Ok(Some(Command::Hscan {
                key: args[1].clone(),
                cursor,
                pattern,
                count,
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
                let c: i64 = std::str::from_utf8(&args[2])
                    .map_err(|_| "value is not an integer or out of range".to_string())?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range".to_string())?;
                if c == i64::MIN {
                    return Err("value is out of range".to_string());
                }
                Some(c)
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
                let c: i64 = std::str::from_utf8(&args[2])
                    .map_err(|_| "value is not an integer or out of range".to_string())?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range".to_string())?;
                if c == i64::MIN {
                    return Err("value is out of range".to_string());
                }
                Some(c)
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
        "SORT" | "SORT_RO" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'sort' command".to_string());
            }
            let key = args[1].clone();
            let mut desc = false;
            let mut alpha = false;
            let mut store = None;
            let mut limit = None;
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
                    "STORE" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        store = Some(args[i + 1].clone());
                        i += 2;
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
                    _ => {
                        i += 1;
                    }
                }
            }
            Ok(Some(Command::Sort {
                key,
                desc,
                alpha,
                store,
                limit,
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
                return Err("value is NaN or Infinity".to_string());
            }
            Ok(Some(Command::Incrbyfloat {
                key: args[1].clone(),
                increment,
            }))
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
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let key = args[2].clone();
            let mut vector = Vec::with_capacity(args.len() - 3);
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
                } else {
                    let val: f32 = s.parse().map_err(|_| "not a valid float")?;
                    vector.push(val);
                }
            }
            Ok(Some(Command::Vadd {
                index,
                key,
                vector,
                metric: None,
                quantize,
                pq,
                tiered,
            }))
        }
        "VQUERY" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'vquery' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
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
                index,
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
            let ttl = if args.len() > 1 {
                let s = std::str::from_utf8(&args[1])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                Some(s)
            } else {
                None
            };
            Ok(Some(Command::CrdtGc(ttl)))
        }
        "VSIM" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'vsim' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let k1 = args[2].clone();
            let k2 = args[3].clone();
            let metric = if args.len() > 4 {
                let s = String::from_utf8_lossy(&args[4]);
                s.parse::<crate::vector::VectorMetric>().ok()
            } else {
                None
            };
            Ok(Some(Command::Vsim {
                index,
                k1,
                k2,
                metric,
            }))
        }
        "VDEL" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'vdel' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            let key = args[2].clone();
            Ok(Some(Command::Vdel { index, key }))
        }
        "VINFO" => {
            if args.len() != 2 {
                return Err("wrong number of arguments for 'vinfo' command".to_string());
            }
            let index = String::from_utf8_lossy(&args[1]).to_string();
            Ok(Some(Command::Vinfo(index)))
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
                    let mut replace = false;
                    let mut code_idx = 2;
                    if args.len() >= 4
                        && String::from_utf8_lossy(&args[2]).to_uppercase() == "REPLACE"
                    {
                        replace = true;
                        code_idx = 3;
                    }
                    let code = args[code_idx].clone();
                    Ok(Some(Command::FunctionLoad { replace, code }))
                }
                "LIST" => Ok(Some(Command::FunctionList)),
                "FLUSH" => Ok(Some(Command::FunctionFlush)),
                "DELETE" => {
                    if args.len() != 3 {
                        return Err(
                            "wrong number of arguments for 'function delete' command".to_string()
                        );
                    }
                    let lib = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::FunctionDelete(lib)))
                }
                _ => Ok(Some(Command::Unknown(format!("FUNCTION {}", sub)))),
            }
        }
        "FCALL" | "FCALL_RO" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'fcall' command".to_string());
            }
            let function = String::from_utf8_lossy(&args[1]).to_string();
            let numkeys: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not an integer or out of range")?
                .parse()
                .map_err(|_| "value is not an integer or out of range")?;
            if args.len() < 3 + numkeys {
                return Err("Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let func_args = args[3 + numkeys..].to_vec();
            Ok(Some(Command::Fcall {
                function,
                keys,
                args: func_args,
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
            if !(args.len() - idx).is_multiple_of(3) || idx == args.len() {
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
            if args.len() < 3 {
                return Err("wrong number of arguments for 'geopos' command".to_string());
            }
            let key = args[1].clone();
            let members = args[2..].to_vec();
            Ok(Some(Command::Geopos { key, members }))
        }
        "GEOHASH" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'geohash' command".to_string());
            }
            let key = args[1].clone();
            let members = args[2..].to_vec();
            Ok(Some(Command::Geohash { key, members }))
        }
        "GEORADIUS" | "GEORADIUS_RO" => {
            if args.len() < 6 {
                return Err("wrong number of arguments for 'georadius' command".to_string());
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
            let radius: f64 = std::str::from_utf8(&args[4])
                .map_err(|_| "value is not a valid float")?
                .parse()
                .map_err(|_| "value is not a valid float")?;
            let unit = crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[5]))?;
            let mut withcoord = false;
            let mut withdist = false;
            let mut withhash = false;
            let mut count = None;
            let mut asc = None;
            let mut idx = 6;
            while idx < args.len() {
                let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
                match opt.as_str() {
                    "WITHCOORD" => withcoord = true,
                    "WITHDIST" => withdist = true,
                    "WITHHASH" => withhash = true,
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
                        count = Some(c);
                    }
                    _ => {}
                }
                idx += 1;
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
                asc,
            }))
        }
        "GEORADIUSBYMEMBER" | "GEORADIUSBYMEMBER_RO" => {
            if args.len() < 5 {
                return Err("wrong number of arguments for 'georadiusbymember' command".to_string());
            }
            let key = args[1].clone();
            let member = args[2].clone();
            let radius: f64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not a valid float")?
                .parse()
                .map_err(|_| "value is not a valid float")?;
            let unit = crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[4]))?;
            let mut withcoord = false;
            let mut withdist = false;
            let mut withhash = false;
            let mut count = None;
            let mut asc = None;
            let mut idx = 5;
            while idx < args.len() {
                let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
                match opt.as_str() {
                    "WITHCOORD" => withcoord = true,
                    "WITHDIST" => withdist = true,
                    "WITHHASH" => withhash = true,
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
                        count = Some(c);
                    }
                    _ => {}
                }
                idx += 1;
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
                asc,
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
            let mut withcoord = false;
            let mut withdist = false;
            let mut withhash = false;
            let mut idx = 2;
            while idx < args.len() {
                let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
                match opt.as_str() {
                    "FROMMEMBER" => {
                        if idx + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        idx += 1;
                        from_member = Some(args[idx].clone());
                    }
                    "FROMLONLAT" => {
                        if idx + 2 >= args.len() {
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
                        idx += 2;
                        from_lonlat = Some((lon, lat));
                    }
                    "BYRADIUS" => {
                        if idx + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let rad: f64 = std::str::from_utf8(&args[idx + 1])
                            .map_err(|_| "value is not a valid float")?
                            .parse()
                            .map_err(|_| "value is not a valid float")?;
                        let u =
                            crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[idx + 2]))?;
                        idx += 2;
                        by_radius = Some((rad, u));
                    }
                    "BYBOX" => {
                        if idx + 3 >= args.len() {
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
                        let u =
                            crate::geo::GeoUnit::parse(&String::from_utf8_lossy(&args[idx + 3]))?;
                        idx += 3;
                        by_box = Some((w, h, u));
                    }
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
                        count = Some(c);
                    }
                    "WITHCOORD" => withcoord = true,
                    "WITHDIST" => withdist = true,
                    "WITHHASH" => withhash = true,
                    _ => {}
                }
                idx += 1;
            }
            Ok(Some(Command::Geosearch {
                key,
                from_member,
                from_lonlat,
                by_radius,
                by_box,
                asc,
                count,
                withcoord,
                withdist,
                withhash,
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
                    for _ in 0..count {
                        if i < args.len() {
                            prefixes.push(String::from_utf8_lossy(&args[i]).to_string());
                            i += 1;
                        }
                    }
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
                        let algorithm = if i < args.len() {
                            String::from_utf8_lossy(&args[i]).to_string()
                        } else {
                            "HNSW".to_string()
                        };
                        i += 1;
                        let mut dim = 128;
                        let mut distance_metric = "COSINE".to_string();
                        while i < args.len() {
                            let sub_opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                            if sub_opt == "DIM" && i + 1 < args.len() {
                                dim = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(128);
                                i += 2;
                            } else if sub_opt == "DISTANCE_METRIC" && i + 1 < args.len() {
                                distance_metric =
                                    String::from_utf8_lossy(&args[i + 1]).to_uppercase();
                                i += 2;
                            } else if sub_opt == "TYPE"
                                || sub_opt == "FLOAT32"
                                || sub_opt == "M"
                                || sub_opt == "EF_CONSTRUCTION"
                            {
                                i += 2;
                            } else if sub_opt == "HNSW"
                                || sub_opt == "FLAT"
                                || sub_opt.parse::<usize>().is_ok()
                            {
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        Some(crate::search::FieldType::Vector {
                            dim,
                            distance_metric,
                            algorithm,
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
                    let mut r_fields = Vec::new();
                    for _ in 0..count {
                        if i < args.len() {
                            r_fields.push(String::from_utf8_lossy(&args[i]).to_string());
                            i += 1;
                        }
                    }
                    options.return_fields = Some(r_fields);
                } else if opt == "PARAMS" && i + 1 < args.len() {
                    let count: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    for _ in 0..(count / 2) {
                        if i + 1 < args.len() {
                            let k = String::from_utf8_lossy(&args[i]).to_string();
                            let v = args[i + 1].to_vec();
                            options.params.insert(k, v);
                            i += 2;
                        }
                    }
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
                    for _ in 0..nargs {
                        if i < args.len() {
                            let mut f = String::from_utf8_lossy(&args[i]).to_string();
                            if let Some(stripped) = f.strip_prefix('@') {
                                f = stripped.to_string();
                            }
                            options.load_fields.push(f);
                            i += 1;
                        }
                    }
                } else if opt == "GROUPBY" && i + 1 < args.len() {
                    let num_fields: usize =
                        String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                    i += 2;
                    let mut group_fields = Vec::new();
                    for _ in 0..num_fields {
                        if i < args.len() {
                            let mut f = String::from_utf8_lossy(&args[i]).to_string();
                            if let Some(stripped) = f.strip_prefix('@') {
                                f = stripped.to_string();
                            }
                            group_fields.push(f);
                            i += 1;
                        }
                    }
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
                        let mut reduce_args = Vec::new();
                        for _ in 0..nargs {
                            if i < args.len() {
                                let mut a = String::from_utf8_lossy(&args[i]).to_string();
                                if let Some(stripped) = a.strip_prefix('@') {
                                    a = stripped.to_string();
                                }
                                reduce_args.push(a);
                                i += 1;
                            }
                        }
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

fn find_crlf(buf: &[u8]) -> Option<usize> {
    find_crlf_at(buf, 0)
}

fn find_crlf_at(buf: &[u8], start: usize) -> Option<usize> {
    if buf.len() < start + 2 {
        return None;
    }
    buf[start..]
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|pos| start + pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resp_get() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Get(Bytes::from_static(b"mykey")));
        assert!(buf.is_empty());
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
            Command::Expire(Bytes::from_static(b"foo"), Duration::from_secs(60))
        );

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
                id: crate::table::StreamAddId::Auto,
                fields: vec![(Bytes::from_static(b"field1"), Bytes::from_static(b"val1"))],
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
                id: crate::table::StreamAddId::Explicit(crate::table::StreamId::new(100, 0)),
                fields: vec![(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))],
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
                count: Some(20)
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
            Command::Del(keys) => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0], Bytes::from_static(b"k1"));
                assert_eq!(keys[1], Bytes::from_static(b"k2"));
            }
            _ => panic!("Expected Del command for UNLINK"),
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
        assert!(err.contains("excessive bulk string length"));
        set_proto_max_bulk_len(old);
    }
}
