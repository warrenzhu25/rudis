use bytes::{Buf, Bytes, BytesMut};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SetSlotSubcommand {
    Migrating(String),
    Importing(String),
    Stable,
    Node(String),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ClusterSubcommand {
    KeySlot(Bytes),
    CountKeysInSlot(u16),
    GetKeysInSlot(u16, usize),
    SetSlot(u16, SetSlotSubcommand),
    Slots,
    Nodes,
    Info,
    Meet { ip: String, port: u16 },
    MyId,
    MigrateSlot { slot: u16, host: String, port: u16 },
    Rebalance { host: String, port: u16, slots: Option<usize> },
    Failover { force: bool },
    Reset { hard: bool },
    Forget(String),
    Replicate(String),
    SaveConfig,
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
    List,
    SetName(String),
    GetName,
    Id,
    Tracking {
        enabled: bool,
        bcast: bool,
        prefixes: Vec<Bytes>,
    },
    Caching(bool),
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
pub enum Command {
    Auth {
        username: Option<String>,
        password: String,
    },
    Acl(AclSubcommand),
    Get(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
    },
    Mget(Vec<Bytes>),
    Mset(Vec<(Bytes, Bytes)>),
    Del(Vec<Bytes>),
    Exists(Vec<Bytes>),
    IncrBy(Bytes, i64),
    Expire(Bytes, Duration),
    Persist(Bytes),
    Ttl(Bytes, bool), // true for PTTL (milliseconds), false for TTL (seconds)
    Cluster(ClusterSubcommand),
    Client(ClientSubcommand),
    Asking,
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
        fields: Vec<(Bytes, Bytes)>,
    },
    Hmset {
        key: Bytes,
        fields: Vec<(Bytes, Bytes)>,
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
    // LIST COMMANDS
    Lpush {
        key: Bytes,
        values: Vec<Bytes>,
    },
    Rpush {
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
        members: Vec<Bytes>,
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
    // ZSET COMMANDS
    Zadd {
        key: Bytes,
        elements: Vec<(f64, Bytes)>,
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
    },
    Zrevrank {
        key: Bytes,
        member: Bytes,
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
    Zpopmin {
        key: Bytes,
        count: usize,
    },
    Zpopmax {
        key: Bytes,
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
    // GENERIC & DATABASE COMMANDS
    Type(Bytes),
    Dbsize,
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
    PubsubChannels(Option<Bytes>),
    PubsubNumsub(Vec<Bytes>),
    PubsubNumpat,
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
        range: Option<(crate::table::StreamId, crate::table::StreamId, usize, Option<Bytes>)>,
    },
    // VALKEY EXTENDED COMMANDS
    Hello {
        proto: Option<u8>,
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
    Unknown(String),
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
        parse_inline_command(buf)
    }
}

fn parse_resp_array(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    let newline_pos = match find_crlf(buf) {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let line = &buf[1..newline_pos];
    let num_args: usize = match std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) => n,
        None => return Err("Invalid array length in RESP frame".to_string()),
    };

    // First check if the full frame is present before consuming any bytes from buf
    let mut scan_cursor = newline_pos + 2;

    for _ in 0..num_args {
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
        let arg_len: usize = match std::str::from_utf8(len_str)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(len) => len,
            None => return Err("Invalid bulk string length".to_string()),
        };

        let data_start = next_crlf + 2;
        let data_end = data_start + arg_len;

        if data_end + 2 > buf.len() {
            return Ok(None);
        }

        if &buf[data_end..data_end + 2] != b"\r\n" {
            return Err("Expected CRLF after bulk string data".to_string());
        }

        scan_cursor = data_end + 2;
    }

    // Full frame is present! Now extract args with zero-copy Bytes::freeze
    buf.advance(newline_pos + 2); // Consume "*N\r\n"
    let mut args = Vec::with_capacity(num_args);

    for _ in 0..num_args {
        let header_crlf = find_crlf(buf).unwrap();
        let arg_len: usize = std::str::from_utf8(&buf[1..header_crlf])
            .unwrap()
            .parse()
            .unwrap();

        buf.advance(header_crlf + 2); // Consume "$len\r\n"
        let data = buf.split_to(arg_len).freeze(); // Zero-copy slice!
        buf.advance(2); // Consume "\r\n"
        args.push(data);
    }

    build_command(args)
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

pub fn build_command(args: Vec<Bytes>) -> Result<Option<Command>, String> {
    if args.is_empty() {
        return Ok(None);
    }

    let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();

    match cmd_name.as_str() {
        "AUTH" => {
            if args.len() == 2 {
                let password = String::from_utf8_lossy(&args[1]).to_string();
                Ok(Some(Command::Auth { username: None, password }))
            } else if args.len() == 3 {
                let username = String::from_utf8_lossy(&args[1]).to_string();
                let password = String::from_utf8_lossy(&args[2]).to_string();
                Ok(Some(Command::Auth { username: Some(username), password }))
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
                        return Err("wrong number of arguments for 'acl|getuser' command".to_string());
                    }
                    let username = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Acl(AclSubcommand::GetUser(username))))
                }
                "SETUSER" => {
                    if args.len() < 3 {
                        return Err("wrong number of arguments for 'acl|setuser' command".to_string());
                    }
                    let username = String::from_utf8_lossy(&args[2]).to_string();
                    let rules = args[3..].iter().map(|a| String::from_utf8_lossy(a).to_string()).collect();
                    Ok(Some(Command::Acl(AclSubcommand::SetUser { username, rules })))
                }
                "DELUSER" => {
                    if args.len() < 3 {
                        return Err("wrong number of arguments for 'acl|deluser' command".to_string());
                    }
                    let usernames = args[2..].iter().map(|a| String::from_utf8_lossy(a).to_string()).collect();
                    Ok(Some(Command::Acl(AclSubcommand::DelUser(usernames))))
                }
                _ => Err(format!("unknown subcommand '{}' for 'acl'", sub)),
            }
        }
        "GET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'get' command".to_string());
            }
            Ok(Some(Command::Get(args[1].clone())))
        }
        "SET" | "PUT" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'set'/'put' command".to_string());
            }
            let mut expire_in = None;
            let mut i = 3;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "EX" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let secs: u64 = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                        expire_in = Some(Duration::from_secs(secs));
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
                    _ => {
                        i += 1;
                    }
                }
            }
            Ok(Some(Command::Set {
                key: args[1].clone(),
                value: args[2].clone(),
                expire_in,
            }))
        }
        "MGET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'mget' command".to_string());
            }
            Ok(Some(Command::Mget(args[1..].to_vec())))
        }
        "MSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
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
        "DEL" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'del' command".to_string());
            }
            Ok(Some(Command::Del(args[1..].to_vec())))
        }
        "EXISTS" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'exists' command".to_string());
            }
            Ok(Some(Command::Exists(args[1..].to_vec())))
        }
        "INCR" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'incr' command".to_string());
            }
            Ok(Some(Command::IncrBy(args[1].clone(), 1)))
        }
        "DECR" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'decr' command".to_string());
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
                "NODES" => Ok(Some(Command::Cluster(ClusterSubcommand::Nodes))),
                "INFO" => Ok(Some(Command::Cluster(ClusterSubcommand::Info))),
                "MYID" => Ok(Some(Command::Cluster(ClusterSubcommand::MyId))),
                "MEET" => {
                    if args.len() < 4 {
                        return Err("wrong number of arguments for 'cluster meet' command".to_string());
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
                    if args.len() < 4 {
                        return Err(
                            "wrong number of arguments for 'cluster rebalance' command"
                                .to_string(),
                        );
                    }
                    let host = String::from_utf8_lossy(&args[2]).to_string();
                    let port: u16 = std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    let slots = if args.len() > 4 {
                        std::str::from_utf8(&args[4]).ok().and_then(|s| s.parse().ok())
                    } else {
                        None
                    };
                    Ok(Some(Command::Cluster(ClusterSubcommand::Rebalance {
                        host,
                        port,
                        slots,
                    })))
                }
                "FAILOVER" => {
                    let force = args.len() > 2 && args[2].eq_ignore_ascii_case(b"force");
                    Ok(Some(Command::Cluster(ClusterSubcommand::Failover { force })))
                }
                "RESET" => {
                    let hard = args.len() > 2 && args[2].eq_ignore_ascii_case(b"hard");
                    Ok(Some(Command::Cluster(ClusterSubcommand::Reset { hard })))
                }
                "FORGET" => {
                    if args.len() < 3 {
                        return Err("wrong number of arguments for 'cluster forget' command".to_string());
                    }
                    let node_id = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Cluster(ClusterSubcommand::Forget(node_id))))
                }
                "REPLICATE" => {
                    if args.len() < 3 {
                        return Err("wrong number of arguments for 'cluster replicate' command".to_string());
                    }
                    let node_id = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Cluster(ClusterSubcommand::Replicate(node_id))))
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
                "LIST" => Ok(Some(Command::Client(ClientSubcommand::List))),
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
                "TRACKING" => {
                    if args.len() < 3 {
                        return Err("wrong number of arguments for 'client tracking' command".to_string());
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
                            "PREFIX" => {
                                if i + 1 < args.len() {
                                    prefixes.push(args[i + 1].clone());
                                    i += 2;
                                } else {
                                    i += 1;
                                }
                            }
                            _ => { i += 1; }
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
                        return Err("wrong number of arguments for 'client caching' command".to_string());
                    }
                    let state = String::from_utf8_lossy(&args[2]).to_uppercase();
                    let flag = state == "YES";
                    Ok(Some(Command::Client(ClientSubcommand::Caching(flag))))
                }
                _ => Ok(Some(Command::Unknown(format!("CLIENT {}", sub)))),
            }
        }

        "HSET" => {
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return Err("wrong number of arguments for 'hset' command".to_string());
            }
            let key = args[1].clone();
            let mut fields = Vec::with_capacity((args.len() - 2) / 2);
            let mut i = 2;
            while i < args.len() {
                fields.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            }
            Ok(Some(Command::Hset { key, fields }))
        }
        "HMSET" => {
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return Err("wrong number of arguments for 'hmset' command".to_string());
            }
            let key = args[1].clone();
            let mut fields = Vec::with_capacity((args.len() - 2) / 2);
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
        "LPUSH" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'lpush' command".to_string());
            }
            Ok(Some(Command::Lpush {
                key: args[1].clone(),
                values: args[2..].to_vec(),
            }))
        }
        "RPUSH" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'rpush' command".to_string());
            }
            Ok(Some(Command::Rpush {
                key: args[1].clone(),
                values: args[2..].to_vec(),
            }))
        }
        "LPOP" => {
            if args.len() < 2 {
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
            if args.len() < 2 {
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
            let timeout: f64 = std::str::from_utf8(args.last().unwrap())
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "timeout is not a float or out of range".to_string())?;
            let keys = args[1..args.len() - 1].to_vec();
            Ok(Some(Command::Blpop { keys, timeout }))
        }
        "BRPOP" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'brpop' command".to_string());
            }
            let timeout: f64 = std::str::from_utf8(args.last().unwrap())
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "timeout is not a float or out of range".to_string())?;
            let keys = args[1..args.len() - 1].to_vec();
            Ok(Some(Command::Brpop { keys, timeout }))
        }
        "SADD" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'sadd' command".to_string());
            }
            Ok(Some(Command::Sadd {
                key: args[1].clone(),
                members: args[2..].to_vec(),
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
            if remaining.is_empty() || remaining.len() % 2 != 0 {
                return Err("syntax error".to_string());
            }
            if flags.incr && remaining.len() != 2 {
                return Err("INCR option supports a single increment-element pair".to_string());
            }
            let mut elements = Vec::with_capacity(remaining.len() / 2);
            let mut j = 0;
            while j < remaining.len() {
                let score_str =
                    std::str::from_utf8(&remaining[j]).map_err(|_| "value is not a valid float")?;
                let score: f64 = score_str
                    .parse()
                    .map_err(|_| "value is not a valid float")?;
                if score.is_nan() {
                    return Err("value is not a valid float".to_string());
                }
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
            if args.len() != 3 {
                return Err("wrong number of arguments for 'zrank' command".to_string());
            }
            Ok(Some(Command::Zrank {
                key: args[1].clone(),
                member: args[2].clone(),
            }))
        }
        "ZREVRANK" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'zrevrank' command".to_string());
            }
            Ok(Some(Command::Zrevrank {
                key: args[1].clone(),
                member: args[2].clone(),
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
            let delta: f64 = delta_str
                .parse()
                .map_err(|_| "value is not a valid float")?;
            if delta.is_nan() {
                return Err("value is not a valid float".to_string());
            }
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
            let mut rev = false;
            let mut with_scores = false;
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
                    "REV" => {
                        rev = true;
                        i += 1;
                    }
                    "WITHSCORES" => {
                        with_scores = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        if i + 2 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let off_str = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?;
                        let cnt_str = std::str::from_utf8(&args[i + 2])
                            .map_err(|_| "value is not an integer or out of range")?;
                        offset = off_str
                            .parse::<usize>()
                            .map_err(|_| "value is not an integer or out of range")?;
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

            let (start, stop, min_score, min_inc, max_score, max_inc) = if by_score {
                let (min, min_i) = parse_score_bound(&args[2])?;
                let (max, max_i) = parse_score_bound(&args[3])?;
                (0, 0, min, min_i, max, max_i)
            } else {
                let start: i64 = std::str::from_utf8(&args[2])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                let stop: i64 = std::str::from_utf8(&args[3])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse()
                    .map_err(|_| "value is not an integer or out of range")?;
                (start, stop, 0.0, true, 0.0, true)
            };

            Ok(Some(Command::Zrange {
                key: args[1].clone(),
                opts: crate::table::ZRangeOpts {
                    start,
                    stop,
                    min_score,
                    min_inc,
                    max_score,
                    max_inc,
                    by_score,
                    rev,
                    with_scores,
                    offset,
                    count,
                },
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
                    min_score: 0.0,
                    min_inc: true,
                    max_score: 0.0,
                    max_inc: true,
                    by_score: false,
                    rev: true,
                    with_scores,
                    offset: 0,
                    count: None,
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
                        offset = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
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
                    start: 0,
                    stop: 0,
                    min_score: min,
                    min_inc,
                    max_score: max,
                    max_inc,
                    by_score: true,
                    rev: false,
                    with_scores,
                    offset,
                    count,
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
                        offset = std::str::from_utf8(&args[i + 1])
                            .map_err(|_| "value is not an integer or out of range")?
                            .parse()
                            .map_err(|_| "value is not an integer or out of range")?;
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
                    start: 0,
                    stop: 0,
                    min_score: min,
                    min_inc,
                    max_score: max,
                    max_inc,
                    by_score: true,
                    rev: true,
                    with_scores,
                    offset,
                    count,
                },
            }))
        }
        "ZPOPMIN" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'zpopmin' command".to_string());
            }
            let count = if args.len() > 2 {
                std::str::from_utf8(&args[2])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse::<usize>()
                    .map_err(|_| "value is not an integer or out of range")?
            } else {
                1
            };
            Ok(Some(Command::Zpopmin {
                key: args[1].clone(),
                count,
            }))
        }
        "ZPOPMAX" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'zpopmax' command".to_string());
            }
            let count = if args.len() > 2 {
                std::str::from_utf8(&args[2])
                    .map_err(|_| "value is not an integer or out of range")?
                    .parse::<usize>()
                    .map_err(|_| "value is not an integer or out of range")?
            } else {
                1
            };
            Ok(Some(Command::Zpopmax {
                key: args[1].clone(),
                count,
            }))
        }
        "ZUNIONSTORE" | "ZINTERSTORE" => {
            let is_union = cmd_name == "ZUNIONSTORE";
            if args.len() < 4 {
                return Err(format!("wrong number of arguments for '{}' command", cmd_name.to_lowercase()));
            }
            let destination = args[1].clone();
            let numkeys: usize = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys == 0 || args.len() < 3 + numkeys {
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
                            .and_then(|s| s.parse().ok())
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
                        _ => return Err("syntax error".to_string()),
                    };
                    i += 2;
                } else {
                    return Err("syntax error".to_string());
                }
            }
            if is_union {
                Ok(Some(Command::Zunionstore { destination, keys, weights, aggregate }))
            } else {
                Ok(Some(Command::Zinterstore { destination, keys, weights, aggregate }))
            }
        }
        "ZDIFFSTORE" => {
            if args.len() < 4 {
                return Err("wrong number of arguments for 'zdiffstore' command".to_string());
            }
            let destination = args[1].clone();
            let numkeys: usize = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys == 0 || args.len() != 3 + numkeys {
                return Err("syntax error".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            Ok(Some(Command::Zdiffstore { destination, keys }))
        }
        "ZDIFF" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'zdiff' command".to_string());
            }
            let numkeys: usize = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys == 0 || args.len() < 2 + numkeys {
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
                return Err(format!("wrong number of arguments for '{}' command", cmd_name.to_lowercase()));
            }
            let numkeys: usize = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            if numkeys == 0 || args.len() < 2 + numkeys {
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
                            .and_then(|s| s.parse().ok())
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
                Ok(Some(Command::Zunion { keys, weights, aggregate, with_scores }))
            } else {
                Ok(Some(Command::Zinter { keys, weights, aggregate, with_scores }))
            }
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
            Ok(Some(Command::Touch(args[1..].to_vec())))
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
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
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
            let offset = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
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
        "EVAL" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'eval' command".to_string());
            }
            let script = args[1].clone();
            let numkeys: usize = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<usize>().ok()) {
                Some(n) => n,
                None => return Err("ERR value is not an integer or out of range".to_string()),
            };
            if 3 + numkeys > args.len() {
                return Err("ERR Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let script_args = args[3 + numkeys..].to_vec();
            Ok(Some(Command::Eval { script, keys, args: script_args }))
        }
        "EVALSHA" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'evalsha' command".to_string());
            }
            let sha = args[1].clone();
            let numkeys: usize = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<usize>().ok()) {
                Some(n) => n,
                None => return Err("ERR value is not an integer or out of range".to_string()),
            };
            if 3 + numkeys > args.len() {
                return Err("ERR Number of keys can't be greater than number of args".to_string());
            }
            let keys = args[3..3 + numkeys].to_vec();
            let script_args = args[3 + numkeys..].to_vec();
            Ok(Some(Command::Evalsha { sha, keys, args: script_args }))
        }
        "SCRIPT" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'script' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "LOAD" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'script|load' command".to_string());
                    }
                    Ok(Some(Command::ScriptLoad(args[2].clone())))
                }
                "EXISTS" => {
                    if args.len() < 3 {
                        return Err("wrong number of arguments for 'script|exists' command".to_string());
                    }
                    Ok(Some(Command::ScriptExists(args[2..].to_vec())))
                }
                "FLUSH" => {
                    Ok(Some(Command::ScriptFlush))
                }
                _ => Err(format!("ERR Unknown SCRIPT subcommand or wrong number of arguments for '{}'", sub)),
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
                        return Err("wrong number of arguments for 'tier spill' command".to_string());
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
                        Ok(Some(Command::Tier(TierSubcommand::Decommit(Some(args[2].clone())))))
                    } else {
                        Err("wrong number of arguments for 'tier decommit' command".to_string())
                    }
                }
                "INFO" => Ok(Some(Command::Tier(TierSubcommand::Info))),
                "SPILLALL" => Ok(Some(Command::Tier(TierSubcommand::SpillAll))),
                "GC" => Ok(Some(Command::Tier(TierSubcommand::Gc))),
                "SNAPSHOT" | "BACKUP" => {
                    if args.len() < 3 {
                        return Err("wrong number of arguments for 'tier snapshot' command".to_string());
                    }
                    let dir = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::Tier(TierSubcommand::Snapshot(std::path::PathBuf::from(dir)))))
                }
                _ => Err(format!(
                    "ERR unknown subcommand '{}'. Try TIER SPILL, TIER COOL, TIER DECOMMIT, TIER LOAD, TIER INFO, TIER SPILLALL, TIER GC, TIER SNAPSHOT.",
                    sub
                )),

            }
        }
        "CONFIG" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'config' command".to_string());
            }
            let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
            match sub.as_str() {
                "GET" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'config get' command".to_string());
                    }
                    Ok(Some(Command::ConfigGet(args[2].clone())))
                }
                "SET" => {
                    if args.len() != 4 {
                        return Err("wrong number of arguments for 'config set' command".to_string());
                    }
                    Ok(Some(Command::ConfigSet(args[2].clone(), args[3].clone())))
                }
                _ => Err(format!("ERR unknown subcommand '{}' for CONFIG", sub)),
            }
        }
        "QUIT" => Ok(Some(Command::Quit)),
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
        "SETBIT" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'setbit' command".to_string());
            }
            let offset: usize = std::str::from_utf8(&args[2])
                .map_err(|_| "bit offset is not an integer or out of range")?
                .parse()
                .map_err(|_| "bit offset is not an integer or out of range")?;
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
                        if i < args.len() && (args[i].as_ref() == b"=" || args[i].as_ref() == b"~") {
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
                        if i < args.len() && (args[i].as_ref() == b"=" || args[i].as_ref() == b"~") {
                            i += 1;
                        }
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let id_str = std::str::from_utf8(&args[i])
                            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
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
            let id = crate::table::StreamAddId::parse(id_str)
                .map_err(|e| e.to_string())?;
            i += 1;
            let rem = args.len() - i;
            if rem == 0 || rem % 2 != 0 {
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
            if rem < 2 || rem % 2 != 0 {
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
                let id = crate::table::StreamId::parse_exact(s)
                    .map_err(|e| e.to_string())?;
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
                        if i < args.len() && (args[i].as_ref() == b"=" || args[i].as_ref() == b"~") {
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
                        if i < args.len() && (args[i].as_ref() == b"=" || args[i].as_ref() == b"~") {
                            i += 1;
                        }
                        if i >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        let id_str = std::str::from_utf8(&args[i])
                            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
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
            Ok(Some(Command::Xtrim {
                key,
                maxlen,
                minid,
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
                        return Err("wrong number of arguments for 'xgroup create' command".to_string());
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
                    Ok(Some(Command::XgroupCreate { key, group, id, mkstream }))
                }
                "DESTROY" => {
                    if args.len() < 4 {
                        return Err("wrong number of arguments for 'xgroup destroy' command".to_string());
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    Ok(Some(Command::XgroupDestroy { key, group }))
                }
                "CREATECONSUMER" => {
                    if args.len() < 5 {
                        return Err("wrong number of arguments for 'xgroup createconsumer' command".to_string());
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    let consumer = args[4].clone();
                    Ok(Some(Command::XgroupCreateConsumer { key, group, consumer }))
                }
                "DELCONSUMER" => {
                    if args.len() < 5 {
                        return Err("wrong number of arguments for 'xgroup delconsumer' command".to_string());
                    }
                    let key = args[2].clone();
                    let group = args[3].clone();
                    let consumer = args[4].clone();
                    Ok(Some(Command::XgroupDelConsumer { key, group, consumer }))
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
            if rem < 2 || rem % 2 != 0 {
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
                let id = crate::table::StreamId::parse_exact(s)
                    .map_err(|e| e.to_string())?;
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
                Ok(Some(Command::Xpending { key, group, range: None }))
            } else {
                let mut start_idx = 3;
                if args[start_idx].eq_ignore_ascii_case(b"IDLE") {
                    start_idx += 2;
                }
                if args.len() < start_idx + 3 {
                    return Err("syntax error".to_string());
                }
                let start_s = std::str::from_utf8(&args[start_idx]).map_err(|_| "syntax error")?;
                let start = if start_s == "-" { crate::table::StreamId::default() } else { crate::table::StreamId::parse(start_s)? };
                let end_s = std::str::from_utf8(&args[start_idx + 1]).map_err(|_| "syntax error")?;
                let end = if end_s == "+" { crate::table::StreamId::new(u64::MAX, u64::MAX) } else { crate::table::StreamId::parse(end_s)? };
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
                if let Ok(p) = String::from_utf8_lossy(&args[i]).parse::<u8>() {
                    proto = Some(p);
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
            Ok(Some(Command::Hello { proto, auth, setname }))
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
            let increment: f64 = std::str::from_utf8(&args[3])
                .map_err(|_| "value is not a valid float".to_string())?
                .parse()
                .map_err(|_| "value is not a valid float".to_string())?;
            if increment.is_nan() || increment.is_infinite() {
                return Err("value is not a valid float".to_string());
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
                Some(c)
            } else {
                None
            };
            let mut with_values = false;
            if args.len() == 4 {
                if String::from_utf8_lossy(&args[3]).eq_ignore_ascii_case("WITHVALUES") {
                    with_values = true;
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
                Some(c)
            } else {
                None
            };
            let mut with_scores = false;
            if args.len() == 4 {
                if String::from_utf8_lossy(&args[3]).eq_ignore_ascii_case("WITHSCORES") {
                    with_scores = true;
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
                        if r == 0 {
                            return Err("RANK can't be zero: use 1 to start from the first match, use -1 from the last".to_string());
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
            if timeout < 0.0 {
                return Err("timeout is negative".to_string());
            }
            Ok(Some(Command::Blmove {
                source: args[1].clone(),
                destination: args[2].clone(),
                where_from,
                where_to,
                timeout,
            }))
        }
        "INCRBYFLOAT" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'incrbyfloat' command".to_string());
            }
            let increment: f64 = std::str::from_utf8(&args[2])
                .map_err(|_| "value is not a valid float".to_string())?
                .parse()
                .map_err(|_| "value is not a valid float".to_string())?;
            if increment.is_nan() || increment.is_infinite() {
                return Err("value is not a valid float".to_string());
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
        "GETRANGE" => {
            if args.len() != 4 {
                return Err("wrong number of arguments for 'getrange' command".to_string());
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
            let mut tiered = false;
            for a in &args[3..] {
                let s = String::from_utf8_lossy(a).to_uppercase();
                if s == "QUANTIZE" || s == "SQ8" {
                    quantize = true;
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
            Ok(Some(Command::Vquery { index, k, query, rerank }))
        }
        "CRDT.SET" => {
            if args.len() != 3 {
                return Err("wrong number of arguments for 'crdt.set' command".to_string());
            }
            Ok(Some(Command::CrdtSet { key: args[1].clone(), val: args[2].clone() }))
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
            Ok(Some(Command::CrdtIncrby { key: args[1].clone(), delta }))
        }
        "CRDT.SADD" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'crdt.sadd' command".to_string());
            }
            Ok(Some(Command::CrdtSadd { key: args[1].clone(), member: args[2].clone() }))
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
            Ok(Some(Command::CrdtSrem { key: args[1].clone(), member: args[2].clone() }))
        }
        "CRDT.DUMP" => {
            Ok(Some(Command::CrdtDump))
        }
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
                crate::vector::VectorMetric::from_str(&s)
            } else {
                None
            };
            Ok(Some(Command::Vsim { index, k1, k2, metric }))
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
                        return Err("wrong number of arguments for 'function load' command".to_string());
                    }
                    let mut replace = false;
                    let mut code_idx = 2;
                    if args.len() >= 4 && String::from_utf8_lossy(&args[2]).to_uppercase() == "REPLACE" {
                        replace = true;
                        code_idx = 3;
                    }
                    let code = args[code_idx].clone();
                    Ok(Some(Command::FunctionLoad { replace, code }))
                }
                "LIST" => Ok(Some(Command::FunctionList)),
                "DELETE" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'function delete' command".to_string());
                    }
                    let lib = String::from_utf8_lossy(&args[2]).to_string();
                    Ok(Some(Command::FunctionDelete(lib)))
                }
                _ => Ok(Some(Command::Unknown(format!("FUNCTION {}", sub)))),
            }
        }
        "FCALL" => {
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
        _ => Ok(Some(Command::Unknown(cmd_name))),

    }
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
        let val: f64 = s
            .parse()
            .map_err(|_| "value is not a valid float".to_string())?;
        if val.is_nan() {
            return Err("value is not a valid float".to_string());
        }
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
                fields: vec![
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
        let mut buf = BytesMut::from("*4\r\n$7\r\nRESTORE\r\n$5\r\nmykey\r\n$1\r\n0\r\n$4\r\ndata\r\n");
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
        let mut buf = BytesMut::from("*6\r\n$7\r\nRESTORE\r\n$5\r\nmykey\r\n$4\r\n1000\r\n$4\r\ndata\r\n$7\r\nREPLACE\r\n$6\r\nABSTTL\r\n");
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
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Echo(Bytes::from_static(b"hi")));

        // HASHES: HINCRBY, HINCRBYFLOAT, HRANDFIELD, HSCAN
        let mut buf = BytesMut::from("HINCRBY h f 5\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Hincrby { key: Bytes::from_static(b"h"), field: Bytes::from_static(b"f"), increment: 5 });
        let mut buf = BytesMut::from("HINCRBYFLOAT h f 2.5\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Hincrbyfloat { key: Bytes::from_static(b"h"), field: Bytes::from_static(b"f"), increment: 2.5 });
        let mut buf = BytesMut::from("HRANDFIELD h 3 WITHVALUES\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Hrandfield { key: Bytes::from_static(b"h"), count: Some(3), with_values: true });
        let mut buf = BytesMut::from("HSCAN h 0 MATCH pat* COUNT 20\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Hscan { key: Bytes::from_static(b"h"), cursor: 0, pattern: Some(Bytes::from_static(b"pat*")), count: Some(20) });

        // SETS: SMISMEMBER, SRANDMEMBER, SMOVE, SSCAN
        let mut buf = BytesMut::from("SMISMEMBER s m1 m2\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Smismember { key: Bytes::from_static(b"s"), members: vec![Bytes::from_static(b"m1"), Bytes::from_static(b"m2")] });
        let mut buf = BytesMut::from("SRANDMEMBER s 2\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Srandmember { key: Bytes::from_static(b"s"), count: Some(2) });
        let mut buf = BytesMut::from("SMOVE s1 s2 m\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Smove { source: Bytes::from_static(b"s1"), destination: Bytes::from_static(b"s2"), member: Bytes::from_static(b"m") });
        let mut buf = BytesMut::from("SSCAN s 0\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Sscan { key: Bytes::from_static(b"s"), cursor: 0, pattern: None, count: None });

        // ZSETS: ZMSCORE, ZRANDMEMBER, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZLEXCOUNT, ZSCAN
        let mut buf = BytesMut::from("ZMSCORE z m1 m2\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Zmscore { key: Bytes::from_static(b"z"), members: vec![Bytes::from_static(b"m1"), Bytes::from_static(b"m2")] });
        let mut buf = BytesMut::from("ZRANDMEMBER z 2 WITHSCORES\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Zrandmember { key: Bytes::from_static(b"z"), count: Some(2), with_scores: true });
        let mut buf = BytesMut::from("ZREMRANGEBYRANK z 0 2\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Zremrangebyrank { key: Bytes::from_static(b"z"), start: 0, stop: 2 });
        let mut buf = BytesMut::from("ZREMRANGEBYSCORE z (1.5 5.0\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Zremrangebyscore { key: Bytes::from_static(b"z"), min_score: 1.5, min_inc: false, max_score: 5.0, max_inc: true });
        let mut buf = BytesMut::from("ZREMRANGEBYLEX z [a (c\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Zremrangebylex { key: Bytes::from_static(b"z"), min: crate::table::LexBound::Inclusive(Bytes::from_static(b"a")), max: crate::table::LexBound::Exclusive(Bytes::from_static(b"c")) });
        let mut buf = BytesMut::from("ZLEXCOUNT z - +\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Zlexcount { key: Bytes::from_static(b"z"), min: crate::table::LexBound::UnboundedMin, max: crate::table::LexBound::UnboundedMax });
        let mut buf = BytesMut::from("ZSCAN z 0\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Zscan { key: Bytes::from_static(b"z"), cursor: 0, pattern: None, count: None });

        // LISTS: LTRIM, LSET, LREM, LPOS, LINSERT, LMOVE, BLMOVE
        let mut buf = BytesMut::from("LTRIM l 1 2\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Ltrim { key: Bytes::from_static(b"l"), start: 1, stop: 2 });
        let mut buf = BytesMut::from("LSET l 0 val\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Lset { key: Bytes::from_static(b"l"), index: 0, element: Bytes::from_static(b"val") });
        let mut buf = BytesMut::from("LREM l 2 val\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Lrem { key: Bytes::from_static(b"l"), count: 2, element: Bytes::from_static(b"val") });
        let mut buf = BytesMut::from("LPOS l val RANK 2 COUNT 3 MAXLEN 100\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Lpos { key: Bytes::from_static(b"l"), element: Bytes::from_static(b"val"), rank: Some(2), count: Some(3), maxlen: Some(100) });
        let mut buf = BytesMut::from("LINSERT l AFTER p e\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Linsert { key: Bytes::from_static(b"l"), before: false, pivot: Bytes::from_static(b"p"), element: Bytes::from_static(b"e") });
        let mut buf = BytesMut::from("LMOVE l1 l2 LEFT RIGHT\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Lmove { source: Bytes::from_static(b"l1"), destination: Bytes::from_static(b"l2"), where_from: crate::table::ListDirection::Left, where_to: crate::table::ListDirection::Right });
        let mut buf = BytesMut::from("BLMOVE l1 l2 RIGHT LEFT 1.5\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Blmove { source: Bytes::from_static(b"l1"), destination: Bytes::from_static(b"l2"), where_from: crate::table::ListDirection::Right, where_to: crate::table::ListDirection::Left, timeout: 1.5 });

        // STRINGS: INCRBYFLOAT, SETRANGE, GETRANGE
        let mut buf = BytesMut::from("INCRBYFLOAT num 1.25\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Incrbyfloat { key: Bytes::from_static(b"num"), increment: 1.25 });
        let mut buf = BytesMut::from("SETRANGE k 2 world\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Setrange { key: Bytes::from_static(b"k"), offset: 2, value: Bytes::from_static(b"world") });
        let mut buf = BytesMut::from("GETRANGE k 0 -1\r\n");
        assert_eq!(parse_command(&mut buf).unwrap().unwrap(), Command::Getrange { key: Bytes::from_static(b"k"), start: 0, end: -1 });
    }
}
