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
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ClientSubcommand {
    List,
    SetName(String),
    GetName,
    Id,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
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
    Info,
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

fn build_command(args: Vec<Bytes>) -> Result<Option<Command>, String> {
    if args.is_empty() {
        return Ok(None);
    }

    let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();

    match cmd_name.as_str() {
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
        "INFO" => Ok(Some(Command::Info)),
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
}
