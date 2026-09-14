use std::time::Duration;
use bytes::{Buf, Bytes, BytesMut};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ClusterSubcommand {
    KeySlot(Bytes),
    CountKeysInSlot(u16),
    GetKeysInSlot(u16, usize),
    Slots,
    Nodes,
    Info,
}

#[derive(Debug, PartialEq, Eq, Clone)]
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
    Ping(Option<Bytes>),
    CommandDocs,
    Info,
    Quit,
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
            Ok(Some(Command::Expire(args[1].clone(), Duration::from_secs(secs))))
        }
        "PEXPIRE" => {
            if args.len() < 3 {
                return Err("wrong number of arguments for 'pexpire' command".to_string());
            }
            let ms: u64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "value is not an integer or out of range".to_string())?;
            Ok(Some(Command::Expire(args[1].clone(), Duration::from_millis(ms))))
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
                        return Err("wrong number of arguments for 'cluster keyslot' command".to_string());
                    }
                    Ok(Some(Command::Cluster(ClusterSubcommand::KeySlot(args[2].clone()))))
                }
                "COUNTKEYSINSLOT" => {
                    if args.len() < 3 {
                        return Err("wrong number of arguments for 'cluster countkeysinslot' command".to_string());
                    }
                    let slot: u16 = std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Cluster(ClusterSubcommand::CountKeysInSlot(slot))))
                }
                "GETKEYSINSLOT" => {
                    if args.len() < 4 {
                        return Err("wrong number of arguments for 'cluster getkeysinslot' command".to_string());
                    }
                    let slot: u16 = std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    let count: usize = std::str::from_utf8(&args[3])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| "value is not an integer or out of range".to_string())?;
                    Ok(Some(Command::Cluster(ClusterSubcommand::GetKeysInSlot(slot, count))))
                }
                "SLOTS" => Ok(Some(Command::Cluster(ClusterSubcommand::Slots))),
                "NODES" => Ok(Some(Command::Cluster(ClusterSubcommand::Nodes))),
                "INFO" => Ok(Some(Command::Cluster(ClusterSubcommand::Info))),
                _ => Ok(Some(Command::Unknown(format!("CLUSTER {}", sub)))),
            }
        }
        "COMMAND" => Ok(Some(Command::CommandDocs)),
        "INFO" => Ok(Some(Command::Info)),
        "QUIT" => Ok(Some(Command::Quit)),
        _ => Ok(Some(Command::Unknown(cmd_name))),
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
        let mut buf = BytesMut::from("*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n");
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
        assert_eq!(cmd, Command::Expire(Bytes::from_static(b"foo"), Duration::from_secs(60)));

        let mut buf = BytesMut::from("TTL foo\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Ttl(Bytes::from_static(b"foo"), false));
    }
}
