use bytes::{Buf, BytesMut};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Command {
    Get(Vec<u8>),
    Set(Vec<u8>, Vec<u8>),
    Ping(Option<Vec<u8>>),
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

    let mut cursor = newline_pos + 2;
    let mut args = Vec::with_capacity(num_args);

    for _ in 0..num_args {
        if cursor >= buf.len() {
            return Ok(None);
        }

        if buf[cursor] != b'$' {
            return Err("Expected bulk string in command array".to_string());
        }

        let next_crlf = match find_crlf_at(buf, cursor) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let len_str = &buf[cursor + 1..next_crlf];
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

        args.push(buf[data_start..data_end].to_vec());
        cursor = data_end + 2;
    }

    buf.advance(cursor);
    build_command(args)
}

fn parse_inline_command(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    let newline_pos = match find_crlf(buf) {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let line = &buf[..newline_pos];
    let parts: Vec<Vec<u8>> = line
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|part| !part.is_empty())
        .map(|part| part.to_vec())
        .collect();

    buf.advance(newline_pos + 2);

    if parts.is_empty() {
        return Ok(None);
    }

    build_command(parts)
}

fn build_command(args: Vec<Vec<u8>>) -> Result<Option<Command>, String> {
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
            Ok(Some(Command::Set(args[1].clone(), args[2].clone())))
        }
        "PING" => {
            let msg = if args.len() > 1 {
                Some(args[1].clone())
            } else {
                None
            };
            Ok(Some(Command::Ping(msg)))
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
        assert_eq!(cmd, Command::Get(b"mykey".to_vec()));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_resp_set_and_put() {
        let mut buf = BytesMut::from("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Set(b"mykey".to_vec(), b"myvalue".to_vec())
        );
        assert!(buf.is_empty());

        let mut buf = BytesMut::from("*3\r\n$3\r\nPUT\r\n$1\r\nk\r\n$1\r\nv\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Set(b"k".to_vec(), b"v".to_vec()));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_inline_commands() {
        let mut buf = BytesMut::from("GET foo\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Get(b"foo".to_vec()));

        let mut buf = BytesMut::from("SET foo bar\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Set(b"foo".to_vec(), b"bar".to_vec()));

        let mut buf = BytesMut::from("PUT hello world\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Set(b"hello".to_vec(), b"world".to_vec()));

        let mut buf = BytesMut::from("PING\r\n");
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Ping(None));
    }
}
