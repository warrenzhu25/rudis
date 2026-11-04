//! RESP3 reply reshaping for commands whose replies are assembled in RESP2
//! across several paths (local, cross-shard merges, blocking wakeups), so
//! the internals keep parsing RESP2 and a RESP3 client still gets the
//! types Redis sends: maps for XREAD/XREADGROUP and XINFO,
//! verbatim strings for INFO, CLIENT INFO/LIST, LOLWUT and the doctors.

/// How a command's RESP2 reply becomes RESP3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reshape {
    /// `*N` of `*2 [key, value]` -> `%N key value ...` (XREAD, XREADGROUP).
    PairsToMap,
    /// A flat `*2N` array -> `%N` (XINFO STREAM).
    FlatToMap,
    /// `*M` of flat `*2N` arrays -> `*M` of `%N` (XINFO GROUPS/CONSUMERS).
    ArrayOfFlatToMaps,
    /// `$N text` -> `=N+4 txt:text`.
    Verbatim,
    /// A nested layout of maps (see [`Shape`]).
    Nested(&'static Shape),
}

/// How the RESP2 value at some place in a reply becomes RESP3.
#[derive(Debug, PartialEq, Eq)]
pub enum Shape {
    /// Unchanged.
    Keep,
    /// A flat `*2N` array -> `%N`, each value reshaped by the first entry
    /// whose key matches (`*` suffix: a prefix match), else kept.
    Map(&'static [(&'static str, Shape)]),
    /// An array whose elements are all reshaped alike.
    ArrayOf(&'static Shape),
    /// A bulk string holding a number -> a double.
    Double,
    /// An array -> a set.
    Set,
}

static FUNCTION_LIST: Shape = Shape::ArrayOf(&Shape::Map(&[(
    "functions",
    Shape::ArrayOf(&Shape::Map(&[("flags", Shape::Set)])),
)]));
static MEMORY_STATS: Shape = Shape::Map(&[
    ("db.*", Shape::Map(&[])),
    ("*.ratio", Shape::Double),
    ("*percentage", Shape::Double),
    ("fragmentation", Shape::Double),
    ("allocator-fragmentation.ratio", Shape::Double),
]);
static LATENCY_HISTOGRAM: Shape =
    Shape::Map(&[("*", Shape::Map(&[("histogram_usec", Shape::Map(&[]))]))]);
static XINFO_FULL: Shape = Shape::Map(&[(
    "groups",
    Shape::ArrayOf(&Shape::Map(&[(
        "consumers",
        Shape::ArrayOf(&Shape::Map(&[])),
    )])),
)]);

/// The reshape a RESP3 client needs for `cmd`, if any.
pub fn reshape_for(cmd: &crate::resp::Command) -> Option<Reshape> {
    use crate::resp::{
        ClientSubcommand, Command, LatencySubcommand, MemorySubcommand, XinfoSubcommand,
    };
    match cmd {
        Command::Xread { .. } | Command::Xreadgroup { .. } => Some(Reshape::PairsToMap),
        Command::Xinfo(XinfoSubcommand::Stream(_)) => Some(Reshape::FlatToMap),
        Command::Xinfo(XinfoSubcommand::StreamFull { .. }) => Some(Reshape::Nested(&XINFO_FULL)),
        Command::FunctionList { .. } => Some(Reshape::Nested(&FUNCTION_LIST)),
        Command::Memory(MemorySubcommand::Stats) => Some(Reshape::Nested(&MEMORY_STATS)),
        Command::Latency(LatencySubcommand::Histogram(_)) => {
            Some(Reshape::Nested(&LATENCY_HISTOGRAM))
        }
        Command::Xinfo(XinfoSubcommand::Groups(_) | XinfoSubcommand::Consumers { .. }) => {
            Some(Reshape::ArrayOfFlatToMaps)
        }
        Command::Info(_)
        | Command::Client(ClientSubcommand::Info | ClientSubcommand::List(_))
        | Command::Lolwut(_)
        | Command::Memory(MemorySubcommand::Doctor)
        | Command::Latency(LatencySubcommand::Doctor) => Some(Reshape::Verbatim),
        _ => None,
    }
}

/// Reshapes the reply in `out[start..]` in place. Replies of another form
/// (errors, nulls, already RESP3) are left alone.
pub fn apply(how: Reshape, out: &mut Vec<u8>, start: usize) {
    let reply = &out[start..];
    let new = match how {
        Reshape::PairsToMap => pairs_to_map(reply),
        Reshape::FlatToMap => flat_to_map(reply),
        Reshape::ArrayOfFlatToMaps => array_of_flat_to_maps(reply),
        Reshape::Verbatim => verbatim(reply),
        Reshape::Nested(shape) => {
            let mut new = Vec::with_capacity(reply.len());
            match nested(shape, reply, 0, &mut new) {
                Some(end) if end == reply.len() => Some(new),
                _ => None,
            }
        }
    };
    if let Some(new) = new {
        out.truncate(start);
        out.extend_from_slice(&new);
    }
}

/// `(count, offset after the header line)` of an aggregate header `t<n>\r\n`.
fn header(buf: &[u8], pos: usize, t: u8) -> Option<(usize, usize)> {
    if buf.get(pos) != Some(&t) {
        return None;
    }
    let nl = pos + buf[pos..].windows(2).position(|w| w == b"\r\n")?;
    let n = std::str::from_utf8(&buf[pos + 1..nl]).ok()?.parse().ok()?;
    Some((n, nl + 2))
}

/// The offset just past the RESP value starting at `pos`.
pub fn value_end(buf: &[u8], pos: usize) -> Option<usize> {
    let t = *buf.get(pos)?;
    let nl = pos + buf[pos..].windows(2).position(|w| w == b"\r\n")?;
    let after = nl + 2;
    let num = || -> Option<i64> { std::str::from_utf8(&buf[pos + 1..nl]).ok()?.parse().ok() };
    match t {
        b'+' | b'-' | b':' | b',' | b'#' | b'(' | b'_' => Some(after),
        b'$' | b'=' | b'!' => {
            let n = num()?;
            if n < 0 {
                Some(after)
            } else {
                let end = after + n as usize + 2;
                (end <= buf.len()).then_some(end)
            }
        }
        b'*' | b'~' | b'>' | b'%' | b'|' => {
            let n = num()?;
            if n < 0 {
                return Some(after);
            }
            let items = if t == b'%' || t == b'|' { 2 * n } else { n };
            let mut p = after;
            for _ in 0..items {
                p = value_end(buf, p)?;
            }
            if t == b'|' {
                // An attribute precedes the value it describes.
                p = value_end(buf, p)?;
            }
            Some(p)
        }
        _ => None,
    }
}

fn key_matches(pattern: &str, key: &[u8]) -> bool {
    if pattern == "*" {
        true
    } else if let Some(prefix) = pattern.strip_suffix('*') {
        key.starts_with(prefix.as_bytes())
    } else if let Some(suffix) = pattern.strip_prefix('*') {
        key.ends_with(suffix.as_bytes())
    } else {
        key == pattern.as_bytes()
    }
}

/// The payload of the bulk string at `pos`, if it is one.
fn bulk_at(buf: &[u8], pos: usize) -> Option<&[u8]> {
    let (n, p) = header(buf, pos, b'$')?;
    buf.get(p..p + n)
}

/// Writes the value at `pos` reshaped by `shape` to `out`; returns the
/// offset after it, or None if it doesn't have the expected form.
fn nested(shape: &Shape, buf: &[u8], pos: usize, out: &mut Vec<u8>) -> Option<usize> {
    let end = value_end(buf, pos)?;
    match shape {
        Shape::Keep => out.extend_from_slice(&buf[pos..end]),
        Shape::Double => match bulk_at(buf, pos) {
            Some(text) if std::str::from_utf8(text).ok()?.parse::<f64>().is_ok() => {
                out.push(b',');
                out.extend_from_slice(text);
                out.extend_from_slice(b"\r\n");
            }
            _ => out.extend_from_slice(&buf[pos..end]),
        },
        Shape::Set => match header(buf, pos, b'*') {
            Some((n, p)) => {
                out.extend_from_slice(format!("~{n}\r\n").as_bytes());
                out.extend_from_slice(&buf[p..end]);
            }
            None => out.extend_from_slice(&buf[pos..end]),
        },
        Shape::ArrayOf(inner) => {
            let Some((n, mut p)) = header(buf, pos, b'*') else {
                out.extend_from_slice(&buf[pos..end]);
                return Some(end);
            };
            out.extend_from_slice(format!("*{n}\r\n").as_bytes());
            for _ in 0..n {
                p = nested(inner, buf, p, out)?;
            }
        }
        Shape::Map(fields) => {
            let Some((n, mut p)) = header(buf, pos, b'*') else {
                out.extend_from_slice(&buf[pos..end]);
                return Some(end);
            };
            if n % 2 != 0 {
                return None;
            }
            out.extend_from_slice(format!("%{}\r\n", n / 2).as_bytes());
            for _ in 0..n / 2 {
                let key_end = value_end(buf, p)?;
                let key = bulk_at(buf, p).unwrap_or(&[]);
                let value_shape = fields
                    .iter()
                    .find(|(pat, _)| key_matches(pat, key))
                    .map_or(&Shape::Keep, |(_, s)| s);
                out.extend_from_slice(&buf[p..key_end]);
                p = nested(value_shape, buf, key_end, out)?;
            }
        }
    }
    Some(end)
}

fn pairs_to_map(reply: &[u8]) -> Option<Vec<u8>> {
    let (n, mut p) = header(reply, 0, b'*')?;
    let mut out = format!("%{n}\r\n").into_bytes();
    for _ in 0..n {
        let (two, inner) = header(reply, p, b'*')?;
        if two != 2 {
            return None;
        }
        let end = value_end(reply, p)?;
        out.extend_from_slice(&reply[inner..end]);
        p = end;
    }
    (p == reply.len()).then_some(out)
}

fn flat_to_map(reply: &[u8]) -> Option<Vec<u8>> {
    let (n, p) = header(reply, 0, b'*')?;
    if n % 2 != 0 || value_end(reply, 0)? != reply.len() {
        return None;
    }
    let mut out = format!("%{}\r\n", n / 2).into_bytes();
    out.extend_from_slice(&reply[p..]);
    Some(out)
}

fn array_of_flat_to_maps(reply: &[u8]) -> Option<Vec<u8>> {
    let (m, mut p) = header(reply, 0, b'*')?;
    let mut out = format!("*{m}\r\n").into_bytes();
    for _ in 0..m {
        let end = value_end(reply, p)?;
        out.extend_from_slice(&flat_to_map(&reply[p..end])?);
        p = end;
    }
    (p == reply.len()).then_some(out)
}

fn verbatim(reply: &[u8]) -> Option<Vec<u8>> {
    if let Some(text) = reply
        .strip_prefix(b"+")
        .and_then(|r| r.strip_suffix(b"\r\n"))
    {
        let mut out = format!("={}\r\ntxt:", text.len() + 4).into_bytes();
        out.extend_from_slice(text);
        out.extend_from_slice(b"\r\n");
        return Some(out);
    }
    let (n, p) = header(reply, 0, b'$')?;
    if p + n + 2 != reply.len() {
        return None;
    }
    let mut out = format!("={}\r\ntxt:", n + 4).into_bytes();
    out.extend_from_slice(&reply[p..]);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reshaped(how: Reshape, reply: &[u8]) -> Vec<u8> {
        let mut out = b"prefix".to_vec();
        out.extend_from_slice(reply);
        apply(how, &mut out, 6);
        assert!(out.starts_with(b"prefix"));
        out[6..].to_vec()
    }

    #[test]
    fn test_xread_pairs_become_a_map() {
        let resp2 = b"*2\r\n*2\r\n$1\r\na\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n\
*2\r\n$1\r\nb\r\n*0\r\n";
        assert_eq!(
            reshaped(Reshape::PairsToMap, resp2),
            b"%2\r\n$1\r\na\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n\
$1\r\nb\r\n*0\r\n"
        );
        // Nulls and errors are left alone.
        assert_eq!(reshaped(Reshape::PairsToMap, b"_\r\n"), b"_\r\n");
        assert_eq!(reshaped(Reshape::PairsToMap, b"-ERR x\r\n"), b"-ERR x\r\n");
    }

    #[test]
    fn test_flat_arrays_become_maps() {
        let flat = b"*4\r\n$4\r\nname\r\n$1\r\ng\r\n$7\r\npending\r\n:0\r\n";
        assert_eq!(
            reshaped(Reshape::FlatToMap, flat),
            b"%2\r\n$4\r\nname\r\n$1\r\ng\r\n$7\r\npending\r\n:0\r\n"
        );
        let many = b"*2\r\n*2\r\n$1\r\na\r\n:1\r\n*2\r\n$1\r\nb\r\n:2\r\n";
        assert_eq!(
            reshaped(Reshape::ArrayOfFlatToMaps, many),
            b"*2\r\n%1\r\n$1\r\na\r\n:1\r\n%1\r\n$1\r\nb\r\n:2\r\n"
        );
        // An odd count is not a map: unchanged.
        assert_eq!(
            reshaped(Reshape::FlatToMap, b"*1\r\n:1\r\n"),
            b"*1\r\n:1\r\n"
        );
    }

    #[test]
    fn test_bulk_text_becomes_verbatim() {
        assert_eq!(
            reshaped(Reshape::Verbatim, b"$5\r\nhello\r\n"),
            b"=9\r\ntxt:hello\r\n"
        );
        assert_eq!(reshaped(Reshape::Verbatim, b"+ok\r\n"), b"=6\r\ntxt:ok\r\n");
    }

    #[test]
    fn test_nested_shapes_reshape_inner_maps_sets_and_doubles() {
        static S: Shape = Shape::ArrayOf(&Shape::Map(&[
            ("flags", Shape::Set),
            ("ratio", Shape::Double),
            ("sub*", Shape::Map(&[])),
        ]));
        let resp2 = b"*1\r\n*8\r\n$5\r\nflags\r\n*1\r\n$2\r\nro\r\n$5\r\nratio\r\n$3\r\n1.5\r\n\
$4\r\nsub1\r\n*2\r\n$1\r\na\r\n:1\r\n$5\r\nother\r\n*0\r\n";
        assert_eq!(
            reshaped(Reshape::Nested(&S), resp2),
            b"*1\r\n%4\r\n$5\r\nflags\r\n~1\r\n$2\r\nro\r\n$5\r\nratio\r\n,1.5\r\n\
$4\r\nsub1\r\n%1\r\n$1\r\na\r\n:1\r\n$5\r\nother\r\n*0\r\n"
        );
    }

    #[test]
    fn test_value_end_walks_nested_resp3() {
        let v = b"%1\r\n$1\r\nk\r\n~2\r\n,1.5\r\n_\r\nTAIL";
        assert_eq!(value_end(v, 0), Some(v.len() - 4));
        assert_eq!(value_end(b"$3\r\nab", 0), None);
    }
}
