//! Redis/Valkey RDB and DUMP payload codec.
//!
//! Reads RDB files and DUMP payloads written by Redis (RDB versions 1 to 12)
//! and Valkey, and writes RDB version 11 files and DUMP payloads that Redis
//! 7.x and Valkey 8 load. The encodings follow Valkey's `rdb.c`,
//! `listpack.c`, `ziplist.c`, `intset.c`, `zipmap.c`, `lzf_d.c` and
//! `t_stream.c`.
//!
//! What Rudis writes:
//! - strings (and integers) as `RDB_TYPE_STRING`, integer-encoded when they
//!   fit in 32 bits, never LZF-compressed;
//! - lists, sets, sorted sets and hashes in the plain (non-compact) types
//!   `RDB_TYPE_LIST`, `RDB_TYPE_SET`, `RDB_TYPE_ZSET_2`, `RDB_TYPE_HASH`;
//! - HyperLogLogs as dense `HYLL` strings (as Redis stores them);
//! - streams as `RDB_TYPE_STREAM_LISTPACKS_3`.
//!
//! State that Redis has no encoding for (JSON documents, probabilistic
//! filters, vector sets, CRDT state, hash field TTLs, AI-native records,
//! stream IDMP/XNACK state) is written as Rudis legacy-format records inside
//! `rudis-ext` AUX fields. Redis and Valkey skip AUX fields they don't know,
//! so such files still load there (without that state), and Rudis restores
//! it losslessly.
//!
//! Everything here treats its input as untrusted: reads are bounds checked,
//! nothing panics on corrupt data, and preallocations are capped by the
//! input that is left.

use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::compact::CompactStr;
use crate::table::{
    RudisHashMap, RudisSet, RudisStream, RudisTable, RudisValue, RudisZSet, StreamConsumer,
    StreamGroup, StreamId, StreamPelEntry,
};

/// The RDB version Rudis writes (Redis 7.0 to 7.2, Valkey 7.2 to 8.1).
pub const RDB_VERSION: u16 = 11;
/// Newest RDB version Rudis reads (Redis 7.4 and 8.x).
pub const MAX_RDB_VERSION: u32 = 12;
/// AUX field holding Rudis legacy-format records for state Redis has no
/// encoding for (see the module docs).
pub const EXT_AUX_KEY: &[u8] = b"rudis-ext";
/// Legacy-format record type of a stream's IDMP / XNACK state, written in a
/// [`EXT_AUX_KEY`] field right after the stream itself.
pub const EXT_TYPE_STREAM_EXTRAS: u8 = 19;

pub const TYPE_STRING: u8 = 0;
pub const TYPE_LIST: u8 = 1;
pub const TYPE_SET: u8 = 2;
pub const TYPE_ZSET: u8 = 3;
pub const TYPE_HASH: u8 = 4;
pub const TYPE_ZSET_2: u8 = 5;
pub const TYPE_MODULE_PRE_GA: u8 = 6;
pub const TYPE_MODULE_2: u8 = 7;
pub const TYPE_HASH_ZIPMAP: u8 = 9;
pub const TYPE_LIST_ZIPLIST: u8 = 10;
pub const TYPE_SET_INTSET: u8 = 11;
pub const TYPE_ZSET_ZIPLIST: u8 = 12;
pub const TYPE_HASH_ZIPLIST: u8 = 13;
pub const TYPE_LIST_QUICKLIST: u8 = 14;
pub const TYPE_STREAM_LISTPACKS: u8 = 15;
pub const TYPE_HASH_LISTPACK: u8 = 16;
pub const TYPE_ZSET_LISTPACK: u8 = 17;
pub const TYPE_LIST_QUICKLIST_2: u8 = 18;
pub const TYPE_STREAM_LISTPACKS_2: u8 = 19;
pub const TYPE_SET_LISTPACK: u8 = 20;
pub const TYPE_STREAM_LISTPACKS_3: u8 = 21;
/// Redis 7.4 hash field expiration types (RDB version 12).
pub const TYPE_HASH_METADATA_PRE_GA: u8 = 22;
pub const TYPE_HASH_LISTPACK_EX_PRE_GA: u8 = 23;
pub const TYPE_HASH_METADATA: u8 = 24;
pub const TYPE_HASH_LISTPACK_EX: u8 = 25;

pub const OP_SLOT_INFO: u8 = 244;
pub const OP_FUNCTION2: u8 = 245;
pub const OP_FUNCTION_PRE_GA: u8 = 246;
pub const OP_MODULE_AUX: u8 = 247;
pub const OP_IDLE: u8 = 248;
pub const OP_FREQ: u8 = 249;
pub const OP_AUX: u8 = 250;
pub const OP_RESIZEDB: u8 = 251;
pub const OP_EXPIRETIME_MS: u8 = 252;
pub const OP_EXPIRETIME: u8 = 253;
pub const OP_SELECTDB: u8 = 254;
pub const OP_EOF: u8 = 255;

const QUICKLIST_NODE_PLAIN: u64 = 1;
const QUICKLIST_NODE_PACKED: u64 = 2;

const STREAM_ITEM_FLAG_DELETED: i64 = 1;
const STREAM_ITEM_FLAG_SAMEFIELDS: i64 = 2;
/// Valkey's `stream-node-max-entries` / `stream-node-max-bytes` defaults.
const STREAM_NODE_MAX_ENTRIES: usize = 100;
const STREAM_NODE_MAX_BYTES: usize = 4096;

/// Hashes of up to this many fields load as `RudisValue::SmallHash`, like
/// the legacy decoder.
const SMALL_HASH_MAX: usize = 64;

/// An LZF back-reference token of 3 bytes expands to at most 264 bytes; a
/// claimed uncompressed length beyond that ratio is corrupt.
const LZF_MAX_RATIO: usize = 90;

/// A decode error. Messages describe what was wrong with the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdbError(Cow<'static, str>);

impl RdbError {
    pub fn new(msg: impl Into<Cow<'static, str>>) -> Self {
        RdbError(msg.into())
    }

    pub fn message(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RdbError {}

pub type RdbResult<T> = Result<T, RdbError>;

fn corrupt<T>(msg: &'static str) -> RdbResult<T> {
    Err(RdbError(Cow::Borrowed(msg)))
}

fn truncated() -> RdbError {
    RdbError(Cow::Borrowed("unexpected end of data"))
}

fn to_usize(n: u64) -> RdbResult<usize> {
    usize::try_from(n).map_err(|_| RdbError::new("length does not fit in memory"))
}

/// `&buf[start..start + len]`, or an error if that is out of bounds.
fn slice(buf: &[u8], start: usize, len: usize) -> RdbResult<&[u8]> {
    let end = start.checked_add(len).ok_or_else(truncated)?;
    buf.get(start..end).ok_or_else(truncated)
}

#[cfg(test)]
fn le_u16(buf: &[u8], at: usize) -> RdbResult<u16> {
    let mut a = [0u8; 2];
    a.copy_from_slice(slice(buf, at, 2)?);
    Ok(u16::from_le_bytes(a))
}

fn le_u32(buf: &[u8], at: usize) -> RdbResult<u32> {
    let mut a = [0u8; 4];
    a.copy_from_slice(slice(buf, at, 4)?);
    Ok(u32::from_le_bytes(a))
}

fn le_u64(buf: &[u8], at: usize) -> RdbResult<u64> {
    let mut a = [0u8; 8];
    a.copy_from_slice(slice(buf, at, 8)?);
    Ok(u64::from_le_bytes(a))
}

/// Preallocation for `count` claimed items of at least `min_item` bytes
/// each: never more than the remaining input could hold.
fn capped(count: u64, remaining: usize, min_item: usize) -> usize {
    let max = remaining / min_item.max(1);
    usize::try_from(count).unwrap_or(usize::MAX).min(max)
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn int_bytes(v: i64) -> Bytes {
    RudisTable::format_i64(v)
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// A bounds-checked cursor over RDB-encoded bytes.
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Reader { data, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    pub fn u8(&mut self) -> RdbResult<u8> {
        let b = *self.data.get(self.pos).ok_or_else(truncated)?;
        self.pos += 1;
        Ok(b)
    }

    pub fn take(&mut self, n: usize) -> RdbResult<&'a [u8]> {
        if n > self.remaining() {
            return Err(truncated());
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn array<const N: usize>(&mut self) -> RdbResult<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    /// A length, or (with `true`) a special string encoding id.
    fn len_or_encoding(&mut self) -> RdbResult<(u64, bool)> {
        let b = self.u8()?;
        match b >> 6 {
            0 => Ok(((b & 0x3F) as u64, false)),
            1 => {
                let b2 = self.u8()?;
                Ok(((((b & 0x3F) as u64) << 8) | b2 as u64, false))
            }
            2 => match b {
                0x80 => Ok((u32::from_be_bytes(self.array()?) as u64, false)),
                0x81 => Ok((u64::from_be_bytes(self.array()?), false)),
                _ => corrupt("unknown length encoding"),
            },
            _ => Ok(((b & 0x3F) as u64, true)),
        }
    }

    /// An RDB length (6, 14, 32 or 64 bit).
    pub fn length(&mut self) -> RdbResult<u64> {
        match self.len_or_encoding()? {
            (n, false) => Ok(n),
            (_, true) => corrupt("encoded value where a length was expected"),
        }
    }

    /// An RDB string: raw, integer-encoded or LZF-compressed.
    pub fn string(&mut self) -> RdbResult<Cow<'a, [u8]>> {
        let (n, encoded) = self.len_or_encoding()?;
        if !encoded {
            return Ok(Cow::Borrowed(self.take(to_usize(n)?)?));
        }
        let v = match n {
            0 => self.u8()? as i8 as i64,
            1 => i16::from_le_bytes(self.array()?) as i64,
            2 => i32::from_le_bytes(self.array()?) as i64,
            3 => {
                let clen = to_usize(self.length()?)?;
                let ulen = to_usize(self.length()?)?;
                let compressed = self.take(clen)?;
                return Ok(Cow::Owned(lzf_decompress(compressed, ulen)?));
            }
            _ => return corrupt("unknown string encoding"),
        };
        Ok(Cow::Owned(int_bytes(v).to_vec()))
    }

    /// [`Self::string`] as `Bytes`.
    pub fn bytes(&mut self) -> RdbResult<Bytes> {
        Ok(match self.string()? {
            Cow::Borrowed(b) => Bytes::copy_from_slice(b),
            Cow::Owned(v) => Bytes::from(v),
        })
    }

    /// Skips an RDB string without decoding (or decompressing) it.
    pub fn skip_string(&mut self) -> RdbResult<()> {
        let (n, encoded) = self.len_or_encoding()?;
        if !encoded {
            self.take(to_usize(n)?)?;
            return Ok(());
        }
        match n {
            0 => self.take(1).map(|_| ()),
            1 => self.take(2).map(|_| ()),
            2 => self.take(4).map(|_| ()),
            3 => {
                let clen = to_usize(self.length()?)?;
                self.length()?;
                self.take(clen).map(|_| ())
            }
            _ => corrupt("unknown string encoding"),
        }
    }

    /// A millisecond timestamp (8 bytes little endian).
    pub fn ms_time(&mut self) -> RdbResult<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn binary_double(&mut self) -> RdbResult<f64> {
        Ok(f64::from_le_bytes(self.array()?))
    }

    /// The pre-RDB-8 textual double (`RDB_TYPE_ZSET`).
    fn ascii_double(&mut self) -> RdbResult<f64> {
        match self.u8()? {
            253 => Ok(f64::NAN),
            254 => Ok(f64::INFINITY),
            255 => Ok(f64::NEG_INFINITY),
            n => parse_f64(self.take(n as usize)?).ok_or_else(|| RdbError::new("invalid double")),
        }
    }

    fn skip_ascii_double(&mut self) -> RdbResult<()> {
        let n = self.u8()?;
        if n < 253 {
            self.take(n as usize)?;
        }
        Ok(())
    }
}

fn parse_f64(s: &[u8]) -> Option<f64> {
    std::str::from_utf8(s).ok()?.parse::<f64>().ok()
}

// ---------------------------------------------------------------------------
// Compact encodings: LZF, ziplist, listpack, intset, zipmap
// ---------------------------------------------------------------------------

/// Decompresses an LZF block (Valkey's `lzf_decompress`) that must expand
/// to exactly `ulen` bytes.
pub fn lzf_decompress(input: &[u8], ulen: usize) -> RdbResult<Vec<u8>> {
    if ulen > input.len().saturating_mul(LZF_MAX_RATIO) {
        return corrupt("LZF uncompressed length is inconsistent");
    }
    let mut out = Vec::with_capacity(ulen);
    let mut i = 0;
    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;
        if ctrl < 32 {
            let n = ctrl + 1;
            let lit = slice(input, i, n)?;
            if out.len() + n > ulen {
                return corrupt("LZF output overrun");
            }
            out.extend_from_slice(lit);
            i += n;
        } else {
            let mut len = ctrl >> 5;
            if len == 7 {
                len += *input.get(i).ok_or_else(truncated)? as usize;
                i += 1;
            }
            let lo = *input.get(i).ok_or_else(truncated)? as usize;
            i += 1;
            let back = ((ctrl & 0x1F) << 8) + lo + 1;
            len += 2;
            if back > out.len() {
                return corrupt("LZF back reference before start");
            }
            if out.len() + len > ulen {
                return corrupt("LZF output overrun");
            }
            let start = out.len() - back;
            for k in 0..len {
                let b = out[start + k];
                out.push(b);
            }
        }
    }
    if out.len() != ulen {
        return corrupt("LZF uncompressed length mismatch");
    }
    Ok(out)
}

/// One element of a ziplist or listpack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Elem<'a> {
    Str(&'a [u8]),
    Int(i64),
}

impl Elem<'_> {
    pub fn to_bytes(self) -> Bytes {
        match self {
            Elem::Str(s) => Bytes::copy_from_slice(s),
            Elem::Int(v) => int_bytes(v),
        }
    }

    fn as_int(self) -> RdbResult<i64> {
        match self {
            Elem::Int(v) => Ok(v),
            Elem::Str(s) => {
                RudisTable::parse_i64_bytes(s).ok_or_else(|| RdbError::new("expected an integer"))
            }
        }
    }

    fn as_score(self) -> RdbResult<f64> {
        let v = match self {
            Elem::Int(v) => v as f64,
            Elem::Str(s) => {
                parse_f64(s).ok_or_else(|| RdbError::new("invalid sorted set score"))?
            }
        };
        if v.is_nan() {
            return corrupt("NaN sorted set score");
        }
        Ok(v)
    }
}

fn sign_extend(v: u64, bits: u32) -> i64 {
    let shift = 64 - bits;
    ((v << shift) as i64) >> shift
}

/// Calls `f` for each element of a ziplist (`RDB_TYPE_*_ZIPLIST`, Redis < 7).
pub fn ziplist_for_each<'a>(
    zl: &'a [u8],
    mut f: impl FnMut(Elem<'a>) -> RdbResult<()>,
) -> RdbResult<()> {
    if zl.len() < 11 || le_u32(zl, 0)? as usize != zl.len() || zl[zl.len() - 1] != 0xFF {
        return corrupt("invalid ziplist header");
    }
    let end = zl.len() - 1;
    let mut p = 10;
    while p < end {
        // prevlen: 1 byte, or 0xFE followed by 4 bytes.
        p += if zl[p] < 254 { 1 } else { 5 };
        let enc = *zl.get(p).ok_or_else(truncated)?;
        let (elem, size) = match enc >> 6 {
            0 => {
                let n = (enc & 0x3F) as usize;
                (Elem::Str(slice(zl, p + 1, n)?), 1 + n)
            }
            1 => {
                let n =
                    (((enc & 0x3F) as usize) << 8) | *zl.get(p + 1).ok_or_else(truncated)? as usize;
                (Elem::Str(slice(zl, p + 2, n)?), 2 + n)
            }
            2 => {
                if enc != 0x80 {
                    return corrupt("invalid ziplist string encoding");
                }
                let mut a = [0u8; 4];
                a.copy_from_slice(slice(zl, p + 1, 4)?);
                let n = u32::from_be_bytes(a) as usize;
                (Elem::Str(slice(zl, p + 5, n)?), 5 + n)
            }
            _ => {
                let int_at = |n: usize| -> RdbResult<u64> {
                    let b = slice(zl, p + 1, n)?;
                    Ok(b.iter().rev().fold(0u64, |acc, &x| (acc << 8) | x as u64))
                };
                match enc {
                    0xC0 => (Elem::Int(sign_extend(int_at(2)?, 16)), 3),
                    0xD0 => (Elem::Int(sign_extend(int_at(4)?, 32)), 5),
                    0xE0 => (Elem::Int(int_at(8)? as i64), 9),
                    0xF0 => (Elem::Int(sign_extend(int_at(3)?, 24)), 4),
                    0xFE => (Elem::Int(sign_extend(int_at(1)?, 8)), 2),
                    0xF1..=0xFD => (Elem::Int((enc & 0x0F) as i64 - 1), 1),
                    _ => return corrupt("invalid ziplist entry encoding"),
                }
            }
        };
        if p + size > end {
            return corrupt("ziplist entry overruns the ziplist");
        }
        f(elem)?;
        p += size;
    }
    if p != end {
        return corrupt("ziplist entries overrun the end marker");
    }
    Ok(())
}

/// Bytes the backlen of a listpack entry of `l` bytes takes.
fn lp_backlen_size(l: usize) -> usize {
    if l <= 127 {
        1
    } else if l < 16383 {
        2
    } else if l < 2097151 {
        3
    } else if l < 268435455 {
        4
    } else {
        5
    }
}

/// Calls `f` for each element of a listpack.
pub fn listpack_for_each<'a>(
    lp: &'a [u8],
    mut f: impl FnMut(Elem<'a>) -> RdbResult<()>,
) -> RdbResult<()> {
    if lp.len() < 7 || le_u32(lp, 0)? as usize != lp.len() || lp[lp.len() - 1] != 0xFF {
        return corrupt("invalid listpack header");
    }
    let end = lp.len() - 1;
    let mut p = 6;
    while p < end {
        let b = lp[p];
        let next = |k: usize| lp.get(p + k).copied().ok_or_else(truncated);
        let int_at = |n: usize| -> RdbResult<u64> {
            let s = slice(lp, p + 1, n)?;
            Ok(s.iter().rev().fold(0u64, |acc, &x| (acc << 8) | x as u64))
        };
        let (elem, len) = match b {
            0x00..=0x7F => (Elem::Int(b as i64), 1),
            0x80..=0xBF => {
                let n = (b & 0x3F) as usize;
                (Elem::Str(slice(lp, p + 1, n)?), 1 + n)
            }
            0xC0..=0xDF => {
                let v = (((b & 0x1F) as u64) << 8) | next(1)? as u64;
                (Elem::Int(sign_extend(v, 13)), 2)
            }
            0xE0..=0xEF => {
                let n = (((b & 0x0F) as usize) << 8) | next(1)? as usize;
                (Elem::Str(slice(lp, p + 2, n)?), 2 + n)
            }
            0xF0 => {
                let n = le_u32(lp, p + 1)? as usize;
                (Elem::Str(slice(lp, p + 5, n)?), 5 + n)
            }
            0xF1 => (Elem::Int(sign_extend(int_at(2)?, 16)), 3),
            0xF2 => (Elem::Int(sign_extend(int_at(3)?, 24)), 4),
            0xF3 => (Elem::Int(sign_extend(int_at(4)?, 32)), 5),
            0xF4 => (Elem::Int(int_at(8)? as i64), 9),
            _ => return corrupt("invalid listpack entry encoding"),
        };
        let total = len + lp_backlen_size(len);
        if p + total > end {
            return corrupt("listpack entry overruns the listpack");
        }
        f(elem)?;
        p += total;
    }
    Ok(())
}

fn listpack_elems(lp: &[u8]) -> RdbResult<Vec<Elem<'_>>> {
    let mut v = Vec::with_capacity(capped(u16::MAX as u64, lp.len(), 2));
    listpack_for_each(lp, |e| {
        v.push(e);
        Ok(())
    })?;
    Ok(v)
}

fn ziplist_elems(zl: &[u8]) -> RdbResult<Vec<Elem<'_>>> {
    let mut v = Vec::with_capacity(capped(u16::MAX as u64, zl.len(), 2));
    ziplist_for_each(zl, |e| {
        v.push(e);
        Ok(())
    })?;
    Ok(v)
}

/// Calls `f` for each member of an intset.
pub fn intset_for_each(is: &[u8], mut f: impl FnMut(i64) -> RdbResult<()>) -> RdbResult<()> {
    let enc = le_u32(is, 0)? as usize;
    let n = le_u32(is, 4)? as usize;
    if !matches!(enc, 2 | 4 | 8)
        || n.checked_mul(enc).and_then(|b| b.checked_add(8)) != Some(is.len())
    {
        return corrupt("invalid intset");
    }
    for i in 0..n {
        let s = &is[8 + i * enc..8 + (i + 1) * enc];
        let raw = s.iter().rev().fold(0u64, |acc, &x| (acc << 8) | x as u64);
        f(sign_extend(raw, (enc * 8) as u32))?;
    }
    Ok(())
}

/// The field/value pairs of a zipmap (`RDB_TYPE_HASH_ZIPMAP`, Redis < 2.6).
pub fn zipmap_pairs(zm: &[u8]) -> RdbResult<Vec<(Bytes, Bytes)>> {
    // Returns None at the end marker.
    fn zm_len(zm: &[u8], p: usize) -> RdbResult<Option<(usize, usize)>> {
        match *zm.get(p).ok_or_else(truncated)? {
            255 => Ok(None),
            254 => Ok(Some((le_u32(zm, p + 1)? as usize, 5))),
            b => Ok(Some((b as usize, 1))),
        }
    }
    let mut pairs = Vec::new();
    let mut p = 1;
    while let Some((klen, adv)) = zm_len(zm, p)? {
        p += adv;
        let key = slice(zm, p, klen)?;
        p += klen;
        let (vlen, adv) =
            zm_len(zm, p)?.ok_or_else(|| RdbError::new("zipmap key without value"))?;
        p += adv;
        let free = *zm.get(p).ok_or_else(truncated)? as usize;
        p += 1;
        let val = slice(zm, p, vlen)?;
        p += vlen + free;
        pairs.push((Bytes::copy_from_slice(key), Bytes::copy_from_slice(val)));
    }
    if p != zm.len() - 1 {
        return corrupt("zipmap has data after its end marker");
    }
    Ok(pairs)
}

// ---------------------------------------------------------------------------
// Value decoding
// ---------------------------------------------------------------------------

/// A decoded value plus, for Redis 7.4 hashes with field TTLs, each field's
/// absolute expiry in unix milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub value: RudisValue,
    pub field_expires: Vec<(Bytes, u64)>,
}

impl Decoded {
    fn plain(value: RudisValue) -> Option<Self> {
        Some(Decoded {
            value,
            field_expires: Vec::new(),
        })
    }
}

/// The value `SET` stores for `s`: an integer when it is a canonical i64.
pub fn string_value(s: &[u8]) -> RudisValue {
    match RudisTable::parse_i64_bytes(s) {
        Some(n) => RudisValue::Int(n),
        None => RudisValue::String(CompactStr::new(s)),
    }
}

fn list_value(list: VecDeque<Bytes>) -> Option<Decoded> {
    if list.is_empty() {
        None
    } else {
        Decoded::plain(RudisValue::List(Box::new(list)))
    }
}

fn set_insert(set: &mut RudisSet, member: Bytes) -> RdbResult<()> {
    if set.insert(member) {
        Ok(())
    } else {
        corrupt("duplicate set member")
    }
}

fn set_value(set: RudisSet) -> Option<Decoded> {
    if set.is_empty() {
        None
    } else {
        Decoded::plain(RudisValue::Set(Box::new(set)))
    }
}

fn zset_insert(z: &mut RudisZSet, member: Bytes, score: f64) -> RdbResult<()> {
    if score.is_nan() {
        return corrupt("NaN sorted set score");
    }
    if z.get_score(&member).is_some() {
        return corrupt("duplicate sorted set member");
    }
    z.insert(score, member);
    Ok(())
}

fn zset_value(z: RudisZSet) -> Option<Decoded> {
    if z.is_empty() {
        None
    } else {
        Decoded::plain(RudisValue::ZSet(Box::new(z)))
    }
}

/// A hash value from its pairs, `SmallHash` up to [`SMALL_HASH_MAX`] fields.
fn hash_value(pairs: Vec<(Bytes, Bytes)>) -> RdbResult<Option<RudisValue>> {
    if pairs.is_empty() {
        return Ok(None);
    }
    if pairs.len() <= SMALL_HASH_MAX {
        for (i, (f, _)) in pairs.iter().enumerate() {
            if pairs[..i].iter().any(|(g, _)| g == f) {
                return corrupt("duplicate hash field");
            }
        }
        return Ok(Some(RudisValue::SmallHash(Box::new(pairs))));
    }
    let mut map = RudisHashMap::with_capacity_and_hasher(pairs.len(), Default::default());
    for (f, v) in pairs {
        if map.insert(f, v).is_some() {
            return corrupt("duplicate hash field");
        }
    }
    Ok(Some(RudisValue::Hash(Box::new(map))))
}

fn pairs_from_elems(elems: &[Elem<'_>]) -> RdbResult<Vec<(Bytes, Bytes)>> {
    if !elems.len().is_multiple_of(2) {
        return corrupt("odd number of hash elements");
    }
    Ok(elems
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| (c[0].to_bytes(), c[1].to_bytes()))
        .collect())
}

fn zset_from_elems(elems: &[Elem<'_>]) -> RdbResult<Option<Decoded>> {
    if !elems.len().is_multiple_of(2) {
        return corrupt("odd number of sorted set elements");
    }
    let mut z = RudisZSet::new();
    for c in elems.as_chunks::<2>().0 {
        zset_insert(&mut z, c[0].to_bytes(), c[1].as_score()?)?;
    }
    Ok(zset_value(z))
}

/// Decodes the value of an object of RDB type `t`. `Ok(None)` is an empty
/// collection, which Redis skips on load. Hash fields whose TTL is at or
/// before `now_ms` are dropped, like Redis does.
pub fn decode_value(t: u8, r: &mut Reader<'_>, now_ms: u64) -> RdbResult<Option<Decoded>> {
    match t {
        TYPE_STRING => Ok(Decoded::plain(string_value(&r.string()?))),
        TYPE_LIST => {
            let n = r.length()?;
            let mut list = VecDeque::with_capacity(capped(n, r.remaining(), 1));
            for _ in 0..n {
                list.push_back(r.bytes()?);
            }
            Ok(list_value(list))
        }
        TYPE_SET => {
            let n = r.length()?;
            let mut set = RudisSet::with_capacity(capped(n, r.remaining(), 1));
            for _ in 0..n {
                set_insert(&mut set, r.bytes()?)?;
            }
            Ok(set_value(set))
        }
        TYPE_ZSET | TYPE_ZSET_2 => {
            let n = r.length()?;
            let mut z = RudisZSet::new();
            for _ in 0..n {
                let member = r.bytes()?;
                let score = if t == TYPE_ZSET_2 {
                    r.binary_double()?
                } else {
                    r.ascii_double()?
                };
                zset_insert(&mut z, member, score)?;
            }
            Ok(zset_value(z))
        }
        TYPE_HASH => {
            let n = r.length()?;
            let mut pairs = Vec::with_capacity(capped(n, r.remaining(), 2));
            for _ in 0..n {
                let f = r.bytes()?;
                let v = r.bytes()?;
                pairs.push((f, v));
            }
            Ok(hash_value(pairs)?.and_then(Decoded::plain))
        }
        TYPE_HASH_ZIPMAP => {
            let blob = r.string()?;
            Ok(hash_value(zipmap_pairs(&blob)?)?.and_then(Decoded::plain))
        }
        TYPE_LIST_ZIPLIST => {
            let blob = r.string()?;
            let mut list = VecDeque::new();
            ziplist_for_each(&blob, |e| {
                list.push_back(e.to_bytes());
                Ok(())
            })?;
            Ok(list_value(list))
        }
        TYPE_SET_INTSET => {
            let blob = r.string()?;
            let mut set = RudisSet::new();
            intset_for_each(&blob, |v| set_insert(&mut set, int_bytes(v)))?;
            Ok(set_value(set))
        }
        TYPE_SET_LISTPACK => {
            let blob = r.string()?;
            let mut set = RudisSet::new();
            listpack_for_each(&blob, |e| set_insert(&mut set, e.to_bytes()))?;
            Ok(set_value(set))
        }
        TYPE_ZSET_ZIPLIST => zset_from_elems(&ziplist_elems(&r.string()?)?),
        TYPE_ZSET_LISTPACK => zset_from_elems(&listpack_elems(&r.string()?)?),
        TYPE_HASH_ZIPLIST => {
            let blob = r.string()?;
            Ok(hash_value(pairs_from_elems(&ziplist_elems(&blob)?)?)?.and_then(Decoded::plain))
        }
        TYPE_HASH_LISTPACK => {
            let blob = r.string()?;
            Ok(hash_value(pairs_from_elems(&listpack_elems(&blob)?)?)?.and_then(Decoded::plain))
        }
        TYPE_LIST_QUICKLIST => {
            let n = r.length()?;
            let mut list = VecDeque::new();
            for _ in 0..n {
                let blob = r.string()?;
                ziplist_for_each(&blob, |e| {
                    list.push_back(e.to_bytes());
                    Ok(())
                })?;
            }
            Ok(list_value(list))
        }
        TYPE_LIST_QUICKLIST_2 => {
            let n = r.length()?;
            let mut list = VecDeque::new();
            for _ in 0..n {
                let container = r.length()?;
                let blob = r.string()?;
                match container {
                    QUICKLIST_NODE_PLAIN => list.push_back(Bytes::copy_from_slice(&blob)),
                    QUICKLIST_NODE_PACKED => listpack_for_each(&blob, |e| {
                        list.push_back(e.to_bytes());
                        Ok(())
                    })?,
                    _ => return corrupt("unknown quicklist node container"),
                }
            }
            Ok(list_value(list))
        }
        TYPE_STREAM_LISTPACKS | TYPE_STREAM_LISTPACKS_2 | TYPE_STREAM_LISTPACKS_3 => Ok(
            Decoded::plain(RudisValue::Stream(Box::new(decode_stream(t, r)?))),
        ),
        TYPE_HASH_METADATA | TYPE_HASH_METADATA_PRE_GA => {
            let min_expire = if t == TYPE_HASH_METADATA {
                Some(r.ms_time()? as u64)
            } else {
                None
            };
            let n = r.length()?;
            let mut pairs = Vec::with_capacity(capped(n, r.remaining(), 3));
            let mut expires = Vec::new();
            for _ in 0..n {
                let ttl = r.length()?;
                let f = r.bytes()?;
                let v = r.bytes()?;
                let at = match (ttl, min_expire) {
                    (0, _) => 0,
                    (ttl, Some(min)) => ttl.wrapping_add(min).wrapping_sub(1),
                    (ttl, None) => ttl,
                };
                if at != 0 && at <= now_ms {
                    continue;
                }
                if at != 0 {
                    expires.push((f.clone(), at));
                }
                pairs.push((f, v));
            }
            Ok(hash_value(pairs)?.map(|value| Decoded {
                value,
                field_expires: expires,
            }))
        }
        TYPE_HASH_LISTPACK_EX | TYPE_HASH_LISTPACK_EX_PRE_GA => {
            if t == TYPE_HASH_LISTPACK_EX {
                r.ms_time()?;
            }
            let blob = r.string()?;
            let elems = listpack_elems(&blob)?;
            if !elems.len().is_multiple_of(3) {
                return corrupt("hash listpack with TTLs is not made of triplets");
            }
            let mut pairs = Vec::with_capacity(elems.len() / 3);
            let mut expires = Vec::new();
            for c in elems.as_chunks::<3>().0 {
                let at = c[2].as_int()? as u64;
                if at != 0 && at <= now_ms {
                    continue;
                }
                let f = c[0].to_bytes();
                if at != 0 {
                    expires.push((f.clone(), at));
                }
                pairs.push((f, c[1].to_bytes()));
            }
            Ok(hash_value(pairs)?.map(|value| Decoded {
                value,
                field_expires: expires,
            }))
        }
        TYPE_MODULE_PRE_GA | TYPE_MODULE_2 => Err(RdbError::new(
            "the data contains a Redis module value, which rudis cannot load",
        )),
        _ => Err(RdbError::new(format!("unknown RDB object type {t}"))),
    }
}

/// Skips the value of an object of type `t` without building it.
pub fn skip_value(t: u8, r: &mut Reader<'_>) -> RdbResult<()> {
    match t {
        TYPE_STRING
        | TYPE_HASH_ZIPMAP
        | TYPE_LIST_ZIPLIST
        | TYPE_SET_INTSET
        | TYPE_ZSET_ZIPLIST
        | TYPE_HASH_ZIPLIST
        | TYPE_HASH_LISTPACK
        | TYPE_ZSET_LISTPACK
        | TYPE_SET_LISTPACK
        | TYPE_HASH_LISTPACK_EX_PRE_GA => r.skip_string(),
        TYPE_LIST | TYPE_SET | TYPE_LIST_QUICKLIST => {
            for _ in 0..r.length()? {
                r.skip_string()?;
            }
            Ok(())
        }
        TYPE_HASH => {
            for _ in 0..r.length()? {
                r.skip_string()?;
                r.skip_string()?;
            }
            Ok(())
        }
        TYPE_ZSET => {
            for _ in 0..r.length()? {
                r.skip_string()?;
                r.skip_ascii_double()?;
            }
            Ok(())
        }
        TYPE_ZSET_2 => {
            for _ in 0..r.length()? {
                r.skip_string()?;
                r.take(8)?;
            }
            Ok(())
        }
        TYPE_LIST_QUICKLIST_2 => {
            for _ in 0..r.length()? {
                r.length()?;
                r.skip_string()?;
            }
            Ok(())
        }
        TYPE_HASH_LISTPACK_EX => {
            r.take(8)?;
            r.skip_string()
        }
        _ => decode_value(t, r, 0).map(|_| ()),
    }
}

fn raw_stream_id(raw: &[u8]) -> RdbResult<StreamId> {
    if raw.len() != 16 {
        return corrupt("stream ID is not 16 bytes");
    }
    let mut ms = [0u8; 8];
    let mut seq = [0u8; 8];
    ms.copy_from_slice(&raw[..8]);
    seq.copy_from_slice(&raw[8..]);
    Ok(StreamId::new(
        u64::from_be_bytes(ms),
        u64::from_be_bytes(seq),
    ))
}

/// Valkey's `streamEstimateDistanceFromFirstEverEntry`, for consumer
/// groups loaded from `RDB_TYPE_STREAM_LISTPACKS` (no stored offset).
fn estimate_entries_read(s: &RudisStream, id: StreamId) -> Option<u64> {
    if s.entries_added == 0 {
        return Some(0);
    }
    if s.entries.is_empty() && id <= s.last_id {
        return Some(s.entries_added);
    }
    match id.cmp(&s.last_id) {
        std::cmp::Ordering::Equal => return Some(s.entries_added),
        std::cmp::Ordering::Greater => return None,
        std::cmp::Ordering::Less => {}
    }
    let first = s.entries.keys().next().copied().unwrap_or_default();
    let max_del = s.max_deleted_entry_id;
    if max_del == StreamId::default() || max_del < first {
        let base = s.entries_added.checked_sub(s.entries.len() as u64)?;
        match id.cmp(&first) {
            std::cmp::Ordering::Less => return Some(base),
            std::cmp::Ordering::Equal => return Some(base + 1),
            std::cmp::Ordering::Greater => {}
        }
    }
    None
}

fn decode_stream(t: u8, r: &mut Reader<'_>) -> RdbResult<RudisStream> {
    let mut s = RudisStream::new();
    let nodes = r.length()?;
    for _ in 0..nodes {
        let master = raw_stream_id(&r.string()?)?;
        let lp = r.string()?;
        let elems = listpack_elems(&lp)?;
        if elems.is_empty() {
            return corrupt("empty listpack inside stream");
        }
        let mut it = elems.into_iter();
        let mut next = || {
            it.next()
                .ok_or_else(|| RdbError::new("truncated stream listpack"))
        };
        let _count = next()?.as_int()?;
        let _deleted = next()?.as_int()?;
        let master_n = to_usize(next()?.as_int()?.max(0) as u64)?;
        if master_n > lp.len() {
            return corrupt("stream master entry has too many fields");
        }
        let mut master_fields = Vec::with_capacity(master_n);
        for _ in 0..master_n {
            master_fields.push(next()?.to_bytes());
        }
        if next()?.as_int()? != 0 {
            return corrupt("stream master entry terminator is not 0");
        }
        loop {
            let flags = match it.next() {
                None => break,
                Some(e) => e.as_int()?,
            };
            let mut next = || {
                it.next()
                    .ok_or_else(|| RdbError::new("truncated stream entry"))
            };
            let ms = master.ms.wrapping_add(next()?.as_int()? as u64);
            let seq = master.seq.wrapping_add(next()?.as_int()? as u64);
            let fields: Vec<(Bytes, Bytes)> = if flags & STREAM_ITEM_FLAG_SAMEFIELDS != 0 {
                let mut v = Vec::with_capacity(master_fields.len());
                for f in &master_fields {
                    v.push((f.clone(), next()?.to_bytes()));
                }
                v
            } else {
                let n = to_usize(next()?.as_int()?.max(0) as u64)?;
                if n > lp.len() {
                    return corrupt("stream entry has too many fields");
                }
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    let f = next()?.to_bytes();
                    v.push((f, next()?.to_bytes()));
                }
                v
            };
            next()?; // lp-count
            if flags & STREAM_ITEM_FLAG_DELETED == 0 {
                s.entries.insert(StreamId::new(ms, seq), fields);
            }
        }
    }
    let length = r.length()?;
    s.last_id = StreamId::new(r.length()?, r.length()?);
    if t >= TYPE_STREAM_LISTPACKS_2 {
        let _first_id = (r.length()?, r.length()?);
        s.max_deleted_entry_id = StreamId::new(r.length()?, r.length()?);
        s.entries_added = r.length()?;
    } else {
        s.max_deleted_entry_id = StreamId::default();
        s.entries_added = length;
    }
    if length != s.entries.len() as u64 {
        return corrupt("stream length does not match its entries");
    }
    let ngroups = r.length()?;
    for _ in 0..ngroups {
        let name = r.bytes()?;
        let last_delivered_id = StreamId::new(r.length()?, r.length()?);
        let entries_read = if t >= TYPE_STREAM_LISTPACKS_2 {
            match r.length()? {
                u64::MAX => None,
                n => Some(n),
            }
        } else {
            estimate_entries_read(&s, last_delivered_id)
        };
        let mut pel = BTreeMap::new();
        for _ in 0..r.length()? {
            let id = raw_stream_id(r.take(16)?)?;
            let delivery_time_ms = r.ms_time()? as u64;
            let delivery_count = to_usize(r.length()?)?;
            let prev = pel.insert(
                id,
                StreamPelEntry {
                    consumer: Bytes::new(),
                    delivery_time_ms,
                    delivery_count,
                    nack_seq: 0,
                },
            );
            if prev.is_some() {
                return corrupt("duplicate stream PEL entry");
            }
        }
        let mut consumers: hashbrown::HashMap<Bytes, StreamConsumer> = Default::default();
        for _ in 0..r.length()? {
            let cname = r.bytes()?;
            let seen_time_ms = r.ms_time()? as u64;
            let active_time_ms = if t >= TYPE_STREAM_LISTPACKS_3 {
                match r.ms_time()? {
                    -1 => None,
                    v => Some(v as u64),
                }
            } else {
                Some(seen_time_ms)
            };
            let mut cpel = BTreeMap::new();
            for _ in 0..r.length()? {
                let id = raw_stream_id(r.take(16)?)?;
                let pe = pel
                    .get_mut(&id)
                    .ok_or_else(|| RdbError::new("consumer PEL entry not in the group PEL"))?;
                if !pe.consumer.is_empty() && pe.consumer != cname {
                    return corrupt("stream PEL entry owned by two consumers");
                }
                pe.consumer = cname.clone();
                cpel.insert(id, pe.delivery_time_ms);
            }
            let consumer = StreamConsumer {
                name: cname.clone(),
                seen_time_ms,
                active_time_ms,
                pel: cpel,
            };
            if consumers.insert(cname, consumer).is_some() {
                return corrupt("duplicate stream consumer");
            }
        }
        // Rudis needs every pending entry to have an owner.
        pel.retain(|_, pe| !pe.consumer.is_empty());
        let group = StreamGroup {
            name: name.clone(),
            last_delivered_id,
            entries_read,
            consumers,
            pel,
            next_nack_seq: 0,
        };
        if s.groups.insert(name, group).is_some() {
            return corrupt("duplicate stream consumer group");
        }
    }
    s.rebuild_nodes();
    Ok(s)
}

/// Decodes a DUMP payload body: the type byte and value, with the 10-byte
/// version + CRC trailer already removed. The value must use up `body`.
pub fn decode_dump_body(body: &[u8], now_ms: u64) -> RdbResult<Decoded> {
    let mut r = Reader::new(body);
    let t = r.u8()?;
    let decoded = decode_value(t, &mut r, now_ms)?;
    if !r.is_empty() {
        return corrupt("trailing bytes after the DUMP value");
    }
    decoded.ok_or_else(|| RdbError::new("empty collection in DUMP payload"))
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Appends an RDB length.
pub fn write_len(out: &mut Vec<u8>, n: u64) {
    if n < 1 << 6 {
        out.push(n as u8);
    } else if n < 1 << 14 {
        out.push(0x40 | (n >> 8) as u8);
        out.push(n as u8);
    } else if n <= u32::MAX as u64 {
        out.push(0x80);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

/// Appends `v` as an integer-encoded string if it fits in 32 bits.
fn write_int_encoded(out: &mut Vec<u8>, v: i64) -> bool {
    if let Ok(b) = i8::try_from(v) {
        out.push(0xC0);
        out.push(b as u8);
    } else if let Ok(h) = i16::try_from(v) {
        out.push(0xC1);
        out.extend_from_slice(&h.to_le_bytes());
    } else if let Ok(w) = i32::try_from(v) {
        out.push(0xC2);
        out.extend_from_slice(&w.to_le_bytes());
    } else {
        return false;
    }
    true
}

/// Appends a string without trying the integer encoding.
pub fn write_raw_string(out: &mut Vec<u8>, s: &[u8]) {
    write_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// Appends a string, integer-encoded when it is a canonical integer that
/// fits in 32 bits (as Redis does).
pub fn write_string(out: &mut Vec<u8>, s: &[u8]) {
    if s.len() <= 11
        && let Some(v) = RudisTable::parse_i64_bytes(s)
        && write_int_encoded(out, v)
    {
        return;
    }
    write_raw_string(out, s);
}

fn write_i64_string(out: &mut Vec<u8>, v: i64) {
    if !write_int_encoded(out, v) {
        write_raw_string(out, &int_bytes(v));
    }
}

pub fn write_aux(out: &mut Vec<u8>, key: &[u8], val: &[u8]) {
    out.push(OP_AUX);
    write_string(out, key);
    write_string(out, val);
}

/// Wraps Rudis legacy-format records in a [`EXT_AUX_KEY`] AUX field.
pub fn write_ext_aux(out: &mut Vec<u8>, records: &[u8]) {
    if !records.is_empty() {
        out.push(OP_AUX);
        write_raw_string(out, EXT_AUX_KEY);
        write_raw_string(out, records);
    }
}

/// The RDB file header: magic, AUX fields, function libraries and
/// `SELECTDB 0`. Records, then [`OP_EOF`] and the CRC64 follow.
pub fn write_file_header(out: &mut Vec<u8>, used_mem: u64) {
    out.extend_from_slice(format!("REDIS{:04}", RDB_VERSION).as_bytes());
    let ctime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    write_aux(out, b"redis-ver", b"7.2.4");
    write_aux(out, b"redis-bits", b"64");
    write_aux(out, b"ctime", ctime.to_string().as_bytes());
    write_aux(out, b"used-mem", used_mem.to_string().as_bytes());
    write_aux(out, b"aof-base", b"0");
    write_aux(out, b"rudis-ver", env!("CARGO_PKG_VERSION").as_bytes());
    for code in crate::scripting::library_codes() {
        out.push(OP_FUNCTION2);
        write_raw_string(out, code.as_bytes());
    }
    out.push(OP_SELECTDB);
    write_len(out, 0);
}

/// RDB type byte `val` is written with; `None` for tiered values, which the
/// caller must read back from the tier first.
pub fn value_type(val: &RudisValue) -> Option<u8> {
    match val {
        RudisValue::String(_) | RudisValue::Int(_) | RudisValue::HyperLogLog(_) => {
            Some(TYPE_STRING)
        }
        RudisValue::List(_) => Some(TYPE_LIST),
        RudisValue::Set(_) => Some(TYPE_SET),
        RudisValue::ZSet(_) => Some(TYPE_ZSET_2),
        RudisValue::SmallHash(_) | RudisValue::Hash(_) => Some(TYPE_HASH),
        RudisValue::Stream(_) => Some(TYPE_STREAM_LISTPACKS_3),
        RudisValue::Tiered(_) => None,
        RudisValue::Cooled(cv) => value_type(&cv.val),
    }
}

/// A Redis dense HyperLogLog string (cardinality cache marked stale).
pub fn hll_dense_string(regs: &[u8; 16384]) -> Vec<u8> {
    let mut s = Vec::with_capacity(crate::hll::HLL_DENSE_SIZE);
    s.extend_from_slice(b"HYLL");
    s.push(crate::hll::HLL_DENSE);
    s.extend_from_slice(&[0, 0, 0]);
    s.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0x80]);
    s.extend_from_slice(&crate::hll::hll_encode_dense(regs));
    s
}

/// Appends `val`'s payload (without the type byte).
pub fn write_value_payload(val: &RudisValue, out: &mut Vec<u8>) {
    match val {
        RudisValue::String(s) => write_string(out, &s.view()),
        RudisValue::Int(n) => write_i64_string(out, *n),
        RudisValue::HyperLogLog(regs) => write_raw_string(out, &hll_dense_string(regs)),
        RudisValue::List(l) => {
            write_len(out, l.len() as u64);
            for item in l.iter() {
                write_string(out, item);
            }
        }
        RudisValue::Set(s) => {
            write_len(out, s.len() as u64);
            for item in s.iter() {
                write_string(out, item);
            }
        }
        RudisValue::ZSet(z) => {
            write_len(out, z.len() as u64);
            z.for_each(|m, score| {
                write_string(out, m);
                out.extend_from_slice(&score.to_le_bytes());
            });
        }
        RudisValue::SmallHash(pairs) => {
            write_len(out, pairs.len() as u64);
            for (f, v) in pairs.iter() {
                write_string(out, f);
                write_string(out, v);
            }
        }
        RudisValue::Hash(h) => {
            write_len(out, h.len() as u64);
            for (f, v) in h.iter() {
                write_string(out, f);
                write_string(out, v);
            }
        }
        RudisValue::Stream(s) => write_stream(s, out),
        RudisValue::Tiered(_) => {}
        RudisValue::Cooled(cv) => write_value_payload(&cv.val, out),
    }
}

/// Appends one key record: optional `EXPIRETIME_MS`, type, key, value.
/// Returns false (writing nothing) for a tiered value.
pub fn write_record(
    out: &mut Vec<u8>,
    key: &[u8],
    val: &RudisValue,
    expire_unix_ms: Option<u64>,
) -> bool {
    let Some(t) = value_type(val) else {
        return false;
    };
    if let Some(ms) = expire_unix_ms {
        out.push(OP_EXPIRETIME_MS);
        out.extend_from_slice(&ms.to_le_bytes());
    }
    out.push(t);
    write_string(out, key);
    write_value_payload(val, out);
    true
}

/// Appends DUMP's trailer: RDB version and CRC64 of everything before it.
fn seal_dump(out: &mut Vec<u8>) {
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = crate::table::crc64(out);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// The DUMP payload of `val`. Streams carrying IDMP or XNACK state, which
/// Redis has no encoding for, are dumped in Rudis's legacy format so that
/// cross-shard moves (which go through DUMP/RESTORE) keep it.
pub fn dump_value(val: &RudisValue) -> Option<Vec<u8>> {
    let val = match val {
        RudisValue::Cooled(cv) => &cv.val,
        v => v,
    };
    if let RudisValue::Stream(s) = val
        && stream_has_extras(s)
    {
        let mut out = Vec::new();
        RudisTable::serialize_val_payload(val, &mut out);
        RudisTable::seal_dump_payload(&mut out);
        return Some(out);
    }
    let t = value_type(val)?;
    let mut out = Vec::new();
    out.push(t);
    write_value_payload(val, &mut out);
    seal_dump(&mut out);
    Some(out)
}

/// A listpack under construction.
struct LpWriter {
    buf: Vec<u8>,
    count: usize,
}

impl LpWriter {
    fn new() -> Self {
        LpWriter {
            buf: vec![0; 6],
            count: 0,
        }
    }

    fn backlen(&mut self, l: usize) {
        let l = l as u64;
        match lp_backlen_size(l as usize) {
            1 => self.buf.push(l as u8),
            2 => self
                .buf
                .extend_from_slice(&[(l >> 7) as u8, ((l & 127) | 128) as u8]),
            3 => self.buf.extend_from_slice(&[
                (l >> 14) as u8,
                (((l >> 7) & 127) | 128) as u8,
                ((l & 127) | 128) as u8,
            ]),
            4 => self.buf.extend_from_slice(&[
                (l >> 21) as u8,
                (((l >> 14) & 127) | 128) as u8,
                (((l >> 7) & 127) | 128) as u8,
                ((l & 127) | 128) as u8,
            ]),
            _ => self.buf.extend_from_slice(&[
                (l >> 28) as u8,
                (((l >> 21) & 127) | 128) as u8,
                (((l >> 14) & 127) | 128) as u8,
                (((l >> 7) & 127) | 128) as u8,
                ((l & 127) | 128) as u8,
            ]),
        }
    }

    fn int(&mut self, v: i64) {
        let start = self.buf.len();
        if (0..=127).contains(&v) {
            self.buf.push(v as u8);
        } else if (-4096..=4095).contains(&v) {
            let u = (v as u64) & 0x1FFF;
            self.buf.push(0xC0 | (u >> 8) as u8);
            self.buf.push(u as u8);
        } else if let Ok(h) = i16::try_from(v) {
            self.buf.push(0xF1);
            self.buf.extend_from_slice(&h.to_le_bytes());
        } else if (-(1 << 23)..(1 << 23)).contains(&v) {
            self.buf.push(0xF2);
            self.buf.extend_from_slice(&(v as i32).to_le_bytes()[..3]);
        } else if let Ok(w) = i32::try_from(v) {
            self.buf.push(0xF3);
            self.buf.extend_from_slice(&w.to_le_bytes());
        } else {
            self.buf.push(0xF4);
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
        let len = self.buf.len() - start;
        self.backlen(len);
        self.count += 1;
    }

    fn str(&mut self, s: &[u8]) {
        let start = self.buf.len();
        let n = s.len();
        if n < 64 {
            self.buf.push(0x80 | n as u8);
        } else if n < 4096 {
            self.buf.push(0xE0 | (n >> 8) as u8);
            self.buf.push(n as u8);
        } else {
            self.buf.push(0xF0);
            self.buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        self.buf.extend_from_slice(s);
        let len = self.buf.len() - start;
        self.backlen(len);
        self.count += 1;
    }

    /// A string element, integer-encoded when it is a canonical integer.
    fn bytes(&mut self, s: &[u8]) {
        match RudisTable::parse_i64_bytes(s) {
            Some(v) => self.int(v),
            None => self.str(s),
        }
    }

    fn finish(mut self) -> Vec<u8> {
        self.buf.push(0xFF);
        let total = self.buf.len() as u32;
        self.buf[0..4].copy_from_slice(&total.to_le_bytes());
        let n = self.count.min(u16::MAX as usize) as u16;
        self.buf[4..6].copy_from_slice(&n.to_le_bytes());
        self.buf
    }
}

fn raw_id(id: StreamId) -> [u8; 16] {
    let mut raw = [0u8; 16];
    raw[..8].copy_from_slice(&id.ms.to_be_bytes());
    raw[8..].copy_from_slice(&id.seq.to_be_bytes());
    raw
}

type StreamEntryRef<'a> = (&'a StreamId, &'a Vec<(Bytes, Bytes)>);

/// One stream radix-tree node: master entry and delta-encoded entries.
fn stream_node_listpack(master: StreamId, entries: &[StreamEntryRef<'_>]) -> Vec<u8> {
    let master_fields = entries[0].1;
    let mut lp = LpWriter::new();
    lp.int(entries.len() as i64);
    lp.int(0);
    lp.int(master_fields.len() as i64);
    for (f, _) in master_fields {
        lp.bytes(f);
    }
    lp.int(0);
    for (id, fields) in entries {
        let same = fields.len() == master_fields.len()
            && fields
                .iter()
                .zip(master_fields.iter())
                .all(|((f, _), (mf, _))| f == mf);
        lp.int(if same { STREAM_ITEM_FLAG_SAMEFIELDS } else { 0 });
        lp.int(id.ms.wrapping_sub(master.ms) as i64);
        lp.int(id.seq.wrapping_sub(master.seq) as i64);
        if same {
            for (_, v) in fields.iter() {
                lp.bytes(v);
            }
            lp.int(fields.len() as i64 + 3);
        } else {
            lp.int(fields.len() as i64);
            for (f, v) in fields.iter() {
                lp.bytes(f);
                lp.bytes(v);
            }
            lp.int(fields.len() as i64 * 2 + 4);
        }
    }
    lp.finish()
}

fn write_stream(s: &RudisStream, out: &mut Vec<u8>) {
    let mut nodes: Vec<(StreamId, Vec<u8>)> = Vec::new();
    let mut batch: Vec<StreamEntryRef<'_>> = Vec::with_capacity(STREAM_NODE_MAX_ENTRIES);
    let mut batch_bytes = 0;
    for entry in s.entries.iter() {
        batch_bytes += entry
            .1
            .iter()
            .map(|(f, v)| f.len() + v.len() + 4)
            .sum::<usize>()
            + 6;
        batch.push(entry);
        if batch.len() >= STREAM_NODE_MAX_ENTRIES || batch_bytes >= STREAM_NODE_MAX_BYTES {
            let master = *batch[0].0;
            nodes.push((master, stream_node_listpack(master, &batch)));
            batch.clear();
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        let master = *batch[0].0;
        nodes.push((master, stream_node_listpack(master, &batch)));
    }
    write_len(out, nodes.len() as u64);
    for (master, lp) in &nodes {
        write_raw_string(out, &raw_id(*master));
        write_raw_string(out, lp);
    }
    write_len(out, s.entries.len() as u64);
    write_len(out, s.last_id.ms);
    write_len(out, s.last_id.seq);
    let first = s.entries.keys().next().copied().unwrap_or_default();
    write_len(out, first.ms);
    write_len(out, first.seq);
    write_len(out, s.max_deleted_entry_id.ms);
    write_len(out, s.max_deleted_entry_id.seq);
    write_len(out, s.entries_added);

    // Groups in name order, like Valkey's radix tree.
    let mut groups: Vec<&StreamGroup> = s.groups.values().collect();
    groups.sort_by(|a, b| a.name.cmp(&b.name));
    write_len(out, groups.len() as u64);
    for g in groups {
        write_raw_string(out, &g.name);
        write_len(out, g.last_delivered_id.ms);
        write_len(out, g.last_delivered_id.seq);
        write_len(out, g.entries_read.unwrap_or(u64::MAX));
        write_len(out, g.pel.len() as u64);
        for (id, pe) in &g.pel {
            out.extend_from_slice(&raw_id(*id));
            out.extend_from_slice(&pe.delivery_time_ms.to_le_bytes());
            write_len(out, pe.delivery_count as u64);
        }
        let mut consumers: Vec<&StreamConsumer> = g.consumers.values().collect();
        consumers.sort_by(|a, b| a.name.cmp(&b.name));
        write_len(out, consumers.len() as u64);
        for c in consumers {
            write_raw_string(out, &c.name);
            out.extend_from_slice(&c.seen_time_ms.to_le_bytes());
            let active = c.active_time_ms.map_or(-1i64, |v| v as i64);
            out.extend_from_slice(&active.to_le_bytes());
            // Only entries the group PEL assigns to this consumer: Redis
            // rejects anything else.
            let owned: Vec<&StreamId> = c
                .pel
                .keys()
                .filter(|id| g.pel.get(id).is_some_and(|pe| pe.consumer == c.name))
                .collect();
            write_len(out, owned.len() as u64);
            for id in owned {
                out.extend_from_slice(&raw_id(*id));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rudis stream extras (IDMP / XNACK state)
// ---------------------------------------------------------------------------

/// Whether `s` has state the Redis stream encoding cannot carry.
pub fn stream_has_extras(s: &RudisStream) -> bool {
    s.idmp_duration.is_some()
        || s.idmp_maxsize.is_some()
        || !s.idmp_producers.is_empty()
        || s.iids_added != 0
        || s.iids_duplicates != 0
        || s.groups.values().any(|g| g.next_nack_seq != 0)
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn put_opt_u64(out: &mut Vec<u8>, v: Option<u64>) {
    match v {
        Some(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_le_bytes());
        }
        None => out.push(0),
    }
}

/// Appends a legacy-format record (`[u32 key len][key][19][payload]`) with
/// `s`'s IDMP / XNACK state, for a [`EXT_AUX_KEY`] field.
pub fn encode_stream_extras(key: &[u8], s: &RudisStream, out: &mut Vec<u8>) {
    put_bytes(out, key);
    out.push(EXT_TYPE_STREAM_EXTRAS);
    put_opt_u64(out, s.idmp_duration);
    put_opt_u64(out, s.idmp_maxsize.map(|v| v as u64));
    out.extend_from_slice(&(s.idmp_producers.len() as u32).to_le_bytes());
    for (pid, prod) in &s.idmp_producers {
        put_bytes(out, pid);
        let iids: Vec<(&Bytes, &(StreamId, u64))> = prod
            .order
            .iter()
            .filter_map(|iid| prod.iids.get(iid).map(|v| (iid, v)))
            .collect();
        out.extend_from_slice(&(iids.len() as u32).to_le_bytes());
        for (iid, (sid, added_at)) in iids {
            put_bytes(out, iid);
            out.extend_from_slice(&sid.ms.to_le_bytes());
            out.extend_from_slice(&sid.seq.to_le_bytes());
            out.extend_from_slice(&added_at.to_le_bytes());
        }
    }
    out.extend_from_slice(&s.iids_added.to_le_bytes());
    out.extend_from_slice(&s.iids_duplicates.to_le_bytes());
    let groups: Vec<&StreamGroup> = s.groups.values().filter(|g| g.next_nack_seq != 0).collect();
    out.extend_from_slice(&(groups.len() as u32).to_le_bytes());
    for g in groups {
        put_bytes(out, &g.name);
        out.extend_from_slice(&g.next_nack_seq.to_le_bytes());
        let nacked: Vec<(&StreamId, &StreamPelEntry)> =
            g.pel.iter().filter(|(_, pe)| pe.nack_seq != 0).collect();
        out.extend_from_slice(&(nacked.len() as u32).to_le_bytes());
        for (id, pe) in nacked {
            out.extend_from_slice(&id.ms.to_le_bytes());
            out.extend_from_slice(&id.seq.to_le_bytes());
            out.extend_from_slice(&pe.nack_seq.to_le_bytes());
        }
    }
}

/// A little-endian cursor for the legacy-format extras payload.
struct LeCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> LeCursor<'a> {
    fn u8(&mut self) -> RdbResult<u8> {
        let b = *self.data.get(self.pos).ok_or_else(truncated)?;
        self.pos += 1;
        Ok(b)
    }

    fn u32(&mut self) -> RdbResult<u32> {
        let v = le_u32(self.data, self.pos)?;
        self.pos += 4;
        Ok(v)
    }

    fn u64(&mut self) -> RdbResult<u64> {
        let v = le_u64(self.data, self.pos)?;
        self.pos += 8;
        Ok(v)
    }

    fn bytes(&mut self) -> RdbResult<Bytes> {
        let n = self.u32()? as usize;
        let b = slice(self.data, self.pos, n)?;
        self.pos += n;
        Ok(Bytes::copy_from_slice(b))
    }

    fn opt_u64(&mut self) -> RdbResult<Option<u64>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => corrupt("invalid option tag"),
        }
    }
}

/// Parses a stream-extras payload (after the type byte), applying it to
/// `stream` when given. Returns the bytes used.
pub fn apply_stream_extras(data: &[u8], stream: Option<&mut RudisStream>) -> RdbResult<usize> {
    let mut c = LeCursor { data, pos: 0 };
    let duration = c.opt_u64()?;
    let maxsize = c.opt_u64()?;
    let mut producers: hashbrown::HashMap<Bytes, crate::table::IdmpProducer> = Default::default();
    for _ in 0..c.u32()? {
        let pid = c.bytes()?;
        let mut prod = crate::table::IdmpProducer::new();
        for _ in 0..c.u32()? {
            let iid = c.bytes()?;
            let sid = StreamId::new(c.u64()?, c.u64()?);
            let added_at = c.u64()?;
            prod.order.push_back(iid.clone());
            prod.iids.insert(iid, (sid, added_at));
        }
        producers.insert(pid, prod);
    }
    let iids_added = c.u64()?;
    let iids_duplicates = c.u64()?;
    let mut nacks = Vec::new();
    for _ in 0..c.u32()? {
        let name = c.bytes()?;
        let next_nack_seq = c.u64()?;
        let mut seqs = Vec::new();
        for _ in 0..c.u32()? {
            seqs.push((StreamId::new(c.u64()?, c.u64()?), c.u64()?));
        }
        nacks.push((name, next_nack_seq, seqs));
    }
    if let Some(s) = stream {
        s.idmp_duration = duration;
        s.idmp_maxsize = maxsize.map(|v| v as usize);
        s.idmp_producers = producers;
        s.iids_added = iids_added;
        s.iids_duplicates = iids_duplicates;
        for (name, next, seqs) in nacks {
            if let Some(g) = s.groups.get_mut(&name) {
                g.next_nack_seq = next;
                for (id, seq) in seqs {
                    if let Some(pe) = g.pel.get_mut(&id) {
                        pe.nack_seq = seq;
                    }
                }
            }
        }
    }
    Ok(c.pos)
}

// ---------------------------------------------------------------------------
// Loading files and record streams into a shard
// ---------------------------------------------------------------------------

/// How [`crate::table::load_rdb_bytes`] should treat a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    /// Redis format only.
    Redis,
    /// `REDIS0011` followed directly by `SELECTDB 0`: written either by an
    /// older Rudis (legacy format) or without AUX fields (Redis format).
    Ambiguous,
}

/// Parses the `REDISnnnn` header, returning the RDB version.
pub fn header_version(data: &[u8]) -> RdbResult<u32> {
    if data.len() < 9 || !data.starts_with(b"REDIS") {
        return corrupt("invalid RDB magic header");
    }
    let digits = &data[5..9];
    if !digits.iter().all(u8::is_ascii_digit) {
        return corrupt("invalid RDB version");
    }
    Ok(digits
        .iter()
        .fold(0u32, |acc, &d| acc * 10 + (d - b'0') as u32))
}

pub fn classify(data: &[u8]) -> FileKind {
    if data.starts_with(b"REDIS0011") && data.get(9..11) == Some(&[OP_SELECTDB, 0][..]) {
        FileKind::Ambiguous
    } else {
        FileKind::Redis
    }
}

/// Where loaded keys go.
struct LoadCtx<'a> {
    db: &'a mut crate::shard::ShardDb,
    /// `(shard_id, num_shards)`: keep only this shard's keys. `None` keeps
    /// everything (a single shard's own chunk).
    shard: Option<(usize, usize)>,
    /// Ambiguous files: also reject empty keys and empty collections, which
    /// Rudis never writes and a legacy file misread as Redis format yields.
    strict: bool,
    load_functions: bool,
    now_ms: u64,
    count: usize,
}

impl LoadCtx<'_> {
    fn owns(&self, key: &[u8]) -> bool {
        match self.shard {
            Some((sid, n)) => crate::router::target_shard(key, n) == sid,
            None => true,
        }
    }

    fn insert(&mut self, key: &[u8], decoded: Decoded, expire_ms: Option<i64>) {
        let now = Instant::now();
        let expire_at = expire_ms
            .map(|ms| now + Duration::from_millis((ms as u64).saturating_sub(self.now_ms)));
        self.db.table.insert_entry(crate::table::RudisEntry::new(
            crate::compact::CompactKey::new(key),
            decoded.value,
            crate::table::Expiry::from(expire_at),
        ));
        if !decoded.field_expires.is_empty() {
            let key = Bytes::copy_from_slice(key);
            let map = self.db.table.hash_field_expires.entry(key).or_default();
            for (f, at) in decoded.field_expires {
                map.insert(
                    f,
                    now + Duration::from_millis(at.saturating_sub(self.now_ms)),
                );
            }
        }
        self.count += 1;
    }
}

/// Reads records until `OP_EOF` (returning true) or, when `chunk` is set,
/// the end of the input (returning false).
fn load_records(r: &mut Reader<'_>, ctx: &mut LoadCtx<'_>, chunk: bool) -> RdbResult<bool> {
    let key_load_delay = crate::connection::key_load_delay_us();
    let mut expire_ms: Option<i64> = None;
    let mut dbid = 0u64;
    loop {
        if chunk && r.is_empty() {
            return Ok(false);
        }
        let t = r.u8()?;
        match t {
            OP_EOF => return Ok(true),
            OP_EXPIRETIME_MS => expire_ms = Some(r.ms_time()?),
            OP_EXPIRETIME => {
                expire_ms = Some(i32::from_le_bytes(r.array()?) as i64 * 1000);
            }
            OP_FREQ => {
                r.u8()?;
            }
            OP_IDLE => {
                r.length()?;
            }
            OP_SELECTDB => dbid = r.length()?,
            OP_RESIZEDB => {
                r.length()?;
                r.length()?;
            }
            OP_SLOT_INFO => {
                r.length()?;
                r.length()?;
                r.length()?;
            }
            OP_AUX => {
                let k = r.string()?;
                let v = r.string()?;
                if k.as_ref() == EXT_AUX_KEY {
                    let (sid, n) = ctx.shard.unwrap_or((0, 1));
                    ctx.count += crate::table::load_legacy_ext(&v, ctx.db, sid, n)
                        .map_err(|e| RdbError::new(format!("invalid rudis-ext record: {e}")))?;
                }
            }
            OP_MODULE_AUX => {
                return Err(RdbError::new(
                    "the RDB contains Redis module auxiliary data, which rudis cannot load",
                ));
            }
            OP_FUNCTION2 => {
                let code = r.string()?;
                if ctx.load_functions {
                    match std::str::from_utf8(&code) {
                        Ok(code) => {
                            if let Err(e) = crate::scripting::load_function(code, true) {
                                tracing::warn!("RDB load: skipping function library: {e}");
                            }
                        }
                        Err(_) => tracing::warn!("RDB load: skipping non-UTF-8 function library"),
                    }
                }
            }
            OP_FUNCTION_PRE_GA => {
                // Redis 7.0 release candidates: name, engine, optional
                // description, code. Redis itself refuses these; skip.
                r.skip_string()?;
                r.skip_string()?;
                if r.length()? != 0 {
                    r.skip_string()?;
                }
                r.skip_string()?;
                tracing::warn!("RDB load: skipped a pre-GA (Redis 7.0 RC) function library");
            }
            _ => {
                let key = r.string()?;
                if dbid != 0 {
                    return Err(RdbError::new(format!(
                        "the RDB has keys in database {dbid}; rudis has a single database (0)"
                    )));
                }
                if ctx.strict && key.is_empty() {
                    return corrupt("empty key name");
                }
                let exp = expire_ms.take();
                let expired = exp.is_some_and(|ms| ms <= ctx.now_ms as i64);
                if !ctx.owns(&key) || (expired && !ctx.strict) {
                    skip_value(t, r)?;
                    continue;
                }
                match decode_value(t, r, ctx.now_ms)? {
                    Some(d) if !expired => {
                        ctx.insert(&key, d, exp);
                        if key_load_delay > 0 {
                            std::thread::sleep(Duration::from_micros(key_load_delay));
                        }
                    }
                    Some(_) => {}
                    None if ctx.strict => return corrupt("empty collection"),
                    None => {}
                }
            }
        }
    }
}

/// Loads a Redis-format RDB file into `db`, keeping the keys of shard
/// `shard_id` of `num_shards`. Returns the number of keys loaded.
pub fn load_file(
    data: &[u8],
    db: &mut crate::shard::ShardDb,
    shard_id: usize,
    num_shards: usize,
    strict: bool,
) -> RdbResult<usize> {
    let ver = header_version(data)?;
    if ver == 0 || ver > MAX_RDB_VERSION {
        return Err(RdbError::new(format!(
            "unsupported RDB version {ver} (rudis reads versions 1 to {MAX_RDB_VERSION})"
        )));
    }
    let body_end = if ver >= 5 {
        if data.len() < 9 + 1 + 8 {
            return Err(truncated());
        }
        let end = data.len() - 8;
        let expected = le_u64(data, end)?;
        if expected != 0 && expected != crate::table::crc64(&data[..end]) {
            return corrupt("CRC64 checksum mismatch in RDB file");
        }
        end
    } else {
        data.len()
    };
    let mut r = Reader::new(&data[9..body_end]);
    let mut ctx = LoadCtx {
        db,
        shard: Some((shard_id, num_shards)),
        strict,
        load_functions: shard_id == 0,
        now_ms: unix_ms_now(),
        count: 0,
    };
    load_records(&mut r, &mut ctx, false).map_err(|e| {
        RdbError::new(format!(
            "RDB parse error at offset {}: {e}",
            9 + r.position()
        ))
    })?;
    if !r.is_empty() {
        return Err(RdbError::new(format!(
            "RDB parse error at offset {}: data after the EOF marker",
            9 + r.position()
        )));
    }
    Ok(ctx.count)
}

/// Loads a header-less record stream (`ShardDb::save_rdb_chunk`'s output)
/// into `db`, keeping every key.
pub fn load_chunk(data: &[u8], db: &mut crate::shard::ShardDb) -> RdbResult<usize> {
    let mut r = Reader::new(data);
    let mut ctx = LoadCtx {
        db,
        shard: None,
        strict: false,
        load_functions: false,
        now_ms: unix_ms_now(),
        count: 0,
    };
    load_records(&mut r, &mut ctx, true)?;
    Ok(ctx.count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lp_of(f: impl FnOnce(&mut LpWriter)) -> Vec<u8> {
        let mut w = LpWriter::new();
        f(&mut w);
        w.finish()
    }

    fn lp_vals(lp: &[u8]) -> Vec<Elem<'_>> {
        listpack_elems(lp).unwrap()
    }

    #[test]
    fn length_encodings_round_trip() {
        for n in [
            0u64,
            1,
            63,
            64,
            16383,
            16384,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX,
        ] {
            let mut out = Vec::new();
            write_len(&mut out, n);
            let mut r = Reader::new(&out);
            assert_eq!(r.length().unwrap(), n);
            assert!(r.is_empty());
        }
        // 14-bit lengths are big endian.
        assert_eq!(Reader::new(&[0x41, 0x02]).length().unwrap(), 0x102);
        assert!(Reader::new(&[0x82]).length().is_err());
        assert!(Reader::new(&[0x80, 0, 0]).length().is_err());
    }

    #[test]
    fn string_encodings() {
        assert_eq!(&*Reader::new(&[0xC0, 0xFF]).string().unwrap(), b"-1");
        assert_eq!(
            &*Reader::new(&[0xC1, 0x39, 0x30]).string().unwrap(),
            b"12345"
        );
        assert_eq!(
            &*Reader::new(&[0xC2, 0x00, 0xCA, 0x9A, 0x3B])
                .string()
                .unwrap(),
            b"1000000000"
        );
        assert!(Reader::new(&[0xC4]).string().is_err());
        for s in [
            &b""[..],
            b"0",
            b"-128",
            b"127",
            b"32767",
            b"-32769",
            b"2147483647",
            b"2147483648",
            b"007",
            b"-0",
            b"hello",
        ] {
            let mut out = Vec::new();
            write_string(&mut out, s);
            let mut r = Reader::new(&out);
            assert_eq!(&*r.string().unwrap(), s);
            assert!(r.is_empty());
            let mut r = Reader::new(&out);
            r.skip_string().unwrap();
            assert!(r.is_empty());
        }
        let mut out = Vec::new();
        write_string(&mut out, b"100");
        assert_eq!(out, [0xC0, 100]);
    }

    #[test]
    fn lzf_decoding() {
        // Literal "a", then a back reference of 9 bytes at distance 1.
        let c = [0x00, b'a', 0xE0, 0x00, 0x00];
        assert_eq!(lzf_decompress(&c, 10).unwrap(), b"aaaaaaaaaa");
        assert!(lzf_decompress(&c, 11).is_err());
        assert!(lzf_decompress(&c, 9).is_err());
        // Back reference before the start of the output.
        assert!(lzf_decompress(&[0x20, 0x05], 3).is_err());
        // Truncated literal.
        assert!(lzf_decompress(&[0x05, b'a'], 6).is_err());
        // Absurd claimed length is rejected before allocating.
        assert!(lzf_decompress(&c, usize::MAX / 2).is_err());
        // Through the string reader: 0xC3 clen ulen data.
        let mut enc = vec![0xC3, 5, 10];
        enc.extend_from_slice(&c);
        let mut r = Reader::new(&enc);
        assert_eq!(&*r.string().unwrap(), b"aaaaaaaaaa");
        let mut r = Reader::new(&enc);
        r.skip_string().unwrap();
        assert!(r.is_empty());
    }

    fn ziplist(entries: &[&[u8]]) -> Vec<u8> {
        // Entries are pre-encoded (encoding + data); prevlen is filled in.
        let mut body = Vec::new();
        let mut prev = 0usize;
        for e in entries {
            let start = body.len();
            if prev < 254 {
                body.push(prev as u8);
            } else {
                body.push(254);
                body.extend_from_slice(&(prev as u32).to_le_bytes());
            }
            body.extend_from_slice(e);
            prev = body.len() - start;
        }
        let mut zl = Vec::new();
        let total = 10 + body.len() + 1;
        zl.extend_from_slice(&(total as u32).to_le_bytes());
        zl.extend_from_slice(&0u32.to_le_bytes());
        zl.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        zl.extend_from_slice(&body);
        zl.push(0xFF);
        zl
    }

    #[test]
    fn ziplist_decoding() {
        let mut long = vec![0x41, 0x2C];
        long.extend(std::iter::repeat_n(b'x', 300));
        let zl = ziplist(&[
            b"\x03abc",
            &[0xF1],
            &[0xFD],
            &[0xFE, 0x80],
            &[0xC0, 0x00, 0x80],
            &[0xF0, 0xFF, 0xFF, 0x7F],
            &[0xD0, 0x00, 0x00, 0x00, 0x80],
            &[0xE0, 1, 0, 0, 0, 0, 0, 0, 0],
            &long,
        ]);
        let got = ziplist_elems(&zl).unwrap();
        assert_eq!(got[0], Elem::Str(b"abc"));
        assert_eq!(got[1], Elem::Int(0));
        assert_eq!(got[2], Elem::Int(12));
        assert_eq!(got[3], Elem::Int(-128));
        assert_eq!(got[4], Elem::Int(-32768));
        assert_eq!(got[5], Elem::Int(8388607));
        assert_eq!(got[6], Elem::Int(i32::MIN as i64));
        assert_eq!(got[7], Elem::Int(1));
        assert_eq!(got[8], Elem::Str(&[b'x'; 300]));
        // Header length mismatch, missing end marker, truncation.
        let mut bad = zl.clone();
        bad[0] ^= 1;
        assert!(ziplist_elems(&bad).is_err());
        let mut bad = zl.clone();
        let last = bad.len() - 1;
        bad[last] = 0;
        assert!(ziplist_elems(&bad).is_err());
        assert!(ziplist_elems(&zl[..zl.len() - 3]).is_err());
        assert!(ziplist_elems(&ziplist(&[b"\x09abc"])).is_err());
    }

    #[test]
    fn listpack_round_trip() {
        let ints = [
            0i64,
            127,
            128,
            -1,
            4095,
            -4096,
            4096,
            32767,
            -32768,
            40000,
            8388607,
            -8388608,
            8388608,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ];
        let long = vec![b'y'; 5000];
        let mid = vec![b'z'; 100];
        let lp = lp_of(|w| {
            for &v in &ints {
                w.int(v);
            }
            w.str(b"");
            w.str(b"hello");
            w.str(&mid);
            w.str(&long);
        });
        assert_eq!(le_u16(&lp, 4).unwrap() as usize, ints.len() + 4);
        let got = lp_vals(&lp);
        for (i, &v) in ints.iter().enumerate() {
            assert_eq!(got[i], Elem::Int(v), "int {v}");
        }
        assert_eq!(got[ints.len()], Elem::Str(b""));
        assert_eq!(got[ints.len() + 1], Elem::Str(b"hello"));
        assert_eq!(got[ints.len() + 2], Elem::Str(&mid));
        assert_eq!(got[ints.len() + 3], Elem::Str(&long));
        assert!(listpack_elems(&lp[..lp.len() - 1]).is_err());
        let mut bad = lp.clone();
        bad[6] = 0xF7;
        assert!(listpack_elems(&bad).is_err());
        // Strings that look like integers are stored as integers.
        let lp = lp_of(|w| w.bytes(b"42"));
        assert_eq!(lp_vals(&lp), vec![Elem::Int(42)]);
    }

    #[test]
    fn intset_and_zipmap_decoding() {
        let mut is = Vec::new();
        is.extend_from_slice(&4u32.to_le_bytes());
        is.extend_from_slice(&2u32.to_le_bytes());
        is.extend_from_slice(&(-5i32).to_le_bytes());
        is.extend_from_slice(&100000i32.to_le_bytes());
        let mut got = Vec::new();
        intset_for_each(&is, |v| {
            got.push(v);
            Ok(())
        })
        .unwrap();
        assert_eq!(got, vec![-5, 100000]);
        assert!(intset_for_each(&is[..is.len() - 1], |_| Ok(())).is_err());
        let mut bad = is.clone();
        bad[0] = 3;
        assert!(intset_for_each(&bad, |_| Ok(())).is_err());

        // zmlen, "a" -> "1" (1 free byte), "bb" -> "", end.
        let zm = [2, 1, b'a', 1, 1, b'1', 0, 2, b'b', b'b', 0, 0, 0xFF];
        let pairs = zipmap_pairs(&zm).unwrap();
        assert_eq!(
            pairs,
            vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
                (Bytes::from_static(b"bb"), Bytes::new()),
            ]
        );
        assert!(zipmap_pairs(&zm[..zm.len() - 1]).is_err());
    }

    fn round_trip(val: &RudisValue) -> RudisValue {
        let dump = dump_value(val).unwrap();
        let n = dump.len();
        assert_eq!(u16::from_le_bytes([dump[n - 10], dump[n - 9]]), RDB_VERSION);
        assert_eq!(
            u64::from_le_bytes(dump[n - 8..].try_into().unwrap()),
            crate::table::crc64(&dump[..n - 8])
        );
        decode_dump_body(&dump[..n - 10], 0).unwrap().value
    }

    #[test]
    fn writer_round_trips_every_type() {
        let s = RudisValue::String(CompactStr::new(b"hello world"));
        assert_eq!(round_trip(&s), s);
        for n in [0i64, -7, 70000, i64::MAX, i64::MIN] {
            assert_eq!(round_trip(&RudisValue::Int(n)), RudisValue::Int(n));
        }
        let list: VecDeque<Bytes> = (0..300).map(|i| Bytes::from(format!("e{i}"))).collect();
        let l = RudisValue::List(Box::new(list));
        assert_eq!(round_trip(&l), l);

        let mut set = RudisSet::new();
        for i in 0..100 {
            set.insert(Bytes::from(format!("m{i}")));
        }
        match round_trip(&RudisValue::Set(Box::new(set.clone()))) {
            RudisValue::Set(got) => {
                assert_eq!(got.len(), 100);
                assert!(set.iter().all(|m| got.contains(m)));
            }
            other => panic!("{other:?}"),
        }

        let mut z = RudisZSet::new();
        z.insert(1.5, Bytes::from_static(b"a"));
        z.insert(f64::INFINITY, Bytes::from_static(b"b"));
        z.insert(-0.25, Bytes::from_static(b"c"));
        match round_trip(&RudisValue::ZSet(Box::new(z.clone()))) {
            RudisValue::ZSet(got) => assert_eq!(got.to_vec(), z.to_vec()),
            other => panic!("{other:?}"),
        }

        let pairs: Vec<(Bytes, Bytes)> = (0..10)
            .map(|i| (Bytes::from(format!("f{i}")), Bytes::from(format!("{i}"))))
            .collect();
        let h = RudisValue::SmallHash(Box::new(pairs));
        assert_eq!(round_trip(&h), h);
        let mut big = RudisHashMap::default();
        for i in 0..200 {
            big.insert(Bytes::from(format!("f{i}")), Bytes::from(format!("v{i}")));
        }
        let h = RudisValue::Hash(Box::new(big));
        assert_eq!(round_trip(&h), h);

        // A HyperLogLog comes back as the dense HYLL string Redis uses.
        let mut regs = Box::new([0u8; 16384]);
        regs[5] = 3;
        regs[16383] = 50;
        match round_trip(&RudisValue::HyperLogLog(regs.clone())) {
            RudisValue::String(s) => {
                let s = s.view().to_vec();
                assert_eq!(s.len(), crate::hll::HLL_DENSE_SIZE);
                assert_eq!(crate::hll::hll_decode_registers(&s).unwrap(), *regs);
            }
            other => panic!("{other:?}"),
        }
    }

    fn sample_stream() -> RudisStream {
        let mut s = RudisStream::new();
        for i in 0..250u64 {
            let fields = if i % 7 == 0 {
                vec![(Bytes::from_static(b"other"), Bytes::from(format!("{i}")))]
            } else {
                vec![
                    (Bytes::from_static(b"name"), Bytes::from(format!("n{i}"))),
                    (
                        Bytes::from_static(b"n"),
                        Bytes::from(format!("{}", i * 1000)),
                    ),
                ]
            };
            s.entries.insert(StreamId::new(1000 + i / 3, i % 3), fields);
        }
        s.last_id = StreamId::new(2000, 5);
        s.entries_added = 300;
        s.max_deleted_entry_id = StreamId::new(1500, 1);
        let mut g = StreamGroup {
            name: Bytes::from_static(b"g1"),
            last_delivered_id: StreamId::new(1010, 0),
            entries_read: Some(31),
            consumers: Default::default(),
            pel: BTreeMap::new(),
            next_nack_seq: 0,
        };
        let mut c = StreamConsumer {
            name: Bytes::from_static(b"alice"),
            seen_time_ms: 1_700_000_000_000,
            active_time_ms: None,
            pel: BTreeMap::new(),
        };
        for id in [StreamId::new(1000, 1), StreamId::new(1003, 2)] {
            g.pel.insert(
                id,
                StreamPelEntry {
                    consumer: c.name.clone(),
                    delivery_time_ms: 1_700_000_000_123,
                    delivery_count: 4,
                    nack_seq: 0,
                },
            );
            c.pel.insert(id, 1_700_000_000_123);
        }
        g.consumers.insert(c.name.clone(), c);
        s.groups.insert(g.name.clone(), g);
        s.groups.insert(
            Bytes::from_static(b"g0"),
            StreamGroup {
                name: Bytes::from_static(b"g0"),
                last_delivered_id: StreamId::default(),
                entries_read: None,
                consumers: Default::default(),
                pel: BTreeMap::new(),
                next_nack_seq: 0,
            },
        );
        s.rebuild_nodes();
        s
    }

    #[test]
    fn stream_round_trip() {
        let s = sample_stream();
        match round_trip(&RudisValue::Stream(Box::new(s.clone()))) {
            RudisValue::Stream(got) => assert_eq!(*got, s),
            other => panic!("{other:?}"),
        }
        let empty = RudisStream::new();
        match round_trip(&RudisValue::Stream(Box::new(empty.clone()))) {
            RudisValue::Stream(got) => assert_eq!(*got, empty),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn stream_extras_round_trip_and_legacy_dump() {
        let mut s = sample_stream();
        s.idmp_duration = Some(500);
        s.idmp_maxsize = Some(12);
        s.iids_added = 3;
        s.iids_duplicates = 1;
        let mut prod = crate::table::IdmpProducer::new();
        prod.order.push_back(Bytes::from_static(b"iid1"));
        prod.iids.insert(
            Bytes::from_static(b"iid1"),
            (StreamId::new(1000, 1), 1_700_000_000_000),
        );
        s.idmp_producers.insert(Bytes::from_static(b"p1"), prod);
        let g = s.groups.get_mut(&b"g1"[..]).unwrap();
        g.next_nack_seq = 2;
        g.pel.get_mut(&StreamId::new(1003, 2)).unwrap().nack_seq = 2;
        assert!(stream_has_extras(&s));

        let mut rec = Vec::new();
        encode_stream_extras(b"k", &s, &mut rec);
        assert_eq!(&rec[..4], &1u32.to_le_bytes());
        assert_eq!(rec[5], EXT_TYPE_STREAM_EXTRAS);
        let payload = &rec[6..];
        let mut plain = s.clone();
        plain.idmp_duration = None;
        plain.idmp_maxsize = None;
        plain.iids_added = 0;
        plain.iids_duplicates = 0;
        plain.idmp_producers.clear();
        let g = plain.groups.get_mut(&b"g1"[..]).unwrap();
        g.next_nack_seq = 0;
        g.pel.get_mut(&StreamId::new(1003, 2)).unwrap().nack_seq = 0;
        assert!(!stream_has_extras(&plain));
        let used = apply_stream_extras(payload, Some(&mut plain)).unwrap();
        assert_eq!(used, payload.len());
        assert_eq!(plain, s);
        for cut in 0..payload.len() {
            assert!(apply_stream_extras(&payload[..cut], None).is_err());
        }

        // Such a stream is dumped in the legacy format (version 10).
        let dump = dump_value(&RudisValue::Stream(Box::new(s))).unwrap();
        let n = dump.len();
        assert_eq!(u16::from_le_bytes([dump[n - 10], dump[n - 9]]), 10);
        assert_eq!(dump[0], 6);
    }

    #[test]
    fn redis_hfe_types_decode() {
        let now = 1_000_000u64;
        // RDB_TYPE_HASH_METADATA: min expire, then (ttl delta, field, value).
        let mut body = vec![TYPE_HASH_METADATA];
        body.extend_from_slice(&(now as i64 + 5000).to_le_bytes());
        write_len(&mut body, 3);
        write_len(&mut body, 0);
        write_string(&mut body, b"keep");
        write_string(&mut body, b"v1");
        write_len(&mut body, 1);
        write_string(&mut body, b"ttl");
        write_string(&mut body, b"v2");
        write_len(&mut body, 101);
        write_string(&mut body, b"ttl2");
        write_string(&mut body, b"v3");
        let d = decode_dump_body(&body, now).unwrap();
        assert_eq!(
            d.field_expires,
            vec![
                (Bytes::from_static(b"ttl"), now + 5000),
                (Bytes::from_static(b"ttl2"), now + 5100)
            ]
        );
        match d.value {
            RudisValue::SmallHash(p) => assert_eq!(p.len(), 3),
            other => panic!("{other:?}"),
        }
        // Expired fields are dropped.
        let d = decode_dump_body(&body, now + 5050).unwrap();
        assert_eq!(d.field_expires.len(), 1);

        // RDB_TYPE_HASH_LISTPACK_EX: min expire, listpack of triplets.
        let lp = lp_of(|w| {
            w.str(b"a");
            w.str(b"1");
            w.int(0);
            w.str(b"b");
            w.str(b"2");
            w.int(now as i64 + 10);
            w.str(b"gone");
            w.str(b"3");
            w.int(now as i64 - 10);
        });
        let mut body = vec![TYPE_HASH_LISTPACK_EX];
        body.extend_from_slice(&(now as i64 + 10).to_le_bytes());
        write_raw_string(&mut body, &lp);
        let d = decode_dump_body(&body, now).unwrap();
        assert_eq!(d.field_expires, vec![(Bytes::from_static(b"b"), now + 10)]);
        match d.value {
            RudisValue::SmallHash(p) => assert_eq!(p.len(), 2),
            other => panic!("{other:?}"),
        }
        // The pre-GA variant has no min expire, and absolute TTLs.
        let mut body = vec![TYPE_HASH_LISTPACK_EX_PRE_GA];
        write_raw_string(&mut body, &lp);
        assert_eq!(decode_dump_body(&body, now).unwrap().field_expires.len(), 1);
    }

    #[test]
    fn compact_types_decode() {
        // Hash listpack, zset listpack, set listpack, quicklist 2.
        let lp = lp_of(|w| {
            w.str(b"f");
            w.int(7);
        });
        let mut body = vec![TYPE_HASH_LISTPACK];
        write_raw_string(&mut body, &lp);
        assert_eq!(
            decode_dump_body(&body, 0).unwrap().value,
            RudisValue::SmallHash(Box::new(vec![(
                Bytes::from_static(b"f"),
                Bytes::from_static(b"7")
            )]))
        );
        let zlp = lp_of(|w| {
            w.str(b"m");
            w.str(b"1.5");
            w.str(b"n");
            w.int(-3);
        });
        let mut body = vec![TYPE_ZSET_LISTPACK];
        write_raw_string(&mut body, &zlp);
        match decode_dump_body(&body, 0).unwrap().value {
            RudisValue::ZSet(z) => assert_eq!(
                z.to_vec(),
                vec![
                    (Bytes::from_static(b"n"), -3.0),
                    (Bytes::from_static(b"m"), 1.5)
                ]
            ),
            other => panic!("{other:?}"),
        }
        let mut body = vec![TYPE_LIST_QUICKLIST_2];
        write_len(&mut body, 2);
        write_len(&mut body, QUICKLIST_NODE_PACKED);
        write_raw_string(&mut body, &lp);
        write_len(&mut body, QUICKLIST_NODE_PLAIN);
        write_raw_string(&mut body, b"big plain");
        match decode_dump_body(&body, 0).unwrap().value {
            RudisValue::List(l) => assert_eq!(
                l.iter().cloned().collect::<Vec<_>>(),
                vec![
                    Bytes::from_static(b"f"),
                    Bytes::from_static(b"7"),
                    Bytes::from_static(b"big plain")
                ]
            ),
            other => panic!("{other:?}"),
        }
        // Duplicates are corrupt; empty collections are not values.
        let dup = lp_of(|w| {
            w.str(b"x");
            w.str(b"x");
        });
        let mut body = vec![TYPE_SET_LISTPACK];
        write_raw_string(&mut body, &dup);
        assert!(decode_dump_body(&body, 0).is_err());
        assert!(decode_dump_body(&[TYPE_LIST, 0], 0).is_err());
        // Old ascii doubles.
        let mut body = vec![TYPE_ZSET];
        write_len(&mut body, 2);
        write_string(&mut body, b"a");
        body.extend_from_slice(b"\x032.5");
        write_string(&mut body, b"b");
        body.push(255);
        match decode_dump_body(&body, 0).unwrap().value {
            RudisValue::ZSet(z) => assert_eq!(z.get_score(b"a"), Some(2.5)),
            other => panic!("{other:?}"),
        }
        assert!(decode_dump_body(&[TYPE_MODULE_2, 0], 0).is_err());
    }

    /// DUMP payloads from the Redis test suite (Redis 7.0, stream types 15
    /// and 19), which the legacy decoder special-cased.
    #[test]
    fn redis_suite_stream_payloads() {
        let p19: &[u8] = b"\x13\x01\x10\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x1D\x1D\x00\x00\x00\x0A\x00\x01\x01\x00\x01\x01\x01\x81\x66\x02\x00\x01\x02\x01\x00\x01\x00\x01\x81\x76\x02\x04\x01\xFF\x01\x01\x01\x01\x01\x00\x00\x01\x01\x01\x67\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\xF5\x5A\x71\xC7\x84\x01\x00\x00\x01\x01\x05\x41\x6C\x69\x63\x65\xF5\x5A\x71\xC7\x84\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x0B\x00\xA7\xA9\x14\xA5\x27\xFF\x9B\x9B";
        let n = p19.len();
        assert_eq!(
            crate::table::crc64(&p19[..n - 8]).to_le_bytes(),
            p19[n - 8..]
        );
        let RudisValue::Stream(s) = decode_dump_body(&p19[..n - 10], 0).unwrap().value else {
            panic!("not a stream");
        };
        assert_eq!(s.last_id, StreamId::new(1, 1));
        assert_eq!(s.entries_added, 1);
        assert_eq!(
            s.entries.get(&StreamId::new(1, 1)).unwrap(),
            &vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))]
        );
        let g = s.groups.get(&b"g"[..]).unwrap();
        assert_eq!(g.entries_read, Some(1));
        assert_eq!(g.pel.get(&StreamId::new(1, 1)).unwrap().consumer, "Alice");
        let alice = g.consumers.get(&b"Alice"[..]).unwrap();
        assert_eq!(alice.seen_time_ms, 1669793405685);
        assert_eq!(alice.active_time_ms, Some(1669793405685));
    }

    #[test]
    fn corrupt_payloads_never_panic() {
        let s = sample_stream();
        let mut payloads = vec![
            dump_value(&RudisValue::Stream(Box::new(s))).unwrap(),
            dump_value(&RudisValue::String(CompactStr::new(&[b'q'; 100]))).unwrap(),
        ];
        let lp = lp_of(|w| {
            w.str(b"a");
            w.int(1);
        });
        for t in [
            TYPE_HASH_LISTPACK,
            TYPE_ZSET_LISTPACK,
            TYPE_SET_LISTPACK,
            TYPE_LIST_ZIPLIST,
            TYPE_HASH_ZIPMAP,
            TYPE_SET_INTSET,
        ] {
            let mut p = vec![t];
            write_raw_string(&mut p, &lp);
            payloads.push(p);
        }
        let mut seed = 0x9E3779B97F4A7C15u64;
        for p in &payloads {
            let body = &p[..];
            for cut in 0..body.len() {
                let _ = decode_dump_body(&body[..cut], 0);
                let mut r = Reader::new(&body[..cut]);
                if let Ok(t) = r.u8() {
                    let _ = skip_value(t, &mut r);
                }
            }
            for _ in 0..2000 {
                let mut m = body.to_vec();
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let at = (seed as usize) % m.len();
                m[at] = (seed >> 32) as u8;
                let _ = decode_dump_body(&m, 0);
            }
        }
    }
}
