//! Small-hash storage: field/value pairs packed back to back in one buffer,
//! like Redis's listpack.
//!
//! The previous `Vec<(Bytes, Bytes)>` spent 64 bytes of handles per pair plus
//! two heap allocations (or two views pinning a whole request buffer). Here a
//! pair is `[varint flen][field][varint vlen][value]`: a 5-byte field and
//! 5-byte value cost 12 bytes, in a single allocation per hash.
//!
//! Lookups scan the buffer linearly, as with the `Vec` before; small hashes
//! (up to `hash-max-listpack-entries`) keep the scan short and cache-local.

use bytes::Bytes;

#[inline]
fn write_varint(buf: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

#[inline]
fn varint_len(n: usize) -> usize {
    match n {
        0..=0x7F => 1,
        0x80..=0x3FFF => 2,
        0x4000..=0x1F_FFFF => 3,
        0x20_0000..=0xFFF_FFFF => 4,
        _ => 5,
    }
}

/// (value, bytes used) of the varint at `off`.
#[inline(always)]
fn read_varint(buf: &[u8], off: usize) -> (usize, usize) {
    let b = buf[off];
    if b < 0x80 {
        return (b as usize, 1);
    }
    let mut n = (b & 0x7F) as usize;
    let mut shift = 7;
    let mut i = off + 1;
    loop {
        let b = buf[i];
        n |= ((b & 0x7F) as usize) << shift;
        i += 1;
        if b & 0x80 == 0 {
            return (n, i - off);
        }
        shift += 7;
    }
}

/// Byte ranges of one pair: field, value (with its length prefix at
/// `vlen_at`), and the end of the pair.
#[derive(Clone, Copy)]
struct Pair {
    field: (usize, usize),
    vlen_at: usize,
    value: (usize, usize),
}

#[inline(always)]
fn parse_pair(buf: &[u8], off: usize) -> Pair {
    let (flen, n) = read_varint(buf, off);
    let fstart = off + n;
    let vlen_at = fstart + flen;
    let (vlen, m) = read_varint(buf, vlen_at);
    let vstart = vlen_at + m;
    Pair {
        field: (fstart, fstart + flen),
        vlen_at,
        value: (vstart, vstart + vlen),
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct SmallHash {
    buf: Vec<u8>,
    len: u32,
}

impl std::fmt::Debug for SmallHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(
                self.iter()
                    .map(|(k, v)| (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))),
            )
            .finish()
    }
}

impl SmallHash {
    /// An empty hash reusing `buf`'s allocation.
    pub fn with_buf(mut buf: Vec<u8>) -> Self {
        buf.clear();
        SmallHash { buf, len: 0 }
    }

    /// The buffer, for recycling.
    pub fn into_buf(self) -> Vec<u8> {
        self.buf
    }

    /// Builds a hash from pairs; a repeated field keeps its last value.
    pub fn from_pairs<'a, I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
    {
        let mut h = SmallHash::default();
        for (f, v) in pairs {
            h.set(f, v);
        }
        h
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Byte offset and ranges of `field`'s pair.
    #[inline]
    fn find(&self, field: &[u8]) -> Option<(usize, Pair)> {
        let buf = &self.buf[..];
        let mut off = 0;
        while off < buf.len() {
            let p = parse_pair(buf, off);
            if &buf[p.field.0..p.field.1] == field {
                return Some((off, p));
            }
            off = p.value.1;
        }
        None
    }

    #[inline]
    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        self.find(field)
            .map(|(_, p)| &self.buf[p.value.0..p.value.1])
    }

    #[inline]
    pub fn contains(&self, field: &[u8]) -> bool {
        self.find(field).is_some()
    }

    /// Appends a pair whose field is known to be absent.
    pub fn push_new(&mut self, field: &[u8], value: &[u8]) {
        self.buf.reserve_exact(
            varint_len(field.len()) + field.len() + varint_len(value.len()) + value.len(),
        );
        write_varint(&mut self.buf, field.len());
        self.buf.extend_from_slice(field);
        write_varint(&mut self.buf, value.len());
        self.buf.extend_from_slice(value);
        self.len += 1;
    }

    /// Sets `field` to `value`; true if the field was added.
    pub fn set(&mut self, field: &[u8], value: &[u8]) -> bool {
        match self.find(field) {
            Some((_, p)) => {
                if p.value.1 - p.value.0 == value.len() {
                    self.buf[p.value.0..p.value.1].copy_from_slice(value);
                } else {
                    let mut enc = Vec::with_capacity(varint_len(value.len()) + value.len());
                    write_varint(&mut enc, value.len());
                    enc.extend_from_slice(value);
                    self.buf.splice(p.vlen_at..p.value.1, enc);
                }
                false
            }
            None => {
                self.push_new(field, value);
                true
            }
        }
    }

    /// Removes `field`, returning its value.
    pub fn take(&mut self, field: &[u8]) -> Option<Bytes> {
        let (off, p) = self.find(field)?;
        let v = Bytes::copy_from_slice(&self.buf[p.value.0..p.value.1]);
        self.buf.drain(off..p.value.1);
        self.len -= 1;
        Some(v)
    }

    /// Removes `field`; true if it was present.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        match self.find(field) {
            Some((off, p)) => {
                self.buf.drain(off..p.value.1);
                self.len -= 1;
                true
            }
            None => false,
        }
    }

    /// Pairs in insertion order.
    #[inline]
    pub fn iter(&self) -> SmallHashIter<'_> {
        SmallHashIter {
            buf: &self.buf,
            off: 0,
            left: self.len as usize,
        }
    }

    /// Owned copies of the pairs.
    pub fn to_vec(&self) -> Vec<(Bytes, Bytes)> {
        self.iter()
            .map(|(k, v)| (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v)))
            .collect()
    }

    /// The pairs as a full hash map (promotion past the small limits).
    pub fn to_map(&self) -> crate::table::RudisHashMap {
        let mut m =
            crate::table::RudisHashMap::with_capacity_and_hasher(self.len(), Default::default());
        for (k, v) in self.iter() {
            m.insert(Bytes::copy_from_slice(k), Bytes::copy_from_slice(v));
        }
        m
    }

    pub fn shrink_to_fit(&mut self) {
        self.buf.shrink_to_fit();
    }

    /// Bytes of payload (fields and values, without length prefixes).
    pub fn payload_bytes(&self) -> usize {
        self.iter().map(|(k, v)| k.len() + v.len()).sum()
    }
}

impl From<Vec<(Bytes, Bytes)>> for SmallHash {
    fn from(pairs: Vec<(Bytes, Bytes)>) -> Self {
        SmallHash::from_pairs(pairs.iter().map(|(f, v)| (f.as_ref(), v.as_ref())))
    }
}

pub struct SmallHashIter<'a> {
    buf: &'a [u8],
    off: usize,
    left: usize,
}

impl<'a> Iterator for SmallHashIter<'a> {
    type Item = (&'a [u8], &'a [u8]);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.off >= self.buf.len() {
            return None;
        }
        let p = parse_pair(self.buf, self.off);
        self.off = p.value.1;
        self.left -= 1;
        Some((
            &self.buf[p.field.0..p.field.1],
            &self.buf[p.value.0..p.value.1],
        ))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for SmallHashIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn matches_a_map_model_under_random_operations() {
        let mut h = SmallHash::default();
        let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut order: Vec<Vec<u8>> = Vec::new();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for step in 0..50_000 {
            let r = next();
            let f = format!("f{}", r % 40).into_bytes();
            // Values from empty to past one varint byte (>127) so updates
            // change the length prefix's size.
            let vlen = match (r >> 8) % 4 {
                0 => 0,
                1 => 5,
                2 => 127,
                _ => 300,
            };
            let v: Vec<u8> = (0..vlen).map(|i| (i as u8) ^ (r >> 16) as u8).collect();
            match (r >> 32) % 5 {
                0..=2 => {
                    let added = h.set(&f, &v);
                    assert_eq!(added, !model.contains_key(&f), "step {step}");
                    if added {
                        order.push(f.clone());
                    }
                    model.insert(f.clone(), v);
                }
                3 => {
                    assert_eq!(h.take(&f).map(|b| b.to_vec()), model.remove(&f));
                    order.retain(|k| *k != f);
                }
                _ => {
                    assert_eq!(h.remove(&f), model.remove(&f).is_some());
                    order.retain(|k| *k != f);
                }
            }
            assert_eq!(h.len(), model.len());
            assert_eq!(h.get(&f).map(<[u8]>::to_vec), model.get(&f).cloned());
            if step % 977 == 0 {
                let got: Vec<(Vec<u8>, Vec<u8>)> =
                    h.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
                let want: Vec<(Vec<u8>, Vec<u8>)> = order
                    .iter()
                    .map(|k| (k.clone(), model[k].clone()))
                    .collect();
                assert_eq!(got, want, "insertion order at step {step}");
                assert_eq!(h.iter().len(), model.len());
                let m = h.to_map();
                assert_eq!(m.len(), model.len());
            }
        }
    }

    #[test]
    fn packs_ten_short_pairs_into_one_small_buffer() {
        let mut h = SmallHash::default();
        for i in 0..10 {
            h.set(format!("f{i:04}").as_bytes(), format!("v{i:04}").as_bytes());
        }
        // 10 pairs x (1 + 5 + 1 + 5) bytes, no per-pair handles.
        assert_eq!(h.buf.len(), 120);
        assert_eq!(h.get(b"f0007"), Some(&b"v0007"[..]));
        assert!(!h.contains(b"f0010"));
    }
}
