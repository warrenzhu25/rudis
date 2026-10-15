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

/// Header bytes before the pairs: pair count and pair-byte length (`u32`
/// each).
const HDR: usize = 8;

/// One exactly sized heap allocation, `[count: u32][len: u32][pairs]`, held
/// by an 8-byte pointer. A `Vec` plus count needed a separate 32-byte box
/// (`RudisValue` holds one pointer), so every small hash cost two
/// allocations and two pointer hops per lookup.
pub struct SmallHash {
    ptr: std::ptr::NonNull<u8>,
}

// SAFETY: `SmallHash` uniquely owns its allocation (like `Box<[u8]>`); no
// shared or thread-local state.
unsafe impl Send for SmallHash {}
// SAFETY: `&SmallHash` only allows reads of the owned bytes.
unsafe impl Sync for SmallHash {}

#[inline(always)]
fn layout(len: usize) -> std::alloc::Layout {
    std::alloc::Layout::from_size_align(HDR + len, 4).expect("small hash layout")
}

impl Drop for SmallHash {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated with `layout(byte_len)` and is not used
        // after this.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout(self.byte_len())) }
    }
}

impl Default for SmallHash {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SmallHash {
    fn clone(&self) -> Self {
        let mut h = SmallHash::new();
        h.resize(self.byte_len());
        h.bytes_mut().copy_from_slice(self.bytes());
        h.set_count(self.len());
        h
    }
}

impl PartialEq for SmallHash {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.bytes() == other.bytes()
    }
}

impl Eq for SmallHash {}

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
    /// An empty hash: just the header.
    pub fn new() -> Self {
        // SAFETY: the layout is non-zero sized (`HDR > 0`).
        let raw = unsafe { std::alloc::alloc(layout(0)) };
        let ptr = std::ptr::NonNull::new(raw)
            .unwrap_or_else(|| std::alloc::handle_alloc_error(layout(0)));
        let mut h = SmallHash { ptr };
        h.set_header(0, 0);
        h
    }

    #[inline(always)]
    fn header(&self) -> (u32, u32) {
        // SAFETY: the allocation always starts with `HDR` initialized bytes,
        // aligned to 4.
        unsafe {
            let p = self.ptr.as_ptr() as *const u32;
            (p.read(), p.add(1).read())
        }
    }

    #[inline(always)]
    fn set_header(&mut self, count: u32, len: u32) {
        // SAFETY: as in `header`; `&mut self` gives exclusive access.
        unsafe {
            let p = self.ptr.as_ptr() as *mut u32;
            p.write(count);
            p.add(1).write(len);
        }
    }

    #[inline(always)]
    fn byte_len(&self) -> usize {
        self.header().1 as usize
    }

    /// The packed pairs.
    #[inline(always)]
    fn bytes(&self) -> &[u8] {
        // SAFETY: the allocation holds `HDR + len` bytes, all initialized.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(HDR), self.byte_len()) }
    }

    #[inline(always)]
    fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `bytes`, with exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(HDR), self.byte_len()) }
    }

    /// Reallocates to exactly `new_len` pair bytes (count unchanged); new
    /// bytes are zeroed.
    fn resize(&mut self, new_len: usize) {
        let old_len = self.byte_len();
        if new_len == old_len {
            return;
        }
        let len32 = u32::try_from(new_len).expect("small hash under 4 GiB");
        // SAFETY: `ptr` was allocated with `layout(old_len)`; the new size is
        // non-zero. On success the first `HDR + min(old, new)` bytes are kept.
        let raw = unsafe { std::alloc::realloc(self.ptr.as_ptr(), layout(old_len), HDR + new_len) };
        self.ptr = std::ptr::NonNull::new(raw)
            .unwrap_or_else(|| std::alloc::handle_alloc_error(layout(new_len)));
        if new_len > old_len {
            // SAFETY: the bytes past the old length are inside the new allocation.
            unsafe {
                self.ptr
                    .as_ptr()
                    .add(HDR + old_len)
                    .write_bytes(0, new_len - old_len)
            };
        }
        let count = self.header().0;
        self.set_header(count, len32);
    }

    /// Replaces `range` of the pair bytes with `with` (a `Vec::splice`).
    fn replace_range(&mut self, range: std::ops::Range<usize>, with: &[u8]) {
        let old_len = self.byte_len();
        let new_len = old_len - range.len() + with.len();
        if new_len > old_len {
            self.resize(new_len);
            self.bytes_mut()
                .copy_within(range.end..old_len, range.start + with.len());
        } else {
            self.bytes_mut()
                .copy_within(range.end..old_len, range.start + with.len());
            self.resize(new_len);
        }
        self.bytes_mut()[range.start..range.start + with.len()].copy_from_slice(with);
    }

    #[inline(always)]
    fn set_count(&mut self, count: usize) {
        let len = self.header().1;
        self.set_header(count as u32, len);
    }

    /// Bytes this hash occupies on the heap (header included).
    pub fn heap_bytes(&self) -> usize {
        HDR + self.byte_len()
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
        self.header().0 as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.header().0 == 0
    }

    /// Byte offset and ranges of `field`'s pair.
    #[inline]
    fn find(&self, field: &[u8]) -> Option<(usize, Pair)> {
        let buf = self.bytes();
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
            .map(|(_, p)| &self.bytes()[p.value.0..p.value.1])
    }

    #[inline]
    pub fn contains(&self, field: &[u8]) -> bool {
        self.find(field).is_some()
    }

    /// Appends a pair whose field is known to be absent.
    pub fn push_new(&mut self, field: &[u8], value: &[u8]) {
        let at = self.byte_len();
        let (fl, vl) = (varint_len(field.len()), varint_len(value.len()));
        self.resize(at + fl + field.len() + vl + value.len());
        let mut enc = Vec::with_capacity(fl + vl);
        write_varint(&mut enc, field.len());
        let dst = &mut self.bytes_mut()[at..];
        dst[..fl].copy_from_slice(&enc);
        dst[fl..fl + field.len()].copy_from_slice(field);
        enc.clear();
        write_varint(&mut enc, value.len());
        let dst = &mut dst[fl + field.len()..];
        dst[..vl].copy_from_slice(&enc);
        dst[vl..].copy_from_slice(value);
        let n = self.len();
        self.set_count(n + 1);
    }

    /// Sets `field` to `value`; true if the field was added.
    pub fn set(&mut self, field: &[u8], value: &[u8]) -> bool {
        match self.find(field) {
            Some((_, p)) => {
                if p.value.1 - p.value.0 == value.len() {
                    self.bytes_mut()[p.value.0..p.value.1].copy_from_slice(value);
                } else {
                    let mut enc = Vec::with_capacity(varint_len(value.len()) + value.len());
                    write_varint(&mut enc, value.len());
                    enc.extend_from_slice(value);
                    self.replace_range(p.vlen_at..p.value.1, &enc);
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
        let v = Bytes::copy_from_slice(&self.bytes()[p.value.0..p.value.1]);
        self.replace_range(off..p.value.1, &[]);
        let n = self.len();
        self.set_count(n - 1);
        Some(v)
    }

    /// Removes `field`; true if it was present.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        match self.find(field) {
            Some((off, p)) => {
                self.replace_range(off..p.value.1, &[]);
                let n = self.len();
                self.set_count(n - 1);
                true
            }
            None => false,
        }
    }

    /// Pairs in insertion order.
    #[inline]
    pub fn iter(&self) -> SmallHashIter<'_> {
        SmallHashIter {
            buf: self.bytes(),
            off: 0,
            left: self.len(),
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
        let mut m = crate::table::RudisHashMap::with_capacity(self.len());
        for (k, v) in self.iter() {
            m.insert(k, v);
        }
        m
    }

    /// Allocations are always exact; kept for callers that shrink values.
    pub fn shrink_to_fit(&mut self) {}

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

    /// The single exactly sized allocation is reallocated on every change:
    /// growing and shrinking values in the middle, removals at both ends, and
    /// clones must all keep the bytes and header consistent.
    #[test]
    fn thin_allocation_survives_resizes_in_place_and_clones() {
        let mut h = SmallHash::new();
        assert!(h.is_empty() && h.heap_bytes() == HDR);
        let mut model: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut x: u64 = 7;
        for round in 0..2000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let f = format!("f{}", x % 24).into_bytes();
            let vlen = ((x >> 8) % 200) as usize; // crosses the 1/2-byte varint edge
            let v = vec![b'a' + (round % 26) as u8; vlen];
            match (x >> 20) % 4 {
                0 | 1 => {
                    h.set(&f, &v);
                    match model.iter_mut().find(|(k, _)| *k == f) {
                        Some(e) => e.1 = v,
                        None => model.push((f, v)),
                    }
                }
                2 => {
                    let got = h.take(&f).map(|b| b.to_vec());
                    let pos = model.iter().position(|(k, _)| *k == f);
                    assert_eq!(got, pos.map(|i| model.remove(i).1));
                }
                _ => {
                    let c = h.clone();
                    assert_eq!(c, h);
                    h = c;
                }
            }
            let pairs: Vec<(Vec<u8>, Vec<u8>)> =
                h.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
            assert_eq!(pairs, model, "round {round}");
            assert_eq!(h.len(), model.len());
            let enc: usize = model
                .iter()
                .map(|(k, v)| varint_len(k.len()) + k.len() + varint_len(v.len()) + v.len())
                .sum();
            assert_eq!(h.heap_bytes(), HDR + enc);
        }
    }

    #[test]
    fn packs_ten_short_pairs_into_one_small_buffer() {
        let mut h = SmallHash::default();
        for i in 0..10 {
            h.set(format!("f{i:04}").as_bytes(), format!("v{i:04}").as_bytes());
        }
        // 10 pairs x (1 + 5 + 1 + 5) bytes, no per-pair handles.
        assert_eq!(h.byte_len(), 120);
        assert_eq!(h.heap_bytes(), 128);
        assert_eq!(std::mem::size_of::<SmallHash>(), 8);
        assert_eq!(h.get(b"f0007"), Some(&b"v0007"[..]));
        assert!(!h.contains(b"f0010"));
    }
}
