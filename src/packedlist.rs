//! Packed list storage: elements stored back to back in byte chunks.
//!
//! A `VecDeque` of 32-byte element handles costs 32 bytes per element however
//! short it is. Here elements of up to [`PACK_MAX`] bytes are packed into
//! chunks holding at most [`CHUNK_MAX`] bytes, each entry encoded as
//! `[varint len][bytes][reversed varint len]` (the trailing length lets the
//! chunk be walked backwards). Longer elements get a chunk of their own
//! holding the `Bytes` they arrived in, shared rather than copied.
//!
//! A packed chunk is a small byte deque: its live bytes are `buf[head..]`.
//! Pushing at the back appends; pushing at the front writes into the gap
//! before `head`, growing the gap (by moving the live bytes once) only when
//! it runs out, so neither end ever shifts the whole chunk per push. Popping
//! the front just advances `head`. A list's first chunk grows by a quarter
//! (at least four entries) at a time, so small lists stay small; later
//! chunks are allocated at full size, so long lists never reallocate.
//!
//! - Push/pop at either end: amortized O(1), no per-element allocation.
//! - Index access: walks chunk element counts from the nearer end, then scans
//!   within one chunk (like Redis's quicklist).
//! - Insert/remove in the middle: a `memmove` of at most one chunk.

use bytes::Bytes;
use std::collections::VecDeque;

/// Longest element stored packed; longer ones get their own chunk, sharing
/// the `Bytes` they arrived in. Packing 128-byte values too saved ~25% of
/// their memory but the copy cost LPUSH/RPUSH ~7% throughput.
pub const PACK_MAX: usize = 63;
/// Most live bytes in one packed chunk: with its 4-byte header a full chunk
/// is exactly 4096 bytes, an allocator size class (4100 would round up to
/// 5120, wasting a fifth of every full chunk).
pub const CHUNK_MAX: usize = 4096 - HDR;
/// Smallest gap or growth step, so tiny lists don't reallocate per push.
const MIN_STEP: usize = 32;

#[inline]
fn varint_len(n: usize) -> usize {
    match n {
        0..=0x7F => 1,
        0x80..=0x3FFF => 2,
        0x4000..=0x1F_FFFF => 3,
        _ => 4,
    }
}

#[inline]
fn entry_size(len: usize) -> usize {
    2 * varint_len(len) + len
}

/// LEB128 bytes of `n`: (bytes, count).
#[inline]
fn leb(mut n: usize) -> ([u8; 5], usize) {
    let mut out = [0u8; 5];
    let mut i = 0;
    while n >= 0x80 {
        out[i] = (n as u8) | 0x80;
        n >>= 7;
        i += 1;
    }
    out[i] = n as u8;
    (out, i + 1)
}

/// Writes the entry for `v` into `dst` (exactly `entry_size(v.len())` long).
#[inline]
fn encode_to(dst: &mut [u8], v: &[u8]) {
    let (l, n) = leb(v.len());
    dst[..n].copy_from_slice(&l[..n]);
    dst[n..n + v.len()].copy_from_slice(v);
    let tail = &mut dst[n + v.len()..];
    for k in 0..n {
        tail[k] = l[n - 1 - k];
    }
}

/// (length, bytes used) of the forward varint at `off`.
#[inline(always)]
fn read_len(buf: &[u8], off: usize) -> (usize, usize) {
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

/// (length, bytes used) of the reversed varint ending at `end`.
#[inline(always)]
fn read_len_back(buf: &[u8], end: usize) -> (usize, usize) {
    let mut n = 0usize;
    let mut shift = 0;
    let mut i = end;
    loop {
        i -= 1;
        let b = buf[i];
        n |= ((b & 0x7F) as usize) << shift;
        if b & 0x80 == 0 {
            return (n, end - i);
        }
        shift += 7;
    }
}

/// The element of the entry starting at `off`, and the entry's end.
#[inline(always)]
fn entry_at(buf: &[u8], off: usize) -> (&[u8], usize) {
    let (len, n) = read_len(buf, off);
    let start = off + n;
    (&buf[start..start + len], start + len + n)
}

/// The element of the entry ending at `end`, and the entry's start.
#[inline(always)]
fn entry_before(buf: &[u8], end: usize) -> (&[u8], usize) {
    let (len, n) = read_len_back(buf, end);
    let stop = end - n;
    (&buf[stop - len..stop], stop - len - n)
}

/// Bytes of the per-chunk header at the start of `buf`: `[count: u16 LE]
/// [head: u16 LE]`. Keeping them in the buffer leaves `Packed` a bare `Vec`
/// (24 bytes), which fits beside `Bytes` in a 32-byte [`Chunk`] without a box.
const HDR: usize = 4;

/// Entries `[varint len][bytes][rev varint len]`, live in `buf[head..]`
/// after a [`HDR`]-byte header holding the entry count and `head`.
#[derive(Clone, Debug)]
struct Packed {
    buf: Vec<u8>,
}

impl Packed {
    fn with_buf(mut buf: Vec<u8>, head: usize, count: usize) -> Self {
        if buf.len() < HDR {
            buf.resize(HDR, 0);
        }
        let mut p = Packed { buf };
        p.set_head(head);
        p.set_count(count);
        p
    }

    fn new() -> Self {
        Self::with_buf(Vec::with_capacity(HDR), HDR, 0)
    }

    /// A full-size chunk to be filled by back pushes.
    fn full_for_back() -> Self {
        Self::with_buf(Vec::with_capacity(HDR + CHUNK_MAX), HDR, 0)
    }

    /// A full-size chunk to be filled by front pushes: all gap.
    fn full_for_front() -> Self {
        Self::with_buf(vec![0; HDR + CHUNK_MAX], HDR + CHUNK_MAX, 0)
    }

    #[inline(always)]
    fn count(&self) -> usize {
        u16::from_le_bytes([self.buf[0], self.buf[1]]) as usize
    }

    #[inline(always)]
    fn set_count(&mut self, n: usize) {
        self.buf[..2].copy_from_slice(&(n as u16).to_le_bytes());
    }

    #[inline(always)]
    fn head(&self) -> usize {
        u16::from_le_bytes([self.buf[2], self.buf[3]]) as usize
    }

    #[inline(always)]
    fn set_head(&mut self, h: usize) {
        debug_assert!(h <= u16::MAX as usize);
        self.buf[2..4].copy_from_slice(&(h as u16).to_le_bytes());
    }

    #[inline]
    fn live_len(&self) -> usize {
        self.buf.len() - self.head()
    }

    #[inline]
    fn fits(&self, need: usize) -> bool {
        self.live_len() + need <= CHUNK_MAX
    }

    fn push_back(&mut self, v: &[u8]) {
        let need = entry_size(v.len());
        let head = self.head();
        if self.buf.len() + need > self.buf.capacity() && (head - HDR) * 2 >= self.buf.len() - HDR {
            // Mostly popped from the front (a queue): reclaim the dead front
            // instead of growing.
            self.buf.copy_within(head.., HDR);
            self.buf.truncate(self.buf.len() - (head - HDR));
            self.set_head(HDR);
        }
        if self.buf.len() + need > HDR + CHUNK_MAX {
            // The front gap would push the chunk past its 4096-byte size
            // class: slide the live bytes down over the gap instead
            // (`fits` guarantees they and `v` fit in `CHUNK_MAX`).
            let head = self.head();
            self.buf.copy_within(head.., HDR);
            self.buf.truncate(self.buf.len() - (head - HDR));
            self.set_head(HDR);
        }
        let len = self.buf.len();
        if len + need > self.buf.capacity() {
            let cap = self.buf.capacity();
            let want = (len + need).max(cap + (cap / 4).max(4 * need).max(MIN_STEP));
            let want = want.min(HDR + CHUNK_MAX).max(len + need);
            self.buf.reserve_exact(want - len);
        }
        self.buf.resize(len + need, 0);
        encode_to(&mut self.buf[len..], v);
        let n = self.count();
        self.set_count(n + 1);
    }

    fn push_front(&mut self, v: &[u8]) {
        let need = entry_size(v.len());
        if self.head() < HDR + need {
            // Re-lay the live bytes behind a fresh gap: a quarter of the live
            // size (at least four entries), never past the chunk limit.
            let live = self.live_len();
            let gap = need
                .max((live / 4).max(4 * need).max(MIN_STEP))
                .min(CHUNK_MAX - live);
            let mut buf = Vec::with_capacity(HDR + gap + live);
            buf.extend_from_slice(&self.buf[..HDR]);
            buf.resize(HDR + gap, 0);
            buf.extend_from_slice(&self.buf[self.head()..]);
            self.buf = buf;
            self.set_head(HDR + gap);
        }
        let at = self.head() - need;
        encode_to(&mut self.buf[at..at + need], v);
        self.set_head(at);
        let n = self.count();
        self.set_count(n + 1);
    }

    fn pop_front(&mut self) -> Bytes {
        let (v, end) = entry_at(&self.buf, self.head());
        let v = Bytes::copy_from_slice(v);
        self.set_head(end);
        let n = self.count();
        self.set_count(n - 1);
        v
    }

    fn pop_back(&mut self) -> Bytes {
        let (v, start) = entry_before(&self.buf, self.buf.len());
        let v = Bytes::copy_from_slice(v);
        self.buf.truncate(start);
        let n = self.count();
        self.set_count(n - 1);
        v
    }

    /// Absolute offset of entry `j`.
    fn offset_of(&self, j: usize) -> usize {
        let mut off = self.head();
        for _ in 0..j {
            off = entry_at(&self.buf, off).1;
        }
        off
    }

    fn get(&self, j: usize) -> &[u8] {
        entry_at(&self.buf, self.offset_of(j)).0
    }

    fn insert_at(&mut self, j: usize, v: &[u8]) {
        if j == 0 {
            return self.push_front(v);
        }
        if j == self.count() {
            return self.push_back(v);
        }
        let off = self.offset_of(j);
        let need = entry_size(v.len());
        let mut enc = vec![0u8; need];
        encode_to(&mut enc, v);
        self.buf.splice(off..off, enc);
        let n = self.count();
        self.set_count(n + 1);
    }

    fn remove_at(&mut self, j: usize) -> Bytes {
        let off = self.offset_of(j);
        let (v, end) = entry_at(&self.buf, off);
        let v = Bytes::copy_from_slice(v);
        if off == self.head() {
            self.set_head(end);
        } else {
            self.buf.drain(off..end);
        }
        let n = self.count();
        self.set_count(n - 1);
        v
    }

    /// Splits off entries `j..` into a new chunk.
    fn split_off(&mut self, j: usize) -> Packed {
        let off = self.offset_of(j);
        let mut tail = Vec::with_capacity(HDR + self.buf.len() - off);
        tail.resize(HDR, 0);
        tail.extend_from_slice(&self.buf[off..]);
        self.buf.truncate(off);
        let tail_count = self.count() - j;
        self.set_count(j);
        Packed::with_buf(tail, HDR, tail_count)
    }
}

/// Packed contents are a bare `Vec` (count and head live in its header), so
/// a chunk stays 32 bytes with no box: a push touches the queue slot and the
/// buffer, nothing in between, and a long-value list (a chunk per element)
/// pays 32 bytes per element, not 40.
#[derive(Clone, Debug)]
enum Chunk {
    Packed(Packed),
    /// One long element, shared with whoever else holds it.
    Big(Bytes),
}

const _: () = assert!(std::mem::size_of::<Chunk>() == 32);

impl Chunk {
    #[inline]
    fn count(&self) -> usize {
        match self {
            Chunk::Packed(p) => p.count(),
            Chunk::Big(_) => 1,
        }
    }

    fn new_packed() -> Chunk {
        Chunk::Packed(Packed::new())
    }
}

#[derive(Clone, Debug, Default)]
pub struct PackedList {
    chunks: VecDeque<Chunk>,
    len: usize,
}

impl PartialEq for PackedList {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}

impl Eq for PackedList {}

impl PackedList {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.chunks.clear();
        self.len = 0;
    }

    /// Reserves chunk slots for pushing `values` (one per long value, plus
    /// enough packed chunks for the short ones), so a bulk push does not
    /// leave the chunk queue doubled past what it holds.
    pub fn reserve_for(&mut self, values: &[Bytes]) {
        let (mut big, mut packed_bytes) = (0usize, 0usize);
        for v in values {
            if v.len() > PACK_MAX {
                big += 1;
            } else {
                packed_bytes += entry_size(v.len());
            }
        }
        let needed = big + packed_bytes.div_ceil(CHUNK_MAX) + 1;
        let free = self.chunks.capacity() - self.chunks.len();
        if needed > free {
            self.chunks.reserve_exact(needed - free);
        }
    }

    fn push_back_impl(&mut self, v: &[u8], shared: Option<&Bytes>) {
        self.len += 1;
        if v.len() > PACK_MAX {
            let b = shared.cloned().unwrap_or_else(|| Bytes::copy_from_slice(v));
            self.chunks.push_back(Chunk::Big(b));
            return;
        }
        let need = entry_size(v.len());
        if !matches!(self.chunks.back(), Some(Chunk::Packed(p)) if p.fits(need)) {
            // A list's first chunk grows gradually (small lists stay small);
            // later ones are allocated full, so long lists never reallocate.
            let p = if self.chunks.is_empty() {
                Packed::new()
            } else {
                Packed::full_for_back()
            };
            self.chunks.push_back(Chunk::Packed(p));
        }
        if let Some(Chunk::Packed(p)) = self.chunks.back_mut() {
            p.push_back(v);
        }
    }

    fn push_front_impl(&mut self, v: &[u8], shared: Option<&Bytes>) {
        self.len += 1;
        if v.len() > PACK_MAX {
            let b = shared.cloned().unwrap_or_else(|| Bytes::copy_from_slice(v));
            self.chunks.push_front(Chunk::Big(b));
            return;
        }
        let need = entry_size(v.len());
        if !matches!(self.chunks.front(), Some(Chunk::Packed(p)) if p.fits(need)) {
            let p = if self.chunks.is_empty() {
                Packed::new()
            } else {
                Packed::full_for_front()
            };
            self.chunks.push_front(Chunk::Packed(p));
        }
        if let Some(Chunk::Packed(p)) = self.chunks.front_mut() {
            p.push_front(v);
        }
    }

    /// Appends `v`, sharing it when it is stored unpacked.
    #[inline]
    pub fn push_back(&mut self, v: &Bytes) {
        self.push_back_impl(v, Some(v));
    }

    #[inline]
    pub fn push_front(&mut self, v: &Bytes) {
        self.push_front_impl(v, Some(v));
    }

    /// Appends a copy of `v`.
    #[inline]
    pub fn push_back_slice(&mut self, v: &[u8]) {
        self.push_back_impl(v, None);
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        let out = match self.chunks.front_mut()? {
            Chunk::Big(_) => match self.chunks.pop_front() {
                Some(Chunk::Big(b)) => b,
                _ => unreachable!(),
            },
            Chunk::Packed(p) => {
                let v = p.pop_front();
                if p.count() == 0 {
                    self.chunks.pop_front();
                }
                v
            }
        };
        self.len -= 1;
        Some(out)
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        let out = match self.chunks.back_mut()? {
            Chunk::Big(_) => match self.chunks.pop_back() {
                Some(Chunk::Big(b)) => b,
                _ => unreachable!(),
            },
            Chunk::Packed(p) => {
                let v = p.pop_back();
                if p.count() == 0 {
                    self.chunks.pop_back();
                }
                v
            }
        };
        self.len -= 1;
        Some(out)
    }

    /// Chunk index and index within it of element `i` (< len).
    fn locate(&self, i: usize) -> (usize, usize) {
        if i < self.len / 2 {
            let mut i = i;
            for (ci, c) in self.chunks.iter().enumerate() {
                let n = c.count();
                if i < n {
                    return (ci, i);
                }
                i -= n;
            }
        } else {
            let mut from_end = self.len - 1 - i;
            for (ci, c) in self.chunks.iter().enumerate().rev() {
                let n = c.count();
                if from_end < n {
                    return (ci, n - 1 - from_end);
                }
                from_end -= n;
            }
        }
        unreachable!("index {i} out of bounds for len {}", self.len)
    }

    pub fn get(&self, i: usize) -> Option<&[u8]> {
        if i >= self.len {
            return None;
        }
        let (ci, j) = self.locate(i);
        Some(match &self.chunks[ci] {
            Chunk::Big(b) => b,
            Chunk::Packed(p) => p.get(j),
        })
    }

    /// An owned copy (or shared handle, for unpacked elements) of element `i`.
    pub fn get_bytes(&self, i: usize) -> Option<Bytes> {
        if i >= self.len {
            return None;
        }
        let (ci, j) = self.locate(i);
        Some(match &self.chunks[ci] {
            Chunk::Big(b) => b.clone(),
            Chunk::Packed(p) => Bytes::copy_from_slice(p.get(j)),
        })
    }

    /// Removes element `i` (< len) and returns it.
    pub fn remove(&mut self, i: usize) -> Option<Bytes> {
        if i >= self.len {
            return None;
        }
        let (ci, j) = self.locate(i);
        let out = match &mut self.chunks[ci] {
            Chunk::Big(_) => match self.chunks.remove(ci) {
                Some(Chunk::Big(b)) => b,
                _ => unreachable!(),
            },
            Chunk::Packed(p) => {
                let v = p.remove_at(j);
                if p.count() == 0 {
                    self.chunks.remove(ci);
                }
                v
            }
        };
        self.len -= 1;
        Some(out)
    }

    /// Inserts `v` before element `i` (`i == len` appends).
    pub fn insert(&mut self, i: usize, v: &Bytes) {
        if i == 0 {
            return self.push_front(v);
        }
        if i >= self.len {
            return self.push_back(v);
        }
        let (ci, j) = self.locate(i);
        if v.len() > PACK_MAX {
            // Split the chunk at `j` (if packed and j > 0) and put a Big chunk
            // between the halves.
            let at = self.split_at(ci, j);
            self.chunks.insert(at, Chunk::Big(v.clone()));
            self.len += 1;
            return;
        }
        let need = entry_size(v.len());
        // Insert into chunk `ci` at `j`, or at the end of the previous packed
        // chunk when `j == 0`; split a full chunk first.
        let (ci, j) = match &self.chunks[ci] {
            Chunk::Big(_) if ci > 0 && matches!(self.chunks[ci - 1], Chunk::Packed(_)) => {
                (ci - 1, self.chunks[ci - 1].count())
            }
            Chunk::Big(_) => {
                self.chunks.insert(ci, Chunk::new_packed());
                (ci, 0)
            }
            Chunk::Packed(_) => (ci, j),
        };
        let full = matches!(&self.chunks[ci], Chunk::Packed(p) if !p.fits(need));
        let (ci, j) = if full {
            let half = self.chunks[ci].count() / 2;
            let at = self.split_at(ci, half);
            if j < half { (ci, j) } else { (at, j - half) }
        } else {
            (ci, j)
        };
        if matches!(&self.chunks[ci], Chunk::Packed(p) if !p.fits(need)) {
            // Halves by count can still be too full when sizes are uneven
            // (a few PACK_MAX elements among short ones): give `v` a chunk
            // of its own at its position.
            let at = self.split_at(ci, j);
            let mut p = Packed::new();
            p.push_back(v);
            self.chunks.insert(at, Chunk::Packed(p));
        } else if let Chunk::Packed(p) = &mut self.chunks[ci] {
            p.insert_at(j, v);
        }
        self.len += 1;
    }

    /// Splits packed chunk `ci` before its element `j`, returning the index
    /// where the second half (or, for `j == 0`/Big chunks, chunk `ci`) is.
    fn split_at(&mut self, ci: usize, j: usize) -> usize {
        match &mut self.chunks[ci] {
            Chunk::Packed(p) if j > 0 && j < p.count() => {
                let tail = p.split_off(j);
                self.chunks.insert(ci + 1, Chunk::Packed(tail));
                ci + 1
            }
            Chunk::Packed(p) if j >= p.count() => ci + 1,
            _ => ci,
        }
    }

    /// Replaces element `i` (< len) with `v`.
    pub fn set(&mut self, i: usize, v: &Bytes) {
        self.remove(i);
        self.insert(i, v);
    }

    /// Keeps only elements `start..=stop` (already clamped by the caller).
    pub fn retain_range(&mut self, start: usize, stop: usize) {
        if start == 0 && stop + 1 >= self.len {
            return;
        }
        for _ in (stop + 1)..self.len {
            self.pop_back();
        }
        for _ in 0..start {
            self.pop_front();
        }
    }

    /// Keeps the elements for which `keep` is true, in order.
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8]) -> bool) {
        let mut out = PackedList::new();
        for c in std::mem::take(&mut self.chunks) {
            match c {
                Chunk::Big(b) => {
                    if keep(&b) {
                        out.chunks.push_back(Chunk::Big(b));
                        out.len += 1;
                    }
                }
                c @ Chunk::Packed(_) => {
                    for v in ChunkIter::new(&c) {
                        if keep(v) {
                            out.push_back_slice(v);
                        }
                    }
                }
            }
        }
        *self = out;
    }

    /// Elements in order, borrowed.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &[u8]> + '_ {
        self.chunks.iter().flat_map(ChunkIter::new)
    }

    /// Elements from index `start` onwards (empty when `start >= len`),
    /// jumping straight to the chunk that holds it.
    pub fn iter_from(&self, start: usize) -> impl Iterator<Item = &[u8]> + '_ {
        let (ci, j) = if start < self.len {
            self.locate(start)
        } else {
            (self.chunks.len(), 0)
        };
        self.chunks
            .range(ci.min(self.chunks.len())..)
            .enumerate()
            .flat_map(move |(k, c)| ChunkIter::new(c).skip(if k == 0 { j } else { 0 }))
    }
}

impl FromIterator<Bytes> for PackedList {
    fn from_iter<I: IntoIterator<Item = Bytes>>(it: I) -> Self {
        let mut l = PackedList::new();
        for b in it {
            l.push_back(&b);
        }
        l
    }
}

/// Iterator over one chunk's elements, from either end.
struct ChunkIter<'a> {
    /// The packed buffer, or the element itself for a Big chunk.
    buf: &'a [u8],
    big: bool,
    front: usize,
    back: usize,
    remaining: usize,
}

impl<'a> ChunkIter<'a> {
    fn new(c: &'a Chunk) -> Self {
        match c {
            Chunk::Big(b) => ChunkIter {
                buf: b,
                big: true,
                front: 0,
                back: 0,
                remaining: 1,
            },
            Chunk::Packed(p) => ChunkIter {
                buf: &p.buf,
                big: false,
                front: p.head(),
                back: p.buf.len(),
                remaining: p.count(),
            },
        }
    }
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        if self.big {
            return Some(self.buf);
        }
        let (v, end) = entry_at(self.buf, self.front);
        self.front = end;
        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a> DoubleEndedIterator for ChunkIter<'a> {
    fn next_back(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        if self.big {
            return Some(self.buf);
        }
        let (v, start) = entry_before(self.buf, self.back);
        self.back = start;
        Some(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &[u8]) -> Bytes {
        Bytes::copy_from_slice(s)
    }

    fn contents(l: &PackedList) -> Vec<Vec<u8>> {
        l.iter().map(<[u8]>::to_vec).collect()
    }

    #[test]
    fn short_and_medium_elements_pack_and_long_ones_are_shared() {
        let mut l = PackedList::new();
        for i in 0..200 {
            l.push_back(&b(format!("e{i}").as_bytes()));
        }
        // 200 short entries fit in one chunk.
        assert_eq!(l.chunks.len(), 1);
        // PACK_MAX-byte elements pack: ~60 per 4 KiB chunk.
        let mut m = PackedList::new();
        for i in 0..300u32 {
            m.push_front(&Bytes::from(vec![i as u8; PACK_MAX]));
        }
        assert!(m.chunks.len() <= 6, "{} chunks", m.chunks.len());
        assert_eq!(m.get(0), Some(&[43u8; PACK_MAX][..]));
        let long = Bytes::from(vec![b'L'; PACK_MAX + 1]);
        l.push_front(&long);
        assert!(matches!(l.chunks.front(), Some(Chunk::Big(x)) if x.as_ptr() == long.as_ptr()));
        assert_eq!(
            l.pop_front().unwrap().as_ptr(),
            long.as_ptr(),
            "long values are not copied"
        );
        assert_eq!(l.get(0), Some(&b"e0"[..]));
        assert_eq!(l.get(199), Some(&b"e199"[..]));
        assert_eq!(l.iter().next_back(), Some(&b"e199"[..]));
        // Boundary sizes.
        for n in [0usize, 1, PACK_MAX, PACK_MAX + 1, 128, 1024] {
            let v = Bytes::from(vec![b'x'; n]);
            l.push_back(&v);
            assert_eq!(l.pop_back(), Some(v.clone()));
            l.push_front(&v);
            assert_eq!(l.pop_front(), Some(v));
        }
    }

    #[test]
    fn matches_a_vecdeque_model_under_random_operations() {
        let mut l = PackedList::new();
        let mut model: VecDeque<Vec<u8>> = VecDeque::new();
        let mut x: u64 = 0xD1B5_4A32_D192_ED03;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for step in 0..80_000 {
            let r = next();
            // Mostly short values, some at the packing boundary, some long.
            let len = match r % 12 {
                0 | 1 => PACK_MAX,
                2 => PACK_MAX + 1 + (r >> 8) as usize % 100,
                3 => 40 + (r >> 8) as usize % 24,
                _ => (r >> 8) as usize % 12,
            };
            let v: Vec<u8> = (0..len)
                .map(|k| (step as u8).wrapping_add(k as u8))
                .collect();
            let vb = Bytes::from(v.clone());
            match (r >> 32) % 12 {
                0..=2 => {
                    l.push_back(&vb);
                    model.push_back(v);
                }
                3..=5 => {
                    l.push_front(&vb);
                    model.push_front(v);
                }
                6 => assert_eq!(l.pop_front().map(|b| b.to_vec()), model.pop_front()),
                7 => assert_eq!(l.pop_back().map(|b| b.to_vec()), model.pop_back()),
                8 if !model.is_empty() => {
                    let i = (r >> 40) as usize % (model.len() + 1);
                    l.insert(i, &vb);
                    model.insert(i, v);
                }
                9 if !model.is_empty() => {
                    let i = (r >> 40) as usize % model.len();
                    assert_eq!(l.remove(i).map(|b| b.to_vec()), model.remove(i));
                }
                10 if !model.is_empty() => {
                    let i = (r >> 40) as usize % model.len();
                    l.set(i, &vb);
                    model[i] = v;
                }
                _ if !model.is_empty() => {
                    let i = (r >> 40) as usize % model.len();
                    assert_eq!(l.get(i), Some(&model[i][..]));
                    assert_eq!(l.get_bytes(i).map(|b| b.to_vec()), Some(model[i].clone()));
                }
                _ => {}
            }
            assert_eq!(l.len(), model.len());
            if step % 1009 == 0 {
                assert_eq!(
                    contents(&l),
                    model.iter().cloned().collect::<Vec<_>>(),
                    "step {step}"
                );
                let rev: Vec<Vec<u8>> = l.iter().rev().map(<[u8]>::to_vec).collect();
                assert_eq!(rev, model.iter().rev().cloned().collect::<Vec<_>>());
                for c in &l.chunks {
                    match c {
                        Chunk::Packed(p) => {
                            assert!(p.count() > 0 && p.live_len() <= CHUNK_MAX);
                        }
                        Chunk::Big(x) => assert!(x.len() > PACK_MAX),
                    }
                }
            }
        }
    }

    /// A full chunk (header included) is exactly 4096 bytes, an allocator
    /// size class; 4100 would round up to 5120 and waste a fifth of it.
    #[test]
    fn full_chunks_allocate_exactly_one_page_size_class() {
        assert_eq!(HDR + CHUNK_MAX, 4096);
        assert_eq!(Packed::full_for_back().buf.capacity(), 4096);
        assert_eq!(Packed::full_for_front().buf.capacity(), 4096);
        // Long lists built from both ends: every chunk stays within 4096 and
        // the contents read back in order.
        let mut l = PackedList::new();
        for i in 0..5_000u32 {
            l.push_back(&b(format!("r{i:05}").as_bytes()));
            l.push_front(&b(format!("l{i:05}").as_bytes()));
        }
        assert!(l.chunks.len() > 4);
        for c in &l.chunks {
            if let Chunk::Packed(p) = c {
                assert!(
                    p.buf.capacity() <= 4096,
                    "chunk capacity {}",
                    p.buf.capacity()
                );
                assert!(p.live_len() <= CHUNK_MAX);
            }
        }
        let all: Vec<Vec<u8>> = l.iter().map(<[u8]>::to_vec).collect();
        assert_eq!(all.len(), 10_000);
        assert_eq!(all[0], format!("l{:05}", 4_999).into_bytes());
        assert_eq!(all[4_999], b"l00000");
        assert_eq!(all[5_000], b"r00000");
        assert_eq!(all[9_999], format!("r{:05}", 4_999).into_bytes());
    }

    #[test]
    fn queue_use_never_grows_a_chunk_past_its_limit() {
        // RPUSH + LPOP: the front advances by `head`, the back appends.
        let mut l = PackedList::new();
        for i in 0..100_000u32 {
            l.push_back(&b(format!("job:{i}").as_bytes()));
            if i >= 50 {
                assert_eq!(l.pop_front().unwrap(), format!("job:{}", i - 50));
            }
        }
        assert_eq!(l.len(), 50);
        for c in &l.chunks {
            if let Chunk::Packed(p) = c {
                assert!(p.buf.capacity() <= p.head() + CHUNK_MAX + MIN_STEP);
            }
        }
    }

    #[test]
    fn retain_and_retain_range() {
        let mut l: PackedList = (0..300).map(|i| b(format!("{i}").as_bytes())).collect();
        l.push_back(&Bytes::from(vec![b'7'; 100]));
        l.retain(|v| v.contains(&b'7'));
        assert!(l.iter().all(|v| v.contains(&b'7')));
        let want = (0..300).filter(|i| format!("{i}").contains('7')).count() + 1;
        assert_eq!(l.len(), want);
        let before = contents(&l);
        l.retain_range(2, 5);
        assert_eq!(contents(&l), before[2..=5].to_vec());
    }
}
