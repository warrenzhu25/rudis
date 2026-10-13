//! Packed list storage: short elements stored back to back in byte chunks.
//!
//! A `VecDeque` of 32-byte element handles costs 32 bytes per element however
//! short it is.
//! Here elements of up to [`PACK_MAX`] bytes are packed into chunks of at
//! most [`CHUNK_BYTES`] bytes, each entry encoded as `[len][bytes][len]` (the
//! trailing length lets the chunk be walked backwards). Longer elements get a
//! chunk of their own holding the `Bytes` they arrived in, shared rather than
//! copied, as before.
//!
//! - Push/pop at either end: O(1), plus a `memmove` of at most `CHUNK_BYTES`
//!   when adding or removing at the front of a packed chunk.
//! - Index access: walks chunk element counts from the nearer end, then scans
//!   within one chunk (like Redis's quicklist).

use bytes::Bytes;
use std::collections::VecDeque;

/// Longest element stored packed; longer ones get their own chunk.
pub const PACK_MAX: usize = 63;
/// Packed chunk capacity in bytes: an exact allocator size class.
pub const CHUNK_BYTES: usize = 512;

/// Entries `[len: u8][bytes][len: u8]`, `count` of them.
#[derive(Clone, Debug)]
struct Packed {
    buf: Vec<u8>,
    count: u32,
}

/// Boxed packed contents keep a chunk at 32 bytes (a `Vec` plus a count
/// beside `Bytes` made it 40), which matters when every element of a
/// long-value list is a chunk of its own.
#[derive(Clone, Debug)]
enum Chunk {
    Packed(Box<Packed>),
    /// One long element, shared with whoever else holds it.
    Big(Bytes),
}

const _: () = assert!(std::mem::size_of::<Chunk>() == 32);

impl Chunk {
    #[inline]
    fn count(&self) -> usize {
        match self {
            Chunk::Packed(p) => p.count as usize,
            Chunk::Big(_) => 1,
        }
    }

    fn new_packed() -> Chunk {
        Chunk::Packed(Box::new(Packed {
            buf: Vec::with_capacity(CHUNK_BYTES),
            count: 0,
        }))
    }
}

#[inline]
fn entry_size(len: usize) -> usize {
    len + 2
}

/// Byte offset of entry `idx` in a packed buffer.
fn offset_of(buf: &[u8], idx: usize) -> usize {
    let mut off = 0;
    for _ in 0..idx {
        off += entry_size(buf[off] as usize);
    }
    off
}

#[inline]
fn entry_at(buf: &[u8], off: usize) -> &[u8] {
    let len = buf[off] as usize;
    &buf[off + 1..off + 1 + len]
}

#[inline]
fn encode_into(buf: &mut Vec<u8>, at: usize, v: &[u8]) {
    let len = v.len() as u8;
    let n = entry_size(v.len());
    buf.splice(at..at, std::iter::repeat_n(0u8, n));
    buf[at] = len;
    buf[at + 1..at + 1 + v.len()].copy_from_slice(v);
    buf[at + 1 + v.len()] = len;
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
        let needed = big + packed_bytes.div_ceil(CHUNK_BYTES) + 1;
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
        let fits = matches!(self.chunks.back(), Some(Chunk::Packed(p)) if p.buf.len() + need <= CHUNK_BYTES);
        if !fits {
            self.chunks.push_back(Chunk::new_packed());
        }
        if let Some(Chunk::Packed(p)) = self.chunks.back_mut() {
            let Packed { buf, count } = &mut **p;
            let at = buf.len();
            encode_into(buf, at, v);
            *count += 1;
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
        let fits = matches!(self.chunks.front(), Some(Chunk::Packed(p)) if p.buf.len() + need <= CHUNK_BYTES);
        if !fits {
            self.chunks.push_front(Chunk::new_packed());
        }
        if let Some(Chunk::Packed(p)) = self.chunks.front_mut() {
            let Packed { buf, count } = &mut **p;
            encode_into(buf, 0, v);
            *count += 1;
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
                let Packed { buf, count } = &mut **p;
                let v = Bytes::copy_from_slice(entry_at(buf, 0));
                buf.drain(..entry_size(v.len()));
                *count -= 1;
                if *count == 0 {
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
                let Packed { buf, count } = &mut **p;
                let len = *buf.last().expect("non-empty chunk") as usize;
                let start = buf.len() - entry_size(len);
                let v = Bytes::copy_from_slice(&buf[start + 1..start + 1 + len]);
                buf.truncate(start);
                *count -= 1;
                if *count == 0 {
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
            Chunk::Packed(p) => entry_at(&p.buf, offset_of(&p.buf, j)),
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
            Chunk::Packed(p) => Bytes::copy_from_slice(entry_at(&p.buf, offset_of(&p.buf, j))),
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
                let Packed { buf, count } = &mut **p;
                let off = offset_of(buf, j);
                let v = Bytes::copy_from_slice(entry_at(buf, off));
                buf.drain(off..off + entry_size(v.len()));
                *count -= 1;
                if *count == 0 {
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
        let full = matches!(&self.chunks[ci], Chunk::Packed(p) if p.buf.len() + need > CHUNK_BYTES);
        let (ci, j) = if full {
            let half = self.chunks[ci].count() / 2;
            let at = self.split_at(ci, half);
            if j < half { (ci, j) } else { (at, j - half) }
        } else {
            (ci, j)
        };
        if let Chunk::Packed(p) = &mut self.chunks[ci] {
            let Packed { buf, count } = &mut **p;
            let off = offset_of(buf, j);
            encode_into(buf, off, v);
            *count += 1;
        }
        self.len += 1;
    }

    /// Splits packed chunk `ci` before its element `j`, returning the index
    /// where the second half (or, for `j == 0`/Big chunks, chunk `ci`) is.
    fn split_at(&mut self, ci: usize, j: usize) -> usize {
        match &mut self.chunks[ci] {
            Chunk::Packed(p) if j > 0 && j < p.count as usize => {
                let Packed { buf, count } = &mut **p;
                let off = offset_of(buf, j);
                let mut tail = Vec::with_capacity(CHUNK_BYTES);
                tail.extend_from_slice(&buf[off..]);
                buf.truncate(off);
                let tail_count = *count - j as u32;
                *count = j as u32;
                self.chunks.insert(
                    ci + 1,
                    Chunk::Packed(Box::new(Packed {
                        buf: tail,
                        count: tail_count,
                    })),
                );
                ci + 1
            }
            Chunk::Packed(p) if j >= p.count as usize => ci + 1,
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
                Chunk::Packed(p) => {
                    let Packed { buf, count } = *p;
                    let mut off = 0;
                    for _ in 0..count {
                        let v = entry_at(&buf, off);
                        off += entry_size(v.len());
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
                front: 0,
                back: p.buf.len(),
                remaining: p.count as usize,
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
        let v = entry_at(self.buf, self.front);
        self.front += entry_size(v.len());
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
        let len = self.buf[self.back - 1] as usize;
        self.back -= entry_size(len);
        Some(&self.buf[self.back + 1..self.back + 1 + len])
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
    fn short_elements_pack_and_long_ones_are_shared() {
        let mut l = PackedList::new();
        for i in 0..200 {
            l.push_back(&b(format!("e{i}").as_bytes()));
        }
        // 200 short entries fit in a handful of 512-byte chunks.
        assert!(l.chunks.len() <= 4, "{} chunks", l.chunks.len());
        let long = Bytes::from(vec![b'L'; 128]);
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
        for n in [0usize, 1, PACK_MAX, PACK_MAX + 1] {
            let v = Bytes::from(vec![b'x'; n]);
            l.push_back(&v);
            assert_eq!(l.pop_back(), Some(v));
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
        for step in 0..60_000 {
            let r = next();
            // Mostly short values, some at the packing boundary, some long.
            let len = match r % 10 {
                0 => PACK_MAX,
                1 => PACK_MAX + 1 + (r >> 8) as usize % 100,
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
                            assert!(p.count > 0 && p.buf.len() <= CHUNK_BYTES);
                            assert!(p.buf.capacity() <= CHUNK_BYTES, "a chunk reallocated");
                        }
                        Chunk::Big(x) => assert!(x.len() > PACK_MAX),
                    }
                }
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
