//! Large-set storage: members packed back to back in 4 KiB pages, indexed by
//! a hash table whose buckets hold each member's *location* directly.
//!
//! Like Dragonfly's `DenseSet`, a bucket points straight at the member: no id,
//! no side location table. A bucket is a `u32` handle, `page << 12 | offset`
//! (pages are 4096 bytes, so the offset fits 12 bits). A page entry is
//! `[varint(len << 1 | dead)][bytes]`, so a 5-byte member costs 6 bytes of
//! page plus a 4-byte bucket (and the table's control byte and slack).
//!
//! Removing a member sets the entry's dead bit. A page is compacted in place
//! once at least half of it is dead; each moved member is re-hashed to find
//! and repoint its bucket. A member longer than [`OWN_PAGE_MIN`] gets a page
//! of its own (offset 0) that is freed when the member is removed.
//!
//! `u32` handles address 2^20 pages. A set that would need more switches,
//! once, to `u64` handles (`page << 32 | offset`), so there is no size limit.

use bytes::Bytes;

/// Normal page size: an exact allocator size class.
const PAGE_BYTES: usize = 4096;
/// Members longer than this get a page of their own.
const OWN_PAGE_MIN: usize = PAGE_BYTES / 4;
/// `current` when no page takes appends.
const NO_PAGE: u32 = u32::MAX;

#[inline(always)]
fn hash_member(m: &[u8]) -> u64 {
    fxhash::hash64(m)
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

#[inline]
fn write_varint(buf: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

/// (member length, dead, start of the member bytes) of the entry at `off`.
/// The dead flag is bit 0 of the varint, i.e. bit 0 of its first byte.
#[inline]
fn parse_entry(buf: &[u8], off: usize) -> (usize, bool, usize) {
    let mut n = 0usize;
    let mut shift = 0;
    let mut i = off;
    loop {
        let b = buf[i];
        n |= ((b & 0x7F) as usize) << shift;
        i += 1;
        if b & 0x80 == 0 {
            return (n >> 1, n & 1 == 1, i);
        }
        shift += 7;
    }
}

/// Where a member lives: page index and byte offset of its entry.
trait Handle: Copy + Eq + Default + std::fmt::Debug {
    fn max_pages() -> usize;
    fn pack(page: usize, off: usize) -> Self;
    fn page(self) -> usize;
    fn off(self) -> usize;
}

impl Handle for u32 {
    #[inline(always)]
    fn max_pages() -> usize {
        narrow_page_limit()
    }
    #[inline(always)]
    fn pack(page: usize, off: usize) -> u32 {
        debug_assert!(off < PAGE_BYTES);
        ((page as u32) << 12) | off as u32
    }
    #[inline(always)]
    fn page(self) -> usize {
        (self >> 12) as usize
    }
    #[inline(always)]
    fn off(self) -> usize {
        (self & 0xFFF) as usize
    }
}

impl Handle for u64 {
    #[inline(always)]
    fn max_pages() -> usize {
        NO_PAGE as usize
    }
    #[inline(always)]
    fn pack(page: usize, off: usize) -> u64 {
        ((page as u64) << 32) | off as u64
    }
    #[inline(always)]
    fn page(self) -> usize {
        (self >> 32) as usize
    }
    #[inline(always)]
    fn off(self) -> usize {
        (self & 0xFFFF_FFFF) as usize
    }
}

#[cfg(not(test))]
#[inline(always)]
fn narrow_page_limit() -> usize {
    1 << 20
}

#[cfg(test)]
thread_local! {
    static NARROW_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(1 << 20) };
}

#[cfg(test)]
fn narrow_page_limit() -> usize {
    NARROW_LIMIT.with(|c| c.get())
}

#[derive(Clone, Debug, Default)]
struct Page {
    buf: Vec<u8>,
    dead: usize,
}

/// The set's pages. Page 0 lives inline in the set, so a set that fits one
/// page (a few hundred short members) reaches its members without first
/// loading a separate page table: one dependent cache miss fewer per lookup.
#[derive(Clone, Debug, Default)]
struct Pages {
    first: Page,
    rest: Vec<Page>,
    n: usize,
}

impl Pages {
    #[inline(always)]
    fn len(&self) -> usize {
        self.n
    }

    fn push(&mut self, p: Page) {
        if self.n == 0 {
            self.first = p;
        } else {
            self.rest.push(p);
        }
        self.n += 1;
    }
}

impl std::ops::Index<usize> for Pages {
    type Output = Page;
    #[inline(always)]
    fn index(&self, i: usize) -> &Page {
        if i == 0 {
            &self.first
        } else {
            &self.rest[i - 1]
        }
    }
}

impl std::ops::IndexMut<usize> for Pages {
    #[inline(always)]
    fn index_mut(&mut self, i: usize) -> &mut Page {
        if i == 0 {
            &mut self.first
        } else {
            &mut self.rest[i - 1]
        }
    }
}

#[inline]
fn get_in<H: Handle>(pages: &Pages, h: H) -> &[u8] {
    let buf = &pages[h.page()].buf;
    let (len, _, start) = parse_entry(buf, h.off());
    &buf[start..start + len]
}

#[derive(Clone, Debug)]
struct Inner<H> {
    dict: hashbrown::HashTable<H>,
    pages: Pages,
    /// Pages released (fully dead), reusable.
    free_pages: Vec<u32>,
    /// Normal page new members are appended to, or `NO_PAGE`.
    current: u32,
    len: usize,
}

impl<H> Default for Inner<H> {
    fn default() -> Self {
        Inner {
            dict: hashbrown::HashTable::new(),
            pages: Pages::default(),
            free_pages: Vec::new(),
            current: NO_PAGE,
            len: 0,
        }
    }
}

impl<H: Handle> Inner<H> {
    #[inline]
    fn find(&self, m: &[u8], hash: u64) -> Option<H> {
        let pages = &self.pages;
        self.dict.find(hash, |&h| get_in(pages, h) == m).copied()
    }

    fn alloc_page(&mut self, cap: usize) -> Option<usize> {
        if let Some(p) = self.free_pages.pop() {
            let p = p as usize;
            self.pages[p] = Page {
                buf: Vec::with_capacity(cap),
                dead: 0,
            };
            return Some(p);
        }
        if self.pages.len() >= H::max_pages() {
            return None;
        }
        self.pages.push(Page {
            buf: Vec::with_capacity(cap),
            dead: 0,
        });
        Some(self.pages.len() - 1)
    }

    /// Adds `m` (known absent). False, with nothing changed, when its
    /// location would not fit a handle `H`.
    fn insert_new(&mut self, m: &[u8], hash: u64) -> bool {
        let need = varint_len(m.len() << 1) + m.len();
        let page = if m.len() > OWN_PAGE_MIN {
            match self.alloc_page(need) {
                Some(p) => p,
                None => return false,
            }
        } else {
            let cur = self.current as usize;
            if self.current != NO_PAGE && self.pages[cur].buf.len() + need <= PAGE_BYTES {
                cur
            } else {
                let Some(p) = self.alloc_page(PAGE_BYTES) else {
                    return false;
                };
                if self.current != NO_PAGE {
                    // The retired page was exempt from compaction while it
                    // took appends; compact it now if it needs it.
                    self.current = NO_PAGE;
                    let old = &self.pages[cur];
                    if old.dead * 2 >= old.buf.len() {
                        self.compact(cur);
                    }
                }
                self.current = p as u32;
                p
            }
        };
        let buf = &mut self.pages[page].buf;
        let off = buf.len();
        write_varint(buf, m.len() << 1);
        buf.extend_from_slice(m);
        let pages = &self.pages;
        self.dict
            .insert_unique(hash, H::pack(page, off), |&x| hash_member(get_in(pages, x)));
        self.len += 1;
        true
    }

    /// Frees the entry at `h`, whose bucket the caller already removed.
    fn release(&mut self, h: H) {
        self.len -= 1;
        if self.len == 0 {
            *self = Self::default();
            return;
        }
        let page = h.page();
        let p = &mut self.pages[page];
        let (len, dead, start) = parse_entry(&p.buf, h.off());
        debug_assert!(!dead);
        p.buf[h.off()] |= 1;
        p.dead += start + len - h.off();
        let (dead, used) = (p.dead, p.buf.len());
        if dead == used {
            if self.current == page as u32 {
                self.current = NO_PAGE;
            }
            self.pages[page] = Page::default();
            self.free_pages.push(page as u32);
        } else if dead * 2 >= used && self.current != page as u32 {
            self.compact(page);
        }
    }

    /// Slides `page`'s live entries to the front of its buffer, in place,
    /// repointing each moved member's bucket.
    fn compact(&mut self, page: usize) {
        let Self { dict, pages, .. } = self;
        let buf = &mut pages[page].buf;
        let (mut off, mut dst) = (0, 0);
        while off < buf.len() {
            let (len, dead, start) = parse_entry(buf, off);
            let size = start + len - off;
            if !dead {
                if dst != off {
                    // Handles are unique among live entries, and every
                    // already-moved entry now sits below `dst`.
                    let old = H::pack(page, off);
                    let hash = hash_member(&buf[start..start + len]);
                    if let Some(slot) = dict.find_mut(hash, |&x| x == old) {
                        *slot = H::pack(page, dst);
                    } else {
                        debug_assert!(false, "live entry without a bucket");
                    }
                    buf.copy_within(off..off + size, dst);
                }
                dst += size;
            }
            off += size;
        }
        buf.truncate(dst);
        pages[page].dead = 0;
    }

    fn remove(&mut self, m: &[u8], hash: u64) -> bool {
        let pages = &self.pages;
        match self.dict.find_entry(hash, |&h| get_in(pages, h) == m) {
            Ok(entry) => {
                let (h, _) = entry.remove();
                self.release(h);
                true
            }
            Err(_) => false,
        }
    }

    fn pop(&mut self) -> Option<Bytes> {
        // Take the first bucket in table order directly: no re-hash or second
        // probe. A dropped `ExtractIf` keeps the elements it did not visit.
        let h = self.dict.extract_if(|_| true).next()?;
        let m = Bytes::copy_from_slice(get_in(&self.pages, h));
        self.release(h);
        Some(m)
    }

    fn iter(&self) -> InnerIter<'_, H> {
        InnerIter {
            it: self.dict.iter(),
            pages: &self.pages,
        }
    }
}

struct InnerIter<'a, H> {
    it: hashbrown::hash_table::Iter<'a, H>,
    pages: &'a Pages,
}

impl<'a, H: Handle> Iterator for InnerIter<'a, H> {
    type Item = &'a [u8];
    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        self.it.next().map(|&h| get_in(self.pages, h))
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.it.size_hint()
    }
}

#[derive(Clone, Debug)]
enum Repr {
    Narrow(Inner<u32>),
    Wide(Inner<u64>),
}

#[derive(Clone, Debug)]
pub struct PackedSet(Repr);

impl Default for PackedSet {
    fn default() -> Self {
        PackedSet(Repr::Narrow(Inner::default()))
    }
}

impl PackedSet {
    pub fn with_capacity(n: usize) -> Self {
        PackedSet(Repr::Narrow(Inner {
            dict: hashbrown::HashTable::with_capacity(n),
            ..Inner::default()
        }))
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.0 {
            Repr::Narrow(s) => s.len,
            Repr::Wide(s) => s.len,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `hash` must be `fxhash::hash64(member)`.
    #[inline]
    pub fn contains_with_hash(&self, member: &[u8], hash: u64) -> bool {
        match &self.0 {
            Repr::Narrow(s) => s.find(member, hash).is_some(),
            Repr::Wide(s) => s.find(member, hash).is_some(),
        }
    }

    #[inline]
    pub fn contains(&self, member: &[u8]) -> bool {
        self.contains_with_hash(member, hash_member(member))
    }

    /// Adds a copy of `member`; false if it was already present.
    pub fn insert(&mut self, member: &[u8]) -> bool {
        let h = hash_member(member);
        match &mut self.0 {
            Repr::Narrow(s) => {
                if s.find(member, h).is_some() {
                    return false;
                }
                if s.insert_new(member, h) {
                    return true;
                }
                self.widen();
                let Repr::Wide(w) = &mut self.0 else {
                    unreachable!("widen() makes the set wide")
                };
                let ok = w.insert_new(member, h);
                debug_assert!(ok);
                true
            }
            Repr::Wide(s) => {
                if s.find(member, h).is_some() {
                    return false;
                }
                s.insert_new(member, h)
            }
        }
    }

    /// Switches to `u64` handles: the set outgrew 2^20 pages.
    #[cold]
    fn widen(&mut self) {
        let Repr::Narrow(n) = &self.0 else {
            return;
        };
        let mut w = Inner::<u64> {
            dict: hashbrown::HashTable::with_capacity(n.len + 1),
            ..Inner::default()
        };
        for m in n.iter() {
            let ok = w.insert_new(m, hash_member(m));
            debug_assert!(ok);
        }
        self.0 = Repr::Wide(w);
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        let h = hash_member(member);
        match &mut self.0 {
            Repr::Narrow(s) => s.remove(member, h),
            Repr::Wide(s) => s.remove(member, h),
        }
    }

    /// Removes and returns some member.
    pub fn pop(&mut self) -> Option<Bytes> {
        match &mut self.0 {
            Repr::Narrow(s) => s.pop(),
            Repr::Wide(s) => s.pop(),
        }
    }

    /// Members in hash-table order.
    pub fn iter(&self) -> PackedSetIter<'_> {
        PackedSetIter(match &self.0 {
            Repr::Narrow(s) => IterRepr::Narrow(s.iter()),
            Repr::Wide(s) => IterRepr::Wide(s.iter()),
        })
    }
}

impl PartialEq for PackedSet {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().all(|m| other.contains(m))
    }
}

impl Eq for PackedSet {}

enum IterRepr<'a> {
    Narrow(InnerIter<'a, u32>),
    Wide(InnerIter<'a, u64>),
}

pub struct PackedSetIter<'a>(IterRepr<'a>);

impl<'a> Iterator for PackedSetIter<'a> {
    type Item = &'a [u8];
    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        match &mut self.0 {
            IterRepr::Narrow(i) => i.next(),
            IterRepr::Wide(i) => i.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.0 {
            IterRepr::Narrow(i) => i.size_hint(),
            IterRepr::Wide(i) => i.size_hint(),
        }
    }
}

impl ExactSizeIterator for PackedSetIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn run_model(steps: usize, seed: u64) -> PackedSet {
        let mut s = PackedSet::default();
        let mut model: HashSet<Vec<u8>> = HashSet::new();
        let mut x: u64 = seed;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for step in 0..steps {
            let r = next();
            let len = if r % 40 == 0 {
                2000
            } else {
                (r >> 8) as usize % 30
            };
            let m: Vec<u8> = format!("{}", (r >> 16) % 3000)
                .into_bytes()
                .into_iter()
                .cycle()
                .take(len.max(1))
                .collect();
            match (r >> 40) % 6 {
                0..=2 => assert_eq!(s.insert(&m), model.insert(m.clone())),
                3 | 4 => assert_eq!(s.remove(&m), model.remove(&m)),
                _ => {
                    if let Some(p) = s.pop() {
                        assert!(model.remove(p.as_ref()), "popped a member not in the model");
                    } else {
                        assert!(model.is_empty());
                    }
                }
            }
            assert_eq!(s.len(), model.len());
            assert_eq!(s.contains(&m), model.contains(&m));
            if step % 1021 == 0 {
                let got: HashSet<Vec<u8>> = s.iter().map(<[u8]>::to_vec).collect();
                assert_eq!(got, model, "step {step}");
                assert_eq!(s.iter().len(), model.len());
            }
        }
        s
    }

    #[test]
    fn matches_a_hashset_model_under_random_operations() {
        let s = run_model(60_000, 0x5851_F42D_4C95_7F2D);
        assert!(matches!(s.0, Repr::Narrow(_)));
    }

    #[test]
    fn widens_past_the_narrow_page_limit_and_stays_correct() {
        NARROW_LIMIT.with(|c| c.set(4));
        let s = run_model(60_000, 0x2545_F491_4F6C_DD1D);
        NARROW_LIMIT.with(|c| c.set(1 << 20));
        assert!(
            matches!(s.0, Repr::Wide(_)),
            "a 4-page limit must force widening"
        );
    }

    #[test]
    fn drains_by_remove_and_pop_with_compaction() {
        let mut s = PackedSet::default();
        let members: Vec<Vec<u8>> = (0..20_000u32)
            .map(|i| format!("member:{i}").into_bytes())
            .collect();
        for m in &members {
            assert!(s.insert(m));
        }
        // Remove every other member: pages compact, buckets get repointed.
        for m in members.iter().step_by(2) {
            assert!(s.remove(m));
        }
        for (i, m) in members.iter().enumerate() {
            assert_eq!(s.contains(m), i % 2 == 1, "member {i}");
        }
        let mut popped = HashSet::new();
        while let Some(p) = s.pop() {
            assert!(popped.insert(p));
        }
        assert_eq!(popped.len(), 10_000);
        assert!(s.is_empty());
        assert!(s.insert(b"again"));
        assert!(s.contains(b"again"));
    }
}
