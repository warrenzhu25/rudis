//! Packed byte storage shared by large sets and sorted sets.
//!
//! Entries are written back to back into pages as
//! `[varint(len << 1 | dead)][payload]` and addressed by a [`Handle`]: page
//! index and byte offset. Normal pages hold up to [`PAGE_BYTES`]; a payload
//! longer than [`OWN_PAGE_MIN`] gets a page of its own (offset 0). The page
//! taking appends grows by about a quarter at a time up to [`PAGE_BYTES`], so
//! a small collection does not pay for a full page.
//!
//! Removing an entry sets its dead bit. Once at least half a page is dead the
//! owner compacts it: [`PageStore::moves`] lists where each live entry will
//! go (read-only, so the owner can still look entries up to find what points
//! at them), then [`PageStore::compact`] slides them down in place and
//! shrinks the page to its live bytes (only the current page takes appends,
//! so a compacted page is never refilled). Handles
//! are never reused while live, and every moved entry lands below every
//! not-yet-moved one, so repointing by handle equality is unambiguous.

/// Normal page size: an exact allocator size class.
pub const PAGE_BYTES: usize = 4096;
/// Payloads longer than this get a page of their own.
pub const OWN_PAGE_MIN: usize = PAGE_BYTES / 4;
/// `current` when no page takes appends.
const NO_PAGE: u32 = u32::MAX;
/// Smallest buffer for a fresh appendable page.
const FIRST_PAGE_CAP: usize = 64;

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

/// (payload length, dead, start of the payload) of the entry at `off`.
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

/// Where an entry lives: page index and byte offset.
pub trait Handle: Copy + Eq + Default + std::fmt::Debug {
    fn max_pages() -> usize;
    fn pack(page: usize, off: usize) -> Self;
    fn page(self) -> usize;
    fn off(self) -> usize;
}

/// `page << 12 | offset`: 2^20 pages.
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

/// `page << 32 | offset`: for collections that outgrow `u32` handles.
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
    pub static NARROW_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(1 << 20) };
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

/// Page 0 lives inline in the owner, so a collection that fits one page
/// reaches its entries without first loading a separate page table: one
/// dependent cache miss fewer per lookup.
#[derive(Clone, Debug, Default)]
struct Pages {
    first: Page,
    rest: Vec<Page>,
    n: usize,
}

impl Pages {
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

#[derive(Clone, Debug)]
pub struct PageStore<H> {
    pages: Pages,
    /// Pages released (fully dead), reusable.
    free_pages: Vec<u32>,
    /// Normal page new entries are appended to, or `NO_PAGE`.
    current: u32,
    _h: std::marker::PhantomData<H>,
}

impl<H> Default for PageStore<H> {
    fn default() -> Self {
        PageStore {
            pages: Pages::default(),
            free_pages: Vec::new(),
            current: NO_PAGE,
            _h: std::marker::PhantomData,
        }
    }
}

impl<H: Handle> PageStore<H> {
    /// Payload of the live entry at `h`.
    #[inline]
    pub fn get(&self, h: H) -> &[u8] {
        let buf = &self.pages[h.page()].buf;
        let (len, _, start) = parse_entry(buf, h.off());
        &buf[start..start + len]
    }

    /// Mutable payload of the live entry at `h` (its length is fixed).
    #[inline]
    pub fn get_mut(&mut self, h: H) -> &mut [u8] {
        let buf = &mut self.pages[h.page()].buf;
        let (len, _, start) = parse_entry(buf, h.off());
        &mut buf[start..start + len]
    }

    fn alloc_page(&mut self, cap: usize) -> Option<usize> {
        let page = Page {
            buf: Vec::with_capacity(cap),
            dead: 0,
        };
        if let Some(p) = self.free_pages.pop() {
            let p = p as usize;
            self.pages[p] = page;
            return Some(p);
        }
        if self.pages.n >= H::max_pages() {
            return None;
        }
        self.pages.push(page);
        Some(self.pages.n - 1)
    }

    /// Appends an entry whose payload is `parts` concatenated. Returns its
    /// handle and, if a page was retired and needs compacting, that page.
    /// `None`, with nothing changed, when the location would not fit `H`.
    pub fn insert(&mut self, parts: &[&[u8]]) -> Option<(H, Option<usize>)> {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        let need = varint_len(len << 1) + len;
        let mut retired = None;
        let page = if len > OWN_PAGE_MIN {
            self.alloc_page(need)?
        } else {
            let cur = self.current as usize;
            if self.current != NO_PAGE && self.pages[cur].buf.len() + need <= PAGE_BYTES {
                let buf = &mut self.pages[cur].buf;
                if buf.len() + need > buf.capacity() {
                    // Grow by a quarter (about one allocator size class
                    // above 1 KiB), never past one page. Doubling left the
                    // append page up to half empty; growing to exactly the
                    // need called `realloc` on nearly every insert.
                    let cap = buf.capacity();
                    let want = (buf.len() + need)
                        .max(cap + cap / 4)
                        .min(PAGE_BYTES)
                        .max(buf.len() + need);
                    buf.reserve_exact(want - buf.len());
                }
                cur
            } else {
                let p = self.alloc_page(FIRST_PAGE_CAP.max(need))?;
                if self.current != NO_PAGE {
                    // The retired page was exempt from compaction while it
                    // took appends; the owner compacts it now if needed.
                    let old = &self.pages[cur];
                    if old.dead * 2 >= old.buf.len() {
                        retired = Some(cur);
                    }
                }
                self.current = p as u32;
                p
            }
        };
        let buf = &mut self.pages[page].buf;
        let off = buf.len();
        write_varint(buf, len << 1);
        for p in parts {
            buf.extend_from_slice(p);
        }
        Some((H::pack(page, off), retired))
    }

    /// Marks the entry at `h` dead. Returns a page the owner must compact.
    pub fn release(&mut self, h: H) -> Option<usize> {
        let page = h.page();
        let p = &mut self.pages[page];
        let (len, dead, start) = parse_entry(&p.buf, h.off());
        debug_assert!(!dead, "double release");
        p.buf[h.off()] |= 1;
        p.dead += start + len - h.off();
        let (dead, used) = (p.dead, p.buf.len());
        if dead == used {
            if self.current == page as u32 {
                self.current = NO_PAGE;
            }
            self.pages[page] = Page::default();
            self.free_pages.push(page as u32);
            None
        } else if dead * 2 >= used && self.current != page as u32 {
            Some(page)
        } else {
            None
        }
    }

    /// (old handle, new handle, payload) of each live entry of `page` that
    /// [`compact`](Self::compact) will move.
    pub fn moves(&self, page: usize) -> impl Iterator<Item = (H, H, &[u8])> + '_ {
        let buf = &self.pages[page].buf;
        let (mut off, mut dst) = (0, 0);
        std::iter::from_fn(move || {
            while off < buf.len() {
                let (len, dead, start) = parse_entry(buf, off);
                let size = start + len - off;
                let at = off;
                off += size;
                if !dead {
                    let to = dst;
                    dst += size;
                    if to != at {
                        return Some((
                            H::pack(page, at),
                            H::pack(page, to),
                            &buf[start..start + len],
                        ));
                    }
                }
            }
            None
        })
    }

    /// Slides `page`'s live entries to the front of its buffer, in place, to
    /// the positions [`moves`](Self::moves) reported.
    pub fn compact(&mut self, page: usize) {
        let p = &mut self.pages[page];
        let buf = &mut p.buf;
        let (mut off, mut dst) = (0, 0);
        while off < buf.len() {
            let (len, dead, start) = parse_entry(buf, off);
            let size = start + len - off;
            if !dead {
                if dst != off {
                    buf.copy_within(off..off + size, dst);
                }
                dst += size;
            }
            off += size;
        }
        buf.truncate(dst);
        // Give the freed tail back: new entries only append to the current
        // page, so a compacted page is never refilled. Keeping its full
        // capacity let churn (remove + insert) pin a near-empty 4 KB page
        // per few survivors, growing without bound.
        buf.shrink_to_fit();
        p.dead = 0;
    }

    /// Heap bytes held: page buffers (by capacity) plus the page table.
    pub fn heap_bytes(&self) -> usize {
        let bufs: usize = (0..self.pages.n)
            .map(|i| self.pages[i].buf.capacity())
            .sum();
        bufs + self.pages.rest.capacity() * std::mem::size_of::<Page>()
            + self.free_pages.capacity() * 4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Remove + insert churn with a small live set must not pin memory:
    /// compacted pages hand back their freed tail.
    #[test]
    fn churn_keeps_heap_bounded_by_live_bytes() {
        let mut ps = PageStore::<u32>::default();
        let mut live: std::collections::VecDeque<(u32, u32)> = Default::default();
        let compact = |ps: &mut PageStore<u32>,
                       live: &mut std::collections::VecDeque<(u32, u32)>,
                       page: usize| {
            let mv: Vec<(u32, u32)> = ps.moves(page).map(|(a, b, _)| (a, b)).collect();
            ps.compact(page);
            for (a, b) in mv {
                for e in live.iter_mut() {
                    if e.0 == a {
                        e.0 = b;
                    }
                }
            }
        };
        let mut peak = 0;
        let mut rng = 0x9E37_79B9u32;
        let prio: Vec<u32> = (0..200_000)
            .map(|_| {
                rng ^= rng << 13;
                rng ^= rng >> 17;
                rng ^= rng << 5;
                rng
            })
            .collect();
        for seq in 0..200_000u32 {
            if live.len() >= 300 {
                // ZPOPMIN-like: drop the lowest random priority, so a few
                // high-priority entries outlive many pages of their peers.
                let i = (0..live.len())
                    .min_by_key(|&i| prio[live[i].1 as usize])
                    .unwrap();
                let (h, _) = live.remove(i).unwrap();
                if let Some(p) = ps.release(h) {
                    compact(&mut ps, &mut live, p);
                }
            }
            let payload = seq.to_le_bytes();
            let (h, retired) = ps.insert(&[&payload, &[7u8; 20]]).unwrap();
            live.push_back((h, seq));
            if let Some(p) = retired {
                compact(&mut ps, &mut live, p);
            }
            if seq % 1000 == 0 {
                peak = peak.max(ps.heap_bytes());
            }
        }
        let live_bytes = live.len() * 25;
        assert!(
            peak <= 2 * live_bytes + 3 * PAGE_BYTES + 64 * 1024,
            "peak heap {peak} for {live_bytes} live bytes"
        );
        for &(h, v) in &live {
            assert_eq!(&ps.get(h)[..4], &v.to_le_bytes());
        }
    }
}
