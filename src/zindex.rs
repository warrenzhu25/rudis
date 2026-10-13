//! Ordered member index for large sorted sets, each member stored once,
//! packed.
//!
//! Each `(score, member)` lives once as a [`PageStore`] entry whose payload is
//! `[score: f64 LE][member bytes]`: a 5-byte member costs 14 bytes of page
//! instead of a 32-byte `(OrderedScore, CompactKey)` slot. A hash table maps
//! members to entry handles (4 bytes each, members compared through the
//! page), and the order is a list of sorted runs of handles, each at most
//! [`CHUNK_MAX`] long.
//!
//! - `get`/`contains_key`: one hash lookup, then the entry.
//! - `insert`/`remove`: hash lookup, binary search over the runs and within
//!   one run, then a `memmove` of at most `CHUNK_MAX` handles.
//! - Rank and offset access walk the run lengths: O(len / CHUNK_MAX).
//! - A score update rewrites the entry's 8 score bytes in place.
//!
//! When a half-dead page is compacted, each moved entry's run slot is found
//! by binary search on its (score, member) and its bucket by re-hashing, both
//! before anything moves, then repointed.
//!
//! Iteration yields `(OrderedScore, &[u8])` in (score, member) order. `u32`
//! handles address 2^20 pages; a bigger set switches once to `u64` handles.

use crate::pagestore::{Handle, PageStore};
use crate::table::OrderedScore;
use bytes::Bytes;
use std::cmp::Ordering;

/// Longest run of handles; a run that grows past it is split in two. Runs
/// are allocated at `CHUNK_MAX + 1` handles = 512 bytes (`u32`), an exact
/// allocator size class.
const CHUNK_MAX: usize = 127;

#[inline(always)]
fn hash_member(m: &[u8]) -> u64 {
    fxhash::hash64(m)
}

/// (score, member) of an entry payload.
#[inline(always)]
fn split(p: &[u8]) -> (OrderedScore, &[u8]) {
    let (s, m) = p.split_at(8);
    (
        OrderedScore(f64::from_le_bytes(s.try_into().expect("8 score bytes"))),
        m,
    )
}

#[derive(Clone)]
struct Inner<H> {
    store: PageStore<H>,
    /// Member -> handle.
    dict: hashbrown::HashTable<H>,
    /// Handles in (score, member) order, split into sorted runs.
    runs: Vec<Vec<H>>,
    len: usize,
}

impl<H> Default for Inner<H> {
    fn default() -> Self {
        Inner {
            store: PageStore::default(),
            dict: hashbrown::HashTable::new(),
            runs: Vec::new(),
            len: 0,
        }
    }
}

impl<H: Handle> Inner<H> {
    #[inline(always)]
    fn entry(&self, h: H) -> (OrderedScore, &[u8]) {
        split(self.store.get(h))
    }

    #[inline]
    fn find(&self, member: &[u8]) -> Option<H> {
        let store = &self.store;
        self.dict
            .find(hash_member(member), |&h| &store.get(h)[8..] == member)
            .copied()
    }

    #[inline]
    fn cmp_h(&self, h: H, score: OrderedScore, member: &[u8]) -> Ordering {
        let (s, m) = self.entry(h);
        s.cmp(&score).then_with(|| m.cmp(member))
    }

    /// Run and position of the first element not less than `(score, member)`.
    /// `(runs.len(), 0)` when every element is less.
    fn lower_bound(&self, score: OrderedScore, member: &[u8]) -> (usize, usize) {
        let ri = self.runs.partition_point(|r| {
            self.cmp_h(*r.last().expect("runs are never empty"), score, member) == Ordering::Less
        });
        if ri == self.runs.len() {
            return (ri, 0);
        }
        let pos =
            self.runs[ri].partition_point(|&h| self.cmp_h(h, score, member) == Ordering::Less);
        (ri, pos)
    }

    #[inline]
    fn new_run() -> Vec<H> {
        Vec::with_capacity(CHUNK_MAX + 1)
    }

    /// Links entry `h` into the order.
    fn place(&mut self, h: H) {
        if self.runs.is_empty() {
            let mut run = Self::new_run();
            run.push(h);
            self.runs.push(run);
            return;
        }
        let (score, member) = self.entry(h);
        let (mut ri, mut pos) = self.lower_bound(score, member);
        if ri == self.runs.len() {
            ri -= 1;
            pos = self.runs[ri].len();
        }
        let run = &mut self.runs[ri];
        run.insert(pos, h);
        if run.len() > CHUNK_MAX {
            // Split into fresh exact-size runs rather than letting `Vec` grow.
            let mut tail = Self::new_run();
            tail.extend_from_slice(&run[CHUNK_MAX / 2..]);
            run.truncate(CHUNK_MAX / 2);
            self.runs.insert(ri + 1, tail);
        }
    }

    /// Unlinks entry `h` from the order.
    fn unplace(&mut self, h: H) {
        let (score, member) = self.entry(h);
        let (ri, pos) = self.lower_bound(score, member);
        debug_assert_eq!(self.runs[ri][pos], h);
        self.unplace_at(ri, pos);
    }

    fn unplace_at(&mut self, ri: usize, pos: usize) {
        self.runs[ri].remove(pos);
        if self.runs[ri].is_empty() {
            self.runs.remove(ri);
        } else if self.runs[ri].len() < CHUNK_MAX / 4
            && ri + 1 < self.runs.len()
            && self.runs[ri].len() + self.runs[ri + 1].len() <= CHUNK_MAX
        {
            // Keep runs from fragmenting under deletes.
            let next = self.runs.remove(ri + 1);
            self.runs[ri].extend_from_slice(&next);
        }
    }

    /// Sets `member`'s score; returns the previous one. `Err(())`, with
    /// nothing changed, when a new entry's location would not fit `H`.
    fn insert(&mut self, member: &[u8], score: f64) -> Result<Option<f64>, ()> {
        if let Some(h) = self.find(member) {
            let old = self.entry(h).0.0;
            if old.total_cmp(&score) != Ordering::Equal {
                self.unplace(h);
                self.store.get_mut(h)[..8].copy_from_slice(&score.to_le_bytes());
                self.place(h);
            }
            return Ok(Some(old));
        }
        let (h, retired) = self
            .store
            .insert(&[&score.to_le_bytes(), member])
            .ok_or(())?;
        let store = &self.store;
        self.dict
            .insert_unique(hash_member(member), h, |&x| hash_member(&store.get(x)[8..]));
        self.place(h);
        self.len += 1;
        if let Some(page) = retired {
            self.compact(page);
        }
        Ok(None)
    }

    /// Frees entry `h` (already unlinked from the order); returns its item.
    fn release(&mut self, h: H) -> (OrderedScore, Bytes) {
        let (score, member) = self.entry(h);
        let item = (score, Bytes::copy_from_slice(member));
        if let Ok(entry) = self.dict.find_entry(hash_member(member), |&x| x == h) {
            entry.remove();
        }
        self.len -= 1;
        if self.len == 0 {
            *self = Self::default();
        } else if let Some(page) = self.store.release(h) {
            self.compact(page);
        }
        item
    }

    /// Compacts `page`, repointing each moved entry's run slot and bucket.
    fn compact(&mut self, page: usize) {
        // Locate everything while the page is still intact.
        let fixes: Vec<(H, H, u64, usize, usize)> = self
            .store
            .moves(page)
            .map(|(old, new, p)| {
                let (score, member) = split(p);
                let (ri, pos) = self.lower_bound(score, member);
                debug_assert_eq!(self.runs[ri][pos], old);
                (old, new, hash_member(member), ri, pos)
            })
            .collect();
        self.store.compact(page);
        for (old, new, hash, ri, pos) in fixes {
            self.runs[ri][pos] = new;
            if let Some(slot) = self.dict.find_mut(hash, |&x| x == old) {
                *slot = new;
            } else {
                debug_assert!(false, "live entry without a bucket");
            }
        }
    }

    fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let h = self.find(member)?;
        self.unplace(h);
        Some(self.release(h).0.0)
    }

    fn pop_first(&mut self) -> Option<(OrderedScore, Bytes)> {
        let h = *self.runs.first()?.first()?;
        self.unplace_at(0, 0);
        Some(self.release(h))
    }

    fn pop_last(&mut self) -> Option<(OrderedScore, Bytes)> {
        let ri = self.runs.len().checked_sub(1)?;
        let pos = self.runs[ri].len() - 1;
        let h = self.runs[ri][pos];
        self.unplace_at(ri, pos);
        Some(self.release(h))
    }

    fn rank(&self, member: &[u8]) -> Option<usize> {
        let h = self.find(member)?;
        let (score, m) = self.entry(h);
        let (ri, pos) = self.lower_bound(score, m);
        Some(self.runs[..ri].iter().map(Vec::len).sum::<usize>() + pos)
    }

    fn iter_at(&self, ri: usize, pos: usize) -> RawIter<'_, H> {
        let first: &[H] = self.runs.get(ri).map_or(&[], |r| &r[pos..]);
        let rest = self.runs.get(ri + 1..).unwrap_or(&[]);
        RawIter {
            it: first.iter().chain(rest.iter().flatten()),
            store: &self.store,
        }
    }

    fn iter(&self) -> RawIter<'_, H> {
        self.iter_at(0, 0)
    }

    fn iter_from_score(&self, min: f64) -> RawIter<'_, H> {
        let (ri, pos) = self.lower_bound(OrderedScore(min), &[]);
        self.iter_at(ri, pos)
    }

    #[cfg(test)]
    fn check_runs(&self) {
        assert!(
            self.runs
                .iter()
                .all(|r| !r.is_empty() && r.len() <= CHUNK_MAX)
        );
        assert!(
            self.runs.iter().all(|r| r.capacity() <= CHUNK_MAX + 1),
            "a run reallocated"
        );
    }
}

type HandleIter<'a, H> =
    std::iter::Chain<std::slice::Iter<'a, H>, std::iter::Flatten<std::slice::Iter<'a, Vec<H>>>>;

struct RawIter<'a, H> {
    it: HandleIter<'a, H>,
    store: &'a PageStore<H>,
}

impl<'a, H: Handle> Iterator for RawIter<'a, H> {
    type Item = (OrderedScore, &'a [u8]);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|&h| split(self.store.get(h)))
    }
}

impl<H: Handle> DoubleEndedIterator for RawIter<'_, H> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.it.next_back().map(|&h| split(self.store.get(h)))
    }
}

#[derive(Clone)]
enum Repr {
    Narrow(Inner<u32>),
    Wide(Inner<u64>),
}

#[derive(Clone)]
pub struct ZIndex(Repr);

impl Default for ZIndex {
    fn default() -> Self {
        ZIndex(Repr::Narrow(Inner::default()))
    }
}

impl std::fmt::Debug for ZIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// Dispatches a read-only call to the narrow or wide representation.
macro_rules! with {
    ($self:expr, $z:ident => $e:expr) => {
        match &$self.0 {
            Repr::Narrow($z) => $e,
            Repr::Wide($z) => $e,
        }
    };
}

/// Dispatches a mutating call to the narrow or wide representation.
macro_rules! with_mut {
    ($self:expr, $z:ident => $e:expr) => {
        match &mut $self.0 {
            Repr::Narrow($z) => $e,
            Repr::Wide($z) => $e,
        }
    };
}

impl ZIndex {
    pub fn with_capacity(n: usize) -> Self {
        ZIndex(Repr::Narrow(Inner {
            dict: hashbrown::HashTable::with_capacity(n),
            ..Inner::default()
        }))
    }

    #[inline]
    pub fn len(&self) -> usize {
        with!(self, z => z.len)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, member: &[u8]) -> Option<f64> {
        with!(self, z => z.find(member).map(|h| z.entry(h).0.0))
    }

    #[inline]
    pub fn contains_key(&self, member: &[u8]) -> bool {
        with!(self, z => z.find(member).is_some())
    }

    /// Sets `member`'s score; returns the previous one.
    pub fn insert(&mut self, member: &[u8], score: f64) -> Option<f64> {
        if let Repr::Narrow(z) = &mut self.0 {
            match z.insert(member, score) {
                Ok(old) => return old,
                Err(()) => self.widen(),
            }
        }
        let Repr::Wide(z) = &mut self.0 else {
            unreachable!("narrow inserts returned above or widened")
        };
        z.insert(member, score)
            .expect("u64 handles address every page")
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
        for (score, member) in n.iter() {
            let fresh = w.insert(member, score.0).expect("u64 handles");
            debug_assert!(fresh.is_none());
        }
        self.0 = Repr::Wide(w);
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        with_mut!(self, z => z.remove(member))
    }

    pub fn pop_first(&mut self) -> Option<(OrderedScore, Bytes)> {
        with_mut!(self, z => z.pop_first())
    }

    pub fn pop_last(&mut self) -> Option<(OrderedScore, Bytes)> {
        with_mut!(self, z => z.pop_last())
    }

    /// Zero-based rank of `member` in ascending order.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        with!(self, z => z.rank(member))
    }

    /// All items in ascending (score, member) order.
    pub fn iter(&self) -> ZIter<'_> {
        ZIter(match &self.0 {
            Repr::Narrow(z) => IterRepr::Narrow(z.iter()),
            Repr::Wide(z) => IterRepr::Wide(z.iter()),
        })
    }

    /// Items from the first with a score of at least `min`, ascending.
    pub fn iter_from_score(&self, min: f64) -> ZIter<'_> {
        ZIter(match &self.0 {
            Repr::Narrow(z) => IterRepr::Narrow(z.iter_from_score(min)),
            Repr::Wide(z) => IterRepr::Wide(z.iter_from_score(min)),
        })
    }
}

enum IterRepr<'a> {
    Narrow(RawIter<'a, u32>),
    Wide(RawIter<'a, u64>),
}

/// `(score, member)` items of a [`ZIndex`] in order.
pub struct ZIter<'a>(IterRepr<'a>);

impl<'a> Iterator for ZIter<'a> {
    type Item = (OrderedScore, &'a [u8]);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            IterRepr::Narrow(i) => i.next(),
            IterRepr::Wide(i) => i.next(),
        }
    }
}

impl DoubleEndedIterator for ZIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            IterRepr::Narrow(i) => i.next_back(),
            IterRepr::Wide(i) => i.next_back(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn ordered(z: &ZIndex) -> Vec<(f64, Vec<u8>)> {
        z.iter().map(|(s, m)| (s.0, m.to_vec())).collect()
    }

    /// Reference: member -> score, ordered by (score, member) on demand.
    fn model_order(m: &BTreeMap<Vec<u8>, f64>) -> Vec<(f64, Vec<u8>)> {
        let mut v: Vec<(f64, Vec<u8>)> = m.iter().map(|(k, s)| (*s, k.clone())).collect();
        v.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        v
    }

    #[test]
    fn insert_update_remove_and_order() {
        let mut z = ZIndex::default();
        assert_eq!(z.insert(b"b", 2.0), None);
        assert_eq!(z.insert(b"a", 2.0), None);
        assert_eq!(z.insert(b"c", 1.0), None);
        assert_eq!(z.insert(b"a", 0.5), Some(2.0));
        assert_eq!(
            ordered(&z),
            vec![
                (0.5, b"a".to_vec()),
                (1.0, b"c".to_vec()),
                (2.0, b"b".to_vec())
            ]
        );
        assert_eq!(z.get(b"c"), Some(1.0));
        assert_eq!(z.rank(b"b"), Some(2));
        assert_eq!(z.remove(b"c"), Some(1.0));
        assert_eq!(z.remove(b"c"), None);
        assert_eq!(z.len(), 2);
        assert_eq!(
            z.pop_first().map(|(s, m)| (s.0, m.to_vec())),
            Some((0.5, b"a".to_vec()))
        );
        assert_eq!(
            z.pop_last().map(|(s, m)| (s.0, m.to_vec())),
            Some((2.0, b"b".to_vec()))
        );
        assert!(z.is_empty());
        assert_eq!(z.pop_first().map(|(s, _)| s.0), None);
    }

    #[test]
    fn iter_from_score_starts_at_the_first_member_with_that_score() {
        let mut z = ZIndex::default();
        for i in 0..1000 {
            z.insert(format!("m{i:04}").as_bytes(), (i / 10) as f64);
        }
        let from: Vec<_> = z
            .iter_from_score(50.0)
            .take(3)
            .map(|(s, m)| (s.0, m.to_vec()))
            .collect();
        assert_eq!(
            from,
            vec![
                (50.0, b"m0500".to_vec()),
                (50.0, b"m0501".to_vec()),
                (50.0, b"m0502".to_vec())
            ]
        );
        assert_eq!(z.iter_from_score(1e9).count(), 0);
        assert_eq!(z.iter_from_score(-1e9).count(), 1000);
        assert_eq!(
            z.iter().next_back().map(|(_, m)| m.to_vec()),
            Some(b"m0999".to_vec())
        );
    }

    /// Small key space and score range so updates, ties and removals are
    /// frequent, runs split and merge, and pages compact repeatedly. Every
    /// 25th member is over 1 KiB (a page of its own).
    fn run_model(seed: u64) -> ZIndex {
        let mut z = ZIndex::default();
        let mut model: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
        let mut x: u64 = seed;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for step in 0..40_000 {
            let r = next();
            let k = r % 700;
            let key = if k % 25 == 0 {
                format!("{}{k}", "L".repeat(1100)).into_bytes()
            } else {
                format!("k{k}").into_bytes()
            };
            let score = ((r >> 20) % 50) as f64 - 25.0;
            match (r >> 40) % 10 {
                0..=5 => assert_eq!(z.insert(&key, score), model.insert(key, score)),
                6 | 7 => assert_eq!(z.remove(&key), model.remove(&key)),
                8 => {
                    let want = model_order(&model).first().cloned();
                    let got = z.pop_first().map(|(s, m)| (s.0, m.to_vec()));
                    assert_eq!(got, want);
                    if let Some((_, k)) = want {
                        model.remove(&k);
                    }
                }
                _ => {
                    let want = model_order(&model).last().cloned();
                    let got = z.pop_last().map(|(s, m)| (s.0, m.to_vec()));
                    assert_eq!(got, want);
                    if let Some((_, k)) = want {
                        model.remove(&k);
                    }
                }
            }
            assert_eq!(z.len(), model.len());
            if step % 997 == 0 {
                let want = model_order(&model);
                assert_eq!(ordered(&z), want, "order at step {step}");
                for (i, (s, k)) in want.iter().enumerate() {
                    assert_eq!(z.get(k), Some(*s));
                    assert_eq!(z.rank(k), Some(i));
                }
                with!(z, inner => inner.check_runs());
            }
        }
        z
    }

    #[test]
    fn matches_a_reference_model_under_random_operations() {
        let z = run_model(0x9E37_79B9_7F4A_7C15);
        assert!(matches!(z.0, Repr::Narrow(_)));
    }

    #[test]
    fn widens_past_the_narrow_page_limit_and_stays_correct() {
        crate::pagestore::NARROW_LIMIT.with(|c| c.set(4));
        let z = run_model(0x2545_F491_4F6C_DD1D);
        crate::pagestore::NARROW_LIMIT.with(|c| c.set(1 << 20));
        assert!(
            matches!(z.0, Repr::Wide(_)),
            "a 4-page limit must force widening"
        );
    }

    #[test]
    fn drains_with_compaction_keeping_order() {
        let mut z = ZIndex::default();
        for i in 0..20_000u32 {
            z.insert(format!("member:{i:05}").as_bytes(), (i % 97) as f64);
        }
        // Remove two of every three, then rescore some survivors.
        for i in (0..20_000u32).filter(|i| i % 3 != 0) {
            assert!(z.remove(format!("member:{i:05}").as_bytes()).is_some());
        }
        for i in (0..20_000u32).step_by(9) {
            assert!(
                z.insert(format!("member:{i:05}").as_bytes(), -(i as f64))
                    .is_some()
            );
        }
        let items = ordered(&z);
        assert_eq!(items.len(), z.len());
        assert!(items.windows(2).all(|w| {
            w[0].0.total_cmp(&w[1].0).then_with(|| w[0].1.cmp(&w[1].1)) == Ordering::Less
        }));
        for (i, (s, m)) in items.iter().enumerate() {
            assert_eq!(z.get(m), Some(*s));
            assert_eq!(z.rank(m), Some(i));
        }
        let mut n = 0;
        while z.pop_last().is_some() {
            n += 1;
        }
        assert_eq!(n, items.len());
        assert!(z.is_empty());
    }
}
