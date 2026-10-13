//! Ordered member index for large sorted sets, storing each member once.
//!
//! The previous encoding kept every member twice, as a `HashMap` key (for
//! `ZSCORE`) and inside a `BTreeSet<(score, member)>` (for ordered access).
//! Here each `(score, member)` pair lives once in a slot array with stable
//! `u32` ids; a hash table maps members to ids (4 bytes per entry, members
//! compared through the slot), and the order is kept as a list of sorted
//! runs of ids, each at most [`CHUNK_MAX`] long.
//!
//! - `get`/`contains_key`: one hash lookup.
//! - `insert`/`remove`: hash lookup, binary search over the runs and within
//!   one run, then a `memmove` of at most `CHUNK_MAX` ids.
//! - Rank and offset access walk the run lengths: O(len / CHUNK_MAX).
//!
//! Iteration yields `&(OrderedScore, CompactKey)` in (score, member) order,
//! the same items a `BTreeSet<(OrderedScore, CompactKey)>` would.

use crate::compact::CompactKey;
use crate::table::OrderedScore;
use std::cmp::Ordering;

/// Longest run of ids; a run that grows past it is split in two. Runs are
/// allocated at `CHUNK_MAX + 1` ids = 512 bytes, an exact allocator size
/// class (one id more spills into the next class, 640 bytes).
const CHUNK_MAX: usize = 127;

/// Slots per page: 64 x 32 bytes = 2048 bytes, an exact size class. A single
/// growing `Vec` was on average ~70% full after doubling, and its large
/// allocation was then rounded up again by the allocator (16.6 KB -> 20 KB
/// for 375 members).
const PAGE: usize = 64;

type Item = (OrderedScore, CompactKey);

#[inline(always)]
fn hash_member(m: &[u8]) -> u64 {
    fxhash::hash64(m)
}

#[inline(always)]
fn slot_in(pages: &[Vec<Item>], id: u32) -> &Item {
    &pages[id as usize / PAGE][id as usize % PAGE]
}

#[derive(Clone, Default)]
pub struct ZIndex {
    /// `(score, member)` by id, in pages of `PAGE` slots (id = page * PAGE +
    /// offset). Freed ids hold an empty member and are reused from `free`.
    pages: Vec<Vec<Item>>,
    free: Vec<u32>,
    /// Member -> id.
    dict: hashbrown::HashTable<u32>,
    /// Ids in (score, member) order, split into sorted runs.
    runs: Vec<Vec<u32>>,
    len: usize,
}

impl std::fmt::Debug for ZIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl ZIndex {
    pub fn with_capacity(n: usize) -> Self {
        ZIndex {
            pages: Vec::with_capacity(n.div_ceil(PAGE)),
            free: Vec::new(),
            dict: hashbrown::HashTable::with_capacity(n),
            runs: Vec::new(),
            len: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    fn slot(&self, id: u32) -> &Item {
        slot_in(&self.pages, id)
    }

    #[inline(always)]
    fn slot_mut(&mut self, id: u32) -> &mut Item {
        &mut self.pages[id as usize / PAGE][id as usize % PAGE]
    }

    #[inline]
    fn find_id(&self, member: &[u8]) -> Option<u32> {
        let pages = &self.pages;
        self.dict
            .find(hash_member(member), |&id| {
                slot_in(pages, id).1.as_slice() == member
            })
            .copied()
    }

    #[inline]
    pub fn get(&self, member: &[u8]) -> Option<f64> {
        self.find_id(member).map(|id| self.slot(id).0.0)
    }

    #[inline]
    pub fn contains_key(&self, member: &[u8]) -> bool {
        self.find_id(member).is_some()
    }

    #[inline]
    fn cmp_id(&self, id: u32, score: OrderedScore, member: &[u8]) -> Ordering {
        let (s, m) = self.slot(id);
        s.cmp(&score).then_with(|| m.as_slice().cmp(member))
    }

    /// Run and position of the first element not less than `(score, member)`.
    /// `(runs.len(), 0)` when every element is less.
    fn lower_bound(&self, score: OrderedScore, member: &[u8]) -> (usize, usize) {
        let ri = self.runs.partition_point(|r| {
            self.cmp_id(*r.last().expect("runs are never empty"), score, member) == Ordering::Less
        });
        if ri == self.runs.len() {
            return (ri, 0);
        }
        let pos =
            self.runs[ri].partition_point(|&id| self.cmp_id(id, score, member) == Ordering::Less);
        (ri, pos)
    }

    #[inline]
    fn new_run() -> Vec<u32> {
        Vec::with_capacity(CHUNK_MAX + 1)
    }

    /// Links slot `id` into the order.
    fn place(&mut self, id: u32) {
        if self.runs.is_empty() {
            let mut run = Self::new_run();
            run.push(id);
            self.runs.push(run);
            return;
        }
        let (score, member) = self.slot(id);
        let (mut ri, mut pos) = self.lower_bound(*score, member.as_slice());
        if ri == self.runs.len() {
            ri -= 1;
            pos = self.runs[ri].len();
        }
        let run = &mut self.runs[ri];
        run.insert(pos, id);
        if run.len() > CHUNK_MAX {
            // Split into fresh exact-size runs rather than letting `Vec` grow.
            let mut tail = Self::new_run();
            tail.extend_from_slice(&run[CHUNK_MAX / 2..]);
            run.truncate(CHUNK_MAX / 2);
            self.runs.insert(ri + 1, tail);
        }
    }

    /// Unlinks slot `id` from the order.
    fn unplace(&mut self, id: u32) {
        let (score, member) = self.slot(id);
        let (ri, pos) = self.lower_bound(*score, member.as_slice());
        debug_assert_eq!(self.runs[ri][pos], id);
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

    /// Sets `member`'s score; returns the previous one.
    pub fn insert(&mut self, member: CompactKey, score: f64) -> Option<f64> {
        if let Some(id) = self.find_id(member.as_slice()) {
            let old = self.slot(id).0.0;
            if old.total_cmp(&score) != Ordering::Equal {
                self.unplace(id);
                self.slot_mut(id).0 = OrderedScore(score);
                self.place(id);
            }
            return Some(old);
        }
        let h = hash_member(member.as_slice());
        let id = match self.free.pop() {
            Some(id) => {
                *self.slot_mut(id) = (OrderedScore(score), member);
                id
            }
            None => {
                if self.pages.last().is_none_or(|p| p.len() == PAGE) {
                    self.pages.push(Vec::with_capacity(PAGE));
                }
                let last = self.pages.len() - 1;
                let id = u32::try_from(last * PAGE + self.pages[last].len())
                    .expect("sorted set exceeds u32 members");
                self.pages[last].push((OrderedScore(score), member));
                id
            }
        };
        let pages = &self.pages;
        self.dict
            .insert_unique(h, id, |&i| hash_member(slot_in(pages, i).1.as_slice()));
        self.place(id);
        self.len += 1;
        None
    }

    /// Frees slot `id` (already unlinked from the order) and returns its item.
    fn release(&mut self, id: u32) -> Item {
        let member = slot_in(&self.pages, id).1.as_slice();
        if let Ok(entry) = self.dict.find_entry(hash_member(member), |&i| i == id) {
            entry.remove();
        }
        let item = std::mem::replace(
            self.slot_mut(id),
            (OrderedScore(0.0), CompactKey::default()),
        );
        self.len -= 1;
        if self.len == 0 {
            *self = ZIndex::default();
        } else {
            self.free.push(id);
        }
        item
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let id = self.find_id(member)?;
        self.unplace(id);
        Some(self.release(id).0.0)
    }

    pub fn pop_first(&mut self) -> Option<Item> {
        let id = *self.runs.first()?.first()?;
        self.unplace_at(0, 0);
        Some(self.release(id))
    }

    pub fn pop_last(&mut self) -> Option<Item> {
        let ri = self.runs.len().checked_sub(1)?;
        let pos = self.runs[ri].len() - 1;
        let id = self.runs[ri][pos];
        self.unplace_at(ri, pos);
        Some(self.release(id))
    }

    /// Zero-based rank of `member` in ascending order.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let id = self.find_id(member)?;
        let (score, m) = self.slot(id);
        let (ri, pos) = self.lower_bound(*score, m.as_slice());
        Some(self.runs[..ri].iter().map(Vec::len).sum::<usize>() + pos)
    }

    /// All items in ascending (score, member) order.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Item> + '_ {
        self.runs.iter().flatten().map(move |&id| self.slot(id))
    }

    /// Items from the first with a score of at least `min`, ascending.
    pub fn iter_from_score(&self, min: f64) -> impl Iterator<Item = &Item> + '_ {
        let (ri, pos) = self.lower_bound(OrderedScore(min), &[]);
        let first: &[u32] = self.runs.get(ri).map_or(&[], |r| &r[pos..]);
        let rest = self.runs.get(ri + 1..).unwrap_or(&[]);
        first
            .iter()
            .chain(rest.iter().flatten())
            .map(move |&id| self.slot(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn ck(s: &str) -> CompactKey {
        CompactKey::new(s.as_bytes())
    }

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
        assert_eq!(z.insert(ck("b"), 2.0), None);
        assert_eq!(z.insert(ck("a"), 2.0), None);
        assert_eq!(z.insert(ck("c"), 1.0), None);
        assert_eq!(z.insert(ck("a"), 0.5), Some(2.0));
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
            z.insert(ck(&format!("m{i:04}")), (i / 10) as f64);
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

    #[test]
    fn matches_a_reference_model_under_random_operations() {
        // Small key space and score range so updates, ties and removals are
        // frequent and runs split and merge repeatedly.
        let mut z = ZIndex::default();
        let mut model: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for step in 0..40_000 {
            let r = next();
            let key = format!("k{}", r % 700).into_bytes();
            let score = ((r >> 20) % 50) as f64 - 25.0;
            match (r >> 40) % 10 {
                0..=5 => {
                    assert_eq!(
                        z.insert(CompactKey::new(&key), score),
                        model.insert(key, score)
                    );
                }
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
                assert!(z.runs.iter().all(|r| !r.is_empty() && r.len() <= CHUNK_MAX));
                assert!(
                    z.runs.iter().all(|r| r.capacity() <= CHUNK_MAX + 1),
                    "a run reallocated"
                );
                assert!(
                    z.pages.iter().all(|p| p.capacity() == PAGE),
                    "a page reallocated"
                );
            }
        }
    }
}
