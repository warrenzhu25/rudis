//! Large-set storage: members packed back to back in 4 KiB pages, indexed by
//! a hash table whose buckets hold each member's *location* directly.
//!
//! Like Dragonfly's `DenseSet`, a bucket points straight at the member: no id,
//! no side location table. A bucket is a `u32` handle, `page << 12 | offset`
//! into a [`PageStore`]; a 5-byte member costs a 6-byte page entry plus a
//! 4-byte bucket (and the table's control byte and slack).
//!
//! Removing a member sets the entry's dead bit; a half-dead page is compacted
//! in place and each moved member re-hashed to find and repoint its bucket.
//!
//! `u32` handles address 2^20 pages. A set that would need more switches,
//! once, to `u64` handles, so there is no size limit.

use crate::pagestore::{Handle, PageStore};
use bytes::Bytes;

#[inline(always)]
fn hash_member(m: &[u8]) -> u64 {
    fxhash::hash64(m)
}

#[derive(Clone, Debug)]
struct Inner<H> {
    dict: hashbrown::HashTable<H>,
    store: PageStore<H>,
    len: usize,
}

impl<H> Default for Inner<H> {
    fn default() -> Self {
        Inner {
            dict: hashbrown::HashTable::new(),
            store: PageStore::default(),
            len: 0,
        }
    }
}

impl<H: Handle> Inner<H> {
    #[inline]
    fn find(&self, m: &[u8], hash: u64) -> Option<H> {
        let store = &self.store;
        self.dict.find(hash, |&h| store.get(h) == m).copied()
    }

    /// Adds `m` (known absent). False, with nothing changed, when its
    /// location would not fit a handle `H`.
    fn insert_new(&mut self, m: &[u8], hash: u64) -> bool {
        let Some((h, retired)) = self.store.insert(&[m]) else {
            return false;
        };
        let store = &self.store;
        self.dict
            .insert_unique(hash, h, |&x| hash_member(store.get(x)));
        self.len += 1;
        if let Some(page) = retired {
            self.compact(page);
        }
        true
    }

    /// Frees the entry at `h`, whose bucket the caller already removed.
    fn release(&mut self, h: H) {
        self.len -= 1;
        if self.len == 0 {
            *self = Self::default();
            return;
        }
        if let Some(page) = self.store.release(h) {
            self.compact(page);
        }
    }

    /// Compacts `page`, repointing each moved member's bucket.
    fn compact(&mut self, page: usize) {
        let fixes: Vec<(H, H, u64)> = self
            .store
            .moves(page)
            .map(|(old, new, m)| (old, new, hash_member(m)))
            .collect();
        self.store.compact(page);
        for (old, new, hash) in fixes {
            if let Some(slot) = self.dict.find_mut(hash, |&x| x == old) {
                *slot = new;
            } else {
                debug_assert!(false, "live entry without a bucket");
            }
        }
    }

    fn remove(&mut self, m: &[u8], hash: u64) -> bool {
        let store = &self.store;
        match self.dict.find_entry(hash, |&h| store.get(h) == m) {
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
        let m = Bytes::copy_from_slice(self.store.get(h));
        self.release(h);
        Some(m)
    }

    fn iter(&self) -> InnerIter<'_, H> {
        InnerIter {
            it: self.dict.iter(),
            store: &self.store,
        }
    }
}

struct InnerIter<'a, H> {
    it: hashbrown::hash_table::Iter<'a, H>,
    store: &'a PageStore<H>,
}

impl<'a, H: Handle> Iterator for InnerIter<'a, H> {
    type Item = &'a [u8];
    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        self.it.next().map(|&h| self.store.get(h))
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
        crate::pagestore::NARROW_LIMIT.with(|c| c.set(4));
        let s = run_model(60_000, 0x2545_F491_4F6C_DD1D);
        crate::pagestore::NARROW_LIMIT.with(|c| c.set(1 << 20));
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
