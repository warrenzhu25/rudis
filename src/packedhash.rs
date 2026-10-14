//! Large-hash storage: field/value pairs packed in a [`PageStore`], indexed by
//! a hash table of entry handles, as [`crate::packedset`] does for sets.
//!
//! The previous `HashMap<Bytes, Bytes>` spent a 64-byte bucket per field
//! (plus load-factor slack) and two heap allocations, or two views pinning
//! whole request buffers. Here a pair is one page entry whose payload is
//! `[varint flen][field][value]`, and a bucket is a 4-byte handle.
//!
//! An update to a value of the same length rewrites it in place; otherwise
//! the pair is re-appended and its bucket repointed. A half-dead page is
//! compacted in place, each moved pair re-hashed to repoint its bucket.
//! `u32` handles address 2^20 pages; a bigger hash switches once to `u64`.

use crate::pagestore::{Handle, PageStore};
use bytes::Bytes;

#[inline(always)]
fn hash_field(f: &[u8]) -> u64 {
    fxhash::hash64(f)
}

#[inline]
fn write_varint(buf: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

/// (field, value) of an entry payload.
#[inline(always)]
fn split(p: &[u8]) -> (&[u8], &[u8]) {
    let b = p[0];
    let (flen, n) = if b < 0x80 {
        (b as usize, 1)
    } else {
        let mut v = (b & 0x7F) as usize;
        let mut shift = 7;
        let mut i = 1;
        loop {
            let b = p[i];
            v |= ((b & 0x7F) as usize) << shift;
            i += 1;
            if b & 0x80 == 0 {
                break (v, i);
            }
            shift += 7;
        }
    };
    (&p[n..n + flen], &p[n + flen..])
}

#[inline(always)]
fn field_of(p: &[u8]) -> &[u8] {
    split(p).0
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
    fn find(&self, f: &[u8], hash: u64) -> Option<H> {
        let store = &self.store;
        self.dict
            .find(hash, |&h| field_of(store.get(h)) == f)
            .copied()
    }

    /// Appends a pair; `None`, with nothing changed, when its location
    /// would not fit `H`. Compacts a retired page.
    fn append(&mut self, f: &[u8], v: &[u8]) -> Option<H> {
        let mut prefix = Vec::with_capacity(5);
        write_varint(&mut prefix, f.len());
        let (h, retired) = self.store.insert(&[&prefix, f, v])?;
        if let Some(page) = retired {
            self.compact(page);
        }
        Some(h)
    }

    /// Sets `f` to `v`. `Ok(true)` if added; `Err(())`, with nothing
    /// changed, when a new entry's location would not fit `H`.
    fn insert(&mut self, f: &[u8], v: &[u8]) -> Result<bool, ()> {
        let hash = hash_field(f);
        if let Some(old) = self.find(f, hash) {
            let p = self.store.get_mut(old);
            let (_, cur) = split(p);
            if cur.len() == v.len() {
                let at = p.len() - v.len();
                p[at..].copy_from_slice(v);
                return Ok(false);
            }
            // Re-append first (it may fail), then retire the old entry.
            let new = self.append(f, v).ok_or(())?;
            // `append` may have compacted a page: look the bucket up again.
            let store = &self.store;
            let slot = self
                .dict
                .find_mut(hash, |&h| h != new && field_of(store.get(h)) == f)
                .expect("updated field has a bucket");
            let old = std::mem::replace(slot, new);
            if let Some(page) = self.store.release(old) {
                self.compact(page);
            }
            return Ok(false);
        }
        let h = self.append(f, v).ok_or(())?;
        let store = &self.store;
        self.dict
            .insert_unique(hash, h, |&x| hash_field(field_of(store.get(x))));
        self.len += 1;
        Ok(true)
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

    /// Compacts `page`, repointing each moved pair's bucket.
    fn compact(&mut self, page: usize) {
        let fixes: Vec<(H, H, u64)> = self
            .store
            .moves(page)
            .map(|(old, new, p)| (old, new, hash_field(field_of(p))))
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

    fn take(&mut self, f: &[u8]) -> Option<Bytes> {
        let store = &self.store;
        let entry = self
            .dict
            .find_entry(hash_field(f), |&h| field_of(store.get(h)) == f)
            .ok()?;
        let (h, _) = entry.remove();
        let v = Bytes::copy_from_slice(split(self.store.get(h)).1);
        self.release(h);
        Some(v)
    }

    fn remove(&mut self, f: &[u8]) -> bool {
        let store = &self.store;
        match self
            .dict
            .find_entry(hash_field(f), |&h| field_of(store.get(h)) == f)
        {
            Ok(entry) => {
                let (h, _) = entry.remove();
                self.release(h);
                true
            }
            Err(_) => false,
        }
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
    type Item = (&'a [u8], &'a [u8]);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|&h| split(self.store.get(h)))
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
pub struct PackedHash(Repr);

impl Default for PackedHash {
    fn default() -> Self {
        PackedHash(Repr::Narrow(Inner::default()))
    }
}

macro_rules! with {
    ($self:expr, $z:ident => $e:expr) => {
        match &$self.0 {
            Repr::Narrow($z) => $e,
            Repr::Wide($z) => $e,
        }
    };
}

macro_rules! with_mut {
    ($self:expr, $z:ident => $e:expr) => {
        match &mut $self.0 {
            Repr::Narrow($z) => $e,
            Repr::Wide($z) => $e,
        }
    };
}

impl PackedHash {
    pub fn with_capacity(n: usize) -> Self {
        PackedHash(Repr::Narrow(Inner {
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
    pub fn get(&self, f: &[u8]) -> Option<&[u8]> {
        with!(self, z => z.find(f, hash_field(f)).map(|h| split(z.store.get(h)).1))
    }

    #[inline]
    pub fn contains_key(&self, f: &[u8]) -> bool {
        with!(self, z => z.find(f, hash_field(f)).is_some())
    }

    /// Sets `f` to `v`; true if the field was added.
    pub fn insert(&mut self, f: &[u8], v: &[u8]) -> bool {
        if let Repr::Narrow(z) = &mut self.0 {
            match z.insert(f, v) {
                Ok(added) => return added,
                Err(()) => self.widen(),
            }
        }
        let Repr::Wide(z) = &mut self.0 else {
            unreachable!("narrow inserts returned above or widened")
        };
        z.insert(f, v).expect("u64 handles address every page")
    }

    /// Switches to `u64` handles: the hash outgrew 2^20 pages.
    #[cold]
    fn widen(&mut self) {
        let Repr::Narrow(n) = &self.0 else {
            return;
        };
        let mut w = Inner::<u64> {
            dict: hashbrown::HashTable::with_capacity(n.len + 1),
            ..Inner::default()
        };
        for (f, v) in n.iter() {
            let added = w.insert(f, v).expect("u64 handles");
            debug_assert!(added);
        }
        self.0 = Repr::Wide(w);
    }

    /// Removes `f`, returning its value.
    pub fn take(&mut self, f: &[u8]) -> Option<Bytes> {
        with_mut!(self, z => z.take(f))
    }

    /// Removes `f`; true if it was present.
    pub fn remove(&mut self, f: &[u8]) -> bool {
        with_mut!(self, z => z.remove(f))
    }

    /// Pairs in hash-table order.
    pub fn iter(&self) -> PackedHashIter<'_> {
        PackedHashIter(match &self.0 {
            Repr::Narrow(z) => IterRepr::Narrow(z.iter()),
            Repr::Wide(z) => IterRepr::Wide(z.iter()),
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.iter().map(|(_, v)| v)
    }
}

impl PartialEq for PackedHash {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().all(|(k, v)| other.get(k) == Some(v))
    }
}

impl Eq for PackedHash {}

impl<'a> IntoIterator for &'a PackedHash {
    type Item = (&'a [u8], &'a [u8]);
    type IntoIter = PackedHashIter<'a>;
    fn into_iter(self) -> PackedHashIter<'a> {
        self.iter()
    }
}

impl<'a> FromIterator<(&'a [u8], &'a [u8])> for PackedHash {
    fn from_iter<I: IntoIterator<Item = (&'a [u8], &'a [u8])>>(it: I) -> Self {
        let mut h = PackedHash::default();
        for (f, v) in it {
            h.insert(f, v);
        }
        h
    }
}

enum IterRepr<'a> {
    Narrow(InnerIter<'a, u32>),
    Wide(InnerIter<'a, u64>),
}

pub struct PackedHashIter<'a>(IterRepr<'a>);

impl<'a> Iterator for PackedHashIter<'a> {
    type Item = (&'a [u8], &'a [u8]);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
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

impl ExactSizeIterator for PackedHashIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn run_model(seed: u64) -> PackedHash {
        let mut h = PackedHash::default();
        let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut x: u64 = seed;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for step in 0..60_000 {
            let r = next();
            let f = format!("field:{}", r % 2000).into_bytes();
            // Mixed lengths: short, around the one-byte varint edge, and
            // over 1 KiB (an own page).
            let vlen = match (r >> 8) % 6 {
                0 => 0,
                1 | 2 => 5,
                3 => 127,
                4 => 300,
                _ => 1500,
            };
            let v: Vec<u8> = (0..vlen).map(|i| (i as u8) ^ (r >> 16) as u8).collect();
            match (r >> 32) % 6 {
                0..=2 => assert_eq!(h.insert(&f, &v), model.insert(f.clone(), v).is_none()),
                3 => assert_eq!(h.take(&f).map(|b| b.to_vec()), model.remove(&f)),
                _ => assert_eq!(h.remove(&f), model.remove(&f).is_some()),
            }
            assert_eq!(h.len(), model.len());
            assert_eq!(h.get(&f).map(<[u8]>::to_vec), model.get(&f).cloned());
            if step % 1009 == 0 {
                let got: HashMap<Vec<u8>, Vec<u8>> =
                    h.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
                assert_eq!(got, model, "step {step}");
                assert_eq!(h.iter().len(), model.len());
            }
        }
        h
    }

    #[test]
    fn matches_a_map_model_under_random_operations() {
        let h = run_model(0x5851_F42D_4C95_7F2D);
        assert!(matches!(h.0, Repr::Narrow(_)));
    }

    #[test]
    fn widens_past_the_narrow_page_limit_and_stays_correct() {
        crate::pagestore::NARROW_LIMIT.with(|c| c.set(4));
        let h = run_model(0x2545_F491_4F6C_DD1D);
        crate::pagestore::NARROW_LIMIT.with(|c| c.set(1 << 20));
        assert!(
            matches!(h.0, Repr::Wide(_)),
            "a 4-page limit must force widening"
        );
    }
}
