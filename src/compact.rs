//! Compact owned byte strings for table keys.
//!
//! [`CompactKey`] is 24 bytes, against 32 for `Bytes`. Strings of up to
//! [`INLINE_CAP`] bytes are stored inline with no heap allocation; longer
//! ones live in an exact-size `Box<[u8]>`. Most real keys are short, so
//! storing them inline saves a 16–32 B allocation per key, and comparing a
//! key during a lookup doesn't need a pointer dereference (a likely cache
//! miss). Written in safe Rust only.

use bytes::Bytes;
use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

/// Longest string stored inline. Chosen so the enum is exactly 24 bytes:
/// 1 B tag + 1 B length + 22 B data.
pub const INLINE_CAP: usize = 22;

#[derive(Clone)]
pub enum CompactKey {
    Inline { len: u8, data: [u8; INLINE_CAP] },
    Heap(Box<[u8]>),
}

const _: () = assert!(std::mem::size_of::<CompactKey>() == 24);
const _: () = assert!(std::mem::size_of::<Option<CompactKey>>() == 24);

impl CompactKey {
    #[inline]
    pub fn new(s: &[u8]) -> Self {
        if s.len() <= INLINE_CAP {
            let mut data = [0u8; INLINE_CAP];
            data[..s.len()].copy_from_slice(s);
            CompactKey::Inline {
                len: s.len() as u8,
                data,
            }
        } else {
            CompactKey::Heap(Box::from(s))
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            CompactKey::Inline { len, data } => &data[..*len as usize],
            CompactKey::Heap(b) => b,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            CompactKey::Inline { len, .. } => *len as usize,
            CompactKey::Heap(b) => b.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        matches!(self, CompactKey::Inline { .. })
    }

    /// Bytes held on the heap beyond the 24-byte struct itself.
    #[inline(always)]
    pub fn heap_bytes(&self) -> usize {
        match self {
            CompactKey::Inline { .. } => 0,
            CompactKey::Heap(b) => b.len(),
        }
    }

    /// Copy into an owned `Bytes` (one allocation, or none when empty).
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        let s = self.as_slice();
        if s.is_empty() {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(s)
        }
    }
}

impl Default for CompactKey {
    #[inline]
    fn default() -> Self {
        CompactKey::Inline {
            len: 0,
            data: [0; INLINE_CAP],
        }
    }
}

impl Deref for CompactKey {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for CompactKey {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Borrow<[u8]> for CompactKey {
    #[inline(always)]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<&[u8]> for CompactKey {
    #[inline]
    fn from(s: &[u8]) -> Self {
        CompactKey::new(s)
    }
}

impl From<&Bytes> for CompactKey {
    #[inline]
    fn from(b: &Bytes) -> Self {
        CompactKey::new(b)
    }
}

impl From<Bytes> for CompactKey {
    #[inline]
    fn from(b: Bytes) -> Self {
        CompactKey::new(&b)
    }
}

impl From<&CompactKey> for Bytes {
    #[inline]
    fn from(k: &CompactKey) -> Self {
        k.to_bytes()
    }
}

impl From<CompactKey> for Bytes {
    #[inline]
    fn from(k: CompactKey) -> Self {
        match k {
            // Reuse the exact-size allocation; no copy.
            CompactKey::Heap(b) => Bytes::from(b),
            inline => inline.to_bytes(),
        }
    }
}

impl PartialEq for CompactKey {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for CompactKey {}

impl PartialEq<[u8]> for CompactKey {
    #[inline(always)]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<&[u8]> for CompactKey {
    #[inline(always)]
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

impl PartialEq<Bytes> for CompactKey {
    #[inline(always)]
    fn eq(&self, other: &Bytes) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl PartialEq<CompactKey> for Bytes {
    #[inline(always)]
    fn eq(&self, other: &CompactKey) -> bool {
        self.as_ref() == other.as_slice()
    }
}

impl PartialOrd for CompactKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactKey {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl std::hash::Hash for CompactKey {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Same as `[u8]`, so `Borrow<[u8]>` lookups agree.
        self.as_slice().hash(state)
    }
}

impl fmt::Debug for CompactKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&Bytes::copy_from_slice(self.as_slice()), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_key_roundtrip_boundaries() {
        for n in [0usize, 1, 14, 21, 22, 23, 64, 4096] {
            let s: Vec<u8> = (0..n).map(|i| (i * 31 + 7) as u8).collect();
            let k = CompactKey::new(&s);
            assert_eq!(k.as_slice(), &s[..]);
            assert_eq!(k.len(), n);
            assert_eq!(k.is_inline(), n <= INLINE_CAP);
            assert_eq!(k.heap_bytes(), if n <= INLINE_CAP { 0 } else { n });
            assert_eq!(k.to_bytes(), Bytes::from(s.clone()));
            assert_eq!(Bytes::from(k.clone()), Bytes::from(s.clone()));
            assert_eq!(k, CompactKey::from(Bytes::from(s.clone())));
            assert!(k == s[..]);
        }
    }

    #[test]
    fn test_compact_key_eq_ord_hash_match_slices() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let samples: [&[u8]; 6] = [
            b"",
            b"a",
            b"key:0000000001",
            b"key:0000000002",
            b"user:1234567:profile_x",
            b"a-much-longer-key-that-lives-on-the-heap",
        ];
        let h = |x: &dyn Fn(&mut DefaultHasher)| {
            let mut s = DefaultHasher::new();
            x(&mut s);
            s.finish()
        };
        for a in samples {
            let ka = CompactKey::new(a);
            assert_eq!(h(&|s| ka.hash(s)), h(&|s| a.hash(s)));
            for b in samples {
                let kb = CompactKey::new(b);
                assert_eq!(ka == kb, a == b);
                assert_eq!(ka.cmp(&kb), a.cmp(b));
            }
        }
        // Inline padding never leaks into equality.
        assert_eq!(CompactKey::default(), CompactKey::new(b""));
    }
}
