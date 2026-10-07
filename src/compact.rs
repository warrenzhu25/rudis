//! Compact owned byte strings for table keys and string values.
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

/// Values at least this long are converted to a (boxed) `Bytes` on their
/// first read, so reads hand out a refcount clone instead of copying.
pub const SHARE_MIN: usize = 256;

/// String value stored in a table slot. 24 bytes, three representations:
/// - `Inline`: up to [`INLINE_CAP`] bytes, no heap allocation.
/// - `Heap`: exact-size `Box<[u8]>`; how every non-inline value is written,
///   so SET costs one allocation.
/// - `Shared`: a boxed `Bytes`. A `Heap` value of [`SHARE_MIN`] bytes or more
///   is converted in place (no copy) on its first read via
///   [`CompactStr::make_shared`], so later GETs (often cross-shard) are a
///   refcount clone rather than a malloc + memcpy.
///
/// Measured alternatives at 1 KiB values: `Arc<[u8]>` puts its refcount in
/// the data allocation, pushing values into the next jemalloc size class
/// (SET -10%); copying on every read costs GET -10%; boxing a `Bytes` at
/// write time adds a second allocation to every SET (-10%).
///
/// Nested in `RudisValue::String` it adds no size: the enum stays 24 bytes
/// because its other variants are boxed and fit beside this type's tag byte.
#[derive(Clone)]
pub enum CompactStr {
    Inline { len: u8, data: [u8; INLINE_CAP] },
    Heap(Box<[u8]>),
    Shared(Box<Bytes>),
}

const _: () = assert!(std::mem::size_of::<CompactStr>() == 24);

impl CompactStr {
    #[inline]
    pub fn new(s: &[u8]) -> Self {
        if s.len() <= INLINE_CAP {
            let mut data = [0u8; INLINE_CAP];
            data[..s.len()].copy_from_slice(s);
            CompactStr::Inline {
                len: s.len() as u8,
                data,
            }
        } else {
            CompactStr::Heap(Box::from(s))
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            CompactStr::Inline { len, data } => &data[..*len as usize],
            CompactStr::Heap(b) => b,
            CompactStr::Shared(b) => b,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            CompactStr::Inline { len, .. } => *len as usize,
            CompactStr::Heap(b) => b.len(),
            CompactStr::Shared(b) => b.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        matches!(self, CompactStr::Inline { .. })
    }

    /// Bytes held on the heap beyond the 24-byte value itself.
    #[inline(always)]
    pub fn heap_bytes(&self) -> usize {
        match self {
            CompactStr::Inline { .. } => 0,
            CompactStr::Heap(b) => b.len(),
            CompactStr::Shared(b) => b.len() + std::mem::size_of::<Bytes>(),
        }
    }

    /// Whether [`Self::make_shared`] would convert this value.
    #[inline(always)]
    pub fn needs_share(&self) -> bool {
        matches!(self, CompactStr::Heap(b) if b.len() >= SHARE_MIN)
    }

    /// Convert a large `Heap` value to `Shared` in place, reusing its buffer.
    #[inline]
    pub fn make_shared(&mut self) {
        if self.needs_share()
            && let CompactStr::Heap(b) = std::mem::take(self)
        {
            *self = CompactStr::Shared(Box::new(Bytes::from(b)));
        }
    }

    /// An owned `Bytes`: a refcount clone for shared values, else a copy.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        match self {
            CompactStr::Shared(b) => Bytes::clone(b),
            other => {
                let s = other.as_slice();
                if s.is_empty() {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(s)
                }
            }
        }
    }
}

impl Default for CompactStr {
    #[inline]
    fn default() -> Self {
        CompactStr::Inline {
            len: 0,
            data: [0; INLINE_CAP],
        }
    }
}

impl Deref for CompactStr {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for CompactStr {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Borrow<[u8]> for CompactStr {
    #[inline(always)]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<&[u8]> for CompactStr {
    #[inline]
    fn from(s: &[u8]) -> Self {
        CompactStr::new(s)
    }
}

impl<const N: usize> From<&[u8; N]> for CompactStr {
    #[inline]
    fn from(s: &[u8; N]) -> Self {
        CompactStr::new(s)
    }
}

impl From<&str> for CompactStr {
    #[inline]
    fn from(s: &str) -> Self {
        CompactStr::new(s.as_bytes())
    }
}

impl From<String> for CompactStr {
    #[inline]
    fn from(s: String) -> Self {
        CompactStr::new(s.as_bytes())
    }
}

impl From<Vec<u8>> for CompactStr {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        CompactStr::new(&v)
    }
}

impl From<&Bytes> for CompactStr {
    #[inline]
    fn from(b: &Bytes) -> Self {
        CompactStr::new(b)
    }
}

impl From<Bytes> for CompactStr {
    #[inline]
    fn from(b: Bytes) -> Self {
        CompactStr::new(&b)
    }
}

impl From<&CompactStr> for Bytes {
    #[inline]
    fn from(s: &CompactStr) -> Self {
        s.to_bytes()
    }
}

impl From<CompactStr> for Bytes {
    #[inline]
    fn from(s: CompactStr) -> Self {
        match s {
            // Reuse the exact-size allocation; no copy.
            CompactStr::Heap(b) => Bytes::from(b),
            CompactStr::Shared(b) => *b,
            inline => inline.to_bytes(),
        }
    }
}

impl PartialEq for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for CompactStr {}

impl PartialEq<[u8]> for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<&[u8]> for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_slice() == &other[..]
    }
}

impl PartialEq<Bytes> for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &Bytes) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl PartialEq<CompactStr> for Bytes {
    #[inline(always)]
    fn eq(&self, other: &CompactStr) -> bool {
        self.as_ref() == other.as_slice()
    }
}

impl std::hash::Hash for CompactStr {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl fmt::Debug for CompactStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&Bytes::copy_from_slice(self.as_slice()), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_str_roundtrip() {
        for n in [0usize, 1, 16, 22, 23, 255, 256, 257, 4096] {
            let s: Vec<u8> = (0..n).map(|i| (i * 13 + 1) as u8).collect();
            let v = CompactStr::new(&s);
            assert_eq!(v.as_slice(), &s[..]);
            assert_eq!(v.len(), n);
            assert_eq!(v.is_inline(), n <= INLINE_CAP);
            assert_eq!(v.to_bytes(), Bytes::from(s.clone()));
            assert_eq!(v, CompactStr::from(Bytes::from(s.clone())));
            assert_eq!(v.clone(), v);
        }
        // Large values are shared once promoted, reusing their buffer.
        let mut big = CompactStr::new(&[7u8; 1024]);
        let ptr = big.as_slice().as_ptr();
        assert!(big.needs_share());
        big.make_shared();
        assert!(!big.needs_share());
        assert_eq!(big.as_slice().as_ptr(), ptr);
        assert_eq!(big.to_bytes().as_ptr(), ptr);
        assert_eq!(big.clone().to_bytes().as_ptr(), ptr);
        let b = Bytes::from(big);
        assert_eq!(b.as_ptr(), ptr);
        assert_eq!(&b[..], &[7u8; 1024][..]);
        // Medium heap values also reuse their allocation when owned.
        let mid = CompactStr::new(&[3u8; 100]);
        let ptr = mid.as_slice().as_ptr();
        assert_eq!(Bytes::from(mid).as_ptr(), ptr);
    }

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
