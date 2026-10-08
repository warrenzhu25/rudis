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
use std::ptr::NonNull;

/// Longest string stored inline. Chosen so the enum is exactly 24 bytes:
/// 1 B tag + 1 B length + 22 B data.
pub const INLINE_CAP: usize = 22;

/// Longest key stored inline together with an expiry deadline:
/// 1 B tag + 1 B length + 17 B data + 5 B deadline.
pub const INLINE_TTL_CAP: usize = 17;

const NANOS_PER_MS: u64 = 1_000_000;

/// Packs a deadline (`table::Expiry` raw form: nanoseconds since its base,
/// plus one) into 40 bits of milliseconds, rounded to the nearest one (at
/// most 0.5 ms off, so PTTL never reports more than the TTL that was set).
/// The `Expiry` base is aligned to a wall-clock millisecond, so absolute
/// deadlines (`PXAT`/`PEXPIREAT`) land on the grid and round-trip exactly
/// through `PEXPIRETIME`. `None` past ~34.8 years from the base; those keys
/// use the heap form.
#[inline(always)]
fn pack_ttl_ms(ttl: u64) -> Option<[u8; 5]> {
    let ns = ttl - 1;
    let ms = ns / NANOS_PER_MS + u64::from(ns % NANOS_PER_MS >= NANOS_PER_MS / 2) + 1;
    if ms >> 40 != 0 {
        return None;
    }
    let b = ms.to_le_bytes();
    Some([b[0], b[1], b[2], b[3], b[4]])
}

#[inline(always)]
fn unpack_ttl_ms(b: [u8; 5]) -> u64 {
    let ms = u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], 0, 0, 0]);
    (ms - 1) * NANOS_PER_MS + 1
}

/// A table key, optionally carrying the key's packed expiry deadline (a
/// non-zero `u64`, see `table::Expiry`).
///
/// Keeping the deadline here instead of in every table entry saves 8 B per
/// key without a TTL. Keys with a TTL pay nothing extra when they fit in
/// [`INLINE_TTL_CAP`] (the deadline is then kept at millisecond precision,
/// Redis's own expiry precision); longer ones store the exact deadline as an 8-byte prefix of
/// their heap allocation, which a lookup reads anyway to compare the key.
#[derive(Clone)]
pub enum CompactKey {
    Inline {
        len: u8,
        data: [u8; INLINE_CAP],
    },
    InlineTtl {
        len: u8,
        data: [u8; INLINE_TTL_CAP],
        /// Deadline in milliseconds, see `pack_ttl_ms`.
        ttl: [u8; 5],
    },
    Heap(Box<[u8]>),
    /// Heap key with a deadline: bytes `[..8]` are the deadline, the key
    /// follows.
    HeapTtl(Box<[u8]>),
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

    /// A key carrying the packed deadline `ttl` (0 = none).
    #[inline]
    pub fn with_ttl(s: &[u8], ttl: u64) -> Self {
        if ttl == 0 {
            Self::new(s)
        } else if s.len() <= INLINE_TTL_CAP
            && let Some(packed) = pack_ttl_ms(ttl)
        {
            let mut data = [0u8; INLINE_TTL_CAP];
            data[..s.len()].copy_from_slice(s);
            CompactKey::InlineTtl {
                len: s.len() as u8,
                data,
                ttl: packed,
            }
        } else {
            let mut v = Vec::with_capacity(8 + s.len());
            v.extend_from_slice(&ttl.to_ne_bytes());
            v.extend_from_slice(s);
            CompactKey::HeapTtl(v.into_boxed_slice())
        }
    }

    /// The packed deadline, or 0 if the key has none. Inline keys return it
    /// rounded to the nearest millisecond.
    #[inline(always)]
    pub fn ttl(&self) -> u64 {
        match self {
            CompactKey::Inline { .. } | CompactKey::Heap(_) => 0,
            CompactKey::InlineTtl { ttl, .. } => unpack_ttl_ms(*ttl),
            CompactKey::HeapTtl(b) => {
                u64::from_ne_bytes(b[..8].try_into().expect("8-byte ttl prefix"))
            }
        }
    }

    /// Sets the packed deadline (0 clears it). Updates in place when the
    /// representation doesn't change, otherwise re-encodes the key.
    #[inline]
    pub fn set_ttl(&mut self, ttl: u64) {
        match self {
            CompactKey::InlineTtl { ttl: t, .. }
                if ttl != 0
                    && let Some(packed) = pack_ttl_ms(ttl) =>
            {
                *t = packed
            }
            CompactKey::HeapTtl(b) if ttl != 0 => b[..8].copy_from_slice(&ttl.to_ne_bytes()),
            CompactKey::Inline { .. } | CompactKey::Heap(_) if ttl == 0 => {}
            _ => {
                let new = Self::with_ttl(self.as_slice(), ttl);
                *self = new;
            }
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            CompactKey::Inline { len, data } => &data[..*len as usize],
            CompactKey::Heap(b) => b,
            CompactKey::InlineTtl { len, data, .. } => &data[..*len as usize],
            CompactKey::HeapTtl(b) => &b[8..],
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            CompactKey::Inline { len, .. } | CompactKey::InlineTtl { len, .. } => *len as usize,
            CompactKey::Heap(b) => b.len(),
            CompactKey::HeapTtl(b) => b.len() - 8,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        matches!(
            self,
            CompactKey::Inline { .. } | CompactKey::InlineTtl { .. }
        )
    }

    /// Bytes held on the heap beyond the 24-byte struct itself.
    #[inline(always)]
    pub fn heap_bytes(&self) -> usize {
        match self {
            CompactKey::Inline { .. } | CompactKey::InlineTtl { .. } => 0,
            CompactKey::Heap(b) | CompactKey::HeapTtl(b) => b.len(),
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

/// Longest string value stored inline as raw bytes.
pub const STR_INLINE_CAP: usize = 14;

/// Longest string value stored inline when every byte is 7-bit ASCII: 16
/// bytes of 7 bits fill the same 14-byte buffer.
pub const STR_PACKED_CAP: usize = 16;

/// String value stored in a table slot. 16 bytes, four representations:
/// - `Inline`: up to [`STR_INLINE_CAP`] bytes, no heap allocation.
/// - `Packed`: [`STR_INLINE_CAP`]` + 1 ..= `[`STR_PACKED_CAP`] bytes of 7-bit
///   ASCII, packed 7 bits per byte, no heap allocation.
/// - `Heap`: an exact-size `Box<[u8]>` split into a thin pointer and a
///   `u32` length (a fat `Box<[u8]>` plus the tag would need 24 bytes). How
///   every other non-inline value is written, so SET costs one allocation.
/// - `Shared`: a boxed `Bytes`. A `Heap` value of [`SHARE_MIN`] bytes or more
///   is converted in place (no copy) on its first read via
///   [`CompactStr::make_shared`], so later GETs (often cross-shard) are a
///   refcount clone rather than a malloc + memcpy.
///
/// Measured alternatives at 1 KiB values: `Arc<[u8]>` puts its refcount in
/// the data allocation, pushing values into the next jemalloc size class
/// (SET -10%); copying on every read costs GET -10%; boxing a `Bytes` at
/// write time adds a second allocation to every SET (-10%). A safe
/// `Box<Box<[u8]>>` for `Heap` would add a 16-byte allocation per value.
///
/// A packed value has no contiguous bytes to borrow, so reads go through
/// [`CompactStr::view`], which unpacks it on the stack.
///
/// Nested in `RudisValue::String` it adds no size: the enum stays 16 bytes
/// because its other variants are boxed and fit beside this type's tag byte.
pub enum CompactStr {
    Inline {
        len: u8,
        data: [u8; STR_INLINE_CAP],
    },
    Packed {
        len: u8,
        data: [u8; STR_INLINE_CAP],
    },
    /// Owns the allocation behind `ptr`: `len` bytes from a `Box<[u8]>`
    /// (see [`CompactStr::from_box`]), freed in `Drop`.
    Heap {
        len: u32,
        ptr: NonNull<u8>,
    },
    Shared(Box<Bytes>),
}

const _: () = assert!(std::mem::size_of::<CompactStr>() == 16);

// SAFETY: `CompactStr` owns its data like a `Box<[u8]>` / `Box<Bytes>`
// would: the `Heap` pointer is unique (never aliased by another value), so
// moving the value to another thread moves sole ownership with it.
unsafe impl Send for CompactStr {}
// SAFETY: shared access only ever reads through the `Heap` pointer (via
// `heap_slice`), exactly like `&Box<[u8]>`, which is `Sync`.
unsafe impl Sync for CompactStr {}

/// Squeezes the low 7 bits of 8 bytes into 56 bits (SWAR: merge pairs,
/// then quads, then halves).
#[inline(always)]
fn pack7_u64(x: u64) -> u64 {
    let x = (x & 0x007f_007f_007f_007f) | ((x & 0x7f00_7f00_7f00_7f00) >> 1);
    let x = (x & 0x0000_3fff_0000_3fff) | ((x & 0x3fff_0000_3fff_0000) >> 2);
    (x & 0x0000_0000_0fff_ffff) | ((x & 0x0fff_ffff_0000_0000) >> 4)
}

/// Inverse of [`pack7_u64`].
#[inline(always)]
fn unpack7_u64(x: u64) -> u64 {
    let x = (x & 0x0000_0000_0fff_ffff) | ((x << 4) & 0x0fff_ffff_0000_0000);
    let x = (x & 0x0000_3fff_0000_3fff) | ((x << 2) & 0x3fff_0000_3fff_0000);
    (x & 0x007f_007f_007f_007f) | ((x << 1) & 0x7f00_7f00_7f00_7f00)
}

/// Packs 15 or 16 ASCII bytes, 7 bits each, into 14 bytes (a 15-byte value
/// packs a trailing zero, which `len` cuts off again on unpack).
#[inline]
fn pack7(s: &[u8]) -> [u8; STR_INLINE_CAP] {
    let mut buf = [0u8; STR_PACKED_CAP];
    buf[..s.len()].copy_from_slice(s);
    let (lo, hi) = buf.split_at(8);
    let lo = pack7_u64(u64::from_le_bytes(lo.try_into().unwrap_or_default()));
    let hi = pack7_u64(u64::from_le_bytes(hi.try_into().unwrap_or_default()));
    let mut out = [0u8; STR_INLINE_CAP];
    out[..7].copy_from_slice(&lo.to_le_bytes()[..7]);
    out[7..].copy_from_slice(&hi.to_le_bytes()[..7]);
    out
}

#[inline]
fn unpack7(data: &[u8; STR_INLINE_CAP]) -> [u8; STR_PACKED_CAP] {
    let mut lo = [0u8; 8];
    let mut hi = [0u8; 8];
    lo[..7].copy_from_slice(&data[..7]);
    hi[..7].copy_from_slice(&data[7..]);
    let mut out = [0u8; STR_PACKED_CAP];
    out[..8].copy_from_slice(&unpack7_u64(u64::from_le_bytes(lo)).to_le_bytes());
    out[8..].copy_from_slice(&unpack7_u64(u64::from_le_bytes(hi)).to_le_bytes());
    out
}

/// Borrowed bytes of a [`CompactStr`]: a slice of its storage, or a packed
/// value unpacked onto the stack. Derefs to `[u8]`.
pub struct StrView<'a>(ViewRepr<'a>);

enum ViewRepr<'a> {
    Slice(&'a [u8]),
    Unpacked { len: u8, buf: [u8; STR_PACKED_CAP] },
}

impl Deref for StrView<'_> {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &[u8] {
        match &self.0 {
            ViewRepr::Slice(s) => s,
            ViewRepr::Unpacked { len, buf } => &buf[..*len as usize],
        }
    }
}

impl AsRef<[u8]> for StrView<'_> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl CompactStr {
    #[inline]
    pub fn new(s: &[u8]) -> Self {
        if s.len() <= STR_INLINE_CAP {
            let mut data = [0u8; STR_INLINE_CAP];
            data[..s.len()].copy_from_slice(s);
            CompactStr::Inline {
                len: s.len() as u8,
                data,
            }
        } else if s.len() <= STR_PACKED_CAP && s.is_ascii() {
            CompactStr::Packed {
                len: s.len() as u8,
                data: pack7(s),
            }
        } else {
            CompactStr::from_box(Box::from(s))
        }
    }

    /// Takes ownership of `b` without copying (as `Heap`; `Shared` past
    /// `u32::MAX` bytes, which the protocol's bulk limit never reaches).
    #[inline]
    fn from_box(b: Box<[u8]>) -> Self {
        let Ok(len) = u32::try_from(b.len()) else {
            return CompactStr::Shared(Box::new(Bytes::from(b)));
        };
        let ptr = NonNull::from(Box::leak(b)).cast::<u8>();
        CompactStr::Heap { len, ptr }
    }

    /// The bytes of a `Heap` value (empty for other representations).
    #[inline(always)]
    fn heap_slice(&self) -> &[u8] {
        match self {
            // SAFETY: a `Heap` value owns `len` initialized bytes at `ptr`
            // (leaked from a `Box<[u8]>` in `from_box`) until it is dropped
            // or `take_box` reclaims them, and both need `&mut self`. The
            // slice borrows `self`, so it cannot outlive the allocation, and
            // nothing writes through `ptr` while it exists.
            CompactStr::Heap { len, ptr } => unsafe {
                std::slice::from_raw_parts(ptr.as_ptr(), *len as usize)
            },
            _ => &[],
        }
    }

    /// Reclaims the `Box<[u8]>` of a `Heap` value, leaving `self` empty.
    /// `None` (and `self` untouched) for other representations.
    #[inline]
    fn take_box(&mut self) -> Option<Box<[u8]>> {
        if !matches!(self, CompactStr::Heap { .. }) {
            return None;
        }
        let taken = std::mem::ManuallyDrop::new(std::mem::take(self));
        match &*taken {
            CompactStr::Heap { len, ptr } => {
                let raw = std::ptr::slice_from_raw_parts_mut(ptr.as_ptr(), *len as usize);
                // SAFETY: `raw` is exactly the pointer and length leaked
                // from a `Box<[u8]>` in `from_box`. `taken` is never dropped
                // (`ManuallyDrop`) and `self` no longer holds the pointer,
                // so the box is rebuilt exactly once.
                Some(unsafe { Box::from_raw(raw) })
            }
            _ => None,
        }
    }

    /// The value's bytes, unpacking a packed value onto the stack.
    #[inline(always)]
    pub fn view(&self) -> StrView<'_> {
        StrView(match self {
            CompactStr::Inline { len, data } => ViewRepr::Slice(&data[..*len as usize]),
            CompactStr::Packed { len, data } => ViewRepr::Unpacked {
                len: *len,
                buf: unpack7(data),
            },
            CompactStr::Heap { .. } => ViewRepr::Slice(self.heap_slice()),
            CompactStr::Shared(b) => ViewRepr::Slice(b),
        })
    }

    /// The value's bytes if they are stored contiguously (`None` for a
    /// packed value).
    #[inline(always)]
    pub fn as_contiguous(&self) -> Option<&[u8]> {
        match self {
            CompactStr::Inline { len, data } => Some(&data[..*len as usize]),
            CompactStr::Packed { .. } => None,
            CompactStr::Heap { .. } => Some(self.heap_slice()),
            CompactStr::Shared(b) => Some(b),
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            CompactStr::Inline { len, .. } | CompactStr::Packed { len, .. } => *len as usize,
            CompactStr::Heap { len, .. } => *len as usize,
            CompactStr::Shared(b) => b.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        matches!(self, CompactStr::Inline { .. } | CompactStr::Packed { .. })
    }

    #[inline(always)]
    pub fn is_heap(&self) -> bool {
        matches!(self, CompactStr::Heap { .. })
    }

    /// Bytes held on the heap beyond the 16-byte value itself.
    #[inline(always)]
    pub fn heap_bytes(&self) -> usize {
        match self {
            CompactStr::Inline { .. } | CompactStr::Packed { .. } => 0,
            CompactStr::Heap { len, .. } => *len as usize,
            CompactStr::Shared(b) => b.len() + std::mem::size_of::<Bytes>(),
        }
    }

    /// Whether [`Self::make_shared`] would convert this value.
    #[inline(always)]
    pub fn needs_share(&self) -> bool {
        matches!(self, CompactStr::Heap { len, .. } if *len as usize >= SHARE_MIN)
    }

    /// Convert a large `Heap` value to `Shared` in place, reusing its buffer.
    #[inline]
    pub fn make_shared(&mut self) {
        if self.needs_share()
            && let Some(b) = self.take_box()
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
                let s = other.view();
                if s.is_empty() {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(&s)
                }
            }
        }
    }

    /// Copies the bytes into a new `Vec`.
    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        self.view().to_vec()
    }
}

impl Drop for CompactStr {
    #[inline]
    fn drop(&mut self) {
        drop(self.take_box());
    }
}

impl Clone for CompactStr {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            CompactStr::Inline { len, data } => CompactStr::Inline {
                len: *len,
                data: *data,
            },
            CompactStr::Packed { len, data } => CompactStr::Packed {
                len: *len,
                data: *data,
            },
            CompactStr::Heap { .. } => CompactStr::from_box(Box::from(self.heap_slice())),
            CompactStr::Shared(b) => CompactStr::Shared(b.clone()),
        }
    }
}

impl Default for CompactStr {
    #[inline]
    fn default() -> Self {
        CompactStr::Inline {
            len: 0,
            data: [0; STR_INLINE_CAP],
        }
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
    fn from(mut s: CompactStr) -> Self {
        // Reuse the exact-size allocation; no copy.
        if let Some(b) = s.take_box() {
            return Bytes::from(b);
        }
        match &mut s {
            CompactStr::Shared(b) => std::mem::take(&mut **b),
            other => other.to_bytes(),
        }
    }
}

impl PartialEq for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        *self.view() == *other.view()
    }
}

impl Eq for CompactStr {}

impl PartialEq<[u8]> for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &[u8]) -> bool {
        *self.view() == *other
    }
}

impl PartialEq<&[u8]> for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &&[u8]) -> bool {
        *self.view() == **other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &&[u8; N]) -> bool {
        *self.view() == other[..]
    }
}

impl PartialEq<Bytes> for CompactStr {
    #[inline(always)]
    fn eq(&self, other: &Bytes) -> bool {
        *self.view() == *other.as_ref()
    }
}

impl PartialEq<CompactStr> for Bytes {
    #[inline(always)]
    fn eq(&self, other: &CompactStr) -> bool {
        *self.as_ref() == *other.view()
    }
}

impl std::hash::Hash for CompactStr {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.view().hash(state)
    }
}

impl fmt::Debug for CompactStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&Bytes::copy_from_slice(&self.view()), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack7_matches_bitwise_reference() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        for round in 0..20_000 {
            let len = 15 + round % 2;
            let s: Vec<u8> = (0..len)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    (seed & 0x7f) as u8
                })
                .collect();
            let mut v: u128 = 0;
            for (i, &b) in s.iter().enumerate() {
                v |= (b as u128) << (7 * i);
            }
            let packed = pack7(&s);
            assert_eq!(&packed[..], &v.to_le_bytes()[..STR_INLINE_CAP]);
            assert_eq!(&unpack7(&packed)[..len], &s[..]);
        }
    }

    #[test]
    fn test_compact_str_roundtrip() {
        use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
        let hash = |x: &dyn Fn(&mut DefaultHasher)| {
            let mut h = BuildHasherDefault::<DefaultHasher>::default().build_hasher();
            x(&mut h);
            std::hash::Hasher::finish(&h)
        };
        let lens = [0usize, 1, 13, 14, 15, 16, 17, 22, 23, 255, 256, 257, 4096];
        // Binary data (bytes >= 0x80), full 7-bit ASCII range, and text.
        let gens: [fn(usize) -> u8; 3] = [
            |i| (i * 13 + 1) as u8,
            |i| if i % 2 == 0 { 0x7f } else { (i % 3) as u8 },
            |i| b'a' + (i % 26) as u8,
        ];
        for (g, gen_byte) in gens.iter().enumerate() {
            for n in lens {
                let s: Vec<u8> = (0..n).map(gen_byte).collect();
                let v = CompactStr::new(&s);
                assert_eq!(&*v.view(), &s[..], "gen {g} len {n}");
                assert_eq!(v.len(), n);
                let packable = n > STR_INLINE_CAP && n <= STR_PACKED_CAP && s.is_ascii();
                assert_eq!(v.is_inline(), n <= STR_INLINE_CAP || packable);
                assert_eq!(v.as_contiguous().is_none(), packable);
                assert_eq!(v.heap_bytes(), if v.is_inline() { 0 } else { n });
                assert_eq!(v.to_bytes(), Bytes::from(s.clone()));
                assert_eq!(v.to_vec(), s);
                assert_eq!(v, CompactStr::from(Bytes::from(s.clone())));
                assert_eq!(v, &s[..]);
                assert_eq!(v.clone(), v);
                assert_eq!(&*v.clone().view(), &s[..]);
                assert_eq!(Bytes::from(v.clone()), Bytes::from(s.clone()));
                assert_eq!(
                    hash(&|h| std::hash::Hash::hash(&v, h)),
                    hash(&|h| std::hash::Hash::hash(&s[..], h))
                );
            }
        }
        // Large values are shared once promoted, reusing their buffer.
        let mut big = CompactStr::new(&[7u8; 1024]);
        let ptr = big.view().as_ptr();
        assert!(big.needs_share());
        big.make_shared();
        assert!(!big.needs_share());
        assert_eq!(big.view().as_ptr(), ptr);
        assert_eq!(big.to_bytes().as_ptr(), ptr);
        assert_eq!(big.clone().to_bytes().as_ptr(), ptr);
        let b = Bytes::from(big);
        assert_eq!(b.as_ptr(), ptr);
        assert_eq!(&b[..], &[7u8; 1024][..]);
        // Medium heap values also reuse their allocation when owned.
        let mid = CompactStr::new(&[3u8; 100]);
        let ptr = mid.view().as_ptr();
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
    fn compact_key_ttl_round_trips_across_representations() {
        assert_eq!(std::mem::size_of::<CompactKey>(), 24);
        // Raw deadlines on a millisecond boundary round-trip exactly inline.
        let ms_exact = 3_600_000 * NANOS_PER_MS + 1;
        for n in [0usize, 1, 13, 14, 17, 18, 22, 23, 100] {
            let s: Vec<u8> = (0..n).map(|i| b'a' + (i % 26) as u8).collect();
            let mut k = CompactKey::new(&s);
            assert_eq!(k.ttl(), 0);
            k.set_ttl(ms_exact);
            assert_eq!(k.ttl(), ms_exact);
            assert_eq!(k.as_slice(), &s[..]);
            assert_eq!(k.len(), n);
            assert_eq!(k.is_inline(), n <= INLINE_TTL_CAP);
            assert_eq!(k.heap_bytes(), if n <= INLINE_TTL_CAP { 0 } else { n + 8 });
            // Off-boundary deadlines round to the nearest millisecond inline,
            // and stay exact on the heap.
            k.set_ttl(ms_exact + 5);
            let want = if n <= INLINE_TTL_CAP {
                ms_exact
            } else {
                ms_exact + 5
            };
            assert_eq!(k.ttl(), want);
            // Too far out for 40 bits of ms: falls back to the exact heap form.
            k.set_ttl(u64::MAX);
            assert_eq!(k.ttl(), u64::MAX);
            assert!(!k.is_inline());
            assert_eq!(k.as_slice(), &s[..]);
            let c = k.clone();
            assert_eq!(c, k);
            assert_eq!(c.ttl(), u64::MAX);
            k.set_ttl(0);
            assert_eq!(k.ttl(), 0);
            assert_eq!(k.as_slice(), &s[..]);
            assert_eq!(k.is_inline(), n <= INLINE_CAP);
            assert_eq!(
                Bytes::from(CompactKey::with_ttl(&s, 7)),
                Bytes::from(s.clone())
            );
        }
        // Smallest deadline (raw 1) survives packing.
        assert_eq!(CompactKey::with_ttl(b"k", 1).ttl(), 1);
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
