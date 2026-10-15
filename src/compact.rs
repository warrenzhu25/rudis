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
/// 1 B tag + 23 B data (`Inline23`), or 1 B tag + 1 B length + 22 B data (`Inline`).
pub const INLINE_CAP: usize = 23;
const INLINE_SHORT_CAP: usize = 22;

/// Longest key stored inline together with an expiry deadline:
/// 1 B tag + 18 B data + 5 B deadline (`InlineTtl18`), or 1 B tag + 1 B length + 17 B data + 5 B deadline.
pub const INLINE_TTL_CAP: usize = 18;
const INLINE_TTL_SHORT_CAP: usize = 17;

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
        data: [u8; INLINE_SHORT_CAP],
    },
    Inline23([u8; INLINE_CAP]),
    InlineTtl {
        len: u8,
        data: [u8; INLINE_TTL_SHORT_CAP],
        /// Deadline in milliseconds, see `pack_ttl_ms`.
        ttl: [u8; 5],
    },
    InlineTtl18 {
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
        if s.len() <= INLINE_SHORT_CAP {
            let mut data = [0u8; INLINE_SHORT_CAP];
            data[..s.len()].copy_from_slice(s);
            CompactKey::Inline {
                len: s.len() as u8,
                data,
            }
        } else if s.len() == INLINE_CAP {
            let mut data = [0u8; INLINE_CAP];
            data.copy_from_slice(s);
            CompactKey::Inline23(data)
        } else {
            CompactKey::Heap(Box::from(s))
        }
    }

    /// A key carrying the packed deadline `ttl` (0 = none).
    #[inline]
    pub fn with_ttl(s: &[u8], ttl: u64) -> Self {
        if ttl == 0 {
            Self::new(s)
        } else if s.len() <= INLINE_TTL_SHORT_CAP
            && let Some(packed) = pack_ttl_ms(ttl)
        {
            let mut data = [0u8; INLINE_TTL_SHORT_CAP];
            data[..s.len()].copy_from_slice(s);
            CompactKey::InlineTtl {
                len: s.len() as u8,
                data,
                ttl: packed,
            }
        } else if s.len() == INLINE_TTL_CAP
            && let Some(packed) = pack_ttl_ms(ttl)
        {
            let mut data = [0u8; INLINE_TTL_CAP];
            data.copy_from_slice(s);
            CompactKey::InlineTtl18 { data, ttl: packed }
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
            CompactKey::Inline { .. } | CompactKey::Inline23(_) | CompactKey::Heap(_) => 0,
            CompactKey::InlineTtl { ttl, .. } | CompactKey::InlineTtl18 { ttl, .. } => {
                unpack_ttl_ms(*ttl)
            }
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
            CompactKey::InlineTtl { ttl: t, .. } | CompactKey::InlineTtl18 { ttl: t, .. }
                if ttl != 0
                    && let Some(packed) = pack_ttl_ms(ttl) =>
            {
                *t = packed
            }
            CompactKey::HeapTtl(b) if ttl != 0 => b[..8].copy_from_slice(&ttl.to_ne_bytes()),
            CompactKey::Inline { .. } | CompactKey::Inline23(_) | CompactKey::Heap(_)
                if ttl == 0 => {}
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
            CompactKey::Inline23(data) => data,
            CompactKey::Heap(b) => b,
            CompactKey::InlineTtl { len, data, .. } => &data[..*len as usize],
            CompactKey::InlineTtl18 { data, .. } => data,
            CompactKey::HeapTtl(b) => &b[8..],
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            CompactKey::Inline { len, .. } | CompactKey::InlineTtl { len, .. } => *len as usize,
            CompactKey::Inline23(_) => INLINE_CAP,
            CompactKey::InlineTtl18 { .. } => INLINE_TTL_CAP,
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
            CompactKey::Inline { .. }
                | CompactKey::Inline23(_)
                | CompactKey::InlineTtl { .. }
                | CompactKey::InlineTtl18 { .. }
        )
    }

    /// Bytes held on the heap beyond the 24-byte struct itself.
    #[inline(always)]
    pub fn heap_bytes(&self) -> usize {
        match self {
            CompactKey::Inline { .. }
            | CompactKey::Inline23(_)
            | CompactKey::InlineTtl { .. }
            | CompactKey::InlineTtl18 { .. } => 0,
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
            data: [0; INLINE_SHORT_CAP],
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

/// Longest string value stored inline as raw contiguous bytes (`Inline` up to
/// 14 B, `Inline15` at 15 B).
pub const STR_INLINE_CAP: usize = 15;
const STR_INLINE_SHORT_CAP: usize = 14;

/// Longest string value stored inline when every byte is 7-bit ASCII: 16
/// bytes fill a 14-byte buffer (`Packed`), 17 bytes fill a 15-byte buffer (`Packed17`).
pub const STR_PACKED_CAP: usize = 17;
const STR_PACKED_SHORT_CAP: usize = 16;

/// Longest string value stored inline using 6-bit (`18..=20` B) or 5-bit (`21..=24` B)
/// alphabet packing inside the 15-byte payload.
pub const STR_MAX_PACKED_CAP: usize = 24;

const ALPHABET_6A: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz:-";
const ALPHABET_6B: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_.";
const ALPHABET_5A: &[u8; 32] = b"0123456789abcdef:-_.TZ /+ghiklnv";
const ALPHABET_5B: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz:-_./ ";

const fn build_encode_table(alphabet: &[u8]) -> [u8; 256] {
    let mut table = [0xFFu8; 256];
    let mut i = 0;
    while i < alphabet.len() {
        table[alphabet[i] as usize] = i as u8;
        i += 1;
    }
    table
}

const ENCODE_6A: [u8; 256] = build_encode_table(ALPHABET_6A);
const ENCODE_6B: [u8; 256] = build_encode_table(ALPHABET_6B);
const ENCODE_5A: [u8; 256] = build_encode_table(ALPHABET_5A);
const ENCODE_5B: [u8; 256] = build_encode_table(ALPHABET_5B);

#[inline]
fn try_pack_bits<const BITS: usize>(s: &[u8], enc: &[u8; 256]) -> Option<[u8; 15]> {
    let mut acc: u128 = 0;
    for (i, &b) in s.iter().enumerate() {
        let code = enc[b as usize];
        if code == 0xFF {
            return None;
        }
        acc |= (code as u128) << (BITS * i);
    }
    let bytes = acc.to_le_bytes();
    let mut out = [0u8; 15];
    out.copy_from_slice(&bytes[..15]);
    Some(out)
}

#[inline]
fn unpack_bits<const BITS: usize>(
    data: &[u8; 15],
    len: usize,
    dec: &[u8],
) -> [u8; STR_MAX_PACKED_CAP] {
    let mut raw = [0u8; 16];
    raw[..15].copy_from_slice(data);
    let mut acc = u128::from_le_bytes(raw);
    let mask = (1u128 << BITS) - 1;
    let mut out = [0u8; STR_MAX_PACKED_CAP];
    for slot in out.iter_mut().take(len) {
        *slot = dec[(acc & mask) as usize];
        acc >>= BITS;
    }
    out
}

/// String value stored in a table slot. 16 bytes, multiple representations:
/// - `Inline` / `Inline15`: up to [`STR_INLINE_CAP`] (15) raw bytes, no heap allocation.
/// - `Packed` / `Packed17`: `16..=17` bytes of 7-bit ASCII, no heap allocation.
/// - `Packed6A_*` / `Packed6B_*`: `18..=20` bytes of 6-bit ASCII (`0-9A-Za-z:-` or `0-9A-Za-z_.`), no heap allocation.
/// - `Packed5A_*` / `Packed5B_*`: `21..=24` bytes of 5-bit ASCII (hex/timestamps/identifiers or lowercase), no heap allocation.
/// - `Heap`: an exact-size `Box<[u8]>` split into a thin pointer and a
///   `u32` length (a fat `Box<[u8]>` plus the tag would need 24 bytes). How
///   every other non-inline value is written, so SET costs one allocation.
/// - `Shared`: a boxed `Bytes`. A `Heap` value of [`SHARE_MIN`] bytes or more
///   is converted in place (no copy) on its first read via
///   [`CompactStr::make_shared`], so later GETs (often cross-shard) are a
///   refcount clone rather than a malloc + memcpy.
pub enum CompactStr {
    Inline {
        len: u8,
        data: [u8; STR_INLINE_SHORT_CAP],
    },
    Inline15([u8; STR_INLINE_CAP]),
    Packed {
        len: u8,
        data: [u8; STR_INLINE_SHORT_CAP],
    },
    Packed17([u8; STR_INLINE_CAP]),
    Packed6A18([u8; STR_INLINE_CAP]),
    Packed6A19([u8; STR_INLINE_CAP]),
    Packed6A20([u8; STR_INLINE_CAP]),
    Packed6B18([u8; STR_INLINE_CAP]),
    Packed6B19([u8; STR_INLINE_CAP]),
    Packed6B20([u8; STR_INLINE_CAP]),
    Packed5A21([u8; STR_INLINE_CAP]),
    Packed5A22([u8; STR_INLINE_CAP]),
    Packed5A23([u8; STR_INLINE_CAP]),
    Packed5A24([u8; STR_INLINE_CAP]),
    Packed5B21([u8; STR_INLINE_CAP]),
    Packed5B22([u8; STR_INLINE_CAP]),
    Packed5B23([u8; STR_INLINE_CAP]),
    Packed5B24([u8; STR_INLINE_CAP]),
    /// Owns the allocation behind `ptr`: `len` bytes from a `Box<[u8]>`
    /// (see [`CompactStr::from_box`]), freed in `Drop`.
    Heap {
        len: u32,
        ptr: NonNull<u8>,
    },
    /// An ASCII value of `len` bytes ([`HEAP_PACK_MIN`]..=[`HEAP_PACK_MAX`])
    /// stored 7 bits per byte in a leaked `Box<[u8]>` of
    /// `packed_len(len)` bytes; used only when that lands in a smaller
    /// allocator size class than the plain bytes would.
    HeapPacked {
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

/// Shortest value [`CompactStr::HeapPacked`] holds (longer than any inline
/// encoding).
pub const HEAP_PACK_MIN: usize = STR_MAX_PACKED_CAP + 1;
/// Longest value [`CompactStr::HeapPacked`] holds: values from
/// [`SHARE_MIN`] up are shared zero-copy on read, which packing would undo.
pub const HEAP_PACK_MAX: usize = SHARE_MIN - 1;

/// Bytes `n` ASCII bytes take packed 7 bits each.
#[inline(always)]
pub(crate) fn packed_len(n: usize) -> usize {
    (n * 7).div_ceil(8)
}

/// jemalloc's size class for a small allocation of `n` bytes (16-byte steps
/// to 128, 32-byte steps to 256). Packing only pays when it changes this.
#[inline(always)]
fn alloc_class(n: usize) -> usize {
    if n <= 128 {
        n.div_ceil(16) * 16
    } else {
        n.div_ceil(32) * 32
    }
}

/// True when `s` should be stored as [`CompactStr::HeapPacked`].
#[inline]
fn heap_pack_pays(s: &[u8]) -> bool {
    let n = s.len();
    (HEAP_PACK_MIN..=HEAP_PACK_MAX).contains(&n)
        && alloc_class(packed_len(n)) < alloc_class(n)
        && s.is_ascii()
}

/// Low 7 bits of each byte: the lanes [`pack7_heap`] keeps.
const LANES7: u64 = 0x7f7f_7f7f_7f7f_7f7f;

/// Packs ASCII `s`, 8 bytes into 7 at a time, into `packed_len(s.len())` bytes.
fn pack7_heap(s: &[u8]) -> Box<[u8]> {
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("bmi2") {
        // SAFETY: BMI2 is available (checked just above).
        return unsafe { pack7_heap_bmi2(s) };
    }
    pack7_heap_with(s, pack7_u64)
}

/// [`pack7_heap`] with `pext` packing each 8-byte group in one instruction.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "bmi2")]
fn pack7_heap_bmi2(s: &[u8]) -> Box<[u8]> {
    pack7_heap_with(s, |w| std::arch::x86_64::_pext_u64(w, LANES7))
}

#[inline(always)]
fn pack7_heap_with(s: &[u8], pack: impl Fn(u64) -> u64) -> Box<[u8]> {
    let plen = packed_len(s.len());
    let mut out = vec![0u8; plen].into_boxed_slice();
    let mut at = 0;
    let (groups, tail) = s.as_chunks::<8>();
    for g in groups {
        let p = pack(u64::from_le_bytes(*g)).to_le_bytes();
        // Fixed-size stores: all 8 bytes while they fit (the 8th is
        // overwritten by the next group), else the 7 that matter.
        if at + 8 <= plen {
            out[at..at + 8].copy_from_slice(&p);
        } else {
            out[at..at + 7].copy_from_slice(&p[..7]);
        }
        at += 7;
    }
    if !tail.is_empty() {
        let mut w = [0u8; 8];
        w[..tail.len()].copy_from_slice(tail);
        let p = pack(u64::from_le_bytes(w)).to_le_bytes();
        let n = plen - at;
        out[at..].copy_from_slice(&p[..n]);
    }
    out
}

/// Appends the `len` bytes packed by [`pack7_heap`] to `out`.
fn unpack7_heap_into(packed: &[u8], len: usize, out: &mut Vec<u8>) {
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("bmi2") {
        // SAFETY: BMI2 is available (checked just above).
        return unsafe { unpack7_heap_bmi2(packed, len, out) };
    }
    unpack7_heap_with(packed, len, out, unpack7_u64)
}

/// [`unpack7_heap_into`] with `pdep` spreading each group in one instruction.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "bmi2")]
fn unpack7_heap_bmi2(packed: &[u8], len: usize, out: &mut Vec<u8>) {
    unpack7_heap_with(packed, len, out, |w| {
        std::arch::x86_64::_pdep_u64(w, LANES7)
    })
}

#[inline(always)]
fn unpack7_heap_with(packed: &[u8], len: usize, out: &mut Vec<u8>, unpack: impl Fn(u64) -> u64) {
    out.reserve(len);
    let full = len / 8;
    for i in 0..full {
        let at = i * 7;
        let w = if at + 8 <= packed.len() {
            u64::from_le_bytes(packed[at..at + 8].try_into().expect("8 bytes"))
                & 0x00ff_ffff_ffff_ffff
        } else {
            let mut b = [0u8; 8];
            b[..7].copy_from_slice(&packed[at..at + 7]);
            u64::from_le_bytes(b)
        };
        out.extend_from_slice(&unpack(w).to_le_bytes());
    }
    let rest = len - full * 8;
    if rest > 0 {
        let at = full * 7;
        let mut b = [0u8; 8];
        b[..packed.len() - at].copy_from_slice(&packed[at..]);
        out.extend_from_slice(&unpack(u64::from_le_bytes(b)).to_le_bytes()[..rest]);
    }
}

/// Packs 15 or 16 ASCII bytes, 7 bits each, into 14 bytes (a 15-byte value
/// packs a trailing zero, which `len` cuts off again on unpack).
#[inline]
fn pack7(s: &[u8]) -> [u8; STR_INLINE_SHORT_CAP] {
    let mut buf = [0u8; STR_PACKED_SHORT_CAP];
    buf[..s.len()].copy_from_slice(s);
    let (lo, hi) = buf.split_at(8);
    let lo = pack7_u64(u64::from_le_bytes(lo.try_into().unwrap_or_default()));
    let hi = pack7_u64(u64::from_le_bytes(hi.try_into().unwrap_or_default()));
    let mut out = [0u8; STR_INLINE_SHORT_CAP];
    out[..7].copy_from_slice(&lo.to_le_bytes()[..7]);
    out[7..].copy_from_slice(&hi.to_le_bytes()[..7]);
    out
}

#[inline]
fn unpack7(data: &[u8; STR_INLINE_SHORT_CAP]) -> [u8; STR_MAX_PACKED_CAP] {
    let mut lo = [0u8; 8];
    let mut hi = [0u8; 8];
    lo[..7].copy_from_slice(&data[..7]);
    hi[..7].copy_from_slice(&data[7..]);
    let mut out = [0u8; STR_MAX_PACKED_CAP];
    out[..8].copy_from_slice(&unpack7_u64(u64::from_le_bytes(lo)).to_le_bytes());
    out[8..16].copy_from_slice(&unpack7_u64(u64::from_le_bytes(hi)).to_le_bytes());
    out
}

#[inline]
fn pack7_17(s: &[u8]) -> [u8; STR_INLINE_CAP] {
    let mut out = [0u8; STR_INLINE_CAP];
    out[..14].copy_from_slice(&pack7(&s[..16]));
    out[14] = s[16];
    out
}

#[inline]
fn unpack7_17(data: &[u8; STR_INLINE_CAP]) -> [u8; STR_MAX_PACKED_CAP] {
    let head: &[u8; STR_INLINE_SHORT_CAP] = data[..14].try_into().unwrap();
    let mut out = unpack7(head);
    out[16] = data[14];
    out
}

/// Borrowed bytes of a [`CompactStr`]: a slice of its storage, or a packed
/// value unpacked onto the stack. Derefs to `[u8]`.
pub struct StrView<'a>(ViewRepr<'a>);

enum ViewRepr<'a> {
    Slice(&'a [u8]),
    /// A [`CompactStr::HeapPacked`] value, unpacked.
    Owned(Vec<u8>),
    Unpacked {
        len: u8,
        buf: [u8; STR_MAX_PACKED_CAP],
    },
}

impl Deref for StrView<'_> {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &[u8] {
        match &self.0 {
            ViewRepr::Slice(s) => s,
            ViewRepr::Owned(v) => v,
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
        let n = s.len();
        if n <= STR_INLINE_SHORT_CAP {
            let mut data = [0u8; STR_INLINE_SHORT_CAP];
            data[..n].copy_from_slice(s);
            CompactStr::Inline { len: n as u8, data }
        } else if n == STR_INLINE_CAP {
            let mut data = [0u8; STR_INLINE_CAP];
            data.copy_from_slice(s);
            CompactStr::Inline15(data)
        } else if n == STR_PACKED_SHORT_CAP && s.is_ascii() {
            CompactStr::Packed {
                len: n as u8,
                data: pack7(s),
            }
        } else if n == STR_PACKED_CAP && s.is_ascii() {
            CompactStr::Packed17(pack7_17(s))
        } else if (18..=20).contains(&n) {
            if let Some(d) = try_pack_bits::<6>(s, &ENCODE_6A) {
                match n {
                    18 => CompactStr::Packed6A18(d),
                    19 => CompactStr::Packed6A19(d),
                    _ => CompactStr::Packed6A20(d),
                }
            } else if let Some(d) = try_pack_bits::<6>(s, &ENCODE_6B) {
                match n {
                    18 => CompactStr::Packed6B18(d),
                    19 => CompactStr::Packed6B19(d),
                    _ => CompactStr::Packed6B20(d),
                }
            } else {
                CompactStr::from_box(Box::from(s))
            }
        } else if (21..=24).contains(&n) {
            if let Some(d) = try_pack_bits::<5>(s, &ENCODE_5A) {
                match n {
                    21 => CompactStr::Packed5A21(d),
                    22 => CompactStr::Packed5A22(d),
                    23 => CompactStr::Packed5A23(d),
                    _ => CompactStr::Packed5A24(d),
                }
            } else if let Some(d) = try_pack_bits::<5>(s, &ENCODE_5B) {
                match n {
                    21 => CompactStr::Packed5B21(d),
                    22 => CompactStr::Packed5B22(d),
                    23 => CompactStr::Packed5B23(d),
                    _ => CompactStr::Packed5B24(d),
                }
            } else {
                CompactStr::from_box(Box::from(s))
            }
        } else if heap_pack_pays(s) {
            let ptr = NonNull::from(Box::leak(pack7_heap(s))).cast::<u8>();
            CompactStr::HeapPacked { len: n as u32, ptr }
        } else {
            CompactStr::from_box(Box::from(s))
        }
    }

    /// The packed bytes of a `HeapPacked` value (empty otherwise).
    #[inline(always)]
    fn packed_slice(&self) -> &[u8] {
        match self {
            // SAFETY: a `HeapPacked` value owns `packed_len(len)` initialized
            // bytes at `ptr` (leaked from a `Box<[u8]>` in `new`) until it is
            // dropped, which needs `&mut self`; the slice borrows `self`.
            CompactStr::HeapPacked { len, ptr } => unsafe {
                std::slice::from_raw_parts(ptr.as_ptr(), packed_len(*len as usize))
            },
            _ => &[],
        }
    }

    /// Appends the value's bytes to `out` (unpacking packed values directly
    /// there, without an intermediate buffer).
    #[inline]
    pub fn extend_into(&self, out: &mut Vec<u8>) {
        match self {
            CompactStr::HeapPacked { len, .. } => {
                unpack7_heap_into(self.packed_slice(), *len as usize, out)
            }
            _ => out.extend_from_slice(&self.view()),
        }
    }

    /// Appends the value as a RESP bulk string (`$len\r\n...\r\n`),
    /// unpacking a packed heap value straight into `out`.
    #[inline]
    pub fn write_resp_bulk(&self, out: &mut Vec<u8>) {
        match self {
            CompactStr::HeapPacked { len, .. } => {
                let n = *len as usize;
                // 25..=255: two or three digits.
                out.reserve(n + 16);
                out.push(b'$');
                if n >= 100 {
                    out.push(b'0' + (n / 100) as u8);
                }
                out.extend_from_slice(&[b'0' + (n / 10 % 10) as u8, b'0' + (n % 10) as u8]);
                out.extend_from_slice(b"\r\n");
                self.extend_into(out);
                out.extend_from_slice(b"\r\n");
            }
            _ => crate::connection::write_resp_bulk(out, &self.view()),
        }
    }

    /// True for a [`CompactStr::HeapPacked`] value.
    #[inline(always)]
    pub fn is_heap_packed(&self) -> bool {
        matches!(self, CompactStr::HeapPacked { .. })
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
            CompactStr::Inline15(data) => ViewRepr::Slice(data),
            CompactStr::Packed { len, data } => ViewRepr::Unpacked {
                len: *len,
                buf: unpack7(data),
            },
            CompactStr::Packed17(data) => ViewRepr::Unpacked {
                len: 17,
                buf: unpack7_17(data),
            },
            CompactStr::Packed6A18(d) => ViewRepr::Unpacked {
                len: 18,
                buf: unpack_bits::<6>(d, 18, ALPHABET_6A),
            },
            CompactStr::Packed6A19(d) => ViewRepr::Unpacked {
                len: 19,
                buf: unpack_bits::<6>(d, 19, ALPHABET_6A),
            },
            CompactStr::Packed6A20(d) => ViewRepr::Unpacked {
                len: 20,
                buf: unpack_bits::<6>(d, 20, ALPHABET_6A),
            },
            CompactStr::Packed6B18(d) => ViewRepr::Unpacked {
                len: 18,
                buf: unpack_bits::<6>(d, 18, ALPHABET_6B),
            },
            CompactStr::Packed6B19(d) => ViewRepr::Unpacked {
                len: 19,
                buf: unpack_bits::<6>(d, 19, ALPHABET_6B),
            },
            CompactStr::Packed6B20(d) => ViewRepr::Unpacked {
                len: 20,
                buf: unpack_bits::<6>(d, 20, ALPHABET_6B),
            },
            CompactStr::Packed5A21(d) => ViewRepr::Unpacked {
                len: 21,
                buf: unpack_bits::<5>(d, 21, ALPHABET_5A),
            },
            CompactStr::Packed5A22(d) => ViewRepr::Unpacked {
                len: 22,
                buf: unpack_bits::<5>(d, 22, ALPHABET_5A),
            },
            CompactStr::Packed5A23(d) => ViewRepr::Unpacked {
                len: 23,
                buf: unpack_bits::<5>(d, 23, ALPHABET_5A),
            },
            CompactStr::Packed5A24(d) => ViewRepr::Unpacked {
                len: 24,
                buf: unpack_bits::<5>(d, 24, ALPHABET_5A),
            },
            CompactStr::Packed5B21(d) => ViewRepr::Unpacked {
                len: 21,
                buf: unpack_bits::<5>(d, 21, ALPHABET_5B),
            },
            CompactStr::Packed5B22(d) => ViewRepr::Unpacked {
                len: 22,
                buf: unpack_bits::<5>(d, 22, ALPHABET_5B),
            },
            CompactStr::Packed5B23(d) => ViewRepr::Unpacked {
                len: 23,
                buf: unpack_bits::<5>(d, 23, ALPHABET_5B),
            },
            CompactStr::Packed5B24(d) => ViewRepr::Unpacked {
                len: 24,
                buf: unpack_bits::<5>(d, 24, ALPHABET_5B),
            },
            CompactStr::Heap { .. } => ViewRepr::Slice(self.heap_slice()),
            CompactStr::HeapPacked { len, .. } => {
                let mut v = Vec::new();
                unpack7_heap_into(self.packed_slice(), *len as usize, &mut v);
                ViewRepr::Owned(v)
            }
            CompactStr::Shared(b) => ViewRepr::Slice(b),
        })
    }

    /// The value's bytes if they are stored contiguously (`None` for a
    /// packed value).
    #[inline(always)]
    pub fn as_contiguous(&self) -> Option<&[u8]> {
        match self {
            CompactStr::Inline { len, data } => Some(&data[..*len as usize]),
            CompactStr::Inline15(data) => Some(data),
            CompactStr::Heap { .. } => Some(self.heap_slice()),
            CompactStr::Shared(b) => Some(b),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            CompactStr::Inline { len, .. } | CompactStr::Packed { len, .. } => *len as usize,
            CompactStr::Inline15(_) => 15,
            CompactStr::Packed17(_) => 17,
            CompactStr::Packed6A18(_) | CompactStr::Packed6B18(_) => 18,
            CompactStr::Packed6A19(_) | CompactStr::Packed6B19(_) => 19,
            CompactStr::Packed6A20(_) | CompactStr::Packed6B20(_) => 20,
            CompactStr::Packed5A21(_) | CompactStr::Packed5B21(_) => 21,
            CompactStr::Packed5A22(_) | CompactStr::Packed5B22(_) => 22,
            CompactStr::Packed5A23(_) | CompactStr::Packed5B23(_) => 23,
            CompactStr::Packed5A24(_) | CompactStr::Packed5B24(_) => 24,
            CompactStr::Heap { len, .. } | CompactStr::HeapPacked { len, .. } => *len as usize,
            CompactStr::Shared(b) => b.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        !matches!(
            self,
            CompactStr::Heap { .. } | CompactStr::HeapPacked { .. } | CompactStr::Shared(_)
        )
    }

    #[inline(always)]
    pub fn is_heap(&self) -> bool {
        matches!(self, CompactStr::Heap { .. })
    }

    /// Bytes held on the heap beyond the 16-byte value itself.
    #[inline(always)]
    pub fn heap_bytes(&self) -> usize {
        match self {
            CompactStr::Heap { len, .. } => *len as usize,
            CompactStr::HeapPacked { len, .. } => packed_len(*len as usize),
            CompactStr::Shared(b) => b.len() + std::mem::size_of::<Bytes>(),
            _ => 0,
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
            CompactStr::HeapPacked { .. } => {
                let mut v = Vec::new();
                self.extend_into(&mut v);
                Bytes::from(v)
            }
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
        let mut v = Vec::new();
        self.extend_into(&mut v);
        v
    }
}

impl Drop for CompactStr {
    #[inline]
    fn drop(&mut self) {
        if let CompactStr::HeapPacked { len, ptr } = self {
            let raw = std::ptr::slice_from_raw_parts_mut(ptr.as_ptr(), packed_len(*len as usize));
            // SAFETY: `raw` is exactly the pointer and length leaked from the
            // `Box<[u8]>` built by `pack7_heap`; `self` is being dropped, so
            // the box is rebuilt (and freed) exactly once.
            drop(unsafe { Box::from_raw(raw) });
            return;
        }
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
            CompactStr::Inline15(d) => CompactStr::Inline15(*d),
            CompactStr::Packed { len, data } => CompactStr::Packed {
                len: *len,
                data: *data,
            },
            CompactStr::Packed17(d) => CompactStr::Packed17(*d),
            CompactStr::Packed6A18(d) => CompactStr::Packed6A18(*d),
            CompactStr::Packed6A19(d) => CompactStr::Packed6A19(*d),
            CompactStr::Packed6A20(d) => CompactStr::Packed6A20(*d),
            CompactStr::Packed6B18(d) => CompactStr::Packed6B18(*d),
            CompactStr::Packed6B19(d) => CompactStr::Packed6B19(*d),
            CompactStr::Packed6B20(d) => CompactStr::Packed6B20(*d),
            CompactStr::Packed5A21(d) => CompactStr::Packed5A21(*d),
            CompactStr::Packed5A22(d) => CompactStr::Packed5A22(*d),
            CompactStr::Packed5A23(d) => CompactStr::Packed5A23(*d),
            CompactStr::Packed5A24(d) => CompactStr::Packed5A24(*d),
            CompactStr::Packed5B21(d) => CompactStr::Packed5B21(*d),
            CompactStr::Packed5B22(d) => CompactStr::Packed5B22(*d),
            CompactStr::Packed5B23(d) => CompactStr::Packed5B23(*d),
            CompactStr::Packed5B24(d) => CompactStr::Packed5B24(*d),
            CompactStr::Heap { .. } => CompactStr::from_box(Box::from(self.heap_slice())),
            CompactStr::HeapPacked { len, .. } => CompactStr::HeapPacked {
                len: *len,
                ptr: NonNull::from(Box::leak(Box::<[u8]>::from(self.packed_slice()))).cast::<u8>(),
            },
            CompactStr::Shared(b) => CompactStr::Shared(b.clone()),
        }
    }
}

impl Default for CompactStr {
    #[inline]
    fn default() -> Self {
        CompactStr::Inline {
            len: 0,
            data: [0; STR_INLINE_SHORT_CAP],
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

    /// ASCII values of 25..=255 bytes are stored 7 bits per byte exactly when
    /// that moves them to a smaller allocator size class, and every accessor
    /// returns the original bytes.
    #[test]
    fn heap_packed_strings_round_trip_through_every_accessor() {
        let mut packed = 0;
        for n in [
            0usize, 1, 24, 25, 32, 64, 80, 97, 100, 128, 129, 150, 200, 255, 256, 300,
        ] {
            for variant in 0..3 {
                let s: Vec<u8> = match variant {
                    0 => (0..n).map(|i| b'!' + (i * 7 % 94) as u8).collect(), // printable ASCII
                    1 => (0..n).map(|i| (i % 128) as u8).collect(),           // all 7-bit values
                    _ => (0..n)
                        .map(|i| if i == n / 2 { 0xC3 } else { b'a' })
                        .collect(), // one non-ASCII byte
                };
                let c = CompactStr::new(&s);
                let want_packed = variant < 2
                    && (HEAP_PACK_MIN..=HEAP_PACK_MAX).contains(&n)
                    && alloc_class(packed_len(n)) < alloc_class(n);
                assert_eq!(c.is_heap_packed(), want_packed, "n={n} variant={variant}");
                packed += want_packed as usize;
                assert_eq!(c.len(), n);
                assert_eq!(&*c.view(), &s[..]);
                assert_eq!(c.to_vec(), s);
                assert_eq!(c.to_bytes(), Bytes::from(s.clone()));
                let mut out = b"x".to_vec();
                c.extend_into(&mut out);
                assert_eq!(&out[1..], &s[..]);
                let mut resp = Vec::new();
                c.write_resp_bulk(&mut resp);
                let mut want = Vec::new();
                crate::connection::write_resp_bulk(&mut want, &s);
                assert_eq!(resp, want);
                let d = c.clone();
                assert_eq!(d, c);
                assert_eq!(&*d.view(), &s[..]);
                if want_packed {
                    assert_eq!(c.heap_bytes(), packed_len(n));
                    assert!(c.as_contiguous().is_none() && !c.is_inline() && !c.needs_share());
                }
            }
        }
        assert!(packed >= 8, "some lengths must pack: {packed}");
    }

    /// The BMI2 (`pext`/`pdep`) and portable heap packers produce the same
    /// bytes, so either path reads what the other wrote.
    #[test]
    fn heap_pack_bmi2_and_portable_paths_agree() {
        let mut x = 0x2545_f491_4f6c_dd1du64;
        for n in HEAP_PACK_MIN..=HEAP_PACK_MAX {
            let s: Vec<u8> = (0..n)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    (x % 128) as u8
                })
                .collect();
            let portable = pack7_heap_with(&s, pack7_u64);
            assert_eq!(&*pack7_heap(&s), &*portable, "n={n}");
            let mut a = Vec::new();
            unpack7_heap_with(&portable, n, &mut a, unpack7_u64);
            let mut b = Vec::new();
            unpack7_heap_into(&portable, n, &mut b);
            assert_eq!(a, s);
            assert_eq!(b, s);
        }
    }

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
            assert_eq!(&packed[..], &v.to_le_bytes()[..STR_INLINE_SHORT_CAP]);
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
        let lens = [
            0usize, 1, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 255, 256, 257, 4096,
        ];
        // Binary data (bytes >= 0x80), full 7-bit ASCII range, lowercase text, and timestamps/hex.
        let gens: [fn(usize) -> u8; 4] = [
            |i| (i * 13 + 1) as u8,
            |i| if i % 2 == 0 { 0x7f } else { (i % 3) as u8 },
            |i| b'a' + (i % 26) as u8,
            |i| b"0123456789abcdef:-_.TZ"[i % 22],
        ];
        for (g, gen_byte) in gens.iter().enumerate() {
            for n in lens {
                let s: Vec<u8> = (0..n).map(gen_byte).collect();
                let v = CompactStr::new(&s);
                assert_eq!(&*v.view(), &s[..], "gen {g} len {n}");
                assert_eq!(v.len(), n);
                let packable = (n > STR_INLINE_CAP && n <= STR_PACKED_CAP && s.is_ascii())
                    || ((18..=20).contains(&n)
                        && (s.iter().all(|&b| ENCODE_6A[b as usize] != 0xFF)
                            || s.iter().all(|&b| ENCODE_6B[b as usize] != 0xFF)))
                    || ((21..=24).contains(&n)
                        && (s.iter().all(|&b| ENCODE_5A[b as usize] != 0xFF)
                            || s.iter().all(|&b| ENCODE_5B[b as usize] != 0xFF)));
                assert_eq!(
                    v.is_inline(),
                    n <= STR_INLINE_CAP || packable,
                    "gen {g} len {n}"
                );
                let heap_packed = heap_pack_pays(&s);
                assert_eq!(v.is_heap_packed(), heap_packed, "gen {g} len {n}");
                assert_eq!(
                    v.as_contiguous().is_none(),
                    packable || heap_packed,
                    "gen {g} len {n}"
                );
                let want_heap = if v.is_inline() {
                    0
                } else if heap_packed {
                    packed_len(n)
                } else {
                    n
                };
                assert_eq!(v.heap_bytes(), want_heap);
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
        // Medium unpacked heap values also reuse their allocation when owned
        // (non-ASCII, so not 7-bit packed).
        let mid = CompactStr::new(&[0x83u8; 100]);
        assert!(mid.is_heap());
        let ptr = mid.view().as_ptr();
        assert_eq!(Bytes::from(mid).as_ptr(), ptr);
        // A packed value converts by unpacking into a new buffer.
        let packed = CompactStr::new(&[3u8; 100]);
        assert!(packed.is_heap_packed());
        assert_eq!(&Bytes::from(packed)[..], &[3u8; 100][..]);
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
