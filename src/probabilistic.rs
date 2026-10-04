use bytes::Bytes;
use hashbrown::HashMap;

/// 64-bit FNV-1a hash with custom seed.
#[inline]
fn fnv1a_hash(data: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325u64 ^ seed;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Computes two independent 64-bit hashes for Kirsch-Mitzenmacher optimization.
#[inline]
fn double_hash(data: &[u8]) -> (u64, u64) {
    let h1 = fnv1a_hash(data, 0x123456789abcdef0);
    let h2 = fnv1a_hash(data, 0x0fedcba987654321);
    (h1, if h2 == 0 { 1 } else { h2 })
}

/// Largest structure `BF.RESERVE`, `CF.RESERVE` or `CMS.INITBYDIM` may allocate:
/// 512 MiB, the same bound Redis puts on a single string value.
pub const MAX_SKETCH_BYTES: usize = 512 << 20;

// ---------------------------------------------------------------------------
// 1. Bloom Filter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    pub capacity: usize,
    pub error_rate: f64,
    pub num_bits: usize,
    pub num_hashes: usize,
    pub count: usize,
    pub bits: Vec<u64>,
}

impl BloomFilter {
    pub fn new(capacity: usize, error_rate: f64) -> Self {
        let cap = capacity.max(1);
        let err = error_rate.clamp(0.00001, 0.5);
        let num_bits = (Self::bits_for(cap, err) as usize).max(64);

        // k = (m / n) * ln(2)
        let k = ((num_bits as f64 / cap as f64) * std::f64::consts::LN_2).round() as usize;
        let num_hashes = k.clamp(1, 30);

        let u64_len = num_bits.div_ceil(64);
        Self {
            capacity: cap,
            error_rate: err,
            num_bits,
            num_hashes,
            count: 0,
            bits: vec![0u64; u64_len],
        }
    }

    /// `new` for client-supplied sizes: refuses filters over [`MAX_SKETCH_BYTES`]
    /// instead of attempting the allocation.
    pub fn try_new(capacity: usize, error_rate: f64) -> Result<Self, String> {
        let bits = Self::bits_for(capacity.max(1), error_rate.clamp(0.00001, 0.5));
        if bits / 8.0 > MAX_SKETCH_BYTES as f64 {
            return Err(format!(
                "Bloom filter too large: at most {MAX_SKETCH_BYTES} bytes"
            ));
        }
        Ok(Self::new(capacity, error_rate))
    }

    /// m = -n * ln(p) / (ln(2)^2), in f64 so huge capacities cannot overflow.
    fn bits_for(cap: usize, err: f64) -> f64 {
        let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        (-(cap as f64) * err.ln() / ln2_sq).ceil()
    }

    #[inline]
    fn get_bit(&self, bit_idx: usize) -> bool {
        let word = bit_idx / 64;
        let offset = bit_idx % 64;
        (self.bits[word] & (1u64 << offset)) != 0
    }

    #[inline]
    fn set_bit(&mut self, bit_idx: usize) {
        let word = bit_idx / 64;
        let offset = bit_idx % 64;
        self.bits[word] |= 1u64 << offset;
    }

    pub fn add(&mut self, item: &[u8]) -> bool {
        let (h1, h2) = double_hash(item);
        let mut was_present = true;

        for i in 0..self.num_hashes {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) % self.num_bits;
            if !self.get_bit(bit) {
                was_present = false;
                self.set_bit(bit);
            }
        }

        if !was_present {
            self.count += 1;
            true
        } else {
            false
        }
    }

    pub fn contains(&self, item: &[u8]) -> bool {
        let (h1, h2) = double_hash(item);
        for i in 0..self.num_hashes {
            let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) % self.num_bits;
            if !self.get_bit(bit) {
                return false;
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// 2. Cuckoo Filter (Insert, Query, and Delete)
// ---------------------------------------------------------------------------

const BUCKET_SIZE: usize = 4;
const MAX_KICKS: usize = 500;

#[derive(Debug, Clone, PartialEq)]
pub struct CuckooFilter {
    pub capacity: usize,
    pub num_buckets: usize,
    pub count: usize,
    pub buckets: Vec<[u16; BUCKET_SIZE]>,
}

impl CuckooFilter {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        let num_buckets = (cap / BUCKET_SIZE).next_power_of_two().max(4);
        Self {
            capacity: cap,
            num_buckets,
            count: 0,
            buckets: vec![[0u16; BUCKET_SIZE]; num_buckets],
        }
    }

    /// `new` for client-supplied sizes: refuses filters over [`MAX_SKETCH_BYTES`].
    pub fn try_new(capacity: usize) -> Result<Self, String> {
        let bucket_bytes = std::mem::size_of::<[u16; BUCKET_SIZE]>();
        match (capacity.max(1) / BUCKET_SIZE).checked_next_power_of_two() {
            Some(n) if n.saturating_mul(bucket_bytes) <= MAX_SKETCH_BYTES => {
                Ok(Self::new(capacity))
            }
            _ => Err(format!(
                "Cuckoo filter too large: at most {MAX_SKETCH_BYTES} bytes"
            )),
        }
    }

    fn fingerprint(item: &[u8]) -> u16 {
        let mut fp = (fnv1a_hash(item, 0x5a5a5a5a5a5a5a5a) & 0xFFFF) as u16;
        if fp == 0 {
            fp = 1;
        }
        fp
    }

    fn indices(&self, item: &[u8], fp: u16) -> (usize, usize) {
        let h = fnv1a_hash(item, 0x1122334455667788) as usize;
        let i1 = h % self.num_buckets;
        let fp_hash = fnv1a_hash(&fp.to_le_bytes(), 0x99aabbccddeeff00) as usize;
        let i2 = (i1 ^ fp_hash) % self.num_buckets;
        (i1, i2)
    }

    fn alt_index(&self, i: usize, fp: u16) -> usize {
        let fp_hash = fnv1a_hash(&fp.to_le_bytes(), 0x99aabbccddeeff00) as usize;
        (i ^ fp_hash) % self.num_buckets
    }

    pub fn add(&mut self, item: &[u8]) -> Result<bool, &'static str> {
        let fp = Self::fingerprint(item);
        let (i1, i2) = self.indices(item, fp);

        // Try insert into i1 or i2
        for slot in self.buckets[i1].iter_mut() {
            if *slot == 0 {
                *slot = fp;
                self.count += 1;
                return Ok(true);
            }
        }
        for slot in self.buckets[i2].iter_mut() {
            if *slot == 0 {
                *slot = fp;
                self.count += 1;
                return Ok(true);
            }
        }

        // Random cuckoo kick
        let mut cur_i = if (fp as usize).is_multiple_of(2) {
            i1
        } else {
            i2
        };
        let mut cur_fp = fp;

        for _ in 0..MAX_KICKS {
            let slot_idx = (cur_fp as usize) % BUCKET_SIZE;
            std::mem::swap(&mut self.buckets[cur_i][slot_idx], &mut cur_fp);
            cur_i = self.alt_index(cur_i, cur_fp);

            for slot in self.buckets[cur_i].iter_mut() {
                if *slot == 0 {
                    *slot = cur_fp;
                    self.count += 1;
                    return Ok(true);
                }
            }
        }

        Err("ERR Cuckoo filter is full")
    }

    pub fn contains(&self, item: &[u8]) -> bool {
        let fp = Self::fingerprint(item);
        let (i1, i2) = self.indices(item, fp);

        self.buckets[i1].contains(&fp) || self.buckets[i2].contains(&fp)
    }

    pub fn delete(&mut self, item: &[u8]) -> bool {
        let fp = Self::fingerprint(item);
        let (i1, i2) = self.indices(item, fp);

        for slot in self.buckets[i1].iter_mut() {
            if *slot == fp {
                *slot = 0;
                self.count = self.count.saturating_sub(1);
                return true;
            }
        }
        for slot in self.buckets[i2].iter_mut() {
            if *slot == fp {
                *slot = 0;
                self.count = self.count.saturating_sub(1);
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// 3. Count-Min Sketch
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct CountMinSketch {
    pub width: usize,
    pub depth: usize,
    pub total_count: u64,
    pub table: Vec<Vec<u64>>,
}

impl CountMinSketch {
    pub fn new(width: usize, depth: usize) -> Self {
        let w = width.max(4);
        let d = depth.clamp(1, 16);
        Self {
            width: w,
            depth: d,
            total_count: 0,
            table: vec![vec![0u64; w]; d],
        }
    }

    /// `new` for client-supplied dimensions: refuses sketches over [`MAX_SKETCH_BYTES`].
    pub fn try_new(width: usize, depth: usize) -> Result<Self, String> {
        let cells = width.max(4).saturating_mul(depth.clamp(1, 16));
        if cells.saturating_mul(std::mem::size_of::<u64>()) > MAX_SKETCH_BYTES {
            return Err(format!(
                "CMS: width x depth too large: at most {MAX_SKETCH_BYTES} bytes"
            ));
        }
        Ok(Self::new(width, depth))
    }

    pub fn from_prob(err: f64, conf: f64) -> Self {
        // width = e / err, depth = ln(1 / (1 - conf))
        let w = (std::f64::consts::E / err.max(0.0001)).ceil() as usize;
        let d = (1.0 / (1.0 - conf.clamp(0.01, 0.9999))).ln().ceil() as usize;
        Self::new(w, d)
    }

    pub fn incr_by(&mut self, item: &[u8], delta: u64) -> u64 {
        let (h1, h2) = double_hash(item);
        let mut min_val = u64::MAX;

        for r in 0..self.depth {
            let col = (h1.wrapping_add((r as u64).wrapping_mul(h2)) as usize) % self.width;
            self.table[r][col] = self.table[r][col].saturating_add(delta);
            min_val = min_val.min(self.table[r][col]);
        }
        self.total_count = self.total_count.saturating_add(delta);
        min_val
    }

    pub fn query(&self, item: &[u8]) -> u64 {
        let (h1, h2) = double_hash(item);
        let mut min_val = u64::MAX;

        for r in 0..self.depth {
            let col = (h1.wrapping_add((r as u64).wrapping_mul(h2)) as usize) % self.width;
            min_val = min_val.min(self.table[r][col]);
        }
        min_val
    }
}

// ---------------------------------------------------------------------------
// 4. Top-K Frequency Tracker (Space-Saving Algorithm)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct TopK {
    pub k: usize,
    pub items: HashMap<Bytes, u64>,
}

impl TopK {
    pub fn new(k: usize) -> Self {
        Self {
            k: k.max(1),
            items: HashMap::new(),
        }
    }

    pub fn add(&mut self, item: Bytes, increment: u64) -> Option<Bytes> {
        let inc = increment.max(1);
        if let Some(count) = self.items.get_mut(&item) {
            *count += inc;
            return None;
        }

        if self.items.len() < self.k {
            self.items.insert(item, inc);
            return None;
        }

        // Space-Saving replacement: find element with minimum count. Ties go
        // to the smallest item, not to the HashMap's per-process iteration
        // order, so replaying the same adds (AOF, replicas) evicts the same.
        let min = self
            .items
            .iter()
            .min_by(|a, b| a.1.cmp(b.1).then_with(|| a.0.cmp(b.0)))
            .map(|(k, &v)| (k.clone(), v));

        if let Some((evicted, min_val)) = min {
            self.items.remove(&evicted);
            self.items.insert(item, min_val + inc);
            return Some(evicted);
        }
        None
    }

    pub fn query(&self, item: &[u8]) -> bool {
        self.items.contains_key(item)
    }

    pub fn count(&self, item: &[u8]) -> u64 {
        self.items.get(item).copied().unwrap_or(0)
    }

    /// Items by descending count, ties by item, so the order is the same
    /// in every process.
    pub fn list(&self) -> Vec<(Bytes, u64)> {
        let mut res: Vec<_> = self.items.iter().map(|(k, v)| (k.clone(), *v)).collect();
        res.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        res
    }
}

// ---------------------------------------------------------------------------
// 5. Unified Probabilistic Store
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ProbabilisticStore {
    pub bloom_filters: HashMap<Bytes, BloomFilter>,
    pub cuckoo_filters: HashMap<Bytes, CuckooFilter>,
    pub cms_sketches: HashMap<Bytes, CountMinSketch>,
    pub topk_trackers: HashMap<Bytes, TopK>,
}

impl ProbabilisticStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every structure as `(kind, key, encoded state)`, for the AOF rewrite:
    /// Bloom and Count-Min state can't be rebuilt from user commands.
    pub fn encoded_entries(&self) -> impl Iterator<Item = (ProbKind, &Bytes, Vec<u8>)> {
        fn enc<'a, T: 'a>(
            kind: ProbKind,
            f: fn(&T, &mut Vec<u8>),
        ) -> impl Fn((&'a Bytes, &'a T)) -> (ProbKind, &'a Bytes, Vec<u8>) {
            move |(key, v)| {
                let mut buf = Vec::new();
                f(v, &mut buf);
                (kind, key, buf)
            }
        }
        let bloom = self
            .bloom_filters
            .iter()
            .map(enc(ProbKind::Bloom, BloomFilter::encode));
        let cuckoo = self
            .cuckoo_filters
            .iter()
            .map(enc(ProbKind::Cuckoo, CuckooFilter::encode));
        let cms = self
            .cms_sketches
            .iter()
            .map(enc(ProbKind::Cms, CountMinSketch::encode));
        let topk = self
            .topk_trackers
            .iter()
            .map(enc(ProbKind::TopK, TopK::encode));
        bloom.chain(cuckoo).chain(cms).chain(topk)
    }

    /// Replaces `key`'s structure of `kind` with one decoded from `payload`
    /// (as written by [`Self::encoded_entries`]). Malformed or trailing
    /// bytes are an error and leave the store untouched.
    pub fn restore(
        &mut self,
        kind: ProbKind,
        key: Bytes,
        payload: &[u8],
    ) -> Result<(), &'static str> {
        fn whole<T>(r: Result<(T, usize), &'static str>, len: usize) -> Result<T, &'static str> {
            match r? {
                (v, used) if used == len => Ok(v),
                _ => Err("trailing bytes after the encoded state"),
            }
        }
        let len = payload.len();
        match kind {
            ProbKind::Bloom => {
                let v = whole(BloomFilter::decode(payload), len)?;
                self.bloom_filters.insert(key, v);
            }
            ProbKind::Cuckoo => {
                let v = whole(CuckooFilter::decode(payload), len)?;
                self.cuckoo_filters.insert(key, v);
            }
            ProbKind::Cms => {
                let v = whole(CountMinSketch::decode(payload), len)?;
                self.cms_sketches.insert(key, v);
            }
            ProbKind::TopK => {
                let v = whole(TopK::decode(payload), len)?;
                self.topk_trackers.insert(key, v);
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 6. Binary encoding (RDB entries and the AOF rewrite's *.RESTORE commands)
// ---------------------------------------------------------------------------

/// Which probabilistic structure an encoded state belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbKind {
    Bloom,
    Cuckoo,
    Cms,
    TopK,
}

impl ProbKind {
    /// The internal command that loads an encoded state of this kind.
    pub fn restore_command(self) -> &'static str {
        match self {
            ProbKind::Bloom => "BF.RESTORE",
            ProbKind::Cuckoo => "CF.RESTORE",
            ProbKind::Cms => "CMS.RESTORE",
            ProbKind::TopK => "TOPK.RESTORE",
        }
    }
}

/// Bounds-checked little-endian reader; never panics on short input.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn take(&mut self, n: usize, err: &'static str) -> Result<&'a [u8], &'static str> {
        let end = self.pos.checked_add(n).ok_or(err)?;
        let s = self.data.get(self.pos..end).ok_or(err)?;
        self.pos = end;
        Ok(s)
    }

    fn u16(&mut self, err: &'static str) -> Result<u16, &'static str> {
        Ok(u16::from_le_bytes(self.take(2, err)?.try_into().unwrap()))
    }

    fn u32(&mut self, err: &'static str) -> Result<usize, &'static str> {
        Ok(u32::from_le_bytes(self.take(4, err)?.try_into().unwrap()) as usize)
    }

    fn u64(&mut self, err: &'static str) -> Result<u64, &'static str> {
        Ok(u64::from_le_bytes(self.take(8, err)?.try_into().unwrap()))
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }
}

impl BloomFilter {
    /// Appends the RDB encoding (type 8 payload).
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.capacity as u64).to_le_bytes());
        buf.extend_from_slice(&self.error_rate.to_bits().to_le_bytes());
        buf.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        buf.extend_from_slice(&(self.num_hashes as u32).to_le_bytes());
        buf.extend_from_slice(&(self.count as u64).to_le_bytes());
        buf.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        for word in &self.bits {
            buf.extend_from_slice(&word.to_le_bytes());
        }
    }

    /// Decodes [`Self::encode`]'s output, returning the bytes consumed.
    pub fn decode(data: &[u8]) -> Result<(Self, usize), &'static str> {
        const HDR: &str = "Truncated BloomFilter header";
        let mut r = Reader::new(data);
        let capacity = r.u64(HDR)? as usize;
        let error_rate = f64::from_bits(r.u64(HDR)?);
        let num_bits = r.u64(HDR)? as usize;
        let num_hashes = r.u32(HDR)?;
        let count = r.u64(HDR)? as usize;
        let bits_len = r.u32(HDR)?;
        if num_bits == 0 || num_hashes == 0 || bits_len != num_bits.div_ceil(64) {
            return Err("Invalid BloomFilter dimensions");
        }
        let raw = r.take(bits_len * 8, "Truncated BloomFilter bits")?;
        let bits = raw
            .as_chunks::<8>()
            .0
            .iter()
            .map(|w| u64::from_le_bytes(*w))
            .collect();
        let bf = Self {
            capacity,
            error_rate,
            num_bits,
            num_hashes,
            count,
            bits,
        };
        Ok((bf, r.pos))
    }
}

impl CuckooFilter {
    /// Appends the RDB encoding (type 10 payload).
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.capacity as u64).to_le_bytes());
        buf.extend_from_slice(&(self.num_buckets as u64).to_le_bytes());
        buf.extend_from_slice(&(self.count as u64).to_le_bytes());
        buf.extend_from_slice(&(self.buckets.len() as u32).to_le_bytes());
        for bucket in &self.buckets {
            for &fp in bucket {
                buf.extend_from_slice(&fp.to_le_bytes());
            }
        }
    }

    /// Decodes [`Self::encode`]'s output, returning the bytes consumed.
    pub fn decode(data: &[u8]) -> Result<(Self, usize), &'static str> {
        const HDR: &str = "Truncated CuckooFilter header";
        const BODY: &str = "Truncated CuckooFilter buckets";
        let mut r = Reader::new(data);
        let capacity = r.u64(HDR)? as usize;
        let num_buckets = r.u64(HDR)? as usize;
        let count = r.u64(HDR)? as usize;
        let buckets_len = r.u32(HDR)?;
        if num_buckets == 0 || buckets_len != num_buckets {
            return Err("Invalid CuckooFilter dimensions");
        }
        if r.remaining() < buckets_len * BUCKET_SIZE * 2 {
            return Err(BODY);
        }
        let mut buckets = Vec::with_capacity(buckets_len);
        for _ in 0..buckets_len {
            let mut bucket = [0u16; BUCKET_SIZE];
            for fp in &mut bucket {
                *fp = r.u16(BODY)?;
            }
            buckets.push(bucket);
        }
        let cf = Self {
            capacity,
            num_buckets,
            count,
            buckets,
        };
        Ok((cf, r.pos))
    }
}

impl CountMinSketch {
    /// Appends the RDB encoding (type 11 payload).
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.width as u64).to_le_bytes());
        buf.extend_from_slice(&(self.depth as u32).to_le_bytes());
        buf.extend_from_slice(&self.total_count.to_le_bytes());
        for row in &self.table {
            for &cell in row {
                buf.extend_from_slice(&cell.to_le_bytes());
            }
        }
    }

    /// Decodes [`Self::encode`]'s output, returning the bytes consumed.
    pub fn decode(data: &[u8]) -> Result<(Self, usize), &'static str> {
        const HDR: &str = "Truncated CountMinSketch header";
        const BODY: &str = "Truncated CountMinSketch cells";
        let mut r = Reader::new(data);
        let width = r.u64(HDR)? as usize;
        let depth = r.u32(HDR)?;
        let total_count = r.u64(HDR)?;
        if width == 0 || depth == 0 {
            return Err("Invalid CountMinSketch dimensions");
        }
        let cells_bytes = width
            .checked_mul(depth)
            .and_then(|c| c.checked_mul(8))
            .ok_or(BODY)?;
        let raw = r.take(cells_bytes, BODY)?;
        let table = raw
            .chunks_exact(width * 8)
            .map(|row| {
                row.as_chunks::<8>()
                    .0
                    .iter()
                    .map(|c| u64::from_le_bytes(*c))
                    .collect()
            })
            .collect();
        let cms = Self {
            width,
            depth,
            total_count,
            table,
        };
        Ok((cms, r.pos))
    }
}

impl TopK {
    /// Appends the RDB encoding (type 12 payload).
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.k as u64).to_le_bytes());
        buf.extend_from_slice(&(self.items.len() as u32).to_le_bytes());
        for (item, &count) in &self.items {
            buf.extend_from_slice(&(item.len() as u32).to_le_bytes());
            buf.extend_from_slice(item);
            buf.extend_from_slice(&count.to_le_bytes());
        }
    }

    /// Decodes [`Self::encode`]'s output, returning the bytes consumed.
    pub fn decode(data: &[u8]) -> Result<(Self, usize), &'static str> {
        const HDR: &str = "Truncated TopK header";
        const ITEM: &str = "Truncated TopK item data";
        let mut r = Reader::new(data);
        let k = r.u64(HDR)? as usize;
        let items_len = r.u32(HDR)?;
        if k == 0 || items_len > k {
            return Err("Invalid TopK dimensions");
        }
        // Each item takes at least 12 bytes; don't trust the count for the
        // allocation.
        let mut items = HashMap::with_capacity(items_len.min(r.remaining() / 12));
        for _ in 0..items_len {
            let item_len = r.u32("Truncated TopK item len")?;
            let item = Bytes::copy_from_slice(r.take(item_len, ITEM)?);
            let count = r.u64(ITEM)?;
            items.insert(item, count);
        }
        Ok((Self { k, items }, r.pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bf = BloomFilter::new(1000, 0.01);
        assert!(bf.add(b"apple"));
        assert!(bf.add(b"banana"));
        assert!(!bf.add(b"apple")); // Duplicate returns false

        assert!(bf.contains(b"apple"));
        assert!(bf.contains(b"banana"));
        assert!(!bf.contains(b"orange"));
    }

    #[test]
    fn test_cuckoo_filter_crud() {
        let mut cf = CuckooFilter::new(100);
        assert!(cf.add(b"hello").unwrap());
        assert!(cf.contains(b"hello"));
        assert!(!cf.contains(b"world"));

        assert!(cf.delete(b"hello"));
        assert!(!cf.contains(b"hello"));
        assert!(!cf.delete(b"hello"));
    }

    #[test]
    fn test_count_min_sketch() {
        let mut cms = CountMinSketch::new(1000, 5);
        cms.incr_by(b"packet_loss", 15);
        cms.incr_by(b"packet_loss", 10);
        assert_eq!(cms.query(b"packet_loss"), 25);
        assert_eq!(cms.query(b"unknown"), 0);
    }

    #[test]
    fn test_topk() {
        let mut tk = TopK::new(2);
        tk.add(Bytes::from_static(b"userA"), 10);
        tk.add(Bytes::from_static(b"userB"), 5);
        assert!(tk.query(b"userA"));
        assert!(tk.query(b"userB"));

        let list = tk.list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0.as_ref(), b"userA");
    }

    #[test]
    fn test_topk_ties_do_not_depend_on_hash_order() {
        // Each tracker has its own random hasher seed; ties must still
        // evict, and list, the same items in every one.
        for _ in 0..20 {
            let mut tk = TopK::new(3);
            for item in ["c", "a", "b"] {
                tk.add(Bytes::from(item), 1);
            }
            assert_eq!(tk.add(Bytes::from("d"), 1), Some(Bytes::from("a")));
            assert_eq!(
                tk.list(),
                vec![
                    (Bytes::from("d"), 2),
                    (Bytes::from("b"), 1),
                    (Bytes::from("c"), 1),
                ]
            );
        }
    }

    fn sample_store() -> ProbabilisticStore {
        let mut s = ProbabilisticStore::new();
        let mut bf = BloomFilter::new(100, 0.01);
        bf.add(b"x");
        s.bloom_filters.insert(Bytes::from("bf"), bf);
        let mut cf = CuckooFilter::new(16);
        cf.add(b"x").unwrap();
        s.cuckoo_filters.insert(Bytes::from("cf"), cf);
        let mut cms = CountMinSketch::new(8, 2);
        cms.incr_by(b"x", 3);
        s.cms_sketches.insert(Bytes::from("cms"), cms);
        let mut tk = TopK::new(2);
        tk.add(Bytes::from("x"), 1);
        tk.add(Bytes::from("y"), 2);
        s.topk_trackers.insert(Bytes::from("tk"), tk);
        s
    }

    #[test]
    fn test_encoded_entries_restore_identical_state() {
        let src = sample_store();
        let mut dst = ProbabilisticStore::new();
        let mut kinds = Vec::new();
        for (kind, key, payload) in src.encoded_entries() {
            kinds.push(kind);
            dst.restore(kind, key.clone(), &payload).unwrap();
        }
        kinds.sort_by_key(|k| *k as u8);
        assert_eq!(
            kinds,
            [
                ProbKind::Bloom,
                ProbKind::Cuckoo,
                ProbKind::Cms,
                ProbKind::TopK
            ]
        );
        assert_eq!(dst, src);
    }

    #[test]
    fn test_restore_rejects_malformed_payloads_without_panicking() {
        let src = sample_store();
        for (kind, key, payload) in src.encoded_entries() {
            let mut dst = ProbabilisticStore::new();
            // Every truncation, and trailing garbage, is an error.
            for cut in 0..payload.len() {
                assert!(dst.restore(kind, key.clone(), &payload[..cut]).is_err());
            }
            let mut long = payload.clone();
            long.push(0);
            assert!(dst.restore(kind, key.clone(), &long).is_err());
            // Nothing was half-loaded.
            assert_eq!(dst, ProbabilisticStore::new(), "{kind:?}");
        }

        // Headers whose dimensions would make later queries panic.
        let mut s = ProbabilisticStore::new();
        let k = Bytes::from("k");
        let mut bf = Vec::new();
        BloomFilter {
            num_bits: 0,
            bits: Vec::new(),
            ..BloomFilter::new(10, 0.1)
        }
        .encode(&mut bf);
        assert!(s.restore(ProbKind::Bloom, k.clone(), &bf).is_err());
        let mut cf = Vec::new();
        CuckooFilter {
            num_buckets: 8,
            ..CuckooFilter::new(16)
        }
        .encode(&mut cf);
        assert!(s.restore(ProbKind::Cuckoo, k.clone(), &cf).is_err());
        let mut cms = 0u64.to_le_bytes().to_vec();
        cms.extend_from_slice(&1u32.to_le_bytes());
        cms.extend_from_slice(&0u64.to_le_bytes());
        assert!(s.restore(ProbKind::Cms, k.clone(), &cms).is_err());
        let mut huge = u64::MAX.to_le_bytes().to_vec();
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        huge.extend_from_slice(&0u64.to_le_bytes());
        assert!(s.restore(ProbKind::Cms, k.clone(), &huge).is_err());
        let mut tk = 0u64.to_le_bytes().to_vec();
        tk.extend_from_slice(&0u32.to_le_bytes());
        assert!(s.restore(ProbKind::TopK, k.clone(), &tk).is_err());
        let mut tk = 1u64.to_le_bytes().to_vec();
        tk.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(s.restore(ProbKind::TopK, k, &tk).is_err());
        assert_eq!(s, ProbabilisticStore::new());
    }

    /// Reserve commands refuse sizes past MAX_SKETCH_BYTES instead of
    /// attempting (and aborting on) the allocation.
    #[test]
    fn test_try_new_bounds_client_sizes() {
        assert!(BloomFilter::try_new(usize::MAX, 0.01).is_err());
        let bf = BloomFilter::try_new(1000, 0.01).unwrap();
        assert_eq!(bf, BloomFilter::new(1000, 0.01));
        // About 448M items at 1% fill 512 MiB of bits.
        assert!(BloomFilter::try_new(400_000_000, 0.01).is_ok());
        assert!(BloomFilter::try_new(500_000_000, 0.01).is_err());
        assert!(BloomFilter::try_new(1, 0.01).is_ok());

        assert!(CuckooFilter::try_new(usize::MAX).is_err());
        assert!(CuckooFilter::try_new(1 << 40).is_err());
        assert_eq!(
            CuckooFilter::try_new(1000).unwrap(),
            CuckooFilter::new(1000)
        );

        assert!(CountMinSketch::try_new(usize::MAX, 16).is_err());
        assert!(CountMinSketch::try_new(MAX_SKETCH_BYTES / 8 + 1, 1).is_err());
        assert!(CountMinSketch::try_new(MAX_SKETCH_BYTES / 8 / 16, 16).is_ok());
        assert_eq!(
            CountMinSketch::try_new(50, 3).unwrap(),
            CountMinSketch::new(50, 3)
        );
    }
}
