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

// ---------------------------------------------------------------------------
// 1. Bloom Filter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
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

        // m = -n * ln(p) / (ln(2)^2)
        let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let m = (-(cap as f64) * err.ln() / ln2_sq).ceil() as usize;
        let num_bits = m.max(64);

        // k = (m / n) * ln(2)
        let k = ((num_bits as f64 / cap as f64) * std::f64::consts::LN_2).round() as usize;
        let num_hashes = k.clamp(1, 30);

        let u64_len = (num_bits + 63) / 64;
        Self {
            capacity: cap,
            error_rate: err,
            num_bits,
            num_hashes,
            count: 0,
            bits: vec![0u64; u64_len],
        }
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

#[derive(Debug, Clone)]
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
        let mut cur_i = if (fp as usize) % 2 == 0 { i1 } else { i2 };
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

        // Space-Saving replacement: find element with minimum count
        let mut min_key: Option<Bytes> = None;
        let mut min_val = u64::MAX;

        for (k, &v) in &self.items {
            if v < min_val {
                min_val = v;
                min_key = Some(k.clone());
            }
        }

        if let Some(evicted) = min_key {
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

    pub fn list(&self) -> Vec<(Bytes, u64)> {
        let mut res: Vec<_> = self.items.iter().map(|(k, v)| (k.clone(), *v)).collect();
        res.sort_by(|a, b| b.1.cmp(&a.1));
        res
    }
}

// ---------------------------------------------------------------------------
// 5. Unified Probabilistic Store
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
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
}
