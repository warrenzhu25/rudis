use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Supported vector distance metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    Cosine,
    L2,
    IP, // Inner Product / Dot Product
}

impl Default for VectorMetric {
    fn default() -> Self {
        Self::Cosine
    }
}

impl VectorMetric {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "COSINE" => Some(Self::Cosine),
            "L2" | "EUCLIDEAN" => Some(Self::L2),
            "IP" | "DOT" | "INNERPRODUCT" => Some(Self::IP),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::L2 => "L2",
            Self::IP => "IP",
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn hsum256_ps(v: core::arch::x86_64::__m256) -> f32 {
    use core::arch::x86_64::*;
    let high = _mm256_extractf128_ps(v, 1);
    let low = _mm256_castps256_ps128(v);
    let sum128 = _mm_add_ps(low, high);
    let shuf = _mm_movehdup_ps(sum128);
    let sums = _mm_add_ps(sum128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let res = _mm_add_ss(sums, shuf2);
    _mm_cvtss_f32(res)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    use core::arch::x86_64::*;
    let len = a.len().min(b.len());
    let mut i = 0;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();

    while i + 16 <= len {
        let va0 = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb0 = _mm256_loadu_ps(b.as_ptr().add(i));
        acc0 = _mm256_fmadd_ps(va0, vb0, acc0);

        let va1 = _mm256_loadu_ps(a.as_ptr().add(i + 8));
        let vb1 = _mm256_loadu_ps(b.as_ptr().add(i + 8));
        acc1 = _mm256_fmadd_ps(va1, vb1, acc1);

        i += 16;
    }

    if i + 8 <= len {
        let va = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i));
        acc0 = _mm256_fmadd_ps(va, vb, acc0);
        i += 8;
    }

    let acc = _mm256_add_ps(acc0, acc1);
    let mut sum = hsum256_ps(acc);

    while i < len {
        sum += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn l2_distance_sq_avx2(a: &[f32], b: &[f32]) -> f32 {
    use core::arch::x86_64::*;
    let len = a.len().min(b.len());
    let mut i = 0;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();

    while i + 16 <= len {
        let va0 = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb0 = _mm256_loadu_ps(b.as_ptr().add(i));
        let diff0 = _mm256_sub_ps(va0, vb0);
        acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);

        let va1 = _mm256_loadu_ps(a.as_ptr().add(i + 8));
        let vb1 = _mm256_loadu_ps(b.as_ptr().add(i + 8));
        let diff1 = _mm256_sub_ps(va1, vb1);
        acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);

        i += 16;
    }

    if i + 8 <= len {
        let va = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i));
        let diff = _mm256_sub_ps(va, vb);
        acc0 = _mm256_fmadd_ps(diff, diff, acc0);
        i += 8;
    }

    let acc = _mm256_add_ps(acc0, acc1);
    let mut sum = hsum256_ps(acc);

    while i < len {
        let diff = *a.get_unchecked(i) - *b.get_unchecked(i);
        sum += diff * diff;
        i += 1;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn dot_f32_u8_avx2(query: &[f32], u8_data: &[u8]) -> f32 {
    use core::arch::x86_64::*;
    let len = query.len().min(u8_data.len());
    let mut i = 0;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();

    while i + 16 <= len {
        let raw8_0 = _mm_loadl_epi64(u8_data.as_ptr().add(i) as *const __m128i);
        let i32_0 = _mm256_cvtepu8_epi32(raw8_0);
        let f32_0 = _mm256_cvtepi32_ps(i32_0);
        let q0 = _mm256_loadu_ps(query.as_ptr().add(i));
        acc0 = _mm256_fmadd_ps(q0, f32_0, acc0);

        let raw8_1 = _mm_loadl_epi64(u8_data.as_ptr().add(i + 8) as *const __m128i);
        let i32_1 = _mm256_cvtepu8_epi32(raw8_1);
        let f32_1 = _mm256_cvtepi32_ps(i32_1);
        let q1 = _mm256_loadu_ps(query.as_ptr().add(i + 8));
        acc1 = _mm256_fmadd_ps(q1, f32_1, acc1);

        i += 16;
    }

    if i + 8 <= len {
        let raw8 = _mm_loadl_epi64(u8_data.as_ptr().add(i) as *const __m128i);
        let i32_v = _mm256_cvtepu8_epi32(raw8);
        let f32_v = _mm256_cvtepi32_ps(i32_v);
        let q = _mm256_loadu_ps(query.as_ptr().add(i));
        acc0 = _mm256_fmadd_ps(q, f32_v, acc0);
        i += 8;
    }

    let acc = _mm256_add_ps(acc0, acc1);
    let mut sum = hsum256_ps(acc);

    while i < len {
        sum += *query.get_unchecked(i) * (*u8_data.get_unchecked(i) as f32);
        i += 1;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn l2_f32_u8_avx2(query: &[f32], u8_data: &[u8], min_val: f32, scale: f32) -> f32 {
    use core::arch::x86_64::*;
    let len = query.len().min(u8_data.len());
    let mut i = 0;
    let mut acc0 = _mm256_setzero_ps();
    let min_vec = _mm256_set1_ps(min_val);
    let scale_vec = _mm256_set1_ps(scale);

    while i + 8 <= len {
        let raw8 = _mm_loadl_epi64(u8_data.as_ptr().add(i) as *const __m128i);
        let i32_v = _mm256_cvtepu8_epi32(raw8);
        let f32_v = _mm256_cvtepi32_ps(i32_v);
        let b_v = _mm256_fmadd_ps(f32_v, scale_vec, min_vec);
        let q = _mm256_loadu_ps(query.as_ptr().add(i));
        let diff = _mm256_sub_ps(q, b_v);
        acc0 = _mm256_fmadd_ps(diff, diff, acc0);
        i += 8;
    }

    let mut sum = hsum256_ps(acc0);
    while i < len {
        let b_val = min_val + (*u8_data.get_unchecked(i) as f32) * scale;
        let diff = *query.get_unchecked(i) - b_val;
        sum += diff * diff;
        i += 1;
    }
    sum
}


/// Computes SIMD-accelerated dot product of two float vectors with runtime AVX2 detection.
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { dot_product_avx2(a, b) };
        }
    }
    dot_product_portable(a, b)
}

#[inline]
fn dot_product_portable(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let chunks_a = a.chunks_exact(8);
    let chunks_b = b.chunks_exact(8);
    let rem_a = chunks_a.remainder();
    let rem_b = chunks_b.remainder();

    for (ca, cb) in chunks_a.zip(chunks_b) {
        let mut s = 0.0f32;
        for i in 0..8 {
            s += ca[i] * cb[i];
        }
        sum += s;
    }
    for (va, vb) in rem_a.iter().zip(rem_b.iter()) {
        sum += va * vb;
    }
    sum
}

/// Computes SIMD-accelerated squared L2 Euclidean distance with runtime AVX2 detection.
#[inline]
pub fn l2_distance_sq(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { l2_distance_sq_avx2(a, b) };
        }
    }
    l2_distance_sq_portable(a, b)
}

#[inline]
fn l2_distance_sq_portable(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let chunks_a = a.chunks_exact(8);
    let chunks_b = b.chunks_exact(8);
    let rem_a = chunks_a.remainder();
    let rem_b = chunks_b.remainder();

    for (ca, cb) in chunks_a.zip(chunks_b) {
        let mut s = 0.0f32;
        for i in 0..8 {
            let diff = ca[i] - cb[i];
            s += diff * diff;
        }
        sum += s;
    }
    for (va, vb) in rem_a.iter().zip(rem_b.iter()) {
        let diff = va - vb;
        sum += diff * diff;
    }
    sum
}

/// Computes vector distance according to selected metric.
#[inline]
pub fn compute_distance(a: &[f32], b: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::L2 => l2_distance_sq(a, b).sqrt(),
        VectorMetric::IP => -dot_product(a, b),
        VectorMetric::Cosine => {
            let dot = dot_product(a, b);
            let norm_a = dot_product(a, a).sqrt();
            let norm_b = dot_product(b, b).sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                1.0
            } else {
                (1.0 - (dot / (norm_a * norm_b))).max(0.0)
            }
        }
    }
}

/// 8-bit Scalar Quantization (SQ8) for high-dimensional vector embeddings.
/// Reduces vector memory consumption by 75% (from 4 bytes/dim down to 1 byte/dim).
#[derive(Clone, Debug, PartialEq)]
pub struct QuantizedVector {
    pub min_val: f32,
    pub scale: f32,
    pub sum_q: f32,
    pub sum_q_sq: f32,
    pub data: Vec<u8>,
}

impl QuantizedVector {
    pub fn quantize(v: &[f32]) -> Self {
        let mut min_val = f32::INFINITY;
        let mut max_val = f32::NEG_INFINITY;
        for &x in v {
            if x < min_val { min_val = x; }
            if x > max_val { max_val = x; }
        }
        let diff = max_val - min_val;
        let scale = if diff == 0.0 { 1.0 } else { diff / 255.0 };
        let inv_scale = 1.0 / scale;

        let mut data = Vec::with_capacity(v.len());
        let mut sum_q = 0.0f32;
        let mut sum_q_sq = 0.0f32;
        for &x in v {
            let q = (((x - min_val) * inv_scale).round().clamp(0.0, 255.0)) as u8;
            let q_f = q as f32;
            sum_q += q_f;
            sum_q_sq += q_f * q_f;
            data.push(q);
        }
        Self { min_val, scale, sum_q, sum_q_sq, data }
    }

    /// Dequantizes back to full-precision float vector.
    pub fn dequantize(&self) -> Vec<f32> {
        self.data.iter().map(|&q| self.min_val + (q as f32) * self.scale).collect()
    }

    /// Fast asymmetric distance computation between full-precision query vector and SQ8 vector.
    pub fn compute_distance(&self, query: &[f32], metric: VectorMetric) -> f32 {
        let min_val = self.min_val;
        let scale = self.scale;
        match metric {
            VectorMetric::IP => {
                let dot_u8 = self.dot_u8(query);
                let sum_query = self.sum_query(query);
                let dot = min_val * sum_query + scale * dot_u8;
                -dot
            }
            VectorMetric::L2 => {
                let l2_sq = self.l2_sq(query);
                l2_sq.sqrt()
            }
            VectorMetric::Cosine => {
                let dot_u8 = self.dot_u8(query);
                let sum_query = self.sum_query(query);
                let dot = min_val * sum_query + scale * dot_u8;

                let dim = self.data.len() as f32;
                let norm_b_sq = (dim * min_val * min_val + 2.0 * min_val * scale * self.sum_q + scale * scale * self.sum_q_sq).max(0.0);
                let norm_a_sq = dot_product(query, query);

                let denom = norm_a_sq.sqrt() * norm_b_sq.sqrt();
                if denom == 0.0 { 1.0 } else { (1.0 - (dot / denom)).max(0.0) }
            }
        }
    }

    #[inline]
    fn dot_u8(&self, query: &[f32]) -> f32 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return unsafe { dot_f32_u8_avx2(query, &self.data) };
            }
        }
        let mut dot = 0.0f32;
        for (&q_val, &d_u8) in query.iter().zip(&self.data) {
            dot += q_val * (d_u8 as f32);
        }
        dot
    }

    #[inline]
    fn sum_query(&self, query: &[f32]) -> f32 {
        let mut sum = 0.0f32;
        for &q in query {
            sum += q;
        }
        sum
    }

    #[inline]
    fn l2_sq(&self, query: &[f32]) -> f32 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return unsafe { l2_f32_u8_avx2(query, &self.data, self.min_val, self.scale) };
            }
        }
        let mut l2 = 0.0f32;
        for (&q_val, &d_u8) in query.iter().zip(&self.data) {
            let diff = q_val - (self.min_val + (d_u8 as f32) * self.scale);
            l2 += diff * diff;
        }
        l2
    }
}

/// Product Quantization (PQ) with Asymmetric Distance Computation (ADC).
/// Decomposes D-dimensional vectors into M sub-vectors of dimension (D/M),
/// compressing vectors by up to 96.9% (e.g. 128-dim Float32 512 bytes -> 16 bytes).
#[derive(Clone, Debug, PartialEq)]
pub struct PQVector {
    pub codes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct ProductQuantizer {
    pub dim: usize,
    pub m: usize,
    pub d_sub: usize,
    pub codebooks: Vec<Vec<Vec<f32>>>,
}

impl ProductQuantizer {
    pub fn new(dim: usize, m: usize) -> Self {
        let m = m.max(1);
        let d_sub = (dim / m).max(1);
        let mut codebooks = Vec::with_capacity(m);
        for sub in 0..m {
            let mut centroids = Vec::with_capacity(256);
            for c in 0..256 {
                let mut centroid = vec![0.0f32; d_sub];
                if c == 0 {
                    // Zero vector
                } else if c <= d_sub {
                    // Positive basis vector
                    centroid[c - 1] = 1.0;
                } else if c <= 2 * d_sub {
                    // Negative basis vector
                    centroid[c - d_sub - 1] = -1.0;
                } else {
                    for i in 0..d_sub {
                        let mut h = (c as u64).wrapping_mul(0x9E3779B97F4A7C15)
                            ^ ((sub + 1) as u64).wrapping_mul(0xC6A4A7935BD1E995)
                            ^ ((i + 1) as u64).wrapping_mul(0x517CC1B727220A95);
                        h = (h ^ (h >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
                        h = (h ^ (h >> 27)).wrapping_mul(0x94D049BB133111EB);
                        h ^= h >> 31;
                        centroid[i] = ((h & 0xFFFF) as f32 / 32767.5) - 1.0;
                    }
                }
                centroids.push(centroid);
            }
            codebooks.push(centroids);
        }
        Self { dim, m, d_sub, codebooks }
    }

    pub fn encode(&self, v: &[f32]) -> PQVector {
        let mut codes = Vec::with_capacity(self.m);
        for m in 0..self.m {
            let start = m * self.d_sub;
            let end = (start + self.d_sub).min(v.len());
            let sub_v = &v[start..end];
            let mut best_c = 0u8;
            let mut best_dist = f32::INFINITY;
            for (c, centroid) in self.codebooks[m].iter().enumerate() {
                let mut dist = 0.0f32;
                for (i, &x) in sub_v.iter().enumerate() {
                    let diff = x - centroid[i];
                    dist += diff * diff;
                }
                if dist < best_dist {
                    best_dist = dist;
                    best_c = c as u8;
                }
            }
            codes.push(best_c);
        }
        PQVector { codes }
    }

    pub fn compute_distance_table(&self, query: &[f32]) -> Vec<[f32; 256]> {
        let mut table = vec![[0.0f32; 256]; self.m];
        for m in 0..self.m {
            let start = m * self.d_sub;
            let end = (start + self.d_sub).min(query.len());
            let sub_q = &query[start..end];
            for c in 0..256 {
                let centroid = &self.codebooks[m][c];
                let mut sq = 0.0f32;
                for (i, &q) in sub_q.iter().enumerate() {
                    let diff = q - centroid[i];
                    sq += diff * diff;
                }
                table[m][c] = sq;
            }
        }
        table
    }

    #[inline]
    pub fn compute_distance_adc(&self, table: &[[f32; 256]], pq: &PQVector) -> f32 {
        let mut sum = 0.0f32;
        for (m, &c) in pq.codes.iter().enumerate() {
            if m < table.len() {
                sum += table[m][c as usize];
            }
        }
        sum
    }

    pub fn compute_distance_with_vec(&self, query: &[f32], pq: &PQVector) -> f32 {
        let table = self.compute_distance_table(query);
        self.compute_distance_adc(&table, pq)
    }
}

#[derive(Clone, Debug)]
pub struct HnswNode {
    pub id: usize,
    pub key: Bytes,
    pub vector: Vec<f32>,
    pub quantized: Option<QuantizedVector>,
    pub pq: Option<PQVector>,
    pub is_tiered: bool,
    /// Neighbors at each layer [0..layer]
    pub neighbors: Vec<Vec<usize>>,
}

#[derive(Copy, Clone, PartialEq)]
struct Candidate {
    id: usize,
    distance: f32,
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap by distance (smaller distance has higher priority)
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone, PartialEq)]
struct FurthestCandidate {
    id: usize,
    distance: f32,
}

impl Eq for FurthestCandidate {}

impl Ord for FurthestCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap by distance (furthest distance has higher priority)
        self.distance.partial_cmp(&other.distance).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for FurthestCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Hierarchical Navigable Small World (HNSW) Vector Index
pub struct HnswIndex {
    pub name: String,
    pub dim: usize,
    pub metric: VectorMetric,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub ml: f64,
    pub entry_point: Option<usize>,
    pub max_layer: usize,
    pub nodes: Vec<Option<HnswNode>>,
    pub key_to_id: HashMap<Bytes, usize>,
    pub pq_quantizer: Option<ProductQuantizer>,
    rng_state: u64,
}

impl HnswIndex {
    pub fn new(name: String, dim: usize, metric: VectorMetric) -> Self {
        let m = 16;
        let m0 = 32;
        let ml = 1.0 / (m as f64).ln();
        Self {
            name,
            dim,
            metric,
            m,
            m0,
            ef_construction: 64,
            ef_search: 32,
            ml,
            entry_point: None,
            max_layer: 0,
            nodes: Vec::new(),
            key_to_id: HashMap::new(),
            pq_quantizer: None,
            rng_state: 0x853c49e6748fea9b,
        }
    }

    pub fn enable_pq(&mut self, m: usize) {
        self.pq_quantizer = Some(ProductQuantizer::new(self.dim, m));
    }

    fn next_random_f64(&mut self) -> f64 {
        self.rng_state ^= self.rng_state << 13;
        self.rng_state ^= self.rng_state >> 7;
        self.rng_state ^= self.rng_state << 17;
        (self.rng_state as f64) / (u64::MAX as f64)
    }

    fn random_level(&mut self) -> usize {
        let r = self.next_random_f64().max(1e-15);
        let level = (-r.ln() * self.ml).floor() as usize;
        level.min(16)
    }

    pub fn len(&self) -> usize {
        self.key_to_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.key_to_id.is_empty()
    }

    pub fn get_vector(&self, key: &Bytes) -> Option<&[f32]> {
        self.key_to_id
            .get(key)
            .and_then(|&id| self.nodes.get(id).and_then(|n| n.as_ref().map(|n| n.vector.as_slice())))
    }

    #[inline]
    pub fn dist_to_node(&self, query: &[f32], node: &HnswNode) -> f32 {
        if let Some(pq) = &node.pq {
            if let Some(quantizer) = &self.pq_quantizer {
                return quantizer.compute_distance_with_vec(query, pq);
            }
        }
        if let Some(quant) = &node.quantized {
            quant.compute_distance(query, self.metric)
        } else {
            compute_distance(query, &node.vector, self.metric)
        }
    }

    /// Adds or updates a vector in the HNSW index.
    pub fn add(&mut self, key: Bytes, vector: Vec<f32>) -> Result<(), &'static str> {
        self.add_quantized(key, vector, false, false)
    }

    /// Adds or updates a vector with optional SQ8 quantization and tiered flag.
    pub fn add_quantized(
        &mut self,
        key: Bytes,
        vector: Vec<f32>,
        quantize: bool,
        tiered: bool,
    ) -> Result<(), &'static str> {
        self.add_quantized_ext(key, vector, quantize, false, tiered)
    }

    /// Adds or updates a vector with flexible quantization (SQ8 or PQ) and tiered storage.
    pub fn add_quantized_ext(
        &mut self,
        key: Bytes,
        vector: Vec<f32>,
        quantize_sq8: bool,
        quantize_pq: bool,
        tiered: bool,
    ) -> Result<(), &'static str> {
        if vector.len() != self.dim {
            return Err("vector dimension mismatch");
        }

        // Remove previous key if exists
        if self.key_to_id.contains_key(&key) {
            self.remove(&key);
        }

        let target_level = self.random_level();
        let new_id = self.nodes.len();

        let quantized = if quantize_sq8 || (tiered && !quantize_pq) {
            Some(QuantizedVector::quantize(&vector))
        } else {
            None
        };

        let pq = if quantize_pq {
            if self.pq_quantizer.is_none() {
                let m = (self.dim / 8).max(1).min(16);
                self.pq_quantizer = Some(ProductQuantizer::new(self.dim, m));
            }
            self.pq_quantizer.as_ref().map(|q| q.encode(&vector))
        } else {
            None
        };

        let node = HnswNode {
            id: new_id,
            key: key.clone(),
            vector: vector.clone(),
            quantized,
            pq,
            is_tiered: tiered,
            neighbors: vec![Vec::new(); target_level + 1],
        };

        if self.entry_point.is_none() {
            self.entry_point = Some(new_id);
            self.max_layer = target_level;
            self.nodes.push(Some(node));
            self.key_to_id.insert(key, new_id);
            return Ok(());
        }

        let mut curr_obj = self.entry_point.unwrap();
        let mut curr_dist = self.dist_to_node(&vector, self.nodes[curr_obj].as_ref().unwrap());

        // 1. Greedy search from top down to target_level + 1
        for lc in (target_level + 1..=self.max_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(curr_node) = &self.nodes[curr_obj] {
                    if lc < curr_node.neighbors.len() {
                        for &neighbor in &curr_node.neighbors[lc] {
                            if let Some(n) = &self.nodes[neighbor] {
                                let d = self.dist_to_node(&vector, n);
                                if d < curr_dist {
                                    curr_dist = d;
                                    curr_obj = neighbor;
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2. Search and link at layers min(target_level, max_layer) down to 0
        self.nodes.push(Some(node));
        self.key_to_id.insert(key, new_id);

        let search_level_max = target_level.min(self.max_layer);
        for lc in (0..=search_level_max).rev() {
            let candidates = self.search_layer(&vector, curr_obj, self.ef_construction, lc);
            let m_max = if lc == 0 { self.m0 } else { self.m };
            let neighbors: Vec<usize> = candidates.into_iter().take(m_max).map(|c| c.id).collect();

            // Connect new node to neighbors
            if let Some(n) = &mut self.nodes[new_id] {
                n.neighbors[lc] = neighbors.clone();
            }

            // Connect neighbors back to new node
            for &nbr_id in &neighbors {
                if let Some(nbr) = &mut self.nodes[nbr_id] {
                    if lc < nbr.neighbors.len() {
                        nbr.neighbors[lc].push(new_id);
                        if nbr.neighbors[lc].len() > m_max {
                            // Prune furthest neighbor
                            self.prune_neighbors(nbr_id, lc, m_max);
                        }
                    }
                }
            }

            if let Some(&closest) = neighbors.first() {
                curr_obj = closest;
            }
        }

        if target_level > self.max_layer {
            self.max_layer = target_level;
            self.entry_point = Some(new_id);
        }

        Ok(())
    }

    fn prune_neighbors(&mut self, node_id: usize, layer: usize, max_neighbors: usize) {
        if let Some(node) = &self.nodes[node_id] {
            let node_vec = node.vector.clone();
            let mut candidates: Vec<(usize, f32)> = node.neighbors[layer]
                .iter()
                .filter_map(|&id| {
                    self.nodes
                        .get(id)
                        .and_then(|opt| opt.as_ref())
                        .map(|n| (id, self.dist_to_node(&node_vec, n)))
                })
                .collect();
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            candidates.truncate(max_neighbors);

            let new_nbrs: Vec<usize> = candidates.into_iter().map(|(id, _)| id).collect();
            if let Some(node_mut) = &mut self.nodes[node_id] {
                node_mut.neighbors[layer] = new_nbrs;
            }
        }
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry_point: usize,
        ef: usize,
        layer: usize,
    ) -> Vec<Candidate> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut w = BinaryHeap::new(); // max-heap of closest elements found

        let initial_dist = self.dist_to_node(query, self.nodes[entry_point].as_ref().unwrap());

        visited.insert(entry_point);
        candidates.push(Candidate {
            id: entry_point,
            distance: initial_dist,
        });
        w.push(FurthestCandidate {
            id: entry_point,
            distance: initial_dist,
        });

        while let Some(curr) = candidates.pop() {
            if let Some(furthest) = w.peek() {
                if curr.distance > furthest.distance && w.len() >= ef {
                    break;
                }
            }

            if let Some(node) = &self.nodes[curr.id] {
                if layer < node.neighbors.len() {
                    for &nbr_id in &node.neighbors[layer] {
                        if visited.insert(nbr_id) {
                            if let Some(nbr_node) = &self.nodes[nbr_id] {
                                let d = self.dist_to_node(query, nbr_node);
                                let furthest_dist = w.peek().map(|f| f.distance).unwrap_or(f32::MAX);

                                if d < furthest_dist || w.len() < ef {
                                    candidates.push(Candidate {
                                        id: nbr_id,
                                        distance: d,
                                    });
                                    w.push(FurthestCandidate {
                                        id: nbr_id,
                                        distance: d,
                                    });
                                    if w.len() > ef {
                                        w.pop();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut results: Vec<Candidate> = w
            .into_iter()
            .map(|f| Candidate {
                id: f.id,
                distance: f.distance,
            })
            .collect();
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));
        results
    }

    /// Searches for top-k nearest neighbors.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(Bytes, f32)> {
        self.search_tiered(query, k, false)
    }

    /// Searches for top-k nearest neighbors with optional exact reranking.
    pub fn search_tiered(&self, query: &[f32], k: usize, rerank: bool) -> Vec<(Bytes, f32)> {
        if self.entry_point.is_none() || self.is_empty() {
            return Vec::new();
        }

        let mut curr_obj = self.entry_point.unwrap();
        let mut curr_dist = self.dist_to_node(query, self.nodes[curr_obj].as_ref().unwrap());

        // 1. Greedy search down to layer 1
        for lc in (1..=self.max_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(curr_node) = &self.nodes[curr_obj] {
                    if lc < curr_node.neighbors.len() {
                        for &nbr in &curr_node.neighbors[lc] {
                            if let Some(n) = &self.nodes[nbr] {
                                let d = self.dist_to_node(query, n);
                                if d < curr_dist {
                                    curr_dist = d;
                                    curr_obj = nbr;
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2. Layer 0 search with ef_search
        let search_ef = if rerank {
            self.ef_search.max(k * 3)
        } else {
            self.ef_search.max(k)
        };
        let candidates = self.search_layer(query, curr_obj, search_ef, 0);

        if rerank {
            let mut exact_results: Vec<(Bytes, f32)> = candidates
                .into_iter()
                .filter_map(|c| {
                    self.nodes[c.id].as_ref().map(|n| {
                        let exact_d = compute_distance(query, &n.vector, self.metric);
                        (n.key.clone(), exact_d)
                    })
                })
                .collect();
            exact_results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            exact_results.truncate(k);
            exact_results
        } else {
            candidates
                .into_iter()
                .take(k)
                .filter_map(|c| {
                    self.nodes[c.id]
                        .as_ref()
                        .map(|n| (n.key.clone(), c.distance))
                })
                .collect()
        }
    }

    /// Removes a key from the index.
    pub fn remove(&mut self, key: &Bytes) -> bool {
        if let Some(id) = self.key_to_id.remove(key) {
            let nbrs_by_layer = self.nodes[id].as_ref().map(|n| n.neighbors.clone());
            if let Some(nbrs_by_layer) = nbrs_by_layer {
                for (layer, nbrs) in nbrs_by_layer.into_iter().enumerate() {
                    for nbr_id in nbrs {
                        if let Some(nbr_node) = &mut self.nodes[nbr_id] {
                            if layer < nbr_node.neighbors.len() {
                                nbr_node.neighbors[layer].retain(|&x| x != id);
                            }
                        }
                    }
                }
            }
            self.nodes[id] = None;
            if self.entry_point == Some(id) {
                // Find next valid entry point
                self.entry_point = self.nodes.iter().position(|n| n.is_some());
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_basic_crud_and_search() {
        let mut index = HnswIndex::new("test_idx".to_string(), 4, VectorMetric::Cosine);

        let v1 = vec![1.0, 0.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0, 0.0];
        let v3 = vec![0.9, 0.1, 0.0, 0.0];

        index.add(Bytes::from("doc1"), v1).unwrap();
        index.add(Bytes::from("doc2"), v2).unwrap();
        index.add(Bytes::from("doc3"), v3).unwrap();

        assert_eq!(index.len(), 3);

        let query = vec![1.0, 0.05, 0.0, 0.0];
        let results = index.search(&query, 2);
        assert_eq!(results.len(), 2);
        // doc1 or doc3 should be closest
        assert!(results[0].0 == Bytes::from("doc1") || results[0].0 == Bytes::from("doc3"));

        assert!(index.remove(&Bytes::from("doc1")));
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn test_sq8_quantization_and_tiered_rerank() {
        let v = vec![0.12, -0.45, 0.98, 0.05, 0.33];
        let q = QuantizedVector::quantize(&v);
        assert_eq!(q.data.len(), 5);
        let deq = q.dequantize();
        for (orig, recon) in v.iter().zip(&deq) {
            assert!((orig - recon).abs() < 0.02, "orig: {}, recon: {}", orig, recon);
        }

        let mut index = HnswIndex::new("sq8_idx".to_string(), 5, VectorMetric::Cosine);
        index.add_quantized(Bytes::from("k1"), v.clone(), true, true).unwrap();
        index.add_quantized(Bytes::from("k2"), vec![0.0, 1.0, 0.0, 0.0, 0.0], true, true).unwrap();

        let query = vec![0.10, -0.40, 0.95, 0.08, 0.30];
        let res_approx = index.search_tiered(&query, 1, false);
        assert_eq!(res_approx[0].0, Bytes::from("k1"));

        let res_rerank = index.search_tiered(&query, 1, true);
        assert_eq!(res_rerank[0].0, Bytes::from("k1"));
    }

    #[test]
    fn test_pq_quantization_and_adc() {
        let dim = 16;
        let m = 4;
        let pq = ProductQuantizer::new(dim, m);
        let v1 = vec![0.5; dim];
        let v2 = vec![-0.5; dim];

        let code1 = pq.encode(&v1);
        let code2 = pq.encode(&v2);
        assert_eq!(code1.codes.len(), m);
        assert_eq!(code2.codes.len(), m);

        let query = vec![0.48; dim];
        let d1 = pq.compute_distance_with_vec(&query, &code1);
        let d2 = pq.compute_distance_with_vec(&query, &code2);
        assert!(d1 < d2, "v1 should be much closer to query than v2: d1={}, d2={}", d1, d2);

        let mut index = HnswIndex::new("pq_idx".to_string(), dim, VectorMetric::L2);
        index.add_quantized_ext(Bytes::from("doc_pos"), v1, false, true, false).unwrap();
        index.add_quantized_ext(Bytes::from("doc_neg"), v2, false, true, false).unwrap();

        let res = index.search(&query, 1);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, Bytes::from("doc_pos"));
    }
}
