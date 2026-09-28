use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Supported vector distance metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorMetric {
    #[default]
    Cosine,
    L2,
    IP, // Inner Product / Dot Product
}

impl std::str::FromStr for VectorMetric {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "COSINE" => Ok(Self::Cosine),
            "L2" | "EUCLIDEAN" => Ok(Self::L2),
            "IP" | "DOT" | "INNERPRODUCT" => Ok(Self::IP),
            _ => Err(()),
        }
    }
}

impl VectorMetric {
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
#[inline]
#[target_feature(enable = "avx512f")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn hsum512_ps(v: core::arch::x86_64::__m512) -> f32 {
    use core::arch::x86_64::*;
    let low = _mm512_castps512_ps256(v);
    let high = _mm512_extractf32x8_ps(v, 1);
    let sum256 = _mm256_add_ps(low, high);
    hsum256_ps(sum256)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn dot_product_avx512(a: &[f32], b: &[f32]) -> f32 {
    use core::arch::x86_64::*;
    let len = a.len().min(b.len());
    let mut i = 0;
    let mut acc0 = _mm512_setzero_ps();
    let mut acc1 = _mm512_setzero_ps();

    while i + 32 <= len {
        let va0 = _mm512_loadu_ps(a.as_ptr().add(i));
        let vb0 = _mm512_loadu_ps(b.as_ptr().add(i));
        acc0 = _mm512_fmadd_ps(va0, vb0, acc0);

        let va1 = _mm512_loadu_ps(a.as_ptr().add(i + 16));
        let vb1 = _mm512_loadu_ps(b.as_ptr().add(i + 16));
        acc1 = _mm512_fmadd_ps(va1, vb1, acc1);

        i += 32;
    }

    if i + 16 <= len {
        let va = _mm512_loadu_ps(a.as_ptr().add(i));
        let vb = _mm512_loadu_ps(b.as_ptr().add(i));
        acc0 = _mm512_fmadd_ps(va, vb, acc0);
        i += 16;
    }

    let acc = _mm512_add_ps(acc0, acc1);
    let mut sum = hsum512_ps(acc);

    while i < len {
        sum += *a.get_unchecked(i) * *b.get_unchecked(i);
        i += 1;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn l2_distance_sq_avx512(a: &[f32], b: &[f32]) -> f32 {
    use core::arch::x86_64::*;
    let len = a.len().min(b.len());
    let mut i = 0;
    let mut acc0 = _mm512_setzero_ps();
    let mut acc1 = _mm512_setzero_ps();

    while i + 32 <= len {
        let va0 = _mm512_loadu_ps(a.as_ptr().add(i));
        let vb0 = _mm512_loadu_ps(b.as_ptr().add(i));
        let diff0 = _mm512_sub_ps(va0, vb0);
        acc0 = _mm512_fmadd_ps(diff0, diff0, acc0);

        let va1 = _mm512_loadu_ps(a.as_ptr().add(i + 16));
        let vb1 = _mm512_loadu_ps(b.as_ptr().add(i + 16));
        let diff1 = _mm512_sub_ps(va1, vb1);
        acc1 = _mm512_fmadd_ps(diff1, diff1, acc1);

        i += 32;
    }

    if i + 16 <= len {
        let va = _mm512_loadu_ps(a.as_ptr().add(i));
        let vb = _mm512_loadu_ps(b.as_ptr().add(i));
        let diff = _mm512_sub_ps(va, vb);
        acc0 = _mm512_fmadd_ps(diff, diff, acc0);
        i += 16;
    }

    let acc = _mm512_add_ps(acc0, acc1);
    let mut sum = hsum512_ps(acc);

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
unsafe fn cosine_distance_avx2(a: &[f32], b: &[f32]) -> f32 {
    use core::arch::x86_64::*;
    let len = a.len().min(b.len());
    let mut i = 0;
    let mut acc_dot = _mm256_setzero_ps();
    let mut acc_na = _mm256_setzero_ps();
    let mut acc_nb = _mm256_setzero_ps();

    while i + 8 <= len {
        let va = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i));
        acc_dot = _mm256_fmadd_ps(va, vb, acc_dot);
        acc_na = _mm256_fmadd_ps(va, va, acc_na);
        acc_nb = _mm256_fmadd_ps(vb, vb, acc_nb);
        i += 8;
    }

    let mut dot = hsum256_ps(acc_dot);
    let mut na = hsum256_ps(acc_na);
    let mut nb = hsum256_ps(acc_nb);

    while i < len {
        let x = *a.get_unchecked(i);
        let y = *b.get_unchecked(i);
        dot += x * y;
        na += x * x;
        nb += y * y;
        i += 1;
    }

    let norm = (na * nb).sqrt();
    if norm == 0.0 {
        1.0
    } else {
        (1.0 - (dot / norm)).max(0.0)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn cosine_distance_avx512(a: &[f32], b: &[f32]) -> f32 {
    use core::arch::x86_64::*;
    let len = a.len().min(b.len());
    let mut i = 0;
    let mut acc_dot = _mm512_setzero_ps();
    let mut acc_na = _mm512_setzero_ps();
    let mut acc_nb = _mm512_setzero_ps();

    while i + 16 <= len {
        let va = _mm512_loadu_ps(a.as_ptr().add(i));
        let vb = _mm512_loadu_ps(b.as_ptr().add(i));
        acc_dot = _mm512_fmadd_ps(va, vb, acc_dot);
        acc_na = _mm512_fmadd_ps(va, va, acc_na);
        acc_nb = _mm512_fmadd_ps(vb, vb, acc_nb);
        i += 16;
    }

    let mut dot = hsum512_ps(acc_dot);
    let mut na = hsum512_ps(acc_na);
    let mut nb = hsum512_ps(acc_nb);

    while i < len {
        let x = *a.get_unchecked(i);
        let y = *b.get_unchecked(i);
        dot += x * y;
        na += x * x;
        nb += y * y;
        i += 1;
    }

    let norm = (na * nb).sqrt();
    if norm == 0.0 {
        1.0
    } else {
        (1.0 - (dot / norm)).max(0.0)
    }
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

/// Computes SIMD-accelerated dot product of two float vectors with runtime AVX-512 / AVX2 detection.
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { dot_product_avx512(a, b) };
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { dot_product_avx2(a, b) };
        }
    }
    dot_product_portable(a, b)
}

#[inline]
fn dot_product_portable(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let (chunks_a, rem_a) = a.as_chunks::<8>();
    let (chunks_b, rem_b) = b.as_chunks::<8>();

    for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
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

/// Computes SIMD-accelerated squared L2 Euclidean distance with runtime AVX-512 / AVX2 detection.
#[inline]
pub fn l2_distance_sq(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { l2_distance_sq_avx512(a, b) };
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { l2_distance_sq_avx2(a, b) };
        }
    }
    l2_distance_sq_portable(a, b)
}

#[inline]
fn l2_distance_sq_portable(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let (chunks_a, rem_a) = a.as_chunks::<8>();
    let (chunks_b, rem_b) = b.as_chunks::<8>();

    for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
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

/// Computes SIMD-accelerated Cosine distance in a single pass with runtime AVX-512 / AVX2 detection.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { cosine_distance_avx512(a, b) };
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { cosine_distance_avx2(a, b) };
        }
    }
    cosine_distance_portable(a, b)
}

#[inline]
fn cosine_distance_portable(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    let (chunks_a, rem_a) = a.as_chunks::<8>();
    let (chunks_b, rem_b) = b.as_chunks::<8>();

    for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
        for i in 0..8 {
            let x = ca[i];
            let y = cb[i];
            dot += x * y;
            na += x * x;
            nb += y * y;
        }
    }
    for (va, vb) in rem_a.iter().zip(rem_b.iter()) {
        dot += va * vb;
        na += va * va;
        nb += vb * vb;
    }
    let norm = (na * nb).sqrt();
    if norm == 0.0 {
        1.0
    } else {
        (1.0 - (dot / norm)).max(0.0)
    }
}

/// Computes vector distance according to selected metric.
#[inline]
pub fn compute_distance(a: &[f32], b: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::L2 => l2_distance_sq(a, b).sqrt(),
        VectorMetric::IP => -dot_product(a, b),
        VectorMetric::Cosine => cosine_distance(a, b),
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
            if x < min_val {
                min_val = x;
            }
            if x > max_val {
                max_val = x;
            }
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
        Self {
            min_val,
            scale,
            sum_q,
            sum_q_sq,
            data,
        }
    }

    /// Dequantizes back to full-precision float vector.
    pub fn dequantize(&self) -> Vec<f32> {
        self.data
            .iter()
            .map(|&q| self.min_val + (q as f32) * self.scale)
            .collect()
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
                let norm_b_sq = (dim * min_val * min_val
                    + 2.0 * min_val * scale * self.sum_q
                    + scale * scale * self.sum_q_sq)
                    .max(0.0);
                let norm_a_sq = dot_product(query, query);

                let denom = norm_a_sq.sqrt() * norm_b_sq.sqrt();
                if denom == 0.0 {
                    1.0
                } else {
                    (1.0 - (dot / denom)).max(0.0)
                }
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
                    for (i, val) in centroid.iter_mut().enumerate() {
                        let mut h = (c as u64).wrapping_mul(0x9E3779B97F4A7C15)
                            ^ ((sub + 1) as u64).wrapping_mul(0xC6A4A7935BD1E995)
                            ^ ((i + 1) as u64).wrapping_mul(0x517CC1B727220A95);
                        h = (h ^ (h >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
                        h = (h ^ (h >> 27)).wrapping_mul(0x94D049BB133111EB);
                        h ^= h >> 31;
                        *val = ((h & 0xFFFF) as f32 / 32767.5) - 1.0;
                    }
                }
                centroids.push(centroid);
            }
            codebooks.push(centroids);
        }
        Self {
            dim,
            m,
            d_sub,
            codebooks,
        }
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
        for (m, row) in table.iter_mut().enumerate() {
            let start = m * self.d_sub;
            let end = (start + self.d_sub).min(query.len());
            let sub_q = &query[start..end];
            for (c, cell) in row.iter_mut().enumerate() {
                let centroid = &self.codebooks[m][c];
                let mut sq = 0.0f32;
                for (i, &q) in sub_q.iter().enumerate() {
                    let diff = q - centroid[i];
                    sq += diff * diff;
                }
                *cell = sq;
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

    /// Reconstructs an approximate `dim`-dimensional vector from its PQ codes.
    pub fn reconstruct(&self, pq: &PQVector) -> Vec<f32> {
        let mut out = Vec::with_capacity(self.dim);
        for (m, &code) in pq.codes.iter().enumerate() {
            if let Some(cb) = self.codebooks.get(m) {
                out.extend_from_slice(&cb[code as usize]);
            }
        }
        out.truncate(self.dim);
        out
    }

    /// Mean squared reconstruction error of `samples` under this quantizer's codebooks.
    pub fn distortion(&self, samples: &[Vec<f32>]) -> f32 {
        if samples.is_empty() {
            return 0.0;
        }
        let mut total = 0.0f32;
        for s in samples {
            let code = self.encode(s);
            let rec = self.reconstruct(&code);
            for (a, b) in s.iter().zip(rec.iter()) {
                let d = a - b;
                total += d * d;
            }
        }
        total / (samples.len() as f32)
    }

    /// Trains per-subspace codebooks on `samples` using furthest-first (deterministic k-means++)
    /// initialization followed by up to `max_iters` Lloyd k-means iterations.
    pub fn train(dim: usize, m: usize, samples: &[Vec<f32>], max_iters: usize) -> Self {
        let mut pq = Self::new(dim, m);
        if samples.is_empty() {
            return pq;
        }
        let k_active = samples.len().clamp(1, 256);
        let iters = max_iters.max(1);

        for sub in 0..pq.m {
            let start = sub * pq.d_sub;
            let sub_vecs: Vec<&[f32]> = samples
                .iter()
                .filter_map(|v| {
                    let end = (start + pq.d_sub).min(v.len());
                    (end - start == pq.d_sub).then_some(&v[start..end])
                })
                .collect();
            if sub_vecs.is_empty() {
                continue;
            }

            // 1. Furthest-first k-means++ seeding for the first `k_active` centroids
            pq.codebooks[sub][0].copy_from_slice(sub_vecs[0]);
            let mut min_sq_dist: Vec<f32> = sub_vecs
                .iter()
                .map(|v| {
                    v.iter()
                        .zip(pq.codebooks[sub][0].iter())
                        .map(|(a, b)| (a - b) * (a - b))
                        .sum()
                })
                .collect();

            let mut actual_k = 1usize;
            for c in 1..k_active {
                let (best_idx, &best_d) = min_sq_dist
                    .iter()
                    .enumerate()
                    .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(Ordering::Equal))
                    .unwrap();
                if best_d <= 1e-12 {
                    break;
                }
                pq.codebooks[sub][c].copy_from_slice(sub_vecs[best_idx]);
                actual_k = c + 1;
                for (i, v) in sub_vecs.iter().enumerate() {
                    let d: f32 = v
                        .iter()
                        .zip(pq.codebooks[sub][c].iter())
                        .map(|(a, b)| (a - b) * (a - b))
                        .sum();
                    if d < min_sq_dist[i] {
                        min_sq_dist[i] = d;
                    }
                }
            }

            // 2. Lloyd's k-means refinement over `0..actual_k`
            let mut assignments = vec![0usize; sub_vecs.len()];
            for _ in 0..iters {
                let mut changed = false;
                for (i, v) in sub_vecs.iter().enumerate() {
                    let mut best_c = 0usize;
                    let mut best_d = f32::INFINITY;
                    for (c, centroid) in pq.codebooks[sub][..actual_k].iter().enumerate() {
                        let d: f32 = v
                            .iter()
                            .zip(centroid.iter())
                            .map(|(a, b)| (a - b) * (a - b))
                            .sum();
                        if d < best_d {
                            best_d = d;
                            best_c = c;
                        }
                    }
                    if assignments[i] != best_c {
                        assignments[i] = best_c;
                        changed = true;
                    }
                }
                if !changed {
                    break;
                }
                let mut sums = vec![vec![0.0f32; pq.d_sub]; actual_k];
                let mut counts = vec![0usize; actual_k];
                for (v, &c) in sub_vecs.iter().zip(assignments.iter()) {
                    counts[c] += 1;
                    for (acc, &x) in sums[c].iter_mut().zip(v.iter()) {
                        *acc += x;
                    }
                }
                for c in 0..actual_k {
                    if counts[c] > 0 {
                        let inv = 1.0 / (counts[c] as f32);
                        for (dst, &s) in pq.codebooks[sub][c].iter_mut().zip(sums[c].iter()) {
                            *dst = s * inv;
                        }
                    }
                }
            }
        }

        pq
    }
}

#[derive(Clone, Debug)]
pub struct HnswNode {
    pub id: usize,
    pub key: Bytes,
    pub vector: Vec<f32>,
    pub quantized: Option<QuantizedVector>,
    pub binary: Option<Vec<u64>>,
    pub pq: Option<PQVector>,
    pub is_tiered: bool,
    /// Neighbors at each layer [0..layer]
    pub neighbors: Vec<Vec<usize>>,
}

/// Packs the sign bits of `v` (`> 0.0` -> 1, else 0) into 64-bit words.
pub fn quantize_binary(v: &[f32]) -> Vec<u64> {
    let words = v.len().div_ceil(64);
    let mut out = vec![0u64; words];
    for (i, &x) in v.iter().enumerate() {
        if x > 0.0 {
            out[i / 64] |= 1u64 << (i % 64);
        }
    }
    out
}

/// Normalized Hamming distance in `[0.0, 2.0]` (approximating Cosine distance) between two
/// binary-quantized vectors of dimension `dim`.
pub fn binary_hamming_cosine_distance(a: &[u64], b: &[u64], dim: usize) -> f32 {
    if dim == 0 {
        return 0.0;
    }
    let mut diff_bits = 0u32;
    for (&wa, &wb) in a.iter().zip(b.iter()) {
        diff_bits += (wa ^ wb).count_ones();
    }
    (2.0 * diff_bits as f32) / (dim as f32)
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
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
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
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for FurthestCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Quantization mode of a Redis 8 vector set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VQuant {
    /// Full-precision `f32` storage (`NOQUANT`).
    #[default]
    NoQuant,
    /// 8-bit scalar quantization (`Q8`, the Redis 8 default for new vector sets).
    Q8,
    /// 1-bit sign quantization (`BIN`).
    Bin,
}

impl VQuant {
    /// Name reported by `VINFO quant-type`.
    pub fn as_str(&self) -> &'static str {
        match self {
            VQuant::NoQuant => "f32",
            VQuant::Q8 => "int8",
            VQuant::Bin => "bin",
        }
    }
}

static NEXT_VSET_UID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Hierarchical Navigable Small World (HNSW) Vector Index
#[derive(Debug, Clone)]
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
    pub free_ids: Vec<usize>,
    pub key_to_id: HashMap<Bytes, usize>,
    pub pq_quantizer: Option<ProductQuantizer>,
    pub pq_trained: bool,
    /// Redis 8 vector-set quantization mode (`NOQUANT` / `Q8` / `BIN`).
    pub quant: VQuant,
    /// Per-element JSON attributes (`VSETATTR` / `VADD ... SETATTR`), used by `VSIM ... FILTER`.
    pub attributes: HashMap<Bytes, String>,
    /// Row-major `dim x input_dim` random projection matrix created by `VADD ... REDUCE`.
    pub projection: Option<Vec<f32>>,
    /// Dimension of vectors supplied by clients before projection (0 when no projection).
    pub input_dim: usize,
    /// Unique id of this vector set (`VINFO vset-uid`).
    pub uid: u64,
    /// True when created via Redis 8 `VADD` syntax (`FP32` / `VALUES` / `REDUCE`).
    pub is_redis_vset: bool,
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
            free_ids: Vec::new(),
            key_to_id: HashMap::new(),
            pq_quantizer: None,
            pq_trained: false,
            quant: VQuant::NoQuant,
            attributes: HashMap::new(),
            projection: None,
            input_dim: 0,
            uid: NEXT_VSET_UID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            is_redis_vset: false,
            rng_state: 0x853c49e6748fea9b,
        }
    }

    /// Creates an HNSW index with explicit graph parameters (`M`, `EF_CONSTRUCTION`, `EF_RUNTIME`).
    pub fn with_params(
        name: String,
        dim: usize,
        metric: VectorMetric,
        m: usize,
        ef_construction: usize,
        ef_runtime: usize,
    ) -> Self {
        let mut idx = Self::new(name, dim, metric);
        let m = m.max(2);
        idx.m = m;
        idx.m0 = m * 2;
        idx.ml = 1.0 / (m as f64).ln();
        idx.ef_construction = ef_construction.max(1);
        idx.ef_search = ef_runtime.max(1);
        idx
    }

    pub fn enable_pq(&mut self, m: usize) {
        self.pq_quantizer = Some(ProductQuantizer::new(self.dim, m));
        self.pq_trained = false;
    }

    /// Trains the index's Product Quantizer codebooks using k-means++ and Lloyd's iterations
    /// over all currently indexed vectors, then re-encodes all PQ-compressed nodes.
    pub fn train_pq(&mut self, max_iters: usize) {
        let m = self
            .pq_quantizer
            .as_ref()
            .map(|q| q.m)
            .unwrap_or_else(|| (self.dim / 8).clamp(1, 16));
        let samples: Vec<Vec<f32>> = self
            .nodes
            .iter()
            .flatten()
            .map(|n| n.vector.clone())
            .collect();
        if samples.is_empty() {
            return;
        }
        let trained = ProductQuantizer::train(self.dim, m, &samples, max_iters);
        for node in self.nodes.iter_mut().flatten() {
            if node.pq.is_some() {
                node.pq = Some(trained.encode(&node.vector));
            }
        }
        self.pq_quantizer = Some(trained);
        self.pq_trained = true;
    }

    /// Installs a deterministic Gaussian random projection from `input_dim` to `self.dim`
    /// (`VADD ... REDUCE`). Entries are scaled by `1/sqrt(dim)` to roughly preserve norms.
    pub fn set_projection(&mut self, input_dim: usize) {
        let out_dim = self.dim;
        let mut state: u64 = 0x9e3779b97f4a7c15 ^ (input_dim as u64) ^ ((out_dim as u64) << 32);
        let mut next_unit = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 11) as f64 / (1u64 << 53) as f64).max(1e-12)
        };
        let scale = 1.0 / (out_dim as f64).sqrt();
        let mut m = Vec::with_capacity(out_dim * input_dim);
        for _ in 0..out_dim * input_dim {
            let (u1, u2) = (next_unit(), next_unit());
            let g = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            m.push((g * scale) as f32);
        }
        self.projection = Some(m);
        self.input_dim = input_dim;
    }

    /// Applies the `REDUCE` projection (if any) to a client-supplied vector.
    pub fn project(&self, v: &[f32]) -> Vec<f32> {
        match &self.projection {
            Some(m) if self.input_dim == v.len() => (0..self.dim)
                .map(|r| dot_product(&m[r * self.input_dim..(r + 1) * self.input_dim], v))
                .collect(),
            _ => v.to_vec(),
        }
    }

    /// Dimension that clients must supply (`input_dim` for projected sets, otherwise `dim`).
    pub fn client_dim(&self) -> usize {
        if self.projection.is_some() {
            self.input_dim
        } else {
            self.dim
        }
    }

    /// Vector as stored for `key`, dequantized when the set uses `Q8` or `BIN`.
    pub fn stored_vector(&self, key: &Bytes) -> Option<Vec<f32>> {
        let id = *self.key_to_id.get(key)?;
        let node = self.nodes.get(id)?.as_ref()?;
        Some(match self.quant {
            VQuant::Q8 => node
                .quantized
                .as_ref()
                .map(|q| q.dequantize())
                .unwrap_or_else(|| node.vector.clone()),
            VQuant::Bin => {
                if let Some(bits) = &node.binary {
                    (0..self.dim)
                        .map(|i| {
                            if (bits[i / 64] >> (i % 64)) & 1 == 1 {
                                1.0
                            } else {
                                -1.0
                            }
                        })
                        .collect()
                } else {
                    node.vector.clone()
                }
            }
            VQuant::NoQuant => node.vector.clone(),
        })
    }

    /// Neighbor lists of `key` from layer 0 up to its highest layer (`VLINKS`).
    pub fn links(&self, key: &Bytes) -> Option<Vec<Vec<(Bytes, f32)>>> {
        let id = *self.key_to_id.get(key)?;
        let node = self.nodes.get(id)?.as_ref()?;
        Some(
            node.neighbors
                .iter()
                .map(|layer| {
                    layer
                        .iter()
                        .filter_map(|&n| self.nodes.get(n).and_then(|o| o.as_ref()))
                        .map(|n| {
                            let d = compute_distance(&node.vector, &n.vector, self.metric);
                            let score = (1.0 - d / 2.0).clamp(0.0, 1.0);
                            (n.key.clone(), score)
                        })
                        .collect()
                })
                .collect(),
        )
    }

    /// Highest node id ever allocated (`VINFO hnsw-max-node-uid`).
    pub fn max_node_uid(&self) -> usize {
        self.nodes.len()
    }

    /// Exact brute-force top-k (`VSIM ... TRUTH`), optionally restricted by `filter`.
    pub fn search_exact(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&dyn Fn(&Bytes) -> bool>,
    ) -> Vec<(Bytes, f32)> {
        let mut all: Vec<(Bytes, f32)> = self
            .nodes
            .iter()
            .flatten()
            .filter(|n| filter.is_none_or(|f| f(&n.key)))
            .map(|n| (n.key.clone(), self.dist_to_node(query, n)))
            .collect();
        all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        all.truncate(k);
        all
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
        self.key_to_id.get(key).and_then(|&id| {
            self.nodes
                .get(id)
                .and_then(|n| n.as_ref().map(|n| n.vector.as_slice()))
        })
    }

    #[inline]
    pub fn dist_to_node(&self, query: &[f32], node: &HnswNode) -> f32 {
        if let Some(pq) = &node.pq
            && let Some(quantizer) = &self.pq_quantizer
        {
            return quantizer.compute_distance_with_vec(query, pq);
        }
        if let Some(bin) = &node.binary {
            let q_bin = quantize_binary(query);
            return binary_hamming_cosine_distance(&q_bin, bin, self.dim);
        }
        if let Some(quant) = &node.quantized {
            quant.compute_distance(query, self.metric)
        } else {
            compute_distance(query, &node.vector, self.metric)
        }
    }

    /// Malkov & Yashunin Algorithm 4 (`SELECT-NEIGHBORS-HEURISTIC`, `keepPrunedConnections = true`).
    /// Prefers candidates that are closer to the base point than to any already-selected neighbor
    /// so edges fan out across diverse directions around clusters, then backfills up to `m_max`
    /// from pruned candidates to preserve graph degree.
    fn select_neighbors_heuristic(&self, candidates: &[Candidate], m_max: usize) -> Vec<usize> {
        if candidates.len() <= m_max {
            return candidates.iter().map(|c| c.id).collect();
        }
        let mut selected: Vec<usize> = Vec::with_capacity(m_max);
        let mut pruned: Vec<usize> = Vec::new();
        for cand in candidates {
            if selected.len() >= m_max {
                break;
            }
            let Some(Some(cand_node)) = self.nodes.get(cand.id) else {
                continue;
            };
            let is_diverse = selected.iter().all(|&sel_id| {
                self.nodes
                    .get(sel_id)
                    .and_then(|o| o.as_ref())
                    .is_none_or(|sel_node| {
                        let dist_to_sel = self.dist_to_node(&sel_node.vector, cand_node);
                        cand.distance <= dist_to_sel
                    })
            });
            if is_diverse {
                selected.push(cand.id);
            } else {
                pruned.push(cand.id);
            }
        }
        for id in pruned {
            if selected.len() >= m_max {
                break;
            }
            selected.push(id);
        }
        selected
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
        let new_id = self.free_ids.pop().unwrap_or(self.nodes.len());

        let use_q8 = quantize_sq8 || self.quant == VQuant::Q8 || (tiered && !quantize_pq);
        let quantized = if use_q8 {
            Some(QuantizedVector::quantize(&vector))
        } else {
            None
        };
        let binary = if self.quant == VQuant::Bin {
            Some(quantize_binary(&vector))
        } else {
            None
        };

        let pq = if quantize_pq {
            if self.pq_quantizer.is_none() {
                let m = (self.dim / 8).clamp(1, 16);
                self.pq_quantizer = Some(ProductQuantizer::new(self.dim, m));
            }
            if !self.pq_trained && self.len() >= 15 {
                let m = self
                    .pq_quantizer
                    .as_ref()
                    .map(|q| q.m)
                    .unwrap_or_else(|| (self.dim / 8).clamp(1, 16));
                let mut samples: Vec<Vec<f32>> = self
                    .nodes
                    .iter()
                    .flatten()
                    .map(|n| n.vector.clone())
                    .collect();
                samples.push(vector.clone());
                let trained = ProductQuantizer::train(self.dim, m, &samples, 10);
                for node in self.nodes.iter_mut().flatten() {
                    if node.pq.is_some() {
                        node.pq = Some(trained.encode(&node.vector));
                    }
                }
                self.pq_quantizer = Some(trained);
                self.pq_trained = true;
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
            binary,
            pq,
            is_tiered: tiered,
            neighbors: vec![Vec::new(); target_level + 1],
        };

        if self.entry_point.is_none() {
            self.entry_point = Some(new_id);
            self.max_layer = target_level;
            if new_id < self.nodes.len() {
                self.nodes[new_id] = Some(node);
            } else {
                self.nodes.push(Some(node));
            }
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
                if let Some(curr_node) = &self.nodes[curr_obj]
                    && lc < curr_node.neighbors.len()
                {
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

        // 2. Search and link at layers min(target_level, max_layer) down to 0
        if new_id < self.nodes.len() {
            self.nodes[new_id] = Some(node);
        } else {
            self.nodes.push(Some(node));
        }
        self.key_to_id.insert(key, new_id);

        let search_level_max = target_level.min(self.max_layer);
        for lc in (0..=search_level_max).rev() {
            let candidates = self.search_layer(&vector, curr_obj, self.ef_construction, lc);
            let m_max = if lc == 0 { self.m0 } else { self.m };
            let neighbors = self.select_neighbors_heuristic(&candidates, m_max);

            // Connect new node to neighbors
            if let Some(n) = &mut self.nodes[new_id] {
                n.neighbors[lc] = neighbors.clone();
            }

            // Connect neighbors back to new node
            for &nbr_id in &neighbors {
                if let Some(nbr) = &mut self.nodes[nbr_id]
                    && lc < nbr.neighbors.len()
                {
                    if !nbr.neighbors[lc].contains(&new_id) {
                        nbr.neighbors[lc].push(new_id);
                    }
                    if nbr.neighbors[lc].len() > m_max {
                        // Prune furthest / redundant neighbor via diversity heuristic
                        self.prune_neighbors(nbr_id, lc, m_max);
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
            let mut candidates: Vec<Candidate> = node.neighbors[layer]
                .iter()
                .filter_map(|&id| {
                    self.nodes
                        .get(id)
                        .and_then(|opt| opt.as_ref())
                        .map(|n| Candidate {
                            id,
                            distance: self.dist_to_node(&node_vec, n),
                        })
                })
                .collect();
            candidates.sort_by(|a, b| {
                a.distance
                    .partial_cmp(&b.distance)
                    .unwrap_or(Ordering::Equal)
            });
            let new_nbrs = self.select_neighbors_heuristic(&candidates, max_neighbors);
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
            if let Some(furthest) = w.peek()
                && curr.distance > furthest.distance
                && w.len() >= ef
            {
                break;
            }

            if let Some(node) = &self.nodes[curr.id]
                && layer < node.neighbors.len()
            {
                for &nbr_id in &node.neighbors[layer] {
                    if visited.insert(nbr_id)
                        && let Some(nbr_node) = &self.nodes[nbr_id]
                    {
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

        let mut results: Vec<Candidate> = w
            .into_iter()
            .map(|f| Candidate {
                id: f.id,
                distance: f.distance,
            })
            .collect();
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });
        results
    }

    /// Layer-0 beam search where only nodes accepted by `filter` enter the result set; rejected
    /// nodes are still traversed so the graph stays navigable under selective filters.
    fn search_layer_filtered(
        &self,
        query: &[f32],
        entry_point: usize,
        ef: usize,
        filter: &dyn Fn(&Bytes) -> bool,
    ) -> Vec<Candidate> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut w: BinaryHeap<FurthestCandidate> = BinaryHeap::new();

        let entry = self.nodes[entry_point].as_ref().unwrap();
        let initial_dist = self.dist_to_node(query, entry);
        visited.insert(entry_point);
        candidates.push(Candidate {
            id: entry_point,
            distance: initial_dist,
        });
        if filter(&entry.key) {
            w.push(FurthestCandidate {
                id: entry_point,
                distance: initial_dist,
            });
        }

        while let Some(curr) = candidates.pop() {
            if w.len() >= ef
                && let Some(furthest) = w.peek()
                && curr.distance > furthest.distance
            {
                break;
            }
            if let Some(node) = &self.nodes[curr.id]
                && !node.neighbors.is_empty()
            {
                for &nbr_id in &node.neighbors[0] {
                    if visited.insert(nbr_id)
                        && let Some(nbr_node) = &self.nodes[nbr_id]
                    {
                        let d = self.dist_to_node(query, nbr_node);
                        let furthest_dist = w.peek().map(|f| f.distance).unwrap_or(f32::MAX);
                        if w.len() < ef || d < furthest_dist {
                            candidates.push(Candidate {
                                id: nbr_id,
                                distance: d,
                            });
                            if filter(&nbr_node.key) {
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

        let mut results: Vec<Candidate> = w
            .into_iter()
            .map(|f| Candidate {
                id: f.id,
                distance: f.distance,
            })
            .collect();
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });
        results
    }

    /// Searches for top-k nearest neighbors.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(Bytes, f32)> {
        self.search_tiered(query, k, false)
    }

    /// Searches for top-k nearest neighbors with optional exact reranking.
    pub fn search_tiered(&self, query: &[f32], k: usize, rerank: bool) -> Vec<(Bytes, f32)> {
        self.search_ext(query, k, None, rerank)
    }

    /// Searches for top-k nearest neighbors with an optional per-query `EF_RUNTIME` override.
    pub fn search_ext(
        &self,
        query: &[f32],
        k: usize,
        ef_runtime: Option<usize>,
        rerank: bool,
    ) -> Vec<(Bytes, f32)> {
        self.search_filtered(query, k, ef_runtime, rerank, None)
    }

    /// Top-k search restricted to keys accepted by `filter` (in-graph pre-filtering).
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_runtime: Option<usize>,
        rerank: bool,
        filter: Option<&dyn Fn(&Bytes) -> bool>,
    ) -> Vec<(Bytes, f32)> {
        if self.entry_point.is_none() || self.is_empty() || k == 0 {
            return Vec::new();
        }
        let base_ef = ef_runtime.unwrap_or(self.ef_search).max(1);

        let mut curr_obj = self.entry_point.unwrap();
        let mut curr_dist = self.dist_to_node(query, self.nodes[curr_obj].as_ref().unwrap());

        // 1. Greedy search down to layer 1
        for lc in (1..=self.max_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(curr_node) = &self.nodes[curr_obj]
                    && lc < curr_node.neighbors.len()
                {
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

        // 2. Layer 0 search with ef_search
        let search_ef = if rerank {
            base_ef.max(k * 3)
        } else {
            base_ef.max(k)
        };
        let candidates = match filter {
            Some(f) => self.search_layer_filtered(query, curr_obj, search_ef, f),
            None => self.search_layer(query, curr_obj, search_ef, 0),
        };

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

    /// Removes a key from the index, repairing local neighbor links and recycling the slot id.
    pub fn remove(&mut self, key: &Bytes) -> bool {
        if let Some(id) = self.key_to_id.remove(key) {
            let nbrs_by_layer = self.nodes[id].as_ref().map(|n| n.neighbors.clone());
            self.nodes[id] = None;
            self.free_ids.push(id);
            if let Some(nbrs_by_layer) = nbrs_by_layer {
                for (layer, nbrs) in nbrs_by_layer.into_iter().enumerate() {
                    let m_max = if layer == 0 { self.m0 } else { self.m };
                    for &nbr_id in &nbrs {
                        if let Some(nbr_node) = &mut self.nodes[nbr_id]
                            && layer < nbr_node.neighbors.len()
                        {
                            nbr_node.neighbors[layer].retain(|&x| x != id);
                        }
                    }
                    // Reconnect former neighbors of the deleted node so graph connectivity holds
                    for &u in &nbrs {
                        let Some(Some(u_node)) = self.nodes.get(u) else {
                            continue;
                        };
                        if layer >= u_node.neighbors.len() || u_node.neighbors[layer].len() >= m_max
                        {
                            continue;
                        }
                        let u_vec = u_node.vector.clone();
                        let mut extra: Vec<(usize, f32)> = nbrs
                            .iter()
                            .copied()
                            .filter(|&v| v != u && !u_node.neighbors[layer].contains(&v))
                            .filter_map(|v| {
                                self.nodes
                                    .get(v)
                                    .and_then(|o| o.as_ref())
                                    .map(|vn| (v, self.dist_to_node(&u_vec, vn)))
                            })
                            .collect();
                        extra.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
                        if let Some(Some(u_mut)) = self.nodes.get_mut(u) {
                            for (v, _) in extra {
                                if u_mut.neighbors[layer].len() >= m_max {
                                    break;
                                }
                                u_mut.neighbors[layer].push(v);
                            }
                        }
                    }
                }
            }
            if self.entry_point == Some(id) || self.key_to_id.is_empty() {
                let best = self
                    .nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, opt)| {
                        opt.as_ref()
                            .map(|n| (idx, n.neighbors.len().saturating_sub(1)))
                    })
                    .max_by_key(|&(_, top)| top);
                self.entry_point = best.map(|(idx, _)| idx);
                self.max_layer = best.map(|(_, top)| top).unwrap_or(0);
            }
            true
        } else {
            false
        }
    }

    /// Returns the raw internal representation for `VEMB key element RAW`:
    /// `(quant_type, raw_blob, norm, q8_range)`.
    pub fn raw_embedding(&self, key: &Bytes) -> Option<(&'static str, Vec<u8>, f32, Option<f32>)> {
        let id = *self.key_to_id.get(key)?;
        let node = self.nodes.get(id)?.as_ref()?;
        let norm = dot_product(&node.vector, &node.vector).sqrt().max(1e-12);
        match self.quant {
            VQuant::Q8 => {
                let q = node
                    .quantized
                    .clone()
                    .unwrap_or_else(|| QuantizedVector::quantize(&node.vector));
                let range = (q.scale * 255.0).abs();
                Some(("q8", q.data, norm, Some(range)))
            }
            VQuant::Bin => {
                let words = node
                    .binary
                    .clone()
                    .unwrap_or_else(|| quantize_binary(&node.vector));
                let byte_len = self.dim.div_ceil(8);
                let mut raw = Vec::with_capacity(byte_len);
                for word in words {
                    for b in word.to_le_bytes() {
                        if raw.len() < byte_len {
                            raw.push(b);
                        }
                    }
                }
                Some(("bin", raw, norm, None))
            }
            VQuant::NoQuant => {
                let mut raw = Vec::with_capacity(node.vector.len() * 4);
                for &v in &node.vector {
                    raw.extend_from_slice(&(v / norm).to_le_bytes());
                }
                Some(("fp32", raw, norm, None))
            }
        }
    }

    /// Samples element keys for `VRANDMEMBER key [count]`.
    pub fn random_members(&mut self, count: i64) -> Vec<Bytes> {
        if self.key_to_id.is_empty() || count == 0 {
            return Vec::new();
        }
        let mut keys: Vec<Bytes> = self.key_to_id.keys().cloned().collect();
        // Sort first for deterministic base ordering before PRNG sampling.
        keys.sort();
        if count > 0 {
            let n = (count as usize).min(keys.len());
            // Partial Fisher-Yates shuffle.
            for i in 0..n {
                let rem = keys.len() - i;
                let r = (self.next_random_f64() * (rem as f64)) as usize;
                let j = i + r.min(rem - 1);
                keys.swap(i, j);
            }
            keys.truncate(n);
            keys
        } else {
            let n = count.unsigned_abs() as usize;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let r = (self.next_random_f64() * (keys.len() as f64)) as usize;
                out.push(keys[r.min(keys.len() - 1)].clone());
            }
            out
        }
    }
}

/// Value produced during Redis 8 `VSIM ... FILTER` expression evaluation.
#[derive(Debug, Clone, PartialEq)]
enum FilterVal {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    List(Vec<FilterVal>),
}

impl FilterVal {
    fn is_truthy(&self) -> bool {
        match self {
            FilterVal::Null => false,
            FilterVal::Bool(b) => *b,
            FilterVal::Num(n) => *n != 0.0 && !n.is_nan(),
            FilterVal::Str(s) => !s.is_empty(),
            FilterVal::List(l) => !l.is_empty(),
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            FilterVal::Num(n) => Some(*n),
            FilterVal::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    fn from_json(v: &serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => FilterVal::Null,
            serde_json::Value::Bool(b) => FilterVal::Bool(*b),
            serde_json::Value::Number(n) => FilterVal::Num(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::String(s) => FilterVal::Str(s.clone()),
            serde_json::Value::Array(arr) => {
                FilterVal::List(arr.iter().map(Self::from_json).collect())
            }
            serde_json::Value::Object(_) => FilterVal::Null,
        }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (FilterVal::Num(a), FilterVal::Num(b)) => (a - b).abs() < 1e-9,
            (FilterVal::Bool(a), FilterVal::Bool(b)) => a == b,
            (FilterVal::Num(a), FilterVal::Bool(b)) | (FilterVal::Bool(b), FilterVal::Num(a)) => {
                (*a != 0.0) == *b
            }
            (FilterVal::Str(a), FilterVal::Str(b)) => a == b,
            (FilterVal::Null, FilterVal::Null) => true,
            (FilterVal::List(a), FilterVal::List(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.equals(y))
            }
            _ => false,
        }
    }

    fn cmp_ord(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (FilterVal::Num(a), FilterVal::Num(b)) => a.partial_cmp(b),
            (FilterVal::Str(a), FilterVal::Str(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

struct FilterParser<'a> {
    src: &'a [u8],
    pos: usize,
    root: Option<&'a serde_json::Value>,
}

impl<'a> FilterParser<'a> {
    fn new(expr: &'a str, root: Option<&'a serde_json::Value>) -> Self {
        Self {
            src: expr.as_bytes(),
            pos: 0,
            root,
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.src.len() && self.src[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.src.get(self.pos).copied()
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        self.skip_ws();
        let bytes = kw.as_bytes();
        let end = self.pos + bytes.len();
        if end <= self.src.len() && self.src[self.pos..end].eq_ignore_ascii_case(bytes) {
            let next_ok = end == self.src.len()
                || (!self.src[end].is_ascii_alphanumeric() && self.src[end] != b'_');
            if next_ok {
                self.pos = end;
                return true;
            }
        }
        false
    }

    fn eat_op(&mut self, op: &[u8]) -> bool {
        self.skip_ws();
        if self.src[self.pos..].starts_with(op) {
            self.pos += op.len();
            true
        } else {
            false
        }
    }

    fn parse_or(&mut self) -> Result<FilterVal, String> {
        let mut left = self.parse_and()?;
        while self.eat_op(b"||") || self.eat_kw("or") {
            let right = self.parse_and()?;
            left = FilterVal::Bool(left.is_truthy() || right.is_truthy());
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<FilterVal, String> {
        let mut left = self.parse_not()?;
        while self.eat_op(b"&&") || self.eat_kw("and") {
            let right = self.parse_not()?;
            left = FilterVal::Bool(left.is_truthy() && right.is_truthy());
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<FilterVal, String> {
        self.skip_ws();
        if (self.src[self.pos..].starts_with(b"!") && !self.src[self.pos..].starts_with(b"!="))
            || self.eat_kw("not")
        {
            if self.src.get(self.pos) == Some(&b'!') {
                self.pos += 1;
            }
            let v = self.parse_not()?;
            return Ok(FilterVal::Bool(!v.is_truthy()));
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<FilterVal, String> {
        let mut left = self.parse_add()?;
        loop {
            if self.eat_op(b"==") {
                let r = self.parse_add()?;
                left = FilterVal::Bool(left.equals(&r));
            } else if self.eat_op(b"!=") {
                let r = self.parse_add()?;
                left = FilterVal::Bool(!left.equals(&r));
            } else if self.eat_op(b"<=") {
                let r = self.parse_add()?;
                left = FilterVal::Bool(
                    left.cmp_ord(&r)
                        .is_some_and(|o| o == Ordering::Less || o == Ordering::Equal),
                );
            } else if self.eat_op(b">=") {
                let r = self.parse_add()?;
                left = FilterVal::Bool(
                    left.cmp_ord(&r)
                        .is_some_and(|o| o == Ordering::Greater || o == Ordering::Equal),
                );
            } else if self.eat_op(b"<") {
                let r = self.parse_add()?;
                left = FilterVal::Bool(left.cmp_ord(&r) == Some(Ordering::Less));
            } else if self.eat_op(b">") {
                let r = self.parse_add()?;
                left = FilterVal::Bool(left.cmp_ord(&r) == Some(Ordering::Greater));
            } else if self.eat_kw("in") {
                let r = self.parse_add()?;
                let matched = match (&left, &r) {
                    (needle, FilterVal::List(items)) => items.iter().any(|it| it.equals(needle)),
                    (FilterVal::Str(sub), FilterVal::Str(hay)) => hay.contains(sub.as_str()),
                    _ => false,
                };
                left = FilterVal::Bool(matched);
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_add(&mut self) -> Result<FilterVal, String> {
        let mut left = self.parse_mul()?;
        loop {
            if self.eat_op(b"+") {
                let r = self.parse_mul()?;
                left = match (left.as_f64(), r.as_f64()) {
                    (Some(a), Some(b)) => FilterVal::Num(a + b),
                    _ => FilterVal::Null,
                };
            } else if self.eat_op(b"-") {
                let r = self.parse_mul()?;
                left = match (left.as_f64(), r.as_f64()) {
                    (Some(a), Some(b)) => FilterVal::Num(a - b),
                    _ => FilterVal::Null,
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<FilterVal, String> {
        let mut left = self.parse_pow()?;
        loop {
            if self.eat_op(b"*") {
                let r = self.parse_pow()?;
                left = match (left.as_f64(), r.as_f64()) {
                    (Some(a), Some(b)) => FilterVal::Num(a * b),
                    _ => FilterVal::Null,
                };
            } else if self.eat_op(b"/") {
                let r = self.parse_pow()?;
                left = match (left.as_f64(), r.as_f64()) {
                    (Some(a), Some(b)) if b != 0.0 => FilterVal::Num(a / b),
                    _ => FilterVal::Null,
                };
            } else if self.eat_op(b"%") {
                let r = self.parse_pow()?;
                left = match (left.as_f64(), r.as_f64()) {
                    (Some(a), Some(b)) if b != 0.0 => FilterVal::Num(a % b),
                    _ => FilterVal::Null,
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_pow(&mut self) -> Result<FilterVal, String> {
        let left = self.parse_unary()?;
        if self.eat_op(b"^") || self.eat_op(b"**") {
            let r = self.parse_pow()?;
            return Ok(match (left.as_f64(), r.as_f64()) {
                (Some(a), Some(b)) => FilterVal::Num(a.powf(b)),
                _ => FilterVal::Null,
            });
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<FilterVal, String> {
        if self.eat_op(b"-") {
            let v = self.parse_unary()?;
            return Ok(v
                .as_f64()
                .map(|n| FilterVal::Num(-n))
                .unwrap_or(FilterVal::Null));
        }
        if self.eat_op(b"+") {
            let v = self.parse_unary()?;
            return Ok(v.as_f64().map(FilterVal::Num).unwrap_or(FilterVal::Null));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<FilterVal, String> {
        let Some(ch) = self.peek() else {
            return Err("unexpected end of filter expression".to_string());
        };
        if ch == b'(' {
            self.pos += 1;
            let first = self.parse_or()?;
            if self.eat_op(b",") {
                let mut items = vec![first];
                while self.peek() != Some(b')') {
                    items.push(self.parse_or()?);
                    if !self.eat_op(b",") {
                        break;
                    }
                }
                if !self.eat_op(b")") {
                    return Err("unclosed tuple in filter expression".to_string());
                }
                return Ok(FilterVal::List(items));
            }
            if !self.eat_op(b")") {
                return Err("unclosed '(' in filter expression".to_string());
            }
            return Ok(first);
        }
        if ch == b'[' {
            self.pos += 1;
            let mut items = Vec::new();
            while self.peek() != Some(b']') {
                items.push(self.parse_or()?);
                if !self.eat_op(b",") {
                    break;
                }
            }
            if !self.eat_op(b"]") {
                return Err("unclosed '[' in filter expression".to_string());
            }
            return Ok(FilterVal::List(items));
        }
        if ch == b'"' || ch == b'\'' {
            let quote = ch;
            self.pos += 1;
            let mut s = String::new();
            while self.pos < self.src.len() {
                let b = self.src[self.pos];
                self.pos += 1;
                if b == quote {
                    return Ok(FilterVal::Str(s));
                }
                if b == b'\\' && self.pos < self.src.len() {
                    let esc = self.src[self.pos];
                    self.pos += 1;
                    s.push(esc as char);
                } else {
                    s.push(b as char);
                }
            }
            return Err("unterminated string literal in filter expression".to_string());
        }
        if ch == b'.' {
            let mut cur = self.root;
            while self.peek() == Some(b'.') {
                self.pos += 1;
                let start = self.pos;
                while self.pos < self.src.len()
                    && (self.src[self.pos].is_ascii_alphanumeric()
                        || self.src[self.pos] == b'_'
                        || self.src[self.pos] == b'-')
                {
                    self.pos += 1;
                }
                if start == self.pos {
                    return Err("empty field selector in filter expression".to_string());
                }
                let field = std::str::from_utf8(&self.src[start..self.pos])
                    .map_err(|_| "invalid UTF-8 in field selector".to_string())?;
                cur = cur.and_then(|v| v.get(field));
            }
            return Ok(cur.map(FilterVal::from_json).unwrap_or(FilterVal::Null));
        }
        if ch.is_ascii_digit() {
            let start = self.pos;
            while self.pos < self.src.len()
                && (self.src[self.pos].is_ascii_digit()
                    || self.src[self.pos] == b'.'
                    || self.src[self.pos] == b'e'
                    || self.src[self.pos] == b'E'
                    || ((self.src[self.pos] == b'+' || self.src[self.pos] == b'-')
                        && (self.src[self.pos - 1] == b'e' || self.src[self.pos - 1] == b'E')))
            {
                self.pos += 1;
            }
            let s = std::str::from_utf8(&self.src[start..self.pos])
                .map_err(|_| "invalid number".to_string())?;
            let n: f64 = s.parse().map_err(|_| "invalid number".to_string())?;
            return Ok(FilterVal::Num(n));
        }
        if self.eat_kw("true") {
            return Ok(FilterVal::Bool(true));
        }
        if self.eat_kw("false") {
            return Ok(FilterVal::Bool(false));
        }
        if self.eat_kw("null") || self.eat_kw("nil") {
            return Ok(FilterVal::Null);
        }
        Err(format!(
            "unexpected token at byte {} in filter expression",
            self.pos
        ))
    }
}

/// Validates that `expr` is syntactically valid for `VSIM ... FILTER`.
pub fn validate_vset_filter(expr: &str) -> Result<(), String> {
    let mut parser = FilterParser::new(expr, None);
    let _ = parser.parse_or()?;
    parser.skip_ws();
    if parser.pos != parser.src.len() {
        return Err("unexpected trailing tokens in filter expression".to_string());
    }
    Ok(())
}

/// Evaluates a Redis 8 `VSIM ... FILTER` expression against an element's optional JSON attributes.
pub fn eval_vset_filter(expr: &str, json_str: Option<&str>) -> Result<bool, String> {
    let Some(raw) = json_str.filter(|s| !s.trim().is_empty()) else {
        return Ok(false);
    };
    let Ok(val) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Ok(false);
    };
    let mut parser = FilterParser::new(expr, Some(&val));
    let res = parser.parse_or()?;
    parser.skip_ws();
    if parser.pos != parser.src.len() {
        return Err("unexpected trailing tokens in filter expression".to_string());
    }
    Ok(res.is_truthy())
}

/// Exact brute-force (`FLAT`) vector index with contiguous storage.
#[derive(Debug, Clone)]
pub struct FlatIndex {
    pub name: String,
    pub dim: usize,
    pub metric: VectorMetric,
    keys: Vec<Bytes>,
    data: Vec<f32>,
    key_to_pos: HashMap<Bytes, usize>,
}

impl FlatIndex {
    pub fn new(name: String, dim: usize, metric: VectorMetric, initial_cap: usize) -> Self {
        let cap = initial_cap.min(1 << 20);
        Self {
            name,
            dim,
            metric,
            keys: Vec::with_capacity(cap),
            data: Vec::with_capacity(cap.saturating_mul(dim)),
            key_to_pos: HashMap::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[inline]
    fn row(&self, pos: usize) -> &[f32] {
        &self.data[pos * self.dim..(pos + 1) * self.dim]
    }

    pub fn get_vector(&self, key: &Bytes) -> Option<&[f32]> {
        self.key_to_pos.get(key).map(|&p| self.row(p))
    }

    pub fn add(&mut self, key: Bytes, vector: Vec<f32>) -> Result<(), &'static str> {
        if vector.len() != self.dim {
            return Err("vector dimension mismatch");
        }
        if let Some(&pos) = self.key_to_pos.get(&key) {
            self.data[pos * self.dim..(pos + 1) * self.dim].copy_from_slice(&vector);
            return Ok(());
        }
        self.key_to_pos.insert(key.clone(), self.keys.len());
        self.keys.push(key);
        self.data.extend_from_slice(&vector);
        Ok(())
    }

    pub fn remove(&mut self, key: &Bytes) -> bool {
        let Some(pos) = self.key_to_pos.remove(key) else {
            return false;
        };
        let last = self.keys.len() - 1;
        if pos != last {
            let (head, tail) = self.data.split_at_mut(last * self.dim);
            head[pos * self.dim..(pos + 1) * self.dim].copy_from_slice(&tail[..self.dim]);
            self.keys.swap(pos, last);
            self.key_to_pos.insert(self.keys[pos].clone(), pos);
        }
        self.keys.pop();
        self.data.truncate(last * self.dim);
        true
    }

    /// Exact top-k search, optionally restricted by a key predicate.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&dyn Fn(&Bytes) -> bool>,
    ) -> Vec<(Bytes, f32)> {
        if k == 0 || query.len() != self.dim {
            return Vec::new();
        }
        let mut heap: BinaryHeap<FurthestCandidate> = BinaryHeap::with_capacity(k + 1);
        for (pos, key) in self.keys.iter().enumerate() {
            if let Some(f) = filter
                && !f(key)
            {
                continue;
            }
            let d = compute_distance(query, self.row(pos), self.metric);
            if heap.len() < k {
                heap.push(FurthestCandidate {
                    id: pos,
                    distance: d,
                });
            } else if let Some(top) = heap.peek()
                && d < top.distance
            {
                heap.pop();
                heap.push(FurthestCandidate {
                    id: pos,
                    distance: d,
                });
            }
        }
        let mut out: Vec<(Bytes, f32)> = heap
            .into_iter()
            .map(|c| (self.keys[c.id].clone(), c.distance))
            .collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        out
    }

    /// Exact range search returning all vectors within `radius`, sorted by distance.
    pub fn range_filtered(
        &self,
        query: &[f32],
        radius: f32,
        filter: Option<&dyn Fn(&Bytes) -> bool>,
    ) -> Vec<(Bytes, f32)> {
        if query.len() != self.dim {
            return Vec::new();
        }
        let mut out = Vec::new();
        for (pos, key) in self.keys.iter().enumerate() {
            if let Some(f) = filter
                && !f(key)
            {
                continue;
            }
            let d = compute_distance(query, self.row(pos), self.metric);
            if d <= radius {
                out.push((key.clone(), d));
            }
        }
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        out
    }

    pub fn memory_usage(&self) -> usize {
        self.data.len() * 4 + self.keys.iter().map(|k| k.len() + 32).sum::<usize>()
    }
}

/// Vector index backing a `VECTOR` field of an FT index (`FLAT` or `HNSW`).
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum VectorFieldIndex {
    Flat(FlatIndex),
    Hnsw(HnswIndex),
}

impl VectorFieldIndex {
    pub fn metric(&self) -> VectorMetric {
        match self {
            Self::Flat(f) => f.metric,
            Self::Hnsw(h) => h.metric,
        }
    }

    pub fn dim(&self) -> usize {
        match self {
            Self::Flat(f) => f.dim,
            Self::Hnsw(h) => h.dim,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Flat(f) => f.len(),
            Self::Hnsw(h) => h.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn algorithm(&self) -> &'static str {
        match self {
            Self::Flat(_) => "FLAT",
            Self::Hnsw(_) => "HNSW",
        }
    }

    pub fn add(&mut self, key: Bytes, vector: Vec<f32>) -> Result<(), &'static str> {
        match self {
            Self::Flat(f) => f.add(key, vector),
            Self::Hnsw(h) => h.add(key, vector),
        }
    }

    pub fn remove(&mut self, key: &Bytes) -> bool {
        match self {
            Self::Flat(f) => f.remove(key),
            Self::Hnsw(h) => h.remove(key),
        }
    }

    pub fn get_vector(&self, key: &Bytes) -> Option<&[f32]> {
        match self {
            Self::Flat(f) => f.get_vector(key),
            Self::Hnsw(h) => h.get_vector(key),
        }
    }

    /// Top-k search with optional `EF_RUNTIME` override.
    pub fn search(&self, query: &[f32], k: usize, ef_runtime: Option<usize>) -> Vec<(Bytes, f32)> {
        self.search_filtered(query, k, ef_runtime, None)
    }

    /// Top-k search restricted to keys accepted by `filter`.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_runtime: Option<usize>,
        filter: Option<&dyn Fn(&Bytes) -> bool>,
    ) -> Vec<(Bytes, f32)> {
        match self {
            Self::Flat(f) => f.search_filtered(query, k, filter),
            Self::Hnsw(h) => h.search_filtered(query, k, ef_runtime, false, filter),
        }
    }

    pub fn memory_usage(&self) -> usize {
        match self {
            Self::Flat(f) => f.memory_usage(),
            Self::Hnsw(h) => h
                .nodes
                .iter()
                .flatten()
                .map(|n| {
                    n.vector.len() * 4
                        + n.key.len()
                        + n.neighbors.iter().map(|l| l.len() * 8 + 24).sum::<usize>()
                        + 64
                })
                .sum(),
        }
    }
}

/// A single cached LLM prompt/response entry inside a [`SemanticCache`] namespace.
#[derive(Debug, Clone)]
pub struct SemanticEntry {
    pub id: Bytes,
    pub prompt: Bytes,
    pub response: Bytes,
    pub scope: Option<Bytes>,
    pub expire_at: Option<std::time::Instant>,
    pub tokens: u64,
}

/// Result returned on a [`SemanticCache::get`] similarity hit.
#[derive(Debug, Clone, PartialEq)]
pub struct SemanticHit {
    pub id: Bytes,
    pub prompt: Bytes,
    pub response: Bytes,
    pub score: f32,
}

/// First-class AI/LLM Semantic Cache namespace backed by an in-shard [`HnswIndex`].
#[derive(Debug, Clone)]
pub struct SemanticCache {
    pub namespace: String,
    pub index: HnswIndex,
    pub entries: HashMap<Bytes, SemanticEntry>,
    pub hits: u64,
    pub misses: u64,
    pub tokens_saved: u64,
    pub evicted_expired: u64,
}

impl SemanticCache {
    pub fn new(namespace: String, dim: usize) -> Self {
        Self {
            index: HnswIndex::new(namespace.clone(), dim.max(1), VectorMetric::Cosine),
            namespace,
            entries: HashMap::new(),
            hits: 0,
            misses: 0,
            tokens_saved: 0,
            evicted_expired: 0,
        }
    }

    /// Passively purges any expired entries from both the metadata map and the HNSW graph.
    pub fn purge_expired(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        let now = std::time::Instant::now();
        let expired_ids: Vec<Bytes> = self
            .entries
            .iter()
            .filter_map(|(id, entry)| {
                if let Some(exp) = entry.expire_at
                    && now >= exp
                {
                    return Some(id.clone());
                }
                None
            })
            .collect();
        for id in expired_ids {
            self.entries.remove(&id);
            self.index.remove(&id);
            self.evicted_expired += 1;
        }
    }

    /// Inserts or updates a semantic cache entry.
    #[allow(clippy::too_many_arguments)]
    pub fn set(
        &mut self,
        id: Bytes,
        prompt: Bytes,
        response: Bytes,
        vector: Vec<f32>,
        ttl: Option<std::time::Duration>,
        scope: Option<Bytes>,
        quantize: bool,
        tokens: Option<u64>,
    ) -> Result<(), String> {
        if vector.is_empty() {
            return Err("ERR vector dimension must be greater than 0".to_string());
        }
        self.purge_expired();
        if self.entries.is_empty() && self.index.dim != vector.len() {
            self.index = HnswIndex::new(self.namespace.clone(), vector.len(), VectorMetric::Cosine);
        }
        if vector.len() != self.index.dim {
            return Err("ERR vector dimension mismatch".to_string());
        }
        let expire_at = ttl.map(|d| std::time::Instant::now() + d);
        let est_tokens = tokens.unwrap_or_else(|| (response.len().div_ceil(4)).max(1) as u64);
        self.index
            .add_quantized(id.clone(), vector, quantize, false)
            .map_err(|e| format!("ERR {}", e))?;
        self.entries.insert(
            id.clone(),
            SemanticEntry {
                id,
                prompt,
                response,
                scope,
                expire_at,
                tokens: est_tokens,
            },
        );
        Ok(())
    }

    /// Queries the semantic cache for the closest non-expired entry matching `scope` whose
    /// cosine similarity is `>= threshold`.
    pub fn get(
        &mut self,
        query: &[f32],
        threshold: f32,
        scope: Option<&[u8]>,
    ) -> Result<Option<SemanticHit>, String> {
        self.purge_expired();
        if self.entries.is_empty() {
            self.misses += 1;
            return Ok(None);
        }
        if query.len() != self.index.dim {
            return Err("ERR vector dimension mismatch".to_string());
        }
        let entries_ref = &self.entries;
        let filter_fn = |key: &Bytes| -> bool {
            let Some(entry) = entries_ref.get(key) else {
                return false;
            };
            if let Some(req_scope) = scope {
                entry.scope.as_deref() == Some(req_scope)
            } else {
                true
            }
        };
        let candidates = self
            .index
            .search_filtered(query, 4, None, true, Some(&filter_fn));
        if let Some((best_id, dist)) = candidates.into_iter().next() {
            let sim = (1.0 - dist).clamp(0.0, 1.0);
            if sim >= threshold
                && let Some(entry) = self.entries.get(&best_id)
            {
                self.hits += 1;
                self.tokens_saved += entry.tokens;
                return Ok(Some(SemanticHit {
                    id: entry.id.clone(),
                    prompt: entry.prompt.clone(),
                    response: entry.response.clone(),
                    score: sim,
                }));
            }
        }
        self.misses += 1;
        Ok(None)
    }

    /// Deletes a single entry by ID.
    pub fn del(&mut self, id: &Bytes) -> bool {
        let removed_entry = self.entries.remove(id).is_some();
        let removed_vec = self.index.remove(id);
        removed_entry || removed_vec
    }

    /// Clears all entries and resets the index while preserving namespace configuration.
    pub fn flush(&mut self) {
        self.entries.clear();
        self.index = HnswIndex::new(self.namespace.clone(), self.index.dim, VectorMetric::Cosine);
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
        assert!(results[0].0 == "doc1" || results[0].0 == "doc3");

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
            assert!(
                (orig - recon).abs() < 0.02,
                "orig: {}, recon: {}",
                orig,
                recon
            );
        }

        let mut index = HnswIndex::new("sq8_idx".to_string(), 5, VectorMetric::Cosine);
        index
            .add_quantized(Bytes::from("k1"), v.clone(), true, true)
            .unwrap();
        index
            .add_quantized(Bytes::from("k2"), vec![0.0, 1.0, 0.0, 0.0, 0.0], true, true)
            .unwrap();

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
        assert!(
            d1 < d2,
            "v1 should be much closer to query than v2: d1={}, d2={}",
            d1,
            d2
        );

        let mut index = HnswIndex::new("pq_idx".to_string(), dim, VectorMetric::L2);
        index
            .add_quantized_ext(Bytes::from("doc_pos"), v1, false, true, false)
            .unwrap();
        index
            .add_quantized_ext(Bytes::from("doc_neg"), v2, false, true, false)
            .unwrap();

        let res = index.search(&query, 1);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, Bytes::from("doc_pos"));
    }

    #[test]
    fn test_simd_distance_acceleration_parity() {
        // Test high-dimensional vector (e.g. 64-dim)
        let a: Vec<f32> = (0..64).map(|i| (i as f32 * 0.1).sin()).collect();
        let b: Vec<f32> = (0..64).map(|i| (i as f32 * 0.1 + 0.5).cos()).collect();

        // 1. Dot product
        let dot_simd = dot_product(&a, &b);
        let dot_port = dot_product_portable(&a, &b);
        assert!(
            (dot_simd - dot_port).abs() < 1e-4,
            "dot_simd ({}) vs dot_port ({}) mismatch",
            dot_simd,
            dot_port
        );

        // 2. L2 distance squared
        let l2_simd = l2_distance_sq(&a, &b);
        let l2_port = l2_distance_sq_portable(&a, &b);
        assert!(
            (l2_simd - l2_port).abs() < 1e-4,
            "l2_simd ({}) vs l2_port ({}) mismatch",
            l2_simd,
            l2_port
        );

        // 3. Cosine distance
        let cos_simd = cosine_distance(&a, &b);
        let cos_port = cosine_distance_portable(&a, &b);
        assert!(
            (cos_simd - cos_port).abs() < 1e-4,
            "cos_simd ({}) vs cos_port ({}) mismatch",
            cos_simd,
            cos_port
        );

        // 4. Distance metrics via compute_distance
        let d_cos = compute_distance(&a, &b, VectorMetric::Cosine);
        let d_l2 = compute_distance(&a, &b, VectorMetric::L2);
        let d_ip = compute_distance(&a, &b, VectorMetric::IP);

        assert!((d_cos - cos_simd).abs() < 1e-4);
        assert!((d_l2 - l2_simd.sqrt()).abs() < 1e-4);
        assert!((d_ip - (-dot_simd)).abs() < 1e-4);
    }

    #[test]
    fn test_semantic_cache_set_get_ttl_scope_and_telemetry() {
        let mut cache = SemanticCache::new("llm:gpt4".to_string(), 4);
        cache
            .set(
                Bytes::from("q1"),
                Bytes::from("What is Rudis?"),
                Bytes::from("Rudis is an AI-native in-memory data store."),
                vec![1.0, 0.0, 0.0, 0.0],
                None,
                Some(Bytes::from("tenant:acme")),
                true,
                Some(42),
            )
            .unwrap();
        cache
            .set(
                Bytes::from("q2"),
                Bytes::from("Expiring prompt"),
                Bytes::from("Temporary answer"),
                vec![0.0, 1.0, 0.0, 0.0],
                Some(std::time::Duration::from_millis(1)),
                Some(Bytes::from("tenant:acme")),
                false,
                Some(10),
            )
            .unwrap();

        // Wait for q2 to expire
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Query near q1 with matching scope -> hit
        let hit = cache
            .get(&[0.99, 0.05, 0.0, 0.0], 0.95, Some(b"tenant:acme"))
            .unwrap()
            .expect("expected semantic hit");
        assert_eq!(hit.id, Bytes::from("q1"));
        assert_eq!(
            hit.response,
            Bytes::from("Rudis is an AI-native in-memory data store.")
        );
        assert!(hit.score >= 0.95);
        assert_eq!(cache.hits, 1);
        assert_eq!(cache.tokens_saved, 42);
        assert_eq!(cache.evicted_expired, 1);

        // Query near q1 with wrong scope -> miss
        let miss_scope = cache
            .get(&[0.99, 0.05, 0.0, 0.0], 0.95, Some(b"tenant:other"))
            .unwrap();
        assert!(miss_scope.is_none());
        assert_eq!(cache.misses, 1);

        // Query with expired q2's vector -> miss
        let miss_expired = cache
            .get(&[0.0, 1.0, 0.0, 0.0], 0.90, Some(b"tenant:acme"))
            .unwrap();
        assert!(miss_expired.is_none());
        assert_eq!(cache.misses, 2);

        // Delete q1
        assert!(cache.del(&Bytes::from("q1")));
        assert!(!cache.del(&Bytes::from("q1")));
        assert_eq!(cache.entries.len(), 0);
    }

    #[test]
    fn test_redis8_vset_filter_evaluator_and_quantization() {
        let json = r#"{"year": 1994, "rating": 9.3, "genre": "drama", "tags": ["crime", "classic"], "meta": {"oscar": true}}"#;
        assert!(
            eval_vset_filter(
                r#".year >= 1990 and .rating > 9.0 and .genre == "drama""#,
                Some(json)
            )
            .unwrap()
        );
        assert!(
            eval_vset_filter(
                r#".genre in ["drama", "sci-fi"] && "classic" in .tags && .meta.oscar"#,
                Some(json)
            )
            .unwrap()
        );
        assert!(!eval_vset_filter(r#"not (.year == 1994)"#, Some(json)).unwrap());
        assert!(eval_vset_filter(r#"(.year - 1990) * 2 == 8"#, Some(json)).unwrap());
        assert!(!eval_vset_filter(r#".year > 1900"#, None).unwrap());

        // Binary quantization + REDUCE projection
        let mut idx = HnswIndex::new("vset_bin".to_string(), 4, VectorMetric::Cosine);
        idx.quant = VQuant::Bin;
        idx.set_projection(8);
        assert_eq!(idx.client_dim(), 8);
        let p1 = idx.project(&[1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0]);
        let p2 = idx.project(&[-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0]);
        idx.add(Bytes::from("e1"), p1.clone()).unwrap();
        idx.add(Bytes::from("e2"), p2).unwrap();
        let hits = idx.search(&p1, 1);
        assert_eq!(hits[0].0, Bytes::from("e1"));
    }

    #[test]
    fn test_hnsw_diversity_heuristic_and_delete_slot_reuse() {
        let mut idx = HnswIndex::with_params("churn".to_string(), 4, VectorMetric::L2, 4, 32, 32);
        for i in 0..30 {
            let angle = (i as f32) * 0.2;
            idx.add(
                Bytes::from(format!("n{i}")),
                vec![angle.cos(), angle.sin(), (i as f32) * 0.05, 1.0],
            )
            .unwrap();
        }
        assert_eq!(idx.nodes.len(), 30);

        // Delete 15 elements (including the current entry point) and re-insert 15 new elements.
        // Slot reuse (`free_ids`) must prevent `nodes.len()` from growing past 30.
        for i in 0..15 {
            assert!(idx.remove(&Bytes::from(format!("n{i}"))));
        }
        assert_eq!(idx.len(), 15);
        assert_eq!(idx.free_ids.len(), 15);

        for i in 30..45 {
            let angle = (i as f32) * 0.2;
            idx.add(
                Bytes::from(format!("n{i}")),
                vec![angle.cos(), angle.sin(), (i as f32) * 0.05, 1.0],
            )
            .unwrap();
        }
        assert_eq!(idx.len(), 30);
        assert_eq!(idx.nodes.len(), 30);
        assert!(idx.free_ids.is_empty());

        // Verify recall on remaining + newly inserted nodes after churn
        let target_i = 37usize;
        let angle = (target_i as f32) * 0.2;
        let q = [angle.cos(), angle.sin(), (target_i as f32) * 0.05, 1.0];
        let hits = idx.search(&q, 3);
        assert_eq!(hits[0].0, Bytes::from("n37"));
    }

    #[test]
    fn test_pq_kmeans_codebook_training_reduces_distortion() {
        // Generate vectors in a non-unit scale [10.0, 50.0] where an untrained [-1, 1] codebook
        // suffers high clamping distortion, and verify k-means++ + Lloyd training drastically
        // lowers reconstruction error and auto-trains on HnswIndex.
        let mut samples = Vec::new();
        for i in 0..24 {
            let base = 10.0 + (i as f32) * 1.5;
            samples.push(vec![
                base,
                base + 2.0,
                base * 0.5,
                base - 3.0,
                -base,
                base + 5.0,
                base * 1.2,
                base + 0.25,
            ]);
        }
        let untrained = ProductQuantizer::new(8, 2);
        let trained = ProductQuantizer::train(8, 2, &samples, 15);
        let err_untrained = untrained.distortion(&samples);
        let err_trained = trained.distortion(&samples);
        assert!(
            err_trained < err_untrained * 0.01,
            "expected trained PQ distortion ({err_trained}) << untrained ({err_untrained})"
        );

        let mut idx = HnswIndex::new("pq_auto".to_string(), 8, VectorMetric::L2);
        for (i, v) in samples.iter().enumerate() {
            idx.add_quantized_ext(Bytes::from(format!("p{i}")), v.clone(), false, true, false)
                .unwrap();
        }
        assert!(idx.pq_trained);
        let hits = idx.search(&samples[10], 1);
        assert_eq!(hits[0].0, Bytes::from("p10"));
    }
}
