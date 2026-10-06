use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

pub type DocId = u32;

#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Text {
        weight: f64,
        sortable: bool,
        nostem: bool,
    },
    Numeric {
        sortable: bool,
    },
    Tag {
        separator: char,
        casesensitive: bool,
    },
    Vector {
        dim: usize,
        distance_metric: String,
        algorithm: String,
        attrs: VectorFieldAttrs,
    },
}

/// Element type of a vector field (`TYPE` attribute of `FT.CREATE ... VECTOR`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorDataType {
    #[default]
    Float32,
    Float64,
    Float16,
    BFloat16,
    Int8,
    Uint8,
}

impl VectorDataType {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "FLOAT32" => Some(Self::Float32),
            "FLOAT64" => Some(Self::Float64),
            "FLOAT16" => Some(Self::Float16),
            "BFLOAT16" => Some(Self::BFloat16),
            "INT8" => Some(Self::Int8),
            "UINT8" => Some(Self::Uint8),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
            Self::Float16 => "FLOAT16",
            Self::BFloat16 => "BFLOAT16",
            Self::Int8 => "INT8",
            Self::Uint8 => "UINT8",
        }
    }

    pub fn elem_size(&self) -> usize {
        match self {
            Self::Float64 => 8,
            Self::Float32 => 4,
            Self::Float16 | Self::BFloat16 => 2,
            Self::Int8 | Self::Uint8 => 1,
        }
    }
}

/// Optional tuning attributes of a vector field.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorFieldAttrs {
    pub data_type: VectorDataType,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_runtime: usize,
    pub initial_cap: usize,
    pub epsilon: f64,
    pub block_size: usize,
}

impl Default for VectorFieldAttrs {
    fn default() -> Self {
        Self {
            data_type: VectorDataType::Float32,
            m: 16,
            ef_construction: 200,
            ef_runtime: 10,
            initial_cap: 1024,
            epsilon: 0.01,
            block_size: 1024,
        }
    }
}

#[inline]
fn f16_to_f32(h: u16) -> f32 {
    let sign = ((h >> 15) & 1) as u32;
    let exp = ((h >> 10) & 0x1f) as u32;
    let frac = (h & 0x3ff) as u32;
    let bits = if exp == 0 {
        if frac == 0 {
            sign << 31
        } else {
            // Subnormal: normalize.
            let mut e: i32 = -14;
            let mut f = frac;
            while f & 0x400 == 0 {
                f <<= 1;
                e -= 1;
            }
            f &= 0x3ff;
            (sign << 31) | (((e + 127) as u32) << 23) | (f << 13)
        }
    } else if exp == 0x1f {
        (sign << 31) | 0x7f80_0000 | (frac << 13)
    } else {
        (sign << 31) | ((exp + 112) << 23) | (frac << 13)
    };
    f32::from_bits(bits)
}

/// Converts an `f32` to IEEE-754 half precision bits (round-to-nearest-even).
pub fn f32_to_f16(v: f32) -> u16 {
    let bits = v.to_bits();
    let sign = ((bits >> 16) & 0x8000) as u16;
    let exp = ((bits >> 23) & 0xff) as i32;
    let mut frac = bits & 0x7f_ffff;
    if exp == 0xff {
        return sign | 0x7c00 | if frac != 0 { 0x200 } else { 0 };
    }
    let e = exp - 127 + 15;
    if e >= 0x1f {
        return sign | 0x7c00;
    }
    if e <= 0 {
        if e < -10 {
            return sign;
        }
        frac |= 0x80_0000;
        let shift = (14 - e) as u32;
        let half = frac >> shift;
        let rem = frac & ((1 << shift) - 1);
        let mid = 1 << (shift - 1);
        let rounded = if rem > mid || (rem == mid && half & 1 == 1) {
            half + 1
        } else {
            half
        };
        return sign | rounded as u16;
    }
    let half = ((e as u32) << 10) | (frac >> 13);
    let rem = frac & 0x1fff;
    let rounded = if rem > 0x1000 || (rem == 0x1000 && half & 1 == 1) {
        half + 1
    } else {
        half
    };
    sign | rounded as u16
}

/// Decodes a binary little-endian vector blob of the given element type.
pub fn decode_typed_blob(bytes: &[u8], data_type: VectorDataType) -> Option<Vec<f32>> {
    let sz = data_type.elem_size();
    if bytes.is_empty() || !bytes.len().is_multiple_of(sz) {
        return None;
    }
    let out = match data_type {
        VectorDataType::Float32 => bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| f32::from_le_bytes(*c))
            .collect(),
        VectorDataType::Float64 => bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|c| f64::from_le_bytes(*c) as f32)
            .collect(),
        VectorDataType::Float16 => bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| f16_to_f32(u16::from_le_bytes(*c)))
            .collect(),
        VectorDataType::BFloat16 => bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| f32::from_bits((u16::from_le_bytes(*c) as u32) << 16))
            .collect(),
        VectorDataType::Int8 => bytes.iter().map(|&b| b as i8 as f32).collect(),
        VectorDataType::Uint8 => bytes.iter().map(|&b| b as f32).collect(),
    };
    Some(out)
}

fn parse_vector_text(bytes: &[u8]) -> Option<Vec<f32>> {
    if bytes.is_empty()
        || !bytes.iter().all(|b| {
            b.is_ascii_digit()
                || matches!(b, b'.' | b',' | b'-' | b'+' | b'e' | b'E' | b'[' | b']')
                || b.is_ascii_whitespace()
        })
    {
        return None;
    }
    let s = std::str::from_utf8(bytes).ok()?;
    let mut out = Vec::new();
    for tok in s.split(|c: char| c == ',' || c.is_whitespace() || c == '[' || c == ']') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        out.push(tok.parse::<f32>().ok()?);
    }
    if out.is_empty() { None } else { Some(out) }
}

/// Decodes a vector value according to the field's `TYPE` and `DIM`.
///
/// Binary little-endian blobs whose length equals `dim * sizeof(TYPE)` are decoded as binary.
/// Otherwise a textual representation (`"1,2,3"` or `"[1, 2, 3]"`) is accepted. Returns `None`
/// when the value cannot be decoded into exactly `dim` finite components (`dim == 0` accepts any).
pub fn decode_vector(bytes: &[u8], data_type: VectorDataType, dim: usize) -> Option<Vec<f32>> {
    let text = parse_vector_text(bytes);
    let v = match text {
        Some(t) if dim == 0 || t.len() == dim => t,
        _ => {
            if dim != 0 && bytes.len() != dim * data_type.elem_size() {
                return None;
            }
            decode_typed_blob(bytes, data_type)?
        }
    };
    if (dim != 0 && v.len() != dim) || v.iter().any(|x| !x.is_finite()) {
        return None;
    }
    Some(v)
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaField {
    pub identifier: String, // e.g. "$.title", "$.price", "title"
    pub alias: String,      // e.g. "title" or "$.title"
    pub field_type: FieldType,
}

#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub name: String,
    pub on_type: String, // "HASH" or "JSON"
    pub prefixes: Vec<String>,
    pub fields: HashMap<String, FieldType>,
    pub schema_fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Posting {
    pub doc_id: DocId,
    pub term_freq: u32,
}

#[derive(Debug, Clone)]
pub struct DocMeta {
    pub key: Bytes,
    pub doc_len: usize, // total tokens
    pub fields: HashMap<String, String>,
    pub numeric_fields: HashMap<String, f64>,
    pub tag_fields: HashMap<String, HashSet<String>>,
    pub vector_fields: HashMap<String, Vec<f32>>,
    pub multi_vector_fields: HashMap<String, Vec<Vec<f32>>>,
    pub terms: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedF64(pub f64);

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Balanced interval index for O(log N + K) numeric range search, inspired by Dragonfly's RangeTree.
#[derive(Debug, Default, Clone)]
pub struct RangeTree {
    pub entries: std::collections::BTreeMap<OrderedF64, Vec<DocId>>,
    pub total_entries: usize,
}

impl RangeTree {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.total_entries
    }

    #[inline]
    pub fn add(&mut self, doc_id: DocId, value: f64) {
        let key = OrderedF64(value);
        let list = self.entries.entry(key).or_default();
        if let Err(pos) = list.binary_search(&doc_id) {
            list.insert(pos, doc_id);
            self.total_entries += 1;
        }
    }

    #[inline]
    pub fn remove(&mut self, doc_id: DocId, value: f64) {
        let key = OrderedF64(value);
        if let Some(list) = self.entries.get_mut(&key) {
            if let Ok(pos) = list.binary_search(&doc_id) {
                list.remove(pos);
                self.total_entries = self.total_entries.saturating_sub(1);
            }
            if list.is_empty() {
                self.entries.remove(&key);
            }
        }
    }

    /// Performs a balanced B-tree range scan in O(log N + K) time.
    #[inline]
    pub fn range(&self, min: f64, max: f64) -> Vec<DocId> {
        // NaN bounds (`@n:[nan 0]`) would make BTreeMap::range panic.
        if min.is_nan() || max.is_nan() || min > max {
            return Vec::new();
        }
        let start = OrderedF64(min);
        let end = OrderedF64(max);
        let mut results = Vec::new();
        for (_val, doc_ids) in self.entries.range(start..=end) {
            results.extend_from_slice(doc_ids);
        }
        results
    }

    #[inline]
    pub fn min(&self) -> Option<f64> {
        self.entries.first_key_value().map(|(k, _)| k.0)
    }

    #[inline]
    pub fn max(&self) -> Option<f64> {
        self.entries.last_key_value().map(|(k, _)| k.0)
    }
}

#[derive(Debug, Default)]
pub struct InvertedIndex {
    pub schema: Option<IndexSchema>,
    // term -> list of postings
    pub inverted: HashMap<String, Vec<Posting>>,
    // numeric field -> balanced RangeTree
    pub numeric_trees: HashMap<String, RangeTree>,
    // key -> dense DocId
    pub key_to_id: HashMap<Bytes, DocId>,
    // doc_id -> metadata
    pub id_to_meta: HashMap<DocId, DocMeta>,
    // vector field -> FLAT or HNSW vector index
    pub vector_indices: HashMap<String, crate::vector::VectorFieldIndex>,
    pub next_doc_id: DocId,
    pub free_ids: Vec<DocId>,
    pub total_docs: usize,
    pub total_terms: usize,
    /// Number of documents rejected because of invalid vector values (`hash_indexing_failures`).
    pub indexing_failures: usize,
}

#[inline]
pub fn get_numeric_tree<'a>(index: &'a InvertedIndex, field: &str) -> Option<&'a RangeTree> {
    if let Some(tree) = index.numeric_trees.get(field) {
        return Some(tree);
    }
    if let Some(stripped) = field.strip_prefix("$.") {
        if let Some(tree) = index.numeric_trees.get(stripped) {
            return Some(tree);
        }
    } else {
        let with_prefix = format!("$.{}", field);
        if let Some(tree) = index.numeric_trees.get(&with_prefix) {
            return Some(tree);
        }
    }
    None
}

static ENGLISH_STOP_WORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "a",
        "about",
        "above",
        "after",
        "again",
        "against",
        "all",
        "am",
        "an",
        "and",
        "any",
        "are",
        "aren't",
        "as",
        "at",
        "be",
        "because",
        "been",
        "before",
        "being",
        "below",
        "between",
        "both",
        "but",
        "by",
        "can't",
        "cannot",
        "could",
        "couldn't",
        "did",
        "didn't",
        "do",
        "does",
        "doesn't",
        "doing",
        "don't",
        "down",
        "during",
        "each",
        "few",
        "for",
        "from",
        "further",
        "had",
        "hadn't",
        "has",
        "hasn't",
        "have",
        "haven't",
        "having",
        "he",
        "he'd",
        "he'll",
        "he's",
        "her",
        "here",
        "here's",
        "hers",
        "herself",
        "him",
        "himself",
        "his",
        "how",
        "how's",
        "i",
        "i'd",
        "i'll",
        "i'm",
        "i've",
        "if",
        "in",
        "into",
        "is",
        "isn't",
        "it",
        "it's",
        "its",
        "itself",
        "let's",
        "me",
        "more",
        "most",
        "mustn't",
        "my",
        "myself",
        "no",
        "nor",
        "not",
        "of",
        "off",
        "on",
        "once",
        "only",
        "or",
        "other",
        "ought",
        "our",
        "ours",
        "ourselves",
        "out",
        "over",
        "own",
        "same",
        "shan't",
        "she",
        "she'd",
        "she'll",
        "she's",
        "should",
        "shouldn't",
        "so",
        "some",
        "such",
        "than",
        "that",
        "that's",
        "the",
        "their",
        "theirs",
        "them",
        "themselves",
        "then",
        "there",
        "there's",
        "these",
        "they",
        "they'd",
        "they'll",
        "they're",
        "they've",
        "this",
        "those",
        "through",
        "to",
        "too",
        "under",
        "until",
        "up",
        "very",
        "was",
        "wasn't",
        "we",
        "we'd",
        "we'll",
        "we're",
        "we've",
        "were",
        "weren't",
        "what",
        "what's",
        "when",
        "when's",
        "where",
        "where's",
        "which",
        "while",
        "who",
        "who's",
        "whom",
        "why",
        "why's",
        "with",
        "won't",
        "would",
        "wouldn't",
        "you",
        "you'd",
        "you'll",
        "you're",
        "you've",
        "your",
        "yours",
        "yourself",
        "yourselves",
    ]
    .into_iter()
    .collect()
});

pub fn simple_stem(word: &str) -> String {
    let lower = word.to_ascii_lowercase();
    if lower.len() > 5 && lower.ends_with("ing") {
        return lower[..lower.len() - 3].to_string();
    }
    if lower.len() > 4 && lower.ends_with("ies") {
        return format!("{}y", &lower[..lower.len() - 3]);
    }
    if lower.len() > 4 && lower.ends_with("es") {
        return lower[..lower.len() - 2].to_string();
    }
    if lower.len() > 3 && lower.ends_with("ed") {
        return lower[..lower.len() - 2].to_string();
    }
    if lower.len() > 3 && lower.ends_with('s') && !lower.ends_with("ss") {
        return lower[..lower.len() - 1].to_string();
    }
    lower
}

pub fn tokenize_text(text: &str, stem: bool) -> Vec<String> {
    let mut tokens = Vec::new();
    for raw in text.split(|c: char| !c.is_alphanumeric()) {
        if raw.is_empty() {
            continue;
        }
        let lower = raw.to_ascii_lowercase();
        if ENGLISH_STOP_WORDS.contains(lower.as_str()) {
            continue;
        }
        if stem {
            tokens.push(simple_stem(&lower));
        } else {
            tokens.push(lower);
        }
    }
    tokens
}

/// Decodes a vector blob assuming `FLOAT32` when the field type is unknown.
pub fn parse_vector_blob(bytes: &[u8]) -> Vec<f32> {
    decode_vector(bytes, VectorDataType::Float32, 0).unwrap_or_default()
}

/// Parses a `DISTANCE_METRIC` string into a [`crate::vector::VectorMetric`].
pub fn metric_from_str(s: &str) -> crate::vector::VectorMetric {
    s.parse().unwrap_or(crate::vector::VectorMetric::Cosine)
}

fn build_vector_index(
    index_name: &str,
    ftype: &FieldType,
    dim_hint: usize,
) -> Option<crate::vector::VectorFieldIndex> {
    let FieldType::Vector {
        dim,
        distance_metric,
        algorithm,
        attrs,
    } = ftype
    else {
        return None;
    };
    let dim = if *dim == 0 { dim_hint } else { *dim };
    if dim == 0 {
        return None;
    }
    let metric = metric_from_str(distance_metric);
    Some(if algorithm.eq_ignore_ascii_case("FLAT") {
        crate::vector::VectorFieldIndex::Flat(crate::vector::FlatIndex::new(
            index_name.to_string(),
            dim,
            metric,
            attrs.initial_cap,
        ))
    } else {
        crate::vector::VectorFieldIndex::Hnsw(crate::vector::HnswIndex::with_params(
            index_name.to_string(),
            dim,
            metric,
            attrs.m,
            attrs.ef_construction,
            attrs.ef_runtime,
        ))
    })
}

impl InvertedIndex {
    pub fn new(mut schema: IndexSchema) -> Self {
        if schema.schema_fields.is_empty() {
            schema.schema_fields = schema
                .fields
                .iter()
                .map(|(k, v)| SchemaField {
                    identifier: k.clone(),
                    alias: k.clone(),
                    field_type: v.clone(),
                })
                .collect();
        }
        let mut vector_indices = HashMap::new();
        for sf in &schema.schema_fields {
            if let Some(vi) = build_vector_index(&schema.name, &sf.field_type, 0) {
                vector_indices.insert(sf.alias.clone(), vi);
            }
        }
        Self {
            schema: Some(schema),
            inverted: HashMap::new(),
            numeric_trees: HashMap::new(),
            key_to_id: HashMap::new(),
            id_to_meta: HashMap::new(),
            vector_indices,
            next_doc_id: 1,
            free_ids: Vec::new(),
            total_docs: 0,
            total_terms: 0,
            indexing_failures: 0,
        }
    }

    /// Resolves a vector field reference (alias, identifier or `$.`-prefixed path) to the
    /// canonical alias used as the key of [`Self::vector_indices`].
    pub fn resolve_vector_field(&self, name: &str) -> Option<&SchemaField> {
        let schema = self.schema.as_ref()?;
        let stripped = name.strip_prefix("$.").unwrap_or(name);
        schema.schema_fields.iter().find(|sf| {
            matches!(sf.field_type, FieldType::Vector { .. })
                && (sf.alias == name
                    || sf.identifier == name
                    || sf.alias == stripped
                    || sf.identifier.strip_prefix("$.").unwrap_or(&sf.identifier) == stripped)
        })
    }

    /// Indexes a HASH document from its raw (binary-safe) field/value pairs.
    ///
    /// Vector fields are decoded from the raw bytes according to their `TYPE`, so binary
    /// `FLOAT32`/`FLOAT16`/... blobs sent by clients are never corrupted by UTF-8 conversion.
    pub fn add_hash_document(&mut self, key: &str, raw: &[(Bytes, Bytes)]) {
        let Some(schema) = self.schema.as_ref() else {
            return;
        };
        let mut str_fields = HashMap::with_capacity(raw.len());
        let mut vectors = HashMap::new();
        let mut failed = false;
        for (f, v) in raw {
            let fname = String::from_utf8_lossy(f).to_string();
            let vec_field = schema.schema_fields.iter().find(|sf| {
                matches!(sf.field_type, FieldType::Vector { .. })
                    && (sf.identifier == fname || sf.alias == fname)
            });
            if let Some(sf) = vec_field
                && let FieldType::Vector { dim, attrs, .. } = &sf.field_type
            {
                match decode_vector(v, attrs.data_type, *dim) {
                    Some(vec) => {
                        vectors.insert(sf.alias.clone(), vec);
                    }
                    None => failed = true,
                }
            }
            str_fields.insert(fname, String::from_utf8_lossy(v).to_string());
        }
        if failed {
            if self.key_to_id.contains_key(key.as_bytes()) {
                self.remove_document(key);
            }
            self.indexing_failures += 1;
            return;
        }
        self.add_document(key, str_fields, Some(vectors));
    }

    #[inline(always)]
    pub fn doc_id_of(&self, key: &[u8]) -> Option<DocId> {
        self.key_to_id.get(key).copied()
    }

    #[inline(always)]
    pub fn key_of(&self, doc_id: DocId) -> Option<&Bytes> {
        self.id_to_meta.get(&doc_id).map(|m| &m.key)
    }

    pub fn avg_doc_len(&self) -> f64 {
        if self.total_docs == 0 {
            0.0
        } else {
            self.total_terms as f64 / self.total_docs as f64
        }
    }

    pub fn add_document(
        &mut self,
        doc_id_str: &str,
        fields: HashMap<String, String>,
        vectors: Option<HashMap<String, Vec<f32>>>,
    ) {
        let key_bytes = Bytes::copy_from_slice(doc_id_str.as_bytes());

        // If document already exists, remove old entries first
        if self.key_to_id.contains_key(&key_bytes) {
            self.remove_document(doc_id_str);
        }

        let schema = match &self.schema {
            Some(s) => s.clone(),
            None => return,
        };

        // Resolve and validate vector fields first: a document with an undecodable vector or a
        // dimension mismatch is rejected entirely (counted in `hash_indexing_failures`).
        let provided = vectors.unwrap_or_default();
        let mut vector_fields: HashMap<String, Vec<f32>> = HashMap::new();
        let mut multi_vector_fields: HashMap<String, Vec<Vec<f32>>> = HashMap::new();
        for sf in &schema.schema_fields {
            let FieldType::Vector { dim, attrs, .. } = &sf.field_type else {
                continue;
            };
            let candidate = provided
                .get(&sf.alias)
                .or_else(|| provided.get(&sf.identifier))
                .cloned()
                .map(Some)
                .or_else(|| {
                    fields
                        .get(&sf.alias)
                        .or_else(|| fields.get(&sf.identifier))
                        .map(|s| decode_vector(s.as_bytes(), attrs.data_type, *dim))
                });
            match candidate {
                None => {}
                Some(Some(v)) if *dim == 0 || v.len() == *dim => {
                    let mut chunks = vec![v.clone()];
                    let mut chunk_idx = 1usize;
                    while let Some(extra) = provided
                        .get(&format!("{}#{}", sf.alias, chunk_idx))
                        .or_else(|| provided.get(&format!("{}#{}", sf.identifier, chunk_idx)))
                    {
                        if *dim != 0 && extra.len() != *dim {
                            self.indexing_failures += 1;
                            return;
                        }
                        chunks.push(extra.clone());
                        chunk_idx += 1;
                    }
                    vector_fields.insert(sf.alias.clone(), v);
                    multi_vector_fields.insert(sf.alias.clone(), chunks);
                }
                Some(_) => {
                    self.indexing_failures += 1;
                    return;
                }
            }
        }

        let mut doc_len = 0;
        let mut numeric_fields = HashMap::new();
        let mut tag_fields = HashMap::new();
        let mut term_positions: HashMap<String, u32> = HashMap::new();

        for (field_name, field_val) in &fields {
            if let Some(ftype) = schema.fields.get(field_name) {
                match ftype {
                    FieldType::Text { nostem, .. } => {
                        let tokens = tokenize_text(field_val, !*nostem);
                        doc_len += tokens.len();
                        for tok in tokens {
                            *term_positions.entry(tok).or_default() += 1;
                        }
                    }
                    FieldType::Numeric { .. } => {
                        if let Ok(num) = field_val.trim().parse::<f64>() {
                            numeric_fields.insert(field_name.clone(), num);
                        }
                    }
                    FieldType::Tag {
                        separator,
                        casesensitive,
                    } => {
                        let mut tags = HashSet::new();
                        for t in field_val.split(*separator) {
                            let clean = t.trim();
                            if !clean.is_empty() {
                                if *casesensitive {
                                    tags.insert(clean.to_string());
                                } else {
                                    tags.insert(clean.to_ascii_lowercase());
                                }
                            }
                        }
                        tag_fields.insert(field_name.clone(), tags);
                    }
                    FieldType::Vector { .. } => {}
                }
            } else if field_name != "$" {
                // Untyped text fallback
                let tokens = tokenize_text(field_val, true);
                doc_len += tokens.len();
                for tok in tokens {
                    *term_positions.entry(tok).or_default() += 1;
                }
            }
        }

        let doc_id = self.free_ids.pop().unwrap_or_else(|| {
            let id = self.next_doc_id;
            self.next_doc_id += 1;
            id
        });

        let mut indexed_terms = Vec::with_capacity(term_positions.len());
        for (term, freq) in term_positions {
            let posting = Posting {
                doc_id,
                term_freq: freq,
            };
            self.inverted.entry(term.clone()).or_default().push(posting);
            indexed_terms.push(term);
        }

        for (field_name, vec) in &vector_fields {
            if !self.vector_indices.contains_key(field_name)
                && let Some(sf) = schema
                    .schema_fields
                    .iter()
                    .find(|sf| &sf.alias == field_name)
                && let Some(vi) = build_vector_index(&schema.name, &sf.field_type, vec.len())
            {
                self.vector_indices.insert(field_name.clone(), vi);
            }
            if let Some(vi) = self.vector_indices.get_mut(field_name) {
                let _ = vi.add(key_bytes.clone(), vec.clone());
                if let Some(chunks) = multi_vector_fields.get(field_name) {
                    for (chunk_idx, chunk_vec) in chunks.iter().enumerate().skip(1) {
                        let chunk_key = Bytes::from(format!("{}\x00{}", doc_id_str, chunk_idx));
                        let _ = vi.add(chunk_key, chunk_vec.clone());
                    }
                }
            }
        }

        for (field_name, &num_val) in &numeric_fields {
            self.numeric_trees
                .entry(field_name.clone())
                .or_default()
                .add(doc_id, num_val);
        }

        let doc_meta = DocMeta {
            key: key_bytes.clone(),
            doc_len,
            fields,
            numeric_fields,
            tag_fields,
            vector_fields,
            multi_vector_fields,
            terms: indexed_terms,
        };

        self.key_to_id.insert(key_bytes, doc_id);
        self.id_to_meta.insert(doc_id, doc_meta);
        self.total_docs += 1;
        self.total_terms += doc_len;
    }

    pub fn remove_document(&mut self, key: &str) {
        let key_bytes = key.as_bytes();
        let doc_id = match self.key_to_id.remove(key_bytes) {
            Some(id) => id,
            None => return,
        };

        if let Some(meta) = self.id_to_meta.remove(&doc_id) {
            self.total_docs = self.total_docs.saturating_sub(1);
            self.total_terms = self.total_terms.saturating_sub(meta.doc_len);
            self.free_ids.push(doc_id);

            // O(1) removal directed by the document's indexed terms
            for term in &meta.terms {
                if let Some(postings) = self.inverted.get_mut(term) {
                    postings.retain(|p| p.doc_id != doc_id);
                    if postings.is_empty() {
                        self.inverted.remove(term);
                    }
                }
            }

            // O(log N) removal directed by the document's numeric fields
            for (field_name, &num_val) in &meta.numeric_fields {
                if let Some(tree) = self.numeric_trees.get_mut(field_name) {
                    tree.remove(doc_id, num_val);
                    if tree.is_empty() {
                        self.numeric_trees.remove(field_name);
                    }
                }
            }

            // Remove from vector indices (including any extra chunk vectors)
            for field_name in meta.vector_fields.keys() {
                if let Some(hnsw) = self.vector_indices.get_mut(field_name) {
                    hnsw.remove(&meta.key);
                    if let Some(chunks) = meta.multi_vector_fields.get(field_name) {
                        for chunk_idx in 1..chunks.len() {
                            let chunk_key = Bytes::from(format!("{}\x00{}", key, chunk_idx));
                            hnsw.remove(&chunk_key);
                        }
                    }
                }
            }
        }
    }

    pub fn bm25_score(&self, term: &str, posting: &Posting, doc_len: usize) -> f64 {
        let n = self.inverted.get(term).map(|v| v.len()).unwrap_or(0);
        if n == 0 || self.total_docs == 0 {
            return 0.0;
        }
        let total_docs = self.total_docs as f64;
        let idf = ((total_docs - n as f64 + 0.5) / (n as f64 + 0.5) + 1.0).ln();
        if idf <= 0.0 {
            return 0.0001;
        }

        let k1 = 1.2;
        let b = 0.75;
        let avgdl = self.avg_doc_len().max(1.0);
        let freq = posting.term_freq as f64;
        let tf = (freq * (k1 + 1.0)) / (freq + k1 * (1.0 - b + b * (doc_len as f64 / avgdl)));

        idf * tf
    }
}

// Global registry of indices: IndexName -> InvertedIndex
static SEARCH_INDICES: LazyLock<RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static SEARCH_INDICES_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[inline(always)]
pub fn has_active_search_indices() -> bool {
    SEARCH_INDICES_COUNT.load(std::sync::atomic::Ordering::Relaxed) > 0
}

pub fn get_search_index(name: &str) -> Option<Arc<RwLock<InvertedIndex>>> {
    let registry = SEARCH_INDICES.read();
    registry.get(name).cloned()
}

pub fn create_search_index(schema: IndexSchema) -> Result<(), String> {
    let mut registry = SEARCH_INDICES.write();
    if registry.contains_key(&schema.name) {
        return Err(format!("Index already exists: {}", schema.name));
    }
    let name = schema.name.clone();
    let idx = Arc::new(RwLock::new(InvertedIndex::new(schema)));
    registry.insert(name, idx);
    SEARCH_INDICES_COUNT.store(registry.len(), std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

pub fn reset_search_index(schema: IndexSchema) {
    let mut registry = SEARCH_INDICES.write();
    let name = schema.name.clone();
    let idx = Arc::new(RwLock::new(InvertedIndex::new(schema)));
    registry.insert(name, idx);
    SEARCH_INDICES_COUNT.store(registry.len(), std::sync::atomic::Ordering::Relaxed);
}

pub fn drop_search_index(name: &str) -> Result<(), String> {
    let mut registry = SEARCH_INDICES.write();
    if registry.remove(name).is_some() {
        SEARCH_INDICES_COUNT.store(registry.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(())
    } else {
        Err(format!("Unknown index name: {}", name))
    }
}

pub fn list_search_indices() -> Vec<String> {
    let registry = SEARCH_INDICES.read();
    let mut names: Vec<String> = registry.keys().cloned().collect();
    names.sort();
    names
}

pub fn index_document_hook(key: &str, raw: &[(Bytes, Bytes)]) {
    let registry = SEARCH_INDICES.read();
    for idx_lock in registry.values() {
        let mut idx = idx_lock.write();
        if let Some(schema) = &idx.schema {
            if schema.on_type.to_uppercase() != "HASH" {
                continue;
            }
            let matched = if schema.prefixes.is_empty() {
                true
            } else {
                schema.prefixes.iter().any(|p| key.starts_with(p))
            };
            if matched {
                idx.add_hash_document(key, raw);
            }
        }
    }
}

pub fn extract_json_fields(
    schema: &IndexSchema,
    root: &serde_json::Value,
) -> (HashMap<String, String>, Option<HashMap<String, Vec<f32>>>) {
    let mut extracted_fields = HashMap::new();
    let mut extracted_vectors = HashMap::new();

    // Store full JSON document under "$"
    let root_str = serde_json::to_string(root).unwrap_or_default();
    extracted_fields.insert("$".to_string(), root_str);

    for sf in &schema.schema_fields {
        let segments = match crate::json::parse_json_path(&sf.identifier) {
            Ok(segs) => segs,
            Err(_) => continue,
        };
        let matches = crate::json::query_json_path(root, &segments);
        if matches.is_empty() {
            continue;
        }

        match &sf.field_type {
            FieldType::Text { .. } => {
                let mut texts = Vec::new();
                for v in &matches {
                    match v {
                        serde_json::Value::String(s) => texts.push(s.clone()),
                        serde_json::Value::Number(n) => texts.push(n.to_string()),
                        serde_json::Value::Bool(b) => texts.push(b.to_string()),
                        serde_json::Value::Array(arr) => {
                            for item in arr {
                                if let serde_json::Value::String(s) = item {
                                    texts.push(s.clone());
                                } else {
                                    texts.push(item.to_string());
                                }
                            }
                        }
                        other => texts.push(other.to_string()),
                    }
                }
                let text_val = texts.join(" ");
                extracted_fields.insert(sf.alias.clone(), text_val.clone());
                if sf.alias != sf.identifier {
                    extracted_fields.insert(sf.identifier.clone(), text_val);
                }
            }
            FieldType::Numeric { .. } => {
                if let Some(first) = matches.first() {
                    let num_opt = match first {
                        serde_json::Value::Number(n) => n.as_f64(),
                        serde_json::Value::String(s) => s.trim().parse::<f64>().ok(),
                        _ => None,
                    };
                    if let Some(num) = num_opt {
                        let num_str = num.to_string();
                        extracted_fields.insert(sf.alias.clone(), num_str.clone());
                        if sf.alias != sf.identifier {
                            extracted_fields.insert(sf.identifier.clone(), num_str);
                        }
                    }
                }
            }
            FieldType::Tag { separator, .. } => {
                let mut tags = Vec::new();
                for v in &matches {
                    match v {
                        serde_json::Value::String(s) => tags.push(s.clone()),
                        serde_json::Value::Array(arr) => {
                            for item in arr {
                                if let serde_json::Value::String(s) = item {
                                    tags.push(s.clone());
                                } else {
                                    tags.push(item.to_string());
                                }
                            }
                        }
                        other => tags.push(other.to_string()),
                    }
                }
                let tag_str = tags.join(&separator.to_string());
                extracted_fields.insert(sf.alias.clone(), tag_str.clone());
                if sf.alias != sf.identifier {
                    extracted_fields.insert(sf.identifier.clone(), tag_str);
                }
            }
            FieldType::Vector { .. } => {
                let mut all_vecs: Vec<Vec<f32>> = Vec::new();
                for matched_val in &matches {
                    let vec_opt: Option<Vec<f32>> = match matched_val {
                        serde_json::Value::Array(arr) => {
                            let v: Vec<f32> = arr
                                .iter()
                                .filter_map(|x| x.as_f64().map(|f| f as f32))
                                .collect();
                            if !v.is_empty() { Some(v) } else { None }
                        }
                        serde_json::Value::String(s) => {
                            let v = parse_vector_blob(s.as_bytes());
                            if !v.is_empty() { Some(v) } else { None }
                        }
                        _ => None,
                    };
                    if let Some(v) = vec_opt {
                        all_vecs.push(v);
                    }
                }
                if let Some(first_vec) = all_vecs.first() {
                    extracted_vectors.insert(sf.alias.clone(), first_vec.clone());
                    if sf.alias != sf.identifier {
                        extracted_vectors.insert(sf.identifier.clone(), first_vec.clone());
                    }
                    for (chunk_idx, chunk_vec) in all_vecs.iter().enumerate().skip(1) {
                        extracted_vectors
                            .insert(format!("{}#{}", sf.alias, chunk_idx), chunk_vec.clone());
                    }
                    let s = first_vec
                        .iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    extracted_fields.insert(sf.alias.clone(), s.clone());
                    if sf.alias != sf.identifier {
                        extracted_fields.insert(sf.identifier.clone(), s);
                    }
                }
            }
        }
    }

    (
        extracted_fields,
        if extracted_vectors.is_empty() {
            None
        } else {
            Some(extracted_vectors)
        },
    )
}

pub fn index_json_document_hook(key: &str, root: &serde_json::Value) {
    let registry = SEARCH_INDICES.read();
    for idx_lock in registry.values() {
        let mut idx = idx_lock.write();
        if let Some(schema) = &idx.schema {
            if schema.on_type.to_uppercase() != "JSON" {
                continue;
            }
            let matched = if schema.prefixes.is_empty() {
                true
            } else {
                schema.prefixes.iter().any(|p| key.starts_with(p))
            };
            if !matched {
                continue;
            }
            let (extracted_fields, extracted_vectors) = extract_json_fields(schema, root);
            idx.add_document(key, extracted_fields, extracted_vectors);
        }
    }
}

pub fn delete_document_hook(key: &str) {
    let registry = SEARCH_INDICES.read();
    for idx_lock in registry.values() {
        let mut idx = idx_lock.write();
        idx.remove_document(key);
    }
}

#[derive(Debug, Clone)]
pub enum QueryAst {
    Term(String),
    Prefix(String),
    Exact(String),
    FieldScope {
        field: String,
        inner: Box<QueryAst>,
    },
    NumericRange {
        field: String,
        min: f64,
        max: f64,
    },
    TagFilter {
        field: String,
        tags: Vec<String>,
    },
    And(Vec<QueryAst>),
    Or(Vec<QueryAst>),
    Not(Box<QueryAst>),
    KnnVector {
        field: String,
        k: usize,
        query_vec: Vec<f32>,
        param_name: String,
        /// `$K`-style parameter reference for K, resolved from `PARAMS` at execution.
        k_param: Option<String>,
        /// `EF_RUNTIME` literal or `$param`.
        ef_runtime: Option<String>,
        /// `AS <alias>` / `$YIELD_DISTANCE_AS` name for the distance field.
        yield_as: Option<String>,
    },
    VectorRange {
        field: String,
        radius: f32,
        radius_param: Option<String>,
        param_name: String,
        /// `$EPSILON` literal or `$param`.
        epsilon: Option<String>,
        /// `$YIELD_DISTANCE_AS` / `AS` alias for the distance field.
        yield_as: Option<String>,
    },
    MatchAll,
}

impl QueryAst {
    /// Returns the effective K of the query's KNN clause, resolving `$K` from `PARAMS`.
    pub fn knn_k(&self, opts: &SearchOptions) -> Option<usize> {
        match self.knn_clause()? {
            QueryAst::KnnVector { k, k_param, .. } => Some(
                k_param
                    .as_deref()
                    .and_then(|p| resolve_usize_param(p, opts))
                    .unwrap_or(*k),
            ),
            _ => None,
        }
    }

    /// Finds the KNN or `VECTOR_RANGE` clause node of the query, if any.
    pub fn knn_clause(&self) -> Option<&QueryAst> {
        match self {
            QueryAst::KnnVector { .. } | QueryAst::VectorRange { .. } => Some(self),
            QueryAst::And(subs) | QueryAst::Or(subs) => subs.iter().find_map(|s| s.knn_clause()),
            QueryAst::FieldScope { inner, .. } => inner.knn_clause(),
            _ => None,
        }
    }

    /// Name of the field carrying the vector distance in results (`AS` / `$YIELD_DISTANCE_AS`
    /// alias or `__<field>_score`).
    pub fn knn_score_field(&self) -> Option<String> {
        match self.knn_clause()? {
            QueryAst::KnnVector {
                field, yield_as, ..
            }
            | QueryAst::VectorRange {
                field, yield_as, ..
            } => Some(
                yield_as
                    .clone()
                    .unwrap_or_else(|| format!("__{}_score", field)),
            ),
            _ => None,
        }
    }

    /// If this query is a hybrid `(text_or_filter)=>[KNN ...]` or `text @vec:[VECTOR_RANGE ...]`
    /// AST where the base query is not `MatchAll`, returns `(base_ast, vec_ast)` for RRF / linear fusion.
    pub fn as_hybrid_rrf(&self) -> Option<(QueryAst, QueryAst)> {
        if let QueryAst::And(subs) = self
            && let Some(vec_pos) = subs.iter().position(|s| {
                matches!(s, QueryAst::KnnVector { .. } | QueryAst::VectorRange { .. })
            })
        {
            let others: Vec<QueryAst> = subs
                .iter()
                .enumerate()
                .filter(|(i, s)| *i != vec_pos && !matches!(s, QueryAst::MatchAll))
                .map(|(_, s)| s.clone())
                .collect();
            if !others.is_empty() {
                let base = if others.len() == 1 {
                    others.into_iter().next().unwrap()
                } else {
                    QueryAst::And(others)
                };
                return Some((base, subs[vec_pos].clone()));
            }
        }
        None
    }
}

/// Resolves a literal or `$param` value as `usize`.
pub fn resolve_usize_param(raw: &str, opts: &SearchOptions) -> Option<usize> {
    resolve_str_param(raw, opts)?.trim().parse().ok()
}

/// Resolves a literal or `$param` value as `f32`.
pub fn resolve_f32_param(raw: &str, opts: &SearchOptions) -> Option<f32> {
    resolve_str_param(raw, opts)?.trim().parse().ok()
}

/// Resolves a literal or `$param` value as a string.
pub fn resolve_str_param(raw: &str, opts: &SearchOptions) -> Option<String> {
    match raw.strip_prefix('$') {
        Some(name) => opts
            .params
            .get(name)
            .or_else(|| opts.params.get(raw))
            .map(|v| String::from_utf8_lossy(v).to_string()),
        None => Some(raw.to_string()),
    }
}

fn strip_outer_parens(s: &str) -> &str {
    let t = s.trim();
    if t.starts_with('(') && t.ends_with(')') {
        let inner = &t[1..t.len() - 1];
        let mut depth = 0i32;
        for c in inner.chars() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth < 0 {
                        return t;
                    }
                }
                _ => {}
            }
        }
        if depth == 0 {
            return inner.trim();
        }
    }
    t
}

/// Parses the body of a `[KNN ...]` clause (without the brackets) plus an optional trailing
/// `=>{$YIELD_DISTANCE_AS: alias}` attribute block.
fn parse_knn_clause(args_part: &str, trailer: &str) -> QueryAst {
    let tokens: Vec<&str> = args_part.split_whitespace().collect();
    let (k, k_param) = match tokens.first() {
        Some(t) if t.starts_with('$') => (10, Some(t.to_string())),
        Some(t) => (t.parse().unwrap_or(10), None),
        None => (10, None),
    };
    let field = tokens
        .get(1)
        .map(|s| s.trim_start_matches('@').to_string())
        .unwrap_or_default();
    let param_name = tokens
        .get(2)
        .map(|s| s.trim_start_matches('$').to_string())
        .unwrap_or_default();
    let mut ef_runtime = None;
    let mut yield_as = None;
    let mut i = 3;
    while i + 1 < tokens.len() {
        match tokens[i].to_ascii_uppercase().as_str() {
            "EF_RUNTIME" => ef_runtime = Some(tokens[i + 1].to_string()),
            "AS" | "YIELD_DISTANCE_AS" => yield_as = Some(tokens[i + 1].to_string()),
            _ => {}
        }
        i += 2;
    }
    // Legacy attribute syntax: =>{$YIELD_DISTANCE_AS: dist; $EF_RUNTIME: 20}
    if let Some(body) = trailer
        .trim()
        .strip_prefix("=>")
        .map(str::trim)
        .and_then(|s| s.strip_prefix('{'))
        .and_then(|s| s.split_once('}').map(|(b, _)| b))
    {
        for attr in body.split(';') {
            if let Some((name, val)) = attr.split_once(':') {
                let name = name.trim().trim_start_matches('$').to_ascii_uppercase();
                let val = val.trim().to_string();
                match name.as_str() {
                    "YIELD_DISTANCE_AS" => yield_as = Some(val),
                    "EF_RUNTIME" => ef_runtime = Some(val),
                    _ => {}
                }
            }
        }
    }
    QueryAst::KnnVector {
        field,
        k,
        query_vec: Vec::new(),
        param_name,
        k_param,
        ef_runtime,
        yield_as,
    }
}

/// Parses `@field:[VECTOR_RANGE <radius|$param> $blob]` and an optional trailing
/// `=>{$EPSILON: ...; $YIELD_DISTANCE_AS: alias}` attribute block.
fn parse_vector_range_clause(field: &str, inside_brackets: &str, trailer: &str) -> QueryAst {
    let tokens: Vec<&str> = inside_brackets.split_whitespace().collect();
    // tokens[0] == "VECTOR_RANGE"
    let (radius, radius_param) = match tokens.get(1) {
        Some(t) if t.starts_with('$') => (0.0, Some(t.to_string())),
        Some(t) => (t.parse().unwrap_or(0.0), None),
        None => (0.0, None),
    };
    let param_name = tokens
        .get(2)
        .map(|s| s.trim_start_matches('$').to_string())
        .unwrap_or_default();
    let mut epsilon = None;
    let mut yield_as = None;
    let mut k = 3;
    while k + 1 < tokens.len() {
        match tokens[k]
            .trim_start_matches('$')
            .to_ascii_uppercase()
            .as_str()
        {
            "EPSILON" => epsilon = Some(tokens[k + 1].to_string()),
            "AS" | "YIELD_DISTANCE_AS" => yield_as = Some(tokens[k + 1].to_string()),
            _ => {}
        }
        k += 2;
    }
    if let Some(body) = trailer
        .trim()
        .strip_prefix("=>")
        .map(str::trim)
        .and_then(|s| s.strip_prefix('{'))
        .and_then(|s| s.split_once('}').map(|(b, _)| b))
    {
        for attr in body.split(';') {
            if let Some((name, val)) = attr.split_once(':') {
                let name = name.trim().trim_start_matches('$').to_ascii_uppercase();
                let val = val.trim().to_string();
                match name.as_str() {
                    "EPSILON" => epsilon = Some(val),
                    "YIELD_DISTANCE_AS" | "AS" => yield_as = Some(val),
                    _ => {}
                }
            }
        }
    }
    QueryAst::VectorRange {
        field: field.to_string(),
        radius,
        radius_param,
        param_name,
        epsilon,
        yield_as,
    }
}

pub fn parse_query(q: &str) -> QueryAst {
    let q = strip_outer_parens(q);
    if q == "*" || q.is_empty() {
        return QueryAst::MatchAll;
    }

    // KNN vector query syntax: "*=>[KNN 10 @vec $param]" or "(query)=>[KNN $K @vec $p AS d]"
    if let Some((base_part, knn_part)) = q.split_once("=>[KNN") {
        let base = strip_outer_parens(base_part);
        let base_ast = if base == "*" || base.is_empty() {
            QueryAst::MatchAll
        } else {
            parse_query(base)
        };

        if let Some((args_part, trailer)) = knn_part.split_once(']') {
            let knn_ast = parse_knn_clause(args_part, trailer);
            return QueryAst::And(vec![base_ast, knn_ast]);
        }
    }

    let mut terms = Vec::new();
    let words: Vec<&str> = q.split_whitespace().collect();

    let mut i = 0;
    while i < words.len() {
        let w = words[i];
        if w.starts_with('-') && w.len() > 1 {
            let inner = parse_query(&w[1..]);
            terms.push(QueryAst::Not(Box::new(inner)));
        } else if w.starts_with('@') && w.contains(':') {
            let (field, rest) = w[1..].split_once(':').unwrap();
            if let Some(stripped) = rest.strip_prefix('[') {
                // Numeric range `@price:[10 100]` or vector range `@vec:[VECTOR_RANGE r $blob]`
                let mut range_str = stripped.to_string();
                while !range_str.contains(']') && i + 1 < words.len() {
                    i += 1;
                    range_str.push(' ');
                    range_str.push_str(words[i]);
                }
                let (inside, after_bracket) = range_str
                    .split_once(']')
                    .map(|(a, b)| (a.to_string(), b.to_string()))
                    .unwrap_or((range_str, String::new()));
                let mut trailer = after_bracket;
                if (trailer.starts_with("=>")
                    || (trailer.is_empty()
                        && i + 1 < words.len()
                        && words[i + 1].starts_with("=>")))
                    && !trailer.contains('}')
                {
                    while i + 1 < words.len() {
                        i += 1;
                        if !trailer.is_empty() {
                            trailer.push(' ');
                        }
                        trailer.push_str(words[i]);
                        if words[i].contains('}') {
                            break;
                        }
                    }
                }
                let parts: Vec<&str> = inside.split_whitespace().collect();
                if parts
                    .first()
                    .is_some_and(|p| p.eq_ignore_ascii_case("VECTOR_RANGE"))
                {
                    terms.push(parse_vector_range_clause(field, &inside, &trailer));
                } else {
                    let min = parts
                        .first()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(f64::NEG_INFINITY);
                    let max = parts
                        .get(1)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(f64::INFINITY);
                    terms.push(QueryAst::NumericRange {
                        field: field.to_string(),
                        min,
                        max,
                    });
                }
            } else if let Some(stripped) = rest.strip_prefix('{') {
                // Tag filter: @category:{electronics | books}
                let mut tag_str = stripped.to_string();
                if !tag_str.contains('}') {
                    while i + 1 < words.len() {
                        i += 1;
                        tag_str.push(' ');
                        tag_str.push_str(words[i]);
                        if words[i].ends_with('}') {
                            break;
                        }
                    }
                }
                let clean = tag_str.trim_end_matches('}');
                let tags: Vec<String> = clean
                    .split('|')
                    .map(|t| t.trim().to_ascii_lowercase())
                    .filter(|t| !t.is_empty())
                    .collect();
                terms.push(QueryAst::TagFilter {
                    field: field.to_string(),
                    tags,
                });
            } else {
                let inner = parse_query(rest);
                terms.push(QueryAst::FieldScope {
                    field: field.to_string(),
                    inner: Box::new(inner),
                });
            }
        } else if w == "|" {
            // OR operator encountered
            if i + 1 < words.len() {
                let left = if terms.is_empty() {
                    QueryAst::MatchAll
                } else if terms.len() == 1 {
                    terms.pop().unwrap()
                } else {
                    QueryAst::And(std::mem::take(&mut terms))
                };
                let right = parse_query(&words[i + 1..].join(" "));
                return QueryAst::Or(vec![left, right]);
            }
        } else if w.ends_with('*') && w.len() > 1 {
            terms.push(QueryAst::Prefix(w[..w.len() - 1].to_ascii_lowercase()));
        } else {
            let clean = if w.starts_with('"') && w.ends_with('"') && w.len() >= 2 {
                &w[1..w.len() - 1]
            } else {
                w
            };
            let sub_tokens = tokenize_text(clean, true);
            if sub_tokens.len() > 1 {
                terms.push(QueryAst::And(
                    sub_tokens.into_iter().map(QueryAst::Term).collect(),
                ));
            } else if sub_tokens.len() == 1 {
                terms.push(QueryAst::Term(sub_tokens[0].clone()));
            } else {
                let stemmed = simple_stem(clean);
                terms.push(QueryAst::Term(stemmed));
            }
        }
        i += 1;
    }

    if terms.is_empty() {
        QueryAst::MatchAll
    } else if terms.len() == 1 {
        terms.pop().unwrap()
    } else {
        QueryAst::And(terms)
    }
}

#[derive(Debug, Clone)]
pub struct SearchHit {
    pub doc_id: String,
    pub score: f64,
    pub sort_val: Option<f64>,
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchOptions {
    pub offset: usize,
    pub limit: usize,
    pub nocontent: bool,
    pub withscores: bool,
    pub rrf_k: Option<f64>,
    pub linear_weights: Option<(f64, f64)>,
    pub sortby: Option<(String, bool)>, // (field, ascending)
    pub return_fields: Option<Vec<String>>,
    pub params: HashMap<String, Vec<u8>>,
    pub dialect: Option<u32>,
    pub timeout_ms: Option<u64>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 10,
            nocontent: false,
            withscores: false,
            rrf_k: None,
            linear_weights: None,
            sortby: None,
            return_fields: None,
            params: HashMap::new(),
            dialect: None,
            timeout_ms: None,
        }
    }
}

fn evaluate_ast(
    index: &InvertedIndex,
    ast: &QueryAst,
    opts: &SearchOptions,
) -> HashMap<DocId, f64> {
    match ast {
        QueryAst::MatchAll => {
            let mut map = HashMap::with_capacity(index.id_to_meta.len());
            for &doc_id in index.id_to_meta.keys() {
                map.insert(doc_id, 1.0);
            }
            map
        }
        QueryAst::Term(t) => {
            let mut map = HashMap::new();
            if let Some(postings) = index.inverted.get(t) {
                for p in postings {
                    if let Some(doc) = index.id_to_meta.get(&p.doc_id) {
                        let score = index.bm25_score(t, p, doc.doc_len);
                        map.insert(p.doc_id, score);
                    }
                }
            }
            map
        }
        QueryAst::Prefix(pfx) => {
            let mut map = HashMap::new();
            for (term, postings) in &index.inverted {
                if term.starts_with(pfx) {
                    for p in postings {
                        if let Some(doc) = index.id_to_meta.get(&p.doc_id) {
                            let score = index.bm25_score(term, p, doc.doc_len);
                            *map.entry(p.doc_id).or_default() += score;
                        }
                    }
                }
            }
            map
        }
        QueryAst::Exact(exact_term) => {
            let mut map = HashMap::new();
            if let Some(postings) = index.inverted.get(exact_term) {
                for p in postings {
                    if let Some(doc) = index.id_to_meta.get(&p.doc_id) {
                        let score = index.bm25_score(exact_term, p, doc.doc_len);
                        map.insert(p.doc_id, score);
                    }
                }
            }
            map
        }
        QueryAst::FieldScope { field, inner } => {
            let sub_scores = evaluate_ast(index, inner, opts);
            let mut map = HashMap::new();
            for (doc_id, score) in sub_scores {
                if let Some(doc) = index.id_to_meta.get(&doc_id)
                    && (doc.fields.contains_key(field)
                        || field
                            .strip_prefix("$.")
                            .is_some_and(|f| doc.fields.contains_key(f)))
                {
                    map.insert(doc_id, score);
                }
            }
            map
        }
        QueryAst::NumericRange { field, min, max } => {
            let mut map = HashMap::new();
            if let Some(tree) = get_numeric_tree(index, field) {
                for doc_id in tree.range(*min, *max) {
                    map.insert(doc_id, 1.0);
                }
            } else {
                for (&doc_id, doc) in &index.id_to_meta {
                    let num_val = doc.numeric_fields.get(field).copied().or_else(|| {
                        field
                            .strip_prefix("$.")
                            .and_then(|f| doc.numeric_fields.get(f).copied())
                    });
                    if let Some(val) = num_val
                        && val >= *min
                        && val <= *max
                    {
                        map.insert(doc_id, 1.0);
                    }
                }
            }
            map
        }
        QueryAst::TagFilter { field, tags } => {
            let mut map = HashMap::new();
            for (&doc_id, doc) in &index.id_to_meta {
                let tags_opt = doc
                    .tag_fields
                    .get(field)
                    .or_else(|| field.strip_prefix("$.").and_then(|f| doc.tag_fields.get(f)));
                if let Some(doc_tags) = tags_opt {
                    let matched = tags.iter().any(|t| doc_tags.contains(t));
                    if matched {
                        map.insert(doc_id, 1.0);
                    }
                }
            }
            map
        }
        QueryAst::And(sub_asts) => {
            if sub_asts.is_empty() {
                return HashMap::new();
            }
            // Hybrid query: evaluate the non-vector filter first and restrict the KNN /
            // VECTOR_RANGE search to it (pre-filtering).
            if let Some(vec_pos) = sub_asts.iter().position(|s| {
                matches!(s, QueryAst::KnnVector { .. } | QueryAst::VectorRange { .. })
            }) {
                let others: Vec<&QueryAst> = sub_asts
                    .iter()
                    .enumerate()
                    .filter(|(i, s)| *i != vec_pos && !matches!(s, QueryAst::MatchAll))
                    .map(|(_, s)| s)
                    .collect();
                let eval_vec = |f: Option<&HashMap<DocId, f64>>| match &sub_asts[vec_pos] {
                    QueryAst::KnnVector { .. } => knn_search(index, &sub_asts[vec_pos], opts, f),
                    QueryAst::VectorRange { .. } => {
                        vector_range_search(index, &sub_asts[vec_pos], opts, f)
                    }
                    _ => HashMap::new(),
                };
                if others.is_empty() {
                    return eval_vec(None);
                }
                let mut filter = evaluate_ast(index, others[0], opts);
                for sub in &others[1..] {
                    if filter.is_empty() {
                        break;
                    }
                    let sub_map = evaluate_ast(index, sub, opts);
                    filter.retain(|id, _| sub_map.contains_key(id));
                }
                if filter.is_empty() {
                    return HashMap::new();
                }
                return eval_vec(Some(&filter));
            }
            let mut current = evaluate_ast(index, &sub_asts[0], opts);
            for sub in &sub_asts[1..] {
                if current.is_empty() {
                    break;
                }
                let sub_map = evaluate_ast(index, sub, opts);
                current.retain(|id, score| {
                    if let Some(sub_score) = sub_map.get(id) {
                        *score += *sub_score;
                        true
                    } else {
                        false
                    }
                });
            }
            current
        }
        QueryAst::Or(sub_asts) => {
            let mut map = HashMap::new();
            for sub in sub_asts {
                let sub_map = evaluate_ast(index, sub, opts);
                for (id, score) in sub_map {
                    *map.entry(id).or_default() += score;
                }
            }
            map
        }
        QueryAst::Not(sub) => {
            let excluded = evaluate_ast(index, sub, opts);
            let mut map = HashMap::new();
            for &doc_id in index.id_to_meta.keys() {
                if !excluded.contains_key(&doc_id) {
                    map.insert(doc_id, 1.0);
                }
            }
            map
        }
        QueryAst::KnnVector { .. } => knn_search(index, ast, opts, None),
        QueryAst::VectorRange { .. } => vector_range_search(index, ast, opts, None),
    }
}

/// Fraction of the index below which a filtered KNN query switches from filtered HNSW
/// traversal to exact brute force over the filtered set (RediSearch "ADHOC_BF" heuristic).
const KNN_ADHOC_BF_RATIO: f64 = 0.1;

fn distance_to_similarity(metric: crate::vector::VectorMetric, dist: f32) -> f64 {
    match metric {
        crate::vector::VectorMetric::Cosine => (1.0 - dist).max(0.0) as f64,
        _ => (1.0 / (1.0 + dist.max(0.0))) as f64,
    }
}

/// Resolves a vector index key (`doc_key` or `doc_key\x00chunk_idx`) back to its parent `DocId`.
#[inline]
fn resolve_vec_key_doc_id(index: &InvertedIndex, vec_key: &Bytes) -> Option<DocId> {
    if let Some(&id) = index.key_to_id.get(vec_key) {
        return Some(id);
    }
    if let Some(pos) = vec_key.iter().position(|&b| b == 0) {
        return index.key_to_id.get(&vec_key[..pos]).copied();
    }
    None
}

/// Computes the minimum vector distance across all indexed chunks of `doc` for `alias`.
fn min_doc_vector_distance(
    doc: &DocMeta,
    alias: &str,
    query: &[f32],
    metric: crate::vector::VectorMetric,
) -> Option<f32> {
    if let Some(chunks) = doc.multi_vector_fields.get(alias) {
        let mut best: Option<f32> = None;
        for v in chunks {
            if v.len() == query.len() {
                let d = crate::vector::compute_distance(query, v, metric);
                best = Some(best.map_or(d, |b| b.min(d)));
            }
        }
        if best.is_some() {
            return best;
        }
    }
    let v = doc.vector_fields.get(alias)?;
    (v.len() == query.len()).then(|| crate::vector::compute_distance(query, v, metric))
}

/// Deduplicates `(DocId, distance)` pairs by keeping the minimum distance per parent document.
fn dedup_min_distance(raw: impl IntoIterator<Item = (DocId, f32)>) -> Vec<(DocId, f32)> {
    let mut best: HashMap<DocId, f32> = HashMap::new();
    for (id, d) in raw {
        best.entry(id)
            .and_modify(|cur| {
                if d < *cur {
                    *cur = d;
                }
            })
            .or_insert(d);
    }
    let mut out: Vec<(DocId, f32)> = best.into_iter().collect();
    out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    out
}

/// Executes a `@field:[VECTOR_RANGE radius $blob]` clause, optionally restricted by `filter`.
fn vector_range_search(
    index: &InvertedIndex,
    vr: &QueryAst,
    opts: &SearchOptions,
    filter: Option<&HashMap<DocId, f64>>,
) -> HashMap<DocId, f64> {
    let QueryAst::VectorRange {
        field,
        radius,
        radius_param,
        param_name,
        epsilon,
        ..
    } = vr
    else {
        return HashMap::new();
    };
    let effective_radius = radius_param
        .as_deref()
        .and_then(|p| resolve_f32_param(p, opts))
        .unwrap_or(*radius);
    let eps = epsilon.as_deref().and_then(|e| resolve_f32_param(e, opts));
    let effective_vec = knn_query_vector(index, field, &[], param_name, opts);
    let mut map = HashMap::new();
    if effective_vec.is_empty() || effective_radius < 0.0 {
        return map;
    }
    let sf = index.resolve_vector_field(field);
    let alias = sf.map(|s| s.alias.as_str()).unwrap_or(field.as_str());
    let vi_opt = index.vector_indices.get(alias);
    let metric = vi_opt
        .map(|vi| vi.metric())
        .or_else(|| match sf.map(|s| &s.field_type) {
            Some(FieldType::Vector {
                distance_metric, ..
            }) => Some(metric_from_str(distance_metric)),
            _ => None,
        })
        .unwrap_or(crate::vector::VectorMetric::Cosine);

    let brute_force_range = |ids: &mut dyn Iterator<Item = DocId>| -> Vec<(DocId, f32)> {
        ids.filter_map(|id| {
            let doc = index.id_to_meta.get(&id)?;
            let d = min_doc_vector_distance(doc, alias, &effective_vec, metric)?;
            (d <= effective_radius).then_some((id, d))
        })
        .collect()
    };

    let results: Vec<(DocId, f32)> = match (vi_opt, filter) {
        (None, None) => brute_force_range(&mut index.id_to_meta.keys().copied()),
        (None, Some(f)) => brute_force_range(&mut f.keys().copied()),
        (Some(vi), None) => dedup_min_distance(
            vi.range_filtered(&effective_vec, effective_radius, eps, None)
                .into_iter()
                .filter_map(|(key, d)| resolve_vec_key_doc_id(index, &key).map(|id| (id, d))),
        ),
        (Some(vi), Some(f)) => {
            let pred = |key: &Bytes| {
                resolve_vec_key_doc_id(index, key).is_some_and(|id| f.contains_key(&id))
            };
            dedup_min_distance(
                vi.range_filtered(&effective_vec, effective_radius, eps, Some(&pred))
                    .into_iter()
                    .filter_map(|(key, d)| resolve_vec_key_doc_id(index, &key).map(|id| (id, d))),
            )
        }
    };
    for (id, d) in results {
        map.insert(id, distance_to_similarity(metric, d));
    }
    map
}

/// Executes a KNN clause, optionally restricted to the documents of `filter`.
fn knn_search(
    index: &InvertedIndex,
    knn: &QueryAst,
    opts: &SearchOptions,
    filter: Option<&HashMap<DocId, f64>>,
) -> HashMap<DocId, f64> {
    let QueryAst::KnnVector {
        field,
        query_vec,
        param_name,
        ef_runtime,
        ..
    } = knn
    else {
        return HashMap::new();
    };
    let effective_vec = knn_query_vector(index, field, query_vec, param_name, opts);
    let k = knn.knn_k(opts).unwrap_or(10);
    let ef = ef_runtime
        .as_deref()
        .and_then(|e| resolve_usize_param(e, opts));
    let mut map = HashMap::new();
    if effective_vec.is_empty() || k == 0 {
        return map;
    }
    let sf = index.resolve_vector_field(field);
    let alias = sf.map(|s| s.alias.as_str()).unwrap_or(field.as_str());
    let vi_opt = index.vector_indices.get(alias);
    let metric = vi_opt
        .map(|vi| vi.metric())
        .or_else(|| match sf.map(|s| &s.field_type) {
            Some(FieldType::Vector {
                distance_metric, ..
            }) => Some(metric_from_str(distance_metric)),
            _ => None,
        })
        .unwrap_or(crate::vector::VectorMetric::Cosine);

    let has_multi_chunks = index.id_to_meta.values().any(|m| {
        m.multi_vector_fields
            .get(alias)
            .is_some_and(|c| c.len() > 1)
    });
    let k_fetch = if has_multi_chunks {
        k.saturating_mul(4).max(k)
    } else {
        k
    };

    let brute_force = |ids: &mut dyn Iterator<Item = DocId>| -> Vec<(DocId, f32)> {
        let mut dists: Vec<(DocId, f32)> = ids
            .filter_map(|id| {
                let doc = index.id_to_meta.get(&id)?;
                min_doc_vector_distance(doc, alias, &effective_vec, metric).map(|d| (id, d))
            })
            .collect();
        dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        dists.truncate(k);
        dists
    };

    let results: Vec<(DocId, f32)> = match (vi_opt, filter) {
        (None, None) => brute_force(&mut index.id_to_meta.keys().copied()),
        (None, Some(f)) => brute_force(&mut f.keys().copied()),
        (Some(vi), None) => {
            let mut deduped = dedup_min_distance(
                vi.search(&effective_vec, k_fetch, ef)
                    .into_iter()
                    .filter_map(|(key, d)| resolve_vec_key_doc_id(index, &key).map(|id| (id, d))),
            );
            deduped.truncate(k);
            deduped
        }
        (Some(vi), Some(f)) => {
            let small = (f.len() as f64) <= (vi.len() as f64 * KNN_ADHOC_BF_RATIO).max(k as f64);
            if small || matches!(vi, crate::vector::VectorFieldIndex::Flat(_)) {
                brute_force(&mut f.keys().copied())
            } else {
                let pred = |key: &Bytes| {
                    resolve_vec_key_doc_id(index, key).is_some_and(|id| f.contains_key(&id))
                };
                let mut found = dedup_min_distance(
                    vi.search_filtered(&effective_vec, k_fetch, ef, Some(&pred))
                        .into_iter()
                        .filter_map(|(key, d)| {
                            resolve_vec_key_doc_id(index, &key).map(|id| (id, d))
                        }),
                );
                found.truncate(k);
                if found.len() < k.min(f.len()) {
                    // The filtered traversal could not reach enough matching nodes (e.g. the
                    // filtered set lives in a poorly connected region): fall back to exact BF.
                    brute_force(&mut f.keys().copied())
                } else {
                    found
                }
            }
        }
    };
    for (id, d) in results {
        map.insert(id, distance_to_similarity(metric, d));
    }
    map
}

/// Resolves the query vector of a KNN clause: inline vector, or `$param` decoded with the
/// target field's `TYPE` and `DIM`.
fn knn_query_vector(
    index: &InvertedIndex,
    field: &str,
    query_vec: &[f32],
    param_name: &str,
    opts: &SearchOptions,
) -> Vec<f32> {
    if !query_vec.is_empty() {
        return query_vec.to_vec();
    }
    if param_name.is_empty() {
        return Vec::new();
    }
    let Some(bytes) = opts
        .params
        .get(param_name)
        .or_else(|| opts.params.get(&format!("${}", param_name)))
    else {
        return Vec::new();
    };
    match index.resolve_vector_field(field).map(|sf| &sf.field_type) {
        Some(FieldType::Vector { dim, attrs, .. }) => {
            decode_vector(bytes, attrs.data_type, *dim).unwrap_or_default()
        }
        _ => parse_vector_blob(bytes),
    }
}

/// Computes the exact vector distance of each candidate document for the query's KNN or
/// `VECTOR_RANGE` clause.
fn knn_distances(
    index: &InvertedIndex,
    ast: &QueryAst,
    opts: &SearchOptions,
    candidates: &HashMap<DocId, f64>,
) -> Option<HashMap<DocId, f32>> {
    let (field, query_vec, param_name): (&str, &[f32], &str) = match ast.knn_clause()? {
        QueryAst::KnnVector {
            field,
            query_vec,
            param_name,
            ..
        } => (field, query_vec, param_name),
        QueryAst::VectorRange {
            field, param_name, ..
        } => (field, &[], param_name),
        _ => return None,
    };
    let sf = index.resolve_vector_field(field);
    let alias = sf.map(|s| s.alias.as_str()).unwrap_or(field);
    let metric = index
        .vector_indices
        .get(alias)
        .map(|vi| vi.metric())
        .or_else(|| match sf.map(|s| &s.field_type) {
            Some(FieldType::Vector {
                distance_metric, ..
            }) => Some(metric_from_str(distance_metric)),
            _ => None,
        })
        .unwrap_or(crate::vector::VectorMetric::Cosine);
    let query = knn_query_vector(index, field, query_vec, param_name, opts);
    let mut out = HashMap::with_capacity(candidates.len());
    if query.is_empty() {
        return Some(out);
    }
    for doc_id in candidates.keys() {
        if let Some(doc) = index.id_to_meta.get(doc_id)
            && let Some(d) = min_doc_vector_distance(doc, alias, &query, metric)
        {
            out.insert(*doc_id, d);
        }
    }
    Some(out)
}

/// Formats a KNN distance the way RediSearch does (shortest round-trip representation).
pub fn format_distance(d: f32) -> String {
    let d = if d == 0.0 { 0.0 } else { d };
    format!("{}", d)
}

pub fn execute_search(
    index: &InvertedIndex,
    ast: &QueryAst,
    opts: &SearchOptions,
) -> (usize, Vec<SearchHit>) {
    if (opts.rrf_k.is_some() || opts.linear_weights.is_some())
        && let Some((bm25_ast, knn_ast)) = ast.as_hybrid_rrf()
    {
        let unpaged_opts = SearchOptions {
            offset: 0,
            limit: usize::MAX,
            rrf_k: None,
            linear_weights: None,
            ..opts.clone()
        };
        let (_bm25_total, bm25_hits) = execute_search(index, &bm25_ast, &unpaged_opts);
        let (_knn_total, vector_hits) = execute_search(index, &knn_ast, &unpaged_opts);
        let fused = if let Some((alpha, beta)) = opts.linear_weights {
            linear_score_fusion(&bm25_hits, &vector_hits, alpha, beta)
        } else {
            reciprocal_rank_fusion(&bm25_hits, &vector_hits, opts.rrf_k.unwrap_or(60.0))
        };
        let total = fused.len();
        let paged = fused
            .into_iter()
            .skip(opts.offset)
            .take(opts.limit)
            .collect();
        return (total, paged);
    }

    let candidate_scores = evaluate_ast(index, ast, opts);
    let total_matches = candidate_scores.len();

    let knn_dists = knn_distances(index, ast, opts, &candidate_scores);
    let knn_field = ast.knn_score_field();
    let sort_by_knn = knn_dists.is_some()
        && match &opts.sortby {
            None => true,
            Some((f, _)) => Some(f) == knn_field.as_ref(),
        };
    let knn_asc = match &opts.sortby {
        Some((_, asc)) if sort_by_knn => *asc,
        _ => true,
    };
    let dist_of = |id: &DocId| -> f32 {
        knn_dists
            .as_ref()
            .and_then(|m| m.get(id).copied())
            .unwrap_or(f32::INFINITY)
    };

    // 2. Sort results
    let mut scored_docs: Vec<(DocId, f64)> = candidate_scores.into_iter().collect();

    if sort_by_knn {
        scored_docs.sort_by(|a, b| {
            let ord = dist_of(&a.0)
                .partial_cmp(&dist_of(&b.0))
                .unwrap_or(std::cmp::Ordering::Equal);
            if knn_asc { ord } else { ord.reverse() }
        });
    } else if let Some((sort_field, asc)) = &opts.sortby {
        scored_docs.sort_by(|a, b| {
            let doc_a = index.id_to_meta.get(&a.0);
            let doc_b = index.id_to_meta.get(&b.0);
            let val_a = doc_a
                .and_then(|d| {
                    d.numeric_fields.get(sort_field).copied().or_else(|| {
                        sort_field
                            .strip_prefix("$.")
                            .and_then(|f| d.numeric_fields.get(f).copied())
                    })
                })
                .unwrap_or(f64::NEG_INFINITY);
            let val_b = doc_b
                .and_then(|d| {
                    d.numeric_fields.get(sort_field).copied().or_else(|| {
                        sort_field
                            .strip_prefix("$.")
                            .and_then(|f| d.numeric_fields.get(f).copied())
                    })
                })
                .unwrap_or(f64::NEG_INFINITY);
            if *asc {
                val_a
                    .partial_cmp(&val_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            } else {
                val_b
                    .partial_cmp(&val_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        });
    } else {
        // Default sort by BM25 relevance score descending
        scored_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    }

    // 3. Paginate
    let paged = scored_docs.into_iter().skip(opts.offset).take(opts.limit);

    // 4. Construct SearchHit results
    let mut hits = Vec::new();
    for (doc_id, mut score) in paged {
        let mut fields = HashMap::new();
        if let Some(doc) = index.id_to_meta.get(&doc_id) {
            let mut sort_val = None;
            let dist = knn_dists.as_ref().and_then(|m| m.get(&doc_id).copied());
            if sort_by_knn {
                // Encode distance so that the cross-shard merge (score desc / sort_val) keeps
                // the nearest neighbours first.
                let d = dist.unwrap_or(f32::INFINITY) as f64;
                score = -d;
                if opts.sortby.is_some() {
                    sort_val = Some(d);
                }
            } else if let Some((sort_field, _)) = &opts.sortby {
                sort_val = doc.numeric_fields.get(sort_field).copied().or_else(|| {
                    sort_field
                        .strip_prefix("$.")
                        .and_then(|f| doc.numeric_fields.get(f).copied())
                });
            }
            if !opts.nocontent {
                if let Some(ret_fields) = &opts.return_fields {
                    for rf in ret_fields {
                        if let Some(v) = doc.fields.get(rf) {
                            fields.insert(rf.clone(), v.clone());
                        } else if let Some(stripped) = rf.strip_prefix("$.")
                            && let Some(v) = doc.fields.get(stripped)
                        {
                            fields.insert(rf.clone(), v.clone());
                        }
                    }
                } else {
                    fields = doc.fields.clone();
                }
                if let (Some(d), Some(kf)) = (dist, &knn_field) {
                    let wanted = opts
                        .return_fields
                        .as_ref()
                        .is_none_or(|r| r.iter().any(|f| f == kf));
                    if wanted {
                        fields.insert(kf.clone(), format_distance(d));
                    }
                }
            }
            hits.push(SearchHit {
                doc_id: String::from_utf8_lossy(&doc.key).to_string(),
                score,
                sort_val,
                fields,
            });
        }
    }

    (total_matches, hits)
}

// Reciprocal Rank Fusion (RRF) for Hybrid Keyword + Vector Retrieval
pub fn reciprocal_rank_fusion(
    bm25_hits: &[SearchHit],
    vector_hits: &[SearchHit],
    k: f64,
) -> Vec<SearchHit> {
    let mut rrf_scores: HashMap<String, f64> = HashMap::new();
    let mut doc_map: HashMap<String, HashMap<String, String>> = HashMap::new();

    for (rank, hit) in bm25_hits.iter().enumerate() {
        let rrf = 1.0 / (k + (rank as f64) + 1.0);
        *rrf_scores.entry(hit.doc_id.clone()).or_default() += rrf;
        doc_map
            .entry(hit.doc_id.clone())
            .or_insert_with(|| hit.fields.clone());
    }

    for (rank, hit) in vector_hits.iter().enumerate() {
        let rrf = 1.0 / (k + (rank as f64) + 1.0);
        *rrf_scores.entry(hit.doc_id.clone()).or_default() += rrf;
        doc_map
            .entry(hit.doc_id.clone())
            .or_insert_with(|| hit.fields.clone());
    }

    let mut merged: Vec<SearchHit> = rrf_scores
        .into_iter()
        .map(|(doc_id, score)| SearchHit {
            fields: doc_map.remove(&doc_id).unwrap_or_default(),
            doc_id,
            score,
            sort_val: None,
        })
        .collect();

    merged.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    merged
}

/// Convex linear combination of min-max normalized BM25 and vector similarity scores:
/// `score = alpha * norm_bm25 + beta * norm_vec`.
pub fn linear_score_fusion(
    bm25_hits: &[SearchHit],
    vector_hits: &[SearchHit],
    alpha: f64,
    beta: f64,
) -> Vec<SearchHit> {
    let mut linear_scores: HashMap<String, f64> = HashMap::new();
    let mut doc_map: HashMap<String, HashMap<String, String>> = HashMap::new();

    if !bm25_hits.is_empty() {
        let min_b = bm25_hits
            .iter()
            .map(|h| h.score)
            .fold(f64::INFINITY, f64::min);
        let max_b = bm25_hits
            .iter()
            .map(|h| h.score)
            .fold(f64::NEG_INFINITY, f64::max);
        let span_b = max_b - min_b;
        for hit in bm25_hits {
            let norm = if span_b > 1e-12 {
                (hit.score - min_b) / span_b
            } else {
                1.0
            };
            *linear_scores.entry(hit.doc_id.clone()).or_default() += alpha * norm;
            doc_map
                .entry(hit.doc_id.clone())
                .or_insert_with(|| hit.fields.clone());
        }
    }

    if !vector_hits.is_empty() {
        // `execute_search` encodes KNN score as `-distance` (<= 0.0); convert to similarity in (0, 1].
        let sims: Vec<f64> = vector_hits
            .iter()
            .map(|h| {
                if h.score <= 0.0 {
                    1.0 / (1.0 + (-h.score))
                } else {
                    h.score
                }
            })
            .collect();
        let min_v = sims.iter().copied().fold(f64::INFINITY, f64::min);
        let max_v = sims.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let span_v = max_v - min_v;
        for (hit, sim) in vector_hits.iter().zip(sims) {
            let norm = if span_v > 1e-12 {
                (sim - min_v) / span_v
            } else {
                sim
            };
            *linear_scores.entry(hit.doc_id.clone()).or_default() += beta * norm;
            doc_map
                .entry(hit.doc_id.clone())
                .and_modify(|existing| {
                    for (k, v) in &hit.fields {
                        existing.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                })
                .or_insert_with(|| hit.fields.clone());
        }
    }

    let mut merged: Vec<SearchHit> = linear_scores
        .into_iter()
        .map(|(doc_id, score)| SearchHit {
            fields: doc_map.remove(&doc_id).unwrap_or_default(),
            doc_id,
            score,
            sort_val: None,
        })
        .collect();

    merged.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    merged
}

#[derive(Debug, Clone, PartialEq)]
pub enum Reducer {
    Count { alias: String },
    Sum { field: String, alias: String },
    Avg { field: String, alias: String },
    Min { field: String, alias: String },
    Max { field: String, alias: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupByStage {
    pub fields: Vec<String>,
    pub reducers: Vec<Reducer>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApplyStage {
    pub expr: String,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortByStage {
    pub fields: Vec<(String, bool)>, // (field, ascending)
    pub max: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateStage {
    Group(GroupByStage),
    Apply(ApplyStage),
    Sort(SortByStage),
    Limit { offset: usize, num: usize },
    Filter(String),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AggregateOptions {
    pub load_fields: Vec<String>,
    pub stages: Vec<AggregateStage>,
}

pub type AggregateRow = Vec<(String, String)>;

pub fn get_row_field<'a>(row: &'a [(String, String)], name: &str) -> Option<&'a str> {
    let clean = name.strip_prefix('@').unwrap_or(name);
    for (k, v) in row {
        let k_clean = k.strip_prefix('@').unwrap_or(k);
        if k_clean == clean {
            return Some(v.as_str());
        }
    }
    None
}

pub fn set_row_field(row: &mut Vec<(String, String)>, name: String, val: String) {
    let clean = name.strip_prefix('@').unwrap_or(&name);
    for (k, v) in row.iter_mut() {
        let k_clean = k.strip_prefix('@').unwrap_or(k);
        if k_clean == clean {
            *v = val;
            return;
        }
    }
    row.push((name, val));
}

/// Simple recursive descent expression evaluator for arithmetic and field references
pub fn evaluate_expr(expr: &str, row: &[(String, String)]) -> Result<f64, String> {
    let tokens = tokenize_expr(expr);
    let mut pos = 0;
    let res = parse_expr_addition(&tokens, &mut pos, row)?;
    Ok(res)
}

#[derive(Debug, PartialEq)]
enum ExprToken {
    Num(f64),
    Field(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
}

fn tokenize_expr(s: &str) -> Vec<ExprToken> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\r' | '\n' => {
                i += 1;
            }
            '+' => {
                tokens.push(ExprToken::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(ExprToken::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(ExprToken::Star);
                i += 1;
            }
            '/' => {
                tokens.push(ExprToken::Slash);
                i += 1;
            }
            '(' => {
                tokens.push(ExprToken::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(ExprToken::RParen);
                i += 1;
            }
            '@' => {
                i += 1;
                let mut field = String::new();
                while i < chars.len()
                    && (chars[i].is_alphanumeric()
                        || chars[i] == '_'
                        || chars[i] == '.'
                        || chars[i] == ':')
                {
                    field.push(chars[i]);
                    i += 1;
                }
                tokens.push(ExprToken::Field(field));
            }
            c if c.is_ascii_digit() || c == '.' => {
                let mut num_str = String::new();
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    num_str.push(chars[i]);
                    i += 1;
                }
                if let Ok(val) = num_str.parse::<f64>() {
                    tokens.push(ExprToken::Num(val));
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                let mut ident = String::new();
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    ident.push(chars[i]);
                    i += 1;
                }
                tokens.push(ExprToken::Field(ident));
            }
            _ => {
                i += 1;
            }
        }
    }
    tokens
}

fn parse_expr_addition(
    tokens: &[ExprToken],
    pos: &mut usize,
    row: &[(String, String)],
) -> Result<f64, String> {
    let mut left = parse_expr_multiplication(tokens, pos, row)?;
    while *pos < tokens.len() {
        match tokens[*pos] {
            ExprToken::Plus => {
                *pos += 1;
                let right = parse_expr_multiplication(tokens, pos, row)?;
                left += right;
            }
            ExprToken::Minus => {
                *pos += 1;
                let right = parse_expr_multiplication(tokens, pos, row)?;
                left -= right;
            }
            _ => break,
        }
    }
    Ok(left)
}

fn parse_expr_multiplication(
    tokens: &[ExprToken],
    pos: &mut usize,
    row: &[(String, String)],
) -> Result<f64, String> {
    let mut left = parse_expr_primary(tokens, pos, row)?;
    while *pos < tokens.len() {
        match tokens[*pos] {
            ExprToken::Star => {
                *pos += 1;
                let right = parse_expr_primary(tokens, pos, row)?;
                left *= right;
            }
            ExprToken::Slash => {
                *pos += 1;
                let right = parse_expr_primary(tokens, pos, row)?;
                if right != 0.0 {
                    left /= right;
                } else {
                    left = 0.0;
                }
            }
            _ => break,
        }
    }
    Ok(left)
}

fn parse_expr_primary(
    tokens: &[ExprToken],
    pos: &mut usize,
    row: &[(String, String)],
) -> Result<f64, String> {
    if *pos >= tokens.len() {
        return Err("Unexpected end of expression".to_string());
    }
    match &tokens[*pos] {
        ExprToken::Num(n) => {
            let val = *n;
            *pos += 1;
            Ok(val)
        }
        ExprToken::Field(name) => {
            let field_val = get_row_field(row, name)
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            *pos += 1;
            Ok(field_val)
        }
        ExprToken::Minus => {
            *pos += 1;
            let val = parse_expr_primary(tokens, pos, row)?;
            Ok(-val)
        }
        ExprToken::LParen => {
            *pos += 1;
            let val = parse_expr_addition(tokens, pos, row)?;
            if *pos < tokens.len() && tokens[*pos] == ExprToken::RParen {
                *pos += 1;
            }
            Ok(val)
        }
        _ => Err("Invalid token in expression".to_string()),
    }
}

pub fn evaluate_filter(filter: &str, row: &[(String, String)]) -> bool {
    let parts: Vec<&str> = filter.split_whitespace().collect();
    if parts.len() < 3 {
        return true;
    }
    let field = parts[0].strip_prefix('@').unwrap_or(parts[0]);
    let op = parts[1];
    let raw_target = parts[2..].join(" ");
    let target = raw_target.trim_matches('\'').trim_matches('"');
    let val = get_row_field(row, field).unwrap_or("");

    match op {
        "==" | "=" => val == target,
        "!=" => val != target,
        ">" => {
            if let (Ok(v), Ok(t)) = (val.parse::<f64>(), target.parse::<f64>()) {
                v > t
            } else {
                val > target
            }
        }
        ">=" => {
            if let (Ok(v), Ok(t)) = (val.parse::<f64>(), target.parse::<f64>()) {
                v >= t
            } else {
                val >= target
            }
        }
        "<" => {
            if let (Ok(v), Ok(t)) = (val.parse::<f64>(), target.parse::<f64>()) {
                v < t
            } else {
                val < target
            }
        }
        "<=" => {
            if let (Ok(v), Ok(t)) = (val.parse::<f64>(), target.parse::<f64>()) {
                v <= t
            } else {
                val <= target
            }
        }
        _ => true,
    }
}

pub fn execute_aggregate_pipeline(
    hits: Vec<SearchHit>,
    options: &AggregateOptions,
) -> Vec<AggregateRow> {
    // 1. Initial rows
    let mut rows: Vec<AggregateRow> = hits
        .into_iter()
        .map(|hit| {
            let mut row = Vec::new();
            row.push(("__key".to_string(), hit.doc_id.clone()));
            row.push(("__score".to_string(), hit.score.to_string()));

            if options.load_fields.is_empty() {
                for (k, v) in hit.fields {
                    row.push((k, v));
                }
            } else {
                for f in &options.load_fields {
                    let clean = f.strip_prefix('@').unwrap_or(f);
                    if let Some(v) = hit.fields.get(clean) {
                        row.push((clean.to_string(), v.clone()));
                    } else if let Some(stripped) = clean.strip_prefix("$.")
                        && let Some(v) = hit.fields.get(stripped)
                    {
                        row.push((clean.to_string(), v.clone()));
                    }
                }
            }
            row
        })
        .collect();

    // 2. Execute pipeline stages sequentially
    for stage in &options.stages {
        match stage {
            AggregateStage::Filter(f) => {
                rows.retain(|r| evaluate_filter(f, r));
            }
            AggregateStage::Apply(apply) => {
                for r in &mut rows {
                    let computed = match evaluate_expr(&apply.expr, r) {
                        Ok(num) => {
                            if num.fract() == 0.0 {
                                (num as i64).to_string()
                            } else {
                                format!("{:.4}", num)
                                    .trim_end_matches('0')
                                    .trim_end_matches('.')
                                    .to_string()
                            }
                        }
                        Err(_) => {
                            let src_field = apply.expr.strip_prefix('@').unwrap_or(&apply.expr);
                            get_row_field(r, src_field).unwrap_or("").to_string()
                        }
                    };
                    set_row_field(r, apply.alias.clone(), computed);
                }
            }
            AggregateStage::Group(grp) => {
                let mut groups: HashMap<Vec<String>, Vec<AggregateRow>> = HashMap::new();
                for r in rows {
                    let group_key: Vec<String> = grp
                        .fields
                        .iter()
                        .map(|f| get_row_field(&r, f).unwrap_or("").to_string())
                        .collect();
                    groups.entry(group_key).or_default().push(r);
                }

                let mut new_rows = Vec::new();
                for (group_key, group_rows) in groups {
                    let mut new_row = Vec::new();
                    for (i, f) in grp.fields.iter().enumerate() {
                        new_row.push((f.clone(), group_key[i].clone()));
                    }
                    for red in &grp.reducers {
                        match red {
                            Reducer::Count { alias } => {
                                new_row.push((alias.clone(), group_rows.len().to_string()));
                            }
                            Reducer::Sum { field, alias } => {
                                let mut sum = 0.0;
                                for r in &group_rows {
                                    if let Some(v) = get_row_field(r, field)
                                        && let Ok(n) = v.parse::<f64>()
                                    {
                                        sum += n;
                                    }
                                }
                                let val_str = if sum.fract() == 0.0 {
                                    (sum as i64).to_string()
                                } else {
                                    sum.to_string()
                                };
                                new_row.push((alias.clone(), val_str));
                            }
                            Reducer::Avg { field, alias } => {
                                let mut sum = 0.0;
                                let mut count = 0;
                                for r in &group_rows {
                                    if let Some(v) = get_row_field(r, field)
                                        && let Ok(n) = v.parse::<f64>()
                                    {
                                        sum += n;
                                        count += 1;
                                    }
                                }
                                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                                let val_str = if avg.fract() == 0.0 {
                                    (avg as i64).to_string()
                                } else {
                                    format!("{:.2}", avg)
                                };
                                new_row.push((alias.clone(), val_str));
                            }
                            Reducer::Min { field, alias } => {
                                let mut min = f64::INFINITY;
                                for r in &group_rows {
                                    if let Some(v) = get_row_field(r, field)
                                        && let Ok(n) = v.parse::<f64>()
                                        && n < min
                                    {
                                        min = n;
                                    }
                                }
                                let val_str = if min.is_infinite() {
                                    "0".to_string()
                                } else if min.fract() == 0.0 {
                                    (min as i64).to_string()
                                } else {
                                    min.to_string()
                                };
                                new_row.push((alias.clone(), val_str));
                            }
                            Reducer::Max { field, alias } => {
                                let mut max = f64::NEG_INFINITY;
                                for r in &group_rows {
                                    if let Some(v) = get_row_field(r, field)
                                        && let Ok(n) = v.parse::<f64>()
                                        && n > max
                                    {
                                        max = n;
                                    }
                                }
                                let val_str = if max.is_infinite() {
                                    "0".to_string()
                                } else if max.fract() == 0.0 {
                                    (max as i64).to_string()
                                } else {
                                    max.to_string()
                                };
                                new_row.push((alias.clone(), val_str));
                            }
                        }
                    }
                    new_rows.push(new_row);
                }
                rows = new_rows;
            }
            AggregateStage::Sort(sort) => {
                rows.sort_by(|a, b| {
                    for (field, asc) in &sort.fields {
                        let va = get_row_field(a, field).unwrap_or("");
                        let vb = get_row_field(b, field).unwrap_or("");
                        let ord = if let (Ok(na), Ok(nb)) = (va.parse::<f64>(), vb.parse::<f64>()) {
                            na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal)
                        } else {
                            va.cmp(vb)
                        };
                        let final_ord = if *asc { ord } else { ord.reverse() };
                        if final_ord != std::cmp::Ordering::Equal {
                            return final_ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                if let Some(max_rows) = sort.max {
                    rows.truncate(max_rows);
                }
            }
            AggregateStage::Limit { offset, num } => {
                rows = rows.into_iter().skip(*offset).take(*num).collect();
            }
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25_inverted_index_crud() {
        let mut schema_fields = HashMap::new();
        schema_fields.insert(
            "title".to_string(),
            FieldType::Text {
                weight: 1.0,
                sortable: true,
                nostem: false,
            },
        );
        schema_fields.insert("price".to_string(), FieldType::Numeric { sortable: true });
        schema_fields.insert(
            "tags".to_string(),
            FieldType::Tag {
                separator: ',',
                casesensitive: false,
            },
        );

        let schema = IndexSchema {
            name: "idx:products".to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec!["product:".to_string()],
            fields: schema_fields,
            schema_fields: Vec::new(),
        };

        let mut idx = InvertedIndex::new(schema);

        let mut doc1 = HashMap::new();
        doc1.insert(
            "title".to_string(),
            "High performance Rust redis engine".to_string(),
        );
        doc1.insert("price".to_string(), "99.5".to_string());
        doc1.insert("tags".to_string(), "database, rust, cache".to_string());

        let mut doc2 = HashMap::new();
        doc2.insert(
            "title".to_string(),
            "Dragonfly memory store in C++".to_string(),
        );
        doc2.insert("price".to_string(), "49.0".to_string());
        doc2.insert("tags".to_string(), "database, memory".to_string());

        idx.add_document("product:1", doc1, None);
        idx.add_document("product:2", doc2, None);

        assert_eq!(idx.total_docs, 2);

        // Search for "rust"
        let ast1 = parse_query("rust");
        let (total, hits) = execute_search(&idx, &ast1, &SearchOptions::default());
        assert_eq!(total, 1);
        assert_eq!(hits[0].doc_id, "product:1");

        // Numeric range query: @price:[50 100]
        let ast2 = parse_query("@price:[50 100]");
        let (total, hits) = execute_search(&idx, &ast2, &SearchOptions::default());
        assert_eq!(total, 1);
        assert_eq!(hits[0].doc_id, "product:1");

        // Tag query: @tags:{memory}
        let ast3 = parse_query("@tags:{memory}");
        let (total, hits) = execute_search(&idx, &ast3, &SearchOptions::default());
        assert_eq!(total, 1);
        assert_eq!(hits[0].doc_id, "product:2");

        // Prefix query: "redi*"
        let ast4 = parse_query("redi*");
        let (total, hits) = execute_search(&idx, &ast4, &SearchOptions::default());
        assert_eq!(total, 1);
        assert_eq!(hits[0].doc_id, "product:1");
    }

    #[test]
    fn test_reciprocal_rank_fusion() {
        let h1 = SearchHit {
            doc_id: "doc1".to_string(),
            score: 10.0,
            sort_val: None,
            fields: HashMap::new(),
        };
        let h2 = SearchHit {
            doc_id: "doc2".to_string(),
            score: 8.0,
            sort_val: None,
            fields: HashMap::new(),
        };
        let h3 = SearchHit {
            doc_id: "doc3".to_string(),
            score: 6.0,
            sort_val: None,
            fields: HashMap::new(),
        };

        let bm25_hits = vec![h1, h2];
        let vec_hits = vec![h2_clone(&bm25_hits[1]), h3];

        let fused = reciprocal_rank_fusion(&bm25_hits, &vec_hits, 60.0);
        // doc2 appears in both lists, so its combined RRF should be highest!
        assert_eq!(fused[0].doc_id, "doc2");
    }

    #[test]
    fn test_knn_vector_search_with_params() {
        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldType::Text {
                weight: 1.0,
                sortable: false,
                nostem: false,
            },
        );
        fields.insert(
            "embedding".to_string(),
            FieldType::Vector {
                dim: 3,
                distance_metric: "COSINE".to_string(),
                algorithm: "FLAT".to_string(),
                attrs: VectorFieldAttrs::default(),
            },
        );
        let schema = IndexSchema {
            name: "test_vec_idx".to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec!["doc:".to_string()],
            fields,
            schema_fields: Vec::new(),
        };
        let mut idx = InvertedIndex::new(schema);

        let mut doc1 = HashMap::new();
        doc1.insert("title".to_string(), "Doc 1".to_string());
        doc1.insert("embedding".to_string(), "1.0, 0.0, 0.0".to_string());

        let mut doc2 = HashMap::new();
        doc2.insert("title".to_string(), "Doc 2".to_string());
        doc2.insert("embedding".to_string(), "0.0, 1.0, 0.0".to_string());

        idx.add_document("doc:1", doc1, None);
        idx.add_document("doc:2", doc2, None);

        let ast = parse_query("*=>[KNN 1 @embedding $q_vec]");
        let mut params = HashMap::new();
        // Query vector closest to doc:1 (1.0, 0.1, 0.0)
        let q_bytes: Vec<u8> = vec![1.0f32, 0.1f32, 0.0f32]
            .into_iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        params.insert("q_vec".to_string(), q_bytes);

        let opts = SearchOptions {
            limit: 10,
            params,
            ..Default::default()
        };
        let (total, hits) = execute_search(&idx, &ast, &opts);
        assert_eq!(total, 1);
        assert_eq!(hits[0].doc_id, "doc:1");
    }

    fn vector_schema(name: &str, algo: &str, dim: usize, data_type: VectorDataType) -> IndexSchema {
        let mut fields = HashMap::new();
        fields.insert(
            "tag".to_string(),
            FieldType::Tag {
                separator: ',',
                casesensitive: false,
            },
        );
        fields.insert(
            "v".to_string(),
            FieldType::Vector {
                dim,
                distance_metric: "L2".to_string(),
                algorithm: algo.to_string(),
                attrs: VectorFieldAttrs {
                    data_type,
                    ..Default::default()
                },
            },
        );
        IndexSchema {
            name: name.to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec![],
            fields,
            schema_fields: Vec::new(),
        }
    }

    #[test]
    fn test_typed_vector_decoding() {
        let f64_blob: Vec<u8> = [1.5f64, -2.0]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        assert_eq!(
            decode_vector(&f64_blob, VectorDataType::Float64, 2),
            Some(vec![1.5, -2.0])
        );
        let f16_blob: Vec<u8> = [0.5f32, -3.0]
            .iter()
            .flat_map(|f| f32_to_f16(*f).to_le_bytes())
            .collect();
        assert_eq!(
            decode_vector(&f16_blob, VectorDataType::Float16, 2),
            Some(vec![0.5, -3.0])
        );
        let bf16_blob: Vec<u8> = [1.0f32, 2.0]
            .iter()
            .flat_map(|f| ((f.to_bits() >> 16) as u16).to_le_bytes())
            .collect();
        assert_eq!(
            decode_vector(&bf16_blob, VectorDataType::BFloat16, 2),
            Some(vec![1.0, 2.0])
        );
        assert_eq!(
            decode_vector(&[0xff, 0x02], VectorDataType::Int8, 2),
            Some(vec![-1.0, 2.0])
        );
        assert_eq!(
            decode_vector(&[0xff, 0x02], VectorDataType::Uint8, 2),
            Some(vec![255.0, 2.0])
        );
        assert_eq!(
            decode_vector(b"1, 2, 3", VectorDataType::Float32, 3),
            Some(vec![1.0, 2.0, 3.0])
        );
        // Wrong dimension is rejected.
        assert_eq!(decode_vector(b"1,2", VectorDataType::Float32, 3), None);
        assert_eq!(decode_vector(&f64_blob, VectorDataType::Float32, 2), None);
    }

    #[test]
    fn test_flat_index_binary_hash_ingest_and_dim_validation() {
        let mut idx = InvertedIndex::new(vector_schema(
            "flat_idx",
            "FLAT",
            3,
            VectorDataType::Float32,
        ));
        assert!(matches!(
            idx.vector_indices.get("v"),
            Some(crate::vector::VectorFieldIndex::Flat(_))
        ));
        // A FLOAT32 blob containing bytes that are invalid UTF-8.
        let v1: Vec<u8> = [1.0f32, f32::from_bits(0x3f80_00ff), 0.0]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        idx.add_hash_document(
            "a",
            &[
                (Bytes::from_static(b"v"), Bytes::from(v1)),
                (Bytes::from_static(b"tag"), Bytes::from_static(b"x")),
            ],
        );
        assert_eq!(idx.vector_indices["v"].len(), 1);
        let got = idx.vector_indices["v"]
            .get_vector(&Bytes::from_static(b"a"))
            .unwrap();
        assert_eq!(got[1].to_bits(), 0x3f80_00ff);

        // Dimension mismatch -> document rejected and counted as failure.
        idx.add_hash_document(
            "b",
            &[(Bytes::from_static(b"v"), Bytes::from_static(b"1,2"))],
        );
        assert_eq!(idx.indexing_failures, 1);
        assert!(idx.doc_id_of(b"b").is_none());

        let hnsw = InvertedIndex::new(vector_schema(
            "hnsw_idx",
            "HNSW",
            3,
            VectorDataType::Float32,
        ));
        assert!(matches!(
            hnsw.vector_indices.get("v"),
            Some(crate::vector::VectorFieldIndex::Hnsw(_))
        ));
    }

    #[test]
    fn test_knn_clause_params_alias_and_distance_sort() {
        let mut idx =
            InvertedIndex::new(vector_schema("knn_idx", "HNSW", 2, VectorDataType::Float32));
        for (k, v) in [("a", "0,0"), ("b", "3,4"), ("c", "1,0"), ("d", "10,10")] {
            idx.add_hash_document(
                k,
                &[(
                    Bytes::from_static(b"v"),
                    Bytes::copy_from_slice(v.as_bytes()),
                )],
            );
        }
        let ast = parse_query("*=>[KNN $K @v $blob EF_RUNTIME $EF AS dist]");
        match ast.knn_clause() {
            Some(QueryAst::KnnVector {
                k_param,
                ef_runtime,
                yield_as,
                ..
            }) => {
                assert_eq!(k_param.as_deref(), Some("$K"));
                assert_eq!(ef_runtime.as_deref(), Some("$EF"));
                assert_eq!(yield_as.as_deref(), Some("dist"));
            }
            other => panic!("unexpected {:?}", other),
        }
        let mut params = HashMap::new();
        params.insert("K".to_string(), b"3".to_vec());
        params.insert("EF".to_string(), b"50".to_vec());
        params.insert(
            "blob".to_string(),
            [0.0f32, 0.0].iter().flat_map(|f| f.to_le_bytes()).collect(),
        );
        let mut opts = SearchOptions {
            params,
            ..Default::default()
        };
        assert_eq!(ast.knn_k(&opts), Some(3));
        let (total, hits) = execute_search(&idx, &ast, &opts);
        assert_eq!(total, 3);
        let ids: Vec<_> = hits.iter().map(|h| h.doc_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "c", "b"]);
        assert_eq!(hits[0].fields.get("dist").map(String::as_str), Some("0"));
        assert_eq!(hits[2].fields.get("dist").map(String::as_str), Some("5"));

        opts.sortby = Some(("dist".to_string(), false));
        let (_, hits) = execute_search(&idx, &ast, &opts);
        let ids: Vec<_> = hits.iter().map(|h| h.doc_id.as_str()).collect();
        assert_eq!(ids, vec!["b", "c", "a"]);

        // Default distance field name and legacy YIELD_DISTANCE_AS attribute block.
        assert_eq!(
            parse_query("*=>[KNN 2 @v $blob]")
                .knn_score_field()
                .as_deref(),
            Some("__v_score")
        );
        assert_eq!(
            parse_query("*=>[KNN 2 @v $blob]=>{$YIELD_DISTANCE_AS: d2}")
                .knn_score_field()
                .as_deref(),
            Some("d2")
        );
    }

    #[test]
    fn test_hybrid_knn_prefilter_returns_k_results() {
        for algo in ["HNSW", "FLAT"] {
            let mut idx =
                InvertedIndex::new(vector_schema("hyb", algo, 2, VectorDataType::Float32));
            // 300 "common" docs clustered near the origin, 40 "rare" docs far away.
            for i in 0..300 {
                let v = format!("{},{}", i as f32 * 0.01, (i % 13) as f32 * 0.01);
                idx.add_hash_document(
                    &format!("c{}", i),
                    &[
                        (Bytes::from_static(b"v"), Bytes::from(v)),
                        (Bytes::from_static(b"tag"), Bytes::from_static(b"common")),
                    ],
                );
            }
            for i in 0..40 {
                let v = format!("{},{}", 100.0 + i as f32, 100.0);
                idx.add_hash_document(
                    &format!("r{}", i),
                    &[
                        (Bytes::from_static(b"v"), Bytes::from(v)),
                        (Bytes::from_static(b"tag"), Bytes::from_static(b"rare")),
                    ],
                );
            }
            let mut params = HashMap::new();
            params.insert(
                "blob".to_string(),
                [0.0f32, 0.0].iter().flat_map(|f| f.to_le_bytes()).collect(),
            );
            let opts = SearchOptions {
                params,
                ..Default::default()
            };
            // Small filtered set -> ad-hoc brute force.
            let ast = parse_query("(@tag:{rare})=>[KNN 5 @v $blob]");
            let (total, hits) = execute_search(&idx, &ast, &opts);
            assert_eq!(total, 5, "{algo}");
            let ids: Vec<_> = hits.iter().map(|h| h.doc_id.clone()).collect();
            assert_eq!(ids, vec!["r0", "r1", "r2", "r3", "r4"], "{algo}");

            // Large filtered set -> filtered graph traversal; all results must satisfy the filter.
            let ast = parse_query("(@tag:{common})=>[KNN 7 @v $blob]");
            let (total, hits) = execute_search(&idx, &ast, &opts);
            assert_eq!(total, 7, "{algo}");
            assert!(hits.iter().all(|h| h.doc_id.starts_with('c')), "{algo}");
            assert_eq!(hits[0].doc_id, "c0", "{algo}");
        }
    }

    #[test]
    fn test_search_on_json_documents() {
        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldType::Text {
                weight: 1.0,
                sortable: true,
                nostem: false,
            },
        );
        fields.insert("price".to_string(), FieldType::Numeric { sortable: true });
        fields.insert(
            "tags".to_string(),
            FieldType::Tag {
                separator: ',',
                casesensitive: false,
            },
        );
        fields.insert("rating".to_string(), FieldType::Numeric { sortable: true });

        let schema_fields = vec![
            SchemaField {
                identifier: "$.title".to_string(),
                alias: "title".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: true,
                    nostem: false,
                },
            },
            SchemaField {
                identifier: "$.price".to_string(),
                alias: "price".to_string(),
                field_type: FieldType::Numeric { sortable: true },
            },
            SchemaField {
                identifier: "$.tags.*".to_string(),
                alias: "tags".to_string(),
                field_type: FieldType::Tag {
                    separator: ',',
                    casesensitive: false,
                },
            },
            SchemaField {
                identifier: "$.meta.rating".to_string(),
                alias: "rating".to_string(),
                field_type: FieldType::Numeric { sortable: true },
            },
        ];

        let schema = IndexSchema {
            name: "idx:inventory_json".to_string(),
            on_type: "JSON".to_string(),
            prefixes: vec!["inv:".to_string()],
            fields,
            schema_fields,
        };

        // Create in global registry
        assert!(create_search_index(schema).is_ok());

        let json1 = serde_json::json!({
            "title": "High performance Rust memory engine",
            "price": 89.99,
            "tags": ["database", "rust", "redis"],
            "meta": { "rating": 4.8 }
        });

        let json2 = serde_json::json!({
            "title": "Distributed stream processor in Go",
            "price": 35.50,
            "tags": ["streaming", "golang"],
            "meta": { "rating": 4.1 }
        });

        index_json_document_hook("inv:1", &json1);
        index_json_document_hook("inv:2", &json2);

        let idx_arc = get_search_index("idx:inventory_json").expect("index exists");
        let idx = idx_arc.read();
        assert_eq!(idx.total_docs, 2);

        // 1. Text search for "rust"
        let ast1 = parse_query("rust");
        let (total1, hits1) = execute_search(&idx, &ast1, &SearchOptions::default());
        assert_eq!(total1, 1);
        assert_eq!(hits1[0].doc_id, "inv:1");

        // 2. Numeric range query on price
        let ast2 = parse_query("@price:[30 50]");
        let (total2, hits2) = execute_search(&idx, &ast2, &SearchOptions::default());
        assert_eq!(total2, 1);
        assert_eq!(hits2[0].doc_id, "inv:2");

        // 3. Tag filter on tags array extracted via wildcard
        let ast3 = parse_query("@tags:{redis}");
        let (total3, hits3) = execute_search(&idx, &ast3, &SearchOptions::default());
        assert_eq!(total3, 1);
        assert_eq!(hits3[0].doc_id, "inv:1");

        // 4. Nested JSONPath numeric range: @rating:[4.5 5.0]
        let ast4 = parse_query("@rating:[4.5 5.0]");
        let (total4, hits4) = execute_search(&idx, &ast4, &SearchOptions::default());
        assert_eq!(total4, 1);
        assert_eq!(hits4[0].doc_id, "inv:1");

        // Drop index cleanup
        drop(idx);
        assert!(drop_search_index("idx:inventory_json").is_ok());
    }

    fn h2_clone(h: &SearchHit) -> SearchHit {
        h.clone()
    }

    #[test]
    fn test_dense_doc_id_and_term_directed_deletion() {
        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldType::Text {
                weight: 1.0,
                sortable: false,
                nostem: false,
            },
        );
        let schema = IndexSchema {
            name: "idx:dense_test".to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec!["doc:".to_string()],
            fields,
            schema_fields: Vec::new(),
        };
        let mut idx = InvertedIndex::new(schema);

        let mut d1 = HashMap::new();
        d1.insert("title".to_string(), "alpha beta gamma".to_string());
        idx.add_document("doc:1", d1, None);

        let mut d2 = HashMap::new();
        d2.insert("title".to_string(), "gamma delta epsilon".to_string());
        idx.add_document("doc:2", d2, None);

        // Verify dense DocIds assigned
        let id1 = idx.doc_id_of(b"doc:1").expect("doc:1 has id");
        let id2 = idx.doc_id_of(b"doc:2").expect("doc:2 has id");
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(idx.key_of(id1).map(|b| b.as_ref()), Some(b"doc:1".as_ref()));
        assert_eq!(idx.total_docs, 2);

        // Verify postings contain DocIds
        assert_eq!(idx.inverted.get("alpha").unwrap().len(), 1);
        assert_eq!(idx.inverted.get("gamma").unwrap().len(), 2);

        // Delete doc:1
        idx.remove_document("doc:1");
        assert_eq!(idx.total_docs, 1);
        assert!(idx.doc_id_of(b"doc:1").is_none());
        assert!(idx.key_of(id1).is_none());

        // Term "alpha" had only doc:1, so it should be pruned completely from inverted index
        assert!(!idx.inverted.contains_key("alpha"));
        // Term "gamma" still has doc:2
        assert_eq!(idx.inverted.get("gamma").unwrap().len(), 1);
        assert_eq!(idx.inverted.get("gamma").unwrap()[0].doc_id, id2);

        // Add doc:3 - should recycle id1 from free_ids
        let mut d3 = HashMap::new();
        d3.insert("title".to_string(), "zeta eta".to_string());
        idx.add_document("doc:3", d3, None);
        let id3 = idx.doc_id_of(b"doc:3").expect("doc:3 has id");
        assert_eq!(id3, id1, "doc:3 should reuse recycled doc_id");
    }

    #[test]
    fn test_balanced_range_tree_numeric_index() {
        // 1. Test standalone RangeTree operations
        let mut tree = RangeTree::new();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);

        tree.add(1, 10.5);
        tree.add(2, 25.0);
        tree.add(3, 50.0);
        tree.add(4, 50.0); // Duplicate value test
        tree.add(5, 100.0);

        assert_eq!(tree.len(), 5);
        assert_eq!(tree.min(), Some(10.5));
        assert_eq!(tree.max(), Some(100.0));

        // Range [20, 60] should include doc 2 (25.0), doc 3 (50.0), and doc 4 (50.0)
        let r1 = tree.range(20.0, 60.0);
        assert_eq!(r1, vec![2, 3, 4]);

        // Range [50, 50] exact point match
        let r2 = tree.range(50.0, 50.0);
        assert_eq!(r2, vec![3, 4]);

        // Range with min > max returns empty
        assert!(tree.range(60.0, 20.0).is_empty());

        // Range out of bounds
        assert!(tree.range(200.0, 300.0).is_empty());

        // Remove doc 3 (one of the 50.0 entries)
        tree.remove(3, 50.0);
        assert_eq!(tree.len(), 4);
        assert_eq!(tree.range(50.0, 50.0), vec![4]);

        // Remove doc 4 (remaining 50.0 entry)
        tree.remove(4, 50.0);
        assert_eq!(tree.len(), 3);
        assert!(tree.range(50.0, 50.0).is_empty());

        // 2. Test InvertedIndex integrated RangeTree numeric queries
        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldType::Text {
                weight: 1.0,
                sortable: false,
                nostem: false,
            },
        );
        fields.insert("price".to_string(), FieldType::Numeric { sortable: true });

        let schema = IndexSchema {
            name: "idx:range_test".to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec!["prod:".to_string()],
            fields,
            schema_fields: Vec::new(),
        };
        let mut idx = InvertedIndex::new(schema);

        let mut p1 = HashMap::new();
        p1.insert("title".to_string(), "keyboard".to_string());
        p1.insert("price".to_string(), "45.0".to_string());
        idx.add_document("prod:1", p1, None);

        let mut p2 = HashMap::new();
        p2.insert("title".to_string(), "mouse".to_string());
        p2.insert("price".to_string(), "25.5".to_string());
        idx.add_document("prod:2", p2, None);

        let mut p3 = HashMap::new();
        p3.insert("title".to_string(), "monitor".to_string());
        p3.insert("price".to_string(), "299.99".to_string());
        idx.add_document("prod:3", p3, None);

        // Verify RangeTree is populated in idx.numeric_trees
        let price_tree = idx.numeric_trees.get("price").expect("price tree exists");
        assert_eq!(price_tree.len(), 3);

        // Query @price:[20 50]
        let ast = parse_query("@price:[20 50]");
        let (total, hits) = execute_search(&idx, &ast, &SearchOptions::default());
        assert_eq!(total, 2);
        let hit_ids: Vec<&str> = hits.iter().map(|h| h.doc_id.as_str()).collect();
        assert!(hit_ids.contains(&"prod:1"));
        assert!(hit_ids.contains(&"prod:2"));
        assert!(!hit_ids.contains(&"prod:3"));

        // Delete prod:2
        idx.remove_document("prod:2");
        let price_tree = idx.numeric_trees.get("price").expect("price tree exists");
        assert_eq!(price_tree.len(), 2);

        // Re-query @price:[20 50]
        let (total2, hits2) = execute_search(&idx, &ast, &SearchOptions::default());
        assert_eq!(total2, 1);
        assert_eq!(hits2[0].doc_id, "prod:1");
    }

    #[test]
    fn test_ft_aggregate_pipeline_groupby_reduce_apply_sort() {
        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldType::Text {
                weight: 1.0,
                sortable: false,
                nostem: false,
            },
        );
        fields.insert(
            "category".to_string(),
            FieldType::Tag {
                separator: ',',
                casesensitive: false,
            },
        );
        fields.insert("price".to_string(), FieldType::Numeric { sortable: true });

        let schema = IndexSchema {
            name: "idx:agg_test".to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec!["item:".to_string()],
            fields,
            schema_fields: Vec::new(),
        };
        let mut idx = InvertedIndex::new(schema);

        // Books
        let mut b1 = HashMap::new();
        b1.insert("title".to_string(), "rust in action".to_string());
        b1.insert("category".to_string(), "books".to_string());
        b1.insert("price".to_string(), "40.0".to_string());
        idx.add_document("item:1", b1, None);

        let mut b2 = HashMap::new();
        b2.insert("title".to_string(), "programming rust".to_string());
        b2.insert("category".to_string(), "books".to_string());
        b2.insert("price".to_string(), "50.0".to_string());
        idx.add_document("item:2", b2, None);

        // Electronics
        let mut e1 = HashMap::new();
        e1.insert("title".to_string(), "wireless mouse".to_string());
        e1.insert("category".to_string(), "electronics".to_string());
        e1.insert("price".to_string(), "30.0".to_string());
        idx.add_document("item:3", e1, None);

        let mut e2 = HashMap::new();
        e2.insert("title".to_string(), "mechanical keyboard".to_string());
        e2.insert("category".to_string(), "electronics".to_string());
        e2.insert("price".to_string(), "120.0".to_string());
        idx.add_document("item:4", e2, None);

        // Execute search for all items
        let ast = parse_query("*");
        let (_total, hits) = execute_search(&idx, &ast, &SearchOptions::default());

        // Pipeline:
        // GROUPBY 1 @category
        //   REDUCE COUNT 0 AS cnt
        //   REDUCE SUM 1 @price AS sum_price
        //   REDUCE AVG 1 @price AS avg_price
        //   REDUCE MIN 1 @price AS min_price
        //   REDUCE MAX 1 @price AS max_price
        // APPLY @sum_price * 1.1 AS taxed
        // SORTBY 2 @taxed DESC
        let options = AggregateOptions {
            load_fields: vec!["category".to_string(), "price".to_string()],
            stages: vec![
                AggregateStage::Group(GroupByStage {
                    fields: vec!["category".to_string()],
                    reducers: vec![
                        Reducer::Count {
                            alias: "cnt".to_string(),
                        },
                        Reducer::Sum {
                            field: "price".to_string(),
                            alias: "sum_price".to_string(),
                        },
                        Reducer::Avg {
                            field: "price".to_string(),
                            alias: "avg_price".to_string(),
                        },
                        Reducer::Min {
                            field: "price".to_string(),
                            alias: "min_price".to_string(),
                        },
                        Reducer::Max {
                            field: "price".to_string(),
                            alias: "max_price".to_string(),
                        },
                    ],
                }),
                AggregateStage::Apply(ApplyStage {
                    expr: "@sum_price * 1.1".to_string(),
                    alias: "taxed".to_string(),
                }),
                AggregateStage::Sort(SortByStage {
                    fields: vec![("taxed".to_string(), false)], // DESC
                    max: None,
                }),
            ],
        };

        let rows = execute_aggregate_pipeline(hits, &options);
        assert_eq!(rows.len(), 2);

        // Row 0 should be electronics (sum 150 * 1.1 = 165)
        let row0 = &rows[0];
        assert_eq!(get_row_field(row0, "category"), Some("electronics"));
        assert_eq!(get_row_field(row0, "cnt"), Some("2"));
        assert_eq!(get_row_field(row0, "sum_price"), Some("150"));
        assert_eq!(get_row_field(row0, "avg_price"), Some("75"));
        assert_eq!(get_row_field(row0, "min_price"), Some("30"));
        assert_eq!(get_row_field(row0, "max_price"), Some("120"));
        assert_eq!(get_row_field(row0, "taxed"), Some("165"));

        // Row 1 should be books (sum 90 * 1.1 = 99)
        let row1 = &rows[1];
        assert_eq!(get_row_field(row1, "category"), Some("books"));
        assert_eq!(get_row_field(row1, "cnt"), Some("2"));
        assert_eq!(get_row_field(row1, "sum_price"), Some("90"));
        assert_eq!(get_row_field(row1, "avg_price"), Some("45"));
        assert_eq!(get_row_field(row1, "min_price"), Some("40"));
        assert_eq!(get_row_field(row1, "max_price"), Some("50"));
        assert_eq!(get_row_field(row1, "taxed"), Some("99"));
    }

    #[test]
    fn test_ft_search_rrf_hybrid_fusion() {
        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldType::Text {
                weight: 1.0,
                sortable: false,
                nostem: false,
            },
        );
        fields.insert(
            "embedding".to_string(),
            FieldType::Vector {
                dim: 3,
                distance_metric: "COSINE".to_string(),
                algorithm: "HNSW".to_string(),
                attrs: VectorFieldAttrs::default(),
            },
        );
        let schema_fields = vec![
            SchemaField {
                identifier: "title".to_string(),
                alias: "title".to_string(),
                field_type: fields["title"].clone(),
            },
            SchemaField {
                identifier: "embedding".to_string(),
                alias: "embedding".to_string(),
                field_type: fields["embedding"].clone(),
            },
        ];
        let schema = IndexSchema {
            name: "idx:rag_rrf".to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec!["chunk:".to_string()],
            fields,
            schema_fields,
        };
        let mut idx = InvertedIndex::new(schema);

        // chunk:1: appears in BOTH BM25 ("rust") and KNN top-2 -> wins RRF!
        let mut c1 = HashMap::new();
        c1.insert(
            "title".to_string(),
            "rust engine for rust ai rag".to_string(),
        );
        c1.insert("embedding".to_string(), "0.96, 0.28, 0.0".to_string());
        idx.add_document("chunk:1", c1, None);

        // chunk:2: #1 in KNN (exact [1,0,0]), but does NOT contain "rust"
        let mut c2 = HashMap::new();
        c2.insert("title".to_string(), "semantic vector store".to_string());
        c2.insert("embedding".to_string(), "1.0, 0.0, 0.0".to_string());
        idx.add_document("chunk:2", c2, None);

        // chunk:3: contains "rust", but orthogonal vector [0,1,0] (not in KNN top-2)
        let mut c3 = HashMap::new();
        c3.insert("title".to_string(), "rust compiler internals".to_string());
        c3.insert("embedding".to_string(), "0.0, 1.0, 0.0".to_string());
        idx.add_document("chunk:3", c3, None);

        let mut params = HashMap::new();
        let q_bytes: Vec<u8> = vec![1.0f32, 0.0f32, 0.0f32]
            .into_iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        params.insert("vec".to_string(), q_bytes);

        let ast_rrf = parse_query("rust=>[KNN 2 @embedding $vec]");
        let opts_rrf = SearchOptions {
            limit: 10,
            params,
            rrf_k: Some(60.0),
            withscores: true,
            ..Default::default()
        };
        let (total_rrf, hits_rrf) = execute_search(&idx, &ast_rrf, &opts_rrf);
        assert_eq!(total_rrf, 3);
        assert_eq!(hits_rrf[0].doc_id, "chunk:1");
        let expected_top_rrf = (1.0 / 61.0) + (1.0 / 62.0);
        assert!((hits_rrf[0].score - expected_top_rrf).abs() < 1e-6);
    }

    #[test]
    fn test_vector_range_query_and_hybrid_filter() {
        let mut fields = HashMap::new();
        fields.insert(
            "cat".to_string(),
            FieldType::Tag {
                separator: ',',
                casesensitive: false,
            },
        );
        fields.insert(
            "vec".to_string(),
            FieldType::Vector {
                dim: 3,
                distance_metric: "L2".to_string(),
                algorithm: "HNSW".to_string(),
                attrs: VectorFieldAttrs::default(),
            },
        );
        let schema_fields = vec![
            SchemaField {
                identifier: "cat".to_string(),
                alias: "cat".to_string(),
                field_type: fields["cat"].clone(),
            },
            SchemaField {
                identifier: "vec".to_string(),
                alias: "vec".to_string(),
                field_type: fields["vec"].clone(),
            },
        ];
        let mut idx = InvertedIndex::new(IndexSchema {
            name: "idx:vr".to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec!["item:".to_string()],
            fields,
            schema_fields,
        });

        for (id, cat, v) in [
            ("item:1", "gpu", "1.0, 0.0, 0.0"),  // sq L2 to [1,0,0] = 0.0
            ("item:2", "cpu", "0.8, 0.2, 0.0"),  // sq L2 = 0.08
            ("item:3", "gpu", "0.6, 0.4, 0.0"),  // sq L2 = 0.32
            ("item:4", "gpu", "-1.0, 0.0, 0.0"), // sq L2 = 4.0
        ] {
            let mut m = HashMap::new();
            m.insert("cat".to_string(), cat.to_string());
            m.insert("vec".to_string(), v.to_string());
            idx.add_document(id, m, None);
        }

        let mut params = HashMap::new();
        let q_bytes: Vec<u8> = [1.0f32, 0.0, 0.0]
            .into_iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        params.insert("q".to_string(), q_bytes);
        params.insert("r".to_string(), b"0.6".to_vec());

        // 1. Standalone VECTOR_RANGE with $r parameter and $YIELD_DISTANCE_AS
        let ast = parse_query(
            "@vec:[VECTOR_RANGE $r $q]=>{$EPSILON: 0.05; $YIELD_DISTANCE_AS: range_dist}",
        );
        let opts = SearchOptions {
            limit: 10,
            params: params.clone(),
            ..Default::default()
        };
        let (total, hits) = execute_search(&idx, &ast, &opts);
        assert_eq!(total, 3);
        assert_eq!(hits[0].doc_id, "item:1");
        assert_eq!(hits[1].doc_id, "item:2");
        assert_eq!(hits[2].doc_id, "item:3");
        assert_eq!(
            hits[0].fields.get("range_dist").map(String::as_str),
            Some("0")
        );

        // 2. Hybrid tag + VECTOR_RANGE query
        let ast_hybrid =
            parse_query("@cat:{gpu} @vec:[VECTOR_RANGE 0.6 $q]=>{$YIELD_DISTANCE_AS: range_dist}");
        let (total_h, hits_h) = execute_search(&idx, &ast_hybrid, &opts);
        assert_eq!(total_h, 2);
        assert_eq!(hits_h[0].doc_id, "item:1");
        assert_eq!(hits_h[1].doc_id, "item:3");
    }

    #[test]
    fn test_multi_vector_json_chunks_and_linear_fusion() {
        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldType::Text {
                weight: 1.0,
                sortable: false,
                nostem: false,
            },
        );
        fields.insert(
            "emb".to_string(),
            FieldType::Vector {
                dim: 3,
                distance_metric: "COSINE".to_string(),
                algorithm: "HNSW".to_string(),
                attrs: VectorFieldAttrs::default(),
            },
        );
        let schema = IndexSchema {
            name: "idx:chunks".to_string(),
            on_type: "JSON".to_string(),
            prefixes: vec!["doc:".to_string()],
            fields: fields.clone(),
            schema_fields: vec![
                SchemaField {
                    identifier: "$.title".to_string(),
                    alias: "title".to_string(),
                    field_type: fields["title"].clone(),
                },
                SchemaField {
                    identifier: "$.chunks[*].emb".to_string(),
                    alias: "emb".to_string(),
                    field_type: fields["emb"].clone(),
                },
            ],
        };
        let mut idx = InvertedIndex::new(schema.clone());

        // doc:1 has 3 chunks; chunk #2 is an exact match for [0, 1, 0]
        let doc1 = serde_json::json!({
            "title": "rust rag architecture",
            "chunks": [
                { "text": "intro", "emb": [1.0, 0.0, 0.0] },
                { "text": "middle", "emb": [0.0, 1.0, 0.0] },
                { "text": "outro", "emb": [0.0, 0.0, 1.0] }
            ]
        });
        let (f1, v1) = extract_json_fields(&schema, &doc1);
        idx.add_document("doc:1", f1, v1);

        // doc:2 has 1 chunk close to [1, 0, 0]
        let doc2 = serde_json::json!({
            "title": "rust compiler",
            "chunks": [
                { "text": "only", "emb": [1.0, 0.0, 0.0] }
            ]
        });
        let (f2, v2) = extract_json_fields(&schema, &doc2);
        idx.add_document("doc:2", f2, v2);

        let mut params = HashMap::new();
        let q_bytes: Vec<u8> = [0.0f32, 1.0, 0.0]
            .into_iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        params.insert("q".to_string(), q_bytes);

        // Querying for [0, 1, 0] matches doc:1 via its second chunk with distance 0, deduplicating doc:1's 3 chunks!
        let ast = parse_query("*=>[KNN 2 @emb $q AS dist]");
        let opts = SearchOptions {
            limit: 10,
            params: params.clone(),
            ..Default::default()
        };
        let (total, hits) = execute_search(&idx, &ast, &opts);
        assert_eq!(total, 2);
        assert_eq!(hits[0].doc_id, "doc:1");
        assert_eq!(hits[0].fields.get("dist").map(String::as_str), Some("0"));

        // Linear score fusion on hybrid query
        let ast_hybrid = parse_query("(rust)=>[KNN 2 @emb $q]");
        let opts_linear = SearchOptions {
            limit: 10,
            params,
            linear_weights: Some((0.3, 0.7)),
            withscores: true,
            ..Default::default()
        };
        let (total_lin, hits_lin) = execute_search(&idx, &ast_hybrid, &opts_linear);
        assert_eq!(total_lin, 2);
        assert_eq!(hits_lin[0].doc_id, "doc:1");
        assert!(hits_lin[0].score > hits_lin[1].score);
    }

    /// NaN bounds (`@n:[nan 0]`) made BTreeMap::range panic.
    #[test]
    fn test_range_tree_nan_bounds_are_empty() {
        let mut tree = RangeTree::new();
        tree.add(1, 1.0);
        assert!(tree.range(f64::NAN, 0.0).is_empty());
        assert!(tree.range(0.0, f64::NAN).is_empty());
        assert_eq!(tree.range(f64::NEG_INFINITY, f64::INFINITY), vec![1]);
    }
}
