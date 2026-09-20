use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, RwLock};

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
    },
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
        if min > max {
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
    // vector field -> HnswIndex for logarithmic vector beam search
    pub vector_indices: HashMap<String, crate::vector::HnswIndex>,
    pub next_doc_id: DocId,
    pub free_ids: Vec<DocId>,
    pub total_docs: usize,
    pub total_terms: usize,
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

pub fn parse_vector_blob(bytes: &[u8]) -> Vec<f32> {
    if bytes.len().is_multiple_of(4) && !bytes.is_empty() {
        let (chunks, _) = bytes.as_chunks::<4>();
        chunks
            .iter()
            .map(|chunk| f32::from_le_bytes(*chunk))
            .collect()
    } else {
        String::from_utf8_lossy(bytes)
            .split(|c: char| c == ',' || c.is_whitespace() || c == '[' || c == ']')
            .filter_map(|s| s.trim().parse::<f32>().ok())
            .collect()
    }
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
        Self {
            schema: Some(schema),
            inverted: HashMap::new(),
            numeric_trees: HashMap::new(),
            key_to_id: HashMap::new(),
            id_to_meta: HashMap::new(),
            vector_indices: HashMap::new(),
            next_doc_id: 1,
            free_ids: Vec::new(),
            total_docs: 0,
            total_terms: 0,
        }
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

        let mut vector_fields = vectors.unwrap_or_default();
        for (field_name, field_val) in &fields {
            if let Some(FieldType::Vector { .. }) = schema.fields.get(field_name)
                && !vector_fields.contains_key(field_name)
            {
                let v = parse_vector_blob(field_val.as_bytes());
                if !v.is_empty() {
                    vector_fields.insert(field_name.clone(), v);
                }
            }
        }

        for (field_name, vec) in &vector_fields {
            let dim = vec.len();
            let metric = match schema.fields.get(field_name) {
                Some(FieldType::Vector {
                    distance_metric, ..
                }) => match distance_metric.to_uppercase().as_str() {
                    "L2" => crate::vector::VectorMetric::L2,
                    "IP" => crate::vector::VectorMetric::IP,
                    _ => crate::vector::VectorMetric::Cosine,
                },
                _ => crate::vector::VectorMetric::Cosine,
            };
            let idx_name = schema.name.clone();
            let hnsw = self
                .vector_indices
                .entry(field_name.clone())
                .or_insert_with(|| crate::vector::HnswIndex::new(idx_name, dim, metric));
            let _ = hnsw.add(key_bytes.clone(), vec.clone());
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

            // Remove from vector indices
            for field_name in meta.vector_fields.keys() {
                if let Some(hnsw) = self.vector_indices.get_mut(field_name) {
                    hnsw.remove(&meta.key);
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
    let registry = SEARCH_INDICES.read().unwrap();
    registry.get(name).cloned()
}

pub fn create_search_index(schema: IndexSchema) -> Result<(), String> {
    let mut registry = SEARCH_INDICES.write().unwrap();
    if registry.contains_key(&schema.name) {
        return Err(format!("Index already exists: {}", schema.name));
    }
    let name = schema.name.clone();
    let idx = Arc::new(RwLock::new(InvertedIndex::new(schema)));
    registry.insert(name, idx);
    SEARCH_INDICES_COUNT.store(registry.len(), std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

pub fn drop_search_index(name: &str) -> Result<(), String> {
    let mut registry = SEARCH_INDICES.write().unwrap();
    if registry.remove(name).is_some() {
        SEARCH_INDICES_COUNT.store(registry.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(())
    } else {
        Err(format!("Unknown index name: {}", name))
    }
}

pub fn list_search_indices() -> Vec<String> {
    let registry = SEARCH_INDICES.read().unwrap();
    registry.keys().cloned().collect()
}

pub fn index_document_hook(key: &str, fields: HashMap<String, String>) {
    let registry = SEARCH_INDICES.read().unwrap();
    for idx_lock in registry.values() {
        let mut idx = idx_lock.write().unwrap();
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
                idx.add_document(key, fields.clone(), None);
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
                if let Some(first) = matches.first() {
                    let vec_opt: Option<Vec<f32>> = match first {
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
                    if let Some(vec) = vec_opt {
                        extracted_vectors.insert(sf.alias.clone(), vec.clone());
                        if sf.alias != sf.identifier {
                            extracted_vectors.insert(sf.identifier.clone(), vec.clone());
                        }
                        let s = vec
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
    let registry = SEARCH_INDICES.read().unwrap();
    for idx_lock in registry.values() {
        let mut idx = idx_lock.write().unwrap();
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
    let registry = SEARCH_INDICES.read().unwrap();
    for idx_lock in registry.values() {
        let mut idx = idx_lock.write().unwrap();
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
    },
    MatchAll,
}

impl QueryAst {
    pub fn knn_k(&self) -> Option<usize> {
        match self {
            QueryAst::KnnVector { k, .. } => Some(*k),
            QueryAst::And(subs) | QueryAst::Or(subs) => {
                for sub in subs {
                    if let Some(k) = sub.knn_k() {
                        return Some(k);
                    }
                }
                None
            }
            QueryAst::FieldScope { inner, .. } => inner.knn_k(),
            _ => None,
        }
    }
}

pub fn parse_query(q: &str) -> QueryAst {
    let q = q.trim();
    if q == "*" || q.is_empty() {
        return QueryAst::MatchAll;
    }

    // Check for KNN vector query syntax: "*=>[KNN 10 @vec $param]" or "(query)=>[KNN 10 @vec $param]"
    if let Some((base_part, knn_part)) = q.split_once("=>[KNN") {
        let base_ast = if base_part.trim() == "*" || base_part.trim().is_empty() {
            QueryAst::MatchAll
        } else {
            parse_query(base_part.trim())
        };

        if let Some((args_part, _)) = knn_part.split_once(']') {
            let tokens: Vec<&str> = args_part.split_whitespace().collect();
            let k: usize = tokens
                .first()
                .and_then(|val| val.parse().ok())
                .unwrap_or(10);
            let field = tokens
                .get(1)
                .map(|s| s.trim_start_matches('@').to_string())
                .unwrap_or_default();
            let param_name = tokens
                .get(2)
                .map(|s| s.trim_start_matches('$').to_string())
                .unwrap_or_default();
            let knn_ast = QueryAst::KnnVector {
                field,
                k,
                query_vec: Vec::new(),
                param_name,
            };
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
                // Numeric range: @price:[10 100]
                let mut range_str = stripped.to_string();
                if !range_str.contains(']') {
                    while i + 1 < words.len() {
                        i += 1;
                        range_str.push(' ');
                        range_str.push_str(words[i]);
                        if words[i].ends_with(']') {
                            break;
                        }
                    }
                }
                let clean = range_str.trim_end_matches(']');
                let parts: Vec<&str> = clean.split_whitespace().collect();
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
    pub sortby: Option<(String, bool)>, // (field, ascending)
    pub return_fields: Option<Vec<String>>,
    pub params: HashMap<String, Vec<u8>>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 10,
            nocontent: false,
            sortby: None,
            return_fields: None,
            params: HashMap::new(),
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
        QueryAst::KnnVector {
            field,
            k,
            query_vec,
            param_name,
        } => {
            let effective_vec = if !query_vec.is_empty() {
                query_vec.clone()
            } else if !param_name.is_empty() {
                let raw_bytes = opts
                    .params
                    .get(param_name)
                    .or_else(|| opts.params.get(&format!("${}", param_name)));
                if let Some(bytes) = raw_bytes {
                    parse_vector_blob(bytes)
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            let mut map = HashMap::new();
            if !effective_vec.is_empty() {
                let hnsw_opt = index.vector_indices.get(field).or_else(|| {
                    field
                        .strip_prefix("$.")
                        .and_then(|f| index.vector_indices.get(f))
                });
                if let Some(hnsw) = hnsw_opt {
                    let results = hnsw.search(&effective_vec, *k);
                    for (doc_key, dist) in results {
                        if let Some(doc_id) = index.key_to_id.get(&doc_key) {
                            let sim = match hnsw.metric {
                                crate::vector::VectorMetric::Cosine => (1.0 - dist).max(0.0) as f64,
                                _ => (1.0 / (1.0 + dist)) as f64,
                            };
                            map.insert(*doc_id, sim);
                        }
                    }
                } else {
                    let mut vector_dists = Vec::new();
                    for (&doc_id, doc) in &index.id_to_meta {
                        let vec_opt = doc.vector_fields.get(field).or_else(|| {
                            field
                                .strip_prefix("$.")
                                .and_then(|f| doc.vector_fields.get(f))
                        });
                        if let Some(doc_vec) = vec_opt
                            && effective_vec.len() == doc_vec.len()
                        {
                            let sim = cosine_similarity(&effective_vec, doc_vec);
                            vector_dists.push((doc_id, sim));
                        }
                    }
                    vector_dists
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                    for (id, sim) in vector_dists.into_iter().take(*k) {
                        map.insert(id, sim as f64);
                    }
                }
            }
            map
        }
    }
}

pub fn execute_search(
    index: &InvertedIndex,
    ast: &QueryAst,
    opts: &SearchOptions,
) -> (usize, Vec<SearchHit>) {
    let candidate_scores = evaluate_ast(index, ast, opts);
    let total_matches = candidate_scores.len();

    // 2. Sort results
    let mut scored_docs: Vec<(DocId, f64)> = candidate_scores.into_iter().collect();

    if let Some((sort_field, asc)) = &opts.sortby {
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
    for (doc_id, score) in paged {
        let mut fields = HashMap::new();
        if let Some(doc) = index.id_to_meta.get(&doc_id) {
            let mut sort_val = None;
            if let Some((sort_field, _)) = &opts.sortby {
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

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    (1.0 - crate::vector::cosine_distance(a, b)).max(0.0)
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
        let idx = idx_arc.read().unwrap();
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
}
