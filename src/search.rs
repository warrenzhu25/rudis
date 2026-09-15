use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, RwLock};

#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Text { weight: f64, sortable: bool, nostem: bool },
    Numeric { sortable: bool },
    Tag { separator: char, casesensitive: bool },
    Vector { dim: usize, distance_metric: String, algorithm: String },
}

#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub name: String,
    pub on_type: String, // "HASH" or "JSON"
    pub prefixes: Vec<String>,
    pub fields: HashMap<String, FieldType>,
}

#[derive(Debug, Clone)]
pub struct Posting {
    pub doc_id: String,
    pub term_freq: u32,
    pub positions: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct DocMeta {
    pub doc_id: String,
    pub doc_len: usize, // total tokens
    pub fields: HashMap<String, String>,
    pub numeric_fields: HashMap<String, f64>,
    pub tag_fields: HashMap<String, HashSet<String>>,
    pub vector_fields: HashMap<String, Vec<f32>>,
}

#[derive(Debug, Default)]
pub struct InvertedIndex {
    pub schema: Option<IndexSchema>,
    // term -> list of postings
    pub inverted: HashMap<String, Vec<Posting>>,
    // doc_id -> metadata
    pub docs: HashMap<String, DocMeta>,
    pub total_docs: usize,
    pub total_terms: usize,
}

static ENGLISH_STOP_WORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
        "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
        "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
        "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
        "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
        "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
        "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
        "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
        "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
        "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
        "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
        "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
        "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
        "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
        "they've", "this", "those", "through", "to", "too", "under", "until", "up",
        "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
        "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
        "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
        "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
        "yourself", "yourselves",
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

impl InvertedIndex {
    pub fn new(schema: IndexSchema) -> Self {
        Self {
            schema: Some(schema),
            inverted: HashMap::new(),
            docs: HashMap::new(),
            total_docs: 0,
            total_terms: 0,
        }
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
        doc_id: &str,
        fields: HashMap<String, String>,
        vectors: Option<HashMap<String, Vec<f32>>>,
    ) {
        // If document already exists, remove old entries first
        self.remove_document(doc_id);

        let schema = match &self.schema {
            Some(s) => s.clone(),
            None => return,
        };

        let mut doc_len = 0;
        let mut numeric_fields = HashMap::new();
        let mut tag_fields = HashMap::new();
        let mut term_positions: HashMap<String, Vec<u32>> = HashMap::new();

        for (field_name, field_val) in &fields {
            if let Some(ftype) = schema.fields.get(field_name) {
                match ftype {
                    FieldType::Text { nostem, .. } => {
                        let tokens = tokenize_text(field_val, !*nostem);
                        doc_len += tokens.len();
                        for (pos, tok) in tokens.into_iter().enumerate() {
                            term_positions.entry(tok).or_default().push(pos as u32);
                        }
                    }
                    FieldType::Numeric { .. } => {
                        if let Ok(num) = field_val.trim().parse::<f64>() {
                            numeric_fields.insert(field_name.clone(), num);
                        }
                    }
                    FieldType::Tag { separator, casesensitive } => {
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
            } else {
                // Untyped text fallback
                let tokens = tokenize_text(field_val, true);
                doc_len += tokens.len();
                for (pos, tok) in tokens.into_iter().enumerate() {
                    term_positions.entry(tok).or_default().push(pos as u32);
                }
            }
        }

        for (term, positions) in term_positions {
            let freq = positions.len() as u32;
            let posting = Posting {
                doc_id: doc_id.to_string(),
                term_freq: freq,
                positions,
            };
            self.inverted.entry(term).or_default().push(posting);
        }

        let doc_meta = DocMeta {
            doc_id: doc_id.to_string(),
            doc_len,
            fields,
            numeric_fields,
            tag_fields,
            vector_fields: vectors.unwrap_or_default(),
        };

        self.docs.insert(doc_id.to_string(), doc_meta);
        self.total_docs += 1;
        self.total_terms += doc_len;
    }

    pub fn remove_document(&mut self, doc_id: &str) {
        if let Some(meta) = self.docs.remove(doc_id) {
            self.total_docs = self.total_docs.saturating_sub(1);
            self.total_terms = self.total_terms.saturating_sub(meta.doc_len);

            for postings in self.inverted.values_mut() {
                postings.retain(|p| p.doc_id != doc_id);
            }
            self.inverted.retain(|_, postings| !postings.is_empty());
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
    Ok(())
}

pub fn drop_search_index(name: &str) -> Result<(), String> {
    let mut registry = SEARCH_INDICES.write().unwrap();
    if registry.remove(name).is_some() {
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
    FieldScope { field: String, inner: Box<QueryAst> },
    NumericRange { field: String, min: f64, max: f64 },
    TagFilter { field: String, tags: Vec<String> },
    And(Vec<QueryAst>),
    Or(Vec<QueryAst>),
    Not(Box<QueryAst>),
    KnnVector { field: String, k: usize, query_vec: Vec<f32> },
    MatchAll,
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
            let k: usize = tokens.first().and_then(|val| val.parse().ok()).unwrap_or(10);
            let field = tokens.get(1).map(|s| s.trim_start_matches('@').to_string()).unwrap_or_default();
            let knn_ast = QueryAst::KnnVector {
                field,
                k,
                query_vec: Vec::new(), // Populated via PARAMS
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
            if rest.starts_with('[') {
                // Numeric range: @price:[10 100]
                let mut range_str = rest[1..].to_string();
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
                let min = parts.first().and_then(|v| v.parse().ok()).unwrap_or(f64::NEG_INFINITY);
                let max = parts.get(1).and_then(|v| v.parse().ok()).unwrap_or(f64::INFINITY);
                terms.push(QueryAst::NumericRange {
                    field: field.to_string(),
                    min,
                    max,
                });
            } else if rest.starts_with('{') {
                // Tag filter: @category:{electronics | books}
                let mut tag_str = rest[1..].to_string();
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
                terms.push(QueryAst::And(sub_tokens.into_iter().map(QueryAst::Term).collect()));
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

pub fn execute_search(
    index: &InvertedIndex,
    ast: &QueryAst,
    opts: &SearchOptions,
) -> (usize, Vec<SearchHit>) {
    let mut candidate_scores: HashMap<String, f64> = HashMap::new();

    // 1. Evaluate AST to produce candidate matching documents with scores
    match ast {
        QueryAst::MatchAll => {
            for (doc_id, _) in &index.docs {
                candidate_scores.insert(doc_id.clone(), 1.0);
            }
        }
        QueryAst::Term(t) => {
            if let Some(postings) = index.inverted.get(t) {
                for p in postings {
                    if let Some(doc) = index.docs.get(&p.doc_id) {
                        let score = index.bm25_score(t, p, doc.doc_len);
                        candidate_scores.insert(p.doc_id.clone(), score);
                    }
                }
            }
        }
        QueryAst::Prefix(pfx) => {
            for (term, postings) in &index.inverted {
                if term.starts_with(pfx) {
                    for p in postings {
                        if let Some(doc) = index.docs.get(&p.doc_id) {
                            let score = index.bm25_score(term, p, doc.doc_len);
                            *candidate_scores.entry(p.doc_id.clone()).or_default() += score;
                        }
                    }
                }
            }
        }
        QueryAst::Exact(exact_term) => {
            if let Some(postings) = index.inverted.get(exact_term) {
                for p in postings {
                    if let Some(doc) = index.docs.get(&p.doc_id) {
                        let score = index.bm25_score(exact_term, p, doc.doc_len);
                        candidate_scores.insert(p.doc_id.clone(), score);
                    }
                }
            }
        }
        QueryAst::FieldScope { field, inner } => {
            let (total, hits) = execute_search(index, inner, &SearchOptions { limit: usize::MAX, ..Default::default() });
            if total > 0 {
                for h in hits {
                    if let Some(doc) = index.docs.get(&h.doc_id) {
                        if doc.fields.contains_key(field) {
                            candidate_scores.insert(h.doc_id, h.score);
                        }
                    }
                }
            }
        }
        QueryAst::NumericRange { field, min, max } => {
            for (doc_id, doc) in &index.docs {
                if let Some(&val) = doc.numeric_fields.get(field) {
                    if val >= *min && val <= *max {
                        candidate_scores.insert(doc_id.clone(), 1.0);
                    }
                }
            }
        }
        QueryAst::TagFilter { field, tags } => {
            for (doc_id, doc) in &index.docs {
                if let Some(doc_tags) = doc.tag_fields.get(field) {
                    let matched = tags.iter().any(|t| doc_tags.contains(t));
                    if matched {
                        candidate_scores.insert(doc_id.clone(), 1.0);
                    }
                }
            }
        }
        QueryAst::And(sub_asts) => {
            if !sub_asts.is_empty() {
                // Intersect candidates
                let mut first = true;
                let mut current_set = HashMap::new();
                for sub in sub_asts {
                    let (_, hits) = execute_search(index, sub, &SearchOptions { limit: usize::MAX, ..Default::default() });
                    let sub_map: HashMap<String, f64> = hits.into_iter().map(|h| (h.doc_id, h.score)).collect();
                    if first {
                        current_set = sub_map;
                        first = false;
                    } else {
                        current_set.retain(|id, score| {
                            if let Some(sub_score) = sub_map.get(id) {
                                *score += *sub_score;
                                true
                            } else {
                                false
                            }
                        });
                    }
                }
                candidate_scores = current_set;
            }
        }
        QueryAst::Or(sub_asts) => {
            for sub in sub_asts {
                let (_, hits) = execute_search(index, sub, &SearchOptions { limit: usize::MAX, ..Default::default() });
                for h in hits {
                    *candidate_scores.entry(h.doc_id).or_default() += h.score;
                }
            }
        }
        QueryAst::Not(sub) => {
            let (_, hits) = execute_search(index, sub, &SearchOptions { limit: usize::MAX, ..Default::default() });
            let excluded: HashSet<String> = hits.into_iter().map(|h| h.doc_id).collect();
            for (doc_id, _) in &index.docs {
                if !excluded.contains(doc_id) {
                    candidate_scores.insert(doc_id.clone(), 1.0);
                }
            }
        }
        QueryAst::KnnVector { field, k, query_vec } => {
            // Compute vector cosine distance against all docs having this vector field
            let mut vector_dists = Vec::new();
            for (doc_id, doc) in &index.docs {
                if let Some(doc_vec) = doc.vector_fields.get(field) {
                    if !query_vec.is_empty() && query_vec.len() == doc_vec.len() {
                        let sim = cosine_similarity(query_vec, doc_vec);
                        vector_dists.push((doc_id.clone(), sim));
                    }
                }
            }
            vector_dists.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            for (id, sim) in vector_dists.into_iter().take(*k) {
                candidate_scores.insert(id, sim as f64);
            }
        }
    }

    let total_matches = candidate_scores.len();

    // 2. Sort results
    let mut scored_docs: Vec<(String, f64)> = candidate_scores.into_iter().collect();

    if let Some((sort_field, asc)) = &opts.sortby {
        scored_docs.sort_by(|a, b| {
            let doc_a = index.docs.get(&a.0);
            let doc_b = index.docs.get(&b.0);
            let val_a = doc_a.and_then(|d| d.numeric_fields.get(sort_field).copied())
                .unwrap_or(f64::NEG_INFINITY);
            let val_b = doc_b.and_then(|d| d.numeric_fields.get(sort_field).copied())
                .unwrap_or(f64::NEG_INFINITY);
            if *asc {
                val_a.partial_cmp(&val_b).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                val_b.partial_cmp(&val_a).unwrap_or(std::cmp::Ordering::Equal)
            }
        });
    } else {
        // Default sort by BM25 relevance score descending
        scored_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    }

    // 3. Paginate
    let paged = scored_docs
        .into_iter()
        .skip(opts.offset)
        .take(opts.limit);

    // 4. Construct SearchHit results
    let mut hits = Vec::new();
    for (doc_id, score) in paged {
        let mut fields = HashMap::new();
        if !opts.nocontent {
            if let Some(doc) = index.docs.get(&doc_id) {
                if let Some(ret_fields) = &opts.return_fields {
                    for rf in ret_fields {
                        if let Some(v) = doc.fields.get(rf) {
                            fields.insert(rf.clone(), v.clone());
                        }
                    }
                } else {
                    fields = doc.fields.clone();
                }
            }
        }
        hits.push(SearchHit {
            doc_id,
            score,
            fields,
        });
    }

    (total_matches, hits)
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = (norm_a * norm_b).sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
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
        doc_map.entry(hit.doc_id.clone()).or_insert_with(|| hit.fields.clone());
    }

    for (rank, hit) in vector_hits.iter().enumerate() {
        let rrf = 1.0 / (k + (rank as f64) + 1.0);
        *rrf_scores.entry(hit.doc_id.clone()).or_default() += rrf;
        doc_map.entry(hit.doc_id.clone()).or_insert_with(|| hit.fields.clone());
    }

    let mut merged: Vec<SearchHit> = rrf_scores
        .into_iter()
        .map(|(doc_id, score)| SearchHit {
            fields: doc_map.remove(&doc_id).unwrap_or_default(),
            doc_id,
            score,
        })
        .collect();

    merged.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25_inverted_index_crud() {
        let mut schema_fields = HashMap::new();
        schema_fields.insert("title".to_string(), FieldType::Text { weight: 1.0, sortable: true, nostem: false });
        schema_fields.insert("price".to_string(), FieldType::Numeric { sortable: true });
        schema_fields.insert("tags".to_string(), FieldType::Tag { separator: ',', casesensitive: false });

        let schema = IndexSchema {
            name: "idx:products".to_string(),
            on_type: "HASH".to_string(),
            prefixes: vec!["product:".to_string()],
            fields: schema_fields,
        };

        let mut idx = InvertedIndex::new(schema);

        let mut doc1 = HashMap::new();
        doc1.insert("title".to_string(), "High performance Rust redis engine".to_string());
        doc1.insert("price".to_string(), "99.5".to_string());
        doc1.insert("tags".to_string(), "database, rust, cache".to_string());

        let mut doc2 = HashMap::new();
        doc2.insert("title".to_string(), "Dragonfly memory store in C++".to_string());
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
        let h1 = SearchHit { doc_id: "doc1".to_string(), score: 10.0, fields: HashMap::new() };
        let h2 = SearchHit { doc_id: "doc2".to_string(), score: 8.0, fields: HashMap::new() };
        let h3 = SearchHit { doc_id: "doc3".to_string(), score: 6.0, fields: HashMap::new() };

        let bm25_hits = vec![h1, h2];
        let vec_hits = vec![h2_clone(&bm25_hits[1]), h3];

        let fused = reciprocal_rank_fusion(&bm25_hits, &vec_hits, 60.0);
        // doc2 appears in both lists, so its combined RRF should be highest!
        assert_eq!(fused[0].doc_id, "doc2");
    }

    fn h2_clone(h: &SearchHit) -> SearchHit {
        h.clone()
    }
}
