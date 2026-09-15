# Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion (`src/search.rs`)

## 1. Architectural Purpose & Scope

`src/search.rs` provides an in-memory full-text search and indexing engine compatible with RediSearch (`FT.CREATE`, `FT.SEARCH`, `FT.DROPINDEX`, `FT.INFO`). It supports multi-field schema definitions (`TEXT`, `TAG`, `NUMERIC`, `VECTOR`), inverted index posting lists, **Okapi BM25** relevance scoring, and **Reciprocal Rank Fusion (RRF)** for hybrid search.

---

## 2. Key Invariants & Concurrency Constraints

1. **Automatic Document Ingestion**: When hashes or JSON documents matching an index prefix are created or updated (`HSET`, `JSON.SET`), the search engine automatically indexes their fields.
2. **Deterministic Tokenization**: Text is normalized, lowercased, stripped of punctuation, and filtered against an English stop-word dictionary.
3. **Lexical and Vector Hybrid Fusion**: Combines lexical relevance rankings with dense vector K-NN similarity using Reciprocal Rank Fusion with standard constant $k = 60$.
4. **Thread-Local Indexing**: Indexes live in the shard managing the document keys, enabling distributed parallel search across the cross-shard mesh.

---

## 3. Component Architecture & Data Structures

```
     Raw Document: HSET "doc:1" title "Rust Systems" body "Distributed io_uring"
                                    │
                                    ▼ (Schema Prefix Matching: "doc:")
                         Tokenization & Extraction
                                    │
                  ┌─────────────────┴─────────────────┐
                  ▼                                   ▼
             Text Fields                         Tag Fields
        ["rust", "systems"]                  ["tech", "database"]
                  │                                   │
                  ▼                                   ▼
          Inverted Posting List                Tag Hash Set
      "rust"    -> [(Doc 1, tf: 1)]         "tech" -> [Doc 1, Doc 4]
      "systems" -> [(Doc 1, tf: 1)]
```

### Core Data Structures

```rust
pub enum FieldType {
    Text { weight: f32 },
    Tag { separator: u8 },
    Numeric,
    Vector { dim: usize, distance_metric: MetricType },
}

pub struct SearchSchema {
    pub name: Bytes,
    pub prefix: Bytes,
    pub fields: HashMap<String, FieldType>,
}

pub struct InvertedIndex {
    pub schema: SearchSchema,
    // Term -> List of (DocId, TermFrequency)
    pub postings: HashMap<String, Vec<(u32, u32)>>,
    // Document ID -> Document Metadata
    pub docs: HashMap<u32, DocumentMeta>,
    pub total_docs: usize,
    pub total_field_lengths: HashMap<String, usize>,
    pub vector_index: Option<HnswIndex>,
}

pub struct DocumentMeta {
    pub external_key: Bytes,
    pub field_lengths: HashMap<String, usize>,
}
```

---

## 4. Execution Algorithms & Code Logic

### 4.1 Okapi BM25 Scoring Algorithm

$$\text{Score}(D, Q) = \sum_{t \in Q} \text{IDF}(t) \cdot \frac{f(t, D) \cdot (k_1 + 1)}{f(t, D) + k_1 \cdot \left(1 - b + b \cdot \frac{|D|}{\text{avgdl}}\right)}$$

- $k_1 = 1.2$: Controls term frequency saturation limit.
- $b = 0.75$: Controls document length normalization penalty.

```rust
impl InvertedIndex {
    pub fn score_bm25(&self, query_terms: &[String], doc_id: u32, field: &str) -> f32 {
        let mut score = 0.0f32;
        let k1 = 1.2f32;
        let b = 0.75f32;

        let doc_meta = match self.docs.get(&doc_id) {
            Some(m) => m,
            None => return 0.0,
        };

        let doc_len = *doc_meta.field_lengths.get(field).unwrap_or(&1) as f32;
        let total_docs = self.total_docs as f32;
        let avg_dl = (*self.total_field_lengths.get(field).unwrap_or(&1) as f32) / total_docs;

        for term in query_terms {
            if let Some(postings) = self.postings.get(term) {
                // Find term frequency in this document
                if let Some(&(_, tf)) = postings.iter().find(|(d, _)| *d == doc_id) {
                    let df = postings.len() as f32;
                    // Inverse Document Frequency (IDF)
                    let idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();

                    let tf_norm = (tf as f32 * (k1 + 1.0))
                        / (tf as f32 + k1 * (1.0 - b + b * (doc_len / avg_dl)));

                    score += idf * tf_norm;
                }
            }
        }
        score
    }
}
```

### 4.2 Reciprocal Rank Fusion (RRF) for Hybrid Search

When combining BM25 keyword rankings with dense vector K-NN similarity rankings:

```rust
pub fn reciprocal_rank_fusion(
    bm25_ranks: &[Bytes], // Document keys ordered by text relevance
    vector_ranks: &[Bytes], // Document keys ordered by vector similarity
    top_k: usize,
) -> Vec<Bytes> {
    let k = 60.0f32; // Standard RRF damping constant
    let mut scores: HashMap<Bytes, f32> = HashMap::new();

    // Score from BM25 ranking
    for (rank, doc_key) in bm25_ranks.iter().enumerate() {
        let rrf_score = 1.0 / (k + (rank + 1) as f32);
        *scores.entry(doc_key.clone()).or_default() += rrf_score;
    }

    // Score from Vector K-NN ranking
    for (rank, doc_key) in vector_ranks.iter().enumerate() {
        let rrf_score = 1.0 / (k + (rank + 1) as f32);
        *scores.entry(doc_key.clone()).or_default() += rrf_score;
    }

    // Sort descending by combined RRF score
    let mut sorted_docs: Vec<(Bytes, f32)> = scores.into_iter().collect();
    sorted_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    sorted_docs.into_iter().take(top_k).map(|(doc, _)| doc).collect()
}
```

---

## 5. Cross-Component Interactions

- **`src/vector.rs`**: Supplies `HnswIndex` to execute vector K-NN searches when `@vector` fields are present.
- **`src/connection.rs`**: Invokes `ft_create` and `ft_search` handlers, serializing document attributes and highlighted snippets.
- **`src/table.rs`**: Intercepts `HSET` and `JSON.SET` updates to trigger automated document re-indexing.

---

## 6. Performance Characteristics

- **Zero-Allocation Tokenizer**: Tokenization operates over string slices, avoiding string copies where possible.
- **Sparse Posting Lists**: Compressed postings use delta-encoded document IDs for minimal memory overhead.
