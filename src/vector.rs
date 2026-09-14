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

/// Computes SIMD-friendly dot product of two float vectors.
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
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

/// Computes SIMD-friendly squared L2 Euclidean distance.
#[inline]
pub fn l2_distance_sq(a: &[f32], b: &[f32]) -> f32 {
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

#[derive(Clone, Debug)]
pub struct HnswNode {
    pub id: usize,
    pub key: Bytes,
    pub vector: Vec<f32>,
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
            rng_state: 0x853c49e6748fea9b,
        }
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

    /// Adds or updates a vector in the HNSW index.
    pub fn add(&mut self, key: Bytes, vector: Vec<f32>) -> Result<(), &'static str> {
        if vector.len() != self.dim {
            return Err("vector dimension mismatch");
        }

        // Remove previous key if exists
        if self.key_to_id.contains_key(&key) {
            self.remove(&key);
        }

        let target_level = self.random_level();
        let new_id = self.nodes.len();

        let mut node = HnswNode {
            id: new_id,
            key: key.clone(),
            vector: vector.clone(),
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
        let mut curr_dist = compute_distance(
            &vector,
            &self.nodes[curr_obj].as_ref().unwrap().vector,
            self.metric,
        );

        // 1. Greedy search from top down to target_level + 1
        for lc in (target_level + 1..=self.max_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(curr_node) = &self.nodes[curr_obj] {
                    if lc < curr_node.neighbors.len() {
                        for &neighbor in &curr_node.neighbors[lc] {
                            if let Some(n) = &self.nodes[neighbor] {
                                let d = compute_distance(&vector, &n.vector, self.metric);
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
        let search_level_max = target_level.min(self.max_layer);
        for lc in (0..=search_level_max).rev() {
            let candidates = self.search_layer(&vector, curr_obj, self.ef_construction, lc);
            let m_max = if lc == 0 { self.m0 } else { self.m };
            let neighbors: Vec<usize> = candidates.into_iter().take(m_max).map(|c| c.id).collect();

            // Connect new node to neighbors
            node.neighbors[lc] = neighbors.clone();

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

        self.nodes.push(Some(node));
        self.key_to_id.insert(key, new_id);

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
                    self.nodes[id]
                        .as_ref()
                        .map(|n| (id, compute_distance(&node_vec, &n.vector, self.metric)))
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

        let initial_dist = compute_distance(
            query,
            &self.nodes[entry_point].as_ref().unwrap().vector,
            self.metric,
        );

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
                                let d = compute_distance(query, &nbr_node.vector, self.metric);
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
        if self.entry_point.is_none() || self.is_empty() {
            return Vec::new();
        }

        let mut curr_obj = self.entry_point.unwrap();
        let mut curr_dist = compute_distance(
            query,
            &self.nodes[curr_obj].as_ref().unwrap().vector,
            self.metric,
        );

        // 1. Greedy search down to layer 1
        for lc in (1..=self.max_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(curr_node) = &self.nodes[curr_obj] {
                    if lc < curr_node.neighbors.len() {
                        for &nbr in &curr_node.neighbors[lc] {
                            if let Some(n) = &self.nodes[nbr] {
                                let d = compute_distance(query, &n.vector, self.metric);
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
        let ef = self.ef_search.max(k);
        let candidates = self.search_layer(query, curr_obj, ef, 0);

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
}
