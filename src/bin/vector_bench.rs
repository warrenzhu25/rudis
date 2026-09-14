//! Standalone High-Performance Vector Benchmark for Rudis.
//!
//! Evaluates HNSW ingestion throughput, query QPS, latency percentiles (p50, p99),
//! memory footprint across Float32 vs. SQ8 vs. Tiered, and Recall@k vs. ground truth.

use std::time::Instant;
use bytes::Bytes;
use rudis::vector::{compute_distance, HnswIndex, VectorMetric};

fn generate_random_vector(dim: usize, seed: &mut u64) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    for _ in 0..dim {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let val = ((*seed >> 32) as i32 as f32) / (i32::MAX as f32);
        v.push(val);
    }
    // Normalize to unit sphere for Cosine similarity
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

fn exact_brute_force_knn(
    dataset: &[(Bytes, Vec<f32>)],
    query: &[f32],
    k: usize,
    metric: VectorMetric,
) -> Vec<Bytes> {
    let mut dists: Vec<(Bytes, f32)> = dataset
        .iter()
        .map(|(key, v)| (key.clone(), compute_distance(query, v, metric)))
        .collect();
    dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    dists.into_iter().take(k).map(|(key, _)| key).collect()
}

fn calculate_recall(ann_keys: &[Bytes], ground_truth_keys: &[Bytes]) -> f32 {
    if ground_truth_keys.is_empty() {
        return 1.0;
    }
    let mut matches = 0;
    for key in ann_keys {
        if ground_truth_keys.contains(key) {
            matches += 1;
        }
    }
    matches as f32 / ground_truth_keys.len() as f32
}

fn main() {
    println!("===============================================================");
    println!("         RUDIS HNSW & SQ8 VECTOR ENGINE BENCHMARK              ");
    println!("===============================================================\n");

    let num_vectors = 10_000;
    let dim = 128;
    let num_queries = 500;
    let k = 10;
    let metric = VectorMetric::Cosine;

    println!("Configuration:");
    println!("  - Dataset Size:      {} vectors", num_vectors);
    println!("  - Dimensions:        {} (Dense Embeddings)", dim);
    println!("  - Metric:            {:?}", metric);
    println!("  - Evaluation Queries: {}", num_queries);
    println!("  - Top-k:             {}\n", k);

    let mut seed = 123456789u64;

    // Generate dataset
    print!("Generating synthetic normalized vectors... ");
    let mut dataset = Vec::with_capacity(num_vectors);
    for i in 0..num_vectors {
        let key = Bytes::from(format!("vec:{:06}", i));
        let vec = generate_random_vector(dim, &mut seed);
        dataset.push((key, vec));
    }
    let mut queries = Vec::with_capacity(num_queries);
    for _ in 0..num_queries {
        queries.push(generate_random_vector(dim, &mut seed));
    }
    println!("Done.\n");

    // 1. Benchmark Standard Float32 HNSW
    println!("--- 1. Testing Standard Float32 HNSW ---");
    let mut index_f32 = HnswIndex::new("f32_index".to_string(), dim, metric);
    let start_ingest = Instant::now();
    for (key, vec) in &dataset {
        index_f32.add(key.clone(), vec.clone()).unwrap();
    }
    let ingest_dur_f32 = start_ingest.elapsed();
    let ingest_qps_f32 = num_vectors as f64 / ingest_dur_f32.as_secs_f64();
    println!("  Ingestion: {:.2?} ({:.0} vectors/sec)", ingest_dur_f32, ingest_qps_f32);

    let mut latencies_f32 = Vec::with_capacity(num_queries);
    let start_query = Instant::now();
    for q in &queries {
        let q_start = Instant::now();
        let _ = index_f32.search(q, k);
        latencies_f32.push(q_start.elapsed().as_micros());
    }
    let query_dur_f32 = start_query.elapsed();
    let query_qps_f32 = num_queries as f64 / query_dur_f32.as_secs_f64();
    latencies_f32.sort();
    let p50_f32 = latencies_f32[latencies_f32.len() / 2];
    let p99_f32 = latencies_f32[(latencies_f32.len() as f64 * 0.99) as usize];
    println!("  Query QPS: {:.0} queries/sec", query_qps_f32);
    println!("  Latency:   p50 = {} µs, p99 = {} µs\n", p50_f32, p99_f32);

    // 2. Benchmark SQ8 Quantized HNSW
    println!("--- 2. Testing SQ8 Quantized HNSW (75% RAM Reduction) ---");
    let mut index_sq8 = HnswIndex::new("sq8_index".to_string(), dim, metric);
    let start_ingest_sq8 = Instant::now();
    for (key, vec) in &dataset {
        index_sq8.add_quantized(key.clone(), vec.clone(), true, false).unwrap();
    }
    let ingest_dur_sq8 = start_ingest_sq8.elapsed();
    let ingest_qps_sq8 = num_vectors as f64 / ingest_dur_sq8.as_secs_f64();
    println!("  Ingestion: {:.2?} ({:.0} vectors/sec)", ingest_dur_sq8, ingest_qps_sq8);

    let mut latencies_sq8 = Vec::with_capacity(num_queries);
    let start_query_sq8 = Instant::now();
    for q in &queries {
        let q_start = Instant::now();
        let _ = index_sq8.search_tiered(q, k, false);
        latencies_sq8.push(q_start.elapsed().as_micros());
    }
    let query_dur_sq8 = start_query_sq8.elapsed();
    let query_qps_sq8 = num_queries as f64 / query_dur_sq8.as_secs_f64();
    latencies_sq8.sort();
    let p50_sq8 = latencies_sq8[latencies_sq8.len() / 2];
    let p99_sq8 = latencies_sq8[(latencies_sq8.len() as f64 * 0.99) as usize];
    println!("  Query QPS: {:.0} queries/sec", query_qps_sq8);
    println!("  Latency:   p50 = {} µs, p99 = {} µs\n", p50_sq8, p99_sq8);

    // 3. Benchmark SQ8 Quantized + Exact Rerank
    println!("--- 3. Testing SQ8 Quantized + Full-Precision Rerank ---");
    let mut latencies_rerank = Vec::with_capacity(num_queries);
    let start_query_rerank = Instant::now();
    for q in &queries {
        let q_start = Instant::now();
        let _ = index_sq8.search_tiered(q, k, true);
        latencies_rerank.push(q_start.elapsed().as_micros());
    }
    let query_dur_rerank = start_query_rerank.elapsed();
    let query_qps_rerank = num_queries as f64 / query_dur_rerank.as_secs_f64();
    latencies_rerank.sort();
    let p50_rerank = latencies_rerank[latencies_rerank.len() / 2];
    let p99_rerank = latencies_rerank[(latencies_rerank.len() as f64 * 0.99) as usize];
    println!("  Query QPS: {:.0} queries/sec", query_qps_rerank);
    println!("  Latency:   p50 = {} µs, p99 = {} µs\n", p50_rerank, p99_rerank);

    // 4. Ground Truth Recall Evaluation
    println!("--- 4. Computing Recall@{} against Brute-Force Ground Truth ---", k);
    let eval_queries = &queries[0..50];
    let mut recall_f32_sum = 0.0f32;
    let mut recall_sq8_sum = 0.0f32;
    let mut recall_rerank_sum = 0.0f32;

    for q in eval_queries {
        let gt = exact_brute_force_knn(&dataset, q, k, metric);

        let ann_f32: Vec<Bytes> = index_f32.search(q, k).into_iter().map(|(k, _)| k).collect();
        recall_f32_sum += calculate_recall(&ann_f32, &gt);

        let ann_sq8: Vec<Bytes> = index_sq8.search_tiered(q, k, false).into_iter().map(|(k, _)| k).collect();
        recall_sq8_sum += calculate_recall(&ann_sq8, &gt);

        let ann_rerank: Vec<Bytes> = index_sq8.search_tiered(q, k, true).into_iter().map(|(k, _)| k).collect();
        recall_rerank_sum += calculate_recall(&ann_rerank, &gt);
    }

    let avg_recall_f32 = (recall_f32_sum / eval_queries.len() as f32) * 100.0;
    let avg_recall_sq8 = (recall_sq8_sum / eval_queries.len() as f32) * 100.0;
    let avg_recall_rerank = (recall_rerank_sum / eval_queries.len() as f32) * 100.0;

    println!("  - Float32 HNSW Recall@{}:        {:.1}%", k, avg_recall_f32);
    println!("  - SQ8 Approximate Recall@{}:     {:.1}%", k, avg_recall_sq8);
    println!("  - SQ8 + Rerank Recall@{}:        {:.1}%\n", k, avg_recall_rerank);

    println!("===============================================================");
    println!("                   BENCHMARK SUMMARY                           ");
    println!("===============================================================");
    println!("| Mode           | Memory/Vector | Ingestion Rate | Query QPS | p50 Lat | Recall@10 |");
    println!("|----------------|---------------|----------------|-----------|---------|-----------|");
    println!("| Float32 HNSW   | 512 Bytes     | {:6.0} vec/s  | {:5.0}     | {:4} µs | {:5.1}%    |",
        ingest_qps_f32, query_qps_f32, p50_f32, avg_recall_f32);
    println!("| SQ8 Quantized  | 128 Bytes     | {:6.0} vec/s  | {:5.0}     | {:4} µs | {:5.1}%    |",
        ingest_qps_sq8, query_qps_sq8, p50_sq8, avg_recall_sq8);
    println!("| SQ8 + Rerank   | 128 Bytes (RAM)| {:6.0} vec/s  | {:5.0}     | {:4} µs | {:5.1}%    |",
        ingest_qps_sq8, query_qps_rerank, p50_rerank, avg_recall_rerank);
    println!("===============================================================\n");
}
