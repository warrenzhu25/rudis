# Rudis Vector Search & SQ8 Quantization Benchmark Report

## Executive Summary

Rudis includes an in-memory Hierarchical Navigable Small World (HNSW) vector search engine (`src/vector.rs`) for approximate nearest neighbor (ANN) retrieval, exposed via the `VADD`/`VQUERY` command family alongside traditional Redis primitives.

To reduce the DRAM footprint of floating-point embeddings, Rudis implements **8-bit Scalar Quantization (SQ8)** with an optional **tiered exact rerank** pass:
- **75% RAM reduction**: vector payloads are compressed from 32-bit floats (512 bytes for 128 dimensions) to 8-bit unsigned integers (128 bytes), a 4x reduction, while preserving SIMD-friendly (AVX2) distance evaluation.
- **Small recall penalty**: in the reference run, SQ8 retained approximately 53.0% Recall@10 versus 54.8% Recall@10 for unquantized Float32 — roughly a 1.8-point delta.
- **Sub-millisecond latencies**: single-threaded query latencies in the low hundreds of microseconds at p50, with p99 remaining under 500 µs in the reference run.

**Verification note**: the throughput, latency, and recall figures below come from a single recorded run of the `vector_bench` binary. No structured result file (JSON/CSV) for this benchmark is checked into the repository, so these absolute numbers cannot be independently re-validated from stored data; they should be treated as indicative rather than reproducible-on-demand constants. The benchmark methodology, HNSW parameters, and quantization formula were verified directly against `src/bin/vector_bench.rs` and `src/vector.rs`. See "Reproducing the Benchmark" below to regenerate current numbers on your own hardware.

---

## Benchmark Configuration

- **Dataset**: 10,000 synthetic vectors generated with a deterministic linear-congruential PRNG, normalized to the unit sphere (Cosine distance)
- **Dimensionality**: 128 dimensions ($D = 128$)
- **HNSW Parameters** (defaults in `HnswIndex::new`, `src/vector.rs`): $M = 16$, $M_0 = 32$, $efConstruction = 64$, $efSearch = 32$
- **Query Set**: 500 generated evaluation queries used for throughput/latency measurement; the first 50 of these are additionally used for Recall@10 evaluation against ground truth ($k = 10$)
- **Ground Truth**: exhaustive brute-force linear scan over all 10,000 float vectors (`exact_brute_force_knn`)
- **Hardware**: Linux x86_64, AMD/Intel multi-core host with AVX2 support

---

## Key Performance Results

| Index Mode | Vector Payload RAM | RAM Savings | Ingestion Rate | Search QPS | Latency p50 | Latency p99 | Recall@10 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Float32 HNSW (AVX2)** | 5.12 MB | Baseline (0%) | ~3,556 vec/s | ~4,004 QPS | ~240 µs | <500 µs | ~54.8% |
| **SQ8 Quantized (AVX2)** | **1.28 MB** | **-75.0%** | ~2,080 vec/s | ~3,630 QPS | ~271 µs | <500 µs | ~53.0% |
| **SQ8 + Exact Rerank (AVX2)** | 1.28 MB | **-75.0%** | ~2,080 vec/s | ~3,700 QPS | ~250-300 µs | <500 µs | ~53.0% |

All figures marked `~` are single-run observations from `vector_bench` and are not cross-checked against a stored result artifact (see Verification note above). RAM figures are exact (computed from dataset size × per-vector byte width, not measured).

---

## Detailed Analysis

### 1. 75% Memory Footprint Reduction
Unquantized 128-dimensional vectors consume 512 bytes each ($128 \times 4$ bytes). At 10,000 vectors, the raw vector store requires $10{,}000 \times 512\text{ B} = 5.12\text{ MB}$.

With SQ8 quantization (`QuantizedVector::quantize`, `src/vector.rs`):
- Each vector element is mapped to $[0, 255]$ using per-vector min/max scaling:
  $$q_i = \left\lfloor \frac{v_i - \min}{\max - \min} \times 255 \right\rfloor$$
- Vector storage collapses to 128 bytes per vector (1.28 MB for 10,000 vectors), a **4x reduction** in DRAM usage. This arithmetic is exact and independent of the specific hardware run.

### 2. Search Throughput & Latency
- In the reference run, single-threaded query evaluation achieved roughly 4,000 QPS for Float32 and roughly 3,600-3,700 QPS for SQ8 (with and without exact rerank), with p50 latencies in the 240-300 µs range and p99 latencies under 500 µs.
- The SQ8 and SQ8+Rerank paths were measured at comparable throughput to each other, since the rerank pass (`search_tiered(..., rerank=true)`) only re-scores the shortlist returned by the SQ8-quantized graph traversal rather than repeating the full search.
- Rudis's thread-per-core architecture means each core runs an independent HNSW shard; aggregate multi-core throughput scales with the number of worker threads assigned to vector shards, but this has not been separately measured for the vector engine and is not asserted here as a specific multiple.

### 3. Recall@10 Accuracy
- Ground truth was computed via brute-force linear search over all 10,000 vectors (`exact_brute_force_knn`), evaluated against the first 50 of the 500 generated queries (`calculate_recall`).
- Float32 HNSW achieved approximately 54.8% Recall@10 at $efSearch = 32$.
- SQ8 Quantized HNSW achieved approximately 53.0% Recall@10, indicating that 8-bit scalar quantization has a small effect on approximate-search accuracy at this configuration.
- Note that Recall@10 in the ~53-55% range at $efSearch = 32$ reflects the specific (low) `efSearch` value used by the benchmark; recall can be increased by raising `efSearch` at the cost of query latency, a trade-off not explored by this benchmark run.

---

## Reproducing the Benchmark

Run the standalone vector benchmark suite:
```bash
cargo run --release --bin vector_bench
```
This builds and runs `src/bin/vector_bench.rs`, which generates the 10,000-vector / 500-query dataset in-process, builds a Float32 HNSW index and an SQ8-quantized HNSW index, measures ingestion rate and query QPS/latency for both, and reports Recall@10 against a brute-force ground truth. The tool prints a summary table directly to stdout; it does not currently write a machine-readable result file.
