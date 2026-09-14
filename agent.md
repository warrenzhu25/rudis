# Agent Guidelines & Engineering Invariants for `rudis`

This document defines the mandatory operating guidelines, architectural invariants, and verification workflows for agents and developers contributing to `rudis`.

---

## 1. Core Architectural Invariants

`rudis` is designed from the ground up for extreme throughput and low tail latency on modern multi-core Linux systems. Every modification must uphold these principles:

1. **Shared-Nothing (Thread-per-Core)**:
   - Every worker thread is pinned to an exclusive physical CPU core via `core_affinity`.
   - Each thread runs its own isolated `monoio` event loop driving an independent Linux `io_uring` instance.
   - Ingress utilizes kernel-level `SO_REUSEPORT` balancing so each core accepts connections independently.

2. **Zero Mutexes / Zero Locks in Data Path**:
   - `ShardDb` is purely thread-local. Operations on local keys execute against thread-local `HashMap` instances with zero mutexes, zero atomic operations, and zero cross-core cache invalidation.
   - Under no circumstances should `Arc<Mutex<...>>` or global locks be introduced to the storage engine or data path.

3. **Parallel Cross-Shard Pipeline Squashing**:
   - Pipelined requests from a connection are parsed in batch and grouped by target shard (`CRC16(key) % num_shards`).
   - Local shard operations execute immediately and inline with zero channel hops.
   - Remote shard operations are dispatched as **one single batched hop per destination shard** (`ShardMessage::Batch`), allowing all remote shards to execute concurrently across cores.
   - Responses must always be returned in the exact original FIFO command sequence.

4. **Zero-Allocation Steady State**:
   - **Zero-Copy Parsing**: Payloads must be parsed as zero-copy buffer slices (`Bytes::split_to(len).freeze()`).
   - **Reusable Channels**: Remote batch responders (`ResponderChannel`) must be pre-allocated per connection and reused across loop iterations. Never allocate one-shot channels (`flume::bounded(1)`) on the hot request path.
   - **Memory Allocator**: Uses `mimalloc` as the global allocator (`#[global_allocator] static GLOBAL: mimalloc::MiMalloc`) to guarantee thread-local heap allocation and lock-free cross-thread deallocation.

---

## 2. Commit Requirements & Quality Gates

Every commit to `main` must strictly adhere to the following three rules:

### Rule 1: All Tests Must Pass
* Every commit must pass the full test suite cleanly:
  ```bash
  source $HOME/.cargo/env && cargo test
  ```
* Any new feature (commands, cluster migration, client management) **must** include corresponding tests in `tests/test_server_e2e.rs` or unit tests in `src/`.
* No compiler warnings, dead code, or broken test assertions are permitted.

### Rule 2: Zero Benchmark Regression vs. Baseline
* Performance is a correctness invariant. Any change that degrades throughput or increases latency relative to the documented baselines must be rejected or optimized before committing.
* Current performance milestones (60-second test, 100% SET, 1KB payload, pipeline 100):
  * **1 Thread**: $\ge 816,000$ Ops/sec, $\le 4.0$ ms average latency.
  * **16 Threads**: $\ge 2,630,000$ Ops/sec, $\le 1.2$ ms average latency, $\le 2.9$ ms p99 tail latency.
* Before committing structural or networking changes, verify with the benchmark protocol (see Section 3).

### Rule 3: Small, Granular, Atomic Commits
* Each logical change must be a distinct, self-contained commit.
* Use clear conventional commit prefixes:
  * `feat:` new user-facing functionality or commands
  * `perf:` performance optimizations
  * `fix:` bug fixes
  * `test:` test additions or improvements
  * `docs:` benchmark records and documentation
* Always push completed commits to `origin main`.

---

## 3. Standard Benchmark Protocol

To ensure reproducible, comparable benchmarks across runs:

### Tool & Environment
* **Benchmark Tool**: `/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark`
* **CPU Core Isolation**:
  * **Server**: Pinned to lower cores (e.g. `taskset -c 0` for 1T, `taskset -c 0-15` for 16T).
  * **Client**: Pinned to upper cores (e.g. `taskset -c 32-63` for 32 client threads).

### Standard Workload Specification
```bash
taskset -c 32-63 /usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark \
    --server 127.0.0.1 --port 6379 \
    --clients 1 --threads 32 --ratio 1:0 --data-size 1024 \
    --pipeline 100 --key-minimum 1 --key-maximum 1000000 \
    --key-pattern S:S \
    --print-percentiles 50,90,95,99,99.9 \
    --test-time 60 \
    --hide-histogram
```

### Benchmark Documents
All official benchmark numbers must be committed under `docs/benchmarks/`:
* `docs/benchmarks/baseline.md`: Initial multi-threaded baseline.
* `docs/benchmarks/write_batching.md`: Write-batching & socket tuning milestone.
* `docs/benchmarks/pipeline_squashing.md`: Cross-shard squashing milestone vs Dragonfly.

---

## 4. Codebase Architecture Map

| Module | Location | Purpose |
| :--- | :--- | :--- |
| **Server Runtime** | `src/server.rs` | Worker thread initialization, `SO_REUSEPORT` binding, Monoio `io_uring` event loop, active expiration cycle task, cross-shard receiver mesh. |
| **Connection & Protocol** | `src/connection.rs` | Per-connection async loop, pipeline squashing, reusable channel pool, socket write batching. |
| **Shard Store** | `src/shard.rs` | Thread-local `ShardDb`, key-value store, expiration timestamps, active/passive TTL eviction. |
| **Router** | `src/router.rs` | CRC16 key hashing (`target_shard`), cross-shard routing mesh senders. |
| **RESP Engine** | `src/resp.rs` | Zero-copy parser for RESP arrays and inline Redis commands. |
| **Entrypoint** | `src/main.rs` | CLI arguments, core affinity mapping, cross-shard channel instantiation. |
| **Tests** | `tests/` | Multi-threaded end-to-end tests (`test_server_e2e.rs`) and channel waker verification (`test_cross_thread.rs`). |
