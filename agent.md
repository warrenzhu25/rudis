# Agent Guidelines & Engineering Invariants for `rudis`

This document defines the mandatory operating guidelines, architectural invariants, and verification workflows for agents and developers contributing to `rudis`.

---

## 1. Core Architectural Invariants

`rudis` is designed from the ground up for extreme throughput and low tail latency on modern multi-core Linux systems. Every modification must uphold these principles:

1. **Shared-Nothing (Thread-per-Core)**:
   - Every worker thread is pinned to an exclusive physical CPU core via `core_affinity` (disabled with `--no-pin`).
   - Each thread runs its own isolated `monoio` event loop driving an independent Linux `io_uring` instance.
   - Ingress utilizes kernel-level `SO_REUSEPORT` balancing so each core accepts connections independently.
   - The default number of shards is `min(available_cores, 8)` (see `src/main.rs`), overridable via `--threads`/`threads`. It need not equal the physical core count.
   - Key routing is mode-dependent: **standalone mode (default)** hashes the key's hash-tag with `FxHash` modulo `num_shards` — there is no 16,384-slot table involved. **Cluster mode** (`cluster-enabled yes`) uses the classic Redis Cluster scheme, `CRC16(hash_tag) % 16384`, mapped onto per-shard slot ranges, for wire compatibility with Redis Cluster clients. Do not assume CRC16/slot routing is universal — it is cluster-mode-only.

2. **Zero Mutexes / Zero Locks in the Per-Key Data Path**:
   - `ShardDb` is purely thread-local. Operations on local keys execute against thread-local table instances with zero mutexes, zero atomic operations, and zero cross-core cache invalidation on the read/write hot path.
   - Under no circumstances should `Arc<Mutex<...>>` or a global lock be introduced around `ShardDb`/`RudisTable` itself or any other per-key data structure.
   - **This invariant has real, deliberate, narrow exceptions elsewhere in the codebase — know them before assuming "zero locks" is absolute:**
     - `src/mailbox.rs`'s cross-shard SPSC ring (capacity 256 per shard pair) is lock-free, but each ring has a `std::sync::Mutex`-guarded overflow `VecDeque` for bursts beyond that capacity. This is a real `Mutex` on the cross-shard message path, gated to the rare overflow case — it is not lock-free in the strict sense, and future changes to the mailbox must preserve (or explicitly re-justify changing) that scoping.
     - `BlockHub` (`src/block.rs`), the cluster topology registry (`ClusterHub`, `src/cluster.rs`), the global search-index registry (`src/search.rs`), and several control-plane globals in `src/connection.rs` (buffer limits, client tracker, `CMD_STATS`, `MAX_MEMORY_POLICY`) are process-wide state protected by `RwLock`/atomics, not thread-local. These are intentional, documented exceptions for state that must be visible identically across every shard (blocking waiters, cluster membership, search index metadata, admin/observability counters) — never treat them as evidence that the per-key hot path is allowed to take a lock, and never add a new global lock without the same level of justification these have in `docs/design/`.
   - **Memory Allocator**: the actual `#[global_allocator]` (see `src/lib.rs`) is `tikv_jemallocator::Jemalloc`, exposed with live stats via `tikv-jemalloc-ctl`. `mimalloc` is listed in `Cargo.toml` but is **not** the configured global allocator — do not assume it is wired in; if you see `mimalloc` referenced in older documentation or comments, treat it as stale.

3. **Parallel Cross-Shard Pipeline Squashing**:
   - Pipelined requests from a connection are parsed in batch and grouped by target shard using the routing rule in point 1 above (not unconditionally CRC16).
   - Local shard operations execute immediately and inline with zero channel hops.
   - Remote shard operations are dispatched as **one single batched hop per destination shard** (`ShardMessage::Batch`, see `src/shard.rs`), allowing all remote shards to execute concurrently across cores.
   - Responses must always be returned in the exact original FIFO command sequence.

4. **Zero-Allocation Steady State (Hot Paths) — Goal, With Known Exceptions**:
   - **Zero-Copy Parsing**: Payloads must be parsed as zero-copy buffer slices (`Bytes::split_to(len).freeze()`).
   - **Reusable Channels on the Fast Path**: The hottest cross-shard call sites (`GET`, `SET`, batched `MGET`/`MSET`, and similar) reuse pooled, pre-allocated reply descriptors (`FastGetDescriptor`, `FastSetDescriptor`, `BatchResponder`, `ScatterMgetDescriptor`, `ScatterMsetDescriptor` in `src/mailbox.rs`) instead of allocating a channel per call. **New hot-path code must follow this pattern, not allocate `flume::bounded(1)` per call.**
   - Be aware this is a **standard to converge on, not yet a universally-enforced invariant**: many other `ShardMessage` variants outside the hottest call sites still allocate a fresh `flume::bounded(1)` request/reply channel per remote call today (see `docs/internal/04_sharding_mesh.md` §3 for the current inventory). Do not assume every cross-shard command already uses a pooled descriptor — check the specific message variant before relying on that assumption, and prefer migrating a variant to a pooled descriptor over adding new one-shot-channel call sites.

---

## 2. Commit Requirements & Quality Gates

Every commit to `main` must strictly adhere to the following four rules:

### Rule 1: All Tests Must Pass & Mandatory Dual-Test Coverage (Unit + Integration)
* Every commit must pass the full test suite cleanly:
  ```bash
  source $HOME/.cargo/env && cargo test
  ```
* **Mandatory Dual-Test Coverage (Unit + Integration Tests)**:
  * Every change (bug fix, new feature, or architectural modification) **must** include both:
    1. **Unit Test(s)** in `src/` (e.g., in module-level `#[cfg(test)] mod tests` blocks) testing the isolated logic, edge conditions, AST/protocol parsing, or internal state transitions.
    2. **Integration Test(s)** in `tests/` (e.g., `tests/test_server_e2e.rs`) testing end-to-end client-server behavior over actual TCP sockets, command pipelines, clustering, or persistence replays.
  * No change may be merged or committed with only one testing tier or without dedicated tests asserting the specific behavior.
* No compiler warnings, dead code, or broken test assertions are permitted.

### Rule 2: Zero Benchmark Regression vs. Baseline
* Performance is a correctness invariant. Any change that degrades throughput or increases latency relative to the documented baselines must be rejected or optimized before committing.
* Current performance milestones (60-second test, 100% SET, 1KB payload, pipeline 100):
  * **1 Thread**: $\ge 816,000$ Ops/sec, $\le 4.0$ ms average latency.
  * **16 Threads**: $\ge 2,630,000$ Ops/sec, $\le 1.2$ ms average latency, $\le 2.9$ ms p99 tail latency.
* Before committing structural or networking changes, verify with the benchmark protocol (see Section 3).

### Rule 3: Strict Atomicity — One Logical Change Per Commit
* **Never bundle multiple distinct features or bug fixes into a single commit**:
  * If a task resolves 4 bugs (e.g. `FCALL` AOF, KNN vector search, CRDT sharding, slot migration check), it **must** be committed as **4 distinct, sequential commits**, not 1 omnibus commit.
  * Each commit must represent a single, cohesive, self-contained unit of work.
* **Bisectability & Verification**:
  * Every single commit in the git history must independently compile, pass `cargo clippy --all-targets -- -D warnings`, and pass all tests (`cargo test`).
  * Never commit an intermediate broken state with the intention of fixing it in a subsequent commit.
* **Atomic Scope & Commit Messages**:
  * Use clear conventional commit headers with precise subsystem scopes:
    * `fix(scripting): thread router AOF writer to call_function in FCALL`
    * `fix(search): wire PARAMS vector blob into FT.SEARCH KnnVector and add_document`
    * `fix(crdt): route keyed CRDT commands through target_shard and execute_local_command`
    * `fix(cluster): enforce slot ownership check in execute_commands_squashed pipeline`
    * `feat(scope):` new user-facing functionality or commands
    * `perf(scope):` performance and memory optimizations
    * `test(scope):` test additions or improvements
    * `docs(scope):` documentation and benchmark records

### Rule 4: Every Commit Must Pass CI Before Push
* **Every commit must pass the full CI quality gate before pushing to `origin main`**:
  Under no circumstances should an unverified commit or failing test ever be pushed. The entire CI check suite matching `.github/workflows/ci.yml` must be executed and pass with zero failures and zero warnings:
  ```bash
  # 1. Check code formatting
  cargo fmt --check

  # 2. Run Clippy linter with zero warnings tolerated
  cargo clippy --all-targets -- -D warnings

  # 3. Run all unit tests
  cargo test --lib

  # 4. Run cross-thread integration tests
  cargo test --test test_cross_thread

  # 5. Run end-to-end integration tests (serial)
  cargo test --test test_server_e2e -- --test-threads=1
  ```
* **Immediate Push Upon Green CI**:
  Once the CI suite passes cleanly, the commit must be pushed immediately to `origin main`:
  ```bash
  git push origin main
  ```
* Never leave completed commits unpushed, and never push without verifying the full CI suite. Local `main` and `origin/main` must remain synchronized and green at all times.


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

| Module | Location | Purpose | Design Doc |
| :--- | :--- | :--- | :--- |
| **Server Runtime** | `src/server.rs`, `src/main.rs` | Worker thread initialization, `SO_REUSEPORT` binding, Monoio `io_uring` event loop, active expiration cycle task, cross-shard receiver mesh. | [01_reactor_runtime](docs/design/01_reactor_runtime.md) |
| **Connection & Protocol** | `src/connection.rs` | Per-connection async loop, pipeline squashing, reusable channel pool, socket write batching. | [02_connection_lifecycle](docs/design/02_connection_lifecycle.md) |
| **Shard Store** | `src/shard.rs`, `src/table.rs` | Thread-local `ShardDb`, key-value store, expiration timestamps, active/passive TTL eviction. | [05_storage_engine](docs/design/05_storage_engine.md) |
| **Router** | `src/router.rs` | Key-to-shard routing (`target_shard`): `FxHash % num_shards` in standalone mode, `CRC16 % 16384` slot routing in cluster mode. | [04_sharding_mesh](docs/design/04_sharding_mesh.md) |
| **Cross-Shard Mailbox** | `src/mailbox.rs` | Lock-free per-shard-pair SPSC rings, `flume`-based wake signal, pooled reply descriptors (`FastGetDescriptor`, `BatchResponder`, etc.), mutex-guarded overflow queue. | [04_sharding_mesh](docs/design/04_sharding_mesh.md) |
| **RESP Engine** | `src/resp.rs` | Zero-copy parser for RESP arrays and inline Redis commands. | [03_resp_engine](docs/design/03_resp_engine.md) |
| **Entrypoint** | `src/main.rs` | CLI arguments, core affinity mapping, cross-shard channel instantiation. | [01_reactor_runtime](docs/design/01_reactor_runtime.md) |
| **Tests** | `tests/` | Multi-threaded end-to-end tests (`test_server_e2e.rs`) and channel waker verification (`test_cross_thread.rs`). | — |

All 19 components are documented in [`docs/design/components.md`](docs/design/components.md) (high-level design and rationale) and [`docs/internal/components.md`](docs/internal/components.md) (concrete implementation and code references),
written as learning guides against the *actual* shipped code — not just what it does, but why
it's built that way, with known limitations and gaps called out explicitly rather than left implicit.
