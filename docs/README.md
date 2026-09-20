# Rudis Architecture & Engineering Documentation

Welcome to the architecture specifications and engineering documentation for **Rudis**, a
shared-nothing, thread-per-core in-memory and NVMe-tiered datastore built in Rust on Linux
`io_uring`. Every document listed here has been checked against the current source tree; where
a described feature is partial, experimental, or not yet wired onto the live request path, the
document says so explicitly rather than leaving it implicit.

Start with the top-level [**README**](../README.md) for a project overview, or
[**agent.md**](../agent.md) for the mandatory engineering invariants contributors and coding
agents must uphold when touching this codebase.

---

## Documentation Index

### Core Architecture & Execution Model
* **[Architecture & Threading Model](architecture.md)**: shared-nothing thread-per-core design on Linux `io_uring` (Monoio runtime), `SO_REUSEPORT` kernel ingress balancing, the lock-free cross-shard mesh, and the life of a command request.
* **[Replication](replication.md)**: the two independent master-side protocols — standard single-connection `PSYNC` (used for all Rudis-to-Rudis replication) and Dragonfly-compatible per-shard `DFLY FLOW` (master-side only, for Dragonfly-protocol clients).
* **[Pub/Sub Architecture](pub-sub.md)**: the striped shard-presence bitmask that avoids broadcasting every `PUBLISH` to every shard, and Redis 7 slot-bound sharded pub/sub (`SPUBLISH`/`SSUBSCRIBE`).
* **[RDB Snapshotting](rdbsave.md)**: what actually triggers a save, why there is no `fork()`, why the save path is blocking (not `io_uring`-accelerated), and why `ioctl(FICLONE)` reflink cloning is a *different* (NVMe-tiering) feature, not part of RDB save.
* **[Differences from Redis & Dragonfly](differences.md)**: architectural and behavioral comparison covering concurrency model, memory, limits, clustering, and persistence.
* **[Rudis Internals Guide](rudis_internals_guide.md)**: a narrative, example-driven tour of the same subsystems for contributors who want a single self-contained read before diving into the per-subsystem docs below.

### Subsystem Specifications & Implementation References
* **[Subsystem Design Specifications](design/README.md)**: high-level architectural design and rationale ("why") across all 19 subsystems ([consolidated guide](design/components.md)).
* **[Subsystem Implementation & Code References](internal/README.md)**: concrete data structures, algorithms, and source line references in `src/` ([consolidated guide](internal/components.md)).

### Benchmarks
* **[Comprehensive Performance Guide](benchmarks/comprehensive_performance_guide.md)**: AMD EPYC benchmark results, throughput, latency distributions, and profiling methodology — includes an explicit data-provenance note on which figures are reproducible from committed data versus illustrative of methodology only.
* **[Multicore Benchmark Report](benchmark_multicore_results.md)**: script-generated head-to-head comparison against Dragonfly at 16 and 32 physical cores, including the documented crossover where Dragonfly overtakes Rudis at higher core counts.
* **[Benchmark Directory & Reproduction Scripts](benchmarks/README.md)**: index of all benchmark documents and the scripts used to reproduce them.

### Other References
* **[Code Review Notes](code_review.md)**: point-in-time review findings against the codebase.
