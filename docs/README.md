# Rudis Architecture & Engineering Documentation

Welcome to the internal architecture specifications and engineering documentation for **Rudis**, the ultra-high-performance, shared-nothing in-memory and NVMe-tiered datastore built in Rust natively on Linux `io_uring`.

---

## Documentation Index

### Core Architecture & Execution Model
* **[Architecture & Threading Model](architecture.md)**: Shared-nothing thread-per-core architecture on Linux `io_uring` (Monoio runtime), `SO_REUSEPORT` kernel ingress balancing, lock-free cross-shard mesh, and the complete life of a command request.
* **[Per-Shard Parallel Replication](replication.md)**: Dragonfly-compatible multi-flow parallel TCP replication (`DFLY FLOW`), handshake protocol state machines, LSN sequence tracking, and zero-lock worker streaming.
* **[Pub/Sub Messaging & Redis 7 Sharded Pub/Sub](pub-sub.md)**: Striped shard presence bitmask, Redis 7 slot-bound sharded pub/sub (`SPUBLISH`, `SSUBSCRIBE`), zero-copy `Bytes` delivery pipeline, and bounded subscriber backpressure.
* **[Fork-less io_uring Snapshots & Reflinks](rdbsave.md)**: Fork-less point-in-time RDB snapshots, sequential shard chunk serialization, and sub-millisecond `ioctl(FICLONE)` reflink checkpoints with zero memory spike.
* **[Differences with Redis & Dragonfly](differences.md)**: Architectural and behavioral comparison between Rudis, Dragonfly, and Redis, covering memory models, limits, clustering, and persistence guarantees.

### Subsystem Specifications & Performance Guides
* **[Subsystem Design Specifications](design/README.md)**: High-level architectural design and rationale ("why") across all 19 subsystems ([consolidated guide](design/components.md)).
* **[Subsystem Implementation & Code References](internal/README.md)**: Concrete data structures, step-by-step algorithms, and source line references in `src/` ([consolidated guide](internal/components.md)).
* **[Comprehensive Performance Guide](benchmarks/comprehensive_performance_guide.md)**: 16-core AMD EPYC benchmark results, throughput, latency distributions, and profiling methodology.
* **[Benchmark Directory & Reproduction Scripts](benchmarks/README.md)**: Automated reproduction scripts and memtier benchmark workloads.
