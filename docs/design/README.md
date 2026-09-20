# Rudis Subsystem Architecture & Design Specifications

This directory contains the **High-Level Design Documents** for all 19 subsystems in Rudis.
Each document focuses on **architectural purpose, design rationale ("why"), concurrency invariants, and performance characteristics**.

For low-level implementation details, data structures, and line-by-line code references, see [`docs/internal/`](../internal/).

---

## Subsystem Design Directory

| # | Subsystem | Scope & Purpose | Source Files | Modular Design Doc |
| :-: | :--- | :--- | :--- | :--- |
| 01 | **Reactor Runtime & Server Lifecycle** | Thread-per-core shared-nothing event loop: pinning, `ShardDb` isolation, cross-shard exceptions | `src/main.rs, src/server.rs` | [01_reactor_runtime.md](01_reactor_runtime.md) |
| 02 | **Connection Lifecycle & Command Execution** | Per-connection read loop, transactions, ACL/redirection gates, pipeline squashing across shards | `src/connection.rs` | [02_connection_lifecycle.md](02_connection_lifecycle.md) |
| 03 | **RESP Protocol Engine & Command Parser** | Zero-copy RESP/inline/Memcached parsing into a typed `Command` enum; no reply encoding | `src/resp.rs` | [03_resp_engine.md](03_resp_engine.md) |
| 04 | **Sharding Architecture & Cross-Core Mesh** | Deterministic key routing and a lock-free mesh, with pooled shared-memory descriptors for cross-shard replies | `src/router.rs, src/shard.rs, src/mailbox.rs` | [04_sharding_mesh.md](04_sharding_mesh.md) |
| 05 | **Storage Engine & Compact Encodings** | Custom SIMD open-addressed hash table inlining value, TTL, and small/full encodings in one slot | `src/table.rs` | [05_storage_engine.md](05_storage_engine.md) |
| 06 | **Blocking Operations & The Reactive Event Hub** | Cross-shard blocking-command wakeup registry (`BLPOP`/`BZPOPMIN`/`XREAD BLOCK`) via one shared mutex | `src/block.rs` | [06_blocking_hub.md](06_blocking_hub.md) |
| 07 | **NVMe SSD Tiered Storage Engine** | Transparent DRAM-to-NVMe cold-data offload via a Hot/Cooled/Tiered lifecycle with SmallBins packing | `src/tiering.rs` | [07_nvme_tiering.md](07_nvme_tiering.md) |
| 08 | **Vector Search Engine: HNSW, SQ8 & PQ** | HNSW graph vector index with SIMD distance kernels and SQ8/Product-Quantization compression | `src/vector.rs` | [08_vector_engine.md](08_vector_engine.md) |
| 09 | **RediSearch Full-Text Engine & Reciprocal Rank Fusion** | Native multi-field BM25/TAG/NUMERIC/vector search, per-shard partitioned, with RRF hybrid fusion | `src/search.rs` | [09_redisearch.md](09_redisearch.md) |
| 10 | **Kernel Bypass & Zero-Copy Networking** | Simulated AF_XDP rings and unused `MSG_ZEROCOPY` primitives; not on the live network path | `src/xdp.rs, src/zerocopy.rs` | [10_kernel_bypass_xdp.md](10_kernel_bypass_xdp.md) |
| 11 | **Redis Cluster Topology & Gossip Protocol** | 16,384-slot gossip cluster with quorum-based failover and genuine DUMP-and-replay slot migration | `src/cluster.rs` | [11_cluster_topology.md](11_cluster_topology.md) |
| 12 | **CRDT Data Types & Manual Multi-Region Sync** | LWW-Register/OR-Set/PN-Counter CRDTs with an HLC, for manual, operator-driven multi-region sync | `src/crdt.rs` | [12_crdt_types.md](12_crdt_types.md) |
| 13 | **Lua Scripting & Redis 7 Functions Engine** | Embedded, unsandboxed Lua 5.4 (`mlua`) engine for `EVAL` and Redis 7 Functions | `src/scripting.rs` | [13_scripting_functions.md](13_scripting_functions.md) |
| 14 | **Persistence & Replication Engines** | Decoupled AOF persistence and PSYNC/DFLY-FLOW replication sharing one command-serialization path | `src/replication.rs, src/aof.rs` | [14_persistence_replication.md](14_persistence_replication.md) |
| 15 | **Security, Memory Allocator & TLS** | Per-port ACLs, a jemalloc global allocator with small-object pooling, and a `rustls` TLS listener | `src/acl.rs, src/allocator.rs, src/tls.rs` | [15_security_tls.md](15_security_tls.md) |
| 16 | **JSON Document Store & JSONPath Engine** | Native RFC 8259 JSON store with a partial (non-recursive-descent) JSONPath engine | `src/json.rs` | [16_json_store.md](16_json_store.md) |
| 17 | **Geospatial Commands** | 52-bit geohash indexing over ZSet scores with geohash-interval-pruned Haversine distance queries | `src/geo.rs` | [17_geospatial.md](17_geospatial.md) |
| 18 | **Probabilistic Data Structures** | Bloom, Cuckoo, Count-Min Sketch, and Top-K structures for bounded-memory approximate queries | `src/probabilistic.rs` | [18_probabilistic.md](18_probabilistic.md) |
| 19 | **Pub/Sub Messaging Hub** | Per-shard Pub/Sub hubs with presence-bitmask-pruned cross-shard fan-out, plus sharded Pub/Sub | `src/pubsub.rs` | [19_pubsub.md](19_pubsub.md) |

---

### Deep-Dive Architectural Specifications
* [**`components.md`**](components.md): The unified consolidated design specification covering all 19 subsystems —
  purpose, key design choice, invariants, and verified findings for each, with links to the full per-subsystem docs above.
* [**`rudis_table.md`**](rudis_table.md): The original bucket-layout deep-dive for `RudisTable` (SwissTable/DASH-inspired,
  64-byte-aligned SIMD buckets, segmented directory), corrected against the actual implementation — every claim is
  labeled **Implemented** or **Future Work / Unrealized**. Companion to [Component 05](05_storage_engine.md).
* [**`tiered_storage.md`**](tiered_storage.md): The original Dragonfly-inspired NVMe tiering deep-dive (packed-pointer
  representation, intrusive LRU cooling queue, mimalloc-style segmented allocator), corrected against the simpler
  `TieredPointer`/`RudisValue::Cooled`/append-only design that actually shipped. Companion to
  [Component 07](07_nvme_tiering.md).
