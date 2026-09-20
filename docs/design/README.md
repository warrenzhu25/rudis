# Rudis Subsystem Architecture & Design Specifications

This directory contains the **High-Level Design Documents** for all 19 subsystems in Rudis.
Each document focuses on **architectural purpose, design rationale ("why"), concurrency invariants, and performance characteristics**.

For low-level implementation details, data structures, and line-by-line code references, see [`docs/internal/`](../internal/).

---

## Subsystem Design Directory

| # | Subsystem | Scope & Purpose | Source Files | Modular Design Doc |
| :-: | :--- | :--- | :--- | :--- |
| 01 | **Reactor Runtime & Server Lifecycle** | Architectural rationale, invariants, and performance | `src/main.rs, src/server.rs` | [01_reactor_runtime.md](01_reactor_runtime.md) |
| 02 | **Connection Lifecycle & Command Execution** | Architectural rationale, invariants, and performance | `src/connection.rs` | [02_connection_lifecycle.md](02_connection_lifecycle.md) |
| 03 | **RESP Protocol Engine & Command Parser** | Architectural rationale, invariants, and performance | `src/resp.rs` | [03_resp_engine.md](03_resp_engine.md) |
| 04 | **Sharding Architecture & Cross-Core Mesh** | Architectural rationale, invariants, and performance | `src/router.rs, src/shard.rs` | [04_sharding_mesh.md](04_sharding_mesh.md) |
| 05 | **Storage Engine & Compact Encodings** | Architectural rationale, invariants, and performance | `src/table.rs` | [05_storage_engine.md](05_storage_engine.md) |
| 06 | **Blocking Operations & The Reactive Event Hub** | Architectural rationale, invariants, and performance | `src/block.rs` | [06_blocking_hub.md](06_blocking_hub.md) |
| 07 | **NVMe SSD Tiered Storage Engine** | Architectural rationale, invariants, and performance | `src/tiering.rs` | [07_nvme_tiering.md](07_nvme_tiering.md) |
| 08 | **Vector Search Engine: HNSW, SQ8 & PQ** | Architectural rationale, invariants, and performance | `src/vector.rs` | [08_vector_engine.md](08_vector_engine.md) |
| 09 | **RediSearch Full-Text Engine & Reciprocal Rank Fusion** | Architectural rationale, invariants, and performance | `src/search.rs` | [09_redisearch.md](09_redisearch.md) |
| 10 | **Kernel Bypass & Zero-Copy Networking** | Architectural rationale, invariants, and performance | `src/xdp.rs, src/zerocopy.rs` | [10_kernel_bypass_xdp.md](10_kernel_bypass_xdp.md) |
| 11 | **Redis Cluster Topology & Gossip Protocol** | Architectural rationale, invariants, and performance | `src/cluster.rs` | [11_cluster_topology.md](11_cluster_topology.md) |
| 12 | **CRDT Data Types & Manual Multi-Region Sync** | Architectural rationale, invariants, and performance | `src/crdt.rs` | [12_crdt_types.md](12_crdt_types.md) |
| 13 | **Lua Scripting & Redis 7 Functions Engine** | Architectural rationale, invariants, and performance | `src/scripting.rs` | [13_scripting_functions.md](13_scripting_functions.md) |
| 14 | **Persistence & Replication Engines** | Architectural rationale, invariants, and performance | `src/replication.rs, src/aof.rs` | [14_persistence_replication.md](14_persistence_replication.md) |
| 15 | **Security, Memory Allocator & TLS** | Architectural rationale, invariants, and performance | `src/acl.rs, src/allocator.rs, src/tls.rs` | [15_security_tls.md](15_security_tls.md) |
| 16 | **JSON Document Store & JSONPath Engine** | Architectural rationale, invariants, and performance | `src/json.rs` | [16_json_store.md](16_json_store.md) |
| 17 | **Geospatial Commands** | Architectural rationale, invariants, and performance | `src/geo.rs` | [17_geospatial.md](17_geospatial.md) |
| 18 | **Probabilistic Data Structures** | Architectural rationale, invariants, and performance | `src/probabilistic.rs` | [18_probabilistic.md](18_probabilistic.md) |
| 19 | **Pub/Sub Messaging Hub** | Architectural rationale, invariants, and performance | `src/pubsub.rs` | [19_pubsub.md](19_pubsub.md) |

---

* **Consolidated Document**: Read the entire unified design specification in [**`components.md`**](components.md).
