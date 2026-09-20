# Rudis Subsystem Implementation & Code References

This directory contains the **Internal Implementation Documents** for all 19 subsystems in Rudis.
Each document focuses on **concrete Rust data structures, memory layouts, step-by-step algorithms, channel IPC protocols, and file/line references in `src/`**.

For high-level architectural design and rationale, see [`docs/design/`](../design/).

---

## Subsystem Implementation Directory

| # | Subsystem | Concrete Implementation Topics | Source Files | Modular Internal Doc |
| :-: | :--- | :--- | :--- | :--- |
| 01 | **Reactor Runtime & Server Lifecycle** | Struct layouts, algorithms, and code references | `src/main.rs, src/server.rs` | [01_reactor_runtime.md](01_reactor_runtime.md) |
| 02 | **Connection Lifecycle & Command Execution** | Struct layouts, algorithms, and code references | `src/connection.rs` | [02_connection_lifecycle.md](02_connection_lifecycle.md) |
| 03 | **RESP Protocol Engine & Command Parser** | Struct layouts, algorithms, and code references | `src/resp.rs` | [03_resp_engine.md](03_resp_engine.md) |
| 04 | **Sharding Architecture & Cross-Core Mesh** | Struct layouts, algorithms, and code references | `src/router.rs, src/shard.rs` | [04_sharding_mesh.md](04_sharding_mesh.md) |
| 05 | **Storage Engine & Compact Encodings** | Struct layouts, algorithms, and code references | `src/table.rs` | [05_storage_engine.md](05_storage_engine.md) |
| 06 | **Blocking Operations & The Reactive Event Hub** | Struct layouts, algorithms, and code references | `src/block.rs` | [06_blocking_hub.md](06_blocking_hub.md) |
| 07 | **NVMe SSD Tiered Storage Engine** | Struct layouts, algorithms, and code references | `src/tiering.rs` | [07_nvme_tiering.md](07_nvme_tiering.md) |
| 08 | **Vector Search Engine: HNSW, SQ8 & PQ** | Struct layouts, algorithms, and code references | `src/vector.rs` | [08_vector_engine.md](08_vector_engine.md) |
| 09 | **RediSearch Full-Text Engine & Reciprocal Rank Fusion** | Struct layouts, algorithms, and code references | `src/search.rs` | [09_redisearch.md](09_redisearch.md) |
| 10 | **Kernel Bypass & Zero-Copy Networking** | Struct layouts, algorithms, and code references | `src/xdp.rs, src/zerocopy.rs` | [10_kernel_bypass_xdp.md](10_kernel_bypass_xdp.md) |
| 11 | **Redis Cluster Topology & Gossip Protocol** | Struct layouts, algorithms, and code references | `src/cluster.rs` | [11_cluster_topology.md](11_cluster_topology.md) |
| 12 | **CRDT Data Types & Manual Multi-Region Sync** | Struct layouts, algorithms, and code references | `src/crdt.rs` | [12_crdt_types.md](12_crdt_types.md) |
| 13 | **Lua Scripting & Redis 7 Functions Engine** | Struct layouts, algorithms, and code references | `src/scripting.rs` | [13_scripting_functions.md](13_scripting_functions.md) |
| 14 | **Persistence & Replication Engines** | Struct layouts, algorithms, and code references | `src/replication.rs, src/aof.rs` | [14_persistence_replication.md](14_persistence_replication.md) |
| 15 | **Security, Memory Allocator & TLS** | Struct layouts, algorithms, and code references | `src/acl.rs, src/allocator.rs, src/tls.rs` | [15_security_tls.md](15_security_tls.md) |
| 16 | **JSON Document Store & JSONPath Engine** | Struct layouts, algorithms, and code references | `src/json.rs` | [16_json_store.md](16_json_store.md) |
| 17 | **Geospatial Commands** | Struct layouts, algorithms, and code references | `src/geo.rs` | [17_geospatial.md](17_geospatial.md) |
| 18 | **Probabilistic Data Structures** | Struct layouts, algorithms, and code references | `src/probabilistic.rs` | [18_probabilistic.md](18_probabilistic.md) |
| 19 | **Pub/Sub Messaging Hub** | Struct layouts, algorithms, and code references | `src/pubsub.rs` | [19_pubsub.md](19_pubsub.md) |

---

* **Consolidated Document**: Read the entire unified implementation specification in [**`components.md`**](components.md).
