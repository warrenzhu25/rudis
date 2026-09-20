# Rudis Subsystem Implementation & Code References

This directory contains the **Internal Implementation Documents** for all 19 subsystems in Rudis.
Each document focuses on **concrete Rust data structures, memory layouts, step-by-step algorithms, channel IPC protocols, and file/line references in `src/`**.

For high-level architectural design and rationale, see [`docs/design/`](../design/).

---

## Subsystem Implementation Directory

| # | Subsystem | Concrete Implementation Topics | Source Files | Modular Internal Doc |
| :-: | :--- | :--- | :--- | :--- |
| 01 | **Reactor Runtime & Server Lifecycle** | `main.rs`/`run_shard_worker` startup sequence, accept loops, cross-shard receiver, panic isolation | `src/main.rs, src/server.rs` | [01_reactor_runtime.md](01_reactor_runtime.md) |
| 02 | **Connection Lifecycle & Command Execution** | `handle_connection`/`execute_commands_squashed` internals, `BatchResponder` mailbox, output-buffer limits | `src/connection.rs` | [02_connection_lifecycle.md](02_connection_lifecycle.md) |
| 03 | **RESP Protocol Engine & Command Parser** | `parse_resp_array` two-pass scan, small-array fast path, 283-arm `build_command` | `src/resp.rs` | [03_resp_engine.md](03_resp_engine.md) |
| 04 | **Sharding Architecture & Cross-Core Mesh** | `Router`/`ShardMessage`/`mailbox.rs` internals, dual routing paths, shared-memory scatter-gather descriptors | `src/router.rs, src/shard.rs, src/mailbox.rs` | [04_sharding_mesh.md](04_sharding_mesh.md) |
| 05 | **Storage Engine & Compact Encodings** | `RudisFlatTable`/`RudisEntry` layouts, small/full encodings, monolithic resize, tiering hooks, arena recycling | `src/table.rs` | [05_storage_engine.md](05_storage_engine.md) |
| 06 | **Blocking Operations & The Reactive Event Hub** | `BlockHub` waiter queues, notify-under-lock pop, disconnect polling, `MULTI`/`EXEC` deferred notification | `src/block.rs` | [06_blocking_hub.md](06_blocking_hub.md) |
| 07 | **NVMe SSD Tiered Storage Engine** | `ShardTierManager` stash/read/GC, SmallBins record format, `OpManager` coalescing, `fallocate` hole punching | `src/tiering.rs` | [07_nvme_tiering.md](07_nvme_tiering.md) |
| 08 | **Vector Search Engine: HNSW, SQ8 & PQ** | `HnswIndex`/`HnswNode` structs, AVX-512/AVX2/portable distance tiers, insertion & search algorithms | `src/vector.rs` | [08_vector_engine.md](08_vector_engine.md) |
| 09 | **RediSearch Full-Text Engine & Reciprocal Rank Fusion** | `InvertedIndex` with dense `u32 DocId`, per-shard partitioning, BM25/RangeTree/RRF algorithms | `src/search.rs` | [09_redisearch.md](09_redisearch.md) |
| 10 | **Kernel Bypass & Zero-Copy Networking** | `XskSocket`/`XskUmem`/`XskRing` structs, CIDR rule engine, token-bucket limiter, unused `send_zc` | `src/xdp.rs, src/zerocopy.rs` | [10_kernel_bypass_xdp.md](10_kernel_bypass_xdp.md) |
| 11 | **Redis Cluster Topology & Gossip Protocol** | `ClusterHub` gossip/election state, DUMP-and-replay slot migration, blocking cluster bus | `src/cluster.rs` | [11_cluster_topology.md](11_cluster_topology.md) |
| 12 | **CRDT Data Types & Manual Multi-Region Sync** | HLC compare-exchange loop, per-type merge algorithms, `CRDT.DUMP`/`MERGE` wire format, on-demand GC | `src/crdt.rs` | [12_crdt_types.md](12_crdt_types.md) |
| 13 | **Lua Scripting & Redis 7 Functions Engine** | Local-shard-only `EVAL`/`FCALL` dispatch, `redis.call` bridge, RESP↔Lua conversion, two-pass `FUNCTION LOAD` | `src/scripting.rs` | [13_scripting_functions.md](13_scripting_functions.md) |
| 14 | **Persistence & Replication Engines** | `AofWriter` flush/fsync, ring-buffer backlog, `BGREWRITEAOF` compaction, PSYNC partial resync, DFLY FLOW | `src/replication.rs, src/aof.rs` | [14_persistence_replication.md](14_persistence_replication.md) |
| 15 | **Security, Memory Allocator & TLS** | `AclUser`/`AclManager` enforcement, jemalloc stats via `tikv-jemalloc-ctl`, `rustls` handshake, kTLS bug | `src/acl.rs, src/allocator.rs, src/tls.rs` | [15_security_tls.md](15_security_tls.md) |
| 16 | **JSON Document Store & JSONPath Engine** | Whole-document `serde_json::Value` storage, hand-duplicated mutable/immutable path traversal, auto-vivification | `src/json.rs` | [16_json_store.md](16_json_store.md) |
| 17 | **Geospatial Commands** | Bit-interleaved geohash encode/decode, `ZADD` delegation, geohash-interval-pruned radius/box search | `src/geo.rs` | [17_geospatial.md](17_geospatial.md) |
| 18 | **Probabilistic Data Structures** | FNV-1a double-hashing, cuckoo eviction kicks, Space-Saving Top-K, RDB-persisted structures | `src/probabilistic.rs` | [18_probabilistic.md](18_probabilistic.md) |
| 19 | **Pub/Sub Messaging Hub** | `PubSubHub` registry, `ShardedPresenceTable` bitmask, hand-written glob matcher, CRC16 sharded-channel routing | `src/pubsub.rs` | [19_pubsub.md](19_pubsub.md) |

---

* **Consolidated Document**: Read the entire unified implementation specification in [**`components.md`**](components.md) —
  concrete data structures, key algorithms, and verified findings for all 19 subsystems, each linking back to its
  full per-subsystem document above for exhaustive code-level depth.
