# Rudis Component Architecture Documentation

This directory contains deep-dive architectural specifications, internal data structures, and code logic walkthroughs for every subsystem in **Rudis**.

These documents are designed for contributors and system engineers who need to understand how Rudis works under the hood without needing to inspect every source file.

> ⚠️ **Unverified — at least 01-05 do not match the current source.** Spot-checking against
> `src/` found concrete mismatches: `04_sharding_and_router_mesh.md` describes a
> `RouterMesh`/`RouterHandle` type built on `oneshot` channels and `xxh3` hashing, but the real
> `src/router.rs` is a `Router` struct using `flume` channels and CRC16 (`key_slot`/
> `target_shard`); `05_storage_engine_and_encodings.md` describes `RudisTable` as a plain
> `hashbrown::HashMap<Bytes, RudisEntry, FxBuildHasher>` with Listpack/Intset/Skiplist
> promotion, but the real `src/table.rs` still uses the custom SIMD `RudisFlatTable`/
> `RudisTable` engine with no Listpack encoding at all; `01_reactor_and_server.md` claims
> signal-safe shutdown (`SIGINT`/`SIGTERM` trapping) that doesn't exist anywhere in
> `src/server.rs`. 06-15 have not been individually re-checked and should not be assumed
> accurate either. Treat every claim in this directory as needing verification against the
> actual file it cites before relying on it.
>
> For **01 (reactor/server)**, **02 (connection)**, **03 (RESP)**, **04 (router)**, and
> **05 (storage engine)**, [`docs/designs/components.md`](../designs/components.md) covers the
> same five subsystems and was written directly against the real code — prefer it until these
> five files are corrected.

---

## Component Index

| # | Component | Primary Source Files | Focus Areas |
| :---: | :--- | :--- | :--- |
| **01** | [**Reactor Runtime & Server Lifecycle**](01_reactor_and_server.md) | `src/main.rs`, `src/server.rs` | Shared-nothing architecture, `monoio` `io_uring` proactor, core pinning, `SO_REUSEPORT` kernel ingress, signal handling. |
| **02** | [**Connection Lifecycle & Command Execution**](02_connection_and_execution.md) | `src/connection.rs` | TCP session state, 64KB slab buffers, pipelined request squashing, local vs remote dispatch, exact `format_score` (`%.17g`). |
| **03** | [**RESP Protocol Engine & Serialization**](03_resp_protocol_engine.md) | `src/resp.rs` | Zero-copy RESP2 & RESP3 framing, inline command gateway, AST definitions for 274+ commands, strict Redis error compatibility. |
| **04** | [**Sharding Architecture & Cross-Core Mesh**](04_sharding_and_router_mesh.md) | `src/router.rs`, `src/shard.rs` | Hash tag `{...}` extraction, CRC16 & XXH3 partitioning, lock-free `flume` channel mesh, `MGET` scatter-gather squashing. |
| **05** | [**Storage Engine & Compact Encodings**](05_storage_engine_and_encodings.md) | `src/table.rs` | `RudisTable`, inlined TTL timestamps, Listpack to FlatHash promotion, Intset binary search, Augmented Skiplist with rank spans. |
| **06** | [**Blocking Operations & The Reactive Event Hub**](06_blocking_hub_and_waiters.md) | `src/block.rs` | `BlockHub`, non-blocking waiter registration, cross-shard wakeups (`BLPOP`, `BZPOPMIN`, `BZMPOP`), duplicate waiter suppression. |
| **07** | [**NVMe SSD Tiered Storage Engine**](07_nvme_tiered_storage.md) | `src/tiering.rs` | Three-state lifecycle (Hot, Cooled, Cold), 4KB `SmallBins` direct I/O packing (`O_DIRECT`), kernel hole punching (`fallocate`). |
| **08** | [**Vector Search Engine: HNSW, SQ8, PQ & ADC**](08_vector_search_hnsw_quantization.md) | `src/vector.rs` | Multi-layer HNSW graph, 8-bit Scalar Quantization (SQ8), Product Quantization (PQ), Asymmetric Distance Computation (ADC). |
| **09** | [**RediSearch Full-Text Engine & RRF**](09_redisearch_fulltext_and_rrf.md) | `src/search.rs` | Inverted indexes, posting lists, Okapi BM25 relevance scoring, Reciprocal Rank Fusion (RRF) for hybrid search. |
| **10** | [**Kernel Bypass & Zero-Copy Networking**](10_kernel_bypass_and_zerocopy.md) | `src/xdp.rs`, `src/zerocopy.rs` | AF_XDP eBPF NIC bypass driver, UMEM packet ring buffers, token bucket DDoS rate limiter, Linux `MSG_ZEROCOPY` TCP transmission. |
| **11** | [**Redis Cluster Topology & Gossip Protocol**](11_cluster_bus_and_gossip.md) | `src/cluster.rs` | 16,384 virtual hash slots, binary gossip frames (Port + 10000), epoch consensus, automated failover, `MOVED`/`ASK` redirection. |
| **12** | [**Active-Active Multi-Region CRDT Engine**](12_crdt_multi_region_replication.md) | `src/crdt.rs` | Multi-region active-active replication, Hybrid Logical Clocks (HLC), LWW-Register, PN-Counter, Add-Wins Observed-Remove Set (OR-Set). |
| **13** | [**Lua Scripting & Redis 7 Functions Engine**](13_lua_scripting_and_functions.md) | `src/scripting.rs` | Sandboxed in-process Lua 5.4 runtime (`mlua`), SHA1 bytecode caching, `redis.call` emulation, `EVAL`, `EVALSHA`, `FCALL`. |
| **14** | [**Persistence & Replication Engines**](14_persistence_and_replication.md) | `src/replication.rs`, `src/aof.rs` | Forkless in-process AOF rewrites, RDB snapshots, `PSYNC` replication stream, in-memory circular replication backlog. |
| **15** | [**Security, Memory Allocator & TLS**](15_security_allocator_and_tls.md) | `src/acl.rs`, `src/allocator.rs`, `src/tls.rs` | Redis ACL v2 user permissions, Jemalloc memory stats & profiling, in-memory self-signed certificates, Linux Kernel TLS (kTLS). |

---

## High-Level Architecture Map

```
                                Client Connections
                                        │
                         (SO_REUSEPORT Kernel Balancing)
                         ┌──────────────┴──────────────┐
                         ▼                             ▼
                 ┌───────────────┐             ┌───────────────┐
                 │    Core 0     │             │    Core 1     │
                 │ Monoio Runtime│             │ Monoio Runtime│
                 │  (01_reactor) │             │  (01_reactor) │
                 ├───────────────┤             ├───────────────┤
                 │  Connection   │             │  Connection   │
                 │ (02_connect)  │             │ (02_connect)  │
                 ├───────────────┤             ├───────────────┤
                 │  RESP Parser  │             │  RESP Parser  │
                 │   (03_resp)   │             │   (03_resp)   │
                 ├───────────────┤             ├───────────────┤
                 │   Storage     │             │   Storage     │
                 │  (05_table)   │             │  (05_table)   │
                 ├───────────────┤             ├───────────────┤
                 │  SSD Tiering  │             │  SSD Tiering  │
                 │  (07_tiering) │             │  (07_tiering) │
                 └───────┬───────┘             └───────┬───────┘
                         │        Cross-Shard Mesh     │
                         └────────◄ (04_router) ►──────┘
```
