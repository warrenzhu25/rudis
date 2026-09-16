# Rudis Component Architecture Documentation

This directory contains deep-dive architectural specifications, internal data structures, and code logic walkthroughs for every subsystem in **Rudis**.

These documents are designed for contributors and system engineers who need to understand how Rudis works under the hood without needing to inspect every source file.

> ℹ️ **All 15 files rewritten and verified against the current source** (each claim, struct, and
> code excerpt checked against the real `src/` file it documents — see each file's own notes on
> what was fixed and, in several cases, real bugs/dead-code findings turned up along the way).
> The original versions of every one of these files were fabricated: invented structs and
> functions with no basis in the actual source, describing a plausible-sounding but fictional
> implementation. Nothing here should be assumed accurate again without re-verifying against
> `src/` if the underlying code changes further — these docs describe a snapshot, not a contract.
>
> [`docs/designs/components.md`](../designs/components.md) also covers the same five subsystems
> as 01-05 (independently written, also verified against real code) — the two should agree;
> if they ever diverge, re-check both against the source rather than trusting either by default.
>
> Every file also ends with a **Future Improvements** section — concrete, prioritized (High/
> Medium/Low) suggestions grounded in the real gaps, bugs, and dead code found while verifying
> that file, not speculative wishlist items. The handful of cross-cutting items that show up in
> more than one file (the `MGET`/`MSET` fan-out gap, the two competing slot-authority mechanisms,
> the squashed-pipeline path skipping slot-migration redirection) are cross-referenced between
> their respective files rather than duplicated in full.

---

## Component Index

| # | Component | Primary Source Files | Focus Areas |
| :---: | :--- | :--- | :--- |
| **01** | [**Reactor Runtime & Server Lifecycle**](01_reactor_and_server.md) | `src/main.rs`, `src/server.rs` | Real per-shard startup sequence (RDB restore, AOF replay, tiering manager, cluster bus on shard 0 only), ~35-variant `ShardMessage`, `BlockHub` as the one lock exception. No signal handling exists. |
| **02** | [**Connection Lifecycle & Command Execution**](02_connection_and_execution.md) | `src/connection.rs` | Real `ClientInfo`/`ClientTracker`, cross-shard `MULTI`/`EXEC` transaction locking, inline live-slot-migration redirects, NVMe-tiered `GET` fallback, RESP3 tracking, real ACL enforcement (`-NOPERM`). `MGET`/`MSET` now genuinely fan out in parallel across shards — but only the first key of a multi-key command is checked against ACL/slot-migration state, and shard indices ≥64 silently drop results. |
| **03** | [**RESP Protocol Engine & Serialization**](03_resp_protocol_engine.md) | `src/resp.rs` | Real three-grammar dispatch (RESP array / Memcached storage-command trial parse / inline), real `Command` enum shape. No dedicated serialization helpers — replies are hand-formatted in `connection.rs`. |
| **04** | [**Sharding Architecture & Cross-Core Mesh**](04_sharding_and_router_mesh.md) | `src/router.rs`, `src/shard.rs` | Real `Router`/`ShardDb` fields, ~40-variant `ShardMessage`, `CompactResp` small-buffer optimization. `MGET`/`MSET` now fan out via pooled channels and a `try_recv` harvest sweep — but route via `slot_owners` while single-key ops still use static `target_shard()`, a new inconsistency risk during live migration. `Router::check_slot_redirection` and the >64-shard bitmask gap are still open. |
| **05** | [**Storage Engine & Compact Encodings**](05_storage_engine_and_encodings.md) | `src/table.rs` | Real SIMD `RudisFlatTable`/`RudisTable` engine (unchanged core mechanism), real 11-variant `RudisValue` (`Int`/`SmallHash`/`Tiered`/`Cooled` included). No Listpack/Intset. `ZRANK` is still O(n) even in the "Full" ZSet representation. |
| **06** | [**Blocking Operations & The Reactive Event Hub**](06_blocking_hub_and_waiters.md) | `src/block.rs` | Real `BlockHub` behind a process-wide `Mutex` (the one deliberate lock exception), `flume` channels, active fd-polling for disconnect detection, `MULTI`/`EXEC`-aware deferred wakeups (distinct from the no-op `CLIENT PAUSE`). |
| **07** | [**NVMe SSD Tiered Storage Engine**](07_nvme_tiered_storage.md) | `src/tiering.rs` | Real `ShardTierManager`/`OpManager`/`SmallBinsManager` 4KB page packing, CRC64-checked records, `fallocate` hole-punching, `FICLONE` reflink snapshots. `O_DIRECT` is opt-in (`RUDIS_DIRECT_IO`) with silent fallback. Real lifecycle is Hot → Cooled → Tiered, with no direct path back to Hot. |
| **08** | [**Vector Search Engine: HNSW, SQ8, PQ & ADC**](08_vector_search_hnsw_quantization.md) | `src/vector.rs` | Real HNSW/SQ8/PQ+ADC with AVX2+FMA SIMD (no NEON path). PQ codebooks are a fixed deterministic basis, not trained on data. Vector indexes are per-shard-local with no cross-shard fan-out. |
| **09** | [**RediSearch Full-Text Engine & RRF**](09_redisearch_fulltext_and_rrf.md) | `src/search.rs` | Real BM25 (k1=1.2, b=0.75) and RRF scoring, a real query DSL. Index registry is process-wide shared state, not thread-local. `KNN`/hybrid search now genuinely works end-to-end — `PARAMS`-supplied query vectors are wired into execution, and auto-indexed documents' vector fields are now actually parsed and stored. |
| **10** | [**Kernel Bypass & Zero-Copy Networking**](10_kernel_bypass_and_zerocopy.md) | `src/xdp.rs`, `src/zerocopy.rs` | No real AF_XDP/eBPF — a userspace-simulated packet pipeline reachable only via `XDP.*` commands, never real NIC ingress. Real `SO_ZEROCOPY`/`MSG_ZEROCOPY` code exists but has zero callers anywhere — dead code. |
| **11** | [**Redis Cluster Topology & Gossip Protocol**](11_cluster_bus_and_gossip.md) | `src/cluster.rs` | Plain-text line gossip protocol (not binary), full-state resend every 500ms, unilateral (non-quorum) failure detection, a real majority-vote replica election. Slot redirection is genuinely wired into the command path via `connection.rs` + `ClusterHub`, now including the pipelined squashed-command path. |
| **12** | [**Active-Active Multi-Region CRDT Engine**](12_crdt_multi_region_replication.md) | `src/crdt.rs` | Real LWW-Register/OR-Set/PN-Counter CRDTs with a CAS-based Hybrid Logical Clock. Single-key commands now correctly route through the normal key-slot mechanism, and `CRDT.DUMP`/`MERGE`/`GC` now fan out to every shard — but sync between separate Rudis *instances* is still entirely manual, no automatic network transport. |
| **13** | [**Lua Scripting & Redis 7 Functions Engine**](13_lua_scripting_and_functions.md) | `src/scripting.rs` | A fresh `mlua::Lua` VM per call (no persistent interpreter or bytecode cache), SHA1-cached script/library *source text*, real `redis.call`/`redis.pcall` via the normal command-execution path. `FCALL`'s AOF-bypass bug is now fixed. |
| **14** | [**Persistence & Replication Engines**](14_persistence_and_replication.md) | `src/replication.rs`, `src/aof.rs` | Still no AOF rewrite/compaction — the file grows forever. Partial `PSYNC` resync (`+CONTINUE`) is now real on the master side, but Rudis's own replica always sends `PSYNC ? -1`, so a Rudis-to-Rudis pair never actually exercises it. A real custom RDB binary format with a CRC64 trailer. |
| **15** | [**Security, Memory Allocator & TLS**](15_security_allocator_and_tls.md) | `src/acl.rs`, `src/allocator.rs`, `src/tls.rs` | ACL now hashes passwords (SHA1 with a hardcoded global salt — weak, and the plaintext is *still also* stored) and genuinely enforces per-command/per-key permissions. TLS is now wired to a `--tls-port` listener, but has a **critical live bug**: its kTLS fast-path marks itself active without ever installing kernel key material, so it silently sends all "encrypted" traffic in cleartext. |

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
