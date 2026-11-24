# Experimental extensions

Rudis's core is a Redis-compatible server: everything in the [README](../README.md) outside
this page. The extensions below go beyond Redis. They are **off by default**: their command
families answer `ERR unknown command` until `enable-experimental-commands yes` is set (config
file or `--enable-experimental-commands yes`); `AGENT.*` and NVMe tiering are not behind that
switch. Their data is persisted to RDB and AOF and replicated like any other key, but they get
less testing than the core: no upstream test suite covers them.

## Status

| Subsystem | Status | Notes | Design Doc |
| :--- | :---: | :--- | :--- |
| **Dragonfly compatibility (state only)** | Partial | `DFLYCLUSTER`/`DFLYMIGRATE` remain bookkeeping-only, distinct from the more complete native cluster migration path above. | [11](design/11_cluster_topology.md) |
| **RedisJSON document store** | Implemented | JSONPath parsing, array slicing, in-place mutation. | [16](design/16_json_store.md) |
| **RediSearch (`FT.*`)** | Implemented | Inverted index, BM25, `RangeTree`, `FLAT` & `HNSW` vector fields, pre-filtered KNN, `VECTOR_RANGE`, `FT.HYBRID` (RRF/Linear), `FT.ALTER`, `FT.PROFILE`, multi-vector JSON chunks (`$.chunks[*].emb`). | [09](design/09_redisearch.md) |
| **RedisBloom-style probabilistic types** | Implemented | Bloom, Cuckoo, Count-Min Sketch, Top-K. | [18](design/18_probabilistic.md) |
| **Agent memory, LLM quota & checkpoints** | Implemented | Working + HNSW episodic memory (`AGENT.MEM.*`), DAG checkpoints (`AGENT.CHECKPOINT.*`), tool leases (`AGENT.TOOL.*`), and the RPM/TPM quota governor (`LLM.QUOTA.*`) all get RDB + AOF/replication coverage. | [20](design/20_agent_memory.md) |
| **MCP server & semantic cache** | Implemented | Built-in MCP JSON-RPC 2.0 server (`MCP.*`, 11 tools) translating into ordinary commands via the normal dispatch path; semantic cache (`SEMANTIC.*`) lives in `src/vector.rs`. | [21](design/21_mcp_server.md), [08](design/08_vector_engine.md) |
| **NVMe tiered storage** | Implemented | Hot/Cooled/Cold lifecycle, `SmallBins`, Direct I/O; tiered keys are written with their real value and type by `SAVE`/`BGSAVE` and `BGREWRITEAOF`. | [07](design/07_nvme_tiering.md), [tiered_storage.md](design/tiered_storage.md) |
| **Multi-region CRDTs** | Implemented, manual sync only | LWW-Register, OR-Set, PN-Counter with HLC ordering; export/merge (`CRDT.DUMP`/`CRDT.MERGE`) is explicit and manual — there is no automatic peer discovery or background cross-region streaming. | [12](design/12_crdt_types.md) |
| **AF_XDP kernel bypass** | Experimental, not on live path | Real, unit-tested data structures; reachable only via `XDP.*` admin commands, not real NIC/eBPF I/O. | [10](design/10_kernel_bypass_xdp.md) |
| **`SO_ZEROCOPY` send path** | Experimental, not on live path | Correct primitives in `src/zerocopy.rs`; unused by the real connection write path. | [10](design/10_kernel_bypass_xdp.md) |

## RediSearch: `RangeTree` Indexing, Hybrid Vector Search & `FT.AGGREGATE`

- Documents are mapped to dense integer document IDs to shrink posting-list memory relative to
  storing string keys directly.
- Numeric fields are indexed with a balanced `RangeTree` for `@field:[min max]` range queries, and
  `VECTOR` fields support both `FLAT` (exact) and `HNSW` (approximate) indexing over Hash and JSON
  documents (including wildcard multi-vector chunk arrays such as `$.chunks[*].emb`).
- Supports pre-filtered KNN (`@tag:{...}=>[KNN k @vec $blob]`), radius search (`@vec:[VECTOR_RANGE r $blob]`),
  dedicated hybrid fusion (`FT.HYBRID` with `RRF` and `LINEAR` combinations), schema evolution (`FT.ALTER`),
  and query execution profiling (`FT.PROFILE`).
- `FT.AGGREGATE` supports a multi-stage pipeline (`GROUPBY`, `REDUCE`, `APPLY`, `SORTBY`).
- Details and known limitations: [`docs/design/09_redisearch.md`](design/09_redisearch.md).

## NVMe Tiered Storage (SmallBins & Direct I/O)

- **Three-state lifecycle**: keys move Hot (DRAM) → Cooled → Cold (NVMe), promoted back to Hot
  on access.
- **`SmallBins` packing**: values under 4 KB are coalesced into aligned 4 KB blocks before a
  single write, avoiding write amplification from many tiny writes.
- **Direct I/O**: reads/writes bypass the page cache via `O_DIRECT`; space reclamation uses
  `fallocate(FALLOC_FL_PUNCH_HOLE)`.
- Details: [`docs/design/07_nvme_tiering.md`](design/07_nvme_tiering.md) and
  [`docs/design/tiered_storage.md`](design/tiered_storage.md). Tiered keys are read back
  and saved with their real value and type by `SAVE`/`BGSAVE` and `BGREWRITEAOF`.

## Kernel-Bypass Networking & TLS: Status

Two subsystems exist as unconditionally-compiled, unit-tested code but are **not on the live
network path today**:
- **`src/xdp.rs`** implements AF_XDP (UMEM rings, an eBPF-style packet classifier) entirely as
  in-process Rust data structures, reachable only via explicit `XDP.*` admin commands. No real
  AF_XDP socket, eBPF program, or NIC binding is created by any code path.
- **`src/zerocopy.rs`** implements correct `SO_ZEROCOPY`/registered-buffer primitives, but
  nothing outside its own unit tests calls them — the real connection write path
  (`src/connection.rs`, on `monoio`) does not use this module.
- **Kernel TLS (kTLS)**: not implemented. `rustls` performs the handshake and all
  encryption in userspace. Offload would need the session keys installed with
  `setsockopt(SOL_TLS, TLS_TX/TLS_RX)`, TLS 1.3 control records handled out of band on
  receive, and the `tls` kernel module; since Rudis does not use `sendfile`, it would mostly
  move the same AES-GCM work into the kernel. An earlier best-effort `TCP_ULP` attach that
  never installed keys (and so never activated) was removed rather than kept as scaffolding.

Treat these as real, tested building blocks for a future kernel-bypass data path, not as
currently active acceleration. See
[`docs/design/10_kernel_bypass_xdp.md`](design/10_kernel_bypass_xdp.md) and
[`docs/design/15_security_tls.md`](design/15_security_tls.md) for the full picture,
including *why* this direction is still worth pursuing despite not being wired up yet.

## Agent Memory, LLM Quota, Checkpoints & MCP Server

- **Hierarchical Agent Memory (`AGENT.MEM.*`, `src/agent.rs`)**: token-budgeted FIFO working
  memory window paired with automatic HNSW episodic spill and hybrid recall (`AGENT.MEM.WINDOW`,
  `AGENT.MEM.SEARCH`, `AGENT.MEM.COMPACT`). Gets RDB and AOF/replication coverage.
- **Dual RPM/TPM LLM Quota Governor (`LLM.QUOTA.*`, `src/agent.rs`)**: sliding-window
  Requests-Per-Minute and Tokens-Per-Minute rate limiting with pre-flight token reservation
  (`LLM.QUOTA.RESERVE`) and post-stream actual token settlement (`LLM.QUOTA.SETTLE`). Persisted in
  RDB snapshots (`EXT_TYPE_LLM_QUOTA`) and logged to AOF / replicated.
- **DAG State Checkpointing & Idempotent Tool Leases (`AGENT.CHECKPOINT.*`, `AGENT.TOOL.*`,
  `src/agent.rs`)**: parent-linked execution state lineage (`PUT`/`GET`/`HISTORY`) and
  TTL-guarded exactly-once tool call execution leases (`CLAIM`/`COMPLETE`). Checkpoint timestamps
  are computed server-side at execution time, so they can diverge between a primary and a
  replica/AOF-replay of the same logical checkpoint.
- **Built-in Model Context Protocol Server (`MCP.*`, `src/mcp.rs`)**: native JSON-RPC 2.0 MCP
  server (`MCP.TOOLS`, `MCP.CALL`, `MCP.RPC`) exposing 11 tools that translate into ordinary
  Rudis commands via the normal `execute_command` dispatch path (same ACL/routing as any client
  command).
- **Semantic Cache (`SEMANTIC.*`, implemented in `src/vector.rs`, not a separate file)**:
  cosine-similarity LLM prompt cache (`SEMANTIC.SET`/`GET`/`DEL`/`FLUSH`/`INFO`).

Details: [Component 20 — Agent Memory, LLM Quota & Checkpoints](design/20_agent_memory.md),
[Component 21 — MCP Server](design/21_mcp_server.md), and
[Component 08 — Vector Search Engine](design/08_vector_engine.md) for `SemanticCache`.
