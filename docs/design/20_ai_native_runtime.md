# Component 20: AI-Native Agent Runtime, Semantic Cache & MCP Server (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/agent.rs`, `src/mcp.rs`, `src/semcache.rs`  
> **Implementation Reference**: [`docs/internal/20_ai_native_runtime.md`](../internal/20_ai_native_runtime.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Modern LLM and autonomous agent workloads require five tightly coupled state primitives that traditional key-value caches do not natively provide:
1. **Token-budgeted working + episodic memory**: agents must assemble a recent conversation window bounded by an exact token budget while simultaneously recalling semantically relevant past turns that have already been evicted from working memory.
2. **Dual RPM + TPM LLM rate-limiting with post-stream settlement**: unlike HTTP APIs where request cost is known upfront, LLM streaming completions only reveal actual output token consumption *after* the stream finishes. Naive token buckets either under-bill or fail to refund unused pre-reserved tokens.
3. **DAG-aware state checkpointing**: frameworks like LangGraph and AutoGen branch, retry, and time-travel across parent-linked execution checkpoints.
4. **Idempotent tool-call execution leases**: retrying an agent step after a timeout must not re-execute non-idempotent side-effecting tool calls (e.g., charging a credit card or creating a ticket) if another worker is already executing or has completed the call.
5. **Model Context Protocol (MCP) interoperability & Semantic LLM Caching**: AI agents speak JSON-RPC 2.0 MCP tools (`initialize`, `tools/list`, `tools/call`) and benefit from sub-millisecond vector-similarity caching of prior prompt/response pairs.

### 1.2 The Rudis Solution
Rudis embeds an **AI-Native Agent Runtime** directly into each thread-per-core `Shard`:
- **Hierarchical Agent Memory (`AGENT.MEM.*`)**: combines a token-budgeted FIFO working memory buffer (`VecDeque<MemoryTurn>`) with automatic HNSW episodic spill (`HnswIndex`) on eviction.
- **Dual RPM/TPM LLM Quota Governor (`LLM.QUOTA.*`)**: enforces sliding-window Requests-Per-Minute and Tokens-Per-Minute limits with pre-request reservation leases (`LLM.QUOTA.RESERVE`) and post-stream actual token settlement (`LLM.QUOTA.SETTLE`).
- **DAG Checkpointing & Tool Leases (`AGENT.CHECKPOINT.*`, `AGENT.TOOL.*`)**: provides parent-linked checkpoint lineage (`PUT`/`GET`/`HISTORY`) and TTL-guarded exactly-once tool execution leases (`CLAIM`/`COMPLETE`).
- **Semantic LLM Cache (`SEMCACHE.*`)**: caches LLM prompt embeddings and completions per namespace using cosine distance thresholds and TTL/LRU eviction.
- **Built-in MCP Server Engine (`MCP.*`)**: exposes Rudis KV, Semantic Cache, Vector Search, and Agent Memory/Checkpoints as native Model Context Protocol JSON-RPC 2.0 tools (`MCP.TOOLS`, `MCP.CALL`, `MCP.RPC`).

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 Key Invariants & Concurrency Constraints
1. **Shared-nothing owner-shard routing**:
   - Every `AGENT.MEM.*` (`session_id`), `LLM.QUOTA.*` (`scope`), `AGENT.CHECKPOINT.*` (`thread_id`), and `AGENT.TOOL.*` (`call_id`) command is routed deterministically to its owning shard via `target_shard_of_cmd` in `src/connection.rs` and `ShardMessage::ExecuteCommand` in `src/shard.rs`.
   - Within the owning shard, state lives inside `Shard::agent_memory`, `Shard::llm_quotas`, `Shard::agent_checkpoints`, and `Shard::agent_tools` (`AHashMap`), requiring zero mutexes or atomic operations.
2. **Automatic working-to-episodic HNSW spill**:
   - When `AGENT.MEM.APPEND` causes `working_tokens` to exceed `max_working_tokens` (and more than 1 turn is present), the oldest `MemoryTurn` is popped from `working` and indexed into `episodic_index: Option<HnswIndex>` if an embedding vector was attached.
3. **Reservation-then-settle token accounting**:
   - `LLM.QUOTA.RESERVE` atomically checks both `used_rpm + 1 <= max_rpm` and `used_tpm + reserved_tpm + estimated_tokens <= max_tpm` over a 60-second sliding window.
   - `LLM.QUOTA.SETTLE` removes the in-flight `TokenReservation` by `reservation_id` and records `actual_tokens` against `used_tpm`, immediately freeing any overestimated token budget for concurrent agent requests.

---

## 3. High-Level Architecture & Workflow Diagram

```
  Agent / LLM Orchestrator
         │
         ├──► AGENT.MEM.APPEND / WINDOW / SEARCH / COMPACT
         │       └─► Working FIFO (token-bounded) ──(evict)──► Episodic HNSW Index
         │
         ├──► LLM.QUOTA.RESERVE ──► [Stream LLM] ──► LLM.QUOTA.SETTLE
         │       └─► 60s Sliding Window (RPM + Used TPM + Reserved TPM)
         │
         ├──► AGENT.CHECKPOINT.PUT / GET / HISTORY
         │       └─► Parent-linked DAG lineage per thread_id
         │
         ├──► AGENT.TOOL.CLAIM ──► [Execute Tool] ──► AGENT.TOOL.COMPLETE
         │       └─► TTL-guarded lease: ACQUIRED / IN_FLIGHT / COMPLETED
         │
         └──► MCP.RPC (JSON-RPC 2.0: initialize, tools/list, tools/call)
                 └─► Dispatches to KV, SEMCACHE, VQUERY, AGENT.MEM, AGENT.CHECKPOINT
```

---

## 4. Supported Command Surface

| Command Family | Commands | Purpose |
| :--- | :--- | :--- |
| **Agent Memory** | `AGENT.MEM.APPEND`, `AGENT.MEM.WINDOW`, `AGENT.MEM.SEARCH`, `AGENT.MEM.COMPACT`, `AGENT.MEM.STATS` | Token-budgeted working memory window + HNSW episodic recall and summary compaction |
| **LLM Quota Governor** | `LLM.QUOTA.CONFIG`, `LLM.QUOTA.RESERVE`, `LLM.QUOTA.SETTLE`, `LLM.QUOTA.STATUS` | Sliding-window RPM & TPM rate limiting with pre-flight reservation and post-stream settlement |
| **DAG Checkpointing** | `AGENT.CHECKPOINT.PUT`, `AGENT.CHECKPOINT.GET`, `AGENT.CHECKPOINT.HISTORY` | Durable parent-linked agent execution state checkpoints and lineage traversal |
| **Idempotent Tool Leases** | `AGENT.TOOL.CLAIM`, `AGENT.TOOL.COMPLETE` | Distributed lease lock and cached result store for side-effecting agent tool invocations |
| **Model Context Protocol** | `MCP.TOOLS`, `MCP.CALL`, `MCP.RPC` | Native MCP tool discovery, invocation, and JSON-RPC 2.0 wire handler |
| **Semantic LLM Cache** | `SEMCACHE.SET`, `SEMCACHE.GET`, `SEMCACHE.DEL`, `SEMCACHE.CLEAR`, `SEMCACHE.INFO` | Cosine-similarity prompt/completion cache with per-namespace statistics |

---

## 5. Implementation References & Contributor Guide

* [**`docs/internal/20_ai_native_runtime.md`**](../internal/20_ai_native_runtime.md): Low-level data structures, algorithms, and code reference.
* **Source Files**: `src/agent.rs`, `src/mcp.rs`, `src/semcache.rs`
