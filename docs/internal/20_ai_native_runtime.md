# Component 20: AI-Native Agent Runtime, Semantic Cache & MCP Server (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/agent.rs`, `src/mcp.rs`, `src/semcache.rs`  
> **High-Level Design Spec**: [`docs/design/20_ai_native_runtime.md`](../design/20_ai_native_runtime.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/agent.rs` | Working + episodic memory, LLM RPM/TPM quota governor, DAG checkpoints, idempotent tool leases | `AgentMemoryBank`, `LlmQuotaTracker`, `AgentCheckpointStore`, `AgentToolRegistry` |
| `src/mcp.rs` | Model Context Protocol (MCP) tool registry, RESP execution, and JSON-RPC 2.0 dispatcher | `mcp_tool_definitions`, `execute_mcp_tool`, `handle_mcp_rpc` |
| `src/semcache.rs` | Vector-similarity semantic prompt cache with TTL and cosine distance threshold | `SemanticCache`, `SemCacheEntry`, `SemCacheStats` |

---

## 2. Core Data Structures

### 2.1 Hierarchical Agent Memory (`src/agent.rs`)

```rust
pub struct MemoryTurn {
    pub id: u64,
    pub role: Bytes,
    pub content: Bytes,
    pub tokens: usize,
    pub timestamp_ms: u64,
    pub vector: Option<Vec<f32>>,
    pub metadata: Option<Bytes>,
}

pub struct AgentMemoryBank {
    pub session_id: String,
    pub max_working_tokens: usize,
    pub working_tokens: usize,
    pub next_turn_id: u64,
    pub working: VecDeque<MemoryTurn>,
    pub episodic_turns: HashMap<u64, MemoryTurn>,
    pub episodic_index: Option<HnswIndex>,
    pub total_spilled: u64,
    pub total_compactions: u64,
}
```

- **`append_turn`**: appends a `MemoryTurn` to `working` and increments `working_tokens`. While `working_tokens > max_working_tokens && working.len() > 1`, `evict_oldest_to_episodic` pops the front turn and inserts it into `episodic_turns` (and `episodic_index` if `turn.vector` is `Some`).
- **`window(budget_tokens, query_vec, recall_k)`**: scans `working` in reverse chronological order (`working.iter().rev()`) accumulating turns up to `budget_tokens`, then restores chronological order. If `query_vec` and `recall_k > 0` are supplied, searches `episodic_index` via `HnswIndex::search` and returns `(working_slice, recalled_episodic_turns)`.
- **`compact(keep_last, summary_role, summary_content, summary_tokens, summary_vec)`**: spills older working turns to episodic memory, retaining only the most recent `keep_last` turns, and prepends a synthesized summary turn at the front of `working`.

### 2.2 Dual RPM/TPM LLM Quota Governor (`src/agent.rs`)

```rust
pub struct LlmQuotaTracker {
    pub scope: String,
    pub max_rpm: u64,
    pub max_tpm: u64,
    pub window_ms: u64,
    pub next_res_id: u64,
    pub events: VecDeque<QuotaEvent>,
    pub reservations: HashMap<u64, TokenReservation>,
    pub used_rpm: u64,
    pub used_tpm: u64,
    pub reserved_tpm: u64,
    pub total_granted: u64,
    pub total_rejected: u64,
}
```

- **`purge_expired(now_ms)`**: evicts both `QuotaEvent` entries older than `now_ms - window_ms` and stale `TokenReservation` leases older than `window_ms` (default 60,000 ms).
- **`reserve(estimated_tokens, now_ms)`**: verifies `used_rpm + 1 <= max_rpm` and `used_tpm + reserved_tpm + estimated_tokens <= max_tpm`. When allowed, logs a request event (`tokens: 0`) and creates a `TokenReservation` holding `estimated_tokens` in `reserved_tpm`.
- **`settle(reservation_id, actual_tokens, now_ms)`**: removes `reservation_id` from `reservations`, subtracts `res.reserved_tokens` from `reserved_tpm`, and records a settled token event of `actual_tokens` in `used_tpm`.

### 2.3 DAG State Checkpointing & Idempotent Tool Leases (`src/agent.rs`)

```rust
pub struct AgentCheckpointStore {
    pub thread_id: String,
    pub latest_checkpoint_id: Option<Bytes>,
    pub nodes: HashMap<Bytes, AgentCheckpointNode>,
}

pub enum ToolLeaseState {
    InFlight { worker_id: Bytes, expires_at_ms: u64 },
    Completed { result: Bytes, completed_at_ms: u64, expires_at_ms: u64 },
}
```

- **`AgentCheckpointStore::history`**: walks parent links (`parent_id`) starting from `from_id` (or `latest_checkpoint_id`) up to `max_depth`, guarded by a `HashSet<Bytes>` cycle detector.
- **`AgentToolRegistry::claim`**: returns `ToolClaimOutcome::Acquired` if `call_id` is absent or its lease has expired (`now_ms >= expires_at_ms`), `ToolClaimOutcome::InFlight` if another worker holds an active lease, or `ToolClaimOutcome::Completed(result)` if the tool call already finished within its retention TTL.

### 2.4 Built-in MCP Server Engine (`src/mcp.rs`)

- **`mcp_tool_definitions()`**: returns 8 strongly-typed `McpToolDef` schemas (`rudis_kv_get`, `rudis_kv_set`, `rudis_semcache_lookup`, `rudis_semcache_store`, `rudis_vector_search`, `rudis_agent_memory_append`, `rudis_agent_memory_window`, `rudis_checkpoint_put`).
- **`handle_mcp_rpc(&mut Shard, payload)`**: parses a JSON-RPC 2.0 request envelope (`{"jsonrpc":"2.0","id":...,"method":...,"params":...}`) via `serde_json`, supporting:
  - `"initialize"`: returns `protocolVersion: "2024-11-05"` and server capabilities.
  - `"tools/list"`: returns all 8 tool definitions with JSON Schema `inputSchema` objects.
  - `"tools/call"`: extracts `params.name` and `params.arguments`, dispatches to `execute_mcp_tool`, and wraps the result in `{"content":[{"type":"text","text":...}],"isError":false}`.

---

## 3. Cross-Component Interactions

- **`src/resp.rs`**: parses `AGENT.MEM.*`, `LLM.QUOTA.*`, `AGENT.CHECKPOINT.*`, `AGENT.TOOL.*`, `MCP.*`, and `SEMCACHE.*` into dedicated `Command` variants.
- **`src/connection.rs`**: routes keyed `AGENT.*` and `LLM.QUOTA.*` commands to their owner shard via `target_shard_of_cmd` (`router.shard_for_key`), while `MCP.*` commands execute against the local shard's `Shard` state.
- **`src/vector.rs`**: `AgentMemoryBank` embeds `HnswIndex` for episodic recall, and `SemanticCache` uses `compute_distance(..., VectorMetric::Cosine)` for prompt similarity matching.

---

## 4. How to Verify Changes

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo test --lib agent::tests
cargo test --lib mcp::tests
cargo test --test test_server_e2e test_agent_memory_working_and_episodic_e2e -- --test-threads=1
cargo test --test test_server_e2e test_llm_quota_governor_reserve_and_settle_e2e -- --test-threads=1
cargo test --test test_server_e2e test_agent_checkpoint_dag_and_tool_idempotency_e2e -- --test-threads=1
cargo test --test test_server_e2e test_mcp_protocol_tools_call_and_rpc_e2e -- --test-threads=1
```
