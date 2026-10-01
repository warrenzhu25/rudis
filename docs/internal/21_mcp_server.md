# Component 21: Built-in Model Context Protocol (MCP) Server Engine — Implementation Reference

> **Source Files**: `src/mcp.rs` (748 lines)
> **High-Level Design Spec**: [`docs/design/21_mcp_server.md`](../design/21_mcp_server.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)
> **Related**: [Component 20 — Agent Memory, LLM Quota & Checkpoints](20_agent_memory.md)
> (`src/agent.rs`) and [Component 08 — Vector Search Engine](08_vector_engine.md)
> (`SemanticCache` lives in `src/vector.rs` — see Gotcha 7)

---

## 1. Module Inventory

`src/mcp.rs` in full — every symbol it defines (one plain struct, no enums/traits beyond what it
imports, a flat set of free functions):

| Kind | Item | Line | Notes |
| :--- | :--- | :-: | :--- |
| struct | `McpToolDef { name, description, input_schema }` | 13–18 | `#[derive(Debug, Clone)]`; `name`/`description` are `&'static str`, `input_schema` is `serde_json::Value` |
| fn | `builtin_mcp_tools() -> Vec<McpToolDef>` | 20–179 | hand-written literal list of 11 tools, §2 |
| fn | `tools_list_json() -> Value` | 181–193 | maps each `McpToolDef` to `{name, description, inputSchema}` and wraps in `{"tools": [...]}` |
| fn | `parse_f32_vec(val: &Value, field_name: &str) -> Result<Vec<f32>, String>` | 195–210 | private; JSON array of numbers → `Vec<f32>`, §4 |
| fn | `get_req_str(args: &Value, field: &str) -> Result<String, String>` | 212–217 | private; required-string-field extractor, §4 |
| fn | `plan_tool_command(tool: &str, args: &Value) -> Result<Command, String>` | 220–445 | tool name + JSON args → native `Command`, §3.4 |
| fn | `parse_resp_frame(buf: &[u8], pos: usize) -> Option<(Value, usize, bool)>` | 448–527 | private; recursive RESP2/RESP3 → JSON frame parser, §3.5 |
| fn | `resp_bytes_to_json(buf: &[u8]) -> (Value, bool)` | 529–537 | public entry point over `parse_resp_frame`; returns `(value, is_error)` |
| fn | `format_mcp_call_result(val: Value, is_error: bool) -> Value` | 539–554 | builds the MCP `{"content":..., "structuredContent":..., "isError":...}` envelope |

`#[cfg(test)] mod tests` occupies lines 556–748 (two tests — see §6).

**Imports** (lines 6–11): `bytes::Bytes`, `serde_json::{Value, json}`, `std::time::Duration`,
`crate::resp::{Command, SetCondition, VsimTarget}`, `crate::search::SearchOptions`. There is no
import of `crate::agent`, `crate::vector`, or anything from `src/table.rs` — `mcp.rs` constructs
`Command` values only; it never touches `AgentMemoryBank`, `SemanticCache`, `HnswIndex`, or
`ShardDb` state directly (verified: grepping the file for `agent::`, `vector::`, `table::` finds
no matches outside doc comments).

---

## 2. `McpToolDef` and the 11-Tool Catalog

```rust
#[derive(Debug, Clone)]
pub struct McpToolDef {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,       // a JSON Schema object literal built with serde_json::json!
}
```

`builtin_mcp_tools()` (lines 20–179) returns exactly **11** `McpToolDef` entries, each with a
`{"type": "object", "properties": {...}, "required": [...]}` JSON Schema. In source order:

| # | Tool name | Lines | Required args | Optional args (w/ default) |
| :-: | :--- | :-: | :--- | :--- |
| 1 | `rudis_kv_get` | 22–32 | `key` | — |
| 2 | `rudis_kv_set` | 33–45 | `key`, `value` | `px` (ms expiry) |
| 3 | `rudis_semantic_set` | 46–62 | `namespace`, `id`, `prompt`, `response`, `vector` | `px`, `scope` |
| 4 | `rudis_semantic_get` | 63–77 | `namespace`, `vector` | `threshold` (0.15), `k` (1), `scope` |
| 5 | `rudis_vector_add` | 78–91 | `key`, `element`, `vector` | `attr` (JSON attribute string) |
| 6 | `rudis_vector_search` | 92–107 | `key`, `vector` | `count` (5), `filter`, `with_scores` (true), `with_attribs` (false) |
| 7 | `rudis_agent_memory_add` | 108–123 | `session`, `role`, `content` | `tokens`, `vector`, `meta` |
| 8 | `rudis_agent_memory_context` | 124–137 | `session` | `max_tokens` (2048), `query_vector`, `recall_k` (3) |
| 9 | `rudis_agent_checkpoint_put` | 138–152 | `key`, `step_id`, `state` | `parent_id`, `meta` |
| 10 | `rudis_agent_checkpoint_get` | 153–164 | `key` | `step_id` |
| 11 | `rudis_ft_search` | 165–177 | `index`, `query` | `limit` (10) |

`tools_list_json()` (lines 181–193) is a pure derived view over `builtin_mcp_tools()` — it maps
`input_schema` to the MCP wire field name `inputSchema` and wraps the list in `{"tools": [...]}`.
Both `MCP.TOOLS` (RESP) and `MCP.RPC {"method":"tools/list"}` (JSON-RPC) are built from this one
function (`connection.rs:5744`, `5828`), so the two surfaces cannot report different tool sets.

---

## 3. Wire Behavior

### 3.1 `MCP.TOOLS` — RESP array of tool records

Parsed in `src/resp.rs:12949`: `"MCP.TOOLS" => Ok(Some(Command::McpTools))` (no arguments
accepted beyond the command name itself — any extra args are simply never read since the arm
takes none).

Handled in `src/connection.rs:5743–5757`: writes a RESP array of `tools.len()` elements, each a
flat 6-element RESP array `[b"name", <name>, b"description", <description>, b"inputSchema",
<schema-as-JSON-string>]`. This is **not** a RESP map (`%`) even under RESP3 — `inputSchema` is
serialized to a JSON string via `serde_json::to_string` and sent as one RESP bulk string, not a
nested RESP structure.

### 3.2 `MCP.CALL <tool> [args_json]` — RESP, JSON in/out

Parsed in `src/resp.rs:12950–12960`: requires 2 or 3 total args (`args.len() < 2 || args.len() >
3` is rejected with `"wrong number of arguments for 'mcp.call' command"`); the JSON arguments
blob is optional and defaults to `Bytes::from_static(b"{}")` (an empty object) when omitted.

Handled in `src/connection.rs:5758–5793`:
1. `serde_json::from_slice(&args_json)` — on parse failure, writes a real RESP error
   (`write_resp_err`, `"invalid JSON arguments: {e}"`) and returns immediately. This is the
   **only** way `MCP.CALL` produces a RESP-level `-ERR` reply.
2. `crate::mcp::plan_tool_command(&tool, &args)` — on `Err(e)`, wraps `e` via
   `format_mcp_call_result(Value::String(e), true)` and writes it as a **RESP bulk string**
   (`write_resp_bulk`), i.e. this is reported as a normal (non-error) RESP reply whose JSON body
   says `"isError": true` — not a RESP `-ERR`.
3. On `Ok(planned)`, a fresh `Vec<u8>` (`sub_out`) is created and `execute_command(planned, ...)`
   is invoked recursively (`Box::pin(...)`, since `execute_command` is `async fn` and Rust
   requires boxing for direct recursion) with the *same* `router`, `client_id`, `client_registry`,
   `asking`, `authenticated`, `auth_user` the outer `MCP.CALL` call received.
4. `sub_out` (the planned command's raw RESP reply bytes) is converted via
   `resp_bytes_to_json(&sub_out)` → `(Value, is_err)`, then wrapped via
   `format_mcp_call_result(val, is_err)` and written as one RESP bulk string containing the
   serialized JSON result object.

### 3.3 `MCP.RPC <json_rpc_request>` — JSON-RPC 2.0 envelope

Parsed in `src/resp.rs:12962–12967`: requires exactly 2 total args (command name + one RESP bulk
string holding the full JSON-RPC request body) — `"wrong number of arguments for 'mcp.rpc'
command"` otherwise.

Handled in `src/connection.rs:5794–5881`. On a JSON parse failure of the request body, writes a
JSON-RPC **Parse error** response (code `-32700`) as a RESP bulk string (not a RESP `-ERR`) with
`"id": null`. Otherwise reads `id` (defaults to `Value::Null` if absent — see Gotcha 4) and
`method` (defaults to `""` if absent or non-string), then dispatches:

| `method` | Behavior |
| :--- | :--- |
| `"initialize"` | Returns a hardcoded result: `protocolVersion: "2024-11-05"`, `capabilities: {"tools":{"listChanged":false}}`, `serverInfo: {"name":"rudis-mcp","version":"0.1.0"}` — none of these three fields reflect the running Rudis build/version; they are literal constants in `connection.rs:5811–5819`. |
| `"ping"` | Returns `{"result": {}}` — a no-op liveness check. |
| `"tools/list"` | Returns `{"result": tools_list_json()}` — identical content to `MCP.TOOLS`, just JSON-RPC-wrapped. |
| `"tools/call"` | Reads `params.name` (defaults `""`) and `params.arguments` (defaults `{}`), calls `plan_tool_command`, and on success recursively re-enters `execute_command` exactly as `MCP.CALL` does (§3.2 step 3), wrapping the sub-reply the same way via `resp_bytes_to_json`/`format_mcp_call_result`. On a `plan_tool_command` error, the error string is wrapped the same way but placed directly in the JSON-RPC `"result"` field (**not** an `"error"` field) — a planning failure for `tools/call` is reported as a successful JSON-RPC response whose result says `isError: true`, matching MCP convention, same as `MCP.CALL`. |
| anything else | `{"error": {"code": -32601, "message": "Method not found: {method}"}}` — standard JSON-RPC 2.0 method-not-found. |

The final JSON-RPC response object is always serialized to one JSON string and written as a
single RESP bulk string (`write_resp_bulk`) — `MCP.RPC` never produces a RESP `-ERR` for anything
except the initial JSON-parse failure.

### 3.4 `plan_tool_command`: tool → `Command` translation table

Every arm of the `match tool { ... }` in `plan_tool_command` (lines 220–445), with the concrete
`Command` variant it builds and the subsystem that variant is ultimately handled by:

| Tool | `Command` produced | Notable field mapping | Backing subsystem (via the recursive `execute_command`/`execute_local_command` dispatch) |
| :--- | :--- | :--- | :--- |
| `rudis_kv_get` | `Command::Get(key)` | — | `src/table.rs` KV store |
| `rudis_kv_set` | `Command::Set{..}` | `condition: SetCondition::None`, `get: false`, `keepttl: false`, `past_expired: false` hardcoded; `px` → `expire_in: Duration::from_millis(ms.max(1))` | `src/table.rs` KV store |
| `rudis_semantic_set` | `Command::SemanticSet{..}` | `quantize: false` hardcoded (no schema field for it); `tokens` passed through if present | `vector::SemanticCache` (lives in `src/vector.rs`, Component 08) |
| `rudis_semantic_get` | `Command::SemanticGet{..}` | `threshold` default `0.15`; `with_score`/`with_prompt`/`with_id` all hardcoded `true` | `vector::SemanticCache` |
| `rudis_vector_add` | `Command::Vadd{..}` | `is_redis_vset: true` (Redis-8-Vector-Set data model, not a raw internal HNSW index); `metric`, `quantize`, `pq`, `tiered`, `reduce`, `quant`, `ef`, `m`, `cas` all hardcoded to `None`/`false` — no schema exposure for any of them | `vector.rs` `VectorSetValue`/`HnswIndex` (Component 08) |
| `rudis_vector_search` | `Command::Vsim{ target: VsimTarget::Vector(vector), .. }` | `count.max(1)` — a `count: 0` argument is silently bumped to 1, never rejected; `epsilon`, `ef`, `filter_ef` hardcoded `None`; `truth`/`no_thread` hardcoded `false` | `vector.rs` HNSW search (Component 08) |
| `rudis_agent_memory_add` | `Command::AgentMemAdd{..}` | `vector` optional — `Some(v) if !v.is_null()` else `None` | `agent::AgentMemoryBank` (Component 20) |
| `rudis_agent_memory_context` | `Command::AgentMemContext{..}` | `max_tokens` default `2048`; `query` optional same null-check pattern; `recall_k` default `3` | `agent::AgentMemoryBank` |
| `rudis_agent_checkpoint_put` | `Command::AgentCheckpointPut{..}` | `parent_id`/`meta` optional, `None` if absent | `agent::AgentCheckpointStore` |
| `rudis_agent_checkpoint_get` | `Command::AgentCheckpointGet{..}` | `step_id` optional (omitted → latest checkpoint) | `agent::AgentCheckpointStore` |
| `rudis_ft_search` | `Command::FtSearch{..}` | `options: SearchOptions{ limit, ..Default::default() }` — every other `SearchOptions` field (e.g. `nocontent`, `withscores`, filters) takes its `Default` value; the tool schema exposes only `index`, `query`, `limit` | `src/search.rs` inverted index (Component 09) |
| anything else | `Err(format!("Unknown MCP tool: {}", tool))` | line 443 | — |

### 3.5 RESP → JSON conversion: `parse_resp_frame`/`resp_bytes_to_json`

`parse_resp_frame(buf, pos)` (lines 448–527) is a **hand-rolled, recursive, position-based** RESP
frame parser (distinct from — and simpler than — `src/resp.rs`'s two-pass wire parser, Component
03, and also distinct from `src/scripting.rs`'s `BytesMut`-cursor parser, Component 13). It
switches on the byte at `buf[pos]`:

| Prefix | JSON result | Notes |
| :-- | :-- | :-- |
| `+...` (simple string) | `Value::String` | `is_error = false` |
| `-...` (error) | `Value::String` | `is_error = true` — the error text itself, not an `{"err":...}` wrapper |
| `:N` (integer) | `json!(n)` (`i64`) | falls back to `0` on an unparseable integer |
| `,F` (RESP3 double) | `json!(f)` (`f64`) | falls back to `0.0` on an unparseable float |
| `_` (RESP3 null) | `Value::Null` | |
| `$-1` (null bulk) | `Value::Null` | |
| `$N...` (bulk string) | `Value::String` (lossy UTF-8) | returns `None` (parse failure) if the buffer doesn't contain `N + 2` more bytes — the **whole** top-level parse then falls through to `resp_bytes_to_json`'s fallback (next row) |
| `*N...` (array) | `Value::Array`, recursively | an error element anywhere inside sets the array's `is_error` to `true` (`any_err` OR-accumulated) |
| `%N...` (RESP3 map) | `Value::Object` | keys are coerced to `String` via `Value::String(s) => s`, else `other.to_string()` (i.e. a non-string RESP3 map key is stringified as JSON, not as its raw text) |
| anything else | `None` | unrecognized prefix |

`resp_bytes_to_json(buf)` (lines 529–537) calls `parse_resp_frame(buf, 0)` and, on any `None`
(including the truncated-buffer case above, or an empty `buf`, or a prefix byte it doesn't
recognize), **falls back** to treating the entire input as a lossy-UTF-8 string with
`is_error = false` — this is a deliberate never-fail fallback: a sub-command reply that
`parse_resp_frame` can't make sense of is reported as plain text, not as a hard error.

### 3.6 `format_mcp_call_result`

```rust
pub fn format_mcp_call_result(val: Value, is_error: bool) -> Value {
    let text = match &val {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    json!({
        "content": [{ "type": "text", "text": text }],
        "structuredContent": val,
        "isError": is_error
    })
}
```

Every MCP tool result — success or failure — carries both a human-readable `content[0].text`
(the raw string if `val` was already a JSON string, else `val` re-serialized to a JSON string)
**and** a `structuredContent` field holding the original, still-structured `val`. `isError` is
the only field that distinguishes a failed tool call; there is no separate error-code field.

---

## 4. Argument Parsing/Validation Helpers

### `parse_f32_vec(val: &Value, field_name: &str) -> Result<Vec<f32>, String>` (lines 195–210)

- Requires `val` to be a JSON array (`as_array()`); otherwise
  `"'{field_name}' must be an array of numbers"`.
- Rejects an empty array outright: `"'{field_name}' cannot be empty"` (lines 199–201) — no MCP
  tool can submit a zero-length embedding vector.
- Each element must be `as_f64()`-coercible (covers JSON integers and floats, not strings);
  otherwise `"'{field_name}' elements must be numbers"`. Every element is narrowed to `f32` via
  `as f32` with no overflow/precision check.

### `get_req_str(args: &Value, field: &str) -> Result<String, String>` (lines 212–217)

- `args.get(field).and_then(|v| v.as_str())` — requires the field to be **present and a JSON
  string**; a present-but-wrong-type field (e.g. `{"key": 123}`) is indistinguishable from a
  missing field and produces the same message, `"missing required string argument '{field}'"`
  (Gotcha 2).

Both helpers return `Result<_, String>`, and `plan_tool_command` propagates failures with `?` —
the first missing/malformed required argument short-circuits the whole planning step before any
`Command` is constructed and before `execute_command` is ever invoked, so a bad argument never
reaches the storage layer.

---

## 5. Concrete Numbers, Limits, and What's Absent

- **11 tools total** (`builtin_mcp_tools()`, §2) — the unit test only asserts `tools.len() >= 10`
  (line 564), so the exact count is a looser invariant than the list itself; the current source
  has 11.
- **3 MCP `Command` variants**: `Command::McpTools`, `Command::McpCall{tool, args_json}`,
  `Command::McpRpc(Bytes)` (`src/resp.rs:1394–1399`).
- **4 JSON-RPC methods implemented**: `initialize`, `ping`, `tools/list`, `tools/call`
  (`connection.rs:5810–5876`). No `resources/list`, `resources/read`, `prompts/list`,
  `prompts/get`, `notifications/*`, `logging/setLevel`, or any other method from the MCP spec is
  implemented — all fall through to the generic `-32601 Method not found`.
- **No batch JSON-RPC support**: `MCP.RPC`'s handler always calls
  `serde_json::from_slice::<Value>` expecting one JSON *object* and always writes back exactly
  one response object. A JSON-RPC batch request (a top-level JSON **array** of request objects)
  is not special-cased: `req.get("id")`/`req.get("method")` on a `Value::Array` return `None`
  (string indices don't resolve against a JSON array), so a batch payload silently becomes one
  `{"error": {"code": -32601, "message": "Method not found: "}}` response instead of a batch
  response array or a proper `-32600 Invalid Request`.
- **No JSON-RPC notification support**: every request gets a response regardless of whether `id`
  was present in the input; there is no special-casing of a missing `id` to mean "notification,
  suppress the reply" per the JSON-RPC 2.0 spec — a missing `id` is simply treated as `null` and
  a response is still written.
- **No tool exists for roughly half of the `Command` surface the rest of this subsystem family
  exposes.** Verified by cross-referencing `plan_tool_command`'s 11 arms against the full set of
  `Command` variants documented in Component 20: there is no MCP tool for `Command::Del`/`Unlink`
  (no way to delete a KV key via MCP), `SemanticDel`/`SemanticFlush`/`SemanticInfo` (no way to
  evict or inspect the semantic cache), `AgentMemCompact`/`AgentMemInfo`/`AgentMemClear`, or
  `AgentCheckpointHistory` (no DAG-lineage traversal tool despite `AgentCheckpointStore::history`
  existing), `AgentToolClaim`/`AgentToolComplete` (no idempotent tool-lease primitive exposed to
  MCP agents at all), any `LlmQuotaReserve`/`Settle`/`Info` tool, or most of the Vector-Set
  surface (`Vdel`, `Vcard`, `Vdim`, `Vemb`, `Vlinks`, `Vrandmember`, `Vsetattr`, `Vgetattr`,
  `Vismember`, `Vquery`, `Vdist`) beyond the one `Vadd`/`Vsim` pair.
- **No upper bound on `rudis_vector_search`'s `count`** — `count.max(1)` only floors it; an agent
  can request an arbitrarily large `count` with no server-side cap inside `mcp.rs` (any cap would
  have to come from `Command::Vsim`'s own handler, not from the MCP translation layer).
- **Two tests, 193 lines (556–748)**: `test_mcp_tools_list_and_command_planning` (560–706) checks
  the tool count/names and exercises every `plan_tool_command` arm plus three error paths
  (unknown tool, missing required field, malformed vector); `test_resp_bytes_to_json_full_spectrum`
  (708–747) round-trips every RESP2/RESP3 prefix `parse_resp_frame` understands, plus
  `format_mcp_call_result`.

---

## 6. Cross-Component Interactions

- **`src/resp.rs`** (Component 03): owns all `MCP.TOOLS`/`MCP.CALL`/`MCP.RPC` wire parsing
  (`resp.rs:12949–12967`) and the three `Command` variant definitions (`resp.rs:1394–1399`).
- **`src/connection.rs`** (Component 02): the sole place `Command::McpTools`/`McpCall`/`McpRpc`
  are handled (`connection.rs:5743–5881`, inside `execute_command`). This handler recursively
  calls `execute_command` itself (`Box::pin`) for the inner planned command — the only subsystem
  doc in this series where a command handler re-enters the top-level dispatcher rather than
  calling `execute_local_command` directly (contrast Component 13's `redis.call`, which calls
  `execute_local_command`).
- **`src/acl.rs`** (Component 15): because the inner planned command re-enters `execute_command`,
  it passes back through the ACL gate at `connection.rs:5000–5024` a second time, checked against
  the *inner* command's own name (e.g. `"VADD"`) and keys — not just a blanket `"MCP"` category
  check for the outer `MCP.CALL`/`MCP.RPC` command.
- **`src/vector.rs`** (Component 08): backs `rudis_semantic_set`/`rudis_semantic_get`
  (`vector::SemanticCache`) and `rudis_vector_add`/`rudis_vector_search`
  (`VectorSetValue`/`HnswIndex`) — reached only via the recursive `Command::SemanticSet`/
  `SemanticGet`/`Vadd`/`Vsim` dispatch, never called directly from `mcp.rs`.
- **`src/agent.rs`** (Component 20): backs `rudis_agent_memory_add`/`_context`
  (`AgentMemoryBank`) and `rudis_agent_checkpoint_put`/`_get` (`AgentCheckpointStore`) the same
  way.
- **`src/search.rs`** (Component 09): backs `rudis_ft_search` (`Command::FtSearch`'s own
  dedicated arm at `connection.rs:11417`, which calls `router.ft_search` directly rather than
  going through the generic `target_shard_of_cmd` group — RediSearch indices are queried via the
  router's own cross-shard aggregation, not single-key routing).

---

## 7. Contributor Gotchas & Debugging Guide

- **Gotcha 1 (schema/planner drift risk)**: `builtin_mcp_tools()`'s JSON Schemas and
  `plan_tool_command`'s field reads are two independently hand-maintained descriptions of the
  same argument contract — nothing enforces they match. One already-verified instance: the
  `rudis_semantic_set` schema (lines 46–62) lists no `tokens` property, but `plan_tool_command`
  (line 260) reads an optional `tokens` field from `args` anyway — a caller following the
  published schema would never know `tokens` is accepted. Conversely `rudis_vector_add`'s
  schema exposes only `key`/`element`/`vector`/`attr`, while the underlying `Command::Vadd` has
  nine more fields (`metric`, `quantize`, `pq`, `tiered`, `reduce`, `quant`, `ef`, `m`, `cas`)
  that `plan_tool_command` hardcodes and a schema-reading caller has no way to influence.
- **Gotcha 2 (misleading "missing" error for wrong-typed fields)**: `get_req_str` reports
  `"missing required string argument '{field}'"` identically whether the field is absent or
  present with a non-string JSON type (e.g. a number or array) — a caller who passes `{"key":
  123}` sees "missing", not "wrong type" (§4).
- **Gotcha 3 (double ACL/stat/slowlog accounting)**: because `MCP.CALL`/`tools/call` re-enters
  `execute_command` for the *same logical operation*, both the outer `MCP.CALL`/`MCP.RPC`
  command and the inner translated command independently run `record_cmd_stat(cmd_name)`
  (`connection.rs:4920`) and the ACL check (`connection.rs:5000–5024`), and may each produce
  their own slowlog entry if slow enough — `COMMAND.STATS`/`SLOWLOG` output for one MCP tool call
  shows up as two distinct command executions (e.g. one `MCP` entry, one `GET`/`VADD`/etc.
  entry), not one.
- **Gotcha 4 (`id` defaulting masks malformed requests)**: `MCP.RPC` defaults a missing `id` to
  `Value::Null` and a missing/non-string `method` to `""` rather than rejecting the request — a
  request body that's valid JSON but missing `method` entirely silently becomes a
  `-32601 Method not found: ` response (empty method name in the message) instead of a
  `-32600 Invalid Request`.
- **Gotcha 5 (no batch/notification support)**: see §5 — a JSON-RPC batch array or a true
  notification (omitted `id` meaning "no reply expected") is not handled per spec.
- **Gotcha 6 (hardcoded `initialize` identity)**: `protocolVersion`, `capabilities`, and
  `serverInfo.version` in the `"initialize"` response are literal constants
  (`connection.rs:5811–5819`), not derived from Rudis's actual build version or an actual
  negotiated capability set — they will not change as the server's real capabilities evolve
  unless this code is edited by hand.
- **Gotcha 7 (a prior combined "Component 20" doc was stale and has been removed)**: an earlier
  `docs/internal/20_ai_native_runtime.md`/`docs/design/20_ai_native_runtime.md` pair described
  `mcp.rs` as exposing **8** tools named `rudis_kv_get`, `rudis_kv_set`, `rudis_semcache_lookup`,
  `rudis_semcache_store`, `rudis_vector_search`, `rudis_agent_memory_append`,
  `rudis_agent_memory_window`, `rudis_checkpoint_put`, and reference functions
  `mcp_tool_definitions()`/`execute_mcp_tool()`/`handle_mcp_rpc()`. **None of those four function
  names and none of the four renamed tool names (`rudis_semcache_lookup`, `rudis_semcache_store`,
  `rudis_agent_memory_append`, `rudis_agent_memory_window`) exist anywhere in `src/mcp.rs`**
  (verified by grep: zero matches for all four) — that doc described an earlier, superseded shape
  of this module. It has been deleted and replaced by this document (Component 21) plus
  [Component 20 — Agent Memory, LLM Quota & Checkpoints](20_agent_memory.md) for `src/agent.rs`.
  The current, verified surface is `builtin_mcp_tools()`/`plan_tool_command()`/`tools_list_json()`
  plus the `MCP.*` handling inlined in `src/connection.rs` (§3), exposing the 11 tools listed in
  §2 of this document.

### How to Verify Changes

```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run mcp.rs's own unit tests
cargo test --lib mcp::tests -- --test-threads=1

# 4. Run the MCP protocol E2E test (MCP.TOOLS / MCP.CALL / MCP.RPC over a real connection)
cargo test --test test_server_e2e test_mcp_protocol_tools_call_and_rpc_e2e -- --test-threads=1
```
