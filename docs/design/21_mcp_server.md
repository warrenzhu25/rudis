# Component 21: Built-in Model Context Protocol (MCP) Server Engine — Design & Architecture

> **Subsystem Scope**: `src/mcp.rs`
> **Implementation Reference**: [`docs/internal/21_mcp_server.md`](../internal/21_mcp_server.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)
> **Related Subsystems**: [Component 20 — Agent Memory, LLM Quota & Checkpoints](20_agent_memory.md)
> (`src/agent.rs`, a separate consumer of MCP tool calls) and
> [Component 08 — Vector Search Engine](08_vector_engine.md) (`SemanticCache` lives in
> `src/vector.rs`, exposed here only via the `rudis_semantic_*` MCP tools).

---

## 1. Purpose

`src/mcp.rs` (748 lines) is a built-in **Model Context Protocol (MCP)** server engine embedded
directly in the Rudis process. It lets any MCP-speaking LLM agent or orchestration framework
(Claude, LangGraph, AutoGen, or a hand-rolled agent loop) discover and invoke a fixed menu of
Rudis operations — plain KV, the Semantic LLM Cache, Redis 8 Vector Sets, Agent Memory,
Agent DAG Checkpoints, and RediSearch full-text queries — as **named MCP tools** with JSON Schema
`inputSchema`s, over three new commands:

- `MCP.TOOLS` — RESP-native tool catalog listing.
- `MCP.CALL <tool> [args_json]` — RESP-native single tool invocation, JSON in, JSON out.
- `MCP.RPC <json_rpc_request>` — a full JSON-RPC 2.0 envelope (`initialize`, `ping`,
  `tools/list`, `tools/call`), for clients that already speak the standard MCP wire protocol
  rather than bespoke RESP commands.

The subsystem's entire job is **translation**: turning an MCP tool name plus a JSON arguments
object into one of Rudis's own native `Command` enum values, and turning that command's RESP
reply back into MCP's JSON content envelope. It deliberately does not duplicate any storage,
vector-search, or agent-memory logic itself.

## 2. Design Rationale ("Why")

### 2.1 Why an MCP server embedded in the data store, not a sidecar

MCP's standard deployment shape is a small stand-alone process ("MCP server") that an agent
framework spawns and speaks JSON-RPC to over stdio or HTTP, which in turn talks to whatever
backend it wraps. Rudis instead embeds the MCP surface directly in the server process and
exposes it as three more RESP commands. This removes an entire extra network hop and process
between "agent decides to call a tool" and "data is read/written" — the MCP tool call executes
on the same shard, in the same event loop tick family, as any other Rudis command, with no serial
round trip through a separate sidecar process. The tradeoff is that the tool menu is fixed at
compile time (§2.4) rather than configurable by an operator at runtime the way a general-purpose
MCP gateway would be.

### 2.2 Why translate to a real `Command`, not a parallel execution path

The central design decision in `src/mcp.rs` is that `plan_tool_command(tool, args)` produces a
genuine `crate::resp::Command` value — the exact same enum that `src/resp.rs`'s wire parser
builds from a RESP-framed client command — and the command handlers for `MCP.CALL`/`MCP.RPC`
then **recursively re-enter `execute_command`**, the same top-level async dispatcher every
ordinary client command runs through (see `docs/internal/21_mcp_server.md` §3 for the call
chain). An MCP tool call therefore gets:

- the same per-command ACL enforcement (`user.can_execute_command`/`can_access_key`) that a
  directly-issued `GET`/`SET`/`VADD`/`AGENT.MEM.ADD` would get, evaluated against the *inner*
  translated command's name and keys, not just a blanket "MCP" permission;
- the same cross-shard routing (`target_shard_of_cmd` plus `execute_remote`/
  `execute_local_command`) that keeps Rudis's shared-nothing, thread-per-core architecture
  intact — a tool call touching a key owned by a different shard is forwarded correctly, it does
  not silently execute against the wrong shard's keyspace;
- the same AOF/replication behavior as the equivalent command issued directly.

This is a deliberately different posture from the Lua scripting engine (Component 13), whose
`redis.call` bridge calls the lower-level, synchronous `execute_local_command` directly and
therefore bypasses both the top-level ACL gate and `EVAL`'s own ad-hoc key-routing check for
everything except the handful of commands special-cased there. MCP tool calls, by contrast, pay
the cost of one extra recursive `execute_command` frame (with its own ACL check, command-stat
increment, and slowlog/ACL bookkeeping run a second time — see the Internal doc's gotchas) in
exchange for inheriting the full normal command-execution contract for free, with no special
casing needed inside `mcp.rs` itself.

### 2.3 Why a fixed, curated tool menu instead of exposing every command

`builtin_mcp_tools()` hand-lists 11 tools, each with its own narrow, explicit JSON Schema and its
own hand-written argument-extraction arm in `plan_tool_command`. Rudis does not auto-generate an
MCP tool for every `Command` variant it supports. This is a deliberate safety/ergonomics
boundary: an LLM agent calling `MCP.CALL` can only ever reach the specific operations the server
author chose to expose (basic KV get/set, semantic cache store/lookup, vector-set add/search,
agent memory append/window, agent checkpoint put/get, and a read-only full-text search) — never
`FLUSHALL`, `CONFIG SET`, `CLUSTER`, `SHUTDOWN`, or any administrative surface, and not even every
read/write variant of the subsystems it *does* expose (no delete/flush/info tools for the
semantic cache, no checkpoint-history or tool-lease tools, no LLM quota tools — see the Internal
doc's coverage gaps). A hostile or confused agent prompt is bounded by this fixed menu.

### 2.4 Why both a RESP surface and a JSON-RPC 2.0 surface

`MCP.TOOLS`/`MCP.CALL` are plain RESP commands returning RESP arrays/bulk strings, useful to any
existing Rudis/Redis client library without an MCP-aware HTTP layer. `MCP.RPC` instead accepts
one RESP bulk-string argument containing a full JSON-RPC 2.0 request object
(`{"jsonrpc":"2.0","id":...,"method":...,"params":...}`) and returns a JSON-RPC 2.0 response —
this is the shape a conformant MCP client actually speaks (`initialize` handshake, `tools/list`,
`tools/call`). Both surfaces are kept because they serve different callers: `MCP.CALL` is
convenient for application code that already knows which tool it wants and doesn't want to
construct a JSON-RPC envelope, while `MCP.RPC` is what lets an off-the-shelf MCP client library
talk to Rudis with (almost) no custom glue, by treating a single Rudis connection as if it were
the MCP transport.

### 2.5 What this design buys, and what it costs

- **Buys**: zero-sidecar MCP interoperability; tool calls inherit real ACL and shard-routing
  semantics for free (§2.2); the tool catalog is a pure function of `src/mcp.rs` (`tools_list_json`)
  so `MCP.TOOLS` and `tools/list` can never drift from each other — both are built from the same
  `builtin_mcp_tools()` list.
- **Costs**: the tool menu is compile-time fixed — adding a new MCP tool (or a delete/info/flush
  variant of an existing one) requires a code change and rebuild, not a runtime registration call;
  there is no tool access control finer than Rudis's existing per-command/per-key ACL (no
  per-tool allow-list distinct from the underlying command's ACL category); and the JSON-RPC
  surface implements only the four methods the current `MCP.RPC` handler recognizes
  (`initialize`, `ping`, `tools/list`, `tools/call`) — there is no `resources/*`, `prompts/*`, or
  batch-request support (see the Internal doc's limitations section for the verified specifics).

## 3. Architecture Overview

```
MCP Client (LLM agent / orchestrator)
    │
    ├─ RESP: MCP.TOOLS ───────────────────► builtin_mcp_tools() ──► RESP array of
    │                                                                {name, description, inputSchema}
    │
    ├─ RESP: MCP.CALL <tool> <args_json> ─┐
    │                                      │
    └─ RESP: MCP.RPC <json_rpc_request> ───┤  method == "tools/call"
                                            │
                                            ▼
                              plan_tool_command(tool, args)
                                            │  (JSON Schema args ──► native Command)
                                            ▼
                         Box::pin(execute_command(planned_cmd, ...))
                              (the SAME top-level dispatcher every
                               ordinary client command runs through:
                               ACL check → shard routing → AOF/replication)
                                            │
                                            ▼
                       ShardDb / AgentMemoryBank / SemanticCache / HnswIndex / InvertedIndex
                         (src/table.rs, src/agent.rs, src/vector.rs, src/search.rs)
                                            │
                                            ▼
                               raw RESP reply bytes (sub_out)
                                            │
                              resp_bytes_to_json(sub_out)
                                            │
                           format_mcp_call_result(value, is_error)
                                            │
                                            ▼
                         {"content":[{"type":"text",...}],
                          "structuredContent": ..., "isError": bool}
```

`src/mcp.rs` itself never imports or calls into `src/agent.rs`, `src/vector.rs`, or
`src/search.rs` directly (its only non-`resp`/`serde_json` imports are `crate::resp::{Command,
SetCondition, VsimTarget}` and `crate::search::SearchOptions`, both just used to *construct*
`Command` values). All actual reads/writes happen inside the recursive `execute_command` call —
`mcp.rs` is a pure translation layer sitting in front of the normal command path, not a second
storage-access path.

## 4. Key Invariants

1. **Every MCP tool call becomes a real `Command` and re-enters `execute_command`.** There is no
   tool whose effect is implemented inline inside `mcp.rs` — `plan_tool_command` either returns a
   `Command` that the normal dispatcher already knows how to execute, or returns an `Err(String)`
   before any execution happens at all.
2. **The tool catalog (`MCP.TOOLS`, `tools/list`) and the tool planner (`plan_tool_command`) are
   the two halves of one contract and must be kept in sync by hand.** `builtin_mcp_tools()`
   declares a tool's JSON Schema; `plan_tool_command` independently re-reads the same `args: &Value`
   using its own field names. Nothing in the type system enforces that a schema's declared
   `properties`/`required` list actually matches what `plan_tool_command`'s corresponding `match`
   arm reads — they are two hand-written, separately maintained descriptions of the same
   contract (see the Internal doc for a verified instance of these already drifting).
3. **`MCP.CALL`/`MCP.RPC`'s `tools/call` never return a RESP `-ERR` for a tool-level failure.**
   Only a malformed outer JSON payload (invalid `args_json`/invalid JSON-RPC request body)
   produces a RESP error or a JSON-RPC `-32700 Parse error`. An unknown tool name, a missing
   required argument, or a failing underlying command all come back as a **successful** RESP bulk
   string / JSON-RPC `result` whose body is `{"isError": true, ...}` — matching the MCP
   convention that tool execution failures are reported inside the result envelope, not as a
   transport-level error.
4. **Tool calls carry the calling connection's authentication context unchanged.** The recursive
   `execute_command` call is passed the same `authenticated`/`auth_user`/`asking` state as the
   outer `MCP.CALL`/`MCP.RPC` invocation, so ACL enforcement for the translated command runs
   against whichever user issued the MCP command — an MCP tool call cannot be used to execute a
   command the calling connection's ACL user would not otherwise be allowed to run.
5. **The tool menu is exactly the 11 tools `builtin_mcp_tools()` lists — nothing more, nothing
   configurable at runtime.** `plan_tool_command`'s final `_ => Err(...)` arm rejects any tool
   name outside that fixed set.

## 5. Implementation Reference

For the concrete `McpToolDef` struct, the full tool-by-tool argument-to-`Command` mapping table,
the hand-rolled RESP→JSON reply converter, the JSON-RPC 2.0 method dispatch table, exact line
references, and verified gaps/bugs found by close reading, see
[`docs/internal/21_mcp_server.md`](../internal/21_mcp_server.md).
