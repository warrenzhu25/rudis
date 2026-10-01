# Component 20: Agent Memory — Working Window, Episodic Recall, Checkpoints & Tool Leases — Design & Architecture

> **Subsystem Scope**: `src/agent.rs` (802 lines)
> **Implementation Reference**: [`docs/internal/20_agent_memory.md`](../internal/20_agent_memory.md)

---

## 1. Purpose

`src/agent.rs` adds four independent, purpose-built in-memory stores for building AI agents on
top of Rudis, each exposed through its own `AGENT.*`/`LLM.*` command family:

- **`AgentMemorySession`** (`AGENT.MEM.ADD`/`CONTEXT`/`COMPACT`/`INFO`/`CLEAR`): token-budgeted
  conversational "working memory" — a chronological list of turns — plus optional semantic
  "episodic memory" over the same turns, backed by an [`HnswIndex`](08_vector_engine.md) reused
  directly from the vector search engine (Component 08) rather than a bespoke ANN structure.
- **`LlmQuotaBucket`** (`LLM.QUOTA.RESERVE`/`SETTLE`/`INFO`): a dual RPM (requests/window) + TPM
  (tokens/window) rate governor with two-phase pre-inference reservation and post-stream
  settlement, so a caller can know *before* paying for an LLM call whether it would exceed quota.
- **`AgentCheckpointThread`** (`AGENT.CHECKPOINT.PUT`/`GET`/`HISTORY`): a parent-pointer DAG of
  named "steps," so an agent's execution state can be durably checkpointed at each step and later
  time-traveled to, including along abandoned/forked branches.
- **`AgentToolRegistry`** (`AGENT.TOOL.CLAIM`/`COMPLETE`): an idempotent lease-and-result-cache
  registry for tool calls, so a flaky or retried agent step does not re-execute (or double-charge
  for) a tool invocation that already succeeded, or race another concurrent attempt at the same
  call.

These four stores share nothing at the data-structure level — they are four separate
`HashMap`s, one per store, embedded directly as fields on `ShardDb` (`src/shard.rs`) — but they
are grouped in one module and one design document because they solve adjacent problems for the
same class of caller (an LLM-driven agent loop running against Rudis as its state backend) and
entered the codebase together (§2 of the internal doc traces the four as four separate commits).

**Scope note**: this document covers only `src/agent.rs`. `src/mcp.rs` (MCP JSON-RPC server) and
`src/semcache.rs`/the `SemanticCache` type in `src/vector.rs` (`SEMANTIC.*` commands) are separate
subsystems that happen to share the same region of the `Command` enum and the same RESP command
table; they are not described here.

## 2. Design Rationale ("Why")

### 2.1 Why not store agent state as ordinary keys in `RudisTable`?

Every one of these four stores could, in principle, be built out of existing primitives — a
conversation as a Redis List, episodic recall as a hand-rolled `VADD`-backed vector set, a
checkpoint DAG as a Hash of Hashes, a tool lease as `SET ... NX PX`. Rudis instead gives each
concern its own typed in-memory structure (`AgentMemorySession`, `AgentCheckpointThread`,
`AgentToolRegistry`) for the same reason the CRDT and probabilistic-structure subsystems do
(Components 12, 18): the operations an agent actually needs — "assemble a token-budgeted prompt
context," "claim-or-return-cached-result for a call id," "walk DAG lineage from an arbitrary
node" — are multi-step, structure-aware algorithms that are awkward and slow to express as a
sequence of generic key/value commands, and are only safe to implement once per key family rather
than once per caller. A single `AGENT.MEM.CONTEXT` call does token-budget accumulation *and*
filtered HNSW search *and* result stitching in one shard-local, lock-free operation; the
equivalent as generic commands would be several round trips with no way to make the filtering
(excluding turns already in the recency window) atomic with the search itself.

### 2.2 Why four independent sub-stores instead of one unified "agent" structure

Each of the four problems has a different natural key, a different natural value shape, and a
different lifecycle:

- A memory session is keyed by a conversation/session id and grows by appending turns.
- An LLM quota bucket is keyed by whatever the caller chooses to rate-limit on (an API key, a
  model name, a tenant id) and is a sliding-window counter, not a growing log.
- A checkpoint thread is keyed by an agent-run/thread id and is a DAG, not a list.
- A tool registry is keyed by a tool-invocation namespace and is a lease table, not a log or DAG.

Folding these into one struct would force every operation to pay for fields it doesn't use and
would make the four independently-reasoned-about correctness properties (token budgeting,
RPM/TPM admission control, DAG lineage integrity, lease mutual exclusion) harder to verify in
isolation. Keeping them as four `HashMap<Bytes, T>` fields on `ShardDb` (mirroring exactly how
`CrdtStore`, `vector_indexes`, and `semantic_caches` are already embedded) costs nothing extra and
keeps each type's invariants local to its own `impl` block.

### 2.3 Why compaction never deletes episodic embeddings

`AgentMemorySession::compact` (§3 internal doc) moves old turns out of the *working* window
(marks them `compacted: true`, so they stop counting against the token budget and stop appearing
in `AGENT.MEM.CONTEXT`'s `recent_turns`) but never removes their vectors from the session's
`HnswIndex`, and `AGENT.MEM.CONTEXT`'s semantic-recall path happily returns a compacted turn as a
recalled episode. This is deliberate: the entire point of pairing a token-budgeted working window
with HNSW-backed recall is that compaction should be *lossy for the prompt budget but not for
retrieval* — an agent can forget something from its immediate context and still semantically
"remember" it minutes or hours later if a new turn's embedding lands close to it. The trade-off is
that the HNSW index, unlike the working window, is never pruned by compaction and has no
independent size cap (§4), so a very long-running session's index grows without bound until the
whole session is dropped via `AGENT.MEM.CLEAR`.

### 2.4 Why checkpoints are a parent-pointer DAG, not a linear append log

An agent execution graph routinely forks (the agent tries one plan, it fails, it backtracks and
tries another) and an operator or the agent itself may want to resume from, or inspect, any
earlier step — not just the most recent one. A linear log (like the AOF or a Redis Stream) can
represent *that something happened at step N*, but cannot cheaply represent *that step N2 is a
sibling of, not a successor to, step N1*, which is exactly what branching execution needs.
`AgentCheckpointThread` instead stores each step as a node carrying an explicit `parent_id`, so
`AGENT.CHECKPOINT.HISTORY` can walk lineage from any named step back toward the root, correctly
skipping over sibling branches, and a new `AGENT.CHECKPOINT.PUT` can target any existing step as
its parent — including an old one — to represent an explicit retry/fork rather than only ever
extending the most recent state.

### 2.5 Why tool calls are idempotent leases, not simple locks

A naive `SETNX`-style lock around a tool call only prevents two concurrent attempts from running
the tool at the same instant; it does nothing for the more common real failure mode in an agent
loop — a caller that times out waiting for a tool result and retries the *same* logical call,
potentially after the first attempt already completed and produced a (possibly
expensive-to-regenerate, or non-idempotent) result. `AgentToolRegistry::claim` (§3 internal doc)
instead models three states per `call_id` — `CLAIMED` (you now own this attempt), `IN_PROGRESS`
(someone else's active lease, come back later), `COMPLETED` (here is the cached result from a
prior attempt, do not re-run the tool) — so a caller that retries a call whose previous attempt
already finished gets the original result back instead of re-executing a possibly
non-idempotent side effect, while a caller that retries a call whose lease has simply expired
(the previous attempt crashed or hung) is allowed to reclaim it and try again.

### 2.6 Why the LLM quota governor reserves before the call, not after

Counting tokens only *after* an LLM call completes cannot prevent that call from being made in
the first place when the caller is already over quota — by the time the actual token count is
known, the (potentially costly) call already happened. `LlmQuotaBucket::reserve` instead commits
an *estimated* token cost up front, atomically checked against both the RPM and TPM ceilings
before the caller is told "go ahead," and `settle` later reconciles that estimate against the
real usage once it is known (§3 internal doc), refunding or charging the difference into the same
sliding window. This two-phase reserve/settle split is the standard pattern for rate-limiting a
cost that is not known until after the gated action starts.

## 3. Architecture Overview

```
                         ┌─────────────────────────── ShardDb (per-shard, thread-local) ───────────────────────────┐
AGENT.MEM.*      ──────► │ agent_memories:    HashMap<session_key, AgentMemorySession>                             │
                         │                      ├─ turns: Vec<AgentTurn>           (working memory, chronological) │
                         │                      └─ index: Option<HnswIndex>        (episodic memory, Cosine)       │
LLM.QUOTA.*       ──────►│ llm_quotas:        HashMap<quota_key, LlmQuotaBucket>    (sliding RPM/TPM window)        │
AGENT.CHECKPOINT.*──────►│ agent_checkpoints: HashMap<thread_key, AgentCheckpointThread> (parent-pointer DAG)       │
AGENT.TOOL.*      ──────►│ agent_tools:       HashMap<tool_key, AgentToolRegistry>  (call_id -> lease/result)       │
                         └────────────────────────────────────────────────────────────────────────────────────────┘
```

Every `AGENT.*`/`LLM.*` command carries exactly one key (a session id, quota key, thread id, or
tool-registry namespace) and is routed to the single shard that owns that key through the same
CRC16/key-routing mechanism used for every other keyed command (Component 04,
`target_shard_of_cmd` in `src/connection.rs`). Unlike CRDT's whole-store `DUMP`/`MERGE`/`GC`
(Component 12), **no command in this subsystem fans out across shards** — every operation here is
single-key and single-shard by construction, which keeps the hot paths (`AGENT.MEM.ADD`,
`AGENT.TOOL.CLAIM`) as cheap as any other keyed Rudis command.

## 4. Key Invariants

1. **A memory session's working window is strictly token-budgeted, not turn-count-budgeted.**
   `AGENT.MEM.CONTEXT`'s `recent_turns` is built by walking turns newest-first and stopping the
   instant the next turn would push cumulative tokens over `max_tokens` — so the window's *turn
   count* varies run to run with content length, but its *token count* never exceeds the caller's
   budget.
2. **Episodic recall never returns a turn already present in the working window.** The HNSW
   search inside `AgentMemorySession::context` is given an exclusion filter built from exactly the
   turn ids just selected for `recent_turns`, so a semantically-close turn that is already in the
   prompt is never duplicated into `recalled_episodes` — but a *compacted* turn is always eligible
   for recall, since compaction removes it from the working window, not from `recent_ids` (§2.3).
3. **Compaction is monotonic and summary-preserving.** `compact` only ever flips active turns to
   `compacted: true` and inserts one new synthetic `"system"`-role summary turn in their place; it
   never deletes a turn's row or its HNSW embedding, so `AGENT.MEM.INFO`'s `total_turns` only ever
   grows and a session's full turn history (compacted or not) remains walkable.
4. **A tool call's `COMPLETED` result is authoritative over any in-flight lease.** `claim` checks
   for a stored `output` before it checks the lease, so once `AGENT.TOOL.COMPLETE` has recorded a
   result, every subsequent `CLAIM` for that `call_id` returns the cached result instead of
   granting a new lease — until that result's own optional TTL (set at `COMPLETE` time, independent
   of the claim TTL) expires.
5. **Checkpoint lineage is walked with cycle protection.** `AgentCheckpointThread::history` tracks
   visited step ids while following `parent_id` pointers and stops rather than looping forever if
   the DAG is malformed (e.g., a step's parent pointer was made to point at one of its own
   descendants by a buggy or adversarial caller) — the walk is bounded by `limit` and by the
   visited-set, never by trusting the graph's shape alone.
6. **No cross-store coupling, no cross-shard fan-out.** The four stores never reference each
   other's keys, and (§3) every command touches exactly one key on exactly one shard — there is no
   analogue to CRDT's node-identity sharing or whole-store aggregation here.

## 5. Implementation Reference

For concrete struct/enum definitions, the exact algorithms behind working-window assembly,
compaction, HNSW-filtered recall, the RPM/TPM admission check, DAG lineage walking, tool-lease
state transitions, and the full persistence/replication picture (RDB extended-record types
`16`–`18`, the AOF/PSYNC coverage gaps), see
[`docs/internal/20_agent_memory.md`](../internal/20_agent_memory.md).
