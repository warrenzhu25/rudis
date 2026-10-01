# Component 20: Agent Memory — Working Window, Episodic Recall, Checkpoints & Tool Leases — Implementation Reference

> **Source Files**: `src/agent.rs` (802 lines)
> **High-Level Design Spec**: [`docs/design/20_agent_memory.md`](../design/20_agent_memory.md)

This document is a line-level account of every struct, function, and wire-format detail in
`src/agent.rs`, plus every touchpoint in `src/resp.rs`, `src/connection.rs`, `src/shard.rs`, and
`src/aof.rs` that agent-memory data flows through. Nothing here is carried forward from memory —
every claim below was checked against the current source in this pass.

---

## 1. Module Responsibilities

| File | Responsibility |
| :--- | :--- |
| `src/agent.rs` | `AgentTurn`/`AgentEpisodeHit`/`AgentContextResult`/`AgentMemorySession` (working memory + HNSW episodic recall); `LlmReserveResult`/`LlmQuotaBucket` (RPM/TPM governor); `AgentCheckpointNode`/`AgentCheckpointThread` (DAG checkpoint store); `ToolClaimState`/`ToolClaimResult`/`ToolCallEntry`/`AgentToolRegistry` (idempotent tool leases). Four independent types, no shared base type, no module-level state. |
| `src/shard.rs` | Four `HashMap<Bytes, T>` fields on `ShardDb` — `agent_memories`, `llm_quotas`, `agent_checkpoints`, `agent_tools` (`shard.rs:612-616`) — plus thin forwarding methods (`shard.rs:3751-3900`), the extended-RDB-chunk save logic for sessions/checkpoints/tools (`shard.rs:2561-2655`, record types `16`/`17`/`18`), the symmetric restore logic (`shard.rs:3140-3260`), and `flushdb` clearing all four stores (`shard.rs:1832-1841`). **`llm_quotas` is the one store with no RDB save/restore branch anywhere in `shard.rs`** — see §4.2. |
| `src/resp.rs` | Parses the thirteen `AGENT.MEM.*`/`LLM.QUOTA.*`/`AGENT.CHECKPOINT.*`/`AGENT.TOOL.*` subcommands into the corresponding `Command::Agent*`/`Command::LlmQuota*` variants (`resp.rs:1327-1392` for the enum arms, `resp.rs:12442-12948` for the parsers). |
| `src/connection.rs` | `target_shard_of_cmd` (`connection.rs:12338`) routes all thirteen variants by their one key field, exactly like every other keyed command (`connection.rs:12516-12528`); `execute_local_command`'s single-shard dispatch arm (`connection.rs:18034-18302`) calls the `ShardDb` forwarding methods and writes RESP replies; mutating arms call `record_change!(cmd)` (§4.1) — **except the three `LlmQuota*` arms, which never call it** (§4.2). |
| `src/aof.rs` | `command_to_resp` has match arms for `AgentMemAdd`/`AgentMemCompact`/`AgentMemClear`/`AgentCheckpointPut`/`AgentToolClaim`/`AgentToolComplete` (`aof.rs:1678-1836`) — re-serializing each back into its canonical RESP command for AOF/replication. **There is no arm for any `LlmQuota*` variant** (`rg -n "Llm" src/aof.rs` returns zero matches) — see §4.2. `rewrite_shard_aof`'s BGREWRITEAOF snapshot path separately re-emits `AGENT.MEM.*`/`AGENT.CHECKPOINT.*`/`AGENT.TOOL.*` state from the live stores (`aof.rs:2431-2521`, exercised by the round-trip test at `aof.rs:2908-3040`) — again with no equivalent for `llm_quotas`. |

---

## 2. Data Structures (verbatim from `src/agent.rs`)

### 2.1 Working memory + episodic recall

```rust
// agent.rs:8-15
pub struct AgentTurn {
    pub id: u64,
    pub role: Bytes,
    pub content: Bytes,
    pub tokens: u64,
    pub meta: Option<Bytes>,
    pub compacted: bool,
}

// agent.rs:18-25 — a semantically recalled past episode outside the active recent window
pub struct AgentEpisodeHit {
    pub id: u64,
    pub role: Bytes,
    pub content: Bytes,
    pub score: f32,          // clamp(1 - cosine_distance, 0.0, 1.0) — see §3.2
    pub meta: Option<Bytes>,
}

// agent.rs:28-32 — the full reply payload for AGENT.MEM.CONTEXT
pub struct AgentContextResult {
    pub recent_turns: Vec<AgentTurn>,
    pub recalled_episodes: Vec<AgentEpisodeHit>,
}

// agent.rs:35-44
pub struct AgentMemorySession {
    pub session_id: String,
    pub turns: Vec<AgentTurn>,
    pub id_to_pos: HashMap<u64, usize>,   // turn id -> index into `turns`
    pub index: Option<HnswIndex>,         // lazily created on first vector; metric is always Cosine
    pub next_turn_id: u64,                // starts at 1
    pub active_tokens: u64,               // sum of tokens for turns with compacted == false
    pub compactions: u64,                 // count of successful AGENT.MEM.COMPACT calls
}
```

`AgentTurn` has six fields — `id`/`tokens` (`u64`), `role`/`content` (`Bytes`, unbounded), `meta`
(`Option<Bytes>`), `compacted` (`bool`). There is no struct-level cap on `content` length or on
`turns.len()` anywhere in the file — a session grows until `AGENT.MEM.CLEAR` removes it outright
(`agent_mem_clear`, `shard.rs:3805-3807`, a single `HashMap::remove`) or the process restarts
without RDB/AOF coverage of that key (it does have RDB coverage — §4.2). `id_to_pos` is a second,
redundant index into `turns` kept in sync by hand on every mutation (`add` pushes-and-inserts;
`compact` fully rebuilds it — see §3.3) rather than `turns` being a `HashMap` itself, so that
`context()`'s chronological-order walk (`turns.iter().rev()`) stays a plain `Vec` scan.

**The HNSW index's metric is hard-coded to `VectorMetric::Cosine`** at both construction sites
(`agent.rs:80-81` in `add`, `agent.rs:198-199` in `compact`) — there is no `AGENT.MEM.ADD` option
to choose Euclidean or dot-product distance for a session's episodic recall, unlike
`VADD`/`FT.CREATE`'s vector fields (Component 08/09), which do allow a caller-chosen metric.

### 2.2 LLM quota governor

```rust
// agent.rs:258-263 — decision returned by LLM.QUOTA.RESERVE
pub struct LlmReserveResult {
    pub allowed: bool,
    pub reservation_id: Option<u64>,
    pub remaining_tokens: u64,
    pub retry_after_ms: u64,
}

// agent.rs:268-273
pub struct LlmQuotaBucket {
    pub requests: VecDeque<(Instant, u64)>,            // settled (ts, actual_tokens)
    pub reservations: HashMap<u64, (Instant, u64)>,    // reservation_id -> (ts, est_tokens)
    pub next_reservation_id: u64,                      // starts at 1
    pub window_ms: u64,                                // sliding-window width, default 60_000
}
```

`Default for LlmQuotaBucket` (`agent.rs:275-279`) is `Self::new(60_000)` — a 60-second default
window, used by `shard.rs:3818-3821`'s `.entry(key).or_insert_with(...)` the first time a key is
seen. **`window_ms` is not fixed at bucket-creation time**: every call to `reserve` that passes an
explicit `window_ms` argument overwrites `self.window_ms` in place (`agent.rs:312-314`) before
evicting expired entries — so a later `LLM.QUOTA.RESERVE ... WINDOW <ms>` call against an
already-existing bucket silently changes that bucket's sliding-window width for all subsequent
calls, not just the current one.

### 2.3 Checkpoint DAG

```rust
// agent.rs:390-397 — one node in an AgentCheckpointThread's execution DAG
pub struct AgentCheckpointNode {
    pub step_id: Bytes,
    pub parent_id: Option<Bytes>,
    pub seq: u64,              // monotonically increasing PUT sequence number for this thread
    pub timestamp_ms: u64,     // wall-clock ms, computed server-side inside `put()` — see §4.3
    pub state: Bytes,
    pub metadata: Option<Bytes>,
}

// agent.rs:401-406
pub struct AgentCheckpointThread {
    pub nodes: hashbrown::HashMap<Bytes, AgentCheckpointNode>,  // step_id -> node (latest PUT wins)
    pub order: Vec<Bytes>,                                      // first-seen insertion order of step_ids
    pub head_step_id: Option<Bytes>,                             // most recently PUT step, any branch
    pub next_seq: u64,                                           // starts at 1
}
```

`nodes` is keyed by `step_id`, so a second `PUT` against an existing `step_id` **overwrites** that
node in place (new `state`/`parent_id`/`metadata`/`timestamp_ms`, a freshly incremented `seq`) —
`order` is only appended to the first time a given `step_id` is seen (`agent.rs:445-447`), so
re-`PUT`ting an existing step does not create a duplicate `order` entry or change its position in
save/RDB iteration order (§4.2). `head_step_id` is unconditionally set to whatever `step_id` was
just `PUT` (`agent.rs:449`), including when that `PUT` was an overwrite of an old node — so a
caller can retroactively make an old, previously-superseded step the thread's head again simply by
re-`PUT`ting it, without that being a "new" branch in any structural sense.

### 2.4 Idempotent tool-call leases

```rust
// agent.rs:485-489
pub enum ToolClaimState { Claimed, InProgress, Completed }

// agent.rs:492-496 — reply payload for AGENT.TOOL.CLAIM
pub struct ToolClaimResult {
    pub state: ToolClaimState,
    pub output: Option<Bytes>,
    pub meta_int: u64,   // attempt count when Claimed; remaining lease ms when InProgress; 0 when Completed
}

// agent.rs:499-505 — crate-private; not constructible or inspectable from outside agent.rs
pub(crate) struct ToolCallEntry {
    pub(crate) input: Option<Bytes>,
    pub(crate) output: Option<Bytes>,
    pub(crate) attempt: u64,
    pub(crate) lease_until: Option<Instant>,   // active-claim expiry
    pub(crate) expire_at: Option<Instant>,     // completed-result TTL (None = never expires)
}

// agent.rs:509-511
pub struct AgentToolRegistry {
    pub(crate) calls: hashbrown::HashMap<Bytes, ToolCallEntry>,
}
```

`lease_until`/`expire_at` are `std::time::Instant` — process-monotonic, not wall-clock, and not
directly serializable; the RDB save path converts them to Unix-epoch milliseconds and the restore
path converts back (§4.2). `AgentToolRegistry` derives `Default`, used by
`.entry(key).or_default()` at both `shard.rs:3886-3888` (claim) and the registry-level
`.or_default()` pattern mirrored for checkpoints (`shard.rs:3853-3856`).

---

## 3. Execution Algorithms

### 3.1 `AgentMemorySession::add` — append a turn, optionally index its embedding (`agent.rs:66-102`)

1. Allocates `id = self.next_turn_id` (pre-increment happens later, at step 4 — so the id used
   below and the stored turn's `id` field are always in sync).
2. If a `vector` was supplied: rejects an empty vector (`"ERR vector dimension must be greater
   than 0"`); lazily creates `self.index` via `HnswIndex::new(session_id.clone(), vec.len(),
   VectorMetric::Cosine)` on the *first* vector a session ever sees (so a session's embedding
   dimension is fixed by whichever turn happens to add a vector first); rejects a dimension
   mismatch against an already-established index (`"ERR vector dimension mismatch"`); inserts the
   vector keyed by `Bytes::from(id.to_string())` — **the HNSW key is the turn id rendered as a
   decimal string**, not the raw `u64`, which is what `context()`'s recall path has to parse back
   out (§3.2).
3. `tokens` is either the caller-supplied value or `Self::estimate_tokens(&content)`
   (`agent.rs:59-62`): `content.len().div_ceil(4).max(1)` — a crude "~4 bytes per token" heuristic,
   not a real tokenizer; it undercounts for any text whose real token/byte ratio differs
   materially from 4 (e.g., CJK text, where one token is often one 3-byte UTF-8 character, not
   four bytes).
4. `next_turn_id += 1`; `active_tokens = active_tokens.saturating_add(tok)`; the new `AgentTurn`
   (`compacted: false`) is pushed onto `turns`, and `id_to_pos` records its index.

### 3.2 `AgentMemorySession::context` — assemble a prompt context (`agent.rs:107-170`)

```rust
// Recent-window pass: newest-first, stop the instant the budget would be exceeded
for turn in self.turns.iter().rev() {
    if turn.compacted { continue; }
    if used_tokens.saturating_add(turn.tokens) > max_tokens { break; }
    used_tokens += turn.tokens;
    recent_ids.insert(turn.id);
    recent_turns.push(turn.clone());
}
recent_turns.reverse();   // restore chronological order for the caller
```

This is a greedy knapsack-by-recency, not a true 0/1 knapsack — the very first turn (scanning
backward) that would overflow `max_tokens` stops the walk entirely, even if an older, smaller turn
further back would have fit. Compacted turns are skipped outright (`continue`), so they never
occupy window budget and never appear in `recent_turns`, matching the design intent (§2.3 design
doc) that compaction is lossy for the prompt window but not for retrieval.

Episodic recall then runs only if both a `query` vector and an existing `self.index` are present
and `recall_k > 0`:

```rust
let filter_fn = |k: &Bytes| -> bool {
    std::str::from_utf8(k).ok().and_then(|s| s.parse::<u64>().ok())
        .is_some_and(|id| !recent_ids.contains(&id))
};
let candidates = idx.search_filtered(q, recall_k, None, false, Some(&filter_fn));
```

`search_filtered` (`src/vector.rs:1799`, signature `(query, k, ef_runtime: Option<usize>, rerank:
bool, filter: Option<&dyn Fn(&Bytes) -> bool>) -> Vec<(Bytes, f32)>`) is called with
`ef_runtime: None` (use the index's own default `ef_search`) and `rerank: false` (no `.vtier`
disk-reranking pass — not applicable; agent-memory HNSW indexes are always in-memory). The filter
closure is the *only* mechanism preventing a turn already selected into `recent_turns` from being
duplicated into `recalled_episodes`; it is an in-graph filter (evaluated during the HNSW walk, not
a post-hoc list subtraction), matching how `FT.HYBRID`'s filtered vector search works (Component
09). For each surviving `(key, dist)` candidate, the key is parsed back from its decimal-string
form to a `u64` id, looked up through `id_to_pos` into `turns` (so a hit is skipped — silently, not
erred — if, inconsistently, `id_to_pos` ever lacked that id), and converted to an
`AgentEpisodeHit` with `score = (1.0 - dist).clamp(0.0, 1.0)`.

**Cosine distance in `src/vector.rs` is already `max(1 - cosine_similarity, 0.0)`**
(`cosine_distance_avx2` etc., e.g. `vector.rs:180-185`: `(1.0 - dot/norm).max(0.0)`), so `score`
here is effectively `clamp(cosine_similarity, 0.0, 1.0)` — a **negative** cosine similarity
(near-opposite embeddings) and a **zero** cosine similarity (orthogonal embeddings) both collapse
to a `score` of `0.0`, losing the ability to distinguish "unrelated" from "opposite meaning" in
the returned score.

### 3.3 `AgentMemorySession::compact` — fold old turns into one summary (`agent.rs:174-238`)

1. Collects the positions of all currently-active (`!compacted`) turns, in `turns` order.
2. No-ops (`Ok(0)`) if there are `<= keep_recent` active turns — compaction never runs backward
   past the point where it would have nothing useful to do.
3. Allocates a fresh `summary_id = next_turn_id` and, if a `vector` was supplied for the summary,
   indexes it into the (lazily-created, same-dimension-enforced) HNSW index exactly as `add` does
   — the summary turn is itself a fully recallable episodic memory.
4. `compact_count = active_positions.len() - keep_recent`; the **oldest** `compact_count` active
   positions are flipped to `compacted: true` and their token cost subtracted from
   `active_tokens`; the newest `keep_recent` active turns are left untouched.
5. The new summary turn (`role: "system"`, `compacted: false`) is `insert`ed into `turns` **in
   place of** the oldest-kept active turn's position (`active_positions[compact_count]`), i.e. it
   is spliced into chronological position immediately before the turns it did not summarize — not
   appended at the end — so a subsequent `context()` call sees `[..., summary, <kept recent
   turns>]` in correct chronological order (exercised by the module's own test at
   `agent.rs:659-684`).
6. `id_to_pos` is **fully rebuilt from scratch** (`clear()` + re-walk of all of `turns`) after the
   insert, because the `Vec::insert` shifted every subsequent turn's index by one — this makes a
   single `compact` call's bookkeeping cost `O(total_turns)`, not just `O(compact_count)`, so
   compaction cost grows with a session's entire lifetime turn count (including already-compacted
   turns), not just with how much is being compacted in that call.
7. `compactions += 1`.

### 3.4 `AgentMemorySession::info` (`agent.rs:240-253`)

Returns a 6-tuple: `(active_turns, total_turns, active_tokens, vector_dim, episodic_vector_count,
compactions)` — `active_turns` is computed by a fresh `O(total_turns)` filter over `turns` on
every call (not cached), `vector_dim`/`episodic_vector_count` come from `self.index.as_ref()` (`0`
if no index exists yet).

### 3.5 `LlmQuotaBucket::reserve` — atomic dual-limit admission check (`agent.rs:305-356`)

```rust
self.evict_expired(now);  // drop requests/reservations older than window_ms (agent.rs:291-302)
let active_reqs = self.requests.len() + self.reservations.len();
let committed = used_tokens_from_requests + reserved_tokens_from_reservations;
if active_reqs < rpm && committed.saturating_add(est_tokens) <= tpm {
    // admit: insert a new reservation keyed by an incrementing id, return allowed=true
} else {
    // deny: compute retry_after_ms from whichever of the oldest request/reservation expires first
}
```

Both the RPM check (`active_reqs < rpm`, strictly less-than — an `rpm` of `N` permits at most `N`
concurrently-active requests+reservations) and the TPM check
(`committed + est_tokens <= tpm`) must pass for admission; failing either denies the whole
reservation (no partial admission). `retry_after_ms` on denial is derived from `window_ms minus
elapsed-since-the-oldest-outstanding-entry`, floored at `1` — i.e. "come back once the oldest
entry would have aged out of the window," not a fixed backoff.

### 3.6 `LlmQuotaBucket::settle` — reconcile estimate vs. actual (`agent.rs:360-370`)

Removes the reservation by id, moves it into `requests` with the **actual** token count (not the
estimate), and returns `(found: bool, delta: i64)` where `delta = actual - estimated` (negative
means the reservation over-estimated and effectively frees up budget for subsequent `reserve`
calls once eviction catches up; the freed budget is realized passively, there is no explicit
"refund" operation — it simply falls out of `used_tokens` now reflecting the smaller actual
value). Settling an unknown/already-evicted `reservation_id` is a no-op returning `(false, 0)`,
not an error.

### 3.7 `AgentCheckpointThread::put`/`get`/`history` (`agent.rs:424-481`)

`put` always computes `timestamp_ms` itself via `SystemTime::now()` at call time (**not** a
parameter of the RESP command — see §4.3 for why this matters for replication), assigns the next
`seq`, and unconditionally sets `head_step_id` to the just-put `step_id` (§2.3). `get(step_id:
Option<&Bytes>)` defaults to `head_step_id` when `None` is passed — this is how `AGENT.CHECKPOINT.GET`
with no `STEP`/third argument returns "whatever the current head is." `history(from_step, limit)`
walks `parent_id` pointers starting from `from_step` (or `head_step_id`), pushing each visited
node (`Vec<AgentCheckpointNode>`, cloned) until it hits a step with no stored node, exhausts
`limit`, or — the cycle guard — revisits a `step_id` already seen in this walk
(`hashbrown::HashSet` `visited`, `agent.rs:466-469`), in which case it stops rather than looping
forever. `limit == 0` short-circuits to an empty `Vec` immediately.

### 3.8 `AgentToolRegistry::claim` — the three-state lease machine (`agent.rs:518-569`)

```
calls.get_mut(call_id)?
  ├─ expire_at.is_some_and(now >= exp)            → remove entry, fall through to "first claim"
  ├─ else if output.is_some()                      → return Completed { output: cached }
  ├─ else if lease_until.is_some() && now < lease  → return InProgress { meta_int: remaining_ms }
  └─ else (no output, lease absent or expired)     → attempt += 1; lease_until = now + ttl;
                                                       if input given, overwrite stored input;
                                                       return Claimed { meta_int: attempt }
(no existing entry) → insert fresh entry (attempt: 1, lease_until: now + ttl) → Claimed { meta_int: 1 }
```

Order of checks matters: a **completed-and-not-yet-expired** result always wins over any lease
state, so `COMPLETE` is effectively "final" for a call id until its own TTL lapses, regardless of
what any lease on that id is doing. An **expired completed** result (`expire_at` in the past) is
treated identically to "never existed" — the entry is deleted outright and the call is claimable
again from `attempt = 1`, losing the original attempt count. A **lease that simply expired**
(no `output`, `lease_until` in the past) does *not* delete the entry — it falls into the `else`
branch, bumping `attempt` and renewing the lease in place, so attempt history across retries is
preserved for in-flight (never-completed) calls but not for calls whose completed result itself
expired.

### 3.9 `AgentToolRegistry::complete` (`agent.rs:571-592`)

Unconditionally sets `output`, clears `lease_until` (the lease is moot once a result exists), and
sets `expire_at = ttl_ms.map(|ms| now + Duration::from_millis(ms.max(1)))` — **`expire_at` stays
`None` (never expires) unless `COMPLETE` was called with an explicit `TTL`/`PX`**, which also means
an `AGENT.TOOL.COMPLETE` with no TTL argument produces a permanently-cached result for that
`call_id` that `CLAIM` will keep returning forever (bounded only by `AGENT.MEM.CLEAR`-style
removal being unavailable for this store — there is no `AGENT.TOOL.CLEAR`/`DEL` command at all;
the only ways an entry leaves `calls` are TTL expiry on a completed result or `FLUSHDB`). If no
entry existed for `call_id`, `complete` creates one directly in the `Completed`-equivalent shape
(`attempt: 1`, no lease) rather than erroring — i.e. `AGENT.TOOL.COMPLETE` can be called without a
prior `CLAIM` and still succeeds.

---

## 4. Cross-Component Interactions, Persistence, and Replication

### 4.1 Command routing and AOF/replication coverage for the three leaf stores

All thirteen variants route through `target_shard_of_cmd` (`connection.rs:12338`,
`connection.rs:12516-12528`) by their single key field, and are dispatched in
`execute_local_command`'s match (`connection.rs:18034-18302`). The `record_change!(cmd)` macro
(`connection.rs:12740-12761`, identical mechanism to the one documented for CRDT in Component 12)
is the single gate for AOF-append and replica-propagation, and it is called **conditionally**,
matching real mutation:

| Command | `record_change!` called when |
| :--- | :--- |
| `AgentMemAdd` | `Ok(id)` (every successful add) |
| `AgentMemContext` | never (pure read) |
| `AgentMemCompact` | only if `compacted > 0` (a true no-op compact records nothing) |
| `AgentMemInfo` | never (pure read) |
| `AgentMemClear` | only if the session existed and was removed |
| `AgentCheckpointPut` | always (every `PUT` mutates, even a same-`step_id` overwrite) |
| `AgentCheckpointGet` / `History` | never (pure reads) |
| `AgentToolClaim` | only when `state == Claimed` (an `InProgress`/`Completed` reply changed nothing) |
| `AgentToolComplete` | always |
| `LlmQuotaReserve` / `Settle` / `Info` | **never, for any of the three** — see §4.2 |

`command_to_resp` in `src/aof.rs` has arms re-serializing `AgentMemAdd` (`aof.rs:1678-1713`),
`AgentMemCompact` (`aof.rs:1715-1747`), `AgentMemClear` (`aof.rs:1749-1755`), `AgentCheckpointPut`
(`aof.rs:1757-1785`), `AgentToolClaim` (`aof.rs:1787-1810`), and `AgentToolComplete`
(`aof.rs:1812-1835`) — each reconstructing the canonical RESP form of the original command
(e.g. `AgentMemAdd` re-adds `TOKENS`/`VEC <dim> <floats...>`/`META` only for the fields that were
originally `Some`). `AgentMemContext`/`AgentMemInfo`/`AgentCheckpointGet`/`AgentCheckpointHistory`
need no arm (they never mutate, so `record_change!` never calls `command_to_resp` for them in the
first place).

### 4.2 The LLM quota governor has zero persistence and zero replication

**Re-verified, three independent angles, all negative:**

1. `connection.rs`'s dispatch arms for `LlmQuotaReserve`/`LlmQuotaSettle`/`LlmQuotaInfo`
   (`connection.rs:18149-18195`) never call `record_change!` — confirmed by direct reading; no
   other mutating command in the file skips this macro for a state-changing operation.
2. `rg -n "Llm" src/aof.rs` returns **zero matches** — there is no `command_to_resp` arm for any
   `LlmQuota*` variant, and no BGREWRITEAOF resnapshot path touches `llm_quotas` either.
3. `src/shard.rs`'s extended-RDB-chunk writer (`save_extended_rdb_chunk`, §4.4) has explicit
   sections for semantic caches (record type `15`), agent memory sessions (`16`), checkpoints
   (`17`), and tool registries (`18`) — **there is no section, and no record type, for
   `llm_quotas`** anywhere in `shard.rs`. The field exists purely as in-process state.

Concretely: `LLM.QUOTA.RESERVE`/`SETTLE` mutate `ShardDb.llm_quotas` in memory (so admission
decisions are correct for the life of the process) but that state is invisible to AOF, to
replicas, and to RDB snapshots. A restart, a failover to a replica, or a `DEBUG RELOAD` all reset
every quota bucket to empty, silently resetting rate-limit history. For a sliding-window RPM/TPM
governor this is arguably benign (an empty window after a restart just means "no requests counted
yet," not incorrect admission), but it is inconsistent with the other three agent stores, all of
which *do* survive an RDB-covered restart, and a caller relying on `LLM.QUOTA.INFO` reflecting
pre-restart usage will be wrong.

### 4.3 Checkpoint timestamps are computed independently on every executing node

`AgentCheckpointNode.timestamp_ms` (§2.3) is **not** a field of `Command::AgentCheckpointPut` — the
RESP command only carries `key`/`step_id`/`parent_id`/`state`/`meta` (`resp.rs:1365-1371`); the
timestamp is computed inside `AgentCheckpointThread::put` via `SystemTime::now()` at the moment
the shard executes the command (`agent.rs:433-436`). Because AOF replay and replica command
application both re-execute the *original command bytes* (not a snapshot of its result), a replica
applying a propagated `AGENT.CHECKPOINT.PUT` — or a primary replaying its own AOF after
restart — computes its **own** `timestamp_ms` at the moment of replay/application, which will not
match the timestamp the primary originally stored and returned to the client. This is a real,
verifiable non-determinism in the replicated/replayed value of `timestamp_ms` specifically (`seq`
and all other fields replay identically, since they are either literal command arguments or purely
derived from `next_seq`, which is itself deterministically replayed in order).

### 4.4 RDB persistence: record types `15`–`18`, confirmed

`ShardDb::save_rdb_chunk` → `save_extended_rdb_chunk` (the same per-shard function path used by
`SAVE`/`BGSAVE`, `Router::generate_full_rdb` for PSYNC full resync, the cross-shard
`ShardMessage::SaveRdbChunk` responder, and `DEBUG RELOAD` — identical fan-out to the one
documented for CRDT in Component 12 §4) writes, in order:

- **Type `14`** (`shard.rs:2474`): hash-field expirations (`HEXPIRE`) — unrelated to this
  subsystem, included here only to show it sits between the CRDT record (`13`) and the AI-native
  records.
- **Type `15`** (`shard.rs:2540`): semantic cache entries — out of scope for this document
  (`src/semcache.rs`/`SemanticCache`), included for completeness of the numbering.
- **Type `16`** (`shard.rs:2561-2586`): one record per non-empty `agent_memories` session —
  `next_turn_id`/`active_tokens`/`compactions` (3×`u64`) followed by every turn
  (`id`, `role`, `content`, `tokens`, `meta`, `compacted`, and its HNSW vector re-fetched via
  `index.get_vector_cow(turn_id_as_string)` if present, else a `0u32` length prefix for "no
  vector"). Restore (`shard.rs:3140-3185`, record type `16`) rebuilds the session, lazily
  recreating the `HnswIndex` (always `VectorMetric::Cosine`) the first time it encounters a turn
  with a non-empty vector, and re-inserts every vector-bearing turn into it — so a reload fully
  reconstructs both the working-memory list and the episodic HNSW graph.
- **Type `17`** (`shard.rs:2588-2610`): one record per non-empty `agent_checkpoints` thread —
  `next_seq`, optional `head_step_id`, then every node **in `order` (first-seen) sequence**, not
  `seq` order (though for a thread with no overwritten steps the two coincide). Restore
  (`shard.rs:3186-3220`, type `17`) replays nodes back into both `thread.order` and `thread.nodes`
  in the saved sequence.
- **Type `18`** (`shard.rs:2612-2655`): one record per non-empty `agent_tools` registry, but
  **only for calls that are not already fully expired** at save time (`entry.expire_at.is_some()
  && exp <= now` is filtered out before writing, `shard.rs:2624-2627`) — `Instant`-based
  `lease_until`/`expire_at` are each converted to absolute Unix-epoch milliseconds
  (`unix_now + remaining_duration`) since a raw `Instant` cannot survive a process restart.
  Restore (`shard.rs:3221-3260`, type `18`) re-derives fresh `Instant`s by adding
  `(saved_unix_ms - current_unix_now)` to a freshly-captured `Instant::now()`, and **skips any
  entry whose saved expiry is already in the past relative to the restoring node's clock**
  (`shard.rs:3232`) — so a tool lease/result that expired during the time a snapshot sat on disk
  is correctly dropped on reload rather than resurrected with a stale expiry.

So the precise durability picture for the three persisted stores:

- **On-disk restart recovery**: `agent_memories`/`agent_checkpoints`/`agent_tools` all survive a
  clean `SAVE`/`BGSAVE` + restart-from-RDB (subject to the same "AOF is authoritative if enabled"
  startup rule documented for CRDT in Component 12 §4 — if AOF is enabled, the RDB snapshot is not
  consulted at all, and these three stores then rely entirely on AOF's per-command coverage, which
  *is* present for their mutating commands per §4.1, unlike CRDT).
- **Replication**: a replica completing a full PSYNC resync receives a point-in-time copy of all
  three stores (riding inside `generate_full_rdb`, same mechanism as CRDT); after that, ongoing
  writes to `AgentMemAdd`/`Compact`/`Clear`, `AgentCheckpointPut`, and `AgentToolClaim`/`Complete`
  **do** continue to reach the replica via the normal AOF-backed replication backlog (§4.1) —
  unlike CRDT, this is not a one-shot snapshot-only subsystem. The one caveat is §4.3: a
  checkpoint's `timestamp_ms` on the replica will not bit-for-bit match the primary's.
- **`llm_quotas`**: no RDB coverage, no AOF coverage, no replication coverage at all (§4.2) — purely
  transient, process-local state.
- **`src/table.rs`**: no relationship — none of the four stores is a `RudisValue` variant; they
  live entirely in their own `ShardDb` fields and are invisible to `EXPIRE`/`TYPE`/`OBJECT
  ENCODING`/`KEYS`/every other command that walks `RudisTable`, exactly as CRDT keys are
  (Component 12 §4).

---

## Contributor Gotchas & Debugging Guide

* **Gotcha 1**: `LLM.QUOTA.*` state is the *only* one of the four agent stores with no persistence
  or replication path whatsoever (§4.2) — confirmed via `rg -n "Llm" src/aof.rs` (zero hits) and
  by the absence of any `llm_quotas` reference in `save_extended_rdb_chunk`/`restore_rdb_chunk`.
  Do not assume `LLM.QUOTA.INFO` reflects usage from before a restart, failover, or `DEBUG RELOAD`.
* **Gotcha 2**: `AgentCheckpointNode.timestamp_ms` is computed with `SystemTime::now()` *inside*
  `put()` at execution time, not carried as a command argument (§4.3) — it will differ between a
  primary and a replica (or between the original write and an AOF replay) for the same logical
  checkpoint. `seq` and every other field replay deterministically; `timestamp_ms` does not.
* **Gotcha 3**: `AGENT.MEM.ADD`/`COMPACT` hard-code the session's HNSW index to
  `VectorMetric::Cosine` (`agent.rs:80-81`, `198-199`) — there is no way to pick a different metric
  per session, unlike `VADD`.
* **Gotcha 4**: episodic recall's `score` field is `clamp(1 - cosine_distance, 0.0, 1.0)`, and
  cosine distance in `src/vector.rs` is itself already floored at `0.0`
  (`(1.0 - dot/norm).max(0.0)`), so `score == 0.0` is returned for both "orthogonal" and
  "opposite-direction" embeddings — the sign of the original cosine similarity is lost (§3.2).
* **Gotcha 5**: `AgentMemorySession::compact` rebuilds `id_to_pos` from scratch
  (`clear()` + full re-walk of `turns`) on every call (§3.3) — its cost is `O(total_turns)`
  (including already-compacted turns), not `O(turns being compacted)`. A session compacted
  frequently over a very long lifetime pays this full-rebuild cost every time.
* **Gotcha 6**: compaction never removes or prunes a turn's HNSW embedding (§2.3 design doc) —
  there is no size cap, eviction policy, or TTL on a session's episodic index anywhere in
  `agent.rs`. The only way to bound or reclaim a session's memory (working *or* episodic) is
  `AGENT.MEM.CLEAR`, which deletes the entire session (and its `HnswIndex`) outright — there is no
  partial/selective eviction.
* **Gotcha 7**: `AgentToolRegistry` has no `DEL`/`CLEAR`/`EXPIRE`-style command at all — a
  `COMPLETE`d entry with no `TTL` argument is cached forever (§3.9); the only ways it leaves
  `calls` are an explicit future `COMPLETE ... TTL` lapsing, or `FLUSHDB`.
* **Gotcha 8**: re-`PUT`ting an existing `step_id` in a checkpoint thread overwrites that node in
  place and moves `head_step_id` back to it, without creating a new `order` entry or a structurally
  distinguishable "new" node (§2.3 design doc, §3.7) — `AGENT.CHECKPOINT.HISTORY` from the new head
  will show the overwritten node's *current* contents, not its prior contents; nothing in
  `agent.rs` preserves the node's pre-overwrite state.
* **Gotcha 9**: `AgentToolRegistry::claim`'s expired-completed-result branch deletes the entry
  outright and restarts `attempt` at `1` (§3.8) — an expired-but-previously-completed call's retry
  history (its `attempt` count) is lost, unlike an expired-*lease* (never-completed) retry, which
  preserves and increments `attempt`.
* **Gotcha 10**: `estimate_tokens` (`agent.rs:59-62`) is `content.len().div_ceil(4).max(1)` — a
  fixed 4-bytes-per-token heuristic with no awareness of the content's actual script/language; it
  is only used when a caller omits the `TOKENS` option on `AGENT.MEM.ADD`/`COMPACT`.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run the module's own unit tests (working-window/recall/compaction, RPM/TPM reserve/settle,
#    checkpoint DAG lineage + tool lease idempotency)
cargo test --lib agent:: -- --test-threads=1

# 4. Run the AOF round-trip test that exercises Agent Memory/Checkpoint/Tool persistence together
cargo test --lib aof:: -- --test-threads=1
```
