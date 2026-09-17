# Code Review: Correctness Bugs, Performance Issues, and Cleanup

**Date:** 2026-09-16
**Scope:** `src/`, focused review at max effort via the `/code-review` skill, independently
re-verified here (every finding below was re-checked with `grep`/direct source reads before
being included — none are taken on the reviewing agent's word alone).

## Why this review is scoped the way it is

There was no pending diff to review (the working tree was clean at review time — the last
commits had all landed already), so a literal "review everything" pass isn't the highest-value
use of review effort: the bulk of `src/`'s known architectural gaps, dead-code findings, and
design trade-offs are **already cataloged** in [`docs/components.md`](components.md)'s 19
per-component "Future Improvements" sections, each one individually verified against source
during that documentation effort. Re-deriving that list here would just duplicate it.

Instead, this review targets the highest-risk, least-reviewed code: the three most recent
performance-focused commits (`e518cf2`, `64b7efb`, `cca3f29` — "thread-local command stats",
"direct socket send / zero-alloc command tracking", "cache-line mailboxes / scatter-gather
descriptors"), which touched `src/connection.rs`, `src/router.rs`, `src/server.rs`,
`src/shard.rs`, `src/tiering.rs`, and added a new file, `src/mailbox.rs`. New performance work
touching the hot request path is exactly where a regression is most likely to hide, and least
likely to have been caught by the broad architectural documentation pass (which read the
codebase for what it *does*, not for what changed most recently or most riskily).

**For the rest of the codebase's known gaps** — the >64-shard MGET/MSET bug, the kTLS
plaintext bug, ACL's weak password hashing, the O(N) GEORADIUS scans, missing RDB persistence
for JSON/Probabilistic/Vector state, and everything else already found — see each component's
own "Future Improvements" section in `docs/components.md`. This document does not repeat those.

---

## Summary

| # | Severity | Area | One-line summary |
| :---: | :--- | :--- | :--- |
| 1 | **Critical (correctness)** | `connection.rs` | RESP3 protocol mode leaks across unrelated clients sharing a shard thread |
| 2 | High (correctness) | `connection.rs` | Blocking commands never update `CLIENT LIST`'s `last_active`/`last_cmd` |
| 3 | Medium (correctness) | `connection.rs` | `CONFIG RESETSTAT`/`INFO commandstats` only touch the calling shard's stats buffer |
| 4 | Medium (perf, silent) | `mailbox.rs` | `CachePadded<T>` — the mechanism the commit claims to add — is defined but never used |
| 5 | Low (latent) | `tiering.rs` / `router.rs` | A cached `Arc<TieringStats>` can be orphaned by `reset_tier_stats` (currently unreachable, but live if wired up) |
| 6 | Low (cleanup) | `router.rs` | Three channel-pool fields and four methods are dead code after a rewrite |
| 7 | Medium (perf) | `server.rs` | Cold-tiering decision logic is copy-pasted across 4 call sites (tripled by this change) |
| 8 | Medium (perf, self-defeating) | `server.rs` | Cold-path reads take 3 `RwLock`s per call instead of using the zero-lock cached field this same change introduced |
| 9 | Trivial | `connection.rs` | A dead no-op `if buf.is_empty() { buf.clear(); }` |
| 10 | Process gap | `mailbox.rs` | New `unsafe` scatter-gather code shipped with no unit or integration tests |

**Fix first:** #1. It's a live, reachable data-corruption bug on any deployment with more than
one connected client per shard where at least one uses RESP3 — not a theoretical edge case.

---

## Findings

### 1. CRITICAL — RESP3 protocol mode leaks across clients sharing a shard thread

**File:** `src/connection.rs` · **Introduced by:** `e518cf2` ("thread-local command stats")

Every shard is a single-threaded `monoio` runtime serving many client connections as
cooperative tasks (`server.rs`'s accept loop spawns one task per connection, Component 01).
Reply formatting (RESP2 vs. RESP3) is controlled by a **thread-local**, `CURRENT_CLIENT_RESP3`,
read at serialization time. Before `e518cf2`, this thread-local was resynced from the
*currently executing* client's own `is_resp3` flag at the top of every command dispatch. That
resync was removed in `e518cf2` and never replaced — verified directly:

```
$ grep -n "CURRENT_CLIENT_RESP3.set" src/connection.rs
5800:                CURRENT_CLIENT_RESP3.set(true);
5806:                CURRENT_CLIENT_RESP3.set(false);
5823:            CURRENT_CLIENT_RESP3.set(false);
```

All three call sites are inside the `HELLO`/`RESET` command handlers — nowhere else in the
file sets this thread-local. Confirmed via `git log -S` that a `CURRENT_CLIENT_RESP3.set(is_resp3)`
call existed at the dispatch entry point as of `b422b5c` and was removed by `e518cf2`: a real
regression, not a pre-existing gap.

**Failure scenario:** Client A sends `HELLO 3` (sets the thread-local `true` for its shard's
thread). Client B, a plain RESP2 client connected to the *same shard* (a near-certainty once a
shard has more than one connection — `SO_REUSEPORT` balancing doesn't guarantee protocol
homogeneity), then runs any command. Nothing resets the thread-local to B's own `is_resp3`
(`false`) before B's command's reply is serialized, so B silently receives RESP3-formatted
replies (maps, booleans, doubles, attribute frames) on a connection that never negotiated
RESP3 — a real client library will fail to parse the reply, or misinterpret it.

**Fix:** restore the per-command resync — read the executing client's `is_resp3` from
`client_registry` and call `CURRENT_CLIENT_RESP3.set(...)` at the top of dispatch (both the
single-command and squashed-batch paths), before executing the command, not only inside
`HELLO`/`RESET`.

---

### 2. HIGH — Blocking commands never refresh `CLIENT LIST`/`CLIENT INFO`'s activity fields

**File:** `src/connection.rs` (blocking-command dispatch branch, `connection.rs` §4.3 in
`docs/components.md`) · **Introduced by:** `e518cf2`

The `last_active`/`last_cmd` update used to live inside `execute_command` itself, firing
uniformly for every call site. `e518cf2` removed that single shared update and replaced it
with manual, per-branch updates in the transaction branch, the single-command branch, and
`execute_commands_squashed` — but the blocking-commands branch (`BLPOP`/`BRPOP`/`BLMOVE`/
`BLMPOP`/`BZPOPMIN`/`BZPOPMAX`/`BZMPOP`/blocking `XREAD`/`XREADGROUP`, Component 06 §4.3) calls
`execute_command` directly with no equivalent replacement.

**Failure scenario:** A client sits blocked in `BLPOP` for its full timeout. `CLIENT LIST`/
`CLIENT INFO` for that connection shows the *previous* command and a stale `idle=`/`age=`
figure for the entire blocking duration, instead of reflecting that the client is actively
waiting — an operational-visibility regression (an operator watching `CLIENT LIST` to diagnose
a stuck-looking connection sees misleading data), not a data-correctness bug.

**Fix:** add the same `client_registry.borrow_mut().get_mut(&client_id)` update (now
duplicated per-branch by design, per `e518cf2`) to the blocking-commands branch too.

---

### 3. MEDIUM — `CONFIG RESETSTAT`/`INFO commandstats` only see the calling shard's buffer

**File:** `src/connection.rs` · **Introduced by:** `e518cf2` ("thread-local command stats")

Command statistics are now buffered per-shard in a thread-local (`LOCAL_CMD_STATS`, up to 1023
buffered calls) before periodically flushing into the process-wide `CMD_STATS` map — a real,
reasonable optimization to avoid a global lock on every command. But `CONFIG RESETSTAT` clears
`CMD_STATS` and only the *calling* shard's local buffer; `INFO commandstats` reads only
`CMD_STATS`.

**Failure scenario:** With multiple shards, `CONFIG RESETSTAT` issued on shard 0's connection
clears the global map and shard 0's buffer, but shards 1..N's buffers still hold pre-reset
counts. When those buffers next flush (on hitting the 1024-call threshold, or on connection
teardown), the "reset" stats silently reappear. Symmetrically, `INFO commandstats` called
before every shard's buffer has flushed under-reports real command volume.

**Fix:** either broadcast a flush/reset `ShardMessage` to every shard on `CONFIG RESETSTAT`
(mirroring the fan-out pattern already used for `PUBLISH`/`CLIENT LIST`, Components 04/19), or
accept the staleness window explicitly and document it in `INFO`'s own output.

---

### 4. MEDIUM (silent) — `CachePadded<T>` is defined but never applied

**File:** `src/mailbox.rs` · **Introduced by:** `cca3f29` ("cache-line mailboxes and
scatter-gather shared memory descriptors")

```
$ grep -n "CachePadded" src/mailbox.rs
7:pub struct CachePadded<T>(pub T);
9:impl<T> std::ops::Deref for CachePadded<T> {
17:impl<T> std::ops::DerefMut for CachePadded<T> {
```

`CachePadded` — the type the commit message's "direct cache-line mailboxes" refers to — is
defined with `Deref`/`DerefMut` impls and then **never wraps any field** of
`FastGetDescriptor`/`FastSetDescriptor`/`ScatterMgetDescriptor`/`ScatterMsetDescriptor`.

**Failure scenario:** During a cross-shard scatter `MGET`, different shard threads on
different physical cores concurrently write results into adjacent indices of a shared
`Box<[UnsafeCell<Option<Bytes>>]>` with no per-slot padding. Adjacent indices commonly land on
the same 64-byte cache line, so concurrent cross-core writes cause false sharing — cache-line
ping-pong between cores — which is precisely the class of cost `CachePadded` exists to prevent.
The type compiles and is exported; nothing signals that it isn't doing anything.

**Fix:** either wrap the actual per-slot result cells in `CachePadded<UnsafeCell<...>>` (the
straightforward completion of the stated intent), or, if profiling shows padding isn't worth
the memory overhead at realistic descriptor sizes, remove the unused type rather than leaving
dead infrastructure that looks load-bearing.

---

### 5. LOW (latent) — a cached `Arc<TieringStats>` can be silently orphaned

**File:** `src/router.rs` (`tier_stats` field) / `src/tiering.rs` (`reset_tier_stats`)

`Router` caches one `Arc<TieringStats>` clone at construction time as a hot-path shortcut
(avoiding a `RwLock`-guarded global map lookup per access — see Finding 8 for where that intent
is currently defeated anyway). `crate::tiering::reset_tier_stats(port)` removes that port's
entry from the global map entirely.

**Failure scenario (currently unreachable — `reset_tier_stats` has zero call sites today, but
this is exactly the kind of latent bug that ships live the moment someone wires it up, e.g. to
a future `CONFIG SET` or a cluster-slot-migration reset path):** any `Router` holding the old
cached `Arc` keeps observing stale `max_memory`/threshold values indefinitely, while any other
call site that goes through `crate::tiering::get_max_memory(port)` fresh sees the new value —
a real inconsistency between "routers that cached the Arc early" and "everything else."

**Fix:** either make `reset_tier_stats` update the existing `Arc`'s contents in place
(`Arc<RwLock<TieringStatsInner>>` or similar) rather than replacing the map entry, or have
`Router` re-fetch from the map instead of caching indefinitely, or — simplest — leave a comment
at `reset_tier_stats`'s definition flagging this hazard for whoever wires up its first caller.

---

### 6. LOW (cleanup) — dead channel-pool fields and methods

**File:** `src/router.rs`

`mget_channel_pool`, `mset_channel_pool`, `set_channel_pool` and their
`acquire_mget_channels`/`release_mget_channels`/`acquire_mset_channels`/`release_mset_channels`
methods have zero call sites anywhere outside their own declarations (and their own
now-orphaned unit tests), confirmed by grep across `src/*.rs`. These predate the `mailbox.rs`
rewrite of `get`/`set`/`mget`/`mset` onto the new descriptor-based scatter-gather path and were
never removed. `set_channel_pool` doesn't even have an accessor method left.

**Fix:** delete the fields, their accessor methods, and their unit tests — per `agent.md`'s own
"no dead code" rule.

---

### 7. MEDIUM (perf/maintainability) — cold-tiering decision logic quadruplicated

**File:** `src/server.rs`

The decision of whether a key is a cold-tiering candidate (checking `max_memory`,
`offload_threshold_pct`, a per-shard threshold, and an `is_constrained` flag) is copied
verbatim across four call sites: the new `FastGet` path, the legacy `Mget` path, and both
branches of the new `ScatterMget` handling. This was already duplicated before `cca3f29`; this
change added two more copies, tripling the total.

**Fix:** factor the shared decision into one function (`fn is_cold_tiering_candidate(port,
key_size_or_whatever_inputs) -> bool` or similar) and call it from all four sites — a future
change to the constrained-memory formula currently has to be applied identically four times by
hand, and a missed copy silently keeps stale logic live on that one path.

---

### 8. MEDIUM (perf, self-defeating) — cold-path reads pay 3 locks the same commit's own caching was meant to avoid

**File:** `src/server.rs`, using fields added to `src/router.rs` by the same commit

```
$ grep -n "get_max_memory(r.port)\|get_offload_threshold_pct(r.port)\|get_tier_stats(r.port)" src/server.rs
240:  let max_mem = crate::tiering::get_max_memory(r.port);
241:  let offload_pct = crate::tiering::get_offload_threshold_pct(r.port);
253:      let stats = crate::tiering::get_tier_stats(r.port);
301:  let max_mem = crate::tiering::get_max_memory(r.port);
302:  let offload_pct = crate::tiering::get_offload_threshold_pct(r.port);
314:      let stats = crate::tiering::get_tier_stats(r.port);
```

`Router.tier_stats: Arc<TieringStats>` (added in `e518cf2`, `router.rs:86`) exists specifically
to let call sites read `max_memory`/`offload_threshold_pct`/`upload_threshold_pct` without a
`RwLock`-guarded global-map lookup. These four cold-path call sites in `server.rs` don't use
it — they call the three global lookup functions fresh every time, each one acquiring its own
`RwLock` read guard, on every cold/tiered key access.

**Fix:** replace these six call sites with reads of `r.tier_stats.max_memory`/
`.offload_threshold_pct`/etc. directly — zero-lock, and consistent with why the field exists.
(Doing so also mechanically fixes Finding 7's duplication, since the four call sites already
need to change together.)

---

### 9. TRIVIAL — a dead no-op

**File:** `src/connection.rs` (post-parse-loop, added by `64b7efb`)

```rust
if buf.is_empty() { buf.clear(); }
```

Clearing an already-empty buffer changes nothing. Harmless, but it's a branch evaluated on
every read iteration where the parse loop fully drained the buffer, for zero effect.

**Fix:** delete the line.

---

### 10. PROCESS GAP — new `unsafe` code shipped with no tests

**File:** `src/mailbox.rs`

No unit tests exist in `src/mailbox.rs` itself (the module defines `unsafe impl Send`/`Sync`
and does raw `UnsafeCell` writes across the new descriptor types), and neither `64b7efb` nor
`cca3f29` — the two commits that introduced and then built on this module — added anything
under `tests/` (`git show --stat <sha> -- tests/` is empty for both). `agent.md`'s own stated
rule requires both a unit test and an integration test for every change; this is the one place
in the reviewed range where that rule visibly wasn't followed, and it's the highest-risk kind
of code (raw pointers, manual `Send`/`Sync`, cross-thread shared-memory writes) to leave
uncovered — Finding 4's false-sharing gap and any future soundness issue in this module have no
regression test to catch them.

**Fix:** add unit tests for the descriptor types' basic read/write correctness in
`src/mailbox.rs`, and an end-to-end test in `tests/` exercising a real cross-shard scattered
`MGET`/`MSET` over actual TCP sockets (per the pattern `tests/test_server_e2e.rs` already
uses elsewhere).

---

## Recommended fix order

1. **#1** (RESP3 leak) — ship this fix alone, immediately; it's a live correctness bug.
2. **#8 + #7 together** (they touch the same four call sites) — real perf win, removes
   duplication at the same time.
3. **#2, #3** — both are observability/correctness gaps in code paths `e518cf2` touched;
   low-risk, contained fixes.
4. **#6, #9** — pure deletions, zero risk, do whenever convenient.
5. **#4** — decide (pad the real hot fields, or delete the unused type) rather than leave it
   ambiguous.
6. **#10** — add coverage for `mailbox.rs` before it grows further; the sooner, the cheaper.
7. **#5** — leave a comment now; fix for real only when `reset_tier_stats` gets its first
   caller.
