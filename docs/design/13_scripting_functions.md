# Component 13: Lua Scripting & Redis 7 Functions Engine — Design & Architecture

> **Subsystem Scope**: `src/scripting.rs`
> **Implementation Reference**: [`docs/internal/13_scripting_functions.md`](../internal/13_scripting_functions.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Purpose

`src/scripting.rs` embeds a Lua 5.4 interpreter (via the `mlua` crate, vendored) to support two
Redis-compatible server-side execution mechanisms: transient scripts (`EVAL`, `EVALSHA`,
`SCRIPT LOAD`/`EXISTS`/`FLUSH`) and Redis 7 function libraries (`FUNCTION LOAD`, `FCALL`,
`FUNCTION LIST`/`DELETE`/`FLUSH`). Both let a client execute a sequence of Redis commands as one
atomic unit of server-side logic, with a `redis.call`/`redis.pcall` bridge back into the real
command-execution path.

## 2. Design Rationale ("Why")

### 2.1 Why server-side scripting at all

Some operations are naturally a small transaction over several keys — read-modify-write
patterns, conditional updates, or custom aggregation — that would otherwise require multiple
round trips between client and server, with the associated latency and the risk that another
client's write interleaves between them. `EVAL`/`FCALL` let that logic run as Lua source
co-located with the data, executing entirely within one shard's event loop with no network hop
per embedded `redis.call`. Because the calling connection's shard processes one command (or
squashed pipeline) at a time on its single-threaded event loop, a script's sequence of
`redis.call`s is naturally atomic relative to any other traffic that shard is handling: nothing
else can execute against that shard's keyspace while the script's Lua code is running.

### 2.2 Why Lua specifically

Lua is the same scripting language upstream Redis chose for `EVAL`, and for largely the same
reasons: a small, fast, embeddable interpreter with a well-understood C API (here, `mlua`'s safe
Rust bindings to it), a simple value model that maps cleanly onto RESP types (tables, strings,
numbers, booleans), and a large body of existing Redis Lua scripts and tooling that a compatible
implementation benefits from being able to run largely unmodified.

### 2.3 Why Redis 7 Functions in addition to `EVAL`

`EVAL` scripts are anonymous and re-sent by the client on every call unless the client tracks
SHA1 hashes itself; there is no server-side concept of a named, versioned, persistently
registered piece of logic. Redis 7's Functions API addresses this by letting an operator load a
named library once (`FUNCTION LOAD`) and have clients invoke individual functions from it by
name (`FCALL`) without resending source on every call — closer to a stored-procedure model than
`EVAL`'s "send source, get SHA1 back" workflow.

### 2.3a Long-running scripts: `busy-reply-threshold`, `SCRIPT KILL`, `FUNCTION KILL`

A script runs synchronously on its shard's thread, so that shard serves nothing else until it
returns. Every running script is registered (`RUNNING_SCRIPTS` in `src/scripting.rs`) and runs
under a global Lua count hook (every 10,000 instructions, inherited by coroutines):

- Past `busy-reply-threshold` (alias `lua-time-limit`, default 5000 ms, 0 = never) the script
  is marked busy and a "Slow script detected" warning is logged. Commands then get
  `-BUSY ... You can only call SCRIPT KILL or SHUTDOWN NOSAVE.` (`FUNCTION KILL` for an
  `FCALL`) on every *other* shard, except the commands Redis flags `allow-busy`. A command
  that needs the busy shard (its key lives there) can instead wait for the script to end.
- `SCRIPT KILL` (for `EVAL`/`EVALSHA`) and `FUNCTION KILL` (for `FCALL`) set the script's kill
  flag. At its next check the hook fails the script with `ERR Script killed by user with ...`,
  and from then on fires on every line so a `pcall` loop can't catch the error and keep going.
  A script that has already run a write command can't be killed (`-UNKILLABLE`, as in Redis).
  Using the wrong kill command gets `-BUSY`, and `-NOTBUSY` means no script is running.
- Shutdown (`SHUTDOWN`, `SIGTERM`) ends a running script the same way, so the shard can drain.

Difference from Redis: the busy shard's own connections are not served while the script runs,
including a `SCRIPT KILL` sent on one of them. With several shards a new connection usually
lands on another shard, and the kill reaches the script from there. With `--threads 1`, the way
to stop a runaway script is to restart the server (`SIGTERM` ends the script, as above).

### 2.4 Sandboxing and isolation model — stated honestly

**Rudis's Lua environment is not sandboxed.** Each `eval_script`/`call_function` invocation
creates a fresh `mlua::Lua::new()` instance and does not strip, replace, or restrict any part of
the default Lua 5.4 standard library — there is no removal of `os`, `io`, `package`, or
`debug`. This is a materially different posture from upstream Redis, which
maintains an explicit Lua sandbox (stripping filesystem/OS access, limiting available globals)
specifically because `EVAL` is reachable by any client with command access. Until this changes,
`EVAL`/`EVALSHA`/`FCALL` should be treated as requiring the same trust level as direct shell
access to the host process — appropriate for an admin/trusted-operator feature, not for
exposing to arbitrary or lower-trust clients. What *is* isolated is state between calls: because
a brand-new interpreter is constructed and torn down for every invocation, there is no
possibility of one script's global variables or state leaking into another's.

### 2.5 What server-side execution buys, and what it costs

- **Buys**: atomicity relative to other shard traffic without an explicit transaction API, and
  (for Functions) a persistent, named alternative to resending script source per call.
  `redis.call`'s argument and reply conversion round-trips through the exact same command
  parsing (`build_command`) and execution path (`execute_local_command`) every ordinary command
  uses — a script is not a separate, parallel command-processing path with its own semantics.
- **Costs**: `EVAL`/`EVALSHA`/`FCALL` always execute against the local shard of whichever
  connection issued them — there is no routing based on the command's declared `KEYS` at all
  (unlike ordinary single-key commands, which are routed to the shard that owns the key). Every
  `redis.call`/`redis.pcall` inside the script executes against that same local shard's keyspace
  with no cross-shard redirection. Consequently, a script only behaves correctly if every key it
  touches — `KEYS` arguments and anything else it happens to read or write — actually belongs to
  the shard the connection is attached to; a script that touches a key belonging to a different
  shard silently reads or writes that *other* key's slot in the *local* shard's keyspace, which
  is simply the wrong data, not an error. Client-side routing (choosing which node/connection to
  send an `EVAL` to based on its keys, the same discipline Redis Cluster requires of `EVAL`
  callers generally) is required for correct behavior in a multi-shard deployment; the server
  does not enforce or correct for it.

## 3. Architecture Overview

```
Client ──► FCALL my_lib my_func 1 mykey arg1 ──► fresh mlua::Lua VM
                                                        │
                                          redis.call('SET', KEYS[1], ARGV[1])
                                                        │
                                     crate::connection::execute_local_command
                                          (same path every ordinary command uses)
                                                        │
                                                shard's ShardDb (local only)
```

## 4. Key Invariants

1. **A fresh interpreter per call; no persistent VM or precompiled bytecode cache.** Both
   `eval_script` and `call_function` construct a new `Lua` instance and let it drop at the end of
   the call — there is no VM pooling and no cached compiled chunk. What is cached is the
   script's **source text**, keyed by its SHA1 digest.
2. **No sandboxing (§2.4).** This is a deliberate note, not an oversight to hide: document and
   operate scripting accordingly until a sandboxing posture is added.
3. **Script execution is atomic relative to the local shard, not the whole cluster.** Atomicity
   is a natural consequence of the shard's single-threaded execution model, not an explicit lock
   or transaction scripting.rs adds — and it only extends to the one shard the script's `EVAL`/
   `FCALL` was routed to.
4. **Script and function caches are process-wide, not per-shard.** `SCRIPT_CACHE` and
   `FUNCTION_LIBS` are global, so a script or library loaded via a connection on one shard is
   immediately `EVALSHA`/`FCALL`-able from a connection on any other shard — a deliberate
   departure from the rest of the codebase's thread-local, shared-nothing design, justified
   because script/function *definitions* are metadata shared cluster-wide by nature, unlike the
   keyspace itself.

## 5. Implementation Reference

For concrete struct definitions, the `redis.call`/`redis.pcall` bridge implementation, the
RESP-to-Lua value conversion rules, and the two-pass `FUNCTION LOAD`/`FCALL` execution model, see
[`docs/internal/13_scripting_functions.md`](../internal/13_scripting_functions.md).
