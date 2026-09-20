# Component 13: Lua Scripting & Redis 7 Functions Engine (Design)

## Component 13: Lua Scripting & Redis 7 Functions Engine

> **Source Files**: ``src/scripting.rs``


---

### 1. Architectural Purpose & Scope

`src/scripting.rs` embeds Lua via `mlua` (`lua54`, vendored) to run `EVAL`/`EVALSHA`/`SCRIPT
LOAD`/`SCRIPT EXISTS`/`SCRIPT FLUSH` and Redis 7 Functions (`FUNCTION LOAD`, `FCALL`,
`FUNCTION LIST`, `FUNCTION DELETE`, `FUNCTION FLUSH`). There is no persistent `ScriptEngine`
struct — every `EVAL`/`EVALSHA`/`FCALL` call creates a **brand-new `mlua::Lua` instance**,
runs once, and drops it. Script *source* is cached (by SHA1, and by function-library name);
compiled bytecode and the Lua VM itself are not.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Fresh interpreter per call, no persistent VM or bytecode cache.** `eval_script` and
   `call_function` both call `Lua::new()` at the top and let it drop at the end of the
   function. There is nothing analogous to the old doc's `ScriptEngine`/`script_cache:
   HashMap<String, Vec<u8>>` (compiled bytecode) — what's cached is the **script's source
   text**, keyed by its SHA1 hex digest, in a global `RwLock<HashMap<String, String>>`.
2. **No sandboxing.** Grepping the file for `io`/`os`/`debug`/`package` global stripping
   finds nothing — `mlua::Lua::new()` is used unmodified, so a script has full access to
   whatever the default Lua 5.4 standard library exposes (the old doc's "Security
   Sandboxing" invariant does not exist in the real code).
3. **Deterministic execution w.r.t. the shard, by construction, not by an explicit lock.**
   Because `redis.call`/`redis.pcall` synchronously call `crate::connection::
   execute_local_command` against the same `Rc<RefCell<ShardDb>>` the calling connection
   task already holds, and Rudis's per-core execution model means nothing else touches
   that `RefCell` concurrently, a script's commands execute atomically relative to other
   traffic on the shard for free — not because scripting.rs added any synchronization of
   its own.
4. **Global, cross-shard-visible caches.** Both `SCRIPT_CACHE` (source by SHA1) and
   `FUNCTION_LIBS` (function libraries) are `static LazyLock<RwLock<HashMap<...>>>` —
   process-wide, not per-shard. A script loaded via `SCRIPT LOAD`/`FUNCTION LOAD` on one
   shard's connection is immediately visible to `EVALSHA`/`FCALL` calls arriving on any
   other shard, because it's the same global map — this is a deliberate (if easy-to-miss)
   departure from the rest of the codebase's thread-local, shared-nothing design.

---

---

### 6. Performance Characteristics

- **No bytecode caching, despite the SHA1 cache's name.** `SCRIPT_CACHE` only saves
  re-transmission of the script *text* for `EVALSHA`; Lua source is re-parsed by `mlua` on
  every single `EVAL`/`EVALSHA` call, and a fresh `Lua::new()` VM is constructed and torn down
  per call — there is no persistent interpreter or precompiled-chunk reuse the old doc's
  "Bytecode Caching" section claimed.
- **In-process, zero-IPC command execution**: real and accurate from the old doc — `redis.call`
  invokes `execute_local_command` directly against the shard's own `Rc<RefCell<ShardDb>>`,
  with no network or channel hop, since scripts only ever run against the local shard.
- **Process-wide global locks on every script/function load or lookup**: `SCRIPT_CACHE`/
  `FUNCTION_LIBS` are `RwLock`s taken on every `EVAL` (write lock, unconditionally, via
  `load_script`), `EVALSHA` (read lock), and `FCALL` (read lock) — a real (if narrow and
  presumably low-contention) departure from the rest of the codebase's lock-free, thread-local
  design, shared with the `BlockHub` exception documented in Component 06/01.

---
