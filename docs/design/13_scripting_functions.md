# Component 13: Lua Scripting & Redis 7 Functions Engine (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/scripting.rs`  
> **Implementation Reference**: [`docs/internal/13_scripting_functions.md`](../internal/13_scripting_functions.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Traditional in-memory datastores encounter severe scalability barriers on modern multi-core, high-throughput cloud hardware. Single-threaded architectures (such as Redis) saturate a single CPU core while leaving the remaining 95%+ of server cores idle. Multi-threaded mutex architectures (such as Memcached) suffer from heavy spinlock contention, CPU cache line bouncing, and global memory allocator lock bottlenecks.

### 1.2 The Rudis Solution
Rudis implements the **Thread-Per-Core (Shared-Nothing)** architectural paradigm natively on Linux `io_uring` via Monoio. Each physical CPU core owns its own isolated event loop, its own thread-local memory database, and its own kernel `SO_REUSEPORT` listener. Operations on local keys execute in nanoseconds with zero locks, zero atomic operations, and zero cross-core cache invalidations.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model
Embedded Lua 5.4 runtime via mlua. Supports transient scripts (EVAL, EVALSHA) and persistent Redis 7 function libraries (FUNCTION LOAD, FCALL) with sandboxed standard library.

### 2.2 Design Rationale (The "Why")
Atomic multi-operation transactions and server-side business logic require script execution without client round-trips. Redis 7 functions provide first-class, versioned library management.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
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

## 3. High-Level Architecture & Workflow Diagram

```
Client ──► FCALL my_lib:my_func ──► Lua 5.4 VM (mlua) ──► redis.call() ──► ShardDb
```

---

## 4. Performance Guarantees & Theoretical Complexity

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

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/13_scripting_functions.md`**](../internal/13_scripting_functions.md): Low-level implementation and code reference.
* **Source Files**: `src/scripting.rs`
