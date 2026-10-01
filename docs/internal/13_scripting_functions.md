# Component 13: Lua Scripting & Redis 7 Functions Engine — Implementation Reference

> **Source Files**: `src/scripting.rs` (739 lines)
> **High-Level Design Spec**: [`docs/design/13_scripting_functions.md`](../design/13_scripting_functions.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Module Inventory

`src/scripting.rs` in full — every symbol it defines (there are no `enum`/`trait`/`impl` blocks
in this file; it is a flat collection of statics and free functions plus one plain struct):

| Kind | Item | Line | Notes |
| :--- | :--- | :-: | :--- |
| static | `SCRIPT_CACHE: LazyLock<RwLock<HashMap<String, String>>>` | 9 | SHA1 hex → raw script source |
| fn | `sha1_hex(data: &[u8]) -> String` | 12 | hex-encodes a `sha1::Sha1` digest |
| fn | `load_script(script: &[u8]) -> String` | 24 | inserts into `SCRIPT_CACHE`, returns SHA1 |
| fn | `get_script(sha: &str) -> Option<String>` | 34 | lower-cases `sha` before lookup |
| fn | `script_exists(shas: &[Bytes]) -> Vec<bool>` | 39 | |
| fn | `flush_scripts()` | 49 | |
| fn | `eval_script(script_content, keys, args, db, aof) -> Result<Vec<u8>, String>` | 53 | `EVAL`/`EVALSHA` engine, §3.1 |
| fn | `resp_bytes_to_lua(lua, out: &[u8]) -> mlua::Result<Value>` | 214 | private; RESP→Lua top-level dispatch |
| fn | `parse_resp_array_to_lua(lua, buf: &mut BytesMut) -> mlua::Result<Value>` | 262 | private; recursive `*N` array parser |
| fn | `lua_val_to_resp(val, out: &mut Vec<u8>) -> Result<(), String>` | 342 | private; Lua→RESP, applied once to a script's final return value |
| struct | `FunctionLib { name, engine, raw_code, functions }` | 394 | `#[derive(Clone, Debug)]` |
| static | `FUNCTION_LIBS: LazyLock<RwLock<HashMap<String, FunctionLib>>>` | 401 | library name → `FunctionLib` |
| fn | `load_function(code: &str, replace: bool) -> Result<String, String>` | 405 | `FUNCTION LOAD` engine, §3.4 |
| fn | `call_function(func_name, keys, args, db, aof) -> Result<Vec<u8>, String>` | 481 | `FCALL` engine, §3.4 |
| fn | `list_functions() -> Vec<FunctionLib>` | 597 | clones every entry out of `FUNCTION_LIBS` |
| fn | `delete_function(lib_name: &str) -> bool` | 603 | |
| fn | `flush_functions()` | 608 | |

`#[cfg(test)] mod tests` occupies lines 612–739 (three tests: basic EVAL/`redis.call` type
coverage, `EVALSHA`/`FUNCTION` management round-trip, and `FCALL` mutating the AOF — see §6).

**mlua**: `Cargo.toml` pins `mlua = { version = "0.12.1", features = ["lua54", "vendored"] }` —
real Lua 5.4 compiled from vendored C source (no system Lua dependency), synchronous API only
(no `async`/`send` feature enabled, consistent with the thread-per-core, no-cross-thread-Send
architecture). `Lua::new()` is called with no feature flags restricting the standard library —
see Gotcha 2.

Both statics are **process-wide** `LazyLock<RwLock<..>>`, not per-shard: `SCRIPT_CACHE` and
`FUNCTION_LIBS` are shared across every shard/thread in the process, so a script loaded via
`SCRIPT LOAD`/`EVAL` on one shard's connection is immediately `EVALSHA`-able from a connection
attached to a different shard, and likewise a `FUNCTION LOAD`ed library is callable from any
shard. This is unlike almost everything else in Rudis (Components 04/05), which is strictly
sharded with no implicit cross-shard state. The *execution* of a script, however, is always
local to one shard's `ShardDb` (§3.1) — only the cached source/library text is global.

`SCRIPT_CACHE` maps SHA1 to raw script **source text**, not compiled bytecode — the source is
re-parsed by `mlua` on every `EVAL`/`EVALSHA` call, since a fresh `Lua::new()` VM is constructed
per call. `FunctionLib` holds no persisted, ready-to-call closure; it records only which
top-level function names a library's source registers, discovered by running the library once
with a stubbed `redis.register_function` (§3.4).

---

## 2. `FunctionLib` Struct (verbatim, lines 394–399)

```rust
#[derive(Clone, Debug)]
pub struct FunctionLib {
    pub name: String,
    pub engine: String,     // always "LUA" — no other engine is ever registered
    pub raw_code: String,   // library source with any "#!" shebang line rewritten to "--" comment
    pub functions: Vec<String>, // function names discovered via a stubbed redis.register_function
}
```

There is no field for a compiled/cached closure — every `FCALL` re-executes `raw_code` from
scratch (§3.4, Gotcha 3).

---

## 3. Execution Algorithms

### 3.1 `EVAL`/`EVALSHA`: key-based cross-shard forwarding, then local execution

`Command::Eval`/`Command::Evalsha` are handled inside the giant async `execute_command` match in
`src/connection.rs` (that match body spans lines 4906–12337; the `Eval` arm is at line 11146, the
`Evalsha` arm at line 11174) — **not** inside `execute_local_command`. Each arm now does its own
explicit first-key routing check:

```rust
Command::Eval { script, keys, args } => {
    if let Some(first_key) = keys.first() {
        let target = router.target_shard(first_key);
        if target != router.shard_id {
            let res = router.execute_remote(target, Command::Eval { script, keys, args }).await;
            out.extend_from_slice(&res);
            return false;
        }
    }
    let script_str = String::from_utf8_lossy(&script);
    crate::scripting::load_script(&script);
    match crate::scripting::eval_script(&script_str, &keys, &args, &router.local_db, router.aof.as_deref()) {
        Ok(resp) => out.extend_from_slice(&resp),
        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
    }
    false
}
```

`Evalsha` (line 11174) is byte-for-byte the same shape, substituting `get_script(&sha_str)` for
the raw `script`. **This is a behavior change from earlier revisions of this document**: `EVAL`
and `EVALSHA` now consult `KEYS[1]` via `router.target_shard(first_key)` and forward the entire
command to the owning shard over the cross-shard mailbox (`execute_remote`, Component 04) when
the first declared key does not belong to the local shard. If `keys` is empty, no forwarding
check runs and the script executes on whichever shard the connection happens to be attached to.

`target_shard_of_cmd` (`src/connection.rs:12338`) — the function that decides whether a command
gets pre-routed to a different shard *before* even reaching this match arm (used by
`server.rs:289`, `router.rs:3160`, `connection.rs:7397`) — still has **no** arm for
`Command::Eval`/`Evalsha`/`Fcall` (verified: grepping its full body, lines 12338–12691, and the
sibling `target_shard_and_hash_of_cmd`, lines 12692–12723, finds no match arm naming any of the
three). So the only key-based routing scripts get is the ad-hoc check written inline in the
`Eval`/`Evalsha` arms themselves — pre-dispatch routing infrastructure does not know about
scripts at all.

**`Fcall` has no equivalent check** (§3.4) — `FCALL` always executes on the connection's local
shard regardless of its `KEYS` argument, even though `EVAL`/`EVALSHA` now forward. This is a
real, verified asymmetry in the current source, not an oversight documented elsewhere: `Command::
Fcall`'s handler (`connection.rs:11306`) calls `crate::scripting::call_function` directly against
`router.local_db` with no `router.target_shard`/`execute_remote` step at all.

Every `EVAL` (not only an explicit `SCRIPT LOAD`) unconditionally caches its own source under its
SHA1 via `load_script`, so a script becomes `EVALSHA`-able the moment it is first `EVAL`'d,
matching real Redis semantics without a separate mandatory load step.

### 3.2 `redis.call`/`redis.pcall`: real command execution, not a separate scripting table

Inside `eval_script` (`scripting.rs:87-169`), both closures:

1. Convert the Lua `MultiValue` argument list to `Vec<Bytes>` — `Value::String`→raw bytes,
   `Value::Integer`→decimal string bytes, `Value::Number`→decimal string bytes, `Value::Boolean`→
   `b"1"`/`b"0"`, `Value::Nil`→empty `Bytes`; any other Lua value type is silently **dropped**
   from the argument list (the `_ => {}` arm at line 97/134 — a table or function argument to
   `redis.call` vanishes rather than erroring).
2. Feed the resulting `Vec<Bytes>` to `crate::resp::build_command` (Component 03) — the exact
   same parser real wire commands use.
3. Run the resulting `Command` through `crate::connection::execute_local_command` (Component 02)
   against `db_call.borrow_mut()` (the caller's `Rc<RefCell<ShardDb>>`).
4. Convert the raw RESP `out: Vec<u8>` back to a Lua `Value` via `resp_bytes_to_lua`.

`redis.call`/`redis.pcall` therefore issue real `Command` values against the real `ShardDb` —
scripting is not a separate command-processing path. Because `execute_local_command` is the same
function that appends to the AOF and propagates to replicas for an ordinary command (the
`record_change!` macro defined at the top of `execute_local_command`, `connection.rs:12740`),
**each write a script performs via `redis.call` is independently AOF-logged and replicated as its
own constituent command** — Rudis propagates a script's *effects*, not its source text, matching
modern Redis's default script-replication mode. `redis.pcall` differs only in that it intercepts
a RESP error reply (`out.starts_with(b"-")`, line 158) and returns a Lua table `{err = "..."}`
instead of raising a Lua error; `redis.call`'s equivalent error path instead returns the error
straight from `execute_local_command`'s `out` into `resp_bytes_to_lua`, which turns a leading `-`
byte into a genuine Lua error (`mlua::Error::RuntimeError`, `scripting.rs:229`) — matching Redis's
`call` (raises) vs. `pcall` (returns an error table) distinction.

The `aof: Option<*const RefCell<AofWriter>>` capture (`aof_call`/`aof_pcall`, lines 86/121 and
523) is a raw pointer specifically so the closure can satisfy `mlua::Lua::create_function`'s
`'static` bound while still referencing a `RefCell` borrowed from the caller's stack;
`unsafe { aof_call.map(|ptr| &*ptr) }` reconstitutes the reference at call time. This is sound
only because the `Lua` instance (and every closure it holds) is dropped before `eval_script`/
`call_function` returns and the borrow ends — a manual invariant enforced by function structure,
not the type system.

`redis.status_reply(msg)`/`redis.error_reply(msg)` build `{ok = msg}`/`{err = msg}` tables
directly (lines 171–191); `redis.sha1hex(s)` calls the same `sha1_hex` helper used for script
cache keys (line 193). **That is the complete Redis API surface exposed to scripts** —
`redis.call`, `redis.pcall`, `redis.status_reply`, `redis.error_reply`, `redis.sha1hex`, plus
(inside `load_function`/`call_function` only) `redis.register_function`. There is no
`redis.log`, `redis.setresp`, `redis.breakpoint`, `redis.debug`, `redis.replicate_commands`, or
`redis.set_repl` — none of these identifiers appear anywhere in `src/scripting.rs`. A script that
calls any of them gets a plain Lua "attempt to call a nil value" runtime error.

### 3.3 RESP ⇄ Lua value conversion

`resp_bytes_to_lua(lua, out: &[u8]) -> mlua::Result<Value>` (line 214) switches on `out[0]`:

| RESP prefix | Lua result |
| :-- | :-- |
| (empty `out`) | `Value::Nil` |
| `+...\r\n` | table `{ok = "..."}` |
| `-...\r\n` | **Lua error** (`mlua::Error::RuntimeError`) — this is what makes a failing `redis.call` raise; `redis.pcall` intercepts one level up before this function is reached |
| `:N\r\n` | `Value::Integer` (falls back to `0` if the digits don't parse) |
| `$-1\r\n` | `Value::Boolean(false)` |
| `$N\r\n...` | `Value::String` (falls back to an empty Lua string if the length framing is inconsistent) |
| `*N\r\n...` | delegated to `parse_resp_array_to_lua` |
| anything else | `Value::Nil` |

`parse_resp_array_to_lua` (line 262) is a **hand-rolled, recursive** RESP array parser operating
on a `bytes::BytesMut` cursor (distinct from — and simpler than — the two-pass parser in
`src/resp.rs`, Component 03): it walks `$`/`:`/`+`/`*` element prefixes one at a time, advancing
the cursor with `Buf::advance`; a negative `$` length becomes `Value::Boolean(false)` (null bulk
inside an array); an unrecognized element prefix `break`s out of the loop early, silently
truncating the resulting Lua table short of `count` elements rather than erroring.

`lua_val_to_resp(&Value, &mut Vec<u8>)` (line 342) is the reverse mapping, applied **once**, to a
script's final return value only (not to values returned mid-script):

| Lua value | RESP written |
| :-- | :-- |
| `nil` | `$-1\r\n` |
| `true` | `:1\r\n` (via `write_resp_integer`, i.e. `:1\r\n`) |
| `false` | `$-1\r\n` (Redis's false-means-nil-reply convention) |
| `Integer`/`Number` | `:N\r\n` (a `Number` is truncated to `i64` via `as i64` — no fractional part is preserved) |
| `String` | bulk string via `write_resp_bulk` |
| `Table` with an `"ok"` string field | `+...\r\n` |
| `Table` with an `"err"` string field | `-...\r\n` |
| other `Table` | treated as a 1-indexed array, length from `t.raw_len()`, each element recursively converted |
| any other `Value` variant (e.g. `Function`, `Thread`, `UserData`) | `$-1\r\n` (silently coerced to nil) |

### 3.4 `FUNCTION LOAD`/`FCALL`: two-pass execution, no persistent function objects

```rust
pub fn load_function(code: &str, replace: bool) -> Result<String, String> {
    // parse library name from a "#!lua name=<lib>" shebang or a "-- ... name=<lib>" line
    // (default "default_lib" if no name= is found anywhere)
    // reject if FUNCTION_LIBS already has lib_name and !replace
    let lua = Lua::new();                     // discovery-only VM
    // redis.register_function(name, fn) stubbed to push `name` into a Vec and discard `fn`
    lua.load(&lua_code).exec()?;              // runs the library's top-level code once
    FUNCTION_LIBS.write().unwrap().insert(lib_name.clone(), FunctionLib { name, engine: "LUA", raw_code, functions });
    Ok(lib_name)
}
```

Library-name parsing (lines 408–424) scans every line of `code` for one starting with `#!lua` or
`--`, then looks for a `name=` substring within it; the first match wins and the loop `break`s.
If no line matches, the library name defaults to the literal string `"default_lib"`. Before
executing anything, any line whose trimmed text starts with `#!` is rewritten to be prefixed with
`--` (turning the shebang into a Lua comment, lines 452–462) — this rewritten `lua_code` is what
gets stored as `raw_code` and is what's actually `lua.load()`ed both here and in `call_function`.

`FUNCTION LOAD` runs the library's entire top-level source **once**, purely to discover which
function names it registers via a stubbed `redis.register_function` that records the name and
discards the closure (lines 439–447); the closures produced during this pass are thrown away
entirely.

```rust
pub fn call_function(func_name, keys, args, db, aof) -> Result<Vec<u8>, String> {
    let lib = /* find the FunctionLib whose .functions contains func_name, or "ERR Function '..' not found" */;
    let lua = Lua::new();                      // a second, fresh VM
    // redis.call bound exactly as in eval_script (§3.2)
    // redis.register_function(name, f) captures f into the Lua registry ONLY if name == func_name
    lua.load(&lib.raw_code).exec()?;           // re-runs the ENTIRE library source again
    let f: mlua::Function = lua.registry_value(&fn_key)?;   // the one closure that was captured
    let res: Value = f.call((keys_tbl, argv_tbl))?;
    lua_val_to_resp(&res, &mut out)?;
    Ok(out)
}
```

`FCALL` re-runs the **entire library source again**, in a second, fresh `Lua::new()` VM, this
time with a real `redis.register_function` that captures the one closure matching the requested
function name into the Lua registry (`create_registry_value`, line 560) and invokes it with
`(KEYS, ARGV)` as two Lua tables (not spread arguments). Any other function names the library
registers on this pass are matched against `target_name` and discarded if they don't equal it
(the `if name == target_name` guard at line 559) — so a library with 5 functions still re-executes
all of its top-level code on every single `FCALL` to any one of them, but only the requested
closure survives. If the requested function name was never captured (e.g. the library's top-level
code is conditional and didn't call `register_function` for it this time), `call_function` fails
with `"ERR Function '{}' registered but failed to capture"`.

**Net effect**: a library's top-level code executes once at `FUNCTION LOAD` (name discovery, using
a throwaway VM and a throwaway closure) and once **per `FCALL` call** (to obtain a callable
closure, using another throwaway VM for everything except the one target closure) — there is no
cached, ready-to-invoke function object retained between calls (Gotcha 3).

`FCALL` passes `router.aof.as_deref()` through to `call_function` exactly as `EVAL`/`EVALSHA` pass
it to `eval_script`, so writes performed via `FCALL` are AOF-logged and replicated the same way
(verified: `Command::Fcall`'s handler in `connection.rs:11306` threads `router.aof.as_deref()`
into `call_function`, and `test_fcall_with_aof_writer`, `scripting.rs:703-738`, asserts the
resulting in-memory AOF buffer contains the constituent `SET` command, key, and value).
`Command::Fcall`'s handler additionally calls `notify_key_invalidation` (`connection.rs:713`) for
each declared `KEYS` entry after a successful call — RESP3 client-side-caching invalidation
(Component 02). `EVAL`/`EVALSHA` do not do this explicitly at their own call sites; they rely on
whatever invalidation `execute_local_command` performs internally for each individual
`redis.call`.

---

## 4. `FUNCTION`/`SCRIPT` Subcommand Coverage — what actually exists

Parsed in `src/resp.rs`: `EVAL`/`EVAL_RO` (lines 5657–5679), `EVALSHA`/`EVALSHA_RO` (5680–5702),
`SCRIPT LOAD`/`EXISTS`/`FLUSH` (5703–5731), `FUNCTION LOAD`/`LIST`/`FLUSH`/`STATS`/`KILL`/`DELETE`
(10154–10192), `FCALL`/`FCALL_RO` (10193–10212). `EVAL_RO`/`EVALSHA_RO`/`FCALL_RO` parse to the
exact same `Command::Eval`/`Evalsha`/`Fcall` variants as their non-`_RO` counterparts — there is
no enforcement anywhere that a `_RO` variant's script is actually read-only; `redis.call('SET',
...)` succeeds identically inside an `EVAL_RO` script (verified: no `_ro`/readonly flag exists on
`Command::Eval`/`Evalsha`/`Fcall`, and neither `eval_script` nor `call_function` takes one).

`FUNCTION LOAD` accepts an optional `REPLACE` keyword as `args[2]` (case-insensitive), shifting
the code argument from index 2 to 3 (`resp.rs:10166-10174`). Any other `FUNCTION` subcommand falls
through to `Command::Unknown(format!("FUNCTION {}", sub))` (line 10190).

**`FUNCTION DUMP` and `FUNCTION RESTORE` do not exist anywhere in this codebase** — verified by
grepping `FunctionDump`/`FunctionRestore` across `src/resp.rs` and `src/connection.rs`: zero
matches. There is no `Command::FunctionDump`/`FunctionRestore` variant, no RESP subcommand
parsing for `DUMP`/`RESTORE` under `FUNCTION`, and consequently no RDB/AOF persistence of loaded
function libraries at all — `FUNCTION_LIBS` is purely in-memory and is lost on restart, with no
serialization format defined anywhere for it (contrast `CrdtDump`/`CrdtMerge`, Component 12, which
do have a real wire dump format).

`FUNCTION STATS` (handled identically in two places — `connection.rs:11362` inside
`execute_command`, and a byte-for-byte duplicate at `connection.rs:18817` inside
`execute_local_command`) reports `running_script: nil` unconditionally — there is no tracking of
an in-flight script anywhere in `scripting.rs`, so this field can never be non-nil. It reports
`engines.LUA.libraries_count`/`functions_count` computed live from `list_functions()`.

`FUNCTION KILL` (also duplicated at `connection.rs:11391` and `19033`) unconditionally replies
`+OK\r\n` — there is no script-execution-in-progress state to check, so it never returns real
Redis's `NOTBUSY No scripts in execution right now.` error, and it never actually kills anything
(consistent with there being no timeout/interrupt mechanism at all — §5).

---

## 5. Execution Context Split: `execute_command` vs. `execute_local_command`

This is the most consequential structural fact about scripting dispatch and is easy to miss on a
partial read, since the two functions are ~6,300 lines apart in the same file.

* **`execute_command`** (`src/connection.rs:4906`, `async fn`, the per-connection top-level
  dispatcher) has match arms for `Command::Eval`, `Evalsha`, `ScriptLoad`, `ScriptExists`,
  `ScriptFlush`, `FunctionLoad`, `Fcall`, `FunctionList`, `FunctionDelete`, `FunctionFlush`,
  `FunctionStats`, and `FunctionKill` (lines 11146–11394). This is the **only** place `EVAL`,
  `EVALSHA`, `SCRIPT *`, `FUNCTION LOAD`, `FCALL`, `FUNCTION LIST`, `FUNCTION DELETE`, and
  `FUNCTION FLUSH` are handled.
* **`execute_local_command`** (`src/connection.rs:12734`, synchronous `fn`, called from inside
  `redis.call`/`redis.pcall`, from AOF replay, and from cross-shard remote-command execution) has
  match arms for only two of these: `FunctionStats` (line 18817) and `FunctionKill` (line 19033),
  both textually identical to their `execute_command` counterparts. Every command not explicitly
  matched falls through to `execute_local_command`'s final catch-all arm, `_ => false`
  (`connection.rs:19041`), which writes **nothing** to `out` and returns `false` (not an error).

**Consequence (verified, not inferred)**: a script that calls `redis.call('EVAL', ...)`,
`redis.call('EVALSHA', ...)`, `redis.call('FCALL', ...)`, `redis.call('SCRIPT', 'LOAD', ...)`, or
`redis.call('FUNCTION', 'LOAD', ...)` — i.e., any attempt at nested/recursive scripting from
inside a running script — has its inner command parsed successfully by `build_command` (all five
are valid `Command` variants) but then silently hits `execute_local_command`'s `_ => false` arm.
`out` stays empty, so `resp_bytes_to_lua(lua, &[])` returns `Value::Nil` (its documented empty-
input case, line 215–217) with **no error raised at all**. From the calling script's perspective,
`redis.call('EVAL', "return 1", "0")` just silently returns `nil` instead of `1` — a
correctness bug, not merely a missing feature, because no error surfaces to warn the script
author.

---

## 6. Concrete Numbers, Limits, and What's Absent

* No execution timeout: no instruction-count budget, no wall-clock deadline, no
  `lua-time-limit`-equivalent configuration key is read anywhere in `src/scripting.rs`, and
  `FUNCTION KILL` cannot interrupt a running script (it doesn't even track one — §4). An infinite
  Lua loop (`while true do end`) blocks the entire shard's reactor thread forever, since script
  execution is fully synchronous on the calling shard.
* No sandboxing: `Lua::new()` (used identically at `scripting.rs:60`, `434`, `498`) is `mlua`'s
  default full-library constructor — scripts have unrestricted access to Lua's standard library
  (`os`, `io`, `debug`, etc.), including `os.execute`, arbitrary file I/O via `io.open`, and so on.
  There is no `sandbox` mode enabled and no custom globals table stripping.
  (Gotcha 2)
- The Redis-facing API surface is exactly 6 functions total: `redis.call`, `redis.pcall`,
  `redis.status_reply`, `redis.error_reply`, `redis.sha1hex` (always available), plus
  `redis.register_function` (only inside `load_function`/`call_function`'s bespoke `redis`
  tables — not present during ordinary `EVAL`/`EVALSHA`, so calling `redis.register_function`
  from a plain `EVAL` script raises a nil-call error).
* `SCRIPT_CACHE`/`FUNCTION_LIBS` have no eviction policy and no size cap — entries live until
  `SCRIPT FLUSH`/`FUNCTION FLUSH`/`FUNCTION DELETE`/process restart.
* `EVAL`/`EVALSHA`/`FCALL` each rebuild `KEYS`/`ARGV` Lua tables and call
  `redis.create_function` 3–5 times per invocation — there is no VM pooling; every call pays the
  cost of `Lua::new()` plus re-`load()`ing the full script/library source (no bytecode caching —
  §1).
* The official Redis TCL test suites `tests/redis-tests/unit/scripting.tcl` and
  `tests/redis-tests/unit/functions.tcl` are vendored in this repo but are **not** listed in
  `scripts/run_redis_test_suite.sh`'s suite set — they are not run against Rudis as part of the
  compatibility gate that other subsystems use (verified: grepping `scripting\|functions` against
  that script finds nothing). Coverage for this subsystem instead comes from the three unit tests
  in `scripting.rs:612-739` and the E2E tests in `tests/test_server_e2e.rs`
  (`test_lua_scripting_engine_e2e`, line 3253; `test_fcall_mutating_aof_persistence_and_replay_e2e`,
  line 5619).

---

## 7. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): owns all `SCRIPT_CACHE`/`FUNCTION_LIBS` mutation entry
  points (inside `execute_command` only, §5); supplies `execute_local_command` for `redis.call`/
  `redis.pcall`, `write_resp_integer`/`write_resp_bulk`/`write_resp_err` for RESP serialization,
  and `notify_key_invalidation` for `FCALL`'s post-call cache invalidation.
- **`src/resp.rs`** (Component 03): `build_command` is invoked from inside the `redis.call`/
  `redis.pcall` closures to turn Lua-supplied arguments back into a real `Command`; also owns all
  `EVAL`/`EVALSHA`/`SCRIPT`/`FUNCTION`/`FCALL` wire parsing (§4).
- **`src/router.rs`/`src/shard.rs`** (Component 04): `router.target_shard`/`router.execute_remote`
  are now consulted by `EVAL`/`EVALSHA` for first-key cross-shard forwarding (§3.1); `FCALL` does
  not consult them. `src/table.rs` (Component 05) is mutated whenever a script's `redis.call`
  performs a write, exactly as an ordinary client-issued command would.
- **`src/aof.rs`/`src/replication.rs`**: every write a script performs is independently
  AOF-appended and replica-propagated as its own constituent command, via the `record_change!`
  macro inside `execute_local_command` (§3.2) — the script's own source text is never itself
  written to the AOF or replication stream.

---

## Contributor Gotchas & Debugging Guide

* **Gotcha 1 (routing asymmetry)**: `EVAL`/`EVALSHA` now forward to the shard owning `KEYS[1]`
  when that differs from the connection's local shard (`connection.rs:11146-11203`); `FCALL` does
  **not** — it always runs on the connection's local shard regardless of `KEYS`
  (`connection.rs:11306-11328`). A script reached via `FCALL` silently reads/writes the wrong
  shard's data for any key that doesn't belong to the connection's local shard; the equivalent
  `EVAL`/`EVALSHA` call would instead transparently forward. `target_shard_of_cmd`
  (`connection.rs:12338`), the general pre-dispatch shard router, has no awareness of any of the
  three commands — the `EVAL`/`EVALSHA` forwarding is a one-off check written directly into their
  own match arms.
* **Gotcha 2**: there is no sandboxing — a script has full access to Lua's standard library
  (`os`, `io`, `debug`, etc.) via `mlua::Lua::new()`'s defaults, and no execution-time or
  instruction-count limit exists anywhere (§6). `FUNCTION KILL` always replies `+OK` and kills
  nothing.
* **Gotcha 3**: `FCALL` re-executes a function library's entire top-level source on every call
  (to re-discover/re-capture the requested closure) — library-level initialization code runs once
  per `FCALL`, not once at load time (§3.4).
* **Gotcha 4**: a script's writes are replicated/AOF-logged as their individual constituent
  commands (effects replication), not as the script's source text.
* **Gotcha 5 (nested scripting silently no-ops)**: `redis.call`/`redis.pcall` run through
  `execute_local_command`, which has no match arm for `Eval`/`Evalsha`/`Fcall`/`ScriptLoad`/
  `FunctionLoad` (only `FunctionStats`/`FunctionKill` are duplicated there). A script that issues
  any of the other five via `redis.call` gets a silent Lua `nil` back with no error — see §5.
* **Gotcha 6**: `FUNCTION DUMP`/`FUNCTION RESTORE` do not exist in this codebase at all (§4);
  `FUNCTION_LIBS` has no persistence — loaded libraries are lost on every restart.
* **Gotcha 7**: `EVAL_RO`/`EVALSHA_RO`/`FCALL_RO` are not actually read-only-enforced — they parse
  to the same `Command` variants as their mutating counterparts and `redis.call('SET', ...)`
  succeeds identically inside them (§4).

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib scripting -- --test-threads=1

# 4. Run the scripting/functions E2E tests
cargo test --test test_server_e2e test_lua_scripting_engine_e2e -- --test-threads=1
cargo test --test test_server_e2e test_fcall_mutating_aof_persistence_and_replay_e2e -- --test-threads=1
```
