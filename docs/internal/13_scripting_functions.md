# Component 13: Lua Scripting & Redis 7 Functions Engine — Implementation Reference

> **Source Files**: `src/scripting.rs`
> **High-Level Design Spec**: [`docs/design/13_scripting_functions.md`](../design/13_scripting_functions.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Module Responsibilities

| File | Responsibility |
| :--- | :--- |
| `src/scripting.rs` | `SCRIPT_CACHE` (source-by-SHA1), `FUNCTION_LIBS` (loaded libraries), `eval_script`, `load_function`/`call_function`, RESP⇄Lua value conversion. |
| `src/connection.rs` | Dispatches `Command::Eval`/`Evalsha`/`ScriptLoad`/`ScriptExists`/`ScriptFlush`/`FunctionLoad`/`Fcall`/`FunctionList`/`Delete`/`Flush`; supplies `write_resp_integer`/`write_resp_bulk` and `execute_local_command`, the single function both `redis.call` and every ordinary command dispatch path funnel through. |
| `src/resp.rs` | Parses `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`, `FCALL` into the matching `Command` variants. |

---

## 2. Data Structures (verbatim from `src/scripting.rs`)

```rust
static SCRIPT_CACHE: LazyLock<RwLock<HashMap<String, String>>> = LazyLock::new(|| RwLock::new(HashMap::new()));
static FUNCTION_LIBS: LazyLock<RwLock<HashMap<String, FunctionLib>>> = LazyLock::new(|| RwLock::new(HashMap::new()));

#[derive(Clone, Debug)]
pub struct FunctionLib {
    pub name: String,
    pub engine: String,     // always "LUA" — no other engine is registered anywhere
    pub raw_code: String,   // the library's full source, with any "#!" shebang line commented out
    pub functions: Vec<String>,  // names registered via redis.register_function during a discovery pass
}
```

`SCRIPT_CACHE` maps SHA1 hex digest to raw script **source text**, not compiled bytecode — the
source is re-parsed by `mlua` on every `EVAL`/`EVALSHA` call, since a fresh `Lua` VM is
constructed per call (§3.1). `FunctionLib` has no persisted, ready-to-call closure; it records
only which top-level function names a library's source registers, discovered by running the
library once with a stubbed `redis.register_function` (§3.4). Both statics are process-wide
(`LazyLock<RwLock<..>>`), not per-shard.

---

## 3. Execution Algorithms

### 3.1 `EVAL`/`EVALSHA`: local-shard-only execution, no key-based routing

```rust
Command::Eval { script, keys, args } => {
    crate::scripting::load_script(&script);                 // caches source by its own SHA1
    crate::scripting::eval_script(&script_str, &keys, &args, &router.local_db, router.aof.as_deref())
}
Command::Evalsha { sha, keys, args } => {
    let script = crate::scripting::get_script(&sha_str).ok_or("NOSCRIPT ...")?;
    crate::scripting::eval_script(&script, &keys, &args, &router.local_db, router.aof.as_deref())
}
```

`router.local_db` is always this connection's **local shard's** `ShardDb`. Unlike ordinary
single-key commands (which are routed to the shard owning the key via `target_shard_of_cmd` —
see Component 04), `Command::Eval`/`Evalsha`/`Fcall` are dispatched at their own match arms with
**no key-based routing check at all**; verified directly against `src/connection.rs` (`Eval`/
`Evalsha`/`Fcall` do not appear among the commands routed through `target_shard_of_cmd`). A
script always runs against whichever shard the issuing connection happens to be attached to,
regardless of what its `KEYS` argument names — correctness in a multi-shard deployment depends
entirely on the caller having already chosen a connection to the shard that owns the relevant
keys (see the design doc's §2.5 for the consequence of getting this wrong).

Every `EVAL` (not only an explicit `SCRIPT LOAD`) unconditionally caches its own source under
its SHA1 via `load_script`, so a script becomes `EVALSHA`-able the moment it is first `EVAL`'d,
matching real Redis semantics without a separate mandatory load step.

### 3.2 `redis.call`/`redis.pcall`: real command execution, not a separate scripting table

```rust
let call_fn = lua.create_function(move |lua, margs: MultiValue| {
    let cmd_args = /* MultiValue -> Vec<Bytes>: String/Integer/Number/Boolean/Nil each converted */;
    let cmd = crate::resp::build_command(cmd_args)?;         // same parser real wire commands use
    let mut out = Vec::new();
    crate::connection::execute_local_command(&cmd, &mut db_call.borrow_mut(), &mut out, aof_ref);
    resp_bytes_to_lua(lua, &out)
})?;
```

`redis.call`'s Lua arguments are converted to `Bytes` and handed to the exact same
`build_command` (Component 03) and `execute_local_command` (Component 02) that parse and execute
commands arriving over the wire — a script is not a separate command-processing path; it issues
real `Command` values against the real `ShardDb`. Because `execute_local_command` is also the
function that appends to the AOF and propagates to connected replicas for an ordinary command
(the `record_change!` macro defined inside it), **each write a script performs via `redis.call`
is independently AOF-logged and replicated as its own constituent command** — i.e., Rudis
propagates a script's *effects* (the individual commands it issued), not the script's source
text, matching modern Redis's default script-replication behavior. `redis.pcall` is identical
except it intercepts a RESP error reply (`out.starts_with(b"-")`) and returns a Lua table
`{err = "..."}` instead of raising a Lua error — matching Redis's `call` (raises) vs. `pcall`
(returns an error table) distinction.

The `aof: Option<*const RefCell<AofWriter>>` capture is a raw pointer specifically so the
closure can satisfy `mlua::Lua::create_function`'s `'static` bound while still referencing a
`RefCell` borrowed from the caller's stack; `unsafe { aof.map(|ptr| &*ptr) }` reconstitutes the
reference at call time. This is sound only because the `Lua` instance (and every closure it
holds) is dropped before `eval_script`/`call_function` returns and the borrow ends — a manual
invariant enforced by the function's structure, not by the type system.

`redis.status_reply(msg)`/`redis.error_reply(msg)` build `{ok = msg}`/`{err = msg}` tables
directly; `redis.sha1hex(s)` calls the same `sha1_hex` helper used for script cache keys.

### 3.3 RESP ⇄ Lua value conversion

`resp_bytes_to_lua(lua, out: &[u8]) -> mlua::Result<Value>` converts a raw RESP reply into a Lua
value following the standard Redis Lua conversion rules: `+OK\r\n` → `{ok = "OK"}` table;
`-ERR ...\r\n` → a **Lua error** (not a returned value — this is what makes a failing
`redis.call` raise, which `redis.pcall` intercepts one level up); `:N\r\n` → integer; `$-1\r\n`
(null bulk) → `false`; a bulk string → a Lua string; `*N\r\n...` → recursively parsed via
`parse_resp_array_to_lua`, which itself handles nested `$`/`:`/`+`/`*` elements.

`lua_val_to_resp(&Value, &mut Vec<u8>)` is the reverse mapping, applied once to a script's
**final** return value: `nil` → `$-1`; `true` → `:1`; `false` → `$-1` (Redis's
false-means-nil-reply convention); a table with an `"ok"` key → `+...`; a table with an `"err"`
key → `-...`; any other table → treated as a 1-indexed array and serialized via `t.raw_len()`.

### 3.4 `FUNCTION LOAD`/`FCALL`: two-pass execution, no persistent function objects

```rust
pub fn load_function(code: &str, replace: bool) -> Result<String, String> {
    // parse "#!lua name=<lib>" shebang (or a "-- name=<lib>" comment) for the library name
    let lua = Lua::new();                                   // discovery-only VM
    // redis.register_function stubbed to just record the function NAME, discarding the closure
    lua.load(&lua_code).exec()?;                             // runs the library's top-level code once
    FUNCTION_LIBS.write().unwrap().insert(lib_name.clone(), FunctionLib { name, engine: "LUA", raw_code, functions });
    Ok(lib_name)
}
```

`FUNCTION LOAD` runs the library's entire top-level source **once**, purely to discover which
function names it registers; the actual Lua closures produced during this pass are discarded.
`FCALL` then re-runs the **entire library source again**, in a second, fresh `Lua::new()`, this
time with a real `redis.register_function` that captures the one closure matching the requested
function name into the Lua registry (`create_registry_value`) and invokes it with `(KEYS,
ARGV)`. Net effect: a library's top-level code executes once at `FUNCTION LOAD` (name discovery)
and once **per `FCALL` call** (to obtain a callable closure) — there is no cached, ready-to-
invoke function object retained between calls. `FCALL` passes `router.aof.as_deref()` through to
`call_function` exactly as `EVAL`/`EVALSHA` do, so writes performed via `FCALL` are AOF-logged
and replicated the same way (verified: `Command::Fcall`'s handler in `connection.rs` threads
`router.aof.as_deref()` into `call_function`, and `test_fcall_with_aof_writer` in
`scripting.rs` asserts the resulting AOF buffer contains the expected written command).
`Command::Fcall`'s handler additionally calls `notify_key_invalidation` for each declared `KEYS`
entry after a successful call (RESP3 client-side-caching invalidation, Component 02) — `EVAL`/
`EVALSHA` do not do this explicitly at the same call site, relying instead on whatever
invalidation `execute_local_command` performs internally for each individual `redis.call`.

---

## 4. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): owns all `SCRIPT_CACHE`/`FUNCTION_LIBS` mutation entry
  points and supplies `execute_local_command`, the shared execution path for both `redis.call`
  and ordinary command dispatch.
- **`src/resp.rs`** (Component 03): `build_command` is invoked from inside the `redis.call`/
  `redis.pcall` closures to turn Lua-supplied arguments back into a real `Command`.
- **`src/shard.rs`/`src/table.rs`** (Components 04/05): mutated whenever a script's `redis.call`
  executes a write, exactly as an ordinary client-issued command would — always against the
  local shard (§3.1).
- **`src/aof.rs`/`src/replication.rs`**: every write a script performs is independently
  AOF-appended and replica-propagated as its own constituent command, via the same
  `record_change!` path ordinary commands use inside `execute_local_command` (§3.2).

---

## Contributor Gotchas & Debugging Guide

* **Gotcha 1**: `EVAL`/`EVALSHA`/`FCALL` never route based on `KEYS` — they always run on the
  connection's local shard. Sending a script to the wrong shard does not fail; it silently reads
  or writes the wrong shard's data for any key that does not actually belong there.
* **Gotcha 2**: there is no sandboxing — a script has full access to Lua's standard library
  (`os`, `io`, `debug`, etc.) via `mlua::Lua::new()`'s defaults, and no execution-time or
  instruction-count limit exists.
* **Gotcha 3**: `FCALL` re-executes a function library's entire top-level source on every call
  (to re-discover the requested closure) — library-level initialization code runs once per
  `FCALL`, not once at load time.
* **Gotcha 4**: a script's writes are replicated/AOF-logged as their individual constituent
  commands (effects replication), not as the script's source text.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib scripting -- --test-threads=1
```
