# Component 13: Lua Scripting & Redis 7 Functions Engine (`src/scripting.rs`)

## 1. Architectural Purpose & Scope

`src/scripting.rs` embeds Lua via `mlua` (`lua54`, vendored) to run `EVAL`/`EVALSHA`/`SCRIPT
LOAD`/`SCRIPT EXISTS`/`SCRIPT FLUSH` and Redis 7 Functions (`FUNCTION LOAD`, `FCALL`,
`FUNCTION LIST`, `FUNCTION DELETE`, `FUNCTION FLUSH`). There is no persistent `ScriptEngine`
struct — every `EVAL`/`EVALSHA`/`FCALL` call creates a **brand-new `mlua::Lua` instance**,
runs once, and drops it. Script *source* is cached (by SHA1, and by function-library name);
compiled bytecode and the Lua VM itself are not.

---

## 2. Key Invariants & Concurrency Constraints

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

## 3. Component Architecture & Data Structures

```
  EVAL "return redis.call('get', KEYS[1])" 1 mykey
                         │
                         ▼
        connection.rs: load_script(&script) → SHA1 into SCRIPT_CACHE
                         │
                         ▼
              eval_script(script_str, keys, args, db, aof)
                         │
                         ▼
                 Lua::new()  (brand-new VM, every call)
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   KEYS table        ARGV table      redis.{call,pcall,
   (1-indexed)        (1-indexed)     status_reply,error_reply,
                                       sha1hex} bound as closures
        │                │                │
        └────────────────┴────────────────┘
                         ▼
              lua.load(script_content).eval()
                         │
              redis.call inside script synchronously invokes
              execute_local_command(&cmd, &mut db.borrow_mut(), &mut out, aof_ref)
                         │
                         ▼
              resp_bytes_to_lua(): raw RESP reply bytes → mlua::Value
                         │
                         ▼
              lua_val_to_resp(): script's final Lua return value → RESP bytes
```

### The real caches (there is no `ScriptEngine`/`FunctionDef` struct)

```rust
static SCRIPT_CACHE: LazyLock<RwLock<HashMap<String, String>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

static FUNCTION_LIBS: LazyLock<RwLock<HashMap<String, FunctionLib>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// A registered Redis 7 Function Library
#[derive(Clone, Debug)]
pub struct FunctionLib {
    pub name: String,
    pub engine: String,
    pub raw_code: String,
    pub functions: Vec<String>,
}
```

`SCRIPT_CACHE` maps SHA1 hex → raw script **source** (not bytecode — `eval_script` re-parses
the source with `lua.load(script_content)` on every single call, since the VM itself is
recreated each time). `FunctionLib` has no `read_only`/`description` fields the old doc
claimed; it just tracks which top-level function names a library registered.

---

## 4. Execution Algorithms & Code Logic

### 4.1 `EVAL`/`EVALSHA`: `connection.rs` owns caching, `scripting.rs` owns execution

```rust
Command::Eval { script, keys, args } => {
    let script_str = String::from_utf8_lossy(&script);
    crate::scripting::load_script(&script);              // cache source by its own SHA1
    match crate::scripting::eval_script(&script_str, &keys, &args, &router.local_db, router.aof.as_deref()) {
        Ok(resp) => out.extend_from_slice(&resp),
        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
    }
}
Command::Evalsha { sha, keys, args } => {
    let sha_str = String::from_utf8_lossy(&sha);
    if let Some(script) = crate::scripting::get_script(&sha_str) {
        match crate::scripting::eval_script(&script, &keys, &args, &router.local_db, router.aof.as_deref()) { ... }
    } else {
        out.extend_from_slice(b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
    }
}
```

Every `EVAL` (not just `SCRIPT LOAD`) unconditionally caches its own source under its SHA1
via `load_script`, so a script becomes `EVALSHA`-able the moment it's first `EVAL`'d — matching
real Redis semantics — without a separate explicit-load step being required. `EVAL`'s AOF
propagation is real: `router.aof.as_deref()` is threaded through so writes a script performs
via `redis.call` get appended, same as an ordinary command.

### 4.2 `redis.call`/`redis.pcall`: a Lua closure that round-trips through the real RESP path

```rust
let call_fn = lua.create_function(move |lua, margs: MultiValue| {
    let mut cmd_args = Vec::with_capacity(margs.len());
    for v in margs {
        match v {
            Value::String(s) => cmd_args.push(Bytes::copy_from_slice(&s.as_bytes())),
            Value::Integer(i) => cmd_args.push(Bytes::from(i.to_string())),
            Value::Number(n) => cmd_args.push(Bytes::from(n.to_string())),
            Value::Boolean(b) => cmd_args.push(Bytes::from(if b { "1" } else { "0" })),
            Value::Nil => cmd_args.push(Bytes::new()),
            _ => {}
        }
    }
    let cmd = match crate::resp::build_command(cmd_args) {
        Ok(Some(c)) => c,
        Ok(None) => return Err(mlua::Error::RuntimeError("ERR empty command".to_string())),
        Err(e) => return Err(mlua::Error::RuntimeError(format!("ERR {}", e))),
    };
    let mut out = Vec::new();
    let aof_ref = unsafe { aof_call.map(|ptr| &*ptr) };
    crate::connection::execute_local_command(&cmd, &mut db_call.borrow_mut(), &mut out, aof_ref);
    resp_bytes_to_lua(lua, &out)
})?;
```

`redis.call`'s arguments are converted to `Bytes` and handed to the **same** `build_command`
that parses commands off the wire (Component 03), then executed through the **same**
`execute_local_command` that the normal squashed/local dispatch path uses (Component 02) —
there is no separate "scripting command table"; a script literally issues real `Command`
values against the real `ShardDb`. `redis.pcall` is identical except it catches a RESP error
reply (`out.starts_with(b"-")`) and turns it into a Lua table `{err = "..."}` instead of
raising a Lua error, matching Redis's `call` (throws) vs. `pcall` (returns an error table)
distinction.

The `aof_call: Option<*const RefCell<AofWriter>>` capture is a raw pointer cast specifically
so the closure can be `'static` (required by `mlua::Lua::create_function`) while still
referencing a `RefCell` borrowed from the caller's stack for the duration of one `eval_script`
call — `unsafe { aof_call.map(|ptr| &*ptr) }` reconstitutes the reference at call time. This
is sound only because the `Lua` instance (and thus every closure it holds) is dropped before
`eval_script` returns and the borrow ends; nothing about `mlua`'s API enforces that guarantee,
so it's a manual invariant, not a compiler-checked one.

### 4.3 RESP ⇄ Lua value conversion (`resp_bytes_to_lua`, `lua_val_to_resp`)

`resp_bytes_to_lua` turns a raw RESP reply (as bytes) into an `mlua::Value` per the real Redis
Lua conversion rules: `+OK` → `{ok = "OK"}` table, `-ERR ...` → a Lua error (not a value —
this makes `redis.call` on a failing command raise instead of return, which `redis.pcall`
intercepts one level up), `:N` → integer, `$-1` (null bulk) → `false`, a bulk string → a Lua
string, and `*N` arrays are recursively parsed by `parse_resp_array_to_lua` (which itself
handles nested `*`, `$`, `:`, `+` elements). `lua_val_to_resp` is the reverse mapping used for
the script's *final* return value: `nil`→`$-1`, `true`→`:1`, `false`→`$-1` (Redis's own
`false`-means-nil-reply convention), a table with an `"ok"` key → `+...`, a table with an
`"err"` key → `-...`, otherwise a table is treated as a 1-indexed array and serialized as a
RESP array via `t.raw_len()`. Both directions call the real `crate::connection::
write_resp_integer`/`write_resp_bulk` helper functions (these do exist in `connection.rs`,
just not in `resp.rs` — see Component 03).

### 4.4 `FUNCTION LOAD`/`FCALL`: a two-pass execution, no persistent function objects

```rust
pub fn load_function(code: &str, replace: bool) -> Result<String, String> {
    // parse "#!lua name=<lib>" shebang for the library name
    ...
    let lua = Lua::new();                       // throwaway VM #1, just to discover names
    let reg_fn = lua.create_function(move |_, (name, _): (String, Value)| {
        func_names_clone.borrow_mut().push(name);
        Ok(())
    })?;
    redis_tbl.set("register_function", reg_fn)?;
    lua.load(&lua_code).exec()?;                 // runs the WHOLE library top-level once
    // ... store FunctionLib { name, engine: "LUA", raw_code, functions: registered }
}
```

`FUNCTION LOAD` runs the library's top-level code **once**, with `redis.register_function`
stubbed out to just record function *names* — it discards whatever Lua closures were
registered. `FCALL` then does the real work by running the library's full source **again**,
in a second fresh `Lua::new()`, this time with a real `register_function` that captures the
one closure matching the requested name into the Lua registry (`create_registry_value`) and
calls it directly with `(keys_tbl, argv_tbl)`. So a library's top-level code executes twice
total per `FCALL` call across its lifetime relative to any single load: once at `FUNCTION
LOAD` (to discover names) and once per `FCALL` (to actually get a callable closure) — there is
no cached, ready-to-call function object between calls. **`FCALL`'s `aof` argument is
hardcoded to `None`** in `connection.rs` (`crate::scripting::call_function(&function, &keys,
&args, &router.local_db, None)`), unlike `EVAL`/`EVALSHA` which thread through
`router.aof.as_deref()` — writes performed via `FCALL` are not appended to the AOF. This looks
like an oversight rather than a documented design choice.

### 4.5 What the old doc got right

`redis.sha1hex` (backed by the same `sha1_hex` helper used for script caching) and
`redis.error_reply`/`redis.status_reply` (building `{err=...}`/`{ok=...}` tables) are real and
match the old doc's description, modulo the exact function names.

---

## 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): owns all `SCRIPT_CACHE`/`FUNCTION_LIBS` mutation
  entry points (`Command::Eval`, `Evalsha`, `ScriptLoad`, `ScriptExists`, `ScriptFlush`,
  `FunctionLoad`, `Fcall`, `FunctionList`/`Delete`/`Flush` — not individually enumerated
  above); also supplies `write_resp_integer`/`write_resp_bulk` used by `lua_val_to_resp`, and
  `execute_local_command`, the single function both `redis.call` and every normal command
  dispatch path funnel through.
- **`src/resp.rs`** (Component 03): `build_command` is called from inside the `redis.call`/
  `redis.pcall` closures to turn Lua-supplied arguments back into a real `Command`.
- **`src/shard.rs`/`src/table.rs`** (Components 04/05): mutated whenever a script's
  `redis.call` executes a write command, exactly as an ordinary client-issued command would.
- **`src/aof.rs`**: `EVAL`/`EVALSHA` writes are appended via the `aof` parameter threaded into
  `eval_script`; `FCALL` writes currently are not (§4.4).

---

## 6. Performance Characteristics

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
