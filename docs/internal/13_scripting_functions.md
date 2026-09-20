# Component 13: Lua Scripting & Redis 7 Functions Engine (Implementation)

> **Source Files**: `src/scripting.rs`


---

### 3. Component Architecture & Data Structures

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

#### The real caches (there is no `ScriptEngine`/`FunctionDef` struct)

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

### 4. Execution Algorithms & Code Logic

#### 4.1 `EVAL`/`EVALSHA`: `connection.rs` owns caching, `scripting.rs` owns execution

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

#### 4.2 `redis.call`/`redis.pcall`: a Lua closure that round-trips through the real RESP path

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

#### 4.3 RESP ⇄ Lua value conversion (`resp_bytes_to_lua`, `lua_val_to_resp`)

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

#### 4.4 `FUNCTION LOAD`/`FCALL`: a two-pass execution, no persistent function objects

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
no cached, ready-to-call function object between calls. **Update: the AOF bug below is now
fixed.** `FCALL`'s `aof` argument was previously hardcoded to `None` in `connection.rs`,
meaning writes performed via `FCALL` were silently dropped from the AOF. As of the current
source, `connection.rs`'s `Command::Fcall` arm passes `router.aof.as_deref()` — the same way
`EVAL`/`EVALSHA` already did — so `FCALL` writes are now correctly persisted:

```rust
Command::Fcall { function, keys, args } => {
    match crate::scripting::call_function(&function, &keys, &args, &router.local_db, router.aof.as_deref()) {
        ...
    }
}
```

This is verified by a new unit test in `scripting.rs` itself (`test_fcall_with_aof_writer`),
which calls `call_function` with a real `AofWriter` and asserts the resulting AOF buffer
contains the expected `SET` command.

#### 4.5 What the old doc got right

`redis.sha1hex` (backed by the same `sha1_hex` helper used for script caching) and
`redis.error_reply`/`redis.status_reply` (building `{err=...}`/`{ok=...}` tables) are real and
match the old doc's description, modulo the exact function names.

---

### 5. Cross-Component Interactions

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
- **`src/aof.rs`**: `EVAL`/`EVALSHA` and, as of the current source, `FCALL` writes are all
  appended via the `aof` parameter threaded into `eval_script`/`call_function` (§4.4).

---

### 7. Future Improvements

- **RESOLVED — `FCALL`'s hardcoded `None` AOF argument (§4.4).** Fixed: `connection.rs`'s `Fcall` arm now passes `router.aof.as_deref()`, the same as `EVAL`/`EVALSHA`, and a new unit test (`test_fcall_with_aof_writer`) verifies `FCALL` writes land in the AOF buffer. `FCALL` writes are also now covered by an E2E integration test per the commit history (`test(scripting): add unit and E2E integration tests for FCALL mutating AOF persistence`).
- **Medium — cache compiled function objects instead of re-running a library's top-level code per `FCALL` (§4.4).** The current two-pass design (run once at `FUNCTION LOAD` just to discover names, run the *entire library* again from scratch on every `FCALL` to get a callable closure) means library init cost is paid on every single call, not just at load time. Since a fresh `Lua::new()` per call already means there's no persistent VM to hold a closure across calls, the more impactful fix is likely pairing this with the next item (a small VM pool) rather than trying to persist closures across genuinely separate VM instances.
- **Medium — consider a small pool of reusable `Lua` VMs (or persistent per-shard VMs) instead of `Lua::new()` per call (§6).** Fresh-VM-per-call is simple and safe (no state leaks between scripts) but means every `EVAL`/`EVALSHA`/`FCALL` pays VM construction plus re-parsing the script source from scratch. A per-shard VM reused across calls (clearing globals between invocations, or using `mlua`'s sandboxing/scope features to isolate one call from the next) would remove both costs for script-heavy workloads, at the cost of more careful state-isolation reasoning than the current always-fresh approach needs.
- **Medium — decide on and document a sandboxing posture (§2.2).** Today a script has full access to Lua's standard library (`io`, `os`, etc.) via `Lua::new()`'s defaults — fine if scripting is treated as an admin/trusted-operator-only feature, a real problem if any less-trusted caller can reach `EVAL`. Either explicitly strip dangerous globals (mirroring real Redis's Lua sandbox) or document clearly that `EVAL`/`FCALL` require the same trust level as shell access.
- **Low — hash script source with a faster non-cryptographic hash for `SCRIPT LOAD`'s cache key** if SHA1 computation ever shows up as measurable overhead on the load path — currently fine since it's a one-time cost per unique script, not per `EVALSHA` call.

---
---
