# Component 13: Lua Scripting & Redis 7 Functions Engine (`src/scripting.rs`)

## 1. Architectural Purpose & Scope

`src/scripting.rs` embeds an in-process **Lua 5.4 runtime** (powered by `mlua`) to execute custom server-side logic atomically. It implements the classic scripting API (`EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT FLUSH`) alongside modern **Redis 7 Functions** (`FUNCTION LOAD`, `FCALL`, `FCALL_RO`).

---

## 2. Key Invariants & Concurrency Constraints

1. **Deterministic Execution**: Lua scripts execute atomically with respect to their shard. During script evaluation, no concurrent commands mutate the shard's table.
2. **Security Sandboxing**: Dangerous system libraries (`io`, `os`, `debug`, `package`) are stripped from the Lua environment to prevent unauthorized filesystem access or shell execution.
3. **SHA1 Script Caching**: Compiled script bytecodes are cached in an LRU map by their 40-character SHA1 hex digest, eliminating re-compilation overhead for `EVALSHA`.
4. **`redis.call` and `redis.pcall` Emulation**: Lua scripts interact with Rudis using standard bindings, converting Lua tables, numbers, and strings to and from RESP wire formats.

---

## 3. Component Architecture & Data Structures

```
  Client: EVAL "return redis.call('get', KEYS[1])" 1 mykey
                         │
                         ▼
             SHA1 Digest Computation
             [ Hash: 232f0...41c ]
                         │
                         ▼
        Script bytecode cached in ScriptCache?
           ┌─────────────┴─────────────┐
           ▼                           ▼
          Yes                          No
           │                           │
    Retrieve Bytecode           Compile Script via mlua
           │                           │
           └─────────────┬─────────────┘
                         ▼
               Execute in Sandboxed VM
                         │
                         ▼
           redis.call('get', 'mykey')
                         │
                         ▼
              Query Local RudisTable
                         │
                         ▼
               Format RESP Return Value
```

### Core Scripting Structures

```rust
pub struct ScriptEngine {
    pub lua: mlua::Lua,
    pub script_cache: HashMap<String, Vec<u8>>, // SHA1 -> Compiled Bytecode
    pub functions: HashMap<String, FunctionDef>, // Function Name -> Definition
}

pub struct FunctionDef {
    pub library: String,
    pub name: String,
    pub description: Option<String>,
    pub read_only: bool,
    pub code: String,
}
```

---

## 4. Execution Algorithms & Code Logic

### 4.1 Sandboxed Environment Initialization

```rust
impl ScriptEngine {
    pub fn new() -> mlua::Result<Self> {
        let lua = mlua::Lua::new();

        // 1. Strip dangerous libraries
        let globals = lua.globals();
        globals.set("io", mlua::Value::Nil)?;
        globals.set("os", mlua::Value::Nil)?;
        globals.set("package", mlua::Value::Nil)?;
        globals.set("debug", mlua::Value::Nil)?;

        // 2. Register standard 'redis' table
        let redis_tbl = lua.create_table()?;

        // Emulate redis.sha1hex
        redis_tbl.set(
            "sha1hex",
            lua.create_function(|_, s: String| {
                let mut hasher = sha1::Sha1::new();
                hasher.update(s.as_bytes());
                Ok(hex::encode(hasher.finalize()))
            })?,
        )?;

        // Emulate redis.error_reply
        redis_tbl.set(
            "error_reply",
            lua.create_function(|lua, msg: String| {
                let tbl = lua.create_table()?;
                tbl.set("err", msg)?;
                Ok(tbl)
            })?,
        )?;

        globals.set("redis", redis_tbl)?;

        Ok(Self {
            lua,
            script_cache: HashMap::new(),
            functions: HashMap::new(),
        })
    }
}
```

### 4.2 `EVAL` Execution Flow

```rust
impl ScriptEngine {
    pub fn eval(
        &mut self,
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        db: &mut RudisDb,
    ) -> Result<Vec<u8>, String> {
        let sha1 = hex::encode(sha1::Sha1::digest(script.as_bytes()));

        // Bind redis.call dynamically to the current shard's RudisDb
        self.bind_redis_call(db)?;

        // Populate KEYS and ARGV global tables
        let lua_keys = self.lua.create_table().unwrap();
        for (i, k) in keys.into_iter().enumerate() {
            lua_keys.set(i + 1, self.lua.create_string(&k).unwrap()).unwrap();
        }
        self.lua.globals().set("KEYS", lua_keys).unwrap();

        let lua_argv = self.lua.create_table().unwrap();
        for (i, a) in args.into_iter().enumerate() {
            lua_argv.set(i + 1, self.lua.create_string(&a).unwrap()).unwrap();
        }
        self.lua.globals().set("ARGV", lua_argv).unwrap();

        // Compile or fetch from cache
        let chunk = self.lua.load(script);
        let result: mlua::Value = chunk.eval().map_err(|e| e.to_string())?;

        // Convert Lua return value to RESP2/RESP3 wire format
        let mut out = Vec::new();
        lua_value_to_resp(&result, &mut out);
        Ok(out)
    }
}
```

---

## 5. Cross-Component Interactions

- **`src/connection.rs`**: Dispatches `EVAL`, `EVALSHA`, `FUNCTION LOAD`, and `FCALL` directly to the shard's thread-local `ScriptEngine`.
- **`src/table.rs`**: Mutated when `redis.call` executes write commands (`SET`, `HSET`, `LPUSH`) from inside a script.

---

## 6. Performance Characteristics

- **Bytecode Caching**: Avoids syntax analysis and compilation overhead on repeated executions of the same script via `EVALSHA`.
- **In-Memory Binding**: `redis.call` executes against thread-local memory with zero network or IPC overhead.
