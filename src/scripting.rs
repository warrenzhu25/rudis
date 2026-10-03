use crate::resp::Command;
use bytes::Bytes;
use mlua::{Lua, MultiValue, Value};
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{LazyLock, RwLock};

static SCRIPT_CACHE: LazyLock<RwLock<HashMap<String, String>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn sha1_hex(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut hasher = Sha1::new();
    hasher.update(data);
    let res = hasher.finalize();
    let mut s = String::with_capacity(res.len() * 2);
    for b in res {
        let _ = write!(&mut s, "{:02x}", b);
    }
    s
}

pub fn load_script(script: &[u8]) -> String {
    let sha = sha1_hex(script);
    let script_str = String::from_utf8_lossy(script).to_string();
    SCRIPT_CACHE
        .write()
        .unwrap()
        .insert(sha.clone(), script_str);
    sha
}

pub fn get_script(sha: &str) -> Option<String> {
    let sha_lower = sha.to_lowercase();
    SCRIPT_CACHE.read().unwrap().get(&sha_lower).cloned()
}

pub fn script_exists(shas: &[Bytes]) -> Vec<bool> {
    let cache = SCRIPT_CACHE.read().unwrap();
    shas.iter()
        .map(|s| {
            let s_str = String::from_utf8_lossy(s).to_lowercase();
            cache.contains_key(&s_str)
        })
        .collect()
}

pub fn flush_scripts() {
    SCRIPT_CACHE.write().unwrap().clear();
}

pub fn cached_scripts_count() -> usize {
    SCRIPT_CACHE.read().unwrap().len()
}

/// A registered Redis 7 Function definition
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FunctionDef {
    pub name: String,
    pub description: String,
    pub flags: Vec<String>,
}

/// A registered Redis 7 Function Library
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FunctionLib {
    pub name: String,
    pub engine: String,
    pub raw_code: String,
    #[serde(default)]
    pub original_code: String,
    pub functions: Vec<FunctionDef>,
}

static FUNCTION_LIBS: LazyLock<RwLock<HashMap<String, FunctionLib>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn is_script_read_only(script: &[u8]) -> bool {
    if let Ok(s) = std::str::from_utf8(script)
        && let Some(rest) = s.strip_prefix("#!")
    {
        let header = rest.lines().next().unwrap_or("");
        for token in header.split_whitespace() {
            if let Some(flags_str) = token.strip_prefix("flags=")
                && flags_str.split(',').any(|f| f == "no-writes")
            {
                return true;
            }
        }
    }
    false
}

pub fn is_sha_read_only(sha: &[u8]) -> bool {
    if let Ok(sha_str) = std::str::from_utf8(sha)
        && let Some(script) = get_script(sha_str)
    {
        return is_script_read_only(script.as_bytes());
    }
    false
}

pub fn is_function_read_only(name: &str) -> bool {
    let cache = FUNCTION_LIBS.read().unwrap();
    for l in cache.values() {
        for f in &l.functions {
            if f.name.eq_ignore_ascii_case(name) {
                return f.flags.iter().any(|flag| flag == "no-writes");
            }
        }
    }
    false
}

pub fn count_functions() -> usize {
    let cache = FUNCTION_LIBS.read().unwrap();
    cache.values().map(|l| l.functions.len()).sum()
}

pub fn count_libraries() -> usize {
    FUNCTION_LIBS.read().unwrap().len()
}

thread_local! {
    pub static SCRIPT_RECORDED_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn validate_library_or_function_name(bytes: &[u8]) -> Result<(), &'static str> {
    if bytes.is_empty()
        || !bytes
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b'_')
    {
        Err(
            "Library names can only contain letters, numbers, or underscores(_) and must be at least one character long",
        )
    } else {
        Ok(())
    }
}

fn is_valid_function_flag(flag: &str) -> bool {
    matches!(
        flag,
        "no-writes" | "allow-oom" | "allow-stale" | "no-cluster" | "allow-cross-slot-keys"
    )
}

fn create_load_redis_proxy(lua: &Lua, reg_fn: mlua::Function) -> mlua::Result<mlua::Table> {
    let raw_load_redis = lua.create_table()?;
    raw_load_redis.set("register_function", reg_fn)?;
    raw_load_redis.set("REDIS_VERSION", "7.2.0")?;
    raw_load_redis.set("REDIS_VERSION_NUM", 0x00070200i64)?;
    raw_load_redis.set("LOG_DEBUG", 0i64)?;
    raw_load_redis.set("LOG_VERBOSE", 1i64)?;
    raw_load_redis.set("LOG_NOTICE", 2i64)?;
    raw_load_redis.set("LOG_WARNING", 3i64)?;

    let make_proxy: mlua::Function = lua
        .load(
            r#"
        local raw_load_redis = ...
        local setmetatable = setmetatable
        local error = error
        local tostring = tostring
        local string = string
        local proxy = {}
        local mt = {
            __index = function(t, k)
                local v = raw_load_redis[k]
                if v ~= nil then return v end
                error(string.format("Script attempted to access nonexistent global variable '%s'", tostring(k)), 2)
            end,
            __newindex = function(t, k, v)
                error("Attempt to modify a readonly table", 2)
            end
        }
        setmetatable(proxy, mt)
        return proxy
        "#,
        )
        .into_function()?;
    make_proxy.call(raw_load_redis)
}

/// Load a Redis 7 Function Library
pub fn load_function(code: &str, replace: bool) -> Result<String, String> {
    let first_line = code.lines().next().unwrap_or("");
    if !first_line.starts_with("#!") {
        return Err("ERR Missing library metadata".to_string());
    }
    let after_shebang = &first_line[2..];
    let (eng_raw, rest_meta) = match after_shebang.find(char::is_whitespace) {
        Some(idx) => (&after_shebang[..idx], &after_shebang[idx..]),
        None => (after_shebang, ""),
    };

    if !eng_raw.eq_ignore_ascii_case("lua") {
        return Err(format!("ERR Engine '{}' not found", eng_raw));
    }
    let engine = "LUA".to_string();

    let mut name_opt: Option<String> = None;
    for part in rest_meta.split_whitespace() {
        if let Some(val_raw) = part.strip_prefix("name=") {
            if name_opt.is_some() {
                return Err(
                    "ERR Invalid metadata value, name argument was given multiple times"
                        .to_string(),
                );
            }
            let val = val_raw.trim_matches('"');
            name_opt = Some(val.to_string());
        } else {
            return Err(format!("ERR Invalid metadata value given: {}", part));
        }
    }

    let lib_name = match name_opt {
        Some(n) => n,
        None => return Err("ERR Library name was not given".to_string()),
    };

    if let Err(msg) = validate_library_or_function_name(lib_name.as_bytes()) {
        return Err(format!("ERR {}", msg));
    }

    {
        let cache = FUNCTION_LIBS.read().unwrap();
        if cache.keys().any(|k| k.eq_ignore_ascii_case(&lib_name)) && !replace {
            return Err(format!("ERR Library '{}' already exists", lib_name));
        }
    }

    let lua = Lua::new();
    let real_g = lua.create_table().map_err(|e| e.to_string())?;
    let globals = lua.globals();
    for pair in globals.clone().pairs::<Value, Value>() {
        let (k, v) = pair.map_err(|e| e.to_string())?;
        real_g.set(k, v).map_err(|e| e.to_string())?;
    }
    if let Ok(table_mod) = real_g.get::<mlua::Table>("table")
        && let Ok(unpack_fn) = table_mod.get::<mlua::Function>("unpack")
    {
        let _ = real_g.set("unpack", unpack_fn);
    }
    // Remove globals not allowed during FUNCTION LOAD
    for disallowed in [
        "math",
        "os",
        "getmetatable",
        "setmetatable",
        "load",
        "loadstring",
        "dofile",
        "loadfile",
        "print",
        "rawget",
        "rawset",
        "rawequal",
        "collectgarbage",
        "package",
        "coroutine",
        "io",
        "debug",
    ] {
        let _ = real_g.set(disallowed, Value::Nil);
    }

    register_bit_module(&lua, &real_g).map_err(|e| e.to_string())?;
    register_cjson_module(&lua, &real_g).map_err(|e| e.to_string())?;
    register_cmsgpack_module(&lua, &real_g).map_err(|e| e.to_string())?;

    let func_defs: Rc<RefCell<Vec<FunctionDef>>> = Rc::new(RefCell::new(Vec::new()));
    let func_defs_clone = func_defs.clone();

    let reg_fn = lua
        .create_function(move |_, args: MultiValue| {
            let args_vec: Vec<Value> = args.into_iter().collect();
            if args_vec.is_empty() || args_vec.len() > 2 {
                return Err(mlua::Error::RuntimeError(
                    "wrong number of arguments to redis.register_function".to_string(),
                ));
            }

            let (name, desc, flags) = if args_vec.len() == 2 {
                let name_s = match &args_vec[0] {
                    Value::String(s) => s,
                    _ => {
                        return Err(mlua::Error::RuntimeError(
                            "first argument to redis.register_function must be a string"
                                .to_string(),
                        ))
                    }
                };
                if let Err(msg) = validate_library_or_function_name(&name_s.as_bytes()) {
                    return Err(mlua::Error::RuntimeError(msg.to_string()));
                }
                let name_str = String::from_utf8_lossy(&name_s.as_bytes()).to_string();
                match &args_vec[1] {
                    Value::Function(_) => {}
                    _ => {
                        return Err(mlua::Error::RuntimeError(
                            "second argument to redis.register_function must be a function"
                                .to_string(),
                        ))
                    }
                }
                (name_str, String::new(), Vec::new())
            } else {
                let t = match &args_vec[0] {
                    Value::Table(t) => t,
                    _ => {
                        return Err(mlua::Error::RuntimeError(
                            "calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments).".to_string(),
                        ))
                    }
                };
                let mut fn_name_opt: Option<String> = None;
                let mut callback_set = false;
                let mut desc_opt: Option<String> = None;
                let mut flags_vec: Vec<String> = Vec::new();

                for pair in t.clone().pairs::<Value, Value>() {
                    let (k, v) = pair?;
                    let key_str = match k {
                        Value::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                        _ => {
                            return Err(mlua::Error::RuntimeError(
                                "unknown argument given to redis.register_function".to_string(),
                            ))
                        }
                    };
                    match key_str.as_str() {
                        "function_name" => match v {
                            Value::String(s) => {
                                if let Err(msg) = validate_library_or_function_name(&s.as_bytes()) {
                                    return Err(mlua::Error::RuntimeError(msg.to_string()));
                                }
                                fn_name_opt =
                                    Some(String::from_utf8_lossy(&s.as_bytes()).to_string());
                            }
                            _ => {
                                return Err(mlua::Error::RuntimeError(
                                    "function_name argument given to redis.register_function must be a string".to_string(),
                                ))
                            }
                        },
                        "callback" => match v {
                            Value::Function(_) => {
                                callback_set = true;
                            }
                            _ => {
                                return Err(mlua::Error::RuntimeError(
                                    "callback argument given to redis.register_function must be a function".to_string(),
                                ))
                            }
                        },
                        "description" => match v {
                            Value::String(s) => {
                                desc_opt = Some(String::from_utf8_lossy(&s.as_bytes()).to_string());
                            }
                            _ => {
                                return Err(mlua::Error::RuntimeError(
                                    "description argument given to redis.register_function must be a string".to_string(),
                                ))
                            }
                        },
                        "flags" => match v {
                            Value::Table(flags_tbl) => {
                                for fpair in flags_tbl.pairs::<Value, Value>() {
                                    let (fk, fv) = fpair?;
                                    if !matches!(fk, Value::Integer(_)) {
                                        return Err(mlua::Error::RuntimeError(
                                            "unknown flag given".to_string(),
                                        ));
                                    }
                                    match fv {
                                        Value::String(fs) => {
                                            let f_str =
                                                String::from_utf8_lossy(&fs.as_bytes()).to_string();
                                            if !is_valid_function_flag(&f_str) {
                                                return Err(mlua::Error::RuntimeError(
                                                    "unknown flag given".to_string(),
                                                ));
                                            }
                                            if !flags_vec.contains(&f_str) {
                                                flags_vec.push(f_str);
                                            }
                                        }
                                        _ => {
                                            return Err(mlua::Error::RuntimeError(
                                                "unknown flag given".to_string(),
                                            ))
                                        }
                                    }
                                }
                            }
                            _ => {
                                return Err(mlua::Error::RuntimeError(
                                    "flags argument to redis.register_function must be a table representing function flags".to_string(),
                                ))
                            }
                        },
                        _ => {
                            return Err(mlua::Error::RuntimeError(
                                "unknown argument given to redis.register_function".to_string(),
                            ))
                        }
                    }
                }

                let name_str = match fn_name_opt {
                    Some(n) => n,
                    None => {
                        return Err(mlua::Error::RuntimeError(
                            "redis.register_function must get a function name argument".to_string(),
                        ))
                    }
                };
                if !callback_set {
                    return Err(mlua::Error::RuntimeError(
                        "redis.register_function must get a callback argument".to_string(),
                    ));
                }
                (name_str, desc_opt.unwrap_or_default(), flags_vec)
            };

            let mut defs = func_defs_clone.borrow_mut();
            if defs.iter().any(|d| d.name.eq_ignore_ascii_case(&name)) {
                return Err(mlua::Error::RuntimeError(
                    "Function already exists in the library".to_string(),
                ));
            }
            defs.push(FunctionDef {
                name,
                description: desc,
                flags,
            });
            Ok(())
        })
        .map_err(|e| e.to_string())?;

    let load_redis_proxy = create_load_redis_proxy(&lua, reg_fn).map_err(|e| e.to_string())?;
    real_g
        .set("redis", load_redis_proxy)
        .map_err(|e| e.to_string())?;

    let setup_load_sandbox: mlua::Function = lua
        .load(
            r#"
        local real_g = ...
        local globals = _G
        local setmetatable = setmetatable
        local error = error
        local pairs = pairs
        local tostring = tostring
        local string = string

        for k in pairs(globals) do
            globals[k] = nil
        end

        local g_mt = {
            __index = function(t, k)
                if k == "_G" then return globals end
                local v = real_g[k]
                if v ~= nil then return v end
                error(string.format("Script attempted to access nonexistent global variable '%s'", tostring(k)), 2)
            end,
            __newindex = function(t, k, v)
                error("Attempt to modify a readonly table", 2)
            end
        }
        setmetatable(globals, g_mt)
        "#,
        )
        .into_function()
        .map_err(|e| e.to_string())?;
    setup_load_sandbox
        .call::<()>(real_g)
        .map_err(|e| e.to_string())?;

    let start_time = std::time::Instant::now();
    let _ = lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(1000),
        move |_, _| {
            if start_time.elapsed() > std::time::Duration::from_millis(500) {
                Err(mlua::Error::RuntimeError(
                    "FUNCTION LOAD timeout".to_string(),
                ))
            } else {
                Ok(mlua::VmState::Continue)
            }
        },
    );

    let lua_code: String = code
        .lines()
        .enumerate()
        .map(|(i, line)| {
            if i == 0 && line.starts_with("#!") {
                format!("--{}", line)
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    lua.load(&lua_code).exec().map_err(|e| {
        let s = e.to_string();
        if s.contains("FUNCTION LOAD timeout") {
            return "ERR FUNCTION LOAD timeout".to_string();
        }
        let first = s.lines().next().unwrap_or(&s);
        format!("ERR Error compiling function: {}", first)
    })?;

    let registered = func_defs.borrow().clone();
    if registered.is_empty() {
        return Err("ERR No functions registered".to_string());
    }

    let mut cache = FUNCTION_LIBS.write().unwrap();
    // Check cross-library function name collision
    for existing_lib in cache.values() {
        if !existing_lib.name.eq_ignore_ascii_case(&lib_name) {
            for f in &registered {
                if existing_lib
                    .functions
                    .iter()
                    .any(|ef| ef.name.eq_ignore_ascii_case(&f.name))
                {
                    return Err(format!("ERR Function {} already exists", f.name));
                }
            }
        }
    }

    if let Some(existing_key) = cache
        .keys()
        .find(|k| k.eq_ignore_ascii_case(&lib_name))
        .cloned()
    {
        cache.remove(&existing_key);
    }

    let lib = FunctionLib {
        name: lib_name.clone(),
        engine,
        raw_code: lua_code,
        original_code: code.to_string(),
        functions: registered,
    };

    cache.insert(lib_name.clone(), lib);
    Ok(lib_name)
}

/// Returns list of registered libraries and functions
pub fn list_functions() -> Vec<FunctionLib> {
    let cache = FUNCTION_LIBS.read().unwrap();
    cache.values().cloned().collect()
}

/// Delete a registered function library
pub fn delete_function(lib_name: &str) -> bool {
    let mut cache = FUNCTION_LIBS.write().unwrap();
    if let Some(k) = cache
        .keys()
        .find(|k| k.eq_ignore_ascii_case(lib_name))
        .cloned()
    {
        cache.remove(&k).is_some()
    } else {
        false
    }
}

/// Flush all registered function libraries
pub fn flush_functions() {
    let mut cache = FUNCTION_LIBS.write().unwrap();
    let func_count: usize = cache.values().map(|l| l.functions.len()).sum();
    if func_count > 64 {
        crate::table::add_lazyfreed_objects((func_count + 1) as u64);
    }
    cache.clear();
}

pub fn dump_functions() -> Vec<u8> {
    let cache = FUNCTION_LIBS.read().unwrap();
    let libs: Vec<FunctionLib> = cache.values().cloned().collect();
    let json = serde_json::to_vec(&libs).unwrap_or_default();
    let mut out = Vec::with_capacity(16 + json.len());
    out.extend_from_slice(b"RUDFUNCv1\x00");
    let crc = crc16::State::<crc16::XMODEM>::calculate(&json);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&json);
    out
}

pub fn restore_functions(payload: &[u8], policy: &str) -> Result<(), String> {
    if payload.len() < 12 || !payload.starts_with(b"RUDFUNCv1\x00") {
        return Err("ERR DUMP payload version or checksum are wrong".to_string());
    }
    let crc_expected = u16::from_be_bytes([payload[10], payload[11]]);
    let json = &payload[12..];
    let crc_actual = crc16::State::<crc16::XMODEM>::calculate(json);
    if crc_expected != crc_actual {
        return Err("ERR DUMP payload version or checksum are wrong".to_string());
    }
    let libs: Vec<FunctionLib> = serde_json::from_slice(json)
        .map_err(|_| "ERR DUMP payload version or checksum are wrong".to_string())?;

    let mut cache = FUNCTION_LIBS.write().unwrap();
    if policy == "FLUSH" {
        cache.clear();
        for lib in libs {
            cache.insert(lib.name.clone(), lib);
        }
    } else if policy == "APPEND" {
        for lib in &libs {
            if cache.keys().any(|k| k.eq_ignore_ascii_case(&lib.name)) {
                return Err(format!("ERR Library {} already exists", lib.name));
            }
            for f in &lib.functions {
                for existing_lib in cache.values() {
                    if existing_lib
                        .functions
                        .iter()
                        .any(|ef| ef.name.eq_ignore_ascii_case(&f.name))
                    {
                        return Err(format!("ERR Function {} already exists", f.name));
                    }
                }
            }
        }
        for lib in libs {
            cache.insert(lib.name.clone(), lib);
        }
    } else if policy == "REPLACE" {
        for lib in &libs {
            for f in &lib.functions {
                for existing_lib in cache.values() {
                    let being_replaced = libs
                        .iter()
                        .any(|nl| nl.name.eq_ignore_ascii_case(&existing_lib.name));
                    if !being_replaced
                        && existing_lib
                            .functions
                            .iter()
                            .any(|ef| ef.name.eq_ignore_ascii_case(&f.name))
                    {
                        return Err(format!("ERR Function {} already exists", f.name));
                    }
                }
            }
        }
        for lib in libs {
            if let Some(existing_key) = cache
                .keys()
                .find(|k| k.eq_ignore_ascii_case(&lib.name))
                .cloned()
            {
                cache.remove(&existing_key);
            }
            cache.insert(lib.name.clone(), lib);
        }
    }
    Ok(())
}

fn format_lua_error(s: &str) -> String {
    let first = s.lines().next().unwrap_or(s);
    if let Some(pos) = first.find("ERR ") {
        return first[pos..].to_string();
    }
    if let Some(pos) = first.find("NOSCRIPT ") {
        return first[pos..].to_string();
    }
    if let Some(pos) = first.find("WRONGTYPE ") {
        return first[pos..].to_string();
    }
    if let Some(pos) = first.find("attempt to call a nil value (field '") {
        let after = &first[pos + 36..];
        let field_name = after.split('\'').next().unwrap_or("");
        return format!("ERR attempt to call field '{}' (a nil value)", field_name);
    }
    if let Some(pos) = first.find("attempt to call a nil value (global '") {
        let after = &first[pos + 37..];
        let global_name = after.split('\'').next().unwrap_or("");
        return format!("ERR attempt to call global '{}' (a nil value)", global_name);
    }
    if first.contains("attempt to index a nil value") {
        return "ERR user_script:1: attempt to index a nil value script".to_string();
    }
    if first.starts_with("ERR ") {
        first.to_string()
    } else {
        format!("ERR {}", first)
    }
}

fn format_eval_error(err: &str, sha: &str) -> String {
    let mut msg = err.lines().next().unwrap_or(err).trim_start();
    if let Some(pos) = msg.find("runtime error: ") {
        msg = &msg[pos + 15..];
    }
    if let Some(stripped) = msg.strip_prefix('@') {
        msg = stripped;
    }
    if let Some(pos) = msg.find("NOREPLICAS ") {
        return msg[pos..].to_string();
    }
    if let Some(pos) = msg.find("OOM ") {
        return msg[pos..].to_string();
    }
    if let Some(pos) = msg.find("attempt to call a nil value (field '") {
        let after = &msg[pos + 36..];
        let field_name = after.split('\'').next().unwrap_or("");
        return format!(
            "ERR attempt to call field '{}' (a nil value) script: {}, on @user_script:1.",
            field_name, sha
        );
    }
    if let Some(pos) = msg.find("attempt to call a nil value (global '") {
        let after = &msg[pos + 37..];
        let global_name = after.split('\'').next().unwrap_or("");
        return format!(
            "ERR attempt to call global '{}' (a nil value) script: {}, on @user_script:1.",
            global_name, sha
        );
    }
    let final_prefix = if msg.starts_with("ERR ")
        || msg.starts_with("NOSCRIPT ")
        || msg.starts_with("WRONGTYPE ")
        || msg.starts_with("NOPERM ")
    {
        msg.to_string()
    } else {
        format!("ERR {}", msg)
    };
    format!("{} script: {}, on @user_script:1.", final_prefix, sha)
}

/// Execute a registered Redis 7 Function via FCALL
pub fn call_function(
    func_name: &str,
    keys: &[Bytes],
    args: &[Bytes],
    db: &Rc<RefCell<crate::shard::ShardDb>>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
    read_only: bool,
) -> Result<Vec<u8>, String> {
    SCRIPT_RECORDED_ERROR.set(false);
    let (lib, func_def) = {
        let cache = FUNCTION_LIBS.read().unwrap();
        let mut found = None;
        for l in cache.values() {
            if let Some(f) = l
                .functions
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(func_name))
            {
                found = Some((l.clone(), f.clone()));
                break;
            }
        }
        found.ok_or_else(|| "ERR Function not found".to_string())?
    };

    if read_only && !func_def.flags.iter().any(|f| f == "no-writes") {
        return Err("ERR Can not execute a script with write flag using *_ro command.".to_string());
    }

    let effective_read_only = read_only || func_def.flags.iter().any(|f| f == "no-writes");

    let lua = Lua::new();
    let aof_raw = aof.map(|a| a as *const _);
    let script_resp_ver = Rc::new(RefCell::new(2u8));
    setup_redis_lua_env(
        &lua,
        db,
        aof_raw,
        effective_read_only,
        script_resp_ver.clone(),
    )?;

    // Set KEYS table
    let keys_tbl = lua.create_table().map_err(|e| e.to_string())?;
    for (i, k) in keys.iter().enumerate() {
        let s = lua.create_string(k.as_ref()).map_err(|e| e.to_string())?;
        keys_tbl.set(i + 1, s).map_err(|e| e.to_string())?;
    }

    // Set ARGV table
    let argv_tbl = lua.create_table().map_err(|e| e.to_string())?;
    for (i, a) in args.iter().enumerate() {
        let s = lua.create_string(a.as_ref()).map_err(|e| e.to_string())?;
        argv_tbl.set(i + 1, s).map_err(|e| e.to_string())?;
    }

    // Capture target function during library load phase
    let target_fn = Rc::new(RefCell::new(None));
    let target_fn_clone = target_fn.clone();
    let target_name_lower = func_name.to_lowercase();
    let in_load_phase = Rc::new(std::cell::Cell::new(true));
    let in_load_phase_clone = in_load_phase.clone();

    let reg_fn = lua
        .create_function(move |lua, margs: MultiValue| {
            if !in_load_phase_clone.get() {
                return Err(mlua::Error::RuntimeError(
                    "redis.register_function can only be called on FUNCTION LOAD command"
                        .to_string(),
                ));
            }
            let mut name_opt = None;
            let mut func_opt = None;
            if let Some(first) = margs.iter().next() {
                match first {
                    Value::String(s) => {
                        if let Ok(name_str) = s.to_str() {
                            name_opt = Some(name_str.to_string());
                        }
                        if let Some(Value::Function(f)) = margs.iter().nth(1) {
                            func_opt = Some(f.clone());
                        }
                    }
                    Value::Table(t) => {
                        if let Ok(name_str) = t
                            .raw_get::<String>("function_name")
                            .or_else(|_| t.raw_get::<String>("name"))
                        {
                            name_opt = Some(name_str);
                        }
                        if let Ok(f) = t.raw_get::<mlua::Function>("callback") {
                            func_opt = Some(f);
                        }
                    }
                    _ => {}
                }
            }
            if let (Some(name), Some(f)) = (name_opt, func_opt)
                && name.to_lowercase() == target_name_lower
            {
                let key = lua.create_registry_value(f)?;
                *target_fn_clone.borrow_mut() = Some(key);
            }
            Ok(())
        })
        .map_err(|e| e.to_string())?;

    let load_redis_proxy = create_load_redis_proxy(&lua, reg_fn).map_err(|e| e.to_string())?;
    let real_g: mlua::Table = lua
        .named_registry_value("__real_G")
        .map_err(|e| e.to_string())?;
    let runtime_redis: Value = real_g.get("redis").map_err(|e| e.to_string())?;
    real_g
        .set("redis", load_redis_proxy)
        .map_err(|e| e.to_string())?;

    // Run library script to define functions
    lua.load(&lib.raw_code)
        .exec()
        .map_err(|e| format!("ERR Failed to compile library: {}", e))?;

    in_load_phase.set(false);
    real_g
        .set("redis", runtime_redis)
        .map_err(|e| e.to_string())?;

    let fn_key = target_fn.borrow_mut().take().ok_or_else(|| {
        format!(
            "ERR Function '{}' registered but failed to capture",
            func_name
        )
    })?;
    let f: mlua::Function = lua.registry_value(&fn_key).map_err(|e| e.to_string())?;

    let res: Value = f
        .call((keys_tbl, argv_tbl))
        .map_err(|e| format_lua_error(&e.to_string()))?;

    let mut out = Vec::new();
    let cur_resp_ver = *script_resp_ver.borrow();
    lua_val_to_resp_with_depth(&res, &mut out, 0, cur_resp_ver)?;
    Ok(out)
}

pub fn eval_script(
    script_content: &str,
    keys: &[Bytes],
    args: &[Bytes],
    db: &Rc<RefCell<crate::shard::ShardDb>>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
    read_only: bool,
) -> Result<Vec<u8>, String> {
    SCRIPT_RECORDED_ERROR.set(false);
    let mut effective_read_only = read_only;
    let mut processed_script = script_content.to_string();

    let first_line = script_content.lines().next().unwrap_or("");
    if first_line.trim_start().starts_with("#!") {
        let trimmed = first_line.trim();
        let after = trimmed[2..].trim();
        let mut parts = after.split_whitespace();
        let engine = parts.next().unwrap_or("");
        if !engine.eq_ignore_ascii_case("lua") {
            return Err("ERR Unexpected engine in script shebang".to_string());
        }
        for opt in parts {
            if let Some(pos) = opt.find('=') {
                let key = &opt[..pos];
                let val = opt[pos + 1..].trim_matches('"');
                if key != "flags" {
                    return Err(format!("ERR Unknown lua shebang option: '{}'", key));
                }
                for f in val.split(',') {
                    let f_trim = f.trim();
                    if f_trim == "no-writes" {
                        effective_read_only = true;
                    } else if f_trim == "allow-oom"
                        || f_trim == "allow-stale"
                        || f_trim == "no-cluster"
                    {
                        // valid flags
                    } else {
                        return Err(format!(
                            "ERR Unexpected flag in script shebang: '{}'",
                            f_trim
                        ));
                    }
                }
            } else {
                return Err(format!("ERR Unknown lua shebang option: '{}'", opt));
            }
        }
        processed_script = script_content
            .lines()
            .enumerate()
            .map(|(i, line)| {
                if i == 0 {
                    format!("--{}", line)
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
    }

    let lua = Lua::new();
    let aof_raw = aof.map(|a| a as *const _);
    let script_resp_ver = Rc::new(RefCell::new(2u8));
    setup_redis_lua_env(
        &lua,
        db,
        aof_raw,
        effective_read_only,
        script_resp_ver.clone(),
    )?;

    // Set KEYS table (1-indexed)
    let keys_tbl = lua.create_table().map_err(|e| e.to_string())?;
    for (i, k) in keys.iter().enumerate() {
        let s = lua.create_string(k.as_ref()).map_err(|e| e.to_string())?;
        keys_tbl.set(i + 1, s).map_err(|e| e.to_string())?;
    }

    // Set ARGV table (1-indexed)
    let argv_tbl = lua.create_table().map_err(|e| e.to_string())?;
    for (i, a) in args.iter().enumerate() {
        let s = lua.create_string(a.as_ref()).map_err(|e| e.to_string())?;
        argv_tbl.set(i + 1, s).map_err(|e| e.to_string())?;
    }

    if let Ok(real_g) = lua.named_registry_value::<mlua::Table>("__real_G") {
        real_g.set("KEYS", keys_tbl).map_err(|e| e.to_string())?;
        real_g.set("ARGV", argv_tbl).map_err(|e| e.to_string())?;
    }

    let chunk_func = lua
        .load(&processed_script)
        .set_name("@user_script")
        .into_function()
        .map_err(|e| format_lua_error(&e.to_string()))?;
    let runner: mlua::Function = lua
        .load(
            r#"
        local f = ...
        return xpcall(f, function(err)
            if type(err) == "table" then
                if err.err then
                    return tostring(err.err)
                else
                    return "ERR unknown error"
                end
            end
            return tostring(err)
        end)
        "#,
        )
        .into_function()
        .map_err(|e| e.to_string())?;

    let (ok, res): (bool, Value) = runner
        .call(chunk_func)
        .map_err(|e| format_lua_error(&e.to_string()))?;

    if !ok {
        let err_msg = match res {
            Value::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
            _ => format!("{:?}", res),
        };
        let sha = load_script(script_content.as_bytes());
        return Err(format_eval_error(&err_msg, &sha));
    }

    let mut resp = Vec::new();
    let cur_resp_ver = *script_resp_ver.borrow();
    lua_val_to_resp_with_depth(&res, &mut resp, 0, cur_resp_ver)?;
    Ok(resp)
}

fn setup_redis_lua_env(
    lua: &Lua,
    db: &Rc<RefCell<crate::shard::ShardDb>>,
    aof: Option<*const RefCell<crate::aof::AofWriter>>,
    read_only: bool,
    script_resp_ver: Rc<RefCell<u8>>,
) -> Result<(), String> {
    let real_g = lua.create_table().map_err(|e| e.to_string())?;

    let globals = lua.globals();
    for pair in globals.clone().pairs::<Value, Value>() {
        let (k, v) = pair.map_err(|e| e.to_string())?;
        real_g.set(k, v).map_err(|e| e.to_string())?;
    }

    static SEED_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let seed = nanos ^ SEED_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _ = lua.load(format!("math.randomseed({})", seed)).exec();

    let table_mod: mlua::Table = real_g.get("table").map_err(|e| e.to_string())?;
    let unpack_fn: mlua::Function = table_mod.get("unpack").map_err(|e| e.to_string())?;
    real_g.set("unpack", unpack_fn).map_err(|e| e.to_string())?;

    let loadstring_fn = lua
        .create_function(|lua, (code, name): (Value, Option<String>)| {
            let load_fn: mlua::Function = lua.globals().get("load")?;
            load_fn.call::<Value>((code, name, "t"))
        })
        .map_err(|e| e.to_string())?;
    real_g
        .set("loadstring", loadstring_fn)
        .map_err(|e| e.to_string())?;

    let nil = Value::Nil;
    real_g
        .set("dofile", nil.clone())
        .map_err(|e| e.to_string())?;
    real_g
        .set("loadfile", nil.clone())
        .map_err(|e| e.to_string())?;
    real_g
        .set("print", nil.clone())
        .map_err(|e| e.to_string())?;

    if let Ok(os_tbl) = real_g.get::<mlua::Table>("os") {
        for pair in os_tbl.clone().pairs::<Value, Value>() {
            if let Ok((k, _)) = pair
                && let Value::String(s) = &k
                && s.as_bytes() != b"clock"
            {
                let _ = os_tbl.set(k, Value::Nil);
            }
        }
    }

    register_bit_module(lua, &real_g).map_err(|e| e.to_string())?;
    register_cjson_module(lua, &real_g).map_err(|e| e.to_string())?;
    register_cmsgpack_module(lua, &real_g).map_err(|e| e.to_string())?;

    let real_redis = register_redis_module(lua, &real_g, db, aof, read_only, script_resp_ver)
        .map_err(|e| e.to_string())?;
    lua.set_named_registry_value("__real_redis", real_redis)
        .map_err(|e| e.to_string())?;

    lua.set_named_registry_value("__real_G", real_g.clone())
        .map_err(|e| e.to_string())?;

    let setup_sandbox: mlua::Function = lua
        .load(
            r#"
        local real_g = ...
        local globals = _G
        local setmetatable = setmetatable
        local getmetatable = getmetatable
        local error = error
        local pairs = pairs
        local type = type
        local tostring = tostring

        local str_proxy = {}
        local str_proxy_mt = {
            __index = real_g.string,
            __newindex = function() error("Attempt to modify a readonly table", 2) end
        }
        setmetatable(str_proxy, str_proxy_mt)

        local g_proxy = {}
        local g_proxy_mt = {
            __newindex = function() error("Attempt to modify a readonly table", 2) end
        }
        setmetatable(g_proxy, g_proxy_mt)

        local orig_getmetatable = real_g.getmetatable
        local function safe_getmetatable(t)
            if t == globals then
                return g_proxy
            end
            if type(t) == "string" then
                return str_proxy
            end
            return orig_getmetatable(t)
        end
        real_g.getmetatable = safe_getmetatable

        local orig_setmetatable = real_g.setmetatable
        local function safe_setmetatable(t, mt)
            if t == globals then
                error("Attempt to modify a readonly table", 2)
            end
            return orig_setmetatable(t, mt)
        end
        real_g.setmetatable = safe_setmetatable

        for k in pairs(globals) do
            globals[k] = nil
        end

        local g_mt = {
            __index = function(t, k)
                if k == "_G" then return globals end
                local v = real_g[k]
                if v ~= nil then return v end
                error(string.format("Script attempted to access nonexistent global variable '%s'", tostring(k)), 2)
            end,
            __newindex = function(t, k, v)
                error("Attempt to modify a readonly table", 2)
            end
        }
        local g_mt_meta = {
            __newindex = function() error("Attempt to modify a readonly table", 2) end
        }
        setmetatable(g_mt, g_mt_meta)
        setmetatable(globals, g_mt)
        "#,
        )
        .into_function()
        .map_err(|e| e.to_string())?;

    setup_sandbox
        .call::<()>(real_g)
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn make_table_readonly_proxy(lua: &Lua, tbl: mlua::Table) -> mlua::Result<mlua::Table> {
    let make_fn: mlua::Function = lua
        .load(
            r#"
        local tbl = ...
        local proxy = {}
        local mt = {
            __index = tbl,
            __newindex = function() error("Attempt to modify a readonly table", 2) end,
            __pairs = function() return pairs(tbl) end,
            __len = function() return #tbl end
        }
        local mt_meta = {
            __newindex = function() error("Attempt to modify a readonly table", 2) end
        }
        setmetatable(mt, mt_meta)
        setmetatable(proxy, mt)
        return proxy
        "#,
        )
        .into_function()?;
    make_fn.call(tbl)
}

fn register_bit_module(lua: &Lua, real_g: &mlua::Table) -> mlua::Result<()> {
    let bit = lua.create_table()?;

    bit.set(
        "tobit",
        lua.create_function(|_lua, x: f64| Ok(x as i64 as i32))?,
    )?;

    bit.set(
        "tohex",
        lua.create_function(|_lua, (val, n): (i64, Option<i64>)| {
            let n = n.unwrap_or(8);
            let uval = (val as i32) as u32;
            let uppercase = n < 0;
            let abs_n = if n == i64::MIN || n <= -8 {
                8
            } else if n < 0 {
                (-n) as usize
            } else if n > 8 {
                8
            } else if n < 1 {
                1
            } else {
                n as usize
            };
            let mask = if abs_n == 8 {
                0xffff_ffff
            } else {
                (1u32 << (abs_n * 4)) - 1
            };
            let masked = uval & mask;
            if uppercase {
                Ok(format!("{:0width$X}", masked, width = abs_n))
            } else {
                Ok(format!("{:0width$x}", masked, width = abs_n))
            }
        })?,
    )?;

    bit.set(
        "bnot",
        lua.create_function(|_lua, v: Value| {
            let n = val_to_i32(&v).unwrap_or(0);
            Ok(!n)
        })?,
    )?;

    bit.set(
        "band",
        lua.create_function(|_lua, args: MultiValue| {
            let mut res = -1i32;
            for v in args {
                if let Some(n) = val_to_i32(&v) {
                    res &= n;
                }
            }
            Ok(res)
        })?,
    )?;

    bit.set(
        "bor",
        lua.create_function(|_lua, args: MultiValue| {
            let mut res = 0i32;
            for v in args {
                if let Some(n) = val_to_i32(&v) {
                    res |= n;
                }
            }
            Ok(res)
        })?,
    )?;

    bit.set(
        "bxor",
        lua.create_function(|_lua, args: MultiValue| {
            let mut res = 0i32;
            for v in args {
                if let Some(n) = val_to_i32(&v) {
                    res ^= n;
                }
            }
            Ok(res)
        })?,
    )?;

    bit.set(
        "lshift",
        lua.create_function(|_lua, (a, b): (i64, i64)| {
            let a = a as i32 as u32;
            let b = (b as u32) & 31;
            Ok((a.wrapping_shl(b)) as i32)
        })?,
    )?;

    bit.set(
        "rshift",
        lua.create_function(|_lua, (a, b): (i64, i64)| {
            let a = a as i32 as u32;
            let b = (b as u32) & 31;
            Ok((a.wrapping_shr(b)) as i32)
        })?,
    )?;

    bit.set(
        "arshift",
        lua.create_function(|_lua, (a, b): (i64, i64)| {
            let a = a as i32;
            let b = (b as u32) & 31;
            Ok(a.wrapping_shr(b))
        })?,
    )?;

    bit.set(
        "rol",
        lua.create_function(|_lua, (a, b): (i64, i64)| {
            let a = a as i32 as u32;
            let b = (b as u32) & 31;
            Ok(a.rotate_left(b) as i32)
        })?,
    )?;

    bit.set(
        "ror",
        lua.create_function(|_lua, (a, b): (i64, i64)| {
            let a = a as i32 as u32;
            let b = (b as u32) & 31;
            Ok(a.rotate_right(b) as i32)
        })?,
    )?;

    bit.set(
        "bswap",
        lua.create_function(|_lua, a: i64| {
            let a = (a as i32 as u32).swap_bytes();
            Ok(a as i32)
        })?,
    )?;

    let proxy = make_table_readonly_proxy(lua, bit)?;
    real_g.set("bit", proxy)?;
    Ok(())
}

fn val_to_i32(v: &Value) -> Option<i32> {
    match v {
        Value::Integer(i) => Some(*i as i32),
        Value::Number(n) => Some(*n as i64 as i32),
        _ => None,
    }
}

#[derive(Clone, Debug)]
struct CJsonConfig {
    decode_array_with_array_mt: bool,
    encode_invalid_numbers: bool,
    encode_max_depth: usize,
    decode_max_depth: usize,
}

impl Default for CJsonConfig {
    fn default() -> Self {
        Self {
            decode_array_with_array_mt: false,
            encode_invalid_numbers: false,
            encode_max_depth: 1000,
            decode_max_depth: 1000,
        }
    }
}

fn register_cjson_module(lua: &Lua, real_g: &mlua::Table) -> mlua::Result<()> {
    let cfg = Rc::new(RefCell::new(CJsonConfig::default()));
    let cjson = lua.create_table()?;

    // cjson.null
    let null_val = lua.create_table()?;
    let null_mt = lua.create_table()?;
    null_mt.set(
        "__tostring",
        lua.create_function(|_lua, ()| Ok("null".to_string()))?,
    )?;
    let _ = null_val.set_metatable(Some(null_mt));
    cjson.set("null", null_val.clone())?;

    // array metatable for decode_array_with_array_mt
    let array_mt = lua.create_table()?;
    let array_mt_meta = lua.create_table()?;
    let array_mt_index = lua.create_table()?;
    array_mt_index.set("__is_cjson_array", true)?;
    array_mt_meta.set("__index", array_mt_index)?;
    let err_fn: mlua::Function = lua
        .load("error('Attempt to modify a readonly table', 2)")
        .into_function()?;
    array_mt_meta.set("__newindex", err_fn)?;
    let _ = array_mt.set_metatable(Some(array_mt_meta));

    // cjson.decode
    let cfg_dec = cfg.clone();
    let null_dec = Value::Table(null_val.clone());
    let array_mt_dec = array_mt.clone();
    let decode_fn = lua.create_function(move |lua, json_str: mlua::LuaString| {
        let s = json_str.to_str()?;
        let parsed: serde_json::Value = serde_json::from_str(&s).map_err(|e| {
            mlua::Error::RuntimeError(format!("Expected value but found invalid token: {}", e))
        })?;
        json_val_to_lua(lua, parsed, &cfg_dec.borrow(), &null_dec, &array_mt_dec, 1)
    })?;
    cjson.set("decode", decode_fn)?;

    // cjson.encode
    let cfg_enc = cfg.clone();
    let null_enc = Value::Table(null_val.clone());
    let encode_fn = lua.create_function(move |_lua, val: Value| {
        lua_val_to_json_str(&val, &cfg_enc.borrow(), &null_enc, 1)
            .map_err(mlua::Error::RuntimeError)
    })?;
    cjson.set("encode", encode_fn)?;

    // cjson.decode_array_with_array_mt
    let cfg_mt = cfg.clone();
    cjson.set(
        "decode_array_with_array_mt",
        lua.create_function(move |_lua, enable: Option<bool>| {
            cfg_mt.borrow_mut().decode_array_with_array_mt = enable.unwrap_or(true);
            Ok(())
        })?,
    )?;

    // cjson.encode_keep_buffer
    cjson.set(
        "encode_keep_buffer",
        lua.create_function(|_lua, _enable: Option<bool>| Ok(()))?,
    )?;

    // cjson.encode_max_depth
    let cfg_emd = cfg.clone();
    cjson.set(
        "encode_max_depth",
        lua.create_function(move |_lua, depth: usize| {
            cfg_emd.borrow_mut().encode_max_depth = depth;
            Ok(())
        })?,
    )?;

    // cjson.decode_max_depth
    let cfg_dmd = cfg.clone();
    cjson.set(
        "decode_max_depth",
        lua.create_function(move |_lua, depth: usize| {
            cfg_dmd.borrow_mut().decode_max_depth = depth;
            Ok(())
        })?,
    )?;

    // cjson.encode_invalid_numbers
    let cfg_ein = cfg.clone();
    cjson.set(
        "encode_invalid_numbers",
        lua.create_function(move |_lua, enable: Option<bool>| {
            cfg_ein.borrow_mut().encode_invalid_numbers = enable.unwrap_or(true);
            Ok(())
        })?,
    )?;

    let proxy = make_table_readonly_proxy(lua, cjson)?;
    real_g.set("cjson", proxy)?;
    Ok(())
}

fn json_val_to_lua(
    lua: &Lua,
    val: serde_json::Value,
    cfg: &CJsonConfig,
    null_val: &Value,
    array_mt: &mlua::Table,
    depth: usize,
) -> mlua::Result<Value> {
    match val {
        serde_json::Value::Null => Ok(null_val.clone()),
        serde_json::Value::Bool(b) => Ok(Value::Boolean(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else if let Some(u) = n.as_u64() {
                Ok(Value::Integer(u as i64))
            } else if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f >= (i64::MIN as f64) && f <= (i64::MAX as f64) {
                    Ok(Value::Integer(f as i64))
                } else {
                    Ok(Value::Number(f))
                }
            } else {
                Ok(Value::Nil)
            }
        }
        serde_json::Value::String(s) => {
            let ls = lua.create_string(&s)?;
            Ok(Value::String(ls))
        }
        serde_json::Value::Array(arr) => {
            if depth > cfg.decode_max_depth {
                return Err(mlua::Error::RuntimeError(
                    "Cannot parse JSON: nesting too deep".to_string(),
                ));
            }
            let tbl = lua.create_table()?;
            if cfg.decode_array_with_array_mt {
                let _ = tbl.set_metatable(Some(array_mt.clone()));
            }
            for (i, elem) in arr.into_iter().enumerate() {
                let elem_depth = match &elem {
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => depth + 1,
                    _ => depth,
                };
                let lv = json_val_to_lua(lua, elem, cfg, null_val, array_mt, elem_depth)?;
                tbl.raw_set(i + 1, lv)?;
            }
            Ok(Value::Table(tbl))
        }
        serde_json::Value::Object(map) => {
            if depth > cfg.decode_max_depth {
                return Err(mlua::Error::RuntimeError(
                    "Cannot parse JSON: nesting too deep".to_string(),
                ));
            }
            let tbl = lua.create_table()?;
            for (k, v) in map {
                let val_depth = match &v {
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => depth + 1,
                    _ => depth,
                };
                let lv = json_val_to_lua(lua, v, cfg, null_val, array_mt, val_depth)?;
                tbl.raw_set(k, lv)?;
            }
            Ok(Value::Table(tbl))
        }
    }
}

fn lua_val_to_json_str(
    val: &Value,
    cfg: &CJsonConfig,
    null_val: &Value,
    depth: usize,
) -> Result<String, String> {
    match val {
        Value::Nil => Ok("null".to_string()),
        Value::Boolean(b) => Ok(if *b {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Number(n) => {
            if n.is_nan() || n.is_infinite() {
                if !cfg.encode_invalid_numbers {
                    return Err("Cannot serialise number: must not be NaN or Inf".to_string());
                } else {
                    return Ok("null".to_string());
                }
            }
            if n.fract() == 0.0 && *n >= (i64::MIN as f64) && *n <= (i64::MAX as f64) {
                Ok((*n as i64).to_string())
            } else {
                Ok(n.to_string())
            }
        }
        Value::String(s) => {
            let s_bytes = s.as_bytes();
            let s_str = String::from_utf8_lossy(&s_bytes);
            Ok(serde_json::to_string(&s_str).unwrap())
        }
        Value::Table(t) => {
            if val == null_val {
                return Ok("null".to_string());
            }
            if depth > cfg.encode_max_depth {
                return Err("Cannot serialise table: nesting too deep".to_string());
            }
            let is_array = if let Some(mt) = t.metatable() {
                mt.get::<bool>("__is_cjson_array").unwrap_or(false)
            } else {
                false
            };

            let mut pairs = Vec::new();
            for pair in t.clone().pairs::<Value, Value>() {
                let (k, v) = pair.map_err(|e| e.to_string())?;
                pairs.push((k, v));
            }

            if is_array {
                let mut items = Vec::new();
                for i in 1..=t.raw_len() {
                    let elem: Value = t.raw_get(i).unwrap_or(Value::Nil);
                    let elem_depth = match &elem {
                        Value::Table(_) => depth + 1,
                        _ => depth,
                    };
                    items.push(lua_val_to_json_str(&elem, cfg, null_val, elem_depth)?);
                }
                return Ok(format!("[{}]", items.join(",")));
            }

            for (k, _) in &pairs {
                match k {
                    Value::Integer(_) => {}
                    Value::Number(n) if n.fract() == 0.0 => {}
                    Value::String(_) => {}
                    _ => {
                        return Err("Cannot serialise table: invalid key type".to_string());
                    }
                }
            }

            let len = t.raw_len();
            let all_int = pairs.iter().all(|(k, _)| match k {
                Value::Integer(_) => true,
                Value::Number(n) if n.fract() == 0.0 => true,
                _ => false,
            });

            if len > 0 && all_int && pairs.len() == len {
                let mut items = Vec::new();
                for i in 1..=len {
                    let elem: Value = t.raw_get(i).unwrap_or(Value::Nil);
                    let elem_depth = match &elem {
                        Value::Table(_) => depth + 1,
                        _ => depth,
                    };
                    items.push(lua_val_to_json_str(&elem, cfg, null_val, elem_depth)?);
                }
                Ok(format!("[{}]", items.join(",")))
            } else if pairs.is_empty() {
                Ok("{}".to_string())
            } else {
                let mut entries = Vec::new();
                for (k, v) in pairs {
                    let key_str = match k {
                        Value::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                        Value::Integer(i) => i.to_string(),
                        Value::Number(n) => (n as i64).to_string(),
                        _ => return Err("Cannot serialise table: invalid key type".to_string()),
                    };
                    let key_json = serde_json::to_string(&key_str).unwrap();
                    let val_depth = match &v {
                        Value::Table(_) => depth + 1,
                        _ => depth,
                    };
                    let val_json = lua_val_to_json_str(&v, cfg, null_val, val_depth)?;
                    entries.push(format!("{}:{}", key_json, val_json));
                }
                Ok(format!("{{{}}}", entries.join(",")))
            }
        }
        _ => Ok("null".to_string()),
    }
}

fn register_cmsgpack_module(lua: &Lua, real_g: &mlua::Table) -> mlua::Result<()> {
    let cmsgpack = lua.create_table()?;

    // pack
    cmsgpack.set(
        "pack",
        lua.create_function(|lua, margs: MultiValue| {
            let mut out = Vec::new();
            for v in margs {
                pack_lua_val(&v, &mut out, 1)?;
            }
            let s = lua.create_string(&out)?;
            Ok(Value::String(s))
        })?,
    )?;

    // unpack
    cmsgpack.set(
        "unpack",
        lua.create_function(|lua, data: mlua::LuaString| {
            let bytes = data.as_bytes();
            let mut off = 0;
            unpack_msgpack_val(lua, &bytes, &mut off)
        })?,
    )?;

    // unpack_one
    cmsgpack.set(
        "unpack_one",
        lua.create_function(|lua, (data, offset): (mlua::LuaString, usize)| {
            let bytes = data.as_bytes();
            let mut off = offset;
            let val = unpack_msgpack_val(lua, &bytes, &mut off)?;
            let next_off = if off >= bytes.len() { -1 } else { off as i64 };
            Ok((next_off, val))
        })?,
    )?;

    // unpack_limit
    cmsgpack.set(
        "unpack_limit",
        lua.create_function(
            |lua, (data, limit, offset): (mlua::LuaString, usize, usize)| {
                let bytes = data.as_bytes();
                let mut off = offset;
                let mut results = Vec::new();
                for _ in 0..limit {
                    if off >= bytes.len() {
                        break;
                    }
                    let val = unpack_msgpack_val(lua, &bytes, &mut off)?;
                    results.push(val);
                }
                let next_off = if off >= bytes.len() { -1 } else { off as i64 };
                let mut ret = Vec::with_capacity(results.len() + 1);
                ret.push(Value::Integer(next_off));
                ret.extend(results);
                Ok(MultiValue::from_vec(ret))
            },
        )?,
    )?;

    let proxy = make_table_readonly_proxy(lua, cmsgpack)?;
    real_g.set("cmsgpack", proxy)?;
    Ok(())
}

fn pack_lua_val(val: &Value, out: &mut Vec<u8>, depth: usize) -> mlua::Result<()> {
    match val {
        Value::Nil => out.push(0xc0),
        Value::Boolean(false) => out.push(0xc2),
        Value::Boolean(true) => out.push(0xc3),
        Value::Integer(i) => {
            pack_integer(*i, out);
        }
        Value::Number(n) => {
            if n.fract() == 0.0 && *n >= (i64::MIN as f64) && *n <= (u64::MAX as f64) {
                if *n >= 0.0 && *n > (i64::MAX as f64) {
                    out.push(0xcf);
                    out.extend_from_slice(&(*n as u64).to_be_bytes());
                } else {
                    pack_integer(*n as i64, out);
                }
            } else {
                out.push(0xcb);
                out.extend_from_slice(&n.to_be_bytes());
            }
        }
        Value::String(s) => {
            let bytes = s.as_bytes();
            let len = bytes.len();
            if len < 32 {
                out.push(0xa0 | (len as u8));
            } else if len <= 255 {
                out.push(0xd9);
                out.push(len as u8);
            } else if len <= 65535 {
                out.push(0xda);
                out.extend_from_slice(&(len as u16).to_be_bytes());
            } else {
                out.push(0xdb);
                out.extend_from_slice(&(len as u32).to_be_bytes());
            }
            out.extend_from_slice(&bytes);
        }
        Value::Table(t) => {
            if depth > 16 {
                out.push(0xc0);
                return Ok(());
            }
            let len = t.raw_len();
            let mut pairs = Vec::new();
            for p in t.clone().pairs::<Value, Value>() {
                pairs.push(p?);
            }
            if len > 0
                && pairs.len() == len
                && (1..=len).all(|i| t.raw_get::<Value>(i).is_ok_and(|v| v != Value::Nil))
            {
                if len <= 15 {
                    out.push(0x90 | (len as u8));
                } else if len <= 65535 {
                    out.push(0xdc);
                    out.extend_from_slice(&(len as u16).to_be_bytes());
                } else {
                    out.push(0xdd);
                    out.extend_from_slice(&(len as u32).to_be_bytes());
                }
                for i in 1..=len {
                    let elem: Value = t.raw_get(i).unwrap_or(Value::Nil);
                    let elem_depth = match &elem {
                        Value::Table(_) => depth + 1,
                        _ => depth,
                    };
                    pack_lua_val(&elem, out, elem_depth)?;
                }
            } else {
                let mlen = pairs.len();
                if mlen <= 15 {
                    out.push(0x80 | (mlen as u8));
                } else if mlen <= 65535 {
                    out.push(0xde);
                    out.extend_from_slice(&(mlen as u16).to_be_bytes());
                } else {
                    out.push(0xdf);
                    out.extend_from_slice(&(mlen as u32).to_be_bytes());
                }
                pairs.sort_by(|(k1, _), (k2, _)| {
                    let b1 = match k1 {
                        Value::String(s) => s.as_bytes(),
                        _ => return std::cmp::Ordering::Equal,
                    };
                    let b2 = match k2 {
                        Value::String(s) => s.as_bytes(),
                        _ => return std::cmp::Ordering::Equal,
                    };
                    b2.as_ref().cmp(b1.as_ref())
                });

                for (k, v) in pairs {
                    pack_lua_val(&k, out, depth)?;
                    let v_depth = match &v {
                        Value::Table(_) => depth + 1,
                        _ => depth,
                    };
                    pack_lua_val(&v, out, v_depth)?;
                }
            }
        }
        _ => out.push(0xc0),
    }
    Ok(())
}

fn pack_integer(i: i64, out: &mut Vec<u8>) {
    if (0..=127).contains(&i) {
        out.push(i as u8);
    } else if (-32..0).contains(&i) {
        out.push(i as i8 as u8);
    } else if (0..=255).contains(&i) {
        out.push(0xcc);
        out.push(i as u8);
    } else if (-128..0).contains(&i) {
        out.push(0xd0);
        out.push(i as i8 as u8);
    } else if (0..=65535).contains(&i) {
        out.push(0xcd);
        out.extend_from_slice(&(i as u16).to_be_bytes());
    } else if (-32768..0).contains(&i) {
        out.push(0xd1);
        out.extend_from_slice(&(i as i16).to_be_bytes());
    } else if i >= 0 && i <= u32::MAX as i64 {
        out.push(0xce);
        out.extend_from_slice(&(i as u32).to_be_bytes());
    } else if i >= i32::MIN as i64 && i < 0 {
        out.push(0xd2);
        out.extend_from_slice(&(i as i32).to_be_bytes());
    } else if i < 0 {
        out.push(0xd3);
        out.extend_from_slice(&i.to_be_bytes());
    } else {
        out.push(0xcf);
        out.extend_from_slice(&(i as u64).to_be_bytes());
    }
}

fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, len: usize) -> mlua::Result<&'a [u8]> {
    if *offset + len > buf.len() {
        return Err(mlua::Error::RuntimeError(
            "unexpected end of msgpack data".to_string(),
        ));
    }
    let slice = &buf[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

fn read_u8(buf: &[u8], offset: &mut usize) -> mlua::Result<u8> {
    Ok(read_bytes(buf, offset, 1)?[0])
}

fn read_u16(buf: &[u8], offset: &mut usize) -> mlua::Result<u16> {
    let b = read_bytes(buf, offset, 2)?;
    Ok(u16::from_be_bytes([b[0], b[1]]))
}

fn read_u32(buf: &[u8], offset: &mut usize) -> mlua::Result<u32> {
    let b = read_bytes(buf, offset, 4)?;
    Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u64(buf: &[u8], offset: &mut usize) -> mlua::Result<u64> {
    let b = read_bytes(buf, offset, 8)?;
    Ok(u64::from_be_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn unpack_msgpack_raw(
    lua: &Lua,
    buf: &[u8],
    offset: &mut usize,
    len: usize,
) -> mlua::Result<Value> {
    let slice = read_bytes(buf, offset, len)?;
    let s = lua.create_string(slice)?;
    Ok(Value::String(s))
}

fn unpack_msgpack_array(
    lua: &Lua,
    buf: &[u8],
    offset: &mut usize,
    len: usize,
) -> mlua::Result<Value> {
    let tbl = lua.create_table()?;
    for i in 1..=len {
        let v = unpack_msgpack_val(lua, buf, offset)?;
        tbl.raw_set(i, v)?;
    }
    Ok(Value::Table(tbl))
}

fn unpack_msgpack_map(
    lua: &Lua,
    buf: &[u8],
    offset: &mut usize,
    len: usize,
) -> mlua::Result<Value> {
    let tbl = lua.create_table()?;
    for _ in 0..len {
        let k = unpack_msgpack_val(lua, buf, offset)?;
        let v = unpack_msgpack_val(lua, buf, offset)?;
        if k != Value::Nil {
            tbl.raw_set(k, v)?;
        }
    }
    Ok(Value::Table(tbl))
}

fn unpack_msgpack_val(lua: &Lua, buf: &[u8], offset: &mut usize) -> mlua::Result<Value> {
    if *offset >= buf.len() {
        return Err(mlua::Error::RuntimeError(
            "unexpected end of msgpack data".to_string(),
        ));
    }
    let b = buf[*offset];
    *offset += 1;
    match b {
        0x00..=0x7f => Ok(Value::Integer(b as i64)),
        0xe0..=0xff => Ok(Value::Integer((b as i8) as i64)),
        0xc0 => Ok(Value::Nil),
        0xc2 => Ok(Value::Boolean(false)),
        0xc3 => Ok(Value::Boolean(true)),
        0x80..=0x8f => {
            let len = (b & 0x0f) as usize;
            unpack_msgpack_map(lua, buf, offset, len)
        }
        0x90..=0x9f => {
            let len = (b & 0x0f) as usize;
            unpack_msgpack_array(lua, buf, offset, len)
        }
        0xa0..=0xbf => {
            let len = (b & 0x1f) as usize;
            unpack_msgpack_raw(lua, buf, offset, len)
        }
        0xcc => {
            let val = read_u8(buf, offset)?;
            Ok(Value::Integer(val as i64))
        }
        0xcd => {
            let val = read_u16(buf, offset)?;
            Ok(Value::Integer(val as i64))
        }
        0xce => {
            let val = read_u32(buf, offset)?;
            Ok(Value::Integer(val as i64))
        }
        0xcf => {
            let val = read_u64(buf, offset)?;
            Ok(Value::Integer(val as i64))
        }
        0xd0 => {
            let val = read_u8(buf, offset)? as i8;
            Ok(Value::Integer(val as i64))
        }
        0xd1 => {
            let val = read_u16(buf, offset)? as i16;
            Ok(Value::Integer(val as i64))
        }
        0xd2 => {
            let val = read_u32(buf, offset)? as i32;
            Ok(Value::Integer(val as i64))
        }
        0xd3 => {
            let val = read_u64(buf, offset)? as i64;
            Ok(Value::Integer(val))
        }
        0xca => {
            let val = f32::from_be_bytes(read_bytes(buf, offset, 4)?.try_into().unwrap());
            Ok(Value::Number(val as f64))
        }
        0xcb => {
            let val = f64::from_be_bytes(read_bytes(buf, offset, 8)?.try_into().unwrap());
            Ok(Value::Number(val))
        }
        0xd9 => {
            let len = read_u8(buf, offset)? as usize;
            unpack_msgpack_raw(lua, buf, offset, len)
        }
        0xda => {
            let len = read_u16(buf, offset)? as usize;
            unpack_msgpack_raw(lua, buf, offset, len)
        }
        0xdb => {
            let len = read_u32(buf, offset)? as usize;
            unpack_msgpack_raw(lua, buf, offset, len)
        }
        0xdc => {
            let len = read_u16(buf, offset)? as usize;
            unpack_msgpack_array(lua, buf, offset, len)
        }
        0xdd => {
            let len = read_u32(buf, offset)? as usize;
            unpack_msgpack_array(lua, buf, offset, len)
        }
        0xde => {
            let len = read_u16(buf, offset)? as usize;
            unpack_msgpack_map(lua, buf, offset, len)
        }
        0xdf => {
            let len = read_u32(buf, offset)? as usize;
            unpack_msgpack_map(lua, buf, offset, len)
        }
        _ => Ok(Value::Nil),
    }
}

fn register_redis_module(
    lua: &Lua,
    real_g: &mlua::Table,
    db: &Rc<RefCell<crate::shard::ShardDb>>,
    aof: Option<*const RefCell<crate::aof::AofWriter>>,
    read_only: bool,
    script_resp_ver: Rc<RefCell<u8>>,
) -> mlua::Result<mlua::Table> {
    let redis = lua.create_table()?;

    let srv_set = script_resp_ver.clone();
    redis.set(
        "setresp",
        lua.create_function(move |_lua, ver: u8| {
            *srv_set.borrow_mut() = ver;
            Ok(())
        })?,
    )?;

    let db_call = db.clone();
    let aof_call = aof;
    let srv_call = script_resp_ver.clone();
    let port = db.borrow().port;
    let call_fn = lua.create_function(move |lua, margs: MultiValue| {
        let mut cmd_args = Vec::with_capacity(margs.len());
        for v in margs {
            match v {
                Value::String(s) => cmd_args.push(Bytes::copy_from_slice(&s.as_bytes())),
                Value::Integer(i) => cmd_args.push(Bytes::from(i.to_string())),
                Value::Number(n) => cmd_args.push(Bytes::from(n.to_string())),
                _ => {
                    return Err(mlua::Error::RuntimeError(
                        "ERR Lua redis lib command arguments must be strings or integers"
                            .to_string(),
                    ));
                }
            }
        }
        if cmd_args.is_empty() {
            return Err(mlua::Error::RuntimeError(
                "ERR Please specify at least one argument for redis.call()".to_string(),
            ));
        }
        let cmd = match crate::resp::build_command(cmd_args) {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Err(mlua::Error::RuntimeError(
                    "ERR Please specify at least one argument for redis.call()".to_string(),
                ));
            }
            Err(e) => {
                if e.contains("wrong number of arguments") {
                    return Err(mlua::Error::RuntimeError(
                        "ERR Wrong number of args calling Redis command from script".to_string(),
                    ));
                } else {
                    return Err(mlua::Error::RuntimeError(format!("ERR {}", e)));
                }
            }
        };
        if let Command::Unknown(_) = &cmd {
            return Err(mlua::Error::RuntimeError(
                "Unknown Redis command called from script".to_string(),
            ));
        }
        if let Some(err) = script_acl_denied(port, &cmd) {
            crate::connection::record_rejected_stat(crate::connection::get_cmd_name(&cmd));
            crate::connection::record_error_stat("NOPERM", None);
            SCRIPT_RECORDED_ERROR.set(true);
            return Err(mlua::Error::RuntimeError(err));
        }
        if crate::connection::script_touches_non_local_key(&cmd) {
            crate::connection::record_rejected_stat(crate::connection::get_cmd_name(&cmd));
            crate::connection::record_error_stat("ERR", None);
            SCRIPT_RECORDED_ERROR.set(true);
            return Err(mlua::Error::RuntimeError(
                crate::connection::SCRIPT_NON_LOCAL_KEY_ERR.to_string(),
            ));
        }
        match &cmd {
            Command::Cluster(_)
            | Command::Replicaof { .. }
            | Command::Shutdown { .. }
            | Command::Save
            | Command::Bgsave
            | Command::Bgrewriteaof => {
                return Err(mlua::Error::RuntimeError(
                    "ERR This Redis command is not allowed from script".to_string(),
                ));
            }
            _ => {}
        }
        if crate::connection::MIN_REPLICAS_TO_WRITE.load(std::sync::atomic::Ordering::Relaxed) > 0
            && cmd.is_write_command()
        {
            return Err(mlua::Error::RuntimeError(
                "NOREPLICAS Not enough good replicas to write.".to_string(),
            ));
        }
        let cmd_name = crate::connection::get_cmd_name(&cmd);
        if read_only
            && (cmd.is_write_command()
                || matches!(
                    cmd,
                    Command::Publish { .. } | Command::Spublish { .. } | Command::Pfcount { .. }
                ))
        {
            crate::connection::record_rejected_stat(cmd_name);
            crate::connection::record_error_stat("ERR", None);
            SCRIPT_RECORDED_ERROR.set(true);
            return Err(mlua::Error::RuntimeError(
                "ERR Write commands are not allowed from read-only scripts.".to_string(),
            ));
        }
        let max_mem = crate::tiering::get_max_memory(port);
        if max_mem > 0
            && crate::connection::get_max_memory_policy() == "noeviction"
            && cmd.is_write_command()
            && !cmd.allows_oom()
        {
            let used = db_call.borrow().table.used_memory;
            let shard_max = max_mem as usize;
            if used > shard_max {
                crate::connection::record_rejected_stat(cmd_name);
                crate::connection::record_error_stat("OOM", None);
                SCRIPT_RECORDED_ERROR.set(true);
                return Err(mlua::Error::RuntimeError(
                    "OOM command not allowed when used memory > 'maxmemory'.".to_string(),
                ));
            }
        }

        crate::connection::record_cmd_stat(cmd_name);
        if cmd.is_write_command() {
            crate::snapshot::note_write();
        }
        crate::connection::inc_active_client_tot_cmds();
        if crate::connection::has_monitor_clients() {
            let monitor_argv = crate::slowlog::command_to_monitor_argv(&cmd);
            crate::connection::broadcast_monitor(port, "lua", &monitor_argv);
        }
        let mut out = Vec::new();
        let aof_ref = unsafe { aof_call.map(|ptr| &*ptr) };
        crate::connection::execute_local_command(
            &cmd,
            &mut db_call.borrow_mut(),
            &mut out,
            aof_ref,
        );

        if out.starts_with(b"-") {
            crate::connection::record_failed_stat(cmd_name);
            let err_line = String::from_utf8_lossy(&out[1..out.len().saturating_sub(2)]);
            let end_idx = err_line.find([' ', '\r', '\n']).unwrap_or(err_line.len());
            let prefix = &err_line[..end_idx];
            crate::connection::record_error_stat(prefix, None);
            SCRIPT_RECORDED_ERROR.set(true);
            if err_line.contains("wrong number of arguments") {
                return Err(mlua::Error::RuntimeError(
                    "ERR Wrong number of args calling Redis command from script".to_string(),
                ));
            } else {
                return Err(mlua::Error::RuntimeError(err_line.to_string()));
            }
        }

        let is_hgetall = matches!(cmd, Command::Hgetall(_));
        resp_bytes_to_lua(lua, &out, is_hgetall && *srv_call.borrow() == 3)
    })?;
    redis.set("call", call_fn)?;

    let db_pcall = db.clone();
    let aof_pcall = aof;
    let srv_pcall = script_resp_ver.clone();
    let pcall_fn = lua.create_function(move |lua, margs: MultiValue| {
        let mut cmd_args = Vec::with_capacity(margs.len());
        for v in margs {
            match v {
                Value::String(s) => cmd_args.push(Bytes::copy_from_slice(&s.as_bytes())),
                Value::Integer(i) => cmd_args.push(Bytes::from(i.to_string())),
                Value::Number(n) => cmd_args.push(Bytes::from(n.to_string())),
                _ => {
                    let tbl = lua.create_table()?;
                    tbl.set(
                        "err",
                        "ERR Lua redis lib command arguments must be strings or integers",
                    )?;
                    return Ok(Value::Table(tbl));
                }
            }
        }
        if cmd_args.is_empty() {
            let tbl = lua.create_table()?;
            tbl.set(
                "err",
                "ERR Please specify at least one argument for redis.pcall()",
            )?;
            return Ok(Value::Table(tbl));
        }
        let cmd = match crate::resp::build_command(cmd_args) {
            Ok(Some(c)) => c,
            Ok(None) => {
                let tbl = lua.create_table()?;
                tbl.set(
                    "err",
                    "ERR Please specify at least one argument for redis.pcall()",
                )?;
                return Ok(Value::Table(tbl));
            }
            Err(e) => {
                let tbl = lua.create_table()?;
                if e.contains("wrong number of arguments") {
                    tbl.set(
                        "err",
                        "ERR Wrong number of args calling Redis command from script",
                    )?;
                } else {
                    tbl.set("err", format!("ERR {}", e))?;
                }
                return Ok(Value::Table(tbl));
            }
        };
        if let Command::Unknown(_) = &cmd {
            let tbl = lua.create_table()?;
            tbl.set("err", "ERR Unknown Redis command called from script")?;
            return Ok(Value::Table(tbl));
        }
        if let Some(err) = script_acl_denied(port, &cmd) {
            crate::connection::record_rejected_stat(crate::connection::get_cmd_name(&cmd));
            crate::connection::record_error_stat("NOPERM", None);
            SCRIPT_RECORDED_ERROR.set(true);
            let tbl = lua.create_table()?;
            tbl.set("err", err)?;
            return Ok(Value::Table(tbl));
        }
        if crate::connection::script_touches_non_local_key(&cmd) {
            crate::connection::record_rejected_stat(crate::connection::get_cmd_name(&cmd));
            crate::connection::record_error_stat("ERR", None);
            SCRIPT_RECORDED_ERROR.set(true);
            let tbl = lua.create_table()?;
            tbl.set("err", crate::connection::SCRIPT_NON_LOCAL_KEY_ERR)?;
            return Ok(Value::Table(tbl));
        }
        match &cmd {
            Command::Cluster(_)
            | Command::Replicaof { .. }
            | Command::Shutdown { .. }
            | Command::Save
            | Command::Bgsave
            | Command::Bgrewriteaof => {
                let tbl = lua.create_table()?;
                tbl.set("err", "ERR This Redis command is not allowed from script")?;
                return Ok(Value::Table(tbl));
            }
            _ => {}
        }
        if crate::connection::MIN_REPLICAS_TO_WRITE.load(std::sync::atomic::Ordering::Relaxed) > 0
            && cmd.is_write_command()
        {
            let tbl = lua.create_table()?;
            tbl.set("err", "NOREPLICAS Not enough good replicas to write.")?;
            return Ok(Value::Table(tbl));
        }
        let cmd_name = crate::connection::get_cmd_name(&cmd);
        if read_only
            && (cmd.is_write_command()
                || matches!(
                    cmd,
                    Command::Publish { .. } | Command::Spublish { .. } | Command::Pfcount { .. }
                ))
        {
            crate::connection::record_rejected_stat(cmd_name);
            crate::connection::record_error_stat("ERR", None);
            SCRIPT_RECORDED_ERROR.set(true);
            let tbl = lua.create_table()?;
            tbl.set(
                "err",
                "ERR Write commands are not allowed from read-only scripts.",
            )?;
            return Ok(Value::Table(tbl));
        }
        let max_mem = crate::tiering::get_max_memory(port);
        if max_mem > 0
            && crate::connection::get_max_memory_policy() == "noeviction"
            && cmd.is_write_command()
            && !cmd.allows_oom()
        {
            let used = db_pcall.borrow().table.used_memory;
            let shard_max = max_mem as usize;
            if used > shard_max {
                crate::connection::record_rejected_stat(cmd_name);
                crate::connection::record_error_stat("OOM", None);
                SCRIPT_RECORDED_ERROR.set(true);
                let tbl = lua.create_table()?;
                tbl.set(
                    "err",
                    "OOM command not allowed when used memory > 'maxmemory'.",
                )?;
                return Ok(Value::Table(tbl));
            }
        }

        crate::connection::record_cmd_stat(cmd_name);
        if cmd.is_write_command() {
            crate::snapshot::note_write();
        }
        crate::connection::inc_active_client_tot_cmds();
        if crate::connection::has_monitor_clients() {
            let monitor_argv = crate::slowlog::command_to_monitor_argv(&cmd);
            crate::connection::broadcast_monitor(port, "lua", &monitor_argv);
        }
        let mut out = Vec::new();
        let aof_ref = unsafe { aof_pcall.map(|ptr| &*ptr) };
        crate::connection::execute_local_command(
            &cmd,
            &mut db_pcall.borrow_mut(),
            &mut out,
            aof_ref,
        );

        if out.starts_with(b"-") {
            crate::connection::record_failed_stat(cmd_name);
            let mut err_str =
                String::from_utf8_lossy(&out[1..out.len().saturating_sub(2)]).to_string();
            let end_idx = err_str.find([' ', '\r', '\n']).unwrap_or(err_str.len());
            let prefix = &err_str[..end_idx];
            crate::connection::record_error_stat(prefix, None);
            SCRIPT_RECORDED_ERROR.set(true);
            if err_str.contains("wrong number of arguments") {
                err_str = "ERR Wrong number of args calling Redis command from script".to_string();
            }
            let tbl = lua.create_table()?;
            tbl.set("err", err_str)?;
            Ok(Value::Table(tbl))
        } else {
            let is_hgetall = matches!(cmd, Command::Hgetall(_));
            resp_bytes_to_lua(lua, &out, is_hgetall && *srv_pcall.borrow() == 3)
        }
    })?;
    redis.set("pcall", pcall_fn)?;

    let status_reply = lua.create_function(|lua, msg: String| {
        let tbl = lua.create_table()?;
        tbl.set("ok", msg)?;
        Ok(Value::Table(tbl))
    })?;
    redis.set("status_reply", status_reply)?;

    let error_reply = lua.create_function(|lua, msg: String| {
        let clean = msg.replace("\r\n", "  ").replace(['\r', '\n'], " ");
        let final_msg = if clean.is_empty() {
            "ERR".to_string()
        } else {
            clean
        };
        let tbl = lua.create_table()?;
        tbl.set("err", final_msg.clone())?;
        let mt = lua.create_table()?;
        let err_clone = final_msg;
        let tostring_fn = lua.create_function(move |_lua, ()| Ok(err_clone.clone()))?;
        mt.set("__tostring", tostring_fn)?;
        let _ = tbl.set_metatable(Some(mt));
        Ok(Value::Table(tbl))
    })?;
    redis.set("error_reply", error_reply)?;

    let sha1hex_fn = lua.create_function(|_lua, margs: MultiValue| {
        if margs.is_empty() {
            return Err(mlua::Error::RuntimeError(
                "ERR wrong number of arguments for redis.sha1hex()".to_string(),
            ));
        }
        match margs.into_iter().next() {
            Some(Value::String(s)) => Ok(sha1_hex(&s.as_bytes())),
            _ => Err(mlua::Error::RuntimeError(
                "ERR wrong number of arguments for redis.sha1hex()".to_string(),
            )),
        }
    })?;
    redis.set("sha1hex", sha1hex_fn)?;

    let log_fn = lua.create_function(|_lua, (_level, _msg): (i32, String)| Ok(()))?;
    redis.set("log", log_fn)?;
    redis.set("LOG_DEBUG", 0)?;
    redis.set("LOG_VERBOSE", 1)?;
    redis.set("LOG_NOTICE", 2)?;
    redis.set("LOG_WARNING", 3)?;
    redis.set("REDIS_VERSION", "7.2.0")?;
    redis.set("REDIS_VERSION_NUM", 0x00070200i64)?;

    let port = db.borrow().port;
    let acl_check_fn = lua.create_function(move |_lua, margs: MultiValue| {
        let mut cmd_args = Vec::with_capacity(margs.len());
        for v in margs {
            match v {
                Value::String(s) => cmd_args.push(Bytes::copy_from_slice(&s.as_bytes())),
                Value::Integer(i) => cmd_args.push(Bytes::from(i.to_string())),
                Value::Number(n) => cmd_args.push(Bytes::from(n.to_string())),
                _ => {
                    return Err(mlua::Error::RuntimeError(
                        "ERR Lua redis lib command arguments must be strings or integers"
                            .to_string(),
                    ));
                }
            }
        }
        if cmd_args.is_empty() {
            return Err(mlua::Error::RuntimeError(
                "ERR Invalid command passed to redis.acl_check_cmd()".to_string(),
            ));
        }
        let cmd = match crate::resp::build_command(cmd_args) {
            Ok(Some(c)) => c,
            Err(e) if e.contains("wrong number of arguments") => {
                return Err(mlua::Error::RuntimeError(
                    "ERR Wrong number of args calling Redis command from script".to_string(),
                ));
            }
            _ => {
                return Err(mlua::Error::RuntimeError(
                    "ERR Invalid command passed to redis.acl_check_cmd()".to_string(),
                ));
            }
        };
        if let Command::Unknown(_) = &cmd {
            return Err(mlua::Error::RuntimeError(
                "ERR Invalid command passed to redis.acl_check_cmd()".to_string(),
            ));
        }

        if script_acl_denied(port, &cmd).is_none() {
            Ok(Value::Integer(1))
        } else {
            Ok(Value::Nil)
        }
    })?;
    redis.set("acl_check_cmd", acl_check_fn)?;

    let proxy = make_table_readonly_proxy(lua, redis.clone())?;
    real_g.set("redis", proxy)?;
    Ok(redis)
}

/// ACL check for commands issued by a script (Redis scriptVerifyACL): they
/// run with the calling user's command, key and channel permissions. Returns
/// the NOPERM error if denied.
fn script_acl_denied(port: u16, cmd: &Command) -> Option<String> {
    let auth_user = crate::connection::CURRENT_AUTH_USER.with(|u| u.borrow().clone());
    let user_name = if auth_user.is_empty() {
        "default"
    } else {
        auth_user.as_str()
    };
    if !crate::acl::HAS_CUSTOM_ACL.load(std::sync::atomic::Ordering::Relaxed)
        && user_name == "default"
    {
        return None;
    }
    let acl = crate::acl::get_acl_for_port(port);
    let guard = acl.read().unwrap();
    let user = guard.users.get(user_name)?;
    let name = crate::connection::acl_cmd_name(cmd);
    if !user.can_execute_command(name) {
        return Some(format!(
            "NOPERM this user has no permissions to run the '{}' command",
            name.to_lowercase()
        ));
    }
    let need = crate::connection::acl_key_perm(cmd);
    let mut key_denied = false;
    crate::connection::for_each_cmd_key(cmd, |k| key_denied |= !user.can_access_key(k, need));
    if key_denied {
        return Some(
            "NOPERM this user has no permissions to access one of the keys used as arguments"
                .to_string(),
        );
    }
    if crate::connection::acl_channels_denied(user, cmd) {
        return Some(
            "NOPERM this user has no permissions to access one of the channels used as arguments"
                .to_string(),
        );
    }
    None
}

fn resp_bytes_to_lua(lua: &Lua, out: &[u8], is_hgetall_map: bool) -> mlua::Result<Value> {
    if out.is_empty() {
        return Ok(Value::Nil);
    }
    match out[0] {
        b'+' => {
            let end = out.len().saturating_sub(2);
            let s = String::from_utf8_lossy(&out[1..end]);
            let tbl = lua.create_table()?;
            tbl.set("ok", s.to_string())?;
            Ok(Value::Table(tbl))
        }
        b'-' => {
            let end = out.len().saturating_sub(2);
            let s = String::from_utf8_lossy(&out[1..end]);
            let s = if s.contains("wrong number of arguments") {
                "ERR Wrong number of args calling Redis command from script".to_string()
            } else {
                s.to_string()
            };
            Err(mlua::Error::RuntimeError(s))
        }
        b':' => {
            let end = out.len().saturating_sub(2);
            let n = std::str::from_utf8(&out[1..end])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            Ok(Value::Integer(n))
        }
        b'$' => {
            if out.starts_with(b"$-1") {
                Ok(Value::Boolean(false))
            } else {
                let first_crlf = out.iter().position(|&b| b == b'\r').unwrap_or(out.len());
                let data_start = first_crlf + 2;
                let data_end = out.len().saturating_sub(2);
                if data_start <= data_end {
                    let s = lua.create_string(&out[data_start..data_end])?;
                    Ok(Value::String(s))
                } else {
                    Ok(Value::String(lua.create_string(b"")?))
                }
            }
        }
        b'*' => {
            let mut cursor = bytes::BytesMut::from(out);
            if is_hgetall_map {
                parse_resp_array_to_map_lua(lua, &mut cursor)
            } else {
                parse_resp_array_to_lua(lua, &mut cursor)
            }
        }
        _ => Ok(Value::Nil),
    }
}

fn parse_resp_array_to_map_lua(lua: &Lua, buf: &mut bytes::BytesMut) -> mlua::Result<Value> {
    use bytes::Buf;
    if buf.is_empty() || buf[0] != b'*' {
        return Ok(Value::Nil);
    }
    let crlf = match buf.windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos,
        None => return Ok(Value::Nil),
    };
    let count: i64 = std::str::from_utf8(&buf[1..crlf])
        .unwrap_or("-1")
        .parse()
        .unwrap_or(-1);
    buf.advance(crlf + 2);
    if count < 0 {
        return Ok(Value::Boolean(false));
    }
    let map_tbl = lua.create_table()?;
    let mt = lua.create_table()?;
    mt.set("__redis_proto_type", "map")?;
    let _ = map_tbl.set_metatable(Some(mt));

    let pairs_count = count / 2;
    for _ in 0..pairs_count {
        if buf.is_empty() {
            break;
        }
        let k = parse_resp_element(lua, buf)?;
        let v = parse_resp_element(lua, buf)?;
        if k != Value::Nil {
            map_tbl.raw_set(k, v)?;
        }
    }
    Ok(Value::Table(map_tbl))
}

fn parse_resp_element(lua: &Lua, buf: &mut bytes::BytesMut) -> mlua::Result<Value> {
    use bytes::Buf;
    if buf.is_empty() {
        return Ok(Value::Nil);
    }
    match buf[0] {
        b'+' => {
            let c = buf
                .windows(2)
                .position(|w| w == b"\r\n")
                .unwrap_or(buf.len());
            let s = String::from_utf8_lossy(&buf[1..c]).to_string();
            buf.advance(c + 2);
            let st = lua.create_table()?;
            st.set("ok", s)?;
            Ok(Value::Table(st))
        }
        b'-' => {
            let c = buf
                .windows(2)
                .position(|w| w == b"\r\n")
                .unwrap_or(buf.len());
            let s = String::from_utf8_lossy(&buf[1..c]).to_string();
            buf.advance(c + 2);
            let et = lua.create_table()?;
            et.set("err", s)?;
            Ok(Value::Table(et))
        }
        b':' => {
            let c = buf
                .windows(2)
                .position(|w| w == b"\r\n")
                .unwrap_or(buf.len());
            let n: i64 = std::str::from_utf8(&buf[1..c])
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            buf.advance(c + 2);
            Ok(Value::Integer(n))
        }
        b'$' => {
            let c = buf
                .windows(2)
                .position(|w| w == b"\r\n")
                .unwrap_or(buf.len());
            let len: i64 = std::str::from_utf8(&buf[1..c])
                .unwrap_or("-1")
                .parse()
                .unwrap_or(-1);
            buf.advance(c + 2);
            if len < 0 {
                Ok(Value::Boolean(false))
            } else {
                let ulen = len as usize;
                let s = lua.create_string(&buf[..ulen])?;
                buf.advance(ulen + 2);
                Ok(Value::String(s))
            }
        }
        b'*' => parse_resp_array_to_lua(lua, buf),
        _ => Ok(Value::Nil),
    }
}

fn parse_resp_array_to_lua(lua: &Lua, buf: &mut bytes::BytesMut) -> mlua::Result<Value> {
    use bytes::Buf;
    if buf.is_empty() || buf[0] != b'*' {
        return Ok(Value::Nil);
    }
    let crlf = match buf.windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos,
        None => return Ok(Value::Nil),
    };
    let count: i64 = std::str::from_utf8(&buf[1..crlf])
        .unwrap_or("-1")
        .parse()
        .unwrap_or(-1);
    buf.advance(crlf + 2);
    if count < 0 {
        return Ok(Value::Boolean(false));
    }
    let tbl = lua.create_table()?;
    for i in 1..=count {
        if buf.is_empty() {
            break;
        }
        let val = parse_resp_element(lua, buf)?;
        tbl.set(i, val)?;
    }
    Ok(Value::Table(tbl))
}

pub fn lua_val_to_resp(val: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    lua_val_to_resp_with_depth(val, out, 0, 2)
}

fn lua_val_to_resp_with_depth(
    val: &Value,
    out: &mut Vec<u8>,
    depth: usize,
    script_resp_ver: u8,
) -> Result<(), String> {
    if depth > 1000 {
        return Err("reached lua stack limit".to_string());
    }
    match val {
        Value::Nil => {
            if crate::connection::CURRENT_CLIENT_RESP3.get() && script_resp_ver == 3 {
                out.extend_from_slice(b"_\r\n");
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
            Ok(())
        }
        Value::Boolean(b) => {
            if crate::connection::CURRENT_CLIENT_RESP3.get() && script_resp_ver == 3 {
                if *b {
                    out.extend_from_slice(b"#t\r\n");
                } else {
                    out.extend_from_slice(b"#f\r\n");
                }
            } else if *b {
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
            Ok(())
        }
        Value::Integer(i) => {
            out.extend_from_slice(format!(":{}\r\n", i).as_bytes());
            Ok(())
        }
        Value::Number(n) => {
            if crate::connection::CURRENT_CLIENT_RESP3.get() && script_resp_ver == 3 {
                out.extend_from_slice(format!(",{}\r\n", n).as_bytes());
            } else {
                out.extend_from_slice(format!(":{}\r\n", *n as i64).as_bytes());
            }
            Ok(())
        }
        Value::String(s) => {
            let bytes = s.as_bytes();
            out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
            out.extend_from_slice(&bytes);
            out.extend_from_slice(b"\r\n");
            Ok(())
        }
        Value::Table(t) => {
            if let Ok(ok_str) = t.raw_get::<String>("ok") {
                out.extend_from_slice(format!("+{}\r\n", ok_str).as_bytes());
                return Ok(());
            }
            if let Ok(err_str) = t.raw_get::<String>("err") {
                let sanitized = err_str.replace("\r\n", "  ").replace(['\r', '\n'], " ");
                let final_err = if sanitized.is_empty() || sanitized == "ERR" {
                    "ERR ".to_string()
                } else {
                    sanitized
                };
                out.extend_from_slice(format!("-{}\r\n", final_err).as_bytes());
                return Ok(());
            }
            if let Ok(f) = t.raw_get::<f64>("double") {
                if crate::connection::CURRENT_CLIENT_RESP3.get() && script_resp_ver == 3 {
                    out.extend_from_slice(format!(",{}\r\n", f).as_bytes());
                } else {
                    crate::connection::write_resp_bulk(out, f.to_string().as_bytes());
                }
                return Ok(());
            }

            let is_resp3_map = if let Some(mt) = t.metatable() {
                mt.get::<String>("__redis_proto_type").unwrap_or_default() == "map"
            } else {
                false
            };

            if is_resp3_map {
                let mut pairs = Vec::new();
                for pair in t.clone().pairs::<Value, Value>() {
                    pairs.push(pair.map_err(|e| e.to_string())?);
                }
                if crate::connection::CURRENT_CLIENT_RESP3.get() && script_resp_ver == 3 {
                    out.extend_from_slice(format!("%{}\r\n", pairs.len()).as_bytes());
                    for (k, v) in pairs {
                        lua_val_to_resp_with_depth(&k, out, depth + 1, script_resp_ver)?;
                        lua_val_to_resp_with_depth(&v, out, depth + 1, script_resp_ver)?;
                    }
                } else {
                    out.extend_from_slice(format!("*{}\r\n", pairs.len() * 2).as_bytes());
                    for (k, v) in pairs {
                        lua_val_to_resp_with_depth(&k, out, depth + 1, script_resp_ver)?;
                        lua_val_to_resp_with_depth(&v, out, depth + 1, script_resp_ver)?;
                    }
                }
                return Ok(());
            }

            let len = t.raw_len();
            out.extend_from_slice(format!("*{}\r\n", len).as_bytes());
            for i in 1..=len {
                let elem: Value = t.raw_get(i).unwrap_or(Value::Nil);
                lua_val_to_resp_with_depth(&elem, out, depth + 1, script_resp_ver)?;
            }
            Ok(())
        }
        _ => {
            out.extend_from_slice(b"$-1\r\n");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::AofWriter;
    use crate::shard::ShardDb;

    #[test]
    fn test_script_commands_are_subject_to_caller_acl() {
        let port = 65041;
        crate::acl::get_acl_for_port(port)
            .write()
            .unwrap()
            .set_user(
                "lua",
                &["on", "nopass", "-@all", "+eval", "+get", "~ok:*", "&news"].map(String::from),
            )
            .unwrap();
        let cmd = |args: &[&str]| {
            crate::resp::build_command(
                args.iter()
                    .map(|a| Bytes::copy_from_slice(a.as_bytes()))
                    .collect(),
            )
            .unwrap()
            .unwrap()
        };
        crate::connection::CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = "lua".to_string());
        assert_eq!(script_acl_denied(port, &cmd(&["GET", "ok:1"])), None);
        let e = script_acl_denied(port, &cmd(&["GET", "secret"])).unwrap();
        assert!(e.starts_with("NOPERM") && e.contains("keys"), "{e}");
        let e = script_acl_denied(port, &cmd(&["SET", "ok:1", "v"])).unwrap();
        assert!(
            e.starts_with("NOPERM") && e.contains("'set' command"),
            "{e}"
        );
        // An unrestricted caller is unaffected.
        crate::connection::CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = "default".to_string());
        assert_eq!(script_acl_denied(port, &cmd(&["SET", "secret", "v"])), None);
        crate::connection::CURRENT_AUTH_USER.with(|u| u.borrow_mut().clear());
        // Surfaced to the client as NOPERM (not wrapped in ERR), like Redis.
        assert_eq!(
            format_eval_error("runtime error: NOPERM denied", "abc"),
            "NOPERM denied script: abc, on @user_script:1."
        );
    }

    #[test]
    fn test_eval_script_basic_types_and_redis_call() {
        let db = Rc::new(RefCell::new(ShardDb::new(6379)));

        let res = eval_script("return 10 + 20", &[], &[], &db, None, false).unwrap();
        assert_eq!(res, b":30\r\n");

        let res = eval_script("return 'rudis_script'", &[], &[], &db, None, false).unwrap();
        assert_eq!(res, b"$12\r\nrudis_script\r\n");

        let res_t = eval_script("return true", &[], &[], &db, None, false).unwrap();
        assert_eq!(res_t, b":1\r\n");
        let res_f = eval_script("return false", &[], &[], &db, None, false).unwrap();
        assert_eq!(res_f, b"$-1\r\n");

        let res_arr = eval_script("return {'x', 'y'}", &[], &[], &db, None, false).unwrap();
        assert_eq!(res_arr, b"*2\r\n$1\r\nx\r\n$1\r\ny\r\n");

        let res_call = eval_script(
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1]);",
            &[Bytes::from("k_eval")],
            &[Bytes::from("v_eval")],
            &db,
            None,
            false,
        )
        .unwrap();
        assert_eq!(res_call, b"$6\r\nv_eval\r\n");

        let err_syn = eval_script("this is invalid lua !!!", &[], &[], &db, None, false);
        assert!(err_syn.is_err());

        let err_rt = eval_script("error('custom lua panic')", &[], &[], &db, None, false);
        assert!(err_rt.is_err());
    }

    #[test]
    fn test_eval_sha_and_function_management() {
        let db = Rc::new(RefCell::new(ShardDb::new(6379)));
        let script = b"return redis.call('PING')";
        let sha = load_script(script);

        assert_eq!(script_exists(&[Bytes::from(sha.clone())]), vec![true]);
        assert_eq!(
            script_exists(&[Bytes::from("0000000000000000000000000000000000000000")]),
            vec![false]
        );

        let cached = get_script(&sha).expect("script should be in cache");
        let res = eval_script(&cached, &[], &[], &db, None, false).unwrap();
        assert_eq!(res, b"+PONG\r\n");

        flush_scripts();
        assert_eq!(script_exists(&[Bytes::from(sha.clone())]), vec![false]);

        let code = "#!lua name=mylib\nredis.register_function('greet', function(keys, args) return 'hello ' .. args[1] end)";
        let lib_name = load_function(code, true).unwrap();
        assert_eq!(lib_name, "mylib");

        let funcs = list_functions();
        assert!(funcs.iter().any(|f| f.name == "mylib"));

        let call_res =
            call_function("greet", &[], &[Bytes::from("world")], &db, None, false).unwrap();
        assert_eq!(call_res, b"$11\r\nhello world\r\n");

        assert!(load_function(code, false).is_err());
        assert!(load_function(code, true).is_ok());

        assert!(delete_function("mylib"));
        assert!(!delete_function("mylib"));
        assert!(!list_functions().iter().any(|f| f.name == "mylib"));
    }

    #[test]
    fn test_fcall_with_aof_writer() {
        let code = "#!lua name=testlib\nredis.register_function('test_set', function(keys, args) return redis.call('SET', keys[1], args[1]) end)";
        let lib_name = load_function(code, true).expect("function load should succeed");
        assert_eq!(lib_name, "testlib");

        let db = Rc::new(RefCell::new(ShardDb::new(6379)));
        let aof = RefCell::new(AofWriter::new_in_memory());

        let res = call_function(
            "test_set",
            &[Bytes::from("test_key")],
            &[Bytes::from("test_val")],
            &db,
            Some(&aof),
            false,
        );
        assert!(res.is_ok());
        assert_eq!(
            db.borrow_mut().get(b"test_key"),
            Some(Bytes::from("test_val"))
        );

        let buf = aof.borrow().buffer().to_vec();
        let aof_str = String::from_utf8_lossy(&buf);
        assert!(
            aof_str.contains("SET"),
            "AOF buffer must contain SET command"
        );
        assert!(
            aof_str.contains("test_key"),
            "AOF buffer must contain test_key"
        );
        assert!(
            aof_str.contains("test_val"),
            "AOF buffer must contain test_val"
        );
    }
}
