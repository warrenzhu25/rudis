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
    SCRIPT_CACHE.write().unwrap().insert(sha.clone(), script_str);
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

pub fn eval_script(
    script_content: &str,
    keys: &[Bytes],
    args: &[Bytes],
    db: &Rc<RefCell<crate::shard::ShardDb>>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
) -> Result<Vec<u8>, String> {
    let lua = Lua::new();

    // Set KEYS table (1-indexed)
    let keys_tbl = lua.create_table().map_err(|e| e.to_string())?;
    for (i, k) in keys.iter().enumerate() {
        let s = lua.create_string(k.as_ref()).map_err(|e| e.to_string())?;
        keys_tbl.set(i + 1, s).map_err(|e| e.to_string())?;
    }
    lua.globals().set("KEYS", keys_tbl).map_err(|e| e.to_string())?;

    // Set ARGV table (1-indexed)
    let argv_tbl = lua.create_table().map_err(|e| e.to_string())?;
    for (i, a) in args.iter().enumerate() {
        let s = lua.create_string(a.as_ref()).map_err(|e| e.to_string())?;
        argv_tbl.set(i + 1, s).map_err(|e| e.to_string())?;
    }
    lua.globals().set("ARGV", argv_tbl).map_err(|e| e.to_string())?;

    // Create redis global table
    let redis = lua.create_table().map_err(|e| e.to_string())?;

    let db_call = db.clone();
    let aof_call = aof.map(|a| a as *const _);
    let call_fn = lua
        .create_function(move |lua, margs: MultiValue| {
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
            crate::connection::execute_local_command(
                &cmd,
                &mut db_call.borrow_mut(),
                &mut out,
                aof_ref,
            );

            resp_bytes_to_lua(lua, &out)
        })
        .map_err(|e| e.to_string())?;
    redis.set("call", call_fn).map_err(|e| e.to_string())?;

    let db_pcall = db.clone();
    let aof_pcall = aof.map(|a| a as *const _);
    let pcall_fn = lua
        .create_function(move |lua, margs: MultiValue| {
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
                Ok(None) => {
                    let tbl = lua.create_table()?;
                    tbl.set("err", "ERR empty command")?;
                    return Ok(Value::Table(tbl));
                }
                Err(e) => {
                    let tbl = lua.create_table()?;
                    tbl.set("err", format!("ERR {}", e))?;
                    return Ok(Value::Table(tbl));
                }
            };

            let mut out = Vec::new();
            let aof_ref = unsafe { aof_pcall.map(|ptr| &*ptr) };
            crate::connection::execute_local_command(
                &cmd,
                &mut db_pcall.borrow_mut(),
                &mut out,
                aof_ref,
            );

            if out.starts_with(b"-") {
                let err_str = String::from_utf8_lossy(&out[1..out.len().saturating_sub(2)]).to_string();
                let tbl = lua.create_table()?;
                tbl.set("err", err_str)?;
                Ok(Value::Table(tbl))
            } else {
                resp_bytes_to_lua(lua, &out)
            }
        })
        .map_err(|e| e.to_string())?;
    redis.set("pcall", pcall_fn).map_err(|e| e.to_string())?;

    let status_reply = lua
        .create_function(|lua, msg: String| {
            let tbl = lua.create_table()?;
            tbl.set("ok", msg)?;
            Ok(Value::Table(tbl))
        })
        .map_err(|e| e.to_string())?;
    redis.set("status_reply", status_reply).map_err(|e| e.to_string())?;

    let error_reply = lua
        .create_function(|lua, msg: String| {
            let tbl = lua.create_table()?;
            tbl.set("err", msg)?;
            Ok(Value::Table(tbl))
        })
        .map_err(|e| e.to_string())?;
    redis.set("error_reply", error_reply).map_err(|e| e.to_string())?;

    let sha1hex_fn = lua
        .create_function(|_lua, s: String| {
            Ok(sha1_hex(s.as_bytes()))
        })
        .map_err(|e| e.to_string())?;
    redis.set("sha1hex", sha1hex_fn).map_err(|e| e.to_string())?;

    lua.globals().set("redis", redis).map_err(|e| e.to_string())?;

    let chunk = lua.load(script_content);
    let val: Value = chunk.eval().map_err(|e| format!("ERR user_script: {}", e))?;

    let mut resp = Vec::new();
    lua_val_to_resp(&val, &mut resp)?;
    Ok(resp)
}

fn resp_bytes_to_lua(lua: &Lua, out: &[u8]) -> mlua::Result<Value> {
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
            Err(mlua::Error::RuntimeError(s.to_string()))
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
            parse_resp_array_to_lua(lua, &mut cursor)
        }
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
        match buf[0] {
            b'$' => {
                let elem_crlf = match buf.windows(2).position(|w| w == b"\r\n") {
                    Some(pos) => pos,
                    None => break,
                };
                let len: i64 = std::str::from_utf8(&buf[1..elem_crlf])
                    .unwrap_or("-1")
                    .parse()
                    .unwrap_or(-1);
                buf.advance(elem_crlf + 2);
                if len < 0 {
                    tbl.set(i, Value::Boolean(false))?;
                } else {
                    let ulen = len as usize;
                    if buf.len() >= ulen + 2 {
                        let data = buf.split_to(ulen);
                        buf.advance(2);
                        let s = lua.create_string(&data)?;
                        tbl.set(i, Value::String(s))?;
                    }
                }
            }
            b':' => {
                let elem_crlf = match buf.windows(2).position(|w| w == b"\r\n") {
                    Some(pos) => pos,
                    None => break,
                };
                let n: i64 = std::str::from_utf8(&buf[1..elem_crlf])
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0);
                buf.advance(elem_crlf + 2);
                tbl.set(i, Value::Integer(n))?;
            }
            b'+' => {
                let elem_crlf = match buf.windows(2).position(|w| w == b"\r\n") {
                    Some(pos) => pos,
                    None => break,
                };
                let s = std::str::from_utf8(&buf[1..elem_crlf]).unwrap_or("").to_string();
                buf.advance(elem_crlf + 2);
                let ok_tbl = lua.create_table()?;
                ok_tbl.set("ok", s)?;
                tbl.set(i, Value::Table(ok_tbl))?;
            }
            b'*' => {
                let nested = parse_resp_array_to_lua(lua, buf)?;
                tbl.set(i, nested)?;
            }
            _ => break,
        }
    }
    Ok(Value::Table(tbl))
}

fn lua_val_to_resp(val: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    match val {
        Value::Nil => {
            out.extend_from_slice(b"$-1\r\n");
            Ok(())
        }
        Value::Boolean(b) => {
            if *b {
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
            Ok(())
        }
        Value::Integer(i) => {
            crate::connection::write_resp_integer(out, *i);
            Ok(())
        }
        Value::Number(n) => {
            crate::connection::write_resp_integer(out, *n as i64);
            Ok(())
        }
        Value::String(s) => {
            crate::connection::write_resp_bulk(out, &s.as_bytes());
            Ok(())
        }
        Value::Table(t) => {
            if let Ok(ok_str) = t.get::<String>("ok") {
                out.extend_from_slice(format!("+{}\r\n", ok_str).as_bytes());
                return Ok(());
            }
            if let Ok(err_str) = t.get::<String>("err") {
                out.extend_from_slice(format!("-{}\r\n", err_str).as_bytes());
                return Ok(());
            }
            let len = t.raw_len();
            out.extend_from_slice(format!("*{}\r\n", len).as_bytes());
            for i in 1..=len {
                let elem: Value = t.get(i).unwrap_or(Value::Nil);
                lua_val_to_resp(&elem, out)?;
            }
            Ok(())
        }
        _ => {
            out.extend_from_slice(b"$-1\r\n");
            Ok(())
        }
    }
}

/// A registered Redis 7 Function Library
#[derive(Clone, Debug)]
pub struct FunctionLib {
    pub name: String,
    pub engine: String,
    pub raw_code: String,
    pub functions: Vec<String>,
}

static FUNCTION_LIBS: LazyLock<RwLock<HashMap<String, FunctionLib>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Load a Redis 7 Function Library
pub fn load_function(code: &str, replace: bool) -> Result<String, String> {
    // Parse library name from shebang or comment, e.g. "#!lua name=mylib"
    let mut lib_name = "default_lib".to_string();
    for line in code.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#!lua") || trimmed.starts_with("--") {
            if let Some(pos) = trimmed.find("name=") {
                let rest = &trimmed[pos + 5..];
                let name = rest.split_whitespace().next().unwrap_or("").trim_matches('"');
                if !name.is_empty() {
                    lib_name = name.to_string();
                    break;
                }
            }
        }
    }

    {
        let cache = FUNCTION_LIBS.read().unwrap();
        if cache.contains_key(&lib_name) && !replace {
            return Err(format!("ERR Library '{}' already exists", lib_name));
        }
    }

    // Execute with mock redis.register_function to collect function names
    let lua = Lua::new();
    let func_names = Rc::new(RefCell::new(Vec::new()));
    let func_names_clone = func_names.clone();

    let redis_tbl = lua.create_table().map_err(|e| e.to_string())?;
    let reg_fn = lua.create_function(move |_, (name, _): (String, Value)| {
        func_names_clone.borrow_mut().push(name);
        Ok(())
    }).map_err(|e| e.to_string())?;
    redis_tbl.set("register_function", reg_fn).map_err(|e| e.to_string())?;
    lua.globals().set("redis", redis_tbl).map_err(|e| e.to_string())?;

    let lua_code: String = code
        .lines()
        .map(|line| {
            if line.trim_start().starts_with("#!") {
                format!("--{}", line)
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    lua.load(&lua_code).exec().map_err(|e| format!("ERR Error registering function: {}", e))?;

    let registered = func_names.borrow().clone();
    let lib = FunctionLib {
        name: lib_name.clone(),
        engine: "LUA".to_string(),
        raw_code: lua_code,
        functions: registered,
    };

    FUNCTION_LIBS.write().unwrap().insert(lib_name.clone(), lib);
    Ok(lib_name)
}

/// Execute a registered Redis 7 Function via FCALL
pub fn call_function(
    func_name: &str,
    keys: &[Bytes],
    args: &[Bytes],
    db: &Rc<RefCell<crate::shard::ShardDb>>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
) -> Result<Vec<u8>, String> {
    // Find which library contains this function
    let lib = {
        let cache = FUNCTION_LIBS.read().unwrap();
        cache.values()
            .find(|l| l.functions.iter().any(|f| f == func_name))
            .cloned()
            .ok_or_else(|| format!("ERR Function '{}' not found", func_name))?
    };

    let lua = Lua::new();

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

    // Capture target function
    let target_fn = Rc::new(RefCell::new(None));
    let target_fn_clone = target_fn.clone();
    let target_name = func_name.to_string();

    let redis_tbl = lua.create_table().map_err(|e| e.to_string())?;

    // Bind redis.call
    let db_call = db.clone();
    let aof_call = aof.map(|a| a as *const _);
    let call_fn = lua
        .create_function(move |lua, margs: MultiValue| {
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
            crate::connection::execute_local_command(
                &cmd,
                &mut db_call.borrow_mut(),
                &mut out,
                aof_ref,
            );

            resp_bytes_to_lua(lua, &out)
        })
        .map_err(|e| e.to_string())?;
    redis_tbl.set("call", call_fn).map_err(|e| e.to_string())?;

    let reg_fn = lua.create_function(move |lua, (name, f): (String, mlua::Function)| {
        if name == target_name {
            let key = lua.create_registry_value(f)?;
            *target_fn_clone.borrow_mut() = Some(key);
        }
        Ok(())
    }).map_err(|e| e.to_string())?;
    redis_tbl.set("register_function", reg_fn).map_err(|e| e.to_string())?;

    lua.globals().set("redis", redis_tbl).map_err(|e| e.to_string())?;

    // Run library script to define functions
    lua.load(&lib.raw_code).exec().map_err(|e| format!("ERR Failed to compile library: {}", e))?;

    let fn_key = target_fn.borrow_mut().take()
        .ok_or_else(|| format!("ERR Function '{}' registered but failed to capture", func_name))?;
    let f: mlua::Function = lua.registry_value(&fn_key).map_err(|e| e.to_string())?;

    let res: Value = f.call((keys_tbl, argv_tbl)).map_err(|e| format!("ERR Error running function '{}': {}", func_name, e))?;

    let mut out = Vec::new();
    lua_val_to_resp(&res, &mut out)?;
    Ok(out)
}

/// Returns list of registered libraries and functions
pub fn list_functions() -> Vec<FunctionLib> {
    let cache = FUNCTION_LIBS.read().unwrap();
    cache.values().cloned().collect()
}

/// Delete a registered function library
pub fn delete_function(lib_name: &str) -> bool {
    FUNCTION_LIBS.write().unwrap().remove(lib_name).is_some()
}

