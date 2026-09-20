# Component 03: RESP Protocol Engine & Command Parser (Implementation)

## Component 03: RESP Protocol Engine & Command Parser — Code Reference & Implementation

> **Source Files**: ``src/resp.rs``


---

### 3. Command Surface & Data Structures

#### 3.1 What `parse_command` actually recognizes

```text
First byte        Grammar                                   Handler
──────────────────────────────────────────────────────────────────────────────
'*'               RESP array: *N\r\n($len\r\ndata\r\n){N}    parse_resp_array
otherwise         Try Memcached "set/add/replace key         parse_memcached_storage_command
                  flags exptime bytes [noreply]\r\n<data>\r\n"
otherwise         Space/tab-separated inline text             parse_inline_command
```

There is no separate frame type for simple strings (`+`), errors (`-`), or integers (`:`) on
the *input* side — those are reply-only prefixes written by `connection.rs`, never parsed
here, because a client never sends a command framed that way.

#### 3.2 The `Command` enum: real shape, real category breakdown

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Auth { username: Option<String>, password: String },
    Acl(AclSubcommand),
    Get(Bytes),
    Getex { key: Bytes, expire_in: Option<Duration>, persist: bool },
    Set {
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        condition: SetCondition,   // None | Nx | Xx | Ifeq(Bytes) | Ifne(Bytes) | Ifdeq(Bytes) | Ifdne(Bytes)
        get: bool,
        keepttl: bool,
        past_expired: bool,
    },
    Mget(Vec<Bytes>),
    Mset(Vec<(Bytes, Bytes)>),
    // ... hundreds more variants, grouped by the file's own section comments:
    // LIST, SET, ZSET, GENERIC & DATABASE, EXTENDED STRING, LUA SCRIPTING,
    // TIERED STORAGE, CONFIG, PUBSUB, KEYSPACE INSPECTION, TRANSACTIONS,
    // BITMAP, HYPERLOGLOG, RDB SERIALIZATION, STREAM, VALKEY EXTENDED,
    // VECTOR, CRDT MULTI-REGION, REDIS 7 FUNCTIONS, REDISJSON, GEOSPATIAL,
    // PROBABILISTIC, FT.* (full-text search), XDP.* (AF_XDP/eBPF control),
    // Dragonfly native extensions, and a Memcached Protocol block:
    MemcachedSet { key: Bytes, flags: u32, exptime: u32, bytes: usize, noreply: bool, data: Bytes },
    MemcachedGet { keys: Vec<Bytes> },
    MemcachedDelete { key: Bytes, noreply: bool },
    MemcachedIncr { key: Bytes, value: u64, noreply: bool },
    MemcachedStats,
    Unknown(String),
}
```

Note the derive is `PartialEq, Clone` — **not `Eq`** — because several variants (`Zadd`,
`Zincrby`, score-range queries, `Set`'s `past_expired` semantics, etc.) carry `f64` fields,
and `f64` has no total order (`NaN != NaN`), so `Eq` can't be derived. This is a real
constraint of the type, not an oversight.

The enum's category list above comes directly from the file's own `// SECTION NAME` comments
(`grep -n "^    // [A-Z]" src/resp.rs`), which is the fastest way to get an up-to-date map of
what's supported without reading all ~1,000 lines of variant declarations.

---

---

### 4. Parsing Algorithms & Code Logic

#### 4.1 `parse_command`: three grammars, tried in a fixed order

```rust
pub fn parse_command(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    if buf.is_empty() {
        return Ok(None);
    }

    if buf[0] == b'*' {
        parse_resp_array(buf)
    } else {
        match parse_memcached_storage_command(buf)? {
            Some(Some(cmd)) => Ok(Some(cmd)),
            Some(None) => Ok(None),
            None => parse_inline_command(buf),
        }
    }
}
```

`parse_memcached_storage_command` returns a triple-layered result deliberately: `Ok(None)`
means "this isn't a memcached storage command at all, try inline parsing next";
`Ok(Some(None))` means "it *is* one, but the data block hasn't fully arrived yet — wait for
more bytes, don't fall through to inline parsing"; `Ok(Some(Some(cmd)))` is a complete parse.
That extra `Option` layer exists specifically to prevent a partially-received memcached
`set key 0 0 1024\r\n<...only 200 bytes so far...>` from being misinterpreted as inline text.

```rust
fn parse_memcached_storage_command(buf: &mut BytesMut) -> Result<Option<Option<Command>>, String> {
    let newline_pos = match find_crlf(buf) { Some(pos) => pos, None => return Ok(None) };
    let line = &buf[..newline_pos];
    let first_space = match line.iter().position(|&b| b == b' ' || b == b'\t') {
        Some(p) => p, None => return Ok(None),
    };
    let first_word = &line[..first_space];
    let is_set = first_word.eq_ignore_ascii_case(b"set");
    let is_add = first_word.eq_ignore_ascii_case(b"add");
    let is_replace = first_word.eq_ignore_ascii_case(b"replace");
    if !is_set && !is_add && !is_replace {
        return Ok(None);
    }
    // ... parse key/flags/exptime/bytes/noreply from the header line ...
    let total_len = newline_pos + 2 + bytes_len + 2;
    if buf.len() < total_len {
        return Ok(Some(None)); // header parsed, but data block hasn't fully arrived
    }
    if &buf[newline_pos + 2 + bytes_len .. total_len] != b"\r\n" {
        return Err("CLIENT_ERROR bad data chunk".to_string());
    }
    // ... build Command::MemcachedSet/Add/Replace, buf.advance(total_len) ...
}
```

#### 4.2 `parse_resp_array`: unchanged two-pass zero-copy scan

This is verbatim the same algorithm as the original design: scan the whole frame for
completeness first, without mutating `buf`, then — only once the entire frame is known to be
present — make a second pass that actually consumes bytes and slices out zero-copy `Bytes`
arguments.

```rust
fn parse_resp_array(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    let newline_pos = match find_crlf(buf) { Some(pos) => pos, None => return Ok(None) };
    let line = &buf[1..newline_pos];
    let num_args: usize = match std::str::from_utf8(line).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return Err("Invalid array length in RESP frame".to_string()),
    };

    // Pass 1: prove the whole frame is present without consuming anything.
    let mut scan_cursor = newline_pos + 2;
    for _ in 0..num_args {
        if scan_cursor >= buf.len() { return Ok(None); }
        if buf[scan_cursor] != b'$' {
            return Err("Expected bulk string in command array".to_string());
        }
        let next_crlf = match find_crlf_at(buf, scan_cursor) { Some(p) => p, None => return Ok(None) };
        let arg_len: usize = std::str::from_utf8(&buf[scan_cursor + 1..next_crlf])
            .ok().and_then(|s| s.parse().ok())
            .ok_or_else(|| "Invalid bulk string length".to_string())?;
        let data_end = next_crlf + 2 + arg_len;
        if data_end + 2 > buf.len() { return Ok(None); }
        if &buf[data_end..data_end + 2] != b"\r\n" {
            return Err("Expected CRLF after bulk string data".to_string());
        }
        scan_cursor = data_end + 2;
    }

    // Pass 2: frame confirmed complete — now actually consume and zero-copy slice.
    buf.advance(newline_pos + 2);
    let mut args = Vec::with_capacity(num_args);
    for _ in 0..num_args {
        let header_crlf = find_crlf(buf).unwrap();
        let arg_len: usize = std::str::from_utf8(&buf[1..header_crlf]).unwrap().parse().unwrap();
        buf.advance(header_crlf + 2);
        let data = buf.split_to(arg_len).freeze(); // zero-copy
        buf.advance(2);
        args.push(data);
    }
    build_command(args)
}
```

`parse_inline_command` is also unchanged: it splits on spaces/tabs and builds each argument
with `Bytes::copy_from_slice` — a real copy, not zero-copy, because this path only serves
interactive/debugging clients (`redis-cli`, `nc`), never the benchmarked pipelined workload.

#### 4.3 `build_command`: one shared constructor, ~5,500 lines, dispatched by uppercased name

```rust
pub fn build_command(args: Vec<Bytes>) -> Result<Option<Command>, String> {
    if args.is_empty() { return Ok(None); }
    let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
    match cmd_name.as_str() {
        "GET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'get' command".to_string());
            }
            if args.len() == 2 {
                Ok(Some(Command::Get(args[1].clone())))
            } else {
                // "get k1 k2 k3" is not valid Redis GET arity — treat it as a
                // Memcached multi-key get instead of rejecting it outright.
                Ok(Some(Command::MemcachedGet { keys: args[1..].to_vec() }))
            }
        }
        // ...
        _ => Ok(Some(Command::Unknown(cmd_name))),
    }
}
```

This `GET`/`MemcachedGet` disambiguation-by-arity is a good example of how the Redis and
Memcached surfaces share one dispatch table without a protocol-mode flag: since real Redis
`GET` is strictly single-key, any extra arguments unambiguously mean the memcached dialect
was intended instead. `DEL`/`DELETE` similarly resolve to `Command::Del` or
`Command::MemcachedDelete` depending on which spelling arrived.

Options are parsed with the same index-walking `while i < args.len()` loop over uppercased
option tokens the original design used for `SET ... EX ...`, now carrying more flags. `SET`
itself has grown Dragonfly-style conditional variants beyond plain `NX`/`XX`:

```rust
"IFEQ" => {
    if condition != SetCondition::None || i + 1 >= args.len() { return Err("syntax error".to_string()); }
    condition = SetCondition::Ifeq(args[i + 1].clone()); // set only if current value == this
    i += 2;
}
```

#### 4.4 `HELLO`: parsed here, acted on in `connection.rs`

```rust
"HELLO" => {
    let mut proto = None;
    // first arg, if a bare integer, is the requested protocol version
    if let Ok(p) = String::from_utf8_lossy(&args[1]).parse::<u8>() { proto = Some(p); ... }
    // then AUTH user pass / SETNAME name in any order
    Ok(Some(Command::Hello { proto, auth, setname }))
}
```

`resp.rs` only produces the `Command::Hello { proto, .. }` value. It's `connection.rs` that
inspects `proto` and flips the client's `is_resp3` flag and the `CURRENT_CLIENT_RESP3`
thread-local, which is what actually changes reply formatting afterward.

#### 4.5 `parse_redis_f64`: libc `strtod` as a fallback for exact Redis float parsing

Sorted-set scores need to accept exactly the float literals real Redis accepts (`inf`,
`+inf`, `-inf`, `infinity`, values Rust's `f64::from_str` is stricter about). Rather than
reimplementing C's `strtod` parsing rules by hand, this file falls back to the real libc
function via FFI when Rust's own parser doesn't accept the input:

```rust
pub fn parse_redis_f64(s: &str) -> Option<f64> {
    if let Ok(v) = s.parse::<f64>() { /* fast path */ return Some(v); }
    if let Ok(c_str) = std::ffi::CString::new(s) {
        unsafe {
            let mut end: *mut libc::c_char = std::ptr::null_mut();
            let val = libc::strtod(c_str.as_ptr(), &mut end);
            if !end.is_null() && *end == 0 && end != c_str.as_ptr() as *mut libc::c_char {
                return Some(val);
            }
        }
    }
    None
}
```

`parse_score_bound` builds on this to handle `ZRANGEBYSCORE`-style `(exclusive` prefixes on
top of the same float grammar.

#### 4.6 `find_crlf`/`find_crlf_at`: still a plain windowed scan

Unchanged from the original design — a `.windows(2).position(|w| w == b"\r\n")` scan. It's
only ever used to find the end of small protocol headers (array lengths, bulk-string length
prefixes), never to scan payload data, so there's no SIMD opportunity being left on the table
here (see the storage engine's SIMD control-byte matching in Part 1 of
`docs/design/components.md` for where that technique actually applies, on 16-byte groups).

---

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: The sole consumer of `parse_command`. All reply serialization
  (RESP2 and RESP3) happens there, not in this file.
- **`src/table.rs`**: Several `Command` variant fields carry types defined in `table.rs`
  directly (e.g. `Zadd`'s `flags: crate::table::ZAddFlags`, `Zrange`'s
  `opts: crate::table::ZRangeOpts`, stream commands' `crate::table::StreamId`), so this file
  and the storage engine share vocabulary rather than each redefining it.
- **`src/block.rs`**: `ClientSubcommand::Unblock` carries a `crate::block::ClientUnblockType`
  defined in the blocking-operations module.

---

---

### 7. Future Improvements

- **High — enforce a maximum bulk-string/array length (§8's "No maximum frame/argument size enforcement").** `parse_resp_array` trusts `arg_len` straight off the wire with no upper bound, so a client claiming a multi-gigabyte bulk string makes the server attempt to buffer that much data before giving up. A `proto-max-bulk-len`-equivalent check (reject the frame early if the declared length exceeds a configurable cap) is a small, high-value change given this is the very first thing untrusted input touches.
- **Medium — split `build_command`'s ~5,500-line single match into per-family functions** (strings, hashes, lists, sets, zsets, streams, cluster/ACL, scripting, vector/search, tiering, Memcached, ...), dispatched from a smaller top-level match on a coarse prefix or category lookup. Purely a maintainability change (§6 already notes the compiler handles the current single match fine performance-wise) — the real motivation is that a 5,500-line function is a place bugs hide, not a place they're compiled away.
- **Medium — reconcile the `GET`-vs-`MemcachedGet` and `DEL`-vs-`MemcachedDelete` arity-based disambiguation (§4.3) with a config flag.** Silently reinterpreting `GET k1 k2` as a Memcached multi-get is convenient for dual-protocol support but means a genuine Redis client typo (`GET` with accidentally-extra arguments) gets a different error message than real Redis would give, which could confuse debugging. Consider gating the Memcached-arity fallback behind an explicit "Memcached gateway enabled" flag so pure-Redis deployments get real Redis-compatible arity errors.
- **Low — add RESP3 *input* parsing** (maps `%`, sets `~`, doubles `,`, booleans `#`, nulls `_`) if any planned feature needs a client to send a RESP3-typed argument rather than only receive RESP3-typed replies (§2.4) — not needed today since every real Redis command is still sent as a flat bulk-string array, but worth flagging as the one genuine protocol-completeness gap versus a full RESP3 implementation.

---
---
