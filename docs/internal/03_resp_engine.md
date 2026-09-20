# Component 03: RESP Protocol Engine & Command Parser (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/resp.rs`  
> **High-Level Design Spec**: [`docs/design/03_resp_engine.md`](../design/03_resp_engine.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

`src/resp.rs` is 9,155 lines and contains no reply-encoding code; it is entry parsing and
command construction only. Reply serialization (RESP2/RESP3) is implemented in
`src/connection.rs`.

| Region (approx. lines) | Responsibility | Key Functions / Types |
| :--- | :--- | :--- |
| 1–172 | Subcommand enums shared by several `Command` variants | `SetSlotSubcommand`, `ClusterSubcommand`, `DflyClusterSubcommand`, `DflyMigrateSubcommand`, `SetCondition`, `MsetexCondition`, `MsetexExpiry`, `MemorySubcommand`, `TierSubcommand`, `ClientSubcommand`, `AclSubcommand` |
| 173–1241 | The command surface itself | `pub enum Command` (108 variants) |
| 1243–1331 | Memcached storage-command header/body parser | `fn parse_memcached_storage_command` |
| 1336–1350 | Top-level entry point / grammar dispatch | `pub fn parse_command` |
| 1352–1384 | Allocation-free decode helpers used by the hot path | `parse_decimal_bytes`, `bytes_to_uppercase_ascii` |
| 1386–1662 | RESP array parser (two-pass scan + ≤16-arg fast path) | `fn parse_resp_array` |
| 1664–1684 | Inline (space-separated) command parser | `fn parse_inline_command` |
| 1686–8157 | The command constructor / dispatch table | `pub fn build_command` (283 top-level match arms) |
| 8160–8199 | Redis-compatible float parsing (libc `strtod` fallback) | `pub fn parse_redis_f64` |
| 8201–8219 | `ZRANGEBYSCORE`-style `(exclusive` bound parsing | `pub fn parse_score_bound` |
| 8221–8233 | CRLF header scanning | `fn find_crlf`, `fn find_crlf_at` |
| 8235–9155 | Unit tests (13 `#[test]` functions) | `mod tests` |

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
here, because a client never sends a command framed that way. Likewise, none of the RESP3
input types (`%` map, `~` set, `,` double, `#` boolean, `_` null, `>` push) are recognized on
input; `resp.rs` never inspects the negotiated protocol version while parsing.

#### 3.2 The `Command` enum: real shape, real category breakdown

`enum Command` spans `src/resp.rs:173`–`1241` (~1,069 lines) and currently has **108
variants**. It derives:

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
    Del(SmallVec<[Bytes; 1]>),      // inline-capacity 1: the common single-key case never allocates
    Exists(SmallVec<[Bytes; 1]>),
    // ... 90+ more variants, grouped by the file's own section comments:
    // LIST, SET, ZSET, GENERIC & DATABASE, EXTENDED STRING, LUA SCRIPTING,
    // TIERED STORAGE, CONFIG, PUBSUB, KEYSPACE INSPECTION, TRANSACTIONS,
    // BITMAP, HYPERLOGLOG, RDB SERIALIZATION, STREAM, VALKEY EXTENDED,
    // VECTOR, CRDT MULTI-REGION, REDIS 7 FUNCTIONS, REDISJSON, GEOSPATIAL,
    // PROBABILISTIC, FT.* (full-text search), XDP.* (AF_XDP/eBPF control),
    // Dragonfly native extensions, and a Memcached Protocol block:
    MemcachedSet { key: Bytes, flags: u32, exptime: u32, bytes: usize, noreply: bool, data: Bytes },
    MemcachedAdd { key: Bytes, flags: u32, exptime: u32, bytes: usize, noreply: bool, data: Bytes },
    MemcachedReplace { key: Bytes, flags: u32, exptime: u32, bytes: usize, noreply: bool, data: Bytes },
    MemcachedGet { keys: Vec<Bytes> },
    MemcachedDelete { key: Bytes, noreply: bool },
    MemcachedIncr { key: Bytes, value: u64, noreply: bool },
    MemcachedDecr { key: Bytes, value: u64, noreply: bool },
    MemcachedStats,
    MemcachedVersion,
    MemcachedQuit,
    Memory(MemorySubcommand),
    Unknown(String),
}
```

Note the derive is `PartialEq, Clone` — **not `Eq`** — because several variants (`Zadd`,
`Zincrby`, score-range queries, `Set`'s `past_expired` semantics, etc.) carry `f64` fields,
and `f64` has no total order (`NaN != NaN`), so `Eq` can't be derived. This is a real
constraint of the type, not an oversight.

The enum's category list above comes directly from the file's own `// SECTION NAME` comments
(`grep -n "^    // [A-Z]" src/resp.rs`), which is the fastest way to get an up-to-date map of
what's supported without reading all ~1,069 lines of variant declarations. As of this writing
there are 26 such section markers.

Several `Command` variants deliberately embed types owned by other modules rather than
redefining them locally — see §5.

---

### 4. Parsing Algorithms & Code Logic

#### 4.1 `parse_command`: three grammars, tried in a fixed order (`src/resp.rs:1336`)

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

This is verbatim the current source, and `git log` shows it has not been modified since it was
introduced. It is
the sole entry point every ingestion loop calls: the primary client command loop, the pub/sub
loop, replication-stream ingestion, and AOF replay in `src/connection.rs` / `src/aof.rs` /
`src/replication.rs` all drive their own `BytesMut` through this one function.

#### 4.2 `parse_memcached_storage_command`: a triple-layered `Option` result (`src/resp.rs:1243`–`1331`)

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

Concretely: the header line must tokenize into at least 5 whitespace-separated fields
(`<verb> <key> <flags> <exptime> <bytes>`, with an optional 6th `noreply` token); fewer than
5 tokens, or a non-numeric `flags`/`exptime`/`bytes`, makes the function return `Ok(None)` —
"not a recognized memcached storage command" — rather than an error, so `parse_command` falls
through to inline parsing instead of rejecting the input outright. Once the header is valid
and the full `bytes`-length data block (plus its trailing `\r\n`) has arrived, the key and data
are each copied with `Bytes::copy_from_slice` (not zero-copy — the memcached path trades
allocation for simplicity, same as the inline-text path) into a `Command::MemcachedSet`,
`MemcachedAdd`, or `MemcachedReplace` depending on which of `set`/`add`/`replace` (matched
case-insensitively) started the line.

#### 4.3 `parse_resp_array`: two-pass validity scan, then a ≤16-arg fast path or a generic path (`src/resp.rs:1386`–`1662`)

The overall shape is unchanged from the original design — scan the whole frame for
completeness first, without mutating `buf`, then only once the entire frame is known to be
present, consume it — but the current implementation adds a fast dispatch path for small
arrays that the original two-pass description did not have. The real control flow is:

1. **Read the array-length header.** `find_crlf` locates the first `\r\n`; the digits between
   `*` and it are decoded with `parse_decimal_bytes` (§4.4) into `num_args`. A non-digit or
   empty header is `Err("Invalid array length in RESP frame")`.
2. **Pass 1 — prove the frame is complete, and opportunistically record offsets.** A fixed
   `[(usize, usize); 16]` stack array (`offsets`) and an `is_small = num_args <= 16` flag are
   set up before the scan. The loop walks each of the `num_args` bulk-string headers
   (`$<len>\r\n<data>\r\n`) with `find_crlf_at`, validating the `$` prefix, decoding the
   length with `parse_decimal_bytes`, and checking the trailing `\r\n` after the data — exactly
   the same four error strings as before (`"Expected bulk string in command array"`,
   `"Invalid bulk string length"`, `"Expected CRLF after bulk string data"`, or `Ok(None)` if
   the buffer runs out mid-scan). **Nothing in this pass mutates `buf`.** If `is_small`, each
   argument's `(data_start, arg_len)` is cached into `offsets[i]` as the scan proceeds — a pure
   bookkeeping side effect that costs nothing extra since the bytes are already being walked.
3. **Pass 2a — small-array fast path (`num_args <= 16`).** The whole frame (header line through
   the last argument's trailing `\r\n`) is split off the receive buffer in one
   `buf.split_to(scan_cursor).freeze()` call, producing a single `Bytes` (`frame`) that owns one
   shared allocation. If `num_args > 0`, the uppercase-folded first argument's byte length
   (`cmd_len`) is matched, and within each length bucket a small number of `eq_ignore_ascii_case`
   checks against the highest-frequency command names build the matching `Command` variant
   directly from `frame.slice(..)` views — no call into `build_command` at all for these cases.
   The commands with a fast-path arm today are:

   | Byte length | Commands |
   | :-: | :--- |
   | 3 | `GET` (exact 2 args), `SET` (exact 3 args, plain form only — no `EX`/`NX`/etc.), `DEL` (2+ args) |
   | 4 | `INCR`, `HGET`, `HSET` (single field/value), `SADD` (single member), `LPOP` (no count), `ZADD` (single score/member), `PING` (no argument), `MGET`, `MSET` |
   | 5 | `LPUSH` (single value) |
   | 6 | `EXISTS`, `LRANGE`, `ZRANGE` (plain `start`/`stop` form) |
   | 9 | `SISMEMBER` |

   Each fast-path arm still checks arity/shape (e.g. `SET` only fast-paths the plain
   `SET key value` form; a `SET key value EX 10` falls through to the general path because it
   has 5 args, not 3). If the command name doesn't match any fast-path arm — including every
   command not listed above — execution falls through to building a generic `Vec<Bytes>` from
   the cached `offsets` (still zero-copy, via `frame.slice(..)` on the already-split frame) and
   calling `build_command`.
4. **Pass 2b — general path (`num_args > 16`).** For arrays too large to fit the fixed
   `offsets` buffer, `buf.advance(newline_pos + 2)` consumes the header line, and each argument
   is decoded and sliced off one at a time with its own `buf.split_to(arg_len).freeze()` call —
   the same per-argument zero-copy split the original design described — before calling
   `build_command`.

Both the fast path and the general path end up calling `Bytes`-level operations only —
`frame.slice(..)` and `BytesMut::split_to(..).freeze()` are both O(1) reference-count bumps,
never a byte copy — so the fast path's advantage over the general path is that it does **one**
`split_to`/refcount bump for the whole command instead of one per argument, and it skips
`build_command`'s dispatch entirely for the highest-frequency verbs.

`parse_inline_command` is also unchanged: it splits on spaces/tabs and builds each argument
with `Bytes::copy_from_slice` — a real copy, not zero-copy, because this path only serves
interactive/debugging clients (`redis-cli`, `nc`), never the benchmarked pipelined workload.
Like `parse_resp_array`, it delegates argument-list construction to `build_command`.

#### 4.4 Allocation-free decode helpers (`src/resp.rs:1352`–`1384`)

Two small `#[inline]` functions exist specifically to keep the parsing hot path free of heap
allocation for values that are almost always short:

```rust
#[inline]
pub fn parse_decimal_bytes(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut val: usize = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(val)
}
```

Used everywhere a RESP length prefix (array length, bulk-string length) needs to become a
`usize`. It replaces a `std::str::from_utf8(..).ok().and_then(|s| s.parse().ok())` round trip
with a manual ASCII-digit accumulator — no UTF-8 validation pass, no intermediate `&str`. The
`checked_mul`/`checked_add` calls make integer overflow (an absurdly long digit string) return
`None` — i.e. a parse error — rather than silently wrapping; this is the only overflow guard
in the length-decoding path (there is still no upper bound on a *valid*, non-overflowing
length — see §7's max-frame-size gap).

```rust
#[inline]
pub fn bytes_to_uppercase_ascii<'a>(
    bytes: &'a [u8],
    buf: &'a mut [u8; 64],
    heap: &'a mut String,
) -> &'a str {
    if bytes.len() <= 64 && bytes.is_ascii() {
        for (i, b) in bytes.iter().enumerate() {
            buf[i] = b.to_ascii_uppercase();
        }
        unsafe { std::str::from_utf8_unchecked(&buf[..bytes.len()]) }
    } else {
        *heap = String::from_utf8_lossy(bytes).to_uppercase();
        heap.as_str()
    }
}
```

Used by `build_command` to case-fold the command-name argument. Every real command name is
short, plain ASCII, and fits comfortably in the 64-byte stack buffer the caller provides, so
the common case never touches the heap; the `String::from_utf8_lossy(..).to_uppercase()` path
is a fallback for the (essentially never-hit, but still memory-safe) case of a long or
non-ASCII first argument. The `unsafe` block is justified by the preceding `bytes.is_ascii()`
check plus the fact that ASCII-uppercasing an ASCII byte cannot produce a non-ASCII byte, so
the written bytes are guaranteed valid UTF-8.

#### 4.5 `build_command`: one shared constructor, ~6,470 lines, dispatched by uppercased name (`src/resp.rs:1686`–`8157`)

```rust
pub fn build_command(mut args: Vec<Bytes>) -> Result<Option<Command>, String> {
    if args.is_empty() {
        return Ok(None);
    }

    let mut cmd_buf = [0u8; 64];
    let mut cmd_heap = String::new();
    let cmd_name = bytes_to_uppercase_ascii(&args[0], &mut cmd_buf, &mut cmd_heap);

    match cmd_name {
        "GET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'get' command".to_string());
            }
            if args.len() == 2 {
                Ok(Some(Command::Get(args[1].clone())))
            } else {
                // "get k1 k2 k3" is not valid Redis GET arity — treat it as a
                // Memcached multi-key get instead of rejecting it outright.
                Ok(Some(Command::MemcachedGet {
                    keys: args[1..].to_vec(),
                }))
            }
        }
        // ... 280+ more top-level arms ...
        _ => Ok(Some(Command::Unknown(cmd_name.to_string()))),
    }
}
```

This is the real current signature (`args: Vec<Bytes>` is `mut` because several arms, e.g.
`DEL`/`EXISTS`, call `args.remove(0)` in place rather than allocating a fresh slice), and the
command-name lookup goes through `bytes_to_uppercase_ascii` (§4.4), not
`String::from_utf8_lossy(..).to_uppercase()` — the latter would allocate a `String` on every
single dispatched command, fast-pathed or not.

The top-level `match cmd_name { .. }` has **283 arms** (some combine aliases with `|`, e.g.
`"DEL" | "DELETE"`), covering **~295 distinct command-name strings**, and the function body
spans roughly 6,470 lines (`src/resp.rs:1686`–`8157`). Nested nesting is common: most arms that
take options (`SET .. EX ..`, `ZADD .. GT LT NX ..`, `HELLO AUTH ..`) run their own
`while i < args.len() { match uppercased_option { .. } }` loop over the remaining arguments
after fixed positional arguments are consumed.

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

#### 4.6 `HELLO`: parsed here, acted on in `connection.rs` (`src/resp.rs:5963`)

```rust
"HELLO" => {
    let mut proto = None;
    let mut auth = None;
    let mut setname = None;
    let mut i = 1;
    if i < args.len()
        && let Ok(p) = String::from_utf8_lossy(&args[i]).parse::<u8>()
    {
        proto = Some(p);
        i += 1;
    }
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "AUTH" => { /* consumes username + password, i += 3 */ }
            "SETNAME" => { /* consumes client name, i += 2 */ }
            _ => { i += 1; }
        }
    }
    Ok(Some(Command::Hello { proto, auth, setname }))
}
```

`Command::Hello { proto: Option<u8>, auth: Option<(String, String)>, setname: Option<String> }`
(`src/resp.rs:709`) is all `resp.rs` produces. It's `connection.rs` that inspects `proto` and
flips the client's `is_resp3` flag and the `CURRENT_CLIENT_RESP3` thread-local, which is what
actually changes reply formatting afterward — `resp.rs` never sets or reads either of those
itself.

#### 4.7 `parse_redis_f64` / `parse_score_bound`: libc `strtod` as a fallback for exact Redis float parsing (`src/resp.rs:8160`–`8219`)

Sorted-set scores need to accept exactly the float literals real Redis accepts (`inf`, `+inf`,
`-inf`, `infinity` and their case-insensitive/sign variants), while rejecting `nan` outright and
rejecting an out-of-range literal that Rust's stricter `f64::from_str` would otherwise silently
turn into `inf`/`-inf`. Rather than reimplementing C's `strtod` parsing rules by hand, this file
falls back to the real libc function via FFI when Rust's own parser doesn't accept the input:

```rust
pub fn parse_redis_f64(s: &str) -> Option<f64> {
    if s.is_empty() || s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return None;
    }
    if s.eq_ignore_ascii_case("nan") || s.eq_ignore_ascii_case("+nan") || s.eq_ignore_ascii_case("-nan") {
        return None;
    }
    let is_literal_inf = s.eq_ignore_ascii_case("inf") || s.eq_ignore_ascii_case("+inf")
        || s.eq_ignore_ascii_case("-inf") || s.eq_ignore_ascii_case("infinity")
        || s.eq_ignore_ascii_case("+infinity") || s.eq_ignore_ascii_case("-infinity");

    if let Ok(v) = s.parse::<f64>() {
        if !v.is_nan() {
            if v.is_infinite() && !is_literal_inf { return None; } // e.g. "1e400" must not become inf
            return Some(v);
        }
        return None;
    }
    if let Ok(c_str) = std::ffi::CString::new(s) {
        unsafe {
            let mut end: *mut libc::c_char = std::ptr::null_mut();
            let val = libc::strtod(c_str.as_ptr(), &mut end);
            if !end.is_null() && *end == 0 && !std::ptr::eq(end, c_str.as_ptr()) && !val.is_nan() {
                if val.is_infinite() && !is_literal_inf { return None; }
                return Some(val);
            }
        }
    }
    None
}
```

`parse_score_bound` (`src/resp.rs:8201`) builds on this to handle `ZRANGEBYSCORE`-style
`(exclusive` prefixes: a leading `(` strips itself off and marks the bound exclusive, `-inf`/
`+inf`/`inf` are special-cased directly (bypassing `parse_redis_f64`'s NaN/whitespace checks
since they're not needed for these literals), and everything else is delegated to
`parse_redis_f64`.

#### 4.8 `find_crlf`/`find_crlf_at`: still a plain windowed scan (`src/resp.rs:8221`–`8233`)

Unchanged from the original design — a `.windows(2).position(|w| w == b"\r\n")` scan. It's
only ever used to find the end of small protocol headers (array lengths, bulk-string length
prefixes), never to scan payload data, so there's no SIMD opportunity being left on the table
here (see the storage engine's SIMD control-byte matching in Part 1 of
`docs/design/components.md` for where that technique actually applies, on 16-byte groups).

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: The primary consumer of `parse_command` (client command loop,
  pub/sub loop). All reply serialization (RESP2 and RESP3) happens there, not in this file —
  see `write_resp_err`, `write_resp_null`, `write_resp_score`, etc. in `connection.rs`. It is
  also where a parse `Err(String)` becomes a wire-format `-ERR ...\r\n` reply:
  `write_resp_err` passes the message through unchanged if it already starts with a known
  error-code word (`WRONGTYPE`, `CROSSSLOT`, `MOVED`, `ASK`, `NOSCRIPT`, `EXECABORT`,
  `BUSYGROUP`, `ERR`), and otherwise prepends `"ERR "`.
- **`src/replication.rs`**: A replica's streaming-sync loop calls `parse_command` on the byte
  stream received from its master to decode each replicated mutation (and to recognize
  `REPLCONF GETACK` so it can reply with its own offset). `src/connection.rs`'s
  replica-tracking loops (on the master side) likewise call `parse_command` on the reverse
  channel to decode `REPLCONF ACK` from each connected replica.
- **`src/aof.rs`**: `replay_aof` calls `parse_command` in a loop to decode persisted commands
  back out of an AOF file during startup replay, independent of any live network connection.
- **`src/server.rs`**: The AF_XDP kernel-bypass RX path (`src/xdp.rs`, see
  [`10_kernel_bypass_xdp.md`](10_kernel_bypass_xdp.md)) also calls `parse_command` on the
  payload extracted from each raw kernel-bypassed packet before routing it to a shard — the
  same parser serves the io_uring socket path and the XDP fast path identically.
- **`src/table.rs`**: Several `Command` variant fields carry types defined in `table.rs`
  directly (e.g. `Zadd`'s `flags: crate::table::ZAddFlags`, `Zrange`'s
  `opts: crate::table::ZRangeOpts`, stream commands' `crate::table::StreamId`), so this file
  and the storage engine share vocabulary rather than each redefining it.
- **`src/block.rs`**: `ClientSubcommand::Unblock` carries a `crate::block::ClientUnblockType`
  defined in the blocking-operations module.
- **`src/search.rs` / `src/xdp.rs`**: `FT.*` and `XDP.*` command variants likewise embed
  reducer/action enums (`crate::search::Reducer`, `crate::xdp::XdpAction`) owned by those
  modules rather than duplicating them in `resp.rs`.

---

### 6. Testing

`resp.rs` carries its own `#[cfg(test)] mod tests` at the bottom of the file
(`src/resp.rs:8235`–`9155`, 13 `#[test]` functions), covering RESP array parsing (including the
`test_resp_get`/`test_resp_set_and_put` round trips), partial-frame handling, and the fast-path
argument extraction. Broader coverage of command *execution* (as opposed to parsing) lives in
`connection.rs`'s own much larger test module, which exercises `parse_command` end to end
against real client-visible behavior.

---

### 7. Future Improvements

- **High — enforce a maximum bulk-string/array length.** `parse_resp_array` decodes `arg_len`/
  `num_args` with `parse_decimal_bytes` (§4.4), which guards against `usize` overflow but not
  against a large-but-valid length: a client claiming a multi-gigabyte bulk string still makes
  the server attempt to buffer that much data before giving up. A `proto-max-bulk-len`-
  equivalent check (reject the frame early if the declared length exceeds a configurable cap)
  is a small, high-value change given this is the very first thing untrusted input touches.
  Tracked as current behavior (not yet fixed) in [`docs/design/03_resp_engine.md`](../design/03_resp_engine.md) §5.
- **Medium — split `build_command`'s ~6,470-line single match into per-family functions**
  (strings, hashes, lists, sets, zsets, streams, cluster/ACL, scripting, vector/search,
  tiering, Memcached, ...), dispatched from a smaller top-level match on a coarse prefix or
  category lookup. Purely a maintainability change (§4.5 already notes the compiler handles
  the current single match fine performance-wise) — the real motivation is that a 6,470-line
  function is a place bugs hide, not a place they're compiled away.
- **Medium — reconcile the `GET`-vs-`MemcachedGet` and `DEL`-vs-`MemcachedDelete` arity-based
  disambiguation (§4.5) with a config flag.** Silently reinterpreting `GET k1 k2` as a
  Memcached multi-get is convenient for dual-protocol support but means a genuine Redis client
  typo (`GET` with accidentally-extra arguments) gets a different error message than real Redis
  would give, which could confuse debugging. Consider gating the Memcached-arity fallback
  behind an explicit "Memcached gateway enabled" flag so pure-Redis deployments get real
  Redis-compatible arity errors.
- **Low — add RESP3 *input* parsing** (maps `%`, sets `~`, doubles `,`, booleans `#`, nulls
  `_`) if any planned feature needs a client to send a RESP3-typed argument rather than only
  receive RESP3-typed replies — not needed today since every real Redis command is still sent
  as a flat bulk-string array, but worth flagging as the one genuine protocol-completeness gap
  versus a full RESP3 implementation.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Inline commands split on whitespace; commands with spaces inside arguments
  must be formatted as RESP bulk arrays.
* **Gotcha 2**: RESP3 push frames use the `>` prefix (e.g. `>3\r\n` for a Pub/Sub `smessage`)
  — but that encoding happens in `src/pubsub.rs`/`src/connection.rs`, never in `resp.rs`,
  which does not parse or emit RESP3 framing at all (§3.1).
* **Gotcha 3**: A ≤16-argument fast path in `parse_resp_array` short-circuits `build_command`
  entirely for `GET`, `SET` (plain form only), `DEL`, `INCR`, `HGET`, `HSET`, `SADD`, `LPOP`,
  `ZADD`, `PING`, `MGET`, `MSET`, `LPUSH`, `EXISTS`, `LRANGE`, `ZRANGE`, and `SISMEMBER` (§4.3).
  Any variant form of these commands (e.g. `SET key value EX 10`) or any command not on this
  list still goes through the general `build_command` dispatch — the fast path is an
  optimization, not an alternate command surface.
* **Gotcha 4**: A malformed RESP frame (bad array/bulk-string length, missing `$`, missing
  trailing `\r\n`) is returned as `Err` *without* advancing the input buffer (§2.3.3 of the
  design doc). Code that calls `parse_command` in a loop must itself decide how to make
  progress after an error (clear the buffer, close the connection, etc.) — `resp.rs` will
  keep re-reporting the same error on the same bytes otherwise.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
