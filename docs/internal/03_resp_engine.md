# Component 03: RESP Protocol Engine & Command Parser (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/resp.rs` (14,357 lines)
> **High-Level Design Spec**: [`docs/design/03_resp_engine.md`](../design/03_resp_engine.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)
>
> This document was rewritten from a full pass over the current `src/resp.rs` (not carried
> forward from a prior version). Every line number, count, and code excerpt below was verified
> against the file as it exists today. The file has roughly **doubled** since the previous
> revision of this document (was 9,155 lines / 108 `Command` variants / ~6,470-line
> `build_command`; is now 14,357 lines / **369** `Command` variants / **~10,719-line**
> `build_command`).

---

## 1. Source Module Map & Responsibilities

`src/resp.rs` contains no reply-encoding code; it is entry parsing and command construction
only. Reply serialization (RESP2/RESP3) is implemented in `src/connection.rs`
(`write_resp_*` free functions) and, for Pub/Sub push frames specifically, in `src/pubsub.rs`.

| Region (lines) | Responsibility | Key Functions / Types |
| :--- | :--- | :--- |
| 1–349 | Shared subcommand/option enums used by `Command` variants, plus two small helper structs and two free functions | 27 enums (`ClusterSubcommand`, `DflyClusterSubcommand`, `DflyMigrateSubcommand`, `SetCondition`, `MsetexCondition`, `MsetexExpiry`, `IncrexIncrement/Bound/Expire`, `MemorySubcommand`, `TierSubcommand`, `ClientSubcommand`, `ClientReplyMode`, `LatencySubcommand`, `AclSubcommand`, `ObjectSubcommand`, `XinfoSubcommand`, `HexpireCondition`, `HFieldExpireOpt`, `HsetexCondition`, `VsimTarget`, `BitfieldOpType/Overflow`, `LmovemMode/Ordering`, `XnackMode`, `SetSlotSubcommand`), `ExpireOptions`/`BitfieldSubOp` structs, `parse_expire_options`, `parse_integer` |
| 350–1784 | The command surface itself | `pub enum Command` — **369 variants**, ~1,435 lines |
| 1786–1896 | Memcached storage-command header/body parser | `fn parse_memcached_storage_command` |
| 1897–1913 | Top-level entry point / grammar dispatch | `pub fn parse_command` |
| 1914–1928 | Allocation-free decimal decoder | `pub fn parse_decimal_bytes` |
| 1929–1958 | Allocation-free ASCII-uppercase folding + the `proto-max-bulk-len` global | `pub fn bytes_to_uppercase_ascii`, `PROTO_MAX_BULK_LEN`, `get_/set_proto_max_bulk_len` |
| 1959–2263 | RESP array parser (two-pass scan + ≤16-arg fast path) | `fn parse_resp_array` |
| 2264–2284 | Inline (space-separated) command parser | `fn parse_inline_command` |
| 2286–2314 | `LMOVEM`'s trailing-option parser | `fn parse_lmovem_trailer` |
| 2316–13034 | The command constructor / dispatch table | `pub fn build_command` — **346 top-level match arms**, ~10,719 lines |
| 13035–13075 | Redis-compatible float parsing (libc `strtod` fallback) | `pub fn parse_redis_f64` |
| 13076–13094 | `ZRANGEBYSCORE`-style `(exclusive` bound parsing | `pub fn parse_score_bound` |
| 13096–13108 | CRLF header scanning | `fn find_crlf`, `fn find_crlf_at` |
| 13110–14357 | Unit tests (15 `#[test]` functions) | `mod tests` |

---

## 2. What Changed Since the File Roughly Doubled

A `git log`-independent read of the current source shows these are now present and were **not**
described (or were described differently) in earlier passes over this file:

1. **A real `proto-max-bulk-len` enforcement** (`src/resp.rs:1947–1957`) — the single biggest gap
   flagged by the previous revision of this document is now closed. See §4.3 step 2 and §7.
2. **Hash field TTLs**: `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT`, `HTTL`/`HPTTL`/
   `HEXPIRETIME`/`HPEXPIRETIME`, `HPERSIST`, `HGETEX`, `HSETEX` — Redis 7.4/8-style per-field
   expiry on hashes (§3.3).
3. **Stream consumer-group claim commands**: `XCLAIM`, `XAUTOCLAIM` plus newer Redis 8
   reference-counted deletion commands `XDELEX`, `XACKDEL`, and a Rudis-specific
   `XIDMPRECORD` (§3.3).
4. **Redis 8 Vector Sets** (`VADD`/`VSIM`/`VEMB`/`VLINKS`/`VGETATTR`/`VSETATTR`/... — 14 `V*`
   variants) layered on top of the pre-existing Rudis-native vector command grammar, sharing a
   single `Command::Vadd` variant with two entirely different argument grammars distinguished by
   content-sniffing, not by command name (§3.5, a real gotcha).
5. **A full AI-native command surface actually lives in `resp.rs`**: semantic cache
   (`SEMANTIC.*`, 5 variants), agent memory (`AGENT.MEM.*`, 5), LLM quota governance
   (`LLM.QUOTA.*`, 3), agent checkpointing (`AGENT.CHECKPOINT.*` / `AGENT.TOOL.*`, 5), and MCP
   (`MCP.TOOLS`/`MCP.CALL`/`MCP.RPC`, 3) — 21 variants total, parsed here and executed by
   `src/agent.rs`/`src/semcache.rs`/`src/mcp.rs` (see subsystem 20).
6. **CRDT multi-region commands** (`CRDT.SET`/`GET`/`DEL`/`INCRBY`/`SADD`/`SMEMBERS`/`SREM`/
   `MERGE`/`GC`/`DUMP` — 10 variants).
7. **Redis 7 Functions** (`FUNCTION LOAD`/`FCALL`/`LIST`/`DELETE`/`FLUSH`, plus `FUNCTION STATS`/
   `KILL` that live in the "unsectioned" prelude of the enum).
8. **RedisJSON** (`JSON.*`, 16 variants), **Geospatial** (`GEOADD`/`GEODIST`/.../
   `GEOSEARCHSTORE`, 8 variants), **Probabilistic** (`BF.*`/`CF.*`/`CMS.*`/`TOPK.*`, 22
   variants), **Full-text search** (`FT.*`, 10 variants — `FT.HYBRID` is parsed but is *not* its
   own `Command` variant, see §3.6), **AF_XDP control** (`XDP.*`, 8 variants), and **Dragonfly
   native extensions** (`DFLYCLUSTER`/`DFLYMIGRATE`/`DFLY FLOW`/`STICK`/`UNSTICK`/`STICKY`/
   `DELEX`, 7 variants).
9. **The ≤16-arg fast path grew from 17 to 20 fast-pathed command names** — `UNLINK`,
   `READONLY`, `READWRITE` were added alongside the original 17 (§4.3 step 3).
10. **The error-reply prefix whitelist in `connection.rs`'s `write_resp_err`** grew from 7 to 9
    recognized prefixes (`NOGROUP`, `INVALIDOBJ` added; see §6).
11. **`Command::Unlink` is dead from the parser's perspective** — `build_command` normalizes a
    client-sent `UNLINK` to `Command::Del`, exactly like `DEL`. `Command::Unlink` is only ever
    constructed internally, by `src/table.rs`'s lazy-expiry path, to give a distinct AOF/
    replication command name when `lazyfree-lazy-expire` is active (§3.4, a real gotcha).

---

## 3. Command Surface & Data Structures

### 3.1 What `parse_command` actually recognizes

```text
First byte        Grammar                                   Handler
──────────────────────────────────────────────────────────────────────────────
'*'               RESP array: *N\r\n($len\r\ndata\r\n){N}    parse_resp_array
otherwise         Try Memcached "set/add/replace key         parse_memcached_storage_command
                  flags exptime bytes [noreply]\r\n<data>\r\n"
otherwise         Space/tab-separated inline text             parse_inline_command
```

There is no separate frame type for simple strings (`+`), errors (`-`), or integers (`:`) on the
*input* side — those are reply-only prefixes written by `connection.rs`, never parsed here,
because a client never sends a command framed that way. None of the RESP3 input types (`%` map,
`~` set, `,` double, `#` boolean, `_` null, `>` push) are recognized on input either;
`resp.rs` never inspects the negotiated protocol version while parsing — protocol-version state
(`CURRENT_CLIENT_RESP3`) lives entirely in `connection.rs` and only affects how replies are
*written*, never how commands are *parsed*.

### 3.2 `enum Command`: 369 variants, grouped by the file's own section comments

`enum Command` spans `src/resp.rs:350`–`1784` (1,435 lines). It derives:

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum Command { /* 369 variants */ }
```

Not `Eq` — several variants (`Zadd`, `Zincrby`, score-range queries, vector-distance fields,
etc.) carry `f64` fields, and `f64` has no total order (`NaN != NaN`), so `Eq` can't be derived.

The enum has **30** `// SECTION NAME` comment markers inside its body (`grep -n "^    // [A-Z]"
src/resp.rs`, restricted to the 350–1784 range). The first 55 variants (lines 350–526) sit
*before* the first section comment — an unlabeled prelude covering `OBJECT`/`XINFO`/`LATENCY`
subcommand wrappers, the new hash-field-expiry family, `XCLAIM`/`XAUTOCLAIM`, `AUTH`, `GET`,
`SET`, `MSETEX`, `LCS`, `DEL`/`UNLINK`/`EXISTS`, `EXPIRE`, `COPY`, `CLUSTER`/`CLIENT` wrappers,
`WAIT`/`WAITAOF`, `MIGRATE`, and the base `HSET`/`HGET`/... hash commands. Exact counts per
section (verified by counting variant-start lines against each section boundary):

| Variants | Section | Variants | Section |
| -: | :--- | -: | :--- |
| 55 | *(unsectioned prelude — see above)* | 5 | AI SEMANTIC CACHE COMMANDS |
| 11 | LIST COMMANDS | 5 | AI AGENT MEMORY COMMANDS |
| 15 | SET COMMANDS | 3 | LLM QUOTA GOVERNOR COMMANDS |
| 23 | ZSET COMMANDS | 5 | AGENT CHECKPOINT & IDEMPOTENT TOOL EXECUTION |
| 9 | GENERIC & DATABASE COMMANDS | 3 | MODEL CONTEXT PROTOCOL (MCP) COMMANDS |
| 18 | EXTENDED STRING COMMANDS | 10 | CRDT MULTI-REGION COMMANDS |
| 5 | LUA SCRIPTING COMMANDS | 5 | REDIS 7 FUNCTIONS |
| 1 | TIERED STORAGE COMMANDS | 16 | REDISJSON COMMANDS |
| 4 | CONFIG COMMANDS | 8 | GEOSPATIAL COMMANDS |
| 13 | PUBSUB COMMANDS | 22 | PROBABILISTIC COMMANDS |
| 4 | KEYSPACE INSPECTION | 10 | Full-Text Search (FT.*) |
| 5 | TRANSACTIONS | 8 | AF_XDP & eBPF (XDP.*) |
| 7 | BITMAP COMMANDS | 7 | Dragonfly native extensions |
| 8 | HYPERLOGLOG COMMANDS | 12 | Memcached Protocol |
| 2 | RDB SERIALIZATION | | |
| 22 | STREAM COMMANDS | | |
| 34 | VALKEY EXTENDED COMMANDS | | |
| 14 | VECTOR COMMANDS | | |

**369 total**, cross-checked two independent ways (a brace-depth walk of the enum body, and a
plain `^    [A-Z]` prefix scan) — both agree exactly.

A representative slice (field shapes are real, taken verbatim):

```rust
pub enum Command {
    Object(ObjectSubcommand),
    Xinfo(XinfoSubcommand),
    Latency(LatencySubcommand),
    ...
    Hexpire { key: Bytes, expire_ms: i64, is_at: bool, condition: HexpireCondition, fields: Vec<Bytes> },
    Httl { key: Bytes, is_ms: bool, is_expiretime: bool, fields: Vec<Bytes> },
    Hpersist { key: Bytes, fields: Vec<Bytes> },
    Hgetex { key: Bytes, expire: HFieldExpireOpt, fields: Vec<Bytes> },
    Hsetex { key: Bytes, condition: HsetexCondition, expire: HFieldExpireOpt, pairs: Vec<(Bytes, Bytes)> },
    Xclaim { key: Bytes, group: Bytes, consumer: Bytes, min_idle_time: u64, ids: Vec<Bytes>,
             idle: Option<u64>, time: Option<u64>, retrycount: Option<usize>, force: bool, justid: bool },
    ...
    Del(SmallVec<[Bytes; 1]>),       // inline-capacity 1: the common single-key case never allocates
    Unlink(SmallVec<[Bytes; 1]>),    // see §3.4 — never built by the parser itself
    Exists(SmallVec<[Bytes; 1]>),
    ...
    MemcachedSet { key: Bytes, flags: u32, exptime: u32, bytes: usize, noreply: bool, data: Bytes },
    ...
    Memory(MemorySubcommand),
    Unknown(String),
}
```

Several variants deliberately embed types owned by other modules rather than redefining them
locally: `Zadd`'s `flags: crate::table::ZAddFlags`, `Zrange`'s `opts: crate::table::ZRangeOpts`,
stream commands' `crate::table::StreamId`/`crate::table::StreamTrimStrategy`, `ClientSubcommand
::Unblock`'s `crate::block::ClientUnblockType`, `FT.*` commands' `crate::search::Reducer`,
`XDP.*` commands' `crate::xdp::XdpAction`, and `VADD`/`VSIM`'s `crate::vector::VQuant` /
`crate::vector::VectorMetric`.

### 3.3 New command families, in detail

**Hash field TTLs** (`HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT` at `src/resp.rs:7840`): one
match arm handles all four spellings. Seconds-vs-milliseconds and absolute-vs-relative are
derived purely from which of the four strings `cmd_name` equals (`raw_time.saturating_mul(1000)`
for the two second-granularity spellings; `is_at = matches!(cmd_name, "HEXPIREAT" |
"HPEXPIREAT")`). Syntax is `HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field
[field ...]`: the function walks condition flags until it hits the literal `FIELDS` token,
requires `numfields` to match the actual trailing field count exactly (too few is a "wrong
number of arguments" error, too many/duplicated `FIELDS`/condition flags after the field list is
a distinct "unknown argument" error), rejecting more than one condition flag
("Multiple condition flags specified"). `HTTL`/`HPTTL`/`HEXPIRETIME`/`HPEXPIRETIME` share the
same `FIELDS numfields field...` tail grammar (`src/resp.rs:7947`); `HPERSIST` (`:7977`) and
`HGETEX`/`HSETEX` (`:7994`, `:8104`) are the remaining field-TTL family members.

**Stream claim/delete commands**: `XCLAIM` (`src/resp.rs:8236`) and `XAUTOCLAIM` (`:8338`) are
the standard Redis consumer-group reassignment commands. `XDELEX key [KEEPREF|DELREF|ACKED] IDS
numids id [id...]` (`:7229`) and `XACKDEL key group [KEEPREF|DELREF|ACKED] IDS numids id
[id...]` (`:7299`) are the Redis 8 "delete with PEL-reference-tracking strategy" commands: the
strategy token is optional and defaults to `KeepRef` (`crate::table::StreamTrimStrategy`), and —
like `HEXPIRE` — `numids` must exactly match the number of trailing ID arguments or the call is
rejected. `XIDMPRECORD key producer-id insertion-id id` (`:7370`) is a Rudis-specific
idempotent-record command (not part of upstream Redis/Valkey) taking exactly 5 arguments.

**Redis 8 Vector Sets and AI-native command families** are covered in §3.5 and §3.7.

### 3.4 Gotcha: `Command::Unlink` is never constructed by the parser

```rust
"DEL" | "DELETE" | "UNLINK" => {                       // src/resp.rs:2905
    if args.len() < 2 {
        return Err("wrong number of arguments for 'del' command".to_string());
    }
    if cmd_name == "DELETE" {
        let noreply = args.len() > 2 && args[2].eq_ignore_ascii_case(b"noreply");
        Ok(Some(Command::MemcachedDelete { key: args[1].clone(), noreply }))
    } else {
        args.remove(0);
        Ok(Some(Command::Del(SmallVec::from_vec(args))))   // UNLINK falls in here too
    }
}
```

A client-sent `DEL` *or* `UNLINK` both become `Command::Del` — there is no behavioral
distinction at parse time (both take the same fast path too, see §4.3). `Command::Unlink` does
exist as a real, separately-matched variant throughout `connection.rs` (its execution arms are
written as `Command::Del(keys) | Command::Unlink(keys) => ...`, so it behaves identically once
built), but the *only* place that ever constructs it is `src/table.rs`'s lazy-expiry path:

```rust
// src/table.rs:4429 (and :4665, an identical second occurrence)
let rep_cmd = if is_lazyfree_lazy_expire() {
    Command::Unlink(smallvec::smallvec![key.clone()])
} else {
    Command::Del(smallvec::smallvec![key.clone()])
};
```

I.e. `Command::Unlink` exists purely so the storage engine can propagate a lazily-expired key to
AOF/replicas under the *name* `UNLINK` (for observability/compatibility with real Redis's
`lazyfree-lazy-expire` semantics) without the parser itself needing any UNLINK-specific
behavior. `src/aof.rs:276` and `src/connection.rs:3998` (`"UNLINK"` display name),
`:5614`/`:5629` (propagation) are the consumers of this internally-synthesized variant.

### 3.5 Gotcha: `VADD` has two unrelated argument grammars selected by content-sniffing

`Command::Vadd` is built by one match arm (`src/resp.rs:9556`) that supports **both**:

1. **Redis 8 Vector Sets syntax**: `VADD key [REDUCE dim] FP32|VALUES <data> element [CAS]
   [NOQUANT|Q8|BIN] [EF ef] [SETATTR json] [M m] [TIERED]` — vectors arrive either as a raw
   little-endian `f32` blob (`FP32`, validated to be a multiple of 4 bytes and each float
   finite) or as `VALUES num v1 v2 ... vnum` (space-separated floats). Sets `is_redis_vset:
   true`.
2. **Rudis-native legacy syntax**: `VADD key element v1 v2 ... [QUANTIZE|SQ8] [PQ] [TIERED]
   [metric]` — the vector is just the trailing numeric tokens themselves, and `quant`/`ef`/
   `setattr`/`m`/`cas` are all left at their defaults (`None`/`false`). Sets
   `is_redis_vset: false`.

The dispatch between the two is **not** based on the command name (both are `VADD`) — it checks
whether `args[2]` (the 3rd argument) uppercases to exactly `"REDUCE"`, `"FP32"`, or `"VALUES"`.
Any other 3rd argument is assumed to be a vector element name in the legacy grammar. This means
a legacy-style element literally named `reduce`, `fp32`, or `values` would misparse as the
Vector Sets grammar — a real (if narrow) ambiguity in the wire protocol, not a doc inaccuracy.

### 3.6 Gotcha: `FT.HYBRID` is parsed but has no `Command` variant of its own

`"FT.HYBRID" => { ... }` (`src/resp.rs:12082`) parses `FT.HYBRID index text-query vector-query
[SCORER RRF|LINEAR ...] [LIMIT ...] [RETURN ...] [PARAMS ...] [WITHSCORES] [NOCONTENT]`, builds
a `crate::search::SearchOptions` (RRF `k` defaults to `60.0`; `LINEAR` alpha/beta default to
`0.5`/`0.5`), and then **rewrites the two queries into one combined query string** before
constructing a plain `Command::FtSearch`:

```rust
let query = if let Some(knn_tail) = trimmed_vec.strip_prefix("*=>").or_else(|| trimmed_vec.strip_prefix("=>")) {
    format!("({})=>{}", text_query, knn_tail)
} else if trimmed_vec.starts_with("[KNN") {
    format!("({})=>{}", text_query, trimmed_vec)
} else {
    format!("{} {}", text_query, trimmed_vec)
};
...
Ok(Some(Command::FtSearch { index, query, options }))
```

So `FT.HYBRID` is purely a **parse-time query-string-rewriting convenience** on top of
`FT.SEARCH`'s existing execution path in `src/search.rs` — there is no `Command::FtHybrid`
variant and no hybrid-specific execution code downstream of `resp.rs`.

### 3.7 AI-native command families actually live in `resp.rs`

Despite being executed by `src/agent.rs`/`src/semcache.rs`/`src/mcp.rs` (subsystem 20), all of
their wire parsing is ordinary `build_command` arms in this file, following the same patterns as
everything else: `SEMANTIC.SET`/`GET`/`DEL`/`FLUSH`/`INFO` (`src/resp.rs:12224`–`12441`),
`AGENT.MEM.ADD`/`CONTEXT`/`COMPACT`/`INFO`/`CLEAR` (`:12442`–`12668`), `LLM.QUOTA.RESERVE`/
`SETTLE`/`INFO` (`:12669`–`12772`), `AGENT.CHECKPOINT.PUT`/`GET`/`HISTORY` and
`AGENT.TOOL.CLAIM`/`COMPLETE` (`:12773`–`12943`), and `MCP.TOOLS`/`CALL`/`RPC` (`:12944`–
`12961`). `MCP.RPC` is the odd one out structurally — it takes exactly one argument and stores it
unparsed as `Command::McpRpc(Bytes)`, deferring all JSON-RPC 2.0 envelope parsing to `mcp.rs`
itself, whereas every other command in this family parses its arguments here.

### 3.8 `XDP.RULE` breaks the file's own Subcommand-enum convention

Every other multi-verb command family with real subcommands (`OBJECT`, `CLUSTER`, `CLIENT`,
`ACL`, `LATENCY`, `XINFO`, `MEMORY`, `TIER`) wraps its subcommands in a dedicated
`*Subcommand` enum and a single `Command::Foo(FooSubcommand)` variant. `XDP.RULE ADD|DEL|LIST`
(`src/resp.rs:12969`) instead dispatches its subcommand inline inside the `"XDP.RULE"` match arm
and constructs three *separate* top-level `Command` variants (`XdpRuleAdd`, `XdpRuleDel`,
`XdpRuleList`) rather than one `Command::XdpRule(XdpRuleSubcommand)` — a structural inconsistency
worth knowing about if extending either family.

---

## 4. Parsing Algorithms & Code Logic

### 4.1 `parse_command`: three grammars, tried in a fixed order (`src/resp.rs:1897`)

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

This is the sole entry point every ingestion loop in the codebase calls — see §5 for the full,
verified list of 9 call sites across 4 files.

### 4.2 `parse_memcached_storage_command`: a triple-layered `Option` result (`src/resp.rs:1786`–`1896`)

Returns a triple-layered result deliberately: `Ok(None)` means "not a memcached storage command
at all, try inline parsing next"; `Ok(Some(None))` means "it *is* one, but the data block hasn't
fully arrived yet — wait for more bytes, don't fall through to inline parsing"; `Ok(Some(Some
(cmd)))` is a complete parse. That extra `Option` layer specifically prevents a partially-
received memcached `set key 0 0 1024\r\n<...only 200 bytes so far...>` from being misinterpreted
as inline text.

The header line must tokenize into at least 5 whitespace-separated fields (`<verb> <key> <flags>
<exptime> <bytes>`, with an optional 6th `noreply` token, matched case-insensitively against
`set`/`add`/`replace`); fewer than 5 tokens or non-numeric `flags`/`exptime`/`bytes` returns
`Ok(None)` (falls through to inline parsing) rather than an error. Once the header is valid and
the full `bytes`-length data block plus its trailing `\r\n` has arrived, key and data are each
copied with `Bytes::copy_from_slice` (a real copy — this path trades allocation for simplicity,
same as the inline-text path) into `Command::MemcachedSet`/`MemcachedAdd`/`MemcachedReplace`.

### 4.3 `parse_resp_array`: two-pass validity scan, then a ≤16-arg fast path or a generic path (`src/resp.rs:1959`–`2263`)

1. **Read the array-length header.** `find_crlf` locates the first `\r\n`; the digits between
   `*` and it decode via `parse_decimal_bytes` (§4.4) into `num_args`. A non-digit/empty header
   is `Err("Invalid array length in RESP frame")`.
2. **Pass 1 — prove the frame is complete, enforce `proto-max-bulk-len`, and opportunistically
   record offsets.** A fixed `[(usize, usize); 16]` stack array (`offsets`) and `is_small =
   num_args <= 16` are set up before the scan. The loop walks each of the `num_args` bulk-string
   headers (`$<len>\r\n<data>\r\n`) with `find_crlf_at`, validating the `$` prefix, decoding the
   length with `parse_decimal_bytes`, **rejecting it outright if `arg_len >
   get_proto_max_bulk_len()`** (`Err("Protocol error: excessive bulk string length")` —
   this check runs unconditionally for every argument of every array, small or large, since it's
   in the shared pass-1 loop), and checking the trailing `\r\n` after the data
   (`"Expected bulk string in command array"`, `"Invalid bulk string length"`,
   `"Expected CRLF after bulk string data"`, or `Ok(None)` if the buffer runs out mid-scan).
   **Nothing in this pass mutates `buf`.** If `is_small`, each argument's `(data_start, arg_len)`
   is cached into `offsets[i]` as the scan proceeds.
3. **Pass 2a — small-array fast path (`num_args <= 16`).** The whole frame is split off the
   receive buffer in one `buf.split_to(scan_cursor).freeze()` call, producing a single `Bytes`
   (`frame`) that owns one shared allocation. If `num_args > 0`, the uppercase-folded first
   argument's byte length (`cmd_len`) is matched, and within each length bucket a small number of
   `eq_ignore_ascii_case` checks against the highest-frequency command names build the matching
   `Command` variant directly from `frame.slice(..)` views — no call into `build_command` at all.
   The current fast-path command list (**20 commands**, up from 17):

   | Byte length | Commands |
   | :-: | :--- |
   | 3 | `GET` (exact 2 args), `SET` (exact 3 args, plain form only), `DEL` (2+ args) |
   | 4 | `INCR`, `HGET`, `HSET` (single field/value), `SADD` (single member), `LPOP` (no count), `ZADD` (single score/member), `PING` (no argument), `MGET`, `MSET` |
   | 5 | `LPUSH` (single value) |
   | 6 | `EXISTS`, `LRANGE`, `ZRANGE` (plain `start`/`stop` form), `UNLINK` (2+ args — also builds `Command::Del`, per §3.4) |
   | 8 | `READONLY` (no argument) |
   | 9 | `READWRITE` (no argument), `SISMEMBER` |

   Each fast-path arm still checks arity/shape (`SET key value EX 10` falls through — 5 args,
   not 3). Anything that doesn't match falls through to building a generic `Vec<Bytes>` from the
   cached `offsets` (still zero-copy, via `frame.slice(..)`) and calling `build_command`.
4. **Pass 2b — general path (`num_args > 16`).** `buf.advance(newline_pos + 2)` consumes the
   header line, and each argument is decoded and sliced off one at a time with its own
   `buf.split_to(arg_len).freeze()` call, before calling `build_command`.

Both paths end up calling only `Bytes`-level operations (`frame.slice(..)`,
`BytesMut::split_to(..).freeze()`) — O(1) reference-count bumps, never a byte copy. The fast
path's advantage is doing **one** `split_to` for the whole command instead of one per argument,
plus skipping `build_command`'s dispatch entirely for the highest-frequency verbs.

`parse_inline_command` (`src/resp.rs:2264`–`2284`) splits on spaces/tabs and builds each
argument with `Bytes::copy_from_slice` — a real copy, because this path only serves
interactive/debugging clients (`redis-cli`, `nc`), never the benchmarked pipelined workload. It
delegates to `build_command` exactly like the general RESP-array path.

### 4.4 Allocation-free decode helpers (`src/resp.rs:1914`–`1958`)

```rust
#[inline]
pub fn parse_decimal_bytes(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() { return None; }
    let mut val: usize = 0;
    for &b in bytes {
        if !b.is_ascii_digit() { return None; }
        val = val.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(val)
}
```

Used everywhere a RESP length prefix needs to become a `usize`. No UTF-8 validation pass, no
intermediate `&str`. `checked_mul`/`checked_add` make integer overflow return `None` (a parse
error) rather than silently wrapping — this remains the *only* overflow guard in the
length-decoding path; see §7 for what's still not bounded.

```rust
#[inline]
pub fn bytes_to_uppercase_ascii<'a>(bytes: &'a [u8], buf: &'a mut [u8; 64], heap: &'a mut String) -> &'a str {
    if bytes.len() <= 64 && bytes.is_ascii() {
        for (i, b) in bytes.iter().enumerate() { buf[i] = b.to_ascii_uppercase(); }
        unsafe { std::str::from_utf8_unchecked(&buf[..bytes.len()]) }
    } else {
        *heap = String::from_utf8_lossy(bytes).to_uppercase();
        heap.as_str()
    }
}
```

Used by `build_command` to case-fold the command-name argument. Every real command name is
short, plain ASCII, and fits the 64-byte stack buffer, so the common case never touches the
heap; the `unsafe` block is justified by the preceding `is_ascii()` check (ASCII-uppercasing an
ASCII byte cannot produce a non-ASCII byte, so the written bytes are valid UTF-8).

```rust
pub static PROTO_MAX_BULK_LEN: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(512 * 1024 * 1024);   // 512 MiB, matches real Redis's default

#[inline(always)]
pub fn get_proto_max_bulk_len() -> usize { PROTO_MAX_BULK_LEN.load(Ordering::Relaxed) }
pub fn set_proto_max_bulk_len(val: usize) { PROTO_MAX_BULK_LEN.store(val, Ordering::Relaxed); }
```

A global, runtime-mutable cap enforced in `parse_resp_array`'s pass 1 (§4.3 step 2). It's also
reused outside the parser proper: `SETBIT`/`GETBIT`/`BITFIELD`'s offset-in-bits checks in
`build_command` (`src/resp.rs:6261`, `6288`) and a symmetric check in `connection.rs:6490`
compare a bit offset shifted right by 3 against this same limit, so one config knob bounds both
"how big can a bulk string argument be" and "how big can a bitmap grow via `SETBIT`". Exposed to
clients via `CONFIG GET/SET proto-max-bulk-len` (`src/connection.rs:6430`–`6527`).

### 4.5 `build_command`: one shared constructor, ~10,719 lines, dispatched by uppercased name (`src/resp.rs:2316`–`13034`)

```rust
pub fn build_command(mut args: Vec<Bytes>) -> Result<Option<Command>, String> {
    if args.is_empty() { return Ok(None); }
    let mut cmd_buf = [0u8; 64];
    let mut cmd_heap = String::new();
    let cmd_name = bytes_to_uppercase_ascii(&args[0], &mut cmd_buf, &mut cmd_heap);
    match cmd_name {
        "AUTH" => { ... }
        "ACL" => { ... }
        "GET" => { ... }
        // ... 343 more top-level arms ...
        _ => Ok(Some(Command::Unknown(cmd_name.to_string()))),
    }
}
```

`args: Vec<Bytes>` is `mut` because several arms (`DEL`/`UNLINK`, see §3.4) call
`args.remove(0)` in place. The command-name lookup goes through `bytes_to_uppercase_ascii`
(§4.4), not `String::from_utf8_lossy(..).to_uppercase()`, which would allocate on every
dispatched command.

The top-level `match cmd_name { .. }` has **346 arms** (verified by counting lines of the exact
shape `^        "NAME"( | "ALIAS")* =>` — some names contain `.`/`-`, e.g. `"CRDT.SET"`,
`"DEFRAG" | "ACTIVE-DEFRAG"`), covering **373 distinct command-name string literals** once
`|`-joined aliases are counted individually (e.g. `"DEL" | "DELETE" | "UNLINK"`,
`"HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT"`). The function body spans
`src/resp.rs:2316`–`13034`, roughly **10,719 lines** — up from ~6,470 the last time this file
was measured; the single-match-statement design has not changed, only grown. Arm order is
roughly the order features were added over time, **not** grouped by the enum's own section
comments — e.g. Memcached `STATS`/`VERSION`/`CONFIG` arms sit textually near the Dragonfly
`STICK`/`UNSTICK`/`DELEX` arms, and the real Redis `CONFIG GET`/`SET` arm is a single,
separate `"CONFIG"` match a few lines later — so §3.2's category table (enum declaration order)
and `build_command`'s arm order are two independent orderings over the same command set.

Nested dispatch is common: most arms that take options (`SET .. EX ..`, `ZADD .. GT LT NX ..`,
`HELLO AUTH ..`, `HEXPIRE .. FIELDS ..`) run their own `while i < args.len() { match
uppercased_option { .. } }` loop over the remaining arguments after fixed positional arguments
are consumed. Subcommand families (`ACL`, `CLUSTER`, `XDP.RULE`, `DFLY`, `DFLYCLUSTER`) do the
same one level deeper, matching on `args[1]` inside the outer arm.

`GET`/`MemcachedGet` and `DEL`/`MemcachedDelete` disambiguate by arity, not by a protocol-mode
flag: real Redis `GET`/`DEL` are strictly defined arities, so extra arguments unambiguously mean
the Memcached dialect was intended instead (`GET k1 k2` → `Command::MemcachedGet`; `DELETE key
[noreply]` → `Command::MemcachedDelete`).

### 4.6 `HELLO`: parsed here, acted on in `connection.rs`

`Command::Hello { proto: Option<u8>, auth: Option<(String, String)>, setname: Option<String> }`
is all `resp.rs` produces. `connection.rs:9325`–`9403` is where `proto` is actually acted on:
`p != 2 && p != 3` → `-NOPROTO unsupported protocol version`; on success it flips both the
per-client `ClientInfo::is_resp3` flag *and* the `CURRENT_CLIENT_RESP3` thread-local (the latter
is what every `write_resp_*` helper actually reads), and replies with a RESP3 map
(`%7\r\n`) or a RESP2 flat array (`*14\r\n`) of the same 7 key/value pairs: `server` (`valkey`),
`version` (`7.2.0`), `proto`, `id`, `mode` (`cluster`/`standalone`), `role` (`master`/`replica`),
`modules` (empty array). `resp.rs` itself never sets or reads either RESP3 flag.

### 4.7 `parse_redis_f64` / `parse_score_bound`: libc `strtod` as a fallback for exact Redis float parsing (`src/resp.rs:13035`–`13094`)

Unchanged in substance from prior revisions of this file, only shifted in line number. Rust's
own `f64::from_str` is tried first; if it fails, the string is handed to the real libc
`strtod` via FFI, because Redis's accepted float grammar (`inf`, `+inf`, `-inf`, `infinity` and
case variants; rejecting `nan` outright; rejecting an out-of-range literal that Rust would
otherwise silently promote to `inf`) needs to match C's parsing rules exactly, and reimplementing
`strtod` by hand is not worth it. `parse_score_bound` layers `ZRANGEBYSCORE`-style `(exclusive`
prefix handling on top: a leading `(` strips itself and marks the bound exclusive; `-inf`/`+inf`/
`inf` are special-cased directly (bypassing `parse_redis_f64`'s NaN/whitespace checks, unneeded
for these literals); everything else delegates to `parse_redis_f64`.

### 4.8 `find_crlf`/`find_crlf_at`: still a plain windowed scan (`src/resp.rs:13096`–`13108`)

Unchanged — a `.windows(2).position(|w| w == b"\r\n")` scan, used only to find the end of small
protocol headers (array lengths, bulk-string length prefixes), never to scan payload data. No
SIMD opportunity is being left on the table here (see the storage engine's SIMD control-byte
matching in `docs/internal/05_storage_engine.md` for where that technique actually applies, on
16-byte groups).

---

## 5. Every Caller of `parse_command` Outside `resp.rs`

Verified by grepping for `resp::parse_command`/`parse_command(` across `src/*.rs` — **8 call
sites across 4 files** (the former separate `handle_tls_connection` call site is gone: TLS clients
now share the generic `handle_client` loop). Line numbers are approximate and drift as the code changes:

| File:Line | Loop | What it does with the parsed `Command` |
| :--- | :--- | :--- |
| `connection.rs` (`handle_client`) | `handle_client<T: ClientTransport>`'s main read loop, shared by plaintext (`PlainTransport`) and TLS (`TlsTransport`) clients | Parses **all** complete commands currently buffered into a `Vec<Command>`, then hands the whole batch to `execute_commands_squashed` — the primary, pipelined client command path, for both transports. |
| `connection.rs:2526` | `run_pubsub_loop`, draining commands already buffered before entering subscribe mode | One-time drain of any pipelined commands that arrived in the same read as the initial `SUBSCRIBE`. |
| `connection.rs:2563` | `run_pubsub_loop`'s steady-state read loop | Parses commands from a client that is in Pub/Sub mode (still accepts `PING`, `SUBSCRIBE`/`UNSUBSCRIBE`, etc.). |
| `connection.rs:2672` | Master-side replica ACK tracking (plain replication) | Decodes `REPLCONF ACK <offset>` frames sent back by a connected replica, feeding `hub.update_replica_ack`. |
| `connection.rs:2753` | Master-side replica ACK tracking (`DFLY FLOW` per-shard replication) | Same as above but for the Dragonfly-style per-shard flow protocol, feeding `hub.update_shard_flow_ack`. |
| `replication.rs:1107` | A replica's streaming-sync loop, reading from its master | Decodes each replicated mutation and applies it via `router.execute_replica_command`; specially recognizes `REPLCONF GETACK` (replies with its own offset) and `PING` (no-op) without executing them as data commands. |
| `aof.rs:1952` | `replay_aof`, at startup | Decodes persisted commands back out of an AOF file and replays them with `execute_local_command`, independent of any live network connection. |
| `server.rs:288` | The AF_XDP kernel-bypass RX path | Parses the payload extracted from each zero-copy packet descriptor (`crate::xdp::extract_transport_payload`) and routes it to the correct shard — the same parser serves the io_uring socket path and the XDP fast path identically. Note: the call itself is in `server.rs`, not `xdp.rs`; `xdp.rs` only supplies the socket/ring machinery and payload extraction. |

`src/mcp.rs` does **not** call `parse_command` — its `MCP.*` commands are parsed as ordinary
`Command` variants here (§3.7), but the JSON-RPC 2.0 envelope *inside* `MCP.RPC`'s single `Bytes`
argument is parsed separately by `mcp.rs` itself.

Other cross-component dependencies:
- **`src/table.rs`**: Several `Command` variant fields carry types defined in `table.rs`
  directly (§3.2), and `table.rs` itself constructs `Command::Unlink`/`Command::Del` internally
  for lazy-expiry replication propagation (§3.4) — a rare case of `table.rs` depending on
  `resp.rs`'s `Command` type rather than the reverse.
- **`src/block.rs`**: `ClientSubcommand::Unblock` carries a `crate::block::ClientUnblockType`.
- **`src/search.rs` / `src/xdp.rs` / `src/vector.rs`**: own the reducer/action/quantization enums
  embedded in `FT.*`, `XDP.*`, and `V*` command variants (§3.2, §3.5).

---

## 6. RESP2 vs RESP3 Reply Encoding (in `connection.rs`/`pubsub.rs`, driven by state this file never touches)

`resp.rs` never emits a reply byte. All encoding lives in `connection.rs`'s `write_resp_*`
family, gated on the thread-local `CURRENT_CLIENT_RESP3: Cell<bool>` (`connection.rs:191`),
which is set once per command batch from the per-client `ClientInfo::is_resp3` flag
(`connection.rs:4917`, `:19114`) and flipped by `HELLO`/`RESET` (§4.6).

Verified, by type, across the whole of `connection.rs`:

| RESP3 type | Used for | RESP2 fallback |
| :-- | :-- | :-- |
| `_\r\n` (null) | `write_resp_null`/`write_resp_null_array` (`:276`, `:285`) | `$-1\r\n` / `*-1\r\n` |
| `,<val>\r\n` (double) | `write_resp_score` (`:324`) — zset scores, geo distances, etc. | a bulk string via `format_score` (handles `inf`/`-inf`/`nan`/`%.17g` via libc `snprintf`, matching `parse_redis_f64`'s own float grammar) |
| `%<N>\r\n` (map) | Ad hoc, wherever a reply is conceptually key/value pairs: `HELLO` (`:9370`, 7 pairs), `FUNCTION STATS` (`:11367`–`11381`, nested maps for the per-engine breakdown), `VINFO` (`:18504`, 9 pairs), `VLINKS ... WITHSCORES` (`:18629`+, per-layer map), `PUBSUB`-adjacent and other `WITHSCORES`-flavored replies | the same data flattened into a `*<2N>\r\n` array of alternating key/value bulk strings |
| `><N>\r\n` (push) | Pub/Sub `message`/`pmessage` frames only, built in **`src/pubsub.rs`** (`build_pubsub_frame`/`build_pubsub_pframe`, `:142`/`:163`), never in `connection.rs` or `resp.rs` | `*3\r\n`/`*4\r\n` (identical payload, ordinary array prefix) |

Grepping the whole of `connection.rs` for RESP3 set (`~`), boolean (`#t`/`#f`), verbatim string
(`=`), and big-number (`(`) prefixes returns **zero matches** — Rudis's RESP3 support is
deliberately partial: null, double, map, and push only. This matches what real clients actually
rely on for scores/maps/messages; it is not a completeness gap by design, just worth knowing
before assuming any RESP3 type is supported.

**Inline commands**: `parse_inline_command` (§4.3) splits strictly on spaces/tabs, so a command
argument containing a space must be sent as a proper RESP bulk-string array — there's no quoting
support in the inline grammar.

**Error reply formatting** (`write_resp_err`, `connection.rs:983`): a parse/execution `Err
(String)` is written verbatim with a `-` prefix if it already starts with one of **9** recognized
error-code words — `WRONGTYPE`, `CROSSSLOT`, `MOVED`, `ASK`, `NOSCRIPT`, `EXECABORT`,
`BUSYGROUP`, `NOGROUP`, `INVALIDOBJ`, `ERR` (`NOGROUP` and `INVALIDOBJ` are new additions to this
whitelist) — and otherwise gets `"ERR "` prepended.

---

## 7. Known Bugs, Edge Cases & Remaining Gaps

- **Fixed since the last pass**: `proto-max-bulk-len` is now a real, config-settable,
  runtime-enforced cap (§4.3 step 2, §4.4) — a client declaring a multi-gigabyte bulk string is
  now rejected at parse time instead of being buffered. Default is 512 MiB, matching real Redis.
- **Still open — no cap on `num_args` (the array *element count* itself).**
  `parse_decimal_bytes` guards against `usize` overflow when decoding `*N\r\n`, and each
  individual bulk string is now bounded by `proto-max-bulk-len`, but `N` itself (e.g. a client
  sending `*2000000000\r\n`) is never compared against a `proto-max-multibulk-len`-equivalent
  limit — real Redis caps this at 1,048,576 elements. In practice this is mostly
  self-limiting (each element needs at least 4 real bytes — `$0\r\n` — already buffered before
  the pass-1 loop advances past it, and an incomplete frame just returns `Ok(None)` to wait for
  more bytes rather than allocating anything proportional to `N` up front), but a hostile client
  that *does* stream that many tiny empty-bulk-string arguments would still make the server
  build a `Vec<Bytes>` of ~2 billion entries in the general (`num_args > 16`) path before
  `build_command` gets a chance to reject the command name — worth adding an explicit early cap.
- **`Command::Unlink` / `Command::Del` have no execution-time distinction** despite Redis
  documenting `UNLINK` as "like DEL but performs the actual memory reclamation in a background
  thread" — here both a client-sent `UNLINK` and `DEL` are normalized to the identical
  `Command::Del` at parse time (§3.4), so any behavioral difference would have to be implemented
  entirely inside the table/storage layer's handling of `Command::Del`, not gated on which verb
  the client actually sent.
- **`VADD`'s grammar-sniffing ambiguity** (§3.5): a legacy-grammar `VADD key reduce ...` (where
  `reduce` is meant as a literal element name, not the `REDUCE dim` option) would misparse as
  Vector Sets syntax and likely fail differently than intended. Same for element names `fp32`/
  `values`.
- **`build_command`'s single 10,719-line `match`** continues to grow linearly with every new
  command family (it roughly doubled in size since the previous review of this file, tracking
  the `Command` enum's own growth from 108 to 369 variants) — still compiles to an efficient
  jump table (Rust/LLVM handle large string matches via a length-then-bytes decision tree, not a
  linear scan, which is exactly why the byte-length-bucketed fast path in §4.3 uses the same
  technique by hand for the hottest dozen or so verbs), so this remains a maintainability
  concern, not a performance one. Splitting it into per-family functions dispatched from a
  smaller top-level match is a purely mechanical refactor that has not happened.
- **`FT.HYBRID`'s query-rewriting is string-concatenation-based** (§3.6): it does not validate
  that the constructed combined query is well-formed before handing it to
  `Command::FtSearch`/`src/search.rs` — a malformed `vector-query` argument (not starting with
  `*=>`, `=>`, or `[KNN`) is silently treated as free text and space-concatenated with the text
  query rather than rejected as a syntax error.

---

## 8. Testing

`resp.rs` carries its own `#[cfg(test)] mod tests` at the bottom of the file
(`src/resp.rs:13110`–`14357`, **15** `#[test]` functions — up from 13): `test_resp_get`,
`test_resp_set_and_put`, `test_inline_commands`, `test_resp_hash_commands`,
`test_resp_keyspace_and_transactions`, `test_resp_bitmaps_and_hll`, `test_resp_dump_and_restore`,
`test_resp_streams`, `test_valkey_extended_parsers`,
`test_parse_decimal_bytes_and_ascii_uppercase`, `test_smallvec_exists_and_sadd_parsing`,
`test_resp_lrange_zrange_fast_slice_parser`, `test_smallvec_del_and_hset_parsing`,
`test_unlink_readonly_wait_object_commands` (covers the §4.3 fast-path additions), and
`test_resp_semantic_cache_commands` (covers the new AI-native `SEMANTIC.*` parsing, §3.7). One
test (`src/resp.rs:14130`–`14135`) temporarily overrides `PROTO_MAX_BULK_LEN` via
`set_proto_max_bulk_len(5)` to exercise the new length-rejection path, then restores it.

Broader coverage of command *execution* (as opposed to parsing) lives in `connection.rs`'s own
much larger test module, which exercises `parse_command` end to end against real client-visible
behavior.

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Inline commands split on whitespace; commands with spaces inside arguments must
  be formatted as RESP bulk arrays (§6).
* **Gotcha 2**: RESP3 push frames use the `>` prefix and are built entirely in `src/pubsub.rs`
  (`build_pubsub_frame`/`build_pubsub_pframe`), never in `resp.rs`, which does not parse or emit
  any RESP3 framing at all (§3.1, §6).
* **Gotcha 3**: A ≤16-argument fast path in `parse_resp_array` short-circuits `build_command`
  entirely for `GET`, `SET` (plain form only), `DEL`, `INCR`, `HGET`, `HSET`, `SADD`, `LPOP`,
  `ZADD`, `PING`, `MGET`, `MSET`, `LPUSH`, `EXISTS`, `LRANGE`, `ZRANGE`, `UNLINK`, `READONLY`,
  `READWRITE`, and `SISMEMBER` (§4.3 — 20 commands, not 17). Any variant form of these (e.g.
  `SET key value EX 10`) or any command not on this list still goes through the general
  `build_command` dispatch.
* **Gotcha 4**: A malformed RESP frame (bad array/bulk-string length, missing `$`, missing
  trailing `\r\n`, or now also an over-`proto-max-bulk-len` length) is returned as `Err`
  *without* advancing the input buffer. Code that calls `parse_command` in a loop must itself
  decide how to make progress after an error (every caller in §5 either `buf.clear()`s or closes
  the connection) — `resp.rs` will keep re-reporting the same error on the same bytes otherwise.
* **Gotcha 5**: `Command::Unlink` is never produced by `build_command`/`parse_resp_array` — a
  client-sent `UNLINK` always becomes `Command::Del`. Only `src/table.rs`'s internal lazy-expiry
  path constructs `Command::Unlink`, purely to name it differently in AOF/replication output
  (§3.4).
* **Gotcha 6**: `VADD`'s argument grammar is selected by sniffing whether `args[2]` is
  `REDUCE`/`FP32`/`VALUES`, not by any explicit flag — see §3.5 for the resulting ambiguity.
* **Gotcha 7**: `FT.HYBRID` has no `Command` variant; it rewrites its two queries into one
  combined query string and constructs `Command::FtSearch` (§3.6).

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
