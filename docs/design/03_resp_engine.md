# Component 03: RESP Protocol Engine & Command Parser (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/resp.rs`  
> **Implementation Reference**: [`docs/internal/03_resp_engine.md`](../internal/03_resp_engine.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
A RESP-compatible server sits between an untrusted, partially-buffered TCP byte stream and a strongly-typed command-execution layer. Two conflicting pressures apply at that boundary. First, throughput: on a thread-per-core, shared-nothing architecture (see [`01_reactor_runtime.md`](01_reactor_runtime.md)), the parser runs on the same core that also executes the command and serializes the reply, so any allocation or copy spent turning bytes into a command is pure overhead subtracted from that core's total command budget. Second, compatibility: real-world clients speak more than one dialect against the same TCP port — resp arrays for pipelined Redis clients, ad hoc inline text for humans typing into `nc` or `redis-cli`, and (for drop-in cache-replacement scenarios) the legacy Memcached ASCII protocol — and the server must recognize and route all of them without a separate listener or handshake per dialect.

### 1.2 The Rudis Solution
`src/resp.rs` (9,155 lines) is the single module responsible for turning a `bytes::BytesMut` receive buffer into a strongly-typed `Command` value. It owns exactly two responsibilities and no others:

1. **Frame parsing**: recognizing where one complete command ends and the next begins inside a byte stream that may contain zero, one, or many pipelined commands, or a partial trailing command still waiting on more socket data.
2. **Command construction**: converting a parsed argument list into one of the 108 variants of the `Command` enum, validating arity and option syntax as it goes.

Everything downstream of a successfully parsed `Command` — RESP2/RESP3 reply encoding, key routing across shards, actual command execution — is out of scope for this file and lives in `src/connection.rs`, `src/router.rs`/`src/shard.rs`, and the per-data-type modules (`src/table.rs`, `src/search.rs`, `src/vector.rs`, etc.). `resp.rs` contains no reply-serialization code whatsoever: there is no `write_resp_*` or `encode_*` function in this file. This strict separation keeps the parser reusable across every ingestion path that needs one — the primary client loop, the pub/sub loop, replication stream ingestion (both directions — a replica decoding its master's stream and a master decoding `REPLCONF ACK` from replicas), AOF replay, and the AF_XDP kernel-bypass fast path (`src/xdp.rs`, see [`10_kernel_bypass_xdp.md`](10_kernel_bypass_xdp.md)) all call the same `parse_command` entry point (§3).

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model
Zero-copy parsing over borrowed byte slices on the hot path, with a strongly-typed `Command`
enum as the sole hand-off contract to the execution layer. A `*N\r\n...` RESP array is walked
twice — once to prove the whole frame is present, once to slice it — and every bulk-string
argument becomes a `bytes::Bytes` view into the original receive buffer's allocation rather
than a freshly allocated `String` or `Vec<u8>`.

### 2.2 Design Rationale (The "Why")
Allocation and copying during command parsing are pure tax on a thread-per-core design: the
same core that parses a command also executes it and serializes the reply, so any cycles the
parser wastes are cycles the whole request pays for, with no other core able to help. Three
concrete choices follow from that constraint:

- **`Bytes` slices, not owned buffers, for arguments.** `BytesMut::split_to(len).freeze()`
  is a reference-count bump on the underlying heap allocation the socket read filled, not a
  byte-for-byte copy. A `Command::Set { key, value, .. }` therefore holds views into the same
  allocation the kernel wrote into, all the way through execution.
- **A dedicated fast dispatch path for small, common commands.** Beyond the generic zero-copy
  array parse, `parse_resp_array` recognizes RESP arrays of 16 or fewer elements, and for a
  fixed set of the highest-frequency commands (`GET`, `SET`, `DEL`, `INCR`, `HGET`, `HSET`,
  `SADD`, `LPOP`, `ZADD`, `PING`, `MGET`, `MSET`, `LPUSH`, `EXISTS`, `LRANGE`, `ZRANGE`,
  `SISMEMBER`) builds the matching `Command` variant directly from cached argument offsets,
  skipping the general-purpose `build_command` dispatch entirely for those calls (§4 of the
  internal doc).
- **Stack buffers instead of heap allocation for short, fixed-size decode work.** Command-name
  case-folding (`bytes_to_uppercase_ascii`) and RESP length-prefix decimal parsing
  (`parse_decimal_bytes`) both use fixed-size stack buffers / manual digit accumulation instead
  of going through `String::to_uppercase()` or UTF-8-validated `str::parse()`, avoiding a heap
  allocation on every single command dispatched, not just the fast-pathed ones.

The inline-text and Memcached parsers make the opposite trade-off deliberately: they copy
argument bytes with `Bytes::copy_from_slice`. Neither is on the pipelined, high-QPS path this
optimization targets — inline text serves interactive debugging clients (`redis-cli`, `nc`)
and the Memcached grammar serves compatibility with Memcached client libraries — so simplicity
was chosen over zero-copy for those two paths.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **Zero-Copy RESP Array Parsing**: Bulk string arguments inside a `*N\r\n...` frame are
   extracted via `BytesMut::split_to(len).freeze()` (or, on the small-array fast path, a single
   `split_to` over the whole frame followed by cheap `Bytes::slice` views into it) — a
   reference-count bump on the underlying buffer, never a byte-for-byte copy.
2. **All-or-Nothing Frame Consumption**: A partially buffered command (still waiting on more
   bytes from the socket) leaves the input buffer completely untouched (`Ok(None)`); a
   complete command is parsed and fully consumed in one call. There's no partial-consumption
   state to track between calls.
3. **Error Paths Do Not Consume Input**: When `parse_resp_array`'s validity scan rejects a
   frame (bad array length, missing `$` prefix, bad bulk-string length, missing trailing
   `\r\n`), it returns `Err` *before* advancing the buffer at all. `resp.rs` never guesses at
   resynchronization; it is each caller in `connection.rs` (or `aof.rs` for AOF replay) that
   decides what to do with a broken buffer — reply and keep waiting for more input, reply and
   close the connection, or discard the buffer outright. Different ingestion loops in
   `connection.rs` make different choices here (see [`02_connection_lifecycle.md`](02_connection_lifecycle.md)).
4. **Three Independent Input Grammars, One Entry Point**: `parse_command` recognizes RESP
   arrays (`*...`), plain space-separated inline text (`GET foo\r\n`), and Memcached's ASCII
   storage-command grammar (`set|add|replace key flags exptime bytes [noreply]\r\n<data>\r\n`)
   — dispatched purely by the first byte of the buffer, or by trial-parsing for the Memcached
   case (see §4.1 of the internal doc).
5. **No RESP3 wire-format parsing in this file.** `HELLO` is recognized and parsed as a
   `Command` (so a client can request protocol v3), but nothing in `resp.rs` parses RESP3
   input types (maps `%`, sets `~`, doubles `,`, booleans `#`, nulls `_`, pushes `>`) — every
   incoming command is still a flat array of `$`-prefixed bulk strings. RESP3 is purely an
   *output*-side concern implemented in `connection.rs` (a per-client `is_resp3` flag and a
   thread-local `CURRENT_CLIENT_RESP3` cell gate which reply format gets written).
6. **Untyped `String` errors, typed by convention at the reply boundary.** `parse_command` and
   every function it calls return `Result<_, String>` — a human-readable message, not a
   structured error code. Most messages (e.g. `"wrong number of arguments for 'get' command"`)
   carry no RESP error prefix; a minority that must preserve a specific Redis error class
   (`WRONGTYPE`, `CROSSSLOT`, `MOVED`, `ASK`, `NOSCRIPT`, `EXECABORT`, `BUSYGROUP`, `ERR`)
   spell that prefix out explicitly in the string itself. `connection.rs::write_resp_err`
   inspects the prefix and either passes the message through unchanged or prepends a default
   `ERR ` — so `resp.rs` never needs to model a Redis error-code enum, and adding a new
   distinguishable error class is a one-line string literal change, not a type change.

---

## 3. High-Level Architecture & Workflow Diagram

```
Raw Ingress Bytes: *3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nalice\r\n
                                      │
                                      ▼
                     parse_command(&mut BytesMut)
                                      │
                first byte == '*' ?───┴───▶ otherwise: try Memcached
                        │                    storage grammar, else
                        ▼                    fall through to inline
                 parse_resp_array                    text grammar
                        │
        ┌───────────────┴───────────────────┐
        ▼                                    ▼
  Pass 1: scan-only validity check    (frame incomplete → Ok(None),
  (proves whole frame is present      buffer left untouched; a
  without mutating the buffer)        malformed frame → Err, buffer
        │                             also left untouched — §2.3.3)
        ▼
  Pass 2: ≤16 args, hot command?
        │
   ┌────┴────┐
   ▼         ▼
 fast     general
 path     path
 (17      (build_command:
 hottest  283 top-level
 verbs,   match arms /
 no       ~295 command-
 build_   name strings,
 command  dispatched by
 call)    uppercased verb)
        │
        ▼
  Command::Set { key: Bytes("user"), value: Bytes("alice"), .. }
```

---

## 4. Performance Characteristics & Complexity

- **Zero-copy on the hot (RESP array) path**: every bulk-string argument is a `Bytes` slice
  sharing the original read buffer's allocation, not a fresh heap copy. For arrays of 16 or
  fewer elements the whole frame is split off the receive buffer once, and every argument is
  then a cheap `Bytes::slice` view into that single shared allocation — one refcount bump for
  the entire command rather than one per argument.
- **The inline and Memcached paths copy**: both exist for compatibility/interactive use, not
  throughput, and neither is on the pipelined hot path the zero-copy design targets.
- **`build_command`'s dispatch is a single large string match, not a lookup table**: the
  top-level `match cmd_name { .. }` in `build_command` has 283 arms covering roughly 295
  distinct command-name strings (some arms match multiple aliases, e.g. `"DEL" | "DELETE"`),
  spanning ~6,470 lines. The compiler is left to optimize this on its own (typically into some
  mix of length-bucketed comparisons and jump tables); no bespoke perfect-hash or trie dispatch
  was built for it. The 17-command fast path in `parse_resp_array` exists precisely to give the
  small set of highest-frequency commands a shorter path than walking into that match at all.
- **All parsing is single-pass over small headers, never over payload data.** `find_crlf` /
  `find_crlf_at` do a plain `.windows(2)` scan, but they are only ever used to locate the end of
  short protocol headers (array-length and bulk-string-length lines) — never to scan bulk
  string payload bytes — so there is no SIMD opportunity being left on the table here (contrast
  with the storage engine's 16-byte SIMD control-byte matching described in
  [`05_storage_engine.md`](05_storage_engine.md)).
- No throughput or latency benchmark numbers are published in this document; any such figures
  would need to be measured against the current build rather than asserted here.

---

## 5. Known Gaps & Future Work

The following are **not** current behavior; they are gaps identified against the file as it
exists today and are tracked here so contributors don't assume they're already handled:

- **No maximum bulk-string/array length is enforced.** `parse_resp_array` trusts the declared
  `arg_len`/`num_args` off the wire (bounded only by `usize` overflow checks in
  `parse_decimal_bytes`), so a client claiming a multi-gigabyte bulk string causes the server
  to attempt to buffer that much data before any rejection occurs. A `proto-max-bulk-len`-style
  cap is not implemented.
- **RESP3 input parsing does not exist.** Every command a client sends is still parsed as a
  flat array of `$`-prefixed bulk strings, regardless of the negotiated protocol version. This
  is sufficient because no current Redis command requires a client to send a RESP3-typed
  argument (map, set, double, boolean, or null) — but it means "RESP3 support" in Rudis is
  strictly output-side today.

## 6. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/03_resp_engine.md`**](../internal/03_resp_engine.md): Low-level implementation and code reference.
* **Source Files**: `src/resp.rs`
