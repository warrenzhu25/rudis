# Component 03: RESP Protocol Engine & Command Parser (Design)

> **Source Files**: `src/resp.rs`


---

### 1. Architectural Purpose & Scope

`src/resp.rs` is Rudis's wire-format decoder. It turns raw bytes read off a TCP socket into
a single, strongly-typed `Command` enum value, one command at a time, and nothing else — it
does **not** serialize replies. Reply formatting (RESP2 bulk strings, integers, arrays, and
RESP3 maps/booleans where applicable) is hand-written directly into the output buffer in
`src/connection.rs`, not in this file. There is no `write_resp_*`/serialization module here.

The file is large (~7,500 lines) almost entirely because of the size of the `Command` enum
and its parser (`build_command`), which now covers well over 200 distinct top-level command
names spanning strings, hashes, lists, sets, sorted sets, streams, bitmaps, HyperLogLog,
pub/sub, transactions, cluster/gossip, ACL, scripting, vector search, geospatial, probabilistic
structures, RDB serialization, tiered-storage control commands, and a Memcached text-protocol
gateway — not because the core parsing algorithm itself grew complex. That algorithm (the
two-pass zero-copy RESP array parser) is unchanged from the original implementation.

---

### 2. Key Invariants & Concurrency Constraints

1. **Zero-Copy RESP Array Parsing**: Bulk string arguments inside a `*N\r\n...` frame are
   extracted via `BytesMut::split_to(len).freeze()` — a reference-count bump on the
   underlying buffer, never a byte-for-byte copy.
2. **All-or-Nothing Frame Consumption**: A partially buffered command (still waiting on more
   bytes from the socket) leaves the input buffer completely untouched (`Ok(None)`); a
   complete command is parsed and fully consumed in one call. There's no partial-consumption
   state to track between calls.
3. **Three Independent Input Grammars, One Entry Point**: `parse_command` recognizes RESP
   arrays (`*...`), plain space-separated inline text (`GET foo\r\n`), and Memcached's ASCII
   storage-command grammar (`set key flags exptime bytes\r\n<data>\r\n`) — dispatched purely
   by the first byte of the buffer, or by trial-parsing for the Memcached case (see §4.1).
4. **No RESP3 wire-format parsing in this file.** `HELLO` is recognized and parsed as a
   `Command` (so a client can request protocol v3), but nothing in `resp.rs` parses RESP3
   input types (maps `%`, sets `~`, doubles `,`, booleans `#`, nulls `_`, pushes `>`) — every
   incoming command is still a flat array of `$`-prefixed bulk strings. RESP3 is purely an
   *output*-side concern implemented in `connection.rs` (a per-client `is_resp3` flag and a
   thread-local `CURRENT_CLIENT_RESP3` cell gate which reply format gets written).

---

### 3. Performance Characteristics

- **Zero-copy on the hot (RESP array) path**: every bulk-string argument is a `Bytes` slice
  sharing the original read buffer's allocation, not a fresh heap copy.
- **The inline and Memcached paths copy**: both exist for compatibility/interactive use, not
  throughput, and neither is on the benchmarked pipeline path.
- **`build_command`'s dispatch is a single string match, not a lookup table**: with 200+ arms,
  this is a large `match` the compiler is left to optimize (typically into some mix of
  length-bucketed comparisons/jump tables); no bespoke perfect-hash or trie dispatch was
  built for it.

---
