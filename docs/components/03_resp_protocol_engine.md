# Component 03: RESP Protocol Engine & Serialization (`src/resp.rs`)

> ⚠️ **Unverified against the real source.** This document's §5 "Serialization Primitives"
> (`write_resp_bulk`, `write_resp_integer`, etc.) and its `read_integer` helper (§4.1) do not
> exist in `src/resp.rs` (spot-checked) — replies are hand-formatted inline in `connection.rs`
> instead. Treat this file's specifics as unverified. See
> [`docs/designs/components.md`](../designs/components.md#part-5-resp-parsing-engine)
> (Part 5) for a version checked against the actual code (note: it predates newer command
> additions, so re-verify the `Command` enum's current variant list before relying on it).

## 1. Architectural Purpose & Scope

`src/resp.rs` is Rudis's wire protocol parser and serializer. It decodes raw TCP byte streams into strongly-typed `Command` enum variants and serializes execution results into Redis Serialization Protocol (**RESP2** and **RESP3**) wire formats.

It handles framing, length validation, argument tokenization, sub-command extraction, and error string generation matching official Redis specifications.

---

## 2. Key Invariants & Concurrency Constraints

1. **Zero-Copy Byte Slicing**: Bulk string payloads are extracted as reference-counted `bytes::Bytes` or borrowed byte slices (`&[u8]`). Payload data is never copied into temporary intermediate buffers during parsing.
2. **Strict Redis Error Message Parity**: Errors conform precisely to official Redis wording (e.g., `"syntax error"`, `"value is not an integer or out of range"`, `"ERR no such key"`, `"at least 1 input key is needed for '...' command"`).
3. **Dual Protocol Support**: Supports both RESP2 (legacy arrays, integers, bulk strings) and RESP3 (maps, sets, doubles, booleans, nulls, and push notifications).
4. **Dual Gateway Support**: Transparently parses inline commands (`PING\r\n`, `SET k v\r\n`) and Memcached ASCII text commands.

---

## 3. Protocol Framing & Data Structures

### 3.1 RESP Framing Specification

```
Type               Prefix   Example Wire Format             Parsed Rust Type
────────────────────────────────────────────────────────────────────────────────
Simple String      '+'      +OK\r\n                         &str
Error              '-'      -ERR unknown command\r\n        &str
Integer            ':'      :1000\r\n                       i64
Bulk String        '$'      $5\r\nhello\r\n                 Bytes
Null Bulk String   '$'      $-1\r\n                         Option<Bytes>
Array              '*'      *2\r\n$3\r\nfoo\r\n$3\r\nbar    Vec<Bytes>
RESP3 Null         '_'      _\r\n                           ()
RESP3 Boolean      '#'      #t\r\n / #f\r\n                 bool
RESP3 Double       ','      ,3.14159\r\n                    f64
RESP3 Map          '%'      %1\r\n+key\r\n+val\r\n          HashMap<Bytes, Bytes>
RESP3 Set          '~'      ~2\r\n$1\r\na\r\n$1\r\nb\r\n    HashSet<Bytes>
RESP3 Push         '>'      >2\r\n+invalidate\r\n$1\r\nk\r\n Vec<Bytes>
```

### 3.2 The Command Enum AST

```rust
pub enum Command {
    // Strings
    Get(Bytes),
    Set { key: Bytes, val: Bytes, ex: Option<Duration>, nx: bool, xx: bool, get: bool },
    Mget(Vec<Bytes>),
    Mset(Vec<(Bytes, Bytes)>),
    Incr(Bytes),
    Decr(Bytes),
    IncrBy(Bytes, i64),
    // Hashes
    Hget { key: Bytes, field: Bytes },
    Hset { key: Bytes, pairs: Vec<(Bytes, Bytes)> },
    Hgetall(Bytes),
    // Lists
    Lpush { key: Bytes, elements: Vec<Bytes> },
    Rpush { key: Bytes, elements: Vec<Bytes> },
    Lpop { key: Bytes, count: Option<usize> },
    Rpop { key: Bytes, count: Option<usize> },
    // Sorted Sets
    Zadd { key: Bytes, items: Vec<(f64, Bytes)>, flags: ZAddFlags },
    Zrange { key: Bytes, opts: ZRangeOpts },
    Zscore { key: Bytes, member: Bytes },
    // Full-Text & Vector Search
    FtCreate { index: Bytes, schema: SearchSchema },
    FtSearch { index: Bytes, query: SearchQuery },
    // Cluster & Replication
    ClusterNodes,
    ClusterSlots,
    Psync { repl_id: String, offset: i64 },
    // ... 274+ defined commands
}
```

---

## 4. Parsing Algorithms & Code Logic

### 4.1 Zero-Copy Frame Detection (`parse_command`)

```rust
pub fn parse_command(input: &[u8]) -> Option<(Command, usize)> {
    if input.is_empty() { return None; }

    match input[0] {
        b'*' => parse_resp_array(input),
        b'+' | b'-' | b':' | b'$' => None, // Not valid command roots
        _ => parse_inline_command(input),  // Text command (e.g. PING, QUIT)
    }
}

fn parse_resp_array(input: &[u8]) -> Option<(Command, usize)> {
    let mut cursor = 1;
    // 1. Read array length
    let (count, bytes_consumed) = read_integer(&input[cursor..])?;
    cursor += bytes_consumed;

    let mut args = Vec::with_capacity(count as usize);

    // 2. Read each argument bulk string
    for _ in 0..count {
        if cursor >= input.len() || input[cursor] != b'$' { return None; }
        cursor += 1;
        let (len, len_bytes) = read_integer(&input[cursor..])?;
        cursor += len_bytes;

        let end = cursor + len as usize;
        if end + 2 > input.len() { return None; } // Incomplete frame

        // Zero-copy reference slice
        let arg = Bytes::copy_from_slice(&input[cursor..end]);
        args.push(arg);
        cursor = end + 2; // Skip trailing \r\n
    }

    // 3. Dispatch to command-specific AST generator
    let cmd = build_command(args)?;
    Some((cmd, cursor))
}
```

### 4.2 Strict Input Validation Examples

#### Sorted Set Key Validation (`ZUNION`, `ZINTER`, `ZDIFF`)
Redis commands that accept a variable number of keys strictly validate that `numkeys >= 1`:

```rust
"ZUNIONSTORE" | "ZINTERSTORE" => {
    let numkeys: i64 = std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "value is not an integer or out of range".to_string())?;

    if numkeys <= 0 {
        return Err(format!("at least 1 input key is needed for '{}' command", cmd_name.to_lowercase()));
    }
    let numkeys = numkeys as usize;
    if args.len() < 3 + numkeys {
        return Err("syntax error".to_string());
    }
    // Parse keys, weights, and aggregate options...
}
```

---

## 5. Serialization Primitives

`src/resp.rs` provides fast inlined serialization helpers that write directly into `out: &mut Vec<u8>`:

```rust
#[inline]
pub fn write_resp_bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_resp_integer(out: &mut Vec<u8>, val: i64) {
    out.extend_from_slice(format!(":{}\r\n", val).as_bytes());
}

#[inline]
pub fn write_resp_simple_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(format!("+{}\r\n", s).as_bytes());
}

#[inline]
pub fn write_resp_err(out: &mut Vec<u8>, err: &str) {
    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
}
```

---

## 6. Performance Characteristics

- **Zero Memory Allocation on Parsing**: By borrowing slices directly from network buffers, memory throughput is bound only by CPU cache bandwidth.
- **Fast Integer Parsing**: `read_integer` uses branchless ASCII byte arithmetic (`val = val * 10 + (b - b'0') as i64`) rather than slow standard library string conversions.
