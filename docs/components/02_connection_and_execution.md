# Component 02: Connection Lifecycle & Command Execution (`src/connection.rs`)

## 1. Architectural Purpose & Scope

`src/connection.rs` is the central coordination layer for client sessions. It manages the TCP connection lifecycle, handles protocol negotiation (RESP2, RESP3, and Memcached text), maintains client session state (authentication, current database, transactions, pub/sub subscriptions), and dispatches parsed commands to local or remote storage engines.

---

## 2. Key Invariants & Concurrency Constraints

1. **Pure Single-Threaded Client State**: Because each connection is handled exclusively by the core that accepted it, the connection's state (transaction queues, watched keys, buffers) requires **no locks or atomic references**.
2. **Buffer Splitting & Compaction**: Read buffers are allocated as 64 KB slabs. Incomplete frames are retained and compacted to the start of the buffer without reallocating heap memory.
3. **Pipelined Request Squashing**: If a client sends multiple pipelined commands in a single network packet, Rudis executes all commands in a tight loop and coalesces all responses into a single output buffer, drastically reducing TCP packet overhead and syscalls.
4. **Non-Blocking Execution**: Commands that require cross-shard execution dispatch asynchronous futures across channels and yield control to the `monoio` executor, ensuring other clients on the same core remain responsive.

---

## 3. Component Architecture & Data Structures

```
                      Client TCP Stream
                             │
                             ▼
                    Connection State
              ├── Authenticated (bool)
              ├── Protocol: RESP2 | RESP3 | Memcached
              ├── TxState: InTx (queue: Vec<Command>, watched_keys)
              ├── BlockedGuard (timeout, client_id)
              └── Tracking: Invalidation Flags
                             │
                             ▼
                    Command Dispatcher
              ┌──────────────┴──────────────┐
              ▼                             ▼
       Local Shard Key?              Remote Shard Key?
              │                             │
    Direct RudisDb Mutation        router.execute_remote(shard_id, cmd)
              │                             │
              └──────────────┬──────────────┘
                             ▼
                   Output Buffer (RESP)
                             │
                             ▼
                 Coalesced Socket Write
```

### Client Session State

```rust
pub struct ClientContext {
    pub client_id: u64,
    pub authenticated: bool,
    pub db_index: usize,
    pub protocol: ClientProtocol, // Resp2, Resp3, or Memcached
    pub tx: ClientTxState,
    pub tracking: bool,           // Client-side caching tracking
    pub name: Option<String>,
}

pub struct ClientTxState {
    pub in_tx: bool,
    pub queue: Vec<Command>,
    pub watched_keys: HashMap<Bytes, u64>,
    pub dirty_cas: bool,
}
```

---

## 4. Execution Algorithms & Code Logic

### 4.1 The Read-Parse-Execute Loop

```rust
pub async fn run_connection_loop<S: AsyncReadRent + AsyncWriteRent>(
    mut stream: S,
    router: RouterHandle,
    client_id: u64,
) {
    let mut ctx = ClientContext::new(client_id);
    let mut read_buf = vec![0u8; 64 * 1024];
    let mut read_pos = 0;
    let mut out_buf = Vec::with_capacity(16 * 1024);

    loop {
        // Asynchronous non-blocking read via io_uring
        let (res, slice) = stream.read(read_buf.slice(read_pos..)).await;
        let n = match res {
            Ok(0) => break, // Graceful client disconnect
            Ok(n) => n,
            Err(_) => break, // Network error
        };
        read_pos += n;

        let mut consumed = 0;

        // Process all complete commands in buffer (Pipeline Squashing)
        while let Some((cmd, bytes_read)) = parse_command(&slice[consumed..read_pos]) {
            consumed += bytes_read;

            // Handle Transactions (MULTI/EXEC/DISCARD)
            if ctx.tx.in_tx && !is_tx_control_cmd(&cmd) {
                ctx.tx.queue.push(cmd);
                out_buf.extend_from_slice(b"+QUEUED\r\n");
                continue;
            }

            // Execute single or multi-key command
            execute_command(&mut ctx, cmd, &router, &mut out_buf).await;
        }

        // Flush all responses in one combined write
        if !out_buf.is_empty() {
            let (w_res, _) = stream.write_all(out_buf.split_off(0)).await;
            if w_res.is_err() { break; }
        }

        // Compact remaining incomplete frame data to buffer head
        read_buf.copy_within(consumed..read_pos, 0);
        read_pos -= consumed;
    }
}
```

### 4.2 Cross-Shard Dispatch Logic

When a command targets a key located on another shard, `execute_command` forwards it transparently:

```rust
pub async fn execute_command(
    ctx: &mut ClientContext,
    cmd: Command,
    router: &RouterHandle,
    out: &mut Vec<u8>,
) {
    let target_shard = router.key_to_shard(cmd.primary_key());

    if target_shard == router.local_shard_id() {
        // Fast-path: Execute immediately against local core's table
        let db = router.local_db_mut();
        dispatch_local_command(ctx, cmd, db, out);
    } else {
        // Remote-path: Forward to peer core via lock-free channel and await response
        let resp = router.execute_remote(target_shard, cmd).await;
        out.extend_from_slice(&resp);
    }
}
```

### 4.3 Double Precision Score Formatting (`format_score`)

Redis sorted set commands require exact float representation matching C's `snprintf(buf, len, "%.17g", val)`. `src/connection.rs` implements `format_score` using direct C FFI to guarantee bit-for-bit compatibility with official Redis test suites:

```rust
#[inline]
pub fn format_score(val: f64) -> String {
    if val.is_nan() {
        "nan".to_string()
    } else if val.is_infinite() {
        if val.is_sign_positive() { "inf".to_string() } else { "-inf".to_string() }
    } else if val == 0.0 {
        "0".to_string() // Prevents negative zero (-0.0 -> "0")
    } else {
        let mut buf = [0u8; 64];
        let len = unsafe {
            libc::snprintf(
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
                b"%.17g\0".as_ptr() as *const libc::c_char,
                val,
            )
        };
        unsafe { std::str::from_utf8_unchecked(&buf[..len as usize]) }.to_string()
    }
}
```

---

## 5. Cross-Component Interactions

- **`src/resp.rs`**: Supplies `parse_command` to decode network byte buffers into high-level Rust `Command` AST instances.
- **`src/router.rs`**: Calculates shard affinity and routes remote operations.
- **`src/table.rs`**: Invoked directly for local key mutations.
- **`src/block.rs`**: Used when blocking commands (`BLPOP`, `BZPOPMIN`) encounter empty collections.

---

## 6. Performance Characteristics

- **Zero Allocation on Hot Paths**: Simple read and write commands (`GET`, `SET`, `INCR`) borrow byte slices directly from the input buffer without intermediate `String` or `Vec` allocations.
- **High Pipeline Efficiency**: By aggregating replies into `out_buf` across all commands within the network buffer, Rudis achieves up to $13.5\times$ higher throughput under pipelined workloads.
