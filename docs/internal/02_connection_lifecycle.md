# Component 02: Connection Lifecycle & Command Execution (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/connection.rs` (20,971 lines — roughly doubled since the previous revision of this document, which was written against a 14,542-line version of the file)
> **High-Level Design Spec**: [`docs/design/02_connection_lifecycle.md`](../design/02_connection_lifecycle.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

This document was rewritten from a systematic, line-by-line audit of the current `src/connection.rs` (every `struct`/`enum`/`impl`/`fn` was enumerated first via `rg`, then every region was read in full). Every claim below was verified against the code as it exists today; nothing is carried forward from memory of a previous revision without being re-checked. Where behavior has changed since the last revision, that is called out explicitly.

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/connection.rs` | Per-connection accept-to-close lifecycle, protocol detection, transactions, pipeline squashing, blocking-command integration, pub/sub and replication mode switches, TLS loop, Memcached command handlers | `handle_connection`, `handle_tls_connection`, `execute_tx_step`, `execute_command`, `execute_local_command`, `execute_commands_squashed`, `ClientInfo`, `ConnScratch` |
| `src/mailbox.rs` | Cross-shard response mailboxes (`BatchResponder`), single-key fast-path descriptors, SPSC ring buffers | `BatchResponder`, `FastGetDescriptor`, `FastSetDescriptor`, `SpscQueue` |
| `src/shard.rs` | Reply payload representation, per-shard-worker message envelope, and (in `server.rs`) the remote-side handler for that envelope | `CompactResp`, `ShardMessage`, `ShardDb` |
| `src/router.rs` | Local-vs-remote routing, cross-shard `MGET`/`MSET` scatter-gather, VLL transaction locks, single-key remote GET/SET fast paths | `Router`, `MgetInFlight`/`MsetInFlight`, `ScatterMgetDescriptor`/`ScatterMsetDescriptor` |
| `src/server.rs` | Accept loop, per-connection task spawn with panic isolation (`catch_unwind_async`), and the **remote-shard-side** handler for `ShardMessage::Batch` (the receiving half of the mechanism described in §6) | `run_shard_worker`, the `ShardMessage::Batch` match arm |

All line numbers below were confirmed against the current `src/connection.rs`/`src/mailbox.rs`/`src/server.rs` at the time of writing (2026-09-30) and may drift by a handful of lines as the file evolves; function names are stable references.

---

## 2. Component Architecture & Data Structures

```
                 Client TCP Stream
                        │
                        ▼
      handle_connection()/handle_tls_connection() loop
                        │
        ┌───────────────┼──────────────┬────────────────┬───────────────────┐
        ▼               ▼               ▼                ▼                   ▼
  SUBSCRIBE/       PSYNC/SYNC       DFLY FLOW        MULTI/WATCH/        Blocking cmd
  PSUBSCRIBE/      seen?            seen?             EXEC active?       (BLPOP/…)?
  SSUBSCRIBE?      → run_master_    → run_shard_       → execute_tx_step  → flush out_buf
  → run_pubsub_loop  replica_stream   replication_flow    per command       first, then
    (mode switch,     (mode switch,    (mode switch,        (§4.2)          execute
    never returns)     never returns)   never returns)         │            sequentially
        │                  └──────────────┴─────────────────┴─────────────────┘
        │                                          ▼
        │                            1 command? execute_command()
        │                            N commands? execute_commands_squashed()
        │                                          │
        │                              ┌───────────┴────────────┐
        │                              ▼                        ▼
        │                       Local shard key           Remote shard key
        │                   execute_local_command()   ShardMessage::Batch over an
        │                     direct on ShardDb        Arc<BatchResponder>, remote
        │                                              shard writes replies directly
        │                                              into the caller's response
        │                                              slice via a raw pointer (§6)
        │                                          │
        │                                          ▼
        │                            out_buf (RESP2/RESP3/Memcached text)
        │                                          │
        │                                          ▼
        │                       one coalesced buffer flushed via non-blocking
        │                       libc::send, falling back to io_uring write_all
        ▼
  dedicated writer task (bounded flume(4096) queue,
  independent output-buffer-limit accounting)
```

Four execution strategies are chosen per read, in priority order (`handle_connection`, line 1535 onward, decision logic at lines 1834–1983): **(a)** a transaction is already open, or this batch contains `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` → every command in the batch is run through `execute_tx_step` (§4.2); **(b)** the batch contains a blocking command → sequential execution with a pre-block flush (§4.3); **(c)** exactly one command → `execute_command` directly; **(d)** more than one command, none of the above → `execute_commands_squashed` (§4.4/§6). A connection can also permanently switch out of this loop into one of three non-returning modes: pub/sub (`run_pubsub_loop`, line 2178), master→replica streaming (`run_master_replica_stream`, line 2597), or Dragonfly-protocol shard-to-shard replication flow (`run_shard_replication_flow`, line 2697).

### 2.1 `ClientInfo`: per-connection state (line 27)

```rust
#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: Option<String>,
    pub lib_name: Option<String>,            // CLIENT SETINFO lib-name
    pub lib_ver: Option<String>,              // CLIENT SETINFO lib-ver
    pub connected_at: Instant,
    pub last_active: Instant,
    pub last_cmd: &'static str,               // interned command name, no per-command allocation
    pub is_resp3: bool,
    pub track_tx: Option<flume::Sender<Vec<u8>>>,   // RESP3 tracking invalidation inbox
    pub raw_fd: std::os::unix::io::RawFd,
    pub omem: usize,                          // bytes currently queued in this client's output buffer
    pub reply_mode: crate::resp::ClientReplyMode,   // CLIENT REPLY ON/OFF/SKIP — see §7 gap
}
```

`lib_name`/`lib_ver` are new fields (populated by `CLIENT SETINFO`, reported back in `CLIENT INFO`'s `lib-name=`/`lib-ver=` fields and in `CLIENT LIST`). `reply_mode` is also new, but as documented in §7, setting it via `CLIENT REPLY OFF`/`SKIP` is tracked and never consulted anywhere else in the file — command replies are written to `out_buf` unconditionally regardless of its value.

### 2.2 `ClientTracker` (RESP3 client-side caching) and thread-locals

```rust
#[derive(Clone, Debug)]
pub struct ClientTracker {
    pub port: u16,
    pub client_id: u64,
    pub bcast: bool,
    pub prefixes: Vec<Bytes>,
    pub tracked_keys: hashbrown::HashSet<Vec<u8>>,
    pub sender: flume::Sender<Vec<u8>>,
    pub is_resp3: bool,
}

thread_local! {
    pub static CURRENT_CLIENT_RESP3: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub static IN_TX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub static CURRENT_ROUTER: std::cell::RefCell<Option<std::rc::Rc<Router>>> = const { std::cell::RefCell::new(None) };
}
```

`CURRENT_ROUTER` is new versus the prior revision of this document: `set_current_router` (line 196) stashes the reactor thread's `Rc<Router>` once at startup so that deeply-nested helpers (`notify_keyspace_event`, line 202) can fire keyspace-notification pub/sub messages without threading a `&Router` argument through every call site.

There is still no `ClientContext`/`ClientTxState`/`ClientProtocol` struct. Per-connection transaction state (`in_multi: bool`, `tx_queue: Vec<Command>`, `tx_has_error: bool`) lives as plain stack locals in `handle_connection`/`handle_tls_connection`, passed by `&mut` into the shared `execute_tx_step` helper (§4.2). RESP3-vs-RESP2 mode is tracked via the `CURRENT_CLIENT_RESP3` thread-local (set at the top of `execute_command`/`execute_commands_squashed` from `ClientInfo.is_resp3`).

### 2.3 Process-wide static state (WATCH, tracking, stats, buffer limits)

```rust
// line 495-501
static WATCHED_KEYS: LazyLock<RwLock<hashbrown::HashMap<u16, hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>>>> = ...;
static CLIENT_WATCH_TAINTED: LazyLock<RwLock<hashbrown::HashMap<(u16, u64), bool>>> = ...;
pub static HAS_WATCHED_KEYS: AtomicBool = AtomicBool::new(false);       // line 576

// line 659-664
static TRACKING_CLIENTS: LazyLock<RwLock<hashbrown::HashMap<(u16, u64), ClientTracker>>> = ...;
pub static HAS_TRACKING_CLIENTS: AtomicBool = AtomicBool::new(false);
```

Both maps are keyed by `(port, client_id)` (or `port` alone for `WATCHED_KEYS`'s outer map) rather than by shard index. In this codebase each shard's reactor thread binds its **own** listening port (`router.port`, used throughout for `-MOVED 127.0.0.1:<port>` redirection — see §8), so a client's `WATCH`/tracking registrations are naturally scoped to the shard it is physically connected to, even though the backing maps are process-wide statics shared by every reactor thread. `touch_watched_key`, `record_client_read`, and `notify_key_invalidation` all check the paired `AtomicBool` (`Ordering::Relaxed`) before touching the `RwLock`, so a deployment that never issues `WATCH`/`CLIENT TRACKING` pays only an atomic load on the hot path.

`CMD_STATS` (`RwLock<HashMap<String, u64>>`, line 503) backs `INFO commandstats`. Per-command increments go through `record_cmd_stat` (line 512), which increments a thread-local `LOCAL_CMD_STATS: HashMap<&'static str, u64>` and merges into the global map only every 1024 calls (`flush_local_cmd_stats`, line 528 — also called from `ClientCleanup::drop` on disconnect so short-lived connections aren't lost). `ERROR_STATS`/`FAILED_CMD_STATS`/`TOTAL_ERROR_REPLIES` (lines 548-552) back `INFO errorstats`; they are populated by `record_error_stat` (line 554), which is invoked by a new `ErrorStatTracker` RAII guard inside `execute_command` — see §7.

### 2.4 Client Output Buffer Limits (`ClientClass` / `BufferLimit`)

```rust
// line 43-65
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferLimit { pub hard_limit: u64, pub soft_limit: u64, pub soft_seconds: u64 }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientClass { Normal, Replica, Pubsub }

pub static HAS_CUSTOM_BUFFER_LIMIT: AtomicBool = AtomicBool::new(false);
pub static NORMAL_BUFFER_LIMIT: RwLock<BufferLimit> = RwLock::new(BufferLimit::new(0, 0, 0));
pub static SLAVE_BUFFER_LIMIT:  RwLock<BufferLimit> = RwLock::new(BufferLimit::new(268_435_456, 67_108_864, 60));
pub static PUBSUB_BUFFER_LIMIT: RwLock<BufferLimit> = RwLock::new(BufferLimit::new(33_554_432, 8_388_608, 60));
```

Defaults mirror Redis (`normal`: unlimited; `replica`: 256MiB hard / 64MiB sustained-60s soft; `pubsub`: 32MiB hard / 8MiB sustained-60s soft), configurable at runtime via `set_client_output_buffer_limit_str` (line 141), which also flips `HAS_CUSTOM_BUFFER_LIMIT` so `get_client_output_buffer_limit(ClientClass::Normal)` (line 77) can short-circuit to "no limit" without a lock acquisition in the common case. `handle_connection`'s main loop (§3) gates *all* of its output-buffer-limit bookkeeping (including `ClientInfo.omem` updates) behind this single atomic, not just the limit comparison.

### 2.5 `ConnScratch`: pooled, cross-connection reusable per-connection buffers (line 2094)

```rust
struct ConnScratch {
    buf: BytesMut,                                         // inbound read buffer
    out_buf: Vec<u8>,                                       // outbound response buffer
    responders: Vec<Arc<crate::mailbox::BatchResponder>>,   // one mailbox per shard, index = shard id
    remote_batches: Vec<Vec<(usize, u64, Command)>>,        // per-shard outgoing (pipeline_idx, key_hash, cmd) buckets
    items_pool: Vec<Vec<(usize, u64, Command)>>,            // recycled Vecs for the above
    results_pool: Vec<Vec<(usize, CompactResp)>>,           // legacy recycled Vec pool — see note below
    squashed_responses: Vec<CompactResp>,
    commands: Vec<Command>,
}

thread_local! {
    static CONN_SCRATCH_POOL: RefCell<Vec<ConnScratch>> = const { RefCell::new(Vec::new()) };
}
```

`take_conn_scratch`/`recycle_conn_scratch` (lines 2109/2140) pool up to 32 `ConnScratch` instances **per reactor thread, shared across successive connections on that core** — not merely reused within one connection's lifetime. `results_pool` still exists as a field and is still threaded through `execute_commands_squashed`'s signature, but that function only ever does `let _ = results_pool;` (line 19813) — with the `BatchResponder` redesign (§6), remote shards now write replies directly into `responses`/`squashed_responses` via a raw pointer instead of handing back a `Vec<(usize, CompactResp)>`, so `results_pool` is effectively unused dead weight carried for pool-shape compatibility.

A scratch instance is only returned to the pool if every one of its `BatchResponder`s is provably idle and unreferenced elsewhere:

```rust
let reusable = s.responders.iter().all(|r| {
    Arc::strong_count(r) == 1 && r.is_idle()
});
if !reusable { return; }  // drop it; a straggling remote shard still holds an Arc clone
```

This guards against the same cross-connection hazard as before: a remote shard holds an `Arc<BatchResponder>` clone for as long as its batch is in flight and writes through it via raw pointer/atomic state. Recycling a scratch whose responder is still referenced by a slow remote shard would let that late write land in a different client's connection state (or clobber a `responses` `Vec` that has since been deallocated/reused). `BatchResponder::is_idle()` (`src/mailbox.rs` line 358) checks `state == BATCH_IDLE`, replacing the old `ready.load(Acquire)` check.

### 2.6 `CompactResp`: small-reply inline storage (`src/shard.rs`, line 9)

```rust
pub enum CompactResp {
    Small { len: u8, data: [u8; 30] },   // inline, no heap allocation
    Big(Vec<u8>),
    Bulk(Bytes),
    Array1Bulk(Bytes),
    RawBytes(Bytes),                     // new variant vs. the prior revision of this document
}
```

Common replies are pre-built as `const` zero-allocation values: `CompactResp::OK`, `INT_0`, `INT_1`, `NULL`, `NULL_RESP3`, `EMPTY_ARRAY` (lines 38-93), selected with `CompactResp::null(is_resp3)`. `from_bulk`/`from_owned_bulk` inline bulk strings up to 20 bytes of payload (fits the `Small` variant's 30-byte buffer alongside the `$<len>\r\n...\r\n` framing) and fall back to `Bulk(Bytes)` above that. This is the reply payload type carried through `ShardMessage::Batch` and written into `responses`/`squashed_responses` by both the local fast paths and the remote shard's `BatchResponder::write_slot`.

---

## 3. Connection Lifecycle End-to-End

### 3.1 Accept → protocol detection

Accept and TLS-vs-plaintext branching happen in `src/server.rs`, not `connection.rs`; each accepted connection is spawned as a `monoio::spawn`ed task wrapped in `catch_unwind_async` (e.g. `src/server.rs` line 355) so a panic inside `handle_connection` is caught at the task boundary, logged, and counted via `crate::connection::inc_isolated_panics()` (line 1153) rather than taking down the reactor thread. `connection.rs` itself contains **no `catch_unwind`** — `ISOLATED_PANICS: AtomicU64` (line 1150) and its `inc`/`get` accessors are the only panic-isolation code here; the actual `catch_unwind_async` wrapping lives in `server.rs` at three call sites (normal accept, `TIER`-adopted-connection handoff, and one more — all three call `inc_isolated_panics()` on `Err`).

Protocol detection (RESP vs. Memcached text vs. plain inline) happens in `parse_command` (`src/resp.rs`, not this file) and is dispatched purely on the first byte: `*` → RESP multi-bulk array; anything else → `parse_memcached_storage_command` is tried first (recognizes only `set`/`add`/`replace`, since those carry a following raw data block whose length must be parsed before the frame is complete); if that returns "not a storage command," `parse_inline_command` handles the remaining Memcached verbs (`get`/`gets`/`delete`/`incr`/`decr`/`stats`/`version`/`quit`) as well as plain inline RESP commands (e.g. a bare `PING`). There is no fixed first-byte lookup table — the only hard branch is presence/absence of a leading `*`.

### 3.2 `handle_connection` (line 1535): the read/parse/dispatch/write loop

Setup: increments `ACTIVE_CLIENTS`, rejects with `-ERR max number of clients reached` if over `MAX_CLIENTS`; creates an unbounded `flume` channel (`track_tx`/`track_rx`) for this client's RESP3 invalidation inbox; inserts a `ClientInfo` into `client_registry`; installs a `ClientCleanup` RAII guard (decrements `ACTIVE_CLIENTS`, flushes cmd stats, removes pub/sub subscriptions — fanning a `ShardMessage::RemoveClientPubSub` to every *other* shard's sender — unregisters tracking, and unregisters any `BlockHub` registration); and pulls a `ConnScratch` from the pool via `take_conn_scratch(router.num_shards)`.

Each loop iteration:

1. **Zero-copy read.** If spare capacity in `buf` is below `MIN_READ_SPARE` (16KiB), `buf.reserve(READ_BUFFER_SIZE)` (`READ_BUFFER_SIZE = 65536`, line 19). The pooled `BytesMut` is then rented directly to monoio's io_uring driver via a custom `RecvBytesMut` wrapper (`stream.read(RecvBytesMut(buf)).await`, struct defined at line 19910, implementing `monoio::buf::IoBufMut` to write directly into `buf`'s spare capacity) — no intermediate `read_buf`/memcpy.
2. **Kernel-buffer drain.** If the io_uring read filled the *entire* spare capacity it was given (`n == avail_before`), the loop assumes more bytes may already be queued in the kernel socket buffer and opportunistically issues a tight loop of non-blocking `libc::recv(raw_fd, ..., MSG_DONTWAIT)` calls directly on the raw fd, growing `buf` each time, until a `recv` returns less than the spare capacity offered or nothing at all — absorbing a large inbound burst without extra reactor round-trips.
3. **Parse every complete frame** in `buf` via `parse_command`, collecting into `commands: Vec<Command>` and tracking `has_special` — a boolean, seeded from `in_multi`, set true the first time `is_special_pipeline_cmd` (line 19046) returns true for any command in the batch. If parsing hits `Ok(None)` (incomplete frame) mid-batch, the same `libc::recv(MSG_DONTWAIT)` drain trick is retried once before giving up and waiting for the next io_uring read. A `QUIT` command truncates the remaining buffer and stops parsing further commands in this read.
4. **Mode-switch checks** (only performed if `has_special`, each a linear scan of `commands` via `.position(...)`): a `Subscribe`/`Psubscribe`/`Ssubscribe` anywhere in the batch → execute everything before it via `execute_command`, flush, then hand the socket to `run_pubsub_loop` and `return` (never comes back to this loop); a `Psync`/`Sync` → same pattern into `run_master_replica_stream`; a `DflyFlow { .. }` → same pattern into `run_shard_replication_flow`. Each of these three checks re-scans `commands` independently (three separate `.position()` calls), so a pathological single pipeline containing e.g. both a write and a `PSYNC` pays up to three linear scans — bounded by `has_special` gating all three behind one flag check in the overwhelmingly common case where none apply.
5. **Execution strategy selection.** `has_tx` is computed as `has_special && (in_multi || commands.iter().any(is a tx-control command))`; if true, every command in the batch is drained through `execute_tx_step` one at a time (§4.2). Otherwise, if `has_special` and the batch contains a blocking command, each command is executed sequentially through `execute_command`, with `out_buf` flushed via a real `stream.write_all` *before* any blocking command specifically (§4.3). Otherwise, exactly one command → `execute_command` directly; more than one → `execute_commands_squashed` (§6).
6. **RESP3 tracking invalidation drain.** If `HAS_TRACKING_CLIENTS` is set, every message already queued on this client's `track_rx` (pushed by *other* connections' writes) is appended to `out_buf`.
7. **Output buffer limit enforcement** (§5) — gated entirely behind `HAS_CUSTOM_BUFFER_LIMIT`.
8. **Flush.** A direct non-blocking `libc::send(raw_fd, ..., MSG_DONTWAIT | MSG_NOSIGNAL)` is attempted first; a full send clears `out_buf` with zero io_uring round-trips. A partial send copies the unsent remainder into a fresh `Vec` and completes it with `stream.write_all` (io_uring). A fully-blocked `send` (`send_ret <= 0`, e.g. `EAGAIN`) falls back to a full `stream.write_all` on the whole buffer. This is one coalesced buffer per read-loop iteration — not scatter-gather/vectored I/O.
9. If `buf.is_empty()`, `buf.try_reclaim(READ_BUFFER_SIZE)` shrinks the allocation back down between bursts.

On loop exit (client disconnect, read error, hard output-buffer-limit breach, or a command handler returning `should_quit = true`), the `ConnScratch` is returned to the pool via `recycle_conn_scratch` and `unwatch_keys(router.port, client_id)` clears this client's `WATCH` state. `ClientCleanup::drop` runs automatically via RAII as the function returns.

### 3.3 `handle_tls_connection` (line 1162): shares transaction logic, skips squashing

Structurally simpler than `handle_connection`: no `ConnScratch` pooling, no pipeline squashing, no output-buffer-limit enforcement, no kernel-drain trick. It decrypts into a plaintext buffer via `crate::tls::TlsSession::read_plaintext`, parses one command at a time, and — **this is new versus the prior revision of this document** — routes every parsed command through the same `execute_tx_step` helper that `handle_connection` uses (§4.2), so TLS connections now get full `MULTI`/`EXEC`/`WATCH` transaction support, not just bare `execute_command` dispatch. What TLS connections still never get is pipeline squashing: every command, transactional or not, pays one `execute_tx_step`→`execute_command` call (and, for remote keys, one full `.await` round-trip) — there is no batch-fan-out fast path over TLS. Flush is a single `session.write_plaintext` per read, with no non-blocking `libc::send` fast path.

### 3.4 Shutdown / close

Three paths converge on cleanup: (1) normal EOF/read-error in the main loop, handled by the `ConnScratch` recycle + `ClientCleanup` drop described above; (2) `Command::Quit`, which is special-cased in `execute_tx_step`/`execute_command` to write `+OK\r\n` and return `true` (interpreted as `should_quit`); (3) a hard client-output-buffer-limit breach (§5), which simply `break`s the loop without writing an error — the client just finds its socket closed, matching Redis's behavior for `client-output-buffer-limit` disconnects.

---

## 4. Command Execution Paths

### 4.1 `execute_command` (line 4906, ~7,432 lines): the gate sequence, then dispatch

Every single-command execution (the sequential path, the blocking-command path, the squashed path's ineligible-batch fallback, and the squashed path's per-command overflow for anything not covered by an inline fast path) funnels through `execute_command`, which runs a fixed gate sequence before reaching its `match cmd { ... }` (164 top-level arms):

0. **Bookkeeping RAII guards**, installed before any gate: a `SlowlogTracker` (captures a cloned `Command` + start `Instant` when `SLOWLOG_LOG_SLOWER_THAN >= 0`, logs elapsed µs via `crate::slowlog::log_command_if_slow` on `Drop`) and a new **`ErrorStatTracker`** (captures `out.len()` before dispatch; on `Drop`, if the reply written during this call starts with `-`, parses the error-code prefix up to the first space/CRLF and calls `record_error_stat(prefix, Some(cmd_name))` — this is how `INFO errorstats`/`commandstats`-failure counters are populated, and it is new versus the prior revision of this document, which did not mention it).
1. **`NOAUTH`**: rejected unless `authenticated` or the command is `AUTH`/`HELLO`/`QUIT`.
2. **ACL** (`-NOPERM`): checked only when `HAS_CUSTOM_ACL` is set or the session's user isn't `"default"` — both `user.can_execute_command(cmd_name)` and, over **every** key from `cmd_keys(&cmd)` (not just the primary key), `user.can_access_key(key)`.
3. **`ASKING`**: sets a one-shot `asking` flag, replies `+OK`, returns immediately. The flag is captured (`is_asking`) and reset (`*asking = false`) at the top of the *next* command's gate sequence, before the slot-state check below.
4. **`CROSSSLOT`**: when `router.cluster_enabled` and `cmd_keys(&cmd)` yields more than one key, every key must hash to the same slot (`key_slot`) or the command is rejected with `-CROSSSLOT`.
5. **Cluster slot-state redirection**, keyed on `cmd_primary_key(&cmd)`'s single representative key and `router.get_slot_state(slot)`: `Moved(target)` → always `-MOVED`; `Importing(source)` → `-MOVED` unless `is_asking`; `Migrating(target)` → `-ASK` unless the key already exists locally (checked via `router.exists(key).await`); `Stable` → if `router.cluster_enabled` and `router.target_shard_for_slot(slot)` names a different local shard, `-MOVED 127.0.0.1:<base_port + target_shard>`; otherwise, in the single-node-router path, if the gossiped `ClusterHub` (`crate::cluster::get_cluster_hub`) says a different, live, non-failed peer node owns the slot, `-MOVED <peer ip>:<peer port>`.
6. **`READONLY`**: rejected (gated on `HAS_SLAVE_INSTANCE`) if this node `is_slave()` and `crate::aof::command_to_resp(&cmd).is_some()` (i.e. the command has a write form).
7. **`OOM`**: for any command where `command_to_resp(&cmd).is_some()`, if `maxmemory` (`crate::tiering::get_max_memory`) is set, the policy is `noeviction`, and this shard has no active `tier_manager` (NVMe tiering off), rejected with `-OOM` when `used_memory > maxmemory / num_shards`.

Only after all seven gates pass does control reach the big `match cmd { ... }`. Two dispatch shapes coexist inside it:

- **Command-specific arms** with inline logic — e.g. `Get` (§4.1.1), `Set` (§4.1.2), `Hello` (negotiates RESP2/RESP3, reports `server: valkey` / `version: 7.2.0` for client-library compatibility, authenticates via `HELLO ... AUTH user pass`), `Client(sub)` (`CLIENT LIST`/`INFO`/`SETNAME`/`SETINFO`/`GETNAME`/`ID`/`TRACKING`/`UNBLOCK`/`REPLY`; `CLIENT KILL`, `CLIENT PAUSE`/`UNPAUSE`, `CLIENT NO-TOUCH`, and `CLIENT CACHING` are all accepted-but-inert `+OK` stubs — see §7), `Eval`/`Evalsha`/`Fcall` (if the script's first key lives on a remote shard, the whole `Eval`/`Evalsha` command is forwarded via `router.execute_remote` and re-executed there — scripting is **local-shard-only**, never distributed across shards mid-script), the eight `MemcachedSet`/`Add`/`Replace`/`Get`/`Delete`/`Incr`/`Decr`/`Stats` arms (§9), `DflyCluster`/`DflyMigrate` (Dragonfly-wire-protocol cluster/migration subcommands), and `Tier(sub)` (`TIER SPILL`/`COOL`/`DECOMMIT`/`LOAD`/`SPILLALL`, the last of which fans out `ShardMessage::TierSpillAll` to every other shard and sums the results).
- **One large merged arm** (lines 7206–7396) combining ~130 simple key-addressed commands (`HSET`/`HGET`/list ops/set ops/zset ops/bitops/streams/JSON/geo/bloom/cuckoo/count-min-sketch/top-k/CRDT/semantic-cache/agent-memory/agent-checkpoint/agent-tool/vector-search/`OBJECT`/`XINFO` subcommands) that all reduce to the same two-line dispatch: `target_shard_of_cmd(&cmd, router.num_shards)` picks a shard; if it's local, `execute_local_command` runs synchronously against `router.local_db`; otherwise `router.execute_remote(target, cmd).await` sends it and awaits the raw RESP bytes back. This merged arm is the generic per-command remote/local dispatch baseline that every specialized fast path elsewhere in the file (§4.3, §6) exists to bypass for hot commands.

#### 4.1.1 `Get` fast path (inline in `execute_command`, not a separate function)

```rust
Command::Get(key) => {
    record_client_read(router.port, client_id, key.as_ref());
    let target = target_shard(&key, router.num_shards);
    let val = if target == router.shard_id {
        match router.local_db.borrow_mut().get(&key) {
            Some(v) => { tier_stats.ram_hits += 1; Some(v) }
            None if router.local_db.borrow_mut().table.is_tiered(&key).is_none() => None,
            None => router.get(key).await,   // NVMe cold-tier fallback
        }
    } else {
        router.get(key).await
    };
    ...
}
```

A local RAM hit never leaves the synchronous path. A local miss that the table also reports as *not tiered* is a definitive miss (`None`, no `.await`). Only a local miss where the key **is** tiered falls through to `router.get(key).await`, which resolves via `crate::tiering` (NVMe read-back). A remote-shard key always goes through `router.get(key).await`, which internally uses `mailbox::FastGetDescriptor` (`src/mailbox.rs` line 260) — a dedicated single-key shared-memory slot (`val: UnsafeCell<Option<Bytes>>`, `done: AtomicBool`, a `flume::bounded(1)` wake channel) distinct from the batch `BatchResponder` machinery in §6, used specifically so a single remote `GET` doesn't pay a `Vec` allocation for a one-element batch.

#### 4.1.2 `Set` fast path (inline in `execute_command`)

For a **local** key, an unconditional plain `SET` (no `NX`/`XX`/`IFEQ`/etc., no `GET`, no `KEEPTTL`, no past-expired backdate) with no AOF writer and no connected replicas takes a direct path: `notify_key_invalidation(router.port, key, client_id)` → `db.set_extended(...)` → `notify_stream_or_defer(&mut db, &key)` → `router.check_auto_tier_after_write()` → `+OK`, entirely bypassing the conditional/digest logic below it. Any other combination of flags, or a locally-existing AOF/replica requirement, falls through to the full branch: fetch `current_val`, evaluate `condition` (`None`/`Nx`/`Xx`/`Ifeq`/`Ifne`/`Ifdeq`/`Ifdne` — the last two compare against a 16-hex-char digest via `crate::table::compute_digest`), handle `past_expired` (treats the write as arriving with an already-expired TTL: deletes any existing value, still returns `+OK`/the old value per `GET`), then `db.set_extended` + AOF append + replication propagate + keyspace-notify. For a **remote** key, `notify_key_invalidation` is called *locally* (on the connection's own shard/port) before the write is even dispatched, then either `router.set(...).await` (unconditional fast case) or a full `router.execute_remote(target, Command::Set{...}).await` round-trip.

### 4.2 `execute_tx_step` (line 1302): `MULTI`/`EXEC`/`WATCH`, shared by TLS and plaintext connections

This function did not exist as a separate unit in the prior revision of this document — transaction handling has been extracted out of `handle_connection` into a standalone `async fn` so that `handle_tls_connection` can share the exact same `MULTI`/`EXEC`/`WATCH`/`DISCARD`/`RESET` state machine (§3.3). Signature takes `&mut in_multi`, `&mut tx_queue`, `&mut tx_has_error` by reference alongside the usual auth/asking state.

When **not** in a transaction: `MULTI` sets `in_multi = true` and clears `tx_queue`/`tx_has_error`; `WATCH(keys)` calls `watch_keys`; `UNWATCH` calls `unwatch_keys`; `DISCARD`/`EXEC` without a preceding `MULTI` are rejected; anything else falls through to `execute_command` directly.

When **in** a transaction: `MULTI` is rejected as nested; `WATCH` is rejected ("WATCH inside MULTI is not allowed"); `UNWATCH` is accepted as a no-op `+OK`; `DISCARD`/`RESET` clear the queue and call `crate::block::get_block_hub_for_port(port).lock().unwrap().clear_pending_notifies()`; `SAVE`/`BGSAVE`/`SHUTDOWN` and unknown commands set `tx_has_error = true` and reply with an error but stay queued-mode; everything else is pushed onto `tx_queue` with a `+QUEUED` reply. `EXEC` is the substantial case:

1. If `tx_has_error` → clear queue, reply `-EXECABORT`.
2. Else if `router.cluster_enabled && tx_has_cross_slot(tx_queue)` (line 256, checks every key of every queued command via `cmd_keys`/`key_slot`) → clear queue, reply `-CROSSSLOT`.
3. Else if `is_watch_tainted(port, client_id)` → clear queue, reply a null array (RESP `*-1\r\n`/`_\r\n`).
4. Else: `unwatch_keys`; compute the sorted, deduplicated set of shards touched by any queued command's keys (`router.target_shard` per key via `cmd_keys`); if more than one shard is touched, acquire a global VLL (very-large-lock) cross-shard transaction lock via `router.acquire_tx_locks(&sorted_shards, tx_id).await` with a monotonic `AtomicU64` transaction id — **shards are locked in sorted order**, a lock-ordering discipline that prevents deadlock against a concurrent transaction touching an overlapping, differently-ordered shard set; `BlockHub::pause()` (via `get_block_hub_for_port(port)`) suppresses list/zset-push wakeups for the transaction's duration; `IN_TX.set(true)`; each queued command runs through `execute_command` in order, writing directly into the shared `out_buf` after a `*<count>\r\n` array header; `IN_TX.set(false)`; if VLL was used, `router.release_tx_locks(...).await`; finally `hub_arc.lock().unwrap().resume()` drains any list/zset-push notifications that were deferred during the pause and replays them — local ones inline via `hub.notify_stream`/`notify_list`/`notify_zset` against `router.local_db`, remote ones via `ShardMessage::NotifyList { keys }` sent to the owning shard's sender.

### 4.3 Blocking commands: flush before you block

`BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BLMOVEM`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `XREAD`/`XREADGROUP` with `block_ms: Some(_)` are detected via `has_special` (they're all members of `is_special_pipeline_cmd`, line 19046) and, when present anywhere in a multi-command batch, force the **entire batch** onto the strictly-sequential `execute_command`-per-command path (never squashed) in `handle_connection`. Immediately before executing *each* blocking command specifically (not the whole batch), any output already queued in `out_buf` is flushed via a real `stream.write_all` — necessary because a blocking command can legitimately suspend the connection's task for up to its timeout while parked on `BlockHub`, and earlier pipelined replies in the same batch must not wait on that.

`handle_bzpop` (line 4385, the shared implementation behind `BZPOPMIN`/`BZPOPMAX`) is representative of the pattern used by all blocking commands: try every key once, locally (`db.zpopmin`/`zpopmax`) or remotely (`router.execute_remote` + `parse_zpop_items`); if nothing popped and `IN_TX.get()` (inside a `MULTI`/`EXEC`), immediately return a null array rather than blocking (Redis semantics: blocking commands never actually block inside a transaction); otherwise install a `BlockedClientGuard` (RAII, unregisters from `BlockHub` on drop even on early return/panic), register with `BlockHub::register_blocked_zset_client`/`register_zset_waiter`, and `wait_for_blocked_result` (line 407) — a polling loop that `monoio::time::timeout`s in 20ms slices against the wake channel, and on each timeout checks `is_fd_closed(raw_fd)` (a zero-length `MSG_PEEK` poll, line 378) so a client that vanished mid-block is detected within 20ms rather than waiting out the full block timeout. On a successful pop, the equivalent `ZPOPMIN`/`ZPOPMAX` is synthesized and appended to the AOF/replication stream exactly as if it had executed eagerly.

---

## 5. Client Output Buffer Limits & Slow-Consumer Protection

Gated behind `HAS_CUSTOM_BUFFER_LIMIT`, every pass through `handle_connection`'s main loop measures `out_buf.len()` against `get_client_output_buffer_limit(ClientClass::Normal)`:

```rust
if norm_limits.hard_limit > 0 && out_buf.len() as u64 >= norm_limits.hard_limit { break; }  // disconnect immediately
if norm_limits.soft_limit > 0 && out_buf.len() as u64 >= norm_limits.soft_limit {
    // disconnect only after the soft limit has been sustained for `soft_seconds`
    ...
} else {
    soft_limit_start = None;   // reset the sustained-breach timer once back under the soft limit
}
```

`ClientInfo.omem` is updated before and after the flush so `CLIENT LIST`/`CLIENT INFO`/`INFO` reflect live queue depth. Pub/sub connections enforce the `Pubsub` class independently and **unconditionally** (not gated on `HAS_CUSTOM_BUFFER_LIMIT`): `run_pubsub_loop` (line 2178) splits the socket and spawns a dedicated `monoio::spawn`ed writer task draining a bounded `flume::bounded(4096)` channel; that task tracks its own running `queued_bytes` total, applies the same hard/soft-limit policy, and simply stops draining (dropping the writer, tearing down the connection) on a breach.

---

## 6. Cross-Shard Interaction: How a Connection Gets a Reply for a Remote Key

**Re-verified finding, materially changed since the prior revision of this document:** `BatchResponder` is still a lock-free, allocation-minimizing mailbox — but it is no longer the single-slot `AtomicBool` + `UnsafeCell<Option<(items, results)>>` design described previously. The current design (`src/mailbox.rs` line 329) writes reply payloads **directly into the caller's pre-allocated response buffer** via a raw pointer, rather than building and handing back a second `Vec`:

```rust
pub struct BatchResponder {
    pub state: CachePadded<AtomicU8>,                                   // BATCH_IDLE|RUNNING|SLEEPING|COMPLETED
    pub responses_ptr: AtomicPtr<crate::shard::CompactResp>,            // points into the caller's `responses: Vec<CompactResp>`
    pub recycled_items: CachePadded<UnsafeCell<Option<Vec<(usize, u64, Command)>>>>,
    pub notify_tx: flume::Sender<()>,
    pub notify_rx: flume::Receiver<()>,
}
pub const BATCH_IDLE: u8 = 0;
pub const BATCH_RUNNING: u8 = 1;
pub const BATCH_SLEEPING: u8 = 2;
pub const BATCH_COMPLETED: u8 = 3;
```

Protocol, driven from `execute_commands_squashed` (caller side) and `src/server.rs`'s `ShardMessage::Batch` handler (remote-shard side):

1. **Caller** (`execute_commands_squashed`, line 19814-19830): `responder.prepare(responses.as_mut_ptr())` stores the pointer and sets `state = BATCH_RUNNING`; a `ShardMessage::Batch { items, responder: responder.clone(), is_resp3 }` is sent over `router.senders[target_shard]`; a `pending_mask: u64` bit is set for that shard.
2. **Remote shard** (`src/server.rs`, the live — non-`needs_async` — branch of the `ShardMessage::Batch` arm, starting line 967): for each `(idx, key_hash, cmd)` in `items`, runs the *same* inline fast-path/generic-dispatch logic as the local half of `execute_commands_squashed` (§6.1) directly against its own `ShardDb`, writing each result with `responder.write_slot(idx, resp)` — which writes through the stored pointer at `responses_ptr.add(idx)`, i.e. **directly into the original caller's `Vec`**, from a different reactor thread, with no channel hop for the payload itself. When the whole batch is done, `responder.finish(items)` stores the (now-empty, reusable) `items` Vec into `recycled_items` and does `state.swap(BATCH_COMPLETED, AcqRel)`; only if the swapped-out value was `BATCH_SLEEPING` does it bother `notify_tx.try_send(())` — if the caller is still spinning (`BATCH_RUNNING`), no channel send happens at all.
3. **Caller harvest** (`execute_commands_squashed`, lines 19841-19867): up to 256 spin iterations sweep every bit still set in `pending_mask`, calling `responder.try_take()` (checks `state == BATCH_COMPLETED`, resets to `BATCH_IDLE`, takes the recycled `items` Vec) and clearing that shard's bit on success, with `std::hint::spin_loop()` between sweeps. Any bit still set after 256 spins falls back to `responder.wait_take().await` — which does a `compare_exchange(RUNNING, SLEEPING)` to register interest (if the remote shard already completed and moved straight to `BATCH_COMPLETED`, the compare-exchange fails and `wait_take` returns immediately without ever touching the channel) before `notify_rx.recv_async().await`.

This trades a bounded amount of CPU spinning to avoid both a scheduler round-trip *and* a channel-send *and* a result-payload allocation when remote shards reply within microseconds (the common case), and degrades gracefully to a real async wait with no wasted notify when they don't. There is no `flume`-channel-based responder pool, and the `pub type ResponderChannel = (flume::Sender<(Vec<(usize, Command)>, Vec<(usize, CompactResp)>)>, ...)` alias still declared near the top of the file (line 21) is now doubly dead: it is referenced nowhere in the crate, **and** its element type no longer matches either `ShardMessage::Batch`'s actual payload (`Vec<(usize, u64, Command)>`, with a key hash now included) or `BatchResponder`'s pointer-based result delivery.

**Known limitation, newly identified in this revision**: the harvest loop's `pending_mask` is a `u64`, and shard indices are mapped to bits via `1u64 << target_shard` (line 19827/19847/19861). For `num_shards <= 64` (any machine with up to 64 cores/shards) this is exact. For `num_shards > 64`, `target_shard` values of 64 and above alias back onto bits 0-63 (in a release build, `1u64 << 64` is a no-overflow-checked shift that wraps to `1u64 << 0` on x86/ARM hardware shift semantics), which would cause the harvest loop to treat a still-outstanding high-numbered shard's batch as already-collected the moment shard 0 (or shard 64 mod 64, etc.) completes, yielding stale/uninitialized `CompactResp::empty()` entries in the response stream. This is architecturally the same class of bug the prior revision of this document flagged in the (now-fixed) `MGET`/`MSET` scatter-gather path — but it has re-appeared here, in a different mechanism, for deployments with more than 64 shards.

### 6.1 Single-key remote `GET`/`SET`: an even more specialized fast path

Distinct from the batch mailbox above, `src/mailbox.rs` also defines `FastGetDescriptor` (line 260) and `FastSetDescriptor` (line 292) — one-shot, single-key, `AtomicBool`-signaled shared-memory slots (`used from src/router.rs`, at the `router.get`/`router.set` call sites reached by `execute_command`'s `Get`/`Set` arms, §4.1.1/4.1.2, and by the Memcached bridge, §9) for the case of exactly one remote key with no batch to amortize allocation over. `FastGetDescriptor::finish(val)` writes the fetched `Option<Bytes>` into an `UnsafeCell`, sets `done` (`Release`), and notifies; `FastSetDescriptor` is the write-side equivalent carrying `key`/`value`/`expire_in`. These exist so that a single remote `GET`/`SET` — the overwhelmingly common single-command case — never pays for a `Vec`-based batch round-trip.

### 6.2 `MGET`/`MSET` shard targeting

`target_shard_of_cmd` (line 12338) returns `Some(shard)` for `MGET`/`MSET` when *every* key happens to hash to the same shard — in that case the command rides the ordinary single-target local/remote path with none of the cross-shard machinery below. Only when keys genuinely span multiple shards does `Router::begin_mget_resp`/`begin_mset` engage, using `Arc<ScatterMgetDescriptor>`/`Arc<ScatterMsetDescriptor>` (`src/mailbox.rs`) with an `AtomicUsize` pending-shard counter — confirmed still accurate: no fixed-width shard-count ceiling exists in this path (unlike the `pending_mask` issue above, which is specific to `execute_commands_squashed`'s generic batch harvest, not the dedicated MGET/MSET scatter-gather). In `execute_commands_squashed`, cross-shard `MGET`/`MSET` are dispatched *before* the remote-batch loop (via `router.begin_mget_resp`/`begin_mset`, without awaiting), so they run concurrently with the ordinary per-shard batches rather than stalling the pipeline; results are gathered via `finish_mget_resp`/`finish_mset` after the batch harvest (§6, step 3) completes.

### 6.3 RESP3 tracking invalidation across the mailbox boundary

Both the local fast paths in `execute_commands_squashed` (§6.4) and the remote shard's mirror of those fast paths in `src/server.rs`'s `ShardMessage::Batch` handler call `crate::connection::notify_key_invalidation` when `HAS_TRACKING_CLIENTS` is set — confirmed present on both sides. One asymmetry: `ShardMessage::Batch::items` carries only `(pipeline_idx, key_hash, Command)`, with no originating `client_id`, so the remote-shard side always calls `notify_key_invalidation(port, key, 0)` with a synthetic sender id of `0` instead of the real client. Per `notify_key_invalidation`'s self-exclusion rule (`tracker.client_id == sender_client_id && !tracker.bcast → skip`), this means a client whose own write crossed a shard boundary will receive its own RESP3 invalidation message for the key it just wrote (unless client id `0` happens to coincide), whereas a same-shard write correctly suppresses that self-notification. This is a minor RESP3-tracking-spec conformance gap, not a data-correctness bug.

### 6.4 `execute_commands_squashed` (line 19097, ~810 lines): eligibility gate, inline fast paths, dispatch, harvest

**Eligibility gate** (lines 19120-19219). `can_squash` starts as `*authenticated`. ACL and cluster-ownership checks run only `if has_special || user.is_some() || is_cluster`, iterating every command and — for ACL — every key of every command via `for_each_cmd_key` (not `cmd_primary_key`); for cluster mode, every key's slot must match the command's first key's slot, be `SlotState::Stable`, and be owned by this node's `my_slots`. Any single ineligible command or command flagged by `is_special_pipeline_cmd` forces the **entire batch** onto the sequential `execute_command` fallback (line 19221-19240), which applies the real `-NOPERM`/`-MOVED`/`-ASK`/`-CROSSSLOT` behavior per command.

**Inline local fast paths** (lines 19265-19805, one `if`/`else if` chain per command inside a `for (idx, cmd) in commands.drain(..).enumerate()` loop, guarded by `target_shard_and_hash_of_cmd` returning `Some((target, key_hash))` with a pre-computed hash so the `_with_hash` table accessors never re-hash the key): `GET` and single-key `EXISTS` are always eligible (including transparent NVMe cold-tier fallback, batched into `local_cold_gets` and resolved via `router.stream_cold_read_local` after the remote dispatch loop so it doesn't block it). `HGET`, `SISMEMBER`, `LRANGE`, `ZRANGE` are read fast paths with no extra gating. The write fast paths — `SET` (unconditional only), `INCRBY` (appears **twice**, see the dead-code note below), `DEL` (single key), `HSET`, `SADD`, `ZADD`, `LPUSH`, `LPOP`, `RPOP` — are each additionally gated on `router.aof.is_none() && !crate::replication::has_connected_replicas(router.port)`, with `SET` further requiring `NOTIFY_KEYSPACE_FLAGS == 0` (keyspace notifications off), `DEL`/`HSET`/`SADD` further requiring `!crate::search::has_active_search_indices()` is *not* checked for `SADD` (only `DEL` and `HSET` check it) , and `ZADD`/`LPUSH` further requiring `!crate::block::has_blocked_waiters(router.port)`. Every write fast path calls **both** `touch_watched_key` (gated on `HAS_WATCHED_KEYS`) **and** `notify_key_invalidation` (gated on `HAS_TRACKING_CLIENTS`) on success.

**Resolved since the prior revision of this document — the squashed `SET` fast path no longer skips `WATCH`/tracking.** The previous revision flagged that the inline `SET` fast path omitted `touch_watched_key`/`notify_key_invalidation` while its sibling fast paths called them. Re-reading the current code (lines 19304-19318) shows the `SET` fast path now calls both, in the same order and under the same atomic-gated pattern as every other fast path in this function. The equivalent fast path mirrored on the remote-shard side in `src/server.rs` (lines 1000-1027, the live branch) was independently checked and also calls both. **This gap is fixed.**

**Newly identified dead code**: two separate `else if` arms in the fast-path chain match `Command::IncrBy(ref key, delta)` under the *exact same* guard condition (`router.aof.is_none() && !crate::replication::has_connected_replicas(router.port)`) — lines 19319-19344 and lines 19646-19663. Since this is a single `if`/`else if` chain evaluated top-to-bottom, the second arm is unreachable: any `IncrBy` command matching the guard is always caught by the first arm first. The two arms are not quite identical in body (the first sets `has_local_writes = true` *before* `match`ing the table call and only inside the `Ok` branch increments watch/tracking notification before setting; the second sets `has_local_writes = true` unconditionally inside the `Ok` arm) but the practical effect is the same — the second block can never execute. This is dead code, not a correctness bug (the first arm's behavior — which does fire — is the one already covered by the `WATCH`/tracking-resolved note above), but it is worth cleaning up since it obscures that there are really only ~14 distinct fast-pathed command shapes, not 15.

**Remote dispatch and harvest**: see §6/§6.2 above — this is where `responder.prepare`/the `pending_mask` spin-then-`wait_take` harvest loop live. After the loop, `router.check_auto_tier_after_write()` runs once if `has_local_writes` was set by any local fast path. Responses are written to `out` in original pipeline order once every local, remote, and `MGET`/`MSET` result has been collected (`resp.write_to(out)` per `CompactResp`, with `out` pre-reserved via `estimated_len()`); two early-return shortcuts exist when *every* command in the batch was an `MGET` or every command was an `MSET` (lines 19872-19898), writing straight to `out` without ever populating `responses`.

---

## 7. Error Handling, Panic Isolation, and Known Bugs/Gaps

**Error reply formatting.** `write_resp_err` (line 983) only prefixes with a bare `ERR` for error strings that don't already start with a recognized Redis error code (`WRONGTYPE`, `CROSSSLOT`, `MOVED`, `ASK`, `NOSCRIPT`, `EXECABORT`, `BUSYGROUP`, `NOGROUP`, `INVALIDOBJ`, `ERR`); anything else gets `-ERR <msg>\r\n`. Every write-path in the file constructs errors as plain strings and routes them through this function (or the `BytesMut` variant, or a direct `out.extend_from_slice(b"-...")` for hardcoded protocol errors), so there is a single place controlling the wire-format contract for errors.

**Error-stat accounting** (new — see §4.1): `ErrorStatTracker`'s `Drop` impl inspects whatever was appended to `out` during the just-finished `execute_command` call and, if it starts with `-`, records the error-code prefix into `ERROR_STATS`/`FAILED_CMD_STATS`/`TOTAL_ERROR_REPLIES`. This is a RAII postcondition check on raw output bytes (an `unsafe { &*self.out }` raw-pointer read of the `out: *const Vec<u8>` field, since the tracker can't hold a live `&mut` alongside the function's own `&mut out` parameter for its whole scope) rather than a return-value-based mechanism — any command handler that writes an error string is automatically counted without having to call `record_error_stat` itself.

**Panic isolation** is not implemented in this file. `connection.rs` exposes only `ISOLATED_PANICS: AtomicU64` and `inc_isolated_panics()`/`get_isolated_panics()` (lines 1150-1160, backing `INFO`'s isolated-panic counter). The actual `catch_unwind_async` wrapping of each spawned connection task lives in `src/server.rs` (three call sites, including one specifically for cross-shard "adopted" connections handed off between shards for load balancing).

**Known gaps, current as of this revision:**

- **`CLIENT REPLY OFF`/`SKIP` is tracked but never enforced.** `ClientInfo.reply_mode` is set by the `Client(ClientSubcommand::Reply(mode))` handlers in both `execute_command` (line ~7195) and `handle_pubsub_cmd` (line ~2460) — both correctly suppress the `+OK` for the `CLIENT REPLY` command itself when switching to `Off`/`Skip` — but `reply_mode` is never read anywhere else in the file. Every subsequent command's normal reply is still written to `out_buf` and flushed exactly as if reply mode were `On`. A client that sends `CLIENT REPLY OFF` to suppress an otherwise-unparseable flood of replies (a common pattern for mass-pipelined writes) gets no suppression at all from this server.
- **`pending_mask: u64` in `execute_commands_squashed`'s remote-batch harvest caps correctness at 64 shards** (§6) — re-appearance, in a different mechanism, of the shard-count-ceiling bug class the prior revision of this document flagged (and which was independently fixed) in the `MGET`/`MSET` scatter-gather path.
- **Dead/unreachable duplicate `IncrBy` fast-path arm** in `execute_commands_squashed` (§6.4) — cosmetic, not a behavioral bug, but worth removing.
- **`ResponderChannel` type alias is dead and now additionally stale** (§6) — unreferenced, and its element type no longer matches either `ShardMessage::Batch`'s payload shape or `BatchResponder`'s pointer-based delivery mechanism.
- **`execute_command`/`execute_local_command`'s hand-synced duplication continues to grow.** The two independent `match` statements over `Command` are now roughly 7,432 and 6,312 lines respectively (both roughly 60-80% larger than the ~5,013/~4,054 lines noted in the prior revision), and a *third*, largely-mirrored copy of the hottest local fast paths (`GET`/`SET`/`INCRBY`/`HSET`/`SISMEMBER`/`SADD`/`LPUSH`/`LPOP`) now also lives in `src/server.rs`'s `ShardMessage::Batch` handler (the remote-shard receiving side, §6) — a third place a new command or a fast-path-eligibility change must be replicated by hand to stay consistent. No macro or exhaustiveness test enforces this today.
- **TLS connections still never take the pipeline-squashing fast path** (§3.3) — a real, current scope limitation: every pipelined command over TLS pays one `execute_command` call (and, for remote keys, one full `.await` round-trip), with no inline fast paths and no cross-shard parallel fan-out.
- **`CLIENT KILL`/`CLIENT PAUSE`/`CLIENT UNPAUSE`/`CLIENT NO-TOUCH`/`CLIENT CACHING` are accepted-but-inert `+OK` stubs** (§4.1) — clients that rely on `CLIENT KILL` to forcibly disconnect another client, or `CLIENT PAUSE` to briefly halt processing, will not observe the expected effect.
- **`src/server.rs`'s `needs_async` branch of the `ShardMessage::Batch` handler is permanently dead** (`let needs_async = false;`, hardcoded) — a substantial, separately-maintained async/`monoio::spawn` copy of the same fast-path dispatch logic that never executes. Out of scope for this document (it lives in `server.rs`, Component 01's file) but directly relevant to anyone auditing the cross-shard mailbox mechanism end-to-end, since it is easy to mistake for the live code path on a casual read.

---

## 8. Cluster Slot Migration Helpers (new since the prior revision)

Two functions implement `CLUSTER`-driven live slot migration directly in `connection.rs`, not delegated to `router.rs`:

- **`migrate_keys_to_node`** (line 3449): given a list of keys, `router.dump_key(k).await`s each one (returning the decoded `RudisValue` + remaining TTL, not a raw RDB blob), opens a plain `TcpStream` to the destination `host:port`, sends a leading `ASKING` command, then re-serializes each value as the equivalent write command(s) against its concrete type — `SET`/`SET ... PX <ms>` for `String`/`Int`, `HSET` (+ `PEXPIRE` if there's a TTL) for `SmallHash`/`Hash`, `RPUSH` for `List`, `SADD` for `Set`, `ZADD` for `ZSet`, a raw 16KiB `SET` of the dense register bytes for `HyperLogLog`, and one `XADD` per stream entry for `Stream`. `Tiered`/`Cooled` values are silently skipped (not migrated). This is a command-replay migration protocol, not a binary `RESTORE`/DUMP-format transfer, despite sending a Redis-Cluster-style leading `ASKING`. After a successful send+ack, if `!copy`, every migrated key is deleted locally via `router.del`.
- **`execute_rebalance_plans`** (line 3735): given a list of `crate::cluster::SlotMigrationPlan`s, for each plan where this node is the source, flips the slot to `SlotState::Migrating`, sends `CLUSTER SETSLOT <slot> IMPORTING <my_id>` to the target over a fresh `TcpStream`, drains the slot in batches of 100 keys via `router.get_keys_in_slot` + `migrate_keys_to_node` (non-copy), sends `CLUSTER SETSLOT <slot> NODE myself` to the target, then flips local state to `SlotState::Moved` and updates the in-memory `ClusterHub` node/slot tables; for each plan where this node is the target, flips straight to `Importing`/updates `my_slots` (the actual key transfer is driven by the *source* node's loop, not this one).

---

## 9. The Memcached Protocol Gateway

Command variants `MemcachedSet`/`MemcachedAdd`/`MemcachedReplace`/`MemcachedGet`/`MemcachedDelete`/`MemcachedIncr`/`MemcachedDecr`/`MemcachedStats`/`MemcachedVersion`/`MemcachedQuit` are parsed by `src/resp.rs` (protocol detection, §3.1) and handled as ordinary arms inside `execute_command` (lines 11869-12002+), routed through the *same* `router.get`/`router.set`/`router.exists`/`router.del`/`router.incr_by` calls that RESP commands use — there is no separate storage backend for Memcached keys/values, just a different wire encoding on the way in and out. `noreply` suppresses the `STORED`/`NOT_STORED`/`DELETED`/`NOT_FOUND` response per-command, matching the Memcached text protocol's fire-and-forget option. `MemcachedAdd`/`MemcachedReplace` implement Memcached's `NX`/`XX`-style semantics by calling `router.exists` first and branching, rather than relying on an atomic conditional set. `MemcachedStats` reports a synthetic `STAT version 1.6.0-rudis-dragonfly` line alongside the real `DBSIZE`. `MemcachedGet` supports multi-key requests (space-separated in the wire format), writing one `VALUE <key> 0 <len>\r\n<data>\r\n` block per hit and a trailing `END\r\n` regardless of hit count.

---

## 10. Cross-Component Interactions

- **`src/resp.rs`**: `parse_command` decodes buffered bytes into `Command` values (RESP2/RESP3 multi-bulk, Memcached text, plain inline — §3.1).
- **`src/router.rs`**: `Router` provides `target_shard`/`key_slot`-based local/remote decisions, `get_slot_state`/`target_shard_for_slot` (cluster migration/ownership), the `senders` mesh, `acquire_tx_locks`/`release_tx_locks` (VLL transaction locking, §4.2), `write_mget_resp`/`mset` (sequential path), `begin_mget_resp`/`finish_mget_resp`/`begin_mset`/`finish_mset` (squashed-path concurrent dispatch, §6.2), `get`/`set` (single-key remote fast path via `FastGetDescriptor`/`FastSetDescriptor`, §6.1), and `stream_cold_read_local`/`check_auto_tier_after_write` (tiered-storage integration).
- **`src/mailbox.rs`**: `BatchResponder` (§6), `FastGetDescriptor`/`FastSetDescriptor` (§6.1), `ScatterMgetDescriptor`/`ScatterMsetDescriptor` (§6.2), and the `SpscQueue` ring-buffer primitive.
- **`src/server.rs`**: spawns each connection task under `catch_unwind_async` (panic isolation, §7); contains the **remote-shard-side** handler for `ShardMessage::Batch` (the receiving half of §6), which mirrors most of `execute_commands_squashed`'s local fast paths.
- **`src/table.rs`** / **`src/shard.rs`**: `execute_local_command` and the squashed path's inline fast paths mutate `ShardDb`/`RudisTable` directly via `*_with_hash` accessors; `CompactResp` (`shard.rs`) is the reply payload type carried through `ShardMessage::Batch` and `BatchResponder`.
- **`src/block.rs`**: `BlockHub` (`get_block_hub_for_port`) — registration/wakeup for `BLPOP`/`BZPOPMIN`/blocking `XREAD` (§4.3), and the `pause`/`resume`/`add_pending_notify`/`clear_pending_notifies` mechanism transactions use (§4.2), plus `has_blocked_waiters` (gates the squashed `ZADD`/`LPUSH` fast paths).
- **`src/pubsub.rs`**: `PubSubHub`, entered via `run_pubsub_loop` on `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`; the pub/sub writer task independently enforces the `Pubsub` output-buffer-limit class (§5).
- **`src/replication.rs`**: replica-stream mode (`run_master_replica_stream`), Dragonfly shard-flow mode (`run_shard_replication_flow`), the `is_slave()`/`HAS_SLAVE_INSTANCE` write-rejection check, `has_connected_replicas`/`propagate_shard_bytes`/`propagate_bytes` (consulted by `record_change!` and by every squashed fast-path gate).
- **`src/cluster.rs`**: `get_cluster_hub`/`HAS_ACTIVE_CLUSTER` supply the gossiped slot-ownership table consulted during `MOVED` redirection (§4.1) and the squashed-path cluster-ownership check (§6.4); `migrate_keys_to_node`/`execute_rebalance_plans` (§8) drive live slot migration.
- **`src/acl.rs`** (Component 15): `HAS_CUSTOM_ACL`/`get_acl_for_port`; both `execute_command` and `execute_commands_squashed` enforce per-command/per-key authorization (`-NOPERM`) via `AclUser::can_execute_command`/`can_access_key`, checked against every key (`cmd_keys`/`for_each_cmd_key`), not just the routing-primary key.
- **`src/aof.rs`**: `execute_local_command`/`record_change!` take an `Option<&RefCell<AofWriter>>` to append write commands for persistence; `command_to_resp` is reused to detect "is this command a write" for the replica read-only guard, the `OOM` gate, and every squashed fast-path AOF-emptiness gate.
- **`src/slowlog.rs`**: `execute_command`'s `SlowlogTracker` RAII guard reports every command's wall-clock duration via `log_command_if_slow` when `SLOWLOG_LOG_SLOWER_THAN >= 0`.
- **`src/tls.rs`**: `handle_tls_connection` (§3.3) — the non-squashed TLS execution path, now sharing `execute_tx_step` with the plaintext path.
- **`src/scripting.rs`**: `Eval`/`Evalsha`/`Fcall` (§4.1) — local-shard-only dispatch, forwarding the whole script command to the key's owning shard rather than distributing execution.

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Memcached text-protocol detection is *not* a fixed first-byte lookup table. `parse_command` treats any frame not starting with `*` as non-RESP, tries the three storage verbs (`set`/`add`/`replace`) first, then falls back to inline parsing for `get`/`gets`/`delete`/`incr`/`decr`/`stats`/`version`/`quit` and bare inline RESP commands (§3.1).
* **Gotcha 2**: Cross-shard fan-out writes replies **directly into the caller's `responses: Vec<CompactResp>`** through a raw pointer stashed in `Arc<mailbox::BatchResponder>` (pooled per-reactor-thread in `ConnScratch`, recycled across connections) — not a `flume`-channel responder pool, and not the tuple-payload single-slot design described in older revisions of this document. The `ResponderChannel` type alias in this file is dead code with a stale signature (§6).
* **Gotcha 3**: The harvest loop that collects cross-shard batch replies uses a `u64 pending_mask` indexed by shard id — correct for up to 64 shards, silently wraps above that (§6, §7).
* **Gotcha 4**: The outbound socket buffer starts at a 64KiB (`READ_BUFFER_SIZE = 65536`) capacity and accumulates one contiguous `Vec<u8>` per read-loop iteration before flushing — a single coalesced write, not `writev`/vectored I/O, normally sent via a direct non-blocking `libc::send` before falling back to `io_uring write_all` (§3.2).
* **Gotcha 5**: The squashed pipeline path's inline write fast paths (`SET`/`INCRBY`/`DEL`/`HSET`/`SADD`/`ZADD`/`LPUSH`/`LPOP`/`RPOP`) are only taken when there is no AOF writer and no connected replica (and, for some commands, no active search index, no blocked waiter, or — for `SET` specifically — no keyspace-notification flags configured) — otherwise the command falls through to the generic dispatch path so persistence/replication/indexing/blocking/notification side effects are not silently skipped. As of this revision, every one of these fast paths — including `SET` — correctly taints `WATCH` and fires RESP3 tracking invalidation (§6.4); this was not true in the prior revision.
* **Gotcha 6**: `CLIENT REPLY OFF`/`SKIP`, `CLIENT KILL`, `CLIENT PAUSE`/`UNPAUSE`, `CLIENT NO-TOUCH`, and `CLIENT CACHING` are all present as parsed commands returning plausible replies, but none of them change connection behavior — do not assume `CLIENT REPLY OFF` will suppress output when debugging a client that expects it to (§7).
* **Gotcha 7**: `src/server.rs` carries a large, permanently-unreachable `async`/`monoio::spawn` duplicate of the `ShardMessage::Batch` fast-path dispatch logic (`needs_async` is hardcoded `false`). When tracing the actual remote-shard-side code path, make sure you're reading the `else` branch, not the dead `if needs_async` branch (§6, §7).

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
