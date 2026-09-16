# Component 14: Persistence & Replication Engines (`src/replication.rs`, `src/aof.rs`)

## 1. Architectural Purpose & Scope

This subsystem covers two related but independent durability mechanisms:

1. **Append-Only File (AOF) Engine (`src/aof.rs`)**: converts mutating `Command`s back into
   RESP bytes (`command_to_resp`) and appends them to a per-shard file via a buffered,
   periodically-flushed `AofWriter`. On restart, `replay_aof` re-parses the file and replays
   every command through the normal command-execution path.
2. **Replication Hub (`src/replication.rs`)**: a per-port, process-wide `ReplicationHub`
   (master or slave role) that fans out every mutating command to connected replicas over
   plain `flume` channels. The master side now supports **real partial resync** (`+CONTINUE`
   from the backlog) when a reconnecting client presents a valid replid+offset, falling back
   to a full RDB snapshot otherwise — see §4.3 for the update to this (this doc previously,
   correctly, documented this as entirely unimplemented; it has since been built).

**Update**: AOF rewrite/compaction is still entirely absent (§4.1 — unchanged). Partial
resynchronization, previously fully unimplemented, is now real on the **master** side
(§4.3) — but see §4.3's caveat: this codebase's own **replica** implementation never
actually requests one.

---

## 2. Key Invariants & Concurrency Constraints

1. **AOF is append-only, forever.** `AofWriter::append` only ever grows `self.buffer`
   (later flushed to disk via `write_all_at` at the current end-of-file `offset`). There is
   no `BGREWRITEAOF`, no periodic compaction, and no mechanism that ever shrinks or rewrites
   the file — it grows for as long as the process runs with AOF enabled.
2. **Master-side partial resync is now real; the replica side never asks for one.**
   `run_master_replica_stream` (`src/connection.rs`) now inspects the `PSYNC` command's own
   replid/offset arguments (previously received but discarded) via
   `ReplicationHub::try_partial_resync`, and replies `+CONTINUE <replid>\r\n<backlog-diff-bytes>`
   when the requested offset still falls inside the retained backlog window for a matching
   replid, falling back to `+FULLRESYNC <replid> <offset>\r\n$<len>\r\n<rdb-bytes>` otherwise
   (§4.3). **But** `run_replica_worker` — this codebase's own replica-side connect logic —
   still unconditionally sends the literal `PSYNC ? -1` on every connection, including
   reconnects; it never tracks or requests its own last-known offset. So a rudis replica
   talking to a rudis (or real Redis) master never actually triggers the new `+CONTINUE` path
   itself — the feature only benefits a *different* client (a real Redis replica, or a future
   rudis version) connecting to a rudis master. See §4.3 and Future Improvements.
3. **The replication backlog is maintained, and now genuinely read back — by the master
   serving a partial resync.** `ReplicationHub::propagate` still appends every propagated
   command to `self.backlog` (a `ReplicationBacklog`), and that data is no longer write-only:
   `try_partial_resync`/`ReplicationBacklog::get_diff` (§4.3) slice it to answer a
   `+CONTINUE` request. It's also still consulted for `INFO replication`'s `repl_backlog_*`
   fields, as before.
4. **Replication uses shared, cross-thread state (`Arc`/`RwLock`), unlike the request path.**
   `ReplicationHub` is looked up via `get_replication_hub(port)` from a process-wide
   `LazyLock<RwLock<HashMap<u16, Arc<ReplicationHub>>>>` — every shard on a given port shares
   the *same* hub instance, guarded by `RwLock`s and atomics throughout. This is a second,
   independent exception to the "zero locks" architecture (the first being the blocking-op
   `BlockHub` documented in Component 06), needed because replication fan-out is inherently
   cross-shard: any shard's write must reach every connected replica, not just the shard that
   handled it.
5. **AOF writing and replication propagation are decoupled but both driven from the same
   call site.** `record_mutation` (in `replication.rs`) is the single function that both
   appends to a shard's local `AofWriter` (if present) and calls `propagate_bytes` — but nothing
   requires both to be enabled together; a node can run with AOF on and no replicas, or
   replication with AOF disabled, independently.

---

## 3. Component Architecture & Data Structures

```
                         Write Command (e.g. SET k v)
                                      │
                                      ▼
                    Local ShardDb / RudisTable mutation (instant)
                                      │
                                      ▼
                    record_mutation(port, aof, &cmd)  [replication.rs]
                                      │
                    command_to_resp(cmd) → RESP bytes  [aof.rs]
                         │                        │
                         ▼                        ▼
              AofWriter::append (if Some)   ReplicationHub::propagate (per port)
                         │                        │
              50ms flush task in server.rs         ├─► backlog.append (Vec<u8>, bounded, drain-on-overflow)
              (write_all_at + ~1s fsync)            └─► every ConnectedReplica.sender.send(bytes)
                                                              (flume channel → replica's own writer task)

                         Replica reconnect / initial sync
                                      │
                          run_replica_worker (replication.rs)
                  PING → REPLCONF listening-port → REPLCONF capa psync2 → PSYNC ? -1
                        (this replica ALWAYS asks for "? -1" -- see §4.3 caveat)
                                      │
                     hub.try_partial_resync(replid, offset)?  [master, connection.rs]
                          ┌──────────yes──────────┐         ┌──────────no───────────┐
                          ▼                        ▼         ▼                        ▼
              "+CONTINUE <replid>\r\n"    backlog.get_diff()   "+FULLRESYNC id off\r\n"   generate_full_rdb()
                          └──────────┬─────────────┘         └───────────┬────────────┘
                                     ▼                                    ▼
                    apply just the missing bytes         router.restore_rdb_bytes(rdb)
                                     └──────────────┬─────────────────────┘
                                                     ▼
                                     then apply the live command stream
```

### `AofWriter` and `AofConfig` (`src/aof.rs`)

```rust
#[derive(Clone, Debug)]
pub struct AofConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub fsync_every_sec: bool,
}

pub struct AofWriter {
    buffer: Vec<u8>,
    file: Option<std::rc::Rc<monoio::fs::File>>,
    path: PathBuf,
    offset: u64,
}
```

`AofWriter::open` creates the directory if needed, opens the file with
`monoio::fs::OpenOptions::new().create(true).write(true)`, and reads the existing file's
length via `metadata()` to initialize `offset` — so re-opening an existing AOF on restart
resumes appending at the correct byte position rather than truncating it.

### `ReplicationHub`, `ReplicationRole`, `ReplicationBacklog`, `ConnectedReplica` (`src/replication.rs`)

```rust
pub enum ReplicationRole {
    Master { replid: String, replid2: String, second_offset: i64 },
    Slave {
        master_host: String,
        master_port: u16,
        link_status: String,          // "connecting" | "up" | "down"
        master_repl_offset: u64,
        sync_in_progress: bool,
    },
}

pub struct ReplicationHub {
    pub port: u16,
    pub role: RwLock<ReplicationRole>,
    pub master_replid: String,
    pub master_repl_offset: AtomicU64,
    pub has_replicas: std::sync::atomic::AtomicBool,
    pub backlog_active: std::sync::atomic::AtomicBool,   // new: keep the backlog live even with zero replicas
    pub backlog: RwLock<ReplicationBacklog>,
    pub replicas: RwLock<HashMap<u64, Arc<ConnectedReplica>>>,
    pub cancel_sync: RwLock<Option<flume::Sender<()>>>,
}

pub struct ReplicationBacklog {
    pub buffer: Vec<u8>,
    pub max_size: usize,
    pub first_byte_offset: u64,
}

pub struct ConnectedReplica {
    pub id: u64,
    pub sender: flume::Sender<Vec<u8>>,
    pub listening_port: AtomicU64,
    pub ack_offset: AtomicU64,
    pub last_ack_time: AtomicU64,
}
```

`replid` (both master's own and the one stored per-role) is generated once via
`fxhash::hash64` over the port and current timestamp, formatted as a 40-hex-char string
(`format!("{:016x}{:016x}{:08x}", h1, h2, port)`) — a real, unique-enough identifier, but
not cryptographically derived the way Redis's own replid generation is.

**`ReplicationBacklog` is not actually a ring buffer.** `append` does
`self.buffer.extend_from_slice(data)` and, if that exceeds `max_size` (1MB, hardcoded in
`ReplicationHub::new`), calls `self.buffer.drain(..overflow)` to shift the oldest bytes out
and recomputes `first_byte_offset` from the new length. This is a correct *bounded sliding
window* (same end effect: only the most recent `max_size` bytes are retained), but it's
implemented as a plain growable `Vec` with a linear `drain`, not a fixed-capacity circular
buffer with modular read/write cursors — the "circular ring buffer" framing in an earlier
draft of this doc was misleading. Per §2.3, it's now genuinely read back to serve partial
resyncs.

**`propagate` now backlogs independently of whether any replica is currently connected.**
Previously (and still true for the live fan-out half) `propagate` only did anything if
`is_master()` — but the backlog-append is now gated on a separate `backlog_active` flag,
not on `has_replicas`. This matters precisely because partial resync needs history to exist
*before* a replica reconnects: if the backlog only grew while a replica was actively
connected, every replica that fully disconnected and came back would find an empty/stale
backlog and be forced into a full resync anyway, defeating the point. `has_connected_replicas`
now also reports `true` whenever `backlog_active` is set, even with zero live replicas.

---

## 4. Execution Algorithms & Code Logic

### 4.1 There is no AOF rewrite — `command_to_resp` is the entire AOF write path

```rust
pub fn command_to_resp(cmd: &Command) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    match cmd {
        Command::Set { key, value, expire_in, .. } => {
            if let Some(dur) = expire_in {
                let ms = dur.as_millis().max(1);
                // ... re-serializes as "SET key value PX <ms>" ...
            } else {
                // ... re-serializes as "SET key value" ...
            }
            Some(buf)
        }
        Command::Mset(pairs) => { /* re-serializes as MSET k1 v1 k2 v2 ... */ Some(buf) }
        Command::Del(keys) => { /* re-serializes as DEL k1 k2 ... */ Some(buf) }
        // ... one arm per write command: IncrBy, Expire→PEXPIRE, Persist, Hset/Hmset→HSET,
        // Hsetnx, Hdel (and Hgetdel, reusing the HDEL wire form), Lpush/Rpush, Lpop/Rpop,
        // Sadd/Srem/Spop, Zadd (re-encoding NX/XX/GT/LT/CH/INCR flags), Zrem, Zincrby,
        // Setnx, Getset→SET, Getdel→DEL, Append, Rename, Flushdb/Flushall→FLUSHDB,
        // Msetnx→MSET, Setbit, Bitop, Pfadd, Pfmerge, Restore, Xadd/Xdel/Xtrim,
        // XgroupCreate/XgroupDestroy, Xack, Hincrby(float), Smove, the Zremrangeby*
        // family, Ltrim, Lset, Lrem, Linsert, Lmove, Incrbyfloat, Setrange, and
        // Sort-with-STORE — all deterministically re-encoded as RESP arrays.
        _ => None,
    }
}
```

This function is the *only* thing that decides what gets logged: any command not matched
falls through to `_ => None` and is silently **not** persisted to the AOF (and, since
`record_mutation` gates propagation on this same function's `Some`, also not replicated).
Every write command that exists has to have an explicit arm here or it's invisible to both
persistence and replication — there's no generic/derived serialization.

`replay_aof` is the mirror image on startup:

```rust
pub fn replay_aof(path: &Path, db: &mut ShardDb) -> std::io::Result<usize> {
    if !path.exists() { return Ok(0); }
    let data = std::fs::read(path)?;
    let mut buf = BytesMut::from(&data[..]);
    let mut count = 0;
    let mut dummy_out = Vec::new();
    while !buf.is_empty() {
        match crate::resp::parse_command(&mut buf) {
            Ok(Some(cmd)) => {
                crate::connection::execute_local_command(&cmd, db, &mut dummy_out, None);
                dummy_out.clear();
                count += 1;
            }
            Ok(None) | Err(_) => break,
        }
    }
    Ok(count)
}
```

It reads the *entire* file into memory in one `std::fs::read`, then parses and replays every
command through the exact same `execute_local_command` used for live traffic (see Component
02) — there is no separate "restore format," the AOF file is just literal RESP commands.
Since nothing ever compacts the file, replay time and file size both grow without bound
relative to total write volume over the file's lifetime, not relative to the current dataset
size (unlike an RDB-based restore, which is proportional to current key count — see
Component 05 for the actual RDB chunk format).

### 4.2 The 50ms/~1s flush-and-fsync cadence (driven from `src/server.rs`, not this file)

`aof.rs` itself has no background task — `AofWriter::take_flush_chunk` just hands ownership
of the pending buffer to whoever calls it:

```rust
pub fn take_flush_chunk(&mut self) -> Option<(std::rc::Rc<monoio::fs::File>, Vec<u8>, u64)> {
    if self.buffer.is_empty() { return None; }
    let file = self.file.clone()?;
    let chunk = std::mem::replace(&mut self.buffer, Vec::with_capacity(65536));
    let off = self.offset;
    self.offset += chunk.len() as u64;
    Some((file, chunk, off))
}
```

The caller (a periodic task spawned in `run_shard_worker`, per Component 01) calls this
every 50ms, and if it returns `Some`, does `file.write_all_at(chunk, offset)` — a
positioned write, so the shared `Rc<monoio::fs::File>` needs no seek state — and calls
`sync_data()` roughly every 20th tick (~1s) when `fsync_every_sec` is set. `flush()`/`sync()`
on `AofWriter` itself just call `take_flush_chunk` once and await the write/fsync inline —
those are the paths used by e.g. an explicit `sync_aof()` call from `Router::save_rdb`
(Component 04), not the steady-state background cadence.

### 4.3 Partial resync is real on the master side; the replica side never requests one

Master side, `run_master_replica_stream` (`src/connection.rs`, entered when a connection
sends `PSYNC`) now genuinely inspects the request instead of discarding it:

```rust
let (req_replid, req_offset) = match &psync_cmd {
    Command::Psync { replid, offset } => (std::str::from_utf8(replid).unwrap_or(""), *offset),
    _ => ("", -1),
};

let partial = hub.try_partial_resync(client_id, write_tx.clone(), req_replid, req_offset);
if let Some((replid, diff, _repl)) = partial {
    let mut initial_msg = format!("+CONTINUE {}\r\n", replid).into_bytes();
    initial_msg.extend_from_slice(&diff);
    writer.write_all(initial_msg).await;
} else {
    let rdb = router.generate_full_rdb().await;
    let _repl = hub.register_replica(client_id, write_tx.clone());
    let replid = hub.master_replid.clone();
    let offset = hub.master_repl_offset.load(Ordering::SeqCst);
    let mut initial_msg = format!("+FULLRESYNC {} {}\r\n${}\r\n", replid, offset, rdb.len()).into_bytes();
    initial_msg.extend_from_slice(&rdb);
    writer.write_all(initial_msg).await;
}
```

`ReplicationHub::try_partial_resync` (`src/replication.rs`) does the real work: it checks
the requested replid against the master's own (or its previous `replid2`, for the "I was
just promoted" case, gated by `second_offset`), then asks `ReplicationBacklog::can_partial_sync`/
`get_diff` whether `target_offset = req_offset + 1` still falls inside the retained backlog
window — if so, it returns the exact missing byte slice; if the offset is already
up-to-date, an empty diff; if the offset predates what the backlog retained, `None` (forcing
the caller to fall back to `+FULLRESYNC`). This is covered by real unit tests
(`test_backlog_append_and_diff`, `test_try_partial_resync`) exercising the boundary cases
(exactly up to date, mid-backlog, evicted-by-overflow, and an offset beyond the master's own).
After either path, the connection is handed a dedicated writer task draining `write_rx` (fed
by `ReplicationHub::propagate`, so this replica now receives every future mutation live) and
a reader loop that only looks for `REPLCONF ACK <offset>` to update
`ConnectedReplica::ack_offset`/`last_ack_time` — unchanged from before.

Replica side, `run_replica_worker` (`src/replication.rs`) — a hand-rolled RESP handshake
using a `send_and_expect_line!` macro (write a command, read until `\r\n`), not the shared
`Router`/connection machinery:

```rust
// 1. PING → expect +PONG
// 2. REPLCONF listening-port <port> → expect +OK
// 3. REPLCONF capa psync2 → expect +OK
// 4. PSYNC ? -1 → expect +FULLRESYNC <replid> <offset>  OR  +CONTINUE <replid>
// 5. (only if +FULLRESYNC) Read "$<rdb_len>\r\n", then read exactly rdb_len more bytes,
//    then router.restore_rdb_bytes(rdb_bytes).await
// 6. mark link_status "up", set master_repl_offset = initial_offset
// 7. loop: parse_command on the ongoing stream; REPLCONF GETACK → reply REPLCONF ACK <offset>;
//    everything else (except PING, which is just a keepalive/no-op) → router.execute_replica_command(cmd).await
```

**The real, precise gap**: step 4's literal payload — `b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"`
— is unconditional, hardcoded on every single call to `run_replica_worker`, including
reconnects after a network blip. `PSYNC ? -1` is Redis's own wire syntax for "I have no
prior state, give me everything," and `try_partial_resync` explicitly rejects any negative
offset (`if req_offset < 0 { return None; }`). So **this codebase's own replica
implementation can never trigger the `+CONTINUE` path it just gained**, against a rudis
master or a real one — the new partial-resync machinery is real and tested, but today it
can only be exercised by some *other* client that actually tracks and sends its own
replid/offset (a real Redis replica, or a future rudis version that fixes this). The code
does now branch on the reply (`is_continue`), so it would correctly *handle* a `+CONTINUE`
if it ever received one — it just never asks for one.

### 4.4 `INFO replication` / `ROLE` are read directly off the atomics/`RwLock`, not cached

`format_info_replication` and `format_role_resp` both take a fresh read-lock on `role` and
load the atomics (`master_repl_offset`, per-replica `ack_offset`/`listening_port`) on every
call — there's no periodic snapshot; the command handler just formats whatever the live
state currently is at request time.

---

## 5. Cross-Component Interactions

- **`src/router.rs`** (Component 04): `Router::del`/`set`/`incr_by`/`expire`/`persist` (the
  per-op local-execution branches) call `crate::aof::command_to_resp` directly and append to
  `self.aof` inline; `Router::save_rdb`/`bgsave`/`generate_full_rdb`/`restore_rdb_bytes` drive
  the RDB side of a full resync by delegating to `crate::table::save_rdb_chunk`/`load_rdb_bytes`
  (Component 05) per shard and stitching per-shard chunks into one blob framed with a
  `"REDIS0011"` header and a trailing CRC64 (`crate::table::crc64`).
- **`src/connection.rs`** (Component 02): owns the actual `PSYNC`-triggered
  `run_master_replica_stream` handler (master side) described in §4.3; also calls
  `crate::replication::record_mutation` after executing local write commands so both AOF and
  live replicas see them.
- **`src/server.rs`** (Component 01): spawns the 50ms AOF flush task and the AOF replay call
  on startup described in §4.1/§4.2; not otherwise involved in replication.
- **`src/table.rs`** (Component 05): supplies the actual RDB chunk serialization/deserialization
  (`save_rdb_chunk`, `load_rdb`, `load_rdb_bytes`) that both startup restore and full-resync
  RDB transfer are built on — this file only stitches those chunks together and moves the
  bytes over the wire.

---

## 6. Performance Characteristics

- **AOF write cost is O(1) amortized per command**, bounded by the 50ms/~1s flush-fsync
  cadence — but **AOF file size and restart replay time are both unbounded** relative to
  total lifetime write volume, since nothing ever compacts the file (§4.1). A long-running,
  write-heavy node with AOF enabled will have an ever-growing file and an ever-growing
  startup replay cost.
- **Full resync still re-transfers the entire dataset** via `generate_full_rdb` (fans out to
  every shard, waits for all chunks) — but this is no longer the *only* path (§4.3): a
  client presenting a still-in-backlog offset gets a `+CONTINUE` and just the missing bytes
  instead. The catch, per §4.3, is that this codebase's own replica never actually asks for
  the cheap path — a rudis-to-rudis pair still pays full-dataset-transfer cost on every
  reconnect today, even though the master is capable of doing better.
- **Replication fan-out itself is cheap per write**: `propagate` is an `O(num_replicas)`
  loop of non-blocking `flume` sends per mutating command, independent of dataset size.

---

## 7. Future Improvements

- **High — implement AOF rewrite/compaction (§4.1).** An AOF-enabled node that runs for a long time under sustained writes has an ever-growing file and an ever-growing restart replay cost, with no relief mechanism (no `BGREWRITEAOF` equivalent exists at all). Since `RudisTable` already has a working RDB chunk format (Component 05) used for full resync, the natural implementation is: periodically (or on an explicit `BGREWRITEAOF`-equivalent command) snapshot the current dataset to a fresh AOF-equivalent-from-RDB, atomically swap it in for the old growing file, and discard the old one — reusing existing RDB serialization rather than building new compaction logic from scratch.
- **RESOLVED (master side) / High (replica side still open) — partial resynchronization (§2.3/§4.3).** The master now genuinely serves `+CONTINUE` with just the missing backlog bytes when a valid replid+offset is presented, tested against boundary cases (`test_backlog_append_and_diff`, `test_try_partial_resync`). What's still missing: `run_replica_worker` always sends `PSYNC ? -1`, never its own last-known offset, so this codebase's own replica can never actually trigger the path it just gained. The remaining fix is narrow: track `master_repl_offset`/`master_replid` across a replica's disconnect (they already live on `ReplicationRole::Slave`, just aren't read at handshake time) and send `PSYNC <replid> <offset>` instead of `PSYNC ? -1` whenever those are known from a prior successful sync.
- **Low — derive `replid` from something closer to Redis's real generation scheme**, or at least document that the current `fxhash`-over-port-and-timestamp approach (§3) is a real, working, but not cryptographically-derived identifier — same category of note as Component 11's node-ID generation.
- **Low — make the 1MB `ReplicationBacklog` size and the 50ms/~1s AOF flush/fsync cadence configurable** rather than hardcoded, once partial resync (above) makes the backlog size an operationally meaningful tuning knob rather than just an `INFO`-reporting detail.
