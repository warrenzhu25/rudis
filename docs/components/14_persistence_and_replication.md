# Component 14: Persistence & Replication Engines (`src/replication.rs`, `src/aof.rs`)

## 1. Architectural Purpose & Scope

This subsystem covers two related but independent durability mechanisms:

1. **Append-Only File (AOF) Engine (`src/aof.rs`)**: converts mutating `Command`s back into
   RESP bytes (`command_to_resp`) and appends them to a per-shard file via a buffered,
   periodically-flushed `AofWriter`. On restart, `replay_aof` re-parses the file and replays
   every command through the normal command-execution path.
2. **Replication Hub (`src/replication.rs`)**: a per-port, process-wide `ReplicationHub`
   (master or slave role) that fans out every mutating command to connected replicas over
   plain `flume` channels, and — on the replica side — connects to a master, requests a
   **full** RDB snapshot every time, then applies the live command stream that follows.

**Correction vs. an earlier draft of this document**: there is no AOF rewrite/compaction
mechanism of any kind, and there is no partial resynchronization. Both "forkless rewrite"
and "PSYNC partial resync" described previously do not exist in the real code — see §4.1
and §4.3 for what's actually there instead.

---

## 2. Key Invariants & Concurrency Constraints

1. **AOF is append-only, forever.** `AofWriter::append` only ever grows `self.buffer`
   (later flushed to disk via `write_all_at` at the current end-of-file `offset`). There is
   no `BGREWRITEAOF`, no periodic compaction, and no mechanism that ever shrinks or rewrites
   the file — it grows for as long as the process runs with AOF enabled.
2. **Replication is always full resync.** The master side (`run_master_replica_stream` in
   `src/connection.rs`) unconditionally replies `+FULLRESYNC <replid> <offset>` followed by
   a complete RDB blob, regardless of what offset the replica actually asked for — the
   `PSYNC` command's own arguments are received but never inspected (the handler's
   `_psync_cmd` parameter is prefixed with `_` and unused). The replica side
   (`run_replica_worker`) also only ever sends `PSYNC ? -1`, i.e. it never even attempts a
   partial resync request.
3. **The replication backlog is maintained but never read back.** `ReplicationHub::propagate`
   appends every propagated command to `self.backlog` (a `ReplicationBacklog`) in addition to
   forwarding it live to connected replicas — but nothing in the codebase ever calls a method
   to serve a range of the backlog to a reconnecting replica. It exists to answer `INFO
   replication`'s `repl_backlog_*` fields, not to actually enable resumption.
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
                                      │
                       always "+FULLRESYNC <replid> <offset>\r\n$<len>\r\n<rdb-bytes>"
                                      │
                    router.restore_rdb_bytes(rdb) then apply the live command stream
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
draft of this doc was misleading. It also, per §2.3, is never actually read from again.

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

### 4.3 Replication is unconditional full resync, both directions

Master side, `run_master_replica_stream` (`src/connection.rs`, entered when a connection
sends `PSYNC`):

```rust
let rdb = router.generate_full_rdb().await;
let _repl = hub.register_replica(client_id, write_tx.clone());
let replid = hub.master_replid.clone();
let offset = hub.master_repl_offset.load(Ordering::SeqCst);
let mut initial_msg = format!("+FULLRESYNC {} {}\r\n${}\r\n", replid, offset, rdb.len()).into_bytes();
initial_msg.extend_from_slice(&rdb);
writer.write_all(initial_msg).await;
```

The command that triggered this (`_psync_cmd`, i.e. whatever replid/offset the replica
actually requested) is received but discarded — there is no code path that ever produces a
`+CONTINUE` reply. After the initial RDB transfer, the connection is handed a dedicated
writer task draining `write_rx` (fed by `ReplicationHub::propagate`, so this replica now
receives every future mutation live) and a reader loop that only looks for
`REPLCONF ACK <offset>` to update `ConnectedReplica::ack_offset`/`last_ack_time`.

Replica side, `run_replica_worker` (`src/replication.rs`) — a hand-rolled RESP handshake
using a `send_and_expect_line!` macro (write a command, read until `\r\n`), not the shared
`Router`/connection machinery:

```rust
// 1. PING → expect +PONG
// 2. REPLCONF listening-port <port> → expect +OK
// 3. REPLCONF capa psync2 → expect +OK
// 4. PSYNC ? -1 → expect +FULLRESYNC <replid> <offset>
// 5. Read "$<rdb_len>\r\n", then read exactly rdb_len more bytes
// 6. router.restore_rdb_bytes(rdb_bytes).await
// 7. mark link_status "up", set master_repl_offset = initial_offset
// 8. loop: parse_command on the ongoing stream; REPLCONF GETACK → reply REPLCONF ACK <offset>;
//    everything else (except PING, which is just a keepalive/no-op) → router.execute_replica_command(cmd).await
```

`PSYNC ? -1` is Redis's own wire syntax for "I have no prior state, give me everything" —
the replica here never has any other code path, so it always looks like a brand-new replica
to the master, every single time it (re)connects, even after a brief network blip.

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
- **Every full resync re-transfers the entire dataset**, serialized fresh via
  `generate_full_rdb` (which itself fans out to every shard and waits for all chunks) —
  there is no incremental/partial catch-up path (§4.3), so a replica that merely blips its
  network connection pays the same full-dataset-transfer cost as a brand-new replica joining
  for the first time.
- **Replication fan-out itself is cheap per write**: `propagate` is an `O(num_replicas)`
  loop of non-blocking `flume` sends per mutating command, independent of dataset size.
