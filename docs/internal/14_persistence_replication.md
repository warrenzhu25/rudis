# Component 14: Persistence & Replication Engines (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/replication.rs`, `src/aof.rs`
> **High-Level Design Spec**: [`docs/design/14_persistence_replication.md`](../design/14_persistence_replication.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/replication.rs` | Replication hub, backlog, PSYNC and DFLY-FLOW state, replica worker | `ReplicationHub`, `ReplicationBacklog`, `ConnectedReplica`, `ShardReplicaFlow`, `run_replica_worker`, `record_mutation` |
| `src/aof.rs` | AOF writer, RESP re-encoding, replay, rewrite/compaction | `AofWriter`, `AofConfig`, `command_to_resp`, `replay_aof`, `rewrite_shard_aof` |

The master-side PSYNC and DFLY-FLOW connection handlers (`run_master_replica_stream`, `run_shard_replication_flow`) live in `src/connection.rs`, not in `replication.rs` itself; they are covered here because they are the other half of the protocols this component implements.

---

## 2. Data Flow Overview

```
                         Write Command (e.g. SET k v)
                                      │
                                      ▼
                    Local ShardDb / RudisTable mutation (instant, in-thread)
                                      │
                                      ▼
                    record_mutation(port, aof, &cmd)        [replication.rs]
                                      │
                    command_to_resp(cmd) → RESP bytes        [aof.rs]
                         │                        │
                         ▼                        ▼
              AofWriter::append (if Some)   ReplicationHub::propagate (per port)
                         │                        │
              5ms flush task in server.rs          ├─► backlog.append (fixed-size ring buffer)
              (write_all_at + ~1s fsync)            ├─► every ConnectedReplica.sender.send (PSYNC path)
                                                     └─► every ShardReplicaFlow.sender.send  (DFLY FLOW path)

        ── PSYNC reconnect (rudis replica, or any Redis/Valkey-protocol client) ──
        run_replica_worker: PING → REPLCONF listening-port → REPLCONF capa psync2
                            → PSYNC <cached_replid> <cached_offset>  (or "? -1" if none cached)
                                      │
                     hub.try_partial_resync(replid, offset)?  [master, connection.rs]
                ┌─────────yes──────────┐              ┌─────────no───────────┐
                ▼                      ▼               ▼                     ▼
     "+CONTINUE <replid>\r\n"   backlog.get_diff()   "+FULLRESYNC id off\r\n" generate_full_rdb()
                └──────────┬───────────┘              └──────────┬───────────┘
                           ▼                                     ▼
             apply just the missing bytes            router.restore_rdb_bytes(rdb)
                           └───────────────┬─────────────────────┘
                                            ▼
                            then apply the live command stream

        ── DFLY FLOW (Dragonfly-protocol client; NOT what rudis's own replica sends) ──
        REPLCONF capa dragonfly → master replies replid/"SYNC1"/num_shards/version/0
        one TCP connection PER SHARD: "DFLY FLOW <replid> <sync_id> <shard_id> [<lsn>]"
        run_shard_replication_flow: "+OK FLOW <shard_id>\r\n$<len>\r\n<this shard's RDB chunk>\r\n"
                                      then this shard's live mutation stream
```

### 2.1 `AofWriter` and `AofConfig` (`src/aof.rs`)

```rust
#[derive(Clone, Debug)]
pub struct AofConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub fsync_every_sec: bool,
}

pub struct AofWriter {
    buffer: Vec<u8>,
    spare_buffer: Option<Vec<u8>>,   // recycled buffer to avoid a fresh allocation every flush
    file: Option<std::rc::Rc<monoio::fs::File>>,
    path: PathBuf,
    offset: u64,
}
```

`AofWriter::open` creates the directory if needed, opens the file with `monoio::fs::OpenOptions::new().create(true).write(true)`, and reads the existing file's length via `metadata()` to initialize `offset` — re-opening an existing AOF on restart resumes appending at the correct byte position rather than truncating it. `take_flush_chunk` swaps the live `buffer` out for either a previously recycled buffer (`spare_buffer`) or a fresh 64KB-capacity `Vec`, returning the drained chunk plus the file handle and the byte offset it should be written at; `recycle_chunk` gives a drained, cleared buffer back for reuse as long as its capacity is at most 4MB (preventing one abnormally large write from permanently inflating the buffer pool). `flush()`/`sync()` call `take_flush_chunk` once and await the write (and, for `sync()`, an `fsync`) inline — used by explicit callers like `Router::save_rdb`'s `sync_aof()` step, not by the steady-state background cadence (§3.2, which lives in `server.rs`).

**There is no `appendfsync always`/`everysec`/`no` policy choice.** `AofConfig.fsync_every_sec` is a single boolean, and `main.rs` always constructs it as `true` — there is no `RudisConfig`/config-file directive that sets it otherwise. In practice this means AOF durability is currently hardcoded to a Redis `everysec`-like policy (background flush every 5ms, `fsync` roughly every 1 second); per-write synchronous `fsync` ("always", the strongest and slowest Redis option) and no periodic `fsync` at all ("no", the weakest and fastest) are not exposed as configuration today.

### 2.2 `ReplicationHub`, `ReplicationRole`, `ReplicationBacklog`, `ConnectedReplica`, `ShardReplicaFlow` (`src/replication.rs`)

```rust
pub enum ReplicationRole {
    Master { replid: String, replid2: String, second_offset: i64 },
    Slave {
        master_host: String,
        master_port: u16,
        link_status: String,          // "connecting" | "up" | "down"
        master_repl_offset: u64,
        master_replid: String,
        sync_in_progress: bool,
    },
}

pub struct ReplicationHub {
    pub port: u16,
    pub role: RwLock<ReplicationRole>,
    pub master_replid: String,
    pub master_repl_offset: AtomicU64,
    pub is_slave_atomic: AtomicBool,
    pub has_replicas: AtomicBool,
    pub backlog_active: AtomicBool,          // keeps the backlog live even with zero replicas
    pub backlog: RwLock<ReplicationBacklog>,
    pub replicas: RwLock<HashMap<u64, Arc<ConnectedReplica>>>,   // PSYNC sessions
    pub cancel_sync: RwLock<Option<flume::Sender<()>>>,
    pub shard_flows: RwLock<HashMap<usize, HashMap<u64, Arc<ShardReplicaFlow>>>>, // DFLY FLOW sessions, keyed by shard then client
    pub has_shard_flows: AtomicBool,
}

pub struct ReplicationBacklog {
    pub buffer: Vec<u8>,       // fixed size: max_size bytes, allocated once
    pub write_idx: usize,      // next write position, wraps modulo max_size
    pub len: usize,            // logical bytes currently retained (<= max_size)
    pub max_size: usize,       // 1MB, hardcoded in ReplicationHub::new
    pub first_byte_offset: u64,
}

pub struct ConnectedReplica {
    pub id: u64,
    pub sender: flume::Sender<Vec<u8>>,
    pub listening_port: AtomicU64,
    pub ack_offset: AtomicU64,
    pub last_ack_time: AtomicU64,
}

pub struct ShardReplicaFlow {
    pub client_id: u64,
    pub shard_id: usize,
    pub sender: flume::Sender<Vec<u8>>,
    pub lsn: AtomicU64,        // per-flow byte counter, incremented by every propagated write
    pub ack_lsn: AtomicU64,
}
```

`replid` (both the hub's own `master_replid` and the one stored per-role) is generated via `fxhash::hash64` over the port and the current timestamp in nanoseconds, formatted as a 40-hex-character string (`format!("{:016x}{:016x}{:08x}", h1, h2, port)`) — a real, practically-unique identifier, but not cryptographically derived the way Redis's own run-id generation is.

**`ReplicationBacklog` is a genuine fixed-capacity ring buffer.** `ReplicationBacklog::new(max_size)` preallocates `vec![0u8; max_size]` once. `append` writes into `buffer` at `write_idx`, wrapping the write across the end of the buffer with two `copy_from_slice` calls when it would overrun, advances `write_idx` modulo `max_size`, and caps `len` at `max_size`; `first_byte_offset` is recomputed from the current master offset and the retained length after every append, so it always reflects the offset of the oldest byte still in the buffer. `get_diff` reads backward from `write_idx` the same way, wrapping as needed. This corrects an earlier revision of this document, which described the backlog as a plain growable `Vec` with a linear `drain` on overflow rather than a true ring buffer; the current implementation is a real circular buffer with modular read/write cursors.

**`propagate` backlogs independently of whether any replica is currently connected.** The backlog-append half of `propagate` is gated on `backlog_active`, not on `has_replicas`; `has_connected_replicas` reports `true` whenever `backlog_active` is set, even with zero live sessions. This matters because partial resync needs history to exist *before* a replica reconnects — if the backlog only grew while something was actively connected, a replica that fully disconnected and came back would always find an empty/stale backlog and be forced into a full resync, defeating the purpose of partial resync.

**`propagate` reaches both PSYNC replicas and DFLY-FLOW sessions in one call.** Beyond backlog-append and the `ConnectedReplica` fan-out, `propagate` also walks every registered `ShardReplicaFlow` (across all shards) and sends the same bytes to each, incrementing that flow's `lsn` by the byte length sent. `propagate_shard(shard_id, bytes)` is the shard-scoped variant, used when the caller already knows which shard produced the mutation: it still appends to the shared backlog and fans out to `ConnectedReplica`s, but only sends to `ShardReplicaFlow`s registered for that specific `shard_id`.

---

## 3. Execution Algorithms & Code Logic

### 3.1 `command_to_resp` is the entire AOF-and-replication write path

```rust
pub fn command_to_resp(cmd: &Command) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    match cmd {
        Command::Set { key, value, expire_in, .. } => { /* re-serializes as SET key value [PX ms] */ Some(buf) }
        Command::Mset(pairs) => { /* MSET k1 v1 k2 v2 ... */ Some(buf) }
        Command::Del(keys) => { /* DEL k1 k2 ... */ Some(buf) }
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

This function is the *only* thing that decides what gets logged and replicated: any command not matched here falls through to `_ => None`, and `record_mutation` (§3.4) gates both AOF append and propagation on this same `Some`. A write command without an explicit arm is invisible to both persistence and replication — there is no generic/derived serialization fallback.

`replay_aof` is the mirror image on startup: it reads the entire AOF file into memory with one `std::fs::read`, then parses and replays every command through the exact same `execute_local_command` used for live traffic — there is no separate "restore format"; the AOF file is literal RESP commands. Since nothing compacts the file automatically, both replay time and file size grow with total lifetime write volume unless `BGREWRITEAOF` is run (§3.3), not with the current dataset size (unlike RDB restore, which is proportional to current key count — see `docs/rdbsave.md`).

### 3.2 The 5ms/~1s flush-and-fsync cadence is driven from `src/server.rs`, not from `aof.rs`

`aof.rs` itself has no background task; `AofWriter::take_flush_chunk` just hands ownership of the pending buffer to whoever calls it. The caller — a periodic task spawned in `run_shard_worker` (Component 01 §3.1) — calls this every 5ms, and if it returns `Some`, does `file.write_all_at(chunk, offset)` (a positioned write, so the shared `Rc<monoio::fs::File>` needs no seek state), recycles the returned buffer, and calls `sync_data()` (fsync) on the 200th tick out of every 200 (≈ once per second) when `fsync_every_sec` is set. This corrects an earlier revision of this document that described a 50ms/~20-tick cadence; the current code flushes ten times more often (5ms) and fsyncs on a longer tick count (200), landing on the same effective ~1-second fsync interval by a different path.

### 3.3 `BGREWRITEAOF` / AOF compaction (`rewrite_shard_aof`, `perform_rewrite_aof`)

`Router::bgrewriteaof` guards against concurrent saves/rewrites with a `compare_exchange` on `is_saving`, then spawns `perform_rewrite_aof` as a background task (`Router::save_rdb`'s synchronous counterpart, `bgsave`, uses the same guard). `perform_rewrite_aof`:

1. `sync_aof()`s the local shard first, so no buffered-but-unflushed write is lost.
2. Calls `rewrite_shard_aof` on the local `ShardDb`: iterates every live (non-expired) table entry, and for each one writes a canonical RESP command that reconstructs it — `SET ... [PX <remaining-ms>]` for strings/ints, `HSET` for hashes, `RPUSH` for lists, `SADD` for sets, `ZADD` for sorted sets, one `SET` per HyperLogLog register blob, one `XADD` per stream entry — followed by a `PEXPIRE` for any non-string key that has a TTL (strings encode their own `PX` inline). JSON documents are re-emitted as `JSON.SET key $ <json>`. All of this is written through a 64KB `std::io::BufWriter` directly to a per-attempt temp file (`appendonly-<shard>.aof.tmp.<pid>_<counter>`), so at most 64KB of the rewritten output is buffered at any instant regardless of dataset size.
3. `fsync`s the temp file, atomically `rename`s it over the live `appendonly-<shard>.aof`, and `fsync`s the containing directory (`sync_parent_dir`) so the rename survives a crash before the directory entry is durable.
4. Calls `AofWriter::reopen_after_rewrite`, which re-opens the (now-compacted) file, resets `offset` to its new length, and — if any writes buffered in the live `AofWriter` between step 1 and this point — flushes them to the newly reopened file at the correct offset, so no write in flight during the rewrite is dropped.
5. Sends `ShardMessage::RewriteAof` to every other shard in turn (sequentially, not concurrently — "remote shards rewritten sequentially one-by-one to eliminate concurrent multi-shard I/O and memory spikes", per the source comment) and sums up the rewritten-key counts.

Note that `RudisValue::Tiered` entries (keys currently offloaded to NVMe tiered storage, Component 07) are skipped by `serialize_val_payload`/serialization inside `rewrite_shard_aof`'s value matching in the same way they are in RDB serialization — see the RDB coverage note in `docs/rdbsave.md`.

### 3.4 `record_mutation`, `propagate`, `propagate_bytes`

```rust
pub fn record_mutation(
    port: u16,
    aof: Option<&std::cell::RefCell<crate::aof::AofWriter>>,
    cmd: &crate::resp::Command,
) {
    if let Some(bytes) = crate::aof::command_to_resp(cmd) {
        if let Some(aof) = aof {
            aof.borrow_mut().append(&bytes);
        }
        propagate_bytes(port, &bytes);
    }
}
```

`propagate_bytes`/`propagate_shard_bytes` short-circuit on a process-wide `HAS_ACTIVE_REPLICATION: AtomicBool` before even looking up the hub, so the cost of this call on a node with replication never activated is a single relaxed atomic load. `Router`'s per-op local-execution branches (`del`/`set`/`incr_by`/`expire`/`persist`/...) call `command_to_resp` directly and append to `self.aof` inline in the same way; `record_mutation` is the shared helper for call sites that want both effects from one call.

### 3.5 Partial resync: master side (`try_partial_resync`) and replica side (`run_replica_worker`)

Master side, `run_master_replica_stream` (`src/connection.rs`, entered when a connection sends `PSYNC`):

```rust
let (req_replid, req_offset) = match &psync_cmd {
    Command::Psync { replid, offset } => (std::str::from_utf8(replid).unwrap_or(""), *offset),
    _ => ("", -1),
};

let partial = hub.try_partial_resync(client_id, write_tx.clone(), req_replid, req_offset);
if let Some((replid, diff, _repl)) = partial {
    // "+CONTINUE <replid>\r\n" + diff bytes
} else {
    let rdb = router.generate_full_rdb().await;
    // "+FULLRESYNC <replid> <offset>\r\n$<len>\r\n" + rdb bytes
}
```

`ReplicationHub::try_partial_resync` checks the requested replid against the master's own current replid (or its previous `replid2`, for the "I was just promoted" case, gated by `second_offset >= 0` and `req_offset <= second_offset`), then asks `ReplicationBacklog::can_partial_sync`/`get_diff` whether `target_offset = req_offset + 1` still falls inside the retained ring-buffer window. If it does, it returns the exact missing byte slice and registers the connection as a `ConnectedReplica` so it receives every future mutation live; if the offset predates what the backlog retained (or the replid does not match), it returns `None`, and the caller falls back to `+FULLRESYNC`. This is covered by unit tests (`test_backlog_append_and_diff`, `test_try_partial_resync`) exercising the boundary cases (exactly up to date, mid-backlog, evicted-by-overflow, offset beyond the master's own).

Replica side, `run_replica_worker` (`src/replication.rs`) — a hand-rolled RESP handshake inside a `'reconnect_loop`, using a `send_and_expect_line!` macro (write a command, read until `\r\n`), not the shared `Router`/connection machinery:

```rust
// 1. PING → expect +PONG
// 2. REPLCONF listening-port <port> → expect +OK
// 3. REPLCONF capa psync2 → expect +OK
// 4. PSYNC <cached_replid> <cached_offset>  — if this worker has a cached replid from a
//    previous successful sync against this same master
//    PSYNC ? -1                              — otherwise (first connection to this master)
//    → expect +FULLRESYNC <replid> <offset>  OR  +CONTINUE <replid>
// 5. (only if +FULLRESYNC) read "$<rdb_len>\r\n", then exactly rdb_len more bytes,
//    then router.restore_rdb_bytes(rdb_bytes).await
// 6. mark link_status "up"; master_repl_offset = initial_offset (cached offset for
//    +CONTINUE, the offset the FULLRESYNC reply carried otherwise)
// 7. loop: parse_command on the ongoing stream; REPLCONF GETACK → reply REPLCONF ACK <offset>;
//    PING is a no-op keepalive; everything else → router.execute_replica_command(cmd).await
```

`master_replid`/`master_repl_offset` are cached in the hub's `ReplicationRole::Slave` state across reconnects (keyed to the same `master_host`/`master_port` — a change of target resets the cache to empty/0). This means the replica worker's own reconnect attempts genuinely exercise the `+CONTINUE` path on the master when the disconnect was brief enough for the offset to still be inside the master's 1MB backlog. This corrects an earlier revision of this document, which stated that `run_replica_worker` unconditionally sent the literal bytes for `PSYNC ? -1` on every call, including reconnects, and that rudis's own replica could therefore never trigger `+CONTINUE` against another rudis node. That gap has been closed; `PSYNC ? -1` is now used only on a worker's first connection to a given master (or after a full cache miss).

### 3.6 The Dragonfly-compatible `DFLY FLOW` path (master side only)

A client that sends `REPLCONF capa dragonfly` (instead of, or in addition to, `capa psync2`) receives a Dragonfly-shaped greeting instead of a plain `+OK`:

```rust
// on REPLCONF capa dragonfly:
"*5\r\n${replid.len()}\r\n{replid}\r\n$5\r\nSYNC1\r\n:{num_shards}\r\n:1\r\n:0\r\n"
```

`"SYNC1"` here is a **hardcoded literal string**, not a freshly generated per-session token — every greeting on a given master returns the same `"SYNC1"` regardless of how many flow sessions or replicas have connected before it. The trailing `:0` is a plain integer, not a 40-character lineage-id bulk string. (See `docs/replication.md` for the operator-facing description of this handshake and its accurate wire format.)

The client is then expected to open **one TCP connection per shard** and, on each, send `DFLY FLOW <master_replid> <sync_id> <shard_id> [<lsn>]`. `src/connection.rs`'s per-connection loop detects a parsed `Command::DflyFlow { shard_id, lsn, .. }` the same way it detects `PSYNC`, and hands the connection to `run_shard_replication_flow(stream, client_id, router, shard_id, lsn)`:

```rust
let _flow = hub.register_shard_flow(shard_id, client_id, write_tx.clone());
let chunk = /* this shard's save_rdb_chunk, fetched locally or via ShardMessage::SaveRdbChunk
               if shard_id != router.shard_id */;
// "+OK FLOW {shard_id}\r\n${chunk.len()}\r\n" + chunk + "\r\n"
// then: a writer task drains write_rx (fed by propagate/propagate_shard) onto the socket,
// and a reader loop watches only for "REPLCONF ACK <offset>" to update ack_lsn.
```

`_lsn` (the requested resume point) is accepted on the wire but not currently used to serve a partial per-flow resync — every `DFLY FLOW` session receives this shard's full RDB chunk regardless of what `lsn` it presents. **`run_replica_worker` (rudis's own replica implementation) never sends `REPLCONF capa dragonfly` or `DFLY FLOW`** — this entire path is reachable only by an external client that speaks the Dragonfly wire protocol itself; it is not how two rudis nodes replicate with each other today.

### 3.7 `INFO replication` / `ROLE` are computed fresh on every call

`format_info_replication` and `format_role_resp` both take a fresh read-lock on `role` and load the relevant atomics (`master_repl_offset`, per-replica `ack_offset`/`listening_port`, backlog `max_size`/`first_byte_offset`/`len`) on every call — there is no periodic snapshot; the command handler formats whatever the live state is at request time. `DFLY FLOW` sessions (`ShardReplicaFlow`) are not currently reflected in `INFO replication`'s `connected_slaves` count or `ROLE`'s replica list — only `ConnectedReplica` (PSYNC) sessions are.

---

## 4. Cross-Component Interactions

- **`src/router.rs`** (Component 04): the per-op local-execution branches call `crate::aof::command_to_resp` directly and append to `self.aof` inline; `Router::save_rdb`/`bgsave`/`bgrewriteaof`/`perform_rewrite_aof`/`generate_full_rdb`/`restore_rdb_bytes` drive the RDB side of a full resync or a manual snapshot, delegating to `crate::table::save_rdb_chunk`/`load_rdb_bytes` per shard and stitching per-shard chunks into one blob framed with a `"REDIS0011"` header and a trailing CRC64 footer — see `docs/rdbsave.md` for the full format.
- **`src/connection.rs`** (Component 02): owns `run_master_replica_stream` (PSYNC, §3.5) and `run_shard_replication_flow` (DFLY FLOW, §3.6); calls `crate::replication::record_mutation` after executing local write commands so both AOF and live replicas/flows observe them.
- **`src/server.rs`** (Component 01): spawns the 5ms/~1s AOF flush-and-fsync task and performs AOF replay on startup (§3.2); also performs the final flush+fsync on shutdown.
- **`src/table.rs`** (Component 05): supplies the actual RDB chunk serialization/deserialization (`save_rdb_chunk`, `load_rdb`, `load_rdb_bytes`) that both startup restore and full-resync RDB transfer are built on — this file only stitches those chunks together and moves the bytes over the wire.

---

## 5. Future Improvements

- **RESOLVED — AOF rewrite and compaction via `BGREWRITEAOF` (§3.3).** `rewrite_shard_aof` snapshots live state across shards with TTL preservation into an atomic temporary file (now also `fsync`-ing the containing directory), and `AofWriter::reopen_after_rewrite` live-reopens the compacted file so subsequent write traffic continues uninterrupted.
- **RESOLVED — partial resynchronization on both the master and the replica side (§3.5).** The master serves `+CONTINUE` with just the missing backlog bytes when a valid replid+offset is presented, `ReplicationBacklog` is now a genuine ring buffer, and `run_replica_worker` tracks its cached `master_replid`/`master_repl_offset` across reconnects and requests a partial resync itself rather than always requesting a full one.
- **Low — `DFLY FLOW`'s `lsn` parameter is accepted but unused for resuming a partial per-flow sync** (§3.6); every flow session currently receives a fresh full RDB chunk for its shard.
- **Low — `DFLY FLOW` sessions are not reflected in `INFO replication`/`ROLE`** (§3.7), only `PSYNC` `ConnectedReplica` sessions are counted there.
- **Low — derive `replid` from something closer to Redis's real run-id generation scheme**, or at least keep this note current: the `fxhash`-over-port-and-timestamp approach (§2.2) is a real, working, but not cryptographically-derived identifier.
- **Low — make the 1MB `ReplicationBacklog` size and the AOF flush/fsync cadence configurable** rather than compile-time constants, and expose an `appendfsync`-style policy choice (`always`/`everysec`/`no`) instead of the current hardcoded always-`true` `fsync_every_sec` (§2.1).

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: `BGSAVE`/full resync stream shard RDB chunks sequentially, one shard at a time, to keep peak memory bounded — do not change this to a concurrent fan-out without re-checking the memory-bound rationale.
* **Gotcha 2**: `BGREWRITEAOF` uses a 64KB `BufWriter` to stream the rewritten AOF file without allocating a large heap buffer for the whole dataset — keep new value types' rewrite logic writing through that same buffered writer rather than building an intermediate `Vec`.
* **Gotcha 3**: A write command is invisible to both AOF and replication unless it has an explicit arm in `command_to_resp` (§3.1) — adding a new mutating command requires adding that arm, or it will silently not persist or replicate.
* **Gotcha 4**: `DFLY FLOW` is a master-side-only capability today; do not assume rudis-to-rudis replication ever uses it — `run_replica_worker` only speaks `PSYNC`.
* **Gotcha 5**: `ReplicationBacklog`'s 1MB size is a compile-time constant (`ReplicationHub::new`); a write burst larger than that between a replica's disconnect and reconnect will force a full resync even though the partial-resync machinery is otherwise working correctly.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
