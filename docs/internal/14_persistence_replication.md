# Component 14: Persistence & Replication Engines (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/replication.rs` (1,404 lines), `src/aof.rs` (3,066 lines — roughly doubled since the last
> revision of this document, almost entirely inside `command_to_resp` and `rewrite_shard_aof`), plus the RDB
> save/load machinery in `src/router.rs`, `src/shard.rs`, `src/table.rs`, and the PSYNC/DFLY-FLOW wire handlers in
> `src/connection.rs`.
> **High-Level Design Spec**: [`docs/design/14_persistence_replication.md`](../design/14_persistence_replication.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

Every claim below was re-verified directly against the current source (2026-09-30) rather than carried forward
from a prior revision of this document. Several sections correct or update earlier findings — each correction is
called out explicitly where it occurs.

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/replication.rs` | Replication hub, ring-buffer backlog, PSYNC/`SYNC`/DFLY-FLOW state, `WAIT`/`WAITAOF` polling, replica worker | `ReplicationHub`, `ReplicationBacklog`, `ConnectedReplica`, `ShardReplicaFlow`, `run_replica_worker`, `record_mutation`, `wait_replicas` |
| `src/aof.rs` | AOF writer, RESP re-encoding (`command_to_resp`), replay, rewrite/compaction | `AofWriter`, `AofConfig`, `command_to_resp`, `replay_aof`, `rewrite_shard_aof` |
| `src/router.rs` | RDB save/load orchestration across shards, `is_saving` guard, atomic-rename write path | `Router::save_rdb`/`bgsave`/`bgrewriteaof`/`perform_save_rdb`/`generate_full_rdb`/`restore_rdb_bytes` |
| `src/shard.rs` | Per-shard RDB chunk serialization (base table **and** all AI-native/search/probabilistic side-stores) | `ShardDb::save_rdb_chunk`, `save_extended_rdb_chunk`, `restore_ai_native_rdb_record` |
| `src/table.rs` | Per-key value payload codec (`serialize_val_payload`/`deserialize_val_payload`), the single production RDB-blob parser (`load_rdb_bytes`), CRC64 | `RudisValue`, `crc64`/`crc64_update`, `load_rdb`/`load_rdb_bytes` |
| `src/connection.rs` | Master-side PSYNC/`SYNC`/DFLY-FLOW stream handlers, `WAIT`/`WAITAOF` command dispatch, the `record_change!` macro that gates AOF+replication for every mutating command handler | `run_master_replica_stream`, `run_shard_replication_flow`, `record_change!` |

---

## 2. Data Flow Overview

```
                         Write Command (e.g. SET k v)
                                      │
                                      ▼
                    Local ShardDb / RudisTable mutation (instant, in-thread)
                                      │
                                      ▼
        record_change!(cmd) macro (connection.rs) / record_mutation() [replication.rs]
                                      │
                    command_to_resp(cmd) → RESP bytes        [aof.rs]
                         │                        │
                    Some(bytes)                  None  ──────────────►  NOT written to AOF,
                         │                                               NOT propagated to replicas
                         ▼                        ▼                      (see §3.8 — this currently
              AofWriter::append (if Some)   ReplicationHub::propagate    affects the entire JSON.*,
                         │                    (per port/shard)           BF./CF./CMS./TOPK.*, and
              5ms flush task in server.rs          │                     CRDT.* command families)
              (write_all_at + ~1s fsync)            ├─► backlog.append (1MB ring buffer)
                                                     ├─► every ConnectedReplica.sender.send (PSYNC path)
                                                     └─► every ShardReplicaFlow.sender.send  (DFLY FLOW path)

        ── PSYNC / SYNC reconnect (rudis replica, or any Redis/Valkey-protocol client) ──
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
        (old-style `SYNC` — no PSYNC negotiation — is also accepted: master sends
         "$<len>\r\n<rdb>" directly, followed by a literal "SELECT 0" command, no
         FULLRESYNC preamble)

        ── DFLY FLOW (Dragonfly-protocol client; NOT what rudis's own replica sends) ──
        REPLCONF capa dragonfly → master replies replid/"SYNC1"/num_shards/version/0
        one TCP connection PER SHARD: "DFLY FLOW <replid> <sync_id> <shard_id> [<lsn>]"
        run_shard_replication_flow: "+OK FLOW <shard_id>\r\n$<len>\r\n<this shard's RDB chunk>\r\n"
                                      then this shard's live mutation stream
```

### 2.1 `AofWriter` and `AofConfig` (`src/aof.rs:7-164`)

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

This struct is byte-for-byte unchanged from the prior revision of this document. `AofWriter::open` (line 33)
creates the directory if needed, opens the file with `monoio::fs::OpenOptions::new().create(true).write(true)`,
and reads the existing file's length via `metadata()` to initialize `offset` — re-opening an existing AOF on
restart resumes appending at the correct byte position rather than truncating it. Both `buffer` and any freshly
allocated `spare_buffer` start at 64KB capacity (`Vec::with_capacity(65536)`).

`take_flush_chunk` (line 86) swaps the live `buffer` out for either a previously recycled buffer (`spare_buffer`)
or a fresh 64KB-capacity `Vec`, returning the drained chunk plus the file handle and the byte offset it should be
written at; `recycle_chunk` (line 78) gives a drained, cleared buffer back for reuse as long as its capacity is at
most 4MB (preventing one abnormally large write from permanently inflating the buffer pool). `flush()`/`sync()`
call `take_flush_chunk` once and await the write (and, for `sync()`, an `fsync`) inline. `flush_rc` (line 114) is
a `Rc<RefCell<Self>>`-taking convenience wrapper around the same logic — it exists in the current codebase but,
per a full-repo grep, its only callers today are two of `aof.rs`'s own unit tests; the production 5ms flush task
in `server.rs` (§3.2) calls `take_flush_chunk` directly rather than through `flush_rc`.

**`appendfsync everysec`/`no` are supported; `always` is rejected.** The config directive sets
`RudisConfig.appendfsync_every_sec`, which seeds `AofConfig.fsync_every_sec`; the flusher reads a per-port
live flag (`aof::fsync_every_sec_flag`) so `CONFIG SET appendfsync` applies at runtime. `always` fails config
loading and `CONFIG SET` because the background flusher can't fsync before replying. AOF
durability is therefore still a fixed Redis `everysec`-like policy (background flush every 5ms, `fsync` roughly
every 1 second, §3.2); per-write synchronous `fsync` ("always") and no periodic `fsync` ("no") remain unreachable
through configuration.

### 2.2 `ReplicationHub`, `ReplicationRole`, `ReplicationBacklog`, `ConnectedReplica`, `ShardReplicaFlow` (`src/replication.rs:5-139`)

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
    pub replicas: RwLock<HashMap<u64, Arc<ConnectedReplica>>>,   // PSYNC/SYNC sessions
    pub cancel_sync: RwLock<Option<flume::Sender<()>>>,
    pub shard_flows: RwLock<HashMap<usize, HashMap<u64, Arc<ShardReplicaFlow>>>>, // DFLY FLOW sessions
    pub has_shard_flows: AtomicBool,
}

pub struct ReplicationBacklog {
    pub buffer: Vec<u8>,       // fixed size: max_size bytes, allocated once
    pub write_idx: usize,      // next write position, wraps modulo max_size
    pub len: usize,            // logical bytes currently retained (<= max_size)
    pub max_size: usize,       // 1MB, hardcoded in ReplicationHub::new (line 164)
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

All five struct definitions and the 1MB (`1024 * 1024`) backlog constant (`ReplicationHub::new`, line 164) are
unchanged from the prior revision of this document — `replication.rs` grew by only ~46 lines overall (1,358 →
1,404), and every field above was re-diffed against the current file.

`replid` (both the hub's own `master_replid` and the one stored per-role) is still generated via
`fxhash::hash64` over the port and the current timestamp in nanoseconds, formatted as a 40-hex-character string
(`format!("{:016x}{:016x}{:08x}", h1, h2, port)`, `ReplicationHub::new`, lines 143-150) — a real, practically-unique
identifier, but not cryptographically derived the way Redis's own run-id generation is.

**`ReplicationBacklog` is still a genuine fixed-capacity ring buffer** (`append`/`get_diff`, lines 67-123):
`ReplicationBacklog::new(max_size)` preallocates `vec![0u8; max_size]` once; `append` writes at `write_idx` with
wraparound via two `copy_from_slice` calls when needed, and `first_byte_offset` is recomputed from the current
master offset and retained length after every append.

**`propagate` still backlogs independently of whether any replica is currently connected** (`backlog_active` vs.
`has_replicas`, lines 394-454) — this is what makes partial resync work for a replica that fully disconnected and
later reconnects within the 1MB window.

**`propagate` still reaches both PSYNC/`SYNC` replicas and DFLY-FLOW sessions in one call**; `propagate_shard`
(the shard-scoped variant used when the caller knows which shard produced the mutation) only fans out to
`ShardReplicaFlow`s registered for that specific `shard_id`, but still appends to the one shared backlog and still
fans out to every `ConnectedReplica` regardless of shard (lines 456-512) — replicas receive the interleaved,
globally-ordered mutation stream from every shard on one PSYNC connection; DFLY-FLOW clients receive one
shard-scoped stream per TCP connection.

---

## 3. Execution Algorithms & Code Logic

### 3.1 `command_to_resp` is still the entire AOF-and-replication write-path gate — and it is now an ~1,770-line, 81-command match statement (`src/aof.rs:166-1937`)

```rust
pub fn command_to_resp(cmd: &Command) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    match cmd {
        Command::Set { .. } => { /* ... */ Some(buf) }
        // ... 80 more explicit arms ...
        _ => None,
    }
}
```

This function alone accounts for almost all of `aof.rs`'s growth from ~1,482 to 3,066 lines. A full enumeration
of every `Command::` arm currently present (81 distinct variants, via `rg -o 'Command::[A-Za-z0-9_]+' | sort -u`)
shows the set has grown well beyond the string/hash/list/set/zset/stream/bitops core covered by the previous
revision of this document, to include:

- **Hash field TTL (Redis 7.4/Valkey 8)**: `Hexpire` (→ `HPEXPIRE`/`HPEXPIREAT`), `Hpersist`, `Hgetex`, `Hsetex`.
- **Vector sets (Redis 8 `VADD`/`VREM`/`VSETATTR`)**: `Vadd` (re-encoded with `Q8`/`BIN`/`M`/`EF`/`ATTR`/`CAS`/
  `SETATTR`/`PQ`/`TIERED` option flags preserved), `Vdel` (→ `VREM`), `Vsetattr`.
- **Semantic cache (`SEMANTIC.*`)**: `SemanticSet`, `SemanticDel`, `SemanticFlush`.
- **Agent runtime (`AGENT.*`)**: `AgentMemAdd`, `AgentMemClear`, `AgentMemCompact`, `AgentCheckpointPut`,
  `AgentToolClaim`, `AgentToolComplete`.
- **Streams, extended**: `Xclaim`, `Xautoclaim`, `Xnack`, `Xidmprecord`, `XgroupSetId` (in addition to the
  previously-covered `Xadd`/`Xdel`/`Xtrim`/`XgroupCreate`/`XgroupDestroy`/`Xack`).
- **Misc newer commands**: `Copy`, `Unlink`, `Bitfield` (readonly bitfields are explicitly excluded — `if
  *readonly { return None; }` — but a mixed GET+SET/INCRBY bitfield is fully re-encoded including an `OVERFLOW`
  directive only when it changes between ops), `Msetex`, `Zrangestore`.

This function is still the *only* thing that decides what gets logged and replicated: any command not matched
here falls through to `_ => None` (line 1936), and both `record_mutation` (§3.4, used by `Router`'s own local
write paths) and the `record_change!` macro (§3.8, used by essentially every command handler in `connection.rs`)
gate both AOF append and propagation on this same `Some`.

**Newly-confirmed correctness gap — `Command::Spop` re-encodes the *input* `count`, not the popped members, but
this is safe.** `RudisSet::pop` (`src/table.rs:880-892`) is deterministic given identical set contents and
history: the `Small` variant pops from the tail of a `Vec`, and the `Full` variant removes
`s.iter().next()` from a `hashbrown::HashSet<Bytes, FxBuildHasher>` — `FxHash` is *not* randomized per process
(unlike `std::collections::HashSet`'s default `SipHash`), so two processes that replayed the exact same mutation
history land on the exact same set contents and the exact same iteration order. Replaying literal `SPOP key
[count]` on a replica or during AOF replay therefore reproduces the same result as the master. This was checked
specifically because it is the classic Redis pitfall (real Redis rewrites `SPOP` into `SREM` of the actual
popped members for exactly this reason) — rudis gets away with the simpler re-encoding only because of the
FxHash choice, and this is a fragile invariant a future `RudisSet` backend change could silently break.

**Newly-found correctness gap — `Command::Xautoclaim` is genuinely non-deterministic when replicated/persisted
as itself.** Unlike `Spop`, `XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]`
(`src/aof.rs:1533-1562`) is re-encoded with the *original filter parameters* (`min_idle_time`, `start` cursor,
`count`) rather than the resolved list of claimed message IDs it actually produced on the master. Because
`min_idle_time` is evaluated against wall-clock "now" at the moment of execution, and AOF replay / PSYNC replica
application happens at a different wall-clock time than the original master execution, a replica or a replayed
AOF can legitimately compute a *different* idle-time-based claim set (different messages claimed, different next
cursor) than what the master actually claimed. Real Redis avoids this specific pitfall by propagating `XAUTOCLAIM`
as one or more explicit `XCLAIM` commands carrying the resolved IDs; rudis does not do this today. `Xclaim`
itself (explicit ID list) is unaffected — it is fully deterministic.

`replay_aof` (`src/aof.rs:1940-1963`) is still the mirror image on startup: it reads the entire AOF file into
memory with one `std::fs::read`, then parses and replays every command through the exact same
`execute_local_command` used for live traffic — there is no separate "restore format"; the AOF file is literal
RESP commands.

### 3.2 The 5ms/~1s flush-and-fsync cadence is still driven from `src/server.rs`, not from `aof.rs`

`server.rs:174-193` is unchanged: a periodic task spawned in `run_shard_worker` calls `take_flush_chunk` every
5ms via `monoio::time::sleep(Duration::from_millis(5))`, and if it returns `Some`, does `file.write_all_at(chunk,
offset)`, recycles the returned buffer via `recycle_chunk`, and calls `sync_data()` (fsync) on the 200th tick out
of every 200 (`ticker.is_multiple_of(200)`) — ≈ once per second — only when `fsync_every_sec` is set (which, per
§2.1, is always `true` in practice today).

### 3.3 `BGREWRITEAOF` / AOF compaction (`rewrite_shard_aof`, `perform_rewrite_aof`) — the `RudisValue::Tiered` zero-byte bug is now fixed at this layer

`Router::bgrewriteaof` (`router.rs:3051-3064`) guards against concurrent saves/rewrites with a
`compare_exchange` on `is_saving` (the same flag `bgsave`/`save_rdb` use — RDB save and AOF rewrite cannot run
concurrently on one shard's router), then spawns `perform_rewrite_aof` (`router.rs:3066-3097`) as a background
task. `perform_rewrite_aof`:

1. `sync_aof()`s the local shard first, so no buffered-but-unflushed write is lost.
2. Calls `rewrite_shard_aof` (`aof.rs:1965-2570`, ~605 lines) on the local `ShardDb`. It iterates every live
   (non-expired) table entry and, for each one, writes a canonical RESP command that reconstructs it —
   `SET ... [PX <remaining-ms>]` for strings/ints, `HSET` for hashes, `RPUSH` for lists, `SADD` for sets, `ZADD`
   for sorted sets, one `SET` per HyperLogLog register blob, one `XADD` per stream entry — followed by a
   `PEXPIRE` for any non-string key with a TTL. All of this is written through a 64KB
   `std::io::BufWriter` directly to a per-attempt temp file (`appendonly-<shard>.aof.tmp.<pid>_<counter>`), so at
   most 64KB of the rewritten output is buffered at any instant regardless of dataset size.
   - **`RudisValue::Tiered` entries are now hydrated, not skipped** (lines 2004-2016): for each `Tiered(ptr)`
     entry, `rewrite_shard_aof` calls `tier_manager.read_ptr_sync(*ptr)` synchronously, re-materializes the value
     as `RudisValue::String(Bytes::from(raw))`, and writes a normal `SET` line for it. If the tier read fails, the
     key is silently dropped from the rewritten AOF (`continue`) rather than writing a placeholder — a live
     dataset with an NVMe-tier read failure during rewrite loses that key from the AOF, but this is a narrow edge
     case (a read failure against local NVMe), not the systemic zero-byte bug this corrects.
   - It also now covers strictly more value families than a prior revision of this document recorded:
     JSON documents (re-emitted as `JSON.SET key $ <json>`, via `db.json_store.iter()`), vector-set index entries
     (`db.vector_indexes`), semantic caches (`db.semantic_caches`), agent memory sessions (`db.agent_memories`),
     agent checkpoint threads (`db.agent_checkpoints`), and agent tool-call registries (`db.agent_tools`) are all
     snapshotted directly from live `ShardDb` state into the rewritten file, each via its own hand-written
     serialization block, **not** by going through `command_to_resp`.
   - **Newly-found gap — probabilistic structures and CRDT state are never rewritten (and never AOF-logged at
     all).** `rewrite_shard_aof` has no code path that touches `db.probabilistic_store` (Bloom filters, Cuckoo
     filters, Count-Min Sketches, Top-K trackers) or `db.crdt_store`. This is consistent with §3.8 below: these
     command families never had AOF-live-append support in the first place (`command_to_resp` has no arm for any
     `BF.*`/`CF.*`/`CMS.*`/`TOPK.*`/`CRDT.*` command), so there is nothing for rewrite to compact — but it also
     means **these data types have zero AOF-based durability**, full stop; they persist only via RDB (§3.9, tags
     8/10/11/12/13).
3. `fsync`s the temp file, atomically `rename`s it over the live `appendonly-<shard>.aof`, and `fsync`s the
   containing directory (`sync_parent_dir`, `aof.rs:2571-2583`) so the rename survives a crash before the
   directory entry is durable.
4. Calls `AofWriter::reopen_after_rewrite`, which re-opens the (now-compacted) file, resets `offset` to its new
   length, and flushes any writes buffered in the live `AofWriter` between step 1 and this point at the correct
   offset, so no write in flight during the rewrite is dropped.
5. Sends `ShardMessage::RewriteAof` to every other shard in turn (sequentially — "remote shards rewritten
   sequentially one-by-one to eliminate concurrent multi-shard I/O and memory spikes", per the source comment at
   `router.rs:3077`) and sums up the rewritten-key counts.

### 3.4 `record_mutation`, `propagate`, `propagate_bytes` (`replication.rs:687-745`)

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

`propagate_bytes`/`propagate_shard_bytes` still short-circuit on the process-wide `HAS_ACTIVE_REPLICATION:
AtomicBool` before even looking up the hub, so the cost of this call on a node with replication never activated
is a single relaxed atomic load. `record_mutation` is the shared helper used by `Router`'s own per-op local
branches; the far more common call site today is the `record_change!` macro in `connection.rs` (§3.8), which
almost every mutating command handler invokes and which inlines the identical `command_to_resp` → AOF-append →
propagate sequence directly (`connection.rs:12740-12761`).

### 3.5 Partial resync: master side (`try_partial_resync`) and replica side (`run_replica_worker`) — unchanged behavior, with one new detail (50ms reconnect backoff)

Master side, `run_master_replica_stream` (`src/connection.rs:2597-2695`, entered when a connection sends `PSYNC`
or the legacy `SYNC`):

```rust
let is_sync = matches!(&psync_cmd, Command::Sync);
if !is_sync {
    let partial = hub.try_partial_resync(client_id, write_tx.clone(), req_replid, req_offset);
    if let Some((replid, diff, _repl)) = partial {
        // "+CONTINUE <replid>\r\n" + diff bytes
    } else {
        let rdb = router.generate_full_rdb().await;
        // "+FULLRESYNC <replid> <offset>\r\n$<len>\r\n" + rdb bytes
    }
} else {
    // legacy SYNC: no negotiation at all.
    let rdb = router.generate_full_rdb().await;
    // "$<len>\r\n" + rdb bytes + "*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"
}
```

The plain `SYNC` command (old-style Redis replication, no replid/offset negotiation) is accepted alongside
`PSYNC`: it always performs a full resync and injects a literal `SELECT 0` immediately after the RDB bytes so a
generic Redis-protocol client that expects the classic `SYNC` framing gets a syntactically valid stream.

`ReplicationHub::try_partial_resync` (`replication.rs:314-392`) is otherwise unchanged from the prior revision:
it checks the requested replid against the master's own current replid (or its previous `replid2`, gated by
`second_offset >= 0` and `req_offset <= second_offset`, for the "I was just promoted" case), then asks
`ReplicationBacklog::can_partial_sync`/`get_diff` whether `target_offset = req_offset + 1` still falls inside the
retained 1MB ring-buffer window.

Replica side, `run_replica_worker` (`src/replication.rs:802-1175`) — a hand-rolled RESP handshake inside a
`'reconnect_loop`, using a `send_and_expect_line!` macro, not the shared `Router`/connection machinery — performs
PING → REPLCONF listening-port → REPLCONF capa psync2 → PSYNC (cached replid/offset if available, else `? -1`) →
(FULLRESYNC: read `$<len>\r\n` + that many RDB bytes + `router.restore_rdb_bytes`, or CONTINUE: nothing further to
read before the live stream) → mark link "up" → stream loop (`REPLCONF GETACK` → reply with `REPLCONF ACK
<offset>`; `PING` is a keepalive no-op; everything else → `router.execute_replica_command`). Every failure branch
(connect failure, a handshake line that doesn't match the expected prefix, a stream read returning `Ok(0)`/`Err`)
sets `link_status = "down"` and retries the whole `'reconnect_loop` after a **50ms** sleep
(`monoio::time::sleep(Duration::from_millis(50))`), unless `cancel_rx` has fired, in which case the worker exits
for good. `master_replid`/`master_repl_offset` remain cached in the hub's `ReplicationRole::Slave` state across
reconnects (keyed to the same `master_host`/`master_port`), so reconnect attempts against the same master
genuinely exercise `+CONTINUE` when the disconnect was brief enough to stay inside the 1MB backlog window.

### 3.6 The Dragonfly-compatible `DFLY FLOW` path (master side only) — unchanged

A client sending `REPLCONF capa dragonfly` still receives, verbatim:

```rust
// connection.rs:6080-6092
"*5\r\n${replid.len()}\r\n{replid}\r\n$5\r\nSYNC1\r\n:{num_shards}\r\n:1\r\n:0\r\n"
```

`"SYNC1"` is still a **hardcoded literal string**, not a freshly generated per-session token, and the trailing
`:0` is still a plain integer rather than a 40-character lineage-id bulk string. The client is expected to open
one TCP connection per shard and send `DFLY FLOW <master_replid> <sync_id> <shard_id> [<lsn>]`;
`run_shard_replication_flow` (`connection.rs:2697-2776`) fetches that shard's RDB chunk (locally, or via
`ShardMessage::SaveRdbChunk` if the shard isn't local), replies `+OK FLOW {shard_id}\r\n${len}\r\n<chunk>\r\n`,
then spawns a writer task draining `ShardReplicaFlow`'s `write_rx` onto the socket while a reader loop watches
only for `REPLCONF ACK <offset>` to update `ack_lsn`. `_lsn` (the requested resume point) is still accepted on
the wire but not used — the parameter is literally named `_lsn` in the function signature — every `DFLY FLOW`
session still receives this shard's full RDB chunk regardless of what `lsn` it presents. `run_replica_worker`
(rudis's own replica implementation) still never sends `REPLCONF capa dragonfly` or `DFLY FLOW`; this path is
reachable only by an external Dragonfly-protocol client.

### 3.7 `INFO replication` / `ROLE` are still computed fresh on every call, and `DFLY FLOW` sessions still aren't reflected in either

`format_info_replication` and `format_role_resp` (`replication.rs:514-635`) both take a fresh read-lock on `role`
and load the relevant atomics on every call. Both still only enumerate `self.replicas` (PSYNC/`SYNC` sessions);
neither iterates `self.shard_flows`, so `DFLY FLOW` clients are invisible to `connected_slaves`/`ROLE`'s replica
list.

### 3.8 `WAIT` / `WAITAOF` (`replication.rs:687-714`, `connection.rs:12024-12040`) — new since the prior revision, and `WAITAOF` does not actually check local AOF state

```rust
pub async fn wait_replicas(port: u16, numreplicas: usize, timeout_ms: u64) -> usize {
    // polls hub.replicas every 5ms until `numreplicas` replicas' ack_offset >= the
    // master_repl_offset captured at call time, or the timeout elapses, or timeout_ms == 0
    // (return immediately with however many already qualify)
}
```

`Command::Wait { numreplicas, timeout }` calls this directly and replies with the resulting count as a RESP
integer. **`Command::WaitAof { numlocal: _, numreplicas, timeout }` calls the exact same `wait_replicas`
function and ignores `numlocal` entirely** (`connection.rs:12032-12040`) — it replies `*2\r\n:1\r\n:{count}\r\n`,
hardcoding the "local AOF persisted" half of the reply to `1` unconditionally, regardless of whether AOF is even
enabled on this node, whether it's caught up to the fsync point, or whether `numlocal` requested `0`. `WAITAOF`
is therefore functionally identical to `WAIT` today except for its reply shape; it provides no actual guarantee
about local AOF durability.

### 3.9 The JSON.*, probabilistic (`BF.*`/`CF.*`/`CMS.*`/`TOPK.*`), and CRDT.* command families have zero live AOF or replication coverage — traced end-to-end, newly confirmed

This is the most significant finding from this pass. Every mutating command handler in `connection.rs` that
wants AOF+replication effects calls the `record_change!` macro (`connection.rs:12740-12761`):

```rust
macro_rules! record_change {
    ($cmd_expr:expr) => {
        DIRTY_CHANGES.fetch_add(1, Ordering::Relaxed);
        if HAS_WATCHED_KEYS.load(Ordering::Relaxed) { /* touch_watched_key for MULTI/WATCH */ }
        let need_aof = aof.is_some();
        let need_rep = crate::replication::has_connected_replicas(db.port);
        if need_aof || need_rep {
            if let Some(bytes) = crate::aof::command_to_resp($cmd_expr) {
                if let Some(aof_w) = aof { aof_w.borrow_mut().append(&bytes); }
                if need_rep { crate::replication::propagate_shard_bytes(db.port, db.shard_id, &bytes); }
            }
        }
    };
}
```

`Command::JsonSet`/`JsonDel`/`JsonArrAppend`/and every other JSON-mutating variant call `record_change!(cmd)`
after mutating `db.json_store` (e.g. `connection.rs:16790-16836`) — but, per §3.1, `command_to_resp` has **no
match arm for any JSON command** (confirmed via a repo-wide `rg "Json" src/aof.rs`, zero hits). The `if let
Some(bytes) = ...` therefore never fires: `DIRTY_CHANGES` still increments and `WATCH`ed-key invalidation still
works correctly (those don't depend on `command_to_resp`), but **the mutation is never appended to the AOF and
never propagated to a live PSYNC replica**. The exact same pattern was independently verified for `Command::BfAdd`
(`connection.rs:17405-17419`, calls `record_change!(cmd)` after mutating `db.probabilistic_store.bloom_filters`)
and holds for the rest of the `BF.*`/`CF.*`/`CMS.*`/`TOPK.*` family and for `CRDT.*` — none of these command
enums appear anywhere in `aof.rs`.

**Practical consequence**: JSON documents, Bloom/Cuckoo filters, Count-Min Sketches, Top-K trackers, and CRDT
state are captured correctly in a point-in-time RDB snapshot (§3.9 below, tags 7/8/10/11/12/13) and in a
`BGREWRITEAOF` compaction snapshot *for JSON specifically* (§3.3 — `rewrite_shard_aof` does snapshot
`db.json_store`, but not the probabilistic stores or CRDT store). But any live mutation to these types that
happens **after** the last RDB save / AOF rewrite is invisible to both AOF replay on restart and to a connected
PSYNC replica's live stream — a replica's JSON/probabilistic/CRDT state silently diverges from the master the
moment such a write occurs, and an AOF-only deployment (`appendonly yes`, no scheduled `SAVE`) permanently loses
these writes on any restart. Vector-set (`VADD`/`VREM`/`VSETATTR`), semantic-cache (`SEMANTIC.*`), and
agent-runtime (`AGENT.*`) commands are **not** affected — all of those do have explicit `command_to_resp` arms
(§3.1) and are correctly AOF-logged and replicated live.

### 3.10 RDB save format — the "14-tag" record format is now (at least) 19 tags, and the `RudisValue::Tiered` zero-byte bug is fixed at the live code path but survives as dead code

`ShardDb::save_rdb_chunk` (`src/shard.rs:2274-2318`) — the function every production caller actually invokes
(`Router::perform_save_rdb`, `Router::generate_full_rdb`, `run_shard_replication_flow`, and the cross-shard
`ShardMessage::SaveRdbChunk` responder all call `ShardDb::save_rdb_chunk`, never `RudisTable::save_rdb_chunk`) —
writes, per live table entry: an optional `0xFC` `EXPIRETIME_MS` opcode + 8-byte unix-ms expiry, then
`key_len:u32 | key_bytes`, then the value payload. For `RudisValue::Tiered(ptr)` specifically, it reads the
record back via `self.tier_manager.read_ptr_sync(*ptr)` *before* writing anything for the entry and, because the
record already holds the value in `serialize_val_payload` form, writes that payload verbatim (after checking its
type tag against `ptr.value_type`), so tiered hashes, lists, sets and zsets keep their type. A failed read skips
the whole entry and logs an error, rather than leaving a dangling `0xFC` opcode. (History: the code once wrote
zero bytes for `Tiered`, then re-wrapped the encoded payload as a `String`, corrupting every spilled key.) After
restore, a previously-tiered key comes back as a regular hot entry of its original type — tiering state itself is
not round-tripped, only the value.

It then calls `save_extended_rdb_chunk` (`shard.rs:2320-2656`), which appends one additional tagged record per
entry in each of these `ShardDb` side-stores, using the **same** `key_len:u32 | key_bytes | type_byte | ...`
framing as the base table:

| Type byte | Contents | Source field |
| :-: | :--- | :--- |
| 0 | String/Int payload (incl. hydrated `Tiered`/`Cooled`) | `RudisTable::serialize_val_payload` |
| 1 | List | ″ |
| 2 | Set | ″ |
| 3 | ZSet | ″ |
| 4 | Hash / SmallHash | ″ |
| 5 | HyperLogLog (16,384-byte register array, written raw) | ″ |
| 6 | Stream (entries, groups, PEL, consumers, idempotency dedup ledger) | ″ |
| 7 | JSON document (`serde_json::Value` serialized to a UTF-8 string) | `db.json_store` |
| 8 | Bloom filter (capacity, error_rate, num_bits, num_hashes, count, raw bit words) | `db.probabilistic_store.bloom_filters` |
| 9 | Vector-set node (index name, metric, raw `f32` vector, quant/PQ/tiered flags, attribute JSON) | `db.vector_indexes` |
| 10 | Cuckoo filter (capacity, buckets, fingerprints) | `db.probabilistic_store.cuckoo_filters` |
| 11 | Count-Min Sketch (width, depth, total_count, full table) | `db.probabilistic_store.cms_sketches` |
| 12 | Top-K tracker (k, item→count map) | `db.probabilistic_store.topk_trackers` |
| 13 | CRDT sync payload (one record under a synthetic `__rudis_crdt_sync__` key) | `db.crdt_store.export_sync_payload()` |
| 14 | Hash field expirations (Redis 7.4/Valkey 8 `HEXPIRE`) | `db.table.hash_field_expires` |
| 15 | Semantic cache (dim, hit/miss/tokens-saved/evicted counters, entries w/ vectors) | `db.semantic_caches` |
| 16 | Agent memory session (turns, tokens, compaction count, per-turn embeddings) | `db.agent_memories` |
| 17 | Agent DAG checkpoint thread (ordered nodes, parent links, state blobs) | `db.agent_checkpoints` |
| 18 | Agent tool-call registry (leases, inputs/outputs, attempt counts) | `db.agent_tools` |

That is 19 distinct record shapes (type bytes 0–18) plus the 4 framing opcodes `0xFC`/`0xFD`/`0xFE`/`0xFF` —
strictly more than the 14-tag count recorded by a prior revision of this document; tags 15–18 (semantic cache,
agent memory, agent checkpoints, agent tool leases) did not exist at that time.

**Dead code found**: `RudisTable::save_rdb_chunk` (`table.rs:13181-13235`) and `RudisTable::restore_rdb_chunk`
(`table.rs:13237+`) are a second, older, self-contained implementation of the base-table (tags 0/14 only)
serialize/restore pair. A repo-wide grep confirms **neither is called from anywhere** — not production code, not
even a unit test. `RudisTable::save_rdb_chunk`'s value serialization still calls into
`serialize_val_payload`/`RudisValue::Tiered(_) => {}` (table.rs:12545) directly, without the hydration
`ShardDb::save_rdb_chunk` performs — meaning the *old, broken, zero-byte-Tiered* behavior technically still
exists in the tree, just in unreachable code. This is confusing for a reader who greps for `save_rdb_chunk` and
lands in `table.rs` first; the live implementation is exclusively the `ShardDb` one in `shard.rs`.

**Load side**: the single production RDB-blob parser is `table::load_rdb_bytes` (`table.rs:14454-14968`, called
by both `Router::restore_rdb_bytes` for full-resync restore and by the startup restore path in `server.rs`). It
handles the `0xFC`/`0xFD`/`0xFE`/`0xFF` opcodes and type bytes 0–14 inline, and delegates type bytes 15–18 to
`ShardDb::restore_ai_native_rdb_record` (`shard.rs:3022+`) — so the full 19-tag format round-trips correctly
through the one production code path. `ShardDb::restore_rdb_chunk` (`shard.rs:2659+`) is a second, self-contained
restore implementation that *is* exercised — but only by `aof.rs`'s own unit tests
(`test_rdb_and_aof_vector_set_persistence`, `test_rdb_and_aof_ai_native_state_persistence`,
`test_rdb_and_aof_hash_field_expiration_persistence`) and by one test in `shard.rs` itself — never by the
production restore path.

### 3.11 RDB save mechanics: still forkless, still blocking `std::fs`, still no CRC64 SIMD, `save N M` is still parsed but never scheduled

`Router::perform_save_rdb` (`router.rs:3221-3280`) is unchanged in shape from the prior revision: it opens a temp
file with **blocking** `std::fs::File::create`/`write_all`/`sync_all` (not `monoio`'s async file I/O) inside an
`async fn`, writes the `"REDIS0011"` + `[0xFE, 0x00]` header, then streams the local shard's chunk followed by
every remote shard's chunk fetched one at a time via `ShardMessage::SaveRdbChunk` — "only one shard ever holds a
serialized chunk in memory at any time" per the source comment (`router.rs:3245-3246`) — computing the CRC64
incrementally (`crc64_update`) as each chunk arrives, then `fsync`s the file, atomically `rename`s it over
`dump.rdb`, and `fsync`s the containing directory via `sync_parent_dir`. There is still no `libc::fork()` call
anywhere in the repo (`rg "libc::fork"` — zero matches); this remains a genuinely forkless engine that trades
Redis's copy-on-write fork isolation for one-shard-at-a-time bounded memory instead. `generate_full_rdb`
(`router.rs:3099-3130`, used for PSYNC/`SYNC` full resync) is a related but distinct code path: it accumulates
**all** shard chunks into one in-memory `Vec<u8>` (not written incrementally to a file) before returning it, since
the whole blob needs to go out as one RESP bulk reply to the connecting replica.

`crc64`/`crc64_update` (`table.rs:14409-14439`) remain a plain 256-entry lookup-table, byte-at-a-time
implementation (`CRC64_TAB[((crc ^ byte) & 0xFF) as usize] ^ (crc >> 8)`, one byte per loop iteration) — no
SIMD/hardware-CRC path exists anywhere in the crate for this.

`save N M` is still not scheduled. `config.rs` parses the `save` directive only into a generic
`extra_directives: HashMap<String, String>` catch-all (confirmed by `config.rs:425`'s own unit test asserting
`cfg.extra_directives.get("save") == Some("900 1")`), and a repo-wide grep of `extra_directives` shows it is read
back only by that same test — nothing in `main.rs`, `server.rs`, or `router.rs` consumes it to schedule a
periodic `bgsave`. A configured `save 900 1` directive is accepted at startup and then has no effect.

---

## 4. Cross-Component Interactions

- **`src/router.rs`** (Component 04): `save_rdb`/`bgsave`/`bgrewriteaof`/`perform_rewrite_aof`/`generate_full_rdb`/
  `restore_rdb_bytes`/`perform_save_rdb` drive the RDB side of a full resync or manual snapshot, delegating to
  `ShardDb::save_rdb_chunk`/`table::load_rdb_bytes` per shard; `is_saving` (a single `AtomicBool`) is shared
  between `BGSAVE` and `BGREWRITEAOF`, so the two cannot run concurrently on one router.
- **`src/connection.rs`** (Component 02): owns `run_master_replica_stream` (PSYNC/`SYNC`, §3.5) and
  `run_shard_replication_flow` (DFLY FLOW, §3.6); the `record_change!` macro (§3.8/§3.9) is the actual call site
  most mutating command handlers use to reach both AOF and replication.
- **`src/server.rs`** (Component 01): spawns the 5ms/~1s AOF flush-and-fsync task and performs AOF replay and RDB
  restore on startup; also performs the final flush+fsync on shutdown.
- **`src/shard.rs`/`src/table.rs`** (Component 05): `ShardDb::save_rdb_chunk`/`save_extended_rdb_chunk`/
  `restore_ai_native_rdb_record` plus `table::load_rdb_bytes`/`serialize_val_payload`/`deserialize_val_payload`
  are the actual RDB chunk codec; `aof.rs`/`router.rs` only stitch chunks together and move bytes over the wire
  or to disk.

---

## 5. Known Bugs & Limitations (re-verified against current source)

- **NEW, significant — JSON.\*, `BF.*`/`CF.*`/`CMS.*`/`TOPK.*`, and `CRDT.*` command families have no live AOF or
  replication coverage** (§3.9): `command_to_resp` has no arm for any command in these three families, so
  `record_change!`'s `if let Some(bytes) = ...` never fires for them. They persist only through point-in-time RDB
  snapshots (and, for JSON only, `BGREWRITEAOF`'s one-shot compaction snapshot); any live write to these types is
  invisible to a connected replica and to AOF replay after restart.
- **NEW — `XAUTOCLAIM` is replicated/persisted as its original wall-clock-relative filter, not as its resolved
  claim set** (§3.1): a replica or an AOF replay can independently compute a different set of claimed messages
  than what the master actually claimed, because `min_idle_time` is re-evaluated against a different "now".
- **NEW — `WAITAOF`'s `numlocal` parameter is accepted but ignored**; it calls the same replica-ack-polling logic
  as `WAIT` and always reports a hardcoded local-fsync count of `1` (§3.8), regardless of whether AOF is enabled
  or caught up.
- **NEW — `RudisTable::save_rdb_chunk`/`restore_rdb_chunk` (`table.rs`) are dead code** that still contains the
  old zero-byte `RudisValue::Tiered` bug; the live path (`ShardDb::save_rdb_chunk`/`shard.rs`) fixed it via
  hydration, but a reader who finds the `table.rs` functions first would see stale, broken behavior (§3.10).
- **RESOLVED — the `RudisValue::Tiered` zero-byte RDB/AOF-rewrite serialization bug**, for the code path actually
  used in production (§3.10, §3.3): both `ShardDb::save_rdb_chunk` and `rewrite_shard_aof` now hydrate tiered
  values via `tier_manager.read_ptr_sync` before serializing them as `String` payloads.
- **RESOLVED — AOF rewrite and compaction via `BGREWRITEAOF`** now additionally covers JSON, vector-set,
  semantic-cache, and agent-runtime state (previously it covered only the base table + HLL + streams) — but still
  does not cover probabilistic structures or CRDT state (§3.3), consistent with those families never having AOF
  support at all.
- **STILL OPEN — `save N M` is parsed into a generic `extra_directives` map and never scheduled** (§3.11): no
  periodic-save mechanism exists anywhere in the codebase.
- **STILL OPEN — RDB save uses blocking `std::fs` calls inside an `async fn`, and there is no `fork()`-based
  isolation anywhere** (§3.11): the forkless design bounds memory by streaming one shard's chunk at a time rather
  than by copy-on-write page sharing.
- **STILL OPEN — CRC64 is a plain byte-at-a-time table lookup, no SIMD/hardware-CRC path** (§3.11).
- **STILL OPEN — `DFLY FLOW`'s `lsn` parameter is accepted but unused for resuming a partial per-flow sync**
  (§3.6); every flow session receives a fresh full RDB chunk for its shard.
- **STILL OPEN — `DFLY FLOW` sessions are not reflected in `INFO replication`/`ROLE`** (§3.7).
- **STILL OPEN — the 1MB `ReplicationBacklog` size and the AOF flush/fsync cadence remain compile-time
  constants**; `appendfsync everysec`/`no` are configurable but `always` is rejected (§2.1).
- **STILL OPEN — `replid` is derived from `fxhash` over port+timestamp**, not a cryptographically-derived run-id
  (§2.2).

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: `BGSAVE`/full resync stream shard RDB chunks sequentially, one shard at a time, to keep peak
  memory bounded — do not change this to a concurrent fan-out without re-checking the memory-bound rationale
  (`router.rs:3245-3246`).
* **Gotcha 2**: `BGREWRITEAOF` uses a 64KB `BufWriter` to stream the rewritten AOF file without allocating a
  large heap buffer for the whole dataset — keep new value types' rewrite logic writing through that same
  buffered writer rather than building an intermediate `Vec` (`aof.rs:1993`).
* **Gotcha 3 (now with concrete, currently-shipping examples)**: A write command is invisible to both AOF and
  replication unless it has an explicit arm in `command_to_resp` (§3.1) — **today this is true for every JSON.\*,
  `BF.*`/`CF.*`/`CMS.*`/`TOPK.*`, and `CRDT.*` mutating command** (§3.9). Adding a new mutating command (or
  fixing one of these families) requires adding a `command_to_resp` arm, or it will silently not persist or
  replicate live.
* **Gotcha 4**: `DFLY FLOW` is a master-side-only capability today; do not assume rudis-to-rudis replication ever
  uses it — `run_replica_worker` only speaks `PSYNC`/`SYNC`.
* **Gotcha 5**: `ReplicationBacklog`'s 1MB size is a compile-time constant (`ReplicationHub::new`); a write burst
  larger than that between a replica's disconnect and reconnect will force a full resync.
* **Gotcha 6 (new)**: If you are implementing persistence/replication for a new random or time-relative command
  (anything like `XAUTOCLAIM`, `SPOP`, `RANDOMKEY`-adjacent operations), re-encoding the *input* command is only
  safe if the operation's outcome is a pure function of already-replicated state plus deterministic, unseeded
  hashing (verify against `RudisSet`'s `FxBuildHasher`-based determinism, §3.1) — if the outcome depends on
  wall-clock time (like `XAUTOCLAIM`'s `min_idle_time`) or a seeded RNG, re-encode the *resolved result* instead.
* **Gotcha 7 (new)**: There are two independent RDB chunk codecs in the tree — the live one (`ShardDb::
  save_rdb_chunk`/`save_extended_rdb_chunk`/`restore_ai_native_rdb_record` in `shard.rs`, plus `table::
  load_rdb_bytes` for parsing) and a dead, unreachable one (`RudisTable::save_rdb_chunk`/`restore_rdb_chunk` in
  `table.rs`). Do not edit the `table.rs` pair expecting it to affect production behavior; grep for callers before
  trusting either implementation (§3.10).

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
