# Rudis Component Architecture Documentation

This is the single reference for how every core module of `rudis` works — 19 subsystems,
each a deep-dive against the **real, current source**, not an idealized description. Components
01-15 merge what used to be 15 separate files (`docs/components/01_*.md` ... `15_*.md`) into
one document; Components 16-19 (JSON, Geospatial, Probabilistic structures, Pub/Sub) were
written directly here to close the remaining gap in module coverage. Cross-references between
components (e.g. "see Component 04 §7") work exactly as written — every component keeps its
own number as a `##` section in this file.

> ℹ️ **All 15 sections were rewritten and verified against the real source** (each claim,
> struct, and code excerpt checked against the actual `src/` file it documents — see each
> section's own notes on what was fixed and, in several cases, real bugs/dead-code findings
> turned up along the way). The original versions of every one of these sections were
> fabricated: invented structs and functions with no basis in the actual source, describing
> a plausible-sounding but fictional implementation. Nothing here should be assumed accurate
> again without re-verifying against `src/` if the underlying code changes further — these
> docs describe a snapshot, not a contract.
>
> [`docs/designs/components.md`](../designs/components.md) also covers the same five
> subsystems as Components 01-05 (independently written, also verified against real code) —
> the two should agree; if they ever diverge, re-check both against the source rather than
> trusting either by default.
>
> Every component section also ends with a **Future Improvements** subsection — concrete,
> prioritized (High/Medium/Low, or RESOLVED where a later fix landed) suggestions grounded in
> the real gaps, bugs, and dead code found while verifying that section, not speculative
> wishlist items. The handful of cross-cutting items that show up in more than one component
> (the `MGET`/`MSET` fan-out gap, the two competing slot-authority mechanisms, the
> squashed-pipeline path's slot-migration check) are cross-referenced between their respective
> sections rather than duplicated in full.

---

## Component Index

| # | Component | Primary Source Files | Focus Areas |
| :---: | :--- | :--- | :--- |
| **01** | **Reactor Runtime & Server Lifecycle** | `src/main.rs`, `src/server.rs` | Real per-shard startup sequence (RDB restore, AOF replay, tiering manager, cluster bus on shard 0 only), ~37-variant `ShardMessage` with burst draining, `BlockHub` as one lock exception, a real `--tls-port` second accept loop. No graceful shutdown. |
| **02** | **Connection Lifecycle & Command Execution** | `src/connection.rs` | Real `ClientInfo`/`ClientTracker`, cross-shard `MULTI`/`EXEC` transaction locking, inline live-slot-migration redirects, NVMe-tiered `GET` fallback, RESP3 tracking, real ACL enforcement (`-NOPERM`). `MGET`/`MSET` now genuinely fan out in parallel across shards — but only the first key of a multi-key command is checked against ACL/slot-migration state, and shard indices ≥64 silently drop results. |
| **03** | **RESP Protocol Engine & Serialization** | `src/resp.rs` | Real three-grammar dispatch (RESP array / Memcached storage-command trial parse / inline), real `Command` enum shape. No dedicated serialization helpers — replies are hand-formatted in `connection.rs`. |
| **04** | **Sharding Architecture & Cross-Core Mesh** | `src/router.rs`, `src/shard.rs` | Real `Router`/`ShardDb` fields, ~40-variant `ShardMessage`, `CompactResp` small-buffer optimization. `MGET`/`MSET` now fan out via pooled channels and a `try_recv` harvest sweep — but route via `slot_owners` while single-key ops still use static `target_shard()`, a new inconsistency risk during live migration. `Router::check_slot_redirection` and the >64-shard bitmask gap are still open. |
| **05** | **Storage Engine & Compact Encodings** | `src/table.rs` | Real SIMD `RudisFlatTable`/`RudisTable` engine (unchanged core mechanism), real 11-variant `RudisValue` (`Int`/`SmallHash`/`Tiered`/`Cooled` included). No Listpack/Intset. `ZRANK` is still O(n) even in the "Full" ZSet representation. |
| **06** | **Blocking Operations & The Reactive Event Hub** | `src/block.rs` | Real `BlockHub` behind a process-wide `Mutex` (the one deliberate lock exception), `flume` channels, active fd-polling for disconnect detection, `MULTI`/`EXEC`-aware deferred wakeups (distinct from the no-op `CLIENT PAUSE`). |
| **07** | **NVMe SSD Tiered Storage Engine** | `src/tiering.rs` | Real `ShardTierManager`/`OpManager`/`SmallBinsManager` 4KB page packing, CRC64-checked records, `fallocate` hole-punching, `FICLONE` reflink snapshots. `O_DIRECT` is opt-in (`RUDIS_DIRECT_IO`) with silent fallback. Real lifecycle is Hot → Cooled → Tiered, with no direct path back to Hot. |
| **08** | **Vector Search Engine: HNSW, SQ8, PQ & ADC** | `src/vector.rs` | Real HNSW/SQ8/PQ+ADC with AVX2+FMA SIMD (no NEON path). PQ codebooks are a fixed deterministic basis, not trained on data. Vector indexes are per-shard-local with no cross-shard fan-out. |
| **09** | **RediSearch Full-Text Engine & RRF** | `src/search.rs` | Real BM25 (k1=1.2, b=0.75) and RRF scoring, a real query DSL. Index registry is process-wide shared state, not thread-local. `KNN`/hybrid search now genuinely works end-to-end — `PARAMS`-supplied query vectors are wired into execution, and auto-indexed documents' vector fields are now actually parsed and stored. |
| **10** | **Kernel Bypass & Zero-Copy Networking** | `src/xdp.rs`, `src/zerocopy.rs` | No real AF_XDP/eBPF — a userspace-simulated packet pipeline reachable only via `XDP.*` commands, never real NIC ingress. Real `SO_ZEROCOPY`/`MSG_ZEROCOPY` code exists but has zero callers anywhere — dead code. |
| **11** | **Redis Cluster Topology & Gossip Protocol** | `src/cluster.rs` | Plain-text line gossip protocol (not binary), full-state resend every 500ms, unilateral (non-quorum) failure detection, a real majority-vote replica election. Slot redirection is genuinely wired into the command path via `connection.rs` + `ClusterHub`, now including the pipelined squashed-command path. |
| **12** | **CRDT Data Types & Manual Multi-Region Sync** | `src/crdt.rs` | Real LWW-Register/OR-Set/PN-Counter CRDTs with a CAS-based Hybrid Logical Clock. Single-key commands now correctly route through the normal key-slot mechanism, and `CRDT.DUMP`/`MERGE`/`GC` now fan out to every shard — but sync between separate Rudis *instances* is still entirely manual, no automatic network transport. |
| **13** | **Lua Scripting & Redis 7 Functions Engine** | `src/scripting.rs` | A fresh `mlua::Lua` VM per call (no persistent interpreter or bytecode cache), SHA1-cached script/library *source text*, real `redis.call`/`redis.pcall` via the normal command-execution path. `FCALL`'s AOF-bypass bug is now fixed. |
| **14** | **Persistence & Replication Engines** | `src/replication.rs`, `src/aof.rs` | Still no AOF rewrite/compaction — the file grows forever. Partial `PSYNC` resync (`+CONTINUE`) is now fully supported on both master and replica sides with automated reconnect and PSYNC2 failover handover. A real custom RDB binary format with a CRC64 trailer. |
| **15** | **Security, Memory Allocator & TLS** | `src/acl.rs`, `src/allocator.rs`, `src/tls.rs` | ACL now hashes passwords (SHA1 with a hardcoded global salt — weak, and the plaintext is *still also* stored) and genuinely enforces per-command/per-key permissions. TLS is now wired to a `--tls-port` listener, but has a **critical live bug**: its kTLS fast-path marks itself active without ever installing kernel key material, so it silently sends all "encrypted" traffic in cleartext. |
| **16** | **JSON Document Store & JSONPath Engine** | `src/json.rs` | A real, hand-written JSONPath subset (no recursive descent, no filter expressions) over `serde_json::Value`. Single-key commands genuinely route per-shard, unlike Vector (08); `JSON.MGET` is still sequential per-key, unlike the already-fixed `MGET`/`MSET`. Not included in RDB persistence. |
| **17** | **Geospatial Commands** | `src/geo.rs` | Owns zero storage — every `GEO*` command is a thin layer over `ZADD`/`ZSCORE`/`ZRANGE` (Component 05), matching real Redis's own architecture. `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` are three independent O(N) full-set brute-force scans, not geohash-neighborhood-pruned. |
| **18** | **Probabilistic Data Structures** | `src/probabilistic.rs` | Real Bloom filter, Cuckoo filter (with real eviction and deletion), Count-Min Sketch, and Space-Saving Top-K — all textbook-correct implementations sharing one FNV-1a-based double-hash scheme. Routes per-key like Components 16/17. Not included in RDB persistence. |
| **19** | **Pub/Sub Messaging Hub** | `src/pubsub.rs` | Genuinely per-shard (not a global-lock exception like `BlockHub`), with real parallel cross-shard fan-out for `PUBLISH`/`PUBSUB *`. Verified gap: every delivered message is hard-coded RESP2 array framing — RESP3-negotiated subscribers never get the RESP3 push type. |

---

## High-Level Architecture Map

```
                                Client Connections
                                        │
                         (SO_REUSEPORT Kernel Balancing)
                         ┌──────────────┴──────────────┐
                         ▼                             ▼
                 ┌───────────────┐             ┌───────────────┐
                 │    Core 0     │             │    Core 1     │
                 │ Monoio Runtime│             │ Monoio Runtime│
                 │  (01_reactor) │             │  (01_reactor) │
                 ├───────────────┤             ├───────────────┤
                 │  Connection   │             │  Connection   │
                 │ (02_connect)  │             │ (02_connect)  │
                 ├───────────────┤             ├───────────────┤
                 │  RESP Parser  │             │  RESP Parser  │
                 │   (03_resp)   │             │   (03_resp)   │
                 ├───────────────┤             ├───────────────┤
                 │   Storage     │             │   Storage     │
                 │  (05_table)   │             │  (05_table)   │
                 ├───────────────┤             ├───────────────┤
                 │  SSD Tiering  │             │  SSD Tiering  │
                 │  (07_tiering) │             │  (07_tiering) │
                 └───────┬───────┘             └───────┬───────┘
                         │        Cross-Shard Mesh     │
                         └────────◄ (04_router) ►──────┘
```

---
---

## Component 01: Reactor Runtime & Server Lifecycle (`src/main.rs`, `src/server.rs`)

### 1. Architectural Purpose & Scope

The **Reactor Runtime & Server Lifecycle** subsystem is responsible for bootstrapping the Rudis server process, pinning worker threads to physical CPU cores, setting up Linux `io_uring` instances via the `monoio` asynchronous runtime, and running each shard's event loop for its entire lifetime.

Unlike Redis (single-threaded event loop) or lock-based multi-threaded servers, Rudis uses a **Shared-Nothing Multi-Reactor** pattern: every worker core runs its own independent `monoio` runtime driving an isolated Linux `io_uring` ring, its own `SO_REUSEPORT` listener, and its own thread-local database. `src/main.rs` parses CLI arguments and spawns one OS thread per shard; `src/server.rs::run_shard_worker` is the entire body of that thread — it never returns.

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-per-Core Pinning**: Every worker thread is pinned to an exclusive CPU core using `core_affinity::set_for_current`, unless `--no-pin` is passed. No worker thread is migrated by the OS scheduler once pinned.
2. **`SO_REUSEPORT` Ingress Balancing**: Every worker thread opens its own listening socket bound to the same port (`socket.set_reuse_port(true)`). The kernel distributes new incoming connections across all bound sockets by a 4-tuple hash, with zero userspace dispatch.
3. **Thread-local database, no locks in the data path**: `ShardDb` lives in a plain `Rc<RefCell<ShardDb>>` — not `Arc<Mutex<_>>`. Because `Rc`/`RefCell` aren't `Send`, the compiler itself refuses to let a `ShardDb` handle cross a thread boundary.
4. **One real exception to "zero locks": the blocking-command hub.** `crate::block::get_block_hub_for_port(port)` returns an `Arc<Mutex<BlockHub>>` from a process-wide `static` map keyed by port (`PORT_BLOCK_HUBS`), so it *is* shared and mutex-guarded across every shard thread serving that port. This is a deliberate, narrow exception: blocking commands (`BLPOP`, `BZPOPMIN`, ...) need cross-shard wakeups, which the shared-nothing model can't give them for free, so a small locked structure was introduced specifically for that coordination rather than for the data path itself.
5. **No graceful shutdown.** There is no signal handler anywhere in `src/server.rs` or `src/main.rs` — no `SIGINT`/`SIGTERM` trap, no drain, no flush-on-exit logic. The process only stops if every thread's infinite loop is killed externally (the accept loop and the cross-shard receiver loop both run forever).

---

### 3. Component Architecture & Data Structures

```
                             Process Startup (main.rs)
                                        │
                          Args::parse() (clap) + tiering config
                                        │
                     Detect Available Hardware Cores (N)
                                        │
             Build N flume::unbounded channels (cross-shard mesh)
                                        │
             ┌──────────────────────────┴──────────────────────────┐
             ▼                                                     ▼
    thread::Builder "rudis-shard-0"                    thread::Builder "rudis-shard-i"
             │                                                     │
    run_shard_worker(0, N, port, senders,               run_shard_worker(i, N, port, senders,
        rx_0, core_id_0, aof_config)                        rx_i, core_id_i, aof_config)
```

#### What `main.rs` actually builds before spawning threads

```rust
struct Args {
    port: u16,
    threads: Option<usize>,
    aof: bool,
    aof_dir: std::path::PathBuf,
    maxmemory: Option<String>,
    tiered_offload_threshold: u64,
    tiered_upload_threshold: u64,
    no_pin: bool,
    tls_port: Option<u16>,
    tls_cert_file: Option<std::path::PathBuf>,
    tls_key_file: Option<std::path::PathBuf>,
}
```

`num_shards` defaults to `num_cores.min(8)` (capped, not "one shard per core", so a large
shared host doesn't silently spawn one `io_uring` ring and listener per core with no flags
given). `main.rs` also applies the tiering config globally via `rudis::tiering::set_max_memory`
/ `set_offload_threshold_pct` / `set_upload_threshold_pct` (keyed by port) before any thread
starts, and builds one `AofConfig { enabled, dir, fsync_every_sec: true }` shared by every
shard.

**TLS is now real and wired up here** (previously dead code per Component 15 — that finding is
now stale). If `--tls-port` is passed, `main.rs` builds one `rustls::ServerConfig` up front
(before any shard thread starts) — from `--tls-cert-file`/`--tls-key-file` if both are given, via
`crate::tls::load_certs_and_key_from_files`, otherwise a fresh in-memory self-signed cert for
`localhost`/`127.0.0.1` via `crate::tls::generate_self_signed_cert` — and wraps it in a
`Clone`-able `TlsWorkerConfig { tls_port, server_config }` that's cloned once per shard, exactly
like `AofConfig`.

#### `run_shard_worker`'s signature (what each thread actually receives)

```rust
pub fn run_shard_worker(
    shard_id: usize,
    num_shards: usize,
    port: u16,
    senders: Vec<flume::Sender<ShardMessage>>,
    rx: flume::Receiver<ShardMessage>,
    core_id: Option<core_affinity::CoreId>,
    aof_config: crate::aof::AofConfig,
    tls_config: Option<crate::tls::TlsWorkerConfig>,
)
```

Every shard gets a clone of the full `senders` vector (so it can reach any other shard) but
only its own `rx`, plus its own clone of the optional `tls_config`.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Startup sequence inside `run_shard_worker`

In order, each shard thread:

1. Pins to its core (if assigned), then builds a `monoio::RuntimeBuilder<IoUringDriver>` with `enable_timer()`.
2. Inside `rt.block_on(async move { ... })`, creates its `SO_REUSEPORT`/`SO_REUSEADDR` socket, sets 512KB send/recv buffers, binds, and listens (backlog 4096). **If `tls_config` is `Some`, immediately builds a second, independent `SO_REUSEPORT` socket bound to `tls_cfg.tls_port`** — the setup code is a near-verbatim copy of the plain-port socket setup (same buffer sizes, same reuse flags), just a different port and address variable, kept as a separate `Option<TcpListener>` (`tls_listener`) alongside the plain `listener`.
3. Creates `let local_db = Rc::new(RefCell::new(ShardDb::new(port)))`.
4. **RDB restore**: if AOF is *not* enabled and `<aof_dir>/dump.rdb` exists, calls `crate::table::load_rdb(&rdb_path, &mut local_db.borrow_mut(), shard_id, num_shards)`.
5. **AOF replay + writer**: if AOF is enabled, replays `<aof_dir>/appendonly-<shard_id>.aof` via `crate::aof::replay_aof`, then opens an `AofWriter`. If opened, spawns a dedicated `monoio::spawn` task that wakes every 50ms, flushes any pending write chunk (`take_flush_chunk`), and `fsync`s roughly every 20 ticks (~1s) when `fsync_every_sec` is set.
6. **Cluster bus**: only `shard_id == 0` calls `crate::cluster::start_cluster_bus(port)` — the gossip/cluster-bus listener is a single, once-per-process background task, not one per shard.
7. **Tiered storage**: opens a `crate::tiering::ShardTierManager` for this shard/port and, if successful, attaches it as `local_db.borrow_mut().tier_manager`.
8. Builds `client_registry` (`Rc<RefCell<HashMap<u64, ClientInfo>>>`), a `pubsub` hub (`Rc<RefCell<PubSubHub>>`), and the shard's `Router` (which itself owns `slot_states`, `slot_owners`, the AOF writer handle, the pubsub handle, and a small transaction lock/waiter queue — see Component 04).
9. Spawns three independent periodic `monoio::spawn` tasks sharing the same `Rc<RefCell<ShardDb>>`/`Router` with no synchronization:
   - every 100ms: `active_db.borrow_mut().active_expire_cycle()`
   - every 20ms: `offload_router.check_auto_tier().await` (auto-tiering pressure check)
   - every 2s: `gc_router.gc_local()` (tiered-storage GC / hole punching)
10. Spawns the **cross-shard receiver task** (§4.2) — the server side of the `Router`/`ShardMessage` mesh.
11. Prints the shard's startup banner, then enters the **accept loop** (§4.3) forever.

#### 4.2 The cross-shard receiver loop: one `match` arm per `ShardMessage` variant, now with burst draining

```rust
monoio::spawn(async move {
    while let Ok(mut msg) = rx.recv_async().await {
        let mut burst = 0;
        loop {
            match msg {
                ShardMessage::Get { key, responder } => { /* ... */ }
                ShardMessage::Set { key, value, expire_in, responder } => { /* ... */ }
                ShardMessage::Batch { items, responder, is_resp3 } => { /* ... */ }
                ShardMessage::Mget { keys, responder } => { /* ... */ }
                ShardMessage::Mset { pairs, responder } => { /* ... */ }
                // ~37 variants total: Del, Exists, IncrBy, Expire, Persist, Ttl,
                // CountKeysInSlot, GetKeysInSlot, ClientList, SetSlotState, SetSlotOwner,
                // DumpKey, SyncAof, SaveRdbChunk, Publish, PubsubChannels, PubsubNumsub,
                // PubsubNumpat, Keys, Scan, RandomKey, ExpireTime, AcquireTxLock,
                // ReleaseTxLock, RestoreRdbChunk, ExecuteReplicaCmd, NotifyList,
                // TierSpill, TierLoad, TierSpillAll, TierCool, TierDecommit,
                // GetUsedMemory, StreamColdRead, TierGc, TierSnapshot, FlushSlots,
                // Stick, Unstick, IsSticky, Delex
            }
            burst += 1;
            if burst >= 64 { break; }
            match rx.try_recv() {
                Ok(next) => msg = next,
                Err(_) => break,
            }
        }
    }
});
```

**New since the last verified version of this doc: the receiver loop drains up to 64 messages
per wakeup instead of one.** The outer `while let Ok(mut msg) = rx.recv_async().await` still
suspends the task when the mailbox is empty, but once woken it now loops on a cheap, non-async
`rx.try_recv()` to keep processing already-queued messages inline — up to a hardcoded cap of
64 — before yielding back to `recv_async().await`. Under sustained cross-shard load (many
remote `Batch`/`Mget`/`Mset` messages arriving back-to-back) this amortizes the async
wakeup/poll overhead across up to 64 messages instead of paying it once per message.

Beyond the plain key ops (`Get`/`Set`/`Del`/...), this match also covers slot ownership/
migration (`SetSlotState`, `SetSlotOwner`, `FlushSlots`), RDB/AOF durability (`SaveRdbChunk`,
`SyncAof`, `RestoreRdbChunk`), pub/sub fan-out (`Publish`, `PubsubChannels`, `PubsubNumsub`,
`PubsubNumpat`), a distributed transaction lock (`AcquireTxLock`/`ReleaseTxLock`, backed by
`Router::tx_lock` + `tx_waiters: VecDeque`), replication apply (`ExecuteReplicaCmd`), blocking-op
wakeups (`NotifyList`, which locks the port's shared `BlockHub` — see §2.4), and the NVMe
tiering control plane (`TierSpill`/`TierLoad`/`TierSpillAll`/`TierCool`/`TierDecommit`/`TierGc`/
`TierSnapshot`/`StreamColdRead`/`GetUsedMemory`).

`ShardMessage::Batch` is still the pipeline-squashing primitive (Component 02/`connection.rs`
sends one `Batch` per remote shard per pipeline flush), and it now checks `has_tier_manager =
db.tier_manager.is_some()` **before** scanning the batch for tiered misses — a shard with
tiering disabled entirely skips the per-item `is_tiered` check rather than paying it on every
`Get` in every batch. When tiering is enabled and at least one `Get` in the batch misses
locally *and* is tiered, the whole batch is handled inside a separate `monoio::spawn`ed task
that can `.await` a cold read (`stream_cold_read_local`) per item; otherwise it's handled
synchronously in the receiver task itself with `execute_local_command`. Writes in the batch now
call the synchronous `router.check_auto_tier_after_write()` directly (not the old async
`check_auto_tier().await` spawned as a separate fire-and-forget task) — this is a real change
from the previously-verified version of this doc, worth noting if you've read an earlier copy.

**`ShardMessage::Mget`/`Mset` are new** — the server-side half of the parallel cross-shard
`MGET`/`MSET` fan-out implemented in Component 04/02 (previously a documented gap: each key was
routed and awaited one at a time). `Mget`'s handler mirrors `Get`'s tiering-aware fast/slow
split, but batched: it fast-paths every key with `db.tier_manager.is_none()` (a plain
per-key `db.get` loop, no async at all), and when tiering is enabled, walks the key list
synchronously until it hits the first tiered miss, then hands the *remaining* keys off to a
spawned task that resolves each one exactly the way a single tiered `Get` would (checking
memory pressure, then either a streaming cold read or a full `ensure_loaded`). `Mset`'s
handler is simpler: apply every pair to the local `ShardDb` inline, append one AOF record for
the whole batch (`Command::Mset(pairs)`, not one record per pair), then call
`check_auto_tier_after_write()` once.

#### 4.3 The accept loop(s): plain TCP always, plus an optional TLS accept loop

```rust
let mut next_client_id: u64 = ((shard_id as u64) << 48) + 1;
loop {
    match listener.accept().await {
        Ok((stream, client_addr)) => {
            let _ = stream.set_nodelay(true);
            let client_id = next_client_id;
            next_client_id += 1;
            monoio::spawn(async move {
                handle_connection(stream, client_addr, client_id, reg, r).await;
            });
        }
        Err(e) => eprintln!("[Shard {}] Accept error: {}", shard_id, e),
    }
}
```

Each accepted connection becomes its own `monoio::spawn`ed task. Client IDs encode the owning
shard in the top 16 bits (`(shard_id << 48) + counter`), so IDs never collide across shards
without any cross-shard coordination. An `accept()` error is logged and the loop just retries —
a transient error never brings down the shard's listener.

**If `tls_listener` is `Some`, a second accept loop is spawned first** (before the plain accept
loop above, both coexisting as separate `monoio::spawn`ed tasks on the same shard runtime).
Its client IDs are deliberately carved from a disjoint bit range of the same shard: `((shard_id
as u64) << 48) | 0x8000_0000_0000` plus an incrementing counter — i.e. the *upper* half of the
shard's 48-bit per-shard ID space, while the plain loop's `+ 1, 2, 3, ...` counter occupies the
*lower* half. This guarantees a TLS client and a plain-TCP client on the same shard can never
collide on client ID without any coordination between the two loops. Each accepted TLS
connection is upgraded via `crate::tls::TlsSession::new` + `session.handshake_monoio(&mut
stream)` (a real `rustls` handshake driven over the `monoio` stream) *before* being handed to
`crate::connection::handle_tls_connection` — a distinct entry point from the plain loop's
`handle_connection`, not the same function with a wrapped stream type.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: every accepted socket is handed off as `handle_connection(...)`, spawned as a task on this shard's runtime. See Component 02.
- **`src/router.rs`**: `Router` is constructed here once per shard and shared (`Rc`) with every connection task; it owns the `senders` mesh, slot ownership state, and the tiering/AOF/pubsub handles used throughout this file. See Component 04.
- **`src/aof.rs`**: AOF replay on startup, the periodic flush/fsync task, and per-command `command_to_resp` appends inside several `ShardMessage` handlers.
- **`src/tiering.rs`**: `ShardTierManager::open`, the 20ms auto-tier check, the 2s GC task, and the `Tier*`/`StreamColdRead` message variants.
- **`src/cluster.rs`**: `start_cluster_bus(port)`, started once from `shard_id == 0` only.
- **`src/block.rs`**: `get_block_hub_for_port(port)` is consulted both for `CLIENT LIST`'s blocked-flag and for `ShardMessage::NotifyList` wakeups — the one place this subsystem reaches for a real, shared mutex instead of thread-local state.
- **`src/pubsub.rs`**: `PubSubHub` is created per-shard but `Publish`/`PubsubChannels`/`PubsubNumsub`/`PubsubNumpat` are also reachable as `ShardMessage` variants so a publish on one shard can fan out to subscribers connected via other shards.
- **`src/tls.rs`** (Component 15 — no longer dead code): `TlsWorkerConfig`, `TlsSession::new`/`handshake_monoio`, and the cert-loading/self-signed-generation functions are now genuinely called from here when `--tls-port` is set; `crate::connection::handle_tls_connection` is the TLS-specific counterpart to the plain loop's `handle_connection`.

---

### 6. Performance Characteristics

- **Zero-syscall-per-connection ingress**: `SO_REUSEPORT` means the kernel — not userspace — decides which shard's listener gets each new connection.
- **No cross-core cache traffic in the common case**: local key access never leaves the owning thread; only the `ShardMessage` mesh and the shared `BlockHub` mutex cross cores, and both are only exercised on non-local or blocking operations.
- **Bounded periodic work, not full scans**: the 100ms expiration cycle, 20ms auto-tier check, and 2s GC task are all designed to do fixed, small amounts of work per tick rather than scanning the whole shard, so they never show up as a latency spike on the shared single-threaded runtime.
- **The cross-shard receiver loop now amortizes async overhead across up to 64 messages per wakeup** (§4.2's `try_recv` burst-draining) instead of paying one `recv_async().await` suspend/resume cycle per message — a real, measurable win under sustained cross-shard traffic (e.g. many concurrent `MGET`/`MSET` fan-outs or heavy pipeline squashing hitting one shard from many peers at once).
- **Batch-level tiering checks are now gated on whether tiering is enabled at all** (`has_tier_manager`, §4.2) — a shard running with no tiered storage configured skips the per-`Get`-in-batch `is_tiered` lookup entirely rather than paying a cheap-but-nonzero check on every batched read.

---

### 7. Future Improvements

- **High — add a graceful shutdown path.** Still open — there is no `SIGINT`/`SIGTERM` handler anywhere (§2.5), so a restart or deploy today is a hard kill: any buffered AOF chunk not yet flushed, any RDB save in progress, and any in-flight client request are simply dropped. Now that a shard can own up to *two* listeners (plain + optional TLS, §4.3), a clean shutdown needs to stop accepting on both. A minimal fix: catch the signal on the main thread, broadcast a shutdown `ShardMessage` (or a shared `AtomicBool` each accept loop polls) to every shard, have each shard flush its `AofWriter` and stop accepting new connections on both listeners, then join all threads before exiting.
- **Medium — reconcile this file's slot/shard model with live cluster migration.** Still open — `run_shard_worker` assigns shards statically at startup and never changes `num_shards`; meanwhile Component 04/11 already have real (if still partially disconnected — see their own Future Improvements) live-migration machinery.
- **Medium — give the 100ms/20ms/2s periodic tasks jitter or adaptive backoff.** Still open — all three are fixed-interval `monoio::time::sleep` loops; on a host running many shards, every shard's tasks tick in near-lockstep, a minor but avoidable source of correlated CPU bursts.
- **Low — de-duplicate the plain and TLS socket setup code (new, §4.1).** The TLS listener's `Socket::new`/`set_reuse_port`/`set_reuse_address`/`set_nonblocking`/buffer-size/bind/listen sequence is a near-verbatim copy of the plain listener's, differing only in which port variable is used. A small `fn bind_reuseport_listener(port: u16) -> io::Result<TcpListener>` helper, called once for the plain port and once (conditionally) for the TLS port, would remove the duplication risk (a future socket-tuning change applied to one copy and forgotten in the other).
- **Low — reduce `SO_REUSEPORT` imbalance sensitivity.** Still open, and now applies independently to *two* listeners per shard when TLS is enabled — the kernel's 4-tuple hash balancing is not perfectly even for a small number of long-lived connections (§5's "no connection-level backpressure" limitation).
- **Low — document/guard the `BlockHub` exception's failure mode.** Still open — if `get_block_hub_for_port`'s `Mutex` were ever poisoned by a panic while held, every future blocking operation on that port would panic on `.lock().unwrap()`.

---
---

## Component 02: Connection Lifecycle & Command Execution (`src/connection.rs`)

### 1. Architectural Purpose & Scope

`src/connection.rs` is the single largest module in Rudis (~10,600 lines) and the central
coordination layer for every client session. It owns the per-connection read/parse/execute/write
loop, protocol-mode transitions (Pub/Sub, replica streaming via `PSYNC`), RESP2/RESP3 reply
formatting and client-side caching invalidation, Redis transactions (`MULTI`/`EXEC`/`WATCH`),
blocking commands (`BLPOP`/`BZPOPMIN`/blocking `XREAD`), Redis Cluster slot-migration
redirection (`MOVED`/`ASK`/`ASKING`), ACL authentication gating, and the local-vs-remote
routing decision — plus command-specific execution logic for the full command surface (strings,
hashes, lists, sets, sorted sets, streams with consumer groups, HyperLogLog, bitmaps, geo,
probabilistic structures, JSON, vector search, Lua scripting, and a Memcached text-protocol
gateway). It is genuinely the busiest file in the codebase, not a thin dispatcher.

---

### 2. Key Invariants & Concurrency Constraints

1. **Pure Single-Threaded Client State**: Every connection is handled exclusively by the
   core that accepted it (`handle_connection`'s locals — `in_multi`, `tx_queue`, `asking`,
   `authenticated`, `auth_user` — are plain stack variables, no `Arc`/`Mutex`).
2. **Blocking Commands Force an Immediate Flush First**: Before executing a command like
   `BLPOP` that may suspend the task for up to its timeout, `handle_connection` flushes any
   already-buffered responses to the socket first — otherwise earlier pipelined replies would
   sit unsent for the entire blocking duration.
3. **Cross-Shard Transactions Use Explicit Locks**: A `MULTI`/`EXEC` block whose queued
   commands touch more than one shard acquires a distributed "VLL" (very-lightweight-locking)
   lock across every touched shard before running the batch, and releases it after — see §4.2.
4. **Squashing Is Gated on More Than Just Routability**: The pipeline-squashing fast path
   (`execute_commands_squashed`) additionally requires the client to already be authenticated,
   have real ACL permission for each command and its key, and (for keyed commands) that this
   node's cluster-gossip table actually confirms ownership of the key's slot — not merely that
   the slot's local `SlotState` is `Stable` — see §4.4.
5. **Non-Blocking Cross-Shard Dispatch**: Remote-shard work is always sent as a
   `ShardMessage::Batch` over pre-allocated `flume` channels and awaited without blocking the
   reactor thread — other connections on the same core keep making progress.

---

### 3. Component Architecture & Data Structures

```
                 Client TCP Stream
                        │
                        ▼
              handle_connection() loop
                        │
        ┌───────────────┼────────────────┬───────────────────┐
        ▼               ▼                ▼                   ▼
  SUBSCRIBE/       PSYNC seen?      MULTI/WATCH/       Blocking cmd
  PSUBSCRIBE?      → replica         EXEC active?       (BLPOP/…)?
  → run_pubsub_loop  stream mode     → transaction      → flush out_buf
    (mode switch,      (mode switch,   branch              first, then
    never returns)      never returns)  (§4.2)              execute
        │                                  │                   │
        └──────────────────┬───────────────┴───────────────────┘
                            ▼
              1 command? execute_command()
              N commands? execute_commands_squashed()
                            │
                ┌───────────┴────────────┐
                ▼                        ▼
         Local shard key           Remote shard key
     execute_local_command()   ShardMessage::Batch over
       direct on ShardDb        pre-allocated responder,
                                 awaited in parallel
                            │
                            ▼
                 out_buf (RESP2/RESP3/Memcached)
                            │
                            ▼
              one coalesced io_uring write_all
```

#### Real Per-Connection & Per-Shard-Port State

```rust
#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: Option<String>,
    pub connected_at: Instant,
    pub last_active: Instant,
    pub last_cmd: String,
    pub is_resp3: bool,
    pub track_tx: Option<flume::Sender<Vec<u8>>>,
    pub raw_fd: std::os::unix::io::RawFd,
}

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
}
```

There is no `ClientContext`/`ClientTxState`/`ClientProtocol` struct anywhere in the file —
per-connection transaction state (`in_multi`, `tx_queue`, `tx_has_error`) lives as plain locals
inside `handle_connection` instead of being grouped into a struct, and RESP3-vs-RESP2 mode is
tracked via the `CURRENT_CLIENT_RESP3` thread-local plus `ClientInfo.is_resp3`, not an enum.

Two `RwLock`-guarded static maps implement optimistic-locking `WATCH` and RESP3 client-side
caching invalidation, both keyed by `(port, client_id)` so state stays scoped to one shard's
listening port even though the maps are process-wide statics:

```rust
static WATCHED_KEYS: LazyLock<RwLock<HashMap<u16, HashMap<Bytes, HashSet<u64>>>>> = ...;
static CLIENT_WATCH_TAINTED: LazyLock<RwLock<HashMap<(u16, u64), bool>>> = ...;
static TRACKING_CLIENTS: LazyLock<RwLock<HashMap<(u16, u64), ClientTracker>>> = ...;
```

`ResponderChannel` is the pre-allocated, per-shard channel pool that
`execute_commands_squashed` reuses across every pipeline flush of a connection's lifetime:

```rust
pub type ResponderChannel = (
    flume::Sender<Vec<(usize, CompactResp)>>,
    flume::Receiver<Vec<(usize, CompactResp)>>,
);
```

(`CompactResp`, defined in `src/shard.rs`, has replaced the plain `Vec<u8>` response payload
used previously — a memory-compacted reply representation, not documented here since it
belongs to `src/shard.rs`.)

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `handle_connection`: a mode-switching read loop, not just read-parse-execute

The loop parses every complete command out of the buffer per read, then branches on **what
kind of pipeline it just parsed** before executing anything:

```rust
// 2. Transition to Pub/Sub mode if SUBSCRIBE or PSUBSCRIBE is received
if let Some(sub_idx) = commands.iter().position(|c| matches!(c, Command::Subscribe(_) | Command::Psubscribe(_))) {
    for c in commands.drain(..sub_idx) { execute_command(c, ...).await; }
    // flush, then...
    run_pubsub_loop(stream, client_id, client_registry, router, initial_sub, commands, buf).await;
    return; // this connection never returns to the normal loop
}

// 2.5 Transition to Replica Stream mode if PSYNC is received
if let Some(psync_idx) = commands.iter().position(|c| matches!(c, Command::Psync { .. })) {
    for c in commands.drain(..psync_idx) { execute_command(c, ...).await; }
    // flush, then...
    run_master_replica_stream(stream, client_id, client_registry, router, psync_cmd).await;
    return;
}
```

Once a connection issues `SUBSCRIBE` or `PSYNC`, `handle_connection` hands the socket off to a
dedicated loop (`run_pubsub_loop` or `run_master_replica_stream`) and never returns — those are
one-way mode switches for the lifetime of that TCP connection.

For ordinary traffic, the remaining branch picks one of four execution strategies per read,
in this priority order: **(a)** a transaction is open or this batch contains
`MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` → the transaction branch (§4.2); **(b)** the batch
contains a blocking command → execute sequentially, flushing `out_buf` immediately before each
blocking one (§4.3); **(c)** exactly one command → `execute_command` directly, no batch
bookkeeping; **(d)** more than one command, none blocking, no transaction control → attempt
`execute_commands_squashed` (§4.4).

#### 4.2 Transactions: `MULTI`/`EXEC`/`WATCH`, with real cross-shard locking

Queued commands are buffered per-connection (`tx_queue: Vec<Command>`); `WATCH` registers keys
in the `WATCHED_KEYS` static, and any write to a watched key anywhere flips
`CLIENT_WATCH_TAINTED` for that client (checked at `EXEC` time to abort optimistically, exactly
like Redis `WATCH` semantics). When `EXEC` actually runs the queue, if it spans more than one
shard it acquires an explicit cross-shard lock before executing any of the queued commands:

```rust
let mut shards = hashbrown::HashSet::new();
for cmd in &tx_queue {
    for k in cmd_keys(cmd) { shards.insert(router.target_shard(k)); }
}
let mut sorted_shards: Vec<usize> = shards.into_iter().collect();
sorted_shards.sort_unstable();

let use_vll = sorted_shards.len() > 1;
let tx_id = if use_vll {
    static NEXT_TX: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_TX.fetch_add(1, Ordering::Relaxed);
    router.acquire_tx_locks(&sorted_shards, id).await;
    id
} else { 0 };

let hub_arc = crate::block::get_block_hub_for_port(router.port);
hub_arc.lock().unwrap().pause();       // suppress blocking-op wakeups mid-transaction
IN_TX.set(true);
for q_cmd in queued { execute_command(q_cmd, ...).await; }
IN_TX.set(false);
if use_vll { router.release_tx_locks(&sorted_shards, tx_id).await; }
let pending = hub_arc.lock().unwrap().resume();  // replay any notifications that arrived mid-tx
```

Sorting `sorted_shards` before acquiring locks is a lock-ordering discipline to avoid
deadlocking against a concurrent transaction that touches an overlapping, differently-ordered
shard set. The `BlockHub` is explicitly paused for the transaction's duration and any
list/zset-push notifications that arrive while paused are queued and replayed afterward,
rather than waking a blocked client mid-transaction.

#### 4.3 Blocking commands: flush before you block

```rust
} else if commands.iter().any(|c| matches!(c, Command::Blpop { .. } | Command::Brpop { .. }
    | Command::Blmove { .. } | Command::Blmpop { .. } | Command::Bzpopmin { .. }
    | Command::Bzpopmax { .. } | Command::Bzmpop { .. }
    | Command::Xread { block_ms: Some(_), .. } | Command::Xreadgroup { block_ms: Some(_), .. })) {
    for cmd in commands {
        if matches!(cmd, /* same blocking set */) {
            if !out_buf.is_empty() {
                // flush everything buffered so far before this command can suspend the task
                let (res, returned_buf) = stream.write_all(write_chunk).await;
                ...
            }
        }
        execute_command(cmd, ...).await;
    }
}
```

Blocking commands are detected up front for the whole batch and executed strictly
sequentially (never squashed), each preceded by a flush of anything already queued in
`out_buf` — necessary because a blocking command can legitimately suspend the connection's
task for seconds while waiting on `BlockHub`, and a client shouldn't see its earlier pipelined
replies delayed by that wait.

#### 4.4 `execute_commands_squashed`: the squash gate now enforces ACL and real cluster ownership, and `MGET`/`MSET` are squashable again

The core squash-or-fallback mechanism is unchanged in shape (bucket local commands inline,
bucket remote commands per target shard, dispatch one `ShardMessage::Batch` per shard,
await all in parallel, reassemble in original order), but the eligibility check has grown
two real security/correctness gates beyond the original authentication-and-migration check:

```rust
let mut can_squash = *authenticated;
if can_squash {
    for cmd in &commands {
        if matches!(cmd, Command::Blpop { .. } | ... | Command::Watch(_) | Command::Unwatch) {
            can_squash = false;
            break;
        }
        {
            let acl = crate::acl::get_acl_for_port(router.port);
            let acl_guard = acl.read().unwrap();
            if let Some(user) = acl_guard.get_user(auth_user) {
                let cmd_name = get_cmd_name(cmd);
                if !user.can_execute_command(cmd_name) { can_squash = false; break; }
                if let Some(k) = cmd_primary_key(cmd)
                    && !user.can_access_key(k.as_ref())
                { can_squash = false; break; }
            }
        }
        if let Some(k) = cmd_primary_key(cmd) {
            let slot = key_slot(k);
            if router.slot_states.borrow()[slot as usize] != crate::shard::SlotState::Stable {
                can_squash = false;   // don't squash while this key's slot is migrating
                break;
            }
            let hub = crate::cluster::get_cluster_hub(router.port);
            let my_slots = hub.my_slots.read().unwrap();
            let owns_slot = my_slots.iter().any(|&(s, e)| slot >= s && slot <= e);
            let nodes = hub.nodes.read().unwrap();
            if !owns_slot && !nodes.is_empty() {
                can_squash = false;   // Stable locally, but cluster gossip says a peer node owns it
                break;
            }
        } else if !matches!(cmd, Command::Ping(_) | Command::CommandDocs | Command::Quit
            | Command::Time | Command::Echo(_) | Command::Mget(_) | Command::Mset(_)) {
            can_squash = false;
            break;
        }
    }
}
```

The `SlotState::Stable`-vs-not check alone was already present before this round of changes
and already correctly deferred squashing during `Migrating`/`Importing`/`Moved` (an earlier
version of this doc's Future Improvements section incorrectly flagged this as unfixed — it
wasn't; see §7). What's genuinely new is the **ACL check** (a squashed batch containing a
command the authenticated user can't run, or whose key they can't access, now correctly
falls back to sequential execution so `execute_command`'s real `-NOPERM` rejection applies
per-command — see §4.5) and the **cluster-gossip ownership check**: a slot can be locally
`Stable` (no migration in progress) while a multi-node cluster's gossip table says a *different
node* now owns it (e.g. after `CLUSTER SETSLOT ... NODE` on another node propagated via gossip)
— that case now also defeats squashing so the sequential fallback's real `-MOVED` check
(§4.5) fires, confirmed by `test_cluster_pipelined_squashed_moved_redirect_e2e` in
`tests/test_server_e2e.rs`.

**One narrow gap in this gate, verified**: for multi-key commands, `cmd_primary_key` returns
only the *first* key (`keys.first()`/`pairs.first()`) — so a squash-eligibility check for an
`MGET`/`MSET`/`SINTER`/etc. spanning several keys validates ACL/slot-state/ownership against
only that first key, not every key in the command. A command whose first key is permitted and
stable but whose second key is ACL-forbidden or mid-migration could still be judged
squash-eligible on this check alone (see §4.6 for how that plays out for `MGET`/`MSET`
specifically once inside the squash path).

`MGET`/`MSET` are now explicitly allowed through the eligibility gate (added to the
`Ping`/`CommandDocs`/... safe list above, since `target_shard_of_cmd` still returns `None`
for them — they have no single target shard). The all-or-nothing squash-defeat behavior for
everything *not* on this expanding safe/eligible list is otherwise unchanged: one ineligible
command in a pipeline still forces the whole pipeline to the sequential fallback.

Inside the squash path, `GET` gets a special inline case for transparent NVMe-tiered reads
(the auto-tiering system can move cold values to disk):

```rust
if let Command::Get(ref key) = cmd {
    let val = router.local_db.borrow_mut().get(key);
    if let Some(v) = val {
        write_resp_bulk(&mut local_buf, &v);
    } else if router.local_db.borrow_mut().table.is_tiered(key).is_some() {
        if let Some(v) = router.stream_cold_read_local(key).await {
            write_resp_bulk(&mut local_buf, &v);
        } else { local_buf.extend_from_slice(b"$-1\r\n"); }
    } else { local_buf.extend_from_slice(b"$-1\r\n"); }
}
```

and any batch containing a local `SET`/`DEL`/`IncrBy` spawns a fire-and-forget background
task afterward to check whether auto-tiering should kick in under memory pressure:

```rust
if has_local_writes {
    let r = router.clone();
    monoio::spawn(async move { r.check_auto_tier().await; });
}
```

#### 4.5 `execute_command`: auth, `ASKING`/`MOVED`/migration redirection, then dispatch

Every single-command execution (squashed or not) runs through the same gate sequence before
reaching its command-specific match arm:

```rust
if !*authenticated && !matches!(cmd, Command::Auth { .. } | Command::Hello { .. } | Command::Quit) {
    out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
    return false;
}

if *authenticated {
    let acl = crate::acl::get_acl_for_port(router.port);
    let acl_guard = acl.read().unwrap();
    if let Some(user) = acl_guard.get_user(auth_user) {
        if !user.can_execute_command(cmd_name) {
            out.extend_from_slice(format!(
                "-NOPERM this user has no permissions to run the '{}' command\r\n",
                cmd_name.to_lowercase()).as_bytes());
            return false;
        }
        if let Some(key) = cmd_primary_key(&cmd) && !user.can_access_key(key.as_ref()) {
            out.extend_from_slice(b"-NOPERM this user has no permissions to access one of the keys used as arguments\r\n");
            return false;
        }
    }
}

if let Command::Asking = cmd { *asking = true; out.extend_from_slice(b"+OK\r\n"); return false; }
let is_asking = *asking;
*asking = false;

if let Some(key) = cmd_primary_key(&cmd) {
    let slot = key_slot(key);
    match router.slot_states.borrow()[slot as usize].clone() {
        SlotState::Moved(target) => { /* -MOVED slot target */ return false; }
        SlotState::Importing(source) => { if !is_asking { /* -MOVED slot source */ return false; } }
        SlotState::Migrating(target) => {
            if !router.exists(key.clone()).await { /* -ASK slot target */ return false; }
        }
        SlotState::Stable => { /* check this node still owns the slot per cluster gossip, else -MOVED */ }
    }
}
if crate::replication::get_replication_hub(router.port).is_slave()
    && crate::aof::command_to_resp(&cmd).is_some() {
    out.extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
    return false;
}
```

This is a genuine, working implementation of Redis ACL authorization (`-NOPERM` for both
disallowed commands and disallowed keys, backed by real per-user command/key rules — see
Component 15 for `AclUser`'s own real permission model), the Redis Cluster live-migration
protocol (`MOVED`/`ASK`/`ASKING`, four-state `SlotState`), and replica-side write rejection —
not placeholders. `execute_command`'s command-specific match arm that follows is roughly 3,900
lines covering the full command surface; `execute_local_command` (the shard-local,
synchronous counterpart called both for genuinely-local keys and for remote batches arriving
via `ShardMessage::Batch`) is a similarly-sized ~3,750-line match. Both are too large to
usefully excerpt command-by-command here — the mechanism worth understanding is that they
are two independent match statements over the same `Command` enum kept in sync by hand (see
`docs/designs/components.md` Part 3 §7 for why that duplication exists rather than a single
shared function — that rationale still holds structurally, though the functions themselves
have grown far beyond what that doc shows).

#### 4.6 `MGET`/`MSET` now genuinely fan out in parallel — real bucketing, a pooled channel set, and a spin-then-block harvest

**This closes the gap this doc previously flagged.** `execute_command`'s `Mget`/`Mset` arms
now delegate to real `Router::mget`/`Router::mset` methods in `src/router.rs` (Component 04)
instead of looping `router.get`/`router.set` per key:

```rust
Command::Mget(keys) => {
    for key in &keys { record_client_read(router.port, client_id, key.as_ref()); }
    let values = router.mget(keys).await;
    write_resp_array_header(out, values.len());
    for val in values { /* write bulk or null per value, in original key order */ }
    false
}
```

`Router::mget`/`mset` (verified in `src/router.rs`, summarized here since the mechanism is
architecturally significant to how this file's commands behave — full detail belongs to
Component 04):
1. **Fast path**: if `num_shards <= 1`, every key is local — no channel involved at all.
2. **Partition by slot using `slot_owners`** (the *dynamic*, migration-aware per-slot
   ownership override — not the static `target_shard` function most other commands use),
   without holding the `RefCell` borrow across an `.await` point.
3. **Local keys execute immediately**, no I/O. If every key turned out to be local, return
   immediately — zero channel operations for an all-local `MGET`/`MSET`.
4. **Remote keys are bucketed one `Vec` per shard** and dispatched as single
   `ShardMessage::Mget`/`Mset` messages (new variants added to `src/shard.rs`'s
   `ShardMessage` enum in this change) over a **pooled, reusable channel set**
   (`acquire_mget_channels`/`release_mget_channels`, backed by `Rc<RefCell<Vec<Vec<MgetChannel>>>>`)
   — not a fresh one-shot channel per call, closing the "Router's per-op methods allocate a
   fresh channel" gap this doc's Future Improvements once raised for the single-command path
   specifically for these two commands.
5. **Harvesting is spin-then-block, not an immediate `.await`**: after sending, up to 128
   iterations of a tight loop non-blockingly `try_recv()`s every still-pending shard's
   channel (with `std::hint::spin_loop()` between sweeps), only falling back to a real
   `rx.recv_async().await` per still-outstanding shard if 128 spins weren't enough. On a
   busy multi-core machine, a cross-shard hop often completes within microseconds — faster
   than a scheduler round-trip would take — so this trades a bounded amount of CPU spinning
   for avoiding that round-trip in the common case.

**Two real, verified gaps in this new mechanism, not present in the old sequential code
(which had no such edge cases because it never batched):**
- **A `> 64` shard correctness bug.** `sent_mask`/`remaining_mask` are `u64` bitmasks, and
  both the send loop and both harvest loops (spin and fallback) gate every bit operation on
  `target_shard < 64`. The *send* to a shard `>= 64` still happens
  (`self.senders[target_shard].send(msg)` is unconditional), but that shard's bit is never
  set in `sent_mask`, so **its response is never collected in either harvest loop** — the
  channel holding that reply is returned to the pool with an unread message still in it, and
  every key that was routed to shard 64+ silently comes back as `None`/not-set in the
  `MGET`/`MSET` reply, even though the key exists. `acquire_mget_channels` itself allocates
  `self.num_shards` channels (not capped at 64), so this isn't an intentional scale limit —
  it's a latent bug that only manifests on a deployment with more than 64 shards (`--threads`
  above 64), which the default `num_cores.min(8)` and typical benchmark configurations never
  exercise.
- **Only the first key of a multi-key `MGET`/`MSET` is checked for squash-eligibility**
  (§4.4's noted gap) — since `router.mget`/`mset` themselves are called either via the
  sequential fallback (`execute_command`, which does its own ACL/slot check but — per the
  code shown above — only against `cmd_primary_key`, i.e. the *first* key, before calling
  `router.mget`) or via the squash path's special case (§4.4), no per-key ACL/slot-state
  check happens inside `Router::mget`/`mset` itself for keys beyond the first. A client with
  access to an `MGET`'s first key but not its second key would not be rejected by anything
  observed in this file.

#### 4.7 `format_score`: exact `%.17g` compatibility, verified accurate

```rust
#[inline]
pub fn format_score(val: f64) -> String {
    if val.is_nan() { "nan".to_string() }
    else if val.is_infinite() { if val.is_sign_positive() { "inf".to_string() } else { "-inf".to_string() } }
    else if val == 0.0 { "0".to_string() } // Prevents "-0" for negative zero
    else {
        let mut buf = [0u8; 64];
        let len = unsafe {
            libc::snprintf(buf.as_mut_ptr() as *mut libc::c_char, buf.len(),
                b"%.17g\0".as_ptr() as *const libc::c_char, val)
        };
        if len > 0 && (len as usize) < buf.len() {
            unsafe { std::str::from_utf8_unchecked(&buf[..len as usize]) }.to_string()
        } else { val.to_string() }
    }
}
```

This one is genuinely as previously documented: sorted-set scores are formatted via a direct
`libc::snprintf(..., "%.17g", ...)` FFI call specifically to match the exact string Redis's C
implementation would produce (double-to-string conversion is not guaranteed bit-identical
across languages' default formatters), which matters for passing the vendored official Redis
test suite (`tests/`) byte-for-byte. `write_resp_score` (also in this file) picks between this
and RESP3's native `,<double>\r\n` double type depending on `CURRENT_CLIENT_RESP3`.

---

### 5. Cross-Component Interactions

- **`src/resp.rs`**: Supplies `parse_command`, decoding buffered bytes into `Command` values.
- **`src/router.rs`**: `Router` provides `target_shard`/`key_slot`-based local/remote
  decisions, the `senders` mesh, `slot_states` (cluster migration state), `slot_owners`
  (the dynamic ownership override `mget`/`mset` route by — see §4.6), the pooled
  `mget`/`mset` channel sets, and `stream_cold_read_local`/`check_auto_tier`
  (tiered-storage integration).
- **`src/table.rs`** / **`src/shard.rs`**: `execute_local_command` mutates `ShardDb`/`RudisTable`
  directly; `CompactResp` (defined in `shard.rs`) is the reply payload type carried through
  `ShardMessage::Batch`.
- **`src/block.rs`**: `BlockHub` (`get_block_hub_for_port`) — registration/wakeup for
  `BLPOP`/`BZPOPMIN`/blocking `XREAD`, and the pause/resume mechanism transactions use.
- **`src/pubsub.rs`**: `PubSubHub`, entered via `run_pubsub_loop` on `SUBSCRIBE`/`PSUBSCRIBE`.
- **`src/replication.rs`**: replica-stream mode (`run_master_replica_stream`) and the
  `is_slave()` write-rejection check.
- **`src/cluster.rs`**: `get_cluster_hub` supplies the gossiped slot-ownership table consulted
  during `MOVED` redirection.
- **`src/acl.rs`** (Component 15): `authenticated`/`auth_user` are checked against
  `get_acl_for_port` at connection start and on every `AUTH`; `execute_command` and the
  squash-eligibility gate (§4.4/§4.5) both now also enforce real per-command/per-key
  authorization (`-NOPERM`) via `AclUser::can_execute_command`/`can_access_key`, not just
  the initial authentication check.
- **`src/aof.rs`**: `execute_local_command` takes an `Option<&RefCell<AofWriter>>` to append
  write commands for persistence; `command_to_resp` is also reused to detect "is this command
  a write" for the replica read-only guard.

---

### 6. Performance Characteristics

- **Pre-allocated responder pool, not one-shot channels**: `ResponderChannel`s are built once
  per connection (`(0..router.num_shards).map(|_| flume::bounded(1))`) and reused for every
  pipeline flush — still the mechanism behind zero-allocation steady-state cross-shard fan-out.
- **`CompactResp` replies**: batched remote responses are carried as `CompactResp` rather than
  a plain `Vec<u8>`, reducing per-reply allocation/copy overhead in the squashed path.
- **Command-name lowercasing/stat tracking is not free**: every executed command updates a
  global `CMD_STATS` map (`record_cmd_stat`) under a `RwLock`, plus per-client `last_cmd`
  bookkeeping — real but modest fixed overhead paid on every command, not just squashed
  batches.
- **`MGET`/`MSET` now genuinely parallelize across shards** (§4.6) via pooled channels and a
  spin-then-block harvest, closing what was previously the single biggest cost on multi-shard
  multi-key workloads — bounded by the slowest remote shard's response time now, not by the
  sum of every remote shard's response time.
- **The spin-then-block harvest trades CPU for latency, with a fairness cost worth naming**:
  up to 128 non-blocking `try_recv` sweeps (`std::hint::spin_loop()` between them) happen
  *without yielding to `monoio`'s cooperative scheduler* — on this shared-nothing,
  single-threaded-per-core design, that means other connections' tasks on the *same core*
  make no progress while an `MGET`/`MSET` is in its spin phase. Fine when remote shards
  reply within microseconds (the common case this was tuned for); a burst of concurrent
  `MGET`/`MSET` calls each waiting on a genuinely slow remote shard could measurably delay
  unrelated connections sharing that core until the 128-iteration cap is hit and each falls
  back to a real `.await`.

---

### 7. Future Improvements

- ~~High — fix `MGET`/`MSET` to bucket-and-fan-out instead of one round-trip per key.~~ **Resolved.** `Router::mget`/`mset` (§4.6) now bucket by shard via `slot_owners`, dispatch via pooled channels, and harvest with a spin-then-block loop. Replaced by two new findings below, both verified in the shipped fan-out code itself.
- **High — fix the `> 64`-shard `MGET`/`MSET` response-collection bug (§4.6).** `sent_mask`/`remaining_mask` are `u64` bitmasks that silently exclude any `target_shard >= 64` from both harvest loops, even though the request is still sent and `acquire_mget_channels` allocates a full `num_shards`-sized channel set. On a deployment with more than 64 shards, every key routed to shard 64+ comes back as a false negative (`None`/missing) in the `MGET`/`MSET` reply. Fix: use a `Vec<bool>`/bitset sized to `num_shards` (or chunk the bitmask across multiple `u64`s) instead of a single fixed-width `u64`.
- **Medium — check every key of a multi-key `MGET`/`MSET`/`SINTER`-style command for ACL/slot-state eligibility, not just the first (§4.4/§4.6).** `cmd_primary_key` deliberately returns one key for routing purposes, but reusing it for squash-eligibility and for `execute_command`'s ACL/slot gate means only that first key is actually checked. A command spanning a permitted-and-stable first key plus a forbidden-or-migrating second key currently isn't rejected by anything in this file. Fix: for known multi-key commands, iterate all their keys in the eligibility/ACL/slot checks rather than delegating to `cmd_primary_key` alone.
- ~~High — close the correctness gap where the squashed fast path skips slot-migration redirection.~~ **Turned out already handled** for the `Migrating`/`Importing`/`Moved` states — that `SlotState::Stable`-vs-not check predates this round of changes. What genuinely *was* missing and has now been added: the `Stable`-but-a-different-node-owns-it-per-gossip case (§4.4), confirmed fixed by `test_cluster_pipelined_squashed_moved_redirect_e2e`.
- **Medium — de-risk `execute_command`/`execute_local_command`'s ~3,900/~3,750-line hand-synced duplication (§4.5).** Two independent `match` statements over the same `Command` enum, kept in sync by hand, is exactly the kind of surface where a new command variant gets full local semantics but is forgotten in the remote-batch arm (or vice versa). A macro that generates both arms from one command-behavior definition, or at minimum a `cargo test` that asserts both matches are exhaustive over the same variant set, would catch that class of bug before it reaches production.
- **Medium — replace the two-fresh-`Lua::new()`-per-`FCALL` pattern's blast radius on this file's dispatch cost.** Not this file's bug directly (Component 13 owns it), but every `Fcall`/`Eval`/`Evalsha` arm here pays for it; consider whether `connection.rs`'s command-dispatch layer should expose a lightweight "is this a scripting command" fast-path hint so future caching work in `scripting.rs` doesn't require touching this file's dispatch tables.
- **Low — give `CMD_STATS`/`record_cmd_stat` a per-shard-then-aggregate design instead of one global `RwLock`ed map (§6).** Currently modest overhead, but as command volume grows this is one more process-wide lock on the per-command hot path alongside `BlockHub`/ACL/search/scripting — consolidating or sharding it would keep the "how many process-wide locks exist" count from growing unnoticed.

---
---

## Component 03: RESP Protocol Engine & Command Parser (`src/resp.rs`)

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

### 3. Command Surface & Data Structures

#### 3.1 What `parse_command` actually recognizes

```text
First byte        Grammar                                   Handler
──────────────────────────────────────────────────────────────────────────────
'*'               RESP array: *N\r\n($len\r\ndata\r\n){N}    parse_resp_array
otherwise         Try Memcached "set/add/replace key         parse_memcached_storage_command
                  flags exptime bytes [noreply]\r\n<data>\r\n"
otherwise         Space/tab-separated inline text             parse_inline_command
```

There is no separate frame type for simple strings (`+`), errors (`-`), or integers (`:`) on
the *input* side — those are reply-only prefixes written by `connection.rs`, never parsed
here, because a client never sends a command framed that way.

#### 3.2 The `Command` enum: real shape, real category breakdown

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Auth { username: Option<String>, password: String },
    Acl(AclSubcommand),
    Get(Bytes),
    Getex { key: Bytes, expire_in: Option<Duration>, persist: bool },
    Set {
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        condition: SetCondition,   // None | Nx | Xx | Ifeq(Bytes) | Ifne(Bytes) | Ifdeq(Bytes) | Ifdne(Bytes)
        get: bool,
        keepttl: bool,
        past_expired: bool,
    },
    Mget(Vec<Bytes>),
    Mset(Vec<(Bytes, Bytes)>),
    // ... hundreds more variants, grouped by the file's own section comments:
    // LIST, SET, ZSET, GENERIC & DATABASE, EXTENDED STRING, LUA SCRIPTING,
    // TIERED STORAGE, CONFIG, PUBSUB, KEYSPACE INSPECTION, TRANSACTIONS,
    // BITMAP, HYPERLOGLOG, RDB SERIALIZATION, STREAM, VALKEY EXTENDED,
    // VECTOR, CRDT MULTI-REGION, REDIS 7 FUNCTIONS, REDISJSON, GEOSPATIAL,
    // PROBABILISTIC, FT.* (full-text search), XDP.* (AF_XDP/eBPF control),
    // Dragonfly native extensions, and a Memcached Protocol block:
    MemcachedSet { key: Bytes, flags: u32, exptime: u32, bytes: usize, noreply: bool, data: Bytes },
    MemcachedGet { keys: Vec<Bytes> },
    MemcachedDelete { key: Bytes, noreply: bool },
    MemcachedIncr { key: Bytes, value: u64, noreply: bool },
    MemcachedStats,
    Unknown(String),
}
```

Note the derive is `PartialEq, Clone` — **not `Eq`** — because several variants (`Zadd`,
`Zincrby`, score-range queries, `Set`'s `past_expired` semantics, etc.) carry `f64` fields,
and `f64` has no total order (`NaN != NaN`), so `Eq` can't be derived. This is a real
constraint of the type, not an oversight.

The enum's category list above comes directly from the file's own `// SECTION NAME` comments
(`grep -n "^    // [A-Z]" src/resp.rs`), which is the fastest way to get an up-to-date map of
what's supported without reading all ~1,000 lines of variant declarations.

---

### 4. Parsing Algorithms & Code Logic

#### 4.1 `parse_command`: three grammars, tried in a fixed order

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

`parse_memcached_storage_command` returns a triple-layered result deliberately: `Ok(None)`
means "this isn't a memcached storage command at all, try inline parsing next";
`Ok(Some(None))` means "it *is* one, but the data block hasn't fully arrived yet — wait for
more bytes, don't fall through to inline parsing"; `Ok(Some(Some(cmd)))` is a complete parse.
That extra `Option` layer exists specifically to prevent a partially-received memcached
`set key 0 0 1024\r\n<...only 200 bytes so far...>` from being misinterpreted as inline text.

```rust
fn parse_memcached_storage_command(buf: &mut BytesMut) -> Result<Option<Option<Command>>, String> {
    let newline_pos = match find_crlf(buf) { Some(pos) => pos, None => return Ok(None) };
    let line = &buf[..newline_pos];
    let first_space = match line.iter().position(|&b| b == b' ' || b == b'\t') {
        Some(p) => p, None => return Ok(None),
    };
    let first_word = &line[..first_space];
    let is_set = first_word.eq_ignore_ascii_case(b"set");
    let is_add = first_word.eq_ignore_ascii_case(b"add");
    let is_replace = first_word.eq_ignore_ascii_case(b"replace");
    if !is_set && !is_add && !is_replace {
        return Ok(None);
    }
    // ... parse key/flags/exptime/bytes/noreply from the header line ...
    let total_len = newline_pos + 2 + bytes_len + 2;
    if buf.len() < total_len {
        return Ok(Some(None)); // header parsed, but data block hasn't fully arrived
    }
    if &buf[newline_pos + 2 + bytes_len .. total_len] != b"\r\n" {
        return Err("CLIENT_ERROR bad data chunk".to_string());
    }
    // ... build Command::MemcachedSet/Add/Replace, buf.advance(total_len) ...
}
```

#### 4.2 `parse_resp_array`: unchanged two-pass zero-copy scan

This is verbatim the same algorithm as the original design: scan the whole frame for
completeness first, without mutating `buf`, then — only once the entire frame is known to be
present — make a second pass that actually consumes bytes and slices out zero-copy `Bytes`
arguments.

```rust
fn parse_resp_array(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    let newline_pos = match find_crlf(buf) { Some(pos) => pos, None => return Ok(None) };
    let line = &buf[1..newline_pos];
    let num_args: usize = match std::str::from_utf8(line).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return Err("Invalid array length in RESP frame".to_string()),
    };

    // Pass 1: prove the whole frame is present without consuming anything.
    let mut scan_cursor = newline_pos + 2;
    for _ in 0..num_args {
        if scan_cursor >= buf.len() { return Ok(None); }
        if buf[scan_cursor] != b'$' {
            return Err("Expected bulk string in command array".to_string());
        }
        let next_crlf = match find_crlf_at(buf, scan_cursor) { Some(p) => p, None => return Ok(None) };
        let arg_len: usize = std::str::from_utf8(&buf[scan_cursor + 1..next_crlf])
            .ok().and_then(|s| s.parse().ok())
            .ok_or_else(|| "Invalid bulk string length".to_string())?;
        let data_end = next_crlf + 2 + arg_len;
        if data_end + 2 > buf.len() { return Ok(None); }
        if &buf[data_end..data_end + 2] != b"\r\n" {
            return Err("Expected CRLF after bulk string data".to_string());
        }
        scan_cursor = data_end + 2;
    }

    // Pass 2: frame confirmed complete — now actually consume and zero-copy slice.
    buf.advance(newline_pos + 2);
    let mut args = Vec::with_capacity(num_args);
    for _ in 0..num_args {
        let header_crlf = find_crlf(buf).unwrap();
        let arg_len: usize = std::str::from_utf8(&buf[1..header_crlf]).unwrap().parse().unwrap();
        buf.advance(header_crlf + 2);
        let data = buf.split_to(arg_len).freeze(); // zero-copy
        buf.advance(2);
        args.push(data);
    }
    build_command(args)
}
```

`parse_inline_command` is also unchanged: it splits on spaces/tabs and builds each argument
with `Bytes::copy_from_slice` — a real copy, not zero-copy, because this path only serves
interactive/debugging clients (`redis-cli`, `nc`), never the benchmarked pipelined workload.

#### 4.3 `build_command`: one shared constructor, ~5,500 lines, dispatched by uppercased name

```rust
pub fn build_command(args: Vec<Bytes>) -> Result<Option<Command>, String> {
    if args.is_empty() { return Ok(None); }
    let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
    match cmd_name.as_str() {
        "GET" => {
            if args.len() < 2 {
                return Err("wrong number of arguments for 'get' command".to_string());
            }
            if args.len() == 2 {
                Ok(Some(Command::Get(args[1].clone())))
            } else {
                // "get k1 k2 k3" is not valid Redis GET arity — treat it as a
                // Memcached multi-key get instead of rejecting it outright.
                Ok(Some(Command::MemcachedGet { keys: args[1..].to_vec() }))
            }
        }
        // ...
        _ => Ok(Some(Command::Unknown(cmd_name))),
    }
}
```

This `GET`/`MemcachedGet` disambiguation-by-arity is a good example of how the Redis and
Memcached surfaces share one dispatch table without a protocol-mode flag: since real Redis
`GET` is strictly single-key, any extra arguments unambiguously mean the memcached dialect
was intended instead. `DEL`/`DELETE` similarly resolve to `Command::Del` or
`Command::MemcachedDelete` depending on which spelling arrived.

Options are parsed with the same index-walking `while i < args.len()` loop over uppercased
option tokens the original design used for `SET ... EX ...`, now carrying more flags. `SET`
itself has grown Dragonfly-style conditional variants beyond plain `NX`/`XX`:

```rust
"IFEQ" => {
    if condition != SetCondition::None || i + 1 >= args.len() { return Err("syntax error".to_string()); }
    condition = SetCondition::Ifeq(args[i + 1].clone()); // set only if current value == this
    i += 2;
}
```

#### 4.4 `HELLO`: parsed here, acted on in `connection.rs`

```rust
"HELLO" => {
    let mut proto = None;
    // first arg, if a bare integer, is the requested protocol version
    if let Ok(p) = String::from_utf8_lossy(&args[1]).parse::<u8>() { proto = Some(p); ... }
    // then AUTH user pass / SETNAME name in any order
    Ok(Some(Command::Hello { proto, auth, setname }))
}
```

`resp.rs` only produces the `Command::Hello { proto, .. }` value. It's `connection.rs` that
inspects `proto` and flips the client's `is_resp3` flag and the `CURRENT_CLIENT_RESP3`
thread-local, which is what actually changes reply formatting afterward.

#### 4.5 `parse_redis_f64`: libc `strtod` as a fallback for exact Redis float parsing

Sorted-set scores need to accept exactly the float literals real Redis accepts (`inf`,
`+inf`, `-inf`, `infinity`, values Rust's `f64::from_str` is stricter about). Rather than
reimplementing C's `strtod` parsing rules by hand, this file falls back to the real libc
function via FFI when Rust's own parser doesn't accept the input:

```rust
pub fn parse_redis_f64(s: &str) -> Option<f64> {
    if let Ok(v) = s.parse::<f64>() { /* fast path */ return Some(v); }
    if let Ok(c_str) = std::ffi::CString::new(s) {
        unsafe {
            let mut end: *mut libc::c_char = std::ptr::null_mut();
            let val = libc::strtod(c_str.as_ptr(), &mut end);
            if !end.is_null() && *end == 0 && end != c_str.as_ptr() as *mut libc::c_char {
                return Some(val);
            }
        }
    }
    None
}
```

`parse_score_bound` builds on this to handle `ZRANGEBYSCORE`-style `(exclusive` prefixes on
top of the same float grammar.

#### 4.6 `find_crlf`/`find_crlf_at`: still a plain windowed scan

Unchanged from the original design — a `.windows(2).position(|w| w == b"\r\n")` scan. It's
only ever used to find the end of small protocol headers (array lengths, bulk-string length
prefixes), never to scan payload data, so there's no SIMD opportunity being left on the table
here (see the storage engine's SIMD control-byte matching in Part 1 of
`docs/designs/components.md` for where that technique actually applies, on 16-byte groups).

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: The sole consumer of `parse_command`. All reply serialization
  (RESP2 and RESP3) happens there, not in this file.
- **`src/table.rs`**: Several `Command` variant fields carry types defined in `table.rs`
  directly (e.g. `Zadd`'s `flags: crate::table::ZAddFlags`, `Zrange`'s
  `opts: crate::table::ZRangeOpts`, stream commands' `crate::table::StreamId`), so this file
  and the storage engine share vocabulary rather than each redefining it.
- **`src/block.rs`**: `ClientSubcommand::Unblock` carries a `crate::block::ClientUnblockType`
  defined in the blocking-operations module.

---

### 6. Performance Characteristics

- **Zero-copy on the hot (RESP array) path**: every bulk-string argument is a `Bytes` slice
  sharing the original read buffer's allocation, not a fresh heap copy.
- **The inline and Memcached paths copy**: both exist for compatibility/interactive use, not
  throughput, and neither is on the benchmarked pipeline path.
- **`build_command`'s dispatch is a single string match, not a lookup table**: with 200+ arms,
  this is a large `match` the compiler is left to optimize (typically into some mix of
  length-bucketed comparisons/jump tables); no bespoke perfect-hash or trie dispatch was
  built for it.

---

### 7. Future Improvements

- **High — enforce a maximum bulk-string/array length (§8's "No maximum frame/argument size enforcement").** `parse_resp_array` trusts `arg_len` straight off the wire with no upper bound, so a client claiming a multi-gigabyte bulk string makes the server attempt to buffer that much data before giving up. A `proto-max-bulk-len`-equivalent check (reject the frame early if the declared length exceeds a configurable cap) is a small, high-value change given this is the very first thing untrusted input touches.
- **Medium — split `build_command`'s ~5,500-line single match into per-family functions** (strings, hashes, lists, sets, zsets, streams, cluster/ACL, scripting, vector/search, tiering, Memcached, ...), dispatched from a smaller top-level match on a coarse prefix or category lookup. Purely a maintainability change (§6 already notes the compiler handles the current single match fine performance-wise) — the real motivation is that a 5,500-line function is a place bugs hide, not a place they're compiled away.
- **Medium — reconcile the `GET`-vs-`MemcachedGet` and `DEL`-vs-`MemcachedDelete` arity-based disambiguation (§4.3) with a config flag.** Silently reinterpreting `GET k1 k2` as a Memcached multi-get is convenient for dual-protocol support but means a genuine Redis client typo (`GET` with accidentally-extra arguments) gets a different error message than real Redis would give, which could confuse debugging. Consider gating the Memcached-arity fallback behind an explicit "Memcached gateway enabled" flag so pure-Redis deployments get real Redis-compatible arity errors.
- **Low — add RESP3 *input* parsing** (maps `%`, sets `~`, doubles `,`, booleans `#`, nulls `_`) if any planned feature needs a client to send a RESP3-typed argument rather than only receive RESP3-typed replies (§2.4) — not needed today since every real Redis command is still sent as a flat bulk-string array, but worth flagging as the one genuine protocol-completeness gap versus a full RESP3 implementation.

---
---

## Component 04: Sharding Architecture & Cross-Core Mesh (`src/router.rs`, `src/shard.rs`)

### 1. Architectural Purpose & Scope

`src/router.rs` and `src/shard.rs` implement Rudis's data partitioning and inter-thread
messaging system. `router.rs` defines the `Router` struct — the per-shard facade every
command goes through to decide "is this key mine, or do I need to hop to a peer core" —
plus CRC16-based key-to-slot-to-shard mapping. `shard.rs` defines the thread-local
`ShardDb` (the actual per-core state container: `RudisTable` plus every other per-shard
subsystem — tiering, vector search, CRDTs, JSON, probabilistic structures, sticky-key
pinning) and the `ShardMessage` enum that is the entire cross-core wire format.

Beyond routing, `Router` has grown into the coordination point for nearly every
multi-shard concern in the codebase: NVMe tiering orchestration, RDB snapshotting,
AOF fsync fan-out, pub/sub broadcast, cross-shard transaction locking, cluster
administration passthroughs, and replication-stream application. Each of these is
documented below because they all live in this file, even though several belong more to
persistence/replication/tiering conceptually.

---

### 2. Key Invariants & Concurrency Constraints

1. **Deterministic Key Ownership (static formula)**: `target_shard(key, num_shards)` maps
   every key to exactly one shard via `crc16(hash_tag(key)) % 16384` → contiguous slot
   range → shard index. This is unchanged from the original design and is what nearly
   every command actually routes through (see §4.4 for the caveat).
2. **Lock-Free Asynchronous Mesh**: cross-shard communication is exclusively `flume`
   channels (unbounded senders held by every shard, one per-shard receiver drained in
   that shard's own event loop). No shared memory, no mutex, no `oneshot` crate — despite
   what an earlier, inaccurate draft of this document claimed.
3. **Hash Tag Compatibility**: `extract_hash_tag` — only the substring between the first
   `{` and the next non-empty `}` is hashed, so `{user:100}:profile` and
   `{user:100}:orders` land on the same shard.
4. **`Router` is `Clone`, not a singleton reference**: cloning just bumps `Rc`/`Arc`
   refcounts on its fields (see §3) — cheap, and necessary because async tasks spawned
   off a `Router` method (e.g. `bgsave`'s background save) need an owned copy to move into
   `monoio::spawn`.

---

### 3. Component Architecture & Data Structures

```
                        Client Request (Any Thread)
                                     │
                                     ▼
                     extract_hash_tag + CRC16 → slot (0..16383)
                                     │
                          slot_to_shard(slot, num_shards)
                             (contiguous range, NOT modulo)
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
              Local Shard?                      Remote Shard?
                    │                                 │
       Direct ShardDb mutation             ShardMessage over flume::Sender
     (+ AOF append, + tiering hooks)                   │
                                                        ▼
                                          Peer shard's ShardMessage receive
                                          loop executes it against its own
                                          ShardDb, replies via the message's
                                          own `responder: flume::Sender<T>`
```

#### `Router` (`src/router.rs`)

```rust
#[derive(Clone)]
pub struct Router {
    pub shard_id: usize,
    pub num_shards: usize,
    pub port: u16,
    pub local_db: Rc<RefCell<ShardDb>>,
    pub senders: Vec<flume::Sender<ShardMessage>>,
    pub slot_states: Rc<RefCell<Vec<crate::shard::SlotState>>>,
    pub slot_owners: Rc<RefCell<Vec<usize>>>,
    pub aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
    pub pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
    pub tx_lock: Rc<RefCell<Option<u64>>>,
    pub tx_waiters: Rc<RefCell<std::collections::VecDeque<(u64, flume::Sender<()>)>>>,
    pub is_saving: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub last_save_time: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub db_dir: std::path::PathBuf,
    pub is_auto_tiering: Rc<Cell<bool>>,
}
```

Every field beyond `shard_id`/`num_shards`/`port`/`local_db`/`senders` (the original
five) was added for a later feature: `slot_states`/`slot_owners` for live cluster slot
migration (§4.4), `aof` for write-ahead logging, `pubsub` for cross-shard channel
fan-out, `tx_lock`/`tx_waiters` for cross-shard `MULTI`/`EXEC` mutual exclusion,
`is_saving`/`last_save_time`/`db_dir` for RDB snapshotting, `is_auto_tiering` to prevent
re-entrant auto-tiering triggers.

#### `ShardMessage` (`src/shard.rs`) — the real wire format

The enum has grown to roughly 40 variants (up from the original 12). Representative
slice, showing the shape that matters most — `Batch`:

```rust
pub enum ShardMessage {
    Get { key: Bytes, responder: flume::Sender<Option<Bytes>> },
    Set { key: Bytes, value: Bytes, expire_in: Option<Duration>, responder: flume::Sender<()> },
    Batch {
        items: Vec<(usize, Command)>,
        responder: flume::Sender<Vec<(usize, CompactResp)>>,
        is_resp3: bool,
    },
    NotifyList { keys: Vec<Bytes> },
    SetSlotState { slot: u16, state: SlotState },
    SetSlotOwner { slot: u16, owner: usize },
    TierSpill { key: Bytes, responder: flume::Sender<bool> },
    TierLoad { key: Bytes, responder: flume::Sender<bool> },
    TierCool { key: Bytes, responder: flume::Sender<bool> },
    TierDecommit { key: Option<Bytes>, responder: flume::Sender<usize> },
    SyncAof { responder: flume::Sender<()> },
    SaveRdbChunk { responder: flume::Sender<Vec<u8>> },
    RestoreRdbChunk { data: Bytes, responder: flume::Sender<()> },
    Publish { channel: Bytes, message: Bytes, responder: flume::Sender<usize> },
    AcquireTxLock { tx_id: u64, responder: flume::Sender<()> },
    ReleaseTxLock { tx_id: u64 },
    ExecuteReplicaCmd { cmd: Command, responder: flume::Sender<()> },
    Stick { keys: Vec<Bytes>, responder: flume::Sender<usize> },
    Delex { key: Bytes, condition: Option<(String, Bytes)>, responder: flume::Sender<bool> },
    // ...plus Del/Exists/IncrBy/Expire/Persist/Ttl/CountKeysInSlot/GetKeysInSlot/
    // ClientList/DumpKey/PubsubChannels/PubsubNumsub/PubsubNumpat/Keys/Scan/RandomKey/
    // ExpireTime/TierSpillAll/TierGc/TierSnapshot/FlushSlots/Unstick/IsSticky/
    // GetUsedMemory/StreamColdRead — one variant per cross-shard operation.
}
```

**`CompactResp` replaced plain `Vec<u8>` as the `Batch` responder payload** — a
small-buffer-optimized type:

```rust
pub enum CompactResp {
    Small { len: u8, data: [u8; 30] },
    Big(Vec<u8>),
}
```

Most RESP replies (`:123\r\n`, `+OK\r\n`, a short bulk string) fit in 30 bytes, so this
avoids a heap allocation for the overwhelmingly common case of a batched cross-shard
reply, falling back to a real `Vec<u8>` only for longer payloads.

**`is_resp3: bool` on `Batch`**: since a remote shard executing a batched command has no
direct knowledge of which protocol the originating client negotiated (RESP2 vs RESP3
differ in null/boolean/double encoding), the flag is read from a thread-local,
`crate::connection::CURRENT_CLIENT_RESP3`, at the point the message is sent and carried
across the channel so the executing shard serializes the reply correctly.

#### `SlotState` — live cluster migration states (see §4.4 for whether this is wired up)

```rust
pub enum SlotState {
    Stable,
    Migrating(String),
    Importing(String),
    Moved(String),
}
```

#### `ShardDb` (`src/shard.rs`) — the real per-core state container

```rust
pub struct ShardDb {
    pub table: crate::table::RudisTable,
    pub port: u16,
    pub tier_manager: Option<Rc<crate::tiering::ShardTierManager>>,
    pub vector_indexes: std::collections::HashMap<String, crate::vector::HnswIndex>,
    pub crdt_store: crate::crdt::CrdtStore,
    pub json_store: crate::json::JsonStore,
    pub probabilistic_store: crate::probabilistic::ProbabilisticStore,
    pub sticky_keys: hashbrown::HashSet<Bytes>,
}
```

`ShardDb` is mostly a thin delegate layer — well over 100 `#[inline] pub fn` methods
(`hset`, `lpush`, `zadd`, `xadd`, `pfadd`, `bitcount`, ...) that just forward to
`self.table.*`. The exceptions are `set`/`set_extended`/`del`, which additionally check
`self.table.is_tiered(&key)` / `is_cooled(&key)` first and update `tier_manager`'s stats
(and cancel any in-flight async stash operation via `op_manager.cancel_pending_stash`)
when overwriting or deleting a key that currently lives (partially) on NVMe — the
storage engine (`RudisTable`, documented in Component 05) and the tiering engine have to
stay in sync on every mutation, and `ShardDb::set`/`del` is where that happens.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Key Routing (unchanged from the original design)

```rust
pub fn key_slot(key: &[u8]) -> u16 {
    let tag = extract_hash_tag(key);
    (crc16::State::<crc16::XMODEM>::calculate(tag) % 16384) as u16
}

pub fn slot_to_shard(slot: u16, num_shards: usize) -> usize {
    if num_shards <= 1 { 0 } else { ((slot as usize) * num_shards) / 16384 }
}

pub fn target_shard(key: &[u8], num_shards: usize) -> usize {
    slot_to_shard(key_slot(key), num_shards)
}
```

Still real CRC16/XMODEM over a contiguous 16384-slot space, still multiply-then-divide
(not modulo) so each shard owns one contiguous slot range — matching what Redis Cluster
clients expect from `CLUSTER SLOTS`.

#### 4.2 The Local/Remote Fork (still one-shot channels on the per-op path)

Every simple accessor (`get`, `set`, `del`, `exists`, `incr_by`, `expire`, `persist`,
`ttl`, `stick`, `unstick`, `is_sticky`, `delex`, the `Tier*` ops, ...) follows the same
shape:

```rust
pub async fn del(&self, key: Bytes) -> bool {
    let target = target_shard(&key, self.num_shards);
    if target == self.shard_id {
        let deleted = self.local_db.borrow_mut().del(&key);
        if deleted {
            if let Some(aof) = &self.aof {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Del(vec![key])) {
                    aof.borrow_mut().append(&bytes);
                }
            }
        }
        deleted
    } else {
        let (tx, rx) = flume::bounded(1);
        let msg = ShardMessage::Del { key, responder: tx };
        if self.senders[target].send(msg).is_ok() {
            rx.recv_async().await.unwrap_or(false)
        } else {
            false
        }
    }
}
```

This confirms the finding from the earlier accurate draft of this document still holds:
**every one of these per-operation methods allocates a fresh `flume::bounded(1)` channel
per remote call.** They are not the hot pipelined-batch path (that's
`ShardMessage::Batch` via `connection.rs`, using a connection-scoped pre-allocated
channel pool — see Component 02) — these are the fallback used for lone commands. The
write-path methods (`set`, `del`, `incr_by`, `expire`, `persist`) also each append their
own AOF record inline on the local-execution branch, via `crate::aof::command_to_resp`.

`get` additionally has a tiering-aware fast/slow split on the local branch — it checks
the fast in-RAM path first, then (if the key is tiered) whether the shard is under enough
memory pressure to warrant a zero-copy streaming cold read (`stream_cold_read_local`)
versus loading the value back into RAM (`load_local`) before returning it.

#### 4.3 `MGET`/`MSET` — now genuinely fan out, via dedicated `Router::mget`/`mset` methods (previously a real, unfixed gap — now resolved)

`src/connection.rs`'s `Mget`/`Mset` arms no longer loop calling `router.get`/`router.set`
per key. They call two new dedicated methods instead:

```rust
Command::Mget(keys) => {
    let values = router.mget(keys).await;
    ...
}
Command::Mset(pairs) => {
    router.mset(pairs).await;
    ...
}
```

`Router::mget`/`mset` bucket keys by target shard, execute every local key inline
(`get_local_direct`, a factored-out version of `get`'s fast/slow DRAM-vs-tiered logic),
and dispatch **one `ShardMessage::Mget`/`Mset` per remote shard that owns at least one
of the keys/pairs** — the same bucket-once-per-shard shape `execute_commands_squashed`
(Component 02) already used for single-key pipelines, now applied to a single multi-key
command's own key list:

```rust
{
    let owners = self.slot_owners.borrow();
    for (idx, key) in keys.into_iter().enumerate() {
        let slot = key_slot(&key);
        let target = owners[slot as usize];
        if target == self.shard_id { local_keys.push((idx, key)); }
        else { has_remote = true; remote_batches[target].push((idx, key)); }
    }
}
```

**Partitioning here uses the *dynamic* `slot_owners` array, not the static `target_shard`
free function** that every single-key method (`get`/`set`/`del`/...) still uses — see the
new inconsistency this creates, noted in §4.4 and §7.

Three things beyond "just bucket and fan out" are worth understanding:

1. **Pooled channel sets, not fresh allocations.** `mget_channel_pool`/`mset_channel_pool:
   Rc<RefCell<Vec<Vec<MgetChannel/MsetChannel>>>>` hold spare `Vec<flume::bounded(1)>` sets
   (one channel per shard) that `acquire_mget_channels`/`release_mget_channels` check out
   and return, so a connection issuing repeated `MGET`s reuses the same channel vector
   instead of calling `flume::bounded` fresh every time — closing the "one-shot channel
   per call" gap that used to apply here (it still applies to the single-key per-op
   methods in §4.2, unchanged).
2. **A local-only fast path with zero channel operations**: if every key/pair in the call
   happens to be local (`!has_remote`), `mget`/`mset` return immediately after the local
   loop — no channel acquire, no send, no receive at all.
3. **User-space "fast harvest" via non-blocking `try_recv`, falling back to a real
   `.await` only if needed:**

```rust
let mut remaining_mask = sent_mask;
for _ in 0..128 {
    for (target_shard, (_, rx)) in channel_set.iter().enumerate() {
        if (remaining_mask & (1 << target_shard)) != 0
            && let Ok(shard_results) = rx.try_recv()
        {
            remaining_mask &= !(1 << target_shard);
            for (idx, val) in shard_results { results[idx] = val; }
        }
    }
    if remaining_mask == 0 { break; }
    std::hint::spin_loop();
}
if remaining_mask != 0 {
    // any shard that hasn't replied within ~128 spin iterations falls back
    // to a real rx.recv_async().await here
}
```

Because remote shards on other cores often finish a trivial `Mget`/`Mset` batch in well
under a microsecond, this spins with `std::hint::spin_loop()` (a CPU hint, not a real
sleep) polling every still-pending shard's channel with a non-blocking `try_recv` up to
128 times before paying the cost of a real async suspend-and-wake. Correctness is
unaffected either way (the slow path below still awaits properly), but this does mean
the calling task **does not yield to other tasks on the same core** during the spin
window — a deliberate latency-vs-fairness trade, bounded to a small fixed iteration
count specifically so it can't spin forever if a remote shard is genuinely slow or stuck.
`shard_id` values `>= 64` are silently excluded from the bitmask fan-out/harvest
entirely (`target_shard < 64` guards throughout) — see §7 for why that's a real, if
currently theoretical, limit.

#### 4.4 `Router::check_slot_redirection` is dead, but the redirect feature itself is live — via a separate, duplicate implementation in `connection.rs` (correction to an earlier draft of this section)

`Router` has real infrastructure for Redis Cluster-style live slot migration:
`slot_owners: Rc<RefCell<Vec<usize>>>` (a per-slot ownership override, seeded from the
static `slot_to_shard` mapping but mutable via `set_slot_owner`), `slot_states` (tracking
`Migrating`/`Importing`/`Moved` per slot), and a helper method with the same shape as the
logic below:

```rust
pub fn check_slot_redirection(&self, slot: u16, key_exists: bool, asking: bool) -> Result<(), String> {
    match self.slot_states.borrow()[slot as usize].clone() {
        SlotState::Migrating(target) => if !key_exists { return Err(format!("-ASK {} {}\r\n", slot, target)); },
        SlotState::Importing(source) => if !asking { return Err(format!("-MOVED {} {}\r\n", slot, source)); },
        SlotState::Moved(target) => return Err(format!("-MOVED {} {}\r\n", slot, target)),
        SlotState::Stable => {}
    }
    Ok(())
}
```

Grepping the whole codebase confirms this specific method, `check_slot_redirection`, is
**never called anywhere** — that part of an earlier draft of this section was right. But
an earlier draft went one step further and concluded the whole *feature* was dead, which
is wrong: `connection.rs::execute_command` inlines the identical `SlotState` match
directly (not via this helper) at the top of its per-command dispatch, gated on
`cmd_primary_key(&cmd)`:

```rust
if let Some(key) = cmd_primary_key(&cmd) {
    let slot = key_slot(key);
    let state = router.slot_states.borrow()[slot as usize].clone();
    match state {
        SlotState::Moved(target) => { out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, target).as_bytes()); return false; }
        SlotState::Importing(source) => if !is_asking {
            out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, source).as_bytes()); return false;
        },
        SlotState::Migrating(target) => {
            let key_exists = router.exists(key.clone()).await;
            if !key_exists { out.extend_from_slice(format!("-ASK {} {}\r\n", slot, target).as_bytes()); return false; }
        }
        SlotState::Stable => {
            // additionally cross-checks live cluster-bus gossip ownership (Component 11's
            // ClusterHub::my_slots) and emits -MOVED to the gossiped owner if this shard's
            // own slot_states says Stable but the gossip table disagrees
        }
    }
}
```

So real `-MOVED`/`-ASK` redirects genuinely are sent from the normal command path — for
commands that go through `execute_command`. The caveat that *does* still hold: this check
lives only in `execute_command`, the single-command/non-squashed-fallback path (see
Component 02 §4/§10) — grepping `slot_states` usage confirms the pipelined
`execute_commands_squashed` fast path never checks it. **A pipelined batch of commands
hitting a migrating/moved slot silently executes against the wrong data instead of
redirecting**, while the same commands sent unpipelined (or as part of a
squash-defeating pipeline) redirect correctly. This is the real, narrower gap — not "live
migration is entirely unwired," which was the earlier draft's overstatement.

**Update since the original finding**: `slot_owners` (a separate field from
`slot_states`) is no longer consulted by only one call site. `Router::mget`/`mset` (§4.3,
newly added) now also partition keys via `self.slot_owners.borrow()[slot]` rather than
the static `target_shard()` free function — so `slot_owners` now has three real
consultation sites (the dynamic `Router::target_shard` method used by `expiretime`, plus
`mget`, plus `mset`) instead of one. **This creates a new, sharper inconsistency rather
than resolving the old one**: every single-key command (`get`, `set`, `del`, `exists`,
...) still routes via the *static* `target_shard()` free function, which has no idea
`slot_owners` exists — meaning during a live slot migration, `MGET foo` and `GET foo`
could now legitimately disagree about which shard owns `foo`, if `set_slot_owner` has
been called for that slot but the static formula would still point elsewhere. Before this
change, at least every read/write path agreed with each other (all wrong in the same way,
consistently); now there are two different, disagreeing notions of ownership active in
the same file. See §7.

#### 4.5 Cross-shard `SCAN` cursor encoding

`Router::scan` packs which shard to resume from into the cursor itself so a stateless
multi-shard `SCAN` can be resumed correctly between calls:

```rust
pub async fn scan(&self, cursor: u64, pattern: Option<&[u8]>, count: usize) -> (u64, Vec<Bytes>) {
    let shard_id = (cursor >> 32) as usize;
    let slot_idx = (cursor & 0xFFFF_FFFF) as usize;
    ...
    let next_cursor = if next_slot == 0 {
        if shard_id + 1 < self.num_shards { ((shard_id + 1) as u64) << 32 } else { 0 }
    } else {
        ((shard_id as u64) << 32) | (next_slot as u64)
    };
    (next_cursor, keys)
}
```

High 32 bits = which shard the client is currently scanning; low 32 bits = that shard's
own internal cursor. When a shard reports it's exhausted (returns slot `0`), the next
call advances to `shard_id + 1`; reaching the last shard wraps the whole scan back to
cursor `0`.

#### 4.6 Cross-shard transaction locking (`MULTI`/`EXEC` across shards)

```rust
pub async fn acquire_tx_locks(&self, shard_ids: &[usize], tx_id: u64) {
    for &sid in shard_ids {
        if sid == self.shard_id {
            let rx = {
                let mut lock = self.tx_lock.borrow_mut();
                if lock.is_none() { *lock = Some(tx_id); None }
                else {
                    let (tx, rx) = flume::bounded(1);
                    self.tx_waiters.borrow_mut().push_back((tx_id, tx));
                    Some(rx)
                }
            };
            if let Some(rx) = rx { let _ = rx.recv_async().await; }
        } else {
            // ...send ShardMessage::AcquireTxLock to shard `sid` and await it
        }
    }
}
```

A single advisory lock per shard (`tx_lock: Option<u64>`) with a FIFO wait queue
(`tx_waiters`), acquired across every shard a transaction's keys touch — in ascending
shard-ID order — before the transaction executes, released in reverse order afterward
(`release_tx_locks`). This is Rudis's mechanism for atomic multi-shard transactions: a
simple mutual-exclusion lock per shard, not a full multi-version scheduler.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): calls `target_shard_of_cmd`/`Router` methods to
  decide local vs. remote dispatch for every command; the pipelined hot path builds
  `ShardMessage::Batch` directly rather than going through `Router`'s per-op methods.
  `Mget`/`Mset` now call `router.mget(keys).await`/`router.mset(pairs).await` directly
  (§4.3) instead of looping over `router.get`/`router.set`.
- **`src/server.rs`** (Component 01): owns the receive side of every `ShardMessage`
  variant, matched in the per-shard event loop, including the two new `Mget`/`Mset`
  variants (§4.3) — the `Mget` handler itself branches on whether the shard has a
  `tier_manager` at all, taking a fully synchronous `db.get()`-per-key loop when tiering
  is disabled and a more careful (tiering-aware) path when it's enabled.
- **`src/aof.rs`**: `Router` holds an optional `AofWriter` and calls
  `crate::aof::command_to_resp` to append write commands.
- **`src/tiering.rs`**: `Router::spill_local`/`load_local`/`cool_local`/`decommit_local`/
  `check_auto_tier` orchestrate NVMe tiering, dispatched cross-shard via `TierSpill`/
  `TierLoad`/`TierCool`/`TierDecommit`/`TierGc`/`TierSnapshot` messages.
- **`src/pubsub.rs`**: `Router::publish`/`pubsub_channels`/`pubsub_numsub`/`pubsub_numpat`
  publish locally then fan out to every other shard and merge results.
- **`src/cluster.rs`**: every `cluster_*` method on `Router` is a thin passthrough to a
  singleton `crate::cluster::get_cluster_hub(self.port)`.
- **`src/replication.rs`**: `Router::execute_replica_command` applies a command received
  from a replication stream against the correct shard (with `FLUSHALL`/`MSET`/`DEL`
  special-cased since they don't have one single target shard).
- **`src/table.rs`** (Component 05): `ShardDb.table: RudisTable` is the actual storage
  engine; everything else on `ShardDb` is a separate, later-added subsystem
  (`tier_manager`, `vector_indexes`, `crdt_store`, `json_store`,
  `probabilistic_store`) living alongside it, not folded into `RudisValue`.

---

### 6. Performance Characteristics

- **Lock-Free Communication**: unchanged — `flume` channels, no mutexes, no atomics on
  the per-key data path itself.
- **Per-op remote calls still allocate**: as documented in §4.2, the non-batched router
  methods allocate a fresh one-shot channel per remote call; only the pipelined
  `ShardMessage::Batch` path (Component 02) uses a pre-allocated pool.
- **`CompactResp`'s 30-byte inline buffer** removes a heap allocation from the
  overwhelmingly common case (short RESP replies) of the cross-shard batch response path.
- **`MGET`/`MSET` now parallelize across shards, with pooled channels and a busy-poll
  fast-harvest phase** (§4.3, resolved) — one `ShardMessage::Mget`/`Mset` per remote
  shard touched, dispatched together and harvested via non-blocking `try_recv` before
  falling back to a real `.await`, so throughput on multi-shard keysets is now bounded by
  the slowest remote shard's response, not by the sum of every key's round-trip.

---

### 7. Future Improvements

- **High — unify `slot_owners` and `slot_states`/`ClusterHub.my_slots` into one slot-authority mechanism (§4.4). Now more urgent, not less.** The `MGET`/`MSET` fan-out fix (§4.3) made this worse in one specific way: `mget`/`mset` now route via `slot_owners` while every single-key command still routes via the static `target_shard()` free function, so the two families of commands can now genuinely disagree about slot ownership during a live migration, not just theoretically. Pick one source of truth (most likely `slot_states`/`ClusterHub`, since that's the one wired into `-MOVED`/`-ASK` redirection) and route every command — single-key and multi-key alike — through it.
- ~~**High — fix `MGET`/`MSET` fan-out (§4.3)**~~ **Resolved.** `Router::mget`/`mset` now bucket by shard, dispatch one `ShardMessage::Mget`/`Mset` per remote shard via pooled channels, and harvest replies with a non-blocking `try_recv` sweep before falling back to `.await` (§4.3). Two real follow-ups from the fix itself: (1) the slot-authority split noted above, and (2) neither method's bitmask-based dispatch/harvest tracks shards with `target_shard >= 64` (the code guards every bitmask operation with `target_shard < 64`) — harmless at the default `num_shards.min(8)`, but if `--threads` is ever set above 64, keys landing on shard 64+ would be sent a message that's never waited on, silently leaving those result slots as `None`/unset. Worth an explicit assertion or a `Vec<bool>`-based tracking scheme instead of a `u64` bitmask if very high shard counts are ever supported.
- **High — `MGET`/`MSET` never check `slot_states` for redirection at all, even on top of §4.4's gap (newly found).** `execute_command`'s slot-migration check (§4.4) is gated on `cmd_primary_key(&cmd)`, which has no arm for `Mget`/`Mset` (multi-key commands don't have one primary key) — so unlike every single-key command, a live `MGET`/`MSET` against a migrating/moved slot never redirects at all, squashed or not. Fixing this needs a per-key (not per-command) redirect check inside `Router::mget`/`mset` itself, likely alongside the `slot_owners` unification above.
- **Medium — delete `Router::check_slot_redirection` or make it the single source of truth (§4.4).** Right now the real redirect logic lives duplicated inline in `connection.rs` while this near-identical helper method sits unused in `router.rs`. Either delete the dead helper (simplest — removes a maintenance trap where someone "fixes" the wrong copy) or refactor `connection.rs` to call it, eliminating the duplication risk either way.
- ~~**Medium — extend the live slot-migration redirect check to the pipelined squash path.**~~ **Turned out already handled, per Component 02 §7's correction.** The squash-eligibility gate's `SlotState::Stable`-vs-not check predates this round of changes — `Migrating`/`Importing`/`Moved` already correctly defeated squashing before. What this round of changes actually added on top: the `Stable`-but-a-different-node-owns-it-per-cluster-gossip case now also correctly defeats squashing, confirmed by the new `test_cluster_pipelined_squashed_moved_redirect_e2e` E2E test — closing the one piece of this that genuinely was missing.
- **Low — reduce the ~10x duplicated local/remote-fork method bodies (§4).** The per-operation methods (`get`/`set`/`del`/`exists`/...) are intentionally monomorphic rather than generic (§4's stated rationale), which is a reasonable trade — but a thin macro that generates the boilerplate (target-shard computation, local-vs-remote branch, `flume::bounded(1)` fallback) from a one-line-per-command table would keep the monomorphic-dispatch benefit while cutting the ~10x copy-pasted structure down to one place to get right.

---
---

## Component 05: Storage Engine & Compact Encodings (`src/table.rs`)

### 1. Architectural Purpose & Scope

`src/table.rs` is Rudis's core in-memory associative storage engine. It provides the
dictionary implementation (`RudisTable`), the definitions and per-command logic for every
`RudisValue` data type (strings, hashes, lists, sets, sorted sets, streams, bitmaps-as-strings,
HyperLogLog), active/passive key expiration, and the bookkeeping hooks that let
`src/tiering.rs` move cold values out to NVMe storage and back.

The dictionary itself is still the custom SIMD flat hash table (`RudisFlatTable`) originally
designed for this project — it has **not** been replaced by `hashbrown` or any Listpack/
Intset/skiplist-based structure. What has grown substantially since the original design is
everything built on top of it: `RudisValue` now has 11 variants instead of 2, several of
which have their own adaptive small/full representations, and `RudisTable` now tracks live
memory usage and NVMe-tiering state per key.

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-Isolation**: Each `RudisTable` belongs to a single shard thread. It contains
   **no mutexes, atomic operations for its own data, or lock-free concurrency wrappers**
   (the two global counters described in §5 are process-wide atomics, but they're simple
   monotonic stats, not synchronization for the table itself).
2. **Inlined Metadata & Expiration**: `expire_at: Option<Instant>` lives directly inside
   `RudisEntry`. Checking key validity never requires a secondary hash lookup.
3. **Adaptive Small/Full Promotion — but not for every type**: `Hash`, `Set`, and `ZSet`
   values each start in a compact linear form and promote to an indexed form once they
   cross a size threshold (§4). Hash's thresholds are runtime-configurable via global
   atomics; Set's and ZSet's are hardcoded constants. `List` and `Stream` have no compact
   form at all — they use the same representation regardless of size.
4. **Passive Inline Eviction, with an escape hatch**: any operation encountering an expired
   key immediately frees it and returns `None`/empty, unless the process-wide
   `crate::connection::ALLOW_ACCESS_EXPIRED` atomic flag is set, in which case expiration
   checks are skipped entirely (see §5.1).
5. **Cold-storage awareness**: a value can be partially or fully moved to NVMe by
   `src/tiering.rs`. `table.rs` doesn't perform that I/O itself, but `RudisValue::Tiered`
   and `RudisValue::Cooled` exist specifically so the table can represent "this key's value
   lives on disk" or "this key's value lives on disk *and* is still cached in RAM" (§5.3).

---

### 3. Data Structures & Memory Layouts

#### 3.1 The SIMD flat table (unchanged core mechanism)

```rust
pub struct RudisFlatTable {
    ctrl: Vec<u8>,
    pub slots: Vec<Option<RudisEntry>>,
    pub capacity: usize,
    mask: usize,
    items: usize,
    growth_left: usize,
    pub slot_counts: Box<[u32; 16384]>,
}
```

Lookup, insert, and delete still work exactly as originally designed: a 7-bit fingerprint
per slot in a separate `ctrl` byte array, 16-slot SIMD group loads (`_mm_cmpeq_epi8` /
`_mm_movemask_epi8`), triangular-step probing, tombstone (`DELETED`) deletion, and a
monolithic doubling `resize()` at 7/8 load factor. None of that has changed. What's new on
this struct is `slot_counts` — see §6.

#### 3.2 `RudisEntry` and the real `RudisValue` enum

```rust
#[derive(Clone, Debug)]
pub struct RudisEntry {
    pub key: Bytes,
    pub val: RudisValue,
    pub expire_at: Option<Instant>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RudisValue {
    String(Bytes),
    Int(i64),
    SmallHash(Vec<(Bytes, Bytes)>),
    Hash(HashMap<Bytes, Bytes>),
    List(std::collections::VecDeque<Bytes>),
    Set(RudisSet),
    ZSet(RudisZSet),
    HyperLogLog(Box<[u8; 16384]>),
    Stream(RudisStream),
    Tiered(TieredPointer),
    Cooled {
        ptr: TieredPointer,
        val: Box<RudisValue>,
    },
}
```

There is no `Bitmap` variant — `SETBIT`/`GETBIT`/bitmap commands operate directly on the
bytes of a `RudisValue::String`. There is no `Json` variant in this enum either (JSON
values are handled by `src/json.rs` as a separate concern, outside `table.rs`'s scope).

#### 3.3 `RudisTable` itself

```rust
pub struct RudisTable {
    table: RudisFlatTable,
    sample_cursor: usize,
    spill_cursor: usize,
    pub used_memory: usize,
}
```

Two cursors, not one: `sample_cursor` drives active TTL expiration (unchanged, §5.2) and
`spill_cursor` independently drives NVMe-tiering candidate sampling (new, §5.3).
`used_memory` is a running estimate of live value bytes, updated on every insert, mutation,
delete, and expiration — it did not exist in the original design and exists to give
`src/tiering.rs` a cheap signal for memory-pressure decisions without walking the table.

---

### 4. Compact Encodings Deep-Dive

There is **no Listpack, Intset, or pointer-based skiplist anywhere in this file.** Three of
the eleven `RudisValue` variants have a genuine small/full adaptive representation; the
rest are always one fixed Rust type.

#### 4.1 Strings: `String(Bytes)` vs. `Int(i64)`

`set` parses every incoming value as a 64-bit integer first; if it parses cleanly, the
entry is stored as `RudisValue::Int`, not `String`:

```rust
let val = if let Some(int_val) = Self::parse_i64_bytes(&value) {
    RudisValue::Int(int_val)
} else {
    RudisValue::String(value)
};
```

`incr_by` then mutates an existing `Int` in place (`*n = nv`) with zero allocation, only
falling back to parsing bytes if the value is still a plain `String`. `parse_i64_bytes` and
`format_i64` are hand-written ASCII digit loops (no `format!`/`str::parse`) used everywhere
an integer needs to become bytes or vice versa.

#### 4.2 Hashes: `SmallHash(Vec<(Bytes, Bytes)>)` vs. `Hash(HashMap<Bytes, Bytes>)`

```rust
let max_entries = crate::connection::HASH_MAX_ENTRIES.load(Ordering::Relaxed);
let max_value = crate::connection::HASH_MAX_VALUE.load(Ordering::Relaxed);
...
RudisValue::SmallHash(pairs) => {
    // ... linear-scan insert/update into `pairs` ...
    if pairs.len() > max_entries || pairs.iter().any(|(k, v)| k.len() > max_value || v.len() > max_value) {
        let map: HashMap<Bytes, Bytes> = pairs.drain(..).collect();
        entry.val = RudisValue::Hash(map);
    }
}
```

`SmallHash` is a plain `Vec` of pairs searched linearly on every read/write — not a
byte-packed buffer. The promotion thresholds are **runtime-configurable** process-wide
atomics (`HASH_MAX_ENTRIES`, `HASH_MAX_VALUE`, owned by `connection.rs`), conceptually
mirroring Redis's `hash-max-listpack-entries`/`-value` config even though the small form
here is a `Vec`, not a listpack.

#### 4.3 Sets: `RudisSet::Small(Vec<Bytes>)` vs. `Full(HashSet<Bytes>)`

```rust
const SMALL_SET_LIMIT: usize = 64;

pub fn insert(&mut self, member: Bytes) -> bool {
    match self {
        RudisSet::Small(v) => {
            // linear contains-check, then push
            if v.len() > SMALL_SET_LIMIT {
                let mut set = hashbrown::HashSet::with_capacity(v.len());
                for m in v.drain(..) { set.insert(m); }
                *self = RudisSet::Full(set);
            }
            true
        }
        RudisSet::Full(s) => s.insert(member),
    }
}
```

Unlike `Hash`, the set threshold is a **hardcoded constant** (64), not configurable.
Deletion from the small form uses `swap_remove` (O(1), reorders the vec) rather than
shifting. A custom `RudisSetIter` enum unifies iteration over both representations behind
one type so callers (`SMEMBERS`, `SINTER`, etc.) don't need to match on the variant.

#### 4.4 Sorted sets: `RudisZSet::Small(Vec<(OrderedScore, Bytes)>)` vs. `Full { dict, tree }`

```rust
const SMALL_ZSET_LIMIT: usize = 64;

pub enum RudisZSet {
    Small(Vec<(OrderedScore, Bytes)>),
    Full {
        dict: hashbrown::HashMap<Bytes, f64>,
        tree: std::collections::BTreeSet<(OrderedScore, Bytes)>,
    },
}
```

The small form is a **sorted** `Vec`: `insert` finds the insertion point with
`binary_search` (O(log n) to *find* the slot, but `Vec::insert` still shifts elements, so
insertion itself is O(n)). The full form pairs a `HashMap<Bytes, f64>` (O(1) score lookup)
with a `BTreeSet<(OrderedScore, Bytes)>` ordered by `(score, member)` for range queries.

**Correction worth being explicit about**: `rank()` is **not** O(log n) in either
representation. Even in the `Full` form it's a linear scan:

```rust
RudisZSet::Full { dict, tree } => {
    if !dict.contains_key(member) { return None; }
    if rev { tree.iter().rev().position(|(_, m)| m.as_ref() == member) }
    else    { tree.iter().position(|(_, m)| m.as_ref() == member) }
}
```

`BTreeSet` has no random-access rank operation in `std`, so `ZRANK`/`ZREVRANK` pay for an
O(n) walk regardless of representation. There is no augmented/spanned skiplist anywhere in
this file providing O(log n) rank.

#### 4.5 Lists: always `VecDeque<Bytes>`

`RudisValue::List(std::collections::VecDeque<Bytes>)` — one representation regardless of
length. `LPUSH`/`RPUSH`/`LPOP`/`RPOP` are `VecDeque::push_front`/`push_back`/`pop_front`/
`pop_back`; there is no compact/large-list distinction.

#### 4.6 Streams: `BTreeMap<StreamId, Vec<(Bytes, Bytes)>>`

```rust
pub struct RudisStream {
    pub entries: std::collections::BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub groups: HashMap<Bytes, StreamGroup>,
}
```

`StreamId { ms: u64, seq: u64 }` derives `Ord`, so entries are naturally ordered by ID via
the `BTreeMap`, which is what makes `XRANGE`-style ID-range queries efficient without any
custom indexing. Each `StreamGroup` tracks its own `last_delivered_id`, a map of
`StreamConsumer`s, and a pending-entries-list (`pel: BTreeMap<StreamId, StreamPelEntry>`)
recording per-entry delivery time and count for `XACK`/`XCLAIM`/`XAUTOCLAIM`.

#### 4.7 HyperLogLog: always a dense `Box<[u8; 16384]>`

`RudisValue::HyperLogLog(Box<[u8; 16384]>)` is a fixed 16,384-register dense array — the
same dense representation real Redis's HLL falls back to for large cardinalities, used
unconditionally here. There is no sparse encoding for small cardinalities.

---

### 5. Expiration & Memory Management

#### 5.1 Passive expiration on read — now with a global bypass and stat counter

```rust
fn check_expired_slot(&mut self, slot_idx: usize) -> bool {
    if crate::connection::ALLOW_ACCESS_EXPIRED.load(Ordering::Relaxed) {
        return false;
    }
    let is_exp = /* compare Instant::now() against entry.expire_at */;
    if is_exp {
        if let Some(removed) = self.table.remove(slot_idx) {
            let freed = removed.key.len() + removed.val.approx_bytes() + 64;
            self.used_memory = self.used_memory.saturating_sub(freed);
            inc_expired_keys();
        }
        true
    } else {
        false
    }
}
```

Same core mechanism as before (check inline, evict on the spot, zero secondary lookups),
plus two additions: `ALLOW_ACCESS_EXPIRED` is a process-wide atomic that, when set, disables
expiration checks everywhere in the table (used for debug/inspection paths that need to see
logically-expired keys); and every real eviction now updates `used_memory` and bumps a
process-wide `EXPIRED_KEYS` atomic counter (`inc_expired_keys`/`get_expired_keys`) presumably
surfaced through `INFO`.

#### 5.2 Active expiration sampling — unchanged

```rust
pub fn active_expire_cycle(&mut self) -> usize {
    let cap = self.table.capacity();
    if cap == 0 || self.table.len() == 0 { return 0; }
    let mut expired_count = 0;
    let mut checked = 0;
    while checked < 20 {
        let idx = self.sample_cursor % cap;
        self.sample_cursor = (self.sample_cursor + 1) % cap;
        if self.check_expired_slot(idx) { expired_count += 1; }
        checked += 1;
    }
    expired_count
}
```

Identical in shape to the original design: bounded 20-slots-per-call sampling via a
persistent cursor, called periodically from the server's event loop.

#### 5.3 NVMe tiering hooks (new since the original design)

`table.rs` doesn't talk to disk itself, but it exposes exactly the primitives
`src/tiering.rs` needs to move values in and out of RAM:

```rust
pub fn get_hot_keys_for_spill(&mut self, limit: usize) -> Vec<Bytes> {
    // round-robins spill_cursor across all slots, collecting keys whose
    // value is NOT already RudisValue::Tiered/Cooled, up to `limit`
}

pub fn decommit_all_cooled(&mut self) -> (usize, u64) {
    // for every RudisValue::Cooled{ptr, val}, drops `val` and replaces it
    // with RudisValue::Tiered(ptr) — freeing the RAM copy but keeping the
    // on-disk pointer, updating used_memory as it goes
}
```

Reading the `RudisValue` enum (§3.2) again in this light: `Tiered(TieredPointer)` means
"this value lives only on disk, referenced by `{file_id, offset, length, value_type}`";
`Cooled { ptr, val }` means "this value has been written to disk *and* is still cached in
RAM" — a transitional state that `get`/`get_entry` transparently unwrap so reads never need
to know which state a value is in:

```rust
let val_ref = match &entry.val {
    RudisValue::Cooled { val, .. } => val.as_ref(),
    other => other,
};
```

`spill_cursor` (separate from expiration's `sample_cursor`) tracks where the next spill scan
should resume, so repeated spill passes sweep the whole table rather than re-scanning from
the start every time.

---

### 6. Cluster Slot Indexing (architecture change from the original design)

The original design maintained a full reverse index, `slot_to_keys: HashMap<u16,
HashSet<Bytes>>`, so `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT` could answer in O(keys in
that slot). **That reverse index is gone.** It's been replaced with a fixed-size count-only
array living inside `RudisFlatTable` itself:

```rust
pub slot_counts: Box<[u32; 16384]>,
```

incremented/decremented directly in `RudisFlatTable::insert`/`remove`/`resize`/`clear`.
This makes `count_keys_in_slot` O(1) **only in the common case that the slot is empty**:

```rust
pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
    if self.table.slot_counts[slot as usize] == 0 {
        return 0;
    }
    // otherwise: linear scan every slot in the table, filtering by
    // crate::router::key_slot(&entry.key) == slot
    ...
}
```

For a **non-empty** slot, both `count_keys_in_slot` and `get_keys_in_slot` now fall back to
an O(table capacity) linear scan over every slot in the table, checking each live entry's
computed cluster slot. This is a real regression versus the original reverse-index design
for any workload that calls these commands against a populated cluster slot — traded, most
likely, for removing the double-bookkeeping cost the old `slot_to_keys` paid on every single
insert/delete (cloning each key into a second `HashSet`). Both scanning functions still do
opportunistic lazy expiration of any expired keys they encounter along the way, same as
before.

---

### 7. Performance Characteristics

- **SIMD group probing is unchanged**: still one 128-bit load and compare per 16-slot group,
  triangular-step probing to avoid primary clustering.
- **Zero-allocation `INCR`/`DECR`** when the value is already `Int`-encoded — mutates the
  `i64` in place instead of round-tripping through string formatting.
- **Hand-written integer/byte conversions** (`parse_i64_bytes`, `format_i64`) avoid
  `std::str`/`format!` overhead on the hottest string-command paths.
- **Resize is still monolithic**: `RudisFlatTable::resize` still rehashes the entire table
  into a fresh allocation at 7/8 load factor — the segmented/incremental resize from the
  original design's roadmap was never built.
- **Small-form promotions trade O(n) linear scans for cache-friendly `Vec` access** below
  their thresholds — real for `Hash`/`Set`/`ZSet`, applied inconsistently (Hash's threshold
  is live-configurable; Set's and ZSet's are compile-time constants).
- **`used_memory` is an estimate, not exact accounting** — it's derived from
  `RudisValue::approx_bytes()` (a fixed per-variant heuristic, e.g. `+16`/`+32` bytes of
  assumed overhead per element) plus a flat `+64` bytes per entry, not a precise allocator
  measurement.

---

### 8. Future Improvements

- **High — implement the original segmented/incremental resize design (§4's Phase 2, still not started).** `RudisFlatTable::resize` is still a monolithic doubling rehash of the *entire* table (§7) — the exact tail-latency spike the original design document (§1) was written to eliminate. This remains the single highest-value structural change to this file if p99.9 write latency at large key counts ever becomes a measured problem.
- **Medium — restore an efficient cluster-slot key index (§6's "real regression").** Replacing `slot_to_keys: HashMap<u16, HashSet<Bytes>>` with a count-only `slot_counts` array traded away O(keys-in-slot) `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT` for O(table capacity) on any non-empty slot. A middle ground — e.g. a small per-slot `Vec<usize>` of slot indices, sized only for slots actually in use, rather than a full `HashSet<Bytes>` clone of every key — could recover most of the lookup speed without paying the original design's full per-insert cloning cost.
- **Medium — give `ZSet`/`ZRANK` a real O(log n) rank operation.** `rank()` is a linear scan even in the `Full` representation because `BTreeSet` has no built-in indexable-rank support (§4.4). An order-statistics structure (a `BTreeMap` augmented with subtree sizes, or a hand-rolled indexable skiplist closer to the original design's roadmap framing) would make `ZRANK`/`ZREVRANK`/`ZRANGEBYSCORE`-with-rank genuinely sub-linear, which matters more as sorted sets grow past the 64-element small-form threshold.
- **Low — make `Set`/`ZSet`'s small-form promotion thresholds runtime-configurable**, consistent with `Hash`'s already-configurable `HASH_MAX_ENTRIES`/`HASH_MAX_VALUE` (§4.2) — currently `SMALL_SET_LIMIT`/`SMALL_ZSET_LIMIT` are compile-time constants (§4.3/§4.4), an inconsistency with no apparent reason beyond historical accident.
- **Low — track `used_memory` per-`RudisValue`-variant more precisely for the variants tiering decisions care about most** (`String`/`Hash`/`List`/`Set`/`ZSet`, the types actually eligible for spill) rather than one flat heuristic (§7) — doesn't need to be exact, but a closer estimate would make `src/tiering.rs`'s offload/upload threshold decisions (Component 07) more accurate without needing real allocator introspection.

---
---

## Component 06: Blocking Operations & The Reactive Event Hub (`src/block.rs`)

### 1. Architectural Purpose & Scope

`src/block.rs` implements Rudis's waiter registration and wakeup engine (**`BlockHub`**). It
powers the blocking list/zset/stream commands — `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPOP`-style
moves, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, and `XREAD ... BLOCK` — plus `CLIENT UNBLOCK` and the
blocked-flag reported by `CLIENT LIST`/`CLIENT INFO`. Unlike the rest of Rudis, `BlockHub` is
**not** thread-local: it is one process-wide, mutex-guarded structure per listening port, shared
by every shard thread serving that port.

### 2. Key Invariants & Concurrency Constraints

1. **The one deliberate exception to "zero locks."** `BlockHub` lives behind a real
   `std::sync::Mutex`, reachable from any shard via `get_block_hub_for_port(port)`:
   ```rust
   pub static PORT_BLOCK_HUBS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>> =
       LazyLock::new(|| Mutex::new(HashMap::new()));

   pub fn get_block_hub_for_port(port: u16) -> Arc<Mutex<BlockHub>> {
       let mut map = PORT_BLOCK_HUBS.lock().unwrap();
       map.entry(port)
           .or_insert_with(|| Arc::new(Mutex::new(BlockHub::new(port))))
           .clone()
   }
   ```
   Every caller does `get_block_hub_for_port(port).lock().unwrap()` around a short, synchronous
   critical section (register a waiter, or walk a key's waiter queue and pop values). This one
   `Mutex` is the price paid for cross-shard wakeups: a client blocked on shard A must be
   wakeable by a write executed on shard B, which the thread-local `Rc<RefCell<ShardDb>>` model
   can't do on its own.
2. **Reactor threads never sleep on a plain timer.** A blocked command doesn't `sleep()` the
   whole event loop — it registers a waiter (a `flume::Sender`), yields by `.await`ing a
   polling helper (`wait_for_blocked_result`, §4.2) that also has to actively watch for client
   disconnect, since a channel receive alone can't detect a dead TCP socket (see §4.2).
3. **FIFO fairness per key.** Waiter queues are `VecDeque`, and `notify_*` always
   `pop_front()`s — the client that has been waiting longest on a key is served first.
4. **Guaranteed cleanup via RAII.** `BlockedClientGuard`'s `Drop` impl calls
   `hub.unregister_blocked_client(client_id)` unconditionally, so a waiter registration can
   never outlive the `.await` that registered it — whether it resolved by pop, by timeout, by
   `CLIENT UNBLOCK`, or by the connection task itself being dropped.
5. **Transaction-aware deferral, not `CLIENT PAUSE`.** `BlockHub::pause()`/`resume()` exist —
   but they're wired to `MULTI`/`EXEC`, not to Redis's `CLIENT PAUSE` command (which is a
   complete no-op stub in Rudis, see §4.4).

---

### 3. Component Architecture & Data Structures

```
   BLPOP k1 k2 0 (both empty)                    LPUSH k1 "v"  (any shard)
            │                                              │
            ▼                                              ▼
  hub.register_blocked_client(cid, tx)          notify_list_or_defer(db, "k1")
  hub.register_list_waiter(cid,"k1",Pop,1,tx)             │
  hub.register_list_waiter(cid,"k2",Pop,1,tx)     paused? ──yes──▶ add_pending_notify("k1")
            │                                              │no
            ▼                                              ▼
  wait_for_blocked_result(&rx, timeout, fd)        hub.notify_list(&mut table, "k1")
   (polls rx every ≤20ms; also polls the fd         │ pop_front waiter for "k1"
    for POLLHUP/POLLRDHUP/EOF to detect a            │ table.lpop("k1", 1)  ◀── pop happens
    client that vanished without closing            │ inside the notify call, under the
    cleanly — a channel recv alone can't see         │ same Mutex-held critical section
    that)                                            ▼ tx.send(Popped(key, vals))
            │                                  remove_waiters_for_client(cid)
            ▼                                  (drops the still-registered "k2" waiter too)
  BlockedClientGuard::drop → unregister
```

#### Real waiter/result types (`src/block.rs`)

```rust
pub enum WaiterOp {
    Pop { pop_type: ListPopType, count: usize },
    Move { where_from: ListPopType, where_to: ListPopType, destination: Bytes },
}

pub struct ListWaiter {
    pub client_id: u64,
    pub key: Bytes,
    pub op: WaiterOp,
    pub sender: Sender<BlockedListResult>,   // flume::Sender, not oneshot
}

pub struct ZSetWaiter {
    pub client_id: u64,
    pub key: Bytes,
    pub pop_type: ZSetPopType,   // Min | Max
    pub count: usize,
    pub is_zmpop: bool,
    pub sender: Sender<BlockedZSetResult>,
}

pub struct StreamWaiter {
    pub key: Bytes,
    pub sender: Sender<()>,   // pure wakeup, no payload — reader re-polls the stream itself
}

pub enum BlockedListResult {
    Popped(Bytes, Vec<Bytes>),
    Unblocked(ClientUnblockType),   // Timeout | Error | WrongType
}

pub struct BlockHub {
    pub port: u16,
    list_waiters: HashMap<Bytes, VecDeque<ListWaiter>>,
    zset_waiters: HashMap<Bytes, VecDeque<ZSetWaiter>>,
    stream_waiters: HashMap<Bytes, Vec<StreamWaiter>>,
    blocked_clients: HashMap<u64, Sender<BlockedListResult>>,      // for CLIENT UNBLOCK / CLIENT LIST's "b" flag
    blocked_zset_clients: HashMap<u64, Sender<BlockedZSetResult>>,
    paused_count: usize,          // MULTI/EXEC deferral, see §4.4 — NOT CLIENT PAUSE
    pending_notifies: Vec<Bytes>,
}
```

There is no unified `Waiter`/`WaiterType`/`BlockedPopResult` type — list, zset, and stream
waiters are three separate types with three separate queues and three separate result enums,
and channels are `flume::Sender`, not `oneshot::Sender`.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Registering a blocked client (`BLPOP`, in `src/connection.rs`)

```rust
let _guard = BlockedClientGuard { port: router.port, client_id };
let (tx, rx) = flume::bounded(1);
{
    let hub_arc = crate::block::get_block_hub_for_port(router.port);
    let mut hub = hub_arc.lock().unwrap();
    hub.register_blocked_client(client_id, tx.clone());
    for k in &keys {
        hub.register_list_waiter(client_id, k.clone(), crate::block::ListPopType::Left, 1, tx.clone());
    }
}
let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
let (recv_res, client_disconnected) = wait_for_blocked_result(&rx, timeout, raw_fd).await;
```

One waiter is registered per key in the `BLPOP` argument list, all sharing the same `tx` clone
— whichever key is satisfied first sends on that shared channel, and `remove_waiters_for_client`
(called from inside `notify_list`) then strips the *other* now-stale waiters for that client from
every other key's queue. `BlockedClientGuard` is held across the whole `.await`, so if the
future is ever dropped early (client disconnect, etc.) its `Drop` impl still unregisters.

#### 4.2 `wait_for_blocked_result`: polling, not a pure channel await

```rust
pub async fn wait_for_blocked_result<T>(
    rx: &flume::Receiver<T>,
    timeout_secs: f64,
    raw_fd: Option<std::os::unix::io::RawFd>,
) -> (Option<T>, bool) {
    let deadline = ...;
    loop {
        let check_dur = /* min(remaining time, 20ms) */;
        match monoio::time::timeout(check_dur, rx.recv_async()).await {
            Ok(Ok(res)) => return (Some(res), false),
            Ok(Err(_)) => return (None, false),
            Err(_) => {
                if let Some(fd) = raw_fd {
                    if is_fd_closed(fd) { return (None, true); }
                }
                if /* deadline passed */ { return (None, false); }
            }
        }
    }
}
```

This is **not** a single `select! { rx.await, sleep(timeout).await }` as one might assume — it
loops, re-`await`ing the channel with a cap of 20ms per iteration, and on every timeout tick
also calls `is_fd_closed(raw_fd)`:

```rust
pub fn is_fd_closed(fd: RawFd) -> bool {
    let mut pollfd = libc::pollfd { fd, events: POLLIN|POLLRDHUP|POLLHUP|POLLERR, revents: 0 };
    let ret = unsafe { libc::poll(&mut pollfd, 1, 0) };
    // POLLRDHUP/POLLHUP/POLLERR ⇒ closed; POLLIN + a 0-byte MSG_PEEK recv ⇒ closed
    ...
}
```

The reason: while a client is blocked on `BLPOP`, its connection task isn't in its normal
read loop — it's parked on `rx.recv_async()` — so a client that disappears (network drop,
process killed) without a clean FIN would otherwise leave its waiter (and the task itself)
registered forever. Polling the raw fd every ≤20ms via `libc::poll` + a non-consuming
`MSG_PEEK` recv is how Rudis detects that case without a dedicated epoll registration for
blocked sockets.

#### 4.3 Waking waiters: the pop happens *inside* `notify_list`/`notify_zset`, under the lock

```rust
pub fn notify_list(&mut self, table: &mut crate::table::RudisTable, key: &Bytes) {
    crate::connection::touch_watched_key(self.port, key.as_ref());
    if table.is_key_expired(key.as_ref()) { return; }
    if let Some(waiters) = self.list_waiters.get_mut(key) {
        while let Some(waiter) = waiters.pop_front() {
            if waiter.sender.is_disconnected() { continue; }
            match waiter.op {
                WaiterOp::Pop { pop_type, count } => {
                    let popped = match pop_type {
                        ListPopType::Left => table.lpop(key.as_ref(), count).ok(),
                        ListPopType::Right => table.rpop(key.as_ref(), count).ok(),
                    };
                    if let Some(vals) = popped {
                        if !vals.is_empty() {
                            let _ = waiter.sender.send(BlockedListResult::Popped(key.clone(), vals));
                            satisfied_clients.push(waiter.client_id);
                            if !table.exists(key.as_ref()) { break; }
                        } else {
                            waiters.push_front(waiter);   // put it back, nothing to give it
                            break;
                        }
                    } else { waiters.push_front(waiter); break; }
                }
                WaiterOp::Move { where_from, where_to, ref destination } => { /* see below */ }
            }
        }
    }
    for cid in satisfied_clients { self.remove_waiters_for_client(cid); }
}
```

Contrary to a design where the *writer* (e.g. `LPUSH`) hands the pushed value directly to the
waiter, Rudis instead re-derives the popped value by calling `table.lpop`/`table.rpop` **from
inside `notify_list` itself**, while the caller (`notify_list_or_defer`, in `connection.rs`)
still holds the `BlockHub` mutex *and* the caller's own `ShardDb` borrow. This makes "pick a
waiter" and "remove the value from the list" atomic with respect to each other by construction
— there's no window where two different code paths could both believe they popped the same
element. `touch_watched_key` is called unconditionally at the top, meaning a blocking-wakeup
write also correctly invalidates any `WATCH` on that key (ties into the `MULTI`/`WATCH`
machinery documented in Component 02).

`WaiterOp::Move` (backing `BLMOVE`/`BRPOPLPUSH`-style commands) pops from the source list and
pushes onto the destination list in the same critical section, then **recursively calls**
`self.notify_list(table, destination)` — so if some *other* client is separately blocked on the
destination key, a single `LPUSH`-triggered wakeup can cascade into a second wakeup for a
completely different blocked client, still inside one lock acquisition.

`notify_zset` is structurally identical (`zpopmin`/`zpopmax` instead of `lpop`/`rpop`), except it
also explicitly skips a waiter if `satisfied_clients` already contains its `client_id` — a
duplicate-suppression check the list path doesn't need in the same spot because
`remove_waiters_for_client` is applied immediately afterward per satisfied client.

`notify_stream` (backing `XREAD ... BLOCK`) is much simpler — it's a pure wakeup, no data
handoff:
```rust
pub fn notify_stream(&mut self, key: &Bytes) {
    if let Some(waiters) = self.stream_waiters.remove(key) {
        for waiter in waiters { let _ = waiter.sender.send(()); }
    }
}
```
The blocked `XREAD` task is responsible for re-reading the stream itself once woken.

#### 4.4 `pause()`/`resume()`: deferred notification across `MULTI`/`EXEC` — not `CLIENT PAUSE`

```rust
// connection.rs, around EXEC:
let hub_arc = crate::block::get_block_hub_for_port(router.port);
hub_arc.lock().unwrap().pause();
IN_TX.set(true);
for q_cmd in queued { execute_command(q_cmd, ...).await; }
IN_TX.set(false);
if use_vll { router.release_tx_locks(&sorted_shards, tx_id).await; }
let pending = hub_arc.lock().unwrap().resume();
for k in pending { /* re-dispatch a real notify for each deferred key, possibly cross-shard */ }
```

Every list/zset write goes through `notify_list_or_defer`/`notify_zset_or_defer` rather than
calling `notify_list`/`notify_zset` directly:
```rust
pub fn notify_list_or_defer(db: &mut ShardDb, key: &Bytes) {
    touch_watched_key(db.port, key.as_ref());
    let mut hub = get_block_hub_for_port(db.port).lock().unwrap();
    if hub.is_paused() {
        hub.add_pending_notify(key.clone());
    } else {
        hub.notify_list(&mut db.table, key);
    }
}
```
So while an `EXEC` is running, every write inside the transaction just records its key in
`pending_notifies` instead of immediately waking any blocked client; only after the whole
transaction (and, for cross-shard transactions, the VLL lock release) completes does `resume()`
drain `pending_notifies` and fire real wakeups for each touched key. This means a blocked client
can never observe a partially-applied transaction as if it were a completed write.

**This machinery is unrelated to Redis's `CLIENT PAUSE`/`CLIENT UNPAUSE`.** Those are parsed
into real `Command::Client(ClientSubcommand::Pause(timeout))`/`Unpause` variants, but the
handler for both (and for `CLIENT NO-TOUCH`) is:
```rust
ClientSubcommand::Pause(_) | ClientSubcommand::Unpause | ClientSubcommand::NoTouch(_) => {
    out.extend_from_slice(b"+OK\r\n");
}
```
— an unconditional `+OK` with no effect. `CLIENT PAUSE` does not actually pause anything in
Rudis today.

#### 4.5 `CLIENT UNBLOCK` and the `CLIENT LIST`/`INFO` blocked flag

```rust
let unblocked = hub.unblock_client(target_id, unblock_type);   // CLIENT UNBLOCK <id> [TIMEOUT|ERROR]
...
let is_blocked = get_block_hub_for_port(router.port).lock().unwrap().is_blocked(c.id);
let flags = if is_blocked { "b" } else { "N" };   // surfaced in CLIENT INFO/LIST
```
`unblock_client` looks the target client up in `blocked_clients`/`blocked_zset_clients`, sends a
`BlockedListResult::Unblocked(unblock_type)` / `BlockedZSetResult::Unblocked(unblock_type)` on
its channel (waking `wait_for_blocked_result` immediately), and strips its now-dead waiters from
every key queue it was registered under.

---

### 5. Cross-Component Interactions

- **`src/server.rs`**: the cross-shard receiver's `ShardMessage::NotifyList { keys }` handler
  locks the port's hub once and, for every key in the batch, calls **both** `hub.notify_list`
  and `hub.notify_zset` unconditionally (the sender doesn't track the target's value type, so
  it just tries both — a miss on the wrong map is a cheap no-op `HashMap` lookup).
- **`src/connection.rs`**: registers waiters for `BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/
  `BZPOPMAX`/`BZMPOP`/`XREAD BLOCK`; drives `wait_for_blocked_result`; owns
  `BlockedClientGuard`, `notify_list_or_defer`/`notify_zset_or_defer`, and the `MULTI`/`EXEC`
  pause/resume sequencing; calls `touch_watched_key` (Component 02's `WATCH` machinery) from
  inside every notify.
- **`src/table.rs`** (Component 05): `notify_list`/`notify_zset` call directly into
  `RudisTable::lpop`/`rpop`/`zpopmin`/`zpopmax`/`is_key_expired`/`exists` — `BlockHub` mutates
  the storage engine itself rather than being handed already-popped values.
- **`src/router.rs`** (Component 04): local writes broadcast `ShardMessage::NotifyList` to every
  *other* shard so a blocked client on shard A can be woken by a write on shard B.

---

### 6. Performance Characteristics

- **Not zero-overhead while blocked**: unlike a pure channel-based design, each blocked client
  costs a wakeup-and-poll cycle at most every 20ms (`wait_for_blocked_result`'s cap) purely to
  detect disconnection via `libc::poll`, in addition to being woken immediately (no polling
  delay) whenever a real `notify_list`/`notify_zset`/`notify_stream` fires.
- **One global mutex per port, held briefly**: every register/notify/unblock operation takes
  `PORT_BLOCK_HUBS`'s per-port `Mutex<BlockHub>` for a short, synchronous, non-`.await`-ing
  critical section (no lock is ever held across an `.await` point) — contention scales with how
  many shards are simultaneously registering or notifying blocking waiters, not with the number
  of ordinary (non-blocking) commands, which never touch this lock at all.
- **Transaction-batched wakeups**: the `pause`/`resume` mechanism (§4.4) turns what could be up
  to one wakeup attempt per write inside a large `MULTI`/`EXEC` into a single deferred batch
  processed once, after the transaction (and any cross-shard lock release) fully completes.

---

### 7. Future Improvements

- **Medium — replace `wait_for_blocked_result`'s active ≤20ms polling with real readiness notification (§4.2).** Polling `libc::poll`/`MSG_PEEK` every tick to detect a vanished client works but costs a syscall per blocked client per tick even when nothing has happened; since `monoio`'s `io_uring` driver already knows how to wait on fd readiness/hangup without polling, registering an explicit disconnect-watch operation on the ring (if `monoio` exposes one) would remove this cost and also lower worst-case disconnect-detection latency below the current 20ms cap.
- **Medium — implement real `CLIENT PAUSE`/`CLIENT UNPAUSE` semantics (§4.4).** They're currently a no-op `+OK` stub, distinct from the real (but differently-purposed) `pause`/`resume` used internally for `MULTI`/`EXEC` deferral. Since that internal mechanism already exists and does almost the right thing (defer notifications, replay after), extending it to also gate new-command acceptance for `CLIENT PAUSE`'s actual contract (pause all commands, or just writes, for a duration) is a smaller lift than building the feature from scratch.
- **Low — bound `pending_notifies`' growth during a very large `MULTI`/`EXEC`.** Every write inside a paused transaction appends to `pending_notifies` (§4.4) with no cap; a transaction touching an unusually large number of distinct keys could accumulate an unbounded `Vec` before `resume()` drains it. Unlikely to matter in practice (transaction size is bounded by client behavior), but worth a sanity cap if very large scripted transactions become common.
- **Low — consider giving `notify_zset`'s duplicate-suppression check (§4.3) and `notify_list`'s equivalent logic a shared helper** rather than two structurally-identical-but-separately-implemented sweeps, purely to reduce the chance the two drift apart if one gets a bugfix the other doesn't.

---
---

## Component 07: NVMe SSD Tiered Storage Engine (`src/tiering.rs`)

### 1. Architectural Purpose & Scope

`src/tiering.rs` implements Rudis's per-shard NVMe/disk offload engine. Each shard owns one
private tiered-storage file (`tier_shard_{id}.db` under a configured directory) and one
`ShardTierManager` that packs small values into 4KB pages (`SmallBins`), writes larger values
as their own aligned blocks, and lets `RudisTable` (`src/table.rs`) replace a hot in-RAM value
with a small pointer (`TieredPointer`) once it has been written to disk. Orchestration (when to
spill, when to reload, the auto-tiering trigger) lives in `src/router.rs`, not here — this file
is the disk I/O and page-packing layer underneath it.

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-local, `Rc`-based, not `Arc`/`Mutex`**: `ShardTierManager` holds `file: Rc<monoio::fs::File>` and uses `RefCell`/`Cell` internally — it is only ever used by the one shard that owns it, consistent with the rest of the shared-nothing architecture. Cross-shard tiering requests go through `ShardMessage::Tier*` variants (Component 04), not by sharing a `ShardTierManager` across threads.
2. **`O_DIRECT` is opt-in and falls back automatically.** `ShardTierManager::open` only attempts `O_DIRECT` if the `RUDIS_DIRECT_IO` environment variable is set to something other than `"0"`; if the `O_DIRECT` open call fails (common on filesystems/kernels that don't support it), it silently retries with a normal buffered open. There is no page-cache-bypass guarantee unless that env var is set *and* the underlying filesystem actually supports it.
3. **Global, per-port shared statistics behind a lock.** `TieringStats` (23 atomic counters) is stored in a process-wide `static TIER_STATS: RwLock<Option<HashMap<u16, Arc<TieringStats>>>>`, one entry per listening port, shared by every shard on that port. This is a real (small, read-mostly) synchronization point outside the shared-nothing data path, used purely for reporting (`INFO`-style stats), not for coordinating storage itself.
4. **Read coalescing, not hole punching, is the concurrency-sensitive part.** `OpManager::read_page_coalesced` ensures that if two in-flight reads target the same 4KB page, only one physical `read_exact_at` happens; the second caller waits on a `flume::bounded(1)` channel fed by the first.
5. **Write backpressure via a byte counter, not a queue depth.** `OpManager::check_write_backpressure` returns `true` once `pending_stash_bytes` (tracked via `AtomicUsize`, incremented in `start_pending_stash`/decremented in `finish_pending_stash`/`cancel_pending_stash`) exceeds a hardcoded 16MB; `ShardTierManager::stash_record` refuses new stashes (`io::ErrorKind::WouldBlock`) while over that limit.

---

### 3. Component Architecture & Data Structures

```
                  Hot (RudisValue::String/Int/List/Set/ZSet/...)
                                   │
                 Router::cool_local  (stash to disk, KEEP ram copy)
                                   ▼
     RudisValue::Cooled { ptr: TieredPointer, val: Box<RudisValue> }
                                   │
                 Router::spill_local / decommit_local (drop ram copy)
                                   ▼
                  RudisValue::Tiered(TieredPointer)     (ram: 4 bytes + tag)
                                   │
                 Router::load_local / stream_cold_read_local
                                   ▼
     RudisValue::Cooled { ptr, val }   <-- reload lands back in Cooled,
                                           NOT plain Hot (see §4.3)
```

#### Core real data structures

```rust
pub const PAGE_SIZE: usize = 4096;
pub const SMALL_VALUE_LIMIT: usize = 2048;   // records below this go into a SmallBin page
pub const TIER_MAGIC: &[u8; 4] = b"TIER";

pub struct TieredPointer {          // src/table.rs — 4+8+4+1 = 17 bytes, held inline in RudisValue
    pub file_id: u32,
    pub offset: u64,
    pub length: u32,
    pub value_type: u8,
}

pub struct ShardTierManager {
    pub shard_id: usize,
    pub port: u16,
    pub file: Rc<monoio::fs::File>,
    pub current_offset: Cell<u64>,      // next unused, page-aligned disk offset
    pub path: PathBuf,
    pub stats: Arc<TieringStats>,
    pub op_manager: Rc<OpManager>,
    pub small_bins: RefCell<SmallBinsManager>,
    pub is_direct_io: bool,
}

pub struct ActiveBin {                  // the one in-progress 4KB page being packed
    pub page_index: u64,
    pub buffer: Vec<u8>,
    pub items: Vec<SmallBinItem>,
}

pub struct SmallBinsManager {
    pub active_bin: Option<ActiveBin>,
    pub page_active_counts: HashMap<u64, usize>,  // live-record count per written page
    pub dead_pages: Vec<u64>,                     // pages whose count hit 0 -> GC candidates
}

pub struct OpManager {
    pub in_flight_reads: RefCell<HashMap<u64, Vec<flume::Sender<Result<Rc<Vec<u8>>, String>>>>>,
    pub pending_stashes: RefCell<HashSet<Bytes>>,
    pub pending_stash_bytes: AtomicUsize,
}
```

`TieringStats` has 23 fields (`tiered_keys`, `cooled_keys`, `disk_reads`, `disk_writes`,
`dead_bytes`, `ram_saved_bytes`, `coalesced_reads`, `gc_reclaimed_bytes`,
`offload_threshold_pct` (default 60), `upload_threshold_pct` (default 80), ...) — all
`AtomicU64`, updated from `router.rs`'s tiering methods (Component 04) and read by whatever
reports tiering stats (`INFO`-style output, not shown in this file).

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Opening the tier file and recovering the write cursor

```rust
pub async fn open(shard_id: usize, port: u16, dir: &Path) -> io::Result<Self> {
    let direct_io_enabled = std::env::var("RUDIS_DIRECT_IO").map(|v| v != "0").unwrap_or(false);
    let (file, is_direct) = if direct_io_enabled {
        let mut opts = monoio::fs::OpenOptions::new();
        opts.read(true).write(true).create(true);
        opts.custom_flags(libc::O_DIRECT);
        match opts.open(&path).await {
            Ok(f) => (f, true),
            Err(_) => { /* fall back to a normal buffered open */ }
        }
    } else { /* normal buffered open */ };

    let raw_len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let current_offset = (raw_len + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64 * PAGE_SIZE as u64;
    ...
}
```

On restart, the write cursor resumes from the file's current length, rounded up to the next
4KB boundary — so a shard restarted mid-page never overwrites a partially-written page, it
just leaves a small gap and starts a fresh page.

#### 4.2 Stashing a value: SmallBins packing vs. standalone aligned block

`stash_record` (called by `Router::spill_local`/`cool_local`, Component 04) branches purely on
size:

```rust
pub async fn stash_record(&self, key: &Bytes, val_payload: &[u8], val_type: u8) -> io::Result<TieredPointer> {
    if self.op_manager.check_write_backpressure() {
        return Err(io::Error::new(io::ErrorKind::WouldBlock, "write backpressure: stash buffer full"));
    }
    let record = encode_tiered_record(key, val_payload, val_type);
    ...
    if record_len < SMALL_VALUE_LIMIT {
        // pack into (or start a new) ActiveBin page; flush it once it can't fit
        // one more ~64-byte record (`should_flush = !ab.can_fit(64)`)
    } else {
        // flush whatever SmallBin is open, then write this record as its own
        // record_len-rounded-up-to-4KB standalone block at current_offset
    }
}
```

Records under 2KB (`SMALL_VALUE_LIMIT`) get packed multiple-to-a-page into the shard's single
`ActiveBin`; a page is flushed to disk (`flush_active_bin`, one `write_all_at` per page) either
when a new record won't fit or when it's nearly full (heuristically, when there's no room left
for one more minimal ~64-byte record). Records at or above 2KB skip bin-packing entirely,
flush whatever bin is currently open first (so nothing gets reordered on disk relative to the
in-memory `current_offset` cursor), then get written as their own page-aligned block.

#### 4.3 The on-disk record format (CRC64-checked, not just a length prefix)

```rust
pub fn encode_tiered_record(key: &[u8], val_payload: &[u8], val_type: u8) -> Vec<u8> {
    // TIER_MAGIC(4) | value_type(1) | key_len(4) | val_len(4) | crc64(8) | key | val_payload
}
```

`decode_tiered_record` verifies the magic bytes, the `value_type` byte matches what the caller
expected, and recomputes a CRC64 (`crate::table::crc64`) over `key + val_payload` before
trusting the bytes — a corrupt or torn write is detected and surfaced as an `io::Error`
(`"crc mismatch on tiered read"`) rather than silently returning garbage.

#### 4.4 Reading a record back: page-read coalescing, and where the RAM copy lands

```rust
pub async fn read_tiered_record(...) -> io::Result<(Bytes, Vec<u8>)> {
    let offset_in_page = (ptr.offset % PAGE_SIZE as u64) as usize;
    if offset_in_page + len <= PAGE_SIZE {
        // check the still-open ActiveBin first — the record may not be flushed yet
        // otherwise: op_manager.read_page_coalesced(file, page_start, stats).await
    } else {
        // record spans/exceeds a page (a standalone large block) — direct read_exact_at
    }
    decode_tiered_record(&data, ptr.value_type)
}
```

`read_page_coalesced` (§2.4) means N concurrent readers of the same still-warm 4KB page cause
exactly one `read_exact_at` syscall; everyone else gets a clone of the same `Rc<Vec<u8>>`.

Critically — and unlike the original design's simple "promote back to hot" description —
`Router::load_local` (which calls this) does **not** turn a `RudisValue::Tiered(ptr)` back into
a plain hot value. It calls `RudisTable::restore_tiered_value`, which sets the slot to
`RudisValue::Cooled { ptr, val: Box::new(decoded_val) }` — i.e. a `Tiered` read always lands as
`Cooled` (RAM-resident *and* still disk-backed), never straight back to a bare `String`/`List`/
etc. Something has to explicitly `decommit` a `Cooled` entry (or it has to be spilled again) to
either free the RAM copy (back to `Tiered`) or fully rejoin the "hot" set — there is no direct
`Cooled → Hot` transition in the code; `Cooled` behaves as a permanent write-through cache
layer once a key has ever been tiered.

#### 4.5 Garbage collection: dead-page tracking + `fallocate` hole punching

```rust
pub fn on_key_deleted(&self, ptr: TieredPointer) {
    if (ptr.length as usize) < SMALL_VALUE_LIMIT {
        // decrement that page's live-record count; if it hits 0, queue the page in dead_pages
    } else {
        // large standalone block: punch the hole immediately, no waiting
    }
}

pub fn run_gc(&self) -> usize {
    let dead = std::mem::take(&mut self.small_bins.borrow_mut().dead_pages);
    for page_idx in &dead {
        Self::punch_hole(&self.file, page_idx * PAGE_SIZE as u64, PAGE_SIZE as u64, &self.stats);
    }
    dead.len() * PAGE_SIZE
}
```

Because SmallBins pack multiple keys per 4KB page, a single deleted key can't reclaim its page
immediately — `SmallBinsManager` tracks a live-record count per page and only queues the page
for `fallocate(FALLOC_FL_PUNCH_HOLE)` once every record on it has been deleted. Standalone
large-value blocks (≥2KB) are punched immediately on delete since they aren't shared with
anything else. `run_gc` is invoked periodically from `src/server.rs`'s 2-second GC task
(Component 01).

#### 4.6 Snapshotting: reflink-first, `copy_file_range` fallback

```rust
pub fn snapshot_file(src_path: &Path, dst_path: &Path) -> io::Result<bool> {
    // Try FICLONE (0x40049409) ioctl first — an instant CoW reflink on filesystems
    // that support it (btrfs, XFS with reflink, some overlay setups).
    // On failure, fall back to looping libc::copy_file_range in 16MB chunks,
    // and if THAT fails too, fall back again to std::fs::copy.
}
```

`ShardTierManager::snapshot` flushes the active bin and `sync_all`s the file first, then calls
`snapshot_file`, then writes a small plain-text manifest (`version`/`shard_id`/`file_size`/
`is_reflink`/`current_offset`) next to the backup — three fallback tiers for the actual copy,
in order of cost: reflink (instant, metadata-only) → `copy_file_range` (in-kernel copy, no
userspace round-trip) → a plain buffered `std::fs::copy`.

---

### 5. Cross-Component Interactions

- **`src/table.rs`** (Component 05): owns the `RudisValue::Tiered`/`RudisValue::Cooled`
  variants and the state-transition methods this file's callers use
  (`set_tiered_pointer`, `set_cooled_pointer`, `restore_tiered_value`, `get_value_for_spill`,
  `decommit_all_cooled`, `get_hot_keys_for_spill`, `is_tiered`, `is_cooled`); also owns
  `serialize_val_payload`/`deserialize_val_payload`, the value-encoding format this file's
  records carry as their payload.
- **`src/router.rs`** (Component 04): the actual orchestration layer —
  `spill_local`/`cool_local`/`load_local`/`stream_cold_read_local`/`decommit_local`/
  `check_auto_tier` decide *when* to call into this file's `ShardTierManager`, using the real
  `offload_threshold_pct`/`upload_threshold_pct` from `TieringStats` and `get_hot_keys_for_spill`
  to pick candidates.
- **`src/shard.rs`**: `ShardDb.tier_manager: Option<Rc<ShardTierManager>>` — one manager
  instance per shard, created during shard startup (Component 01 §4.1 step 7).
- **`src/connection.rs`** (Component 02): a `GET` on a key whose value is
  `RudisValue::Tiered`/`Cooled` falls through to `stream_cold_read_local`/`load_local` rather
  than being served directly from the table.
- **`src/main.rs`** / **`src/server.rs`**: `RUDIS_DIRECT_IO` is read as a process environment
  variable, not a CLI flag; `--maxmemory`/`--tiered-offload-threshold`/
  `--tiered-upload-threshold` (Component 01) feed `set_max_memory`/`set_offload_threshold_pct`/
  `set_upload_threshold_pct` in this file.

---

### 6. Performance Characteristics

- **`O_DIRECT` is conditional, not guaranteed** (§2.2) — actual page-cache-bypass behavior
  depends on `RUDIS_DIRECT_IO` being set and the filesystem/kernel actually honoring the flag;
  silently falls back to normal buffered I/O otherwise.
- **Read coalescing collapses concurrent hot-page reads** to one physical read plus N
  in-memory channel deliveries, avoiding redundant disk I/O when several keys on the same
  4KB SmallBin page are accessed close together.
- **Write backpressure is a simple byte-budget gate** (16MB of in-flight stash data), not a
  queue-depth or per-key limit — a burst of large concurrent spills can hit it and get
  `WouldBlock` back to the caller.
- **GC reclaims whole 4KB pages, not individual records** — a page with even one surviving
  record can't be punched; deletion-heavy small-value workloads can accumulate dead-but-unfreed
  bytes (`dead_bytes` stat) until every record sharing a page happens to be deleted.
- **Snapshotting cost depends entirely on filesystem reflink support** — instant on
  btrfs/XFS-with-reflink, an in-kernel `copy_file_range` loop otherwise (still avoiding a
  full userspace read+write round trip), and only falls all the way back to `std::fs::copy`
  if both kernel-assisted paths are unavailable.

---

### 7. Future Improvements

- **Medium — add a `Cooled → Hot` transition (§4.4).** Today `Cooled` is a one-way permanent write-through layer: once a key has ever been tiered, every future read/write pays the bookkeeping overhead of maintaining a `TieredPointer` alongside the RAM value, even if the key becomes consistently hot again. A simple heuristic (e.g. N consecutive accesses without a re-spill, or a periodic sweep during low memory pressure) that fully promotes a long-stable `Cooled` entry back to a bare hot value — freeing the disk pointer and its GC bookkeeping — would avoid permanently taxing keys that were only cold briefly.
- **Medium — support partial-page compaction, not just whole-page GC (§4.5).** `run_gc` can only reclaim a 4KB `SmallBins` page once every record on it has been deleted; a page with one long-lived survivor among many deleted neighbors stays fully allocated indefinitely. A periodic "read the survivors, repack into a fresh page, punch the old one" compaction pass (amortized, background, rate-limited like the existing 2s GC task) would bound worst-case `dead_bytes` growth under delete-heavy small-value workloads.
- **Low — surface `O_DIRECT` fallback as a visible event, not just a silent retry (§4.1/§2.2).** An operator who sets `RUDIS_DIRECT_IO=1` expecting page-cache bypass has no way to discover the open silently fell back to buffered I/O (e.g. an unsupported filesystem) short of instrumenting the syscalls themselves. A one-time log line or a `TieringStats` flag would make this observable.
- **Low — make the 16MB write-backpressure threshold and the 2KB SmallBins cutoff configurable** (§2.5/§4.2) rather than hardcoded constants, so tiering behavior can be tuned per deployment (fast NVMe vs. slower SSD, high-value-count vs. large-value-heavy workloads) without a rebuild.

---
---

## Component 08: Vector Search Engine: HNSW, SQ8 & Product Quantization (`src/vector.rs`)

### 1. Architectural Purpose & Scope

`src/vector.rs` implements an in-memory approximate nearest-neighbor (ANN) vector index:
a **Hierarchical Navigable Small World (HNSW)** graph (`HnswIndex`), an **8-bit scalar
quantization** scheme (`QuantizedVector`), and a **Product Quantization with Asymmetric
Distance Computation** scheme (`ProductQuantizer`/`PQVector`). It is exposed to clients
through five bespoke commands parsed in `src/resp.rs` and dispatched in `src/connection.rs`:
`VADD`, `VQUERY`, `VSIM`, `VDEL`, `VINFO`. There is no `FT.SEARCH ... KNN` integration —
that syntax does not exist anywhere in this codebase; full-text search (`src/search.rs`,
Component 09) is a separate engine with no code-level link to this one.

**Each shard owns a completely independent set of named indexes** (`ShardDb.vector_indexes:
HashMap<String, HnswIndex>`), and every vector command only ever touches
`router.local_db` — there is no cross-shard routing for `VADD`/`VQUERY`/`VSIM`/`VDEL`/`VINFO`
at all (confirmed: none of the five appear in `target_shard_of_cmd`, and none of `Router`'s
methods reference the vector engine). This means an index named `"products"` on shard 0 and
an index named `"products"` on shard 1 are two entirely separate, unrelated HNSW graphs —
which shard a given connection lands on (decided by the kernel via `SO_REUSEPORT`, per
Component 01) silently determines which index a `VADD`/`VQUERY` actually reads or writes.
There is no fan-out, no merge, and no consistency check across shards. Treat this as the
single most important operational caveat for this subsystem.

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-local, not cross-shard**: consistent with the rest of the codebase, `HnswIndex`
   instances live inside one shard's `ShardDb` with no locks — but unlike the key-value
   store, there is no `ShardMessage` variant to reach a vector index on another shard at all
   (see §1). This isn't a locking decision, it's simply unimplemented cross-shard support.
2. **Runtime AVX2 detection, x86_64 only**: `dot_product`/`l2_distance_sq` check
   `is_x86_feature_detected!("avx2")`/`"fma"` at call time and fall back to a portable
   8-lane-unrolled scalar implementation otherwise. There is **no ARM/NEON code path** —
   only `#[cfg(target_arch = "x86_64")]` SIMD kernels exist; any other architecture always
   takes the portable path.
3. **All three metrics return a "smaller is closer" distance, not a raw similarity score**:
   `VectorMetric::IP` (inner product) returns `-dot_product(a, b)` specifically so that, like
   `L2` and `Cosine`, a smaller returned value always means "more similar" — letting
   `search_layer`'s single min/max-heap logic work identically regardless of metric.
4. **Product Quantization codebooks are not trained on data.** `ProductQuantizer::new`
   generates each subvector's 256 centroids deterministically: centroid 0 is the zero
   vector, centroids `1..=d_sub` are positive unit basis vectors, `d_sub+1..=2*d_sub` are
   negative unit basis vectors, and the remainder are filled by a fixed SplitMix64-style
   hash of `(centroid_id, subvector_id, dim_id)` mapped into `[-1, 1]`. There is no k-means
   or any training pass over real vectors — every `ProductQuantizer` for a given `(dim, m)`
   produces byte-for-byte identical codebooks. Real PQ implementations cluster the actual
   data distribution; this one does not, which will cost recall accordingly.
5. **HNSW layer assignment is deterministic across index instances.** `HnswIndex::new`
   seeds a custom xorshift64 PRNG (`rng_state`) with the fixed constant
   `0x853c49e6748fea9b` every time — not from OS randomness, the clock, or the index name.
   Two indexes built by inserting the same vectors in the same order will have identical
   graph topology.

---

### 3. Component Architecture & Data Structures

```
                 VADD index key <floats...> [QUANTIZE|SQ8] [PQ] [TIERED]
                 VQUERY index k <floats...> [RERANK]
                 VSIM index key1 key2 [METRIC ...]
                 VDEL index key
                 VINFO index
                                     │
                        ShardDb.vector_indexes["index"]  (per-shard, independent)
                                     │
                                     ▼
                              HnswIndex
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
         nodes: Vec<Option<HnswNode>>          key_to_id: HashMap<Bytes, usize>
         (tombstoned via None on delete,          (external key -> internal id)
          never compacted)
                    │
                    ▼
         HnswNode { vector: Vec<f32>, quantized: Option<QuantizedVector>,
                     pq: Option<PQVector>, neighbors: Vec<Vec<usize>> }
                     (neighbors[layer] = adjacency list at that layer)
```

#### Real core types

```rust
pub enum VectorMetric { Cosine, L2, IP }   // Cosine is Default

pub struct HnswIndex {
    pub name: String,
    pub dim: usize,
    pub metric: VectorMetric,
    pub m: usize,               // 16 — max neighbors per node, layer > 0
    pub m0: usize,              // 32 — max neighbors per node, layer 0
    pub ef_construction: usize, // 64
    pub ef_search: usize,       // 32
    pub ml: f64,                // 1 / ln(m) — level-generation scale
    pub entry_point: Option<usize>,
    pub max_layer: usize,
    pub nodes: Vec<Option<HnswNode>>,
    pub key_to_id: HashMap<Bytes, usize>,
    pub pq_quantizer: Option<ProductQuantizer>,
    rng_state: u64,             // fixed-seed xorshift64, see §2.5
}

pub struct HnswNode {
    pub id: usize,
    pub key: Bytes,
    pub vector: Vec<f32>,             // full-precision vector always kept
    pub quantized: Option<QuantizedVector>,  // SQ8, if requested
    pub pq: Option<PQVector>,                // PQ codes, if requested
    pub is_tiered: bool,
    pub neighbors: Vec<Vec<usize>>,   // one adjacency list per layer this node exists on
}

pub struct QuantizedVector {
    pub min_val: f32,
    pub scale: f32,      // (max - min) / 255
    pub sum_q: f32,      // precomputed for fast cosine norm
    pub sum_q_sq: f32,
    pub data: Vec<u8>,
}

pub struct PQVector { pub codes: Vec<u8> }   // one byte (0-255) per subvector

pub struct ProductQuantizer {
    pub dim: usize,
    pub m: usize,                       // number of subvectors
    pub d_sub: usize,                   // dim / m
    pub codebooks: Vec<Vec<Vec<f32>>>,  // [subvector][centroid 0..256][d_sub floats]
}
```

Note the node always keeps its full `Vec<f32>` regardless of whether SQ8/PQ is also
enabled — quantization here is an additional fast-path structure for candidate scoring,
not a memory-savings replacement for the raw vector (the "reranking" pass in §4.3 depends
on the exact float vector still being present).

---

### 4. Execution Algorithms & Code Logic

#### 4.1 SIMD distance kernels

```rust
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    { if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        return unsafe { dot_product_avx2(a, b) };
    } }
    dot_product_portable(a, b)
}
```

`dot_product_avx2`/`l2_distance_sq_avx2` process 16 floats per iteration (two accumulated
`__m256` lanes via `_mm256_fmadd_ps`), with an 8-wide tail and a scalar remainder — real
FMA-fused AVX2, not a placeholder. `dot_f32_u8_avx2`/`l2_f32_u8_avx2` do the same for a
`f32` query against a `u8`-quantized vector (`_mm256_cvtepu8_epi32` widening + convert),
used by `QuantizedVector::compute_distance` so SQ8-accelerated scoring is itself
SIMD-accelerated, not just smaller.

`compute_distance` unifies the three metrics into one "smaller is closer" scale:

```rust
pub fn compute_distance(a: &[f32], b: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::L2 => l2_distance_sq(a, b).sqrt(),
        VectorMetric::IP => -dot_product(a, b),
        VectorMetric::Cosine => {
            let dot = dot_product(a, b);
            let norm_a = dot_product(a, a).sqrt();
            let norm_b = dot_product(b, b).sqrt();
            if norm_a == 0.0 || norm_b == 0.0 { 1.0 }
            else { (1.0 - (dot / (norm_a * norm_b))).max(0.0) }
        }
    }
}
```

#### 4.2 SQ8 quantization: precomputed sums avoid dequantizing on every comparison

`QuantizedVector::quantize` linearly maps each float into `[0, 255]` using the vector's own
min/max (`scale = (max - min) / 255`), and precomputes `sum_q`/`sum_q_sq` once at insert
time. `compute_distance` (query vs. stored SQ8 vector) then reconstructs the true dot
product / L2 / cosine algebraically from `dot_u8` (float·u8 SIMD dot) plus the precomputed
sums, without ever materializing a dequantized `Vec<f32>` — e.g. inner product is
`min_val * sum_query + scale * dot_u8`. `dequantize()` (full float reconstruction) exists
but is not on this hot path.

#### 4.3 HNSW insertion (`add_quantized_ext`)

```rust
let target_level = self.random_level();     // -ln(rand) * ml, capped at 16
...
// 1. Greedy descent from entry_point through layers ABOVE target_level
for lc in (target_level + 1..=self.max_layer).rev() { /* follow nearest neighbor at lc */ }
// 2. From target_level down to 0: beam search (search_layer) at each layer,
//    keep the nearest m_max candidates (m0 at layer 0, m elsewhere) as new neighbors,
//    link bidirectionally, and prune any neighbor whose list now exceeds m_max
for lc in (0..=target_level.min(self.max_layer)).rev() { ... self.prune_neighbors(...) }
if target_level > self.max_layer { self.max_layer = target_level; self.entry_point = Some(new_id); }
```

`prune_neighbors` prunes by **plain nearest-distance truncation** (sort candidates by
distance to the node, keep the closest `max_neighbors`) — not the original HNSW paper's
diversity-aware neighbor-selection heuristic. Simpler, but can leave the graph less
navigable under adversarial insertion orders than the paper's algorithm.

#### 4.4 Search (`search`/`search_tiered`) with optional exact rerank

```rust
pub fn search_tiered(&self, query: &[f32], k: usize, rerank: bool) -> Vec<(Bytes, f32)> {
    // 1. Greedy descent through layers max_layer..=1 (single nearest-neighbor hop per layer)
    // 2. Beam search at layer 0 via search_layer(query, curr_obj, search_ef, 0)
    //    where search_ef = ef_search.max(if rerank { k*3 } else { k })
    if rerank {
        // 3. Re-score every candidate with compute_distance() on the EXACT f32 vector
        //    (bypassing any SQ8/PQ approximation used during graph traversal), re-sort, truncate to k
    } else {
        // return the approximate candidates as-is, truncated to k
    }
}
```

`dist_to_node` (used throughout traversal) prefers `PQVector`+`ProductQuantizer` ADC if
both are present, else `QuantizedVector` SQ8, else the exact float vector — so a node
built with `PQ` or `QUANTIZE` is scored approximately during graph traversal, and only
gets compared against the true vector if the caller passes `RERANK` (`Command::Vquery {
rerank, .. }`).

#### 4.5 PQ encode + ADC scoring

```rust
pub fn compute_distance_table(&self, query: &[f32]) -> Vec<[f32; 256]> {
    // one 256-entry L2 table per subvector, precomputed once per query
}
pub fn compute_distance_adc(&self, table: &[[f32; 256]], pq: &PQVector) -> f32 {
    pq.codes.iter().enumerate().map(|(m, &c)| table[m][c as usize]).sum()
}
```

Classic ADC: encode the query's distance to all 256 centroids per subvector once, then
score every stored PQ-coded vector as a sum of table lookups — no per-candidate float
math, at the cost of the codebook-quality caveat in §2.4.

#### 4.6 Deletion leaves tombstoned slots, no compaction

`remove(key)` walks every layer's neighbor list to strip references to the removed id,
sets `self.nodes[id] = None` (leaving a hole — ids are never reused or compacted), and if
the removed node was the entry point, picks the first `Some` slot in `nodes` as the new
one (`self.nodes.iter().position(|n| n.is_some())`) — not necessarily a well-connected or
central node, just the first surviving slot.

---

### 5. Cross-Component Interactions

- **`src/resp.rs`**: parses `VADD`/`VQUERY`/`VSIM`/`VDEL`/`VINFO` into `Command` variants;
  `VADD`'s `metric` field is always parsed as `None` (per-call metric override isn't
  actually accepted on `VADD` — the metric is fixed at index-creation time only).
- **`src/shard.rs`**: `ShardDb::vadd` lazily creates the `HnswIndex` on first use
  (`vector_indexes.entry(index_name).or_insert_with(...)`), defaulting the metric to
  `VectorMetric::Cosine` if the index didn't already exist; `vsim` allows a one-off
  `metric_override` for that single comparison without changing the index's stored metric.
- **`src/connection.rs`**: dispatches all five commands straight to
  `router.local_db.borrow()[_mut]()` — no `target_shard_of_cmd` entry, no remote path (§1).
- **`src/search.rs`** (Component 09): no code-level relationship — separate engine, despite
  both being "search" subsystems.
- **`src/table.rs`**: no relationship — vector data lives entirely in `ShardDb.vector_indexes`,
  not in `RudisValue`/`RudisTable` at all.

---

### 6. Performance Characteristics

- Distance kernels are genuinely AVX2+FMA accelerated at 16 floats/iteration when the CPU
  supports it, with a correct portable fallback otherwise — no unconditional `unsafe` on
  unsupported hardware.
- SQ8 scoring reuses the same AVX2 kernels against `u8` data, so approximate scoring during
  graph traversal is not meaningfully slower per-comparison than exact float scoring.
- No numbers in this document are benchmarked — the previous version's "\>3,500 vectors/sec",
  "\<400µs p99", and "75% RAM reduction" figures were unsourced and have been removed rather
  than repeated unverified. SQ8's memory reduction ratio (4 bytes/dim -> 1 byte/dim, i.e. 4x
  smaller for the quantized copy, kept *alongside* the original `Vec<f32>` per §3's note) is
  the one ratio derivable directly from the type definitions, not from measurement.

---

### 7. Future Improvements

- **High — give vector indexes cross-shard reach (§1).** This is the single most important gap in this subsystem: an index name is currently silently scoped to whichever shard happened to receive the `VADD`/`VQUERY` connection, with no fan-out, no merge, and no error telling the caller their view is partial. At minimum, either (a) route all vector commands for a given index name to one designated "owner" shard (hash the index name, forward via a new `ShardMessage::Vector*` variant, mirroring how `src/block.rs`/`src/search.rs` already accept a narrow cross-shard exception for subsystems that need global visibility), or (b) document loudly at the protocol level (a startup warning, or a real error if `VADD`/`VQUERY` land on different shards for the same index) so this isn't a silent correctness surprise.
- **Medium — train PQ codebooks on real data (§2.4).** The current fixed deterministic basis (never trained via k-means or any clustering pass) will under-perform a real Product Quantization implementation on actual data distributions — recall will be measurably worse than the "PQ" name implies. A one-time or periodic k-means pass over inserted vectors (even a simple mini-batch k-means) per subvector would bring this in line with what PQ is normally expected to deliver.
- **Medium — support index persistence.** No `Cross-Component Interactions` entry connects `HnswIndex`/`ShardDb.vector_indexes` to the RDB save/restore path (Component 05/14) — a restart appears to lose all vector indexes with no explicit warning. Either wire `VADD`-built indexes into the RDB chunk format so they survive a restart, or document explicitly (in `VINFO`'s output, and in user-facing docs) that vector indexes are ephemeral today.
- **Low — implement the paper's diversity-aware neighbor selection instead of plain nearest-distance pruning (§4.3).** `prune_neighbors`'s simple truncation is simpler and cheaper but can leave the HNSW graph less navigable under adversarial or highly-clustered insertion orders than the original algorithm's heuristic — worth revisiting if recall on real workloads underperforms expectations.
- **Low — seed `rng_state` from something other than a fixed constant outside of test contexts (§2.5)**, so two indexes inserting the same vectors in the same order don't necessarily produce identical graph topology in production — currently a reasonable choice for reproducible tests, but worth an explicit opt-out for real deployments if graph-topology diversity ever matters for load distribution or resilience.

---
---

## Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion (`src/search.rs`)

### 1. Architectural Purpose & Scope

`src/search.rs` provides an in-memory full-text search and indexing engine compatible with a
subset of RediSearch (`FT.CREATE`, `FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX`, `FT.EXPLAIN`,
`FT.ADD`). It supports multi-field schema definitions (`TEXT`, `TAG`, `NUMERIC`, `VECTOR`), a
hand-written inverted-index posting-list structure, real Okapi BM25 relevance scoring, a small
RediSearch-like query-string parser (`parse_query`/`QueryAst`), and Reciprocal Rank Fusion for
merging two ranked result lists. Auto-indexing is wired into `HSET`/`HMSET`/`JSON.SET` (root
path only) in `src/connection.rs`.

---

### 2. Key Invariants & Concurrency Constraints

1. **Automatic Document Ingestion, but only for `HSET`/`HMSET`/`JSON.SET`**: every `HSET`,
   `HMSET`, and `JSON.SET key $ ...` call in `connection.rs` calls
   `crate::search::index_document_hook(key, str_fields)` after the write succeeds, converting
   whatever field values it has into `HashMap<String, String>` and re-indexing that document
   against every index whose prefix matches. Other write paths (`SET`, `LPUSH`, ...) do **not**
   trigger re-indexing.
2. **The index registry is a real, global, cross-shard-shared data structure — not
   thread-local.** Despite this looking like a per-shard subsystem, `SEARCH_INDICES` is a
   single process-wide `static LazyLock<RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>>`.
   Every shard thread that handles an `HSET`/`FT.SEARCH`/etc. call takes a real
   `std::sync::RwLock` read or write lock on it. This is a genuine, deliberate exception to
   the shared-nothing/zero-lock architecture — the same category of exception as `BlockHub`
   (Component 06) — needed because a search index has to see writes from every shard, not just
   the one that happens to own a given key's slot.
3. **Deterministic Tokenization with a real (small) stop-word list and a heuristic stemmer**:
   `tokenize_text` splits on non-alphanumeric characters, lowercases, drops any token in the
   ~170-word `ENGLISH_STOP_WORDS` set, and optionally passes survivors through `simple_stem` — a
   handful of suffix-stripping rules (`-ing`, `-ies`→`-y`, `-es`, `-ed`, trailing `-s`), not a
   real Porter/Snowball stemmer.
4. **Real BM25, but with a single whole-document length, not one length per field.** `DocMeta`
   stores one `doc_len: usize` — the total token count across *all* indexed text fields of a
   document combined — and `InvertedIndex::avg_doc_len()` is `total_terms / total_docs` across
   the whole index, not per field. This is architecturally simpler than genuine multi-field
   BM25F (which needs per-field lengths/weights) despite `FieldType::Text` carrying a `weight`
   field — that `weight` is accepted by `FT.CREATE`'s parser but **`bm25_score` never reads
   it** (verified: no reference to any field's `weight` anywhere in the scoring function).

---

### 3. Component Architecture & Data Structures

```
     Raw Document: HSET "doc:1" title "Rust Systems" body "Distributed io_uring"
                                    │
                                    ▼ index_document_hook() — checked against EVERY
                                      registered index's prefixes, not just one
                         tokenize_text() + simple_stem()
                                    │
                  ┌─────────────────┴─────────────────┬───────────────┐
                  ▼                                   ▼               ▼
             Text Fields                         Tag Fields    Numeric Fields
        ["rust", "system"]  (stemmed)        {"tech","db"}        99.5
                  │
                  ▼
        InvertedIndex.inverted: HashMap<String, Vec<Posting>>
        "rust"   -> [Posting{doc_id:"doc:1", term_freq:1, positions:[0]}]
        "system" -> [Posting{doc_id:"doc:1", term_freq:1, positions:[1]}]
```

#### Real Data Structures (`src/search.rs`)

```rust
pub enum FieldType {
    Text { weight: f64, sortable: bool, nostem: bool },
    Numeric { sortable: bool },
    Tag { separator: char, casesensitive: bool },
    Vector { dim: usize, distance_metric: String, algorithm: String },
}

pub struct IndexSchema {
    pub name: String,
    pub on_type: String,       // "HASH" or "JSON", from FT.CREATE ... ON <type>
    pub prefixes: Vec<String>, // FT.CREATE ... PREFIX <n> <p1> <p2> ...
    pub fields: HashMap<String, FieldType>,
}

pub struct Posting {
    pub doc_id: String,       // real docs are string keys, not u32 ids
    pub term_freq: u32,
    pub positions: Vec<u32>,  // token positions are tracked but never used for phrase queries
}

pub struct DocMeta {
    pub doc_id: String,
    pub doc_len: usize,       // whole-document token count, not per-field
    pub fields: HashMap<String, String>,
    pub numeric_fields: HashMap<String, f64>,
    pub tag_fields: HashMap<String, HashSet<String>>,
    pub vector_fields: HashMap<String, Vec<f32>>,
}

pub struct InvertedIndex {
    pub schema: Option<IndexSchema>,
    pub inverted: HashMap<String, Vec<Posting>>, // term -> postings
    pub docs: HashMap<String, DocMeta>,           // doc_id -> metadata
    pub total_docs: usize,
    pub total_terms: usize,
}

// Real global registry — see §2.2
static SEARCH_INDICES: LazyLock<RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
```

There is **no `vector_index: Option<HnswIndex>` field anywhere** — vectors are stored per-document
as plain `Vec<f32>` inside `DocMeta.vector_fields`, and KNN search (§4.4) is a brute-force linear
scan, not an HNSW lookup. `src/vector.rs`'s `HnswIndex` (Component 08) is a completely separate
data structure used only by the standalone `VECTOR.*`-style commands, not by this file.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 BM25 Scoring — real formula, whole-document length

```rust
pub fn bm25_score(&self, term: &str, posting: &Posting, doc_len: usize) -> f64 {
    let n = self.inverted.get(term).map(|v| v.len()).unwrap_or(0);
    if n == 0 || self.total_docs == 0 { return 0.0; }
    let total_docs = self.total_docs as f64;
    let idf = ((total_docs - n as f64 + 0.5) / (n as f64 + 0.5) + 1.0).ln();
    if idf <= 0.0 { return 0.0001; }  // floor for very-common terms, not in the classic formula

    let k1 = 1.2;
    let b = 0.75;
    let avgdl = self.avg_doc_len().max(1.0);
    let freq = posting.term_freq as f64;
    let tf = (freq * (k1 + 1.0)) / (freq + k1 * (1.0 - b + b * (doc_len as f64 / avgdl)));
    idf * tf
}
```

$k_1 = 1.2$, $b = 0.75$ and the IDF smoothing term match the textbook Okapi BM25 formula
exactly. The one deviation from the classic formula is the `idf <= 0.0` floor (returns `0.0001`
instead of a zero/negative score) — this matters for very common terms in a small corpus where
the classic IDF can go negative or zero, which would otherwise make that term's contribution
vanish or invert; the floor keeps it a small positive tiebreaker instead. `doc_len` is passed in
by the caller as `doc.doc_len` — the whole document's combined token count across every indexed
text field, not the length of just the field the term was found in.

#### 4.2 Query Parsing (`parse_query` / `QueryAst`) — real and considerably richer than posting lists alone

```rust
pub enum QueryAst {
    Term(String), Prefix(String), Exact(String),
    FieldScope { field: String, inner: Box<QueryAst> },
    NumericRange { field: String, min: f64, max: f64 },
    TagFilter { field: String, tags: Vec<String> },
    And(Vec<QueryAst>), Or(Vec<QueryAst>), Not(Box<QueryAst>),
    KnnVector { field: String, k: usize, query_vec: Vec<f32>, param_name: String },
    MatchAll,
}
```

`parse_query` hand-parses a real subset of RediSearch query syntax: bare words (AND'd, each
stemmed via `simple_stem`), `word*` prefix matches, `-term` negation, `@field:[min max]`
numeric range, `@field:{tag1|tag2}` tag filters, `@field:...` scoped sub-queries, `term1 | term2`
top-level OR, and a `*=>[KNN <k> @<field> $<param>]` suffix for hybrid vector search appended to
a base query. `execute_search` recursively walks this AST, building a `HashMap<doc_id, f64>` of
candidate scores per node (`And` intersects with position-preserved score accumulation, `Or`
unions with accumulation, `Not` inverts against the full doc set), then sorts by `sortby` if
given or by score descending, and paginates by `offset`/`limit`.

#### 4.3 KNN vector search — a previously-verified dead-code gap, now fixed

An earlier version of this doc documented a precise, verified dead-code gap here: `parse_query`'s
`KnnVector` node was always built with `query_vec: Vec::new()`, and nothing ever populated it
from `PARAMS`, so `*=>[KNN ...]` queries silently matched zero vectors. **That has since been
fixed.** `QueryAst::KnnVector` now carries a `param_name: String` alongside `query_vec`, captured
from the query string itself:

```rust
let param_name = tokens.get(2).map(|s| s.trim_start_matches('$').to_string()).unwrap_or_default();
let knn_ast = QueryAst::KnnVector { field, k, query_vec: Vec::new(), param_name };
```

and `execute_search`'s `KnnVector` arm now resolves an `effective_vec` at query time — using
`query_vec` directly if it's already non-empty (e.g. for callers that build a `QueryAst`
programmatically), otherwise looking `param_name` up in `opts.params` (trying both the bare name
and a `$`-prefixed variant, so it matches however the caller keyed it) and decoding it via the
new `parse_vector_blob`:

```rust
let effective_vec = if !query_vec.is_empty() {
    query_vec.clone()
} else if !param_name.is_empty() {
    opts.params.get(param_name).or_else(|| opts.params.get(&format!("${}", param_name)))
        .map(|bytes| parse_vector_blob(bytes)).unwrap_or_default()
} else { Vec::new() };
```

`parse_vector_blob` accepts two real formats, chosen automatically by a length check — not by an
explicit format flag:

```rust
pub fn parse_vector_blob(bytes: &[u8]) -> Vec<f32> {
    if bytes.len().is_multiple_of(4) && !bytes.is_empty() {
        // interpret as tightly-packed little-endian f32s, 4 bytes each
        let (chunks, _) = bytes.as_chunks::<4>();
        chunks.iter().map(|c| f32::from_le_bytes(*c)).collect()
    } else {
        // fall back to a comma/whitespace/bracket-separated text encoding, e.g. "1.0, 0.0, 0.0"
        String::from_utf8_lossy(bytes)
            .split(|c: char| c == ',' || c.is_whitespace() || c == '[' || c == ']')
            .filter_map(|s| s.trim().parse::<f32>().ok())
            .collect()
    }
}
```

The auto-indexing path (`index_document_hook` → `InvertedIndex::add_document`) got the matching
other half of this fix: a document field declared `Vector` in the schema is now run through the
same `parse_vector_blob` at index time (previously `vector_fields` was only ever populated by
whatever the `vectors` parameter passed in directly, which nothing supplied via the normal
`HSET`-driven ingestion path — so indexed documents' vector fields were themselves silently
empty before this change, a second half of the same gap not fully called out in the original
finding). **Hybrid keyword+vector search via `FT.SEARCH`'s KNN syntax is now a real, working
path end to end**, verified by a new test (`test_knn_vector_search_with_params`) that indexes two
documents with text-encoded vectors, queries with a raw-little-endian-float `PARAMS` blob, and
asserts the nearer document is returned.

**One real ambiguity worth knowing about `parse_vector_blob`'s auto-detection**: a byte string
that happens to be a multiple of 4 bytes long is *always* treated as packed floats, never as
text, even if it was actually meant as a short text encoding. `"1,0,0,1"` (8 ASCII bytes) would
be parsed as text (8 is not relevant, its length just needs checking) — but a text vector whose
byte length happens to land on a multiple of 4 (e.g. `"1,0,0"` is 5 bytes — fine — but `"1,-1"` is
4 bytes exactly) would be silently reinterpreted as one packed `f32` instead of two text numbers.
This is a real, narrow edge case, not a defect in the common case (real callers use one format
consistently), but worth flagging (see §7).

#### 4.4 Vector similarity when it *is* invoked directly (`FT.ADD` + brute-force cosine)

```rust
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0; let mut norm_a = 0.0; let mut norm_b = 0.0;
    for (x, y) in a.iter().zip(b.iter()) { dot += x*y; norm_a += x*x; norm_b += y*y; }
    let denom = (norm_a * norm_b).sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}
```

Now that `query_vec`/`param_name` are genuinely resolved (§4.3), it's worth being precise about
what kind of search that vector actually drives: the scan itself is still a full `O(num_docs)`
loop computing cosine similarity against every document that has a vector in the queried field —
there is no ANN index (no HNSW, no quantization) inside `search.rs` itself. The fix in §4.3 makes
KNN *correct*, not fast — it's still brute force per query.

#### 4.5 Reciprocal Rank Fusion — real, and matches the standard formula

```rust
pub fn reciprocal_rank_fusion(bm25_hits: &[SearchHit], vector_hits: &[SearchHit], k: f64) -> Vec<SearchHit> {
    let mut rrf_scores: HashMap<String, f64> = HashMap::new();
    for (rank, hit) in bm25_hits.iter().enumerate() {
        *rrf_scores.entry(hit.doc_id.clone()).or_default() += 1.0 / (k + (rank as f64) + 1.0);
    }
    for (rank, hit) in vector_hits.iter().enumerate() {
        *rrf_scores.entry(hit.doc_id.clone()).or_default() += 1.0 / (k + (rank as f64) + 1.0);
    }
    // ...collect, sort descending by combined score
}
```

Standard RRF (`1 / (k + rank + 1)` per list, summed across lists a document appears in), with
`k` passed in by the caller rather than hardcoded — real and correct as far as the fusion math
goes. What's missing (§4.3) is a working vector-ranked list to fuse *with* via the `FT.SEARCH`
KNN path; the function itself has its own passing unit test (`test_reciprocal_rank_fusion`)
that constructs both hit lists manually rather than through a real KNN search.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: `Command::FtCreate/FtSearch/FtInfo/FtDropIndex/FtExplain/FtAdd`
  arms call `create_search_index`/`get_search_index`+`parse_query`+`execute_search`/
  `drop_search_index` directly; separately, `Hset`/`Hmset`/`JsonSet` (root path only) call
  `index_document_hook` after a successful write, and `Del`/similar paths call
  `delete_document_hook`.
- **`src/resp.rs`**: parses `FT.CREATE`'s `ON`/`PREFIX`/`SCHEMA` clauses and each field's
  `TEXT [WEIGHT w] [SORTABLE] [NOSTEM]` / `NUMERIC [SORTABLE]` / `TAG [SEPARATOR c]
  [CASESENSITIVE]` / `VECTOR ...` options into the real `IndexSchema`/`FieldType` values shown
  in §3, and parses `FT.SEARCH ... PARAMS n k v ...` into `SearchOptions.params`, keyed by the
  bare parameter name with no `$` prefix (§4.3 — `execute_search` tries both forms when looking
  a param up, so either convention on the query-string side resolves correctly).
- **`src/vector.rs`** (Component 08): **not actually used by this file** — `HnswIndex` and this
  file's brute-force per-document vector scan are two independent, unconnected
  implementations of vector similarity search in the codebase.
- **`src/json.rs`**: `JsonSet` on the root path (`$`) flattens the top-level JSON object's
  scalar fields into `HashMap<String, String>` before calling `index_document_hook` — nested
  objects/arrays are stringified via `.to_string()`, not recursively flattened into
  dotted-path fields.

---

### 6. Performance Characteristics

- **Global `RwLock` contention, not per-shard isolation** (§2.2): every indexed write and every
  `FT.SEARCH` call takes a real lock on the process-wide index (a write lock for indexing, a
  read lock for search) — under concurrent writers across many shards to prefixed keys, this is
  a real, shared contention point unlike the rest of the storage engine.
- **`And`/`Or`/`Not` evaluation re-runs `execute_search` recursively per sub-clause** with
  `limit: usize::MAX`, materializing a full intermediate `Vec<SearchHit>`/`HashMap` at every AST
  node rather than streaming or short-circuiting — fine for the small corpora this has been
  exercised against, not optimized for deep or wide boolean queries.
- **No compression**: posting lists are plain `Vec<Posting>` (doc_id `String` + `u32` term
  frequency + `Vec<u32>` positions per entry) — no delta-encoding, no compression, and the
  tracked term `positions` are never actually read by anything (no phrase-query support uses
  them).

---

### 7. Future Improvements

- **RESOLVED — `PARAMS`-supplied vectors are now wired into `QueryAst::KnnVector` (§4.3).** Fixed by adding a `param_name` field captured at parse time, resolving it against `opts.params` (bare or `$`-prefixed) at execution time via the new `parse_vector_blob`, and — the other half of the same underlying gap — teaching the auto-indexing path (`add_document`) to populate `vector_fields` from a `Vector`-typed field's stored string value using the same decoder, since `index_document_hook` never supplied a `vectors` map directly. Verified by a new passing test (`test_knn_vector_search_with_params`). Hybrid keyword+vector search via `FT.SEARCH`'s KNN syntax is now real end to end, still brute-force (§4.4), not ANN-accelerated.
- **Low — new, from the fix above: resolve `parse_vector_blob`'s format-detection ambiguity for short vectors (§4.3).** A byte string that happens to be a multiple of 4 bytes long is always decoded as packed little-endian floats, never as text — a short text-encoded vector whose byte length is coincidentally a multiple of 4 (e.g. `"1,-1"`, 4 bytes) would be silently misinterpreted as one packed float instead of two text numbers. An explicit format hint (e.g. requiring `PARAMS` values for vector fields to always be one format, documented and enforced) would remove the ambiguity; low priority since real callers use one format consistently in practice.
- **Medium — either read `FieldType::Text.weight` in `bm25_score`, or remove it from `FT.CREATE`'s accepted syntax (§2.4).** Accepting and storing a per-field weight that scoring silently ignores is worse than not accepting it at all — a user who sets `WEIGHT 5.0` on a field reasonably expects it to matter. Implementing real per-field BM25F (per-field lengths and weighted term contributions) is the "correct" fix; dropping/erroring on `WEIGHT` until then is the honest one.
- **Medium — shard or otherwise reduce contention on the global `SEARCH_INDICES` `RwLock` (§6).** Every `HSET`/`FT.SEARCH` across every shard takes this one process-wide lock, which is the same class of exception as `BlockHub` (Component 06) but on a much hotter path (every indexed write, not just blocking commands). A per-index `RwLock` (already partially true — `Arc<RwLock<InvertedIndex>>` per index — but the *registry* itself is one lock) or sharding indexes by name hash across a small pool of registries would reduce contention when many indexes are in active use concurrently.
- **Low — either use the tracked term `positions` for real phrase-query support (`"exact phrase"` matching), or stop tracking them (§6).** Currently pure dead weight: computed and stored on every `Posting`, read by nothing.
- **Low — replace `simple_stem`'s handful of suffix rules with a real Porter/Snowball stemmer (§2.3)** if search-quality on real English text becomes a priority — the current heuristic is a reasonable placeholder but will both over-stem and under-stem relative to a proper algorithm.

---
---

## Component 10: Kernel Bypass & Zero-Copy Networking (`src/xdp.rs`, `src/zerocopy.rs`)

### 1. Architectural Purpose & Scope

This component is two independent, mostly-unconnected pieces of code, neither of which does
what its name and the previous version of this document claimed:

1. **`src/xdp.rs`**: **Not real AF_XDP/eBPF kernel bypass.** There is no `aya`/`libbpf`/`xsk`
   dependency in `Cargo.toml`, no `bpf()` syscall, no raw socket, no UMEM ring buffers, and no
   attachment of any program to a NIC driver. What actually exists is a pure-userspace
   `XdpEngine`: a CIDR-based allow/drop/redirect rule table plus a per-source-IP token-bucket
   rate limiter, driven entirely by a Redis command (`XDP.PACKET <payload>`) that lets a client
   hand it a raw byte buffer to run through the simulated pipeline. It never touches real
   inbound network traffic.
2. **`src/zerocopy.rs`**: **Real Linux zero-copy syscalls, but entirely disconnected from the
   live request path.** `SO_ZEROCOPY`/`MSG_ZEROCOPY` usage here is genuine and correctly
   implemented (real `libc` FFI, real `ENOBUFS` fallback handling), and there's a real
   page-aligned `RegisteredBufferPool` with `io_uring`-crate-compatible `iovec`s. But grepping
   the entire codebase shows **zero call sites** for any of it outside this file's own unit
   tests — `server.rs`/`connection.rs`/`main.rs` never construct a `ZeroCopyEngine` or call
   `send_zc`. The actual connection path (`connection.rs`, via `monoio`'s `io_uring` driver)
   never uses this code.

---

### 2. Key Invariants & Concurrency Constraints

1. **`XdpEngine` is a single global, not per-shard**: `get_xdp_engine()` returns a clone of an
   `Arc<XdpEngine>` behind a process-wide `static GLOBAL_XDP_ENGINE: LazyLock<Arc<XdpEngine>>`
   — every shard thread that calls `XDP.*` commands shares the exact same instance, coordinated
   via `RwLock<Vec<XdpRule>>` and `RwLock<HashMap<u32, TokenBucket>>` (a real, if narrow,
   exception to the shared-nothing model, similar in shape to `BlockHub` in Component 06).
2. **`XdpMode` is cosmetic, not functional**: the global engine picks `XdpMode::Skb` if
   `/sys/class/net` exists on the host, else `XdpMode::Simulated` — this only changes what
   `XDP.INFO` reports as a string; it does not change `process_packet`'s behavior or attach
   anything to a real interface in either mode.
3. **`RegisteredBufferPool` owns raw allocated memory directly** (`alloc_zeroed`/`dealloc` via
   `std::alloc`, not a `Vec`), page-aligned via `Layout::from_size_align(total_size, PAGE_SIZE)`,
   with a manual `unsafe impl Send + Sync` — correct in isolation, but again: nothing in the
   codebase actually constructs one outside its own test.
4. **`ZeroCopyEngine::send_zc` degrades gracefully**: only applies `MSG_ZEROCOPY` for payloads
   `>= PAGE_SIZE` (4KB, per a comment citing page-locking overhead for smaller sends), and on
   `ENOBUFS` (kernel zero-copy completion queue full) or general zero-copy failure, retries once
   with a plain blocking `send()` — this fallback logic is real and correct, it's just never
   invoked by anything.

---

### 3. Component Architecture

```
                    XDP.* Redis commands (client-issued, e.g. redis-cli)
                                        │
                                        ▼
                     crate::xdp::get_xdp_engine()  (global Arc<XdpEngine>)
                                        │
                ┌───────────────────────┼───────────────────────┐
                ▼                       ▼                       ▼
        XDP.RULEADD/DEL/LIST    XDP.STATS / XDP.INFO      XDP.PACKET <bytes>
       (CIDR allow/drop table)   (atomic counters)      (manually parses the
                                                          given bytes as an
                                                          Ethernet/IPv4 frame
                                                          and runs the same
                                                          filter+rate-limit
                                                          pipeline used above)

        src/zerocopy.rs: RegisteredBufferPool + ZeroCopyEngine
        — fully implemented, fully unit-tested, ZERO callers anywhere
          else in the codebase.
```

#### `XdpEngine` (`src/xdp.rs`) — the real struct

```rust
pub struct XdpEngine {
    pub ifname: String,
    pub mode: XdpMode,
    pub frame_size: usize,          // 2048, cosmetic (reported by XDP.INFO only)
    pub num_frames: usize,          // 4096, cosmetic
    pub rules: RwLock<Vec<XdpRule>>,
    pub next_rule_id: AtomicU32,
    pub rate_limiters: RwLock<HashMap<u32, TokenBucket>>,  // keyed by source IPv4, as u32
    pub default_rate_limit: f64,     // 100_000.0 tokens/sec
    pub default_rate_capacity: f64,  // 50_000.0 burst capacity
    pub rx_packets: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub dropped_packets: AtomicU64,
    pub redirected_packets: AtomicU64,
    pub pass_packets: AtomicU64,
    pub rate_limit_drops: AtomicU64,
}
```

There is no `XdpSocket`, `XdpUmem`, `XdpRxRing`/`XdpTxRing`/`XdpFillRing`/`XdpCompRing`, and no
`xsk_fd: RawFd` anywhere in the file — those were invented in the prior version of this
document. The real per-rule type is:

```rust
pub struct XdpRule {
    pub id: u32,
    pub action: XdpAction,   // Pass | Drop | Redirect | Tx
    pub cidr: String,
    pub network: u32,        // pre-computed via parse_cidr for fast masking
    pub netmask: u32,
}
```

#### `TokenBucket` (`src/xdp.rs`) — the real rate limiter, one instance per source IP

```rust
pub struct TokenBucket {
    pub tokens: f64,
    pub capacity: f64,
    pub refill_rate: f64,   // tokens per second
    pub last_update: Instant,
}

impl TokenBucket {
    pub fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        if self.tokens >= 1.0 { self.tokens -= 1.0; true } else { false }
    }
}
```

Standard continuous-refill token bucket, costing exactly 1 token per packet regardless of
packet size (the prior doc's `TokenBucketLimiter::allow_packet(&mut self, packet_len: u64)`,
which spent tokens proportional to byte length, does not exist — the real bucket is
per-*packet*, not per-*byte*). Buckets are created lazily per source IP the first time that IP
is seen (`rate_limiters.entry(ip).or_insert_with(...)`), never evicted — a real, if minor,
unbounded-memory-growth characteristic worth knowing (one `TokenBucket` per distinct source IP
ever seen, for the lifetime of the process).

#### `src/zerocopy.rs` — the real (but unused) types

```rust
pub struct RegisteredBufferPool {
    ptr: *mut u8,
    layout: Layout,
    slot_size: usize,
    slot_count: usize,
    free_slots: Vec<usize>,
    iovecs: Vec<libc::iovec>,
    stats: Arc<ZeroCopyStats>,
}

pub struct ZeroCopyEngine {
    stats: Arc<ZeroCopyStats>,
}
```

Note these are **separate** structs — `ZeroCopyEngine` does not own a `buffer_pool` field the
way the prior doc claimed; a caller would need to wire the two together manually (and no caller
does).

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `XdpEngine::process_packet` — manual, CPU-side Ethernet/IPv4 parsing

```rust
pub fn process_packet(&self, packet: &[u8]) -> XdpAction {
    self.rx_packets.fetch_add(1, Ordering::Relaxed);
    self.rx_bytes.fetch_add(packet.len() as u64, Ordering::Relaxed);
    if packet.is_empty() {
        self.dropped_packets.fetch_add(1, Ordering::Relaxed);
        return XdpAction::Drop;
    }
    // Detects either a 14-byte Ethernet header (checking bytes[12..14] == 0x0800 for IPv4)
    // followed by an IPv4 packet, OR a bare IPv4 packet (checking the top nibble of byte 0 == 4).
    // Extracts source IP (bytes 12..16 of the IPv4 header) and, for TCP (protocol byte == 6),
    // the destination port — computed but currently unused (`let _ = payload_offset;`).
    ...
    if let Some(ip) = src_ip {
        // 1. Linear scan of CIDR rules (first match wins: Drop/Pass/Redirect/Tx)
        // 2. Per-source-IP TokenBucket rate limit (lazily created)
    }
    // 3. Anything not matched by a rule or rate-limited is unconditionally Redirect'd
    //    (the doc comment says "Redis port 6379 or cluster bus", but the code does not
    //    actually inspect the destination port to decide this — it's an unconditional default)
    self.redirected_packets.fetch_add(1, Ordering::Relaxed);
    XdpAction::Redirect
}
```

The byte-offset parsing itself is real and reasonably careful (bounds-checked slice access,
handles both frame shapes), but it operates on whatever byte slice was handed to it — there is
no code anywhere that reads this slice from an actual NIC, a raw socket, or an XDP program.

#### 4.2 The only caller: `Command::XdpPacket` in `connection.rs`

```rust
Command::XdpPacket(payload) => {
    let action = crate::xdp::get_xdp_engine().process_packet(&payload);
    let s = format!("+{}\r\n", action);
    out.extend_from_slice(s.as_bytes());
    false
}
```

`payload` is a `Bytes` argument taken directly from the client's `XDP.PACKET` command — a
Redis client can construct an arbitrary byte string and ask the server to run it through the
simulated filter/rate-limit pipeline and report back `+PASS`, `+DROP`, `+REDIRECT`, or `+TX`.
This confirms the design: it's a **testable simulation of what an XDP filter's logic would do**,
reachable as an ordinary command, not a hook into real packet ingress. The companion commands
(`XDP.INFO`, `XDP.RULEADD`, `XDP.RULEDEL`, `XDP.RULELIST`, `XDP.STATS`) manage the same global
rule table and read back the same atomic counters — all of it is only ever exercised by
whatever a client explicitly sends to `XDP.PACKET`, never by this server's own real 6379
listener traffic.

#### 4.3 `ZeroCopyEngine::send_zc` — real syscall usage, verified unreachable

```rust
pub fn send_zc(&self, fd: RawFd, data: &[u8]) -> io::Result<usize> {
    if data.is_empty() { return Ok(0); }
    let flags = if data.len() >= PAGE_SIZE {
        libc::MSG_NOSIGNAL | MSG_ZEROCOPY
    } else {
        libc::MSG_NOSIGNAL
    };
    let ret = unsafe { libc::send(fd, data.as_ptr() as *const libc::c_void, data.len(), flags) };
    if ret >= 0 {
        // record zc_send_calls / zc_bytes_sent, return Ok(sent)
    } else {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOBUFS) || flags & MSG_ZEROCOPY != 0 {
            // fall back to a plain blocking libc::send with just MSG_NOSIGNAL
        }
        Err(err)
    }
}
```

This is correct, idiomatic use of Linux's real zero-copy send path (including the documented
requirement to poll `MSG_ERRQUEUE` for completion notifications in a full implementation — which
this code does *not* do; there is no `MSG_ERRQUEUE`/`recvmsg` polling anywhere in the file, so
even if this were wired up, the caller would have no way to know when the kernel has actually
finished the zero-copy DMA and it's safe to reuse/free the source buffer). `fd: RawFd` is a raw
file descriptor — but `connection.rs`'s actual sockets are `monoio::net::TcpStream` objects
whose file descriptors aren't exposed or passed to this function anywhere. There is no
integration point today.

`build_io_uring_send_zc` similarly constructs a real `io_uring::opcode::SendZc` entry using the
`io-uring` crate (a genuine dependency in `Cargo.toml`, used elsewhere for direct-I/O tiering —
see Component 07), but nothing ever calls it or submits the resulting entry to a ring.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: the only real integration point — six `Xdp*` `Command` variants
  (`XdpInfo`, `XdpRuleAdd`, `XdpRuleDel`, `XdpRuleList`, `XdpStats`, `XdpPacket`) are dispatched
  here, each just forwarding to `crate::xdp::get_xdp_engine()`. **Not connected**: the real
  `handle_connection`/accept-loop path in `server.rs` (Component 01) always uses `monoio`'s
  `SO_REUSEPORT` `TcpListener`, regardless of `XdpEngine`'s state.
- **`src/resp.rs`**: parses the six `XDP.*` command names/arguments into the `Command` enum
  variants above (including mapping `"DROP"`/`"PASS"`/`"REDIRECT"`/`"TX"` strings to
  `XdpAction`).
- **`src/zerocopy.rs`**: no cross-component interactions to document — verified zero callers
  outside its own `#[cfg(test)]` module.

---

### 6. Performance Characteristics

- **No measured network-layer performance benefit exists from either file.** `xdp.rs`'s cost is
  whatever it costs to run `process_packet` once per `XDP.PACKET` command a client explicitly
  sends — i.e., it's exercised at whatever rate a test or admin script chooses to call it, not
  at line rate against real traffic. `zerocopy.rs` is entirely inert in the running server.
- **The token-bucket rate limiter and CIDR rule table are real, correct, O(1)-per-packet
  (rules are a linear scan, but the rule list is expected to be small) userspace logic** — useful
  as a testable filter-policy engine, just not connected to anything that would make it a DDoS
  defense in practice.
- Any performance claims in the previous version of this document (100GbE line-rate, "28 million
  packets/sec", "&lt;5% CPU utilization" for `MSG_ZEROCOPY`) were invented and have been removed;
  none of it has ever been benchmarked because none of it runs on the real request path.

---

### 7. Future Improvements

- **High-priority decision, not a fix: decide whether either file has a real future, and act accordingly.** Both are currently fully-implemented-but-disconnected code with real maintenance cost (they compile, they have tests, they need to keep compiling as the rest of the codebase changes) and zero runtime value. Concretely: (a) wire `zerocopy.rs`'s `send_zc`/`RegisteredBufferPool` into `connection.rs`'s large-reply write path (e.g. big `HGETALL`/`SMEMBERS`/`FT.SEARCH` responses above a size threshold) where zero-copy send could plausibly help, since `monoio`'s socket file descriptors would need to be exposed for this to even be possible — or (b) delete it and its tests if there's no near-term plan to integrate it. The same either/or applies to `xdp.rs`, except the honest path there is narrower: real AF_XDP kernel bypass is a large undertaking (a genuine `aya`/`xsk` dependency, root/capabilities, NIC driver support) — if that's not the actual goal, rename this module to reflect what it really is (a testable packet-filter/rate-limiter simulation) rather than implying kernel bypass.
- **Medium — if `zerocopy.rs` is kept, add the missing `MSG_ERRQUEUE` completion polling (§4.3).** `send_zc` submits `MSG_ZEROCOPY` sends but never polls `MSG_ERRQUEUE` for the kernel's completion notification, so even a wired-up caller would have no correct way to know when the source buffer is safe to reuse or free — a real correctness gap in the zero-copy contract, not just "unused code."
- **Low — bound `rate_limiters`' unbounded growth (§3's `TokenBucket` note).** A `TokenBucket` is created and kept forever for every distinct source IP `XDP.PACKET` has ever been asked to evaluate, with no eviction — low risk given it's only reachable via an explicit client command today, but worth a periodic sweep (evict buckets untouched for N minutes) if this is ever exposed more broadly.
- **Low — make `process_packet`'s default-Redirect fallback actually inspect the destination port**, matching what its own doc comment claims ("Redis port 6379 or cluster bus") rather than unconditionally redirecting everything unmatched (§4.1) — cheap to fix and removes a doc-vs-code mismatch inside the file itself.

---
---

## Component 11: Redis Cluster Topology & Gossip Protocol (`src/cluster.rs`)

### 1. Architectural Purpose & Scope

`src/cluster.rs` implements a simplified Redis Cluster control plane: per-node slot
ownership tracked as `(start, end)` ranges (not a real Redis Cluster deployment's
16,384-bit bitmask), a plain-text line-oriented gossip protocol between nodes on
`port + 10000`, unilateral (non-consensus) failure detection based on ping/pong
staleness, and a real majority-vote replica election for failover. It is a single
process-wide singleton per listening port (`get_cluster_hub(port)`), and only the
shard-0 worker thread ever starts the cluster-bus listener for that port
(`start_cluster_bus`, called from `run_shard_worker` — see Component 01).

---

### 2. Key Invariants & Concurrency Constraints

1. **One `ClusterHub` per port, shared via a global registry**: `CLUSTER_HUBS:
   LazyLock<RwLock<HashMap<u16, Arc<ClusterHub>>>>`. `get_cluster_hub(port)`
   lazily creates and caches one `Arc<ClusterHub>` per port — this is process-wide
   shared, mutex/rwlock-guarded state, not thread-local (a deliberate, narrow
   exception to the shared-nothing model, same category as `BlockHub` in
   Component 06).
2. **Plain-text wire protocol, not binary framing**: every cluster-bus message
   (`MEET`, `PING`, `FAIL`, `FAILOVER`, `FAILOVER_AUTH_REQUEST`,
   `FAILOVER_ANNOUNCE`) is a `\r\n`-terminated space-separated ASCII line, parsed
   with `split_whitespace()`. There is no binary struct, no magic-byte signature,
   no `#[repr(C, packed)]` header of any kind.
3. **Synchronous blocking I/O on dedicated OS threads, not `io_uring`/`monoio`**:
   the cluster bus listener runs on its own `std::thread`, using plain
   `std::net::TcpStream`/`TcpListener` with short (200-500ms) read/write
   timeouts — completely separate from the rest of Rudis's async, io_uring-based
   networking. Each inbound connection also gets its own `std::thread::spawn`.
4. **Unilateral failure detection, not quorum-based**: a peer is marked `"fail?"`
   after 5s of silence and `"fail"` after 10s, decided independently by each node
   from its own `pong_recv` timestamps. The `pfail_reports: HashMap<String,
   HashSet<String>>` field that `cluster_nodes()` reads to check "has anyone else
   reported this node as failing" is **never written to** anywhere in the file
   except being cleared on `CLUSTER RESET` — there is no real distributed PFAIL→FAIL
   consensus, despite the data structure existing for it.
5. **Replica election *is* a real majority vote**: `start_election` does send
   `FAILOVER_AUTH_REQUEST` to every known master and only promotes itself after
   collecting `>= (total_masters / 2) + 1` `FAILOVER_AUTH_ACK` replies, gated by
   `last_vote_epoch` (one vote per epoch per master) — this part matches the
   Architectural Purpose's claim, unlike the failure-detection consensus.

---

### 3. Component Architecture & Data Structures

```
   CLUSTER MEET ip port          Client Connection ── CLUSTER SETSLOT / GET / SET
          │                              │
          ▼                              ▼
   ClusterHub::cluster_meet    connection.rs: read router.slot_states[slot]
   (connects to peer's cport,      (Migrating/Importing/Moved/Stable)
    exchanges MEET/PONG)                 │
          │                    Stable ──► also checks ClusterHub.my_slots /
          ▼                              .nodes for a DIFFERENT node owning
   nodes: HashMap<id, ClusterNodeInfo>    this slot ── -MOVED if so
          │
          ▼
   cluster_bus_tick() every 500ms: PING every known peer,
   embed full node table as a ";"-joined gossip blob in the PING line,
   update pong_recv / flags from the PONG reply or its absence
```

#### Real data structures (`src/cluster.rs`)

```rust
#[derive(Clone, Debug)]
pub struct ClusterNodeInfo {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    pub flags: String, // "myself,master", "master", "slave", "fail?", "fail"
    pub master_id: String,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub config_epoch: u64,
    pub link_state: String, // "connected", "disconnected"
    pub slots: Vec<(u16, u16)>,
}

pub struct ClusterHub {
    pub port: u16,
    pub cport: u16,
    pub my_id: RwLock<String>,
    pub current_epoch: AtomicU64,
    pub config_epoch: AtomicU64,
    pub last_vote_epoch: AtomicU64,
    pub election_in_progress: AtomicBool,
    pub role: RwLock<String>,          // "master" or "slave"
    pub master_id: RwLock<String>,     // "-" or an id
    pub nodes: RwLock<HashMap<String, ClusterNodeInfo>>,
    pub my_slots: RwLock<Vec<(u16, u16)>>,
    pub pfail_reports: RwLock<HashMap<String, HashSet<String>>>, // dead — see §2.4
    pub bus_running: AtomicBool,
    pub cancel_bus: RwLock<Option<flume::Sender<()>>>,
    pub active_migration: RwLock<Option<ActiveMigration>>,
}
```

`slots`/`my_slots` are `Vec<(u16, u16)>` range lists, kept normalized by
`compact_slots` (sorts, then merges adjacent/overlapping ranges) — there is no
16,384-bit bitmask anywhere in this file. Node IDs are **not** the real Redis
40-hex-char SHA1-derived ID; `generate_node_id` builds a 40-hex-char string from
two `fxhash::hash64` calls over the port and the current time-in-nanoseconds
(`format!("{:016x}{:016x}{:08x}", h1, h2, port)`), which happens to be the same
length but is not cryptographically meaningful.

`ActiveMigration` backs the Dragonfly-style (`DFLYCLUSTER`) migration status
commands (`dfly_migrate_init`/`dfly_migrate_flow`/`dfly_migrate_ack`/
`dfly_slot_migration_status`) — these are simple state bookkeeping (a state
string, a counter incremented by whatever `flow_id` value is passed in) rather
than any real data-transfer protocol; no keys are actually copied between nodes
by this code.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `CLUSTER MEET`: a synchronous one-shot handshake

```rust
pub fn cluster_meet(&self, ip: &str, port: u16) -> Result<(), String> {
    // ...pre-insert a temp node entry so it's visible immediately...
    let addr = format!("{}:{}", ip, port + 10000);
    if let Ok(mut stream) = TcpStream::connect_timeout(&addr.parse()?, Duration::from_millis(300)) {
        let meet_frame = format!("MEET 127.0.0.1 {} {} {} {}\r\n", self.port, self.my_id(), my_epoch, slots_repr);
        stream.write_all(meet_frame.as_bytes())?;
        // read the "+PONG <id> <epoch> <role> <slots>" reply, replace the temp entry with the real one
    }
    Ok(())
}
```

This blocks the calling connection task's thread for up to ~300ms (connect) plus
another ~300ms (read) in the worst case — it's a direct synchronous socket call
made from inside `CLUSTER MEET`'s command handler in `connection.rs`, not
dispatched to a background task.

#### 4.2 The gossip tick — `cluster_bus_tick`, called every 500ms from the bus thread

```rust
if last_tick.elapsed() >= Duration::from_millis(500) {
    last_tick = std::time::Instant::now();
    cluster_bus_tick(&hub_clone);
}
```

For every known peer, it opens a fresh `TcpStream`, sends:

```rust
let ping_msg = format!("PING {} {} {} {} GOSSIP {}\r\n",
    hub.my_id(), my_epoch, role, slots_repr, gossip_payload);
```

where `gossip_payload` is the *entire* known node table serialized as
`id,ip,port,cport,flags,epoch;id,ip,port,cport,flags,epoch;...` — every tick
re-sends full state to every peer (no incremental/random-sample gossip despite
what the old doc claimed). A successful `+PONG id epoch role slots` reply updates
that peer's `pong_recv`/`slots`/`config_epoch`/`flags`; a failed connection or
timeout instead re-evaluates that peer's failure state from elapsed silence
(`> 5000ms` → `"fail?"`, `> 10000ms` → `"fail"`). After processing all peers, it
checks whether the *local* node is a replica whose recorded master is `"fail"`,
and if so spawns `start_election` on a fresh `std::thread` (guarded by
`election_in_progress` so only one election runs at a time).

#### 4.3 Receiving a message — `handle_cluster_bus_conn`, one thread per inbound connection

A single `match` on the first whitespace-separated token handles `MEET`, `PING`
(also parses the embedded `GOSSIP <payload>` section — this is how third-party
nodes are learned about transitively, without ever calling `CLUSTER MEET` on
them directly), `FAIL`, `FAILOVER`, `FAILOVER_AUTH_REQUEST`, and
`FAILOVER_ANNOUNCE`. Every branch replies inline on the same blocking stream
(`+PONG ...`, `+OK\r\n`, or `+FAILOVER_AUTH_ACK id epoch\r\n` /
`-ERR vote rejected\r\n` for the vote request).

#### 4.4 Replica election — the one place with a real quorum

```rust
pub fn start_election(&self) {
    let req_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;
    let masters = /* every known peer whose flags contain "master" and not "fail" */;
    let mut votes = 1; // self
    for (ip, cport) in &masters {
        // connect, send "FAILOVER_AUTH_REQUEST <id> <epoch> <master_id>", count a "+FAILOVER_AUTH_ACK" reply
    }
    let majority = (masters.len() + 1) / 2 + 1;
    if votes >= majority {
        // become master, inherit the old master's slots, broadcast FAILOVER_ANNOUNCE
    }
}
```

A voter (`FAILOVER_AUTH_REQUEST` handler, §4.3) only grants a vote if it is
itself a master, the requested epoch is newer than its `last_vote_epoch`, and it
believes the claimed old master is already `"fail"` — this is the one piece of
this file that is genuinely a distributed-consensus mechanism, not just local
bookkeeping. `cluster_failover FORCE` (manual failover via `CLUSTER FAILOVER`)
skips the vote entirely and just claims mastership + broadcasts `FAILOVER`
unconditionally.

#### 4.5 How this actually reaches `-MOVED`/`-ASK` on the command path — corrects a claim in Component 04

Component 04's doc states cluster slot-migration/redirection is "dead code" because
the standalone helper `Router::check_slot_redirection` has no call sites. That is
true for that specific function, but the underlying mechanism it would have
implemented **is** live — just inlined directly in `connection.rs` instead of
calling out to that helper:

```rust
if let Some(key) = cmd_primary_key(&cmd) {
    let slot = key_slot(key);
    let state = router.slot_states.borrow()[slot as usize].clone();
    match state {
        SlotState::Moved(target) => { /* -MOVED slot target, always */ }
        SlotState::Importing(source) => { if !is_asking { /* -MOVED slot source */ } }
        SlotState::Migrating(target) => {
            if !router.exists(key.clone()).await { /* -ASK slot target */ }
        }
        SlotState::Stable => {
            // even in the common case, check this file's ClusterHub for a
            // DIFFERENT node (not shard) owning this slot, and -MOVED to it:
            let hub = crate::cluster::get_cluster_hub(router.port);
            if !hub.my_slots.read().unwrap().iter().any(|&(s,e)| slot>=s && slot<=e) {
                if let Some(peer) = hub.nodes.read().unwrap().values()
                    .find(|n| n.flags.contains("master") && !n.flags.contains("fail")
                              && n.slots.iter().any(|&(s,e)| slot>=s && slot<=e)) {
                    out.extend_from_slice(format!("-MOVED {} {}:{}\r\n", slot, peer.ip, peer.port).as_bytes());
                    return false;
                }
            }
        }
    }
}
```

This runs on **every command that has a primary key**, for every connection —
`router.slot_states` (set via `CLUSTER SETSLOT`, see `router.rs::set_slot_state`,
which broadcasts a `ShardMessage::SetSlotState` to every local shard) and this
file's `ClusterHub.my_slots`/`.nodes` (populated by gossip/`MEET`) are both
genuinely consulted, and a real `-MOVED`/`-ASK` is genuinely written to the
client. `router.slot_owners` (a *separate*, per-shard-local ownership-override
vector — see Component 04) is a different piece of machinery not used by this
path at all; don't conflate the two.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: reads `router.slot_states` and `get_cluster_hub(port)`
  directly on every keyed command (§4.5) to decide `-MOVED`/`-ASK`; also the
  entire `CLUSTER *` subcommand family (`MEET`, `NODES`, `INFO`, `ADDSLOTS`,
  `ADDSLOTSRANGE`, `DELSLOTS`, `DELSLOTSRANGE`, `SETSLOT`, `FAILOVER`,
  `REPLICATE`, `RESET`, `SLOTS`, `SHARDS`, `LINKS`, `FORGET`) is dispatched from
  `connection.rs` through thin `Router::cluster_*` passthrough methods straight
  into this file's `ClusterHub` methods.
- **`src/router.rs`**: `Router::set_slot_state`/`slot_states` is a *different*
  slot-state mechanism than this file's `my_slots`/gossip table — see §4.5's
  correction. They coexist and are both real, but they're not the same system.
- **`src/replication.rs`**: `cluster_failover`/`start_election` both call
  `crate::replication::get_replication_hub(self.port).make_master()` when this
  node wins/executes a failover, so cluster-level role changes propagate into
  the replication subsystem.
- **`src/server.rs`** (Component 01): calls `start_cluster_bus(port)` exactly
  once, only from the `shard_id == 0` worker thread.

---

### 6. Performance Characteristics

- **Not zero-allocation, not io_uring-based**: every gossip tick and every
  `CLUSTER MEET`/`FAILOVER` opens a brand-new blocking `TcpStream` per peer
  (connect + write + read, each with its own 200-500ms timeout) on a plain OS
  thread — the opposite of the rest of Rudis's zero-copy/`monoio` design.
  Acceptable for a control-plane path that runs a few times a second, not
  something to model the data-path invariants on.
- **Full-state gossip, not incremental**: `cluster_bus_tick` re-sends the entire
  known node table to every peer on every 500ms tick — bandwidth is O(peers²)
  per tick, not the randomized-sample gossip real Redis Cluster uses. Fine at
  small cluster sizes (a handful of nodes), not validated or designed for
  hundreds of nodes despite what an earlier draft of this doc claimed about
  "100-node cluster convergence."
- **Failure detection is local and synchronous, not a distributed vote** (§2.4)
  — a node can mark a peer `"fail"` purely from its own missed-PONG timer, with
  no corroboration from other nodes, unlike the real replica-election step which
  does require a genuine majority.

---

### 7. Future Improvements

- **High — wire up real PFAIL corroboration instead of unilateral failure marking (§2.4).** `pfail_reports` already exists as a field on `ClusterHub` and is read by `cluster_nodes()`, but nothing ever writes to it — a single node's missed-PONG timer alone flips a peer to `"fail"`. A transient network blip to just one node in the cluster can currently trigger a failover that a real quorum-based PFAIL→FAIL promotion would have prevented. Implementing this is mostly plumbing that's already half-built: when a node locally marks a peer `"fail?"`, gossip that opinion to other nodes (piggybacked on the existing PING gossip payload) and only escalate to `"fail"` once enough peers agree.
- **High — unify with Component 04's `slot_owners` (see that doc's §7) rather than maintaining two independent slot-authority systems.** This file's `ClusterHub.my_slots`/`.nodes` is the one actually consulted by the real `-MOVED` redirect path (§4.5); `router.rs`'s `slot_owners` is a parallel, mostly-unread mechanism. Consolidating avoids a future bug where the two disagree.
- **Medium — replace full-state gossip with incremental/randomized-sample gossip (§6)** if cluster sizes beyond a handful of nodes become a real target — the current O(peers²)-per-tick full node-table resend every 500ms is fine at small scale but won't hold up at real Redis Cluster-scale membership counts.
- ~~**Medium — extend the slot-state check (§4.5) to the pipelined squashed-command path**~~ **Resolved** — the squash-eligibility gate now also checks whether this node still owns a `Stable` slot per this file's `ClusterHub.my_slots`/`.nodes` gossip table (not just `slot_states`'s `Migrating`/`Importing`/`Moved`), confirmed by a new E2E test (`test_cluster_pipelined_squashed_moved_redirect_e2e`) — see Component 02 §7 and Component 04 §7 for the full correction (the `Migrating`/`Importing`/`Moved` half of this check already existed before; the gossip-ownership half was the genuinely new piece).
- **Low — derive node IDs from something closer to Redis's real scheme**, or at minimum document clearly that `generate_node_id`'s two-`fxhash`-calls-over-port-and-time approach (§3) is not cryptographically meaningful — it's "good enough" for uniqueness within one test cluster but shouldn't be assumed collision-resistant across a long-running fleet the way real Redis's ID generation is designed to be.

---
---

## Component 12: CRDT Data Types & Manual Multi-Region Sync (`src/crdt.rs`)

### 1. Architectural Purpose & Scope

`src/crdt.rs` (645 lines) implements a small, self-contained library of three
**Conflict-Free Replicated Data Types (CRDTs)** — a Last-Write-Wins Register, an
Observed-Remove Set, and a Positive-Negative Counter — each ordered by a **Hybrid Logical
Clock (HLC)**, plus a binary export/import format for merging one instance's CRDT state
into another's.

**What this is not**: there is no automatic cross-region network replication. There is no
peer/region configuration anywhere in `main.rs`, no background sync task, and no wiring
into `src/replication.rs` (which handles the unrelated primary/replica `PSYNC` stream).
`CRDT.MERGE` takes its payload as a plain command argument (`Command::CrdtMerge(Bytes)`),
which means getting CRDT state from one Rudis instance to another is entirely
operator/client-driven: read it out with `CRDT.DUMP`, transport those bytes yourself
(script, sidecar, whatever), and feed them into the target instance with `CRDT.MERGE
<payload>`. The "multi-region" framing in this file's doc comments describes the
data types' *convergence properties*, not a built network protocol.

**Update — the routing gap below is now fixed.** An earlier version of this document found
that every `Command::Crdt*` handler called `router.local_db.borrow_mut().crdt_*(...)`
directly, bypassing normal key-based routing entirely, so the same key name could hold
completely independent state on different shards. As of the current source, the single-key
CRDT commands (`CrdtSet`/`CrdtGet`/`CrdtDel`/`CrdtIncrby`/`CrdtSadd`/`CrdtSmembers`/
`CrdtSrem`) are now included in both `cmd_primary_key` and `target_shard_of_cmd`
(`connection.rs`) and dispatch through the same local-vs-`execute_remote` fork every other
keyed command uses — a `CRDT.SET foo bar` now always lands on the one shard `foo` actually
hashes to, regardless of which shard's connection issued it. Separately, `CRDT.DUMP`,
`CRDT.MERGE`, and `CRDT.GC` — which operate on an entire store, not one key — now
explicitly fan out to *every* shard (`for sid in 0..router.num_shards { ... }`, via
`router.execute_remote`) and aggregate the results: `CrdtDump` concatenates every shard's
exported payload into one response, `CrdtMerge` sums the per-shard merged-item counts, and
`CrdtGc` sums the per-shard tombstones-pruned counts. In effect, `CrdtStore` is still a
genuinely separate `CrdtStore` instance per shard (the underlying data structure hasn't
changed — see §3), but the command layer now presents it as one logical whole-node store:
single-key operations are correctly routed to the one shard that owns the key, and
whole-store operations correctly touch every shard rather than just the connection's local
one.

---

### 2. Key Invariants & Concurrency Constraints

1. **Deterministic Convergence (real, and tested)**: `LwwRegister::merge`, `OrSet::merge`,
   and `PnCounter::merge` are each commutative/idempotent by construction (see §4) — the
   file's own `#[cfg(test)]` module (`test_lww_register_convergence`,
   `test_pn_counter_convergence`, `test_orset_add_wins`) exercises exactly this.
2. **HLC via lock-free CAS, not a mutex**: `HybridLogicalClock` stores
   `latest_physical_ms: AtomicU64` / `latest_logical: AtomicU32` and advances them with a
   compare-exchange retry loop (§4.1) — real lock-free code, not a fabrication.
3. **Add-Wins semantics for `OrSet`**: a concurrent add and remove of the same element
   resolve in favor of the add, because `remove` only tombstones the specific add-tags
   (`HlcTimestamp`s) it has *observed so far* — a later add carries a fresh tag the remove
   never saw, so it survives merge. Verified by `test_orset_add_wins`.
4. **No consensus, because there's no network layer to reach consensus over**: with sync
   entirely manual (§1), there's no Paxos/Raft and also no automatic conflict detection —
   whoever runs `CRDT.MERGE` decides when and with what payload merging happens.

---

### 3. Component Architecture & Data Structures

```
   Client, any shard's connection             Client, any shard's connection
        CRDT.SET foo bar                            CRDT.SET foo baz
                  │                                            │
        target_shard_of_cmd("foo")                  target_shard_of_cmd("foo")
                  │                                            │
                  └──────────── both resolve to the SAME shard ────────────┘
                                (foo's owning shard, via CRC16 —
                                 local execute or router.execute_remote,
                                 exactly like any other keyed command)

   CRDT.DUMP / CRDT.MERGE <payload> / CRDT.GC  (whole-store commands)
                  │
                  ▼
   local shard's CrdtStore  +  execute_remote(sid, same cmd) for every OTHER shard
                  │
                  ▼
   aggregated result (concatenated dump / summed merge count / summed GC count)
```

#### Real data structures (verbatim from `src/crdt.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HlcTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
    pub node_id: u16,
}
// Ord: physical_ms, then logical, then node_id (lexicographic tuple compare)

pub struct HybridLogicalClock {
    pub node_id: u16,
    latest_physical_ms: AtomicU64,
    latest_logical: AtomicU32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LwwRegister {
    pub value: Bytes,
    pub timestamp: HlcTimestamp,
    pub tombstone: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrSet {
    pub elements: HashMap<Bytes, HashSet<HlcTimestamp>>,
    pub tombstones: HashSet<HlcTimestamp>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PnCounter {
    pub p: HashMap<u16, i64>,  // per-node positive contributions
    pub n: HashMap<u16, i64>,  // per-node negative contributions
}

pub struct CrdtStore {
    pub clock: HybridLogicalClock,
    pub registers: HashMap<Bytes, LwwRegister>,
    pub sets: HashMap<Bytes, OrSet>,
    pub counters: HashMap<Bytes, PnCounter>,
}
```

Note the real shapes differ from what an earlier, unverified draft of this document
claimed: `LwwRegister`/`OrSet` are concretely `Bytes`-keyed (not generic `<T>`), `OrSet`
tracks per-element `HashSet<HlcTimestamp>` tags directly (no separate UUID type), and
`PnCounter`'s fields are named `p`/`n` (not `increments`/`decrements`) and store signed
`i64` per-node deltas rather than only-positive `u64` add/remove counts.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 HLC generation and remote-update (real CAS loops)

```rust
pub fn now(&self) -> HlcTimestamp {
    let phys_now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    loop {
        let cur_phys = self.latest_physical_ms.load(AtomicOrdering::Acquire);
        let cur_log = self.latest_logical.load(AtomicOrdering::Acquire);
        let (next_phys, next_log) = if phys_now > cur_phys { (phys_now, 0) } else { (cur_phys, cur_log + 1) };
        if self.latest_physical_ms.compare_exchange(cur_phys, next_phys, AtomicOrdering::Release, AtomicOrdering::Relaxed).is_ok() {
            self.latest_logical.store(next_log, AtomicOrdering::Release);
            return HlcTimestamp::new(next_phys, next_log, self.node_id);
        }
    }
}
```

`update(&self, remote: &HlcTimestamp)` is the same CAS-retry shape, but seeds `max_phys`
from `phys_now.max(cur_phys).max(remote.physical_ms)` — this is the standard HLC rule
(physical clock never goes backward; logical counter only increments when physical time
doesn't advance) and is called from `merge_sync_payload` (§4.3) every time a remote
timestamp is observed, so the local clock is always causally ahead of anything it has
merged in.

#### 4.2 Merge logic for each type (all real, all as originally documented)

```rust
// LwwRegister: later HLC timestamp wins outright
pub fn merge(&mut self, other: &LwwRegister) -> bool {
    if other.timestamp > self.timestamp {
        self.value = other.value.clone();
        self.timestamp = other.timestamp;
        self.tombstone = other.tombstone;
        true
    } else { false }
}

// PnCounter: per-node component-wise max (each node's own counter only grows)
pub fn merge(&mut self, other: &PnCounter) {
    for (&node_id, &val) in &other.p { let e = self.p.entry(node_id).or_default(); *e = (*e).max(val); }
    for (&node_id, &val) in &other.n { let e = self.n.entry(node_id).or_default(); *e = (*e).max(val); }
}
pub fn value(&self) -> i64 { self.p.values().sum::<i64>() - self.n.values().sum::<i64>() }

// OrSet: union tags, union tombstones, then drop any tag that's now tombstoned
pub fn merge(&mut self, other: &OrSet) {
    for ts in &other.tombstones { self.tombstones.insert(*ts); }
    for (elem, other_tags) in &other.elements {
        let my_tags = self.elements.entry(elem.clone()).or_default();
        for tag in other_tags { my_tags.insert(*tag); }
    }
    self.elements.retain(|_, tags| { tags.retain(|tag| !self.tombstones.contains(tag)); !tags.is_empty() });
}
```

#### 4.3 The manual sync format: `export_sync_payload` / `merge_sync_payload`

`CrdtStore::export_sync_payload` serializes the *entire* local store into one flat
`Vec<u8>` using a hand-rolled binary format (one byte tag per item — `1`=register,
`2`=counter, `3`=set — followed by little-endian length-prefixed fields, no compression,
no framing beyond simple concatenation):

```rust
pub fn export_sync_payload(&self) -> Vec<u8> {
    let mut buf = Vec::new();
    for (k, r) in &self.registers {
        buf.push(1u8);
        buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
        buf.extend_from_slice(k);
        buf.extend_from_slice(&(r.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&r.value);
        buf.extend_from_slice(&r.timestamp.physical_ms.to_le_bytes());
        buf.extend_from_slice(&r.timestamp.logical.to_le_bytes());
        buf.extend_from_slice(&r.timestamp.node_id.to_le_bytes());
        buf.push(if r.tombstone { 1 } else { 0 });
    }
    // ...counters (type 2), then sets (type 3), same length-prefixed shape
    buf
}
```

`merge_sync_payload(&mut self, data: &[u8])` walks that same format byte-by-byte,
reconstructs each `LwwRegister`/`PnCounter`/`OrSet`, calls `self.clock.update(&ts)` for
every timestamp it decodes (so the local clock catches up to whatever it just merged),
and merges each reconstructed value into the matching local map via the real `merge()`
methods from §4.2 — falling through to `Err(format!("Unknown CRDT item type: {}",
item_type))` for anything but `1`/`2`/`3`. This whole export→transport→merge cycle is
what a caller (script, sidecar, whatever "region sync" job exists outside this repo)
would run periodically; nothing inside Rudis itself schedules or triggers it.

#### 4.4 Tombstone GC

```rust
pub fn gc_tombstones(&mut self, ttl_ms: u64) -> (usize, usize) {
    let cutoff = now_ms.saturating_sub(ttl_ms);
    self.registers.retain(|_, reg| !reg.tombstone || reg.timestamp.physical_ms >= cutoff);
    // + OrSet::prune_tombstones(cutoff) per set
}
```
Exposed as `CRDT.GC [ttl_ms]` (default handled at the command layer, not shown here).
Only prunes registers/set-tombstones by age; there's no automatic scheduled GC task
anywhere in `server.rs` — it's an on-demand command.

---

### 5. Cross-Component Interactions

- **`src/shard.rs`**: `ShardDb.crdt_store: CrdtStore` plus thin `#[inline]` wrappers
  (`crdt_set`, `crdt_get`, `crdt_del`, `crdt_incrby`, `crdt_sadd`, `crdt_smembers`,
  `crdt_srem`, `crdt_dump`, `crdt_merge`, `crdt_gc`) that just forward to the store.
- **`src/resp.rs`**: parses `CRDT.SET|GET|DEL|INCRBY|SADD|SMEMBERS|SREM|DUMP|MERGE|GC`
  into the matching `Command::Crdt*` variants (`CrdtMerge(Bytes)` carries the raw sync
  payload as a normal bulk-string argument).
- **`src/connection.rs`**: single-key `Command::Crdt*` arms now route via
  `target_shard_of_cmd`/`execute_remote` like any other keyed command (see §1's update);
  the whole-store commands (`CrdtDump`/`CrdtMerge`/`CrdtGc`) fan out to every shard and
  aggregate. `CrdtSet`/`CrdtDel`/`CrdtIncrby`/`CrdtSadd`/`CrdtSrem` also call
  `notify_key_invalidation` (RESP3 client-side-caching) the same way ordinary mutating
  commands do.
- **`src/replication.rs`**: no interaction. Primary/replica `PSYNC` streaming is a
  separate mechanism and does not carry CRDT state.
- **`src/table.rs`**: no interaction. CRDT values are **not** stored as `RudisValue`
  variants — they live entirely in `CrdtStore`'s own maps, a parallel store next to
  `RudisTable`, not inside it.

---

### 6. Performance Characteristics

- **Lock-free clock advancement**: `HybridLogicalClock::now`/`update` use CAS retry loops,
  not a mutex — cheap even under contention from multiple connections on the same shard.
- **Export is O(total CRDT state size) and single-threaded**: `export_sync_payload` builds
  one `Vec<u8>` for the *entire* store in one call; there's no incremental/delta export —
  every `CRDT.DUMP` re-serializes everything currently held.
- **No network cost inside Rudis**: since sync is manual (§1), there's no WAN traffic,
  retry logic, or delta-batching to account for here at all — that cost (if any) lives
  entirely in whatever external process actually transports the dump/merge payloads.

---

### 7. Future Improvements

- **RESOLVED — route CRDT commands through the normal key-slot mechanism (§1's update).** Fixed: single-key `Command::Crdt*` variants now go through `target_shard_of_cmd`/`execute_remote`, and `CrdtDump`/`CrdtMerge`/`CrdtGc` now fan out to every shard and aggregate, so `CrdtStore` is presented as one logical whole-node store instead of silently-independent per-shard state.
- **High — build real automatic multi-region *network* sync.** Still open: "multi-region CRDT" is now a correct whole-node toolkit (per the fix above), but sync between separate Rudis *instances* is still entirely manual (`CRDT.DUMP` → transport the bytes yourself → `CRDT.MERGE`). The data types genuinely support real automatic sync (their merge functions are commutative/idempotent, exactly what's needed); a minimal real version would be a background task that periodically pushes each node's `export_sync_payload()`-equivalent (now whole-node, thanks to the fan-out fix) to a configured list of peer *nodes'* addresses and merges whatever it receives back — turning this from a manual toolkit into an actual active-active feature matching its name.
- **Medium — schedule `CRDT.GC` automatically (§4.4)** rather than leaving tombstone cleanup entirely on-demand — a long-running instance with many deletes/removes will accumulate tombstones indefinitely otherwise, growing `export_sync_payload`'s output and memory footprint for no ongoing benefit once tombstones are older than any plausible in-flight merge.
- **Low — add incremental/delta export** so `CRDT.DUMP` doesn't have to re-serialize the entire store on every call (§6) — matters once the store holds enough registers/sets/counters that a full dump becomes a non-trivial cost per sync cycle.

---
---

## Component 13: Lua Scripting & Redis 7 Functions Engine (`src/scripting.rs`)

### 1. Architectural Purpose & Scope

`src/scripting.rs` embeds Lua via `mlua` (`lua54`, vendored) to run `EVAL`/`EVALSHA`/`SCRIPT
LOAD`/`SCRIPT EXISTS`/`SCRIPT FLUSH` and Redis 7 Functions (`FUNCTION LOAD`, `FCALL`,
`FUNCTION LIST`, `FUNCTION DELETE`, `FUNCTION FLUSH`). There is no persistent `ScriptEngine`
struct — every `EVAL`/`EVALSHA`/`FCALL` call creates a **brand-new `mlua::Lua` instance**,
runs once, and drops it. Script *source* is cached (by SHA1, and by function-library name);
compiled bytecode and the Lua VM itself are not.

---

### 2. Key Invariants & Concurrency Constraints

1. **Fresh interpreter per call, no persistent VM or bytecode cache.** `eval_script` and
   `call_function` both call `Lua::new()` at the top and let it drop at the end of the
   function. There is nothing analogous to the old doc's `ScriptEngine`/`script_cache:
   HashMap<String, Vec<u8>>` (compiled bytecode) — what's cached is the **script's source
   text**, keyed by its SHA1 hex digest, in a global `RwLock<HashMap<String, String>>`.
2. **No sandboxing.** Grepping the file for `io`/`os`/`debug`/`package` global stripping
   finds nothing — `mlua::Lua::new()` is used unmodified, so a script has full access to
   whatever the default Lua 5.4 standard library exposes (the old doc's "Security
   Sandboxing" invariant does not exist in the real code).
3. **Deterministic execution w.r.t. the shard, by construction, not by an explicit lock.**
   Because `redis.call`/`redis.pcall` synchronously call `crate::connection::
   execute_local_command` against the same `Rc<RefCell<ShardDb>>` the calling connection
   task already holds, and Rudis's per-core execution model means nothing else touches
   that `RefCell` concurrently, a script's commands execute atomically relative to other
   traffic on the shard for free — not because scripting.rs added any synchronization of
   its own.
4. **Global, cross-shard-visible caches.** Both `SCRIPT_CACHE` (source by SHA1) and
   `FUNCTION_LIBS` (function libraries) are `static LazyLock<RwLock<HashMap<...>>>` —
   process-wide, not per-shard. A script loaded via `SCRIPT LOAD`/`FUNCTION LOAD` on one
   shard's connection is immediately visible to `EVALSHA`/`FCALL` calls arriving on any
   other shard, because it's the same global map — this is a deliberate (if easy-to-miss)
   departure from the rest of the codebase's thread-local, shared-nothing design.

---

### 3. Component Architecture & Data Structures

```
  EVAL "return redis.call('get', KEYS[1])" 1 mykey
                         │
                         ▼
        connection.rs: load_script(&script) → SHA1 into SCRIPT_CACHE
                         │
                         ▼
              eval_script(script_str, keys, args, db, aof)
                         │
                         ▼
                 Lua::new()  (brand-new VM, every call)
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   KEYS table        ARGV table      redis.{call,pcall,
   (1-indexed)        (1-indexed)     status_reply,error_reply,
                                       sha1hex} bound as closures
        │                │                │
        └────────────────┴────────────────┘
                         ▼
              lua.load(script_content).eval()
                         │
              redis.call inside script synchronously invokes
              execute_local_command(&cmd, &mut db.borrow_mut(), &mut out, aof_ref)
                         │
                         ▼
              resp_bytes_to_lua(): raw RESP reply bytes → mlua::Value
                         │
                         ▼
              lua_val_to_resp(): script's final Lua return value → RESP bytes
```

#### The real caches (there is no `ScriptEngine`/`FunctionDef` struct)

```rust
static SCRIPT_CACHE: LazyLock<RwLock<HashMap<String, String>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

static FUNCTION_LIBS: LazyLock<RwLock<HashMap<String, FunctionLib>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// A registered Redis 7 Function Library
#[derive(Clone, Debug)]
pub struct FunctionLib {
    pub name: String,
    pub engine: String,
    pub raw_code: String,
    pub functions: Vec<String>,
}
```

`SCRIPT_CACHE` maps SHA1 hex → raw script **source** (not bytecode — `eval_script` re-parses
the source with `lua.load(script_content)` on every single call, since the VM itself is
recreated each time). `FunctionLib` has no `read_only`/`description` fields the old doc
claimed; it just tracks which top-level function names a library registered.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `EVAL`/`EVALSHA`: `connection.rs` owns caching, `scripting.rs` owns execution

```rust
Command::Eval { script, keys, args } => {
    let script_str = String::from_utf8_lossy(&script);
    crate::scripting::load_script(&script);              // cache source by its own SHA1
    match crate::scripting::eval_script(&script_str, &keys, &args, &router.local_db, router.aof.as_deref()) {
        Ok(resp) => out.extend_from_slice(&resp),
        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
    }
}
Command::Evalsha { sha, keys, args } => {
    let sha_str = String::from_utf8_lossy(&sha);
    if let Some(script) = crate::scripting::get_script(&sha_str) {
        match crate::scripting::eval_script(&script, &keys, &args, &router.local_db, router.aof.as_deref()) { ... }
    } else {
        out.extend_from_slice(b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
    }
}
```

Every `EVAL` (not just `SCRIPT LOAD`) unconditionally caches its own source under its SHA1
via `load_script`, so a script becomes `EVALSHA`-able the moment it's first `EVAL`'d — matching
real Redis semantics — without a separate explicit-load step being required. `EVAL`'s AOF
propagation is real: `router.aof.as_deref()` is threaded through so writes a script performs
via `redis.call` get appended, same as an ordinary command.

#### 4.2 `redis.call`/`redis.pcall`: a Lua closure that round-trips through the real RESP path

```rust
let call_fn = lua.create_function(move |lua, margs: MultiValue| {
    let mut cmd_args = Vec::with_capacity(margs.len());
    for v in margs {
        match v {
            Value::String(s) => cmd_args.push(Bytes::copy_from_slice(&s.as_bytes())),
            Value::Integer(i) => cmd_args.push(Bytes::from(i.to_string())),
            Value::Number(n) => cmd_args.push(Bytes::from(n.to_string())),
            Value::Boolean(b) => cmd_args.push(Bytes::from(if b { "1" } else { "0" })),
            Value::Nil => cmd_args.push(Bytes::new()),
            _ => {}
        }
    }
    let cmd = match crate::resp::build_command(cmd_args) {
        Ok(Some(c)) => c,
        Ok(None) => return Err(mlua::Error::RuntimeError("ERR empty command".to_string())),
        Err(e) => return Err(mlua::Error::RuntimeError(format!("ERR {}", e))),
    };
    let mut out = Vec::new();
    let aof_ref = unsafe { aof_call.map(|ptr| &*ptr) };
    crate::connection::execute_local_command(&cmd, &mut db_call.borrow_mut(), &mut out, aof_ref);
    resp_bytes_to_lua(lua, &out)
})?;
```

`redis.call`'s arguments are converted to `Bytes` and handed to the **same** `build_command`
that parses commands off the wire (Component 03), then executed through the **same**
`execute_local_command` that the normal squashed/local dispatch path uses (Component 02) —
there is no separate "scripting command table"; a script literally issues real `Command`
values against the real `ShardDb`. `redis.pcall` is identical except it catches a RESP error
reply (`out.starts_with(b"-")`) and turns it into a Lua table `{err = "..."}` instead of
raising a Lua error, matching Redis's `call` (throws) vs. `pcall` (returns an error table)
distinction.

The `aof_call: Option<*const RefCell<AofWriter>>` capture is a raw pointer cast specifically
so the closure can be `'static` (required by `mlua::Lua::create_function`) while still
referencing a `RefCell` borrowed from the caller's stack for the duration of one `eval_script`
call — `unsafe { aof_call.map(|ptr| &*ptr) }` reconstitutes the reference at call time. This
is sound only because the `Lua` instance (and thus every closure it holds) is dropped before
`eval_script` returns and the borrow ends; nothing about `mlua`'s API enforces that guarantee,
so it's a manual invariant, not a compiler-checked one.

#### 4.3 RESP ⇄ Lua value conversion (`resp_bytes_to_lua`, `lua_val_to_resp`)

`resp_bytes_to_lua` turns a raw RESP reply (as bytes) into an `mlua::Value` per the real Redis
Lua conversion rules: `+OK` → `{ok = "OK"}` table, `-ERR ...` → a Lua error (not a value —
this makes `redis.call` on a failing command raise instead of return, which `redis.pcall`
intercepts one level up), `:N` → integer, `$-1` (null bulk) → `false`, a bulk string → a Lua
string, and `*N` arrays are recursively parsed by `parse_resp_array_to_lua` (which itself
handles nested `*`, `$`, `:`, `+` elements). `lua_val_to_resp` is the reverse mapping used for
the script's *final* return value: `nil`→`$-1`, `true`→`:1`, `false`→`$-1` (Redis's own
`false`-means-nil-reply convention), a table with an `"ok"` key → `+...`, a table with an
`"err"` key → `-...`, otherwise a table is treated as a 1-indexed array and serialized as a
RESP array via `t.raw_len()`. Both directions call the real `crate::connection::
write_resp_integer`/`write_resp_bulk` helper functions (these do exist in `connection.rs`,
just not in `resp.rs` — see Component 03).

#### 4.4 `FUNCTION LOAD`/`FCALL`: a two-pass execution, no persistent function objects

```rust
pub fn load_function(code: &str, replace: bool) -> Result<String, String> {
    // parse "#!lua name=<lib>" shebang for the library name
    ...
    let lua = Lua::new();                       // throwaway VM #1, just to discover names
    let reg_fn = lua.create_function(move |_, (name, _): (String, Value)| {
        func_names_clone.borrow_mut().push(name);
        Ok(())
    })?;
    redis_tbl.set("register_function", reg_fn)?;
    lua.load(&lua_code).exec()?;                 // runs the WHOLE library top-level once
    // ... store FunctionLib { name, engine: "LUA", raw_code, functions: registered }
}
```

`FUNCTION LOAD` runs the library's top-level code **once**, with `redis.register_function`
stubbed out to just record function *names* — it discards whatever Lua closures were
registered. `FCALL` then does the real work by running the library's full source **again**,
in a second fresh `Lua::new()`, this time with a real `register_function` that captures the
one closure matching the requested name into the Lua registry (`create_registry_value`) and
calls it directly with `(keys_tbl, argv_tbl)`. So a library's top-level code executes twice
total per `FCALL` call across its lifetime relative to any single load: once at `FUNCTION
LOAD` (to discover names) and once per `FCALL` (to actually get a callable closure) — there is
no cached, ready-to-call function object between calls. **Update: the AOF bug below is now
fixed.** `FCALL`'s `aof` argument was previously hardcoded to `None` in `connection.rs`,
meaning writes performed via `FCALL` were silently dropped from the AOF. As of the current
source, `connection.rs`'s `Command::Fcall` arm passes `router.aof.as_deref()` — the same way
`EVAL`/`EVALSHA` already did — so `FCALL` writes are now correctly persisted:

```rust
Command::Fcall { function, keys, args } => {
    match crate::scripting::call_function(&function, &keys, &args, &router.local_db, router.aof.as_deref()) {
        ...
    }
}
```

This is verified by a new unit test in `scripting.rs` itself (`test_fcall_with_aof_writer`),
which calls `call_function` with a real `AofWriter` and asserts the resulting AOF buffer
contains the expected `SET` command.

#### 4.5 What the old doc got right

`redis.sha1hex` (backed by the same `sha1_hex` helper used for script caching) and
`redis.error_reply`/`redis.status_reply` (building `{err=...}`/`{ok=...}` tables) are real and
match the old doc's description, modulo the exact function names.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): owns all `SCRIPT_CACHE`/`FUNCTION_LIBS` mutation
  entry points (`Command::Eval`, `Evalsha`, `ScriptLoad`, `ScriptExists`, `ScriptFlush`,
  `FunctionLoad`, `Fcall`, `FunctionList`/`Delete`/`Flush` — not individually enumerated
  above); also supplies `write_resp_integer`/`write_resp_bulk` used by `lua_val_to_resp`, and
  `execute_local_command`, the single function both `redis.call` and every normal command
  dispatch path funnel through.
- **`src/resp.rs`** (Component 03): `build_command` is called from inside the `redis.call`/
  `redis.pcall` closures to turn Lua-supplied arguments back into a real `Command`.
- **`src/shard.rs`/`src/table.rs`** (Components 04/05): mutated whenever a script's
  `redis.call` executes a write command, exactly as an ordinary client-issued command would.
- **`src/aof.rs`**: `EVAL`/`EVALSHA` and, as of the current source, `FCALL` writes are all
  appended via the `aof` parameter threaded into `eval_script`/`call_function` (§4.4).

---

### 6. Performance Characteristics

- **No bytecode caching, despite the SHA1 cache's name.** `SCRIPT_CACHE` only saves
  re-transmission of the script *text* for `EVALSHA`; Lua source is re-parsed by `mlua` on
  every single `EVAL`/`EVALSHA` call, and a fresh `Lua::new()` VM is constructed and torn down
  per call — there is no persistent interpreter or precompiled-chunk reuse the old doc's
  "Bytecode Caching" section claimed.
- **In-process, zero-IPC command execution**: real and accurate from the old doc — `redis.call`
  invokes `execute_local_command` directly against the shard's own `Rc<RefCell<ShardDb>>`,
  with no network or channel hop, since scripts only ever run against the local shard.
- **Process-wide global locks on every script/function load or lookup**: `SCRIPT_CACHE`/
  `FUNCTION_LIBS` are `RwLock`s taken on every `EVAL` (write lock, unconditionally, via
  `load_script`), `EVALSHA` (read lock), and `FCALL` (read lock) — a real (if narrow and
  presumably low-contention) departure from the rest of the codebase's lock-free, thread-local
  design, shared with the `BlockHub` exception documented in Component 06/01.

---

### 7. Future Improvements

- **RESOLVED — `FCALL`'s hardcoded `None` AOF argument (§4.4).** Fixed: `connection.rs`'s `Fcall` arm now passes `router.aof.as_deref()`, the same as `EVAL`/`EVALSHA`, and a new unit test (`test_fcall_with_aof_writer`) verifies `FCALL` writes land in the AOF buffer. `FCALL` writes are also now covered by an E2E integration test per the commit history (`test(scripting): add unit and E2E integration tests for FCALL mutating AOF persistence`).
- **Medium — cache compiled function objects instead of re-running a library's top-level code per `FCALL` (§4.4).** The current two-pass design (run once at `FUNCTION LOAD` just to discover names, run the *entire library* again from scratch on every `FCALL` to get a callable closure) means library init cost is paid on every single call, not just at load time. Since a fresh `Lua::new()` per call already means there's no persistent VM to hold a closure across calls, the more impactful fix is likely pairing this with the next item (a small VM pool) rather than trying to persist closures across genuinely separate VM instances.
- **Medium — consider a small pool of reusable `Lua` VMs (or persistent per-shard VMs) instead of `Lua::new()` per call (§6).** Fresh-VM-per-call is simple and safe (no state leaks between scripts) but means every `EVAL`/`EVALSHA`/`FCALL` pays VM construction plus re-parsing the script source from scratch. A per-shard VM reused across calls (clearing globals between invocations, or using `mlua`'s sandboxing/scope features to isolate one call from the next) would remove both costs for script-heavy workloads, at the cost of more careful state-isolation reasoning than the current always-fresh approach needs.
- **Medium — decide on and document a sandboxing posture (§2.2).** Today a script has full access to Lua's standard library (`io`, `os`, etc.) via `Lua::new()`'s defaults — fine if scripting is treated as an admin/trusted-operator-only feature, a real problem if any less-trusted caller can reach `EVAL`. Either explicitly strip dangerous globals (mirroring real Redis's Lua sandbox) or document clearly that `EVAL`/`FCALL` require the same trust level as shell access.
- **Low — hash script source with a faster non-cryptographic hash for `SCRIPT LOAD`'s cache key** if SHA1 computation ever shows up as measurable overhead on the load path — currently fine since it's a one-time cost per unique script, not per `EVALSHA` call.

---
---

## Component 14: Persistence & Replication Engines (`src/replication.rs`, `src/aof.rs`)

### 1. Architectural Purpose & Scope

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
resynchronization is now real on both the **master** and **replica** sides
(§4.3) — `run_replica_worker` tracks its `master_replid` and `master_repl_offset`,
reconnects automatically with `PSYNC <replid> <offset>`, and applies `+CONTINUE` diffs
without full RDB snapshots.

---

### 2. Key Invariants & Concurrency Constraints

1. **AOF is append-only, forever.** `AofWriter::append` only ever grows `self.buffer`
   (later flushed to disk via `write_all_at` at the current end-of-file `offset`). There is
   no `BGREWRITEAOF`, no periodic compaction, and no mechanism that ever shrinks or rewrites
   the file — it grows for as long as the process runs with AOF enabled.
2. **Partial resync is supported on both master and replica sides.**
   `run_master_replica_stream` (`src/connection.rs`) inspects the `PSYNC` command's
   replid/offset arguments via `ReplicationHub::try_partial_resync`, and replies `+CONTINUE <replid>\r\n<backlog-diff-bytes>`
   when the requested offset falls inside the retained backlog window for a matching
   replid (or `replid2`), falling back to `+FULLRESYNC <replid> <offset>\r\n$<len>\r\n<rdb-bytes>` otherwise
   (§4.3). `run_replica_worker` tracks its `master_repl_offset` and `master_replid`, sending
   `PSYNC <cached_replid> <cached_offset>` on reconnects, receiving `+CONTINUE`, and executing
   the backlog diff commands without requesting a full RDB snapshot.
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

### 3. Component Architecture & Data Structures

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

#### `AofWriter` and `AofConfig` (`src/aof.rs`)

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

#### `ReplicationHub`, `ReplicationRole`, `ReplicationBacklog`, `ConnectedReplica` (`src/replication.rs`)

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

### 4. Execution Algorithms & Code Logic

#### 4.1 There is no AOF rewrite — `command_to_resp` is the entire AOF write path

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

#### 4.2 The 50ms/~1s flush-and-fsync cadence (driven from `src/server.rs`, not this file)

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

#### 4.3 Partial resync is real on the master side; the replica side never requests one

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

#### 4.4 `INFO replication` / `ROLE` are read directly off the atomics/`RwLock`, not cached

`format_info_replication` and `format_role_resp` both take a fresh read-lock on `role` and
load the atomics (`master_repl_offset`, per-replica `ack_offset`/`listening_port`) on every
call — there's no periodic snapshot; the command handler just formats whatever the live
state currently is at request time.

---

### 5. Cross-Component Interactions

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

### 6. Performance Characteristics

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

### 7. Future Improvements

- **High — implement AOF rewrite/compaction (§4.1).** An AOF-enabled node that runs for a long time under sustained writes has an ever-growing file and an ever-growing restart replay cost, with no relief mechanism (no `BGREWRITEAOF` equivalent exists at all). Since `RudisTable` already has a working RDB chunk format (Component 05) used for full resync, the natural implementation is: periodically (or on an explicit `BGREWRITEAOF`-equivalent command) snapshot the current dataset to a fresh AOF-equivalent-from-RDB, atomically swap it in for the old growing file, and discard the old one — reusing existing RDB serialization rather than building new compaction logic from scratch.
- **RESOLVED — partial resynchronization on master and replica (§2.3/§4.3).** The master genuinely serves `+CONTINUE` with just the missing backlog bytes when a valid replid+offset is presented, and `run_replica_worker` now tracks its `master_replid` and `master_repl_offset`, automatically reconnects via `'reconnect_loop`, sends `PSYNC <replid> <offset>`, and applies `+CONTINUE` diff streams directly without requesting full RDB re-transfer. Tested via `test_replica_partial_resync_and_psync2_failover` and `test_replica_partial_resync_reconnect_e2e`.
- **Low — derive `replid` from something closer to Redis's real generation scheme**, or at least document that the current `fxhash`-over-port-and-timestamp approach (§3) is a real, working, but not cryptographically-derived identifier — same category of note as Component 11's node-ID generation.
- **Low — make the 1MB `ReplicationBacklog` size and the 50ms/~1s AOF flush/fsync cadence configurable** rather than hardcoded, once partial resync (above) makes the backlog size an operationally meaningful tuning knob rather than just an `INFO`-reporting detail.

---
---

## Component 15: Security, Memory Allocator & TLS (`src/acl.rs`, `src/allocator.rs`, `src/tls.rs`)

### 1. Architectural Purpose & Scope

> **Update note**: since this doc was last verified, a real batch of fixes landed — salted
> password hashing, real per-command/per-key ACL enforcement, and a genuinely-wired `--tls-port`
> listener. This revision re-verifies all three against the current source. Two of the three are
> improvements as advertised; the TLS wiring introduced a new, more severe problem than the
> "dead code" state it replaced — see §2.4/§4.4's ⚠️ for a real plaintext-over-the-wire bug in the
> kTLS path. Read that section before treating `--tls-port` as safe to enable.

Three unrelated system-services modules bundled under one doc:
1. **Access control (`src/acl.rs`)**: a per-port, multi-user authentication *and, as of this update, real authorization* store (`AUTH user pass`, `ACL SETUSER/GETUSER/LIST/USERS/DELUSER/WHOAMI`) modeled loosely on Redis ACL syntax. Per-command and per-key checks are now genuinely enforced — see §2.2 — though password storage still has real weaknesses, see §2.3.
2. **Jemalloc telemetry (`src/allocator.rs`)**: read-only statistics via `tikv-jemalloc-ctl`, surfaced through `INFO`'s memory section. No profiling/heap-dump capability. Unchanged by this update.
3. **TLS certificate/handshake plumbing (`src/tls.rs`)**: a real `rustls` handshake wrapper with genuine in-memory self-signed cert generation (`rcgen`), now genuinely wired to a `--tls-port` listener (§4.4) — but the `kTLS` fast-path it also wires up has a real bug that causes it to silently transmit **unencrypted** application data once activated (§2.4). This is worse than the previous "dead code" state, not better, for anyone who enables `--tls-port` on Linux with the kernel `tls` module available.

---

### 2. Key Invariants & Concurrency Constraints

1. **One `AclManager` per listening port, not global**: `PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>>`, looked up via `get_acl_for_port(port)`. Every shard thread serving the same port shares the same `Arc<RwLock<AclManager>>` — this is, like `BlockHub` (Component 06), a deliberate exception to the shared-nothing/zero-lock architecture, needed because auth state has to be consistent across every shard's independently-accepted connections on that port.
2. **Authentication is enforced, and authorization is now real too (updated).** `execute_command` gates on `authenticated: bool` exactly as before (`-NOAUTH` for anything but `AUTH`/`HELLO`/`QUIT` while unauthenticated), but immediately after that gate it now looks up the authenticated user's `AclUser` by `auth_user` and calls two new real methods before dispatching: `can_execute_command(cmd_name)` (checks `disallowed_commands`/`allowed_commands` depending on `all_commands`, always allowing `PING`/`RESET`/`QUIT`/`AUTH`/`HELLO`) and, if the command has a primary key, `can_access_key(key)` (checks `allowed_key_patterns`, a `prefix*`/exact-match list, unless `all_keys`). A denied command gets a real `-NOPERM` reply and returns without executing (§4.1). The pipelined squash-eligibility check (`execute_commands_squashed`) also consults these two methods per command — a command an ACL would deny just falls back to the sequential path, where the real `-NOPERM` denial happens.
3. **Passwords are now hashed, but plaintext storage was not removed — this is a real gap, not full resolution.** `AclUser` gained a `password_hashes: Vec<String>` field, and `hash_password` computes `SHA1("rudis_acl_salt_v1:" + password)`. But `ACL SETUSER user >password` still pushes the plaintext into `passwords` *and* the hash into `password_hashes` (§4.2) — the plaintext field was never removed, so anyone who could previously read plaintext passwords from memory/a core dump still can. The hash itself is also weak by password-hashing standards: SHA1 is a fast general-purpose hash (not a slow KDF like Argon2/bcrypt/scrypt, so no work-factor resistance to offline brute force), and the salt (`"rudis_acl_salt_v1:"`) is a single hardcoded constant shared by every user and every deployment, not a per-user random salt — identical passwords across users or across a fleet of Rudis instances produce identical hashes, and the fixed salt is trivially precomputable into a rainbow table once known. `check_auth` accepts a match against either the plaintext or the hash (`§4.1`), so both weaknesses are live simultaneously.
4. **TLS is now genuinely wired up — and its kTLS fast-path has a real, severe bug: it silently transmits plaintext.** A `--tls-port` listener now exists (§4.4) and performs a real `rustls` handshake via `TlsSession::handshake_monoio`. After a successful handshake, it unconditionally attempts `enable_ktls`, which only calls `setsockopt(IPPROTO_TCP, TCP_ULP, "tls")` — attaching the kernel's TLS upper-layer-protocol module — and, if that syscall merely *succeeds* (which it will on any Linux host with the `tls` kernel module loadable, regardless of whether any key material was ever installed), sets `is_ktls_active = true`. Both `TlsSession::read_plaintext` and `write_plaintext` then branch on `is_ktls_active`: when true, they read/write **raw socket bytes directly, with no rustls encryption/decryption at all**, on the assumption the kernel is doing it. But the second, actually-required `setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` call that installs the negotiated key into the kernel socket is still missing (this exact gap was already flagged when the code was dead — see §4.4) — so the kernel never encrypts anything, and **every byte of application data after the handshake goes out on the wire in cleartext**, while both client and server believe they completed a real TLS session. This is worse than TLS simply not existing: a client connecting to `--tls-port` gets a real, correctly-negotiated handshake and then a false sense of security for the rest of the connection.
5. **Allocator stats are read-only telemetry**, gated behind no lock beyond jemalloc's own internal epoch counter (`tikv_jemalloc_ctl::epoch::advance()`), safely callable from any thread without coordination with the rest of Rudis.

---

### 3. Component Architecture & Data Structures

```
                 AUTH user pass  /  ACL SETUSER|GETUSER|LIST|USERS|DELUSER|WHOAMI
                                     │
                     PORT_ACLS: Mutex<HashMap<port, Arc<RwLock<AclManager>>>>
                                     │
                          AclManager { users: HashMap<String, AclUser> }
                                     │
                     check_auth(username, password) -> Result<String, &str>
                     (accepts a plaintext OR password_hashes match — §2.3)
                                     │
                     sets `authenticated = true`, then on EVERY subsequent command:
                     user.can_execute_command(name) && user.can_access_key(key)?
                     -NOPERM if either check fails (§2.2/§4.1) — real enforcement now


                 INFO command (memory section)                    (unchanged)
                                     │
                     allocator::format_memory_info(used_mem, max_mem, ...)
                                     │
                     allocator::get_allocator_stats()  →  tikv_jemalloc_ctl::stats::*


                 --tls-port listener (server.rs, new) ──► TlsSession::handshake_monoio
                                                            (real rustls handshake)
                                                                     │
                                                            enable_ktls(TCP_ULP) "succeeds"
                                                                     │
                                                  is_ktls_active = true, NO key installed
                                                                     │
                                            ⚠️ read_plaintext/write_plaintext skip rustls
                                               entirely and touch the raw socket — every
                                               byte after the handshake is sent in the
                                               clear (§2.4/§4.4)
```

#### The real `AclUser` / `AclManager` (`src/acl.rs`, updated)

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    pub passwords: Vec<String>,          // still plaintext, still populated (§2.3)
    pub password_hashes: Vec<String>,    // new: SHA1 with a fixed global salt (§2.3)
    pub nopass: bool,
    pub all_commands: bool,
    pub allowed_commands: hashbrown::HashSet<String>,   // new
    pub disallowed_commands: hashbrown::HashSet<String>, // new
    pub all_keys: bool,
    pub allowed_key_patterns: Vec<String>,               // new
}
```

`allowed_commands`/`disallowed_commands`/`allowed_key_patterns` now exist and are genuinely
read by `can_execute_command`/`can_access_key` (§2.2/§4.1) — the old doc's "no such fields
exist" finding no longer holds. `AclManager` is unchanged:

```rust
pub struct AclManager {
    pub users: HashMap<String, AclUser>,
}
```

#### The real allocator stats (`src/allocator.rs`)

```rust
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocatorStats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub metadata: usize,
    pub mapped: usize,
    pub fragmentation_ratio: f64,
}
```

(`fragmentation_ratio` is derived as `resident / allocated`, not a jemalloc-native field.)

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Authentication + real authorization (updated) — `check_auth`, `can_execute_command`, `can_access_key`

```rust
pub fn check_auth(&self, username: Option<&str>, password: &str) -> Result<String, &'static str> {
    let user_name = username.unwrap_or("default");
    if let Some(user) = self.users.get(user_name) {
        if !user.enabled { return Err("WRONGPASS User is disabled"); }
        let hashed = hash_password(password);
        if user.nopass
            || user.passwords.iter().any(|p| p == password)
            || user.password_hashes.iter().any(|h| h == &hashed || h == password)
        {
            Ok(user_name.to_string())
        } else {
            Err("WRONGPASS invalid username-password pair or user is disabled.")
        }
    } else {
        Err("WRONGPASS invalid username-password pair or user is disabled.")
    }
}
```

Note `check_auth` accepts a match against `password_hashes` via **either** the freshly-computed
hash **or** the raw password string itself (`h == password`) — this lets `ACL SETUSER user
#<precomputed-hash>` (Redis's real syntax for pre-hashed passwords) work by comparing the
stored value directly against whatever the client sent, without knowing in advance whether
that stored value is a hash or plaintext.

The real new enforcement, in `connection.rs`'s `execute_command`, runs immediately after the
existing `-NOAUTH` gate and before the command's match arm:

```rust
if !*authenticated && !matches!(cmd, Command::Auth { .. } | Command::Hello { .. } | Command::Quit) {
    out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
    return false;
}

if *authenticated {
    let acl = crate::acl::get_acl_for_port(router.port);
    let acl_guard = acl.read().unwrap();
    if let Some(user) = acl_guard.get_user(auth_user) {
        if !user.can_execute_command(cmd_name) {
            out.extend_from_slice(format!(
                "-NOPERM this user has no permissions to run the '{}' command\r\n",
                cmd_name.to_lowercase()
            ).as_bytes());
            return false;
        }
        if let Some(key) = cmd_primary_key(&cmd)
            && !user.can_access_key(key.as_ref())
        {
            out.extend_from_slice(b"-NOPERM this user has no permissions to access one of the keys used as arguments\r\n");
            return false;
        }
    }
}
```

with the real check methods on `AclUser`:

```rust
pub fn can_execute_command(&self, cmd_name: &str) -> bool {
    let name = cmd_name.to_lowercase();
    if matches!(name.as_str(), "ping" | "reset" | "quit" | "auth" | "hello") { return true; }
    if self.all_commands { !self.disallowed_commands.contains(&name) }
    else { self.allowed_commands.contains(&name) }
}

pub fn can_access_key(&self, key: &[u8]) -> bool {
    if self.all_keys { return true; }
    let key_str = String::from_utf8_lossy(key);
    self.allowed_key_patterns.iter().any(|pat| {
        pat == "*"
            || pat.strip_suffix('*').is_some_and(|prefix| key_str.starts_with(prefix))
            || key_str == *pat
    })
}
```

A denied user genuinely gets `-NOPERM`, not a silent allow. `execute_commands_squashed`'s
squash-eligibility loop calls the same two methods per queued command; a command an ACL
would deny simply falls back to the sequential path, where the real denial above fires.

#### 4.2 `ACL SETUSER` — now recognizes command/key rules too, but still silently drops the rest

```rust
for rule in rules {
    if rule == "on" { user.enabled = true; }
    else if rule == "off" { user.enabled = false; }
    else if rule == "nopass" { user.nopass = true; user.passwords.clear(); user.password_hashes.clear(); }
    else if let Some(p) = rule.strip_prefix('>') {
        user.nopass = false;
        user.passwords.push(p.to_string());              // plaintext still stored — §2.3
        user.password_hashes.push(hash_password(p));       // hash added alongside it
    }
    else if let Some(h) = rule.strip_prefix('#') { user.password_hashes.push(format!("#{}", h)); }
    else if let Some(p) = rule.strip_prefix('<') { user.passwords.retain(|pass| pass != p); }
    else if rule == "+@all" || rule == "+all" { user.all_commands = true; user.disallowed_commands.clear(); }
    else if rule == "-@all" || rule == "-all" { user.all_commands = false; user.allowed_commands.clear(); }
    else if let Some(cmd) = rule.strip_prefix('+') {       // new: per-command allow
        let c = cmd.to_lowercase();
        if user.all_commands { user.disallowed_commands.remove(&c); } else { user.allowed_commands.insert(c); }
    }
    else if let Some(cmd) = rule.strip_prefix('-') {       // new: per-command deny
        let c = cmd.to_lowercase();
        if user.all_commands { user.disallowed_commands.insert(c); } else { user.allowed_commands.remove(&c); }
    }
    else if rule == "~*" || rule == "allkeys" { user.all_keys = true; user.allowed_key_patterns.clear(); }
    else if rule == "resetkeys" { user.all_keys = false; user.allowed_key_patterns.clear(); }
    else if let Some(pat) = rule.strip_prefix('~') { user.all_keys = false; user.allowed_key_patterns.push(pat.to_string()); }
}
```

`ACL SETUSER bob on >pw -@all +get ~user:*` now genuinely produces a user who can only run
`GET` (plus the always-allowed `PING`/`RESET`/`QUIT`/`AUTH`/`HELLO`) against keys matching
`user:*` — verified by a real unit test (`test_acl_command_and_key_enforcement`). The old
doc's finding that `+@category` tokens (e.g. `+@read`/`-@write`) and other glob forms like
`&channel:*` are silently accepted-but-ignored still holds — only bare per-command `+cmd`/
`-cmd` tokens and simple `prefix*`/exact-match key patterns are real; category-level and
pub/sub-channel ACL rules are not implemented.

#### 4.3 Allocator telemetry — real jemalloc reads, exposed through `INFO`

```rust
pub fn get_allocator_stats() -> AllocatorStats {
    let _ = tikv_jemalloc_ctl::epoch::advance();
    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0);
    let active = tikv_jemalloc_ctl::stats::active::read().unwrap_or(0);
    let resident = tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0);
    let metadata = tikv_jemalloc_ctl::stats::metadata::read().unwrap_or(0);
    let mapped = tikv_jemalloc_ctl::stats::mapped::read().unwrap_or(0);
    let fragmentation_ratio = if allocated > 0 { resident as f64 / allocated as f64 } else { 1.0 };
    AllocatorStats { allocated, active, resident, metadata, mapped, fragmentation_ratio }
}
```

`format_memory_info` (also in `allocator.rs`) wraps this into the RESP bulk string returned by `INFO`'s memory section — confirmed by its single real call site in `connection.rs` (`crate::allocator::format_memory_info(...)`). There is no heap-profiling/dump capability (no `jemalloc_pprof`-style export) — this module is stats-only.

#### 4.4 TLS is now wired end-to-end — and its kTLS fast-path has a live plaintext bug

`main.rs` gained `--tls-port`/`--tls-cert-file`/`--tls-key-file` flags; when `--tls-port` is
set, each shard's `run_shard_worker` (Component 01) now binds a **second** `SO_REUSEPORT`
listener on that port, parallel to the plain one, and spawns a dedicated accept loop for it:

```rust
// server.rs — spawned only if tls_config is Some
match tls_listener.accept().await {
    Ok((mut stream, client_addr)) => {
        let mut session = crate::tls::TlsSession::new(s_cfg)?;
        session.handshake_monoio(&mut stream).await?;          // real rustls handshake
        crate::connection::handle_tls_connection(stream, session, client_addr, client_id, reg_clone, router_clone).await;
    }
    ...
}
```

`handshake_monoio` is a genuine, correctly-written async adaptation of the `rustls` handshake
loop (`wants_write`/`write_tls`/`wants_read`/`read_tls`/`process_new_packets`, driven through
`monoio`'s `AsyncReadRent`/`AsyncWriteRentExt` instead of blocking I/O) — the handshake itself
is real and correctly negotiates a TLS session. `handle_tls_connection` (new, in
`connection.rs`) then runs the same command-execution machinery as `handle_connection`, but
reads/writes through `TlsSession::read_plaintext`/`write_plaintext` instead of the raw socket.

**⚠️ The bug**: at the end of a successful handshake, both `complete_handshake` (still unused
directly) and `handshake_monoio` unconditionally call `enable_ktls`, and `enable_ktls` — same
as before — only performs the *first* of the two Linux kTLS setup calls:

```rust
pub fn enable_ktls(raw_fd: RawFd) -> io::Result<()> {
    let ret = unsafe { libc::setsockopt(raw_fd, IPPROTO_TCP, TCP_ULP, b"tls\0".as_ptr() as *const _, 4) };
    if ret == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}
// handshake_monoio, after a successful handshake:
if enable_ktls(raw_fd).is_ok() { self.is_ktls_active = true; }
```

`setsockopt(IPPROTO_TCP, TCP_ULP, "tls")` only attaches the kernel's TLS upper-layer-protocol
module to the socket — it succeeds regardless of whether any key material is ever installed,
and the Linux kernel does not encrypt anything from this call alone. The second, actually
required call — `setsockopt(SOL_TLS, TLS_TX, ...)`/`TLS_RX` installing the negotiated
cipher/key/IV — still does not exist anywhere in this file, exactly as before this update.
The difference now is that `is_ktls_active` gates real I/O behavior:

```rust
// TlsSession::read_plaintext / write_plaintext
if self.is_ktls_active {
    // reads/writes the RAW socket directly — no rustls encrypt/decrypt at all
    let (res, returned) = stream.read(std::mem::take(read_buf)).await;
    ...
} else {
    // real rustls-mediated encrypt/decrypt path
}
```

Since `enable_ktls` "succeeds" on any Linux host where the `tls` kernel module is loadable
(common on modern distros) regardless of key installation, `is_ktls_active` becomes `true` on
essentially every real Linux deployment, and **every byte of application data sent after the
handshake goes out on the wire completely unencrypted** — while the client and server both
believe they completed a real TLS session, because the handshake itself genuinely succeeded.
This is strictly worse than the previous "TLS code exists but nothing calls it" state: before,
no one could accidentally rely on TLS that wasn't there; now, enabling `--tls-port` produces a
working handshake followed by silent plaintext, which is the worst version of this failure
mode for anyone who trusts it. Treat `--tls-port` as unsafe to use until either `enable_ktls`
performs the real key-install `setsockopt` call, or (much simpler and lower-risk) `is_ktls_active`
is just never set to `true` by a bare `TCP_ULP` success, and the code always takes the real
`rustls`-mediated encrypt/decrypt path.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: `get_acl_for_port` backs both the `AUTH`/`ACL *` command handlers and the new per-command/per-key enforcement in `execute_command`/`execute_commands_squashed` (§4.1); `allocator::format_memory_info` is called via `INFO`; and, new, `handle_tls_connection` is a second connection-handling entry point (alongside plain `handle_connection`) that routes all I/O through a `TlsSession` (§4.4).
- **`src/server.rs`** (Component 01): now conditionally binds a second `SO_REUSEPORT` listener on `--tls-port` and spawns a dedicated TLS accept loop per shard (§4.4), in addition to everything it did before.
- **`src/main.rs`**: parses the new `--tls-port`/`--tls-cert-file`/`--tls-key-file` CLI flags, builds a `rustls::ServerConfig` once (via `tls::create_server_config` or `tls::generate_self_signed_cert` if no cert/key files are given) before spawning any shard thread, and passes it down as `TlsWorkerConfig`.
- **`src/tiering.rs`**: does not call `allocator::get_allocator_stats()` — memory-pressure decisions for auto-tiering are driven by a separately tracked `used_memory` estimate on `RudisTable` (see Component 05), not by live jemalloc RSS figures. Unchanged by this update.

---

### 6. Performance Characteristics

- **Auth check cost**: one `RwLock::read()` acquisition plus a linear scan over `passwords`/`password_hashes` (typically 0-1 entries each) plus one SHA1 computation per `AUTH` call — negligible, and only paid once per connection lifetime in the common case.
- **Real per-command ACL overhead now exists (updated)**: every command after authentication takes an `AclManager` read-lock and a `HashMap`/`HashSet` lookup via `can_execute_command`/`can_access_key` (§4.1) — small, but no longer zero as the old doc stated; this is a real, permanent per-command cost on every connection now, not just at `AUTH` time.
- **Allocator stats are cheap but not free**: unchanged — `epoch::advance()` triggers jemalloc to refresh its internal counters, more than a simple atomic load; only invoked from `INFO`, not a hot-path command.
- **TLS has real handshake and per-byte I/O cost now that it's wired up**: the `rustls` handshake (§4.4) is genuine CPU work paid once per TLS connection; ongoing traffic either goes through real `rustls` encrypt/decrypt (the safe, intended path) or — per the §4.4 bug — bypasses encryption entirely once `is_ktls_active` is (incorrectly) set, which is *faster* than real encryption precisely because it isn't doing any. Do not read that speed as a feature.

---

### 7. Future Improvements

- **CRITICAL, was Medium — fix or disable the kTLS plaintext-bypass bug before `--tls-port` is used anywhere (§2.4/§4.4).** This is now the single most urgent item in this entire document: enabling `--tls-port` produces connections that complete a real TLS handshake and then silently send all application data unencrypted, because `is_ktls_active` is set from a `TCP_ULP` `setsockopt` success alone, with no actual key-install call ever made. The fastest safe fix is the smallest one: stop setting `is_ktls_active = true` from `enable_ktls`'s current (incomplete) implementation — always take the real `rustls` encrypt/decrypt path in `read_plaintext`/`write_plaintext` until `enable_ktls` is extended to also perform the `setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` key-install call. Until one of these lands, `--tls-port` should not be documented or offered as a secure option.
- **High — remove plaintext password storage now that hashing exists (§2.3).** `ACL SETUSER user >password` still pushes the plaintext into `AclUser.passwords` in addition to hashing it into `password_hashes` — the hashing work was done but the vulnerability it was meant to close (plaintext credentials sitting in process memory / reachable via a core dump) was not actually closed. Stop populating `passwords` from the `>` rule (keep it only for backward-compatible reads of already-stored plaintext, if any migration path requires that), and have `check_auth` compare only against `password_hashes`.
- **High — replace the fixed-salt SHA1 scheme with a real per-user-salted slow hash (§2.3).** `hash_password` uses one hardcoded global salt (`"rudis_acl_salt_v1:"`) shared across every user and every Rudis instance, with a fast general-purpose hash (SHA1) that has no work-factor resistance to offline brute force. A per-user random salt plus Argon2id (or at minimum bcrypt/scrypt/PBKDF2 with a real iteration count) closes both weaknesses — the fixed-salt SHA1 hash is barely better than plaintext against a determined offline attacker.
- **Medium — extend `ACL SETUSER` to support command categories and pub/sub channel patterns (§4.2).** `+@read`/`-@write`-style category tokens and `&channel:*` pub/sub ACL rules are still silently accepted and ignored, exactly as before this update — only bare per-command and per-key-prefix rules are real. Either implement categories/channels or make `ACL SETUSER` reject unrecognized rule tokens with an error, so a deployment can't believe it applied a restriction that was silently dropped.
- **Low — expose jemalloc heap-profiling/dump capability, not just aggregate stats (§4.3)**, if deep memory-leak/fragmentation debugging in production ever becomes a need — `tikv-jemalloc-ctl` supports profiling hooks beyond the stats-only reads currently used. Unchanged by this update.

---
---

## Component 16: JSON Document Store & JSONPath Engine (`src/json.rs`)

### 1. Architectural Purpose & Scope

`src/json.rs` implements a RedisJSON-compatible document store: a hand-written JSONPath
parser/evaluator operating directly on `serde_json::Value` trees, plus `JsonStore`, the
per-shard map of key → JSON document that backs `JSON.SET`/`GET`/`DEL`/`TYPE`/`NUMINCRBY`/
`STRAPPEND`/`STRLEN`/`ARRAPPEND`/`ARRLEN`/`ARRPOP`/`OBJKEYS`/`OBJLEN`/`TOGGLE`/`CLEAR`/`MGET`.
Unlike `src/vector.rs` (Component 08) and unlike `src/crdt.rs` before its fix (Component 12),
single-key JSON commands are **genuinely routed per-key across shards** — verified directly
in `connection.rs`: every `Command::Json*` variant (except `JsonMget`, see §4.5) appears in
the same `target_shard_of_cmd`/local-vs-`execute_remote` dispatch arm as ordinary string/hash/
list commands, so a `JSON.SET`/`GET` on a given key always lands on the one shard that key
actually hashes to, regardless of which shard's connection issued it.

---

### 2. Key Invariants & Concurrency Constraints

1. **A real, but partial, JSONPath implementation.** `parse_json_path` hand-parses `$`, bare
   `.field` traversal, `[idx]` (including negative indices), `[*]` wildcards, `[start:end]`
   slices (including negative/omitted bounds), and `["quoted"]`/`['quoted']` field names. There
   is **no recursive descent (`$..field`) and no filter-expression syntax (`?(@.price < 10)`)**
   — both real RedisJSON/JSONPath features. A path using either silently fails to match
   anything (parses as a literal field name containing those characters) rather than erroring.
2. **Whole-document storage, no incremental structure.** `JsonStore.docs: HashMap<Bytes,
   Value>` stores one complete `serde_json::Value` tree per key. A `JSON.SET`/`NUMINCRBY`/etc.
   on a deeply nested path still has to parse the target's own sub-value in place (via
   `query_json_path_mut`, no full-document re-parse), but `JSON.GET` always calls
   `serde_json::to_string` fresh on whatever subtree matched — there's no cached serialized
   form, and a `JSON.GET key $` on a huge document re-serializes the entire thing every call.
3. **`query_json_path`/`query_json_path_mut` are structurally identical, hand-duplicated for
   `&`/`&mut`.** Every match arm in the immutable traversal (§4.2) has a corresponding
   `_mut` arm doing the identical navigation logic against `.get`/`.get_mut`,
   `.values()`/`.values_mut()`, `&arr[i]`/`&mut arr[i]`. This is a real, verified
   duplication (not a design choice with a stated rationale) — a bugfix to one traversal
   rule (e.g. how negative slice bounds clamp) has to be applied to both copies by hand.
4. **Auto-vivification on `SET`, not on read.** `set_json_path` creates intermediate
   `Object`/`Array` containers as needed when writing to a path whose parents don't exist yet
   (§4.3) — real Redis JSON has the same behavior. `NX`/`XX` are checked once, up front,
   against whether the *target* path already resolves to something, before any mutation.

---

### 3. Component Architecture & Data Structures

```
JSON.SET doc:1 $.user.name "\"Alice\""
              │
              ▼
   parse_json_path("$.user.name") -> [Root, Field("user"), Field("name")]
              │
              ▼
   set_json_path: walk parent segments (Root, Field("user")),
   auto-vivifying an empty Object at "user" if it doesn't exist yet,
   then insert "name" -> Value::String("Alice") into that object
              │
              ▼
   JsonStore.docs[doc:1] = { "user": { "name": "Alice" } }
```

### Real data structures (verbatim from `src/json.rs`)

```rust
pub enum PathSegment {
    Root,
    Field(String),
    Index(isize),
    Wildcard,
    Slice { start: Option<isize>, end: Option<isize> },
}

pub struct JsonStore {
    docs: HashMap<Bytes, Value>,   // Value = serde_json::Value
}
```

There is no bespoke JSON representation — every stored document is a plain `serde_json::Value`
(the same enum `Value::{Null, Bool, Number, String, Array, Object}` any `serde_json` consumer
would use), not a Redis-specific compact encoding.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `parse_json_path`: character-by-character, no grammar/lexer library

The parser is a single hand-rolled `while let Some(&ch) = chars.peek()` loop over a
`Peekable<Chars>` iterator, branching on `.`, `*`, `[`, or "anything else starts a bare field
name" — no `nom`/`pest`/regex dependency. Bracket contents (`[...]`) are further classified by
trying, in order: `*` (wildcard), a quoted string, a `:`-containing slice, a parseable integer
(index), then falling back to an unquoted field name. This ordering matters: `[0:2]` is
recognized as a slice before the parser ever tries to parse it as an integer.

#### 4.2 `query_json_path` / `query_json_path_mut`: breadth-first accumulation per segment

Both functions maintain a `Vec` of "current matches" and, for each `PathSegment` in turn,
build a new `Vec` of every match's children that satisfy that segment — so `$.items[*].id`
naturally fans out to multiple simultaneous matches (one per array element) by the time it
reaches the final `Field("id")` segment. A `Slice`'s `start`/`end` are each independently
clamped: negative values count from the end (`len + s`), values are `.max(0)`/`.min(len)`
bounded, and `s_idx < e_idx` is required for anything to be included — an empty or
backward-ordered slice range simply matches nothing rather than erroring.

#### 4.3 `set_json_path`: parent traversal with auto-vivification, then a final insert

```rust
for seg in parent_segments {
    match seg {
        PathSegment::Field(name) => {
            if !curr.is_object() { *curr = Value::Object(serde_json::Map::new()); }
            let map = curr.as_object_mut().unwrap();
            if !map.contains_key(name) { map.insert(name.clone(), Value::Object(...)); }
            curr = map.get_mut(name).unwrap();
        }
        PathSegment::Index(idx) => {
            if !curr.is_array() { *curr = Value::Array(Vec::new()); }
            let arr = curr.as_array_mut().unwrap();
            while arr.len() <= actual_idx { arr.push(Value::Null); }
            curr = &mut arr[actual_idx];
        }
        _ => return Err("ERR wildcards not supported as parent path for SET"),
    }
}
```

If an intermediate path element exists but is the *wrong type* (e.g. `$.user.name` where
`user` is currently a string, not an object), it's silently **overwritten** with a fresh empty
container (`*curr = Value::Object(...)`) rather than erroring — a real, permissive behavior
worth knowing: `JSON.SET` can silently destroy a differently-typed intermediate value on the
way to setting a deep path. Setting through a `Wildcard`/`Slice` parent segment is the one
case that does return a real error (`"ERR wildcards not supported as parent path for SET"`).

#### 4.4 `delete_json_path`: wildcard deletes clear whole containers

A `Field`/`Index` last-segment delete removes one entry; a `Wildcard` last segment instead
clears the entire matched `Object`/`Array` in place (`map.clear()`/`arr.clear()`) and counts
every removed entry — so `JSON.DEL key $.items[*]` empties the `items` array (leaving an empty
array behind, not removing the array itself) rather than deleting each element one at a time.

#### 4.5 `JSON.MGET`: sequential per-key, not fanned out — the same gap `MGET`/`MSET` had before their fix

```rust
Command::JsonMget { keys, path } => {
    for k in keys {
        let single_cmd = Command::JsonGet { key: k.clone(), paths: vec![path.clone()] };
        if let Some(target) = target_shard_of_cmd(&single_cmd, router.num_shards) {
            if target == router.shard_id { /* local execute_local_command */ }
            else { let res = router.execute_remote(target, single_cmd).await; /* ... */ }
        } else { out.extend_from_slice(b"$-1\r\n"); }
    }
}
```

Each key in a `JSON.MGET` is routed and awaited **one at a time** — exactly the shape
`MGET`/`MSET` had before the fix documented in Component 02 §4.6/Component 04 §4.3. Unlike
plain `MGET`, `JsonMget` was never given the bucket-by-shard-then-fan-out treatment; a
`JSON.MGET` spanning several remote shards still pays one serialized round-trip per key.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): dispatches every single-key `Command::Json*` through
  the shared `target_shard_of_cmd`/local-vs-`execute_remote` fork (§1); `JsonMget` is a
  standalone arm with its own sequential per-key loop (§4.5).
- **`src/search.rs`** (Component 09): `JSON.SET` on the root path (`$`) triggers
  `index_document_hook` for auto-indexing after a successful write, flattening top-level
  scalar fields into the search engine's document representation — nested objects/arrays are
  stringified, not recursively flattened (already documented in Component 09 §5).
- **`src/table.rs`** (Component 05): no relationship — JSON documents live entirely in
  `ShardDb.json_store: JsonStore`, a separate per-shard map alongside `RudisTable`, never as a
  `RudisValue` variant.
- **RDB persistence: no relationship, verified.** Grepping `table.rs`/`router.rs`'s RDB
  save/restore chunk logic for any reference to `json_store` finds none — `JsonStore` is not
  included in `save_rdb_chunk`/`load_rdb`, so JSON documents do **not** survive a restart via
  RDB. This is the same gap Component 08 §7 flagged for vector indexes.

---

### 6. Performance Characteristics

- **`JSON.GET` cost scales with matched-subtree size, not query specificity** — every call
  does a fresh `serde_json::to_string` of whatever `query_json_path` returned, with no
  memoization; repeatedly reading the same small field from a large sibling-heavy document is
  cheap, but repeatedly reading `$` on a large document is not.
- **Path traversal is O(document breadth) per segment, not indexed** — `Field` lookups on an
  `Object` are O(1) (backed by `serde_json`'s own map), but `Wildcard`/`Slice` segments
  necessarily visit every child at that level; there's no precomputed path index.
- **`JSON.MGET`'s sequential fan-out (§4.5) is the single biggest addressable cost** on
  multi-key JSON reads spread across shards — see Future Improvements.

---

### 7. Future Improvements

- **Medium — give `JSON.MGET` the same bucket-and-fan-out treatment `MGET`/`MSET` already got (§4.5).** The building blocks are identical to Component 02/04's fix: bucket the requested keys by target shard, dispatch one batched request per remote shard, await all in parallel, reassemble in original order. Until then, `JSON.MGET` is the one JSON command that doesn't benefit from the cross-shard parallelism the rest of this subsystem already has.
- **Medium — include `JsonStore` in RDB save/restore (§5).** A shared gap with vector indexes (Component 08) and, before its own fix, CRDT state (Component 12) — any of these auxiliary per-shard stores silently losing all data on restart is a real durability surprise for a feature that otherwise looks fully persistent (ordinary keys in the same process do survive a restart).
- **Low — deduplicate `query_json_path`/`query_json_path_mut` (§2.3)**, e.g. via a macro or a trait abstracting over `&`/`&mut` child access, to remove the risk of the two traversal implementations drifting apart on a future bugfix.
- **Low — extend JSONPath coverage** (recursive descent `$..field`, filter expressions `?(@.price < N)`) if closer RedisJSON/JSONPath-spec compatibility becomes a goal (§2.1) — the current subset covers the common cases (field access, indexing, wildcards, slices) but silently no-ops on anything more advanced rather than erroring, which could surprise a client library that assumes full JSONPath support.

---
---

## Component 17: Geospatial Commands (`src/geo.rs`)

### 1. Architectural Purpose & Scope

`src/geo.rs` is pure math and reply-formatting — it owns **no storage of its own**. Every
`GEOADD`/`GEODIST`/`GEOPOS`/`GEOHASH`/`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` command is
implemented directly in `src/connection.rs` on top of the existing sorted-set (`RudisZSet`,
Component 05) API — `GEOADD` is a `ZADD` whose "score" is a 52-bit interleaved geohash encoding
of (longitude, latitude), and every other geo command decodes that score back into
coordinates. This is architecturally identical to how real Redis implements its own `GEO*`
command family as a thin layer over `ZSET`, and it means `ZRANGE`/`ZSCORE`/any other ZSET
command works unmodified against a "geo set" key too — a real compatibility feature and a real
footgun (an arbitrary `ZADD` against a geo key can insert a member with a score that isn't a
valid geohash at all, and nothing rejects it).

---

### 2. Key Invariants & Concurrency Constraints

1. **Encoding is 52-bit interleaved (26 bits longitude + 26 bits latitude), matching real
   Redis's internal encoding** — not the 5-bit-alphabet, 11-character textual geohash;
   that (`geohash_to_base32`) is only computed on demand for the `GEOHASH` command's text
   output, never used as the stored representation.
2. **Latitude is clamped to the real Web Mercator-projectable range**
   (`GEO_LAT_MIN`/`MAX` = ±85.05112878°, not ±90°) — this matches real Redis's own
   documented limitation exactly (values interleave cleanly only within this range); a
   `GEOADD` outside it is rejected with `"ERR invalid latitude"`.
3. **Distance is Haversine (great-circle on a sphere), not an ellipsoidal (Vincenty) model** —
   using `EARTH_RADIUS_METERS = 6372797.560856`, the same constant real Redis's own Haversine
   implementation uses. This is an approximation (Earth isn't a perfect sphere) but is exactly
   what real Redis does too, so behavior matches rather than diverges.
4. **Geo commands route per-key across shards exactly like ordinary ZSET commands** (verified:
   `Geoadd`/`Geodist`/`Geopos`/`Geosearch`/etc. all appear in the same `target_shard_of_cmd`
   dispatch arm as other keyed commands, Component 02 §4.5) — there is no geo-specific routing
   concern; a "geo set" is routed by the same CRC16 key-slot mechanism as any other key.

---

### 3. Component Architecture & Data Structures

```
GEOADD geo:cities -122.4194 37.7749 "San Francisco"
              │
              ▼
   encode_geohash(-122.4194, 37.7749) -> u64 (52-bit interleaved)
              │
              ▼
   db.zadd("geo:cities", [(hash as f64, "San Francisco")], flags)
              │
              ▼
   RudisZSet (Component 05) — "San Francisco" -> score = hash as f64

GEODIST geo:cities "San Francisco" "Oakland" km
              │
              ▼
   db.zscore() twice -> decode_geohash() twice -> haversine_distance() -> GeoUnit::from_meters()
```

### Real supporting types (`src/geo.rs`)

```rust
pub enum GeoUnit { Meters, Kilometers, Miles, Feet }   // to_meters/from_meters conversion factors:
                                                         // km=1000, mi=1609.344, ft=0.3048 — match real Redis

pub struct GeoItemResult {
    pub member: Bytes,
    pub dist: Option<f64>,
    pub hash: Option<u64>,
    pub coord: Option<(f64, f64)>,
}
```

`GeoItemResult` + `format_geo_results` unify the reply-formatting for every command that can
return `WITHCOORD`/`WITHDIST`/`WITHHASH` options (`GEORADIUS`, `GEORADIUSBYMEMBER`,
`GEOSEARCH`) — one shared formatter rather than three separately hand-written reply encoders.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `encode_geohash`/`decode_geohash`: bit interleaving, not a lookup table

```rust
for i in 0..26 {
    let bit_x = (x >> i) & 1;
    let bit_y = (y >> i) & 1;
    hash |= bit_x << (2 * i);
    hash |= bit_y << (2 * i + 1);
}
```

Longitude and latitude are each linearly normalized into a 26-bit integer, then their bits are
interleaved (x at even positions, y at odd) into one 52-bit value — the standard Z-order/Morton
encoding real geohashing uses, decoded by the exact inverse bit-extraction in `decode_geohash`.
Because this is lossy quantization (26 bits per axis over the real coordinate range), a
decode-then-re-encode round trip returns a coordinate close to, but not bit-identical to, the
original input — `decode_geohash` returns the *center* of the encoded cell
(`(x + 0.5) / max_val`), which is why the round-trip test in this file asserts closeness
(`< 1e-5`) rather than exact equality.

#### 4.2 `GEOADD`'s real implementation: encode, then delegate entirely to `ZADD`

```rust
for (lon, lat, member) in items {
    match crate::geo::encode_geohash(*lon, *lat) {
        Ok(hash) => elements.push((hash as f64, member.clone())),
        Err(e) => { out.extend_from_slice(...); return false; }
    }
}
let flags = crate::table::ZAddFlags { nx: *nx, xx: *xx, ch: *ch, gt: false, lt: false, incr: false };
match db.zadd(key.clone(), elements, flags) { ... }
```

`NX`/`XX`/`CH` flags pass straight through to the underlying `ZADD` semantics (Component 05) —
`GEOADD` adds no geo-specific conflict handling of its own beyond the encoding step.

#### 4.3 `GEODIST`: two `zscore` lookups, decode, Haversine, unit conversion

Straightforward: `db.zscore(key, m1)`/`db.zscore(key, m2)` fetch the two raw geohash scores,
`decode_geohash` turns each back into coordinates, `haversine_distance` computes meters, and
the requested `GeoUnit` (default meters) converts the final figure — a member with no score
(never added, or added via a non-geo `ZADD` with a score that happens to decode to garbage
coordinates) returns `$-1\r\n` only if the `zscore` lookup itself misses, not if the decoded
"coordinates" are nonsensical.

#### 4.4 `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`: three independent, near-identical brute-force scans

All three commands share the exact same shape, implemented as three separate, hand-duplicated
blocks (not a shared helper function):

```rust
let z_opts = crate::table::ZRangeOpts { start: 0, stop: -1, with_scores: true, ..Default::default() };
if let Ok(pairs) = db.zrange(key, &z_opts) {
    for (member, score) in pairs {
        let (m_lon, m_lat) = crate::geo::decode_geohash(score as u64);
        let dist = crate::geo::haversine_distance(center_lon, center_lat, m_lon, m_lat);
        if dist <= radius_meters { /* collect into results */ }
    }
}
```

**This is a full O(N) scan of every member in the geo set for every search**, decoding and
computing Haversine distance against each one, regardless of the search radius or how many
members actually match. Real Redis's own `GEORADIUS`/`GEOSEARCH` implementation instead uses
the sorted-by-geohash structure of the underlying skiplist to narrow the scan to a small
neighborhood of 52-bit-geohash-adjacent score ranges (the "9 neighboring geohash cells"
technique) before doing exact distance filtering — this implementation does none of that
narrowing; it always decodes and distance-checks the entire set. `GEOSEARCH`'s `BY BOX` variant
additionally approximates a box as `dlat_m = Δlat° × 111,320` /
`dlon_m = Δlon° × 111,320 × cos(center_lat)` — a flat-Earth-locally approximation, not exact
geodesic box math, reasonable at the box sizes these commands are typically used for but not
precise at very large box dimensions.

---

### 5. Cross-Component Interactions

- **`src/table.rs`** (Component 05): every geo command is built entirely on `RudisTable`'s
  `zadd`/`zscore`/`zrange` — there is no geo-specific storage anywhere; a "geo set" *is* a
  `RudisZSet`.
- **`src/connection.rs`** (Component 02): owns every `Command::Geo*` match arm (`geo.rs` itself
  contains no command dispatch, only the math/formatting helpers those arms call); routes all
  of them through the standard `target_shard_of_cmd` keyed-command path (§2.4).
- **`src/resp.rs`** (Component 03): parses `GEOADD`/`GEODIST`/etc.'s arguments (including unit
  strings `m`/`km`/`mi`/`ft` and `BYRADIUS`/`BYBOX`/`ASC`/`DESC`/`WITHCOORD`/`WITHDIST`/
  `WITHHASH` option flags) into the `Command::Geo*` variants this file's helpers consume.

---

### 6. Performance Characteristics

- **`GEOADD`/`GEODIST`/`GEOPOS` are O(1)-ish**, bounded by the underlying `ZADD`/`ZSCORE` cost
  (Component 05) plus a fixed amount of bit-interleaving/Haversine math — no scan involved.
- **`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` are all O(N) in the size of the geo set**
  (§4.4), not O(matches) or O(log N + matches) the way a geohash-neighborhood-aware
  implementation would be — a large geo set with a small-radius search still decodes and
  distance-checks every member.
- **No caching of decoded coordinates** — every scan re-runs `decode_geohash` (cheap bit
  extraction) per member per call; not a measurable cost relative to the Haversine
  trigonometry, which dominates.

---

### 7. Future Improvements

- **Medium — narrow `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`'s scan using geohash-neighborhood pruning instead of a full O(N) scan (§4.4).** Since scores are already geohash-ordered when read via a range on the sorted structure, computing the target radius/box's covering geohash cell(s) and querying only score ranges near them (real Redis's approach) would turn this into roughly O(log N + matches) instead of O(N) — the highest-value fix here for any geo set large enough to matter.
- **Low — factor `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`'s shared scan-and-filter logic into one helper (§4.4).** Three independently hand-written copies of the same loop is a real duplication-drift risk — a bugfix or the geohash-neighborhood optimization above would otherwise need to be applied three times.
- **Low — replace the `BY BOX` flat-Earth approximation with exact geodesic box math** (§4.4) if very large box searches (spanning enough latitude/longitude to make the flat approximation's error non-negligible) become a real use case — not a concern at typical city-scale search radii.
- **Low — reject non-geo `ZADD`s against a key already used as a geo set, or document the compatibility footgun explicitly (§1)** — since a geo set is just a `ZSET`, nothing stops an ordinary `ZADD member not-a-geohash-score` from corrupting later `GEODIST`/`GEOPOS`/`GEOSEARCH` calls against that key with silently-nonsensical decoded coordinates.

---
---

## Component 18: Probabilistic Data Structures (`src/probabilistic.rs`)

### 1. Architectural Purpose & Scope

`src/probabilistic.rs` implements four independent approximate-membership/frequency data
structures — a **Bloom Filter**, a **Cuckoo Filter**, a **Count-Min Sketch**, and a **Top-K
frequency tracker** (Space-Saving algorithm) — exposed via RedisBloom-compatible commands
(`BF.*`, `CF.*`, `CMS.*`, `TOPK.*`). Each structure type has its own per-key map inside
`ProbabilisticStore`, which lives in `ShardDb` alongside `vector_indexes`/`crdt_store`/
`json_store`. Like `src/json.rs` (Component 16) and `src/geo.rs` (Component 17) and unlike
`src/vector.rs` (Component 08), every single-key command here is genuinely routed per-key
across shards — confirmed directly in `connection.rs`: `BfAdd`/`CfAdd`/`CmsIncrby`/`TopkAdd`/
etc. all appear in the same `target_shard_of_cmd`/local-vs-`execute_remote` dispatch arm as
ordinary keyed commands.

---

### 2. Key Invariants & Concurrency Constraints

1. **A single custom hash function underlies all four structures.** `fnv1a_hash` (a
   seeded 64-bit FNV-1a) and `double_hash` (two independent FNV-1a calls with different fixed
   seeds, used for Kirsch-Mitzenmacher double-hashing) are shared by the Bloom filter, Cuckoo
   filter, and Count-Min Sketch — there is no per-structure hash family, and no cryptographic
   hash anywhere in this file (not a concern for these structures' intended use, unlike an
   auth-adjacent context).
2. **Bloom filter sizing follows the standard formulas, computed once at creation.**
   `BloomFilter::new(capacity, error_rate)` derives bit-array size via
   $m = \lceil -n \ln(p) / (\ln 2)^2 \rceil$ and hash count via $k = \text{round}((m/n)\ln 2)$,
   clamped to `[1, 30]` hashes — real, textbook Bloom filter parameter derivation, not
   hardcoded constants.
3. **The Cuckoo filter is a real, complete implementation including eviction ("cuckoo
   kicks").** `add` tries both candidate buckets first, and only falls back to the
   randomized-kick relocation loop (`MAX_KICKS = 500`) if both are full — a genuine cuckoo
   hashing insert, not a simplified always-fails-when-full variant. `delete` is also real
   (removes a matching fingerprint from either candidate bucket), which is one of the
   Cuckoo filter's actual advantages over a Bloom filter (Bloom filters can't support
   deletion at all without a counting variant, which isn't implemented here).
4. **The Top-K tracker is a real Space-Saving algorithm, not an exact top-K.** Once at
   capacity, `TopK::add` evicts the *minimum-count* tracked item and gives the new item that
   evicted item's count plus the increment — the standard Space-Saving guarantee (every
   tracked count is an overestimate, bounded by the true frequency of whatever was evicted
   last), not an exact frequency count.
5. **No structure ever shrinks or is auto-resized.** A Bloom/Cuckoo filter's bit array or
   bucket count is fixed at creation time (`BF.RESERVE`/`CF.RESERVE`'s capacity argument); a
   Count-Min Sketch's width/depth are likewise fixed at `CMS.INITBYDIM`/`INITBYPROB` time.
   There is no `BF.INSERT ... EXPANSION` auto-scaling behavior — once a filter created with a
   given capacity is over-inserted, its false-positive rate silently degrades rather than the
   structure growing.

---

### 3. Component Architecture & Data Structures

```
BF.RESERVE myfilter 0.01 1000        CF.ADD mycuckoo item
        │                                     │
        ▼                                     ▼
BloomFilter::new(1000, 0.01)          CuckooFilter::add("item")
  m,k derived from formula                    │
        │                          fingerprint(item) -> u16
        ▼                          indices(item, fp) -> (i1, i2)
ProbabilisticStore                  try buckets[i1]/[i2], else
  .bloom_filters["myfilter"]        cuckoo-kick up to 500 times
```

### Real data structures (verbatim from `src/probabilistic.rs`)

```rust
pub struct BloomFilter { pub capacity: usize, pub error_rate: f64, pub num_bits: usize,
                          pub num_hashes: usize, pub count: usize, pub bits: Vec<u64> }

pub struct CuckooFilter { pub capacity: usize, pub num_buckets: usize, pub count: usize,
                           pub buckets: Vec<[u16; 4]> }   // BUCKET_SIZE = 4

pub struct CountMinSketch { pub width: usize, pub depth: usize, pub total_count: u64,
                             pub table: Vec<Vec<u64>> }

pub struct TopK { pub k: usize, pub items: HashMap<Bytes, u64> }   // Space-Saving

pub struct ProbabilisticStore {
    pub bloom_filters: HashMap<Bytes, BloomFilter>,
    pub cuckoo_filters: HashMap<Bytes, CuckooFilter>,
    pub cms_sketches: HashMap<Bytes, CountMinSketch>,
    pub topk_trackers: HashMap<Bytes, TopK>,
}
```

Each structure type gets its own separate `HashMap` inside `ProbabilisticStore` — a key used
for a Bloom filter and a key of the same name used for a Cuckoo filter would be two completely
independent entries (in different maps), not a naming collision, since the command layer
(`connection.rs`) dispatches to the right map by command family (`BF.*` vs `CF.*` vs...), not
by inspecting what's already stored under that key.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Bloom filter: bit array indexed by `h1 + i*h2 mod num_bits`

```rust
pub fn add(&mut self, item: &[u8]) -> bool {
    let (h1, h2) = double_hash(item);
    let mut was_present = true;
    for i in 0..self.num_hashes {
        let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) % self.num_bits;
        if !self.get_bit(bit) { was_present = false; self.set_bit(bit); }
    }
    if !was_present { self.count += 1; true } else { false }
}
```

The classic Kirsch-Mitzenmacher trick: instead of computing `num_hashes` independent hash
functions, only two real hashes (`h1`, `h2`) are computed, and the `i`-th "hash" is derived
cheaply as `h1 + i*h2` — mathematically indistinguishable from independent hashing for Bloom
filter purposes, and far cheaper than running up to 30 real hash functions per `add`/`contains`
call. `add`'s return value (and the `count` increment) reflect whether the item was *probably
already present* (all bits already set) before this call — a real, if approximate,
already-seen signal, not just "operation succeeded."

#### 4.2 Cuckoo filter: two candidate buckets via XOR, eviction via random kicks

```rust
fn indices(&self, item: &[u8], fp: u16) -> (usize, usize) {
    let h = fnv1a_hash(item, SEED) as usize;
    let i1 = h % self.num_buckets;
    let i2 = (i1 ^ fnv1a_hash(&fp.to_le_bytes(), OTHER_SEED) as usize) % self.num_buckets;
    (i1, i2)
}
```

This is the standard partial-key cuckoo hashing trick: `i2` is derived from `i1` XORed with a
hash of the *fingerprint itself* (`alt_index`, §below), which is what makes `alt_index(alt_index(i,
fp), fp) == i` — applying the same XOR twice cancels out — so an item's alternate bucket can
always be recomputed from its current bucket and fingerprint alone, without needing to
re-hash the original item during a kick chain. The kick loop swaps a random existing
fingerprint out of a full bucket, relocates it to its own alternate bucket, and repeats up to
`MAX_KICKS = 500` times before giving up with `Err("ERR Cuckoo filter is full")` — real
cuckoo-hashing eviction, not a stub.

#### 4.3 Count-Min Sketch: `depth` independent hash rows, `min` across them as the estimate

```rust
pub fn incr_by(&mut self, item: &[u8], delta: u64) -> u64 {
    let (h1, h2) = double_hash(item);
    let mut min_val = u64::MAX;
    for r in 0..self.depth {
        let col = (h1.wrapping_add((r as u64).wrapping_mul(h2)) as usize) % self.width;
        self.table[r][col] = self.table[r][col].saturating_add(delta);
        min_val = min_val.min(self.table[r][col]);
    }
    self.total_count = self.total_count.saturating_add(delta);
    min_val
}
```

Same Kirsch-Mitzenmacher double-hash reuse as the Bloom filter (§4.1) to derive `depth`
independent-enough row hashes from two real hash computations. Taking the **minimum** across
rows after incrementing is the standard Count-Min Sketch estimator: any single row can only
*overestimate* a true count (due to hash collisions with other items sharing that row's
column), so the minimum across independent rows is the tightest available overestimate.
`CMS.INITBYPROB`'s width/depth derivation (`from_prob`) uses the textbook formulas
$w = \lceil e/\epsilon \rceil$, $d = \lceil \ln(1/(1-\delta)) \rceil$.

#### 4.4 Top-K: Space-Saving eviction of the current minimum

```rust
pub fn add(&mut self, item: Bytes, increment: u64) -> Option<Bytes> {
    if let Some(count) = self.items.get_mut(&item) { *count += increment; return None; }
    if self.items.len() < self.k { self.items.insert(item, increment); return None; }
    // find (Bytes, u64) with minimum count, evict it, insert new item with min_val + increment
}
```

Finding the minimum-count tracked item is a **linear scan over all `k` tracked items** on
every eviction (`for (k, &v) in &self.items { if v < min_val { ... } }`) — fine for the small
`k` values `TOPK.RESERVE` is realistically used with, but not the heap-based O(log k) eviction
a larger-scale Space-Saving implementation would use.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): dispatches every `BF.*`/`CF.*`/`CMS.*`/`TOPK.*`
  command through the shared `target_shard_of_cmd`/local-vs-`execute_remote` fork (§1),
  identical in shape to how `Json*`/`Geo*` commands are routed (Components 16/17).
- **`src/resp.rs`** (Component 03): parses each command family's arguments (e.g.
  `BF.RESERVE`'s error-rate/capacity, `CMS.INITBYPROB`'s error/confidence, `TOPK.RESERVE`'s
  `k`) into the matching `Command::Bf*`/`Cf*`/`Cms*`/`Topk*` variants.
- **`src/table.rs`**: no relationship — none of these four structures are `RudisValue`
  variants; they live entirely in `ShardDb.probabilistic_store`, a separate per-shard map.
- **RDB persistence: no relationship, verified** — same gap as `JsonStore` (Component 16 §5)
  and vector indexes (Component 08 §7): none of `bloom_filters`/`cuckoo_filters`/
  `cms_sketches`/`topk_trackers` are referenced anywhere in the RDB save/restore chunk logic,
  so all four structure types are lost on restart.

---

### 6. Performance Characteristics

- **Bloom/Cuckoo `add`/`contains` are O(num_hashes) / O(1)** respectively — a Bloom filter
  check costs up to 30 bit-array probes (bounded, per §2.2's clamp), a Cuckoo filter check is
  two fixed-size (4-slot) bucket scans regardless of fill level.
- **Cuckoo insertion degrades under high load factor** — the `MAX_KICKS = 500` eviction chain
  only triggers once both candidate buckets are full, and a filter approaching its rated
  capacity will trigger it increasingly often before either succeeding or returning
  `"ERR Cuckoo filter is full"` — a real, expected cuckoo-hashing characteristic, not a bug.
- **Count-Min Sketch `incr_by`/`query` are O(depth)**, independent of how many distinct items
  have been tracked — the whole point of a fixed-size sketch over an exact per-item counter map.
- **Top-K's eviction is O(k) per new distinct item once at capacity** (§4.4) — negligible at
  small `k`, would matter if `TOPK.RESERVE` were ever used with a very large `k`.

---

### 7. Future Improvements

- **Medium — include these four structures in RDB save/restore (§5)** — the same cross-cutting durability gap already flagged for `JsonStore` (Component 16) and vector indexes (Component 08): a restart silently discards all Bloom/Cuckoo/CMS/Top-K state with no warning.
- **Low — replace Top-K's O(k) linear-scan eviction with a min-heap for O(log k) (§4.4)** — only matters if `TOPK.RESERVE` is used with a large `k`; negligible at the small-k values this structure is typically used for.
- **Low — support counting Bloom filters or scalable/auto-expanding variants** (§2.5) if `BF.INSERT ... EXPANSION`-style auto-growth or deletion-capable Bloom semantics become a compatibility target — today, over-inserting a fixed-capacity Bloom filter just silently raises its real false-positive rate above the configured target with no signal to the caller.
- **Low — document the false-positive-rate implications of Cuckoo filter fingerprint collisions explicitly** (§4.2) — `contains` can return a false positive if two different items hash to the same 16-bit fingerprint in the same bucket, same as any Cuckoo filter design; not a bug, but worth stating plainly given the structure's "no false negatives" framing can otherwise be read as "always exact."

---
---

## Component 19: Pub/Sub Messaging Hub (`src/pubsub.rs`)

### 1. Architectural Purpose & Scope

`src/pubsub.rs` implements `PubSubHub`, the per-shard channel/pattern subscription registry
backing `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`PUBLISH`/`PUBSUB CHANNELS`/
`NUMSUB`/`NUMPAT`. Like `BlockHub` (Component 06), a client that issues `SUBSCRIBE` hands its
connection off to a dedicated, permanent mode-switch loop (`run_pubsub_loop` in
`connection.rs`) that never returns to ordinary command processing for the lifetime of that
TCP connection. Unlike `BlockHub`, `PubSubHub` is genuinely **per-shard** (one instance per
shard, owned by `Router.pubsub`, not a process-wide `Arc<Mutex<_>>`) — cross-shard delivery
(a publisher on shard A reaching a subscriber connected via shard B) is handled by `Router::
publish` fanning the message out to every other shard's own `PubSubHub`, not by sharing one
hub across shards.

---

### 2. Key Invariants & Concurrency Constraints

1. **Genuinely per-shard, not a `BlockHub`/`ACL`/search-registry-style global exception.**
   `Router.pubsub: Rc<RefCell<PubSubHub>>` — a plain `Rc`/`RefCell`, exactly like `ShardDb`
   itself, with no `Arc`/`Mutex` anywhere. A subscriber's registration (`channels`,
   `patterns`, `clients`, `client_channels`, `client_patterns`) only ever exists in the
   `PubSubHub` of the one shard that accepted that subscriber's connection.
2. **Cross-shard delivery is a real, parallel fan-out — send-then-await, matching the
   squashed-pipeline pattern.** `Router::publish` (Component 04) delivers to local
   subscribers immediately, then dispatches one `ShardMessage::Publish` to every *other*
   shard (all sends issued before any await), and sums each shard's returned delivery count —
   the same "dispatch all, then await all" shape used throughout the codebase for
   parallelism (Components 02/04).
3. **A hand-written glob matcher, not a regex or a crate.** `glob_match` implements `*`/`?`
   wildcard matching via an explicit backtracking scan (tracking the last `*` position and
   resuming from there on a mismatch) rather than compiling a regex or pulling in a glob
   crate — used both for `PSUBSCRIBE` pattern matching against published channels and for
   `PUBSUB CHANNELS <pattern>`'s filtering.
4. **No RESP3 push-type framing anywhere in this file — verified.** Every delivered message
   (`message`/`pmessage`) and every subscribe/unsubscribe confirmation is hard-coded RESP2
   array framing (`*3\r\n$7\r\nmessage\r\n...`); grepping this file for `is_resp3`/`resp3`
   finds zero matches. A RESP3-negotiated client (Component 02's `CURRENT_CLIENT_RESP3`
   machinery) still receives ordinary array-type frames for pub/sub messages instead of the
   RESP3 push type (`>3\r\n...`) real Redis sends once a client has opted into RESP3 — see §7.
5. **A subscriber count of zero triggers cleanup, not a lingering empty entry.** Every
   `unsubscribe`/`punsubscribe`/`unsubscribe_all`/`punsubscribe_all` path removes the
   channel/pattern's `HashSet` entirely once it's empty (not left as an empty set), and
   removes the client's `flume::Sender` from `clients` once its last subscription anywhere
   drops to zero (`total_subscriptions(client_id) == 0`) — no unbounded growth from
   subscribe/unsubscribe churn.

---

### 3. Component Architecture & Data Structures

```
Client A (shard 0)              Client B (shard 2)
   SUBSCRIBE news                  PUBLISH news "hello"
        │                                  │
        ▼                                  ▼
shard 0's PubSubHub              shard 2's Router::publish
  .channels["news"] = {A}                  │
  .clients[A] = write_tx_A          1. shard 2's own PubSubHub.publish("news", "hello")
                                        (0 local subscribers on shard 2 itself)
                                     2. ShardMessage::Publish sent to every OTHER shard,
                                        including shard 0, in parallel
                                     3. shard 0's PubSubHub.publish("news", "hello") finds A,
                                        sends the RESP frame on write_tx_A
                                     4. shard 0's dedicated pubsub writer task (run_pubsub_loop,
                                        Component 02) drains write_tx_A and writes the socket
```

### Real data structures (verbatim from `src/pubsub.rs`)

```rust
pub struct PubSubHub {
    pub channels: HashMap<Bytes, HashSet<u64>>,          // channel -> subscriber client IDs
    pub patterns: HashMap<Bytes, HashSet<u64>>,          // pattern -> subscriber client IDs
    pub clients: HashMap<u64, flume::Sender<Vec<u8>>>,   // client ID -> its delivery channel
    pub client_channels: HashMap<u64, HashSet<Bytes>>,   // reverse index: client -> its channels
    pub client_patterns: HashMap<u64, HashSet<Bytes>>,   // reverse index: client -> its patterns
}
```

Two reverse indices (`client_channels`/`client_patterns`) exist purely so
`unsubscribe_all`/`punsubscribe_all`/`total_subscriptions`/connection-drop cleanup don't have
to scan every channel/pattern in the hub looking for a given client — a real, deliberate
O(subscriptions for this client) design instead of O(all channels/patterns).

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `glob_match`: backtracking `*`/`?` matcher

```rust
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t, mut star_p, mut match_t) = (0, 0, None, 0);
    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) { p += 1; t += 1; }
        else if p < pattern.len() && pattern[p] == b'*' { star_p = Some(p); p += 1; match_t = t; }
        else if let Some(sp) = star_p { p = sp + 1; match_t += 1; t = match_t; }
        else { return false; }
    }
    while p < pattern.len() && pattern[p] == b'*' { p += 1; }
    p == pattern.len()
}
```

This is the standard greedy-with-backtracking wildcard algorithm: on a literal mismatch, if a
`*` was seen earlier, the matcher "backtracks" by advancing `match_t` (trying to let the `*`
consume one more character of `text`) and resuming pattern matching from just after that `*` —
rather than a recursive/exponential-worst-case naive implementation. `?` matches exactly one
character; there's no character-class (`[abc]`) support, matching real Redis's own
`PSUBSCRIBE` glob semantics (which also lack character classes... actually real Redis glob
*does* support `[...]` classes — this implementation's `?`/`*`-only subset is narrower than
real Redis's pattern language, worth knowing if a client relies on bracket-class patterns).

#### 4.2 `publish`: two independent passes, direct channels then pattern channels

```rust
pub fn publish(&self, channel: &[u8], message: &[u8]) -> usize {
    let mut count = 0;
    // 1. Direct channel subscribers — one frame built once, cloned per subscriber
    if let Some(subscribers) = self.channels.get(channel) {
        let frame = /* build "*3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<message>\r\n" once */;
        for client_id in subscribers {
            if let Some(tx) = self.clients.get(client_id) && tx.send(frame.clone()).is_ok() { count += 1; }
        }
    }
    // 2. Pattern subscribers — iterate EVERY registered pattern, glob-match against this channel
    for (pattern, subscribers) in &self.patterns {
        if glob_match(pattern, channel) { /* build "*4\r\n$8\r\npmessage\r\n..." once, send to each */ }
    }
    count
}
```

Direct-channel delivery is O(subscribers to this exact channel); pattern delivery is
**O(total registered patterns)**, since every pattern has to be glob-matched against the
published channel name — there's no pattern index (e.g. a trie or prefix grouping) to narrow
the set of patterns actually worth checking. `send(frame.clone())` is a `flume` channel send
(non-blocking, delivers to the subscriber's dedicated writer task — Component 02's
`run_pubsub_loop`); a `count` increment happens only if the send actually succeeds, so a
subscriber whose connection has already dropped (channel disconnected) doesn't count as
delivered even though it's still technically registered until the next cleanup path runs.

#### 4.3 Cleanup: `remove_client` is the single exit-path hook

```rust
pub fn remove_client(&mut self, client_id: u64) {
    self.unsubscribe_all(client_id);
    self.punsubscribe_all(client_id);
}
```

Called when a pub/sub-mode connection's loop exits (disconnect, `QUIT`, or a hard error) —
tears down every channel and pattern subscription for that client in one call, relying on
`unsubscribe_all`/`punsubscribe_all`'s own per-entry cleanup (§2.5) to leave no dangling
`HashSet`/`clients` entries behind.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): `run_pubsub_loop` is the sole caller of
  `subscribe`/`unsubscribe`/`psubscribe`/`punsubscribe`/`remove_client` — a permanent
  mode-switch entered on `SUBSCRIBE`/`PSUBSCRIBE`, with its own split reader/writer tasks (a
  dedicated `flume::unbounded` channel feeds a background writer task that drains published
  messages onto the socket, independent of the loop that reads new `SUBSCRIBE`/`UNSUBSCRIBE`/
  `PING`/`QUIT` commands from the client).
- **`src/router.rs`** (Component 04): `Router::publish`/`pubsub_channels`/`pubsub_numsub`/
  `pubsub_numpat` are the only entry points that reach across shards — each publishes/queries
  the local `PubSubHub` first, then fans out to every other shard's `ShardMessage::Publish`/
  equivalent and merges results (§2.2).
- **`src/shard.rs`** (Component 04): `ShardMessage::Publish { channel, message, responder }`
  is the wire message a remote shard's `PubSubHub.publish` call is delivered through.
- **`src/table.rs`**: no relationship — pub/sub state is entirely separate from `RudisTable`;
  a channel name is never a Redis key and never interacts with expiration/eviction.

---

### 6. Performance Characteristics

- **Direct-channel publish is O(subscribers to that channel)** — no overhead from unrelated
  channels or patterns.
- **Pattern publish is O(total registered patterns) per publish**, not O(matching patterns)
  (§4.2) — a deployment with many distinct active `PSUBSCRIBE` patterns pays a glob-match per
  pattern on every single `PUBLISH`, regardless of how many (if any) actually match.
- **Cross-shard fan-out cost is O(num_shards) per `PUBLISH`, done in parallel** (§2.2) — every
  publish touches every other shard's `PubSubHub` once via a `flume` message, dispatched
  concurrently rather than sequentially, matching the codebase's general cross-shard fan-out
  pattern.
- **One frame is built once and cloned per subscriber**, not re-serialized per recipient —
  the `Vec<u8>` frame construction cost is paid once regardless of subscriber count.

---

### 7. Future Improvements

- **Medium — send RESP3 push-type frames (`>`) to RESP3-negotiated subscribers instead of always RESP2 arrays (§2.4).** This is a real, verified protocol-compliance gap: a client that sent `HELLO 3` and is tracked as `is_resp3` elsewhere in the codebase (Component 02) still receives plain `*3\r\n...`/`*4\r\n...` array frames for `message`/`pmessage` delivery here, not the RESP3 push type real Redis switches to. Since one frame is currently built once and cloned to every subscriber (§4.2, a real performance benefit), fixing this requires either building two frame variants up front (RESP2 and RESP3) and picking per-subscriber based on a tracked protocol flag, or moving per-subscriber protocol awareness into `PubSubHub` itself (currently it has none).
- **Medium — index patterns to avoid an O(total patterns) scan per publish (§4.2/§6).** A simple first step: group patterns by their literal (non-wildcard) prefix so a publish only glob-matches against patterns whose prefix could plausibly match the channel, rather than every registered pattern unconditionally.
- **Low — add character-class (`[abc]`/`[a-z]`) support to `glob_match` (§4.1)** if closer compatibility with real Redis's full glob pattern language (which does support bracket classes) becomes a goal — today `?`/`*` are the only wildcard forms recognized.
- **Low — consider whether `PubSubHub` being per-shard (rather than a single process-wide hub like `BlockHub`) has any subtle ordering implications worth documenting** — e.g. two publishes to the same channel from different shards in quick succession have no cross-shard ordering guarantee relative to each other, only FIFO delivery within whichever shard's `flume` channel a given subscriber is registered on.

---
---

