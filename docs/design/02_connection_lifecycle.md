# Component 02: Connection Lifecycle & Command Execution (Design)

> **Source Files**: `src/connection.rs`


---

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

### 3. Performance Characteristics

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
