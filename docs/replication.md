# Replication

This document describes how Rudis replicates data from a master to one or more replicas: the wire protocols involved, how a replica catches up after a restart or a brief disconnect, and the operational commands and `INFO` fields used to observe replication state. It is written for operators and client authors; for struct-level detail and source line references, see [`docs/internal/14_persistence_replication.md`](internal/14_persistence_replication.md).

Rudis implements **two** independent protocols on the master side:

1. **Standard Redis-family replication (`PSYNC`)** — a single TCP connection per replica, wire-compatible with Redis/Valkey replicas and with rudis's own replica implementation. This is the protocol rudis uses when replicating against another rudis node, or against a real Redis/Valkey instance.
2. **Dragonfly-compatible parallel flow replication (`DFLY FLOW`)** — one TCP connection per shard, reachable only by a client that performs the Dragonfly-specific handshake itself. **Rudis's own replica implementation never uses this path.** It exists on the master side for interoperability with Dragonfly-protocol-aware clients; there is currently no rudis-to-rudis mode that uses it.

Both protocols share the same underlying replication state (`ReplicationHub`, one instance per listening port) and the same write-propagation call site (`record_mutation`), so a master can serve `PSYNC` replicas and `DFLY FLOW` sessions at the same time.

---

## 1. Starting and Stopping Replication

Replication is controlled at runtime with the `REPLICAOF` (alias `SLAVEOF`) command — there is currently no `replicaof <host> <port>` directive in the config file format (`src/config.rs` has no such key); a replica relationship must be established either via the command line after startup or by an external supervisor issuing the command.

```
REPLICAOF <host> <port>   # start replicating from <host>:<port>
REPLICAOF NO ONE          # stop replicating and become a master (promotes in place)
```

`REPLICAOF NO ONE` calls `ReplicationHub::make_master`, which generates a fresh `replid` for this node while remembering the old master's replid as `replid2` (with `second_offset` set to the offset at the moment of promotion). This lets a *former* replica of this node — one that was tracking the old master's replid — still qualify for a partial resync against the newly promoted node for writes up to `second_offset`, instead of unconditionally forcing a full resync after every promotion.

There is no `masterauth`/`requirepass`-gated replication link today — a replica connects to a master without any credential exchange in the replication handshake itself.

---

## 2. The `PSYNC` Protocol (Standard Replication)

### 2.1 Handshake

A replica (`run_replica_worker` in `src/replication.rs`) connects to the master's client port and performs, in order:

1. `PING` → expects `+PONG`
2. `REPLCONF listening-port <port>` → expects `+OK`
3. `REPLCONF capa psync2` → expects `+OK`
4. `PSYNC <replid> <offset>` → expects `+FULLRESYNC <replid> <offset>` or `+CONTINUE <replid>`

On a replica's **first** connection to a given master (or after any change of target host/port), it has no cached state and sends `PSYNC ? -1`, which always yields `+FULLRESYNC`. On a **reconnect** to the same master (e.g. after a network blip), the replica worker sends `PSYNC <cached_replid> <cached_offset>` using the replid and offset it last observed from that master — a real attempt at a partial resync, not a placeholder.

### 2.2 Full resync (`+FULLRESYNC`)

The master fans a request out to every shard (`Router::generate_full_rdb`), collecting each shard's serialized keyspace chunk **one shard at a time** — only one shard's chunk is ever held in memory at once — and concatenates them into a single blob framed the way an RDB file is (`"REDIS0011"` header, per-key records, `0xFF` end marker, CRC64 footer; see [`docs/rdbsave.md`](rdbsave.md) for the exact format). It replies:

```
+FULLRESYNC <replid> <offset>\r\n$<len>\r\n<rdb-bytes>
```

The replica reads exactly `<len>` bytes and applies them via `Router::restore_rdb_bytes`, which — like RDB startup restore — has every shard independently filter the full blob down to the keys it owns.

### 2.3 Partial resync (`+CONTINUE`)

If the requested replid matches the master's current replid (or its immediately-previous `replid2`, within the bound described in §1) **and** the requested offset still falls inside the master's retained *replication backlog* — a fixed 1MB, in-memory ring buffer of the most recently propagated bytes, per listening port — the master replies:

```
+CONTINUE <replid>\r\n<only the missing bytes>
```

and the replica applies just that diff instead of receiving the dataset again. If the offset has already fallen out of the backlog's retained window (the master accepted more than ~1MB of write traffic while the replica was away), the master falls back to `+FULLRESYNC`. The 1MB backlog size is currently a compile-time constant, not configurable.

The backlog is retained even while zero replicas are connected, specifically so that a replica which disconnects and later reconnects has something to diff against; it is not populated only while a replica happens to be live.

### 2.4 Steady-state streaming and acknowledgment

After either sync path, the master registers the connection and streams every subsequent propagated write to it live, in the same canonical RESP encoding used for AOF (§3 below covers this shared encoding). The replica applies each command through the normal command-execution path (`Router::execute_replica_command`), which routes it to whichever shard owns the affected key(s) rather than assuming the replica's shard layout mirrors the master's.

The replica periodically (in response to the master's `REPLCONF GETACK`) sends `REPLCONF ACK <offset>` back to the master, which the master records per-replica (`ConnectedReplica::ack_offset`, `last_ack_time`) and exposes via `INFO replication` / `ROLE` (§4).

---

## 3. What Gets Replicated

Every mutating command that has been applied locally is re-encoded once, deterministically, into canonical RESP by `command_to_resp` (`src/aof.rs`) — the same function used to write the Append-Only File (see [`docs/internal/14_persistence_replication.md`](internal/14_persistence_replication.md) §3.1 for the exhaustive list of covered commands). A command without an explicit encoding there is **not** replicated (and not persisted to AOF) at all; there is no generic fallback serialization. This is why, for example, a non-deterministic command is never propagated verbatim — commands with a random or time-dependent outcome are re-encoded into their deterministic effect (e.g. `SPOP` propagates as the specific member removed, `EXPIRE` propagates as `PEXPIRE` with an absolute remaining duration) by virtue of going through this same re-serialization step rather than being forwarded as originally typed.

Replication and AOF persistence are independent: a node can run with replication active and AOF disabled, or vice versa, or both, or neither.

---

## 4. Observing Replication State

### 4.1 `INFO replication`

On a **master**, `INFO replication` reports (per listening port): `role:master`, `connected_slaves` (count of `PSYNC` sessions — **`DFLY FLOW` sessions are not currently counted here**), `master_replid`, `master_replid2`, `master_repl_offset`, `second_repl_offset`, `repl_backlog_active`, `repl_backlog_size` (the 1MB constant), `repl_backlog_first_byte_offset`, and `repl_backlog_histlen` (bytes currently retained in the ring buffer). These fields are computed fresh on every call from live atomics and lock reads — there is no periodic snapshot.

On a **replica**, it reports `role:slave`, `master_host`, `master_port`, `master_link_status` (`up` or `down` — there is no distinct in-between state exposed here beyond what `ROLE` calls `"connecting"` internally), `master_sync_in_progress`, `slave_repl_offset`, `slave_priority` (fixed at 100), `slave_read_only` (fixed at 1), and its own `master_replid`/`master_repl_offset`.

### 4.2 `ROLE`

Returns the same role/offset information in the RESP array format Redis clients expect: `["master", <offset>, [[<ip>, <port>, <ack_offset>], ...]]` for a master (replica IP is always reported as `127.0.0.1` — the master does not track a replica's real source address here, only the port it announced via `REPLCONF listening-port`), or `["slave", <master_host>, <master_port>, <state>, <offset>]` for a replica.

---

## 5. The Dragonfly-Compatible `DFLY FLOW` Path

This section describes a **master-side-only** capability. It is documented here for completeness and for anyone building a client against it; it is not part of how two rudis nodes replicate with each other.

A client that sends `REPLCONF capa dragonfly` (instead of, or in addition to, `capa psync2`) receives a five-element multibulk reply instead of a plain `+OK`:

```
*5
$<len of replid>
<master_replid>
$5
SYNC1
:<num_shards>
:1
:0
```

Two details worth being precise about, since they differ from what a Dragonfly-familiar reader might expect: the fourth line, `"SYNC1"`, is a **hardcoded literal string** on the current master implementation — it is not a freshly generated, unique-per-session token, and every client that performs this handshake against the same master sees the same value. The final line is a plain integer `0`, not a 40-character lineage-id string.

The client is then expected to open **one TCP connection per shard** and, on each, send:

```
DFLY FLOW <master_replid> <sync_id> <shard_id> [<lsn>]
```

The master accepts `master_replid` and `sync_id` on the wire but does not currently validate either against the values it handed out during the greeting — only `shard_id` (which selects which shard's data this flow serves) and the optional `lsn` (accepted but not currently used to serve a partial resync for that flow; every flow receives a full RDB chunk for its shard regardless of the `lsn` presented) affect behavior. The master replies:

```
+OK FLOW <shard_id>\r\n$<len>\r\n<this shard's RDB chunk>\r\n
```

and then streams that shard's live mutation stream over the same connection, tagged internally with a per-flow, monotonically increasing byte counter (`ShardReplicaFlow::lsn`) rather than a separate log format. The connection also accepts `REPLCONF ACK <offset>` lines from the client to update that flow's acknowledged offset.

Because each shard streams independently and in parallel over its own socket, this path avoids funneling every core's writes through one TCP connection the way the single-socket `PSYNC` path does — the potential benefit of `DFLY FLOW` over `PSYNC` for a client that speaks it. No throughput numbers are claimed here; none have been independently measured for this document.

---

## 6. Known Limitations

- The 1MB replication backlog size and the AOF/backlog-adjacent flush cadence are compile-time constants today, not runtime-configurable.
- `DFLY FLOW` sessions do not appear in `INFO replication`'s `connected_slaves` or in `ROLE`'s replica list.
- `DFLY FLOW`'s `lsn` resume parameter is accepted but not wired up to a partial per-flow resync.
- There is no authentication on the replication link itself (no `masterauth`-equivalent).
- Rudis's own replica implementation only ever speaks `PSYNC`; there is no rudis-to-rudis `DFLY FLOW` mode.
