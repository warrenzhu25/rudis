# RDB Snapshotting

This document describes how Rudis persists a point-in-time snapshot of the keyspace to an
RDB-format file: what triggers a save, how the data is serialized, how the operation interacts
with the thread-per-core shard model, the on-disk file format, and the configuration and commands
that control it.

Every claim below has been verified against the current implementation, which lives primarily in
`src/router.rs` (save/load orchestration), `src/table.rs` (record serialization/deserialization),
`src/shard.rs` (per-shard extended-type serialization), and `src/server.rs` (startup restore).
There is no dedicated `src/rdb.rs` module. Where the implementation diverges from what a reader
familiar with real Redis might expect, that is called out explicitly.

---

## 1. Summary: What Rudis Actually Does

- **No `fork()`.** Rudis's `BGSAVE` does not call `fork()` and does not rely on Linux
  copy-on-write page semantics for snapshot isolation. A grep for `libc::fork`/`fn fork` across
  `src/` returns zero results. This is a real architectural difference from stock Redis, not
  merely a re-description of it.
- **File I/O on a dedicated `rdb-writer` thread, not `io_uring`.** `Router::perform_save_rdb`
  (`src/router.rs`) spawns an `rdb-writer` thread that writes the header, appends every shard's
  serialized pieces, computes the CRC64, then `sync_all`s, renames and fsyncs the directory,
  using ordinary blocking `std::fs`/`std::io::Write`. No shard thread does file I/O or the CRC
  for a save. (The AOF uses `monoio`'s async `write_all_at`/`sync_data` instead.)
- **No true isolation from concurrent writes.** There is no fork and no MVCC, so a save is a
  scan of each shard's live table while that shard keeps serving clients. Each key is written as
  it is when the scan reaches it: the snapshot is consistent per key, not a single point in time
  (closer to a fuzzy dump).
- **Shards stream one after another.** The coordinating shard asks each shard in turn
  (`ShardMessage::StreamRdbChunk`) to serialize its keyspace and send it to the writer in
  ~4 MB pieces through a rendezvous channel, so the file layout is one shard after another and
  memory stays at a few pieces rather than a whole shard.
- **Serialization yields every ~1 ms.** `save_rdb_chunk_yielding` (`src/shard.rs`) serializes
  a few table segments at a time and yields to the shard's other tasks between steps, so a
  `BGSAVE` adds latency in ~1 ms slices instead of stalling a shard for its whole dataset. A
  key moved by a segment split mid-save can be written twice; loading keeps the later copy.
  Replica full syncs still serialize each shard in one non-yielding pass
  (`ShardDb::save_rdb_chunk`), because the replication stream is cut right after serializing.
- **NVMe-tiered keys are saved with their real value and type.** A `RudisValue::Tiered` entry
  is read back from the tier file (CRC- and type-checked) and its payload is written verbatim,
  before anything else for that key, so a failed read skips the key cleanly (logged) instead of
  corrupting the file. Tiering state itself is not persisted: the key loads as an ordinary
  in-memory key. `BGREWRITEAOF` decodes tiered values the same way.
- **CRC64 checksum is a plain bitwise loop, not SIMD.** `crc64`/`crc64_update` in `src/table.rs`
  are a textbook bit-at-a-time CRC-64/XZ implementation (poly `0x42F0E1EBA9EA3693`). There is no
  vectorized/SIMD CRC64 anywhere in the codebase.
- **Reflink (`FICLONE`) snapshotting is real — but it belongs to a different subsystem.**
  `ioctl(fd, FICLONE, ...)`-based reflink cloning does exist in `src/tiering.rs` and is real,
  working code — but it snapshots the NVMe tiered-storage backing file, reachable via the
  `TIER.SNAPSHOT` command family, not the in-memory keyspace RDB file. `SAVE`/`BGSAVE` never call
  into `tiering.rs` and never use `FICLONE`. Documentation or tooling that describes RDB
  snapshotting as reflink-based is describing a different, unrelated feature.
- **Transparent Huge Page disablement is real, general, and not RDB-specific.** `src/main.rs`
  calls `libc::prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0)` once at process startup, for the whole
  process. It reduces general copy-on-write amplification risk but has no special interaction
  with RDB save specifically (there being no fork/CoW involved in RDB save to begin with).

No specific memory (RSS), latency, or throughput benchmark figures are given in this document
for the RDB save path — none have been independently measured for this rewrite, and prior
figures in this file were not traceable to any benchmark artifact in the repository.

---

## 2. What Triggers a Save

| Trigger | Command / mechanism | Blocking? |
| :--- | :--- | :--- |
| Client request | `SAVE` | Yes — blocks the issuing connection until the whole multi-shard save (all shards) completes. |
| Client request | `BGSAVE` | No — returns `+Background saving started` immediately; the save runs in a `monoio::spawn`-ed task on the same core as the connection that issued it. |
| Replica initial sync | `PSYNC`/full resync | The master serializes a full in-memory RDB image (`Router::generate_full_rdb`) and streams it as the bulk payload following a `+FULLRESYNC <replid> <offset>\r\n$<len>\r\n` header. This reuses the same per-shard chunk serialization as `SAVE`/`BGSAVE`, but builds the entire image in memory and never writes it to disk. |
| Automatic/periodic | `save <seconds> <changes>` | No — shard 0 checks the configured save points every 100 ms (`run_shard_worker`, `src/server.rs`) and runs a `BGSAVE` when one is due, like Redis's `serverCron`. There are no default save points; with none configured nothing is saved automatically. Save points also make `SHUTDOWN`/`SIGTERM` save before exiting. |
| Server startup | `dump.rdb` restore | If AOF persistence is **disabled**, each shard independently reads and parses the entire `dump.rdb` file from its data directory on startup (`crate::table::load_rdb`, called from `src/server.rs`) and keeps only the keys that hash to itself, discarding the rest. If AOF is enabled, AOF is authoritative and RDB restore is skipped entirely (matching the same AOF-precedence convention as Redis). |

`is_saving: AtomicBool` on `Router` guards both `SAVE` and `BGSAVE` (and `BGREWRITEAOF`, which
shares the same flag) with a compare-and-swap — a `SAVE`/`BGSAVE`/`BGREWRITEAOF` issued while one
is already in flight is rejected with an error rather than queued.

`LASTSAVE` returns the real Unix timestamp of the most recent successful save
(`Router::lastsave`, backed by an `AtomicU64` updated at the end of `perform_save_rdb`). Note,
however, that `INFO`'s `# Persistence` section hardcodes `rdb_bgsave_in_progress:0` and
`rdb_last_save_time:0` regardless of actual state — those two fields do not reflect reality; only
the dedicated `LASTSAVE` command does. `rdb_changes_since_last_save` in the same `INFO` section is
backed by a real global dirty-write counter.

---

## 3. The Save Algorithm (`Router::perform_save_rdb`, `src/router.rs`)

1. A uniquely named temporary file is created in the configured data directory:
   `dump.rdb.tmp.<pid>_<counter>` (the counter is a process-wide `AtomicU64`, so concurrent
   attempts — blocked in practice by `is_saving` — would not collide).
2. The 9-byte header `b"REDIS0011"` followed by a `SELECTDB` opcode pair (`0xFE, 0x00`) is
   written, and a running CRC64 accumulator is seeded from those bytes.
3. The writer side runs on an `rdb-writer` thread; the coordinating shard asks each shard, in
   shard-index order, to stream its keyspace (`ShardMessage::StreamRdbChunk`). Each shard runs
   `save_rdb_chunk_yielding`: it serializes ~1 ms worth of table segments at a time (yielding to
   its clients in between) and sends ~4 MB pieces to the writer through a rendezvous channel.
4. The writer folds every piece into the CRC64 and writes it with a buffered writer.
5. A single `0xFF` EOF opcode and the final 8-byte little-endian CRC64 are appended.
6. `file.sync_all()` fsyncs the temp file's data and metadata.
7. `std::fs::rename(tmp, "dump.rdb")` atomically replaces the previous snapshot.
8. The parent directory is explicitly opened and `fsync`'d (`crate::aof::sync_parent_dir`) so the
   rename itself is crash-consistent — this matches the directory-fsync durability work applied
   to both RDB and AOF persistence in this codebase.
9. `last_save_time` is updated and the `is_saving` flag is cleared.

`ShardDb::save_rdb_chunk` itself does two things, not one:
- `self.table.save_rdb_chunk(buf)` — walks the shard's main hash table (see §4 for the record
  format) covering strings/ints, lists, sets, sorted sets, hashes, HyperLogLogs, and streams.
- `self.save_extended_rdb_chunk(buf)` — additionally serializes, into the **same** chunk stream
  using their own type tags: JSON documents (`json_store`), Bloom filters, Cuckoo filters,
  Count-Min sketches, Top-K trackers (all from `probabilistic_store`), HNSW vector index entries
  (`vector_indexes`), and a CRDT sync payload (`crdt_store.export_sync_payload()`, written under
  a synthetic key `__rudis_crdt_sync__`).

So an RDB file produced by `SAVE`/`BGSAVE` is not limited to the classic Redis value types — it
also round-trips Rudis's JSON store, probabilistic structures, vector indexes, and CRDT state.

### 3.1 Tiered (spilled) values

`RudisValue` has two variants used by the NVMe tiered-storage subsystem (Component 07):
`Tiered(TieredPointer)` (value spilled to disk, not resident) and `Cooled { ptr, val }` (resident
but tracked for eviction). `Cooled` is saved from its in-memory `val`. For `Tiered`,
`ShardDb::save_rdb_entry` reads the record back from the tier file (`read_ptr_sync`, which checks
the record's CRC and header type) and, since the record already holds the value in
`serialize_val_payload` form, writes that payload verbatim after checking its type tag. The read
happens before the expiry opcode or key is written, so a failed read skips the key (logged as an
error) without corrupting the rest of the chunk. Earlier versions wrote nothing for `Tiered`
(desynchronizing the reader) and later re-encoded the payload as a string (losing the value's type).

---

## 4. On-Disk Format

The file begins with the literal 9 bytes `REDIS0011` (a fixed, hardcoded version string — the
trailing `0011` is not derived from any format-negotiation logic) followed by a `SELECTDB`
marker pair (`0xFE`, then a single DB-number byte, currently always `0x00` — Rudis does not
implement multiple numbered logical databases the way Redis's `SELECT` does). The body is a flat
sequence of records; the file ends with `0xFF` (EOF marker) followed by an 8-byte little-endian
CRC64 of every preceding byte in the file.

Each record is:

```
[0xFC <8-byte LE unix-ms expiry>]?   -- optional, only present if the key has a TTL
<4-byte LE key length> <key bytes>
<1-byte type tag> <type-specific payload>
```

Type tags (from `RudisTable::serialize_val_payload` / `ShardDb::save_extended_rdb_chunk`):

| Tag | Value type | Payload shape (all lengths 4-byte LE `u32` unless noted) |
| :-: | :--- | :--- |
| 0 | String / Int | length-prefixed bytes (`Int` is re-encoded as its decimal-string form) |
| 1 | List | count, then that many length-prefixed elements |
| 2 | Set | count, then that many length-prefixed members |
| 3 | Sorted Set | count, then (length-prefixed member, 8-byte LE `f64` bits) pairs |
| 4 | Hash | count, then (length-prefixed field, length-prefixed value) pairs |
| 5 | HyperLogLog | fixed 16384 raw dense-register bytes |
| 6 | Stream | entry count, last-ID (ms, seq as two 8-byte LE `u64`), then per entry: ID (ms, seq) + field count + (field, value) pairs |
| 7 | JSON document | length-prefixed UTF-8 JSON text (`serde_json::Value::to_string()`) |
| 8 | Bloom filter | capacity, error-rate bits, num_bits, num_hashes, count, then the raw `u64` bit-array words |
| 9 | Vector index entry | index name, member key, metric byte, dimension count, then `f32` bits per dimension |
| 10 | Cuckoo filter | capacity, num_buckets, count, then raw bucket fingerprint arrays |
| 11 | Count-Min sketch | width, depth, total_count, then the raw `u64` cell table |
| 12 | Top-K tracker | k, item count, then (length-prefixed item, 8-byte LE count) pairs |
| 13 | CRDT sync payload | length-prefixed opaque bytes from `CrdtStore::export_sync_payload()`, keyed under the literal key `__rudis_crdt_sync__` |

This is a **Rudis-specific binary layout inspired by, but not compatible with, real Redis's RDB
format** (no length-encoding varints, no per-type Redis object encodings like ziplist/listpack,
no Redis RDB opcode set beyond reusing `0xFC`/`0xFE`/`0xFF` as familiar-looking markers). A file
produced by Rudis cannot be loaded by real Redis or vice versa. `DUMP`/`RESTORE` (single-key
serialization, used for `MIGRATE`-style key transfer) reuse the same `serialize_val_payload`/
`deserialize_val_payload` pair, wrapped with an additional 2-byte format-version field (currently
hardcoded to `10`, rejecting anything `> 15` on restore) and their own CRC64 trailer — this is
conceptually parallel to, but a separate code path from, whole-file `SAVE`/`BGSAVE`.

The production load path for all of the above — both the on-disk `dump.rdb` at startup and the
in-memory chunks exchanged between shards or over replication — is the free function
`crate::table::load_rdb_bytes` (and its file-reading wrapper `load_rdb`), which switches on every
tag 0–13 inline. (`RudisTable::restore_rdb_chunk`, a separate, narrower method covering only tags
0–6, exists in `src/table.rs` but is not called anywhere in the current codebase — it is dead
code, not the path actually exercised by `SAVE`/`BGSAVE`/startup/replication. A third variant,
`ShardDb::restore_rdb_chunk` in `src/shard.rs`, mirrors the full 0–13 tag set but is currently
exercised only by that file's own unit test, not by production save/restore.)

Both load paths honor expiry: a `0xFC`-prefixed key whose stored expiry timestamp is already in
the past is parsed (to keep the cursor correctly positioned) and then discarded rather than
inserted. On multi-shard loads (`load_rdb_bytes`, `ShardMessage::RestoreRdbChunk`), each shard
independently re-parses the same full byte stream and keeps only the entries whose key hashes to
itself (`crate::router::target_shard(&key, num_shards) == shard_id`) — every shard does the full
`O(total keys)` parse, not just its own share, which is simple and correct but not
work-proportional to shard size.

---

## 5. Relationship to Replication

A replica's initial full resync (`PSYNC`, handled in `src/connection.rs`) does not read
`dump.rdb` from disk at all. It calls `Router::generate_full_rdb`, which runs the exact same
per-shard, one-at-a-time chunk collection as `perform_save_rdb` (§3, steps 3–5) but accumulates
the result entirely in an in-memory `Vec<u8>` rather than writing to a file, then sends it as the
bulk payload following the `+FULLRESYNC <replid> <offset>\r\n` header. The replica
(`src/replication.rs`) reads the declared length, buffers the full payload, and hands it to
`Router::restore_rdb_bytes`, which is a thin wrapper over the same `load_rdb_bytes` used for
startup and cross-shard restore. Partial resync (`+CONTINUE`), when eligible, sends a replication
backlog diff instead and does not involve RDB serialization at all.

---

## 6. Relevant Configuration

| Directive / setting | Effect |
| :--- | :--- |
| `dir` | Data directory; `dump.rdb` (and its `dump.rdb.tmp.*` staging files) are always written directly under it. |
| `appendonly` | If `yes`, RDB restore on startup is skipped in favor of AOF replay (§2). Does not disable `SAVE`/`BGSAVE` themselves. |
| `save <seconds> <changes>` | Parsed and stored, but **not acted upon** — see §2. Present for `rudis.conf` compatibility with Redis-style configs, not a functioning save-point scheduler today. |

There is no `dbfilename` directive — the snapshot filename is hardcoded to `dump.rdb` in
`src/router.rs` and `src/server.rs`; it cannot be renamed via configuration.

---

## 7. Commands

| Command | Behavior |
| :--- | :--- |
| `SAVE` | Synchronous full save; blocks the issuing connection until complete. Returns `+OK` or an error (e.g. if a save is already in progress). |
| `BGSAVE` | Spawns the save as a separate `monoio` task on the same core; returns `+Background saving started` immediately. Note the thread-per-core caveat in §1 — it is "background" relative to the client, not relative to other connections sharing that shard's core. |
| `BGREWRITEAOF` | Not an RDB operation — triggers AOF compaction (`Router::perform_rewrite_aof`), sharing the same `is_saving` guard flag as `SAVE`/`BGSAVE`, so the two cannot run concurrently. |
| `LASTSAVE` | Returns the real Unix timestamp of the last successful save. |
| `DUMP` / `RESTORE` | Single-key serialize/deserialize using the same record payload format as whole-file save (§4), independent of `SAVE`/`BGSAVE`. |
