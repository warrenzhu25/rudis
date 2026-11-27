---
paths:
  - "src/aof.rs"
  - "src/redis_rdb.rs"
  - "src/replication.rs"
  - "src/asm.rs"
  - "src/snapshot.rs"
  - "src/shard.rs"
---

# Persistence, Replication and Slot Migration

These paths decide whether acknowledged data survives. They read untrusted
bytes (RDB/AOF files, the replication stream, RESTORE payloads), and their
failures show up hours later in a soak, not in a unit test.

## Rules

- **Readers of untrusted bytes are total.** Every `Command` arm in AOF
  replay, every RDB/DUMP decoder path, and every stream parser returns an
  error on bad input. No `unwrap`, no unchecked indexing, no allocation sized
  by an untrusted length (the fuzz targets run with
  `-malloc_limit_mb=2048`). A new replay arm gets a fuzz seed in
  `fuzz/corpus/load_aof`.
- **Snapshot and stream stay consistent.** Whatever arms a cut (a full
  sync's `FullSyncCut`, a migration's `arm_migration`, the AOF image) and
  whatever serializes the keys happen at one point per shard. A write after
  the cut goes to the stream exactly once and never into the snapshot too.
  Check that the bytes counted (CRC, offsets, output limits) are the bytes
  sent.
- **Limits apply to what Redis limits.** The replica output buffer limit
  covers the stream, not a full-sync or migration snapshot (Redis sends
  those on a channel of its own). Test anything with a limit at more than the
  limit: a snapshot over 256 MB.
- **Retries must be able to succeed.** A failure that's deterministic for a
  given dataset (a limit, a CRC mismatch) must not loop forever, re-dumping
  every second. Log it once with the cause.
- **A failed load keeps or drops data the way Redis does** (swapdb keeps the
  old dataset; disk-based loads leave it empty), and says which in the log.

## Before you push (beyond agent.md Rule 4)

- `integration/replication`, `replication-psync`, `psync2*`, `failover` and
  `unit/cluster/atomic-slot-migration` TCL suites (all in `scripts/ci_local.sh`
  lanes).
- For changes to sync images, the RDB writer or migration: one full sync and
  one migration at **≥ 0.5 GB** with writes going on (memtier or a write
  loop), checking DEBUG DIGEST on both sides.
- Let the next soak segment (`~/soak-runs/continuous/summary.log`) run on
  the change before calling it done. Zero `lost`, `replicas_same=True`.
