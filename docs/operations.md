# Operations runbook

This page covers running Rudis in production: what to alert on, and how to handle the usual incidents. It assumes the deployment described in the README's
[Production Deployment](../README.md#production-deployment) section: systemd (`rudis.service`)
or a container, AOF on, and a supervisor that restarts the process.

## 1. What to alert on

Scrape `--metrics-port` (`GET /metrics`), and watch the log for `FATAL` and `Slow script
detected` (§4). Alert on:

| Metric | Condition | Meaning |
| :--- | :--- | :--- |
| `up` (scrape fails) / process restarts | any | Crash, OOM kill, or a dead shard thread (the process exits 1, logging `FATAL: shard N panicked`). |
| `rudis_aof_last_write_ok` | `== 0` | AOF writes are failing (disk full, I/O error); writes get `-MISCONF`. |
| `rudis_rdb_last_bgsave_ok` | `== 0` | The last snapshot failed; the previous `dump.rdb` is kept. |
| `rudis_used_memory_bytes / rudis_max_memory_bytes` | `> 0.9` | Close to `maxmemory`: eviction (or `-OOM` under `noeviction`) is near. |
| `rudis_process_resident_memory_bytes` | near the host/container limit | RSS runs above `used_memory` (fragmentation, buffers); the kernel OOM killer is near. |
| `rudis_shard_keys`, `rudis_shard_used_memory_bytes` | one shard far above the others | A hot key or a hash-tag hot spot: that shard's core limits the node. |
| `rate(rudis_mailbox_overflows_total)` | sustained `> 0` | One shard can't keep up with what the others send it. |
| `rudis_replication_master_last_io_seconds_ago` (on a replica) | `-1` or growing past a few seconds | The replica isn't following its master (`INFO replication`'s `master_link_status`). |
| `rate(rudis_isolated_panics_total)` | `> 0` | A client command panicked and was isolated; file a bug with the log line. |

## 2. Restarts and recovery

- **Graceful stop** (`SIGTERM`, `SHUTDOWN`): saves if save points are set, drains clients,
  flushes and fsyncs the AOF, exits 0. Give it at least 10 s (`TimeoutStopSec`, `docker stop -t`).
- **Crash / `SIGKILL` / dead shard**: on restart the server loads the AOF (`appendonly yes`) or
  else `dump.rdb` from `dir`. With `appendfsync everysec` up to about a second of
  acknowledged writes can be lost; with `always`, none.
- **Corrupt AOF tail** (torn last write): with `aof-load-truncated yes` (the default) the tail
  is cut and the server starts; with `no` it refuses to start and logs the offset. A corruption
  in the middle of the file always refuses: restore the AOF from a backup, or start from
  `dump.rdb` with `appendonly no`, then turn AOF back on with `CONFIG SET appendonly yes` (it
  writes an AOF holding the whole dataset; wait for `aof_rewrite_in_progress:0`) and
  `CONFIG REWRITE`.

## 3. Backups and restore

- Take a snapshot with `BGSAVE` (it never forks; shards serialize in ~1 ms steps) and copy
  `dir/dump.rdb` once `rudis_rdb_bgsave_in_progress` is back to 0. A snapshot is consistent
  per key, not a single point in time across keys (see [rdbsave.md](rdbsave.md)).
- To restore, stop the server, put the RDB at `dir/dump.rdb` and start it with `appendonly no`
  (with `appendonly yes` the AOF is loaded instead). Then `CONFIG SET appendonly yes` (and
  `CONFIG REWRITE` to keep it on).
- RDB files from Redis/Valkey load; see `tests/test_redis_rdb_compat.rs` for the versions
  covered.

## 4. A runaway script

A script runs on its shard's thread until it returns. Past `busy-reply-threshold` (default
5 s) the log shows `Slow script detected`, and commands on other shards get `-BUSY`.

1. Connect, send `SCRIPT KILL` (or `FUNCTION KILL` for an `FCALL`). If the connection lands on
   the busy shard it waits, so try again from a new connection.
2. `-UNKILLABLE` means the script already wrote: wait for it, or restart the server (`SIGTERM`
   ends the script; writes acknowledged before it are in the AOF).

## 5. Replication and failover

- Point a replica at a master with `REPLICAOF <host> <port>` (or `replicaof` in the config).
  `INFO replication` shows the link and offsets.
- **Dead links**: a replica that hears nothing from its master (no data, no PING) for
  `repl-timeout` seconds (default 60) marks the link down and reconnects, and a master drops
  a replica that hasn't acked for that long. Keep `repl-timeout` above
  `repl-ping-replica-period` (default 10), or idle links time out.
- **Planned switchover**: `FAILOVER [TO <host> <port>] [TIMEOUT <ms> [FORCE]]` on the master
  pauses writes, waits for the replica to catch up, promotes it and turns the old master into
  its replica, with no acknowledged write lost. `INFO replication`'s `master_failover_state`
  shows progress; `FAILOVER ABORT` cancels it, and without `FORCE` it aborts at the timeout.
- **Promote a replica**: `REPLICAOF NO ONE` on it, then repoint clients and the other
  replicas. There is no Sentinel: promotion is manual or done by your orchestrator.
- In cluster mode, `CLUSTER FAILOVER` on a replica switches roles without losing
  acknowledged writes. Without `FORCE`, the master must be reachable.
- **Write-heavy masters: raise the replica output buffer limit.** As in Redis, a master drops a
  replica whose output buffer passes `client-output-buffer-limit replica` (default
  `256mb 64mb 60`), and the replica then full-syncs. A multi-core rudis master can write ~1 GB/s
  (SET of 1 KB values at ~0.85M ops/s on 8 threads), so the default holds only a fraction of a
  second: a lag spike, or the writes a full sync must buffer while it runs, trips it, and the
  replica can loop through full syncs (`sync_full` and `client_output_buffer_limit_disconnections`
  in `INFO stats` climbing, "scheduled to be closed ASAP for overcoming of output buffer limits"
  in the log). In our soak test, `CONFIG SET client-output-buffer-limit "replica 2gb 1gb 60"`
  kept a replica linked through 90 s at that rate with no resync; size it to a few seconds of
  your write rate, and watch the replica's memory headroom too. For the full syncs, also turn on
  `dual-channel-replication-enabled yes` on master and replicas: the stream written during the
  sync then goes to the replica as it happens, instead of waiting in the master's buffer (see
  [`replication.md`](replication.md) §2.2.1).

## 6. Cluster resharding

`CLUSTER SETSLOT`, `CLUSTER REBALANCE` and `CLUSTER RESHARD` move keys between nodes
(sequentially, one slot at a time, in batches of 100 keys). Clients that follow `-ASK`/`-MOVED`
keep working during the move. Gossip resends the full node table on each tick, so it suits
clusters of up to a few dozen nodes ([design doc](design/11_cluster_topology.md)).

## 7. Upgrades and rollback

1. Check `rudis --version` and `INFO server` (`rudis_version`, `redis_git_sha1`) on the old
   and new builds.
2. Upgrade replicas first, check `INFO replication`, then fail over and upgrade the old master.
3. To roll back, keep the previous binary and the last `dump.rdb`/AOF from before the upgrade.
   Rudis writes Redis-format RDB and AOF, so an older build can load what a newer one wrote,
   except for keys of types the older build doesn't know.
