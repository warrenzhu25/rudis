# Must Read Before Modifying

Read the listed documents before editing these areas. They record decisions
and traps that aren't visible from the code.

| Area | Read first |
| :--- | :--- |
| Replication (`src/replication.rs`, the replica paths in `src/connection/replica_sync.rs`) | `docs/replication.md`, `docs/differences.md` §3 (Replication, Full-Sync Loading), `.agents/rules/persistence-replication.md` |
| RDB / AOF (`src/redis_rdb.rs`, `src/aof.rs`, `src/snapshot.rs`, `AofImage` in `src/shard.rs`) | `docs/rdbsave.md`, `.agents/rules/persistence-replication.md` |
| Atomic slot migration and trimming (`src/asm.rs`) | `docs/differences.md` §Atomic Slot Migration, `.agents/rules/never-stall-a-shard.md` |
| Cluster bus and topology (`src/cluster.rs`) | `docs/design/11_cluster_topology.md` |
| Cross-shard messaging (`src/mailbox.rs`, `ShardMessage`) | `docs/internal/04_sharding_mesh.md` (§6 reply mechanisms) |
| Command dispatch and parsing (`src/connection/`, `src/resp.rs`) | `.agents/rules/hot-path.md`, `.agents/rules/redis-compat.md` |
| Memory accounting and eviction (`src/router.rs` memory state, `src/allocator.rs`, `src/tiering.rs`) | `docs/operations.md` (memory sizing), agent.md §1.4 |
| COMMAND / ACL tables (`src/acl_categories.rs`, `src/command_docs.rs`) | Generated: edit `scripts/gen_acl_categories.py` / `scripts/gen_command_docs.py` and regenerate, never by hand |
| TCL skip lists and patches | `.agents/rules/test-admission.md` §Redis TCL suites |
| CI scripts (`scripts/ci_local.sh`, `.github/workflows/`) | agent.md Rule 4. Never edit `ci_local.sh` while it runs, or `soak_continuous.sh` while the soak runs |
