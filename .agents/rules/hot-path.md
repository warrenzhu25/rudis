---
paths:
  - "src/connection.rs"
  - "src/router.rs"
  - "src/mailbox.rs"
  - "src/shard.rs"
  - "src/table.rs"
  - "src/resp.rs"
---

# Hot Path

agent.md §1 lists the invariants (shared-nothing, no locks on per-key data,
pooled reply descriptors). In review, check these concretely:

- **An optional feature costs one relaxed atomic load when it's off.** Gate it
  like `slot_stats::accounting()` and `hotkeys::active()`, and keep the work
  behind the gate. Don't add a lock, map lookup or allocation that runs for
  every command.
- **No per-call channels or `Vec`s** on GET/SET/MGET/MSET-class paths. Reuse
  the pooled descriptors in `src/mailbox.rs`, or thread-local scratch.
- **No new process-wide lock** reachable from a data command. Control-plane
  globals (`RwLock` + atomics) are for admin and observability state only
  (agent.md §1.2).
- **Byte-at-a-time loops over large data** (checksums, scans, copies) get
  looked at. CRC64 byte-at-a-time was half of a migration's dump time.
- **Measure.** Any `src/` change gets the perf A/B from agent.md Rule 4
  (`scripts/check_perf_regression.py`). Report the deltas and the run-to-run
  spread, not just a single number. On a noisy host, compare server CPU time
  for a fixed workload (pipeline 1 and 16, interleaved rounds) instead of
  throughput.

## File size

`connection.rs` (~37k lines) and `table.rs` (~22k) are far past what anyone
can review. Don't grow them with new subsystems:
- a new feature gets its own module (`src/hotkeys.rs`, `src/asm.rs`,
  `src/slot_stats.rs`) and only a thin hook in the dispatcher;
- a function that grows past ~100 lines gets split into named helpers, with
  the top level reading like the algorithm.
