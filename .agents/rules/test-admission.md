---
paths:
  - "src/**/*.rs"
  - "tests/**/*.rs"
  - "scripts/tcl_skips/**"
  - "scripts/tcl_patches/**"
---

# Test Admission

Every test must answer: **"what future diff would turn this red?"** If the
only answer is "editing the test itself", don't add it.

## What a new test must be

1. **A bug regression.** It guards a bug that actually happened (CI failure,
   soak anomaly, TCL failure, fuzz input). **Run it on the pre-fix code and
   see it fail** before committing the fix. The doc comment describes the
   bug as black-box behavior.
   Example: `test_migration_snapshot_is_left_out_of_the_output_limit` fails
   without the exemption.
2. **A derived property.** It pins down something that needed reasoning to
   establish: wire formats and round-trips (RDB/AOF/DUMP), CRC against a known
   check value, slot/shard math, ordering guarantees (FIFO replies, stream
   order across shards). This guards against "looks equivalent" rewrites.
3. **Bookkeeping completeness.** "Someone added X without updating Y":
   every write command is propagated (`test_every_write_command_is_propagated`),
   every config round-trips through CONFIG GET, every command has an ACL entry.

Not admissible:
- tautologies and tests that restate the implementation as assertions;
- stress loops that can't reproduce the failure even on the buggy code.

## Concurrency and timing

- **Reproduce the interleaving deterministically** (channels, explicit
  `yield`, a held lock) instead of hoping a loop hits it. A test that passes
  on the buggy code has no value as a guard.
- **No bare timing thresholds.** Use the existing `latency_bound(...)`
  helpers, or assert on state you can poll. A test that times work must keep
  enough margin to survive a faster implementation too. The AOF rewrite test
  broke when CRC64 got 2x faster: it needed more data, not a looser assert.
- **Unique resources per test.** Every unit test that touches process-wide
  state (replication hubs, AOF progress, cluster hubs, by port) uses a port
  no other test uses: grep for it first. Two tests sharing port 19962 made
  one flaky for a day.

## Scale

A test with 100 keys can't catch an O(slots × keys) loop or a 256 MB buffer
limit. For anything that walks a table, sends a snapshot or holds a buffer,
add a check at realistic size, at least in a manual or nightly script, or
measure it once and record the numbers in the commit message.
Today's examples: the slot trim (a million keys) and the migration snapshot
(more than 256 MB).

## Redis TCL suites

- A test goes into `scripts/tcl_skips/<suite>.txt` only **with a comment
  giving the reason**, grouped under one of:
  - `N/A`: a fork/pipe/Redis-internal mechanism rudis doesn't have;
  - a documented difference (with a link to `docs/differences.md`);
  - "Not yet implemented" (with a pointer to the gap list);
  - timing (say why the timing can't hold for rudis).
- A sed patch in `scripts/tcl_patches/` changes test *isolation or timing*,
  never what is asserted, and its header says why.
- When you fix a gap, remove its skip entry in the same commit and run the
  suite to show the test passes.

## Where tests go

- Unit tests next to the code (`#[cfg(test)] mod tests`); end-to-end tests
  in the existing `tests/e2e_<area>.rs` for the subsystem. Add a new binary
  only for a new fixture or CI contract: each one costs link time in every
  lane.
- agent.md's two-tier rule (unit + integration) still applies. Each tier has
  to guard its own failure mode, not duplicate the other.
