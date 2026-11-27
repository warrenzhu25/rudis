# Reviewing a Change

How an agent (or a person) reviews a diff to rudis. Findings come in order of
cost to users: lost or wrong data, then a hang or stall, then a compatibility
break, then performance, then code quality.

## 1. Correctness

- **Concurrency.** Which thread runs each piece? What else can run at each
  `.await`? Is any `RefCell` borrow held across an await? Is a cross-shard
  operation still atomic where Redis is (MULTI, MSET, cross-shard-strict)?
- **Consistency points.** For snapshots, streams and migrations: is the cut
  taken at one point per shard, and does every byte that's counted (CRC,
  offset, limit) match a byte that's sent? See
  `persistence-replication.md`.
- **Untrusted input.** New parse paths are total (no panics, no allocations
  sized by input).
- **Error paths.** On failure, what state is left behind, and does a retry
  succeed or loop forever?

## 2. Stalls and scale

- Any loop over keys, slots, shards or bytes: is it bounded per step, and
  how does it behave at 1M keys or 1 GB? (`never-stall-a-shard.md`)
- Nested scans: O(N × M) hidden behind a helper that looks O(1).

## 3. Redis compatibility

- Observable behavior matches Redis (errors, replies, INFO, logs, configs),
  with the source cited. Deliberate differences are in
  `docs/differences.md`. (`redis-compat.md`)

## 4. Performance

- Hot-path changes: the gate costs one relaxed load when off, no per-call
  allocation, and an A/B result is attached. (`hot-path.md`)

## 5. Tests and docs

- Each new test answers "what diff turns this red?", and bug fixes come with a
  regression test that fails on the old code. (`test-admission.md`)
- New TCL skips have reasons. Fixed gaps are removed from skip lists.
- `CHANGELOG.md` `[Unreleased]` and `docs/differences.md` are updated in the
  same commit.
- Comments state live constraints, not history. (`comment-style.md`)

## Writing findings

- Lead with the concrete failure: inputs, sequence and observed effect
  ("migrating > 256 MB fails with 'output buffer limit reached', then
  retries forever"), not "this might be a problem".
- Verify each finding against the source (and run it, where cheap) before
  reporting it. Mark anything unverified as such.
- Say what you checked and found fine, so the author knows the coverage.
