---
paths:
  - "**/*.rs"
---

# Comment Style

Comments are reviewed like code, and the author has to justify each one, not
the reviewer its removal. A comment states a fact the reader cannot get from
the code in front of them.

## What a comment is for

- **Cross-thread and cross-shard constraints.** What another shard, the
  mailbox, the replication stream or the AOF depends on: ordering, which
  thread owns a value, what must happen before an `.await`. Nothing in the
  local function shows the other side.

  ```rust
  // Registered before any shard serializes, so each shard's changes after
  // its snapshot reach the replica (see FullSyncCut).
  ```

- **Why rudis matches (or deliberately doesn't match) Redis.** Name the
  Redis behavior, function or test that dictates it. Compatibility code
  looks arbitrary without that anchor.

  ```rust
  // Like Redis's getClientType: a monitor counts as normal, and so does a
  // RESP3 subscriber.
  ```

- **Units and layout the name can't carry**: bytes vs keys vs slots vs
  shards, ms vs µs, wire formats.
- **Where a magic number came from**: a measurement, a Redis default, a
  hardware limit, or an admission that it's arbitrary (which tells the next
  person it's safe to change).

  ```rust
  /// Positions a trim step looks at: ~1 ms of work on a 1M-key shard.
  ```

- **Workarounds, with a reference and a retirement condition.** Without
  both, nobody can ever prove a workaround is safe to delete.
- **Contracts that are decisions, not mechanisms**: what a sentinel means,
  why something is deliberately left out, an ordering that looks accidental.

## What does not belong in a comment

- **History.** "X used to …", "this was changed because …", "now we also
  handle …" go in the commit message. Rudis commit messages carry the
  mechanism, the symptom and the measurement; `git log -S` / `git blame`
  find them. Next to the code, keep only the constraint that is still live,
  written for a reader who has never seen the commit.

  ```rust
  // Bad:  Asking for each slot's keys in turn used to scan the table once per
  //       slot, which stalled the shard for minutes.
  // Good: One pass over the table for all slots, a bounded step at a time.
  ```

- Prose restating the next line, the function name or the branch condition.
- Review attribution ("as suggested in review") and PR/commit numbers used
  as a changelog. An upstream Redis/Valkey issue or commit as a workaround's
  anchor is fine.
- Commented-out code.

## Editing other people's comments

A comment-only diff may remove or condense multi-line prose blocks
(restatements, history). **It does not touch one-line comments** unless the
line is wrong, stale or commented-out code. Telling a restatement from the last
written record of a cross-file fact takes more context than the line costs,
and deleting a load-bearing one-liner inside a big diff is never caught in
review.

## Doc comments (`///`)

- Public items and anything other modules call: state the contract (what
  it returns, what the caller must guarantee, what thread it runs on).
- Private helpers: a doc comment only if the contract isn't obvious from the
  signature.
- Tests: one or two lines saying which black-box behavior must not come
  back (see `test-admission.md`).
