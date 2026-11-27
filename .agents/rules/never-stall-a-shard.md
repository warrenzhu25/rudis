---
paths:
  - "src/**/*.rs"
---

# Never Stall a Shard

Each shard thread serves its clients, its mailbox, replication and expiry
from one event loop. Anything that runs on it without yielding stops all of
that, including SIGTERM handling and INFO. A stalled shard looks like a
dead server.

## Rules

- **Work proportional to the dataset is done in bounded steps that yield**
  (`monoio::time::sleep(Duration::ZERO)`, `mailbox::yield_now()`), with a
  budget in positions, keys or bytes, not "until done". Existing examples to
  copy:
  - `asm::run_trim_shard`: cursor passes over the table, 4096 positions per
    step;
  - `redis_rdb::load_file_async`: 128 keys and 1 MB of CRC per step;
  - `router::flush_shard_for_full_sync`: drops segments with yields;
  - the AOF/sync images (`AofImage::step`): a time budget plus a piece size.
- **No nested scans of the table.** A per-slot, per-key or per-pattern loop
  that rescans the whole table is O(N × M). Collect into a set and do **one**
  pass (`keys_in_slot_set_step`). Per-slot counts (`slot_key_count`) are O(1);
  per-slot key listings (`slot_keys`) scan everything.
- **Know which thread runs it.** Code reached from the cluster bus thread,
  a script (`*_blocking` variants) or a `Drop` can't await. Keep it O(1), or
  hand the work to the shard as a message.
- **Snapshot-style work that must stay consistent** (dump-and-arm, a sync
  image's cut) may block only for as long as it truly needs. Document the
  cost per GB and measure it. Example: `docs/differences.md`, "The source
  dumps the slots' keys in one step per shard".
- **Bounded memory, too.** Don't build a whole-dataset `Vec` and then copy it
  again (`concat()`). Append into the destination buffer once
  (`send_bytes_unlimited_parts`).

## How to check

- Run the operation on **1M keys** (`DEBUG POPULATE 1000000 key 1024`, with
  `--threads 1` so one shard holds them all), ping the server every 10 ms
  from a second connection while it runs, and report the slowest PING in the
  commit message.
- If a shard won't answer, `perf record -g -p <pid>` shows where it spins
  (`ptrace`/gdb attach may not be allowed). That's how the trim bug showed
  up: 99.8% in `slot_keys`.
