# Component 07: NVMe SSD Tiered Storage Engine (Design)

## Component 07: NVMe SSD Tiered Storage Engine

> **Source Files**: ``src/tiering.rs``


---

### 1. Architectural Purpose & Scope

`src/tiering.rs` implements Rudis's per-shard NVMe/disk offload engine. Each shard owns one
private tiered-storage file (`tier_shard_{id}.db` under a configured directory) and one
`ShardTierManager` that packs small values into 4KB pages (`SmallBins`), writes larger values
as their own aligned blocks, and lets `RudisTable` (`src/table.rs`) replace a hot in-RAM value
with a small pointer (`TieredPointer`) once it has been written to disk. Orchestration (when to
spill, when to reload, the auto-tiering trigger) lives in `src/router.rs`, not here — this file
is the disk I/O and page-packing layer underneath it.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-local, `Rc`-based, not `Arc`/`Mutex`**: `ShardTierManager` holds `file: Rc<monoio::fs::File>` and uses `RefCell`/`Cell` internally — it is only ever used by the one shard that owns it, consistent with the rest of the shared-nothing architecture. Cross-shard tiering requests go through `ShardMessage::Tier*` variants (Component 04), not by sharing a `ShardTierManager` across threads.
2. **`O_DIRECT` is opt-in and falls back automatically.** `ShardTierManager::open` only attempts `O_DIRECT` if the `RUDIS_DIRECT_IO` environment variable is set to something other than `"0"`; if the `O_DIRECT` open call fails (common on filesystems/kernels that don't support it), it silently retries with a normal buffered open. There is no page-cache-bypass guarantee unless that env var is set *and* the underlying filesystem actually supports it.
3. **Global, per-port shared statistics behind a lock.** `TieringStats` (23 atomic counters) is stored in a process-wide `static TIER_STATS: RwLock<Option<HashMap<u16, Arc<TieringStats>>>>`, one entry per listening port, shared by every shard on that port. This is a real (small, read-mostly) synchronization point outside the shared-nothing data path, used purely for reporting (`INFO`-style stats), not for coordinating storage itself.
4. **Read coalescing, not hole punching, is the concurrency-sensitive part.** `OpManager::read_page_coalesced` ensures that if two in-flight reads target the same 4KB page, only one physical `read_exact_at` happens; the second caller waits on a `flume::bounded(1)` channel fed by the first.
5. **Write backpressure via a byte counter, not a queue depth.** `OpManager::check_write_backpressure` returns `true` once `pending_stash_bytes` (tracked via `AtomicUsize`, incremented in `start_pending_stash`/decremented in `finish_pending_stash`/`cancel_pending_stash`) exceeds a hardcoded 16MB; `ShardTierManager::stash_record` refuses new stashes (`io::ErrorKind::WouldBlock`) while over that limit.

---

---

### 6. Performance Characteristics

- **`O_DIRECT` is conditional, not guaranteed** (§2.2) — actual page-cache-bypass behavior
  depends on `RUDIS_DIRECT_IO` being set and the filesystem/kernel actually honoring the flag;
  silently falls back to normal buffered I/O otherwise.
- **Read coalescing collapses concurrent hot-page reads** to one physical read plus N
  in-memory channel deliveries, avoiding redundant disk I/O when several keys on the same
  4KB SmallBin page are accessed close together.
- **Write backpressure is a simple byte-budget gate** (16MB of in-flight stash data), not a
  queue-depth or per-key limit — a burst of large concurrent spills can hit it and get
  `WouldBlock` back to the caller.
- **GC reclaims whole 4KB pages, not individual records** — a page with even one surviving
  record can't be punched; deletion-heavy small-value workloads can accumulate dead-but-unfreed
  bytes (`dead_bytes` stat) until every record sharing a page happens to be deleted.
- **Snapshotting cost depends entirely on filesystem reflink support** — instant on
  btrfs/XFS-with-reflink, an in-kernel `copy_file_range` loop otherwise (still avoiding a
  full userspace read+write round trip), and only falls all the way back to `std::fs::copy`
  if both kernel-assisted paths are unavailable.

---
