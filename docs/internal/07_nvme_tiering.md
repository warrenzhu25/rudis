# Component 07: NVMe SSD Tiered Storage Engine (Implementation Deep-Dive & Code Reference)

> **Source File**: `src/tiering.rs` (1,082 lines)
> **Integration points**: `src/router.rs` (orchestration), `src/table.rs` (`RudisValue::Tiered`/`Cooled`,
> the extendible-hashing directory), `src/shard.rs`/`src/server.rs` (lifecycle, background tasks,
> squashed-pipeline fast paths), `src/connection.rs` (`TIER`/`MIGRATE`/`CLUSTER` command surfaces),
> `src/aof.rs` (AOF-compaction hydration)
> **High-Level Design Spec**: [`docs/design/07_nvme_tiering.md`](../design/07_nvme_tiering.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)
>
> This revision re-verifies every claim in the previous pass line-for-line against the current
> 1,082-line `src/tiering.rs` (previously ~932 lines) and its callers. Two of the three previously
> documented issues have changed: the free-space-reuse bug is **fixed**; the `upload_threshold_pct`
> dead-config issue is **unchanged**; the cross-file `MIGRATE`/tiering data-loss bug from the
> Component 11 pass is **confirmed, and found to exist in two independently hand-duplicated
> copies**. Three new findings are added in §7.

---

## 1. Source Module Map & Responsibilities

| File | Role in this subsystem |
| :--- | :--- |
| `src/tiering.rs` | `ShardTierManager`, `OpManager`, `SmallBins*`, on-disk record codec, `fallocate`/`FICLONE` syscalls, `TieringStats` |
| `src/router.rs` | `Router::spill_local`/`cool_local`/`load_local`/`decommit_local`/`stream_cold_read_local`/`check_auto_tier`/`ensure_loaded`/`get_local_direct`/`read_cold_key_local` — decides *when* tiering fires |
| `src/table.rs` | `RudisValue::Tiered`/`Cooled` variants, `TieredPointer`, the extendible-hashing segment/directory table, and the pointer-bookkeeping methods `router.rs` calls |
| `src/shard.rs`, `src/server.rs` | one `ShardTierManager` per shard at startup, 20 ms auto-tier poll loop, 2 s GC loop, squashed-pipeline cold-GET deferral |
| `src/connection.rs` | `TIER SPILL/LOAD/COOL/DECOMMIT/SPILLALL/GC/SNAPSHOT/INFO`, plus the `MIGRATE` command and `migrate_keys_to_node` (CLUSTER SETSLOT/REBALANCE/RESHARD) serialization paths that read tiered values |
| `src/aof.rs` | hydrates `Tiered` values back to plain bytes during AOF rewrite/compaction |

---

## 2. Component Architecture & Data Structures

```
                  Hot (RudisValue::String/Int/List/Set/ZSet/Hash/HLL/Stream)
                                   │
                 Router::cool_local  (stash to disk, KEEP ram copy)
                                   ▼
     RudisValue::Cooled { ptr: TieredPointer, val: Box<RudisValue> }
                                   │
                 Router::spill_local / decommit_local (drop ram copy)
                                   ▼
                  RudisValue::Tiered(TieredPointer)     (ram: pointer only)
                                   │
                 Router::load_local / stream_cold_read_local
                                   ▼
     RudisValue::Cooled { ptr, val }   <-- reload lands back in Cooled,
                                           NOT plain Hot (§3.4)
```

There is still no direct `Cooled → Hot` transition anywhere in the code (re-verified: `table.rs`'s
`decommit_cooled_key`/`decommit_all_cooled`, `:3734-3763`, only ever rewrite `Cooled → Tiered`, never
`Cooled → <plain variant>`). Once a key has ever been tiered, it permanently carries the 24-byte
`TieredPointer` bookkeeping overhead in one of its two live representations.

### 2.1 Core data structures (verified line-for-line against current `src/tiering.rs`)

```rust
pub const TIER_MAGIC: &[u8; 4] = b"TIER";              // tiering.rs:14
pub const PAGE_SIZE: usize = 4096;                       // tiering.rs:17 — Direct I/O & SmallBins page size
pub const SMALL_VALUE_LIMIT: usize = 2048;               // tiering.rs:20 — 2 KiB, NOT 4 KiB

pub struct TieredPointer {                 // table.rs:1407-1413
    pub file_id: u32,    // == shard_id, not a real multi-file id
    pub offset: u64,
    pub length: u32,
    pub value_type: u8,
}
```

`TieredPointer`'s fields sum to 17 logical bytes, but `u64` alignment pads the in-memory `struct` to
24 bytes — confirmed by `RudisValue::approx_bytes()` (`table.rs:1434-1448`), which charges
`RudisValue::Tiered(_)` a flat `24` and `Cooled { val, .. }` `24 + val.approx_bytes()`. This 24-byte
figure is also the exact constant `set_cooled_pointer`/`restore_tiered_value` add/subtract from
`used_memory` (`table.rs:3683-3731`) when transitioning a slot.

```rust
pub struct ShardTierManager {                      // tiering.rs:399-412
    pub shard_id: usize,
    pub port: u16,
    pub file: Rc<monoio::fs::File>,
    pub current_offset: Cell<u64>,       // next unused, page-aligned disk offset (append cursor)
    pub preallocated_len: Cell<u64>,     // NEW — high-water mark of fallocate'd file length
    pub path: PathBuf,
    pub stats: Arc<TieringStats>,
    pub op_manager: Rc<OpManager>,
    pub small_bins: RefCell<SmallBinsManager>,
    pub is_direct_io: bool,
    pub free_pages: RefCell<Vec<u64>>,        // NEW — reclaimed whole-page indices, LIFO free list
    pub free_extents: RefCell<Vec<(u64, u64)>>, // NEW — reclaimed (offset, len) multi-page extents
}

pub struct ActiveBin {                   // tiering.rs:321-325 — the one in-progress 4KB page being packed
    pub page_index: u64,
    pub buffer: Vec<u8>,
    pub items: Vec<SmallBinItem>,
}

pub struct SmallBinsManager {            // tiering.rs:363-367
    pub active_bin: Option<ActiveBin>,
    pub page_active_counts: HashMap<u64, usize>,  // live-record count per written page
    pub dead_pages: Vec<u64>,                     // pages whose count hit 0 -> GC candidates
}

pub struct OpManager {                   // tiering.rs:210-217 — Dragonfly-inspired read coalescing
    pub in_flight_reads: RefCell<HashMap<u64, Vec<flume::Sender<Result<Rc<Vec<u8>>, String>>>>>,
    pub pending_stashes: RefCell<HashSet<Bytes>>,     // in-flight stash keys (stale-overwrite guard)
    pub pending_stash_bytes: AtomicUsize,             // write-backpressure byte counter
}
```

**The two `free_pages`/`free_extents` fields are new since the last documentation pass** (added in
commit `8d899a1`, "implement extent and hole reuse to bound NVMe logical file growth") and directly
fix the previously-documented space-amplification bug — see §3.3 and §7#1.

`RudisValue` itself (`table.rs:1415-1431`) has 11 variants; the two tiering-relevant ones are:

```rust
pub enum RudisValue {
    // ... String, Int, SmallHash, Hash, List, Set, ZSet, HyperLogLog, Stream ...
    Tiered(TieredPointer),
    Cooled { ptr: TieredPointer, val: Box<RudisValue> },
}
```

`TieringStats` (`tiering.rs:24-46`) has **21 `AtomicU64` fields**: `tiered_keys`, `tiered_bytes`,
`ram_saved_bytes`, `disk_reads`, `disk_writes`, `dead_bytes`, `cooled_keys`, `decommit_count`,
`max_memory`, `ram_hits`, `ram_misses`, `total_stashes`, `total_fetches`, `total_deletes`,
`coalesced_reads`, `bin_pages`, `streaming_reads`, `gc_reclaimed_bytes`, `gc_cycles`,
`offload_threshold_pct` (default 60), `upload_threshold_pct` (default 80) — unchanged from the prior
pass. `reset_counters` (`:77-96`) zeroes all of them **except `max_memory`**, and `reset_tier_stats`
(`:200-207`) mutates the existing `Arc<TieringStats>` in place (documented hazard: `Router` caches a
clone of this `Arc` at startup for zero-lock reads — replacing the map entry instead of mutating it
in place would orphan that cached reference).

---

## 3. Execution Algorithms & Code Logic

### 3.1 Opening the tier file and recovering the write cursor (`ShardTierManager::open`, `:415-459`)

```rust
let direct_io_enabled = std::env::var("RUDIS_DIRECT_IO").map(|v| v != "0").unwrap_or(false);
// if enabled: OpenOptions + custom_flags(libc::O_DIRECT); on error, silently falls back
// to a normal buffered open (is_direct_io = false either way on fallback)
let raw_len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
let current_offset = raw_len.div_ceil(PAGE_SIZE as u64) * PAGE_SIZE as u64;
```

On restart the append cursor resumes from the file's length rounded **up** to the next 4 KiB
boundary — a shard that crashed mid-page leaves a small gap and starts fresh rather than risking a
torn-write overwrite. `preallocated_len` is initialized to the raw (unrounded) file length, and
`free_pages`/`free_extents` always start **empty** on open — free-list state is not persisted or
rebuilt from the file on restart (see §7#4).

### 3.2 `fallocate`-based chunk preallocation (`ensure_preallocated`, `:580-594`) — new since last pass

```rust
pub fn ensure_preallocated(&self, needed_end: u64) {
    if needed_end > self.preallocated_len.get() {
        let chunk = 64 * 1024 * 1024u64; // 64 MB chunks
        let new_len = needed_end.div_ceil(chunk) * chunk;
        // fallocate(fd, 0, 0, new_len); on failure, ftruncate(fd, new_len) instead
        self.preallocated_len.set(new_len);
    }
}
```

Every allocation that advances `current_offset` (`allocate_page`, `allocate_extent`) calls this
first. Growing the file in 64 MiB `fallocate` mode-0 (allocate, don't punch) chunks instead of
relying on implicit sparse-file extension on each 4 KiB/extent write reduces the number of extent
metadata updates the filesystem has to perform under sustained write load — this is the "64MB
fallocate" half of commit `089ca7a`'s title. `fallocate` failure (e.g. unsupported fs) degrades to
`ftruncate`, which still grows the file length but does **not** guarantee physical block reservation.

### 3.3 Allocation with free-list reuse (`allocate_page`/`allocate_extent`, `:596-625`) — the fix for the prior space-amplification bug

```rust
pub fn allocate_page(&self) -> u64 {
    if let Some(page_idx) = self.free_pages.borrow_mut().pop() {
        page_idx                                   // reuse a hole-punched page, no file growth
    } else {
        let cur = self.current_offset.get();
        let next_page_idx = cur / PAGE_SIZE as u64;
        self.current_offset.set(cur + PAGE_SIZE as u64);
        self.ensure_preallocated(cur + PAGE_SIZE as u64);
        next_page_idx
    }
}

pub fn allocate_extent(&self, aligned_len: usize) -> u64 {
    // first-fit scan of free_extents (Vec<(offset,len)>); splits a larger free extent
    // and pushes back the leftover (offset+req_len, len-req_len); else grows current_offset
}
```

**This directly contradicts the previously documented finding that hole-punched space was never
reused by the append-only write cursor — that bug is fixed as of commit `8d899a1`.** `free_pages`
is a simple LIFO stack of whole-page indices (no size discrimination needed, all SmallBins pages are
exactly `PAGE_SIZE`). `free_extents` is an unsorted `Vec<(offset, len)>` searched **first-fit**
(`iter().position(|(_, len)| *len >= req_len)`, `:612`) — not best-fit — so repeated alloc/free of
different-sized large values can still fragment the free-extent list over the file's lifetime (see
§7#2), but the file no longer grows without bound under a steady-state delete-heavy workload the way
it did before this fix. A dedicated unit test, `test_free_extent_and_page_reuse`
(`tiering.rs:1043-1078`), exercises exactly this: free a page/extent, reallocate, assert
`current_offset` did **not** advance.

Free lists are populated from two call sites:
- **`on_key_deleted`** (`:737-762`) — a key's `TieredPointer` is freed after `DEL`/expiry. For
  large standalone blocks (≥ `SMALL_VALUE_LIMIT`), the hole is punched immediately and the page/extent
  pushed onto the appropriate free list right there. Small (SmallBins-packed) records instead
  decrement `SmallBinsManager::page_active_counts` — see §3.5, they're not freed until their whole
  page is dead.
- **`on_key_overwritten`** (`:764-789`) — new since last pass, called whenever a `SET`/write
  **replaces** a key that was previously `Tiered`/`Cooled`, so the *old* disk pointer's space is
  reclaimed even though the key itself wasn't deleted. Threaded through **every** write fast path
  that can overwrite an existing key:
  - `table.rs`'s `set_extended_with_hash` (`:3123`) now **returns** `Option<(TieredPointer, bool)>` —
    the freed pointer plus an `is_cooled` flag — instead of `()`; both `set_with_hash` (`:3101`) and
    `set_extended` (`:3111`) were updated to propagate/discard it.
  - The cross-shard squashed-pipeline `SET` path in `connection.rs::execute_commands_squashed`
    and the local `ShardMessage::Batch` handler in `server.rs` (`:987-993`) both now check the
    `Some((ptr, is_cooled))` return and call `tm.on_key_overwritten(ptr, is_cooled)` — this fast
    path used to leak the old tiered pointer's disk space entirely before this commit.

### 3.4 Stashing a value: SmallBins packing vs. standalone aligned block (`stash_record`, `:630-710`)

Branches purely on the **encoded record's** length vs. `SMALL_VALUE_LIMIT` (2048 bytes):

- **< 2048 bytes**: packed into the shard's single in-progress `ActiveBin` (`ActiveBin::can_fit`,
  `:336-338`, `buffer.len() + record_len <= PAGE_SIZE`). If the current bin can't fit the record, the
  bin is flushed (`flush_active_bin`) and a **new page is obtained via `allocate_page()`** (so a
  freed hole can be reused here too) before packing continues. After appending, the bin is
  proactively flushed if it can no longer fit one more minimal ~64-byte record
  (`should_flush = !ab.can_fit(64)`, `:668`) — a heuristic "nearly full" cutoff, not an exact
  best-fit check.
- **≥ 2048 bytes**: whatever bin is currently open is flushed first (preserving on-disk order
  relative to the in-memory cursor), then the record is written as its own extent via
  `allocate_extent(aligned_len)` where `aligned_len = record_len.div_ceil(PAGE_SIZE) * PAGE_SIZE`. In
  Direct I/O mode the write buffer is explicitly zero-padded to `aligned_len` before the
  `write_all_at` (`:690-691`) — required because `O_DIRECT` writes must be page-aligned in length as
  well as offset.

`OpManager::start_pending_stash`/`finish_pending_stash` bracket the whole operation, and
`check_write_backpressure` (`:253-255`) rejects new stashes with `io::ErrorKind::WouldBlock` once
**16 MiB** of stashes are in flight (`pending_stash_bytes.load() > 16 * 1024 * 1024`) — unchanged
hardcoded threshold from the prior pass.

### 3.5 Garbage collection: dead-page tracking + `fallocate(FALLOC_FL_PUNCH_HOLE)`

```rust
pub fn on_key_deleted(&self, ptr: TieredPointer) {          // tiering.rs:737-762
    if (ptr.length as usize) < SMALL_VALUE_LIMIT {
        // SmallBins-packed: decrement that page's live-record count via
        // SmallBinsManager::decrement_page_key (:384-396); queues the page in dead_pages
        // only once the count hits 0 (other live records on the page block reclamation)
    } else {
        // large standalone block: punch the hole immediately, push page/extent onto the
        // free list immediately — no waiting for a GC pass
    }
}

pub fn run_gc(&self) -> usize {                              // tiering.rs:490-505
    let dead = std::mem::take(&mut small_bins.borrow_mut().dead_pages);
    for page_idx in &dead {
        Self::punch_hole(&self.file, page_idx * PAGE_SIZE as u64, PAGE_SIZE as u64, &self.stats);
        self.free_pages.borrow_mut().push(*page_idx);          // NEW — feeds allocate_page's free list
    }
    dead.len() * PAGE_SIZE
}
```

`punch_hole` (`:463-487`) issues `fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset,
length)` — `FALLOC_FL_KEEP_SIZE` means the *logical* file length (as seen by `stat`/`current_offset`)
is unaffected; only the physical block allocation is released, tracked via `gc_reclaimed_bytes`.
`run_gc` is invoked from a dedicated 2-second background loop in `server.rs` (`:261-268`,
`gc_router.gc_local()`), unchanged from the prior pass, plus on-demand via `TIER GC`.

Large-block deletes/overwrites (`on_key_deleted`/`on_key_overwritten`, ≥ `SMALL_VALUE_LIMIT`) punch
the hole **synchronously and immediately**, outside the periodic GC cycle, since they aren't shared
with any other live record.

### 3.6 On-disk record format and integrity check — CRC64 replaced with SIMD `xxh3_64`

```rust
pub fn encode_tiered_record(key: &[u8], val_payload: &[u8], val_type: u8) -> Vec<u8> {
    // TIER_MAGIC(4) | value_type(1) | key_len(4) | val_len(4) | xxh3_64_checksum(8) | key | val
    // total header = 21 bytes; checksum computed in-place over buf[21..] after the buffer
    // is fully built (single allocation, no separate temp Vec)
}
```

This is a change from the previously documented `crate::table::crc64`: commit `089ca7a`
("accelerate NVMe tiered storage with SIMD xxh3_64...") replaced the bit-by-bit CRC64 with
`xxhash_rust::xxh3::xxh3_64`, a SIMD-accelerated non-cryptographic hash, and eliminated a temporary
`Vec` allocation that the old CRC64 path needed. `decode_tiered_record` (`:842-885`) still verifies,
in order: minimum length (21 bytes), the `TIER_MAGIC` prefix, the `value_type` byte against what the
caller expects, declared-length bounds, and finally recomputes `xxh3_64` over the key+value body and
compares — any mismatch surfaces as `io::Error` (`"crc mismatch on tiered read"` — the error string
was not updated to say "checksum", a minor naming leftover from the CRC64 era).

### 3.7 Reading a record back: page-read coalescing, and where the RAM copy lands

```rust
pub async fn read_tiered_record(...) -> io::Result<(Bytes, Vec<u8>)> {   // tiering.rs:887-939
    if offset_in_page + len <= PAGE_SIZE {
        // 1. check the still-open ActiveBin first (record may not be flushed to disk yet)
        // 2. else: op_manager.read_page_coalesced(file, page_start, stats).await
    } else {
        // record spans/exceeds a page (a standalone large block) — direct read_exact_at
    }
    decode_tiered_record(&data, ptr.value_type)
}
```

`read_page_coalesced` (`:260-308`) still means N concurrent readers of the same still-warm 4 KiB page
cause exactly one `read_exact_at` syscall; late arrivals register a `flume` channel in
`in_flight_reads` and receive a clone of the same `Rc<Vec<u8>>` the initiator read.

Critically — unchanged from the prior pass — `Router::load_local` does **not** turn a
`RudisValue::Tiered(ptr)` back into a plain hot value. It calls `RudisTable::restore_tiered_value`
(`table.rs:3716-3731`), which sets the slot to `RudisValue::Cooled { ptr, val: Box::new(decoded) }`.
There is still no direct `Tiered → Hot` or `Cooled → Hot` transition anywhere in the code.

A second, **synchronous** reader exists for contexts that can't `.await`: `read_ptr_sync`
(`tiering.rs:791-806`), which checks the in-memory `ActiveBin` first (via `try_borrow`, so it won't
panic if the async side holds the `RefCell` — it just falls through to the blocking disk path
instead) and otherwise calls the free function `read_tiered_record_sync` (`:809-815`, a plain
`std::fs::File::read_exact_at`). This is new since the last pass and exists specifically for RDB/AOF
persistence — see §3.8.

### 3.8 Persistence hydration — new since last pass (commit `7c7061e`)

RDB and AOF both need plain bytes, not tiering pointers, in the serialized output. `ShardDb::save_rdb_chunk`
(`shard.rs:2273-2318`) branches on `entry.val`:

```rust
match &entry.val {
    RudisValue::Tiered(ptr) => {
        if let Some(ref tm) = self.tier_manager
            && let Ok((_, raw)) = tm.read_ptr_sync(*ptr)
        { /* write key + serialize_val_payload(&RudisValue::String(raw), buf) */ }
        // if read_ptr_sync fails (I/O error, checksum mismatch): the entry is silently
        // skipped — no key/value bytes are written for it at all (see §7#5)
    }
    RudisValue::Cooled { val, .. } => { /* serialize_val_payload(val, buf) — uses the RAM copy directly, no disk read */ }
    other => { /* normal path */ }
}
```

`aof.rs:2003-2011` does the equivalent for AOF compaction/rewrite, also via `read_ptr_sync`. Note the
asymmetry: `Tiered` values always cost a synchronous disk read during a snapshot; `Cooled` values are
free (RAM copy already present) — this is one more reason the engine keeps `Cooled` as a semi-permanent
state rather than eagerly decommitting.

### 3.9 Snapshotting: reflink-first, `copy_file_range` fallback (unchanged from prior pass)

```rust
pub fn snapshot_file(src_path: &Path, dst_path: &Path) -> io::Result<bool> {
    // 1. ioctl(FICLONE = 0x40049409) — instant CoW reflink (btrfs, XFS-reflink, some overlay setups)
    // 2. on failure: loop libc::copy_file_range in 16 MiB chunks (in-kernel copy, no userspace round-trip)
    // 3. on failure: std::fs::copy (plain buffered copy)
}
```

`ShardTierManager::snapshot` (`:553-578`) flushes the active `SmallBins` page and `sync_all`s the
file first, then calls `snapshot_file`, then writes a plain-text manifest
(`version`/`shard_id`/`file_size`/`is_reflink`/`current_offset`) next to the backup. **The manifest
does not record `free_pages`/`free_extents`** — a restored-from-snapshot shard starts with empty free
lists exactly like a fresh `open()` (see §7#4).

### 3.10 The auto-tiering trigger and spill loop (`Router::check_auto_tier`, `router.rs:622-675`)

```rust
pub async fn check_auto_tier(&self) {
    if max_mem == 0 { return; }                       // tiering never activates without --maxmemory
    let shard_max_mem = max_mem / num_shards;
    if used <= shard_max_mem { return; }

    // Phase 1: zero-I/O — decommit_local(None) drops the RAM copy of every Cooled entry
    let decommitted = self.decommit_local(None);
    if decommitted > 0 && used_after <= shard_max_mem { return; }

    // Phase 2: spill Hot keys in 64-key slices until under target_mem (NOT shard_max_mem)
    let target_mem = shard_max_mem.saturating_sub((shard_max_mem / 20).max(128 * 1024));
    loop {
        if used <= target_mem { break; }
        let hot_keys = table.get_hot_keys_for_spill(64);   // was 256 in a single call, pre-089ca7a
        if hot_keys.is_empty() { break; }
        for k in hot_keys {
            spill_local_internal(&k, /* flush_bin */ false).await;
            if used <= target_mem { break; }
        }
    }
    tm.flush_active_bin().await;   // one flush after the whole spill loop, not per-key
}
```

**Changed since the last pass** (commit `089ca7a`): Phase 2 used to fetch up to 256 hot keys in one
shot and stop exactly at `shard_max_mem`. It now spills in repeated 64-key slices down to a
**5%-headroom target** (`target_mem`, floor 128 KiB below `shard_max_mem`) — reducing oscillation
where a shard immediately re-triggers auto-tiering on the very next write after landing exactly at
the threshold. `get_hot_keys_for_spill` (`table.rs:3765-3788`) still walks a persistent round-robin
`spill_cursor`, not an LRU/frequency ranking, but now addresses slots through the table's
`cursor_bound()`/`cursor_to_global_idx()` abstraction (`table.rs:2296-2314`) rather than a raw index
— see §6 for why this matters against the segmented/directory table redesign.

`check_auto_tier` is invoked from **two** places: reactively, inline from `Router::set` whenever a
write pushes `used_memory` over `shard_max_mem` (`router.rs:984-999`, spawned via `monoio::spawn` so
the triggering write isn't blocked on it), and proactively from a dedicated 20 ms background loop in
`server.rs` (`:252-259`) — both paths are guarded by the same `is_auto_tiering: Cell<bool>` re-entrancy
flag so they can't run concurrently on one shard.

### 3.11 Cold-read gating: `offload_threshold_pct` only, `upload_threshold_pct` still dead

```rust
pub fn is_memory_constrained(&self) -> bool {        // router.rs:608-620
    if max_mem == 0 { return false; }
    let offload_pct = self.tier_stats.offload_threshold_pct.load(Relaxed);   // NOT upload_threshold_pct
    used_mem >= (shard_threshold * offload_pct) / 100
}

pub async fn read_cold_key_local(&self, key: &Bytes) -> Option<Bytes> {    // router.rs:793-806
    if self.is_memory_constrained() {
        self.stream_cold_read_local(key).await   // streams without promoting to RAM
    } else {
        self.load_local(key).await;              // promotes Tiered -> Cooled, lands in RAM
        self.local_db.borrow_mut().get(key)
    }
}
```

Re-confirmed: `upload_threshold_pct` is fully plumbed through `CONFIG SET/GET` and `TIER INFO`
(`connection.rs:6284-6285, 6461, 6550`) and stored/loaded via `get_upload_threshold_pct`/
`set_upload_threshold_pct` (`tiering.rs:126-137`), but **grepping `router.rs`, `shard.rs`,
`table.rs`, and `server.rs` for `upload_threshold` turns up zero reads** — the config exists,
defaults to 80, and does nothing. `is_memory_constrained` is the sole gate for "stream vs. promote,"
and it only ever consults `offload_threshold_pct` (default 60).

`read_cold_key_local` is the real GET-path entry point (called from `server.rs:389,429,1353` and
`router.rs:819` via `get_local_direct`) — not `ensure_loaded`, which is reserved for the explicit
`TIER LOAD` command and `dump_key` (§3.12/§7#5). Note `stream_cold_read_local`
(`router.rs:469-491`) only decodes `RudisValue::String`/`Int` — any other tiered value type reaching
this path under memory pressure returns `None` rather than streaming, since GET is only ever issued
against string-typed keys in normal operation this is not currently reachable as a user-visible bug,
but it means the streaming path is not a general-purpose "read any tiered value without RAM
promotion" primitive, only a string-GET fast path.

### 3.12 "Parallel cold reads" for squashed/cross-shard pipelines (commit `089ca7a`)

Before this commit, a squashed pipeline batch (`connection.rs::execute_commands_squashed`) that hit a
local `GET` on a `Tiered` key would immediately `.await` the disk read inline, serializing it with
every other command in the batch. The fix defers local cold `GET`s into a side buffer
(`local_cold_gets: SmallVec<[(usize, Bytes); 8]>`) while building the batch, dispatches all remote
shard batches first, and **then** awaits the local disk reads — so the local NVMe read runs
concurrently with the in-flight remote-shard round trips rather than blocking them. The equivalent
change in `server.rs`'s `ShardMessage::Batch` handler (`:948-993` region) goes further: if any
`cold_gets` were collected, the whole responder completion is moved into a `monoio::spawn`'d task that
loops over `stream_cold_read_local` for each cold key before calling `responder.finish(items)` —
letting the shard's reactor move on to the next message instead of blocking the worker loop on disk
I/O. This is also the commit that removed a previous `needs_async` heuristic in `server.rs` (`let
needs_async = false;` now unconditionally set, `:595` region) in favor of always deferring via this
mechanism.

---

## 4. Value-type tag encoding (`table.rs::get_value_for_spill`, `:3804-3811`)

| `value_type` byte | `RudisValue` variant(s) |
| :--- | :--- |
| 0 | `String`, `Int` |
| 1 | `List` |
| 2 | `Set` |
| 3 | `ZSet` |
| 4 | `SmallHash`, `Hash` |
| 5 | `HyperLogLog` |
| 6 | `Stream` |

`Tiered`/`Cooled` values themselves are rejected by `get_value_for_spill` (returns `None` — a
tiered/cooled key can't be spilled again without first being loaded) — this is the guard that keeps
`spill_local`/`cool_local` idempotent against a key that's already in one of those two states.

---

## 5. Concrete Numbers Reference

| Constant | Value | Where |
| :--- | :--- | :--- |
| `PAGE_SIZE` / Direct I/O & SmallBins page size | 4096 bytes | `tiering.rs:17` |
| `SMALL_VALUE_LIMIT` (SmallBins packing cutoff) | 2048 bytes | `tiering.rs:20` |
| On-disk record header size | 21 bytes (4 magic + 1 type + 4 key_len + 4 val_len + 8 checksum) | `encode_tiered_record`, `:817-840` |
| Checksum algorithm | `xxh3_64` (SIMD, was bit-by-bit CRC64) | `:837, :874` |
| `ActiveBin` "nearly full" flush heuristic | `!can_fit(64)` — flush if < 64 bytes of headroom left | `:668` |
| `fallocate` preallocation chunk | 64 MiB | `ensure_preallocated`, `:584` |
| Write backpressure threshold | 16 MiB of in-flight pending stashes | `check_write_backpressure`, `:254` |
| GC background cycle interval | 2 seconds | `server.rs:261-268` |
| Auto-tier background poll interval | 20 ms | `server.rs:252-259` |
| Auto-tier spill batch size | 64 keys per `get_hot_keys_for_spill` call (was 256, pre-`089ca7a`) | `router.rs:657` |
| Auto-tier spill target headroom | `max(5% of shard_max_mem, 128 KiB)` below threshold | `router.rs:652` |
| Default `offload_threshold_pct` | 60 | `TieringStats::default`, `:70` |
| Default `upload_threshold_pct` (stored, unused) | 80 | `TieringStats::default`, `:71` |
| `TieredPointer` logical / in-memory size | 17 bytes serialized / 24 bytes in RAM (alignment padding) | `table.rs:1407-1413`, `approx_bytes`, `:1445-1446` |
| `TieringStats` field count | 21 `AtomicU64` fields | `tiering.rs:24-46` |
| `FICLONE` ioctl constant | `0x40049409` | `snapshot_file`, `:520` |
| `copy_file_range` fallback chunk size | 16 MiB | `snapshot_file`, `:530` |

---

## 6. Cross-Component Interactions

- **`src/table.rs`**: owns `RudisValue::Tiered`/`Cooled` and the pointer-bookkeeping methods
  (`set_tiered_pointer`, `set_cooled_pointer`, `restore_tiered_value`, `get_value_for_spill`,
  `decommit_cooled_key`, `decommit_all_cooled`, `is_tiered`, `is_cooled`, `get_hot_keys_for_spill`,
  `get_entry`), plus `serialize_val_payload`/`deserialize_val_payload` (the value-encoding format this
  file's records carry as their payload). **Integration with the extendible-hashing segment/directory
  table**: `get_hot_keys_for_spill`'s round-robin `spill_cursor` addresses slots via
  `RudisFlatTable::cursor_bound()`/`cursor_to_global_idx()` (`:2296-2314`) instead of a raw `Vec`
  index — these two methods abstract over whether the table is still in its single-segment form
  (`cursor_bound() == segments[0].slots.len()`) or has grown into multiple fixed-capacity segments
  under the directory (`cursor_bound() == segments.len() * (SEG_CAP + STASH_CAP)`, with
  `cursor_to_global_idx` mapping a linear cursor to a `(segment, local_idx)`-derived global index via
  `GLOBAL_IDX_SHIFT`/`MASK`). This means the tiering spill cursor survives a directory
  split/grow transparently — re-verified against the current, much-larger `table.rs`, this
  integration is correct and was evidently designed for exactly this caller.
- **`src/router.rs`**: the orchestration layer described in §3.10-§3.12. `Router::dump_key`
  (`:944-961`) is the one place outside the normal GET path that calls `ensure_loaded` before
  reading a value — used by both `MIGRATE` and `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` (§7#5).
- **`src/shard.rs`**: `ShardDb.tier_manager: Option<Rc<ShardTierManager>>`, one instance per shard,
  created at shard startup; `save_rdb_chunk` hydrates `Tiered` values via `read_ptr_sync` (§3.8).
- **`src/aof.rs`**: AOF rewrite/compaction hydrates `Tiered` values the same way (§3.8).
- **`src/connection.rs`**: `GET` on a `Tiered`/`Cooled` key routes through
  `Router::get`→`get_local_direct`→`read_cold_key_local` (§3.11), not through the table directly.
  The `TIER` command family (§8 below) and the `MIGRATE`/cluster-migration serialization paths
  (§7#5) also live here.
- **`src/main.rs`/`src/server.rs`**: `RUDIS_DIRECT_IO` and `RUDIS_TIER_DIR` are process env vars, not
  CLI flags; every shard unconditionally opens a `ShardTierManager` at startup regardless of whether
  `maxmemory` is configured. `--maxmemory`/`--tiered-offload-threshold`/`--tiered-upload-threshold`
  feed `set_max_memory`/`set_offload_threshold_pct`/`set_upload_threshold_pct`. With `maxmemory` unset
  (`0`), `check_auto_tier` returns immediately and the tier file, while open, is never written to via
  the automatic path — only the manual `TIER SPILL`/`TIER SPILLALL`/`TIER COOL` commands can populate
  it.

---

## 7. Known Bugs & Limitations (re-verified + new)

1. **Free-space reuse — previously a confirmed space-amplification bug, now FIXED.** The prior
   documentation pass found that `fallocate(FALLOC_FL_PUNCH_HOLE)`-reclaimed disk space was never
   reused by the append-only write cursor, so a delete-heavy small-value workload would grow the
   tier file's logical length without bound even though physical block usage stayed flat. Commit
   `8d899a1` ("implement extent and hole reuse to bound NVMe logical file growth") added
   `free_pages`/`free_extents` to `ShardTierManager` and wired `allocate_page`/`allocate_extent`
   to check them first (§3.3); `on_key_deleted` and the new `on_key_overwritten` (§3.3) populate
   them. Verified present and exercised by `test_free_extent_and_page_reuse`
   (`tiering.rs:1043-1078`). **Residual limitation**: `free_extents` is searched first-fit, not
   best-fit or size-bucketed, so a long-running large-value-heavy workload with varied value sizes
   can still fragment the free-extent list into many small, non-contiguous reusable ranges over time
   — bounded growth, not zero fragmentation.
2. **`upload_threshold_pct` is still fully configured and reported but never read anywhere in
   `router.rs`/`shard.rs`/`table.rs`/`server.rs` — confirmed still present, unchanged from the prior
   pass.** `is_memory_constrained` (`router.rs:608-620`, the sole gate deciding "stream a cold read
   vs. promote it to RAM") consults only `offload_threshold_pct`. Either give `upload_threshold_pct`
   a real second gate or remove the config surface — it currently implies (per its own doc comment,
   `tiering.rs:45`) behavior it does not have.
3. **Data loss during cluster slot migration for keys still in the `Tiered` (fully RAM-evicted)
   state — confirmed present, and found in two independently hand-duplicated copies.** Both the
   client-facing `MIGRATE` command handler (`connection.rs:10598-10937`, empty arm at
   `:10908-10909`) and the internal `migrate_keys_to_node` helper used by `CLUSTER SETSLOT
   MIGRATING`/`REBALANCE`/`RESHARD` (`connection.rs:3449-3732`, empty arm at `:3711`) build their
   destination `SET`/`HSET`/... wire commands with a `match val { ... RudisValue::Tiered(_) |
   RudisValue::Cooled { .. } => {} }` — an empty arm that serializes nothing for a value still in
   either tiered representation. Both call `Router::dump_key` (`router.rs:944-961`), which calls
   `self.ensure_loaded(key)` first, but `ensure_loaded` (`router.rs:695-715`) returns `false`
   **without loading anything** whenever `is_memory_constrained()` is true. **In practice only the
   pure `Tiered` state is reachable here, not `Cooled`**: `RudisTable::get_entry`
   (`table.rs:2984-3007`), which both `dump_key` implementations read through, unwraps
   `Cooled { val, .. }` to `(**val).clone()` before returning — so a `Cooled` key's in-RAM copy is
   always available and gets serialized correctly regardless of memory pressure. A `Tiered` key,
   however, is returned as-is (`other => other.clone()`) and hits the empty arm. Both migration
   paths then **unconditionally delete every key `dump_key` returned** from the source
   (`connection.rs:10929-10933` and `:3726-3730`) whenever `copy` was not requested — with no check
   that anything was actually written to the destination for that key. **Net effect: migrating a
   slot (via `MIGRATE`, `CLUSTER SETSLOT`, `REBALANCE`, or `RESHARD`) while the source shard is at or
   above its `offload_threshold_pct` memory threshold silently drops any key that is fully
   tiered-to-disk (not merely cooled) at the moment of migration — deleted from the source, never
   written to the destination.** This is unaffected by the free-space-reuse fix in §7#1 (different
   mechanism entirely) and is unaffected by the `089ca7a` parallel-cold-read changes (§3.12), which
   only touch the local GET fast path, not `dump_key`/migration. Fixing this requires either making
   `dump_key` force-load a `Tiered` value regardless of memory pressure (accepting the RAM spike) or
   giving both match blocks a real arm that reads the value via `tm.read_ptr_sync`/an async
   equivalent before serializing it — and doing so in **both** hand-duplicated copies, since nothing
   shares this match logic between them today.
4. **New — restart/snapshot-restore does not repopulate `free_pages`/`free_extents`.**
   `ShardTierManager::open` (`:415-459`) always initializes both free lists empty, and the snapshot
   manifest (`:568-574`) records only `version`/`shard_id`/`file_size`/`is_reflink`/`current_offset`
   — no free-list state. A shard that restarts (or is restored from a `TIER SNAPSHOT`) resumes
   writing strictly from `current_offset` onward even if the on-disk file contains hole-punched,
   physically-unallocated pages/extents from before the restart. This doesn't reintroduce the old
   unbounded-growth bug (holes are still real holes, sparse-file-wise — `FALLOC_FL_KEEP_SIZE` keeps
   the logical length as-is and the filesystem still reports the freed physical blocks) but it does
   mean a long-lived shard's *logical* file length (and thus `current_offset`) only ever grows across
   restarts, never shrinks, and the benefit of hole reuse (§3.3/§7#1) resets to zero at every process
   restart until new deletes repopulate the free lists from scratch.
5. **New — RDB snapshot silently drops a `Tiered` entry if its disk read fails.**
   `ShardDb::save_rdb_chunk`'s `Tiered` branch (`shard.rs:2291-2303`) only emits key+value bytes
   `if let Ok((_, raw)) = tm.read_ptr_sync(*ptr)` — on an `Err` (I/O error, checksum mismatch, or a
   stale/corrupt pointer), the `if let` simply doesn't execute and **no bytes at all** are written for
   that entry, silently omitting the key from the RDB snapshot rather than erroring the whole dump or
   logging the failure. The expire-time byte (`0xFC` + timestamp, written just before the `match`, if
   the key has a TTL) is also already written by that point regardless of the read's success — meaning
   a failed hydration can leave an orphaned expire-opcode with no following key/value pair in the RDB
   stream, which is a stream-format concern for whatever parses this custom RDB variant back in,
   beyond just the missing key.
6. **New — no buffer-alignment handling for `O_DIRECT` writes/reads.** When `RUDIS_DIRECT_IO=1`
   successfully opens the file with `libc::O_DIRECT`, all I/O still goes through plain heap-allocated
   `Vec<u8>` buffers (`vec![0u8; PAGE_SIZE]` in `read_page_coalesced`, the record `Vec<u8>` built by
   `encode_tiered_record`, etc.) — there is no `posix_memalign`/`std::alloc::Layout`-based aligned
   allocation anywhere in `tiering.rs`. Disk *offsets* and *lengths* are correctly page-aligned
   (`current_offset` always advances in `PAGE_SIZE` multiples; large-record buffers are explicitly
   padded to `aligned_len` before direct-mode writes, `:690-691`), but the *buffer's memory address*
   is left to the allocator, which on Linux does not guarantee 4096-byte alignment for arbitrary-sized
   `Vec` allocations. Genuine `O_DIRECT` I/O on most Linux filesystems requires the userspace buffer
   address itself to be page-aligned in addition to the file offset/length; whether this actually
   works in practice depends on `monoio`'s internal buffer handling (e.g., bounce-buffering through an
   aligned intermediate) rather than anything `tiering.rs` does itself. Worth verifying directly
   against the `monoio` version in use rather than assuming; if `monoio` does not bounce-buffer,
   `RUDIS_DIRECT_IO=1` could intermittently fail individual reads/writes with `EINVAL` depending on
   allocator behavior, which would currently surface as an opaque `io::Error` propagated up through
   `stash_record`/`read_tiered_record`.

---

## 8. `TIER` Command Family (`connection.rs:6150-6263`, dispatch via `crate::resp::TierSubcommand`)

| Subcommand | Behavior | Return |
| :--- | :--- | :--- |
| `TIER SPILL <key>` | `Router::spill_key` → local `spill_local` or routed to the owning shard via `ShardMessage::TierSpill`; force-spills one hot key to disk, dropping the RAM copy (`Hot → Tiered`) | `:1`/`:0` |
| `TIER COOL <key>` | `Router::cool_key` → `cool_local`; force-cools one hot key, **keeping** the RAM copy (`Hot → Cooled`) | `:1`/`:0` |
| `TIER LOAD <key>` (alias `PROMOTE`) | `Router::ensure_loaded` → `load_local`; force-loads a `Tiered` key back into RAM, landing as `Cooled`, not plain hot | `:1`/`:0` |
| `TIER DECOMMIT [key]` | `Router::decommit`; drops the RAM copy of one `Cooled` key (`Cooled → Tiered`) or, with no key, every `Cooled` key on every shard | `:<count>` |
| `TIER SPILLALL` | `Router::spill_all` per shard (local + fan-out via `ShardMessage::TierSpillAll`); iterates **every** key on each shard (`table.keys(b"*")`) and attempts `spill_local` on each — already-tiered/cooled/sticky keys are silently skipped by `spill_local_internal`'s own guards | `:<count>` |
| `TIER GC` | `Router::gc_all`; runs one `run_gc` pass (§3.5) on every shard | `:<bytes reclaimed>` |
| `TIER SNAPSHOT <dir>` (alias `BACKUP`) | `Router::tier_snapshot_all`; calls `ShardTierManager::snapshot` (§3.9) on every shard, aggregating `(all_reflink, total_bytes, shard_count)` | `+OK snapshot created in <ms>ms, shards:<n>, bytes:<n>, reflink:<bool>` |
| `TIER INFO` | Dumps all 21 `TieringStats` fields plus `maxmemory`/`used_memory` (the latter via `Router::get_total_used_memory`, which sums **every** shard's `table.used_memory` over the mesh, not just the local shard) | RESP bulk string, `# Tiered Storage (io_uring NVMe)` header + `key:value\r\n` lines |

---

## 9. Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: There is no `Tiered → Hot` or `Cooled → Hot` transition anywhere in the code (§2).
  Once a key is ever spilled or cooled, it permanently carries `TieredPointer` bookkeeping in every
  future representation — `Cooled` behaves as a one-way, permanent write-through cache layer.
* **Gotcha 2**: `stream_cold_read_local` only decodes `RudisValue::String`/`Int` (§3.11) — it is a
  GET-specific fast path, not a general "read any tiered value without RAM promotion" primitive.
* **Gotcha 3**: `Direct I/O` (`RUDIS_DIRECT_IO=1`) page-aligns disk offsets/lengths but not buffer
  memory addresses (§7#6) — do not assume this mode is verified end-to-end without checking
  `monoio`'s buffer handling directly.
* **Gotcha 4**: `fallocate(FALLOC_FL_PUNCH_HOLE)` reclaims physical blocks without shrinking the
  logical file (`FALLOC_FL_KEEP_SIZE`) — freed space is tracked purely in the in-memory
  `free_pages`/`free_extents` lists (§3.3), which are **not** persisted or rebuilt across a restart
  or `TIER SNAPSHOT` restore (§7#4).
* **Gotcha 5**: sub-millisecond `TIER SNAPSHOT`/`BACKUP` relies on `ioctl(FICLONE)` reflink support
  (btrfs, XFS-reflink); it transparently falls back to `copy_file_range`, then plain `std::fs::copy`
  (§3.9) — don't assume a fast snapshot on ext4 or a non-CoW filesystem.
* **Gotcha 6**: if testing `MIGRATE`/`CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` against a dataset with
  tiered keys, check for silently-dropped data (§7#3) whenever the source shard is at or above its
  `offload_threshold_pct` — this is a real, currently-unfixed data-loss path, not a hypothetical one,
  and exists in two separate hand-duplicated copies of the same match logic that must both be
  patched together.
* **Gotcha 7**: the two match blocks in `connection.rs` that serialize a `RudisValue` for wire
  transfer (`Command::Migrate`'s handler and `migrate_keys_to_node`) are not shared code — a fix
  applied to one (e.g. adding a real `Tiered`/`Cooled` arm) must be applied to both, or the
  maintenance divergence documented in Component 11 §6/§7#6 repeats itself here.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests (includes test_free_extent_and_page_reuse, tiering.rs:1043-1078)
cargo test --lib -- --test-threads=1
```
