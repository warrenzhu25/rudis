# Component 07: NVMe SSD Tiered Storage Engine (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/tiering.rs, src/tiering/`  
> **High-Level Design Spec**: [`docs/design/07_nvme_tiering.md`](../design/07_nvme_tiering.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/tiering.rs` | Core implementation and logic | Primary data structures and algorithms |
| `src/tiering/` | Core implementation and logic | Primary data structures and algorithms |

---

### 3. Component Architecture & Data Structures

```
                  Hot (RudisValue::String/Int/List/Set/ZSet/...)
                                   │
                 Router::cool_local  (stash to disk, KEEP ram copy)
                                   ▼
     RudisValue::Cooled { ptr: TieredPointer, val: Box<RudisValue> }
                                   │
                 Router::spill_local / decommit_local (drop ram copy)
                                   ▼
                  RudisValue::Tiered(TieredPointer)     (ram: 4 bytes + tag)
                                   │
                 Router::load_local / stream_cold_read_local
                                   ▼
     RudisValue::Cooled { ptr, val }   <-- reload lands back in Cooled,
                                           NOT plain Hot (see §4.3)
```

#### Core real data structures

```rust
pub const PAGE_SIZE: usize = 4096;
pub const SMALL_VALUE_LIMIT: usize = 2048;   // records below this go into a SmallBin page
pub const TIER_MAGIC: &[u8; 4] = b"TIER";

pub struct TieredPointer {          // src/table.rs — 4+8+4+1 = 17 bytes, held inline in RudisValue
    pub file_id: u32,
    pub offset: u64,
    pub length: u32,
    pub value_type: u8,
}

pub struct ShardTierManager {
    pub shard_id: usize,
    pub port: u16,
    pub file: Rc<monoio::fs::File>,
    pub current_offset: Cell<u64>,      // next unused, page-aligned disk offset
    pub path: PathBuf,
    pub stats: Arc<TieringStats>,
    pub op_manager: Rc<OpManager>,
    pub small_bins: RefCell<SmallBinsManager>,
    pub is_direct_io: bool,
}

pub struct ActiveBin {                  // the one in-progress 4KB page being packed
    pub page_index: u64,
    pub buffer: Vec<u8>,
    pub items: Vec<SmallBinItem>,
}

pub struct SmallBinsManager {
    pub active_bin: Option<ActiveBin>,
    pub page_active_counts: HashMap<u64, usize>,  // live-record count per written page
    pub dead_pages: Vec<u64>,                     // pages whose count hit 0 -> GC candidates
}

pub struct OpManager {
    pub in_flight_reads: RefCell<HashMap<u64, Vec<flume::Sender<Result<Rc<Vec<u8>>, String>>>>>,
    pub pending_stashes: RefCell<HashSet<Bytes>>,
    pub pending_stash_bytes: AtomicUsize,
}
```

`TieringStats` has 23 fields (`tiered_keys`, `cooled_keys`, `disk_reads`, `disk_writes`,
`dead_bytes`, `ram_saved_bytes`, `coalesced_reads`, `gc_reclaimed_bytes`,
`offload_threshold_pct` (default 60), `upload_threshold_pct` (default 80), ...) — all
`AtomicU64`, updated from `router.rs`'s tiering methods (Component 04) and read by whatever
reports tiering stats (`INFO`-style output, not shown in this file).

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Opening the tier file and recovering the write cursor

```rust
pub async fn open(shard_id: usize, port: u16, dir: &Path) -> io::Result<Self> {
    let direct_io_enabled = std::env::var("RUDIS_DIRECT_IO").map(|v| v != "0").unwrap_or(false);
    let (file, is_direct) = if direct_io_enabled {
        let mut opts = monoio::fs::OpenOptions::new();
        opts.read(true).write(true).create(true);
        opts.custom_flags(libc::O_DIRECT);
        match opts.open(&path).await {
            Ok(f) => (f, true),
            Err(_) => { /* fall back to a normal buffered open */ }
        }
    } else { /* normal buffered open */ };

    let raw_len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let current_offset = (raw_len + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64 * PAGE_SIZE as u64;
    ...
}
```

On restart, the write cursor resumes from the file's current length, rounded up to the next
4KB boundary — so a shard restarted mid-page never overwrites a partially-written page, it
just leaves a small gap and starts a fresh page.

#### 4.2 Stashing a value: SmallBins packing vs. standalone aligned block

`stash_record` (called by `Router::spill_local`/`cool_local`, Component 04) branches purely on
size:

```rust
pub async fn stash_record(&self, key: &Bytes, val_payload: &[u8], val_type: u8) -> io::Result<TieredPointer> {
    if self.op_manager.check_write_backpressure() {
        return Err(io::Error::new(io::ErrorKind::WouldBlock, "write backpressure: stash buffer full"));
    }
    let record = encode_tiered_record(key, val_payload, val_type);
    ...
    if record_len < SMALL_VALUE_LIMIT {
        // pack into (or start a new) ActiveBin page; flush it once it can't fit
        // one more ~64-byte record (`should_flush = !ab.can_fit(64)`)
    } else {
        // flush whatever SmallBin is open, then write this record as its own
        // record_len-rounded-up-to-4KB standalone block at current_offset
    }
}
```

Records under 2KB (`SMALL_VALUE_LIMIT`) get packed multiple-to-a-page into the shard's single
`ActiveBin`; a page is flushed to disk (`flush_active_bin`, one `write_all_at` per page) either
when a new record won't fit or when it's nearly full (heuristically, when there's no room left
for one more minimal ~64-byte record). Records at or above 2KB skip bin-packing entirely,
flush whatever bin is currently open first (so nothing gets reordered on disk relative to the
in-memory `current_offset` cursor), then get written as their own page-aligned block.

#### 4.3 The on-disk record format (CRC64-checked, not just a length prefix)

```rust
pub fn encode_tiered_record(key: &[u8], val_payload: &[u8], val_type: u8) -> Vec<u8> {
    // TIER_MAGIC(4) | value_type(1) | key_len(4) | val_len(4) | crc64(8) | key | val_payload
}
```

`decode_tiered_record` verifies the magic bytes, the `value_type` byte matches what the caller
expected, and recomputes a CRC64 (`crate::table::crc64`) over `key + val_payload` before
trusting the bytes — a corrupt or torn write is detected and surfaced as an `io::Error`
(`"crc mismatch on tiered read"`) rather than silently returning garbage.

#### 4.4 Reading a record back: page-read coalescing, and where the RAM copy lands

```rust
pub async fn read_tiered_record(...) -> io::Result<(Bytes, Vec<u8>)> {
    let offset_in_page = (ptr.offset % PAGE_SIZE as u64) as usize;
    if offset_in_page + len <= PAGE_SIZE {
        // check the still-open ActiveBin first — the record may not be flushed yet
        // otherwise: op_manager.read_page_coalesced(file, page_start, stats).await
    } else {
        // record spans/exceeds a page (a standalone large block) — direct read_exact_at
    }
    decode_tiered_record(&data, ptr.value_type)
}
```

`read_page_coalesced` (§2.4) means N concurrent readers of the same still-warm 4KB page cause
exactly one `read_exact_at` syscall; everyone else gets a clone of the same `Rc<Vec<u8>>`.

Critically — and unlike the original design's simple "promote back to hot" description —
`Router::load_local` (which calls this) does **not** turn a `RudisValue::Tiered(ptr)` back into
a plain hot value. It calls `RudisTable::restore_tiered_value`, which sets the slot to
`RudisValue::Cooled { ptr, val: Box::new(decoded_val) }` — i.e. a `Tiered` read always lands as
`Cooled` (RAM-resident *and* still disk-backed), never straight back to a bare `String`/`List`/
etc. Something has to explicitly `decommit` a `Cooled` entry (or it has to be spilled again) to
either free the RAM copy (back to `Tiered`) or fully rejoin the "hot" set — there is no direct
`Cooled → Hot` transition in the code; `Cooled` behaves as a permanent write-through cache
layer once a key has ever been tiered.

#### 4.5 Garbage collection: dead-page tracking + `fallocate` hole punching

```rust
pub fn on_key_deleted(&self, ptr: TieredPointer) {
    if (ptr.length as usize) < SMALL_VALUE_LIMIT {
        // decrement that page's live-record count; if it hits 0, queue the page in dead_pages
    } else {
        // large standalone block: punch the hole immediately, no waiting
    }
}

pub fn run_gc(&self) -> usize {
    let dead = std::mem::take(&mut self.small_bins.borrow_mut().dead_pages);
    for page_idx in &dead {
        Self::punch_hole(&self.file, page_idx * PAGE_SIZE as u64, PAGE_SIZE as u64, &self.stats);
    }
    dead.len() * PAGE_SIZE
}
```

Because SmallBins pack multiple keys per 4KB page, a single deleted key can't reclaim its page
immediately — `SmallBinsManager` tracks a live-record count per page and only queues the page
for `fallocate(FALLOC_FL_PUNCH_HOLE)` once every record on it has been deleted. Standalone
large-value blocks (≥2KB) are punched immediately on delete since they aren't shared with
anything else. `run_gc` is invoked periodically from `src/server.rs`'s 2-second GC task
(Component 01).

#### 4.6 Snapshotting: reflink-first, `copy_file_range` fallback

```rust
pub fn snapshot_file(src_path: &Path, dst_path: &Path) -> io::Result<bool> {
    // Try FICLONE (0x40049409) ioctl first — an instant CoW reflink on filesystems
    // that support it (btrfs, XFS with reflink, some overlay setups).
    // On failure, fall back to looping libc::copy_file_range in 16MB chunks,
    // and if THAT fails too, fall back again to std::fs::copy.
}
```

`ShardTierManager::snapshot` flushes the active bin and `sync_all`s the file first, then calls
`snapshot_file`, then writes a small plain-text manifest (`version`/`shard_id`/`file_size`/
`is_reflink`/`current_offset`) next to the backup — three fallback tiers for the actual copy,
in order of cost: reflink (instant, metadata-only) → `copy_file_range` (in-kernel copy, no
userspace round-trip) → a plain buffered `std::fs::copy`.

---

### 5. Cross-Component Interactions

- **`src/table.rs`** (Component 05): owns the `RudisValue::Tiered`/`RudisValue::Cooled`
  variants and the state-transition methods this file's callers use
  (`set_tiered_pointer`, `set_cooled_pointer`, `restore_tiered_value`, `get_value_for_spill`,
  `decommit_all_cooled`, `get_hot_keys_for_spill`, `is_tiered`, `is_cooled`); also owns
  `serialize_val_payload`/`deserialize_val_payload`, the value-encoding format this file's
  records carry as their payload.
- **`src/router.rs`** (Component 04): the actual orchestration layer —
  `spill_local`/`cool_local`/`load_local`/`stream_cold_read_local`/`decommit_local`/
  `check_auto_tier` decide *when* to call into this file's `ShardTierManager`, using the real
  `offload_threshold_pct`/`upload_threshold_pct` from `TieringStats` and `get_hot_keys_for_spill`
  to pick candidates.
- **`src/shard.rs`**: `ShardDb.tier_manager: Option<Rc<ShardTierManager>>` — one manager
  instance per shard, created during shard startup (Component 01 §4.1 step 7).
- **`src/connection.rs`** (Component 02): a `GET` on a key whose value is
  `RudisValue::Tiered`/`Cooled` falls through to `stream_cold_read_local`/`load_local` rather
  than being served directly from the table.
- **`src/main.rs`** / **`src/server.rs`**: `RUDIS_DIRECT_IO` is read as a process environment
  variable, not a CLI flag; `--maxmemory`/`--tiered-offload-threshold`/
  `--tiered-upload-threshold` (Component 01) feed `set_max_memory`/`set_offload_threshold_pct`/
  `set_upload_threshold_pct` in this file.

---

### 7. Future Improvements

- **Medium — add a `Cooled → Hot` transition (§4.4).** Today `Cooled` is a one-way permanent write-through layer: once a key has ever been tiered, every future read/write pays the bookkeeping overhead of maintaining a `TieredPointer` alongside the RAM value, even if the key becomes consistently hot again. A simple heuristic (e.g. N consecutive accesses without a re-spill, or a periodic sweep during low memory pressure) that fully promotes a long-stable `Cooled` entry back to a bare hot value — freeing the disk pointer and its GC bookkeeping — would avoid permanently taxing keys that were only cold briefly.
- **Medium — support partial-page compaction, not just whole-page GC (§4.5).** `run_gc` can only reclaim a 4KB `SmallBins` page once every record on it has been deleted; a page with one long-lived survivor among many deleted neighbors stays fully allocated indefinitely. A periodic "read the survivors, repack into a fresh page, punch the old one" compaction pass (amortized, background, rate-limited like the existing 2s GC task) would bound worst-case `dead_bytes` growth under delete-heavy small-value workloads.
- **Low — surface `O_DIRECT` fallback as a visible event, not just a silent retry (§4.1/§2.2).** An operator who sets `RUDIS_DIRECT_IO=1` expecting page-cache bypass has no way to discover the open silently fell back to buffered I/O (e.g. an unsupported filesystem) short of instrumenting the syscalls themselves. A one-time log line or a `TieringStats` flag would make this observable.
- **Low — make the 16MB write-backpressure threshold and the 2KB SmallBins cutoff configurable** (§2.5/§4.2) rather than hardcoded constants, so tiering behavior can be tuned per deployment (fast NVMe vs. slower SSD, high-value-count vs. large-value-heavy workloads) without a rebuild.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Direct I/O requires 4096-byte memory alignment for both buffer pointers and disk offsets.
* **Gotcha 2**: fallocate(FALLOC_FL_PUNCH_HOLE) reclaims freed disk space without file fragmentation.
* **Gotcha 3**: Sub-millisecond checkpoints use ioctl(FICLONE) reflink cloning on XFS/Btrfs.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
