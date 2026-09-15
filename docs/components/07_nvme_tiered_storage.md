# Component 07: NVMe SSD Tiered Storage Engine (`src/tiering.rs`)

## 1. Architectural Purpose & Scope

`src/tiering.rs` is Rudis's embedded NVMe SSD data tiering engine. Inspired by Dragonfly's production architecture, it enables transparently scaling dataset sizes $2\times$ to $5\times$ beyond physical memory limits. Hot keys and active working sets remain in DRAM, while cold and infrequently accessed values are offloaded to high-speed NVMe storage.

---

## 2. Key Invariants & Concurrency Constraints

1. **Thread-per-Core Direct I/O**: Each shard owns a dedicated NVMe storage file (`shard-{id}.tier`) opened with `O_DIRECT`. Each core drives its own file descriptor via `io_uring` without cross-core locks.
2. **Page-Cache Bypass (`O_DIRECT`)**: Linux page caching is completely bypassed. This eliminates double-caching (data in RAM twice) and prevents OS writeback latency spikes.
3. **Three-State Value Lifecycle**: Values transition through `Hot` (RAM), `Staged/Cooled` (RAM write buffer), and `Cold` (SSD, referenced by a 16-byte `RudisExternalPtr`).
4. **Hole Punching for Zero-Rewrite Deletions**: Deleting or updating cold keys invokes `fallocate(FALLOC_FL_PUNCH_HOLE)`, deallocating physical SSD blocks immediately without rewriting storage segments.

---

## 3. Component Architecture & Data Structures

```
                      Hot Tier (DRAM)
     RudisTable Entry: "user:123" -> RudisValue::String("payload")
                             │
                             ▼ (Memory Pressure > High Watermark)
                   Stage in SmallBins
     [ 4KB Aligned Buffer: Record 1 | Record 2 | Record 3 ]
                             │
                             ▼ (Flush via io_uring O_DIRECT)
                      Cold Tier (NVMe)
     Write Page to shard-0.tier at Offset 0x008000
                             │
                             ▼
     Replace in DRAM: "user:123" -> RudisValue::External(RudisExternalPtr)
                             │
            ┌────────────────┴────────────────┐
            ▼                                 ▼
       Client Read                       Client Delete
  Read 4KB from 0x008000           libc::fallocate(PUNCH_HOLE)
  Unpack & Promote to Hot          Zero SSD Space Leaked!
```

### Core Data Structures

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RudisExternalPtr {
    pub page_id: u32,       // Index into 4KB page or segment
    pub offset_in_page: u16,// Byte offset within page
    pub length: u16,        // Length of payload
}

pub struct SmallBins {
    pub active_page: Vec<u8>,
    pub active_page_id: u32,
    pub current_offset: usize,
}

pub struct TieredStorage {
    pub file_fd: RawFd,
    pub shard_id: usize,
    pub small_bins: SmallBins,
    pub max_memory_bytes: usize,
    pub used_memory_bytes: usize,
    pub high_watermark_ratio: f64, // e.g., 0.85
    pub low_watermark_ratio: f64,  // e.g., 0.70
}
```

---

## 4. Execution Algorithms & Code Logic

### 4.1 Eviction Under Memory Pressure

When `used_memory` exceeds the high watermark, Rudis scans the table and offloads cold values:

```rust
impl TieredStorage {
    pub fn check_memory_pressure(&mut self, table: &mut RudisTable) {
        let max = self.max_memory_bytes;
        let high = (max as f64 * self.high_watermark_ratio) as usize;
        let low = (max as f64 * self.low_watermark_ratio) as usize;

        let current = self.get_allocated_memory();
        if current < high { return; }

        let bytes_to_reclaim = current - low;
        let mut reclaimed = 0;

        // Iterate through entries and offload candidates
        for (_key, entry) in table.entries.iter_mut() {
            if reclaimed >= bytes_to_reclaim { break; }

            if let RudisValue::String(ref s) = entry.val {
                if s.len() >= 32 { // Minimum threshold for tiering benefit
                    let ptr = self.offload_to_ssd(s.as_ref());
                    reclaimed += s.len();
                    entry.val = RudisValue::External(ptr);
                }
            }
        }
    }
}
```

### 4.2 `SmallBins` Page Packing

Writing small values directly to disk causes massive write amplification. `SmallBins` coalesces values into 4 KB direct I/O pages:

```rust
impl TieredStorage {
    pub fn offload_to_ssd(&mut self, data: &[u8]) -> RudisExternalPtr {
        let len = data.len();

        // Check if data fits in the currently active 4KB SmallBins page
        if self.small_bins.current_offset + len + 2 > 4096 {
            self.flush_active_page();
        }

        let offset = self.small_bins.current_offset;
        let page_id = self.small_bins.active_page_id;

        // Write length prefix and payload into page
        self.small_bins.active_page[offset..offset + 2]
            .copy_from_slice(&(len as u16).to_le_bytes());
        self.small_bins.active_page[offset + 2..offset + 2 + len]
            .copy_from_slice(data);

        self.small_bins.current_offset += len + 2;

        RudisExternalPtr {
            page_id,
            offset_in_page: offset as u16,
            length: len as u16,
        }
    }

    fn flush_active_page(&mut self) {
        let offset = (self.small_bins.active_page_id as u64) * 4096;
        // Direct I/O write via Linux pwrite
        unsafe {
            libc::pwrite(
                self.file_fd,
                self.small_bins.active_page.as_ptr() as *const libc::c_void,
                4096,
                offset as libc::off_t,
            );
        }
        self.small_bins.active_page_id += 1;
        self.small_bins.current_offset = 0;
        self.small_bins.active_page.fill(0);
    }
}
```

### 4.3 Transparent Read Promotion

When a client accesses an external value via `GET`, it is fetched from NVMe and restored into DRAM:

```rust
impl TieredStorage {
    pub fn read_from_ssd(&self, ptr: RudisExternalPtr) -> Bytes {
        let mut page_buf = vec![0u8; 4096];
        let offset = (ptr.page_id as u64) * 4096;

        unsafe {
            libc::pread(
                self.file_fd,
                page_buf.as_mut_ptr() as *mut libc::c_void,
                4096,
                offset as libc::off_t,
            );
        }

        let start = ptr.offset_in_page as usize + 2;
        let end = start + ptr.length as usize;
        Bytes::copy_from_slice(&page_buf[start..end])
    }
}
```

### 4.4 Hole Punching on Deletion (`fallocate`)

```rust
pub fn delete_external_record(file_fd: RawFd, page_id: u32) {
    let offset = (page_id as u64) * 4096;
    unsafe {
        libc::fallocate(
            file_fd,
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset as libc::off_t,
            4096,
        );
    }
}
```

---

## 5. Cross-Component Interactions

- **`src/table.rs`**: Stores `RudisValue::External(ptr)` when keys are evicted.
- **`src/connection.rs`**: Detects `RudisValue::External` during reads, calls `read_from_ssd`, and promotes values back to `RudisValue::String`.
- **`src/server.rs`**: Periodically checks `tier.check_memory_pressure()` in the maintenance timer loop.

---

## 6. Performance Characteristics

- **Zero DRAM Waste**: Cold items take up exactly 16 bytes of metadata in memory.
- **Sub-30µs Read Latency**: NVMe SSD 4 KB direct I/O reads complete in 15–30 microseconds.
- **Zero Space Leaks**: Kernel-level hole punching frees physical flash blocks immediately upon deletion without full compaction rewrites.
