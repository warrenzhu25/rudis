use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

use crate::table::{crc64, TieredPointer};

/// Magic header for tiered disk records: "TIER"
pub const TIER_MAGIC: &[u8; 4] = b"TIER";

/// Direct I/O and SmallBins page size (4 KB)
pub const PAGE_SIZE: usize = 4096;

/// Threshold below which records are packed into 4KB SmallBins
pub const SMALL_VALUE_LIMIT: usize = 2048;

/// Per-port shared statistics for tiered storage
#[derive(Debug)]
pub struct TieringStats {
    pub tiered_keys: AtomicU64,
    pub tiered_bytes: AtomicU64,
    pub ram_saved_bytes: AtomicU64,
    pub disk_reads: AtomicU64,
    pub disk_writes: AtomicU64,
    pub dead_bytes: AtomicU64,
    pub cooled_keys: AtomicU64,
    pub decommit_count: AtomicU64,
    pub max_memory: AtomicU64,
    pub ram_hits: AtomicU64,
    pub ram_misses: AtomicU64,
    pub total_stashes: AtomicU64,
    pub total_fetches: AtomicU64,
    pub total_deletes: AtomicU64,
    pub coalesced_reads: AtomicU64,
    pub bin_pages: AtomicU64,
    pub streaming_reads: AtomicU64,
    pub offload_threshold_pct: AtomicU64, // e.g. 60 (trigger background offload when memory >= 60% maxmemory)
    pub upload_threshold_pct: AtomicU64,  // e.g. 80 (stream cold reads without promotion when memory >= 80% maxmemory)
}

impl Default for TieringStats {
    fn default() -> Self {
        Self {
            tiered_keys: AtomicU64::new(0),
            tiered_bytes: AtomicU64::new(0),
            ram_saved_bytes: AtomicU64::new(0),
            disk_reads: AtomicU64::new(0),
            disk_writes: AtomicU64::new(0),
            dead_bytes: AtomicU64::new(0),
            cooled_keys: AtomicU64::new(0),
            decommit_count: AtomicU64::new(0),
            max_memory: AtomicU64::new(0),
            ram_hits: AtomicU64::new(0),
            ram_misses: AtomicU64::new(0),
            total_stashes: AtomicU64::new(0),
            total_fetches: AtomicU64::new(0),
            total_deletes: AtomicU64::new(0),
            coalesced_reads: AtomicU64::new(0),
            bin_pages: AtomicU64::new(0),
            streaming_reads: AtomicU64::new(0),
            offload_threshold_pct: AtomicU64::new(60),
            upload_threshold_pct: AtomicU64::new(80),
        }
    }
}

#[inline]
pub fn set_max_memory(port: u16, bytes: u64) {
    get_tier_stats(port).max_memory.store(bytes, Ordering::Relaxed);
}

#[inline]
pub fn get_max_memory(port: u16) -> u64 {
    get_tier_stats(port).max_memory.load(Ordering::Relaxed)
}

#[inline]
pub fn set_offload_threshold_pct(port: u16, pct: u64) {
    get_tier_stats(port).offload_threshold_pct.store(pct.min(100), Ordering::Relaxed);
}

#[inline]
pub fn get_offload_threshold_pct(port: u16) -> u64 {
    get_tier_stats(port).offload_threshold_pct.load(Ordering::Relaxed)
}

#[inline]
pub fn set_upload_threshold_pct(port: u16, pct: u64) {
    get_tier_stats(port).upload_threshold_pct.store(pct.min(100), Ordering::Relaxed);
}

#[inline]
pub fn get_upload_threshold_pct(port: u16) -> u64 {
    get_tier_stats(port).upload_threshold_pct.load(Ordering::Relaxed)
}

pub fn format_bytes_human(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2}K", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2}M", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

pub fn parse_memory_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let lower = s.to_lowercase();
    let (num_part, multiplier) = if lower.ends_with("gb") {
        (&lower[..lower.len() - 2], 1024 * 1024 * 1024)
    } else if lower.ends_with('g') {
        (&lower[..lower.len() - 1], 1024 * 1024 * 1024)
    } else if lower.ends_with("mb") {
        (&lower[..lower.len() - 2], 1024 * 1024)
    } else if lower.ends_with('m') {
        (&lower[..lower.len() - 1], 1024 * 1024)
    } else if lower.ends_with("kb") {
        (&lower[..lower.len() - 2], 1024)
    } else if lower.ends_with('k') {
        (&lower[..lower.len() - 1], 1024)
    } else if lower.ends_with('b') {
        (&lower[..lower.len() - 1], 1)
    } else {
        (lower.as_str(), 1)
    };
    num_part.trim().parse::<u64>().ok().map(|n| n * multiplier)
}

static TIER_STATS: RwLock<Option<HashMap<u16, Arc<TieringStats>>>> = RwLock::new(None);

pub fn get_tier_stats(port: u16) -> Arc<TieringStats> {
    let mut map = TIER_STATS.write().unwrap();
    let entry = map.get_or_insert_with(HashMap::new);
    entry
        .entry(port)
        .or_insert_with(|| Arc::new(TieringStats::default()))
        .clone()
}

pub fn reset_tier_stats(port: u16) {
    let mut map = TIER_STATS.write().unwrap();
    if let Some(map) = map.as_mut() {
        map.remove(&port);
    }
}

/// Dragonfly-inspired OpManager: Read Coalescing and In-Flight Stash Protection
pub struct OpManager {
    /// In-flight page reads: page_start offset -> list of subscriber responder channels
    pub in_flight_reads: RefCell<HashMap<u64, Vec<flume::Sender<Result<Rc<Vec<u8>>, String>>>>>,
    /// Tracks in-flight stashes by key to prevent stale overwrites on DEL/SET
    pub pending_stashes: RefCell<HashSet<Bytes>>,
    /// Tracks total in-flight stash bytes for write backpressure
    pub pending_stash_bytes: AtomicUsize,
}

impl OpManager {
    pub fn new() -> Self {
        Self {
            in_flight_reads: RefCell::new(HashMap::new()),
            pending_stashes: RefCell::new(HashSet::new()),
            pending_stash_bytes: AtomicUsize::new(0),
        }
    }

    pub fn start_pending_stash(&self, key: &Bytes, size: usize) {
        self.pending_stashes.borrow_mut().insert(key.clone());
        self.pending_stash_bytes.fetch_add(size, Ordering::Relaxed);
    }

    pub fn cancel_pending_stash(&self, key: &[u8]) -> bool {
        self.pending_stashes.borrow_mut().remove(key)
    }

    pub fn finish_pending_stash(&self, key: &[u8], size: usize) -> bool {
        let existed = self.pending_stashes.borrow_mut().remove(key);
        self.pending_stash_bytes.fetch_sub(size, Ordering::Relaxed);
        existed
    }

    pub fn is_stash_pending(&self, key: &[u8]) -> bool {
        self.pending_stashes.borrow().contains(key)
    }

    pub fn check_write_backpressure(&self) -> bool {
        self.pending_stash_bytes.load(Ordering::Relaxed) > 16 * 1024 * 1024
    }

    /// Read a 4KB page from disk with coalescing.
    /// If multiple reads request the same 4KB page simultaneously, they share a single
    /// disk read operation and a single DMA buffer.
    pub async fn read_page_coalesced(
        &self,
        file: &Rc<monoio::fs::File>,
        page_start: u64,
        stats: &TieringStats,
    ) -> Result<Rc<Vec<u8>>, io::Error> {
        let (rx, _is_initiator) = {
            let mut in_flight = self.in_flight_reads.borrow_mut();
            if let Some(waiters) = in_flight.get_mut(&page_start) {
                stats.coalesced_reads.fetch_add(1, Ordering::Relaxed);
                let (tx, rx) = flume::bounded(1);
                waiters.push(tx);
                (Some(rx), false)
            } else {
                in_flight.insert(page_start, Vec::new());
                (None, true)
            }
        };

        if let Some(rx) = rx {
            let res = rx
                .recv_async()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            return res.map_err(|e| io::Error::new(io::ErrorKind::Other, e));
        }

        // Initiator executes the physical read
        let buf = vec![0u8; PAGE_SIZE];
        let (res, data) = file.read_exact_at(buf, page_start).await;
        let result = match res {
            Ok(()) => {
                stats.disk_reads.fetch_add(1, Ordering::Relaxed);
                Ok(Rc::new(data))
            }
            Err(e) => Err(e.to_string()),
        };

        let waiters = self
            .in_flight_reads
            .borrow_mut()
            .remove(&page_start)
            .unwrap_or_default();
        for tx in waiters {
            let _ = tx.send(result.clone());
        }

        result.map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

/// Metadata item within a SmallBin 4KB page
#[derive(Clone, Debug)]
pub struct SmallBinItem {
    pub key: Bytes,
    pub offset_in_page: u16,
    pub length: u32,
    pub value_type: u8,
}

/// An active in-memory 4096-byte SmallBin accumulator
pub struct ActiveBin {
    pub page_index: u64,
    pub buffer: Vec<u8>,
    pub items: Vec<SmallBinItem>,
}

impl ActiveBin {
    pub fn new(page_index: u64) -> Self {
        Self {
            page_index,
            buffer: Vec::with_capacity(PAGE_SIZE),
            items: Vec::new(),
        }
    }

    pub fn can_fit(&self, record_len: usize) -> bool {
        self.buffer.len() + record_len <= PAGE_SIZE
    }

    pub fn append(&mut self, key: Bytes, record: &[u8], val_type: u8) -> SmallBinItem {
        let offset = self.buffer.len() as u16;
        let length = record.len() as u32;
        self.buffer.extend_from_slice(record);
        let item = SmallBinItem {
            key,
            offset_in_page: offset,
            length,
            value_type: val_type,
        };
        self.items.push(item.clone());
        item
    }

    pub fn seal(mut self) -> (Vec<u8>, Vec<SmallBinItem>) {
        if self.buffer.len() < PAGE_SIZE {
            self.buffer.resize(PAGE_SIZE, 0);
        }
        (self.buffer, self.items)
    }
}

/// Manages active small bin aggregation and page occupancy tracking
pub struct SmallBinsManager {
    pub active_bin: Option<ActiveBin>,
    pub page_active_counts: HashMap<u64, usize>,
}

impl SmallBinsManager {
    pub fn new() -> Self {
        Self {
            active_bin: None,
            page_active_counts: HashMap::new(),
        }
    }

    pub fn decrement_page_key(&mut self, page_index: u64, stats: &TieringStats) {
        if let Some(count) = self.page_active_counts.get_mut(&page_index) {
            if *count > 0 {
                *count -= 1;
                if *count == 0 {
                    stats.dead_bytes.fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
                }
            }
        }
    }
}

pub struct ShardTierManager {
    pub shard_id: usize,
    pub port: u16,
    pub file: Rc<monoio::fs::File>,
    pub current_offset: Cell<u64>,
    pub path: PathBuf,
    pub stats: Arc<TieringStats>,
    pub op_manager: Rc<OpManager>,
    pub small_bins: RefCell<SmallBinsManager>,
}

impl ShardTierManager {
    pub async fn open(shard_id: usize, port: u16, dir: &Path) -> io::Result<Self> {
        let _ = std::fs::create_dir_all(dir);
        let path = dir.join(format!("tier_shard_{}.db", shard_id));
        let file = monoio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;
        let raw_len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        // Align offset to 4KB page boundary
        let current_offset = (raw_len + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64 * PAGE_SIZE as u64;
        let stats = get_tier_stats(port);
        Ok(Self {
            shard_id,
            port,
            file: Rc::new(file),
            current_offset: Cell::new(current_offset),
            path,
            stats,
            op_manager: Rc::new(OpManager::new()),
            small_bins: RefCell::new(SmallBinsManager::new()),
        })
    }

    /// Stash a single record onto disk.
    /// Values < 2048 bytes are packed into 4096-byte SmallBins with direct I/O alignment.
    /// Values >= 2048 bytes flush the active bin and write in aligned 4096-byte blocks.
    pub async fn stash_record(
        &self,
        key: &Bytes,
        val_payload: &[u8],
        val_type: u8,
    ) -> io::Result<TieredPointer> {
        if self.op_manager.check_write_backpressure() {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "write backpressure: stash buffer full"));
        }

        let record = encode_tiered_record(key, val_payload, val_type);
        let record_len = record.len();

        self.op_manager.start_pending_stash(key, record_len);

        let ptr_res = if record_len < SMALL_VALUE_LIMIT {
            let need_new_bin = {
                let bins = self.small_bins.borrow();
                match &bins.active_bin {
                    Some(ab) => !ab.can_fit(record_len),
                    None => true,
                }
            };

            if need_new_bin {
                self.flush_active_bin().await?;
                let next_page_idx = self.current_offset.get() / PAGE_SIZE as u64;
                self.current_offset.set(self.current_offset.get() + PAGE_SIZE as u64);
                self.small_bins.borrow_mut().active_bin = Some(ActiveBin::new(next_page_idx));
            }

            let (ptr, should_flush) = {
                let mut bins = self.small_bins.borrow_mut();
                let ab = bins.active_bin.as_mut().unwrap();
                let page_idx = ab.page_index;
                let item = ab.append(key.clone(), &record, val_type);
                let should_flush = !ab.can_fit(64);
                let ptr = TieredPointer {
                    file_id: self.shard_id as u32,
                    offset: page_idx * PAGE_SIZE as u64 + item.offset_in_page as u64,
                    length: item.length,
                    value_type: item.value_type,
                };
                (ptr, should_flush)
            };

            if should_flush {
                self.flush_active_bin().await?;
            }

            self.stats.total_stashes.fetch_add(1, Ordering::Relaxed);
            Ok(ptr)
        } else {
            // Large record (>= 2KB)
            self.flush_active_bin().await?;

            let aligned_len = (record_len + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;
            let mut write_buf = record;
            if write_buf.len() < aligned_len {
                write_buf.resize(aligned_len, 0);
            }
            let offset = self.current_offset.get();
            let (res, _) = self.file.write_all_at(write_buf, offset).await;
            res?;
            self.current_offset.set(offset + aligned_len as u64);
            self.stats.disk_writes.fetch_add(1, Ordering::Relaxed);
            self.stats.total_stashes.fetch_add(1, Ordering::Relaxed);

            let ptr = TieredPointer {
                file_id: self.shard_id as u32,
                offset,
                length: record_len as u32,
                value_type: val_type,
            };
            Ok(ptr)
        };

        self.op_manager.finish_pending_stash(key, record_len);
        ptr_res
    }

    pub async fn flush_active_bin(&self) -> io::Result<()> {
        let (page_buf, page_idx, items_len) = {
            let mut bins = self.small_bins.borrow_mut();
            if let Some(ab) = bins.active_bin.take() {
                let page_idx = ab.page_index;
                let (page_buf, items) = ab.seal();
                (Some(page_buf), page_idx, items.len())
            } else {
                (None, 0, 0)
            }
        };
        if let Some(page_buf) = page_buf {
            let page_offset = page_idx * PAGE_SIZE as u64;
            let (res, _) = self.file.write_all_at(page_buf, page_offset).await;
            res?;
            self.stats.disk_writes.fetch_add(1, Ordering::Relaxed);
            self.stats.bin_pages.fetch_add(1, Ordering::Relaxed);
            self.small_bins.borrow_mut().page_active_counts.insert(page_idx, items_len);
        }
        Ok(())
    }

    pub fn on_key_deleted(&self, ptr: TieredPointer) {
        let page_index = ptr.offset / PAGE_SIZE as u64;
        self.small_bins.borrow_mut().decrement_page_key(page_index, &self.stats);
        self.stats.dead_bytes.fetch_add(ptr.length as u64, Ordering::Relaxed);
    }
}

pub fn encode_tiered_record(key: &[u8], val_payload: &[u8], val_type: u8) -> Vec<u8> {
    let key_len = key.len() as u32;
    let val_len = val_payload.len() as u32;
    let total_len = 4 + 1 + 4 + 4 + 8 + key.len() + val_payload.len();
    let mut buf = Vec::with_capacity(total_len);

    // 1. Magic (4 bytes)
    buf.extend_from_slice(TIER_MAGIC);
    // 2. Value type (1 byte)
    buf.push(val_type);
    // 3. Key length (4 bytes)
    buf.extend_from_slice(&key_len.to_le_bytes());
    // 4. Value length (4 bytes)
    buf.extend_from_slice(&val_len.to_le_bytes());

    // 5. CRC64 (8 bytes) over key + val_payload
    let mut crc_data = Vec::with_capacity(key.len() + val_payload.len());
    crc_data.extend_from_slice(key);
    crc_data.extend_from_slice(val_payload);
    let crc = crc64(&crc_data);
    buf.extend_from_slice(&crc.to_le_bytes());

    // 6. Key and Payload
    buf.extend_from_slice(key);
    buf.extend_from_slice(val_payload);
    buf
}

pub fn decode_tiered_record(data: &[u8], expected_val_type: u8) -> io::Result<(Bytes, Vec<u8>)> {
    if data.len() < 21 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "corrupt tiered record header",
        ));
    }
    if &data[0..4] != TIER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid tier magic",
        ));
    }
    let val_type = data[4];
    if val_type != expected_val_type {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "value type mismatch",
        ));
    }
    let key_len = u32::from_le_bytes(data[5..9].try_into().unwrap()) as usize;
    let val_len = u32::from_le_bytes(data[9..13].try_into().unwrap()) as usize;
    let expected_crc = u64::from_le_bytes(data[13..21].try_into().unwrap());

    if 21 + key_len + val_len > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "corrupt record length",
        ));
    }

    let body = &data[21..21 + key_len + val_len];
    let actual_crc = crc64(body);
    if actual_crc != expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "crc mismatch on tiered read",
        ));
    }

    let key = Bytes::copy_from_slice(&data[21..21 + key_len]);
    let val_payload = data[21 + key_len..21 + key_len + val_len].to_vec();
    Ok((key, val_payload))
}

pub async fn read_tiered_record(
    file: &Rc<monoio::fs::File>,
    op_manager: &OpManager,
    small_bins: Option<&RefCell<SmallBinsManager>>,
    ptr: TieredPointer,
    stats: &TieringStats,
) -> io::Result<(Bytes, Vec<u8>)> {
    let len = ptr.length as usize;
    if len < 21 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "corrupt tiered record header",
        ));
    }

    let offset_in_page = (ptr.offset % PAGE_SIZE as u64) as usize;
    let data: Vec<u8> = if offset_in_page + len <= PAGE_SIZE {
        let page_start = (ptr.offset / PAGE_SIZE as u64) * PAGE_SIZE as u64;
        let page_idx = ptr.offset / PAGE_SIZE as u64;

        let in_mem = if let Some(sb) = small_bins {
            let bins = sb.borrow();
            if let Some(ab) = &bins.active_bin {
                if ab.page_index == page_idx && ab.buffer.len() >= offset_in_page + len {
                    Some(ab.buffer[offset_in_page..offset_in_page + len].to_vec())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(d) = in_mem {
            d
        } else {
            let page_rc = op_manager.read_page_coalesced(file, page_start, stats).await?;
            page_rc[offset_in_page..offset_in_page + len].to_vec()
        }
    } else {
        let buf = Vec::with_capacity(len);
        let (res, data) = file.read_exact_at(buf, ptr.offset).await;
        res?;
        stats.disk_reads.fetch_add(1, Ordering::Relaxed);
        data
    };

    decode_tiered_record(&data, ptr.value_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_record_integrity() {
        let key = b"user:profile:1000";
        let val = b"{\"name\":\"alice\",\"score\":9999,\"metadata\":[1,2,3,4]}";
        let val_type = 0u8;

        let record = encode_tiered_record(key, val, val_type);
        assert!(record.len() > 21);
        assert_eq!(&record[0..4], TIER_MAGIC);
        assert_eq!(record[4], val_type);

        let (decoded_key, decoded_val) = decode_tiered_record(&record, val_type).unwrap();
        assert_eq!(decoded_key, Bytes::copy_from_slice(key));
        assert_eq!(decoded_val, val);
    }

    #[test]
    fn test_max_memory_parsing_and_formatting() {
        assert_eq!(parse_memory_bytes("1024"), Some(1024));
        assert_eq!(parse_memory_bytes("64k"), Some(64 * 1024));
        assert_eq!(parse_memory_bytes("128KB"), Some(128 * 1024));
        assert_eq!(parse_memory_bytes("16m"), Some(16 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("2MB"), Some(2 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("1g"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("4GB"), Some(4 * 1024 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("invalid"), None);

        assert_eq!(format_bytes_human(500), "500B");
        assert_eq!(format_bytes_human(2048), "2.00K");
        assert_eq!(format_bytes_human(10 * 1024 * 1024), "10.00M");
        assert_eq!(format_bytes_human(2 * 1024 * 1024 * 1024), "2.00G");

        set_max_memory(0, 50 * 1024 * 1024);
        assert_eq!(get_max_memory(0), 50 * 1024 * 1024);
        set_max_memory(0, 0);
        assert_eq!(get_max_memory(0), 0);
    }

    #[test]
    fn test_small_bins_active_bin_packing() {
        let mut ab = ActiveBin::new(0);
        let rec1 = encode_tiered_record(b"k1", b"val1", 0);
        let rec2 = encode_tiered_record(b"k2", b"val2", 0);

        assert!(ab.can_fit(rec1.len()));
        let item1 = ab.append(Bytes::from("k1"), &rec1, 0);
        assert_eq!(item1.offset_in_page, 0);
        assert_eq!(item1.length as usize, rec1.len());

        assert!(ab.can_fit(rec2.len()));
        let item2 = ab.append(Bytes::from("k2"), &rec2, 0);
        assert_eq!(item2.offset_in_page as usize, rec1.len());

        let (page_buf, items) = ab.seal();
        assert_eq!(page_buf.len(), PAGE_SIZE);
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_op_manager_in_flight_and_backpressure() {
        let op_mgr = OpManager::new();
        let key = Bytes::from_static(b"mykey");
        assert!(!op_mgr.is_stash_pending(&key));
        assert!(!op_mgr.check_write_backpressure());

        op_mgr.start_pending_stash(&key, 100);
        assert!(op_mgr.is_stash_pending(&key));
        assert_eq!(op_mgr.pending_stash_bytes.load(Ordering::Relaxed), 100);

        // Cancel pending stash
        assert!(op_mgr.cancel_pending_stash(&key));
        assert!(!op_mgr.is_stash_pending(&key));

        // Start pending stash up to backpressure limit (> 16MB)
        let big_key = Bytes::from_static(b"big1");
        op_mgr.start_pending_stash(&big_key, 17 * 1024 * 1024);
        assert!(op_mgr.check_write_backpressure());

        // Finish pending stash relieves backpressure
        op_mgr.finish_pending_stash(&big_key, 17 * 1024 * 1024);
        assert!(!op_mgr.check_write_backpressure());
    }
}
