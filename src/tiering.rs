use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::RwLock;

use crate::table::{crc64, TieredPointer};

/// Magic header for tiered disk records: "TIER"
pub const TIER_MAGIC: &[u8; 4] = b"TIER";

/// Per-port shared statistics for tiered storage
#[derive(Debug, Default)]
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
}

#[inline]
pub fn set_max_memory(port: u16, bytes: u64) {
    get_tier_stats(port).max_memory.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn get_max_memory(port: u16) -> u64 {
    get_tier_stats(port).max_memory.load(std::sync::atomic::Ordering::Relaxed)
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

pub struct ShardTierManager {
    pub shard_id: usize,
    pub port: u16,
    pub file: Rc<monoio::fs::File>,
    pub current_offset: u64,
    pub path: PathBuf,
    pub stats: Arc<TieringStats>,
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
        let current_offset = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        let stats = get_tier_stats(port);
        Ok(Self {
            shard_id,
            port,
            file: Rc::new(file),
            current_offset,
            path,
            stats,
        })
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

pub async fn read_tiered_record(
    file: &Rc<monoio::fs::File>,
    ptr: TieredPointer,
) -> io::Result<(Bytes, Vec<u8>)> {
    let len = ptr.length as usize;
    if len < 21 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "corrupt tiered record header",
        ));
    }
    let buf = Vec::with_capacity(len);
    let (res, data) = file.read_exact_at(buf, ptr.offset).await;
    res?;

    if &data[0..4] != TIER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid tier magic",
        ));
    }
    let val_type = data[4];
    if val_type != ptr.value_type {
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

        let key_len = u32::from_le_bytes(record[5..9].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(record[9..13].try_into().unwrap()) as usize;
        assert_eq!(key_len, key.len());
        assert_eq!(val_len, val.len());

        let expected_crc = u64::from_le_bytes(record[13..21].try_into().unwrap());
        let body = &record[21..21 + key_len + val_len];
        let actual_crc = crc64(body);
        assert_eq!(expected_crc, actual_crc);

        assert_eq!(&record[21..21 + key_len], key);
        assert_eq!(&record[21 + key_len..21 + key_len + val_len], val);
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
}
