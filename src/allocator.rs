/// Memory Allocator profiling and telemetry module using tikv-jemalloc-ctl.

#[derive(Debug, Clone, Copy, Default)]
pub struct AllocatorStats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub metadata: usize,
    pub mapped: usize,
    pub fragmentation_ratio: f64,
}

/// Query real-time jemalloc allocator statistics.
pub fn get_allocator_stats() -> AllocatorStats {
    let _ = tikv_jemalloc_ctl::epoch::advance();
    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0);
    let active = tikv_jemalloc_ctl::stats::active::read().unwrap_or(0);
    let resident = tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0);
    let metadata = tikv_jemalloc_ctl::stats::metadata::read().unwrap_or(0);
    let mapped = tikv_jemalloc_ctl::stats::mapped::read().unwrap_or(0);

    let fragmentation_ratio = if allocated > 0 {
        resident as f64 / allocated as f64
    } else {
        1.0
    };

    AllocatorStats {
        allocated,
        active,
        resident,
        metadata,
        mapped,
        fragmentation_ratio,
    }
}

/// Format memory section for INFO command.
pub fn format_memory_info(
    used_mem: usize,
    max_mem: u64,
    cooled_keys: u64,
    tiered_keys: u64,
) -> String {
    let stats = get_allocator_stats();
    let rss = if stats.resident > 0 {
        stats.resident
    } else {
        used_mem
    };
    let frag = if stats.allocated > 0 {
        stats.fragmentation_ratio
    } else {
        1.00
    };

    format!(
        "# Memory\r\n\
        used_memory:{}\r\n\
        used_memory_human:{}\r\n\
        used_memory_rss:{}\r\n\
        used_memory_rss_human:{}\r\n\
        maxmemory:{}\r\n\
        maxmemory_human:{}\r\n\
        mem_fragmentation_ratio:{:.2}\r\n\
        mem_allocator:libc\r\n\
        allocator_allocated:{}\r\n\
        allocator_active:{}\r\n\
        allocator_resident:{}\r\n\
        allocator_metadata:{}\r\n\
        allocator_mapped:{}\r\n\
        cooled_keys:{}\r\n\
        tiered_keys:{}\r\n",
        used_mem,
        crate::tiering::format_bytes_human(used_mem as u64),
        rss,
        crate::tiering::format_bytes_human(rss as u64),
        max_mem,
        crate::tiering::format_bytes_human(max_mem),
        frag,
        stats.allocated,
        stats.active,
        stats.resident,
        stats.metadata,
        stats.mapped,
        cooled_keys,
        tiered_keys,
    )
}

use bytes::Bytes;
use std::collections::VecDeque;

pub const MAX_ARENA_POOLED: usize = 1024;

/// High-performance thread-local slab & arena pool for small collections.
/// Eliminates allocator locks and reduces fragmentation for high-frequency
/// List (LPUSH/LPOP), Hash (HSET/HDEL), Set (SADD/SREM), and ZSet (ZADD/ZREM) workloads.
#[derive(Debug, Default)]
pub struct SmallCollectionArena {
    list_pool: Vec<VecDeque<Bytes>>,
    hash_pool: Vec<Vec<(Bytes, Bytes)>>,
    set_pool: Vec<Vec<Bytes>>,
    zset_pool: Vec<Vec<(crate::table::OrderedScore, Bytes)>>,
    pub allocations_saved: u64,
    pub recycles_count: u64,
}

impl SmallCollectionArena {
    pub fn new() -> Self {
        Self {
            list_pool: Vec::with_capacity(128),
            hash_pool: Vec::with_capacity(128),
            set_pool: Vec::with_capacity(128),
            zset_pool: Vec::with_capacity(128),
            allocations_saved: 0,
            recycles_count: 0,
        }
    }

    #[inline(always)]
    pub fn acquire_list(&mut self, min_cap: usize) -> VecDeque<Bytes> {
        if let Some(mut d) = self.list_pool.pop() {
            self.allocations_saved += 1;
            if d.capacity() < min_cap {
                d.reserve(min_cap - d.capacity());
            }
            d
        } else {
            VecDeque::with_capacity(min_cap.max(16))
        }
    }

    #[inline(always)]
    pub fn recycle_list(&mut self, mut d: VecDeque<Bytes>) {
        d.clear();
        if d.capacity() <= 512 && self.list_pool.len() < MAX_ARENA_POOLED {
            self.recycles_count += 1;
            self.list_pool.push(d);
        }
    }

    #[inline(always)]
    pub fn acquire_small_hash(&mut self, min_cap: usize) -> Vec<(Bytes, Bytes)> {
        if let Some(mut v) = self.hash_pool.pop() {
            self.allocations_saved += 1;
            if v.capacity() < min_cap {
                v.reserve(min_cap - v.capacity());
            }
            v
        } else {
            Vec::with_capacity(min_cap.max(8))
        }
    }

    #[inline(always)]
    pub fn recycle_small_hash(&mut self, mut v: Vec<(Bytes, Bytes)>) {
        v.clear();
        if v.capacity() <= 512 && self.hash_pool.len() < MAX_ARENA_POOLED {
            self.recycles_count += 1;
            self.hash_pool.push(v);
        }
    }

    #[inline(always)]
    pub fn acquire_small_set(&mut self, min_cap: usize) -> Vec<Bytes> {
        if let Some(mut v) = self.set_pool.pop() {
            self.allocations_saved += 1;
            if v.capacity() < min_cap {
                v.reserve(min_cap - v.capacity());
            }
            v
        } else {
            Vec::with_capacity(min_cap.max(16))
        }
    }

    #[inline(always)]
    pub fn recycle_small_set(&mut self, mut v: Vec<Bytes>) {
        v.clear();
        if v.capacity() <= 512 && self.set_pool.len() < MAX_ARENA_POOLED {
            self.recycles_count += 1;
            self.set_pool.push(v);
        }
    }

    #[inline(always)]
    pub fn acquire_small_zset(
        &mut self,
        min_cap: usize,
    ) -> Vec<(crate::table::OrderedScore, Bytes)> {
        if let Some(mut v) = self.zset_pool.pop() {
            self.allocations_saved += 1;
            if v.capacity() < min_cap {
                v.reserve(min_cap - v.capacity());
            }
            v
        } else {
            Vec::with_capacity(min_cap.max(16))
        }
    }

    #[inline(always)]
    pub fn recycle_small_zset(&mut self, mut v: Vec<(crate::table::OrderedScore, Bytes)>) {
        v.clear();
        if v.capacity() <= 512 && self.zset_pool.len() < MAX_ARENA_POOLED {
            self.recycles_count += 1;
            self.zset_pool.push(v);
        }
    }

    #[inline(always)]
    pub fn pool_stats(&self) -> (usize, usize, usize, usize) {
        (
            self.list_pool.len(),
            self.hash_pool.len(),
            self.set_pool.len(),
            self.zset_pool.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{OrderedScore, RudisTable, ZAddFlags};

    #[test]
    fn test_small_collection_arena_lifecycle() {
        let mut arena = SmallCollectionArena::new();
        assert_eq!(arena.pool_stats(), (0, 0, 0, 0));

        // 1. List pool
        let mut d = arena.acquire_list(10);
        d.push_back(Bytes::from_static(b"v1"));
        d.push_front(Bytes::from_static(b"v0"));
        assert_eq!(d.len(), 2);
        arena.recycle_list(d);
        assert_eq!(arena.recycles_count, 1);
        assert_eq!(arena.pool_stats().0, 1);

        let d2 = arena.acquire_list(5);
        assert_eq!(arena.allocations_saved, 1);
        assert!(d2.is_empty());
        assert!(d2.capacity() >= 10);
        assert_eq!(arena.pool_stats().0, 0);

        // 2. Small Hash pool
        let mut h = arena.acquire_small_hash(4);
        h.push((Bytes::from_static(b"f1"), Bytes::from_static(b"v1")));
        arena.recycle_small_hash(h);
        assert_eq!(arena.recycles_count, 2);
        assert_eq!(arena.pool_stats().1, 1);

        let h2 = arena.acquire_small_hash(2);
        assert_eq!(arena.allocations_saved, 2);
        assert!(h2.is_empty());
        assert_eq!(arena.pool_stats().1, 0);

        // 3. Set pool
        let mut s = arena.acquire_small_set(8);
        s.push(Bytes::from_static(b"m1"));
        arena.recycle_small_set(s);
        assert_eq!(arena.recycles_count, 3);
        assert_eq!(arena.pool_stats().2, 1);

        let s2 = arena.acquire_small_set(4);
        assert_eq!(arena.allocations_saved, 3);
        assert!(s2.is_empty());
        assert_eq!(arena.pool_stats().2, 0);

        // 4. ZSet pool
        let mut z = arena.acquire_small_zset(8);
        z.push((OrderedScore(1.0), Bytes::from_static(b"zm1")));
        arena.recycle_small_zset(z);
        assert_eq!(arena.recycles_count, 4);
        assert_eq!(arena.pool_stats().3, 1);

        let z2 = arena.acquire_small_zset(4);
        assert_eq!(arena.allocations_saved, 4);
        assert!(z2.is_empty());
        assert_eq!(arena.pool_stats().3, 0);
    }

    #[test]
    fn test_collection_arena_rudis_table_integration() {
        let mut table = RudisTable::new();
        assert_eq!(table.arena.pool_stats(), (0, 0, 0, 0));

        // List push, pop to empty, recycle
        let key = b"mylist";
        let val1 = Bytes::from_static(b"a");
        let val2 = Bytes::from_static(b"b");
        assert_eq!(table.lpush_slice(key, &[val1, val2]).unwrap(), 2);
        let popped = table.lpop(key, 2).unwrap();
        assert_eq!(popped.len(), 2);
        assert!(!table.exists(key));
        assert_eq!(table.arena.pool_stats().0, 1);

        // Re-push reuses arena deque
        let val3 = Bytes::from_static(b"c");
        assert_eq!(table.lpush_slice(key, &[val3]).unwrap(), 1);
        assert_eq!(table.arena.pool_stats().0, 0);
        assert_eq!(table.arena.allocations_saved, 1);

        // Hash hset_slice and hdel to empty
        let hkey = b"myhash";
        let f1 = (Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        assert_eq!(
            table.hset_slice(hkey, std::slice::from_ref(&f1)).unwrap(),
            1
        );
        assert_eq!(table.hdel(hkey, std::slice::from_ref(&f1.0)).unwrap(), 1);
        assert!(!table.exists(hkey));
        assert_eq!(table.arena.pool_stats().1, 1);

        // Set sadd_slice and srem to empty
        let skey = b"myset";
        let m1 = Bytes::from_static(b"member1");
        assert_eq!(
            table.sadd_slice(skey, std::slice::from_ref(&m1)).unwrap(),
            1
        );
        assert_eq!(table.srem(skey, std::slice::from_ref(&m1)).unwrap(), 1);
        assert!(!table.exists(skey));
        assert_eq!(table.arena.pool_stats().2, 1);

        // ZSet zadd_slice and zrem to empty
        let zkey = b"myzset";
        let zm1 = (10.5, Bytes::from_static(b"zmember1"));
        assert_eq!(
            table
                .zadd_slice(zkey, std::slice::from_ref(&zm1), ZAddFlags::default())
                .unwrap(),
            (1, None)
        );
        assert_eq!(table.zrem(zkey, std::slice::from_ref(&zm1.1)).unwrap(), 1);
        assert!(!table.exists(zkey));
        assert_eq!(table.arena.pool_stats().3, 1);
    }
}
