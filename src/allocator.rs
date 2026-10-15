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

/// Turns jemalloc's background purge thread on or off. With it on, dirty
/// pages are returned to the OS on the decay schedule even when the server
/// is idle (otherwise purging only happens on later allocations). Returns
/// whether jemalloc accepted the change.
pub fn set_background_thread(on: bool) -> bool {
    tikv_jemalloc_ctl::background_thread::write(on).is_ok()
}

/// Whether jemalloc's background purge thread is running.
pub fn background_thread_enabled() -> bool {
    tikv_jemalloc_ctl::background_thread::read().unwrap_or(false)
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

thread_local! {
    /// jemalloc's per-thread cumulative allocated/deallocated byte counters.
    /// Reading them is two plain loads (~1 ns), cheap enough for the write
    /// path. `None` if the allocator doesn't provide them.
    static THREAD_COUNTERS: Option<(
        tikv_jemalloc_ctl::thread::ThreadLocal<u64>,
        tikv_jemalloc_ctl::thread::ThreadLocal<u64>,
    )> = tikv_jemalloc_ctl::thread::allocatedp::read()
        .ok()
        .zip(tikv_jemalloc_ctl::thread::deallocatedp::read().ok());
}

/// Bytes this thread allocated minus bytes it freed, since it started.
/// Memory freed by another thread stays charged to the allocating thread
/// (and goes negative on the freeing one), so only the sum over all
/// threads is meaningful as a total; the change across a stretch of code
/// on one thread is exact for what that code allocated and freed.
#[inline]
pub fn thread_net_bytes() -> i64 {
    THREAD_COUNTERS
        .try_with(|c| {
            c.as_ref()
                .map_or(0, |(a, d)| a.get().wrapping_sub(d.get()) as i64)
        })
        .unwrap_or(0)
}

/// Bytes currently allocated by the whole process (jemalloc
/// `stats.allocated`, refreshed first). Costs ~15 µs: call from a cron, not
/// per command.
pub fn global_allocated() -> Option<usize> {
    tikv_jemalloc_ctl::epoch::advance().ok()?;
    tikv_jemalloc_ctl::stats::allocated::read().ok()
}

/// Inputs of [`format_memory_stats`] and [`memory_doctor`]: the server's
/// accounting summed over shards, plus `used_memory` and `maxmemory`.
pub struct MemoryReport {
    pub breakdown: crate::table::MemoryBreakdown,
    pub used: usize,
    pub maxmemory: u64,
    pub alloc: AllocatorStats,
}

impl MemoryReport {
    /// Bytes the allocator holds that `used_memory` (what `maxmemory`
    /// eviction compares) does not count.
    pub fn untracked(&self) -> usize {
        self.alloc.allocated.saturating_sub(self.used)
    }
}

/// `MEMORY STATS` reply (RESP2 flat map). Field names follow Redis where one
/// exists; `rudis.*` fields expose the incremental counters next to the
/// same figures recomputed from scratch, and memory `used_memory` misses.
pub fn format_memory_stats(r: &MemoryReport, out: &mut Vec<u8>) {
    let b = &r.breakdown;
    let ratio = |a: usize, d: usize| if d == 0 { 0.0 } else { a as f64 / d as f64 };
    let ints: [(&str, usize); 13] = [
        ("total.allocated", r.alloc.allocated),
        ("used_memory", r.used),
        ("maxmemory", r.maxmemory as usize),
        ("keys.count", b.keys),
        (
            "keys.bytes-per-key",
            r.used.checked_div(b.keys).unwrap_or(0),
        ),
        ("dataset.bytes", b.dataset),
        ("overhead.hashtable.main", b.table),
        ("allocator.allocated", r.alloc.allocated),
        ("allocator.active", r.alloc.active),
        ("allocator.resident", r.alloc.resident),
        ("rudis.dataset.exact.bytes", b.dataset_exact),
        ("rudis.hashtable.exact.bytes", b.table_exact),
        ("rudis.untracked.bytes", r.untracked()),
    ];
    let floats: [(&str, f64); 3] = [
        ("dataset.percentage", 100.0 * ratio(b.dataset, r.used)),
        (
            "allocator-fragmentation.ratio",
            ratio(r.alloc.active, r.alloc.allocated),
        ),
        ("fragmentation", ratio(r.alloc.resident, r.used)),
    ];
    crate::connection::write_resp_array_header(out, 2 * (ints.len() + floats.len() + 1));
    for (k, v) in ints {
        crate::connection::write_resp_bulk(out, k.as_bytes());
        crate::connection::write_resp_integer(out, v as i64);
    }
    crate::connection::write_resp_bulk(out, b"rudis.free-arena.bytes");
    crate::connection::write_resp_integer(out, b.free_arena as i64);
    for (k, v) in floats {
        crate::connection::write_resp_bulk(out, k.as_bytes());
        crate::connection::write_resp_bulk(out, format!("{v:.2}").as_bytes());
    }
}

/// `MEMORY DOCTOR`: findings an operator should act on, or a clean bill.
pub fn memory_doctor(r: &MemoryReport) -> String {
    const MB: usize = 1 << 20;
    let mut issues = Vec::new();
    let b = &r.breakdown;
    if r.alloc.resident > 100 * MB && r.alloc.resident as f64 > 1.4 * r.used as f64 {
        issues.push(format!(
            "High fragmentation: resident {} MB is {:.2}x used_memory {} MB. MEMORY PURGE \
             compacts the keyspace; consider maxmemory headroom.",
            r.alloc.resident / MB,
            r.alloc.resident as f64 / r.used.max(1) as f64,
            r.used / MB
        ));
    }
    if r.maxmemory > 0 && r.alloc.resident as u64 > r.maxmemory + r.maxmemory / 5 {
        issues.push(format!(
            "Resident memory {} MB exceeds maxmemory {} MB by over 20%: maxmemory bounds \
             used_memory, not RSS. Leave ~25% of RAM above maxmemory.",
            r.alloc.resident / MB,
            r.maxmemory as usize / MB
        ));
    }
    let drift = |inc: usize, exact: usize| inc.abs_diff(exact) > exact / 50 + MB;
    if drift(b.dataset, b.dataset_exact) || drift(b.table, b.table_exact) {
        issues.push(format!(
            "Memory accounting drift: dataset {} vs {} bytes recomputed, table {} vs {}. \
             MEMORY PURGE recomputes it.",
            b.dataset, b.dataset_exact, b.table, b.table_exact
        ));
    }
    if r.untracked() > 64 * MB && r.untracked() > r.used / 10 {
        issues.push(format!(
            "{} MB allocated is not counted in used_memory ({} MB), so maxmemory eviction \
             will not see it.",
            r.untracked() / MB,
            r.used / MB
        ));
    }
    if issues.is_empty() {
        "Hi Sam, I can't find any memory issues in your instance. I can only account for what occurs on this base.".to_string()
    } else {
        format!(
            "Sam, I detected a few issues in this instance memory implementation:\n\n * {}",
            issues.join("\n * ")
        )
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
    // Redis semantics: RSS relative to the logical dataset size.
    let frag = if used_mem > 0 {
        rss as f64 / used_mem as f64
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
        mem_not_counted_for_evict:0\r\n\
        mem_fragmentation_ratio:{:.2}\r\n\
        mem_allocator:jemalloc\r\n\
        allocator_allocated:{}\r\n\
        allocator_active:{}\r\n\
        allocator_resident:{}\r\n\
        allocator_metadata:{}\r\n\
        allocator_mapped:{}\r\n\
        cooled_keys:{}\r\n\
        tiered_keys:{}\r\n\
        number_of_cached_scripts:{}\r\n\
        number_of_functions:{}\r\n\
        number_of_libraries:{}\r\n\
        used_memory_vm_functions:{}\r\n\
        lazyfree_pending_objects:0\r\n\
        lazyfreed_objects:{}\r\n",
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
        crate::scripting::cached_scripts_count(),
        crate::scripting::count_functions(),
        crate::scripting::count_libraries(),
        32768 + crate::scripting::count_functions() * 256,
        crate::table::get_lazyfreed_objects(),
    )
}

#[cfg(test)]
use bytes::Bytes;

pub const MAX_ARENA_POOLED: usize = 1024;

/// High-performance thread-local slab & arena pool for small collections.
/// Eliminates allocator locks and reduces fragmentation for high-frequency
/// List (LPUSH/LPOP), Hash (HSET/HDEL), Set (SADD/SREM), and ZSet (ZADD/ZREM) workloads.
#[derive(Debug, Default)]
pub struct SmallCollectionArena {
    list_pool: Vec<crate::packedlist::PackedList>,
    set_pool: Vec<Vec<crate::table::SmallSetEntry>>,
    zset_pool: Vec<Vec<(crate::table::OrderedScore, crate::compact::CompactKey)>>,
    pub allocations_saved: u64,
    pub recycles_count: u64,
}

impl SmallCollectionArena {
    pub fn new() -> Self {
        Self {
            list_pool: Vec::new(),
            set_pool: Vec::new(),
            zset_pool: Vec::new(),
            allocations_saved: 0,
            recycles_count: 0,
        }
    }

    #[inline(always)]
    pub fn acquire_list(&mut self, _min_cap: usize) -> crate::packedlist::PackedList {
        if let Some(d) = self.list_pool.pop() {
            self.allocations_saved += 1;
            d
        } else {
            crate::packedlist::PackedList::new()
        }
    }

    #[inline(always)]
    pub fn recycle_list(&mut self, mut d: crate::packedlist::PackedList) {
        d.clear();
        if self.list_pool.len() < MAX_ARENA_POOLED {
            self.recycles_count += 1;
            self.list_pool.push(d);
        }
    }

    /// A new empty small hash. Not pooled: a small hash is one exactly sized
    /// allocation that every insert reallocates, so a recycled one saves
    /// nothing over the allocator's own thread cache.
    #[inline(always)]
    pub fn acquire_small_hash(&mut self, _min_pairs: usize) -> crate::smallhash::SmallHash {
        crate::smallhash::SmallHash::new()
    }

    #[inline(always)]
    pub fn recycle_small_hash(&mut self, h: crate::smallhash::SmallHash) {
        drop(h);
    }

    #[inline(always)]
    pub fn acquire_small_set(&mut self, min_cap: usize) -> Vec<crate::table::SmallSetEntry> {
        if let Some(mut v) = self.set_pool.pop() {
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
    pub fn recycle_small_set(&mut self, mut v: Vec<crate::table::SmallSetEntry>) {
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
    ) -> Vec<(crate::table::OrderedScore, crate::compact::CompactKey)> {
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
    pub fn recycle_small_zset(
        &mut self,
        mut v: Vec<(crate::table::OrderedScore, crate::compact::CompactKey)>,
    ) {
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
            0,
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
    fn test_thread_net_bytes_tracks_local_allocations() {
        let before = thread_net_bytes();
        let v: Vec<u8> = vec![1u8; 1 << 20];
        let grown = thread_net_bytes() - before;
        assert!(grown >= 1 << 20, "grew by {grown}");
        // Freed on another thread: still charged here.
        std::thread::spawn(move || drop(v)).join().unwrap();
        assert!(thread_net_bytes() - before >= 1 << 20);
        let w: Vec<u8> = vec![1u8; 1 << 20];
        let mid = thread_net_bytes();
        // While `w` is live, so the process-wide count can't depend on what
        // other tests happen to hold.
        assert!(global_allocated().is_some_and(|g| g >= 1 << 20));
        drop(w);
        assert!(mid - thread_net_bytes() >= 1 << 20);
    }

    fn report(used_mb: usize, resident_mb: usize, alloc_mb: usize, max_mb: u64) -> MemoryReport {
        const MB: usize = 1 << 20;
        let b = crate::table::MemoryBreakdown {
            keys: 1000,
            dataset: 10 * MB,
            dataset_exact: 10 * MB,
            table: 5 * MB,
            table_exact: 5 * MB,
            free_arena: MB,
        };
        MemoryReport {
            breakdown: b,
            used: used_mb * MB,
            maxmemory: max_mb << 20,
            alloc: AllocatorStats {
                allocated: alloc_mb * MB,
                active: alloc_mb * MB,
                resident: resident_mb * MB,
                metadata: 0,
                mapped: 0,
                fragmentation_ratio: 0.0,
            },
        }
    }

    #[test]
    fn test_memory_stats_reply_is_a_flat_map_of_named_fields() {
        let mut out = Vec::new();
        format_memory_stats(&report(200, 220, 210, 256), &mut out);
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("*34\r\n"), "{s}");
        for f in [
            "used_memory",
            "maxmemory",
            "keys.count",
            "dataset.bytes",
            "overhead.hashtable.main",
            "allocator.allocated",
            "allocator.resident",
            "rudis.dataset.exact.bytes",
            "rudis.hashtable.exact.bytes",
            "rudis.untracked.bytes",
            "rudis.free-arena.bytes",
            "fragmentation",
        ] {
            assert!(s.contains(&format!("\r\n{f}\r\n")), "missing {f}");
        }
        assert!(s.contains("rudis.untracked.bytes\r\n:10485760\r\n"), "{s}");
    }

    #[test]
    fn test_memory_doctor_flags_each_problem_and_passes_a_healthy_instance() {
        assert!(memory_doctor(&report(200, 220, 205, 256)).contains("can't find any"));
        let frag = memory_doctor(&report(200, 300, 205, 0));
        assert!(frag.contains("High fragmentation"), "{frag}");
        let over = memory_doctor(&report(250, 340, 255, 256));
        assert!(over.contains("exceeds maxmemory"), "{over}");
        let mut drift = report(200, 220, 205, 0);
        drift.breakdown.dataset_exact += 64 << 20;
        assert!(memory_doctor(&drift).contains("accounting drift"));
        let untracked = memory_doctor(&report(200, 290, 290, 0));
        assert!(
            untracked.contains("not counted in used_memory"),
            "{untracked}"
        );
    }

    #[test]
    fn test_background_thread_switch_round_trips() {
        assert!(set_background_thread(false));
        assert!(!background_thread_enabled());
        assert!(set_background_thread(true));
        assert!(background_thread_enabled());
    }

    #[test]
    fn test_small_collection_arena_lifecycle() {
        let mut arena = SmallCollectionArena::new();
        assert_eq!(arena.pool_stats(), (0, 0, 0, 0));

        // 1. List pool
        let mut d = arena.acquire_list(10);
        d.push_back(&Bytes::from_static(b"v1"));
        d.push_front(&Bytes::from_static(b"v0"));
        assert_eq!(d.len(), 2);
        arena.recycle_list(d);
        assert_eq!(arena.recycles_count, 1);
        assert_eq!(arena.pool_stats().0, 1);

        let d2 = arena.acquire_list(5);
        assert_eq!(arena.allocations_saved, 1);
        assert!(d2.is_empty());
        assert_eq!(arena.pool_stats().0, 0);

        // 2. Small hashes are not pooled (one exact allocation each).
        let mut h = arena.acquire_small_hash(4);
        h.push_new(b"f1", b"v1");
        arena.recycle_small_hash(h);
        assert_eq!(arena.recycles_count, 1);
        assert_eq!(arena.pool_stats().1, 0);

        let h2 = arena.acquire_small_hash(2);
        assert_eq!(arena.allocations_saved, 1);
        assert!(h2.is_empty());

        // 3. Set pool
        let mut s = arena.acquire_small_set(8);
        s.push(crate::table::SmallSetEntry {
            hash: fxhash::hash64(b"m1"),
            member: crate::compact::CompactKey::new(b"m1"),
        });
        arena.recycle_small_set(s);
        assert_eq!(arena.recycles_count, 2);
        assert_eq!(arena.pool_stats().2, 1);

        let s2 = arena.acquire_small_set(4);
        assert_eq!(arena.allocations_saved, 2);
        assert!(s2.is_empty());
        assert_eq!(arena.pool_stats().2, 0);

        // 4. ZSet pool
        let mut z = arena.acquire_small_zset(8);
        z.push((OrderedScore(1.0), crate::compact::CompactKey::new(b"zm1")));
        arena.recycle_small_zset(z);
        assert_eq!(arena.recycles_count, 3);
        assert_eq!(arena.pool_stats().3, 1);

        let z2 = arena.acquire_small_zset(4);
        assert_eq!(arena.allocations_saved, 3);
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
        assert_eq!(table.arena.pool_stats().1, 0);

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
