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
pub fn format_memory_info(used_mem: usize, max_mem: u64, cooled_keys: u64, tiered_keys: u64) -> String {
    let stats = get_allocator_stats();
    let rss = if stats.resident > 0 { stats.resident } else { used_mem };
    let frag = if stats.allocated > 0 { stats.fragmentation_ratio } else { 1.00 };

    format!(
        "# Memory\r\n\
        used_memory:{}\r\n\
        used_memory_human:{}\r\n\
        used_memory_rss:{}\r\n\
        used_memory_rss_human:{}\r\n\
        maxmemory:{}\r\n\
        maxmemory_human:{}\r\n\
        mem_fragmentation_ratio:{:.2}\r\n\
        mem_allocator:jemalloc\r\n\
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
