use std::fs;
use std::path::Path;

/// Represents the result of Linux OS and kernel sanity checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemSanityReport {
    pub overcommit_memory: Option<i32>,
    pub somaxconn: Option<usize>,
    pub max_open_files: Option<u64>,
    pub transparent_hugepage: Option<String>,
    pub warnings: Vec<String>,
}

impl SystemSanityReport {
    /// Returns true if all checked system settings meet production recommendations.
    pub fn is_optimal(&self) -> bool {
        self.warnings.is_empty()
    }
}

/// Reads a single integer from a sysfs or procfs path.
fn read_sysfs_int(path: &str) -> Option<i64> {
    if Path::new(path).exists() {
        fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
    } else {
        None
    }
}

/// Reads the transparent hugepage setting from /sys/kernel/mm/transparent_hugepage/enabled.
fn read_thp_status(path: &str) -> Option<String> {
    if Path::new(path).exists() {
        fs::read_to_string(path).ok().map(|s| s.trim().to_string())
    } else {
        None
    }
}

/// Queries current process max open file descriptor limit (`ulimit -n`).
fn get_nofile_limit() -> Option<u64> {
    unsafe {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            Some(rlim.rlim_cur)
        } else {
            None
        }
    }
}

/// Runs OS and Linux kernel checks against paths (defaulting to standard /proc and /sys locations).
pub fn run_sanity_checks_with_paths(
    overcommit_path: &str,
    somaxconn_path: &str,
    thp_path: &str,
    nofile_override: Option<u64>,
) -> SystemSanityReport {
    let mut warnings = Vec::new();

    // 1. Check overcommit_memory (should be 1)
    let overcommit_memory = read_sysfs_int(overcommit_path).map(|v| v as i32);
    if let Some(val) = overcommit_memory
        && val != 1
    {
        warnings.push(format!(
            "WARNING: /proc/sys/vm/overcommit_memory is set to {}. Redis/Rudis background saves may fail under low memory conditions. Fix: 'sysctl vm.overcommit_memory=1'",
            val
        ));
    }

    // 2. Check somaxconn (should be >= 512, ideally >= 2048)
    let somaxconn = read_sysfs_int(somaxconn_path).map(|v| v as usize);
    if let Some(val) = somaxconn
        && val < 512
    {
        warnings.push(format!(
            "WARNING: The TCP backlog is set to {}. High connection rate may be throttled. Fix: 'sysctl -w net.core.somaxconn=4096'",
            val
        ));
    }

    // 3. Check ulimit -n (max open files, should be >= 10000)
    let max_open_files = nofile_override.or_else(get_nofile_limit);
    if let Some(val) = max_open_files
        && val < 10000
    {
        warnings.push(format!(
            "WARNING: Max open files limit is {}. Connection limit may be restricted. Fix: 'ulimit -n 65536' in /etc/security/limits.conf",
            val
        ));
    }

    // 4. Check Transparent Huge Pages (THP should be 'never' or '[never]')
    let transparent_hugepage = read_thp_status(thp_path);
    if let Some(ref val) = transparent_hugepage {
        // e.g. "always madvise [never]" is optimal, "[always] madvise never" is bad
        if val.contains("[always]") {
            warnings.push(
                "WARNING: Transparent Huge Pages (THP) is enabled in kernel ('always'). This will cause high latency spikes and memory bloat. Fix: 'echo never > /sys/kernel/mm/transparent_hugepage/enabled'".to_string(),
            );
        }
    }

    SystemSanityReport {
        overcommit_memory,
        somaxconn,
        max_open_files,
        transparent_hugepage,
        warnings,
    }
}

/// Runs standard Linux OS production sanity checks.
pub fn run_system_sanity_checks() -> SystemSanityReport {
    run_sanity_checks_with_paths(
        "/proc/sys/vm/overcommit_memory",
        "/proc/sys/net/core/somaxconn",
        "/sys/kernel/mm/transparent_hugepage/enabled",
        None,
    )
}

/// Emits warnings to console if any system sanity checks fail.
pub fn print_sanity_warnings(report: &SystemSanityReport) {
    if !report.is_optimal() {
        println!("  System Kernel Checks:");
        for w in &report.warnings {
            println!("  [!] {}", w);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_sanity_checks_detect_optimal() {
        let dir = std::env::temp_dir().join(format!("rudis_opt_{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);

        let overcommit = dir.join("overcommit_memory");
        let mut f1 = fs::File::create(&overcommit).unwrap();
        writeln!(f1, "1").unwrap();

        let somaxconn = dir.join("somaxconn");
        let mut f2 = fs::File::create(&somaxconn).unwrap();
        writeln!(f2, "4096").unwrap();

        let thp = dir.join("enabled");
        let mut f3 = fs::File::create(&thp).unwrap();
        writeln!(f3, "always madvise [never]").unwrap();

        let report = run_sanity_checks_with_paths(
            overcommit.to_str().unwrap(),
            somaxconn.to_str().unwrap(),
            thp.to_str().unwrap(),
            Some(65536),
        );

        assert!(report.is_optimal());
        assert_eq!(report.overcommit_memory, Some(1));
        assert_eq!(report.somaxconn, Some(4096));
        assert_eq!(report.max_open_files, Some(65536));
        assert!(report.warnings.is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_sanity_checks_detect_suboptimal_warnings() {
        let dir = std::env::temp_dir().join(format!("rudis_subopt_{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);

        let overcommit = dir.join("overcommit_memory");
        let mut f1 = fs::File::create(&overcommit).unwrap();
        writeln!(f1, "0").unwrap();

        let somaxconn = dir.join("somaxconn");
        let mut f2 = fs::File::create(&somaxconn).unwrap();
        writeln!(f2, "128").unwrap();

        let thp = dir.join("enabled");
        let mut f3 = fs::File::create(&thp).unwrap();
        writeln!(f3, "[always] madvise never").unwrap();

        let report = run_sanity_checks_with_paths(
            overcommit.to_str().unwrap(),
            somaxconn.to_str().unwrap(),
            thp.to_str().unwrap(),
            Some(1024),
        );

        assert!(!report.is_optimal());
        assert_eq!(report.warnings.len(), 4);
        assert!(report.warnings[0].contains("overcommit_memory"));
        assert!(report.warnings[1].contains("somaxconn"));
        assert!(report.warnings[2].contains("Max open files"));
        assert!(report.warnings[3].contains("Transparent Huge Pages"));

        let _ = fs::remove_dir_all(dir);
    }
}
