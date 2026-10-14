//! Cheap monotonic timestamps for timing every command of a batched pipeline
//! (SLOWLOG), where two `Instant::now()` calls per command (~45 ns of vDSO
//! `clock_gettime`) cost cheap commands like INCR ~5% throughput.
//!
//! On x86-64 with an invariant TSC (`constant_tsc` and `nonstop_tsc` in
//! /proc/cpuinfo) a stamp is one `rdtsc` (~6 ns), converted to microseconds
//! with a rate calibrated once against `Instant`. Elsewhere, stamps are
//! nanoseconds from `Instant`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// f64 bits of TSC ticks per microsecond; `UNSET` before calibration,
/// `NO_TSC` when stamps come from `Instant`.
static TICKS_PER_US: AtomicU64 = AtomicU64::new(UNSET);
const UNSET: u64 = 0;
const NO_TSC: u64 = u64::MAX;

static EPOCH: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn rdtsc() -> u64 {
    // SAFETY: `rdtsc` has no preconditions on x86-64; it only reads the
    // time-stamp counter.
    unsafe { core::arch::x86_64::_rdtsc() }
}

fn calibrate() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        let invariant = std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|s| {
                s.lines().find(|l| l.starts_with("flags")).map(|l| {
                    let f: Vec<&str> = l.split_whitespace().collect();
                    f.contains(&"constant_tsc") && f.contains(&"nonstop_tsc")
                })
            })
            .unwrap_or(false);
        if invariant {
            let t0 = Instant::now();
            let c0 = rdtsc();
            while t0.elapsed() < Duration::from_millis(2) {
                std::hint::spin_loop();
            }
            let c1 = rdtsc();
            let us = t0.elapsed().as_nanos() as f64 / 1000.0;
            let rate = (c1.wrapping_sub(c0)) as f64 / us;
            if rate.is_finite() && rate > 1.0 {
                return rate.to_bits();
            }
        }
    }
    NO_TSC
}

/// Calibrates now (a ~2 ms spin) rather than on the first timed command.
pub fn init() {
    if TICKS_PER_US.load(Ordering::Relaxed) == UNSET {
        TICKS_PER_US.store(calibrate(), Ordering::Relaxed);
    }
    let _ = *EPOCH;
}

#[inline(always)]
fn rate_bits() -> u64 {
    let r = TICKS_PER_US.load(Ordering::Relaxed);
    if r != UNSET {
        return r;
    }
    init();
    TICKS_PER_US.load(Ordering::Relaxed)
}

/// A timestamp for [`micros_since`].
#[inline(always)]
pub fn now() -> u64 {
    #[cfg(target_arch = "x86_64")]
    if rate_bits() != NO_TSC {
        return rdtsc();
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = rate_bits();
    EPOCH.elapsed().as_nanos() as u64
}

/// Whole microseconds since `start` (from [`now`]).
#[inline(always)]
pub fn micros_since(start: u64) -> u64 {
    let r = rate_bits();
    #[cfg(target_arch = "x86_64")]
    if r != NO_TSC {
        return (rdtsc().wrapping_sub(start) as f64 / f64::from_bits(r)) as u64;
    }
    let _ = r;
    (EPOCH.elapsed().as_nanos() as u64).saturating_sub(start) / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measures_a_sleep_within_a_few_milliseconds() {
        init();
        let t = now();
        std::thread::sleep(Duration::from_millis(20));
        let us = micros_since(t);
        assert!(
            (19_000..40_000).contains(&us),
            "measured {us} us for a 20 ms sleep"
        );
        // Back-to-back stamps are (near) zero apart.
        let t = now();
        assert!(micros_since(t) < 1_000);
    }
}
