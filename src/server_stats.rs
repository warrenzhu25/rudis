//! Process-wide counters behind the INFO `# Stats` section:
//! total_commands_processed, total_connections_received,
//! rejected_connections, total_net_input_bytes / total_net_output_bytes and
//! the instantaneous_* rates derived from them.
//!
//! Every thread owns one cache-line-aligned slot and is its only writer, so
//! bumping a counter is a relaxed load and store on a line no other thread
//! writes: no locked instruction and no cache-line ping-pong between shards.
//! Readers sum all slots. CONFIG RESETSTAT records the current sums as a
//! baseline instead of zeroing the slots, which would race with their
//! owners.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A counter reported in INFO stats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum Stat {
    Commands,
    ConnectionsReceived,
    RejectedConnections,
    NetInputBytes,
    NetOutputBytes,
}

const NUM_STATS: usize = 5;

#[repr(align(128))]
struct Slot {
    counters: [AtomicU64; NUM_STATS],
}

static SLOTS: Mutex<Vec<&'static Slot>> = Mutex::new(Vec::new());
static BASELINE: [AtomicU64; NUM_STATS] = [const { AtomicU64::new(0) }; NUM_STATS];

thread_local! {
    static LOCAL: &'static Slot = register_slot();
}

fn register_slot() -> &'static Slot {
    // Slots outlive their thread so the counts of exited threads stay in
    // the totals.
    let slot: &'static Slot = Box::leak(Box::new(Slot {
        counters: [const { AtomicU64::new(0) }; NUM_STATS],
    }));
    SLOTS.lock().unwrap_or_else(|e| e.into_inner()).push(slot);
    slot
}

/// Adds `n` to `stat` in this thread's slot.
#[inline]
pub fn add(stat: Stat, n: u64) {
    LOCAL.with(|slot| {
        let c = &slot.counters[stat as usize];
        c.store(c.load(Ordering::Relaxed).wrapping_add(n), Ordering::Relaxed);
    });
}

fn raw_total(stat: Stat) -> u64 {
    SLOTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|s| s.counters[stat as usize].load(Ordering::Relaxed))
        .fold(0u64, u64::wrapping_add)
}

/// Value of `stat` since start-up or the last [`reset`].
pub fn total(stat: Stat) -> u64 {
    raw_total(stat).saturating_sub(BASELINE[stat as usize].load(Ordering::Relaxed))
}

const ALL: [Stat; NUM_STATS] = [
    Stat::Commands,
    Stat::ConnectionsReceived,
    Stat::RejectedConnections,
    Stat::NetInputBytes,
    Stat::NetOutputBytes,
];

/// Restarts every counter from zero (CONFIG RESETSTAT).
pub fn reset() {
    for stat in ALL {
        BASELINE[stat as usize].store(raw_total(stat), Ordering::Relaxed);
    }
    let mut s = SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
    *s = Sampler::new();
}

/// Number of samples averaged by the instantaneous metrics, as in Redis.
const SAMPLES: usize = 16;
/// Minimum spacing of samples; Redis samples every 100ms.
const SAMPLE_PERIOD_MS: u128 = 100;

/// Metrics with an instantaneous rate: ops/sec, input and output bytes/sec.
const RATED: [Stat; 3] = [Stat::Commands, Stat::NetInputBytes, Stat::NetOutputBytes];

struct Sampler {
    last_time: Option<Instant>,
    last_value: [u64; 3],
    rates: [[u64; SAMPLES]; 3],
    idx: usize,
}

impl Sampler {
    const fn new() -> Self {
        Self {
            last_time: None,
            last_value: [0; 3],
            rates: [[0; SAMPLES]; 3],
            idx: 0,
        }
    }
}

static SAMPLER: Mutex<Sampler> = Mutex::new(Sampler::new());

/// Takes one sample of the rated counters. Called periodically; calls
/// closer than 100ms to the previous sample are ignored, so several shards
/// or servers in one process may all call it.
pub fn sample() {
    let now = Instant::now();
    let values = RATED.map(raw_total);
    let mut s = SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(last) = s.last_time {
        let ms = now.duration_since(last).as_millis();
        if ms < SAMPLE_PERIOD_MS {
            return;
        }
        let idx = s.idx;
        for (i, v) in values.iter().enumerate() {
            let delta = v.wrapping_sub(s.last_value[i]);
            s.rates[i][idx] = (u128::from(delta) * 1000 / ms) as u64;
        }
        s.idx = (idx + 1) % SAMPLES;
    }
    s.last_time = Some(now);
    s.last_value = values;
}

/// Average per-second rates over the last samples:
/// (ops/sec, input bytes/sec, output bytes/sec).
pub fn instantaneous() -> (u64, u64, u64) {
    let s = SAMPLER.lock().unwrap_or_else(|e| e.into_inner());
    let avg = |i: usize| s.rates[i].iter().sum::<u64>() / SAMPLES as u64;
    (avg(0), avg(1), avg(2))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_sum_across_threads() {
        let before = raw_total(Stat::RejectedConnections);
        add(Stat::RejectedConnections, 2);
        std::thread::spawn(|| add(Stat::RejectedConnections, 3))
            .join()
            .unwrap();
        // Other tests may count too, so only a lower bound holds.
        assert!(raw_total(Stat::RejectedConnections) - before >= 5);
    }
}
