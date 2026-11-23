//! The clock data commands see. Normally the real one; while a script runs
//! it is frozen at the script's start, as Redis freezes time for a script:
//! a key can't expire half-way through it, and TIME, TTL, EXPIRETIME and
//! relative expirations (EX, PX, EXPIRE...) all agree across its commands.

use std::cell::Cell;
use std::time::{Instant, SystemTime};

thread_local! {
    static FROZEN: Cell<Option<(Instant, SystemTime)>> = const { Cell::new(None) };
}

/// `Instant::now()`, or the frozen time while a script runs here.
#[inline(always)]
pub fn now() -> Instant {
    FROZEN.get().map_or_else(Instant::now, |f| f.0)
}

/// `SystemTime::now()`, or the frozen time while a script runs here.
#[inline(always)]
pub fn system_now() -> SystemTime {
    FROZEN.get().map_or_else(SystemTime::now, |f| f.1)
}

/// `now()` and the Unix time (`system_now()` since the epoch) of the same
/// moment, to convert deadlines between the two clocks. Read one after the
/// other, the two can be apart by however long the thread was preempted in
/// between, and an absolute deadline converted in with one pair and out
/// with another came back a millisecond off (HPEXPIREAT, then
/// HPEXPIRETIME). This reads the monotonic clock on both sides of the
/// system clock, retrying until they are close, and takes their midpoint.
pub fn now_pair() -> (Instant, std::time::Duration) {
    use std::time::{Duration, UNIX_EPOCH};
    let since_epoch = |s: SystemTime| s.duration_since(UNIX_EPOCH).unwrap_or_default();
    if let Some((i, s)) = FROZEN.get() {
        return (i, since_epoch(s));
    }
    let mut best: Option<(Duration, Instant, SystemTime)> = None;
    for _ in 0..4 {
        let a = Instant::now();
        let s = SystemTime::now();
        let b = Instant::now();
        let gap = b - a;
        if best.is_none_or(|(g, _, _)| gap < g) {
            best = Some((gap, a + gap / 2, s));
        }
        if gap < Duration::from_micros(20) {
            break;
        }
    }
    let (_, i, s) = best.expect("sampled at least once");
    (i, since_epoch(s))
}

/// The current time, to freeze a script at.
pub fn snapshot() -> (Instant, SystemTime) {
    (Instant::now(), SystemTime::now())
}

/// Freezes this thread's clock at `at` until dropped (restoring what was
/// there: a script resumed inside another's freeze keeps the outer one).
pub struct Frozen(Option<(Instant, SystemTime)>);

impl Frozen {
    pub fn at(at: (Instant, SystemTime)) -> Self {
        Self(FROZEN.replace(Some(at)))
    }
}

impl Drop for Frozen {
    fn drop(&mut self) {
        FROZEN.set(self.0);
    }
}
