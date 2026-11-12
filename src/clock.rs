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
