//! Snapshot (RDB) bookkeeping shared by SAVE, BGSAVE, the `save` schedule,
//! LASTSAVE and INFO persistence. Keyed by the server's base port because
//! tests run several servers in one process.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// After a failed scheduled save, wait this long before trying again
/// (Redis `CONFIG_BGSAVE_RETRY_DELAY`).
pub const SAVE_RETRY_DELAY_SECS: u64 = 5;

pub struct SnapshotState {
    last_save_unix: AtomicU64,
    last_attempt_unix: AtomicU64,
    dirty_at_last_save: AtomicU64,
    last_save_ok: AtomicBool,
    in_progress: AtomicBool,
}

static STATES: Mutex<Option<HashMap<u16, Arc<SnapshotState>>>> = Mutex::new(None);

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Write commands are counted per thread, each on its own cache line, so
/// shards never contend on a shared counter in the hot path.
#[repr(align(128))]
struct WriteSlot(AtomicU64);

const WRITE_SLOTS: usize = 256;
static WRITES: [WriteSlot; WRITE_SLOTS] = [const { WriteSlot(AtomicU64::new(0)) }; WRITE_SLOTS];
static NEXT_WRITE_SLOT: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static MY_WRITE_SLOT: usize =
        (NEXT_WRITE_SLOT.fetch_add(1, Ordering::Relaxed) as usize) % WRITE_SLOTS;
}

/// Counts one client write command toward `save <secs> <changes>`. Like
/// Redis's `dirty` counter but per command: a write that turns out to be a
/// no-op (e.g. DEL of a missing key) still counts, so saves can come a
/// little early, never late.
#[inline]
pub fn note_write() {
    MY_WRITE_SLOT.with(|&i| WRITES[i].0.fetch_add(1, Ordering::Relaxed));
}

/// Counts `n` changes made by one command, for the commands that count
/// their actual changes (see `connection::dirty_counted_on_change`).
#[inline]
pub fn note_changes(n: u64) {
    if n > 0 {
        MY_WRITE_SLOT.with(|&i| WRITES[i].0.fetch_add(n, Ordering::Relaxed));
    }
}

fn dirty_now() -> u64 {
    WRITES.iter().map(|s| s.0.load(Ordering::Relaxed)).sum()
}

/// State for `port`, created on first use as if a save just happened (Redis
/// sets `lastsave` to the start time).
pub fn state(port: u16) -> Arc<SnapshotState> {
    let mut map = STATES.lock().unwrap_or_else(|e| e.into_inner());
    map.get_or_insert_with(HashMap::new)
        .entry(port)
        .or_insert_with(|| {
            let now = unix_now();
            Arc::new(SnapshotState {
                last_save_unix: AtomicU64::new(now),
                last_attempt_unix: AtomicU64::new(now),
                dirty_at_last_save: AtomicU64::new(dirty_now()),
                last_save_ok: AtomicBool::new(true),
                in_progress: AtomicBool::new(false),
            })
        })
        .clone()
}

impl SnapshotState {
    /// Call when a save starts, before replying to the client, so INFO
    /// shows it in progress from then on; returns the change count it will
    /// cover.
    pub fn begin(&self) -> u64 {
        self.last_attempt_unix.store(unix_now(), Ordering::Relaxed);
        self.in_progress.store(true, Ordering::SeqCst);
        dirty_now()
    }

    /// Call when a save ends, after the save lock is released, so a client
    /// that sees it finished can start the next one. On success the changes
    /// made before it began are no longer pending; writes during the save
    /// still are. The outcome is stored before `in_progress` is cleared.
    pub fn finish(&self, dirty_before: u64, ok: bool) {
        if ok {
            self.dirty_at_last_save
                .fetch_max(dirty_before, Ordering::Relaxed);
            self.last_save_unix.store(unix_now(), Ordering::Relaxed);
        }
        self.last_save_ok.store(ok, Ordering::Relaxed);
        self.in_progress.store(false, Ordering::SeqCst);
    }

    pub fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }

    pub fn last_save_unix(&self) -> u64 {
        self.last_save_unix.load(Ordering::Relaxed)
    }

    pub fn last_save_ok(&self) -> bool {
        self.last_save_ok.load(Ordering::Relaxed)
    }

    pub fn changes_since_last_save(&self) -> u64 {
        dirty_now().saturating_sub(self.dirty_at_last_save.load(Ordering::Relaxed))
    }

    /// Whether a `save <secs> <changes>` point has been reached at `now`.
    pub fn save_due(&self, points: &[(u64, u64)], now: u64) -> bool {
        let retry_ok = self.last_save_ok()
            || now.saturating_sub(self.last_attempt_unix.load(Ordering::Relaxed))
                >= SAVE_RETRY_DELAY_SECS;
        retry_ok
            && save_point_reached(
                points,
                self.changes_since_last_save(),
                now.saturating_sub(self.last_save_unix()),
            )
    }
}

/// Redis rule: save when, for some point, at least `changes` writes happened
/// and at least `secs` seconds passed since the last successful save.
pub fn save_point_reached(points: &[(u64, u64)], changes: u64, secs_since_save: u64) -> bool {
    points
        .iter()
        .any(|&(secs, min_changes)| changes >= min_changes && secs_since_save >= secs)
}

/// Formats save points the way CONFIG GET save does ("3600 1 300 100").
pub fn format_save_points(points: &[(u64, u64)]) -> String {
    points
        .iter()
        .map(|(s, c)| format!("{} {}", s, c))
        .collect::<Vec<_>>()
        .join(" ")
}

/// BGREWRITEAOF status for INFO persistence (`aof_rewrite_in_progress`,
/// `aof_last_bgrewrite_status`).
pub struct AofRewriteState {
    in_progress: AtomicBool,
    last_ok: AtomicBool,
}

static AOF_REWRITE_STATES: Mutex<Option<HashMap<u16, Arc<AofRewriteState>>>> = Mutex::new(None);

pub fn aof_rewrite_state(port: u16) -> Arc<AofRewriteState> {
    let mut map = AOF_REWRITE_STATES.lock().unwrap_or_else(|e| e.into_inner());
    map.get_or_insert_with(HashMap::new)
        .entry(port)
        .or_insert_with(|| {
            Arc::new(AofRewriteState {
                in_progress: AtomicBool::new(false),
                last_ok: AtomicBool::new(true),
            })
        })
        .clone()
}

/// Same contract as `SnapshotState`: `begin` before the reply, `finish`
/// after the save lock is released, and the outcome is visible by the time
/// `in_progress` reads false (INFO may run on another shard thread).
impl AofRewriteState {
    pub fn begin(&self) {
        self.in_progress.store(true, Ordering::SeqCst);
    }

    pub fn finish(&self, ok: bool) {
        self.last_ok.store(ok, Ordering::SeqCst);
        self.in_progress.store(false, Ordering::SeqCst);
    }

    pub fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }

    pub fn last_ok(&self) -> bool {
        self.last_ok.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_point_rule() {
        let points = [(3600, 1), (300, 100), (60, 10000)];
        assert!(!save_point_reached(&points, 0, 100_000));
        assert!(!save_point_reached(&points, 1, 3599));
        assert!(save_point_reached(&points, 1, 3600));
        assert!(save_point_reached(&points, 100, 300));
        assert!(!save_point_reached(&points, 99, 300));
        assert!(save_point_reached(&points, 10000, 60));
        assert!(!save_point_reached(&[], 1_000_000, 1_000_000));
        assert_eq!(format_save_points(&points), "3600 1 300 100 60 10000");
        assert_eq!(format_save_points(&[]), "");
    }

    #[test]
    fn test_state_tracks_success_failure_and_retry_delay() {
        let st = state(1);
        let t0 = st.last_save_unix();
        let base = st.changes_since_last_save();
        for _ in 0..5 {
            note_write();
        }
        assert!(st.changes_since_last_save() >= base + 5);
        assert!(st.save_due(&[(0, 1)], t0));

        // A failed save keeps the changes pending and backs off.
        let before = st.begin();
        st.finish(before, false);
        assert!(!st.last_save_ok());
        assert!(st.changes_since_last_save() >= 5);
        let now = unix_now();
        assert!(!st.save_due(&[(0, 1)], now));
        assert!(st.save_due(&[(0, 1)], now + SAVE_RETRY_DELAY_SECS));

        // A successful one clears what it covered.
        let before = st.begin();
        st.finish(before, true);
        assert!(st.last_save_ok());
        assert!(st.changes_since_last_save() <= dirty_now() - before);
        assert!(st.last_save_unix() >= t0);
    }
}
