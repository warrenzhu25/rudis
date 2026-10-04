use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Returns whether a server shutdown has been initiated.
pub fn is_shutting_down() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}

/// Initiates a graceful server shutdown.
pub fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Resets the shutdown state (primarily used in tests between test cases).
pub fn reset_shutdown() {
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    SHARDS_DRAINED.store(0, Ordering::SeqCst);
}

/// Installs OS signal handlers (SIGINT, SIGTERM) to trigger graceful server shutdown.
pub fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        libc::signal(
            libc::SIGINT,
            handle_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            handle_signal as *const () as libc::sighandler_t,
        );
    }
}

extern "C" fn handle_signal(_sig: libc::c_int) {
    // Async-signal-safe: only set a flag. A shard task picks it up, saves
    // if configured, and then calls `request_shutdown`.
    SHUTDOWN_SIGNALLED.store(true, Ordering::SeqCst);
}

static SHUTDOWN_SIGNALLED: AtomicBool = AtomicBool::new(false);

/// Takes a pending SIGTERM/SIGINT, if any.
pub fn take_shutdown_signal() -> bool {
    SHUTDOWN_SIGNALLED.swap(false, Ordering::SeqCst)
}

/// Simulates a signal (for tests).
pub fn signal_shutdown() {
    SHUTDOWN_SIGNALLED.store(true, Ordering::SeqCst);
}

/// How long a shard waits for its clients to finish their in-flight
/// commands once shutdown starts. Clients still connected after this (for
/// example blocked in `BLPOP`) are dropped.
pub const CLIENT_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// How long a drained shard keeps serving cross-shard requests while the
/// other shards finish draining.
pub const SHARD_BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

static SHARDS_DRAINED: AtomicUsize = AtomicUsize::new(0);

/// Records that this shard has no more clients that can issue commands.
pub fn mark_shard_drained() {
    SHARDS_DRAINED.fetch_add(1, Ordering::SeqCst);
}

/// Whether every one of `num_shards` shards has drained its clients, so no
/// new cross-shard request can arrive any more.
pub fn all_shards_drained(num_shards: usize) -> bool {
    SHARDS_DRAINED.load(Ordering::SeqCst) >= num_shards
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::thread;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_shutdown_flag_state_transitions() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_shutdown();
        assert!(!is_shutting_down());

        request_shutdown();
        assert!(is_shutting_down());

        reset_shutdown();
        assert!(!is_shutting_down());
    }

    #[test]
    fn test_signal_is_pending_until_taken_and_does_not_stop_by_itself() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_shutdown();
        signal_shutdown();
        // The save step decides; the signal alone must not stop the server.
        assert!(!is_shutting_down());
        assert!(take_shutdown_signal());
        assert!(!take_shutdown_signal());
    }

    #[test]
    fn test_shutdown_concurrent_observation() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_shutdown();
        let handle = thread::spawn(|| {
            while !is_shutting_down() {
                std::hint::spin_loop();
            }
            true
        });

        thread::sleep(std::time::Duration::from_millis(10));
        request_shutdown();
        assert!(handle.join().unwrap());
        reset_shutdown();
    }

    #[test]
    fn test_drain_barrier_waits_for_every_shard_and_resets() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_shutdown();
        assert!(!all_shards_drained(3));
        let shards: Vec<_> = (0..2).map(|_| thread::spawn(mark_shard_drained)).collect();
        shards.into_iter().for_each(|s| s.join().unwrap());
        assert!(!all_shards_drained(3));
        mark_shard_drained();
        assert!(all_shards_drained(3));
        assert!(!all_shards_drained(4));
        reset_shutdown();
        assert!(!all_shards_drained(1));
    }
}
