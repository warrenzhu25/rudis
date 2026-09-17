use std::sync::atomic::{AtomicBool, Ordering};

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
}

/// Installs OS signal handlers (SIGINT, SIGTERM) to trigger graceful server shutdown.
pub fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGINT, handle_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, handle_signal as *const () as libc::sighandler_t);
    }
}

extern "C" fn handle_signal(_sig: libc::c_int) {
    request_shutdown();
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
}
