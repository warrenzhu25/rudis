use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Returns whether a server shutdown has been initiated.
pub fn is_shutting_down() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}

/// SHUTDOWN commands running now (see [`ShutdownCommand`]).
static SHUTDOWN_COMMANDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A SHUTDOWN command in progress, until dropped. A SIGTERM / SIGINT
/// arriving meanwhile stays pending: it takes effect only if this one
/// fails or is aborted, instead of running a second shutdown beside it
/// (which waited for replicas, moving the offset, and saved again).
pub struct ShutdownCommand(());

impl ShutdownCommand {
    pub fn begin() -> Self {
        SHUTDOWN_COMMANDS.fetch_add(1, Ordering::SeqCst);
        Self(())
    }
}

impl Drop for ShutdownCommand {
    fn drop(&mut self) {
        SHUTDOWN_COMMANDS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Whether a SHUTDOWN command is in progress.
pub fn shutdown_command_running() -> bool {
    SHUTDOWN_COMMANDS.load(Ordering::SeqCst) > 0
}

/// Initiates a graceful server shutdown.
pub fn request_shutdown() {
    if !SHUTDOWN_REQUESTED.swap(true, Ordering::SeqCst) {
        let _ = sd_notify("STOPPING=1\n");
    }
}

/// Sends a systemd notification datagram (`READY=1\n`, `STOPPING=1\n`, ...) to
/// the socket path in `$NOTIFY_SOCKET`, if set.
pub fn sd_notify(state: &str) -> bool {
    let Ok(socket_path) = std::env::var("NOTIFY_SOCKET") else {
        return false;
    };
    sd_notify_to(&socket_path, state)
}

/// Sends a systemd notification datagram to `socket_path` (filesystem path or
/// `@abstract` Linux socket).
pub fn sd_notify_to(socket_path: &str, state: &str) -> bool {
    if socket_path.is_empty() {
        return false;
    }
    let Ok(sock) = std::os::unix::net::UnixDatagram::unbound() else {
        return false;
    };
    #[cfg(target_os = "linux")]
    if let Some(abstract_name) = socket_path.strip_prefix('@') {
        use std::os::linux::net::SocketAddrExt;
        if let Ok(addr) =
            std::os::unix::net::SocketAddr::from_abstract_name(abstract_name.as_bytes())
        {
            return sock.send_to_addr(state.as_bytes(), &addr).is_ok();
        }
        return false;
    }
    sock.send_to(state.as_bytes(), socket_path).is_ok()
}

/// Set from the moment a shutdown is decided until the process exits (or the
/// shutdown is abandoned because the final save failed): no new command
/// batch may start, so nothing is acknowledged after the final save.
static QUIESCING: AtomicBool = AtomicBool::new(false);
const INFLIGHT_SLOTS: usize = 256;
/// Command batches executing, per shard thread (each slot written only by
/// its own shard, so no contention on the hot path).
static INFLIGHT: [crate::mailbox::CachePadded<AtomicUsize>; INFLIGHT_SLOTS] =
    [const { crate::mailbox::CachePadded(AtomicUsize::new(0)) }; INFLIGHT_SLOTS];

/// A command batch counted as executing on its shard; see [`enter_batch`].
pub struct BatchGuard(usize);

impl Drop for BatchGuard {
    fn drop(&mut self) {
        INFLIGHT[self.0].fetch_sub(1, Ordering::SeqCst);
    }
}

/// Registers a command batch about to execute on `shard`, or `None` while a
/// shutdown is quiescing the server (the caller must wait, and close once
/// [`is_shutting_down`]). Counting before checking (both SeqCst) pairs with
/// [`quiesce`] setting the flag before reading the counts: either the batch
/// sees the flag or `quiesce` sees the batch.
#[inline]
pub fn enter_batch(shard: usize) -> Option<BatchGuard> {
    let slot = shard % INFLIGHT_SLOTS;
    INFLIGHT[slot].fetch_add(1, Ordering::SeqCst);
    if QUIESCING.load(Ordering::SeqCst) {
        INFLIGHT[slot].fetch_sub(1, Ordering::SeqCst);
        return None;
    }
    Some(BatchGuard(slot))
}

/// Whether a shutdown is holding new command batches back.
#[inline]
pub fn is_quiescing() -> bool {
    QUIESCING.load(Ordering::Relaxed)
}

/// Stops new command batches and waits (up to `timeout`) until at most
/// `own` are still executing (the caller's own, e.g. the batch running
/// SHUTDOWN). Then a final save sees every acknowledged write and nothing
/// is acknowledged after it. (The save used to run while other clients'
/// writes kept being executed and acknowledged until they were drained,
/// ~200 ms later, and those writes were lost.) A batch blocked in, e.g.,
/// BLPOP is not waited for past `timeout`.
pub async fn quiesce(own: usize, timeout: std::time::Duration) {
    QUIESCING.store(true, Ordering::SeqCst);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let busy: usize = INFLIGHT.iter().map(|c| c.load(Ordering::SeqCst)).sum();
        if busy <= own {
            return;
        }
        if std::time::Instant::now() >= deadline {
            crate::log_warning!(
                "Shutdown: {} command batch(es) still running after {:?}; saving anyway.",
                busy - own,
                timeout
            );
            return;
        }
        monoio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

/// Lets command batches run again (the shutdown was abandoned).
pub fn unquiesce() {
    QUIESCING.store(false, Ordering::SeqCst);
}

/// Resets the shutdown state (primarily used in tests between test cases).
pub fn reset_shutdown() {
    QUIESCING.store(false, Ordering::SeqCst);
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    SHARDS_DRAINED.store(0, Ordering::SeqCst);
}

/// Installs OS signal handlers (SIGINT, SIGTERM) to trigger graceful server
/// shutdown, and SIGHUP to reopen the log file (logrotate).
pub fn install_signal_handlers() {
    // SAFETY: signal(2) with SIG_IGN or an `extern "C" fn(c_int)` handler is
    // valid, and the handlers only store to atomics, which is
    // async-signal-safe.
    unsafe {
        libc::signal(
            libc::SIGHUP,
            handle_sighup as *const () as libc::sighandler_t,
        );
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

extern "C" fn handle_signal(sig: libc::c_int) {
    // Async-signal-safe: only set a flag. A shard task picks it up, saves
    // if configured, and then calls `request_shutdown`.
    LAST_SHUTDOWN_SIGNAL.store(sig, Ordering::SeqCst);
    SHUTDOWN_SIGNALLED.store(true, Ordering::SeqCst);
}

extern "C" fn handle_sighup(_sig: libc::c_int) {
    // Async-signal-safe: only sets a flag the next log write checks.
    crate::log::request_reopen();
}

static SHUTDOWN_SIGNALLED: AtomicBool = AtomicBool::new(false);
static LAST_SHUTDOWN_SIGNAL: AtomicI32 = AtomicI32::new(0);

/// Name of the last shutdown signal received, for the log.
pub fn last_shutdown_signal_name() -> &'static str {
    match LAST_SHUTDOWN_SIGNAL.load(Ordering::SeqCst) {
        libc::SIGINT => "SIGINT",
        libc::SIGTERM => "SIGTERM",
        _ => "shutdown signal",
    }
}

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

/// Held by unit tests that set the process-wide shutdown flag, and by those
/// that need it to stay clear (a running script ends when it is set).
#[cfg(test)]
pub(crate) static TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_shutdown_command_is_tracked_while_it_runs() {
        let before = SHUTDOWN_COMMANDS.load(Ordering::SeqCst);
        let cmd = ShutdownCommand::begin();
        assert!(shutdown_command_running());
        drop(cmd);
        assert_eq!(SHUTDOWN_COMMANDS.load(Ordering::SeqCst), before);
    }

    #[test]
    fn test_a_released_batch_no_longer_counts() {
        let busy = || INFLIGHT[254 % INFLIGHT_SLOTS].load(Ordering::SeqCst);
        let before = busy();
        let mut guard = enter_batch(254);
        assert_eq!(busy(), before + 1);
        // A connection turning into a replica link lets its batch go.
        guard.take();
        assert_eq!(busy(), before);
    }

    #[test]
    fn test_quiesce_holds_new_batches_and_waits_for_running_ones() {
        let _guard = TEST_LOCK.lock();
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            // A batch running on shard 255 (a slot no test server uses).
            let running = enter_batch(255).expect("not quiescing");
            let t0 = std::time::Instant::now();
            quiesce(0, std::time::Duration::from_millis(100)).await;
            assert!(
                t0.elapsed() >= std::time::Duration::from_millis(90),
                "returned while a batch was still running"
            );
            assert!(is_quiescing());
            assert!(
                enter_batch(255).is_none(),
                "a new batch started while quiescing"
            );
            // The caller's own running batch is not waited for.
            let t0 = std::time::Instant::now();
            quiesce(1, std::time::Duration::from_secs(5)).await;
            assert!(t0.elapsed() < std::time::Duration::from_secs(4));
            drop(running);
            unquiesce();
            assert!(!is_quiescing());
            let again = enter_batch(255);
            assert!(again.is_some(), "batches run again after unquiesce");
        });
    }

    #[test]
    fn test_shutdown_flag_state_transitions() {
        let _guard = TEST_LOCK.lock();
        reset_shutdown();
        assert!(!is_shutting_down());

        request_shutdown();
        assert!(is_shutting_down());

        reset_shutdown();
        assert!(!is_shutting_down());
    }

    #[test]
    fn test_signal_is_pending_until_taken_and_does_not_stop_by_itself() {
        let _guard = TEST_LOCK.lock();
        reset_shutdown();
        signal_shutdown();
        // The save step decides; the signal alone must not stop the server.
        assert!(!is_shutting_down());
        assert!(take_shutdown_signal());
        assert!(!take_shutdown_signal());
    }

    #[test]
    fn test_shutdown_concurrent_observation() {
        let _guard = TEST_LOCK.lock();
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
        let _guard = TEST_LOCK.lock();
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

    #[test]
    fn test_sd_notify_to_filesystem_and_abstract_sockets() {
        let dir = std::env::temp_dir().join(format!("rudis-sd-notify-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let sock_path = dir.join("notify.sock");
        let receiver = std::os::unix::net::UnixDatagram::bind(&sock_path).unwrap();
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        assert!(sd_notify_to(sock_path.to_str().unwrap(), "READY=1\n"));
        let mut buf = [0u8; 32];
        let n = receiver.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"READY=1\n");

        #[cfg(target_os = "linux")]
        {
            use std::os::linux::net::SocketAddrExt;
            let abs_name = format!("rudis-sd-notify-abs-{}", std::process::id());
            let addr =
                std::os::unix::net::SocketAddr::from_abstract_name(abs_name.as_bytes()).unwrap();
            let abs_rx = std::os::unix::net::UnixDatagram::bind_addr(&addr).unwrap();
            abs_rx
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .unwrap();
            assert!(sd_notify_to(&format!("@{}", abs_name), "STOPPING=1\n"));
            let n = abs_rx.recv(&mut buf).unwrap();
            assert_eq!(&buf[..n], b"STOPPING=1\n");
        }

        assert!(!sd_notify_to("", "READY=1\n"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
