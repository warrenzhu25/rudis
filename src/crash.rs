//! Crash reports: a fatal signal (SIGSEGV, SIGBUS, SIGFPE, SIGILL,
//! SIGABRT) logs what crashed, with a backtrace, before the process dies,
//! as Redis's crash report does. Without it a crash in `unsafe` code, a
//! native library or an abort left nothing in the log: the process was
//! just gone (the supervisor saw a signal exit, the operator no reason).
//! SIGALRM logs a backtrace of the thread it lands on and is otherwise
//! ignored, as in Redis (`kill -ALRM` asks a stuck server where it is).
//!
//! The report is best effort. Its first line ("crashed by signal") is
//! formatted on the stack and written with write(2), which is safe in a
//! signal handler. The rest (the dated line, the backtrace) allocates,
//! which a handler must not do (the crash may have happened inside the
//! allocator, holding its lock), so before it the handler arms `alarm(2)`
//! with SIGALRM's default action: if the report deadlocks, the alarm kills
//! the process anyway. A second fatal signal during the report dies at
//! once. Handlers run on the thread's alternate signal stack (so a stack
//! overflow is reported too); a backtrace needs far more than the few KB
//! Rust gives each thread, so [`install`] (main thread) and
//! [`install_thread_stack`] (each shard thread) give theirs
//! [`ALT_STACK_SIZE`], and on a smaller one the backtrace is skipped. After the report the
//! signal's previous action runs again: Rust's own SIGSEGV/SIGBUS handler
//! (which reports a thread's stack overflow), else the default (the
//! process dies by the signal, with a core dump if enabled).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

const FATAL_SIGNALS: [libc::c_int; 5] = [
    libc::SIGSEGV,
    libc::SIGBUS,
    libc::SIGFPE,
    libc::SIGILL,
    libc::SIGABRT,
];

/// The actions the fatal signals had before ours (Rust's stack-overflow
/// handler for SIGSEGV and SIGBUS), indexed like [`FATAL_SIGNALS`].
static PREVIOUS: OnceLock<[libc::sigaction; 5]> = OnceLock::new();
static CRASHING: AtomicBool = AtomicBool::new(false);

/// The alternate signal stack [`install_thread_stack`] gives a thread:
/// enough to capture and symbolize a backtrace.
pub const ALT_STACK_SIZE: usize = 1 << 20;

/// Gives the calling thread an alternate signal stack of
/// [`ALT_STACK_SIZE`], so a crash report on it has its backtrace. The
/// stack is mapped outside the allocator, so it doesn't count in
/// `used_memory` (nor against `maxmemory`), and its pages are only
/// touched by a crash report. It is never unmapped: threads that get one
/// live as long as the process.
pub fn install_thread_stack() {
    // SAFETY: an anonymous private mapping, checked; sigaltstack(2) gets
    // a valid stack of `ss_size` bytes that stays mapped.
    unsafe {
        let sp = libc::mmap(
            std::ptr::null_mut(),
            ALT_STACK_SIZE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE | libc::MAP_STACK,
            -1,
            0,
        );
        if sp == libc::MAP_FAILED {
            return; // Rust's small stack stays: reports skip the backtrace.
        }
        let ss = libc::stack_t {
            ss_sp: sp,
            ss_flags: 0,
            ss_size: ALT_STACK_SIZE,
        };
        libc::sigaltstack(&ss, std::ptr::null_mut());
    }
}

/// Whether the handler runs on a signal stack too small for a backtrace.
fn on_small_signal_stack() -> bool {
    // SAFETY: sigaltstack(2) only reads the current settings into `cur`.
    unsafe {
        let mut cur: libc::stack_t = std::mem::zeroed();
        libc::sigaltstack(std::ptr::null(), &mut cur);
        cur.ss_flags & libc::SS_ONSTACK != 0 && cur.ss_size < ALT_STACK_SIZE
    }
}

/// Formats `n` into `buf` at `at`, without allocating; returns the new end.
fn put_num(buf: &mut [u8], mut at: usize, n: i64) -> usize {
    if n < 0 {
        at = put_str(buf, at, b"-");
    }
    let mut digits = [0u8; 20];
    let mut v = n.unsigned_abs();
    let mut len = 0;
    loop {
        digits[len] = b'0' + (v % 10) as u8;
        len += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    for i in (0..len).rev() {
        at = put_str(buf, at, &digits[i..i + 1]);
    }
    at
}

fn put_str(buf: &mut [u8], at: usize, s: &[u8]) -> usize {
    let n = s.len().min(buf.len() - at);
    buf[at..at + n].copy_from_slice(&s[..n]);
    at + n
}

/// The report's first lines, formatted without allocating.
fn first_lines(buf: &mut [u8], sig: libc::c_int, code: libc::c_int) -> usize {
    let mut at = put_str(
        buf,
        0,
        b"\n\n=== RUDIS BUG REPORT START: Cut & paste starting from here ===\n",
    );
    at = put_num(buf, at, i64::from(std::process::id()));
    at = put_str(buf, at, b":M # Rudis crashed by signal: ");
    at = put_num(buf, at, i64::from(sig));
    at = put_str(buf, at, b" (");
    at = put_str(buf, at, signal_name(sig).as_bytes());
    at = put_str(buf, at, b"), si_code: ");
    at = put_num(buf, at, i64::from(code));
    put_str(buf, at, b"\n")
}

/// Installs the crash and SIGALRM handlers. Call once, early in `main`
/// (after Rust's runtime set up its own handlers).
pub fn install() {
    install_thread_stack();
    // SAFETY: plain sigaction(2) calls with zero-initialized structs and
    // `extern "C"` handlers of the SA_SIGINFO signature.
    unsafe {
        let mut previous: [libc::sigaction; 5] = std::mem::zeroed();
        for (i, &sig) in FATAL_SIGNALS.iter().enumerate() {
            let mut act: libc::sigaction = std::mem::zeroed();
            act.sa_sigaction = on_fatal_signal as *const () as libc::sighandler_t;
            // SA_ONSTACK: Rust gives each thread an alternate signal stack,
            // so a stack overflow can still be reported.
            act.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
            libc::sigemptyset(&mut act.sa_mask);
            libc::sigaction(sig, &act, &mut previous[i]);
        }
        let _ = PREVIOUS.set(previous);
        let mut act: libc::sigaction = std::mem::zeroed();
        act.sa_sigaction = on_sigalrm as *const () as libc::sighandler_t;
        act.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK | libc::SA_RESTART;
        libc::sigemptyset(&mut act.sa_mask);
        libc::sigaction(libc::SIGALRM, &act, std::ptr::null_mut());
    }
}

fn signal_name(sig: libc::c_int) -> &'static str {
    match sig {
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGBUS => "SIGBUS",
        libc::SIGFPE => "SIGFPE",
        libc::SIGILL => "SIGILL",
        libc::SIGABRT => "SIGABRT",
        libc::SIGALRM => "SIGALRM",
        _ => "signal",
    }
}

/// The crash report's text: Redis's markers (scripts and the Redis test
/// suite look for "crashed by signal"), the signal, the faulting address
/// and the thread, then the backtrace.
/// The rest of the report, after [`first_lines`]: when, which version and
/// thread, the faulting address, and the backtrace (unless `small_stack`).
fn crash_report(addr: usize, small_stack: bool) -> String {
    let thread = std::thread::current();
    let trace = if small_stack {
        "(skipped: the handler runs on a small signal stack)".to_string()
    } else {
        std::backtrace::Backtrace::force_capture().to_string()
    };
    format!(
        "{}\n\
         Rudis version: {}\n\
         Accessing address: {:#x}\n\
         Thread: {}\n\
         ------ STACK TRACE ------\n{}\n\
         ------ STACK TRACE DONE ------\n\
         === RUDIS BUG REPORT END. Make sure to include from START to END. ===\n",
        report_line_prefix(),
        crate::version::long_version(),
        addr,
        thread.name().unwrap_or("unnamed"),
        trace
    )
}

/// A log line's prefix (`pid:M date #`), so the report is found with the
/// rest of the log.
fn report_line_prefix() -> String {
    let line = crate::log::format_now(crate::log::Level::Warning, "Crash report");
    line.trim_end().to_string()
}

extern "C" fn on_fatal_signal(
    sig: libc::c_int,
    info: *mut libc::siginfo_t,
    ctx: *mut libc::c_void,
) {
    if CRASHING.swap(true, Ordering::SeqCst) {
        // A fault while reporting one (or two threads crashing at once):
        // die by this signal now.
        die(sig);
    }
    // SAFETY: async-signal-safe calls (signal, alarm); `info` is the
    // kernel's siginfo for this signal (SA_SIGINFO).
    let (code, addr) = unsafe {
        // If the report deadlocks (the crash was inside the allocator or a
        // lock the report needs), this alarm ends the process.
        libc::signal(libc::SIGALRM, libc::SIG_DFL);
        libc::alarm(10);
        if info.is_null() {
            (0, 0)
        } else {
            ((*info).si_code, (*info).si_addr() as usize)
        }
    };
    let mut first = [0u8; 256];
    let n = first_lines(&mut first, sig, code);
    crate::log::write_crash_report(&first[..n]);
    let report = crash_report(addr, on_small_signal_stack());
    crate::log::write_crash_report(report.as_bytes());
    // SAFETY: PREVIOUS holds the actions sigaction(2) returned in
    // `install`; re-installing one, or calling its SA_SIGINFO handler with
    // this signal's arguments, is what the kernel would have done.
    unsafe {
        libc::alarm(0);
        if let Some(prev) = PREVIOUS.get()
            && let Some(i) = FATAL_SIGNALS.iter().position(|&s| s == sig)
        {
            let prev = prev[i];
            let h = prev.sa_sigaction;
            if h != libc::SIG_DFL && h != libc::SIG_IGN {
                if prev.sa_flags & libc::SA_SIGINFO != 0 {
                    let f: extern "C" fn(libc::c_int, *mut libc::siginfo_t, *mut libc::c_void) =
                        std::mem::transmute(h);
                    // Rust's handler: reports a stack overflow and aborts,
                    // else restores the default action and returns, and a
                    // fault happens again with it.
                    libc::sigaction(sig, &prev, std::ptr::null_mut());
                    f(sig, info, ctx);
                } else {
                    libc::sigaction(sig, &prev, std::ptr::null_mut());
                    let f: extern "C" fn(libc::c_int) = std::mem::transmute(h);
                    f(sig);
                }
                // A fault (si_code > 0) happens again on return; a signal
                // sent with kill(2) or raise(3) doesn't, and must still end
                // the process.
                if code > 0 {
                    return;
                }
            }
        }
    }
    die(sig);
}

/// Dies by `sig`, with its default action (a core dump where enabled).
fn die(sig: libc::c_int) -> ! {
    // SAFETY: async-signal-safe calls; `sig` is a valid signal number.
    unsafe {
        libc::signal(sig, libc::SIG_DFL);
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, sig);
        libc::pthread_sigmask(libc::SIG_UNBLOCK, &set, std::ptr::null_mut());
        libc::raise(sig);
        libc::_exit(128 + sig);
    }
}

extern "C" fn on_sigalrm(_: libc::c_int, _: *mut libc::siginfo_t, _: *mut libc::c_void) {
    if CRASHING.load(Ordering::SeqCst) {
        return;
    }
    let mut buf = [0u8; 64];
    let mut at = put_num(&mut buf, 0, i64::from(std::process::id()));
    at = put_str(&mut buf, at, b":M # Received SIGALRM\n");
    crate::log::write_crash_report(&buf[..at]);
    if !on_small_signal_stack() {
        let report = format!(
            "------ STACK TRACE ------\n{}\n------ STACK TRACE DONE ------\n",
            std::backtrace::Backtrace::force_capture()
        );
        crate::log::write_crash_report(report.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crash_report_has_redis_markers_and_a_backtrace() {
        let mut buf = [0u8; 256];
        let n = first_lines(&mut buf, libc::SIGSEGV, -6);
        let first = String::from_utf8_lossy(&buf[..n]);
        assert!(first.contains("=== RUDIS BUG REPORT START"), "{first}");
        assert!(
            first.contains(&format!(
                "{}:M # Rudis crashed by signal: 11 (SIGSEGV), si_code: -6\n",
                std::process::id()
            )),
            "{first}"
        );
        let r = crash_report(0x10, false);
        assert!(r.contains("Accessing address: 0x10"), "{r}");
        assert!(r.contains("------ STACK TRACE DONE ------"), "{r}");
        assert!(r.contains("test_crash_report_has_redis_markers"), "{r}");
        assert!(crash_report(0, true).contains("(skipped"));
    }
}
