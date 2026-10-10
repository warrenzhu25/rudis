//! Redis-compatible server log.
//!
//! Lines look like Redis/Valkey's `serverLogRaw` output:
//!
//! ```text
//! 12345:M 10 Oct 2026 20:36:35.123 * Ready to accept connections tcp
//! ```
//!
//! `<pid>:<role> <timestamp> <level> <message>`, where the role is `M`
//! (master) or `S` (replica) and the level is `.` (debug), `-` (verbose),
//! `*` (notice) or `#` (warning). Valkey 8.1's `log-format logfmt` and
//! `log-timestamp-format iso8601|milliseconds` are supported too.
//!
//! Logging is meant for operational events, never for the command path:
//! [`enabled`] is one relaxed atomic load, so a filtered-out message costs
//! nothing beyond that (the `log_*!` macros check it before formatting).
//! Each line is written with a single `write(2)` under a mutex, so lines
//! from different shard threads never interleave.
//!
//! `logfile` (empty: stdout) is opened in append mode. Like Redis, which
//! reopens the file on every call, each write first checks that the path
//! still names the open file (one `stat`), so `logrotate`'s rename-and-create
//! works without a signal; SIGHUP also forces a reopen.
//!
//! `tracing` events are routed here too (see [`RedisLogLayer`]).

use parking_lot::Mutex;
use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

/// A log level, ordered like Redis's `LL_*` constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Level {
    Debug = 0,
    Verbose = 1,
    Notice = 2,
    Warning = 3,
}

impl Level {
    /// The level character of the legacy format.
    pub fn as_char(self) -> char {
        match self {
            Level::Debug => '.',
            Level::Verbose => '-',
            Level::Notice => '*',
            Level::Warning => '#',
        }
    }

    /// The level name of the logfmt format (Valkey's syslog-style names).
    fn logfmt_name(self) -> &'static str {
        match self {
            Level::Debug => "debug",
            Level::Verbose => "info",
            Level::Notice => "notice",
            Level::Warning => "warning",
        }
    }
}

/// `loglevel` values; the index is the minimum level logged (4: nothing).
const LOGLEVEL_NAMES: [&str; 5] = ["debug", "verbose", "notice", "warning", "nothing"];
const LOG_FORMAT_NAMES: [&str; 2] = ["legacy", "logfmt"];
const LOG_TIMESTAMP_FORMAT_NAMES: [&str; 3] = ["legacy", "iso8601", "milliseconds"];

/// Like Redis's LOG_MAX_LEN: longer messages are truncated.
pub const LOG_MAX_LEN: usize = 1024;

static VERBOSITY: AtomicU8 = AtomicU8::new(Level::Notice as u8);
static LOG_FORMAT: AtomicU8 = AtomicU8::new(0);
static LOG_TIMESTAMP_FORMAT: AtomicU8 = AtomicU8::new(0);
static ROLE_REPLICA: AtomicBool = AtomicBool::new(false);
/// Set by SIGHUP: reopen the log file before the next write.
static REOPEN: AtomicBool = AtomicBool::new(false);

struct Sink {
    /// `logfile`; empty means stdout.
    path: String,
    /// The open log file and the (device, inode) it was opened as.
    file: Option<(std::fs::File, u64, u64)>,
}

static SINK: Mutex<Sink> = Mutex::new(Sink {
    path: String::new(),
    file: None,
});

/// Whether a message at `level` would be logged. One relaxed atomic load.
#[inline(always)]
pub fn enabled(level: Level) -> bool {
    level as u8 >= VERBOSITY.load(Ordering::Relaxed)
}

/// Parses an enum config value; the error is Redis's CONFIG SET wording.
fn parse_enum(names: &[&str], value: &str) -> Result<u8, String> {
    names
        .iter()
        .position(|n| n.eq_ignore_ascii_case(value.trim()))
        .map(|i| i as u8)
        .ok_or_else(|| {
            format!(
                "argument(s) must be one of the following: {}",
                names.join(", ")
            )
        })
}

/// Checks a `loglevel` value without applying it.
pub fn validate_loglevel(value: &str) -> Result<(), String> {
    parse_enum(&LOGLEVEL_NAMES, value).map(|_| ())
}

/// Sets `loglevel`: debug, verbose, notice, warning or nothing.
pub fn set_loglevel(value: &str) -> Result<(), String> {
    let v = parse_enum(&LOGLEVEL_NAMES, value)?;
    VERBOSITY.store(v, Ordering::Relaxed);
    Ok(())
}

pub fn loglevel() -> &'static str {
    LOGLEVEL_NAMES[VERBOSITY.load(Ordering::Relaxed).min(4) as usize]
}

/// Checks a `log-format` value without applying it.
pub fn validate_log_format(value: &str) -> Result<(), String> {
    parse_enum(&LOG_FORMAT_NAMES, value).map(|_| ())
}

/// Sets `log-format`: legacy or logfmt.
pub fn set_log_format(value: &str) -> Result<(), String> {
    let v = parse_enum(&LOG_FORMAT_NAMES, value)?;
    LOG_FORMAT.store(v, Ordering::Relaxed);
    Ok(())
}

pub fn log_format() -> &'static str {
    LOG_FORMAT_NAMES[LOG_FORMAT.load(Ordering::Relaxed).min(1) as usize]
}

/// Checks a `log-timestamp-format` value without applying it.
pub fn validate_log_timestamp_format(value: &str) -> Result<(), String> {
    parse_enum(&LOG_TIMESTAMP_FORMAT_NAMES, value).map(|_| ())
}

/// Sets `log-timestamp-format`: legacy, iso8601 or milliseconds.
pub fn set_log_timestamp_format(value: &str) -> Result<(), String> {
    let v = parse_enum(&LOG_TIMESTAMP_FORMAT_NAMES, value)?;
    LOG_TIMESTAMP_FORMAT.store(v, Ordering::Relaxed);
    Ok(())
}

pub fn log_timestamp_format() -> &'static str {
    LOG_TIMESTAMP_FORMAT_NAMES[LOG_TIMESTAMP_FORMAT.load(Ordering::Relaxed).min(2) as usize]
}

/// Sets `logfile` (empty: stdout). The file is opened (created) right away
/// so a bad path fails at startup, as in Redis.
pub fn set_logfile(path: &str) -> Result<(), String> {
    let mut sink = SINK.lock();
    if path.is_empty() {
        sink.path.clear();
        sink.file = None;
        return Ok(());
    }
    let file = open_logfile(path).map_err(|e| format!("Can't open the log file: {}", e))?;
    sink.path = path.to_string();
    sink.file = Some(file);
    Ok(())
}

pub fn logfile() -> String {
    SINK.lock().path.clone()
}

/// The role shown in each line: `S` while this server replicates from a
/// master, `M` otherwise.
pub fn set_role_replica(replica: bool) {
    ROLE_REPLICA.store(replica, Ordering::Relaxed);
}

/// Asks for the log file to be reopened before the next write. Only
/// stores to an atomic, so it is async-signal-safe (SIGHUP handler).
pub fn request_reopen() {
    REOPEN.store(true, Ordering::Relaxed);
}

fn open_logfile(path: &str) -> std::io::Result<(std::fs::File, u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let meta = file.metadata()?;
    Ok((file, meta.dev(), meta.ino()))
}

/// Local broken-down time of a log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalTime {
    pub year: i32,
    /// 0-11.
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub millis: u32,
    /// Seconds east of UTC.
    pub utc_offset: i64,
    /// Milliseconds since the Unix epoch.
    pub unix_ms: i64,
}

impl LocalTime {
    pub fn now() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs() as libc::time_t;
        let mut tm = std::mem::MaybeUninit::<libc::tm>::zeroed();
        // SAFETY: `secs` and `tm` are valid for the call; localtime_r is the
        // reentrant variant, writes only to `tm` and returns null on failure
        // (then the zeroed `tm` is used).
        let tm = unsafe {
            libc::localtime_r(&secs, tm.as_mut_ptr());
            tm.assume_init()
        };
        LocalTime {
            year: tm.tm_year + 1900,
            month: tm.tm_mon.clamp(0, 11) as u32,
            day: tm.tm_mday as u32,
            hour: tm.tm_hour as u32,
            minute: tm.tm_min as u32,
            second: tm.tm_sec as u32,
            millis: now.subsec_millis(),
            utc_offset: tm.tm_gmtoff,
            unix_ms: now.as_millis() as i64,
        }
    }
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

fn format_timestamp(out: &mut String, t: &LocalTime, ts_format: u8) {
    match ts_format {
        1 => {
            let off = t.utc_offset;
            let _ = write!(
                out,
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}{}{:02}:{:02}",
                t.year,
                t.month + 1,
                t.day,
                t.hour,
                t.minute,
                t.second,
                t.millis,
                if off >= 0 { '+' } else { '-' },
                off.abs() / 3600,
                off.abs() % 3600 / 60
            );
        }
        2 => {
            let _ = write!(out, "{}", t.unix_ms);
        }
        _ => {
            let _ = write!(
                out,
                "{:02} {} {} {:02}:{:02}:{:02}.{:03}",
                t.day, MONTHS[t.month as usize], t.year, t.hour, t.minute, t.second, t.millis
            );
        }
    }
}

/// Cuts `msg` to at most `LOG_MAX_LEN - 1` bytes on a character boundary.
fn truncate_msg(msg: &str) -> &str {
    if msg.len() < LOG_MAX_LEN {
        return msg;
    }
    let mut end = LOG_MAX_LEN - 1;
    while !msg.is_char_boundary(end) {
        end -= 1;
    }
    &msg[..end]
}

/// Formats one complete line (with the trailing newline).
pub fn format_line(
    level: Level,
    msg: &str,
    pid: u32,
    replica: bool,
    t: &LocalTime,
    format: u8,
    ts_format: u8,
) -> String {
    let msg = truncate_msg(msg);
    let mut out = String::with_capacity(msg.len() + 64);
    if format == 1 {
        let _ = write!(
            out,
            "pid={} role={} timestamp=\"",
            pid,
            if replica { "replica" } else { "primary" }
        );
        format_timestamp(&mut out, t, ts_format);
        let _ = write!(out, "\" level={} message=\"", level.logfmt_name());
        for c in msg.chars() {
            out.push(match c {
                '"' => '\'',
                '\n' | '\r' => ' ',
                c => c,
            });
        }
        out.push_str("\"\n");
    } else {
        let _ = write!(out, "{}:{} ", pid, if replica { 'S' } else { 'M' });
        format_timestamp(&mut out, t, ts_format);
        let _ = writeln!(out, " {} {}", level.as_char(), msg);
    }
    out
}

/// Logs `msg` at `level` if `loglevel` allows it.
pub fn log_str(level: Level, msg: &str) {
    if !enabled(level) {
        return;
    }
    let line = format_line(
        level,
        msg,
        std::process::id(),
        ROLE_REPLICA.load(Ordering::Relaxed),
        &LocalTime::now(),
        LOG_FORMAT.load(Ordering::Relaxed),
        LOG_TIMESTAMP_FORMAT.load(Ordering::Relaxed),
    );
    write_line(line.as_bytes());
}

/// Logs formatted arguments; use the `log_*!` macros, which check
/// [`enabled`] before formatting anything.
pub fn log_args(level: Level, args: std::fmt::Arguments<'_>) {
    if !enabled(level) {
        return;
    }
    match args.as_str() {
        Some(s) => log_str(level, s),
        None => log_str(level, &args.to_string()),
    }
}

fn write_line(line: &[u8]) {
    let mut sink = SINK.lock();
    if sink.path.is_empty() {
        drop(sink);
        write_stdout(line);
        return;
    }
    // Reopen if asked to (SIGHUP) or if the path no longer names the open
    // file (renamed or deleted by logrotate): cheap enough for log lines,
    // which never come from the command path.
    let stale = REOPEN.swap(false, Ordering::Relaxed)
        || match (&sink.file, std::fs::metadata(&sink.path)) {
            (Some((_, dev, ino)), Ok(meta)) => {
                use std::os::unix::fs::MetadataExt;
                meta.dev() != *dev || meta.ino() != *ino
            }
            _ => true,
        };
    if stale && let Ok(f) = open_logfile(&sink.path) {
        sink.file = Some(f);
    }
    if let Some((file, _, _)) = sink.file.as_mut() {
        // O_APPEND: one write(2) per line, atomic against other writers.
        let _ = file.write_all(line);
    }
}

fn write_stdout(line: &[u8]) {
    // Unit tests: go through the test harness's output capture.
    #[cfg(test)]
    {
        print!("{}", String::from_utf8_lossy(line));
    }
    #[cfg(not(test))]
    {
        // A line-buffered stdout writes a complete line straight through
        // (one write(2)); errors (closed stdout) are ignored, as in Redis.
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(line);
        let _ = out.flush();
    }
}

/// Logs an error the process is about to exit for: at warning level to the
/// log, and always to stderr too (like Redis's config errors), so whoever
/// started the server sees why it stopped even when `logfile` is set.
pub fn fatal(args: std::fmt::Arguments<'_>) {
    let msg = args.to_string();
    log_str(Level::Warning, &msg);
    eprintln!("{msg}");
}

/// The message of a caught panic payload.
pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic")
}

/// Lets a warning that can be triggered by requests (an OOM rejection, a
/// client dropped for its output buffer) through at most once per
/// interval, so a misbehaving workload cannot flood the log.
pub struct RateLimit {
    /// Milliseconds since `rate_limit_epoch` of the last allowed message
    /// (0: none yet).
    last_ms: std::sync::atomic::AtomicU64,
}

fn rate_limit_epoch() -> std::time::Instant {
    static EPOCH: std::sync::LazyLock<std::time::Instant> =
        std::sync::LazyLock::new(std::time::Instant::now);
    *EPOCH
}

impl RateLimit {
    pub const fn new() -> Self {
        RateLimit {
            last_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// True if the last allowed message is at least `interval` old.
    pub fn allow(&self, interval: std::time::Duration) -> bool {
        let now = rate_limit_epoch().elapsed().as_millis() as u64 + 1;
        let last = self.last_ms.load(Ordering::Relaxed);
        if last != 0 && now.saturating_sub(last) < interval.as_millis() as u64 {
            return false;
        }
        self.last_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }
}

impl Default for RateLimit {
    fn default() -> Self {
        Self::new()
    }
}

#[macro_export]
macro_rules! log_at {
    ($level:expr, $($arg:tt)+) => {{
        let level = $level;
        if $crate::log::enabled(level) {
            $crate::log::log_args(level, format_args!($($arg)+));
        }
    }};
}

/// Logs an error the process exits for, to the log and to stderr.
#[macro_export]
macro_rules! log_fatal {
    ($($arg:tt)+) => { $crate::log::fatal(format_args!($($arg)+)) };
}

/// Logs at `debug` level (`.`).
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)+) => { $crate::log_at!($crate::log::Level::Debug, $($arg)+) };
}

/// Logs at `verbose` level (`-`).
#[macro_export]
macro_rules! log_verbose {
    ($($arg:tt)+) => { $crate::log_at!($crate::log::Level::Verbose, $($arg)+) };
}

/// Logs at `notice` level (`*`).
#[macro_export]
macro_rules! log_notice {
    ($($arg:tt)+) => { $crate::log_at!($crate::log::Level::Notice, $($arg)+) };
}

/// Logs at `warning` level (`#`).
#[macro_export]
macro_rules! log_warning {
    ($($arg:tt)+) => { $crate::log_at!($crate::log::Level::Warning, $($arg)+) };
}

/// A `tracing` layer writing events through the Redis log: ERROR and WARN
/// become warnings, INFO notices, DEBUG and TRACE debug. Events are
/// filtered by `loglevel` at runtime, so CONFIG SET loglevel applies to
/// them too.
pub struct RedisLogLayer;

fn tracing_level(level: &tracing::Level) -> Level {
    match *level {
        tracing::Level::ERROR | tracing::Level::WARN => Level::Warning,
        tracing::Level::INFO => Level::Notice,
        tracing::Level::DEBUG | tracing::Level::TRACE => Level::Debug,
    }
}

struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let fields = std::mem::take(&mut self.0);
            let _ = write!(self.0, "{:?}", value);
            self.0.push_str(&fields);
        } else {
            let _ = write!(self.0, " {}={:?}", field.name(), value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            let fields = std::mem::take(&mut self.0);
            self.0.push_str(value);
            self.0.push_str(&fields);
        } else {
            let _ = write!(self.0, " {}={}", field.name(), value);
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RedisLogLayer {
    fn register_callsite(
        &self,
        _metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        // `loglevel` can change at runtime: never cache the decision.
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(
        &self,
        metadata: &tracing::Metadata<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        enabled(tracing_level(metadata.level()))
    }

    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let level = tracing_level(event.metadata().level());
        if !enabled(level) {
            return;
        }
        let mut v = MessageVisitor(String::new());
        event.record(&mut v);
        log_str(level, &v.0);
    }
}

#[cfg(test)]
pub(crate) static LOG_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> LocalTime {
        LocalTime {
            year: 2026,
            month: 9,
            day: 3,
            hour: 7,
            minute: 5,
            second: 9,
            millis: 42,
            utc_offset: -(7 * 3600 + 30 * 60),
            unix_ms: 1_791_000_309_042,
        }
    }

    #[test]
    fn test_legacy_format_matches_redis() {
        let line = format_line(
            Level::Notice,
            "Ready to accept connections tcp",
            4242,
            false,
            &t(),
            0,
            0,
        );
        assert_eq!(
            line,
            "4242:M 03 Oct 2026 07:05:09.042 * Ready to accept connections tcp\n"
        );
        let line = format_line(Level::Warning, "x", 1, true, &t(), 0, 0);
        assert_eq!(line, "1:S 03 Oct 2026 07:05:09.042 # x\n");
        assert!(format_line(Level::Debug, "x", 1, false, &t(), 0, 0).contains(" . x\n"));
        assert!(format_line(Level::Verbose, "x", 1, false, &t(), 0, 0).contains(" - x\n"));
    }

    #[test]
    fn test_timestamp_formats() {
        let iso = format_line(Level::Notice, "m", 1, false, &t(), 0, 1);
        assert_eq!(iso, "1:M 2026-10-03T07:05:09.042-07:30 * m\n");
        let ms = format_line(Level::Notice, "m", 1, false, &t(), 0, 2);
        assert_eq!(ms, "1:M 1791000309042 * m\n");
    }

    #[test]
    fn test_logfmt_format_escapes_message() {
        let line = format_line(Level::Verbose, "a \"b\"\nc", 7, true, &t(), 1, 0);
        assert_eq!(
            line,
            "pid=7 role=replica timestamp=\"03 Oct 2026 07:05:09.042\" level=info message=\"a 'b' c\"\n"
        );
    }

    #[test]
    fn test_long_messages_are_truncated_on_char_boundary() {
        let msg = "é".repeat(LOG_MAX_LEN);
        let line = format_line(Level::Notice, &msg, 1, false, &t(), 0, 0);
        let body = line.rsplit_once(" * ").unwrap().1;
        assert!(body.len() <= LOG_MAX_LEN);
        assert!(body.trim_end().chars().all(|c| c == 'é'));
    }

    #[test]
    fn test_level_filtering_and_config_values() {
        let _g = LOG_TEST_LOCK.lock();
        let prev = loglevel();
        set_loglevel("warning").unwrap();
        assert_eq!(loglevel(), "warning");
        assert!(enabled(Level::Warning));
        assert!(!enabled(Level::Notice));
        set_loglevel("NOTHING").unwrap();
        assert!(!enabled(Level::Warning));
        set_loglevel("debug").unwrap();
        assert!(enabled(Level::Debug));
        let err = set_loglevel("loud").unwrap_err();
        assert_eq!(
            err,
            "argument(s) must be one of the following: debug, verbose, notice, warning, nothing"
        );
        assert_eq!(loglevel(), "debug");
        set_loglevel(prev).unwrap();

        assert!(validate_log_format("logfmt").is_ok());
        assert!(validate_log_format("json").is_err());
        assert!(validate_log_timestamp_format("iso8601").is_ok());
        assert!(validate_log_timestamp_format("nope").is_err());
    }

    #[test]
    fn test_filtered_message_is_not_formatted() {
        let _g = LOG_TEST_LOCK.lock();
        let prev = loglevel();
        set_loglevel("warning").unwrap();
        struct Bomb;
        impl std::fmt::Display for Bomb {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("formatted a filtered-out message");
            }
        }
        crate::log_notice!("{}", Bomb);
        crate::log_debug!("{}", Bomb);
        set_loglevel(prev).unwrap();
    }

    #[test]
    fn test_logfile_append_and_reopen_after_rotation() {
        let _g = LOG_TEST_LOCK.lock();
        let dir = std::env::temp_dir().join(format!("rudis-log-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("server.log");
        let rotated = dir.join("server.log.1");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&rotated);
        std::fs::write(&path, "existing\n").unwrap();

        let prev_level = loglevel();
        set_loglevel("notice").unwrap();
        set_logfile(path.to_str().unwrap()).unwrap();
        crate::log_notice!("first {}", 1);
        crate::log_verbose!("hidden");
        std::fs::rename(&path, &rotated).unwrap();
        crate::log_warning!("second");
        request_reopen();
        crate::log_notice!("third");
        set_logfile("").unwrap();
        set_loglevel(prev_level).unwrap();

        let old = std::fs::read_to_string(&rotated).unwrap();
        let new = std::fs::read_to_string(&path).unwrap();
        assert!(old.starts_with("existing\n"), "{old}");
        assert!(old.contains(" * first 1\n"), "{old}");
        assert!(!old.contains("hidden"));
        assert!(!old.contains("second"), "{old}");
        assert!(new.contains(" # second\n"), "{new}");
        assert!(new.contains(" * third\n"), "{new}");
        let pid = std::process::id();
        assert!(new.starts_with(&format!("{pid}:")), "{new}");
        let _ = std::fs::remove_dir_all(&dir);

        assert!(set_logfile("/nonexistent-dir-rudis/x.log").is_err());
        assert_eq!(logfile(), "");
    }

    #[test]
    fn test_panic_message() {
        let p = std::panic::catch_unwind(|| panic!("boom {}", 1)).unwrap_err();
        assert_eq!(panic_message(&*p), "boom 1");
        let p = std::panic::catch_unwind(|| std::panic::panic_any(5u8)).unwrap_err();
        assert_eq!(panic_message(&*p), "unknown panic");
    }
}
