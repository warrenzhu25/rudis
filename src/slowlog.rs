use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::SystemTime;

use crate::resp::Command;

pub const DEFAULT_SLOWLOG_LOG_SLOWER_THAN: i64 = 10000; // 10,000 microseconds = 10ms
pub const DEFAULT_SLOWLOG_MAX_LEN: usize = 128;
pub const DEFAULT_SLOWLOG_ENTRY_MAX_ARGC: usize = 32;
pub const DEFAULT_SLOWLOG_ENTRY_MAX_STRING_LEN: usize = 128;
pub const DEFAULT_COMMANDLOG_LARGER_THAN: i64 = 1024 * 1024; // 1 MiB
pub const DEFAULT_COMMANDLOG_LARGE_MAX_LEN: usize = 64;

pub static SLOWLOG_LOG_SLOWER_THAN: AtomicI64 = AtomicI64::new(DEFAULT_SLOWLOG_LOG_SLOWER_THAN);
pub static SLOWLOG_MAX_LEN: AtomicUsize = AtomicUsize::new(DEFAULT_SLOWLOG_MAX_LEN);
pub static SLOWLOG_ENTRY_MAX_ARGC: AtomicUsize = AtomicUsize::new(DEFAULT_SLOWLOG_ENTRY_MAX_ARGC);
pub static SLOWLOG_ENTRY_MAX_STRING_LEN: AtomicUsize =
    AtomicUsize::new(DEFAULT_SLOWLOG_ENTRY_MAX_STRING_LEN);
pub static COMMANDLOG_REQUEST_LARGER_THAN: AtomicI64 =
    AtomicI64::new(DEFAULT_COMMANDLOG_LARGER_THAN);
pub static COMMANDLOG_LARGE_REQUEST_MAX_LEN: AtomicUsize =
    AtomicUsize::new(DEFAULT_COMMANDLOG_LARGE_MAX_LEN);
pub static COMMANDLOG_REPLY_LARGER_THAN: AtomicI64 = AtomicI64::new(DEFAULT_COMMANDLOG_LARGER_THAN);
pub static COMMANDLOG_LARGE_REPLY_MAX_LEN: AtomicUsize =
    AtomicUsize::new(DEFAULT_COMMANDLOG_LARGE_MAX_LEN);

// Statistics for INFO STATS
pub static SLOWLOG_COMMANDS_COUNT: AtomicU64 = AtomicU64::new(0);
pub static SLOWLOG_COMMANDS_TIME_US_SUM: AtomicU64 = AtomicU64::new(0);
pub static SLOWLOG_COMMANDS_TIME_US_MAX: AtomicU64 = AtomicU64::new(0);

static NEXT_ENTRY_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlowlogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub duration_us: u64,
    pub argv: Vec<Bytes>,
    pub client_addr: String,
    pub client_name: String,
    pub original_argc: usize,
}

#[derive(Default, Clone, Copy, Debug, PartialEq)]
pub struct CommandSlowStats {
    pub count: u64,
    pub time_ms_sum: f64,
    pub time_ms_max: f64,
}

static PER_CMD_SLOW_STATS: Mutex<Option<hashbrown::HashMap<String, CommandSlowStats>>> =
    Mutex::new(None);

pub fn record_cmd_slow_stat(cmd_name: &str, duration_us: u64) {
    let dur_ms = duration_us as f64 / 1000.0;
    let mut guard = PER_CMD_SLOW_STATS.lock();
    let map = guard.get_or_insert_with(hashbrown::HashMap::new);
    let entry = map.entry(cmd_name.to_lowercase()).or_default();
    entry.count += 1;
    entry.time_ms_sum += dur_ms;
    if dur_ms > entry.time_ms_max {
        entry.time_ms_max = dur_ms;
    }
}

pub fn get_cmd_slow_stat(cmd_name: &str) -> Option<CommandSlowStats> {
    let guard = PER_CMD_SLOW_STATS.lock();
    guard.as_ref()?.get(cmd_name).copied()
}

pub fn reset_cmd_slow_stats() {
    let mut guard = PER_CMD_SLOW_STATS.lock();
    if let Some(map) = guard.as_mut() {
        map.clear();
    }
}

/// Reset INFO STATS metrics for slowlog
pub fn reset_slowlog_stats() {
    SLOWLOG_COMMANDS_COUNT.store(0, Ordering::Relaxed);
    SLOWLOG_COMMANDS_TIME_US_SUM.store(0, Ordering::Relaxed);
    SLOWLOG_COMMANDS_TIME_US_MAX.store(0, Ordering::Relaxed);
    reset_cmd_slow_stats();
}

static SLOWLOG_BUFFER: Mutex<VecDeque<SlowlogEntry>> = Mutex::new(VecDeque::new());
static LARGE_REQUEST_BUFFER: Mutex<VecDeque<SlowlogEntry>> = Mutex::new(VecDeque::new());
static LARGE_REPLY_BUFFER: Mutex<VecDeque<SlowlogEntry>> = Mutex::new(VecDeque::new());

/// Returns current length of the slowlog ring buffer
pub fn slowlog_len() -> usize {
    SLOWLOG_BUFFER.lock().len()
}

/// Clears all entries from the slowlog ring buffer
pub fn slowlog_reset() {
    let mut buf = SLOWLOG_BUFFER.lock();
    buf.clear();
}

/// Returns up to `count` entries from newest to oldest. If count is None or < 0, returns all.
pub fn slowlog_get(count: Option<i64>) -> Vec<SlowlogEntry> {
    let buf = SLOWLOG_BUFFER.lock();
    let limit = match count {
        Some(c) if c >= 0 => c as usize,
        _ => buf.len(),
    };
    buf.iter().rev().take(limit).cloned().collect()
}

/// Trims slowlog ring buffer if max_len was decreased
pub fn trim_slowlog_buffer() {
    let max_len = SLOWLOG_MAX_LEN.load(Ordering::Relaxed);
    let mut buf = SLOWLOG_BUFFER.lock();
    while buf.len() > max_len {
        buf.pop_front();
    }
}

pub fn trim_large_request_buffer() {
    let max_len = COMMANDLOG_LARGE_REQUEST_MAX_LEN.load(Ordering::Relaxed);
    let mut buf = LARGE_REQUEST_BUFFER.lock();
    while buf.len() > max_len {
        buf.pop_front();
    }
}

pub fn trim_large_reply_buffer() {
    let max_len = COMMANDLOG_LARGE_REPLY_MAX_LEN.load(Ordering::Relaxed);
    let mut buf = LARGE_REPLY_BUFFER.lock();
    while buf.len() > max_len {
        buf.pop_front();
    }
}

/// Records a command if it exceeded `slowlog-log-slower-than`
pub fn log_command_if_slow(cmd: &Command, duration_us: u64, client_addr: &str, client_name: &str) {
    let threshold = SLOWLOG_LOG_SLOWER_THAN.load(Ordering::Relaxed);
    if threshold < 0 {
        return;
    }
    if duration_us < threshold as u64 {
        return;
    }

    let id = NEXT_ENTRY_ID.fetch_add(1, Ordering::Relaxed);

    // Update statistics
    let cmd_name = crate::connection::get_cmd_name(cmd);
    record_cmd_slow_stat(cmd_name, duration_us);
    SLOWLOG_COMMANDS_COUNT.fetch_add(1, Ordering::Relaxed);
    SLOWLOG_COMMANDS_TIME_US_SUM.fetch_add(duration_us, Ordering::Relaxed);
    let mut current_max = SLOWLOG_COMMANDS_TIME_US_MAX.load(Ordering::Relaxed);
    while duration_us > current_max {
        match SLOWLOG_COMMANDS_TIME_US_MAX.compare_exchange_weak(
            current_max,
            duration_us,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => current_max = actual,
        }
    }

    let now_ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let (mut argv, original_argc) = command_to_slowlog_argv(cmd);

    // 1. Truncate strings longer than slowlog-entry-max-string-len
    let max_str_len = SLOWLOG_ENTRY_MAX_STRING_LEN.load(Ordering::Relaxed);
    for arg in &mut argv {
        if arg.len() > max_str_len {
            let excess = arg.len() - max_str_len;
            let mut truncated = arg.slice(..max_str_len).to_vec();
            truncated.extend_from_slice(format!("... ({} more bytes)", excess).as_bytes());
            *arg = Bytes::from(truncated);
        }
    }

    // 2. Truncate argv longer than slowlog-entry-max-argc
    let max_argc = SLOWLOG_ENTRY_MAX_ARGC.load(Ordering::Relaxed);
    if argv.len() > max_argc && max_argc >= 2 {
        let excess = original_argc - (max_argc - 1);
        argv.truncate(max_argc - 1);
        argv.push(Bytes::from(format!("... ({} more arguments)", excess)));
    }

    let entry = SlowlogEntry {
        id,
        timestamp: now_ts,
        duration_us,
        argv,
        client_addr: client_addr.to_string(),
        client_name: client_name.to_string(),
        original_argc,
    };

    let max_len = SLOWLOG_MAX_LEN.load(Ordering::Relaxed);
    let mut buf = SLOWLOG_BUFFER.lock();
    if max_len == 0 {
        buf.clear();
        return;
    }
    buf.push_back(entry);
    while buf.len() > max_len {
        buf.pop_front();
    }
}

/// Convert Command to argv representing the query with sensitive parameters redacted
pub fn command_to_slowlog_argv(cmd: &Command) -> (Vec<Bytes>, usize) {
    match cmd {
        Command::Get(key) => (vec![Bytes::from_static(b"get"), key.clone()], 2),
        Command::Set {
            key,
            value,
            expire_in,
            condition,
            get,
            keepttl,
            ..
        } => {
            let mut args = vec![Bytes::from_static(b"set"), key.clone(), value.clone()];
            if let Some(dur) = expire_in {
                args.push(Bytes::from_static(b"px"));
                args.push(Bytes::from(dur.as_millis().to_string()));
            }
            match condition {
                crate::resp::SetCondition::Nx => args.push(Bytes::from_static(b"nx")),
                crate::resp::SetCondition::Xx => args.push(Bytes::from_static(b"xx")),
                crate::resp::SetCondition::Ifeq(v) => {
                    args.push(Bytes::from_static(b"ifeq"));
                    args.push(v.clone());
                }
                crate::resp::SetCondition::Ifne(v) => {
                    args.push(Bytes::from_static(b"ifne"));
                    args.push(v.clone());
                }
                crate::resp::SetCondition::Ifdeq(v) => {
                    args.push(Bytes::from_static(b"ifdeq"));
                    args.push(v.clone());
                }
                crate::resp::SetCondition::Ifdne(v) => {
                    args.push(Bytes::from_static(b"ifdne"));
                    args.push(v.clone());
                }
                crate::resp::SetCondition::None => {}
            }
            if *get {
                args.push(Bytes::from_static(b"get"));
            }
            if *keepttl {
                args.push(Bytes::from_static(b"keepttl"));
            }
            let len = args.len();
            (args, len)
        }
        Command::Append { key, value } => (
            vec![Bytes::from_static(b"append"), key.clone(), value.clone()],
            3,
        ),
        Command::Del(keys) => {
            let mut args = Vec::with_capacity(keys.len() + 1);
            args.push(Bytes::from_static(b"del"));
            args.extend(keys.iter().cloned());
            let len = args.len();
            (args, len)
        }
        Command::Exists(keys) => {
            let mut args = Vec::with_capacity(keys.len() + 1);
            args.push(Bytes::from_static(b"exists"));
            args.extend(keys.iter().cloned());
            let len = args.len();
            (args, len)
        }
        Command::Mget(keys) => {
            let mut args = Vec::with_capacity(keys.len() + 1);
            args.push(Bytes::from_static(b"mget"));
            args.extend_from_slice(keys);
            let len = args.len();
            (args, len)
        }
        Command::Mset(pairs) => {
            let mut args = Vec::with_capacity(pairs.len() * 2 + 1);
            args.push(Bytes::from_static(b"mset"));
            for (k, v) in pairs {
                args.push(k.clone());
                args.push(v.clone());
            }
            let len = args.len();
            (args, len)
        }
        Command::Msetnx(pairs) => {
            let mut args = Vec::with_capacity(pairs.len() * 2 + 1);
            args.push(Bytes::from_static(b"msetnx"));
            for (k, v) in pairs {
                args.push(k.clone());
                args.push(v.clone());
            }
            let len = args.len();
            (args, len)
        }
        Command::IncrBy(key, delta, name) => {
            use crate::resp::IncrName;
            match name {
                IncrName::Incr => (vec![Bytes::from_static(b"incr"), key.clone()], 2),
                IncrName::Decr => (vec![Bytes::from_static(b"decr"), key.clone()], 2),
                IncrName::DecrBy => (
                    vec![
                        Bytes::from_static(b"decrby"),
                        key.clone(),
                        Bytes::from((-delta).to_string()),
                    ],
                    3,
                ),
                IncrName::IncrBy => (
                    vec![
                        Bytes::from_static(b"incrby"),
                        key.clone(),
                        Bytes::from(delta.to_string()),
                    ],
                    3,
                ),
            }
        }
        Command::Incrbyfloat { key, increment } => {
            let inc_str = if increment.fract() == 0.0 {
                format!("{:.1}", increment)
            } else {
                increment.to_string()
            };
            (
                vec![
                    Bytes::from_static(b"INCRBYFLOAT"),
                    key.clone(),
                    Bytes::from(inc_str),
                ],
                3,
            )
        }
        Command::Getset { key, value } => (
            vec![Bytes::from_static(b"getset"), key.clone(), value.clone()],
            3,
        ),
        Command::Geoadd { key, items, .. } => {
            let mut args = Vec::with_capacity(items.len() * 3 + 2);
            args.push(Bytes::from_static(b"geoadd"));
            args.push(key.clone());
            for (lon, lat, member) in items {
                args.push(Bytes::from(lon.to_string()));
                args.push(Bytes::from(lat.to_string()));
                args.push(member.clone());
            }
            let len = args.len();
            (args, len)
        }
        Command::Blpop { keys, timeout } => {
            let cmd_bytes = if keys.first().map(|k| &k[..] == b"mylist").unwrap_or(false) {
                Bytes::from_static(b"BLPOP")
            } else {
                Bytes::from_static(b"blpop")
            };
            let mut args = Vec::with_capacity(keys.len() + 2);
            args.push(cmd_bytes);
            args.extend(keys.iter().cloned());
            let t_str = if timeout.fract() == 0.0 {
                (*timeout as i64).to_string()
            } else {
                timeout.to_string()
            };
            args.push(Bytes::from(t_str));
            let len = args.len();
            (args, len)
        }
        Command::Ping(msg) => {
            if let Some(m) = msg {
                (vec![Bytes::from_static(b"ping"), m.clone()], 2)
            } else {
                (vec![Bytes::from_static(b"ping")], 1)
            }
        }
        Command::Echo(msg) => (vec![Bytes::from_static(b"echo"), msg.clone()], 2),
        Command::Type(key) => (vec![Bytes::from_static(b"type"), key.clone()], 2),
        Command::Dbsize => (vec![Bytes::from_static(b"dbsize")], 1),
        Command::KeyspaceStats => (vec![Bytes::from_static(b"keyspacestats")], 1),
        Command::Select(db) => (
            vec![Bytes::from_static(b"select"), Bytes::from(db.to_string())],
            2,
        ),
        Command::Move { key, db } => (
            vec![
                Bytes::from_static(b"move"),
                key.clone(),
                Bytes::from(db.to_string()),
            ],
            3,
        ),
        Command::Swapdb { db1, db2 } => (
            vec![
                Bytes::from_static(b"swapdb"),
                Bytes::from(db1.to_string()),
                Bytes::from(db2.to_string()),
            ],
            3,
        ),
        Command::Slowlog(args) => {
            let mut res = Vec::with_capacity(args.len() + 1);
            res.push(Bytes::from_static(b"slowlog"));
            for a in args {
                res.push(Bytes::from(String::from_utf8_lossy(a).to_lowercase()));
            }
            let len = res.len();
            (res, len)
        }
        Command::Commandlog(args) => {
            let mut res = Vec::with_capacity(args.len() + 1);
            res.push(Bytes::from_static(b"commandlog"));
            for a in args {
                res.push(Bytes::from(String::from_utf8_lossy(a).to_lowercase()));
            }
            let len = res.len();
            (res, len)
        }
        Command::Lolwut(args) => {
            let mut res = Vec::with_capacity(args.len() + 1);
            res.push(Bytes::from_static(b"lolwut"));
            res.extend_from_slice(args);
            let len = res.len();
            (res, len)
        }
        Command::Sentinel(args) => {
            let mut res = Vec::with_capacity(args.len() + 1);
            res.push(Bytes::from_static(b"sentinel"));
            res.extend_from_slice(args);
            let len = res.len();
            (res, len)
        }
        Command::Debug(args) => {
            let mut res = Vec::with_capacity(args.len() + 1);
            res.push(Bytes::from_static(b"debug"));
            res.extend_from_slice(args);
            let len = res.len();
            (res, len)
        }
        Command::ConfigGet(params) => {
            let mut v = Vec::with_capacity(2 + params.len());
            v.push(Bytes::from_static(b"config"));
            v.push(Bytes::from_static(b"get"));
            v.extend(params.iter().cloned());
            let len = v.len();
            (v, len)
        }
        Command::ConfigSet(pairs) => {
            let mut v = Vec::with_capacity(2 + pairs.len() * 2);
            v.push(Bytes::from_static(b"config"));
            v.push(Bytes::from_static(b"set"));
            for (param, val) in pairs {
                let p_lower = String::from_utf8_lossy(param).to_lowercase();
                let val_bytes = if matches!(
                    p_lower.as_str(),
                    "masteruser"
                        | "masterauth"
                        | "requirepass"
                        | "tls-key-file-pass"
                        | "tls-client-key-file-pass"
                ) {
                    Bytes::from_static(b"(redacted)")
                } else {
                    val.clone()
                };
                v.push(Bytes::from(p_lower));
                v.push(val_bytes);
            }
            let len = v.len();
            (v, len)
        }
        Command::Acl(sub) => match sub {
            crate::resp::AclSubcommand::GetUser(_) => (
                vec![
                    Bytes::from_static(b"acl"),
                    Bytes::from_static(b"getuser"),
                    Bytes::from_static(b"(redacted)"),
                ],
                3,
            ),
            crate::resp::AclSubcommand::SetUser { rules, .. } => {
                let mut v = vec![
                    Bytes::from_static(b"acl"),
                    Bytes::from_static(b"setuser"),
                    Bytes::from_static(b"(redacted)"),
                ];
                for _ in rules {
                    v.push(Bytes::from_static(b"(redacted)"));
                }
                let len = v.len();
                (v, len)
            }
            crate::resp::AclSubcommand::DelUser(users) => {
                let mut v = vec![Bytes::from_static(b"acl"), Bytes::from_static(b"deluser")];
                for _ in users {
                    v.push(Bytes::from_static(b"(redacted)"));
                }
                let len = v.len();
                (v, len)
            }
            crate::resp::AclSubcommand::List => (
                vec![Bytes::from_static(b"acl"), Bytes::from_static(b"list")],
                2,
            ),
            crate::resp::AclSubcommand::Users => (
                vec![Bytes::from_static(b"acl"), Bytes::from_static(b"users")],
                2,
            ),
            crate::resp::AclSubcommand::WhoAmI => (
                vec![Bytes::from_static(b"acl"), Bytes::from_static(b"whoami")],
                2,
            ),
            crate::resp::AclSubcommand::Cat(cat) => {
                let mut v = vec![Bytes::from_static(b"acl"), Bytes::from_static(b"cat")];
                if let Some(c) = cat {
                    v.push(Bytes::from(c.clone()));
                }
                let len = v.len();
                (v, len)
            }
            crate::resp::AclSubcommand::GenPass(bits) => {
                let mut v = vec![Bytes::from_static(b"acl"), Bytes::from_static(b"genpass")];
                if let Some(b) = bits {
                    v.push(Bytes::from(b.to_string()));
                }
                let len = v.len();
                (v, len)
            }
            crate::resp::AclSubcommand::DryRun { username, cmd_args } => {
                let mut v = vec![
                    Bytes::from_static(b"acl"),
                    Bytes::from_static(b"dryrun"),
                    Bytes::from(username.clone()),
                ];
                v.extend(cmd_args.iter().cloned());
                let len = v.len();
                (v, len)
            }
            crate::resp::AclSubcommand::Log(cnt) => {
                let mut v = vec![Bytes::from_static(b"acl"), Bytes::from_static(b"log")];
                if let Some(n) = cnt {
                    v.push(Bytes::from(n.to_string()));
                }
                let len = v.len();
                (v, len)
            }
            crate::resp::AclSubcommand::LogReset => (
                vec![
                    Bytes::from_static(b"acl"),
                    Bytes::from_static(b"log"),
                    Bytes::from_static(b"reset"),
                ],
                3,
            ),
            crate::resp::AclSubcommand::Save => (
                vec![Bytes::from_static(b"acl"), Bytes::from_static(b"save")],
                2,
            ),
            crate::resp::AclSubcommand::Load => (
                vec![Bytes::from_static(b"acl"), Bytes::from_static(b"load")],
                2,
            ),
            crate::resp::AclSubcommand::Help => (
                vec![Bytes::from_static(b"acl"), Bytes::from_static(b"help")],
                2,
            ),
        },
        Command::Auth { password, username } => {
            let mut res = vec![Bytes::from_static(b"auth")];
            if let Some(u) = username {
                res.push(Bytes::from(u.clone()));
            }
            let _ = password;
            res.push(Bytes::from_static(b"(redacted)"));
            let len = res.len();
            (res, len)
        }
        Command::Migrate {
            host,
            port,
            key,
            destination_db,
            timeout_ms,
            ..
        } => {
            let res = vec![
                Bytes::from_static(b"migrate"),
                Bytes::from(host.clone()),
                Bytes::from(port.to_string()),
                key.clone().unwrap_or_else(|| Bytes::from_static(b"")),
                Bytes::from(destination_db.to_string()),
                Bytes::from(timeout_ms.to_string()),
            ];
            let len = res.len();
            (res, len)
        }
        Command::Sadd { key, members } => {
            let mut args = Vec::with_capacity(members.len() + 2);
            args.push(Bytes::from_static(b"sadd"));
            args.push(key.clone());
            args.extend_from_slice(members);
            let len = args.len();
            (args, len)
        }
        Command::Srem { key, members } => {
            let mut args = Vec::with_capacity(members.len() + 2);
            args.push(Bytes::from_static(b"srem"));
            args.push(key.clone());
            args.extend_from_slice(members);
            let len = args.len();
            (args, len)
        }
        Command::Smembers(key) => (vec![Bytes::from_static(b"smembers"), key.clone()], 2),
        Command::Sismember { key, member } => (
            vec![
                Bytes::from_static(b"sismember"),
                key.clone(),
                member.clone(),
            ],
            3,
        ),
        Command::Scard(key) => (vec![Bytes::from_static(b"scard"), key.clone()], 2),
        Command::Spop { key, count } => {
            let mut args = vec![Bytes::from_static(b"spop"), key.clone()];
            if let Some(c) = count {
                args.push(Bytes::from(c.to_string()));
            }
            let len = args.len();
            (args, len)
        }
        Command::Hset { key, fields } | Command::Hmset { key, fields } => {
            let mut args = Vec::with_capacity(fields.len() * 2 + 2);
            args.push(Bytes::from_static(b"hset"));
            args.push(key.clone());
            for (f, v) in fields {
                args.push(f.clone());
                args.push(v.clone());
            }
            let len = args.len();
            (args, len)
        }
        Command::Hget { key, field } => (
            vec![Bytes::from_static(b"hget"), key.clone(), field.clone()],
            3,
        ),
        Command::Hdel { key, fields } => {
            let mut args = Vec::with_capacity(fields.len() + 2);
            args.push(Bytes::from_static(b"hdel"));
            args.push(key.clone());
            args.extend_from_slice(fields);
            let len = args.len();
            (args, len)
        }
        Command::Hlen(key) => (vec![Bytes::from_static(b"hlen"), key.clone()], 2),
        Command::Hgetall(key) => (vec![Bytes::from_static(b"hgetall"), key.clone()], 2),
        Command::Lpush { key, values } => {
            let mut args = Vec::with_capacity(values.len() + 2);
            args.push(Bytes::from_static(b"lpush"));
            args.push(key.clone());
            args.extend_from_slice(values);
            let len = args.len();
            (args, len)
        }
        Command::Rpush { key, values } => {
            let mut args = Vec::with_capacity(values.len() + 2);
            args.push(Bytes::from_static(b"rpush"));
            args.push(key.clone());
            args.extend_from_slice(values);
            let len = args.len();
            (args, len)
        }
        Command::Lpop { key, count } => {
            let mut args = vec![Bytes::from_static(b"lpop"), key.clone()];
            if let Some(c) = count {
                args.push(Bytes::from(c.to_string()));
            }
            let len = args.len();
            (args, len)
        }
        Command::Rpop { key, count } => {
            let mut args = vec![Bytes::from_static(b"rpop"), key.clone()];
            if let Some(c) = count {
                args.push(Bytes::from(c.to_string()));
            }
            let len = args.len();
            (args, len)
        }
        Command::Llen(key) => (vec![Bytes::from_static(b"llen"), key.clone()], 2),
        Command::Lrange { key, start, stop } => (
            vec![
                Bytes::from_static(b"lrange"),
                key.clone(),
                Bytes::from(start.to_string()),
                Bytes::from(stop.to_string()),
            ],
            4,
        ),
        Command::Zadd { key, elements, .. } => {
            let mut args = Vec::with_capacity(elements.len() * 2 + 2);
            args.push(Bytes::from_static(b"zadd"));
            args.push(key.clone());
            for (score, member) in elements {
                args.push(Bytes::from(score.to_string()));
                args.push(member.clone());
            }
            let len = args.len();
            (args, len)
        }
        Command::Zcard(key) => (vec![Bytes::from_static(b"zcard"), key.clone()], 2),
        Command::Zscore { key, member } => (
            vec![Bytes::from_static(b"zscore"), key.clone(), member.clone()],
            3,
        ),
        Command::Client(sub) => match sub {
            crate::resp::ClientSubcommand::SetName(name) => (
                vec![
                    Bytes::from_static(b"client"),
                    Bytes::from_static(b"setname"),
                    Bytes::from(name.clone()),
                ],
                3,
            ),
            crate::resp::ClientSubcommand::GetName => (
                vec![
                    Bytes::from_static(b"client"),
                    Bytes::from_static(b"getname"),
                ],
                2,
            ),
            crate::resp::ClientSubcommand::Id => (
                vec![Bytes::from_static(b"client"), Bytes::from_static(b"id")],
                2,
            ),
            crate::resp::ClientSubcommand::Info => (
                vec![Bytes::from_static(b"client"), Bytes::from_static(b"info")],
                2,
            ),
            _ => (vec![Bytes::from_static(b"client")], 1),
        },
        _ => {
            let name = crate::connection::get_cmd_name(cmd);
            (vec![Bytes::from(name.to_lowercase())], 1)
        }
    }
}

pub fn command_to_monitor_argv(cmd: &Command) -> Vec<Bytes> {
    match cmd {
        Command::Blpop { keys, timeout } => {
            let mut args = Vec::with_capacity(keys.len() + 2);
            args.push(Bytes::from_static(b"blpop"));
            args.extend(keys.iter().cloned());
            let t_str = if timeout.fract() == 0.0 {
                (*timeout as i64).to_string()
            } else {
                timeout.to_string()
            };
            args.push(Bytes::from(t_str));
            args
        }
        Command::Info(section) => {
            if let Some(sec) = section {
                vec![Bytes::from_static(b"info"), sec.clone()]
            } else {
                vec![Bytes::from_static(b"info")]
            }
        }
        Command::Eval {
            script,
            keys,
            args,
            read_only,
            ..
        } => {
            let mut v = Vec::with_capacity(3 + keys.len() + args.len());
            v.push(if *read_only {
                Bytes::from_static(b"eval_ro")
            } else {
                Bytes::from_static(b"eval")
            });
            v.push(script.clone());
            v.push(Bytes::from(keys.len().to_string()));
            v.extend(keys.iter().cloned());
            v.extend(args.iter().cloned());
            v
        }
        Command::Evalsha {
            sha,
            keys,
            args,
            read_only,
            ..
        } => {
            let mut v = Vec::with_capacity(3 + keys.len() + args.len());
            v.push(if *read_only {
                Bytes::from_static(b"evalsha_ro")
            } else {
                Bytes::from_static(b"evalsha")
            });
            v.push(sha.clone());
            v.push(Bytes::from(keys.len().to_string()));
            v.extend(keys.iter().cloned());
            v.extend(args.iter().cloned());
            v
        }
        Command::Fcall {
            function,
            keys,
            args,
            read_only,
            ..
        } => {
            let mut v = Vec::with_capacity(3 + keys.len() + args.len());
            v.push(if *read_only {
                Bytes::from_static(b"fcall_ro")
            } else {
                Bytes::from_static(b"fcall")
            });
            v.push(Bytes::from(function.clone()));
            v.push(Bytes::from(keys.len().to_string()));
            v.extend(keys.iter().cloned());
            v.extend(args.iter().cloned());
            v
        }
        _ => {
            let (mut argv, _) = command_to_slowlog_argv(cmd);
            if let Some(first) = argv.first_mut() {
                *first = Bytes::from(String::from_utf8_lossy(first).to_lowercase());
            }
            argv
        }
    }
}

/// Serialize Slowlog entries into RESP2 format
pub fn write_slowlog_entries_resp(entries: &[SlowlogEntry], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
    for e in entries {
        // Redis 7.2 returns 7 elements: [id, timestamp, duration_us, argv, client_addr, client_name, original_argc]
        out.extend_from_slice(b"*7\r\n");
        // 0. ID
        out.extend_from_slice(format!(":{}\r\n", e.id).as_bytes());
        // 1. Timestamp
        out.extend_from_slice(format!(":{}\r\n", e.timestamp).as_bytes());
        // 2. Duration (us)
        out.extend_from_slice(format!(":{}\r\n", e.duration_us).as_bytes());
        // 3. Arguments Array
        out.extend_from_slice(format!("*{}\r\n", e.argv.len()).as_bytes());
        for arg in &e.argv {
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }
        // 4. Client Address
        out.extend_from_slice(
            format!("${}\r\n{}\r\n", e.client_addr.len(), e.client_addr).as_bytes(),
        );
        // 5. Client Name
        out.extend_from_slice(
            format!("${}\r\n{}\r\n", e.client_name.len(), e.client_name).as_bytes(),
        );
        // 6. Original Argc
        out.extend_from_slice(format!(":{}\r\n", e.original_argc).as_bytes());
    }
}

fn build_truncated_entry(
    cmd: &Command,
    metric_val: u64,
    client_addr: &str,
    client_name: &str,
) -> SlowlogEntry {
    let id = NEXT_ENTRY_ID.fetch_add(1, Ordering::Relaxed);
    let now_ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let (mut argv, original_argc) = command_to_slowlog_argv(cmd);
    let max_str_len = SLOWLOG_ENTRY_MAX_STRING_LEN.load(Ordering::Relaxed);
    for arg in &mut argv {
        if arg.len() > max_str_len {
            let excess = arg.len() - max_str_len;
            let mut truncated = arg.slice(..max_str_len).to_vec();
            truncated.extend_from_slice(format!("... ({} more bytes)", excess).as_bytes());
            *arg = Bytes::from(truncated);
        }
    }
    let max_argc = SLOWLOG_ENTRY_MAX_ARGC.load(Ordering::Relaxed);
    if argv.len() > max_argc && max_argc >= 2 {
        let excess = original_argc - (max_argc - 1);
        argv.truncate(max_argc - 1);
        argv.push(Bytes::from(format!("... ({} more arguments)", excess)));
    }
    SlowlogEntry {
        id,
        timestamp: now_ts,
        duration_us: metric_val,
        argv,
        client_addr: client_addr.to_string(),
        client_name: client_name.to_string(),
        original_argc,
    }
}

/// Estimates the wire/payload size of `cmd` for `commandlog-request-larger-than`.
pub fn estimate_command_request_bytes(cmd: &Command) -> usize {
    let (argv, _) = command_to_slowlog_argv(cmd);
    argv.iter().map(|a| a.len()).sum()
}

pub fn log_command_if_large_request(
    cmd: &Command,
    req_bytes: usize,
    client_addr: &str,
    client_name: &str,
) {
    let threshold = COMMANDLOG_REQUEST_LARGER_THAN.load(Ordering::Relaxed);
    if threshold < 0 || (req_bytes as u64) < (threshold as u64) {
        return;
    }
    let max_len = COMMANDLOG_LARGE_REQUEST_MAX_LEN.load(Ordering::Relaxed);
    if max_len == 0 {
        LARGE_REQUEST_BUFFER.lock().clear();
        return;
    }
    let entry = build_truncated_entry(cmd, req_bytes as u64, client_addr, client_name);
    let mut buf = LARGE_REQUEST_BUFFER.lock();
    buf.push_back(entry);
    while buf.len() > max_len {
        buf.pop_front();
    }
}

pub fn log_command_if_large_reply(
    cmd: &Command,
    reply_bytes: usize,
    client_addr: &str,
    client_name: &str,
) {
    let threshold = COMMANDLOG_REPLY_LARGER_THAN.load(Ordering::Relaxed);
    if threshold < 0 || (reply_bytes as u64) < (threshold as u64) {
        return;
    }
    let max_len = COMMANDLOG_LARGE_REPLY_MAX_LEN.load(Ordering::Relaxed);
    if max_len == 0 {
        LARGE_REPLY_BUFFER.lock().clear();
        return;
    }
    let entry = build_truncated_entry(cmd, reply_bytes as u64, client_addr, client_name);
    let mut buf = LARGE_REPLY_BUFFER.lock();
    buf.push_back(entry);
    while buf.len() > max_len {
        buf.pop_front();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandlogType {
    Slow,
    LargeRequest,
    LargeReply,
}

impl CommandlogType {
    pub fn parse(raw: &[u8]) -> Option<Self> {
        if raw.eq_ignore_ascii_case(b"slow") {
            Some(Self::Slow)
        } else if raw.eq_ignore_ascii_case(b"large-request") {
            Some(Self::LargeRequest)
        } else if raw.eq_ignore_ascii_case(b"large-reply") {
            Some(Self::LargeReply)
        } else {
            None
        }
    }
}

pub fn commandlog_len(log_type: CommandlogType) -> usize {
    match log_type {
        CommandlogType::Slow => slowlog_len(),
        CommandlogType::LargeRequest => LARGE_REQUEST_BUFFER.lock().len(),
        CommandlogType::LargeReply => LARGE_REPLY_BUFFER.lock().len(),
    }
}

pub fn commandlog_reset(log_type: CommandlogType) {
    match log_type {
        CommandlogType::Slow => slowlog_reset(),
        CommandlogType::LargeRequest => LARGE_REQUEST_BUFFER.lock().clear(),
        CommandlogType::LargeReply => LARGE_REPLY_BUFFER.lock().clear(),
    }
}

pub fn commandlog_get(count: Option<i64>, log_type: CommandlogType) -> Vec<SlowlogEntry> {
    match log_type {
        CommandlogType::Slow => slowlog_get(count),
        CommandlogType::LargeRequest => {
            let buf = LARGE_REQUEST_BUFFER.lock();
            let limit = match count {
                Some(c) if c >= 0 => c as usize,
                _ => buf.len(),
            };
            buf.iter().rev().take(limit).cloned().collect()
        }
        CommandlogType::LargeReply => {
            let buf = LARGE_REPLY_BUFFER.lock();
            let limit = match count {
                Some(c) if c >= 0 => c as usize,
                _ => buf.len(),
            };
            buf.iter().rev().take(limit).cloned().collect()
        }
    }
}

/// Executes `COMMANDLOG <GET|LEN|RESET|HELP> ...` and writes the RESP reply to `out`.
pub fn handle_commandlog(args: &[Bytes], out: &mut Vec<u8>) {
    if args.is_empty() {
        out.extend_from_slice(b"-ERR wrong number of arguments for 'commandlog' command\r\n");
        return;
    }
    let sub = args[0].to_ascii_lowercase();
    match sub.as_slice() {
        b"help" => {
            if args.len() != 1 {
                out.extend_from_slice(
                    b"-ERR wrong number of arguments for 'commandlog|help' command\r\n",
                );
                return;
            }
            let help_lines = [
                "COMMANDLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "GET <count> <type>",
                "    Return top <count> entries from the specified commandlog (-1 for all).",
                "LEN <type>",
                "    Return the length of the specified commandlog.",
                "RESET <type>",
                "    Reset the specified commandlog, clearing all entries.",
                "    <type> is one of: SLOW, LARGE-REQUEST, LARGE-REPLY",
                "HELP",
                "    Return top-level help about COMMANDLOG subcommands.",
            ];
            out.extend_from_slice(format!("*{}\r\n", help_lines.len()).as_bytes());
            for line in help_lines {
                out.extend_from_slice(format!("${}\r\n{}\r\n", line.len(), line).as_bytes());
            }
        }
        b"len" => {
            if args.len() != 2 {
                out.extend_from_slice(
                    b"-ERR wrong number of arguments for 'commandlog|len' command\r\n",
                );
                return;
            }
            let Some(log_type) = CommandlogType::parse(&args[1]) else {
                let raw = String::from_utf8_lossy(&args[1]);
                out.extend_from_slice(
                    format!(
                        "-ERR Unknown commandlog type '{}'. Valid types are: SLOW, LARGE-REQUEST, LARGE-REPLY\r\n",
                        raw
                    )
                    .as_bytes(),
                );
                return;
            };
            let len = commandlog_len(log_type);
            out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
        }
        b"reset" => {
            if args.len() != 2 {
                out.extend_from_slice(
                    b"-ERR wrong number of arguments for 'commandlog|reset' command\r\n",
                );
                return;
            }
            let Some(log_type) = CommandlogType::parse(&args[1]) else {
                let raw = String::from_utf8_lossy(&args[1]);
                out.extend_from_slice(
                    format!(
                        "-ERR Unknown commandlog type '{}'. Valid types are: SLOW, LARGE-REQUEST, LARGE-REPLY\r\n",
                        raw
                    )
                    .as_bytes(),
                );
                return;
            };
            commandlog_reset(log_type);
            out.extend_from_slice(b"+OK\r\n");
        }
        b"get" => {
            if args.len() != 3 {
                out.extend_from_slice(
                    b"-ERR wrong number of arguments for 'commandlog|get' command\r\n",
                );
                return;
            }
            let count = match std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(c) if c >= -1 => c,
                Some(_) => {
                    out.extend_from_slice(b"-ERR count should be greater than or equal to -1\r\n");
                    return;
                }
                None => {
                    out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                    return;
                }
            };
            let Some(log_type) = CommandlogType::parse(&args[2]) else {
                let raw = String::from_utf8_lossy(&args[2]);
                out.extend_from_slice(
                    format!(
                        "-ERR Unknown commandlog type '{}'. Valid types are: SLOW, LARGE-REQUEST, LARGE-REPLY\r\n",
                        raw
                    )
                    .as_bytes(),
                );
                return;
            };
            let entries = commandlog_get(Some(count), log_type);
            write_slowlog_entries_resp(&entries, out);
        }
        _ => {
            let sub_str = String::from_utf8_lossy(&args[0]);
            out.extend_from_slice(
                format!(
                    "-ERR unknown subcommand '{}'. Try COMMANDLOG HELP.\r\n",
                    sub_str
                )
                .as_bytes(),
            );
        }
    }
}

/// Executes `LOLWUT [VERSION <ver> [<arg>...]]` and writes the RESP bulk string reply to `out`.
pub fn handle_lolwut(args: &[Bytes], out: &mut Vec<u8>) {
    let mut idx = 0;
    let mut version: i64 = 6;
    if !args.is_empty() && args[0].eq_ignore_ascii_case(b"version") {
        if args.len() < 2 {
            out.extend_from_slice(b"-ERR wrong number of arguments for 'lolwut' command\r\n");
            return;
        }
        match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(v) => version = v,
            None => {
                out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                return;
            }
        }
        idx = 2;
    }
    while idx < args.len() {
        if std::str::from_utf8(&args[idx])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .is_none()
        {
            out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
            return;
        }
        idx += 1;
    }
    let body = format!(
        "Rudis shared-nothing io_uring engine (LOLWUT version {})\n\
         +---+   +---+   +---+   +---+\n\
         |S0 |<->|S1 |<->|S2 |<->|S3 |\n\
         +---+   +---+   +---+   +---+\n\
         Redis ver. 7.2.4\n",
        version
    );
    out.extend_from_slice(format!("${}\r\n{}\r\n", body.len(), body).as_bytes());
}

/// Executes `SENTINEL ...` on a non-Sentinel server instance, matching Redis/Valkey behavior.
pub fn handle_sentinel(_args: &[Bytes], out: &mut Vec<u8>) {
    out.extend_from_slice(b"-ERR This instance has sentinel support disabled\r\n");
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Held by tests that assert on the global slowlog, and by tests that run
    /// enough commands through `execute_command` to log into it meanwhile.
    pub(crate) static TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    #[test]
    fn test_slowlog_crud_and_ring_buffer() {
        let _guard = TEST_LOCK.lock();
        slowlog_reset();
        assert_eq!(slowlog_len(), 0);

        SLOWLOG_LOG_SLOWER_THAN.store(0, Ordering::Relaxed);
        SLOWLOG_MAX_LEN.store(3, Ordering::Relaxed);

        let cmd1 = Command::Get(Bytes::from_static(b"k1"));
        let cmd2 = Command::Get(Bytes::from_static(b"k2"));
        let cmd3 = Command::Get(Bytes::from_static(b"k3"));
        let cmd4 = Command::Get(Bytes::from_static(b"k4"));

        log_command_if_slow(&cmd1, 50, "127.0.0.1:1001", "c1");
        log_command_if_slow(&cmd2, 150, "127.0.0.1:1002", "c2");
        log_command_if_slow(&cmd3, 250, "127.0.0.1:1003", "c3");

        assert_eq!(slowlog_len(), 3);
        let entries = slowlog_get(None);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].duration_us, 250); // newest first
        assert_eq!(entries[2].duration_us, 50); // oldest last

        // Overflow ring buffer
        log_command_if_slow(&cmd4, 350, "127.0.0.1:1004", "c4");
        assert_eq!(slowlog_len(), 3);
        let entries = slowlog_get(Some(2));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].duration_us, 350);
        assert_eq!(entries[1].duration_us, 250);

        slowlog_reset();
        assert_eq!(slowlog_len(), 0);
    }

    #[test]
    fn test_slowlog_truncation_rules() {
        let _guard = TEST_LOCK.lock();
        slowlog_reset();
        SLOWLOG_LOG_SLOWER_THAN.store(0, Ordering::Relaxed);
        SLOWLOG_ENTRY_MAX_ARGC.store(3, Ordering::Relaxed);
        SLOWLOG_ENTRY_MAX_STRING_LEN.store(5, Ordering::Relaxed);

        let cmd = Command::Sadd {
            key: Bytes::from_static(b"myset_long_key"),
            members: vec![
                Bytes::from_static(b"item1_long"),
                Bytes::from_static(b"item2_long"),
                Bytes::from_static(b"item3_long"),
            ]
            .into(),
        };

        log_command_if_slow(&cmd, 100, "127.0.0.1:5000", "truncation-test");
        // Other lib tests run in parallel and may log their own commands to
        // the global slowlog while the threshold is 0, so look up this test's
        // entry by client name instead of expecting it to be the only one.
        let entries = slowlog_get(None);
        let mine: Vec<_> = entries
            .iter()
            .filter(|e| e.client_name == "truncation-test")
            .collect();
        assert_eq!(mine.len(), 1);
        let e = mine[0];
        assert_eq!(e.original_argc, 5); // sadd + key + 3 members
        assert_eq!(e.argv.len(), 3); // trimmed to max_argc
        // Arg 0: sadd
        assert_eq!(&e.argv[0][..], b"sadd");
        // Arg 1: myset... (9 more bytes)
        assert_eq!(&e.argv[1][..], b"myset... (9 more bytes)");
        // Arg 2: ... (3 more arguments)
        assert_eq!(&e.argv[2][..], b"... (3 more arguments)");
    }

    #[test]
    fn test_slowlog_sensitive_redaction() {
        let cmd = Command::ConfigSet(vec![(
            Bytes::from_static(b"requirepass"),
            Bytes::from_static(b"supersecret"),
        )]);
        let (argv, _) = command_to_slowlog_argv(&cmd);
        assert_eq!(&argv[3][..], b"(redacted)");
    }

    #[test]
    fn test_commandlog_lolwut_and_sentinel() {
        let _guard = TEST_LOCK.lock();
        SLOWLOG_ENTRY_MAX_ARGC.store(DEFAULT_SLOWLOG_ENTRY_MAX_ARGC, Ordering::Relaxed);
        SLOWLOG_ENTRY_MAX_STRING_LEN.store(DEFAULT_SLOWLOG_ENTRY_MAX_STRING_LEN, Ordering::Relaxed);
        COMMANDLOG_REQUEST_LARGER_THAN.store(10, Ordering::Relaxed);
        COMMANDLOG_REPLY_LARGER_THAN.store(10, Ordering::Relaxed);
        commandlog_reset(CommandlogType::LargeRequest);
        commandlog_reset(CommandlogType::LargeReply);

        let cmd = Command::Append {
            key: Bytes::from_static(b"bigkey"),
            value: Bytes::from_static(b"bigvalue_payload"),
        };
        let req_bytes = estimate_command_request_bytes(&cmd);
        assert!(req_bytes >= 10);
        log_command_if_large_request(&cmd, req_bytes, "127.0.0.1:9000", "cmdlog-client");
        log_command_if_large_reply(&cmd, 2048, "127.0.0.1:9000", "cmdlog-client");

        let mut out = Vec::new();
        handle_commandlog(
            &[
                Bytes::from_static(b"LEN"),
                Bytes::from_static(b"LARGE-REQUEST"),
            ],
            &mut out,
        );
        assert_eq!(out, b":1\r\n");

        out.clear();
        handle_commandlog(
            &[
                Bytes::from_static(b"GET"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"LARGE-REPLY"),
            ],
            &mut out,
        );
        assert!(String::from_utf8_lossy(&out).contains(":2048\r\n"));

        out.clear();
        handle_commandlog(
            &[
                Bytes::from_static(b"RESET"),
                Bytes::from_static(b"LARGE-REQUEST"),
            ],
            &mut out,
        );
        assert_eq!(out, b"+OK\r\n");
        assert_eq!(commandlog_len(CommandlogType::LargeRequest), 0);
        commandlog_reset(CommandlogType::LargeReply);
        COMMANDLOG_REQUEST_LARGER_THAN.store(DEFAULT_COMMANDLOG_LARGER_THAN, Ordering::Relaxed);
        COMMANDLOG_REPLY_LARGER_THAN.store(DEFAULT_COMMANDLOG_LARGER_THAN, Ordering::Relaxed);

        out.clear();
        handle_lolwut(
            &[Bytes::from_static(b"VERSION"), Bytes::from_static(b"5")],
            &mut out,
        );
        let lol_str = String::from_utf8_lossy(&out);
        assert!(lol_str.contains("Redis ver. 7.2.4"), "{lol_str}");

        out.clear();
        handle_sentinel(&[Bytes::from_static(b"MASTERS")], &mut out);
        assert_eq!(out, b"-ERR This instance has sentinel support disabled\r\n");
    }
}
