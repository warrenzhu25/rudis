use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::SystemTime;

use crate::resp::Command;

pub const DEFAULT_SLOWLOG_LOG_SLOWER_THAN: i64 = 10000; // 10,000 microseconds = 10ms
pub const DEFAULT_SLOWLOG_MAX_LEN: usize = 128;
pub const DEFAULT_SLOWLOG_ENTRY_MAX_ARGC: usize = 32;
pub const DEFAULT_SLOWLOG_ENTRY_MAX_STRING_LEN: usize = 128;

pub static SLOWLOG_LOG_SLOWER_THAN: AtomicI64 = AtomicI64::new(DEFAULT_SLOWLOG_LOG_SLOWER_THAN);
pub static SLOWLOG_MAX_LEN: AtomicUsize = AtomicUsize::new(DEFAULT_SLOWLOG_MAX_LEN);
pub static SLOWLOG_ENTRY_MAX_ARGC: AtomicUsize = AtomicUsize::new(DEFAULT_SLOWLOG_ENTRY_MAX_ARGC);
pub static SLOWLOG_ENTRY_MAX_STRING_LEN: AtomicUsize =
    AtomicUsize::new(DEFAULT_SLOWLOG_ENTRY_MAX_STRING_LEN);

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
    let mut guard = PER_CMD_SLOW_STATS.lock().unwrap();
    let map = guard.get_or_insert_with(hashbrown::HashMap::new);
    let entry = map.entry(cmd_name.to_lowercase()).or_default();
    entry.count += 1;
    entry.time_ms_sum += dur_ms;
    if dur_ms > entry.time_ms_max {
        entry.time_ms_max = dur_ms;
    }
}

pub fn get_cmd_slow_stat(cmd_name: &str) -> Option<CommandSlowStats> {
    let guard = PER_CMD_SLOW_STATS.lock().unwrap();
    guard.as_ref()?.get(cmd_name).copied()
}

pub fn reset_cmd_slow_stats() {
    let mut guard = PER_CMD_SLOW_STATS.lock().unwrap();
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

/// Returns current length of the slowlog ring buffer
pub fn slowlog_len() -> usize {
    SLOWLOG_BUFFER.lock().unwrap().len()
}

/// Clears all entries from the slowlog ring buffer
pub fn slowlog_reset() {
    let mut buf = SLOWLOG_BUFFER.lock().unwrap();
    buf.clear();
}

/// Returns up to `count` entries from newest to oldest. If count is None or < 0, returns all.
pub fn slowlog_get(count: Option<i64>) -> Vec<SlowlogEntry> {
    let buf = SLOWLOG_BUFFER.lock().unwrap();
    let limit = match count {
        Some(c) if c >= 0 => c as usize,
        _ => buf.len(),
    };
    buf.iter().rev().take(limit).cloned().collect()
}

/// Trims slowlog ring buffer if max_len was decreased
pub fn trim_slowlog_buffer() {
    let max_len = SLOWLOG_MAX_LEN.load(Ordering::Relaxed);
    let mut buf = SLOWLOG_BUFFER.lock().unwrap();
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
    let mut buf = SLOWLOG_BUFFER.lock().unwrap();
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
        Command::IncrBy(key, delta) => {
            if *delta == 1 {
                (vec![Bytes::from_static(b"incr"), key.clone()], 2)
            } else if *delta == -1 {
                (vec![Bytes::from_static(b"decr"), key.clone()], 2)
            } else if *delta < 0 {
                (
                    vec![
                        Bytes::from_static(b"decrby"),
                        key.clone(),
                        Bytes::from((-delta).to_string()),
                    ],
                    3,
                )
            } else {
                (
                    vec![
                        Bytes::from_static(b"incrby"),
                        key.clone(),
                        Bytes::from(delta.to_string()),
                    ],
                    3,
                )
            }
        }
        Command::Incrbyfloat { key, increment } => (
            vec![
                Bytes::from_static(b"incrbyfloat"),
                key.clone(),
                Bytes::from(increment.to_string()),
            ],
            3,
        ),
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
        Command::Select(db) => (
            vec![Bytes::from_static(b"select"), Bytes::from(db.to_string())],
            2,
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
        Command::Debug(args) => {
            let mut res = Vec::with_capacity(args.len() + 1);
            res.push(Bytes::from_static(b"debug"));
            res.extend_from_slice(args);
            let len = res.len();
            (res, len)
        }
        Command::ConfigGet(param) => (
            vec![
                Bytes::from_static(b"config"),
                Bytes::from_static(b"get"),
                param.clone(),
            ],
            3,
        ),
        Command::ConfigSet(param, val) => {
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
            (
                vec![
                    Bytes::from_static(b"config"),
                    Bytes::from_static(b"set"),
                    Bytes::from(p_lower),
                    val_bytes,
                ],
                4,
            )
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
            crate::resp::AclSubcommand::Cat => (
                vec![Bytes::from_static(b"acl"), Bytes::from_static(b"cat")],
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slowlog_crud_and_ring_buffer() {
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

        log_command_if_slow(&cmd, 100, "127.0.0.1:5000", "test");
        let entries = slowlog_get(None);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
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
        let cmd = Command::ConfigSet(
            Bytes::from_static(b"requirepass"),
            Bytes::from_static(b"supersecret"),
        );
        let (argv, _) = command_to_slowlog_argv(&cmd);
        assert_eq!(&argv[3][..], b"(redacted)");
    }
}
