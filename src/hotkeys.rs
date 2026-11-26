//! `HOTKEYS` (Redis 8.4): a session that samples the commands run on this
//! server and reports the keys that cost the most CPU time and network
//! bytes. `HOTKEYS START METRICS <n> CPU|NET ... [COUNT k] [DURATION s]
//! [SAMPLE ratio] [SLOTS n slot ...]`, `STOP`, `RESET`, `GET`.
//!
//! Commands are accounted where their statistics are (see
//! `connection::CmdStatGuard`): each key a sampled command names gets the
//! command's CPU time and request bytes. While a session runs, commands
//! take the per-command path (as for CLUSTER SLOT-STATS).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static SESSION: Mutex<Option<Session>> = Mutex::new(None);
static SAMPLE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Whether a session is collecting now (one load).
#[inline(always)]
pub fn active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

struct Session {
    cpu: bool,
    net: bool,
    count: usize,
    sample: u64,
    /// The SLOTS asked for (sorted), None for all of the node's.
    slots: Option<Vec<u16>>,
    /// What `selected-slots` reports.
    ranges: Vec<(u16, u16)>,
    tracking: bool,
    start_ms: u64,
    end_ms: u64,
    deadline_ms: Option<u64>,
    rusage_start: (u64, u64),
    rusage_end: Option<(u64, u64)>,
    all_ns: u64,
    all_net: u64,
    selected_ns: u64,
    selected_net: u64,
    sampled_selected_ns: u64,
    sampled_selected_net: u64,
    /// Per key: the metric's total and when the key was first seen (ties
    /// rank in that order, as in Redis).
    by_cpu: HashMap<Bytes, (u64, u64)>,
    by_net: HashMap<Bytes, (u64, u64)>,
    seen: u64,
}

/// Most keys a session remembers per metric.
const MAX_TRACKED_KEYS: usize = 1 << 16;

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Process CPU time so far, (user, sys) in µs.
fn rusage() -> (u64, u64) {
    // SAFETY: `rusage` is plain integers, valid when zeroed.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: getrusage only writes the struct we pass, which outlives it.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let us = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    (us(ru.ru_utime), us(ru.ru_stime))
}

fn slots_to_ranges(slots: &[u16]) -> Vec<(u16, u16)> {
    let mut out: Vec<(u16, u16)> = Vec::new();
    for &s in slots {
        match out.last_mut() {
            Some((_, e)) if *e + 1 == s => *e = s,
            _ => out.push((s, s)),
        }
    }
    out
}

/// Accounts one command: `keys` it named, its CPU time and request bytes.
pub fn record(keys: &[Bytes], nanos: u64, net: u64) {
    let mut guard = SESSION.lock();
    let Some(s) = guard.as_mut().filter(|s| s.tracking) else {
        return;
    };
    if s.deadline_ms.is_some_and(|d| unix_ms() >= d) {
        stop_locked(s);
        return;
    }
    s.all_ns += nanos;
    s.all_net += net;
    let in_slots = |k: &Bytes| {
        s.slots
            .as_ref()
            .is_none_or(|sl| sl.binary_search(&crate::router::key_slot(k)).is_ok())
    };
    let selected: Vec<&Bytes> = keys.iter().filter(|k| in_slots(k)).collect();
    if s.slots.is_some() && !selected.is_empty() {
        s.selected_ns += nanos;
        s.selected_net += net;
    }
    let sampled = s.sample <= 1
        || SAMPLE_SEQ
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(s.sample);
    if !sampled || selected.is_empty() {
        return;
    }
    if s.slots.is_some() {
        s.sampled_selected_ns += nanos;
        s.sampled_selected_net += net;
    }
    for k in selected {
        s.seen += 1;
        let seen = s.seen;
        if s.cpu && (s.by_cpu.len() < MAX_TRACKED_KEYS || s.by_cpu.contains_key(k)) {
            s.by_cpu.entry(k.clone()).or_insert((0, seen)).0 += nanos;
        }
        if s.net && (s.by_net.len() < MAX_TRACKED_KEYS || s.by_net.contains_key(k)) {
            s.by_net.entry(k.clone()).or_insert((0, seen)).0 += net;
        }
    }
}

fn stop_locked(s: &mut Session) {
    if s.tracking {
        s.tracking = false;
        s.end_ms = unix_ms();
        s.rusage_end = Some(rusage());
        ACTIVE.store(false, Ordering::Relaxed);
    }
}

fn err(out: &mut Vec<u8>, msg: &str) {
    out.extend_from_slice(b"-ERR ");
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Runs `HOTKEYS <args>` on the server on `port`.
pub fn run(port: u16, cluster_enabled: bool, args: &[Bytes], out: &mut Vec<u8>) {
    let sub = args
        .first()
        .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
        .unwrap_or_default();
    match sub.as_str() {
        "START" => match parse_start(port, cluster_enabled, &args[1..]) {
            Ok(session) => {
                let mut guard = SESSION.lock();
                if guard.as_ref().is_some_and(|s| s.tracking) {
                    return err(out, "hotkey tracking session already in progress");
                }
                *guard = Some(session);
                ACTIVE.store(true, Ordering::Relaxed);
                out.extend_from_slice(b"+OK\r\n");
            }
            Err(e) => err(out, &e),
        },
        "STOP" => {
            if let Some(s) = SESSION.lock().as_mut() {
                stop_locked(s);
            }
            out.extend_from_slice(b"+OK\r\n");
        }
        "RESET" => {
            let mut guard = SESSION.lock();
            if guard.as_ref().is_some_and(|s| s.tracking) {
                return err(
                    out,
                    "hotkey tracking session in progress, stop tracking first",
                );
            }
            *guard = None;
            out.extend_from_slice(b"+OK\r\n");
        }
        "GET" => {
            let mut guard = SESSION.lock();
            match guard.as_mut() {
                None => out.extend_from_slice(b"*-1\r\n"),
                Some(s) => {
                    if s.deadline_ms.is_some_and(|d| unix_ms() >= d) {
                        stop_locked(s);
                    }
                    write_get(s, out);
                }
            }
        }
        "HELP" if args.len() == 1 => {
            let lines = [
                "HOTKEYS <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "START METRICS <count> [CPU] [NET] [COUNT <k>] [DURATION <seconds>]",
                "      [SAMPLE <ratio>] [SLOTS <count> <slot> [<slot> ...]]",
                "    Start tracking the keys commands access, by CPU time and/or network",
                "    bytes. A tracking session's results stay until RESET.",
                "STOP",
                "    Stop tracking; the results stay available to GET.",
                "GET",
                "    Return the session's top keys by each metric and its totals.",
                "RESET",
                "    Discard a stopped session's results.",
                "HELP",
                "    Print this help.",
            ];
            out.extend_from_slice(format!("*{}\r\n", lines.len()).as_bytes());
            for line in lines {
                out.extend_from_slice(format!("+{line}\r\n").as_bytes());
            }
        }
        _ => err(
            out,
            &format!("unknown subcommand '{sub}'. Try HOTKEYS HELP."),
        ),
    }
}

fn parse_start(port: u16, cluster_enabled: bool, args: &[Bytes]) -> Result<Session, String> {
    let upper = |a: &Bytes| String::from_utf8_lossy(a).to_ascii_uppercase();
    let int =
        |a: Option<&Bytes>| -> Option<i64> { std::str::from_utf8(a?).ok()?.parse::<i64>().ok() };
    let (mut cpu, mut net, mut metrics) = (false, false, false);
    let mut count = 10usize;
    let mut duration: Option<u64> = None;
    let mut sample = 1u64;
    let mut slots: Option<Vec<u16>> = None;
    let mut i = 0;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "METRICS" => {
                let n = int(args.get(i + 1))
                    .filter(|&n| n > 0)
                    .ok_or("METRICS count must be a positive integer".to_string())?
                    as usize;
                if args.len() < i + 2 + n {
                    return Err(
                        "METRICS count does not match number of metric types provided".to_string(),
                    );
                }
                let mut any = false;
                for a in &args[i + 2..i + 2 + n] {
                    match upper(a).as_str() {
                        "CPU" if cpu => return Err("METRICS CPU specified twice".into()),
                        "NET" if net => return Err("METRICS NET specified twice".into()),
                        "CPU" => {
                            cpu = true;
                            any = true;
                        }
                        "NET" => {
                            net = true;
                            any = true;
                        }
                        // Unknown metrics are allowed, for forward
                        // compatibility.
                        _ => {}
                    }
                }
                if !any {
                    return Err("METRICS no valid metrics given".to_string());
                }
                metrics = true;
                i += 2 + n;
            }
            "COUNT" => {
                count = int(args.get(i + 1))
                    .filter(|&c| (1..=64).contains(&c))
                    .ok_or("COUNT must be between 1 and 64".to_string())?
                    as usize;
                i += 2;
            }
            "DURATION" => {
                duration = Some(
                    int(args.get(i + 1))
                        .filter(|&d| d > 0)
                        .ok_or("DURATION must be a positive integer".to_string())?
                        as u64,
                );
                i += 2;
            }
            "SAMPLE" => {
                sample = int(args.get(i + 1))
                    .filter(|&s| s > 0)
                    .ok_or("SAMPLE ratio must be positive".to_string())?
                    as u64;
                i += 2;
            }
            "SLOTS" => {
                if !cluster_enabled {
                    return Err("SLOTS parameter cannot be used in non-cluster mode".into());
                }
                if slots.is_some() {
                    return Err("SLOTS parameter already specified".into());
                }
                let n = int(args.get(i + 1))
                    .filter(|&n| n > 0)
                    .ok_or("Invalid or out of range slot".to_string())?
                    as usize;
                if args.len() < i + 2 + n {
                    return Err("not enough slot numbers provided".into());
                }
                let mut list = Vec::with_capacity(n);
                for a in &args[i + 2..i + 2 + n] {
                    let s = int(Some(a))
                        .filter(|&s| (0..16384).contains(&s))
                        .ok_or("Invalid or out of range slot".to_string())?
                        as u16;
                    if list.contains(&s) {
                        return Err("duplicate slot number".into());
                    }
                    list.push(s);
                }
                let cluster = crate::cluster::get_cluster_hub(port);
                if let Some(s) = list.iter().find(|&&s| !cluster.owns_slot(s)) {
                    return Err(format!("slot {s} not handled by this node"));
                }
                list.sort_unstable();
                slots = Some(list);
                i += 2 + n;
            }
            _ => return Err("syntax error".to_string()),
        }
    }
    if !metrics {
        return Err("METRICS parameter is required".to_string());
    }
    let ranges = match &slots {
        Some(list) => slots_to_ranges(list),
        None if cluster_enabled => {
            let owned: Vec<u16> = {
                let cluster = crate::cluster::get_cluster_hub(port);
                (0..16384u16).filter(|&s| cluster.owns_slot(s)).collect()
            };
            slots_to_ranges(&owned)
        }
        None => vec![(0, 16383)],
    };
    let now = unix_ms();
    Ok(Session {
        cpu,
        net,
        count,
        sample,
        slots,
        ranges,
        tracking: true,
        start_ms: now,
        end_ms: 0,
        deadline_ms: duration.map(|d| now + d * 1000),
        rusage_start: rusage(),
        rusage_end: None,
        all_ns: 0,
        all_net: 0,
        selected_ns: 0,
        selected_net: 0,
        sampled_selected_ns: 0,
        sampled_selected_net: 0,
        by_cpu: HashMap::new(),
        by_net: HashMap::new(),
        seen: 0,
    })
}

fn write_get(s: &Session, out: &mut Vec<u8>) {
    use crate::connection::{write_resp_bulk, write_resp_integer};
    let field = |out: &mut Vec<u8>, name: &str| write_resp_bulk(out, name.as_bytes());
    let mut fields: Vec<(&str, Vec<u8>)> = Vec::new();
    let int = |v: i64| {
        let mut b = Vec::new();
        write_resp_integer(&mut b, v);
        b
    };
    fields.push(("tracking-active", int(i64::from(s.tracking))));
    fields.push(("sample-ratio", int(s.sample as i64)));
    let mut ranges = format!("*{}\r\n", s.ranges.len()).into_bytes();
    for &(a, b) in &s.ranges {
        if a == b {
            ranges.extend_from_slice(b"*1\r\n");
            write_resp_integer(&mut ranges, a as i64);
        } else {
            ranges.extend_from_slice(b"*2\r\n");
            write_resp_integer(&mut ranges, a as i64);
            write_resp_integer(&mut ranges, b as i64);
        }
    }
    fields.push(("selected-slots", ranges));
    let selected = s.slots.is_some();
    if selected && s.sample > 1 {
        fields.push((
            "sampled-commands-selected-slots-us",
            int((s.sampled_selected_ns / 1000) as i64),
        ));
    }
    if selected {
        fields.push((
            "all-commands-selected-slots-us",
            int((s.selected_ns / 1000) as i64),
        ));
    }
    fields.push(("all-commands-all-slots-us", int((s.all_ns / 1000) as i64)));
    if selected && s.sample > 1 {
        fields.push((
            "net-bytes-sampled-commands-selected-slots",
            int(s.sampled_selected_net as i64),
        ));
    }
    if selected {
        fields.push((
            "net-bytes-all-commands-selected-slots",
            int(s.selected_net as i64),
        ));
    }
    fields.push(("net-bytes-all-commands-all-slots", int(s.all_net as i64)));
    let end = if s.tracking { unix_ms() } else { s.end_ms };
    fields.push(("collection-start-time-unix-ms", int(s.start_ms as i64)));
    fields.push((
        "collection-duration-ms",
        int(end.saturating_sub(s.start_ms) as i64),
    ));
    let (u1, s1) = s.rusage_end.unwrap_or_else(rusage);
    let (u0, s0) = s.rusage_start;
    let top = |m: &HashMap<Bytes, (u64, u64)>, scale: u64| {
        let mut v: Vec<(&Bytes, u64, u64)> = m
            .iter()
            .map(|(k, &(x, seen))| (k, x / scale, seen))
            .collect();
        v.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.2.cmp(&b.2)));
        v.truncate(s.count);
        let mut b = format!("*{}\r\n", v.len() * 2).into_bytes();
        for (k, x, _) in v {
            write_resp_bulk(&mut b, k);
            write_resp_integer(&mut b, x as i64);
        }
        b
    };
    if s.cpu {
        fields.push((
            "total-cpu-time-user-ms",
            int((u1.saturating_sub(u0) / 1000) as i64),
        ));
        fields.push((
            "total-cpu-time-sys-ms",
            int((s1.saturating_sub(s0) / 1000) as i64),
        ));
    }
    if s.net {
        fields.push(("total-net-bytes", int(s.all_net as i64)));
    }
    if s.cpu {
        fields.push(("by-cpu-time-us", top(&s.by_cpu, 1000)));
    }
    if s.net {
        fields.push(("by-net-bytes", top(&s.by_net, 1)));
    }
    // One element: the session's fields (a map for RESP3 clients).
    out.extend_from_slice(format!("*1\r\n*{}\r\n", fields.len() * 2).as_bytes());
    for (name, value) in fields {
        field(out, name);
        out.extend_from_slice(&value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_ranges_merge_runs() {
        assert_eq!(
            slots_to_ranges(&[0, 1, 2, 5, 6, 10]),
            vec![(0, 2), (5, 6), (10, 10)]
        );
    }

    #[test]
    fn start_validates_like_redis() {
        let a = |s: &str| -> Vec<Bytes> {
            s.split_whitespace()
                .map(|w| Bytes::copy_from_slice(w.as_bytes()))
                .collect()
        };
        let e = |s: &str| parse_start(1, false, &a(s)).err().unwrap_or_default();
        assert!(e("").contains("METRICS parameter is required"));
        assert!(e("METRICS 0").contains("METRICS count"));
        assert!(e("METRICS 2 CPU").contains("does not match"));
        assert!(e("METRICS 1 CPU NET").contains("syntax error"));
        assert!(e("METRICS 1 GPU").contains("no valid metrics"));
        assert!(e("METRICS 2 CPU CPU").contains("METRICS CPU"));
        assert!(e("METRICS 1 CPU COUNT 65").contains("between 1 and 64"));
        assert!(e("METRICS 1 CPU SAMPLE 0").contains("must be positive"));
        assert!(e("METRICS 1 CPU SLOTS 1 0").contains("non-cluster"));
        assert!(parse_start(1, false, &a("METRICS 2 GPU CPU")).is_ok());
    }
}
