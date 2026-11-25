//! `CLUSTER SLOT-STATS` (Valkey 8): per-slot statistics of this node.
//!
//! `key-count` is always reported (each shard's table keeps a per-slot key
//! count). With `cluster-slot-stats-enabled` (`yes`, `no`, or any of
//! `cpu net mem`), it also reports `cpu-usec`, `network-bytes-in` and
//! `network-bytes-out` (counted per shard as commands run, see
//! [`record`]) and `memory-bytes` (computed when asked for). Memory tracking
//! can only be on from startup: once off, `CONFIG SET` can't turn it on.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

pub const CPU: u8 = 1;
pub const NET: u8 = 2;
pub const MEM: u8 = 4;

static FLAGS: AtomicU8 = AtomicU8::new(0);
/// Memory tracking may be (re)enabled: only until startup ends, and only
/// while it was on since.
static MEM_ALLOWED: AtomicBool = AtomicBool::new(true);
static STARTED: AtomicBool = AtomicBool::new(false);

/// The enabled metrics (`CPU | NET | MEM`).
#[inline]
pub fn flags() -> u8 {
    FLAGS.load(Ordering::Relaxed)
}

/// Parses a `cluster-slot-stats-enabled` value.
fn parse(val: &str) -> Result<u8, String> {
    let mut f = 0;
    for tok in val.split_whitespace() {
        f |= match tok.to_ascii_lowercase().as_str() {
            "yes" => CPU | NET | MEM,
            "no" => 0,
            "cpu" => CPU,
            "net" => NET,
            "mem" => MEM,
            other => {
                return Err(format!(
                    "argument(s) must be one of the following: yes, no, cpu, net, mem (got '{other}')"
                ));
            }
        };
    }
    Ok(f)
}

/// `CONFIG SET cluster-slot-stats-enabled` (and the config file directive,
/// before [`startup_done`]).
pub fn set(val: &str) -> Result<(), String> {
    let f = parse(val)?;
    if f & MEM != 0 && (STARTED.load(Ordering::Relaxed) && !MEM_ALLOWED.load(Ordering::Relaxed)) {
        return Err("memory tracking cannot be enabled at runtime; set cluster-slot-stats-enabled with mem at startup".into());
    }
    if f & MEM == 0 && STARTED.load(Ordering::Relaxed) {
        MEM_ALLOWED.store(false, Ordering::Relaxed);
    }
    FLAGS.store(f, Ordering::Relaxed);
    Ok(())
}

/// The value `CONFIG GET` reports.
pub fn config_value() -> String {
    match flags() {
        0 => "no".into(),
        f if f == CPU | NET | MEM => "yes".into(),
        f => [(CPU, "cpu"), (NET, "net"), (MEM, "mem")]
            .iter()
            .filter(|(b, _)| f & b != 0)
            .map(|(_, n)| *n)
            .collect::<Vec<_>>()
            .join(" "),
    }
}

/// Startup config is applied: from now on memory tracking can't be
/// turned on unless it is on already.
pub fn startup_done() {
    if flags() & MEM == 0 {
        MEM_ALLOWED.store(false, Ordering::Relaxed);
    }
    STARTED.store(true, Ordering::Relaxed);
}

/// `cpu-usec`, `network-bytes-in` and `network-bytes-out` per slot. Global
/// (each slot lives on one shard, so nothing contends), allocated on first
/// use.
static COUNTERS: std::sync::OnceLock<Box<[[std::sync::atomic::AtomicU64; 3]]>> =
    std::sync::OnceLock::new();

fn counters_table() -> &'static [[std::sync::atomic::AtomicU64; 3]] {
    COUNTERS.get_or_init(|| (0..16384).map(|_| Default::default()).collect())
}

/// Adds a command's CPU time and network bytes to `slot`.
#[inline]
pub fn record(slot: u16, cpu_usec: u64, bytes_in: u64, bytes_out: u64) {
    let e = &counters_table()[slot as usize & 16383];
    for (c, v) in e.iter().zip([cpu_usec, bytes_in, bytes_out]) {
        if v != 0 {
            c.fetch_add(v, Ordering::Relaxed);
        }
    }
}

/// The counters of `slot`.
pub fn counters(slot: u16) -> [u64; 3] {
    match COUNTERS.get() {
        Some(t) => t[slot as usize & 16383]
            .each_ref()
            .map(|c| c.load(Ordering::Relaxed)),
        None => [0; 3],
    }
}

/// Zeroes the counters: all (`CONFIG RESETSTAT`), or `slot`'s (it
/// changed owner).
pub fn reset(slot: Option<u16>) {
    let Some(t) = COUNTERS.get() else { return };
    let zero = |e: &[std::sync::atomic::AtomicU64; 3]| {
        e.iter().for_each(|c| c.store(0, Ordering::Relaxed))
    };
    match slot {
        Some(s) => zero(&t[s as usize & 16383]),
        None => t.iter().for_each(zero),
    }
}

/// The slots owned when last looked at: a slot that changed owner since
/// starts from zero, as Valkey resets a slot's stats when it is assigned
/// or removed.
static LAST_OWNED: parking_lot::Mutex<Option<Vec<bool>>> = parking_lot::Mutex::new(None);

/// Resets the counters of every slot whose ownership changed since the
/// last call, given the slots owned now.
pub fn reset_changed_owners(owned: &[u16]) {
    let mut now = vec![false; 16384];
    for &s in owned {
        now[s as usize & 16383] = true;
    }
    let mut last = LAST_OWNED.lock();
    if let Some(prev) = last.as_ref() {
        for (slot, (a, b)) in prev.iter().zip(&now).enumerate() {
            if a != b {
                reset(Some(slot as u16));
            }
        }
    }
    *last = Some(now);
}

/// Whether commands are being accounted per slot: cluster mode with CPU or
/// network tracking on.
#[inline]
pub fn accounting(cluster: bool) -> bool {
    cluster && flags() & (CPU | NET) != 0
}

thread_local! {
    /// The slot the command about to run is accounted to, handed from the
    /// read loop to the command's statistics (taken before its first
    /// await, so another client's command can't see it).
    static EXEC_SLOT: std::cell::Cell<Option<u16>> = const { std::cell::Cell::new(None) };
}

/// Hands `slot` to the next command that runs on this thread.
#[inline]
pub fn hand_over(slot: Option<u16>) {
    EXEC_SLOT.set(slot);
}

/// The slot handed over for the command now starting (and clears it).
#[inline]
pub fn take_exec_slot() -> Option<u16> {
    EXEC_SLOT.take()
}

/// Records a command's CPU time against `slot` if CPU tracking is on.
#[inline]
pub fn record_cpu(slot: Option<u16>, nanos: u64) {
    if let Some(s) = slot
        && flags() & CPU != 0
    {
        record(s, nanos / 1000, 0, 0);
    }
}

thread_local! {
    /// The slot the running script is accounted to (-1: none yet), and
    /// whether it touched a key of another slot: then, as in Valkey, the
    /// script isn't accounted to any slot.
    static SCRIPT_SLOT: std::cell::Cell<i32> = const { std::cell::Cell::new(-1) };
    static SCRIPT_CROSSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// The last command's accounting was dropped (a cross-slot script):
    /// the read loop doesn't count its bytes either.
    static DROPPED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A script starts, accounted to its first declared key's slot.
pub fn script_begin(first_key_slot: Option<u16>) {
    SCRIPT_SLOT.set(first_key_slot.map_or(-1, i32::from));
    SCRIPT_CROSSED.set(false);
}

/// A script that may touch keys of any slot touched one in `slot`.
#[inline]
pub fn script_touch(slot: u16) {
    match SCRIPT_SLOT.get() {
        -1 => SCRIPT_SLOT.set(i32::from(slot)),
        s if s != i32::from(slot) => SCRIPT_CROSSED.set(true),
        _ => {}
    }
}

/// Called as a command's statistics are recorded: whether a script it ran
/// crossed slots (then nothing of it is accounted to a slot).
pub fn take_crossed() -> bool {
    let c = SCRIPT_CROSSED.take();
    DROPPED.set(c);
    c
}

/// Whether network bytes are accounted per slot: cluster mode with
/// network tracking on.
#[inline]
pub fn net_accounting() -> bool {
    flags() & NET != 0 && crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed)
}

/// Splits RESP commands (arrays of bulk strings) in `bytes` and returns,
/// for each, its length and its first argument after the name (its key,
/// for the writes a replication stream carries).
pub fn resp_commands(bytes: &[u8]) -> Vec<(usize, &[u8])> {
    fn line(b: &[u8], at: usize) -> Option<(&[u8], usize)> {
        let end = at + b.get(at..)?.windows(2).position(|w| w == b"\r\n")?;
        Some((&b[at..end], end + 2))
    }
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let start = pos;
        let Some((hdr, mut p)) = line(bytes, pos) else {
            break;
        };
        let Some(n) = hdr
            .strip_prefix(b"*")
            .and_then(|n| std::str::from_utf8(n).ok()?.parse::<usize>().ok())
        else {
            break;
        };
        let mut key: &[u8] = &[];
        let mut name: &[u8] = &[];
        for i in 0..n {
            let Some((len_line, q)) = line(bytes, p) else {
                return out;
            };
            let Some(len) = len_line
                .strip_prefix(b"$")
                .and_then(|l| std::str::from_utf8(l).ok()?.parse::<usize>().ok())
            else {
                return out;
            };
            match i {
                0 => name = bytes.get(q..q + len).unwrap_or(&[]),
                1 => key = bytes.get(q..q + len).unwrap_or(&[]),
                _ => {}
            }
            p = q + len + 2;
        }
        pos = p.min(bytes.len());
        // SPUBLISH reaches replicas here but goes over the cluster bus in
        // Valkey, which doesn't count it: leave it out.
        if name.eq_ignore_ascii_case(b"spublish") {
            key = &[];
        }
        out.push((pos - start, key));
    }
    out
}

/// A master streamed `bytes` (its own writes) to `replicas` replicas: each
/// command's bytes, times the replicas, go out on its key's slot.
pub fn record_replication_egress(bytes: &[u8], replicas: usize) {
    if replicas == 0 {
        return;
    }
    for (len, key) in resp_commands(bytes) {
        if !key.is_empty() {
            record(crate::router::key_slot(key), 0, 0, (len * replicas) as u64);
        }
    }
}

/// One connection's network accounting: what a transaction has received
/// so far goes to its slot at EXEC, as Valkey counts it.
#[derive(Debug, Default)]
pub struct ClientAcct {
    pending_in: u64,
    tx_slot: Option<u16>,
    /// The request bytes of the command now running, counted after it.
    deferred_in: u64,
}

/// Where a command stands with respect to MULTI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxRole {
    /// Not in a transaction.
    None,
    Multi,
    /// Queued inside MULTI.
    Queued,
    Exec,
    Discard,
}

impl ClientAcct {
    /// Before a command runs: counts its `in_bytes` (now, or at EXEC) and
    /// returns the slot its CPU time and reply go to.
    pub fn before(&mut self, role: TxRole, cmd_slot: Option<u16>, in_bytes: u64) -> Option<u16> {
        match role {
            TxRole::Multi => {
                self.pending_in = in_bytes;
                self.tx_slot = None;
                None
            }
            TxRole::Queued => {
                self.pending_in += in_bytes;
                if cmd_slot.is_some() {
                    self.tx_slot = cmd_slot;
                }
                None
            }
            TxRole::Discard => {
                *self = Self::default();
                None
            }
            TxRole::Exec => {
                let slot = self.tx_slot;
                self.deferred_in = self.pending_in + in_bytes;
                self.pending_in = 0;
                self.tx_slot = None;
                slot
            }
            TxRole::None => {
                self.deferred_in = in_bytes;
                cmd_slot
            }
        }
    }

    /// After the command: its request (counted once it ran, so a blocked
    /// command counts when it unblocks) and its reply, `out_bytes` long, go
    /// to `slot`, unless the command's accounting was dropped.
    pub fn after(&mut self, slot: Option<u16>, out_bytes: u64) {
        let in_bytes = std::mem::take(&mut self.deferred_in);
        if DROPPED.take() {
            return;
        }
        if let Some(s) = slot
            && flags() & NET != 0
        {
            record(s, 0, in_bytes, out_bytes);
        }
    }
}

/// The metrics, in reply order, with the flag each needs (0: always).
pub const METRICS: [(&str, u8); 5] = [
    ("key-count", 0),
    ("cpu-usec", CPU),
    ("network-bytes-in", NET),
    ("network-bytes-out", NET),
    ("memory-bytes", MEM),
];

/// Which part of the slots `CLUSTER SLOT-STATS` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// `SLOTSRANGE start end`: the owned slots in it, by slot.
    Range(u16, u16),
    /// `ORDERBY metric [LIMIT n] [ASC|DESC]`: the first `n` owned slots by
    /// `metric` (an index into [`METRICS`]), ties by slot.
    OrderBy {
        metric: usize,
        limit: usize,
        desc: bool,
    },
}

/// Parses the arguments after `CLUSTER SLOT-STATS`.
pub fn parse_query(args: &[bytes::Bytes]) -> Result<Query, String> {
    let s = |i: usize| {
        args.get(i)
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
    };
    let num = |i: usize| -> Option<i64> { std::str::from_utf8(args.get(i)?).ok()?.parse().ok() };
    match s(0).as_deref() {
        Some("SLOTSRANGE") => {
            if args.len() != 3 {
                return Err("syntax error".into());
            }
            let (Some(a), Some(b)) = (num(1), num(2)) else {
                return Err("value is not an integer or out of range".into());
            };
            if !(0..16384).contains(&a) || !(0..16384).contains(&b) {
                return Err("Invalid or out of range slot".into());
            }
            if a > b {
                return Err(format!(
                    "Start slot number {a} is greater than end slot number {b}"
                ));
            }
            Ok(Query::Range(a as u16, b as u16))
        }
        Some("ORDERBY") => {
            let name = s(1).ok_or("syntax error")?.to_ascii_lowercase();
            let metric = METRICS
                .iter()
                .position(|(m, _)| *m == name)
                .ok_or_else(|| format!("Unrecognized sort metric for ORDERBY: {name}"))?;
            let (mut limit, mut desc) = (16usize, true);
            let mut i = 2;
            while i < args.len() {
                match s(i).as_deref() {
                    Some("LIMIT") => {
                        let n = num(i + 1).ok_or("value is not an integer or out of range")?;
                        if !(1..=16384).contains(&n) {
                            return Err("Limit has to lie in between 1 and 16384 (maximum number of slots).".into());
                        }
                        limit = n as usize;
                        i += 2;
                    }
                    Some("ASC") => {
                        desc = false;
                        i += 1;
                    }
                    Some("DESC") => {
                        desc = true;
                        i += 1;
                    }
                    _ => return Err("syntax error".into()),
                }
            }
            Ok(Query::OrderBy {
                metric,
                limit,
                desc,
            })
        }
        _ => Err("syntax error".into()),
    }
}

/// The slots to report for `query`, from `owned` (sorted) and each slot's
/// metric values (`values[slot][metric]`).
pub fn select(query: &Query, owned: &[u16], values: &dyn Fn(u16) -> [u64; 5]) -> Vec<u16> {
    match *query {
        Query::Range(a, b) => owned
            .iter()
            .copied()
            .filter(|s| (a..=b).contains(s))
            .collect(),
        Query::OrderBy {
            metric,
            limit,
            desc,
        } => {
            let mut v: Vec<(u64, u16)> = owned.iter().map(|&s| (values(s)[metric], s)).collect();
            v.sort_by(|x, y| {
                let by = if desc { y.0.cmp(&x.0) } else { x.0.cmp(&y.0) };
                by.then(x.1.cmp(&y.1))
            });
            v.into_iter().take(limit).map(|(_, s)| s).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn q(a: &[&str]) -> Result<Query, String> {
        parse_query(
            &a.iter()
                .map(|s| Bytes::from(s.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn test_parse_query() {
        assert_eq!(q(&["SLOTSRANGE", "1", "3"]), Ok(Query::Range(1, 3)));
        assert!(q(&["SLOTSRANGE", "3", "1"]).is_err());
        assert!(q(&["SLOTSRANGE", "0", "16384"]).is_err());
        assert_eq!(
            q(&["orderby", "key-count"]),
            Ok(Query::OrderBy {
                metric: 0,
                limit: 16,
                desc: true
            })
        );
        assert_eq!(
            q(&["ORDERBY", "cpu-usec", "LIMIT", "5", "ASC"]),
            Ok(Query::OrderBy {
                metric: 1,
                limit: 5,
                desc: false
            })
        );
        assert!(q(&["ORDERBY", "key-count", "DESC", "LIMIT", "-1"]).is_err());
        assert!(q(&["ORDERBY", "key-count", "non-existent-arg"]).is_err());
        assert!(q(&["ORDERBY", "nope"]).is_err());
        assert!(q(&["BOGUS"]).is_err());
    }

    #[test]
    fn test_select_orders_by_metric_then_slot() {
        let owned = [1u16, 2, 3, 4];
        let vals = |s: u16| [[0, 5, 5, 1][s as usize - 1], 0, 0, 0, 0];
        let desc = Query::OrderBy {
            metric: 0,
            limit: 3,
            desc: true,
        };
        assert_eq!(select(&desc, &owned, &vals), [2, 3, 4]);
        let asc = Query::OrderBy {
            metric: 0,
            limit: 2,
            desc: false,
        };
        assert_eq!(select(&asc, &owned, &vals), [1, 4]);
        assert_eq!(select(&Query::Range(2, 9), &owned, &vals), [2, 3, 4]);
    }

    #[test]
    fn test_transaction_bytes_go_to_its_slot_at_exec() {
        FLAGS.store(CPU | NET, Ordering::Relaxed);
        reset(None);
        let mut a = ClientAcct::default();
        assert_eq!(a.before(TxRole::Multi, None, 15), None);
        assert_eq!(a.before(TxRole::Queued, Some(7), 33), None);
        assert_eq!(counters(7), [0, 0, 0]);
        assert_eq!(a.before(TxRole::Exec, None, 14), Some(7));
        assert_eq!(counters(7), [0, 0, 0]);
        a.after(Some(7), 5);
        assert_eq!(counters(7), [0, 62, 5]);
        assert_eq!(a.before(TxRole::None, Some(7), 10), Some(7));
        a.after(Some(7), 0);
        record_cpu(Some(7), 2500);
        assert_eq!(counters(7), [2, 72, 5]);
        reset(Some(7));
        assert_eq!(counters(7), [0, 0, 0]);
        FLAGS.store(0, Ordering::Relaxed);
    }

    #[test]
    fn test_resp_commands_splits_frames_and_finds_keys() {
        let b = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nVALUE\r\n*1\r\n$4\r\nPING\r\n";
        let v = resp_commands(b);
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], (33, &b"key"[..]));
        assert_eq!(v[1].0, 14);
        assert!(v[1].1.is_empty());
    }

    #[test]
    fn test_config_values() {
        assert_eq!(parse("cpu net").unwrap(), CPU | NET);
        assert_eq!(parse("yes").unwrap(), CPU | NET | MEM);
        assert_eq!(parse("no").unwrap(), 0);
        assert!(parse("disk").is_err());
    }
}
