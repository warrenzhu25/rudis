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

thread_local! {
    /// This shard's `cpu-usec`, `network-bytes-in` and `network-bytes-out`
    /// per slot, allocated on first use.
    static COUNTERS: std::cell::RefCell<Option<Box<[[u64; 3]; 16384]>>> =
        const { std::cell::RefCell::new(None) };
}

/// Adds a command's CPU time and network bytes to `slot` on this shard.
#[inline]
pub fn record(slot: u16, cpu_usec: u64, bytes_in: u64, bytes_out: u64) {
    COUNTERS.with(|c| {
        let mut c = c.borrow_mut();
        let arr = c.get_or_insert_with(|| {
            vec![[0u64; 3]; 16384]
                .into_boxed_slice()
                .try_into()
                .expect("16384 slots")
        });
        let e = &mut arr[slot as usize & 16383];
        e[0] += cpu_usec;
        e[1] += bytes_in;
        e[2] += bytes_out;
    });
}

/// This shard's counters of `slot`.
pub fn counters(slot: u16) -> [u64; 3] {
    COUNTERS.with(|c| {
        c.borrow()
            .as_ref()
            .map_or([0; 3], |a| a[slot as usize & 16383])
    })
}

/// Zeroes this shard's counters: all (`CONFIG RESETSTAT`), or `slot`'s (it
/// changed owner).
pub fn reset(slot: Option<u16>) {
    COUNTERS.with(|c| {
        if let Some(a) = c.borrow_mut().as_mut() {
            match slot {
                Some(s) => a[s as usize & 16383] = [0; 3],
                None => a.iter_mut().for_each(|e| *e = [0; 3]),
            }
        }
    });
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
    fn test_config_values() {
        assert_eq!(parse("cpu net").unwrap(), CPU | NET);
        assert_eq!(parse("yes").unwrap(), CPU | NET | MEM);
        assert_eq!(parse("no").unwrap(), 0);
        assert!(parse("disk").is_err());
    }
}
