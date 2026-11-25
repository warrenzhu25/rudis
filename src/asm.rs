//! Atomic slot migration (Redis 8.4's `CLUSTER MIGRATION`): the node that
//! takes over a set of slots imports them from their owner as one unit (a
//! snapshot of the slots, then the writes to them, then a short write pause
//! on the source while it hands the slots over), instead of the key-by-key
//! `MIGRATE` and `ASK` redirects of a manual migration.
//!
//! This module holds the tasks both nodes keep for a migration (an `import`
//! on the destination, a `migrate` on the source), the commands that start,
//! cancel and list them, and `TRIMSLOTS`. Nodes run the migration over
//! rudis's own links, so it works between rudis nodes.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// How many finished tasks a node keeps for `CLUSTER MIGRATION STATUS`.
const FINISHED_TASKS_KEPT: usize = 64;

/// Which side of a migration a task is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    /// This node takes the slots over (`CLUSTER MIGRATION IMPORT` ran here).
    Import,
    /// This node gives the slots away.
    Migrate,
}

impl Operation {
    pub fn name(self) -> &'static str {
        match self {
            Operation::Import => "import",
            Operation::Migrate => "migrate",
        }
    }
}

/// A task's state, as `CLUSTER MIGRATION STATUS` shows it (Redis's names).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Import: created, not started yet.
    None,
    /// Import: connecting to the source.
    Connecting,
    /// Import: receiving the slots' snapshot (the writes to them that come
    /// meanwhile are applied after it, in order).
    AccumulateBuffer,
    /// Import: snapshot loaded; applying the writes that keep coming.
    StreamingBuffer,
    /// Import: asked the source to pause writes to the slots; applying what
    /// is left until the source's end-of-stream mark.
    WaitStreamEof,
    /// Import: taking the slots over.
    Takeover,
    /// Migrate: sending the snapshot, and the writes to the slots.
    SendBulkAndStream,
    /// Migrate: snapshot sent; sending the writes to the slots.
    SendStream,
    /// Migrate: writes to the slots paused; waiting for the destination to
    /// take them over.
    StreamDone,
    Completed,
    Failed,
    Canceled,
}

impl State {
    pub fn name(self) -> &'static str {
        match self {
            State::None => "none",
            State::Connecting => "connecting",
            State::AccumulateBuffer => "accumulate-buffer",
            State::StreamingBuffer => "streaming-buffer",
            State::WaitStreamEof => "wait-stream-eof",
            State::Takeover => "takeover",
            State::SendBulkAndStream => "send-bulk-and-stream",
            State::SendStream => "send-stream",
            State::StreamDone => "stream-done",
            State::Completed => "completed",
            State::Failed => "failed",
            State::Canceled => "canceled",
        }
    }

    pub fn is_finished(self) -> bool {
        matches!(self, State::Completed | State::Failed | State::Canceled)
    }
}

/// One migration task, as `CLUSTER MIGRATION STATUS` lists it.
#[derive(Clone, Debug)]
pub struct Task {
    pub id: String,
    pub slots: Vec<(u16, u16)>,
    pub source: String,
    pub dest: String,
    pub operation: Operation,
    pub state: State,
    pub last_error: String,
    pub retries: u64,
    /// Unix ms.
    pub create_time: u64,
    pub start_time: u64,
    pub end_time: u64,
    /// How long the source paused writes to the slots for the handover.
    pub write_pause_ms: u64,
}

impl Task {
    fn overlaps(&self, slots: &[(u16, u16)]) -> bool {
        self.slots
            .iter()
            .any(|&(s, e)| slots.iter().any(|&(s2, e2)| s <= e2 && s2 <= e))
    }
}

/// The tasks of the server on one port: running ones and the most recent
/// finished ones, newest first.
#[derive(Default)]
pub struct AsmHub {
    tasks: Mutex<VecDeque<Task>>,
    /// Running tasks' cancel signals, by task id.
    cancels: Mutex<HashMap<String, flume::Sender<String>>>,
}

static HUBS: LazyLock<Mutex<HashMap<u16, Arc<AsmHub>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The migration tasks of the server on `port`.
pub fn hub(port: u16) -> Arc<AsmHub> {
    HUBS.lock().entry(port).or_default().clone()
}

static TASK_SEQ: AtomicU64 = AtomicU64::new(0);

/// A new task id: 40 hex characters, like a node id.
pub fn new_task_id() -> String {
    let seq = TASK_SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let a = crate::table::hash_key(&nanos.to_le_bytes()) ^ seq.rotate_left(17);
    let b = crate::table::hash_key(&a.to_le_bytes()) ^ std::process::id() as u64;
    format!("{a:016x}{b:016x}{:08x}", (seq as u32) ^ (nanos as u32))
}

pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl AsmHub {
    /// Adds a running task and its cancel signal.
    pub fn add(&self, task: Task, cancel: flume::Sender<String>) {
        self.cancels.lock().insert(task.id.clone(), cancel);
        let mut tasks = self.tasks.lock();
        tasks.push_front(task);
        let finished = tasks.iter().filter(|t| t.state.is_finished()).count();
        if finished > FINISHED_TASKS_KEPT
            && let Some(pos) = tasks.iter().rposition(|t| t.state.is_finished())
        {
            tasks.remove(pos);
        }
    }

    /// Runs `f` on task `id`, if it is still listed.
    pub fn update(&self, id: &str, f: impl FnOnce(&mut Task)) {
        if let Some(t) = self.tasks.lock().iter_mut().find(|t| t.id == id) {
            f(t);
        }
    }

    pub fn set_state(&self, id: &str, state: State) {
        self.update(id, |t| t.state = state);
    }

    /// Ends task `id` in `state` (completed, failed or canceled).
    pub fn finish(&self, id: &str, state: State, error: &str) {
        self.cancels.lock().remove(id);
        self.update(id, |t| {
            if !t.state.is_finished() {
                t.state = state;
                t.end_time = unix_ms();
                if !error.is_empty() {
                    t.last_error = error.to_string();
                }
            }
        });
    }

    pub fn get(&self, id: &str) -> Option<Task> {
        self.tasks.lock().iter().find(|t| t.id == id).cloned()
    }

    pub fn all(&self) -> Vec<Task> {
        self.tasks.lock().iter().cloned().collect()
    }

    /// Running tasks.
    pub fn active(&self) -> Vec<Task> {
        self.tasks
            .lock()
            .iter()
            .filter(|t| !t.state.is_finished())
            .cloned()
            .collect()
    }

    pub fn active_count(&self) -> usize {
        self.tasks
            .lock()
            .iter()
            .filter(|t| !t.state.is_finished())
            .count()
    }

    /// Whether a running task covers a slot of `slots`.
    pub fn overlapping_active(&self, slots: &[(u16, u16)]) -> bool {
        self.tasks
            .lock()
            .iter()
            .any(|t| !t.state.is_finished() && t.overlaps(slots))
    }

    /// Asks task `id` to stop with `reason`; whether it was running.
    pub fn cancel(&self, id: &str, reason: &str) -> bool {
        let tx = self.cancels.lock().get(id).cloned();
        match tx {
            Some(tx) => {
                let _ = tx.send(reason.to_string());
                // The task records the end itself; mark it now too, so a
                // STATUS right after the reply shows it.
                self.finish(id, State::Canceled, reason);
                true
            }
            None => false,
        }
    }

    /// Cancels every running task; how many there were.
    pub fn cancel_all(&self, reason: &str) -> usize {
        let ids: Vec<String> = self.cancels.lock().keys().cloned().collect();
        ids.iter().filter(|id| self.cancel(id, reason)).count()
    }

    /// Cancels the running tasks that cover a slot of `slots`.
    pub fn cancel_overlapping(&self, slots: &[(u16, u16)], reason: &str) -> usize {
        let ids: Vec<String> = self
            .active()
            .into_iter()
            .filter(|t| t.overlaps(slots))
            .map(|t| t.id)
            .collect();
        ids.iter().filter(|id| self.cancel(id, reason)).count()
    }
}

/// Checks that this node (on `port`) may import `ranges` and returns the
/// id of the node they come from, with Redis's errors, in Redis's order.
pub fn validate_import(port: u16, ranges: &[(u16, u16)]) -> Result<String, String> {
    let cluster = crate::cluster::get_cluster_hub(port);
    if *cluster.role.read() != "master" {
        return Err("ERR slot migration is not allowed on replica".to_string());
    }
    if !cluster.slot_states.read().is_empty() {
        return Err(
            "ERR all slots must be STABLE to start an atomic slot migration (a manual slot migration is in progress)"
                .to_string(),
        );
    }
    let mut source: Option<String> = None;
    for &(s, e) in ranges {
        for slot in s..=e {
            if cluster.owns_slot(slot) {
                return Err(format!(
                    "ERR this node is already the owner of the slot: {slot}"
                ));
            }
            let Some(owner) = cluster.slot_owner_id(slot) else {
                return Err(format!("ERR slot has no owner: {slot}"));
            };
            match &source {
                Some(src) if *src != owner => {
                    return Err("ERR slots belong to different source nodes".to_string());
                }
                Some(_) => {}
                None => source = Some(owner),
            }
        }
    }
    if hub(port).overlapping_active(ranges) {
        return Err("ERR an overlapping import exists for the slots".to_string());
    }
    source.ok_or_else(|| "ERR no slots given".to_string())
}

/// `TRIMSLOTS`: deletes this node's keys in the slots of `ranges` it doesn't
/// serve (propagated to replicas and the AOF like any delete), as each shard
/// does with slots lost to a newer claim. Slots it serves are left alone.
pub fn trim_slots(port: u16, ranges: &[(u16, u16)]) {
    let cluster = crate::cluster::get_cluster_hub(port);
    let slots: Vec<u16> = ranges
        .iter()
        .flat_map(|&(s, e)| s..=e)
        .filter(|&s| !cluster.owns_slot(s))
        .collect();
    if !slots.is_empty() {
        cluster.queue_slot_purge(&slots);
    }
}

/// Runs `CLUSTER MIGRATION ...`, writing the reply to `out`.
pub fn run_migration_command(router: &crate::router::Router, cmd: MigrationCmd, out: &mut Vec<u8>) {
    use crate::connection::{write_resp_bulk, write_resp_integer};
    let port = router.base_port;
    let tasks = hub(port);
    let write_tasks = |out: &mut Vec<u8>, list: Vec<Task>| {
        out.extend_from_slice(format!("*{}\r\n", list.len()).as_bytes());
        for t in &list {
            write_task(out, t);
        }
    };
    match cmd {
        MigrationCmd::Import(ranges) => match validate_import(port, &ranges) {
            Ok(source) => {
                let id = start_import(router, ranges, source);
                write_resp_bulk(out, id.as_bytes());
            }
            Err(e) => {
                out.push(b'-');
                out.extend_from_slice(e.replace(['\r', '\n'], " ").as_bytes());
                out.extend_from_slice(b"\r\n");
            }
        },
        MigrationCmd::CancelId(id) => {
            write_resp_integer(out, i64::from(tasks.cancel(&id, "canceled by user")));
        }
        MigrationCmd::CancelAll => {
            write_resp_integer(out, tasks.cancel_all("canceled by user") as i64);
        }
        MigrationCmd::StatusId(id) => write_tasks(out, tasks.get(&id).into_iter().collect()),
        MigrationCmd::StatusAll => write_tasks(out, tasks.all()),
    }
}

/// Creates the import task for `ranges` (from node `source`) and starts it;
/// returns its id.
fn start_import(router: &crate::router::Router, ranges: Vec<(u16, u16)>, source: String) -> String {
    let port = router.base_port;
    let id = new_task_id();
    let now = unix_ms();
    let (cancel_tx, _cancel_rx) = flume::unbounded();
    hub(port).add(
        Task {
            id: id.clone(),
            slots: ranges,
            source,
            dest: crate::cluster::get_cluster_hub(port).my_id(),
            operation: Operation::Import,
            state: State::None,
            last_error: String::new(),
            retries: 0,
            create_time: now,
            start_time: now,
            end_time: 0,
            write_pause_ms: 0,
        },
        cancel_tx,
    );
    hub(port).finish(
        &id,
        State::Failed,
        "atomic slot migration is not implemented yet",
    );
    id
}

/// Parses an integer slot number: `out of range slot` otherwise.
fn parse_slot(arg: &[u8]) -> Result<u16, String> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| (0..16384).contains(&n))
        .map(|n| n as u16)
        .ok_or_else(|| "ERR Invalid or out of range slot".to_string())
}

/// Parses `<start> <end> [<start> <end> ...]` into ranges, sorted, with
/// adjacent ones merged. A slot given twice is an error.
pub fn parse_ranges(args: &[bytes::Bytes]) -> Result<Vec<(u16, u16)>, String> {
    let mut ranges = Vec::with_capacity(args.len() / 2);
    for pair in args.chunks(2) {
        let start = parse_slot(&pair[0])?;
        let end = parse_slot(&pair[1])?;
        if start > end {
            return Err(format!(
                "ERR start slot number {start} is greater than end slot number {end}"
            ));
        }
        ranges.push((start, end));
    }
    ranges.sort_unstable();
    let mut merged: Vec<(u16, u16)> = Vec::with_capacity(ranges.len());
    for (s, e) in ranges {
        match merged.last_mut() {
            Some(last) if s <= last.1 => {
                return Err(format!("ERR Slot {s} specified multiple times"));
            }
            Some(last) if s == last.1 + 1 => last.1 = e,
            _ => merged.push((s, e)),
        }
    }
    Ok(merged)
}

/// Ranges as STATUS shows them: `0-100 200-300`.
pub fn ranges_repr(ranges: &[(u16, u16)]) -> String {
    ranges
        .iter()
        .map(|(s, e)| format!("{s}-{e}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// `CLUSTER MIGRATION <subcommand> ...` as given.
#[derive(Clone, Debug, PartialEq)]
pub enum MigrationCmd {
    Import(Vec<(u16, u16)>),
    CancelId(String),
    CancelAll,
    StatusId(String),
    StatusAll,
}

const WRONG_ARGS: &str = "ERR wrong number of arguments for 'cluster|migration' command";

/// Parses the arguments after `CLUSTER MIGRATION`.
pub fn parse_migration(args: &[bytes::Bytes]) -> Result<MigrationCmd, String> {
    let Some(sub) = args.first() else {
        return Err(WRONG_ARGS.to_string());
    };
    let rest = &args[1..];
    let upper = String::from_utf8_lossy(sub).to_ascii_uppercase();
    match upper.as_str() {
        "IMPORT" => {
            if rest.is_empty() || !rest.len().is_multiple_of(2) {
                return Err(WRONG_ARGS.to_string());
            }
            Ok(MigrationCmd::Import(parse_ranges(rest)?))
        }
        "CANCEL" | "STATUS" => {
            let cancel = upper == "CANCEL";
            let Some(kind) = rest.first() else {
                return Err(WRONG_ARGS.to_string());
            };
            if kind.eq_ignore_ascii_case(b"ID") {
                if rest.len() != 2 {
                    return Err(WRONG_ARGS.to_string());
                }
                let id = String::from_utf8_lossy(&rest[1]).into_owned();
                Ok(if cancel {
                    MigrationCmd::CancelId(id)
                } else {
                    MigrationCmd::StatusId(id)
                })
            } else if kind.eq_ignore_ascii_case(b"ALL") {
                if rest.len() != 1 {
                    return Err(WRONG_ARGS.to_string());
                }
                Ok(if cancel {
                    MigrationCmd::CancelAll
                } else {
                    MigrationCmd::StatusAll
                })
            } else {
                Err(format!(
                    "ERR unknown argument '{}'",
                    String::from_utf8_lossy(kind)
                ))
            }
        }
        _ => {
            if args.len() < 3 {
                return Err(WRONG_ARGS.to_string());
            }
            Err(format!(
                "ERR unknown argument '{}'",
                String::from_utf8_lossy(sub)
            ))
        }
    }
}

/// Parses `TRIMSLOTS RANGES <n> <start> <end> ...` (the arguments after
/// `TRIMSLOTS`).
pub fn parse_trimslots(args: &[bytes::Bytes]) -> Result<Vec<(u16, u16)>, String> {
    const WRONG: &str = "ERR wrong number of arguments for 'trimslots' command";
    if args.len() < 4 {
        return Err(WRONG.to_string());
    }
    if !args[0].eq_ignore_ascii_case(b"RANGES") {
        return Err("ERR missing ranges argument".to_string());
    }
    let n = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n > 0 && n <= 16384)
        .ok_or_else(|| "ERR invalid number of ranges".to_string())?;
    if args.len() != 2 + 2 * n as usize {
        return Err("ERR invalid number of ranges".to_string());
    }
    let mut ranges = Vec::with_capacity(n as usize);
    for pair in args[2..].chunks(2) {
        let start = parse_slot(&pair[0])?;
        let end = parse_slot(&pair[1])?;
        if start > end {
            return Err(format!(
                "ERR start slot number {start} is greater than end slot number {end}"
            ));
        }
        ranges.push((start, end));
    }
    let mut ranges: Vec<(u16, u16)> = ranges;
    crate::cluster::compact_slots(&mut ranges);
    Ok(ranges)
}

/// Writes task `t` as `CLUSTER MIGRATION STATUS` shows it: a map with
/// Redis's fields, in Redis's order.
pub fn write_task(out: &mut Vec<u8>, t: &Task) {
    use crate::connection::{write_resp_bulk, write_resp_integer, write_resp_map_len};
    write_resp_map_len(out, 12);
    let field = |out: &mut Vec<u8>, k: &str, v: &str| {
        write_resp_bulk(out, k.as_bytes());
        write_resp_bulk(out, v.as_bytes());
    };
    field(out, "id", &t.id);
    field(out, "slots", &ranges_repr(&t.slots));
    field(out, "source", &t.source);
    field(out, "dest", &t.dest);
    field(out, "operation", t.operation.name());
    field(out, "state", t.state.name());
    field(out, "last_error", &t.last_error);
    for (k, v) in [
        ("retries", t.retries),
        ("create_time", t.create_time),
        ("start_time", t.start_time),
        ("end_time", t.end_time),
        ("write_pause_ms", t.write_pause_ms),
    ] {
        write_resp_bulk(out, k.as_bytes());
        write_resp_integer(out, v as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn args(s: &str) -> Vec<Bytes> {
        s.split_whitespace()
            .map(|a| Bytes::copy_from_slice(a.as_bytes()))
            .collect()
    }

    #[test]
    fn test_parse_ranges_sorts_merges_and_rejects_repeats() {
        assert_eq!(
            parse_ranges(&args("7050 7051 7010 7049 7000 7005")),
            Ok(vec![(7000, 7005), (7010, 7051)])
        );
        assert_eq!(
            parse_ranges(&args("7007 7007 7008 7009 7006 7006")),
            Ok(vec![(7006, 7009)])
        );
        assert!(
            parse_ranges(&args("7000 8000 8000 9000"))
                .unwrap_err()
                .contains("specified multiple times")
        );
        assert!(
            parse_ranges(&args("7000 8000 7900 9000"))
                .unwrap_err()
                .contains("specified multiple times")
        );
        assert!(
            parse_ranges(&args("100 200 201 150"))
                .unwrap_err()
                .contains("greater than end slot number")
        );
        for bad in ["0 16384", "-1 2", "10 a", "sd sd", "14000 18000"] {
            assert!(
                parse_ranges(&args(bad))
                    .unwrap_err()
                    .contains("out of range slot"),
                "{bad}"
            );
        }
    }

    #[test]
    fn test_parse_migration_arguments_like_redis() {
        let err = |s: &str| parse_migration(&args(s)).unwrap_err();
        for s in [
            "",
            "IMPORT",
            "IMPORT 100",
            "IMPORT 100 200 300",
            "CANCEL",
            "CANCEL ID",
            "CANCEL ID 1 X",
            "CANCEL ALL X",
            "STATUS",
            "STATUS ID",
            "STATUS ID id X",
            "STATUS ALL X",
        ] {
            assert!(err(s).contains("wrong number of arguments"), "{s}");
        }
        for s in [
            "UNKNOWN 1 2",
            "CANCEL UNKNOWNARG",
            "CANCEL abc def",
            "STATUS ABC DEF",
            "STATUS UNKNOWNARG",
        ] {
            assert!(err(s).contains("unknown argument"), "{s}");
        }
        assert_eq!(
            parse_migration(&args("status all")),
            Ok(MigrationCmd::StatusAll)
        );
    }

    #[test]
    fn test_parse_trimslots_like_redis() {
        let err = |s: &str| parse_trimslots(&args(s)).unwrap_err();
        for s in [
            "",
            "RANGES",
            "RANGES 1",
            "RANGES 2 100",
            "RANGES 17000 1",
            "RANGES abc",
        ] {
            assert!(err(s).contains("wrong number of arguments"), "{s}");
        }
        assert!(err("UNKNOWN 1 100 200").contains("missing ranges argument"));
        for s in [
            "RANGES 0 1 1",
            "RANGES -1 2 2",
            "RANGES 17000 1 2",
            "RANGES 2 100 200 300",
        ] {
            assert!(err(s).contains("invalid number of ranges"), "{s}");
        }
        for s in [
            "RANGES 1 -1 0",
            "RANGES 1 -2 -1",
            "RANGES 1 0 16384",
            "RANGES 1 abc def",
            "RANGES 1 100 abc",
        ] {
            assert!(err(s).contains("out of range slot"), "{s}");
        }
        assert!(err("RANGES 1 200 100").contains("greater than end slot number"));
        assert_eq!(parse_trimslots(&args("RANGES 2 5 9 0 4")), Ok(vec![(0, 9)]));
    }
}
