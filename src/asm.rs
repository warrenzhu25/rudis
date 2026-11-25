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

/// `cluster-slot-migration-write-pause-timeout` (ms): longest the source
/// pauses writes to the slots for the handover.
pub static WRITE_PAUSE_TIMEOUT_MS: AtomicU64 = AtomicU64::new(10_000);
/// `cluster-slot-migration-handoff-max-lag-bytes`: accepted (the
/// destination asks for the handover once the snapshot is in).
pub static HANDOFF_MAX_LAG_BYTES: AtomicU64 = AtomicU64::new(1 << 20);

/// `DEBUG ASM-TRIM-METHOD none`: a source keeps its keys of slots it handed
/// over (tests compare both nodes' data then). `default`, `active` and
/// `bg` all trim.
static TRIM_DISABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_trim_method(method: &[u8]) -> Result<(), String> {
    let disabled = if method.eq_ignore_ascii_case(b"none") {
        true
    } else if [b"default".as_slice(), b"active", b"bg"]
        .iter()
        .any(|m| method.eq_ignore_ascii_case(m))
    {
        false
    } else {
        return Err("ERR unknown trim method (use default|active|bg|none)".to_string());
    };
    TRIM_DISABLED.store(disabled, Ordering::Relaxed);
    Ok(())
}

/// Whether keys of slots this node lost are kept (`DEBUG ASM-TRIM-METHOD
/// none`).
pub fn trim_disabled() -> bool {
    TRIM_DISABLED.load(Ordering::Relaxed)
}

/// `DEBUG ASM-FAILPOINT <point> <state>`: the migration fails when it
/// reaches `state` on `point` (`import-main-channel`, `import-rdb-channel`,
/// `migrate-main-channel`, `migrate-rdb-channel`), as in Redis. Rudis runs a
/// migration over one link, so each Redis state maps to the step of it that
/// corresponds (see the `fail_point` calls).
static FAIL_POINT: Mutex<Option<(String, String)>> = Mutex::new(None);

pub fn set_fail_point(point: &[u8], state: &[u8]) {
    let point = String::from_utf8_lossy(point).to_ascii_lowercase();
    let state = String::from_utf8_lossy(state).to_ascii_lowercase();
    *FAIL_POINT.lock() = (!point.is_empty()).then_some((point, state));
}

/// The error a migration fails with if a fail point is set for `side`
/// (`import` or `migrate`) at one of `states` (one per channel: a step of
/// rudis's link that stands for both Redis channels' states).
fn fail_point(side: Operation, states: &[(&str, &str)]) -> Option<String> {
    let fp = FAIL_POINT.lock();
    let (point, state) = fp.as_ref()?;
    let rest = point.strip_prefix(side.name())?.strip_prefix('-')?;
    let channel = rest.strip_suffix("-channel")?;
    states
        .iter()
        .find(|(c, s)| *c == channel && s == state)
        .map(|(c, s)| format!("{c} channel failed in state {s} (fail point)"))
}

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
    let (cancel_tx, cancel_rx) = flume::unbounded();
    hub(port).add(
        Task {
            id: id.clone(),
            slots: ranges.clone(),
            source: source.clone(),
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
    monoio::spawn(run_import(
        router.clone(),
        id.clone(),
        ranges,
        source,
        cancel_rx,
    ));
    id
}

// ---------------------------------------------------------------------
// Write pause (source side, while the slots are handed over)
// ---------------------------------------------------------------------

static PAUSE_ANY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
type PausedSlots = HashMap<u16, Vec<(String, Vec<(u16, u16)>)>>;
static PAUSED: LazyLock<Mutex<PausedSlots>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Pauses writes to `ranges` on the server on `port` for task `task`.
pub fn pause_slots(port: u16, task: &str, ranges: &[(u16, u16)]) {
    let mut paused = PAUSED.lock();
    paused
        .entry(port)
        .or_default()
        .push((task.to_string(), ranges.to_vec()));
    PAUSE_ANY.store(true, Ordering::SeqCst);
}

/// Ends task `task`'s write pause.
pub fn unpause_slots(port: u16, task: &str) {
    let mut paused = PAUSED.lock();
    if let Some(list) = paused.get_mut(&port) {
        list.retain(|(t, _)| t != task);
        if list.is_empty() {
            paused.remove(&port);
        }
    }
    PAUSE_ANY.store(!paused.is_empty(), Ordering::SeqCst);
}

/// Whether some slot migration pauses writes now (cheap: one load).
#[inline]
pub fn any_write_pause() -> bool {
    PAUSE_ANY.load(Ordering::Relaxed)
}

/// Whether `cmd`, a write, must wait: it touches a slot whose migration
/// is being handed over.
pub fn write_paused(port: u16, cmd: &crate::resp::Command) -> bool {
    if !any_write_pause() {
        return false;
    }
    let paused = PAUSED.lock();
    let Some(list) = paused.get(&port) else {
        return false;
    };
    crate::connection::cmd_keys(cmd).iter().any(|k| {
        let slot = crate::router::key_slot(k);
        list.iter()
            .any(|(_, r)| r.iter().any(|&(s, e)| slot >= s && slot <= e))
    })
}

/// Longest a write waits at a slot migration's pause before it goes on
/// (the migration then fails: see `run_migrate_source`).
fn pause_wait_max() -> std::time::Duration {
    std::time::Duration::from_millis(WRITE_PAUSE_TIMEOUT_MS.load(Ordering::Relaxed).max(1))
}

/// Holds `cmd` back while its slot is being handed over; afterwards the
/// caller's cluster check redirects it if the slot moved.
pub async fn wait_write_pause(port: u16, cmd: &crate::resp::Command) {
    let start = std::time::Instant::now();
    let max = pause_wait_max();
    while write_paused(port, cmd) && start.elapsed() < max {
        monoio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

// ---------------------------------------------------------------------
// Stream contents
// ---------------------------------------------------------------------

fn resp_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    crate::connection::write_resp_command(&mut out, args);
    out
}

fn syncslots(args: &[&[u8]]) -> Vec<u8> {
    let mut all: Vec<&[u8]> = vec![b"CLUSTER", b"SYNCSLOTS"];
    all.extend_from_slice(args);
    resp_command(&all)
}

/// On shard `shard_id`'s thread: writes the shard's keys in `ranges` as
/// `RESTORE ... REPLACE [ABSTTL]` commands into migration `task`'s stream
/// and arms the shard, so its later writes to them follow (see
/// `ReplicationHub::arm_migration`). Returns the number of keys.
pub fn dump_slots(
    db: &mut crate::shard::ShardDb,
    port: u16,
    shard_id: usize,
    task: u64,
    ranges: &[(u16, u16)],
) -> usize {
    let keys = db.keys_in_slot_ranges(ranges);
    let mut bytes = Vec::new();
    let mut n = 0;
    for key in keys {
        let Some(payload) = db.dump(&key) else {
            continue;
        };
        let expire_at = db.expiretime(&key, true);
        let ttl = expire_at.max(0).to_string();
        let mut args: Vec<&[u8]> = vec![b"RESTORE", &key, ttl.as_bytes(), &payload, b"REPLACE"];
        if expire_at > 0 {
            args.push(b"ABSTTL");
        }
        crate::connection::write_resp_command(&mut bytes, &args);
        n += 1;
    }
    crate::replication::get_replication_hub(port).arm_migration(task, shard_id, &bytes);
    n
}

fn in_ranges(ranges: &[(u16, u16)], key: &[u8]) -> bool {
    let slot = crate::router::key_slot(key);
    ranges.iter().any(|&(s, e)| slot >= s && slot <= e)
}

/// The part of `data` (whole commands, as the replication stream holds
/// them) the destination gets: commands on keys of `ranges` (with the
/// MULTI/EXEC around them), the migration's own markers and function
/// libraries. Everything else (other slots, SELECT, PING) is left out.
pub fn filter_stream(data: &[u8], ranges: &[(u16, u16)]) -> Vec<u8> {
    use crate::resp::Command;
    let mut out = Vec::with_capacity(data.len());
    let mut buf = bytes::BytesMut::from(data);
    let mut multi: Option<(usize, usize)> = None;
    let mut kept_in_tx = false;
    while !buf.is_empty() {
        let start = data.len() - buf.len();
        let cmd = match crate::resp::parse_command_trusted(&mut buf) {
            Ok(Some(cmd)) => cmd,
            Ok(None) => break,
            Err(e) => {
                crate::log_warning!("Slot migration: unparsable stream bytes dropped: {}", e);
                break;
            }
        };
        let raw = start..data.len() - buf.len();
        let keep = match &cmd {
            Command::Multi => {
                multi = Some((raw.start, raw.end));
                kept_in_tx = false;
                continue;
            }
            Command::Exec => {
                if kept_in_tx {
                    out.extend_from_slice(&data[raw]);
                }
                multi = None;
                kept_in_tx = false;
                continue;
            }
            Command::SyncSlots(_)
            | Command::FunctionRestore { .. }
            | Command::FunctionLoad { .. }
            | Command::FunctionDelete(_)
            | Command::FunctionFlush => true,
            other => {
                let keys = crate::connection::cmd_keys(other);
                !keys.is_empty() && keys.iter().all(|k| in_ranges(ranges, k))
            }
        };
        if keep {
            if let Some((s, e)) = multi.take() {
                out.extend_from_slice(&data[s..e]);
                kept_in_tx = true;
            }
            out.extend_from_slice(&data[raw]);
        }
    }
    out
}

// ---------------------------------------------------------------------
// Source side
// ---------------------------------------------------------------------

/// Ends the source's part on drop: the stream is unregistered and the
/// write pause lifted, whatever way the migration ends.
struct SourceGuard {
    port: u16,
    rep_id: u64,
    task: String,
}

impl Drop for SourceGuard {
    fn drop(&mut self) {
        crate::replication::get_replication_hub(self.port).unregister_migration(self.rep_id);
        unpause_slots(self.port, &self.task);
    }
}

/// Reads whole commands off `reader` into `tx` until the link ends.
async fn read_commands<R: monoio::io::AsyncReadRent>(
    mut reader: R,
    mut buf: bytes::BytesMut,
    tx: flume::Sender<crate::resp::Command>,
) {
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        loop {
            match crate::resp::parse_command_trusted(&mut buf) {
                Ok(Some(cmd)) => {
                    if tx.send(cmd).is_err() {
                        return;
                    }
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
        let (res, c) = reader.read(chunk).await;
        chunk = c;
        match res {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

/// The source side of a migration: a destination sent `CLUSTER SYNCSLOTS
/// START <task> <dest> <ranges>` on this connection. Streams the slots'
/// keys and then the writes to them; on the destination's `PAUSE`, pauses
/// writes to the slots and ends the stream; on its `DONE <epoch>
/// <slots>`, hands the slots over.
pub async fn run_migrate_source(
    stream: monoio::net::TcpStream,
    client_id: u64,
    router: std::rc::Rc<crate::router::Router>,
    args: Vec<bytes::Bytes>,
) {
    use monoio::io::{AsyncWriteRentExt, Splitable};
    let port = router.base_port;
    let (reader, mut writer) = stream.into_split();
    let arg = |i: usize| args.get(i).map(|a| String::from_utf8_lossy(a).into_owned());
    let (Some(task_id), Some(dest), Some(ranges_repr)) = (arg(1), arg(2), arg(3)) else {
        let _ = writer
            .write_all(b"-ERR syntax: CLUSTER SYNCSLOTS START <task> <dest> <ranges>\r\n".to_vec())
            .await;
        return;
    };
    let ranges = crate::cluster::parse_slot_ranges(&ranges_repr);
    let cluster = crate::cluster::get_cluster_hub(port);
    let not_owned = ranges
        .iter()
        .flat_map(|&(s, e)| s..=e)
        .find(|&s| !cluster.owns_slot(s));
    let refuse = if *cluster.role.read() != "master" {
        Some("-ERR not a master\r\n".to_string())
    } else if ranges.is_empty() {
        Some("-ERR no slots to migrate\r\n".to_string())
    } else {
        not_owned.map(|slot| format!("-ERR I don't own slot {slot}\r\n"))
    };
    if let Some(err) = refuse {
        let _ = writer.write_all(err.into_bytes()).await;
        return;
    }
    let tasks = hub(port);
    let (cancel_tx, cancel_rx) = flume::unbounded::<String>();
    let now = unix_ms();
    tasks.add(
        Task {
            id: task_id.clone(),
            slots: ranges.clone(),
            source: cluster.my_id(),
            dest: dest.clone(),
            operation: Operation::Migrate,
            state: State::SendBulkAndStream,
            last_error: String::new(),
            retries: 0,
            create_time: now,
            start_time: now,
            end_time: 0,
            write_pause_ms: 0,
        },
        cancel_tx,
    );
    let repl = crate::replication::get_replication_hub(port);
    let (wake_tx, wake_rx) = flume::unbounded::<Vec<u8>>();
    let rep = repl.register_migration(client_id, wake_tx, router.num_shards);
    let _guard = SourceGuard {
        port,
        rep_id: client_id,
        task: task_id.clone(),
    };
    if writer.write_all(b"+OK\r\n".to_vec()).await.0.is_err() {
        tasks.finish(&task_id, State::Failed, "connection lost");
        return;
    }
    crate::log_notice!(
        "Slot migration {}: sending slots {} to {}",
        task_id,
        ranges_repr,
        dest
    );
    let point = |states: &[(&str, &str)]| fail_point(Operation::Migrate, states);
    if let Some(why) = point(&[("main", "wait-rdbchannel"), ("rdb", "wait-bgsave-start")]) {
        crate::log_warning!("Slot migration {} failed: {}", task_id, why);
        tasks.finish(&task_id, State::Failed, &why);
        return;
    }
    // The writer: the stream, filtered to the slots.
    let (write_err_tx, write_err_rx) = flume::bounded::<String>(1);
    let writer_ranges = ranges.clone();
    let writer_rep = rep.clone();
    monoio::spawn(async move {
        while wake_rx.recv_async().await.is_ok() {
            if writer_rep.is_overflowed() {
                let _ = write_err_tx.send("output buffer limit reached".to_string());
                return;
            }
            let data = writer_rep.take_pending();
            if data.is_empty() {
                continue;
            }
            let out = filter_stream(&data, &writer_ranges);
            writer_rep.finish_write();
            if !out.is_empty() && writer.write_all(out).await.0.is_err() {
                let _ = write_err_tx.send("connection lost".to_string());
                return;
            }
        }
    });
    // The destination's requests.
    let (req_tx, req_rx) = flume::unbounded();
    monoio::spawn(read_commands(reader, bytes::BytesMut::new(), req_tx));

    if let Some(why) = point(&[
        ("main", "send-bulk-and-stream"),
        ("rdb", "send-bulk-and-stream"),
    ]) {
        crate::log_warning!("Slot migration {} failed: {}", task_id, why);
        tasks.finish(&task_id, State::Failed, &why);
        return;
    }
    // The snapshot: each shard dumps its keys of the slots and arms itself.
    let mut keys = 0usize;
    let mut waits = Vec::new();
    for (sid, sender) in router.senders.iter().enumerate() {
        if sid == router.shard_id {
            continue;
        }
        let (tx, rx) = flume::bounded(1);
        let _ = sender.send(crate::shard::ShardMessage::AsmDump {
            task: client_id,
            ranges: ranges.clone(),
            responder: tx,
        });
        waits.push(rx);
    }
    keys += dump_slots(
        &mut router.local_db.borrow_mut(),
        port,
        router.shard_id,
        client_id,
        &ranges,
    );
    for rx in waits {
        keys += rx.recv_async().await.unwrap_or(0);
    }
    // `rdb-key-save-delay`: lets tests act while the slots transfer.
    let delay_us = crate::connection::RDB_KEY_SAVE_DELAY.load(Ordering::Relaxed);
    if delay_us > 0 && keys > 0 {
        let total = std::time::Duration::from_micros(delay_us.saturating_mul(keys as u64));
        if let Ok(reason) = monoio::time::timeout(total, cancel_rx.recv_async()).await {
            let reason = reason.unwrap_or_default();
            tasks.finish(&task_id, State::Canceled, &reason);
            return;
        }
    }
    let mut tail = Vec::new();
    let functions = crate::scripting::dump_functions();
    if !functions.is_empty() {
        tail.extend_from_slice(&resp_command(&[
            b"FUNCTION",
            b"RESTORE",
            &functions,
            b"REPLACE",
        ]));
    }
    tail.extend_from_slice(&syncslots(&[b"SNAPSHOT-EOF"]));
    repl.finish_migration_snapshot(client_id, &tail);
    tasks.set_state(&task_id, State::SendStream);
    if let Some(why) = point(&[("main", "send-stream")]) {
        crate::log_warning!("Slot migration {} failed: {}", task_id, why);
        tasks.finish(&task_id, State::Failed, &why);
        return;
    }
    crate::log_notice!(
        "Slot migration {}: sent {} keys; streaming the writes to the slots",
        task_id,
        keys
    );

    let mut pause_started: Option<std::time::Instant> = None;
    let fail = |state: State, why: &str| {
        crate::log_warning!("Slot migration {} {}: {}", task_id, state.name(), why);
        tasks.finish(&task_id, state, why);
    };
    loop {
        let req = monoio::select! {
            r = req_rx.recv_async() => r.ok(),
            c = cancel_rx.recv_async() => {
                fail(State::Canceled, &c.unwrap_or_default());
                return;
            }
            e = write_err_rx.recv_async() => {
                fail(State::Failed, &e.unwrap_or_else(|_| "connection lost".to_string()));
                return;
            }
        };
        let Some(crate::resp::Command::SyncSlots(req)) = req else {
            fail(State::Failed, "connection closed by the destination");
            return;
        };
        let what = req
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
            .unwrap_or_default();
        match what.as_str() {
            "PAUSE" => {
                if let Some(why) = point(&[("main", "handoff")]) {
                    fail(State::Failed, &why);
                    return;
                }
                pause_slots(port, &task_id, &ranges);
                pause_started = Some(std::time::Instant::now());
                // Writes that passed the pause gate before it closed, and
                // bytes shards still hold, go out before the end mark.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                while crate::connection::writes_running() > 0
                    || !crate::replication::streams_flushed(port, router.num_shards)
                {
                    if std::time::Instant::now() >= deadline {
                        fail(State::Failed, "writes did not settle for the handover");
                        return;
                    }
                    monoio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                repl.append_migration(client_id, &syncslots(&[b"STREAM-EOF"]));
                // Nothing more goes into the stream: once the slots are
                // handed over, this node deletes its keys there, and those
                // deletes must not reach the destination.
                repl.unregister_migration(client_id);
                tasks.set_state(&task_id, State::StreamDone);
            }
            "DONE" => {
                let epoch = req
                    .get(1)
                    .and_then(|e| std::str::from_utf8(e).ok()?.parse::<u64>().ok());
                let slots = req
                    .get(2)
                    .map(|s| crate::cluster::parse_slot_ranges(&String::from_utf8_lossy(s)))
                    .unwrap_or_default();
                cluster.claim_slots(&dest, epoch, slots);
                let _ = cluster.save_config();
                router.sync_slot_tables().await;
                let still_mine = ranges
                    .iter()
                    .flat_map(|&(s, e)| s..=e)
                    .any(|s| cluster.owns_slot(s));
                let paused_ms = pause_started.map_or(0, |t| t.elapsed().as_millis() as u64);
                tasks.update(&task_id, |t| t.write_pause_ms = paused_ms);
                if still_mine {
                    fail(
                        State::Failed,
                        "the destination's claim did not take the slots",
                    );
                    return;
                }
                unpause_slots(port, &task_id);
                rep.send_bytes(&syncslots(&[b"ACK"]));
                tasks.finish(&task_id, State::Completed, "");
                crate::log_notice!(
                    "Slot migration {}: slots {} now served by {} (writes paused {} ms)",
                    task_id,
                    ranges_repr,
                    dest,
                    paused_ms
                );
                // Let the writer send the ACK before the link closes.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
                while rep.has_pending() && std::time::Instant::now() < deadline {
                    monoio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                monoio::time::sleep(std::time::Duration::from_millis(5)).await;
                return;
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------
// Destination side
// ---------------------------------------------------------------------

/// Reads one reply line (`+OK`, `-ERR ...`) off `stream`.
async fn read_line(
    stream: &mut monoio::net::TcpStream,
    buf: &mut bytes::BytesMut,
) -> Result<String, String> {
    use monoio::io::AsyncReadRent;
    loop {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = buf.split_to(pos + 2);
            return Ok(String::from_utf8_lossy(&line[..pos]).into_owned());
        }
        let chunk = vec![0u8; 4096];
        let (res, chunk) = stream.read(chunk).await;
        match res {
            Ok(0) => return Err("connection closed".to_string()),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => return Err(e.to_string()),
        }
    }
}

fn slots_csv(ranges: &[(u16, u16)]) -> String {
    ranges
        .iter()
        .map(|(s, e)| format!("{s}-{e}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Takes the slots over: a new config epoch, then the slots, announced to
/// the cluster. Returns the epoch and all our slots, for the source.
async fn take_over(router: &crate::router::Router, ranges: &[(u16, u16)]) -> (u64, String) {
    let cluster = crate::cluster::get_cluster_hub(router.base_port);
    let epoch = cluster.take_slots(ranges);
    // The shards' slot tables too, as CLUSTER ADDSLOTS does: a slot this
    // node gave away earlier may still say MOVED there.
    for &(s, e) in ranges {
        for slot in s..=e {
            router.set_slot_state(slot, crate::shard::SlotState::Stable);
            router.set_slot_owner(slot, crate::router::slot_to_shard(slot, router.num_shards));
        }
    }
    router.sync_slot_tables().await;
    cluster.broadcast_config();
    (epoch, slots_csv(&cluster.announced_slots()))
}

async fn run_import(
    router: crate::router::Router,
    task_id: String,
    ranges: Vec<(u16, u16)>,
    source: String,
    cancel_rx: flume::Receiver<String>,
) {
    let port = router.base_port;
    let tasks = hub(port);
    let result = import(&router, &task_id, &ranges, &source, &cancel_rx).await;
    match result {
        Ok(()) => {
            tasks.finish(&task_id, State::Completed, "");
            crate::log_notice!(
                "Slot migration {}: imported slots {}",
                task_id,
                ranges_repr(&ranges)
            );
        }
        Err((state, why)) => {
            crate::log_warning!("Slot migration {} {}: {}", task_id, state.name(), why);
            tasks.finish(&task_id, state, &why);
            // What was imported of slots we didn't take over goes.
            trim_slots(port, &ranges);
        }
    }
}

async fn import(
    router: &crate::router::Router,
    task_id: &str,
    ranges: &[(u16, u16)],
    source: &str,
    cancel_rx: &flume::Receiver<String>,
) -> Result<(), (State, String)> {
    use monoio::io::{AsyncWriteRentExt, Splitable};
    let port = router.base_port;
    let tasks = hub(port);
    let failed = |why: String| (State::Failed, why);
    let point = |states: &[(&str, &str)]| match fail_point(Operation::Import, states) {
        Some(why) => Err((State::Failed, why)),
        None => Ok(()),
    };
    tasks.set_state(task_id, State::Connecting);
    point(&[("main", "connecting"), ("rdb", "connecting")])?;
    let addr = {
        let cluster = crate::cluster::get_cluster_hub(port);
        let nodes = cluster.nodes.read();
        let n = nodes
            .get(source)
            .ok_or_else(|| failed(format!("source node {source} is unknown")))?;
        format!("{}:{}", n.ip, n.port)
    };
    let mut stream = match monoio::time::timeout(
        std::time::Duration::from_secs(5),
        monoio::net::TcpStream::connect(addr.as_str()),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(failed(format!("connecting to {addr}: {e}"))),
        Err(_) => return Err(failed(format!("connecting to {addr}: timed out"))),
    };
    let _ = stream.set_nodelay(true);
    let mut buf = bytes::BytesMut::new();
    point(&[("main", "auth-reply"), ("rdb", "auth-reply")])?;
    if let Some(auth) = crate::replication::master_auth_command(port) {
        if stream.write_all(auth).await.0.is_err() {
            return Err(failed("connection lost".to_string()));
        }
        let line = read_line(&mut stream, &mut buf).await.map_err(failed)?;
        if !line.starts_with('+') {
            return Err(failed(format!("source refused AUTH: {line}")));
        }
    }
    point(&[("main", "handshake-reply")])?;
    let me = crate::cluster::get_cluster_hub(port).my_id();
    let ranges_arg = slots_csv(ranges);
    let start = syncslots(&[
        b"START",
        task_id.as_bytes(),
        me.as_bytes(),
        ranges_arg.as_bytes(),
    ]);
    if stream.write_all(start).await.0.is_err() {
        return Err(failed("connection lost".to_string()));
    }
    let line = read_line(&mut stream, &mut buf).await.map_err(failed)?;
    if !line.starts_with('+') {
        return Err(failed(format!("source refused the migration: {line}")));
    }
    point(&[("main", "syncslots-reply"), ("rdb", "rdbchannel-reply")])?;
    tasks.set_state(task_id, State::AccumulateBuffer);
    let (reader, mut writer) = stream.into_split();
    let (cmd_tx, cmd_rx) = flume::unbounded();
    monoio::spawn(read_commands(reader, buf, cmd_tx));
    let mut batch: Vec<Vec<crate::resp::Command>> = vec![Vec::new(); router.num_shards];
    let mut stream_ended = false;
    let mut checked = State::None;
    loop {
        let state = tasks.get(task_id).map_or(State::None, |t| t.state);
        if state != checked {
            checked = state;
            let states: &[(&str, &str)] = match state {
                State::AccumulateBuffer => &[
                    ("main", "accumulate-buffer"),
                    ("rdb", "rdbchannel-transfer"),
                ],
                State::StreamingBuffer => &[("main", "streaming-buffer")],
                State::WaitStreamEof => &[("main", "wait-stream-eof")],
                _ => &[],
            };
            if let Err(e) = point(states) {
                router.flush_replica_batch(&mut batch).await;
                return Err(e);
            }
        }
        let cmd = monoio::select! {
            c = cmd_rx.recv_async() => c.ok(),
            c = cancel_rx.recv_async() => {
                router.flush_replica_batch(&mut batch).await;
                return Err((State::Canceled, c.unwrap_or_default()));
            }
        };
        let Some(cmd) = cmd else {
            router.flush_replica_batch(&mut batch).await;
            return Err(failed("connection to the source lost".to_string()));
        };
        match cmd {
            crate::resp::Command::SyncSlots(args) => {
                router.flush_replica_batch(&mut batch).await;
                let what = args
                    .first()
                    .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
                    .unwrap_or_default();
                match what.as_str() {
                    "SNAPSHOT-EOF" => {
                        tasks.set_state(task_id, State::StreamingBuffer);
                        point(&[("main", "streaming-buffer")])?;
                        if writer.write_all(syncslots(&[b"PAUSE"])).await.0.is_err() {
                            return Err(failed("connection lost".to_string()));
                        }
                        tasks.set_state(task_id, State::WaitStreamEof);
                    }
                    "STREAM-EOF" => {
                        tasks.set_state(task_id, State::Takeover);
                        stream_ended = true;
                        let (epoch, slots) = take_over(router, ranges).await;
                        let epoch = epoch.to_string();
                        let done = syncslots(&[b"DONE", epoch.as_bytes(), slots.as_bytes()]);
                        if writer.write_all(done).await.0.is_err() {
                            // We serve the slots already: the source learns
                            // of it from the cluster bus.
                            return Ok(());
                        }
                    }
                    "ACK" => return Ok(()),
                    _ => {}
                }
            }
            // After the end mark, the source sends nothing but its ACK.
            _ if stream_ended => {}
            cmd => {
                crate::connection::WithCurrentDb {
                    router,
                    db: 0,
                    fut: router.apply_replica_command_batched(cmd, &mut batch),
                }
                .await;
                if cmd_rx.is_empty() {
                    router.flush_replica_batch(&mut batch).await;
                }
            }
        }
    }
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
