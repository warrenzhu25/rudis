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

/// `DEBUG ASM-FAILPOINT <point> <state>`: the migration fails when it
/// reaches `state` on `point` (`import-main-channel`, `import-rdb-channel`,
/// `migrate-main-channel`, `migrate-rdb-channel`), as in Redis. Rudis runs a
/// migration over one link, so each Redis state maps to the step of it that
/// corresponds (see the `fail_point` calls).
static REPL_PAUSE: Mutex<String> = Mutex::new(String::new());

/// `DEBUG REPL-PAUSE <point>|clear`.
pub fn set_repl_pause(point: &[u8]) {
    *REPL_PAUSE.lock() = String::from_utf8_lossy(point).to_ascii_lowercase();
}

/// Stops the whole process (until SIGCONT) when `DEBUG REPL-PAUSE` named
/// `point`, as Redis does for its tests.
fn repl_pause_point(point: &str) {
    if *REPL_PAUSE.lock() == point {
        crate::log_notice!("Pausing the process at {} (DEBUG REPL-PAUSE)", point);
        // SAFETY: raise(3) with a valid signal.
        unsafe {
            libc::raise(libc::SIGSTOP);
        }
    }
}

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
    /// Import: failed, and to be tried again (it is still running).
    pub retry_pending: bool,
}

impl Task {
    /// Running, or failed and waiting to be tried again.
    pub fn is_active(&self) -> bool {
        !self.state.is_finished() || self.retry_pending
    }

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
        // A destination's retry of a task replaces the earlier attempt.
        tasks.retain(|t| t.id != task.id);
        tasks.push_front(task);
        let finished = tasks.iter().filter(|t| !t.is_active()).count();
        if finished > FINISHED_TASKS_KEPT
            && let Some(pos) = tasks.iter().rposition(|t| !t.is_active())
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
            if t.is_active() {
                t.retry_pending = false;
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
            .filter(|t| t.is_active())
            .cloned()
            .collect()
    }

    pub fn active_count(&self) -> usize {
        self.tasks.lock().iter().filter(|t| t.is_active()).count()
    }

    /// Whether a running task covers a slot of `slots`.
    pub fn overlapping_active(&self, slots: &[(u16, u16)]) -> bool {
        self.tasks
            .lock()
            .iter()
            .any(|t| t.is_active() && t.overlaps(slots))
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

/// How often running tasks check the cluster and their timeouts.
const TICK: std::time::Duration = std::time::Duration::from_millis(100);

/// `cluster-slot-migration-sync-buffer-drain-timeout` (ms): longest a
/// source's stream may sit unsent before the migration fails.
pub static SYNC_BUFFER_DRAIN_TIMEOUT_MS: AtomicU64 = AtomicU64::new(60_000);

/// Why running task `id` (of side `op`) must stop because this node or
/// the cluster changed under it, if it must.
fn config_changed(port: u16, tasks: &AsmHub, id: &str, op: Operation) -> Option<String> {
    let task = tasks.get(id)?;
    let cluster = crate::cluster::get_cluster_hub(port);
    if *cluster.role.read() != "master" || cluster.mf_paused.load(Ordering::Relaxed) {
        return Some("canceled due to failover".to_string());
    }
    let peer = match op {
        Operation::Import => &task.source,
        Operation::Migrate => &task.dest,
    };
    if !cluster.nodes.read().contains_key(peer) {
        return Some(format!("canceled: node deleted ({peer})"));
    }
    let me = cluster.my_id();
    // A slot another node took (an import doesn't stop for a slot its
    // source dropped: the source stops, and the import fails with it).
    let moved = task.slots.iter().flat_map(|&(s, e)| s..=e).any(|slot| {
        let owner = cluster.slot_owner_id(slot);
        if owner.is_none() && op == Operation::Import {
            return false;
        }
        owner.as_deref() != Some(me.as_str()) && owner.as_deref() != Some(peer.as_str())
    });
    if moved {
        // The source failed over to one of its replicas: an import tries
        // again from the new master (see `run_import`), so it fails.
        if op == Operation::Import {
            let mut candidates: Vec<String> =
                SOURCE_REPLICAS.lock().get(id).cloned().unwrap_or_default();
            if let Some(n) = cluster.nodes.read().get(peer).filter(|n| !n.is_master()) {
                candidates.push(n.master_id.clone());
            }
            let all_owned_by = |m: &str| {
                task.slots
                    .iter()
                    .flat_map(|&(s, e)| s..=e)
                    .all(|slot| cluster.slot_owner_id(slot).as_deref() == Some(m))
            };
            if let Some(m) = candidates.iter().find(|m| all_owned_by(m)) {
                return Some(format!("{FAILED_PREFIX}the source failed over to {m}"));
            }
        }
        return Some("canceled: slots configuration updated".to_string());
    }
    if op == Operation::Import
        && task.state != State::None
        && crate::connection::is_client_paused().is_some()
    {
        return Some("canceled due to client pause".to_string());
    }
    None
}

/// The replicas each import's source had when the attempt started: one of
/// them taking the slots over is a failover, not a reshard.
static SOURCE_REPLICAS: LazyLock<Mutex<HashMap<String, Vec<String>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A [`config_changed`] reason starting with this fails the task (an
/// import is then tried again) rather than canceling it.
const FAILED_PREFIX: &str = "failed: ";

fn change_outcome(why: String) -> (State, String) {
    if why.starts_with(FAILED_PREFIX) {
        (State::Failed, why)
    } else {
        (State::Canceled, why)
    }
}

/// FLUSHALL / FLUSHDB: the running tasks stop.
pub fn cancel_on_flush(port: u16) {
    hub(port).cancel_all("canceled due to flush");
}

/// Shutdown: the running tasks stop.
pub fn cancel_on_shutdown(port: u16) {
    let n = hub(port).cancel_all("Cancelled due to server shutdown");
    if n > 0 {
        crate::log_notice!(
            "Slot migration: {} task(s) Cancelled due to server shutdown",
            n
        );
    }
}

/// Whether a running task moves `slot` (CLUSTER SETSLOT is refused then).
pub fn slot_in_active_task(port: u16, slot: u16) -> bool {
    hub(port).overlapping_active(&[(slot, slot)])
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
            retry_pending: false,
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
// Trimming: deleting this node's keys in slots it doesn't serve
// ---------------------------------------------------------------------

/// How keys of slots a node gave away (or failed to import) are deleted
/// (`DEBUG ASM-TRIM-METHOD <method> [delay]`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrimMethod {
    /// `bg`, or `active` while clients track keys (they get invalidations).
    Default,
    /// At once, as a background free (`lazyfreed_objects`).
    Bg,
    /// A key at a time, between other work, with the job's progress in
    /// CLUSTER INFO.
    Active,
    /// Kept (tests compare both nodes' data then).
    None,
}

/// The method, and for `active` the microseconds it waits after each key
/// (negative: it doesn't run, the job just waits).
static TRIM_METHOD: Mutex<(TrimMethod, i64)> = Mutex::new((TrimMethod::Default, 0));

pub fn set_trim_method(method: &[u8], delay: Option<&[u8]>) -> Result<(), String> {
    let m = match method.to_ascii_lowercase().as_slice() {
        b"default" => TrimMethod::Default,
        b"bg" => TrimMethod::Bg,
        b"active" => TrimMethod::Active,
        b"none" => TrimMethod::None,
        _ => return Err("ERR unknown trim method (use default|active|bg|none)".to_string()),
    };
    let delay = match delay {
        Some(d) => std::str::from_utf8(d)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| "ERR invalid active trim delay".to_string())?,
        None => 0,
    };
    *TRIM_METHOD.lock() = (m, delay);
    Ok(())
}

/// Whether keys of slots this node lost are kept (`DEBUG ASM-TRIM-METHOD
/// none`).
pub fn trim_disabled() -> bool {
    TRIM_METHOD.lock().0 == TrimMethod::None
}

fn active_trim_delay() -> i64 {
    TRIM_METHOD.lock().1
}

/// Whether a new trim job deletes all at once (else actively).
fn trim_in_bg() -> bool {
    match TRIM_METHOD.lock().0 {
        TrimMethod::Bg => true,
        TrimMethod::Active => false,
        _ => !crate::connection::HAS_TRACKING_CLIENTS.load(Ordering::Relaxed),
    }
}

/// One trim: the slots, and its progress.
pub struct TrimJob {
    slots: Vec<u16>,
    bg: bool,
    /// Logged to the AOF and sent to replicas (`TRIMSLOTS`), by each shard
    /// for its slots, before it deletes anything.
    log: bool,
    keys: AtomicU64,
    trimmed: AtomicU64,
    shards_left: std::sync::atomic::AtomicUsize,
    cancelled: std::sync::atomic::AtomicBool,
    /// Past its wait for paused clients: a job held back by a pause
    /// before it began is pending, not running (CLUSTER INFO).
    started: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct TrimHub {
    jobs: Mutex<Vec<Arc<TrimJob>>>,
    /// The most recent job (CLUSTER INFO's `current_job` numbers).
    current: Mutex<Option<Arc<TrimJob>>>,
    started: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
}

static TRIM_HUBS: LazyLock<Mutex<HashMap<u16, Arc<TrimHub>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn trim_hub(port: u16) -> Arc<TrimHub> {
    TRIM_HUBS.lock().entry(port).or_default().clone()
}

static TRIMMING_ANY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// The union of all ports' slots being trimmed, one bit per slot.
static TRIMMING_BITS: [AtomicU64; 256] = [const { AtomicU64::new(0) }; 256];

fn rebuild_trimming() {
    let mut bits = [0u64; 256];
    let mut any = false;
    for hub in TRIM_HUBS.lock().values() {
        for job in hub.jobs.lock().iter() {
            any = true;
            for &slot in &job.slots {
                bits[slot as usize / 64] |= 1 << (slot % 64);
            }
        }
    }
    for (cell, b) in TRIMMING_BITS.iter().zip(bits) {
        cell.store(b, Ordering::SeqCst);
    }
    TRIMMING_ANY.store(any, Ordering::SeqCst);
}

/// Whether some slots are being trimmed now (cheap: one load).
#[inline(always)]
pub fn trimming_any() -> bool {
    TRIMMING_ANY.load(Ordering::Relaxed)
}

/// Whether `slot` is being trimmed (by any server of this process).
#[inline]
pub fn is_trimming_slot(slot: u16) -> bool {
    trimming_any()
        && TRIMMING_BITS[slot as usize / 64].load(Ordering::Relaxed) & (1 << (slot % 64)) != 0
}

/// Whether a trim job on `port` covers a slot of `ranges`.
fn trim_overlaps(port: u16, ranges: &[(u16, u16)]) -> bool {
    trim_hub(port).jobs.lock().iter().any(|j| {
        j.slots
            .iter()
            .any(|&s| ranges.iter().any(|&(a, b)| s >= a && s <= b))
    })
}

/// Whether a trim job on `port` covers `slot` (CLUSTER SETSLOT is refused
/// then).
pub fn slot_has_trim_job(port: u16, slot: u16) -> bool {
    trim_overlaps(port, &[(slot, slot)])
}

/// Waits (briefly) for a trim job of `slot` to finish when nothing holds
/// it back anymore (clients resumed).
pub async fn wait_unpaused_trim(port: u16, slot: u16) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while slot_has_trim_job(port, slot)
        && crate::connection::is_client_paused().is_none()
        && std::time::Instant::now() < deadline
    {
        monoio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// The `TRIMSLOTS RANGES ...` command for `slots`.
fn trimslots_command(slots: &[u16]) -> Vec<u8> {
    let ranges = slots_to_ranges(slots);
    let n = ranges.len().to_string();
    let mut parts: Vec<String> = Vec::with_capacity(ranges.len() * 2);
    for (s, e) in &ranges {
        parts.push(s.to_string());
        parts.push(e.to_string());
    }
    let mut args: Vec<&[u8]> = vec![b"TRIMSLOTS", b"RANGES", n.as_bytes()];
    args.extend(parts.iter().map(|p| p.as_bytes()));
    resp_command(&args)
}

fn slots_to_ranges(slots: &[u16]) -> Vec<(u16, u16)> {
    let mut sorted = slots.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut out: Vec<(u16, u16)> = Vec::new();
    for s in sorted {
        match out.last_mut() {
            Some((_, e)) if *e + 1 == s => *e = s,
            _ => out.push((s, s)),
        }
    }
    out
}

/// Starts a job deleting the server's keys in `slots` on every shard (none
/// is logged per key: with `log`, each shard logs `TRIMSLOTS` for its
/// slots to its AOF and our replicas first, and they trim the same way).
pub fn start_trim(router: &crate::router::Router, slots: Vec<u16>, log: bool) {
    if slots.is_empty() || trim_disabled() {
        return;
    }
    let port = router.base_port;
    let job = Arc::new(TrimJob {
        bg: trim_in_bg(),
        slots,
        log,
        keys: AtomicU64::new(0),
        trimmed: AtomicU64::new(0),
        shards_left: std::sync::atomic::AtomicUsize::new(router.num_shards),
        cancelled: std::sync::atomic::AtomicBool::new(false),
        started: std::sync::atomic::AtomicBool::new(
            crate::connection::is_client_paused().is_none(),
        ),
    });
    let hub = trim_hub(port);
    hub.jobs.lock().push(job.clone());
    *hub.current.lock() = Some(job.clone());
    if !job.bg {
        hub.started.fetch_add(1, Ordering::Relaxed);
    }
    rebuild_trimming();
    for (sid, sender) in router.senders.iter().enumerate() {
        if sid != router.shard_id {
            let _ = sender.send(crate::shard::ShardMessage::AsmTrim(job.clone()));
        }
    }
    monoio::spawn(run_trim_shard(
        router.local_db.clone(),
        router.aof_slot.clone(),
        port,
        router.shard_id,
        router.num_shards,
        job,
    ));
}

/// `TRIMSLOTS` (from a client, our master or the AOF): trims the slots of
/// `ranges` this node doesn't serve.
pub fn trim_slots(router: &crate::router::Router, ranges: &[(u16, u16)]) {
    let cluster = crate::cluster::get_cluster_hub(router.base_port);
    let slots: Vec<u16> = ranges
        .iter()
        .flat_map(|&(s, e)| s..=e)
        .filter(|&s| !cluster.owns_slot(s))
        .collect();
    start_trim(router, slots, true);
}

/// Stops every trim job of the server on `port` (its data is replaced).
pub fn cancel_trims(port: u16) {
    for job in trim_hub(port).jobs.lock().iter() {
        job.cancelled.store(true, Ordering::Relaxed);
    }
}

/// One shard's part of `job`, on that shard's thread.
pub async fn run_trim_shard(
    db: std::rc::Rc<std::cell::RefCell<crate::shard::ShardDb>>,
    aof: crate::aof::AofSlot,
    port: u16,
    shard_id: usize,
    num_shards: usize,
    job: Arc<TrimJob>,
) {
    let mine: Vec<u16> = job
        .slots
        .iter()
        .copied()
        .filter(|&s| num_shards <= 1 || crate::router::slot_to_shard(s, num_shards) == shard_id)
        .collect();
    let hub = trim_hub(port);
    // A job doesn't begin while clients are paused (it writes).
    while crate::connection::is_client_paused().is_some() && !job.cancelled.load(Ordering::Relaxed)
    {
        monoio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    job.started.store(true, Ordering::Relaxed);
    if !mine.is_empty() {
        if job.log {
            let bytes = trimslots_command(&mine);
            if let Some(aof) = aof.get() {
                aof.borrow_mut().append(&bytes);
            }
            let cluster = crate::cluster::get_cluster_hub(port);
            if *cluster.role.read() == "master" {
                crate::replication::propagate_shard_bytes(port, shard_id, &bytes);
            }
        }
        let keys: u64 = {
            let mut d = db.borrow_mut();
            mine.iter().map(|&s| d.table.slot_key_count(s) as u64).sum()
        };
        job.keys.fetch_add(keys, Ordering::Relaxed);
        let mut set = [0u64; 256];
        for &s in &mine {
            set[s as usize >> 6] |= 1 << (s & 63);
        }
        // Passes over the table, a bounded step at a time with the shard's
        // other work in between. (Asking for each slot's keys in turn
        // scanned the whole table once per slot, and in bg mode never
        // yielded: trimming 8192 slots of a million keys stalled the shard
        // for minutes after a migration.) A pass can miss keys that deletes
        // moved behind its cursor, so it repeats while any are left.
        'passes: for _pass in 0..4 {
            let mut cursor = (0, 0);
            loop {
                // A job waits while clients are paused (it writes), and an
                // active one while its delay says so.
                loop {
                    if job.cancelled.load(Ordering::Relaxed) {
                        break 'passes;
                    }
                    let held = crate::connection::is_client_paused().is_some()
                        || (!job.bg && active_trim_delay() < 0);
                    if !held {
                        break;
                    }
                    monoio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                let delay = if job.bg {
                    0
                } else {
                    active_trim_delay().max(0) as u64
                };
                let budget = if delay > 0 { 256 } else { 4096 };
                let (keys, done) =
                    db.borrow()
                        .table
                        .keys_in_slot_set_step(&set, &mut cursor, budget);
                if delay > 0 {
                    // One key at a time, the delay apart, holding while
                    // clients are paused or the delay turns negative.
                    for key in keys {
                        loop {
                            if job.cancelled.load(Ordering::Relaxed) {
                                break 'passes;
                            }
                            if crate::connection::is_client_paused().is_none()
                                && active_trim_delay() >= 0
                            {
                                break;
                            }
                            monoio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                        let n = db.borrow_mut().trim_keys(vec![key]);
                        job.trimmed.fetch_add(n as u64, Ordering::Relaxed);
                        let delay = active_trim_delay().max(0) as u64;
                        monoio::time::sleep(std::time::Duration::from_micros(delay)).await;
                    }
                } else {
                    let n = db.borrow_mut().trim_keys(keys);
                    job.trimmed.fetch_add(n as u64, Ordering::Relaxed);
                    if job.bg {
                        crate::table::add_lazyfreed_objects(n as u64);
                    }
                    // Between other work.
                    monoio::time::sleep(std::time::Duration::ZERO).await;
                }
                if done {
                    break;
                }
            }
            if db.borrow_mut().slot_set_key_count(&set) == 0 {
                break;
            }
        }
    }
    if job.shards_left.fetch_sub(1, Ordering::AcqRel) == 1 {
        hub.jobs.lock().retain(|j| !Arc::ptr_eq(j, &job));
        if job.cancelled.load(Ordering::Relaxed) {
            hub.cancelled.fetch_add(1, Ordering::Relaxed);
        } else if !job.bg {
            hub.completed.fetch_add(1, Ordering::Relaxed);
        }
        rebuild_trimming();
    }
}

/// Whether `key` belongs to a slot being trimmed.
#[inline]
pub fn is_trimming_key(key: &[u8]) -> bool {
    is_trimming_slot(crate::router::key_slot(key))
}

/// Keys written to and left out of the snapshot of SAVE (0) and of the AOF
/// rewrite (1), since it started.
static SNAPSHOT_COUNTS: [(AtomicU64, AtomicU64); 2] = [
    (AtomicU64::new(0), AtomicU64::new(0)),
    (AtomicU64::new(0), AtomicU64::new(0)),
];

pub fn snapshot_counts_reset(kind: usize) {
    if let Some((saved, skipped)) = SNAPSHOT_COUNTS.get(kind) {
        saved.store(0, Ordering::Relaxed);
        skipped.store(0, Ordering::Relaxed);
    }
}

#[inline]
pub fn snapshot_count(kind: Option<usize>, saved: bool) {
    if let Some((s, k)) = kind.and_then(|k| SNAPSHOT_COUNTS.get(k)) {
        if saved { s } else { k }.fetch_add(1, Ordering::Relaxed);
    }
}

/// `(saved, skipped)` of the last SAVE (0) or AOF rewrite (1).
pub fn snapshot_counts(kind: usize) -> (u64, u64) {
    SNAPSHOT_COUNTS.get(kind).map_or((0, 0), |(s, k)| {
        (s.load(Ordering::Relaxed), k.load(Ordering::Relaxed))
    })
}

/// Number of trim jobs running on `port` (CLUSTER INFO).
pub fn trim_jobs_running(port: u16) -> usize {
    trim_hub(port)
        .jobs
        .lock()
        .iter()
        .filter(|j| j.started.load(Ordering::Relaxed))
        .count()
}

/// The CLUSTER INFO lines of trimming (`purging`: lost slots whose keys
/// the cluster's own purge still deletes).
pub fn trim_info(port: u16, purging: usize) -> String {
    let hub = trim_hub(port);
    let (keys, trimmed) = hub.current.lock().as_ref().map_or((0, 0), |j| {
        (
            j.keys.load(Ordering::Relaxed),
            j.trimmed.load(Ordering::Relaxed),
        )
    });
    format!(
        "cluster_slot_migration_active_trim_running:{}\r\n\
         cluster_slot_migration_active_trim_current_job_keys:{}\r\n\
         cluster_slot_migration_active_trim_current_job_trimmed:{}\r\n\
         cluster_slot_migration_stats_active_trim_started:{}\r\n\
         cluster_slot_migration_stats_active_trim_completed:{}\r\n\
         cluster_slot_migration_stats_active_trim_cancelled:{}\r\n",
        trim_jobs_running(port) + purging,
        keys,
        trimmed,
        hub.started.load(Ordering::Relaxed),
        hub.completed.load(Ordering::Relaxed),
        hub.cancelled.load(Ordering::Relaxed),
    )
}

/// Per shard: whether it checked for keys of slots it doesn't serve since
/// it became a master (or started).
static UNOWNED_CHECKED: LazyLock<Mutex<HashMap<(u16, usize), bool>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A shard's slot migration upkeep, from its cron: once it is a master
/// (at startup, or promoted), its keys of slots other nodes serve go.
pub fn shard_cron(
    router: &crate::router::Router,
    db: &std::rc::Rc<std::cell::RefCell<crate::shard::ShardDb>>,
    shard_id: usize,
    is_replica: bool,
) {
    let port = router.base_port;
    if !crate::cluster::has_active_cluster(port) {
        return;
    }
    let key = (port, shard_id);
    if is_replica {
        UNOWNED_CHECKED.lock().insert(key, false);
        return;
    }
    if UNOWNED_CHECKED.lock().insert(key, true) == Some(true) {
        return;
    }
    let cluster = crate::cluster::get_cluster_hub(port);
    if *cluster.role.read() != "master" {
        UNOWNED_CHECKED.lock().insert(key, false);
        return;
    }
    // A promoted replica: an import its old master ran is over.
    if !hub(port)
        .active()
        .iter()
        .any(|t| t.operation == Operation::Import)
    {
        end_all_importing(port);
    }
    let slots: Vec<u16> = {
        let mut d = db.borrow_mut();
        if d.table.is_empty() {
            return;
        }
        (0..16384u16)
            .filter(|&s| !cluster.owns_slot(s) && d.table.slot_key_count(s) > 0)
            .collect()
    };
    trim_unowned_keys(router, &slots);
}

/// A replica (or an import) applying `cmd` from its master waits while
/// the slot it writes to is being trimmed here.
pub async fn wait_trim_for(
    router: &crate::router::Router,
    cmd: &crate::resp::Command,
    batch: &mut [Vec<crate::resp::Command>],
) {
    let keys = crate::connection::cmd_keys(cmd);
    let Some(slot) = keys.first().map(|k| crate::router::key_slot(k)) else {
        return;
    };
    if !is_trimming_slot(slot) {
        return;
    }
    // What came before goes first (it may be the trim itself).
    router.flush_replica_batch(batch).await;
    if !is_trimming_slot(slot) {
        return;
    }
    crate::log_notice!(
        "Blocking master client until trim job is done (slot {})",
        slot
    );
    while is_trimming_slot(slot) {
        monoio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    crate::log_notice!("Unblocking master client after active trim");
}

/// Keys of slots another node serves, found at startup or once a replica
/// becomes a master (a migration's leftovers): trimmed. Slots nobody
/// serves are left alone.
pub fn trim_unowned_keys(router: &crate::router::Router, slots_with_keys: &[u16]) {
    let cluster = crate::cluster::get_cluster_hub(router.base_port);
    let tasks = hub(router.base_port);
    let slots: Vec<u16> = slots_with_keys
        .iter()
        .copied()
        .filter(|&s| {
            !cluster.owns_slot(s)
                && cluster.slot_owner_id(s).is_some()
                && !is_importing_slot(s)
                && !tasks.overlapping_active(&[(s, s)])
                && !slot_has_trim_job(router.base_port, s)
        })
        .collect();
    if slots.is_empty() {
        return;
    }
    crate::log_notice!(
        "Detected keys in slots that do not belong to this node. Scheduling trim for {} slots",
        slots.len()
    );
    start_trim(router, slots, true);
}

// ---------------------------------------------------------------------
// Buffers
// ---------------------------------------------------------------------

/// `replica-full-sync-buffer-limit`: most bytes of a migration's stream a
/// destination reads ahead of applying them (0: the replica class's
/// `client-output-buffer-limit` hard limit).
pub static FULL_SYNC_BUFFER_LIMIT: AtomicU64 = AtomicU64::new(0);
/// Stream bytes destinations read and haven't applied yet.
static INPUT_BUFFER: AtomicU64 = AtomicU64::new(0);
static INPUT_BUFFER_PEAK: AtomicU64 = AtomicU64::new(0);

fn input_buffer_limit() -> u64 {
    match FULL_SYNC_BUFFER_LIMIT.load(Ordering::Relaxed) {
        0 => {
            crate::connection::get_client_output_buffer_limit(
                crate::connection::ClientClass::Replica,
            )
            .hard_limit
        }
        n => n,
    }
}

/// The INFO memory lines of slot migrations.
pub fn memory_info() -> String {
    let ports: Vec<u16> = HUBS.lock().keys().copied().collect();
    let output: usize = ports
        .into_iter()
        .map(|p| crate::replication::get_replication_hub(p).migration_output_buffer())
        .sum();
    format!(
        "mem_cluster_slot_migration_output_buffer:{}\r\n\
         mem_cluster_slot_migration_input_buffer:{}\r\n\
         mem_cluster_slot_migration_input_buffer_peak:{}\r\n",
        output,
        INPUT_BUFFER.load(Ordering::Relaxed),
        INPUT_BUFFER_PEAK.load(Ordering::Relaxed)
    )
}

// ---------------------------------------------------------------------
// Importing slots (destination side and its replicas)
// ---------------------------------------------------------------------

static IMPORTING_ANY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
type ImportingSlots = HashMap<(u16, String), Vec<(u16, u16)>>;
static IMPORTING: LazyLock<Mutex<ImportingSlots>> = LazyLock::new(|| Mutex::new(HashMap::new()));
/// The union of all ports' importing slots, one bit per slot.
static IMPORTING_BITS: [AtomicU64; 256] = [const { AtomicU64::new(0) }; 256];

fn rebuild_importing(map: &ImportingSlots) {
    let mut bits = [0u64; 256];
    for ranges in map.values() {
        for &(s, e) in ranges {
            for slot in s..=e {
                bits[slot as usize / 64] |= 1 << (slot % 64);
            }
        }
    }
    for (cell, b) in IMPORTING_BITS.iter().zip(bits) {
        cell.store(b, Ordering::SeqCst);
    }
    IMPORTING_ANY.store(!map.is_empty(), Ordering::SeqCst);
}

/// Marks `ranges` as being imported by task `task` on the server on
/// `port`: their keys are hidden from key listings and counts, and are
/// neither expired nor evicted, until [`end_importing`].
pub fn begin_importing(port: u16, task: &str, ranges: &[(u16, u16)]) {
    let mut map = IMPORTING.lock();
    map.insert((port, task.to_string()), ranges.to_vec());
    rebuild_importing(&map);
}

/// Ends task `task`'s importing state.
pub fn end_importing(port: u16, task: &str) {
    let mut map = IMPORTING.lock();
    if map.remove(&(port, task.to_string())).is_some() {
        rebuild_importing(&map);
    }
}

/// Ends every importing state of the server on `port` (its data was
/// replaced, e.g. by a full sync).
pub fn end_all_importing(port: u16) {
    let mut map = IMPORTING.lock();
    let before = map.len();
    map.retain(|(p, _), _| *p != port);
    if map.len() != before {
        rebuild_importing(&map);
    }
}

/// Whether some slots are being imported now (cheap: one load).
#[inline(always)]
pub fn importing_any() -> bool {
    IMPORTING_ANY.load(Ordering::Relaxed)
}

/// Whether `slot` is being imported (by any server of this process).
#[inline]
pub fn is_importing_slot(slot: u16) -> bool {
    importing_any()
        && IMPORTING_BITS[slot as usize / 64].load(Ordering::Relaxed) & (1 << (slot % 64)) != 0
}

/// Whether `key` belongs to a slot being imported: such keys don't exist
/// yet for clients, expiry and eviction.
#[inline(always)]
pub fn is_importing_key(key: &[u8]) -> bool {
    importing_any() && is_importing_slot(crate::router::key_slot(key))
}

/// All slots being imported now.
pub fn importing_slots() -> Vec<u16> {
    if !importing_any() {
        return Vec::new();
    }
    (0..16384u16).filter(|&s| is_importing_slot(s)).collect()
}

/// A replica's handling of its master's `CLUSTER SYNCSLOTS IMPORTING
/// <task> <ranges>` / `IMPORT-END <task>` (see `import`); the bytes reach
/// sub-replicas with the rest of the master's stream.
pub fn apply_replicated_syncslots(port: u16, args: &[bytes::Bytes]) {
    let what = args
        .first()
        .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
        .unwrap_or_default();
    let task = args
        .get(1)
        .map(|a| String::from_utf8_lossy(a).into_owned())
        .unwrap_or_default();
    match what.as_str() {
        "IMPORTING" => {
            let ranges = args
                .get(2)
                .map(|r| crate::cluster::parse_slot_ranges(&String::from_utf8_lossy(r)))
                .unwrap_or_default();
            begin_importing(port, &task, &ranges);
        }
        "IMPORT-END" => end_importing(port, &task),
        _ => {}
    }
}

/// Sets the importing state of task `task` here and on our replicas.
fn announce_importing(port: u16, task: &str, ranges: &[(u16, u16)]) {
    begin_importing(port, task, ranges);
    let csv = slots_csv(ranges);
    crate::replication::propagate_bytes(
        port,
        &syncslots(&[b"IMPORTING", task.as_bytes(), csv.as_bytes()]),
    );
}

/// Ends the importing state of task `task` here and on our replicas.
fn announce_import_end(port: u16, task: &str) {
    if !IMPORTING.lock().contains_key(&(port, task.to_string())) {
        return;
    }
    end_importing(port, task);
    crate::replication::propagate_bytes(port, &syncslots(&[b"IMPORT-END", task.as_bytes()]));
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

/// On shard `shard_id`'s thread: returns the shard's keys in `ranges` as
/// `RESTORE ... REPLACE [ABSTTL]` commands, and arms the shard, so its
/// later writes to them are held for migration `task`'s stream until the
/// snapshot is sent (see `ReplicationHub::arm_migration`).
pub fn dump_slots(
    db: &mut crate::shard::ShardDb,
    port: u16,
    shard_id: usize,
    task: u64,
    ranges: &[(u16, u16)],
) -> Vec<Vec<u8>> {
    let keys = db.keys_in_slot_ranges(ranges);
    let mut cmds = Vec::with_capacity(keys.len());
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
        let mut bytes = Vec::new();
        crate::connection::write_resp_command(&mut bytes, &args);
        cmds.push(bytes);
    }
    // Extended structures (JSON, probabilistic, vector sets) aren't in the
    // table: one `CLUSTER SYNCSLOTS EXT <key> <records>` each.
    for (kind, key) in db.ext_item_keys() {
        if !in_ranges(ranges, &key) {
            continue;
        }
        let mut records = Vec::new();
        db.save_ext_item(kind, &key, &mut records);
        if !records.is_empty() {
            cmds.push(syncslots(&[b"EXT", &key, &records]));
        }
    }
    crate::replication::get_replication_hub(port).arm_migration(task, shard_id, &[]);
    cmds
}

fn in_ranges(ranges: &[(u16, u16)], key: &[u8]) -> bool {
    let slot = crate::router::key_slot(key);
    ranges.iter().any(|&(s, e)| slot >= s && slot <= e)
}

/// `CLUSTER SYNCSLOTS EXT <key> <records>` (on a destination from its
/// source, or on its replica): loads the extended structure on its shard,
/// which logs the command to its AOF; a destination passes it on to its
/// replicas.
pub async fn load_ext(router: &crate::router::Router, args: &[bytes::Bytes]) {
    let (Some(key), Some(records)) = (args.get(1), args.get(2)) else {
        return;
    };
    let refs: Vec<&[u8]> = args.iter().map(|a| a.as_ref()).collect();
    let command = syncslots(&refs);
    crate::replication::propagate_bytes(router.base_port, &command);
    let target = crate::router::target_shard(key, router.num_shards);
    if target == router.shard_id {
        load_ext_local(
            &mut router.local_db.borrow_mut(),
            router.aof().as_deref(),
            router.shard_id,
            router.num_shards,
            records,
            &command,
        );
    } else {
        let (tx, rx) = flume::bounded(1);
        if router.senders[target]
            .send(crate::shard::ShardMessage::AsmLoadExt {
                records: records.to_vec(),
                command,
                responder: tx,
            })
            .is_ok()
        {
            let _ = rx.recv_async().await;
        }
    }
}

/// [`load_ext`] on the key's shard.
pub fn load_ext_local(
    db: &mut crate::shard::ShardDb,
    aof: Option<&std::cell::RefCell<crate::aof::AofWriter>>,
    shard_id: usize,
    num_shards: usize,
    records: &[u8],
    command: &[u8],
) {
    db.select_db(0);
    match crate::table::load_legacy_ext(records, db, shard_id, num_shards) {
        Ok(_) => {
            if let Some(aof) = aof {
                aof.borrow_mut().append(command);
            }
        }
        Err(e) => crate::log_warning!("Slot migration: bad extended structure record: {}", e),
    }
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
    rep: Arc<crate::replication::ConnectedReplica>,
    task: String,
    /// Dropped with the guard: stops the writer task.
    _stop: flume::Sender<()>,
    link: LinkShutdown,
}

/// Shuts a migration's socket down when dropped, so the tasks still
/// reading or writing it (on both nodes) end and it closes.
struct LinkShutdown(i32);

impl Drop for LinkShutdown {
    fn drop(&mut self) {
        // SAFETY: shutdown(2) takes no pointers; the socket is still open
        // (its read half is owned by a task that ends once it sees this).
        unsafe {
            libc::shutdown(self.0, libc::SHUT_RDWR);
        }
    }
}

impl Drop for SourceGuard {
    fn drop(&mut self) {
        let _ = &self.link;
        self.rep.detach_fd();
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

/// Bytes of a migration's stream a destination has read and not applied.
#[derive(Default)]
struct InputBuffer(std::cell::Cell<u64>);

impl InputBuffer {
    fn add(&self, n: u64) {
        self.0.set(self.0.get() + n);
        let total = INPUT_BUFFER.fetch_add(n, Ordering::Relaxed) + n;
        INPUT_BUFFER_PEAK.fetch_max(total, Ordering::Relaxed);
    }

    fn sub(&self, n: u64) {
        let n = n.min(self.0.get());
        self.0.set(self.0.get() - n);
        INPUT_BUFFER.fetch_sub(n, Ordering::Relaxed);
    }
}

impl Drop for InputBuffer {
    fn drop(&mut self) {
        INPUT_BUFFER.fetch_sub(self.0.get(), Ordering::Relaxed);
    }
}

/// The destination's reader: whole commands (with their sizes) off
/// `reader` into `tx` until the link ends. It stops reading while more
/// than `replica-full-sync-buffer-limit` bytes wait to be applied, so the
/// rest waits on the source.
async fn read_stream<R: monoio::io::AsyncReadRent>(
    mut reader: R,
    mut buf: bytes::BytesMut,
    tx: flume::Sender<(crate::resp::Command, u64)>,
    queued: std::rc::Rc<InputBuffer>,
) {
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        loop {
            let before = buf.len();
            match crate::resp::parse_command_trusted(&mut buf) {
                Ok(Some(cmd)) => {
                    let size = (before - buf.len()) as u64;
                    queued.add(size);
                    if tx.send((cmd, size)).is_err() {
                        return;
                    }
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
        let limit = input_buffer_limit();
        while limit > 0 && queued.0.get() > limit {
            if tx.is_disconnected() {
                return;
            }
            monoio::time::sleep(std::time::Duration::from_millis(1)).await;
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
    let fd = std::os::fd::AsRawFd::as_raw_fd(&stream);
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
    let refuse = if fail_point(Operation::Migrate, &[("main", "none")]).is_some() {
        Some("-NOTREADY slot migration can't start now, try again later\r\n".to_string())
    } else if *cluster.role.read() != "master" {
        Some("-ERR not a master\r\n".to_string())
    } else if !cluster.nodes.read().contains_key(&dest) {
        Some(format!("-ERR unknown destination node {dest}\r\n"))
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
            retry_pending: false,
        },
        cancel_tx,
    );
    let repl = crate::replication::get_replication_hub(port);
    let (wake_tx, wake_rx) = flume::unbounded::<Vec<u8>>();
    let rep = repl.register_migration(client_id, wake_tx, router.num_shards);
    // Lets an output buffer overflow cut the link (see `wake_replicas`).
    rep.attach_fd(fd);
    let (stop_tx, stop_rx) = flume::bounded::<()>(1);
    let _guard = SourceGuard {
        port,
        rep_id: client_id,
        rep: rep.clone(),
        task: task_id.clone(),
        _stop: stop_tx,
        link: LinkShutdown(fd),
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
    // The writer: the stream, filtered to the slots. A write that makes no
    // progress for `repl-timeout` fails the migration.
    let (write_err_tx, write_err_rx) = flume::bounded::<String>(1);
    let writer_ranges = ranges.clone();
    let writer_rep = rep.clone();
    let writer_task = task_id.clone();
    let last_progress = Arc::new(AtomicU64::new(unix_ms()));
    let writer_progress = last_progress.clone();
    let overflowed = move || {
        crate::log_warning!(
            "Client id={} closed (slot migration {}) for overcoming of output buffer limits.",
            client_id,
            writer_task
        );
        "client output buffer limit reached".to_string()
    };
    monoio::spawn(async move {
        loop {
            let woke = monoio::select! {
                w = wake_rx.recv_async() => w.is_ok(),
                _ = stop_rx.recv_async() => false,
            };
            if !woke {
                return;
            }
            if writer_rep.is_overflowed() {
                let _ = write_err_tx.send(overflowed());
                return;
            }
            let data = writer_rep.take_pending();
            if data.is_empty() {
                writer_progress.store(unix_ms(), Ordering::Relaxed);
                continue;
            }
            let out = filter_stream(&data, &writer_ranges);
            let failed = if out.is_empty() {
                None
            } else {
                match monoio::time::timeout(
                    crate::replication::repl_timeout(),
                    writer.write_all(out),
                )
                .await
                {
                    Ok((Ok(_), _)) => None,
                    Ok((Err(_), _)) => Some("connection lost"),
                    Err(_) => Some("Connection timeout"),
                }
            };
            writer_rep.finish_write();
            writer_progress.store(unix_ms(), Ordering::Relaxed);
            if let Some(why) = failed {
                let why = if writer_rep.is_overflowed() {
                    overflowed()
                } else {
                    why.to_string()
                };
                let _ = write_err_tx.send(why);
                return;
            }
        }
    });
    // The destination's requests.
    let (req_tx, req_rx) = flume::unbounded();
    monoio::spawn(read_commands(reader, bytes::BytesMut::new(), req_tx));
    let fail = |state: State, why: &str| {
        crate::log_warning!("Slot migration {} {}: {}", task_id, state.name(), why);
        tasks.finish(&task_id, state, why);
        // Slots the destination took over anyway: our keys there go.
        let lost: Vec<u16> = ranges
            .iter()
            .flat_map(|&(s, e)| s..=e)
            .filter(|&s| !cluster.owns_slot(s) && cluster.slot_owner_id(s).is_some())
            .collect();
        start_trim(&router, lost, true);
    };
    // What a failed write to the destination means in the current state.
    let link_error = |why: &str| {
        let state = tasks.get(&task_id).map_or(State::SendStream, |t| t.state);
        if state == State::SendBulkAndStream {
            format!(
                "RDB channel - Failed to send slots snapshot: {why} (state: {})",
                state.name()
            )
        } else {
            format!("Main channel - {why} (state: {})", state.name())
        }
    };

    if let Some(why) = point(&[
        ("main", "send-bulk-and-stream"),
        ("rdb", "send-bulk-and-stream"),
    ]) {
        fail(State::Failed, &why);
        return;
    }
    // The snapshot: each shard dumps its keys of the slots and arms itself.
    let mut snapshot: Vec<Vec<u8>> = Vec::new();
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
    snapshot.extend(dump_slots(
        &mut router.local_db.borrow_mut(),
        port,
        router.shard_id,
        client_id,
        &ranges,
    ));
    for rx in waits {
        snapshot.extend(rx.recv_async().await.unwrap_or_default());
    }
    let keys = snapshot.len();
    // The keys go out ahead of the writes held meanwhile. With
    // `rdb-key-save-delay` (tests), one key per delay, as a forked child
    // saving them would.
    let delay_us = crate::connection::RDB_KEY_SAVE_DELAY.load(Ordering::Relaxed);
    if delay_us == 0 {
        // Straight into the replica's buffer: concatenating first copied
        // the whole snapshot once more on this thread.
        rep.send_bytes_unlimited_parts(&snapshot);
        drop(snapshot);
    } else {
        let per_key = std::time::Duration::from_micros(delay_us);
        for cmd in snapshot {
            rep.send_bytes_unlimited(&cmd);
            if let Ok(reason) = monoio::time::timeout(per_key, cancel_rx.recv_async()).await {
                fail(State::Canceled, &reason.unwrap_or_default());
                return;
            }
            if let Ok(why) = write_err_rx.try_recv() {
                fail(State::Failed, &link_error(&why));
                return;
            }
            if let Some(why) = config_changed(port, &tasks, &task_id, Operation::Migrate) {
                fail(State::Canceled, &why);
                return;
            }
        }
    }
    let mut head = Vec::new();
    let functions = crate::scripting::dump_functions();
    if !functions.is_empty() {
        head.extend_from_slice(&resp_command(&[
            b"FUNCTION",
            b"RESTORE",
            &functions,
            b"REPLACE",
        ]));
    }
    head.extend_from_slice(&syncslots(&[b"SNAPSHOT-EOF"]));
    repl.finish_migration_snapshot(client_id, &head, &syncslots(&[b"BUFFER-EOF"]));
    tasks.set_state(&task_id, State::SendStream);
    if let Some(why) = point(&[("main", "send-stream")]) {
        fail(State::Failed, &why);
        return;
    }
    crate::log_notice!(
        "Slot migration {}: sent {} keys; streaming the writes to the slots",
        task_id,
        keys
    );

    let mut pause_requested = false;
    let mut pause_started: Option<std::time::Instant> = None;
    let mut stream_ended = false;
    let mut last_ping = std::time::Instant::now();
    let pause_timeout =
        || std::time::Duration::from_millis(WRITE_PAUSE_TIMEOUT_MS.load(Ordering::Relaxed));
    loop {
        let req = monoio::select! {
            r = req_rx.recv_async() => Some(r.ok()),
            c = cancel_rx.recv_async() => {
                fail(State::Canceled, &c.unwrap_or_default());
                return;
            }
            e = write_err_rx.recv_async() => {
                if stream_ended {
                    // Only the ACK was left to send: the destination
                    // learns the outcome from the cluster bus.
                    None
                } else {
                    fail(
                        State::Failed,
                        &link_error(&e.unwrap_or_else(|_| "connection lost".to_string())),
                    );
                    return;
                }
            }
            _ = monoio::time::sleep(TICK) => None,
        };
        if let Some(why) = config_changed(port, &tasks, &task_id, Operation::Migrate) {
            fail(State::Canceled, &why);
            return;
        }
        if let Some(req) = req {
            let Some(crate::resp::Command::SyncSlots(req)) = req else {
                // A link we cut (output buffer limit, a stuck write) shows
                // here first: the writer's reason is the one to report.
                let write_err = monoio::time::timeout(
                    std::time::Duration::from_millis(100),
                    write_err_rx.recv_async(),
                )
                .await
                .ok()
                .and_then(|e| e.ok());
                let why = if let Some(e) = write_err.filter(|_| !stream_ended) {
                    link_error(&e)
                } else if stream_ended
                    && pause_started.is_some_and(|t| t.elapsed() >= pause_timeout())
                {
                    "Write pause timeout (state: handoff)".to_string()
                } else {
                    "Main channel - connection closed by the destination".to_string()
                };
                fail(State::Failed, &why);
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
                    pause_requested = true;
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
                    // Our keys of the slots go (and our replicas', from the
                    // TRIMSLOTS the trim logs): the job counts as running
                    // from before the task ends.
                    start_trim(
                        &router,
                        ranges.iter().flat_map(|&(s, e)| s..=e).collect(),
                        true,
                    );
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
        if stream_ended {
            // Writes stay paused only so long, if the destination never
            // says it took the slots.
            if pause_started.is_some_and(|t| t.elapsed() > pause_timeout()) {
                fail(State::Failed, "Write pause timeout (state: handoff)");
                return;
            }
            continue;
        }
        // The destination isn't taking what we send.
        let drain_ms = SYNC_BUFFER_DRAIN_TIMEOUT_MS.load(Ordering::Relaxed);
        if rep.has_pending()
            && unix_ms().saturating_sub(last_progress.load(Ordering::Relaxed)) > drain_ms
        {
            fail(
                State::Failed,
                "Sync buffer drain timeout (state: send-stream)",
            );
            return;
        }
        if last_ping.elapsed() >= std::time::Duration::from_secs(1) {
            last_ping = std::time::Instant::now();
            rep.send_bytes(&syncslots(&[b"PING"]));
        }
        // The handover, once asked for (a `handoff-prep` fail point holds
        // it back).
        if !pause_requested || point(&[("main", "handoff-prep")]).is_some() {
            continue;
        }
        pause_slots(port, &task_id, &ranges);
        let started = std::time::Instant::now();
        pause_started = Some(started);
        // Writes that passed the pause gate before it closed, and bytes
        // shards still hold, go out before the end mark.
        while crate::connection::writes_running() > 0
            || !crate::replication::streams_flushed(port, router.num_shards)
        {
            if started.elapsed() > pause_timeout() {
                break;
            }
            monoio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        if started.elapsed() >= pause_timeout() {
            fail(State::Failed, "Write pause timeout (state: handoff)");
            return;
        }
        repl.append_migration(client_id, &syncslots(&[b"STREAM-EOF"]));
        // Nothing more goes into the stream: once the slots are handed
        // over, this node deletes its keys there, and those deletes must
        // not reach the destination.
        repl.unregister_migration(client_id);
        tasks.set_state(&task_id, State::StreamDone);
        stream_ended = true;
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
    mut source: String,
    cancel_rx: flume::Receiver<String>,
) {
    let port = router.base_port;
    let tasks = hub(port);
    loop {
        let result = import(&router, &task_id, &ranges, &source, &cancel_rx).await;
        match result {
            Ok(()) => {
                tasks.finish(&task_id, State::Completed, "");
                crate::log_notice!(
                    "Slot migration {}: imported slots {}",
                    task_id,
                    ranges_repr(&ranges)
                );
                break;
            }
            Err((state, why)) => {
                crate::log_warning!("Slot migration {} {}: {}", task_id, state.name(), why);
                // What was imported of slots we didn't take over goes.
                trim_slots(&router, &ranges);
                announce_import_end(port, &task_id);
                if state != State::Failed {
                    tasks.finish(&task_id, state, &why);
                    break;
                }
                // Still running: tried again after a pause (and while
                // `DEBUG PAUSE-CRON` holds retries), until it completes or
                // is canceled.
                tasks.update(&task_id, |t| {
                    t.state = State::Failed;
                    t.last_error = why.clone();
                    t.retry_pending = true;
                });
                let mut waited = std::time::Duration::ZERO;
                let canceled = loop {
                    if let Ok(reason) = monoio::time::timeout(TICK, cancel_rx.recv_async()).await {
                        break Some(reason.unwrap_or_default());
                    }
                    waited += TICK;
                    if waited >= RETRY_DELAY
                        && !crate::connection::PAUSE_CRON.load(Ordering::Relaxed)
                    {
                        break None;
                    }
                };
                if let Some(reason) = canceled {
                    tasks.finish(&task_id, State::Canceled, &reason);
                    break;
                }
                if !tasks.get(&task_id).is_some_and(|t| t.is_active()) {
                    break;
                }
                // The slots may have a new owner by now (a failover).
                match retry_source(port, &ranges) {
                    Ok(owner) => source = owner,
                    Err(e) => {
                        crate::log_warning!("Slot migration {}: not retried: {}", task_id, e);
                        tasks.finish(
                            &task_id,
                            State::Failed,
                            &format!("{why} (not retried: {e})"),
                        );
                        break;
                    }
                }
                tasks.update(&task_id, |t| {
                    t.retries += 1;
                    t.retry_pending = false;
                    t.state = State::None;
                    t.source = source.clone();
                });
                crate::log_notice!("Slot migration {}: trying again (from {})", task_id, source);
            }
        }
    }
    announce_import_end(port, &task_id);
    SOURCE_REPLICAS.lock().remove(&task_id);
}

/// How long a failed import waits before it is tried again.
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// The node a failed import of `ranges` is tried again from: we are still
/// a master and one other node serves all of them.
fn retry_source(port: u16, ranges: &[(u16, u16)]) -> Result<String, String> {
    let cluster = crate::cluster::get_cluster_hub(port);
    if *cluster.role.read() != "master" {
        return Err("this node is a replica now".to_string());
    }
    let me = cluster.my_id();
    let mut owner: Option<String> = None;
    for slot in ranges.iter().flat_map(|&(s, e)| s..=e) {
        match cluster.slot_owner_id(slot) {
            None => return Err(format!("slot {slot} has no owner")),
            Some(o) if o == me => return Err(format!("this node serves slot {slot} already")),
            Some(o) => match &owner {
                Some(prev) if *prev != o => {
                    return Err("the slots are served by different nodes".to_string());
                }
                Some(_) => {}
                None => owner = Some(o),
            },
        }
    }
    owner.ok_or_else(|| "no slots".to_string())
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
    // Not while slots of ours are still trimmed.
    let mut logged = false;
    while trim_overlaps(port, ranges) {
        if !logged {
            logged = true;
            crate::log_notice!(
                "Can not start import task {}: trim in progress for some of the slots",
                task_id
            );
        }
        if let Ok(reason) = monoio::time::timeout(TICK, cancel_rx.recv_async()).await {
            return Err((State::Canceled, reason.unwrap_or_default()));
        }
    }
    // Not while clients are paused: the task waits, in state `none`.
    while crate::connection::is_client_paused().is_some() {
        if let Ok(reason) = monoio::time::timeout(TICK, cancel_rx.recv_async()).await {
            return Err((State::Canceled, reason.unwrap_or_default()));
        }
    }
    tasks.set_state(task_id, State::Connecting);
    point(&[("main", "connecting"), ("rdb", "connecting")])?;
    let addr = {
        let cluster = crate::cluster::get_cluster_hub(port);
        let nodes = cluster.nodes.read();
        let n = nodes
            .get(source)
            .ok_or_else(|| failed(format!("source node {source} is unknown")))?;
        let replicas = nodes
            .iter()
            .filter(|(_, r)| r.master_id == source)
            .map(|(id, _)| id.clone())
            .collect();
        SOURCE_REPLICAS.lock().insert(task_id.to_string(), replicas);
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
    let _link = LinkShutdown(std::os::fd::AsRawFd::as_raw_fd(&stream));
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
    if line.starts_with("-NOTREADY") {
        crate::log_notice!(
            "Slot migration {}: Source node replied to SYNCSLOTS SYNC with -NOTREADY, will retry later",
            task_id
        );
        return Err(failed(format!("source not ready: {line}")));
    }
    if !line.starts_with('+') {
        return Err(failed(format!("source refused the migration: {line}")));
    }
    point(&[("main", "syncslots-reply"), ("rdb", "rdbchannel-reply")])?;
    announce_importing(port, task_id, ranges);
    tasks.set_state(task_id, State::AccumulateBuffer);
    let (reader, mut writer) = stream.into_split();
    let (cmd_tx, cmd_rx) = flume::unbounded();
    let queued = std::rc::Rc::new(InputBuffer::default());
    INPUT_BUFFER_PEAK.store(INPUT_BUFFER.load(Ordering::Relaxed), Ordering::Relaxed);
    monoio::spawn(read_stream(reader, buf, cmd_tx, queued.clone()));
    let mut batch: Vec<Vec<crate::resp::Command>> = vec![Vec::new(); router.num_shards];
    let mut stream_ended = false;
    let mut buffer_ended = false;
    let mut accumulated: Option<(std::collections::VecDeque<(crate::resp::Command, u64)>, u64)> =
        None;
    let mut checked = State::None;
    // When the source last sent something (its snapshot only, while that
    // transfers): it keeps the link alive with PINGs after.
    let mut last_data = std::time::Instant::now();
    let mut last_check = std::time::Instant::now();
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
        // Caught up with what the source buffered during the snapshot:
        // ask it to hand the slots over.
        if buffer_ended
            && state == State::StreamingBuffer
            && queued.0.get() <= HANDOFF_MAX_LAG_BYTES.load(Ordering::Relaxed)
        {
            router.flush_replica_batch(&mut batch).await;
            if writer.write_all(syncslots(&[b"PAUSE"])).await.0.is_err() {
                return Err(failed("Main channel - connection lost".to_string()));
            }
            tasks.set_state(task_id, State::WaitStreamEof);
            continue;
        }
        let cmd = monoio::select! {
            c = cmd_rx.recv_async() => Some(c.ok()),
            c = cancel_rx.recv_async() => {
                router.flush_replica_batch(&mut batch).await;
                return Err((State::Canceled, c.unwrap_or_default()));
            }
            _ = monoio::time::sleep(TICK) => None,
        };
        // The cluster may have changed under the import (checked at least
        // every tick, however busy the stream).
        if last_check.elapsed() >= TICK {
            last_check = std::time::Instant::now();
            if let Some(why) = config_changed(port, &tasks, task_id, Operation::Import) {
                router.flush_replica_batch(&mut batch).await;
                return Err(change_outcome(why));
            }
        }
        let Some(cmd) = cmd else {
            router.flush_replica_batch(&mut batch).await;
            if last_data.elapsed() >= crate::replication::repl_timeout() {
                let why = if state == State::AccumulateBuffer {
                    "RDB channel - Connection timeout (state: rdbchannel-transfer)".to_string()
                } else {
                    format!(
                        "Main channel - Connection timeout (state: {})",
                        state.name()
                    )
                };
                return Err(failed(why));
            }
            continue;
        };
        let Some((cmd, size)) = cmd else {
            router.flush_replica_batch(&mut batch).await;
            return Err(failed(
                "Main channel - connection to the source lost".to_string(),
            ));
        };
        let is_ping = matches!(&cmd, crate::resp::Command::SyncSlots(a)
            if a.first().is_some_and(|w| w.eq_ignore_ascii_case(b"PING")));
        if !is_ping || state != State::AccumulateBuffer {
            last_data = std::time::Instant::now();
        }
        let marker = match &cmd {
            crate::resp::Command::SyncSlots(args) => args
                .first()
                .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase()),
            _ => None,
        };
        // What the source held back during the snapshot is read ahead into
        // a buffer (Redis's destination accumulates it while the snapshot
        // loads), up to `replica-full-sync-buffer-limit`; then applied.
        if let Some((buffered, bytes)) = accumulated.as_mut()
            && marker.as_deref() != Some("BUFFER-EOF")
        {
            *bytes += size;
            buffered.push_back((cmd, size));
            let limit = input_buffer_limit();
            if limit > 0 && *bytes > limit {
                crate::log_notice!(
                    "Slot migration {}: Slots sync buffer limit has been reached ({} bytes); \
                     the rest of the stream waits on the source",
                    task_id,
                    limit
                );
                let (buffered, _) = accumulated.take().expect("set above");
                apply_buffered(router, &mut batch, &queued, buffered, cancel_rx).await?;
            }
            continue;
        }
        queued.sub(size);
        match cmd {
            crate::resp::Command::SyncSlots(args) => {
                router.flush_replica_batch(&mut batch).await;
                match marker.as_deref().unwrap_or_default() {
                    "SNAPSHOT-EOF" => {
                        tasks.set_state(task_id, State::StreamingBuffer);
                        point(&[("main", "streaming-buffer")])?;
                        repl_pause_point("on-streaming-repl-buf");
                        accumulated = Some((std::collections::VecDeque::new(), 0));
                    }
                    "BUFFER-EOF" => {
                        if let Some((buffered, _)) = accumulated.take() {
                            apply_buffered(router, &mut batch, &queued, buffered, cancel_rx)
                                .await?;
                        }
                        buffer_ended = true;
                    }
                    "EXT" => load_ext(router, &args).await,
                    "STREAM-EOF" => {
                        tasks.set_state(task_id, State::Takeover);
                        point(&[("main", "takeover")])?;
                        stream_ended = true;
                        let (epoch, slots) = take_over(router, ranges).await;
                        announce_import_end(port, task_id);
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
                apply_imported(router, &mut batch, cmd).await;
                if cmd_rx.is_empty() {
                    router.flush_replica_batch(&mut batch).await;
                }
            }
        }
    }
}

/// Applies one command of a migration's stream on the destination.
async fn apply_imported(
    router: &crate::router::Router,
    batch: &mut [Vec<crate::resp::Command>],
    cmd: crate::resp::Command,
) {
    crate::connection::WithCurrentDb {
        router,
        db: 0,
        fut: router.apply_replica_command_batched(cmd, batch),
    }
    .await;
}

/// Applies what the destination buffered of the stream (slowly under
/// `key-load-delay`, as tests use it).
async fn apply_buffered(
    router: &crate::router::Router,
    batch: &mut [Vec<crate::resp::Command>],
    queued: &InputBuffer,
    buffered: std::collections::VecDeque<(crate::resp::Command, u64)>,
    cancel_rx: &flume::Receiver<String>,
) -> Result<(), (State, String)> {
    let delay = crate::connection::key_load_delay_us();
    for (cmd, size) in buffered {
        if let Ok(reason) = cancel_rx.try_recv() {
            router.flush_replica_batch(batch).await;
            return Err((State::Canceled, reason));
        }
        queued.sub(size);
        apply_imported(router, batch, cmd).await;
        if delay > 0 {
            router.flush_replica_batch(batch).await;
            monoio::time::sleep(std::time::Duration::from_micros(delay)).await;
        }
    }
    router.flush_replica_batch(batch).await;
    Ok(())
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

    #[test]
    fn slots_to_ranges_merges_runs() {
        assert_eq!(
            slots_to_ranges(&[5, 1, 2, 3, 9, 3]),
            vec![(1, 3), (5, 5), (9, 9)]
        );
        assert!(slots_to_ranges(&[]).is_empty());
    }

    #[test]
    fn trimslots_command_lists_ranges() {
        assert_eq!(
            trimslots_command(&[0, 1, 2, 7]),
            b"*7\r\n$9\r\nTRIMSLOTS\r\n$6\r\nRANGES\r\n$1\r\n2\r\n$1\r\n0\r\n$1\r\n2\r\n$1\r\n7\r\n$1\r\n7\r\n"
                .to_vec()
        );
    }

    /// Slots a task imports are hidden until it ends, per port and task.
    #[test]
    fn importing_slots_follow_tasks() {
        let (port, other) = (61001, 61002);
        begin_importing(port, "t1", &[(100, 102)]);
        begin_importing(other, "t2", &[(200, 200)]);
        assert!(is_importing_slot(101) && is_importing_slot(200));
        assert!(!is_importing_slot(103));
        assert!(importing_slots().contains(&102));
        end_importing(port, "t1");
        assert!(!is_importing_slot(101) && is_importing_slot(200));
        end_all_importing(other);
        assert!(!is_importing_slot(200));
    }

    /// The destination's read-ahead counter feeds the INFO totals and is
    /// given back when dropped.
    #[test]
    fn input_buffer_accounting() {
        let before = INPUT_BUFFER.load(Ordering::Relaxed);
        {
            let b = InputBuffer::default();
            b.add(1000);
            b.sub(400);
            assert_eq!(b.0.get(), 600);
            assert!(INPUT_BUFFER_PEAK.load(Ordering::Relaxed) >= 1000);
        }
        assert_eq!(INPUT_BUFFER.load(Ordering::Relaxed), before);
    }

    #[test]
    fn trim_method_parses() {
        assert!(set_trim_method(b"bogus", None).is_err());
        assert!(set_trim_method(b"active", Some(b"x")).is_err());
        set_trim_method(b"active", Some(b"-1")).unwrap();
        assert_eq!(active_trim_delay(), -1);
        assert!(!trim_in_bg());
        set_trim_method(b"none", None).unwrap();
        assert!(trim_disabled());
        set_trim_method(b"default", None).unwrap();
        assert!(!trim_disabled());
    }
}
