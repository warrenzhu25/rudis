use bytes::BytesMut;
use std::path::{Path, PathBuf};

use crate::resp::Command;
use crate::shard::ShardDb;

/// Absolute unix-ms deadline `d` from now. Expiries are written to the AOF as
/// absolute times (like Redis' PEXPIREAT/PXAT propagation) so that downtime
/// between write and replay does not extend a key's lifetime.
fn unix_ms_after(d: std::time::Duration) -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        + d.as_millis()
}

#[derive(Clone, Debug)]
pub struct AofConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub fsync_every_sec: bool,
}

impl Default for AofConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: PathBuf::from("."),
            fsync_every_sec: true,
        }
    }
}

/// One shard's AOF progress, readable from any thread: bytes appended so far
/// and bytes handed to the kernel (both monotonic, unaffected by rewrites).
/// A connection compares them before replying to a write batch (see
/// [`AofWriter::write_through`]).
#[derive(Debug, Default)]
pub struct AofProgress {
    pub appended: std::sync::atomic::AtomicU64,
    pub written: std::sync::atomic::AtomicU64,
    /// Appended bytes an fsync has made durable (WAITAOF's local copy).
    pub fsynced: std::sync::atomic::AtomicU64,
    /// errno of this shard's last failed AOF write, 0 once a write succeeds.
    pub write_error: std::sync::atomic::AtomicI32,
}

/// Shards (of any server) whose last AOF write failed: lets the write path
/// skip the per-port check while there are none.
static AOF_SHARDS_IN_ERROR: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl AofProgress {
    /// Records the outcome of an AOF write (`Err(errno)` or `Ok`), logging
    /// the transitions like Redis.
    fn record_write(&self, res: Result<(), i32>) {
        use std::sync::atomic::Ordering::SeqCst;
        static LOG_LIMIT: crate::log::RateLimit = crate::log::RateLimit::new();
        match res {
            Err(errno) => {
                let errno = errno.max(1);
                if self.write_error.swap(errno, SeqCst) == 0 {
                    AOF_SHARDS_IN_ERROR.fetch_add(1, SeqCst);
                }
                if LOG_LIMIT.allow(std::time::Duration::from_secs(1)) {
                    crate::log_warning!(
                        "Error writing to the AOF file: {}",
                        std::io::Error::from_raw_os_error(errno)
                    );
                }
            }
            Ok(()) => {
                if self.write_error.load(SeqCst) != 0 && self.write_error.swap(0, SeqCst) != 0 {
                    AOF_SHARDS_IN_ERROR.fetch_sub(1, SeqCst);
                    crate::log_warning!("AOF write error looks solved, Redis can write again.");
                }
            }
        }
    }

    /// Whether some appended bytes are not yet written to the kernel.
    #[inline]
    pub fn pending(&self) -> bool {
        self.written.load(std::sync::atomic::Ordering::Acquire)
            < self.appended.load(std::sync::atomic::Ordering::Acquire)
    }
}

type ProgressMap = std::collections::HashMap<(u16, usize), std::sync::Arc<AofProgress>>;
static AOF_PROGRESS: std::sync::LazyLock<parking_lot::Mutex<ProgressMap>> =
    std::sync::LazyLock::new(Default::default);

/// Shard `shard`'s progress on the server at `port` (created on first use,
/// so the writer and the connections share it whichever comes first).
pub fn aof_progress(port: u16, shard: usize) -> std::sync::Arc<AofProgress> {
    AOF_PROGRESS
        .lock()
        .entry((port, shard))
        .or_default()
        .clone()
}

/// The `-MISCONF` reply for write commands while an AOF write of the server
/// at `port` is failing (e.g. disk full), as Redis refuses writes then: it
/// would otherwise keep acknowledging writes it cannot persist. `None` when
/// writes are allowed.
pub fn aof_write_misconf(port: u16) -> Option<String> {
    if AOF_SHARDS_IN_ERROR.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return None;
    }
    let errno = AOF_PROGRESS
        .lock()
        .iter()
        .filter(|((p, _), _)| *p == port)
        .map(|(_, pr)| pr.write_error.load(std::sync::atomic::Ordering::Relaxed))
        .find(|&e| e != 0)?;
    Some(format!(
        "-MISCONF Errors writing to the AOF file: {}\r\n",
        std::io::Error::from_raw_os_error(errno)
    ))
}

/// Every shard's progress on the server at `port`.
pub fn aof_progress_all(port: u16, shards: usize) -> Vec<std::sync::Arc<AofProgress>> {
    (0..shards).map(|s| aof_progress(port, s)).collect()
}

/// A shard's AOF writer, if AOF is on, shared by everything on the shard
/// that appends to it (its router and their clones, its mailbox loop, the
/// expiry log). Swapped when `CONFIG SET appendonly` turns AOF on or off.
#[derive(Clone, Default)]
pub struct AofSlot(
    std::rc::Rc<std::cell::RefCell<Option<std::rc::Rc<std::cell::RefCell<AofWriter>>>>>,
);

impl AofSlot {
    pub fn new(writer: Option<std::rc::Rc<std::cell::RefCell<AofWriter>>>) -> Self {
        Self(std::rc::Rc::new(std::cell::RefCell::new(writer)))
    }

    /// The writer now (None with AOF off).
    #[inline]
    pub fn get(&self) -> Option<std::rc::Rc<std::cell::RefCell<AofWriter>>> {
        self.0.borrow().clone()
    }

    #[inline]
    pub fn is_on(&self) -> bool {
        self.0.borrow().is_some()
    }

    pub fn set(&self, writer: Option<std::rc::Rc<std::cell::RefCell<AofWriter>>>) {
        *self.0.borrow_mut() = writer;
    }

    /// Whether `writer` is the one in the slot (its flusher stops once not).
    pub fn holds(&self, writer: &std::rc::Rc<std::cell::RefCell<AofWriter>>) -> bool {
        self.0
            .borrow()
            .as_ref()
            .is_some_and(|w| std::rc::Rc::ptr_eq(w, writer))
    }
}

impl From<Option<std::rc::Rc<std::cell::RefCell<AofWriter>>>> for AofSlot {
    fn from(writer: Option<std::rc::Rc<std::cell::RefCell<AofWriter>>>) -> Self {
        Self::new(writer)
    }
}

pub struct AofWriter {
    buffer: Vec<u8>,
    spare_buffer: Option<Vec<u8>>,
    file: Option<std::rc::Rc<monoio::fs::File>>,
    path: PathBuf,
    offset: u64,
    current_db: usize,
    /// Chunks handed out by `take_flush_chunk` whose write hasn't finished.
    /// Chunks go to pre-assigned offsets from several tasks, so an fsync
    /// only covers everything up to `offset` once none is in flight.
    inflight: usize,
    /// Everything before this offset is on disk (see [`AofWriter::sync_durable`]).
    durable: u64,
    /// Bytes ever appended (monotonic, unlike `offset`, which a rewrite resets).
    appended_total: u64,
    progress: std::sync::Arc<AofProgress>,
}

impl AofWriter {
    pub async fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let file = monoio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await?;
        let offset = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            buffer: Vec::with_capacity(65536),
            spare_buffer: None,
            file: Some(std::rc::Rc::new(file)),
            path,
            offset,
            current_db: 0,
            inflight: 0,
            durable: offset,
            appended_total: 0,
            progress: std::sync::Arc::new(AofProgress::default()),
        })
    }

    pub fn new_in_memory() -> Self {
        Self {
            buffer: Vec::with_capacity(65536),
            spare_buffer: None,
            file: None,
            path: PathBuf::new(),
            offset: 0,
            current_db: 0,
            inflight: 0,
            durable: 0,
            appended_total: 0,
            progress: std::sync::Arc::new(AofProgress::default()),
        }
    }

    /// Publishes this writer's progress in `progress` (shared with the
    /// connections of its server). Call right after opening.
    pub fn set_progress(&mut self, progress: std::sync::Arc<AofProgress>) {
        use std::sync::atomic::Ordering::Relaxed;
        let base = progress
            .appended
            .load(Relaxed)
            .max(progress.written.load(Relaxed));
        self.appended_total = base + self.buffer.len() as u64;
        progress.written.store(base, Relaxed);
        progress.fsynced.fetch_max(base, Relaxed);
        progress.appended.store(self.appended_total, Relaxed);
        self.progress = progress;
    }

    /// Appended bytes already handed to the kernel: what an fsync started
    /// now makes durable.
    #[inline]
    pub fn written_total(&self) -> u64 {
        self.progress
            .written
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Records that an fsync made the first `upto` appended bytes durable.
    #[inline]
    pub fn mark_fsynced(&self, upto: u64) {
        self.progress
            .fsynced
            .fetch_max(upto, std::sync::atomic::Ordering::Release);
    }

    /// With no chunk in flight, everything taken from the buffer is written.
    #[inline]
    fn mark_written_if_idle(&mut self) {
        if self.inflight == 0 {
            self.progress.written.store(
                self.appended_total - self.buffer.len() as u64,
                std::sync::atomic::Ordering::Release,
            );
        }
    }

    #[inline]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline]
    pub fn append(&mut self, data: &[u8]) {
        let db = crate::connection::CURRENT_DB.get();
        let before = self.buffer.len();
        if db != self.current_db {
            self.emit_select_db(db);
        }
        self.buffer.extend_from_slice(data);
        self.note_appended(before);
    }

    /// Appends `cmd` encoded as RESP (see [`encode_command`]) and returns
    /// where it starts in [`Self::buffer`], or `None` (appending nothing)
    /// if it has no encoding. Encoded in place: no temporary buffer per
    /// command, its regrowth or the copy out of it.
    #[inline]
    pub fn append_command(&mut self, cmd: &Command) -> Option<usize> {
        let db = crate::connection::CURRENT_DB.get();
        let (before, prev_db) = (self.buffer.len(), self.current_db);
        if db != self.current_db {
            self.emit_select_db(db);
        }
        let start = self.buffer.len();
        if let Command::Set {
            expire_in: None,
            keepttl: false,
            ..
        } = cmd
        {
            // The most common logged write: skip the generic encoder.
            encode_plain_set(cmd, &mut self.buffer);
        } else if !encode_command(cmd, &mut self.buffer) {
            self.buffer.truncate(before);
            self.current_db = prev_db;
            return None;
        }
        self.note_appended(before);
        Some(start)
    }

    #[inline]
    fn note_appended(&mut self, before: usize) {
        self.appended_total += (self.buffer.len() - before) as u64;
        self.progress
            .appended
            .store(self.appended_total, std::sync::atomic::Ordering::Release);
        if APPENDFSYNC_ALWAYS_ANY.load(std::sync::atomic::Ordering::Relaxed) {
            AOF_APPENDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[cold]
    fn emit_select_db(&mut self, db: usize) {
        self.current_db = db;
        let db_str = db.to_string();
        self.buffer.extend_from_slice(
            format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).as_bytes(),
        );
    }

    #[inline]
    pub fn recycle_chunk(&mut self, mut chunk: Vec<u8>) {
        if chunk.capacity() <= 4 * 1024 * 1024 {
            chunk.clear();
            self.spare_buffer = Some(chunk);
        }
    }

    /// Reports that a chunk from `take_flush_chunk` (taken at `offset`)
    /// finished writing: `res` is the write's outcome and `chunk` its buffer
    /// back. A failed chunk is put back in front of the buffer (when no later
    /// chunk was taken since), so the next write retries it, as Redis keeps
    /// unwritten AOF data: dropped, nothing was left to retry and writes
    /// stayed refused (-MISCONF) even after the disk had room again.
    #[inline]
    pub fn chunk_written(&mut self, res: &std::io::Result<()>, chunk: Vec<u8>, offset: u64) {
        self.inflight = self.inflight.saturating_sub(1);
        match res {
            Ok(()) => {
                self.recycle_chunk(chunk);
                self.mark_written_if_idle();
                self.progress.record_write(Ok(()));
            }
            Err(e) => {
                if offset + chunk.len() as u64 == self.offset {
                    let mut buf = chunk;
                    buf.extend_from_slice(&self.buffer);
                    self.buffer = buf;
                    self.offset = offset;
                }
                self.progress
                    .record_write(Err(e.raw_os_error().unwrap_or(libc::EIO)));
            }
        }
    }

    #[inline]
    pub fn take_flush_chunk(&mut self) -> Option<(std::rc::Rc<monoio::fs::File>, Vec<u8>, u64)> {
        if self.buffer.is_empty() {
            return None;
        }
        let file = self.file.clone()?;
        let next_buf = self
            .spare_buffer
            .take()
            .unwrap_or_else(|| Vec::with_capacity(65536));
        let chunk = std::mem::replace(&mut self.buffer, next_buf);
        let off = self.offset;
        self.offset += chunk.len() as u64;
        self.inflight += 1;
        Some((file, chunk, off))
    }

    #[inline]
    pub fn get_file(&self) -> Option<std::rc::Rc<monoio::fs::File>> {
        self.file.clone()
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        if let Some((file, chunk, offset)) = self.take_flush_chunk() {
            let (res, buf) = file.write_all_at(chunk, offset).await;
            self.chunk_written(&res, buf, offset);
            res?;
        }
        Ok(())
    }

    pub async fn flush_rc(aof: &std::rc::Rc<std::cell::RefCell<Self>>) -> std::io::Result<()> {
        let chunk_and_off = aof.borrow_mut().take_flush_chunk();
        if let Some((file, chunk, offset)) = chunk_and_off {
            let (res, buf) = file.write_all_at(chunk, offset).await;
            aof.borrow_mut().chunk_written(&res, buf, offset);
            res?;
        }
        Ok(())
    }

    /// Makes everything appended so far durable: writes the pending buffer,
    /// waits for chunks other tasks are still writing, then fsyncs. Group
    /// commit: a caller whose data an fsync already covered returns without
    /// another one.
    pub async fn sync_durable(aof: &std::rc::Rc<std::cell::RefCell<Self>>) -> std::io::Result<()> {
        let (chunk, target, logical) = {
            let mut w = aof.borrow_mut();
            let c = w.take_flush_chunk();
            (c, w.offset, w.appended_total)
        };
        if let Some((file, chunk, offset)) = chunk {
            let (res, buf) = file.write_all_at(chunk, offset).await;
            aof.borrow_mut().chunk_written(&res, buf, offset);
            res?;
        }
        loop {
            let (durable, inflight) = {
                let w = aof.borrow();
                (w.durable, w.inflight)
            };
            if durable >= target {
                aof.borrow().mark_fsynced(logical);
                return Ok(());
            }
            if inflight == 0 {
                break;
            }
            monoio::time::sleep(std::time::Duration::from_micros(20)).await;
        }
        let file = aof.borrow().get_file();
        if let Some(file) = file {
            file.sync_data().await?;
        }
        let mut w = aof.borrow_mut();
        // A rewrite may have swapped in a new (already synced) file meanwhile.
        if w.offset >= target {
            w.durable = w.durable.max(target);
        }
        // A swapped-in file was synced too, so either way all is durable.
        w.mark_fsynced(logical);
        Ok(())
    }

    /// Hands everything appended so far to the kernel (no fsync): writes the
    /// pending buffer and waits for chunks other tasks are still writing.
    /// Group commit: returns at once if another write already covered it.
    ///
    /// Under `appendfsync everysec`/`no`, replies to a write batch wait for
    /// this, as Redis writes the AOF before replying: a crash of the process
    /// alone then loses no acknowledged write (only an OS crash can lose up
    /// to the last second). Buffered for the 5 ms flusher, acknowledged
    /// writes were lost on kill -9, and each shard's file was cut at a
    /// different point.
    pub async fn write_through(aof: &std::rc::Rc<std::cell::RefCell<Self>>) -> std::io::Result<()> {
        let (target, progress) = {
            let w = aof.borrow();
            (w.appended_total, w.progress.clone())
        };
        // Usual case: nothing in flight, so write the buffer right here with
        // a plain pwrite (a few KB into the page cache, like Redis's write;
        // an io_uring write per batch, waited for, halved write throughput).
        if aof.borrow_mut().write_pending_sync()? {
            return Ok(());
        }
        loop {
            if progress.written.load(std::sync::atomic::Ordering::Acquire) >= target {
                return Ok(());
            }
            let chunk = aof.borrow_mut().take_flush_chunk();
            match chunk {
                Some((file, chunk, offset)) => {
                    let (res, buf) = file.write_all_at(chunk, offset).await;
                    aof.borrow_mut().chunk_written(&res, buf, offset);
                    res?;
                }
                None if aof.borrow().inflight == 0 => {
                    // Nothing pending and nothing in flight (e.g. the file
                    // was swapped by a rewrite): all is written.
                    aof.borrow_mut().mark_written_if_idle();
                    return Ok(());
                }
                None => monoio::time::sleep(std::time::Duration::from_micros(20)).await,
            }
        }
    }

    /// Whether appended bytes are waiting in the buffer.
    #[inline]
    pub fn has_pending(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// Writes the pending buffer with a blocking pwrite, if no chunk is in
    /// flight (else returns false and writes nothing: the in-flight chunk,
    /// at an earlier offset, must land first for `written` to advance).
    pub fn write_pending_sync(&mut self) -> std::io::Result<bool> {
        if self.inflight > 0 {
            return Ok(false);
        }
        if !self.buffer.is_empty()
            && let Some(file) = &self.file
        {
            use std::os::fd::{AsRawFd, FromRawFd};
            use std::os::unix::fs::FileExt;
            // SAFETY: borrows the writer's open fd for one call; ManuallyDrop
            // keeps the std File from closing it.
            let f = std::mem::ManuallyDrop::new(unsafe {
                std::fs::File::from_raw_fd(file.as_raw_fd())
            });
            // On failure (e.g. ENOSPC part way) the buffer and offset stay,
            // so the next attempt rewrites the same range.
            if let Err(e) = f.write_all_at(&self.buffer, self.offset) {
                self.progress
                    .record_write(Err(e.raw_os_error().unwrap_or(libc::EIO)));
                return Err(e);
            }
            self.offset += self.buffer.len() as u64;
            self.buffer.clear();
            self.progress.record_write(Ok(()));
        }
        self.mark_written_if_idle();
        Ok(true)
    }

    pub async fn sync(&mut self) -> std::io::Result<()> {
        self.flush().await?;
        if let Some(file) = &self.file {
            file.sync_data().await?;
        }
        Ok(())
    }

    /// Where an incremental rewrite starts: what is appended from now on
    /// is the new AOF's tail ([`rewrite_shard_aof_live`]).
    fn rewrite_mark(&self) -> RewriteMark {
        RewriteMark {
            file_pos: self.offset + self.buffer.len() as u64,
            logical: self.appended_total,
            db: self.current_db,
        }
    }

    /// Points the writer at the new AOF `rewrite_shard_aof_live` just
    /// renamed into place, `new_size` bytes long, holding everything
    /// appended so far (its tail ends in the DB this writer last selected,
    /// so that is kept). Synchronous, with nothing pending or in flight.
    fn swap_after_live_rewrite(&mut self, new_size: u64) -> std::io::Result<()> {
        debug_assert!(self.buffer.is_empty() && self.inflight == 0);
        let std_file = std::fs::OpenOptions::new().write(true).open(&self.path)?;
        self.file = Some(std::rc::Rc::new(monoio::fs::File::from_std(std_file)?));
        self.offset = new_size;
        self.durable = new_size;
        self.progress
            .written
            .store(self.appended_total, std::sync::atomic::Ordering::Release);
        self.mark_fsynced(self.appended_total);
        Ok(())
    }

    /// Points the writer at the freshly rewritten AOF at `self.path`.
    ///
    /// Must run synchronously right after the rewrite, with no `.await` in
    /// between, so no command can run in the gap: anything appended before is
    /// part of the rewritten snapshot (the pending buffer is dropped rather
    /// than appended twice) and anything appended after goes to the new file.
    /// A flush already in flight still targets the old, replaced file, which
    /// is harmless because its commands are in the snapshot too.
    pub fn swap_after_rewrite(&mut self) -> std::io::Result<u64> {
        if self.path.as_os_str().is_empty() {
            return Ok(0);
        }
        // The rewrite just renamed the new file into place.
        let std_file = std::fs::OpenOptions::new().write(true).open(&self.path)?;
        let new_size = std_file.metadata()?.len();
        self.file = Some(std::rc::Rc::new(monoio::fs::File::from_std(std_file)?));
        self.offset = new_size;
        // The rewrite fsynced the new file before renaming it in.
        self.durable = new_size;
        self.buffer.clear();
        // What was pending is in the rewritten file.
        self.progress
            .written
            .store(self.appended_total, std::sync::atomic::Ordering::Release);
        self.mark_fsynced(self.appended_total);
        self.current_db = 0;
        Ok(new_size)
    }
}

/// Rewrites shard `shard_id`'s AOF from `db` and, if `aof` is the live
/// writer, switches it to the new file in the same synchronous step.
pub fn rewrite_and_swap_shard_aof(
    db: &mut ShardDb,
    dir: &Path,
    shard_id: usize,
    aof: Option<&std::rc::Rc<std::cell::RefCell<AofWriter>>>,
) -> std::io::Result<usize> {
    let count = rewrite_shard_aof(db, dir, shard_id)?;
    if let Some(aof) = aof {
        aof.borrow_mut().swap_after_rewrite()?;
    }
    Ok(count)
}

/// Where an incremental rewrite started in the live AOF (see
/// [`AofWriter::rewrite_mark`]).
#[derive(Clone, Copy)]
struct RewriteMark {
    /// Offset in the old file of the first byte appended after the start.
    file_pos: u64,
    /// `appended_total` at the start.
    logical: u64,
    /// The DB the old file's tail starts in.
    db: usize,
}

enum LiveRewriteError {
    Io(std::io::Error),
    /// The image could not stay point-in-time (see
    /// [`crate::shard::poison_aof_image`]): rewrite in one blocking pass.
    Fallback(String),
}

impl From<std::io::Error> for LiveRewriteError {
    fn from(e: std::io::Error) -> Self {
        LiveRewriteError::Io(e)
    }
}

/// Live rewrites that fell back to a blocking pass.
pub static LIVE_REWRITE_FALLBACKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Bytes of the old AOF's tail copied per step while catching up.
const TAIL_COPY_BYTES: u64 = 4 << 20;
/// Tail left for the final, synchronous copy.
const TAIL_SYNC_BYTES: u64 = 1 << 20;

/// Rewrites shard `shard_id`'s AOF without blocking it: an RDB image of
/// the shard as of the start ([`crate::shard::AofImage`]) is built a few
/// segments at a time between commands and written by another thread;
/// then what the live AOF received since the start is copied after it,
/// the last bit (and the switch to the new file) in one synchronous step.
/// A shard used to stop for the whole rewrite (~0.75 s per GB). Falls back
/// to that blocking pass if the image cannot stay point-in-time.
pub async fn rewrite_shard_aof_live(
    db: &std::rc::Rc<std::cell::RefCell<ShardDb>>,
    dir: &Path,
    shard_id: usize,
    aof: Option<&std::rc::Rc<std::cell::RefCell<AofWriter>>>,
) -> std::io::Result<usize> {
    let (tmp_path, target_path) = rewrite_paths(dir, shard_id);
    let res = rewrite_live_inner(db, &tmp_path, &target_path, aof).await;
    if res.is_err() {
        crate::shard::take_aof_image();
        let _ = std::fs::remove_file(&tmp_path);
    }
    match res {
        Ok(n) => Ok(n),
        Err(LiveRewriteError::Io(e)) => Err(e),
        Err(LiveRewriteError::Fallback(why)) => {
            LIVE_REWRITE_FALLBACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            crate::log_warning!(
                "[Shard {}] AOF rewrite: {}; rewriting in one blocking pass",
                shard_id,
                why
            );
            rewrite_and_swap_shard_aof(&mut db.borrow_mut(), dir, shard_id, aof)
        }
    }
}

fn rewrite_paths(dir: &Path, shard_id: usize) -> (std::path::PathBuf, std::path::PathBuf) {
    static TMP_LIVE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let tmp_id = TMP_LIVE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    (
        dir.join(format!(
            "appendonly-{}.aof.tmp.{}_live{}",
            shard_id,
            std::process::id(),
            tmp_id
        )),
        dir.join(format!("appendonly-{}.aof", shard_id)),
    )
}

async fn rewrite_live_inner(
    db: &std::rc::Rc<std::cell::RefCell<ShardDb>>,
    tmp_path: &Path,
    target_path: &Path,
    aof: Option<&std::rc::Rc<std::cell::RefCell<AofWriter>>>,
) -> Result<usize, LiveRewriteError> {
    // Start: the image and the AOF mark in one synchronous step, so the
    // image is the state right before the first byte of the tail.
    let (keys, mark) = {
        let d = db.borrow();
        let image =
            crate::shard::AofImage::start(&d).map_err(|e| std::io::Error::other(e.to_string()))?;
        crate::shard::install_aof_image(image);
        let keys: usize = d.keyspace_stats_by_db().iter().map(|&(_, k, _, _)| k).sum();
        (keys, aof.map(|w| w.borrow().rewrite_mark()))
    };
    let (tx, done_rx) = spawn_image_writer(tmp_path.to_path_buf())?;
    let gone = || std::io::Error::other("AOF rewrite writer thread failed");
    loop {
        let step = crate::shard::with_aof_image(|img| {
            img.step(&db.borrow(), crate::shard::RDB_SAVE_STEP, IMAGE_FLUSH_BYTES)
        })
        .expect("the AOF image stays installed during its rewrite");
        let (piece, done) = step.map_err(LiveRewriteError::Fallback)?;
        if let Some(piece) = piece {
            tx.send_async(piece).await.map_err(|_| gone())?;
        }
        if done {
            break;
        }
        let delay_us =
            crate::connection::RDB_KEY_SAVE_DELAY.load(std::sync::atomic::Ordering::Relaxed);
        if delay_us > 0 {
            monoio::time::sleep(std::time::Duration::from_micros(delay_us.clamp(1, 50_000))).await;
        }
        crate::mailbox::yield_now().await;
    }
    let image = crate::shard::take_aof_image().expect("installed above");
    let rest = image.finish().map_err(LiveRewriteError::Fallback)?;
    tx.send_async(rest).await.map_err(|_| gone())?;
    drop(tx);
    let file = done_rx.recv_async().await.map_err(|_| gone())??;
    match (aof, mark) {
        (Some(aof), Some(mark)) => {
            catch_up_and_swap(aof, file, mark, tmp_path, target_path).await?;
        }
        _ => {
            drop(file);
            std::fs::rename(tmp_path, target_path)?;
            let _ = sync_parent_dir(target_path);
        }
    }
    Ok(keys)
}

/// Writes an image's pieces (as they come) to `tmp`, then its CRC64, and
/// fsyncs: the CRC and the disk writes stay off the shard thread. Hands the
/// file back once the sender is dropped.
fn spawn_image_writer(
    tmp: std::path::PathBuf,
) -> std::io::Result<(
    flume::Sender<Vec<u8>>,
    flume::Receiver<std::io::Result<std::fs::File>>,
)> {
    let (tx, rx) = flume::bounded::<Vec<u8>>(2);
    let (done_tx, done_rx) = flume::bounded(1);
    std::thread::Builder::new()
        .name("aof-rewrite".into())
        .spawn(move || {
            let res = (|| {
                let mut w = ImageFileWriter::create(&tmp)?;
                for piece in rx.iter() {
                    w.write(&piece)?;
                }
                w.finish()
            })();
            let _ = done_tx.send(res);
        })?;
    Ok((tx, done_rx))
}

/// The live AOF, opened for reading through the writer's own descriptor
/// (it is write-only), so even a file unlinked or replaced under it reads
/// what the writer wrote. Falls back to the path.
fn open_for_reading(
    aof: &std::rc::Rc<std::cell::RefCell<AofWriter>>,
    path: &Path,
) -> std::io::Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    if let Some(file) = aof.borrow().get_file()
        && let Ok(f) = std::fs::File::open(format!("/proc/self/fd/{}", file.as_raw_fd()))
    {
        return Ok(f);
    }
    std::fs::File::open(path)
}

/// Appends to `file` (the new AOF, holding the image) what the live AOF
/// received since `mark`, then renames it into place and switches the
/// writer to it. All but the last `TAIL_SYNC_BYTES` are copied between
/// commands; the rest, with the rename and the switch, in one synchronous
/// step after the writer's pending bytes are written, so no write is lost
/// or doubled.
async fn catch_up_and_swap(
    aof: &std::rc::Rc<std::cell::RefCell<AofWriter>>,
    file: std::fs::File,
    mark: RewriteMark,
    tmp_path: &Path,
    target_path: &Path,
) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    let old = open_for_reading(aof, target_path)?;
    let mut out_pos = file.metadata()?.len();
    if mark.db != 0 {
        let db = mark.db.to_string();
        let select = format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db.len(), db);
        file.write_all_at(select.as_bytes(), out_pos)?;
        out_pos += select.len() as u64;
    }
    let mut copied = mark.file_pos;
    let mut chunk = vec![0u8; TAIL_COPY_BYTES as usize];
    let mut copy =
        |from: u64, to: u64, copied: &mut u64, out_pos: &mut u64| -> std::io::Result<()> {
            let mut at = from;
            while at < to {
                let n = (to - at).min(TAIL_COPY_BYTES) as usize;
                old.read_exact_at(&mut chunk[..n], at)?;
                file.write_all_at(&chunk[..n], *out_pos)?;
                at += n as u64;
                *out_pos += n as u64;
            }
            *copied = to;
            Ok(())
        };
    // Catch up between commands while the tail keeps growing.
    loop {
        let (written_end, appended_end) = {
            let w = aof.borrow();
            let written = w
                .progress
                .written
                .load(std::sync::atomic::Ordering::Acquire);
            (
                mark.file_pos + written.saturating_sub(mark.logical),
                mark.file_pos + w.appended_total.saturating_sub(mark.logical),
            )
        };
        if appended_end.saturating_sub(copied) <= TAIL_SYNC_BYTES {
            break;
        }
        if written_end > copied {
            let to = written_end.min(copied + TAIL_COPY_BYTES);
            copy(copied, to, &mut copied, &mut out_pos)?;
            crate::mailbox::yield_now().await;
        } else {
            monoio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
    // The last bit and the switch, with no command in between.
    loop {
        {
            let mut w = aof.borrow_mut();
            if w.inflight == 0 {
                w.write_pending_sync()?;
                let end = w.offset;
                copy(copied, end, &mut copied, &mut out_pos)?;
                file.sync_data()?;
                std::fs::rename(tmp_path, target_path)?;
                let _ = sync_parent_dir(target_path);
                return w.swap_after_live_rewrite(out_pos);
            }
        }
        monoio::time::sleep(std::time::Duration::from_micros(20)).await;
    }
}

static UNPROPAGATED_CHANGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Called when a command changed data but `command_to_resp` cannot encode
/// it, so the change reaches neither the AOF nor the replicas. Counts it
/// (INFO persistence `unpropagated_changes`) and logs a warning the first
/// time each command does it.
pub fn note_unpropagated(cmd: &Command) {
    static WARNED: std::sync::OnceLock<
        parking_lot::Mutex<std::collections::HashSet<&'static str>>,
    > = std::sync::OnceLock::new();
    UNPROPAGATED_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let name = crate::connection::get_cmd_name(cmd);
    if WARNED.get_or_init(Default::default).lock().insert(name) {
        crate::log_warning!(
            "{} changed data but cannot be propagated to replicas/AOF: replicas and the AOF will miss this change",
            name
        );
    }
}

/// Changes that could not be propagated to replicas or the AOF.
pub fn unpropagated_changes() -> u64 {
    UNPROPAGATED_CHANGES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Total size of the per-shard `appendonly-{i}.aof` files in `dir` (what
/// the kernel has; the writer's unflushed buffer is at most ~1 s of
/// writes). Drives `auto-aof-rewrite-*` and INFO `aof_current_size`.
pub fn aof_files_size(dir: &Path, num_shards: usize) -> u64 {
    (0..num_shards)
        .filter_map(|i| std::fs::metadata(dir.join(format!("appendonly-{}.aof", i))).ok())
        .map(|m| m.len())
        .sum()
}

/// Redis default `auto-aof-rewrite-percentage`.
pub const DEFAULT_AUTO_AOF_REWRITE_PERCENTAGE: u64 = 100;
/// Redis default `auto-aof-rewrite-min-size` (64mb).
pub const DEFAULT_AUTO_AOF_REWRITE_MIN_SIZE: u64 = 64 * 1024 * 1024;

/// Live `auto-aof-rewrite-percentage` / `auto-aof-rewrite-min-size` of
/// the server on one port.
pub struct AutoRewriteConfig {
    pub percentage: std::sync::atomic::AtomicU64,
    pub min_size: std::sync::atomic::AtomicU64,
}

type AutoRewriteByPort = std::collections::HashMap<u16, std::sync::Arc<AutoRewriteConfig>>;
static AUTO_REWRITE: parking_lot::Mutex<Option<AutoRewriteByPort>> = parking_lot::Mutex::new(None);

pub fn auto_rewrite_config(port: u16) -> std::sync::Arc<AutoRewriteConfig> {
    AUTO_REWRITE
        .lock()
        .get_or_insert_with(Default::default)
        .entry(port)
        .or_insert_with(|| {
            std::sync::Arc::new(AutoRewriteConfig {
                percentage: std::sync::atomic::AtomicU64::new(DEFAULT_AUTO_AOF_REWRITE_PERCENTAGE),
                min_size: std::sync::atomic::AtomicU64::new(DEFAULT_AUTO_AOF_REWRITE_MIN_SIZE),
            })
        })
        .clone()
}

/// Redis `serverCron` rule for an automatic AOF rewrite: the AOF is larger
/// than `min_size` and has grown by at least `percentage` percent over its
/// size after the last rewrite (`base`, 1 if unknown). `percentage == 0`
/// disables it. Returns the growth in percent when a rewrite is due.
pub fn auto_rewrite_due(percentage: u64, min_size: u64, current: u64, base: u64) -> Option<u64> {
    if percentage == 0 || current <= min_size {
        return None;
    }
    let growth = (u128::from(current) * 100 / u128::from(base.max(1))).saturating_sub(100);
    let growth = u64::try_from(growth).unwrap_or(u64::MAX);
    (growth >= percentage).then_some(growth)
}

/// Appends `HDEL key field...` (also how HGETDEL is logged).
fn encode_hdel(buf: &mut Vec<u8>, key: &[u8], fields: &[bytes::Bytes]) {
    buf.extend_from_slice(
        format!("*{}\r\n$4\r\nHDEL\r\n${}\r\n", 2 + fields.len(), key.len()).as_bytes(),
    );
    buf.extend_from_slice(key);
    buf.extend_from_slice(b"\r\n");
    for f in fields {
        buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
        buf.extend_from_slice(f);
        buf.extend_from_slice(b"\r\n");
    }
}

/// Appends `SET key value` for a SET without TTL or KEEPTTL to `buf`.
#[inline]
fn encode_plain_set(cmd: &Command, buf: &mut Vec<u8>) {
    if let Command::Set { key, value, .. } = cmd {
        buf.reserve(40 + key.len() + value.len());
        buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
        push_bulk(buf, key);
        push_bulk(buf, value);
    }
}

/// Appends `data` as a RESP bulk string, without `format!`.
#[inline]
fn push_bulk(buf: &mut Vec<u8>, data: &[u8]) {
    push_len(buf, b"$", data.len());
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

pub fn command_to_resp(cmd: &Command) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    encode_command(cmd, &mut buf).then_some(buf)
}

/// Appends `cmd` encoded as RESP (as [`command_to_resp`] returns it) to
/// `buf`; returns false, appending nothing, if it has no encoding. Lets the
/// AOF encode straight into its buffer, without a temporary one per command.
pub fn encode_command(cmd: &Command, buf: &mut Vec<u8>) -> bool {
    let start = buf.len();
    match encode_command_inner(cmd, buf) {
        Some(own) => {
            buf.extend_from_slice(&own);
            true
        }
        None => {
            buf.truncate(start);
            false
        }
    }
}

/// Appends `n` in decimal after `prefix`, then CRLF (RESP lengths), without
/// `format!`.
#[inline]
fn push_len(buf: &mut Vec<u8>, prefix: &[u8], n: usize) {
    let mut digits = [0u8; 20];
    let mut i = digits.len();
    let mut n = n;
    loop {
        i -= 1;
        digits[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    buf.extend_from_slice(prefix);
    buf.extend_from_slice(&digits[i..]);
    buf.extend_from_slice(b"\r\n");
}

/// Body of [`encode_command`]: either writes the encoding into `buf` and
/// returns `Some` of an empty `Vec` (which does not allocate), or returns a
/// separately built encoding for the caller to append, or `None`.
fn encode_command_inner(cmd: &Command, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    use std::io::Write;
    match cmd {
        Command::Set {
            key,
            value,
            expire_in,
            keepttl,
            ..
        } => {
            if *keepttl {
                let _ = write!(buf, "*4\r\n$3\r\nSET\r\n${}\r\n", key.len());
                buf.extend_from_slice(key);
                push_len(buf, b"\r\n$", value.len());
                buf.extend_from_slice(value);
                buf.extend_from_slice(b"\r\n$7\r\nKEEPTTL\r\n");
            } else if let Some(dur) = expire_in {
                let ms = dur.as_millis().max(1);
                let abs_ms = if ms < 1_000_000_000_000 {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    now_ms + ms
                } else {
                    ms
                };
                let ms_str = abs_ms.to_string();
                let _ = write!(buf, "*5\r\n$3\r\nSET\r\n${}\r\n", key.len());
                buf.extend_from_slice(key);
                push_len(buf, b"\r\n$", value.len());
                buf.extend_from_slice(value);
                let _ = write!(buf, "\r\n$4\r\nPXAT\r\n${}\r\n{}\r\n", ms_str.len(), ms_str);
            } else {
                encode_plain_set(cmd, buf);
            }
            Some(Vec::new())
        }
        Command::Mset(pairs) => {
            let _ = write!(buf, "*{}\r\n$4\r\nMSET\r\n", 1 + pairs.len() * 2);
            for (k, v) in pairs {
                push_len(buf, b"$", k.len());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
                push_len(buf, b"$", v.len());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Msetex {
            pairs,
            condition,
            expiry,
        } => {
            let mut num_args = 2 + pairs.len() * 2;
            if *condition != crate::resp::MsetexCondition::None {
                num_args += 1;
            }
            match expiry {
                crate::resp::MsetexExpiry::None => {}
                crate::resp::MsetexExpiry::KeepTtl => num_args += 1,
                crate::resp::MsetexExpiry::ExpireIn(_) => num_args += 2,
            }
            let _ = write!(buf, "*{}\r\n$6\r\nMSETEX\r\n", num_args);
            let numkeys_str = pairs.len().to_string();
            let _ = write!(buf, "${}\r\n{}\r\n", numkeys_str.len(), numkeys_str);
            for (k, v) in pairs {
                push_len(buf, b"$", k.len());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
                push_len(buf, b"$", v.len());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            match condition {
                crate::resp::MsetexCondition::Nx => buf.extend_from_slice(b"$2\r\nNX\r\n"),
                crate::resp::MsetexCondition::Xx => buf.extend_from_slice(b"$2\r\nXX\r\n"),
                crate::resp::MsetexCondition::None => {}
            }
            match expiry {
                crate::resp::MsetexExpiry::KeepTtl => buf.extend_from_slice(b"$7\r\nKEEPTTL\r\n"),
                crate::resp::MsetexExpiry::ExpireIn(d) => {
                    buf.extend_from_slice(b"$4\r\nPXAT\r\n");
                    let ms_str = unix_ms_after(*d).to_string();
                    let _ = write!(buf, "${}\r\n{}\r\n", ms_str.len(), ms_str);
                }
                crate::resp::MsetexExpiry::None => {}
            }
            Some(Vec::new())
        }
        Command::Del(keys) => {
            let _ = write!(buf, "*{}\r\n$3\r\nDEL\r\n", 1 + keys.len());
            for k in keys {
                push_len(buf, b"$", k.len());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Unlink(keys) => {
            let _ = write!(buf, "*{}\r\n$6\r\nUNLINK\r\n", 1 + keys.len());
            for k in keys {
                push_len(buf, b"$", k.len());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::IncrBy(key, delta, name) => {
            // Like Redis, the command as sent: INCR / DECR / INCRBY / DECRBY.
            let (verb, arg): (&str, Option<i64>) = match name {
                crate::resp::IncrName::Incr if *delta == 1 => ("INCR", None),
                crate::resp::IncrName::Decr if *delta == -1 => ("DECR", None),
                crate::resp::IncrName::DecrBy if *delta != i64::MIN => ("DECRBY", Some(-*delta)),
                _ => ("INCRBY", Some(*delta)),
            };
            let _ = write!(
                buf,
                "*{}\r\n${}\r\n{}\r\n${}\r\n",
                if arg.is_some() { 3 } else { 2 },
                verb.len(),
                verb,
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            if let Some(d) = arg {
                let d_str = d.to_string();
                let _ = write!(buf, "${}\r\n{}\r\n", d_str.len(), d_str);
            }
            Some(Vec::new())
        }
        Command::Expire { key, duration, .. } => {
            let ms_str = unix_ms_after(*duration).to_string();
            let _ = write!(buf, "*3\r\n$9\r\nPEXPIREAT\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", ms_str.len(), ms_str);
            Some(Vec::new())
        }
        Command::Persist(key) => {
            let _ = write!(buf, "*2\r\n$7\r\nPERSIST\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Hset { key, fields } | Command::Hmset { key, fields } => {
            let _ = write!(
                buf,
                "*{}\r\n$4\r\nHSET\r\n${}\r\n",
                2 + fields.len() * 2,
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for (f, v) in fields {
                push_len(buf, b"$", f.len());
                buf.extend_from_slice(f);
                buf.extend_from_slice(b"\r\n");
                push_len(buf, b"$", v.len());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Hsetnx { key, field, value } => {
            let _ = write!(buf, "*4\r\n$6\r\nHSETNX\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            push_len(buf, b"$", field.len());
            buf.extend_from_slice(field);
            buf.extend_from_slice(b"\r\n");
            push_len(buf, b"$", value.len());
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Hdel { key, fields } => {
            encode_hdel(buf, key, fields);
            Some(Vec::new())
        }
        Command::Hgetdel { key, fields } => {
            encode_hdel(buf, key, fields);
            Some(Vec::new())
        }
        Command::Lpush { key, values } => {
            let _ = write!(
                buf,
                "*{}\r\n$5\r\nLPUSH\r\n${}\r\n",
                2 + values.len(),
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for v in values {
                push_len(buf, b"$", v.len());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Rpush { key, values } => {
            let _ = write!(
                buf,
                "*{}\r\n$5\r\nRPUSH\r\n${}\r\n",
                2 + values.len(),
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for v in values {
                push_len(buf, b"$", v.len());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Lpop { key, count } => {
            if let Some(c) = count {
                let c_str = c.to_string();
                let _ = write!(buf, "*3\r\n$4\r\nLPOP\r\n${}\r\n", key.len());
                buf.extend_from_slice(key);
                let _ = write!(buf, "\r\n${}\r\n{}\r\n", c_str.len(), c_str);
            } else {
                let _ = write!(buf, "*2\r\n$4\r\nLPOP\r\n${}\r\n", key.len());
                buf.extend_from_slice(key);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Rpop { key, count } => {
            if let Some(c) = count {
                let c_str = c.to_string();
                let _ = write!(buf, "*3\r\n$4\r\nRPOP\r\n${}\r\n", key.len());
                buf.extend_from_slice(key);
                let _ = write!(buf, "\r\n${}\r\n{}\r\n", c_str.len(), c_str);
            } else {
                let _ = write!(buf, "*2\r\n$4\r\nRPOP\r\n${}\r\n", key.len());
                buf.extend_from_slice(key);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Sadd { key, members } => {
            let _ = write!(
                buf,
                "*{}\r\n$4\r\nSADD\r\n${}\r\n",
                2 + members.len(),
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for m in members {
                push_len(buf, b"$", m.len());
                buf.extend_from_slice(m);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Srem { key, members } => {
            let _ = write!(
                buf,
                "*{}\r\n$4\r\nSREM\r\n${}\r\n",
                2 + members.len(),
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for m in members {
                push_len(buf, b"$", m.len());
                buf.extend_from_slice(m);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Spop { key, count } => {
            if let Some(c) = count {
                let c_str = c.to_string();
                let _ = write!(buf, "*3\r\n$4\r\nSPOP\r\n${}\r\n", key.len());
                buf.extend_from_slice(key);
                let _ = write!(buf, "\r\n${}\r\n{}\r\n", c_str.len(), c_str);
            } else {
                let _ = write!(buf, "*2\r\n$4\r\nSPOP\r\n${}\r\n", key.len());
                buf.extend_from_slice(key);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Zadd {
            key,
            elements,
            flags,
        } => {
            let mut num_args = 2 + elements.len() * 2;
            if flags.nx {
                num_args += 1;
            }
            if flags.xx {
                num_args += 1;
            }
            if flags.gt {
                num_args += 1;
            }
            if flags.lt {
                num_args += 1;
            }
            if flags.ch {
                num_args += 1;
            }
            if flags.incr {
                num_args += 1;
            }
            let _ = write!(buf, "*{}\r\n$4\r\nZADD\r\n${}\r\n", num_args, key.len());
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            if flags.nx {
                buf.extend_from_slice(b"$2\r\nNX\r\n");
            }
            if flags.xx {
                buf.extend_from_slice(b"$2\r\nXX\r\n");
            }
            if flags.gt {
                buf.extend_from_slice(b"$2\r\nGT\r\n");
            }
            if flags.lt {
                buf.extend_from_slice(b"$2\r\nLT\r\n");
            }
            if flags.ch {
                buf.extend_from_slice(b"$2\r\nCH\r\n");
            }
            if flags.incr {
                buf.extend_from_slice(b"$4\r\nINCR\r\n");
            }
            for (s, m) in elements {
                let s_str = s.to_string();
                let _ = write!(buf, "${}\r\n{}\r\n", s_str.len(), s_str);
                push_len(buf, b"$", m.len());
                buf.extend_from_slice(m);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Zrem { key, members } => {
            let _ = write!(
                buf,
                "*{}\r\n$4\r\nZREM\r\n${}\r\n",
                2 + members.len(),
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for m in members {
                push_len(buf, b"$", m.len());
                buf.extend_from_slice(m);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Zincrby { key, delta, member } => {
            let d_str = delta.to_string();
            let _ = write!(buf, "*4\r\n$7\r\nZINCRBY\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", d_str.len(), d_str);
            push_len(buf, b"$", member.len());
            buf.extend_from_slice(member);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Setnx { key, value } => {
            let _ = write!(buf, "*3\r\n$5\r\nSETNX\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", value.len());
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Getset { key, value } => {
            let _ = write!(buf, "*3\r\n$3\r\nSET\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", value.len());
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Getdel(key) => {
            let _ = write!(buf, "*2\r\n$3\r\nDEL\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Append { key, value } => {
            let _ = write!(buf, "*3\r\n$6\r\nAPPEND\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", value.len());
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Rename { key, newkey, .. } => {
            let _ = write!(buf, "*3\r\n$6\r\nRENAME\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", newkey.len());
            buf.extend_from_slice(newkey);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Copy {
            source,
            destination,
            destination_db,
            replace,
        } => {
            let mut count = 3;
            if destination_db.is_some() {
                count += 2;
            }
            if *replace {
                count += 1;
            }
            let _ = write!(buf, "*{}\r\n$4\r\nCOPY\r\n${}\r\n", count, source.len());
            buf.extend_from_slice(source);
            push_len(buf, b"\r\n$", destination.len());
            buf.extend_from_slice(destination);
            if let Some(db_id) = destination_db {
                let _ = write!(
                    buf,
                    "\r\n$2\r\nDB\r\n${}\r\n{}",
                    db_id.to_string().len(),
                    db_id
                );
            }
            if *replace {
                buf.extend_from_slice(b"\r\n$7\r\nREPLACE");
            }
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Flushdb => {
            buf.extend_from_slice(b"*1\r\n$7\r\nFLUSHDB\r\n");
            Some(Vec::new())
        }
        Command::Flushall => {
            buf.extend_from_slice(b"*1\r\n$8\r\nFLUSHALL\r\n");
            Some(Vec::new())
        }
        Command::Move { key, db } => {
            let s = db.to_string();
            let _ = write!(buf, "*3\r\n$4\r\nMOVE\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", s.len(), s);
            Some(Vec::new())
        }
        Command::Swapdb { db1, db2 } => {
            let s1 = db1.to_string();
            let s2 = db2.to_string();
            let _ = write!(
                buf,
                "*3\r\n$6\r\nSWAPDB\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                s1.len(),
                s1,
                s2.len(),
                s2
            );
            Some(Vec::new())
        }
        Command::Msetnx(pairs) => {
            let _ = write!(buf, "*{}\r\n$4\r\nMSET\r\n", 1 + pairs.len() * 2);
            for (k, v) in pairs {
                push_len(buf, b"$", k.len());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
                push_len(buf, b"$", v.len());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Setbit { key, offset, value } => {
            let off_str = offset.to_string();
            let val_str = value.to_string();
            let _ = write!(buf, "*4\r\n$6\r\nSETBIT\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", off_str.len(), off_str);
            let _ = write!(buf, "${}\r\n{}\r\n", val_str.len(), val_str);
            Some(Vec::new())
        }
        Command::Bitfield { key, ops, readonly } => {
            if *readonly {
                return None;
            }
            let mut parts: Vec<Vec<u8>> = Vec::new();
            parts.push(b"BITFIELD".to_vec());
            parts.push(key.to_vec());
            let mut last_overflow = crate::resp::BitfieldOverflow::Wrap;
            for op in ops {
                if op.overflow != last_overflow {
                    parts.push(b"OVERFLOW".to_vec());
                    match op.overflow {
                        crate::resp::BitfieldOverflow::Wrap => parts.push(b"WRAP".to_vec()),
                        crate::resp::BitfieldOverflow::Sat => parts.push(b"SAT".to_vec()),
                        crate::resp::BitfieldOverflow::Fail => parts.push(b"FAIL".to_vec()),
                    }
                    last_overflow = op.overflow;
                }
                let type_str = format!("{}{}", if op.sign { "i" } else { "u" }, op.bits);
                let off_str = op.offset.to_string();
                match op.op_type {
                    crate::resp::BitfieldOpType::Get => {
                        parts.push(b"GET".to_vec());
                        parts.push(type_str.into_bytes());
                        parts.push(off_str.into_bytes());
                    }
                    crate::resp::BitfieldOpType::Set(val) => {
                        parts.push(b"SET".to_vec());
                        parts.push(type_str.into_bytes());
                        parts.push(off_str.into_bytes());
                        parts.push(val.to_string().into_bytes());
                    }
                    crate::resp::BitfieldOpType::Incrby(incr) => {
                        parts.push(b"INCRBY".to_vec());
                        parts.push(type_str.into_bytes());
                        parts.push(off_str.into_bytes());
                        parts.push(incr.to_string().into_bytes());
                    }
                }
            }
            push_len(buf, b"*", parts.len());
            for p in parts {
                push_len(buf, b"$", p.len());
                buf.extend_from_slice(&p);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Bitop {
            op,
            destkey,
            srckeys,
        } => {
            let _ = write!(
                buf,
                "*{}\r\n$5\r\nBITOP\r\n${}\r\n{}\r\n${}\r\n",
                3 + srckeys.len(),
                op.len(),
                op,
                destkey.len()
            );
            buf.extend_from_slice(destkey);
            buf.extend_from_slice(b"\r\n");
            for k in srckeys {
                push_len(buf, b"$", k.len());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Pfadd { key, elements } => {
            let _ = write!(
                buf,
                "*{}\r\n$5\r\nPFADD\r\n${}\r\n",
                2 + elements.len(),
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for e in elements {
                push_len(buf, b"$", e.len());
                buf.extend_from_slice(e);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Pfmerge { destkey, srckeys } => {
            let _ = write!(
                buf,
                "*{}\r\n$7\r\nPFMERGE\r\n${}\r\n",
                2 + srckeys.len(),
                destkey.len()
            );
            buf.extend_from_slice(destkey);
            buf.extend_from_slice(b"\r\n");
            for k in srckeys {
                push_len(buf, b"$", k.len());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Restore {
            key,
            ttl_ms,
            serialized,
            replace,
            absttl,
            ..
        } => {
            let mut num_args = 4;
            if *replace {
                num_args += 1;
            }
            if *absttl {
                num_args += 1;
            }
            let ttl_str = ttl_ms.to_string();
            let _ = write!(buf, "*{}\r\n$7\r\nRESTORE\r\n${}\r\n", num_args, key.len());
            buf.extend_from_slice(key);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n",
                ttl_str.len(),
                ttl_str,
                serialized.len()
            );
            buf.extend_from_slice(serialized);
            buf.extend_from_slice(b"\r\n");
            if *replace {
                buf.extend_from_slice(b"$7\r\nREPLACE\r\n");
            }
            if *absttl {
                buf.extend_from_slice(b"$6\r\nABSTTL\r\n");
            }
            Some(Vec::new())
        }
        // XADD and XTRIM log their trim as given, which their handlers
        // first make exact (see `stream_exact_trim_args`), and XADD logs the
        // ID it generated. IDMP is never logged here: XADD with IDMP logs
        // XIDMPRECORD after the entry instead.
        Command::Xadd {
            key,
            nomkstream,
            maxlen,
            minid,
            approx,
            trim_strategy,
            idmp: _,
            id,
            fields,
            limit,
        } => {
            let mut args: Vec<Vec<u8>> = vec![b"XADD".to_vec(), key.to_vec()];
            if *nomkstream {
                args.push(b"NOMKSTREAM".to_vec());
            }
            push_stream_trim_strategy(&mut args, *trim_strategy);
            push_stream_trim_args(&mut args, *maxlen, *minid, *approx, *limit);
            let id_str = match id {
                crate::table::StreamAddId::Explicit(sid) => sid.to_string(),
                crate::table::StreamAddId::AutoSeq(ms) => format!("{}-*", ms),
                crate::table::StreamAddId::Auto => "*".to_string(),
            };
            args.push(id_str.into_bytes());
            for (k, v) in fields {
                args.push(k.to_vec());
                args.push(v.to_vec());
            }
            Some(resp_argv(&args))
        }
        Command::Xdel { key, ids } => {
            let _ = write!(
                buf,
                "*{}\r\n$4\r\nXDEL\r\n${}\r\n",
                2 + ids.len(),
                key.len()
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for id in ids {
                let s = id.to_string();
                let _ = write!(buf, "${}\r\n{}\r\n", s.len(), s);
            }
            Some(Vec::new())
        }
        Command::Xidmprecord {
            key,
            pid,
            iid,
            id_raw,
        } => {
            let _ = write!(buf, "*5\r\n$11\r\nXIDMPRECORD\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", pid.len());
            buf.extend_from_slice(pid);
            push_len(buf, b"\r\n$", iid.len());
            buf.extend_from_slice(iid);
            push_len(buf, b"\r\n$", id_raw.len());
            buf.extend_from_slice(id_raw);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Xtrim {
            key,
            maxlen,
            minid,
            approx,
            trim_strategy,
            limit,
        } => {
            let mut args: Vec<Vec<u8>> = vec![b"XTRIM".to_vec(), key.to_vec()];
            push_stream_trim_args(&mut args, *maxlen, *minid, *approx, *limit);
            push_stream_trim_strategy(&mut args, *trim_strategy);
            Some(resp_argv(&args))
        }
        Command::Xcfgset {
            key,
            duration,
            maxsize,
        } => {
            let mut args: Vec<Vec<u8>> = vec![b"XCFGSET".to_vec(), key.to_vec()];
            if let Some(d) = duration {
                args.push(b"IDMP-DURATION".to_vec());
                args.push(d.to_string().into_bytes());
            }
            if let Some(m) = maxsize {
                args.push(b"IDMP-MAXSIZE".to_vec());
                args.push(m.to_string().into_bytes());
            }
            Some(resp_argv(&args))
        }
        // XDELEX and XACKDEL are deterministic: they are logged as given.
        Command::Xdelex { key, strategy, ids } => {
            let mut args: Vec<Vec<u8>> = vec![b"XDELEX".to_vec(), key.to_vec()];
            push_stream_trim_strategy(&mut args, *strategy);
            push_stream_ids(&mut args, ids);
            Some(resp_argv(&args))
        }
        Command::Xackdel {
            key,
            group,
            strategy,
            ids,
        } => {
            let mut args: Vec<Vec<u8>> = vec![b"XACKDEL".to_vec(), key.to_vec(), group.to_vec()];
            push_stream_trim_strategy(&mut args, *strategy);
            push_stream_ids(&mut args, ids);
            Some(resp_argv(&args))
        }
        Command::XgroupCreate {
            key,
            group,
            id,
            mkstream,
            entries_read,
        } => {
            let id_str = id.to_string();
            let mut num_args = 5;
            if *mkstream {
                num_args += 1;
            }
            if entries_read.is_some() {
                num_args += 2;
            }
            let _ = write!(
                buf,
                "*{}\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n",
                num_args,
                key.len()
            );
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", group.len());
            buf.extend_from_slice(group);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", id_str.len(), id_str);
            if *mkstream {
                buf.extend_from_slice(b"$8\r\nMKSTREAM\r\n");
            }
            if let Some(er) = entries_read {
                let er_str = er.to_string();
                buf.extend_from_slice(b"$11\r\nENTRIESREAD\r\n");
                let _ = write!(buf, "${}\r\n{}\r\n", er_str.len(), er_str);
            }
            Some(Vec::new())
        }
        Command::XgroupDestroy { key, group } => {
            let _ = write!(
                buf,
                "*4\r\n$6\r\nXGROUP\r\n$7\r\nDESTROY\r\n${}\r\n",
                key.len()
            );
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", group.len());
            buf.extend_from_slice(group);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::XgroupSetId {
            key,
            group,
            id,
            entries_read,
        } => {
            let mut num_args = 5;
            if entries_read.is_some() {
                num_args += 2;
            }
            let _ = write!(
                buf,
                "*{}\r\n$6\r\nXGROUP\r\n$5\r\nSETID\r\n${}\r\n",
                num_args,
                key.len()
            );
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", group.len());
            buf.extend_from_slice(group);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", id.len(), id);
            if let Some(er) = entries_read {
                let er_str = er.to_string();
                let _ = write!(
                    buf,
                    "$11\r\nENTRIESREAD\r\n${}\r\n{}\r\n",
                    er_str.len(),
                    er_str
                );
            }
            Some(Vec::new())
        }
        Command::Xack { key, group, ids } => {
            let _ = write!(
                buf,
                "*{}\r\n$4\r\nXACK\r\n${}\r\n",
                3 + ids.len(),
                key.len()
            );
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", group.len());
            buf.extend_from_slice(group);
            buf.extend_from_slice(b"\r\n");
            for id in ids {
                let s = id.to_string();
                let _ = write!(buf, "${}\r\n{}\r\n", s.len(), s);
            }
            Some(Vec::new())
        }
        Command::Xnack {
            key,
            group,
            mode,
            ids,
            retrycount,
            force,
        } => {
            let mut num_args = 3 + 1 + 2 + ids.len();
            if retrycount.is_some() {
                num_args += 2;
            }
            if *force {
                num_args += 1;
            }
            let _ = write!(buf, "*{}\r\n$5\r\nXNACK\r\n${}\r\n", num_args, key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", group.len());
            buf.extend_from_slice(group);
            buf.extend_from_slice(b"\r\n");
            let mode_str = match mode {
                crate::resp::XnackMode::Silent => "SILENT",
                crate::resp::XnackMode::Fail => "FAIL",
                crate::resp::XnackMode::Fatal => "FATAL",
            };
            let _ = write!(buf, "${}\r\n{}\r\n", mode_str.len(), mode_str);
            buf.extend_from_slice(b"$3\r\nIDS\r\n");
            let numids_s = ids.len().to_string();
            let _ = write!(buf, "${}\r\n{}\r\n", numids_s.len(), numids_s);
            for id in ids {
                let id_s = id.to_string();
                let _ = write!(buf, "${}\r\n{}\r\n", id_s.len(), id_s);
            }
            if let Some(rc) = retrycount {
                let rc_s = rc.to_string();
                buf.extend_from_slice(b"$10\r\nRETRYCOUNT\r\n");
                let _ = write!(buf, "${}\r\n{}\r\n", rc_s.len(), rc_s);
            }
            if *force {
                buf.extend_from_slice(b"$5\r\nFORCE\r\n");
            }
            Some(Vec::new())
        }
        Command::Hincrby {
            key,
            field,
            increment,
        } => {
            let s = increment.to_string();
            let _ = write!(buf, "*4\r\n$7\r\nHINCRBY\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", field.len());
            buf.extend_from_slice(field);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", s.len(), s);
            Some(Vec::new())
        }
        Command::Hincrbyfloat {
            key,
            field,
            increment,
        } => {
            let s = increment.to_string();
            let _ = write!(buf, "*4\r\n$12\r\nHINCRBYFLOAT\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", field.len());
            buf.extend_from_slice(field);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", s.len(), s);
            Some(Vec::new())
        }
        Command::Smove {
            source,
            destination,
            member,
        } => {
            let _ = write!(buf, "*4\r\n$5\r\nSMOVE\r\n${}\r\n", source.len());
            buf.extend_from_slice(source);
            push_len(buf, b"\r\n$", destination.len());
            buf.extend_from_slice(destination);
            push_len(buf, b"\r\n$", member.len());
            buf.extend_from_slice(member);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Zremrangebyrank { key, start, stop } => {
            let st = start.to_string();
            let sp = stop.to_string();
            let _ = write!(buf, "*4\r\n$16\r\nZREMRANGEBYRANK\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                st.len(),
                st,
                sp.len(),
                sp
            );
            Some(Vec::new())
        }
        Command::Zremrangebyscore {
            key,
            min_score,
            min_inc,
            max_score,
            max_inc,
        } => {
            let min_s = if *min_inc {
                min_score.to_string()
            } else {
                format!("({}", min_score)
            };
            let max_s = if *max_inc {
                max_score.to_string()
            } else {
                format!("({}", max_score)
            };
            let _ = write!(buf, "*4\r\n$17\r\nZREMRANGEBYSCORE\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                min_s.len(),
                min_s,
                max_s.len(),
                max_s
            );
            Some(Vec::new())
        }
        Command::Zremrangebylex { key, min, max } => {
            let min_s = match min {
                crate::table::LexBound::UnboundedMin => "-".to_string(),
                crate::table::LexBound::UnboundedMax => "+".to_string(),
                crate::table::LexBound::Inclusive(b) => format!("[{}", String::from_utf8_lossy(b)),
                crate::table::LexBound::Exclusive(b) => format!("({}", String::from_utf8_lossy(b)),
            };
            let max_s = match max {
                crate::table::LexBound::UnboundedMin => "-".to_string(),
                crate::table::LexBound::UnboundedMax => "+".to_string(),
                crate::table::LexBound::Inclusive(b) => format!("[{}", String::from_utf8_lossy(b)),
                crate::table::LexBound::Exclusive(b) => format!("({}", String::from_utf8_lossy(b)),
            };
            let _ = write!(buf, "*4\r\n$15\r\nZREMRANGEBYLEX\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                min_s.len(),
                min_s,
                max_s.len(),
                max_s
            );
            Some(Vec::new())
        }
        Command::Ltrim { key, start, stop } => {
            let st = start.to_string();
            let sp = stop.to_string();
            let _ = write!(buf, "*4\r\n$5\r\nLTRIM\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                st.len(),
                st,
                sp.len(),
                sp
            );
            Some(Vec::new())
        }
        Command::Lset {
            key,
            index,
            element,
        } => {
            let idx_s = index.to_string();
            let _ = write!(buf, "*4\r\n$4\r\nLSET\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n",
                idx_s.len(),
                idx_s,
                element.len()
            );
            buf.extend_from_slice(element);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Lrem {
            key,
            count,
            element,
        } => {
            let cnt_s = count.to_string();
            let _ = write!(buf, "*4\r\n$4\r\nLREM\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n",
                cnt_s.len(),
                cnt_s,
                element.len()
            );
            buf.extend_from_slice(element);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Linsert {
            key,
            before,
            pivot,
            element,
        } => {
            let dir = if *before { "BEFORE" } else { "AFTER" };
            let _ = write!(buf, "*5\r\n$7\r\nLINSERT\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n${}\r\n", dir.len(), dir, pivot.len());
            buf.extend_from_slice(pivot);
            push_len(buf, b"\r\n$", element.len());
            buf.extend_from_slice(element);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Lmove {
            source,
            destination,
            where_from,
            where_to,
            is_rpoplpush,
        } => {
            if *is_rpoplpush {
                let _ = write!(buf, "*3\r\n$9\r\nRPOPLPUSH\r\n${}\r\n", source.len());
                buf.extend_from_slice(source);
                push_len(buf, b"\r\n$", destination.len());
                buf.extend_from_slice(destination);
                buf.extend_from_slice(b"\r\n");
                return Some(Vec::new());
            }
            let from_s = match where_from {
                crate::table::ListDirection::Left => "LEFT",
                crate::table::ListDirection::Right => "RIGHT",
            };
            let to_s = match where_to {
                crate::table::ListDirection::Left => "LEFT",
                crate::table::ListDirection::Right => "RIGHT",
            };
            let _ = write!(buf, "*5\r\n$5\r\nLMOVE\r\n${}\r\n", source.len());
            buf.extend_from_slice(source);
            push_len(buf, b"\r\n$", destination.len());
            buf.extend_from_slice(destination);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                from_s.len(),
                from_s,
                to_s.len(),
                to_s
            );
            Some(Vec::new())
        }
        Command::Lmovem {
            source,
            destination,
            where_from,
            where_to,
            mode,
            count,
            ordering,
            raw_tokens,
        } => {
            let (from_s, to_s, mode_s, order_s) = if let Some(tokens) = raw_tokens {
                (
                    &tokens[0][..],
                    &tokens[1][..],
                    &tokens[2][..],
                    &tokens[3][..],
                )
            } else {
                let from_s = match where_from {
                    crate::table::ListDirection::Left => b"left".as_slice(),
                    crate::table::ListDirection::Right => b"right".as_slice(),
                };
                let to_s = match where_to {
                    crate::table::ListDirection::Left => b"left".as_slice(),
                    crate::table::ListDirection::Right => b"right".as_slice(),
                };
                let mode_s = match mode {
                    crate::resp::LmovemMode::Count => b"COUNT".as_slice(),
                    crate::resp::LmovemMode::Exactly => b"EXACTLY".as_slice(),
                };
                let order_s = match ordering {
                    crate::resp::LmovemOrdering::Obo => b"OBO".as_slice(),
                    crate::resp::LmovemOrdering::Bulk => b"BULK".as_slice(),
                };
                (from_s, to_s, mode_s, order_s)
            };
            let count_s = count.to_string();
            let _ = write!(buf, "*8\r\n$6\r\nLMOVEM\r\n${}\r\n", source.len());
            buf.extend_from_slice(source);
            push_len(buf, b"\r\n$", destination.len());
            buf.extend_from_slice(destination);
            push_len(buf, b"\r\n$", from_s.len());
            buf.extend_from_slice(from_s);
            push_len(buf, b"\r\n$", to_s.len());
            buf.extend_from_slice(to_s);
            push_len(buf, b"\r\n$", mode_s.len());
            buf.extend_from_slice(mode_s);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n",
                count_s.len(),
                count_s,
                order_s.len()
            );
            buf.extend_from_slice(order_s);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Incrbyfloat { key, increment } => {
            let s = increment.to_string();
            let _ = write!(buf, "*3\r\n$11\r\nINCRBYFLOAT\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(buf, "\r\n${}\r\n{}\r\n", s.len(), s);
            Some(Vec::new())
        }
        Command::Setrange { key, offset, value } => {
            let off_s = offset.to_string();
            let _ = write!(buf, "*4\r\n$8\r\nSETRANGE\r\n${}\r\n", key.len());
            buf.extend_from_slice(key);
            let _ = write!(
                buf,
                "\r\n${}\r\n{}\r\n${}\r\n",
                off_s.len(),
                off_s,
                value.len()
            );
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Sort {
            key,
            desc,
            alpha,
            store: Some(dest),
            limit,
            by,
            get,
            readonly: _,
        } => {
            let mut args: Vec<Vec<u8>> = vec![b"SORT".to_vec(), key.to_vec()];
            if let Some(by_pat) = by {
                args.push(b"BY".to_vec());
                args.push(by_pat.to_vec());
            }
            if let Some((offset, count)) = limit {
                args.push(b"LIMIT".to_vec());
                args.push(offset.to_string().into_bytes());
                args.push(count.to_string().into_bytes());
            }
            for get_pat in get {
                args.push(b"GET".to_vec());
                args.push(get_pat.to_vec());
            }
            if *desc {
                args.push(b"DESC".to_vec());
            }
            if *alpha {
                args.push(b"ALPHA".to_vec());
            }
            args.push(b"STORE".to_vec());
            args.push(dest.to_vec());
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Hexpire {
            key,
            expire_ms,
            is_at,
            condition,
            fields,
        } => {
            // Always propagate as an absolute deadline (see `unix_ms_after`).
            let cmd_name = "HPEXPIREAT";
            let exp_s = if *is_at {
                expire_ms.to_string()
            } else {
                unix_ms_after(std::time::Duration::from_millis((*expire_ms).max(0) as u64))
                    .to_string()
            };
            let numfields_s = fields.len().to_string();
            let mut args: Vec<&[u8]> = vec![cmd_name.as_bytes(), key.as_ref(), exp_s.as_bytes()];
            match condition {
                crate::resp::HexpireCondition::None => {}
                crate::resp::HexpireCondition::Nx => args.push(b"NX"),
                crate::resp::HexpireCondition::Xx => args.push(b"XX"),
                crate::resp::HexpireCondition::Gt => args.push(b"GT"),
                crate::resp::HexpireCondition::Lt => args.push(b"LT"),
            }
            args.push(b"FIELDS");
            args.push(numfields_s.as_bytes());
            for f in fields {
                args.push(f.as_ref());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Hpersist { key, fields } => {
            let numfields_s = fields.len().to_string();
            let mut args: Vec<&[u8]> =
                vec![b"HPERSIST", key.as_ref(), b"FIELDS", numfields_s.as_bytes()];
            for f in fields {
                args.push(f.as_ref());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Hgetex {
            key,
            expire,
            fields,
        } => {
            if matches!(
                expire,
                crate::resp::HFieldExpireOpt::None | crate::resp::HFieldExpireOpt::KeepTtl
            ) {
                return None;
            }
            let numfields_s = fields.len().to_string();
            let val_s;
            let mut args: Vec<&[u8]> = vec![b"HGETEX", key.as_ref()];
            match expire {
                crate::resp::HFieldExpireOpt::Persist => {
                    args.push(b"PERSIST");
                }
                crate::resp::HFieldExpireOpt::ExMs(ms) => {
                    val_s = unix_ms_after(std::time::Duration::from_millis((*ms).max(0) as u64))
                        .to_string();
                    args.push(b"PXAT");
                    args.push(val_s.as_bytes());
                }
                crate::resp::HFieldExpireOpt::ExAtMs(ms) => {
                    val_s = ms.to_string();
                    args.push(b"PXAT");
                    args.push(val_s.as_bytes());
                }
                _ => {}
            }
            args.push(b"FIELDS");
            args.push(numfields_s.as_bytes());
            for f in fields {
                args.push(f.as_ref());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Hsetex {
            key,
            condition,
            expire,
            pairs,
        } => {
            let numfields_s = pairs.len().to_string();
            let val_s;
            let mut args: Vec<&[u8]> = vec![b"HSETEX", key.as_ref()];
            match condition {
                crate::resp::HsetexCondition::None => {}
                crate::resp::HsetexCondition::Fnx => args.push(b"FNX"),
                crate::resp::HsetexCondition::Fxx => args.push(b"FXX"),
            }
            match expire {
                crate::resp::HFieldExpireOpt::None | crate::resp::HFieldExpireOpt::Persist => {}
                crate::resp::HFieldExpireOpt::KeepTtl => args.push(b"KEEPTTL"),
                crate::resp::HFieldExpireOpt::ExMs(ms) => {
                    val_s = unix_ms_after(std::time::Duration::from_millis((*ms).max(0) as u64))
                        .to_string();
                    args.push(b"PXAT");
                    args.push(val_s.as_bytes());
                }
                crate::resp::HFieldExpireOpt::ExAtMs(ms) => {
                    val_s = ms.to_string();
                    args.push(b"PXAT");
                    args.push(val_s.as_bytes());
                }
            }
            args.push(b"FIELDS");
            args.push(numfields_s.as_bytes());
            for (f, v) in pairs {
                args.push(f.as_ref());
                args.push(v.as_ref());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Xclaim {
            key,
            group,
            consumer,
            min_idle_time,
            ids,
            idle,
            time,
            retrycount,
            force,
            justid,
            lastid,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"XCLAIM".to_vec(),
                key.to_vec(),
                group.to_vec(),
                consumer.to_vec(),
                min_idle_time.to_string().into_bytes(),
            ];
            for id in ids.iter() {
                args.push(id.to_vec());
            }
            if let Some(i) = idle {
                args.push(b"IDLE".to_vec());
                args.push(i.to_string().into_bytes());
            }
            if let Some(t) = time {
                args.push(b"TIME".to_vec());
                args.push(t.to_string().into_bytes());
            }
            if let Some(r) = retrycount {
                args.push(b"RETRYCOUNT".to_vec());
                args.push(r.to_string().into_bytes());
            }
            if *force {
                args.push(b"FORCE".to_vec());
            }
            if *justid {
                args.push(b"JUSTID".to_vec());
            }
            if let Some(l) = lastid {
                args.push(b"LASTID".to_vec());
                args.push(l.to_string().into_bytes());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Xautoclaim {
            key,
            group,
            consumer,
            min_idle_time,
            start,
            count,
            justid,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"XAUTOCLAIM".to_vec(),
                key.to_vec(),
                group.to_vec(),
                consumer.to_vec(),
                min_idle_time.to_string().into_bytes(),
                start.to_vec(),
            ];
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
            if *justid {
                args.push(b"JUSTID".to_vec());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Zrangestore { dst, src, opts } => {
            let (start_s, stop_s) = if opts.by_score {
                let min_s = if opts.min_inc {
                    opts.min_score.to_string()
                } else {
                    format!("({}", opts.min_score)
                };
                let max_s = if opts.max_inc {
                    opts.max_score.to_string()
                } else {
                    format!("({}", opts.max_score)
                };
                (min_s.into_bytes(), max_s.into_bytes())
            } else {
                (
                    opts.start.to_string().into_bytes(),
                    opts.stop.to_string().into_bytes(),
                )
            };
            let mut args: Vec<Vec<u8>> = vec![
                b"ZRANGESTORE".to_vec(),
                dst.to_vec(),
                src.to_vec(),
                start_s,
                stop_s,
            ];
            if opts.by_score {
                args.push(b"BYSCORE".to_vec());
            } else if opts.by_lex {
                args.push(b"BYLEX".to_vec());
            }
            if opts.rev {
                args.push(b"REV".to_vec());
            }
            if let Some(count) = opts.count {
                args.push(b"LIMIT".to_vec());
                args.push(opts.offset.to_string().into_bytes());
                args.push(count.to_string().into_bytes());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        // JSON.* writes are logged as issued; replaying them in order
        // rebuilds the same document.
        Command::JsonSet {
            key,
            path,
            json_val,
            nx,
            xx,
        } => {
            let mut args: Vec<&[u8]> = vec![b"JSON.SET", key, path.as_bytes(), json_val.as_bytes()];
            if *nx {
                args.push(b"NX");
            }
            if *xx {
                args.push(b"XX");
            }
            Some(resp_argv(&args))
        }
        Command::JsonDel { key, path } | Command::JsonClear { key, path } => {
            let name: &[u8] = if matches!(cmd, Command::JsonDel { .. }) {
                b"JSON.DEL"
            } else {
                b"JSON.CLEAR"
            };
            let mut args: Vec<&[u8]> = vec![name, key];
            args.extend(path.as_deref().map(str::as_bytes));
            Some(resp_argv(&args))
        }
        Command::JsonNumIncrBy { key, path, delta } => Some(resp_argv(&[
            b"JSON.NUMINCRBY".as_slice(),
            key,
            path.as_bytes(),
            delta.to_string().as_bytes(),
        ])),
        Command::JsonNumMultBy { key, path, factor } => Some(resp_argv(&[
            b"JSON.NUMMULTBY".as_slice(),
            key,
            path.as_bytes(),
            factor.to_string().as_bytes(),
        ])),
        Command::JsonStrAppend { key, path, value } => {
            let mut args: Vec<&[u8]> = vec![b"JSON.STRAPPEND", key];
            args.extend(path.as_deref().map(str::as_bytes));
            args.push(value.as_bytes());
            Some(resp_argv(&args))
        }
        Command::JsonArrAppend { key, path, values } => {
            let mut args: Vec<&[u8]> = vec![b"JSON.ARRAPPEND", key, path.as_bytes()];
            args.extend(values.iter().map(String::as_bytes));
            Some(resp_argv(&args))
        }
        Command::JsonArrPop { key, path, index } => {
            let index = index.map(|i| i.to_string());
            let mut args: Vec<&[u8]> = vec![b"JSON.ARRPOP", key];
            // The parser only reads an index after a path.
            args.extend(path.as_deref().map(str::as_bytes));
            if path.is_some() {
                args.extend(index.as_deref().map(str::as_bytes));
            }
            Some(resp_argv(&args))
        }
        Command::JsonToggle { key, path } => Some(resp_argv(&[
            b"JSON.TOGGLE".as_slice(),
            key,
            path.as_bytes(),
        ])),
        // BF/CF/CMS/TOPK.* writes are logged as issued: the structures hash
        // deterministically (no RNG, Top-K ties broken by item), so replaying
        // them in order rebuilds the same state.
        Command::BfReserve {
            key,
            error_rate,
            capacity,
        } => Some(resp_argv(&[
            b"BF.RESERVE".as_slice(),
            key,
            error_rate.to_string().as_bytes(),
            capacity.to_string().as_bytes(),
        ])),
        Command::BfAdd { key, item } => Some(resp_argv(&[
            b"BF.ADD".as_slice(),
            key.as_ref(),
            item.as_ref(),
        ])),
        Command::BfMadd { key, items } | Command::TopkAdd { key, items } => {
            let name: &[u8] = if matches!(cmd, Command::BfMadd { .. }) {
                b"BF.MADD"
            } else {
                b"TOPK.ADD"
            };
            let mut args: Vec<&[u8]> = vec![name, key];
            args.extend(items.iter().map(|i| i.as_ref()));
            Some(resp_argv(&args))
        }
        Command::CfReserve { key, capacity } => Some(resp_argv(&[
            b"CF.RESERVE".as_slice(),
            key,
            capacity.to_string().as_bytes(),
        ])),
        Command::CfAdd { key, item }
        | Command::CfAddnx { key, item }
        | Command::CfDel { key, item } => {
            let name: &[u8] = match cmd {
                Command::CfAdd { .. } => b"CF.ADD",
                Command::CfAddnx { .. } => b"CF.ADDNX",
                _ => b"CF.DEL",
            };
            Some(resp_argv(&[name, key.as_ref(), item.as_ref()]))
        }
        Command::CmsInitbydim { key, width, depth } => Some(resp_argv(&[
            b"CMS.INITBYDIM".as_slice(),
            key,
            width.to_string().as_bytes(),
            depth.to_string().as_bytes(),
        ])),
        Command::CmsInitbyprob {
            key,
            error,
            probability,
        } => Some(resp_argv(&[
            b"CMS.INITBYPROB".as_slice(),
            key,
            error.to_string().as_bytes(),
            probability.to_string().as_bytes(),
        ])),
        Command::CmsIncrby { key, pairs } => {
            let incs: Vec<String> = pairs.iter().map(|(_, inc)| inc.to_string()).collect();
            let mut args: Vec<&[u8]> = vec![b"CMS.INCRBY", key];
            for ((item, _), inc) in pairs.iter().zip(&incs) {
                args.push(item);
                args.push(inc.as_bytes());
            }
            Some(resp_argv(&args))
        }
        Command::TopkReserve { key, topk } => Some(resp_argv(&[
            b"TOPK.RESERVE".as_slice(),
            key,
            topk.to_string().as_bytes(),
        ])),
        Command::ProbRestore { kind, key, payload } => Some(resp_argv(&[
            kind.restore_command().as_bytes(),
            key,
            payload,
        ])),
        // CRDT.SET/DEL/INCRBY/SADD/SREM stamp HLC timestamps and the node id,
        // so replaying them would not rebuild the same state; their handlers
        // log the change as a CRDT.MERGE of the affected key instead.
        Command::CrdtSet { .. }
        | Command::CrdtDel(_)
        | Command::CrdtIncrby { .. }
        | Command::CrdtSadd { .. }
        | Command::CrdtSrem { .. } => None,
        Command::CrdtMerge(payload) => Some(resp_argv(&[b"CRDT.MERGE".as_slice(), payload])),
        // Function libraries are server-wide; like Redis, the commands that
        // change them are replicated and written to the AOF as sent.
        Command::FunctionLoad { replace, code } => Some(if *replace {
            resp_argv(&[b"FUNCTION".as_slice(), b"LOAD", b"REPLACE", code])
        } else {
            resp_argv(&[b"FUNCTION".as_slice(), b"LOAD", code])
        }),
        Command::FunctionDelete(lib) => Some(resp_argv(&[
            b"FUNCTION".as_slice(),
            b"DELETE",
            lib.as_bytes(),
        ])),
        Command::FunctionFlush => Some(resp_argv(&[b"FUNCTION".as_slice(), b"FLUSH"])),
        Command::FunctionRestore { payload, policy } => Some(resp_argv(&[
            b"FUNCTION".as_slice(),
            b"RESTORE",
            payload,
            policy.as_bytes(),
        ])),
        // Search index definitions are server-wide too. The documents are
        // the HASH/JSON keys, logged as their own writes; every shard logs
        // the definition and re-indexes its keys on replay. FT.CREATE is
        // written in canonical form (see `search::ft_create_args`).
        Command::FtCreate {
            index,
            on_type,
            prefixes,
            schema_fields,
            ..
        } => Some(resp_argv(&crate::search::ft_create_args(
            index,
            on_type,
            prefixes,
            schema_fields,
        ))),
        Command::FtAlter {
            index,
            schema_fields,
            ..
        } => Some(resp_argv(&crate::search::ft_alter_args(
            index,
            schema_fields,
        ))),
        Command::FtDropIndex { index, dd } => Some(if *dd {
            resp_argv(&[b"FT.DROPINDEX".as_slice(), index.as_bytes(), b"DD"])
        } else {
            resp_argv(&[b"FT.DROPINDEX".as_slice(), index.as_bytes()])
        }),
        Command::CrdtGc(horizon) => Some(match horizon {
            crate::crdt::GcHorizon::Ttl(None) => resp_argv(&[b"CRDT.GC"]),
            crate::crdt::GcHorizon::Ttl(Some(ttl)) => {
                resp_argv(&[b"CRDT.GC".as_slice(), ttl.to_string().as_bytes()])
            }
            crate::crdt::GcHorizon::Before(cutoff) => resp_argv(&[
                b"CRDT.GC".as_slice(),
                b"BEFORE",
                cutoff.to_string().as_bytes(),
            ]),
        }),
        Command::SemanticSet {
            namespace,
            id,
            prompt,
            response,
            vector,
            ttl,
            scope,
            quantize,
            tokens,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"SEMANTIC.SET".to_vec(),
                namespace.to_vec(),
                id.to_vec(),
                prompt.to_vec(),
                response.to_vec(),
                b"VECTOR".to_vec(),
                vector.len().to_string().into_bytes(),
            ];
            for v in vector.iter() {
                args.push(v.to_string().into_bytes());
            }
            if let Some(d) = ttl {
                args.push(b"PX".to_vec());
                args.push(d.as_millis().max(1).to_string().into_bytes());
            }
            if let Some(s) = scope {
                args.push(b"SCOPE".to_vec());
                args.push(s.to_vec());
            }
            if *quantize {
                args.push(b"QUANTIZE".to_vec());
            }
            if let Some(t) = tokens {
                args.push(b"TOKENS".to_vec());
                args.push(t.to_string().into_bytes());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::SemanticDel { namespace, ids } => {
            let _ = write!(buf, "*{}\r\n$12\r\nSEMANTIC.DEL\r\n", 2 + ids.len());
            push_len(buf, b"$", namespace.len());
            buf.extend_from_slice(namespace);
            buf.extend_from_slice(b"\r\n");
            for id in ids {
                push_len(buf, b"$", id.len());
                buf.extend_from_slice(id);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::SemanticFlush(namespace) => {
            let _ = write!(
                buf,
                "*2\r\n$14\r\nSEMANTIC.FLUSH\r\n${}\r\n",
                namespace.len()
            );
            buf.extend_from_slice(namespace);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::AgentMemAdd {
            session,
            role,
            content,
            tokens,
            vector,
            meta,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.MEM.ADD".to_vec(),
                session.to_vec(),
                role.to_vec(),
                content.to_vec(),
            ];
            if let Some(t) = tokens {
                args.push(b"TOKENS".to_vec());
                args.push(t.to_string().into_bytes());
            }
            if let Some(v) = vector {
                args.push(b"VEC".to_vec());
                args.push(v.len().to_string().into_bytes());
                for &f in v {
                    args.push(f.to_string().into_bytes());
                }
            }
            if let Some(m) = meta {
                args.push(b"META".to_vec());
                args.push(m.to_vec());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::AgentMemCompact {
            session,
            keep_recent,
            summary,
            tokens,
            vector,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.MEM.COMPACT".to_vec(),
                session.to_vec(),
                b"KEEP_RECENT".to_vec(),
                keep_recent.to_string().into_bytes(),
                b"SUMMARY".to_vec(),
                summary.to_vec(),
            ];
            if let Some(t) = tokens {
                args.push(b"TOKENS".to_vec());
                args.push(t.to_string().into_bytes());
            }
            if let Some(v) = vector {
                args.push(b"VEC".to_vec());
                args.push(v.len().to_string().into_bytes());
                for &f in v {
                    args.push(f.to_string().into_bytes());
                }
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::AgentMemClear(session) => {
            let _ = write!(
                buf,
                "*2\r\n$15\r\nAGENT.MEM.CLEAR\r\n${}\r\n",
                session.len()
            );
            buf.extend_from_slice(session);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::AgentCheckpointPut {
            key,
            step_id,
            parent_id,
            state,
            meta,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.CHECKPOINT.PUT".to_vec(),
                key.to_vec(),
                step_id.to_vec(),
            ];
            if let Some(p) = parent_id {
                args.push(b"PARENT".to_vec());
                args.push(p.to_vec());
            }
            args.push(b"STATE".to_vec());
            args.push(state.to_vec());
            if let Some(m) = meta {
                args.push(b"META".to_vec());
                args.push(m.to_vec());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::AgentToolClaim {
            key,
            call_id,
            ttl_ms,
            input,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.TOOL.CLAIM".to_vec(),
                key.to_vec(),
                call_id.to_vec(),
                b"TTL".to_vec(),
                ttl_ms.to_string().into_bytes(),
            ];
            if let Some(inp) = input {
                args.push(b"INPUT".to_vec());
                args.push(inp.to_vec());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::AgentToolComplete {
            key,
            call_id,
            output,
            ttl_ms,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.TOOL.COMPLETE".to_vec(),
                key.to_vec(),
                call_id.to_vec(),
                b"OUTPUT".to_vec(),
                output.to_vec(),
            ];
            if let Some(ttl) = ttl_ms {
                args.push(b"TTL".to_vec());
                args.push(ttl.to_string().into_bytes());
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Vadd {
            key,
            element,
            vector,
            metric,
            quantize,
            pq,
            tiered,
            reduce,
            quant,
            ef,
            setattr,
            m,
            cas,
            is_redis_vset,
        } => {
            let mut args: Vec<Vec<u8>> = Vec::with_capacity(14);
            args.push(b"VADD".to_vec());
            args.push(key.to_vec());
            if *is_redis_vset {
                if let Some(r) = reduce {
                    args.push(b"REDUCE".to_vec());
                    args.push(r.to_string().into_bytes());
                }
                args.push(b"FP32".to_vec());
                let mut blob = Vec::with_capacity(vector.len() * 4);
                for &v in vector {
                    blob.extend_from_slice(&v.to_le_bytes());
                }
                args.push(blob);
                args.push(element.to_vec());
                if *cas {
                    args.push(b"CAS".to_vec());
                }
                if let Some(q) = quant {
                    args.push(match q {
                        crate::vector::VQuant::NoQuant => b"NOQUANT".to_vec(),
                        crate::vector::VQuant::Q8 => b"Q8".to_vec(),
                        crate::vector::VQuant::Bin => b"BIN".to_vec(),
                    });
                }
                if let Some(e) = ef {
                    args.push(b"EF".to_vec());
                    args.push(e.to_string().into_bytes());
                }
                if let Some(attr) = setattr {
                    args.push(b"SETATTR".to_vec());
                    args.push(attr.as_bytes().to_vec());
                }
                if let Some(m_val) = m {
                    args.push(b"M".to_vec());
                    args.push(m_val.to_string().into_bytes());
                }
            } else {
                args.push(element.to_vec());
                for &v in vector {
                    args.push(v.to_string().into_bytes());
                }
                if let Some(m_type) = metric {
                    args.push(m_type.as_str().as_bytes().to_vec());
                }
                if *quantize {
                    args.push(b"QUANTIZE".to_vec());
                }
                if *pq {
                    args.push(b"PQ".to_vec());
                }
                if *tiered {
                    args.push(b"TIERED".to_vec());
                }
            }
            push_len(buf, b"*", args.len());
            for a in args {
                push_len(buf, b"$", a.len());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(Vec::new())
        }
        Command::Vdel { key, element } => {
            buf.extend_from_slice(b"*3\r\n$4\r\nVREM\r\n");
            push_len(buf, b"$", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", element.len());
            buf.extend_from_slice(element);
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Vsetattr { key, element, attr } => {
            buf.extend_from_slice(b"*4\r\n$8\r\nVSETATTR\r\n");
            push_len(buf, b"$", key.len());
            buf.extend_from_slice(key);
            push_len(buf, b"\r\n$", element.len());
            buf.extend_from_slice(element);
            push_len(buf, b"\r\n$", attr.len());
            buf.extend_from_slice(attr.as_bytes());
            buf.extend_from_slice(b"\r\n");
            Some(Vec::new())
        }
        Command::Lpushx { key, values } | Command::Rpushx { key, values } => {
            let name: &[u8] = if matches!(cmd, Command::Lpushx { .. }) {
                b"LPUSHX"
            } else {
                b"RPUSHX"
            };
            let mut args: Vec<&[u8]> = Vec::with_capacity(values.len() + 2);
            args.push(name);
            args.push(key);
            args.extend(values.iter().map(|v| v.as_ref()));
            Some(resp_argv(&args))
        }
        Command::Zpopmin { key, count } | Command::Zpopmax { key, count } => {
            let name: &[u8] = if matches!(cmd, Command::Zpopmin { .. }) {
                b"ZPOPMIN"
            } else {
                b"ZPOPMAX"
            };
            match count {
                Some(c) => Some(resp_argv(&[name, key, c.to_string().as_bytes()])),
                None => Some(resp_argv(&[name, key])),
            }
        }
        Command::Zmpop {
            keys,
            is_min,
            count,
        } => {
            let mut args: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 5);
            args.push(b"ZMPOP".to_vec());
            args.push(keys.len().to_string().into_bytes());
            args.extend(keys.iter().map(|k| k.to_vec()));
            args.push(if *is_min {
                b"MIN".to_vec()
            } else {
                b"MAX".to_vec()
            });
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
            Some(resp_argv(&args))
        }
        Command::Sinterstore { destination, keys }
        | Command::Sunionstore { destination, keys }
        | Command::Sdiffstore { destination, keys }
        | Command::Zdiffstore { destination, keys } => {
            let name: &[u8] = match cmd {
                Command::Sinterstore { .. } => b"SINTERSTORE",
                Command::Sunionstore { .. } => b"SUNIONSTORE",
                Command::Sdiffstore { .. } => b"SDIFFSTORE",
                _ => b"ZDIFFSTORE",
            };
            let numkeys = keys.len().to_string();
            let mut args: Vec<&[u8]> = Vec::with_capacity(keys.len() + 3);
            args.push(name);
            args.push(destination);
            if matches!(cmd, Command::Zdiffstore { .. }) {
                args.push(numkeys.as_bytes());
            }
            args.extend(keys.iter().map(|k| k.as_ref()));
            Some(resp_argv(&args))
        }
        Command::Zunionstore {
            destination,
            keys,
            weights,
            aggregate,
        }
        | Command::Zinterstore {
            destination,
            keys,
            weights,
            aggregate,
        } => {
            let mut args: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + weights.len() + 6);
            args.push(if matches!(cmd, Command::Zunionstore { .. }) {
                b"ZUNIONSTORE".to_vec()
            } else {
                b"ZINTERSTORE".to_vec()
            });
            args.push(destination.to_vec());
            args.push(keys.len().to_string().into_bytes());
            args.extend(keys.iter().map(|k| k.to_vec()));
            if !weights.is_empty() {
                args.push(b"WEIGHTS".to_vec());
                args.extend(weights.iter().map(|w| w.to_string().into_bytes()));
            }
            let agg: Option<&[u8]> = match aggregate {
                crate::table::Aggregate::Sum => None,
                crate::table::Aggregate::Min => Some(b"MIN"),
                crate::table::Aggregate::Max => Some(b"MAX"),
                crate::table::Aggregate::Count => Some(b"COUNT"),
            };
            if let Some(a) = agg {
                args.push(b"AGGREGATE".to_vec());
                args.push(a.to_vec());
            }
            Some(resp_argv(&args))
        }
        Command::Geoadd {
            key,
            items,
            nx,
            xx,
            ch,
        } => {
            let mut args: Vec<Vec<u8>> = Vec::with_capacity(items.len() * 3 + 5);
            args.push(b"GEOADD".to_vec());
            args.push(key.to_vec());
            if *nx {
                args.push(b"NX".to_vec());
            }
            if *xx {
                args.push(b"XX".to_vec());
            }
            if *ch {
                args.push(b"CH".to_vec());
            }
            for (lon, lat, member) in items {
                args.push(lon.to_string().into_bytes());
                args.push(lat.to_string().into_bytes());
                args.push(member.to_vec());
            }
            Some(resp_argv(&args))
        }
        Command::Geosearchstore {
            dest,
            key,
            from_member,
            from_lonlat,
            by_radius,
            by_box,
            asc,
            count,
            any,
            storedist,
        } => {
            let unit = |u: &crate::geo::GeoUnit| -> Vec<u8> {
                match u {
                    crate::geo::GeoUnit::Meters => b"m".to_vec(),
                    crate::geo::GeoUnit::Kilometers => b"km".to_vec(),
                    crate::geo::GeoUnit::Miles => b"mi".to_vec(),
                    crate::geo::GeoUnit::Feet => b"ft".to_vec(),
                }
            };
            let mut args: Vec<Vec<u8>> = Vec::with_capacity(14);
            args.push(b"GEOSEARCHSTORE".to_vec());
            args.push(dest.to_vec());
            args.push(key.to_vec());
            if let Some(m) = from_member {
                args.push(b"FROMMEMBER".to_vec());
                args.push(m.to_vec());
            }
            if let Some((lon, lat)) = from_lonlat {
                args.push(b"FROMLONLAT".to_vec());
                args.push(lon.to_string().into_bytes());
                args.push(lat.to_string().into_bytes());
            }
            if let Some((r, u)) = by_radius {
                args.push(b"BYRADIUS".to_vec());
                args.push(r.to_string().into_bytes());
                args.push(unit(u));
            }
            if let Some((w, h, u)) = by_box {
                args.push(b"BYBOX".to_vec());
                args.push(w.to_string().into_bytes());
                args.push(h.to_string().into_bytes());
                args.push(unit(u));
            }
            match asc {
                Some(true) => args.push(b"ASC".to_vec()),
                Some(false) => args.push(b"DESC".to_vec()),
                None => {}
            }
            if let Some(c) = count {
                args.push(b"COUNT".to_vec());
                args.push(c.to_string().into_bytes());
                if *any {
                    args.push(b"ANY".to_vec());
                }
            }
            if *storedist {
                args.push(b"STOREDIST".to_vec());
            }
            Some(resp_argv(&args))
        }
        Command::Xsetid {
            key,
            last_id,
            entries_added,
            max_deleted_id,
        } => {
            let mut args: Vec<Vec<u8>> = Vec::with_capacity(7);
            args.push(b"XSETID".to_vec());
            args.push(key.to_vec());
            args.push(last_id.to_string().into_bytes());
            if let Some(n) = entries_added {
                args.push(b"ENTRIESADDED".to_vec());
                args.push(n.to_string().into_bytes());
            }
            if let Some(id) = max_deleted_id {
                args.push(b"MAXDELETEDID".to_vec());
                args.push(id.to_string().into_bytes());
            }
            Some(resp_argv(&args))
        }
        Command::XgroupCreateConsumer {
            key,
            group,
            consumer,
        } => Some(resp_argv(&[
            b"XGROUP".as_slice(),
            b"CREATECONSUMER",
            key,
            group,
            consumer,
        ])),
        Command::XgroupDelConsumer {
            key,
            group,
            consumer,
        } => Some(resp_argv(&[
            b"XGROUP".as_slice(),
            b"DELCONSUMER",
            key,
            group,
            consumer,
        ])),
        // DELEX only records a change when the key was actually deleted, so
        // the condition has already been evaluated: replay it as a plain DEL.
        Command::Delex { key, .. } => Some(resp_argv(&[b"DEL".as_slice(), key])),
        _ => None,
    }
}

/// `[KEEPREF | DELREF | ACKED]`; KEEPREF, the default, is left out so the
/// command stays valid for servers without the Redis 8.2 strategies.
fn push_stream_trim_strategy(args: &mut Vec<Vec<u8>>, strategy: crate::table::StreamTrimStrategy) {
    match strategy {
        crate::table::StreamTrimStrategy::KeepRef => {}
        crate::table::StreamTrimStrategy::DelRef => args.push(b"DELREF".to_vec()),
        crate::table::StreamTrimStrategy::Acked => args.push(b"ACKED".to_vec()),
    }
}

/// `MAXLEN|MINID <=|~> <threshold> [LIMIT <count>]` for each threshold given.
fn push_stream_trim_args(
    args: &mut Vec<Vec<u8>>,
    maxlen: Option<usize>,
    minid: Option<crate::table::StreamId>,
    approx: bool,
    limit: Option<usize>,
) {
    let op: &[u8] = if approx { b"~" } else { b"=" };
    if let Some(max) = maxlen {
        args.push(b"MAXLEN".to_vec());
        args.push(op.to_vec());
        args.push(max.to_string().into_bytes());
    }
    if let Some(min) = minid {
        args.push(b"MINID".to_vec());
        args.push(op.to_vec());
        args.push(min.to_string().into_bytes());
    }
    if let Some(l) = limit
        && (maxlen.is_some() || minid.is_some())
    {
        args.push(b"LIMIT".to_vec());
        args.push(l.to_string().into_bytes());
    }
}

/// `IDS <numids> <id> ...`
fn push_stream_ids(args: &mut Vec<Vec<u8>>, ids: &[crate::table::StreamId]) {
    args.push(b"IDS".to_vec());
    args.push(ids.len().to_string().into_bytes());
    args.extend(ids.iter().map(|id| id.to_string().into_bytes()));
}

/// Encodes `args` as a RESP array of bulk strings.
pub(crate) fn resp_argv<A: AsRef<[u8]>>(args: &[A]) -> Vec<u8> {
    let mut buf =
        Vec::with_capacity(16 + args.iter().map(|a| a.as_ref().len() + 16).sum::<usize>());
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        let a = a.as_ref();
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

static AOF_LOAD_TRUNCATED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// `aof-load-truncated`: when true (Redis default), an AOF whose last command
/// is incomplete is truncated to the last complete command and loaded.
pub fn set_aof_load_truncated(v: bool) {
    AOF_LOAD_TRUNCATED.store(v, std::sync::atomic::Ordering::Relaxed);
}

pub fn aof_load_truncated() -> bool {
    AOF_LOAD_TRUNCATED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Parses an `appendfsync` value into "fsync every second?" (`always`
/// fsyncs every second too, on top of its per-write fsyncs; see
/// [`appendfsync_is_always`]).
pub fn parse_appendfsync(v: &str) -> Result<bool, String> {
    match v.to_ascii_lowercase().as_str() {
        "everysec" | "always" => Ok(true),
        "no" => Ok(false),
        other => Err(format!(
            "invalid appendfsync '{}' (expected always, everysec or no)",
            other
        )),
    }
}

type FsyncFlags = std::collections::HashMap<u16, std::sync::Arc<std::sync::atomic::AtomicBool>>;
static FSYNC_EVERY_SEC: parking_lot::Mutex<Option<FsyncFlags>> = parking_lot::Mutex::new(None);
static FSYNC_ALWAYS: parking_lot::Mutex<Option<FsyncFlags>> = parking_lot::Mutex::new(None);

/// Whether any server in the process runs `appendfsync always`: only then
/// do AOF appends bump [`AOF_APPENDS`] (one shared counter would otherwise
/// cost every write a contended cache line).
pub static APPENDFSYNC_ALWAYS_ANY: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
/// AOF appends while [`APPENDFSYNC_ALWAYS_ANY`] is set. A connection that
/// sees it move during a batch fsyncs the AOFs before replying.
pub static AOF_APPENDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Live `appendfsync` policy for the server on `port` (true = everysec). The
/// AOF flusher reads it on every tick, so `CONFIG SET appendfsync` applies
/// without a restart. Defaults to everysec.
pub fn fsync_every_sec_flag(port: u16) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    let mut map = FSYNC_EVERY_SEC.lock();
    map.get_or_insert_with(Default::default)
        .entry(port)
        .or_insert_with(|| std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)))
        .clone()
}

/// `appendfsync always` for the server on `port`.
pub fn fsync_always_flag(port: u16) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    let mut map = FSYNC_ALWAYS.lock();
    map.get_or_insert_with(Default::default)
        .entry(port)
        .or_insert_with(|| std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)))
        .clone()
}

pub fn set_fsync_every_sec(port: u16, every_sec: bool) {
    fsync_every_sec_flag(port).store(every_sec, std::sync::atomic::Ordering::Relaxed);
}

/// Sets the full `appendfsync` policy from its config value.
pub fn set_appendfsync(port: u16, value: &str) -> Result<(), String> {
    let every_sec = parse_appendfsync(value)?;
    let always = value.eq_ignore_ascii_case("always");
    set_fsync_every_sec(port, every_sec);
    fsync_always_flag(port).store(always, std::sync::atomic::Ordering::Relaxed);
    if always {
        APPENDFSYNC_ALWAYS_ANY.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}

#[inline]
pub fn appendfsync_is_always(port: u16) -> bool {
    APPENDFSYNC_ALWAYS_ANY.load(std::sync::atomic::Ordering::Relaxed)
        && fsync_always_flag(port).load(std::sync::atomic::Ordering::Relaxed)
}

/// The `appendfsync` value reported by CONFIG GET.
pub fn appendfsync_name(port: u16) -> &'static str {
    if appendfsync_is_always(port) {
        "always"
    } else if fsync_every_sec_flag(port).load(std::sync::atomic::Ordering::Relaxed) {
        "everysec"
    } else {
        "no"
    }
}

/// Replays an AOF into `db`, returning the number of commands applied.
///
/// Like Redis: an incomplete final command (e.g. a crash mid-write) is cut
/// off and the file truncated to the last complete command when
/// `aof-load-truncated` is on, otherwise it is an error. Any malformed
/// command before the end of the file is always an error, so the caller can
/// refuse to start rather than silently dropping everything after it.
pub fn replay_aof(path: &Path, db: &mut ShardDb) -> std::io::Result<usize> {
    replay_aof_with(path, db, aof_load_truncated())
}

/// A file mapped read-only for loading. Reading an AOF or RDB file whole
/// into memory doubled the memory a restart needed (the dataset plus the
/// file, ~5.6 GB for a 2.9 GB dataset), until jemalloc returned the freed
/// buffer; a mapping is page cache the kernel can reclaim, and it is not
/// counted against maxmemory.
pub(crate) struct MappedFile {
    ptr: *mut libc::c_void,
    len: usize,
}

impl MappedFile {
    pub(crate) fn open(path: &Path) -> std::io::Result<Self> {
        use std::os::unix::io::AsRawFd;
        let file = std::fs::File::open(path)?;
        let len = usize::try_from(file.metadata()?.len())
            .map_err(|_| std::io::Error::other("file too large to map"))?;
        if len == 0 {
            return Ok(MappedFile {
                ptr: std::ptr::null_mut(),
                len: 0,
            });
        }
        // SAFETY: a fresh read-only private mapping of `len` bytes of an open
        // file; the fd may be closed once mapped. Load files are not
        // truncated while mapped (the AOF writer only appends to them, and
        // only after loading), which would otherwise SIGBUS on access.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: advice on the mapping just made; failure is harmless.
        unsafe { libc::madvise(ptr, len, libc::MADV_SEQUENTIAL) };
        Ok(MappedFile { ptr, len })
    }
}

// SAFETY: a read-only mapping, never written through, owned by this value
// alone: moving it to another thread (a replica's shards load from one
// mapped RDB) is as safe as moving a `Box<[u8]>`.
unsafe impl Send for MappedFile {}
// SAFETY: only ever read, so shared references from several threads are as
// safe as sharing a `&[u8]`.
unsafe impl Sync for MappedFile {}

impl AsRef<[u8]> for MappedFile {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::ops::Deref for MappedFile {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: `ptr` maps `len` readable bytes for as long as `self`
        // lives (unmapped in Drop).
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        if self.len != 0 {
            // SAFETY: unmaps exactly the mapping made in `open`; no slice of
            // it outlives `self`.
            unsafe { libc::munmap(self.ptr, self.len) };
        }
    }
}

pub fn replay_aof_with(
    path: &Path,
    db: &mut ShardDb,
    load_truncated: bool,
) -> std::io::Result<usize> {
    let replayed = replay_aof_inner(path, db, load_truncated);
    // The replay's freed buffers would otherwise sit in this thread's
    // allocator cache, counted against maxmemory.
    crate::allocator::flush_thread_cache();
    replayed
}

fn replay_aof_inner(path: &Path, db: &mut ShardDb, load_truncated: bool) -> std::io::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let data = MappedFile::open(path)?;
    if data.is_empty() {
        return Ok(0);
    }
    // Like Redis loading: nothing expires until the replay is done.
    let _no_expiry = crate::connection::ReplayingWrites::new();
    let bad = |offset: usize, why: &str| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Bad file format reading the append only file {:?}: bad AOF format at offset {} of {}: {}",
                path,
                offset,
                data.len(),
                why
            ),
        )
    };
    let eof_err = |offset: usize, why: &str| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "Unexpected end of file reading the append only file {:?}: bad AOF format at offset {} of {}: {}",
                path,
                offset,
                data.len(),
                why
            ),
        )
    };
    let (rdb_preamble_len, mut count) = if data.starts_with(b"REDIS") {
        let loaded = crate::redis_rdb::load_rdb_preamble(&data, db)
            .map_err(|e| bad(0, &format!("invalid RDB preamble: {}", e)))?;
        // Its keys bypassed the indexing hooks (as an RDB load's do): index
        // them before the commands after it, which index as they apply.
        db.rebuild_search_indices();
        loaded
    } else {
        (0, 0)
    };
    // The commands after the preamble are parsed a chunk at a time: copying
    // them whole into the parse buffer put the whole tail in memory on top
    // of the dataset (a restart with a long tail peaked at ~3x the data).
    const TAIL_CHUNK: usize = 8 << 20;
    let tail = &data[rdb_preamble_len..];
    // How much of the tail has been copied into `buf`.
    let next = std::cell::Cell::new(0usize);
    let mut buf = BytesMut::new();
    // Appends the next chunk of the tail; false once it is all in `buf`.
    let refill = |buf: &mut BytesMut| -> bool {
        let from = next.get();
        if from == tail.len() {
            return false;
        }
        let end = (from + TAIL_CHUNK).min(tail.len());
        buf.extend_from_slice(&tail[from..end]);
        next.set(end);
        true
    };
    refill(&mut buf);
    let mut dummy_out = Vec::new();
    let saved_db = crate::connection::CURRENT_DB.get();
    crate::connection::CURRENT_DB.set(0);
    db.select_db(0);
    // A MULTI ... EXEC block (as Redis logs transactions): the offset of its
    // MULTI and the commands queued so far. They apply only at EXEC, so a
    // block cut off at the end of the file is dropped as a whole.
    let mut in_multi: Option<(usize, Vec<Command>)> = None;
    let res = (|| -> std::io::Result<usize> {
        loop {
            if buf.len() < TAIL_CHUNK / 2 {
                refill(&mut buf);
            }
            // Skip Redis 7+ `#TS:<unix_ts>\r\n` timestamp annotations.
            while buf.starts_with(b"#") {
                if let Some(nl_pos) = buf.iter().position(|&b| b == b'\n') {
                    let _ = buf.split_to(nl_pos + 1);
                } else if !refill(&mut buf) {
                    buf.clear();
                    break;
                }
            }
            if buf.is_empty() && !refill(&mut buf) {
                break;
            }
            if buf.starts_with(b"#") {
                continue;
            }
            let offset = rdb_preamble_len + next.get() - buf.len();
            // rudis and Redis only ever append RESP arrays (after #TS comments);
            // anything else is corruption.
            if buf[0] != b'*' {
                return Err(bad(offset, "expected a RESP array"));
            }
            match crate::resp::parse_command_trusted(&mut buf) {
                Ok(Some(cmd)) => match cmd {
                    Command::Unknown(ref name) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Unknown command '{}' reading the append only file {:?}",
                                name.to_ascii_lowercase(),
                                path
                            ),
                        ));
                    }
                    // Redis rejects a nested MULTI and an EXEC or DISCARD
                    // outside one; loading ignores the error replies.
                    Command::Multi => {
                        if in_multi.is_none() {
                            in_multi = Some((offset, Vec::new()));
                        }
                    }
                    Command::Discard => {
                        in_multi = None;
                    }
                    Command::Exec => {
                        if let Some((_, queued)) = in_multi.take() {
                            for qcmd in queued {
                                apply_aof_command(&qcmd, db, &mut dummy_out, &mut count);
                            }
                        }
                    }
                    other => {
                        if let Some((_, ref mut queued)) = in_multi {
                            queued.push(other);
                        } else {
                            apply_aof_command(&other, db, &mut dummy_out, &mut count);
                        }
                    }
                },
                // A command that continues in the next chunk.
                Ok(None) if refill(&mut buf) => {}
                Ok(None) if buf.is_empty() => break,
                Ok(None) => {
                    // An open MULTI block is cut off along with the command.
                    let trunc_offset = in_multi.as_ref().map_or(offset, |(m_off, _)| *m_off);
                    if !load_truncated {
                        return Err(eof_err(
                            offset,
                            "truncated final command (set aof-load-truncated yes to load anyway)",
                        ));
                    }
                    in_multi = None;
                    crate::log_warning!(
                        "!!! Warning: short read while loading the AOF file {:?}: truncating it from {} to {} bytes (last complete command).",
                        path,
                        data.len(),
                        trunc_offset
                    );
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(path)?
                        .set_len(trunc_offset as u64)?;
                    break;
                }
                Err(e) => return Err(bad(offset, &e)),
            }
        }
        // Like Redis, a MULTI without its EXEC at the end of the file is a
        // truncated transaction: dropped (and cut off) under
        // aof-load-truncated, an error otherwise.
        if let Some((multi_offset, _)) = in_multi.take() {
            if !load_truncated {
                return Err(eof_err(
                    multi_offset,
                    "unfinished MULTI transaction at end of AOF (set aof-load-truncated yes to load anyway)",
                ));
            }
            crate::log_warning!(
                "!!! Warning: unfinished MULTI while loading the AOF file {:?}: truncating it from {} to {} bytes.",
                path,
                data.len(),
                multi_offset
            );
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)?
                .set_len(multi_offset as u64)?;
        }
        Ok(count)
    })();
    crate::connection::CURRENT_DB.set(saved_db);
    db.select_db(saved_db);
    res
}

/// Applies one replayed command. `EVAL`/`EVALSHA` (which Redis before 5.0
/// logged verbatim) run the script against `db`; everything else goes
/// through the shard executor.
fn apply_aof_command(cmd: &Command, db: &mut ShardDb, dummy_out: &mut Vec<u8>, count: &mut usize) {
    match cmd {
        Command::Eval {
            script, keys, args, ..
        } => {
            crate::scripting::load_script(script);
            replay_script(&String::from_utf8_lossy(script), keys, args, db);
            *count += 1;
        }
        Command::Evalsha {
            sha, keys, args, ..
        } => {
            let sha_str = String::from_utf8_lossy(sha).into_owned();
            if let Some(script_str) = crate::scripting::get_script(&sha_str) {
                replay_script(&script_str, keys, args, db);
            }
            *count += 1;
        }
        _ => {
            let is_select = matches!(cmd, Command::Select(_));
            crate::connection::execute_local_command(cmd, db, dummy_out, None);
            dummy_out.clear();
            if !is_select {
                *count += 1;
            }
        }
    }
}

/// Runs a replayed script against `db`, which `eval_script` needs behind an
/// `Rc<RefCell<_>>`: `db` is moved in for the call and back out after it.
fn replay_script(script: &str, keys: &[bytes::Bytes], args: &[bytes::Bytes], db: &mut ShardDb) {
    let saved_db = crate::connection::CURRENT_DB.get();
    let placeholder = ShardDb::new(db.port);
    let cell = std::rc::Rc::new(std::cell::RefCell::new(std::mem::replace(db, placeholder)));
    let _ = crate::scripting::eval_script(script, keys, args, &cell, None, false);
    // The script's `redis.call` closures are scoped to `eval_script`, so
    // this is the only reference left.
    *db = std::rc::Rc::try_unwrap(cell)
        .map_err(|_| ())
        .expect("a replayed script kept a reference to the db")
        .into_inner();
    crate::connection::CURRENT_DB.set(saved_db);
    db.select_db(saved_db);
}

/// Splits a manifest line like Redis's `sdssplitargs`: whitespace-separated
/// tokens, where a token may be `"double quoted"` (with `\"`, `\\`, `\n`,
/// `\r`, `\t`, `\xHH` escapes) or `'single quoted'` (with `\'`). Returns
/// `None` for unbalanced quotes or a closing quote not followed by a space.
fn split_manifest_args(line: &str) -> Option<Vec<String>> {
    let b = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == b.len() {
            return Some(out);
        }
        let mut tok: Vec<u8> = Vec::new();
        match b[i] {
            b'"' => {
                i += 1;
                loop {
                    match *b.get(i)? {
                        b'"' => break,
                        b'\\' => {
                            let c = *b.get(i + 1)?;
                            if c == b'x'
                                && let Some(hex) = b.get(i + 2..i + 4)
                                && let Ok(v) =
                                    u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16)
                            {
                                tok.push(v);
                                i += 4;
                                continue;
                            }
                            tok.push(match c {
                                b'n' => b'\n',
                                b'r' => b'\r',
                                b't' => b'\t',
                                b'b' => 8,
                                b'a' => 7,
                                other => other,
                            });
                            i += 2;
                        }
                        c => {
                            tok.push(c);
                            i += 1;
                        }
                    }
                }
                i += 1;
                if b.get(i).is_some_and(|c| !c.is_ascii_whitespace()) {
                    return None;
                }
            }
            b'\'' => {
                i += 1;
                loop {
                    match *b.get(i)? {
                        b'\'' => break,
                        b'\\' if b.get(i + 1) == Some(&b'\'') => {
                            tok.push(b'\'');
                            i += 2;
                        }
                        c => {
                            tok.push(c);
                            i += 1;
                        }
                    }
                }
                i += 1;
                if b.get(i).is_some_and(|c| !c.is_ascii_whitespace()) {
                    return None;
                }
            }
            _ => {
                while i < b.len() && !b[i].is_ascii_whitespace() {
                    tok.push(b[i]);
                    i += 1;
                }
            }
        }
        out.push(String::from_utf8(tok).ok()?);
    }
}

/// Parses a Redis 7+ `appendonly.aof.manifest` file and returns the ordered
/// list of AOF files to load (base file first, then incremental files ordered
/// by `seq`).
fn parse_redis_aof_manifest(manifest_path: &Path) -> std::io::Result<Vec<PathBuf>> {
    let invalid = |msg: String| std::io::Error::new(std::io::ErrorKind::InvalidData, msg);
    let parent = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let content = std::fs::read_to_string(manifest_path)?;
    if content.is_empty() {
        return Err(invalid(format!(
            "Found an empty AOF manifest in {:?}",
            manifest_path
        )));
    }
    let mut base_files: Vec<(u64, PathBuf)> = Vec::new();
    let mut incr_files: Vec<(u64, PathBuf)> = Vec::new();
    let mut saw_any_entry = false;
    for raw_line in content.lines() {
        if raw_line.len() > 1024 {
            return Err(invalid(format!(
                "The AOF manifest file contains too long line in {:?}",
                manifest_path
            )));
        }
        let line = raw_line.trim();
        if line.is_empty() {
            return Err(invalid(format!(
                "Invalid AOF manifest file format in {:?}: blank line",
                manifest_path
            )));
        }
        if line.starts_with('#') {
            continue;
        }
        let bad_line = || {
            invalid(format!(
                "Invalid AOF manifest file format in {:?}: {:?}",
                manifest_path, line
            ))
        };
        let parts = split_manifest_args(line).ok_or_else(bad_line)?;
        if !parts.len().is_multiple_of(2) {
            return Err(bad_line());
        }
        // `key value` pairs in any order; unknown keys are ignored, as in Redis.
        let (mut file_name, mut seq, mut kind) = (None, None, None);
        for pair in parts.chunks(2) {
            match pair[0].as_str() {
                "file" => file_name = Some(pair[1].as_str()),
                "seq" => seq = Some(pair[1].as_str()),
                "type" => kind = Some(pair[1].as_str()),
                _ => {}
            }
        }
        let (Some(file_name), Some(seq), Some(kind)) = (file_name, seq, kind) else {
            return Err(bad_line());
        };
        let seq: u64 = match seq.parse::<u64>() {
            Ok(s) if s > 0 => s,
            _ => return Err(bad_line()),
        };
        // Like Redis: entries are file names inside the AOF dir, never paths.
        if file_name.is_empty()
            || file_name == "."
            || file_name == ".."
            || file_name.contains('/')
            || file_name.contains('\\')
        {
            return Err(invalid(format!(
                "File can't be a path, just a filename, in {:?}: {:?}",
                manifest_path, file_name
            )));
        }
        let file_path = parent.join(file_name);
        saw_any_entry = true;
        match kind {
            "b" => {
                if !base_files.is_empty() {
                    return Err(invalid(format!(
                        "Found duplicate base file information in {:?}",
                        manifest_path
                    )));
                }
                base_files.push((seq, file_path));
            }
            "i" => {
                if let Some(&(prev_seq, _)) = incr_files.last()
                    && seq <= prev_seq
                {
                    return Err(invalid(format!(
                        "Found a non-monotonic sequence number in {:?}: {:?}",
                        manifest_path, line
                    )));
                }
                incr_files.push((seq, file_path));
            }
            "h" => {} // history file: ignored by Redis on load
            other => {
                return Err(invalid(format!(
                    "Unknown AOF file type '{}' in {:?}: {:?}",
                    other, manifest_path, line
                )));
            }
        }
    }
    if !saw_any_entry {
        return Err(invalid(format!(
            "Found an empty AOF manifest in {:?}",
            manifest_path
        )));
    }
    let mut out: Vec<PathBuf> = base_files.into_iter().map(|(_, p)| p).collect();
    out.extend(incr_files.into_iter().map(|(_, p)| p));
    for p in &out {
        if !p.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Failed to open AOF file {:?}: No such file or directory", p),
            ));
        }
    }
    Ok(out)
}

/// Discovers Redis 7+ multi-part AOF (`appendonlydir/appendonly.aof.manifest`)
/// or legacy single-file Redis AOF (`appendonly.aof`) inside `dir`.
fn find_external_redis_aof_files(
    dir: &Path,
) -> std::io::Result<Option<(Option<PathBuf>, Vec<PathBuf>)>> {
    let manifest_in_subdir = dir.join("appendonlydir").join("appendonly.aof.manifest");
    if manifest_in_subdir.exists() {
        let files = parse_redis_aof_manifest(&manifest_in_subdir)?;
        return Ok(Some((Some(manifest_in_subdir), files)));
    }
    let manifest_in_dir = dir.join("appendonly.aof.manifest");
    if manifest_in_dir.exists() {
        let files = parse_redis_aof_manifest(&manifest_in_dir)?;
        return Ok(Some((Some(manifest_in_dir), files)));
    }
    let single_in_subdir = dir.join("appendonlydir").join("appendonly.aof");
    if single_in_subdir.exists() {
        return Ok(Some((None, vec![single_in_subdir])));
    }
    let single_in_dir = dir.join("appendonly.aof");
    if single_in_dir.exists() {
        return Ok(Some((None, vec![single_in_dir])));
    }
    Ok(None)
}

fn compute_external_aof_crc(manifest: &Option<PathBuf>, files: &[PathBuf]) -> u64 {
    let mut buf = Vec::new();
    if let Some(m) = manifest
        && let Ok(bytes) = std::fs::read(m)
    {
        buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(&bytes);
    }
    for f in files {
        // Basenames only: moving the whole dir must not look like a change.
        if let Some(name) = f.file_name() {
            buf.extend_from_slice(name.to_string_lossy().as_bytes());
        }
        if let Ok(bytes) = std::fs::read(f) {
            buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            buf.extend_from_slice(&bytes);
        }
    }
    crate::table::crc64(&buf)
}

/// Splits a canonical rewritten AOF buffer across `num_shards` per-shard buffers.
fn split_rewritten_aof_to_shards(
    rewritten: &[u8],
    num_shards: usize,
) -> std::io::Result<Vec<Vec<u8>>> {
    let invalid = |msg: String| std::io::Error::new(std::io::ErrorKind::InvalidData, msg);
    let mut outputs: Vec<Vec<u8>> = vec![Vec::new(); num_shards];
    let mut cur_db: usize = 0;
    let mut shard_db: Vec<usize> = vec![0; num_shards];
    let mut buf = BytesMut::from(rewritten);
    while !buf.is_empty() {
        let start = rewritten.len() - buf.len();
        let cmd = match crate::resp::parse_command_trusted(&mut buf) {
            Ok(Some(cmd)) => cmd,
            _ => return Err(invalid(format!("bad rewritten AOF at offset {}", start))),
        };
        if let Command::Select(idx) = &cmd {
            cur_db = *idx as usize;
            continue;
        }
        let raw = &rewritten[start..rewritten.len() - buf.len()];
        // Every shard needs the index definitions (they lead the rewrite,
        // in DB 0, before any SELECT).
        if matches!(cmd, Command::FtCreate { .. }) {
            for out in outputs.iter_mut() {
                out.extend_from_slice(raw);
            }
            continue;
        }
        let shard = crate::connection::target_shard_of_cmd(&cmd, num_shards)
            .or_else(|| raw_resp_arg(raw, 1).map(|k| crate::router::target_shard(k, num_shards)))
            .unwrap_or(0);
        if shard_db[shard] != cur_db {
            let db_str = cur_db.to_string();
            outputs[shard].extend_from_slice(
                format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).as_bytes(),
            );
            shard_db[shard] = cur_db;
        }
        outputs[shard].extend_from_slice(raw);
    }
    // The live writer appends assuming each file ends in DB 0 (it only logs
    // SELECT on a change), so leave every file there, as the rewrite does.
    for (out, db) in outputs.iter_mut().zip(shard_db) {
        if db != 0 {
            out.extend_from_slice(b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n");
        }
    }
    Ok(outputs)
}

/// Records how many shards wrote the `appendonly-{i}.aof` files in a dir.
pub const AOF_SHARDS_MANIFEST: &str = "appendonly.shards";
/// Present only while [`reshard_aof_dir`] is swapping files in; if it is
/// found at startup the swap was interrupted and needs a human.
pub const AOF_RESHARD_MARKER: &str = "appendonly.reshard-in-progress";
const AOF_RESHARD_STAGING: &str = ".aof-reshard-staging";

fn aof_shard_file_index(name: &str) -> Option<usize> {
    name.strip_prefix("appendonly-")?
        .strip_suffix(".aof")?
        .parse()
        .ok()
}

fn write_file_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    f.write_all(data)?;
    f.sync_all()
}

/// How keys were spread over the per-shard AOF files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AofLayout {
    shards: usize,
    /// Cluster mode routes by CRC16 slot, standalone by key hash.
    cluster_slots: bool,
}

impl AofLayout {
    fn current(shards: usize) -> Self {
        Self {
            shards,
            cluster_slots: crate::cluster::HAS_ACTIVE_CLUSTER
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    fn routing_name(&self) -> &'static str {
        if self.cluster_slots { "slots" } else { "hash" }
    }
}

fn write_aof_shards_manifest_with_ext(
    dir: &Path,
    layout: AofLayout,
    ext_crc: Option<u64>,
) -> std::io::Result<()> {
    let tmp = dir.join(format!("{}.tmp", AOF_SHARDS_MANIFEST));
    let text = match ext_crc {
        Some(crc) => format!(
            "shards {}\nrouting {}\next_crc {}\n",
            layout.shards,
            layout.routing_name(),
            crc
        ),
        None => format!(
            "shards {}\nrouting {}\n",
            layout.shards,
            layout.routing_name()
        ),
    };
    write_file_synced(&tmp, text.as_bytes())?;
    let target = dir.join(AOF_SHARDS_MANIFEST);
    std::fs::rename(&tmp, &target)?;
    sync_parent_dir(&target)
}

fn read_aof_shards_manifest_ext_crc(dir: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(dir.join(AOF_SHARDS_MANIFEST)).ok()?;
    for line in text.lines() {
        if let Some(("ext_crc", v)) = line.trim().split_once(' ') {
            return v.trim().parse::<u64>().ok();
        }
    }
    None
}

fn read_aof_shards_manifest(dir: &Path) -> std::io::Result<Option<AofLayout>> {
    let path = dir.join(AOF_SHARDS_MANIFEST);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut shards = None;
    let mut cluster_slots = None;
    for line in text.lines() {
        match line.trim().split_once(' ') {
            Some(("shards", n)) => shards = n.trim().parse::<usize>().ok().filter(|n| *n > 0),
            Some(("routing", "hash")) => cluster_slots = Some(false),
            Some(("routing", "slots")) => cluster_slots = Some(true),
            _ => {}
        }
    }
    match (shards, cluster_slots) {
        (Some(shards), Some(cluster_slots)) => Ok(Some(AofLayout {
            shards,
            cluster_slots,
        })),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("bad AOF shard manifest {:?}: {:?}", path, text),
        )),
    }
}

/// Returns argument `idx` of a raw RESP array command.
fn raw_resp_arg(raw: &[u8], idx: usize) -> Option<&[u8]> {
    fn line(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
        let end = pos + buf.get(pos..)?.windows(2).position(|w| w == b"\r\n")?;
        Some((&buf[pos..end], end + 2))
    }
    let (hdr, mut pos) = line(raw, 0)?;
    let argc: usize = std::str::from_utf8(hdr.strip_prefix(b"*")?)
        .ok()?
        .parse()
        .ok()?;
    if idx >= argc {
        return None;
    }
    for i in 0..=idx {
        let (len_line, next) = line(raw, pos)?;
        let len: usize = std::str::from_utf8(len_line.strip_prefix(b"$")?)
            .ok()?
            .parse()
            .ok()?;
        let arg = raw.get(next..next + len)?;
        if i == idx {
            return Some(arg);
        }
        pos = next + len + 2;
    }
    None
}

/// Makes the per-shard AOF files in `dir` match `num_shards`.
///
/// Each shard replays only `appendonly-{shard}.aof`, and keys are owned by
/// `hash(key) % num_shards`, so restarting with a different `--threads` would
/// load keys into shards that never look them up (or drop whole files). When
/// the count changed, this replays every old file into one in-memory db,
/// rewrites it, and splits the rewrite by key owner into new per-shard files.
/// The old files are kept in a backup dir. The shard count and routing scheme
/// (key hash, or cluster slots) are recorded in [`AOF_SHARDS_MANIFEST`]; dirs
/// from before the manifest existed are assumed to have one file per shard
/// (the writer creates every shard's file) and the current routing scheme.
///
/// Returns the old shard count if a reshard happened.
pub fn reshard_aof_dir(dir: &Path, num_shards: usize, port: u16) -> std::io::Result<Option<usize>> {
    remove_stale_rewrite_temps(dir);
    let invalid = |msg: String| std::io::Error::new(std::io::ErrorKind::InvalidData, msg);
    let marker = dir.join(AOF_RESHARD_MARKER);
    if marker.exists() {
        let backup = std::fs::read_to_string(&marker).unwrap_or_default();
        return Err(invalid(format!(
            "a previous AOF reshard in {:?} was interrupted. The original AOF files are in {:?}. \
             Restore them into {:?} (removing any appendonly-*.aof there), delete {:?}, and restart",
            dir,
            backup.trim(),
            dir,
            marker
        )));
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut max_index: Option<usize> = None;
    for entry in entries {
        let name = entry?.file_name();
        if let Some(i) = name.to_str().and_then(aof_shard_file_index) {
            max_index = Some(max_index.map_or(i, |m| m.max(i)));
        }
    }
    // Leftovers from a reshard that crashed before touching the real files.
    let staging = dir.join(AOF_RESHARD_STAGING);
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }

    let new_layout = AofLayout::current(num_shards);
    let prev_ext_crc = read_aof_shards_manifest_ext_crc(dir);
    // AOF files written by Redis (a Redis 7+ multi-part manifest, or a single
    // `appendonly.aof`) are imported into per-shard files the first time they
    // are seen, and again only if they change. If they later disappear the
    // imported shard files are kept: they hold every write since.
    if let Some((manifest_opt, ext_files)) = find_external_redis_aof_files(dir)? {
        let cur_crc = compute_external_aof_crc(&manifest_opt, &ext_files);
        if prev_ext_crc != Some(cur_crc) {
            let rudis_has_data = max_index.is_some_and(|max| {
                (0..=max).any(|i| {
                    dir.join(format!("appendonly-{}.aof", i))
                        .metadata()
                        .is_ok_and(|m| m.len() > 0)
                })
            });
            if rudis_has_data && prev_ext_crc.is_none() {
                return Err(invalid(format!(
                    "{:?} holds both Redis AOF files ({:?}) and non-empty rudis appendonly-*.aof \
                     files that were not imported from them. Refusing to guess which to load: move \
                     one set away and restart",
                    dir,
                    manifest_opt.as_ref().unwrap_or(&ext_files[0])
                )));
            }
            let old_shards = read_aof_shards_manifest(dir)?
                .map(|l| l.shards)
                .or(max_index.map(|i| i + 1))
                .unwrap_or(0);
            import_external_aof(&ext_files, num_shards, port, &staging)?;
            swap_in_staged_aof(
                dir,
                &staging,
                &marker,
                old_shards,
                new_layout,
                Some(cur_crc),
                &format!("aof-import-backup-{}", unix_ms_now()),
            )?;
            crate::log_notice!(
                "Imported the Redis AOF files {:?} into {} shard AOF files",
                ext_files,
                num_shards
            );
            return Ok(None);
        }
    }

    let old_layout = match (read_aof_shards_manifest(dir)?, max_index) {
        (Some(l), Some(i)) if i >= l.shards => {
            return Err(invalid(format!(
                "{:?} says {} shards but appendonly-{}.aof exists",
                dir.join(AOF_SHARDS_MANIFEST),
                l.shards,
                i
            )));
        }
        (Some(l), _) => l,
        (None, Some(i)) => AofLayout {
            shards: i + 1,
            ..new_layout
        },
        (None, None) => new_layout,
    };
    let old_shards = old_layout.shards;
    if old_layout == new_layout {
        if !dir.join(AOF_SHARDS_MANIFEST).exists() {
            write_aof_shards_manifest_with_ext(dir, new_layout, prev_ext_crc)?;
        }
        return Ok(None);
    }

    // 1. Load everything. Each key only ever lived in one file, so its
    //    history replays in order.
    let mut combined = ShardDb::new(port);
    for i in 0..old_shards {
        replay_aof(&dir.join(format!("appendonly-{}.aof", i)), &mut combined)?;
    }

    // 2. Rewrite to canonical per-key commands and split them by owner.
    std::fs::create_dir_all(&staging)?;
    rewrite_shard_aof_canonical(&mut combined, &staging, 0)?;
    // Replaying registered the indexes globally; the shards register them
    // again when they replay their new files.
    for name in combined.search_indices.keys() {
        let _ = crate::search::drop_search_index(name);
    }
    drop(combined);
    let rewritten = std::fs::read(staging.join("appendonly-0.aof"))?;
    let outputs = split_rewritten_aof_to_shards(&rewritten, num_shards)?;
    for (i, out) in outputs.iter().enumerate() {
        write_file_synced(&staging.join(format!("appendonly-{}.aof", i)), out)?;
    }
    sync_parent_dir(&staging.join("appendonly-0.aof"))?;

    // 3. Swap: old files to a backup dir, new files in, manifest last.
    swap_in_staged_aof(
        dir,
        &staging,
        &marker,
        old_shards,
        new_layout,
        prev_ext_crc,
        &format!(
            "aof-reshard-backup-{}to{}-{}",
            old_shards,
            num_shards,
            unix_ms_now()
        ),
    )?;
    Ok(Some(old_shards))
}

fn unix_ms_now() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Replays AOF files written by Redis, in order, into one in-memory db and
/// writes it to `staging` as `appendonly-{i}.aof` files split by key owner.
///
/// Replay follows Redis's loading rules (nothing expires mid-load; see
/// [`crate::connection::REPLAYING_WRITES`]); keys already expired at the
/// end are left out of the rewrite.
fn import_external_aof(
    ext_files: &[PathBuf],
    num_shards: usize,
    port: u16,
    staging: &Path,
) -> std::io::Result<()> {
    let mut combined = ShardDb::new(port);
    let replayed = (|| {
        for (idx, ext_path) in ext_files.iter().enumerate() {
            // Only the newest file can end in a torn write.
            let allow_trunc = idx + 1 == ext_files.len() && aof_load_truncated();
            replay_aof_with(ext_path, &mut combined, allow_trunc)?;
        }
        Ok::<(), std::io::Error>(())
    })();
    replayed?;
    std::fs::create_dir_all(staging)?;
    rewrite_shard_aof_canonical(&mut combined, staging, 0)?;
    // Replaying registered the indexes globally; the shards register them
    // again when they replay their new files.
    for name in combined.search_indices.keys() {
        let _ = crate::search::drop_search_index(name);
    }
    drop(combined);
    let rewritten = std::fs::read(staging.join("appendonly-0.aof"))?;
    let outputs = split_rewritten_aof_to_shards(&rewritten, num_shards)?;
    for (i, out) in outputs.iter().enumerate() {
        write_file_synced(&staging.join(format!("appendonly-{}.aof", i)), out)?;
    }
    sync_parent_dir(&staging.join("appendonly-0.aof"))
}

/// Moves the current `appendonly-{0..old_shards}.aof` files into a backup
/// dir named `backup_name`, moves the staged per-shard files in, and writes
/// the shards manifest last. A marker file naming the backup exists for the
/// whole swap, so an interrupted swap is detected at the next start.
fn swap_in_staged_aof(
    dir: &Path,
    staging: &Path,
    marker: &Path,
    old_shards: usize,
    layout: AofLayout,
    ext_crc: Option<u64>,
    backup_name: &str,
) -> std::io::Result<()> {
    let backup = dir.join(backup_name);
    write_file_synced(marker, backup.to_string_lossy().as_bytes())?;
    sync_parent_dir(marker)?;
    std::fs::create_dir_all(&backup)?;
    for i in 0..old_shards {
        let name = format!("appendonly-{}.aof", i);
        let old = dir.join(&name);
        if old.exists() {
            std::fs::rename(&old, backup.join(&name))?;
        }
    }
    sync_parent_dir(&backup.join("x"))?;
    for i in 0..layout.shards {
        let name = format!("appendonly-{}.aof", i);
        std::fs::rename(staging.join(&name), dir.join(&name))?;
    }
    write_aof_shards_manifest_with_ext(dir, layout, ext_crc)?;
    std::fs::remove_file(marker)?;
    sync_parent_dir(marker)?;
    let _ = std::fs::remove_dir_all(staging);
    Ok(())
}

pub fn rewrite_shard_aof(db: &mut ShardDb, dir: &Path, shard_id: usize) -> std::io::Result<usize> {
    static TMP_REWRITE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let tmp_id = TMP_REWRITE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp_path = dir.join(format!(
        "appendonly-{}.aof.tmp.{}_{}",
        shard_id,
        std::process::id(),
        tmp_id
    ));
    let target_path = dir.join(format!("appendonly-{}.aof", shard_id));
    let orig_db = db.current_db;
    let res = write_rewritten_aof(db, &tmp_path, &target_path);
    if res.is_err() {
        // Don't leave a partial rewrite behind; the live AOF is untouched.
        let _ = std::fs::remove_file(&tmp_path);
        // A failure mid-way can leave another DB selected.
        db.select_db(orig_db);
    }
    res
}

/// Like [`rewrite_shard_aof`], but as canonical commands, which resharding
/// can split by key owner.
pub fn rewrite_shard_aof_canonical(
    db: &mut ShardDb,
    dir: &Path,
    shard_id: usize,
) -> std::io::Result<usize> {
    static TMP_REWRITE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let tmp_id = TMP_REWRITE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp_path = dir.join(format!(
        "appendonly-{}.aof.tmp.{}_{}",
        shard_id,
        std::process::id(),
        tmp_id
    ));
    let target_path = dir.join(format!("appendonly-{}.aof", shard_id));
    let orig_db = db.current_db;
    let res = write_canonical_aof(db, &tmp_path, &target_path);
    if res.is_err() {
        // Don't leave a partial rewrite behind; the live AOF is untouched.
        let _ = std::fs::remove_file(&tmp_path);
        // A failure mid-way can leave another DB selected.
        db.select_db(orig_db);
    }
    res
}

/// Writes `db` as an AOF to `tmp_path`, fsyncs it and renames it to
/// `target_path`. The AOF is an RDB image of the shard (an RDB preamble,
/// like Redis's default `aof-use-rdb-preamble yes`): it holds everything
/// a snapshot does, function libraries included. Returns the number of
/// keys written.
fn write_rewritten_aof(
    db: &mut ShardDb,
    tmp_path: &Path,
    target_path: &Path,
) -> std::io::Result<usize> {
    let keys = db
        .keyspace_stats_by_db()
        .iter()
        .map(|&(_, k, _, _)| k)
        .sum();
    let mut image =
        crate::shard::AofImage::start(db).map_err(|e| std::io::Error::other(e.to_string()))?;
    let mut writer = ImageFileWriter::create(tmp_path)?;
    loop {
        let (piece, done) = image
            .step(db, std::time::Duration::MAX, IMAGE_FLUSH_BYTES)
            .map_err(std::io::Error::other)?;
        if let Some(piece) = piece {
            writer.write(&piece)?;
        }
        if done {
            break;
        }
    }
    writer.write(&image.finish().map_err(std::io::Error::other)?)?;
    drop(writer.finish()?);
    std::fs::rename(tmp_path, target_path)?;
    let _ = sync_parent_dir(target_path);
    Ok(keys)
}

/// Bytes of an AOF rewrite image buffered before they are written out.
const IMAGE_FLUSH_BYTES: usize = 4 << 20;

/// Writes an RDB image to a file, computing the CRC64 the RDB format ends
/// with as it goes.
struct ImageFileWriter {
    file: std::io::BufWriter<std::fs::File>,
    crc: u64,
}

impl ImageFileWriter {
    fn create(path: &Path) -> std::io::Result<Self> {
        Ok(ImageFileWriter {
            file: std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(path)?),
            crc: 0,
        })
    }

    fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        self.crc = crate::table::crc64_update(self.crc, bytes);
        self.file.write_all(bytes)
    }

    /// Appends the CRC64 and fsyncs; returns the file, positioned at its end.
    fn finish(mut self) -> std::io::Result<std::fs::File> {
        use std::io::Write;
        self.file.write_all(&self.crc.to_le_bytes())?;
        let file = self.file.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        Ok(file)
    }
}

/// Writes `db` as canonical commands (one write per key or structure) to
/// `tmp_path`, fsyncs it and renames it to `target_path`. Resharding splits
/// such a file by key owner (see [`split_rewritten_aof_to_shards`]).
fn write_canonical_aof(
    db: &mut ShardDb,
    tmp_path: &Path,
    target_path: &Path,
) -> std::io::Result<usize> {
    use std::io::Write;
    let mut count = 0;
    let now = std::time::Instant::now();
    let unix_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let orig_db = db.current_db;
    let max_db = db.max_db_len();

    let file = std::fs::File::create(tmp_path)?;
    let mut writer = std::io::BufWriter::with_capacity(65536, file);
    let mut emitted_db: usize = 0;

    // 0. Search index definitions, before the keys: replaying the key
    //    writes after them indexes the documents again.
    let mut index_names: Vec<&String> = db.search_indices.keys().collect();
    index_names.sort();
    for name in index_names {
        if let Some(schema) = &db.search_indices[name].schema {
            writer.write_all(&resp_argv(&schema.ft_create_args()))?;
            count += 1;
        }
    }

    // 1. Snapshot all non-expired entries across all DBs in RudisTable as canonical RESP commands
    for db_idx in 0..max_db {
        if db_idx > 0 && db.table_for_db(db_idx).is_none_or(|t| t.is_empty()) {
            continue;
        }
        db.select_db(db_idx);
        if !db.table.hash_field_expires.is_empty() {
            let keys_with_field_exp: Vec<bytes::Bytes> =
                db.table.hash_field_expires.keys().cloned().collect();
            for k in keys_with_field_exp {
                db.table.purge_expired_hash_fields(&k);
            }
        }
        if db.table.dbsize() == 0 {
            continue;
        }
        if emitted_db != db_idx {
            let db_str = db_idx.to_string();
            writer.write_all(
                format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).as_bytes(),
            )?;
            emitted_db = db_idx;
        }
        for entry in db.table.entries() {
            if let Some(exp) = entry.expire_at()
                && exp <= now
            {
                continue;
            }
            let k = entry.key.as_ref();
            let hydrated_val;
            let val_ref = match &entry.val {
                crate::table::RudisValue::Cooled(cv) => &cv.val,
                crate::table::RudisValue::Tiered(ptr) => match db.hydrate_tiered(**ptr) {
                    Some(v) => {
                        hydrated_val = v;
                        &hydrated_val
                    }
                    None => {
                        // Fail the rewrite: renaming a file without this key
                        // over the complete old AOF would lose it for good.
                        let err = crate::shard::TieredReadError {
                            key: bytes::Bytes::copy_from_slice(&entry.key),
                        };
                        tracing::error!("AOF rewrite failed: {}", err);
                        return Err(std::io::Error::other(err.to_string()));
                    }
                },
                other => other,
            };
            match val_ref {
                crate::table::RudisValue::String(val) => {
                    if let Some(exp) = entry.expire_at() {
                        let rem_ms_str =
                            unix_ms_after(exp.saturating_duration_since(now)).to_string();
                        writer.write_all(b"*5\r\n$3\r\nSET\r\n$")?;
                        writer.write_all(k.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(k)?;
                        writer.write_all(b"\r\n$")?;
                        writer.write_all(val.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(&val.view())?;
                        writer.write_all(b"\r\n$4\r\nPXAT\r\n$")?;
                        writer.write_all(rem_ms_str.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(rem_ms_str.as_bytes())?;
                        writer.write_all(b"\r\n")?;
                    } else {
                        writer.write_all(b"*3\r\n$3\r\nSET\r\n$")?;
                        writer.write_all(k.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(k)?;
                        writer.write_all(b"\r\n$")?;
                        writer.write_all(val.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(&val.view())?;
                        writer.write_all(b"\r\n")?;
                    }
                    count += 1;
                }
                crate::table::RudisValue::Int(val) => {
                    let val_str = val.to_string();
                    writer.write_all(b"*3\r\n$3\r\nSET\r\n$")?;
                    writer.write_all(k.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n$")?;
                    writer.write_all(val_str.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(val_str.as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    count += 1;
                }
                crate::table::RudisValue::SmallHash(pairs) if !pairs.is_empty() => {
                    writer.write_all(
                        format!("*{}\r\n$4\r\nHSET\r\n${}\r\n", pairs.len() * 2 + 2, k.len())
                            .as_bytes(),
                    )?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n")?;
                    for (field, v) in pairs.iter() {
                        writer.write_all(format!("${}\r\n", field.len()).as_bytes())?;
                        writer.write_all(field.as_ref())?;
                        writer.write_all(format!("\r\n${}\r\n", v.len()).as_bytes())?;
                        writer.write_all(v.as_ref())?;
                        writer.write_all(b"\r\n")?;
                    }
                    count += 1;
                }
                crate::table::RudisValue::Hash(map) if !map.is_empty() => {
                    writer.write_all(
                        format!("*{}\r\n$4\r\nHSET\r\n${}\r\n", map.len() * 2 + 2, k.len())
                            .as_bytes(),
                    )?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n")?;
                    for (field, v) in map.as_ref() {
                        writer.write_all(format!("${}\r\n", field.len()).as_bytes())?;
                        writer.write_all(field.as_ref())?;
                        writer.write_all(format!("\r\n${}\r\n", v.len()).as_bytes())?;
                        writer.write_all(v.as_ref())?;
                        writer.write_all(b"\r\n")?;
                    }
                    count += 1;
                }
                crate::table::RudisValue::List(list) if !list.is_empty() => {
                    writer.write_all(
                        format!("*{}\r\n$5\r\nRPUSH\r\n${}\r\n", list.len() + 2, k.len())
                            .as_bytes(),
                    )?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n")?;
                    for item in list.iter() {
                        writer.write_all(format!("${}\r\n", item.len()).as_bytes())?;
                        writer.write_all(item.as_ref())?;
                        writer.write_all(b"\r\n")?;
                    }
                    count += 1;
                }
                crate::table::RudisValue::Set(set) if !set.is_empty() => {
                    let members: Vec<bytes::Bytes> = set.to_vec();
                    writer.write_all(
                        format!("*{}\r\n$4\r\nSADD\r\n${}\r\n", members.len() + 2, k.len())
                            .as_bytes(),
                    )?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n")?;
                    for m in members {
                        writer.write_all(format!("${}\r\n", m.len()).as_bytes())?;
                        writer.write_all(m.as_ref())?;
                        writer.write_all(b"\r\n")?;
                    }
                    count += 1;
                }
                crate::table::RudisValue::ZSet(zset) if !zset.is_empty() => {
                    let elements = zset.to_vec();
                    writer.write_all(
                        format!(
                            "*{}\r\n$4\r\nZADD\r\n${}\r\n",
                            elements.len() * 2 + 2,
                            k.len()
                        )
                        .as_bytes(),
                    )?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n")?;
                    for (member, score) in elements {
                        let score_str = score.to_string();
                        writer.write_all(
                            format!(
                                "${}\r\n{}\r\n${}\r\n",
                                score_str.len(),
                                score_str,
                                member.len()
                            )
                            .as_bytes(),
                        )?;
                        writer.write_all(member.as_ref())?;
                        writer.write_all(b"\r\n")?;
                    }
                    count += 1;
                }
                crate::table::RudisValue::HyperLogLog(hll) => {
                    writer.write_all(b"*3\r\n$3\r\nSET\r\n$")?;
                    writer.write_all(k.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n$")?;
                    writer.write_all(hll.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(hll.as_ref())?;
                    writer.write_all(b"\r\n")?;
                    count += 1;
                }
                crate::table::RudisValue::Stream(stream) => {
                    if !stream.entries.is_empty() {
                        for (sid, fields) in &stream.entries {
                            let id_str = sid.to_string();
                            let num_args = 2 + 1 + fields.len() * 2;
                            writer.write_all(
                                format!("*{}\r\n$4\r\nXADD\r\n${}\r\n", num_args, k.len())
                                    .as_bytes(),
                            )?;
                            writer.write_all(k)?;
                            writer.write_all(
                                format!("\r\n${}\r\n{}\r\n", id_str.len(), id_str).as_bytes(),
                            )?;
                            for (f, v) in fields {
                                writer.write_all(format!("${}\r\n", f.len()).as_bytes())?;
                                writer.write_all(f.as_ref())?;
                                writer.write_all(format!("\r\n${}\r\n", v.len()).as_bytes())?;
                                writer.write_all(v.as_ref())?;
                                writer.write_all(b"\r\n")?;
                            }
                            count += 1;
                        }
                    }
                    if stream.last_id != crate::table::StreamId::default()
                        || stream.entries_added > 0
                    {
                        let last_id_str = stream.last_id.to_string();
                        let ea_str = stream.entries_added.to_string();
                        let md_str = stream.max_deleted_entry_id.to_string();
                        writer.write_all(
                            format!("*7\r\n$6\r\nXSETID\r\n${}\r\n", k.len()).as_bytes(),
                        )?;
                        writer.write_all(k)?;
                        writer.write_all(
                        format!(
                            "\r\n${}\r\n{}\r\n$12\r\nENTRIESADDED\r\n${}\r\n{}\r\n$12\r\nMAXDELETEDID\r\n${}\r\n{}\r\n",
                            last_id_str.len(), last_id_str,
                            ea_str.len(), ea_str,
                            md_str.len(), md_str
                        ).as_bytes()
                    )?;
                        count += 1;
                    }
                    for grp in stream.groups.values() {
                        let last_deliv_str = grp.last_delivered_id.to_string();
                        if let Some(er) = grp.entries_read {
                            let er_str = er.to_string();
                            writer.write_all(
                                format!("*7\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n", k.len())
                                    .as_bytes(),
                            )?;
                            writer.write_all(k)?;
                            writer.write_all(format!("\r\n${}\r\n", grp.name.len()).as_bytes())?;
                            writer.write_all(&grp.name)?;
                            writer.write_all(
                                format!(
                                    "\r\n${}\r\n{}\r\n$11\r\nENTRIESREAD\r\n${}\r\n{}\r\n",
                                    last_deliv_str.len(),
                                    last_deliv_str,
                                    er_str.len(),
                                    er_str
                                )
                                .as_bytes(),
                            )?;
                        } else {
                            writer.write_all(
                                format!("*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n", k.len())
                                    .as_bytes(),
                            )?;
                            writer.write_all(k)?;
                            writer.write_all(format!("\r\n${}\r\n", grp.name.len()).as_bytes())?;
                            writer.write_all(&grp.name)?;
                            writer.write_all(
                                format!("\r\n${}\r\n{}\r\n", last_deliv_str.len(), last_deliv_str)
                                    .as_bytes(),
                            )?;
                        }
                        count += 1;

                        for (c_name, cons) in &grp.consumers {
                            for (&sid, &deliv_time) in &cons.pel {
                                let pe = match grp.pel.get(&sid) {
                                    Some(p) => p,
                                    None => continue,
                                };
                                let sid_str = sid.to_string();
                                let dt_str = deliv_time.to_string();
                                let rc_str = pe.delivery_count.to_string();
                                writer.write_all(
                                    format!("*10\r\n$6\r\nXCLAIM\r\n${}\r\n", k.len()).as_bytes(),
                                )?;
                                writer.write_all(k)?;
                                writer
                                    .write_all(format!("\r\n${}\r\n", grp.name.len()).as_bytes())?;
                                writer.write_all(&grp.name)?;
                                writer
                                    .write_all(format!("\r\n${}\r\n", c_name.len()).as_bytes())?;
                                writer.write_all(c_name)?;
                                writer.write_all(
                                format!(
                                    "\r\n$1\r\n0\r\n${}\r\n{}\r\n$4\r\nTIME\r\n${}\r\n{}\r\n$10\r\nRETRYCOUNT\r\n${}\r\n{}\r\n$6\r\nJUSTID\r\n",
                                    sid_str.len(), sid_str,
                                    dt_str.len(), dt_str,
                                    rc_str.len(), rc_str,
                                ).as_bytes()
                            )?;
                                count += 1;
                            }
                        }

                        for (&sid, pe) in &grp.pel {
                            if pe.consumer.is_empty() {
                                let sid_str = sid.to_string();
                                let rc_str = pe.delivery_count.to_string();
                                writer.write_all(
                                    format!("*10\r\n$5\r\nXNACK\r\n${}\r\n", k.len()).as_bytes(),
                                )?;
                                writer.write_all(k)?;
                                writer
                                    .write_all(format!("\r\n${}\r\n", grp.name.len()).as_bytes())?;
                                writer.write_all(&grp.name)?;
                                writer.write_all(
                                format!(
                                    "\r\n$4\r\nFAIL\r\n$3\r\nIDS\r\n$1\r\n1\r\n${}\r\n{}\r\n$10\r\nRETRYCOUNT\r\n${}\r\n{}\r\n$5\r\nFORCE\r\n",
                                    sid_str.len(), sid_str,
                                    rc_str.len(), rc_str,
                                ).as_bytes()
                            )?;
                                count += 1;
                            }
                        }
                    }
                }
                _ => {}
            }

            if !matches!(val_ref, crate::table::RudisValue::String(_))
                && let Some(exp) = entry.expire_at()
            {
                let rem_ms_str = unix_ms_after(exp.saturating_duration_since(now)).to_string();
                writer.write_all(b"*3\r\n$9\r\nPEXPIREAT\r\n$")?;
                writer.write_all(k.len().to_string().as_bytes())?;
                writer.write_all(b"\r\n")?;
                writer.write_all(k)?;
                writer.write_all(b"\r\n$")?;
                writer.write_all(rem_ms_str.len().to_string().as_bytes())?;
                writer.write_all(b"\r\n")?;
                writer.write_all(rem_ms_str.as_bytes())?;
                writer.write_all(b"\r\n")?;
            }

            if !db.table.hash_field_expires.is_empty()
                && let Some(fmap) = db.table.hash_field_expires.get(k)
            {
                for (field, &exp) in fmap {
                    if exp > now {
                        let rem_ms = exp.duration_since(now).as_millis() as u64;
                        let exp_unix_ms_str = (unix_now + rem_ms).to_string();
                        writer.write_all(b"*6\r\n$10\r\nHPEXPIREAT\r\n$")?;
                        writer.write_all(k.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(k)?;
                        writer.write_all(b"\r\n$")?;
                        writer.write_all(exp_unix_ms_str.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(exp_unix_ms_str.as_bytes())?;
                        writer.write_all(b"\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$")?;
                        writer.write_all(field.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(field.as_ref())?;
                        writer.write_all(b"\r\n")?;
                    }
                }
            }
        }
    }
    db.select_db(orig_db);
    if emitted_db != 0 {
        writer.write_all(b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n")?;
    }

    // 2. Snapshot JSON documents
    for (k, doc) in db.json_store.iter() {
        let doc_str = serde_json::to_string(doc).unwrap_or_default();
        writer.write_all(format!("*4\r\n$8\r\nJSON.SET\r\n${}\r\n", k.len()).as_bytes())?;
        writer.write_all(k.as_ref())?;
        writer.write_all(
            format!("\r\n$1\r\n$\r\n${}\r\n{}\r\n", doc_str.len(), doc_str).as_bytes(),
        )?;
        count += 1;
    }

    // 3. Snapshot Vector sets / HNSW indexes
    for (name, index) in &db.vector_indexes {
        for (elem, &node_id) in &index.key_to_id {
            if let Some(Some(node)) = index.nodes.get(node_id) {
                let cmd = Command::Vadd {
                    key: bytes::Bytes::from(name.clone()),
                    element: elem.clone(),
                    vector: index.node_vector_cow(node).into_owned(),
                    metric: Some(index.metric),
                    quantize: node.quantized.is_some(),
                    pq: node.pq.is_some(),
                    tiered: node.is_tiered,
                    reduce: None,
                    quant: Some(index.quant),
                    ef: None,
                    setattr: index.attributes.get(elem).cloned(),
                    m: Some(index.m),
                    cas: false,
                    is_redis_vset: index.is_redis_vset,
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 4. Snapshot Semantic Caches (SEMANTIC.*)
    for (ns, cache) in &db.semantic_caches {
        for (id, entry) in &cache.entries {
            if let Some(exp) = entry.expire_at
                && exp <= now
            {
                continue;
            }
            let ttl = entry
                .expire_at
                .map(|exp| exp.saturating_duration_since(now));
            if let Some(&node_id) = cache.index.key_to_id.get(id)
                && let Some(Some(node)) = cache.index.nodes.get(node_id)
            {
                let cmd = Command::SemanticSet {
                    namespace: ns.clone(),
                    id: id.clone(),
                    prompt: entry.prompt.clone(),
                    response: entry.response.clone(),
                    vector: Box::new(cache.index.node_vector_cow(node).into_owned()),
                    ttl,
                    scope: entry.scope.clone().map(Box::new),
                    quantize: node.quantized.is_some(),
                    tokens: Some(entry.tokens),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 5. Snapshot Agent Memory Sessions (AGENT.MEM.*)
    for (sess_key, session) in &db.agent_memories {
        let get_turn_vec = |turn_id: u64| -> Option<Vec<f32>> {
            session
                .index
                .as_ref()
                .and_then(|idx| idx.get_vector_cow(&bytes::Bytes::from(turn_id.to_string())))
                .map(|c| c.into_owned())
        };
        let has_compacted = session.turns.iter().any(|t| t.compacted);
        if has_compacted {
            for turn in session.turns.iter().filter(|t| t.compacted) {
                let cmd = Command::AgentMemAdd {
                    session: sess_key.clone(),
                    role: turn.role.clone(),
                    content: turn.content.clone(),
                    tokens: Some(turn.tokens),
                    vector: get_turn_vec(turn.id),
                    meta: turn.meta.clone(),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
            let mut active_iter = session.turns.iter().filter(|t| !t.compacted);
            if let Some(summary_turn) = active_iter.next() {
                let compact_cmd = Command::AgentMemCompact {
                    session: sess_key.clone(),
                    keep_recent: 0,
                    summary: summary_turn.content.clone(),
                    tokens: Some(summary_turn.tokens),
                    vector: get_turn_vec(summary_turn.id),
                };
                if let Some(resp) = command_to_resp(&compact_cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
                for turn in active_iter {
                    let cmd = Command::AgentMemAdd {
                        session: sess_key.clone(),
                        role: turn.role.clone(),
                        content: turn.content.clone(),
                        tokens: Some(turn.tokens),
                        vector: get_turn_vec(turn.id),
                        meta: turn.meta.clone(),
                    };
                    if let Some(resp) = command_to_resp(&cmd) {
                        writer.write_all(&resp)?;
                        count += 1;
                    }
                }
            }
        } else {
            for turn in &session.turns {
                let cmd = Command::AgentMemAdd {
                    session: sess_key.clone(),
                    role: turn.role.clone(),
                    content: turn.content.clone(),
                    tokens: Some(turn.tokens),
                    vector: get_turn_vec(turn.id),
                    meta: turn.meta.clone(),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 6. Snapshot Agent Checkpoints (AGENT.CHECKPOINT.*)
    for (ck_key, thread) in &db.agent_checkpoints {
        for step_id in &thread.order {
            if let Some(node) = thread.nodes.get(step_id) {
                let cmd = Command::AgentCheckpointPut {
                    key: ck_key.clone(),
                    step_id: node.step_id.clone(),
                    parent_id: node.parent_id.clone(),
                    state: node.state.clone(),
                    meta: node.metadata.clone(),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 7. Snapshot Agent Tool Registries (AGENT.TOOL.*)
    for (tool_key, reg) in &db.agent_tools {
        for (call_id, entry) in &reg.calls {
            if let Some(exp) = entry.expire_at
                && exp <= now
            {
                continue;
            }
            if let Some(ref output) = entry.output {
                let ttl_ms = entry
                    .expire_at
                    .map(|exp| exp.saturating_duration_since(now).as_millis().max(1) as u64);
                let cmd = Command::AgentToolComplete {
                    key: tool_key.clone(),
                    call_id: call_id.clone(),
                    output: output.clone(),
                    ttl_ms,
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            } else if let Some(lease_until) = entry.lease_until
                && lease_until > now
            {
                let rem_ms = lease_until.duration_since(now).as_millis().max(1) as u64;
                let cmd = Command::AgentToolClaim {
                    key: tool_key.clone(),
                    call_id: call_id.clone(),
                    ttl_ms: rem_ms,
                    input: entry.input.clone(),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 8. Snapshot probabilistic structures (BF/CF/CMS/TOPK.*) as their raw
    //    state: Bloom and Count-Min contents can't be rebuilt from commands.
    for (kind, key, payload) in db.probabilistic_store.encoded_entries() {
        let cmd = Command::ProbRestore {
            kind,
            key: key.clone(),
            payload: payload.into(),
        };
        if let Some(resp) = command_to_resp(&cmd) {
            writer.write_all(&resp)?;
            count += 1;
        }
    }

    // 9. Snapshot CRDT state as one CRDT.MERGE per register, counter and set
    //    (tombstones included), the same form CRDT writes are logged in.
    for payload in db.crdt_store.export_entry_payloads() {
        if let Some(resp) = command_to_resp(&Command::CrdtMerge(payload.into())) {
            writer.write_all(&resp)?;
            count += 1;
        }
    }

    writer.flush()?;
    let file = writer.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()?;
    std::fs::rename(tmp_path, target_path)?;
    let _ = sync_parent_dir(target_path);

    Ok(count)
}

/// Removes the temporary files of AOF rewrites a crash or kill -9 cut
/// short (`appendonly-N.aof.tmp.*`); they are never read and, as rewrites
/// now run for a while in the background, would pile up. Startup only: no
/// rewrite is running yet.
fn remove_stale_rewrite_temps(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("appendonly-")
            && name.contains(".aof.tmp.")
            && std::fs::remove_file(entry.path()).is_ok()
        {
            crate::log_notice!("Removed {} left by an interrupted AOF rewrite", name);
        }
    }
}

/// Flushes directory metadata so that recently renamed files are durable across sudden power loss
pub fn sync_parent_dir(path: &std::path::Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        let dir_path = if parent.as_os_str().is_empty() {
            std::path::Path::new(".")
        } else {
            parent
        };
        if let Ok(dir_file) = std::fs::File::open(dir_path) {
            dir_file.sync_all()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The commands after an AOF's preamble are parsed a chunk at a time:
    /// replaying a 64 MB tail that keeps overwriting one key stays far below
    /// the tail's size in memory (it was copied whole first), and commands,
    /// `#TS` annotations and a MULTI block that straddle chunk boundaries
    /// still apply.
    #[test]
    fn test_replay_parses_the_tail_in_chunks() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-chunks-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("appendonly-0.aof");
        let mut log = Vec::new();
        let value = "v".repeat(1 << 20);
        let mut i = 0;
        while log.len() < 64 << 20 {
            log.extend_from_slice(&resp_argv(&["SET", "big", &value]));
            log.extend_from_slice(format!("#TS:{i}\r\n").as_bytes());
            log.extend_from_slice(&resp_argv(&["MULTI"]));
            log.extend_from_slice(&resp_argv(&["INCR", "n"]));
            log.extend_from_slice(&resp_argv(&["SET", &format!("k{i}"), "x"]));
            log.extend_from_slice(&resp_argv(&["EXEC"]));
            i += 1;
        }
        std::fs::write(&path, &log).unwrap();
        drop(log);
        let peak_mb = std::thread::spawn(move || {
            let mut db = ShardDb::new(6379);
            replay_aof(&path, &mut db).unwrap();
            let mut out = Vec::new();
            for (cmd, want) in [
                (
                    Command::Get(Bytes::from_static(b"n")),
                    format!("${}\r\n{i}\r\n", i.to_string().len()),
                ),
                (
                    Command::Strlen(Bytes::from_static(b"big")),
                    format!(":{}\r\n", 1 << 20),
                ),
            ] {
                out.clear();
                crate::connection::execute_local_command(&cmd, &mut db, &mut out, None);
                assert_eq!(String::from_utf8_lossy(&out), want);
            }
            assert_eq!(db.table.len(), i + 2);
            // SAFETY: `thread.peak.read` is a u64 ctl.
            unsafe { tikv_jemalloc_ctl::raw::read::<u64>(b"thread.peak.read\0").unwrap() >> 20 }
        })
        .join()
        .unwrap();
        assert!(
            peak_mb < 32,
            "replaying a 64 MB tail peaked at {peak_mb} MB"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Replaying an AOF maps its file rather than reading it into memory:
    /// loading a 64 MB value allocates about the value, not the value plus
    /// the file (which doubled the memory a restart needed).
    #[test]
    fn test_replay_maps_the_aof_file() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-mmap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let big = 64usize << 20;
        let mut src = ShardDb::new(6379);
        // Incompressible, so the file is as big as the value.
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let value: Vec<u8> = (0..big)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let mut out = Vec::new();
        crate::connection::execute_local_command(
            &Command::Set {
                key: Bytes::from_static(b"big"),
                value: Bytes::from(value),
                expire_in: None,
                condition: crate::resp::SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            },
            &mut src,
            &mut out,
            None,
        );
        // An RDB-preamble AOF, as a rewrite writes it.
        let mut image = crate::shard::AofImage::start(&src).unwrap();
        let mut file = Vec::new();
        loop {
            let (piece, done) = image.step(&src, Duration::ZERO, 1 << 30).unwrap();
            file.extend(piece.unwrap_or_default());
            if done {
                break;
            }
        }
        file.extend(image.finish().unwrap());
        let crc = crate::table::crc64(&file);
        file.extend_from_slice(&crc.to_le_bytes());
        let path = dir.join("appendonly-0.aof");
        std::fs::write(&path, &file).unwrap();
        drop((src, file));
        let allocated = tikv_jemalloc_ctl::thread::allocatedp::read().unwrap();
        let mut db = ShardDb::new(6379);
        let before = allocated.get();
        replay_aof(&path, &mut db).unwrap();
        let used = allocated.get() - before;
        assert_eq!(db.table.len(), 1);
        assert!(
            used < big as u64 + big as u64 / 2,
            "replaying a {} MB value allocated {} MB",
            big >> 20,
            used >> 20
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An AOF replay leaves this thread's allocator cache empty: buffers
    /// freed by it (or before it) go back to the arena instead of staying
    /// counted as allocated, against maxmemory.
    #[test]
    fn test_replay_aof_flushes_thread_cache() {
        std::thread::spawn(|| {
            crate::allocator::bind_thread_to_new_arena();
            let dir = std::env::temp_dir().join(format!("rudis-aof-tcache-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("log.aof");
            std::fs::write(&path, resp_argv(&["SET", "k", "v"])).unwrap();
            let mut db = ShardDb::new(6379);
            // Fill the cache's large bins with freed 24 KB buffers.
            let bufs: Vec<Vec<u8>> = (0..16)
                .map(|_| std::hint::black_box(vec![1u8; 24 << 10]))
                .collect();
            drop(bufs);
            let before = crate::allocator::thread_cache_bytes();
            assert!(before >= 8 * (24 << 10), "cache holds {before} bytes");
            assert_eq!(replay_aof(&path, &mut db).unwrap(), 1);
            let after = crate::allocator::thread_cache_bytes();
            assert!(
                after < 16 << 10,
                "cache still holds {after} bytes after the replay"
            );
            let _ = std::fs::remove_dir_all(&dir);
        })
        .join()
        .unwrap();
    }
    use bytes::Bytes;
    use std::time::Duration;

    fn aof_tmp(name: &str, content: &[u8]) -> PathBuf {
        let p = std::env::temp_dir().join(format!("rudis-aoftail-{}-{}", std::process::id(), name));
        std::fs::write(&p, content).unwrap();
        p
    }

    #[test]
    fn test_json_writes_round_trip_through_aof_encoding() {
        let writes: &[&[&str]] = &[
            &[
                "JSON.SET",
                "j",
                "$",
                r#"{"a":[1,2],"s":"x","b":true,"n":1.5}"#,
            ],
            &["JSON.SET", "j", "$.a", "3", "NX"],
            &["JSON.SET", "j", "$.a", "3", "XX"],
            &["JSON.DEL", "j"],
            &["JSON.DEL", "j", "$.a"],
            &["JSON.CLEAR", "j"],
            &["JSON.CLEAR", "j", "$.a"],
            &["JSON.NUMINCRBY", "j", "$.n", "0.1"],
            &["JSON.NUMMULTBY", "j", "$.n", "-2.5"],
            &["JSON.STRAPPEND", "j", r#""y""#],
            &["JSON.STRAPPEND", "j", "$.s", r#""y""#],
            &["JSON.ARRAPPEND", "j", "$.a", "3", r#"{"k":1}"#],
            &["JSON.ARRPOP", "j"],
            &["JSON.ARRPOP", "j", "$.a"],
            &["JSON.ARRPOP", "j", "$.a", "-1"],
            &["JSON.TOGGLE", "j", "$.b"],
        ];
        for args in writes {
            let cmd = parse_resp(&resp_argv(args));
            assert!(cmd.is_write_command(), "{args:?} should be a write");
            let bytes = command_to_resp(&cmd).unwrap_or_else(|| panic!("{args:?} not encoded"));
            assert_eq!(parse_resp(&bytes), cmd, "{args:?}");
        }
    }

    #[test]
    fn test_probabilistic_writes_round_trip_through_aof_encoding() {
        let writes: &[&[&str]] = &[
            &["BF.RESERVE", "b", "0.001", "500"],
            &["BF.RESERVE", "b", "0.1", "1"],
            &["BF.ADD", "b", "x"],
            &["BF.MADD", "b", "x", "y", "z"],
            &["CF.RESERVE", "c", "64"],
            &["CF.ADD", "c", "x"],
            &["CF.ADDNX", "c", "x"],
            &["CF.DEL", "c", "x"],
            &["CMS.INITBYDIM", "m", "100", "4"],
            &["CMS.INITBYPROB", "m", "0.001", "0.995"],
            &["CMS.INCRBY", "m", "x", "3"],
            &["CMS.INCRBY", "m", "x", "3", "y", "18446744073709551615"],
            &["TOPK.RESERVE", "t", "10"],
            &["TOPK.ADD", "t", "x"],
            &["TOPK.ADD", "t", "x", "y", "x"],
            &["BF.RESTORE", "b", "\x00\r\n\x01"],
            &["CF.RESTORE", "c", ""],
            &["CMS.RESTORE", "m", "p"],
            &["TOPK.RESTORE", "t", "p"],
        ];
        for args in writes {
            let cmd = parse_resp(&resp_argv(args));
            assert!(cmd.is_write_command(), "{args:?} should be a write");
            let bytes = command_to_resp(&cmd).unwrap_or_else(|| panic!("{args:?} not encoded"));
            assert_eq!(parse_resp(&bytes), cmd, "{args:?}");
        }
    }

    /// Replaying the logged BF/CF/CMS/TOPK writes, or a rewrite of the
    /// result, rebuilds exactly the same structures.
    /// Nothing expires mid-replay (Redis loading rules): a key whose
    /// PEXPIREAT is in the past by replay time but that later writes still
    /// found live replays like the original run.
    #[test]
    fn test_replay_does_not_expire_keys_mid_load() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-noexp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let past = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - 60_000)
            .to_string();
        let mut log = Vec::new();
        for args in [
            // Expired by replay time, but PERSISTed before it expired.
            &["SET", "kept", "v"][..],
            &["PEXPIREAT", "kept", &past],
            &["PERSIST", "kept"],
            // As a rewritten AOF stores a TTL: SET ... PXAT <past>, then
            // PERSISTed before it expired.
            &["SET", "rewritten", "v", "PXAT", &past],
            &["PERSIST", "rewritten"],
            // Expired by replay time after an APPEND: gone afterwards, not
            // recreated as "tail" without a TTL.
            &["SET", "gone", "v"],
            &["PEXPIREAT", "gone", &past],
            &["APPEND", "gone", "tail"],
        ] {
            log.extend_from_slice(&resp_argv(args));
        }
        // Kept, but with a deadline that has really passed: not one rounded
        // to the nearest stored millisecond, which can sit in the future.
        for i in 0..200 {
            let key = format!("late{i}");
            log.extend_from_slice(&resp_argv(&["SET", &key, "v", "PXAT", &past]));
        }
        let path = dir.join("log.aof");
        std::fs::write(&path, &log).unwrap();
        let mut db = ShardDb::new(6379);
        replay_aof(&path, &mut db).unwrap();
        let after = std::time::Instant::now();
        let not_passed = db
            .table
            .entries()
            .filter(|e| e.key.starts_with(b"late") && e.expire_at().is_none_or(|t| t > after))
            .count();
        assert_eq!(not_passed, 0, "replayed past PXAT must stay passed");
        assert!(!crate::connection::REPLAYING_WRITES.get());
        let mut out = Vec::new();
        let mut run = |db: &mut ShardDb, args: &[&str]| {
            out.clear();
            let cmd = parse_resp(&resp_argv(args));
            crate::connection::execute_local_command(&cmd, db, &mut out, None);
            String::from_utf8_lossy(&out).into_owned()
        };
        assert_eq!(run(&mut db, &["GET", "kept"]), "$1\r\nv\r\n");
        assert_eq!(run(&mut db, &["TTL", "kept"]), ":-1\r\n");
        assert_eq!(run(&mut db, &["GET", "rewritten"]), "$1\r\nv\r\n");
        assert_eq!(run(&mut db, &["TTL", "rewritten"]), ":-1\r\n");
        assert_eq!(run(&mut db, &["GET", "gone"]), "$-1\r\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `append_command` logs exactly what `command_to_resp` encodes (plain
    /// SETs are encoded in place, everything else through it), behind a
    /// SELECT when the DB changes, and appends nothing for commands with no
    /// encoding.
    #[test]
    fn test_append_command_matches_command_to_resp() {
        let mut w = AofWriter::new_in_memory();
        let mut expected = Vec::new();
        let big = "v".repeat(10_000);
        for (db, line) in [
            (0, "SET k v".to_string()),
            (0, "SET k v NX".to_string()),
            (0, format!("SET big {big}")),
            (0, "SET k v KEEPTTL".to_string()),
            (3, "SET k3 v".to_string()),
            (3, "INCR n".to_string()),
            (3, "HSET h f v".to_string()),
            (0, "GET k".to_string()),
            (0, "SET back v".to_string()),
        ] {
            crate::connection::CURRENT_DB.set(db);
            let cmd = parse_line(&line);
            let before = w.buffer().len();
            let start = w.append_command(&cmd);
            match command_to_resp(&cmd) {
                Some(bytes) => {
                    let start = start.unwrap_or_else(|| panic!("{line}: not appended"));
                    assert_eq!(&w.buffer()[start..], &bytes[..], "{line}");
                    if start != before {
                        expected.extend_from_slice(&w.buffer()[before..start]);
                    }
                    expected.extend_from_slice(&bytes);
                }
                None => {
                    assert_eq!(start, None, "{line}");
                    assert_eq!(w.buffer().len(), before, "{line}: appended bytes");
                }
            }
        }
        crate::connection::CURRENT_DB.set(0);
        assert_eq!(w.buffer(), &expected[..]);
        // The shared plain-SET encoding itself, byte for byte.
        assert_eq!(
            command_to_resp(&parse_line("SET k v")).unwrap(),
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
        );
        let mut long = b"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n$10000\r\n".to_vec();
        long.extend_from_slice(big.as_bytes());
        long.extend_from_slice(b"\r\n");
        assert_eq!(
            command_to_resp(&parse_line(&format!("SET big {big}"))).unwrap(),
            long
        );
        let log = String::from_utf8_lossy(w.buffer());
        assert_eq!(log.matches("SELECT").count(), 2, "{log:.300}");
    }

    /// `encode_command` appends after what `buf` holds, exactly the bytes
    /// `command_to_resp` returns, and leaves `buf` untouched for a command
    /// without an encoding.
    #[test]
    fn test_encode_command_appends_in_place() {
        let mut buf = b"prefix".to_vec();
        for line in [
            "HSET hash field value field2 value2",
            "INCR counter",
            "DECRBY counter 1234567",
            "LPUSH list alpha beta",
            "SADD set member",
            "ZADD zset NX 1.5 alpha",
            "DEL key1 key22",
            "XADD stream 1-1 field value",
        ] {
            let cmd = parse_line(line);
            let before = buf.len();
            assert!(encode_command(&cmd, &mut buf), "{line}");
            assert_eq!(
                &buf[before..],
                &command_to_resp(&cmd).unwrap()[..],
                "{line}"
            );
            assert_eq!(parse_resp(&buf[before..]), parse_line(line), "{line}");
        }
        let len = buf.len();
        assert!(!encode_command(&parse_line("GET key"), &mut buf));
        assert_eq!(buf.len(), len);
        assert!(buf.starts_with(b"prefix*6\r\n$4\r\nHSET\r\n"));
    }

    /// A key expiring on access is logged as DEL before the command that
    /// found it expired, so replaying the AOF (where nothing expires)
    /// rebuilds what the master had.
    #[test]
    fn test_lazy_expiry_logs_del_before_the_command() {
        let aof = std::rc::Rc::new(std::cell::RefCell::new(AofWriter::new_in_memory()));
        crate::replication::set_shard_log_context(6379, 0, Some(aof.clone()));
        let mut db = ShardDb::new(6379);
        let mut out = Vec::new();
        let mut run = |db: &mut ShardDb, args: &[&str]| {
            out.clear();
            let cmd = parse_resp(&resp_argv(args));
            crate::connection::execute_local_command(&cmd, db, &mut out, Some(&*aof));
            String::from_utf8_lossy(&out).into_owned()
        };
        run(&mut db, &["SET", "k", "v", "PX", "1"]);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(run(&mut db, &["APPEND", "k", "tail"]), ":4\r\n");
        crate::replication::set_shard_log_context(6379, 0, None);
        let log = String::from_utf8_lossy(aof.borrow().buffer()).into_owned();
        let del = log.find("DEL\r\n$1\r\nk").expect("expiry not logged");
        let append = log.find("APPEND").expect("append not logged");
        assert!(del < append, "DEL must precede the APPEND: {log:?}");
        // Replaying it gives the master's value, not "v" + "tail".
        let dir = std::env::temp_dir().join(format!("rudis-aof-expdel-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("log.aof"), aof.borrow().buffer()).unwrap();
        let mut replayed = ShardDb::new(6379);
        replay_aof(&dir.join("log.aof"), &mut replayed).unwrap();
        assert_eq!(run(&mut replayed, &["GET", "k"]), "$4\r\ntail\r\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An expired hash field (HEXPIRE) is logged as HDEL when it is
    /// dropped, before the command that dropped it, and a replay (where
    /// nothing expires) gives the master's hash.
    #[test]
    fn test_hash_field_expiry_logs_hdel() {
        let aof = std::rc::Rc::new(std::cell::RefCell::new(AofWriter::new_in_memory()));
        crate::replication::set_shard_log_context(6379, 0, Some(aof.clone()));
        let mut db = ShardDb::new(6379);
        let mut out = Vec::new();
        let mut run = |db: &mut ShardDb, args: &[&str]| {
            out.clear();
            let cmd = parse_resp(&resp_argv(args));
            crate::connection::execute_local_command(&cmd, db, &mut out, Some(&*aof));
            String::from_utf8_lossy(&out).into_owned()
        };
        run(&mut db, &["HSET", "h", "f", "v", "g", "w"]);
        run(&mut db, &["HPEXPIRE", "h", "1", "FIELDS", "1", "f"]);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(run(&mut db, &["HLEN", "h"]), ":1\r\n");
        crate::replication::set_shard_log_context(6379, 0, None);
        let log = String::from_utf8_lossy(aof.borrow().buffer()).into_owned();
        assert!(log.contains("HDEL\r\n$1\r\nh\r\n$1\r\nf"), "{log:?}");
        let dir = std::env::temp_dir().join(format!("rudis-aof-hdel-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("log.aof"), aof.borrow().buffer()).unwrap();
        let mut replayed = ShardDb::new(6379);
        replay_aof(&dir.join("log.aof"), &mut replayed).unwrap();
        assert_eq!(
            run(&mut replayed, &["HGETALL", "h"]),
            "*2\r\n$1\r\ng\r\n$1\r\nw\r\n"
        );
        // A field deadline already past at replay time but HPERSISTed later
        // in the log is kept: nothing expires while replaying.
        let past = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - 60_000)
            .to_string();
        let mut log = Vec::new();
        for args in [
            &["HSET", "p", "f", "v"][..],
            &["HPEXPIREAT", "p", &past, "FIELDS", "1", "f"],
            &["HPERSIST", "p", "FIELDS", "1", "f"],
        ] {
            log.extend_from_slice(&resp_argv(args));
        }
        std::fs::write(dir.join("log2.aof"), &log).unwrap();
        let mut replayed = ShardDb::new(6379);
        replay_aof(&dir.join("log2.aof"), &mut replayed).unwrap();
        assert_eq!(run(&mut replayed, &["HGET", "p", "f"]), "$1\r\nv\r\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_probabilistic_state_survives_aof_replay_and_rewrite() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-prob-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut db = ShardDb::new(6379);
        let aof = std::cell::RefCell::new(AofWriter::new_in_memory());
        let mut out = Vec::new();
        let mut run = |db: &mut ShardDb, args: &[&str]| {
            out.clear();
            let cmd = parse_resp(&resp_argv(args));
            crate::connection::execute_local_command(&cmd, db, &mut out, Some(&aof));
            String::from_utf8_lossy(&out).into_owned()
        };
        let scripted: &[&[&str]] = &[
            &["BF.RESERVE", "bf", "0.001", "50"],
            &["BF.ADD", "bf", "a"],
            &["BF.MADD", "bf", "b", "c", "a"],
            &["BF.ADD", "bf2", "x"],
            &["CF.RESERVE", "cf", "4"],
            &["CF.ADDNX", "cf", "i0"],
            &["CF.DEL", "cf", "i0"],
            &["CMS.INITBYDIM", "cms", "50", "3"],
            &["CMS.INCRBY", "cms", "a", "5", "b", "7"],
            &["CMS.INITBYPROB", "cms2", "0.01", "0.99"],
            &["CMS.INCRBY", "cms2", "a", "1"],
            &["CMS.INCRBY", "cms3", "x", "2"],
            &["TOPK.RESERVE", "tk", "2"],
            &["TOPK.ADD", "tk", "a", "b", "c", "d", "a"],
            &["TOPK.ADD", "tk2", "z"],
        ];
        for args in scripted {
            let reply = run(&mut db, args);
            assert!(!reply.starts_with('-'), "{args:?}: {reply}");
        }
        // Overfill the cuckoo filter: failed inserts still move
        // fingerprints, so they must be logged too.
        let mut failed = 0;
        for i in 0..64 {
            let item = format!("i{i}");
            failed += run(&mut db, &["CF.ADD", "cf", &item]).starts_with('-') as usize;
        }
        assert!(failed > 0, "the filter never filled up");

        let log = dir.join("log.aof");
        std::fs::write(&log, aof.borrow().buffer()).unwrap();
        let mut replayed = ShardDb::new(6379);
        replay_aof(&log, &mut replayed).unwrap();
        assert_eq!(replayed.probabilistic_store, db.probabilistic_store);

        let n = rewrite_shard_aof(&mut db, &dir, 0).unwrap();
        assert_eq!(n, 8, "one *.RESTORE per structure");
        let mut rewritten = ShardDb::new(6379);
        let applied = replay_aof(&dir.join("appendonly-0.aof"), &mut rewritten).unwrap();
        assert_eq!(applied, 8);
        assert_eq!(rewritten.probabilistic_store, db.probabilistic_store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_crdt_writes_round_trip_through_aof_encoding() {
        let mut remote = crate::crdt::CrdtStore::new(9);
        remote.set(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"\x00\r\n\xff"),
        );
        let payload = remote.export_sync_payload();
        let writes: &[&[&[u8]]] = &[
            &[b"CRDT.MERGE", &payload],
            &[b"CRDT.MERGE", b""],
            &[b"CRDT.GC"],
            &[b"CRDT.GC", b"0"],
            &[b"CRDT.GC", b"BEFORE", b"1700000000000"],
        ];
        for args in writes {
            let cmd = parse_resp(&resp_argv(args));
            assert!(cmd.is_write_command(), "{cmd:?} should be a write");
            let bytes = command_to_resp(&cmd).unwrap_or_else(|| panic!("{cmd:?} not encoded"));
            assert_eq!(parse_resp(&bytes), cmd);
        }
        // These are logged as the CRDT.MERGE of their effect instead.
        for args in [
            &["CRDT.SET", "k", "v"][..],
            &["CRDT.DEL", "k"],
            &["CRDT.INCRBY", "k", "1"],
            &["CRDT.SADD", "k", "m"],
            &["CRDT.SREM", "k", "m"],
        ] {
            assert_eq!(command_to_resp(&parse_resp(&resp_argv(args))), None);
        }
    }

    /// Replaying the logged CRDT writes, or a rewrite of the result, rebuilds
    /// exactly the same CRDT state, deletions and removals included.
    #[test]
    fn test_crdt_state_survives_aof_replay_and_rewrite() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-crdt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut db = ShardDb::new(6379);
        let aof = std::cell::RefCell::new(AofWriter::new_in_memory());
        let mut out = Vec::new();
        let mut run = |db: &mut ShardDb, args: &[&[u8]]| {
            out.clear();
            let cmd = parse_resp(&resp_argv(args));
            crate::connection::execute_local_command(&cmd, db, &mut out, Some(&aof));
            String::from_utf8_lossy(&out).into_owned()
        };
        // State from another region, merged in below.
        let mut remote = crate::crdt::CrdtStore::new(9);
        remote.set(Bytes::from_static(b"m:reg"), Bytes::from_static(b"remote"));
        remote
            .counter_incr(Bytes::from_static(b"ctr"), 100)
            .unwrap();
        remote.set_add(Bytes::from_static(b"set"), Bytes::from_static(b"r1"));
        let remote_payload = remote.export_sync_payload();

        let scripted: &[(&[&[u8]], &str)] = &[
            (&[b"CRDT.SET", b"reg", b"v1"], "+OK"),
            (&[b"CRDT.SET", b"reg", b"v2"], "+OK"),
            (&[b"CRDT.SET", b"gone", b"x"], "+OK"),
            (&[b"CRDT.DEL", b"gone"], ":1"),
            (&[b"CRDT.DEL", b"never"], ":0"),
            (&[b"CRDT.INCRBY", b"ctr", b"10"], ":10"),
            (&[b"CRDT.INCRBY", b"ctr", b"-3"], ":7"),
            (&[b"CRDT.SADD", b"set", b"a"], ":1"),
            (&[b"CRDT.SADD", b"set", b"b"], ":1"),
            (&[b"CRDT.SADD", b"set", b"a"], ":0"),
            (&[b"CRDT.SREM", b"set", b"a"], ":1"),
            (&[b"CRDT.SREM", b"set", b"zz"], ":0"),
            (&[b"CRDT.SADD", b"set2", b"x"], ":1"),
            (&[b"CRDT.SREM", b"set2", b"x"], ":1"),
            (&[b"CRDT.MERGE", &remote_payload], ":3"),
            (&[b"CRDT.MERGE", b"\x07bad"], "-ERR"),
        ];
        for (args, want) in scripted {
            let reply = run(&mut db, args);
            assert!(reply.starts_with(want), "{args:?}: {reply}");
        }
        // An add of 'b' as it is now, merged again after 'b' is removed and
        // its tombstone collected, resurrects it; replay must agree.
        let stale_b = db
            .crdt_store
            .set_member_payload(b"set", &Bytes::from_static(b"b"));
        assert!(run(&mut db, &[b"CRDT.SREM", b"set", b"b"]).starts_with(":1"));
        std::thread::sleep(std::time::Duration::from_millis(5));
        let gc = run(&mut db, &[b"CRDT.GC", b"0"]);
        assert!(gc.contains("registers_pruned\r\n:1\r\n"), "{gc}");
        assert!(run(&mut db, &[b"CRDT.MERGE", &stale_b]).starts_with(":1"));

        let observe = |db: &ShardDb| {
            let mut members: Vec<Vec<Bytes>> = ["set", "set2"]
                .iter()
                .map(|k| db.crdt_smembers(&Bytes::from(*k)))
                .collect();
            members.iter_mut().for_each(|m| m.sort());
            (
                ["reg", "gone", "never", "m:reg"].map(|k| db.crdt_get(&Bytes::from(k))),
                db.crdt_counter_get(&Bytes::from_static(b"ctr")),
                members,
            )
        };
        let want = observe(&db);
        assert_eq!(want.0[0], Some(Bytes::from_static(b"v2")));
        assert_eq!(want.0[1], None);
        assert_eq!(want.0[3], Some(Bytes::from_static(b"remote")));
        assert_eq!(want.1, 107);
        assert_eq!(
            want.2,
            vec![
                vec![Bytes::from_static(b"b"), Bytes::from_static(b"r1")],
                vec![]
            ]
        );
        let same_state = |a: &ShardDb, b: &ShardDb| {
            assert_eq!(a.crdt_store.registers, b.crdt_store.registers);
            assert_eq!(a.crdt_store.counters, b.crdt_store.counters);
            assert_eq!(a.crdt_store.sets, b.crdt_store.sets);
            assert_eq!(observe(a), observe(b));
        };

        let log = dir.join("log.aof");
        std::fs::write(&log, aof.borrow().buffer()).unwrap();
        let mut replayed = ShardDb::new(6380);
        replay_aof(&log, &mut replayed).unwrap();
        same_state(&replayed, &db);

        // Both rewrite forms: the RDB image of a live rewrite, and the
        // canonical commands of a reshard (one CRDT.MERGE per register,
        // counter and set).
        rewrite_shard_aof(&mut db, &dir, 0).unwrap();
        let mut rewritten = ShardDb::new(6380);
        replay_aof(&dir.join("appendonly-0.aof"), &mut rewritten).unwrap();
        same_state(&rewritten, &db);
        let entries =
            db.crdt_store.registers.len() + db.crdt_store.counters.len() + db.crdt_store.sets.len();
        let n = rewrite_shard_aof_canonical(&mut db, &dir, 0).unwrap();
        assert_eq!(n, entries, "one CRDT.MERGE per register, counter and set");
        let mut rewritten = ShardDb::new(6380);
        let applied = replay_aof(&dir.join("appendonly-0.aof"), &mut rewritten).unwrap();
        assert_eq!(applied, entries);
        same_state(&rewritten, &db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A rewritten AOF is an RDB image (preamble) of the shard, and keeps
    /// the function libraries, which the old command form dropped: after a
    /// rewrite and a restart, FCALL failed with "Function not found".
    #[test]
    fn test_rewritten_aof_is_an_rdb_image_keeping_functions() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-img-fn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let lib = "aofimgfnlib";
        let code = format!(
            "#!lua name={lib}\nredis.register_function('aofimgfn', function(keys, args) return 7 end)"
        );
        crate::scripting::load_function(&code, true).unwrap();
        let mut db = ShardDb::new(6381);
        db.set(Bytes::from("k"), Bytes::from("v"), None);
        db.rpush(Bytes::from("l"), vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(rewrite_shard_aof(&mut db, &dir, 0).unwrap(), 2);
        let file = std::fs::read(dir.join("appendonly-0.aof")).unwrap();
        assert!(file.starts_with(b"REDIS"), "an RDB preamble");
        assert!(crate::scripting::delete_function(lib));
        let mut restored = ShardDb::new(6381);
        replay_aof(&dir.join("appendonly-0.aof"), &mut restored).unwrap();
        assert!(
            crate::scripting::list_functions()
                .iter()
                .any(|l| l.name == lib),
            "function library restored"
        );
        assert_eq!(restored.get(b"k"), Some(Bytes::from("v")));
        assert_eq!(restored.llen(b"l").unwrap(), 2);
        crate::scripting::delete_function(lib);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Nothing expires while an AOF's RDB preamble loads (as in Redis): a
    /// key or hash field whose deadline passed is kept, with that deadline,
    /// so a later PERSIST / HPERSIST in the AOF still finds it.
    #[test]
    fn test_rdb_preamble_keeps_passed_deadlines_for_later_commands() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-img-ttl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut db = ShardDb::new(6384);
        let mut out = Vec::new();
        for line in [
            "SET k v PX 150",
            "SET gone v PX 150",
            "HSET h f v g w",
            "HPEXPIRE h 150 FIELDS 2 f g",
        ] {
            crate::connection::execute_local_command(&parse_line(line), &mut db, &mut out, None);
        }
        rewrite_shard_aof(&mut db, &dir, 0).unwrap();
        let path = dir.join("appendonly-0.aof");
        let mut file = std::fs::read(&path).unwrap();
        file.extend_from_slice(&resp_argv(&["PERSIST", "k"]));
        file.extend_from_slice(&resp_argv(&["HPERSIST", "h", "FIELDS", "1", "f"]));
        std::fs::write(&path, &file).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        let mut restored = ShardDb::new(6384);
        replay_aof(&path, &mut restored).unwrap();
        let mut run = |line: &str| {
            out.clear();
            crate::connection::execute_local_command(
                &parse_line(line),
                &mut restored,
                &mut out,
                None,
            );
            String::from_utf8_lossy(&out).into_owned()
        };
        assert_eq!(run("GET k"), "$1\r\nv\r\n");
        assert_eq!(run("TTL k"), ":-1\r\n");
        assert_eq!(run("GET gone"), "$-1\r\n");
        assert_eq!(run("HGETALL h"), "*2\r\n$1\r\nf\r\n$1\r\nv\r\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A live rewrite runs between writes (here: random INCR, LPUSH,
    /// APPEND, RENAME, DEL, MOVE... in two DBs, many while its image is
    /// being built), yet the new AOF replays to exactly the live dataset:
    /// the image holds each key as of the start, and the tail everything
    /// after. Without the before-write hook, a key changed before the walk
    /// reached it was in the image with the change and in the tail again.
    #[monoio::test(timer_enabled = true)]
    async fn test_live_rewrite_replays_to_the_live_dataset() {
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;
        let dir = std::env::temp_dir().join(format!("rudis-aof-live-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = Rc::new(RefCell::new(ShardDb::new(6383)));
        let aof = Rc::new(RefCell::new(
            AofWriter::open(dir.join("appendonly-0.aof")).await.unwrap(),
        ));
        let mut out = Vec::new();
        let mut run = |dbi: usize, line: String| {
            crate::connection::CURRENT_DB.set(dbi);
            out.clear();
            let cmd = parse_line(&line);
            crate::connection::execute_local_command(
                &cmd,
                &mut db.borrow_mut(),
                &mut out,
                Some(&aof),
            );
        };
        for i in 0..20_000 {
            run(0, format!("SET s{i} v{i}"));
            run(0, format!("INCR c{}", i % 3000));
            run(0, format!("RPUSH l{} x{i}", i % 700));
            // DB 1 large enough to be walked a segment at a time.
            run(1, format!("INCR d{}", i % 5000));
            if i % 20 == 0 {
                run(1, format!("HSET h{} f v g w", i % 300));
                run(1, format!("HPEXPIRE h{} 3600000 FIELDS 1 g", i % 300));
                run(0, format!("JSON.SET j{} $ '{{\"n\":{i}}}'", i % 400));
            }
        }
        run(0, "CMS.INITBYDIM cms 100 4".to_string());
        run(0, "CF.RESERVE cf 10000".to_string());
        AofWriter::write_through(&aof).await.unwrap();
        let fallbacks = LIVE_REWRITE_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed);
        // A short pause between image steps, so plenty of writes land while
        // the image is built however fast this build walks it (a release
        // build finished the walk within ~40 writes).
        struct SaveDelay;
        impl Drop for SaveDelay {
            fn drop(&mut self) {
                crate::connection::RDB_KEY_SAVE_DELAY
                    .store(0, std::sync::atomic::Ordering::Relaxed);
            }
        }
        crate::connection::RDB_KEY_SAVE_DELAY.store(200, std::sync::atomic::Ordering::Relaxed);
        let _save_delay = SaveDelay;
        let done = Rc::new(Cell::new(false));
        let task = {
            let (db, aof, dir, done) = (db.clone(), aof.clone(), dir.clone(), done.clone());
            monoio::spawn(async move {
                let res = rewrite_shard_aof_live(&db, &dir, 0, Some(&aof)).await;
                done.set(true);
                res
            })
        };
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = |m: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % m
        };
        let mut during_image = 0;
        while !done.get() {
            for _ in 0..40 {
                if crate::shard::aof_image_active() {
                    during_image += 1;
                }
                let (a, b) = (next(20_000), next(20_000));
                match next(14) {
                    0 => run(0, format!("INCR c{}", next(3000))),
                    1 => run(0, format!("LPUSH l{} y{a}", next(700))),
                    2 => run(0, format!("APPEND s{a} z")),
                    3 => run(0, format!("RENAME s{a} s{b}")),
                    4 => run(0, format!("DEL s{a}")),
                    5 => run(0, format!("SET n{a} {b}")),
                    6 => run(0, format!("MOVE c{} 1", next(3000))),
                    7 => run(1, format!("INCR d{}", next(5000))),
                    8 => run(1, format!("HINCRBY h{} f2 1", next(300))),
                    9 => run(0, format!("JSON.NUMINCRBY j{} $.n 1", next(400))),
                    10 => run(0, format!("CMS.INCRBY cms i{} 1", next(50))),
                    11 => run(0, format!("CF.ADD cf i{}", next(50))),
                    12 => run(1, format!("RENAME d{} d{}", next(5000), next(5000))),
                    _ => run(0, format!("LPOP l{}", next(700))),
                }
            }
            crate::mailbox::yield_now().await;
        }
        task.await.unwrap();
        assert!(
            during_image > 100,
            "writes while the image was built: {during_image}"
        );
        assert_eq!(
            LIVE_REWRITE_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed),
            fallbacks,
            "no blocking fallback"
        );
        // Writes after the switch go to the new file.
        run(0, "INCR after".to_string());
        run(1, "SET after1 x".to_string());
        AofWriter::write_through(&aof).await.unwrap();
        crate::connection::CURRENT_DB.set(0);
        let file = std::fs::read(dir.join("appendonly-0.aof")).unwrap();
        assert!(file.starts_with(b"REDIS"));
        let mut replayed = ShardDb::new(6383);
        replay_aof(&dir.join("appendonly-0.aof"), &mut replayed).unwrap();
        assert_eq!(
            crate::digest::shard_digest(&replayed),
            crate::digest::shard_digest(&db.borrow()),
            "the rewritten AOF replays to the live dataset"
        );
        // The extended structures and DB 1's hash field deadlines too.
        let live = db.borrow();
        let items = live.ext_item_keys();
        assert!(
            items.iter().any(|(k, _)| *k == crate::shard::ExtKind::Cms)
                && items
                    .iter()
                    .any(|(k, _)| *k == crate::shard::ExtKind::Cuckoo),
            "{items:?}"
        );
        for (kind, key) in items {
            let (mut a, mut b) = (Vec::new(), Vec::new());
            live.save_ext_item(kind, &key, &mut a);
            replayed.save_ext_item(kind, &key, &mut b);
            assert!(a == b, "{kind:?} {key:?} differs after replay");
        }
        let fields = |d: &ShardDb| {
            let mut v: Vec<(Bytes, Bytes)> = d
                .table_for_db(1)
                .unwrap()
                .hash_field_expires
                .iter()
                .flat_map(|(k, m)| m.keys().map(move |f| (k.clone(), f.clone())))
                .collect();
            v.sort();
            v
        };
        assert_eq!(fields(&replayed), fields(&live));
        assert!(!fields(&live).is_empty());
        drop(live);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Startup removes the temp files of rewrites a crash cut short, and
    /// nothing else.
    #[test]
    fn test_startup_removes_stale_rewrite_temps() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-stale-tmp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("appendonly-0.aof"), SET_A).unwrap();
        std::fs::write(dir.join("appendonly-0.aof.tmp.4242_live1"), b"REDIS0012").unwrap();
        std::fs::write(dir.join("appendonly-1.aof.tmp.4242_3"), b"*1").unwrap();
        std::fs::write(dir.join("dump.rdb.tmp"), b"x").unwrap();
        reshard_aof_dir(&dir, 1, 0).unwrap();
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| !n.starts_with(AOF_SHARDS_MANIFEST))
            .collect();
        names.sort();
        assert_eq!(names, ["appendonly-0.aof", "dump.rdb.tmp"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn parse_line(line: &str) -> Command {
        let mut buf = bytes::BytesMut::from(format!("{line}\r\n").as_str());
        match crate::resp::parse_command(&mut buf) {
            Ok(Some(cmd)) => cmd,
            other => panic!("{line}: {other:?}"),
        }
    }

    fn parse_resp(bytes: &[u8]) -> Command {
        let mut buf = bytes::BytesMut::from(bytes);
        match crate::resp::parse_command(&mut buf) {
            Ok(Some(cmd)) => cmd,
            other => panic!("{:?}: {other:?}", String::from_utf8_lossy(bytes)),
        }
    }

    /// A write command without an encoder arm is silently never written to
    /// the AOF nor sent to replicas, so the replica diverges.
    #[test]
    fn test_write_commands_are_logged_and_replicated() {
        // Commands whose encoding re-parses to the very same command.
        let round_trip = [
            "LPUSHX l a b",
            "RPUSHX l a",
            "ZPOPMIN z",
            "ZPOPMAX z 3",
            "ZMPOP 2 a b MAX COUNT 2",
            "SINTERSTORE d a b",
            "SUNIONSTORE d a",
            "SDIFFSTORE d a b c",
            "ZDIFFSTORE d 2 a b",
            "ZUNIONSTORE d 2 a b WEIGHTS 2 0.5 AGGREGATE MAX",
            "ZINTERSTORE d 2 a b AGGREGATE MIN",
            "ZUNIONSTORE d 1 a",
            "GEOADD g NX CH 13.361389 38.115556 Palermo 15.087269 37.502669 Catania",
            "GEOSEARCHSTORE d s FROMLONLAT 15 37 BYRADIUS 200 km ASC COUNT 2 ANY STOREDIST",
            "GEOSEARCHSTORE d s FROMMEMBER m BYBOX 400 300 mi DESC",
            "XSETID s 5-1 ENTRIESADDED 7 MAXDELETEDID 3-0",
            "XSETID s 9-9",
            "XGROUP CREATECONSUMER s g c",
            "XGROUP DELCONSUMER s g c",
            "XGROUP SETID s g 5-0 ENTRIESREAD 3",
            "XADD s NOMKSTREAM MAXLEN = 10 1-1 f v",
            "XADD s DELREF MINID = 5-0 2-0 f v g w",
            "XADD s ACKED MAXLEN ~ 10 LIMIT 5 3-0 f v",
            "XADD s 1-* f v",
            "XTRIM s MAXLEN = 10",
            "XTRIM s MINID ~ 5-0 LIMIT 100 ACKED",
            "XTRIM s MAXLEN = 0 DELREF",
            "XCFGSET s IDMP-DURATION 60 IDMP-MAXSIZE 50",
            "XCFGSET s IDMP-MAXSIZE 5",
            "XDELEX s IDS 2 1-0 2-0",
            "XDELEX s ACKED IDS 1 1-0",
            "XACKDEL s g DELREF IDS 1 1-0",
            "XACKDEL s g IDS 2 1-0 3-5",
            "XCLAIM s g c 0 1-0 TIME 1700000000000 RETRYCOUNT 2 FORCE JUSTID LASTID 4-0",
            "XCLAIM s g c 10 1-0 2-0 IDLE 5",
            "XIDMPRECORD s p i 1-0",
            "XNACK s g FAIL IDS 1 1-0",
        ];
        for line in round_trip {
            let cmd = parse_line(line);
            let bytes = command_to_resp(&cmd).unwrap_or_else(|| panic!("{line} not encoded"));
            assert_eq!(parse_resp(&bytes), cmd, "{line}");
        }
        // DELEX only records a change once the key was deleted.
        let bytes = command_to_resp(&parse_line("DELEX k IFEQ v")).unwrap();
        assert_eq!(bytes, b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n");

        // Every write command must encode. GETEX, LMPOP and INCREX are
        // excluded: their handlers log a translated command instead.
        let writes = [
            "APPEND k v",
            "BITFIELD k SET i5 0 1",
            "BITOP AND d a b",
            "COPY a b",
            "DECR k",
            "DEL k",
            "EXPIRE k 10",
            "FLUSHALL",
            "GETDEL k",
            "GETSET k v",
            "HDEL h f",
            "HINCRBYFLOAT h f 1.5",
            "HSET h f v",
            "INCR k",
            "LINSERT l BEFORE a b",
            "LMOVE a b LEFT RIGHT",
            "LPOP l",
            "LPUSH l a",
            "LREM l 1 a",
            "LSET l 0 a",
            "LTRIM l 0 1",
            "MSET a 1 b 2",
            "MSETNX a 1",
            "PERSIST k",
            "PFADD h a",
            "PFMERGE d a b",
            "RENAME a b",
            "RPOPLPUSH a b",
            "SADD s a",
            "SET k v",
            "SETBIT k 0 1",
            "SETRANGE k 0 v",
            "SMOVE a b m",
            "SORT l STORE d",
            "SPOP s",
            "SREM s a",
            "UNLINK k",
            "XACK s g 1-1",
            "XADD s * f v",
            "XDEL s 1-1",
            "XGROUP CREATE s g $ MKSTREAM",
            "XTRIM s MAXLEN 10",
            "ZADD z 1 a",
            "ZINCRBY z 1 a",
            "ZRANGESTORE d z 0 -1",
            "ZREM z a",
            "ZREMRANGEBYRANK z 0 1",
        ];
        for line in writes.iter().chain(round_trip.iter()) {
            let cmd = parse_line(line);
            assert!(cmd.is_write_command(), "{line} should be a write");
            assert!(command_to_resp(&cmd).is_some(), "{line} not encoded");
        }
    }

    /// FT.CREATE / FT.ALTER / FT.DROPINDEX encode to commands that parse
    /// back to the same definition (FT.CREATE in canonical form).
    #[test]
    fn test_search_index_commands_round_trip_through_aof_encoding() {
        let lines = [
            "FT.CREATE i ON HASH PREFIX 2 a: b: SCHEMA t TEXT WEIGHT 2.5 NOSTEM SORTABLE n NUMERIC SORTABLE g TAG SEPARATOR ; CASESENSITIVE",
            "FT.CREATE i SCHEMA t TEXT",
            "FT.CREATE i ON JSON PREFIX 1 u: SCHEMA $.name AS name TEXT $.tags.* AS tags TAG $.age NUMERIC",
            "FT.CREATE i ON HASH SCHEMA v VECTOR HNSW 10 TYPE FLOAT64 DIM 4 DISTANCE_METRIC IP M 8 EF_RUNTIME 20 EPSILON 0.5",
            "FT.CREATE i ON HASH SCHEMA v AS vec VECTOR FLAT 8 TYPE FLOAT32 DIM 3 DISTANCE_METRIC L2 BLOCK_SIZE 64",
            "FT.ALTER i SCHEMA ADD x NUMERIC y AS z TEXT WEIGHT 3 SORTABLE",
            "FT.ALTER i SCHEMA ADD w TAG SEPARATOR |",
            "FT.ALTER i SCHEMA ADD v VECTOR HNSW 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC COSINE",
            "FT.DROPINDEX i",
            "FT.DROPINDEX i DD",
        ];
        for line in lines {
            let cmd = parse_line(line);
            assert!(cmd.is_write_command(), "{line} should be a write");
            let bytes = command_to_resp(&cmd).unwrap_or_else(|| panic!("{line} not encoded"));
            assert_eq!(parse_resp(&bytes), cmd, "{line}");
        }
    }

    /// Replaying a shard's AOF, or a rewrite of the result, redefines its
    /// search indexes and re-indexes its keys; a logged FT.DROPINDEX stays
    /// dropped.
    #[test]
    fn test_search_indexes_survive_aof_replay_and_rewrite() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-ft-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut db = ShardDb::new(6379);
        let aof = std::cell::RefCell::new(AofWriter::new_in_memory());
        let mut out = Vec::new();
        let mut run = |db: &mut ShardDb, line: &str| {
            out.clear();
            crate::connection::execute_local_command(&parse_line(line), db, &mut out, Some(&aof));
            let reply = String::from_utf8_lossy(&out).into_owned();
            assert!(!reply.starts_with('-'), "{line}: {reply}");
        };
        // A document from before the index (backfilled), one after (hooked),
        // one outside the prefix, and an index that is dropped again.
        run(&mut db, "HSET aofft:1 title hello price 5");
        run(
            &mut db,
            "FT.CREATE idx:aofft ON HASH PREFIX 1 aofft: SCHEMA title TEXT",
        );
        run(&mut db, "HSET aofft:2 title hello price 50");
        run(&mut db, "HSET other:3 title hello");
        run(
            &mut db,
            "FT.ALTER idx:aofft SCHEMA ADD price NUMERIC SORTABLE",
        );
        run(
            &mut db,
            "FT.CREATE idx:aofft_gone ON HASH SCHEMA title TEXT",
        );
        run(&mut db, "FT.DROPINDEX idx:aofft_gone");

        let hits = |db: &ShardDb, query: &str| -> Vec<String> {
            let idx = db.search_indices.get("idx:aofft").expect("index defined");
            let ast = crate::search::parse_query(query);
            let (_, hits) =
                crate::search::execute_search(idx, &ast, &crate::search::SearchOptions::default());
            let mut ids: Vec<String> = hits.into_iter().map(|h| h.doc_id).collect();
            ids.sort();
            ids
        };
        let check = |db: &ShardDb| {
            assert_eq!(hits(db, "hello"), ["aofft:1", "aofft:2"]);
            assert_eq!(hits(db, "@price:[10 100]"), ["aofft:2"]);
            assert!(!db.search_indices.contains_key("idx:aofft_gone"));
            let schema = db.search_indices["idx:aofft"].schema.clone().unwrap();
            assert_eq!(schema.schema_fields.len(), 2);
        };
        check(&db);

        let log = dir.join("log.aof");
        std::fs::write(&log, aof.borrow().buffer()).unwrap();
        let mut replayed = ShardDb::new(6379);
        replay_aof(&log, &mut replayed).unwrap();
        check(&replayed);

        rewrite_shard_aof(&mut db, &dir, 0).unwrap();
        let mut rewritten = ShardDb::new(6379);
        replay_aof(&dir.join("appendonly-0.aof"), &mut rewritten).unwrap();
        check(&rewritten);

        let _ = crate::search::drop_search_index("idx:aofft");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Resharding the AOF gives every new shard file the index definitions.
    #[test]
    fn test_reshard_aof_dir_copies_search_indexes_to_every_shard() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-ft-reshard-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let create = command_to_resp(&parse_line(
            "FT.CREATE idx:reshardft ON HASH PREFIX 1 rft: SCHEMA title TEXT",
        ))
        .unwrap();
        let mut old = create.clone();
        for i in 0..20 {
            old.extend_from_slice(&resp_argv(&[
                "HSET".to_string(),
                format!("rft:{i}"),
                "title".to_string(),
                "hello".to_string(),
            ]));
        }
        std::fs::write(dir.join("appendonly-0.aof"), &old).unwrap();
        assert_eq!(reshard_aof_dir(&dir, 3, 0).unwrap(), Some(1));
        let mut total = 0;
        for i in 0..3 {
            let mut db = ShardDb::new(6379);
            replay_aof(&dir.join(format!("appendonly-{i}.aof")), &mut db).unwrap();
            let idx = db
                .search_indices
                .get("idx:reshardft")
                .expect("shard has the index");
            total += idx.key_to_id.len();
        }
        assert_eq!(total, 20, "every document indexed on exactly one shard");
        let _ = crate::search::drop_search_index("idx:reshardft");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every write command reaches the AOF and the replicas: either
    /// `command_to_resp` encodes it, or it is listed below with the path
    /// that logs its change instead. A write command that silently falls
    /// into `command_to_resp`'s wildcard arm is never persisted nor
    /// replicated (as FT.CREATE once was).
    ///
    /// The write commands come from the command table (the `@write`
    /// category, see `acl_categories`), and each needs a sample here, so a
    /// newly added write command fails this test until it is encoded or
    /// listed. The rudis-only families (BF., FT., JSON., ...) share one
    /// table entry each, so their samples list the writing subcommands.
    #[test]
    fn test_every_write_command_is_propagated() {
        // Not encoded on purpose: the handler logs the effect instead.
        const PROPAGATED_ELSEWHERE: &[(&str, &str)] = &[
            (
                "BLPOP l 0",
                "the served pop is logged as LPOP/RPOP (block.rs)",
            ),
            (
                "BRPOP l 0",
                "the served pop is logged as LPOP/RPOP (block.rs)",
            ),
            ("BLMOVE a b LEFT RIGHT 0", "logged as LMOVE"),
            ("BLMOVEM a b LEFT RIGHT 0", "logged as LMOVE/LMOVEM"),
            ("BRPOPLPUSH a b 0", "logged as RPOPLPUSH"),
            ("BLMPOP 0 1 l LEFT", "logged as the LPOP/RPOP of its effect"),
            ("LMPOP 1 l LEFT", "logged as the LPOP/RPOP of its effect"),
            ("BZPOPMAX z 0", "logged as ZPOPMAX"),
            ("BZPOPMIN z 0", "logged as ZPOPMIN"),
            ("BZMPOP 0 1 z MIN", "logged as ZPOPMIN/ZPOPMAX"),
            ("GETEX k PX 100", "logged as EXPIRE/PERSIST"),
            ("INCREX k BYINT 1", "logged as the SET of the result"),
            (
                "GEORADIUS g 15 37 200 km STORE d",
                "logged as DEL + ZADD of the result",
            ),
            (
                "GEORADIUSBYMEMBER g m 100 km STORE d",
                "logged as DEL + ZADD of the result",
            ),
            (
                "MIGRATE 127.0.0.1 1 k 0 1000",
                "logged as the DEL of the moved keys",
            ),
            (
                "XREADGROUP GROUP g c STREAMS s >",
                "logged as XCLAIM / XGROUP SETID",
            ),
            (
                "PFDEBUG TODENSE h",
                "changes the encoding only, not the value",
            ),
        ];
        // Known gaps, kept failing loudly once fixed: FT.ADD documents live
        // only in the global search registry (not in the keyspace, and not
        // seen by FT.SEARCH on shards with the index), so nothing persists
        // them.
        const NOT_PROPAGATED: &[&str] = &["FT.ADD i d 1 FIELDS t v"];
        // In the command table but not implemented: they reply an error.
        const NOT_IMPLEMENTED: &[(&str, &str)] = &[
            ("delifeq", "DELIFEQ k v"),
            ("cluster|flushslot", "CLUSTER FLUSHSLOT 0"),
        ];
        let samples: &[&str] = &[
            "APPEND k v",
            "BF.ADD b x",
            "BF.MADD b x y",
            "BF.RESERVE b 0.01 100",
            "BF.RESTORE b p",
            "BITFIELD k SET i5 0 1",
            "BITOP AND d a b",
            "CF.ADD c x",
            "CF.ADDNX c x",
            "CF.DEL c x",
            "CF.RESERVE c 64",
            "CF.RESTORE c p",
            "CMS.INCRBY m x 1",
            "CMS.INITBYDIM m 10 2",
            "CMS.INITBYPROB m 0.01 0.99",
            "CMS.RESTORE m p",
            "COPY a b",
            "CRDT.MERGE p",
            "CRDT.GC",
            "DECR k",
            "DECRBY k 2",
            "DEL k",
            "DELEX k IFEQ v",
            "EXPIRE k 10",
            "EXPIREAT k 2000000000",
            "FLUSHALL",
            "FLUSHDB",
            "FT.CREATE i ON HASH SCHEMA t TEXT",
            "FT.ALTER i SCHEMA ADD n NUMERIC",
            "FT.DROPINDEX i",
            "FUNCTION DELETE lib",
            "FUNCTION FLUSH",
            "FUNCTION LOAD code",
            "FUNCTION RESTORE payload",
            "GEOADD g 13.361389 38.115556 Palermo",
            "GEOSEARCHSTORE d s FROMLONLAT 15 37 BYRADIUS 200 km",
            "GETDEL k",
            "GETSET k v",
            "HDEL h f",
            "HEXPIRE h 10 FIELDS 1 f",
            "HEXPIREAT h 2000000000 FIELDS 1 f",
            "HGETDEL h FIELDS 1 f",
            "HGETEX h PX 100 FIELDS 1 f",
            "HINCRBY h f 1",
            "HINCRBYFLOAT h f 1.5",
            "HMSET h f v",
            "HPERSIST h FIELDS 1 f",
            "HPEXPIRE h 100 FIELDS 1 f",
            "HPEXPIREAT h 2000000000000 FIELDS 1 f",
            "HSET h f v",
            "HSETEX h PX 100 FIELDS 1 f v",
            "HSETNX h f v",
            "INCR k",
            "INCRBY k 2",
            "INCRBYFLOAT k 1.5",
            "JSON.SET j $ 1",
            "JSON.DEL j",
            "JSON.FORGET j",
            "JSON.CLEAR j",
            "JSON.NUMINCRBY j $ 1",
            "JSON.NUMMULTBY j $ 2",
            "JSON.STRAPPEND j \"x\"",
            "JSON.ARRAPPEND j $ 1",
            "JSON.ARRPOP j",
            "JSON.TOGGLE j $",
            "LINSERT l BEFORE a b",
            "LMOVE a b LEFT RIGHT",
            "LMOVEM a b LEFT RIGHT",
            "LPOP l",
            "LPUSH l a",
            "LPUSHX l a",
            "LREM l 1 a",
            "LSET l 0 a",
            "LTRIM l 0 1",
            "MOVE k 1",
            "MSET a 1 b 2",
            "MSETEX 1 k v PX 100",
            "MSETNX a 1",
            "PERSIST k",
            "PEXPIRE k 100",
            "PEXPIREAT k 2000000000000",
            "PFADD h a",
            "PFMERGE d a b",
            "PSETEX k 100 v",
            "RENAME a b",
            "RENAMENX a b",
            "RESTORE k 0 payload",
            "RESTORE-ASKING k 0 payload",
            "RPOP l",
            "RPOPLPUSH a b",
            "RPUSH l a",
            "RPUSHX l a",
            "SADD s a",
            "SDIFFSTORE d a b",
            "SEMANTIC.SET ns id prompt response VECTOR 2 1 2",
            "SEMANTIC.DEL ns id",
            "SEMANTIC.FLUSH ns",
            "SET k v",
            "SETBIT k 0 1",
            "SETEX k 10 v",
            "SETNX k v",
            "SETRANGE k 0 v",
            "SINTERSTORE d a b",
            "SMOVE a b m",
            "SORT l STORE d",
            "SPOP s",
            "SREM s a",
            "SUNIONSTORE d a b",
            "SWAPDB 0 1",
            "TOPK.ADD t x",
            "TOPK.RESERVE t 10",
            "TOPK.RESTORE t p",
            "UNLINK k",
            "VADD v VALUES 2 1 2 e",
            "VREM v e",
            "VSETATTR v e {}",
            "XACK s g 1-1",
            "XACKDEL s g IDS 1 1-0",
            "XADD s * f v",
            "XAUTOCLAIM s g c 0 0",
            "XCFGSET s IDMP-MAXSIZE 5",
            "XCLAIM s g c 10 1-0",
            "XDEL s 1-1",
            "XDELEX s IDS 1 1-0",
            "XGROUP CREATE s g $ MKSTREAM",
            "XGROUP CREATECONSUMER s g c",
            "XGROUP DELCONSUMER s g c",
            "XGROUP DESTROY s g",
            "XGROUP SETID s g 5-0",
            "XIDMPRECORD s p i 1-0",
            "XNACK s g FAIL IDS 1 1-0",
            "XSETID s 9-9",
            "XTRIM s MAXLEN 10",
            "ZADD z 1 a",
            "ZDIFFSTORE d 2 a b",
            "ZINCRBY z 1 a",
            "ZINTERSTORE d 2 a b",
            "ZMPOP 1 z MIN",
            "ZPOPMAX z",
            "ZPOPMIN z",
            "ZRANGESTORE d z 0 -1",
            "ZREM z a",
            "ZREMRANGEBYLEX z [a [b",
            "ZREMRANGEBYRANK z 0 1",
            "ZREMRANGEBYSCORE z 0 1",
            "ZUNIONSTORE d 2 a b",
            // Rudis-only writes outside the command table.
            "AGENT.MEM.ADD s user hi",
            "AGENT.MEM.CLEAR s",
        ];

        let parse = |line: &str| {
            let mut buf = bytes::BytesMut::from(format!("{line}\r\n").as_str());
            match crate::resp::parse_command(&mut buf) {
                Ok(Some(cmd)) => cmd,
                other => panic!("{line}: {other:?}"),
            }
        };
        // The command table name a sample line exercises: `parent|sub` for
        // container commands, the family (`bf`, `ft`, ...) for dotted ones.
        let table_name = |line: &str| -> String {
            let mut words = line.split(' ').map(str::to_ascii_lowercase);
            let first = words.next().unwrap_or_default();
            let sub = format!("{first}|{}", words.next().unwrap_or_default());
            if crate::acl_categories::COMMANDS
                .iter()
                .any(|c| c.name == sub)
            {
                sub
            } else {
                first.split('.').next().unwrap_or_default().to_string()
            }
        };
        let mut covered = std::collections::HashSet::new();
        let mut unencoded = Vec::new();
        for line in samples {
            let cmd = parse(line);
            covered.insert(table_name(line));
            if command_to_resp(&cmd).is_none() {
                unencoded.push(*line);
            }
        }
        assert!(
            unencoded.is_empty(),
            "write commands never logged: {unencoded:?}"
        );
        let elsewhere = PROPAGATED_ELSEWHERE.iter().map(|(line, _)| *line);
        for line in elsewhere.chain(NOT_PROPAGATED.iter().copied()) {
            let cmd = parse(line);
            assert!(cmd.is_write_command(), "{line} should be a write");
            assert!(
                command_to_resp(&cmd).is_none(),
                "{line} is encoded now: move it to the samples"
            );
            covered.insert(table_name(line));
        }
        for (name, line) in NOT_IMPLEMENTED {
            assert!(
                matches!(parse(line), Command::Unknown(_)),
                "{line} is implemented now: add a sample"
            );
            covered.insert(name.to_string());
        }

        let write_bit = crate::acl_categories::CATEGORIES
            .iter()
            .position(|c| *c == "write")
            .map(|i| 1u32 << i)
            .unwrap();
        let missing: Vec<&str> = crate::acl_categories::COMMANDS
            .iter()
            .filter(|c| c.cats & write_bit != 0 && c.subs.0 == c.subs.1)
            .map(|c| c.name)
            .filter(|n| !covered.contains(*n))
            .collect();
        assert!(
            missing.is_empty(),
            "write commands without a sample: {missing:?}"
        );
    }

    /// A change `command_to_resp` cannot encode is counted (and logged once
    /// per command) instead of silently vanishing from the AOF/replicas.
    #[test]
    fn test_unencodable_changes_are_counted() {
        let cmd = parse_line("PFDEBUG TODENSE h");
        assert!(command_to_resp(&cmd).is_none());
        let before = unpropagated_changes();
        note_unpropagated(&cmd);
        note_unpropagated(&cmd);
        assert!(unpropagated_changes() >= before + 2);
    }

    #[test]
    fn test_appendfsync_parse_and_live_policy() {
        assert_eq!(parse_appendfsync("everysec"), Ok(true));
        assert_eq!(parse_appendfsync("No"), Ok(false));
        assert_eq!(parse_appendfsync("always"), Ok(true));
        assert!(parse_appendfsync("").is_err());

        // Per-port, defaults to everysec, and the flusher's handle sees updates.
        let port = 59_101;
        assert_eq!(appendfsync_name(port), "everysec");
        let flag = fsync_every_sec_flag(port);
        set_fsync_every_sec(port, false);
        assert!(!flag.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(appendfsync_name(port), "no");
        assert_eq!(appendfsync_name(59_102), "everysec");
        set_fsync_every_sec(port, true);
        assert!(flag.load(std::sync::atomic::Ordering::Relaxed));
    }

    const SET_A: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";

    /// Replays each `appendonly-{i}.aof` and checks every key sits in the
    /// shard that owns it. Returns the total key count.
    fn assert_aof_keys_owned(dir: &Path, num_shards: usize) -> usize {
        let mut total = 0;
        for i in 0..num_shards {
            let mut db = ShardDb::new(0);
            replay_aof(&dir.join(format!("appendonly-{}.aof", i)), &mut db).unwrap();
            for e in db.table.entries() {
                assert_eq!(
                    crate::router::target_shard(&e.key, num_shards),
                    i,
                    "key {:?} in wrong shard file",
                    e.key
                );
            }
            total += db.table.dbsize();
        }
        assert!(!dir.join(format!("appendonly-{}.aof", num_shards)).exists());
        total
    }

    #[test]
    fn test_reshard_aof_dir_moves_keys_to_owning_shards() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-reshard-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Legacy layout (no manifest), written by 4 shards.
        let mut dbs: Vec<ShardDb> = (0..4).map(|_| ShardDb::new(0)).collect();
        for k in 0..100 {
            let key = Bytes::from(format!("k{}", k));
            let ttl = (k % 10 == 0).then(|| Duration::from_secs(1000));
            dbs[crate::router::target_shard(&key, 4)].set(key, Bytes::from("v"), ttl);
        }
        for (i, db) in dbs.iter_mut().enumerate() {
            rewrite_shard_aof(db, &dir, i).unwrap();
        }

        assert_eq!(reshard_aof_dir(&dir, 2, 0).unwrap(), Some(4));
        assert_eq!(assert_aof_keys_owned(&dir, 2), 100);
        assert_eq!(
            read_aof_shards_manifest(&dir).unwrap(),
            Some(AofLayout::current(2))
        );
        assert!(!dir.join(AOF_RESHARD_MARKER).exists());
        assert!(!dir.join(AOF_RESHARD_STAGING).exists());
        let backups: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("aof-reshard-backup-4to2-")
            })
            .collect();
        assert_eq!(backups.len(), 1);
        assert_eq!(std::fs::read_dir(backups[0].path()).unwrap().count(), 4);

        // Same layout again: nothing to do.
        assert_eq!(reshard_aof_dir(&dir, 2, 0).unwrap(), None);
        // Grow, and TTLs survive the round trips.
        assert_eq!(reshard_aof_dir(&dir, 8, 0).unwrap(), Some(2));
        assert_eq!(assert_aof_keys_owned(&dir, 8), 100);
        let key = Bytes::from("k10");
        let mut db = ShardDb::new(0);
        let owner = crate::router::target_shard(&key, 8);
        replay_aof(&dir.join(format!("appendonly-{}.aof", owner)), &mut db).unwrap();
        assert!(
            db.table
                .entries()
                .any(|e| e.key == key && e.expire_at().is_some())
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_reshard_aof_dir_refuses_after_interrupted_swap_or_bad_manifest() {
        let dir =
            std::env::temp_dir().join(format!("rudis-aof-reshard-bad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("appendonly-0.aof"), SET_A).unwrap();

        std::fs::write(dir.join(AOF_RESHARD_MARKER), "/backup/here").unwrap();
        let err = reshard_aof_dir(&dir, 1, 0).unwrap_err();
        assert!(err.to_string().contains("/backup/here"), "{err}");
        std::fs::remove_file(dir.join(AOF_RESHARD_MARKER)).unwrap();

        // Manifest says 1 shard but a second shard file exists.
        std::fs::write(dir.join(AOF_SHARDS_MANIFEST), "shards 1\nrouting hash\n").unwrap();
        std::fs::write(dir.join("appendonly-1.aof"), b"").unwrap();
        assert!(reshard_aof_dir(&dir, 2, 0).is_err());
        std::fs::remove_file(dir.join("appendonly-1.aof")).unwrap();

        // Staging leftovers from a crash before the swap are discarded.
        std::fs::create_dir_all(dir.join(AOF_RESHARD_STAGING)).unwrap();
        assert_eq!(reshard_aof_dir(&dir, 1, 0).unwrap(), None);
        assert!(!dir.join(AOF_RESHARD_STAGING).exists());

        std::fs::write(dir.join(AOF_SHARDS_MANIFEST), "garbage").unwrap();
        assert!(reshard_aof_dir(&dir, 1, 0).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_raw_resp_arg() {
        assert_eq!(raw_resp_arg(SET_A, 0), Some(&b"SET"[..]));
        assert_eq!(raw_resp_arg(SET_A, 1), Some(&b"a"[..]));
        assert_eq!(raw_resp_arg(SET_A, 2), Some(&b"1"[..]));
        assert_eq!(raw_resp_arg(SET_A, 3), None);
        assert_eq!(raw_resp_arg(b"*2\r\n$3\r\nGET", 1), None);
    }

    #[test]
    fn test_replay_aof_truncated_tail_is_cut_when_allowed() {
        let mut content = SET_A.to_vec();
        content.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1"); // crash mid-write
        let p = aof_tmp("trunc-yes", &content);
        let mut db = ShardDb::new(0);
        assert_eq!(replay_aof_with(&p, &mut db, true).unwrap(), 1);
        assert_eq!(db.table.dbsize(), 1);
        // File now ends at the last complete command, so new appends are readable.
        assert_eq!(std::fs::read(&p).unwrap(), SET_A);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn test_replay_aof_truncated_tail_is_fatal_when_disallowed() {
        let mut content = SET_A.to_vec();
        content.extend_from_slice(b"*3\r\n$3\r\nSET");
        let p = aof_tmp("trunc-no", &content);
        let mut db = ShardDb::new(0);
        let err = replay_aof_with(&p, &mut db, false).unwrap_err();
        assert!(err.to_string().contains("aof-load-truncated"), "{err}");
        assert_eq!(
            std::fs::read(&p).unwrap(),
            content,
            "file must be untouched"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn test_replay_aof_mid_file_corruption_is_fatal() {
        let mut content = SET_A.to_vec();
        content.extend_from_slice(b"garbage\r\n");
        content.extend_from_slice(SET_A);
        let p = aof_tmp("corrupt", &content);
        let mut db = ShardDb::new(0);
        let err = replay_aof_with(&p, &mut db, true).unwrap_err();
        assert!(
            err.to_string().contains(&format!("offset {}", SET_A.len())),
            "{err}"
        );
        // Bad multibulk header is corruption too, even with aof-load-truncated.
        let p2 = aof_tmp("corrupt2", b"*x\r\n");
        assert!(replay_aof_with(&p2, &mut db, true).is_err());
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(&p2);
    }

    #[test]
    fn test_aof_expiries_are_absolute_so_downtime_does_not_extend_ttl() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-abs-exp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Live AOF: SET, then EXPIRE/PEXPIRE propagated as an absolute deadline.
        let expire = Command::Expire {
            key: Bytes::from_static(b"live"),
            duration: Duration::from_millis(50),
            opts: Default::default(),
        };
        let resp = command_to_resp(&expire).unwrap();
        assert!(
            resp.starts_with(b"*3\r\n$9\r\nPEXPIREAT\r\n"),
            "{:?}",
            String::from_utf8_lossy(&resp)
        );
        let mut live = SET_A.to_vec(); // SET a 1 (no TTL)
        live.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\nlive\r\n$1\r\nx\r\n");
        live.extend_from_slice(&resp);
        let live_path = dir.join("live.aof");
        std::fs::write(&live_path, &live).unwrap();

        // Rewrite output for keys with TTLs (string and non-string).
        let mut src = ShardDb::new(0);
        src.set(
            Bytes::from("rw_str"),
            Bytes::from("v"),
            Some(Duration::from_millis(50)),
        );
        src.rpush(Bytes::from("rw_list"), vec![Bytes::from("e")])
            .unwrap();
        src.table
            .expire(b"rw_list", Duration::from_millis(50), Default::default());
        rewrite_shard_aof(&mut src, &dir, 0).unwrap();

        // "Downtime" longer than every TTL before replaying.
        std::thread::sleep(Duration::from_millis(120));

        // Like Redis, nothing expires during the load itself: keys whose
        // deadline passed are kept until right after it (lazy or active
        // expiry), so count the keys still live, not the keys present.
        let live = |db: &ShardDb| {
            let now = std::time::Instant::now();
            db.table
                .entries()
                .filter(|e| e.expire_at().is_none_or(|t| t > now))
                .count()
        };
        let mut db = ShardDb::new(0);
        replay_aof_with(&live_path, &mut db, true).unwrap();
        assert_eq!(live(&db), 1, "only the key without TTL may survive");

        let mut db = ShardDb::new(0);
        replay_aof_with(&dir.join("appendonly-0.aof"), &mut db, true).unwrap();
        assert_eq!(live(&db), 0, "rewritten keys must not outlive their TTL");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_aof_compaction_and_rewrite() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-aof-rewrite-unit-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let mut db = ShardDb::new(6379);

        // 1. Populate various data types in ShardDb
        db.set(Bytes::from("str1"), Bytes::from("val1"), None);
        db.set(
            Bytes::from("str2"),
            Bytes::from("val2"),
            Some(Duration::from_secs(3600)),
        );
        db.hset(
            Bytes::from("hash1"),
            vec![
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        )
        .unwrap();
        db.rpush(
            Bytes::from("list1"),
            vec![Bytes::from("elem1"), Bytes::from("elem2")],
        )
        .unwrap();
        db.sadd(
            Bytes::from("set1"),
            vec![Bytes::from("m1"), Bytes::from("m2")],
        )
        .unwrap();
        db.zadd(
            Bytes::from("zset1"),
            vec![(10.5, Bytes::from("z1")), (20.0, Bytes::from("z2"))],
            crate::table::ZAddFlags::default(),
        )
        .unwrap();
        db.json_store
            .json_set(b"doc1", "$", r#"{"title":"rewrite"}"#, false, false)
            .unwrap();

        // 2. Perform AOF rewrite
        let rewritten_count =
            rewrite_shard_aof(&mut db, &temp_dir, 0).expect("rewrite should succeed");
        assert_eq!(rewritten_count, 7);

        let aof_file = temp_dir.join("appendonly-0.aof");
        assert!(aof_file.exists());

        // 3. Replay rewritten AOF into fresh ShardDb and verify all state is restored
        let mut new_db = ShardDb::new(6379);
        let replayed = replay_aof(&aof_file, &mut new_db).expect("replay should succeed");
        assert_eq!(replayed, 7);

        assert_eq!(new_db.get(b"str1"), Some(Bytes::from("val1")));
        assert_eq!(new_db.get(b"str2"), Some(Bytes::from("val2")));
        assert_eq!(
            new_db.hget(b"hash1", b"f1").unwrap(),
            Some(Bytes::from("v1"))
        );
        assert_eq!(
            new_db.lrange(b"list1", 0, -1).unwrap(),
            vec![Bytes::from("elem1"), Bytes::from("elem2")]
        );
        assert!(new_db.sismember(b"set1", b"m1").unwrap());
        assert_eq!(new_db.zscore(b"zset1", b"z1").unwrap(), Some(10.5));
        assert!(
            new_db
                .json_store
                .json_get(b"doc1", &["$"])
                .unwrap()
                .contains("rewrite")
        );

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_aof_writer_reopen_and_continuous_logging() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-aof-reopen-unit-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let aof_file = temp_dir.join("appendonly-0.aof");

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .build()
            .unwrap();

        rt.block_on(async {
            let writer = std::rc::Rc::new(std::cell::RefCell::new(
                AofWriter::open(aof_file.clone()).await.unwrap(),
            ));
            writer
                .borrow_mut()
                .append(b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n");
            AofWriter::flush_rc(&writer).await.unwrap();

            // Populate db with k1 and rewrite
            let mut db = ShardDb::new(6379);
            db.set(Bytes::from("k1"), Bytes::from("v1"), None);
            let count = rewrite_shard_aof(&mut db, &temp_dir, 0).unwrap();
            assert_eq!(count, 1);

            // Switch the writer to the rewritten file and log k2
            let new_size = writer.borrow_mut().swap_after_rewrite().unwrap();
            assert!(new_size > 0);
            writer
                .borrow_mut()
                .append(b"*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
            AofWriter::flush_rc(&writer).await.unwrap();

            // Replay and verify both k1 and k2
            let mut new_db = ShardDb::new(6379);
            let replayed = replay_aof(&aof_file, &mut new_db).unwrap();
            assert_eq!(replayed, 2);
            assert_eq!(new_db.get(b"k1"), Some(Bytes::from("v1")));
            assert_eq!(new_db.get(b"k2"), Some(Bytes::from("v2")));
        });

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_rewrite_swap_neither_duplicates_nor_loses_writes() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-aof-swap-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");
        const INCR: &[u8] = b"*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n";

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .build()
            .unwrap();
        rt.block_on(async {
            let writer = std::rc::Rc::new(std::cell::RefCell::new(
                AofWriter::open(aof_file.clone()).await.unwrap(),
            ));
            let mut db = ShardDb::new(6379);
            // Two INCRs applied; the second is still only in the AOF buffer
            // when the rewrite snapshots the db.
            for flush in [true, false] {
                db.incr_by(Bytes::from("n"), 1).unwrap();
                writer.borrow_mut().append(INCR);
                if flush {
                    AofWriter::flush_rc(&writer).await.unwrap();
                }
            }
            rewrite_and_swap_shard_aof(&mut db, &temp_dir, 0, Some(&writer)).unwrap();
            // A write after the swap must land in the new file.
            db.incr_by(Bytes::from("n"), 1).unwrap();
            writer.borrow_mut().append(INCR);
            AofWriter::flush_rc(&writer).await.unwrap();

            let mut replayed = ShardDb::new(6379);
            replay_aof(&aof_file, &mut replayed).unwrap();
            assert_eq!(replayed.get(b"n"), Some(Bytes::from("3")));
        });
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_full_sync_load_rewrites_the_aof() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-aof-fullsync-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");

        let mut master = ShardDb::new(6379);
        master.set(Bytes::from("fresh"), Bytes::from("1"), None);
        let mut rdb = b"REDIS0011\xFE\x00".to_vec();
        master.save_rdb_chunk(&mut rdb).unwrap();
        rdb.push(0xFF);
        let crc = crate::table::crc64(&rdb);
        rdb.extend_from_slice(&crc.to_le_bytes());

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .build()
            .unwrap();
        rt.block_on(async {
            let writer = std::rc::Rc::new(std::cell::RefCell::new(
                AofWriter::open(aof_file.clone()).await.unwrap(),
            ));
            let mut replica = ShardDb::new(6379);
            replica.set(Bytes::from("stale"), Bytes::from("1"), None);
            writer
                .borrow_mut()
                .append(b"*3\r\n$3\r\nSET\r\n$5\r\nstale\r\n$1\r\n1\r\n");
            AofWriter::flush_rc(&writer).await.unwrap();

            replica
                .load_full_sync_rdb(&rdb, 0, 1, Some((&temp_dir, &writer)))
                .unwrap();
            let mut replayed = ShardDb::new(6379);
            replay_aof(&aof_file, &mut replayed).unwrap();
            assert_eq!(replayed.get(b"fresh"), Some(Bytes::from("1")));
            assert_eq!(replayed.get(b"stale"), None);

            // A failed load leaves both the shard and its AOF empty.
            assert!(
                replica
                    .load_full_sync_rdb(&rdb[..rdb.len() - 1], 0, 1, Some((&temp_dir, &writer)))
                    .is_err()
            );
            let mut replayed = ShardDb::new(6379);
            replay_aof(&aof_file, &mut replayed).unwrap();
            assert_eq!(replayed.dbsize(), 0);
        });
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_sync_parent_dir_durability() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-sync-dir-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let test_file = temp_dir.join("test.txt");
        std::fs::write(&test_file, b"data").unwrap();

        assert!(sync_parent_dir(&test_file).is_ok());

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_rdb_and_aof_hash_field_expiration_persistence() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-hexpire-persist-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let mut db = ShardDb::new(6379);
        db.hset(
            Bytes::from("session_h"),
            vec![
                (Bytes::from("token"), Bytes::from("abc123")),
                (Bytes::from("perm"), Bytes::from("admin")),
            ],
        )
        .unwrap();
        let res = db
            .table
            .hexpire(
                b"session_h",
                60_000,
                false,
                crate::resp::HexpireCondition::None,
                &[Bytes::from("token")],
            )
            .unwrap();
        assert_eq!(res, vec![1]);

        // 1. Verify RDB chunk round-trip preserves field TTL
        let mut rdb_chunk = Vec::new();
        db.save_rdb_chunk(&mut rdb_chunk).unwrap();

        let mut restored_rdb_db = ShardDb::new(6379);
        restored_rdb_db.restore_rdb_chunk(&rdb_chunk).unwrap();
        let ttls = restored_rdb_db
            .table
            .httl(
                b"session_h",
                false,
                false,
                &[Bytes::from("token"), Bytes::from("perm")],
            )
            .unwrap();
        assert!(ttls[0] > 0 && ttls[0] <= 60);
        assert_eq!(ttls[1], -1);

        // 2. Verify AOF rewrite & replay preserves field TTL
        rewrite_shard_aof(&mut db, &temp_dir, 0).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");
        let mut restored_aof_db = ShardDb::new(6379);
        replay_aof(&aof_file, &mut restored_aof_db).unwrap();
        let aof_ttls = restored_aof_db
            .table
            .httl(
                b"session_h",
                false,
                false,
                &[Bytes::from("token"), Bytes::from("perm")],
            )
            .unwrap();
        assert!(aof_ttls[0] > 0 && aof_ttls[0] <= 60);
        assert_eq!(aof_ttls[1], -1);

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_rdb_and_aof_vector_set_persistence() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-vset-persist-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let mut db = ShardDb::new(6379);
        db.vadd_ext(
            "movies",
            Bytes::from("m1"),
            vec![1.0, 0.0, 0.0],
            None,
            false,
            false,
            false,
            None,
            Some(crate::vector::VQuant::Q8),
            Some(64),
            Some(r#"{"genre":"sci-fi","year":1999}"#.to_string()),
            Some(24),
            true,
        )
        .unwrap();
        db.vadd_ext(
            "movies",
            Bytes::from("m2"),
            vec![0.0, 1.0, 0.0],
            None,
            false,
            false,
            false,
            None,
            Some(crate::vector::VQuant::Q8),
            None,
            Some(r#"{"genre":"drama","year":2020}"#.to_string()),
            Some(24),
            true,
        )
        .unwrap();

        // 1. Extended RDB round-trip
        let mut rdb_chunk = Vec::new();
        db.save_extended_rdb_chunk(&mut rdb_chunk);
        let mut restored_rdb = ShardDb::new(6379);
        restored_rdb.restore_rdb_chunk(&rdb_chunk).unwrap();
        let idx = restored_rdb.vector_indexes.get("movies").unwrap();
        assert!(idx.is_redis_vset);
        assert_eq!(idx.quant, crate::vector::VQuant::Q8);
        assert_eq!(idx.m, 24);
        assert_eq!(idx.len(), 2);
        assert_eq!(
            restored_rdb.vgetattr("movies", &Bytes::from("m1")).unwrap(),
            Some(r#"{"genre":"sci-fi","year":1999}"#.to_string())
        );

        // 2. AOF rewrite & replay round-trip
        rewrite_shard_aof(&mut db, &temp_dir, 0).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");
        let mut restored_aof = ShardDb::new(6379);
        replay_aof(&aof_file, &mut restored_aof).unwrap();
        let aof_idx = restored_aof.vector_indexes.get("movies").unwrap();
        assert!(aof_idx.is_redis_vset);
        assert_eq!(aof_idx.quant, crate::vector::VQuant::Q8);
        assert_eq!(aof_idx.m, 24);
        assert_eq!(aof_idx.len(), 2);
        assert_eq!(
            restored_aof.vgetattr("movies", &Bytes::from("m2")).unwrap(),
            Some(r#"{"genre":"drama","year":2020}"#.to_string())
        );

        // 3. Incremental VSETATTR and VREM command_to_resp replay
        let setattr_cmd = Command::Vsetattr {
            key: Bytes::from("movies"),
            element: Bytes::from("m1"),
            attr: r#"{"genre":"action","year":2001}"#.to_string(),
        };
        let vrem_cmd = Command::Vdel {
            key: Bytes::from("movies"),
            element: Bytes::from("m2"),
        };
        let mut stream = command_to_resp(&setattr_cmd).unwrap();
        stream.extend_from_slice(&command_to_resp(&vrem_cmd).unwrap());
        std::fs::write(&aof_file, &stream).unwrap();
        replay_aof(&aof_file, &mut restored_aof).unwrap();
        assert_eq!(restored_aof.vector_indexes.get("movies").unwrap().len(), 1);
        assert_eq!(
            restored_aof.vgetattr("movies", &Bytes::from("m1")).unwrap(),
            Some(r#"{"genre":"action","year":2001}"#.to_string())
        );

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_rdb_and_aof_ai_native_state_persistence() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-ai-persist-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let mut db = ShardDb::new(6379);

        // 1. Populate Semantic Cache
        db.semantic_set(
            Bytes::from("llm:prod"),
            Bytes::from("q1"),
            Bytes::from("What is Rudis?"),
            Bytes::from("Rudis is a thread-per-core AI-native data platform."),
            vec![1.0, 0.0, 0.0],
            Some(Duration::from_secs(3600)),
            Some(Bytes::from("tenant:acme")),
            false,
            Some(42),
        )
        .unwrap();

        // 2. Populate Agent Memory (with compaction + episodic vectors)
        db.agent_mem_add(
            Bytes::from("sess:agent1"),
            Bytes::from("user"),
            Bytes::from(" Secret code is 7788."),
            Some(8),
            Some(vec![1.0, 0.0, 0.0]),
            Some(Bytes::from(r#"{"turn":1}"#)),
        )
        .unwrap();
        db.agent_mem_add(
            Bytes::from("sess:agent1"),
            Bytes::from("assistant"),
            Bytes::from("Understood, I stored 7788."),
            Some(8),
            Some(vec![0.9, 0.1, 0.0]),
            None,
        )
        .unwrap();
        let compacted = db
            .agent_mem_compact(
                &Bytes::from("sess:agent1"),
                0,
                Bytes::from("User shared secret code."),
                Some(6),
                Some(vec![0.5, 0.5, 0.0]),
            )
            .unwrap();
        assert_eq!(compacted, 2);
        db.agent_mem_add(
            Bytes::from("sess:agent1"),
            Bytes::from("user"),
            Bytes::from("Now help me deploy."),
            Some(5),
            Some(vec![0.0, 1.0, 0.0]),
            None,
        )
        .unwrap();

        // 3. Populate Agent Checkpoints (DAG lineage)
        db.agent_checkpoint_put(
            Bytes::from("thread:wf1"),
            Bytes::from("step1"),
            None,
            Bytes::from(r#"{"phase":"plan"}"#),
            Some(Bytes::from(r#"{"agent":"planner"}"#)),
        );
        db.agent_checkpoint_put(
            Bytes::from("thread:wf1"),
            Bytes::from("step2"),
            Some(Bytes::from("step1")),
            Bytes::from(r#"{"phase":"act"}"#),
            Some(Bytes::from(r#"{"agent":"executor"}"#)),
        );

        // 4. Populate Agent Tool Registry (one completed, one in-flight lease)
        let claim_done = db.agent_tool_claim(
            Bytes::from("tools:wf1"),
            Bytes::from("call:done"),
            60_000,
            Some(Bytes::from(r#"{"q":"weather"}"#)),
        );
        assert_eq!(claim_done.state, crate::agent::ToolClaimState::Claimed);
        db.agent_tool_complete(
            Bytes::from("tools:wf1"),
            Bytes::from("call:done"),
            Bytes::from(r#"{"temp":72}"#),
            Some(3_600_000),
        );
        let claim_inflight = db.agent_tool_claim(
            Bytes::from("tools:wf1"),
            Bytes::from("call:inflight"),
            60_000,
            Some(Bytes::from(r#"{"q":"search"}"#)),
        );
        assert_eq!(claim_inflight.state, crate::agent::ToolClaimState::Claimed);

        let verify_restored = |target: &mut ShardDb| {
            // Verify Semantic Cache
            let hit = target
                .semantic_get(
                    &Bytes::from("llm:prod"),
                    &[1.0, 0.0, 0.0],
                    0.95,
                    Some(b"tenant:acme"),
                )
                .unwrap()
                .expect("semantic cache hit expected");
            assert_eq!(hit.id, Bytes::from("q1"));
            assert_eq!(
                hit.response,
                Bytes::from("Rudis is a thread-per-core AI-native data platform.")
            );

            // Verify Agent Memory working window + episodic recall
            let ctx = target
                .agent_mem_context(&Bytes::from("sess:agent1"), 100, Some(&[1.0, 0.0, 0.0]), 2)
                .unwrap();
            assert_eq!(ctx.recent_turns.len(), 2);
            assert_eq!(
                ctx.recent_turns[0].content,
                Bytes::from("User shared secret code.")
            );
            assert_eq!(
                ctx.recent_turns[1].content,
                Bytes::from("Now help me deploy.")
            );
            assert!(!ctx.recalled_episodes.is_empty());
            assert_eq!(
                ctx.recalled_episodes[0].content,
                Bytes::from(" Secret code is 7788.")
            );

            // Verify Agent Checkpoint DAG history
            let history = target.agent_checkpoint_history(&Bytes::from("thread:wf1"), None, 10);
            assert_eq!(history.len(), 2);
            assert_eq!(history[0].step_id, Bytes::from("step2"));
            assert_eq!(history[0].parent_id, Some(Bytes::from("step1")));
            assert_eq!(history[1].step_id, Bytes::from("step1"));

            // Verify Agent Tool Registry (completed & in-flight)
            let check_done = target.agent_tool_claim(
                Bytes::from("tools:wf1"),
                Bytes::from("call:done"),
                30_000,
                None,
            );
            assert_eq!(check_done.state, crate::agent::ToolClaimState::Completed);
            assert_eq!(check_done.output, Some(Bytes::from(r#"{"temp":72}"#)));

            let check_inflight = target.agent_tool_claim(
                Bytes::from("tools:wf1"),
                Bytes::from("call:inflight"),
                30_000,
                None,
            );
            assert_eq!(
                check_inflight.state,
                crate::agent::ToolClaimState::InProgress
            );
        };

        // Test 1: RDB chunk save & restore round-trip
        let mut rdb_chunk = Vec::new();
        db.save_rdb_chunk(&mut rdb_chunk).unwrap();
        let mut rdb_restored = ShardDb::new(6379);
        rdb_restored.restore_rdb_chunk(&rdb_chunk).unwrap();
        verify_restored(&mut rdb_restored);

        // Test 2: AOF rewrite & replay round-trip
        rewrite_shard_aof(&mut db, &temp_dir, 0).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");
        let mut aof_restored = ShardDb::new(6379);
        replay_aof(&aof_file, &mut aof_restored).unwrap();
        verify_restored(&mut aof_restored);

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_aof_rewrite_keeps_tiered_values_and_types() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-tier-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut db = ShardDb::new(0);
            let tm = std::rc::Rc::new(
                crate::tiering::ShardTierManager::open(0, 55557, &dir.join("tier"))
                    .await
                    .unwrap(),
            );
            db.tier_manager = Some(tm.clone());
            db.set(
                bytes::Bytes::from_static(b"s"),
                bytes::Bytes::from_static(b"plain-value"),
                Some(std::time::Duration::from_secs(1000)),
            );
            db.hset(
                bytes::Bytes::from_static(b"h"),
                vec![(
                    bytes::Bytes::from_static(b"f"),
                    bytes::Bytes::from_static(b"v"),
                )],
            )
            .unwrap();
            db.zadd(
                bytes::Bytes::from_static(b"z"),
                vec![(2.5, bytes::Bytes::from_static(b"m"))],
                crate::table::ZAddFlags::default(),
            )
            .unwrap();
            for key in [&b"s"[..], b"h", b"z"] {
                let (payload, val_type) = db.table.get_value_for_spill(key).unwrap();
                let ptr = tm
                    .stash_record(&bytes::Bytes::copy_from_slice(key), &payload, val_type)
                    .await
                    .unwrap();
                assert!(db.table.set_tiered_pointer(key, ptr));
            }

            rewrite_shard_aof(&mut db, &dir, 0).unwrap();
            let mut restored = ShardDb::new(0);
            replay_aof(&dir.join("appendonly-0.aof"), &mut restored).unwrap();
            assert_eq!(
                restored.get(b"s"),
                Some(bytes::Bytes::from_static(b"plain-value"))
            );
            assert!(restored.ttl(b"s", false) > 900);
            assert_eq!(
                restored.hget(b"h", b"f").unwrap(),
                Some(bytes::Bytes::from_static(b"v"))
            );
            assert_eq!(restored.zscore(b"z", b"m").unwrap(), Some(2.5));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_aof_rewrite_fails_on_unreadable_tiered_value_and_keeps_old_aof() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-tier-fail-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut db = ShardDb::new(0);
            let tm = std::rc::Rc::new(
                crate::tiering::ShardTierManager::open(0, 55581, &dir.join("tier"))
                    .await
                    .unwrap(),
            );
            db.tier_manager = Some(tm.clone());
            db.set(
                bytes::Bytes::from_static(b"kept"),
                bytes::Bytes::from_static(b"k"),
                None,
            );
            // In DB 1, so the failure happens with another DB selected.
            db.select_db(1);
            // Big enough to bypass the in-memory small bins.
            db.set(
                bytes::Bytes::from_static(b"lost"),
                bytes::Bytes::from(vec![b'v'; 64 * 1024]),
                None,
            );
            let (payload, val_type) = db.table.get_value_for_spill(b"lost").unwrap();
            let ptr = tm
                .stash_record(&bytes::Bytes::from_static(b"lost"), &payload, val_type)
                .await
                .unwrap();
            assert!(db.table.set_tiered_pointer(b"lost", ptr));
            db.select_db(0);

            // A complete AOF from before the fault.
            rewrite_shard_aof(&mut db, &dir, 0).unwrap();
            let aof_path = dir.join("appendonly-0.aof");
            let before = std::fs::read(&aof_path).unwrap();

            std::fs::OpenOptions::new()
                .write(true)
                .open(&tm.path)
                .unwrap()
                .set_len(0)
                .unwrap();

            let err = rewrite_shard_aof(&mut db, &dir, 0).unwrap_err();
            assert!(err.to_string().contains("'lost'"), "{err}");
            // The old AOF is untouched, no temp file is left behind, and
            // the selected DB is restored.
            assert_eq!(std::fs::read(&aof_path).unwrap(), before);
            let leftovers: Vec<_> = std::fs::read_dir(&dir)
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.contains(".tmp."))
                .collect();
            assert!(leftovers.is_empty(), "{leftovers:?}");
            assert_eq!(db.current_db, 0);

            let mut restored = ShardDb::new(0);
            replay_aof(&aof_path, &mut restored).unwrap();
            restored.select_db(1);
            assert_eq!(restored.get(b"lost").map(|v| v.len()), Some(64 * 1024));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A Redis-format RDB file holding `db`'s keys.
    fn redis_rdb_of(db: &mut ShardDb) -> Vec<u8> {
        let mut file = Vec::new();
        crate::redis_rdb::write_file_header(&mut file, 0);
        db.save_rdb_chunk(&mut file).unwrap();
        file.push(crate::redis_rdb::OP_EOF);
        let crc = crate::table::crc64(&file);
        file.extend_from_slice(&crc.to_le_bytes());
        file
    }

    /// A Redis 7 multi-part AOF dir: a base RDB plus an incremental AOF.
    fn write_redis7_aof_dir(dir: &Path, incr: &[u8]) {
        let aof_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&aof_dir).unwrap();
        let mut base_db = ShardDb::new(0);
        base_db.set(
            Bytes::from_static(b"k_base"),
            Bytes::from_static(b"v_base"),
            None,
        );
        std::fs::write(
            aof_dir.join("appendonly.aof.1.base.rdb"),
            redis_rdb_of(&mut base_db),
        )
        .unwrap();
        std::fs::write(aof_dir.join("appendonly.aof.1.incr.aof"), incr).unwrap();
        std::fs::write(
            aof_dir.join("appendonly.aof.manifest"),
            "file appendonly.aof.1.base.rdb seq 1 type b\n\
             file appendonly.aof.0.incr.aof seq 1 type h\n\
             file appendonly.aof.1.incr.aof seq 1 type i\n",
        )
        .unwrap();
    }

    fn redis7_incr_sample() -> Vec<u8> {
        let mut incr = Vec::new();
        incr.extend_from_slice(b"#TS:1700000000\r\n");
        incr.extend_from_slice(&resp_argv(&[b"MULTI".as_slice()]));
        incr.extend_from_slice(&resp_argv(&[b"SET".as_slice(), b"k_tx", b"v_tx"]));
        incr.extend_from_slice(&resp_argv(&[b"EXEC".as_slice()]));
        // Redis applied the second RPUSH to the list it had not yet expired,
        // so the whole list is gone once loaded.
        incr.extend_from_slice(&resp_argv(&[b"RPUSH".as_slice(), b"exp_list", b"foo"]));
        incr.extend_from_slice(&resp_argv(&[b"PEXPIREAT".as_slice(), b"exp_list", b"1000"]));
        incr.extend_from_slice(&resp_argv(&[b"RPUSH".as_slice(), b"exp_list", b"bar"]));
        incr
    }

    fn load_shards(dir: &Path, n: usize) -> ShardDb {
        let mut loaded = ShardDb::new(0);
        for i in 0..n {
            replay_aof(&dir.join(format!("appendonly-{}.aof", i)), &mut loaded).unwrap();
        }
        loaded
    }

    #[test]
    fn test_redis_multipart_aof_manifest_and_past_pexpireat_replay() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-mp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        write_redis7_aof_dir(&dir, &redis7_incr_sample());

        assert_eq!(reshard_aof_dir(&dir, 2, 0).unwrap(), None);
        let mut loaded = load_shards(&dir, 2);
        assert_eq!(loaded.get(b"k_base"), Some(Bytes::from_static(b"v_base")));
        assert_eq!(loaded.get(b"k_tx"), Some(Bytes::from_static(b"v_tx")));
        assert_eq!(loaded.llen(b"exp_list").unwrap(), 0);
        // The expired list was left out of the import, not kept until accessed.
        assert_eq!(loaded.table.dbsize(), 2);
        // The import flag is per-thread and was reset.
        assert!(!crate::connection::REPLAYING_WRITES.get());

        // A restart with unchanged Redis files keeps rudis's own writes.
        let shard0 = dir.join("appendonly-0.aof");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&shard0)
            .unwrap();
        std::io::Write::write_all(
            &mut f,
            &resp_argv(&[b"SET".as_slice(), b"after_import", b"1"]),
        )
        .unwrap();
        drop(f);
        assert_eq!(reshard_aof_dir(&dir, 2, 0).unwrap(), None);
        let mut loaded = load_shards(&dir, 2);
        assert_eq!(loaded.get(b"after_import"), Some(Bytes::from_static(b"1")));

        // A reshard keeps the record of what was imported.
        assert_eq!(reshard_aof_dir(&dir, 3, 0).unwrap(), Some(2));
        assert!(read_aof_shards_manifest_ext_crc(&dir).is_some());
        assert_eq!(reshard_aof_dir(&dir, 3, 0).unwrap(), None);
        let mut loaded = load_shards(&dir, 3);
        assert_eq!(loaded.get(b"after_import"), Some(Bytes::from_static(b"1")));

        // Removing the Redis files keeps the imported data.
        std::fs::remove_dir_all(dir.join("appendonlydir")).unwrap();
        assert_eq!(reshard_aof_dir(&dir, 3, 0).unwrap(), None);
        let mut loaded = load_shards(&dir, 3);
        assert_eq!(loaded.get(b"k_base"), Some(Bytes::from_static(b"v_base")));
        assert_eq!(loaded.get(b"after_import"), Some(Bytes::from_static(b"1")));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_changed_redis_aof_is_reimported_with_a_backup() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-reimport-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let single = dir.join("appendonly.aof");
        std::fs::write(&single, resp_argv(&[b"SET".as_slice(), b"a", b"1"])).unwrap();
        assert_eq!(reshard_aof_dir(&dir, 1, 0).unwrap(), None);
        assert_eq!(
            load_shards(&dir, 1).get(b"a"),
            Some(Bytes::from_static(b"1"))
        );

        std::fs::write(&single, resp_argv(&[b"SET".as_slice(), b"b", b"2"])).unwrap();
        assert_eq!(reshard_aof_dir(&dir, 1, 0).unwrap(), None);
        let mut loaded = load_shards(&dir, 1);
        assert_eq!(loaded.get(b"a"), None);
        assert_eq!(loaded.get(b"b"), Some(Bytes::from_static(b"2")));
        // The previous shard file went to a backup dir, and no marker is left.
        let backups: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("aof-import-backup-")
            })
            .collect();
        assert_eq!(backups.len(), 2, "one per import");
        assert!(!dir.join(AOF_RESHARD_MARKER).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_redis_aof_next_to_unimported_rudis_data_is_refused() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-both-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("appendonly-0.aof"),
            resp_argv(&[b"SET".as_slice(), b"rudis", b"1"]),
        )
        .unwrap();
        std::fs::write(
            dir.join("appendonly.aof"),
            resp_argv(&[b"SET".as_slice(), b"redis", b"1"]),
        )
        .unwrap();
        let err = reshard_aof_dir(&dir, 1, 0).unwrap_err();
        assert!(err.to_string().contains("Refusing to guess"), "{err}");
        // Nothing was touched.
        assert_eq!(
            load_shards(&dir, 1).get(b"rudis"),
            Some(Bytes::from_static(b"1"))
        );

        // Empty rudis files (created at startup) don't count as data.
        std::fs::write(dir.join("appendonly-0.aof"), b"").unwrap();
        assert_eq!(reshard_aof_dir(&dir, 1, 0).unwrap(), None);
        assert_eq!(
            load_shards(&dir, 1).get(b"redis"),
            Some(Bytes::from_static(b"1"))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_redis_aof_manifest_parsing() {
        assert_eq!(
            split_manifest_args(r#"file "a b.aof" seq 1 type 'i'"#).unwrap(),
            ["file", "a b.aof", "seq", "1", "type", "i"]
        );
        assert_eq!(
            split_manifest_args(r#"file "q\"\x41" seq 2"#).unwrap(),
            ["file", "q\"A", "seq", "2"]
        );
        assert!(split_manifest_args(r#"file "unterminated"#).is_none());
        assert!(split_manifest_args(r#"file "a"b seq 1"#).is_none());

        let dir = std::env::temp_dir().join(format!("rudis-aof-manifest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let manifest = dir.join("appendonly.aof.manifest");
        for f in ["b.rdb", "i 1.aof", "i2.aof"] {
            std::fs::write(dir.join(f), b"").unwrap();
        }
        // Any key order, quoted names, history entries ignored.
        std::fs::write(
            &manifest,
            "type b seq 1 file b.rdb\n\
             file old.aof seq 1 type h\n\
             file \"i 1.aof\" seq 1 type i\n\
             seq 2 type i file i2.aof\n",
        )
        .unwrap();
        assert_eq!(
            parse_redis_aof_manifest(&manifest).unwrap(),
            [dir.join("b.rdb"), dir.join("i 1.aof"), dir.join("i2.aof")]
        );
        // Paths are rejected.
        std::fs::write(&manifest, "file ../evil.aof seq 1 type i\n").unwrap();
        let err = parse_redis_aof_manifest(&manifest).unwrap_err();
        assert!(err.to_string().contains("just a filename"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_split_shard_files_end_in_db_0() {
        let mut rewritten = resp_argv(&[b"SELECT".as_slice(), b"3"]);
        rewritten.extend_from_slice(&resp_argv(&[b"SET".as_slice(), b"k", b"v"]));
        rewritten.extend_from_slice(&resp_argv(&[b"SELECT".as_slice(), b"0"]));
        let outputs = split_rewritten_aof_to_shards(&rewritten, 2).unwrap();
        // A write the live writer appends afterwards (no SELECT, DB 0).
        let mut db = ShardDb::new(0);
        for mut out in outputs {
            out.extend_from_slice(&resp_argv(&[b"SET".as_slice(), b"later", b"x"]));
            let path = std::env::temp_dir().join(format!(
                "rudis-split-db0-{}-{}.aof",
                std::process::id(),
                db.table.dbsize()
            ));
            std::fs::write(&path, &out).unwrap();
            replay_aof(&path, &mut db).unwrap();
            let _ = std::fs::remove_file(&path);
        }
        assert_eq!(db.get(b"later"), Some(Bytes::from_static(b"x")));
        db.select_db(3);
        assert_eq!(db.get(b"k"), Some(Bytes::from_static(b"v")));
        assert_eq!(db.get(b"later"), None);
    }

    #[test]
    fn test_auto_aof_rewrite_trigger_and_file_sizes() {
        // Redis serverCron: over min-size and grown by >= percentage.
        assert_eq!(
            auto_rewrite_due(100, 1000, 1000, 100),
            None,
            "not over min-size"
        );
        assert_eq!(auto_rewrite_due(100, 1000, 3000, 1500), Some(100));
        assert_eq!(auto_rewrite_due(100, 1000, 2999, 1500), None);
        assert_eq!(auto_rewrite_due(0, 0, u64::MAX, 1), None, "0 disables it");
        // No base yet (e.g. empty at startup) counts as 1 byte.
        assert!(auto_rewrite_due(100, 10, 5000, 0).is_some());

        let dir = std::env::temp_dir().join(format!("rudis-aof-sizes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(aof_files_size(&dir, 3), 0);
        std::fs::write(dir.join("appendonly-0.aof"), [0u8; 10]).unwrap();
        std::fs::write(dir.join("appendonly-2.aof"), [0u8; 5]).unwrap();
        std::fs::write(dir.join("appendonly-3.aof"), [0u8; 100]).unwrap();
        assert_eq!(
            aof_files_size(&dir, 3),
            15,
            "only this layout's shard files"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_aof_write_errors_refuse_writes_until_a_write_succeeds() {
        // A port no server uses, so its shards' state is this test's alone.
        let port = 65_001;
        assert!(aof_write_misconf(port).is_none());
        let p0 = aof_progress(port, 0);
        let p1 = aof_progress(port, 1);
        p1.record_write(Err(libc::ENOSPC));
        let err = aof_write_misconf(port).expect("writes refused while a shard fails");
        assert!(
            err.starts_with("-MISCONF Errors writing to the AOF file: No space left"),
            "{err}"
        );
        // Another shard succeeding does not clear it; that shard's does.
        p0.record_write(Ok(()));
        assert!(aof_write_misconf(port).is_some());
        p1.record_write(Ok(()));
        assert!(aof_write_misconf(port).is_none());
        // Repeated failures count once (the global fast-path counter).
        p1.record_write(Err(libc::EIO));
        p1.record_write(Err(libc::EIO));
        p1.record_write(Ok(()));
        assert!(aof_write_misconf(port).is_none());
    }

    #[test]
    fn test_failed_chunk_is_retried_not_dropped() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-requeue-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("appendonly-0.aof");
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let progress = aof_progress(65_003, 0);
            let mut w = AofWriter::open(path.clone()).await.unwrap();
            w.set_progress(progress.clone());
            w.append(b"first;");
            let (_file, chunk, off) = w.take_flush_chunk().unwrap();
            w.append(b"second;");
            // The write of `first;` failed (e.g. disk full).
            let failed: std::io::Result<()> = Err(std::io::Error::from_raw_os_error(libc::ENOSPC));
            w.chunk_written(&failed, chunk, off);
            assert!(aof_write_misconf(65_003).is_some());
            assert!(progress.pending());
            // The next write retries it, in order, and clears the error.
            assert!(w.write_pending_sync().unwrap());
            assert_eq!(std::fs::read(&path).unwrap(), b"first;second;");
            assert!(aof_write_misconf(65_003).is_none());
            assert!(!progress.pending());
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_write_through_puts_appends_in_the_file_and_tracks_progress() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-wt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("appendonly-0.aof");
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let progress = std::sync::Arc::new(AofProgress::default());
            let mut w = AofWriter::open(path.clone()).await.unwrap();
            w.set_progress(progress.clone());
            let aof = std::rc::Rc::new(std::cell::RefCell::new(w));
            assert!(!progress.pending());
            aof.borrow_mut().append(b"first;");
            assert!(progress.pending(), "an append is pending until written");
            // Another task took a chunk and hasn't written it yet: write_through
            // must wait for it, not just write its own part.
            let (file, chunk, off) = aof.borrow_mut().take_flush_chunk().unwrap();
            aof.borrow_mut().append(b"second;");
            let done = std::rc::Rc::new(std::cell::Cell::new(false));
            let (a, d) = (aof.clone(), done.clone());
            let writer = monoio::spawn(async move {
                AofWriter::write_through(&a).await.unwrap();
                d.set(true);
            });
            monoio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(
                !done.get(),
                "returned before the in-flight chunk was written"
            );
            let (res, buf) = file.write_all_at(chunk, off).await;
            aof.borrow_mut().chunk_written(&res, buf, off);
            res.unwrap();
            writer.await;
            assert!(!progress.pending());
            // No fsync needed: the bytes are in the file for any reader.
            assert_eq!(std::fs::read(&path).unwrap(), b"first;second;");
            // Nothing pending: returns at once, still nothing pending.
            AofWriter::write_through(&aof).await.unwrap();
            assert!(!progress.pending());
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_aof_slot_swaps_the_writer_for_every_holder() {
        let slot = AofSlot::default();
        let other_holder = slot.clone();
        assert!(!slot.is_on() && other_holder.get().is_none());
        let w = std::rc::Rc::new(std::cell::RefCell::new(AofWriter::new_in_memory()));
        slot.set(Some(w.clone()));
        assert!(other_holder.is_on() && other_holder.holds(&w));
        let w2 = std::rc::Rc::new(std::cell::RefCell::new(AofWriter::new_in_memory()));
        assert!(!other_holder.holds(&w2));
        other_holder.set(None);
        assert!(!slot.is_on() && !slot.holds(&w));
    }

    /// WAITAOF's local copy: appended bytes count as fsynced only once an
    /// fsync covered them, not when written to the kernel.
    #[test]
    fn test_fsynced_tracks_only_fsynced_appends() {
        use std::sync::atomic::Ordering::Acquire;
        let dir = std::env::temp_dir().join(format!("rudis-aof-fsynced-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("appendonly-0.aof");
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let progress = std::sync::Arc::new(AofProgress::default());
            let mut w = AofWriter::open(path.clone()).await.unwrap();
            w.set_progress(progress.clone());
            let aof = std::rc::Rc::new(std::cell::RefCell::new(w));
            aof.borrow_mut().append(b"first;");
            AofWriter::write_through(&aof).await.unwrap();
            assert_eq!(progress.written.load(Acquire), 6);
            assert_eq!(progress.fsynced.load(Acquire), 0, "written is not fsynced");
            // The everysec fsync covers what was written when it started.
            let upto = aof.borrow().written_total();
            aof.borrow_mut().append(b"second;");
            aof.borrow().mark_fsynced(upto);
            assert_eq!(progress.fsynced.load(Acquire), 6);
            // sync_durable covers everything appended before it.
            AofWriter::sync_durable(&aof).await.unwrap();
            assert_eq!(progress.fsynced.load(Acquire), 13);
            // Never goes back.
            aof.borrow().mark_fsynced(3);
            assert_eq!(progress.fsynced.load(Acquire), 13);
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_sync_durable_waits_for_in_flight_chunks() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-durable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("appendonly-0.aof");
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let aof = std::rc::Rc::new(std::cell::RefCell::new(
                AofWriter::open(path.clone()).await.unwrap(),
            ));
            aof.borrow_mut().append(b"first;");
            // Another task took this chunk and hasn't written it yet.
            let (file, chunk, off) = aof.borrow_mut().take_flush_chunk().unwrap();
            aof.borrow_mut().append(b"second;");
            let done = std::rc::Rc::new(std::cell::Cell::new(false));
            let (a, d) = (aof.clone(), done.clone());
            let syncer = monoio::spawn(async move {
                AofWriter::sync_durable(&a).await.unwrap();
                d.set(true);
            });
            monoio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(
                !done.get(),
                "returned before the in-flight chunk was written"
            );
            let (res, buf) = file.write_all_at(chunk, off).await;
            aof.borrow_mut().chunk_written(&res, buf, off);
            res.unwrap();
            syncer.await;
            assert!(done.get());
            assert_eq!(std::fs::read(&path).unwrap(), b"first;second;");
            {
                let w = aof.borrow();
                assert_eq!((w.durable, w.inflight), (w.offset, 0));
            }
            // Nothing new: covered by the last fsync.
            AofWriter::sync_durable(&aof).await.unwrap();
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
