use bytes::Bytes;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Default)]
#[repr(align(64))]
pub struct CachePadded<T>(pub T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Suspends the current task until every task already queued on this thread
/// has run, so monoio also gets to reap I/O completions in between. monoio
/// 0.2 has no public `yield_now`, and a task that wakes itself while being
/// polled is re-queued at the *front* (`LocalScheduler::yield_now`), so it
/// would just run again. A wake from another task goes to the back instead,
/// so a tiny spawned task delivers the wake.
pub async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            let waker = cx.waker().clone();
            monoio::spawn(async move { waker.wake() });
            std::task::Poll::Pending
        }
    })
    .await
}

/// `cross-shard-spin`: how many times a connection polls for a cross-shard
/// reply before parking its task. Polling burns the shard's CPU while other
/// connections on it wait, so the default is 0 (park immediately and let the
/// event loop serve them); a small value can shave latency on idle servers.
static CROSS_SHARD_SPIN: AtomicUsize = AtomicUsize::new(0);

pub fn set_cross_shard_spin(iters: usize) {
    CROSS_SHARD_SPIN.store(iters, Ordering::Relaxed);
}

#[inline(always)]
pub fn cross_shard_spin() -> usize {
    CROSS_SHARD_SPIN.load(Ordering::Relaxed)
}

const BELL_IDLE: u8 = 0;
const BELL_WAITING: u8 = 1;
const BELL_RUNG: u8 = 2;

/// Cross-thread wake-up for a single waiting task (or thread).
///
/// Replaces `flume::bounded::<()>(1)` used purely as a signal. On the
/// cross-shard GET/SET path flume cost ~11% of CPU (its `try_send` takes a
/// lock and walks the waiter list on every completion, the waiter then
/// drains with `try_recv`). Here a ring with nobody parked is one atomic
/// swap; the waker slot's lock is only taken when the waiter actually
/// parked, and then it is uncontended (one waiter, one waking ringer).
///
/// Any number of threads may `ring`; only one task waits at a time. A ring
/// is never lost: it either finds the waiter parked and wakes it, or leaves
/// the bell `RUNG` so the next `wait` returns at once.
#[derive(Default)]
pub struct Doorbell {
    state: std::sync::atomic::AtomicU8,
    waker: parking_lot::Mutex<Option<std::task::Waker>>,
}

impl Doorbell {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn ring(&self) {
        // AcqRel: publishes everything the ringer wrote before (the reply,
        // the queued message) to the waiter that consumes this ring.
        if self.state.swap(BELL_RUNG, Ordering::AcqRel) == BELL_WAITING {
            let waker = self.waker.lock().take();
            if let Some(w) = waker {
                w.wake();
            }
        }
    }

    /// Consumes a pending ring, if any, without waiting.
    #[inline(always)]
    pub fn try_consume(&self) -> bool {
        self.state.load(Ordering::Relaxed) == BELL_RUNG
            && self.state.swap(BELL_IDLE, Ordering::AcqRel) == BELL_RUNG
    }

    fn poll_wait(&self, waker: &std::task::Waker) -> std::task::Poll<()> {
        use std::task::Poll;
        // Consume a ring that is already there (also the wake-up path).
        if self.state.swap(BELL_IDLE, Ordering::AcqRel) == BELL_RUNG {
            return Poll::Ready(());
        }
        {
            let mut slot = self.waker.lock();
            match &*slot {
                Some(w) if w.will_wake(waker) => {}
                _ => *slot = Some(waker.clone()),
            }
        }
        // The waker is stored before WAITING is visible, so a ringer that
        // sees WAITING always finds it.
        match self.state.compare_exchange(
            BELL_IDLE,
            BELL_WAITING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Poll::Pending,
            Err(_) => {
                // Rung in between: consume it.
                self.state.swap(BELL_IDLE, Ordering::AcqRel);
                Poll::Ready(())
            }
        }
    }

    /// Waits for the next ring (or consumes one that is already pending).
    pub async fn wait(&self) {
        std::future::poll_fn(|cx| self.poll_wait(cx.waker())).await
    }

    /// Blocking [`Self::wait`] for plain threads (tests, sync helpers).
    pub fn wait_blocking(&self) {
        struct Unpark(std::thread::Thread);
        impl std::task::Wake for Unpark {
            fn wake(self: std::sync::Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = std::task::Waker::from(std::sync::Arc::new(Unpark(std::thread::current())));
        while self.poll_wait(&waker).is_pending() {
            std::thread::park();
        }
    }
}

/// Shared-memory Scatter-Gather Descriptor for multi-shard MGET.
pub const DESC_RUNNING: u8 = 0;
pub const DESC_COMPLETED: u8 = 1;
pub const DESC_SLEEPING: u8 = 2;

/// Remote shards write their looked up values directly into their respective slots.
pub struct ScatterMgetDescriptor {
    pub results: Box<[CachePadded<UnsafeCell<Option<Bytes>>>]>,
    pub pending: AtomicUsize,
    pub state: std::sync::atomic::AtomicU8,
    pub notify: CachePadded<UnsafeCell<flume::Sender<()>>>,
    pub recycled_keys: Box<[CachePadded<UnsafeCell<Vec<(usize, Bytes)>>>]>,
}

// SAFETY: Every field is itself Send (Bytes, Vec, flume::Sender, atomics);
// `UnsafeCell<T>` is Send for `T: Send`, so this impl adds no new capability.
unsafe impl Send for ScatterMgetDescriptor {}
// SAFETY: The UnsafeCells follow the scatter-gather protocol: `results[i]` is
// written only by the one remote shard that owns key `i`, and `recycled_keys[s]`
// only by shard `s`, both before that shard's AcqRel `finish_shard`. The
// coordinator reads them only after `wait_completed` observes DESC_COMPLETED
// (Acquire), and calls `reset` (writing `notify` and the slots) only while no
// remote shard holds the descriptor. `notify` is read only by the last
// finisher, and only when the coordinator is parked in DESC_SLEEPING.
unsafe impl Sync for ScatterMgetDescriptor {}

impl ScatterMgetDescriptor {
    pub fn new(
        total_keys: usize,
        num_shards: usize,
        pending_shards: usize,
        notify: flume::Sender<()>,
    ) -> Self {
        let mut vec = Vec::with_capacity(total_keys);
        for _ in 0..total_keys {
            vec.push(CachePadded(UnsafeCell::new(None)));
        }
        let mut recycled = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            recycled.push(CachePadded(UnsafeCell::new(Vec::new())));
        }
        Self {
            results: vec.into_boxed_slice(),
            pending: AtomicUsize::new(pending_shards),
            state: std::sync::atomic::AtomicU8::new(if pending_shards == 0 {
                DESC_COMPLETED
            } else {
                DESC_RUNNING
            }),
            notify: CachePadded(UnsafeCell::new(notify)),
            recycled_keys: recycled.into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn reset(&self, total_keys: usize, pending_shards: usize, notify: flume::Sender<()>) {
        self.state.store(
            if pending_shards == 0 {
                DESC_COMPLETED
            } else {
                DESC_RUNNING
            },
            Ordering::Relaxed,
        );
        // SAFETY: `reset` runs on the coordinator before the descriptor is handed to
        // any remote shard, so no other thread is reading `notify`. The router only
        // reuses a pooled descriptor once `Arc::strong_count == 1`, i.e. after the
        // last finisher dropped its clone, so a `finish_shard` still inside
        // `try_send` on the old sender cannot overlap this write.
        unsafe {
            *self.notify.get() = notify;
        }
        for i in 0..total_keys.min(self.results.len()) {
            // SAFETY: Same as above: no remote shard holds the descriptor yet, so the
            // coordinator has exclusive access to every result slot.
            unsafe {
                *self.results[i].get() = None;
            }
        }
        self.pending.store(pending_shards, Ordering::Release);
    }

    #[inline(always)]
    pub fn write_result(&self, idx: usize, val: Option<Bytes>) {
        // SAFETY: Each `idx` is owned by exactly one remote shard (the shard that owns
        // that key) and is written once, before that shard calls `finish_shard`; the
        // coordinator does not read it until DESC_COMPLETED is observed.
        unsafe {
            *self.results[idx].get() = val;
        }
    }

    #[inline(always)]
    pub fn recycle_keys(&self, shard_id: usize, keys: Vec<(usize, Bytes)>) {
        // SAFETY: `recycled_keys[shard_id]` is written only by shard `shard_id`, before
        // its `finish_shard`; the coordinator reads it only after completion.
        unsafe {
            *self.recycled_keys[shard_id].get() = keys;
        }
    }

    #[inline(always)]
    pub fn finish_shard(&self) {
        if self.pending.fetch_sub(1, Ordering::AcqRel) == 1
            && self.state.swap(DESC_COMPLETED, Ordering::AcqRel) == DESC_SLEEPING
        {
            // SAFETY: We are the last finisher and saw DESC_SLEEPING, so the coordinator
            // is parked on `notify_rx` and cannot be in `reset` writing `notify`.
            let tx = unsafe { &*self.notify.get() };
            let _ = tx.try_send(());
        }
    }

    #[inline(always)]
    pub async fn wait_completed(&self, spin_iters: usize, notify_rx: &flume::Receiver<()>) {
        if self.state.load(Ordering::Acquire) != DESC_COMPLETED {
            for _ in 0..spin_iters {
                std::hint::spin_loop();
                if self.state.load(Ordering::Acquire) == DESC_COMPLETED {
                    return;
                }
            }
            if self
                .state
                .compare_exchange(
                    DESC_RUNNING,
                    DESC_SLEEPING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                let _ = notify_rx.recv_async().await;
            }
        }
    }

    #[inline(always)]
    pub fn into_results(&self) -> Vec<Option<Bytes>> {
        self.into_results_prefix(self.results.len())
    }

    #[inline(always)]
    pub fn into_results_prefix(&self, total_keys: usize) -> Vec<Option<Bytes>> {
        let n = total_keys.min(self.results.len());
        let mut out = Vec::with_capacity(n);
        for cell in self.results[..n].iter() {
            // SAFETY: Called by the coordinator after `wait_completed`, so every remote
            // write happened-before this read and no shard touches the slots anymore.
            out.push(unsafe { (*cell.get()).take() });
        }
        out
    }

    #[inline(always)]
    pub fn take_recycled_keys(&self) -> Vec<Vec<(usize, Bytes)>> {
        let mut out = Vec::with_capacity(self.recycled_keys.len());
        for cell in self.recycled_keys.iter() {
            // SAFETY: Called by the coordinator after `wait_completed`; no remote shard
            // writes `recycled_keys` anymore.
            out.push(unsafe { std::mem::take(&mut *cell.get()) });
        }
        out
    }
}

/// Shared-memory Scatter-Gather Descriptor for multi-shard MSET.
pub struct ScatterMsetDescriptor {
    pub pending: AtomicUsize,
    pub state: std::sync::atomic::AtomicU8,
    pub notify: CachePadded<UnsafeCell<flume::Sender<()>>>,
    pub recycled_pairs: Box<[CachePadded<UnsafeCell<Vec<(Bytes, Bytes)>>>]>,
}

// SAFETY: Every field is itself Send; `UnsafeCell<T>` is Send for `T: Send`.
unsafe impl Send for ScatterMsetDescriptor {}
// SAFETY: `recycled_pairs[s]` is written only by shard `s` before its AcqRel
// `finish_shard` and read by the coordinator only after `wait_completed`
// observes DESC_COMPLETED; `notify` is written in `reset` only while no remote
// shard holds the descriptor, and read only by the last finisher while the
// coordinator is parked in DESC_SLEEPING.
unsafe impl Sync for ScatterMsetDescriptor {}

impl ScatterMsetDescriptor {
    pub fn new(num_shards: usize, pending_shards: usize, notify: flume::Sender<()>) -> Self {
        let mut recycled = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            recycled.push(CachePadded(UnsafeCell::new(Vec::new())));
        }
        Self {
            pending: AtomicUsize::new(pending_shards),
            state: std::sync::atomic::AtomicU8::new(if pending_shards == 0 {
                DESC_COMPLETED
            } else {
                DESC_RUNNING
            }),
            notify: CachePadded(UnsafeCell::new(notify)),
            recycled_pairs: recycled.into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn reset(&self, pending_shards: usize, notify: flume::Sender<()>) {
        self.state.store(
            if pending_shards == 0 {
                DESC_COMPLETED
            } else {
                DESC_RUNNING
            },
            Ordering::Relaxed,
        );
        // SAFETY: `reset` runs before the descriptor is handed to any remote shard,
        // so no other thread is reading `notify`. Pooled descriptors are reused only
        // at `Arc::strong_count == 1` (see `Router::acquire_mset_descriptor`), so no
        // finisher is still borrowing the old sender.
        unsafe {
            *self.notify.get() = notify;
        }
        self.pending.store(pending_shards, Ordering::Release);
    }

    #[inline(always)]
    pub fn recycle_pairs(&self, shard_id: usize, pairs: Vec<(Bytes, Bytes)>) {
        // SAFETY: `recycled_pairs[shard_id]` is written only by shard `shard_id`,
        // before its `finish_shard`; the coordinator reads it only after completion.
        unsafe {
            *self.recycled_pairs[shard_id].get() = pairs;
        }
    }

    #[inline(always)]
    pub fn finish_shard(&self) {
        if self.pending.fetch_sub(1, Ordering::AcqRel) == 1
            && self.state.swap(DESC_COMPLETED, Ordering::AcqRel) == DESC_SLEEPING
        {
            // SAFETY: We are the last finisher and saw DESC_SLEEPING, so the coordinator
            // is parked on `notify_rx` and cannot be in `reset` writing `notify`.
            let tx = unsafe { &*self.notify.get() };
            let _ = tx.try_send(());
        }
    }

    #[inline(always)]
    pub async fn wait_completed(&self, spin_iters: usize, notify_rx: &flume::Receiver<()>) {
        if self.state.load(Ordering::Acquire) != DESC_COMPLETED {
            for _ in 0..spin_iters {
                std::hint::spin_loop();
                if self.state.load(Ordering::Acquire) == DESC_COMPLETED {
                    return;
                }
            }
            if self
                .state
                .compare_exchange(
                    DESC_RUNNING,
                    DESC_SLEEPING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                let _ = notify_rx.recv_async().await;
            }
        }
    }

    #[inline(always)]
    pub fn take_recycled_pairs(&self) -> Vec<Vec<(Bytes, Bytes)>> {
        let mut out = Vec::with_capacity(self.recycled_pairs.len());
        for cell in self.recycled_pairs.iter() {
            // SAFETY: Called by the coordinator after `wait_completed`; no remote shard
            // writes `recycled_pairs` anymore.
            out.push(unsafe { std::mem::take(&mut *cell.get()) });
        }
        out
    }
}

/// Direct shared-memory slot for single remote GET (bypasses responder channel allocations)
pub struct FastGetDescriptor {
    pub key: Bytes,
    pub val: CachePadded<UnsafeCell<Option<Bytes>>>,
    /// Set by [`FastGetDescriptor::finish_wrong_type`]: the key holds a
    /// non-string value.
    pub wrong_type: AtomicBool,
    pub done: AtomicBool,
    pub bell: Doorbell,
}

// SAFETY: Every field is itself Send; `UnsafeCell<T>` is Send for `T: Send`.
unsafe impl Send for FastGetDescriptor {}
// SAFETY: `val` is written once by the remote shard in `finish`, before the
// Release store of `done`; the requester reads it only after observing
// `done == true` with Acquire (`wait_done`), and the writer never touches it
// again, so the cell is never accessed concurrently.
unsafe impl Sync for FastGetDescriptor {}

impl FastGetDescriptor {
    #[inline(always)]
    pub fn new(key: Bytes) -> Self {
        Self {
            key,
            val: CachePadded(UnsafeCell::new(None)),
            wrong_type: AtomicBool::new(false),
            done: AtomicBool::new(false),
            bell: Doorbell::new(),
        }
    }

    /// Waits (after `spin` polls) until a remote shard has called `finish`.
    #[inline(always)]
    pub async fn wait_done(&self, spin: usize) {
        for _ in 0..spin {
            if self.done.load(Ordering::Acquire) {
                return;
            }
            std::hint::spin_loop();
        }
        while !self.done.load(Ordering::Acquire) {
            self.bell.wait().await;
        }
    }

    /// Completes the GET with a WRONGTYPE error.
    #[inline(always)]
    pub fn finish_wrong_type(&self) {
        self.wrong_type.store(true, Ordering::Relaxed);
        self.finish(None);
    }

    #[inline(always)]
    pub fn finish(&self, val: Option<Bytes>) {
        // SAFETY: Only the single remote shard that received this descriptor calls
        // `finish`, once; the requester does not read `val` until it observes the
        // Release store of `done` below.
        unsafe {
            *self.val.get() = val;
        }
        self.done.store(true, Ordering::Release);
        self.bell.ring();
    }
}

/// Direct shared-memory slot for single remote SET (bypasses responder channel allocations)
pub struct FastSetDescriptor {
    pub key: Bytes,
    pub value: Bytes,
    pub expire_in: Option<Duration>,
    pub done: AtomicBool,
    pub bell: Doorbell,
}

// SAFETY: Every field is Send (Bytes, Duration, atomics, Doorbell).
unsafe impl Send for FastSetDescriptor {}
// SAFETY: There is no interior mutability besides atomics and the Doorbell's
// mutex, all of which are Sync; the other fields are immutable after `new`.
unsafe impl Sync for FastSetDescriptor {}

impl FastSetDescriptor {
    pub fn new(key: Bytes, value: Bytes, expire_in: Option<Duration>) -> Self {
        Self {
            key,
            value,
            expire_in,
            done: AtomicBool::new(false),
            bell: Doorbell::new(),
        }
    }

    #[inline(always)]
    pub fn finish(&self) {
        self.done.store(true, Ordering::Release);
        self.bell.ring();
    }

    /// Waits (after `spin` polls) until a remote shard has called `finish`.
    #[inline(always)]
    pub async fn wait_done(&self, spin: usize) {
        for _ in 0..spin {
            if self.done.load(Ordering::Acquire) {
                return;
            }
            std::hint::spin_loop();
        }
        while !self.done.load(Ordering::Acquire) {
            self.bell.wait().await;
        }
    }
}

/// Direct shared-memory slot for cross-shard squashed batch responses.
/// Remote shards write responses directly into the caller's `responses` slice via `responses_ptr`,
/// and signal completion via the 3-state Parker handshake without Flume mutex contention.
pub struct BatchResponder {
    pub state: CachePadded<std::sync::atomic::AtomicU8>,
    pub responses_ptr: std::sync::atomic::AtomicPtr<crate::shard::CompactResp>,
    /// Length of the slice behind `responses_ptr`; `write_slot` bounds-checks
    /// against it.
    pub responses_len: AtomicUsize,
    pub recycled_items: CachePadded<UnsafeCell<Option<Vec<(usize, u64, crate::resp::Command)>>>>,
    pub bell: Doorbell,
}

// SAFETY: Every field is Send; `UnsafeCell<T>` is Send for `T: Send`, and the
// raw pointer lives in an AtomicPtr, which is Send.
unsafe impl Send for BatchResponder {}
// SAFETY: Cross-thread access follows the BATCH_* handshake: the owner calls
// `prepare` (Release) before handing the responder to one remote shard; that
// shard writes response slots and `recycled_items` and then publishes
// BATCH_COMPLETED with an AcqRel swap; the owner reads them only after an
// Acquire load of BATCH_COMPLETED. The two sides never overlap.
unsafe impl Sync for BatchResponder {}

pub const BATCH_IDLE: u8 = 0;
pub const BATCH_RUNNING: u8 = 1;
pub const BATCH_SLEEPING: u8 = 2;
pub const BATCH_COMPLETED: u8 = 3;

impl BatchResponder {
    pub fn new() -> Self {
        Self {
            state: CachePadded(std::sync::atomic::AtomicU8::new(BATCH_IDLE)),
            responses_ptr: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
            responses_len: AtomicUsize::new(0),
            recycled_items: CachePadded(UnsafeCell::new(None)),
            bell: Doorbell::new(),
        }
    }

    #[inline(always)]
    pub fn is_idle(&self) -> bool {
        self.state.0.load(Ordering::Acquire) == BATCH_IDLE
    }

    #[inline(always)]
    pub fn prepare(&self, responses_ptr: *mut crate::shard::CompactResp, len: usize) {
        self.responses_ptr.store(responses_ptr, Ordering::Relaxed);
        self.responses_len.store(len, Ordering::Relaxed);
        self.state.0.store(BATCH_RUNNING, Ordering::Release);
    }

    #[inline(always)]
    pub fn write_slot(&self, idx: usize, resp: crate::shard::CompactResp) {
        // A bad index would be a write past the owner's slice: refuse it.
        assert!(
            idx < self.responses_len.load(Ordering::Relaxed),
            "BatchResponder::write_slot index out of bounds"
        );
        // SAFETY: Relies on the caller protocol: `prepare` stored a pointer to (and
        // the length of) the owner's initialized `responses` slice, which the owner
        // keeps alive and untouched until it observes BATCH_COMPLETED; `idx` was
        // bounds-checked above and each slot is written by exactly one remote shard,
        // so the in-place assignment (which drops the old value) does not race.
        unsafe {
            let ptr = self.responses_ptr.load(Ordering::Relaxed);
            *ptr.add(idx) = resp;
        }
    }

    #[inline(always)]
    pub fn finish(&self, items: Vec<(usize, u64, crate::resp::Command)>) {
        // SAFETY: Only the one remote shard serving this batch calls `finish`, once,
        // and the owner reads `recycled_items` only after the AcqRel swap below
        // publishes BATCH_COMPLETED.
        unsafe {
            *self.recycled_items.get() = Some(items);
        }
        if self.state.0.swap(BATCH_COMPLETED, Ordering::AcqRel) == BATCH_SLEEPING {
            self.bell.ring();
        }
    }

    #[inline(always)]
    pub fn try_take(&self) -> Option<Vec<(usize, u64, crate::resp::Command)>> {
        if self.state.0.load(Ordering::Acquire) == BATCH_COMPLETED {
            self.state.0.store(BATCH_IDLE, Ordering::Relaxed);
            // SAFETY: The Acquire load above saw BATCH_COMPLETED, so the remote shard's
            // write in `finish` happened-before this and it no longer touches the cell.
            unsafe { (*self.recycled_items.get()).take() }
        } else {
            None
        }
    }

    #[inline(always)]
    pub async fn wait_take(&self) -> Option<Vec<(usize, u64, crate::resp::Command)>> {
        if self.state.0.load(Ordering::Acquire) != BATCH_COMPLETED
            && self
                .state
                .0
                .compare_exchange(
                    BATCH_RUNNING,
                    BATCH_SLEEPING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            // Exactly one ring answers a successful RUNNING -> SLEEPING; the
            // loop only guards against a stray ring from an earlier round.
            while self.state.0.load(Ordering::Acquire) != BATCH_COMPLETED {
                self.bell.wait().await;
            }
        }
        self.state.0.store(BATCH_IDLE, Ordering::Relaxed);
        // SAFETY: We either observed BATCH_COMPLETED with Acquire above, or the remote
        // shard rang the bell after its AcqRel swap to BATCH_COMPLETED and the loop
        // re-checked it with Acquire; either way `finish` is done with the cell.
        unsafe { (*self.recycled_items.get()).take() }
    }
}

impl Default for BatchResponder {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache-line aligned, lock-free Single-Producer Single-Consumer circular ring buffer
/// with fallback overflow queue for unbounded durability.
#[repr(align(64))]
pub struct SpscQueue<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    buffer: Box<[UnsafeCell<Option<T>>]>,
    capacity: usize,
    mask: usize,
    overflow: parking_lot::Mutex<std::collections::VecDeque<T>>,
    has_overflow: AtomicBool,
}

// SAFETY: The queue owns its `T`s (ring slots and overflow); sending it to
// another thread moves them, which `T: Send` permits.
unsafe impl<T: Send> Send for SpscQueue<T> {}
// SAFETY: Sound only under the SPSC discipline documented on `push`/`pop`: at
// most one thread pushes and at most one thread pops at a time (enforced by
// how `create_shard_mesh` hands out rings, not by the type). Under it, a slot
// is written only by the producer while outside `[head, tail)` and read only
// by the consumer while inside it, with `tail`/`head` Release/Acquire pairs
// transferring each slot; the overflow queue is behind a mutex.
unsafe impl<T: Send> Sync for SpscQueue<T> {}

impl<T> SpscQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two();
        let mut buf = Vec::with_capacity(cap);
        for _ in 0..cap {
            buf.push(UnsafeCell::new(None));
        }
        Self {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            buffer: buf.into_boxed_slice(),
            capacity: cap,
            mask: cap - 1,
            overflow: parking_lot::Mutex::new(std::collections::VecDeque::new()),
            has_overflow: AtomicBool::new(false),
        }
    }

    /// Pushes from the single producer. Only the producer ever writes `tail`
    /// and the ring slots behind it, so the ring stays a true SPSC queue.
    #[inline(always)]
    pub fn push(&self, item: T) {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        // `has_overflow` is only set by this thread and only cleared once the
        // overflow is empty, so `false` means every earlier item is in the
        // ring and appending to it keeps FIFO order.
        if !self.has_overflow.load(Ordering::Acquire) && tail.wrapping_sub(head) < self.capacity {
            // SAFETY: Single producer: `tail - head < capacity`, so slot `tail & mask` is
            // not in the consumer's readable range `[head, tail)` (the Acquire load of
            // `head` saw the consumer finish taking it), and the consumer will not read
            // it until the Release store of `tail + 1` below.
            unsafe {
                *self.buffer[tail & self.mask].get() = Some(item);
            }
            self.tail.store(tail.wrapping_add(1), Ordering::Release);
        } else {
            self.push_slow(item);
        }
    }

    #[cold]
    fn push_slow(&self, item: T) {
        let mut q = self.overflow.lock();
        // Move older overflow entries into the ring first, oldest first, so
        // the overflow only ever holds items newer than everything in the ring.
        let mut tail = self.tail.load(Ordering::Relaxed);
        while !q.is_empty() && tail.wrapping_sub(self.head.load(Ordering::Acquire)) < self.capacity
        {
            let next = q.pop_front();
            // SAFETY: Same as `push`: we are the single producer and the loop condition
            // keeps `tail - head < capacity`, so this slot is outside `[head, tail)`.
            unsafe {
                *self.buffer[tail & self.mask].get() = next;
            }
            tail = tail.wrapping_add(1);
            self.tail.store(tail, Ordering::Release);
        }
        if q.is_empty() && tail.wrapping_sub(self.head.load(Ordering::Acquire)) < self.capacity {
            // SAFETY: Same as `push`: single producer, and `tail - head < capacity` was
            // just checked, so the consumer cannot be reading this slot.
            unsafe {
                *self.buffer[tail & self.mask].get() = Some(item);
            }
            self.tail.store(tail.wrapping_add(1), Ordering::Release);
        } else {
            q.push_back(item);
        }
        self.has_overflow.store(!q.is_empty(), Ordering::Release);
    }

    /// Pops from the single consumer. The consumer only writes `head`.
    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head != tail {
            // SAFETY: Single consumer: `head != tail` with `tail` loaded Acquire, so the
            // producer's write of this slot happened-before; the producer will not
            // overwrite it until our Release store of `head + 1`.
            let item = unsafe { (*self.buffer[head & self.mask].get()).take() };
            self.head.store(head.wrapping_add(1), Ordering::Release);
            item
        } else if self.has_overflow.load(Ordering::Acquire) {
            self.pop_overflow(head)
        } else {
            None
        }
    }

    #[cold]
    fn pop_overflow(&self, head: usize) -> Option<T> {
        let mut q = self.overflow.lock();
        // The producer may have moved overflow entries into the ring before we
        // took the lock; those are older than the overflow front.
        if self.tail.load(Ordering::Acquire) != head {
            drop(q);
            // SAFETY: Single consumer, and the Acquire load of `tail` above is `!= head`,
            // so slot `head` was published by the producer and is ours to take.
            let item = unsafe { (*self.buffer[head & self.mask].get()).take() };
            self.head.store(head.wrapping_add(1), Ordering::Release);
            return item;
        }
        let item = q.pop_front();
        if q.is_empty() {
            self.has_overflow.store(false, Ordering::Release);
        }
        item
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head != tail {
            return false;
        }
        if self.has_overflow.load(Ordering::Acquire) {
            let q = self.overflow.lock();
            return q.is_empty();
        }
        true
    }
}

impl<T> Drop for SpscQueue<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

/// Shared by every sender to one target shard. When the last of them is
/// dropped the receiver is disconnected: the bell is rung so a parked
/// receiver wakes up and returns `RecvError` (as the flume channel used
/// for this did).
pub struct SenderLife {
    bell: std::sync::Arc<Doorbell>,
}

impl Drop for SenderLife {
    fn drop(&mut self) {
        self.bell.ring();
    }
}

/// Sender handle from one shard to a specific target shard.
/// Pushes to a dedicated lock-free SPSC ring and notifies the target thread.
#[derive(Clone)]
pub struct ShardSender {
    pub target_shard: usize,
    pub ring: std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>,
    pub target_notify: std::sync::Arc<Doorbell>,
    pub target_sleeping: std::sync::Arc<CachePadded<AtomicBool>>,
    pub _life: std::sync::Arc<SenderLife>,
}

impl ShardSender {
    #[inline(always)]
    pub fn send(&self, msg: crate::shard::ShardMessage) -> Result<(), SendError> {
        self.ring.push(msg);
        // Dekker handshake with `ShardReceiver::recv*`: the receiver stores
        // `sleeping = true` and then re-checks the rings; we publish the
        // message and then read `sleeping`. Without a full fence the push
        // (a Release store, or a mutex unlock on the overflow path) may be
        // ordered after the load, so both sides miss each other and the
        // receiver sleeps with a message queued.
        std::sync::atomic::fence(Ordering::SeqCst);
        if self.target_sleeping.0.load(Ordering::SeqCst) {
            self.target_notify.ring();
        }
        Ok(())
    }
}

/// Receiver handle for a shard worker.
/// Checks incoming SPSC rings from all shards without locks, sleeping only when all are drained.
pub struct ShardReceiver {
    pub shard_id: usize,
    pub incoming_rings: Vec<std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>>,
    pub notify_rx: std::sync::Arc<Doorbell>,
    /// Dead once every [`ShardSender`] to this shard is gone.
    pub senders: std::sync::Weak<SenderLife>,
    pub sleeping: std::sync::Arc<CachePadded<AtomicBool>>,
    /// Ring that `try_recv` checks first; it moves past the ring that last
    /// delivered, so every producer shard gets a turn. Only the consumer
    /// touches it, so Relaxed is enough; it is atomic to keep the type Sync.
    pub next_ring: AtomicUsize,
}

impl ShardReceiver {
    /// Pops from the incoming rings round-robin. Always starting at ring 0
    /// let a busy low-numbered shard delay every message from the others.
    /// Each ring stays FIFO; there is no ordering across rings anyway.
    #[inline(always)]
    pub fn try_recv(&self) -> Result<crate::shard::ShardMessage, RecvError> {
        let n = self.incoming_rings.len();
        let start = self.next_ring.load(Ordering::Relaxed);
        for i in 0..n {
            let mut idx = start + i;
            if idx >= n {
                idx -= n;
            }
            if let Some(msg) = self.incoming_rings[idx].pop() {
                self.next_ring
                    .store(if idx + 1 == n { 0 } else { idx + 1 }, Ordering::Relaxed);
                return Ok(msg);
            }
        }
        Err(RecvError)
    }

    pub fn recv(&self) -> Result<crate::shard::ShardMessage, RecvError> {
        loop {
            if let Ok(msg) = self.try_recv() {
                return Ok(msg);
            }

            self.sleeping.0.store(true, Ordering::SeqCst);
            // Pairs with the fence in `ShardSender::send`: the flag must be
            // visible before the rings are re-checked.
            std::sync::atomic::fence(Ordering::SeqCst);
            if let Ok(msg) = self.try_recv() {
                self.sleeping.0.store(false, Ordering::Relaxed);
                return Ok(msg);
            }

            if self.senders.strong_count() == 0 {
                self.sleeping.0.store(false, Ordering::Relaxed);
                return self.try_recv();
            }
            self.notify_rx.wait_blocking();
            self.sleeping.0.store(false, Ordering::Relaxed);

            if let Ok(msg) = self.try_recv() {
                return Ok(msg);
            }
        }
    }

    pub async fn recv_async(&self) -> Result<crate::shard::ShardMessage, RecvError> {
        loop {
            if let Ok(msg) = self.try_recv() {
                return Ok(msg);
            }

            self.sleeping.0.store(true, Ordering::SeqCst);
            // Pairs with the fence in `ShardSender::send`: the flag must be
            // visible before the rings are re-checked.
            std::sync::atomic::fence(Ordering::SeqCst);
            if let Ok(msg) = self.try_recv() {
                self.sleeping.0.store(false, Ordering::Relaxed);
                return Ok(msg);
            }

            if self.senders.strong_count() == 0 {
                self.sleeping.0.store(false, Ordering::Relaxed);
                return self.try_recv();
            }
            self.notify_rx.wait().await;
            self.sleeping.0.store(false, Ordering::Relaxed);

            if let Ok(msg) = self.try_recv() {
                return Ok(msg);
            }
        }
    }
}

/// Creates a fully-connected lock-free cross-shard communication mesh.
pub fn create_shard_mesh(num_shards: usize) -> (Vec<Vec<ShardSender>>, Vec<ShardReceiver>) {
    let mut notifiers = Vec::with_capacity(num_shards);
    let mut sleeping_flags = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        notifiers.push(std::sync::Arc::new(Doorbell::new()));
        sleeping_flags.push(std::sync::Arc::new(CachePadded(AtomicBool::new(false))));
    }
    let lives: Vec<std::sync::Arc<SenderLife>> = notifiers
        .iter()
        .map(|bell| std::sync::Arc::new(SenderLife { bell: bell.clone() }))
        .collect();

    // Matrix of SPSC rings: rings[i][j] is the ring from producer shard i to consumer shard j
    let mut rings: Vec<Vec<std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>>> =
        Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        let mut row = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            row.push(std::sync::Arc::new(SpscQueue::new(256)));
        }
        rings.push(row);
    }

    let mut senders_mesh = Vec::with_capacity(num_shards);
    for row in &rings {
        let mut shard_senders = Vec::with_capacity(num_shards);
        for (j, ring) in row.iter().enumerate().take(num_shards) {
            shard_senders.push(ShardSender {
                target_shard: j,
                ring: ring.clone(),
                target_notify: notifiers[j].clone(),
                target_sleeping: sleeping_flags[j].clone(),
                _life: lives[j].clone(),
            });
        }
        senders_mesh.push(shard_senders);
    }

    let mut receivers = Vec::with_capacity(num_shards);
    for (j, (notifier, sleeping)) in notifiers
        .into_iter()
        .zip(sleeping_flags)
        .enumerate()
        .take(num_shards)
    {
        let mut incoming = Vec::with_capacity(num_shards);
        for row in &rings {
            incoming.push(row[j].clone());
        }
        receivers.push(ShardReceiver {
            shard_id: j,
            incoming_rings: incoming,
            notify_rx: notifier,
            senders: std::sync::Arc::downgrade(&lives[j]),
            sleeping,
            next_ring: AtomicUsize::new(0),
        });
    }

    (senders_mesh, receivers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_cache_padded_alignment() {
        assert_eq!(
            std::mem::align_of::<CachePadded<UnsafeCell<Option<Bytes>>>>(),
            64
        );
        assert!(std::mem::size_of::<CachePadded<UnsafeCell<Option<Bytes>>>>() >= 64);
    }

    #[test]
    fn test_cross_shard_spin_knob_defaults_to_parking() {
        assert_eq!(cross_shard_spin(), 0);
        set_cross_shard_spin(64);
        assert_eq!(cross_shard_spin(), 64);
        set_cross_shard_spin(0);
        assert_eq!(cross_shard_spin(), 0);
    }

    #[test]
    fn test_try_recv_round_robins_across_producer_shards() {
        let (senders, receivers) = create_shard_mesh(3);
        let msg = |key: &'static str| crate::shard::ShardMessage::ExpireTime {
            key: Bytes::from_static(key.as_bytes()),
            in_millis: false,
            responder: flume::bounded(1).0,
        };
        let key_of = |m: crate::shard::ShardMessage| match m {
            crate::shard::ShardMessage::ExpireTime { key, .. } => key,
            _ => unreachable!(),
        };
        // Shard 0 has a backlog for shard 1; shard 2 then sends one message.
        for _ in 0..10 {
            senders[0][1].send(msg("from0")).unwrap();
        }
        senders[2][1].send(msg("from2")).unwrap();
        let rx = &receivers[1];
        let first_two = [
            key_of(rx.try_recv().unwrap()),
            key_of(rx.try_recv().unwrap()),
        ];
        assert!(
            first_two.contains(&Bytes::from_static(b"from2")),
            "shard 2's message waited behind shard 0's backlog: {first_two:?}"
        );
        // The rest of shard 0's backlog is still delivered.
        let mut rest = 0;
        while let Ok(m) = rx.try_recv() {
            assert_eq!(key_of(m), Bytes::from_static(b"from0"));
            rest += 1;
        }
        assert_eq!(rest, 9);
    }

    #[test]
    fn test_yield_now_lets_other_tasks_run() {
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            // Spawned like the mailbox task in server.rs (block_on's own
            // future is re-polled before queued tasks, so it can't yield).
            monoio::spawn(async {
                let ran = std::rc::Rc::new(std::cell::Cell::new(false));
                let ran2 = ran.clone();
                monoio::spawn(async move { ran2.set(true) });
                // A task that never awaits anything else, like the mailbox
                // task under a steady message stream, must still let others
                // run.
                let mut spins = 0;
                while !ran.get() && spins < 1000 {
                    yield_now().await;
                    spins += 1;
                }
                assert!(ran.get(), "spawned task never ran");
            })
            .await;
        });
    }

    #[test]
    fn test_fast_get_descriptor() {
        let desc = Arc::new(FastGetDescriptor::new(Bytes::from("key1")));
        assert!(!desc.done.load(Ordering::Acquire));

        let desc_clone = desc.clone();
        let handle = thread::spawn(move || {
            desc_clone.finish(Some(Bytes::from("val1")));
        });

        while !desc.done.load(Ordering::Acquire) {
            desc.bell.wait_blocking();
        }
        handle.join().unwrap();

        assert!(desc.done.load(Ordering::Acquire));
        // SAFETY: The writer thread has been joined and `done` is set; nothing else
        // accesses `val`.
        let val = unsafe { (*desc.val.get()).take() };
        assert_eq!(val, Some(Bytes::from("val1")));
    }

    #[test]
    fn test_fast_set_descriptor() {
        let desc = Arc::new(FastSetDescriptor::new(
            Bytes::from("key_set"),
            Bytes::from("val_set"),
            Some(Duration::from_secs(10)),
        ));
        assert!(!desc.done.load(Ordering::Acquire));
        assert_eq!(desc.key, Bytes::from("key_set"));
        assert_eq!(desc.value, Bytes::from("val_set"));
        assert_eq!(desc.expire_in, Some(Duration::from_secs(10)));

        let desc_clone = desc.clone();
        let handle = thread::spawn(move || {
            desc_clone.finish();
        });

        while !desc.done.load(Ordering::Acquire) {
            desc.bell.wait_blocking();
        }
        handle.join().unwrap();
        assert!(desc.done.load(Ordering::Acquire));
    }

    #[test]
    fn test_scatter_mget_descriptor_concurrent() {
        let total_keys = 20;
        let num_shards = 4;
        let pending_shards = 4;
        let (tx, rx) = flume::bounded(1);

        let desc = Arc::new(ScatterMgetDescriptor::new(
            total_keys,
            num_shards,
            pending_shards,
            tx,
        ));

        let mut handles = Vec::new();
        for shard_id in 0..num_shards {
            let desc_clone = desc.clone();
            handles.push(thread::spawn(move || {
                let mut recycled = Vec::new();
                for k_idx in 0..5 {
                    let global_idx = shard_id * 5 + k_idx;
                    desc_clone
                        .write_result(global_idx, Some(Bytes::from(format!("val_{}", global_idx))));
                    recycled.push((global_idx, Bytes::from(format!("key_{}", global_idx))));
                }
                desc_clone.recycle_keys(shard_id, recycled);
                desc_clone.finish_shard();
            }));
        }

        if desc
            .state
            .compare_exchange(
                DESC_RUNNING,
                DESC_SLEEPING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            rx.recv_timeout(Duration::from_secs(2))
                .expect("mget notification timeout");
        }
        for h in handles {
            h.join().unwrap();
        }

        let results = desc.into_results();
        assert_eq!(results.len(), total_keys);
        for (i, r) in results.iter().enumerate() {
            assert_eq!(r, &Some(Bytes::from(format!("val_{}", i))));
        }

        let recycled = desc.take_recycled_keys();
        assert_eq!(recycled.len(), num_shards);
        for (shard_id, shard_keys) in recycled.iter().enumerate() {
            assert_eq!(shard_keys.len(), 5);
            for (k_idx, (global_idx, key)) in shard_keys.iter().enumerate() {
                assert_eq!(*global_idx, shard_id * 5 + k_idx);
                assert_eq!(key, &Bytes::from(format!("key_{}", global_idx)));
            }
        }
    }

    #[test]
    fn test_scatter_mset_descriptor_concurrent() {
        let num_shards = 3;
        let pending_shards = 3;
        let (tx, rx) = flume::bounded(1);

        let desc = Arc::new(ScatterMsetDescriptor::new(num_shards, pending_shards, tx));

        let mut handles = Vec::new();
        for shard_id in 0..num_shards {
            let desc_clone = desc.clone();
            handles.push(thread::spawn(move || {
                let pairs = vec![
                    (Bytes::from(format!("k_{}_1", shard_id)), Bytes::from("v1")),
                    (Bytes::from(format!("k_{}_2", shard_id)), Bytes::from("v2")),
                ];
                desc_clone.recycle_pairs(shard_id, pairs);
                desc_clone.finish_shard();
            }));
        }

        if desc
            .state
            .compare_exchange(
                DESC_RUNNING,
                DESC_SLEEPING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            rx.recv_timeout(Duration::from_secs(2))
                .expect("mset notification timeout");
        }
        for h in handles {
            h.join().unwrap();
        }

        let recycled = desc.take_recycled_pairs();
        assert_eq!(recycled.len(), num_shards);
        for (shard_id, pairs) in recycled.iter().enumerate() {
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs[0].0, Bytes::from(format!("k_{}_1", shard_id)));
            assert_eq!(pairs[1].0, Bytes::from(format!("k_{}_2", shard_id)));
        }
    }

    #[test]
    fn test_descriptors_finish_nonblocking_when_notify_channel_full() {
        // Regression guard: if the bounded(1) notify channel already has a permit,
        // finish_shard() or finish() MUST NOT block the calling shard thread.
        let (tx, _rx) = flume::bounded(1);
        tx.send(()).unwrap(); // fill channel to capacity 1

        let mget_desc = ScatterMgetDescriptor::new(1, 1, 1, tx.clone());
        mget_desc.finish_shard(); // must return immediately without blocking

        let mset_desc = ScatterMsetDescriptor::new(1, 1, tx.clone());
        mset_desc.finish_shard(); // must return immediately without blocking

        let fast_get = FastGetDescriptor::new(Bytes::from("k"));
        fast_get.finish(None); // must return immediately without blocking
        fast_get.finish(None); // ringing an already-rung bell too

        let fast_set = FastSetDescriptor::new(Bytes::from("k"), Bytes::from("v"), None);
        fast_set.finish(); // must return immediately without blocking
    }

    #[test]
    fn test_spsc_queue_push_pop_and_overflow() {
        let queue = SpscQueue::new(4);
        assert!(queue.is_empty());

        queue.push(1);
        queue.push(2);
        queue.push(3);
        assert!(!queue.is_empty());

        queue.push(4);
        queue.push(5);

        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), Some(4));
        assert_eq!(queue.pop(), Some(5));
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_spsc_queue_concurrent_overflow_keeps_order() {
        // Bursts slightly larger than the ring make the overflow fill and
        // empty on nearly every burst while the producer is still pushing,
        // which is where the consumer used to move overflow items into the
        // ring concurrently with the producer writing the same slot.
        const CAP: u64 = 4;
        const BURSTS: u64 = 200_000;
        let queue = Arc::new(SpscQueue::new(CAP as usize));
        let consumed = Arc::new(AtomicUsize::new(0));
        let producer = {
            let (queue, consumed) = (queue.clone(), consumed.clone());
            thread::spawn(move || {
                let mut next = 0u64;
                for b in 0..BURSTS {
                    for _ in 0..CAP + 1 + b % 3 {
                        queue.push(next);
                        next += 1;
                    }
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                    while consumed.load(Ordering::Acquire) as u64 != next {
                        assert!(std::time::Instant::now() < deadline, "consumer stuck");
                        std::hint::spin_loop();
                    }
                }
                next
            })
        };
        let total = BURSTS * (CAP + 1) + (0..BURSTS).map(|b| b % 3).sum::<u64>();
        let mut expected = 0;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while expected < total {
            match queue.pop() {
                Some(v) => {
                    assert_eq!(v, expected, "item lost or reordered");
                    expected += 1;
                    consumed.store(expected as usize, Ordering::Release);
                }
                None => {
                    assert!(std::time::Instant::now() < deadline, "stuck at {expected}");
                    std::hint::spin_loop();
                }
            }
        }
        assert_eq!(producer.join().unwrap(), total);
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_mesh_sleeping_flag_bypass() {
        let (mesh, receivers) = create_shard_mesh(2);
        let sender = &mesh[0][1];
        let rx = &receivers[1];

        // 1. When receiver is not sleeping, sending does NOT hit notify_rx channel
        assert!(!rx.sleeping.load(Ordering::SeqCst));
        sender
            .send(crate::shard::ShardMessage::NotifyList {
                keys: vec![Bytes::from("k1")],
            })
            .unwrap();
        assert!(!rx.notify_rx.try_consume());
        let msg = rx.try_recv().unwrap();
        if let crate::shard::ShardMessage::NotifyList { keys } = msg {
            assert_eq!(keys[0], Bytes::from("k1"));
        } else {
            panic!("unexpected message");
        }

        // 2. When receiver is sleeping, sending triggers notify_rx
        rx.sleeping.store(true, Ordering::SeqCst);
        sender
            .send(crate::shard::ShardMessage::NotifyList {
                keys: vec![Bytes::from("k2")],
            })
            .unwrap();
        assert!(rx.notify_rx.try_consume());
        let msg = rx.try_recv().unwrap();
        if let crate::shard::ShardMessage::NotifyList { keys } = msg {
            assert_eq!(keys[0], Bytes::from("k2"));
        } else {
            panic!("unexpected message");
        }
    }

    #[test]
    fn test_mesh_ping_pong_never_loses_wakeup() {
        // Each side sleeps in `recv()` while the other sends, which exercises
        // the sleeping-flag handshake on every round. A lost wakeup leaves a
        // thread asleep with a message queued, and the watchdog fires.
        const ROUNDS: usize = 200_000;
        let (mesh, mut receivers) = create_shard_mesh(2);
        let msg = || crate::shard::ShardMessage::NotifyList { keys: Vec::new() };
        let rx_b = receivers.pop().unwrap();
        let rx_a = receivers.pop().unwrap();
        let (tx_a, tx_b) = (mesh[0][1].clone(), mesh[1][0].clone());
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let done_b = done_tx.clone();
        thread::spawn(move || {
            for _ in 0..ROUNDS {
                tx_a.send(msg()).unwrap();
                rx_a.recv().unwrap();
            }
            let _ = done_tx.send(());
        });
        thread::spawn(move || {
            for _ in 0..ROUNDS {
                rx_b.recv().unwrap();
                tx_b.send(msg()).unwrap();
            }
            let _ = done_b.send(());
        });
        for _ in 0..2 {
            done_rx
                .recv_timeout(std::time::Duration::from_secs(60))
                .expect("a shard slept through a queued message (lost wakeup)");
        }
    }

    #[test]
    fn test_doorbell_ring_before_wait_is_kept() {
        let bell = Doorbell::new();
        bell.ring();
        bell.ring(); // coalesces
        bell.wait_blocking(); // returns at once
        assert!(!bell.try_consume());
    }

    #[test]
    fn test_doorbell_ping_pong_never_loses_wakeup() {
        // Two threads hand a token back and forth 200k times through two
        // bells; a single lost wake-up hangs the test.
        let a = Arc::new(Doorbell::new());
        let b = Arc::new(Doorbell::new());
        let turn = Arc::new(AtomicUsize::new(0));
        const N: usize = 200_000;
        let (a2, b2, t2) = (a.clone(), b.clone(), turn.clone());
        let h = thread::spawn(move || {
            for i in 0..N {
                while t2.load(Ordering::Acquire) != 2 * i + 1 {
                    a2.wait_blocking();
                }
                t2.store(2 * i + 2, Ordering::Release);
                b2.ring();
            }
        });
        for i in 0..N {
            turn.store(2 * i + 1, Ordering::Release);
            a.ring();
            while turn.load(Ordering::Acquire) != 2 * i + 2 {
                b.wait_blocking();
            }
        }
        h.join().unwrap();
    }

    #[test]
    fn test_doorbell_many_ringers_one_async_waiter() {
        // 8 producer threads bump a counter and ring; a monoio task waits on
        // the bell until it has seen every increment.
        let bell = Arc::new(Doorbell::new());
        let count = Arc::new(AtomicUsize::new(0));
        const PER: usize = 20_000;
        const THREADS: usize = 8;
        let producers: Vec<_> = (0..THREADS)
            .map(|_| {
                let (bell, count) = (bell.clone(), count.clone());
                thread::spawn(move || {
                    for _ in 0..PER {
                        count.fetch_add(1, Ordering::Release);
                        bell.ring();
                    }
                })
            })
            .collect();
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            while count.load(Ordering::Acquire) < PER * THREADS {
                bell.wait().await;
            }
        });
        for p in producers {
            p.join().unwrap();
        }
    }

    #[test]
    fn test_receiver_disconnects_when_all_senders_drop() {
        let (senders, mut receivers) = create_shard_mesh(2);
        let rx = receivers.remove(1);
        let h = thread::spawn(move || rx.recv().is_err());
        thread::sleep(Duration::from_millis(50)); // let it park
        drop(senders);
        assert!(h.join().unwrap(), "parked receiver must see the disconnect");
    }
}
