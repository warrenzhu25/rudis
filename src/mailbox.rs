use bytes::Bytes;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

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

/// Shared-memory Scatter-Gather Descriptor for multi-shard MGET.
/// Remote shards write their looked up values directly into their respective slots.
pub struct ScatterMgetDescriptor {
    pub results: Box<[CachePadded<UnsafeCell<Option<Bytes>>>]>,
    pub pending: AtomicUsize,
    pub notify: CachePadded<UnsafeCell<flume::Sender<()>>>,
    pub recycled_keys: Box<[CachePadded<UnsafeCell<Vec<(usize, Bytes)>>>]>,
}

unsafe impl Send for ScatterMgetDescriptor {}
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
            notify: CachePadded(UnsafeCell::new(notify)),
            recycled_keys: recycled.into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn reset(&self, total_keys: usize, pending_shards: usize, notify: flume::Sender<()>) {
        self.pending.store(pending_shards, Ordering::Release);
        unsafe {
            *self.notify.get() = notify;
        }
        for i in 0..total_keys.min(self.results.len()) {
            unsafe {
                *self.results[i].get() = None;
            }
        }
    }

    #[inline(always)]
    pub fn write_result(&self, idx: usize, val: Option<Bytes>) {
        unsafe {
            *self.results[idx].get() = val;
        }
    }

    #[inline(always)]
    pub fn recycle_keys(&self, shard_id: usize, keys: Vec<(usize, Bytes)>) {
        unsafe {
            *self.recycled_keys[shard_id].get() = keys;
        }
    }

    #[inline(always)]
    pub fn finish_shard(&self) {
        if self.pending.fetch_sub(1, Ordering::Release) == 1 {
            let tx = unsafe { &*self.notify.get() };
            let _ = tx.send(());
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
            out.push(unsafe { (*cell.get()).take() });
        }
        out
    }

    #[inline(always)]
    pub fn take_recycled_keys(&self) -> Vec<Vec<(usize, Bytes)>> {
        let mut out = Vec::with_capacity(self.recycled_keys.len());
        for cell in self.recycled_keys.iter() {
            out.push(unsafe { std::mem::take(&mut *cell.get()) });
        }
        out
    }
}

/// Shared-memory Scatter-Gather Descriptor for multi-shard MSET.
pub struct ScatterMsetDescriptor {
    pub pending: AtomicUsize,
    pub notify: CachePadded<UnsafeCell<flume::Sender<()>>>,
    pub recycled_pairs: Box<[CachePadded<UnsafeCell<Vec<(Bytes, Bytes)>>>]>,
}

unsafe impl Send for ScatterMsetDescriptor {}
unsafe impl Sync for ScatterMsetDescriptor {}

impl ScatterMsetDescriptor {
    pub fn new(num_shards: usize, pending_shards: usize, notify: flume::Sender<()>) -> Self {
        let mut recycled = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            recycled.push(CachePadded(UnsafeCell::new(Vec::new())));
        }
        Self {
            pending: AtomicUsize::new(pending_shards),
            notify: CachePadded(UnsafeCell::new(notify)),
            recycled_pairs: recycled.into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn reset(&self, pending_shards: usize, notify: flume::Sender<()>) {
        self.pending.store(pending_shards, Ordering::Release);
        unsafe {
            *self.notify.get() = notify;
        }
    }

    #[inline(always)]
    pub fn recycle_pairs(&self, shard_id: usize, pairs: Vec<(Bytes, Bytes)>) {
        unsafe {
            *self.recycled_pairs[shard_id].get() = pairs;
        }
    }

    #[inline(always)]
    pub fn finish_shard(&self) {
        if self.pending.fetch_sub(1, Ordering::Release) == 1 {
            let tx = unsafe { &*self.notify.get() };
            let _ = tx.send(());
        }
    }

    #[inline(always)]
    pub fn take_recycled_pairs(&self) -> Vec<Vec<(Bytes, Bytes)>> {
        let mut out = Vec::with_capacity(self.recycled_pairs.len());
        for cell in self.recycled_pairs.iter() {
            out.push(unsafe { std::mem::take(&mut *cell.get()) });
        }
        out
    }
}

/// Direct shared-memory slot for single remote GET (bypasses responder channel allocations)
pub struct FastGetDescriptor {
    pub key: Bytes,
    pub val: CachePadded<UnsafeCell<Option<Bytes>>>,
    pub done: AtomicBool,
    pub notify: flume::Sender<()>,
}

unsafe impl Send for FastGetDescriptor {}
unsafe impl Sync for FastGetDescriptor {}

impl FastGetDescriptor {
    #[inline(always)]
    pub fn new(key: Bytes, notify: flume::Sender<()>) -> Self {
        Self {
            key,
            val: CachePadded(UnsafeCell::new(None)),
            done: AtomicBool::new(false),
            notify,
        }
    }

    #[inline(always)]
    pub fn finish(&self, val: Option<Bytes>) {
        unsafe {
            *self.val.get() = val;
        }
        self.done.store(true, Ordering::Release);
        let _ = self.notify.send(());
    }
}

/// Direct shared-memory slot for single remote SET (bypasses responder channel allocations)
pub struct FastSetDescriptor {
    pub key: Bytes,
    pub value: Bytes,
    pub expire_in: Option<Duration>,
    pub done: AtomicBool,
    pub notify: flume::Sender<()>,
}

unsafe impl Send for FastSetDescriptor {}
unsafe impl Sync for FastSetDescriptor {}

impl FastSetDescriptor {
    #[inline(always)]
    pub fn new(
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        notify: flume::Sender<()>,
    ) -> Self {
        Self {
            key,
            value,
            expire_in,
            done: AtomicBool::new(false),
            notify,
        }
    }

    #[inline(always)]
    pub fn finish(&self) {
        self.done.store(true, Ordering::Release);
        let _ = self.notify.send(());
    }
}

/// Direct shared-memory slot for cross-shard squashed batch responses.
/// Completely eliminates Flume mutex contention during multi-core spin-waits.
pub struct BatchResponder {
    pub ready: CachePadded<AtomicBool>,
    pub payload: CachePadded<
        UnsafeCell<
            Option<(
                Vec<(usize, u64, crate::resp::Command)>,
                Vec<(usize, crate::shard::CompactResp)>,
            )>,
        >,
    >,
    pub notify_tx: flume::Sender<()>,
    pub notify_rx: flume::Receiver<()>,
}

unsafe impl Send for BatchResponder {}
unsafe impl Sync for BatchResponder {}

impl BatchResponder {
    pub fn new() -> Self {
        let (tx, rx) = flume::bounded(1);
        Self {
            ready: CachePadded(AtomicBool::new(false)),
            payload: CachePadded(UnsafeCell::new(None)),
            notify_tx: tx,
            notify_rx: rx,
        }
    }

    #[inline(always)]
    pub fn finish(
        &self,
        items: Vec<(usize, u64, crate::resp::Command)>,
        results: Vec<(usize, crate::shard::CompactResp)>,
    ) {
        unsafe {
            *self.payload.get() = Some((items, results));
        }
        self.ready.store(true, Ordering::Release);
        let _ = self.notify_tx.try_send(());
    }

    #[inline(always)]
    pub fn try_take(
        &self,
    ) -> Option<(
        Vec<(usize, u64, crate::resp::Command)>,
        Vec<(usize, crate::shard::CompactResp)>,
    )> {
        if self.ready.load(Ordering::Acquire) {
            self.ready.store(false, Ordering::Relaxed);
            unsafe { (*self.payload.get()).take() }
        } else {
            None
        }
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
    overflow: std::sync::Mutex<std::collections::VecDeque<T>>,
    has_overflow: AtomicBool,
}

unsafe impl<T: Send> Send for SpscQueue<T> {}
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
            overflow: std::sync::Mutex::new(std::collections::VecDeque::new()),
            has_overflow: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    pub fn push(&self, item: T) {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if tail.wrapping_sub(head) < self.capacity {
            unsafe {
                *self.buffer[tail & self.mask].get() = Some(item);
            }
            self.tail.store(tail.wrapping_add(1), Ordering::Release);
        } else {
            let mut q = self.overflow.lock().unwrap();
            q.push_back(item);
            self.has_overflow.store(true, Ordering::Release);
        }
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head != tail {
            let item = unsafe { (*self.buffer[head & self.mask].get()).take() };
            self.head.store(head.wrapping_add(1), Ordering::Release);
            item
        } else if self.has_overflow.load(Ordering::Acquire) {
            let mut q = self.overflow.lock().unwrap();
            let item = q.pop_front();
            if q.is_empty() {
                self.has_overflow.store(false, Ordering::Release);
            }
            item
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head != tail {
            return false;
        }
        if self.has_overflow.load(Ordering::Acquire) {
            let q = self.overflow.lock().unwrap();
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

/// Sender handle from one shard to a specific target shard.
/// Pushes to a dedicated lock-free SPSC ring and conditionally notifies the target thread.
#[derive(Clone)]
pub struct ShardSender {
    pub target_shard: usize,
    pub ring: std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>,
    pub target_notify: flume::Sender<()>,
}

impl ShardSender {
    #[inline(always)]
    pub fn send(&self, msg: crate::shard::ShardMessage) -> Result<(), SendError> {
        self.ring.push(msg);
        let _ = self.target_notify.try_send(());
        Ok(())
    }
}

/// Receiver handle for a shard worker.
/// Checks incoming SPSC rings from all shards without locks, sleeping only when all are drained.
#[derive(Clone)]
pub struct ShardReceiver {
    pub shard_id: usize,
    pub incoming_rings: Vec<std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>>,
    pub notify_rx: flume::Receiver<()>,
}

impl ShardReceiver {
    #[inline(always)]
    pub fn try_recv(&self) -> Result<crate::shard::ShardMessage, RecvError> {
        for ring in &self.incoming_rings {
            if let Some(msg) = ring.pop() {
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

            if self.notify_rx.recv().is_err() {
                return self.try_recv();
            }
            while self.notify_rx.try_recv().is_ok() {}

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

            if self.notify_rx.recv_async().await.is_err() {
                return self.try_recv();
            }
            while self.notify_rx.try_recv().is_ok() {}

            if let Ok(msg) = self.try_recv() {
                return Ok(msg);
            }
        }
    }
}

/// Creates a fully-connected lock-free cross-shard communication mesh.
pub fn create_shard_mesh(num_shards: usize) -> (Vec<Vec<ShardSender>>, Vec<ShardReceiver>) {
    let mut notifiers = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        notifiers.push(flume::bounded::<()>(1));
    }

    // Matrix of SPSC rings: rings[i][j] is the ring from producer shard i to consumer shard j
    let mut rings: Vec<Vec<std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>>> =
        Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        let mut row = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            row.push(std::sync::Arc::new(SpscQueue::new(4096)));
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
                target_notify: notifiers[j].0.clone(),
            });
        }
        senders_mesh.push(shard_senders);
    }

    let mut receivers = Vec::with_capacity(num_shards);
    for (j, notifier) in notifiers.into_iter().enumerate().take(num_shards) {
        let mut incoming = Vec::with_capacity(num_shards);
        for row in &rings {
            incoming.push(row[j].clone());
        }
        receivers.push(ShardReceiver {
            shard_id: j,
            incoming_rings: incoming,
            notify_rx: notifier.1,
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
    fn test_fast_get_descriptor() {
        let (tx, rx) = flume::bounded(1);
        let desc = Arc::new(FastGetDescriptor::new(Bytes::from("key1"), tx));
        assert!(!desc.done.load(Ordering::Acquire));

        let desc_clone = desc.clone();
        let handle = thread::spawn(move || {
            desc_clone.finish(Some(Bytes::from("val1")));
        });

        rx.recv_timeout(Duration::from_secs(1))
            .expect("notification failed");
        handle.join().unwrap();

        assert!(desc.done.load(Ordering::Acquire));
        let val = unsafe { (*desc.val.get()).take() };
        assert_eq!(val, Some(Bytes::from("val1")));
    }

    #[test]
    fn test_fast_set_descriptor() {
        let (tx, rx) = flume::bounded(1);
        let desc = Arc::new(FastSetDescriptor::new(
            Bytes::from("key_set"),
            Bytes::from("val_set"),
            Some(Duration::from_secs(10)),
            tx,
        ));
        assert!(!desc.done.load(Ordering::Acquire));
        assert_eq!(desc.key, Bytes::from("key_set"));
        assert_eq!(desc.value, Bytes::from("val_set"));
        assert_eq!(desc.expire_in, Some(Duration::from_secs(10)));

        let desc_clone = desc.clone();
        let handle = thread::spawn(move || {
            desc_clone.finish();
        });

        rx.recv_timeout(Duration::from_secs(1))
            .expect("notification failed");
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

        rx.recv_timeout(Duration::from_secs(2))
            .expect("mget notification timeout");
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

        rx.recv_timeout(Duration::from_secs(2))
            .expect("mset notification timeout");
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
}
