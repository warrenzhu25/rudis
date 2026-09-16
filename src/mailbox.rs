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
    pub results: Box<[UnsafeCell<Option<Bytes>>]>,
    pub pending: AtomicUsize,
    pub notify: flume::Sender<()>,
    pub recycled_keys: Box<[UnsafeCell<Vec<(usize, Bytes)>>]>,
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
            vec.push(UnsafeCell::new(None));
        }
        let mut recycled = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            recycled.push(UnsafeCell::new(Vec::new()));
        }
        Self {
            results: vec.into_boxed_slice(),
            pending: AtomicUsize::new(pending_shards),
            notify,
            recycled_keys: recycled.into_boxed_slice(),
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
            let _ = self.notify.send(());
        }
    }

    #[inline(always)]
    pub fn into_results(&self) -> Vec<Option<Bytes>> {
        let mut out = Vec::with_capacity(self.results.len());
        for cell in self.results.iter() {
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
    pub notify: flume::Sender<()>,
    pub recycled_pairs: Box<[UnsafeCell<Vec<(Bytes, Bytes)>>]>,
}

unsafe impl Send for ScatterMsetDescriptor {}
unsafe impl Sync for ScatterMsetDescriptor {}

impl ScatterMsetDescriptor {
    pub fn new(num_shards: usize, pending_shards: usize, notify: flume::Sender<()>) -> Self {
        let mut recycled = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            recycled.push(UnsafeCell::new(Vec::new()));
        }
        Self {
            pending: AtomicUsize::new(pending_shards),
            notify,
            recycled_pairs: recycled.into_boxed_slice(),
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
            let _ = self.notify.send(());
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
    pub val: UnsafeCell<Option<Bytes>>,
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
            val: UnsafeCell::new(None),
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
