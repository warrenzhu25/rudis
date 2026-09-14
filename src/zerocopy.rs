use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Default page size for buffer memory alignment (4 KB)
pub const PAGE_SIZE: usize = 4096;

/// Default size of each registered buffer slot (64 KB)
pub const DEFAULT_SLOT_SIZE: usize = 64 * 1024;

/// Default number of registered buffer slots per pool (16 slots = 1 MB)
pub const DEFAULT_SLOT_COUNT: usize = 16;

/// Linux socket constant for MSG_ZEROCOPY (0x4000000)
pub const MSG_ZEROCOPY: libc::c_int = 0x04000000;

/// Telemetry metrics for Zero-Copy and Registered Buffer operations.
#[derive(Debug, Default)]
pub struct ZeroCopyStats {
    pub zc_send_calls: AtomicU64,
    pub zc_bytes_sent: AtomicU64,
    pub registered_buffer_hits: AtomicU64,
    pub registered_buffer_misses: AtomicU64,
    pub fallback_sends: AtomicU64,
}

impl ZeroCopyStats {
    pub fn new() -> Self {
        Self::default()
    }
}

/// A pre-allocated, page-aligned fixed buffer pool for Linux `io_uring` buffer registration.
/// Eliminates page-table traversal and MMU mapping on high-throughput network transfers.
pub struct RegisteredBufferPool {
    ptr: *mut u8,
    layout: Layout,
    slot_size: usize,
    slot_count: usize,
    free_slots: Vec<usize>,
    iovecs: Vec<libc::iovec>,
    stats: Arc<ZeroCopyStats>,
}

unsafe impl Send for RegisteredBufferPool {}
unsafe impl Sync for RegisteredBufferPool {}

impl RegisteredBufferPool {
    /// Creates and aligns a new fixed buffer pool.
    pub fn new(slot_count: usize, slot_size: usize, stats: Arc<ZeroCopyStats>) -> io::Result<Self> {
        let total_size = slot_count * slot_size;
        let layout = Layout::from_size_align(total_size, PAGE_SIZE)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "failed to allocate page-aligned registered buffer pool",
            ));
        }

        let mut iovecs = Vec::with_capacity(slot_count);
        let mut free_slots = Vec::with_capacity(slot_count);

        for i in 0..slot_count {
            let offset = i * slot_size;
            let slot_ptr = unsafe { ptr.add(offset) };
            iovecs.push(libc::iovec {
                iov_base: slot_ptr as *mut libc::c_void,
                iov_len: slot_size,
            });
            free_slots.push(i);
        }

        Ok(Self {
            ptr,
            layout,
            slot_size,
            slot_count,
            free_slots,
            iovecs,
            stats,
        })
    }

    /// Returns the array of iovecs suitable for `io_uring::Submitter::register_buffers`.
    pub fn iovecs(&self) -> &[libc::iovec] {
        &self.iovecs
    }

    /// Acquires an available buffer slot from the pool.
    pub fn acquire_slot(&mut self) -> Option<usize> {
        if let Some(slot_idx) = self.free_slots.pop() {
            self.stats.registered_buffer_hits.fetch_add(1, Ordering::Relaxed);
            Some(slot_idx)
        } else {
            self.stats.registered_buffer_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Releases a previously acquired slot back to the free list.
    pub fn release_slot(&mut self, slot_idx: usize) {
        if slot_idx < self.slot_count && !self.free_slots.contains(&slot_idx) {
            self.free_slots.push(slot_idx);
        }
    }

    /// Returns a mutable slice for the specified slot.
    pub fn get_slot_mut(&mut self, slot_idx: usize) -> Option<&mut [u8]> {
        if slot_idx < self.slot_count {
            let offset = slot_idx * self.slot_size;
            unsafe {
                Some(std::slice::from_raw_parts_mut(
                    self.ptr.add(offset),
                    self.slot_size,
                ))
            }
        } else {
            None
        }
    }

    /// Returns an immutable slice for the specified slot.
    pub fn get_slot(&self, slot_idx: usize) -> Option<&[u8]> {
        if slot_idx < self.slot_count {
            let offset = slot_idx * self.slot_size;
            unsafe {
                Some(std::slice::from_raw_parts(
                    self.ptr.add(offset),
                    self.slot_size,
                ))
            }
        } else {
            None
        }
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    pub fn slot_count(&self) -> usize {
        self.slot_count
    }

    pub fn available_slots(&self) -> usize {
        self.free_slots.len()
    }
}

impl Drop for RegisteredBufferPool {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                dealloc(self.ptr, self.layout);
            }
        }
    }
}

/// Zero-Copy socket configuration and transfer utilities.
pub struct ZeroCopyEngine {
    stats: Arc<ZeroCopyStats>,
}

impl ZeroCopyEngine {
    pub fn new(stats: Arc<ZeroCopyStats>) -> Self {
        Self { stats }
    }

    /// Enables `SO_ZEROCOPY` on the given raw socket file descriptor.
    pub fn enable_so_zerocopy(fd: RawFd) -> io::Result<()> {
        let enable: libc::c_int = 1;
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ZEROCOPY,
                &enable as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Attempts zero-copy send on a socket using `MSG_ZEROCOPY`.
    /// Falls back to standard non-blocking `send` if the kernel buffer is full or unsupported.
    pub fn send_zc(&self, fd: RawFd, data: &[u8]) -> io::Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }

        // According to Linux kernel docs, zero-copy incurs page-locking overhead,
        // so MSG_ZEROCOPY is optimal for payloads >= 4 KB.
        let flags = if data.len() >= PAGE_SIZE {
            libc::MSG_NOSIGNAL | MSG_ZEROCOPY
        } else {
            libc::MSG_NOSIGNAL
        };

        let ret = unsafe {
            libc::send(
                fd,
                data.as_ptr() as *const libc::c_void,
                data.len(),
                flags,
            )
        };

        if ret >= 0 {
            let sent = ret as usize;
            self.stats.zc_send_calls.fetch_add(1, Ordering::Relaxed);
            self.stats.zc_bytes_sent.fetch_add(sent as u64, Ordering::Relaxed);
            Ok(sent)
        } else {
            let err = io::Error::last_os_error();
            // Fall back to standard send on ENOBUFS or if MSG_ZEROCOPY is not supported
            if err.raw_os_error() == Some(libc::ENOBUFS) || flags & MSG_ZEROCOPY != 0 {
                self.stats.fallback_sends.fetch_add(1, Ordering::Relaxed);
                let fallback_ret = unsafe {
                    libc::send(
                        fd,
                        data.as_ptr() as *const libc::c_void,
                        data.len(),
                        libc::MSG_NOSIGNAL,
                    )
                };
                if fallback_ret >= 0 {
                    let sent = fallback_ret as usize;
                    self.stats.zc_bytes_sent.fetch_add(sent as u64, Ordering::Relaxed);
                    return Ok(sent);
                }
            }
            Err(err)
        }
    }

    /// Builds an `io_uring::opcode::SendZc` entry for registered fixed buffer zero-copy transmission.
    pub fn build_io_uring_send_zc(
        fd: RawFd,
        buf_ptr: *const u8,
        len: u32,
        slot_index: Option<u16>,
    ) -> io_uring::squeue::Entry {
        let mut op = io_uring::opcode::SendZc::new(
            io_uring::types::Fd(fd),
            buf_ptr,
            len,
        );
        if let Some(idx) = slot_index {
            op = op.buf_index(Some(idx));
        }
        op.build()
    }

    pub fn stats(&self) -> &Arc<ZeroCopyStats> {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::net::UnixStream;

    #[test]
    fn test_registered_buffer_pool_alignment_and_lifecycle() {
        let stats = Arc::new(ZeroCopyStats::new());
        let mut pool = RegisteredBufferPool::new(4, 4096, stats.clone()).unwrap();

        assert_eq!(pool.slot_count(), 4);
        assert_eq!(pool.slot_size(), 4096);
        assert_eq!(pool.available_slots(), 4);

        // Verify page-alignment of the base pointer
        assert_eq!(pool.ptr as usize % PAGE_SIZE, 0);

        // Check iovecs
        let iovecs = pool.iovecs();
        assert_eq!(iovecs.len(), 4);
        for iov in iovecs {
            assert_eq!(iov.iov_len, 4096);
            assert_eq!(iov.iov_base as usize % PAGE_SIZE, 0);
        }

        // Acquire slots
        let s0 = pool.acquire_slot().expect("slot 0");
        let s1 = pool.acquire_slot().expect("slot 1");
        assert_eq!(pool.available_slots(), 2);

        // Write and read from slot
        {
            let buf = pool.get_slot_mut(s0).unwrap();
            buf[0] = 0xAA;
            buf[4095] = 0xBB;
        }
        {
            let buf = pool.get_slot(s0).unwrap();
            assert_eq!(buf[0], 0xAA);
            assert_eq!(buf[4095], 0xBB);
        }

        // Release slots
        pool.release_slot(s0);
        pool.release_slot(s1);
        assert_eq!(pool.available_slots(), 4);
        assert_eq!(stats.registered_buffer_hits.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_socket_zerocopy_engine_transfer() {
        let (s1, s2) = UnixStream::pair().expect("unix stream pair");
        let fd1 = s1.as_raw_fd();
        let fd2 = s2.as_raw_fd();

        let stats = Arc::new(ZeroCopyStats::new());
        let engine = ZeroCopyEngine::new(stats.clone());

        // Enable zerocopy on fd1
        let _ = ZeroCopyEngine::enable_so_zerocopy(fd1);

        // Send payload via zero-copy engine
        let msg = b"+PONG\r\n";
        let sent = engine.send_zc(fd1, msg).expect("send_zc succeeded");
        assert_eq!(sent, msg.len());

        // Receive on peer
        let mut recv_buf = [0u8; 32];
        let n = unsafe {
            libc::recv(
                fd2,
                recv_buf.as_mut_ptr() as *mut libc::c_void,
                recv_buf.len(),
                0,
            )
        };
        assert_eq!(n as usize, msg.len());
        assert_eq!(&recv_buf[..n as usize], msg);
        assert_eq!(stats.zc_send_calls.load(Ordering::Relaxed), 1);
        assert_eq!(stats.zc_bytes_sent.load(Ordering::Relaxed), msg.len() as u64);
    }
}
