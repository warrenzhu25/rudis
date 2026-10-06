//! The byte stream a client connection runs over.
//!
//! The client loop in `connection.rs` is written once, generic over
//! [`ClientTransport`], and monomorphized per transport: plaintext TCP
//! ([`PlainTransport`]) and TLS (`crate::tls::TlsTransport`). Generics rather
//! than `dyn` keep the plaintext hot path exactly what it was before the loop
//! became generic: every call below inlines to the same io_uring read with a
//! rented `BytesMut`, the same non-blocking `recv` drains and the same
//! `send`-then-io_uring write.
//!
//! A transport also says how *other* threads reach the client
//! ([`PushTarget`]): MONITOR lines and client-side-caching invalidations are
//! produced on whichever shard ran the command. A plaintext socket takes them
//! straight on the fd; a TLS session has to encrypt them on the shard that
//! owns it, so they are queued there instead.

use std::io;
use std::os::unix::io::{AsRawFd, RawFd};

use bytes::{Bytes, BytesMut};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt, OwnedReadHalf, OwnedWriteHalf, Splitable};
use monoio::net::TcpStream;

use crate::connection::{READ_BUFFER_SIZE, RecvBytesMut};

/// The read buffer always keeps at least this much spare room before a read.
pub(crate) const MIN_READ_SPARE: usize = 16 * 1024;

/// How many out-of-band messages may wait for a TLS client before new ones
/// are dropped. Plaintext pushes are non-blocking sends that drop once the
/// socket buffer is full; this is the TLS equivalent of that bound.
pub(crate) const PUSH_QUEUE_CAPACITY: usize = 4096;

/// Where out-of-band data for a client (MONITOR output, tracking
/// invalidations) is delivered from any thread.
#[derive(Clone, Debug)]
pub enum PushTarget {
    /// Plaintext socket: write directly to the fd, without blocking.
    Fd(RawFd),
    /// Encrypted session: queue for the owning connection to encrypt and send.
    Queue(flume::Sender<Bytes>),
}

impl PushTarget {
    /// Delivers `msg` if there is room, dropping it otherwise (never blocks).
    pub fn push(&self, msg: &[u8]) {
        match self {
            // SAFETY: `msg` is a live slice and the length matches; send only reads it,
            // so even a stale fd cannot corrupt memory. The fd comes from a client
            // registry entry, and the connection removes its registry entry (ClientCleanup / PubsubCleanup) before its socket is closed, so the fd is not a reused number.
            PushTarget::Fd(fd) => unsafe {
                libc::send(
                    *fd,
                    msg.as_ptr() as *const libc::c_void,
                    msg.len(),
                    libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                );
            },
            PushTarget::Queue(tx) => {
                let _ = tx.try_send(Bytes::copy_from_slice(msg));
            }
        }
    }
}

/// A client connection's transport, seen as a plaintext byte stream.
pub(crate) trait ClientTransport: Sized + 'static {
    type ReadHalf: TransportRead;
    type WriteHalf: TransportWrite;

    /// The underlying socket, for `shutdown` (CLIENT KILL, shutdown drain)
    /// and disconnect polling while a command blocks.
    fn raw_fd(&self) -> RawFd;

    /// How other threads deliver out-of-band data to this client.
    fn push_target(&self) -> PushTarget;

    /// Waits for client input and appends it to `buf`'s spare capacity.
    /// Returns the number of bytes appended; 0 means the client closed.
    async fn read(&mut self, buf: BytesMut) -> (io::Result<usize>, BytesMut);

    /// Appends input that has already arrived without waiting for more.
    /// Returns the number of bytes appended (0 if nothing was ready).
    fn read_ready(&mut self, buf: &mut BytesMut) -> usize;

    /// Writes all of `data` and hands the buffer back.
    async fn write_all(&mut self, data: Vec<u8>) -> (io::Result<()>, Vec<u8>);

    /// Writes all of `out_buf` (leaving it empty on success), trying a
    /// non-blocking send first. `set_omem` is told how many bytes are still
    /// queued while the write has to wait, and 0 once it is done.
    async fn flush(&mut self, out_buf: &mut Vec<u8>, set_omem: impl FnMut(usize))
    -> io::Result<()>;

    /// Splits for Pub/Sub mode, where replies and published messages are
    /// written by a separate task fed through `writer_tx`. A transport that
    /// needs its writer to act on something the reader saw, or has its own
    /// queued pushes, sends them through `writer_tx`.
    fn into_split(self, writer_tx: &flume::Sender<Bytes>) -> (Self::ReadHalf, Self::WriteHalf);

    /// The raw TCP stream, for the replication links that take over the
    /// socket. Transports that cannot hand it out return themselves.
    fn into_tcp_stream(self) -> Result<TcpStream, Self>;
}

/// The read side of a transport split for Pub/Sub mode.
pub(crate) trait TransportRead {
    /// Waits for client input and appends it to `buf`. 0 means closed.
    async fn read_append(&mut self, buf: &mut BytesMut) -> io::Result<usize>;
}

/// The write side of a transport split for Pub/Sub mode.
pub(crate) trait TransportWrite: 'static {
    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()>;
}

/// Plaintext TCP.
pub(crate) struct PlainTransport {
    stream: TcpStream,
    fd: RawFd,
}

impl PlainTransport {
    pub(crate) fn new(stream: TcpStream) -> Self {
        let fd = stream.as_raw_fd();
        Self { stream, fd }
    }
}

/// Non-blocking `recv` into `buf`'s spare capacity (growing it first if
/// short). Returns the bytes appended and the room that was offered.
#[inline]
pub(crate) fn recv_ready(fd: RawFd, buf: &mut BytesMut) -> (usize, usize) {
    if buf.capacity() - buf.len() < MIN_READ_SPARE {
        buf.reserve(READ_BUFFER_SIZE);
    }
    let spare = buf.spare_capacity_mut();
    let spare_len = spare.len();
    // SAFETY: `spare` is `buf`'s spare capacity of `spare_len` bytes and recv
    // writes at most that many; `fd` is the caller's live socket.
    let n = unsafe {
        libc::recv(
            fd,
            spare.as_mut_ptr() as *mut libc::c_void,
            spare_len,
            libc::MSG_DONTWAIT,
        )
    };
    if n > 0 {
        // SAFETY: recv wrote `n > 0` bytes (at most `spare_len`) into the spare
        // capacity, so the new length is within capacity and fully initialized.
        unsafe { buf.set_len(buf.len() + n as usize) };
        (n as usize, spare_len)
    } else {
        (0, spare_len)
    }
}

impl ClientTransport for PlainTransport {
    type ReadHalf = PlainReadHalf;
    type WriteHalf = PlainWriteHalf;

    #[inline(always)]
    fn raw_fd(&self) -> RawFd {
        self.fd
    }

    fn push_target(&self) -> PushTarget {
        PushTarget::Fd(self.fd)
    }

    #[inline(always)]
    async fn read(&mut self, buf: BytesMut) -> (io::Result<usize>, BytesMut) {
        let avail_before = buf.capacity() - buf.len();
        // Rent BytesMut directly to monoio's io_uring driver (zero intermediate read_buf memcpy)
        let (res, RecvBytesMut(mut buf)) = self.stream.read(RecvBytesMut(buf)).await;
        match res {
            // Drain any additional bytes waiting in kernel TCP socket buffer if spare capacity was completely filled
            Ok(n) if n == avail_before => {
                let mut total = n;
                loop {
                    let (drained, spare_len) = recv_ready(self.fd, &mut buf);
                    total += drained;
                    if drained == 0 || drained < spare_len {
                        break;
                    }
                }
                (Ok(total), buf)
            }
            res => (res, buf),
        }
    }

    #[inline(always)]
    fn read_ready(&mut self, buf: &mut BytesMut) -> usize {
        recv_ready(self.fd, buf).0
    }

    #[inline(always)]
    async fn write_all(&mut self, data: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
        let (res, data) = self.stream.write_all(data).await;
        (res.map(|_| ()), data)
    }

    #[inline(always)]
    async fn flush(
        &mut self,
        out_buf: &mut Vec<u8>,
        mut set_omem: impl FnMut(usize),
    ) -> io::Result<()> {
        let len = out_buf.len();
        // SAFETY: `out_buf` holds `len` initialized bytes, borrowed for the call;
        // `self.fd` belongs to `self.stream`.
        let send_ret = unsafe {
            libc::send(
                self.fd,
                out_buf.as_ptr() as *const libc::c_void,
                len,
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if send_ret == len as isize {
            out_buf.clear();
            Ok(())
        } else if send_ret > 0 {
            let rem = out_buf[send_ret as usize..].to_vec();
            out_buf.clear();
            set_omem(rem.len());
            let (write_res, _) = self.stream.write_all(rem).await;
            set_omem(0);
            write_res.map(|_| ())
        } else {
            set_omem(len);
            let (write_res, returned_buf) = self.stream.write_all(std::mem::take(out_buf)).await;
            *out_buf = returned_buf;
            out_buf.clear();
            set_omem(0);
            write_res.map(|_| ())
        }
    }

    fn into_split(self, _writer_tx: &flume::Sender<Bytes>) -> (PlainReadHalf, PlainWriteHalf) {
        let (reader, writer) = self.stream.into_split();
        (
            PlainReadHalf {
                reader,
                read_buf: vec![0u8; READ_BUFFER_SIZE],
            },
            PlainWriteHalf { writer },
        )
    }

    fn into_tcp_stream(self) -> Result<TcpStream, Self> {
        Ok(self.stream)
    }
}

pub(crate) struct PlainReadHalf {
    reader: OwnedReadHalf<TcpStream>,
    read_buf: Vec<u8>,
}

impl TransportRead for PlainReadHalf {
    async fn read_append(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        let (res, returned_buf) = self.reader.read(std::mem::take(&mut self.read_buf)).await;
        self.read_buf = returned_buf;
        let n = res?;
        buf.extend_from_slice(&self.read_buf[..n]);
        Ok(n)
    }
}

pub(crate) struct PlainWriteHalf {
    writer: OwnedWriteHalf<TcpStream>,
}

impl TransportWrite for PlainWriteHalf {
    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        self.writer.write_all(data).await.0.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn push_target_fd_writes_to_socket() {
        let (mut peer, ours) = std::os::unix::net::UnixStream::pair().unwrap();
        let target = PushTarget::Fd(ours.as_raw_fd());
        target.push(b"+hello\r\n");
        let mut got = [0u8; 8];
        peer.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"+hello\r\n");
    }

    #[test]
    fn push_target_queue_is_bounded_and_never_blocks() {
        let (tx, rx) = flume::bounded(2);
        let target = PushTarget::Queue(tx);
        for msg in [&b"a"[..], b"b", b"c"] {
            target.push(msg);
        }
        // The third message is dropped rather than blocking the producer.
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"a"));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"b"));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn recv_ready_appends_available_bytes_and_grows_buffer() {
        let (mut peer, ours) = std::os::unix::net::UnixStream::pair().unwrap();
        ours.set_nonblocking(true).unwrap();
        let fd = ours.as_raw_fd();
        let mut buf = BytesMut::new();
        assert_eq!(recv_ready(fd, &mut buf).0, 0, "nothing ready yet");
        assert!(buf.capacity() >= MIN_READ_SPARE, "grown before reading");
        peer.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let (n, offered) = recv_ready(fd, &mut buf);
        assert_eq!(n, 14);
        assert!(offered >= MIN_READ_SPARE);
        assert_eq!(&buf[..], b"*1\r\n$4\r\nPING\r\n");
        peer.write_all(b"more").unwrap();
        assert_eq!(recv_ready(fd, &mut buf).0, 4);
        assert_eq!(&buf[..], b"*1\r\n$4\r\nPING\r\nmore");
    }

    #[test]
    fn plain_transport_round_trip_over_tcp() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::thread::spawn(move || {
            let mut s = std::net::TcpStream::connect(addr).unwrap();
            s.write_all(b"ping").unwrap();
            let mut got = [0u8; 4];
            s.read_exact(&mut got).unwrap();
            assert_eq!(&got, b"pong");
        });
        let (std_stream, _) = listener.accept().unwrap();
        std_stream.set_nonblocking(true).unwrap();
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async move {
            let stream = TcpStream::from_std(std_stream).unwrap();
            let mut t = PlainTransport::new(stream);
            let mut buf = BytesMut::with_capacity(READ_BUFFER_SIZE);
            let mut got = 0;
            while got < 4 {
                let (res, b) = t.read(buf).await;
                buf = b;
                got += res.unwrap();
            }
            assert_eq!(&buf[..], b"ping");
            let mut out = b"pong".to_vec();
            let mut omem = Vec::new();
            t.flush(&mut out, |n| omem.push(n)).await.unwrap();
            assert!(out.is_empty());
            assert!(omem.iter().all(|&n| n == 0 || n == 4));
        });
        client.join().unwrap();
    }
}
