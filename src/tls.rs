//! TLS support for Rudis, in userspace with `rustls`.
//!
//! Provides in-memory and file-based TLS certificate provisioning, the
//! server-side handshake, and [`TlsTransport`], which lets TLS clients run the
//! same connection loop as plaintext ones (see `crate::transport`).
//!
//! Kernel TLS offload is deliberately not implemented: it needs the session
//! keys installed with `setsockopt(SOL_TLS, TLS_TX/TLS_RX)` (via rustls'
//! secret extraction), TLS 1.3 control records (key updates, tickets, alerts)
//! handled out of band on receive, and the `tls` kernel module. Rudis does not
//! use `sendfile`, so offload would mostly move the same AES-GCM work into the
//! kernel.

use std::cell::RefCell;
use std::io::{self, BufRead, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use monoio::buf::IoBuf;
use monoio::io::{
    AsyncReadRent, AsyncWriteRentExt, CancelableAsyncReadRent, Canceller, OwnedReadHalf,
    OwnedWriteHalf, Splitable,
};
use monoio::net::TcpStream;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection};

use crate::connection::READ_BUFFER_SIZE;
use crate::transport::{
    ClientTransport, PUSH_QUEUE_CAPACITY, PushTarget, TransportRead, TransportWrite,
};

/// Generates an in-memory self-signed TLS certificate and private key for development/tests.
pub fn generate_self_signed_cert(
    subject_alt_names: Vec<String>,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut params = rcgen::CertificateParams::new(subject_alt_names)
        .map_err(|e| format!("rcgen CertificateParams error: {}", e))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Rudis In-Memory Dev Cert");
    params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "Rudis Server");

    let key_pair = rcgen::KeyPair::generate().map_err(|e| format!("rcgen KeyPair error: {}", e))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| format!("rcgen self_signed error: {}", e))?;

    let cert_der = cert.der().to_vec();
    let key_der = key_pair.serialize_der();
    Ok((cert_der, key_der))
}

/// Builds a `rustls::ServerConfig` from DER-encoded certificate and private key.
pub fn create_server_config(cert_der: &[u8], key_der: &[u8]) -> Result<Arc<ServerConfig>, String> {
    let cert = CertificateDer::from(cert_der.to_vec());
    let key = PrivateKeyDer::try_from(key_der.to_vec())
        .map_err(|e| format!("Invalid private key DER: {:?}", e))?;
    server_config_from(vec![cert], key)
}

fn server_config_from(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<ServerConfig>, String> {
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("Failed to create rustls ServerConfig: {}", e))?;

    Ok(Arc::new(config))
}

/// Loads the certificate chain and private key from files.
///
/// PEM files, the format `tls-cert-file`/`tls-key-file` take in Redis, may
/// hold a full chain (leaf first) and a PKCS#8, PKCS#1 or SEC1 key. Files that
/// are not PEM are read as one DER certificate and a DER key.
pub fn load_certs_and_key_from_files(
    cert_path: &Path,
    key_path: &Path,
) -> Result<Arc<ServerConfig>, String> {
    let cert_bytes = std::fs::read(cert_path)
        .map_err(|e| format!("Cannot read cert file {}: {}", cert_path.display(), e))?;
    let key_bytes = std::fs::read(key_path)
        .map_err(|e| format!("Cannot read key file {}: {}", key_path.display(), e))?;

    let certs = if is_pem(&cert_bytes) {
        let certs = CertificateDer::pem_slice_iter(&cert_bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Invalid certificate PEM in {}: {}", cert_path.display(), e))?;
        if certs.is_empty() {
            return Err(format!(
                "No certificate (CERTIFICATE PEM block) in {}",
                cert_path.display()
            ));
        }
        certs
    } else {
        vec![CertificateDer::from(cert_bytes)]
    };
    let key = if is_pem(&key_bytes) {
        PrivateKeyDer::from_pem_slice(&key_bytes)
            .map_err(|e| format!("Invalid private key PEM in {}: {}", key_path.display(), e))?
    } else {
        PrivateKeyDer::try_from(key_bytes)
            .map_err(|e| format!("Invalid private key DER in {}: {}", key_path.display(), e))?
    };
    server_config_from(certs, key)
}

/// Whether `bytes` look like PEM text rather than binary DER.
fn is_pem(bytes: &[u8]) -> bool {
    bytes.windows(11).any(|w| w == b"-----BEGIN ")
}

fn tls_error(e: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("TLS error: {}", e))
}

/// A server-side `rustls` session.
pub struct TlsSession {
    pub conn: rustls::ServerConnection,
}

impl TlsSession {
    pub fn new(config: Arc<ServerConfig>) -> Result<Self, String> {
        let conn = rustls::ServerConnection::new(config)
            .map_err(|e| format!("Failed to create ServerConnection: {}", e))?;
        Ok(Self { conn })
    }

    /// Asynchronously performs TLS handshake using monoio TcpStream
    pub async fn handshake_monoio(
        &mut self,
        stream: &mut monoio::net::TcpStream,
    ) -> io::Result<()> {
        let mut read_buf = vec![0u8; 4096];
        while self.conn.is_handshaking() {
            while self.conn.wants_write() {
                let mut out = Vec::new();
                self.conn.write_tls(&mut out)?;
                if !out.is_empty() {
                    let (res, _) = stream.write_all(out).await;
                    res?;
                }
            }
            if self.conn.wants_read() {
                let (res, returned) = stream.read(read_buf).await;
                read_buf = returned;
                let n = res?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "TLS handshake EOF",
                    ));
                }
                // A client may send its first request right behind its
                // Finished message; feed everything so none of it is lost.
                // Decrypted early requests wait in rustls for the first read.
                let mut slice = &read_buf[..n];
                while !slice.is_empty() {
                    self.conn.read_tls(&mut slice)?;
                    self.conn.process_new_packets().map_err(tls_error)?;
                }
            }
        }
        while self.conn.wants_write() {
            let mut out = Vec::new();
            self.conn.write_tls(&mut out)?;
            if !out.is_empty() {
                let (res, _) = stream.write_all(out).await;
                res?;
            }
        }

        Ok(())
    }
}

/// Moves all plaintext rustls has decrypted into `buf`. Returns the bytes
/// appended and whether the client has sent `close_notify`.
fn drain_plaintext(conn: &mut ServerConnection, buf: &mut BytesMut) -> io::Result<(usize, bool)> {
    let mut appended = 0;
    let mut reader = conn.reader();
    loop {
        match reader.fill_buf() {
            Ok([]) => return Ok((appended, true)),
            Ok(chunk) => {
                let n = chunk.len();
                buf.extend_from_slice(chunk);
                reader.consume(n);
                appended += n;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok((appended, false)),
            Err(e) => return Err(e),
        }
    }
}

/// Feeds ciphertext received from the client into `conn` and appends every
/// byte of plaintext it yields to `buf`.
///
/// `read_tls` takes only a few KiB per call and rustls buffers at most
/// 16 KiB of decrypted data, so this loops until all of `wire` is consumed,
/// moving plaintext out after each step. Returns the plaintext bytes appended
/// and whether the client has sent `close_notify`.
pub(crate) fn decrypt_into(
    conn: &mut ServerConnection,
    mut wire: &[u8],
    buf: &mut BytesMut,
) -> io::Result<(usize, bool)> {
    let mut appended = 0;
    while !wire.is_empty() {
        conn.read_tls(&mut wire)?;
        conn.process_new_packets().map_err(tls_error)?;
        let (n, closed) = drain_plaintext(conn, buf)?;
        appended += n;
        if closed {
            return Ok((appended, true));
        }
    }
    Ok((appended, false))
}

/// Encrypts `plain` and appends the resulting TLS records, plus anything
/// else rustls has queued for the client (alerts, key updates), to `wire`.
pub(crate) fn encrypt_into(
    conn: &mut ServerConnection,
    plain: &[u8],
    wire: &mut Vec<u8>,
) -> io::Result<()> {
    // The send buffer limit is lifted in `TlsTransport::new`, so this takes
    // the whole of `plain` at once; it is drained into `wire` right away.
    conn.writer().write_all(plain)?;
    while conn.wants_write() {
        conn.write_tls(wire)?;
    }
    Ok(())
}

/// TLS over TCP: records are decrypted and encrypted by `rustls` on the
/// shard that owns the connection.
pub(crate) struct TlsTransport {
    stream: TcpStream,
    conn: ServerConnection,
    fd: RawFd,
    /// Ciphertext read from the socket, reused for every read.
    wire_in: Vec<u8>,
    /// Ciphertext waiting to be written, reused for every write.
    wire_out: Vec<u8>,
    /// Requests that arrived with the handshake may already be decrypted.
    check_buffered: bool,
    /// Out-of-band messages for this client queued by other threads.
    push_tx: flume::Sender<Bytes>,
    push_rx: flume::Receiver<Bytes>,
    /// Cancels the pending read when a push arrives while the client is idle.
    canceller: Canceller,
}

impl TlsTransport {
    /// Wraps a stream whose handshake `session` has completed.
    pub(crate) fn new(stream: TcpStream, session: TlsSession) -> Self {
        let mut conn = session.conn;
        // Encrypted replies are drained into `wire_out` straight away, so
        // rustls never holds more than one reply; its 64 KiB default cap
        // would only make larger replies fail.
        conn.set_buffer_limit(None);
        let fd = stream.as_raw_fd();
        let (push_tx, push_rx) = flume::bounded(PUSH_QUEUE_CAPACITY);
        Self {
            stream,
            conn,
            fd,
            wire_in: Vec::with_capacity(READ_BUFFER_SIZE),
            wire_out: Vec::with_capacity(READ_BUFFER_SIZE),
            check_buffered: true,
            push_tx,
            push_rx,
            canceller: Canceller::new(),
        }
    }

    /// Decrypts the `n` bytes just read into `wire_in`, queueing any TLS
    /// response rustls produced (alerts, key updates) for the next write.
    fn absorb(&mut self, n: usize, buf: &mut BytesMut) -> io::Result<(usize, bool)> {
        let res = decrypt_into(&mut self.conn, &self.wire_in[..n], buf)?;
        while self.conn.wants_write() {
            self.conn.write_tls(&mut self.wire_out)?;
        }
        Ok(res)
    }

    /// Encrypts every push queued so far into `wire_out`.
    fn take_pushes(&mut self) -> io::Result<()> {
        while let Ok(msg) = self.push_rx.try_recv() {
            encrypt_into(&mut self.conn, &msg, &mut self.wire_out)?;
        }
        Ok(())
    }

    /// Writes out `wire_out`: a non-blocking send first, io_uring for the rest.
    async fn send_wire(&mut self, mut set_omem: impl FnMut(usize)) -> io::Result<()> {
        let len = self.wire_out.len();
        if len == 0 {
            return Ok(());
        }
        let sent = unsafe {
            libc::send(
                self.fd,
                self.wire_out.as_ptr() as *const libc::c_void,
                len,
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if sent == len as isize {
            self.wire_out.clear();
            return Ok(());
        }
        let start = sent.max(0) as usize;
        set_omem(len - start);
        let wire = std::mem::take(&mut self.wire_out);
        let (res, slice) = self.stream.write_all(wire.slice(start..)).await;
        self.wire_out = slice.into_inner();
        self.wire_out.clear();
        set_omem(0);
        res.map(|_| ())
    }

    /// Reads ciphertext into `wire_in`. While MONITOR or client tracking is in
    /// use anywhere, an idle client may be sent pushes, so the read races the
    /// push queue; `Ok(None)` means a push interrupted it (and was encrypted
    /// into `wire_out`) before anything was read.
    async fn read_wire(&mut self) -> io::Result<Option<usize>> {
        let wire = std::mem::take(&mut self.wire_in);
        if !crate::connection::has_monitor_clients()
            && !crate::connection::HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed)
        {
            let (res, wire) = self.stream.read(wire).await;
            self.wire_in = wire;
            return res.map(Some);
        }
        let read = self.stream.cancelable_read(wire, self.canceller.handle());
        let mut read = std::pin::pin!(read);
        let pushed = monoio::select! {
            (res, wire) = &mut read => {
                self.wire_in = wire;
                return res.map(Some);
            }
            msg = self.push_rx.recv_async() => msg,
        };
        self.canceller = std::mem::take(&mut self.canceller).cancel();
        // The read may still have completed before the cancel took effect.
        let (res, wire) = read.await;
        self.wire_in = wire;
        if let Ok(msg) = pushed {
            encrypt_into(&mut self.conn, &msg, &mut self.wire_out)?;
        }
        match res {
            Ok(n) => Ok(Some(n)),
            Err(e) if e.raw_os_error() == Some(libc::ECANCELED) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn read_plain(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        if std::mem::take(&mut self.check_buffered) {
            let (n, closed) = drain_plaintext(&mut self.conn, buf)?;
            if n > 0 || closed {
                return Ok(n);
            }
        }
        loop {
            self.take_pushes()?;
            self.send_wire(|_| {}).await?;
            let n = match self.read_wire().await? {
                None => continue,
                Some(0) => return Ok(0),
                Some(n) => n,
            };
            let filled = n == self.wire_in.capacity();
            let (mut plain, closed) = self.absorb(n, buf)?;
            if closed {
                return Ok(plain);
            }
            if filled {
                plain += self.read_ready_plain(buf)?;
            }
            if plain > 0 {
                return Ok(plain);
            }
            // Only part of a record so far: wait for the rest.
        }
    }

    /// Decrypts whatever ciphertext is already waiting in the socket.
    fn read_ready_plain(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        let mut plain = 0;
        loop {
            let cap = self.wire_in.capacity();
            let n = unsafe {
                libc::recv(
                    self.fd,
                    self.wire_in.as_mut_ptr() as *mut libc::c_void,
                    cap,
                    libc::MSG_DONTWAIT,
                )
            };
            if n <= 0 {
                return Ok(plain);
            }
            let n = n as usize;
            unsafe { self.wire_in.set_len(n) };
            let (p, closed) = self.absorb(n, buf)?;
            plain += p;
            if closed || n < cap {
                return Ok(plain);
            }
        }
    }
}

impl ClientTransport for TlsTransport {
    type ReadHalf = TlsReadHalf;
    type WriteHalf = TlsWriteHalf;

    fn raw_fd(&self) -> RawFd {
        self.fd
    }

    fn push_target(&self) -> PushTarget {
        PushTarget::Queue(self.push_tx.clone())
    }

    async fn read(&mut self, mut buf: BytesMut) -> (io::Result<usize>, BytesMut) {
        let res = self.read_plain(&mut buf).await;
        (res, buf)
    }

    fn read_ready(&mut self, buf: &mut BytesMut) -> usize {
        match self.read_ready_plain(buf) {
            Ok(n) => n,
            Err(_) => {
                // A corrupt record ends the session: make the next read fail.
                unsafe { libc::shutdown(self.fd, libc::SHUT_RDWR) };
                0
            }
        }
    }

    async fn write_all(&mut self, data: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
        if let Err(e) = encrypt_into(&mut self.conn, &data, &mut self.wire_out) {
            return (Err(e), data);
        }
        (self.send_wire(|_| {}).await, data)
    }

    async fn flush(
        &mut self,
        out_buf: &mut Vec<u8>,
        set_omem: impl FnMut(usize),
    ) -> io::Result<()> {
        encrypt_into(&mut self.conn, out_buf, &mut self.wire_out)?;
        out_buf.clear();
        self.take_pushes()?;
        self.send_wire(set_omem).await
    }

    fn into_split(self, writer_tx: &flume::Sender<Bytes>) -> (TlsReadHalf, TlsWriteHalf) {
        let TlsTransport {
            stream,
            conn,
            wire_in,
            wire_out,
            push_rx,
            ..
        } = self;
        // Pushes queued for this client now go out through the Pub/Sub writer.
        // This ends once the client is unregistered and the senders are gone.
        let forward = writer_tx.clone();
        monoio::spawn(async move {
            while let Ok(msg) = push_rx.recv_async().await {
                if forward.send_async(msg).await.is_err() {
                    break;
                }
            }
        });
        let (reader, writer) = stream.into_split();
        let conn = Rc::new(RefCell::new(conn));
        (
            TlsReadHalf {
                reader,
                conn: conn.clone(),
                wire_in,
                writer_tx: writer_tx.clone(),
            },
            TlsWriteHalf {
                writer,
                conn,
                wire_out,
            },
        )
    }

    fn into_tcp_stream(self) -> Result<TcpStream, Self> {
        Err(self)
    }
}

/// The read side of a TLS session in Pub/Sub mode. The session state is
/// shared with the writer task; it is only borrowed between awaits.
pub(crate) struct TlsReadHalf {
    reader: OwnedReadHalf<TcpStream>,
    conn: Rc<RefCell<ServerConnection>>,
    wire_in: Vec<u8>,
    writer_tx: flume::Sender<Bytes>,
}

impl TransportRead for TlsReadHalf {
    async fn read_append(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        loop {
            let (res, wire) = self.reader.read(std::mem::take(&mut self.wire_in)).await;
            self.wire_in = wire;
            let n = res?;
            if n == 0 {
                return Ok(0);
            }
            let mut conn = self.conn.borrow_mut();
            let (plain, closed) = decrypt_into(&mut conn, &self.wire_in[..n], buf)?;
            if conn.wants_write() {
                // rustls has a response to send (e.g. to a key update); an
                // empty message makes the writer task flush it.
                let _ = self.writer_tx.try_send(Bytes::new());
            }
            if plain > 0 || closed {
                return Ok(plain);
            }
        }
    }
}

/// The write side of a TLS session in Pub/Sub mode.
pub(crate) struct TlsWriteHalf {
    writer: OwnedWriteHalf<TcpStream>,
    conn: Rc<RefCell<ServerConnection>>,
    wire_out: Vec<u8>,
}

impl TransportWrite for TlsWriteHalf {
    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        encrypt_into(&mut self.conn.borrow_mut(), &data, &mut self.wire_out)?;
        if self.wire_out.is_empty() {
            return Ok(());
        }
        let (res, wire) = self
            .writer
            .write_all(std::mem::take(&mut self.wire_out))
            .await;
        self.wire_out = wire;
        self.wire_out.clear();
        res.map(|_| ())
    }
}

/// Configuration for running a TLS listener on worker shards.
#[derive(Clone)]
pub struct TlsWorkerConfig {
    pub tls_port: u16,
    pub server_config: Arc<ServerConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_tls_cert_generation_and_config() {
        let (cert_der, key_der) =
            generate_self_signed_cert(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .expect("Failed to generate test self-signed cert");
        assert!(!cert_der.is_empty());
        assert!(!key_der.is_empty());

        let config =
            create_server_config(&cert_der, &key_der).expect("Failed to create ServerConfig");
        let session = TlsSession::new(config);
        assert!(session.is_ok());
    }

    #[test]
    fn test_tls_worker_config() {
        let (cert_der, key_der) =
            generate_self_signed_cert(vec!["localhost".to_string()]).expect("generate cert failed");
        let server_config = create_server_config(&cert_der, &key_der).unwrap();
        let worker_cfg = TlsWorkerConfig {
            tls_port: 16379,
            server_config,
        };
        assert_eq!(worker_cfg.tls_port, 16379);

        // Invalid key der returns error
        assert!(create_server_config(&cert_der, &[0xDE, 0xAD, 0xBE, 0xEF]).is_err());
    }

    /// Writes `files` into a fresh per-test temp directory and returns it.
    fn temp_files(test: &str, files: &[(&str, &[u8])]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rudis-tls-{}-{}", test, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for (name, data) in files {
            std::fs::write(dir.join(name), data).unwrap();
        }
        dir
    }

    #[test]
    fn test_load_pem_cert_and_key_files() {
        let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let dir = temp_files(
            "pem",
            &[
                ("cert.pem", cert.pem().as_bytes()),
                ("key.pem", key.serialize_pem().as_bytes()),
            ],
        );
        let config = load_certs_and_key_from_files(&dir.join("cert.pem"), &dir.join("key.pem"))
            .expect("PEM cert and key load");
        let (client, server) = handshake(config, cert.der().to_vec());
        assert!(!client.is_handshaking() && !server.is_handshaking());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_pem_chain_presents_leaf_signed_by_ca() {
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .signed_by(&leaf_key, &issuer)
            .unwrap();
        // Leaf first, then the issuer, as certbot's fullchain.pem.
        let chain = format!("{}{}", leaf.pem(), ca_cert.pem());
        let dir = temp_files(
            "chain",
            &[
                ("fullchain.pem", chain.as_bytes()),
                ("key.pem", leaf_key.serialize_pem().as_bytes()),
            ],
        );
        let config =
            load_certs_and_key_from_files(&dir.join("fullchain.pem"), &dir.join("key.pem"))
                .expect("PEM chain loads");
        // The client trusts only the CA: the handshake succeeds only if the
        // leaf from the file is served and verifies against its issuer.
        let (client, server) = handshake(config, ca_cert.der().to_vec());
        assert!(!client.is_handshaking() && !server.is_handshaking());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_der_cert_and_key_files() {
        let (cert_der, key_der) = generate_self_signed_cert(vec!["localhost".to_string()]).unwrap();
        let dir = temp_files("der", &[("cert.der", &cert_der), ("key.der", &key_der)]);
        let config = load_certs_and_key_from_files(&dir.join("cert.der"), &dir.join("key.der"))
            .expect("DER cert and key load");
        let (client, server) = handshake(config, cert_der);
        assert!(!client.is_handshaking() && !server.is_handshaking());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_cert_files_report_bad_input() {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let key_pem = key.serialize_pem();
        let dir = temp_files(
            "bad",
            &[
                ("cert.pem", cert.pem().as_bytes()),
                ("key.pem", key_pem.as_bytes()),
                // A PEM file holding only a key, given as the certificate.
                ("nocert.pem", key_pem.as_bytes()),
                ("nokey.pem", cert.pem().as_bytes()),
            ],
        );
        let load = |c: &str, k: &str| load_certs_and_key_from_files(&dir.join(c), &dir.join(k));
        let err = load("nocert.pem", "key.pem").unwrap_err();
        assert!(err.contains("certificate"), "{}", err);
        let err = load("cert.pem", "nokey.pem").unwrap_err();
        assert!(err.contains("private key"), "{}", err);
        let err = load("missing.pem", "key.pem").unwrap_err();
        assert!(err.contains("missing.pem"), "{}", err);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A client and server session that have completed a handshake in memory.
    fn connected_pair() -> (rustls::ClientConnection, ServerConnection) {
        let (cert_der, key_der) = generate_self_signed_cert(vec!["localhost".to_string()]).unwrap();
        let server_config = create_server_config(&cert_der, &key_der).unwrap();
        handshake(server_config, cert_der)
    }

    /// Completes an in-memory handshake against `server_config`, with a
    /// client that trusts only `trusted_cert_der` and connects to "localhost".
    fn handshake(
        server_config: Arc<ServerConfig>,
        trusted_cert_der: Vec<u8>,
    ) -> (rustls::ClientConnection, ServerConnection) {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from(trusted_cert_der)).unwrap();
        let client_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let mut client =
            rustls::ClientConnection::new(client_config, "localhost".try_into().unwrap()).unwrap();
        let mut server = ServerConnection::new(server_config).unwrap();
        while client.is_handshaking() || server.is_handshaking() {
            let mut c2s = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut c2s).unwrap();
            }
            let mut slice = &c2s[..];
            while !slice.is_empty() {
                server.read_tls(&mut slice).unwrap();
                server.process_new_packets().unwrap();
            }
            let mut s2c = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut s2c).unwrap();
            }
            let mut slice = &s2c[..];
            while !slice.is_empty() {
                client.read_tls(&mut slice).unwrap();
                client.process_new_packets().unwrap();
            }
        }
        // Lift the 64 KiB default on both sides, as `TlsTransport::new` does.
        client.set_buffer_limit(None);
        server.set_buffer_limit(None);
        (client, server)
    }

    #[test]
    fn decrypt_into_consumes_every_record_of_a_large_burst() {
        let (mut client, mut server) = connected_pair();
        // ~200 KiB of pipelined requests arriving in a single socket read:
        // many records, far more than one read_tls call or rustls' 16 KiB
        // plaintext buffer takes.
        let request: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        client.writer().write_all(&request).unwrap();
        let mut wire = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut wire).unwrap();
        }
        let mut buf = BytesMut::new();
        let (n, closed) = decrypt_into(&mut server, &wire, &mut buf).unwrap();
        assert_eq!(n, request.len());
        assert!(!closed);
        assert_eq!(&buf[..], &request[..]);
    }

    #[test]
    fn decrypt_into_handles_records_split_across_reads() {
        let (mut client, mut server) = connected_pair();
        client.writer().write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut wire = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut wire).unwrap();
        }
        let mut buf = BytesMut::new();
        let (first, last) = wire.split_at(wire.len() / 2);
        assert_eq!(
            decrypt_into(&mut server, first, &mut buf).unwrap(),
            (0, false)
        );
        assert_eq!(
            decrypt_into(&mut server, last, &mut buf).unwrap(),
            (14, false)
        );
        assert_eq!(&buf[..], b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn decrypt_into_reports_close_notify() {
        let (mut client, mut server) = connected_pair();
        client.writer().write_all(b"+last\r\n").unwrap();
        client.send_close_notify();
        let mut wire = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut wire).unwrap();
        }
        let mut buf = BytesMut::new();
        assert_eq!(
            decrypt_into(&mut server, &wire, &mut buf).unwrap(),
            (7, true)
        );
        assert_eq!(&buf[..], b"+last\r\n");
    }

    #[test]
    fn decrypt_into_rejects_garbage() {
        let (_client, mut server) = connected_pair();
        let mut buf = BytesMut::new();
        assert!(decrypt_into(&mut server, b"*1\r\n$4\r\nPING\r\n", &mut buf).is_err());
    }

    #[test]
    fn encrypt_into_takes_replies_larger_than_the_default_send_buffer() {
        let (mut client, mut server) = connected_pair();
        let reply: Vec<u8> = (0..300_000u32).map(|i| (i % 253) as u8).collect();
        let mut wire = Vec::new();
        encrypt_into(&mut server, &reply, &mut wire).unwrap();
        assert!(!server.wants_write(), "everything drained into wire");
        let mut got = Vec::new();
        let mut slice = &wire[..];
        while !slice.is_empty() {
            client.read_tls(&mut slice).unwrap();
            client.process_new_packets().unwrap();
            let mut chunk = [0u8; 16384];
            while let Ok(n) = client.reader().read(&mut chunk) {
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&chunk[..n]);
            }
        }
        assert_eq!(got, reply);
    }
}
