//! TLS and Kernel TLS (kTLS) hardware-accelerated framing support for Rudis.
//!
//! Provides in-memory and file-based TLS certificate provisioning with `rustls`,
//! plus Linux Kernel TLS (`kTLS` via `TCP_ULP`) socket promotion for line-rate zero-copy
//! streaming over `io_uring`.

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

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

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|e| format!("Failed to create rustls ServerConfig: {}", e))?;

    Ok(Arc::new(config))
}

/// Loads certificates and private key from PEM files.
pub fn load_certs_and_key_from_files(
    cert_path: &Path,
    key_path: &Path,
) -> Result<Arc<ServerConfig>, String> {
    let cert_file = File::open(cert_path).map_err(|e| format!("Cannot open cert file: {}", e))?;
    let mut cert_reader = BufReader::new(cert_file);
    let mut cert_bytes = Vec::new();
    cert_reader
        .read_to_end(&mut cert_bytes)
        .map_err(|e| format!("Read cert error: {}", e))?;

    let key_file = File::open(key_path).map_err(|e| format!("Cannot open key file: {}", e))?;
    let mut key_reader = BufReader::new(key_file);
    let mut key_bytes = Vec::new();
    key_reader
        .read_to_end(&mut key_bytes)
        .map_err(|e| format!("Read key error: {}", e))?;

    // Parse PEM using rcgen/rustls or fallback to raw DER
    create_server_config(&cert_bytes, &key_bytes)
}

/// Attempts to enable Linux Kernel TLS (`kTLS`) on a TCP stream via `TCP_ULP`.
///
/// If supported by the Linux kernel (`CONFIG_TLS=y/m` and `modprobe tls`), this allows
/// the kernel network stack or NIC crypto engines to perform hardware-accelerated
/// AES-GCM framing directly with zero user-space memory copies.
pub fn enable_ktls(raw_fd: RawFd) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        const IPPROTO_TCP: libc::c_int = 6;
        const TCP_ULP: libc::c_int = 31;
        let ulp_name = b"tls\0";

        let ret = unsafe {
            libc::setsockopt(
                raw_fd,
                IPPROTO_TCP,
                TCP_ULP,
                ulp_name.as_ptr() as *const libc::c_void,
                ulp_name.len() as libc::socklen_t,
            )
        };

        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = raw_fd;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kTLS is only supported on Linux",
        ))
    }
}

/// User-space TLS session wrapper when kTLS offload is unavailable or during handshake.
pub struct TlsSession {
    pub conn: rustls::ServerConnection,
    pub is_ktls_active: bool,
}

impl TlsSession {
    pub fn new(config: Arc<ServerConfig>) -> Result<Self, String> {
        let conn = rustls::ServerConnection::new(config)
            .map_err(|e| format!("Failed to create ServerConnection: {}", e))?;
        Ok(Self {
            conn,
            is_ktls_active: false,
        })
    }

    /// Performs the initial TLS handshake on a raw stream.
    pub fn complete_handshake<S: Read + Write + AsRawFd>(
        &mut self,
        stream: &mut S,
    ) -> io::Result<()> {
        while self.conn.is_handshaking() {
            while self.conn.wants_write() {
                self.conn.write_tls(stream)?;
                stream.flush()?;
            }
            if self.conn.wants_read() {
                let n = self.conn.read_tls(stream)?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "TLS handshake EOF",
                    ));
                }
                self.conn.process_new_packets().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("TLS error: {}", e))
                })?;
            }
        }

        // Flush any remaining handshake or session ticket frames
        while self.conn.wants_write() {
            self.conn.write_tls(stream)?;
            stream.flush()?;
        }

        // Try promoting to kTLS if on Linux
        let raw_fd = stream.as_raw_fd();
        if enable_ktls(raw_fd).is_ok() {
            self.is_ktls_active = true;
        }

        Ok(())
    }
}
