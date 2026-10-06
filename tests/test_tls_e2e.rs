//! End-to-end tests for clients on the TLS port.
//!
//! TLS clients must behave exactly like plaintext ones: same pipelining (with
//! cross-shard squashing), transactions, Pub/Sub push delivery, blocking
//! commands, CLIENT REPLY / CLIENT KILL, MONITOR and client-side-caching
//! invalidation pushes. The in-process tests share one 4-shard server (plain
//! port 17090, TLS port 17091) and each uses its own key prefix; the PEM
//! certificate test runs the real binary on ports 17093/17094.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use rudis::server::run_shard_worker;

const PORT: u16 = 17090;
const TLS_PORT: u16 = 17091;
const NUM_SHARDS: usize = 4;

/// Starts the shared server once and returns the client TLS config trusting it.
fn server() -> Arc<rustls::ClientConfig> {
    static CLIENT_CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CLIENT_CONFIG
        .get_or_init(|| {
            let (cert_der, key_der) = rudis::tls::generate_self_signed_cert(vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
            ])
            .expect("generate test cert");
            let server_config =
                rudis::tls::create_server_config(&cert_der, &key_der).expect("server config");
            let tls_config = rudis::tls::TlsWorkerConfig {
                tls_port: TLS_PORT,
                server_config,
            };
            let dir =
                std::env::temp_dir().join(format!("rudis-tls-e2e-{}-{}", std::process::id(), PORT));
            let _ = std::fs::create_dir_all(&dir);
            let aof_config = rudis::aof::AofConfig {
                enabled: false,
                dir,
                fsync_every_sec: false,
            };
            rudis::shutdown::reset_shutdown();
            let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(NUM_SHARDS);
            for (shard_id, rx) in receivers.into_iter().enumerate() {
                let shard_senders = senders_mesh[shard_id].clone();
                let shard_aof_config = aof_config.clone();
                let shard_tls_config = Some(tls_config.clone());
                thread::Builder::new()
                    .name(format!("tls-e2e-shard-{}", shard_id))
                    .stack_size(rudis::server::SHARD_THREAD_STACK_SIZE)
                    .spawn(move || {
                        run_shard_worker(
                            shard_id,
                            NUM_SHARDS,
                            PORT,
                            shard_senders,
                            rx,
                            None,
                            shard_aof_config,
                            shard_tls_config,
                            false,
                        );
                    })
                    .expect("spawn shard");
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                if TcpStream::connect(("127.0.0.1", PORT)).is_ok()
                    && TcpStream::connect(("127.0.0.1", TLS_PORT)).is_ok()
                {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }

            let mut roots = rustls::RootCertStore::empty();
            roots
                .add(rustls::pki_types::CertificateDer::from(cert_der))
                .expect("trust test cert");
            Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        })
        .clone()
}

/// A decoded RESP2/RESP3 reply.
#[derive(Debug, Clone, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
    Push(Vec<Reply>),
    Map(Vec<(Reply, Reply)>),
    Null,
}

impl Reply {
    fn bulk(s: &str) -> Reply {
        Reply::Bulk(Some(s.as_bytes().to_vec()))
    }
    fn ok() -> Reply {
        Reply::Simple("OK".to_string())
    }
}

fn read_reply<R: BufRead>(r: &mut R) -> std::io::Result<Reply> {
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line)?;
    if line.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connection closed",
        ));
    }
    assert!(line.ends_with(b"\r\n"), "bad line {:?}", line);
    let body = String::from_utf8_lossy(&line[1..line.len() - 2]).to_string();
    let count = || body.parse::<i64>().expect("length");
    Ok(match line[0] {
        b'+' => Reply::Simple(body),
        b'-' => Reply::Error(body),
        b':' => Reply::Int(count()),
        b'_' => Reply::Null,
        b'$' => {
            let n = count();
            if n < 0 {
                Reply::Bulk(None)
            } else {
                let mut data = vec![0u8; n as usize + 2];
                r.read_exact(&mut data)?;
                data.truncate(n as usize);
                Reply::Bulk(Some(data))
            }
        }
        b'*' | b'>' => {
            let n = count();
            if n < 0 {
                Reply::Array(None)
            } else {
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(read_reply(r)?);
                }
                if line[0] == b'>' {
                    Reply::Push(items)
                } else {
                    Reply::Array(Some(items))
                }
            }
        }
        b'%' => {
            let mut pairs = Vec::new();
            for _ in 0..count() {
                let k = read_reply(r)?;
                let v = read_reply(r)?;
                pairs.push((k, v));
            }
            Reply::Map(pairs)
        }
        other => panic!(
            "unexpected RESP type byte {:?} in {:?}",
            other as char, line
        ),
    })
}

type TlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

/// A TLS client speaking RESP.
struct TlsClient {
    reader: BufReader<TlsStream>,
}

impl TlsClient {
    fn connect() -> TlsClient {
        let config = server();
        let conn = rustls::ClientConnection::new(config, "localhost".try_into().unwrap())
            .expect("client connection");
        let sock = TcpStream::connect(("127.0.0.1", TLS_PORT)).expect("connect TLS port");
        sock.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        TlsClient {
            reader: BufReader::with_capacity(1 << 20, rustls::StreamOwned::new(conn, sock)),
        }
    }

    fn send_raw(&mut self, data: &[u8]) {
        let s = self.reader.get_mut();
        s.write_all(data).unwrap();
        s.flush().unwrap();
    }

    fn send(&mut self, args: &[&[u8]]) {
        self.send_raw(&encode(args));
    }

    fn read(&mut self) -> Reply {
        read_reply(&mut self.reader).expect("read reply")
    }

    fn cmd(&mut self, args: &[&[u8]]) -> Reply {
        self.send(args);
        self.read()
    }

    fn set_timeout(&mut self, d: Duration) {
        self.reader
            .get_ref()
            .sock
            .set_read_timeout(Some(d))
            .unwrap();
    }
}

/// A plaintext RESP client, for driving the other side of a scenario.
struct PlainClient {
    reader: BufReader<TcpStream>,
}

impl PlainClient {
    fn connect() -> PlainClient {
        server();
        let sock = TcpStream::connect(("127.0.0.1", PORT)).expect("connect plain port");
        sock.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        PlainClient {
            reader: BufReader::new(sock),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> Reply {
        self.reader.get_mut().write_all(&encode(args)).unwrap();
        read_reply(&mut self.reader).expect("read reply")
    }
}

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

#[test]
fn test_tls_deep_cross_shard_pipeline_replies_in_order() {
    let mut c = TlsClient::connect();
    // One write of ~2000 mixed commands over ~100 keys spread across all 4
    // shards: far more than one TLS record (16 KiB) and one read.
    let mut payload = Vec::new();
    let mut expected = Vec::new();
    let mut model: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for i in 0..2000usize {
        let key = format!("tls:pipe:{}", i % 97);
        match i % 4 {
            0 => {
                payload.extend(encode(&[b"SET", key.as_bytes(), i.to_string().as_bytes()]));
                model.insert(key, i as i64);
                expected.push(Reply::ok());
            }
            1 => {
                payload.extend(encode(&[b"GET", key.as_bytes()]));
                expected.push(match model.get(&key) {
                    Some(v) => Reply::bulk(&v.to_string()),
                    None => Reply::Bulk(None),
                });
            }
            2 => {
                payload.extend(encode(&[b"INCR", key.as_bytes()]));
                let v = model.entry(key).or_insert(0);
                *v += 1;
                expected.push(Reply::Int(*v));
            }
            _ => {
                payload.extend(encode(&[b"DEL", key.as_bytes()]));
                let existed = model.remove(&key).is_some();
                expected.push(Reply::Int(existed as i64));
            }
        }
    }
    assert!(payload.len() > 64 * 1024, "pipeline must span many records");
    c.send_raw(&payload);
    for (i, want) in expected.iter().enumerate() {
        assert_eq!(&c.read(), want, "reply #{} out of order or wrong", i);
    }
    // The connection is still healthy afterwards.
    assert_eq!(c.cmd(&[b"PING"]), Reply::Simple("PONG".into()));
}

#[test]
fn test_tls_large_value_round_trip() {
    let mut c = TlsClient::connect();
    // Bigger than rustls' 64 KiB default send buffer and its 16 KiB records.
    let value: Vec<u8> = (0..300_000u32).map(|i| b'a' + (i % 26) as u8).collect();
    assert_eq!(c.cmd(&[b"SET", b"tls:big", &value]), Reply::ok());
    assert_eq!(c.cmd(&[b"GET", b"tls:big"]), Reply::Bulk(Some(value)));
}

#[test]
fn test_tls_multi_exec_across_shards() {
    let mut c = TlsClient::connect();
    assert_eq!(c.cmd(&[b"MULTI"]), Reply::ok());
    let queued = Reply::Simple("QUEUED".into());
    assert_eq!(c.cmd(&[b"SET", b"tls:tx:a", b"1"]), queued);
    assert_eq!(c.cmd(&[b"INCR", b"tls:tx:b"]), queued);
    assert_eq!(c.cmd(&[b"GET", b"tls:tx:a"]), queued);
    assert_eq!(
        c.cmd(&[b"EXEC"]),
        Reply::Array(Some(vec![Reply::ok(), Reply::Int(1), Reply::bulk("1")]))
    );
    // A pipelined transaction in a single write.
    let mut p = encode(&[b"MULTI"]);
    p.extend(encode(&[b"INCR", b"tls:tx:b"]));
    p.extend(encode(&[b"INCR", b"tls:tx:c"]));
    p.extend(encode(&[b"EXEC"]));
    c.send_raw(&p);
    assert_eq!(c.read(), Reply::ok());
    assert_eq!(c.read(), queued);
    assert_eq!(c.read(), queued);
    assert_eq!(
        c.read(),
        Reply::Array(Some(vec![Reply::Int(2), Reply::Int(1)]))
    );
}

#[test]
fn test_tls_subscribe_receives_published_messages() {
    let mut sub = TlsClient::connect();
    assert_eq!(
        sub.cmd(&[b"SUBSCRIBE", b"tls:chan"]),
        Reply::Array(Some(vec![
            Reply::bulk("subscribe"),
            Reply::bulk("tls:chan"),
            Reply::Int(1)
        ]))
    );
    let mut plain = PlainClient::connect();
    assert_eq!(
        plain.cmd(&[b"PUBLISH", b"tls:chan", b"hello"]),
        Reply::Int(1)
    );
    assert_eq!(
        sub.read(),
        Reply::Array(Some(vec![
            Reply::bulk("message"),
            Reply::bulk("tls:chan"),
            Reply::bulk("hello")
        ]))
    );
    // And from another TLS client.
    let mut publisher = TlsClient::connect();
    assert_eq!(
        publisher.cmd(&[b"PUBLISH", b"tls:chan", b"again"]),
        Reply::Int(1)
    );
    assert_eq!(
        sub.read(),
        Reply::Array(Some(vec![
            Reply::bulk("message"),
            Reply::bulk("tls:chan"),
            Reply::bulk("again")
        ]))
    );
    // Commands outside the pub/sub set are refused in RESP2 subscribe mode.
    match sub.cmd(&[b"GET", b"x"]) {
        Reply::Error(e) => assert!(e.contains("only (P|S)SUBSCRIBE"), "{}", e),
        other => panic!("expected error, got {:?}", other),
    }
}

#[test]
fn test_tls_blpop_woken_by_lpush_from_another_client() {
    let mut blocked = TlsClient::connect();
    blocked.send(&[b"BLPOP", b"tls:list", b"5"]);
    thread::sleep(Duration::from_millis(200));
    let mut plain = PlainClient::connect();
    assert_eq!(plain.cmd(&[b"LPUSH", b"tls:list", b"v1"]), Reply::Int(1));
    assert_eq!(
        blocked.read(),
        Reply::Array(Some(vec![Reply::bulk("tls:list"), Reply::bulk("v1")]))
    );
}

#[test]
fn test_tls_client_reply_off_skip() {
    let mut c = TlsClient::connect();
    c.send(&[b"CLIENT", b"REPLY", b"OFF"]);
    c.send(&[b"SET", b"tls:reply", b"1"]);
    c.send(&[b"INCR", b"tls:reply"]);
    c.send(&[b"CLIENT", b"REPLY", b"SKIP"]);
    c.send(&[b"INCR", b"tls:reply"]);
    assert_eq!(c.cmd(&[b"CLIENT", b"REPLY", b"ON"]), Reply::ok());
    c.send(&[b"CLIENT", b"REPLY", b"SKIP"]);
    c.send(&[b"INCR", b"tls:reply"]);
    // Only this reply comes back: 1, +1, +1 (skipped), +1 (skipped), +1.
    assert_eq!(c.cmd(&[b"INCR", b"tls:reply"]), Reply::Int(5));
}

#[test]
fn test_tls_client_kill_by_id() {
    let mut victim = TlsClient::connect();
    let id = match victim.cmd(&[b"CLIENT", b"ID"]) {
        Reply::Int(id) => id,
        other => panic!("CLIENT ID: {:?}", other),
    };
    assert_eq!(
        victim.cmd(&[b"CLIENT", b"SETNAME", b"tls-victim"]),
        Reply::ok()
    );
    let mut admin = PlainClient::connect();
    match admin.cmd(&[b"CLIENT", b"LIST"]) {
        Reply::Bulk(Some(list)) => {
            let list = String::from_utf8(list).unwrap();
            assert!(
                list.lines()
                    .any(|l| l.contains(&format!("id={} ", id)) && l.contains("name=tls-victim")),
                "TLS client missing from CLIENT LIST:\n{}",
                list
            );
        }
        other => panic!("CLIENT LIST: {:?}", other),
    }
    assert_eq!(
        admin.cmd(&[b"CLIENT", b"KILL", b"ID", id.to_string().as_bytes()]),
        Reply::Int(1)
    );
    victim.set_timeout(Duration::from_secs(5));
    victim.send(&[b"PING"]);
    assert!(
        read_reply(&mut victim.reader).is_err(),
        "killed TLS client must be disconnected"
    );
}

#[test]
fn test_tls_monitor_streams_commands_encrypted() {
    let mut mon = TlsClient::connect();
    assert_eq!(mon.cmd(&[b"MONITOR"]), Reply::ok());
    let mut plain = PlainClient::connect();
    assert_eq!(plain.cmd(&[b"SET", b"tls:monitored", b"seen"]), Reply::ok());
    // Other tests share the server: skip lines until ours arrives. A
    // plaintext line injected into the TLS stream would fail decryption here.
    loop {
        match mon.read() {
            Reply::Simple(line) if line.contains("\"tls:monitored\"") => {
                assert!(
                    line.contains("\"set\"") || line.contains("\"SET\""),
                    "{}",
                    line
                );
                break;
            }
            Reply::Simple(_) => continue,
            other => panic!("unexpected MONITOR output {:?}", other),
        }
    }
}

#[test]
fn test_tls_client_tracking_invalidation_push() {
    let mut c = TlsClient::connect();
    match c.cmd(&[b"HELLO", b"3"]) {
        Reply::Map(_) => {}
        other => panic!("HELLO 3: {:?}", other),
    }
    assert_eq!(c.cmd(&[b"CLIENT", b"TRACKING", b"ON"]), Reply::ok());
    let miss = c.cmd(&[b"GET", b"tls:tracked"]);
    assert!(
        matches!(miss, Reply::Null | Reply::Bulk(None)),
        "GET of a missing key: {:?}",
        miss
    );
    let mut plain = PlainClient::connect();
    assert_eq!(plain.cmd(&[b"SET", b"tls:tracked", b"v"]), Reply::ok());
    // The invalidation arrives while the TLS client is idle.
    assert_eq!(
        c.read(),
        Reply::Push(vec![
            Reply::bulk("invalidate"),
            Reply::Array(Some(vec![Reply::bulk("tls:tracked")]))
        ])
    );
}

#[test]
fn test_binary_serves_tls_with_pem_cert_files() {
    // The real binary with --tls-cert-file/--tls-key-file in PEM, the format
    // openssl and certbot write.
    const BIN_PORT: u16 = 17093;
    const BIN_TLS_PORT: u16 = 17094;
    let dir = std::env::temp_dir().join(format!("rudis-tls-e2e-pem-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let (cert_path, key_path) = (dir.join("rudis.crt"), dir.join("rudis.key"));
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();

    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = KillOnDrop(
        std::process::Command::new(env!("CARGO_BIN_EXE_rudis"))
            // Keeps the default tier dir (under the working dir) out of the repo.
            .current_dir(std::env::temp_dir())
            .args([
                "--port",
                &BIN_PORT.to_string(),
                "--threads",
                "2",
                "--no-pin",
            ])
            .args(["--tls-port", &BIN_TLS_PORT.to_string()])
            .arg("--tls-cert-file")
            .arg(&cert_path)
            .arg("--tls-key-file")
            .arg(&key_path)
            .current_dir(&dir)
            .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn rudis"),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let sock = loop {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", BIN_TLS_PORT)) {
            break s;
        }
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!("rudis exited before listening: {}", status);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "TLS port never opened"
        );
        thread::sleep(Duration::from_millis(50));
    };
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let config = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let conn = rustls::ClientConnection::new(config, "localhost".try_into().unwrap()).unwrap();
    let mut reader = BufReader::new(rustls::StreamOwned::new(conn, sock));
    reader.get_mut().write_all(&encode(&[b"PING"])).unwrap();
    assert_eq!(
        read_reply(&mut reader).expect("PING over TLS"),
        Reply::Simple("PONG".into())
    );
    drop(reader);

    let mut plain = TcpStream::connect(("127.0.0.1", BIN_PORT)).unwrap();
    let _ = plain.write_all(&encode(&[b"SHUTDOWN", b"NOSAVE"]));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while child.0.try_wait().unwrap().is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "rudis did not shut down"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Driver for spawned server binaries: inherits `MONOIO_FORCE_LEGACY_DRIVER`
/// from the test process (CI runs one job per driver) and defaults to the
/// legacy driver, which avoids io_uring memlock limits on old kernels.
fn legacy_driver_env() -> String {
    std::env::var("MONOIO_FORCE_LEGACY_DRIVER").unwrap_or_else(|_| "1".to_string())
}
