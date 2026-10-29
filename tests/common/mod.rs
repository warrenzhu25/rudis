//! Helpers shared by the e2e test binaries (split from the old
//! single `test_server_e2e.rs`). Each binary uses a subset.
#![allow(dead_code, unused_imports, unused_macros)]

use rudis::router::target_shard;
use rudis::server::run_shard_worker;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

/// Removes the `rudis-{srv,cluster,tls}-<pid>-<port>` dirs of test processes
/// that are gone.
/// In-process servers never shut down, so their dirs (RDB, AOF, tier files)
/// cannot be removed by the test that made them; the next run does it.
pub fn sweep_dead_test_server_dirs() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
            return;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let pid = ["rudis-srv-", "rudis-cluster-", "rudis-tls-"]
                .iter()
                .find_map(|prefix| name.strip_prefix(prefix))
                .and_then(|rest| rest.split('-').next())
                .and_then(|pid| pid.parse::<u32>().ok());
            if let Some(pid) = pid
                && !std::path::Path::new(&format!("/proc/{pid}")).exists()
            {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
    });
}

/// The server binary, run from its own scratch dir (swept like the others)
/// so files it puts in its working dir, such as the default tier dir or the
/// `dump.rdb` its default save points write on shutdown, stay out of the
/// repo and out of the next spawned server's way.
/// Appended to spawned servers' arguments: the binary defaults to
/// `enable-debug-command no`, as Redis does, and many tests use DEBUG.
pub const DEBUG_ON: [&str; 2] = ["--enable-debug-command", "yes"];

pub fn rudis_bin() -> std::process::Command {
    sweep_dead_test_server_dirs();
    static SPAWNED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = SPAWNED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let cwd = std::env::temp_dir().join(format!("rudis-srv-{}-cwd{}", std::process::id(), n));
    std::fs::create_dir_all(&cwd).unwrap();
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rudis"));
    cmd.current_dir(cwd);
    cmd
}

pub fn start_test_server(port: u16, num_shards: usize) {
    sweep_dead_test_server_dirs();
    let dir = std::env::temp_dir().join(format!("rudis-srv-{}-{}", std::process::id(), port));
    let _ = std::fs::create_dir_all(&dir);
    start_test_server_with_aof(
        port,
        num_shards,
        rudis::aof::AofConfig {
            enabled: false,
            dir,
            fsync_every_sec: false,
        },
    );
}

pub fn start_test_server_with_aof(port: u16, num_shards: usize, aof_config: rudis::aof::AofConfig) {
    rudis::shutdown::reset_shutdown();
    // These in-process servers also cover the experimental families; the
    // default (off) is tested against the real binary.
    rudis::resp::set_experimental_commands(true);
    // The real binary defaults to `no`, as Redis; tests use DEBUG.
    rudis::netsec::set_enable_debug_command(port, rudis::netsec::ProtectedAction::Yes);
    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
        let shard_aof_config = aof_config.clone();
        thread::Builder::new()
            .name(format!("test-shard-{}", shard_id))
            .stack_size(rudis::server::SHARD_THREAD_STACK_SIZE)
            .spawn(move || {
                run_shard_worker(
                    shard_id,
                    num_shards,
                    port,
                    shard_senders,
                    rx,
                    None,
                    shard_aof_config,
                    None,
                    false,
                );
            })
            .expect("Failed to spawn test shard");
    }

    // Wait until server is listening and accepting connections
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

pub fn start_test_server_cluster(port: u16, num_shards: usize) {
    rudis::shutdown::reset_shutdown();
    let hub = rudis::cluster::get_cluster_hub(port);
    hub.cluster_enabled
        .store(true, std::sync::atomic::Ordering::Release);
    hub.num_shards
        .store(num_shards, std::sync::atomic::Ordering::Release);

    sweep_dead_test_server_dirs();
    let dir = std::env::temp_dir().join(format!("rudis-cluster-{}-{}", std::process::id(), port));
    let _ = std::fs::create_dir_all(&dir);
    let aof_config = rudis::aof::AofConfig {
        enabled: false,
        dir,
        fsync_every_sec: false,
    };

    // The real binary defaults to `no`, as Redis; tests use DEBUG.

    rudis::netsec::set_enable_debug_command(port, rudis::netsec::ProtectedAction::Yes);

    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
        let shard_aof_config = aof_config.clone();
        thread::Builder::new()
            .name(format!("test-cluster-shard-{}", shard_id))
            .stack_size(rudis::server::SHARD_THREAD_STACK_SIZE)
            .spawn(move || {
                run_shard_worker(
                    shard_id,
                    num_shards,
                    port,
                    shard_senders,
                    rx,
                    None,
                    shard_aof_config,
                    None,
                    true,
                );
            })
            .expect("Failed to spawn test cluster shard");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

pub fn start_test_server_with_tls(
    port: u16,
    tls_port: u16,
    num_shards: usize,
) -> (Vec<u8>, Vec<u8>) {
    rudis::shutdown::reset_shutdown();
    let (cert_der, key_der) = rudis::tls::generate_self_signed_cert(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("Failed to generate test self-signed cert");

    let server_config = rudis::tls::create_server_config(&cert_der, &key_der)
        .expect("Failed to build rustls ServerConfig");

    let tls_config = rudis::tls::TlsWorkerConfig {
        tls_port,
        server_config,
    };

    sweep_dead_test_server_dirs();
    let dir = std::env::temp_dir().join(format!("rudis-tls-{}-{}", std::process::id(), port));
    let _ = std::fs::create_dir_all(&dir);
    let aof_config = rudis::aof::AofConfig {
        enabled: false,
        dir,
        fsync_every_sec: false,
    };

    // The real binary defaults to `no`, as Redis; tests use DEBUG.

    rudis::netsec::set_enable_debug_command(port, rudis::netsec::ProtectedAction::Yes);

    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
        let shard_aof_config = aof_config.clone();
        let shard_tls_config = Some(tls_config.clone());
        thread::Builder::new()
            .name(format!("test-tls-shard-{}", shard_id))
            .stack_size(rudis::server::SHARD_THREAD_STACK_SIZE)
            .spawn(move || {
                run_shard_worker(
                    shard_id,
                    num_shards,
                    port,
                    shard_senders,
                    rx,
                    None,
                    shard_aof_config,
                    shard_tls_config,
                    false,
                );
            })
            .expect("Failed to spawn test shard");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok()
            && TcpStream::connect(format!("127.0.0.1:{}", tls_port)).is_ok()
        {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    (cert_der, key_der)
}

pub fn send_and_read(stream: &mut TcpStream, cmd: &[u8]) -> String {
    stream.write_all(cmd).unwrap();
    let mut buf = [0u8; 16384];
    let n = stream.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

/// End of the complete RESP value starting at `at`, or `None` if `buf` holds
/// only part of it.
pub fn resp_value_end(buf: &[u8], at: usize) -> Option<usize> {
    let nl = at + buf.get(at..)?.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&buf[at + 1..nl]).ok()?;
    match buf[at] {
        b'$' => {
            let n: i64 = line.parse().ok()?;
            if n < 0 {
                return Some(nl + 2);
            }
            let end = nl + 2 + n as usize + 2;
            (buf.len() >= end).then_some(end)
        }
        b'*' | b'%' | b'~' => {
            let n: i64 = line.parse().ok()?;
            let items = if buf[at] == b'%' { 2 * n } else { n };
            let mut pos = nl + 2;
            for _ in 0..items.max(0) {
                pos = resp_value_end(buf, pos)?;
            }
            Some(pos)
        }
        _ => Some(nl + 2),
    }
}

/// Like [`send_and_read`], but keeps reading until one complete RESP reply
/// has arrived (a large reply can span several reads under load).
pub fn send_and_read_reply(stream: &mut TcpStream, cmd: &[u8]) -> String {
    stream.write_all(cmd).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 16384];
    while resp_value_end(&got, 0).is_none() {
        let n = stream.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed mid-reply");
        got.extend_from_slice(&buf[..n]);
    }
    String::from_utf8_lossy(&got).to_string()
}

pub fn send_and_read_bytes(stream: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
    stream.write_all(cmd).unwrap();
    let mut buf = [0u8; 16384];
    let n = stream.read(&mut buf).unwrap();
    buf[..n].to_vec()
}

/// Whether a CLIENT LIST reply has a client with exactly this id. A substring
/// check is not enough: ids carry the shard in their high bits, so shard 0's
/// `id=2` is a prefix of shard 1's `id=281474976710657`.
pub fn client_list_has_id(list: &str, id: u64) -> bool {
    let field = format!("id={}", id);
    list.lines()
        .any(|line| line.split(' ').next() == Some(field.as_str()))
}

/// CLUSTER REPLICATE starts a real sync: wait until it is done (link up,
/// dataset loaded) before acting on the replica, which answers -LOADING
/// meanwhile.
pub fn wait_for_master_link(c: &mut TcpStream) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let info = send_and_read(c, b"INFO replication\r\n");
        if info.contains("master_link_status:up") && !info.contains("master_sync_in_progress:1") {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica never synced: {info}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// Reads the per-shard connection census from `INFO clients`.
///
/// This does not use `send_and_read` on purpose: that helper performs a single
/// 1KB `read()`, while an INFO reply spans several KB and several TCP segments.
/// A truncated read leaves the remainder queued in the socket, so a subsequent
/// command on the same connection would observe the stale tail of this reply.
/// Here we open a fresh connection, request only the `clients` section, and
/// drain until the RESP bulk string (`$<len>\r\n<payload>\r\n`) is complete.
pub fn read_shard_census(port: u16) -> Vec<usize> {
    let mut s = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(b"INFO clients\r\n").unwrap();

    let mut data = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0, "server closed before the full INFO reply arrived");
        data.extend_from_slice(&chunk[..n]);
        if let Some(hdr_end) = data.windows(2).position(|w| w == b"\r\n") {
            assert_eq!(data[0], b'$', "expected a RESP bulk string from INFO");
            let len: usize = std::str::from_utf8(&data[1..hdr_end])
                .unwrap()
                .parse()
                .unwrap();
            if data.len() >= hdr_end + 2 + len + 2 {
                break;
            }
        }
    }

    let text = String::from_utf8_lossy(&data).to_string();
    let line = text
        .lines()
        .find(|l| l.starts_with("shard_connections:"))
        .unwrap_or_else(|| panic!("INFO clients missing shard_connections. Got:\n{}", text));
    line.trim_start_matches("shard_connections:")
        .trim()
        .split(',')
        .map(|v| v.parse::<usize>().unwrap())
        .collect()
}

pub fn format_resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
    }
    out
}

pub fn format_resp_bytes(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Sends one RESP-encoded command and returns the raw reply.
pub fn resp_cmd(stream: &mut TcpStream, args: &[&str]) -> String {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    send_and_read(stream, out.as_bytes())
}

/// A local, non-loopback source address (no packets are sent), if the host has one.
pub fn local_non_loopback_ip() -> Option<std::net::IpAddr> {
    let s = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("192.0.2.1:9").ok()?;
    let ip = s.local_addr().ok()?.ip();
    if ip.is_loopback() || ip.is_unspecified() {
        None
    } else {
        Some(ip)
    }
}

/// Connects from a non-loopback address, sends PING, returns everything read until EOF/timeout.
pub fn external_ping(ip: std::net::IpAddr, port: u16) -> String {
    let mut s = TcpStream::connect((ip, port)).expect("connect via external interface");
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let _ = s.write_all(b"PING\r\n");
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                out.extend_from_slice(&buf[..n]);
                if out.ends_with(b"\r\n") && !out.starts_with(b"-DENIED") {
                    break;
                }
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

/// Runs the real binary until it exits (or fails the test after 20s).
pub fn run_rudis_until_exit(args: &[&str]) -> (std::process::ExitStatus, String) {
    let mut child = rudis_bin()
        .args(args)
        .args(DEBUG_ON)
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rudis");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("rudis kept running despite unloadable data files");
        }
        thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    (status, stderr)
}

/// Starts the real binary and waits until it accepts connections.
/// A spawned server killed when dropped, so a failing test does not leave it
/// running (holding its port for the next run).
pub struct ServerGuard(pub std::process::Child);

impl std::ops::Deref for ServerGuard {
    type Target = std::process::Child;
    fn deref(&self) -> &std::process::Child {
        &self.0
    }
}

impl std::ops::DerefMut for ServerGuard {
    fn deref_mut(&mut self) -> &mut std::process::Child {
        &mut self.0
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub fn spawn_rudis_listening(args: &[&str], port: u16) -> std::process::Child {
    spawn_rudis_listening_driver(args, port, &legacy_driver_env())
}

/// [`spawn_rudis_listening`] with `MONOIO_FORCE_LEGACY_DRIVER` set to
/// `legacy` (`"0"` runs the io_uring driver production uses).
pub fn spawn_rudis_listening_driver(args: &[&str], port: u16, legacy: &str) -> std::process::Child {
    let mut child = rudis_bin()
        .args(args)
        .args(DEBUG_ON)
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rudis");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    // A bare connect is not enough: right after a previous instance on the
    // same port was SIGKILLed, the kernel may still hold its io_uring
    // listener, which completes handshakes but never answers. Any reply
    // (`+PONG`, `-NOAUTH`, `-LOADING`, ...) comes from a live server.
    let ready = || -> bool {
        let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) else {
            return false;
        };
        let _ = s.set_read_timeout(Some(Duration::from_millis(500)));
        let mut buf = [0u8; 64];
        s.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok()
            && matches!(s.read(&mut buf), Ok(n) if n > 0 && matches!(buf[0], b'+' | b'-'))
    };
    while !ready() {
        if let Some(s) = child.try_wait().unwrap() {
            panic!("rudis exited early: {s}");
        }
        assert!(std::time::Instant::now() < deadline, "rudis did not start");
        thread::sleep(Duration::from_millis(50));
    }
    child
}

pub fn shutdown_and_wait(port: u16, child: &mut std::process::Child) {
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let _ = c.write_all(b"*1\r\n$8\r\nSHUTDOWN\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("rudis did not shut down");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub fn wait_exit(
    child: &mut std::process::Child,
    within: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        if let Some(s) = child.try_wait().unwrap() {
            return Some(s);
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

pub fn info_field(c: &mut TcpStream, field: &str) -> String {
    let info = resp_cmd(c, &["INFO", "persistence"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{}:", field)))
        .unwrap_or_else(|| panic!("{field} missing in {info}"))
        .trim()
        .to_string()
}

/// Subscribes to every keyevent channel, runs `cmds` one at a time and
/// returns the (event, key) pairs published for them.
pub fn keyevents_for(port: u16, setup: &[&[&str]], cmds: &[&[&str]]) -> Vec<(String, String)> {
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        "+OK\r\n"
    );
    resp_cmd(&mut c, &["FLUSHALL"]);
    for cmd in setup {
        resp_cmd(&mut c, cmd);
    }
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_millis(400)))
        .unwrap();
    let r = resp_cmd(&mut sub, &["PSUBSCRIBE", "__keyevent@0__:*"]);
    assert!(r.contains("psubscribe"), "{r}");
    for cmd in cmds {
        let r = resp_cmd(&mut c, cmd);
        assert!(!r.starts_with('-'), "{cmd:?}: {r}");
    }
    let mut raw = Vec::new();
    let mut buf = [0u8; 65536];
    while let Ok(n) = sub.read(&mut buf) {
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&raw).to_string();
    let parts: Vec<&str> = text.split("\r\n").collect();
    let mut events = Vec::new();
    let mut i = 0;
    while i + 8 < parts.len() {
        if parts[i] == "*4" && parts[i + 2] == "pmessage" {
            let event = parts[i + 6].trim_start_matches("__keyevent@0__:");
            events.push((event.to_string(), parts[i + 8].to_string()));
            i += 9;
        } else {
            i += 1;
        }
    }
    events.sort();
    events
}

pub fn sorted_events(list: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = list
        .iter()
        .map(|(e, k)| (e.to_string(), k.to_string()))
        .collect();
    v.sort();
    v
}

/// Sends `INFO <section>` and returns the whole bulk reply.
pub fn info_section(c: &mut TcpStream, section: &str) -> String {
    let req = format!("*2\r\n$4\r\nINFO\r\n${}\r\n{}\r\n", section.len(), section);
    c.write_all(req.as_bytes()).unwrap();
    let mut info = Vec::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        info.extend_from_slice(&buf[..n]);
        if let Some(hdr_end) = info.windows(2).position(|w| w == b"\r\n") {
            let len: usize = std::str::from_utf8(&info[1..hdr_end])
                .unwrap()
                .parse()
                .unwrap();
            if info.len() >= hdr_end + 2 + len + 2 {
                return String::from_utf8(info).unwrap();
            }
        }
    }
}

/// Returns the `field=` value of `cmdstat_<cmd>` in an INFO commandstats reply.
pub fn cmdstat_field(info: &str, cmd: &str, field: &str) -> f64 {
    let prefix = format!("cmdstat_{cmd}:");
    let line = info
        .lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("no {prefix} in:\n{info}"));
    line.split(',')
        .find_map(|kv| kv.strip_prefix(&format!("{field}=")))
        .unwrap_or_else(|| panic!("no {field} in {line}"))
        .parse()
        .unwrap()
}

/// Sends `args` followed by PING and returns everything before the PONG.
pub fn resp_cmd_full(c: &mut TcpStream, args: &[&str]) -> String {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    out.push_str("*1\r\n$4\r\nPING\r\n");
    c.write_all(out.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    got.truncate(got.len() - b"+PONG\r\n".len());
    String::from_utf8(got).unwrap()
}

/// Returns the (bucket_usec, cumulative_count) pairs of a single-command
/// LATENCY HISTOGRAM reply.
pub fn histogram_pairs(reply: &str) -> Vec<(u64, u64)> {
    let (_, tail) = reply
        .split_once("histogram_usec\r\n")
        .unwrap_or_else(|| panic!("no histogram_usec in {reply:?}"));
    let nums: Vec<u64> = tail
        .lines()
        .filter_map(|l| l.strip_prefix(':'))
        .map(|n| n.parse().unwrap())
        .collect();
    nums.chunks(2).map(|p| (p[0], p[1])).collect()
}

/// Returns the value of `field:` in an INFO reply.
pub fn info_num(info: &str, field: &str) -> f64 {
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("no {field} in:\n{info}"))
        .trim()
        .parse()
        .unwrap()
}

/// Writes to all four probabilistic families, optionally rewrites the AOF,
/// restarts without an RDB and checks every query answers the same.
pub fn probabilistic_writes_survive_restart(port: u16, rewrite: bool) {
    let port_s = port.to_string();
    let dir = std::env::temp_dir().join(format!(
        "rudis-prob-aof-e2e-{}-{}",
        port,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir_s,
        "--enable-experimental-commands",
        "yes",
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for cmd in [
        &["BF.RESERVE", "pb:bf", "0.001", "100"][..],
        &["BF.ADD", "pb:bf", "a"],
        &["BF.MADD", "pb:bf", "b", "c"],
        &["BF.ADD", "pb:bf2", "x"],
        &["CF.RESERVE", "pb:cf", "64"],
        &["CF.ADD", "pb:cf", "a"],
        &["CF.ADD", "pb:cf", "b"],
        &["CF.ADDNX", "pb:cf", "c"],
        &["CF.DEL", "pb:cf", "b"],
        &["CMS.INITBYDIM", "pb:cms", "100", "4"],
        &["CMS.INCRBY", "pb:cms", "a", "5", "b", "7"],
        &["CMS.INITBYPROB", "pb:cms2", "0.01", "0.99"],
        &["CMS.INCRBY", "pb:cms2", "a", "3"],
        &["TOPK.RESERVE", "pb:tk", "3"],
        // Ties: which item gets evicted must not depend on the process.
        &["TOPK.ADD", "pb:tk", "c", "a", "b", "d", "e", "a", "f"],
    ] {
        let reply = resp_cmd(&mut c, cmd);
        assert!(!reply.starts_with('-'), "{cmd:?}: {reply}");
    }
    // Overfill a small cuckoo filter; the failed inserts change it too.
    assert_eq!(resp_cmd(&mut c, &["CF.RESERVE", "pb:full", "4"]), "+OK\r\n");
    let mut failed = 0;
    for i in 0..40 {
        let item = format!("i{i}");
        failed += resp_cmd(&mut c, &["CF.ADD", "pb:full", &item]).starts_with('-') as usize;
    }
    assert!(failed > 0, "pb:full never filled up");

    let queries: Vec<Vec<String>> = {
        let mut q: Vec<Vec<String>> = vec![
            vec![
                "BF.MEXISTS".into(),
                "pb:bf".into(),
                "a".into(),
                "b".into(),
                "c".into(),
                "z".into(),
            ],
            vec!["BF.EXISTS".into(), "pb:bf2".into(), "x".into()],
            vec!["BF.INFO".into(), "pb:bf".into()],
            vec!["CF.EXISTS".into(), "pb:cf".into(), "a".into()],
            vec!["CF.EXISTS".into(), "pb:cf".into(), "b".into()],
            vec!["CF.EXISTS".into(), "pb:cf".into(), "c".into()],
            vec!["CF.INFO".into(), "pb:cf".into()],
            vec!["CF.INFO".into(), "pb:full".into()],
            vec![
                "CMS.QUERY".into(),
                "pb:cms".into(),
                "a".into(),
                "b".into(),
                "z".into(),
            ],
            vec!["CMS.INFO".into(), "pb:cms".into()],
            vec!["CMS.QUERY".into(), "pb:cms2".into(), "a".into()],
            vec!["CMS.INFO".into(), "pb:cms2".into()],
            vec![
                "TOPK.QUERY".into(),
                "pb:tk".into(),
                "a".into(),
                "b".into(),
                "c".into(),
                "f".into(),
            ],
            vec!["TOPK.LIST".into(), "pb:tk".into()],
            vec!["TOPK.INFO".into(), "pb:tk".into()],
        ];
        q.extend((0..40).map(|i| vec!["CF.EXISTS".into(), "pb:full".into(), format!("i{i}")]));
        q
    };
    let ask = |c: &mut TcpStream| -> Vec<String> {
        queries
            .iter()
            .map(|q| {
                let q: Vec<&str> = q.iter().map(String::as_str).collect();
                resp_cmd(c, &q)
            })
            .collect()
    };
    let mut before = ask(&mut c);
    assert_eq!(before[0], "*4\r\n:1\r\n:1\r\n:1\r\n:0\r\n");
    assert_eq!(before[8], "*3\r\n:5\r\n:7\r\n:0\r\n");

    if rewrite {
        assert!(resp_cmd(&mut c, &["BGREWRITEAOF"]).starts_with('+'));
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while info_field(&mut c, "aof_rewrite_in_progress") != "0" {
            assert!(
                std::time::Instant::now() < deadline,
                "rewrite never finished"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(info_field(&mut c, "aof_last_bgrewrite_status"), "ok");
        // Writes after the rewrite are appended to the rewritten file.
        assert_eq!(
            resp_cmd(&mut c, &["CMS.INCRBY", "pb:cms", "a", "1"]),
            "*1\r\n:6\r\n"
        );
        before = ask(&mut c);
    }
    drop(c);
    {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
            .unwrap();
    }
    let _ = child.wait();
    // Nothing but the AOF can bring the structures back.
    assert!(!dir.join("dump.rdb").exists());

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let after = ask(&mut c);
    for ((q, b), a) in queries.iter().zip(&before).zip(&after) {
        assert_eq!(a, b, "{q:?}");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The keys of `crdt_remote_payload`: a register, a counter and a set.
pub const CRDT_REMOTE_KEYS: [&str; 3] = ["cr:remote2", "cr:ctr", "cr:set"];

/// A `CRDT.MERGE` payload with one register, one counter and one set entry
/// from region node 9, whose keys span both shards of a `--threads 2`
/// server. Every byte is below 0x80, so it can go through `resp_cmd` as a
/// `&str`.
pub fn crdt_remote_payload() -> String {
    let shards: std::collections::HashSet<usize> = CRDT_REMOTE_KEYS
        .iter()
        .map(|k| rudis::router::target_shard(k.as_bytes(), 2))
        .collect();
    assert_eq!(shards.len(), 2, "the payload must span both shards");
    let mut p = Vec::new();
    let ts = |p: &mut Vec<u8>, phys: u64| {
        p.extend_from_slice(&phys.to_le_bytes());
        p.extend_from_slice(&0u32.to_le_bytes());
        p.extend_from_slice(&9u16.to_le_bytes());
    };
    let bytes = |p: &mut Vec<u8>, b: &[u8]| {
        p.extend_from_slice(&(b.len() as u32).to_le_bytes());
        p.extend_from_slice(b);
    };
    // Register cr:remote2 = "far" (deliberately old timestamp).
    p.push(1);
    bytes(&mut p, b"cr:remote2");
    bytes(&mut p, b"far");
    ts(&mut p, 65);
    p.push(0);
    // Counter cr:ctr: node 9 contributed +5.
    p.push(2);
    bytes(&mut p, b"cr:ctr");
    p.extend_from_slice(&1u32.to_le_bytes());
    p.extend_from_slice(&9u16.to_le_bytes());
    p.extend_from_slice(&5i64.to_le_bytes());
    p.extend_from_slice(&0u32.to_le_bytes());
    // Set cr:set gains "r1".
    p.push(3);
    bytes(&mut p, b"cr:set");
    p.extend_from_slice(&1u32.to_le_bytes());
    bytes(&mut p, b"r1");
    p.extend_from_slice(&1u32.to_le_bytes());
    ts(&mut p, 66);
    p.extend_from_slice(&0u32.to_le_bytes());
    String::from_utf8(p).unwrap()
}

/// Writes CRDT registers, counters and sets (with deletions, removals and a
/// cross-shard merge), optionally rewrites the AOF, restarts without an RDB
/// and checks every read answers the same.
pub fn crdt_writes_survive_restart(port: u16, rewrite: bool) {
    let port_s = port.to_string();
    let dir = std::env::temp_dir().join(format!(
        "rudis-crdt-aof-e2e-{}-{}",
        port,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir_s,
        "--enable-experimental-commands",
        "yes",
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let remote = crdt_remote_payload();
    for (cmd, want) in [
        (&["CRDT.SET", "cr:reg", "v1"][..], "+OK"),
        (&["CRDT.SET", "cr:reg", "v2"], "+OK"),
        (&["CRDT.SET", "cr:gone", "x"], "+OK"),
        (&["CRDT.DEL", "cr:gone"], ":1"),
        (&["CRDT.INCRBY", "cr:ctr", "10"], ":10"),
        (&["CRDT.INCRBY", "cr:ctr", "-3"], ":7"),
        (&["CRDT.INCRBY", "cr:ctr2", "4"], ":4"),
        (&["CRDT.SADD", "cr:set", "a"], ":1"),
        (&["CRDT.SADD", "cr:set", "b"], ":1"),
        (&["CRDT.SADD", "cr:set", "a"], ":0"),
        (&["CRDT.SREM", "cr:set", "a"], ":1"),
        (&["CRDT.SADD", "cr:set2", "x"], ":1"),
        (&["CRDT.SREM", "cr:set2", "x"], ":1"),
        // Its keys live on different shards; each must keep its own.
        (&["CRDT.MERGE", &remote], ":3"),
        // Refused increments change nothing and log nothing, so the counter
        // still reads 12 after the restart below.
        (
            &["CRDT.INCRBY", "cr:ctr", "9223372036854775807"],
            "-ERR increment or decrement would overflow",
        ),
        (
            &["CRDT.INCRBY", "cr:ctr2", "-9223372036854775808"],
            "-ERR increment or decrement would overflow",
        ),
    ] {
        let reply = resp_cmd(&mut c, cmd);
        assert!(reply.starts_with(want), "{cmd:?}: {reply}");
    }

    let queries: &[&[&str]] = &[
        &["CRDT.GET", "cr:reg"],
        &["CRDT.GET", "cr:gone"],
        &["CRDT.GET", "cr:remote2"],
        &["CRDT.SMEMBERS", "cr:set"],
        &["CRDT.SMEMBERS", "cr:set2"],
        // INCRBY 0 reads a counter.
        &["CRDT.INCRBY", "cr:ctr", "0"],
        &["CRDT.INCRBY", "cr:ctr2", "0"],
    ];
    // SMEMBERS order is not stable across processes.
    let ask = |c: &mut TcpStream| -> Vec<String> {
        queries
            .iter()
            .map(|q| {
                let reply = resp_cmd(c, q);
                if q[0] == "CRDT.SMEMBERS" {
                    let mut lines: Vec<&str> = reply.split("\r\n").collect();
                    lines[1..].sort();
                    lines.join("\r\n")
                } else {
                    reply
                }
            })
            .collect()
    };
    let mut before = ask(&mut c);
    assert_eq!(before[0], "$2\r\nv2\r\n");
    assert_eq!(before[1], "$-1\r\n");
    assert_eq!(before[2], "$3\r\nfar\r\n");
    assert!(
        before[3].contains("\r\nb") && before[3].contains("\r\nr1") && !before[3].contains("\r\na"),
        "{}",
        before[3]
    );
    assert!(before[4].starts_with("*0"), "{}", before[4]);
    assert_eq!(before[5], ":12\r\n");
    assert_eq!(before[6], ":4\r\n");

    if rewrite {
        assert!(resp_cmd(&mut c, &["BGREWRITEAOF"]).starts_with('+'));
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while info_field(&mut c, "aof_rewrite_in_progress") != "0" {
            assert!(
                std::time::Instant::now() < deadline,
                "rewrite never finished"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(info_field(&mut c, "aof_last_bgrewrite_status"), "ok");
        // Writes after the rewrite are appended to the rewritten file.
        assert_eq!(resp_cmd(&mut c, &["CRDT.SREM", "cr:set", "b"]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["CRDT.DEL", "cr:reg"]), ":1\r\n");
        before = ask(&mut c);
    }
    drop(c);
    {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
            .unwrap();
    }
    let _ = child.wait();
    // Nothing but the AOF can bring the CRDT state back.
    assert!(!dir.join("dump.rdb").exists());

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let after = ask(&mut c);
    for ((q, b), a) in queries.iter().zip(&before).zip(&after) {
        assert_eq!(a, b, "{q:?}");
    }
    // The restarted clock is past every replayed timestamp: a new write to
    // a deleted key wins.
    assert!(resp_cmd(&mut c, &["CRDT.SET", "cr:gone", "back"]).starts_with("+OK"));
    assert_eq!(resp_cmd(&mut c, &["CRDT.GET", "cr:gone"]), "$4\r\nback\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Occurrences of `key` as an entry key in a `CRDT.DUMP` reply.
pub fn crdt_dump_key_count(c: &mut TcpStream, key: &str) -> usize {
    let dump = send_and_read_bytes(c, b"*1\r\n$9\r\nCRDT.DUMP\r\n");
    assert!(dump.starts_with(b"$"), "{dump:?}");
    let mut needle = (key.len() as u32).to_le_bytes().to_vec();
    needle.extend_from_slice(key.as_bytes());
    dump.windows(needle.len()).filter(|w| *w == needle).count()
}

/// With several shards, a CRDT.MERGE queued in MULTI or issued from a
/// script must not put keys on shards that don't own them, and neither may
/// loading an RDB (which holds every shard's CRDT state in one record).
pub fn crdt_keys_stay_on_their_shard(port: u16) {
    let port_s = port.to_string();
    let dir = std::env::temp_dir().join(format!(
        "rudis-crdt-rdb-e2e-{}-{}",
        port,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof-dir",
        dir.to_str().unwrap(),
        "--enable-experimental-commands",
        "yes",
    ];
    let remote = crdt_remote_payload();
    let remote_keys = CRDT_REMOTE_KEYS;
    let local_keys = ["cr:a", "cr:b", "cr:c", "cr:d"];

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for k in local_keys {
        assert!(resp_cmd(&mut c, &["CRDT.SET", k, &format!("v-{k}")]).starts_with("+OK"));
    }
    // A script runs on one shard; a payload spanning shards is refused like
    // any other non-local key, instead of landing on the wrong shard.
    let script = "return redis.call('CRDT.MERGE', ARGV[1])";
    let reply = resp_cmd(&mut c, &["EVAL", script, "0", &remote]);
    assert!(reply.contains("non local key"), "{reply}");
    assert_eq!(resp_cmd(&mut c, &["CRDT.GET", "cr:remote2"]), "$-1\r\n");
    // In MULTI it goes through the router, which splits it by shard.
    assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["CRDT.MERGE", &remote]), "+QUEUED\r\n");
    assert_eq!(resp_cmd(&mut c, &["EXEC"]), "*1\r\n:3\r\n");
    for k in remote_keys.iter().chain(&local_keys) {
        assert_eq!(crdt_dump_key_count(&mut c, k), 1, "{k} before restart");
    }
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    drop(c);
    {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
            .unwrap();
    }
    let _ = child.wait();

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for k in remote_keys.iter().chain(&local_keys) {
        assert_eq!(crdt_dump_key_count(&mut c, k), 1, "{k} after RDB load");
    }
    for k in local_keys {
        assert_eq!(
            resp_cmd(&mut c, &["CRDT.GET", k]),
            format!("${}\r\nv-{k}\r\n", k.len() + 2)
        );
    }
    assert_eq!(
        resp_cmd(&mut c, &["CRDT.GET", "cr:remote2"]),
        "$3\r\nfar\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["CRDT.INCRBY", "cr:ctr", "0"]), ":5\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

pub fn http_get(port: u16, path: &str) -> String {
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write!(c, "GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
    let mut out = String::new();
    c.read_to_string(&mut out).unwrap();
    out
}

/// Runs the binary expecting it to refuse to start; returns its stderr.
pub fn rudis_startup_error(args: &[&str]) -> String {
    let mut child = rudis_bin()
        .args(args)
        .args(DEBUG_ON)
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rudis");
    let Some(status) = wait_exit(&mut child, Duration::from_secs(10)) else {
        let _ = child.kill();
        let _ = child.wait();
        panic!("rudis {:?} started instead of refusing", args);
    };
    let mut err = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert_eq!(status.code(), Some(1), "{err}");
    err
}

/// Counts the top-level elements of a RESP2 array reply.
pub fn resp2_top_level_len(reply: &str) -> usize {
    fn skip(b: &[u8], mut i: usize) -> usize {
        let end = i + b[i..].windows(2).position(|w| w == b"\r\n").unwrap();
        let n: i64 = std::str::from_utf8(&b[i + 1..end])
            .unwrap()
            .parse()
            .unwrap_or(0);
        let t = b[i];
        i = end + 2;
        match t {
            b'*' => (0..n.max(0)).fold(i, |i, _| skip(b, i)),
            b'$' if n >= 0 => i + n as usize + 2,
            _ => i,
        }
    }
    let b = reply.as_bytes();
    let end = b.windows(2).position(|w| w == b"\r\n").unwrap();
    let n: usize = reply[1..end].parse().unwrap();
    let mut i = end + 2;
    for _ in 0..n {
        i = skip(b, i);
    }
    assert_eq!(i, b.len(), "trailing data after the reply");
    n
}

/// Reads `allocator_allocated` (live jemalloc bytes) from `INFO memory`.
pub fn info_allocated_bytes(c: &mut TcpStream) -> u64 {
    let info = resp_cmd(c, &["INFO", "memory"]);
    info.lines()
        .find_map(|l| l.strip_prefix("allocator_allocated:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("no allocator_allocated in INFO memory: {}", info))
}

pub fn proc_status_kb(pid: u32, field: &str) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    status
        .lines()
        .find_map(|l| l.strip_prefix(field))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or_else(|| panic!("no {} in /proc/{}/status", field, pid))
}

pub fn info_field_u64(c: &mut TcpStream, section: &str, field: &str) -> u64 {
    let info = resp_cmd(c, &["INFO", section]);
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("no {field} in INFO {section}: {info}"))
}

/// Streams `n` pipelined `SET key:<i> <32-byte value>` on one connection while
/// draining replies concurrently; returns the raw reply bytes.
pub fn pipelined_set_load(port: u16, n: usize) -> Vec<u8> {
    let conn = TcpStream::connect(("127.0.0.1", port)).unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    let mut writer = conn.try_clone().unwrap();
    let w = thread::spawn(move || {
        let val = "v".repeat(32);
        let mut chunk = Vec::with_capacity(1 << 20);
        for i in 0..n {
            let key = format!("key:{i:08}");
            chunk.extend_from_slice(
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$32\r\n{}\r\n",
                    key.len(),
                    key,
                    val
                )
                .as_bytes(),
            );
            if chunk.len() >= 1 << 20 {
                writer.write_all(&chunk).unwrap();
                chunk.clear();
            }
        }
        writer.write_all(&chunk).unwrap();
    });
    let mut reader = conn;
    let mut replies = Vec::new();
    let mut lines = 0usize;
    let mut buf = vec![0u8; 1 << 16];
    while lines < n {
        let r = reader.read(&mut buf).expect("load reply");
        assert!(r > 0, "server closed the load connection");
        lines += buf[..r].iter().filter(|&&b| b == b'\n').count();
        replies.extend_from_slice(&buf[..r]);
    }
    w.join().unwrap();
    replies
}

/// Driver for spawned server binaries: inherits `MONOIO_FORCE_LEGACY_DRIVER`
/// from the test process (CI runs one job per driver) and defaults to the
/// legacy driver, which avoids io_uring memlock limits on old kernels.
pub fn legacy_driver_env() -> String {
    std::env::var("MONOIO_FORCE_LEGACY_DRIVER").unwrap_or_else(|_| "1".to_string())
}

pub fn info_u64(c: &mut TcpStream, section: &str, field: &str) -> u64 {
    let info = resp_cmd_full(c, &["INFO", section]);
    info.lines()
        .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
        .unwrap_or_else(|| panic!("{field} missing from INFO {section}"))
        .trim()
        .parse()
        .unwrap()
}

/// Whether `line` is a Redis legacy-format log line:
/// `<pid>:<role> <dd> <Mon> <yyyy> <HH:MM:SS.mmm> <level> <message>`.
pub fn is_redis_log_line(line: &str, pid: u32) -> bool {
    let Some((head, rest)) = line.split_once(' ') else {
        return false;
    };
    let Some((p, role)) = head.split_once(':') else {
        return false;
    };
    let parts: Vec<&str> = rest.splitn(6, ' ').collect();
    if parts.len() < 6 {
        return false;
    }
    let [day, mon, year, time, level, _msg] = parts[..] else {
        return false;
    };
    let digits = |s: &str, n: usize| s.len() == n && s.bytes().all(|b| b.is_ascii_digit());
    let t: Vec<&str> = time.split([':', '.']).collect();
    p == pid.to_string()
        && matches!(role, "M" | "S")
        && digits(day, 2)
        && [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ]
        .contains(&mon)
        && digits(year, 4)
        && t.len() == 4
        && digits(t[0], 2)
        && digits(t[1], 2)
        && digits(t[2], 2)
        && digits(t[3], 3)
        && matches!(level, "." | "-" | "*" | "#")
}

/// Waits until the log file at `path` has a line containing `needle`;
/// returns the whole file.
pub fn wait_for_log(path: &std::path::Path, needle: &str) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let log = std::fs::read_to_string(path).unwrap_or_default();
        if log.lines().any(|l| l.contains(needle)) {
            return log;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{needle:?} never logged to {}:\n{log}",
            path.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

pub struct KillRudisOnDrop(pub std::process::Child);

impl Drop for KillRudisOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Reads one complete RESP reply (simple line, bulk, or a flat array of
/// lines/bulks), however it arrives split over reads.
pub fn read_full_reply(stream: &mut TcpStream) -> String {
    fn complete(buf: &[u8]) -> bool {
        fn line_end(buf: &[u8], from: usize) -> Option<usize> {
            buf.get(from..)?
                .windows(2)
                .position(|w| w == b"\r\n")
                .map(|p| from + p)
        }
        // Returns the index after the element starting at `pos`.
        fn element(buf: &[u8], pos: usize) -> Option<usize> {
            let end = line_end(buf, pos)?;
            match buf.get(pos)? {
                b'$' => {
                    let len: i64 = std::str::from_utf8(&buf[pos + 1..end]).ok()?.parse().ok()?;
                    if len < 0 {
                        return Some(end + 2);
                    }
                    let stop = end + 2 + len as usize + 2;
                    (buf.len() >= stop).then_some(stop)
                }
                b'*' => {
                    let n: i64 = std::str::from_utf8(&buf[pos + 1..end]).ok()?.parse().ok()?;
                    let mut p = end + 2;
                    for _ in 0..n.max(0) {
                        p = element(buf, p)?;
                    }
                    Some(p)
                }
                _ => Some(end + 2),
            }
        }
        element(buf, 0).is_some()
    }
    let mut out = Vec::new();
    let mut buf = [0u8; 16384];
    while out.is_empty() || !complete(&out) {
        let n = stream.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed mid-reply");
        out.extend_from_slice(&buf[..n]);
    }
    String::from_utf8_lossy(&out).to_string()
}

/// [`resp_cmd`] that waits for the whole reply.
pub fn resp_cmd_all(stream: &mut TcpStream, args: &[&str]) -> String {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    stream.write_all(out.as_bytes()).unwrap();
    read_full_reply(stream)
}

/// `DEBUG DIGEST`'s 40 hex characters.
pub fn debug_digest(c: &mut TcpStream) -> String {
    let r = resp_cmd_all(c, &["DEBUG", "DIGEST"]);
    let hex = r
        .strip_prefix('+')
        .and_then(|s| s.strip_suffix("\r\n"))
        .unwrap_or_else(|| panic!("DEBUG DIGEST reply: {r:?}"));
    assert_eq!(hex.len(), 40, "{r:?}");
    assert!(hex.bytes().all(|b| b.is_ascii_hexdigit()), "{r:?}");
    hex.to_string()
}

/// Loads the same logical dataset in one of two orders, with different
/// encodings on the way (integers set as strings and back, hashes/sets/
/// zsets grown past their small encodings then shrunk, ...).
pub fn load_digest_dataset(c: &mut TcpStream, reverse: bool) {
    let mut cmds: Vec<Vec<String>> = Vec::new();
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    for i in 0..40 {
        cmds.push(s(&["SET", &format!("str:{i}"), &format!("value-{i}")]));
    }
    cmds.push(s(&["SET", "num", "12345"]));
    cmds.push(s(&["SET", "ttl", "v", "EX", "1000"]));
    cmds.push(s(&["RPUSH", "list", "a", "b", "c"]));
    cmds.push(s(&["SADD", "set", "x", "y", "z", "1", "2"]));
    cmds.push(s(&["ZADD", "zset", "1.5", "a", "0", "b", "-3", "c"]));
    cmds.push(s(&["HSET", "hash", "f1", "v1", "f2", "v2"]));
    cmds.push(s(&["PFADD", "hll", "a", "b", "c"]));
    if reverse {
        cmds.reverse();
    }
    // Stream ids must grow, so these keep their order.
    cmds.push(s(&["XADD", "stream", "1-1", "f", "v"]));
    cmds.push(s(&["XADD", "stream", "2-1", "g", "w"]));
    cmds.push(s(&["XGROUP", "CREATE", "stream", "grp", "0"]));
    for cmd in &cmds {
        let args: Vec<&str> = cmd.iter().map(|x| x.as_str()).collect();
        let r = resp_cmd_all(c, &args);
        assert!(!r.starts_with('-'), "{cmd:?}: {r}");
    }
    if reverse {
        // Same final content, reached through other encodings and orders.
        let big: Vec<String> = (0..200).map(|i| format!("m{i}")).collect();
        let mut sadd = vec!["SADD", "bigset"];
        sadd.extend(big.iter().map(|x| x.as_str()));
        resp_cmd_all(c, &sadd);
        let mut hset = vec!["HSET".to_string(), "bighash".to_string()];
        for m in big.iter().rev() {
            hset.push(m.clone());
            hset.push(format!("v-{m}"));
        }
        let hset: Vec<&str> = hset.iter().map(|x| x.as_str()).collect();
        resp_cmd_all(c, &hset);
        let mut zadd = vec!["ZADD".to_string(), "bigzset".to_string()];
        for (i, m) in big.iter().enumerate() {
            zadd.push(i.to_string());
            zadd.push(m.clone());
        }
        let zadd: Vec<&str> = zadd.iter().map(|x| x.as_str()).collect();
        resp_cmd_all(c, &zadd);
        resp_cmd_all(c, &["INCRBY", "num", "0"]);
    } else {
        let big: Vec<String> = (0..200).map(|i| format!("m{i}")).collect();
        for m in big.iter().rev() {
            resp_cmd_all(c, &["SADD", "bigset", m]);
            resp_cmd_all(c, &["HSET", "bighash", m, &format!("v-{m}")]);
        }
        for (i, m) in big.iter().enumerate().rev() {
            resp_cmd_all(c, &["ZADD", "bigzset", &i.to_string(), m]);
        }
    }
}

/// Sends `cmds` as one pipeline and reads until the trailing ECHO marker
/// comes back, so every reply has arrived.
pub fn pipeline_all(c: &mut TcpStream, cmds: &[Vec<String>], marker: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for cmd in cmds.iter().chain(std::iter::once(&vec![
        "ECHO".to_string(),
        marker.to_string(),
    ])) {
        out.extend_from_slice(format!("*{}\r\n", cmd.len()).as_bytes());
        for a in cmd {
            out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
        }
    }
    c.write_all(&out).unwrap();
    let end = format!("${}\r\n{}\r\n", marker.len(), marker);
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !got.ends_with(end.as_bytes()) {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    got
}

/// Flattens a RESP reply into its scalar values, dropping the nesting.
pub fn resp_scalars(reply: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut lines = reply.split("\r\n");
    while let Some(l) = lines.next() {
        match l.as_bytes().first() {
            None | Some(b'*') | Some(b'%') | Some(b'~') => {}
            Some(b'$') if l == "$-1" => out.push("(nil)".to_string()),
            Some(b'$') => out.push(lines.next().unwrap_or_default().to_string()),
            Some(_) => out.push(l[1..].to_string()),
        }
    }
    out
}

/// What a replica (or an AOF replay) must reproduce of a stream with
/// consumer groups: entries, XINFO STREAM FULL, groups, consumers and the
/// PEL with delivery counts and times. Only times a replica sets with its
/// own clock (consumer seen/active times, idle times) are left out.
pub fn stream_repl_state(c: &mut TcpStream, key: &str) -> Vec<String> {
    let drop_after = |tokens: Vec<String>, keys: &[&str]| {
        let mut out = Vec::new();
        let mut skip = false;
        for t in tokens {
            if skip {
                skip = false;
                continue;
            }
            skip = keys.contains(&t.as_str());
            out.push(t);
        }
        out.join(" ")
    };
    let mut state = vec![
        format!("XRANGE {}", resp_cmd_all(c, &["XRANGE", key, "-", "+"])),
        drop_after(
            resp_scalars(&resp_cmd_all(c, &["XINFO", "STREAM", key, "FULL"])),
            &["seen-time", "active-time"],
        ),
    ];
    let groups = resp_cmd_all(c, &["XINFO", "GROUPS", key]);
    state.push(format!("GROUPS {groups}"));
    let tokens = resp_scalars(&groups);
    let names: Vec<String> = tokens
        .iter()
        .zip(tokens.iter().skip(1))
        .filter(|(k, _)| *k == "name")
        .map(|(_, v)| v.clone())
        .collect();
    for g in &names {
        let pending = resp_scalars(&resp_cmd_all(c, &["XPENDING", key, g, "-", "+", "1000"]));
        // id, consumer, idle, delivery count: idle is time-dependent.
        let rows: Vec<String> = pending
            .chunks(4)
            .map(|r| format!("{} {} count={}", r[0], r[1], r[3]))
            .collect();
        state.push(format!("XPENDING {g} {rows:?}"));
        state.push(drop_after(
            resp_scalars(&resp_cmd_all(c, &["XINFO", "CONSUMERS", key, g])),
            &["idle", "inactive"],
        ));
    }
    state
}

/// `field` of `INFO <section>` as a string.
pub fn info_str(c: &mut TcpStream, section: &str, field: &str) -> String {
    let info = resp_cmd(c, &["INFO", section]);
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("no {field} in INFO {section}: {info}"))
}

/// Polls `cond` for up to 10 seconds.
pub fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(std::time::Instant::now() < deadline, "timed out: {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

pub fn psync2_client(port: u16) -> TcpStream {
    let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    c
}

/// Whether the server closed `s`: EOF or a reset, not a read timeout.
pub fn wait_for_server_close(s: &mut TcpStream) -> bool {
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) => return true,
            Ok(_) => continue,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return false;
            }
            Err(_) => return true,
        }
    }
}

pub fn wait_for_info_field(c: &mut TcpStream, field: &str) -> String {
    let mut info = String::new();
    for _ in 0..100 {
        info = resp_cmd(c, &["INFO", "stats"]);
        if info.contains(field) {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    info
}

/// `FT.SEARCH index query NOCONTENT LIMIT 0 100`'s total and document ids.
pub fn ft_search_ids(c: &mut TcpStream, index: &str, query: &str) -> (i64, Vec<String>) {
    let r = resp_cmd_all(
        c,
        &["FT.SEARCH", index, query, "NOCONTENT", "LIMIT", "0", "100"],
    );
    let mut lines = r.split("\r\n");
    assert!(
        lines.next().is_some_and(|l| l.starts_with('*')),
        "FT.SEARCH {index} {query}: {r:?}"
    );
    let total = lines
        .next()
        .and_then(|l| l.strip_prefix(':'))
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("FT.SEARCH {index} {query}: {r:?}"));
    let mut ids: Vec<String> = lines
        .filter(|l| !l.is_empty() && !l.starts_with('$'))
        .map(str::to_string)
        .collect();
    ids.sort();
    (total, ids)
}

pub fn wait_aof_rewrite_done(c: &mut TcpStream) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !resp_cmd_all(c, &["INFO", "persistence"]).contains("aof_rewrite_in_progress:0") {
        assert!(
            std::time::Instant::now() < deadline,
            "BGREWRITEAOF did not finish"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

/// One RESP array command, as written to an AOF.
pub fn resp_argv_bytes(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
}

/// Spawns rudis without [`DEBUG_ON`], so `enable-debug-command` takes the
/// value from `extra` (or the default).
pub fn spawn_rudis_debug_setting(port: u16, extra: &[&str]) -> std::process::Child {
    let ps = port.to_string();
    let mut child = rudis_bin()
        .args(["--port", &ps, "--threads", "2", "--no-pin"])
        .args(extra)
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rudis");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        if let Some(s) = child.try_wait().unwrap() {
            panic!("rudis exited early: {s}");
        }
        assert!(std::time::Instant::now() < deadline, "rudis did not start");
        thread::sleep(Duration::from_millis(50));
    }
    child
}

/// Reads one RESP array command (as a replica sends during its handshake)
/// from `s`, returning its arguments.
pub fn read_resp_command(s: &mut TcpStream) -> Vec<String> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    let mut read_line = |s: &mut TcpStream, line: &mut Vec<u8>| {
        line.clear();
        while !line.ends_with(b"\r\n") {
            assert_eq!(
                s.read(&mut byte).unwrap(),
                1,
                "replica closed the connection"
            );
            line.push(byte[0]);
        }
    };
    read_line(s, &mut line);
    assert_eq!(line[0], b'*', "{:?}", String::from_utf8_lossy(&line));
    let n: usize = std::str::from_utf8(&line[1..line.len() - 2])
        .unwrap()
        .parse()
        .unwrap();
    let mut args = Vec::new();
    for _ in 0..n {
        read_line(s, &mut line);
        let len: usize = std::str::from_utf8(&line[1..line.len() - 2])
            .unwrap()
            .parse()
            .unwrap();
        let mut arg = vec![0u8; len + 2];
        s.read_exact(&mut arg).unwrap();
        arg.truncate(len);
        args.push(String::from_utf8(arg).unwrap());
    }
    args
}

/// Connects to `port`, retrying while it refuses: `spawn_rudis_listening`
/// waits for the base port, and the other shards of a cluster-mode node
/// may start listening on theirs a moment later.
pub fn connect_ready(port: u16) -> TcpStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(c) => {
                c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                return c;
            }
            Err(e) => {
                assert!(std::time::Instant::now() < deadline, "port {port}: {e}");
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

/// Waits until every node's CLUSTER SLOTS shows all three ranges of a
/// 3-master test cluster: a master's slots are only learned from its own
/// PINGs/PONGs, so killing one before that leaves the others (its replicas
/// included) without its slots to fail over.
pub fn wait_for_slot_views(ports: &[u16]) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let views: Vec<String> = ports
            .iter()
            .map(|p| {
                let mut c = TcpStream::connect(("127.0.0.1", *p)).unwrap();
                c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                resp_cmd(&mut c, &["CLUSTER", "SLOTS"])
            })
            .collect();
        let full = |v: &String| {
            [
                ":0\r\n:5460\r\n",
                ":5461\r\n:10922\r\n",
                ":10923\r\n:16383\r\n",
            ]
            .iter()
            .all(|r| v.contains(r))
        };
        if views.iter().all(full) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "slot views: {views:?}"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

/// Sends `cmd` and reads until `n` complete RESP replies arrived.
pub fn send_and_read_reply_n(stream: &mut TcpStream, cmd: &[u8], n: usize) -> String {
    stream.write_all(cmd).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 16384];
    loop {
        let mut at = 0;
        let mut complete = 0;
        while complete < n {
            match resp_value_end(&got, at) {
                Some(end) => {
                    at = end;
                    complete += 1;
                }
                None => break,
            }
        }
        if complete == n {
            return String::from_utf8_lossy(&got).to_string();
        }
        let k = stream.read(&mut buf).unwrap();
        assert!(k > 0, "connection closed mid-reply");
        got.extend_from_slice(&buf[..k]);
    }
}
