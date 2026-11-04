//! Network-partition chaos tests: a master and its replica (real server
//! processes) talk through a TCP proxy that can cut the link, as a network
//! partition would, and heal it. Checks that the replica notices, keeps
//! serving stale reads, resyncs (partially when the backlog still covers
//! the gap, fully otherwise) and ends with the master's exact dataset, and
//! that `min-replicas-to-write` refuses writes while no replica is reachable.

mod common;
use common::*;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// A TCP proxy from `listen` to `upstream` that can be partitioned.
///
/// While cut, bytes stop flowing in both directions on existing connections
/// (the proxy stops reading, so they wait in kernel buffers, as packets do
/// in retransmission during a short partition) and new connections are
/// closed at once (as unreachable). Healing resumes the flow; connections
/// either side closed meanwhile are then torn down on both sides.
struct Partition {
    cut: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<TcpStream>>>,
}

impl Partition {
    fn new(listen: u16, upstream: u16) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", listen)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let cut = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let conns = Arc::new(Mutex::new(Vec::new()));
        {
            let (cut, stop, conns) = (cut.clone(), stop.clone(), conns.clone());
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((down, _)) => {
                            if cut.load(Ordering::Relaxed) {
                                drop(down);
                                continue;
                            }
                            let Ok(up) = TcpStream::connect(("127.0.0.1", upstream)) else {
                                continue;
                            };
                            down.set_nonblocking(false).unwrap();
                            {
                                let mut c = conns.lock().unwrap();
                                c.push(down.try_clone().unwrap());
                                c.push(up.try_clone().unwrap());
                            }
                            for (from, to) in [
                                (down.try_clone().unwrap(), up.try_clone().unwrap()),
                                (up, down),
                            ] {
                                let (cut, stop) = (cut.clone(), stop.clone());
                                thread::spawn(move || pump(from, to, &cut, &stop));
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => thread::sleep(Duration::from_millis(5)),
                    }
                }
            });
        }
        Partition { cut, stop, conns }
    }

    fn cut(&self) {
        self.cut.store(true, Ordering::SeqCst);
    }

    fn heal(&self) {
        self.cut.store(false, Ordering::SeqCst);
    }
}

impl Drop for Partition {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for c in self.conns.lock().unwrap().drain(..) {
            let _ = c.shutdown(Shutdown::Both);
        }
    }
}

/// Copies `from` to `to` until either closes, pausing while cut.
fn pump(mut from: TcpStream, mut to: TcpStream, cut: &AtomicBool, stop: &AtomicBool) {
    from.set_read_timeout(Some(Duration::from_millis(20)))
        .unwrap();
    let mut buf = vec![0u8; 64 * 1024];
    while !stop.load(Ordering::Relaxed) {
        if cut.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(10));
            continue;
        }
        match from.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
            Err(_) => break,
        }
    }
    let _ = from.shutdown(Shutdown::Both);
    let _ = to.shutdown(Shutdown::Both);
}

fn info_field(port: u16, section: &str, field: &str) -> Option<String> {
    let mut c = TcpStream::connect(("127.0.0.1", port)).ok()?;
    c.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let info = resp_cmd(&mut c, &["INFO", section]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
}

fn cmd(port: u16, args: &[&str]) -> String {
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    resp_cmd(&mut c, args)
}

fn wait_for(what: &str, secs: u64, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

/// Writes `n` keys `prefix:<i>` with `size`-byte values, pipelined.
fn write_keys(port: u16, prefix: &str, n: usize, size: usize) {
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let val = "v".repeat(size);
    for chunk in (0..n).collect::<Vec<_>>().chunks(100) {
        let mut req = String::new();
        for i in chunk {
            let k = format!("{prefix}:{i}");
            req.push_str(&format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                k.len(),
                k,
                val.len(),
                val
            ));
        }
        c.write_all(req.as_bytes()).unwrap();
        let mut got = 0;
        let mut buf = vec![0u8; 8192];
        while got < chunk.len() * 5 {
            let r = c.read(&mut buf).unwrap();
            assert!(r > 0, "connection closed");
            assert!(
                !buf[..r].contains(&b'-'),
                "{}",
                String::from_utf8_lossy(&buf[..r])
            );
            got += r;
        }
    }
}

fn spawn_master(port: u16, extra: &[&str]) -> ServerGuard {
    let p = port.to_string();
    let mut args = vec![
        "--port",
        &p,
        "--threads",
        "4",
        "--no-pin",
        "--save",
        "",
        "--repl-ping-replica-period",
        "1",
    ];
    args.extend_from_slice(extra);
    ServerGuard(spawn_rudis_listening(&args, port))
}

fn spawn_replica(port: u16, master_via: u16) -> ServerGuard {
    let p = port.to_string();
    let m = master_via.to_string();
    ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &p,
            "--threads",
            "4",
            "--no-pin",
            "--save",
            "",
            "--replicaof",
            "127.0.0.1",
            &m,
            "--repl-timeout",
            "2",
        ],
        port,
    ))
}

fn assert_converged(master: u16, replica: u16) {
    wait_for("the replica to catch up", 30, || {
        info_field(replica, "replication", "master_link_status").as_deref() == Some("up")
            && info_field(master, "replication", "master_repl_offset")
                == info_field(replica, "replication", "master_repl_offset")
    });
    assert_eq!(cmd(master, &["DBSIZE"]), cmd(replica, &["DBSIZE"]));
    assert_eq!(
        cmd(master, &["DEBUG", "DIGEST"]),
        cmd(replica, &["DEBUG", "DIGEST"])
    );
}

/// A partition shorter than the backlog: the replica notices (repl-timeout),
/// keeps serving the data it has, and after the heal continues with a
/// partial resync, ending with exactly the master's dataset.
#[test]
fn test_short_partition_heals_with_a_partial_resync_e2e() {
    let (mp, pp, rp) = (15800u16, 15801u16, 15802u16);
    let _m = spawn_master(mp, &[]);
    let proxy = Partition::new(pp, mp);
    let _r = spawn_replica(rp, pp);
    write_keys(mp, "before", 1000, 64);
    wait_for("the first sync", 30, || {
        cmd(mp, &["WAIT", "1", "100"]) == ":1\r\n"
    });
    assert_eq!(info_field(mp, "stats", "sync_full").as_deref(), Some("1"));

    proxy.cut();
    write_keys(mp, "during", 2000, 64);
    assert_eq!(cmd(mp, &["WAIT", "1", "300"]), ":0\r\n");
    wait_for("the replica to see the link down", 15, || {
        info_field(rp, "replication", "master_link_status").as_deref() == Some("down")
    });
    // Stale reads keep working (replica-serve-stale-data yes).
    assert_eq!(cmd(rp, &["EXISTS", "before:7"]), ":1\r\n");
    assert_eq!(cmd(rp, &["EXISTS", "during:7"]), ":0\r\n");

    proxy.heal();
    assert_converged(mp, rp);
    assert_eq!(cmd(rp, &["DBSIZE"]), ":3000\r\n");
    assert_eq!(info_field(mp, "stats", "sync_full").as_deref(), Some("1"));
    let partial: u64 = info_field(mp, "stats", "sync_partial_ok")
        .unwrap()
        .parse()
        .unwrap();
    assert!(partial >= 1, "no partial resync");
    assert_eq!(cmd(mp, &["WAIT", "1", "2000"]), ":1\r\n");
}

/// A partition during which the master writes more than its backlog holds:
/// the partial resync is refused and a full one restores the dataset.
#[test]
fn test_partition_past_the_backlog_heals_with_a_full_resync_e2e() {
    let (mp, pp, rp) = (15805u16, 15806u16, 15807u16);
    let _m = spawn_master(mp, &["--repl-backlog-size", "1mb"]);
    let proxy = Partition::new(pp, mp);
    let _r = spawn_replica(rp, pp);
    write_keys(mp, "before", 500, 64);
    wait_for("the first sync", 30, || {
        cmd(mp, &["WAIT", "1", "100"]) == ":1\r\n"
    });

    proxy.cut();
    wait_for("the replica to see the link down", 15, || {
        info_field(rp, "replication", "master_link_status").as_deref() == Some("down")
    });
    // ~4 MB of writes: four times the backlog.
    write_keys(mp, "during", 4000, 1000);

    proxy.heal();
    assert_converged(mp, rp);
    assert_eq!(cmd(rp, &["DBSIZE"]), ":4500\r\n");
    assert_eq!(info_field(mp, "stats", "sync_full").as_deref(), Some("2"));
}

/// With min-replicas-to-write 1, a master cut off from its only replica
/// refuses writes with NOREPLICAS once the replica's acks are older than
/// min-replicas-max-lag, and accepts them again after the heal. No write
/// it acknowledged meanwhile is missing from the replica afterwards.
#[test]
fn test_min_replicas_to_write_refuses_writes_while_partitioned_e2e() {
    let (mp, pp, rp) = (15810u16, 15811u16, 15812u16);
    let _m = spawn_master(
        mp,
        &[
            "--min-replicas-to-write",
            "1",
            "--min-replicas-max-lag",
            "2",
        ],
    );
    let proxy = Partition::new(pp, mp);
    let _r = spawn_replica(rp, pp);
    wait_for("the first sync", 30, || {
        cmd(mp, &["WAIT", "1", "100"]) == ":1\r\n"
    });
    wait_for("writes to be accepted", 15, || {
        cmd(mp, &["SET", "k", "0"]) == "+OK\r\n"
    });

    proxy.cut();
    // Keep writing; record what the master accepted.
    let mut accepted = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    for i in 0.. {
        assert!(Instant::now() < deadline, "writes never refused");
        let r = cmd(mp, &["SET", &format!("w:{i}"), "1"]);
        if r == "+OK\r\n" {
            accepted.push(i);
        } else {
            assert!(r.starts_with("-NOREPLICAS"), "{r}");
            // Stays refused for the rest of the partition.
            thread::sleep(Duration::from_millis(500));
            let r = cmd(mp, &["SET", "late", "1"]);
            assert!(r.starts_with("-NOREPLICAS"), "{r}");
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    proxy.heal();
    wait_for("writes to be accepted again", 30, || {
        cmd(mp, &["SET", "after", "1"]) == "+OK\r\n"
    });
    assert_converged(mp, rp);
    for i in accepted {
        assert_eq!(
            cmd(rp, &["EXISTS", &format!("w:{i}")]),
            ":1\r\n",
            "w:{i} lost"
        );
    }
    assert_eq!(cmd(rp, &["EXISTS", "late"]), ":0\r\n");
}

fn signal(server: &ServerGuard, sig: &str) {
    let status = std::process::Command::new("kill")
        .args([sig, &server.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

/// A cluster master cut off from the majority of masters (here: the other
/// two are frozen, so it hears from neither) marks the cluster down and
/// refuses its keys with CLUSTERDOWN after the node timeout, instead of
/// taking writes the majority side would discard on a failover. With
/// cluster-allow-reads-when-down it keeps serving reads. Once the majority
/// answers again it serves again, after the rejoin delay.
#[test]
fn test_cluster_master_in_a_minority_refuses_its_keys_e2e() {
    let ports = [15820u16, 15822, 15824];
    let ranges = [(0, 5460), (5461, 10922), (10923, 16383)];
    let servers: Vec<ServerGuard> = ports
        .iter()
        .map(|p| {
            let dir = std::env::temp_dir().join(format!(
                "rudis_partition_cluster_{}_{}",
                p,
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            ServerGuard(spawn_rudis_listening(
                &[
                    "--port",
                    &p.to_string(),
                    "--dir",
                    dir.to_str().unwrap(),
                    "--cluster-enabled",
                    "yes",
                    "--cluster-node-timeout",
                    "1000",
                    "--threads",
                    "1",
                    "--no-pin",
                    "--save",
                    "",
                ],
                *p,
            ))
        })
        .collect();
    for p in ports {
        assert_eq!(
            cmd(p, &["CLUSTER", "DELSLOTSRANGE", "0", "16383"]),
            "+OK\r\n"
        );
    }
    for (p, (s, e)) in ports.iter().zip(ranges) {
        let (s, e) = (s.to_string(), e.to_string());
        assert_eq!(cmd(*p, &["CLUSTER", "ADDSLOTSRANGE", &s, &e]), "+OK\r\n");
    }
    for p in &ports[1..] {
        assert_eq!(
            cmd(ports[0], &["CLUSTER", "MEET", "127.0.0.1", &p.to_string()]),
            "+OK\r\n"
        );
    }
    wait_for_slot_views(&ports);
    let lone = ports[2];
    let key = (0..)
        .map(|i| format!("mk{i}"))
        .find(|k| rudis::router::key_slot(k.as_bytes()) >= 10923)
        .unwrap();
    assert_eq!(cmd(lone, &["SET", &key, "v1"]), "+OK\r\n");
    wait_for("cluster_state ok everywhere", 20, || {
        ports
            .iter()
            .all(|p| cmd(*p, &["CLUSTER", "INFO"]).contains("cluster_state:ok"))
    });

    // Partition: the lone master hears from neither other master.
    signal(&servers[0], "-STOP");
    signal(&servers[1], "-STOP");
    let cut_at = Instant::now();
    wait_for("the lone master to refuse writes", 20, || {
        let r = cmd(lone, &["SET", &key, "minority-write"]);
        if r == "+OK\r\n" {
            return false;
        }
        assert_eq!(r, "-CLUSTERDOWN The cluster is down\r\n");
        true
    });
    eprintln!("refused after {:?}", cut_at.elapsed());
    assert_eq!(
        cmd(lone, &["GET", &key]),
        "-CLUSTERDOWN The cluster is down\r\n"
    );
    assert!(cmd(lone, &["CLUSTER", "INFO"]).contains("cluster_state:fail"));
    // Keyless commands still work.
    assert_eq!(cmd(lone, &["PING"]), "+PONG\r\n");
    // Reads allowed when configured; writes still refused.
    assert_eq!(
        cmd(
            lone,
            &["CONFIG", "SET", "cluster-allow-reads-when-down", "yes"]
        ),
        "+OK\r\n"
    );
    let r = cmd(lone, &["GET", &key]);
    assert!(r == "$2\r\nv1\r\n" || r.starts_with("$14"), "{r}");
    assert_eq!(
        cmd(lone, &["SET", &key, "x"]),
        "-CLUSTERDOWN The cluster is down\r\n"
    );

    // Heal.
    signal(&servers[0], "-CONT");
    signal(&servers[1], "-CONT");
    wait_for("the lone master to serve again", 30, || {
        cmd(lone, &["SET", &key, "v2"]) == "+OK\r\n"
    });
    assert!(cmd(lone, &["CLUSTER", "INFO"]).contains("cluster_state:ok"));
    assert_eq!(cmd(lone, &["GET", &key]), "$2\r\nv2\r\n");
}
