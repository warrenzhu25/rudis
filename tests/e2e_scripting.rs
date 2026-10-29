//! End-to-end tests: scripting (see `tests/common` for the helpers).

#[macro_use]
mod common;
#[allow(unused_imports)]
use common::*;
#[allow(unused_imports)]
use rudis::router::target_shard;
#[allow(unused_imports)]
use rudis::server::run_shard_worker;
#[allow(unused_imports)]
use std::io::{Read, Write};
#[allow(unused_imports)]
use std::net::TcpStream;
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::time::Duration;

#[test]
fn test_lua_scripting_engine_e2e() {
    let port = 16430;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    fn send_cmd(stream: &mut TcpStream, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        send_and_read(stream, out.as_bytes())
    }

    // 1. Primitive Return Values
    assert_eq!(
        send_cmd(&mut client, &["EVAL", "return 42", "0"]),
        ":42\r\n"
    );
    assert_eq!(
        send_cmd(&mut client, &["EVAL", "return 'hello world'", "0"]),
        "$11\r\nhello world\r\n"
    );
    assert_eq!(
        send_cmd(&mut client, &["EVAL", "return true", "0"]),
        ":1\r\n"
    );
    assert_eq!(
        send_cmd(&mut client, &["EVAL", "return false", "0"]),
        "$-1\r\n"
    );
    assert_eq!(
        send_cmd(&mut client, &["EVAL", "return {10, 'rudis', false}", "0"]),
        "*3\r\n:10\r\n$5\r\nrudis\r\n$-1\r\n"
    );

    // 2. KEYS and ARGV Passing
    let keys_argv_resp = send_cmd(
        &mut client,
        &[
            "EVAL",
            "return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}",
            "2",
            "{l}keyA",
            "{l}keyB",
            "val1",
            "val2",
        ],
    );
    assert_eq!(
        keys_argv_resp,
        "*4\r\n$7\r\n{l}keyA\r\n$7\r\n{l}keyB\r\n$4\r\nval1\r\n$4\r\nval2\r\n"
    );

    // 3. redis.call SET and GET
    let set_resp = send_cmd(
        &mut client,
        &[
            "EVAL",
            "return redis.call('SET', KEYS[1], ARGV[1])",
            "1",
            "lua_key",
            "lua_val",
        ],
    );
    assert_eq!(set_resp, "+OK\r\n");

    let get_resp = send_cmd(
        &mut client,
        &["EVAL", "return redis.call('GET', KEYS[1])", "1", "lua_key"],
    );
    assert_eq!(get_resp, "$7\r\nlua_val\r\n");

    // Multiple operations and table inspection (keys share a hash tag so
    // they live on one shard; scripts may not span shards)
    let multi_resp = send_cmd(
        &mut client,
        &[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], ARGV[2])",
            "2",
            "{lua}k_str",
            "{lua}k_num",
            "hello",
            "50",
        ],
    );
    assert_eq!(multi_resp, ":50\r\n");

    // 4. redis.pcall error handling
    let pcall_resp = send_cmd(
        &mut client,
        &[
            "EVAL",
            "local res = redis.pcall('INCRBY', KEYS[1], 'not_a_num'); if res['err'] then return res['err'] else return 'ok' end",
            "1",
            "{lua}k_num",
        ],
    );
    assert!(
        pcall_resp.starts_with("$"),
        "Expected bulk string error returned from pcall, got {}",
        pcall_resp
    );
    assert!(pcall_resp.contains("integer") || pcall_resp.contains("ERR"));

    // 5. redis.sha1hex helper
    let sha_calc = send_cmd(
        &mut client,
        &["EVAL", "return redis.sha1hex('test-string')", "0"],
    );
    // sha1 of 'test-string' is 4f49d69613b186e71104c7ca1b26c1e5b78c9193
    assert_eq!(
        sha_calc,
        "$40\r\n4f49d69613b186e71104c7ca1b26c1e5b78c9193\r\n"
    );

    // 6. SCRIPT LOAD, SCRIPT EXISTS, EVALSHA, SCRIPT FLUSH
    let script_code = "return redis.call('GET', KEYS[1])";
    let load_resp = send_cmd(&mut client, &["SCRIPT", "LOAD", script_code]);
    assert!(load_resp.starts_with("$40\r\n"));
    let sha = load_resp
        .trim_start_matches("$40\r\n")
        .trim_end_matches("\r\n");

    // SCRIPT EXISTS
    let exists_resp = send_cmd(
        &mut client,
        &[
            "SCRIPT",
            "EXISTS",
            sha,
            "0000000000000000000000000000000000000000",
        ],
    );
    assert_eq!(exists_resp, "*2\r\n:1\r\n:0\r\n");

    // EVALSHA execution
    let evalsha_resp = send_cmd(&mut client, &["EVALSHA", sha, "1", "{lua}k_str"]);
    assert_eq!(evalsha_resp, "$5\r\nhello\r\n");

    // SCRIPT FLUSH
    let flush_resp = send_cmd(&mut client, &["SCRIPT", "FLUSH"]);
    assert_eq!(flush_resp, "+OK\r\n");

    let exists_after_flush = send_cmd(&mut client, &["SCRIPT", "EXISTS", sha]);
    assert_eq!(exists_after_flush, "*1\r\n:0\r\n");

    // EVALSHA after flush should return NOSCRIPT error
    let evalsha_err = send_cmd(&mut client, &["EVALSHA", sha, "1", "{lua}k_str"]);
    assert!(
        evalsha_err.starts_with("-NOSCRIPT"),
        "Expected -NOSCRIPT error, got {}",
        evalsha_err
    );
}

#[test]
fn test_client_setinfo_latency_pubsub_function_e2e() {
    let port = 19123;
    start_test_server(port, 4);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. CLIENT SETINFO lib-name & lib-ver
    assert_eq!(
        send_and_read(&mut stream, b"CLIENT SETINFO lib-name test-driver\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CLIENT SETINFO lib-ver 1.2.3\r\n"),
        "+OK\r\n"
    );

    let client_info = send_and_read(&mut stream, b"CLIENT INFO\r\n");
    assert!(client_info.contains("lib-name=test-driver"));
    assert!(client_info.contains("lib-ver=1.2.3"));

    let client_list = send_and_read(&mut stream, b"CLIENT LIST\r\n");
    assert!(client_list.contains("lib-name=test-driver"));
    assert!(client_list.contains("lib-ver=1.2.3"));

    // 2. LATENCY subcommands
    assert_eq!(send_and_read(&mut stream, b"LATENCY LATEST\r\n"), "*0\r\n");
    let doctor = send_and_read(&mut stream, b"LATENCY DOCTOR\r\n");
    assert!(doctor.contains("Dave"));

    assert_eq!(
        send_and_read(&mut stream, b"LATENCY HISTORY cmd\r\n"),
        "*0\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"LATENCY RESET\r\n"), ":0\r\n");
    let graph = send_and_read(&mut stream, b"LATENCY GRAPH cmd\r\n");
    assert!(graph.contains("No samples available"));

    let latency_help = send_and_read(&mut stream, b"LATENCY HELP\r\n");
    assert!(latency_help.contains("LATEST"));

    // 3. PUBSUB HELP
    let pubsub_help = send_and_read(&mut stream, b"PUBSUB HELP\r\n");
    assert!(pubsub_help.contains("CHANNELS"));
    assert!(pubsub_help.contains("NUMPAT"));

    // 4. FUNCTION STATS & KILL
    let func_stats = send_and_read(&mut stream, b"FUNCTION STATS\r\n");
    assert!(func_stats.contains("running_script"));
    assert!(func_stats.contains("libraries_count"));
    // Nothing is running, so Valkey replies NOTBUSY.
    assert_eq!(
        send_and_read(&mut stream, b"FUNCTION KILL\r\n"),
        "-NOTBUSY No scripts in execution right now.\r\n"
    );
}

#[test]
fn test_scripts_reject_keys_on_several_shards_e2e() {
    // A script runs against one shard's database. Declared keys owned by
    // other shards must be rejected, not silently read/written on the wrong
    // shard.
    let port = 16978;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "4", "--no-pin"];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    // 16 plain keys always span more than one of 4 shards.
    let spread: Vec<String> = (0..16).map(|i| format!("ev:k{}", i)).collect();
    let mut cmd: Vec<&str> = vec!["EVAL", "redis.call('set', KEYS[1], 'x') return 1", "16"];
    cmd.extend(spread.iter().map(|s| s.as_str()));
    let r = resp_cmd(&mut c, &cmd);
    assert!(r.starts_with("-CROSSSLOT"), "EVAL across shards: {}", r);
    assert_eq!(resp_cmd(&mut c, &["GET", "ev:k0"]), "$-1\r\n");

    let sha = resp_cmd(
        &mut c,
        &["SCRIPT", "LOAD", "redis.call('set', KEYS[1], 'x') return 1"],
    );
    let sha = sha.lines().nth(1).unwrap().to_string();
    let mut cmd: Vec<&str> = vec!["EVALSHA", &sha, "16"];
    cmd.extend(spread.iter().map(|s| s.as_str()));
    let r = resp_cmd(&mut c, &cmd);
    assert!(r.starts_with("-CROSSSLOT"), "EVALSHA across shards: {}", r);

    let lib = "#!lua name=evlib\nredis.register_function('evset', function(keys, args) return redis.call('set', keys[1], 'f') end)";
    assert_eq!(
        resp_cmd(&mut c, &["FUNCTION", "LOAD", lib]),
        "$5\r\nevlib\r\n"
    );
    let mut cmd: Vec<&str> = vec!["FCALL", "evset", "16"];
    cmd.extend(spread.iter().map(|s| s.as_str()));
    let r = resp_cmd(&mut c, &cmd);
    assert!(r.starts_with("-CROSSSLOT"), "FCALL across shards: {}", r);
    assert_eq!(resp_cmd(&mut c, &["GET", "ev:k0"]), "$-1\r\n");

    // Keys sharing a hash tag live on one shard and keep working, and the
    // writes are visible to plain reads afterwards.
    for i in 0..8 {
        let (a, b) = (format!("{{ev{}}}a", i), format!("{{ev{}}}b", i));
        let r = resp_cmd(
            &mut c,
            &[
                "EVAL",
                "redis.call('set', KEYS[1], '1') redis.call('set', KEYS[2], '2') return 1",
                "2",
                &a,
                &b,
            ],
        );
        assert_eq!(r, ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["GET", &a]), "$1\r\n1\r\n");
        assert_eq!(resp_cmd(&mut c, &["GET", &b]), "$1\r\n2\r\n");
        let r = resp_cmd(&mut c, &["FCALL", "evset", "2", &a, &b]);
        assert_eq!(r, "+OK\r\n");
        assert_eq!(resp_cmd(&mut c, &["GET", &a]), "$1\r\nf\r\n");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_lua_publish_from_two_shards_does_not_deadlock_e2e() {
    let port = 16953;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    // Subscribers on both shards, so each shard's PUBLISH must reach the other.
    let mut subs: Vec<TcpStream> = (0..6)
        .map(|_| {
            let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
            s.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\npc\r\n")
                .unwrap();
            s
        })
        .collect();
    for s in &mut subs {
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut buf = [0u8; 256];
        let n = s.read(&mut buf).unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("subscribe"));
    }
    // Keep the subscriber sockets drained so the server never blocks on them.
    let drainers: Vec<_> = subs
        .into_iter()
        .map(|mut s| {
            std::thread::spawn(move || {
                s.set_read_timeout(Some(Duration::from_millis(500)))
                    .unwrap();
                let mut buf = [0u8; 65536];
                let mut idle = 0;
                while idle < 10 {
                    match s.read(&mut buf) {
                        Ok(0) => break,
                        Ok(_) => idle = 0,
                        Err(_) => idle += 1,
                    }
                }
            })
        })
        .collect();

    // With nobody else publishing, a script gets the full receiver count.
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(
            &mut c,
            &["EVAL", "return redis.call('publish','pc','x')", "0"]
        ),
        ":6\r\n"
    );

    // Scripts on both shards publishing at once used to block each shard
    // thread on the other's reply forever, hanging the whole server.
    let script = "for i=1,200 do redis.call('publish','pc','x') end return 1";
    let publishers: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(move || {
                let mut p = TcpStream::connect(("127.0.0.1", port)).unwrap();
                p.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                for _ in 0..5 {
                    assert_eq!(resp_cmd(&mut p, &["EVAL", script, "0"]), ":1\r\n");
                }
            })
        })
        .collect();
    for p in publishers {
        p.join().expect("publisher got no EVAL reply");
    }
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    for d in drainers {
        let _ = d.join();
    }
}

#[test]
fn test_script_access_to_non_local_key_is_rejected_e2e() {
    let port = 16952;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let keys: Vec<String> = (0..8).map(|i| format!("sk:{i}")).collect();
    // A keyless script runs on the connection's shard. Writing a key owned
    // by another shard used to land in the wrong table: KEYS and DBSIZE
    // listed it but GET returned nil. (A script whose first command touches
    // another shard's key is rerun there, so start with a keyless command.)
    let mut rejected = 0;
    for k in &keys {
        let r = resp_cmd(
            &mut c,
            &[
                "EVAL",
                "redis.call('ping'); return redis.call('set',ARGV[1],'v')",
                "0",
                k,
            ],
        );
        if r == "+OK\r\n" {
            assert_eq!(resp_cmd(&mut c, &["GET", k]), "$1\r\nv\r\n", "{k}");
        } else {
            assert!(
                r.starts_with("-ERR Script attempted to access a non local key"),
                "{k}: {r}"
            );
            assert_eq!(resp_cmd(&mut c, &["EXISTS", k]), ":0\r\n", "{k}");
            // redis.pcall reports it as an error reply too.
            let r = resp_cmd(
                &mut c,
                &[
                    "EVAL",
                    "redis.call('ping'); return redis.pcall('get',ARGV[1])",
                    "0",
                    k,
                ],
            );
            assert!(
                r.starts_with("-ERR Script attempted to access a non local key"),
                "{r}"
            );
            rejected += 1;
        }
    }
    assert!(rejected > 0, "all 8 keys on the connection's shard?");
    // Declaring the key routes the script to its shard, so it always works.
    for k in &keys {
        assert_eq!(
            resp_cmd(
                &mut c,
                &["EVAL", "return redis.call('set',KEYS[1],'w')", "1", k]
            ),
            "+OK\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["GET", k]), "$1\r\nw\r\n", "{k}");
    }
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":8\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_keyless_script_runs_on_owner_of_first_key_e2e() {
    let port = 16934u16;
    let ps = port.to_string();
    let mut server = spawn_rudis_listening(&["--port", &ps, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Scripts that declare no keys used to fail whenever the key they
    // touch lived on another shard than the client's connection.
    for i in 0..32 {
        let key = format!("ks:{i}");
        let r = resp_cmd(
            &mut c,
            &["EVAL", "return redis.call('SET', ARGV[1], 'v')", "0", &key],
        );
        assert_eq!(r, "+OK\r\n", "{key}");
        assert_eq!(resp_cmd(&mut c, &["GET", &key]), "$1\r\nv\r\n");
    }
    resp_cmd(
        &mut c,
        &[
            "FUNCTION",
            "LOAD",
            "REPLACE",
            "#!lua name=kl\nredis.register_function('kl', function(k, a) return redis.call('INCR', a[1]) end)",
        ],
    );
    for i in 0..32 {
        let key = format!("kf:{i}");
        assert_eq!(
            resp_cmd(&mut c, &["FCALL", "kl", "0", &key]),
            ":1\r\n",
            "{key}"
        );
    }
    // Only a first command may move the script: after anything ran, a key
    // of another shard is still an error, and nothing ran twice.
    let mut errors = 0;
    for i in 0..32 {
        let key = format!("kp:{i}");
        let r = resp_cmd(
            &mut c,
            &[
                "EVAL",
                "redis.call('INCR', 'kp:counter{x}'); return redis.call('SET', ARGV[1], 'v')",
                "0",
                &key,
            ],
        );
        if r.starts_with("-ERR Script attempted to access a non local key") {
            errors += 1;
        } else {
            assert_eq!(r, "+OK\r\n");
        }
    }
    assert!(errors > 0);
    assert_eq!(resp_cmd(&mut c, &["GET", "kp:counter{x}"]), "$2\r\n32\r\n");
    let _ = server.kill();
    let _ = server.wait();
}

#[test]
fn test_multi_db_commands_watch_lua_and_blocking_e2e() {
    let port = 17113u16;
    start_test_server(port, 4);

    let mut c1 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c1.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut c2 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // 1. SELECT bounds and CONFIG GET databases
    let cfg = resp_cmd(&mut c1, &["CONFIG", "GET", "databases"]);
    assert!(cfg.contains("16"), "{cfg:?}");
    assert!(resp_cmd(&mut c1, &["SELECT", "-1"]).starts_with("-ERR DB index is out of range"));
    assert!(resp_cmd(&mut c1, &["SELECT", "16"]).starts_with("-ERR DB index is out of range"));
    assert_eq!(resp_cmd(&mut c1, &["SELECT", "2"]), "+OK\r\n");
    assert!(resp_cmd(&mut c1, &["CLIENT", "INFO"]).contains("db=2"));
    assert_eq!(resp_cmd(&mut c1, &["RESET"]), "+RESET\r\n");
    assert!(resp_cmd(&mut c1, &["CLIENT", "INFO"]).contains("db=0"));

    // 2. Per-connection DB isolation across shards (MSET, MGET, DBSIZE, INFO keyspace)
    assert_eq!(
        resp_cmd(
            &mut c1,
            &[
                "MSET", "mdb_k1", "db0_v1", "mdb_k2", "db0_v2", "mdb_k3", "db0_v3"
            ]
        ),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c2, &["SELECT", "1"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c2, &["MGET", "mdb_k1", "mdb_k2", "mdb_k3"]),
        "*3\r\n$-1\r\n$-1\r\n$-1\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c2, &["MSET", "mdb_k1", "db1_v1", "mdb_k2", "db1_v2"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c1, &["DBSIZE"]), ":3\r\n");
    assert_eq!(resp_cmd(&mut c2, &["DBSIZE"]), ":2\r\n");
    let ks = resp_cmd(&mut c1, &["INFO", "keyspace"]);
    assert!(
        ks.contains("db0:keys=3,") && ks.contains("db1:keys=2,"),
        "{ks:?}"
    );

    // 3. MOVE (same-DB, missing, existing dest, and successful move with TTL)
    assert_eq!(resp_cmd(&mut c1, &["MOVE", "mdb_k1", "0"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c1, &["MOVE", "no_such_key", "1"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c1, &["MOVE", "mdb_k1", "1"]), ":0\r\n"); // exists in db 1
    assert_eq!(resp_cmd(&mut c1, &["PEXPIRE", "mdb_k3", "60000"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c1, &["MOVE", "mdb_k3", "1"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c1, &["GET", "mdb_k3"]), "$-1\r\n");
    assert_eq!(resp_cmd(&mut c2, &["GET", "mdb_k3"]), "$6\r\ndb0_v3\r\n");
    let pttl = resp_cmd(&mut c2, &["PTTL", "mdb_k3"]);
    assert!(!pttl.starts_with(":-"), "TTL lost on MOVE: {pttl:?}");

    // 4. COPY ... DB <n> [REPLACE] across different shards
    let mut k_s0 = String::new();
    let mut k_s1 = String::new();
    for i in 0..200 {
        let cand = format!("cp_key_{i}");
        let sh = target_shard(cand.as_bytes(), 4);
        if sh == 0 && k_s0.is_empty() {
            k_s0 = cand;
        } else if sh == 1 && k_s1.is_empty() {
            k_s1 = cand;
        }
        if !k_s0.is_empty() && !k_s1.is_empty() {
            break;
        }
    }
    assert_eq!(
        resp_cmd(&mut c1, &["SET", &k_s0, "cross_val", "PX", "60000"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c1, &["COPY", &k_s0, &k_s1, "DB", "1"]),
        ":1\r\n"
    );
    assert_eq!(resp_cmd(&mut c2, &["GET", &k_s1]), "$9\r\ncross_val\r\n");
    assert_eq!(
        resp_cmd(&mut c1, &["COPY", &k_s0, &k_s1, "DB", "1"]),
        ":0\r\n"
    );
    assert_eq!(resp_cmd(&mut c1, &["SET", &k_s0, "new_val"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c1, &["COPY", &k_s0, &k_s1, "DB", "1", "REPLACE"]),
        ":1\r\n"
    );
    assert_eq!(resp_cmd(&mut c2, &["GET", &k_s1]), "$7\r\nnew_val\r\n");

    // 5. SWAPDB swaps across all shards
    assert_eq!(resp_cmd(&mut c1, &["SWAPDB", "0", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c1, &["GET", &k_s1]), "$7\r\nnew_val\r\n");
    assert_eq!(resp_cmd(&mut c2, &["GET", &k_s0]), "$7\r\nnew_val\r\n");
    assert_eq!(resp_cmd(&mut c1, &["SWAPDB", "0", "1"]), "+OK\r\n");

    // 6. WATCH per-DB isolation
    assert_eq!(resp_cmd(&mut c1, &["SET", "watch_k", "v0"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c1, &["WATCH", "watch_k"]), "+OK\r\n");
    // Modifying watch_k or FLUSHDB in DB 1 must NOT abort c1's transaction in DB 0.
    assert_eq!(resp_cmd(&mut c2, &["SET", "watch_k", "v1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c2, &["FLUSHDB"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c1, &["MULTI"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c1, &["SET", "watch_k", "v0_tx"]),
        "+QUEUED\r\n"
    );
    assert_eq!(resp_cmd(&mut c1, &["EXEC"]), "*1\r\n+OK\r\n");
    assert_eq!(resp_cmd(&mut c1, &["GET", "watch_k"]), "$5\r\nv0_tx\r\n");

    // SWAPDB 0 1 DOES taint WATCH on DB 0.
    assert_eq!(resp_cmd(&mut c1, &["WATCH", "watch_k"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c2, &["SWAPDB", "0", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c1, &["MULTI"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c1, &["SET", "watch_k", "aborted"]),
        "+QUEUED\r\n"
    );
    assert_eq!(resp_cmd(&mut c1, &["EXEC"]), "*-1\r\n");
    assert_eq!(resp_cmd(&mut c2, &["SWAPDB", "0", "1"]), "+OK\r\n");

    // 7. Lua EVAL with redis.call('SELECT', 3) restores caller's DB afterwards
    assert_eq!(
        resp_cmd(
            &mut c1,
            &[
                "EVAL",
                "redis.call('SELECT', 3); redis.call('SET', KEYS[1], 'ok3'); return redis.call('GET', KEYS[1])",
                "1",
                "lua_db3"
            ]
        ),
        "$3\r\nok3\r\n"
    );
    assert!(resp_cmd(&mut c1, &["CLIENT", "INFO"]).contains("db=0"));
    assert_eq!(resp_cmd(&mut c1, &["GET", "lua_db3"]), "$-1\r\n");
    assert_eq!(resp_cmd(&mut c2, &["SELECT", "3"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c2, &["GET", "lua_db3"]), "$3\r\nok3\r\n");

    // 8. BLPOP per-DB isolation
    assert_eq!(resp_cmd(&mut c2, &["SELECT", "2"]), "+OK\r\n");
    c2.write_all(&format_resp_cmd(&["BLPOP", "blk_list", "2"]))
        .unwrap();
    thread::sleep(Duration::from_millis(50));
    // LPUSH in DB 0 does NOT wake c2 in DB 2.
    assert_eq!(
        resp_cmd(&mut c1, &["LPUSH", "blk_list", "from_db0"]),
        ":1\r\n"
    );
    assert_eq!(resp_cmd(&mut c1, &["SELECT", "2"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c1, &["LPUSH", "blk_list", "from_db2"]),
        ":1\r\n"
    );
    let mut buf = [0u8; 256];
    let n = c2.read(&mut buf).unwrap();
    let reply = String::from_utf8_lossy(&buf[..n]);
    assert!(reply.contains("from_db2"), "{reply:?}");
    assert_eq!(resp_cmd(&mut c1, &["SELECT", "0"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c1, &["LPOP", "blk_list"]),
        "$8\r\nfrom_db0\r\n"
    );
}

/// A script that never returns occupies its shard's thread. Past
/// `busy-reply-threshold` other clients get -BUSY, and SCRIPT KILL from a
/// connection on another shard ends it (FUNCTION KILL can't).
#[test]
fn test_script_kill_ends_endless_script_e2e() {
    let port = 29721;
    start_test_server(port, 4);
    let shard_of = |c: &mut TcpStream| -> u64 {
        let id = resp_cmd(c, &["CLIENT", "ID"]);
        id.trim_start_matches(':').trim().parse::<u64>().unwrap() >> 48
    };
    let mut script_conn = TcpStream::connect(("127.0.0.1", port)).unwrap();
    script_conn
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let script_shard = shard_of(&mut script_conn);
    // An admin connection served by another shard thread.
    let mut admin = loop {
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        if shard_of(&mut c) != script_shard {
            break c;
        }
    };
    assert_eq!(
        resp_cmd(&mut admin, &["SCRIPT", "KILL"]),
        "-NOTBUSY No scripts in execution right now.\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut admin,
            &["CONFIG", "SET", "busy-reply-threshold", "100"]
        ),
        "+OK\r\n"
    );

    script_conn
        .write_all(b"*3\r\n$4\r\nEVAL\r\n$17\r\nwhile true do end\r\n$1\r\n0\r\n")
        .unwrap();
    // PING: a command whose key lives on the busy shard may just wait for
    // the script instead (a fast path can route it there directly).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let r = resp_cmd(&mut admin, &["PING"]);
        if r.starts_with("-BUSY") {
            assert!(r.contains("SCRIPT KILL"), "{r}");
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never got -BUSY: {r}");
        thread::sleep(Duration::from_millis(20));
    }
    assert!(resp_cmd(&mut admin, &["FUNCTION", "KILL"]).starts_with("-BUSY"));
    assert_eq!(resp_cmd(&mut admin, &["SCRIPT", "KILL"]), "+OK\r\n");

    let mut buf = [0u8; 512];
    let n = script_conn.read(&mut buf).unwrap();
    let reply = String::from_utf8_lossy(&buf[..n]);
    assert!(
        reply.starts_with("-ERR") && reply.contains("killed by user"),
        "{reply}"
    );
    // Both shards serve again.
    assert_eq!(resp_cmd(&mut script_conn, &["PING"]), "+PONG\r\n");
    assert_eq!(resp_cmd(&mut admin, &["GET", "somekey"]), "$-1\r\n");
    assert_eq!(
        resp_cmd(
            &mut admin,
            &["CONFIG", "SET", "busy-reply-threshold", "5000"]
        ),
        "+OK\r\n"
    );
}
