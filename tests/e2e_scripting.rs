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

/// Server on `port` with `threads` shards, its busy-reply-threshold at
/// 100 ms, and a connection to it.
fn busy_script_server(port: u16, threads: &str) -> (KillRudisOnDrop, TcpStream) {
    let ps = port.to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        threads,
        "--no-pin",
        "--enable-debug-command",
        "yes",
    ];
    let srv = KillRudisOnDrop(spawn_rudis_listening(&args, port));
    let c = connect_busy(port, 5);
    let mut c2 = c.try_clone().unwrap();
    assert_eq!(
        resp_cmd(&mut c2, &["CONFIG", "SET", "busy-reply-threshold", "100"]),
        "+OK\r\n"
    );
    (srv, c)
}

fn connect_busy(port: u16, timeout_s: u64) -> TcpStream {
    let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(timeout_s)))
        .unwrap();
    c
}

/// Sends `args` without waiting for the reply.
fn send_cmd(c: &mut TcpStream, args: &[&str]) {
    c.write_all(&format_resp_cmd(args)).unwrap();
}

/// Waits (up to 5 s) until a fresh connection gets -BUSY for PING. Not
/// before the script is past its 100 ms threshold: until then it runs
/// without handing its shard a turn (as in Redis), and a PING to that shard
/// would wait for the whole script.
fn wait_busy(port: u16) {
    thread::sleep(Duration::from_millis(300));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut c = connect_busy(port, 2);
        if resp_cmd(&mut c, &["PING"]).starts_with("-BUSY") {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server never reported BUSY"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// A script past busy-reply-threshold doesn't freeze its shard: every
/// connection (on its shard too, and with one thread: all of them) gets
/// -BUSY at once, SCRIPT KILL from any of them ends it, and the server then
/// serves normally. Before, connections on the script's shard got no reply
/// at all and, with --threads 1, only kill -9 ended a runaway script. Also
/// with the script's key on another shard than its connection.
#[test]
fn test_runaway_script_busy_and_killable_from_any_connection_e2e() {
    for (port, threads) in [(20300u16, "1"), (20301u16, "4")] {
        for key in ["k1", "k2", "k3", "k4"] {
            let (mut srv, mut c) = busy_script_server(port, threads);
            assert_eq!(resp_cmd(&mut c, &["SET", key, "v"]), "+OK\r\n");
            let mut runner = connect_busy(port, 10);
            send_cmd(
                &mut runner,
                &[
                    "EVAL",
                    "redis.call('GET', KEYS[1]) while true do end",
                    "1",
                    key,
                ],
            );
            wait_busy(port);
            for _ in 0..8 {
                let mut o = connect_busy(port, 2);
                let t0 = std::time::Instant::now();
                let r = resp_cmd(&mut o, &["GET", key]);
                assert!(
                    r.starts_with("-BUSY Redis is busy running a script"),
                    "{threads}/{key}: {r}"
                );
                assert!(
                    t0.elapsed() < Duration::from_secs(1),
                    "{threads}/{key}: slow -BUSY"
                );
            }
            let mut k = connect_busy(port, 5);
            assert_eq!(
                resp_cmd(&mut k, &["SCRIPT", "KILL"]),
                "+OK\r\n",
                "{threads}/{key}"
            );
            let r = read_full_reply(&mut runner);
            assert!(r.contains("Script killed by user with SCRIPT KILL"), "{r}");
            assert_eq!(resp_cmd(&mut k, &["PING"]), "+PONG\r\n");
            assert_eq!(resp_cmd(&mut k, &["GET", key]), "$1\r\nv\r\n");
            shutdown_and_wait(port, &mut srv.0);
        }
    }
}

/// A runaway script that already wrote can't be killed (UNKILLABLE), and
/// SHUTDOWN NOSAVE from another connection, on its shard too, stops the
/// server, as in Redis.
#[test]
fn test_runaway_script_that_wrote_unkillable_shutdown_nosave_e2e() {
    for (port, threads) in [(20302u16, "1"), (20303u16, "4")] {
        let (mut srv, _c) = busy_script_server(port, threads);
        let mut runner = connect_busy(port, 10);
        send_cmd(
            &mut runner,
            &[
                "EVAL",
                "redis.call('SET', KEYS[1], 'x') while true do end",
                "1",
                "w",
            ],
        );
        wait_busy(port);
        let mut k = connect_busy(port, 5);
        let r = resp_cmd(&mut k, &["SCRIPT", "KILL"]);
        assert!(r.starts_with("-UNKILLABLE"), "{threads}: {r}");
        send_cmd(&mut k, &["SHUTDOWN", "NOSAVE"]);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(Some(_)) = srv.0.try_wait() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{threads}: server did not stop"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
}

/// FUNCTION KILL ends a looping function the same way, and the -BUSY reply
/// names FUNCTION KILL.
#[test]
fn test_runaway_function_busy_and_function_kill_e2e() {
    let port = 20304u16;
    let (mut srv, mut c) = busy_script_server(port, "1");
    let lib = "#!lua name=spin\nredis.register_function('spin', function() while true do end end)";
    assert_eq!(
        resp_cmd(&mut c, &["FUNCTION", "LOAD", lib]),
        "$4\r\nspin\r\n"
    );
    let mut runner = connect_busy(port, 10);
    send_cmd(&mut runner, &["FCALL", "spin", "0"]);
    wait_busy(port);
    let mut o = connect_busy(port, 2);
    let r = resp_cmd(&mut o, &["PING"]);
    assert!(
        r.contains("You can only call FUNCTION KILL or SHUTDOWN NOSAVE"),
        "{r}"
    );
    assert_eq!(resp_cmd(&mut o, &["FUNCTION", "KILL"]), "+OK\r\n");
    let r = read_full_reply(&mut runner);
    assert!(
        r.contains("Script killed by user with FUNCTION KILL"),
        "{r}"
    );
    assert_eq!(resp_cmd(&mut o, &["PING"]), "+PONG\r\n");
    shutdown_and_wait(port, &mut srv.0);
}

/// A script past its time limit still looks atomic: while it runs (and
/// hands its shard turns), a write from another connection to a key it
/// reads gets -BUSY and is not seen by the script; the script reads the
/// same value before and after.
#[test]
fn test_slow_script_stays_atomic_e2e() {
    for (port, threads) in [(20305u16, "1"), (20306u16, "4")] {
        let (mut srv, mut c) = busy_script_server(port, threads);
        assert_eq!(resp_cmd(&mut c, &["SET", "a", "before"]), "+OK\r\n");
        let script = "local first = redis.call('GET', KEYS[1]) \
                      local t = redis.call('TIME') local start = t[1] * 1000000 + t[2] \
                      repeat t = redis.call('TIME') until t[1] * 1000000 + t[2] - start > 800000 \
                      return {first, redis.call('GET', KEYS[1])}";
        let mut runner = connect_busy(port, 10);
        send_cmd(&mut runner, &["EVAL", script, "1", "a"]);
        wait_busy(port);
        let mut w = connect_busy(port, 2);
        let r = resp_cmd(&mut w, &["SET", "a", "during"]);
        assert!(r.starts_with("-BUSY"), "{threads}: {r}");
        let r = read_full_reply(&mut runner);
        assert_eq!(r, "*2\r\n$6\r\nbefore\r\n$6\r\nbefore\r\n", "{threads}");
        assert_eq!(resp_cmd(&mut w, &["GET", "a"]), "$6\r\nbefore\r\n");
        shutdown_and_wait(port, &mut srv.0);
    }
}

/// FUNCTION STATS names the function running past its threshold, with its
/// FCALL command and run time, as FUNCTION KILL users look for it. Before,
/// `running_script` was always null.
#[test]
fn test_function_stats_shows_running_function_e2e() {
    let port = 20307u16;
    let (mut srv, mut c) = busy_script_server(port, "1");
    let lib = "#!lua name=spin\nredis.register_function('spin', function() while true do end end)";
    assert_eq!(
        resp_cmd(&mut c, &["FUNCTION", "LOAD", lib]),
        "$4\r\nspin\r\n"
    );
    assert!(
        resp_cmd(&mut c, &["FUNCTION", "STATS"])
            .starts_with("*4\r\n$14\r\nrunning_script\r\n$-1\r\n")
    );
    let mut runner = connect_busy(port, 10);
    send_cmd(&mut runner, &["FCALL", "spin", "0", "arg1"]);
    wait_busy(port);
    let mut o = connect_busy(port, 2);
    let r = read_full_reply({
        send_cmd(&mut o, &["FUNCTION", "STATS"]);
        &mut o
    });
    assert!(
        r.to_ascii_lowercase().starts_with(
            "*4\r\n$14\r\nrunning_script\r\n*6\r\n$4\r\nname\r\n$4\r\nspin\r\n\
             $7\r\ncommand\r\n*4\r\n$5\r\nfcall\r\n$4\r\nspin\r\n$1\r\n0\r\n$4\r\narg1\r\n\
             $11\r\nduration_ms\r\n:"
        ),
        "{r}"
    );
    assert_eq!(resp_cmd(&mut o, &["FUNCTION", "KILL"]), "+OK\r\n");
    read_full_reply(&mut runner);
    shutdown_and_wait(port, &mut srv.0);
}

/// A script looping on slow `redis.call`s is flagged busy on time too (the
/// instruction hook alone noticed late), CLIENT LIST works from a script
/// (it returned nil), and a client blocked on a list the script pushes to
/// twice is served once the script is done, with its last push (it popped
/// the first push mid-run). Mirrors Redis' "Timedout scripts and unblocked
/// command".
#[test]
fn test_busy_redis_call_loop_serves_blocked_client_after_script_e2e() {
    for (port, threads) in [(20308u16, "1"), (20309u16, "4")] {
        let (mut srv, mut c) = busy_script_server(port, threads);
        // CLIENT is refused in scripts unless allowed (Redis' test does this).
        assert_eq!(
            resp_cmd(&mut c, &["DEBUG", "set-disable-deny-scripts", "1"]),
            "+OK\r\n"
        );
        let mut blocked = connect_busy(port, 10);
        send_cmd(&mut blocked, &["BLPOP", "x", "0"]);
        thread::sleep(Duration::from_millis(200));
        let script = "local clients \
                      redis.call('lpush', KEYS[1], 'y') \
                      while true do \
                          clients = redis.call('client', 'list') \
                          if string.find(clients, 'abortscript') ~= nil then break end \
                      end \
                      redis.call('lpush', KEYS[1], 'z') \
                      return clients";
        let mut runner = connect_busy(port, 10);
        send_cmd(&mut runner, &["EVAL", script, "1", "x"]);
        wait_busy(port);
        let mut namer = connect_busy(port, 5);
        assert_eq!(
            resp_cmd(&mut namer, &["CLIENT", "SETNAME", "abortscript"]),
            "+OK\r\n"
        );
        assert_eq!(
            read_full_reply(&mut blocked),
            "*2\r\n$1\r\nx\r\n$1\r\nz\r\n",
            "{threads}"
        );
        let r = read_full_reply(&mut runner);
        assert!(r.contains("name=abortscript"), "{threads}: {r}");
        assert_eq!(
            resp_cmd(&mut namer, &["LRANGE", "x", "0", "-1"]),
            "*1\r\n$1\r\ny\r\n"
        );
        shutdown_and_wait(port, &mut srv.0);
    }
}

/// Clients blocked on keys a script changes are served after the script,
/// as in Redis, even for a quick script: a BLPOP sees the list as the
/// script left it, not its first push (which it popped mid-script).
#[test]
fn test_quick_script_serves_blocked_client_after_it_ends_e2e() {
    for (port, threads) in [(20310u16, "1"), (20311u16, "4")] {
        let ps = port.to_string();
        let mut srv = KillRudisOnDrop(spawn_rudis_listening(
            &["--port", &ps, "--threads", threads, "--no-pin"],
            port,
        ));
        let mut blocked = connect_busy(port, 5);
        send_cmd(&mut blocked, &["BLPOP", "x", "0"]);
        thread::sleep(Duration::from_millis(200));
        let mut c = connect_busy(port, 5);
        assert_eq!(
            resp_cmd(
                &mut c,
                &[
                    "EVAL",
                    "redis.call('lpush', KEYS[1], 'y') redis.call('lpush', KEYS[1], 'z')",
                    "1",
                    "x"
                ]
            ),
            "$-1\r\n"
        );
        assert_eq!(
            read_full_reply(&mut blocked),
            "*2\r\n$1\r\nx\r\n$1\r\nz\r\n",
            "{threads}"
        );
        assert_eq!(
            resp_cmd(&mut c, &["LRANGE", "x", "0", "-1"]),
            "*1\r\n$1\r\ny\r\n"
        );
        shutdown_and_wait(port, &mut srv.0);
    }
}

/// Once SHUTDOWN has replied, new clients are refused, as Redis (which
/// exits first) refuses them. Before, the listeners accepted for up to
/// 200 ms more and dropped the connections ("I/O error reading reply").
#[test]
fn test_shutdown_refuses_new_connections_once_replied_e2e() {
    let port = 20312u16;
    let ps = port.to_string();
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(
        &["--port", &ps, "--threads", "2", "--no-pin"],
        port,
    ));
    let mut c = connect_busy(port, 5);
    assert_eq!(resp_cmd(&mut c, &["SHUTDOWN", "NOSAVE"]), "+OK\r\n");
    let r = TcpStream::connect(("127.0.0.1", port));
    assert!(r.is_err(), "connected after SHUTDOWN replied");
    let _ = srv.0.wait();
}

/// Scripts share one Lua state per thread now, so nothing one script does
/// may reach the next: library tables, globals (also via rawset and
/// setmetatable) and redis.setresp are protected or reset per run.
#[test]
fn test_reused_lua_state_does_not_leak_between_scripts_e2e() {
    for (port, threads) in [(20314u16, "1"), (20315u16, "2")] {
        let ps = port.to_string();
        let mut srv = KillRudisOnDrop(spawn_rudis_listening(
            &["--port", &ps, "--threads", threads, "--no-pin"],
            port,
        ));
        let mut c = connect_busy(port, 5);
        for script in [
            "string.rep = nil",
            "table.insert = nil",
            "rawset(_G, 'leak', 1)",
            "rawset(string, 'rep', nil)",
            "setmetatable(string, {})",
            "getmetatable(string).__index = {}",
            "getmetatable('').__index = {}",
            "rawset(getmetatable(''), 'leak', 1)",
        ] {
            let r = resp_cmd(&mut c, &["EVAL", script, "0"]);
            assert!(
                r.contains("Attempt to modify a readonly table"),
                "{threads}: {script}: {r}"
            );
        }
        assert_eq!(
            resp_cmd(&mut c, &["EVAL", "return string.rep('ab', 2)", "0"]),
            "$4\r\nabab\r\n"
        );
        let r = resp_cmd(&mut c, &["EVAL", "return leak", "0"]);
        assert!(r.contains("nonexistent global variable 'leak'"), "{r}");
        // RESP3 set by one script is gone in the next: a map reply comes
        // back as a RESP2 flat array again.
        let resp3 = "redis.setresp(3) return redis.call('HGETALL', KEYS[1])";
        let resp2 = "return redis.call('HGETALL', KEYS[1])";
        assert_eq!(resp_cmd(&mut c, &["HSET", "h", "f", "v"]), ":1\r\n");
        resp_cmd(&mut c, &["EVAL", resp3, "1", "h"]);
        assert_eq!(
            resp_cmd(&mut c, &["EVAL", resp2, "1", "h"]),
            "*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
        );
        // KEYS / ARGV are each run's own.
        assert_eq!(
            resp_cmd(
                &mut c,
                &["EVAL", "return {KEYS[1], ARGV[1]}", "1", "h", "a1"]
            ),
            "*2\r\n$1\r\nh\r\n$2\r\na1\r\n"
        );
        assert_eq!(
            resp_cmd(&mut c, &["EVAL", "return {#KEYS, #ARGV}", "0"]),
            "*2\r\n:0\r\n:0\r\n"
        );
        shutdown_and_wait(port, &mut srv.0);
    }
}

/// FCALL runs a library's current code: a library replaced by FUNCTION
/// LOAD REPLACE, deleted, or restored is not served from a stale copy in
/// a thread's reused Lua state; and a killed script leaves the state fit
/// for the next one.
#[test]
fn test_reused_lua_state_follows_library_changes_and_kills_e2e() {
    let port = 20316u16;
    let (mut srv, mut c) = busy_script_server(port, "1");
    let v1 = "#!lua name=lib\nredis.register_function('f', function() return 1 end)";
    let v2 = "#!lua name=lib\nredis.register_function('f', function() return 2 end)";
    assert_eq!(resp_cmd(&mut c, &["FUNCTION", "LOAD", v1]), "$3\r\nlib\r\n");
    assert_eq!(resp_cmd(&mut c, &["FCALL", "f", "0"]), ":1\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["FUNCTION", "LOAD", "REPLACE", v2]),
        "$3\r\nlib\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["FCALL", "f", "0"]), ":2\r\n");
    assert_eq!(resp_cmd(&mut c, &["FUNCTION", "DELETE", "lib"]), "+OK\r\n");
    assert!(resp_cmd(&mut c, &["FCALL", "f", "0"]).starts_with("-ERR Function not found"));
    assert_eq!(resp_cmd(&mut c, &["FUNCTION", "LOAD", v1]), "$3\r\nlib\r\n");
    assert_eq!(resp_cmd(&mut c, &["FCALL", "f", "0"]), ":1\r\n");

    let mut runner = connect_busy(port, 10);
    send_cmd(&mut runner, &["EVAL", "while true do end", "0"]);
    wait_busy(port);
    let mut k = connect_busy(port, 5);
    assert_eq!(resp_cmd(&mut k, &["SCRIPT", "KILL"]), "+OK\r\n");
    read_full_reply(&mut runner);
    assert_eq!(resp_cmd(&mut k, &["EVAL", "return 7", "0"]), ":7\r\n");
    assert_eq!(resp_cmd(&mut k, &["FCALL", "f", "0"]), ":1\r\n");
    shutdown_and_wait(port, &mut srv.0);
}

/// A script flagged `allow-oom` (a function's flag, or EVAL's shebang)
/// may write over maxmemory, as in Redis. Before, its redis.call writes
/// still got -OOM.
#[test]
fn test_allow_oom_script_writes_over_maxmemory_e2e() {
    let port = 20317u16;
    let ps = port.to_string();
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(
        &["--port", &ps, "--threads", "1", "--no-pin"],
        port,
    ));
    let mut c = connect_busy(port, 5);
    let lib = "#!lua name=f1\nredis.register_function{function_name='f1', \
               callback=function() return redis.call('set', 'x', '1') end, flags={'allow-oom'}}";
    assert_eq!(resp_cmd(&mut c, &["FUNCTION", "LOAD", lib]), "$2\r\nf1\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "1"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["FCALL", "f1", "1", "x"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(
            &mut c,
            &[
                "EVAL",
                "#!lua flags=allow-oom\nreturn redis.call('set', KEYS[1], '2')",
                "1",
                "y"
            ]
        ),
        "+OK\r\n"
    );
    let r = resp_cmd(
        &mut c,
        &["EVAL", "return redis.call('set', KEYS[1], '3')", "1", "z"],
    );
    assert!(r.contains("OOM"), "{r}");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "0"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["GET", "y"]), "$1\r\n2\r\n");
    shutdown_and_wait(port, &mut srv.0);
}

/// Over maxmemory, FCALL of a function that may write (neither
/// `no-writes` nor `allow-oom`) is refused up front with -OOM, as in
/// Redis, instead of starting and failing at its first write with a
/// script runtime error. A `no-writes` function still runs.
#[test]
fn test_fcall_of_writing_function_refused_over_maxmemory_e2e() {
    let port = 20318u16;
    let ps = port.to_string();
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(
        &["--port", &ps, "--threads", "1", "--no-pin"],
        port,
    ));
    let mut c = connect_busy(port, 5);
    let lib = "#!lua name=t\n\
               redis.register_function('w', function() return redis.call('set', 'x', '1') end)\n\
               redis.register_function{function_name='r', callback=function() return 'hello' end, flags={'no-writes'}}";
    assert_eq!(resp_cmd(&mut c, &["FUNCTION", "LOAD", lib]), "$1\r\nt\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "1"]),
        "+OK\r\n"
    );
    let r = resp_cmd(&mut c, &["FCALL", "w", "1", "x"]);
    assert!(
        r.starts_with("-OOM command not allowed when used memory > 'maxmemory'"),
        "{r}"
    );
    assert_eq!(
        resp_cmd(&mut c, &["FCALL", "r", "1", "x"]),
        "$5\r\nhello\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["FCALL_RO", "r", "1", "x"]),
        "$5\r\nhello\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "0"]),
        "+OK\r\n"
    );
    shutdown_and_wait(port, &mut srv.0);
}

/// On a replica whose master link is down with replica-serve-stale-data
/// no, a script runs only if flagged allow-stale (function flag or EVAL /
/// EVALSHA shebang), as in Redis; inside it, commands not flagged stale
/// get "Can not execute the command on a stale replica" while ECHO runs. Before, FCALL and EVAL always ran there, reading stale data.
#[test]
fn test_scripts_on_stale_replica_need_allow_stale_e2e() {
    let port = 20319u16;
    let ps = port.to_string();
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(
        &["--port", &ps, "--threads", "2", "--no-pin"],
        port,
    ));
    let mut c = connect_busy(port, 5);
    let lib = "#!lua name=t\n\
        redis.register_function{function_name='f1', callback=function() return 'hello' end, flags={'no-writes'}}\n\
        redis.register_function{function_name='f2', callback=function() return 'hello' end, flags={'allow-stale', 'no-writes'}}\n\
        redis.register_function{function_name='f3', callback=function() return redis.call('get', 'x') end, flags={'allow-stale', 'no-writes'}}\n\
        redis.register_function{function_name='f4', callback=function() return redis.call('echo', 'foobar') end, flags={'allow-stale', 'no-writes'}}";
    assert_eq!(resp_cmd(&mut c, &["FUNCTION", "LOAD", lib]), "$1\r\nt\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "replica-serve-stale-data", "no"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["REPLICAOF", "127.0.0.1", "1"]),
        "+OK\r\n"
    );
    assert!(resp_cmd(&mut c, &["FCALL", "f1", "0"]).starts_with("-MASTERDOWN"));
    assert_eq!(resp_cmd(&mut c, &["FCALL", "f2", "0"]), "$5\r\nhello\r\n");
    assert!(
        resp_cmd(&mut c, &["FCALL", "f3", "1", "x"])
            .contains("Can not execute the command on a stale replica")
    );
    assert_eq!(resp_cmd(&mut c, &["FCALL", "f4", "0"]), "$6\r\nfoobar\r\n");
    assert!(resp_cmd(&mut c, &["EVAL", "return 1", "0"]).starts_with("-MASTERDOWN"));
    let stale_ok = "#!lua flags=allow-stale,no-writes\nreturn redis.call('echo','foobar')";
    assert_eq!(
        resp_cmd(&mut c, &["EVAL", stale_ok, "0"]),
        "$6\r\nfoobar\r\n"
    );
    let sha = resp_cmd(&mut c, &["SCRIPT", "LOAD", stale_ok]);
    let sha = sha.lines().nth(1).unwrap().to_string();
    assert_eq!(
        resp_cmd(&mut c, &["EVALSHA", &sha, "0"]),
        "$6\r\nfoobar\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["FCALL", "f1", "0"]), "$5\r\nhello\r\n");
    shutdown_and_wait(port, &mut srv.0);
}

/// A runaway script sent pipelined with other commands is suspended past
/// its threshold like one sent alone: other connections get -BUSY and
/// SCRIPT KILL ends it; the commands after it in the pipeline then run.
#[test]
fn test_runaway_script_in_pipeline_busy_and_killable_e2e() {
    for (port, threads) in [(20320u16, "1"), (20321u16, "4")] {
        let (mut srv, _c) = busy_script_server(port, threads);
        let mut runner = connect_busy(port, 10);
        let mut req = format_resp_cmd(&["SET", "a", "1"]);
        req.extend(format_resp_cmd(&["EVAL", "while true do end", "1", "k"]));
        req.extend(format_resp_cmd(&["SET", "b", "2"]));
        runner.write_all(&req).unwrap();
        wait_busy(port);
        let mut k = connect_busy(port, 5);
        assert_eq!(resp_cmd(&mut k, &["SCRIPT", "KILL"]), "+OK\r\n");
        let mut got = String::new();
        let mut buf = [0u8; 1024];
        while !got.ends_with("+OK\r\n-ERR Script killed by user with SCRIPT KILL...\r\n+OK\r\n") {
            let n = runner.read(&mut buf).unwrap();
            assert!(n > 0, "{threads}: closed after {got:?}");
            got.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
        assert_eq!(resp_cmd(&mut k, &["GET", "b"]), "$1\r\n2\r\n");
        shutdown_and_wait(port, &mut srv.0);
    }
}

/// Redis' noscript commands (CLIENT, CONFIG, MULTI, SUBSCRIBE, EVAL, ...)
/// are refused in scripts unless DEBUG set-disable-deny-scripts 1. Before,
/// rudis refused only a handful (SHUTDOWN, SAVE, ...) and let scripts
/// change connection or server state.
#[test]
fn test_noscript_commands_refused_in_scripts_e2e() {
    let port = 20323u16;
    let (mut srv, mut c) = busy_script_server(port, "1");
    for call in [
        "redis.call('client', 'list')",
        "redis.call('config', 'set', 'maxclients', '100')",
        "redis.call('multi')",
        "redis.call('eval', 'return 1', '0')",
        "redis.call('subscribe', 'ch')",
    ] {
        let r = resp_cmd(&mut c, &["EVAL", &format!("return {call}"), "0"]);
        assert!(
            r.contains("This Redis command is not allowed from script"),
            "{call}: {r}"
        );
    }
    let r = resp_cmd(
        &mut c,
        &["EVAL", "return redis.pcall('client', 'list')", "0"],
    );
    assert!(
        r.contains("This Redis command is not allowed from script"),
        "{r}"
    );
    assert_eq!(
        resp_cmd(&mut c, &["EVAL", "return redis.call('echo', 'x')", "0"]),
        "$1\r\nx\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["DEBUG", "set-disable-deny-scripts", "1"]),
        "+OK\r\n"
    );
    let r = resp_cmd(
        &mut c,
        &["EVAL", "return redis.call('client', 'list')", "0"],
    );
    assert!(r.contains("id="), "{r}");
    assert_eq!(
        resp_cmd(&mut c, &["DEBUG", "set-disable-deny-scripts", "0"]),
        "+OK\r\n"
    );
    shutdown_and_wait(port, &mut srv.0);
}
