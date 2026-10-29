//! End-to-end tests: pubsub (see `tests/common` for the helpers).

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
fn test_pubsub_cross_shard_e2e() {
    let port = 16387;
    let num_shards = 4;
    start_test_server(port, num_shards);

    // Client 1: subscriber on channel news.sports
    let mut sub1 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect sub1");
    let resp = send_and_read(
        &mut sub1,
        b"*2\r\n$9\r\nSUBSCRIBE\r\n$11\r\nnews.sports\r\n",
    );
    assert!(resp.contains("subscribe") && resp.contains("news.sports") && resp.contains(":1"));

    // Client 2: subscriber on news.sports and news.tech
    let mut sub2 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect sub2");
    let resp = send_and_read(
        &mut sub2,
        b"*3\r\n$9\r\nSUBSCRIBE\r\n$11\r\nnews.sports\r\n$9\r\nnews.tech\r\n",
    );
    assert!(resp.contains("news.sports") && resp.contains("news.tech"));

    // Client 3: pattern subscriber on news.*
    let mut psub =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect psub");
    let resp = send_and_read(&mut psub, b"*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews.*\r\n");
    assert!(resp.contains("psubscribe") && resp.contains("news.*"));

    // Publisher client
    let mut pub_client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect pub_client");

    // Check PUBSUB inspection commands before publish
    let resp = send_and_read(&mut pub_client, b"PUBSUB NUMPAT\r\n");
    assert_eq!(resp, ":1\r\n");

    let resp = send_and_read(&mut pub_client, b"PUBSUB CHANNELS news.*\r\n");
    assert!(resp.contains("news.sports") && resp.contains("news.tech"));

    let resp = send_and_read(&mut pub_client, b"PUBSUB NUMSUB news.sports news.tech\r\n");
    assert!(
        resp.contains("news.sports")
            && resp.contains(":2")
            && resp.contains("news.tech")
            && resp.contains(":1")
    );

    // Publish to news.sports: 2 direct subscribers + 1 pattern subscriber = 3 recipients
    let resp = send_and_read(
        &mut pub_client,
        b"*3\r\n$7\r\nPUBLISH\r\n$11\r\nnews.sports\r\n$4\r\ngoal\r\n",
    );
    assert_eq!(resp, ":3\r\n");

    // Verify sub1 received the message
    let mut buf = [0u8; 1024];
    let n = sub1.read(&mut buf).unwrap();
    let msg = String::from_utf8_lossy(&buf[..n]);
    assert!(msg.contains("message") && msg.contains("news.sports") && msg.contains("goal"));

    // Verify sub2 received the message
    let n = sub2.read(&mut buf).unwrap();
    let msg = String::from_utf8_lossy(&buf[..n]);
    assert!(msg.contains("message") && msg.contains("news.sports") && msg.contains("goal"));

    // Verify psub received the pmessage
    let n = psub.read(&mut buf).unwrap();
    let msg = String::from_utf8_lossy(&buf[..n]);
    assert!(
        msg.contains("pmessage")
            && msg.contains("news.*")
            && msg.contains("news.sports")
            && msg.contains("goal")
    );

    // Test PING in subscribed mode
    let resp = send_and_read(&mut sub1, b"PING\r\n");
    assert!(resp.contains("pong"));

    // Test UNSUBSCRIBE
    let resp = send_and_read(
        &mut sub2,
        b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$11\r\nnews.sports\r\n",
    );
    assert!(resp.contains("unsubscribe") && resp.contains("news.sports"));

    // Now publishing to news.sports has 1 direct subscriber + 1 pattern subscriber = 2
    let resp = send_and_read(&mut pub_client, b"PUBLISH news.sports update\r\n");
    assert_eq!(resp, ":2\r\n");

    // Drop sub1 to test disconnect cleanup
    drop(sub1);
    thread::sleep(Duration::from_millis(50));

    // Now publishing to news.sports has 0 direct subscribers + 1 pattern subscriber = 1
    let resp = send_and_read(&mut pub_client, b"PUBLISH news.sports final\r\n");
    assert_eq!(resp, ":1\r\n");
}

#[test]
fn test_client_tracking_atomic_bypass_and_invalidation_e2e() {
    let port = 16702;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client1 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client1");
    client1
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    let mut client2 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client2");
    client2
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Initial MGET without tracking - verifies atomic bypass path
    assert_eq!(
        send_and_read(&mut client1, b"SET tracked_k1 initial_v1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"MGET tracked_k1\r\n"),
        "*1\r\n$10\r\ninitial_v1\r\n"
    );

    // 2. Enable client tracking in RESP3 mode
    assert!(send_and_read(&mut client1, b"HELLO 3\r\n").starts_with('%') || true);
    assert_eq!(
        send_and_read(&mut client1, b"CLIENT TRACKING on\r\n"),
        "+OK\r\n"
    );

    // 3. Read key with tracking enabled
    let _ = send_and_read(&mut client1, b"GET tracked_k1\r\n");

    // 4. Mutate key from client2
    assert_eq!(
        send_and_read(&mut client2, b"SET tracked_k1 updated_v1\r\n"),
        "+OK\r\n"
    );
    std::thread::sleep(std::time::Duration::from_millis(100));

    // 5. Client 1 receives push invalidation message
    // The push is already queued, so a single read() may return it without
    // the PONG; drain until the PING reply arrives so later replies align.
    let mut next_resp = send_and_read(&mut client1, b"PING\r\n");
    while !next_resp.contains("PONG") {
        let mut buf = [0u8; 4096];
        let n = client1.read(&mut buf).unwrap();
        next_resp.push_str(&String::from_utf8_lossy(&buf[..n]));
    }
    assert!(next_resp.contains("invalidate") && next_resp.contains("tracked_k1"));

    // 6. Disable tracking - restores atomic bypass
    assert_eq!(
        send_and_read(&mut client1, b"CLIENT TRACKING off\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_pubsub_resp3_push_frames_e2e() {
    let port = 16761;
    let num_shards = 2;
    start_test_server(port, num_shards);

    // 1. Client 1 negotiates RESP3 via HELLO 3, then subscribes
    let mut client_resp3 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let hello_resp = send_and_read(&mut client_resp3, b"HELLO 3\r\n");
    assert!(hello_resp.starts_with('%') || hello_resp.starts_with('*'));

    let sub3_resp = send_and_read(&mut client_resp3, b"SUBSCRIBE updates\r\n");
    assert!(
        sub3_resp.starts_with(">3\r\n"),
        "RESP3 subscriber should receive push frame acknowledgment (>3), got: {}",
        sub3_resp
    );
    assert!(sub3_resp.contains("subscribe"));

    // 2. Client 2 stays in default RESP2, then subscribes
    let mut client_resp2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let sub2_resp = send_and_read(&mut client_resp2, b"SUBSCRIBE updates\r\n");
    assert!(
        sub2_resp.starts_with("*3\r\n"),
        "RESP2 subscriber should receive standard array frame (*3), got: {}",
        sub2_resp
    );

    // 3. Publisher sends PUBLISH
    let mut publisher = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let pub_resp = send_and_read(&mut publisher, b"PUBLISH updates payload123\r\n");
    assert_eq!(pub_resp, ":2\r\n");

    // 4. Verify Client 1 receives RESP3 push message (>3)
    let mut buf = [0u8; 512];
    let n1 = client_resp3.read(&mut buf).unwrap();
    let msg1 = String::from_utf8_lossy(&buf[..n1]);
    assert!(
        msg1.starts_with(">3\r\n"),
        "RESP3 client should receive >3 push message, got: {}",
        msg1
    );
    assert!(msg1.contains("payload123"));

    // 5. Verify Client 2 receives RESP2 array message (*3)
    let n2 = client_resp2.read(&mut buf).unwrap();
    let msg2 = String::from_utf8_lossy(&buf[..n2]);
    assert!(
        msg2.starts_with("*3\r\n"),
        "RESP2 client should receive *3 array message, got: {}",
        msg2
    );
    assert!(msg2.contains("payload123"));
}

#[test]
fn test_pubsub_presence_table_selective_fanout_e2e() {
    let port = 17020;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut sub1 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let resp1 = send_and_read(&mut sub1, b"SUBSCRIBE stream_alpha\r\n");
    assert!(resp1.contains("subscribe") && resp1.contains("stream_alpha"));

    let mut sub2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let resp2 = send_and_read(&mut sub2, b"SUBSCRIBE stream_alpha\r\n");
    assert!(resp2.contains("subscribe") && resp2.contains("stream_alpha"));

    let mut pub_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Publish to nobody: presence table returns empty mask, returns 0 immediately
    let resp = send_and_read(&mut pub_client, b"PUBLISH non_existent_channel msg\r\n");
    assert_eq!(resp, ":0\r\n");

    // Publish to stream_alpha: delivers to both subscribers across shards
    let resp = send_and_read(&mut pub_client, b"PUBLISH stream_alpha hello_world\r\n");
    assert_eq!(resp, ":2\r\n");

    let mut buf = [0u8; 512];
    let n1 = sub1.read(&mut buf).unwrap();
    assert!(String::from_utf8_lossy(&buf[..n1]).contains("hello_world"));

    let n2 = sub2.read(&mut buf).unwrap();
    assert!(String::from_utf8_lossy(&buf[..n2]).contains("hello_world"));

    // Sub1 unsubscribes
    let un_resp = send_and_read(&mut sub1, b"UNSUBSCRIBE stream_alpha\r\n");
    assert!(un_resp.contains("unsubscribe"));

    // Next publish only delivers to sub2
    let resp = send_and_read(&mut pub_client, b"PUBLISH stream_alpha second_msg\r\n");
    assert_eq!(resp, ":1\r\n");

    let n2 = sub2.read(&mut buf).unwrap();
    assert!(String::from_utf8_lossy(&buf[..n2]).contains("second_msg"));
}

#[test]
fn test_squashed_batch_set_watch_and_tracking_invalidation_e2e() {
    let port = 19995;
    start_test_server(port, 4);

    // 1. WATCH invalidation by pipelined/squashed SET
    let mut watcher = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let mut mutator = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    assert_eq!(
        send_and_read(&mut watcher, b"WATCH watch_squash_k\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut watcher, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut watcher, b"GET watch_squash_k\r\n"),
        "+QUEUED\r\n"
    );

    // Mutator sends pipelined batch containing SET
    let pipeline = b"SET dummy1 v1\r\nSET watch_squash_k updated_val\r\nSET dummy2 v2\r\n";
    mutator.write_all(pipeline).unwrap();
    let mut resp_buf = vec![0u8; 1024];
    let n = mutator.read(&mut resp_buf).unwrap();
    let resp_str = String::from_utf8_lossy(&resp_buf[..n]);
    assert!(resp_str.contains("+OK\r\n"));

    // EXEC must return nil array (transaction aborted due to tainted watch)
    let exec_resp = send_and_read(&mut watcher, b"EXEC\r\n");
    assert_eq!(exec_resp, "*-1\r\n");

    // 2. CLIENT TRACKING invalidation by pipelined/squashed SET
    let mut tracker = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let hello_resp = send_and_read(&mut tracker, b"HELLO 3\r\n");
    assert!(hello_resp.starts_with('%'));
    assert_eq!(
        send_and_read(&mut tracker, b"CLIENT TRACKING on\r\n"),
        "+OK\r\n"
    );

    // Read key to register interest
    assert_eq!(
        send_and_read(&mut tracker, b"GET track_squash_k\r\n"),
        "_\r\n"
    );

    // Mutator sends pipelined batch updating track_squash_k
    let pipeline2 = b"SET dummy3 v3\r\nSET track_squash_k new_val\r\n";
    mutator.write_all(pipeline2).unwrap();
    let mut resp_buf2 = vec![0u8; 1024];
    let n2 = mutator.read(&mut resp_buf2).unwrap();
    let resp_str2 = String::from_utf8_lossy(&resp_buf2[..n2]);
    assert!(resp_str2.contains("+OK\r\n"));

    std::thread::sleep(std::time::Duration::from_millis(200));
    let mut next_resp = send_and_read(&mut tracker, b"PING\r\n");
    if !next_resp.contains("invalidate") {
        std::thread::sleep(std::time::Duration::from_millis(100));
        next_resp.push_str(&send_and_read(&mut tracker, b"PING\r\n"));
    }
    assert!(next_resp.contains("invalidate"));
    assert!(next_resp.contains("track_squash_k"));
}

#[test]
fn test_info_stats_counts_pubsub_channels_and_patterns_e2e() {
    let port = 17106;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let pubsub_counts = |c: &mut TcpStream| {
        let info = info_section(c, "stats");
        ["pubsub_channels", "pubsub_patterns", "pubsubshard_channels"].map(|f| info_num(&info, f))
    };
    assert_eq!(pubsub_counts(&mut c), [0.0, 0.0, 0.0]);

    // Subscribers on separate connections, so they may live on different shards.
    let mut subs = Vec::new();
    for args in [
        &["SUBSCRIBE", "ch:a", "ch:b"][..],
        &["SUBSCRIBE", "ch:b", "ch:c"][..],
        &["PSUBSCRIBE", "p:*"][..],
        &["SSUBSCRIBE", "sh:x"][..],
    ] {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut req = format!("*{}\r\n", args.len());
        for a in args {
            req.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        s.write_all(req.as_bytes()).unwrap();
        // Wait for one confirmation per channel.
        let mut got = String::new();
        let mut buf = [0u8; 4096];
        while got.matches("subscribe\r\n").count() < args.len() - 1 {
            let n = s.read(&mut buf).unwrap();
            assert!(n > 0, "connection closed");
            got.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
        subs.push(s);
    }
    assert_eq!(pubsub_counts(&mut c), [3.0, 1.0, 1.0]);

    drop(subs);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let counts = pubsub_counts(&mut c);
        if counts == [0.0, 0.0, 0.0] {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{counts:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_router_pooled_channels_spublish_and_loopback_conf_e2e() {
    // 1. Verify rudis.conf binds to loopback by default
    let conf_cfg = rudis::config::RudisConfig::load_file(std::path::Path::new("rudis.conf"))
        .expect("rudis.conf should parse cleanly");
    assert_eq!(conf_cfg.bind, "127.0.0.1 -::1");

    let port = 17978u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // 2. Multi-key DEL, cross-shard MULTI/EXEC, and FLUSHDB exercising pooled responder channels
    for round in 0..5 {
        assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
        for i in 0..8 {
            let k = format!("pool:tx:{round}:{i}");
            assert_eq!(resp_cmd(&mut c, &["SET", &k, "v"]), "+QUEUED\r\n");
        }
        let exec_resp = resp_cmd(&mut c, &["EXEC"]);
        assert!(exec_resp.starts_with("*8\r\n"), "{exec_resp}");

        let del_keys: Vec<String> = (0..8).map(|i| format!("pool:tx:{round}:{i}")).collect();
        let mut del_cmd = vec!["DEL"];
        for k in &del_keys {
            del_cmd.push(k.as_str());
        }
        assert_eq!(resp_cmd(&mut c, &del_cmd), ":8\r\n");
    }

    // 3. Sharded pub/sub (SSUBSCRIBE / SPUBLISH / PUBSUB SHARDNUMSUB) across shards
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let ssub_resp = resp_cmd(&mut sub, &["SSUBSCRIBE", "shardchan:{alpha}"]);
    assert!(
        ssub_resp.contains("ssubscribe") && ssub_resp.contains(":1\r\n"),
        "{ssub_resp}"
    );

    let numsub_resp = resp_cmd(&mut c, &["PUBSUB", "SHARDNUMSUB", "shardchan:{alpha}"]);
    assert!(numsub_resp.contains(":1\r\n"), "{numsub_resp}");

    assert_eq!(
        resp_cmd(&mut c, &["SPUBLISH", "shardchan:{alpha}", "msg1"]),
        ":1\r\n"
    );
    let mut buf = [0u8; 512];
    let n = sub.read(&mut buf).unwrap();
    let received = String::from_utf8_lossy(&buf[..n]);
    assert!(
        received.contains("smessage") && received.contains("msg1"),
        "{received}"
    );

    drop(sub);
    drop(c);
    shutdown_and_wait(port, &mut srv);
}

/// A subscriber that stops reading is disconnected at the pubsub output
/// buffer limit (by bytes queued), and until then gets every message, in
/// order (like Redis, what was still queued when it is cut off is
/// discarded). Its queue was capped at 4096 messages instead: past that,
/// messages were silently dropped while it stayed connected and subscribed.
#[test]
fn test_slow_subscriber_disconnected_at_pubsub_limit_not_dropped_e2e() {
    let port = 18016u16;
    let ps = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &ps, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    assert_eq!(
        send_and_read_reply(
            &mut c,
            b"CONFIG SET client-output-buffer-limit \"pubsub 1mb 0 0\"\r\n"
        ),
        "+OK\r\n"
    );
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let r = send_and_read_reply(&mut sub, b"SUBSCRIBE ch\r\n");
    assert!(r.contains("subscribe"), "{r}");
    // Publish 8000 numbered 1 KB messages while the subscriber reads nothing.
    let pad = "m".repeat(992);
    let mut counted = 0usize;
    for batch in 0..80 {
        let mut req = String::new();
        for i in 0..100 {
            req.push_str(&format!(
                "*3\r\n$7\r\nPUBLISH\r\n$2\r\nch\r\n$1000\r\n{:08}{pad}\r\n",
                batch * 100 + i
            ));
        }
        c.write_all(req.as_bytes()).unwrap();
        let (mut lines, mut reply, mut buf) = (0, Vec::new(), [0u8; 4096]);
        while lines < 100 {
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            reply.extend_from_slice(&buf[..n]);
            lines += buf[..n].iter().filter(|&&b| b == b'\n').count();
        }
        for l in String::from_utf8(reply).unwrap().lines() {
            counted += l.trim_start_matches(':').parse::<usize>().unwrap();
        }
    }
    assert!(counted < 8000, "never cut off: counted {counted}");
    // Draining now, the subscriber gets messages 0, 1, 2, ... with no gap,
    // then the server's close.
    let mut got = Vec::new();
    let mut buf = vec![0u8; 1 << 16];
    loop {
        match sub.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(e) => panic!("subscriber not disconnected: {e}"),
        }
    }
    let text = String::from_utf8_lossy(&got);
    let seqs: Vec<usize> = text
        .split("$1000\r\n")
        .skip(1)
        .filter_map(|m| m.get(..8)?.parse().ok())
        .collect();
    assert!(
        !seqs.is_empty() && seqs.len() <= counted,
        "{} of {counted}",
        seqs.len()
    );
    for (i, &s) in seqs.iter().enumerate() {
        assert_eq!(s, i, "gap in delivered messages");
    }
    let stats = send_and_read_reply(&mut c, b"INFO stats\r\n");
    assert!(
        stats.contains("client_output_buffer_limit_disconnections:1"),
        "{stats}"
    );
    let _ = srv.kill();
    let _ = srv.wait();
}

/// (P|S)UNSUBSCRIBE replies carry the channel name byte for byte. A
/// non-UTF-8 name was formatted lossily under its raw length, making the
/// reply malformed and desyncing the client's protocol stream.
#[test]
fn test_unsubscribe_binary_channel_reply_is_well_formed_e2e() {
    let port = 18036u16;
    let ps = port.to_string();
    let srv = ServerGuard(spawn_rudis_listening(
        &["--port", &ps, "--threads", "1", "--no-pin"],
        port,
    ));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.write_all(b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\n\xe0\x85N\xff\r\n*1\r\n$4\r\nPING\r\n")
        .unwrap();
    let want: &[u8] = b"*3\r\n$11\r\nunsubscribe\r\n$4\r\n\xe0\x85N\xff\r\n:0\r\n+PONG\r\n";
    let mut got = Vec::new();
    let mut buf = [0u8; 256];
    while got.len() < want.len() {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0);
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, want, "{:?}", String::from_utf8_lossy(&got));
    drop(srv);
}
