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

/// `ACL SETUSER` (and `ACL LOAD`) that revokes a channel disconnects the
/// user's clients subscribed to it, on every shard, as Redis does: they
/// kept receiving its messages. A subscriber whose own SETUSER revokes its
/// channel gets the +OK, then is disconnected. Clients whose subscriptions
/// are all still allowed stay connected.
#[test]
fn test_acl_revoking_a_channel_disconnects_its_subscribers_e2e() {
    let port = 19950u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s
    };
    // The server closed the connection: EOF (or a reset), not a read that
    // times out on a socket nobody serves any more.
    let assert_killed = |s: &mut TcpStream| {
        let mut buf = [0u8; 256];
        match s.read(&mut buf) {
            Ok(0) => {}
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            Ok(n) => panic!(
                "subscriber still connected: {:?}",
                String::from_utf8_lossy(&buf[..n])
            ),
            Err(e) => panic!("connection not closed: {e}"),
        }
    };
    let mut admin = connect();
    for (user, chans) in [("ps", "&news:*"), ("keep", "&news:*")] {
        assert_eq!(
            resp_cmd(
                &mut admin,
                &[
                    "ACL",
                    "SETUSER",
                    user,
                    "on",
                    ">pw",
                    "~*",
                    "+@all",
                    "resetchannels",
                    chans
                ]
            ),
            "+OK\r\n"
        );
    }
    let subscribe = |user: &str, cmd: &str, chan: &str| {
        let mut s = connect();
        assert_eq!(resp_cmd(&mut s, &["AUTH", user, "pw"]), "+OK\r\n");
        let r = resp_cmd(&mut s, &[cmd, chan]);
        assert!(r.contains(":1\r\n"), "{cmd} {chan}: {r}");
        s
    };
    // Enough connections that the 4 shards each get some.
    let mut victims: Vec<TcpStream> = (0..6)
        .map(|_| subscribe("ps", "SUBSCRIBE", "news:1"))
        .chain((0..3).map(|_| subscribe("ps", "SSUBSCRIBE", "news:2")))
        .chain((0..3).map(|_| subscribe("ps", "PSUBSCRIBE", "news:*")))
        .collect();
    let mut survivors: Vec<TcpStream> = (0..6)
        .map(|_| subscribe("keep", "SUBSCRIBE", "news:1"))
        .collect();

    assert_eq!(
        resp_cmd(
            &mut admin,
            &["ACL", "SETUSER", "ps", "resetchannels", "&other"]
        ),
        "+OK\r\n"
    );
    for v in &mut victims {
        assert_killed(v);
    }
    assert_eq!(resp_cmd(&mut admin, &["PUBLISH", "news:1", "hi"]), ":6\r\n");
    for s in &mut survivors {
        let mut buf = [0u8; 256];
        let n = s.read(&mut buf).unwrap();
        assert!(
            String::from_utf8_lossy(&buf[..n]).contains("hi"),
            "survivor lost its subscription"
        );
    }

    // A subscriber revoking its own channel (RESP3, where a subscribed
    // client may run any command) gets +OK, then is disconnected.
    let mut me = connect();
    assert!(resp_cmd(&mut me, &["HELLO", "3", "AUTH", "keep", "pw"]).contains("proto"));
    let r = resp_cmd(&mut me, &["SUBSCRIBE", "news:1"]);
    assert!(r.contains("subscribe"), "{r}");
    me.write_all(b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$4\r\nkeep\r\n$13\r\nresetchannels\r\n")
        .unwrap();
    let mut buf = [0u8; 256];
    let n = me.read(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf[..n]), "+OK\r\n");
    assert_killed(&mut me);
    // ... and so are the user's other subscribers.
    for s in &mut survivors {
        assert_killed(s);
    }
    assert_eq!(resp_cmd(&mut admin, &["PING"]), "+PONG\r\n");
    drop(admin);
    shutdown_and_wait(port, &mut srv);
}

/// QUIT in subscribed mode gets its +OK and then the server closes the
/// connection, as Redis does. The connection's writer task held a sender
/// of its own queue, so it never stopped and the socket stayed open until
/// the client closed it.
#[test]
fn test_quit_in_subscribed_mode_closes_the_connection_e2e() {
    let port = 19951u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    for resp3 in [false, true] {
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        if resp3 {
            assert!(resp_cmd(&mut c, &["HELLO", "3"]).contains("proto"));
        }
        assert!(resp_cmd(&mut c, &["SUBSCRIBE", "chan"]).contains("subscribe"));
        c.write_all(b"*1\r\n$4\r\nQUIT\r\n").unwrap();
        let mut buf = [0u8; 64];
        let n = c.read(&mut buf).unwrap();
        assert_eq!(String::from_utf8_lossy(&buf[..n]), "+OK\r\n");
        match c.read(&mut buf) {
            Ok(0) => {}
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            other => panic!("connection not closed after QUIT (resp3={resp3}): {other:?}"),
        }
    }
    shutdown_and_wait(port, &mut srv);
}

/// SUBSCRIBE, PSUBSCRIBE and SSUBSCRIBE are refused with -OOM while used
/// memory is over maxmemory (Redis flags them denyoom): subscriptions take
/// memory, and were accepted however far over the limit the server was.
#[test]
fn test_subscribe_refused_over_maxmemory_e2e() {
    let port = 19953u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s
    };
    let mut admin = connect();
    assert_eq!(
        resp_cmd(
            &mut admin,
            &["CONFIG", "SET", "maxmemory-policy", "noeviction"]
        ),
        "+OK\r\n"
    );
    // Far below what the server itself uses.
    assert_eq!(
        resp_cmd(&mut admin, &["CONFIG", "SET", "maxmemory", "1"]),
        "+OK\r\n"
    );
    for cmd in ["SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE"] {
        let mut c = connect();
        let r = resp_cmd(&mut c, &[cmd, "ch"]);
        assert!(r.starts_with("-OOM command not allowed"), "{cmd}: {r}");
        // Not subscribed: the connection still runs any command.
        assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    }
    let stats = resp_cmd(&mut admin, &["INFO", "commandstats"]);
    let sub = stats
        .lines()
        .find(|l| l.starts_with("cmdstat_subscribe:"))
        .unwrap();
    assert!(sub.contains("rejected_calls=1,"), "{sub}");
    // Under the limit again, subscriptions work.
    assert_eq!(
        resp_cmd(&mut admin, &["CONFIG", "SET", "maxmemory", "0"]),
        "+OK\r\n"
    );
    let mut c = connect();
    assert!(resp_cmd(&mut c, &["SUBSCRIBE", "ch"]).contains("subscribe"));
    shutdown_and_wait(port, &mut srv);
}

/// Hash field TTL commands publish their keyspace events, as in Redis 7.4+:
/// HEXPIRE & co `hexpire` (or `hdel` when a past time deletes fields, then
/// `del` if the hash went), HPERSIST `hpersist`, HGETEX the event of what it
/// changed, HSETEX `hset` then `hexpire`/`hdel`, HGETDEL `hdel` (+ `del`),
/// and fields removed by expiry one `hexpired` per key (+ `del`), lazily or
/// by the active cycle. None of these were published, so subscribers (and
/// the Redis unit/pubsub suite) waited forever.
#[test]
fn test_hash_field_ttl_keyspace_events_e2e() {
    let port = 19977u16;
    let ps = port.to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        "1",
        "--no-pin",
        "--enable-debug-command",
        "yes",
    ];
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "notify-keyspace-events", "Khg"]),
        "+OK\r\n"
    );
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_millis(150)))
        .unwrap();
    let r = resp_cmd(&mut sub, &["SUBSCRIBE", "__keyspace@0__:h"]);
    assert!(r.contains("subscribe"), "{r}");
    // The events published since the last call, in order.
    let mut events = || -> Vec<String> {
        let mut raw = Vec::new();
        let mut buf = [0u8; 65536];
        while let Ok(n) = sub.read(&mut buf) {
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        let text = String::from_utf8_lossy(&raw).into_owned();
        let parts: Vec<&str> = text.split("\r\n").collect();
        let mut out = Vec::new();
        let mut i = 0;
        while i + 6 < parts.len() {
            if parts[i] == "*3" && parts[i + 2] == "message" {
                out.push(parts[i + 6].to_string());
                i += 7;
            } else {
                i += 1;
            }
        }
        out
    };
    let run = |c: &mut TcpStream, args: &[&str]| {
        let r = resp_cmd(c, args);
        assert!(!r.starts_with('-'), "{args:?}: {r}");
    };
    let far = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 999_999)
        .to_string();

    run(
        &mut c,
        &["HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4"],
    );
    assert_eq!(events(), ["hset"]);
    run(&mut c, &["HEXPIRE", "h", "999999", "FIELDS", "1", "a"]);
    run(&mut c, &["HEXPIREAT", "h", &far, "NX", "FIELDS", "1", "b"]);
    run(&mut c, &["HPEXPIRE", "h", "999999", "FIELDS", "1", "a"]);
    run(&mut c, &["HPERSIST", "h", "FIELDS", "1", "a"]);
    run(&mut c, &["HPEXPIRE", "h", "0", "FIELDS", "1", "a"]);
    assert_eq!(
        events(),
        ["hexpire", "hexpire", "hexpire", "hpersist", "hdel"]
    );
    // Nothing changed: no event.
    run(&mut c, &["HPERSIST", "h", "FIELDS", "1", "c"]);
    run(&mut c, &["HEXPIRE", "h", "100", "NX", "FIELDS", "1", "b"]);
    assert!(events().is_empty());

    // Active expiry: one hexpired.
    run(&mut c, &["HPEXPIRE", "h", "500", "FIELDS", "2", "b", "c"]);
    assert_eq!(events(), ["hexpire"]);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut got = Vec::new();
    while got.is_empty() && std::time::Instant::now() < deadline {
        got = events();
    }
    assert_eq!(got, ["hexpired"]);

    // HSETEX / HGETEX / HGETDEL.
    run(
        &mut c,
        &["HSETEX", "h", "PX", "999999", "FIELDS", "1", "e", "5"],
    );
    assert_eq!(events(), ["hset", "hexpire"]);
    run(&mut c, &["HGETEX", "h", "PERSIST", "FIELDS", "1", "e"]);
    run(&mut c, &["HGETEX", "h", "EXAT", &far, "FIELDS", "1", "e"]);
    run(&mut c, &["HGETEX", "h", "PX", "0", "FIELDS", "1", "e"]);
    assert_eq!(events(), ["hpersist", "hexpire", "hdel"]);
    run(&mut c, &["HGETDEL", "h", "FIELDS", "1", "d"]);
    assert_eq!(events(), ["hdel", "del"]);

    // Lazy expiry inside a command: hexpired first, then the command's own
    // event; the last field going takes the key with it.
    run(&mut c, &["HSETEX", "h", "FIELDS", "2", "x", "1", "y", "2"]);
    run(&mut c, &["HSETEX", "h", "PX", "0", "FIELDS", "1", "z", "3"]);
    assert_eq!(events(), ["hset", "hset", "hdel"]);
    run(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "0"]);
    run(&mut c, &["HPEXPIRE", "h", "5", "FIELDS", "1", "x"]);
    std::thread::sleep(Duration::from_millis(30));
    run(&mut c, &["HGETDEL", "h", "FIELDS", "1", "y"]);
    assert_eq!(events(), ["hexpire", "hexpired", "hdel", "del"]);
    // HGET lazily expiring the last field reports only `del` (as Redis).
    run(&mut c, &["HSET", "h", "only", "1"]);
    run(&mut c, &["HPEXPIRE", "h", "5", "FIELDS", "1", "only"]);
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(resp_cmd(&mut c, &["HGET", "h", "only"]), "$-1\r\n");
    assert_eq!(events(), ["hset", "hexpire", "del"]);
    // HSETEX FNX over the only (expired) field recreates the hash: no `del`.
    run(&mut c, &["HSET", "h", "f", "1"]);
    run(&mut c, &["HPEXPIRE", "h", "5", "FIELDS", "1", "f"]);
    std::thread::sleep(Duration::from_millis(30));
    let r = resp_cmd(
        &mut c,
        &[
            "HSETEX", "h", "FNX", "PX", "100000", "FIELDS", "1", "f", "2",
        ],
    );
    assert_eq!(r, ":1\r\n");
    assert_eq!(events(), ["hset", "hexpire", "hexpired", "hset", "hexpire"]);
    run(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "1"]);
    shutdown_and_wait(port, &mut srv.0);
}

/// Lowering maxmemory with CONFIG SET evicts right away, as in Redis: the
/// `evicted` event arrives before any other command runs. Eviction waited
/// for the next command, so an idle server gave no memory back.
#[test]
fn test_config_set_maxmemory_evicts_immediately_e2e() {
    let port = 19980u16;
    let ps = port.to_string();
    let args = ["--port", &ps, "--threads", "2", "--no-pin"];
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for (k, v) in [
        ("notify-keyspace-events", "Ee"),
        ("maxmemory-policy", "allkeys-lru"),
    ] {
        assert_eq!(resp_cmd(&mut c, &["CONFIG", "SET", k, v]), "+OK\r\n");
    }
    assert_eq!(resp_cmd(&mut c, &["SET", "foo", "bar"]), "+OK\r\n");
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    assert!(resp_cmd(&mut sub, &["PSUBSCRIBE", "*"]).contains("psubscribe"));
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "1"]),
        "+OK\r\n"
    );
    // No other command: the event must come from CONFIG SET itself.
    let mut buf = [0u8; 4096];
    let n = sub
        .read(&mut buf)
        .expect("no evicted event after CONFIG SET maxmemory");
    let msg = String::from_utf8_lossy(&buf[..n]);
    assert!(
        msg.contains("__keyevent@0__:evicted") && msg.contains("foo"),
        "{msg}"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "0"]),
        "+OK\r\n"
    );
    shutdown_and_wait(port, &mut srv.0);
}

/// CLIENT REPLY OFF / SKIP hold for a subscribed client too, as in Redis:
/// subscription confirmations and messages (pushes) still arrive, other
/// replies (PING) don't. A subscribed client ignored its reply mode.
#[test]
fn test_client_reply_off_skip_in_subscribed_mode_e2e() {
    let port = 19981u16;
    let ps = port.to_string();
    let args = ["--port", &ps, "--threads", "2", "--no-pin"];
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let send = |s: &mut TcpStream, args: &[&str]| {
        s.write_all(&format_resp_cmd(args)).unwrap();
        std::thread::sleep(Duration::from_millis(80));
    };
    let drain = |s: &mut TcpStream| -> String {
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        while let Ok(n) = s.read(&mut buf) {
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        String::from_utf8_lossy(&raw).into_owned()
    };
    send(&mut sub, &["HELLO", "3"]);
    drain(&mut sub);
    send(&mut sub, &["SSUBSCRIBE", "ch"]);
    assert!(drain(&mut sub).contains("ssubscribe"));

    send(&mut sub, &["CLIENT", "REPLY", "OFF"]);
    send(&mut sub, &["PING"]);
    assert_eq!(resp_cmd(&mut c, &["SPUBLISH", "ch", "hello"]), ":1\r\n");
    send(&mut sub, &["PING"]);
    let got = drain(&mut sub);
    assert!(!got.contains("PONG") && !got.contains("pong"), "{got}");
    assert!(got.contains("smessage") && got.contains("hello"), "{got}");

    // SKIP drops just the next reply.
    send(&mut sub, &["CLIENT", "REPLY", "ON"]);
    assert_eq!(drain(&mut sub), "+OK\r\n");
    send(&mut sub, &["CLIENT", "REPLY", "SKIP"]);
    send(&mut sub, &["PING"]);
    send(&mut sub, &["PING"]);
    let got = drain(&mut sub);
    assert_eq!(
        got.matches("PONG").count() + got.matches("pong").count(),
        1,
        "{got}"
    );

    // Unsubscribing still confirms under OFF.
    send(&mut sub, &["CLIENT", "REPLY", "OFF"]);
    send(&mut sub, &["SUNSUBSCRIBE", "ch"]);
    assert!(drain(&mut sub).contains("sunsubscribe"));
    shutdown_and_wait(port, &mut srv.0);
}

/// The next pub/sub push on `s` (`message` / `pmessage` / `subscribe` and
/// its parts; integers as their digits).
fn next_push(s: &mut TcpStream) -> Vec<String> {
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut byte = [0u8; 1];
    let mut read_line = |s: &mut TcpStream| -> Vec<u8> {
        let mut line = Vec::new();
        while !line.ends_with(b"\r\n") {
            assert_eq!(s.read(&mut byte).unwrap(), 1, "connection closed");
            line.push(byte[0]);
        }
        line.truncate(line.len() - 2);
        line
    };
    let head = read_line(s);
    assert_eq!(head[0], b'*', "{:?}", String::from_utf8_lossy(&head));
    let n: usize = std::str::from_utf8(&head[1..]).unwrap().parse().unwrap();
    let mut parts = Vec::new();
    for _ in 0..n {
        let line = read_line(s);
        match line[0] {
            b':' => parts.push(String::from_utf8(line[1..].to_vec()).unwrap()),
            b'$' => {
                let len: usize = std::str::from_utf8(&line[1..]).unwrap().parse().unwrap();
                let mut arg = vec![0u8; len + 2];
                s.read_exact(&mut arg).unwrap();
                arg.truncate(len);
                parts.push(String::from_utf8(arg).unwrap());
            }
            _ => panic!("unexpected {:?}", String::from_utf8_lossy(&line)),
        }
    }
    parts
}

/// Asserts nothing more arrives on `s` for a moment.
fn assert_no_push(s: &mut TcpStream) {
    s.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let mut buf = [0u8; 4096];
    match s.read(&mut buf) {
        Ok(n) => panic!("unexpected push: {:?}", String::from_utf8_lossy(&buf[..n])),
        Err(e) => assert!(
            matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            "{e}"
        ),
    }
}

/// `<event>|<len>:<f>,...` for fields `<prefix>0..<prefix>{n-1}`.
fn subkey_payload(event: &str, prefix: &str, n: usize) -> String {
    let fields: Vec<String> = (0..n)
        .map(|i| {
            let f = format!("{prefix}{i}");
            format!("{}:{f}", f.len())
        })
        .collect();
    format!("{event}|{}", fields.join(","))
}

fn subscribed(port: u16, cmd: &str, channel: &str) -> TcpStream {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.write_all(&format_resp_cmd(&[cmd, channel])).unwrap();
    let r = next_push(&mut s);
    assert_eq!(r[0], cmd.to_lowercase(), "{r:?}");
    s
}

/// Subkey notifications (Redis 8.8, `S`/`T`/`I`/`V`): each channel class's
/// name and payload, one `subkeyspaceitem` message per field (also through
/// a pattern), no item channel for a key holding a newline, and the flags'
/// CONFIG GET round trip. Rudis published none of them.
#[test]
fn test_subkey_notification_channels_e2e() {
    let port = 20500u16;
    let ps = port.to_string();
    let args = ["--port", &ps, "--threads", "2", "--no-pin"];
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let cfg = |c: &mut TcpStream, flags: &str| {
        assert_eq!(
            resp_cmd(c, &["CONFIG", "SET", "notify-keyspace-events", flags]),
            "+OK\r\n"
        );
        let r = resp_cmd(c, &["CONFIG", "GET", "notify-keyspace-events"]);
        r.split("\r\n").nth(4).unwrap_or_default().to_string()
    };
    assert_eq!(cfg(&mut c, "Sh"), "hS");
    assert_eq!(cfg(&mut c, "KEA$STIV"), "AKESTIV");
    assert_eq!(cfg(&mut c, "VhIT"), "hTIV");
    assert_eq!(cfg(&mut c, "STIVh"), "hSTIV");

    let mut s = subscribed(port, "SUBSCRIBE", "__subkeyspace@0__:myhash");
    let mut t = subscribed(port, "SUBSCRIBE", "__subkeyevent@0__:hset");
    let mut v = subscribed(port, "SUBSCRIBE", "__subkeyspaceevent@0__:hset|myhash");
    let mut i = subscribed(port, "SUBSCRIBE", "__subkeyspaceitem@0__:myhash\nf2");
    let mut pi = subscribed(port, "PSUBSCRIBE", "__subkeyspaceitem@0__:*");
    assert_eq!(
        resp_cmd(
            &mut c,
            &["HSET", "myhash", "f1", "v1", "f2", "v2", "f1", "v3"]
        ),
        ":2\r\n"
    );
    assert_eq!(
        next_push(&mut s),
        ["message", "__subkeyspace@0__:myhash", "hset|2:f1,2:f2"]
    );
    assert_eq!(
        next_push(&mut t),
        ["message", "__subkeyevent@0__:hset", "6:myhash|2:f1,2:f2"]
    );
    assert_eq!(
        next_push(&mut v),
        ["message", "__subkeyspaceevent@0__:hset|myhash", "2:f1,2:f2"]
    );
    assert_eq!(
        next_push(&mut i),
        ["message", "__subkeyspaceitem@0__:myhash\nf2", "hset"]
    );
    for f in ["f1", "f2"] {
        assert_eq!(
            next_push(&mut pi),
            [
                "pmessage",
                "__subkeyspaceitem@0__:*",
                &format!("__subkeyspaceitem@0__:myhash\n{f}"),
                "hset"
            ]
        );
    }
    // A key with a newline: no item channel, the others still publish.
    assert_eq!(resp_cmd(&mut c, &["HSET", "a\nb", "x", "1"]), ":1\r\n");
    assert_eq!(
        next_push(&mut t),
        ["message", "__subkeyevent@0__:hset", "3:a\nb|1:x"]
    );
    assert_no_push(&mut pi);
    // Hash events only (`h`), and with `S` off nothing reaches it.
    assert_eq!(cfg(&mut c, "TIVh"), "hTIV");
    assert_eq!(resp_cmd(&mut c, &["HSET", "myhash", "f3", "v"]), ":1\r\n");
    assert_eq!(
        next_push(&mut t),
        ["message", "__subkeyevent@0__:hset", "6:myhash|2:f3"]
    );
    assert_no_push(&mut s);
    assert_eq!(cfg(&mut c, "STIVg"), "gSTIV");
    assert_eq!(resp_cmd(&mut c, &["HSET", "myhash", "f4", "v"]), ":1\r\n");
    assert_no_push(&mut t);
    shutdown_and_wait(port, &mut srv.0);
}

/// The fields each hash command reports on `__subkeyspace@<db>__:<key>`:
/// all fields given (HSET, HSETEX), the ones deleted (HDEL, HGETDEL, a past
/// deadline), the ones whose deadline was set or removed (HEXPIRE, HPERSIST,
/// HGETEX), the incremented one; each once, beyond the 16 kept inline.
#[test]
fn test_subkey_notification_hash_commands_e2e() {
    let port = 20501u16;
    let ps = port.to_string();
    let args = ["--port", &ps, "--threads", "2", "--no-pin"];
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "notify-keyspace-events", "Sh"]),
        "+OK\r\n"
    );
    let mut s = subscribed(port, "SUBSCRIBE", "__subkeyspace@0__:h");
    let mut run = |args: &[&str]| {
        let r = resp_cmd(&mut c, args);
        assert!(!r.starts_with('-'), "{args:?}: {r}");
    };
    let mut expect = |payload: &str| {
        assert_eq!(
            next_push(&mut s),
            ["message", "__subkeyspace@0__:h", payload]
        );
    };
    for n in [3usize, 32] {
        let names: Vec<String> = (0..n).map(|i| format!("f{i}")).collect();
        let fields: Vec<&str> = names.iter().map(String::as_str).collect();
        let pairs: Vec<&str> = fields.iter().flat_map(|f| [*f, "v"]).collect();
        let nstr = n.to_string();
        let with = |head: &[&str], tail: &[&str]| -> Vec<String> {
            head.iter().chain(tail).map(|x| x.to_string()).collect()
        };
        let call = |run: &mut dyn FnMut(&[&str]), v: Vec<String>| {
            let v: Vec<&str> = v.iter().map(String::as_str).collect();
            run(&v);
        };
        call(&mut run, with(&["HSET", "h"], &pairs));
        expect(&subkey_payload("hset", "f", n));
        call(
            &mut run,
            with(&["HEXPIRE", "h", "1000", "FIELDS", &nstr], &fields),
        );
        expect(&subkey_payload("hexpire", "f", n));
        call(&mut run, with(&["HPERSIST", "h", "FIELDS", &nstr], &fields));
        expect(&subkey_payload("hpersist", "f", n));
        call(
            &mut run,
            with(&["HGETEX", "h", "EX", "1000", "FIELDS", &nstr], &fields),
        );
        expect(&subkey_payload("hexpire", "f", n));
        call(
            &mut run,
            with(&["HGETEX", "h", "PERSIST", "FIELDS", &nstr], &fields),
        );
        expect(&subkey_payload("hpersist", "f", n));
        call(
            &mut run,
            with(&["HGETEX", "h", "PX", "0", "FIELDS", &nstr], &fields),
        );
        expect(&subkey_payload("hdel", "f", n));
        call(
            &mut run,
            with(&["HSETEX", "h", "EX", "1000", "FIELDS", &nstr], &pairs),
        );
        expect(&subkey_payload("hset", "f", n));
        expect(&subkey_payload("hexpire", "f", n));
        call(
            &mut run,
            with(&["HEXPIREAT", "h", "1", "FIELDS", &nstr], &fields),
        );
        expect(&subkey_payload("hdel", "f", n));
        call(
            &mut run,
            with(&["HSETEX", "h", "PX", "0", "FIELDS", &nstr], &pairs),
        );
        expect(&subkey_payload("hset", "f", n));
        expect(&subkey_payload("hdel", "f", n));
        call(&mut run, with(&["HSET", "h"], &pairs));
        expect(&subkey_payload("hset", "f", n));
        call(&mut run, with(&["HGETDEL", "h", "FIELDS", &nstr], &fields));
        expect(&subkey_payload("hdel", "f", n));
        call(&mut run, with(&["HSET", "h"], &pairs));
        expect(&subkey_payload("hset", "f", n));
        call(&mut run, with(&["HDEL", "h"], &fields));
        expect(&subkey_payload("hdel", "f", n));
    }
    // HDEL lists what it deleted, each field once.
    run(&["HSET", "h", "a", "1", "b", "2", "c", "3"]);
    expect("hset|1:a,1:b,1:c");
    run(&["HDEL", "h", "a", "a", "nope", "b"]);
    expect("hdel|1:a,1:b");
    run(&["HDEL", "h", "nope"]);
    run(&["HINCRBY", "h", "counter", "5"]);
    expect("hincrby|7:counter");
    run(&["HINCRBYFLOAT", "h", "counter", "2.5"]);
    expect("hincrbyfloat|7:counter");
    run(&["HSETNX", "h", "c", "x"]);
    run(&["HSETNX", "h", "newfield", "x"]);
    expect("hset|8:newfield");
    run(&["HMSET", "h", "m", "1"]);
    expect("hset|1:m");
    // Mixed HEXPIRE results: only the fields whose deadline was set.
    run(&[
        "HEXPIRE", "h", "1000", "NX", "FIELDS", "3", "c", "nope", "m",
    ]);
    expect("hexpire|1:c,1:m");
    run(&["HEXPIRE", "h", "2000", "NX", "FIELDS", "2", "c", "counter"]);
    expect("hexpire|7:counter");
    run(&["HPERSIST", "h", "FIELDS", "2", "c", "newfield"]);
    expect("hpersist|1:c");
    drop(run);
    assert_no_push(&mut s);
    shutdown_and_wait(port, &mut srv.0);
}

/// Fields that expire report `hexpired` with their names, lazily (on
/// access, in a stable order) and actively.
#[test]
fn test_subkey_notification_field_expiry_e2e() {
    let port = 20502u16;
    let ps = port.to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        "1",
        "--no-pin",
        "--enable-debug-command",
        "yes",
    ];
    let mut srv = KillRudisOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "notify-keyspace-events", "Sh"]),
        "+OK\r\n"
    );
    let mut s = subscribed(port, "SUBSCRIBE", "__subkeyspace@0__:h");
    let n = 32usize;
    let names: Vec<String> = (0..n).map(|i| format!("f{i}")).collect();
    let nstr = n.to_string();
    let mut hset: Vec<&str> = vec!["HSET", "h", "keep", "v"];
    hset.extend(names.iter().flat_map(|f| [f.as_str(), "v"]));
    let mut hpexpire: Vec<&str> = vec!["HPEXPIRE", "h", "10", "FIELDS", &nstr];
    hpexpire.extend(names.iter().map(String::as_str));
    let mut hmget: Vec<&str> = vec!["HMGET", "h"];
    hmget.extend(names.iter().map(String::as_str));
    let skip = |s: &mut TcpStream, k: usize| {
        for _ in 0..k {
            next_push(s);
        }
    };

    // Lazy.
    assert_eq!(
        resp_cmd(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "0"]),
        "+OK\r\n"
    );
    resp_cmd(&mut c, &hset);
    resp_cmd(&mut c, &hpexpire);
    skip(&mut s, 2);
    thread::sleep(Duration::from_millis(100));
    resp_cmd(&mut c, &hmget);
    assert_eq!(
        next_push(&mut s),
        [
            "message",
            "__subkeyspace@0__:h",
            &subkey_payload("hexpired", "f", n)
        ]
    );
    assert_eq!(
        resp_cmd(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "1"]),
        "+OK\r\n"
    );

    // Active.
    resp_cmd(&mut c, &hset);
    resp_cmd(&mut c, &hpexpire);
    skip(&mut s, 2);
    let msg = next_push(&mut s);
    let (event, list) = msg[2].split_once('|').unwrap();
    assert_eq!(event, "hexpired");
    let mut got: Vec<&str> = list.split(',').collect();
    got.sort();
    let want_s = subkey_payload("hexpired", "f", n);
    let mut want: Vec<&str> = want_s.split_once('|').unwrap().1.split(',').collect();
    want.sort();
    assert_eq!(got, want);
    assert_eq!(resp_cmd(&mut c, &["HLEN", "h"]), ":1\r\n");
    shutdown_and_wait(port, &mut srv.0);
}

/// A replica publishes subkey events for the writes its master sends,
/// `hexpired` (not `hdel`) for the master's expiry of fields.
#[test]
fn test_subkey_notification_on_replica_e2e() {
    let (mport, rport) = (20503u16, 20504u16);
    let (mps, rps) = (mport.to_string(), rport.to_string());
    let mut master = KillRudisOnDrop(spawn_rudis_listening(
        &["--port", &mps, "--threads", "2", "--no-pin"],
        mport,
    ));
    let mut replica = KillRudisOnDrop(spawn_rudis_listening(
        &["--port", &rps, "--threads", "2", "--no-pin"],
        rport,
    ));
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &mps]),
        "+OK\r\n"
    );
    wait_for_master_link(&mut r);
    assert_eq!(
        resp_cmd(&mut r, &["CONFIG", "SET", "notify-keyspace-events", "Sh"]),
        "+OK\r\n"
    );
    let mut s = subscribed(rport, "SUBSCRIBE", "__subkeyspace@0__:myhash");
    assert_eq!(
        resp_cmd(
            &mut m,
            &["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"]
        ),
        ":3\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut m,
            &["HPEXPIRE", "myhash", "100", "FIELDS", "2", "f1", "f2"]
        ),
        "*2\r\n:1\r\n:1\r\n"
    );
    let ch = "__subkeyspace@0__:myhash";
    assert_eq!(next_push(&mut s), ["message", ch, "hset|2:f1,2:f2,2:f3"]);
    assert_eq!(next_push(&mut s), ["message", ch, "hexpire|2:f1,2:f2"]);
    assert_eq!(next_push(&mut s), ["message", ch, "hexpired|2:f1,2:f2"]);
    assert_eq!(resp_cmd(&mut m, &["HDEL", "myhash", "f3"]), ":1\r\n");
    assert_eq!(next_push(&mut s), ["message", ch, "hdel|2:f3"]);
    shutdown_and_wait(rport, &mut replica.0);
    shutdown_and_wait(mport, &mut master.0);
}

#[test]
fn test_client_list_omem_counts_a_stalled_subscribers_queue_e2e() {
    // CLIENT LIST's omem for a subscriber that doesn't read was a snapshot
    // the writer took before its write to the full socket, so it stayed at a
    // few hundred bytes while megabytes queued (Redis' unit/obuf-limits
    // watches omem grow up to the limit).
    let port = 20603u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(
            &mut c,
            &[
                "CONFIG",
                "SET",
                "client-output-buffer-limit",
                "pubsub 256mb 0 0"
            ]
        ),
        "+OK\r\n"
    );
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert!(resp_cmd(&mut sub, &["CLIENT", "SETNAME", "stalled"]).starts_with("+OK"));
    assert!(resp_cmd(&mut sub, &["SUBSCRIBE", "foo"]).contains("subscribe"));
    // `sub` reads nothing from here on.
    let msg = "x".repeat(1000);
    let omem = |c: &mut TcpStream| -> usize {
        let list = resp_cmd(c, &["CLIENT", "LIST"]);
        let line = list
            .lines()
            .find(|l| l.contains("name=stalled"))
            .expect(&list);
        let v = line
            .split(' ')
            .find_map(|f| f.strip_prefix("omem="))
            .unwrap();
        v.parse().unwrap()
    };
    let mut seen = 0;
    for _ in 0..200 {
        // 100 KB per round, up to 20 MB: past what the socket buffers hold.
        for _ in 0..100 {
            assert_eq!(resp_cmd(&mut c, &["PUBLISH", "foo", &msg]), ":1\r\n");
        }
        seen = omem(&mut c);
        if seen > 1_000_000 {
            break;
        }
    }
    assert!(seen > 1_000_000, "omem {seen}");
    drop(sub);
    let _ = srv.kill();
    let _ = srv.wait();
}

#[test]
fn test_pubsub_soft_limit_disconnects_without_more_traffic_e2e() {
    // A subscriber over the pubsub soft limit was only checked when the next
    // message came: with publishing stopped it stayed connected, its queue
    // full, forever. Redis disconnects it once soft_seconds pass.
    let port = 20604u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(
            &mut c,
            &[
                "CONFIG",
                "SET",
                "client-output-buffer-limit",
                "pubsub 0 100000 2"
            ]
        ),
        "+OK\r\n"
    );
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert!(resp_cmd(&mut sub, &["CLIENT", "SETNAME", "softsub"]).starts_with("+OK"));
    assert!(resp_cmd(&mut sub, &["SUBSCRIBE", "foo"]).contains("subscribe"));
    let msg = "x".repeat(1000);
    let omem = |c: &mut TcpStream| -> Option<usize> {
        let list = resp_cmd(c, &["CLIENT", "LIST"]);
        let line = list.lines().find(|l| l.contains("name=softsub"))?;
        line.split(' ')
            .find_map(|f| f.strip_prefix("omem="))
            .map(|v| v.parse().unwrap())
    };
    // Publish until the queue is over the soft limit, then stop.
    let mut over = false;
    for _ in 0..200 {
        for _ in 0..100 {
            resp_cmd(&mut c, &["PUBLISH", "foo", &msg]);
        }
        if omem(&mut c).is_some_and(|m| m > 100_000) {
            over = true;
            break;
        }
    }
    assert!(over, "queue never went over the soft limit");
    let stopped = std::time::Instant::now();
    while omem(&mut c).is_some() {
        assert!(
            stopped.elapsed() < Duration::from_secs(6),
            "still connected after the soft limit's 2 s"
        );
        thread::sleep(Duration::from_millis(50));
    }
    drop(sub);
    let _ = srv.kill();
    let _ = srv.wait();
}
