#[test]
fn test_client_list_type_filter() {
    let list = "id=1 flags=N resp=2\nid=2 flags=S resp=2\nid=3 flags=P resp=2\n\
                    id=4 flags=P resp=3\nid=5 flags=O resp=2\n"
        .to_string();
    let ids = |ty: &str| -> Vec<String> {
        super::filter_client_list_type(list.clone(), Some(ty))
            .lines()
            .map(|l| l.split(' ').next().unwrap().to_string())
            .collect()
    };
    assert_eq!(ids("normal"), ["id=1", "id=4", "id=5"]);
    assert_eq!(ids("replica"), ["id=2"]);
    assert_eq!(ids("slave"), ["id=2"]);
    assert_eq!(ids("pubsub"), ["id=3"]);
    assert!(ids("master").is_empty());
    assert_eq!(super::filter_client_list_type(list.clone(), None), list);
}

#[test]
fn test_port_in_span_covers_every_shard_port() {
    assert!(super::port_in_span(7000, 4, 7000));
    assert!(super::port_in_span(7000, 4, 7003));
    assert!(!super::port_in_span(7000, 4, 7004));
    assert!(!super::port_in_span(7000, 4, 6999));
    // Non-cluster: only the base port.
    assert!(super::port_in_span(7000, 1, 7000));
    assert!(!super::port_in_span(7000, 1, 7001));
    assert!(super::port_in_span(7000, 0, 7000));
    // No overflow at the top of the port range.
    assert!(super::port_in_span(65534, 4, 65535));
}

/// A write past the pause gate counts as running until it ends, and a
/// squashed batch with writes is refused (per-command path) while
/// writes are paused: so once writes are paused and nothing runs, the
/// replication offset is final (FAILOVER, shutdown).
#[test]
fn test_move_element_from_source_waits_for_the_destination_push() {
    use std::cell::RefCell;
    use std::rc::Rc;
    let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
    let senders = senders_mesh.remove(0);
    drop(senders_mesh);
    let rx1 = receivers.remove(1);
    let router = crate::router::Router::new(
        0,
        2,
        9977,
        Rc::new(RefCell::new(crate::shard::ShardDb::new(9977))),
        senders,
        None,
        Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
        std::env::temp_dir(),
    );
    let mut out = Vec::new();
    execute_local_command(
        &Command::Rpush {
            key: Bytes::from_static(b"src"),
            values: smallvec::smallvec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        },
        &mut router.local_db.borrow_mut(),
        &mut out,
        None,
    );
    // The fake destination: checks the push, holds it a while, answers.
    let answered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dest = {
        let answered = answered.clone();
        std::thread::spawn(move || {
            let Ok(ShardMessage::Batch {
                items,
                responder,
                replicate_as,
                ..
            }) = rx1.recv()
            else {
                panic!("no push");
            };
            assert_eq!(replicate_as.as_deref(), Some(&b"LOGICAL"[..]));
            // Claimed at once, as the real destination does: the source
            // then waits however long the push takes.
            assert!(responder.claim());
            match &items[0].2 {
                Command::Lpush { key, values } => {
                    assert_eq!(key.as_ref(), b"dst");
                    assert_eq!(values[0].as_ref(), b"b");
                }
                other => panic!("pushed {other:?}"),
            }
            std::thread::sleep(std::time::Duration::from_millis(30));
            answered.store(true, std::sync::atomic::Ordering::SeqCst);
            responder.write_slot(0, crate::shard::CompactResp::from_slice(b":1\r\n"));
            responder.finish(items);
        })
    };
    let reply = move_element_from_source(
        &router,
        0,
        &Command::Rpop {
            key: Bytes::from_static(b"src"),
            count: None,
        },
        1,
        crate::shard::MovePush::List {
            key: Bytes::from_static(b"dst"),
            to: crate::table::ListDirection::Left,
        },
        Bytes::from_static(b"LOGICAL"),
        0,
    );
    // Returned only once the destination had pushed.
    assert!(answered.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(reply, b"$1\r\nb\r\n");
    dest.join().unwrap();
    // Nothing to move: no push is sent, the pop's reply comes back.
    let reply = move_element_from_source(
        &router,
        0,
        &Command::Rpop {
            key: Bytes::from_static(b"missing"),
            count: None,
        },
        1,
        crate::shard::MovePush::List {
            key: Bytes::from_static(b"dst"),
            to: crate::table::ListDirection::Left,
        },
        Bytes::from_static(b"LOGICAL"),
        0,
    );
    assert!(reply == b"$-1\r\n" || reply == b"_\r\n", "{reply:?}");
}

/// A destination that does not take the push in time: the source
/// withdraws it, puts the popped element back where it was, and fails
/// the move; the destination then skips the push. Nothing is lost or
/// duplicated.
#[test]
fn test_move_element_from_source_undoes_the_pop_when_the_push_is_not_taken() {
    use std::cell::RefCell;
    use std::rc::Rc;
    let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
    let senders = senders_mesh.remove(0);
    drop(senders_mesh);
    let rx1 = receivers.remove(1);
    let router = crate::router::Router::new(
        0,
        2,
        9976,
        Rc::new(RefCell::new(crate::shard::ShardDb::new(9976))),
        senders,
        None,
        Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
        std::env::temp_dir(),
    );
    let mut out = Vec::new();
    execute_local_command(
        &Command::Rpush {
            key: Bytes::from_static(b"usrc"),
            values: smallvec::smallvec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        },
        &mut router.local_db.borrow_mut(),
        &mut out,
        None,
    );
    // The fake destination: busy past the source's timeout, then finds
    // the push withdrawn.
    let dest = std::thread::spawn(move || {
        let Ok(ShardMessage::Batch { responder, .. }) = rx1.recv() else {
            panic!("no push");
        };
        std::thread::sleep(std::time::Duration::from_millis(300));
        assert!(!responder.claim(), "a withdrawn push must not run");
    });
    let prev = CROSS_SHARD_MOVE_TIMEOUT_MS.swap(50, std::sync::atomic::Ordering::Relaxed);
    let reply = move_element_from_source(
        &router,
        0,
        &Command::Rpop {
            key: Bytes::from_static(b"usrc"),
            count: None,
        },
        1,
        crate::shard::MovePush::List {
            key: Bytes::from_static(b"udst"),
            to: crate::table::ListDirection::Left,
        },
        Bytes::from_static(b"LOGICAL"),
        0,
    );
    CROSS_SHARD_MOVE_TIMEOUT_MS.store(prev, std::sync::atomic::Ordering::Relaxed);
    dest.join().unwrap();
    assert!(
        reply.starts_with(b"-TRYAGAIN"),
        "{:?}",
        String::from_utf8_lossy(&reply)
    );
    let mut out = Vec::new();
    execute_local_command(
        &Command::Lrange {
            key: Bytes::from_static(b"usrc"),
            start: 0,
            stop: -1,
        },
        &mut router.local_db.borrow_mut(),
        &mut out,
        None,
    );
    assert_eq!(out, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
}

#[test]
fn test_keys_on_several_shards() {
    let one: &[&[u8]] = &[b"{u}a", b"{u}b", b"{u}c"];
    assert!(!keys_on_several_shards(one.iter().copied(), 8));
    assert!(!keys_on_several_shards(std::iter::empty::<&[u8]>(), 8));
    // Some pair of plain keys lands on two shards.
    let keys: Vec<Vec<u8>> = (0..64).map(|i| format!("k{i}").into_bytes()).collect();
    assert!(keys_on_several_shards(keys.iter().map(|k| k.as_slice()), 8));
    // One shard: nothing spans.
    assert!(!keys_on_several_shards(
        keys.iter().map(|k| k.as_slice()),
        1
    ));
}

#[test]
fn test_running_writes_are_counted_around_the_pause_gate() {
    let set = Command::Mset(vec![(Bytes::from_static(b"k"), Bytes::from_static(b"v"))]);
    let get = Command::Get(Bytes::from_static(b"k"));
    assert!(super::counts_as_running_write(&set));
    assert!(!super::counts_as_running_write(&get));
    let g = super::WriteRunning::enter(true);
    assert!(super::writes_running() >= 1);
    drop(g);
    // Other tests may pause clients meanwhile (the pause is global):
    // then the batch is refused instead.
    match super::enter_write_batch(&[set.clone(), get.clone()]) {
        Some(batch) => drop(batch),
        None => assert!(super::is_client_paused().is_some()),
    }
    // Reads only: never counted, never refused.
    assert!(super::enter_write_batch(&[get.clone(), get]).is_some());
}

/// Replica links are not counted in INFO connected_clients.
#[test]
fn test_connected_clients_leaves_out_replicas() {
    assert_eq!(super::connected_clients_of(4, 1), 3);
    assert_eq!(super::connected_clients_of(0, 1), 0);
}

#[test]
fn test_watch_is_the_nodes_on_every_shard_port() {
    // WATCH on a shard port, FLUSHALL (or a write) arriving on the base
    // port: the watcher's EXEC must fail.
    let (base, shard) = (41897u16, 41898u16);
    set_node_port(shard, base);
    watch_keys(shard, 9_000_002, &[Bytes::from_static(b"wk")]);
    assert!(!is_watch_tainted(shard, 9_000_002));
    touch_watched_keys_on_flushall(base);
    assert!(is_watch_tainted(shard, 9_000_002));
    unwatch_keys(shard, 9_000_002);
    watch_keys(shard, 9_000_003, &[Bytes::from_static(b"wk2")]);
    touch_watched_key_in_db(base, CURRENT_DB.get(), b"wk2");
    assert!(is_watch_tainted(shard, 9_000_003));
    unwatch_keys(shard, 9_000_003);
}

#[test]
fn test_proto_max_bulk_len_takes_memory_units() {
    assert_eq!(super::proto_max_bulk_len_arg("512mb"), Some(512 << 20));
    assert_eq!(super::proto_max_bulk_len_arg("1048576"), Some(1 << 20));
    assert_eq!(super::proto_max_bulk_len_arg("lots"), None);
}

#[test]
fn test_monitor_feed_is_the_nodes_on_every_shard_port() {
    // A monitor on the base port gets commands broadcast from a shard
    // port of the same node, and a monitor on a shard port gets the
    // base port's.
    let (base, shard) = (41895u16, 41896u16);
    set_node_port(shard, base);
    let (tx, rx) = flume::unbounded();
    register_monitor_client(base, 9_000_001, crate::transport::PushTarget::Queue(tx));
    broadcast_monitor(
        shard,
        "127.0.0.1:1",
        &[Bytes::from_static(b"echo-from-shard")],
    );
    let got = rx.try_recv().expect("the base port's monitor got nothing");
    assert!(String::from_utf8_lossy(&got).contains("echo-from-shard"));
    unregister_monitor_client(shard, 9_000_001);
    broadcast_monitor(base, "127.0.0.1:1", &[Bytes::from_static(b"after")]);
    assert!(
        rx.try_recv().is_err(),
        "unregistering by the shard port left it"
    );
}

#[test]
fn batch_has_writes_spots_any_write() {
    let parse = |line: &[u8]| {
        let mut b = bytes::BytesMut::from(line);
        crate::resp::parse_command(&mut b).unwrap().unwrap()
    };
    let get = parse(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    let set = parse(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
    let incr = parse(b"*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n");
    assert!(!super::batch_has_writes(&[get.clone(), get.clone()]));
    assert!(super::batch_has_writes(&[get.clone(), set]));
    assert!(super::batch_has_writes(&[incr, get]));
}

#[test]
fn take_doubling_chunk_runs_a_batch_1_2_4_in_order() {
    let mut rest: Vec<u32> = (0..20).collect();
    let mut chunk = 1;
    let mut sizes = Vec::new();
    let mut seen = Vec::new();
    while !rest.is_empty() {
        let part = super::take_doubling_chunk(&mut rest, &mut chunk);
        sizes.push(part.len());
        seen.extend(part);
    }
    assert_eq!(sizes, vec![1, 2, 4, 8, 5]);
    assert_eq!(seen, (0..20).collect::<Vec<_>>());
}

#[test]
fn unsubscribe_reply_is_binary_safe() {
    let r = super::unsubscribe_reply("unsubscribe", b"\xe0\x85N\xff", 3);
    assert_eq!(
        &r[..],
        &b"*3\r\n$11\r\nunsubscribe\r\n$4\r\n\xe0\x85N\xff\r\n:3\r\n"[..]
    );
}

use super::*;

#[test]
fn test_err_arg_keeps_an_error_reply_on_one_line() {
    assert!(matches!(
        err_arg("plain"),
        std::borrow::Cow::Borrowed("plain")
    ));
    assert_eq!(err_arg("a\r\n+OK\r\n"), "a  +OK  ");
    assert_eq!(err_arg("a\nb\rc"), "a b c");
}

/// A streamed full sync ends with a valid RDB end and checksum, then
/// the mark as the very last bytes: Redis replicas detect the end by the
/// mark ending a read, so the stream must not share the write.
#[test]
fn test_streamed_sync_tail_ends_with_the_mark() {
    let mut rdb = Vec::new();
    crate::redis_rdb::write_file_header(&mut rdb, 0);
    let crc = crate::table::crc64_update(0, &rdb);
    let mark = "0123456789abcdefghij0123456789ABCDEFGHIJ";
    for history in [None, Some(("f".repeat(40), 7, 0))] {
        let tail = streamed_sync_tail(history, crc, mark);
        assert!(tail.ends_with(mark.as_bytes()));
        let mut file = rdb.clone();
        file.extend_from_slice(&tail[..tail.len() - mark.len()]);
        let mut db = ShardDb::new(0);
        crate::table::load_rdb_bytes(&file, &mut db, 0, 1).unwrap();
    }
}

#[test]
fn integral_scores_format_like_printf_g17() {
    fn g17(v: f64) -> String {
        let mut buf = [0u8; 64];
        // SAFETY: as in `format_score`.
        let n = unsafe {
            libc::snprintf(
                buf.as_mut_ptr() as *mut libc::c_char,
                64,
                c"%.17g".as_ptr(),
                v,
            )
        };
        String::from_utf8_lossy(&buf[..n as usize]).into_owned()
    }
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut vals = vec![
        1.0,
        -1.0,
        7.0,
        42.0,
        -375.0,
        1e14,
        999_999_999_999_999.0,
        -999_999_999_999_999.0,
    ];
    for _ in 0..10_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let v = (x % 2_000_000_000_000_000) as f64 - 1e15;
        vals.push(v.trunc());
    }
    for v in vals {
        let mut d = [0u8; 20];
        match integral_score_digits(v, &mut d) {
            Some(start) => assert_eq!(std::str::from_utf8(&d[start..]).unwrap(), g17(v), "{v}"),
            None => assert!(v.abs() >= 1e15 || v == 0.0, "{v} should take the fast path"),
        }
        assert_eq!(
            format_score(v),
            if v == 0.0 { "0".to_string() } else { g17(v) },
            "{v}"
        );
    }
    assert_eq!(integral_score_digits(1.5, &mut [0u8; 20]), None);
    assert_eq!(integral_score_digits(1e15, &mut [0u8; 20]), None);
}

#[test]
fn test_oom_score_adj_targets() {
    let v = parse_oom_score_adj_values("0 200 800").unwrap();
    assert_eq!(v, [0, 200, 800]);
    assert_eq!(oom_score_adj_target("no", &v, 50, false), Ok(None));
    assert_eq!(oom_score_adj_target("yes", &v, 50, false), Ok(Some(50)));
    assert_eq!(
        oom_score_adj_target("relative", &v, 50, true),
        Ok(Some(250))
    );
    assert_eq!(
        oom_score_adj_target("ABSOLUTE", &v, 50, true),
        Ok(Some(200))
    );
    // Clamped to the kernel's range.
    let high = parse_oom_score_adj_values("2000 -2000 0").unwrap();
    assert_eq!(
        oom_score_adj_target("relative", &high, 100, false),
        Ok(Some(1000))
    );
    assert_eq!(
        oom_score_adj_target("absolute", &high, 0, true),
        Ok(Some(-1000))
    );
    assert!(oom_score_adj_target("maybe", &v, 0, false).is_err());
    for bad in ["0 200", "0 200 800 1", "0 x 800", "0 2001 800", ""] {
        assert!(parse_oom_score_adj_values(bad).is_err(), "{bad}");
    }
}

#[test]
fn test_pending_big_arg_start() {
    let big = 64 * 1024;
    let mut buf = format!("*3\r\n$3\r\nset\r\n$1\r\na\r\n${big}\r\n").into_bytes();
    let start = buf.len();
    assert_eq!(pending_big_arg_start(&buf), Some(start));
    buf.extend(std::iter::repeat_n(b'v', 1000));
    assert_eq!(pending_big_arg_start(&buf), Some(start));
    // A small pending argument, or an incomplete header: the whole buffer.
    assert_eq!(pending_big_arg_start(b"*2\r\n$3\r\nget\r\n$5\r\nab"), None);
    assert_eq!(pending_big_arg_start(b"*2\r\n$3\r\nget\r\n$700"), None);
}

#[test]
fn test_parse_maxmemory_clients() {
    assert_eq!(parse_maxmemory_clients("0"), Ok((0, 0)));
    assert_eq!(parse_maxmemory_clients(" 1gb "), Ok((1 << 30, 0)));
    assert_eq!(parse_maxmemory_clients("10%"), Ok((0, 10)));
    assert_eq!(parse_maxmemory_clients("100%"), Ok((0, 100)));
    assert!(parse_maxmemory_clients("101%").is_err());
    assert!(parse_maxmemory_clients("junk").is_err());
}

#[test]
fn test_maxmemory_samples_arg_range() {
    assert_eq!(maxmemory_samples_arg("1"), Ok(1));
    assert_eq!(maxmemory_samples_arg(" 64 "), Ok(64));
    for bad in ["0", "65", "-1", "five", ""] {
        assert!(maxmemory_samples_arg(bad).is_err(), "{bad}");
    }
}

/// `replica-ignore-maxmemory` (alias `slave-…`) defaults to yes and only
/// exempts a replica from `maxmemory`: a master keeps enforcing it, and
/// `no` makes a replica enforce it too.
#[test]
fn test_replica_ignore_maxmemory_gates_only_replicas() {
    use std::sync::atomic::Ordering;
    const PORT: u16 = 65011;
    assert!(
        REPLICA_IGNORE_MAXMEMORY.load(Ordering::Relaxed),
        "default yes"
    );
    assert!(!maxmemory_ignored_as_replica(PORT), "a master enforces it");

    let hub = crate::replication::cached_hub(PORT);
    crate::replication::HAS_SLAVE_INSTANCE.store(true, Ordering::Release);
    hub.is_slave_atomic.store(true, Ordering::Release);
    assert!(maxmemory_ignored_as_replica(PORT));

    assert_eq!(
        apply_config_value(PORT, PORT, "slave-ignore-maxmemory", "no"),
        Ok(true)
    );
    assert!(!maxmemory_ignored_as_replica(PORT), "`no` enforces it");
    assert!(apply_config_value(PORT, PORT, "replica-ignore-maxmemory", "maybe").is_err());
    assert!(!REPLICA_IGNORE_MAXMEMORY.load(Ordering::Relaxed));
    assert_eq!(
        apply_config_value(PORT, PORT, "replica-ignore-maxmemory", "YES"),
        Ok(true)
    );
    assert!(maxmemory_ignored_as_replica(PORT));
    hub.is_slave_atomic.store(false, Ordering::Release);
    assert!(!maxmemory_ignored_as_replica(PORT));
}

/// Client handlers run under `catch_unwind`; with std locks a panic
/// while holding a global lock poisoned it and every later `.lock()
/// .unwrap()` on any connection panicked too.
#[test]
fn test_panic_holding_global_lock_does_not_cascade() {
    let r = std::thread::spawn(|| {
        let _g = ERROR_STATS.write();
        panic!("handler bug while holding ERROR_STATS");
    })
    .join();
    assert!(r.is_err());
    record_error_stat("POISONTEST", Some("get"));
    assert!(ERROR_STATS.read().get("POISONTEST").copied().unwrap_or(0) >= 1);
}
use crate::resp::Command;
use bytes::Bytes;

#[test]
fn test_query_buffer_limit_checks() {
    let limit = 1024 * 1024;
    // Partial command: only bytes actually buffered count.
    assert!(!partial_query_over_limit(0, limit));
    assert!(!partial_query_over_limit(limit, limit));
    assert!(partial_query_over_limit(limit + 1, limit));
    // Complete command: its own length counts, not what follows it.
    assert!(!parsed_query_over_limit(100, 50, limit));
    assert!(!parsed_query_over_limit(10 * limit, 10 * limit - 64, limit));
    assert!(!parsed_query_over_limit(limit + 10, 10, limit));
    assert!(parsed_query_over_limit(limit + 10, 9, limit));
    assert!(parsed_query_over_limit(limit + 1, 0, limit));
    // Unlimited (u64::MAX on a 64-bit target) never trips.
    assert!(!partial_query_over_limit(usize::MAX - 1, usize::MAX));
}

/// Drives the parser the way the client loop does and applies the same
/// checks: a huge declared bulk that never completes trips the limit
/// once its buffered bytes pass it, while a pipeline of small commands
/// far larger than the limit in total does not.
#[test]
fn test_query_buffer_limit_with_parser() {
    let limit = 1024 * 1024;

    let mut buf = BytesMut::from(&b"*1\r\n$100000000\r\n"[..]);
    buf.extend_from_slice(&vec![b'a'; limit]);
    assert!(matches!(parse_command(&mut buf), Ok(None)));
    assert!(partial_query_over_limit(buf.len(), limit));

    let mut buf = BytesMut::new();
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    while buf.len() <= 4 * limit {
        buf.extend_from_slice(cmd);
    }
    buf.extend_from_slice(b"*3\r\n$3\r\nSET"); // trailing partial frame
    let mut parsed = 0usize;
    loop {
        let before = buf.len();
        match parse_command(&mut buf) {
            Ok(Some(_)) => {
                assert!(!parsed_query_over_limit(before, buf.len(), limit));
                parsed += 1;
            }
            Ok(None) => {
                assert!(!partial_query_over_limit(buf.len(), limit));
                break;
            }
            Err(e) => panic!("unexpected parse error: {e}"),
        }
    }
    assert!(parsed * cmd.len() > 4 * limit);

    // One complete command larger than the limit trips it.
    let mut buf = BytesMut::from(format!("*2\r\n$4\r\nECHO\r\n${}\r\n", limit).as_bytes());
    buf.extend_from_slice(&vec![b'x'; limit]);
    buf.extend_from_slice(b"\r\n");
    let before = buf.len();
    assert!(matches!(parse_command(&mut buf), Ok(Some(_))));
    assert!(parsed_query_over_limit(before, buf.len(), limit));
}

#[test]
fn test_client_default_resp_config_accepts_only_2_or_3() {
    // The parser only: setting the process-wide default here would race
    // other tests' connections.
    assert_eq!(parse_client_default_resp("3"), Ok(true));
    assert_eq!(parse_client_default_resp("2"), Ok(false));
    assert!(parse_client_default_resp("4").is_err());
    assert!(parse_client_default_resp("").is_err());
}

#[test]
fn test_client_query_buffer_limit_config_minimum() {
    let before = CONFIG_CLIENT_QUERY_BUFFER_LIMIT.load(std::sync::atomic::Ordering::Relaxed);
    assert!(apply_config_value(0, 0, "client-query-buffer-limit", "1000").is_err());
    assert!(apply_config_value(0, 0, "client-query-buffer-limit", "0").is_err());
    assert!(apply_config_value(0, 0, "client-query-buffer-limit", "bogus").is_err());
    assert_eq!(
        CONFIG_CLIENT_QUERY_BUFFER_LIMIT.load(std::sync::atomic::Ordering::Relaxed),
        before
    );
}

#[test]
fn test_take_simple_replies_keeps_order_and_partial_lines() {
    let mut replies = Vec::new();
    let buf = b"+OK\r\n-BUSYKEY Target key name already exists.\r\n:1\r\n+O";
    let used = take_simple_replies(buf, &mut replies);
    // The trailing "+O" is incomplete and stays for the next read.
    assert_eq!(used, buf.len() - 2);
    assert_eq!(replies.len(), 3);
    assert_eq!(replies[0], Ok(()));
    assert_eq!(
        replies[1],
        Err("BUSYKEY Target key name already exists.".to_string())
    );
    assert_eq!(replies[2], Ok(()), "an integer reply (DEL's) is an OK");
    let mut bulk = Vec::new();
    assert_eq!(take_simple_replies(b"$2\r\n", &mut bulk), 4);
    assert!(bulk[0].is_err(), "a bulk reply is not an OK");
    assert_eq!(take_simple_replies(b"+OK\r\n", &mut replies), 5);
    assert_eq!(replies[3], Ok(()));
}

#[test]
fn test_write_resp_command_encodes_bulk_array() {
    let mut out = Vec::new();
    write_resp_command(&mut out, &[b"RESTORE", b"k", b"0", b""]);
    assert_eq!(
        out,
        b"*4\r\n$7\r\nRESTORE\r\n$1\r\nk\r\n$1\r\n0\r\n$0\r\n\r\n"
    );
}

#[test]
fn test_crdt_primary_key_and_target_shard() {
    let num_shards = 4;
    let k = Bytes::from("crdt:test:key");
    let expected_shard = target_shard(&k, num_shards);

    let cmds = vec![
        Command::CrdtSet {
            key: k.clone(),
            val: Bytes::from("val"),
        },
        Command::CrdtGet(k.clone()),
        Command::CrdtDel(k.clone()),
        Command::CrdtIncrby {
            key: k.clone(),
            delta: 10,
        },
        Command::CrdtSadd {
            key: k.clone(),
            member: Bytes::from("m1"),
        },
        Command::CrdtSmembers(k.clone()),
        Command::CrdtSrem {
            key: k.clone(),
            member: Bytes::from("m1"),
        },
    ];

    for cmd in &cmds {
        assert_eq!(cmd_primary_key(cmd), Some(&k));
        assert_eq!(target_shard_of_cmd(cmd, num_shards), Some(expected_shard));
    }
}

/// CRDT.MERGE reports the keys in its payload (so scripts, WATCH and ACL
/// see them) but has no single shard: the router splits it.
#[test]
fn test_crdt_merge_keys_come_from_its_payload() {
    let mut store = crate::crdt::CrdtStore::new(1);
    store.set(Bytes::from_static(b"r"), Bytes::from_static(b"v"));
    store.counter_incr(Bytes::from_static(b"c"), 1).unwrap();
    store.set_add(Bytes::from_static(b"s"), Bytes::from_static(b"m"));
    let cmd = Command::CrdtMerge(store.export_sync_payload().into());
    let mut keys = cmd_keys(&cmd);
    keys.sort();
    assert_eq!(keys, vec![&b"c"[..], b"r", b"s"]);
    assert_eq!(target_shard_of_cmd(&cmd, 4), None);
    assert!(cmd_keys(&Command::CrdtMerge(Bytes::from_static(b"\x01\xff"))).is_empty());
}

#[test]
fn test_cluster_key_slot_and_tag_extraction() {
    use crate::router::{extract_hash_tag, key_slot};

    let key1 = b"user1000";
    let slot1 = key_slot(key1);
    assert!(slot1 < 16384);

    // Keys with identical hash tags must hash to identical cluster slots
    let tagged1 = b"{user:123}:profile";
    let tagged2 = b"{user:123}:settings";
    assert_eq!(extract_hash_tag(tagged1), b"user:123");
    assert_eq!(extract_hash_tag(tagged2), b"user:123");
    assert_eq!(key_slot(tagged1), key_slot(tagged2));
}

#[test]
fn test_target_shard_of_cmd_mget_mset() {
    let num_shards = 4;
    let k1 = Bytes::from("{user:123}:a");
    let k2 = Bytes::from("{user:123}:b");
    let expected_shard = target_shard(&k1, num_shards);
    assert_eq!(target_shard(&k2, num_shards), expected_shard);

    // Co-located MGET
    let mget_colocated = Command::Mget(vec![k1.clone(), k2.clone()]);
    assert_eq!(
        target_shard_of_cmd(&mget_colocated, num_shards),
        Some(expected_shard)
    );

    // Co-located MSET
    let mset_colocated = Command::Mset(vec![
        (k1.clone(), Bytes::from("val1")),
        (k2.clone(), Bytes::from("val2")),
    ]);
    assert_eq!(
        target_shard_of_cmd(&mset_colocated, num_shards),
        Some(expected_shard)
    );

    // Non-colocated MGET
    let mut diff_key = Bytes::from("different_key");
    for i in 0..1000 {
        let candidate = Bytes::from(format!("k_{}", i));
        if target_shard(&candidate, num_shards) != expected_shard {
            diff_key = candidate;
            break;
        }
    }
    let mget_cross = Command::Mget(vec![k1.clone(), diff_key.clone()]);
    assert_eq!(target_shard_of_cmd(&mget_cross, num_shards), None);

    let mset_cross = Command::Mset(vec![
        (k1, Bytes::from("val1")),
        (diff_key, Bytes::from("val2")),
    ]);
    assert_eq!(target_shard_of_cmd(&mset_cross, num_shards), None);
}

#[test]
fn test_write_resp_array_header() {
    let mut buf = Vec::new();
    write_resp_array_header(&mut buf, 0);
    assert_eq!(buf, b"*0\r\n");

    buf.clear();
    write_resp_array_header(&mut buf, 5);
    assert_eq!(buf, b"*5\r\n");

    buf.clear();
    write_resp_array_header(&mut buf, 50);
    assert_eq!(buf, b"*50\r\n");

    buf.clear();
    write_resp_array_header(&mut buf, 1024);
    assert_eq!(buf, b"*1024\r\n");
}

#[test]
fn test_mode_switch_denied_enforces_auth_and_acl() {
    let port = 65021;
    let psync = Command::Psync {
        replid: Bytes::from_static(b"?"),
        offset: -1,
        failover: false,
    };
    let sub = Command::Subscribe(vec![Bytes::from_static(b"ch")]);
    // Not a mode switch: never gated here.
    assert!(mode_switch_denied(&Command::Ping(None), port, false, "default").is_none());
    // Unauthenticated clients may not switch modes.
    for c in [&psync, &Command::Sync, &sub] {
        let e = mode_switch_denied(c, port, false, "default").unwrap();
        assert!(e.starts_with(b"-NOAUTH"));
    }
    // Authenticated default user with no custom ACL: allowed.
    assert!(mode_switch_denied(&psync, port, true, "default").is_none());
    // Restricted user lacks the command.
    crate::acl::get_acl_for_port(port)
        .write()
        .set_user(
            "limited",
            &["on", "nopass", "-@all", "+get", "+subscribe", "&ch"].map(String::from),
        )
        .unwrap();
    let e = mode_switch_denied(&psync, port, true, "limited").unwrap();
    assert!(e.starts_with(b"-NOPERM"));
    assert!(mode_switch_denied(&sub, port, true, "limited").is_none());
    // Allowed command, but a channel outside the user's & patterns.
    let other = Command::Subscribe(vec![Bytes::from_static(b"ch"), Bytes::from_static(b"x")]);
    let e = mode_switch_denied(&other, port, true, "limited").unwrap();
    assert!(e.starts_with(b"-NOPERM") && e.windows(8).any(|w| w == b"channels"));
}

#[test]
fn test_tracking_clients_atomic_fast_path() {
    let port = 65432;
    let cid = 999999;
    register_client_tracking(port, cid, None, false, Vec::new(), false, false, false).unwrap();
    assert!(HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed));

    record_client_read(port, cid, b"test_key");

    unregister_client_tracking(port, cid);
}

#[test]
fn test_latency_percentile_interpolates_in_bucket() {
    let mut hist = [0u64; LATENCY_BUCKETS];
    // 90 calls <= 1 us, 10 calls in (512, 1024] us.
    hist[0] = 90;
    hist[10] = 10;
    assert!((latency_percentile_usec(&hist, 100, 50.0) - 50.0 / 90.0).abs() < 1e-9);
    assert_eq!(latency_percentile_usec(&hist, 100, 90.0), 1.0);
    assert_eq!(latency_percentile_usec(&hist, 100, 95.0), 768.0);
    assert_eq!(latency_percentile_usec(&hist, 100, 100.0), 1024.0);
    assert_eq!(latency_percentile_usec(&hist, 0, 50.0), 0.0);
    assert_eq!(format_percentile(99.9), "99.9");
    assert_eq!(format_percentile(50.0), "50");
}

#[test]
fn test_unwatch_drops_keys_nobody_watches() {
    let port = 65429;
    watch_keys(port, 1, &[Bytes::from("w:a"), Bytes::from("w:b")]);
    watch_keys(port, 2, &[Bytes::from("w:b")]);
    unwatch_keys(port, 1);
    {
        let map = WATCHED_KEYS.read();
        let pm = map.get(&port).expect("client 2 still watches w:b");
        assert!(!pm.contains_key(&(0, Bytes::from("w:a"))));
        assert_eq!(pm.len(), 1);
    }
    unwatch_keys(port, 2);
    assert!(!WATCHED_KEYS.read().contains_key(&port));
}

/// EXEC expires a client's watched keys first: `watched_keys_of` lists
/// them with their database, and nobody else's.
#[test]
fn test_watched_keys_of_lists_a_clients_keys_with_their_db() {
    let port = 65427;
    CURRENT_DB.set(3);
    watch_keys(port, 7, &[Bytes::from("a"), Bytes::from("b")]);
    CURRENT_DB.set(0);
    watch_keys(port, 8, &[Bytes::from("c")]);
    let mut keys = watched_keys_of(port, 7);
    keys.sort();
    assert_eq!(keys, vec![(3, Bytes::from("a")), (3, Bytes::from("b"))]);
    assert_eq!(watched_keys_of(port, 8), vec![(0, Bytes::from("c"))]);
    unwatch_keys(port, 7);
    unwatch_keys(port, 8);
    assert!(watched_keys_of(port, 7).is_empty());
}

#[test]
fn test_watched_keys_atomic_bypass() {
    let port = 65431;
    let cid = 888888;
    assert!(!is_watch_tainted(port, cid));

    // When no keys are watched, touch_watched_key returns immediately
    touch_watched_key(port, b"unwatched_key");
    assert!(!is_watch_tainted(port, cid));

    // Watch a key -> HAS_WATCHED_KEYS becomes true
    watch_keys(port, cid, &[Bytes::from("watched_k")]);
    assert!(HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed));
    assert!(!is_watch_tainted(port, cid));

    // Mutate watched key -> marks client tainted
    touch_watched_key(port, b"watched_k");
    assert!(is_watch_tainted(port, cid));

    // Unwatch clears taint
    unwatch_keys(port, cid);
    assert!(!is_watch_tainted(port, cid));
}

/// A concurrent UNWATCH on another port must never clear
/// `HAS_WATCHED_KEYS` while a key is still watched; otherwise writes skip
/// the taint check and EXEC runs on stale data.
#[test]
fn test_concurrent_unwatch_never_clears_the_watched_flag() {
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let churn = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                watch_keys(65433, 1, &[Bytes::from("churn")]);
                unwatch_keys(65433, 1);
            }
        })
    };
    for _ in 0..20_000 {
        watch_keys(65432, 2, &[Bytes::from("held")]);
        touch_watched_key(65432, b"held");
        assert!(
            is_watch_tainted(65432, 2),
            "write to a watched key was missed"
        );
        unwatch_keys(65432, 2);
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    churn.join().unwrap();
}

#[test]
fn test_fragmented_frame_parsing_and_buffer_growth() {
    let mut buf = BytesMut::new();
    // Feed partial command
    buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1");
    assert!(matches!(parse_command(&mut buf), Ok(None)));
    assert_eq!(buf.len(), 21);

    // Feed remainder of command plus a complete second command
    buf.extend_from_slice(b"\r\n$4\r\nval1\r\n*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n");
    let cmd1 = parse_command(&mut buf).unwrap().unwrap();
    assert!(matches!(cmd1, Command::Set { .. }));

    let cmd2 = parse_command(&mut buf).unwrap().unwrap();
    assert!(matches!(cmd2, Command::Get(_)));
    assert!(buf.is_empty());
}

#[test]
fn test_pipeline1_execute_command_lockless_bypass() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
        let db = Rc::new(RefCell::new(ShardDb::new(9993)));
        let router = Router::new(
            0,
            1,
            9993,
            db,
            senders_mesh[0].clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let client_registry = RefCell::new(hashbrown::HashMap::new());
        let mut out = Vec::new();
        let mut asking = false;
        let mut authenticated = true;
        let mut auth_user = String::from("default");

        // Execute SET
        let set_cmd = Command::Set {
            key: Bytes::from("pipe1_k"),
            value: Bytes::from("pipe1_v"),
            expire_in: None,
            condition: crate::resp::SetCondition::None,
            get: false,
            keepttl: false,
            past_expired: false,
        };
        let quit = execute_command(
            set_cmd,
            &router,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        assert!(!quit);
        assert_eq!(out, b"+OK\r\n");

        // Execute GET
        out.clear();
        let get_cmd = Command::Get(Bytes::from("pipe1_k"));
        let quit = execute_command(
            get_cmd,
            &router,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        assert!(!quit);
        assert_eq!(out, b"$7\r\npipe1_v\r\n");
    });
}

#[test]
fn test_execute_command_local_sync_fast_path_and_mget_colocated() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
        let db = Rc::new(RefCell::new(ShardDb::new(9994)));
        let router = Router::new(
            0,
            1,
            9994,
            db,
            senders_mesh[0].clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let client_registry = RefCell::new(hashbrown::HashMap::new());
        let mut asking = false;
        let mut authenticated = true;
        let mut auth_user = String::from("default");
        let mut out = Vec::new();

        // Set a key
        let set_cmd = Command::Set {
            key: Bytes::from("local_k1"),
            value: Bytes::from("local_v1"),
            expire_in: None,
            condition: crate::resp::SetCondition::None,
            get: false,
            keepttl: false,
            past_expired: false,
        };
        execute_command(
            set_cmd,
            &router,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        assert_eq!(out, b"+OK\r\n");

        // Local GET hit
        out.clear();
        execute_command(
            Command::Get(Bytes::from("local_k1")),
            &router,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        assert_eq!(out, b"$8\r\nlocal_v1\r\n");

        // Local GET miss
        out.clear();
        execute_command(
            Command::Get(Bytes::from("missing_k")),
            &router,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        assert_eq!(out, b"$-1\r\n");

        // Local co-located MGET
        out.clear();
        execute_command(
            Command::Mget(vec![Bytes::from("local_k1"), Bytes::from("missing_k")]),
            &router,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        assert_eq!(out, b"*2\r\n$8\r\nlocal_v1\r\n$-1\r\n");
    });
}

#[test]
fn test_cmd_stats_thread_local_buffering_and_flush() {
    record_cmd_stat("GET");
    record_cmd_stat("GET");
    record_cmd_stat("SET");

    // Flush and verify in global map
    flush_local_cmd_stats();
    let map = CMD_STATS.read();
    assert!(map.get("get").map_or(0, |s| s.calls) >= 2);
    assert!(map.get("set").map_or(0, |s| s.calls) >= 1);
}

#[test]
fn test_cmd_stat_batch_counts_and_times_every_command() {
    let pending =
        |n: &str| LOCAL_CMD_STATS.with(|m| m.borrow().get(n).copied().unwrap_or_default());
    let mut batch = CmdStatBatch::new();
    for name in ["GET", "GET", "SET", "GET", "GET"] {
        batch.note(name);
    }
    std::thread::sleep(std::time::Duration::from_millis(5));
    batch.flush();
    let (get, set) = (pending("GET"), pending("SET"));
    assert_eq!((get.calls, set.calls), (4, 1));
    // The segment's time is split 4:1 between GET and SET.
    assert!(set.nanos >= 1_000_000, "SET nanos {}", set.nanos);
    assert!(
        get.nanos >= 4 * set.nanos - 4,
        "GET {} SET {}",
        get.nanos,
        set.nanos
    );

    // Time between flush and restart is not attributed to anything.
    batch.flush();
    std::thread::sleep(std::time::Duration::from_millis(20));
    batch.restart();
    batch.note("GET");
    batch.flush();
    let after = pending("GET");
    assert_eq!(after.calls, 5);
    assert!(
        after.nanos - get.nanos < 20_000_000,
        "{}",
        after.nanos - get.nanos
    );
}

#[test]
fn test_blocking_commands_are_not_timed() {
    let blpop = Command::Blpop {
        keys: vec![Bytes::from("k")],
        timeout: 0.0,
    };
    assert!(may_block(&blpop));
    assert!(!may_block(&Command::Get(Bytes::from("k"))));
}

#[test]
fn test_incr_family_keeps_command_name() {
    for (wire, name, delta, argv) in [
        ("INCR", "INCR", 1, "incr k"),
        ("DECR", "DECR", -1, "decr k"),
        ("INCRBY 1", "INCRBY", 1, "incrby k 1"),
        ("DECRBY 1", "DECRBY", -1, "decrby k 1"),
        ("INCRBY -5", "INCRBY", -5, "incrby k -5"),
    ] {
        let mut parts: Vec<&str> = wire.split(' ').collect();
        parts.insert(1, "k");
        let mut raw = format!("*{}\r\n", parts.len());
        for p in &parts {
            raw.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        let mut buf = bytes::BytesMut::from(raw.as_str());
        let cmd = crate::resp::parse_command(&mut buf).unwrap().unwrap();
        assert!(
            matches!(cmd, Command::IncrBy(_, d, _) if d == delta),
            "{wire}: {cmd:?}"
        );
        assert_eq!(get_cmd_name(&cmd), name, "{wire}");
        assert_eq!(acl_cmd_name(&cmd), name, "{wire}");
        let (args, _) = crate::slowlog::command_to_slowlog_argv(&cmd);
        let shown: Vec<String> = args
            .iter()
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .collect();
        assert_eq!(shown.join(" "), argv, "{wire}");
    }
}

#[test]
fn test_latency_histogram_buckets_and_reply() {
    let buckets: Vec<usize> = [0, 1, 2, 3, 4, 5, 128, 129, 1 << 40]
        .iter()
        .map(|&us| latency_bucket(us))
        .collect();
    assert_eq!(buckets, [0, 0, 1, 2, 2, 3, 7, 8, LATENCY_BUCKETS - 1]);

    let mut stat = CmdStat::default();
    stat.add(1, 500); // < 1us
    stat.add(1, 3_000); // 3us -> <= 4us
    stat.add(2, 200_000); // two calls averaging 100us -> <= 128us
    let mut merged = CmdStat::default();
    merged.merge(&stat);
    assert_eq!((merged.calls, merged.nanos), (4, 203_500));
    CMD_STATS
        .write()
        .insert("unittest|histo".to_string(), merged);

    let mut out = Vec::new();
    write_latency_response(
        &LatencySubcommand::Histogram(vec!["UNITTEST".into()]),
        &mut out,
    );
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "*2\r\n$14\r\nunittest|histo\r\n*4\r\n$5\r\ncalls\r\n:4\r\n$14\r\nhistogram_usec\r\n\
             *6\r\n:1\r\n:1\r\n:4\r\n:2\r\n:128\r\n:4\r\n"
    );
    // A prefix that is not a whole parent command name does not match.
    let mut out = Vec::new();
    write_latency_response(
        &LatencySubcommand::Histogram(vec!["unittest|h".into()]),
        &mut out,
    );
    assert_eq!(out, b"*0\r\n");
    CMD_STATS.write().remove("unittest|histo");
}

#[test]
fn test_only_loading_flagged_commands_run_while_loading() {
    for cmd in [
        Command::Role,
        Command::Time,
        Command::Info(None),
        Command::Client(ClientSubcommand::Id),
        Command::PubsubNumpat,
    ] {
        assert!(allowed_while_loading(&cmd), "{}", get_cmd_name(&cmd));
    }
    for cmd in [
        Command::Dbsize,
        Command::Flushall,
        Command::Get(Bytes::from_static(b"k")),
    ] {
        assert!(!allowed_while_loading(&cmd), "{}", get_cmd_name(&cmd));
    }
}

#[test]
fn test_isolated_panics_counter() {
    let initial = get_isolated_panics();
    inc_isolated_panics();
    assert_eq!(get_isolated_panics(), initial + 1);
}

#[test]
fn test_cmd_keys_multi_key_acl_coverage() {
    let mget = Command::Mget(vec![
        Bytes::from("k1"),
        Bytes::from("k2"),
        Bytes::from("k3"),
    ]);
    assert_eq!(cmd_keys(&mget), vec![b"k1", b"k2", b"k3"]);

    let mset = Command::Mset(vec![
        (Bytes::from("k1"), Bytes::from("v1")),
        (Bytes::from("k2"), Bytes::from("v2")),
    ]);
    assert_eq!(cmd_keys(&mset), vec![b"k1", b"k2"]);

    let del = Command::Del(smallvec![Bytes::from("d1"), Bytes::from("d2")]);
    assert_eq!(cmd_keys(&del), vec![b"d1", b"d2"]);

    let sinter = Command::Sinter(vec![Bytes::from("s1"), Bytes::from("s2")]);
    assert_eq!(cmd_keys(&sinter), vec![b"s1", b"s2"]);
}

#[test]
fn test_recv_bytes_mut_zero_copy_iobuf_mut() {
    use monoio::buf::IoBufMut;

    let mut b = BytesMut::with_capacity(64);
    b.extend_from_slice(b"*2\r\n$3\r\nGET\r\n");
    let mut recv_buf = RecvBytesMut(b);
    assert_eq!(recv_buf.bytes_total(), 64 - 13);
    let suffix = b"$4\r\nmyk1\r\n";
    // SAFETY: `suffix.len()` (10) is within `bytes_total()` (51), the source is a
    // separate static array so the ranges don't overlap, and `set_init` covers
    // exactly the bytes just written.
    unsafe {
        std::ptr::copy_nonoverlapping(suffix.as_ptr(), recv_buf.write_ptr(), suffix.len());
        recv_buf.set_init(suffix.len());
    }
    let mut recovered = recv_buf.0;
    let cmd = parse_command(&mut recovered).unwrap().unwrap();
    assert_eq!(cmd, Command::Get(Bytes::from_static(b"myk1")));
    assert!(recovered.is_empty());
    assert!(!recovered.try_reclaim(64));
    drop(cmd);
    assert!(recovered.try_reclaim(64));
    assert_eq!(recovered.capacity(), 64);
}

#[test]
fn test_conn_scratch_pool_reuse_and_inflight_responder_guard() {
    // Drain anything a previous test on this thread may have pooled.
    CONN_SCRATCH_POOL.with(|p| p.borrow_mut().clear());

    // 1. An idle scratch round-trips through the pool and reuses its allocations.
    let mut scratch = take_conn_scratch(4);
    assert_eq!(scratch.responders.len(), 4);
    let out_ptr = scratch.out_buf.as_ptr();
    scratch.out_buf.extend_from_slice(b"+OK\r\n");
    scratch
        .commands
        .push(Command::Get(Bytes::from_static(b"k")));
    recycle_conn_scratch(scratch);

    let reused = take_conn_scratch(4);
    assert!(reused.out_buf.is_empty(), "pooled out_buf must be cleared");
    assert!(
        reused.commands.is_empty(),
        "pooled commands must be cleared"
    );
    assert_eq!(
        reused.out_buf.as_ptr(),
        out_ptr,
        "pooled scratch must reuse the same heap allocation"
    );

    // 2. Scratch whose responder is still referenced by an in-flight remote shard
    //    batch must NOT be pooled, or a late finish() would write another client's
    //    results into the next connection's responder.
    let inflight = std::sync::Arc::clone(&reused.responders[2]);
    recycle_conn_scratch(reused);
    CONN_SCRATCH_POOL.with(|p| {
        assert!(
            p.borrow().is_empty(),
            "scratch with an outstanding responder Arc must not be pooled"
        );
    });
    drop(inflight);

    // 3. Scratch carrying an unharvested ready=true payload must also be rejected.
    let stale = take_conn_scratch(4);
    stale.responders[1].state.0.store(
        crate::mailbox::BATCH_COMPLETED,
        std::sync::atomic::Ordering::Release,
    );
    recycle_conn_scratch(stale);
    CONN_SCRATCH_POOL.with(|p| {
        assert!(
            p.borrow().is_empty(),
            "scratch with a stale ready responder must not be pooled"
        );
    });

    // 4. A different shard count never aliases a pooled entry.
    let clean = take_conn_scratch(4);
    recycle_conn_scratch(clean);
    let other = take_conn_scratch(8);
    assert_eq!(other.responders.len(), 8);
    assert_eq!(other.remote_batches.len(), 8);
    recycle_conn_scratch(other);
}

#[test]
fn test_keys_span_shards() {
    let shard_of = |k: &[u8]| crate::router::target_shard(k, 4);
    assert!(!keys_span_shards(&[], shard_of));
    assert!(!keys_span_shards(&[Bytes::from("a")], shard_of));
    let tagged = [
        Bytes::from("{t}a"),
        Bytes::from("{t}b"),
        Bytes::from("{t}c"),
    ];
    assert!(!keys_span_shards(&tagged, shard_of));
    // With 4 shards, some pair of plain keys must land on different shards.
    let keys: Vec<Bytes> = (0..16).map(|i| Bytes::from(format!("k{i}"))).collect();
    assert!(keys_span_shards(&keys, shard_of));
    // Only the first key's owner matters as the reference shard.
    assert!(keys_span_shards(
        &[Bytes::from("x"), Bytes::from("y")],
        |k| k[0] as usize
    ));
}

#[test]
fn test_write_mget_reply_maps_errors_to_nil() {
    let items = [
        CompactResp::from_bulk(&Bytes::from("v1")),
        CompactResp::null(false),
        CompactResp::from_slice(b"-WRONGTYPE Operation against a key\r\n"),
        CompactResp::Bulk(Bytes::from(vec![b'x'; 40])),
    ];
    let mut got = Vec::new();
    write_mget_reply(&mut got, &items, false);
    let mut want = b"*4\r\n$2\r\nv1\r\n$-1\r\n$-1\r\n$40\r\n".to_vec();
    want.extend_from_slice(&[b'x'; 40]);
    want.extend_from_slice(b"\r\n");
    assert_eq!(got, want);
    let resp3_items = [CompactResp::null(true), items[2].clone()];
    let mut resp3 = Vec::new();
    write_mget_reply(&mut resp3, &resp3_items, true);
    assert_eq!(resp3, b"*2\r\n_\r\n_\r\n");
    let mut empty = Vec::new();
    write_mget_reply(&mut empty, &[], false);
    assert_eq!(empty, b"*0\r\n");
}

#[test]
fn test_notifies_same_when_batched_allows_reads_and_checked_writes() {
    assert!(notifies_same_when_batched(&Command::Get(Bytes::from("k"))));
    assert!(notifies_same_when_batched(&Command::Mget(vec![
        Bytes::from("k")
    ])));
    assert!(notifies_same_when_batched(&Command::Del(smallvec![
        Bytes::from("k")
    ])));
    assert!(notifies_same_when_batched(&Command::IncrBy(
        Bytes::from("k"),
        1,
        crate::resp::IncrName::Incr
    )));
    // MSET is a write that was not checked: it keeps the pipeline unbatched.
    assert!(!notifies_same_when_batched(&Command::Mset(vec![(
        Bytes::from("k"),
        Bytes::from("v")
    )])));
}

#[test]
fn test_resp_integer_or_zero() {
    assert_eq!(resp_integer_or_zero(b":0\r\n"), 0);
    assert_eq!(resp_integer_or_zero(b":42\r\n"), 42);
    assert_eq!(resp_integer_or_zero(b"-ERR boom\r\n"), 0);
    assert_eq!(resp_integer_or_zero(b"$1\r\n5\r\n"), 0);
    assert_eq!(resp_integer_or_zero(b":-1\r\n"), 0);
    assert_eq!(resp_integer_or_zero(b""), 0);
}

/// Runs each command on a one-shard server and checks the exact keyevent
/// names it publishes, in order.
fn assert_commands_publish(cases: &'static [(&'static [&'static str], &'static [&'static str])]) {
    let _flags = NOTIFY_FLAGS_TEST_LOCK.lock();
    // execute_command's future is too deep for the default test stack in debug builds.
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(move || {
            monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .enable_all()
                .build()
                .unwrap()
                .block_on(commands_publish(cases))
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn commands_publish(cases: &[(&[&str], &[&str])]) {
    let dir = std::env::temp_dir().join(format!("rudis-notify-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
    let pubsub = std::rc::Rc::new(std::cell::RefCell::new(crate::pubsub::PubSubHub::new()));
    let router = std::rc::Rc::new(Router::new(
        0,
        1,
        6397,
        std::rc::Rc::new(std::cell::RefCell::new(crate::shard::ShardDb::new(6397))),
        senders_mesh[0].clone(),
        None,
        pubsub.clone(),
        dir,
    ));
    set_current_router(router.clone());
    let (tx, rx) = flume::unbounded();
    pubsub.borrow_mut().psubscribe(
        1,
        Bytes::from_static(b"__keyevent@0__:*"),
        crate::pubsub::SubscriberTx::new(tx, 0, 0),
        false,
    );
    let prev_flags = get_notify_keyspace_events_str();
    set_notify_keyspace_events_str("KEA");

    let client_registry = std::cell::RefCell::new(hashbrown::HashMap::new());
    let (mut asking, mut authenticated) = (false, true);
    let mut auth_user = String::from("default");
    for (args, events) in cases {
        let mut wire = bytes::BytesMut::from(format!("*{}\r\n", args.len()).as_bytes());
        for a in args.iter() {
            wire.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        let cmd = crate::resp::parse_command(&mut wire).unwrap().unwrap();
        let mut out = Vec::new();
        execute_command(
            cmd,
            &router,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        assert!(
            !out.starts_with(b"-"),
            "{args:?}: {}",
            String::from_utf8_lossy(&out)
        );
        let got: Vec<String> = rx
            .try_iter()
            .map(|f| {
                let f = String::from_utf8_lossy(&f).to_string();
                let event = f.split("__keyevent@0__:").nth(2).unwrap_or("");
                event.split("\r\n").next().unwrap_or("").to_string()
            })
            .collect();
        assert_eq!(got, *events, "{args:?}");
    }
    set_notify_keyspace_events_str(&prev_flags);
}

#[test]
fn test_collection_writes_publish_keyspace_events() {
    assert_commands_publish(&[
        (&["HSETNX", "h", "f", "1"], &["hset"]),
        (&["HSETNX", "h", "f", "2"], &[]),
        (&["HMSET", "h", "g", "1"], &["hset"]),
        (&["HINCRBY", "h", "f", "2"], &["hincrby"]),
        (&["HINCRBYFLOAT", "h", "g", "0.5"], &["hincrbyfloat"]),
        (&["RPUSH", "l", "a", "b", "c", "b"], &["rpush"]),
        (&["LSET", "l", "0", "z"], &["lset"]),
        (&["LINSERT", "l", "BEFORE", "c", "y"], &["linsert"]),
        (&["LINSERT", "l", "BEFORE", "nope", "y"], &[]),
        (&["LREM", "l", "0", "b"], &["lrem"]),
        (&["LREM", "l", "0", "b"], &[]),
        (&["LTRIM", "l", "0", "0"], &["ltrim"]),
        (&["LTRIM", "missing", "0", "0"], &[]),
        (&["LREM", "l", "0", "z"], &["lrem", "del"]),
        (&["SADD", "s", "a"], &["sadd"]),
        (&["SPOP", "s"], &["spop", "del"]),
        (&["SPOP", "s"], &[]),
        (&["ZADD", "z", "1", "a", "2", "b"], &["zadd"]),
        (&["ZPOPMIN", "z"], &["zpopmin"]),
        (&["ZPOPMAX", "z"], &["zpopmax", "del"]),
        (&["RPUSH", "t", "a"], &["rpush"]),
        (&["LTRIM", "t", "1", "0"], &["ltrim", "del"]),
    ]);
}

/// A plain SET does not look up the old value: no keyspace hit or miss
/// (as in Redis), unlike SET ... GET and the IF* conditions.
#[test]
fn test_plain_set_counts_no_keyspace_lookup() {
    use crate::server_stats::{Stat, thread_count};
    let mut db = crate::shard::ShardDb::new(6397);
    let aof = RefCell::new(crate::aof::AofWriter::new_in_memory());
    let mut out = Vec::new();
    let mut run = |args: &[&str]| {
        let cmd =
            crate::resp::build_command(args.iter().map(|a| Bytes::from(a.to_string())).collect())
                .unwrap()
                .unwrap();
        execute_local_command(&cmd, &mut db, &mut out, Some(&aof));
    };
    let lookups = || thread_count(Stat::KeyspaceHits) + thread_count(Stat::KeyspaceMisses);
    let before = lookups();
    run(&["SET", "k", "v1"]);
    run(&["SET", "k", "v2"]);
    run(&["SET", "k", "v3", "NX"]);
    assert_eq!(lookups(), before, "plain SETs looked keys up");
    run(&["SET", "k", "v4", "GET"]);
    assert_eq!(lookups(), before + 1);
    assert_eq!(out, b"+OK\r\n+OK\r\n$-1\r\n$2\r\nv2\r\n");
}

#[test]
fn test_getex_records_ttl_change_in_aof() {
    let mut db = crate::shard::ShardDb::new(6396);
    let aof = RefCell::new(crate::aof::AofWriter::new_in_memory());
    let mut out = Vec::new();
    db.set(Bytes::from("k"), Bytes::from("v"), None);
    let getex = |expire_in, persist| Command::Getex {
        key: Bytes::from("k"),
        expire_in,
        persist,
    };

    // A plain GETEX changes nothing and logs nothing.
    execute_local_command(&getex(None, false), &mut db, &mut out, Some(&aof));
    assert!(aof.borrow().buffer().is_empty());

    execute_local_command(
        &getex(Some(std::time::Duration::from_secs(100)), false),
        &mut db,
        &mut out,
        Some(&aof),
    );
    let logged = String::from_utf8_lossy(aof.borrow().buffer()).to_uppercase();
    assert!(logged.contains("EXPIRE"), "{logged:?}");

    let before = aof.borrow().buffer().len();
    execute_local_command(&getex(None, true), &mut db, &mut out, Some(&aof));
    let logged = String::from_utf8_lossy(&aof.borrow().buffer()[before..]).to_uppercase();
    assert!(logged.contains("PERSIST"), "{logged:?}");
    assert_eq!(out, b"$1\r\nv\r\n$1\r\nv\r\n$1\r\nv\r\n");
}

#[test]
fn test_string_writes_publish_keyspace_events() {
    assert_commands_publish(&[
        (&["INCR", "n"], &["incrby"]),
        (&["INCRBYFLOAT", "f", "1.5"], &["incrbyfloat"]),
        (&["APPEND", "a", "x"], &["append"]),
        (&["SETNX", "nx", "v"], &["set"]),
        (&["SETNX", "nx", "w"], &[]),
        (&["GETSET", "a", "y"], &["set"]),
        (&["GETEX", "a", "EX", "100"], &["expire"]),
        (&["GETEX", "a", "PERSIST"], &["persist"]),
        (&["SETRANGE", "r", "1", "z"], &["setrange"]),
        (&["SETBIT", "bit", "3", "1"], &["setbit"]),
        (&["PFADD", "h", "e"], &["pfadd"]),
        (&["GETDEL", "a"], &["del"]),
        (&["GETDEL", "a"], &[]),
        (&["MSETNX", "m1", "1"], &["set"]),
    ]);
}

/// MULTI and EXEC refuse writes over maxmemory as a command would be
/// (`oom_refuses`): only writes that may use memory, and only with a
/// limit set.
#[monoio::test]
async fn test_oom_refuses_memory_using_writes_over_the_limit() {
    let dir = std::env::temp_dir().join(format!("rudis-oom-refuses-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
    let router = Router::new(
        0,
        1,
        6391,
        std::rc::Rc::new(std::cell::RefCell::new(crate::shard::ShardDb::new(6391))),
        senders_mesh[0].clone(),
        None,
        std::rc::Rc::new(std::cell::RefCell::new(crate::pubsub::PubSubHub::new())),
        dir,
    );
    let set = Command::Set {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v"),
        expire_in: None,
        condition: crate::resp::SetCondition::None,
        get: false,
        keepttl: false,
        past_expired: false,
    };
    let get = Command::Get(Bytes::from_static(b"k"));
    let mut out = Vec::new();
    assert!(!oom_refuses(&router, &set, 1, &mut out).await, "no limit");
    router
        .tier_stats
        .max_memory
        .store(1, std::sync::atomic::Ordering::Relaxed);
    assert!(oom_refuses(&router, &set, 1, &mut out).await);
    assert!(!oom_refuses(&router, &get, 1, &mut out).await, "a read");
    router
        .tier_stats
        .max_memory
        .store(0, std::sync::atomic::Ordering::Relaxed);
}

#[monoio::test]
async fn test_flush_remote_batches_waits_for_owner_replies() {
    let dir = std::env::temp_dir().join(format!("rudis-flush-batches-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let (senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
    let router = Router::new(
        0,
        2,
        6398,
        std::rc::Rc::new(std::cell::RefCell::new(crate::shard::ShardDb::new(6398))),
        senders_mesh[0].clone(),
        None,
        std::rc::Rc::new(std::cell::RefCell::new(crate::pubsub::PubSubHub::new())),
        dir,
    );
    // Fake owner of shard 1: answers one Batch with an integer per item.
    let rx1 = receivers.remove(1);
    let owner = std::thread::spawn(move || {
        loop {
            if let Ok(ShardMessage::Batch {
                mut items,
                responder,
                ..
            }) = rx1.try_recv()
            {
                for (idx, _, _) in items.iter() {
                    responder.write_slot(*idx, CompactResp::from_integer(*idx as i64 + 100));
                }
                items.clear();
                responder.finish(items);
                return;
            }
            std::thread::yield_now();
        }
    });

    let mut scratch = take_conn_scratch(2);
    let mut responses = vec![CompactResp::empty(); 3];
    // Nothing pending: returns at once without touching responses.
    flush_remote_batches(
        &router,
        &scratch.responders,
        &mut scratch.remote_batches,
        &mut scratch.items_pool,
        &mut responses,
        false,
    )
    .await;
    scratch.remote_batches[1].push((0, 0, Command::Get(Bytes::from("a"))));
    scratch.remote_batches[1].push((2, 0, Command::Get(Bytes::from("b"))));
    flush_remote_batches(
        &router,
        &scratch.responders,
        &mut scratch.remote_batches,
        &mut scratch.items_pool,
        &mut responses,
        false,
    )
    .await;
    owner.join().unwrap();
    // Replies are in place when it returns, and the batch was handed off.
    let mut out = Vec::new();
    responses[0].write_to(&mut out);
    responses[2].write_to(&mut out);
    assert_eq!(String::from_utf8_lossy(&out), ":100\r\n:102\r\n");
    assert!(scratch.remote_batches.iter().all(|b| b.is_empty()));
    recycle_conn_scratch(scratch);
}

#[monoio::test(timer_enabled = true)]
async fn test_memory_usage_sizes_non_strings_on_their_owner_shard() {
    let dir = std::env::temp_dir().join(format!("rudis-memusage-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let (senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
    let router = Router::new(
        0,
        2,
        6397,
        std::rc::Rc::new(std::cell::RefCell::new(crate::shard::ShardDb::new(6397))),
        senders_mesh[0].clone(),
        None,
        std::rc::Rc::new(std::cell::RefCell::new(crate::pubsub::PubSubHub::new())),
        dir,
    );
    let key = (0..)
        .map(|i| Bytes::from(format!("h{i}")))
        .find(|k| router.target_shard(k) == 1)
        .unwrap();
    // Owner of shard 1 holds `key` as a hash and runs one Batch.
    let rx1 = receivers.remove(1);
    let owner_key = key.clone();
    let owner = std::thread::spawn(move || {
        let mut db = crate::shard::ShardDb::new(6397);
        let hset = crate::resp::build_command(vec![
            Bytes::from_static(b"HSET"),
            owner_key,
            Bytes::from_static(b"f"),
            Bytes::from_static(b"v"),
        ])
        .unwrap()
        .unwrap();
        execute_local_command(&hset, &mut db, &mut Vec::new(), None);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if let Ok(ShardMessage::Batch {
                mut items,
                responder,
                ..
            }) = rx1.try_recv()
            {
                for (idx, _, cmd) in items.iter() {
                    let mut out = Vec::new();
                    execute_local_command(cmd, &mut db, &mut out, None);
                    let n = std::str::from_utf8(&out[1..out.len() - 2])
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok());
                    let reply = match n {
                        Some(n) if out[0] == b':' => CompactResp::from_integer(n),
                        _ => CompactResp::from_integer(-1),
                    };
                    responder.write_slot(*idx, reply);
                }
                items.clear();
                responder.finish(items);
                return;
            }
            std::thread::yield_now();
        }
    });
    let reply = monoio::time::timeout(
        std::time::Duration::from_secs(5),
        memory_usage_reply(&router, key),
    )
    .await
    .expect("MEMORY USAGE of a remote hash: no reply from its owner shard");
    owner.join().unwrap();
    let reply = String::from_utf8_lossy(&reply).into_owned();
    assert!(
        reply.starts_with(':') && !reply.starts_with(":-"),
        "MEMORY USAGE of a remote hash: {reply:?}"
    );
}

#[monoio::test]
async fn test_flush_remote_batches_drops_returned_commands_on_owner() {
    // Records the thread that drops a key's buffer.
    struct DropSpy(std::sync::Arc<std::sync::Mutex<Vec<std::thread::ThreadId>>>);
    impl AsRef<[u8]> for DropSpy {
        fn as_ref(&self) -> &[u8] {
            b"spy-key"
        }
    }
    impl Drop for DropSpy {
        fn drop(&mut self) {
            self.0.lock().unwrap().push(std::thread::current().id());
        }
    }
    let dir = std::env::temp_dir().join(format!("rudis-flush-owner-drop-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let (senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
    let router = Router::new(
        0,
        2,
        6398,
        std::rc::Rc::new(std::cell::RefCell::new(crate::shard::ShardDb::new(6398))),
        senders_mesh[0].clone(),
        None,
        std::rc::Rc::new(std::cell::RefCell::new(crate::pubsub::PubSubHub::new())),
        dir,
    );
    // Fake shard 1 that, like the real one, hands the batch back with its
    // commands still in it.
    let rx1 = receivers.remove(1);
    let remote = std::thread::spawn(move || {
        loop {
            if let Ok(ShardMessage::Batch {
                items, responder, ..
            }) = rx1.try_recv()
            {
                for (idx, _, _) in items.iter() {
                    responder.write_slot(*idx, CompactResp::OK);
                }
                responder.finish(items);
                return std::thread::current().id();
            }
            std::thread::yield_now();
        }
    });

    let drops = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut scratch = take_conn_scratch(2);
    let mut responses = vec![CompactResp::empty(); 2];
    for idx in 0..2 {
        let key = Bytes::from_owner(DropSpy(drops.clone()));
        scratch.remote_batches[1].push((idx, 0, Command::Get(key)));
    }
    flush_remote_batches(
        &router,
        &scratch.responders,
        &mut scratch.remote_batches,
        &mut scratch.items_pool,
        &mut responses,
        false,
    )
    .await;
    let remote_id = remote.join().unwrap();
    assert!(responses.iter().all(|r| r.as_slice() == b"+OK\r\n"));
    // The returned batch is emptied before it is pooled, and its keys were
    // freed on this (the owner's) thread, not on the remote shard.
    assert!(scratch.items_pool.iter().all(|v| v.is_empty()));
    let drops = drops.lock().unwrap();
    assert_eq!(drops.len(), 2);
    assert!(
        drops
            .iter()
            .all(|id| *id == std::thread::current().id() && *id != remote_id)
    );
    drop(drops);
    recycle_conn_scratch(scratch);
}

#[monoio::test]
async fn test_consolidated_batch_borrowing() {
    let dir = std::env::temp_dir().join(format!("rudis-batch-borrow-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let (senders_mesh, _) = crate::mailbox::create_shard_mesh(1);
    let local_db = std::rc::Rc::new(std::cell::RefCell::new(crate::shard::ShardDb::new(6399)));
    let router = Router::new(
        0,
        1,
        6399,
        local_db.clone(),
        senders_mesh[0].clone(),
        None,
        std::rc::Rc::new(std::cell::RefCell::new(crate::pubsub::PubSubHub::new())),
        dir,
    );

    let mut scratch = take_conn_scratch(1);
    let client_registry = std::cell::RefCell::new(hashbrown::HashMap::new());
    let mut out = Vec::new();
    let mut asking = false;
    let mut authenticated = true;
    let mut auth_user = String::from("default");

    // Execute batch of mixed mutation and read commands on local shard
    scratch.commands.push(Command::Set {
        key: Bytes::from("k1"),
        value: Bytes::from("v1"),
        expire_in: None,
        condition: crate::resp::SetCondition::None,
        get: false,
        keepttl: false,
        past_expired: false,
    });
    scratch.commands.push(Command::Get(Bytes::from("k1")));
    scratch
        .commands
        .push(Command::Exists(smallvec::smallvec![Bytes::from("k1")]));
    scratch.commands.push(Command::IncrBy(
        Bytes::from("cnt"),
        5,
        crate::resp::IncrName::IncrBy,
    ));
    scratch
        .commands
        .push(Command::Del(smallvec::smallvec![Bytes::from("k1")]));
    scratch
        .commands
        .push(Command::Exists(smallvec::smallvec![Bytes::from("k1")]));

    let should_close = execute_commands_squashed(
        &mut scratch.commands,
        false,
        &router,
        &scratch.responders,
        &mut scratch.remote_batches,
        &mut scratch.items_pool,
        &mut scratch.squashed_responses,
        1,
        &client_registry,
        &mut out,
        &mut asking,
        &mut authenticated,
        &mut auth_user,
        &mut 0,
    )
    .await;

    assert!(!should_close);
    assert_eq!(
        String::from_utf8_lossy(&out),
        "+OK\r\n$2\r\nv1\r\n:1\r\n:5\r\n:1\r\n:0\r\n"
    );
    recycle_conn_scratch(scratch);
}

#[test]
fn test_client_output_buffer_limit_parsing_and_config() {
    assert_eq!(parse_limit_bytes("10mb").unwrap(), 10 * 1024 * 1024);
    assert_eq!(parse_limit_bytes("2gb").unwrap(), 2 * 1024 * 1024 * 1024);
    assert_eq!(parse_limit_bytes("500kb").unwrap(), 500 * 1024);
    assert_eq!(parse_limit_bytes("1024").unwrap(), 1024);

    // Validation errors
    assert!(set_client_output_buffer_limit_str("wrong number").is_err());
    assert!(set_client_output_buffer_limit_str("invalid_class 10mb 10mb 60").is_err());
    assert!(set_client_output_buffer_limit_str("normal 10mbs 10mb 60").is_err());
    assert!(set_client_output_buffer_limit_str("replica 10mb 10mbs 60").is_err());
    assert!(set_client_output_buffer_limit_str("pubsub 10mb 10mb 60s").is_err());

    // Valid multi-class set
    assert!(
        set_client_output_buffer_limit_str(
            "normal 1mb 2mb 60 replica 3mb 4mb 70 pubsub 5mb 6mb 80"
        )
        .is_ok()
    );
    let cfg = format_client_output_buffer_limit_config();
    assert_eq!(
        cfg,
        "normal 1048576 2097152 60 slave 3145728 4194304 70 pubsub 5242880 6291456 80"
    );

    // Reset to default
    let _ = set_client_output_buffer_limit_str(
        "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60",
    );
}

#[test]
fn test_global_client_entry_is_the_servers_own_not_a_same_id_one() {
    let entry = |replica: bool| {
        let stats = std::sync::Arc::new(ClientStats::new(
            Instant::now(),
            "127.0.0.1:1".parse().unwrap(),
        ));
        stats
            .is_replica
            .store(replica, std::sync::atomic::Ordering::Relaxed);
        GlobalClientEntry {
            raw_fd: -1,
            push: crate::transport::PushTarget::Fd(-1),
            track_tx: flume::unbounded().0,
            is_resp3: false,
            auth_user: "default".to_string(),
            is_pubsub: false,
            stats,
        }
    };
    let is_rep = |e: Option<&GlobalClientEntry>| {
        e.map(|e| {
            e.stats
                .is_replica
                .load(std::sync::atomic::Ordering::Relaxed)
        })
    };
    // Two servers in one process gave out the same id 7.
    let mut map = hashbrown::HashMap::new();
    map.insert((16425, 7), entry(false));
    map.insert((16424, 7), entry(true));
    assert_eq!(is_rep(global_client_entry(&map, 16424, 1, 7)), Some(true));
    assert_eq!(is_rep(global_client_entry(&map, 16425, 1, 7)), Some(false));
    assert!(global_client_entry(&map, 16426, 1, 7).is_none());
    // A cluster-mode node of 2 shards: its shard 1 registers at base + 1.
    map.insert((7001, 9), entry(true));
    assert_eq!(is_rep(global_client_entry(&map, 7000, 2, 9)), Some(true));
    assert!(global_client_entry(&map, 7000, 1, 9).is_none());
}

#[test]
fn test_setslot_reply_must_be_ok() {
    assert_eq!(setslot_reply_ok(b"+OK\r\n"), Ok(()));
    assert_eq!(
        setslot_reply_ok(b"-NOPERM this user has no permissions\r\n"),
        Err("NOPERM this user has no permissions".to_string())
    );
    assert_eq!(setslot_reply_ok(b""), Err("no reply".to_string()));
    assert_eq!(setslot_reply_ok(b"+OK"), Err("OK".to_string())); // cut short
    assert!(setslot_reply_ok(b"+QUEUED\r\n").is_err());
}

#[test]
fn test_squashed_pending_bitmask_polling() {
    let responder = std::sync::Arc::new(crate::mailbox::BatchResponder::new());
    let mut resp_slot = [crate::shard::CompactResp::NULL; 4];
    responder.prepare(resp_slot.as_mut_ptr(), resp_slot.len());

    let mut pending = crate::shardset::ShardSet::new();
    pending.insert(0);
    pending.insert(2);
    let responders = [responder.clone(), responder.clone(), responder.clone()];

    // Finish shard 0
    responder.finish(vec![]);
    for target_shard in pending.iter() {
        if target_shard == 0 {
            let r = &responders[target_shard];
            if let Some(_items) = r.try_take() {
                pending.remove(target_shard);
            }
        }
    }
    // Only shard 2 remains.
    assert_eq!(pending.iter().collect::<Vec<_>>(), [2]);
}

#[monoio::test]
async fn test_readonly_readwrite_object_and_config() {
    let mut db = crate::shard::ShardDb::new(6398);
    db.set(
        bytes::Bytes::from("strkey"),
        bytes::Bytes::from("hello"),
        None,
    );
    let _ = db.hset(
        bytes::Bytes::from("hashkey"),
        vec![(bytes::Bytes::from("f"), bytes::Bytes::from("v"))],
    );

    let mut out = Vec::new();

    // READONLY & READWRITE
    execute_local_command(&Command::Readonly, &mut db, &mut out, None);
    assert_eq!(&out[..], b"+OK\r\n");
    out.clear();

    execute_local_command(&Command::Readwrite, &mut db, &mut out, None);
    assert_eq!(&out[..], b"+OK\r\n");
    out.clear();

    // OBJECT ENCODING
    execute_local_command(
        &Command::Object(crate::resp::ObjectSubcommand::Encoding(bytes::Bytes::from(
            "strkey",
        ))),
        &mut db,
        &mut out,
        None,
    );
    assert_eq!(&out[..], b"$3\r\nraw\r\n");
    out.clear();

    execute_local_command(
        &Command::Object(crate::resp::ObjectSubcommand::Encoding(bytes::Bytes::from(
            "hashkey",
        ))),
        &mut db,
        &mut out,
        None,
    );
    assert!(
        std::str::from_utf8(&out).unwrap().contains("listpack")
            || std::str::from_utf8(&out).unwrap().contains("hashtable")
    );
    out.clear();

    execute_local_command(
        &Command::Object(crate::resp::ObjectSubcommand::Encoding(bytes::Bytes::from(
            "missing",
        ))),
        &mut db,
        &mut out,
        None,
    );
    assert_eq!(&out[..], b"$-1\r\n");
    out.clear();

    // OBJECT IDLETIME / REFCOUNT / FREQ
    execute_local_command(
        &Command::Object(crate::resp::ObjectSubcommand::Idletime(bytes::Bytes::from(
            "strkey",
        ))),
        &mut db,
        &mut out,
        None,
    );
    assert_eq!(&out[..], b":0\r\n");
    out.clear();

    execute_local_command(
        &Command::Object(crate::resp::ObjectSubcommand::Refcount(bytes::Bytes::from(
            "strkey",
        ))),
        &mut db,
        &mut out,
        None,
    );
    assert_eq!(&out[..], b":1\r\n");
    out.clear();

    execute_local_command(
        &Command::Object(crate::resp::ObjectSubcommand::Freq(bytes::Bytes::from(
            "strkey",
        ))),
        &mut db,
        &mut out,
        None,
    );
    // Not tracked unless an LFU maxmemory policy is selected.
    assert!(out.starts_with(b"-ERR An LFU maxmemory policy is not selected"));
    out.clear();

    // OBJECT HELP
    execute_local_command(
        &Command::Object(crate::resp::ObjectSubcommand::Help),
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains("ENCODING"));
    out.clear();

    // XINFO STREAM / GROUPS / CONSUMERS / HELP
    let _ = db.xadd(
        bytes::Bytes::from("teststream"),
        crate::table::StreamAddId::Auto,
        vec![(bytes::Bytes::from("f1"), bytes::Bytes::from("v1"))],
        false,
        None,
        None,
        false,
        crate::table::StreamTrimStrategy::KeepRef,
        None,
        None,
    );
    let _ = db.xgroup_create(
        bytes::Bytes::from("teststream"),
        bytes::Bytes::from("grp1"),
        "0",
        false,
        None,
    );
    let _ = db.xgroup_createconsumer(b"teststream", b"grp1", bytes::Bytes::from("c1"));

    execute_local_command(
        &Command::Xinfo(crate::resp::XinfoSubcommand::Stream(bytes::Bytes::from(
            "teststream",
        ))),
        &mut db,
        &mut out,
        None,
    );
    assert!(
        std::str::from_utf8(&out)
            .unwrap()
            .contains("radix-tree-keys")
    );
    out.clear();

    execute_local_command(
        &Command::Xinfo(crate::resp::XinfoSubcommand::Groups(bytes::Bytes::from(
            "teststream",
        ))),
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains("grp1"));
    out.clear();

    execute_local_command(
        &Command::Xinfo(crate::resp::XinfoSubcommand::Consumers {
            key: bytes::Bytes::from("teststream"),
            group: bytes::Bytes::from("grp1"),
        }),
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains("c1"));
    out.clear();

    execute_local_command(
        &Command::Xinfo(crate::resp::XinfoSubcommand::Help),
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains("CONSUMERS"));
    out.clear();

    // COMMAND COUNT / LIST
    execute_local_command(&Command::CommandCount, &mut db, &mut out, None);
    let count: usize = std::str::from_utf8(&out[1..out.len() - 2])
        .unwrap()
        .parse()
        .unwrap();
    assert!(count > 200, "COMMAND COUNT {count}");
    out.clear();

    execute_local_command(&Command::CommandList, &mut db, &mut out, None);
    assert!(std::str::from_utf8(&out).unwrap().contains("get"));
    out.clear();

    // LATENCY LATEST / DOCTOR / HELP / RESET
    execute_local_command(
        &Command::Latency(LatencySubcommand::Latest),
        &mut db,
        &mut out,
        None,
    );
    assert_eq!(&out[..], b"*0\r\n");
    out.clear();

    execute_local_command(
        &Command::Latency(LatencySubcommand::Doctor),
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains("Dave"));
    out.clear();

    execute_local_command(
        &Command::Latency(LatencySubcommand::Reset(vec![])),
        &mut db,
        &mut out,
        None,
    );
    assert_eq!(&out[..], b":0\r\n");
    out.clear();

    execute_local_command(
        &Command::Latency(LatencySubcommand::Help),
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains("LATEST"));
    out.clear();

    // PUBSUB HELP
    execute_local_command(&Command::PubsubHelp, &mut db, &mut out, None);
    assert!(std::str::from_utf8(&out).unwrap().contains("CHANNELS"));
    out.clear();

    // FUNCTION STATS & KILL
    execute_local_command(&Command::FunctionStats, &mut db, &mut out, None);
    assert!(
        std::str::from_utf8(&out)
            .unwrap()
            .contains("running_script")
    );
    out.clear();

    execute_local_command(&Command::FunctionKill, &mut db, &mut out, None);
    assert_eq!(&out[..], b"+OK\r\n");
    out.clear();

    // HEXPIRE / HTTL / HPERSIST
    let _ = db.hset(
        bytes::Bytes::from("hexp_hash"),
        vec![
            (bytes::Bytes::from("f1"), bytes::Bytes::from("v1")),
            (bytes::Bytes::from("f2"), bytes::Bytes::from("v2")),
        ],
    );
    execute_local_command(
        &Command::Hexpire {
            key: bytes::Bytes::from("hexp_hash"),
            expire_ms: 10000,
            is_at: false,
            condition: crate::resp::HexpireCondition::Nx,
            fields: vec![bytes::Bytes::from("f1"), bytes::Bytes::from("missing")],
        },
        &mut db,
        &mut out,
        None,
    );
    assert_eq!(&out[..], b"*2\r\n:1\r\n:-2\r\n");
    out.clear();

    execute_local_command(
        &Command::Httl {
            key: bytes::Bytes::from("hexp_hash"),
            is_ms: false,
            is_expiretime: false,
            fields: vec![bytes::Bytes::from("f1"), bytes::Bytes::from("f2")],
        },
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains(":-1\r\n"));
    out.clear();

    execute_local_command(
        &Command::Hpersist {
            key: bytes::Bytes::from("hexp_hash"),
            fields: vec![bytes::Bytes::from("f1"), bytes::Bytes::from("f2")],
        },
        &mut db,
        &mut out,
        None,
    );
    assert_eq!(&out[..], b"*2\r\n:1\r\n:-1\r\n");
    out.clear();

    // XCLAIM & XAUTOCLAIM
    let sid = match db
        .xadd(
            bytes::Bytes::from("xclaim_s"),
            crate::table::StreamAddId::Explicit(crate::table::StreamId::new(1000, 0)),
            vec![(bytes::Bytes::from("k"), bytes::Bytes::from("v"))],
            false,
            None,
            None,
            false,
            crate::table::StreamTrimStrategy::KeepRef,
            None,
            None,
        )
        .unwrap()
    {
        crate::table::StreamAddResult::Added(id) => id,
        _ => panic!("expected Added"),
    };
    let _ = db.xgroup_create(
        bytes::Bytes::from("xclaim_s"),
        bytes::Bytes::from("grp"),
        "0",
        false,
        None,
    );
    let mut te = 0;
    let mut tb = 0;
    let _ = db.xreadgroup(
        b"xclaim_s",
        b"grp",
        bytes::Bytes::from("c1"),
        ">",
        Some(1),
        false,
        None,
        usize::MAX,
        &mut te,
        &mut tb,
    );
    execute_local_command(
        &Command::Xclaim {
            key: bytes::Bytes::from("xclaim_s"),
            group: bytes::Bytes::from("grp"),
            consumer: bytes::Bytes::from("c2"),
            min_idle_time: 0,
            ids: Box::new(vec![bytes::Bytes::from(sid.to_string())]),
            idle: None,
            time: None,
            retrycount: None,
            force: false,
            justid: true,
            lastid: None,
        },
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains("1000-0"));
    out.clear();

    execute_local_command(
        &Command::Xautoclaim {
            key: bytes::Bytes::from("xclaim_s"),
            group: bytes::Bytes::from("grp"),
            consumer: bytes::Bytes::from("c3"),
            min_idle_time: 0,
            start: bytes::Bytes::from("0-0"),
            count: 10,
            justid: true,
        },
        &mut db,
        &mut out,
        None,
    );
    assert!(std::str::from_utf8(&out).unwrap().contains("1000-0"));
    out.clear();
}

#[test]
fn test_client_tracking_invalidation_and_watch_flow() {
    let port = 21001;
    let (tx, _rx) = flume::bounded::<Vec<u8>>(10);
    let (mut peer, ours) = std::os::unix::net::UnixStream::pair().unwrap();
    peer.set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    register_global_client(
        port,
        100,
        std::os::unix::io::AsRawFd::as_raw_fd(&ours),
        crate::transport::PushTarget::Fd(std::os::unix::io::AsRawFd::as_raw_fd(&ours)),
        tx,
        true,
        "default".to_string(),
        false,
        std::sync::Arc::new(ClientStats::new(Instant::now(), addr)),
    );
    register_client_tracking(port, 100, None, false, vec![], false, false, false).unwrap();
    record_client_read(port, 100, b"my_tracked_key");

    // Invalidate from another client
    notify_key_invalidation(port, b"my_tracked_key", 101);
    let mut msg = [0u8; 256];
    let n = std::io::Read::read(&mut peer, &mut msg).expect("should receive invalidation");
    assert!(
        std::str::from_utf8(&msg[..n])
            .unwrap()
            .contains("my_tracked_key")
    );

    unregister_client_tracking(port, 100);
    unregister_global_client(port, 100);

    // Watch keys test
    let watch_port = 21002;
    watch_keys(watch_port, 200, &[Bytes::from("watched_k1")]);
    assert!(!is_watch_tainted(watch_port, 200));

    touch_watched_key(watch_port, b"watched_k1");
    assert!(is_watch_tainted(watch_port, 200));

    unwatch_keys(watch_port, 200);
    assert!(!is_watch_tainted(watch_port, 200));
}

#[test]
fn test_help_lines_are_simple_strings() {
    let mut out = Vec::new();
    write_resp_status(&mut out, "HELP -- Print this help.");
    assert_eq!(out, b"+HELP -- Print this help.\r\n");
}

#[test]
fn test_resp_formatting_and_limit_bytes() {
    // format_score
    assert_eq!(format_score(1.5), "1.5");
    assert_eq!(format_score(f64::NAN), "nan");
    assert_eq!(format_score(f64::INFINITY), "inf");
    assert_eq!(format_score(f64::NEG_INFINITY), "-inf");

    // parse_limit_bytes
    assert_eq!(parse_limit_bytes("1gb"), Some(1024 * 1024 * 1024));
    assert_eq!(parse_limit_bytes("2mb"), Some(2 * 1024 * 1024));
    assert_eq!(parse_limit_bytes("4kb"), Some(4 * 1024));
    assert_eq!(parse_limit_bytes("500b"), Some(500));
    assert_eq!(parse_limit_bytes("100"), Some(100));
    assert_eq!(parse_limit_bytes("invalid"), None);

    // max clients and memory policy
    set_max_clients(5000);
    assert_eq!(get_max_clients(), 5000);

    {
        let _policy_guard = MAX_MEMORY_POLICY_TEST_LOCK.lock();
        set_max_memory_policy("allkeys-lru");
        assert_eq!(get_max_memory_policy(), "allkeys-lru");
        set_max_memory_policy("volatile-random");
        assert_eq!(get_max_memory_policy(), "volatile-random");
        set_max_memory_policy("noeviction");
    }

    // write_resp primitives
    let mut out = Vec::new();
    write_resp_integer(&mut out, -42);
    assert_eq!(out, b":-42\r\n");

    out.clear();
    write_resp_bulk(&mut out, b"hello_world");
    assert_eq!(out, b"$11\r\nhello_world\r\n");

    out.clear();
    write_resp_err(&mut out, "custom error");
    assert_eq!(out, b"-ERR custom error\r\n");

    out.clear();
    format_zmpop_response(&mut out, &Bytes::from("zk"), &[(Bytes::from("m1"), 10.0)]);
    assert!(out.starts_with(b"*2\r\n"));

    out.clear();
    format_bzpop_response(&mut out, &Bytes::from("zk"), &Bytes::from("m1"), 10.0);
    assert!(out.starts_with(b"*3\r\n"));
}

/// Client-chosen sizes that pass the parser must still get a bounded reply
/// when executed: KNN k, VSIM COUNT/EF, DIM, M, INITIAL_CAP, reserve sizes,
/// JSON array indexes, counts and ranges used to panic, loop for 2^63
/// iterations or abort on a huge allocation. Pins each fixed input, then
/// runs every family's templates with each hostile number in each slot.
#[test]
fn test_hostile_sizes_execute_with_bounded_replies() {
    // Thousands of commands, some slow, would land in the global slowlog
    // while the slowlog tests assert on its exact contents.
    let _slowlog = crate::slowlog::tests::TEST_LOCK.lock();
    // execute_command's future is too deep for the default test stack in debug builds.
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(|| {
            monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .enable_all()
                .build()
                .unwrap()
                .block_on(hostile_sizes())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn hostile_sizes() {
    const PORT: u16 = 6393;
    let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
    let router = std::rc::Rc::new(Router::new(
        0,
        1,
        PORT,
        std::rc::Rc::new(std::cell::RefCell::new(crate::shard::ShardDb::new(PORT))),
        senders_mesh[0].clone(),
        None,
        std::rc::Rc::new(std::cell::RefCell::new(crate::pubsub::PubSubHub::new())),
        std::env::temp_dir(),
    ));
    set_current_router(router.clone());
    let registry = std::cell::RefCell::new(hashbrown::HashMap::new());
    let blob = |a: f32, b: f32| [a.to_le_bytes(), b.to_le_bytes()].concat();
    let run = async |args: &[&str]| {
        let args: Vec<Bytes> = args
            .iter()
            .map(|a| match *a {
                "@V1" => Bytes::from(blob(1.0, 2.0)),
                "@V2" => Bytes::from(blob(3.0, 4.0)),
                "@B" => Bytes::from(blob(1.0, 1.0)),
                a => Bytes::from(a.to_string()),
            })
            .collect();
        let cmd = match crate::resp::build_command(args) {
            Ok(Some(cmd)) => cmd,
            Ok(None) => return String::new(),
            Err(e) => return format!("-ERR {e}"),
        };
        let mut out = Vec::new();
        let (mut asking, mut authenticated) = (false, true);
        let mut auth_user = String::from("default");
        execute_command(
            cmd,
            &router,
            1,
            &registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        String::from_utf8_lossy(&out).into_owned()
    };
    #[rustfmt::skip]
        let setup: &[&[&str]] = &[
            &["FT.CREATE", "hz_idx", "ON", "HASH", "PREFIX", "1", "hzdoc:", "SCHEMA", "t", "TEXT",
              "n", "NUMERIC", "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
              "DISTANCE_METRIC", "L2"],
            &["FT.CREATE", "hz_hidx", "ON", "HASH", "PREFIX", "1", "hzhd:", "SCHEMA", "t", "TEXT",
              "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "COSINE"],
            &["HSET", "hzdoc:1", "t", "hello", "n", "1", "v", "@V1"],
            &["HSET", "hzdoc:2", "t", "hello", "n", "2", "v", "@V2"],
            &["HSET", "hzhd:1", "t", "hello", "v", "@V1"], &["HSET", "hzhd:2", "t", "bye", "v", "@V2"],
            &["VADD", "vs", "VALUES", "2", "1", "2", "a"], &["VADD", "vs", "VALUES", "2", "3", "4", "b"],
            &["SADD", "s", "a", "b"], &["HSET", "h", "f", "v"], &["ZADD", "z", "1", "a"],
            &["JSON.SET", "j", "$", r#"{"a":[1,2,3]}"#],
        ];
    for args in setup {
        let reply = run(args).await;
        assert!(!reply.starts_with('-'), "{args:?}: {reply}");
    }

    let huge = "9223372036854775807";
    #[rustfmt::skip]
        let errors: &[(&[&str], &str)] = &[
            (&["FT.CREATE", "hz_x", "SCHEMA", "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
               "DIM", huge, "DISTANCE_METRIC", "L2"], "argument DIM"),
            (&["FT.CREATE", "hz_x", "SCHEMA", "v", "VECTOR", "HNSW", "8", "TYPE", "FLOAT32",
               "DIM", "2", "DISTANCE_METRIC", "L2", "M", huge], "argument M"),
            (&["FT.ALTER", "hz_idx", "SCHEMA", "ADD", "w", "VECTOR", "FLAT", "6", "TYPE",
               "FLOAT32", "DIM", huge, "DISTANCE_METRIC", "L2"], "argument DIM"),
            (&["VADD", "v2", "REDUCE", "4294967296", "VALUES", "2", "1", "1", "e"],
             "invalid vector specification"),
            (&["VADD", "v2", "VALUES", "2", "1", "1", "e", "M", huge], "invalid M"),
            (&["VSIM", "vs", "ELE", "a", "EF", huge], "invalid EF"),
            (&["VRANDMEMBER", "vs", "-4294967296"], "out of range"),
            (&["SRANDMEMBER", "s", "-9223372036854775807"], "out of range"),
            (&["BF.RESERVE", "b2", "0.01", huge], "Bloom filter too large"),
            (&["CF.RESERVE", "c2", huge], "Cuckoo filter too large"),
            (&["CMS.INITBYDIM", "m2", huge, huge], "CMS: width x depth too large"),
            (&["CRDT.INCRBY", "c", huge], ""),
            (&["CRDT.INCRBY", "c", "1"], "increment or decrement would overflow"),
            (&["JSON.SET", "j", &format!("$.a[{huge}]"), "1"], "index out of bounds"),
            (&["JSON.SET", "j", "$.a[-9]", "1"], "index out of bounds"),
        ];
    for (args, want) in errors {
        let reply = run(args).await;
        if want.is_empty() {
            assert!(!reply.starts_with('-'), "{args:?}: {reply}");
        } else {
            assert!(
                reply.starts_with("-ERR") && reply.contains(want),
                "{args:?}: {reply}"
            );
        }
    }
    // Huge k/COUNT/LIMIT/COUNT are clamped to what exists.
    #[rustfmt::skip]
        let bounded: &[(&[&str], &str)] = &[
            (&["FT.SEARCH", "hz_idx", &format!("*=>[KNN {huge} @v $B]"), "PARAMS", "2", "B", "@B"],
             "*5\r\n:2\r\n"),
            (&["FT.SEARCH", "hz_hidx", &format!("*=>[KNN {huge} @v $B EF_RUNTIME {huge}]"),
               "PARAMS", "2", "B", "@B"], "*5\r\n:2\r\n"),
            (&["FT.SEARCH", "hz_idx", "@n:[nan 0]"], "*1\r\n:0\r\n"),
            (&["VSIM", "vs", "ELE", "a", "COUNT", huge], "*2\r\n"),
            (&["VSIM", "vs", "ELE", "a", "COUNT", huge, "FILTER", ".x > 1"], "*0\r\n"),
            (&["HSCAN", "h", "0", "COUNT", "18446744073709551615"], "*2\r\n$1\r\n0\r\n*2\r\n"),
        ];
    for (args, want) in bounded {
        let reply = run(args).await;
        assert!(reply.starts_with(want), "{args:?}: {reply}");
    }

    // Every hostile number in each `#` slot (others "2"), then in all slots.
    #[rustfmt::skip]
        const HOSTILE: &[&str] = &[
            "9223372036854775807", "18446744073709551615", "4294967296", "4294967295", "2147483648",
            "-1", "-9223372036854775808", "-9223372036854775807", "-4294967296", "1e308", "inf",
            "nan", "-inf", "1000000000", "16777217", "0",
        ];
    #[rustfmt::skip]
        let templates: &[&[&str]] = &[
            &["FT.SEARCH", "hz_idx", "*=>[KNN # @v $B]", "PARAMS", "2", "B", "@B", "LIMIT", "#", "#"],
            &["FT.SEARCH", "hz_idx", "*=>[KNN $K @v $B]", "PARAMS", "4", "K", "#", "B", "@B"],
            &["FT.SEARCH", "hz_hidx", "*=>[KNN # @v $B EF_RUNTIME #]", "PARAMS", "2", "B", "@B"],
            &["FT.SEARCH", "hz_hidx", "@t:hello=>[KNN # @v $B]", "PARAMS", "2", "B", "@B"],
            &["FT.SEARCH", "hz_idx", "@n:[# #]", "LIMIT", "#", "#"],
            &["FT.SEARCH", "hz_idx", "hello|*=>[KNN # @v $B]", "PARAMS", "2", "B", "@B"],
            &["FT.AGGREGATE", "hz_idx", "*", "LOAD", "1", "@n", "SORTBY", "2", "@n", "DESC", "MAX", "#",
              "LIMIT", "#", "#"],
            &["FT.CREATE", "hz_c1", "SCHEMA", "v", "VECTOR", "FLAT", "8", "TYPE", "FLOAT32", "DIM", "#",
              "DISTANCE_METRIC", "L2", "INITIAL_CAP", "#"],
            &["FT.CREATE", "hz_c2", "SCHEMA", "v", "VECTOR", "HNSW", "10", "TYPE", "FLOAT32", "DIM", "2",
              "DISTANCE_METRIC", "L2", "M", "#", "EF_CONSTRUCTION", "#"],
            &["FT.ALTER", "hz_idx", "SCHEMA", "ADD", "w", "VECTOR", "HNSW", "8", "TYPE", "FLOAT32",
              "DIM", "#", "INITIAL_CAP", "#", "M", "#"],
            &["VSIM", "vs", "VALUES", "2", "1", "1", "COUNT", "#", "EF", "#"],
            &["VSIM", "vs", "ELE", "a", "FILTER", ".x > 1", "FILTER-EF", "#", "COUNT", "#"],
            &["VADD", "v3", "REDUCE", "#", "VALUES", "2", "1", "1", "e"],
            &["VADD", "v4", "VALUES", "2", "1", "1", "e", "M", "#", "EF", "#"],
            &["VRANDMEMBER", "vs", "#"], &["SRANDMEMBER", "s", "#"], &["HRANDFIELD", "h", "#"],
            &["ZRANDMEMBER", "z", "#", "WITHSCORES"],
            &["BF.RESERVE", "b3", "#", "#"], &["CF.RESERVE", "c3", "#"],
            &["CMS.INITBYDIM", "m3", "#", "#"], &["CMS.INITBYPROB", "m4", "#", "#"],
            &["TOPK.RESERVE", "t3", "#"], &["CRDT.INCRBY", "c2", "#"],
            &["JSON.SET", "j", "$.a[#]", "1"], &["JSON.SET", "j", "$.x[#][#]", "1"],
            &["JSON.ARRTRIM", "j", "$.a", "#", "#"], &["JSON.ARRINSERT", "j", "$.a", "#", "1"],
            &["HSCAN", "h", "#", "COUNT", "#"], &["SSCAN", "s", "0", "COUNT", "#"],
            &["LPOS", "l", "a", "RANK", "#", "COUNT", "#", "MAXLEN", "#"],
            &["ZRANGE", "z", "0", "-1", "BYSCORE", "LIMIT", "#", "#"],
            &["XADD", "st", "MAXLEN", "~", "#", "LIMIT", "#", "*", "f", "v"],
            &["XRANGE", "st", "-", "+", "COUNT", "#"], &["SPOP", "s", "#"],
        ];
    for t in templates {
        let slots = t.iter().map(|a| a.matches('#').count()).sum::<usize>();
        for h in HOSTILE {
            for target in (0..slots).chain([usize::MAX]) {
                let mut slot = 0;
                let args: Vec<String> = t
                    .iter()
                    .map(|a| {
                        let mut out = String::new();
                        for ch in a.chars() {
                            if ch == '#' {
                                out += if target == usize::MAX || target == slot {
                                    h
                                } else {
                                    "2"
                                };
                                slot += 1;
                            } else {
                                out.push(ch);
                            }
                        }
                        out
                    })
                    .collect();
                let refs: Vec<&str> = args.iter().map(String::as_str).collect();
                let reply = run(&refs).await;
                assert!(
                    reply.len() < 1 << 20,
                    "{refs:?}: reply of {} bytes",
                    reply.len()
                );
            }
        }
    }
    for name in ["hz_idx", "hz_hidx", "hz_c1", "hz_c2"] {
        let _ = crate::search::drop_search_index(name);
    }
}

#[test]
fn test_remaining_wait_ms_rounds_up_until_deadline() {
    let t0 = Instant::now();
    let dl = t0 + std::time::Duration::from_millis(100);
    // After a 50.05ms first poll step, 49.95ms remains: rounding down to
    // 49ms and then 0.90ms down to 0ms used to time out at 99.10ms.
    assert_eq!(
        remaining_wait_ms(dl, t0 + std::time::Duration::from_micros(50_050)),
        50
    );
    assert_eq!(
        remaining_wait_ms(dl, t0 + std::time::Duration::from_micros(99_100)),
        1
    );
    assert_eq!(remaining_wait_ms(dl, dl), 0);
    assert_eq!(
        remaining_wait_ms(dl, dl + std::time::Duration::from_nanos(1)),
        0
    );
}

#[test]
fn test_squashed_pipeline_folds_multikey_del_and_exists() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
        let db = Rc::new(RefCell::new(ShardDb::new(9995)));
        let router = Router::new(
            0,
            1,
            9995,
            db,
            senders_mesh[0].clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let client_registry = RefCell::new(hashbrown::HashMap::new());
        let mut asking = false;
        let mut authenticated = true;
        let mut auth_user = String::from("default");
        let mut out = Vec::new();
        let responders = vec![std::sync::Arc::new(crate::mailbox::BatchResponder::new())];
        let mut remote_batches = vec![Vec::new()];
        let mut items_pool = vec![Vec::new()];
        let mut squashed_responses = Vec::new();

        let mut cmds = vec![
            Command::Set {
                key: Bytes::from("mk_a"),
                value: Bytes::from("va"),
                expire_in: None,
                condition: crate::resp::SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            },
            Command::Set {
                key: Bytes::from("mk_b"),
                value: Bytes::from("vb"),
                expire_in: None,
                condition: crate::resp::SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            },
            Command::Exists(smallvec::smallvec![
                Bytes::from("mk_a"),
                Bytes::from("mk_a"),
                Bytes::from("mk_b"),
                Bytes::from("mk_missing"),
            ]),
            Command::Del(smallvec::smallvec![
                Bytes::from("mk_a"),
                Bytes::from("mk_a"),
                Bytes::from("mk_b"),
                Bytes::from("mk_missing"),
            ]),
            Command::Exists(smallvec::smallvec![
                Bytes::from("mk_a"),
                Bytes::from("mk_b"),
            ]),
        ];

        let mut fallbacks = 0u64;
        let quit = execute_commands_squashed(
            &mut cmds,
            false,
            &router,
            &responders,
            &mut remote_batches,
            &mut items_pool,
            &mut squashed_responses,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
            &mut fallbacks,
        )
        .await;
        assert!(!quit);
        assert_eq!(out, b"+OK\r\n+OK\r\n:3\r\n:2\r\n:0\r\n");
        // Commands handed to `execute_command` are counted there; whether
        // any are depends on process-wide state other tests change, so
        // only the bound is checked here (the e2e test checks the count).
        assert!(fallbacks <= 5, "{fallbacks}");
    });
}

#[test]
fn test_squashed_pipeline_folds_mset() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
        let db = Rc::new(RefCell::new(ShardDb::new(9996)));
        let router = Router::new(
            0,
            1,
            9996,
            db,
            senders_mesh[0].clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let client_registry = RefCell::new(hashbrown::HashMap::new());
        let mut asking = false;
        let mut authenticated = true;
        let mut auth_user = String::from("default");
        let mut out = Vec::new();
        let responders = vec![std::sync::Arc::new(crate::mailbox::BatchResponder::new())];
        let mut remote_batches = vec![Vec::new()];
        let mut items_pool = vec![Vec::new()];
        let mut squashed_responses = Vec::new();

        let mut cmds = vec![
            Command::Mset(vec![
                (Bytes::from("ms_a"), Bytes::from("v1")),
                (Bytes::from("ms_b"), Bytes::from("v2")),
                (Bytes::from("ms_a"), Bytes::from("v1_last")),
            ]),
            Command::Mset(vec![
                (Bytes::from("ms_b"), Bytes::from("v2_next")),
                (Bytes::from("ms_c"), Bytes::from("v3")),
            ]),
            Command::Mget(vec![
                Bytes::from("ms_a"),
                Bytes::from("ms_b"),
                Bytes::from("ms_c"),
            ]),
        ];

        let quit = execute_commands_squashed(
            &mut cmds,
            false,
            &router,
            &responders,
            &mut remote_batches,
            &mut items_pool,
            &mut squashed_responses,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
            &mut 0,
        )
        .await;
        assert!(!quit);
        assert_eq!(
            out,
            b"+OK\r\n+OK\r\n*3\r\n$7\r\nv1_last\r\n$7\r\nv2_next\r\n$2\r\nv3\r\n"
        );
    });
}

#[test]
fn test_squashed_pipeline_unlink_touch_and_container_multi_reads() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
            let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
            let db = Rc::new(RefCell::new(ShardDb::new(9995)));
            let router = Router::new(
                0,
                1,
                9995,
                db,
                senders_mesh[0].clone(),
                None,
                Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
                std::env::temp_dir(),
            );
            let client_registry = RefCell::new(hashbrown::HashMap::new());
            let mut asking = false;
            let mut authenticated = true;
            let mut auth_user = String::from("default");
            let mut out = Vec::new();
            let responders = vec![std::sync::Arc::new(crate::mailbox::BatchResponder::new())];
            let mut remote_batches = vec![Vec::new()];
            let mut items_pool = vec![Vec::new()];
            let mut squashed_responses = Vec::new();

            let mut cmds = vec![
                Command::Hset {
                    key: Bytes::from("h1"),
                    fields: smallvec::smallvec![
                        (Bytes::from("f1"), Bytes::from("v1")),
                        (Bytes::from("f2"), Bytes::from("v2")),
                    ],
                },
                Command::Hmget {
                    key: Bytes::from("h1"),
                    fields: vec![Bytes::from("f1"), Bytes::from("missing"), Bytes::from("f2")],
                },
                Command::Sadd {
                    key: Bytes::from("s1"),
                    members: smallvec::smallvec![Bytes::from("m1"), Bytes::from("m2")],
                },
                Command::Smismember {
                    key: Bytes::from("s1"),
                    members: vec![Bytes::from("m1"), Bytes::from("m_none"), Bytes::from("m2")],
                },
                Command::Zadd {
                    key: Bytes::from("z1"),
                    elements: smallvec::smallvec![
                        (1.5, Bytes::from("zm1")),
                        (2.5, Bytes::from("zm2")),
                    ],
                    flags: crate::table::ZAddFlags::default(),
                },
                Command::Zmscore {
                    key: Bytes::from("z1"),
                    members: vec![
                        Bytes::from("zm1"),
                        Bytes::from("zm_none"),
                        Bytes::from("zm2"),
                    ],
                },
                Command::Touch(smallvec::smallvec![
                    Bytes::from("h1"),
                    Bytes::from("s1"),
                    Bytes::from("z1"),
                    Bytes::from("no_such_key"),
                ]),
                Command::Unlink(smallvec::smallvec![
                    Bytes::from("h1"),
                    Bytes::from("s1"),
                    Bytes::from("no_such_key"),
                ]),
                Command::Touch(smallvec::smallvec![
                    Bytes::from("h1"),
                    Bytes::from("s1"),
                    Bytes::from("z1"),
                ]),
            ];

            let quit = execute_commands_squashed(
                &mut cmds,
                false,
                &router,
                &responders,
                &mut remote_batches,
                &mut items_pool,
                &mut squashed_responses,
                1,
                &client_registry,
                &mut out,
                &mut asking,
                &mut authenticated,
                &mut auth_user,
                &mut 0,
            )
            .await;
            assert!(!quit);
            let expected = concat!(
                ":2\r\n",
                "*3\r\n$2\r\nv1\r\n$-1\r\n$2\r\nv2\r\n",
                ":2\r\n",
                "*3\r\n:1\r\n:0\r\n:1\r\n",
                ":2\r\n",
                "*3\r\n$3\r\n1.5\r\n$-1\r\n$3\r\n2.5\r\n",
                ":3\r\n",
                ":2\r\n",
                ":1\r\n",
            );
            assert_eq!(out, expected.as_bytes());
        });
}

#[test]
fn test_squashed_pipeline_hdel_srem_zrem_fast_paths() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
        let db = Rc::new(RefCell::new(ShardDb::new(9993)));
        let router = Router::new(
            0,
            1,
            9993,
            db,
            senders_mesh[0].clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let client_registry = RefCell::new(hashbrown::HashMap::new());
        let mut asking = false;
        let mut authenticated = true;
        let mut auth_user = String::from("default");
        let mut out = Vec::new();
        let responders = vec![std::sync::Arc::new(crate::mailbox::BatchResponder::new())];
        let mut remote_batches = vec![Vec::new()];
        let mut items_pool = vec![Vec::new()];
        let mut squashed_responses = Vec::new();
        let b = |s: &'static str| Bytes::from_static(s.as_bytes());

        let mut cmds = vec![
            Command::Hset {
                key: b("h"),
                fields: smallvec::smallvec![(b("a"), b("1")), (b("b"), b("2")), (b("c"), b("3"))],
            },
            Command::Hdel {
                key: b("h"),
                fields: smallvec::smallvec![b("a")],
            },
            Command::Hdel {
                key: b("h"),
                fields: smallvec::smallvec![b("b"), b("c"), b("x")],
            },
            Command::Hdel {
                key: b("h"),
                fields: smallvec::smallvec![b("a")],
            },
            Command::Sadd {
                key: b("s"),
                members: smallvec::smallvec![b("a"), b("b")],
            },
            Command::Srem {
                key: b("s"),
                members: smallvec::smallvec![b("a"), b("x")],
            },
            Command::Srem {
                key: b("s"),
                members: smallvec::smallvec![b("b")],
            },
            Command::Zadd {
                key: b("z"),
                elements: smallvec::smallvec![(1.0, b("a")), (2.0, b("b"))],
                flags: crate::table::ZAddFlags::default(),
            },
            Command::Zrem {
                key: b("z"),
                members: smallvec::smallvec![b("a"), b("b"), b("x")],
            },
            // Removing the last item deletes the key.
            Command::Exists(smallvec::smallvec![b("h"), b("s"), b("z")]),
            Command::Set {
                key: b("str"),
                value: b("v"),
                expire_in: None,
                condition: crate::resp::SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            },
            Command::Hdel {
                key: b("str"),
                fields: smallvec::smallvec![b("a")],
            },
            Command::Srem {
                key: b("str"),
                members: smallvec::smallvec![b("a")],
            },
            Command::Zrem {
                key: b("str"),
                members: smallvec::smallvec![b("a")],
            },
        ];

        let quit = execute_commands_squashed(
            &mut cmds,
            false,
            &router,
            &responders,
            &mut remote_batches,
            &mut items_pool,
            &mut squashed_responses,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
            &mut 0,
        )
        .await;
        assert!(!quit);
        let wt = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        let expected = format!(
            ":3\r\n:1\r\n:2\r\n:0\r\n:2\r\n:1\r\n:1\r\n:2\r\n:2\r\n:0\r\n+OK\r\n{wt}{wt}{wt}"
        );
        assert_eq!(String::from_utf8_lossy(&out), expected);
    });
}

#[test]
fn test_squashed_pipeline_rpush_spop_zpopmin_fast_paths() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
        let db = Rc::new(RefCell::new(ShardDb::new(9994)));
        let router = Router::new(
            0,
            1,
            9994,
            db,
            senders_mesh[0].clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let client_registry = RefCell::new(hashbrown::HashMap::new());
        let mut asking = false;
        let mut authenticated = true;
        let mut auth_user = String::from("default");
        let mut out = Vec::new();
        let responders = vec![std::sync::Arc::new(crate::mailbox::BatchResponder::new())];
        let mut remote_batches = vec![Vec::new()];
        let mut items_pool = vec![Vec::new()];
        let mut squashed_responses = Vec::new();

        let mut cmds = vec![
            Command::Rpush {
                key: Bytes::from("l"),
                values: smallvec::smallvec![Bytes::from("a"), Bytes::from("b")],
            },
            Command::Rpush {
                key: Bytes::from("l"),
                values: smallvec::smallvec![Bytes::from("c")],
            },
            Command::Lrange {
                key: Bytes::from("l"),
                start: 0,
                stop: -1,
            },
            Command::Sadd {
                key: Bytes::from("s"),
                members: smallvec::smallvec![Bytes::from("m1")],
            },
            Command::Spop {
                key: Bytes::from("s"),
                count: None,
            },
            Command::Spop {
                key: Bytes::from("s"),
                count: None,
            },
            Command::Zadd {
                key: Bytes::from("z"),
                elements: smallvec::smallvec![(2.5, Bytes::from("zb")), (1.5, Bytes::from("za")),],
                flags: crate::table::ZAddFlags::default(),
            },
            Command::Zpopmin {
                key: Bytes::from("z"),
                count: None,
            },
            Command::Zpopmin {
                key: Bytes::from("z"),
                count: None,
            },
            Command::Zpopmin {
                key: Bytes::from("z"),
                count: None,
            },
            // Popping the last member deletes the key.
            Command::Exists(smallvec::smallvec![Bytes::from("s"), Bytes::from("z")]),
            Command::Spop {
                key: Bytes::from("l"),
                count: None,
            },
            Command::Zpopmin {
                key: Bytes::from("l"),
                count: None,
            },
        ];

        let quit = execute_commands_squashed(
            &mut cmds,
            false,
            &router,
            &responders,
            &mut remote_batches,
            &mut items_pool,
            &mut squashed_responses,
            1,
            &client_registry,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
            &mut 0,
        )
        .await;
        assert!(!quit);
        let expected = concat!(
            ":2\r\n",
            ":3\r\n",
            "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
            ":1\r\n",
            "$2\r\nm1\r\n",
            "$-1\r\n",
            ":2\r\n",
            "*2\r\n$2\r\nza\r\n$3\r\n1.5\r\n",
            "*2\r\n$2\r\nzb\r\n$3\r\n2.5\r\n",
            "*0\r\n",
            ":0\r\n",
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        );
        assert_eq!(String::from_utf8_lossy(&out), expected);
    });
}
