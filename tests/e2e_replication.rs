//! End-to-end tests: replication (see `tests/common` for the helpers).

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
fn test_primary_replica_replication_e2e() {
    let master_port = 16420;
    let replica_port = 16421;

    start_test_server(master_port, 2);
    start_test_server(replica_port, 2);

    let mut master_client = TcpStream::connect(format!("127.0.0.1:{}", master_port)).unwrap();
    let mut replica_client = TcpStream::connect(format!("127.0.0.1:{}", replica_port)).unwrap();

    // 1. Verify master role
    let master_role = send_and_read(&mut master_client, b"ROLE\r\n");
    assert!(master_role.starts_with("*3\r\n$6\r\nmaster\r\n"));

    // 2. Pre-populate data on master before replica connects
    assert_eq!(
        send_and_read(&mut master_client, b"SET init_k1 val1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut master_client, b"SET init_k2 val2\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut master_client, b"HSET myhash field1 hello\r\n"),
        ":1\r\n"
    );

    // 3. Initiate replication on replica (dispatches PSYNC to master)
    let rep_resp = send_and_read(
        &mut replica_client,
        format!("REPLICAOF 127.0.0.1 {}\r\n", master_port).as_bytes(),
    );
    assert_eq!(rep_resp, "+OK\r\n");

    // Wait for handshake, RDB snapshot generation, transfer, and restore
    let mut replica_role = String::new();
    for _ in 0..40 {
        replica_role = send_and_read(&mut replica_client, b"ROLE\r\n");
        if replica_role.contains("slave") && replica_role.contains("connected") {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    // 4. Verify replica role and link status
    assert!(
        replica_role.contains("slave"),
        "Expected slave role, got {}",
        replica_role
    );
    assert!(
        replica_role.contains("connected"),
        "Expected connected state, got {}",
        replica_role
    );

    // 5. Verify pre-existing data was restored from RDB on replica
    assert_eq!(
        send_and_read(&mut replica_client, b"GET init_k1\r\n"),
        "$4\r\nval1\r\n"
    );
    assert_eq!(
        send_and_read(&mut replica_client, b"GET init_k2\r\n"),
        "$4\r\nval2\r\n"
    );
    assert_eq!(
        send_and_read(&mut replica_client, b"HGET myhash field1\r\n"),
        "$5\r\nhello\r\n"
    );

    // 6. Test read-only replica enforcement
    let write_resp = send_and_read(&mut replica_client, b"SET forbidden_key write_val\r\n");
    assert!(
        write_resp.contains("READONLY"),
        "Expected READONLY error, got: {}",
        write_resp
    );

    // 7. Live streaming mutation replication
    assert_eq!(
        send_and_read(&mut master_client, b"SET live_key live_val\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut master_client, b"INCRBY live_counter 42\r\n"),
        ":42\r\n"
    );
    assert_eq!(
        send_and_read(&mut master_client, b"RPUSH mylist itemA itemB\r\n"),
        ":2\r\n"
    );

    // Wait for replication stream propagation. Each write is propagated on
    // its own (from whichever shard owns the key, with no order across
    // shards), so seeing the first one says nothing about the others: wait
    // for each.
    for (cmd, want) in [
        (&b"GET live_key\r\n"[..], "$8\r\nlive_val\r\n"),
        (b"GET live_counter\r\n", "$2\r\n42\r\n"),
        (
            b"LRANGE mylist 0 -1\r\n",
            "*2\r\n$5\r\nitemA\r\n$5\r\nitemB\r\n",
        ),
    ] {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut got = send_and_read(&mut replica_client, cmd);
        while got != want && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
            got = send_and_read(&mut replica_client, cmd);
        }
        // Verify replicated on replica
        assert_eq!(got, want, "{}", String::from_utf8_lossy(cmd).trim());
    }

    // 8. Promotion via REPLICAOF NO ONE
    assert_eq!(
        send_and_read(&mut replica_client, b"REPLICAOF NO ONE\r\n"),
        "+OK\r\n"
    );
    let promoted_role = send_and_read(&mut replica_client, b"ROLE\r\n");
    assert!(
        promoted_role.starts_with("*3\r\n$6\r\nmaster\r\n"),
        "Expected master after promotion, got {}",
        promoted_role
    );

    // Writes should now succeed on promoted node
    assert_eq!(
        send_and_read(&mut replica_client, b"SET promoted_key promoted_value\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut replica_client, b"GET promoted_key\r\n"),
        "$14\r\npromoted_value\r\n"
    );
}

#[test]
fn test_replica_partial_resync_reconnect_e2e() {
    let master_port = 16424;
    let replica_port = 16425;

    start_test_server(master_port, 2);
    start_test_server(replica_port, 2);

    let mut master_client = TcpStream::connect(format!("127.0.0.1:{}", master_port)).unwrap();
    let mut replica_client = TcpStream::connect(format!("127.0.0.1:{}", replica_port)).unwrap();

    // 1. Pre-populate master with initial keys
    assert_eq!(
        send_and_read(&mut master_client, b"SET k1 v1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut master_client, b"SET k2 v2\r\n"),
        "+OK\r\n"
    );

    // 2. Start replication: initial full resync
    assert_eq!(
        send_and_read(
            &mut replica_client,
            format!("REPLICAOF 127.0.0.1 {}\r\n", master_port).as_bytes()
        ),
        "+OK\r\n"
    );

    // Wait for replica to be connected
    let mut replica_role = String::new();
    for _ in 0..40 {
        replica_role = send_and_read(&mut replica_client, b"ROLE\r\n");
        if replica_role.contains("slave") && replica_role.contains("connected") {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(replica_role.contains("slave") && replica_role.contains("connected"));

    // Verify initial keys present on replica
    assert_eq!(
        send_and_read(&mut replica_client, b"GET k1\r\n"),
        "$2\r\nv1\r\n"
    );
    assert_eq!(
        send_and_read(&mut replica_client, b"GET k2\r\n"),
        "$2\r\nv2\r\n"
    );

    // 3. Write live mutations to master
    assert_eq!(
        send_and_read(&mut master_client, b"SET k3 v3\r\n"),
        "+OK\r\n"
    );
    for _ in 0..40 {
        if send_and_read(&mut replica_client, b"GET k3\r\n") == "$2\r\nv3\r\n" {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        send_and_read(&mut replica_client, b"GET k3\r\n"),
        "$2\r\nv3\r\n"
    );

    // Check INFO replication on replica to verify replid and offset
    let rep_info = send_and_read(&mut replica_client, b"INFO replication\r\n");
    assert!(rep_info.contains("role:slave"));
    assert!(rep_info.contains("master_link_status:up"));

    // 4. Drop the replication link from the master side. The replica
    // reconnects on its own with its cached master_replid and offset and
    // resumes from the backlog (PSYNC <replid> <offset>). Re-issuing
    // REPLICAOF to the same master is a no-op ("Already connected"), as in
    // Redis, so it can't be used to force a reconnect.
    assert_eq!(
        send_and_read(&mut master_client, b"SET k4 v4\r\n"),
        "+OK\r\n"
    );
    // In-process servers share the process-wide stats with other tests, so
    // compare counters before and after rather than absolute values.
    let stat = |c: &mut TcpStream, name: &str| -> u64 {
        let info = send_and_read(c, b"INFO stats\r\n");
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}:")))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| panic!("no {name} in {info}"))
    };
    let partial_before = stat(&mut master_client, "sync_partial_ok");
    let full_before = stat(&mut master_client, "sync_full");
    assert_eq!(
        send_and_read(&mut master_client, b"CLIENT KILL TYPE replica\r\n"),
        ":1\r\n"
    );

    // Wait for the partial resync.
    let mut partial = partial_before;
    for _ in 0..100 {
        partial = stat(&mut master_client, "sync_partial_ok");
        if partial > partial_before {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(partial, partial_before + 1, "no partial resync");
    assert_eq!(
        stat(&mut master_client, "sync_full"),
        full_before,
        "the reconnect fell back to a full sync"
    );

    // Verify k4 is replicated after partial sync
    for _ in 0..40 {
        if send_and_read(&mut replica_client, b"GET k4\r\n") == "$2\r\nv4\r\n" {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        send_and_read(&mut replica_client, b"GET k4\r\n"),
        "$2\r\nv4\r\n"
    );

    // 5. Subsequent mutations replicate cleanly
    assert_eq!(
        send_and_read(&mut master_client, b"SET k5 v5\r\n"),
        "+OK\r\n"
    );
    for _ in 0..40 {
        if send_and_read(&mut replica_client, b"GET k5\r\n") == "$2\r\nv5\r\n" {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        send_and_read(&mut replica_client, b"GET k5\r\n"),
        "$2\r\nv5\r\n"
    );

    // The master counted the full and the partial resync. The stats are
    // process-wide and other tests in this binary replicate too, so only
    // lower bounds hold.
    let stats = send_and_read(&mut master_client, b"INFO stats\r\n");
    let stat = |name: &str| -> u64 {
        stats
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| panic!("{name} missing: {stats}"))
    };
    assert!(stat("sync_full:") >= 1, "{stats}");
    assert!(stat("sync_partial_ok:") >= 1, "{stats}");

    // 6. Test promotion to master via REPLICAOF NO ONE
    assert_eq!(
        send_and_read(&mut replica_client, b"REPLICAOF NO ONE\r\n"),
        "+OK\r\n"
    );
    thread::sleep(Duration::from_millis(100));
    let promoted_info = send_and_read(&mut replica_client, b"INFO replication\r\n");
    assert!(promoted_info.contains("role:master"));
    assert!(promoted_info.contains("second_repl_offset:"));
}

#[test]
fn test_crdt_multi_region_replication_and_gc_e2e() {
    let port1 = 16550;
    let port2 = 16551;
    start_test_server(port1, 2);
    start_test_server(port2, 2);

    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();

    // 1. LWW-Register on Node 1
    let set_resp = send_and_read(&mut client1, b"CRDT.SET geo_key region_us_east\r\n");
    assert!(set_resp.starts_with("+OK"));
    assert_eq!(
        send_and_read(&mut client1, b"CRDT.GET geo_key\r\n"),
        "$14\r\nregion_us_east\r\n"
    );

    // 2. PN-Counters across both nodes
    assert_eq!(
        send_and_read(&mut client1, b"CRDT.INCRBY user_counter 42\r\n"),
        ":42\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"CRDT.INCRBY user_counter 8\r\n"),
        ":8\r\n"
    );

    // 3. OR-Sets across both nodes
    assert_eq!(
        send_and_read(&mut client1, b"CRDT.SADD active_tags tag_gaming\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"CRDT.SADD active_tags tag_social\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"CRDT.SADD active_tags tag_mobile\r\n"),
        ":1\r\n"
    );

    // 4. Cross-Region Replication: Dump Node 1 state and merge into Node 2
    let dump_bytes = send_and_read_bytes(&mut client1, b"CRDT.DUMP\r\n");
    // Parse bulk string payload
    assert!(dump_bytes.starts_with(b"$"));
    let first_newline = dump_bytes.iter().position(|&b| b == b'\n').unwrap();
    let payload = &dump_bytes[first_newline + 1..dump_bytes.len() - 2];

    let mut merge_cmd = Vec::new();
    merge_cmd
        .extend_from_slice(format!("*2\r\n$10\r\nCRDT.MERGE\r\n${}\r\n", payload.len()).as_bytes());
    merge_cmd.extend_from_slice(payload);
    merge_cmd.extend_from_slice(b"\r\n");
    let merge_resp = send_and_read(&mut client2, &merge_cmd);
    assert!(merge_resp.starts_with(":"));

    // Verify converged state on Node 2
    assert_eq!(
        send_and_read(&mut client2, b"CRDT.GET geo_key\r\n"),
        "$14\r\nregion_us_east\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"CRDT.INCRBY user_counter 0\r\n"),
        ":50\r\n"
    ); // 42 + 8 = 50
    let members = send_and_read(&mut client2, b"CRDT.SMEMBERS active_tags\r\n");
    assert!(members.contains("tag_gaming"));
    assert!(members.contains("tag_social"));
    assert!(members.contains("tag_mobile"));

    // 5. Automated Tombstone TTL Garbage Collection
    assert_eq!(
        send_and_read(&mut client1, b"CRDT.DEL geo_key\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"CRDT.GET geo_key\r\n"),
        "$-1\r\n"
    );
    // Run CRDT.GC with 0ms cutoff to instantly reclaim tombstones
    let gc_resp = send_and_read(&mut client1, b"CRDT.GC 0\r\n");
    assert!(gc_resp.contains("registers_pruned"));
    assert!(gc_resp.contains("set_tombstones_pruned"));
}

#[test]
fn test_psync_partial_resync_continue_e2e() {
    let port = 16680;
    start_test_server(port, 2);

    let mut master = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    master
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Query master replication info
    let info = send_and_read(&mut master, b"INFO replication\r\n");
    let mut replid = String::new();
    for line in info.lines() {
        if let Some(stripped) = line.strip_prefix("master_replid:") {
            replid = stripped.trim().to_string();
        }
    }
    assert!(!replid.is_empty(), "Failed to extract master_replid");

    // Like Redis, the backlog starts recording once a replica attaches, so
    // attach one first instead of relying on other tests having done so.
    let mut first = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    first
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    first.write_all(b"PSYNC ? -1\r\n").unwrap();
    let mut fbuf = [0u8; 256];
    let n = first.read(&mut fbuf).unwrap();
    assert!(
        String::from_utf8_lossy(&fbuf[..n]).starts_with("+FULLRESYNC"),
        "{}",
        String::from_utf8_lossy(&fbuf[..n])
    );

    // 2. Pre-populate some keys and check offset
    assert_eq!(send_and_read(&mut master, b"SET k1 v1\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut master, b"SET k2 v2\r\n"), "+OK\r\n");

    let info2 = send_and_read(&mut master, b"INFO replication\r\n");
    let mut offset1: i64 = 0;
    for line in info2.lines() {
        if let Some(stripped) = line.strip_prefix("master_repl_offset:") {
            offset1 = stripped.trim().parse::<i64>().unwrap_or(0);
        }
    }
    assert!(offset1 > 0, "Expected master_repl_offset > 0");

    // 3. Mutate more keys after offset1
    assert_eq!(send_and_read(&mut master, b"SET k3 v3\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut master, b"SET k4 v4\r\n"), "+OK\r\n");

    // 4. Connect replica client requesting partial resync at offset1
    let mut replica = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    replica
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // Like a Redis replica, ask for the next byte after what it has.
    let psync_cmd = format!("PSYNC {} {}\r\n", replid, offset1 + 1);
    replica.write_all(psync_cmd.as_bytes()).unwrap();

    // 5. Read response from master on replica connection
    let mut buf = [0u8; 4096];
    let n = replica.read(&mut buf).unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);

    // Must start with +CONTINUE <replid>
    assert!(
        resp.starts_with(&format!("+CONTINUE {}", replid)),
        "Expected +CONTINUE {}, got: {}",
        replid,
        resp
    );

    // Diff must contain k3 and k4 mutations
    assert!(
        resp.contains("k3") && resp.contains("v3"),
        "Diff should contain k3/v3: {}",
        resp
    );
    assert!(
        resp.contains("k4") && resp.contains("v4"),
        "Diff should contain k4/v4: {}",
        resp
    );

    // 6. Test invalid replid triggers +FULLRESYNC
    let mut bad_replica = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    bad_replica
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    bad_replica
        .write_all(b"PSYNC 0000000000000000000000000000000000000000 0\r\n")
        .unwrap();
    let n = bad_replica.read(&mut buf).unwrap();
    let bad_resp = String::from_utf8_lossy(&buf[..n]);
    assert!(
        bad_resp.starts_with("+FULLRESYNC"),
        "Expected +FULLRESYNC on invalid replid, got: {}",
        bad_resp
    );
}

#[test]
fn test_replication_atomic_bypass_and_slave_gate_e2e() {
    let master_port = 16753;
    let replica_port = 16754;

    start_test_server(master_port, 2);
    start_test_server(replica_port, 2);

    let mut master_client = TcpStream::connect(format!("127.0.0.1:{}", master_port)).unwrap();
    let mut replica_client = TcpStream::connect(format!("127.0.0.1:{}", replica_port)).unwrap();

    // 1. High-volume standalone writes before replica attaches (verifying atomic bypass)
    for i in 0..50 {
        let cmd = format!("SET bypass_k_{} val_{}\r\n", i, i);
        assert_eq!(send_and_read(&mut master_client, cmd.as_bytes()), "+OK\r\n");
    }

    // 2. Attach replica
    let rep_resp = send_and_read(
        &mut replica_client,
        format!("REPLICAOF 127.0.0.1 {}\r\n", master_port).as_bytes(),
    );
    assert_eq!(rep_resp, "+OK\r\n");
    for _ in 0..40 {
        let role = send_and_read(&mut replica_client, b"ROLE\r\n");
        if role.contains("slave") {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    // 3. Replica rejects direct writes with -READONLY
    let write_rep = send_and_read(&mut replica_client, b"SET forbidden_k val\r\n");
    assert!(
        write_rep.contains("-READONLY"),
        "Expected READONLY error on replica, got {}",
        write_rep
    );

    // 4. Master write replicates to replica
    assert_eq!(
        send_and_read(&mut master_client, b"SET live_k live_v\r\n"),
        "+OK\r\n"
    );
    for _ in 0..40 {
        if send_and_read(&mut replica_client, b"GET live_k\r\n") == "$6\r\nlive_v\r\n" {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        send_and_read(&mut replica_client, b"GET live_k\r\n"),
        "$6\r\nlive_v\r\n"
    );

    // 5. Promote replica via REPLICAOF NO ONE
    assert_eq!(
        send_and_read(&mut replica_client, b"REPLICAOF NO ONE\r\n"),
        "+OK\r\n"
    );
    thread::sleep(Duration::from_millis(100));

    // Replica is now master and accepts direct writes
    assert_eq!(
        send_and_read(&mut replica_client, b"SET promoted_k ok\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut replica_client, b"GET promoted_k\r\n"),
        "$2\r\nok\r\n"
    );
}

#[test]
fn test_sync_local_fast_path_and_reactive_harvest_e2e() {
    let port = 16473;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Single GET hit and miss (pipeline 1)
    let resp = send_and_read(&mut client, b"GET not_exist\r\n");
    assert_eq!(resp, "$-1\r\n");

    let resp = send_and_read(&mut client, b"SET single_k single_v\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut client, b"GET single_k\r\n");
    assert_eq!(resp, "$8\r\nsingle_v\r\n");

    // 2. Co-located MGET with hash tags
    let resp = send_and_read(&mut client, b"SET {user}:a va\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut client, b"SET {user}:b vb\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut client, b"MGET {user}:a {user}:b {user}:c\r\n");
    assert_eq!(resp, "*3\r\n$2\r\nva\r\n$2\r\nvb\r\n$-1\r\n");

    // 3. Multi-round scattered MSET and MGET triggering reactive harvest
    for round in 0..5 {
        let mut mset = String::from("MSET");
        for i in 0..10 {
            mset.push_str(&format!(" sc_k_{} val_{}_{}", i, round, i));
        }
        mset.push_str("\r\n");
        let resp = send_and_read(&mut client, mset.as_bytes());
        assert_eq!(resp, "+OK\r\n");

        let mut mget = String::from("MGET");
        for i in 0..10 {
            mget.push_str(&format!(" sc_k_{}", i));
        }
        mget.push_str("\r\n");
        let resp = send_and_read(&mut client, mget.as_bytes());
        assert!(resp.starts_with("*10\r\n"));
        for i in 0..10 {
            assert!(resp.contains(&format!("val_{}_{}", round, i)));
        }
    }
}

#[test]
fn test_per_shard_parallel_replication_stream_e2e() {
    let port = 17040;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    let mut ctrl = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    // Fail instead of hanging the whole serial run if a reply never comes.
    ctrl.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // 1. Handshake with REPLCONF capa dragonfly
    let capa_cmd = format_resp_cmd(&["REPLCONF", "capa", "dragonfly"]);
    let capa_resp = send_and_read(&mut ctrl, &capa_cmd);
    // Expect 5-element array: *5\r\n${replid.len()}\r\n{replid}\r\n$5\r\nSYNC1\r\n:4\r\n:1\r\n:0\r\n
    assert!(
        capa_resp.starts_with("*5\r\n"),
        "Expected array response, got: {}",
        capa_resp
    );
    assert!(capa_resp.contains("SYNC1"));
    assert!(capa_resp.contains(":4\r\n")); // 4 shards

    // 2. Open 4 parallel TCP flow streams for the 4 shards
    let mut flows = Vec::new();
    for sid in 0..num_shards {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let sid_str = sid.to_string();
        let flow_cmd = format_resp_cmd(&["DFLY", "FLOW", "mock_replid", "SYNC1", &sid_str]);
        let resp = send_and_read(&mut stream, &flow_cmd);
        assert!(
            resp.starts_with(&format!("+OK FLOW {}", sid)),
            "Got: {}",
            resp
        );
        flows.push(stream);
    }
    for f in &flows {
        f.set_read_timeout(Some(Duration::from_millis(1000)))
            .unwrap();
    }

    // 3. Find keys that map to shard 0 and shard 1
    let mut key_shard0 = String::new();
    let mut key_shard1 = String::new();
    for i in 0..100 {
        let k = format!("flow_key:{}", i);
        let sid = rudis::router::target_shard(k.as_bytes(), num_shards);
        if sid == 0 && key_shard0.is_empty() {
            key_shard0 = k;
        } else if sid == 1 && key_shard1.is_empty() {
            key_shard1 = k;
        }
        if !key_shard0.is_empty() && !key_shard1.is_empty() {
            break;
        }
    }

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 4. Write key_shard0
    let set_cmd0 = format_resp_cmd(&["SET", &key_shard0, "val0"]);
    assert_eq!(send_and_read(&mut client, &set_cmd0), "+OK\r\n");

    // Flow 0 receives the streamed mutation
    let mut buf = [0u8; 512];
    let deadline0 = std::time::Instant::now() + Duration::from_millis(2000);
    let mut received0 = String::new();
    while std::time::Instant::now() < deadline0 {
        if let Ok(n0) = flows[0].read(&mut buf)
            && n0 > 0
        {
            received0.push_str(&String::from_utf8_lossy(&buf[..n0]));
            if received0.contains("SET") && received0.contains(&key_shard0) {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(received0.contains("SET") && received0.contains(&key_shard0));

    // 5. Write key_shard1
    let set_cmd1 = format_resp_cmd(&["SET", &key_shard1, "val1"]);
    assert_eq!(send_and_read(&mut client, &set_cmd1), "+OK\r\n");

    // Flow 1 receives the streamed mutation
    let deadline1 = std::time::Instant::now() + Duration::from_millis(2000);
    let mut received1 = String::new();
    while std::time::Instant::now() < deadline1 {
        if let Ok(n1) = flows[1].read(&mut buf)
            && n1 > 0
        {
            received1.push_str(&String::from_utf8_lossy(&buf[..n1]));
            if received1.contains("SET") && received1.contains(&key_shard1) {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(received1.contains("SET") && received1.contains(&key_shard1));

    // 6. Flow sends ACK
    let ack_cmd = format_resp_cmd(&["REPLCONF", "ACK", "42"]);
    flows[0].write_all(&ack_cmd).unwrap();
}

#[test]
fn test_e2e_unlink_readonly_wait_object_and_proto_max() {
    let port = 17060;
    start_test_server(port, 2);
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. UNLINK
    assert_eq!(
        send_and_read(&mut stream, b"SET unlink_k hello\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"UNLINK unlink_k nonexistent\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"EXISTS unlink_k\r\n"), ":0\r\n");

    // 2. READONLY and READWRITE
    assert_eq!(send_and_read(&mut stream, b"READONLY\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut stream, b"READWRITE\r\n"), "+OK\r\n");

    // 3. WAIT and WAITAOF
    assert_eq!(send_and_read(&mut stream, b"WAIT 1 50\r\n"), ":0\r\n");
    // No AOF here: there is no local copy to wait for.
    assert!(
        send_and_read(&mut stream, b"WAITAOF 1 1 50\r\n")
            .contains("numlocal is set but appendonly is disabled")
    );
    assert_eq!(
        send_and_read(&mut stream, b"WAITAOF 0 1 50\r\n"),
        "*2\r\n:0\r\n:0\r\n"
    );

    // 4. OBJECT ENCODING, IDLETIME, REFCOUNT, FREQ, HELP
    assert_eq!(
        send_and_read(&mut stream, b"SET obj_k world\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"OBJECT ENCODING obj_k\r\n"),
        "$3\r\nraw\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"OBJECT ENCODING nonexistent\r\n"),
        "$-1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"OBJECT IDLETIME obj_k\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"OBJECT REFCOUNT obj_k\r\n"),
        ":1\r\n"
    );
    // Not tracked unless an LFU maxmemory policy is selected, as in Redis.
    assert!(
        send_and_read(&mut stream, b"OBJECT FREQ obj_k\r\n")
            .starts_with("-ERR An LFU maxmemory policy is not selected")
    );
    let help_resp = send_and_read(&mut stream, b"OBJECT HELP\r\n");
    assert!(help_resp.contains("ENCODING"));

    // 5. CONFIG GET / SET proto-max-bulk-len
    let cfg_get = send_and_read(&mut stream, b"CONFIG GET proto-max-bulk-len\r\n");
    assert!(cfg_get.contains("proto-max-bulk-len"));
    assert_eq!(
        send_and_read(&mut stream, b"CONFIG SET proto-max-bulk-len 1048576\r\n"),
        "+OK\r\n"
    );
    let cfg_get2 = send_and_read(&mut stream, b"CONFIG GET proto-max-bulk-len\r\n");
    assert!(cfg_get2.contains("1048576"));
    // Reset to default 512MB
    assert_eq!(
        send_and_read(&mut stream, b"CONFIG SET proto-max-bulk-len 536870912\r\n"),
        "+OK\r\n"
    );

    // 6. XINFO STREAM, GROUPS, CONSUMERS, HELP
    assert!(!send_and_read(&mut stream, b"XADD e2e_stream * sensor 42\r\n").starts_with('-'));
    assert_eq!(
        send_and_read(&mut stream, b"XGROUP CREATE e2e_stream e2e_grp 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            b"XGROUP CREATECONSUMER e2e_stream e2e_grp e2e_c1\r\n"
        ),
        ":1\r\n"
    );

    let xinfo_stream_resp = send_and_read(&mut stream, b"XINFO STREAM e2e_stream\r\n");
    assert!(xinfo_stream_resp.contains("radix-tree-keys"));
    assert!(xinfo_stream_resp.contains("groups"));

    let xinfo_groups_resp = send_and_read(&mut stream, b"XINFO GROUPS e2e_stream\r\n");
    assert!(xinfo_groups_resp.contains("e2e_grp"));

    let xinfo_consumers_resp =
        send_and_read(&mut stream, b"XINFO CONSUMERS e2e_stream e2e_grp\r\n");
    assert!(xinfo_consumers_resp.contains("e2e_c1"));

    let xinfo_help_resp = send_and_read(&mut stream, b"XINFO HELP\r\n");
    assert!(xinfo_help_resp.contains("CONSUMERS"));

    // 7. COMMAND COUNT and COMMAND LIST
    let count = send_and_read(&mut stream, b"COMMAND COUNT\r\n");
    let count: usize = count.trim().trim_start_matches(':').parse().unwrap();
    assert!(count > 200, "COMMAND COUNT {count}");
    let cmd_list_resp = send_and_read(&mut stream, b"COMMAND LIST\r\n");
    assert!(cmd_list_resp.contains("xinfo"));
    assert!(cmd_list_resp.contains("unlink"));
}

#[test]
fn test_ai_native_durability_and_replication_e2e() {
    let primary_port = 19160;
    let replica_port = 19161;
    let cold_port = 19162;
    let data_dir =
        std::env::temp_dir().join(format!("rudis-ai-durability-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::create_dir_all(&data_dir);

    start_test_server_with_aof(
        primary_port,
        2,
        rudis::aof::AofConfig {
            enabled: true,
            dir: data_dir.clone(),
            fsync_every_sec: false,
        },
    );
    start_test_server(replica_port, 2);

    let mut primary = TcpStream::connect(("127.0.0.1", primary_port)).unwrap();
    let mut replica = TcpStream::connect(("127.0.0.1", replica_port)).unwrap();
    primary
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    replica
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. Pre-populate AI-native state on primary before replica connects
    assert_eq!(
        send_and_read(
            &mut primary,
            &format_resp_cmd(&[
                "SEMANTIC.SET",
                "llm:e2e",
                "q_init",
                "What is Rudis?",
                "Rudis is an AI-native data store.",
                "VECTOR",
                "3",
                "1.0",
                "0.0",
                "0.0",
                "SCOPE",
                "org:alpha",
                "TOKENS",
                "20",
            ]),
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut primary,
            &format_resp_cmd(&[
                "AGENT.MEM.ADD",
                "sess:e2e",
                "user",
                "Remember launch code 9911.",
                "TOKENS",
                "6",
                "VEC",
                "3",
                "1.0",
                "0.0",
                "0.0",
            ]),
        ),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut primary,
            &format_resp_cmd(&[
                "AGENT.CHECKPOINT.PUT",
                "ckpt:e2e",
                "step1",
                "STATE",
                r#"{"stage":"init"}"#,
            ]),
        ),
        ":1\r\n"
    );

    // 2. Connect replica via REPLICAOF and wait for full-resync RDB transfer
    assert_eq!(
        send_and_read(
            &mut replica,
            format!("REPLICAOF 127.0.0.1 {}\r\n", primary_port).as_bytes(),
        ),
        "+OK\r\n"
    );
    for _ in 0..40 {
        let role = send_and_read(&mut replica, b"ROLE\r\n");
        if role.contains("slave") && role.contains("connected") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    // Verify full-resync RDB restored AI state on replica
    let rep_sem = send_and_read(
        &mut replica,
        &format_resp_cmd(&[
            "SEMANTIC.GET",
            "llm:e2e",
            "THRESHOLD",
            "0.90",
            "VECTOR",
            "3",
            "1.0",
            "0.0",
            "0.0",
            "SCOPE",
            "org:alpha",
        ]),
    );
    assert!(
        rep_sem.contains("Rudis is an AI-native data store."),
        "Replica should serve SEMANTIC.GET after RDB full-resync: {}",
        rep_sem
    );

    // 3. Write live AI-native mutations on primary and verify incremental replication + BGREWRITEAOF + SAVE
    assert_eq!(
        send_and_read(
            &mut primary,
            &format_resp_cmd(&[
                "AGENT.MEM.COMPACT",
                "sess:e2e",
                "KEEP_RECENT",
                "0",
                "SUMMARY",
                "User shared launch code.",
                "TOKENS",
                "5",
                "VEC",
                "3",
                "0.5",
                "0.5",
                "0.0",
            ]),
        ),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut primary,
            &format_resp_cmd(&[
                "AGENT.MEM.ADD",
                "sess:e2e",
                "user",
                "Ready for liftoff.",
                "TOKENS",
                "4",
                "VEC",
                "3",
                "0.0",
                "1.0",
                "0.0",
            ]),
        ),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut primary,
            &format_resp_cmd(&[
                "AGENT.CHECKPOINT.PUT",
                "ckpt:e2e",
                "step2",
                "PARENT",
                "step1",
                "STATE",
                r#"{"stage":"ready"}"#,
            ]),
        ),
        ":2\r\n"
    );
    let claim_resp = send_and_read(
        &mut primary,
        &format_resp_cmd(&[
            "AGENT.TOOL.CLAIM",
            "tools:e2e",
            "call:1",
            "TTL",
            "60000",
            "INPUT",
            r#"{"fn":"deploy"}"#,
        ]),
    );
    assert!(
        claim_resp.contains("CLAIMED"),
        "Expected CLAIMED response: {}",
        claim_resp
    );
    assert_eq!(
        send_and_read(
            &mut primary,
            &format_resp_cmd(&[
                "AGENT.TOOL.COMPLETE",
                "tools:e2e",
                "call:1",
                "OUTPUT",
                r#"{"ok":true}"#,
                "TTL",
                "60000",
            ]),
        ),
        ":1\r\n"
    );

    // Wait for incremental replication to reach replica
    for _ in 0..40 {
        let hist = send_and_read(
            &mut replica,
            &format_resp_cmd(&["AGENT.CHECKPOINT.HISTORY", "ckpt:e2e"]),
        );
        if hist.contains("step2") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let rep_ctx = send_and_read(
        &mut replica,
        &format_resp_cmd(&[
            "AGENT.MEM.CONTEXT",
            "sess:e2e",
            "MAX_TOKENS",
            "100",
            "QUERY",
            "3",
            "1.0",
            "0.0",
            "0.0",
            "RECALL",
            "2",
        ]),
    );
    assert!(
        rep_ctx.contains("User shared launch code.")
            && rep_ctx.contains("Ready for liftoff.")
            && rep_ctx.contains("Remember launch code 9911."),
        "Replica should have compacted working window and recalled episode: {}",
        rep_ctx
    );

    // 4. Compact AOF and save RDB on primary, then verify cold-start restore
    let bg_resp = send_and_read(&mut primary, b"BGREWRITEAOF\r\n");
    assert!(
        bg_resp.starts_with('+'),
        "BGREWRITEAOF response: {}",
        bg_resp
    );
    let mut save_ok = false;
    for _ in 0..40 {
        let save_resp = send_and_read(&mut primary, b"SAVE\r\n");
        if save_resp == "+OK\r\n" {
            save_ok = true;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(save_ok, "SAVE did not succeed after BGREWRITEAOF");

    start_test_server_with_aof(
        cold_port,
        2,
        rudis::aof::AofConfig {
            enabled: false,
            dir: data_dir.clone(),
            fsync_every_sec: false,
        },
    );
    let mut cold = TcpStream::connect(("127.0.0.1", cold_port)).unwrap();
    cold.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    let cold_ctx = send_and_read(
        &mut cold,
        &format_resp_cmd(&[
            "AGENT.MEM.CONTEXT",
            "sess:e2e",
            "MAX_TOKENS",
            "100",
            "QUERY",
            "3",
            "1.0",
            "0.0",
            "0.0",
            "RECALL",
            "2",
        ]),
    );
    assert!(
        cold_ctx.contains("User shared launch code.")
            && cold_ctx.contains("Ready for liftoff.")
            && cold_ctx.contains("Remember launch code 9911."),
        "Cold-start server should restore AGENT.MEM state from RDB: {}",
        cold_ctx
    );
    let cold_tool = send_and_read(
        &mut cold,
        &format_resp_cmd(&["AGENT.TOOL.CLAIM", "tools:e2e", "call:1"]),
    );
    assert!(
        cold_tool.contains("COMPLETED") && cold_tool.contains(r#"{"ok":true}"#),
        "Cold-start server should restore completed tool call from RDB: {}",
        cold_tool
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// A server starting from RDB files cargo-fuzz found overflowing a size or
/// counter (a panic in builds with overflow checks) either serves or
/// refuses the file cleanly; it never panics.
#[test]
fn test_startup_survives_overflowing_rdb_fields_e2e() {
    let dir = std::env::temp_dir().join(format!("rudis-rdb-overflow-{}", std::process::id()));
    for (i, body) in [
        &include_bytes!("fixtures/fuzz/load_rdb_resizedb_overflow.bin")[..],
        &include_bytes!("fixtures/fuzz/load_rdb_semantic_counter_overflow.bin")[..],
    ]
    .into_iter()
    .enumerate()
    {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut rdb = b"REDIS0011".to_vec();
        rdb.extend_from_slice(body);
        let crc = rudis::table::crc64(&rdb);
        rdb.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(dir.join("dump.rdb"), &rdb).unwrap();
        let mut child = rudis_bin()
            .args(["--port", "18039", "--threads", "2", "--no-pin", "--aof-dir"])
            .arg(&dir)
            .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn rudis");
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let served = loop {
            if child.try_wait().unwrap().is_some() {
                break false;
            }
            // A connection the previous input's SIGKILLed server still held
            // may be reset: only a reply means this server is up.
            if let Ok(mut c) = TcpStream::connect("127.0.0.1:18039") {
                let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
                let mut buf = [0u8; 64];
                if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok()
                    && let Ok(n) = c.read(&mut buf)
                    && n > 0
                {
                    assert_eq!(&buf[..n], b"+PONG\r\n", "input {i}");
                    break true;
                }
            }
            assert!(std::time::Instant::now() < deadline, "input {i}: no start");
            thread::sleep(Duration::from_millis(50));
        };
        let _ = child.kill();
        let status = child.wait().unwrap();
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert!(!stderr.contains("panicked"), "input {i}: {stderr}");
        if !served {
            assert_eq!(status.code(), Some(1), "input {i}: {stderr}");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_appendonly_and_appendfsync_are_reported_truthfully_e2e() {
    let port = 16977;
    let dir = std::env::temp_dir().join(format!("rudis-appendfsync-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let conf = dir.join("aof.conf");
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof-dir",
        dir.to_str().unwrap(),
        "-c",
        conf.to_str().unwrap(),
    ];

    // A bad value refuses to start rather than run with some other policy.
    std::fs::write(&conf, "appendonly yes\nappendfsync sometimes\n").unwrap();
    let (status, stderr) = run_rudis_until_exit(&args);
    assert!(!status.success(), "{stderr}");
    assert!(stderr.contains("invalid appendfsync"), "{stderr}");

    std::fs::write(&conf, "appendonly yes\nappendfsync no\n").unwrap();
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let get = |c: &mut TcpStream, p: &str| resp_cmd(c, &["CONFIG", "GET", p]);
    assert_eq!(
        get(&mut c, "appendfsync"),
        "*2\r\n$11\r\nappendfsync\r\n$2\r\nno\r\n"
    );
    assert_eq!(
        get(&mut c, "appendonly"),
        "*2\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n"
    );

    // appendfsync switches at runtime (always is covered by its own test).
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "appendfsync", "everysec"]),
        "+OK\r\n"
    );
    assert_eq!(
        get(&mut c, "appendfsync"),
        "*2\r\n$11\r\nappendfsync\r\n$8\r\neverysec\r\n"
    );
    let r = resp_cmd(&mut c, &["CONFIG", "SET", "appendfsync", "sometimes"]);
    assert!(r.starts_with("-ERR"), "{r}");
    assert_eq!(
        get(&mut c, "appendfsync"),
        "*2\r\n$11\r\nappendfsync\r\n$8\r\neverysec\r\n"
    );

    // Setting appendonly to what it is changes nothing.
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "appendonly", "yes"]),
        "+OK\r\n"
    );
    assert_eq!(
        get(&mut c, "appendonly"),
        "*2\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n"
    );

    // The AOF is still written and replayed.
    assert_eq!(resp_cmd(&mut c, &["SET", "fsync:k", "v"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["GET", "fsync:k"]), "$1\r\nv\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_blocking_pops_and_rare_writes_replicate_e2e() {
    let (mport, rport) = (16945u16, 16944u16);
    let (ms, rs) = (mport.to_string(), rport.to_string());
    let mut master = spawn_rudis_listening(&["--port", &ms, "--threads", "4", "--no-pin"], mport);
    let mut replica = spawn_rudis_listening(&["--port", &rs, "--threads", "2", "--no-pin"], rport);
    let connect = |port: u16| {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c
    };
    let mut m = connect(mport);
    let mut r = connect(rport);
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    let mut role = String::new();
    for _ in 0..100 {
        role = resp_cmd(&mut r, &["ROLE"]);
        if role.contains("connected") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(role.contains("connected"), "{role}");

    // Immediate pops: with 4 shards most keys are owned by another shard than
    // the connection's, which used to replicate the pop twice.
    let lists: Vec<String> = (0..8).map(|i| format!("l{i}")).collect();
    let zsets: Vec<String> = (0..8).map(|i| format!("z{i}")).collect();
    for (l, z) in lists.iter().zip(&zsets) {
        resp_cmd(&mut m, &["RPUSH", l, "a", "b", "c", "d", "e"]);
        resp_cmd(&mut m, &["ZADD", z, "1", "a", "2", "b", "3", "c", "4", "d"]);
        resp_cmd(&mut m, &["BLPOP", l, "1"]);
        resp_cmd(&mut m, &["BRPOP", l, "1"]);
        resp_cmd(&mut m, &["BZPOPMIN", z, "1"]);
        resp_cmd(&mut m, &["BZPOPMAX", z, "1"]);
        resp_cmd(&mut m, &["LPUSHX", l, "x"]);
    }
    // Blocked pops served by a later push: the block hub pops on the key's
    // shard and never used to replicate BLPOP/BRPOP.
    let blocked: Vec<(&str, &str)> = vec![
        ("BLPOP", "bl"),
        ("BRPOP", "br"),
        ("BZPOPMIN", "bz"),
        ("BZPOPMAX", "bx"),
    ];
    let waiters: Vec<_> = blocked
        .iter()
        .map(|(cmd, key)| {
            let (cmd, key) = (cmd.to_string(), key.to_string());
            thread::spawn(move || {
                let mut c = connect(mport);
                resp_cmd(&mut c, &[&cmd, &key, "5"])
            })
        })
        .collect();
    let mut queued = false;
    for _ in 0..100 {
        let info = resp_cmd(&mut m, &["INFO", "clients"]);
        if info.contains("blocked_clients:4") {
            queued = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(queued, "waiters never blocked");
    for key in ["bl", "br"] {
        resp_cmd(&mut m, &["RPUSH", key, "x", "y", "z"]);
    }
    for key in ["bz", "bx"] {
        resp_cmd(&mut m, &["ZADD", key, "1", "x", "2", "y", "3", "z"]);
    }
    for w in waiters {
        let reply = w.join().unwrap();
        assert!(reply.starts_with("*"), "{reply}");
    }
    // Writes the AOF encoder used to drop.
    resp_cmd(&mut m, &["SADD", "{s}a", "1", "2", "3"]);
    resp_cmd(&mut m, &["SADD", "{s}b", "2", "3", "4"]);
    resp_cmd(&mut m, &["SINTERSTORE", "{s}i", "{s}a", "{s}b"]);
    resp_cmd(&mut m, &["ZADD", "{z}a", "1", "m", "2", "n"]);
    resp_cmd(
        &mut m,
        &[
            "ZUNIONSTORE",
            "{z}u",
            "2",
            "{z}a",
            "{z}a",
            "WEIGHTS",
            "1",
            "2",
        ],
    );
    resp_cmd(
        &mut m,
        &["GEOADD", "geo", "13.361389", "38.115556", "Palermo"],
    );
    resp_cmd(&mut m, &["SET", "dx", "v"]);
    resp_cmd(&mut m, &["DELEX", "dx", "IFEQ", "v"]);
    resp_cmd(&mut m, &["SET", "repl:done", "1"]);
    let mut synced = false;
    for _ in 0..100 {
        if resp_cmd(&mut r, &["GET", "repl:done"]) == "$1\r\n1\r\n" {
            synced = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(synced, "replica never caught up");

    let mut checks: Vec<Vec<String>> = Vec::new();
    for l in lists.iter().chain(["bl", "br"].map(String::from).iter()) {
        checks.push(vec!["LRANGE".into(), l.clone(), "0".into(), "-1".into()]);
    }
    for z in zsets
        .iter()
        .chain(["bz", "bx", "{z}u"].map(String::from).iter())
    {
        checks.push(vec![
            "ZRANGE".into(),
            z.clone(),
            "0".into(),
            "-1".into(),
            "WITHSCORES".into(),
        ]);
    }
    checks.push(vec!["SORT_RO".into(), "{s}i".into()]);
    checks.push(vec!["GEOHASH".into(), "geo".into(), "Palermo".into()]);
    checks.push(vec!["EXISTS".into(), "dx".into()]);
    checks.push(vec!["DBSIZE".into()]);
    for check in &checks {
        let args: Vec<&str> = check.iter().map(String::as_str).collect();
        assert_eq!(
            resp_cmd(&mut m, &args),
            resp_cmd(&mut r, &args),
            "{check:?}"
        );
    }
    drop(m);
    drop(r);
    shutdown_and_wait(rport, &mut replica);
    shutdown_and_wait(mport, &mut master);
}

#[test]
fn test_replica_output_buffer_limit_disconnects_stuck_replica_e2e() {
    use std::io::{Read, Write};
    let port = 16936u16;
    let ps = port.to_string();
    let mut master = spawn_rudis_listening(&["--port", &ps, "--threads", "2", "--no-pin"], port);
    let mut m = TcpStream::connect(("127.0.0.1", port)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(
        resp_cmd(
            &mut m,
            &[
                "CONFIG",
                "SET",
                "client-output-buffer-limit",
                "replica 1mb 0 0"
            ]
        ),
        "+OK\r\n"
    );

    // A replica that takes the snapshot and then stops reading.
    let mut stuck = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stuck
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    stuck
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut head = [0u8; 64];
    let n = stuck.read(&mut head).unwrap();
    assert!(head[..n].starts_with(b"+FULLRESYNC"), "{:?}", &head[..n]);
    let mut connected = false;
    for _ in 0..100 {
        if resp_cmd(&mut m, &["INFO", "replication"]).contains("connected_slaves:1") {
            connected = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(connected);

    // Far more stream than the limit plus what the sockets can hold.
    let value = "v".repeat(256 * 1024);
    for i in 0..256 {
        assert_eq!(
            resp_cmd(&mut m, &["SET", &format!("big:{i}"), &value]),
            "+OK\r\n"
        );
    }

    let mut stats = String::new();
    let mut dropped = false;
    for _ in 0..250 {
        stats = resp_cmd(&mut m, &["INFO", "stats"]);
        if stats.contains("client_output_buffer_limit_disconnections:1") {
            dropped = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(dropped, "{stats}");
    let info = resp_cmd(&mut m, &["INFO", "replication"]);
    assert!(info.contains("connected_slaves:0"), "{info}");

    // The stuck replica sees the link closed once it drains it.
    let mut buf = vec![0u8; 1 << 20];
    let mut drained = 0usize;
    loop {
        match stuck.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => drained += n,
        }
    }
    assert!(drained < 64 << 20, "{drained}");
    // The master keeps serving.
    assert_eq!(resp_cmd(&mut m, &["PING"]), "+PONG\r\n");
    let _ = master.kill();
    let _ = master.wait();
}

#[test]
fn test_full_sync_under_concurrent_writes_matches_master_e2e() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (mport, rport) = (16943u16, 16942u16);
    let (ms, rs) = (mport.to_string(), rport.to_string());
    let mut master = spawn_rudis_listening(&["--port", &ms, "--threads", "4", "--no-pin"], mport);
    let mut replica = spawn_rudis_listening(&["--port", &rs, "--threads", "2", "--no-pin"], rport);
    let connect = |port: u16| {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c
    };
    let mut m = connect(mport);
    let mut r = connect(rport);

    // repl-backlog-size used to be stored but never applied.
    assert_eq!(
        resp_cmd(&mut m, &["CONFIG", "SET", "repl-backlog-size", "8mb"]),
        "+OK\r\n"
    );
    let info = resp_cmd(&mut m, &["INFO", "replication"]);
    assert!(info.contains("repl_backlog_size:8388608"), "{info}");

    // Bulk data so each shard's snapshot takes a moment.
    for batch in 0..20 {
        let mut args: Vec<String> = vec!["MSET".into()];
        for i in 0..1000 {
            args.push(format!("bulk:{batch}:{i}"));
            args.push("v".repeat(64));
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert_eq!(resp_cmd(&mut m, &args), "+OK\r\n");
    }

    // Non-idempotent writes on keys of every shard, running across the full
    // sync and a partial resync. A change that the snapshot misses or the
    // stream repeats leaves a counter off.
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let stop = stop.clone();
            thread::spawn(move || {
                let mut c = connect(mport);
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("ctr:{}", (n * 7 + w) % 64);
                    let reply = resp_cmd(&mut c, &["INCR", &key]);
                    assert!(reply.starts_with(':'), "{reply}");
                    n += 1;
                }
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(200));
    let wait_connected = |r: &mut TcpStream| {
        let mut role = String::new();
        for _ in 0..200 {
            role = resp_cmd(r, &["ROLE"]);
            if role.contains("connected") {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("replica never connected: {role}");
    };
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    wait_connected(&mut r);
    thread::sleep(Duration::from_millis(300));
    // Drop the link from the master side: the replica reconnects on its
    // own and resumes from the backlog. (Re-issuing REPLICAOF to the same
    // master is a no-op, as in Redis.)
    assert_eq!(
        resp_cmd(&mut m, &["CLIENT", "KILL", "TYPE", "replica"]),
        ":1\r\n"
    );
    let mut stats = String::new();
    for _ in 0..200 {
        stats = resp_cmd(&mut m, &["INFO", "stats"]);
        if stats.contains("sync_partial_ok:1\r\n") {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(stats.contains("sync_partial_ok:1\r\n"), "{stats}");
    wait_connected(&mut r);
    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }

    resp_cmd(&mut m, &["SET", "repl:done", "1"]);
    let mut synced = false;
    for _ in 0..200 {
        if resp_cmd(&mut r, &["GET", "repl:done"]) == "$1\r\n1\r\n" {
            synced = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(synced, "replica never caught up");
    for i in 0..64 {
        let key = format!("ctr:{i}");
        assert_eq!(
            resp_cmd(&mut m, &["GET", &key]),
            resp_cmd(&mut r, &["GET", &key]),
            "{key}"
        );
    }
    assert_eq!(resp_cmd(&mut m, &["DBSIZE"]), resp_cmd(&mut r, &["DBSIZE"]));
    let stats = resp_cmd(&mut m, &["INFO", "stats"]);
    assert!(stats.contains("sync_full:1\r\n"), "{stats}");
    assert!(stats.contains("sync_partial_ok:1\r\n"), "{stats}");
    drop(m);
    drop(r);
    shutdown_and_wait(rport, &mut replica);
    shutdown_and_wait(mport, &mut master);
}

#[test]
fn test_replicaof_and_masterauth_in_config_file_e2e() {
    let (mport, rport) = (16956u16, 16957u16);
    let dir = std::env::temp_dir().join(format!("rudis-replicaof-conf-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mconf = dir.join("master.conf");
    std::fs::write(&mconf, "requirepass s3cret\n").unwrap();
    // A host name (not an IP literal) and masterauth, as typically deployed.
    let rconf = dir.join("replica.conf");
    std::fs::write(
        &rconf,
        format!("replicaof localhost {mport}\nmasterauth s3cret\n"),
    )
    .unwrap();
    let (mport_s, rport_s) = (mport.to_string(), rport.to_string());
    let mut master = spawn_rudis_listening(
        &[
            "--port",
            &mport_s,
            "--threads",
            "2",
            "--no-pin",
            "-c",
            mconf.to_str().unwrap(),
        ],
        mport,
    );
    let mut replica = spawn_rudis_listening(
        &[
            "--port",
            &rport_s,
            "--threads",
            "2",
            "--no-pin",
            "-c",
            rconf.to_str().unwrap(),
        ],
        rport,
    );
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut m, &["AUTH", "s3cret"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut m, &["SET", "conf:k", "v1"]), "+OK\r\n");
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if resp_cmd(&mut r, &["GET", "conf:k"]) == "$2\r\nv1\r\n" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica never synced: {}",
            resp_cmd(&mut r, &["INFO", "replication"])
        );
        thread::sleep(Duration::from_millis(100));
    }
    let info = resp_cmd(&mut r, &["INFO", "replication"]);
    assert!(info.contains("role:slave"), "{info}");
    assert!(info.contains("master_link_status:up"), "{info}");
    let _ = replica.kill();
    let _ = replica.wait();
    let _ = master.kill();
    let _ = master.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_timeout_keepalive_and_replica_read_only_directives_e2e() {
    let (port, mport, rport) = (17100u16, 17101u16, 17102u16);
    let dir = std::env::temp_dir().join(format!("rudis-idle-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("idle.conf");
    std::fs::write(&conf, "timeout 1\ntcp-keepalive 60\n").unwrap();
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "2",
            "--no-pin",
            "-c",
            conf.to_str().unwrap(),
        ],
        port,
    );
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c
    };
    let mut active = connect();
    assert_eq!(
        resp_cmd(&mut active, &["CONFIG", "GET", "timeout"]),
        "*2\r\n$7\r\ntimeout\r\n$1\r\n1\r\n"
    );
    assert_eq!(
        resp_cmd(&mut active, &["CONFIG", "GET", "tcp-keepalive"]),
        "*2\r\n$13\r\ntcp-keepalive\r\n$2\r\n60\r\n"
    );
    let mut idle = connect();
    assert_eq!(resp_cmd(&mut idle, &["PING"]), "+PONG\r\n");
    let mut sub = connect();
    sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nidch\r\n")
        .unwrap();
    let mut buf = [0u8; 256];
    assert!(sub.read(&mut buf).unwrap() > 0);
    // Keep one client busy past the timeout.
    for _ in 0..8 {
        thread::sleep(Duration::from_millis(300));
        assert_eq!(resp_cmd(&mut active, &["PING"]), "+PONG\r\n");
    }
    // The idle client was closed by the server...
    assert_eq!(
        idle.read(&mut buf).unwrap_or(0),
        0,
        "idle client still open"
    );
    // ...while the subscriber, which only listens, was kept.
    assert_eq!(resp_cmd(&mut active, &["PUBLISH", "idch", "hi"]), ":1\r\n");
    let n = sub.read(&mut buf).unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains("hi"));
    drop(active);
    let _ = child.kill();
    let _ = child.wait();

    // replica-read-only no lets a replica's clients write.
    let rconf = dir.join("replica.conf");
    std::fs::write(
        &rconf,
        format!("replicaof 127.0.0.1 {mport}\nreplica-read-only no\n"),
    )
    .unwrap();
    let (mport_s, rport_s) = (mport.to_string(), rport.to_string());
    let mut master =
        spawn_rudis_listening(&["--port", &mport_s, "--threads", "2", "--no-pin"], mport);
    let mut replica = spawn_rudis_listening(
        &[
            "--port",
            &rport_s,
            "--threads",
            "2",
            "--no-pin",
            "-c",
            rconf.to_str().unwrap(),
        ],
        rport,
    );
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !resp_cmd(&mut r, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(
            std::time::Instant::now() < deadline,
            "replica never connected"
        );
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(resp_cmd(&mut r, &["SET", "ro:k", "v"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut r, &["CONFIG", "SET", "replica-read-only", "yes"]),
        "+OK\r\n"
    );
    assert!(resp_cmd(&mut r, &["SET", "ro:k", "v"]).starts_with("-READONLY"));
    let _ = replica.kill();
    let _ = replica.wait();
    let _ = master.kill();
    let _ = master.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A hostile master (and replica) must only ever cost the link: the replica
/// drops it and reconnects, the master closes the flow, and both keep
/// serving clients.
#[test]
fn test_hostile_replication_peer_drops_link_e2e() {
    use std::net::TcpListener;
    use std::sync::mpsc;

    let port: u16 = 17087;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "2", "--no-pin"];
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = KillOnDrop(spawn_rudis_listening(&args, port));

    // As a master: a flow for a shard that does not exist is refused, and a
    // replica streaming an endless partial command is cut off.
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let reply = resp_cmd(&mut c, &["DFLY", "FLOW", "x", "y", "18446744073709551615"]);
    assert_eq!(reply, "-ERR invalid shard id\r\n");
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let reply = resp_cmd(&mut c, &["DFLY", "FLOW", "x", "y", "0"]);
    assert!(reply.starts_with("+OK FLOW 0\r\n"), "{reply:?}");
    let _ = c.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$999999999\r\n");
    let _ = c.write_all(&vec![b'1'; 1 << 20]);
    let mut sink = [0u8; 65536];
    let closed = loop {
        match c.read(&mut sink) {
            Ok(0) => break true,
            Ok(_) => {}
            Err(e) => break e.kind() == std::io::ErrorKind::ConnectionReset,
        }
    };
    assert!(closed, "master kept a replica link with an endless command");
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");

    // As a replica: each hostile master reply must make it reconnect.
    let master = TcpListener::bind("127.0.0.1:0").unwrap();
    let master_port = master.local_addr().unwrap().port();
    let (conn_tx, conn_rx) = mpsc::channel::<usize>();
    thread::spawn(move || {
        let replid = "a".repeat(40);
        let attacks: Vec<Vec<u8>> = vec![
            // A reply line that never ends.
            vec![b'+'; 1 << 20],
            // A FULLRESYNC with a bogus replication id.
            b"+FULLRESYNC not-an-id 0\r\n$0\r\n".to_vec(),
            // An RDB length that is not a number.
            format!("+FULLRESYNC {replid} 0\r\n$abc\r\n").into_bytes(),
            // A command stream that does not parse.
            format!("+FULLRESYNC {replid} 0\r\n$0\r\n*1\r\n$x\r\n").into_bytes(),
        ];
        let mut held = Vec::new();
        for (i, conn) in master.incoming().enumerate() {
            let Ok(mut conn) = conn else { return };
            let _ = conn_tx.send(i);
            let Some(attack) = attacks.get(i) else {
                held.push(conn);
                continue;
            };
            conn.set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut got = Vec::new();
            let mut buf = [0u8; 4096];
            let mut answer = |conn: &mut TcpStream, until: &[u8], reply: &[u8]| {
                while !got.windows(until.len()).any(|w| w == until) {
                    match conn.read(&mut buf) {
                        Ok(n) if n > 0 => got.extend_from_slice(&buf[..n]),
                        _ => return false,
                    }
                }
                got.clear();
                conn.write_all(reply).is_ok()
            };
            if i == 0 {
                answer(&mut conn, b"PING\r\n", attack);
            } else {
                let _ = answer(&mut conn, b"PING\r\n", b"+PONG\r\n")
                    && answer(&mut conn, b"listening-port\r\n", b"+OK\r\n")
                    && answer(&mut conn, b"psync2\r\n", b"+OK\r\n")
                    && answer(&mut conn, b"PSYNC\r\n", attack);
            }
            // Keep the connection open: only the replica may end it.
            held.push(conn);
        }
    });

    let mport = master_port.to_string();
    assert_eq!(
        resp_cmd(&mut c, &["REPLICAOF", "127.0.0.1", &mport]),
        "+OK\r\n"
    );
    for want in 0..5 {
        let got = conn_rx
            .recv_timeout(Duration::from_secs(20))
            .unwrap_or_else(|_| panic!("replica did not reconnect after attack {want}"));
        assert_eq!(got, want);
    }
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    let info = resp_cmd(&mut c, &["INFO", "replication"]);
    assert!(info.contains("role:slave"), "{info}");
    assert!(!info.contains("master_link_status:up"), "{info}");
    assert_eq!(resp_cmd(&mut c, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child.0);
}

/// A full sync replaces the replica's dataset (old keys go, like Redis), and
/// an RDB that fails to load leaves it empty with the link down and the
/// master's history forgotten, so the next attempt is a full resync.
#[test]
fn test_full_sync_replaces_dataset_and_failed_load_leaves_none_e2e() {
    use std::net::TcpListener;
    use std::sync::mpsc;

    let port: u16 = 17087;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "2", "--no-pin"];
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = KillOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "stale", "1"]), "+OK\r\n");

    let mut src = rudis::shard::ShardDb::new(0);
    src.table.set(
        bytes::Bytes::from_static(b"fresh"),
        bytes::Bytes::from_static(b"1"),
        None,
    );
    let mut chunk = Vec::new();
    src.save_rdb_chunk(&mut chunk).unwrap();
    let rdb = |chunk: &[u8]| {
        let mut v = b"REDIS0011\xFE\x00".to_vec();
        v.extend_from_slice(chunk);
        v.push(0xFF);
        let crc = rudis::table::crc64(&v);
        v.extend_from_slice(&crc.to_le_bytes());
        v
    };
    let good = rdb(&chunk);
    // Valid checksum, but the last record is cut short.
    let corrupt = rdb(&chunk[..chunk.len() - 1]);

    let master = TcpListener::bind("127.0.0.1:0").unwrap();
    let master_port = master.local_addr().unwrap().port();
    let (psync_tx, psync_rx) = mpsc::channel::<String>();
    let (close_tx, close_rx) = mpsc::channel::<()>();
    thread::spawn(move || {
        let replid = "a".repeat(40);
        let mut held = Vec::new();
        for (i, conn) in master.incoming().enumerate() {
            let Ok(mut conn) = conn else { return };
            conn.set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut got = Vec::new();
            let mut buf = [0u8; 4096];
            // Reads until `until` and then `crlfs` more line ends.
            let mut read_to = |conn: &mut TcpStream, until: &[u8], crlfs: usize| {
                got.clear();
                loop {
                    if let Some(p) = got.windows(until.len()).position(|w| w == until) {
                        let tail = &got[p + until.len()..];
                        if tail.windows(2).filter(|w| w == b"\r\n").count() >= crlfs {
                            return Some(String::from_utf8_lossy(&got).into_owned());
                        }
                    }
                    match conn.read(&mut buf) {
                        Ok(n) if n > 0 => got.extend_from_slice(&buf[..n]),
                        _ => return None,
                    }
                }
            };
            let ok = read_to(&mut conn, b"PING\r\n", 0).is_some()
                && conn.write_all(b"+PONG\r\n").is_ok()
                && read_to(&mut conn, b"listening-port\r\n", 2).is_some()
                && conn.write_all(b"+OK\r\n").is_ok()
                && read_to(&mut conn, b"psync2\r\n", 0).is_some()
                && conn.write_all(b"+OK\r\n").is_ok();
            let Some(psync) = read_to(&mut conn, b"PSYNC\r\n", 4).filter(|_| ok) else {
                return;
            };
            let _ = psync_tx.send(psync);
            let payload = match i {
                0 => &good,
                1 => &corrupt,
                _ => {
                    held.push(conn);
                    continue;
                }
            };
            let mut reply =
                format!("+FULLRESYNC {replid} 0\r\n${}\r\n", payload.len()).into_bytes();
            reply.extend_from_slice(payload);
            let _ = conn.write_all(&reply);
            if i == 0 {
                let _ = close_rx.recv();
            } else {
                held.push(conn);
            }
        }
    });

    let wait_for = |c: &mut TcpStream, args: &[&str], want: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let got = resp_cmd(c, args);
            if got.contains(want) {
                return got;
            }
            assert!(std::time::Instant::now() < deadline, "{args:?}: {got:?}");
            thread::sleep(Duration::from_millis(50));
        }
    };
    let mport = master_port.to_string();
    assert_eq!(
        resp_cmd(&mut c, &["REPLICAOF", "127.0.0.1", &mport]),
        "+OK\r\n"
    );
    let first = psync_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(first.contains("\r\n?\r\n"), "{first:?}");
    wait_for(&mut c, &["INFO", "replication"], "master_link_status:up");
    assert_eq!(resp_cmd(&mut c, &["GET", "fresh"]), "$1\r\n1\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "stale"]), "$-1\r\n");

    // The link drops; the next full sync delivers an RDB that fails to load.
    close_tx.send(()).unwrap();
    let second = psync_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(second.contains(&"a".repeat(40)), "{second:?}");
    wait_for(&mut c, &["DBSIZE"], ":0\r\n");
    let third = psync_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(third.contains("\r\n?\r\n"), "{third:?}");
    let info = resp_cmd(&mut c, &["INFO", "replication"]);
    assert!(!info.contains("master_link_status:up"), "{info}");
    assert_eq!(resp_cmd(&mut c, &["GET", "fresh"]), "$-1\r\n");
    assert_eq!(resp_cmd(&mut c, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child.0);
}

#[test]
fn test_replica_ack_beyond_master_offset_does_not_satisfy_wait_e2e() {
    let port: u16 = 17088;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "2", "--no-pin"];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let repl_field = |c: &mut TcpStream, field: &str| {
        let info = resp_cmd(c, &["INFO", "replication"]);
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("{field} missing in {info}"))
            .trim()
            .to_string()
    };

    let mut replica = TcpStream::connect(("127.0.0.1", port)).unwrap();
    replica
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    replica
        .write_all(&format_resp_cmd(&["PSYNC", "?", "-1"]))
        .unwrap();
    let mut buf = [0u8; 256];
    let n = replica.read(&mut buf).unwrap();
    let reply = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(reply.starts_with("+FULLRESYNC"), "{reply:?}");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while repl_field(&mut c, "connected_slaves") != "1" {
        assert!(
            std::time::Instant::now() < deadline,
            "replica never attached"
        );
        thread::sleep(Duration::from_millis(20));
    }

    // An ack for bytes the master never produced must not count.
    replica
        .write_all(&format_resp_cmd(&[
            "REPLCONF",
            "ACK",
            &u64::MAX.to_string(),
        ]))
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    assert_eq!(resp_cmd(&mut c, &["SET", "wait:k", "v"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["WAIT", "1", "500"]), ":0\r\n");

    // An honest ack of everything sent so far still does.
    let offset = repl_field(&mut c, "master_repl_offset");
    replica
        .write_all(&format_resp_cmd(&["REPLCONF", "ACK", &offset]))
        .unwrap();
    assert_eq!(resp_cmd(&mut c, &["WAIT", "1", "5000"]), ":1\r\n");

    drop(replica);
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_min_replicas_to_write_counts_good_replicas_and_info_lists_them_e2e() {
    let port: u16 = 17099;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "2", "--no-pin"];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    // Without replicas, min-slaves-to-write (the old name) blocks writes.
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "min-slaves-to-write", "1"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "min-replicas-to-write"]),
        "*2\r\n$21\r\nmin-replicas-to-write\r\n$1\r\n1\r\n"
    );
    assert!(resp_cmd(&mut c, &["SET", "mr:k", "v"]).starts_with("-NOREPLICAS"));
    assert_eq!(resp_cmd(&mut c, &["GET", "mr:k"]), "$-1\r\n");

    // A replica announces its port before PSYNC, as Redis replicas do.
    let mut replica = TcpStream::connect(("127.0.0.1", port)).unwrap();
    replica
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    assert_eq!(
        resp_cmd(&mut replica, &["REPLCONF", "listening-port", "7777"]),
        "+OK\r\n"
    );
    replica
        .write_all(&format_resp_cmd(&["PSYNC", "?", "-1"]))
        .unwrap();
    let mut buf = [0u8; 256];
    let n = replica.read(&mut buf).unwrap();
    assert!(buf[..n].starts_with(b"+FULLRESYNC"));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let info = resp_cmd(&mut c, &["INFO", "replication"]);
        if info.contains("slave0:ip=127.0.0.1,port=7777,state=online,") {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{info}");
        thread::sleep(Duration::from_millis(20));
    }

    // One fresh replica satisfies 1 but not 2.
    assert_eq!(resp_cmd(&mut c, &["SET", "mr:k", "v"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "min-replicas-to-write", "2"]),
        "+OK\r\n"
    );
    assert!(resp_cmd(&mut c, &["SET", "mr:k", "v2"]).starts_with("-NOREPLICAS"));
    assert!(
        resp_cmd(
            &mut c,
            &["EVAL", "return redis.call('set','mr:k','v3')", "0"]
        )
        .contains("NOREPLICAS")
    );

    // With max-lag 1, a replica whose last ack is 2+ seconds old is no
    // longer good (this fake replica never acks on its own).
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "min-replicas-to-write", "1"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "min-slaves-max-lag", "1"]),
        "+OK\r\n"
    );
    thread::sleep(Duration::from_millis(2500));
    assert!(resp_cmd(&mut c, &["SET", "mr:k", "v4"]).starts_with("-NOREPLICAS"));
    let offset = resp_cmd(&mut c, &["INFO", "replication"])
        .lines()
        .find_map(|l| {
            l.strip_prefix("master_repl_offset:")
                .map(|s| s.trim().to_string())
        })
        .unwrap();
    replica
        .write_all(&format_resp_cmd(&["REPLCONF", "ACK", &offset]))
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(resp_cmd(&mut c, &["SET", "mr:k", "v5"]), "+OK\r\n");

    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "min-replicas-to-write", "0"]),
        "+OK\r\n"
    );

    // The link shows as a replica client and CLIENT KILL TYPE replica
    // drops it.
    let list = resp_cmd(&mut c, &["CLIENT", "LIST"]);
    assert!(
        list.lines()
            .any(|l| l.contains("flags=S") && l.contains("cmd=psync")),
        "{list}"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CLIENT", "KILL", "TYPE", "replica"]),
        ":1\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !resp_cmd(&mut c, &["INFO", "replication"]).contains("connected_slaves:0") {
        assert!(
            std::time::Instant::now() < deadline,
            "replica link survived the kill"
        );
        thread::sleep(Duration::from_millis(20));
    }
    drop(replica);
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_replica_aof_holds_the_synced_dataset_after_a_restart_e2e() {
    let master_port: u16 = 17089;
    let replica_port: u16 = 17088;
    let dir = std::env::temp_dir().join(format!("rudis-syncaof-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mport = master_port.to_string();
    let rport = replica_port.to_string();
    let master_args = ["--port", &mport, "--threads", "2", "--no-pin"];
    let replica_args = [
        "--port",
        &rport,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];
    let wait_for = |c: &mut TcpStream, args: &[&str], want: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let got = resp_cmd(c, args);
            if got.contains(want) {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "{args:?}: {got:?}");
            thread::sleep(Duration::from_millis(50));
        }
    };

    let mut master = spawn_rudis_listening(&master_args, master_port);
    let mut m = TcpStream::connect(("127.0.0.1", master_port)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut replica = spawn_rudis_listening(&replica_args, replica_port);
    let mut r = TcpStream::connect(("127.0.0.1", replica_port)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    for i in 0..20 {
        let (mk, rk) = (format!("m:{i}"), format!("stale:{i}"));
        assert_eq!(resp_cmd(&mut m, &["SET", &mk, "1"]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut r, &["SET", &rk, "1"]), "+OK\r\n");
    }

    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &mport]),
        "+OK\r\n"
    );
    wait_for(&mut r, &["INFO", "replication"], "master_link_status:up");
    // A change streamed after the sync must land in the new AOF too.
    assert_eq!(resp_cmd(&mut m, &["SET", "m:after", "2"]), "+OK\r\n");
    wait_for(&mut r, &["GET", "m:after"], "$1\r\n2\r\n");
    assert_eq!(resp_cmd(&mut r, &["DBSIZE"]), ":21\r\n");
    drop(r);
    shutdown_and_wait(replica_port, &mut replica);
    drop(m);
    shutdown_and_wait(master_port, &mut master);

    // Restarted on its own, the replica comes back with the master's data.
    let mut replica = spawn_rudis_listening(&replica_args, replica_port);
    let mut r = TcpStream::connect(("127.0.0.1", replica_port)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(resp_cmd(&mut r, &["DBSIZE"]), ":21\r\n");
    for i in 0..20 {
        let (mk, rk) = (format!("m:{i}"), format!("stale:{i}"));
        assert_eq!(resp_cmd(&mut r, &["GET", &mk]), "$1\r\n1\r\n", "{mk}");
        assert_eq!(resp_cmd(&mut r, &["EXISTS", &rk]), ":0\r\n", "{rk}");
    }
    assert_eq!(resp_cmd(&mut r, &["GET", "m:after"]), "$1\r\n2\r\n");
    drop(r);
    shutdown_and_wait(replica_port, &mut replica);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_replica_replies_loading_while_it_loads_a_full_sync_e2e() {
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let port: u16 = 17087;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "2", "--no-pin"];
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = KillOnDrop(spawn_rudis_listening(&args, port));

    // Every key on one shard, which key-load-delay slows down: the other
    // shard finishes its part early and serves its clients meanwhile.
    let mut src = rudis::shard::ShardDb::new(0);
    for i in 0..20_000 {
        src.table.set(
            bytes::Bytes::from(format!("{{t}}:{i}")),
            bytes::Bytes::from_static(b"v"),
            None,
        );
    }
    let mut rdb = b"REDIS0011\xFE\x00".to_vec();
    src.save_rdb_chunk(&mut rdb).unwrap();
    rdb.push(0xFF);
    let crc = rudis::table::crc64(&rdb);
    rdb.extend_from_slice(&crc.to_le_bytes());
    drop(src);

    let master = TcpListener::bind("127.0.0.1:0").unwrap();
    let master_port = master.local_addr().unwrap().port();
    thread::spawn(move || {
        let Ok((mut conn, _)) = master.accept() else {
            return;
        };
        conn.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut got = Vec::new();
        let mut buf = [0u8; 4096];
        let mut read_to = |conn: &mut TcpStream, until: &[u8], crlfs: usize| {
            got.clear();
            loop {
                if let Some(p) = got.windows(until.len()).position(|w| w == until) {
                    let tail = &got[p + until.len()..];
                    if tail.windows(2).filter(|w| w == b"\r\n").count() >= crlfs {
                        return true;
                    }
                }
                match conn.read(&mut buf) {
                    Ok(n) if n > 0 => got.extend_from_slice(&buf[..n]),
                    _ => return false,
                }
            }
        };
        let ok = read_to(&mut conn, b"PING\r\n", 0)
            && conn.write_all(b"+PONG\r\n").is_ok()
            && read_to(&mut conn, b"listening-port\r\n", 2)
            && conn.write_all(b"+OK\r\n").is_ok()
            && read_to(&mut conn, b"psync2\r\n", 0)
            && conn.write_all(b"+OK\r\n").is_ok()
            && read_to(&mut conn, b"PSYNC\r\n", 4);
        if !ok {
            return;
        }
        let replid = "b".repeat(40);
        let mut reply = format!("+FULLRESYNC {replid} 0\r\n${}\r\n", rdb.len()).into_bytes();
        reply.extend_from_slice(&rdb);
        let _ = conn.write_all(&reply);
        // Hold the link open until the test is done (the replica's acks
        // arrive meanwhile).
        while matches!(conn.read(&mut buf), Ok(n) if n > 0) {}
    });

    // Clients PING (not allowed while loading, like in Redis) rather than
    // touch keys: a key command already queued on the loading shard would
    // just wait for it, while PING runs on the client's own shard thread.
    let loading_seen = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    // Enough clients that both shard threads have some.
    let clients: Vec<_> = (0..16)
        .map(|_| {
            let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
            let (loading_seen, stop) = (loading_seen.clone(), stop.clone());
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let r = resp_cmd(&mut c, &["PING"]);
                    if r.starts_with("-LOADING") {
                        assert_eq!(r, "-LOADING Redis is loading the dataset in memory\r\n");
                        loading_seen.fetch_add(1, Ordering::Relaxed);
                    } else {
                        assert_eq!(r, "+PONG\r\n");
                    }
                }
            })
        })
        .collect();

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "key-load-delay", "50"]),
        "+OK\r\n"
    );
    let mport = master_port.to_string();
    assert_eq!(
        resp_cmd(&mut c, &["REPLICAOF", "127.0.0.1", &mport]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !resp_cmd(&mut c, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "sync never finished");
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);
    for t in clients {
        t.join().unwrap();
    }
    assert!(loading_seen.load(Ordering::Relaxed) > 0, "no -LOADING seen");
    assert_eq!(resp_cmd(&mut c, &["GET", "{t}:19999"]), "$1\r\nv\r\n");
    assert_eq!(info_field(&mut c, "loading"), "0");
    assert_eq!(resp_cmd(&mut c, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child.0);
}

#[test]
fn test_redis_log_replication_messages_e2e() {
    let master_port = 18520u16;
    let replica_port = 18521u16;
    let base =
        std::env::temp_dir().join(format!("rudis-srv-{}-{}", std::process::id(), master_port));
    let _ = std::fs::remove_dir_all(&base);
    let start = |port: u16, extra: &str| {
        let dir = base.join(port.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("rudis.log");
        let conf = dir.join("rudis.conf");
        std::fs::write(
            &conf,
            format!(
                "dir {}\nsave \"\"\nlogfile {}\n{extra}",
                dir.display(),
                log.display()
            ),
        )
        .unwrap();
        let port_s = port.to_string();
        let args = [
            "--port",
            &port_s,
            "--threads",
            "2",
            "--no-pin",
            "-c",
            conf.to_str().unwrap(),
        ];
        (KillRudisOnDrop(spawn_rudis_listening(&args, port)), log)
    };
    let (master, master_log) = start(master_port, "");
    let mut m = TcpStream::connect(("127.0.0.1", master_port)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(resp_cmd(&mut m, &["SET", "k", "v"]), "+OK\r\n");

    let (replica, replica_log) = start(
        replica_port,
        &format!("replicaof 127.0.0.1 {master_port}\n"),
    );
    let text = wait_for_log(
        &replica_log,
        "MASTER <-> REPLICA sync: Finished with success",
    );
    for needle in [
        &format!(" * Connecting to MASTER 127.0.0.1:{master_port}\n"),
        " * MASTER <-> REPLICA sync started\n",
        " * Partial resynchronization not possible (no cached master)\n",
        " * Full resync from master: ",
        " * MASTER <-> REPLICA sync: Loading DB in memory\n",
    ] {
        assert!(text.contains(needle), "{needle:?} missing:\n{text}");
    }
    // Lines written while replicating show the replica role.
    let finished = text
        .lines()
        .find(|l| l.contains("Finished with success"))
        .unwrap();
    assert!(is_redis_log_line(finished, replica.0.id()), "{finished}");
    assert!(
        finished.split(' ').next().unwrap().ends_with(":S"),
        "{finished}"
    );

    let name = format!("127.0.0.1:{replica_port}");
    let text = wait_for_log(
        &master_log,
        &format!("Synchronization with replica {name} succeeded"),
    );
    assert!(
        text.contains(&format!(" * Replica {name} asks for synchronization\n")),
        "{text}"
    );
    assert!(
        text.contains(&format!(" * Full resync requested by replica {name}\n")),
        "{text}"
    );

    // Breaking the link: the replica reconnects with a partial resync.
    let mut r = TcpStream::connect(("127.0.0.1", replica_port)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(
        resp_cmd(
            &mut r,
            &["CLIENT", "KILL", &format!("127.0.0.1:{master_port}")]
        ),
        "+OK\r\n"
    );
    let text = wait_for_log(
        &replica_log,
        "Successful partial resynchronization with master.",
    );
    assert!(text.contains(" * Connection with master lost.\n"), "{text}");
    assert!(
        text.contains(" * Trying a partial resynchronization (request "),
        "{text}"
    );
    let text = wait_for_log(
        &master_log,
        &format!("Partial resynchronization request from {name} accepted."),
    );
    assert!(
        text.contains(&format!(" * Connection with replica {name} lost.\n")),
        "{text}"
    );

    // Promotion back to master shows in the role of later lines.
    assert_eq!(resp_cmd(&mut r, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    let text = wait_for_log(&replica_log, "MASTER MODE enabled");
    let line = text
        .lines()
        .find(|l| l.contains("MASTER MODE enabled"))
        .unwrap();
    assert!(line.split(' ').next().unwrap().ends_with(":M"), "{line}");

    drop((m, r, master, replica));
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn test_debug_digest_replica_matches_master_e2e() {
    let (mp, rp) = (18405, 18406);
    start_test_server(mp, 2);
    start_test_server(rp, 3);
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();

    load_digest_dataset(&mut m, false);
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    let wait_equal = |m: &mut TcpStream, r: &mut TcpStream| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (dm, dr) = (debug_digest(m), debug_digest(r));
            if dm == dr {
                return dm;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "replica digest {dr} never matched master {dm}"
            );
            thread::sleep(Duration::from_millis(100));
        }
    };
    let synced = wait_equal(&mut m, &mut r);
    assert_ne!(synced, "0".repeat(40));

    // Live stream after the full sync.
    resp_cmd_all(&mut m, &["HSET", "bighash", "new", "field"]);
    resp_cmd_all(&mut m, &["LPUSH", "list", "front"]);
    resp_cmd_all(&mut m, &["ZINCRBY", "zset", "2", "a"]);
    resp_cmd_all(&mut m, &["EXPIRE", "str:1", "1000"]);
    let live = wait_equal(&mut m, &mut r);
    assert_ne!(live, synced);

    // DEBUG REPLICATE sends a command to the replica without running it
    // here: the datasets diverge.
    assert_eq!(
        resp_cmd_all(
            &mut m,
            &["DEBUG", "REPLICATE", "SET", "only-on-replica", "1"]
        ),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd_all(&mut r, &["EXISTS", "only-on-replica"]) != ":1\r\n" {
        assert!(
            std::time::Instant::now() < deadline,
            "DEBUG REPLICATE not applied"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        resp_cmd_all(&mut m, &["EXISTS", "only-on-replica"]),
        ":0\r\n"
    );
    assert_ne!(debug_digest(&mut m), debug_digest(&mut r));
    resp_cmd_all(&mut r, &["REPLICAOF", "NO", "ONE"]);
}

/// Cross-shard LMOVE / SMOVE replicate as one logical command, from the
/// destination's shard right after the push. Other clients' writes
/// meanwhile must still reach the replica (the moves once muted the whole
/// stream while they ran), and later pushes to the destination must stay
/// after the move. The source's shard waits for the push, so a plain
/// write to the source in between (an RPUSH racing an RPOPLPUSH of the
/// same list) can't reach the replica on the other side of the move either.
/// Each move waits 300 us between its halves (DEBUG CROSS-SHARD-MOVE-DELAY)
/// to make those races likely.
#[test]
fn test_cross_shard_moves_replicate_without_losing_concurrent_writes_e2e() {
    let (mp, rp) = (18410, 18411);
    start_test_server(mp, 4);
    start_test_server(rp, 2);
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd_all(&mut m, &["WAIT", "1", "100"]) != ":1\r\n" {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
    }
    assert_eq!(
        resp_cmd_all(&mut m, &["DEBUG", "CROSS-SHARD-MOVE-DELAY", "300"]),
        "+OK\r\n"
    );
    // Keys on different shards, so the moves really cross shards.
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let handles: Vec<_> = (0..4)
        .map(|c| {
            let mut cmds = Vec::new();
            for i in 0..1500 {
                cmds.push(s(&["SET", &format!("k:{c}:{i}"), "v"]));
                if i % 5 == 0 {
                    cmds.push(s(&[
                        "RPUSH",
                        &format!("list:{}", c % 2),
                        &format!("{c}-{i}"),
                    ]));
                    cmds.push(s(&["SADD", "set:a", &format!("{c}-{i}")]));
                }
                if i % 7 == 0 {
                    cmds.push(s(&["LMOVE", "list:0", "list:1", "LEFT", "RIGHT"]));
                    cmds.push(s(&["RPOPLPUSH", "list:1", "list:0"]));
                    cmds.push(s(&[
                        "SMOVE",
                        "set:a",
                        "set:b",
                        &format!("{c}-{}", i / 5 * 5),
                    ]));
                }
            }
            thread::spawn(move || {
                let mut conn = TcpStream::connect(("127.0.0.1", mp)).unwrap();
                pipeline_all(&mut conn, &cmds, &format!("done-{c}"));
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    resp_cmd_all(&mut m, &["DEBUG", "CROSS-SHARD-MOVE-DELAY", "0"]);
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "1", "5000"]), ":1\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (dm, dr) = (debug_digest(&mut m), debug_digest(&mut r));
        if dm == dr {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "digests differ: {dm} {dr}"
        );
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        resp_cmd_all(&mut r, &["DBSIZE"]),
        resp_cmd_all(&mut m, &["DBSIZE"])
    );
    for key in ["list:0", "list:1"] {
        assert_eq!(
            resp_cmd_all(&mut r, &["LRANGE", key, "0", "-1"]),
            resp_cmd_all(&mut m, &["LRANGE", key, "0", "-1"])
        );
    }
    resp_cmd_all(&mut r, &["REPLICAOF", "NO", "ONE"]);
}

/// A full sync while clients do nothing but cross-shard LMOVEs, each
/// waiting 2 ms between its pop and its push (DEBUG
/// CROSS-SHARD-MOVE-DELAY), so one is nearly always half done. Each
/// shard's snapshot is
/// cut at its own moment, so without holding moves back a move could be
/// popped before the source's cut and pushed after the destination's,
/// then replicated (as one LMOVE) on top of a snapshot already missing
/// the element: the replica would pop a second one.
#[test]
fn test_full_sync_during_cross_shard_moves_matches_master_e2e() {
    let (mp, rp) = (18412, 18413);
    start_test_server(mp, 4);
    start_test_server(rp, 2);
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    assert_eq!(
        resp_cmd_all(&mut m, &["DEBUG", "CROSS-SHARD-MOVE-DELAY", "2000"]),
        "+OK\r\n"
    );
    // Eight list pairs: most pairs straddle shards.
    let pairs = 8;
    for p in 0..pairs {
        for side in ["a", "b"] {
            let key = format!("ring:{p}:{side}");
            let mut cmd = vec!["RPUSH".to_string(), key.clone()];
            cmd.extend((0..200).map(|i| format!("{side}{i}")));
            let args: Vec<&str> = cmd.iter().map(String::as_str).collect();
            assert_eq!(resp_cmd_all(&mut m, &args), ":200\r\n");
        }
    }
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let writers: Vec<_> = (0..4)
        .map(|c| {
            let mut cmds = Vec::new();
            for i in 0..400 {
                let p = (i + c) % pairs;
                let (from, to) = if c % 2 == 0 { ("a", "b") } else { ("b", "a") };
                cmds.push(s(&[
                    "LMOVE",
                    &format!("ring:{p}:{from}"),
                    &format!("ring:{p}:{to}"),
                    "LEFT",
                    "RIGHT",
                ]));
            }
            thread::spawn(move || {
                let mut conn = TcpStream::connect(("127.0.0.1", mp)).unwrap();
                pipeline_all(&mut conn, &cmds, &format!("moved-{c}"));
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    for w in writers {
        w.join().unwrap();
    }
    resp_cmd_all(&mut m, &["DEBUG", "CROSS-SHARD-MOVE-DELAY", "0"]);
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "1", "10000"]), ":1\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (dm, dr) = (debug_digest(&mut m), debug_digest(&mut r));
        if dm == dr {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let lens: Vec<String> = (0..pairs)
                .flat_map(|p| ["a", "b"].map(|side| format!("ring:{p}:{side}")))
                .map(|k| {
                    format!(
                        "{k} {}/{}",
                        resp_cmd_all(&mut m, &["LLEN", &k]).trim(),
                        resp_cmd_all(&mut r, &["LLEN", &k]).trim()
                    )
                })
                .collect();
            panic!("digests differ: master {dm} replica {dr}; LLEN master/replica: {lens:?}");
        }
        thread::sleep(Duration::from_millis(100));
    }
    resp_cmd_all(&mut r, &["REPLICAOF", "NO", "ONE"]);
}

/// Clients blocked in cross-shard BLMOVEs (tail pops) are served while
/// feeders RPUSH to the same lists' tails and a second replica full-syncs.
/// A served move runs as a locked LMOVE (the source's shard only wakes the
/// client), so replicas get each move in its place: a racing RPUSH stays on
/// its side, and a full sync never catches one half done. Each move waits
/// 300 us between its halves (DEBUG CROSS-SHARD-MOVE-DELAY).
#[test]
fn test_blocked_cross_shard_moves_replicate_in_place_e2e() {
    let (mp, r1, r2) = (18414u16, 18415u16, 18416u16);
    start_test_server(mp, 4);
    start_test_server(r1, 2);
    start_test_server(r2, 3);
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    let mut rep1 = TcpStream::connect(("127.0.0.1", r1)).unwrap();
    let mut rep2 = TcpStream::connect(("127.0.0.1", r2)).unwrap();
    assert_eq!(
        resp_cmd_all(&mut rep1, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd_all(&mut m, &["WAIT", "1", "100"]) != ":1\r\n" {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
    }
    assert_eq!(
        resp_cmd_all(&mut m, &["DEBUG", "CROSS-SHARD-MOVE-DELAY", "300"]),
        "+OK\r\n"
    );
    // Two source/destination pairs on different shards.
    let mut pairs = Vec::new();
    for i in 0.. {
        let (src, dst) = (format!("bsrc:{i}"), format!("bdst:{i}"));
        if rudis::router::target_shard(src.as_bytes(), 4)
            != rudis::router::target_shard(dst.as_bytes(), 4)
        {
            pairs.push((src, dst));
            if pairs.len() == 2 {
                break;
            }
        }
    }
    const MOVES: usize = 120;
    // Two movers per pair, blocked whenever the source is empty.
    let movers: Vec<_> = (0..4)
        .map(|c| {
            let (src, dst) = pairs[c % 2].clone();
            thread::spawn(move || {
                let mut conn = TcpStream::connect(("127.0.0.1", mp)).unwrap();
                for _ in 0..MOVES {
                    let r = resp_cmd_all(&mut conn, &["BLMOVE", &src, &dst, "RIGHT", "LEFT", "10"]);
                    assert!(r.starts_with('$') && r != "$-1\r\n", "{r}");
                }
            })
        })
        .collect();
    // One feeder per pair: as many pushes as its two movers take.
    let feeders: Vec<_> = (0..2)
        .map(|f| {
            let (src, _) = pairs[f].clone();
            thread::spawn(move || {
                let mut conn = TcpStream::connect(("127.0.0.1", mp)).unwrap();
                for i in 0..2 * MOVES {
                    resp_cmd_all(&mut conn, &["RPUSH", &src, &format!("{f}-{i}")]);
                    if i % 8 == 0 {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(30));
    assert_eq!(
        resp_cmd_all(&mut rep2, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    for t in movers.into_iter().chain(feeders) {
        t.join().unwrap();
    }
    resp_cmd_all(&mut m, &["DEBUG", "CROSS-SHARD-MOVE-DELAY", "0"]);
    for (src, dst) in &pairs {
        assert_eq!(resp_cmd_all(&mut m, &["LLEN", src]), ":0\r\n");
        assert_eq!(
            resp_cmd_all(&mut m, &["LLEN", dst]),
            format!(":{}\r\n", 2 * MOVES)
        );
    }
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "2", "10000"]), ":2\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let dm = debug_digest(&mut m);
        let (d1, d2) = (debug_digest(&mut rep1), debug_digest(&mut rep2));
        if dm == d1 && dm == d2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "digests differ: master {dm} replica1 {d1} replica2 {d2}"
        );
        thread::sleep(Duration::from_millis(100));
    }
    resp_cmd_all(&mut rep1, &["REPLICAOF", "NO", "ONE"]);
    resp_cmd_all(&mut rep2, &["REPLICAOF", "NO", "ONE"]);
}

/// A transaction reaches replicas as one MULTI ... EXEC block, which a
/// replica applies only once it has all of it. Promoted while the master
/// streams cross-shard `MULTI; INCR a; INCR b; EXEC`s, a replica must never
/// hold half of one (a != b), whatever read the stream was cut at.
#[test]
fn test_replica_never_keeps_half_a_transaction_e2e() {
    let mp = 18417u16;
    start_test_server(mp, 4);
    // Two keys on different shards.
    let (a, b) = (0..)
        .map(|i| (format!("txa:{i}"), format!("txb:{i}")))
        .find(|(a, b)| {
            rudis::router::target_shard(a.as_bytes(), 4)
                != rudis::router::target_shard(b.as_bytes(), 4)
        })
        .unwrap();
    for (attempt, rp) in (18418u16..18421).enumerate() {
        start_test_server(rp, 2);
        let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
        let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
        resp_cmd_all(&mut m, &["DEL", &a, &b]);
        assert_eq!(
            resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
            "+OK\r\n"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while resp_cmd_all(&mut m, &["WAIT", "1", "100"]) != ":1\r\n" {
            assert!(std::time::Instant::now() < deadline, "replica never linked");
        }
        let (wa, wb) = (a.clone(), b.clone());
        let writer = thread::spawn(move || {
            let mut conn = TcpStream::connect(("127.0.0.1", mp)).unwrap();
            let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
            let mut cmds = Vec::new();
            for _ in 0..3000 {
                cmds.push(s(&["MULTI"]));
                cmds.push(s(&["INCR", &wa]));
                cmds.push(s(&["INCR", &wb]));
                cmds.push(s(&["EXEC"]));
            }
            pipeline_all(&mut conn, &cmds, "txs-done");
        });
        // Promote the replica while the transactions stream in.
        thread::sleep(Duration::from_millis(20 + 15 * attempt as u64));
        assert_eq!(resp_cmd_all(&mut r, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
        writer.join().unwrap();
        let (ra, rb) = (
            resp_cmd_all(&mut r, &["GET", &a]),
            resp_cmd_all(&mut r, &["GET", &b]),
        );
        assert_eq!(ra, rb, "attempt {attempt}: replica kept half a transaction");
        assert_eq!(
            resp_cmd_all(&mut m, &["GET", &a]),
            resp_cmd_all(&mut m, &["GET", &b])
        );
    }
}

/// A replica reads its master's stream in big pieces and keeps applying
/// while earlier pieces are still in its shards; it hands its snapshot
/// lock over to a full sync only once they are applied. A sub-replica
/// full-syncing from it while the master streams pipelined writes over
/// every shard (and transactions) must end with the same data.
#[test]
fn test_sub_replica_full_sync_while_replica_applies_in_flight_e2e() {
    let (mp, rp, sp) = (18423u16, 18424u16, 18425u16);
    start_test_server(mp, 4);
    start_test_server(rp, 4);
    start_test_server(sp, 2);
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    let mut sub = TcpStream::connect(("127.0.0.1", sp)).unwrap();
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd_all(&mut m, &["WAIT", "1", "100"]) != ":1\r\n" {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
    }
    let writers: Vec<_> = (0..4)
        .map(|c| {
            thread::spawn(move || {
                let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
                let mut conn = TcpStream::connect(("127.0.0.1", mp)).unwrap();
                for round in 0..4 {
                    let mut cmds = Vec::new();
                    for i in 0..4000 {
                        cmds.push(s(&[
                            "SET",
                            &format!("w:{c}:{i}"),
                            &format!("{round}-{}", "x".repeat(200)),
                        ]));
                        cmds.push(s(&["INCR", &format!("n:{c}")]));
                        if i % 50 == 0 {
                            cmds.push(s(&["MULTI"]));
                            cmds.push(s(&["INCR", &format!("ta:{c}")]));
                            cmds.push(s(&["INCR", &format!("tb:{c}")]));
                            cmds.push(s(&["EXEC"]));
                        }
                    }
                    pipeline_all(&mut conn, &cmds, &format!("w-{c}-{round}"));
                }
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        resp_cmd_all(&mut sub, &["REPLICAOF", "127.0.0.1", &rp.to_string()]),
        "+OK\r\n"
    );
    for w in writers {
        w.join().unwrap();
    }
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "1", "10000"]), ":1\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (dm, dr, ds) = (
            debug_digest(&mut m),
            debug_digest(&mut r),
            debug_digest(&mut sub),
        );
        if dm == dr && dm == ds {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "digests differ: master {dm} replica {dr} sub-replica {ds}"
        );
        thread::sleep(Duration::from_millis(100));
    }
    for c in 0..4 {
        assert_eq!(
            resp_cmd_all(&mut sub, &["GET", &format!("n:{c}")]),
            "$5\r\n16000\r\n"
        );
        assert_eq!(
            resp_cmd_all(&mut sub, &["GET", &format!("ta:{c}")]),
            resp_cmd_all(&mut sub, &["GET", &format!("tb:{c}")])
        );
    }
    resp_cmd_all(&mut sub, &["REPLICAOF", "NO", "ONE"]);
    resp_cmd_all(&mut r, &["REPLICAOF", "NO", "ONE"]);
}

/// A replica whose master's stream is idle (pings off) still serves a
/// sub-replica's full sync at once: its stream loop must not keep its
/// snapshot lock while it waits for the next read.
#[test]
fn test_idle_replica_serves_a_sub_replica_full_sync_e2e() {
    let (mp, rp, sp) = (18426u16, 18427u16, 18428u16);
    start_test_server(mp, 2);
    start_test_server(rp, 2);
    start_test_server(sp, 2);
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    let mut sub = TcpStream::connect(("127.0.0.1", sp)).unwrap();
    assert_eq!(
        resp_cmd_all(
            &mut m,
            &["CONFIG", "SET", "repl-ping-replica-period", "3600"]
        ),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd_all(&mut m, &["WAIT", "1", "100"]) != ":1\r\n" {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
    }
    resp_cmd_all(&mut m, &["SET", "k", "v"]);
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "1", "5000"]), ":1\r\n");
    // The stream is idle now.
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        resp_cmd_all(&mut sub, &["REPLICAOF", "127.0.0.1", &rp.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while resp_cmd_all(&mut sub, &["GET", "k"]) != "$1\r\nv\r\n" {
        assert!(
            std::time::Instant::now() < deadline,
            "the sub-replica's full sync waited on the idle stream"
        );
        thread::sleep(Duration::from_millis(50));
    }
    resp_cmd_all(&mut sub, &["REPLICAOF", "NO", "ONE"]);
    resp_cmd_all(&mut r, &["REPLICAOF", "NO", "ONE"]);
}

/// A replica acks what its shards applied even when no more of the stream
/// comes (pings off): the last read's batches finish after the loop went
/// back to reading, and the periodic ack must still report them, or the
/// master sees the replica lagging (INFO, min-replicas-max-lag) until the
/// next write.
#[test]
fn test_idle_replica_acks_everything_it_applied_e2e() {
    let (mp, rp) = (18429u16, 18430u16);
    start_test_server(mp, 4);
    start_test_server(rp, 4);
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    assert_eq!(
        resp_cmd_all(
            &mut m,
            &["CONFIG", "SET", "repl-ping-replica-period", "3600"]
        ),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd_all(&mut m, &["WAIT", "1", "100"]) != ":1\r\n" {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
    }
    let acked = |m: &mut TcpStream| -> Option<u64> {
        let info = resp_cmd_all(m, &["INFO", "replication"]);
        let line = info.lines().find(|l| l.starts_with("slave0:"))?;
        line.split(',')
            .find_map(|kv| kv.strip_prefix("offset="))
            .and_then(|v| v.parse().ok())
    };
    for round in 0..5 {
        // No WAIT: its GETACK would make the replica drain and ack.
        pipelined_set_load(mp, 50_000);
        let target = info_field_u64(&mut m, "replication", "master_repl_offset");
        let deadline = std::time::Instant::now() + Duration::from_secs(4);
        while acked(&mut m) != Some(target) {
            assert!(
                std::time::Instant::now() < deadline,
                "round {round}: the replica acks {:?} of {target} on an idle stream",
                acked(&mut m)
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
    resp_cmd_all(&mut r, &["REPLICAOF", "NO", "ONE"]);
}

/// Shards hold their replication bytes for a mailbox burst and stream them
/// once. Pipelined writes from several clients over every shard (DB
/// switches, cross-shard LMOVE, a FLUSHALL right after remote writes, a
/// full sync started mid-stream) must still reach replicas in the master's
/// order, and WAIT must count them.
#[test]
fn test_batched_replication_stream_keeps_order_and_wait_counts_it_e2e() {
    let (mp, r1, r2) = (18407, 18408, 18409);
    start_test_server(mp, 4);
    start_test_server(r1, 2);
    start_test_server(r2, 3);
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    let mut rep1 = TcpStream::connect(("127.0.0.1", r1)).unwrap();
    let mut rep2 = TcpStream::connect(("127.0.0.1", r2)).unwrap();
    assert_eq!(
        resp_cmd_all(&mut rep1, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd_all(&mut m, &["WAIT", "1", "100"]) != ":1\r\n" {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
    }

    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    // Cross-shard writes too: LMOVE, MSET, MULTI/EXEC. A full sync holds
    // them back while it cuts each shard, so none is half in the snapshot.
    let writes = |c: usize, round: usize| {
        let mut cmds = Vec::new();
        // Round 1 runs long enough for the full sync to start inside it.
        let n = if round == 1 { 6000 } else { 1500 };
        for i in 0..n {
            cmds.push(s(&["SET", &format!("k:{c}:{i}"), &format!("v{round}-{i}")]));
            cmds.push(s(&["INCR", &format!("ctr:{c}")]));
            if i % 10 == 0 {
                cmds.push(s(&["SELECT", "1"]));
                cmds.push(s(&["SET", &format!("d1:{c}:{i}"), "x"]));
                cmds.push(s(&["INCR", &format!("d1ctr:{c}")]));
                cmds.push(s(&["SELECT", "0"]));
            }
            if i % 7 == 0 {
                cmds.push(s(&[
                    "RPUSH",
                    &format!("list:{}", c % 2),
                    &format!("{c}-{i}"),
                ]));
            }
            if i % 11 == 0 {
                let (a, b) = (format!("ma:{c}:{i}"), format!("mb:{c}:{i}"));
                cmds.push(s(&[
                    "MSET",
                    &a,
                    "1",
                    &b,
                    "2",
                    "mcount",
                    &format!("{c}-{i}"),
                ]));
            }
            if i % 17 == 0 {
                cmds.push(s(&["MULTI"]));
                cmds.push(s(&["INCR", "tx:a"]));
                cmds.push(s(&["INCR", "tx:b"]));
                cmds.push(s(&["RPUSH", "tx:log", &format!("{c}-{i}")]));
                cmds.push(s(&["EXEC"]));
            }
            if i % 13 == 0 {
                cmds.push(s(&["LMOVE", "list:0", "list:1", "LEFT", "RIGHT"]));
            }
        }
        cmds
    };
    let run_round = |round: usize| {
        let handles: Vec<_> = (0..4)
            .map(|c| {
                let cmds = writes(c, round);
                thread::spawn(move || {
                    let mut conn = TcpStream::connect(("127.0.0.1", mp)).unwrap();
                    let replies = pipeline_all(&mut conn, &cmds, &format!("done-{c}"));
                    assert!(!replies.starts_with(b"-"), "{:?}", &replies[..64]);
                })
            })
            .collect();
        handles
    };

    for h in run_round(0) {
        h.join().unwrap();
    }
    // A FLUSHALL right behind writes other shards still hold, then more.
    let mut cmds = vec![
        s(&["SET", "before-flush-a", "1"]),
        s(&["SET", "before-flush-b", "1"]),
        s(&["SELECT", "1"]),
        s(&["SET", "before-flush-db1", "1"]),
        s(&["SELECT", "0"]),
        s(&["FLUSHALL"]),
    ];
    for i in 0..200 {
        cmds.push(s(&["SET", &format!("after-flush:{i}"), "1"]));
    }
    // WAIT straight behind pipelined writes counts them.
    cmds.push(s(&["WAIT", "1", "5000"]));
    let replies = pipeline_all(&mut m, &cmds, "flushed");
    assert!(
        String::from_utf8_lossy(&replies).contains(":1\r\n$7\r\nflushed"),
        "{}",
        String::from_utf8_lossy(&replies[replies.len().saturating_sub(64)..])
    );
    assert_eq!(
        resp_cmd_all(&mut rep1, &["EXISTS", "after-flush:199"]),
        ":1\r\n"
    );
    assert_eq!(
        resp_cmd_all(&mut rep1, &["EXISTS", "before-flush-a"]),
        ":0\r\n"
    );

    // A second replica full-syncs while four clients write.
    let writers = run_round(1);
    thread::sleep(Duration::from_millis(20));
    assert_eq!(
        resp_cmd_all(&mut rep2, &["REPLICAOF", "127.0.0.1", &mp.to_string()]),
        "+OK\r\n"
    );
    for h in writers {
        h.join().unwrap();
    }
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "2", "10000"]), ":2\r\n");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let dm = debug_digest(&mut m);
        let (d1, d2) = (debug_digest(&mut rep1), debug_digest(&mut rep2));
        if dm == d1 && dm == d2 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let mut report = String::new();
            for (name, r) in [("replica1", &mut rep1), ("replica2", &mut rep2)] {
                for db in ["0", "1"] {
                    resp_cmd_all(&mut m, &["SELECT", db]);
                    resp_cmd_all(r, &["SELECT", db]);
                    let mk = resp_scalars(&resp_cmd_all(&mut m, &["KEYS", "*"]));
                    let rk = resp_scalars(&resp_cmd_all(r, &["KEYS", "*"]));
                    let missing: Vec<_> = mk.iter().filter(|k| !rk.contains(k)).take(5).collect();
                    let extra: Vec<_> = rk.iter().filter(|k| !mk.contains(k)).take(5).collect();
                    let mut diff = Vec::new();
                    for k in &mk {
                        let t = resp_cmd_all(&mut m, &["TYPE", k]);
                        let q: &[&str] = if t.contains("list") {
                            &["LRANGE", k, "0", "-1"]
                        } else {
                            &["GET", k]
                        };
                        let mut args = q.to_vec();
                        args[1] = k;
                        if resp_cmd_all(&mut m, &args) != resp_cmd_all(r, &args) {
                            diff.push(k.clone());
                        }
                    }
                    report += &format!(
                        "{name} db{db}: master {} keys, replica {}; missing {missing:?} extra {extra:?} differ {:?}\n",
                        mk.len(),
                        rk.len(),
                        &diff[..diff.len().min(8)]
                    );
                }
            }
            panic!("digests differ: master {dm} replica1 {d1} replica2 {d2}\n{report}");
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(resp_cmd_all(&mut rep1, &["GET", "ctr:3"]), "$4\r\n6000\r\n");
    resp_cmd_all(&mut rep1, &["REPLICAOF", "NO", "ONE"]);
    resp_cmd_all(&mut rep2, &["REPLICAOF", "NO", "ONE"]);
}

/// Consumer-group writes reach a replica, and the AOF, the way Redis
/// propagates them (XREADGROUP/XAUTOCLAIM as XCLAIM + XGROUP SETID /
/// CREATECONSUMER, approximate trims as exact ones), so both end with the
/// master's groups, PELs, delivery counts and times.
#[test]
fn test_stream_consumer_groups_replicate_and_persist_e2e() {
    let (mport, rport) = (18301u16, 18302u16);
    let (ms, rs) = (mport.to_string(), rport.to_string());
    let dir = std::env::temp_dir().join(format!("rudis-stream-repl-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("master.conf");
    std::fs::write(&conf, format!("dir {}\nsave \"\"\n", dir.display())).unwrap();
    let rdir = dir.join("replica");
    std::fs::create_dir_all(&rdir).unwrap();
    let rconf = dir.join("replica.conf");
    std::fs::write(&rconf, format!("dir {}\nsave \"\"\n", rdir.display())).unwrap();
    let aof_dir = dir.join("aof");
    let master_args = [
        "--port",
        &ms,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        aof_dir.to_str().unwrap(),
        "-c",
        conf.to_str().unwrap(),
    ];
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut master = KillOnDrop(spawn_rudis_listening(&master_args, mport));
    let _replica = KillOnDrop(spawn_rudis_listening(
        &[
            "--port",
            &rs,
            "--threads",
            "3",
            "--no-pin",
            "-c",
            rconf.to_str().unwrap(),
        ],
        rport,
    ));
    let connect = |port: u16| {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c
    };
    let mut m = connect(mport);
    let mut r = connect(rport);
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !resp_cmd_all(&mut r, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never synced");
        thread::sleep(Duration::from_millis(50));
    }

    let keys = ["s1", "s2", "s3", "s4"];
    let ok = |c: &mut TcpStream, args: &[&str]| {
        let reply = resp_cmd_all(c, args);
        assert!(!reply.starts_with('-'), "{args:?}: {reply}");
        reply
    };
    // Waits for the replica to apply everything so far, then compares.
    let check = |m: &mut TcpStream, r: &mut TcpStream, step: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while debug_digest(m) != debug_digest(r) && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(50));
        }
        // The digest leaves delivery counts and times out: compare those.
        thread::sleep(Duration::from_millis(100));
        for k in keys {
            assert_eq!(
                stream_repl_state(m, k),
                stream_repl_state(r, k),
                "after {step}: {k} differs on the replica"
            );
        }
        assert_eq!(debug_digest(m), debug_digest(r), "after {step}");
    };

    for i in 1..=10 {
        ok(
            &mut m,
            &["XADD", "s1", &format!("{i}-0"), "f", &i.to_string()],
        );
    }
    ok(&mut m, &["XGROUP", "CREATE", "s1", "g", "0"]);
    check(&mut m, &mut r, "XGROUP CREATE");

    // XREADGROUP: a new consumer reading new entries.
    let reply = ok(
        &mut m,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "alice",
            "COUNT",
            "3",
            "STREAMS",
            "s1",
            ">",
        ],
    );
    assert!(reply.contains("3-0"), "{reply}");
    check(&mut m, &mut r, "XREADGROUP >");
    // NOACK: no PEL, but the consumer and the group's last id move.
    ok(
        &mut m,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "bob",
            "NOACK",
            "COUNT",
            "2",
            "STREAMS",
            "s1",
            ">",
        ],
    );
    check(&mut m, &mut r, "XREADGROUP NOACK");
    // History reads bump delivery counts and times.
    ok(
        &mut m,
        &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s1", "0"],
    );
    ok(
        &mut m,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "alice",
            "COUNT",
            "1",
            "STREAMS",
            "s1",
            "0",
        ],
    );
    check(&mut m, &mut r, "XREADGROUP history");
    // A consumer created by a read that delivers nothing.
    ok(
        &mut m,
        &["XREADGROUP", "GROUP", "g", "carol", "STREAMS", "s1", "0"],
    );
    check(&mut m, &mut r, "XREADGROUP empty history");

    // XCLAIM, with each option the propagation uses.
    ok(&mut m, &["XCLAIM", "s1", "g", "dave", "0", "1-0", "2-0"]);
    ok(
        &mut m,
        &[
            "XCLAIM", "s1", "g", "dave", "0", "3-0", "JUSTID", "LASTID", "6-0",
        ],
    );
    ok(
        &mut m,
        &[
            "XCLAIM",
            "s1",
            "g",
            "erin",
            "0",
            "9-0",
            "FORCE",
            "RETRYCOUNT",
            "5",
            "IDLE",
            "1000",
        ],
    );
    ok(
        &mut m,
        &[
            "XCLAIM",
            "s1",
            "g",
            "erin",
            "0",
            "1-0",
            "TIME",
            "1700000000000",
        ],
    );
    check(&mut m, &mut r, "XCLAIM");
    // XAUTOCLAIM claims and drops the PEL entry of a deleted entry.
    ok(&mut m, &["XDEL", "s1", "2-0"]);
    let reply = ok(
        &mut m,
        &["XAUTOCLAIM", "s1", "g", "frank", "0", "0-0", "COUNT", "10"],
    );
    assert!(reply.contains("2-0"), "{reply}");
    check(&mut m, &mut r, "XAUTOCLAIM");
    ok(&mut m, &["XACK", "s1", "g", "1-0"]);
    check(&mut m, &mut r, "XACK");
    ok(&mut m, &["XACKDEL", "s1", "g", "IDS", "1", "3-0"]);
    check(&mut m, &mut r, "XACKDEL");
    ok(&mut m, &["XDELEX", "s1", "DELREF", "IDS", "1", "9-0"]);
    check(&mut m, &mut r, "XDELEX");
    assert_eq!(
        ok(&mut m, &["XGROUP", "CREATECONSUMER", "s1", "g", "gina"]),
        ":1\r\n"
    );
    ok(&mut m, &["XGROUP", "DELCONSUMER", "s1", "g", "carol"]);
    check(&mut m, &mut r, "XGROUP CREATECONSUMER/DELCONSUMER");
    // Moving the group back re-delivers pending entries to a new owner.
    ok(
        &mut m,
        &["XGROUP", "SETID", "s1", "g", "0", "ENTRIESREAD", "0"],
    );
    check(&mut m, &mut r, "XGROUP SETID");
    ok(
        &mut m,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "alice",
            "COUNT",
            "4",
            "STREAMS",
            "s1",
            ">",
        ],
    );
    check(&mut m, &mut r, "XREADGROUP after SETID");

    // Approximate trims: the master trims whole nodes, the replica must
    // trim the very same entries.
    for i in 1..=250 {
        ok(&mut m, &["XADD", "s2", &format!("{i}-0"), "f", "v"]);
    }
    ok(&mut m, &["XGROUP", "CREATE", "s2", "g2", "0"]);
    ok(
        &mut m,
        &[
            "XREADGROUP",
            "GROUP",
            "g2",
            "c",
            "COUNT",
            "120",
            "STREAMS",
            "s2",
            ">",
        ],
    );
    ok(&mut m, &["XADD", "s2", "MAXLEN", "~", "100", "*", "f", "v"]);
    assert_eq!(ok(&mut m, &["XLEN", "s2"]), ":151\r\n");
    check(&mut m, &mut r, "XADD MAXLEN ~");
    ok(
        &mut m,
        &["XADD", "s2", "MINID", "~", "240-0", "*", "f", "v"],
    );
    check(&mut m, &mut r, "XADD MINID ~");
    ok(&mut m, &["XTRIM", "s2", "MAXLEN", "~", "10"]);
    check(&mut m, &mut r, "XTRIM MAXLEN ~");
    for _ in 0..150 {
        ok(&mut m, &["XADD", "s2", "*", "f", "v"]);
    }
    ok(
        &mut m,
        &[
            "XTRIM",
            "s2",
            "MINID",
            "~",
            "999999999999999-0",
            "LIMIT",
            "100",
        ],
    );
    check(&mut m, &mut r, "XTRIM MINID ~ LIMIT");
    ok(
        &mut m,
        &[
            "XADD", "s2", "ACKED", "MAXLEN", "~", "5", "LIMIT", "3", "*", "f", "v",
        ],
    );
    check(&mut m, &mut r, "XADD ACKED MAXLEN ~ LIMIT");

    // Blocked XREADGROUPs served by a later XADD.
    ok(&mut m, &["XGROUP", "CREATE", "s3", "g3", "$", "MKSTREAM"]);
    let waiters: Vec<_> = [("hank", false), ("ivan", true)]
        .into_iter()
        .map(|(consumer, noack)| {
            thread::spawn(move || {
                let mut c = TcpStream::connect(("127.0.0.1", mport)).unwrap();
                c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                let mut args = vec!["XREADGROUP", "GROUP", "g3", consumer, "BLOCK", "5000"];
                if noack {
                    args.push("NOACK");
                }
                args.extend(["COUNT", "1", "STREAMS", "s3", ">"]);
                resp_cmd_all(&mut c, &args)
            })
        })
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !resp_cmd_all(&mut m, &["INFO", "clients"]).contains("blocked_clients:2") {
        assert!(
            std::time::Instant::now() < deadline,
            "readers never blocked"
        );
        thread::sleep(Duration::from_millis(20));
    }
    ok(&mut m, &["XADD", "s3", "1-0", "f", "v"]);
    ok(&mut m, &["XADD", "s3", "2-0", "f", "v"]);
    for w in waiters {
        let reply = w.join().unwrap();
        assert!(reply.contains("-0"), "{reply}");
    }
    check(&mut m, &mut r, "blocked XREADGROUP");
    // One XREADGROUP over streams of different shards.
    ok(&mut m, &["XGROUP", "CREATE", "s2", "g", "0"]);
    ok(
        &mut m,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "zed",
            "COUNT",
            "2",
            "STREAMS",
            "s1",
            "s2",
            ">",
            ">",
        ],
    );
    check(&mut m, &mut r, "multi-stream XREADGROUP");

    // IDMP and its configuration.
    ok(&mut m, &["XADD", "s4", "*", "f", "v"]);
    ok(
        &mut m,
        &[
            "XCFGSET",
            "s4",
            "IDMP-DURATION",
            "600",
            "IDMP-MAXSIZE",
            "10",
        ],
    );
    ok(&mut m, &["XADD", "s4", "IDMP", "p1", "i1", "*", "f", "v"]);
    ok(
        &mut m,
        &[
            "XADD", "s4", "IDMPAUTO", "p2", "MAXLEN", "~", "1", "*", "f", "w",
        ],
    );
    check(&mut m, &mut r, "XADD IDMP / XCFGSET");
    assert_eq!(info_field(&mut m, "unpropagated_changes"), "0");

    // An AOF restart rebuilds the same groups, PELs, counts and times.
    let before: Vec<Vec<String>> = keys.iter().map(|k| stream_repl_state(&mut m, k)).collect();
    let digest = debug_digest(&mut m);
    drop(m);
    shutdown_and_wait(mport, &mut master.0);
    for e in std::fs::read_dir(&dir).unwrap().flatten() {
        if e.path().extension().is_some_and(|x| x == "rdb") {
            std::fs::remove_file(e.path()).unwrap();
        }
    }
    master = KillOnDrop(spawn_rudis_listening(&master_args, mport));
    let mut m = connect(mport);
    let after: Vec<Vec<String>> = keys.iter().map(|k| stream_repl_state(&mut m, k)).collect();
    assert_eq!(before, after, "AOF replay differs");
    assert_eq!(debug_digest(&mut m), digest);
    drop(m);
    shutdown_and_wait(mport, &mut master.0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// FUNCTION LOAD/DELETE, SORT ... STORE and ZADD score updates reach a
/// replica and survive an AOF restart (they used to stay on the master).
#[test]
fn test_functions_sort_store_and_zadd_updates_replicate_and_persist_e2e() {
    let (mport, rport) = (17109u16, 17110u16);
    let dir = std::env::temp_dir().join(format!("rudis-fnrepl-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap();
    let (mport_s, rport_s) = (mport.to_string(), rport.to_string());
    let margs = [
        "--port",
        &mport_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir_s,
    ];
    let mut master = spawn_rudis_listening(&margs, mport);
    let mut replica =
        spawn_rudis_listening(&["--port", &rport_s, "--threads", "2", "--no-pin"], rport);
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &mport_s]),
        "+OK\r\n"
    );
    let offset = |c: &mut TcpStream, field: &str| -> String {
        resp_cmd(c, &["INFO", "replication"])
            .lines()
            .find_map(|l| {
                l.strip_prefix(&format!("{field}:"))
                    .map(|v| v.trim().to_string())
            })
            .unwrap_or_default()
    };
    let synced = |m: &mut TcpStream, r: &mut TcpStream| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let link_up = resp_cmd(r, &["INFO", "replication"]).contains("master_link_status:up");
            if link_up && offset(r, "slave_repl_offset") == offset(m, "master_repl_offset") {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "replica never caught up"
            );
            thread::sleep(Duration::from_millis(20));
        }
    };
    synced(&mut m, &mut r);

    let lib = "#!lua name=replib\nredis.register_function('rf', function(keys, args) return 1 end)";
    assert_eq!(
        resp_cmd(&mut m, &["FUNCTION", "LOAD", lib]),
        "$6\r\nreplib\r\n"
    );
    assert_eq!(resp_cmd(&mut m, &["RPUSH", "srt", "3", "1", "2"]), ":3\r\n");
    assert_eq!(
        resp_cmd(&mut m, &["SORT", "srt", "STORE", "srt:out"]),
        ":3\r\n"
    );
    // A score update adds nothing (reply 0) but is a change.
    assert_eq!(resp_cmd(&mut m, &["ZADD", "zup", "1", "a"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut m, &["ZADD", "zup", "2", "a"]), ":0\r\n");
    assert_eq!(
        resp_cmd(
            &mut m,
            &[
                "GEOADD", "geo", "13.36", "38.11", "pa", "15.08", "37.5", "ca"
            ]
        ),
        ":2\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut m,
            &[
                "GEORADIUS",
                "geo",
                "15",
                "37",
                "200",
                "km",
                "STORE",
                "geo:st"
            ]
        ),
        ":2\r\n"
    );
    synced(&mut m, &mut r);
    assert_eq!(
        resp_cmd(&mut r, &["ZRANGE", "geo:st", "0", "-1"]),
        resp_cmd(&mut m, &["ZRANGE", "geo:st", "0", "-1"])
    );
    assert_eq!(resp_cmd(&mut r, &["ZSCORE", "zup", "a"]), "$1\r\n2\r\n");
    assert!(resp_cmd(&mut r, &["FUNCTION", "LIST"]).contains("replib"));
    let sorted = "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n";
    assert_eq!(resp_cmd(&mut r, &["LRANGE", "srt:out", "0", "-1"]), sorted);

    assert_eq!(
        resp_cmd(&mut m, &["FUNCTION", "DELETE", "replib"]),
        "+OK\r\n"
    );
    synced(&mut m, &mut r);
    assert_eq!(resp_cmd(&mut r, &["FUNCTION", "LIST"]), "*0\r\n");

    // From the AOF alone (no RDB written): the library and the stored list.
    assert_eq!(
        resp_cmd(&mut m, &["FUNCTION", "LOAD", lib]),
        "$6\r\nreplib\r\n"
    );
    drop(r);
    shutdown_and_wait(rport, &mut replica);
    let _ = m.write_all(&format_resp_cmd(&["SHUTDOWN", "NOSAVE"]));
    drop(m);
    let _ = master.wait();
    let mut master = spawn_rudis_listening(&margs, mport);
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert!(resp_cmd(&mut m, &["FUNCTION", "LIST"]).contains("replib"));
    assert_eq!(resp_cmd(&mut m, &["LRANGE", "srt:out", "0", "-1"]), sorted);
    assert_eq!(resp_cmd(&mut m, &["ZSCORE", "zup", "a"]), "$1\r\n2\r\n");
    drop(m);
    shutdown_and_wait(mport, &mut master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A client blocked in BLPOP on a master that becomes a replica gets
/// UNBLOCKED, as in Redis; served later from the replication stream, its pop
/// would make the replica diverge from its master.
#[test]
fn test_replicaof_unblocks_clients_blocked_on_writes_e2e() {
    let (aport, bport) = (17111u16, 17112u16);
    let (as_, bs) = (aport.to_string(), bport.to_string());
    let mut a = spawn_rudis_listening(&["--port", &as_, "--threads", "2", "--no-pin"], aport);
    let mut b = spawn_rudis_listening(&["--port", &bs, "--threads", "2", "--no-pin"], bport);
    let mut blocked = TcpStream::connect(("127.0.0.1", aport)).unwrap();
    blocked
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    blocked
        .write_all(&format_resp_cmd(&["BLPOP", "ub:list", "0"]))
        .unwrap();
    let mut c = TcpStream::connect(("127.0.0.1", aport)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !resp_cmd(&mut c, &["INFO", "clients"]).contains("blocked_clients:1") {
        assert!(std::time::Instant::now() < deadline, "client never blocked");
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        resp_cmd(&mut c, &["REPLICAOF", "127.0.0.1", &bs]),
        "+OK\r\n"
    );
    let mut buf = [0u8; 256];
    let n = blocked.read(&mut buf).unwrap();
    let reply = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(reply.starts_with("-UNBLOCKED"), "{reply:?}");
    assert_eq!(resp_cmd(&mut c, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    drop((blocked, c));
    shutdown_and_wait(aport, &mut a);
    shutdown_and_wait(bport, &mut b);
}

#[test]
fn test_multi_db_persistence_and_replication_e2e() {
    let (mport, rport) = (17114u16, 17115u16);
    let dir = std::env::temp_dir().join(format!("rudis-multidb-repl-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mdir = dir.join("m");
    let rdir = dir.join("r");
    std::fs::create_dir_all(&mdir).unwrap();
    std::fs::create_dir_all(&rdir).unwrap();
    let (ms, rs) = (mport.to_string(), rport.to_string());
    let (mds, rds) = (
        mdir.to_str().unwrap().to_string(),
        rdir.to_str().unwrap().to_string(),
    );
    let margs = [
        "--port",
        &ms,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        &mds,
        "--appendonly",
        "yes",
    ];
    let rargs = ["--port", &rs, "--threads", "2", "--no-pin", "--dir", &rds];
    let mut master = spawn_rudis_listening(&margs, mport);
    let mut replica = spawn_rudis_listening(&rargs, rport);

    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    // Populate DB 0 and DB 2 before attaching replica (tests full-sync RDB multi-DB)
    assert_eq!(resp_cmd(&mut m, &["SET", "k0", "val0"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut m, &["SELECT", "2"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut m, &["SET", "k2", "val2"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut m, &["HSET", "h2", "f", "v"]), ":1\r\n");

    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !resp_cmd(&mut r, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica did not sync");
        thread::sleep(Duration::from_millis(20));
    }

    // Stream live multi-DB mutations: SELECT 5, SET, MOVE, COPY ... DB, SWAPDB, FLUSHDB
    assert_eq!(resp_cmd(&mut m, &["SELECT", "5"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut m, &["SET", "k5", "val5"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut m, &["MOVE", "k5", "4"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut m, &["SELECT", "4"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut m, &["COPY", "k5", "k5_copy", "DB", "2"]),
        ":1\r\n"
    );
    assert_eq!(resp_cmd(&mut m, &["SWAPDB", "2", "3"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut m, &["WAIT", "1", "5000"]), ":1\r\n");

    let m_digest = resp_cmd(&mut m, &["DEBUG", "DIGEST"]);
    let r_digest = resp_cmd(&mut r, &["DEBUG", "DIGEST"]);
    assert_eq!(
        m_digest, r_digest,
        "master/replica multi-DB digest mismatch"
    );

    assert_eq!(resp_cmd(&mut r, &["SELECT", "0"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut r, &["GET", "k0"]), "$4\r\nval0\r\n");
    assert_eq!(resp_cmd(&mut r, &["SELECT", "3"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut r, &["GET", "k2"]), "$4\r\nval2\r\n");
    assert_eq!(resp_cmd(&mut r, &["GET", "k5_copy"]), "$4\r\nval5\r\n");
    assert_eq!(resp_cmd(&mut r, &["SELECT", "4"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut r, &["GET", "k5"]), "$4\r\nval5\r\n");

    // Rewrite AOF on master, restart master without saving RDB, and verify AOF replay restores all DBs
    assert!(resp_cmd(&mut m, &["BGREWRITEAOF"]).starts_with("+"));
    drop(r);
    shutdown_and_wait(rport, &mut replica);
    let _ = m.write_all(&format_resp_cmd(&["SHUTDOWN", "NOSAVE"]));
    drop(m);
    let _ = master.wait();
    let _ = std::fs::remove_file(mdir.join("dump.rdb"));

    let mut master = spawn_rudis_listening(&margs, mport);
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(resp_cmd(&mut m, &["DEBUG", "DIGEST"]), m_digest);
    drop(m);
    shutdown_and_wait(mport, &mut master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Keys evicted under `maxmemory` must be propagated like Redis does (a
/// `DEL` in the AOF and the replication stream, in the key's DB): they used
/// to be dropped silently, so the replica kept every evicted key and an AOF
/// replay resurrected them.
#[test]
fn test_evicted_keys_propagate_to_replica_and_aof_e2e() {
    let (mport, rport) = (17950u16, 17951u16);
    let dir = std::env::temp_dir().join(format!("rudis-evict-prop-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mdir = dir.join("m");
    let rdir = dir.join("r");
    std::fs::create_dir_all(&mdir).unwrap();
    std::fs::create_dir_all(&rdir).unwrap();
    let (ms, rs) = (mport.to_string(), rport.to_string());
    let (mds, rds) = (
        mdir.to_str().unwrap().to_string(),
        rdir.to_str().unwrap().to_string(),
    );
    let margs = [
        "--port",
        &ms,
        "--threads",
        "2",
        "--no-pin",
        "--bind",
        "127.0.0.1",
        "--dir",
        &mds,
        "--appendonly",
        "yes",
    ];
    let rargs = [
        "--port",
        &rs,
        "--threads",
        "2",
        "--no-pin",
        "--bind",
        "127.0.0.1",
        "--dir",
        &rds,
    ];
    let mut master = spawn_rudis_listening(&margs, mport);
    let mut replica = spawn_rudis_listening(&rargs, rport);
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(30))).unwrap();

    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    wait_for("replica link up", || {
        resp_cmd(&mut r, &["INFO", "replication"]).contains("master_link_status:up")
    });

    const LIMIT: u64 = 24 << 20;
    assert_eq!(
        resp_cmd(&mut m, &["CONFIG", "SET", "maxmemory", &LIMIT.to_string()]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut m,
            &["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]
        ),
        "+OK\r\n"
    );

    // Far more than fits, in DB 0 and DB 2, so both DBs see evictions.
    for (db, n) in [("0", 400_000usize), ("2", 200_000)] {
        let mut c = TcpStream::connect(("127.0.0.1", mport)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        assert_eq!(resp_cmd(&mut c, &["SELECT", db]), "+OK\r\n");
        let mut writer = c.try_clone().unwrap();
        let w = thread::spawn(move || {
            let val = "v".repeat(64);
            let mut chunk = Vec::with_capacity(1 << 20);
            for i in 0..n {
                let key = format!("db{db}:{i:08}");
                chunk.extend_from_slice(
                    format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$64\r\n{}\r\n",
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
        let mut lines = 0usize;
        let mut buf = vec![0u8; 1 << 16];
        let mut oom = false;
        while lines < n {
            let got = c.read(&mut buf).expect("load reply");
            assert!(got > 0, "server closed the load connection");
            lines += buf[..got].iter().filter(|&&b| b == b'\n').count();
            oom |= buf[..got].windows(4).any(|w| w == b"-OOM");
        }
        w.join().unwrap();
        assert!(!oom, "allkeys-lru rejected writes in db {db}");
    }

    // Stop evicting (CONFIG SET skips the maxmemory check) so the keyspace
    // holds still while it is compared.
    assert_eq!(
        resp_cmd(&mut m, &["CONFIG", "SET", "maxmemory", "0"]),
        "+OK\r\n"
    );
    let evicted = info_field_u64(&mut m, "stats", "evicted_keys");
    assert!(evicted > 0, "nothing was evicted");
    let dbsize = |c: &mut TcpStream, db: &str| -> String {
        assert_eq!(resp_cmd(c, &["SELECT", db]), "+OK\r\n");
        resp_cmd(c, &["DBSIZE"])
    };
    let (m0, m2) = (dbsize(&mut m, "0"), dbsize(&mut m, "2"));
    assert_ne!(m0, ":400000\r\n", "db 0 saw no eviction");
    assert_ne!(m2, ":200000\r\n", "db 2 saw no eviction");
    assert_eq!(resp_cmd(&mut m, &["WAIT", "1", "10000"]), ":1\r\n");
    wait_for("replica keyspace matches the master's", || {
        dbsize(&mut r, "0") == m0 && dbsize(&mut r, "2") == m2
    });
    let m_digest = resp_cmd(&mut m, &["DEBUG", "DIGEST"]);
    assert_eq!(
        resp_cmd(&mut r, &["DEBUG", "DIGEST"]),
        m_digest,
        "replica kept keys the master evicted"
    );

    // Replaying the AOF alone (no RDB) yields the same keyspace.
    drop(r);
    shutdown_and_wait(rport, &mut replica);
    let _ = m.write_all(&format_resp_cmd(&["SHUTDOWN", "NOSAVE"]));
    drop(m);
    let _ = master.wait();
    let _ = std::fs::remove_file(mdir.join("dump.rdb"));
    let mut master = spawn_rudis_listening(&margs, mport);
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    wait_for("AOF loaded", || {
        !resp_cmd(&mut m, &["PING"]).starts_with("-LOADING")
    });
    assert_eq!(dbsize(&mut m, "0"), m0, "AOF replay resurrected db 0 keys");
    assert_eq!(dbsize(&mut m, "2"), m2, "AOF replay resurrected db 2 keys");
    assert_eq!(resp_cmd(&mut m, &["DEBUG", "DIGEST"]), m_digest);
    drop(m);
    shutdown_and_wait(mport, &mut master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Like Redis's default `replica-ignore-maxmemory yes`, a replica over its
/// own `maxmemory` must not evict: it used to, dropping keys its master
/// still had. `replica-ignore-maxmemory no` makes it enforce the limit.
#[test]
fn test_replica_ignores_maxmemory_by_default_e2e() {
    let (mport, rport) = (17952u16, 17953u16);
    let (ms, rs) = (mport.to_string(), rport.to_string());
    let margs = [
        "--port",
        &ms,
        "--threads",
        "2",
        "--no-pin",
        "--bind",
        "127.0.0.1",
    ];
    let rargs = [
        "--port",
        &rs,
        "--threads",
        "2",
        "--no-pin",
        "--bind",
        "127.0.0.1",
    ];
    let mut master = spawn_rudis_listening(&margs, mport);
    let mut replica = spawn_rudis_listening(&rargs, rport);
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    assert_eq!(
        resp_cmd(&mut r, &["CONFIG", "GET", "replica-ignore-maxmemory"]),
        "*2\r\n$24\r\nreplica-ignore-maxmemory\r\n$3\r\nyes\r\n"
    );
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    wait_for("replica link up", || {
        resp_cmd(&mut r, &["INFO", "replication"]).contains("master_link_status:up")
    });
    for i in 0..500 {
        let k = format!("k{i}");
        assert_eq!(resp_cmd(&mut m, &["SET", &k, "value"]), "+OK\r\n");
    }
    assert_eq!(resp_cmd(&mut m, &["WAIT", "1", "5000"]), ":1\r\n");

    // Any dataset is over a 1-byte limit.
    assert_eq!(
        resp_cmd(&mut r, &["CONFIG", "SET", "maxmemory", "1"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut r,
            &["CONFIG", "SET", "maxmemory-policy", "allkeys-random"]
        ),
        "+OK\r\n"
    );
    for i in 0..20 {
        let k = format!("k{i}");
        assert_eq!(resp_cmd(&mut r, &["GET", &k]), "$5\r\nvalue\r\n");
    }
    assert_eq!(resp_cmd(&mut r, &["DBSIZE"]), ":500\r\n");
    assert_eq!(info_field_u64(&mut r, "stats", "evicted_keys"), 0);

    // Opting out makes the replica enforce its own limit.
    assert_eq!(
        resp_cmd(&mut r, &["CONFIG", "SET", "slave-ignore-maxmemory", "no"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut r, &["CONFIG", "GET", "replica-ignore-maxmemory"]),
        "*2\r\n$24\r\nreplica-ignore-maxmemory\r\n$2\r\nno\r\n"
    );
    resp_cmd(&mut r, &["GET", "k0"]);
    assert!(info_field_u64(&mut r, "stats", "evicted_keys") > 0);
    assert!(
        resp_cmd(
            &mut r,
            &["CONFIG", "SET", "replica-ignore-maxmemory", "maybe"]
        )
        .starts_with("-ERR")
    );

    drop(r);
    shutdown_and_wait(rport, &mut replica);
    drop(m);
    shutdown_and_wait(mport, &mut master);
}

/// PSYNC2 on a chain master -> replica -> sub-replica: the replica proxies
/// its master's stream, so all three share one replication id and offset;
/// promoting the middle node and re-pointing the old master make partial
/// resyncs only, like Redis.
#[test]
fn test_psync2_chained_replication_and_failover_e2e() {
    let (pa, pb, pc) = (18240u16, 18241u16, 18242u16);
    for p in [pa, pb, pc] {
        start_test_server(p, 2);
    }
    let (mut a, mut b, mut c) = (psync2_client(pa), psync2_client(pb), psync2_client(pc));

    assert_eq!(resp_cmd(&mut a, &["SET", "before", "1"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut b, &["REPLICAOF", "127.0.0.1", &pa.to_string()]),
        "+OK\r\n"
    );
    wait_for("B link up", || {
        info_str(&mut b, "replication", "master_link_status") == "up"
    });
    assert_eq!(
        resp_cmd(&mut c, &["REPLICAOF", "127.0.0.1", &pb.to_string()]),
        "+OK\r\n"
    );
    wait_for("C link up", || {
        info_str(&mut c, "replication", "master_link_status") == "up"
    });
    assert_eq!(info_field_u64(&mut b, "replication", "connected_slaves"), 1);

    // Writes on the master reach the sub-replica through the replica.
    for i in 0..100 {
        resp_cmd(&mut a, &["INCR", "x"]);
        resp_cmd(&mut a, &["RPUSH", "l", &i.to_string()]);
    }
    resp_cmd(&mut a, &["HSET", "h", "f1", "v1", "f2", "v2"]);
    resp_cmd(&mut a, &["ZADD", "z", "1", "a", "2", "b"]);
    resp_cmd(&mut a, &["XADD", "s", "*", "f", "v"]);
    resp_cmd(&mut a, &["XGROUP", "CREATE", "s", "g", "0"]);
    resp_cmd(&mut a, &["SET", "after", "1"]);
    wait_for("x on C", || {
        resp_cmd(&mut c, &["GET", "x"]) == "$3\r\n100\r\n"
    });
    let off = |s: &mut TcpStream| info_field_u64(s, "replication", "master_repl_offset");
    wait_for("offsets equal", || {
        let o = off(&mut a);
        o == off(&mut b) && o == off(&mut c)
    });
    let replid_a = info_str(&mut a, "replication", "master_replid");
    assert_eq!(info_str(&mut b, "replication", "master_replid"), replid_a);
    assert_eq!(info_str(&mut c, "replication", "master_replid"), replid_a);
    let digest = debug_digest(&mut a);
    assert_eq!(debug_digest(&mut b), digest);
    assert_eq!(debug_digest(&mut c), digest);

    // Like Redis, writes made on a writable replica are not propagated.
    assert_eq!(
        resp_cmd(&mut b, &["CONFIG", "SET", "replica-read-only", "no"]),
        "+OK\r\n"
    );
    let off_before_local = off(&mut b);
    if resp_cmd(&mut b, &["SET", "local", "1"]) == "+OK\r\n" {
        assert_eq!(off(&mut b), off_before_local);
        resp_cmd(&mut a, &["SET", "marker", "1"]);
        wait_for("marker on C", || {
            resp_cmd(&mut c, &["GET", "marker"]) == "$1\r\n1\r\n"
        });
        assert_eq!(resp_cmd(&mut c, &["GET", "local"]), "$-1\r\n");
        resp_cmd(&mut b, &["DEL", "local"]);
    }
    resp_cmd(&mut b, &["CONFIG", "SET", "replica-read-only", "yes"]);
    wait_for("offsets equal again", || {
        let o = off(&mut a);
        o == off(&mut b) && o == off(&mut c)
    });

    // Promote B: it keeps A's history as replid2 up to its offset, and C
    // (dropped to learn the new id) continues with a partial resync.
    let full_before = info_field_u64(&mut a, "stats", "sync_full");
    let partial_before = info_field_u64(&mut a, "stats", "sync_partial_ok");
    let offset = off(&mut a);
    assert_eq!(resp_cmd(&mut b, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    let new_replid = info_str(&mut b, "replication", "master_replid");
    assert_ne!(new_replid, replid_a);
    assert_eq!(info_str(&mut b, "replication", "master_replid2"), replid_a);
    assert_eq!(
        info_field_u64(&mut b, "replication", "second_repl_offset"),
        offset + 1
    );
    assert_eq!(off(&mut b), offset);
    wait_for("C learns B's new id", || {
        info_str(&mut c, "replication", "master_replid") == new_replid
            && info_str(&mut c, "replication", "master_link_status") == "up"
    });
    assert_eq!(info_str(&mut c, "replication", "master_replid2"), replid_a);
    assert_eq!(
        info_field_u64(&mut c, "replication", "second_repl_offset"),
        offset + 1
    );

    // The old master rejoins under B with its own history: partial too.
    assert_eq!(
        resp_cmd(&mut a, &["REPLICAOF", "127.0.0.1", &pb.to_string()]),
        "+OK\r\n"
    );
    wait_for("A link up", || {
        info_str(&mut a, "replication", "master_link_status") == "up"
    });
    assert_eq!(info_str(&mut a, "replication", "master_replid"), new_replid);
    assert_eq!(info_field_u64(&mut b, "replication", "connected_slaves"), 2);
    // Stats are process-wide: C's and A's resyncs were both partial.
    assert_eq!(info_field_u64(&mut a, "stats", "sync_full"), full_before);
    assert!(info_field_u64(&mut a, "stats", "sync_partial_ok") >= partial_before + 2);

    for _ in 0..10 {
        resp_cmd(&mut b, &["INCR", "x"]);
    }
    wait_for("x on A and C", || {
        resp_cmd(&mut a, &["GET", "x"]) == "$3\r\n110\r\n"
            && resp_cmd(&mut c, &["GET", "x"]) == "$3\r\n110\r\n"
    });
    wait_for("offsets equal after failover", || {
        let o = off(&mut b);
        o == off(&mut a) && o == off(&mut c)
    });
    let digest = debug_digest(&mut b);
    assert_eq!(debug_digest(&mut a), digest);
    assert_eq!(debug_digest(&mut c), digest);
    // ROLE on the sub-replica shows the shared offset.
    let role = resp_cmd(&mut c, &["ROLE"]);
    assert!(
        role.contains("connected") && role.contains(&format!(":{}\r\n", off(&mut b))),
        "{role}"
    );
}

/// A replica whose own master link is down refuses (P)SYNC like Redis,
/// and SYNC on a sub-replica gets the master's commands in order.
#[test]
fn test_psync2_sub_replica_sync_stream_and_nomasterlink_e2e() {
    let (pa, pb) = (18245u16, 18246u16);
    start_test_server(pa, 2);
    start_test_server(pb, 2);
    let (mut a, mut b) = (psync2_client(pa), psync2_client(pb));
    // Pointing at a dead master: no link, so no SYNC.
    assert_eq!(
        resp_cmd(&mut b, &["REPLICAOF", "127.0.0.1", "1"]),
        "+OK\r\n"
    );
    let mut sub = psync2_client(pb);
    let r = resp_cmd(&mut sub, &["PSYNC", "?", "-1"]);
    assert!(r.starts_with("-NOMASTERLINK"), "{r:?}");

    assert_eq!(
        resp_cmd(&mut b, &["REPLICAOF", "127.0.0.1", &pa.to_string()]),
        "+OK\r\n"
    );
    wait_for("B link up", || {
        info_str(&mut b, "replication", "master_link_status") == "up"
    });
    let mut sub = psync2_client(pb);
    sub.write_all(b"SYNC\r\n").unwrap();
    // Skip the RDB: `$<len>\r\n<payload>`.
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    let rdb_end = loop {
        let n = sub.read(&mut buf).unwrap();
        assert!(n > 0);
        got.extend_from_slice(&buf[..n]);
        let skip = got.iter().take_while(|&&c| c == b'\n').count();
        if let Some(pos) = got[skip..].windows(2).position(|w| w == b"\r\n") {
            let len: usize = std::str::from_utf8(&got[skip + 1..skip + pos])
                .unwrap()
                .parse()
                .unwrap();
            let end = skip + pos + 2 + len;
            if got.len() >= end {
                break end;
            }
        }
    };
    let mut stream = got.split_off(rdb_end);
    resp_cmd(&mut a, &["SET", "k1", "v1"]);
    resp_cmd(&mut a, &["FLUSHALL"]);
    resp_cmd(&mut a, &["SET", "k2", "v2"]);
    let want: &[&[u8]] = &[b"k1", b"FLUSHALL", b"k2"];
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let text = String::from_utf8_lossy(&stream).to_string();
        let pos: Vec<Option<usize>> = want
            .iter()
            .map(|w| text.find(std::str::from_utf8(w).unwrap()))
            .collect();
        if pos.iter().all(Option::is_some) {
            let p: Vec<usize> = pos.into_iter().map(Option::unwrap).collect();
            assert!(p[0] < p[1] && p[1] < p[2], "{text:?}");
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{text:?}");
        sub.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        if let Ok(n) = sub.read(&mut buf) {
            stream.extend_from_slice(&buf[..n]);
        }
    }
}

/// Search indexes (FT.CREATE / FT.ALTER / FT.DROPINDEX) survive restarts
/// from the AOF (before and after BGREWRITEAOF) and from the RDB (SAVE and
/// DEBUG RELOAD), and reach replicas by full sync and by the stream. The
/// documents are ordinary hashes spread over several shards; each shard
/// re-indexes its own after the load.
#[test]
fn test_search_indexes_persist_and_replicate_e2e() {
    let (port, rport) = (17181u16, 17182u16);
    let dir = std::env::temp_dir().join(format!("rudis-ft-persist-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (adir, rdir, repdir) = (dir.join("aof"), dir.join("rdb"), dir.join("replica"));
    for d in [&adir, &rdir, &repdir] {
        std::fs::create_dir_all(d).unwrap();
    }
    let ps = port.to_string();
    let rps = rport.to_string();
    let docs: Vec<String> = (0..12).map(|i| format!("ftp:{i:02}")).collect();
    let expect_all = |c: &mut TcpStream, what: &str| {
        assert_eq!(
            ft_search_ids(c, "idx:ftp", "hello"),
            (docs.len() as i64, docs.clone()),
            "{what}"
        );
        // The field added by FT.ALTER is indexed too.
        assert_eq!(
            ft_search_ids(c, "idx:ftp", "@n:[5 7]"),
            (3, vec!["ftp:05".into(), "ftp:06".into(), "ftp:07".into()]),
            "{what}"
        );
        let list = resp_cmd_all(c, &["FT._LIST"]);
        assert!(
            list.contains("idx:ftp") && !list.contains("idx:gone"),
            "{what}: {list:?}"
        );
    };
    let populate = |c: &mut TcpStream| {
        // Half the documents before FT.CREATE (backfilled), half after.
        for d in &docs[..6] {
            let n = d[4..].trim_start_matches('0').to_string();
            let n = if n.is_empty() { "0".to_string() } else { n };
            assert_eq!(
                resp_cmd(c, &["HSET", d, "title", "hello", "n", &n]),
                ":2\r\n"
            );
        }
        assert_eq!(
            resp_cmd(
                c,
                &[
                    "FT.CREATE",
                    "idx:ftp",
                    "ON",
                    "HASH",
                    "PREFIX",
                    "1",
                    "ftp:",
                    "SCHEMA",
                    "title",
                    "TEXT"
                ]
            ),
            "+OK\r\n"
        );
        for d in &docs[6..] {
            let n = d[4..].trim_start_matches('0').to_string();
            assert_eq!(
                resp_cmd(c, &["HSET", d, "title", "hello", "n", &n]),
                ":2\r\n"
            );
        }
        assert_eq!(
            resp_cmd(c, &["FT.ALTER", "idx:ftp", "SCHEMA", "ADD", "n", "NUMERIC"]),
            "+OK\r\n"
        );
        assert_eq!(
            resp_cmd(
                c,
                &[
                    "FT.CREATE",
                    "idx:gone",
                    "ON",
                    "HASH",
                    "SCHEMA",
                    "title",
                    "TEXT"
                ]
            ),
            "+OK\r\n"
        );
        assert_eq!(resp_cmd(c, &["FT.DROPINDEX", "idx:gone"]), "+OK\r\n");
    };

    // 1. AOF: restart replays every shard's AOF, then again after a rewrite.
    let aargs = [
        "--port",
        &ps,
        "--threads",
        "3",
        "--no-pin",
        "--enable-experimental-commands",
        "yes",
        "--dir",
        adir.to_str().unwrap(),
        "--appendonly",
        "yes",
    ];
    let mut child = spawn_rudis_listening(&aargs, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    populate(&mut c);
    expect_all(&mut c, "before restart");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_file(adir.join("dump.rdb"));
    let mut child = spawn_rudis_listening(&aargs, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    expect_all(&mut c, "after AOF replay");
    assert!(resp_cmd(&mut c, &["BGREWRITEAOF"]).starts_with('+'));
    wait_aof_rewrite_done(&mut c);
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_file(adir.join("dump.rdb"));
    let mut child = spawn_rudis_listening(&aargs, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    expect_all(&mut c, "after rewritten AOF replay");
    drop(c);
    shutdown_and_wait(port, &mut child);

    // 2. RDB: SAVE + restart, DEBUG RELOAD, and full sync + stream to a replica.
    let rargs = [
        "--port",
        &ps,
        "--threads",
        "3",
        "--no-pin",
        "--enable-experimental-commands",
        "yes",
        "--dir",
        rdir.to_str().unwrap(),
    ];
    let mut child = spawn_rudis_listening(&rargs, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    populate(&mut c);
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    drop(c);
    let _ = TcpStream::connect(("127.0.0.1", port))
        .unwrap()
        .write_all(&format_resp_cmd(&["SHUTDOWN", "NOSAVE"]));
    let _ = child.wait();
    let mut child = spawn_rudis_listening(&rargs, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    expect_all(&mut c, "after RDB load");
    assert_eq!(resp_cmd_all(&mut c, &["DEBUG", "RELOAD"]), "+OK\r\n");
    expect_all(&mut c, "after DEBUG RELOAD");

    let repargs = [
        "--port",
        &rps,
        "--threads",
        "2",
        "--no-pin",
        "--enable-experimental-commands",
        "yes",
        "--dir",
        repdir.to_str().unwrap(),
    ];
    let mut replica = spawn_rudis_listening(&repargs, rport);
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &ps]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !resp_cmd_all(&mut r, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica did not sync");
        thread::sleep(Duration::from_millis(20));
    }
    expect_all(&mut r, "replica after full sync");
    // Live: a new index, an altered one and a dropped one over the stream.
    assert_eq!(
        resp_cmd(
            &mut c,
            &[
                "FT.CREATE",
                "idx:live",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "ftp:",
                "SCHEMA",
                "n",
                "NUMERIC"
            ]
        ),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["HSET", "ftp:99", "n", "99"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["FT.DROPINDEX", "idx:ftp"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["WAIT", "1", "5000"]), ":1\r\n");
    let list = resp_cmd_all(&mut r, &["FT._LIST"]);
    assert!(
        list.contains("idx:live") && !list.contains("idx:ftp"),
        "{list:?}"
    );
    assert_eq!(
        ft_search_ids(&mut r, "idx:live", "@n:[90 100]"),
        (1, vec!["ftp:99".to_string()])
    );
    assert_eq!(ft_search_ids(&mut r, "idx:live", "@n:[0 100]").0, 13);

    drop(r);
    shutdown_and_wait(rport, &mut replica);
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_appendfsync_always_writes_before_replying_e2e() {
    let port = 17969u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-fsync-always-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        "4",
        "--no-pin",
        "--dir",
        &ds,
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
        "--save",
        "",
    ];
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c
    };
    let aof_bytes = || -> Vec<u8> {
        (0..4)
            .flat_map(|i| {
                std::fs::read(dir.join(format!("appendonly-{i}.aof"))).unwrap_or_default()
            })
            .collect()
    };
    let contains =
        |hay: &[u8], needle: &str| hay.windows(needle.len()).any(|w| w == needle.as_bytes());

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = connect();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "appendfsync"]),
        "*2\r\n$11\r\nappendfsync\r\n$6\r\nalways\r\n"
    );
    // Every write is in its shard's AOF file by the time +OK arrives,
    // whichever shard owns the key (the connection's or another).
    for i in 0..40 {
        let (k, v) = (format!("dur:{i}"), format!("val-{i}-x"));
        assert_eq!(resp_cmd(&mut c, &["SET", &k, &v]), "+OK\r\n");
        assert!(
            contains(&aof_bytes(), &v),
            "{k} not in the AOF when SET replied"
        );
    }
    // A pipeline: one fsync round for the batch, all of it on disk.
    let mut pipe = Vec::new();
    for i in 0..50 {
        pipe.extend_from_slice(&format_resp_cmd(&[
            "SET",
            &format!("p:{i}"),
            &format!("pv-{i}-y"),
        ]));
    }
    c.write_all(&pipe).unwrap();
    let mut got = Vec::new();
    while got.windows(5).filter(|w| *w == b"+OK\r\n").count() < 50 {
        let mut b = [0u8; 4096];
        let n = c.read(&mut b).unwrap();
        got.extend_from_slice(&b[..n]);
    }
    let bytes = aof_bytes();
    assert!((0..50).all(|i| contains(&bytes, &format!("pv-{i}-y"))));
    // Switchable at runtime, like Redis.
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "appendfsync", "everysec"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "appendfsync"]),
        "*2\r\n$11\r\nappendfsync\r\n$8\r\neverysec\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "appendfsync", "always"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["SET", "last", "last-z"]), "+OK\r\n");
    assert!(contains(&aof_bytes(), "last-z"));
    drop(c);
    // A crash right after the replies loses nothing.
    child.kill().unwrap();
    child.wait().unwrap();
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = connect();
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":91\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "dur:39"]), "$8\r\nval-39-x\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_replica_announce_and_priority_e2e() {
    let m_port = 17972;
    let r_port = 17973;
    let m_port_s = m_port.to_string();
    let r_port_s = r_port.to_string();
    let mut master =
        spawn_rudis_listening(&["--port", &m_port_s, "--threads", "2", "--no-pin"], m_port);
    let mut replica =
        spawn_rudis_listening(&["--port", &r_port_s, "--threads", "2", "--no-pin"], r_port);

    let mut mc = TcpStream::connect(("127.0.0.1", m_port)).unwrap();
    mc.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut rc = TcpStream::connect(("127.0.0.1", r_port)).unwrap();
    rc.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Configure announce-ip, announce-port, and replica-priority on the replica before syncing
    assert_eq!(
        resp_cmd(&mut rc, &["CONFIG", "SET", "replica-priority", "25"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut rc,
            &["CONFIG", "SET", "replica-announce-ip", "192.0.2.99"]
        ),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut rc,
            &["CONFIG", "SET", "replica-announce-port", "36379"]
        ),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut rc, &["CONFIG", "GET", "slave-priority"]),
        "*2\r\n$14\r\nslave-priority\r\n$2\r\n25\r\n"
    );
    assert_eq!(
        resp_cmd(&mut rc, &["CONFIG", "GET", "replica-announce-ip"]),
        "*2\r\n$19\r\nreplica-announce-ip\r\n$10\r\n192.0.2.99\r\n"
    );
    assert_eq!(
        resp_cmd(&mut rc, &["CONFIG", "GET", "replica-announce-port"]),
        "*2\r\n$21\r\nreplica-announce-port\r\n$5\r\n36379\r\n"
    );

    // Reject duplicate alias in single CONFIG SET and invalid integer
    assert!(
        resp_cmd(
            &mut rc,
            &[
                "CONFIG",
                "SET",
                "replica-priority",
                "10",
                "slave-priority",
                "20"
            ]
        )
        .starts_with("-ERR")
    );
    assert!(resp_cmd(&mut rc, &["CONFIG", "SET", "replica-priority", "bad"]).starts_with("-ERR"));
    assert!(
        resp_cmd(
            &mut rc,
            &["CONFIG", "SET", "replica-announce-port", "99999"]
        )
        .starts_with("-ERR")
    );

    // Start replication
    assert_eq!(
        resp_cmd(&mut rc, &["REPLICAOF", "127.0.0.1", &m_port_s]),
        "+OK\r\n"
    );

    // Wait for replica link to come up
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let info = resp_cmd(&mut rc, &["INFO", "replication"]);
        if info.contains("master_link_status:up\r\n") {
            assert!(info.contains("slave_priority:25\r\n"), "{info}");
            assert!(
                !info.contains("master_last_io_seconds_ago:-1\r\n"),
                "{info}"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica did not sync in time: {info}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    // Master INFO replication and ROLE should report the announced IP and port
    let m_info = resp_cmd(&mut mc, &["INFO", "replication"]);
    assert!(
        m_info.contains("slave0:ip=192.0.2.99,port=36379,state=online"),
        "{m_info}"
    );
    let m_role = resp_cmd(&mut mc, &["ROLE"]);
    assert!(
        m_role.contains("$10\r\n192.0.2.99\r\n$5\r\n36379\r\n"),
        "{m_role}"
    );

    drop(rc);
    drop(mc);
    shutdown_and_wait(r_port, &mut replica);
    shutdown_and_wait(m_port, &mut master);
}

/// WAIT asks replicas for an ack (REPLCONF GETACK) instead of waiting for
/// their once-a-second report: every WAIT took ~1 s, now a round trip.
#[test]
fn test_wait_returns_promptly_with_getack_e2e() {
    let (mp, rp) = (18022u16, 18023u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let master = ServerGuard(spawn_rudis_listening(
        &["--port", &mps, "--threads", "2", "--no-pin"],
        mp,
    ));
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut worst = Duration::ZERO;
    for i in 0..10 {
        assert_eq!(
            send_and_read_reply(&mut m, format!("SET w{i} v\r\n").as_bytes()),
            "+OK\r\n"
        );
        let t = std::time::Instant::now();
        assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 5000\r\n"), ":1\r\n");
        worst = worst.max(t.elapsed());
    }
    assert!(worst < Duration::from_millis(300), "WAIT took {worst:?}");
    // The replica really has the write WAIT confirmed.
    assert_eq!(send_and_read_reply(&mut r, b"GET w9\r\n"), "$1\r\nv\r\n");
    drop(replica);
    drop(master);
}

/// A replica does not expire keys itself (no active expiry, and writes from
/// its master see expired keys as live), like Redis: it waits for the
/// master's DEL. Expiring on its own, it lost a key the master PERSISTed
/// just after its TTL ran out, and the two diverged.
#[test]
fn test_replica_waits_for_master_to_expire_keys_e2e() {
    let (mp, rp) = (18024u16, 18025u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let master = ServerGuard(spawn_rudis_listening(
        &["--port", &mps, "--threads", "2", "--no-pin"],
        mp,
    ));
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // The master keeps the key past its TTL (as if its expiry had not run
    // yet) and then PERSISTs it.
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG SET-ACTIVE-EXPIRE 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG SET-ALLOW-ACCESS-EXPIRED 1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"SET k v PX 300\r\n"),
        "+OK\r\n"
    );
    thread::sleep(Duration::from_millis(800));
    assert_eq!(send_and_read_reply(&mut m, b"PERSIST k\r\n"), ":1\r\n");
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    assert_eq!(send_and_read_reply(&mut r, b"GET k\r\n"), "$1\r\nv\r\n");
    assert_eq!(send_and_read_reply(&mut r, b"TTL k\r\n"), ":-1\r\n");
    drop(replica);
    drop(master);
}

/// A key that expires on the master is logged as DEL (AOF and replicas)
/// before the command that found it expired: an APPEND to an expired key
/// gives a fresh "tail" on the master, its replica and after a restart from
/// the AOF. Expiries were not logged, so the replica and the replayed AOF
/// (which do not expire keys themselves) appended to the old value.
#[test]
fn test_expiry_is_logged_as_del_for_replica_and_aof_e2e() {
    let (mp, rp) = (18026u16, 18027u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let dir = std::env::temp_dir().join(format!("rudis-e2e-expdel-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let margs = [
        "--port",
        &mps,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        &ds,
        "--appendonly",
        "yes",
        "--appendfsync",
        "everysec",
    ];
    let master = ServerGuard(spawn_rudis_listening(&margs, mp));
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Active expiry off, so the APPEND is what finds the key expired.
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG SET-ACTIVE-EXPIRE 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"SET k v PX 100\r\n"),
        "+OK\r\n"
    );
    thread::sleep(Duration::from_millis(300));
    assert_eq!(send_and_read_reply(&mut m, b"APPEND k tail\r\n"), ":4\r\n");
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    assert_eq!(send_and_read_reply(&mut r, b"GET k\r\n"), "$4\r\ntail\r\n");
    drop(replica);
    // Restart the master from its AOF (kill -9: the AOF is written before
    // replies).
    drop(master);
    let master = ServerGuard(spawn_rudis_listening(&margs, mp));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(send_and_read_reply(&mut m, b"GET k\r\n"), "$4\r\ntail\r\n");
    drop(master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A replica attaching to a master at `maxmemory` does not make it evict:
/// the full-sync snapshot (built in-process, about the dataset's size held
/// more than once) does not count toward the limit, as in Redis. It made
/// the master evict essentially its whole dataset.
#[test]
fn test_full_sync_does_not_make_master_evict_e2e() {
    let (mp, rp) = (18028u16, 18029u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let master = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &mps,
            "--threads",
            "2",
            "--no-pin",
            "--maxmemory",
            "40mb",
            "--maxmemory-policy",
            "allkeys-lru",
        ],
        mp,
    ));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let val = "v".repeat(1000);
    for b in 0..34 {
        let mut req = String::new();
        for i in 0..1000 {
            let k = format!("k{b}:{i}");
            req.push_str(&format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1000\r\n{val}\r\n",
                k.len()
            ));
        }
        m.write_all(req.as_bytes()).unwrap();
        let (mut oks, mut buf) = (0, [0u8; 8192]);
        while oks < 1000 {
            let n = m.read(&mut buf).unwrap();
            assert!(n > 0);
            oks += buf[..n].windows(3).filter(|w| w == b"OK\r").count();
        }
    }
    let evicted = |m: &mut TcpStream| -> u64 {
        send_and_read_reply(m, b"INFO stats\r\n")
            .lines()
            .find_map(|l| l.strip_prefix("evicted_keys:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let keys_before = send_and_read_reply(&mut m, b"DBSIZE\r\n");
    let ev_before = evicted(&mut m);
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Writes keep coming while the replica syncs. They overwrite the same
    // 100 keys: new keys grew the dataset itself past maxmemory when the
    // sync was slow (a busy host), and those evictions were legitimate.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut j = 0;
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        for _ in 0..20 {
            send_and_read_reply(&mut m, format!("SET w{} x\r\n", j % 100).as_bytes());
            j += 1;
        }
    }
    let diag = format!(
        "writes during sync: {j}\n{}\n{}",
        send_and_read_reply(&mut m, b"INFO replication\r\n"),
        send_and_read_reply(&mut m, b"INFO memory\r\n")
    );
    assert_eq!(
        evicted(&mut m),
        ev_before,
        "evicted during the full sync\n{diag}"
    );
    let keys_after: u64 = send_and_read_reply(&mut m, b"DBSIZE\r\n")[1..]
        .trim()
        .parse()
        .unwrap();
    let before: u64 = keys_before[1..].trim().parse().unwrap();
    assert!(
        keys_after >= before,
        "{before} keys before, {keys_after} after"
    );
    drop(replica);
    drop(master);
}

/// A read-only replica hides an expired key from reads but keeps it until
/// its master's DEL, like Redis: a PERSIST the master applied just before
/// the deadline then still finds it. The replica deleted it on read, so the
/// PERSIST was lost and the replica diverged. Pipelined writes to the
/// replica are refused like single ones.
#[test]
fn test_replica_hides_expired_keys_until_master_del_e2e() {
    let (mp, rp) = (18031u16, 18032u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let master = ServerGuard(spawn_rudis_listening(
        &["--port", &mps, "--threads", "2", "--no-pin"],
        mp,
    ));
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    // Let the replica's cron see its role.
    thread::sleep(Duration::from_millis(300));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG SET-ACTIVE-EXPIRE 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG SET-ALLOW-ACCESS-EXPIRED 1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"SET k v PX 300\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    thread::sleep(Duration::from_millis(500));
    // Expired: hidden from reads on the replica...
    assert_eq!(send_and_read_reply(&mut r, b"GET k\r\n"), "$-1\r\n");
    // ...but kept, so the master's PERSIST still applies.
    assert_eq!(send_and_read_reply(&mut m, b"PERSIST k\r\n"), ":1\r\n");
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    assert_eq!(send_and_read_reply(&mut r, b"GET k\r\n"), "$1\r\nv\r\n");
    drop(replica);
    drop(master);
}

/// Pipelined writes to a read-only replica are refused (-READONLY) like
/// single ones. Batched, they bypassed the check and were applied, so the
/// replica silently diverged from its master.
#[test]
fn test_replica_refuses_pipelined_writes_e2e() {
    let (mp, rp) = (18033u16, 18034u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let master = ServerGuard(spawn_rudis_listening(
        &["--port", &mps, "--threads", "2", "--no-pin"],
        mp,
    ));
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    let reply = send_and_read_reply(&mut r, b"SET a 1\r\nSET b 2\r\nGET a\r\n");
    assert!(reply.starts_with("-READONLY"), "{reply}");
    assert_eq!(reply.matches("-READONLY").count(), 2, "{reply}");
    assert!(
        reply.ends_with("$-1\r\n"),
        "the write must not have applied: {reply}"
    );
    drop(replica);
    drop(master);
}

/// SETs logged with the in-place AOF encoding (binary keys and values,
/// empty and large values, several DBs, mixed with other writes) reach a
/// replica and a restart from the AOF byte for byte.
#[test]
fn test_set_log_encoding_round_trips_through_replica_and_aof_e2e() {
    fn resp(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }
    fn bulk(v: &[u8]) -> String {
        let mut out = format!("${}\r\n", v.len()).into_bytes();
        out.extend_from_slice(v);
        out.extend_from_slice(b"\r\n");
        String::from_utf8_lossy(&out).into_owned()
    }
    let (mp, rp) = (18041u16, 18042u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let dir = std::env::temp_dir().join(format!("rudis-e2e-setenc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let margs = [
        "--port",
        &mps,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        &ds,
        "--appendonly",
        "yes",
        "--appendfsync",
        "everysec",
    ];
    let master = ServerGuard(spawn_rudis_listening(&margs, mp));
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    let big = vec![b'x'; 100_000];
    let data: Vec<(&[u8], Vec<u8>, Vec<u8>)> = vec![
        (b"0", b"plain".to_vec(), b"v".to_vec()),
        (b"0", b"bin\r\n\0key".to_vec(), b"val\r\n\0ue".to_vec()),
        (b"0", b"empty".to_vec(), Vec::new()),
        (b"0", b"big".to_vec(), big.clone()),
        (b"2", b"plain".to_vec(), b"db2".to_vec()),
        (b"2", b"n".to_vec(), b"41".to_vec()),
    ];
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for (db, k, v) in &data {
        send_and_read_reply(&mut m, &resp(&[b"SELECT", db]));
        assert_eq!(
            send_and_read_reply(&mut m, &resp(&[b"SET", k, v])),
            "+OK\r\n"
        );
    }
    // Other writes between SETs, and a SET that does not apply.
    assert_eq!(send_and_read_reply(&mut m, b"INCR n\r\n"), ":42\r\n");
    assert_eq!(send_and_read_reply(&mut m, b"SET n 0 NX\r\n"), "$-1\r\n");
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    let expect: Vec<(&[u8], Vec<u8>, String)> = data
        .iter()
        .map(|(db, k, v)| {
            let v = if k == b"n" { b"42".to_vec() } else { v.clone() };
            (*db, k.clone(), bulk(&v))
        })
        .collect();
    let check = |c: &mut TcpStream, who: &str| {
        for (db, k, want) in &expect {
            send_and_read_reply(c, &resp(&[b"SELECT", db]));
            let got = send_and_read_reply(c, &resp(&[b"GET", k]));
            assert!(got == *want, "{who}: db {db:?} key {k:?}: {:.80}", got);
        }
    };
    check(&mut r, "replica");
    drop(replica);
    drop(master);
    let master = ServerGuard(spawn_rudis_listening(&margs, mp));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    check(&mut m, "restart from AOF");
    drop(master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Writes of many types logged through the in-place AOF encoder (binary
/// keys, members and values) leave a replica and a restart from the AOF
/// with the master's exact dataset (DEBUG DIGEST).
#[test]
fn test_mixed_writes_log_encoding_matches_replica_and_aof_e2e() {
    fn resp(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }
    let (mp, rp) = (18043u16, 18044u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let dir = std::env::temp_dir().join(format!("rudis-e2e-mixenc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let margs = [
        "--port",
        &mps,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        &ds,
        "--appendonly",
        "yes",
        "--appendfsync",
        "everysec",
        "--enable-debug-command",
        "yes",
    ];
    let master = ServerGuard(spawn_rudis_listening(&margs, mp));
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
            "--enable-debug-command",
            "yes",
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let bin: &[u8] = b"b\r\n\0in";
    for i in 0..50u32 {
        let k = format!("k{i}");
        let k = k.as_bytes();
        let n = i.to_string();
        let n = n.as_bytes();
        for cmd in [
            resp(&[b"HSET", k, b"f", bin, bin, n]),
            resp(&[b"HINCRBY", k, b"cnt", n]),
            resp(&[b"INCRBY", &[k, b"n"].concat(), n]),
            resp(&[b"DECR", &[k, b"n"].concat()]),
            resp(&[b"LPUSH", &[k, b"l"].concat(), bin, n]),
            resp(&[b"RPUSH", &[k, b"l"].concat(), b""]),
            resp(&[b"SADD", &[k, b"s"].concat(), bin, n]),
            resp(&[b"ZADD", &[k, b"z"].concat(), n, bin, b"2.5", n]),
            resp(&[b"APPEND", &[k, b"a"].concat(), bin]),
            resp(&[b"SETRANGE", &[k, b"a"].concat(), b"2", n]),
            resp(&[b"PEXPIRE", &[k, b"a"].concat(), b"100000"]),
            resp(&[b"XADD", &[k, b"x"].concat(), b"*", b"f", bin]),
            resp(&[b"SREM", &[k, b"s"].concat(), n]),
            resp(&[b"LPOP", &[k, b"l"].concat()]),
            resp(&[b"ZINCRBY", &[k, b"z"].concat(), b"1.25", bin]),
            resp(&[b"HDEL", k, b"f"]),
        ] {
            let reply = send_and_read_reply(&mut m, &cmd);
            assert!(!reply.starts_with('-'), "{reply}");
        }
    }
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    let digest = send_and_read_reply(&mut m, b"DEBUG DIGEST\r\n");
    assert!(
        digest.len() > 10 && !digest.contains("0000000000000000000000000000000000000000"),
        "{digest}"
    );
    assert_eq!(
        send_and_read_reply(&mut r, b"DEBUG DIGEST\r\n"),
        digest,
        "replica"
    );
    drop(replica);
    drop(master);
    let master = ServerGuard(spawn_rudis_listening(&margs, mp));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG DIGEST\r\n"),
        digest,
        "restart from AOF"
    );
    drop(master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// On a replica, a hash field past its deadline reads as gone even before
/// the master's HDEL arrives (as Redis replicas answer), but stays stored
/// so the master's later commands on it still apply. Reads used to return
/// it until the master expired it.
#[test]
fn test_replica_hides_expired_hash_fields_e2e() {
    let (mp, rp) = (18055u16, 18056u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let master = ServerGuard(spawn_rudis_listening(
        &["--port", &mps, "--threads", "2", "--no-pin"],
        mp,
    ));
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !send_and_read_reply(&mut r, b"INFO replication\r\n").contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    // Let the replica's cron see its role.
    thread::sleep(Duration::from_millis(300));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Keep the master from deleting the field, so no HDEL reaches the replica.
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG SET-ACTIVE-EXPIRE 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG SET-ALLOW-ACCESS-EXPIRED 1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"HSET h f1 v1 f2 v2\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"HPEXPIRE h 300 FIELDS 1 f1\r\n"),
        "*1\r\n:1\r\n"
    );
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read_reply(&mut r, b"HGET h f1\r\n"),
        "$2\r\nv1\r\n"
    );
    thread::sleep(Duration::from_millis(500));
    // Expired: hidden from reads on the replica...
    assert_eq!(send_and_read_reply(&mut r, b"HGET h f1\r\n"), "$-1\r\n");
    assert_eq!(send_and_read_reply(&mut r, b"HEXISTS h f1\r\n"), ":0\r\n");
    assert_eq!(
        send_and_read_reply(&mut r, b"HMGET h f1 f2\r\n"),
        "*2\r\n$-1\r\n$2\r\nv2\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut r, b"HGETALL h\r\n"),
        "*2\r\n$2\r\nf2\r\n$2\r\nv2\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut r, b"HTTL h FIELDS 2 f1 f2\r\n"),
        "*2\r\n:-2\r\n:-1\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut r, b"HSCAN h 0\r\n"),
        "*2\r\n$1\r\n0\r\n*2\r\n$2\r\nf2\r\n$2\r\nv2\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut r, b"HRANDFIELD h -3\r\n"),
        "*3\r\n$2\r\nf2\r\n$2\r\nf2\r\n$2\r\nf2\r\n"
    );
    // ...but kept, so the master's HPERSIST still applies.
    assert_eq!(
        send_and_read_reply(&mut m, b"HPERSIST h FIELDS 1 f1\r\n"),
        "*1\r\n:1\r\n"
    );
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read_reply(&mut r, b"HGET h f1\r\n"),
        "$2\r\nv1\r\n"
    );
    drop(replica);
    drop(master);
}

/// A full sync of a snapshot over 2 GiB reaches the replica. The master
/// sent the snapshot in one write, which failed (EINVAL) past 2 GiB: it
/// dropped the connection without a word and the replica retried forever.
#[test]
fn test_full_sync_over_2gib_e2e() {
    let mp = 18058u16;
    let mps = mp.to_string();
    // On io_uring, as in production: the legacy driver's send() takes a
    // partial write instead of failing.
    let master = ServerGuard(spawn_rudis_listening_driver(
        &[
            "--port",
            &mps,
            "--threads",
            "2",
            "--no-pin",
            "--enable-debug-command",
            "yes",
        ],
        mp,
        "0",
    ));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    // 1100 x 2 MB values: a ~2.2 GB snapshot.
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG POPULATE 1100 big 2000000\r\n"),
        "+OK\r\n"
    );
    // A replica's handshake, as a Redis replica (or redis-cli --rdb) sends it.
    let mut r = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    r.write_all(b"PSYNC ? -1\r\n").unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    // "+FULLRESYNC <id> <offset>\r\n$<len>\r\n"
    while head.iter().filter(|&&b| b == b'\n').count() < 2 {
        assert_eq!(
            r.read(&mut byte).unwrap(),
            1,
            "the master closed the connection"
        );
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).unwrap();
    assert!(head.starts_with("+FULLRESYNC "), "{head}");
    let len: u64 = head.lines().nth(1).unwrap()[1..].parse().unwrap();
    assert!(len > 1 << 31, "snapshot of {len} bytes is not over 2 GiB");
    let mut left = len;
    let mut buf = vec![0u8; 1 << 20];
    while left > 0 {
        let n = r.read(&mut buf[..left.min(1 << 20) as usize]).unwrap();
        assert!(
            n > 0,
            "the master closed the connection with {left} bytes to go"
        );
        left -= n as u64;
    }
    drop(master);
}

/// A replica takes a diskless full sync (`$EOF:<mark>` instead of a length,
/// as masters stream it) and advertises that it can (`REPLCONF capa eof`).
/// The stream that follows the mark in the same write applies too. Such a
/// transfer used to be refused, so the replica could not sync.
#[test]
fn test_replica_takes_eof_mark_full_sync_e2e() {
    // A real snapshot to send: fetched from a server with a raw PSYNC.
    let sp = 18060u16;
    let sps = sp.to_string();
    let source = ServerGuard(spawn_rudis_listening(
        &["--port", &sps, "--threads", "2", "--no-pin"],
        sp,
    ));
    let mut c = TcpStream::connect(("127.0.0.1", sp)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(send_and_read_reply(&mut c, b"SET k v\r\n"), "+OK\r\n");
    c.write_all(b"PSYNC ? -1\r\n").unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while head.iter().filter(|&&b| b == b'\n').count() < 2 {
        assert_eq!(c.read(&mut byte).unwrap(), 1);
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).unwrap();
    let len: usize = head.lines().nth(1).unwrap()[1..].parse().unwrap();
    let mut rdb = vec![0u8; len];
    c.read_exact(&mut rdb).unwrap();
    drop(c);
    drop(source);

    // A fake master serving it as a diskless transfer.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mport = listener.local_addr().unwrap().port().to_string();
    let master = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut capas = Vec::new();
        loop {
            let args = read_resp_command(&mut s);
            match args[0].to_ascii_uppercase().as_str() {
                "PING" => s.write_all(b"+PONG\r\n").unwrap(),
                "REPLCONF" => {
                    if args[1].eq_ignore_ascii_case("capa") {
                        capas.extend(
                            args[2..]
                                .iter()
                                .filter(|a| !a.eq_ignore_ascii_case("capa"))
                                .cloned(),
                        );
                    }
                    s.write_all(b"+OK\r\n").unwrap();
                }
                "PSYNC" => break,
                other => panic!("unexpected handshake command {other}"),
            }
        }
        let mark = "0123456789abcdefghij0123456789ABCDEFGHIJ";
        let mut out = format!("+FULLRESYNC {} 0\r\n$EOF:{mark}\r\n", "f".repeat(40)).into_bytes();
        out.extend_from_slice(&rdb);
        out.extend_from_slice(mark.as_bytes());
        out.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
        s.write_all(&out).unwrap();
        thread::sleep(Duration::from_secs(5));
        capas
    });

    let rp = 18061u16;
    let rps = rp.to_string();
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--replicaof",
            "127.0.0.1",
            &mport,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while send_and_read_reply(&mut r, b"GET k2\r\n") != "$2\r\nv2\r\n" {
        assert!(
            std::time::Instant::now() < deadline,
            "the replica never applied the sync"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(send_and_read_reply(&mut r, b"GET k\r\n"), "$1\r\nv\r\n");
    let capas = master.join().unwrap();
    assert!(
        capas.iter().any(|c| c.eq_ignore_ascii_case("eof")),
        "capas: {capas:?}"
    );
    drop(replica);
}

/// A full sync to a replica that takes `$EOF:` transfers is streamed: the
/// master keeps serving (each shard writes its part in short steps) and
/// the replica ends up identical, writes made during the sync included.
/// The snapshot used to be built in one blocking step per shard (~110 ms
/// per GB) and held whole in memory.
#[test]
fn test_streamed_full_sync_e2e() {
    let (mp, rp) = (18062u16, 18063u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let master = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &mps,
            "--threads",
            "2",
            "--no-pin",
            "--enable-debug-command",
            "yes",
        ],
        mp,
    ));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    // ~600 MB: ~300 MB a shard, which the buffered path serialized in one go.
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG POPULATE 3000000 key 200\r\n"),
        "+OK\r\n"
    );
    for i in 0..100 {
        send_and_read_reply(&mut m, format!("RPUSH list{i} a b\r\n").as_bytes());
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worst = std::sync::Arc::new(std::sync::Mutex::new(Duration::ZERO));
    let (stop2, worst2) = (stop.clone(), worst.clone());
    // Writes (non-idempotent ones too) and latency probes during the sync.
    let writer = thread::spawn(move || {
        let mut w = TcpStream::connect(("127.0.0.1", mp)).unwrap();
        w.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        // Several connections, so both shards' threads are probed.
        let mut probes: Vec<TcpStream> = (0..8)
            .map(|_| {
                let p = TcpStream::connect(("127.0.0.1", mp)).unwrap();
                p.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                p
            })
            .collect();
        let mut i = 0u64;
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            for p in probes.iter_mut() {
                let t = std::time::Instant::now();
                assert_eq!(send_and_read_reply(p, b"PING\r\n"), "+PONG\r\n");
                let dt = t.elapsed();
                let mut wst = worst2.lock().unwrap();
                *wst = (*wst).max(dt);
            }
            let k = i % 100;
            send_and_read_reply(&mut w, format!("INCR cnt{k}\r\n").as_bytes());
            send_and_read_reply(&mut w, format!("LPUSH list{k} x{i}\r\n").as_bytes());
            send_and_read_reply(
                &mut w,
                format!("SET key:{} changed{i}\r\n", i * 7919 % 3_000_000).as_bytes(),
            );
            i += 1;
            thread::sleep(Duration::from_millis(1));
        }
    });
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--enable-debug-command",
            "yes",
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let info = send_and_read_reply(&mut r, b"INFO replication\r\n");
        if info.contains("master_link_status:up") && info.contains("master_sync_in_progress:0") {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no sync: {info}");
        thread::sleep(Duration::from_millis(100));
    }
    // Keep writing a little past the sync, then let the replica catch up.
    thread::sleep(Duration::from_millis(300));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    let worst = *worst.lock().unwrap();
    eprintln!("worst master PING during the full sync: {worst:?}");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let md = send_and_read_reply(&mut m, b"DEBUG DIGEST\r\n");
        let rd = send_and_read_reply(&mut r, b"DEBUG DIGEST\r\n");
        if md == rd {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica differs: {md} vs {rd}"
        );
        thread::sleep(Duration::from_millis(200));
    }
    assert!(
        worst < Duration::from_millis(200),
        "a PING took {worst:?} during the full sync"
    );
    drop(replica);
    drop(master);
}

/// After a streamed (`$EOF:`) full sync, nothing follows the mark until the
/// replica acks: Redis replicas find the end of such a transfer by the mark
/// being the last bytes of a read, so the stream written meanwhile is held
/// until their first `REPLCONF ACK` (as Redis masters do), then sent. It
/// followed the mark at once, and real Redis replicas waited forever.
#[test]
fn test_streamed_full_sync_waits_for_ack_e2e() {
    let mp = 18067u16;
    let mps = mp.to_string();
    let master = ServerGuard(spawn_rudis_listening(
        &["--port", &mps, "--threads", "2", "--no-pin"],
        mp,
    ));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    for i in 0..2000 {
        send_and_read_reply(
            &mut m,
            format!("SET key:{i} {}\r\n", "x".repeat(300)).as_bytes(),
        );
    }
    // Writes during the sync, so there is stream to hold back.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = stop.clone();
    let writer = thread::spawn(move || {
        let mut w = TcpStream::connect(("127.0.0.1", mp)).unwrap();
        w.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            send_and_read_reply(&mut w, b"INCR counter\r\n");
        }
    });
    thread::sleep(Duration::from_millis(50));
    // A replica's handshake, as Redis replicas send it.
    let mut r = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    for cmd in [
        &b"PING\r\n"[..],
        b"REPLCONF listening-port 6399\r\n",
        b"REPLCONF capa eof capa psync2\r\n",
    ] {
        r.write_all(cmd).unwrap();
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        while !line.ends_with(b"\r\n") {
            assert_eq!(r.read(&mut byte).unwrap(), 1);
            line.push(byte[0]);
        }
        assert!(line[0] == b'+', "{}", String::from_utf8_lossy(&line));
    }
    r.write_all(b"PSYNC ? -1\r\n").unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while head.iter().filter(|&&b| b == b'\n').count() < 2 {
        assert_eq!(r.read(&mut byte).unwrap(), 1);
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).unwrap();
    let mark = head
        .lines()
        .nth(1)
        .and_then(|l| l.strip_prefix("$EOF:"))
        .unwrap_or_else(|| panic!("not a streamed sync: {head}"))
        .to_string();
    // Read up to the mark.
    let mut got = Vec::new();
    let mut buf = vec![0u8; 65536];
    let end = loop {
        if let Some(p) = got.windows(40).position(|w| w == mark.as_bytes()) {
            break p + 40;
        }
        let n = r.read(&mut buf).unwrap();
        assert!(n > 0, "the master closed the connection");
        got.extend_from_slice(&buf[..n]);
    };
    // Nothing after the mark until we ack, though the master takes writes.
    let early = got.len() - end;
    r.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let more = r.read(&mut buf).unwrap_or_default();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    assert_eq!(
        early + more,
        0,
        "{} bytes followed the mark before the ack",
        early + more
    );
    // After the ack, the held-back stream arrives.
    r.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n")
        .unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let n = r.read(&mut buf).unwrap();
    assert!(n > 0);
    let stream = String::from_utf8_lossy(&buf[..n]).to_lowercase();
    assert!(
        stream.contains("incr") || stream.contains("counter"),
        "no stream after the ack: {stream:?}"
    );
    drop(master);
}

/// A replica receives a full sync's RDB into a file it then maps, as Redis
/// replicas do by default: right after syncing ~200 MB, its anonymous memory
/// is about the dataset. It held the whole RDB in memory before loading it,
/// so a full sync needed about twice the dataset's memory on the replica.
#[test]
fn test_replica_spools_full_sync_to_disk_e2e() {
    let (mp, rp) = (18065u16, 18066u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let dir = std::env::temp_dir().join(format!("rudis-e2e-spool-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let master = ServerGuard(spawn_rudis_listening(
        &["--port", &mps, "--threads", "2", "--no-pin"],
        mp,
    ));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    // 200 incompressible 1 MB values.
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for i in 0..200 {
        let value: Vec<u8> = (0..1 << 20)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                b'a' + (x % 26) as u8
            })
            .collect();
        let key = format!("big{i}");
        let mut cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n",
            key.len(),
            value.len()
        )
        .into_bytes();
        cmd.extend_from_slice(&value);
        cmd.extend_from_slice(b"\r\n");
        assert_eq!(send_and_read_reply(&mut m, &cmd), "+OK\r\n");
    }
    let replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--dir",
            &ds,
            "--replicaof",
            "127.0.0.1",
            &mps,
        ],
        rp,
    ));
    let mut r = TcpStream::connect(("127.0.0.1", rp)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let info = send_and_read_reply(&mut r, b"INFO replication\r\n");
        if info.contains("master_link_status:up") && info.contains("master_sync_in_progress:0") {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no sync: {info}");
        thread::sleep(Duration::from_millis(50));
    }
    let status = std::fs::read_to_string(format!("/proc/{}/status", replica.0.id())).unwrap();
    let anon_mb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("RssAnon:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .unwrap()
        / 1024;
    assert_eq!(send_and_read_reply(&mut r, b"DBSIZE\r\n"), ":200\r\n");
    eprintln!("replica anonymous memory right after syncing 200 MB: {anon_mb} MB");
    // The spool file is gone once loaded.
    let left: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("temp-repl-"))
        .collect();
    assert!(left.is_empty(), "spool file left behind: {left:?}");
    assert!(
        anon_mb < 320,
        "{anon_mb} MB anonymous memory on the replica after syncing 200 MB"
    );
    drop(replica);
    drop(master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `proto-max-bulk-len` limits what clients send, not the server's own
/// data: a server restarted with a lower limit than values already in its
/// AOF loads them, and a replica with a low limit follows a master whose
/// writes exceed it (both used to fail on "invalid bulk length").
#[test]
fn test_proto_max_bulk_len_spares_aof_and_master_stream_e2e() {
    let (port, rport) = (18207u16, 18213u16);
    let dir = std::env::temp_dir().join(format!("rudis_bulklen_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap().to_string();
    let ps = port.to_string();
    let cmd = |p: u16, args: &[&str]| {
        let mut c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        resp_cmd_full(&mut c, args)
    };
    let big = "b".repeat(4096);
    let base = [
        "--port",
        &ps,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        &dir_s,
        "--appendonly",
        "yes",
    ];
    let first = ServerGuard(spawn_rudis_listening(&base, port));
    assert_eq!(cmd(port, &["SET", "big", &big]), "+OK\r\n");
    drop(first);

    // Restarted with a limit below the value it holds in its AOF.
    let mut low = base.to_vec();
    low.extend(["--proto-max-bulk-len", "1kb"]);
    let _master = ServerGuard(spawn_rudis_listening(&low, port));
    assert_eq!(cmd(port, &["STRLEN", "big"]), ":4096\r\n");
    assert_eq!(
        cmd(port, &["CONFIG", "SET", "proto-max-bulk-len", "536870912"]),
        "+OK\r\n"
    );

    // A replica with a 1kb limit gets the master's 4 KB writes.
    let rdir = dir.join("replica");
    std::fs::create_dir_all(&rdir).unwrap();
    let rps = rport.to_string();
    let _replica = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &rps,
            "--threads",
            "2",
            "--no-pin",
            "--dir",
            rdir.to_str().unwrap(),
            "--proto-max-bulk-len",
            "1kb",
            "--replicaof",
            "127.0.0.1",
            &ps,
        ],
        rport,
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !cmd(rport, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline, "replica never linked");
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(cmd(port, &["SET", "streamed", &big]), "+OK\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let got = cmd(rport, &["STRLEN", "streamed"]);
        if got == ":4096\r\n" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica holds {got:?}"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(cmd(rport, &["INFO", "replication"]).contains("master_link_status:up"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Keys a client writes with a TTL on a writable replica expire there (no
/// master will send their DEL), while the master's keys still wait for it.
#[test]
fn test_writable_replica_expires_its_own_keys_e2e() {
    let (mport, rport) = (29831u16, 29832u16);
    start_test_server(mport, 2);
    start_test_server(rport, 2);
    let mut master = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    let mut replica = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    replica
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    assert_eq!(
        resp_cmd(
            &mut replica,
            &["REPLICAOF", "127.0.0.1", &mport.to_string()]
        ),
        "+OK\r\n"
    );
    wait_for_master_link(&mut replica);
    assert_eq!(
        resp_cmd(&mut master, &["SET", "from_master", "v"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut replica, &["CONFIG", "SET", "replica-read-only", "no"]),
        "+OK\r\n"
    );
    for k in ["k1", "k2", "k3"] {
        assert_eq!(
            resp_cmd(&mut replica, &["SET", k, "1", "PX", "300"]),
            "+OK\r\n"
        );
    }
    // Pipelined writes too.
    replica
        .write_all(b"SET k4 1 PX 300\r\nSET k5 1 PX 300\r\n")
        .unwrap();
    let mut buf = [0u8; 64];
    let mut got = 0;
    while got < 10 {
        got += replica.read(&mut buf[got..]).unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let size = resp_cmd(&mut replica, &["DBSIZE"]);
        if size == ":1\r\n" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica kept its expired keys: DBSIZE {size}"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        resp_cmd(&mut replica, &["GET", "from_master"]),
        "$1\r\nv\r\n"
    );
    assert_eq!(
        resp_cmd(&mut replica, &["CONFIG", "SET", "replica-read-only", "yes"]),
        "+OK\r\n"
    );
}

/// WAIT refuses a negative timeout, or one so large it overflows when
/// added to the clock, instead of blocking the client for good.
#[test]
fn test_wait_refuses_out_of_range_timeouts_e2e() {
    let port = 29851;
    start_test_server(port, 2);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let r = resp_cmd(&mut c, &["WAIT", "2", "9223372036854775807"]);
    assert!(r.contains("timeout is out of range"), "{r}");
    let r = resp_cmd(&mut c, &["WAIT", "2", "9223372036854775808"]);
    assert!(r.contains("out of range"), "{r}");
    let r = resp_cmd(&mut c, &["WAIT", "2", "-1"]);
    assert!(r.contains("timeout is negative"), "{r}");
    // No replicas: WAIT 0 returns at once.
    assert_eq!(resp_cmd(&mut c, &["WAIT", "0", "100"]), ":0\r\n");
}

/// A master shutting down (SIGTERM) waits up to `shutdown-timeout` for its
/// replica to acknowledge everything, as Redis does: a replica that catches
/// up within it has every acknowledged write, and one that doesn't is
/// logged as lagging.
#[test]
fn test_master_shutdown_waits_for_replicas_e2e() {
    for (mport, rport, resume) in [(29861u16, 29862u16, true), (29863, 29864, false)] {
        let dir = std::env::temp_dir().join(format!("rudis-srv-{}-{}", std::process::id(), mport));
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("master.log");
        let mut master = rudis_bin()
            .args([
                "--port",
                &mport.to_string(),
                "--threads",
                "2",
                "--no-pin",
                "--save",
                "",
            ])
            .args(["--dir", dir.to_str().unwrap()])
            .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut replica = spawn_rudis_listening(
            &[
                "--port",
                &rport.to_string(),
                "--threads",
                "2",
                "--no-pin",
                "--save",
                "",
            ],
            rport,
        );
        let connect = |port: u16| {
            for _ in 0..100 {
                if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                    return s;
                }
                thread::sleep(Duration::from_millis(50));
            }
            panic!("no server on {port}");
        };
        let (mut m, mut r) = (connect(mport), connect(rport));
        assert_eq!(
            resp_cmd(&mut m, &["CONFIG", "SET", "shutdown-timeout", "2"]),
            "+OK\r\n"
        );
        assert_eq!(
            resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &mport.to_string()]),
            "+OK\r\n"
        );
        wait_for_master_link(&mut r);
        assert_eq!(resp_cmd(&mut m, &["SET", "k", "1"]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut m, &["WAIT", "1", "5000"]), ":1\r\n");
        // SAFETY: kill(2) on our own live child.
        unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGSTOP) };
        assert_eq!(resp_cmd(&mut m, &["INCR", "k"]), ":2\r\n");
        // SAFETY: as above.
        unsafe { libc::kill(master.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let info = resp_cmd_full(&mut connect(mport), &["INFO", "server"]);
            if info.lines().any(|l| {
                l.starts_with("shutdown_in_milliseconds:") && l != "shutdown_in_milliseconds:0"
            }) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no shutdown wait: {info}"
            );
            thread::sleep(Duration::from_millis(20));
        }
        if resume {
            // SAFETY: as above.
            unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGCONT) };
        }
        assert!(wait_exit(&mut master, Duration::from_secs(20)).is_some());
        let text = std::fs::read_to_string(&log).unwrap();
        if resume {
            assert!(text.contains("1 of 1 replicas are in sync"), "{text}");
            assert!(!text.contains("Lagging replica"), "{text}");
            assert_eq!(resp_cmd(&mut r, &["GET", "k"]), "$1\r\n2\r\n");
        } else {
            assert!(text.contains("0 of 1 replicas are in sync"), "{text}");
            assert!(text.contains("Lagging replica"), "{text}");
            // SAFETY: as above.
            unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGCONT) };
        }
        let _ = replica.kill();
        let _ = replica.wait();
    }
}

/// WAITAOF counts a copy only once it is fsynced to an AOF, as Redis does:
/// the local one by the AOF's own fsync (never under `appendfsync no`), a
/// replica's by its `FACK`, which a replica without AOF does not send. A
/// WAIT with no timeout blocks (and is counted in `blocked_clients`) until
/// the replica acks.
#[test]
fn test_waitaof_counts_fsynced_copies_e2e() {
    let (mport, aport, nport) = (29871u16, 29872u16, 29873u16);
    let base = std::env::temp_dir().join(format!("rudis-waitaof-{}", std::process::id()));
    let dirs: Vec<String> = ["m", "a", "n"]
        .iter()
        .map(|d| {
            let p = base.join(d);
            std::fs::create_dir_all(&p).unwrap();
            p.to_str().unwrap().to_string()
        })
        .collect();
    let spawn = |port: u16, dir: &str, aof: &str| {
        let p = port.to_string();
        let args = [
            "--port",
            &p,
            "--threads",
            "2",
            "--no-pin",
            "--save",
            "",
            "--dir",
            dir,
            "--appendonly",
            aof,
        ];
        spawn_rudis_listening(&args, port)
    };
    let mut master = spawn(mport, &dirs[0], "yes");
    let mut aof_replica = spawn(aport, &dirs[1], "yes");
    let mut plain_replica = spawn(nport, &dirs[2], "no");
    let connect = |port: u16| {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    };
    let (mut m, mut a, mut n) = (connect(mport), connect(aport), connect(nport));

    // Without AOF there is no local copy to wait for.
    let r = resp_cmd(&mut n, &["WAITAOF", "1", "0", "0"]);
    assert!(
        r.contains("numlocal is set but appendonly is disabled"),
        "{r}"
    );
    assert_eq!(
        resp_cmd(&mut n, &["WAITAOF", "0", "0", "0"]),
        "*2\r\n:0\r\n:0\r\n"
    );

    // appendfsync no never fsyncs: the local copy times out.
    assert_eq!(
        resp_cmd(&mut m, &["CONFIG", "SET", "appendfsync", "no"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut m, &["INCR", "k"]), ":1\r\n");
    assert_eq!(
        resp_cmd(&mut m, &["WAITAOF", "1", "0", "50"]),
        "*2\r\n:0\r\n:0\r\n"
    );
    // everysec: the next fsync counts it.
    assert_eq!(
        resp_cmd(&mut m, &["CONFIG", "SET", "appendfsync", "everysec"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut m, &["WAITAOF", "1", "0", "0"]),
        "*2\r\n:1\r\n:0\r\n"
    );
    assert_eq!(
        resp_cmd(&mut m, &["CONFIG", "SET", "appendfsync", "always"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut m, &["INCR", "k"]), ":2\r\n");
    assert_eq!(
        resp_cmd(&mut m, &["WAITAOF", "1", "0", "0"]),
        "*2\r\n:1\r\n:0\r\n"
    );

    for r in [&mut a, &mut n] {
        assert_eq!(
            resp_cmd(r, &["REPLICAOF", "127.0.0.1", &mport.to_string()]),
            "+OK\r\n"
        );
        wait_for_master_link(r);
    }
    let r = resp_cmd(&mut a, &["WAITAOF", "0", "0", "0"]);
    assert!(r.contains("cannot be used with replica instances"), "{r}");
    assert_eq!(resp_cmd(&mut m, &["INCR", "k"]), ":3\r\n");
    assert_eq!(resp_cmd(&mut m, &["WAIT", "2", "5000"]), ":2\r\n");
    // Both replicas have the write, but only the one with AOF fsyncs it.
    assert_eq!(
        resp_cmd(&mut m, &["WAITAOF", "1", "1", "5000"]),
        "*2\r\n:1\r\n:1\r\n"
    );
    assert_eq!(
        resp_cmd(&mut m, &["WAITAOF", "0", "2", "1500"]),
        "*2\r\n:1\r\n:1\r\n"
    );

    // WAIT 0 has no timeout: it blocks until the stopped replica acks.
    // SAFETY: kill(2) on our own live child.
    unsafe { libc::kill(aof_replica.id() as libc::pid_t, libc::SIGSTOP) };
    assert_eq!(resp_cmd(&mut m, &["INCR", "k"]), ":4\r\n");
    m.write_all(b"*3\r\n$4\r\nWAIT\r\n$1\r\n2\r\n$1\r\n0\r\n")
        .unwrap();
    let mut probe = connect(mport);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !resp_cmd_full(&mut probe, &["INFO", "clients"]).contains("blocked_clients:1\r\n") {
        assert!(std::time::Instant::now() < deadline, "WAIT did not block");
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(300));
    // SAFETY: as above.
    unsafe { libc::kill(aof_replica.id() as libc::pid_t, libc::SIGCONT) };
    let mut buf = [0u8; 16];
    let len = m.read(&mut buf).unwrap();
    assert_eq!(&buf[..len], b":2\r\n");
    assert!(resp_cmd_full(&mut probe, &["INFO", "clients"]).contains("blocked_clients:0\r\n"));

    for c in [&mut master, &mut aof_replica, &mut plain_replica] {
        let _ = c.kill();
        let _ = c.wait();
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// ROLE on a replica names its link's phase as Redis does: `connecting`
/// while no master answers, `handshake` while the master has not answered
/// PSYNC, `sync` while the RDB transfer runs, `connected` once up.
#[test]
fn test_role_shows_replica_link_phases_e2e() {
    let port = 29891u16;
    let p = port.to_string();
    let mut replica = spawn_rudis_listening(
        &["--port", &p, "--threads", "2", "--no-pin", "--save", ""],
        port,
    );
    let mut r = TcpStream::connect(("127.0.0.1", port)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let wait_state = |r: &mut TcpStream, state: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let role = resp_cmd_full(r, &["ROLE"]);
            if role.contains(&format!("\r\n{state}\r\n")) {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "no {state}: {role}");
            thread::sleep(Duration::from_millis(50));
        }
    };
    // Nobody listens on 29892.
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", "29892"]),
        "+OK\r\n"
    );
    wait_state(&mut r, "connecting");

    // A fake master: answers the handshake but holds back PSYNC's reply
    // until told, then announces a full sync and sends no RDB.
    let listener = std::net::TcpListener::bind("127.0.0.1:29893").unwrap();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let fake = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        // The replica sends one command and waits for its reply.
        let mut buf = [0u8; 4096];
        loop {
            let n = s.read(&mut buf).unwrap();
            assert!(n > 0, "replica hung up");
            let cmd = String::from_utf8_lossy(&buf[..n]).to_string();
            if cmd.contains("PSYNC") {
                break;
            }
            let reply: &[u8] = if cmd.contains("PING") {
                b"+PONG\r\n"
            } else {
                b"+OK\r\n"
            };
            s.write_all(reply).unwrap();
        }
        go_rx.recv().unwrap();
        s.write_all(b"+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n")
            .unwrap();
        // Hold the connection until the test ends.
        let _ = go_rx.recv();
    });
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", "29893"]),
        "+OK\r\n"
    );
    wait_state(&mut r, "handshake");
    go_tx.send(()).unwrap();
    wait_state(&mut r, "sync");
    drop(go_tx);
    let _ = fake.join();

    assert_eq!(resp_cmd(&mut r, &["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    let _ = replica.kill();
    let _ = replica.wait();
}

/// INFO connected_clients leaves out replica links, as in Redis (they are
/// connected_slaves): a master with one client and one replica reports 1.
#[test]
fn test_connected_clients_excludes_replicas_e2e() {
    let (mport, rport) = (29895u16, 29896u16);
    let spawn = |port: u16| {
        let p = port.to_string();
        spawn_rudis_listening(
            &["--port", &p, "--threads", "2", "--no-pin", "--save", ""],
            port,
        )
    };
    let mut master = spawn(mport);
    let mut replica = spawn(rport);
    let mut r = TcpStream::connect(("127.0.0.1", rport)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    wait_for_master_link(&mut r);
    let mut m = TcpStream::connect(("127.0.0.1", mport)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let info = resp_cmd_full(&mut m, &["INFO"]);
        if info.contains("connected_slaves:1\r\n") {
            assert!(info.contains("connected_clients:1\r\n"), "{info}");
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{info}");
        thread::sleep(Duration::from_millis(50));
    }
    for c in [&mut master, &mut replica] {
        let _ = c.kill();
        let _ = c.wait();
    }
}

/// FAILOVER (Redis 6.2+): the master pauses writes, waits for the target
/// to catch up, hands over with PSYNC FAILOVER and continues as its
/// replica, losing no write. A failover whose target never catches up is
/// aborted at its timeout and writes resume. Bad requests get Redis's
/// errors.
#[test]
fn test_failover_command_switches_master_and_replica_e2e() {
    let (mport, aport, bport) = (29901u16, 29902u16, 29903u16);
    let spawn = |port: u16| {
        let p = port.to_string();
        spawn_rudis_listening(
            &["--port", &p, "--threads", "2", "--no-pin", "--save", ""],
            port,
        )
    };
    let mut nodes = [spawn(mport), spawn(aport), spawn(bport)];
    let connect = |port: u16| {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    };
    let (mut m, mut a, mut b) = (connect(mport), connect(aport), connect(bport));
    let err = |c: &mut TcpStream, args: &[&str], want: &str| {
        let r = resp_cmd(c, args);
        assert!(r.starts_with("-ERR") && r.contains(want), "{args:?}: {r}");
    };
    err(
        &mut m,
        &["FAILOVER"],
        "FAILOVER requires connected replicas.",
    );
    err(&mut m, &["FAILOVER", "ABORT"], "No failover in progress.");
    for r in [&mut a, &mut b] {
        assert_eq!(
            resp_cmd(r, &["REPLICAOF", "127.0.0.1", &mport.to_string()]),
            "+OK\r\n"
        );
        wait_for_master_link(r);
    }
    let wait_info = |c: &mut TcpStream, what: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let info = resp_cmd_full(c, &["INFO", "replication"]);
            if info.contains(what) {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "no {what}: {info}");
            thread::sleep(Duration::from_millis(50));
        }
    };
    wait_info(&mut m, "connected_slaves:2");
    assert!(
        resp_cmd_full(&mut m, &["INFO", "replication"])
            .contains("master_failover_state:no-failover")
    );
    err(
        &mut m,
        &["FAILOVER", "TO", "127.0.0.1", "1"],
        "is not a replica",
    );
    err(
        &mut m,
        &["FAILOVER", "FORCE", "TIMEOUT", "100"],
        "requires both a timeout",
    );
    err(&mut a, &["FAILOVER"], "not valid when server is a replica");

    // Hand over to a.
    assert_eq!(resp_cmd(&mut m, &["SET", "k", "1"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut m, &["FAILOVER", "TO", "127.0.0.1", &aport.to_string()]),
        "+OK\r\n"
    );
    wait_info(&mut a, "role:master");
    wait_info(&mut m, &format!("master_port:{aport}"));
    wait_info(&mut m, "master_link_status:up");
    wait_info(&mut m, "master_failover_state:no-failover");
    assert_eq!(resp_cmd(&mut a, &["GET", "k"]), "$1\r\n1\r\n");
    let r = resp_cmd(&mut m, &["SET", "k", "x"]);
    assert!(r.starts_with("-READONLY"), "{r}");
    assert_eq!(resp_cmd(&mut a, &["SET", "k", "2"]), "+OK\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd(&mut m, &["GET", "k"]) != "$1\r\n2\r\n" {
        assert!(
            std::time::Instant::now() < deadline,
            "old master did not follow"
        );
        thread::sleep(Duration::from_millis(50));
    }

    // a's only replica (m) is stopped: the failover times out and aborts.
    wait_info(&mut a, "connected_slaves:1");
    // SAFETY: kill(2) on our own live child.
    unsafe { libc::kill(nodes[0].id() as libc::pid_t, libc::SIGSTOP) };
    assert_eq!(resp_cmd(&mut a, &["SET", "k", "3"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut a, &["FAILOVER", "TIMEOUT", "500"]), "+OK\r\n");
    assert!(
        resp_cmd_full(&mut a, &["INFO", "replication"])
            .contains("master_failover_state:waiting-for-sync")
    );
    wait_info(&mut a, "master_failover_state:no-failover");
    assert!(resp_cmd_full(&mut a, &["INFO", "replication"]).contains("role:master"));
    assert_eq!(resp_cmd(&mut a, &["SET", "k", "4"]), "+OK\r\n");
    // SAFETY: as above.
    unsafe { libc::kill(nodes[0].id() as libc::pid_t, libc::SIGCONT) };

    for n in nodes.iter_mut() {
        let _ = n.kill();
        let _ = n.wait();
    }
}

/// A master restarted from its RDB keeps its backlog for replicas to come
/// back to, as Redis does: one returning promptly continues with a partial
/// resync even after a write. With no replica back within the restart
/// grace (at most repl-backlog-ttl), the backlog is released
/// (repl_backlog_active:0, writes no longer pay for it) and a replica
/// returning later full-syncs.
#[test]
fn test_restarted_master_releases_an_unused_backlog_e2e() {
    let (mport, rport) = (29921u16, 29922u16);
    let mdir = std::env::temp_dir().join(format!("rudis-lazybl-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&mdir);
    std::fs::create_dir_all(&mdir).unwrap();
    let md = mdir.to_str().unwrap().to_string();
    let ms = mport.to_string();
    let margs = [
        "--port",
        &ms,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        &md,
        "--repl-backlog-ttl",
        "3",
    ];
    let rs = rport.to_string();
    let mut master = spawn_rudis_listening(&margs, mport);
    let mut replica = spawn_rudis_listening(
        &["--port", &rs, "--threads", "2", "--no-pin", "--save", ""],
        rport,
    );
    let connect = |port: u16| {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    };
    let mut r = connect(rport);
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    wait_for_master_link(&mut r);
    let mut m = connect(mport);
    assert_eq!(resp_cmd(&mut m, &["SET", "k", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut m, &["WAIT", "1", "5000"]), ":1\r\n");

    let stat = |c: &mut TcpStream, name: &str| -> String {
        let info = resp_cmd_full(c, &["INFO"]);
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}:")).map(str::to_string))
            .unwrap_or_else(|| panic!("no {name}: {info}"))
    };
    let wait_value = |c: &mut TcpStream, want: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while resp_cmd(c, &["GET", "k"]) != want {
            assert!(
                std::time::Instant::now() < deadline,
                "replica lacks {want:?}"
            );
            thread::sleep(Duration::from_millis(50));
        }
    };
    // Restarts the master from its RDB with the replica stopped, runs
    // `between` on the new master, then lets the replica come back.
    let mut restart = |master: &mut std::process::Child, between: &dyn Fn(&mut TcpStream)| {
        // SAFETY: kill(2) on our own live child.
        unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGSTOP) };
        let mut m = connect(mport);
        let _ = m.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$4\r\nSAVE\r\n");
        let _ = master.wait();
        *master = spawn_rudis_listening(&margs, mport);
        let mut m = connect(mport);
        assert_eq!(stat(&mut m, "repl_backlog_active"), "1");
        between(&mut m);
        // SAFETY: as above.
        unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGCONT) };
        wait_for_master_link(&mut r);
        m
    };
    // Back at once, after a write: partial resync, write included.
    let mut m = restart(&mut master, &|m| {
        assert_eq!(resp_cmd(m, &["SET", "k", "2"]), "+OK\r\n");
    });
    assert_eq!(stat(&mut m, "sync_partial_ok"), "1");
    assert_eq!(stat(&mut m, "sync_full"), "0");
    wait_value(&mut connect(rport), "$1\r\n2\r\n");
    // Back after the grace: the backlog was released; full sync.
    let mut m = restart(&mut master, &|m| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while stat(m, "repl_backlog_active") != "0" {
            assert!(
                std::time::Instant::now() < deadline,
                "backlog never released"
            );
            thread::sleep(Duration::from_millis(100));
        }
        assert_eq!(resp_cmd(m, &["SET", "k", "3"]), "+OK\r\n");
    });
    assert_eq!(stat(&mut m, "sync_full"), "1");
    assert_eq!(stat(&mut m, "sync_partial_ok"), "0");
    assert_eq!(stat(&mut m, "repl_backlog_active"), "1");
    wait_value(&mut connect(rport), "$1\r\n3\r\n");
    for c in [&mut master, &mut replica] {
        let _ = c.kill();
        let _ = c.wait();
    }
    let _ = std::fs::remove_dir_all(&mdir);
}

/// CLIENT UNBLOCK ends a client blocked in WAIT, as in Redis: TIMEOUT
/// replies with the replicas counted so far, ERROR with UNBLOCKED.
#[test]
fn test_client_unblock_ends_wait_e2e() {
    let port = 29941u16;
    start_test_server(port, 2);
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    };
    let (mut w, mut c) = (connect(), connect());
    let id = resp_cmd(&mut w, &["CLIENT", "ID"]);
    let id = id.trim().trim_start_matches(':').to_string();
    assert_eq!(resp_cmd(&mut c, &["CLIENT", "UNBLOCK", &id]), ":0\r\n");
    for (how, want) in [
        ("TIMEOUT", ":0\r\n"),
        (
            "ERROR",
            "-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n",
        ),
    ] {
        w.write_all(b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$1\r\n0\r\n")
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while resp_cmd(&mut c, &["CLIENT", "UNBLOCK", &id, how]) != ":1\r\n" {
            assert!(std::time::Instant::now() < deadline, "WAIT not blocked");
            thread::sleep(Duration::from_millis(10));
        }
        let mut buf = [0u8; 128];
        let n = w.read(&mut buf).unwrap();
        assert_eq!(String::from_utf8_lossy(&buf[..n]), want);
    }
}

/// Dual-channel replication: under writes heavier than a small
/// `client-output-buffer-limit replica` holds for a whole full sync, the
/// sync still completes at the first attempt, as the stream after the
/// snapshot flows over the main connection (buffered by the replica) while
/// the RDB arrives over its RDB channel and loads. The replica ends up with
/// the master's data.
#[test]
fn test_dual_channel_full_sync_under_writes_e2e() {
    let (mp, rp) = (29961u16, 29962u16);
    let (ms, rs) = (mp.to_string(), rp.to_string());
    let common = [
        "--threads",
        "4",
        "--no-pin",
        "--save",
        "",
        "--dual-channel-replication-enabled",
        "yes",
    ];
    let margs: Vec<&str> = ["--port", ms.as_str()]
        .iter()
        .chain(common.iter())
        .copied()
        .collect();
    let rargs: Vec<&str> = ["--port", rs.as_str()]
        .iter()
        .chain(common.iter())
        .copied()
        .collect();
    // Killed even if an assertion fails.
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _master = Kill(spawn_rudis_listening(&margs, mp));
    let _replica = Kill(spawn_rudis_listening(&rargs, rp));
    let connect = |port: u16| {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s
    };
    let mut m = connect(mp);
    let mut r = connect(rp);
    // ~100 MB of data: its transfer and load take a while.
    assert_eq!(
        resp_cmd_all(&mut m, &["DEBUG", "POPULATE", "400000", "key", "256"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd_all(
            &mut m,
            &[
                "CONFIG",
                "SET",
                "client-output-buffer-limit",
                "replica 8mb 4mb 10"
            ]
        ),
        "+OK\r\n"
    );
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut w = connect(mp);
            let val = "x".repeat(512);
            let mut n = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut pipe = String::new();
                for i in 0..200 {
                    let k = format!("w:{}", (n + i) % 50_000);
                    pipe.push_str(&format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n${}\r\n{val}\r\n",
                        k.len(),
                        val.len()
                    ));
                }
                w.write_all(pipe.as_bytes()).unwrap();
                let mut got = 0;
                let mut buf = [0u8; 4096];
                while got < 200 * 5 {
                    got += w.read(&mut buf).unwrap();
                }
                n += 200;
                // ~10 MB/s: what the replica keeps up with, but more than
                // the limit holds for a whole transfer and load.
                thread::sleep(Duration::from_millis(10));
            }
        })
    };
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !resp_cmd_all(&mut r, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(
            std::time::Instant::now() < deadline,
            "the replica never synced"
        );
        thread::sleep(Duration::from_millis(50));
    }
    // Keep writing a little longer: the buffered stream applies and the
    // live one continues.
    thread::sleep(Duration::from_millis(500));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "1", "10000"]), ":1\r\n");
    let stats = resp_cmd_all(&mut m, &["INFO", "stats"]);
    assert!(
        stats.contains("sync_full:1\r\n"),
        "the first full sync didn't hold: {stats}"
    );
    assert_eq!(
        resp_cmd_all(&mut r, &["DBSIZE"]),
        resp_cmd_all(&mut m, &["DBSIZE"])
    );
    assert_eq!(
        resp_cmd_all(&mut r, &["DEBUG", "DIGEST"]),
        resp_cmd_all(&mut m, &["DEBUG", "DIGEST"])
    );
}

/// A master restarted from its RDB drops the keys that expired meanwhile
/// and tells the replicas resuming from it, in each key's database: the
/// DELs went to whatever database the stream had selected, so a replica
/// kept the expired keys of every other one (Redis's
/// psync2-master-restart "with expire").
#[test]
fn test_expired_on_load_deletes_reach_replicas_in_their_db_e2e() {
    let (mp, rp) = (29981u16, 29982u16);
    let dir = std::env::temp_dir().join(format!("rudis-expload-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let (ms, rs) = (mp.to_string(), rp.to_string());
    let margs = ["--port", &ms, "--threads", "2", "--no-pin", "--dir", &ds];
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut master = Kill(spawn_rudis_listening(&margs, mp));
    let _replica = Kill(spawn_rudis_listening(
        &["--port", &rs, "--threads", "2", "--no-pin", "--save", ""],
        rp,
    ));
    let connect = |port: u16| {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    };
    let mut r = connect(rp);
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    wait_for_master_link(&mut r);
    let mut m = connect(mp);
    assert_eq!(
        resp_cmd_all(&mut m, &["DEBUG", "SET-ACTIVE-EXPIRE", "0"]),
        "+OK\r\n"
    );
    for db in ["0", "5"] {
        assert_eq!(resp_cmd_all(&mut m, &["SELECT", db]), "+OK\r\n");
        for i in 0..10 {
            let k = format!("e{i}");
            assert_eq!(
                resp_cmd_all(&mut m, &["SET", &k, "v", "PX", "100"]),
                "+OK\r\n"
            );
        }
    }
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "1", "5000"]), ":1\r\n");
    // Saved with the keys, which expire while the master is down.
    let _ = m.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$4\r\nSAVE\r\n");
    let _ = master.0.wait();
    thread::sleep(Duration::from_millis(200));
    master = Kill(spawn_rudis_listening(&margs, mp));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    for db in ["0", "5"] {
        assert_eq!(resp_cmd_all(&mut r, &["SELECT", db]), "+OK\r\n");
        while resp_cmd_all(&mut r, &["DBSIZE"]) != ":0\r\n" {
            assert!(
                std::time::Instant::now() < deadline,
                "the replica keeps the keys that expired in DB {db}"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
    let stats = resp_cmd_all(&mut connect(mp), &["INFO", "stats"]);
    assert!(stats.contains("sync_partial_ok:1"), "{stats}");
    drop(master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A replica in a dual-channel full sync buffers the stream itself only up
/// to its `client-output-buffer-limit replica` hard limit; then it stops
/// reading and the rest waits at the master (whose limit is larger here).
/// The sync still completes at once, with all the data.
#[test]
fn test_dual_channel_replica_buffer_stops_at_its_limit_e2e() {
    let (mp, rp) = (29994u16, 29995u16);
    let (ms, rs) = (mp.to_string(), rp.to_string());
    let dir = std::env::temp_dir().join(format!("rudis-duallim-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let rlog = dir.join("replica.log");
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dual = ["--dual-channel-replication-enabled", "yes"];
    let common = ["--threads", "4", "--no-pin", "--save", ""];
    let mut margs = vec!["--port", ms.as_str()];
    margs.extend(common);
    margs.extend(dual);
    let _master = Kill(spawn_rudis_listening(&margs, mp));
    let replica = rudis_bin()
        .args(["--port", rs.as_str()])
        .args(common)
        .args(dual)
        .args(DEBUG_ON)
        .args(["--loglevel", "notice"])
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::fs::File::create(&rlog).unwrap())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let _replica = Kill(replica);
    let connect = |port: u16| {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                return s;
            }
            assert!(std::time::Instant::now() < deadline, "no server on {port}");
            thread::sleep(Duration::from_millis(50));
        }
    };
    let mut m = connect(mp);
    let mut r = connect(rp);
    assert_eq!(
        resp_cmd_all(&mut m, &["DEBUG", "POPULATE", "300000", "key", "256"]),
        "+OK\r\n"
    );
    // The replica holds at most 1 MB itself; the master up to 256 MB.
    assert_eq!(
        resp_cmd_all(
            &mut r,
            &[
                "CONFIG",
                "SET",
                "client-output-buffer-limit",
                "replica 1mb 0 0"
            ]
        ),
        "+OK\r\n"
    );
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut w = connect(mp);
            let val = "x".repeat(512);
            let mut n = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut pipe = String::new();
                for i in 0..200 {
                    let k = format!("w:{}", (n + i) % 50_000);
                    pipe.push_str(&format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n${}\r\n{val}\r\n",
                        k.len(),
                        val.len()
                    ));
                }
                w.write_all(pipe.as_bytes()).unwrap();
                let mut got = 0;
                let mut buf = [0u8; 4096];
                while got < 200 * 5 {
                    got += w.read(&mut buf).unwrap();
                }
                n += 200;
                thread::sleep(Duration::from_millis(10));
            }
        })
    };
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        resp_cmd_all(&mut r, &["REPLICAOF", "127.0.0.1", &ms]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !resp_cmd_all(&mut r, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(
            std::time::Instant::now() < deadline,
            "the replica never synced"
        );
        thread::sleep(Duration::from_millis(50));
    }
    thread::sleep(Duration::from_millis(500));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    assert_eq!(resp_cmd_all(&mut m, &["WAIT", "1", "10000"]), ":1\r\n");
    let stats = resp_cmd_all(&mut m, &["INFO", "stats"]);
    assert!(stats.contains("sync_full:1\r\n"), "{stats}");
    assert_eq!(
        resp_cmd_all(&mut r, &["DEBUG", "DIGEST"]),
        resp_cmd_all(&mut m, &["DEBUG", "DIGEST"])
    );
    let log = std::fs::read_to_string(&rlog).unwrap();
    assert!(
        log.contains("Dual-channel sync: buffered"),
        "the replica never stopped at its limit:\n{log}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// `replica-serve-stale-data no`: a replica whose master link is down
/// refuses all but the commands Redis flags `stale` with -MASTERDOWN; with
/// yes (the default) it serves them.
#[test]
fn test_replica_serve_stale_data_e2e() {
    let port = 29984u16;
    let ps = port.to_string();
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _server = Kill(spawn_rudis_listening(
        &["--port", &ps, "--threads", "2", "--no-pin", "--save", ""],
        port,
    ));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut r = |args: &[&str]| resp_cmd_all(&mut c, args);
    assert_eq!(r(&["SET", "k", "v"]), "+OK\r\n");
    assert_eq!(
        r(&["CONFIG", "SET", "replica-serve-stale-data", "no"]),
        "+OK\r\n"
    );
    assert_eq!(
        r(&["CONFIG", "GET", "replica-serve-stale-data"]),
        "*2\r\n$24\r\nreplica-serve-stale-data\r\n$2\r\nno\r\n"
    );
    // A master that isn't there: the link stays down.
    assert_eq!(r(&["REPLICAOF", "127.0.0.1", "1"]), "+OK\r\n");
    let down =
        "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n";
    assert_eq!(r(&["GET", "k"]), down);
    assert_eq!(r(&["PING"]), down);
    assert!(r(&["INFO", "replication"]).contains("master_link_status:down"));
    assert_eq!(
        r(&["CONFIG", "SET", "replica-serve-stale-data", "yes"]),
        "+OK\r\n"
    );
    assert_eq!(r(&["GET", "k"]), "$1\r\nv\r\n");
    assert_eq!(r(&["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
}
