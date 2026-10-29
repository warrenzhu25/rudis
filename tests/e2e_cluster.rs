//! End-to-end tests: cluster (see `tests/common` for the helpers).

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
fn test_cluster_slot_migration_and_redirection() {
    let port = 16381;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis server");

    // 1. SET key and verify it works when slot is stable
    let resp = send_and_read(&mut stream, b"SET local_key value1\r\n");
    assert_eq!(resp, "+OK\r\n");

    let local_slot = rudis::router::key_slot(b"local_key");

    // 2. Set slot to MIGRATING 127.0.0.1:7001
    let resp = send_and_read(
        &mut stream,
        format!(
            "CLUSTER SETSLOT {} MIGRATING 127.0.0.1:7001\r\n",
            local_slot
        )
        .as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");

    // Existing key on migrating slot should still be returned
    let resp = send_and_read(&mut stream, b"GET local_key\r\n");
    assert_eq!(resp, "$6\r\nvalue1\r\n");

    // Non-existing key on migrating slot should return -ASK
    let tagged_missing = "{local_key}missing".to_string();
    assert_eq!(
        rudis::router::key_slot(tagged_missing.as_bytes()),
        local_slot
    );
    let resp = send_and_read(
        &mut stream,
        format!("GET {}\r\n", tagged_missing).as_bytes(),
    );
    assert_eq!(resp, format!("-ASK {} 127.0.0.1:7001\r\n", local_slot));

    // 3. Set slot to IMPORTING 127.0.0.1:7000
    let import_slot = 5000;
    let resp = send_and_read(
        &mut stream,
        format!(
            "CLUSTER SETSLOT {} IMPORTING 127.0.0.1:7000\r\n",
            import_slot
        )
        .as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");

    let mut target_key = Vec::new();
    for i in 0..100000 {
        let k = format!("k_{}", i);
        if rudis::router::key_slot(k.as_bytes()) == import_slot {
            target_key = k.into_bytes();
            break;
        }
    }

    // Querying importing slot without ASKING should return -MOVED
    let resp = send_and_read(
        &mut stream,
        format!("GET {}\r\n", String::from_utf8_lossy(&target_key)).as_bytes(),
    );
    assert_eq!(resp, format!("-MOVED {} 127.0.0.1:7000\r\n", import_slot));

    // With ASKING command preceding it:
    let resp = send_and_read(&mut stream, b"ASKING\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(
        &mut stream,
        format!(
            "SET {} imported_val\r\n",
            String::from_utf8_lossy(&target_key)
        )
        .as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");

    // After running, ASKING flag was consumed, next command without ASKING should return -MOVED again
    let resp = send_and_read(
        &mut stream,
        format!("GET {}\r\n", String::from_utf8_lossy(&target_key)).as_bytes(),
    );
    assert_eq!(resp, format!("-MOVED {} 127.0.0.1:7000\r\n", import_slot));

    // Send ASKING again:
    let resp = send_and_read(&mut stream, b"ASKING\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(
        &mut stream,
        format!("GET {}\r\n", String::from_utf8_lossy(&target_key)).as_bytes(),
    );
    assert_eq!(resp, "$12\r\nimported_val\r\n");

    // 4. Set slot to STABLE
    let resp = send_and_read(
        &mut stream,
        format!("CLUSTER SETSLOT {} STABLE\r\n", import_slot).as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(
        &mut stream,
        format!("GET {}\r\n", String::from_utf8_lossy(&target_key)).as_bytes(),
    );
    assert_eq!(resp, "$12\r\nimported_val\r\n");
}

#[test]
fn test_migrate_command_e2e() {
    let port1 = 16382;
    let port2 = 16383;
    start_test_server(port1, 2);
    start_test_server(port2, 2);

    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();

    // 1. Setup keys on server 1
    let resp = send_and_read(&mut stream1, b"SET mig_str hello_world\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut stream1, b"HSET mig_hash f1 v1 f2 v2\r\n");
    assert_eq!(resp, ":2\r\n");

    // 2. Migrate mig_str from server 1 to server 2 with COPY
    let resp = send_and_read(
        &mut stream1,
        format!("MIGRATE 127.0.0.1 {} mig_str 0 5000 COPY\r\n", port2).as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");

    // COPY preserves key on server 1
    let resp = send_and_read(&mut stream1, b"GET mig_str\r\n");
    assert_eq!(resp, "$11\r\nhello_world\r\n");

    // Key now exists on server 2
    let resp = send_and_read(&mut stream2, b"GET mig_str\r\n");
    assert_eq!(resp, "$11\r\nhello_world\r\n");

    // 3. Migrate mig_hash without COPY (should delete from server 1)
    let resp = send_and_read(
        &mut stream1,
        format!("MIGRATE 127.0.0.1 {} mig_hash 0 5000\r\n", port2).as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");

    // Key deleted from server 1
    let resp = send_and_read(&mut stream1, b"EXISTS mig_hash\r\n");
    assert_eq!(resp, ":0\r\n");

    // Hash exists on server 2
    let resp = send_and_read(&mut stream2, b"HGET mig_hash f2\r\n");
    assert_eq!(resp, "$2\r\nv2\r\n");

    // 4. Migrate mig_list from server 1 to server 2
    let resp = send_and_read(&mut stream1, b"RPUSH mig_list e1 e2 e3\r\n");
    assert_eq!(resp, ":3\r\n");
    let resp = send_and_read(
        &mut stream1,
        format!("MIGRATE 127.0.0.1 {} mig_list 0 5000\r\n", port2).as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut stream1, b"EXISTS mig_list\r\n");
    assert_eq!(resp, ":0\r\n");
    let resp = send_and_read(&mut stream2, b"LRANGE mig_list 0 -1\r\n");
    assert_eq!(resp, "*3\r\n$2\r\ne1\r\n$2\r\ne2\r\n$2\r\ne3\r\n");

    // 5. Migrate mig_set from server 1 to server 2
    let resp = send_and_read(&mut stream1, b"SADD mig_set s1 s2\r\n");
    assert_eq!(resp, ":2\r\n");
    let resp = send_and_read(
        &mut stream1,
        format!("MIGRATE 127.0.0.1 {} mig_set 0 5000\r\n", port2).as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut stream1, b"EXISTS mig_set\r\n");
    assert_eq!(resp, ":0\r\n");
    let resp = send_and_read(&mut stream2, b"SCARD mig_set\r\n");
    assert_eq!(resp, ":2\r\n");

    // 6. Migrate mig_zset from server 1 to server 2
    let resp = send_and_read(&mut stream1, b"ZADD mig_zset 1.5 item1 2.5 item2\r\n");
    assert_eq!(resp, ":2\r\n");
    let resp = send_and_read(
        &mut stream1,
        format!("MIGRATE 127.0.0.1 {} mig_zset 0 5000\r\n", port2).as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut stream1, b"EXISTS mig_zset\r\n");
    assert_eq!(resp, ":0\r\n");
    let resp = send_and_read(&mut stream2, b"ZCARD mig_zset\r\n");
    assert_eq!(resp, ":2\r\n");
    let resp = send_and_read(&mut stream2, b"ZSCORE mig_zset item1\r\n");
    assert_eq!(resp, "$3\r\n1.5\r\n");

    // 7. Test MIGRATE on non-existing key returns +NOKEY
    let resp = send_and_read(
        &mut stream1,
        format!("MIGRATE 127.0.0.1 {} non_existing_key 0 5000\r\n", port2).as_bytes(),
    );
    assert_eq!(resp, "+NOKEY\r\n");
}

#[test]
fn test_cluster_gossip_and_meet_e2e() {
    let port1 = 16399;
    let port2 = 16400;
    start_test_server(port1, 2);
    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);

    let mut client1 = TcpStream::connect(("127.0.0.1", port1)).unwrap();
    let mut client2 = TcpStream::connect(("127.0.0.1", port2)).unwrap();

    // 1. CLUSTER MYID
    let myid1 = send_and_read(&mut client1, b"CLUSTER MYID\r\n");
    assert!(myid1.starts_with('$'), "MYID should be bulk string");

    let myid2 = send_and_read(&mut client2, b"CLUSTER MYID\r\n");
    assert!(myid2.starts_with('$'), "MYID should be bulk string");

    // 2. CLUSTER INFO
    let info = send_and_read(&mut client1, b"CLUSTER INFO\r\n");
    assert!(info.contains("cluster_state:ok"));
    assert!(info.contains("cluster_slots_assigned:16384"));

    // 3. CLUSTER MEET
    let meet_cmd = format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2);
    assert_eq!(send_and_read(&mut client1, meet_cmd.as_bytes()), "+OK\r\n");

    // 4. CLUSTER NODES shows local and met remote node
    let nodes = send_and_read(&mut client1, b"CLUSTER NODES\r\n");
    assert!(
        nodes.contains("myself,master"),
        "Should show myself as master"
    );
    assert!(
        nodes.contains(&format!("127.0.0.1:{}", port2)),
        "Should list the met remote node"
    );
}

#[test]
fn test_cluster_migrate_slot_e2e() {
    let port1 = 18230;
    let port2 = 18231;
    start_test_server(port1, 2);
    start_test_server(port2, 2);

    let mut client1 = TcpStream::connect(("127.0.0.1", port1)).unwrap();
    let mut client2 = TcpStream::connect(("127.0.0.1", port2)).unwrap();

    // Write a key on server1
    let key = "migrated_key";
    let slot = rudis::router::key_slot(key.as_bytes());
    assert_eq!(
        send_and_read(
            &mut client1,
            format!("SET {} \"transferred\"\r\n", key).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, format!("GET {}\r\n", key).as_bytes()),
        "$11\r\ntransferred\r\n"
    );

    let mut c1_conns: Vec<TcpStream> = (0..8)
        .map(|_| TcpStream::connect(("127.0.0.1", port1)).unwrap())
        .collect();
    let mut c2_conns: Vec<TcpStream> = (0..8)
        .map(|_| TcpStream::connect(("127.0.0.1", port2)).unwrap())
        .collect();

    // Migrate this slot to server2
    let migrate_cmd = format!("CLUSTER MIGRATE-SLOT {} 127.0.0.1 {}\r\n", slot, port2);
    assert_eq!(
        send_and_read(&mut client1, migrate_cmd.as_bytes()),
        "+OK\r\n"
    );

    // Every shard on server 1 must redirect with MOVED immediately after +OK,
    // and every shard on server 2 must serve the migrated key.
    for c in &mut c1_conns {
        let moved_resp = send_and_read(c, format!("GET {}\r\n", key).as_bytes());
        assert_eq!(
            moved_resp,
            format!("-MOVED {} 127.0.0.1:{}\r\n", slot, port2)
        );
    }
    for c in &mut c2_conns {
        assert_eq!(
            send_and_read(c, format!("GET {}\r\n", key).as_bytes()),
            "$11\r\ntransferred\r\n"
        );
    }
    assert_eq!(
        send_and_read(&mut client2, format!("GET {}\r\n", key).as_bytes()),
        "$11\r\ntransferred\r\n"
    );

    // Test CLUSTER REBALANCE
    let rebal_resp = send_and_read(
        &mut client1,
        format!("CLUSTER REBALANCE 127.0.0.1 {} 2\r\n", port2).as_bytes(),
    );
    assert_eq!(rebal_resp, ":2\r\n");
}

#[test]
fn test_cluster_bus_gossip_failover_e2e() {
    let port1 = 16440;
    let port2 = 16441;
    let port3 = 16442;

    start_test_server(port1, 2);

    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);
    start_test_server(port3, 2);
    rudis::cluster::start_cluster_bus(port3);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();
    let mut c3 = TcpStream::connect(format!("127.0.0.1:{}", port3)).unwrap();

    // 1. Verify Cluster Bus listeners on port + 10000 are active
    let bus_stream1 = TcpStream::connect(format!("127.0.0.1:{}", port1 + 10000));
    assert!(
        bus_stream1.is_ok(),
        "Cluster bus on port {} should be listening",
        port1 + 10000
    );
    drop(bus_stream1);

    // 2. Initial CLUSTER MYID & INFO
    let myid1_resp = send_and_read(&mut c1, b"CLUSTER MYID\r\n");
    let myid1 = myid1_resp
        .trim_start_matches('$')
        .split("\r\n")
        .nth(1)
        .unwrap()
        .to_string();
    assert_eq!(myid1.len(), 40);

    let myid2_resp = send_and_read(&mut c2, b"CLUSTER MYID\r\n");
    let myid2 = myid2_resp
        .trim_start_matches('$')
        .split("\r\n")
        .nth(1)
        .unwrap()
        .to_string();
    assert_eq!(myid2.len(), 40);

    let myid3_resp = send_and_read(&mut c3, b"CLUSTER MYID\r\n");
    let myid3 = myid3_resp
        .trim_start_matches('$')
        .split("\r\n")
        .nth(1)
        .unwrap()
        .to_string();
    assert_eq!(myid3.len(), 40);

    // 3. CLUSTER MEET: Node 1 meets Node 2, Node 2 meets Node 3
    assert_eq!(
        send_and_read(
            &mut c1,
            format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut c2,
            format!("CLUSTER MEET 127.0.0.1 {}\r\n", port3).as_bytes()
        ),
        "+OK\r\n"
    );

    // Wait for cluster bus gossip heartbeats to propagate transitively
    let mut nodes1 = String::new();
    for _ in 0..40 {
        nodes1 = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
        if nodes1.contains(&format!("127.0.0.1:{}@{}", port2, port2 + 10000))
            && (nodes1.contains(&myid3) || nodes1.contains(&format!("127.0.0.1:{}", port3)))
        {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Verify Node 1 cluster nodes shows cport (26440, 26441)
    assert!(
        nodes1.contains("myself,master"),
        "Node 1 should be myself,master"
    );
    assert!(
        nodes1.contains(&format!("127.0.0.1:{}@{}", port2, port2 + 10000)),
        "Node 1 should have met Node 2 with cport"
    );

    // Verify transitive gossip: Node 1 should discover Node 3
    assert!(
        nodes1.contains(&myid3) || nodes1.contains(&format!("127.0.0.1:{}", port3)),
        "Node 1 should discover Node 3 transitively through gossip! Nodes:\n{}",
        nodes1
    );

    // 4. Test CLUSTER REPLICATE (like Redis, only a node without slots may
    // become a replica: its keys would be replaced by the master's)
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"),
        "+OK\r\n"
    );
    let rep_resp = send_and_read(
        &mut c2,
        format!("CLUSTER REPLICATE {}\r\n", myid1).as_bytes(),
    );
    assert_eq!(rep_resp, "+OK\r\n");

    let mut nodes2_after_rep = String::new();
    for _ in 0..30 {
        nodes2_after_rep = send_and_read(&mut c2, b"CLUSTER NODES\r\n");
        if nodes2_after_rep.contains("myself,slave") && nodes2_after_rep.contains(&myid1) {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        nodes2_after_rep.contains("myself,slave"),
        "Node 2 should be myself,slave"
    );
    assert!(
        nodes2_after_rep.contains(&myid1),
        "Node 2 should list Node 1 as its master"
    );

    // 5. Test CLUSTER FAILOVER
    wait_for_master_link(&mut c2);
    let failover_resp = send_and_read(&mut c2, b"CLUSTER FAILOVER\r\n");
    assert_eq!(failover_resp, "+OK\r\n");

    let mut nodes2_after_failover = String::new();
    for _ in 0..30 {
        nodes2_after_failover = send_and_read(&mut c2, b"CLUSTER NODES\r\n");
        if nodes2_after_failover.contains("myself,master") {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        nodes2_after_failover.contains("myself,master"),
        "Node 2 should be promoted to myself,master after failover"
    );

    let info2 = send_and_read(&mut c2, b"CLUSTER INFO\r\n");
    assert!(info2.contains("cluster_state:ok"));
    assert!(
        info2.contains("cluster_current_epoch:2") || info2.contains("cluster_my_epoch:2"),
        "Epoch should increment after failover. Info: {}",
        info2
    );

    // 6. Test CLUSTER FORGET
    assert_eq!(
        send_and_read(&mut c1, format!("CLUSTER FORGET {}\r\n", myid3).as_bytes()),
        "+OK\r\n"
    );
    let mut nodes1_after_forget = String::new();
    for _ in 0..30 {
        nodes1_after_forget = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
        if !nodes1_after_forget.contains(&myid3) {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        !nodes1_after_forget.contains(&myid3),
        "Node 3 should be forgotten from Node 1"
    );

    // 7. Test CLUSTER RESET HARD
    assert_eq!(send_and_read(&mut c3, b"CLUSTER RESET HARD\r\n"), "+OK\r\n");
    let info3_reset = send_and_read(&mut c3, b"CLUSTER INFO\r\n");
    assert!(
        info3_reset.contains("cluster_known_nodes:1"),
        "Reset node should only know itself"
    );
    assert!(
        info3_reset.contains("cluster_current_epoch:1"),
        "Current epoch should reset to 1"
    );
}

#[test]
fn test_cluster_bus_shards_and_automated_failover_e2e() {
    let port1 = 16610;
    let port2 = 16611;
    let port3 = 16612;

    start_test_server(port1, 2);

    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);
    start_test_server(port3, 2);
    rudis::cluster::start_cluster_bus(port3);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();
    let mut c3 = TcpStream::connect(format!("127.0.0.1:{}", port3)).unwrap();

    // 1. Cluster Slot Mutations: ADDSLOTS, DELSLOTS, ADDSLOTSRANGE, DELSLOTSRANGE
    // Node 1 clears all slots then adds 0..=8191
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER ADDSLOTSRANGE 0 8191\r\n"),
        "+OK\r\n"
    );

    // Test DELSLOTS and ADDSLOTS on Node 1
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER DELSLOTS 100\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER ADDSLOTS 100\r\n"),
        "+OK\r\n"
    );

    // Node 2 clears all slots then adds 8192..=16383
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER ADDSLOTSRANGE 8192 16383\r\n"),
        "+OK\r\n"
    );

    // Node 3 clears slots (will act as replica)
    assert_eq!(
        send_and_read(&mut c3, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"),
        "+OK\r\n"
    );

    // 2. Fetch Node IDs
    let myid1_resp = send_and_read(&mut c1, b"CLUSTER MYID\r\n");
    let _myid1 = myid1_resp
        .trim_start_matches('$')
        .split("\r\n")
        .nth(1)
        .unwrap()
        .to_string();

    let myid2_resp = send_and_read(&mut c2, b"CLUSTER MYID\r\n");
    let myid2 = myid2_resp
        .trim_start_matches('$')
        .split("\r\n")
        .nth(1)
        .unwrap()
        .to_string();

    let myid3_resp = send_and_read(&mut c3, b"CLUSTER MYID\r\n");
    let myid3 = myid3_resp
        .trim_start_matches('$')
        .split("\r\n")
        .nth(1)
        .unwrap()
        .to_string();

    // 3. CLUSTER MEET: Node 1 meets Node 2, Node 2 meets Node 3
    assert_eq!(
        send_and_read(
            &mut c1,
            format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut c2,
            format!("CLUSTER MEET 127.0.0.1 {}\r\n", port3).as_bytes()
        ),
        "+OK\r\n"
    );

    // Wait for gossip tick & slot exchange over cluster bus
    let mut slots_resp1 = String::new();
    for _ in 0..40 {
        slots_resp1 = send_and_read(&mut c1, b"CLUSTER SLOTS\r\n");
        if slots_resp1.contains(":0\r\n:8191\r\n") && slots_resp1.contains(":8192\r\n:16383\r\n") {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        slots_resp1.contains(":0\r\n:8191\r\n"),
        "Node 1 should report range 0-8191. Resp: {}",
        slots_resp1
    );
    assert!(
        slots_resp1.contains(":8192\r\n:16383\r\n"),
        "Node 1 should report peer range 8192-16383. Resp: {}",
        slots_resp1
    );

    // 5. Test CLUSTER SHARDS (Redis 7 specification)
    let shards_resp = send_and_read(&mut c1, b"CLUSTER SHARDS\r\n");
    assert!(
        shards_resp.contains("slots"),
        "Shards output should contain 'slots'. Resp: {}",
        shards_resp
    );
    assert!(
        shards_resp.contains("nodes"),
        "Shards output should contain 'nodes'. Resp: {}",
        shards_resp
    );
    assert!(
        shards_resp.contains("endpoint"),
        "Shards output should contain 'endpoint'. Resp: {}",
        shards_resp
    );
    assert!(
        shards_resp.contains("health"),
        "Shards output should contain 'health'. Resp: {}",
        shards_resp
    );
    assert!(
        shards_resp.contains("online"),
        "Shards output should contain 'online'. Resp: {}",
        shards_resp
    );

    // 6. Test CLUSTER LINKS telemetry
    let links_resp = send_and_read(&mut c1, b"CLUSTER LINKS\r\n");
    assert!(
        links_resp.contains("direction"),
        "Links output should contain 'direction'. Resp: {}",
        links_resp
    );
    assert!(
        links_resp.contains("to") || links_resp.contains("from"),
        "Links output should contain 'to' or 'from'. Resp: {}",
        links_resp
    );
    assert!(
        links_resp.contains("events"),
        "Links output should contain 'events'. Resp: {}",
        links_resp
    );

    // 7. Test Dynamic -MOVED Redirection:
    let slot_foo = rudis::router::key_slot(b"foo");
    assert!(slot_foo >= 8192, "foo slot should be >= 8192");

    // When sent to Node 1, it should return -MOVED <slot_foo> 127.0.0.1:16611
    let moved_resp = send_and_read(&mut c1, b"SET foo bar\r\n");
    assert_eq!(
        moved_resp,
        format!("-MOVED {} 127.0.0.1:{}\r\n", slot_foo, port2)
    );

    // When sent to Node 2 (the owner), it should succeed with +OK
    let ok_resp = send_and_read(&mut c2, b"SET foo bar\r\n");
    assert_eq!(ok_resp, "+OK\r\n");

    // Find a key belonging to slot < 8192 owned by Node 1
    let mut low_key = String::new();
    let mut low_slot = 0;
    for i in 0..1000 {
        let k = format!("k_{}", i);
        let s = rudis::router::key_slot(k.as_bytes());
        if s < 8192 {
            low_key = k;
            low_slot = s;
            break;
        }
    }
    // Sending low_key to Node 2 should redirect to Node 1
    let moved_low = send_and_read(&mut c2, format!("SET {} myval\r\n", low_key).as_bytes());
    assert_eq!(
        moved_low,
        format!("-MOVED {} 127.0.0.1:{}\r\n", low_slot, port1)
    );

    // 8. Test Consensus-based Failover:
    // Node 3 replicates Node 2
    assert_eq!(
        send_and_read(
            &mut c3,
            format!("CLUSTER REPLICATE {}\r\n", myid2).as_bytes()
        ),
        "+OK\r\n"
    );

    // Verify FAILOVER_AUTH_REQUEST vote handling directly on cluster bus
    let mut bus_client = TcpStream::connect(format!("127.0.0.1:{}", port1 + 10000)).unwrap();
    // First, request vote without master being failed -> should reject
    bus_client
        .write_all(format!("FAILOVER_AUTH_REQUEST {} 10 {}\r\n", myid3, myid2).as_bytes())
        .unwrap();
    let mut vbuf = [0u8; 128];
    let n = bus_client.read(&mut vbuf).unwrap();
    let vote_resp = String::from_utf8_lossy(&vbuf[..n]);
    assert!(
        vote_resp.contains("ERR vote rejected"),
        "Should reject vote if master is not failed"
    );

    // Now mark Node 2 as fail on Node 1 via CLUSTER bus FAIL message
    bus_client
        .write_all(format!("FAIL {}\r\n", myid2).as_bytes())
        .unwrap();
    let n = bus_client.read(&mut vbuf).unwrap();
    assert_eq!(&vbuf[..n], b"+OK\r\n");

    // Now request vote again with epoch 11 -> should grant ACK!
    bus_client
        .write_all(format!("FAILOVER_AUTH_REQUEST {} 11 {}\r\n", myid3, myid2).as_bytes())
        .unwrap();
    let n = bus_client.read(&mut vbuf).unwrap();
    let vote_ack = String::from_utf8_lossy(&vbuf[..n]);
    assert!(
        vote_ack.starts_with("+FAILOVER_AUTH_ACK"),
        "Master Node 1 should grant vote ACK to Node 3. Resp: {}",
        vote_ack
    );

    // Duplicate vote in same epoch should be rejected
    bus_client
        .write_all(format!("FAILOVER_AUTH_REQUEST {} 11 {}\r\n", myid3, myid2).as_bytes())
        .unwrap();
    let n = bus_client.read(&mut vbuf).unwrap();
    let dup_vote = String::from_utf8_lossy(&vbuf[..n]);
    assert!(
        dup_vote.contains("ERR vote rejected"),
        "Duplicate vote in same epoch should be rejected"
    );

    // Trigger failover on Node 3
    wait_for_master_link(&mut c3);
    assert_eq!(send_and_read(&mut c3, b"CLUSTER FAILOVER\r\n"), "+OK\r\n");
    let mut nodes3 = String::new();
    for _ in 0..30 {
        nodes3 = send_and_read(&mut c3, b"CLUSTER NODES\r\n");
        if nodes3.contains("myself,master") {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        nodes3.contains("myself,master"),
        "Node 3 should be promoted to myself,master. Nodes:\n{}",
        nodes3
    );
}

#[test]
fn test_cluster_pipelined_squashed_moved_redirect_e2e() {
    let port1 = 16660;
    let port2 = 16661;

    start_test_server(port1, 2);

    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();

    // Node 1: slots 0..=8191
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER ADDSLOTSRANGE 0 8191\r\n"),
        "+OK\r\n"
    );

    // Node 2: slots 8192..=16383
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER ADDSLOTSRANGE 8192 16383\r\n"),
        "+OK\r\n"
    );

    // Node 1 meets Node 2
    assert_eq!(
        send_and_read(
            &mut c1,
            format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2).as_bytes()
        ),
        "+OK\r\n"
    );

    // Wait for gossip tick & slot exchange
    for _ in 0..40 {
        let slots = send_and_read(&mut c1, b"CLUSTER SLOTS\r\n");
        if slots.contains(":8192\r\n:16383\r\n") {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Find a key belonging to Node 1 (slot < 8192) and one to Node 2 (slot >= 8192)
    let mut key_node1 = String::new();
    let mut key_node2 = String::new();
    let mut slot_node2 = 0;

    for i in 0..1000 {
        let candidate = format!("test_key_{}", i);
        let s = rudis::router::key_slot(candidate.as_bytes());
        if s < 8192 && key_node1.is_empty() {
            key_node1 = candidate.clone();
        } else if s >= 8192 && key_node2.is_empty() {
            key_node2 = candidate.clone();
            slot_node2 = s;
        }
        if !key_node1.is_empty() && !key_node2.is_empty() {
            break;
        }
    }

    // Connect a fresh client connection to Node 1
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();

    // Send a pipelined batch in a single TCP write:
    // Cmd 1: SET <key_node1> owned_val (should succeed: +OK)
    // Cmd 2: SET <key_node2> unowned_val (should fail: -MOVED <slot_node2> 127.0.0.1:16661)
    let pipeline_req = format!(
        "SET {} owned_val\r\nSET {} unowned_val\r\n",
        key_node1, key_node2
    );
    let resp = send_and_read(&mut client, pipeline_req.as_bytes());

    let expected_moved = format!("-MOVED {} 127.0.0.1:{}\r\n", slot_node2, port2);
    assert_eq!(
        resp,
        format!("+OK\r\n{}", expected_moved),
        "Pipelined squashed commands must return MOVED redirect for unowned slots"
    );

    // Verify key_node1 was actually set on Node 1
    let get_resp = send_and_read(&mut client, format!("GET {}\r\n", key_node1).as_bytes());
    assert_eq!(get_resp, "$9\r\nowned_val\r\n");

    // Also send a pipeline of multiple unowned commands
    let pipeline_unowned = format!("GET {}\r\nDEL {}\r\n", key_node2, key_node2);
    let resp = send_and_read(&mut client, pipeline_unowned.as_bytes());
    assert_eq!(
        resp,
        format!("{}{}", expected_moved, expected_moved),
        "All commands for unowned slots in a squashed pipeline must return MOVED redirect"
    );
}

#[test]
fn test_cluster_mode_moved_redirection_and_per_shard_ports_e2e() {
    let base_port = 17540;
    start_test_server_cluster(base_port, 4);

    // 1. Connect to Shard 0 (base_port)
    let mut client0 = TcpStream::connect(format!("127.0.0.1:{}", base_port)).unwrap();

    // 2. Query CLUSTER SLOTS and verify per-shard ports
    let slots_resp = send_and_read(&mut client0, b"CLUSTER SLOTS\r\n");
    assert!(slots_resp.starts_with("*4\r\n"));
    assert!(slots_resp.contains(&format!(":{}", base_port)));
    assert!(slots_resp.contains(&format!(":{}", base_port + 1)));
    assert!(slots_resp.contains(&format!(":{}", base_port + 2)));
    assert!(slots_resp.contains(&format!(":{}", base_port + 3)));

    // 3. Find a key belonging to Shard 0 (slots 0..4095) and Shard 1 (slots 4096..8191)
    let mut key_shard0 = None;
    let mut key_shard1 = None;
    for i in 0..2000 {
        let k = format!("test_ck_{}", i);
        let slot = rudis::router::key_slot(k.as_bytes());
        let shard = rudis::router::slot_to_shard(slot, 4);
        if shard == 0 && key_shard0.is_none() {
            key_shard0 = Some((k, slot));
        } else if shard == 1 && key_shard1.is_none() {
            key_shard1 = Some((k, slot));
        }
        if key_shard0.is_some() && key_shard1.is_some() {
            break;
        }
    }

    let (k0, _slot0) = key_shard0.unwrap();
    let (k1, slot1) = key_shard1.unwrap();

    // 4. Local key on Shard 0 succeeds directly
    let resp = send_and_read(&mut client0, format!("SET {} val0\r\n", k0).as_bytes());
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut client0, format!("GET {}\r\n", k0).as_bytes());
    assert_eq!(resp, "$4\r\nval0\r\n");

    // 5. Remote key on Shard 0 returns -MOVED pointing to Shard 1 port
    let resp = send_and_read(&mut client0, format!("GET {}\r\n", k1).as_bytes());
    assert_eq!(
        resp,
        format!("-MOVED {} 127.0.0.1:{}\r\n", slot1, base_port + 1)
    );

    // 6. Connect directly to Shard 1 (base_port + 1) and execute the key directly
    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", base_port + 1)).unwrap();
    let resp = send_and_read(&mut client1, format!("SET {} val1\r\n", k1).as_bytes());
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut client1, format!("GET {}\r\n", k1).as_bytes());
    assert_eq!(resp, "$4\r\nval1\r\n");

    // 7. Verify CLUSTER NODES shows all 4 shards
    let nodes_resp = send_and_read(&mut client0, b"CLUSTER NODES\r\n");
    assert!(nodes_resp.contains("myself,master"));
    assert!(nodes_resp.contains(&format!("127.0.0.1:{}", base_port + 1)));
    assert!(nodes_resp.contains(&format!("127.0.0.1:{}", base_port + 2)));
    assert!(nodes_resp.contains(&format!("127.0.0.1:{}", base_port + 3)));

    // 8. Verify CLUSTER SHARDS shows 4 shards with dedicated ports
    let shards_resp = send_and_read(&mut client0, b"CLUSTER SHARDS\r\n");
    assert!(shards_resp.starts_with("*4\r\n"));
    assert!(shards_resp.contains(&format!(":{}", base_port + 1)));
}

#[test]
fn test_multikey_acl_and_crossslot_enforcement_e2e() {
    let port = 16735;
    start_test_server(port, 2);

    let mut admin = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(
        send_and_read(&mut admin, b"SET allowed:1 v1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut admin, b"SET allowed:2 v2\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut admin, b"SET secret:99 secret_value\r\n"),
        "+OK\r\n"
    );

    // Configure user 'restricted' with access only to ~allowed:*
    assert_eq!(
        send_and_read(
            &mut admin,
            b"ACL SETUSER restricted on >secpass +@all ~allowed:*\r\n"
        ),
        "+OK\r\n"
    );

    let mut user = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(
        send_and_read(&mut user, b"AUTH restricted secpass\r\n"),
        "+OK\r\n"
    );

    // Permitted: MGET with all allowed keys
    assert_eq!(
        send_and_read(&mut user, b"MGET allowed:1 allowed:2\r\n"),
        "*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n"
    );

    // Rejected: MGET where first key is allowed, but second key is forbidden
    let mget_resp = send_and_read(&mut user, b"MGET allowed:1 secret:99\r\n");
    assert!(
        mget_resp.starts_with("-NOPERM"),
        "Expected -NOPERM on MGET with forbidden key, got {}",
        mget_resp
    );

    // Rejected: DEL where first key is allowed, but second key is forbidden
    let del_resp = send_and_read(&mut user, b"DEL allowed:1 secret:99\r\n");
    assert!(
        del_resp.starts_with("-NOPERM"),
        "Expected -NOPERM on DEL with forbidden key, got {}",
        del_resp
    );

    // Rejected: MSET where second key is forbidden
    let mset_resp = send_and_read(&mut user, b"MSET allowed:1 new1 secret:99 newsecret\r\n");
    assert!(
        mset_resp.starts_with("-NOPERM"),
        "Expected -NOPERM on MSET with forbidden key, got {}",
        mset_resp
    );

    // Verify secret:99 was never modified
    assert_eq!(
        send_and_read(&mut admin, b"GET secret:99\r\n"),
        "$12\r\nsecret_value\r\n"
    );
}

#[test]
fn test_cluster_quorum_failure_detection_and_gossip_e2e() {
    let port1 = 16762;
    let port2 = 16763;
    let port3 = 16764;

    start_test_server(port1, 2);

    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);
    start_test_server(port3, 2);
    rudis::cluster::start_cluster_bus(port3);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();
    let mut c3 = TcpStream::connect(format!("127.0.0.1:{}", port3)).unwrap();

    // Meet nodes into 3-node cluster
    assert_eq!(
        send_and_read(
            &mut c1,
            format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut c2,
            format!("CLUSTER MEET 127.0.0.1 {}\r\n", port3).as_bytes()
        ),
        "+OK\r\n"
    );

    // Wait for cluster bus gossip convergence
    for _ in 0..40 {
        let nodes1 = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
        if nodes1.contains(&format!("127.0.0.1:{}", port2))
            && nodes1.contains(&format!("127.0.0.1:{}", port3))
        {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Identify Node 3 ID
    let myid3 = {
        let resp3 = send_and_read(&mut c3, b"CLUSTER MYID\r\n");
        resp3
            .trim_start_matches('$')
            .split("\r\n")
            .nth(1)
            .unwrap()
            .to_string()
    };

    // Before FAIL message, all nodes are connected masters
    let nodes_init = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
    assert!(!nodes_init.contains(&format!("{} fail", myid3)));

    // Connect to Node 1's cluster bus port and broadcast FAIL message for Node 3
    let mut bus_client = TcpStream::connect(format!("127.0.0.1:{}", port1 + 10000)).unwrap();
    bus_client
        .write_all(format!("FAIL {}\r\n", myid3).as_bytes())
        .unwrap();
    let mut vbuf = [0u8; 64];
    let n = bus_client.read(&mut vbuf).unwrap();
    assert_eq!(&vbuf[..n], b"+OK\r\n");

    // Verify Node 1 now flags Node 3 as fail
    let nodes_after_fail = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
    assert!(
        nodes_after_fail.contains(&format!(
            "{} 127.0.0.1:{}@{} master,fail ",
            myid3,
            port3,
            port3 + 10000
        )),
        "Node 3 should be marked fail on Node 1 after FAIL message. Nodes:\n{}",
        nodes_after_fail
    );
}

#[test]
fn test_cluster_setslot_live_migration_and_ask_redirection_e2e() {
    let port1 = 16771;
    let port2 = 16772;
    start_test_server(port1, 2);
    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();

    // 1. Assign slots: Node 1 owns 0-8191, Node 2 owns 8192-16383
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER ADDSLOTS-RANGE 0 8191\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER ADDSLOTS-RANGE 8192 16383\r\n"),
        "+OK\r\n"
    );

    // 2. Connect nodes via MEET
    let meet_cmd = format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2);
    assert_eq!(send_and_read(&mut c1, meet_cmd.as_bytes()), "+OK\r\n");
    thread::sleep(Duration::from_millis(200));

    let myid1 = send_and_read(&mut c1, b"CLUSTER MYID\r\n")
        .trim()
        .replace("$40\r\n", "")
        .replace("\r\n", "");
    let myid2 = send_and_read(&mut c2, b"CLUSTER MYID\r\n")
        .trim()
        .replace("$40\r\n", "")
        .replace("\r\n", "");

    // 3. Find keys for slot (owned by Node 1: slot < 8192)
    let k_existing = "{tag1}k1";
    let k_migrated = "{tag1}k2";
    let slot = rudis::router::key_slot(k_existing.as_bytes());
    assert!(slot < 8192, "Slot {} must be owned by Node 1", slot);

    // Populate k_existing on Node 1
    assert_eq!(
        send_and_read(
            &mut c1,
            format!("SET {} val_exist\r\n", k_existing).as_bytes()
        ),
        "+OK\r\n"
    );

    // 4. Set slot 100 into MIGRATING state on Node 1 and IMPORTING on Node 2
    let setslot_mig = format!("CLUSTER SETSLOT {} MIGRATING {}\r\n", slot, myid2);
    assert_eq!(send_and_read(&mut c1, setslot_mig.as_bytes()), "+OK\r\n");

    let setslot_imp = format!("CLUSTER SETSLOT {} IMPORTING {}\r\n", slot, myid1);
    assert_eq!(send_and_read(&mut c2, setslot_imp.as_bytes()), "+OK\r\n");

    // Verify CLUSTER NODES on Node 1 shows [100->-<myid2>]
    let nodes1 = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
    assert!(
        nodes1.contains(&format!("[{}->-{}]", slot, myid2)),
        "Node 1 should show migrating slot in CLUSTER NODES:\n{}",
        nodes1
    );

    // 5. Querying k_existing from Node 1 succeeds locally
    assert_eq!(
        send_and_read(&mut c1, format!("GET {}\r\n", k_existing).as_bytes()),
        "$9\r\nval_exist\r\n"
    );

    // 6. Querying non-existent k_migrated from Node 1 triggers -ASK redirection to Node 2!
    let ask_resp = send_and_read(&mut c1, format!("GET {}\r\n", k_migrated).as_bytes());
    assert!(
        ask_resp.starts_with(&format!("-ASK {} 127.0.0.1:{}", slot, port2)),
        "Expected -ASK redirect to Node 2, got: {}",
        ask_resp
    );

    // 7. Querying k_migrated on Node 2 without ASKING is rejected with -MOVED
    let moved_resp = send_and_read(&mut c2, format!("GET {}\r\n", k_migrated).as_bytes());
    assert!(
        moved_resp.starts_with(&format!("-MOVED {}", slot)),
        "Expected -MOVED redirect back without ASKING, got: {}",
        moved_resp
    );

    // 8. Following -ASK protocol: sending ASKING followed by command succeeds on Node 2!
    assert_eq!(send_and_read(&mut c2, b"ASKING\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(
            &mut c2,
            format!("SET {} val_new\r\n", k_migrated).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut c2, b"ASKING\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut c2, format!("GET {}\r\n", k_migrated).as_bytes()),
        "$7\r\nval_new\r\n"
    );

    // 9. Finalize migration: CLUSTER SETSLOT <slot> NODE
    assert_eq!(
        send_and_read(
            &mut c2,
            format!("CLUSTER SETSLOT {} NODE myself\r\n", slot).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut c1,
            format!("CLUSTER SETSLOT {} NODE {}\r\n", slot, myid2).as_bytes()
        ),
        "+OK\r\n"
    );

    // 10. After migration, querying Node 2 succeeds directly without ASKING
    assert_eq!(
        send_and_read(&mut c2, format!("GET {}\r\n", k_migrated).as_bytes()),
        "$7\r\nval_new\r\n"
    );

    // And querying Node 1 redirects with -MOVED permanently to Node 2
    let perm_moved = send_and_read(&mut c1, format!("GET {}\r\n", k_migrated).as_bytes());
    assert!(
        perm_moved.starts_with(&format!("-MOVED {} 127.0.0.1:{}", slot, port2)),
        "Expected permanent -MOVED redirect, got: {}",
        perm_moved
    );
}

#[test]
fn test_cluster_check_and_rebalance_live_migration_e2e() {
    let port1 = 16776;
    let port2 = 16777;
    start_test_server(port1, 2);
    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();
    c1.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(3))).unwrap();

    // 1. Assign slots: Node 1 initially owns 0-12000, Node 2 owns 12001-16383 (imbalanced)
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER ADDSLOTS-RANGE 0 12000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER ADDSLOTS-RANGE 12001 16383\r\n"),
        "+OK\r\n"
    );

    // 2. Connect nodes via MEET
    let meet_cmd = format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2);
    assert_eq!(send_and_read(&mut c1, meet_cmd.as_bytes()), "+OK\r\n");

    let myid1 = send_and_read(&mut c1, b"CLUSTER MYID\r\n")
        .trim()
        .replace("$40\r\n", "")
        .replace("\r\n", "");
    let myid2 = send_and_read(&mut c2, b"CLUSTER MYID\r\n")
        .trim()
        .replace("$40\r\n", "")
        .replace("\r\n", "");

    // 3. CLUSTER CHECK on Node 1: verifies all 16384 slots are covered across the 2 nodes.
    // MEET's handshake is bounded by short timeouts; if it misses under load,
    // Node 1 learns Node 2's slots from the next bus tick, so poll (bounded).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let check1 = loop {
        let check = send_and_read(&mut c1, b"CLUSTER CHECK\r\n");
        if check.contains("[OK] All 16384 slots covered") || std::time::Instant::now() > deadline {
            break check;
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert!(
        check1.contains("[OK] All 16384 slots covered"),
        "{}",
        check1
    );

    // 4. CLUSTER REBALANCE SIMULATE: preview rebalancing plan without moving slots
    let sim_resp = send_and_read(&mut c1, b"CLUSTER REBALANCE SIMULATE\r\n");
    assert!(sim_resp.contains("Moving slot"));
    assert!(sim_resp.contains(&myid1));
    assert!(sim_resp.contains(&myid2));

    // 5. CLUSTER RESHARD: move 10 slots from Node 1 to Node 2
    let reshard_cmd = format!("CLUSTER RESHARD {} {} 10\r\n", myid2, myid1);
    let reshard_resp = send_and_read(&mut c1, reshard_cmd.as_bytes());
    assert_eq!(reshard_resp, ":10\r\n");

    // 6. Targeted CLUSTER REBALANCE: move 5 slots to Node 2
    let rebalance_cmd = format!("CLUSTER REBALANCE 127.0.0.1 {} 5\r\n", port2);
    let rebalance_resp = send_and_read(&mut c1, rebalance_cmd.as_bytes());
    assert_eq!(rebalance_resp, ":5\r\n");

    // 7. Verify CLUSTER CHECK is healthy after migrations (poll with bounded deadline for gossip sync)
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let check_after = loop {
        let check = send_and_read(&mut c1, b"CLUSTER CHECK\r\n");
        if check.contains("[OK]") || std::time::Instant::now() > deadline {
            break check;
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert!(check_after.contains("[OK]"), "{}", check_after);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let check_c2 = loop {
        let check = send_and_read(&mut c2, b"CLUSTER CHECK\r\n");
        if check.contains("[OK]") || std::time::Instant::now() > deadline {
            break check;
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert!(check_c2.contains("[OK]"), "{}", check_c2);
}

#[test]
fn test_sharded_pubsub_slot_bound_e2e() {
    let port = 17025;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut sub1 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let resp1 = send_and_read(&mut sub1, b"SSUBSCRIBE shard:orders:1\r\n");
    assert_eq!(
        resp1,
        "*3\r\n$10\r\nssubscribe\r\n$14\r\nshard:orders:1\r\n:1\r\n"
    );

    let mut sub2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let resp2 = send_and_read(&mut sub2, b"SSUBSCRIBE shard:orders:1 shard:orders:2\r\n");
    assert_eq!(
        resp2,
        "*3\r\n$10\r\nssubscribe\r\n$14\r\nshard:orders:1\r\n:1\r\n*3\r\n$10\r\nssubscribe\r\n$14\r\nshard:orders:2\r\n:2\r\n"
    );

    let mut pub_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. SPUBLISH to shard:orders:1 should reach both subscribers (sub1 and sub2)
    let resp = send_and_read(
        &mut pub_client,
        b"SPUBLISH shard:orders:1 payload_alpha\r\n",
    );
    assert_eq!(resp, ":2\r\n");

    let mut buf = [0u8; 512];
    let n1 = sub1.read(&mut buf).unwrap();
    assert_eq!(
        &buf[..n1],
        b"*3\r\n$8\r\nsmessage\r\n$14\r\nshard:orders:1\r\n$13\r\npayload_alpha\r\n"
    );

    let n2 = sub2.read(&mut buf).unwrap();
    assert_eq!(
        &buf[..n2],
        b"*3\r\n$8\r\nsmessage\r\n$14\r\nshard:orders:1\r\n$13\r\npayload_alpha\r\n"
    );

    // 2. SPUBLISH to shard:orders:2 should only reach sub2
    let resp = send_and_read(&mut pub_client, b"SPUBLISH shard:orders:2 payload_beta\r\n");
    assert_eq!(resp, ":1\r\n");

    let n2 = sub2.read(&mut buf).unwrap();
    assert_eq!(
        &buf[..n2],
        b"*3\r\n$8\r\nsmessage\r\n$14\r\nshard:orders:2\r\n$12\r\npayload_beta\r\n"
    );

    // 3. SPUBLISH to un-subscribed channel returns 0
    let resp = send_and_read(&mut pub_client, b"SPUBLISH shard:unsubscribed none\r\n");
    assert_eq!(resp, ":0\r\n");

    // 4. PUBSUB SHARDCHANNELS
    let channels_resp = send_and_read(&mut pub_client, b"PUBSUB SHARDCHANNELS\r\n");
    assert!(channels_resp.contains("shard:orders:1"));
    assert!(channels_resp.contains("shard:orders:2"));

    // 5. PUBSUB SHARDNUMSUB
    let numsub_resp = send_and_read(
        &mut pub_client,
        b"PUBSUB SHARDNUMSUB shard:orders:1 shard:orders:2 shard:non_existent\r\n",
    );
    assert_eq!(
        numsub_resp,
        "*6\r\n$14\r\nshard:orders:1\r\n:2\r\n$14\r\nshard:orders:2\r\n:1\r\n$18\r\nshard:non_existent\r\n:0\r\n"
    );

    // 6. Subscribed mode restrictions: sub1 cannot execute GET or SET
    let err_resp = send_and_read(&mut sub1, b"GET key\r\n");
    assert!(
        err_resp.contains("only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"),
        "unexpected: {err_resp}"
    );

    let pong_resp = send_and_read(&mut sub1, b"PING\r\n");
    assert_eq!(pong_resp, "*2\r\n$4\r\npong\r\n$0\r\n\r\n");

    // 7. SUNSUBSCRIBE
    let un_resp = send_and_read(&mut sub1, b"SUNSUBSCRIBE shard:orders:1\r\n");
    assert_eq!(
        un_resp,
        "*3\r\n$12\r\nsunsubscribe\r\n$14\r\nshard:orders:1\r\n:0\r\n"
    );

    // Next SPUBLISH to shard:orders:1 only reaches sub2
    let resp = send_and_read(
        &mut pub_client,
        b"SPUBLISH shard:orders:1 payload_gamma\r\n",
    );
    assert_eq!(resp, ":1\r\n");

    let n2 = sub2.read(&mut buf).unwrap();
    assert_eq!(
        &buf[..n2],
        b"*3\r\n$8\r\nsmessage\r\n$14\r\nshard:orders:1\r\n$13\r\npayload_gamma\r\n"
    );
}

#[test]
fn test_cross_shard_xread_cluster_crossslot_e2e() {
    let port = 19129;
    start_test_server_cluster(port, 4);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Pick two keys with different cluster slots
    let mut s1 = String::new();
    let mut s2 = String::new();
    for i in 0..100 {
        let k = format!("cluster_s_{}", i);
        let slot = rudis::router::key_slot(k.as_bytes());
        if s1.is_empty() {
            s1 = k;
        } else if rudis::router::key_slot(s1.as_bytes()) != slot {
            s2 = k;
            break;
        }
    }
    assert_ne!(
        rudis::router::key_slot(s1.as_bytes()),
        rudis::router::key_slot(s2.as_bytes())
    );

    let res = send_and_read(
        &mut stream,
        format!("XREAD STREAMS {} {} 0 0\r\n", s1, s2).as_bytes(),
    );
    assert_eq!(
        res,
        "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
    );

    let res_group = send_and_read(
        &mut stream,
        format!("XREADGROUP GROUP grp c1 STREAMS {} {} > >\r\n", s1, s2).as_bytes(),
    );
    assert_eq!(
        res_group,
        "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
    );
}

#[test]
fn test_cluster_multi_exec_crossslot_e2e() {
    let port = 19135;
    let num_shards = 4;
    start_test_server_cluster(port, num_shards);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Find a hash tag owned by shard 0 (slot < 4096)
    let mut tag = String::new();
    for i in 0..1000 {
        let candidate = format!("tx_{}", i);
        let probe = format!("k{{{}}}", candidate);
        if rudis::router::key_slot(probe.as_bytes()) < 4096 {
            tag = candidate;
            break;
        }
    }

    // 1. Same hash slot succeeds in cluster MULTI/EXEC
    assert_eq!(send_and_read(&mut stream, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("SET k1{{{}}} val1\r\n", tag).as_bytes()
        ),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("SET k2{{{}}} val2\r\n", tag).as_bytes()
        ),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("MGET k1{{{}}} k2{{{}}}\r\n", tag, tag).as_bytes()
        ),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"EXEC\r\n"),
        "*3\r\n+OK\r\n+OK\r\n*2\r\n$4\r\nval1\r\n$4\r\nval2\r\n"
    );

    // 2. Different hash slots across queued commands in MULTI/EXEC returns -CROSSSLOT
    let mut s1 = String::new();
    let mut s2 = String::new();
    for i in 0..1000 {
        let k = format!("ctx_key_{}", i);
        let slot = rudis::router::key_slot(k.as_bytes());
        if slot < 4096 {
            if s1.is_empty() {
                s1 = k;
            } else if rudis::router::key_slot(s1.as_bytes()) != slot {
                s2 = k;
                break;
            }
        }
    }
    assert_eq!(send_and_read(&mut stream, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, format!("SET {} v1\r\n", s1).as_bytes()),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, format!("SET {} v2\r\n", s2).as_bytes()),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"EXEC\r\n"),
        "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, format!("GET {}\r\n", s1).as_bytes()),
        "$-1\r\n"
    );
}

#[test]
fn test_bind_restricts_interfaces_and_standalone_has_no_cluster_bus_e2e() {
    let port = 16991;
    rudis::netsec::set_bind_addrs(port, rudis::netsec::parse_bind_spec("127.0.0.1").unwrap());
    start_test_server(port, 2);

    let mut c = TcpStream::connect(("127.0.0.1", port)).expect("bound address reachable");
    let bind = resp_cmd(&mut c, &["CONFIG", "GET", "bind"]);
    assert!(bind.contains("127.0.0.1"), "CONFIG GET bind: {}", bind);

    // 127.0.0.2 routes to loopback on Linux but is not a bound address.
    assert!(
        TcpStream::connect(("127.0.0.2", port)).is_err(),
        "server must not listen beyond its bind addresses"
    );
    // Standalone mode must not open the cluster bus.
    assert!(
        TcpStream::connect(("127.0.0.1", port + 10000)).is_err(),
        "cluster bus must stay closed when cluster mode is disabled"
    );

    // Runtime rebinding is refused rather than silently accepted.
    let r = resp_cmd(&mut c, &["CONFIG", "SET", "bind", "0.0.0.0"]);
    assert!(r.starts_with("-ERR CONFIG SET failed"), "{}", r);
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "bind", "127.0.0.1"]),
        "+OK\r\n"
    );
    let r = resp_cmd(&mut c, &["CONFIG", "SET", "bind", "999.1.1.1"]);
    assert!(r.contains("Failed to bind"), "{}", r);
}

#[test]
fn test_cluster_shard_ports_share_requirepass_and_users_e2e() {
    let base = 16995;
    start_test_server_cluster(base, 2);
    let shard1 = base + 1;

    let mut admin = TcpStream::connect(("127.0.0.1", base)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(
        resp_cmd(&mut admin, &["CONFIG", "SET", "requirepass", "clusterpw"]),
        "+OK\r\n"
    );

    // Every shard port enforces the server-wide password.
    let mut c = TcpStream::connect(("127.0.0.1", shard1)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert!(resp_cmd(&mut c, &["GET", "k"]).starts_with("-NOAUTH"));
    assert!(resp_cmd(&mut c, &["SUBSCRIBE", "ch"]).starts_with("-NOAUTH"));
    assert_eq!(resp_cmd(&mut c, &["AUTH", "clusterpw"]), "+OK\r\n");

    // Users created via one shard port can log in on another.
    assert_eq!(
        resp_cmd(
            &mut c,
            &["ACL", "SETUSER", "op", "on", ">oppw", "+@all", "~*"]
        ),
        "+OK\r\n"
    );
    let mut c0 = TcpStream::connect(("127.0.0.1", base)).unwrap();
    c0.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert_eq!(resp_cmd(&mut c0, &["AUTH", "op", "oppw"]), "+OK\r\n");
}

#[test]
fn test_migrate_never_drops_keys_the_target_did_not_store_e2e() {
    let (src_port, dst_port) = (17075, 17076);
    let (src_s, dst_s) = (src_port.to_string(), dst_port.to_string());
    let mut src =
        spawn_rudis_listening(&["--port", &src_s, "--threads", "2", "--no-pin"], src_port);
    let mut dst =
        spawn_rudis_listening(&["--port", &dst_s, "--threads", "2", "--no-pin"], dst_port);
    let mut s = TcpStream::connect(("127.0.0.1", src_port)).unwrap();
    let mut d = TcpStream::connect(("127.0.0.1", dst_port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    d.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    assert_eq!(resp_cmd(&mut s, &["HSET", "mg:h", "f", "v"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut s, &["TIER", "SPILL", "mg:h"]), ":1\r\n");
    assert_eq!(
        resp_cmd(&mut s, &["SET", "mg:t", "x", "PX", "100000"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut s, &["SET", "mg:busy", "src"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut d, &["SET", "mg:busy", "dst"]), "+OK\r\n");
    // Keep the tiered key on the tier while it is migrated.
    assert_eq!(
        resp_cmd(&mut s, &["CONFIG", "SET", "maxmemory", "1"]),
        "+OK\r\n"
    );

    // The target already has mg:busy and no REPLACE was given: it rejects
    // that key, which must stay on the source. The others move.
    let migrate = |s: &mut TcpStream, extra: &[&str]| {
        let mut args = vec!["MIGRATE", "127.0.0.1", dst_s.as_str(), "", "0", "5000"];
        args.extend_from_slice(extra);
        resp_cmd(s, &args)
    };
    let reply = migrate(&mut s, &["KEYS", "mg:h", "mg:t", "mg:busy"]);
    assert!(
        reply.starts_with("-ERR Target instance replied with error: BUSYKEY"),
        "{reply:?}"
    );
    assert_eq!(resp_cmd(&mut s, &["EXISTS", "mg:h", "mg:t"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut s, &["GET", "mg:busy"]), "$3\r\nsrc\r\n");
    assert_eq!(resp_cmd(&mut d, &["HGET", "mg:h", "f"]), "$1\r\nv\r\n");
    assert_eq!(resp_cmd(&mut d, &["GET", "mg:t"]), "$1\r\nx\r\n");
    let pttl = resp_cmd(&mut d, &["PTTL", "mg:t"]);
    let pttl: i64 = pttl.trim_start_matches(':').trim_end().parse().unwrap();
    assert!(pttl > 0 && pttl <= 100_000, "{pttl}");
    assert_eq!(resp_cmd(&mut d, &["GET", "mg:busy"]), "$3\r\ndst\r\n");

    assert_eq!(migrate(&mut s, &["REPLACE", "KEYS", "mg:busy"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut s, &["EXISTS", "mg:busy"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut d, &["GET", "mg:busy"]), "$3\r\nsrc\r\n");
    assert_eq!(migrate(&mut s, &["KEYS", "mg:gone"]), "+NOKEY\r\n");

    drop(s);
    drop(d);
    shutdown_and_wait(src_port, &mut src);
    shutdown_and_wait(dst_port, &mut dst);
}

/// A hostile cluster-bus peer gets error replies or a hang-up, never a crash:
/// MEET with a port above 55535 used to overflow `port + 10000` while holding
/// the node-table lock, poisoning it for every later CLUSTER command.
#[test]
fn test_cluster_bus_survives_hostile_peer_e2e() {
    let port: u16 = 17088;
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--cluster-enabled",
        "yes",
    ];
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = KillOnDrop(spawn_rudis_listening(&args, port));
    let bus = || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port + 10000)) {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                return s;
            }
            assert!(std::time::Instant::now() < deadline, "cluster bus not up");
            thread::sleep(Duration::from_millis(50));
        }
    };
    let line = |s: &mut TcpStream, frame: &str| -> String {
        s.write_all(frame.as_bytes()).unwrap();
        let mut buf = [0u8; 4096];
        let n = s.read(&mut buf).unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    };

    let mut b = bus();
    assert_eq!(
        line(&mut b, "MEET 127.0.0.1 65535 evil 1 0-1\r\n"),
        "-ERR invalid port\r\n"
    );
    assert!(line(&mut b, "MEET 127.0.0.1 7001 peer 1 0-65535,9-3,1-2\r\n").starts_with("+PONG "));
    let ranges = vec!["0-65535"; 2000].join(",");
    assert_eq!(
        line(&mut b, &format!("FAILOVER_ANNOUNCE peer 2 {ranges}\r\n")),
        "+OK\r\n"
    );
    assert_eq!(
        line(&mut b, "GARBAGE \u{1}\u{2}\r\n"),
        "-ERR unknown clusterbus command\r\n"
    );
    drop(b);

    // An endless line is cut off instead of buffered.
    let mut b = bus();
    let chunk = vec![b'A'; 64 * 1024];
    let mut hung_up = false;
    for _ in 0..64 {
        if b.write_all(&chunk).is_err() {
            hung_up = true;
            break;
        }
    }
    if !hung_up {
        let mut rest = Vec::new();
        assert!(matches!(b.read_to_end(&mut rest), Ok(0) | Err(_)));
    }

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    let nodes = resp_cmd_full(&mut c, &["CLUSTER", "NODES"]);
    assert!(nodes.contains("peer 127.0.0.1:7001@17001"), "{nodes}");
    assert!(
        !nodes.contains("evil") && !nodes.contains("65535"),
        "{nodes}"
    );
    assert!(resp_cmd_full(&mut c, &["CLUSTER", "INFO"]).contains("cluster_known_nodes:"));
    drop(c);
    shutdown_and_wait(port, &mut child.0);
}

#[test]
fn test_cluster_bumpepoch_and_set_config_epoch_e2e() {
    let port = 18200;
    start_test_server(port, 2);
    rudis::cluster::start_cluster_bus(port);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Invalid epoch 0
    let resp = send_and_read(&mut client, b"CLUSTER SET-CONFIG-EPOCH 0\r\n");
    assert!(
        resp.starts_with("-ERR Invalid config epoch"),
        "Expected invalid epoch error, got: {}",
        resp
    );

    // 2. Set valid epoch 5 on fresh node
    let resp = send_and_read(&mut client, b"CLUSTER SET-CONFIG-EPOCH 5\r\n");
    assert_eq!(resp, "+OK\r\n");

    let nodes = send_and_read(&mut client, b"CLUSTER NODES\r\n");
    assert!(
        nodes.contains(" 5 ") || nodes.contains("connected"),
        "Nodes output: {}",
        nodes
    );

    // 3. Reject setting when epoch is already non-zero
    let resp = send_and_read(&mut client, b"CLUSTER SET-CONFIG-EPOCH 6\r\n");
    assert!(
        resp.starts_with("-ERR Node config epoch is already set"),
        "Expected already set error, got: {}",
        resp
    );

    // 4. BUMPEPOCH bumps from 5 to 6
    let resp = send_and_read(&mut client, b"CLUSTER BUMPEPOCH\r\n");
    assert_eq!(resp, "+BUMPED 6\r\n");

    // 5. BUMPEPOCH again bumps to 7
    let resp = send_and_read(&mut client, b"CLUSTER BUMPEPOCH\r\n");
    assert_eq!(resp, "+BUMPED 7\r\n");

    // 6. Hard reset clears epoch to 0 and slots
    let resp = send_and_read(&mut client, b"CLUSTER RESET HARD\r\n");
    assert_eq!(resp, "+OK\r\n");

    // 7. After hard reset, SET-CONFIG-EPOCH works again
    let resp = send_and_read(&mut client, b"CLUSTER SET-CONFIG-EPOCH 42\r\n");
    assert_eq!(resp, "+OK\r\n");

    // 8. Add slot and verify SET-CONFIG-EPOCH is rejected
    let resp = send_and_read(&mut client, b"CLUSTER ADDSLOTS 100\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut client, b"CLUSTER SET-CONFIG-EPOCH 99\r\n");
    assert!(
        resp.starts_with("-ERR Node"),
        "Expected error when slots assigned or epoch already set, got: {}",
        resp
    );
}

#[test]
fn test_cluster_slot_routing_moved_and_clusterdown_e2e() {
    let port1 = 18205;
    let port2 = 18206;
    start_test_server(port1, 2);
    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();
    c1.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(3))).unwrap();

    // Node 1 owns 0..8191, Node 2 owns 8192..16300 (leaving 16301..16383 unassigned)
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER ADDSLOTS-RANGE 0 8191\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER ADDSLOTS-RANGE 8192 16300\r\n"),
        "+OK\r\n"
    );

    // Connect via MEET
    let meet_cmd = format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2);
    assert_eq!(send_and_read(&mut c1, meet_cmd.as_bytes()), "+OK\r\n");

    // Wait until Node 1 discovers Node 2
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let nodes = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
        if nodes.contains(&format!(":{}", port2)) && nodes.contains("8192-16300") {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Timeout waiting for cluster node discovery:\n{}", nodes);
        }
        thread::sleep(Duration::from_millis(50));
    }

    // Find keys matching specific slots
    let mut key_node1 = None;
    let mut key_node2 = None;
    let mut key_unassigned = None;
    for i in 0..100000 {
        let k = format!("k_{}", i);
        let s = rudis::router::key_slot(k.as_bytes());
        if s == 100 && key_node1.is_none() {
            key_node1 = Some((k, s));
        } else if s == 9000 && key_node2.is_none() {
            key_node2 = Some((k, s));
        } else if s == 16350 && key_unassigned.is_none() {
            key_unassigned = Some((k, s));
        }
        if key_node1.is_some() && key_node2.is_some() && key_unassigned.is_some() {
            break;
        }
    }

    let (k1, s1) = key_node1.unwrap();
    let (k2, s2) = key_node2.unwrap();
    let (k3, _s3) = key_unassigned.unwrap();

    // 1. Key in slot 100 on Node 1: served locally
    let resp = send_and_read(&mut c1, format!("SET {} val1\r\n", k1).as_bytes());
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut c1, format!("GET {}\r\n", k1).as_bytes());
    assert_eq!(resp, "$4\r\nval1\r\n");

    // 2. Key in slot 9000 on Node 1: redirected with -MOVED to Node 2
    let resp = send_and_read(&mut c1, format!("GET {}\r\n", k2).as_bytes());
    assert_eq!(resp, format!("-MOVED {} 127.0.0.1:{}\r\n", s2, port2));

    // 3. Key in unassigned slot 16350 on Node 1: returns -CLUSTERDOWN
    let resp = send_and_read(&mut c1, format!("GET {}\r\n", k3).as_bytes());
    assert_eq!(resp, "-CLUSTERDOWN Hash slot not served\r\n");

    // 4. Delete slot 100 on Node 1: now returns -CLUSTERDOWN
    let resp = send_and_read(&mut c1, format!("CLUSTER DELSLOTS {}\r\n", s1).as_bytes());
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut c1, format!("GET {}\r\n", k1).as_bytes());
    assert_eq!(resp, "-CLUSTERDOWN Hash slot not served\r\n");

    // 5. Re-add slot 100 on Node 1: query served locally again
    let resp = send_and_read(&mut c1, format!("CLUSTER ADDSLOTS {}\r\n", s1).as_bytes());
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut c1, format!("SET {} val_readded\r\n", k1).as_bytes());
    assert_eq!(resp, "+OK\r\n");
}

#[test]
fn test_cluster_rebalance_pipeline_e2e() {
    let port1 = 18215;
    let port2 = 18216;
    start_test_server(port1, 2);
    rudis::cluster::start_cluster_bus(port1);
    start_test_server(port2, 2);
    rudis::cluster::start_cluster_bus(port2);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();
    c1.set_read_timeout(Some(Duration::from_secs(4))).unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(4))).unwrap();

    // Node 1: 0..12000, Node 2: 12001..16383
    assert_eq!(
        send_and_read(&mut c1, b"CLUSTER ADDSLOTS-RANGE 0 12000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c2, b"CLUSTER ADDSLOTS-RANGE 12001 16383\r\n"),
        "+OK\r\n"
    );

    let meet_cmd = format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2);
    assert_eq!(send_and_read(&mut c1, meet_cmd.as_bytes()), "+OK\r\n");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let check = send_and_read(&mut c1, b"CLUSTER CHECK\r\n");
        if check.contains("[OK] All 16384 slots covered") || std::time::Instant::now() > deadline {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    // Run rebalance with custom target and PIPELINE option
    let rebalance_cmd = format!("CLUSTER REBALANCE 127.0.0.1 {} 5 PIPELINE 5\r\n", port2);
    let rebalance_resp = send_and_read(&mut c1, rebalance_cmd.as_bytes());
    assert_eq!(rebalance_resp, ":5\r\n");
}

#[test]
fn test_cluster_persistence_restore_asking_and_dynamic_slots_e2e() {
    let port = 17974;
    let port_s = port.to_string();
    let dir = std::env::temp_dir().join(format!(
        "rudis_cluster_e2e_{}_{}",
        port,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap().to_string();

    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--dir",
            &dir_s,
            "--cluster-enabled",
            "yes",
            "--threads",
            "1",
            "--no-pin",
        ],
        port,
    );

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Verify default cluster-config-file and custom CONFIG SET
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "cluster-config-file"]),
        "*2\r\n$19\r\ncluster-config-file\r\n$10\r\nnodes.conf\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut c,
            &["CONFIG", "SET", "cluster-config-file", "nodes-17974.conf"]
        ),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "cluster-config-file"]),
        "*2\r\n$19\r\ncluster-config-file\r\n$16\r\nnodes-17974.conf\r\n"
    );

    let orig_id_resp = resp_cmd(&mut c, &["CLUSTER", "MYID"]);
    let orig_id = orig_id_resp
        .lines()
        .nth(1)
        .expect("CLUSTER MYID bulk line")
        .to_string();
    assert_eq!(orig_id.len(), 40);

    // Restrict slots to 0..=100, bump epoch, and save config
    assert_eq!(resp_cmd(&mut c, &["CLUSTER", "FLUSHSLOTS"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CLUSTER", "ADDSLOTSRANGE", "0", "100"]),
        "+OK\r\n"
    );
    assert!(resp_cmd(&mut c, &["CLUSTER", "BUMPEPOCH"]).starts_with("+BUMPED "));
    assert_eq!(resp_cmd(&mut c, &["CLUSTER", "SAVECONFIG"]), "+OK\r\n");

    let nodes_conf_path = dir.join("nodes-17974.conf");
    assert!(nodes_conf_path.exists(), "nodes-17974.conf should exist");

    // Test RESTORE-ASKING on an IMPORTING slot without prior ASKING command:
    // Create a key in slot <= 100 or set slot of "k_mig" as IMPORTING.
    assert_eq!(resp_cmd(&mut c, &["CLUSTER", "ADDSLOTS", "500"]), "+OK\r\n");
    // Find a key that hashes to slot 500 or use a hash tag.
    let slot_tag_resp = resp_cmd(&mut c, &["CLUSTER", "KEYSLOT", "{mig}key"]);
    let mig_slot: u16 = slot_tag_resp
        .trim()
        .strip_prefix(':')
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CLUSTER", "ADDSLOTS", &mig_slot.to_string()]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["SET", "{mig}src", "hello_mig"]),
        "+OK\r\n"
    );
    let dump_raw = {
        use std::io::{Read, Write};
        c.write_all(b"*2\r\n$4\r\nDUMP\r\n$8\r\n{mig}src\r\n")
            .unwrap();
        let mut hdr = [0u8; 64];
        let mut pos = 0;
        loop {
            let n = c.read(&mut hdr[pos..pos + 1]).unwrap();
            assert_eq!(n, 1);
            pos += 1;
            if pos >= 2 && &hdr[pos - 2..pos] == b"\r\n" {
                break;
            }
        }
        let len_str = std::str::from_utf8(&hdr[1..pos - 2]).unwrap();
        let bulk_len: usize = len_str.parse().unwrap();
        let mut payload = vec![0u8; bulk_len + 2];
        c.read_exact(&mut payload).unwrap();
        payload.truncate(bulk_len);
        payload
    };

    // Mark mig_slot as IMPORTING from a peer node
    let peer_id = "2222222222222222222222222222222222222222";
    assert_eq!(
        resp_cmd(
            &mut c,
            &[
                "CLUSTER",
                "SETSLOT",
                &mig_slot.to_string(),
                "IMPORTING",
                peer_id
            ]
        ),
        "+OK\r\n"
    );

    // Plain RESTORE without ASKING on an IMPORTING slot returns -MOVED
    {
        use std::io::{Read, Write};
        let mut frame = Vec::new();
        frame.extend_from_slice(
            format!(
                "*4\r\n$7\r\nRESTORE\r\n$8\r\n{{mig}}dst\r\n$1\r\n0\r\n${}\r\n",
                dump_raw.len()
            )
            .as_bytes(),
        );
        frame.extend_from_slice(&dump_raw);
        frame.extend_from_slice(b"\r\n");
        c.write_all(&frame).unwrap();
        let mut buf = [0u8; 128];
        let n = c.read(&mut buf).unwrap();
        let reply = String::from_utf8_lossy(&buf[..n]);
        assert!(reply.starts_with("-MOVED "), "expected -MOVED, got {reply}");
    }

    // RESTORE-ASKING without a preceding ASKING command succeeds on an IMPORTING slot
    {
        use std::io::{Read, Write};
        let mut frame = Vec::new();
        frame.extend_from_slice(
            format!(
                "*5\r\n$14\r\nRESTORE-ASKING\r\n$8\r\n{{mig}}dst\r\n$1\r\n0\r\n${}\r\n",
                dump_raw.len()
            )
            .as_bytes(),
        );
        frame.extend_from_slice(&dump_raw);
        frame.extend_from_slice(b"\r\n$7\r\nREPLACE\r\n");
        c.write_all(&frame).unwrap();
        let mut buf = [0u8; 128];
        let n = c.read(&mut buf).unwrap();
        let reply = String::from_utf8_lossy(&buf[..n]);
        assert_eq!(reply, "+OK\r\n");
    }

    // Stabilize slot and verify restored value
    assert_eq!(
        resp_cmd(
            &mut c,
            &["CLUSTER", "SETSLOT", &mig_slot.to_string(), "STABLE"]
        ),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["GET", "{mig}dst"]),
        "$9\r\nhello_mig\r\n"
    );

    drop(c);
    shutdown_and_wait(port, &mut srv);

    // Restart server with custom cluster-config-file in rudis.conf and verify state is reloaded
    let conf_path = dir.join("rudis.conf");
    std::fs::write(
        &conf_path,
        format!(
            "port {}\ndir {}\ncluster-enabled yes\ncluster-config-file nodes-17974.conf\n",
            port, dir_s
        ),
    )
    .unwrap();

    let mut srv2 = spawn_rudis_listening(
        &[conf_path.to_str().unwrap(), "--threads", "1", "--no-pin"],
        port,
    );
    let mut c2 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    let reloaded_id_resp = resp_cmd(&mut c2, &["CLUSTER", "MYID"]);
    let reloaded_id = reloaded_id_resp.lines().nth(1).unwrap().to_string();
    assert_eq!(reloaded_id, orig_id);

    let nodes_out = resp_cmd(&mut c2, &["CLUSTER", "NODES"]);
    assert!(nodes_out.contains(&orig_id), "{nodes_out}");
    assert!(nodes_out.contains("0-100"), "{nodes_out}");

    drop(c2);
    shutdown_and_wait(port, &mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

/// In cluster mode each shard has its own client port, and clients write to
/// it directly (MOVED points there). The node still has one identity and one
/// replication stream: writes taken on any shard's port must reach the
/// replica, and `CLUSTER MYID` must not depend on the port asked.
#[test]
fn test_cluster_shard_port_writes_reach_the_replica_e2e() {
    let (mport, rport) = (18069u16, 18075u16);
    let mk_dir = |p: u16| {
        let d =
            std::env::temp_dir().join(format!("rudis_shardport_repl_{}_{}", p, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_str().unwrap().to_string()
    };
    let spawn = |p: u16, dir: &str| {
        ServerGuard(spawn_rudis_listening(
            &[
                "--port",
                &p.to_string(),
                "--dir",
                dir,
                "--cluster-enabled",
                "yes",
                "--threads",
                "2",
                "--no-pin",
            ],
            p,
        ))
    };
    let (mdir, rdir) = (mk_dir(mport), mk_dir(rport));
    let _m = spawn(mport, &mdir);
    let _r = spawn(rport, &rdir);
    let conn = connect_ready;
    let mut m0 = conn(mport);
    let mut m1 = conn(mport + 1);
    assert_eq!(
        resp_cmd(&mut m0, &["CLUSTER", "MYID"]),
        resp_cmd(&mut m1, &["CLUSTER", "MYID"]),
        "both shard ports are the same node"
    );
    let mut r0 = conn(rport);
    assert_eq!(
        resp_cmd(&mut r0, &["REPLICAOF", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    let wait_for = |c: &mut TcpStream, want: &str, what: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let info = resp_cmd(c, &["INFO", "replication"]);
            if info.contains(want) {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "{what}: {info}");
            thread::sleep(Duration::from_millis(50));
        }
    };
    wait_for(&mut r0, "master_link_status:up", "replica link");

    // Each key is written on the port of the shard that owns it.
    let n = 400;
    for i in 0..n {
        let k = format!("sp_{i}");
        let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2);
        let c = if shard == 0 { &mut m0 } else { &mut m1 };
        assert_eq!(resp_cmd(c, &["SET", &k, "v"]), "+OK\r\n", "{k}");
    }
    let dbsize = |c: &mut TcpStream| resp_cmd(c, &["DBSIZE"]);
    assert_eq!(dbsize(&mut m0), format!(":{n}\r\n"));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let got = dbsize(&mut r0);
        if got == format!(":{n}\r\n") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica holds {got:?} of {n} keys"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

/// `CLUSTER REPLICATE` makes the node an actual replica: it receives the
/// master's dataset and writes, resumes after a restart (from nodes.conf),
/// and CLUSTER FAILOVER ends the link before the promotion.
#[test]
fn test_cluster_replicate_copies_the_masters_data_e2e() {
    let (mport, rport) = (18081u16, 18087u16);
    let mk_dir = |p: u16| {
        let d = std::env::temp_dir().join(format!(
            "rudis_cluster_replicate_{}_{}",
            p,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_str().unwrap().to_string()
    };
    let spawn = |p: u16, dir: &str| {
        ServerGuard(spawn_rudis_listening(
            &[
                "--port",
                &p.to_string(),
                "--dir",
                dir,
                "--cluster-enabled",
                "yes",
                "--threads",
                "2",
                "--no-pin",
            ],
            p,
        ))
    };
    let conn = connect_ready;
    let bulk = |r: String| r.lines().nth(1).unwrap_or_default().to_string();
    let wait_until = |what: &str, mut ok: Box<dyn FnMut() -> Option<String> + '_>| {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            match ok() {
                None => return,
                Some(state) => assert!(std::time::Instant::now() < deadline, "{what}: {state}"),
            }
            thread::sleep(Duration::from_millis(50));
        }
    };
    let (mdir, rdir) = (mk_dir(mport), mk_dir(rport));
    let _m = spawn(mport, &mdir);
    let mut r_srv = spawn(rport, &rdir);
    let mut m0 = conn(mport);
    let mut m1 = conn(mport + 1);
    let mut r0 = conn(rport);
    let master_id = bulk(resp_cmd(&mut m0, &["CLUSTER", "MYID"]));

    // Writes each key on the port of the master shard that owns it.
    let mut write = |from: usize, to: usize| {
        for i in from..to {
            let k = format!("cr_{i}");
            let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2);
            let c = if shard == 0 { &mut m0 } else { &mut m1 };
            assert_eq!(resp_cmd(c, &["SET", &k, "v"]), "+OK\r\n", "{k}");
        }
    };
    write(0, 300);

    // The replica serves no slots (CLUSTER REPLICATE refuses otherwise).
    assert_eq!(
        resp_cmd(&mut r0, &["CLUSTER", "DELSLOTSRANGE", "0", "16383"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut r0,
            &["CLUSTER", "MEET", "127.0.0.1", &mport.to_string()]
        ),
        "+OK\r\n"
    );
    wait_until(
        "replica learns the master",
        Box::new(|| {
            let nodes = resp_cmd(&mut r0, &["CLUSTER", "NODES"]);
            (!nodes.contains(&master_id)).then_some(nodes)
        }),
    );
    assert_eq!(
        resp_cmd(&mut r0, &["CLUSTER", "REPLICATE", &master_id]),
        "+OK\r\n"
    );
    let dbsize_is = |c: &mut TcpStream, n: usize| {
        let got = resp_cmd(c, &["DBSIZE"]);
        (got != format!(":{n}\r\n")).then_some(got)
    };
    wait_until("initial sync", Box::new(|| dbsize_is(&mut r0, 300)));
    write(300, 400);
    wait_until("streamed writes", Box::new(|| dbsize_is(&mut r0, 400)));

    // A restarted replica resumes replicating its master.
    drop(r0);
    let _ = r_srv.0.kill();
    let _ = r_srv.0.wait();
    r_srv = spawn(rport, &rdir);
    let mut r0 = conn(rport);
    let info = resp_cmd(&mut r0, &["INFO", "replication"]);
    assert!(info.contains("role:slave"), "{info}");
    write(400, 500);
    wait_until(
        "writes after the restart",
        Box::new(|| dbsize_is(&mut r0, 500)),
    );

    // Failover: the promoted node stops following the old master and takes
    // its slots; like Redis, the old master then replicates it.
    assert_eq!(resp_cmd(&mut r0, &["CLUSTER", "FAILOVER"]), "+OK\r\n");
    let info = resp_cmd(&mut r0, &["INFO", "replication"]);
    assert!(info.contains("role:master"), "{info}");
    let new_id = bulk(resp_cmd(&mut r0, &["CLUSTER", "MYID"]));
    wait_until(
        "old master follows the promoted node",
        Box::new(|| {
            let nodes = resp_cmd(&mut conn(mport), &["CLUSTER", "NODES"]);
            let me = nodes
                .lines()
                .find(|l| l.contains("myself"))
                .unwrap_or_default();
            (!(me.contains("myself,slave") && me.contains(&new_id))).then_some(nodes)
        }),
    );
    let k = "cr_after_failover";
    let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2);
    let moved = resp_cmd(&mut conn(mport + shard as u16), &["SET", k, "v"]);
    assert!(moved.starts_with("-MOVED"), "{moved}");
    assert_eq!(
        resp_cmd(&mut conn(rport + shard as u16), &["SET", k, "v"]),
        "+OK\r\n"
    );
    wait_until(
        "old master replicates the promoted node",
        Box::new(|| dbsize_is(&mut conn(mport), 501)),
    );
    drop(r_srv);
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

/// Killing a master of a 3-master cluster: the survivors agree it failed
/// (FAIL, keeping its role and slots) and stay responsive throughout, and
/// its replica takes over its slots with the data it replicated. When the
/// old master comes back it yields the slots to the newer owner and
/// replicates it, instead of serving them too.
#[test]
fn test_cluster_master_failure_promotes_its_replica_e2e() {
    let ports = [18093u16, 18099, 18105, 18111];
    let ranges = [(0, 5460), (5461, 10922), (10923, 16383)];
    let dirs: Vec<String> = ports
        .iter()
        .map(|p| {
            let d = std::env::temp_dir().join(format!(
                "rudis_cluster_failover_{}_{}",
                p,
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
            d.to_str().unwrap().to_string()
        })
        .collect();
    let mut servers: Vec<Option<ServerGuard>> = ports
        .iter()
        .zip(&dirs)
        .map(|(p, d)| {
            Some(ServerGuard(spawn_rudis_listening(
                &[
                    "--port",
                    &p.to_string(),
                    "--dir",
                    d,
                    "--cluster-enabled",
                    "yes",
                    "--threads",
                    "2",
                    "--no-pin",
                ],
                *p,
            )))
        })
        .collect();
    let conn = |p: u16| {
        let c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c
    };
    let cmd = |p: u16, args: &[&str]| resp_cmd(&mut conn(p), args);
    let bulk = |r: String| r.lines().nth(1).unwrap_or_default().to_string();
    let wait_until = |what: &str, ok: &mut dyn FnMut() -> Option<String>| {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            match ok() {
                None => return,
                Some(state) => assert!(std::time::Instant::now() < deadline, "{what}: {state}"),
            }
            thread::sleep(Duration::from_millis(100));
        }
    };

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
    let victim_id = bulk(cmd(ports[2], &["CLUSTER", "MYID"]));
    let replica = ports[3];
    wait_until("replica learns the victim", &mut || {
        let nodes = cmd(replica, &["CLUSTER", "NODES"]);
        (!nodes.contains(&victim_id)).then_some(nodes)
    });
    assert_eq!(
        cmd(replica, &["CLUSTER", "REPLICATE", &victim_id]),
        "+OK\r\n"
    );
    wait_for_slot_views(&ports);

    // Keys in the victim's slots, written on the victim's shard ports.
    let keys: Vec<String> = (0..2000)
        .map(|i| format!("fo_{i}"))
        .filter(|k| rudis::router::key_slot(k.as_bytes()) >= 10923)
        .take(100)
        .collect();
    for k in &keys {
        let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2);
        assert_eq!(cmd(ports[2] + shard as u16, &["SET", k, "v"]), "+OK\r\n");
    }
    wait_until("replica has the victim's keys", &mut || {
        let n = cmd(replica, &["DBSIZE"]);
        (n != format!(":{}\r\n", keys.len())).then_some(n)
    });

    servers[2] = None; // SIGKILL
    let started = std::time::Instant::now();
    wait_until("both masters see the victim FAIL", &mut || {
        // The survivors keep answering while they judge the failure.
        let views: Vec<String> = [ports[0], ports[1]]
            .iter()
            .map(|p| {
                assert_eq!(cmd(*p, &["PING"]), "+PONG\r\n", "master {p} stuck");
                cmd(*p, &["CLUSTER", "NODES"])
            })
            .collect();
        let failed = |v: &String| {
            v.lines()
                .any(|l| l.starts_with(&victim_id) && l.contains(" master,fail "))
        };
        (!views.iter().all(failed)).then(|| views.join("\n"))
    });
    eprintln!("FAIL agreed after {:?}", started.elapsed());
    for p in [ports[0], ports[1]] {
        assert_eq!(cmd(p, &["PING"]), "+PONG\r\n", "master {p} stuck");
    }
    wait_until("replica promoted with the victim's slots", &mut || {
        let nodes = cmd(replica, &["CLUSTER", "NODES"]);
        let me = nodes
            .lines()
            .find(|l| l.contains("myself"))
            .unwrap_or_default();
        (!(me.contains("master") && me.ends_with("10923-16383"))).then_some(nodes)
    });
    eprintln!("promoted after {:?}", started.elapsed());
    let info = cmd(replica, &["INFO", "replication"]);
    assert!(info.contains("role:master"), "{info}");
    assert_eq!(cmd(replica, &["DBSIZE"]), format!(":{}\r\n", keys.len()));
    // The other masters route the victim's slots to the promoted replica.
    let slot_owner_is_replica = |p: u16| {
        let slots = cmd(p, &["CLUSTER", "SLOTS"]);
        slots.contains(&format!(
            ":10923\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n:{replica}\r\n"
        ))
    };
    wait_until("masters route the slots to the replica", &mut || {
        (![ports[0], ports[1]]
            .iter()
            .all(|p| slot_owner_is_replica(*p)))
        .then(|| cmd(ports[0], &["CLUSTER", "SLOTS"]))
    });

    // The old master returns: nodes.conf still says it serves
    // 10923-16383, at an older epoch.
    servers[2] = Some(ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &ports[2].to_string(),
            "--dir",
            &dirs[2],
            "--cluster-enabled",
            "yes",
            "--threads",
            "2",
            "--no-pin",
        ],
        ports[2],
    )));
    let new_master_id = bulk(cmd(replica, &["CLUSTER", "MYID"]));
    wait_until(
        "old master rejoins as a replica of the new one",
        &mut || {
            let nodes = cmd(ports[2], &["CLUSTER", "NODES"]);
            let me = nodes
                .lines()
                .find(|l| l.contains("myself"))
                .unwrap_or_default();
            (!(me.contains("myself,slave") && me.contains(&new_master_id))).then_some(nodes)
        },
    );
    let k = &keys[1];
    let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2);
    let moved = cmd(ports[2] + shard as u16, &["SET", k, "split"]);
    assert!(
        moved.starts_with("-MOVED"),
        "old master still serves: {moved}"
    );
    wait_until("old master replicates the new master", &mut || {
        let info = cmd(ports[2], &["INFO", "replication"]);
        let want = format!("master_port:{replica}");
        (!(info.contains(&want) && info.contains("master_link_status:up"))).then_some(info)
    });
    let n = cmd(replica, &["DBSIZE"]);
    wait_until("old master holds the new master's dataset", &mut || {
        let got = cmd(ports[2], &["DBSIZE"]);
        (got != n).then_some(got)
    });
    assert_eq!(cmd(replica + shard as u16, &["GET", k]), "$1\r\nv\r\n");

    // A manual failover back: the old master takes its slots over again
    // (at an epoch above the one it lost them at), and the node promoted
    // in its place becomes its replica.
    assert_eq!(cmd(ports[2], &["CLUSTER", "FAILOVER"]), "+OK\r\n");
    wait_until("manual failover back", &mut || {
        let back = cmd(ports[2], &["CLUSTER", "NODES"]);
        let me = back
            .lines()
            .find(|l| l.contains("myself"))
            .unwrap_or_default();
        let other = cmd(replica, &["CLUSTER", "NODES"]);
        let them = other
            .lines()
            .find(|l| l.contains("myself"))
            .unwrap_or_default();
        (!(me.contains("myself,master")
            && me.ends_with(" 10923-16383")
            && them.contains("myself,slave")))
        .then(|| format!("{back}\n{other}"))
    });
    assert_eq!(cmd(ports[2] + shard as u16, &["SET", k, "back"]), "+OK\r\n");

    drop(servers);
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A fresh cluster-mode node shows its shards as pseudo nodes, but must
/// not persist them: after a restart they came back as peers that never
/// answer (and claimed slots), visible once the node meets a real peer.
#[test]
fn test_cluster_restart_does_not_resurrect_pseudo_shard_nodes_e2e() {
    let (port, peer) = (18117u16, 18123u16);
    let mk_dir = |p: u16| {
        let d = std::env::temp_dir().join(format!("rudis_pseudo_{}_{}", p, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_str().unwrap().to_string()
    };
    let spawn = |p: u16, dir: &str| {
        ServerGuard(spawn_rudis_listening(
            &[
                "--port",
                &p.to_string(),
                "--dir",
                dir,
                "--cluster-enabled",
                "yes",
                "--threads",
                "2",
                "--no-pin",
            ],
            p,
        ))
    };
    let cmd = |p: u16, args: &[&str]| {
        let mut c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        resp_cmd(&mut c, args)
    };
    let (dir, peer_dir) = (mk_dir(port), mk_dir(peer));
    let pseudo = format!("{:040x}", 2);
    let first = spawn(port, &dir);
    assert!(cmd(port, &["CLUSTER", "NODES"]).contains(&pseudo));
    assert_eq!(cmd(port, &["CLUSTER", "SAVECONFIG"]), "+OK\r\n");
    drop(first);
    let _node = spawn(port, &dir);
    let _peer = spawn(peer, &peer_dir);
    assert_eq!(
        cmd(peer, &["CLUSTER", "DELSLOTSRANGE", "0", "16383"]),
        "+OK\r\n"
    );
    assert_eq!(
        cmd(port, &["CLUSTER", "MEET", "127.0.0.1", &peer.to_string()]),
        "+OK\r\n"
    );
    let nodes = cmd(port, &["CLUSTER", "NODES"]);
    assert!(
        !nodes.contains(&pseudo),
        "pseudo node came back as a peer:\n{nodes}"
    );
    assert!(nodes.contains(&format!("127.0.0.1:{peer}@")), "{nodes}");
    let mine = nodes.lines().find(|l| l.contains("myself")).unwrap();
    assert!(mine.ends_with(" 0-16383"), "{mine}");
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&peer_dir);
}

/// 3 masters with a replica each. Heartbeats must stay timely with six
/// nodes: no live node is ever judged failing, and killing a master gets
/// its replica promoted. (Accepting bus connections used to wait behind
/// sequential pings: PINGs were read seconds late, so live nodes looked
/// silent and a dead one kept "talking" for a minute.)
#[test]
fn test_cluster_six_node_heartbeats_stay_timely_e2e() {
    let ports = [18129u16, 18135, 18141, 18147, 18153, 18159];
    let ranges = [(0, 5460), (5461, 10922), (10923, 16383)];
    let dirs: Vec<String> = ports
        .iter()
        .map(|p| {
            let d = std::env::temp_dir().join(format!("rudis_six_{}_{}", p, std::process::id()));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
            d.to_str().unwrap().to_string()
        })
        .collect();
    let mut servers: Vec<Option<ServerGuard>> = ports
        .iter()
        .zip(&dirs)
        .map(|(p, d)| {
            Some(ServerGuard(spawn_rudis_listening(
                &[
                    "--port",
                    &p.to_string(),
                    "--dir",
                    d,
                    "--cluster-enabled",
                    "yes",
                    "--threads",
                    "2",
                    "--no-pin",
                ],
                *p,
            )))
        })
        .collect();
    let cmd = |p: u16, args: &[&str]| {
        let mut c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        resp_cmd(&mut c, args)
    };
    let bulk = |r: String| r.lines().nth(1).unwrap_or_default().to_string();
    let wait_until = |what: &str, ok: &mut dyn FnMut() -> Option<String>| {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            match ok() {
                None => return,
                Some(state) => assert!(std::time::Instant::now() < deadline, "{what}: {state}"),
            }
            thread::sleep(Duration::from_millis(100));
        }
    };
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
    let ids: Vec<String> = ports
        .iter()
        .map(|p| bulk(cmd(*p, &["CLUSTER", "MYID"])))
        .collect();
    wait_until("everyone knows everyone", &mut || {
        let views: Vec<String> = ports
            .iter()
            .map(|p| cmd(*p, &["CLUSTER", "NODES"]))
            .collect();
        (!views
            .iter()
            .all(|v| ids.iter().all(|id| v.contains(id.as_str()))))
        .then(|| views.join("\n"))
    });
    for i in 0..3 {
        assert_eq!(
            cmd(ports[3 + i], &["CLUSTER", "REPLICATE", &ids[i]]),
            "+OK\r\n"
        );
    }
    wait_for_slot_views(&ports);

    // Ten seconds (twice the node timeout) of a healthy cluster: no node
    // may see a live one as failing.
    let healthy_until = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < healthy_until {
        for p in ports {
            let nodes = cmd(p, &["CLUSTER", "NODES"]);
            assert!(
                !nodes.contains("fail"),
                "node {p} sees a live node failing:\n{nodes}"
            );
        }
        thread::sleep(Duration::from_millis(250));
    }

    servers[0] = None; // SIGKILL the master of 0-5460
    let started = std::time::Instant::now();
    wait_until("its replica takes over", &mut || {
        let nodes = cmd(ports[3], &["CLUSTER", "NODES"]);
        let me = nodes
            .lines()
            .find(|l| l.contains("myself"))
            .unwrap_or_default();
        (!(me.contains("master") && me.ends_with(" 0-5460"))).then_some(nodes)
    });
    eprintln!("promoted after {:?}", started.elapsed());
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "{:?}",
        started.elapsed()
    );
    drop(servers);
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A master with two replicas fails: one replica takes over, and the
/// other follows the winner (replicating it) instead of staying attached
/// to the dead master or taking the slots in a later election.
#[test]
fn test_cluster_second_replica_follows_the_promoted_one_e2e() {
    let ports = [18165u16, 18171, 18177, 18183, 18189];
    let ranges = [(0, 5460), (5461, 10922), (10923, 16383)];
    let dirs: Vec<String> = ports
        .iter()
        .map(|p| {
            let d =
                std::env::temp_dir().join(format!("rudis_two_rep_{}_{}", p, std::process::id()));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
            d.to_str().unwrap().to_string()
        })
        .collect();
    let mut servers: Vec<Option<ServerGuard>> = ports
        .iter()
        .zip(&dirs)
        .map(|(p, d)| {
            Some(ServerGuard(spawn_rudis_listening(
                &[
                    "--port",
                    &p.to_string(),
                    "--dir",
                    d,
                    "--cluster-enabled",
                    "yes",
                    "--threads",
                    "2",
                    "--no-pin",
                ],
                *p,
            )))
        })
        .collect();
    let cmd = |p: u16, args: &[&str]| {
        let mut c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        resp_cmd(&mut c, args)
    };
    let bulk = |r: String| r.lines().nth(1).unwrap_or_default().to_string();
    let wait_until = |what: &str, ok: &mut dyn FnMut() -> Option<String>| {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            match ok() {
                None => return,
                Some(state) => assert!(std::time::Instant::now() < deadline, "{what}: {state}"),
            }
            thread::sleep(Duration::from_millis(100));
        }
    };
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
    let ids: Vec<String> = ports
        .iter()
        .map(|p| bulk(cmd(*p, &["CLUSTER", "MYID"])))
        .collect();
    wait_until("everyone knows everyone", &mut || {
        let views: Vec<String> = ports
            .iter()
            .map(|p| cmd(*p, &["CLUSTER", "NODES"]))
            .collect();
        (!views
            .iter()
            .all(|v| ids.iter().all(|id| v.contains(id.as_str()))))
        .then(|| views.join("\n"))
    });
    // Both replicate the master of 10923-16383.
    for r in [ports[3], ports[4]] {
        assert_eq!(cmd(r, &["CLUSTER", "REPLICATE", &ids[2]]), "+OK\r\n");
    }
    wait_for_slot_views(&ports);
    let keys: Vec<String> = (0..2000)
        .map(|i| format!("tr_{i}"))
        .filter(|k| rudis::router::key_slot(k.as_bytes()) >= 10923)
        .take(50)
        .collect();
    for k in &keys {
        let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2);
        assert_eq!(cmd(ports[2] + shard as u16, &["SET", k, "v"]), "+OK\r\n");
    }
    for r in [ports[3], ports[4]] {
        wait_until("replica synced", &mut || {
            let n = cmd(r, &["DBSIZE"]);
            (n != format!(":{}\r\n", keys.len())).then_some(n)
        });
    }

    servers[2] = None; // SIGKILL
    let me_line = |p: u16| {
        let nodes = cmd(p, &["CLUSTER", "NODES"]);
        nodes
            .lines()
            .find(|l| l.contains("myself"))
            .unwrap_or_default()
            .to_string()
    };
    let mut winner = 0;
    wait_until("one replica takes over", &mut || {
        winner = [ports[3], ports[4]]
            .into_iter()
            .find(|p| me_line(*p).ends_with(" 10923-16383"))
            .unwrap_or(0);
        (winner == 0).then(|| {
            [ports[0], ports[1], ports[3], ports[4]]
                .iter()
                .map(|p| {
                    format!(
                        "--- {p}:\n{}{}",
                        cmd(*p, &["CLUSTER", "NODES"]),
                        cmd(*p, &["INFO", "replication"])
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
    });
    let loser = if winner == ports[3] {
        ports[4]
    } else {
        ports[3]
    };
    let winner_id = bulk(cmd(winner, &["CLUSTER", "MYID"]));
    wait_until("the other follows the winner", &mut || {
        let me = me_line(loser);
        let info = cmd(loser, &["INFO", "replication"]);
        (!(me.contains("myself,slave")
            && me.contains(&winner_id)
            && info.contains(&format!("master_port:{winner}"))
            && info.contains("master_link_status:up")))
        .then(|| format!("{me}\n{info}"))
    });
    // It stays a replica (no later election takes the slots back).
    thread::sleep(Duration::from_secs(3));
    assert!(
        me_line(loser).contains("myself,slave"),
        "{}",
        me_line(loser)
    );
    assert!(
        me_line(winner).ends_with(" 10923-16383"),
        "{}",
        me_line(winner)
    );
    // A new key on the winner reaches the follower.
    let k = format!("{}_new", keys[0]);
    let k = (0..100)
        .map(|i| format!("{k}{i}"))
        .find(|k| rudis::router::key_slot(k.as_bytes()) >= 10923)
        .unwrap();
    let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2);
    assert_eq!(cmd(winner + shard as u16, &["SET", &k, "new"]), "+OK\r\n");
    wait_until("the follower gets the winner's writes", &mut || {
        let n = cmd(loser, &["DBSIZE"]);
        (n != format!(":{}\r\n", keys.len() + 1)).then_some(n)
    });
    drop(servers);
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Client commands redirected (MOVED) by a node, while its own slots keep
/// changing (DELSLOTS/ADDSLOTS) and its cluster bus judges and claims slots:
/// the node must stay responsive (no lock-order cycle between the slot
/// check, slot writers and the bus).
#[test]
fn test_cluster_moved_checks_race_slot_changes_e2e() {
    let (a, b) = (18195u16, 18201u16);
    let mk_dir = |p: u16| {
        let d = std::env::temp_dir().join(format!("rudis_lockorder_{}_{}", p, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_str().unwrap().to_string()
    };
    let spawn = |p: u16, dir: &str| {
        ServerGuard(spawn_rudis_listening(
            &[
                "--port",
                &p.to_string(),
                "--dir",
                dir,
                "--cluster-enabled",
                "yes",
                "--threads",
                "2",
                "--no-pin",
            ],
            p,
        ))
    };
    let (da, db) = (mk_dir(a), mk_dir(b));
    let _na = spawn(a, &da);
    let _nb = spawn(b, &db);
    let cmd = |p: u16, args: &[&str]| {
        let mut c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        resp_cmd(&mut c, args)
    };
    assert_eq!(
        cmd(a, &["CLUSTER", "DELSLOTSRANGE", "8192", "16383"]),
        "+OK\r\n"
    );
    assert_eq!(
        cmd(b, &["CLUSTER", "DELSLOTSRANGE", "0", "8191"]),
        "+OK\r\n"
    );
    assert_eq!(
        cmd(a, &["CLUSTER", "MEET", "127.0.0.1", &b.to_string()]),
        "+OK\r\n"
    );
    let foreign: Vec<String> = (0..5000)
        .map(|i| format!("lo_{i}"))
        .filter(|k| rudis::router::key_slot(k.as_bytes()) >= 8192)
        .take(64)
        .collect();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut workers = Vec::new();
    for shard in 0..2u16 {
        let (stop, foreign) = (stop.clone(), foreign.clone());
        workers.push(thread::spawn(move || {
            let mut c = TcpStream::connect(("127.0.0.1", a + shard)).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                for k in &foreign {
                    let r = resp_cmd(&mut c, &["GET", k]);
                    assert!(
                        r.starts_with("-MOVED") || r.starts_with("-CLUSTERDOWN"),
                        "{r}"
                    );
                }
            }
        }));
    }
    let flipper = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut c = TcpStream::connect(("127.0.0.1", a)).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                assert_eq!(resp_cmd(&mut c, &["CLUSTER", "DELSLOTS", "100"]), "+OK\r\n");
                assert_eq!(resp_cmd(&mut c, &["CLUSTER", "ADDSLOTS", "100"]), "+OK\r\n");
            }
        })
    };
    let until = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < until {
        assert_eq!(cmd(a, &["PING"]), "+PONG\r\n");
        assert!(cmd(a, &["CLUSTER", "NODES"]).contains("myself"));
        thread::sleep(Duration::from_millis(100));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in workers {
        w.join().unwrap();
    }
    flipper.join().unwrap();
    let _ = std::fs::remove_dir_all(&da);
    let _ = std::fs::remove_dir_all(&db);
}

/// A cluster-mode node with 2 shards (a port each) at its maxmemory under
/// allkeys-lru: writes on either shard's port evict rather than fail, the
/// node stays near its limit, and every shard port reports the limit.
/// (Shard 1 used to run with maxmemory 0 and its own usage table: the node
/// overshot the limit, then refused writes with OOM.)
#[test]
fn test_cluster_shard_ports_share_maxmemory_e2e() {
    let port = 18219u16;
    let dir = std::env::temp_dir().join(format!("rudis_cl_maxmem_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _srv = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &port.to_string(),
            "--dir",
            dir.to_str().unwrap(),
            "--cluster-enabled",
            "yes",
            "--threads",
            "2",
            "--no-pin",
            "--maxmemory",
            "20mb",
            "--maxmemory-policy",
            "allkeys-lru",
        ],
        port,
    ));
    let conn = connect_ready;
    let (mut c0, mut c1) = (conn(port), conn(port + 1));
    assert_eq!(
        resp_cmd(&mut c1, &["CONFIG", "GET", "maxmemory"]),
        "*2\r\n$9\r\nmaxmemory\r\n$8\r\n20971520\r\n"
    );
    let val = "x".repeat(1000);
    for i in 0..50_000 {
        let k = format!("mm{i}");
        let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2);
        let c = if shard == 0 { &mut c0 } else { &mut c1 };
        let r = resp_cmd(c, &["SET", &k, &val]);
        assert_eq!(r, "+OK\r\n", "write {i} on shard {shard}");
    }
    let info = resp_cmd(&mut c0, &["INFO", "memory"]);
    let used: u64 = info
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(used < 22 << 20, "used_memory {used} over a 20 MB limit");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Pub/sub within one cluster-mode node: a message published on one
/// shard's port reaches subscribers connected to another shard's port
/// (PUBLISH used to return 0 and drop it).
#[test]
fn test_cluster_publish_reaches_every_shard_port_e2e() {
    let port = 18225u16;
    let dir = std::env::temp_dir().join(format!("rudis_cl_pubsub_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _srv = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &port.to_string(),
            "--dir",
            dir.to_str().unwrap(),
            "--cluster-enabled",
            "yes",
            "--threads",
            "2",
            "--no-pin",
        ],
        port,
    ));
    let conn = connect_ready;
    for (sub_port, pub_port) in [(port + 1, port), (port, port + 1)] {
        let mut sub = conn(sub_port);
        let ch = format!("ch{sub_port}");
        let r = resp_cmd(&mut sub, &["SUBSCRIBE", &ch]);
        assert!(r.contains("subscribe"), "{r}");
        let mut publisher = conn(pub_port);
        assert_eq!(
            resp_cmd(&mut publisher, &["PUBLISH", &ch, "hello"]),
            ":1\r\n"
        );
        let mut buf = [0u8; 256];
        let n = sub.read(&mut buf).unwrap();
        let got = String::from_utf8_lossy(&buf[..n]);
        assert!(got.contains("message") && got.contains("hello"), "{got}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// MONITOR on a cluster-mode node shows the commands sent to every shard's
/// port, not just its own.
#[test]
fn test_cluster_monitor_sees_every_shard_port_e2e() {
    let port = 18297u16;
    let dir = std::env::temp_dir().join(format!("rudis_cl_monitor_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _srv = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &port.to_string(),
            "--dir",
            dir.to_str().unwrap(),
            "--cluster-enabled",
            "yes",
            "--threads",
            "2",
            "--no-pin",
        ],
        port,
    ));
    let conn = connect_ready;
    let mut mon = conn(port);
    assert_eq!(resp_cmd(&mut mon, &["MONITOR"]), "+OK\r\n");
    let mut other = conn(port + 1);
    let _ = resp_cmd(&mut other, &["ECHO", "from-shard-1"]);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut seen = String::new();
    while !seen.contains("from-shard-1") {
        assert!(std::time::Instant::now() < deadline, "monitor saw: {seen}");
        let mut buf = [0u8; 1024];
        if let Ok(n) = mon.read(&mut buf) {
            seen.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// READONLY on a cluster replica: reads for its master's slots are served
/// there; writes, and reads after READWRITE, are still redirected.
#[test]
fn test_cluster_replica_serves_readonly_reads_e2e() {
    let (mport, rport) = (18237u16, 18243u16);
    let mk_dir = |p: u16| {
        let d = std::env::temp_dir().join(format!("rudis_ro_{}_{}", p, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_str().unwrap().to_string()
    };
    let spawn = |p: u16, dir: &str| {
        ServerGuard(spawn_rudis_listening(
            &[
                "--port",
                &p.to_string(),
                "--dir",
                dir,
                "--cluster-enabled",
                "yes",
                "--threads",
                "2",
                "--no-pin",
            ],
            p,
        ))
    };
    let (md, rd) = (mk_dir(mport), mk_dir(rport));
    let _m = spawn(mport, &md);
    let _r = spawn(rport, &rd);
    let cmd = |p: u16, args: &[&str]| {
        let mut c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        resp_cmd(&mut c, args)
    };
    let master_id = cmd(mport, &["CLUSTER", "MYID"])
        .lines()
        .nth(1)
        .unwrap()
        .to_string();
    assert_eq!(
        cmd(rport, &["CLUSTER", "DELSLOTSRANGE", "0", "16383"]),
        "+OK\r\n"
    );
    assert_eq!(
        cmd(rport, &["CLUSTER", "MEET", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !cmd(rport, &["CLUSTER", "NODES"]).contains(&master_id) {
        assert!(std::time::Instant::now() < deadline);
        thread::sleep(Duration::from_millis(50));
    }
    // Slots known on the replica's side before acting on them.
    while !cmd(rport, &["CLUSTER", "SLOTS"]).contains(&format!(":{mport}")) {
        assert!(std::time::Instant::now() < deadline);
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(cmd(rport, &["CLUSTER", "REPLICATE", &master_id]), "+OK\r\n");
    let k = "ro_key";
    let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2) as u16;
    assert_eq!(cmd(mport + shard, &["SET", k, "v"]), "+OK\r\n");
    while cmd(rport, &["DBSIZE"]) != ":1\r\n" {
        assert!(std::time::Instant::now() < deadline, "replica never synced");
        thread::sleep(Duration::from_millis(50));
    }
    let mut c = TcpStream::connect(("127.0.0.1", rport + shard)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert!(resp_cmd(&mut c, &["GET", k]).starts_with("-MOVED"));
    assert_eq!(resp_cmd(&mut c, &["READONLY"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", k]), "$1\r\nv\r\n");
    assert!(resp_cmd(&mut c, &["SET", k, "w"]).starts_with("-MOVED"));
    assert_eq!(resp_cmd(&mut c, &["READWRITE"]), "+OK\r\n");
    assert!(resp_cmd(&mut c, &["GET", k]).starts_with("-MOVED"));
    let _ = std::fs::remove_dir_all(&md);
    let _ = std::fs::remove_dir_all(&rd);
}

/// `cluster-node-timeout` sets how fast a dead master is failed over: at
/// 1 s its replica takes over in well under the default's 5 s.
#[test]
fn test_cluster_node_timeout_is_configurable_e2e() {
    let ports = [18249u16, 18255, 18261, 18267];
    let ranges = [(0, 5460), (5461, 10922), (10923, 16383)];
    let dirs: Vec<String> = ports
        .iter()
        .map(|p| {
            let d = std::env::temp_dir().join(format!("rudis_nto_{}_{}", p, std::process::id()));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
            d.to_str().unwrap().to_string()
        })
        .collect();
    let mut servers: Vec<Option<ServerGuard>> = ports
        .iter()
        .zip(&dirs)
        .map(|(p, d)| {
            Some(ServerGuard(spawn_rudis_listening(
                &[
                    "--port",
                    &p.to_string(),
                    "--dir",
                    d,
                    "--cluster-enabled",
                    "yes",
                    "--cluster-node-timeout",
                    "1000",
                    "--threads",
                    "2",
                    "--no-pin",
                ],
                *p,
            )))
        })
        .collect();
    let cmd = |p: u16, args: &[&str]| {
        let mut c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        resp_cmd(&mut c, args)
    };
    assert_eq!(
        cmd(ports[0], &["CONFIG", "GET", "cluster-node-timeout"]),
        "*2\r\n$20\r\ncluster-node-timeout\r\n$4\r\n1000\r\n"
    );
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
    let victim_id = cmd(ports[2], &["CLUSTER", "MYID"])
        .lines()
        .nth(1)
        .unwrap()
        .to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !cmd(ports[3], &["CLUSTER", "NODES"]).contains(&victim_id) {
        assert!(std::time::Instant::now() < deadline);
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        cmd(ports[3], &["CLUSTER", "REPLICATE", &victim_id]),
        "+OK\r\n"
    );
    wait_for_slot_views(&ports);
    while !cmd(ports[3], &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline);
        thread::sleep(Duration::from_millis(50));
    }
    servers[2] = None; // SIGKILL
    let started = std::time::Instant::now();
    loop {
        let nodes = cmd(ports[3], &["CLUSTER", "NODES"]);
        let me = nodes
            .lines()
            .find(|l| l.contains("myself"))
            .unwrap_or_default()
            .to_string();
        if me.contains("master") && me.ends_with(" 10923-16383") {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(15), "{nodes}");
        thread::sleep(Duration::from_millis(50));
    }
    eprintln!("promoted after {:?}", started.elapsed());
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
    drop(servers);
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A manual CLUSTER FAILOVER while a client keeps writing to the master:
/// the master pauses its writers, the replica catches up and takes over,
/// and no acknowledged write is lost (the counter on the new master equals
/// the last INCR the old one acknowledged); writers then get MOVED.
#[test]
fn test_cluster_manual_failover_loses_no_acknowledged_write_e2e() {
    let (mport, rport) = (18273u16, 18279u16);
    let mk_dir = |p: u16| {
        let d = std::env::temp_dir().join(format!("rudis_mf_{}_{}", p, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_str().unwrap().to_string()
    };
    let spawn = |p: u16, dir: &str| {
        ServerGuard(spawn_rudis_listening(
            &[
                "--port",
                &p.to_string(),
                "--dir",
                dir,
                "--cluster-enabled",
                "yes",
                "--threads",
                "2",
                "--no-pin",
            ],
            p,
        ))
    };
    let (md, rd) = (mk_dir(mport), mk_dir(rport));
    let _m = spawn(mport, &md);
    let _r = spawn(rport, &rd);
    let cmd = |p: u16, args: &[&str]| {
        let mut c = TcpStream::connect(("127.0.0.1", p)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        resp_cmd(&mut c, args)
    };
    let master_id = cmd(mport, &["CLUSTER", "MYID"])
        .lines()
        .nth(1)
        .unwrap()
        .to_string();
    assert_eq!(
        cmd(rport, &["CLUSTER", "DELSLOTSRANGE", "0", "16383"]),
        "+OK\r\n"
    );
    assert_eq!(
        cmd(rport, &["CLUSTER", "MEET", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !cmd(rport, &["CLUSTER", "SLOTS"]).contains(&format!(":{mport}")) {
        assert!(std::time::Instant::now() < deadline);
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(cmd(rport, &["CLUSTER", "REPLICATE", &master_id]), "+OK\r\n");
    while !cmd(rport, &["INFO", "replication"]).contains("master_link_status:up") {
        assert!(std::time::Instant::now() < deadline);
        thread::sleep(Duration::from_millis(50));
    }
    let k = "mf_counter";
    let shard = rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2) as u16;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut c = TcpStream::connect(("127.0.0.1", mport + shard)).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut last_acked = 0i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let r = resp_cmd(&mut c, &["INCR", k]);
                match r.strip_prefix(':') {
                    Some(n) => last_acked = n.trim().parse().unwrap(),
                    None => {
                        assert!(r.starts_with("-MOVED"), "{r}");
                        break;
                    }
                }
            }
            last_acked
        })
    };
    thread::sleep(Duration::from_millis(300));
    assert_eq!(cmd(rport, &["CLUSTER", "FAILOVER"]), "+OK\r\n");
    let last_acked = {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !writer.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "writer never redirected"
            );
            thread::sleep(Duration::from_millis(20));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap()
    };
    assert!(last_acked > 0);
    assert_eq!(
        cmd(rport + shard, &["GET", k]),
        format!("${}\r\n{last_acked}\r\n", last_acked.to_string().len()),
        "acknowledged writes lost in the failover"
    );
    let _ = std::fs::remove_dir_all(&md);
    let _ = std::fs::remove_dir_all(&rd);
}

/// WATCH on a cluster-mode node holds across its shard ports: a FLUSHALL
/// sent to the base port aborts the EXEC of a client watching on another
/// shard's port (the EXEC used to go through).
#[test]
fn test_cluster_watch_sees_flushall_from_another_port_e2e() {
    let port = 18285u16;
    let dir = std::env::temp_dir().join(format!("rudis_cl_watch_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _srv = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &port.to_string(),
            "--dir",
            dir.to_str().unwrap(),
            "--cluster-enabled",
            "yes",
            "--threads",
            "2",
            "--no-pin",
        ],
        port,
    ));
    let conn = connect_ready;
    let k = (0..1000)
        .map(|i| format!("w{i}"))
        .find(|k| rudis::router::slot_to_shard(rudis::router::key_slot(k.as_bytes()), 2) == 1)
        .unwrap();
    let mut w = conn(port + 1);
    assert_eq!(resp_cmd(&mut w, &["SET", &k, "1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut w, &["WATCH", &k]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut conn(port), &["FLUSHALL"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut w, &["MULTI"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut w, &["SET", &k, "2"]), "+QUEUED\r\n");
    assert_eq!(resp_cmd(&mut w, &["EXEC"]), "*-1\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CLIENT KILL on a cluster-mode node closes a client connected on another
/// shard's port (it used to report the kill and leave it connected).
#[test]
fn test_cluster_client_kill_reaches_every_shard_port_e2e() {
    let port = 18291u16;
    let dir = std::env::temp_dir().join(format!("rudis_cl_kill_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _srv = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &port.to_string(),
            "--dir",
            dir.to_str().unwrap(),
            "--cluster-enabled",
            "yes",
            "--threads",
            "2",
            "--no-pin",
        ],
        port,
    ));
    let mut victim = TcpStream::connect(("127.0.0.1", port + 1)).unwrap();
    victim
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let id = resp_cmd(&mut victim, &["CLIENT", "ID"]);
    let id = id.trim().trim_start_matches(':').to_string();
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert_eq!(
        resp_cmd(&mut admin, &["CLIENT", "KILL", "ID", &id]),
        ":1\r\n"
    );
    victim.write_all(b"*1\r\n$4\r\nPING\r\n").ok();
    let mut buf = [0u8; 64];
    let n = victim.read(&mut buf).unwrap_or(0);
    assert_eq!(
        n,
        0,
        "killed client still answered: {:?}",
        String::from_utf8_lossy(&buf[..n])
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A slot moves in batches of keys over one connection to the target,
/// not a new connection per batch.
#[test]
fn test_slot_migration_reuses_one_connection_e2e() {
    let (port1, port2) = (29811u16, 29812u16);
    start_test_server(port1, 2);
    start_test_server(port2, 2);
    let mut c1 = TcpStream::connect(("127.0.0.1", port1)).unwrap();
    let mut c2 = TcpStream::connect(("127.0.0.1", port2)).unwrap();
    c1.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let n = 1000;
    for i in 0..n {
        assert_eq!(
            resp_cmd(&mut c1, &["SET", &format!("{{mig}}:{i}"), "v"]),
            "+OK\r\n"
        );
    }
    let slot = rudis::router::key_slot(b"{mig}:0");
    let accepted = |c: &mut TcpStream| info_u64(c, "stats", "total_connections_received");
    let before = accepted(&mut c2);
    assert_eq!(
        resp_cmd(
            &mut c1,
            &[
                "CLUSTER",
                "MIGRATE-SLOT",
                &slot.to_string(),
                "127.0.0.1",
                &port2.to_string()
            ]
        ),
        "+OK\r\n"
    );
    let opened = accepted(&mut c2) - before;
    // SETSLOT IMPORTING, the key transfer and SETSLOT NODE: 3, where one
    // per batch of 100 keys made it 12.
    assert!(opened <= 4, "{opened} connections for {n} keys");
    assert_eq!(resp_cmd(&mut c2, &["DBSIZE"]), format!(":{n}\r\n"));
}
