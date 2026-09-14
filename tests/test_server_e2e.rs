use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use rudis::router::target_shard;
use rudis::server::run_shard_worker;
use rudis::shard::ShardMessage;

fn start_test_server(port: u16, num_shards: usize) {
    start_test_server_with_aof(port, num_shards, rudis::aof::AofConfig::default());
}

fn start_test_server_with_aof(port: u16, num_shards: usize, aof_config: rudis::aof::AofConfig) {
    let mut senders = Vec::with_capacity(num_shards);
    let mut receivers = Vec::with_capacity(num_shards);

    for _ in 0..num_shards {
        let (tx, rx) = flume::unbounded::<ShardMessage>();
        senders.push(tx);
        receivers.push(rx);
    }

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders.clone();
        let shard_aof_config = aof_config.clone();
        thread::Builder::new()
            .name(format!("test-shard-{}", shard_id))
            .spawn(move || {
                run_shard_worker(
                    shard_id,
                    num_shards,
                    port,
                    shard_senders,
                    rx,
                    None,
                    shard_aof_config,
                );
            })
            .expect("Failed to spawn test shard");
    }

    // Give server threads time to bind and listen
    thread::sleep(Duration::from_millis(200));
}

fn send_and_read(stream: &mut TcpStream, cmd: &[u8]) -> String {
    stream.write_all(cmd).unwrap();
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[test]
fn test_multithread_shared_nothing_e2e() {
    let port = 16380;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis server");

    // 1. Test PING
    let resp = send_and_read(&mut stream, b"PING\r\n");
    assert_eq!(resp, "+PONG\r\n");

    // 2. Test simple SET and GET (inline)
    let resp = send_and_read(&mut stream, b"SET user:1 alice\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut stream, b"GET user:1\r\n");
    assert_eq!(resp, "$5\r\nalice\r\n");

    // 3. Test PUT (requested alias for SET)
    let resp = send_and_read(&mut stream, b"PUT user:2 bob\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut stream, b"GET user:2\r\n");
    assert_eq!(resp, "$3\r\nbob\r\n");

    // 4. Test non-existent key
    let resp = send_and_read(&mut stream, b"GET non_existent_key\r\n");
    assert_eq!(resp, "$-1\r\n");

    // 5. Test cross-shard routing across all shards
    // Find keys that hit each of shard 0, 1, 2, 3
    let mut shard_hit = vec![false; num_shards];
    for i in 0..100 {
        let key = format!("cross_shard_key_{}", i);
        let s = target_shard(key.as_bytes(), num_shards);
        shard_hit[s] = true;

        let cmd = format!("SET {} value_{}\r\n", key, i);
        let resp = send_and_read(&mut stream, cmd.as_bytes());
        assert_eq!(resp, "+OK\r\n");

        let cmd = format!("GET {}\r\n", key);
        let resp = send_and_read(&mut stream, cmd.as_bytes());
        let expected = format!("${}\r\nvalue_{}\r\n", format!("value_{}", i).len(), i);
        assert_eq!(resp, expected);
    }

    // Verify all shards were tested
    for (shard, hit) in shard_hit.iter().enumerate() {
        assert!(hit, "Shard {} was never routed to!", shard);
    }

    // 6. Test RESP array format (standard redis-cli format)
    let set_resp_array = b"*3\r\n$3\r\nSET\r\n$7\r\nrespkey\r\n$7\r\nrespval\r\n";
    let resp = send_and_read(&mut stream, set_resp_array);
    assert_eq!(resp, "+OK\r\n");

    let get_resp_array = b"*2\r\n$3\r\nGET\r\n$7\r\nrespkey\r\n";
    let resp = send_and_read(&mut stream, get_resp_array);
    assert_eq!(resp, "$7\r\nrespval\r\n");

    // 7. Test EXISTS and DEL
    let resp = send_and_read(&mut stream, b"EXISTS respkey user:1 non_existent\r\n");
    assert_eq!(resp, ":2\r\n");

    let resp = send_and_read(&mut stream, b"DEL respkey user:1 non_existent\r\n");
    assert_eq!(resp, ":2\r\n");

    let resp = send_and_read(&mut stream, b"EXISTS respkey\r\n");
    assert_eq!(resp, ":0\r\n");

    let resp = send_and_read(&mut stream, b"GET respkey\r\n");
    assert_eq!(resp, "$-1\r\n");

    // 8. Test INCR, DECR, INCRBY, DECRBY
    let resp = send_and_read(&mut stream, b"INCR counter\r\n");
    assert_eq!(resp, ":1\r\n");

    let resp = send_and_read(&mut stream, b"INCR counter\r\n");
    assert_eq!(resp, ":2\r\n");

    let resp = send_and_read(&mut stream, b"INCRBY counter 10\r\n");
    assert_eq!(resp, ":12\r\n");

    let resp = send_and_read(&mut stream, b"DECR counter\r\n");
    assert_eq!(resp, ":11\r\n");

    let resp = send_and_read(&mut stream, b"DECRBY counter 5\r\n");
    assert_eq!(resp, ":6\r\n");

    let resp = send_and_read(&mut stream, b"GET counter\r\n");
    assert_eq!(resp, "$1\r\n6\r\n");

    // 9. Test MSET and MGET (scatter-gather across shards)
    let resp = send_and_read(&mut stream, b"MSET mkey1 alpha mkey2 beta mkey3 gamma\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut stream, b"MGET mkey1 mkey2 mkey3 non_existent\r\n");
    let expected = "*4\r\n$5\r\nalpha\r\n$4\r\nbeta\r\n$5\r\ngamma\r\n$-1\r\n";
    assert_eq!(resp, expected);

    // 10. Test TTL, PTTL, EXPIRE, and PERSIST
    // 10a. SET with EX (1 second expiration)
    let resp = send_and_read(
        &mut stream,
        b"*5\r\n$3\r\nSET\r\n$6\r\nex_key\r\n$8\r\ntemp_val\r\n$2\r\nEX\r\n$1\r\n1\r\n",
    );
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut stream, b"TTL ex_key\r\n");
    assert!(
        resp == ":1\r\n" || resp == ":0\r\n",
        "Expected TTL 1 or 0, got {}",
        resp
    );

    let resp = send_and_read(&mut stream, b"GET ex_key\r\n");
    assert_eq!(resp, "$8\r\ntemp_val\r\n");

    // Wait for key to expire
    thread::sleep(Duration::from_millis(1100));

    let resp = send_and_read(&mut stream, b"GET ex_key\r\n");
    assert_eq!(resp, "$-1\r\n", "Key should have expired");

    let resp = send_and_read(&mut stream, b"TTL ex_key\r\n");
    assert_eq!(resp, ":-2\r\n", "TTL of expired key should be -2");

    // 10b. EXPIRE and PERSIST commands
    let resp = send_and_read(&mut stream, b"SET persist_key pval\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut stream, b"TTL persist_key\r\n");
    assert_eq!(resp, ":-1\r\n", "TTL of non-expiring key should be -1");

    let resp = send_and_read(&mut stream, b"EXPIRE persist_key 10\r\n");
    assert_eq!(resp, ":1\r\n");

    let resp = send_and_read(&mut stream, b"PERSIST persist_key\r\n");
    assert_eq!(resp, ":1\r\n");

    let resp = send_and_read(&mut stream, b"TTL persist_key\r\n");
    assert_eq!(resp, ":-1\r\n", "TTL after PERSIST should be -1");

    let resp = send_and_read(&mut stream, b"PERSIST nonexistent\r\n");
    assert_eq!(resp, ":0\r\n");

    let resp = send_and_read(&mut stream, b"EXPIRE nonexistent 10\r\n");
    assert_eq!(resp, ":0\r\n");

    // 11. Test Pipelined Squashing across multiple shards in a single write
    let mut pipeline_req = Vec::new();
    let mut expected_resp = String::new();
    for i in 0..50 {
        pipeline_req.extend_from_slice(format!("SET pipe_{} val_{}\r\n", i, i).as_bytes());
        expected_resp.push_str("+OK\r\n");
    }
    for i in 0..50 {
        pipeline_req.extend_from_slice(format!("GET pipe_{}\r\n", i).as_bytes());
        expected_resp.push_str(&format!("${}\r\nval_{}\r\n", format!("val_{}", i).len(), i));
    }

    stream.write_all(&pipeline_req).unwrap();
    let mut actual_resp = Vec::new();
    let mut total_read = 0;
    let expected_len = expected_resp.len();
    while total_read < expected_len {
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        actual_resp.extend_from_slice(&buf[..n]);
        total_read += n;
    }
    assert_eq!(String::from_utf8_lossy(&actual_resp), expected_resp);

    // 12. Test CLUSTER commands and Slot Migration secondary index
    // 12a. CLUSTER KEYSLOT with hash tag support
    let resp1 = send_and_read(&mut stream, b"CLUSTER KEYSLOT {user:42}:profile\r\n");
    let resp2 = send_and_read(&mut stream, b"CLUSTER KEYSLOT {user:42}:orders\r\n");
    assert_eq!(
        resp1, resp2,
        "Keys with same hash tag must have identical slot"
    );
    assert!(resp1.starts_with(':'), "Slot response should be integer");

    // Parse the slot number
    let slot_str = resp1.trim_matches(|c| c == ':' || c == '\r' || c == '\n');
    let slot: u16 = slot_str.parse().unwrap();
    assert!(slot < 16384, "Slot must be < 16384");

    // 12b. Populate keys in slot and query COUNTKEYSINSLOT & GETKEYSINSLOT
    let _ = send_and_read(&mut stream, b"SET {user:42}:k1 val1\r\n");
    let _ = send_and_read(&mut stream, b"SET {user:42}:k2 val2\r\n");
    let _ = send_and_read(&mut stream, b"SET {user:42}:k3 val3\r\n");

    let count_resp = send_and_read(
        &mut stream,
        format!("CLUSTER COUNTKEYSINSLOT {}\r\n", slot).as_bytes(),
    );
    assert_eq!(count_resp, ":3\r\n");

    let get_keys_resp = send_and_read(
        &mut stream,
        format!("CLUSTER GETKEYSINSLOT {} 10\r\n", slot).as_bytes(),
    );
    assert!(
        get_keys_resp.starts_with("*3\r\n"),
        "Expected 3 keys returned"
    );
    assert!(get_keys_resp.contains("{user:42}:k1"));
    assert!(get_keys_resp.contains("{user:42}:k2"));
    assert!(get_keys_resp.contains("{user:42}:k3"));

    // 12c. CLUSTER SLOTS, NODES, INFO
    let slots_resp = send_and_read(&mut stream, b"CLUSTER SLOTS\r\n");
    assert!(
        slots_resp.starts_with("*4\r\n"),
        "Expected 4 shard slots ranges"
    );

    let nodes_resp = send_and_read(&mut stream, b"CLUSTER NODES\r\n");
    assert!(nodes_resp.contains("myself,master"));
    assert!(nodes_resp.contains("connected"));

    let info_resp = send_and_read(&mut stream, b"CLUSTER INFO\r\n");
    assert!(info_resp.contains("cluster_state:ok"));
    assert!(info_resp.contains("cluster_slots_assigned:16384"));

    // 13. Test Connection Management and Client Tracking (CLIENT ID, SETNAME, GETNAME, LIST)
    let id_resp = send_and_read(&mut stream, b"CLIENT ID\r\n");
    assert!(
        id_resp.starts_with(':'),
        "CLIENT ID should return an integer: {}",
        id_resp
    );
    let client_id: u64 = id_resp
        .trim_matches(|c| c == ':' || c == '\r' || c == '\n')
        .parse()
        .unwrap();
    assert!(client_id > 0);

    let getname_resp = send_and_read(&mut stream, b"CLIENT GETNAME\r\n");
    assert_eq!(getname_resp, "$-1\r\n", "Initial client name should be nil");

    let setname_resp = send_and_read(&mut stream, b"CLIENT SETNAME test_client_1\r\n");
    assert_eq!(setname_resp, "+OK\r\n");

    let getname_resp2 = send_and_read(&mut stream, b"CLIENT GETNAME\r\n");
    assert_eq!(getname_resp2, "$13\r\ntest_client_1\r\n");

    let list_resp = send_and_read(&mut stream, b"CLIENT LIST\r\n");
    assert!(list_resp.contains(&format!("id={}", client_id)));
    assert!(list_resp.contains("name=test_client_1"));

    // Connect a second client to test multi-client listing and cross-core aggregation
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let id_resp2 = send_and_read(&mut stream2, b"CLIENT ID\r\n");
    let client_id2: u64 = id_resp2
        .trim_matches(|c| c == ':' || c == '\r' || c == '\n')
        .parse()
        .unwrap();
    assert_ne!(client_id, client_id2);

    let _ = send_and_read(&mut stream2, b"CLIENT SETNAME test_client_2\r\n");
    let list_resp2 = send_and_read(&mut stream, b"CLIENT LIST\r\n");
    assert!(list_resp2.contains(&format!("id={}", client_id)));
    assert!(list_resp2.contains(&format!("id={}", client_id2)));
    assert!(list_resp2.contains("name=test_client_1"));
    assert!(list_resp2.contains("name=test_client_2"));

    // Drop second client and verify cleanup
    drop(stream2);
    thread::sleep(Duration::from_millis(50));
    let list_resp3 = send_and_read(&mut stream, b"CLIENT LIST\r\n");
    assert!(list_resp3.contains(&format!("id={}", client_id)));
    assert!(
        !list_resp3.contains(&format!("id={}", client_id2)),
        "Disconnected client should be removed"
    );

    // 14. Test Redis Hash data structure (HSET, HGET, HMGET, HDEL, HEXISTS, HLEN, HGETALL, HKEYS, HVALS)
    let resp = send_and_read(&mut stream, b"HSET user:100 name alice age 30 city nyc\r\n");
    assert_eq!(resp, ":3\r\n", "Should return 3 fields added");

    // Adding existing field updates and returns 0 new fields
    let resp = send_and_read(&mut stream, b"HSET user:100 age 31\r\n");
    assert_eq!(resp, ":0\r\n", "Updating existing field returns 0 added");

    let resp = send_and_read(&mut stream, b"HGET user:100 name\r\n");
    assert_eq!(resp, "$5\r\nalice\r\n");

    let resp = send_and_read(&mut stream, b"HGET user:100 age\r\n");
    assert_eq!(resp, "$2\r\n31\r\n");

    let resp = send_and_read(&mut stream, b"HGET user:100 nonexistent\r\n");
    assert_eq!(resp, "$-1\r\n");

    let resp = send_and_read(&mut stream, b"HMGET user:100 name age nonexistent\r\n");
    assert_eq!(resp, "*3\r\n$5\r\nalice\r\n$2\r\n31\r\n$-1\r\n");

    let resp = send_and_read(&mut stream, b"HEXISTS user:100 city\r\n");
    assert_eq!(resp, ":1\r\n");

    let resp = send_and_read(&mut stream, b"HEXISTS user:100 nonexistent\r\n");
    assert_eq!(resp, ":0\r\n");

    let resp = send_and_read(&mut stream, b"HLEN user:100\r\n");
    assert_eq!(resp, ":3\r\n");

    let resp = send_and_read(&mut stream, b"HGETALL user:100\r\n");
    assert!(resp.starts_with("*6\r\n"));
    assert!(resp.contains("$4\r\ncity\r\n$3\r\nnyc\r\n") || resp.contains("$3\r\nnyc\r\n"));

    let resp = send_and_read(&mut stream, b"HKEYS user:100\r\n");
    assert!(resp.starts_with("*3\r\n"));
    assert!(resp.contains("name") && resp.contains("age") && resp.contains("city"));

    let resp = send_and_read(&mut stream, b"HVALS user:100\r\n");
    assert!(resp.starts_with("*3\r\n"));
    assert!(resp.contains("alice") && resp.contains("31") && resp.contains("nyc"));

    let resp = send_and_read(&mut stream, b"HDEL user:100 age nonexistent\r\n");
    assert_eq!(resp, ":1\r\n", "Should delete 1 existing field");

    let resp = send_and_read(&mut stream, b"HLEN user:100\r\n");
    assert_eq!(resp, ":2\r\n");

    // Test WRONGTYPE: performing INCR on a Hash key
    let resp = send_and_read(&mut stream, b"INCR user:100\r\n");
    assert!(resp.starts_with("-ERR WRONGTYPE"));

    // Test cross-shard Hash operations via pipelined squashing
    let mut hash_pipe = Vec::new();
    let mut expected_prefix = String::new();
    for i in 0..20 {
        hash_pipe.extend_from_slice(format!("HSET hash_pipe_{} f1 v1 f2 v2\r\n", i).as_bytes());
        expected_prefix.push_str(":2\r\n");
    }
    for i in 0..20 {
        hash_pipe.extend_from_slice(format!("HGET hash_pipe_{} f1\r\n", i).as_bytes());
        expected_prefix.push_str("$2\r\nv1\r\n");
    }
    stream.write_all(&hash_pipe).unwrap();
    let mut actual_hash_resp = Vec::new();
    let mut total_read = 0;
    let expected_len = expected_prefix.len();
    while total_read < expected_len {
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        actual_hash_resp.extend_from_slice(&buf[..n]);
        total_read += n;
    }
    assert_eq!(String::from_utf8_lossy(&actual_hash_resp), expected_prefix);
}

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
    let tagged_missing = format!("{{local_key}}missing");
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
fn test_lists_and_sets_e2e() {
    let port = 16384;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis server");

    // --- LIST TESTS ---
    // LPUSH
    let resp = send_and_read(&mut stream, b"LPUSH mylist world\r\n");
    assert_eq!(resp, ":1\r\n");
    let resp = send_and_read(&mut stream, b"LPUSH mylist hello\r\n");
    assert_eq!(resp, ":2\r\n");

    // RPUSH
    let resp = send_and_read(&mut stream, b"RPUSH mylist foo bar\r\n");
    assert_eq!(resp, ":4\r\n");

    // LLEN
    let resp = send_and_read(&mut stream, b"LLEN mylist\r\n");
    assert_eq!(resp, ":4\r\n");

    // LRANGE
    let resp = send_and_read(&mut stream, b"LRANGE mylist 0 -1\r\n");
    assert_eq!(
        resp,
        "*4\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    );

    let resp = send_and_read(&mut stream, b"LRANGE mylist 1 2\r\n");
    assert_eq!(resp, "*2\r\n$5\r\nworld\r\n$3\r\nfoo\r\n");

    // LINDEX
    let resp = send_and_read(&mut stream, b"LINDEX mylist 0\r\n");
    assert_eq!(resp, "$5\r\nhello\r\n");

    let resp = send_and_read(&mut stream, b"LINDEX mylist -1\r\n");
    assert_eq!(resp, "$3\r\nbar\r\n");

    let resp = send_and_read(&mut stream, b"LINDEX mylist 100\r\n");
    assert_eq!(resp, "$-1\r\n");

    // LPOP single
    let resp = send_and_read(&mut stream, b"LPOP mylist\r\n");
    assert_eq!(resp, "$5\r\nhello\r\n");

    // RPOP with count
    let resp = send_and_read(&mut stream, b"RPOP mylist 2\r\n");
    assert_eq!(resp, "*2\r\n$3\r\nbar\r\n$3\r\nfoo\r\n");

    // LLEN should be 1 now
    let resp = send_and_read(&mut stream, b"LLEN mylist\r\n");
    assert_eq!(resp, ":1\r\n");

    // Pop remaining element
    let resp = send_and_read(&mut stream, b"LPOP mylist\r\n");
    assert_eq!(resp, "$5\r\nworld\r\n");

    // Now empty
    let resp = send_and_read(&mut stream, b"LLEN mylist\r\n");
    assert_eq!(resp, ":0\r\n");

    let resp = send_and_read(&mut stream, b"LPOP mylist\r\n");
    assert_eq!(resp, "$-1\r\n");

    // WRONGTYPE test
    let resp = send_and_read(&mut stream, b"SET str_key test_val\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut stream, b"LPUSH str_key val\r\n");
    assert!(resp.starts_with("-ERR WRONGTYPE"));

    // --- SET TESTS ---
    // SADD
    let resp = send_and_read(&mut stream, b"SADD myset a b c a\r\n");
    assert_eq!(resp, ":3\r\n");

    // SCARD
    let resp = send_and_read(&mut stream, b"SCARD myset\r\n");
    assert_eq!(resp, ":3\r\n");

    // SISMEMBER
    let resp = send_and_read(&mut stream, b"SISMEMBER myset a\r\n");
    assert_eq!(resp, ":1\r\n");
    let resp = send_and_read(&mut stream, b"SISMEMBER myset z\r\n");
    assert_eq!(resp, ":0\r\n");

    // SMEMBERS
    let resp = send_and_read(&mut stream, b"SMEMBERS myset\r\n");
    assert!(resp.starts_with("*3\r\n"));
    assert!(resp.contains("a") && resp.contains("b") && resp.contains("c"));

    // SREM
    let resp = send_and_read(&mut stream, b"SREM myset b nonexistent\r\n");
    assert_eq!(resp, ":1\r\n");
    let resp = send_and_read(&mut stream, b"SCARD myset\r\n");
    assert_eq!(resp, ":2\r\n");

    // SPOP single
    let resp = send_and_read(&mut stream, b"SPOP myset\r\n");
    assert!(resp.starts_with("$1\r\n"));
    let resp = send_and_read(&mut stream, b"SCARD myset\r\n");
    assert_eq!(resp, ":1\r\n");

    // SPOP count
    let resp = send_and_read(&mut stream, b"SPOP myset 5\r\n");
    assert_eq!(resp.lines().next().unwrap(), "*1");
    let resp = send_and_read(&mut stream, b"SCARD myset\r\n");
    assert_eq!(resp, ":0\r\n");

    let resp = send_and_read(&mut stream, b"SPOP myset\r\n");
    assert_eq!(resp, "$-1\r\n");

    // WRONGTYPE on Set
    let resp = send_and_read(&mut stream, b"SADD str_key elem\r\n");
    assert!(resp.starts_with("-ERR WRONGTYPE"));

    // --- PIPELINED SQUASHED CROSS-SHARD OPERATIONS ---
    let mut pipe = Vec::new();
    let mut expected_prefix = String::new();
    for i in 0..20 {
        pipe.extend_from_slice(format!("RPUSH pipe_list_{} v1 v2 v3\r\n", i).as_bytes());
        expected_prefix.push_str(":3\r\n");
        pipe.extend_from_slice(format!("SADD pipe_set_{} m1 m2\r\n", i).as_bytes());
        expected_prefix.push_str(":2\r\n");
    }
    for i in 0..20 {
        pipe.extend_from_slice(format!("LLEN pipe_list_{}\r\n", i).as_bytes());
        expected_prefix.push_str(":3\r\n");
        pipe.extend_from_slice(format!("SCARD pipe_set_{}\r\n", i).as_bytes());
        expected_prefix.push_str(":2\r\n");
    }

    stream.write_all(&pipe).unwrap();
    let mut actual_resp = Vec::new();
    let mut total_read = 0;
    let expected_len = expected_prefix.len();
    while total_read < expected_len {
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        actual_resp.extend_from_slice(&buf[..n]);
        total_read += n;
    }
    assert_eq!(String::from_utf8_lossy(&actual_resp), expected_prefix);
}

#[test]
fn test_aof_persistence_and_replay_e2e() {
    let aof_dir = std::env::temp_dir().join(format!("rudis-aof-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&aof_dir);
    std::fs::create_dir_all(&aof_dir).unwrap();

    let port_server1 = 16392;
    let num_shards = 4;
    let aof_config1 = rudis::aof::AofConfig {
        enabled: true,
        dir: aof_dir.clone(),
        fsync_every_sec: true,
    };

    // 1. Start Server 1 with AOF enabled
    start_test_server_with_aof(port_server1, num_shards, aof_config1);

    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", port_server1))
        .expect("Failed to connect to rudis server 1");

    // Write String data
    let resp = send_and_read(&mut stream1, b"SET user:a alice\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut stream1, b"SET user:b bob\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut stream1, b"SET to_delete val\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut stream1, b"DEL to_delete\r\n");
    assert_eq!(resp, ":1\r\n");

    // Write Counter
    let resp = send_and_read(&mut stream1, b"INCRBY page_views 100\r\n");
    assert_eq!(resp, ":100\r\n");

    // Write Hash
    let resp = send_and_read(&mut stream1, b"HSET myhash name rudis version 1\r\n");
    assert_eq!(resp, ":2\r\n");
    let resp = send_and_read(&mut stream1, b"HDEL myhash version\r\n");
    assert_eq!(resp, ":1\r\n");

    // Write List
    let resp = send_and_read(&mut stream1, b"RPUSH mylist a b c d\r\n");
    assert_eq!(resp, ":4\r\n");
    let resp = send_and_read(&mut stream1, b"LPOP mylist\r\n");
    assert_eq!(resp, "$1\r\na\r\n");

    // Write Set
    let resp = send_and_read(&mut stream1, b"SADD myset s1 s2 s3\r\n");
    assert_eq!(resp, ":3\r\n");
    let resp = send_and_read(&mut stream1, b"SREM myset s1\r\n");
    assert_eq!(resp, ":1\r\n");

    // Write key with TTL (60 seconds)
    let resp = send_and_read(&mut stream1, b"SET ttl_key temp_val PX 60000\r\n");
    assert_eq!(resp, "+OK\r\n");

    // Write Sorted Set
    let resp = send_and_read(
        &mut stream1,
        b"ZADD myzset 100 alice 200 bob 300 charlie\r\n",
    );
    assert_eq!(resp, ":3\r\n");
    let resp = send_and_read(&mut stream1, b"ZINCRBY myzset 50 alice\r\n");
    assert_eq!(resp, "$3\r\n150\r\n");
    let resp = send_and_read(&mut stream1, b"ZPOPMIN myzset\r\n");
    assert_eq!(resp, "*2\r\n$5\r\nalice\r\n$3\r\n150\r\n");

    // Sync all shards to disk via SAVE
    let resp = send_and_read(&mut stream1, b"SAVE\r\n");
    assert_eq!(resp, "+OK\r\n");

    drop(stream1);
    thread::sleep(Duration::from_millis(200));

    // Verify AOF files were created
    let mut aof_files_found = 0;
    for sid in 0..num_shards {
        let p = aof_dir.join(format!("appendonly-{}.aof", sid));
        if p.exists() && p.metadata().map(|m| m.len()).unwrap_or(0) > 0 {
            aof_files_found += 1;
        }
    }
    assert!(
        aof_files_found > 0,
        "At least one shard AOF file should exist and contain data"
    );

    // 2. Start Server 2 on a new port using the SAME AOF directory
    let port_server2 = 16393;
    let aof_config2 = rudis::aof::AofConfig {
        enabled: true,
        dir: aof_dir.clone(),
        fsync_every_sec: true,
    };
    start_test_server_with_aof(port_server2, num_shards, aof_config2);

    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", port_server2))
        .expect("Failed to connect to rudis server 2");

    // Verify Strings
    let resp = send_and_read(&mut stream2, b"GET user:a\r\n");
    assert_eq!(resp, "$5\r\nalice\r\n");
    let resp = send_and_read(&mut stream2, b"GET user:b\r\n");
    assert_eq!(resp, "$3\r\nbob\r\n");
    let resp = send_and_read(&mut stream2, b"GET to_delete\r\n");
    assert_eq!(resp, "$-1\r\n");

    // Verify Counter
    let resp = send_and_read(&mut stream2, b"GET page_views\r\n");
    assert_eq!(resp, "$3\r\n100\r\n");

    // Verify Hash
    let resp = send_and_read(&mut stream2, b"HGET myhash name\r\n");
    assert_eq!(resp, "$5\r\nrudis\r\n");
    let resp = send_and_read(&mut stream2, b"HEXISTS myhash version\r\n");
    assert_eq!(resp, ":0\r\n");

    // Verify List
    let resp = send_and_read(&mut stream2, b"LLEN mylist\r\n");
    assert_eq!(resp, ":3\r\n");
    let resp = send_and_read(&mut stream2, b"LRANGE mylist 0 -1\r\n");
    assert_eq!(resp, "*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n");

    // Verify Set
    let resp = send_and_read(&mut stream2, b"SCARD myset\r\n");
    assert_eq!(resp, ":2\r\n");
    let resp = send_and_read(&mut stream2, b"SISMEMBER myset s1\r\n");
    assert_eq!(resp, ":0\r\n");
    let resp = send_and_read(&mut stream2, b"SISMEMBER myset s2\r\n");
    assert_eq!(resp, ":1\r\n");
    let resp = send_and_read(&mut stream2, b"SISMEMBER myset s3\r\n");
    assert_eq!(resp, ":1\r\n");

    // Verify TTL
    let resp = send_and_read(&mut stream2, b"GET ttl_key\r\n");
    assert_eq!(resp, "$8\r\ntemp_val\r\n");
    let resp = send_and_read(&mut stream2, b"TTL ttl_key\r\n");
    assert!(resp.starts_with(':'));
    let ttl_val: i64 = resp.trim_start_matches(':').trim_end().parse().unwrap();
    assert!(ttl_val > 0, "TTL should be positive");

    // Verify Sorted Set
    let resp = send_and_read(&mut stream2, b"ZCARD myzset\r\n");
    assert_eq!(resp, ":2\r\n");
    let resp = send_and_read(&mut stream2, b"ZSCORE myzset bob\r\n");
    assert_eq!(resp, "$3\r\n200\r\n");
    let resp = send_and_read(&mut stream2, b"ZSCORE myzset charlie\r\n");
    assert_eq!(resp, "$3\r\n300\r\n");
    let resp = send_and_read(&mut stream2, b"ZSCORE myzset alice\r\n");
    assert_eq!(resp, "$-1\r\n");

    // Cleanup
    drop(stream2);
    let _ = std::fs::remove_dir_all(&aof_dir);
}

#[test]
fn test_sorted_sets_zset_e2e() {
    let port = 16385;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis server");

    // 1. ZADD multiple elements
    let resp = send_and_read(
        &mut stream,
        b"ZADD myzset 10 one 20 two 30 three 40 four\r\n",
    );
    assert_eq!(resp, ":4\r\n");

    // 2. ZCARD
    let resp = send_and_read(&mut stream, b"ZCARD myzset\r\n");
    assert_eq!(resp, ":4\r\n");

    // 3. ZSCORE
    let resp = send_and_read(&mut stream, b"ZSCORE myzset two\r\n");
    assert_eq!(resp, "$2\r\n20\r\n");
    let resp = send_and_read(&mut stream, b"ZSCORE myzset nonexistent\r\n");
    assert_eq!(resp, "$-1\r\n");

    // 4. ZRANK and ZREVRANK
    let resp = send_and_read(&mut stream, b"ZRANK myzset one\r\n");
    assert_eq!(resp, ":0\r\n");
    let resp = send_and_read(&mut stream, b"ZRANK myzset two\r\n");
    assert_eq!(resp, ":1\r\n");
    let resp = send_and_read(&mut stream, b"ZREVRANK myzset four\r\n");
    assert_eq!(resp, ":0\r\n");
    let resp = send_and_read(&mut stream, b"ZREVRANK myzset three\r\n");
    assert_eq!(resp, ":1\r\n");

    // 5. ZCOUNT
    let resp = send_and_read(&mut stream, b"ZCOUNT myzset 15 35\r\n");
    assert_eq!(resp, ":2\r\n");
    let resp = send_and_read(&mut stream, b"ZCOUNT myzset (10 30\r\n");
    assert_eq!(resp, ":2\r\n"); // 20 and 30
    let resp = send_and_read(&mut stream, b"ZCOUNT myzset -inf +inf\r\n");
    assert_eq!(resp, ":4\r\n");

    // 6. ZINCRBY
    let resp = send_and_read(&mut stream, b"ZINCRBY myzset 15 one\r\n");
    assert_eq!(resp, "$2\r\n25\r\n");
    // New rank of 'one' is 1 (order: two=20, one=25, three=30, four=40)
    let resp = send_and_read(&mut stream, b"ZRANK myzset one\r\n");
    assert_eq!(resp, ":1\r\n");

    // 7. ZRANGE basic & WITHSCORES & REV
    let resp = send_and_read(&mut stream, b"ZRANGE myzset 0 -1\r\n");
    assert_eq!(
        resp,
        "*4\r\n$3\r\ntwo\r\n$3\r\none\r\n$5\r\nthree\r\n$4\r\nfour\r\n"
    );

    let resp = send_and_read(&mut stream, b"ZRANGE myzset 0 1 WITHSCORES\r\n");
    assert_eq!(
        resp,
        "*4\r\n$3\r\ntwo\r\n$2\r\n20\r\n$3\r\none\r\n$2\r\n25\r\n"
    );

    let resp = send_and_read(&mut stream, b"ZRANGE myzset 0 1 REV\r\n");
    assert_eq!(resp, "*2\r\n$4\r\nfour\r\n$5\r\nthree\r\n");

    // 8. ZRANGE BYSCORE
    let resp = send_and_read(&mut stream, b"ZRANGE myzset 20 30 BYSCORE\r\n");
    assert_eq!(resp, "*3\r\n$3\r\ntwo\r\n$3\r\none\r\n$5\r\nthree\r\n");

    // 9. Legacy commands: ZREVRANGE and ZRANGEBYSCORE
    let resp = send_and_read(&mut stream, b"ZREVRANGE myzset 0 1\r\n");
    assert_eq!(resp, "*2\r\n$4\r\nfour\r\n$5\r\nthree\r\n");

    let resp = send_and_read(&mut stream, b"ZRANGEBYSCORE myzset 20 25\r\n");
    assert_eq!(resp, "*2\r\n$3\r\ntwo\r\n$3\r\none\r\n");

    // 10. ZPOPMIN and ZPOPMAX
    let resp = send_and_read(&mut stream, b"ZPOPMIN myzset\r\n");
    assert_eq!(resp, "*2\r\n$3\r\ntwo\r\n$2\r\n20\r\n");

    let resp = send_and_read(&mut stream, b"ZPOPMAX myzset 1\r\n");
    assert_eq!(resp, "*2\r\n$4\r\nfour\r\n$2\r\n40\r\n");

    let resp = send_and_read(&mut stream, b"ZCARD myzset\r\n");
    assert_eq!(resp, ":2\r\n");

    // 11. ZREM
    let resp = send_and_read(&mut stream, b"ZREM myzset one three\r\n");
    assert_eq!(resp, ":2\r\n");
    let resp = send_and_read(&mut stream, b"ZCARD myzset\r\n");
    assert_eq!(resp, ":0\r\n");

    // 12. ZADD flags (NX, XX, GT, LT, CH, INCR)
    let resp = send_and_read(&mut stream, b"ZADD flagz 10 a\r\n");
    assert_eq!(resp, ":1\r\n");
    let resp = send_and_read(&mut stream, b"ZADD flagz NX 20 a\r\n");
    assert_eq!(resp, ":0\r\n"); // NX: already exists, no update
    let resp = send_and_read(&mut stream, b"ZADD flagz XX 20 a\r\n");
    assert_eq!(resp, ":0\r\n"); // XX: updated, but not new element (CH not set)
    let resp = send_and_read(&mut stream, b"ZADD flagz XX CH 30 a\r\n");
    assert_eq!(resp, ":1\r\n"); // CH set: changed element counted
    let resp = send_and_read(&mut stream, b"ZADD flagz GT 20 a\r\n");
    assert_eq!(resp, ":0\r\n"); // GT: 20 not > 30, no update
    let resp = send_and_read(&mut stream, b"ZADD flagz GT CH 40 a\r\n");
    assert_eq!(resp, ":1\r\n"); // GT: 40 > 30, updated
    let resp = send_and_read(&mut stream, b"ZADD flagz INCR 5 a\r\n");
    assert_eq!(resp, "$2\r\n45\r\n"); // INCR: 40 + 5 = 45

    // 13. Cross-shard routing test
    for i in 0..20 {
        let key = format!("zset_shard_key_{}", i);
        let cmd = format!("ZADD {} 10.5 mem_{}\r\n", key, i);
        let resp = send_and_read(&mut stream, cmd.as_bytes());
        assert_eq!(resp, ":1\r\n");

        let cmd = format!("ZSCORE {} mem_{}\r\n", key, i);
        let resp = send_and_read(&mut stream, cmd.as_bytes());
        assert_eq!(resp, "$4\r\n10.5\r\n");
    }

    // 14. WRONGTYPE error check
    send_and_read(&mut stream, b"SET str_test_key foo\r\n");
    let resp = send_and_read(&mut stream, b"ZADD str_test_key 10 m\r\n");
    assert!(resp.starts_with("-ERR WRONGTYPE"));
}

#[test]
fn test_generic_and_string_commands_e2e() {
    let port = 16386;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis server");

    // 1. Test TYPE
    let resp = send_and_read(&mut stream, b"TYPE non_existent_key\r\n");
    assert_eq!(resp, "+none\r\n");

    send_and_read(&mut stream, b"SET k_str hello\r\n");
    assert_eq!(send_and_read(&mut stream, b"TYPE k_str\r\n"), "+string\r\n");

    send_and_read(&mut stream, b"HSET k_hash f v\r\n");
    assert_eq!(send_and_read(&mut stream, b"TYPE k_hash\r\n"), "+hash\r\n");

    send_and_read(&mut stream, b"RPUSH k_list e\r\n");
    assert_eq!(send_and_read(&mut stream, b"TYPE k_list\r\n"), "+list\r\n");

    send_and_read(&mut stream, b"SADD k_set s\r\n");
    assert_eq!(send_and_read(&mut stream, b"TYPE k_set\r\n"), "+set\r\n");

    send_and_read(&mut stream, b"ZADD k_zset 1.0 z\r\n");
    assert_eq!(send_and_read(&mut stream, b"TYPE k_zset\r\n"), "+zset\r\n");

    // 2. Test DBSIZE
    let resp = send_and_read(&mut stream, b"DBSIZE\r\n");
    assert_eq!(resp, ":5\r\n");

    // 3. Test TOUCH
    let resp = send_and_read(&mut stream, b"TOUCH k_str k_hash non_existent\r\n");
    assert_eq!(resp, ":2\r\n");

    // 4. Test STRLEN and APPEND
    let resp = send_and_read(&mut stream, b"STRLEN k_str\r\n");
    assert_eq!(resp, ":5\r\n");

    let resp = send_and_read(&mut stream, b"APPEND k_str _world\r\n");
    assert_eq!(resp, ":11\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET k_str\r\n"),
        "$11\r\nhello_world\r\n"
    );

    // 5. Test SETNX
    let resp = send_and_read(&mut stream, b"SETNX k_str new_val\r\n");
    assert_eq!(resp, ":0\r\n"); // already exists
    let resp = send_and_read(&mut stream, b"SETNX k_new_nx brand_new\r\n");
    assert_eq!(resp, ":1\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET k_new_nx\r\n"),
        "$9\r\nbrand_new\r\n"
    );

    // 6. Test SETEX and PSETEX
    let resp = send_and_read(&mut stream, b"SETEX k_ex 100 ex_val\r\n");
    assert_eq!(resp, "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET k_ex\r\n"),
        "$6\r\nex_val\r\n"
    );
    let resp = send_and_read(&mut stream, b"TTL k_ex\r\n");
    assert!(resp.starts_with(':'));

    let resp = send_and_read(&mut stream, b"PSETEX k_pex 100000 pex_val\r\n");
    assert_eq!(resp, "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET k_pex\r\n"),
        "$7\r\npex_val\r\n"
    );

    // 7. Test GETSET
    let resp = send_and_read(&mut stream, b"GETSET k_getset initial\r\n");
    assert_eq!(resp, "$-1\r\n");
    let resp = send_and_read(&mut stream, b"GETSET k_getset updated\r\n");
    assert_eq!(resp, "$7\r\ninitial\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET k_getset\r\n"),
        "$7\r\nupdated\r\n"
    );

    // 8. Test GETDEL
    let resp = send_and_read(&mut stream, b"GETDEL k_getset\r\n");
    assert_eq!(resp, "$7\r\nupdated\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GETDEL k_getset\r\n"),
        "$-1\r\n"
    );

    // 9. Test RENAME and RENAMENX with hash tags (guaranteed same slot)
    send_and_read(&mut stream, b"SET {user:1}:tag_a val_a\r\n");
    let resp = send_and_read(&mut stream, b"RENAME {user:1}:tag_a {user:1}:tag_b\r\n");
    assert_eq!(resp, "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET {user:1}:tag_a\r\n"),
        "$-1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"GET {user:1}:tag_b\r\n"),
        "$5\r\nval_a\r\n"
    );

    send_and_read(&mut stream, b"SET {user:1}:tag_c val_c\r\n");
    let resp = send_and_read(&mut stream, b"RENAMENX {user:1}:tag_c {user:1}:tag_b\r\n");
    assert_eq!(resp, ":0\r\n"); // tag_b exists
    let resp = send_and_read(&mut stream, b"RENAMENX {user:1}:tag_c {user:1}:tag_d\r\n");
    assert_eq!(resp, ":1\r\n"); // tag_d does not exist

    // 10. Test MSETNX with same slot
    let resp = send_and_read(&mut stream, b"MSETNX {user:1}:m1 v1 {user:1}:m2 v2\r\n");
    assert_eq!(resp, ":1\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET {user:1}:m1\r\n"),
        "$2\r\nv1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"GET {user:1}:m2\r\n"),
        "$2\r\nv2\r\n"
    );

    let resp = send_and_read(&mut stream, b"MSETNX {user:1}:m1 new {user:1}:m3 v3\r\n");
    assert_eq!(resp, ":0\r\n"); // m1 exists, aborts all
    assert_eq!(
        send_and_read(&mut stream, b"EXISTS {user:1}:m3\r\n"),
        ":0\r\n"
    );

    // 11. Test EXPIREAT and PEXPIREAT
    let future_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let cmd = format!("EXPIREAT k_str {}\r\n", future_ts);
    let resp = send_and_read(&mut stream, cmd.as_bytes());
    assert_eq!(resp, ":1\r\n");

    // 12. Test FLUSHDB
    let resp = send_and_read(&mut stream, b"FLUSHDB\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = send_and_read(&mut stream, b"DBSIZE\r\n");
    assert_eq!(resp, ":0\r\n");
}

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
