use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use rudis::router::target_shard;
use rudis::server::run_shard_worker;
use rudis::shard::ShardMessage;

fn start_test_server(port: u16, num_shards: usize) {
    let mut senders = Vec::with_capacity(num_shards);
    let mut receivers = Vec::with_capacity(num_shards);

    for _ in 0..num_shards {
        let (tx, rx) = flume::unbounded::<ShardMessage>();
        senders.push(tx);
        receivers.push(rx);
    }

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders.clone();
        thread::Builder::new()
            .name(format!("test-shard-{}", shard_id))
            .spawn(move || {
                run_shard_worker(shard_id, num_shards, port, shard_senders, rx, None);
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
    let resp = send_and_read(&mut stream, b"*5\r\n$3\r\nSET\r\n$6\r\nex_key\r\n$8\r\ntemp_val\r\n$2\r\nEX\r\n$1\r\n1\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut stream, b"TTL ex_key\r\n");
    assert!(resp == ":1\r\n" || resp == ":0\r\n", "Expected TTL 1 or 0, got {}", resp);

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
    assert_eq!(resp1, resp2, "Keys with same hash tag must have identical slot");
    assert!(resp1.starts_with(':'), "Slot response should be integer");

    // Parse the slot number
    let slot_str = resp1.trim_matches(|c| c == ':' || c == '\r' || c == '\n');
    let slot: u16 = slot_str.parse().unwrap();
    assert!(slot < 16384, "Slot must be < 16384");

    // 12b. Populate keys in slot and query COUNTKEYSINSLOT & GETKEYSINSLOT
    let _ = send_and_read(&mut stream, b"SET {user:42}:k1 val1\r\n");
    let _ = send_and_read(&mut stream, b"SET {user:42}:k2 val2\r\n");
    let _ = send_and_read(&mut stream, b"SET {user:42}:k3 val3\r\n");

    let count_resp = send_and_read(&mut stream, format!("CLUSTER COUNTKEYSINSLOT {}\r\n", slot).as_bytes());
    assert_eq!(count_resp, ":3\r\n");

    let get_keys_resp = send_and_read(&mut stream, format!("CLUSTER GETKEYSINSLOT {} 10\r\n", slot).as_bytes());
    assert!(get_keys_resp.starts_with("*3\r\n"), "Expected 3 keys returned");
    assert!(get_keys_resp.contains("{user:42}:k1"));
    assert!(get_keys_resp.contains("{user:42}:k2"));
    assert!(get_keys_resp.contains("{user:42}:k3"));

    // 12c. CLUSTER SLOTS, NODES, INFO
    let slots_resp = send_and_read(&mut stream, b"CLUSTER SLOTS\r\n");
    assert!(slots_resp.starts_with("*4\r\n"), "Expected 4 shard slots ranges");

    let nodes_resp = send_and_read(&mut stream, b"CLUSTER NODES\r\n");
    assert!(nodes_resp.contains("myself,master"));
    assert!(nodes_resp.contains("connected"));

    let info_resp = send_and_read(&mut stream, b"CLUSTER INFO\r\n");
    assert!(info_resp.contains("cluster_state:ok"));
    assert!(info_resp.contains("cluster_slots_assigned:16384"));

    // 13. Test Connection Management and Client Tracking (CLIENT ID, SETNAME, GETNAME, LIST)
    let id_resp = send_and_read(&mut stream, b"CLIENT ID\r\n");
    assert!(id_resp.starts_with(':'), "CLIENT ID should return an integer: {}", id_resp);
    let client_id: u64 = id_resp.trim_matches(|c| c == ':' || c == '\r' || c == '\n').parse().unwrap();
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
    let client_id2: u64 = id_resp2.trim_matches(|c| c == ':' || c == '\r' || c == '\n').parse().unwrap();
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
    assert!(!list_resp3.contains(&format!("id={}", client_id2)), "Disconnected client should be removed");

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
        format!("CLUSTER SETSLOT {} MIGRATING 127.0.0.1:7001\r\n", local_slot).as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");

    // Existing key on migrating slot should still be returned
    let resp = send_and_read(&mut stream, b"GET local_key\r\n");
    assert_eq!(resp, "$6\r\nvalue1\r\n");

    // Non-existing key on migrating slot should return -ASK
    let tagged_missing = format!("{{local_key}}missing");
    assert_eq!(rudis::router::key_slot(tagged_missing.as_bytes()), local_slot);
    let resp = send_and_read(&mut stream, format!("GET {}\r\n", tagged_missing).as_bytes());
    assert_eq!(resp, format!("-ASK {} 127.0.0.1:7001\r\n", local_slot));

    // 3. Set slot to IMPORTING 127.0.0.1:7000
    let import_slot = 5000;
    let resp = send_and_read(
        &mut stream,
        format!("CLUSTER SETSLOT {} IMPORTING 127.0.0.1:7000\r\n", import_slot).as_bytes(),
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
        format!("SET {} imported_val\r\n", String::from_utf8_lossy(&target_key)).as_bytes(),
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

    // 4. Test MIGRATE on non-existing key returns +NOKEY
    let resp = send_and_read(
        &mut stream1,
        format!("MIGRATE 127.0.0.1 {} non_existing_key 0 5000\r\n", port2).as_bytes(),
    );
    assert_eq!(resp, "+NOKEY\r\n");
}
