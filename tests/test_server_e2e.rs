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

fn send_and_read_bytes(stream: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
    stream.write_all(cmd).unwrap();
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).unwrap();
    buf[..n].to_vec()
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

#[test]
fn test_keyspace_inspection_e2e() {
    let port = 16388;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");

    // 1. FLUSHDB and verify empty database
    let resp = send_and_read(&mut stream, b"FLUSHDB\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut stream, b"RANDOMKEY\r\n");
    assert_eq!(resp, "$-1\r\n");

    let resp = send_and_read(&mut stream, b"KEYS *\r\n");
    assert_eq!(resp, "*0\r\n");

    // 2. Populate keys across multiple shards
    assert_eq!(send_and_read(&mut stream, b"SET user:1 alice\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut stream, b"SET user:2 bob\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"SET product:100 apple\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SET product:200 banana\r\n"),
        "+OK\r\n"
    );

    // 3. Test KEYS pattern
    let resp = send_and_read(&mut stream, b"KEYS user:*\r\n");
    assert!(resp.starts_with("*2\r\n"));
    assert!(resp.contains("user:1") && resp.contains("user:2"));
    assert!(!resp.contains("product:"));

    let resp = send_and_read(&mut stream, b"KEYS *\r\n");
    assert!(resp.starts_with("*4\r\n"));
    assert!(
        resp.contains("user:1")
            && resp.contains("user:2")
            && resp.contains("product:100")
            && resp.contains("product:200")
    );

    let resp = send_and_read(&mut stream, b"KEYS *1*\r\n");
    assert!(resp.starts_with("*2\r\n"));
    assert!(resp.contains("user:1") && resp.contains("product:100"));

    // 4. Test RANDOMKEY
    let resp = send_and_read(&mut stream, b"RANDOMKEY\r\n");
    assert!(
        resp.contains("user:1")
            || resp.contains("user:2")
            || resp.contains("product:100")
            || resp.contains("product:200")
    );

    // 5. Test SCAN full iteration
    let mut scanned_keys = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let cmd = format!("SCAN {} COUNT 10\r\n", cursor);
        let resp = send_and_read(&mut stream, cmd.as_bytes());
        // Parse cursor from "*2\r\n$<len>\r\n<cursor>\r\n*<klen>\r\n..."
        let lines: Vec<&str> = resp.split("\r\n").collect();
        assert!(lines[0] == "*2");
        cursor = lines[2].to_string();
        for line in &lines[4..] {
            if !line.is_empty() && !line.starts_with('$') && !line.starts_with('*') {
                scanned_keys.push(line.to_string());
            }
        }
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(scanned_keys.len(), 4);
    assert!(scanned_keys.contains(&"user:1".to_string()));
    assert!(scanned_keys.contains(&"user:2".to_string()));
    assert!(scanned_keys.contains(&"product:100".to_string()));
    assert!(scanned_keys.contains(&"product:200".to_string()));

    // 6. Test SCAN with MATCH pattern
    let mut matched_keys = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let cmd = format!("SCAN {} MATCH user:* COUNT 10\r\n", cursor);
        let resp = send_and_read(&mut stream, cmd.as_bytes());
        let lines: Vec<&str> = resp.split("\r\n").collect();
        assert!(lines[0] == "*2");
        cursor = lines[2].to_string();
        for line in &lines[4..] {
            if !line.is_empty() && !line.starts_with('$') && !line.starts_with('*') {
                matched_keys.push(line.to_string());
            }
        }
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(matched_keys.len(), 2);
    assert!(matched_keys.contains(&"user:1".to_string()));
    assert!(matched_keys.contains(&"user:2".to_string()));

    // 7. Test EXPIRETIME and PEXPIRETIME
    assert_eq!(
        send_and_read(&mut stream, b"EXPIRETIME non_existing\r\n"),
        ":-2\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"PEXPIRETIME non_existing\r\n"),
        ":-2\r\n"
    );

    assert_eq!(
        send_and_read(&mut stream, b"EXPIRETIME user:1\r\n"),
        ":-1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"PEXPIRETIME user:1\r\n"),
        ":-1\r\n"
    );

    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert_eq!(
        send_and_read(&mut stream, b"EXPIRE user:1 100\r\n"),
        ":1\r\n"
    );

    let resp = send_and_read(&mut stream, b"EXPIRETIME user:1\r\n");
    let exp_ts: i64 = resp.trim_start_matches(':').trim().parse().unwrap();
    assert!((exp_ts - (now_unix + 100)).abs() <= 2);

    let resp = send_and_read(&mut stream, b"PEXPIRETIME user:1\r\n");
    let exp_ts_ms: i64 = resp.trim_start_matches(':').trim().parse().unwrap();
    let now_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!((exp_ts_ms - (now_unix_ms + 100_000)).abs() <= 2000);
}

#[test]
fn test_transactions_multi_exec_e2e() {
    let port = 16389;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");

    // 1. DISCARD/EXEC without MULTI errors
    assert_eq!(
        send_and_read(&mut stream, b"DISCARD\r\n"),
        "-ERR DISCARD without MULTI\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"EXEC\r\n"),
        "-ERR EXEC without MULTI\r\n"
    );

    // 2. DISCARD transaction
    assert_eq!(send_and_read(&mut stream, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"MULTI\r\n"),
        "-ERR MULTI calls can not be nested\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SET tx:discard_key v1\r\n"),
        "+QUEUED\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"DISCARD\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET tx:discard_key\r\n"),
        "$-1\r\n"
    );

    // 3. Successful MULTI / EXEC transaction
    assert_eq!(send_and_read(&mut stream, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"SET tx:key1 myval\r\n"),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"INCRBY tx:counter 10\r\n"),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"LPUSH tx:list a b\r\n"),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"GET tx:key1\r\n"),
        "+QUEUED\r\n"
    );

    let exec_resp = send_and_read(&mut stream, b"EXEC\r\n");
    assert_eq!(
        exec_resp,
        "*4\r\n+OK\r\n:10\r\n:2\r\n$5\r\nmyval\r\n"
    );

    assert_eq!(
        send_and_read(&mut stream, b"GET tx:counter\r\n"),
        "$2\r\n10\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"LLEN tx:list\r\n"),
        ":2\r\n"
    );

    // 4. Runtime error inside transaction (non-aborting, error returned as element in array)
    assert_eq!(send_and_read(&mut stream, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"SET tx:str hello\r\n"),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"LPUSH tx:str world\r\n"),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"GET tx:str\r\n"),
        "+QUEUED\r\n"
    );

    let exec_resp = send_and_read(&mut stream, b"EXEC\r\n");
    assert!(exec_resp.starts_with("*3\r\n+OK\r\n-ERR"));
    assert!(exec_resp.ends_with("$5\r\nhello\r\n"));

    // 5. Syntax error causing EXECABORT
    assert_eq!(send_and_read(&mut stream, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"SET tx:abort_key 1\r\n"),
        "+QUEUED\r\n"
    );
    let err_resp = send_and_read(&mut stream, b"SET\r\n");
    assert!(err_resp.starts_with("-ERR"));
    assert_eq!(
        send_and_read(&mut stream, b"EXEC\r\n"),
        "-EXECABORT Transaction discarded because of previous errors.\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"GET tx:abort_key\r\n"),
        "$-1\r\n"
    );
}

#[test]
fn test_vll_multi_shard_transactions_e2e() {
    let port = 16390;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client1 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client1");
    let mut client2 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client2");

    // Find keys that hit different shards
    let mut keys_by_shard = vec![String::new(); num_shards];
    let mut found = 0;
    let mut i = 0;
    while found < num_shards {
        let k = format!("vll_key_{}", i);
        let s = target_shard(k.as_bytes(), num_shards);
        if keys_by_shard[s].is_empty() {
            keys_by_shard[s] = k;
            found += 1;
        }
        i += 1;
    }

    let k0 = &keys_by_shard[0];
    let k1 = &keys_by_shard[1];
    let k2 = &keys_by_shard[2];
    let k3 = &keys_by_shard[3];

    // Multi-shard transaction touching all 4 shards
    assert_eq!(send_and_read(&mut client1, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client1, format!("SET {} val0\r\n", k0).as_bytes()),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, format!("SET {} val1\r\n", k1).as_bytes()),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, format!("SET {} val2\r\n", k2).as_bytes()),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, format!("SET {} val3\r\n", k3).as_bytes()),
        "+QUEUED\r\n"
    );

    let resp = send_and_read(&mut client1, b"EXEC\r\n");
    assert_eq!(resp, "*4\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n");

    // Verify all keys set across all shards
    assert_eq!(
        send_and_read(&mut client2, format!("GET {}\r\n", k0).as_bytes()),
        "$4\r\nval0\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, format!("GET {}\r\n", k1).as_bytes()),
        "$4\r\nval1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, format!("GET {}\r\n", k2).as_bytes()),
        "$4\r\nval2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, format!("GET {}\r\n", k3).as_bytes()),
        "$4\r\nval3\r\n"
    );

    // Concurrent multi-shard transactions with reverse shard keys (tests deterministic deadlock prevention)
    assert_eq!(send_and_read(&mut client1, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client1, format!("SET {} new0\r\n", k0).as_bytes()),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, format!("SET {} new3\r\n", k3).as_bytes()),
        "+QUEUED\r\n"
    );

    assert_eq!(send_and_read(&mut client2, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client2, format!("SET {} rev3\r\n", k3).as_bytes()),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, format!("SET {} rev0\r\n", k0).as_bytes()),
        "+QUEUED\r\n"
    );

    let resp1 = send_and_read(&mut client1, b"EXEC\r\n");
    let resp2 = send_and_read(&mut client2, b"EXEC\r\n");
    assert_eq!(resp1, "*2\r\n+OK\r\n+OK\r\n");
    assert_eq!(resp2, "*2\r\n+OK\r\n+OK\r\n");
}

#[test]
fn test_bitmaps_and_hyperloglog_e2e() {
    let port = 16391;
    let _server = start_test_server(port, 4);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. SETBIT and GETBIT
    // Set bits to produce byte 'a' (0b01100001: bits 1, 2, 7)
    assert_eq!(send_and_read(&mut client, b"SETBIT mybm 1 1\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"SETBIT mybm 2 1\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"SETBIT mybm 7 1\r\n"), ":0\r\n");
    // Setting bit 1 again should return old bit 1
    assert_eq!(send_and_read(&mut client, b"SETBIT mybm 1 1\r\n"), ":1\r\n");

    assert_eq!(send_and_read(&mut client, b"GETBIT mybm 1\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"GETBIT mybm 2\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"GETBIT mybm 3\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"GETBIT mybm 7\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"GETBIT mybm 100\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"GET mybm\r\n"), "$1\r\na\r\n");

    // 2. BITCOUNT
    assert_eq!(send_and_read(&mut client, b"BITCOUNT mybm\r\n"), ":3\r\n");
    // Set offset 15 (byte 1, bit 7)
    assert_eq!(send_and_read(&mut client, b"SETBIT mybm 15 1\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"BITCOUNT mybm\r\n"), ":4\r\n");
    assert_eq!(send_and_read(&mut client, b"BITCOUNT mybm 0 0\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client, b"BITCOUNT mybm 1 1\r\n"), ":1\r\n");

    // 3. BITPOS
    assert_eq!(send_and_read(&mut client, b"BITPOS mybm 1\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"BITPOS mybm 0\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"BITPOS mybm 1 1\r\n"), ":15\r\n");

    // 4. BITOP (AND, OR, XOR, NOT) using same hashtag to guarantee same shard
    assert_eq!(send_and_read(&mut client, b"SET {t}k1 \x0f\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SET {t}k2 \x33\r\n"), "+OK\r\n");

    assert_eq!(send_and_read(&mut client, b"BITOP AND {t}and {t}k1 {t}k2\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"GET {t}and\r\n"), "$1\r\n\x03\r\n");

    assert_eq!(send_and_read(&mut client, b"BITOP OR {t}or {t}k1 {t}k2\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"GET {t}or\r\n"), "$1\r\n\x3f\r\n");

    assert_eq!(send_and_read(&mut client, b"BITOP XOR {t}xor {t}k1 {t}k2\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"GET {t}xor\r\n"), "$1\r\n\x3c\r\n");

    assert_eq!(send_and_read(&mut client, b"BITOP NOT {t}not {t}k1\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read_bytes(&mut client, b"GET {t}not\r\n"),
        b"$1\r\n\xf0\r\n".to_vec()
    );

    // 5. HYPERLOGLOG: PFADD, PFCOUNT, PFMERGE
    assert_eq!(
        send_and_read(&mut client, b"PFADD {h}1 foo bar zap a\r\n"),
        ":1\r\n"
    );
    // Adding duplicates returns 0
    assert_eq!(
        send_and_read(&mut client, b"PFADD {h}1 foo bar\r\n"),
        ":0\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"PFCOUNT {h}1\r\n"), ":4\r\n");

    assert_eq!(
        send_and_read(&mut client, b"PFADD {h}2 a b c foo\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"PFCOUNT {h}2\r\n"), ":4\r\n");

    // Combined PFCOUNT across same shard keys
    assert_eq!(send_and_read(&mut client, b"PFCOUNT {h}1 {h}2\r\n"), ":6\r\n");

    // PFMERGE
    assert_eq!(send_and_read(&mut client, b"PFMERGE {h}dest {h}1 {h}2\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"PFCOUNT {h}dest\r\n"), ":6\r\n");

    // TYPE of HLL returns string
    assert_eq!(send_and_read(&mut client, b"TYPE {h}dest\r\n"), "+string\r\n");
}

#[test]
fn test_dump_and_restore_e2e() {
    let port = 16394;
    let _server = start_test_server(port, 4);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. DUMP non-existing key
    assert_eq!(send_and_read(&mut client, b"DUMP non_exist\r\n"), "$-1\r\n");

    // 2. SET and DUMP string
    assert_eq!(send_and_read(&mut client, b"SET mykey hello_world\r\n"), "+OK\r\n");
    let dump_resp = send_and_read_bytes(&mut client, b"DUMP mykey\r\n");
    assert!(dump_resp.starts_with(b"$"));

    // Extract payload from RESP bulk string "$<len>\r\n<payload>\r\n"
    let crlf_pos = dump_resp.windows(2).position(|w| w == b"\r\n").unwrap();
    let payload = &dump_resp[crlf_pos + 2..dump_resp.len() - 2];

    // 3. RESTORE to a new key
    let mut restore_cmd = Vec::new();
    restore_cmd.extend_from_slice(format!("*4\r\n$7\r\nRESTORE\r\n$7\r\ncopykey\r\n$1\r\n0\r\n${}\r\n", payload.len()).as_bytes());
    restore_cmd.extend_from_slice(payload);
    restore_cmd.extend_from_slice(b"\r\n");

    assert_eq!(send_and_read(&mut client, &restore_cmd), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"GET copykey\r\n"), "$11\r\nhello_world\r\n");

    // 4. RESTORE without REPLACE on existing key -> BUSYKEY error
    assert_eq!(
        send_and_read(&mut client, &restore_cmd),
        "-BUSYKEY Target key name already exists.\r\n"
    );

    // 5. RESTORE with REPLACE
    let mut restore_replace_cmd = Vec::new();
    restore_replace_cmd.extend_from_slice(format!("*5\r\n$7\r\nRESTORE\r\n$7\r\ncopykey\r\n$1\r\n0\r\n${}\r\n", payload.len()).as_bytes());
    restore_replace_cmd.extend_from_slice(payload);
    restore_replace_cmd.extend_from_slice(b"\r\n$7\r\nREPLACE\r\n");

    assert_eq!(send_and_read(&mut client, &restore_replace_cmd), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"GET copykey\r\n"), "$11\r\nhello_world\r\n");

    // 6. Corrupt checksum
    let mut corrupt_cmd = Vec::new();
    let mut corrupt_payload = payload.to_vec();
    let last = corrupt_payload.len() - 1;
    corrupt_payload[last] ^= 0xFF;
    corrupt_cmd.extend_from_slice(format!("*4\r\n$7\r\nRESTORE\r\n$9\r\ncorrupt_k\r\n$1\r\n0\r\n${}\r\n", corrupt_payload.len()).as_bytes());
    corrupt_cmd.extend_from_slice(&corrupt_payload);
    corrupt_cmd.extend_from_slice(b"\r\n");

    assert_eq!(
        send_and_read(&mut client, &corrupt_cmd),
        "-ERR DUMP payload version or checksum are wrong\r\n"
    );

    // 7. RESTORE with TTL (150ms)
    let mut restore_ttl_cmd = Vec::new();
    restore_ttl_cmd.extend_from_slice(format!("*4\r\n$7\r\nRESTORE\r\n$6\r\nttlkey\r\n$3\r\n150\r\n${}\r\n", payload.len()).as_bytes());
    restore_ttl_cmd.extend_from_slice(payload);
    restore_ttl_cmd.extend_from_slice(b"\r\n");

    assert_eq!(send_and_read(&mut client, &restore_ttl_cmd), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS ttlkey\r\n"), ":1\r\n");
    thread::sleep(Duration::from_millis(200));
    assert_eq!(send_and_read(&mut client, b"EXISTS ttlkey\r\n"), ":0\r\n");
}

#[test]
fn test_streams_engine_e2e() {
    let port = 16395;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis server");

    // 1. NOMKSTREAM on non-existent stream -> nil ($-1\r\n)
    assert_eq!(
        send_and_read(&mut client, b"XADD nonexist NOMKSTREAM * f1 v1\r\n"),
        "$-1\r\n"
    );

    // 2. XADD with explicit ID
    let r1 = send_and_read(&mut client, b"XADD mystream 1000-1 sensor temp val 25\r\n");
    assert_eq!(r1, "$6\r\n1000-1\r\n");

    // 3. TYPE mystream -> "stream"
    assert_eq!(send_and_read(&mut client, b"TYPE mystream\r\n"), "+stream\r\n");

    // 4. Monotonicity validation: specified ID <= top item
    let r_err = send_and_read(&mut client, b"XADD mystream 1000-1 f v\r\n");
    assert!(r_err.starts_with("-ERR"));

    // 5. XADD auto ID sequence
    let r2 = send_and_read(&mut client, b"XADD mystream 1000-* val 26\r\n");
    assert_eq!(r2, "$6\r\n1000-2\r\n");

    let r3 = send_and_read(&mut client, b"XADD mystream 1001-* val 27\r\n");
    assert_eq!(r3, "$6\r\n1001-0\r\n");

    // 6. XLEN
    assert_eq!(send_and_read(&mut client, b"XLEN mystream\r\n"), ":3\r\n");

    // 7. XRANGE full
    let range_all = send_and_read(&mut client, b"XRANGE mystream - +\r\n");
    assert!(range_all.starts_with("*3\r\n"));
    assert!(range_all.contains("1000-1"));
    assert!(range_all.contains("1000-2"));
    assert!(range_all.contains("1001-0"));

    // 8. XRANGE with COUNT 2
    let range_cnt = send_and_read(&mut client, b"XRANGE mystream - + COUNT 2\r\n");
    assert!(range_cnt.starts_with("*2\r\n"));
    assert!(range_cnt.contains("1000-1"));
    assert!(range_cnt.contains("1000-2"));
    assert!(!range_cnt.contains("1001-0"));

    // 9. XREVRANGE
    let rev_all = send_and_read(&mut client, b"XREVRANGE mystream + - COUNT 2\r\n");
    assert!(rev_all.starts_with("*2\r\n"));
    assert!(rev_all.contains("1001-0"));
    assert!(rev_all.contains("1000-2"));

    // 10. XREAD
    let read_resp = send_and_read(&mut client, b"XREAD STREAMS mystream 1000-1\r\n");
    assert!(read_resp.starts_with("*1\r\n"));
    assert!(read_resp.contains("mystream"));
    assert!(read_resp.contains("1000-2"));
    assert!(read_resp.contains("1001-0"));

    // XREAD with $
    let read_dollar = send_and_read(&mut client, b"XREAD STREAMS mystream $\r\n");
    assert_eq!(read_dollar, "$-1\r\n");

    // 11. XDEL
    assert_eq!(send_and_read(&mut client, b"XDEL mystream 1000-2\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"XLEN mystream\r\n"), ":2\r\n");

    // 12. XTRIM with MAXLEN
    for i in 10..20 {
        let cmd = format!("XADD mystream {}-0 item {}\r\n", 2000 + i, i);
        let _ = send_and_read(&mut client, cmd.as_bytes());
    }
    assert_eq!(send_and_read(&mut client, b"XLEN mystream\r\n"), ":12\r\n");
    assert_eq!(send_and_read(&mut client, b"XTRIM mystream MAXLEN = 5\r\n"), ":7\r\n");
    assert_eq!(send_and_read(&mut client, b"XLEN mystream\r\n"), ":5\r\n");

    // 13. DUMP & RESTORE of stream
    let dump_resp = send_and_read_bytes(&mut client, b"DUMP mystream\r\n");
    assert!(dump_resp.starts_with(b"$"));
    let crlf_pos = dump_resp.windows(2).position(|w| w == b"\r\n").unwrap();
    let payload = &dump_resp[crlf_pos + 2..dump_resp.len() - 2];

    let mut restore_cmd = Vec::new();
    restore_cmd.extend_from_slice(format!("*4\r\n$7\r\nRESTORE\r\n$11\r\nstream_copy\r\n$1\r\n0\r\n${}\r\n", payload.len()).as_bytes());
    restore_cmd.extend_from_slice(payload);
    restore_cmd.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &restore_cmd), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"XLEN stream_copy\r\n"), ":5\r\n");
    assert_eq!(send_and_read(&mut client, b"TYPE stream_copy\r\n"), "+stream\r\n");
}

#[test]
fn test_rdb_snapshot_forkless_e2e() {
    let port = 16396;
    let num_shards = 4;
    let rdb_dir = std::env::temp_dir().join(format!("rudis-rdb-e2e-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&rdb_dir);
    let aof_config = rudis::aof::AofConfig {
        enabled: false,
        dir: rdb_dir.clone(),
        fsync_every_sec: false,
    };
    start_test_server_with_aof(port, num_shards, aof_config);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Write various data types across shards
    assert_eq!(send_and_read(&mut client, b"SET rdb_str hello_world\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SET rdb_int 12345\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"HSET rdb_hash f1 v1 f2 v2\r\n"), ":2\r\n");
    assert_eq!(send_and_read(&mut client, b"RPUSH rdb_list a b c\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client, b"SADD rdb_set m1 m2\r\n"), ":2\r\n");
    assert_eq!(send_and_read(&mut client, b"ZADD rdb_zset 10 one 20 two\r\n"), ":2\r\n");

    // 2. Test LASTSAVE
    let lastsave_resp = send_and_read(&mut client, b"LASTSAVE\r\n");
    assert!(lastsave_resp.starts_with(':'), "LASTSAVE should return integer timestamp");

    // 3. Test synchronous SAVE
    let save_resp = send_and_read(&mut client, b"SAVE\r\n");
    assert_eq!(save_resp, "+OK\r\n");

    // 4. Verify dump.rdb file was created and has valid header
    let rdb_path = rdb_dir.join("dump.rdb");
    assert!(rdb_path.exists(), "dump.rdb should exist after SAVE");
    let content = std::fs::read(&rdb_path).unwrap();
    assert!(content.starts_with(b"REDIS0011"), "RDB file should have REDIS0011 header");

    // 5. Test asynchronous BGSAVE
    let bgsave_resp = send_and_read(&mut client, b"BGSAVE\r\n");
    assert_eq!(bgsave_resp, "+Background saving started\r\n");

    // Clean up dump file & dir
    let _ = std::fs::remove_dir_all(&rdb_dir);
}

#[test]
fn test_memory_compact_encodings_e2e() {
    let port = 16397;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Integer inlined value (RudisValue::Int)
    assert_eq!(send_and_read(&mut client, b"SET count 42\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"TYPE count\r\n"), "+string\r\n");
    assert_eq!(send_and_read(&mut client, b"GET count\r\n"), "$2\r\n42\r\n");
    assert_eq!(send_and_read(&mut client, b"STRLEN count\r\n"), ":2\r\n");

    // INCRBY on inlined integer (mutated in place without allocation)
    assert_eq!(send_and_read(&mut client, b"INCRBY count 10\r\n"), ":52\r\n");
    assert_eq!(send_and_read(&mut client, b"INCRBY count -100\r\n"), ":-48\r\n");
    assert_eq!(send_and_read(&mut client, b"GET count\r\n"), "$3\r\n-48\r\n");

    // APPEND promotes inlined Int to String
    assert_eq!(send_and_read(&mut client, b"APPEND count _extra\r\n"), ":9\r\n");
    assert_eq!(send_and_read(&mut client, b"GET count\r\n"), "$9\r\n-48_extra\r\n");

    // 2. Small Hash flat vector representation (RudisValue::SmallHash)
    assert_eq!(send_and_read(&mut client, b"HSET compact_hash a 1 b 2 c 3\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client, b"HLEN compact_hash\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client, b"HEXISTS compact_hash b\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"HGET compact_hash b\r\n"), "$1\r\n2\r\n");
    assert_eq!(send_and_read(&mut client, b"HDEL compact_hash b\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"HLEN compact_hash\r\n"), ":2\r\n");

    // Auto-promotion from SmallHash to full Hash when exceeding 64 keys
    let mut large_hset = String::from("HSET compact_hash");
    for i in 0..70 {
        large_hset.push_str(&format!(" k{} v{}", i, i));
    }
    large_hset.push_str("\r\n");
    let resp = send_and_read(&mut client, large_hset.as_bytes());
    assert!(resp.starts_with(':'));

    // Should now be promoted to full Hash and readable
    assert_eq!(send_and_read(&mut client, b"HLEN compact_hash\r\n"), ":72\r\n");
    assert_eq!(send_and_read(&mut client, b"HGET compact_hash k50\r\n"), "$3\r\nv50\r\n");
}

#[test]
fn test_streams_consumer_groups_e2e() {
    let port = 16398;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Create stream with initial entry
    assert_eq!(
        send_and_read(&mut client, b"XADD stream_cg 1000-0 sensor temp val 20\r\n"),
        "$6\r\n1000-0\r\n"
    );

    // 2. Create consumer group
    assert_eq!(
        send_and_read(&mut client, b"XGROUP CREATE stream_cg groupA $\r\n"),
        "+OK\r\n"
    );

    // Duplicate creation returns BUSYGROUP
    let dup_resp = send_and_read(&mut client, b"XGROUP CREATE stream_cg groupA $\r\n");
    assert!(dup_resp.contains("BUSYGROUP"), "Duplicate group should return BUSYGROUP");

    // 3. Create consumer
    assert_eq!(
        send_and_read(&mut client, b"XGROUP CREATECONSUMER stream_cg groupA worker1\r\n"),
        ":1\r\n"
    );

    // 4. Add new entries that arrive after group was created at $
    assert_eq!(
        send_and_read(&mut client, b"XADD stream_cg 1001-0 sensor temp val 25\r\n"),
        "$6\r\n1001-0\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"XADD stream_cg 1002-0 sensor temp val 30\r\n"),
        "$6\r\n1002-0\r\n"
    );

    // 5. Read from group as consumer worker1
    let read_resp = send_and_read(
        &mut client,
        b"XREADGROUP GROUP groupA worker1 COUNT 1 STREAMS stream_cg >\r\n"
    );
    assert!(read_resp.contains("1001-0"), "Should receive entry 1001-0");

    // 6. Inspect PEL using XPENDING summary
    let pending_summary = send_and_read(&mut client, b"XPENDING stream_cg groupA\r\n");
    assert!(pending_summary.starts_with("*4\r\n:1\r\n"), "Summary should report 1 pending entry");
    assert!(pending_summary.contains("1001-0"));
    assert!(pending_summary.contains("worker1"));

    // 7. Inspect PEL range
    let pending_range = send_and_read(&mut client, b"XPENDING stream_cg groupA - + 10\r\n");
    assert!(pending_range.contains("1001-0"));
    assert!(pending_range.contains("worker1"));

    // 8. Acknowledge entry
    assert_eq!(
        send_and_read(&mut client, b"XACK stream_cg groupA 1001-0\r\n"),
        ":1\r\n"
    );

    // 9. Confirm PEL is now 0
    let pending_after = send_and_read(&mut client, b"XPENDING stream_cg groupA\r\n");
    assert!(pending_after.starts_with("*4\r\n:0\r\n"), "PEL should now be empty");

    // 10. Destroy consumer group
    assert_eq!(
        send_and_read(&mut client, b"XGROUP DESTROY stream_cg groupA\r\n"),
        ":1\r\n"
    );
}

#[test]
fn test_cluster_gossip_and_meet_e2e() {
    let port1 = 16399;
    let port2 = 16400;
    start_test_server(port1, 2);
    start_test_server(port2, 2);

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
    assert!(nodes.contains("myself,master"), "Should show myself as master");
    assert!(nodes.contains(&format!("127.0.0.1:{}", port2)), "Should list the met remote node");
}

#[test]
fn test_rdb_cold_start_restore_e2e() {
    let port1 = 16401;
    let num_shards = 4;
    let rdb_dir = std::env::temp_dir().join(format!("rudis-cold-start-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&rdb_dir);
    let aof_config1 = rudis::aof::AofConfig {
        enabled: false,
        dir: rdb_dir.clone(),
        fsync_every_sec: false,
    };
    start_test_server_with_aof(port1, num_shards, aof_config1);

    let mut client1 = TcpStream::connect(("127.0.0.1", port1)).unwrap();

    // Populate keys across shards
    assert_eq!(send_and_read(&mut client1, b"SET cold_str \"rudis_is_fast\"\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client1, b"SET cold_int 424242\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client1, b"HSET cold_hash name rudis speed maximum\r\n"), ":2\r\n");
    assert_eq!(send_and_read(&mut client1, b"RPUSH cold_list alpha beta gamma\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client1, b"SADD cold_set s1 s2 s3\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client1, b"ZADD cold_zset 100 z1 200 z2\r\n"), ":2\r\n");

    // Save RDB snapshot
    assert_eq!(send_and_read(&mut client1, b"SAVE\r\n"), "+OK\r\n");
    let rdb_path = rdb_dir.join("dump.rdb");
    assert!(rdb_path.exists(), "dump.rdb must exist");

    drop(client1);
    thread::sleep(Duration::from_millis(200));

    // Start Server 2 on port 16402 with the SAME rdb_dir and AOF disabled
    let port2 = 16402;
    let aof_config2 = rudis::aof::AofConfig {
        enabled: false,
        dir: rdb_dir.clone(),
        fsync_every_sec: false,
    };
    start_test_server_with_aof(port2, num_shards, aof_config2);

    let mut client2 = TcpStream::connect(("127.0.0.1", port2)).unwrap();

    // Verify all keys restored cleanly across shards
    assert_eq!(send_and_read(&mut client2, b"GET cold_str\r\n"), "$15\r\n\"rudis_is_fast\"\r\n");
    assert_eq!(send_and_read(&mut client2, b"GET cold_int\r\n"), "$6\r\n424242\r\n");
    assert_eq!(send_and_read(&mut client2, b"HGET cold_hash name\r\n"), "$5\r\nrudis\r\n");
    assert_eq!(send_and_read(&mut client2, b"HGET cold_hash speed\r\n"), "$7\r\nmaximum\r\n");
    assert_eq!(send_and_read(&mut client2, b"LLEN cold_list\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client2, b"LRANGE cold_list 0 -1\r\n"), "*3\r\n$5\r\nalpha\r\n$4\r\nbeta\r\n$5\r\ngamma\r\n");
    assert_eq!(send_and_read(&mut client2, b"SCARD cold_set\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client2, b"ZCARD cold_zset\r\n"), ":2\r\n");

    let _ = std::fs::remove_dir_all(&rdb_dir);
}

#[test]
fn test_cluster_migrate_slot_e2e() {
    let port1 = 16403;
    let port2 = 16404;
    start_test_server(port1, 2);
    start_test_server(port2, 2);

    let mut client1 = TcpStream::connect(("127.0.0.1", port1)).unwrap();
    let mut client2 = TcpStream::connect(("127.0.0.1", port2)).unwrap();

    // Write a key on server1
    let key = "migrated_key";
    let slot = rudis::router::key_slot(key.as_bytes());
    assert_eq!(send_and_read(&mut client1, format!("SET {} \"transferred\"\r\n", key).as_bytes()), "+OK\r\n");
    assert_eq!(send_and_read(&mut client1, format!("GET {}\r\n", key).as_bytes()), "$13\r\n\"transferred\"\r\n");

    // Migrate this slot to server2
    let migrate_cmd = format!("CLUSTER MIGRATE-SLOT {} 127.0.0.1 {}\r\n", slot, port2);
    assert_eq!(send_and_read(&mut client1, migrate_cmd.as_bytes()), "+OK\r\n");

    // Server 1 should now redirect with MOVED for this slot
    let moved_resp = send_and_read(&mut client1, format!("GET {}\r\n", key).as_bytes());
    assert_eq!(moved_resp, format!("-MOVED {} 127.0.0.1:{}\r\n", slot, port2));

    // Server 2 should now have the key
    assert_eq!(send_and_read(&mut client2, format!("GET {}\r\n", key).as_bytes()), "$13\r\n\"transferred\"\r\n");

    // Test CLUSTER REBALANCE
    let rebal_resp = send_and_read(&mut client1, format!("CLUSTER REBALANCE 127.0.0.1 {} 2\r\n", port2).as_bytes());
    assert_eq!(rebal_resp, ":2\r\n");
}

#[test]
fn test_blocking_operations_e2e() {
    let port = 16405;
    start_test_server(port, 2);

    let mut client1 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    client1.set_read_timeout(Some(Duration::from_secs(4))).unwrap();

    // 1. Immediate BLPOP and BRPOP
    assert_eq!(send_and_read(&mut client1, b"RPUSH {t}:list a b c\r\n"), ":3\r\n");
    let resp = send_and_read(&mut client1, b"BLPOP {t}:list 1\r\n");
    assert_eq!(resp, "*2\r\n$8\r\n{t}:list\r\n$1\r\na\r\n");

    let resp = send_and_read(&mut client1, b"BRPOP {t}:list 1\r\n");
    assert_eq!(resp, "*2\r\n$8\r\n{t}:list\r\n$1\r\nc\r\n");

    // Pop the remaining element 'b'
    assert_eq!(send_and_read(&mut client1, b"LPOP {t}:list\r\n"), "$1\r\nb\r\n");

    // 2. BLPOP timeout on empty list
    let start = std::time::Instant::now();
    let resp = send_and_read(&mut client1, b"BLPOP {t}:list 0.5\r\n");
    assert_eq!(resp, "*-1\r\n");
    assert!(start.elapsed() >= Duration::from_millis(400));

    // 3. BLPOP unblocked by concurrent LPUSH
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        let mut client2 = TcpStream::connect(("127.0.0.1", 16405)).unwrap();
        let r = send_and_read(&mut client2, b"LPUSH {t}:list \"woken_val\"\r\n");
        assert_eq!(r, ":1\r\n");
    });

    let resp = send_and_read(&mut client1, b"BLPOP {t}:list 3\r\n");
    assert_eq!(resp, "*2\r\n$8\r\n{t}:list\r\n$11\r\n\"woken_val\"\r\n");
    handle.join().unwrap();

    // 4. XREAD BLOCK timeout on non-existent stream
    let start = std::time::Instant::now();
    let resp = send_and_read(&mut client1, b"XREAD BLOCK 400 STREAMS {t}:stream $\r\n");
    assert_eq!(resp, "$-1\r\n");
    assert!(start.elapsed() >= Duration::from_millis(300));
}

#[test]
fn test_multi_key_set_and_zset_e2e() {
    let port = 16406;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. SET MULTI-KEY OPERATIONS
    assert_eq!(send_and_read(&mut client, b"SADD {s}:1 a b c\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client, b"SADD {s}:2 b c d\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client, b"SADD {s}:3 c d e\r\n"), ":3\r\n");

    // SINTER {s}:1 {s}:2 -> b, c (order independent check)
    let sinter_resp = send_and_read(&mut client, b"SINTER {s}:1 {s}:2\r\n");
    assert!(sinter_resp.contains("$1\r\nb\r\n"));
    assert!(sinter_resp.contains("$1\r\nc\r\n"));
    assert!(!sinter_resp.contains("$1\r\na\r\n"));

    // SDIFF {s}:1 {s}:2 -> a
    assert_eq!(send_and_read(&mut client, b"SDIFF {s}:1 {s}:2\r\n"), "*1\r\n$1\r\na\r\n");

    // SUNION {s}:1 {s}:2 -> a, b, c, d
    let sunion_resp = send_and_read(&mut client, b"SUNION {s}:1 {s}:2\r\n");
    assert_eq!(sunion_resp.lines().next().unwrap(), "*4");

    // SINTERSTORE
    assert_eq!(send_and_read(&mut client, b"SINTERSTORE {s}:inter {s}:1 {s}:2\r\n"), ":2\r\n");
    assert_eq!(send_and_read(&mut client, b"SCARD {s}:inter\r\n"), ":2\r\n");

    // SUNIONSTORE
    assert_eq!(send_and_read(&mut client, b"SUNIONSTORE {s}:union {s}:1 {s}:2\r\n"), ":4\r\n");
    assert_eq!(send_and_read(&mut client, b"SCARD {s}:union\r\n"), ":4\r\n");

    // SDIFFSTORE
    assert_eq!(send_and_read(&mut client, b"SDIFFSTORE {s}:diff {s}:1 {s}:2\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"SMEMBERS {s}:diff\r\n"), "*1\r\n$1\r\na\r\n");

    // 2. ZSET MULTI-KEY OPERATIONS
    assert_eq!(send_and_read(&mut client, b"ZADD {z}:1 1.0 a 2.0 b 3.0 c\r\n"), ":3\r\n");
    assert_eq!(send_and_read(&mut client, b"ZADD {z}:2 2.0 b 3.0 c 4.0 d\r\n"), ":3\r\n");

    // ZDIFF {z}:1 {z}:2 -> a
    assert_eq!(send_and_read(&mut client, b"ZDIFF 2 {z}:1 {z}:2\r\n"), "*1\r\n$1\r\na\r\n");

    // ZINTER {z}:1 {z}:2 WITHSCORES -> b:4, c:6
    let zinter_resp = send_and_read(&mut client, b"ZINTER 2 {z}:1 {z}:2 WITHSCORES\r\n");
    assert_eq!(zinter_resp, "*4\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n6\r\n");

    // ZUNION {z}:1 {z}:2 -> a, b, c, d
    let zunion_resp = send_and_read(&mut client, b"ZUNION 2 {z}:1 {z}:2\r\n");
    assert_eq!(zunion_resp.lines().next().unwrap(), "*4");

    // ZUNIONSTORE with WEIGHTS and AGGREGATE MAX
    assert_eq!(
        send_and_read(&mut client, b"ZUNIONSTORE {z}:out 2 {z}:1 {z}:2 WEIGHTS 2 3 AGGREGATE MAX\r\n"),
        ":4\r\n"
    );
    // c was (3.0*2=6 vs 3.0*3=9) -> MAX is 9.0
    assert_eq!(send_and_read(&mut client, b"ZSCORE {z}:out c\r\n"), "$1\r\n9\r\n");

    // ZINTERSTORE
    assert_eq!(send_and_read(&mut client, b"ZINTERSTORE {z}:inter 2 {z}:1 {z}:2\r\n"), ":2\r\n");
    assert_eq!(send_and_read(&mut client, b"ZCARD {z}:inter\r\n"), ":2\r\n");

    // ZDIFFSTORE
    assert_eq!(send_and_read(&mut client, b"ZDIFFSTORE {z}:diff 2 {z}:1 {z}:2\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"ZCARD {z}:diff\r\n"), ":1\r\n");
}

#[test]
fn test_auth_and_acl_e2e() {
    let port = 16407;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Check default user info
    assert_eq!(send_and_read(&mut client, b"ACL WHOAMI\r\n"), "$7\r\ndefault\r\n");
    let users = send_and_read(&mut client, b"ACL USERS\r\n");
    assert!(users.contains("$7\r\ndefault\r\n"));

    let list = send_and_read(&mut client, b"ACL LIST\r\n");
    assert!(list.contains("default on nopass"));

    // 2. Create user alice
    assert_eq!(
        send_and_read(&mut client, b"ACL SETUSER alice on >secret123 +@all ~*\r\n"),
        "+OK\r\n"
    );

    let getuser = send_and_read(&mut client, b"ACL GETUSER alice\r\n");
    assert!(getuser.contains("$9\r\nsecret123\r\n"));

    // 3. Authenticate as alice
    assert_eq!(send_and_read(&mut client, b"AUTH alice secret123\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"ACL WHOAMI\r\n"), "$5\r\nalice\r\n");

    // Wrong password check
    assert!(send_and_read(&mut client, b"AUTH alice wrongpass\r\n").starts_with("-WRONGPASS"));

    // 4. Delete user alice
    assert_eq!(send_and_read(&mut client, b"ACL DELUSER alice\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"ACL GETUSER alice\r\n"), "$-1\r\n");

    // 5. Enforce password on default user and verify NOAUTH
    assert_eq!(
        send_and_read(&mut client, b"ACL SETUSER default >defpass -nopass\r\n"),
        "+OK\r\n"
    );

    // Open new connection - must be unauthenticated now
    let mut new_client = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let noauth_resp = send_and_read(&mut new_client, b"PING\r\n");
    assert_eq!(noauth_resp, "-NOAUTH Authentication required.\r\n");

    // Authenticate
    assert_eq!(send_and_read(&mut new_client, b"AUTH default defpass\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut new_client, b"PING\r\n"), "+PONG\r\n");

    // Restore default user to nopass for subsequent tests
    assert_eq!(
        send_and_read(&mut new_client, b"ACL SETUSER default nopass\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_valkey_missing_features_e2e() {
    let port = 16410;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis server");

    // 1. Handshake & System commands: HELLO, RESET, TIME, ECHO
    let hello = send_and_read(&mut client, b"HELLO\r\n");
    assert!(hello.contains("server\r\n$6\r\nvalkey\r\n"));
    assert!(hello.contains("version\r\n$5\r\n7.2.0\r\n"));

    let hello_proto = send_and_read(&mut client, b"HELLO 3 SETNAME myapp\r\n");
    assert!(hello_proto.contains("proto\r\n:3\r\n"));
    let getname = send_and_read(&mut client, b"CLIENT GETNAME\r\n");
    assert_eq!(getname, "$5\r\nmyapp\r\n");

    let echo_resp = send_and_read(&mut client, b"ECHO hello_valkey\r\n");
    assert_eq!(echo_resp, "$12\r\nhello_valkey\r\n");

    let time_resp = send_and_read(&mut client, b"TIME\r\n");
    assert!(time_resp.starts_with("*2\r\n$"));

    let reset_resp = send_and_read(&mut client, b"RESET\r\n");
    assert_eq!(reset_resp, "+RESET\r\n");
    let getname_after_reset = send_and_read(&mut client, b"CLIENT GETNAME\r\n");
    assert_eq!(getname_after_reset, "$-1\r\n");

    // 2. Hash commands: HINCRBY, HINCRBYFLOAT, HRANDFIELD, HSCAN
    send_and_read(&mut client, b"HSET {h1} f1 10\r\n");
    let hincrby_resp = send_and_read(&mut client, b"HINCRBY {h1} f1 5\r\n");
    assert_eq!(hincrby_resp, ":15\r\n");

    let hincrbyfloat_resp = send_and_read(&mut client, b"HINCRBYFLOAT {h1} f1 2.5\r\n");
    assert_eq!(hincrbyfloat_resp, "$4\r\n17.5\r\n");

    send_and_read(&mut client, b"HSET {h1} f2 20\r\n");
    let hrand_single = send_and_read(&mut client, b"HRANDFIELD {h1}\r\n");
    assert!(hrand_single == "$2\r\nf1\r\n" || hrand_single == "$2\r\nf2\r\n");

    let hrand_count = send_and_read(&mut client, b"HRANDFIELD {h1} 2\r\n");
    assert_eq!(hrand_count.lines().next().unwrap(), "*2");

    let hrand_values = send_and_read(&mut client, b"HRANDFIELD {h1} 2 WITHVALUES\r\n");
    assert_eq!(hrand_values.lines().next().unwrap(), "*4");

    let hscan_resp = send_and_read(&mut client, b"HSCAN {h1} 0 MATCH f* COUNT 10\r\n");
    assert!(hscan_resp.starts_with("*2\r\n$1\r\n0\r\n"));

    // 3. Set commands: SMISMEMBER, SRANDMEMBER, SMOVE, SSCAN
    send_and_read(&mut client, b"SADD {s1} a b c\r\n");
    let smismember_resp = send_and_read(&mut client, b"SMISMEMBER {s1} a d b\r\n");
    assert_eq!(smismember_resp, "*3\r\n:1\r\n:0\r\n:1\r\n");

    let srand_single = send_and_read(&mut client, b"SRANDMEMBER {s1}\r\n");
    assert!(srand_single == "$1\r\na\r\n" || srand_single == "$1\r\nb\r\n" || srand_single == "$1\r\nc\r\n");

    let srand_count = send_and_read(&mut client, b"SRANDMEMBER {s1} 2\r\n");
    assert_eq!(srand_count.lines().next().unwrap(), "*2");

    let sscan_resp = send_and_read(&mut client, b"SSCAN {s1} 0\r\n");
    assert!(sscan_resp.starts_with("*2\r\n$1\r\n0\r\n"));

    // SMOVE same slot (using hash tags {s1})
    let smove_ok = send_and_read(&mut client, b"SMOVE {s1} {s1}_dst a\r\n");
    assert_eq!(smove_ok, ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"SISMEMBER {s1} a\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"SISMEMBER {s1}_dst a\r\n"), ":1\r\n");

    // SMOVE cross-slot error
    let smove_cross = send_and_read(&mut client, b"SMOVE {s1} cross_slot_dest b\r\n");
    assert!(smove_cross.starts_with("-CROSSSLOT"));

    // 4. Sorted Set commands: ZMSCORE, ZRANDMEMBER, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZLEXCOUNT, ZSCAN
    send_and_read(&mut client, b"ZADD {z1} 10 m1 20 m2 30 m3\r\n");
    let zmscore_resp = send_and_read(&mut client, b"ZMSCORE {z1} m1 non_existing m3\r\n");
    assert_eq!(zmscore_resp, "*3\r\n$2\r\n10\r\n$-1\r\n$2\r\n30\r\n");

    let zrand_single = send_and_read(&mut client, b"ZRANDMEMBER {z1}\r\n");
    assert!(zrand_single.starts_with("$2\r\nm"));

    let zrand_scores = send_and_read(&mut client, b"ZRANDMEMBER {z1} 2 WITHSCORES\r\n");
    assert_eq!(zrand_scores.lines().next().unwrap(), "*4");

    let zlexcount_resp = send_and_read(&mut client, b"ZLEXCOUNT {z1} [m1 (m3\r\n");
    assert_eq!(zlexcount_resp, ":2\r\n");

    let zscan_resp = send_and_read(&mut client, b"ZSCAN {z1} 0\r\n");
    assert!(zscan_resp.starts_with("*2\r\n$1\r\n0\r\n"));

    let zrem_rank = send_and_read(&mut client, b"ZREMRANGEBYRANK {z1} 0 0\r\n");
    assert_eq!(zrem_rank, ":1\r\n"); // removes m1

    let zrem_score = send_and_read(&mut client, b"ZREMRANGEBYSCORE {z1} 20 20\r\n");
    assert_eq!(zrem_score, ":1\r\n"); // removes m2

    let zrem_lex = send_and_read(&mut client, b"ZREMRANGEBYLEX {z1} [m3 [m3\r\n");
    assert_eq!(zrem_lex, ":1\r\n"); // removes m3
    assert_eq!(send_and_read(&mut client, b"ZCARD {z1}\r\n"), ":0\r\n");

    // 5. List commands: LTRIM, LSET, LREM, LPOS, LINSERT, LMOVE, BLMOVE
    send_and_read(&mut client, b"RPUSH {l1} zero one two three four five\r\n");
    let ltrim_resp = send_and_read(&mut client, b"LTRIM {l1} 1 4\r\n");
    assert_eq!(ltrim_resp, "+OK\r\n"); // now: one two three four

    let lset_resp = send_and_read(&mut client, b"LSET {l1} 1 updated\r\n");
    assert_eq!(lset_resp, "+OK\r\n"); // now: one updated three four

    let lpos_resp = send_and_read(&mut client, b"LPOS {l1} updated\r\n");
    assert_eq!(lpos_resp, ":1\r\n");

    let lrem_resp = send_and_read(&mut client, b"LREM {l1} 1 updated\r\n");
    assert_eq!(lrem_resp, ":1\r\n"); // now: one three four

    let linsert_resp = send_and_read(&mut client, b"LINSERT {l1} BEFORE three inserted\r\n");
    assert_eq!(linsert_resp, ":4\r\n"); // now: one inserted three four

    let lmove_resp = send_and_read(&mut client, b"LMOVE {l1} {l1}_dst LEFT RIGHT\r\n");
    assert_eq!(lmove_resp, "$3\r\none\r\n");

    let lmove_cross = send_and_read(&mut client, b"LMOVE {l1} cross_slot LEFT RIGHT\r\n");
    assert!(lmove_cross.starts_with("-CROSSSLOT"));

    // BLMOVE immediate
    let blmove_immediate = send_and_read(&mut client, b"BLMOVE {l1}_dst {l1} LEFT LEFT 0.1\r\n");
    assert_eq!(blmove_immediate, "$3\r\none\r\n");

    // BLMOVE timeout
    let blmove_timeout = send_and_read(&mut client, b"BLMOVE {empty_q} {empty_q}_dst LEFT RIGHT 0.1\r\n");
    assert_eq!(blmove_timeout, "$-1\r\n");

    // BLMOVE blocking cross-thread notification
    let pusher_port = port;
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        let mut pusher = TcpStream::connect(format!("127.0.0.1:{}", pusher_port)).unwrap();
        send_and_read(&mut pusher, b"LPUSH {blmove_q} blocked_item\r\n");
    });

    let blmove_blocked = send_and_read(&mut client, b"BLMOVE {blmove_q} {blmove_q}_dst LEFT RIGHT 2.0\r\n");
    assert_eq!(blmove_blocked, "$12\r\nblocked_item\r\n");
    let dest_pop = send_and_read(&mut client, b"RPOP {blmove_q}_dst\r\n");
    assert_eq!(dest_pop, "$12\r\nblocked_item\r\n");

    // 6. String commands: INCRBYFLOAT, SETRANGE, GETRANGE
    send_and_read(&mut client, b"SET str1 hello_valkey\r\n");
    let getrange_resp = send_and_read(&mut client, b"GETRANGE str1 0 4\r\n");
    assert_eq!(getrange_resp, "$5\r\nhello\r\n");

    let getrange_neg = send_and_read(&mut client, b"GETRANGE str1 -6 -1\r\n");
    assert_eq!(getrange_neg, "$6\r\nvalkey\r\n");

    let setrange_resp = send_and_read(&mut client, b"SETRANGE str1 6 world\r\n");
    assert_eq!(setrange_resp, ":12\r\n");
    assert_eq!(send_and_read(&mut client, b"GET str1\r\n"), "$12\r\nhello_worldy\r\n");

    send_and_read(&mut client, b"SET num 10.5\r\n");
    let incrbyfloat_resp = send_and_read(&mut client, b"INCRBYFLOAT num 2.25\r\n");
    assert_eq!(incrbyfloat_resp, "$5\r\n12.75\r\n");
}

#[test]
fn test_primary_replica_replication_e2e() {
    let master_port = 16420;
    let replica_port = 16421;

    let _master = start_test_server(master_port, 2);
    let _replica = start_test_server(replica_port, 2);

    let mut master_client = TcpStream::connect(format!("127.0.0.1:{}", master_port)).unwrap();
    let mut replica_client = TcpStream::connect(format!("127.0.0.1:{}", replica_port)).unwrap();

    // 1. Verify master role
    let master_role = send_and_read(&mut master_client, b"ROLE\r\n");
    assert!(master_role.starts_with("*3\r\n$6\r\nmaster\r\n"));

    // 2. Pre-populate data on master before replica connects
    assert_eq!(send_and_read(&mut master_client, b"SET init_k1 val1\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut master_client, b"SET init_k2 val2\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut master_client, b"HSET myhash field1 hello\r\n"), ":1\r\n");

    // 3. Initiate replication on replica
    let rep_resp = send_and_read(&mut replica_client, format!("REPLICAOF 127.0.0.1 {}\r\n", master_port).as_bytes());
    assert_eq!(rep_resp, "+OK\r\n");

    // Wait for handshake, RDB snapshot generation, transfer, and restore
    thread::sleep(Duration::from_millis(300));

    // 4. Verify replica role and link status
    let replica_role = send_and_read(&mut replica_client, b"ROLE\r\n");
    assert!(replica_role.contains("slave"), "Expected slave role, got {}", replica_role);
    assert!(replica_role.contains("connected"), "Expected connected state, got {}", replica_role);

    // 5. Verify pre-existing data was restored from RDB on replica
    assert_eq!(send_and_read(&mut replica_client, b"GET init_k1\r\n"), "$4\r\nval1\r\n");
    assert_eq!(send_and_read(&mut replica_client, b"GET init_k2\r\n"), "$4\r\nval2\r\n");
    assert_eq!(send_and_read(&mut replica_client, b"HGET myhash field1\r\n"), "$5\r\nhello\r\n");

    // 6. Test read-only replica enforcement
    let write_resp = send_and_read(&mut replica_client, b"SET forbidden_key write_val\r\n");
    assert!(write_resp.contains("READONLY"), "Expected READONLY error, got: {}", write_resp);

    // 7. Live streaming mutation replication
    assert_eq!(send_and_read(&mut master_client, b"SET live_key live_val\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut master_client, b"INCRBY live_counter 42\r\n"), ":42\r\n");
    assert_eq!(send_and_read(&mut master_client, b"RPUSH mylist itemA itemB\r\n"), ":2\r\n");

    // Wait for replication stream propagation
    thread::sleep(Duration::from_millis(150));

    // Verify replicated on replica
    assert_eq!(send_and_read(&mut replica_client, b"GET live_key\r\n"), "$8\r\nlive_val\r\n");
    assert_eq!(send_and_read(&mut replica_client, b"GET live_counter\r\n"), "$2\r\n42\r\n");
    assert_eq!(send_and_read(&mut replica_client, b"LRANGE mylist 0 -1\r\n"), "*2\r\n$5\r\nitemA\r\n$5\r\nitemB\r\n");

    // 8. Promotion via REPLICAOF NO ONE
    assert_eq!(send_and_read(&mut replica_client, b"REPLICAOF NO ONE\r\n"), "+OK\r\n");
    let promoted_role = send_and_read(&mut replica_client, b"ROLE\r\n");
    assert!(promoted_role.starts_with("*3\r\n$6\r\nmaster\r\n"), "Expected master after promotion, got {}", promoted_role);

    // Writes should now succeed on promoted node
    assert_eq!(send_and_read(&mut replica_client, b"SET promoted_key promoted_value\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut replica_client, b"GET promoted_key\r\n"), "$14\r\npromoted_value\r\n");
}

#[test]
fn test_lua_scripting_engine_e2e() {
    let port = 16430;
    let _server = start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    fn send_cmd(stream: &mut TcpStream, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        send_and_read(stream, out.as_bytes())
    }

    // 1. Primitive Return Values
    assert_eq!(send_cmd(&mut client, &["EVAL", "return 42", "0"]), ":42\r\n");
    assert_eq!(send_cmd(&mut client, &["EVAL", "return 'hello world'", "0"]), "$11\r\nhello world\r\n");
    assert_eq!(send_cmd(&mut client, &["EVAL", "return true", "0"]), ":1\r\n");
    assert_eq!(send_cmd(&mut client, &["EVAL", "return false", "0"]), "$-1\r\n");
    assert_eq!(send_cmd(&mut client, &["EVAL", "return {10, 'rudis', false}", "0"]), "*3\r\n:10\r\n$5\r\nrudis\r\n$-1\r\n");

    // 2. KEYS and ARGV Passing
    let keys_argv_resp = send_cmd(&mut client, &[
        "EVAL",
        "return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}",
        "2",
        "keyA",
        "keyB",
        "val1",
        "val2",
    ]);
    assert_eq!(keys_argv_resp, "*4\r\n$4\r\nkeyA\r\n$4\r\nkeyB\r\n$4\r\nval1\r\n$4\r\nval2\r\n");

    // 3. redis.call SET and GET
    let set_resp = send_cmd(&mut client, &[
        "EVAL",
        "return redis.call('SET', KEYS[1], ARGV[1])",
        "1",
        "lua_key",
        "lua_val",
    ]);
    assert_eq!(set_resp, "+OK\r\n");

    let get_resp = send_cmd(&mut client, &[
        "EVAL",
        "return redis.call('GET', KEYS[1])",
        "1",
        "lua_key",
    ]);
    assert_eq!(get_resp, "$7\r\nlua_val\r\n");

    // Multiple operations and table inspection
    let multi_resp = send_cmd(&mut client, &[
        "EVAL",
        "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], ARGV[2])",
        "2",
        "k_str",
        "k_num",
        "hello",
        "50",
    ]);
    assert_eq!(multi_resp, ":50\r\n");

    // 4. redis.pcall error handling
    let pcall_resp = send_cmd(&mut client, &[
        "EVAL",
        "local res = redis.pcall('INCRBY', KEYS[1], 'not_a_num'); if res['err'] then return res['err'] else return 'ok' end",
        "1",
        "k_num",
    ]);
    assert!(pcall_resp.starts_with("$"), "Expected bulk string error returned from pcall, got {}", pcall_resp);
    assert!(pcall_resp.contains("integer") || pcall_resp.contains("ERR"));

    // 5. redis.sha1hex helper
    let sha_calc = send_cmd(&mut client, &[
        "EVAL",
        "return redis.sha1hex('test-string')",
        "0",
    ]);
    // sha1 of 'test-string' is 4f49d69613b186e71104c7ca1b26c1e5b78c9193
    assert_eq!(sha_calc, "$40\r\n4f49d69613b186e71104c7ca1b26c1e5b78c9193\r\n");

    // 6. SCRIPT LOAD, SCRIPT EXISTS, EVALSHA, SCRIPT FLUSH
    let script_code = "return redis.call('GET', KEYS[1])";
    let load_resp = send_cmd(&mut client, &["SCRIPT", "LOAD", script_code]);
    assert!(load_resp.starts_with("$40\r\n"));
    let sha = load_resp.trim_start_matches("$40\r\n").trim_end_matches("\r\n");

    // SCRIPT EXISTS
    let exists_resp = send_cmd(&mut client, &["SCRIPT", "EXISTS", sha, "0000000000000000000000000000000000000000"]);
    assert_eq!(exists_resp, "*2\r\n:1\r\n:0\r\n");

    // EVALSHA execution
    let evalsha_resp = send_cmd(&mut client, &["EVALSHA", sha, "1", "k_str"]);
    assert_eq!(evalsha_resp, "$5\r\nhello\r\n");

    // SCRIPT FLUSH
    let flush_resp = send_cmd(&mut client, &["SCRIPT", "FLUSH"]);
    assert_eq!(flush_resp, "+OK\r\n");

    let exists_after_flush = send_cmd(&mut client, &["SCRIPT", "EXISTS", sha]);
    assert_eq!(exists_after_flush, "*1\r\n:0\r\n");

    // EVALSHA after flush should return NOSCRIPT error
    let evalsha_err = send_cmd(&mut client, &["EVALSHA", sha, "1", "k_str"]);
    assert!(evalsha_err.starts_with("-NOSCRIPT"), "Expected -NOSCRIPT error, got {}", evalsha_err);
}

#[test]
fn test_cluster_bus_gossip_failover_e2e() {
    let port1 = 16440;
    let port2 = 16441;
    let port3 = 16442;

    let _s1 = start_test_server(port1, 2);
    let _s2 = start_test_server(port2, 2);
    let _s3 = start_test_server(port3, 2);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();
    let mut c3 = TcpStream::connect(format!("127.0.0.1:{}", port3)).unwrap();

    // 1. Verify Cluster Bus listeners on port + 10000 are active
    let bus_stream1 = TcpStream::connect(format!("127.0.0.1:{}", port1 + 10000));
    assert!(bus_stream1.is_ok(), "Cluster bus on port {} should be listening", port1 + 10000);
    drop(bus_stream1);

    // 2. Initial CLUSTER MYID & INFO
    let myid1_resp = send_and_read(&mut c1, b"CLUSTER MYID\r\n");
    let myid1 = myid1_resp.trim_start_matches('$').split("\r\n").nth(1).unwrap().to_string();
    assert_eq!(myid1.len(), 40);

    let myid2_resp = send_and_read(&mut c2, b"CLUSTER MYID\r\n");
    let myid2 = myid2_resp.trim_start_matches('$').split("\r\n").nth(1).unwrap().to_string();
    assert_eq!(myid2.len(), 40);

    let myid3_resp = send_and_read(&mut c3, b"CLUSTER MYID\r\n");
    let myid3 = myid3_resp.trim_start_matches('$').split("\r\n").nth(1).unwrap().to_string();
    assert_eq!(myid3.len(), 40);

    // 3. CLUSTER MEET: Node 1 meets Node 2, Node 2 meets Node 3
    assert_eq!(send_and_read(&mut c1, format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2).as_bytes()), "+OK\r\n");
    assert_eq!(send_and_read(&mut c2, format!("CLUSTER MEET 127.0.0.1 {}\r\n", port3).as_bytes()), "+OK\r\n");

    // Wait for cluster bus gossip heartbeats to propagate transitively
    thread::sleep(Duration::from_millis(1200));

    // Verify Node 1 cluster nodes shows cport (26440, 26441)
    let nodes1 = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
    assert!(nodes1.contains("myself,master"), "Node 1 should be myself,master");
    assert!(nodes1.contains(&format!("127.0.0.1:{}@{}", port2, port2 + 10000)), "Node 1 should have met Node 2 with cport");

    // Verify transitive gossip: Node 1 should discover Node 3
    assert!(nodes1.contains(&myid3) || nodes1.contains(&format!("127.0.0.1:{}", port3)),
        "Node 1 should discover Node 3 transitively through gossip! Nodes:\n{}", nodes1);

    // 4. Test CLUSTER REPLICATE
    let rep_resp = send_and_read(&mut c2, format!("CLUSTER REPLICATE {}\r\n", myid1).as_bytes());
    assert_eq!(rep_resp, "+OK\r\n");

    let nodes2_after_rep = send_and_read(&mut c2, b"CLUSTER NODES\r\n");
    assert!(nodes2_after_rep.contains("myself,slave"), "Node 2 should be myself,slave");
    assert!(nodes2_after_rep.contains(&myid1), "Node 2 should list Node 1 as its master");

    // 5. Test CLUSTER FAILOVER
    let failover_resp = send_and_read(&mut c2, b"CLUSTER FAILOVER\r\n");
    assert_eq!(failover_resp, "+OK\r\n");

    let nodes2_after_failover = send_and_read(&mut c2, b"CLUSTER NODES\r\n");
    assert!(nodes2_after_failover.contains("myself,master"), "Node 2 should be promoted to myself,master after failover");

    let info2 = send_and_read(&mut c2, b"CLUSTER INFO\r\n");
    assert!(info2.contains("cluster_state:ok"));
    assert!(info2.contains("cluster_current_epoch:2") || info2.contains("cluster_my_epoch:2"),
        "Epoch should increment after failover. Info: {}", info2);

    // 6. Test CLUSTER FORGET
    assert_eq!(send_and_read(&mut c1, format!("CLUSTER FORGET {}\r\n", myid3).as_bytes()), "+OK\r\n");
    let nodes1_after_forget = send_and_read(&mut c1, b"CLUSTER NODES\r\n");
    assert!(!nodes1_after_forget.contains(&myid3), "Node 3 should be forgotten from Node 1");

    // 7. Test CLUSTER RESET HARD
    assert_eq!(send_and_read(&mut c3, b"CLUSTER RESET HARD\r\n"), "+OK\r\n");
    let info3_reset = send_and_read(&mut c3, b"CLUSTER INFO\r\n");
    assert!(info3_reset.contains("cluster_known_nodes:1"), "Reset node should only know itself");
    assert!(info3_reset.contains("cluster_current_epoch:1"), "Current epoch should reset to 1");
}

#[test]
fn test_nvme_tiered_storage_e2e() {
    let port = 16480;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Initially check TIER INFO and INFO storage
    let info = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info.contains("tier_enabled:1"));
    assert!(info.contains("tiered_keys:0"));

    let storage_info = send_and_read(&mut client, b"INFO storage\r\n");
    assert!(storage_info.contains("# Storage"));
    assert!(storage_info.contains("tier_enabled:1"));

    // 2. Insert keys of various sizes
    let val_256 = "A".repeat(256);
    let val_512 = "B".repeat(512);
    assert_eq!(
        send_and_read(&mut client, format!("SET key:256 {}\r\n", val_256).as_bytes()),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, format!("SET key:512 {}\r\n", val_512).as_bytes()),
        "+OK\r\n"
    );

    // 3. Spill key:256 to NVMe disk
    let spill_resp = send_and_read(&mut client, b"TIER SPILL key:256\r\n");
    assert_eq!(spill_resp, ":1\r\n");

    // Re-spilling already tiered key returns :0
    assert_eq!(send_and_read(&mut client, b"TIER SPILL key:256\r\n"), ":0\r\n");

    // 4. Verify stats after spill
    let info_after_spill = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_spill.contains("tiered_keys:1"));
    assert!(info_after_spill.contains("disk_writes:1"));

    // 5. EXISTS works on tiered key without disk retrieval
    assert_eq!(send_and_read(&mut client, b"EXISTS key:256\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"TYPE key:256\r\n"), "+string\r\n");

    // 6. Transparent async GET reads from NVMe via io_uring
    let get_resp = send_and_read(&mut client, b"GET key:256\r\n");
    assert_eq!(get_resp, format!("${}\r\n{}\r\n", val_256.len(), val_256));

    let info_after_get = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_get.contains("disk_reads:1"));

    // 7. Explicit TIER LOAD back into RAM
    let spill_resp2 = send_and_read(&mut client, b"TIER SPILL key:512\r\n");
    assert_eq!(spill_resp2, ":1\r\n");
    let load_resp = send_and_read(&mut client, b"TIER LOAD key:512\r\n");
    assert_eq!(load_resp, ":1\r\n");
    // Loading already-loaded key returns :0
    assert_eq!(send_and_read(&mut client, b"TIER LOAD key:512\r\n"), ":0\r\n");
    // Verify data intact
    let get_512 = send_and_read(&mut client, b"GET key:512\r\n");
    assert_eq!(get_512, format!("${}\r\n{}\r\n", val_512.len(), val_512));

    // 8. Test TIER SPILLALL across all shards
    for i in 0..10 {
        let val = format!("val_{}", i).repeat(20);
        assert_eq!(
            send_and_read(&mut client, format!("SET item:{} {}\r\n", i, val).as_bytes()),
            "+OK\r\n"
        );
    }
    let spillall_resp = send_and_read(&mut client, b"TIER SPILLALL\r\n");
    assert!(spillall_resp.starts_with(':'));
    let count: i64 = spillall_resp.trim_start_matches(':').trim().parse().unwrap();
    assert!(count >= 10);

    // Read back all keys from disk
    for i in 0..10 {
        let expected = format!("val_{}", i).repeat(20);
        let resp = send_and_read(&mut client, format!("GET item:{}\r\n", i).as_bytes());
        assert_eq!(resp, format!("${}\r\n{}\r\n", expected.len(), expected));
    }

    // 9. Mutate and Delete tiered keys
    assert_eq!(send_and_read(&mut client, b"TIER SPILL item:0\r\n"), ":1\r\n");
    // Overwrite tiered key
    assert_eq!(send_and_read(&mut client, b"SET item:0 new_overwritten_value\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"GET item:0\r\n"), "$21\r\nnew_overwritten_value\r\n");

    // Delete tiered key
    assert_eq!(send_and_read(&mut client, b"TIER SPILL item:1\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"DEL item:1\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS item:1\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"GET item:1\r\n"), "$-1\r\n");
}

#[test]
fn test_auto_tiering_memory_pressure_e2e() {
    let port = 16490;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Test CONFIG SET and CONFIG GET maxmemory
    let get_maxmem = send_and_read(&mut client, b"CONFIG GET maxmemory\r\n");
    assert!(get_maxmem.contains("maxmemory"));

    let set_maxmem = send_and_read(&mut client, b"CONFIG SET maxmemory 500000\r\n");
    assert_eq!(set_maxmem, "+OK\r\n");

    let get_maxmem2 = send_and_read(&mut client, b"CONFIG GET maxmemory\r\n");
    assert_eq!(get_maxmem2, "*2\r\n$9\r\nmaxmemory\r\n$6\r\n500000\r\n");

    let set_human = send_and_read(&mut client, b"CONFIG SET maxmemory 100mb\r\n");
    assert_eq!(set_human, "+OK\r\n");
    let get_human = send_and_read(&mut client, b"CONFIG GET maxmemory\r\n");
    assert_eq!(get_human, "*2\r\n$9\r\nmaxmemory\r\n$9\r\n104857600\r\n");

    // 2. Test INFO memory
    let mem_info = send_and_read(&mut client, b"INFO memory\r\n");
    assert!(mem_info.contains("# Memory"));
    assert!(mem_info.contains("used_memory:"));
    assert!(mem_info.contains("maxmemory:104857600"));

    // 3. Test Three-State Value Lifecycle: Hot -> Cooled -> Cold -> Cooled
    let val_payload = "X".repeat(300);
    assert_eq!(
        send_and_read(&mut client, format!("SET cool_key {}\r\n", val_payload).as_bytes()),
        "+OK\r\n"
    );

    // Stash to NVMe but retain in DRAM (Hot -> Cooled)
    let cool_resp = send_and_read(&mut client, b"TIER COOL cool_key\r\n");
    assert_eq!(cool_resp, ":1\r\n");

    let tier_info = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(tier_info.contains("cooled_keys:1"));
    assert!(tier_info.contains("disk_writes:1"));

    // Reading Cooled key is an instant DRAM hit with ZERO disk reads!
    let get_cooled = send_and_read(&mut client, b"GET cool_key\r\n");
    assert_eq!(get_cooled, format!("${}\r\n{}\r\n", val_payload.len(), val_payload));
    let tier_info2 = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(tier_info2.contains("disk_reads:0"));

    // Instant Zero-I/O Decommit: Cooled -> Cold (drops RAM buffer without disk I/O)
    let decommit_resp = send_and_read(&mut client, b"TIER DECOMMIT cool_key\r\n");
    assert_eq!(decommit_resp, ":1\r\n");

    let tier_info3 = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(tier_info3.contains("cooled_keys:0"));
    assert!(tier_info3.contains("tiered_keys:1"));
    assert!(tier_info3.contains("decommit_count:1"));
    assert!(tier_info3.contains("disk_writes:1")); // No new disk writes!

    // Reading Cold key fetches from disk via io_uring and promotes to Cooled!
    let get_cold = send_and_read(&mut client, b"GET cool_key\r\n");
    assert_eq!(get_cold, format!("${}\r\n{}\r\n", val_payload.len(), val_payload));
    let tier_info4 = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(tier_info4.contains("disk_reads:1"));
    assert!(tier_info4.contains("cooled_keys:1"));
    assert!(tier_info4.contains("tiered_keys:0"));

    // Subsequent read is again a fast zero-I/O DRAM hit!
    let get_again = send_and_read(&mut client, b"GET cool_key\r\n");
    assert_eq!(get_again, format!("${}\r\n{}\r\n", val_payload.len(), val_payload));
    let tier_info5 = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(tier_info5.contains("disk_reads:1")); // disk_reads did NOT increment!

    // Decommit all cooled keys via TIER DECOMMIT
    let decommit_all = send_and_read(&mut client, b"TIER DECOMMIT\r\n");
    assert_eq!(decommit_all, ":1\r\n");
    let tier_info6 = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(tier_info6.contains("cooled_keys:0"));
    assert!(tier_info6.contains("tiered_keys:1"));

    // 4. Auto-Tiering under Memory Pressure
    // Retrieve current used memory
    let mem_info2 = send_and_read(&mut client, b"INFO memory\r\n");
    let used_mem_line = mem_info2
        .lines()
        .find(|l| l.starts_with("used_memory:"))
        .unwrap();
    let cur_used: u64 = used_mem_line.strip_prefix("used_memory:").unwrap().trim().parse().unwrap();

    // Set maxmemory just slightly above current used memory (+500 bytes)
    let limit = cur_used + 500;
    assert_eq!(
        send_and_read(&mut client, format!("CONFIG SET maxmemory {}\r\n", limit).as_bytes()),
        "+OK\r\n"
    );

    // Insert multiple keys that will exceed the threshold
    for i in 0..10 {
        let val = "Z".repeat(200);
        let resp = send_and_read(&mut client, format!("SET autotier:{} {}\r\n", i, val).as_bytes());
        assert_eq!(resp, "+OK\r\n");
    }

    // Auto-tiering should have triggered spilling hot keys to NVMe
    let tier_info_auto = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(tier_info_auto.contains("tier_enabled:1"));
    // Disk writes must have increased due to auto-tiering
    assert!(!tier_info_auto.contains("disk_writes:1\r\n"));

    // Verify all keys remain accessible and return correct data
    for i in 0..10 {
        let expected = "Z".repeat(200);
        let resp = send_and_read(&mut client, format!("GET autotier:{}\r\n", i).as_bytes());
        assert_eq!(resp, format!("${}\r\n{}\r\n", expected.len(), expected));
    }

    // Reset maxmemory to 0 so subsequent tests are not affected by low memory limits
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET maxmemory 0\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_tiered_storage_tracks_e2e() {
    let port = 16415;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis test server");

    // 1. Dynamic Watermark Thresholds via CONFIG GET/SET
    let offload_get = send_and_read(&mut client, b"CONFIG GET tiered-offload-threshold\r\n");
    assert!(offload_get.contains("60"));

    let upload_get = send_and_read(&mut client, b"CONFIG GET tiered-upload-threshold\r\n");
    assert!(upload_get.contains("80"));

    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET tiered-offload-threshold 75\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET tiered-upload-threshold 65\r\n"),
        "+OK\r\n"
    );

    let offload_get2 = send_and_read(&mut client, b"CONFIG GET tiered-offload-threshold\r\n");
    assert!(offload_get2.contains("75"));

    let upload_get2 = send_and_read(&mut client, b"CONFIG GET tiered-upload-threshold\r\n");
    assert!(upload_get2.contains("65"));

    // 2. Verify TIER INFO telemetry fields
    let info = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info.contains("offload_threshold_pct:75"));
    assert!(info.contains("upload_threshold_pct:65"));
    assert!(info.contains("bin_pages:"));
    assert!(info.contains("coalesced_reads:"));
    assert!(info.contains("ram_hits:"));
    assert!(info.contains("ram_misses:"));
    assert!(info.contains("total_stashes:"));
    assert!(info.contains("total_fetches:"));
    assert!(info.contains("total_deletes:"));

    // 3. SmallBins aggregation: pack multiple small records (<2KB)
    for i in 0..15 {
        let val = format!("small_val_{:03}_{}", i, "A".repeat(150));
        let set_cmd = format!("SET sb_key:{} {}\r\n", i, val);
        assert_eq!(send_and_read(&mut client, set_cmd.as_bytes()), "+OK\r\n");

        let cool_cmd = format!("TIER COOL sb_key:{}\r\n", i);
        assert_eq!(send_and_read(&mut client, cool_cmd.as_bytes()), ":1\r\n");
    }

    let info_after_cool = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_cool.contains("cooled_keys:15"));
    assert!(info_after_cool.contains("total_stashes:15"));

    // 4. Instant Decommit: Cooled -> Cold
    let decommit_resp = send_and_read(&mut client, b"TIER DECOMMIT\r\n");
    assert_eq!(decommit_resp, ":15\r\n");

    let info_after_decommit = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_decommit.contains("cooled_keys:0"));
    assert!(info_after_decommit.contains("tiered_keys:15"));

    // 5. Read back keys - verifying fetch promotion and data integrity
    for i in 0..15 {
        let expected = format!("small_val_{:03}_{}", i, "A".repeat(150));
        let get_cmd = format!("GET sb_key:{}\r\n", i);
        let resp = send_and_read(&mut client, get_cmd.as_bytes());
        assert_eq!(resp, format!("${}\r\n{}\r\n", expected.len(), expected));
    }

    let info_after_get = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_get.contains("total_fetches:15"));

    // 6. Overwrite a key and verify total_deletes increments
    assert_eq!(
        send_and_read(&mut client, b"SET sb_key:0 new_val\r\n"),
        "+OK\r\n"
    );
    let info_after_overwrite = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_overwrite.contains("total_deletes:1"));
}

#[test]
fn test_tiered_storage_gc_and_hole_punching_e2e() {
    let port = 16500;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Spill a large key (>2KB)
    let large_val = "Z".repeat(3000);
    assert_eq!(
        send_and_read(&mut client, format!("SET large_key {}\r\n", large_val).as_bytes()),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"TIER SPILL large_key\r\n"), ":1\r\n");

    // 2. Delete the large key - should immediately punch hole in the physical NVMe storage!
    assert_eq!(send_and_read(&mut client, b"DEL large_key\r\n"), ":1\r\n");
    let info = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info.contains("total_deletes:1"));
    assert!(info.contains("gc_reclaimed_bytes:4096"));

    // 3. Test explicit TIER GC command
    let gc_resp = send_and_read(&mut client, b"TIER GC\r\n");
    assert!(gc_resp.starts_with(':'));

    let _ = std::fs::remove_dir_all(format!("/tmp/rudis_tier_{}", port));
}

#[test]
fn test_option1_zero_copy_snapshots_e2e() {
    let port = 16510;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Write keys and spill one to tier
    assert_eq!(send_and_read(&mut client, b"SET snap_key1 hello\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SET snap_key2 world\r\n"), "+OK\r\n");
    let spill_resp = send_and_read(&mut client, b"TIER SPILL snap_key1\r\n");
    assert_eq!(spill_resp, ":1\r\n");

    // 2. Perform zero-copy snapshot
    let snap_dir = format!("/tmp/rudis_snapshot_test_{}", port);
    let _ = std::fs::remove_dir_all(&snap_dir);

    let snap_cmd = format!("TIER SNAPSHOT {}\r\n", snap_dir);
    let snap_resp = send_and_read(&mut client, snap_cmd.as_bytes());
    assert!(snap_resp.starts_with("+OK"));

    // Verify snapshot directory exists and has files
    assert!(std::path::Path::new(&snap_dir).exists());
    let entries = std::fs::read_dir(&snap_dir).unwrap().count();
    assert!(entries > 0);

    let _ = std::fs::remove_dir_all(&snap_dir);
    let _ = std::fs::remove_dir_all(format!("/tmp/rudis_tier_{}", port));
}

#[test]
fn test_option2_vector_search_hnsw_e2e() {
    let port = 16520;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Add vectors to HNSW index
    assert_eq!(send_and_read(&mut client, b"VADD v_idx doc1 1.0 0.0 0.0\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"VADD v_idx doc2 0.0 1.0 0.0\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"VADD v_idx doc3 0.9 0.1 0.0\r\n"), "+OK\r\n");

    // 2. Query index info
    let info = send_and_read(&mut client, b"VINFO v_idx\r\n");
    assert!(info.contains("num_elements"));
    assert!(info.contains(":3\r\n"));
    assert!(info.contains("dimension"));
    assert!(info.contains(":3\r\n"));

    // 3. Compute vector similarity (Cosine metric by default)
    // doc1 [1,0,0] vs doc3 [0.9, 0.1, 0] is very close (distance close to 0)
    let sim_1_3 = send_and_read(&mut client, b"VSIM v_idx doc1 doc3\r\n");
    assert!(sim_1_3.starts_with("$"));

    // doc1 [1,0,0] vs doc2 [0,1,0] is orthogonal (distance 1.0)
    let sim_1_2 = send_and_read(&mut client, b"VSIM v_idx doc1 doc2\r\n");
    assert!(sim_1_2.contains("1.000000"));

    // 4. Query top-2 nearest neighbors for [1.0, 0.0, 0.0]
    let query_resp = send_and_read(&mut client, b"VQUERY v_idx 2 1.0 0.0 0.0\r\n");
    assert!(query_resp.contains("doc1"));
    assert!(query_resp.contains("doc3"));

    // 5. Delete element
    assert_eq!(send_and_read(&mut client, b"VDEL v_idx doc1\r\n"), ":1\r\n");
    let info_after = send_and_read(&mut client, b"VINFO v_idx\r\n");
    assert!(info_after.contains(":2\r\n"));
}

#[test]
fn test_option3_modern_redis7_features_e2e() {
    let port = 16530;
    start_test_server(port, 2);
    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client1.set_read_timeout(Some(Duration::from_millis(500))).unwrap();

    // 1. RESP3 Negotiation via HELLO 3
    let hello_resp = send_and_read(&mut client1, b"HELLO 3\r\n");
    assert!(hello_resp.starts_with("%"));
    assert!(hello_resp.contains("server"));
    assert!(hello_resp.contains("valkey"));
    assert!(hello_resp.contains("proto"));

    // 2. Client tracking & invalidation
    assert_eq!(send_and_read(&mut client1, b"CLIENT TRACKING on\r\n"), "+OK\r\n");

    // Client 1 reads a key to track it
    assert_eq!(send_and_read(&mut client1, b"GET track_k1\r\n"), "$-1\r\n");

    // Client 2 modifies track_k1
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut client2, b"SET track_k1 updated_val\r\n"), "+OK\r\n");

    // Client 1 should receive the invalidation message on next interaction or read
    let next_resp = send_and_read(&mut client1, b"PING\r\n");
    assert!(next_resp.contains("invalidate"));
    assert!(next_resp.contains("track_k1"));

    // 3. Redis 7 Functions: FUNCTION LOAD and FCALL
    let func_code = "#!lua name=mathlib\nredis.register_function('add_nums', function(keys, args) return tonumber(args[1]) + tonumber(args[2]) end)\n";
    let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", func_code.len(), func_code);
    let load_resp = send_and_read(&mut client2, load_cmd.as_bytes());
    assert_eq!(load_resp, "$7\r\nmathlib\r\n");

    // Call function via FCALL
    let fcall_cmd = "*5\r\n$5\r\nFCALL\r\n$8\r\nadd_nums\r\n$1\r\n0\r\n$2\r\n15\r\n$2\r\n27\r\n";
    let fcall_resp = send_and_read(&mut client2, fcall_cmd.as_bytes());
    assert_eq!(fcall_resp, ":42\r\n");

    // FUNCTION LIST
    let list_resp = send_and_read(&mut client2, b"FUNCTION LIST\r\n");
    assert!(list_resp.contains("mathlib"));
    assert!(list_resp.contains("add_nums"));

    // FUNCTION DELETE
    assert_eq!(send_and_read(&mut client2, b"FUNCTION DELETE mathlib\r\n"), "+OK\r\n");
}

#[test]
fn test_option4_jemalloc_memory_profiling_e2e() {
    let port = 16540;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    let info_resp = send_and_read(&mut client, b"INFO memory\r\n");
    assert!(info_resp.contains("# Memory"));
    assert!(info_resp.contains("used_memory:"));
    assert!(info_resp.contains("used_memory_rss:"));
    assert!(info_resp.contains("allocator_allocated:"));
    assert!(info_resp.contains("allocator_active:"));
    assert!(info_resp.contains("allocator_resident:"));
    assert!(info_resp.contains("mem_fragmentation_ratio:"));
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
    assert_eq!(send_and_read(&mut client1, b"CRDT.GET geo_key\r\n"), "$14\r\nregion_us_east\r\n");

    // 2. PN-Counters across both nodes
    assert_eq!(send_and_read(&mut client1, b"CRDT.INCRBY user_counter 42\r\n"), ":42\r\n");
    assert_eq!(send_and_read(&mut client2, b"CRDT.INCRBY user_counter 8\r\n"), ":8\r\n");

    // 3. OR-Sets across both nodes
    assert_eq!(send_and_read(&mut client1, b"CRDT.SADD active_tags tag_gaming\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client1, b"CRDT.SADD active_tags tag_social\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client2, b"CRDT.SADD active_tags tag_mobile\r\n"), ":1\r\n");

    // 4. Cross-Region Replication: Dump Node 1 state and merge into Node 2
    let dump_bytes = send_and_read_bytes(&mut client1, b"CRDT.DUMP\r\n");
    // Parse bulk string payload
    assert!(dump_bytes.starts_with(b"$"));
    let first_newline = dump_bytes.iter().position(|&b| b == b'\n').unwrap();
    let payload = &dump_bytes[first_newline + 1..dump_bytes.len() - 2];

    let mut merge_cmd = Vec::new();
    merge_cmd.extend_from_slice(format!("*2\r\n$10\r\nCRDT.MERGE\r\n${}\r\n", payload.len()).as_bytes());
    merge_cmd.extend_from_slice(payload);
    merge_cmd.extend_from_slice(b"\r\n");
    let merge_resp = send_and_read(&mut client2, &merge_cmd);
    assert!(merge_resp.starts_with(":"));

    // Verify converged state on Node 2
    assert_eq!(send_and_read(&mut client2, b"CRDT.GET geo_key\r\n"), "$14\r\nregion_us_east\r\n");
    assert_eq!(send_and_read(&mut client2, b"CRDT.INCRBY user_counter 0\r\n"), ":50\r\n"); // 42 + 8 = 50
    let members = send_and_read(&mut client2, b"CRDT.SMEMBERS active_tags\r\n");
    assert!(members.contains("tag_gaming"));
    assert!(members.contains("tag_social"));
    assert!(members.contains("tag_mobile"));

    // 5. Automated Tombstone TTL Garbage Collection
    assert_eq!(send_and_read(&mut client1, b"CRDT.DEL geo_key\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client1, b"CRDT.GET geo_key\r\n"), "$-1\r\n");
    // Run CRDT.GC with 0ms cutoff to instantly reclaim tombstones
    let gc_resp = send_and_read(&mut client1, b"CRDT.GC 0\r\n");
    assert!(gc_resp.contains("registers_pruned"));
    assert!(gc_resp.contains("set_tombstones_pruned"));
}

#[test]
fn test_sq8_quantized_vector_and_rerank_e2e() {
    let port = 16560;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Ingest vectors with SQ8 quantization and tiered storage flag
    assert_eq!(
        send_and_read(&mut client, b"VADD doc_sq8 docA 1.0 0.0 0.0 QUANTIZE TIERED\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VADD doc_sq8 docB 0.0 1.0 0.0 QUANTIZE TIERED\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VADD doc_sq8 docC 0.88 0.12 0.0 QUANTIZE TIERED\r\n"),
        "+OK\r\n"
    );

    // 2. Query with full precision reranking
    let query_resp = send_and_read(&mut client, b"VQUERY doc_sq8 2 1.0 0.0 0.0 RERANK\r\n");
    assert!(query_resp.contains("docA"));
    assert!(query_resp.contains("docC"));

    // 3. Verify VINFO reflects elements
    let info = send_and_read(&mut client, b"VINFO doc_sq8\r\n");
    assert!(info.contains(":3\r\n"));
}

#[test]
fn test_tls_in_memory_cert_and_ktls_e2e() {
    // 1. Test in-memory self-signed certificate generation
    let (cert_der, key_der) = rudis::tls::generate_self_signed_cert(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ]).expect("Failed to generate test self-signed cert");
    assert!(!cert_der.is_empty());
    assert!(!key_der.is_empty());

    // 2. Test rustls ServerConfig creation
    let config = rudis::tls::create_server_config(&cert_der, &key_der)
        .expect("Failed to build rustls ServerConfig");

    // 3. Test TlsSession wrapper instantiation
    let session = rudis::tls::TlsSession::new(config);
    assert!(session.is_ok());
}

#[test]
fn test_redis_json_engine_e2e() {
    let port = 16570;
    let _server = start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. JSON.SET root
    let doc = r#"{"name":"Bob","age":28,"tags":["rust","io_uring"],"online":true}"#;
    let set_cmd = format!("*4\r\n$8\r\nJSON.SET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n${}\r\n{}\r\n", doc.len(), doc);
    assert_eq!(send_and_read(&mut stream, set_cmd.as_bytes()), "+OK\r\n");

    // 2. JSON.GET root
    let resp = send_and_read(&mut stream, b"*3\r\n$8\r\nJSON.GET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n");
    assert!(resp.contains("Bob"));
    assert!(resp.contains("io_uring"));

    // 3. JSON.GET path $.name
    let resp = send_and_read(&mut stream, b"*3\r\n$8\r\nJSON.GET\r\n$6\r\nuser:1\r\n$6\r\n$.name\r\n");
    assert!(resp.contains("\"Bob\""));

    // 4. JSON.TYPE
    let resp = send_and_read(&mut stream, b"*3\r\n$9\r\nJSON.TYPE\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n");
    assert_eq!(resp, "+array\r\n");

    // 5. JSON.NUMINCRBY
    let resp = send_and_read(&mut stream, b"*4\r\n$14\r\nJSON.NUMINCRBY\r\n$6\r\nuser:1\r\n$5\r\n$.age\r\n$1\r\n2\r\n");
    assert!(resp.contains("30"));

    // 6. JSON.ARRAPPEND
    let resp = send_and_read(&mut stream, b"*4\r\n$14\r\nJSON.ARRAPPEND\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n$11\r\n\"high_perf\"\r\n");
    assert_eq!(resp, ":3\r\n");

    // 7. JSON.ARRLEN
    let resp = send_and_read(&mut stream, b"*3\r\n$11\r\nJSON.ARRLEN\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n");
    assert_eq!(resp, ":3\r\n");

    // 8. JSON.ARRPOP
    let resp = send_and_read(&mut stream, b"*3\r\n$11\r\nJSON.ARRPOP\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n");
    assert!(resp.contains("\"high_perf\""));

    // 9. JSON.TOGGLE
    let resp = send_and_read(&mut stream, b"*3\r\n$11\r\nJSON.TOGGLE\r\n$6\r\nuser:1\r\n$8\r\n$.online\r\n");
    assert!(resp.contains("false"));

    // 10. JSON.OBJKEYS
    let resp = send_and_read(&mut stream, b"*3\r\n$12\r\nJSON.OBJKEYS\r\n$6\r\nuser:1\r\n$1\r\n$\r\n");
    assert!(resp.contains("name"));
    assert!(resp.contains("age"));

    // 11. JSON.OBJLEN
    let resp = send_and_read(&mut stream, b"*3\r\n$11\r\nJSON.OBJLEN\r\n$6\r\nuser:1\r\n$1\r\n$\r\n");
    assert_eq!(resp, ":4\r\n");

    // 12. JSON.DEL nested
    let resp = send_and_read(&mut stream, b"*3\r\n$8\r\nJSON.DEL\r\n$6\r\nuser:1\r\n$8\r\n$.online\r\n");
    assert_eq!(resp, ":1\r\n");

    // 13. JSON.DEL root
    let resp = send_and_read(&mut stream, b"*2\r\n$8\r\nJSON.DEL\r\n$6\r\nuser:1\r\n");
    assert_eq!(resp, ":1\r\n");

    // 14. Verify deleted
    let resp = send_and_read(&mut stream, b"*2\r\n$8\r\nJSON.GET\r\n$6\r\nuser:1\r\n");
    assert_eq!(resp, "$-1\r\n");
}

#[test]
fn test_zero_copy_network_engine_e2e() {
    use rudis::zerocopy::{PAGE_SIZE, RegisteredBufferPool, ZeroCopyEngine, ZeroCopyStats};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let stats = Arc::new(ZeroCopyStats::new());
    let mut pool = RegisteredBufferPool::new(8, PAGE_SIZE, stats.clone())
        .expect("Failed to create registered buffer pool");

    assert_eq!(pool.available_slots(), 8);
    assert_eq!(pool.slot_size(), PAGE_SIZE);

    let s = pool.acquire_slot().expect("acquire slot");
    {
        let buf = pool.get_slot_mut(s).unwrap();
        buf[0] = 0x55;
        buf[1] = 0xAA;
    }
    assert_eq!(pool.get_slot(s).unwrap()[0], 0x55);
    pool.release_slot(s);
    assert_eq!(pool.available_slots(), 8);

    // Test zero-copy socket loopback transfer
    let (s1, s2) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let fd1 = std::os::unix::io::AsRawFd::as_raw_fd(&s1);
    let fd2 = std::os::unix::io::AsRawFd::as_raw_fd(&s2);

    let engine = ZeroCopyEngine::new(stats.clone());
    let _ = ZeroCopyEngine::enable_so_zerocopy(fd1);

    let test_msg = b"+PONG_ZEROCOPY\r\n";
    let sent = engine.send_zc(fd1, test_msg).expect("zero-copy send");
    assert_eq!(sent, test_msg.len());

    let mut recv_buf = [0u8; 32];
    let n = unsafe {
        libc::recv(
            fd2,
            recv_buf.as_mut_ptr() as *mut libc::c_void,
            recv_buf.len(),
            0,
        )
    };
    assert_eq!(n as usize, test_msg.len());
    assert_eq!(&recv_buf[..n as usize], test_msg);
    assert_eq!(stats.zc_send_calls.load(Ordering::Relaxed), 1);
}

#[test]
fn test_geospatial_engine_e2e() {
    let port = 16580;
    let _server = start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. GEOADD
    assert_eq!(send_and_read(&mut stream, b"GEOADD sicily 13.361389 38.115556 Palermo\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut stream, b"GEOADD sicily 15.087269 37.502669 Catania\r\n"), ":1\r\n");

    // 2. GEODIST
    let dist_resp = send_and_read(&mut stream, b"GEODIST sicily Palermo Catania km\r\n");
    assert!(dist_resp.starts_with("$"));
    assert!(dist_resp.contains("166.2"));

    let dist_m = send_and_read(&mut stream, b"GEODIST sicily Palermo Catania m\r\n");
    assert!(dist_m.contains("16627"));

    // 3. GEOPOS
    let pos_resp = send_and_read(&mut stream, b"GEOPOS sicily Palermo NonExistent\r\n");
    assert!(pos_resp.starts_with("*2\r\n"));
    assert!(pos_resp.contains("13.36"));
    assert!(pos_resp.contains("38.11"));
    assert!(pos_resp.contains("*-1\r\n"));

    // 4. GEOHASH
    let hash_resp = send_and_read(&mut stream, b"GEOHASH sicily Palermo Catania\r\n");
    assert!(hash_resp.starts_with("*2\r\n"));
    assert!(hash_resp.contains("$11\r\ntc1q585vb58"));
    assert!(hash_resp.contains("$11\r\ntc26yj70z7h"));

    // 5. GEORADIUS with WITHDIST and WITHCOORD
    let rad_resp = send_and_read(&mut stream, b"GEORADIUS sicily 15 37 200 km WITHDIST WITHCOORD\r\n");
    assert!(rad_resp.starts_with("*2\r\n"));
    assert!(rad_resp.contains("Palermo"));
    assert!(rad_resp.contains("Catania"));

    // 6. GEORADIUSBYMEMBER
    let rad_member = send_and_read(&mut stream, b"GEORADIUSBYMEMBER sicily Palermo 100 km WITHDIST\r\n");
    assert!(rad_member.contains("Palermo"));
    assert!(!rad_member.contains("Catania")); // Catania is > 166km away

    // 7. GEOSEARCH FROMLONLAT BYRADIUS
    let search_resp = send_and_read(&mut stream, b"GEOSEARCH sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC WITHDIST\r\n");
    let pos_catania = search_resp.find("Catania").unwrap();
    let pos_palermo = search_resp.find("Palermo").unwrap();
    assert!(pos_catania < pos_palermo); // ASC order: Catania closer to (15, 37) than Palermo

    // 8. GEOSEARCH FROMMEMBER BYBOX
    let box_resp = send_and_read(&mut stream, b"GEOSEARCH sicily FROMMEMBER Palermo BYBOX 400 400 km\r\n");
    assert!(box_resp.contains("Palermo"));
    assert!(box_resp.contains("Catania"));
}

#[test]
fn test_probabilistic_data_structures_e2e() {
    let port = 16590;
    let _server = start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Bloom Filter (BF.*)
    assert_eq!(send_and_read(&mut stream, b"BF.RESERVE mybf 0.01 1000\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut stream, b"BF.RESERVE mybf 0.01 1000\r\n"), "-ERR item exists\r\n");
    assert_eq!(send_and_read(&mut stream, b"BF.ADD mybf apple\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut stream, b"BF.ADD mybf apple\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut stream, b"BF.EXISTS mybf apple\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut stream, b"BF.EXISTS mybf orange\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut stream, b"BF.MADD mybf banana grape\r\n"), "*2\r\n:1\r\n:1\r\n");
    assert_eq!(send_and_read(&mut stream, b"BF.MEXISTS mybf apple banana melon\r\n"), "*3\r\n:1\r\n:1\r\n:0\r\n");
    let info = send_and_read(&mut stream, b"BF.INFO mybf\r\n");
    assert!(info.contains("Capacity"));
    assert!(info.contains("1000"));

    // 2. Cuckoo Filter (CF.*)
    assert_eq!(send_and_read(&mut stream, b"CF.RESERVE mycf 1000\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut stream, b"CF.RESERVE mycf 1000\r\n"), "-ERR item exists\r\n");
    assert_eq!(send_and_read(&mut stream, b"CF.ADD mycf foo\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut stream, b"CF.ADDNX mycf foo\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut stream, b"CF.ADDNX mycf bar\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut stream, b"CF.EXISTS mycf foo\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut stream, b"CF.DEL mycf foo\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut stream, b"CF.EXISTS mycf foo\r\n"), ":0\r\n");
    let cf_info = send_and_read(&mut stream, b"CF.INFO mycf\r\n");
    assert!(cf_info.contains("Number of buckets"));

    // 3. Count-Min Sketch (CMS.*)
    assert_eq!(send_and_read(&mut stream, b"CMS.INITBYDIM mycms 200 5\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut stream, b"CMS.INCRBY mycms item1 42 item2 17\r\n"), "*2\r\n:42\r\n:17\r\n");
    assert_eq!(send_and_read(&mut stream, b"CMS.QUERY mycms item1 item2 item3\r\n"), "*3\r\n:42\r\n:17\r\n:0\r\n");
    let cms_info = send_and_read(&mut stream, b"CMS.INFO mycms\r\n");
    assert!(cms_info.contains("width"));
    assert!(cms_info.contains("depth"));

    // 4. Top-K (TOPK.*)
    assert_eq!(send_and_read(&mut stream, b"TOPK.RESERVE mytopk 3\r\n"), "+OK\r\n");
    let add_res = send_and_read(&mut stream, b"TOPK.ADD mytopk alpha alpha alpha beta beta gamma\r\n");
    assert!(add_res.starts_with("*6\r\n"));
    assert_eq!(send_and_read(&mut stream, b"TOPK.QUERY mytopk alpha beta gamma delta\r\n"), "*4\r\n:1\r\n:1\r\n:1\r\n:0\r\n");
    let topk_list = send_and_read(&mut stream, b"TOPK.LIST mytopk\r\n");
    assert!(topk_list.contains("alpha"));
    assert!(topk_list.contains("beta"));
    assert!(topk_list.contains("gamma"));
    let topk_info = send_and_read(&mut stream, b"TOPK.INFO mytopk\r\n");
    assert!(topk_info.contains("k"));
}

#[test]
fn test_product_quantization_adc_e2e() {
    let port = 16600;
    let _server = start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Add vectors with PQ option
    assert_eq!(
        send_and_read(&mut stream, b"VADD pq_idx doc1 1.0 0.0 0.0 0.0 PQ\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"VADD pq_idx doc2 0.0 1.0 0.0 0.0 PQ\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"VADD pq_idx doc3 0.9 0.1 0.0 0.0 PQ\r\n"),
        "+OK\r\n"
    );

    // 2. Query top-2 nearest neighbors using Asymmetric Distance Computation (ADC)
    let resp = send_and_read(&mut stream, b"VQUERY pq_idx 2 1.0 0.0 0.0 0.0\r\n");
    assert!(resp.starts_with("*4\r\n"));
    assert!(resp.contains("doc1"));
    assert!(resp.contains("doc3"));

    // 3. Query with exact Float32 RERANK
    let resp_rerank = send_and_read(&mut stream, b"VQUERY pq_idx 2 1.0 0.0 0.0 0.0 RERANK\r\n");
    assert!(resp_rerank.starts_with("*4\r\n"));
    assert!(resp_rerank.contains("doc1"));
    assert!(resp_rerank.contains("doc3"));

    // First result must be doc1
    let pos_doc1 = resp_rerank.find("doc1").unwrap();
    let pos_doc3 = resp_rerank.find("doc3").unwrap();
    assert!(pos_doc1 < pos_doc3);
}

#[test]
fn test_cluster_bus_shards_and_automated_failover_e2e() {
    let port1 = 16610;
    let port2 = 16611;
    let port3 = 16612;

    start_test_server(port1, 2);
    start_test_server(port2, 2);
    start_test_server(port3, 2);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();
    let mut c3 = TcpStream::connect(format!("127.0.0.1:{}", port3)).unwrap();

    // 1. Cluster Slot Mutations: ADDSLOTS, DELSLOTS, ADDSLOTSRANGE, DELSLOTSRANGE
    // Node 1 clears all slots then adds 0..=8191
    assert_eq!(send_and_read(&mut c1, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut c1, b"CLUSTER ADDSLOTSRANGE 0 8191\r\n"), "+OK\r\n");

    // Test DELSLOTS and ADDSLOTS on Node 1
    assert_eq!(send_and_read(&mut c1, b"CLUSTER DELSLOTS 100\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut c1, b"CLUSTER ADDSLOTS 100\r\n"), "+OK\r\n");

    // Node 2 clears all slots then adds 8192..=16383
    assert_eq!(send_and_read(&mut c2, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut c2, b"CLUSTER ADDSLOTSRANGE 8192 16383\r\n"), "+OK\r\n");

    // Node 3 clears slots (will act as replica)
    assert_eq!(send_and_read(&mut c3, b"CLUSTER DELSLOTSRANGE 0 16383\r\n"), "+OK\r\n");

    // 2. Fetch Node IDs
    let myid1_resp = send_and_read(&mut c1, b"CLUSTER MYID\r\n");
    let _myid1 = myid1_resp.trim_start_matches('$').split("\r\n").nth(1).unwrap().to_string();

    let myid2_resp = send_and_read(&mut c2, b"CLUSTER MYID\r\n");
    let myid2 = myid2_resp.trim_start_matches('$').split("\r\n").nth(1).unwrap().to_string();

    let myid3_resp = send_and_read(&mut c3, b"CLUSTER MYID\r\n");
    let myid3 = myid3_resp.trim_start_matches('$').split("\r\n").nth(1).unwrap().to_string();

    // 3. CLUSTER MEET: Node 1 meets Node 2, Node 2 meets Node 3
    assert_eq!(send_and_read(&mut c1, format!("CLUSTER MEET 127.0.0.1 {}\r\n", port2).as_bytes()), "+OK\r\n");
    assert_eq!(send_and_read(&mut c2, format!("CLUSTER MEET 127.0.0.1 {}\r\n", port3).as_bytes()), "+OK\r\n");

    // Wait for gossip tick & slot exchange over cluster bus
    thread::sleep(Duration::from_millis(1500));

    // 4. Test CLUSTER SLOTS introspection
    let slots_resp1 = send_and_read(&mut c1, b"CLUSTER SLOTS\r\n");
    assert!(slots_resp1.contains(":0\r\n:8191\r\n"), "Node 1 should report range 0-8191. Resp: {}", slots_resp1);
    assert!(slots_resp1.contains(":8192\r\n:16383\r\n"), "Node 1 should report peer range 8192-16383. Resp: {}", slots_resp1);

    // 5. Test CLUSTER SHARDS (Redis 7 specification)
    let shards_resp = send_and_read(&mut c1, b"CLUSTER SHARDS\r\n");
    assert!(shards_resp.contains("slots"), "Shards output should contain 'slots'. Resp: {}", shards_resp);
    assert!(shards_resp.contains("nodes"), "Shards output should contain 'nodes'. Resp: {}", shards_resp);
    assert!(shards_resp.contains("endpoint"), "Shards output should contain 'endpoint'. Resp: {}", shards_resp);
    assert!(shards_resp.contains("health"), "Shards output should contain 'health'. Resp: {}", shards_resp);
    assert!(shards_resp.contains("online"), "Shards output should contain 'online'. Resp: {}", shards_resp);

    // 6. Test CLUSTER LINKS telemetry
    let links_resp = send_and_read(&mut c1, b"CLUSTER LINKS\r\n");
    assert!(links_resp.contains("direction"), "Links output should contain 'direction'. Resp: {}", links_resp);
    assert!(links_resp.contains("to") || links_resp.contains("from"), "Links output should contain 'to' or 'from'. Resp: {}", links_resp);
    assert!(links_resp.contains("events"), "Links output should contain 'events'. Resp: {}", links_resp);

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
        send_and_read(&mut c3, format!("CLUSTER REPLICATE {}\r\n", myid2).as_bytes()),
        "+OK\r\n"
    );

    // Verify FAILOVER_AUTH_REQUEST vote handling directly on cluster bus
    let mut bus_client = TcpStream::connect(format!("127.0.0.1:{}", port1 + 10000)).unwrap();
    // First, request vote without master being failed -> should reject
    bus_client.write_all(format!("FAILOVER_AUTH_REQUEST {} 10 {}\r\n", myid3, myid2).as_bytes()).unwrap();
    let mut vbuf = [0u8; 128];
    let n = bus_client.read(&mut vbuf).unwrap();
    let vote_resp = String::from_utf8_lossy(&vbuf[..n]);
    assert!(vote_resp.contains("ERR vote rejected"), "Should reject vote if master is not failed");

    // Now mark Node 2 as fail on Node 1 via CLUSTER bus FAIL message
    bus_client.write_all(format!("FAIL {}\r\n", myid2).as_bytes()).unwrap();
    let n = bus_client.read(&mut vbuf).unwrap();
    assert_eq!(&vbuf[..n], b"+OK\r\n");

    // Now request vote again with epoch 11 -> should grant ACK!
    bus_client.write_all(format!("FAILOVER_AUTH_REQUEST {} 11 {}\r\n", myid3, myid2).as_bytes()).unwrap();
    let n = bus_client.read(&mut vbuf).unwrap();
    let vote_ack = String::from_utf8_lossy(&vbuf[..n]);
    assert!(vote_ack.starts_with("+FAILOVER_AUTH_ACK"), "Master Node 1 should grant vote ACK to Node 3. Resp: {}", vote_ack);

    // Duplicate vote in same epoch should be rejected
    bus_client.write_all(format!("FAILOVER_AUTH_REQUEST {} 11 {}\r\n", myid3, myid2).as_bytes()).unwrap();
    let n = bus_client.read(&mut vbuf).unwrap();
    let dup_vote = String::from_utf8_lossy(&vbuf[..n]);
    assert!(dup_vote.contains("ERR vote rejected"), "Duplicate vote in same epoch should be rejected");

    // Trigger failover on Node 3
    assert_eq!(send_and_read(&mut c3, b"CLUSTER FAILOVER\r\n"), "+OK\r\n");
    thread::sleep(Duration::from_millis(500));

    // Verify Node 3 is now master with config epoch updated
    let nodes3 = send_and_read(&mut c3, b"CLUSTER NODES\r\n");
    assert!(nodes3.contains("myself,master"), "Node 3 should be promoted to myself,master. Nodes:\n{}", nodes3);
}

#[test]
fn test_redisearch_fulltext_and_hybrid_vector_e2e() {
    let port = 16620;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Create index on HASH
    let create_resp = send_and_read(
        &mut client,
        b"FT.CREATE idx:books ON HASH PREFIX 1 book: SCHEMA title TEXT WEIGHT 2.0 author TEXT price NUMERIC SORTABLE category TAG SEPARATOR ,\r\n"
    );
    assert_eq!(create_resp, "+OK\r\n");

    // 2. Duplicate index creation should fail
    let dup_resp = send_and_read(
        &mut client,
        b"FT.CREATE idx:books ON HASH PREFIX 1 book: SCHEMA title TEXT\r\n"
    );
    assert!(dup_resp.contains("ERR Index already exists"));

    // 3. Inspect index metadata via FT.INFO
    let info_resp = send_and_read(&mut client, b"FT.INFO idx:books\r\n");
    assert!(info_resp.contains("idx:books"));
    assert!(info_resp.contains("num_docs"));

    // 4. Populate books via HSET (automatically indexed via hook)
    let hset1 = b"*10\r\n$4\r\nHSET\r\n$6\r\nbook:1\r\n$5\r\ntitle\r\n$14\r\nRust in Action\r\n$6\r\nauthor\r\n$12\r\nTim McNamara\r\n$5\r\nprice\r\n$4\r\n45.0\r\n$8\r\ncategory\r\n$16\r\ntech,programming\r\n";
    assert_eq!(send_and_read(&mut client, hset1), ":4\r\n");

    let hset2 = b"*10\r\n$4\r\nHSET\r\n$6\r\nbook:2\r\n$5\r\ntitle\r\n$29\r\nThe Rust Programming Language\r\n$6\r\nauthor\r\n$13\r\nSteve Klabnik\r\n$5\r\nprice\r\n$5\r\n39.99\r\n$8\r\ncategory\r\n$9\r\ntech,rust\r\n";
    assert_eq!(send_and_read(&mut client, hset2), ":4\r\n");

    let hset3 = b"*10\r\n$4\r\nHSET\r\n$6\r\nbook:3\r\n$5\r\ntitle\r\n$37\r\nDesigning Data-Intensive Applications\r\n$6\r\nauthor\r\n$16\r\nMartin Kleppmann\r\n$5\r\nprice\r\n$4\r\n55.0\r\n$8\r\ncategory\r\n$13\r\ntech,database\r\n";
    assert_eq!(send_and_read(&mut client, hset3), ":4\r\n");

    // 5. Query 1: Keyword search for "Rust" (matches book:1 and book:2)
    let search1 = send_and_read(&mut client, b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$4\r\nRust\r\n");
    assert!(search1.starts_with("*5\r\n:2\r\n"), "Search 'Rust' should return 2 hits. Resp: {}", search1);
    assert!(search1.contains("book:1"));
    assert!(search1.contains("book:2"));

    // 6. Query 2: Keyword search for "Data-Intensive" (matches book:3)
    let search2 = send_and_read(&mut client, b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$14\r\nData-Intensive\r\n");
    assert!(search2.starts_with("*3\r\n:1\r\n"), "Search 'Data-Intensive' should return 1 hit. Resp: {}", search2);
    assert!(search2.contains("book:3"));

    // 7. Query 3: Numeric range query: @price:[40 50] (matches book:1 with price 45.0)
    let search3 = send_and_read(&mut client, b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$14\r\n@price:[40 50]\r\n");
    assert!(search3.starts_with("*3\r\n:1\r\n"), "Range query should return 1 hit. Resp: {}", search3);
    assert!(search3.contains("book:1"));

    // 8. Query 4: Tag filter: @category:{database} (matches book:3)
    let search4 = send_and_read(&mut client, b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$20\r\n@category:{database}\r\n");
    assert!(search4.starts_with("*3\r\n:1\r\n"), "Tag query should return 1 hit. Resp: {}", search4);
    assert!(search4.contains("book:3"));

    // 9. Query 5: Prefix query: "progra*" (matches programming in book:2 title)
    let search5 = send_and_read(&mut client, b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$7\r\nprogra*\r\n");
    assert!(search5.starts_with("*3\r\n:1\r\n"), "Prefix query should return 1 hit for book:2. Resp: {}", search5);
    assert!(search5.contains("book:2"));

    // 10. Query 6: NOCONTENT flag (returns doc IDs only)
    let search_nocontent = send_and_read(&mut client, b"*4\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$4\r\nRust\r\n$9\r\nNOCONTENT\r\n");
    assert_eq!(search_nocontent, "*3\r\n:2\r\n$6\r\nbook:1\r\n$6\r\nbook:2\r\n");

    // 11. Query 7: FT.EXPLAIN
    let explain_resp = send_and_read(&mut client, b"*3\r\n$10\r\nFT.EXPLAIN\r\n$9\r\nidx:books\r\n$15\r\nRust | database\r\n");
    assert!(explain_resp.contains("Or"), "Explain output should describe parsed AST");

    // 12. Document deletion via DEL automatically removes from index
    assert_eq!(send_and_read(&mut client, b"DEL book:1\r\n"), ":1\r\n");
    let search_after_del = send_and_read(&mut client, b"FT.SEARCH idx:books Action\r\n");
    assert_eq!(search_after_del, "*1\r\n:0\r\n");

    // 13. Drop index
    assert_eq!(send_and_read(&mut client, b"FT.DROPINDEX idx:books\r\n"), "+OK\r\n");
    let search_after_drop = send_and_read(&mut client, b"FT.SEARCH idx:books Rust\r\n");
    assert!(search_after_drop.contains("ERR Unknown Index name"));

    // 14. JSON Document Indexing via FT.CREATE and JSON.SET
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE idx:users ON JSON PREFIX 1 user: SCHEMA name TEXT role TAG\r\n"
        ),
        "+OK\r\n"
    );
    let json_payload = b"{\"name\":\"Alice Engineer\",\"role\":\"admin\"}";
    let mut json_set_cmd = format!("*4\r\n$8\r\nJSON.SET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n${}\r\n", json_payload.len()).into_bytes();
    json_set_cmd.extend_from_slice(json_payload);
    json_set_cmd.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &json_set_cmd), "+OK\r\n");

    let search_json = send_and_read(&mut client, b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:users\r\n$5\r\nAlice\r\n");
    assert!(search_json.contains("user:1"));
    assert!(search_json.contains("Alice Engineer"));
}

#[test]
fn test_af_xdp_ebpf_kernel_bypass_e2e() {
    let port = 16621;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Inspect XDP engine info
    let info = send_and_read(&mut client, b"XDP.INFO\r\n");
    assert!(info.contains("interface:eth0"));
    assert!(info.contains("mode:"));
    assert!(info.contains("umem_frame_size:2048"));

    // 2. Add eBPF packet filter rules
    let r1 = send_and_read(&mut client, b"XDP.RULE ADD DROP 10.0.0.0/8\r\n");
    assert_eq!(r1, ":1\r\n");

    let r2 = send_and_read(&mut client, b"XDP.RULE ADD PASS 192.168.1.0/24\r\n");
    assert_eq!(r2, ":2\r\n");

    // 3. List active eBPF rules
    let rules = send_and_read(&mut client, b"XDP.RULE LIST\r\n");
    assert!(rules.starts_with("*2\r\n"));
    assert!(rules.contains("action:DROP cidr:10.0.0.0/8"));
    assert!(rules.contains("action:PASS cidr:192.168.1.0/24"));

    // 4. Inject test packets via XDP.PACKET diagnostic command
    // Packet from 10.1.2.3 (should be DROPPED by rule 1)
    let mut pkt_drop = vec![0u8; 40];
    pkt_drop[0] = 0x45;
    pkt_drop[12] = 10;
    pkt_drop[13] = 1;
    pkt_drop[14] = 2;
    pkt_drop[15] = 3;
    let mut cmd_drop = format!("*2\r\n$10\r\nXDP.PACKET\r\n${}\r\n", pkt_drop.len()).into_bytes();
    cmd_drop.extend_from_slice(&pkt_drop);
    cmd_drop.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd_drop), "+DROP\r\n");

    // Packet from 192.168.1.50 (should be PASSED by rule 2)
    let mut pkt_pass = vec![0u8; 40];
    pkt_pass[0] = 0x45;
    pkt_pass[12] = 192;
    pkt_pass[13] = 168;
    pkt_pass[14] = 1;
    pkt_pass[15] = 50;
    let mut cmd_pass = format!("*2\r\n$10\r\nXDP.PACKET\r\n${}\r\n", pkt_pass.len()).into_bytes();
    cmd_pass.extend_from_slice(&pkt_pass);
    cmd_pass.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd_pass), "+PASS\r\n");

    // Packet from 172.16.0.1 (no match -> redirected to userspace UMEM ring)
    let mut pkt_redir = vec![0u8; 40];
    pkt_redir[0] = 0x45;
    pkt_redir[12] = 172;
    pkt_redir[13] = 16;
    pkt_redir[14] = 0;
    pkt_redir[15] = 1;
    let mut cmd_redir = format!("*2\r\n$10\r\nXDP.PACKET\r\n${}\r\n", pkt_redir.len()).into_bytes();
    cmd_redir.extend_from_slice(&pkt_redir);
    cmd_redir.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd_redir), "+REDIRECT\r\n");

    // 5. Check real-time XDP stats
    let stats = send_and_read(&mut client, b"XDP.STATS\r\n");
    assert!(stats.contains("dropped_packets"));
    assert!(stats.contains("pass_packets"));
    assert!(stats.contains("redirected_packets"));

    // 6. Delete rule 1
    assert_eq!(send_and_read(&mut client, b"XDP.RULE DEL 1\r\n"), "+OK\r\n");

    // Packet from 10.1.2.3 now redirects instead of dropping
    assert_eq!(send_and_read(&mut client, &cmd_drop), "+REDIRECT\r\n");
}

#[test]
fn test_dragonfly_compatibility_suite_e2e() {
    let port = 16630;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Test DFLYCLUSTER MYID
    let myid = send_and_read(&mut client, b"DFLYCLUSTER MYID\r\n");
    assert!(myid.starts_with("$40\r\n"));

    // 2. Test DFLYCLUSTER CONFIG with JSON
    let cfg_json = r#"{"slot_ranges": [[0, 8191], [8192, 16383]]}"#;
    let cfg_cmd = format!("*3\r\n$11\r\nDFLYCLUSTER\r\n$6\r\nCONFIG\r\n${}\r\n{}\r\n", cfg_json.len(), cfg_json);
    assert_eq!(send_and_read(&mut client, cfg_cmd.as_bytes()), "+OK\r\n");

    // 3. Test DFLYCLUSTER GETSLOTINFO
    let slot_info = send_and_read(&mut client, b"DFLYCLUSTER GETSLOTINFO SLOTS 100 200\r\n");
    assert!(slot_info.starts_with("*2\r\n"));
    assert!(slot_info.contains(":100\r\n"));
    assert!(slot_info.contains(":200\r\n"));

    // 4. Test STICK, UNSTICK, STICKY
    assert_eq!(send_and_read(&mut client, b"SET stick_key1 val1\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"STICKY stick_key1\r\n"), ":0\r\n");
    // STICK key1 key_missing -> should mark key1, return 1
    assert_eq!(send_and_read(&mut client, b"STICK stick_key1 key_missing\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"STICKY stick_key1\r\n"), ":1\r\n");
    // UNSTICK key1
    assert_eq!(send_and_read(&mut client, b"UNSTICK stick_key1\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"STICKY stick_key1\r\n"), ":0\r\n");

    // 5. Test DELEX (conditional deletion)
    assert_eq!(send_and_read(&mut client, b"SET cond_key 100\r\n"), "+OK\r\n");
    // IFEQ mismatch -> returns 0
    assert_eq!(send_and_read(&mut client, b"DELEX cond_key IFEQ 999\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS cond_key\r\n"), ":1\r\n");
    // IFEQ match -> returns 1 and deletes
    assert_eq!(send_and_read(&mut client, b"DELEX cond_key IFEQ 100\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS cond_key\r\n"), ":0\r\n");

    // 6. Test DFLYCLUSTER FLUSHSLOTS
    let slot = rudis::router::key_slot(b"slot_test_key");
    assert_eq!(send_and_read(&mut client, b"SET slot_test_key hello\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS slot_test_key\r\n"), ":1\r\n");
    let flush_cmd = format!("DFLYCLUSTER FLUSHSLOTS {} {}\r\n", slot, slot);
    assert_eq!(send_and_read(&mut client, flush_cmd.as_bytes()), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS slot_test_key\r\n"), ":0\r\n");

    // 7. Test DFLYCLUSTER SLOT-MIGRATION-STATUS & DFLYMIGRATE
    let mig_status = send_and_read(&mut client, b"DFLYCLUSTER SLOT-MIGRATION-STATUS\r\n");
    assert!(mig_status.contains("IDLE"));
    // Init migration
    assert_eq!(send_and_read(&mut client, b"DFLYMIGRATE INIT node123 2 0 100\r\n"), "+OK\r\n");
    let mig_status2 = send_and_read(&mut client, b"DFLYCLUSTER SLOT-MIGRATION-STATUS\r\n");
    assert!(mig_status2.contains("MIGRATING"));
    assert!(mig_status2.contains("node123"));
    // Ack migration
    assert_eq!(send_and_read(&mut client, b"DFLYMIGRATE ACK 42\r\n"), "+OK\r\n");
    let mig_status3 = send_and_read(&mut client, b"DFLYCLUSTER SLOT-MIGRATION-STATUS\r\n");
    assert!(mig_status3.contains("IDLE"));

    // 8. Test Dual-Protocol Memcached Gateway
    // Memcached SET command
    let mc_set = b"set mc_fruit 0 0 5\r\napple\r\n";
    assert_eq!(send_and_read(&mut client, mc_set), "STORED\r\n");

    // Shared Keyspace: read via Redis protocol
    assert_eq!(send_and_read(&mut client, b"GET mc_fruit\r\n"), "$5\r\napple\r\n");

    // Memcached GET command (retrieves multiple keys)
    let mc_get = b"get mc_fruit non_existent\r\n";
    assert_eq!(send_and_read(&mut client, mc_get), "VALUE mc_fruit 0 5\r\napple\r\nEND\r\n");

    // Memcached STATS
    let mc_stats = send_and_read(&mut client, b"stats\r\n");
    assert!(mc_stats.contains("STAT pid"));
    assert!(mc_stats.contains("STAT version 1.6.0-rudis-dragonfly"));
    assert!(mc_stats.contains("END\r\n"));

    // Memcached VERSION
    let mc_ver = send_and_read(&mut client, b"version\r\n");
    assert_eq!(mc_ver, "VERSION 1.6.0-rudis-dragonfly\r\n");

    // Memcached DELETE
    assert_eq!(send_and_read(&mut client, b"delete mc_fruit\r\n"), "DELETED\r\n");
    assert_eq!(send_and_read(&mut client, b"delete mc_fruit\r\n"), "NOT_FOUND\r\n");
}













