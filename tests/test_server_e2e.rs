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




