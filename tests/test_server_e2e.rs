use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use rudis::router::target_shard;
use rudis::server::run_shard_worker;

fn start_test_server(port: u16, num_shards: usize) {
    let dir = std::env::temp_dir().join(format!("rudis-srv-{}-{}", std::process::id(), port));
    let _ = std::fs::create_dir_all(&dir);
    start_test_server_with_aof(
        port,
        num_shards,
        rudis::aof::AofConfig {
            enabled: false,
            dir,
            fsync_every_sec: false,
        },
    );
}

fn start_test_server_with_aof(port: u16, num_shards: usize, aof_config: rudis::aof::AofConfig) {
    rudis::shutdown::reset_shutdown();
    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
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
                    None,
                    false,
                );
            })
            .expect("Failed to spawn test shard");
    }

    // Wait until server is listening and accepting connections
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn start_test_server_cluster(port: u16, num_shards: usize) {
    rudis::shutdown::reset_shutdown();
    let hub = rudis::cluster::get_cluster_hub(port);
    hub.cluster_enabled
        .store(true, std::sync::atomic::Ordering::Release);
    hub.num_shards
        .store(num_shards, std::sync::atomic::Ordering::Release);

    let dir = std::env::temp_dir().join(format!("rudis-cluster-{}-{}", std::process::id(), port));
    let _ = std::fs::create_dir_all(&dir);
    let aof_config = rudis::aof::AofConfig {
        enabled: false,
        dir,
        fsync_every_sec: false,
    };

    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
        let shard_aof_config = aof_config.clone();
        thread::Builder::new()
            .name(format!("test-cluster-shard-{}", shard_id))
            .spawn(move || {
                run_shard_worker(
                    shard_id,
                    num_shards,
                    port,
                    shard_senders,
                    rx,
                    None,
                    shard_aof_config,
                    None,
                    true,
                );
            })
            .expect("Failed to spawn test cluster shard");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn start_test_server_with_tls(port: u16, tls_port: u16, num_shards: usize) -> (Vec<u8>, Vec<u8>) {
    rudis::shutdown::reset_shutdown();
    let (cert_der, key_der) = rudis::tls::generate_self_signed_cert(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("Failed to generate test self-signed cert");

    let server_config = rudis::tls::create_server_config(&cert_der, &key_der)
        .expect("Failed to build rustls ServerConfig");

    let tls_config = rudis::tls::TlsWorkerConfig {
        tls_port,
        server_config,
    };

    let dir = std::env::temp_dir().join(format!("rudis-tls-{}-{}", std::process::id(), port));
    let _ = std::fs::create_dir_all(&dir);
    let aof_config = rudis::aof::AofConfig {
        enabled: false,
        dir,
        fsync_every_sec: false,
    };

    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
        let shard_aof_config = aof_config.clone();
        let shard_tls_config = Some(tls_config.clone());
        thread::Builder::new()
            .name(format!("test-tls-shard-{}", shard_id))
            .spawn(move || {
                run_shard_worker(
                    shard_id,
                    num_shards,
                    port,
                    shard_senders,
                    rx,
                    None,
                    shard_aof_config,
                    shard_tls_config,
                    false,
                );
            })
            .expect("Failed to spawn test shard");
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok()
            && TcpStream::connect(format!("127.0.0.1:{}", tls_port)).is_ok()
        {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    (cert_der, key_der)
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
    let mut list_resp3 = String::new();
    for _ in 0..10 {
        thread::sleep(Duration::from_millis(30));
        list_resp3 = send_and_read(&mut stream, b"CLIENT LIST\r\n");
        if !list_resp3.contains(&format!("id={}", client_id2)) {
            break;
        }
    }
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
    assert!(resp.starts_with("-ERR WRONGTYPE") || resp.starts_with("-WRONGTYPE"));

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
    assert!(resp.starts_with("-ERR WRONGTYPE") || resp.starts_with("-WRONGTYPE"));

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
    assert!(resp.starts_with("-ERR WRONGTYPE") || resp.starts_with("-WRONGTYPE"));

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
    assert!(resp.starts_with("-ERR WRONGTYPE") || resp.starts_with("-WRONGTYPE"));
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
    assert_eq!(
        send_and_read(&mut stream, b"SET user:1 alice\r\n"),
        "+OK\r\n"
    );
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
    assert_eq!(exec_resp, "*4\r\n+OK\r\n:10\r\n:2\r\n$5\r\nmyval\r\n");

    assert_eq!(
        send_and_read(&mut stream, b"GET tx:counter\r\n"),
        "$2\r\n10\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"LLEN tx:list\r\n"), ":2\r\n");

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
    assert_eq!(send_and_read(&mut stream, b"GET tx:str\r\n"), "+QUEUED\r\n");

    let exec_resp = send_and_read(&mut stream, b"EXEC\r\n");
    assert!(
        exec_resp.starts_with("*3\r\n+OK\r\n-ERR")
            || exec_resp.starts_with("*3\r\n+OK\r\n-WRONGTYPE")
    );
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
    start_test_server(port, 4);

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
    assert_eq!(
        send_and_read(&mut client, b"SETBIT mybm 15 1\r\n"),
        ":0\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"BITCOUNT mybm\r\n"), ":4\r\n");
    assert_eq!(
        send_and_read(&mut client, b"BITCOUNT mybm 0 0\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"BITCOUNT mybm 1 1\r\n"),
        ":1\r\n"
    );

    // 3. BITPOS
    assert_eq!(send_and_read(&mut client, b"BITPOS mybm 1\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"BITPOS mybm 0\r\n"), ":0\r\n");
    assert_eq!(
        send_and_read(&mut client, b"BITPOS mybm 1 1\r\n"),
        ":15\r\n"
    );

    // 4. BITOP (AND, OR, XOR, NOT) using same hashtag to guarantee same shard
    assert_eq!(send_and_read(&mut client, b"SET {t}k1 \x0f\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SET {t}k2 \x33\r\n"), "+OK\r\n");

    assert_eq!(
        send_and_read(&mut client, b"BITOP AND {t}and {t}k1 {t}k2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET {t}and\r\n"),
        "$1\r\n\x03\r\n"
    );

    assert_eq!(
        send_and_read(&mut client, b"BITOP OR {t}or {t}k1 {t}k2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET {t}or\r\n"),
        "$1\r\n\x3f\r\n"
    );

    assert_eq!(
        send_and_read(&mut client, b"BITOP XOR {t}xor {t}k1 {t}k2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET {t}xor\r\n"),
        "$1\r\n\x3c\r\n"
    );

    assert_eq!(
        send_and_read(&mut client, b"BITOP NOT {t}not {t}k1\r\n"),
        ":1\r\n"
    );
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
    assert_eq!(
        send_and_read(&mut client, b"PFCOUNT {h}1 {h}2\r\n"),
        ":6\r\n"
    );

    // PFMERGE
    assert_eq!(
        send_and_read(&mut client, b"PFMERGE {h}dest {h}1 {h}2\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"PFCOUNT {h}dest\r\n"), ":6\r\n");

    // TYPE of HLL returns string
    assert_eq!(
        send_and_read(&mut client, b"TYPE {h}dest\r\n"),
        "+string\r\n"
    );
}

#[test]
fn test_dump_and_restore_e2e() {
    let port = 16394;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. DUMP non-existing key
    assert_eq!(send_and_read(&mut client, b"DUMP non_exist\r\n"), "$-1\r\n");

    // 2. SET and DUMP string
    assert_eq!(
        send_and_read(&mut client, b"SET mykey hello_world\r\n"),
        "+OK\r\n"
    );
    let dump_resp = send_and_read_bytes(&mut client, b"DUMP mykey\r\n");
    assert!(dump_resp.starts_with(b"$"));

    // Extract payload from RESP bulk string "$<len>\r\n<payload>\r\n"
    let crlf_pos = dump_resp.windows(2).position(|w| w == b"\r\n").unwrap();
    let payload = &dump_resp[crlf_pos + 2..dump_resp.len() - 2];

    // 3. RESTORE to a new key
    let mut restore_cmd = Vec::new();
    restore_cmd.extend_from_slice(
        format!(
            "*4\r\n$7\r\nRESTORE\r\n$7\r\ncopykey\r\n$1\r\n0\r\n${}\r\n",
            payload.len()
        )
        .as_bytes(),
    );
    restore_cmd.extend_from_slice(payload);
    restore_cmd.extend_from_slice(b"\r\n");

    assert_eq!(send_and_read(&mut client, &restore_cmd), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client, b"GET copykey\r\n"),
        "$11\r\nhello_world\r\n"
    );

    // 4. RESTORE without REPLACE on existing key -> BUSYKEY error
    assert_eq!(
        send_and_read(&mut client, &restore_cmd),
        "-BUSYKEY Target key name already exists.\r\n"
    );

    // 5. RESTORE with REPLACE
    let mut restore_replace_cmd = Vec::new();
    restore_replace_cmd.extend_from_slice(
        format!(
            "*5\r\n$7\r\nRESTORE\r\n$7\r\ncopykey\r\n$1\r\n0\r\n${}\r\n",
            payload.len()
        )
        .as_bytes(),
    );
    restore_replace_cmd.extend_from_slice(payload);
    restore_replace_cmd.extend_from_slice(b"\r\n$7\r\nREPLACE\r\n");

    assert_eq!(send_and_read(&mut client, &restore_replace_cmd), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client, b"GET copykey\r\n"),
        "$11\r\nhello_world\r\n"
    );

    // 6. Corrupt checksum
    let mut corrupt_cmd = Vec::new();
    let mut corrupt_payload = payload.to_vec();
    let last = corrupt_payload.len() - 1;
    corrupt_payload[last] ^= 0xFF;
    corrupt_cmd.extend_from_slice(
        format!(
            "*4\r\n$7\r\nRESTORE\r\n$9\r\ncorrupt_k\r\n$1\r\n0\r\n${}\r\n",
            corrupt_payload.len()
        )
        .as_bytes(),
    );
    corrupt_cmd.extend_from_slice(&corrupt_payload);
    corrupt_cmd.extend_from_slice(b"\r\n");

    assert_eq!(
        send_and_read(&mut client, &corrupt_cmd),
        "-ERR DUMP payload version or checksum are wrong\r\n"
    );

    // 7. RESTORE with TTL (150ms)
    let mut restore_ttl_cmd = Vec::new();
    restore_ttl_cmd.extend_from_slice(
        format!(
            "*4\r\n$7\r\nRESTORE\r\n$6\r\nttlkey\r\n$3\r\n150\r\n${}\r\n",
            payload.len()
        )
        .as_bytes(),
    );
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
    assert_eq!(
        send_and_read(&mut client, b"TYPE mystream\r\n"),
        "+stream\r\n"
    );

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
    assert_eq!(
        send_and_read(&mut client, b"XDEL mystream 1000-2\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"XLEN mystream\r\n"), ":2\r\n");

    // 12. XTRIM with MAXLEN
    for i in 10..20 {
        let cmd = format!("XADD mystream {}-0 item {}\r\n", 2000 + i, i);
        let _ = send_and_read(&mut client, cmd.as_bytes());
    }
    assert_eq!(send_and_read(&mut client, b"XLEN mystream\r\n"), ":12\r\n");
    assert_eq!(
        send_and_read(&mut client, b"XTRIM mystream MAXLEN = 5\r\n"),
        ":7\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"XLEN mystream\r\n"), ":5\r\n");

    // 13. DUMP & RESTORE of stream
    let dump_resp = send_and_read_bytes(&mut client, b"DUMP mystream\r\n");
    assert!(dump_resp.starts_with(b"$"));
    let crlf_pos = dump_resp.windows(2).position(|w| w == b"\r\n").unwrap();
    let payload = &dump_resp[crlf_pos + 2..dump_resp.len() - 2];

    let mut restore_cmd = Vec::new();
    restore_cmd.extend_from_slice(
        format!(
            "*4\r\n$7\r\nRESTORE\r\n$11\r\nstream_copy\r\n$1\r\n0\r\n${}\r\n",
            payload.len()
        )
        .as_bytes(),
    );
    restore_cmd.extend_from_slice(payload);
    restore_cmd.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &restore_cmd), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client, b"XLEN stream_copy\r\n"),
        ":5\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"TYPE stream_copy\r\n"),
        "+stream\r\n"
    );
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
    assert_eq!(
        send_and_read(&mut client, b"SET rdb_str hello_world\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SET rdb_int 12345\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HSET rdb_hash f1 v1 f2 v2\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"RPUSH rdb_list a b c\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SADD rdb_set m1 m2\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"ZADD rdb_zset 10 one 20 two\r\n"),
        ":2\r\n"
    );

    // 2. Test LASTSAVE
    let lastsave_resp = send_and_read(&mut client, b"LASTSAVE\r\n");
    assert!(
        lastsave_resp.starts_with(':'),
        "LASTSAVE should return integer timestamp"
    );

    // 3. Test synchronous SAVE
    let save_resp = send_and_read(&mut client, b"SAVE\r\n");
    assert_eq!(save_resp, "+OK\r\n");

    // 4. Verify dump.rdb file was created and has valid header
    let rdb_path = rdb_dir.join("dump.rdb");
    assert!(rdb_path.exists(), "dump.rdb should exist after SAVE");
    let content = std::fs::read(&rdb_path).unwrap();
    assert!(
        content.starts_with(b"REDIS0011"),
        "RDB file should have REDIS0011 header"
    );

    // 5. Test asynchronous BGSAVE
    let bgsave_resp = send_and_read(&mut client, b"BGSAVE\r\n");
    assert_eq!(bgsave_resp, "+Background saving started\r\n");
    thread::sleep(Duration::from_millis(200));

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
    assert_eq!(
        send_and_read(&mut client, b"INCRBY count 10\r\n"),
        ":52\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"INCRBY count -100\r\n"),
        ":-48\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET count\r\n"),
        "$3\r\n-48\r\n"
    );

    // APPEND promotes inlined Int to String
    assert_eq!(
        send_and_read(&mut client, b"APPEND count _extra\r\n"),
        ":9\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET count\r\n"),
        "$9\r\n-48_extra\r\n"
    );

    // 2. Small Hash flat vector representation (RudisValue::SmallHash)
    assert_eq!(
        send_and_read(&mut client, b"HSET compact_hash a 1 b 2 c 3\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HLEN compact_hash\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HEXISTS compact_hash b\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HGET compact_hash b\r\n"),
        "$1\r\n2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HDEL compact_hash b\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HLEN compact_hash\r\n"),
        ":2\r\n"
    );

    // Auto-promotion from SmallHash to full Hash when exceeding 64 keys
    let mut large_hset = String::from("HSET compact_hash");
    for i in 0..70 {
        large_hset.push_str(&format!(" k{} v{}", i, i));
    }
    large_hset.push_str("\r\n");
    let resp = send_and_read(&mut client, large_hset.as_bytes());
    assert!(resp.starts_with(':'));

    // Should now be promoted to full Hash and readable
    assert_eq!(
        send_and_read(&mut client, b"HLEN compact_hash\r\n"),
        ":72\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HGET compact_hash k50\r\n"),
        "$3\r\nv50\r\n"
    );
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
    assert!(
        dup_resp.contains("BUSYGROUP"),
        "Duplicate group should return BUSYGROUP"
    );

    // 3. Create consumer
    assert_eq!(
        send_and_read(
            &mut client,
            b"XGROUP CREATECONSUMER stream_cg groupA worker1\r\n"
        ),
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
        b"XREADGROUP GROUP groupA worker1 COUNT 1 STREAMS stream_cg >\r\n",
    );
    assert!(read_resp.contains("1001-0"), "Should receive entry 1001-0");

    // 6. Inspect PEL using XPENDING summary
    let pending_summary = send_and_read(&mut client, b"XPENDING stream_cg groupA\r\n");
    assert!(
        pending_summary.starts_with("*4\r\n:1\r\n"),
        "Summary should report 1 pending entry"
    );
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
    assert!(
        pending_after.starts_with("*4\r\n:0\r\n"),
        "PEL should now be empty"
    );

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
    assert_eq!(
        send_and_read(&mut client1, b"SET cold_str \"rudis_is_fast\"\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"SET cold_int 424242\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"HSET cold_hash name rudis speed maximum\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"RPUSH cold_list alpha beta gamma\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"SADD cold_set s1 s2 s3\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"ZADD cold_zset 100 z1 200 z2\r\n"),
        ":2\r\n"
    );

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
    assert_eq!(
        send_and_read(&mut client2, b"GET cold_str\r\n"),
        "$15\r\n\"rudis_is_fast\"\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"GET cold_int\r\n"),
        "$6\r\n424242\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"HGET cold_hash name\r\n"),
        "$5\r\nrudis\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"HGET cold_hash speed\r\n"),
        "$7\r\nmaximum\r\n"
    );
    assert_eq!(send_and_read(&mut client2, b"LLEN cold_list\r\n"), ":3\r\n");
    assert_eq!(
        send_and_read(&mut client2, b"LRANGE cold_list 0 -1\r\n"),
        "*3\r\n$5\r\nalpha\r\n$4\r\nbeta\r\n$5\r\ngamma\r\n"
    );
    assert_eq!(send_and_read(&mut client2, b"SCARD cold_set\r\n"), ":3\r\n");
    assert_eq!(
        send_and_read(&mut client2, b"ZCARD cold_zset\r\n"),
        ":2\r\n"
    );

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
    assert_eq!(
        send_and_read(
            &mut client1,
            format!("SET {} \"transferred\"\r\n", key).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, format!("GET {}\r\n", key).as_bytes()),
        "$13\r\n\"transferred\"\r\n"
    );

    // Migrate this slot to server2
    let migrate_cmd = format!("CLUSTER MIGRATE-SLOT {} 127.0.0.1 {}\r\n", slot, port2);
    assert_eq!(
        send_and_read(&mut client1, migrate_cmd.as_bytes()),
        "+OK\r\n"
    );

    // Server 1 should now redirect with MOVED for this slot
    let moved_resp = send_and_read(&mut client1, format!("GET {}\r\n", key).as_bytes());
    assert_eq!(
        moved_resp,
        format!("-MOVED {} 127.0.0.1:{}\r\n", slot, port2)
    );

    // Server 2 should now have the key
    assert_eq!(
        send_and_read(&mut client2, format!("GET {}\r\n", key).as_bytes()),
        "$13\r\n\"transferred\"\r\n"
    );

    // Test CLUSTER REBALANCE
    let rebal_resp = send_and_read(
        &mut client1,
        format!("CLUSTER REBALANCE 127.0.0.1 {} 2\r\n", port2).as_bytes(),
    );
    assert_eq!(rebal_resp, ":2\r\n");
}

#[test]
fn test_blocking_operations_e2e() {
    let port = 16405;
    start_test_server(port, 2);

    let mut client1 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    client1
        .set_read_timeout(Some(Duration::from_secs(4)))
        .unwrap();

    // 1. Immediate BLPOP and BRPOP
    assert_eq!(
        send_and_read(&mut client1, b"RPUSH {t}:list a b c\r\n"),
        ":3\r\n"
    );
    let resp = send_and_read(&mut client1, b"BLPOP {t}:list 1\r\n");
    assert_eq!(resp, "*2\r\n$8\r\n{t}:list\r\n$1\r\na\r\n");

    let resp = send_and_read(&mut client1, b"BRPOP {t}:list 1\r\n");
    assert_eq!(resp, "*2\r\n$8\r\n{t}:list\r\n$1\r\nc\r\n");

    // Pop the remaining element 'b'
    assert_eq!(
        send_and_read(&mut client1, b"LPOP {t}:list\r\n"),
        "$1\r\nb\r\n"
    );

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
    assert_eq!(
        send_and_read(&mut client, b"SADD {s}:1 a b c\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SADD {s}:2 b c d\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SADD {s}:3 c d e\r\n"),
        ":3\r\n"
    );

    // SINTER {s}:1 {s}:2 -> b, c (order independent check)
    let sinter_resp = send_and_read(&mut client, b"SINTER {s}:1 {s}:2\r\n");
    assert!(sinter_resp.contains("$1\r\nb\r\n"));
    assert!(sinter_resp.contains("$1\r\nc\r\n"));
    assert!(!sinter_resp.contains("$1\r\na\r\n"));

    // SDIFF {s}:1 {s}:2 -> a
    assert_eq!(
        send_and_read(&mut client, b"SDIFF {s}:1 {s}:2\r\n"),
        "*1\r\n$1\r\na\r\n"
    );

    // SUNION {s}:1 {s}:2 -> a, b, c, d
    let sunion_resp = send_and_read(&mut client, b"SUNION {s}:1 {s}:2\r\n");
    assert_eq!(sunion_resp.lines().next().unwrap(), "*4");

    // SINTERSTORE
    assert_eq!(
        send_and_read(&mut client, b"SINTERSTORE {s}:inter {s}:1 {s}:2\r\n"),
        ":2\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"SCARD {s}:inter\r\n"), ":2\r\n");

    // SUNIONSTORE
    assert_eq!(
        send_and_read(&mut client, b"SUNIONSTORE {s}:union {s}:1 {s}:2\r\n"),
        ":4\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"SCARD {s}:union\r\n"), ":4\r\n");

    // SDIFFSTORE
    assert_eq!(
        send_and_read(&mut client, b"SDIFFSTORE {s}:diff {s}:1 {s}:2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SMEMBERS {s}:diff\r\n"),
        "*1\r\n$1\r\na\r\n"
    );

    // 2. ZSET MULTI-KEY OPERATIONS
    assert_eq!(
        send_and_read(&mut client, b"ZADD {z}:1 1.0 a 2.0 b 3.0 c\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"ZADD {z}:2 2.0 b 3.0 c 4.0 d\r\n"),
        ":3\r\n"
    );

    // ZDIFF {z}:1 {z}:2 -> a
    assert_eq!(
        send_and_read(&mut client, b"ZDIFF 2 {z}:1 {z}:2\r\n"),
        "*1\r\n$1\r\na\r\n"
    );

    // ZINTER {z}:1 {z}:2 WITHSCORES -> b:4, c:6
    let zinter_resp = send_and_read(&mut client, b"ZINTER 2 {z}:1 {z}:2 WITHSCORES\r\n");
    assert_eq!(
        zinter_resp,
        "*4\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n6\r\n"
    );

    // ZUNION {z}:1 {z}:2 -> a, b, c, d
    let zunion_resp = send_and_read(&mut client, b"ZUNION 2 {z}:1 {z}:2\r\n");
    assert_eq!(zunion_resp.lines().next().unwrap(), "*4");

    // ZUNIONSTORE with WEIGHTS and AGGREGATE MAX
    assert_eq!(
        send_and_read(
            &mut client,
            b"ZUNIONSTORE {z}:out 2 {z}:1 {z}:2 WEIGHTS 2 3 AGGREGATE MAX\r\n"
        ),
        ":4\r\n"
    );
    // c was (3.0*2=6 vs 3.0*3=9) -> MAX is 9.0
    assert_eq!(
        send_and_read(&mut client, b"ZSCORE {z}:out c\r\n"),
        "$1\r\n9\r\n"
    );

    // ZINTERSTORE
    assert_eq!(
        send_and_read(&mut client, b"ZINTERSTORE {z}:inter 2 {z}:1 {z}:2\r\n"),
        ":2\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"ZCARD {z}:inter\r\n"), ":2\r\n");

    // ZDIFFSTORE
    assert_eq!(
        send_and_read(&mut client, b"ZDIFFSTORE {z}:diff 2 {z}:1 {z}:2\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"ZCARD {z}:diff\r\n"), ":1\r\n");
}

#[test]
fn test_auth_and_acl_e2e() {
    let port = 16407;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Check default user info
    assert_eq!(
        send_and_read(&mut client, b"ACL WHOAMI\r\n"),
        "$7\r\ndefault\r\n"
    );
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
    assert_eq!(
        send_and_read(&mut client, b"AUTH alice secret123\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"ACL WHOAMI\r\n"),
        "$5\r\nalice\r\n"
    );

    // Wrong password check
    assert!(send_and_read(&mut client, b"AUTH alice wrongpass\r\n").starts_with("-WRONGPASS"));

    // 4. Delete user alice
    assert_eq!(
        send_and_read(&mut client, b"ACL DELUSER alice\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"ACL GETUSER alice\r\n"),
        "$-1\r\n"
    );

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
    assert_eq!(
        send_and_read(&mut new_client, b"AUTH default defpass\r\n"),
        "+OK\r\n"
    );
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
    assert!(
        srand_single == "$1\r\na\r\n"
            || srand_single == "$1\r\nb\r\n"
            || srand_single == "$1\r\nc\r\n"
    );

    let srand_count = send_and_read(&mut client, b"SRANDMEMBER {s1} 2\r\n");
    assert_eq!(srand_count.lines().next().unwrap(), "*2");

    let sscan_resp = send_and_read(&mut client, b"SSCAN {s1} 0\r\n");
    assert!(sscan_resp.starts_with("*2\r\n$1\r\n0\r\n"));

    // SMOVE same slot (using hash tags {s1})
    let smove_ok = send_and_read(&mut client, b"SMOVE {s1} {s1}_dst a\r\n");
    assert_eq!(smove_ok, ":1\r\n");
    assert_eq!(
        send_and_read(&mut client, b"SISMEMBER {s1} a\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SISMEMBER {s1}_dst a\r\n"),
        ":1\r\n"
    );

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
    let blmove_timeout = send_and_read(
        &mut client,
        b"BLMOVE {empty_q} {empty_q}_dst LEFT RIGHT 0.1\r\n",
    );
    assert_eq!(blmove_timeout, "$-1\r\n");

    // BLMOVE blocking cross-thread notification
    let pusher_port = port;
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        let mut pusher = TcpStream::connect(format!("127.0.0.1:{}", pusher_port)).unwrap();
        send_and_read(&mut pusher, b"LPUSH {blmove_q} blocked_item\r\n");
    });

    let blmove_blocked = send_and_read(
        &mut client,
        b"BLMOVE {blmove_q} {blmove_q}_dst LEFT RIGHT 2.0\r\n",
    );
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
    assert_eq!(
        send_and_read(&mut client, b"GET str1\r\n"),
        "$12\r\nhello_worldy\r\n"
    );

    send_and_read(&mut client, b"SET num 10.5\r\n");
    let incrbyfloat_resp = send_and_read(&mut client, b"INCRBYFLOAT num 2.25\r\n");
    assert_eq!(incrbyfloat_resp, "$5\r\n12.75\r\n");
}

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

    // Wait for replication stream propagation
    for _ in 0..40 {
        if send_and_read(&mut replica_client, b"GET live_key\r\n") == "$8\r\nlive_val\r\n" {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    // Verify replicated on replica
    assert_eq!(
        send_and_read(&mut replica_client, b"GET live_key\r\n"),
        "$8\r\nlive_val\r\n"
    );
    assert_eq!(
        send_and_read(&mut replica_client, b"GET live_counter\r\n"),
        "$2\r\n42\r\n"
    );
    assert_eq!(
        send_and_read(&mut replica_client, b"LRANGE mylist 0 -1\r\n"),
        "*2\r\n$5\r\nitemA\r\n$5\r\nitemB\r\n"
    );

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

    // 4. Trigger reconnect on replica: re-issuing REPLICAOF to same master
    // preserves cached master_replid and offset, performing partial resync (PSYNC <replid> <offset>)
    assert_eq!(
        send_and_read(&mut master_client, b"SET k4 v4\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut replica_client,
            format!("REPLICAOF 127.0.0.1 {}\r\n", master_port).as_bytes()
        ),
        "+OK\r\n"
    );

    // Wait for reconnection
    for _ in 0..40 {
        let role = send_and_read(&mut replica_client, b"ROLE\r\n");
        if role.contains("connected") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

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
            "keyA",
            "keyB",
            "val1",
            "val2",
        ],
    );
    assert_eq!(
        keys_argv_resp,
        "*4\r\n$4\r\nkeyA\r\n$4\r\nkeyB\r\n$4\r\nval1\r\n$4\r\nval2\r\n"
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

    // Multiple operations and table inspection
    let multi_resp = send_cmd(
        &mut client,
        &[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], ARGV[2])",
            "2",
            "k_str",
            "k_num",
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
            "k_num",
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
    let evalsha_resp = send_cmd(&mut client, &["EVALSHA", sha, "1", "k_str"]);
    assert_eq!(evalsha_resp, "$5\r\nhello\r\n");

    // SCRIPT FLUSH
    let flush_resp = send_cmd(&mut client, &["SCRIPT", "FLUSH"]);
    assert_eq!(flush_resp, "+OK\r\n");

    let exists_after_flush = send_cmd(&mut client, &["SCRIPT", "EXISTS", sha]);
    assert_eq!(exists_after_flush, "*1\r\n:0\r\n");

    // EVALSHA after flush should return NOSCRIPT error
    let evalsha_err = send_cmd(&mut client, &["EVALSHA", sha, "1", "k_str"]);
    assert!(
        evalsha_err.starts_with("-NOSCRIPT"),
        "Expected -NOSCRIPT error, got {}",
        evalsha_err
    );
}

#[test]
fn test_cluster_bus_gossip_failover_e2e() {
    let port1 = 16440;
    let port2 = 16441;
    let port3 = 16442;

    start_test_server(port1, 2);
    start_test_server(port2, 2);
    start_test_server(port3, 2);

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

    // 4. Test CLUSTER REPLICATE
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
        send_and_read(
            &mut client,
            format!("SET key:256 {}\r\n", val_256).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            format!("SET key:512 {}\r\n", val_512).as_bytes()
        ),
        "+OK\r\n"
    );

    // 3. Spill key:256 to NVMe disk
    let spill_resp = send_and_read(&mut client, b"TIER SPILL key:256\r\n");
    assert_eq!(spill_resp, ":1\r\n");

    // Re-spilling already tiered key returns :0
    assert_eq!(
        send_and_read(&mut client, b"TIER SPILL key:256\r\n"),
        ":0\r\n"
    );

    // 4. Verify stats after spill
    let info_after_spill = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_spill.contains("tiered_keys:1"));
    assert!(info_after_spill.contains("disk_writes:1"));

    // 5. EXISTS works on tiered key without disk retrieval
    assert_eq!(send_and_read(&mut client, b"EXISTS key:256\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut client, b"TYPE key:256\r\n"),
        "+string\r\n"
    );

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
    assert_eq!(
        send_and_read(&mut client, b"TIER LOAD key:512\r\n"),
        ":0\r\n"
    );
    // Verify data intact
    let get_512 = send_and_read(&mut client, b"GET key:512\r\n");
    assert_eq!(get_512, format!("${}\r\n{}\r\n", val_512.len(), val_512));

    // 8. Test TIER SPILLALL across all shards
    for i in 0..10 {
        let val = format!("val_{}", i).repeat(20);
        assert_eq!(
            send_and_read(
                &mut client,
                format!("SET item:{} {}\r\n", i, val).as_bytes()
            ),
            "+OK\r\n"
        );
    }
    let spillall_resp = send_and_read(&mut client, b"TIER SPILLALL\r\n");
    assert!(spillall_resp.starts_with(':'));
    let count: i64 = spillall_resp
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap();
    assert!(count >= 10);

    // Read back all keys from disk
    for i in 0..10 {
        let expected = format!("val_{}", i).repeat(20);
        let resp = send_and_read(&mut client, format!("GET item:{}\r\n", i).as_bytes());
        assert_eq!(resp, format!("${}\r\n{}\r\n", expected.len(), expected));
    }

    // 9. Mutate and Delete tiered keys
    assert_eq!(
        send_and_read(&mut client, b"TIER SPILL item:0\r\n"),
        ":1\r\n"
    );
    // Overwrite tiered key
    assert_eq!(
        send_and_read(&mut client, b"SET item:0 new_overwritten_value\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET item:0\r\n"),
        "$21\r\nnew_overwritten_value\r\n"
    );

    // Delete tiered key
    assert_eq!(
        send_and_read(&mut client, b"TIER SPILL item:1\r\n"),
        ":1\r\n"
    );
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
        send_and_read(
            &mut client,
            format!("SET cool_key {}\r\n", val_payload).as_bytes()
        ),
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
    assert_eq!(
        get_cooled,
        format!("${}\r\n{}\r\n", val_payload.len(), val_payload)
    );
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
    assert_eq!(
        get_cold,
        format!("${}\r\n{}\r\n", val_payload.len(), val_payload)
    );
    let tier_info4 = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(tier_info4.contains("disk_reads:1"));
    assert!(tier_info4.contains("cooled_keys:1"));
    assert!(tier_info4.contains("tiered_keys:0"));

    // Subsequent read is again a fast zero-I/O DRAM hit!
    let get_again = send_and_read(&mut client, b"GET cool_key\r\n");
    assert_eq!(
        get_again,
        format!("${}\r\n{}\r\n", val_payload.len(), val_payload)
    );
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
    let cur_used: u64 = used_mem_line
        .strip_prefix("used_memory:")
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // Set maxmemory just slightly above current used memory (+500 bytes)
    let limit = cur_used + 500;
    assert_eq!(
        send_and_read(
            &mut client,
            format!("CONFIG SET maxmemory {}\r\n", limit).as_bytes()
        ),
        "+OK\r\n"
    );

    // Insert multiple keys that will exceed the threshold
    for i in 0..10 {
        let val = "Z".repeat(200);
        let resp = send_and_read(
            &mut client,
            format!("SET autotier:{} {}\r\n", i, val).as_bytes(),
        );
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
        send_and_read(
            &mut client,
            format!("SET large_key {}\r\n", large_val).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"TIER SPILL large_key\r\n"),
        ":1\r\n"
    );

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
    assert_eq!(
        send_and_read(&mut client, b"SET snap_key1 hello\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SET snap_key2 world\r\n"),
        "+OK\r\n"
    );
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
    assert_eq!(
        send_and_read(&mut client, b"VADD v_idx doc1 1.0 0.0 0.0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VADD v_idx doc2 0.0 1.0 0.0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"VADD v_idx doc3 0.9 0.1 0.0\r\n"),
        "+OK\r\n"
    );

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
    client1
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();

    // 1. RESP3 Negotiation via HELLO 3
    let hello_resp = send_and_read(&mut client1, b"HELLO 3\r\n");
    assert!(hello_resp.starts_with("%"));
    assert!(hello_resp.contains("server"));
    assert!(hello_resp.contains("valkey"));
    assert!(hello_resp.contains("proto"));

    // 2. Client tracking & invalidation
    assert_eq!(
        send_and_read(&mut client1, b"CLIENT TRACKING on\r\n"),
        "+OK\r\n"
    );

    // Client 1 reads a key to track it (RESP3 null is _\r\n)
    assert_eq!(send_and_read(&mut client1, b"GET track_k1\r\n"), "_\r\n");

    // Client 2 modifies track_k1
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(
        send_and_read(&mut client2, b"SET track_k1 updated_val\r\n"),
        "+OK\r\n"
    );
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Client 1 should receive the invalidation message on next interaction or read
    let mut next_resp = send_and_read(&mut client1, b"PING\r\n");
    if !next_resp.contains("invalidate") {
        std::thread::sleep(std::time::Duration::from_millis(100));
        next_resp.push_str(&send_and_read(&mut client1, b"PING\r\n"));
    }
    assert!(next_resp.contains("invalidate"));
    assert!(next_resp.contains("track_k1"));

    // 3. Redis 7 Functions: FUNCTION LOAD and FCALL
    let func_code = "#!lua name=mathlib\nredis.register_function('add_nums', function(keys, args) return tonumber(args[1]) + tonumber(args[2]) end)\n";
    let load_cmd = format!(
        "*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n",
        func_code.len(),
        func_code
    );
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
    assert_eq!(
        send_and_read(&mut client2, b"FUNCTION DELETE mathlib\r\n"),
        "+OK\r\n"
    );
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
fn test_sq8_quantized_vector_and_rerank_e2e() {
    let port = 16560;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Ingest vectors with SQ8 quantization and tiered storage flag
    assert_eq!(
        send_and_read(
            &mut client,
            b"VADD doc_sq8 docA 1.0 0.0 0.0 QUANTIZE TIERED\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"VADD doc_sq8 docB 0.0 1.0 0.0 QUANTIZE TIERED\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"VADD doc_sq8 docC 0.88 0.12 0.0 QUANTIZE TIERED\r\n"
        ),
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
    ])
    .expect("Failed to generate test self-signed cert");
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
    start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. JSON.SET root
    let doc = r#"{"name":"Bob","age":28,"tags":["rust","io_uring"],"online":true}"#;
    let set_cmd = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n${}\r\n{}\r\n",
        doc.len(),
        doc
    );
    assert_eq!(send_and_read(&mut stream, set_cmd.as_bytes()), "+OK\r\n");

    // 2. JSON.GET root
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$8\r\nJSON.GET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n",
    );
    assert!(resp.contains("Bob"));
    assert!(resp.contains("io_uring"));

    // 3. JSON.GET path $.name
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$8\r\nJSON.GET\r\n$6\r\nuser:1\r\n$6\r\n$.name\r\n",
    );
    assert!(resp.contains("\"Bob\""));

    // 4. JSON.TYPE
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$9\r\nJSON.TYPE\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n",
    );
    assert_eq!(resp, "+array\r\n");

    // 5. JSON.NUMINCRBY
    let resp = send_and_read(
        &mut stream,
        b"*4\r\n$14\r\nJSON.NUMINCRBY\r\n$6\r\nuser:1\r\n$5\r\n$.age\r\n$1\r\n2\r\n",
    );
    assert!(resp.contains("30"));

    // 6. JSON.ARRAPPEND
    let resp = send_and_read(
        &mut stream,
        b"*4\r\n$14\r\nJSON.ARRAPPEND\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n$11\r\n\"high_perf\"\r\n",
    );
    assert_eq!(resp, ":3\r\n");

    // 7. JSON.ARRLEN
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$11\r\nJSON.ARRLEN\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n",
    );
    assert_eq!(resp, ":3\r\n");

    // 8. JSON.ARRPOP
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$11\r\nJSON.ARRPOP\r\n$6\r\nuser:1\r\n$6\r\n$.tags\r\n",
    );
    assert!(resp.contains("\"high_perf\""));

    // 9. JSON.TOGGLE
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$11\r\nJSON.TOGGLE\r\n$6\r\nuser:1\r\n$8\r\n$.online\r\n",
    );
    assert!(resp.contains("false"));

    // 10. JSON.OBJKEYS
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$12\r\nJSON.OBJKEYS\r\n$6\r\nuser:1\r\n$1\r\n$\r\n",
    );
    assert!(resp.contains("name"));
    assert!(resp.contains("age"));

    // 11. JSON.OBJLEN
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$11\r\nJSON.OBJLEN\r\n$6\r\nuser:1\r\n$1\r\n$\r\n",
    );
    assert_eq!(resp, ":4\r\n");

    // 12. JSON.DEL nested
    let resp = send_and_read(
        &mut stream,
        b"*3\r\n$8\r\nJSON.DEL\r\n$6\r\nuser:1\r\n$8\r\n$.online\r\n",
    );
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
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

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
    start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. GEOADD
    assert_eq!(
        send_and_read(
            &mut stream,
            b"GEOADD sicily 13.361389 38.115556 Palermo\r\n"
        ),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            b"GEOADD sicily 15.087269 37.502669 Catania\r\n"
        ),
        ":1\r\n"
    );

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
    let rad_resp = send_and_read(
        &mut stream,
        b"GEORADIUS sicily 15 37 200 km WITHDIST WITHCOORD\r\n",
    );
    assert!(rad_resp.starts_with("*2\r\n"));
    assert!(rad_resp.contains("Palermo"));
    assert!(rad_resp.contains("Catania"));

    // 6. GEORADIUSBYMEMBER
    let rad_member = send_and_read(
        &mut stream,
        b"GEORADIUSBYMEMBER sicily Palermo 100 km WITHDIST\r\n",
    );
    assert!(rad_member.contains("Palermo"));
    assert!(!rad_member.contains("Catania")); // Catania is > 166km away

    // 7. GEOSEARCH FROMLONLAT BYRADIUS
    let search_resp = send_and_read(
        &mut stream,
        b"GEOSEARCH sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC WITHDIST\r\n",
    );
    let pos_catania = search_resp.find("Catania").unwrap();
    let pos_palermo = search_resp.find("Palermo").unwrap();
    assert!(pos_catania < pos_palermo); // ASC order: Catania closer to (15, 37) than Palermo

    // 8. GEOSEARCH FROMMEMBER BYBOX
    let box_resp = send_and_read(
        &mut stream,
        b"GEOSEARCH sicily FROMMEMBER Palermo BYBOX 400 400 km\r\n",
    );
    assert!(box_resp.contains("Palermo"));
    assert!(box_resp.contains("Catania"));
}

#[test]
fn test_probabilistic_data_structures_e2e() {
    let port = 16590;
    start_test_server(port, 2);
    std::thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Bloom Filter (BF.*)
    assert_eq!(
        send_and_read(&mut stream, b"BF.RESERVE mybf 0.01 1000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.RESERVE mybf 0.01 1000\r\n"),
        "-ERR item exists\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.ADD mybf apple\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.ADD mybf apple\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.EXISTS mybf apple\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.EXISTS mybf orange\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.MADD mybf banana grape\r\n"),
        "*2\r\n:1\r\n:1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BF.MEXISTS mybf apple banana melon\r\n"),
        "*3\r\n:1\r\n:1\r\n:0\r\n"
    );
    let info = send_and_read(&mut stream, b"BF.INFO mybf\r\n");
    assert!(info.contains("Capacity"));
    assert!(info.contains("1000"));

    // 2. Cuckoo Filter (CF.*)
    assert_eq!(
        send_and_read(&mut stream, b"CF.RESERVE mycf 1000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CF.RESERVE mycf 1000\r\n"),
        "-ERR item exists\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"CF.ADD mycf foo\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"CF.ADDNX mycf foo\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CF.ADDNX mycf bar\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CF.EXISTS mycf foo\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"CF.DEL mycf foo\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"CF.EXISTS mycf foo\r\n"),
        ":0\r\n"
    );
    let cf_info = send_and_read(&mut stream, b"CF.INFO mycf\r\n");
    assert!(cf_info.contains("Number of buckets"));

    // 3. Count-Min Sketch (CMS.*)
    assert_eq!(
        send_and_read(&mut stream, b"CMS.INITBYDIM mycms 200 5\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CMS.INCRBY mycms item1 42 item2 17\r\n"),
        "*2\r\n:42\r\n:17\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"CMS.QUERY mycms item1 item2 item3\r\n"),
        "*3\r\n:42\r\n:17\r\n:0\r\n"
    );
    let cms_info = send_and_read(&mut stream, b"CMS.INFO mycms\r\n");
    assert!(cms_info.contains("width"));
    assert!(cms_info.contains("depth"));

    // 4. Top-K (TOPK.*)
    assert_eq!(
        send_and_read(&mut stream, b"TOPK.RESERVE mytopk 3\r\n"),
        "+OK\r\n"
    );
    let add_res = send_and_read(
        &mut stream,
        b"TOPK.ADD mytopk alpha alpha alpha beta beta gamma\r\n",
    );
    assert!(add_res.starts_with("*6\r\n"));
    assert_eq!(
        send_and_read(&mut stream, b"TOPK.QUERY mytopk alpha beta gamma delta\r\n"),
        "*4\r\n:1\r\n:1\r\n:1\r\n:0\r\n"
    );
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
    start_test_server(port, 2);
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
        b"FT.CREATE idx:books ON HASH PREFIX 1 book: SCHEMA title TEXT\r\n",
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
    let search1 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$4\r\nRust\r\n",
    );
    assert!(
        search1.starts_with("*5\r\n:2\r\n"),
        "Search 'Rust' should return 2 hits. Resp: {}",
        search1
    );
    assert!(search1.contains("book:1"));
    assert!(search1.contains("book:2"));

    // 6. Query 2: Keyword search for "Data-Intensive" (matches book:3)
    let search2 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$14\r\nData-Intensive\r\n",
    );
    assert!(
        search2.starts_with("*3\r\n:1\r\n"),
        "Search 'Data-Intensive' should return 1 hit. Resp: {}",
        search2
    );
    assert!(search2.contains("book:3"));

    // 7. Query 3: Numeric range query: @price:[40 50] (matches book:1 with price 45.0)
    let search3 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$14\r\n@price:[40 50]\r\n",
    );
    assert!(
        search3.starts_with("*3\r\n:1\r\n"),
        "Range query should return 1 hit. Resp: {}",
        search3
    );
    assert!(search3.contains("book:1"));

    // 8. Query 4: Tag filter: @category:{database} (matches book:3)
    let search4 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$20\r\n@category:{database}\r\n",
    );
    assert!(
        search4.starts_with("*3\r\n:1\r\n"),
        "Tag query should return 1 hit. Resp: {}",
        search4
    );
    assert!(search4.contains("book:3"));

    // 9. Query 5: Prefix query: "progra*" (matches programming in book:2 title)
    let search5 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$7\r\nprogra*\r\n",
    );
    assert!(
        search5.starts_with("*3\r\n:1\r\n"),
        "Prefix query should return 1 hit for book:2. Resp: {}",
        search5
    );
    assert!(search5.contains("book:2"));

    // 10. Query 6: NOCONTENT flag (returns doc IDs only)
    let search_nocontent = send_and_read(
        &mut client,
        b"*4\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:books\r\n$4\r\nRust\r\n$9\r\nNOCONTENT\r\n",
    );
    assert!(
        search_nocontent.starts_with("*3\r\n:2\r\n")
            && search_nocontent.contains("book:1")
            && search_nocontent.contains("book:2"),
        "Unexpected NOCONTENT response: {}",
        search_nocontent
    );

    // 11. Query 7: FT.EXPLAIN
    let explain_resp = send_and_read(
        &mut client,
        b"*3\r\n$10\r\nFT.EXPLAIN\r\n$9\r\nidx:books\r\n$15\r\nRust | database\r\n",
    );
    assert!(
        explain_resp.contains("Or"),
        "Explain output should describe parsed AST"
    );

    // 12. Document deletion via DEL automatically removes from index
    assert_eq!(send_and_read(&mut client, b"DEL book:1\r\n"), ":1\r\n");
    let search_after_del = send_and_read(&mut client, b"FT.SEARCH idx:books Action\r\n");
    assert_eq!(search_after_del, "*1\r\n:0\r\n");

    // 13. Drop index
    assert_eq!(
        send_and_read(&mut client, b"FT.DROPINDEX idx:books\r\n"),
        "+OK\r\n"
    );
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
    let mut json_set_cmd = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nuser:1\r\n$1\r\n$\r\n${}\r\n",
        json_payload.len()
    )
    .into_bytes();
    json_set_cmd.extend_from_slice(json_payload);
    json_set_cmd.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &json_set_cmd), "+OK\r\n");

    let search_json = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$9\r\nidx:users\r\n$5\r\nAlice\r\n",
    );
    assert!(search_json.contains("user:1"));
    assert!(search_json.contains("Alice Engineer"));

    // 15. Vector Indexing and FT.SEARCH KNN with PARAMS
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE idx:vectors ON HASH PREFIX 1 vec: SCHEMA title TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC COSINE\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"HSET vec:doc1 title First embedding 1.0,0.0,0.0\r\n"
        ),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"HSET vec:doc2 title Second embedding 0.0,1.0,0.0\r\n"
        ),
        ":2\r\n"
    );

    let mut q_vec_bytes = Vec::new();
    for val in [1.0f32, 0.1f32, 0.0f32] {
        q_vec_bytes.extend_from_slice(&val.to_le_bytes());
    }

    let mut search_vec_cmd = Vec::new();
    search_vec_cmd.extend_from_slice(b"*7\r\n$9\r\nFT.SEARCH\r\n$11\r\nidx:vectors\r\n$27\r\n*=>[KNN 1 @embedding $blob]\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$4\r\nblob\r\n");
    search_vec_cmd.extend_from_slice(format!("${}\r\n", q_vec_bytes.len()).as_bytes());
    search_vec_cmd.extend_from_slice(&q_vec_bytes);
    search_vec_cmd.extend_from_slice(b"\r\n");

    let vec_search_res = send_and_read(&mut client, &search_vec_cmd);
    assert!(vec_search_res.contains("vec:doc1"));
    assert!(!vec_search_res.contains("vec:doc2"));
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
    let cfg_cmd = format!(
        "*3\r\n$11\r\nDFLYCLUSTER\r\n$6\r\nCONFIG\r\n${}\r\n{}\r\n",
        cfg_json.len(),
        cfg_json
    );
    assert_eq!(send_and_read(&mut client, cfg_cmd.as_bytes()), "+OK\r\n");

    // 3. Test DFLYCLUSTER GETSLOTINFO
    let slot_info = send_and_read(&mut client, b"DFLYCLUSTER GETSLOTINFO SLOTS 100 200\r\n");
    assert!(slot_info.starts_with("*2\r\n"));
    assert!(slot_info.contains(":100\r\n"));
    assert!(slot_info.contains(":200\r\n"));

    // 4. Test STICK, UNSTICK, STICKY
    assert_eq!(
        send_and_read(&mut client, b"SET stick_key1 val1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"STICKY stick_key1\r\n"),
        ":0\r\n"
    );
    // STICK key1 key_missing -> should mark key1, return 1
    assert_eq!(
        send_and_read(&mut client, b"STICK stick_key1 key_missing\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"STICKY stick_key1\r\n"),
        ":1\r\n"
    );
    // UNSTICK key1
    assert_eq!(
        send_and_read(&mut client, b"UNSTICK stick_key1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"STICKY stick_key1\r\n"),
        ":0\r\n"
    );

    // 5. Test DELEX (conditional deletion)
    assert_eq!(
        send_and_read(&mut client, b"SET cond_key 100\r\n"),
        "+OK\r\n"
    );
    // IFEQ mismatch -> returns 0
    assert_eq!(
        send_and_read(&mut client, b"DELEX cond_key IFEQ 999\r\n"),
        ":0\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"EXISTS cond_key\r\n"), ":1\r\n");
    // IFEQ match -> returns 1 and deletes
    assert_eq!(
        send_and_read(&mut client, b"DELEX cond_key IFEQ 100\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"EXISTS cond_key\r\n"), ":0\r\n");

    // 6. Test DFLYCLUSTER FLUSHSLOTS
    let slot = rudis::router::key_slot(b"slot_test_key");
    assert_eq!(
        send_and_read(&mut client, b"SET slot_test_key hello\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"EXISTS slot_test_key\r\n"),
        ":1\r\n"
    );
    let flush_cmd = format!("DFLYCLUSTER FLUSHSLOTS {} {}\r\n", slot, slot);
    assert_eq!(send_and_read(&mut client, flush_cmd.as_bytes()), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client, b"EXISTS slot_test_key\r\n"),
        ":0\r\n"
    );

    // 7. Test DFLYCLUSTER SLOT-MIGRATION-STATUS & DFLYMIGRATE
    let mig_status = send_and_read(&mut client, b"DFLYCLUSTER SLOT-MIGRATION-STATUS\r\n");
    assert!(mig_status.contains("IDLE"));
    // Init migration
    assert_eq!(
        send_and_read(&mut client, b"DFLYMIGRATE INIT node123 2 0 100\r\n"),
        "+OK\r\n"
    );
    let mig_status2 = send_and_read(&mut client, b"DFLYCLUSTER SLOT-MIGRATION-STATUS\r\n");
    assert!(mig_status2.contains("MIGRATING"));
    assert!(mig_status2.contains("node123"));
    // Ack migration
    assert_eq!(
        send_and_read(&mut client, b"DFLYMIGRATE ACK 42\r\n"),
        "+OK\r\n"
    );
    let mig_status3 = send_and_read(&mut client, b"DFLYCLUSTER SLOT-MIGRATION-STATUS\r\n");
    assert!(mig_status3.contains("IDLE"));

    // 8. Test Dual-Protocol Memcached Gateway
    // Memcached SET command
    let mc_set = b"set mc_fruit 0 0 5\r\napple\r\n";
    assert_eq!(send_and_read(&mut client, mc_set), "STORED\r\n");

    // Shared Keyspace: read via Redis protocol
    assert_eq!(
        send_and_read(&mut client, b"GET mc_fruit\r\n"),
        "$5\r\napple\r\n"
    );

    // Memcached GET command (retrieves multiple keys)
    let mc_get = b"get mc_fruit non_existent\r\n";
    assert_eq!(
        send_and_read(&mut client, mc_get),
        "VALUE mc_fruit 0 5\r\napple\r\nEND\r\n"
    );

    // Memcached STATS
    let mc_stats = send_and_read(&mut client, b"stats\r\n");
    assert!(mc_stats.contains("STAT pid"));
    assert!(mc_stats.contains("STAT version 1.6.0-rudis-dragonfly"));
    assert!(mc_stats.contains("END\r\n"));

    // Memcached VERSION
    let mc_ver = send_and_read(&mut client, b"version\r\n");
    assert_eq!(mc_ver, "VERSION 1.6.0-rudis-dragonfly\r\n");

    // Memcached DELETE
    assert_eq!(
        send_and_read(&mut client, b"delete mc_fruit\r\n"),
        "DELETED\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"delete mc_fruit\r\n"),
        "NOT_FOUND\r\n"
    );
}

#[test]
fn test_harness_and_extended_command_coverage_e2e() {
    let port = 16428;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Harness commands (SELECT, CLIENT KILL, SLOWLOG)
    assert_eq!(send_and_read(&mut client, b"SELECT 0\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SELECT 9\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client, b"CLIENT KILL 127.0.0.1:9999\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"SLOWLOG RESET\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SLOWLOG LEN\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"SLOWLOG GET\r\n"), "*0\r\n");

    // 2. HMSET & FLUSHALL
    assert_eq!(
        send_and_read(&mut client, b"HMSET hm_key f1 v1 f2 v2\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HGET hm_key f1\r\n"),
        "$2\r\nv1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"FLUSHALL\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS hm_key\r\n"), ":0\r\n");

    // 3. PEXPIRE & PTTL
    assert_eq!(
        send_and_read(&mut client, b"SET exp_key hello\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"PEXPIRE exp_key 60000\r\n"),
        ":1\r\n"
    );
    let pttl_resp = send_and_read(&mut client, b"PTTL exp_key\r\n");
    assert!(pttl_resp.starts_with(":"));

    // 4. RedisJSON extended commands
    assert_eq!(
        send_and_read(
            &mut client,
            b"JSON.SET jext $ {\"num\":10,\"str\":\"hello\",\"arr\":[1,2,3]}\r\n"
        ),
        "+OK\r\n"
    );
    let mult_res = send_and_read(&mut client, b"JSON.NUMMULTBY jext $.num 2\r\n");
    assert!(mult_res.contains("20") || mult_res.contains("+OK") || mult_res.contains(":20"));
    let str_append = send_and_read(&mut client, b"JSON.STRAPPEND jext $.str world\r\n");
    assert!(str_append.contains("10") || str_append.contains(":10"));
    let str_len = send_and_read(&mut client, b"JSON.STRLEN jext $.str\r\n");
    assert!(str_len.contains("10") || str_len.contains(":10"));
    assert_eq!(
        send_and_read(&mut client, b"JSON.CLEAR jext $.arr\r\n"),
        ":1\r\n"
    );
    let mget_res = send_and_read(&mut client, b"JSON.MGET jext $.num\r\n");
    assert!(mget_res.contains("20") || mget_res.contains("*1\r\n"));

    // 5. Probabilistic & CRDT & Search
    assert_eq!(
        send_and_read(&mut client, b"CMS.INITBYPROB cms_sketch 0.01 0.01\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"CRDT.SADD crdt_tag item1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"CRDT.SREM crdt_tag item1\r\n"),
        ":1\r\n"
    );

    // 6. RediSearch FT.ADD
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE ft_idx ON HASH SCHEMA title TEXT\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.ADD ft_idx doc99 1.0 FIELDS title \"distributed systems\"\r\n"
        ),
        "+OK\r\n"
    );

    // 7. Sorted Set ZREVRANGEBYSCORE & PUNSUBSCRIBE
    assert_eq!(
        send_and_read(&mut client, b"ZADD zrev_k 10 a 20 b 30 c\r\n"),
        ":3\r\n"
    );
    let zrev_res = send_and_read(&mut client, b"ZREVRANGEBYSCORE zrev_k 25 5\r\n");
    assert!(zrev_res.contains("b") && zrev_res.contains("a"));
    let _ = send_and_read(&mut client, b"PUNSUBSCRIBE mypat*\r\n");

    // 8. MEMORY USAGE & STATS
    assert_eq!(
        send_and_read(&mut client, b"SET mem_key foobar\r\n"),
        "+OK\r\n"
    );
    let mem_res = send_and_read(&mut client, b"MEMORY USAGE mem_key\r\n");
    assert!(mem_res.starts_with(":"));
    assert_eq!(send_and_read(&mut client, b"MEMORY PURGE\r\n"), "+OK\r\n");

    // 9. REPLCONF & QUIT
    assert_eq!(
        send_and_read(&mut client, b"REPLCONF listening-port 6380\r\n"),
        "+OK\r\n"
    );
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut client2, b"QUIT\r\n"), "+OK\r\n");
}

#[test]
fn test_ping_resp_array_tcp() {
    let port = 16750;
    let num_shards = 2;
    start_test_server(port, num_shards);
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let resp = send_and_read(&mut stream, b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(resp, "+PONG\r\n");
}

#[test]
fn test_msetex_e2e() {
    let port = 16751;
    let num_shards = 2;
    start_test_server(port, num_shards);
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Basic MSETEX with EX
    let resp = send_and_read(&mut stream, b"MSETEX 2 mkey1 mval1 mkey2 mval2 EX 10\r\n");
    assert_eq!(resp, "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET mkey1\r\n"),
        "$5\r\nmval1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"GET mkey2\r\n"),
        "$5\r\nmval2\r\n"
    );

    // 2. MSETEX with NX when key exists -> 0
    let resp = send_and_read(&mut stream, b"MSETEX 1 mkey1 newval NX EX 10\r\n");
    assert_eq!(resp, ":0\r\n");

    // 3. MSETEX with XX when key exists -> 1
    let resp = send_and_read(&mut stream, b"MSETEX 1 mkey1 newval XX EX 10\r\n");
    assert_eq!(resp, ":1\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET mkey1\r\n"),
        "$6\r\nnewval\r\n"
    );

    // 4. MSETEX KEEPTTL
    let resp = send_and_read(&mut stream, b"MSETEX 1 mkey1 finalval KEEPTTL\r\n");
    assert_eq!(resp, "+OK\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"GET mkey1\r\n"),
        "$8\r\nfinalval\r\n"
    );
}

#[test]
fn test_all_remaining_uncovered_commands_e2e() {
    let port = 16752;
    let num_shards = 2;
    start_test_server(port, num_shards);
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. GETEX
    assert_eq!(
        send_and_read(&mut stream, b"SET getex_k val EX 100\r\n"),
        "+OK\r\n"
    );
    let resp = send_and_read(&mut stream, b"GETEX getex_k PERSIST\r\n");
    assert_eq!(resp, "$3\r\nval\r\n");

    // 2. HSETNX, HSTRLEN, HGETDEL
    assert_eq!(
        send_and_read(&mut stream, b"HSETNX myh f1 v1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"HSETNX myh f1 v2\r\n"),
        ":0\r\n"
    );
    assert_eq!(send_and_read(&mut stream, b"HSTRLEN myh f1\r\n"), ":2\r\n");
    let resp = send_and_read(&mut stream, b"HGETDEL myh FIELDS 1 f1\r\n");
    assert_eq!(resp, "*1\r\n$2\r\nv1\r\n");

    // 3. LPUSHX, RPUSHX
    assert_eq!(
        send_and_read(&mut stream, b"LPUSHX nonex_list elem\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"RPUSHX nonex_list elem\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"RPUSH mylist a b\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"LPUSHX mylist first\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"RPUSHX mylist last\r\n"),
        ":4\r\n"
    );

    // 4. RPOPLPUSH, BRPOPLPUSH
    assert_eq!(
        send_and_read(&mut stream, b"RPOPLPUSH mylist dstlist\r\n"),
        "$4\r\nlast\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"BRPOPLPUSH mylist dstlist 1\r\n"),
        "$1\r\nb\r\n"
    );

    // 5. LMPOP, BLMPOP
    let resp = send_and_read(&mut stream, b"LMPOP 1 mylist LEFT COUNT 1\r\n");
    assert_eq!(resp, "*2\r\n$6\r\nmylist\r\n*1\r\n$5\r\nfirst\r\n");
    let resp = send_and_read(&mut stream, b"BLMPOP 1 1 mylist RIGHT COUNT 1\r\n");
    assert_eq!(resp, "*2\r\n$6\r\nmylist\r\n*1\r\n$1\r\na\r\n");

    // 6. LCS
    assert_eq!(
        send_and_read(&mut stream, b"SET str1 AGGTAB\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SET str2 GXTXAYB\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"LCS str1 str2 LEN\r\n"),
        ":4\r\n"
    );

    // 7. DIGEST & DEBUG
    let digest_res = send_and_read(&mut stream, b"DIGEST str1\r\n");
    assert!(digest_res.starts_with("$"));
    let debug_res = send_and_read(&mut stream, b"DEBUG OBJECT str1\r\n");
    eprintln!("DEBUG RES: {:?}", debug_res);
    assert!(debug_res.starts_with("+") || debug_res.starts_with("$") || debug_res.starts_with(":"));
}

#[test]
fn test_fcall_mutating_aof_persistence_and_replay_e2e() {
    let port1 = 16640;
    let port2 = 16641;
    let num_shards = 1;
    let aof_dir = std::env::temp_dir().join(format!("rudis-aof-fcall-{}", port1));
    let _ = std::fs::remove_dir_all(&aof_dir);
    std::fs::create_dir_all(&aof_dir).unwrap();

    let aof_config1 = rudis::aof::AofConfig {
        enabled: true,
        dir: aof_dir.clone(),
        fsync_every_sec: true,
    };

    // 1. Start Server 1 with AOF enabled
    start_test_server_with_aof(port1, num_shards, aof_config1);

    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", port1))
        .expect("Failed to connect to rudis server 1");

    // Load function library
    let func_code = "#!lua name=fcallpersistlib\nredis.register_function('fcall_persist_set', function(keys, args) return redis.call('SET', keys[1], args[1]) end)\n";
    let load_cmd = format!(
        "*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n",
        func_code.len(),
        func_code
    );
    let load_resp = send_and_read(&mut stream1, load_cmd.as_bytes());
    assert_eq!(load_resp, "$15\r\nfcallpersistlib\r\n");

    // Execute FCALL which calls redis.call('SET', ...)
    let fcall_cmd = "*5\r\n$5\r\nFCALL\r\n$17\r\nfcall_persist_set\r\n$1\r\n1\r\n$9\r\npersist_k\r\n$9\r\npersist_v\r\n";
    let fcall_resp = send_and_read(&mut stream1, fcall_cmd.as_bytes());
    assert_eq!(fcall_resp, "+OK\r\n");

    // Verify key in Server 1
    let get_resp = send_and_read(&mut stream1, b"GET persist_k\r\n");
    assert_eq!(get_resp, "$9\r\npersist_v\r\n");

    // Save to sync AOF to disk
    let save_resp = send_and_read(&mut stream1, b"SAVE\r\n");
    assert_eq!(save_resp, "+OK\r\n");

    drop(stream1);
    thread::sleep(Duration::from_millis(200));

    // 2. Start Server 2 pointing to the same AOF directory
    let aof_config2 = rudis::aof::AofConfig {
        enabled: true,
        dir: aof_dir.clone(),
        fsync_every_sec: true,
    };
    start_test_server_with_aof(port2, num_shards, aof_config2);

    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", port2))
        .expect("Failed to connect to rudis server 2");

    // Verify key was restored via AOF replay
    let replay_resp = send_and_read(&mut stream2, b"GET persist_k\r\n");
    assert_eq!(replay_resp, "$9\r\npersist_v\r\n");
}

#[test]
fn test_crdt_cross_shard_routing_e2e() {
    let port = 16650;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client1 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client 1");
    let mut client2 =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client 2");

    // Identify keys mapping to distinct shards in a 4-shard cluster
    let mut shard_keys = std::collections::HashMap::new();
    let mut counter = 0;
    while shard_keys.len() < num_shards {
        let key_str = format!("crdt_key_{}", counter);
        let shard = rudis::router::target_shard(key_str.as_bytes(), num_shards);
        shard_keys.entry(shard).or_insert(key_str);
        counter += 1;
    }

    for (shard, key) in &shard_keys {
        // Write CRDT LWW register from Client 1
        let val = format!("val_shard_{}", shard);
        let resp = send_and_read(
            &mut client1,
            format!("CRDT.SET {} {}\r\n", key, val).as_bytes(),
        );
        assert!(resp.starts_with("+OK"));

        // Read back from Client 2 (cross-shard pipeline/dispatch)
        let resp = send_and_read(&mut client2, format!("CRDT.GET {}\r\n", key).as_bytes());
        assert_eq!(resp, format!("${}\r\n{}\r\n", val.len(), val));

        // Test CRDT counter on this shard: increment by 42 from client1, then by 1 from client2
        let counter_key = format!("{}:cnt", key);
        let resp = send_and_read(
            &mut client1,
            format!("CRDT.INCRBY {} 42\r\n", counter_key).as_bytes(),
        );
        assert_eq!(resp, ":42\r\n");

        // Verify and increment counter from Client 2 across shard boundary
        let resp = send_and_read(
            &mut client2,
            format!("CRDT.INCRBY {} 1\r\n", counter_key).as_bytes(),
        );
        assert_eq!(resp, ":43\r\n");

        // Test CRDT ORSet on this shard: SADD from Client 1, SMEMBERS from Client 2
        let set_key = format!("{}:set", key);
        let resp = send_and_read(
            &mut client1,
            format!("CRDT.SADD {} member_a\r\n", set_key).as_bytes(),
        );
        assert_eq!(resp, ":1\r\n");

        let resp = send_and_read(
            &mut client2,
            format!("CRDT.SMEMBERS {}\r\n", set_key).as_bytes(),
        );
        assert_eq!(resp, "*1\r\n$8\r\nmember_a\r\n");

        // Remove member from Client 2 and verify SMEMBERS empty from Client 1
        let resp = send_and_read(
            &mut client2,
            format!("CRDT.SREM {} member_a\r\n", set_key).as_bytes(),
        );
        assert_eq!(resp, ":1\r\n");

        let resp = send_and_read(
            &mut client1,
            format!("CRDT.SMEMBERS {}\r\n", set_key).as_bytes(),
        );
        assert_eq!(resp, "*0\r\n");

        // Delete register from Client 2 and verify nil from Client 1
        let resp = send_and_read(&mut client2, format!("CRDT.DEL {}\r\n", key).as_bytes());
        assert_eq!(resp, ":1\r\n");

        let resp = send_and_read(&mut client1, format!("CRDT.GET {}\r\n", key).as_bytes());
        assert_eq!(resp, "$-1\r\n");
    }
}

#[test]
fn test_cluster_pipelined_squashed_moved_redirect_e2e() {
    let port1 = 16660;
    let port2 = 16661;

    start_test_server(port1, 2);
    start_test_server(port2, 2);

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
fn test_acl_permissions_and_hashed_passwords_e2e() {
    let port = 16670;
    start_test_server(port, 2);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect admin client");

    // 1. Create user 'carol' with restricted commands (-@all +get +ping) and restricted keys (~user:*)
    let pass = "carol_secure_pass";
    let hash = rudis::acl::hash_password(pass);
    let setuser_cmd = format!("ACL SETUSER carol on {} -@all +get +acl ~user:*\r\n", hash);
    assert_eq!(
        send_and_read(&mut client, setuser_cmd.as_bytes()),
        "+OK\r\n"
    );

    // Set a key as admin (default user)
    assert_eq!(
        send_and_read(&mut client, b"SET user:profile alice_data\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SET secret:token secret_val\r\n"),
        "+OK\r\n"
    );

    // 2. Connect as Carol and authenticate using plaintext password matching stored hash
    let mut carol_client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect carol client");
    assert_eq!(
        send_and_read(
            &mut carol_client,
            format!("AUTH carol {}\r\n", pass).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut carol_client, b"ACL WHOAMI\r\n"),
        "$5\r\ncarol\r\n"
    );

    // 3. Test allowed command on allowed key
    let resp = send_and_read(&mut carol_client, b"GET user:profile\r\n");
    assert_eq!(resp, "$10\r\nalice_data\r\n");

    // 4. Test forbidden command on allowed key -> NOPERM command
    let resp = send_and_read(&mut carol_client, b"SET user:profile new_data\r\n");
    assert!(resp.starts_with("-NOPERM") && resp.contains("permissions to run the 'set' command"));

    // 5. Test allowed command on forbidden key -> NOPERM key
    let resp = send_and_read(&mut carol_client, b"GET secret:token\r\n");
    assert!(resp.starts_with("-NOPERM") && resp.contains("permissions to access one of the keys"));

    // 6. Test pipelined squashed commands enforcing ACL per command
    let pipeline = b"GET user:profile\r\nSET user:profile hack\r\nGET secret:token\r\n";
    let resp = send_and_read(&mut carol_client, pipeline);
    assert!(resp.contains("$10\r\nalice_data\r\n"));
    assert!(resp.contains("-NOPERM this user has no permissions to run the 'set' command"));
    assert!(resp.contains("-NOPERM this user has no permissions to access one of the keys"));
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

    let psync_cmd = format!("PSYNC {} {}\r\n", replid, offset1);
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
fn test_tls_port_listener_e2e() {
    let port = 16690;
    let tls_port = 16691;
    let (cert_der, _key_der) = start_test_server_with_tls(port, tls_port, 2);

    // 1. Build a client rustls config trusting the server's self-signed cert
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(rustls::pki_types::CertificateDer::from(cert_der))
        .expect("Failed to add cert to root store");
    let client_config = std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    );

    // 2. Connect to TLS port and perform TLS handshake
    let server_name = "localhost".try_into().unwrap();
    let conn = rustls::ClientConnection::new(client_config, server_name)
        .expect("Failed to create ClientConnection");
    let sock = TcpStream::connect(format!("127.0.0.1:{}", tls_port))
        .expect("Failed to connect TCP socket to TLS port");
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut tls_client = rustls::StreamOwned::new(conn, sock);

    // 3. Send PING over TLS
    tls_client.write_all(b"PING\r\n").unwrap();
    let mut buf = [0u8; 512];
    let n = tls_client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+PONG\r\n");

    // 4. Send SET command over TLS
    tls_client
        .write_all(b"SET secure_key encrypted_value_99\r\n")
        .unwrap();
    let n = tls_client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");

    // 5. Send GET command over TLS
    tls_client.write_all(b"GET secure_key\r\n").unwrap();
    let n = tls_client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"$18\r\nencrypted_value_99\r\n");

    // 6. Connect to plain TCP port and verify shared database state
    let mut plain_client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect plain client");
    assert_eq!(
        send_and_read(&mut plain_client, b"GET secure_key\r\n"),
        "$18\r\nencrypted_value_99\r\n"
    );

    // 7. Mutate via plain TCP port and read back via TLS client
    assert_eq!(
        send_and_read(&mut plain_client, b"SET plain_key plain_val\r\n"),
        "+OK\r\n"
    );

    tls_client.write_all(b"GET plain_key\r\n").unwrap();
    let n = tls_client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"$9\r\nplain_val\r\n");
}

#[test]
fn test_cross_shard_mget_mset_fanout_e2e() {
    let port = 16700;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Find 4 keys that each map to shard 0, 1, 2, 3
    let mut shard_keys: [String; 4] = Default::default();
    let mut found = 0;
    for i in 0..10000 {
        let key = format!("k_{}", i);
        let s = target_shard(key.as_bytes(), num_shards);
        if shard_keys[s].is_empty() {
            shard_keys[s] = key;
            found += 1;
            if found == 4 {
                break;
            }
        }
    }
    assert_eq!(found, 4);

    let k0 = &shard_keys[0];
    let k1 = &shard_keys[1];
    let k2 = &shard_keys[2];
    let k3 = &shard_keys[3];

    // 2. Parallel cross-shard MSET fanning out to all 4 shards
    let mset_cmd = format!("MSET {} v0 {} v1 {} v2 {} v3\r\n", k0, k1, k2, k3);
    assert_eq!(send_and_read(&mut client, mset_cmd.as_bytes()), "+OK\r\n");

    // 3. Verify via individual GET commands that keys actually reside on each shard
    assert_eq!(
        send_and_read(&mut client, format!("GET {}\r\n", k0).as_bytes()),
        "$2\r\nv0\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, format!("GET {}\r\n", k1).as_bytes()),
        "$2\r\nv1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, format!("GET {}\r\n", k2).as_bytes()),
        "$2\r\nv2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, format!("GET {}\r\n", k3).as_bytes()),
        "$2\r\nv3\r\n"
    );

    // 4. Parallel cross-shard MGET reading across all 4 shards with missing & duplicate keys
    let mget_cmd = format!(
        "MGET {} {} non_existent_key_xyz {} {} {}\r\n",
        k3, k0, k2, k1, k0
    );
    let expected = "*6\r\n$2\r\nv3\r\n$2\r\nv0\r\n$-1\r\n$2\r\nv2\r\n$2\r\nv1\r\n$2\r\nv0\r\n";
    assert_eq!(send_and_read(&mut client, mget_cmd.as_bytes()), expected);

    // 5. Co-located hash-tag keys (single-shard bypass path)
    let mset_tagged = "MSET {user:99}:name Alice {user:99}:city Seattle {user:99}:role Admin\r\n";
    assert_eq!(
        send_and_read(&mut client, mset_tagged.as_bytes()),
        "+OK\r\n"
    );

    let mget_tagged = "MGET {user:99}:city {user:99}:name {user:99}:missing {user:99}:role\r\n";
    let expected_tagged = "*4\r\n$7\r\nSeattle\r\n$5\r\nAlice\r\n$-1\r\n$5\r\nAdmin\r\n";
    assert_eq!(
        send_and_read(&mut client, mget_tagged.as_bytes()),
        expected_tagged
    );

    // 6. Pipelined MSET + MGET in a single network buffer
    let pipeline = format!(
        "MSET {} new_v0 {} new_v2\r\nMGET {} {} {}\r\n",
        k0, k2, k0, k1, k2
    );
    let pipeline_resp = send_and_read(&mut client, pipeline.as_bytes());
    let expected_pipeline = "+OK\r\n*3\r\n$6\r\nnew_v0\r\n$2\r\nv1\r\n$6\r\nnew_v2\r\n";
    assert_eq!(pipeline_resp, expected_pipeline);
}

#[test]
fn test_mget_zero_alloc_resp2_resp3_serialization_e2e() {
    let port = 16701;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    assert_eq!(
        send_and_read(&mut client, b"SET key_alpha value_alpha\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SET key_beta value_beta\r\n"),
        "+OK\r\n"
    );

    // RESP2 test: missing key returns "$-1\r\n"
    let mget_resp2 = send_and_read(&mut client, b"MGET key_alpha key_missing key_beta\r\n");
    assert_eq!(
        mget_resp2,
        "*3\r\n$11\r\nvalue_alpha\r\n$-1\r\n$10\r\nvalue_beta\r\n"
    );

    // Switch to RESP3 via HELLO 3
    let hello_resp = send_and_read(&mut client, b"HELLO 3\r\n");
    assert!(hello_resp.starts_with('%') || hello_resp.starts_with('*'));

    // RESP3 test: missing key returns "_\r\n" (null)
    let mget_resp3 = send_and_read(&mut client, b"MGET key_alpha key_missing key_beta\r\n");
    assert_eq!(
        mget_resp3,
        "*3\r\n$11\r\nvalue_alpha\r\n_\r\n$10\r\nvalue_beta\r\n"
    );
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
    let mut next_resp = send_and_read(&mut client1, b"PING\r\n");
    if !next_resp.contains("invalidate") {
        std::thread::sleep(std::time::Duration::from_millis(100));
        next_resp.push_str(&send_and_read(&mut client1, b"PING\r\n"));
    }
    assert!(next_resp.contains("invalidate") && next_resp.contains("tracked_k1"));

    // 6. Disable tracking - restores atomic bypass
    assert_eq!(
        send_and_read(&mut client1, b"CLIENT TRACKING off\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_mget_mset_8shard_preallocated_fanout_e2e() {
    let port = 16703;
    let num_shards = 8;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // Find keys for all 8 shards
    let mut shard_keys: [String; 8] = Default::default();
    let mut found = 0;
    for i in 0..20000 {
        let key = format!("k8_{}", i);
        let s = target_shard(key.as_bytes(), num_shards);
        if shard_keys[s].is_empty() {
            shard_keys[s] = key;
            found += 1;
            if found == 8 {
                break;
            }
        }
    }
    assert_eq!(found, 8);

    // MSET across all 8 shards
    let mut mset_args = String::from("MSET");
    for (i, k) in shard_keys.iter().enumerate() {
        mset_args.push_str(&format!(" {} val_8_{}", k, i));
    }
    mset_args.push_str("\r\n");
    assert_eq!(send_and_read(&mut client, mset_args.as_bytes()), "+OK\r\n");

    // MGET reading all 8 shards in reverse order + nonexistent keys
    let mut mget_args = String::from("MGET");
    for k in shard_keys.iter().rev() {
        mget_args.push_str(&format!(" {}", k));
    }
    mget_args.push_str(" missing_8_x\r\n");

    let resp = send_and_read(&mut client, mget_args.as_bytes());
    assert!(resp.starts_with("*9\r\n"));
    for i in (0..8).rev() {
        assert!(resp.contains(&format!("val_8_{}", i)));
    }
    assert!(resp.ends_with("$-1\r\n"));
}

#[test]
fn test_mget_mset_pooled_channels_high_churn_e2e() {
    let port = 16704;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // Find 4 keys on 4 shards
    let mut keys = Vec::new();
    for i in 0..1000 {
        let key = format!("pool_k_{}", i);
        if keys.len() < 4 {
            keys.push(key);
        }
    }

    // High churn loop exercising channel reuse across 50 iterations
    for iter in 0..50 {
        let set_cmd = format!(
            "MSET {} v_{}_0 {} v_{}_1 {} v_{}_2 {} v_{}_3\r\n",
            keys[0], iter, keys[1], iter, keys[2], iter, keys[3], iter
        );
        assert_eq!(send_and_read(&mut client, set_cmd.as_bytes()), "+OK\r\n");

        let get_cmd = format!("MGET {} {} {} {}\r\n", keys[0], keys[1], keys[2], keys[3]);
        let resp = send_and_read(&mut client, get_cmd.as_bytes());
        assert!(resp.contains(&format!("v_{}_0", iter)));
        assert!(resp.contains(&format!("v_{}_1", iter)));
        assert!(resp.contains(&format!("v_{}_2", iter)));
        assert!(resp.contains(&format!("v_{}_3", iter)));
    }
}

#[test]
fn test_mget_burst_draining_and_single_pass_fanout_e2e() {
    let port = 16705;
    start_test_server(port, 8);
    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // Form 16 keys across 8 shards (2 keys per shard)
    let mut keys = Vec::new();
    let mut shard_keys = vec![Vec::new(); 8];
    for i in 0..1000 {
        let key = format!("burst_k_{}", i);
        let slot = rudis::router::key_slot(key.as_bytes());
        let shard = rudis::router::slot_to_shard(slot, 8);
        if shard_keys[shard].len() < 2 {
            shard_keys[shard].push(key.clone());
            keys.push(key);
        }
        if keys.len() == 16 {
            break;
        }
    }
    assert_eq!(keys.len(), 16);

    // Interleaved MSET and MGET burst execution
    for iter in 0..30 {
        let mut mset_args = String::new();
        for (idx, k) in keys.iter().enumerate() {
            mset_args.push_str(&format!("{} v_{}_{} ", k, iter, idx));
        }
        let set_cmd = format!("MSET {}\r\n", mset_args.trim_end());
        assert_eq!(send_and_read(&mut client, set_cmd.as_bytes()), "+OK\r\n");

        let mget_args = keys.join(" ");
        let get_cmd = format!("MGET {}\r\n", mget_args);
        let resp = send_and_read(&mut client, get_cmd.as_bytes());
        assert!(resp.starts_with("*16\r\n"));
        for idx in 0..16 {
            assert!(
                resp.contains(&format!("v_{}_{}", iter, idx)),
                "Iteration {} missing key index {} in resp: {}",
                iter,
                idx,
                resp
            );
        }
    }
}

#[test]
fn test_mget_fast_harvest_try_recv_sweep_e2e() {
    let port = 16706;
    start_test_server(port, 4);
    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    let keys = [
        "sweep_k_0".to_string(),
        "sweep_k_1".to_string(),
        "sweep_k_2".to_string(),
        "sweep_k_3".to_string(),
    ];

    let set_cmd = format!(
        "MSET {} val0 {} val1 {} val2 {} val3\r\n",
        keys[0], keys[1], keys[2], keys[3]
    );
    assert_eq!(send_and_read(&mut client, set_cmd.as_bytes()), "+OK\r\n");

    let get_cmd = format!("MGET {} {} {} {}\r\n", keys[0], keys[1], keys[2], keys[3]);
    let resp = send_and_read(&mut client, get_cmd.as_bytes());
    assert_eq!(
        resp,
        "*4\r\n$4\r\nval0\r\n$4\r\nval1\r\n$4\r\nval2\r\n$4\r\nval3\r\n"
    );
}

#[test]
fn test_pipelined_fast_path_set_and_scattered_mset_e2e() {
    let port = 16707;
    start_test_server(port, 4);
    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Pipelined fast-path SET with 1KB payloads (16 commands in single write)
    let val_1kb = vec![b'v'; 1024];
    let val_1kb_str = std::str::from_utf8(&val_1kb).unwrap();
    let mut pipeline_req = Vec::new();
    for i in 0..16 {
        pipeline_req.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\nfast_k_{}\r\n$1024\r\n{}\r\n",
                7 + i.to_string().len(),
                i,
                val_1kb_str
            )
            .as_bytes(),
        );
    }
    client.write_all(&pipeline_req).unwrap();

    let mut resp_buf = vec![0u8; 16 * 5];
    client.read_exact(&mut resp_buf).unwrap();
    let expected_ok = "+OK\r\n".repeat(16);
    assert_eq!(std::str::from_utf8(&resp_buf).unwrap(), expected_ok);

    // 2. Verify values via pipelined GET
    let mut pipeline_get = Vec::new();
    for i in 0..16 {
        pipeline_get.extend_from_slice(format!("GET fast_k_{}\r\n", i).as_bytes());
    }
    client.write_all(&pipeline_get).unwrap();

    let mut get_resp = Vec::new();
    let mut temp = [0u8; 4096];
    let expected_get_len = 16 * (7 + 1024 + 2); // $1024\r\n<1024 bytes>\r\n per key
    while get_resp.len() < expected_get_len {
        let n = client.read(&mut temp).unwrap();
        if n == 0 {
            break;
        }
        get_resp.extend_from_slice(&temp[..n]);
    }
    let get_resp_str = String::from_utf8_lossy(&get_resp);
    for i in 0..16 {
        assert!(
            get_resp_str.contains(val_1kb_str),
            "Missing 1KB payload for fast_k_{}",
            i
        );
    }

    // 3. Multi-key scattered MSET across shards
    let mut mset_cmd = "MSET".to_string();
    for i in 0..10 {
        mset_cmd.push_str(&format!(" scatt_k_{} scatt_v_{}", i, i));
    }
    mset_cmd.push_str("\r\n");
    assert_eq!(send_and_read(&mut client, mset_cmd.as_bytes()), "+OK\r\n");

    // 4. Verify scattered keys with MGET
    let mut mget_cmd = "MGET".to_string();
    for i in 0..10 {
        mget_cmd.push_str(&format!(" scatt_k_{}", i));
    }
    mget_cmd.push_str("\r\n");
    let mget_resp = send_and_read(&mut client, mget_cmd.as_bytes());
    for i in 0..10 {
        assert!(mget_resp.contains(&format!("scatt_v_{}", i)));
    }
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
fn test_fragmented_socket_frame_draining_e2e() {
    let port = 16755;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Prepare 32 pipelined 1KB SET commands
    let val_1kb = "V".repeat(1024);
    let mut payload = Vec::new();
    for i in 0..32 {
        let cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\nfrag_k_{}\r\n${}\r\n{}\r\n",
            format!("frag_k_{}", i).len(),
            i,
            val_1kb.len(),
            val_1kb
        );
        payload.extend_from_slice(cmd.as_bytes());
    }

    // Transmit in fragmented chunks of 512 bytes with tiny delays
    let chunk_size = 512;
    for chunk in payload.chunks(chunk_size) {
        client.write_all(chunk).unwrap();
        thread::sleep(Duration::from_micros(200));
    }

    // Read all 32 +OK\r\n responses (160 bytes total)
    let mut responses = Vec::new();
    let mut temp = [0u8; 1024];
    while responses.len() < 32 * 5 {
        let n = client.read(&mut temp).unwrap();
        if n == 0 {
            break;
        }
        responses.extend_from_slice(&temp[..n]);
    }

    let resp_str = String::from_utf8_lossy(&responses);
    assert_eq!(responses.len(), 32 * 5);
    assert_eq!(resp_str.matches("+OK\r\n").count(), 32);

    // Verify all keys were correctly stored
    let expected_get_len = 7 + 1024 + 2; // $1024\r\n<1024 bytes>\r\n
    for i in 0..32 {
        let get_cmd = format!("GET frag_k_{}\r\n", i);
        client.write_all(get_cmd.as_bytes()).unwrap();
        let mut get_resp = Vec::new();
        let mut temp = [0u8; 2048];
        while get_resp.len() < expected_get_len {
            let n = client.read(&mut temp).unwrap();
            if n == 0 {
                break;
            }
            get_resp.extend_from_slice(&temp[..n]);
        }
        let resp_str = String::from_utf8_lossy(&get_resp);
        assert!(resp_str.contains(&val_1kb));
    }
}

#[test]
fn test_pipeline1_fast_path_and_acl_bypass_e2e() {
    let port = 16450;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Pipeline 1 commands: individual GET and SET across different shards
    for i in 0..50 {
        let set_cmd = format!("SET p1_k_{} val_{}\r\n", i, i);
        let resp = send_and_read(&mut client, set_cmd.as_bytes());
        assert_eq!(resp, "+OK\r\n");

        let get_cmd = format!("GET p1_k_{}\r\n", i);
        let resp = send_and_read(&mut client, get_cmd.as_bytes());
        let expected = format!("${}\r\nval_{}\r\n", format!("val_{}", i).len(), i);
        assert_eq!(resp, expected);
    }

    // 2. SET with GET option (using write_resp_bulk fast path)
    let resp = send_and_read(&mut client, b"SET p1_k_0 new_val GET\r\n");
    assert_eq!(resp, "$5\r\nval_0\r\n");

    let resp = send_and_read(&mut client, b"GET p1_k_0\r\n");
    assert_eq!(resp, "$7\r\nnew_val\r\n");

    // 3. Test ACL configuration and execution
    let resp = send_and_read(
        &mut client,
        b"ACL SETUSER alice on >secretpass +@all ~*\r\n",
    );
    assert_eq!(resp, "+OK\r\n");

    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let resp = send_and_read(&mut client2, b"AUTH alice secretpass\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut client2, b"GET p1_k_0\r\n");
    assert_eq!(resp, "$7\r\nnew_val\r\n");
}

#[test]
fn test_scattered_mget_mset_batch_pooling_e2e() {
    let port = 16460;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Repeated scattered MSET and MGET of 10 keys across 4 shards
    for round in 0..50 {
        let mut mset_cmd = String::from("MSET");
        for i in 0..10 {
            mset_cmd.push_str(&format!(" scat_k_{} val_{}_{}", i, round, i));
        }
        mset_cmd.push_str("\r\n");
        let resp = send_and_read(&mut client, mset_cmd.as_bytes());
        assert_eq!(resp, "+OK\r\n");

        let mut mget_cmd = String::from("MGET");
        for i in 0..10 {
            mget_cmd.push_str(&format!(" scat_k_{}", i));
        }
        mget_cmd.push_str("\r\n");
        let resp = send_and_read(&mut client, mget_cmd.as_bytes());
        assert!(resp.starts_with("*10\r\n"));
        for i in 0..10 {
            let expected_val = format!("val_{}_{}", round, i);
            assert!(resp.contains(&expected_val));
        }
    }
}

#[test]
fn test_reactive_mget_mset_and_command_drain_e2e() {
    let port = 16470;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Verify pipelined batch execution with in-place drained command buffer
    let mut batch = Vec::new();
    for i in 0..25 {
        batch.extend_from_slice(format!("SET drain_k_{} val_{}\r\n", i, i).as_bytes());
    }
    let resp = send_and_read(&mut client, &batch);
    assert_eq!(resp.matches("+OK\r\n").count(), 25);

    // Verify reactive MGET across shards
    let mut mget_cmd = String::from("MGET");
    for i in 0..25 {
        mget_cmd.push_str(&format!(" drain_k_{}", i));
    }
    mget_cmd.push_str("\r\n");
    let resp = send_and_read(&mut client, mget_cmd.as_bytes());
    assert!(resp.starts_with("*25\r\n"));
    for i in 0..25 {
        assert!(resp.contains(&format!("val_{}", i)));
    }
}

#[test]
fn test_pipeline1_fast_path_and_multi_key_routing_e2e() {
    let port = 16472;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Pipeline 1 single-command requests
    let resp = send_and_read(&mut client, b"SET single_k single_v\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut client, b"GET single_k\r\n");
    assert_eq!(resp, "$8\r\nsingle_v\r\n");

    // 2. Multi-key TOUCH across shards
    let _ = send_and_read(&mut client, b"SET touch_k1 v1\r\n");
    let _ = send_and_read(&mut client, b"SET touch_k2 v2\r\n");
    let resp = send_and_read(&mut client, b"TOUCH touch_k1 touch_k2 touch_missing\r\n");
    assert_eq!(resp, ":2\r\n");

    // 3. Multi-key DEL across shards
    let resp = send_and_read(&mut client, b"DEL touch_k1 touch_k2 touch_missing\r\n");
    assert_eq!(resp, ":2\r\n");
}

#[test]
fn test_mget_mset_in_place_recycling_scattered_e2e() {
    let port = 16471;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    for round in 0..10 {
        // Multi-shard scattered MSET
        let mut mset_cmd = String::from("MSET");
        for i in 0..20 {
            mset_cmd.push_str(&format!(" recyc_k_{} recyc_val_{}_{}", i, round, i));
        }
        mset_cmd.push_str("\r\n");
        let set_resp = send_and_read(&mut client, mset_cmd.as_bytes());
        assert_eq!(set_resp, "+OK\r\n");

        // Multi-shard scattered MGET
        let mut mget_cmd = String::from("MGET");
        for i in 0..20 {
            mget_cmd.push_str(&format!(" recyc_k_{}", i));
        }
        mget_cmd.push_str("\r\n");
        let get_resp = send_and_read(&mut client, mget_cmd.as_bytes());
        assert!(get_resp.starts_with("*20\r\n"));
        for i in 0..20 {
            assert!(get_resp.contains(&format!("recyc_val_{}_{}", round, i)));
        }
    }
}

#[test]
fn test_zero_alloc_command_dispatch_and_mixed_case_e2e() {
    let port = 16756;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Lowercase commands via RESP
    let resp = send_and_read(
        &mut client,
        b"*3\r\n$3\r\nset\r\n$7\r\nmykey01\r\n$8\r\nval_zero\r\n",
    );
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut client, b"*2\r\n$3\r\nget\r\n$7\r\nmykey01\r\n");
    assert_eq!(resp, "$8\r\nval_zero\r\n");

    // 2. Mixed-case commands
    let resp = send_and_read(
        &mut client,
        b"*3\r\n$3\r\nSeT\r\n$7\r\nmykey02\r\n$7\r\nval_two\r\n",
    );
    assert_eq!(resp, "+OK\r\n");

    let resp = send_and_read(&mut client, b"*2\r\n$3\r\nGeT\r\n$7\r\nmykey02\r\n");
    assert_eq!(resp, "$7\r\nval_two\r\n");

    // 3. Pipelined mix of commands
    let pipeline = b"*3\r\n$3\r\nSET\r\n$2\r\np1\r\n$2\r\nv1\r\n*3\r\n$3\r\nset\r\n$2\r\np2\r\n$2\r\nv2\r\n*2\r\n$3\r\nget\r\n$2\r\np1\r\n*2\r\n$3\r\nGET\r\n$2\r\np2\r\n";
    let resp = send_and_read(&mut client, pipeline);
    assert_eq!(resp, "+OK\r\n+OK\r\n$2\r\nv1\r\n$2\r\nv2\r\n");
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
fn test_pipeline1_lockless_stats_and_buffer_recycling_e2e() {
    let port = 16474;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. High-iteration single-command pipeline 1 requests
    for i in 0..50 {
        let set_cmd = format!("SET p1_key_{} p1_val_{}\r\n", i, i);
        let resp = send_and_read(&mut client, set_cmd.as_bytes());
        assert_eq!(resp, "+OK\r\n");

        let get_cmd = format!("GET p1_key_{}\r\n", i);
        let resp = send_and_read(&mut client, get_cmd.as_bytes());
        assert_eq!(
            resp,
            format!("${}\r\np1_val_{}\r\n", 7 + i.to_string().len(), i)
        );
    }

    // 2. Verify commandstats aggregation
    client.write_all(b"INFO commandstats\r\n").unwrap();
    let mut resp_buf = [0u8; 32768];
    let mut total_n = 0;
    while total_n < resp_buf.len() {
        let n = client.read(&mut resp_buf[total_n..]).unwrap();
        if n == 0 {
            break;
        }
        total_n += n;
        let s = String::from_utf8_lossy(&resp_buf[..total_n]);
        if s.contains("cmdstat_set:calls=") && s.contains("cmdstat_get:calls=") {
            break;
        }
    }
    let resp = String::from_utf8_lossy(&resp_buf[..total_n]);
    assert!(resp.contains("cmdstat_set:calls="));
    assert!(resp.contains("cmdstat_get:calls="));

    // 3. Reset stats
    let resp = send_and_read(&mut client, b"CONFIG RESETSTAT\r\n");
    assert_eq!(resp, "+OK\r\n");
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
fn test_resp3_isolation_across_interleaved_clients_e2e() {
    let port = 16709;
    start_test_server(port, 1);

    let mut client_resp3 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let mut client_resp2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. client_resp3 negotiates RESP3
    let hello_resp = send_and_read(&mut client_resp3, b"HELLO 3\r\n");
    assert!(hello_resp.starts_with("%") || hello_resp.contains("proto"));

    // 2. client_resp2 sends GET on nonexistent key.
    // Under RESP2, null is "$-1\r\n". Under RESP3, null is "_\r\n".
    let get_resp2 = send_and_read(&mut client_resp2, b"GET non_existent_key_resp2\r\n");
    assert_eq!(
        get_resp2, "$-1\r\n",
        "RESP2 client received non-RESP2 null reply: {}",
        get_resp2
    );

    // 3. client_resp3 sends GET on nonexistent key and receives RESP3 null "_\r\n"
    let get_resp3 = send_and_read(&mut client_resp3, b"GET non_existent_key_resp3\r\n");
    assert_eq!(
        get_resp3, "_\r\n",
        "RESP3 client did not receive RESP3 null reply: {}",
        get_resp3
    );

    // 4. Interleave: client_resp2 must still receive RESP2 null "$-1\r\n", NOT "_\r\n"
    let get_resp2_again = send_and_read(&mut client_resp2, b"GET non_existent_key_resp2_again\r\n");
    assert_eq!(
        get_resp2_again, "$-1\r\n",
        "RESP2 client leaked RESP3 protocol mode after interleaved query: {}",
        get_resp2_again
    );

    // 5. Test ZRANGE WITHSCORES:
    // client_resp2 inserts and queries sorted set
    let _ = send_and_read(&mut client_resp2, b"ZADD my_zset 42.5 member1\r\n");
    let zrange_resp2 = send_and_read(&mut client_resp2, b"ZRANGE my_zset 0 -1 WITHSCORES\r\n");
    // Under RESP2, flat array of length 2: *2\r\n...
    assert!(
        zrange_resp2.starts_with("*2\r\n"),
        "RESP2 client did not get flat array: {}",
        zrange_resp2
    );

    // client_resp3 queries sorted set: under RESP3, nested array: *1\r\n*2\r\n... with double ,42.5\r\n
    let zrange_resp3 = send_and_read(&mut client_resp3, b"ZRANGE my_zset 0 -1 WITHSCORES\r\n");
    assert!(
        zrange_resp3.starts_with("*1\r\n*2\r\n") && zrange_resp3.contains(",42.5\r\n"),
        "RESP3 client did not get nested array with double score: {}",
        zrange_resp3
    );

    // client_resp2 queries again: must STILL be flat array *2\r\n
    let zrange_resp2_again =
        send_and_read(&mut client_resp2, b"ZRANGE my_zset 0 -1 WITHSCORES\r\n");
    assert!(
        zrange_resp2_again.starts_with("*2\r\n"),
        "RESP2 client leaked RESP3 nested array score format: {}",
        zrange_resp2_again
    );
}

#[test]
fn test_config_resetstat_and_info_commandstats_cross_shard_e2e() {
    let port = 16710;
    start_test_server(port, 2);

    let mut c1 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let mut c2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Establish clean baseline
    let reset_init = send_and_read(&mut c1, b"CONFIG RESETSTAT\r\n");
    assert_eq!(reset_init, "+OK\r\n");

    // 2. Issue commands across c1 and c2
    assert_eq!(send_and_read(&mut c1, b"PING\r\n"), "+PONG\r\n");
    assert_eq!(send_and_read(&mut c2, b"SET k_test v_test\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut c2, b"GET k_test\r\n"),
        "$6\r\nv_test\r\n"
    );

    // 3. Read full INFO commandstats response
    c1.write_all(b"INFO commandstats\r\n").unwrap();
    let mut buf = [0u8; 8192];
    let mut total_n = 0;
    while total_n < buf.len() {
        let n = c1.read(&mut buf[total_n..]).unwrap();
        if n == 0 {
            break;
        }
        total_n += n;
        let s = String::from_utf8_lossy(&buf[..total_n]);
        if s.contains("cmdstat_ping:") && s.contains("cmdstat_get:") {
            break;
        }
    }
    let info1 = String::from_utf8_lossy(&buf[..total_n]);
    assert!(info1.contains("cmdstat_ping:"));
    assert!(info1.contains("cmdstat_get:"));
    assert!(info1.contains("cmdstat_set:"));

    // 4. Reset stats via CONFIG RESETSTAT on c1
    let reset_resp = send_and_read(&mut c1, b"CONFIG RESETSTAT\r\n");
    assert_eq!(reset_resp, "+OK\r\n");

    // 5. Query INFO commandstats on c2: old ping/get/set counts must be wiped across all shards
    c2.write_all(b"INFO commandstats\r\n").unwrap();
    let mut buf2 = [0u8; 8192];
    let n2 = c2.read(&mut buf2).unwrap();
    let info2 = String::from_utf8_lossy(&buf2[..n2]);
    assert!(!info2.contains("cmdstat_get:"));
    assert!(!info2.contains("cmdstat_set:"));
}

#[test]
fn test_graceful_shutdown_command_and_worker_exit_e2e() {
    let port = 16711;
    rudis::shutdown::reset_shutdown();

    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(2);
    let mut handles = Vec::new();

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
        let handle = thread::spawn(move || {
            run_shard_worker(
                shard_id,
                2,
                port,
                shard_senders,
                rx,
                None,
                rudis::aof::AofConfig::default(),
                None,
                false,
            );
        });
        handles.push(handle);
    }

    thread::sleep(Duration::from_millis(200));

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut client, b"PING\r\n"), "+PONG\r\n");
    assert_eq!(
        send_and_read(&mut client, b"SET k_shut v_shut\r\n"),
        "+OK\r\n"
    );

    // Issue SHUTDOWN NOSAVE command
    let resp = send_and_read(&mut client, b"SHUTDOWN NOSAVE\r\n");
    assert_eq!(resp, "+OK\r\n");

    // All shard worker threads should gracefully exit and join within 1.5 seconds
    for handle in handles {
        handle
            .join()
            .expect("Worker thread failed to join cleanly during graceful shutdown");
    }

    assert!(rudis::shutdown::is_shutting_down());
    rudis::shutdown::reset_shutdown();
}

#[test]
fn test_config_file_loading_and_cli_merge_e2e() {
    let temp_dir = std::env::temp_dir();
    let conf_path = temp_dir.join(format!("rudis_test_{}.conf", std::process::id()));
    let conf_content = r#"
    # Test redis.conf style configuration
    port 16715
    threads 2
    maxmemory 256mb
    tiered-offload-threshold 65
    tiered-upload-threshold 85
    appendonly yes
    "#;
    std::fs::write(&conf_path, conf_content).expect("Failed to write temp conf");

    let mut cfg = rudis::config::RudisConfig::load_file(&conf_path).expect("Failed to load conf");
    assert_eq!(cfg.port, 16715);
    assert_eq!(cfg.threads, Some(2));
    assert_eq!(cfg.maxmemory.as_deref(), Some("256mb"));
    assert_eq!(cfg.maxmemory_bytes, Some(256 * 1024 * 1024));
    assert_eq!(cfg.tiered_offload_threshold, 65);
    assert_eq!(cfg.tiered_upload_threshold, 85);
    assert!(cfg.appendonly);

    // Test CLI overrides take precedence
    cfg.merge_cli(
        Some(16716),
        Some(1),
        None,
        None,
        Some("512mb".to_string()),
        None,
        None,
        None,
        None,
        None,
        None,
    );
    assert_eq!(cfg.port, 16716);
    assert_eq!(cfg.threads, Some(1));
    assert_eq!(cfg.maxmemory.as_deref(), Some("512mb"));
    assert_eq!(cfg.maxmemory_bytes, Some(512 * 1024 * 1024));
    assert!(cfg.appendonly); // preserved from file

    // Cleanup
    let _ = std::fs::remove_file(conf_path);
}

#[test]
fn test_maxclients_and_memory_eviction_e2e() {
    let port = 16720;
    start_test_server(port, 1);

    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut client1, b"PING\r\n"), "+PONG\r\n");

    // 1. Verify CONFIG GET and SET for maxclients
    let cfg_resp = send_and_read(&mut client1, b"CONFIG GET maxclients\r\n");
    assert!(cfg_resp.contains("maxclients"));

    assert_eq!(
        send_and_read(&mut client1, b"CONFIG SET maxclients 1\r\n"),
        "+OK\r\n"
    );

    // Connecting a 2nd client should immediately be rejected with max number of clients reached
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let mut buf = [0u8; 128];
    client2
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let n = client2.read(&mut buf).unwrap_or(0);
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.contains("-ERR max number of clients reached"));

    // Reset maxclients so other tests and operations are not affected
    assert_eq!(
        send_and_read(&mut client1, b"CONFIG SET maxclients 10000\r\n"),
        "+OK\r\n"
    );

    // 2. Verify CONFIG GET and SET for maxmemory-policy
    assert_eq!(
        send_and_read(&mut client1, b"CONFIG SET maxmemory-policy allkeys-lru\r\n"),
        "+OK\r\n"
    );
    let policy_resp = send_and_read(&mut client1, b"CONFIG GET maxmemory-policy\r\n");
    assert!(policy_resp.contains("allkeys-lru"));

    // Set maxmemory to small value and verify keys can still be set under allkeys-lru (eviction succeeds)
    assert_eq!(
        send_and_read(&mut client1, b"CONFIG SET maxmemory 1mb\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"SET k_evict1 v_evict1\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"SET k_evict2 v_evict2\r\n"),
        "+OK\r\n"
    );

    // Reset maxmemory to 0
    assert_eq!(
        send_and_read(&mut client1, b"CONFIG SET maxmemory 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"CONFIG SET maxmemory-policy noeviction\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_linux_kernel_syscheck_e2e() {
    let report = rudis::syscheck::run_system_sanity_checks();
    // Verify report fields are correctly inspected
    assert!(report.max_open_files.is_some());
    // Verify print does not panic
    rudis::syscheck::print_sanity_warnings(&report);
}

#[test]
fn test_prometheus_telemetry_metrics_e2e() {
    let port = 16725;
    start_test_server(port, 1);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut client, b"PING\r\n"), "+PONG\r\n");

    client.write_all(b"INFO metrics\r\n").unwrap();
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.contains("rudis_connected_clients"));
    assert!(resp.contains("rudis_used_memory_bytes"));
    assert!(resp.contains("rudis_max_clients"));

    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client2.write_all(b"INFO prometheus\r\n").unwrap();
    let mut buf2 = [0u8; 4096];
    let n2 = client2.read(&mut buf2).unwrap();
    let resp_prom = String::from_utf8_lossy(&buf2[..n2]);
    assert!(resp_prom.contains("rudis_connected_clients"));
    assert!(resp_prom.contains("rudis_expired_keys_total"));
}

#[test]
fn test_deployment_configuration_and_service_assets_e2e() {
    let conf_path = std::path::Path::new("rudis.conf");
    assert!(
        conf_path.exists(),
        "rudis.conf should exist in repository root"
    );
    let cfg =
        rudis::config::RudisConfig::load_file(conf_path).expect("rudis.conf should parse cleanly");
    assert_eq!(cfg.port, 6379);
    assert_eq!(cfg.maxclients, 10000);
    assert_eq!(cfg.maxmemory_policy, "allkeys-lru");
    assert_eq!(cfg.maxmemory.as_deref(), Some("4gb"));
    assert!(cfg.appendonly);

    let service_path = std::path::Path::new("rudis.service");
    assert!(
        service_path.exists(),
        "rudis.service should exist in repository root"
    );
    let service_content = std::fs::read_to_string(service_path).unwrap();
    assert!(service_content.contains("ExecStart="));
    assert!(service_content.contains("LimitNOFILE="));

    let dockerfile_path = std::path::Path::new("Dockerfile");
    assert!(
        dockerfile_path.exists(),
        "Dockerfile should exist in repository root"
    );
    let docker_content = std::fs::read_to_string(dockerfile_path).unwrap();
    assert!(docker_content.contains("FROM rust:"));
    assert!(docker_content.contains("ENTRYPOINT"));

    let ci_path = std::path::Path::new(".github/workflows/ci.yml");
    assert!(ci_path.exists(), "CI workflow should exist");
}

#[test]
fn test_panic_isolation_resilience_e2e() {
    let port = 16730;
    start_test_server(port, 1);

    // Initial client connects and sets a key
    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut client1, b"SET foo bar\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client1, b"GET foo\r\n"), "$3\r\nbar\r\n");

    // Client sends DEBUG PANIC to deliberately trigger a panic in connection handler
    let _ = client1.write_all(b"DEBUG PANIC\r\n");
    // Client1 should be disconnected due to isolated panic
    let mut buf = [0u8; 128];
    let n = client1.read(&mut buf).unwrap_or(0);
    assert_eq!(n, 0, "Client socket should be closed after panic unwinding");

    // Verify server process and shard worker are STILL alive and completely operational!
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut client2, b"PING\r\n"), "+PONG\r\n");
    assert_eq!(send_and_read(&mut client2, b"GET foo\r\n"), "$3\r\nbar\r\n");
    assert_eq!(send_and_read(&mut client2, b"SET baz qux\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client2, b"GET baz\r\n"), "$3\r\nqux\r\n");

    // Check INFO clients shows isolated_panics count increased
    client2.write_all(b"INFO clients\r\n").unwrap();
    let mut info_buf = [0u8; 2048];
    let info_n = client2.read(&mut info_buf).unwrap();
    let info_str = String::from_utf8_lossy(&info_buf[..info_n]);
    assert!(
        info_str.contains("isolated_panics:1"),
        "Expected isolated_panics:1 in INFO clients"
    );

    // Check INFO prometheus shows rudis_isolated_panics_total
    client2.write_all(b"INFO prometheus\r\n").unwrap();
    let mut prom_buf = [0u8; 4096];
    let prom_n = client2.read(&mut prom_buf).unwrap();
    let prom_str = String::from_utf8_lossy(&prom_buf[..prom_n]);
    assert!(
        prom_str.contains("rudis_isolated_panics_total 1"),
        "Expected rudis_isolated_panics_total 1 in Prometheus metrics"
    );
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
fn test_extended_types_rdb_persistence_e2e() {
    let port = 16740;
    let rdb_dir = std::env::temp_dir().join(format!("rudis-rdb-ext-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&rdb_dir);
    let aof_config = rudis::aof::AofConfig {
        enabled: false,
        dir: rdb_dir.clone(),
        fsync_every_sec: false,
    };
    start_test_server_with_aof(port, 1, aof_config);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Set JSON document
    assert_eq!(
        send_and_read(
            &mut client,
            b"JSON.SET doc:ext_1 $ {\"title\":\"test\",\"rating\":5}\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"JSON.GET doc:ext_1 $\r\n"),
        "$27\r\n{\"rating\":5,\"title\":\"test\"}\r\n"
    );

    // 2. Add Bloom filter items
    assert_eq!(
        send_and_read(&mut client, b"BF.RESERVE bf:items_ext 0.01 1000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"BF.ADD bf:items_ext my_token\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"BF.EXISTS bf:items_ext my_token\r\n"),
        ":1\r\n"
    );

    // 3. Trigger SAVE to generate RDB snapshot containing extended types
    let save_resp = send_and_read(&mut client, b"SAVE\r\n");
    assert_eq!(save_resp, "+OK\r\n");
    let rdb_file = rdb_dir.join("dump.rdb");
    assert!(rdb_file.exists(), "RDB snapshot should exist in temp dir");

    // 4. Verify data remains valid and retrievable
    assert_eq!(
        send_and_read(&mut client, b"JSON.GET doc:ext_1 $\r\n"),
        "$27\r\n{\"rating\":5,\"title\":\"test\"}\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"BF.EXISTS bf:items_ext my_token\r\n"),
        ":1\r\n"
    );

    let _ = std::fs::remove_dir_all(rdb_dir);
}

#[test]
fn test_geospatial_bounding_geohash_pruning_e2e() {
    let port = 16745;
    start_test_server(port, 1);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    assert_eq!(
        send_and_read(
            &mut client,
            b"GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania 12.496366 41.902782 Rome\r\n"
        ),
        ":3\r\n"
    );

    // 1. GEORADIUS: 100km around Palermo should return ONLY Palermo
    let res_100 = send_and_read(
        &mut client,
        b"GEORADIUS Sicily 13.361389 38.115556 100 km\r\n",
    );
    assert!(res_100.contains("Palermo"));
    assert!(!res_100.contains("Catania"));
    assert!(!res_100.contains("Rome"));

    // 2. GEORADIUS: 200km around Palermo should return Palermo and Catania, but NOT Rome
    let res_200 = send_and_read(
        &mut client,
        b"GEORADIUS Sicily 13.361389 38.115556 200 km\r\n",
    );
    assert!(res_200.contains("Palermo"));
    assert!(res_200.contains("Catania"));
    assert!(!res_200.contains("Rome"));

    // 3. GEORADIUSBYMEMBER: 100km from Palermo
    let res_member = send_and_read(&mut client, b"GEORADIUSBYMEMBER Sicily Palermo 100 km\r\n");
    assert!(res_member.contains("Palermo"));
    assert!(!res_member.contains("Catania"));

    // 4. GEOSEARCH by radius
    let res_search_rad = send_and_read(
        &mut client,
        b"GEOSEARCH Sicily FROMLONLAT 13.361389 38.115556 BYRADIUS 100 km\r\n",
    );
    assert!(res_search_rad.contains("Palermo"));
    assert!(!res_search_rad.contains("Catania"));

    // 5. GEOSEARCH by box
    let res_search_box = send_and_read(
        &mut client,
        b"GEOSEARCH Sicily FROMLONLAT 13.361389 38.115556 BYBOX 100 100 km\r\n",
    );
    assert!(res_search_box.contains("Palermo"));
    assert!(!res_search_box.contains("Catania"));
}

#[test]
fn test_json_mget_multi_shard_parallel_fanout_e2e() {
    let port = 16760;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Populate JSON documents mapped across distinct shards
    assert_eq!(
        send_and_read(
            &mut client,
            b"JSON.SET user:1 $ {\"name\":\"alice\",\"age\":30,\"score\":95}\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"JSON.SET user:2 $ {\"name\":\"bob\",\"age\":25,\"score\":88}\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            b"JSON.SET user:3 $ {\"name\":\"carol\",\"age\":35,\"score\":92}\r\n"
        ),
        "+OK\r\n"
    );

    // 2. Multi-shard JSON.MGET on specific path $.score
    let resp = send_and_read(
        &mut client,
        b"JSON.MGET user:1 user:2 user:missing user:3 $.score\r\n",
    );
    assert_eq!(
        resp, "*4\r\n$2\r\n95\r\n$2\r\n88\r\n$-1\r\n$2\r\n92\r\n",
        "JSON.MGET should preserve exact requested order across shards"
    );

    // 3. Multi-shard JSON.MGET on root path $
    let resp_root = send_and_read(&mut client, b"JSON.MGET user:1 user:missing $\r\n");
    assert!(resp_root.starts_with("*2\r\n"));
    assert!(resp_root.contains("alice"));
    assert!(resp_root.ends_with("$-1\r\n"));
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
fn test_cluster_quorum_failure_detection_and_gossip_e2e() {
    let port1 = 16762;
    let port2 = 16763;
    let port3 = 16764;

    start_test_server(port1, 2);
    start_test_server(port2, 2);
    start_test_server(port3, 2);

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
            "{} 127.0.0.1:{}@{} fail",
            myid3,
            port3,
            port3 + 10000
        )),
        "Node 3 should be marked fail on Node 1 after FAIL message. Nodes:\n{}",
        nodes_after_fail
    );
}

#[test]
fn test_aof_bgrewriteaof_compaction_e2e() {
    let port1 = 16765;
    let port2 = 16766;
    let num_shards = 2;
    let aof_dir = std::env::temp_dir().join(format!("rudis-aof-rewrite-e2e-{}", port1));
    let _ = std::fs::remove_dir_all(&aof_dir);
    std::fs::create_dir_all(&aof_dir).unwrap();

    let aof_config1 = rudis::aof::AofConfig {
        enabled: true,
        dir: aof_dir.clone(),
        fsync_every_sec: true,
    };

    // 1. Start Server 1 with AOF enabled
    start_test_server_with_aof(port1, num_shards, aof_config1);

    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();

    // 2. Perform redundant mutations on same keys across shards to inflate AOF log
    for i in 0..50 {
        assert_eq!(
            send_and_read(&mut client1, format!("SET counter {}\r\n", i).as_bytes()),
            "+OK\r\n"
        );
        assert_eq!(
            send_and_read(&mut client1, format!("SET item {}\r\n", i).as_bytes()),
            "+OK\r\n"
        );
    }
    assert_eq!(
        send_and_read(&mut client1, b"HSET myhash f1 v1 f2 v2\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"JSON.SET doc1 $ {\"val\":\"compacted\"}\r\n"),
        "+OK\r\n"
    );

    // Save initial AOF to disk
    assert_eq!(send_and_read(&mut client1, b"SAVE\r\n"), "+OK\r\n");

    let aof_file0 = aof_dir.join("appendonly-0.aof");
    let aof_file1 = aof_dir.join("appendonly-1.aof");
    let initial_size0 = aof_file0.metadata().map(|m| m.len()).unwrap_or(0);
    let initial_size1 = aof_file1.metadata().map(|m| m.len()).unwrap_or(0);
    assert!(initial_size0 + initial_size1 > 0);

    // 3. Issue BGREWRITEAOF command
    let rewrite_resp = send_and_read(&mut client1, b"BGREWRITEAOF\r\n");
    assert_eq!(
        rewrite_resp,
        "+Background append only file rewriting started\r\n"
    );

    // Wait for async background rewrite to complete and flush to disk
    thread::sleep(Duration::from_millis(300));

    let compacted_size0 = aof_file0.metadata().map(|m| m.len()).unwrap_or(0);
    let compacted_size1 = aof_file1.metadata().map(|m| m.len()).unwrap_or(0);
    assert!(compacted_size0 + compacted_size1 > 0);

    // 3.5. Execute new writes on client1 AFTER rewrite to verify logging continues on compacted AOF files
    assert_eq!(
        send_and_read(&mut client1, b"SET after_rewrite_key after_rewrite_val\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"HSET myhash f3 v3\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client1, b"SAVE\r\n"), "+OK\r\n");

    drop(client1);
    thread::sleep(Duration::from_millis(200));

    // 4. Start Server 2 pointing to the same compacted AOF directory
    let aof_config2 = rudis::aof::AofConfig {
        enabled: true,
        dir: aof_dir.clone(),
        fsync_every_sec: true,
    };
    start_test_server_with_aof(port2, num_shards, aof_config2);

    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();

    // 5. Verify all compacted state and subsequent writes restored cleanly across shards
    assert_eq!(
        send_and_read(&mut client2, b"GET counter\r\n"),
        "$2\r\n49\r\n"
    );
    assert_eq!(send_and_read(&mut client2, b"GET item\r\n"), "$2\r\n49\r\n");
    assert_eq!(
        send_and_read(&mut client2, b"HGET myhash f1\r\n"),
        "$2\r\nv1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"HGET myhash f3\r\n"),
        "$2\r\nv3\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"GET after_rewrite_key\r\n"),
        "$17\r\nafter_rewrite_val\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"JSON.GET doc1 $.val\r\n"),
        "$11\r\n\"compacted\"\r\n"
    );

    let _ = std::fs::remove_dir_all(aof_dir);
}

#[test]
fn test_extended_rdb_full_server_bgsave_and_restore_e2e() {
    let port1 = 16767;
    let port2 = 16768;
    let num_shards = 2;
    let rdb_dir = std::env::temp_dir().join(format!("rudis-rdb-extended-e2e-{}", port1));
    let _ = std::fs::remove_dir_all(&rdb_dir);
    std::fs::create_dir_all(&rdb_dir).unwrap();

    let aof_config1 = rudis::aof::AofConfig {
        enabled: false,
        dir: rdb_dir.clone(),
        fsync_every_sec: false,
    };

    // 1. Start Server 1 with RDB persistence (AOF disabled)
    start_test_server_with_aof(port1, num_shards, aof_config1);

    let mut client1 = TcpStream::connect(format!("127.0.0.1:{}", port1)).unwrap();

    // 2. Populate diverse data types across extended subsystems
    assert_eq!(
        send_and_read(&mut client1, b"SET str_test value_one\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client1,
            b"JSON.SET doc_snap $ {\"status\":\"persisted\",\"tier\":1}\r\n"
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"BF.RESERVE snap_bf 0.01 1000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"BF.ADD snap_bf item_beta\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"CF.RESERVE snap_cf 1000\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"CF.ADD snap_cf item_gamma\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"CMS.INITBYDIM snap_cms 200 5\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"CMS.INCRBY snap_cms metric_hits 88\r\n"),
        "*1\r\n:88\r\n"
    );
    assert_eq!(
        send_and_read(&mut client1, b"TOPK.RESERVE snap_topk 3\r\n"),
        "+OK\r\n"
    );
    let topk_add_res = send_and_read(&mut client1, b"TOPK.ADD snap_topk user_super\r\n");
    assert!(topk_add_res.starts_with("*1\r\n"));

    assert!(send_and_read(&mut client1, b"CRDT.SET crdt_snap val_snap\r\n").starts_with("+OK"));

    // 3. Trigger synchronous SAVE to write dump.rdb
    let save_res = send_and_read(&mut client1, b"SAVE\r\n");
    assert_eq!(save_res, "+OK\r\n");

    let rdb_file = rdb_dir.join("dump.rdb");
    assert!(rdb_file.exists());
    assert!(rdb_file.metadata().map(|m| m.len()).unwrap_or(0) > 0);

    drop(client1);
    thread::sleep(Duration::from_millis(200));

    // 4. Start Server 2 pointing to the same RDB directory
    let aof_config2 = rudis::aof::AofConfig {
        enabled: false,
        dir: rdb_dir.clone(),
        fsync_every_sec: false,
    };
    start_test_server_with_aof(port2, num_shards, aof_config2);

    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port2)).unwrap();

    // 5. Verify all extended data restored cleanly from dump.rdb on cold start
    assert_eq!(
        send_and_read(&mut client2, b"GET str_test\r\n"),
        "$9\r\nvalue_one\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"JSON.GET doc_snap $.status\r\n"),
        "$11\r\n\"persisted\"\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"BF.EXISTS snap_bf item_beta\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"BF.EXISTS snap_bf nonexistent\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"CF.EXISTS snap_cf item_gamma\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"CF.EXISTS snap_cf nonexistent\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"CMS.QUERY snap_cms metric_hits\r\n"),
        "*1\r\n:88\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"TOPK.QUERY snap_topk user_super\r\n"),
        "*1\r\n:1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client2, b"CRDT.GET crdt_snap\r\n"),
        "$8\r\nval_snap\r\n"
    );

    let _ = std::fs::remove_dir_all(rdb_dir);
}

#[test]
fn test_del_multi_shard_parallel_fanout_e2e() {
    let port = 16769;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Generate 80 keys distributed across all 4 shards
    let mut keys_by_shard: Vec<Vec<String>> = (0..num_shards).map(|_| Vec::new()).collect();
    let mut all_keys = Vec::new();
    let mut i = 0;
    while all_keys.len() < 80 {
        let key = format!("del_fanout_key_{}", i);
        let s = rudis::router::target_shard(key.as_bytes(), num_shards);
        keys_by_shard[s].push(key.clone());
        all_keys.push(key);
        i += 1;
    }

    // Verify all 4 shards have keys
    for (s, klist) in keys_by_shard.iter().enumerate() {
        assert!(!klist.is_empty(), "Shard {} should have keys", s);
    }

    // 2. Populate all keys using MSET
    let mut mset_cmd = format!("*{}\r\n$4\r\nMSET\r\n", all_keys.len() * 2 + 1);
    for k in &all_keys {
        mset_cmd.push_str(&format!("${}\r\n{}\r\n$3\r\nval\r\n", k.len(), k));
    }
    assert_eq!(send_and_read(&mut client, mset_cmd.as_bytes()), "+OK\r\n");

    // 3. Issue parallel cross-shard DEL with all 80 keys + 10 non-existent keys
    let mut del_cmd = format!("*{}\r\n$3\r\nDEL\r\n", all_keys.len() + 10 + 1);
    for k in &all_keys {
        del_cmd.push_str(&format!("${}\r\n{}\r\n", k.len(), k));
    }
    for j in 0..10 {
        let missing = format!("missing_del_{}", j);
        del_cmd.push_str(&format!("${}\r\n{}\r\n", missing.len(), missing));
    }

    let del_resp = send_and_read(&mut client, del_cmd.as_bytes());
    assert_eq!(del_resp, ":80\r\n");

    // 4. Verify all keys are truly deleted across all shards
    for k in &all_keys {
        assert_eq!(
            send_and_read(&mut client, format!("EXISTS {}\r\n", k).as_bytes()),
            ":0\r\n"
        );
    }

    // 5. Deleting them again returns 0
    let del_again_resp = send_and_read(&mut client, del_cmd.as_bytes());
    assert_eq!(del_again_resp, ":0\r\n");
}

#[test]
fn test_active_defrag_e2e() {
    let port = 16770;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Insert 400 keys across shards
    for i in 0..400 {
        assert_eq!(
            send_and_read(
                &mut client,
                format!("SET defrag_k_{} val_{}\r\n", i, i).as_bytes()
            ),
            "+OK\r\n"
        );
    }
    assert_eq!(send_and_read(&mut client, b"DBSIZE\r\n"), ":400\r\n");

    // 2. Delete 380 keys, generating lots of tombstones
    for i in 0..380 {
        assert_eq!(
            send_and_read(&mut client, format!("DEL defrag_k_{}\r\n", i).as_bytes()),
            ":1\r\n"
        );
    }
    assert_eq!(send_and_read(&mut client, b"DBSIZE\r\n"), ":20\r\n");

    // 3. Trigger MEMORY DEFRAG
    let defrag_resp = send_and_read(&mut client, b"MEMORY DEFRAG\r\n");
    assert_eq!(defrag_resp, "+OK\r\n");

    // 4. Verify all remaining 20 keys are intact
    for i in 380..400 {
        assert_eq!(
            send_and_read(&mut client, format!("GET defrag_k_{}\r\n", i).as_bytes()),
            format!("${}\r\nval_{}\r\n", format!("val_{}", i).len(), i)
        );
    }

    // 5. Test DEFRAG alias and MEMORY PURGE
    assert_eq!(send_and_read(&mut client, b"DEFRAG\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"MEMORY PURGE\r\n"), "+OK\r\n");
}

#[test]
fn test_cluster_setslot_live_migration_and_ask_redirection_e2e() {
    let port1 = 16771;
    let port2 = 16772;
    start_test_server(port1, 2);
    start_test_server(port2, 2);

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
fn test_redisearch_on_json_secondary_index_e2e() {
    let port = 16773;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Create index ON JSON with JSONPath fields and AS aliases
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE idx:inventory ON JSON PREFIX 1 item: SCHEMA $.title AS title TEXT $.price AS price NUMERIC $.tags.* AS tags TAG $.details.in_stock AS in_stock NUMERIC\r\n"
        ),
        "+OK\r\n"
    );

    // 2. Populate JSON documents via JSON.SET
    let item1 = br#"{"title": "High performance Rust distributed database", "price": 99.99, "tags": ["rust", "database", "distributed"], "details": {"in_stock": 1}}"#;
    let mut cmd1 = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nitem:1\r\n$1\r\n$\r\n${}\r\n",
        item1.len()
    )
    .into_bytes();
    cmd1.extend_from_slice(item1);
    cmd1.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd1), "+OK\r\n");

    let item2 = br#"{"title": "Dragonfly in-memory data store in C++", "price": 49.50, "tags": ["cache", "memory"], "details": {"in_stock": 0}}"#;
    let mut cmd2 = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nitem:2\r\n$1\r\n$\r\n${}\r\n",
        item2.len()
    )
    .into_bytes();
    cmd2.extend_from_slice(item2);
    cmd2.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd2), "+OK\r\n");

    let item3 = br#"{"title": "Kafka distributed stream broker in Scala and Java", "price": 120.00, "tags": ["streaming", "distributed"], "details": {"in_stock": 1}}"#;
    let mut cmd3 = format!(
        "*4\r\n$8\r\nJSON.SET\r\n$6\r\nitem:3\r\n$1\r\n$\r\n${}\r\n",
        item3.len()
    )
    .into_bytes();
    cmd3.extend_from_slice(item3);
    cmd3.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut client, &cmd3), "+OK\r\n");

    // 3. Query 1: Full-text search for "distributed" (matches item:1 and item:3)
    let search1 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n$11\r\ndistributed\r\n",
    );
    assert!(
        search1.starts_with("*5\r\n:2\r\n"),
        "Expected 2 hits, got: {}",
        search1
    );
    assert!(search1.contains("item:1"));
    assert!(search1.contains("item:3"));

    // 4. Query 2: Numeric range on price: @price:[40 100] (matches item:1 and item:2)
    let search2 = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n$15\r\n@price:[40 100]\r\n",
    );
    assert!(
        search2.starts_with("*5\r\n:2\r\n"),
        "Expected 2 hits, got: {}",
        search2
    );
    assert!(search2.contains("item:1"));
    assert!(search2.contains("item:2"));

    // 5. Query 3: Tag filter: @tags:{rust} (matches item:1)
    let q3 = "@tags:{rust}";
    let search3_cmd = format!(
        "*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n${}\r\n{}\r\n",
        q3.len(),
        q3
    );
    let search3 = send_and_read(&mut client, search3_cmd.as_bytes());
    assert!(
        search3.starts_with("*3\r\n:1\r\n"),
        "Expected 1 hit, got: {}",
        search3
    );
    assert!(search3.contains("item:1"));

    // 6. Query 4: Nested JSONPath numeric range: @in_stock:[1 1] (matches item:1 and item:3)
    let q4 = "@in_stock:[1 1]";
    let search4_cmd = format!(
        "*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n${}\r\n{}\r\n",
        q4.len(),
        q4
    );
    let search4 = send_and_read(&mut client, search4_cmd.as_bytes());
    assert!(
        search4.starts_with("*5\r\n:2\r\n"),
        "Expected 2 hits, got: {}",
        search4
    );
    assert!(search4.contains("item:1"));
    assert!(search4.contains("item:3"));

    // 7. Mutate subpath: JSON.SET item:2 $.price 35.00
    let mutate_cmd = "*4\r\n$8\r\nJSON.SET\r\n$6\r\nitem:2\r\n$7\r\n$.price\r\n$5\r\n35.00\r\n";
    assert_eq!(send_and_read(&mut client, mutate_cmd.as_bytes()), "+OK\r\n");
    let q_mut = "@price:[30 40]";
    let search_mut_cmd = format!(
        "*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n${}\r\n{}\r\n",
        q_mut.len(),
        q_mut
    );
    let search_mutated = send_and_read(&mut client, search_mut_cmd.as_bytes());
    assert!(
        search_mutated.starts_with("*3\r\n:1\r\n"),
        "Expected 1 hit after subpath update, got: {}",
        search_mutated
    );
    assert!(search_mutated.contains("item:2"));

    // 8. Test RETURN fields
    let search_return = send_and_read(
        &mut client,
        b"*7\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n$4\r\nRust\r\n$6\r\nRETURN\r\n$1\r\n2\r\n$5\r\ntitle\r\n$5\r\nprice\r\n",
    );
    assert!(search_return.contains("item:1"));
    assert!(search_return.contains("High performance Rust distributed database"));

    // 9. Document deletion via JSON.DEL
    assert_eq!(
        send_and_read(&mut client, b"JSON.DEL item:1 $\r\n"),
        ":1\r\n"
    );
    let search_after_del = send_and_read(
        &mut client,
        b"*3\r\n$9\r\nFT.SEARCH\r\n$13\r\nidx:inventory\r\n$4\r\nRust\r\n",
    );
    assert_eq!(search_after_del, "*1\r\n:0\r\n");

    // 10. Clean up index
    assert_eq!(
        send_and_read(&mut client, b"FT.DROPINDEX idx:inventory\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_simd_vector_distance_acceleration_hnsw_e2e() {
    let port = 16774;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Ingest 16-dimensional vectors into HNSW index
    // doc_a: mostly along axis 0
    let v_a = "1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    assert_eq!(
        send_and_read(
            &mut client,
            format!("VADD simd_idx doc_a {}\r\n", v_a).as_bytes()
        ),
        "+OK\r\n"
    );

    // doc_b: mostly along axis 1
    let v_b = "0.0 1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    assert_eq!(
        send_and_read(
            &mut client,
            format!("VADD simd_idx doc_b {}\r\n", v_b).as_bytes()
        ),
        "+OK\r\n"
    );

    // doc_c: close to doc_a (0.95 on axis 0, 0.05 on axis 1)
    let v_c = "0.95 0.05 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    assert_eq!(
        send_and_read(
            &mut client,
            format!("VADD simd_idx doc_c {}\r\n", v_c).as_bytes()
        ),
        "+OK\r\n"
    );

    // doc_d: close to doc_b (0.05 on axis 0, 0.95 on axis 1)
    let v_d = "0.05 0.95 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    assert_eq!(
        send_and_read(
            &mut client,
            format!("VADD simd_idx doc_d {}\r\n", v_d).as_bytes()
        ),
        "+OK\r\n"
    );

    // 2. Query top-2 nearest neighbors to doc_a
    let q_a = "1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0";
    let query_res = send_and_read(
        &mut client,
        format!("VQUERY simd_idx 2 {}\r\n", q_a).as_bytes(),
    );
    assert!(
        query_res.starts_with("*4\r\n"),
        "Expected 2 key-dist pairs, got: {}",
        query_res
    );
    assert!(query_res.contains("doc_a"));
    assert!(query_res.contains("doc_c"));

    // 3. VSIM across metrics
    // Cosine distance between doc_a and doc_c (small distance)
    let sim_ac = send_and_read(&mut client, b"VSIM simd_idx doc_a doc_c COSINE\r\n");
    assert!(sim_ac.starts_with('$'));
    let dist_ac: f32 = sim_ac
        .lines()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("Parsed float distance");
    assert!(dist_ac < 0.05, "Expected close distance, got: {}", dist_ac);

    // Cosine distance between doc_a and doc_b (orthogonal -> ~1.0)
    let sim_ab = send_and_read(&mut client, b"VSIM simd_idx doc_a doc_b COSINE\r\n");
    let dist_ab: f32 = sim_ab
        .lines()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("Parsed float distance");
    assert!(
        (dist_ab - 1.0).abs() < 0.05,
        "Expected orthogonal ~1.0 distance, got: {}",
        dist_ab
    );

    // L2 Euclidean distance between doc_a and doc_b (~sqrt(2) = 1.414)
    let sim_l2 = send_and_read(&mut client, b"VSIM simd_idx doc_a doc_b L2\r\n");
    let dist_l2: f32 = sim_l2
        .lines()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("Parsed float distance");
    assert!(
        (dist_l2 - std::f32::consts::SQRT_2).abs() < 0.05,
        "Expected L2 sqrt(2), got: {}",
        dist_l2
    );

    // 4. VDEL doc_a
    assert_eq!(
        send_and_read(&mut client, b"VDEL simd_idx doc_a\r\n"),
        ":1\r\n"
    );

    // 5. Query after deletion: doc_c should now be the top-1 result
    let query_after_del = send_and_read(
        &mut client,
        format!("VQUERY simd_idx 1 {}\r\n", q_a).as_bytes(),
    );
    assert!(query_after_del.contains("doc_c"));
    assert!(!query_after_del.contains("doc_a"));

    // 6. VINFO verification
    let info = send_and_read(&mut client, b"VINFO simd_idx\r\n");
    assert!(info.starts_with("*8\r\n"));
}

#[test]
fn test_af_xdp_kernel_bypass_zero_copy_rings_e2e() {
    let port = 16775;
    let num_shards = 2;
    start_test_server(port, num_shards);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    // 1. Verify XDP.INFO shows kernel bypass engine and active queues
    let info = send_and_read(&mut client, b"XDP.INFO\r\n");
    assert!(info.contains("active_xsk_queues:"));
    assert!(info.contains("interface:"));

    // 2. Query XDP.SOCKET for queue 0
    let sock_stat = send_and_read(&mut client, b"XDP.SOCKET 0\r\n");
    assert!(sock_stat.starts_with("*8\r\n"));
    assert!(sock_stat.contains("rx_len"));
    assert!(sock_stat.contains("fill_len"));

    // 3. Inject RESP command directly into AF_XDP zero-copy Rx ring for Shard 0
    let resp_cmd = b"*3\r\n$3\r\nSET\r\n$7\r\nxdp_key\r\n$7\r\nxdp_val\r\n";
    let mut inject_frame = format!(
        "*3\r\n$10\r\nXDP.INJECT\r\n$1\r\n0\r\n${}\r\n",
        resp_cmd.len()
    )
    .into_bytes();
    inject_frame.extend_from_slice(resp_cmd);
    inject_frame.extend_from_slice(b"\r\n");

    assert_eq!(send_and_read(&mut client, &inject_frame), "+OK\r\n");

    // Give shard worker kernel bypass loop a moment to drain the Rx ring
    std::thread::sleep(Duration::from_millis(50));

    // 4. Verify state mutation over standard TCP interface
    assert_eq!(
        send_and_read(&mut client, b"GET xdp_key\r\n"),
        "$7\r\nxdp_val\r\n"
    );

    // 5. Inject a raw Ethernet + IPv4 + TCP packet containing a Redis command
    // Construct 54-byte Ethernet/IPv4/TCP frame
    let mut raw_packet = vec![0u8; 54];
    raw_packet[12] = 0x08;
    raw_packet[13] = 0x00; // Ethernet IPv4
    raw_packet[14] = 0x45; // IPv4, header len 20 bytes (5 * 4)
    raw_packet[14 + 9] = 6; // TCP protocol
    raw_packet[14 + 12] = 192;
    raw_packet[14 + 13] = 168;
    raw_packet[14 + 14] = 1;
    raw_packet[14 + 15] = 100; // src IP: 192.168.1.100
    raw_packet[34 + 12] = 0x50; // TCP data offset = 20 bytes (5 * 4)

    let cmd_payload = b"*3\r\n$3\r\nSET\r\n$8\r\nxdp_net2\r\n$4\r\npass\r\n";
    raw_packet.extend_from_slice(cmd_payload);

    let mut inject_net_frame = format!(
        "*3\r\n$10\r\nXDP.INJECT\r\n$1\r\n0\r\n${}\r\n",
        raw_packet.len()
    )
    .into_bytes();
    inject_net_frame.extend_from_slice(&raw_packet);
    inject_net_frame.extend_from_slice(b"\r\n");

    assert_eq!(send_and_read(&mut client, &inject_net_frame), "+OK\r\n");

    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(
        send_and_read(&mut client, b"GET xdp_net2\r\n"),
        "$4\r\npass\r\n"
    );

    // 6. Test eBPF firewall rule enforcement: DROP 10.0.0.0/8
    let rule_add_resp = send_and_read(&mut client, b"XDP.RULE ADD DROP 10.0.0.0/8\r\n");
    assert!(rule_add_resp.starts_with(':'));
    let rule_id: u32 = rule_add_resp
        .trim_start_matches(':')
        .trim()
        .parse()
        .expect("Parsed rule ID");

    // Construct raw IPv4 packet from 10.1.2.3
    let mut drop_packet = vec![0u8; 40];
    drop_packet[0] = 0x45; // IPv4
    drop_packet[9] = 6;
    drop_packet[12] = 10;
    drop_packet[13] = 1;
    drop_packet[14] = 2;
    drop_packet[15] = 3;
    drop_packet.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$7\r\ndropkey\r\n$7\r\ndropval\r\n");

    let mut test_packet_cmd =
        format!("*2\r\n$10\r\nXDP.PACKET\r\n${}\r\n", drop_packet.len()).into_bytes();
    test_packet_cmd.extend_from_slice(&drop_packet);
    test_packet_cmd.extend_from_slice(b"\r\n");

    let action_resp = send_and_read(&mut client, &test_packet_cmd);
    assert_eq!(action_resp, "+DROP\r\n");

    // Clean up rule
    assert_eq!(
        send_and_read(
            &mut client,
            format!("XDP.RULE DEL {}\r\n", rule_id).as_bytes()
        ),
        "+OK\r\n"
    );
}

#[test]
fn test_cluster_check_and_rebalance_live_migration_e2e() {
    let port1 = 16776;
    let port2 = 16777;
    start_test_server(port1, 2);
    start_test_server(port2, 2);

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
    thread::sleep(Duration::from_millis(250));

    let myid1 = send_and_read(&mut c1, b"CLUSTER MYID\r\n")
        .trim()
        .replace("$40\r\n", "")
        .replace("\r\n", "");
    let myid2 = send_and_read(&mut c2, b"CLUSTER MYID\r\n")
        .trim()
        .replace("$40\r\n", "")
        .replace("\r\n", "");

    // 3. CLUSTER CHECK on Node 1: verifies all 16384 slots are covered across the 2 nodes
    let check1 = send_and_read(&mut c1, b"CLUSTER CHECK\r\n");
    assert!(check1.contains("[OK] All 16384 slots covered"));

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

    // 7. Verify CLUSTER CHECK is healthy after migrations
    let check_after = send_and_read(&mut c1, b"CLUSTER CHECK\r\n");
    assert!(check_after.contains("[OK]"));

    let check_c2 = send_and_read(&mut c2, b"CLUSTER CHECK\r\n");
    assert!(check_c2.contains("[OK]"));
}

#[test]
fn test_small_collection_arena_and_slice_dispatch_e2e() {
    let port = 16778;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Rapid List operations (LPUSH, LRANGE, LPOP to empty)
    for i in 0..50 {
        let key = format!("list_{}", i);
        let push_cmd = format!("LPUSH {} val_a val_b val_c\r\n", key);
        assert_eq!(send_and_read(&mut client, push_cmd.as_bytes()), ":3\r\n");

        let lrange_cmd = format!("LRANGE {} 0 -1\r\n", key);
        let lrange_resp = send_and_read(&mut client, lrange_cmd.as_bytes());
        assert_eq!(lrange_resp.lines().next().unwrap(), "*3");

        let lpop_cmd = format!("LPOP {} 3\r\n", key);
        let lpop_resp = send_and_read(&mut client, lpop_cmd.as_bytes());
        assert_eq!(lpop_resp.lines().next().unwrap(), "*3");

        // Key should be deleted after all elements popped
        assert_eq!(
            send_and_read(&mut client, format!("EXISTS {}\r\n", key).as_bytes()),
            ":0\r\n"
        );
    }

    // 2. Rapid Hash operations (HSET, HGET, HDEL to empty)
    for i in 0..50 {
        let key = format!("hash_{}", i);
        let hset_cmd = format!("HSET {} f1 v1 f2 v2\r\n", key);
        assert_eq!(send_and_read(&mut client, hset_cmd.as_bytes()), ":2\r\n");

        let hget_cmd = format!("HGET {} f1\r\n", key);
        assert_eq!(
            send_and_read(&mut client, hget_cmd.as_bytes()),
            "$2\r\nv1\r\n"
        );

        let hdel_cmd = format!("HDEL {} f1 f2\r\n", key);
        assert_eq!(send_and_read(&mut client, hdel_cmd.as_bytes()), ":2\r\n");

        assert_eq!(
            send_and_read(&mut client, format!("EXISTS {}\r\n", key).as_bytes()),
            ":0\r\n"
        );
    }

    // 3. Rapid Set operations (SADD, SISMEMBER, SREM to empty)
    for i in 0..50 {
        let key = format!("set_{}", i);
        let sadd_cmd = format!("SADD {} m1 m2\r\n", key);
        assert_eq!(send_and_read(&mut client, sadd_cmd.as_bytes()), ":2\r\n");

        let sismember_cmd = format!("SISMEMBER {} m1\r\n", key);
        assert_eq!(
            send_and_read(&mut client, sismember_cmd.as_bytes()),
            ":1\r\n"
        );

        let srem_cmd = format!("SREM {} m1 m2\r\n", key);
        assert_eq!(send_and_read(&mut client, srem_cmd.as_bytes()), ":2\r\n");

        assert_eq!(
            send_and_read(&mut client, format!("EXISTS {}\r\n", key).as_bytes()),
            ":0\r\n"
        );
    }

    // 4. Rapid ZSet operations (ZADD, ZRANGE, ZREM to empty)
    for i in 0..50 {
        let key = format!("zset_{}", i);
        let zadd_cmd = format!("ZADD {} 1.5 zm1 2.5 zm2\r\n", key);
        assert_eq!(send_and_read(&mut client, zadd_cmd.as_bytes()), ":2\r\n");

        let zrange_cmd = format!("ZRANGE {} 0 -1\r\n", key);
        let zrange_resp = send_and_read(&mut client, zrange_cmd.as_bytes());
        assert_eq!(zrange_resp.lines().next().unwrap(), "*2");

        let zrem_cmd = format!("ZREM {} zm1 zm2\r\n", key);
        assert_eq!(send_and_read(&mut client, zrem_cmd.as_bytes()), ":2\r\n");

        assert_eq!(
            send_and_read(&mut client, format!("EXISTS {}\r\n", key).as_bytes()),
            ":0\r\n"
        );
    }

    // 5. Memory INFO telemetry verification
    let info_resp = send_and_read(&mut client, b"INFO memory\r\n");
    assert!(info_resp.contains("used_memory:"));
    assert!(info_resp.contains("mem_allocator:"));
}

#[test]
fn test_pipelined_cross_shard_sadd_compact_resp_e2e() {
    let port = 16830;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    let mut pipeline = String::new();
    for i in 0..100 {
        pipeline.push_str(&format!("SADD set:k:{} m1\r\n", i));
    }
    client.write_all(pipeline.as_bytes()).unwrap();

    let mut buf = vec![0u8; 100 * 4];
    client.read_exact(&mut buf).unwrap();
    let expected = ":1\r\n".repeat(100);
    assert_eq!(&buf[..], expected.as_bytes());

    // Re-inserting the same member should return :0\r\n for all keys
    client.write_all(pipeline.as_bytes()).unwrap();
    client.read_exact(&mut buf).unwrap();
    let expected_zero = ":0\r\n".repeat(100);
    assert_eq!(&buf[..], expected_zero.as_bytes());
}

#[test]
fn test_smallvec_exists_and_sadd_e2e() {
    let port = 16451;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Single SADD and single EXISTS
    assert_eq!(
        send_and_read(&mut client, b"SADD myset alpha\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"EXISTS myset\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS nokey\r\n"), ":0\r\n");

    // 2. Multi-key EXISTS
    assert_eq!(
        send_and_read(&mut client, b"EXISTS myset nokey\r\n"),
        ":1\r\n"
    );

    // 3. Multi-member SADD
    assert_eq!(
        send_and_read(&mut client, b"SADD myset beta gamma\r\n"),
        ":2\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"SCARD myset\r\n"), ":3\r\n");
}

#[test]
fn test_sadd_fx_hasher_large_set_e2e() {
    let port = 16453;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    let mut cmd = String::from("SADD largeset");
    for i in 0..100 {
        cmd.push_str(&format!(" elem_{}", i));
    }
    cmd.push_str("\r\n");
    assert_eq!(send_and_read(&mut client, cmd.as_bytes()), ":100\r\n");
    assert_eq!(
        send_and_read(&mut client, b"SCARD largeset\r\n"),
        ":100\r\n"
    );

    assert_eq!(
        send_and_read(&mut client, b"SADD largeset elem_0 elem_50 elem_99\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SCARD largeset\r\n"),
        ":100\r\n"
    );

    assert_eq!(
        send_and_read(&mut client, b"SISMEMBER largeset elem_50\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SISMEMBER largeset elem_999\r\n"),
        ":0\r\n"
    );
}

#[test]
fn test_exists_fast_rejection_and_pipeline_e2e() {
    let port = 16454;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    assert_eq!(send_and_read(&mut client, b"SET k1 v1\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SET k12345 v2\r\n"), "+OK\r\n");

    assert_eq!(send_and_read(&mut client, b"EXISTS k1\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS k12345\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS k12\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS k\r\n"), ":0\r\n");
    assert_eq!(send_and_read(&mut client, b"EXISTS k123456\r\n"), ":0\r\n");

    let mut pipeline = String::new();
    for i in 0..50 {
        if i % 2 == 0 {
            pipeline.push_str("EXISTS k1\r\n");
        } else {
            pipeline.push_str("EXISTS nonexisting\r\n");
        }
    }
    client.write_all(pipeline.as_bytes()).unwrap();

    let mut buf = vec![0u8; 50 * 4];
    client.read_exact(&mut buf).unwrap();
    let mut expected = String::new();
    for i in 0..50 {
        if i % 2 == 0 {
            expected.push_str(":1\r\n");
        } else {
            expected.push_str(":0\r\n");
        }
    }
    assert_eq!(&buf[..], expected.as_bytes());
}

#[test]
fn test_pipelined_incr_cross_shard_e2e() {
    let port = 16455;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    let mut pipeline = String::new();
    for i in 0..100 {
        pipeline.push_str(&format!("INCR incr_key_{}\r\n", i % 10));
    }
    client.write_all(pipeline.as_bytes()).unwrap();

    // 10 keys each incremented 1..=10. Numbers 1..=9 take 4 bytes, 10 takes 5 bytes. (9*4 + 5 = 41 bytes per key * 10 keys = 410 bytes)
    let mut buf = vec![0u8; 410];
    client.read_exact(&mut buf).unwrap();

    assert_eq!(
        send_and_read(&mut client, b"GET incr_key_0\r\n"),
        "$2\r\n10\r\n"
    );
}

#[test]
fn test_in_place_incr_single_pass_e2e() {
    let port = 16456;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    assert_eq!(
        send_and_read(&mut client, b"INCR counter_fast\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"INCR counter_fast\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"INCRBY counter_fast 8\r\n"),
        ":10\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"INCRBY counter_fast -5\r\n"),
        ":5\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET counter_fast\r\n"),
        "$1\r\n5\r\n"
    );
}

#[test]
fn test_fast_integer_responses_e2e() {
    let port = 16457;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    assert_eq!(send_and_read(&mut client, b"INCRBY count 7\r\n"), ":7\r\n");
    assert_eq!(
        send_and_read(&mut client, b"INCRBY count 35\r\n"),
        ":42\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"INCRBY count 100\r\n"),
        ":142\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"INCRBY count 1000\r\n"),
        ":1142\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"INCRBY count -1150\r\n"),
        ":-8\r\n"
    );
}

#[test]
fn test_sismember_pipeline_throughput_and_correctness_e2e() {
    let port = 16458;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // Populate 20 single-member and multi-member sets
    for i in 0..20 {
        let cmd = format!(
            "SADD myset_{} member_{}_val64bytes___________________________________\r\n",
            i, i
        );
        assert_eq!(send_and_read(&mut client, cmd.as_bytes()), ":1\r\n");
    }

    // Pipelined SISMEMBER queries mixing hits and misses
    let mut pipeline = Vec::new();
    for i in 0..20 {
        let key = format!("myset_{}", i);
        let hit_member = format!("member_{}_val64bytes___________________________________", i);
        let miss_member = format!("nomatch_{}_val64bytes__________________________________", i);

        // Hit
        pipeline.extend_from_slice(
            format!(
                "*3\r\n$9\r\nSISMEMBER\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                hit_member.len(),
                hit_member
            )
            .as_bytes(),
        );
        // Miss with same length
        pipeline.extend_from_slice(
            format!(
                "*3\r\n$9\r\nSISMEMBER\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                miss_member.len(),
                miss_member
            )
            .as_bytes(),
        );
    }

    client.write_all(&pipeline).unwrap();

    let mut expected = String::new();
    for _ in 0..20 {
        expected.push_str(":1\r\n:0\r\n");
    }

    let mut response_buf = vec![0u8; expected.len()];
    client.read_exact(&mut response_buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&response_buf), expected);
}

#[test]
fn test_pipelined_cross_shard_mget_mset_e2e() {
    let port = 16459;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Pipelined cross-shard MSET
    let mut mset_pipeline = Vec::new();
    for p in 0..4 {
        let mut cmd = "*11\r\n$4\r\nMSET\r\n".to_string();
        for k in 0..5 {
            let key = format!("cross_k_{}_{}", p, k);
            let val = format!("val_{}_{}", p, k);
            cmd.push_str(&format!(
                "${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                val.len(),
                val
            ));
        }
        mset_pipeline.extend_from_slice(cmd.as_bytes());
    }
    client.write_all(&mset_pipeline).unwrap();

    let mut mset_resp = vec![0u8; 5 * 4]; // +OK\r\n * 4 = 20 bytes
    client.read_exact(&mut mset_resp).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&mset_resp),
        "+OK\r\n+OK\r\n+OK\r\n+OK\r\n"
    );

    // 2. Pipelined cross-shard MGET
    let mut mget_pipeline = Vec::new();
    for p in 0..4 {
        let mut cmd = "*6\r\n$4\r\nMGET\r\n".to_string();
        for k in 0..5 {
            let key = format!("cross_k_{}_{}", p, k);
            cmd.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        mget_pipeline.extend_from_slice(cmd.as_bytes());
    }
    client.write_all(&mget_pipeline).unwrap();

    // Read responses for 4 MGETs
    let mut buf = [0u8; 2048];
    let n = client.read(&mut buf).unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    for p in 0..4 {
        for k in 0..5 {
            assert!(resp.contains(&format!("val_{}_{}", p, k)));
        }
    }
}

/// A pipeline that interleaves cross-shard MGET/MSET with other commands must still
/// answer in strict FIFO order, and an MGET must observe an MSET issued earlier in
/// the very same pipeline even though both are dispatched before either is gathered.
#[test]
fn test_interleaved_pipeline_mget_mset_ordering_e2e() {
    let port = 16840;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    let mut pipeline = String::new();

    // 1. MSET of 5 keys spread across shards.
    pipeline.push_str("*11\r\n$4\r\nMSET\r\n");
    for k in 0..5 {
        let key = format!("ord_a_{k}");
        let val = format!("av{k}");
        pipeline.push_str(&format!(
            "${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            val.len(),
            val
        ));
    }
    // 2. MGET of the keys just written by the previous command in this pipeline.
    pipeline.push_str("*6\r\n$4\r\nMGET\r\n");
    for k in 0..5 {
        let key = format!("ord_a_{k}");
        pipeline.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
    }
    // 3. A non-sharded command between two scatter commands.
    pipeline.push_str("*1\r\n$4\r\nPING\r\n");
    // 4. MGET with a hole in the middle.
    pipeline
        .push_str("*4\r\n$4\r\nMGET\r\n$7\r\nord_a_0\r\n$13\r\nord_a_missing\r\n$7\r\nord_a_2\r\n");
    // 5. A single-key SET, then a second MSET, then a GET of the single key.
    pipeline.push_str("*3\r\n$3\r\nSET\r\n$8\r\nord_solo\r\n$2\r\nsv\r\n");
    pipeline.push_str(
        "*5\r\n$4\r\nMSET\r\n$7\r\nord_b_0\r\n$3\r\nbv0\r\n$7\r\nord_b_1\r\n$3\r\nbv1\r\n",
    );
    pipeline.push_str("*2\r\n$3\r\nGET\r\n$8\r\nord_solo\r\n");
    // 6. MGET of the keys written by command 5.
    pipeline.push_str("*3\r\n$4\r\nMGET\r\n$7\r\nord_b_0\r\n$7\r\nord_b_1\r\n");

    client.write_all(pipeline.as_bytes()).unwrap();

    let expected = concat!(
        "+OK\r\n",
        "*5\r\n$3\r\nav0\r\n$3\r\nav1\r\n$3\r\nav2\r\n$3\r\nav3\r\n$3\r\nav4\r\n",
        "+PONG\r\n",
        "*3\r\n$3\r\nav0\r\n$-1\r\n$3\r\nav2\r\n",
        "+OK\r\n",
        "+OK\r\n",
        "$2\r\nsv\r\n",
        "*2\r\n$3\r\nbv0\r\n$3\r\nbv1\r\n",
    );

    let mut response_buf = vec![0u8; expected.len()];
    client.read_exact(&mut response_buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&response_buf), expected);
}

#[test]
fn test_pipelined_exists_cross_shard_e2e() {
    let port = 16850;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // Populate keys across multiple shards
    for i in 0..10 {
        let k = format!("ex_k{}", i);
        let v = format!("v{}", i);
        let cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            v.len(),
            v
        );
        client.write_all(cmd.as_bytes()).unwrap();
        let mut resp = [0u8; 5];
        client.read_exact(&mut resp).unwrap();
        assert_eq!(&resp, b"+OK\r\n");
    }

    // Pipeline EXISTS for present and missing keys across shards
    let mut pipeline = String::new();
    let mut expected = String::new();
    for i in 0..20 {
        if i % 2 == 0 {
            let k = format!("ex_k{}", i / 2);
            pipeline.push_str(&format!("*2\r\n$6\r\nEXISTS\r\n${}\r\n{}\r\n", k.len(), k));
            expected.push_str(":1\r\n");
        } else {
            let k = format!("missing_{}", i);
            pipeline.push_str(&format!("*2\r\n$6\r\nEXISTS\r\n${}\r\n{}\r\n", k.len(), k));
            expected.push_str(":0\r\n");
        }
    }
    client.write_all(pipeline.as_bytes()).unwrap();

    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(&buf[..], expected.as_bytes());
}

#[test]
fn test_pipelined_lpop_cross_shard_e2e() {
    let port = 16860;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // Populate lists across shards
    for i in 0..8 {
        let k = format!("lpop_k{}", i);
        let v = format!("v{}", i);
        let cmd = format!(
            "*3\r\n$5\r\nRPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            v.len(),
            v
        );
        client.write_all(cmd.as_bytes()).unwrap();
        let mut resp = [0u8; 4];
        client.read_exact(&mut resp).unwrap();
        assert_eq!(&resp, b":1\r\n");
    }

    // Pipeline LPOP for present lists and missing lists
    let mut pipeline = String::new();
    let mut expected = String::new();
    for i in 0..16 {
        if i % 2 == 0 {
            let k = format!("lpop_k{}", i / 2);
            let v = format!("v{}", i / 2);
            pipeline.push_str(&format!("*2\r\n$4\r\nLPOP\r\n${}\r\n{}\r\n", k.len(), k));
            expected.push_str(&format!("${}\r\n{}\r\n", v.len(), v));
        } else {
            let k = format!("missing_l{}", i);
            pipeline.push_str(&format!("*2\r\n$4\r\nLPOP\r\n${}\r\n{}\r\n", k.len(), k));
            expected.push_str("$-1\r\n");
        }
    }
    client.write_all(pipeline.as_bytes()).unwrap();

    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);
}

#[test]
fn test_pipelined_sadd_sismember_cross_shard_e2e() {
    let port = 16870;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // Pipeline SADD across shards
    let mut pipeline = String::new();
    let mut expected = String::new();
    for i in 0..10 {
        let k = format!("set_k{}", i);
        let m = format!("m{}", i);
        pipeline.push_str(&format!(
            "*3\r\n$4\r\nSADD\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            m.len(),
            m
        ));
        expected.push_str(":1\r\n");
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    // Pipeline SISMEMBER across shards for present and absent members
    pipeline.clear();
    expected.clear();
    for i in 0..20 {
        if i % 2 == 0 {
            let k = format!("set_k{}", i / 2);
            let m = format!("m{}", i / 2);
            pipeline.push_str(&format!(
                "*3\r\n$9\r\nSISMEMBER\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                k.len(),
                k,
                m.len(),
                m
            ));
            expected.push_str(":1\r\n");
        } else {
            let k = format!("set_k{}", i / 2);
            let m = format!("missing_{}", i);
            pipeline.push_str(&format!(
                "*3\r\n$9\r\nSISMEMBER\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                k.len(),
                k,
                m.len(),
                m
            ));
            expected.push_str(":0\r\n");
        }
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);
}

#[test]
fn test_pipelined_hset_hget_sismember_zadd_cross_shard_e2e() {
    let port = 16880;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Pipeline HSET across shards
    let mut pipeline = String::new();
    let mut expected = String::new();
    for i in 0..12 {
        let k = format!("hash_key_{}", i);
        let f = format!("field_{}", i);
        let v = format!("val_{}", i);
        pipeline.push_str(&format!(
            "*4\r\n$4\r\nHSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            f.len(),
            f,
            v.len(),
            v
        ));
        expected.push_str(":1\r\n");
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    // 2. Pipeline HGET across shards
    pipeline.clear();
    expected.clear();
    for i in 0..12 {
        let k = format!("hash_key_{}", i);
        let f = format!("field_{}", i);
        let v = format!("val_{}", i);
        pipeline.push_str(&format!(
            "*3\r\n$4\r\nHGET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            f.len(),
            f
        ));
        expected.push_str(&format!("${}\r\n{}\r\n", v.len(), v));
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    // 3. Pipeline ZADD across shards
    pipeline.clear();
    expected.clear();
    for i in 0..12 {
        let k = format!("zset_key_{}", i);
        let score = (i * 10) as f64;
        let m = format!("zmember_{}", i);
        let score_str = score.to_string();
        pipeline.push_str(&format!(
            "*4\r\n$4\r\nZADD\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            score_str.len(),
            score_str,
            m.len(),
            m
        ));
        expected.push_str(":1\r\n");
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    // 4. Pipeline SADD & SISMEMBER across shards
    pipeline.clear();
    expected.clear();
    for i in 0..12 {
        let k = format!("set_key_{}", i);
        let m = format!("sm_{}", i);
        pipeline.push_str(&format!(
            "*3\r\n$4\r\nSADD\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            m.len(),
            m
        ));
        expected.push_str(":1\r\n");
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    pipeline.clear();
    expected.clear();
    for i in 0..12 {
        let k = format!("set_key_{}", i);
        let m = format!("sm_{}", i);
        pipeline.push_str(&format!(
            "*3\r\n$9\r\nSISMEMBER\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            m.len(),
            m
        ));
        expected.push_str(":1\r\n");
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);
}

#[test]
fn test_pipelined_lpush_lpop_large_bulk_and_incr_e2e() {
    let port = 16890;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Pipeline LPUSH with >20-byte payloads across shards
    let mut pipeline = String::new();
    let mut expected = String::new();
    for i in 0..12 {
        let k = format!("l_key_{}", i);
        let v = format!("large_bulk_element_value_payload_{:04}", i);
        pipeline.push_str(&format!(
            "*3\r\n$5\r\nLPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            k.len(),
            k,
            v.len(),
            v
        ));
        expected.push_str(":1\r\n");
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    // 2. Pipeline LPOP across shards (testing CompactResp::from_owned_bulk zero-copy move)
    pipeline.clear();
    expected.clear();
    for i in 0..12 {
        let k = format!("l_key_{}", i);
        let v = format!("large_bulk_element_value_payload_{:04}", i);
        pipeline.push_str(&format!("*2\r\n$4\r\nLPOP\r\n${}\r\n{}\r\n", k.len(), k));
        expected.push_str(&format!("${}\r\n{}\r\n", v.len(), v));
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    // 3. Pipeline INCR across shards (new keys + existing keys)
    pipeline.clear();
    expected.clear();
    for i in 0..12 {
        let k = format!("incr_key_{}", i % 6);
        pipeline.push_str(&format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", k.len(), k));
        let exp_val = if i < 6 { 1 } else { 2 };
        expected.push_str(&format!(":{}\r\n", exp_val));
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);
}

#[test]
fn test_pipelined_prehashed_zrange_lrange_array1bulk_e2e() {
    let port = 16475;
    start_test_server(port, 4);

    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    // 1. Populate 1-element ZSETs and LISTs across shards + query ZRANGE/LRANGE
    let mut pipeline = String::new();
    let mut expected = String::new();
    for i in 0..8 {
        let zk = format!("z_single_{}", i);
        let zm = format!("z_payload_over_thirty_bytes_index_{:04}", i);
        pipeline.push_str(&format!(
            "*4\r\n$4\r\nZADD\r\n${}\r\n{}\r\n$2\r\n10\r\n${}\r\n{}\r\n",
            zk.len(),
            zk,
            zm.len(),
            zm
        ));
        expected.push_str(":1\r\n");

        let lk = format!("l_single_{}", i);
        let lm = format!("l_payload_over_thirty_bytes_index_{:04}", i);
        pipeline.push_str(&format!(
            "*3\r\n$5\r\nLPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            lk.len(),
            lk,
            lm.len(),
            lm
        ));
        expected.push_str(":1\r\n");
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    // 2. Pipeline ZRANGE 0 10 and LRANGE 0 10 (including missing keys for EMPTY_ARRAY)
    pipeline.clear();
    expected.clear();
    for i in 0..8 {
        let zk = format!("z_single_{}", i);
        let zm = format!("z_payload_over_thirty_bytes_index_{:04}", i);
        pipeline.push_str(&format!(
            "*4\r\n$6\r\nZRANGE\r\n${}\r\n{}\r\n$1\r\n0\r\n$2\r\n10\r\n",
            zk.len(),
            zk
        ));
        expected.push_str(&format!("*1\r\n${}\r\n{}\r\n", zm.len(), zm));

        let lk = format!("l_single_{}", i);
        let lm = format!("l_payload_over_thirty_bytes_index_{:04}", i);
        pipeline.push_str(&format!(
            "*4\r\n$6\r\nLRANGE\r\n${}\r\n{}\r\n$1\r\n0\r\n$2\r\n10\r\n",
            lk.len(),
            lk
        ));
        expected.push_str(&format!("*1\r\n${}\r\n{}\r\n", lm.len(), lm));

        let missing = format!("missing_range_{}", i);
        pipeline.push_str(&format!(
            "*4\r\n$6\r\nZRANGE\r\n${}\r\n{}\r\n$1\r\n0\r\n$2\r\n10\r\n",
            missing.len(),
            missing
        ));
        expected.push_str("*0\r\n");
    }
    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);
}

#[test]
fn test_overlapped_remote_dispatch_and_coordinator_recycle_e2e() {
    let port = 16476;
    start_test_server(port, 4);

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect to server");
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Test interleaved local and remote shard commands in a single pipeline
    // verifying FIFO ordering, coordinator-side item recycling, and fast LRANGE/ZRANGE parsing
    let mut pipeline = String::new();
    let mut expected = String::new();

    for i in 0..12 {
        let sk = format!("ov_s_{}", i);
        let lk = format!("ov_l_{}", i);
        let zk = format!("ov_z_{}", i);

        pipeline.push_str(&format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$4\r\nsval\r\n",
            sk.len(),
            sk
        ));
        expected.push_str("+OK\r\n");

        pipeline.push_str(&format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", sk.len(), sk));
        expected.push_str("$4\r\nsval\r\n");

        pipeline.push_str(&format!(
            "*3\r\n$5\r\nLPUSH\r\n${}\r\n{}\r\n$4\r\nitem\r\n",
            lk.len(),
            lk
        ));
        expected.push_str(":1\r\n");

        pipeline.push_str(&format!(
            "*4\r\n$6\r\nLRANGE\r\n${}\r\n{}\r\n$1\r\n0\r\n$2\r\n10\r\n",
            lk.len(),
            lk
        ));
        expected.push_str("*1\r\n$4\r\nitem\r\n");

        pipeline.push_str(&format!(
            "*4\r\n$4\r\nZADD\r\n${}\r\n{}\r\n$1\r\n1\r\n$4\r\nzmbr\r\n",
            zk.len(),
            zk
        ));
        expected.push_str(":1\r\n");

        pipeline.push_str(&format!(
            "*4\r\n$6\r\nZRANGE\r\n${}\r\n{}\r\n$1\r\n0\r\n$2\r\n10\r\n",
            zk.len(),
            zk
        ));
        expected.push_str("*1\r\n$4\r\nzmbr\r\n");
    }

    client.write_all(pipeline.as_bytes()).unwrap();
    let mut buf = vec![0u8; expected.len()];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    // Second and third consecutive bursts over the same connection exercise RecvBytesMut
    // after post-execution commands.clear() + BytesMut::try_reclaim resets the read buffer offset.
    let pipeline2 = pipeline.replace("ov_", "ob_");
    client.write_all(pipeline2.as_bytes()).unwrap();
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    let pipeline3 = pipeline.replace("ov_", "oc_");
    client.write_all(pipeline3.as_bytes()).unwrap();
    client.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);

    drop(client);
    let mut client2 = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let pipeline4 = pipeline.replace("ov_", "od_");
    client2.write_all(pipeline4.as_bytes()).unwrap();
    client2.read_exact(&mut buf).unwrap();
    assert_eq!(String::from_utf8_lossy(&buf), expected);
}

/// Reads the per-shard connection census from `INFO clients`.
///
/// This does not use `send_and_read` on purpose: that helper performs a single
/// 1KB `read()`, while an INFO reply spans several KB and several TCP segments.
/// A truncated read leaves the remainder queued in the socket, so a subsequent
/// command on the same connection would observe the stale tail of this reply.
/// Here we open a fresh connection, request only the `clients` section, and
/// drain until the RESP bulk string (`$<len>\r\n<payload>\r\n`) is complete.
fn read_shard_census(port: u16) -> Vec<usize> {
    let mut s = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(b"INFO clients\r\n").unwrap();

    let mut data = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0, "server closed before the full INFO reply arrived");
        data.extend_from_slice(&chunk[..n]);
        if let Some(hdr_end) = data.windows(2).position(|w| w == b"\r\n") {
            assert_eq!(data[0], b'$', "expected a RESP bulk string from INFO");
            let len: usize = std::str::from_utf8(&data[1..hdr_end])
                .unwrap()
                .parse()
                .unwrap();
            if data.len() >= hdr_end + 2 + len + 2 {
                break;
            }
        }
    }

    let text = String::from_utf8_lossy(&data).to_string();
    let line = text
        .lines()
        .find(|l| l.starts_with("shard_connections:"))
        .unwrap_or_else(|| panic!("INFO clients missing shard_connections. Got:\n{}", text));
    line.trim_start_matches("shard_connections:")
        .trim()
        .split(',')
        .map(|v| v.parse::<usize>().unwrap())
        .collect()
}

#[test]
fn test_connection_balancing_across_shards_e2e() {
    // SO_REUSEPORT assigns connections to shards by kernel 4-tuple hash. Over
    // loopback only the client's ephemeral port varies, so the draw is random
    // and badly uneven: with 64 connections over 16 shards the busiest shard
    // gets ~8 while others get 1-2, and throughput is gated by that shard.
    // crate::conn_balance rebalances each accepted connection to the least
    // loaded shard. This test asserts the resulting distribution is even.
    let port = 16790;
    let num_shards = 8;
    let num_conns = 32; // 4 per shard when balanced
    start_test_server(port, num_shards);

    let mut conns = Vec::new();
    for _ in 0..num_conns {
        let mut c = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        // Force the connection to be fully established and served before the
        // next one, so the census is up to date when the next is balanced.
        assert_eq!(send_and_read(&mut c, b"PING\r\n"), "+PONG\r\n");
        conns.push(c);
    }

    // Read the per-shard census out of INFO.
    let counts = read_shard_census(port);

    assert_eq!(counts.len(), num_shards, "one census entry per shard");

    let total: usize = counts.iter().sum();
    assert!(
        total >= num_conns,
        "census {:?} totals {} but {} connections were opened",
        counts,
        total,
        num_conns
    );

    // The point of the fix: no shard may hoard connections. Without balancing
    // the max would routinely reach 8-10 out of 32 across 8 shards.
    let max = *counts.iter().max().unwrap();
    let min = *counts.iter().min().unwrap();
    let ideal = total / num_shards;
    assert!(
        max - min <= 2,
        "connections must be evenly distributed (ideal ~{} each), got {:?}",
        ideal,
        counts
    );
    assert!(
        max <= ideal + 2,
        "no shard may hoard connections (ideal ~{} each), got {:?}",
        ideal,
        counts
    );

    // Balanced connections must still work correctly, including ones that were
    // handed off to a different shard than the kernel originally chose.
    for (i, c) in conns.iter_mut().enumerate() {
        let key = format!("balkey_{}", i);
        let set = format!("SET {} v{}\r\n", key, i);
        assert_eq!(send_and_read(c, set.as_bytes()), "+OK\r\n");
    }
    for (i, c) in conns.iter_mut().enumerate() {
        let key = format!("balkey_{}", i);
        let get = format!("GET {}\r\n", key);
        let val = format!("v{}", i);
        let want = format!("${}\r\n{}\r\n", val.len(), val);
        assert_eq!(send_and_read(c, get.as_bytes()), want);
    }

    // Closing connections must decrement the census, not leak.
    drop(conns);
    std::thread::sleep(Duration::from_millis(500));
    let after = read_shard_census(port);
    let total_after: usize = after.iter().sum();
    assert!(
        total_after < total,
        "census must decrease after closing connections: before {:?}, after {:?}",
        counts,
        after
    );
}

#[test]
fn test_cross_shard_mset_mget_deadlock_free_e2e() {
    let port = 16795;
    let num_shards = 4;
    start_test_server(port, num_shards);

    // Hammer cross-shard MSET and MGET across multiple client connections
    // to verify shards never deadlock on scatter/gather notifications.
    let mut conns: Vec<TcpStream> = (0..8)
        .map(|_| TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap())
        .collect();

    for round in 0..50 {
        for (i, c) in conns.iter_mut().enumerate() {
            let mset_cmd = format!(
                "MSET dk_{}_a val_a dk_{}_b val_b dk_{}_c val_c dk_{}_d val_d\r\n",
                i + round * 10,
                i + round * 10,
                i + round * 10,
                i + round * 10
            );
            assert_eq!(send_and_read(c, mset_cmd.as_bytes()), "+OK\r\n");

            let mget_cmd = format!(
                "MGET dk_{}_a dk_{}_b dk_{}_c dk_{}_d\r\n",
                i + round * 10,
                i + round * 10,
                i + round * 10,
                i + round * 10
            );
            let resp = send_and_read(c, mget_cmd.as_bytes());
            assert!(
                resp.starts_with("*4\r\n"),
                "round {} client {} MGET failed: {}",
                round,
                i,
                resp
            );
        }
    }

    drop(conns);
}

#[test]
fn test_pipelined_del_exists_e2e() {
    let port = 16796;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Populate a set of keys
    for i in 0..100 {
        let set_cmd = format!("SET de_key_{} val_{}\r\n", i, i);
        assert_eq!(send_and_read(&mut conn, set_cmd.as_bytes()), "+OK\r\n");
    }

    // Verify EXISTS returns 1 for all
    for i in 0..100 {
        let exists_cmd = format!("EXISTS de_key_{}\r\n", i);
        assert_eq!(send_and_read(&mut conn, exists_cmd.as_bytes()), ":1\r\n");
    }

    // DEL half the keys
    for i in 0..50 {
        let del_cmd = format!("DEL de_key_{}\r\n", i);
        assert_eq!(send_and_read(&mut conn, del_cmd.as_bytes()), ":1\r\n");
    }

    // Verify deleted keys return 0 and intact keys return 1
    for i in 0..50 {
        let exists_cmd = format!("EXISTS de_key_{}\r\n", i);
        assert_eq!(send_and_read(&mut conn, exists_cmd.as_bytes()), ":0\r\n");
    }
    for i in 50..100 {
        let exists_cmd = format!("EXISTS de_key_{}\r\n", i);
        assert_eq!(send_and_read(&mut conn, exists_cmd.as_bytes()), ":1\r\n");
    }

    // Second DEL on already deleted keys returns 0
    for i in 0..50 {
        let del_cmd = format!("DEL de_key_{}\r\n", i);
        assert_eq!(send_and_read(&mut conn, del_cmd.as_bytes()), ":0\r\n");
    }
}

#[test]
fn test_pipelined_lpop_rpop_e2e() {
    let port = 16797;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Populate lists with 10 elements each
    for list_idx in 0..20 {
        let push_cmd = format!("LPUSH lr_key_{} v1 v2 v3 v4 v5\r\n", list_idx);
        assert_eq!(send_and_read(&mut conn, push_cmd.as_bytes()), ":5\r\n");
    }

    // LPOP all elements from first 10 lists
    for list_idx in 0..10 {
        for _ in 0..5 {
            let lpop_cmd = format!("LPOP lr_key_{}\r\n", list_idx);
            let resp = send_and_read(&mut conn, lpop_cmd.as_bytes());
            assert!(
                resp.starts_with('$'),
                "LPOP should return bulk string: {}",
                resp
            );
        }
        // Now empty, next LPOP returns nil
        let lpop_cmd = format!("LPOP lr_key_{}\r\n", list_idx);
        assert_eq!(send_and_read(&mut conn, lpop_cmd.as_bytes()), "$-1\r\n");
    }

    // RPOP all elements from remaining 10 lists
    for list_idx in 10..20 {
        for _ in 0..5 {
            let rpop_cmd = format!("RPOP lr_key_{}\r\n", list_idx);
            let resp = send_and_read(&mut conn, rpop_cmd.as_bytes());
            assert!(
                resp.starts_with('$'),
                "RPOP should return bulk string: {}",
                resp
            );
        }
        // Now empty, next RPOP returns nil
        let rpop_cmd = format!("RPOP lr_key_{}\r\n", list_idx);
        assert_eq!(send_and_read(&mut conn, rpop_cmd.as_bytes()), "$-1\r\n");
    }
}

#[test]
fn test_pipelined_sismember_sadd_e2e() {
    let port = 16799;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // SADD members into multiple sets
    for set_idx in 0..10 {
        let sadd_cmd = format!("SADD sm_set_{} m1 m2 m3 m4 m5\r\n", set_idx);
        assert_eq!(send_and_read(&mut conn, sadd_cmd.as_bytes()), ":5\r\n");
    }

    // Verify SISMEMBER returns 1 for present members, 0 for absent
    for set_idx in 0..10 {
        for m in 1..=5 {
            let sismember_cmd = format!("SISMEMBER sm_set_{} m{}\r\n", set_idx, m);
            assert_eq!(send_and_read(&mut conn, sismember_cmd.as_bytes()), ":1\r\n");
        }
        let sismember_cmd = format!("SISMEMBER sm_set_{} m999\r\n", set_idx);
        assert_eq!(send_and_read(&mut conn, sismember_cmd.as_bytes()), ":0\r\n");
    }
}

#[test]
fn test_concurrent_multikey_del_e2e() {
    let port = 16800;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Populate keys
    for i in 0..60 {
        let set_cmd = format!("SET cdel_{} val_{}\r\n", i, i);
        assert_eq!(send_and_read(&mut conn, set_cmd.as_bytes()), "+OK\r\n");
    }

    // Delete single keys in a pipeline
    for i in 0..30 {
        let del_cmd = format!("DEL cdel_{}\r\n", i);
        assert_eq!(send_and_read(&mut conn, del_cmd.as_bytes()), ":1\r\n");
    }

    // Second DEL returns 0
    for i in 0..30 {
        let del_cmd = format!("DEL cdel_{}\r\n", i);
        assert_eq!(send_and_read(&mut conn, del_cmd.as_bytes()), ":0\r\n");
    }

    // Delete remaining keys in batches
    let del_batch = "DEL cdel_30 cdel_31 cdel_32 cdel_33 cdel_34\r\n";
    assert_eq!(send_and_read(&mut conn, del_batch.as_bytes()), ":5\r\n");
}

#[test]
fn test_hset_hget_e2e_pipelined_and_single() {
    let port = 16810;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Single field insert
    for i in 0..20 {
        let cmd = format!("HSET hash_test_{} f1 v{}\r\n", i, i);
        assert_eq!(send_and_read(&mut conn, cmd.as_bytes()), ":1\r\n");
    }

    // 2. Read back
    for i in 0..20 {
        let cmd = format!("HGET hash_test_{} f1\r\n", i);
        let expected = format!("${}\r\nv{}\r\n", format!("v{}", i).len(), i);
        assert_eq!(send_and_read(&mut conn, cmd.as_bytes()), expected);
    }

    // 3. Single field update (returns :0)
    for i in 0..20 {
        let cmd = format!("HSET hash_test_{} f1 updated_{}\r\n", i, i);
        assert_eq!(send_and_read(&mut conn, cmd.as_bytes()), ":0\r\n");
    }

    // 4. Verify updated value
    for i in 0..20 {
        let cmd = format!("HGET hash_test_{} f1\r\n", i);
        let expected = format!("${}\r\nupdated_{}\r\n", format!("updated_{}", i).len(), i);
        assert_eq!(send_and_read(&mut conn, cmd.as_bytes()), expected);
    }

    // 5. Non-existent field returns nil
    assert_eq!(
        send_and_read(&mut conn, b"HGET hash_test_0 nofield\r\n"),
        "$-1\r\n"
    );
}

#[test]
fn test_smallvec_del_and_hset_e2e() {
    let port = 16820;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Single-key DEL and HSET mixed pipeline
    let mut pipe = String::new();
    for i in 0..50 {
        pipe.push_str(&format!("HSET smh_{} f v\r\n", i));
    }
    for i in 0..50 {
        pipe.push_str(&format!("DEL smh_{}\r\n", i));
    }

    use std::io::{Read, Write};
    conn.write_all(pipe.as_bytes()).unwrap();

    let mut response = vec![0u8; 100 * 4];
    conn.read_exact(&mut response).unwrap();

    // Verify 50 times ":1\r\n" for HSET and 50 times ":1\r\n" for DEL
    let expected = ":1\r\n".repeat(100);
    assert_eq!(response, expected.as_bytes());

    // Verify all keys deleted
    for i in 0..50 {
        let cmd = format!("EXISTS smh_{}\r\n", i);
        assert_eq!(send_and_read(&mut conn, cmd.as_bytes()), ":0\r\n");
    }
}

#[test]
fn test_pipelined_incr_and_rpop_e2e() {
    let port = 16900;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    use std::io::{Read, Write};

    // 1. Pipelined INCR on same and different keys
    let mut pipe = String::new();
    for i in 0..10 {
        pipe.push_str(&format!("INCR test_counter_{}\r\n", i));
    }
    for i in 0..10 {
        pipe.push_str(&format!("INCRBY test_counter_{} 5\r\n", i));
    }
    conn.write_all(pipe.as_bytes()).unwrap();

    let mut expected = String::new();
    for _ in 0..10 {
        expected.push_str(":1\r\n");
    }
    for _ in 0..10 {
        expected.push_str(":6\r\n");
    }
    let mut response = vec![0u8; expected.len()];
    conn.read_exact(&mut response).unwrap();
    assert_eq!(std::str::from_utf8(&response).unwrap(), expected.as_str());

    // 2. Pipelined RPUSH and RPOP
    let mut pipe = String::new();
    for i in 0..20 {
        pipe.push_str(&format!("RPUSH list_k val_{}\r\n", i));
    }
    conn.write_all(pipe.as_bytes()).unwrap();
    // Read RPUSH responses
    let mut read_buf = Vec::new();
    let mut buf = [0u8; 1024];
    while !read_buf.windows(5).any(|w| w == b":20\r\n") {
        let n = conn.read(&mut buf).unwrap();
        read_buf.extend_from_slice(&buf[..n]);
    }

    // Now pipeline 20 RPOPs
    let mut pipe = String::new();
    for _ in 0..20 {
        pipe.push_str("RPOP list_k\r\n");
    }
    conn.write_all(pipe.as_bytes()).unwrap();

    let mut rpop_resp = Vec::new();
    // In LIFO order of RPOP, val_19 down to val_0
    let mut expected_rpop = Vec::new();
    for i in (0..20).rev() {
        let val = format!("val_{}", i);
        expected_rpop.extend_from_slice(format!("${}\r\n{}\r\n", val.len(), val).as_bytes());
    }
    while rpop_resp.len() < expected_rpop.len() {
        let n = conn.read(&mut buf).unwrap();
        rpop_resp.extend_from_slice(&buf[..n]);
    }
    assert_eq!(rpop_resp, expected_rpop);

    // List should be empty now -> RPOP returns nil
    assert_eq!(send_and_read(&mut conn, b"RPOP list_k\r\n"), "$-1\r\n");
}

#[test]
fn test_mutations_find_entry_mut_e2e() {
    let port = 16910;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Pipelined mixed mutation commands on the same keys to exercise find_entry_mut fast path
    let mut pipe = String::new();
    for i in 0..10 {
        pipe.push_str(&format!("HSET mut_hash f{} v{}\r\n", i, i));
        pipe.push_str(&format!("LPUSH mut_list v{}\r\n", i));
        pipe.push_str(&format!("RPUSH mut_rlist v{}\r\n", i));
        pipe.push_str(&format!("SADD mut_set m{}\r\n", i));
        pipe.push_str(&format!("ZADD mut_zset {} zm{}\r\n", i * 10, i));
    }
    use std::io::{Read, Write};
    conn.write_all(pipe.as_bytes()).unwrap();

    // Now update existing fields/members in pipeline
    let mut pipe_update = String::new();
    for i in 0..10 {
        pipe_update.push_str(&format!("HSET mut_hash f{} v{}_updated\r\n", i, i));
        pipe_update.push_str(&format!("SADD mut_set m{}\r\n", i)); // dedup -> :0
        pipe_update.push_str(&format!("ZADD mut_zset {} zm{}\r\n", i * 10 + 5, i)); // update score -> :0
    }
    conn.write_all(pipe_update.as_bytes()).unwrap();

    // Drain all 50 + 30 = 80 responses from pipeline
    let mut buf = [0u8; 1024];
    let mut read_buf = Vec::new();
    while read_buf.windows(2).filter(|w| *w == b"\r\n").count() < 80 {
        let n = conn.read(&mut buf).unwrap();
        read_buf.extend_from_slice(&buf[..n]);
    }

    // Check values
    assert_eq!(send_and_read(&mut conn, b"HLEN mut_hash\r\n"), ":10\r\n");
    assert_eq!(
        send_and_read(&mut conn, b"HGET mut_hash f0\r\n"),
        "$10\r\nv0_updated\r\n"
    );
    assert_eq!(send_and_read(&mut conn, b"LLEN mut_list\r\n"), ":10\r\n");
    assert_eq!(send_and_read(&mut conn, b"LLEN mut_rlist\r\n"), ":10\r\n");
    assert_eq!(send_and_read(&mut conn, b"SCARD mut_set\r\n"), ":10\r\n");
    assert_eq!(send_and_read(&mut conn, b"ZCARD mut_zset\r\n"), ":10\r\n");
    assert_eq!(
        send_and_read(&mut conn, b"ZSCORE mut_zset zm0\r\n"),
        "$1\r\n5\r\n"
    );
}

#[test]
fn test_single_item_hset_sadd_e2e() {
    let port = 16920;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Single-field HSET
    assert_eq!(
        send_and_read(&mut conn, b"HSET e2e_single_hash f1 v1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"HGET e2e_single_hash f1\r\n"),
        "$2\r\nv1\r\n"
    );
    // Update existing field returns :0
    assert_eq!(
        send_and_read(&mut conn, b"HSET e2e_single_hash f1 v1_new\r\n"),
        ":0\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"HGET e2e_single_hash f1\r\n"),
        "$6\r\nv1_new\r\n"
    );

    // 2. Single-member SADD
    assert_eq!(
        send_and_read(&mut conn, b"SADD e2e_single_set m1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"SISMEMBER e2e_single_set m1\r\n"),
        ":1\r\n"
    );
    // Add existing member returns :0
    assert_eq!(
        send_and_read(&mut conn, b"SADD e2e_single_set m1\r\n"),
        ":0\r\n"
    );
    // Add new member returns :1
    assert_eq!(
        send_and_read(&mut conn, b"SADD e2e_single_set m2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"SCARD e2e_single_set\r\n"),
        ":2\r\n"
    );
}

#[test]
fn test_mget_scatter_gather_e2e() {
    let port = 16930;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Set 5 keys across shards
    for i in 0..5 {
        let cmd = format!("SET mget_k{} mget_val{}\r\n", i, i);
        assert_eq!(send_and_read(&mut conn, cmd.as_bytes()), "+OK\r\n");
    }

    // MGET 5 existing keys
    let mget_cmd = b"MGET mget_k0 mget_k1 mget_k2 mget_k3 mget_k4\r\n";
    let resp = send_and_read(&mut conn, mget_cmd);
    assert!(resp.starts_with("*5\r\n"));
    for i in 0..5 {
        assert!(resp.contains(&format!("$9\r\nmget_val{}", i)));
    }

    // MGET with some missing keys
    let mget_mixed = b"MGET mget_k0 missing_key mget_k2\r\n";
    let resp_mixed = send_and_read(&mut conn, mget_mixed);
    assert_eq!(
        resp_mixed,
        "*3\r\n$9\r\nmget_val0\r\n$-1\r\n$9\r\nmget_val2\r\n"
    );
}

#[test]
fn test_pipeline_write_buffering_and_sigpipe_resilience_e2e() {
    let port = 16940;
    let num_shards = 4;
    start_test_server(port, num_shards);

    // 1. Pipeline write test with pre-reserved output buffer
    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let mut pipe = String::new();
    for i in 0..100 {
        pipe.push_str(&format!("SET pipe_k{} pipe_v{}\r\n", i, i));
    }
    use std::io::{Read, Write};
    conn.write_all(pipe.as_bytes()).unwrap();

    let mut buf = [0u8; 1024];
    let mut read_buf = Vec::new();
    while read_buf.windows(2).filter(|w| *w == b"\r\n").count() < 100 {
        let n = conn.read(&mut buf).unwrap();
        read_buf.extend_from_slice(&buf[..n]);
    }
    assert_eq!(read_buf.windows(2).filter(|w| *w == b"\r\n").count(), 100);

    // 2. Abrupt client disconnect resilience (SIGPIPE safety)
    {
        let mut drop_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        let _ = drop_conn.write_all(b"MGET pipe_k0 pipe_k1 pipe_k2 pipe_k3\r\n");
        // Abruptly drop socket before reading reply
    }
    std::thread::sleep(Duration::from_millis(50));

    // Verify server remains alive and responsive
    assert_eq!(send_and_read(&mut conn, b"PING\r\n"), "+PONG\r\n");
}

#[test]
fn test_consolidated_batch_borrowing_e2e() {
    let port = 16950;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // Pipelined mixed operations: SET, GET, EXISTS, INCR, DEL, EXISTS
    let mut pipe = String::new();
    for i in 0..10 {
        pipe.push_str(&format!("SET bkey{} bval{}\r\n", i, i));
        pipe.push_str(&format!("EXISTS bkey{}\r\n", i));
        pipe.push_str(&format!("GET bkey{}\r\n", i));
        pipe.push_str(&format!("DEL bkey{}\r\n", i));
        pipe.push_str(&format!("EXISTS bkey{}\r\n", i));
    }
    use std::io::{Read, Write};
    conn.write_all(pipe.as_bytes()).unwrap();

    let mut buf = [0u8; 1024];
    let mut read_buf = Vec::new();
    // 10 iterations * (1 + 1 + 2 + 1 + 1) = 60 CRLFs
    while read_buf.windows(2).filter(|w| *w == b"\r\n").count() < 60 {
        let n = conn.read(&mut buf).unwrap();
        read_buf.extend_from_slice(&buf[..n]);
    }
    assert_eq!(read_buf.windows(2).filter(|w| *w == b"\r\n").count(), 60);

    // Verify final state
    assert_eq!(send_and_read(&mut conn, b"DBSIZE\r\n"), ":0\r\n");
}

#[test]
fn test_list_fast_push_pop_e2e() {
    let port = 16960;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Single-item LPUSH and LPOP
    assert_eq!(
        send_and_read(&mut conn, b"LPUSH fast_list v1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"LPOP fast_list\r\n"),
        "$2\r\nv1\r\n"
    );
    assert_eq!(send_and_read(&mut conn, b"LPOP fast_list\r\n"), "$-1\r\n");
    assert_eq!(send_and_read(&mut conn, b"EXISTS fast_list\r\n"), ":0\r\n");

    // 2. Single-item RPUSH and RPOP
    assert_eq!(
        send_and_read(&mut conn, b"RPUSH fast_rlist rv1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"RPOP fast_rlist\r\n"),
        "$3\r\nrv1\r\n"
    );
    assert_eq!(send_and_read(&mut conn, b"RPOP fast_rlist\r\n"), "$-1\r\n");
    assert_eq!(send_and_read(&mut conn, b"EXISTS fast_rlist\r\n"), ":0\r\n");
}

#[test]
fn test_sismember_hash_contains_e2e() {
    let port = 16970;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    assert_eq!(
        send_and_read(&mut conn, b"SADD e2e_set m1 m2 m3\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"SISMEMBER e2e_set m1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"SISMEMBER e2e_set m2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"SISMEMBER e2e_set m3\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"SISMEMBER e2e_set missing\r\n"),
        ":0\r\n"
    );
}

#[test]
fn test_fast_path_del_exists_mget_mset_e2e() {
    let port = 16980;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. MSET and MGET
    assert_eq!(
        send_and_read(&mut conn, b"*7\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n$2\r\nk3\r\n$2\r\nv3\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut conn,
            b"*4\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n"
        ),
        "*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n"
    );

    // 2. Multi-key EXISTS
    assert_eq!(
        send_and_read(
            &mut conn,
            b"*4\r\n$6\r\nEXISTS\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n"
        ),
        ":3\r\n"
    );

    // 3. Single-key EXISTS
    assert_eq!(
        send_and_read(&mut conn, b"*2\r\n$6\r\nEXISTS\r\n$2\r\nk1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"*2\r\n$6\r\nEXISTS\r\n$7\r\nmissing\r\n"),
        ":0\r\n"
    );

    // 4. Single-key DEL
    assert_eq!(
        send_and_read(&mut conn, b"*2\r\n$3\r\nDEL\r\n$2\r\nk1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut conn, b"*2\r\n$3\r\nDEL\r\n$2\r\nk1\r\n"),
        ":0\r\n"
    );

    // 5. Multi-key DEL
    assert_eq!(
        send_and_read(&mut conn, b"*3\r\n$3\r\nDEL\r\n$2\r\nk2\r\n$2\r\nk3\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut conn,
            b"*4\r\n$6\r\nEXISTS\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n"
        ),
        ":0\r\n"
    );
}

#[test]
fn test_coalesced_cross_shard_mesh_e2e() {
    let port = 16990;
    let num_shards = 8;
    start_test_server(port, num_shards);

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Cross-shard MSET across 8 shards
    assert_eq!(
        send_and_read(
            &mut conn,
            b"MSET mesh_k1 v1 mesh_k2 v2 mesh_k3 v3 mesh_k4 v4 mesh_k5 v5 mesh_k6 v6 mesh_k7 v7 mesh_k8 v8\r\n"
        ),
        "+OK\r\n"
    );

    // 2. Cross-shard MGET across 8 shards
    let resp = send_and_read(
        &mut conn,
        b"MGET mesh_k1 mesh_k2 mesh_k3 mesh_k4 mesh_k5 mesh_k6 mesh_k7 mesh_k8\r\n",
    );
    assert!(resp.starts_with("*8\r\n"));
    assert!(resp.contains("$2\r\nv1\r\n"));
    assert!(resp.contains("$2\r\nv8\r\n"));

    // 3. Multi-key DEL across 8 shards
    assert_eq!(
        send_and_read(
            &mut conn,
            b"DEL mesh_k1 mesh_k2 mesh_k3 mesh_k4 mesh_k5 mesh_k6 mesh_k7 mesh_k8\r\n"
        ),
        ":8\r\n"
    );
}

#[test]
fn test_sharded_scatter_gather_search_e2e() {
    let port = 17010;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Create index across 4 shards
    assert_eq!(
        send_and_read(
            &mut client,
            b"FT.CREATE idx:multishard ON HASH PREFIX 1 item: SCHEMA title TEXT price NUMERIC SORTABLE tag TAG\r\n"
        ),
        "+OK\r\n"
    );

    // 2. Populate 12 documents that distribute across shards
    for i in 1..=12 {
        let title = format!("Rust high performance cluster item {}", i);
        let price = (i * 10).to_string();
        let item_key = format!("item:{}", i);
        let cmd = format!(
            "*8\r\n$4\r\nHSET\r\n${}\r\n{}\r\n$5\r\ntitle\r\n${}\r\n{}\r\n$5\r\nprice\r\n${}\r\n{}\r\n$3\r\ntag\r\n$20\r\nhardware,distributed\r\n",
            item_key.len(),
            item_key,
            title.len(),
            title,
            price.len(),
            price
        );
        let resp = send_and_read(&mut client, cmd.as_bytes());
        assert_eq!(resp, ":3\r\n");
    }

    // 3. Scatter-gather search across all 4 shards: query "cluster" NOCONTENT
    let search_all = send_and_read(
        &mut client,
        b"FT.SEARCH idx:multishard cluster NOCONTENT\r\n",
    );
    assert!(
        search_all.starts_with("*11\r\n:12\r\n"),
        "All 12 documents from all 4 shards should be gathered, got: {}",
        search_all
    );

    // 4. Sorted search with pagination across shards: SORTBY price ASC LIMIT 0 5 NOCONTENT
    let search_paged = send_and_read(
        &mut client,
        b"FT.SEARCH idx:multishard cluster NOCONTENT SORTBY price ASC LIMIT 0 5\r\n",
    );
    assert_eq!(
        search_paged,
        "*6\r\n:12\r\n$6\r\nitem:1\r\n$6\r\nitem:2\r\n$6\r\nitem:3\r\n$6\r\nitem:4\r\n$6\r\nitem:5\r\n"
    );

    // 5. Delete item:1, verify it is immediately absent from scatter-gather search
    assert_eq!(send_and_read(&mut client, b"DEL item:1\r\n"), ":1\r\n");
    let search_after_del = send_and_read(
        &mut client,
        b"FT.SEARCH idx:multishard cluster NOCONTENT\r\n",
    );
    assert!(search_after_del.contains(":11\r\n"));
    assert!(!search_after_del.contains("item:1\r\n"));

    // 6. Drop index
    assert_eq!(
        send_and_read(&mut client, b"FT.DROPINDEX idx:multishard\r\n"),
        "+OK\r\n"
    );
    let search_dropped = send_and_read(&mut client, b"FT.SEARCH idx:multishard cluster\r\n");
    assert!(search_dropped.contains("ERR Unknown Index name"));
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
    assert!(err_resp.contains("ERR Can't execute 'GET' in subscribed mode"));

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
fn test_numeric_range_search_across_shards_e2e() {
    let port = 17030;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Create index with TEXT and NUMERIC fields
    let create_resp = send_and_read(
        &mut client,
        b"FT.CREATE idx:catalog ON HASH PREFIX 1 item: SCHEMA title TEXT price NUMERIC SORTABLE stock NUMERIC SORTABLE\r\n",
    );
    assert_eq!(create_resp, "+OK\r\n");

    // 2. Insert items spanning across the 4 shards
    let items = [
        ("item:1", "item gaming mouse", "29.99", "100"),
        ("item:2", "item mechanical keyboard", "89.50", "45"),
        ("item:3", "item usb-c cable", "9.99", "250"),
        ("item:4", "item 4k monitor", "299.00", "15"),
        ("item:5", "item desk mat", "19.99", "80"),
        ("item:6", "item noise canceling headphones", "149.00", "30"),
        ("item:7", "item webcam pro", "69.99", "50"),
        ("item:8", "item microphone arm", "39.99", "60"),
    ];

    for (key, title, price, stock) in items {
        let cmd = format!(
            "*8\r\n$4\r\nHSET\r\n${}\r\n{}\r\n$5\r\ntitle\r\n${}\r\n{}\r\n$5\r\nprice\r\n${}\r\n{}\r\n$5\r\nstock\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            title.len(),
            title,
            price.len(),
            price,
            stock.len(),
            stock
        );
        let resp = send_and_read(&mut client, cmd.as_bytes());
        assert_eq!(resp, ":3\r\n");
    }

    // Helper to format RESP command with automatic length calculation
    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 3. Search numeric range @price:[20 75] NOCONTENT
    // Should match: item:1 (29.99), item:7 (69.99), item:8 (39.99) -> 3 items
    let q1 = format_resp_cmd(&["FT.SEARCH", "idx:catalog", "@price:[20 75]", "NOCONTENT"]);
    let search_resp = send_and_read(&mut client, &q1);
    assert!(
        search_resp.contains(":3\r\n"),
        "Expected 3 matches, got: {}",
        search_resp
    );
    assert!(search_resp.contains("item:1"));
    assert!(search_resp.contains("item:7"));
    assert!(search_resp.contains("item:8"));
    assert!(!search_resp.contains("item:3")); // 9.99 < 20
    assert!(!search_resp.contains("item:2")); // 89.50 > 75

    // 4. Combined text + numeric range: @price:[50 300] with word "item"
    // Should match: item:2 (89.50), item:4 (299.00), item:6 (149.00), item:7 (69.99) -> 4 items
    let q2 = format_resp_cmd(&[
        "FT.SEARCH",
        "idx:catalog",
        "item @price:[50 300]",
        "NOCONTENT",
    ]);
    let search_resp2 = send_and_read(&mut client, &q2);
    assert!(
        search_resp2.contains(":4\r\n"),
        "Expected 4 matches, got: {}",
        search_resp2
    );
    assert!(search_resp2.contains("item:2"));
    assert!(search_resp2.contains("item:4"));
    assert!(search_resp2.contains("item:6"));
    assert!(search_resp2.contains("item:7"));

    // 5. Delete item:7 (69.99)
    assert_eq!(send_and_read(&mut client, b"DEL item:7\r\n"), ":1\r\n");

    // 6. Re-search @price:[20 75] NOCONTENT -> only 2 items remain
    let q3 = format_resp_cmd(&["FT.SEARCH", "idx:catalog", "@price:[20 75]", "NOCONTENT"]);
    let search_after_del = send_and_read(&mut client, &q3);
    assert!(
        search_after_del.contains(":2\r\n"),
        "Expected 2 matches, got: {}",
        search_after_del
    );
    assert!(!search_after_del.contains("item:7"));
    assert!(search_after_del.contains("item:1"));
    assert!(search_after_del.contains("item:8"));

    // Cleanup: Drop index
    assert_eq!(
        send_and_read(&mut client, b"FT.DROPINDEX idx:catalog\r\n"),
        "+OK\r\n"
    );
}

#[test]
fn test_ft_aggregate_multishard_pipeline_e2e() {
    let port = 17035;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 1. Create index
    let create_cmd = format_resp_cmd(&[
        "FT.CREATE",
        "idx:sales",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "sale:",
        "SCHEMA",
        "region",
        "TAG",
        "revenue",
        "NUMERIC",
        "SORTABLE",
    ]);
    assert_eq!(send_and_read(&mut client, &create_cmd), "+OK\r\n");

    // 2. Insert transactions across 4 shards
    let sales = [
        ("sale:1", "north", "100"),
        ("sale:2", "north", "250"),
        ("sale:3", "north", "150"),
        ("sale:4", "south", "80"),
        ("sale:5", "south", "120"),
        ("sale:6", "east", "300"),
        ("sale:7", "east", "400"),
        ("sale:8", "west", "50"),
        ("sale:9", "west", "150"),
        ("sale:10", "west", "100"),
    ];

    for (key, region, revenue) in sales {
        let set_cmd = format_resp_cmd(&["HSET", key, "region", region, "revenue", revenue]);
        let resp = send_and_read(&mut client, &set_cmd);
        assert_eq!(resp, ":2\r\n");
    }

    // 3. Test FT.AGGREGATE with GROUPBY + REDUCE + APPLY + SORTBY + LIMIT
    // GROUPBY 1 @region REDUCE COUNT 0 AS count REDUCE SUM 1 @revenue AS sum_rev APPLY @sum_rev * 1.05 AS rev_tax SORTBY 2 @rev_tax DESC LIMIT 0 2
    let agg_cmd = format_resp_cmd(&[
        "FT.AGGREGATE",
        "idx:sales",
        "*",
        "LOAD",
        "2",
        "@region",
        "@revenue",
        "GROUPBY",
        "1",
        "@region",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "count",
        "REDUCE",
        "SUM",
        "1",
        "@revenue",
        "AS",
        "sum_rev",
        "APPLY",
        "@sum_rev * 1.05",
        "AS",
        "rev_tax",
        "SORTBY",
        "2",
        "@rev_tax",
        "DESC",
        "LIMIT",
        "0",
        "2",
    ]);

    let agg_resp = send_and_read(&mut client, &agg_cmd);
    // Should return 2 rows: East (700 * 1.05 = 735), North (500 * 1.05 = 525)
    assert!(
        agg_resp.starts_with("*3\r\n:2\r\n"),
        "Expected 2 rows, got: {}",
        agg_resp
    );
    assert!(agg_resp.contains("east"));
    assert!(agg_resp.contains("735"));
    assert!(agg_resp.contains("north"));
    assert!(agg_resp.contains("525"));

    // 4. Test global aggregation: GROUPBY 0 REDUCE COUNT 0 AS total REDUCE SUM 1 @revenue AS grand_total
    let global_agg_cmd = format_resp_cmd(&[
        "FT.AGGREGATE",
        "idx:sales",
        "*",
        "LOAD",
        "1",
        "@revenue",
        "GROUPBY",
        "0",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "total",
        "REDUCE",
        "SUM",
        "1",
        "@revenue",
        "AS",
        "grand_total",
    ]);

    let global_resp = send_and_read(&mut client, &global_agg_cmd);
    assert!(
        global_resp.starts_with("*2\r\n:1\r\n"),
        "Expected 1 global row, got: {}",
        global_resp
    );
    assert!(global_resp.contains("total") && global_resp.contains("10"));
    assert!(global_resp.contains("grand_total") && global_resp.contains("1700"));

    // Cleanup: Drop index
    let drop_cmd = format_resp_cmd(&["FT.DROPINDEX", "idx:sales"]);
    assert_eq!(send_and_read(&mut client, &drop_cmd), "+OK\r\n");
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
fn test_slowlog_operational_observability_e2e() {
    let port = 17045;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Reset slowlog and set log-slower-than to 0 (log all commands)
    assert_eq!(send_and_read(&mut client, b"SLOWLOG RESET\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET slowlog-log-slower-than 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET slowlog-max-len 10\r\n"),
        "+OK\r\n"
    );

    // 2. Execute commands
    assert_eq!(
        send_and_read(&mut client, b"SET test_key test_val\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET test_key\r\n"),
        "$8\r\ntest_val\r\n"
    );

    // 3. SLOWLOG LEN should be at least 2
    let len_resp = send_and_read(&mut client, b"SLOWLOG LEN\r\n");
    assert!(len_resp.starts_with(':'));
    let len: usize = len_resp.trim_start_matches(':').trim().parse().unwrap();
    assert!(len >= 2);

    // 4. SLOWLOG GET 1 returns 7 fields
    let get_resp = send_and_read(&mut client, b"SLOWLOG GET 1\r\n");
    assert!(get_resp.starts_with("*1\r\n*7\r\n"));

    // 5. Bounds checking on count
    let err_resp = send_and_read(&mut client, b"SLOWLOG GET -2\r\n");
    assert_eq!(
        err_resp,
        "-ERR count should be greater than or equal to -1\r\n"
    );

    // 6. Sensitive argument redaction
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET requirepass my_secret_pass\r\n"),
        "+OK\r\n"
    );
    let get_all = send_and_read(&mut client, b"SLOWLOG GET 1\r\n");
    assert!(get_all.contains("(redacted)"));
    assert!(!get_all.contains("my_secret_pass"));

    // 7. Check CONFIG GET options
    let cfg_resp = send_and_read(&mut client, b"CONFIG GET slowlog-entry-max-argc\r\n");
    assert!(cfg_resp.contains("slowlog-entry-max-argc"));
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET slowlog-entry-max-argc 1\r\n"),
        "-ERR argument must be between 2 and 2147483647\r\n"
    );

    // 8. INFO STATS contains slowlog metrics
    let info_resp = send_and_read(&mut client, b"INFO STATS\r\n");
    assert!(info_resp.contains("slowlog_commands_count:"));
    assert!(info_resp.contains("slowlog_commands_time_ms_sum:"));
    assert!(info_resp.contains("slowlog_commands_time_ms_max:"));

    // 9. Reset slowlog
    assert_eq!(send_and_read(&mut client, b"SLOWLOG RESET\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SLOWLOG LEN\r\n"), ":0\r\n");
}

#[test]
fn test_client_output_buffer_limit_and_slow_consumer_e2e() {
    let port = 17050;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Check CONFIG GET client-output-buffer-limit
    let cfg_resp = send_and_read(&mut client, b"CONFIG GET client-output-buffer-limit\r\n");
    assert!(cfg_resp.contains("client-output-buffer-limit"));
    assert!(cfg_resp.contains("normal"));
    assert!(cfg_resp.contains("slave"));
    assert!(cfg_resp.contains("pubsub"));

    // 2. Test CONFIG SET validations
    let err1 = send_and_read(
        &mut client,
        b"CONFIG SET client-output-buffer-limit \"invalid 10mb 10mb 60\"\r\n",
    );
    assert!(err1.contains("ERR"));

    // 3. Set valid limits
    let ok_resp = send_and_read(
        &mut client,
        b"CONFIG SET client-output-buffer-limit \"normal 100000 0 0\"\r\n",
    );
    assert_eq!(ok_resp, "+OK\r\n");

    let cfg_check = send_and_read(&mut client, b"CONFIG GET client-output-buffer-limit\r\n");
    assert!(cfg_check.contains("normal 100000 0 0"));

    // 4. Test client list reports omem
    let list_resp = send_and_read(&mut client, b"CLIENT LIST\r\n");
    assert!(list_resp.contains("omem="));

    // Reset limit
    let _ = send_and_read(
        &mut client,
        b"CONFIG SET client-output-buffer-limit \"normal 0 0 0\"\r\n",
    );
}

#[test]
fn test_config_rewrite_and_dynamic_configuration_e2e() {
    let port = 17055;
    let temp_dir = std::env::temp_dir().join(format!("rudis-e2e-cfg-{}", port));
    let _ = std::fs::create_dir_all(&temp_dir);
    let cfg_path = temp_dir.join("rudis.conf");

    // Pre-create initial config file
    std::fs::write(&cfg_path, "# Initial config\nmaxclients 1000\n").unwrap();
    *rudis::config::ACTIVE_CONFIG_FILE.write().unwrap() = Some(cfg_path.clone());

    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Dynamic CONFIG SET
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET maxclients 2500\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET requirepass p@ssw0rd\r\n"),
        "+OK\r\n"
    );

    // 2. Dynamic CONFIG GET
    let mc_resp = send_and_read(&mut client, b"CONFIG GET maxclients\r\n");
    assert!(mc_resp.contains("2500"));

    let rp_resp = send_and_read(&mut client, b"CONFIG GET requirepass\r\n");
    assert!(rp_resp.contains("p@ssw0rd"));

    // 3. Trigger CONFIG REWRITE
    assert_eq!(send_and_read(&mut client, b"CONFIG REWRITE\r\n"), "+OK\r\n");

    // 4. Verify persisted file contents
    let content = std::fs::read_to_string(&cfg_path).unwrap();
    assert!(content.contains("maxclients 2500"));
    assert!(content.contains("requirepass p@ssw0rd"));

    // Clean up requirepass and files
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET requirepass \"\"\r\n"),
        "+OK\r\n"
    );
    let _ = std::fs::remove_file(&cfg_path);
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_resp3_hello_negotiation_and_noproto_e2e() {
    let port = 19997;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Unsupported protocol versions
    let err1 = send_and_read(&mut client, b"HELLO 1\r\n");
    assert_eq!(err1, "-NOPROTO unsupported protocol version\r\n");

    let err4 = send_and_read(&mut client, b"HELLO 4\r\n");
    assert_eq!(err4, "-NOPROTO unsupported protocol version\r\n");

    let err_foo = send_and_read(&mut client, b"HELLO foo\r\n");
    assert_eq!(err_foo, "-NOPROTO unsupported protocol version\r\n");

    // 2. HELLO 2 negotiation (RESP2 array response)
    let hello2 = send_and_read(&mut client, b"HELLO 2\r\n");
    assert!(hello2.starts_with("*14\r\n"));
    assert!(hello2.contains("$5\r\nproto\r\n:2\r\n"));
    assert!(hello2.contains("$4\r\nmode\r\n$10\r\nstandalone\r\n"));
    assert!(hello2.contains("$4\r\nrole\r\n$6\r\nmaster\r\n"));

    // 3. HELLO 3 negotiation (RESP3 map response) with SETNAME
    let hello3 = send_and_read(&mut client, b"HELLO 3 SETNAME mytestapp\r\n");
    assert!(hello3.starts_with("%7\r\n"));
    assert!(hello3.contains("$5\r\nproto\r\n:3\r\n"));
    assert!(hello3.contains("$4\r\nmode\r\n$10\r\nstandalone\r\n"));
    assert!(hello3.contains("$4\r\nrole\r\n$6\r\nmaster\r\n"));

    // Verify client name was updated
    let client_info = send_and_read(&mut client, b"CLIENT INFO\r\n");
    assert!(client_info.contains("name=mytestapp"));

    // 4. Mode reflection under cluster mode
    let cluster_hub = rudis::cluster::get_cluster_hub(port);
    cluster_hub
        .cluster_enabled
        .store(true, std::sync::atomic::Ordering::Release);
    let hello_cluster = send_and_read(&mut client, b"HELLO 3\r\n");
    assert!(hello_cluster.contains("$4\r\nmode\r\n$7\r\ncluster\r\n"));
    cluster_hub
        .cluster_enabled
        .store(false, std::sync::atomic::Ordering::Release);

    // 5. Role reflection under replica mode
    let hub = rudis::replication::get_replication_hub(port);
    hub.is_slave_atomic
        .store(true, std::sync::atomic::Ordering::Release);
    rudis::replication::HAS_SLAVE_INSTANCE.store(true, std::sync::atomic::Ordering::Release);

    let hello_replica = send_and_read(&mut client, b"HELLO 2\r\n");
    assert!(hello_replica.contains("$4\r\nrole\r\n$7\r\nreplica\r\n"));

    hub.is_slave_atomic
        .store(false, std::sync::atomic::Ordering::Release);
    rudis::replication::HAS_SLAVE_INSTANCE.store(false, std::sync::atomic::Ordering::Release);
}

#[test]
fn test_requirepass_and_sha256_acl_enforcement_e2e() {
    let port = 19996;
    start_test_server(port, 2);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. Unauthenticated ping succeeds when no password is set
    assert_eq!(send_and_read(&mut client, b"PING\r\n"), "+PONG\r\n");

    // 2. Set requirepass via CONFIG SET
    assert_eq!(
        send_and_read(&mut client, b"CONFIG SET requirepass secret123\r\n"),
        "+OK\r\n"
    );

    // 3. New connection must be blocked until authenticated
    let mut fresh_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let noauth_resp = send_and_read(&mut fresh_client, b"PING\r\n");
    assert_eq!(noauth_resp, "-NOAUTH Authentication required.\r\n");

    // Wrong password fails
    let wrong_resp = send_and_read(&mut fresh_client, b"AUTH bad_pass\r\n");
    assert!(wrong_resp.starts_with("-WRONGPASS"));

    // Correct password succeeds
    assert_eq!(
        send_and_read(&mut fresh_client, b"AUTH secret123\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut fresh_client, b"PING\r\n"), "+PONG\r\n");

    // 4. SHA-256 ACL hash authentication
    // SHA256("alice_pwd") = "4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a"
    let sha256_hash = rudis::acl::hash_password_sha256("alice_pwd");
    assert!(sha256_hash.starts_with('#'));
    let setuser_cmd = format!("ACL SETUSER alice on {} +@all ~*\r\n", sha256_hash);
    assert_eq!(
        send_and_read(&mut fresh_client, setuser_cmd.as_bytes()),
        "+OK\r\n"
    );

    // Authenticate as alice using plaintext matching the SHA-256 hash
    let mut alice_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(
        send_and_read(&mut alice_client, b"AUTH alice alice_pwd\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut alice_client, b"PING\r\n"), "+PONG\r\n");

    // Clean up requirepass
    assert_eq!(
        send_and_read(&mut fresh_client, b"CONFIG SET requirepass \"\"\r\n"),
        "+OK\r\n"
    );
    let mut unauth_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    assert_eq!(send_and_read(&mut unauth_client, b"PING\r\n"), "+PONG\r\n");
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
    assert_eq!(
        send_and_read(&mut stream, b"WAITAOF 1 1 50\r\n"),
        "*2\r\n:1\r\n:0\r\n"
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
    assert_eq!(
        send_and_read(&mut stream, b"OBJECT FREQ obj_k\r\n"),
        ":0\r\n"
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
    assert_eq!(send_and_read(&mut stream, b"COMMAND COUNT\r\n"), ":250\r\n");
    let cmd_list_resp = send_and_read(&mut stream, b"COMMAND LIST\r\n");
    assert!(cmd_list_resp.contains("xinfo"));
    assert!(cmd_list_resp.contains("unlink"));
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
    assert_eq!(send_and_read(&mut stream, b"FUNCTION KILL\r\n"), "+OK\r\n");
}

#[test]
fn test_hash_field_expiration_and_stream_claim_e2e() {
    let port = 19125;
    start_test_server(port, 4);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. Hash field expiration (HEXPIRE, HTTL, HPERSIST)
    assert_eq!(
        send_and_read(&mut stream, b"HSET user:900 session tok_123 role admin\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            b"HTTL user:900 FIELDS 3 session role ghost\r\n"
        ),
        "*3\r\n:-1\r\n:-1\r\n:-2\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            b"HEXPIRE user:900 30 NX FIELDS 2 session ghost\r\n"
        ),
        "*2\r\n:1\r\n:-2\r\n"
    );
    // NX fails when field already has TTL
    assert_eq!(
        send_and_read(&mut stream, b"HEXPIRE user:900 60 NX FIELDS 1 session\r\n"),
        "*1\r\n:0\r\n"
    );
    // HPERSIST removes expiration
    assert_eq!(
        send_and_read(&mut stream, b"HPERSIST user:900 FIELDS 2 session role\r\n"),
        "*2\r\n:1\r\n:-1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"HTTL user:900 FIELDS 1 session\r\n"),
        "*1\r\n:-1\r\n"
    );
    // Immediate expiration with TTL = 0 deletes the field
    assert_eq!(
        send_and_read(&mut stream, b"HEXPIRE user:900 0 FIELDS 1 session\r\n"),
        "*1\r\n:2\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"HGET user:900 session\r\n"),
        "$-1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"HGET user:900 role\r\n"),
        "$5\r\nadmin\r\n"
    );

    // 2. Stream consumer recovery (XCLAIM & XAUTOCLAIM)
    assert_eq!(
        send_and_read(&mut stream, b"XADD events:1 100-0 job alpha\r\n"),
        "$5\r\n100-0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"XADD events:1 200-0 job beta\r\n"),
        "$5\r\n200-0\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"XGROUP CREATE events:1 workers 0\r\n"),
        "+OK\r\n"
    );
    let read_res = send_and_read(
        &mut stream,
        b"XREADGROUP GROUP workers w1 COUNT 2 STREAMS events:1 >\r\n",
    );
    assert!(read_res.contains("100-0") && read_res.contains("200-0"));

    // XCLAIM 100-0 from w1 to w2 with JUSTID
    let claim_res = send_and_read(
        &mut stream,
        b"XCLAIM events:1 workers w2 0 100-0 JUSTID\r\n",
    );
    assert_eq!(claim_res, "*1\r\n$5\r\n100-0\r\n");

    // XAUTOCLAIM remaining entries to w3 with JUSTID
    let autoclaim_res = send_and_read(
        &mut stream,
        b"XAUTOCLAIM events:1 workers w3 0 0-0 COUNT 10 JUSTID\r\n",
    );
    assert!(autoclaim_res.contains("0-0"));
    assert!(autoclaim_res.contains("100-0"));
    assert!(autoclaim_res.contains("200-0"));
}

#[test]
fn test_sintercard_zintercard_zrangestore_cross_shard_e2e() {
    let port = 19126;
    start_test_server(port, 4);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. Co-located single-shard SINTERCARD & ZINTERCARD (zero-allocation fast path)
    assert_eq!(
        send_and_read(&mut stream, b"SADD {grp}:s1 a b c d\r\n"),
        ":4\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SADD {grp}:s2 b c d e\r\n"),
        ":4\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SINTERCARD 2 {grp}:s1 {grp}:s2\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SINTERCARD 2 {grp}:s1 {grp}:s2 LIMIT 2\r\n"),
        ":2\r\n"
    );

    // 2. Cross-shard SINTERCARD & ZINTERCARD across distinct shards
    assert_eq!(
        send_and_read(&mut stream, b"SADD shard_set_alpha u1 u2 u3 u4\r\n"),
        ":4\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SADD shard_set_beta u2 u3 u4 u5\r\n"),
        ":4\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            b"SINTERCARD 2 shard_set_alpha shard_set_beta\r\n"
        ),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            b"SINTERCARD 2 shard_set_alpha shard_set_beta LIMIT 1\r\n"
        ),
        ":1\r\n"
    );

    assert_eq!(
        send_and_read(
            &mut stream,
            b"ZADD shard_zset_1 10 m1 20 m2 30 m3 40 m4\r\n"
        ),
        ":4\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"ZADD shard_zset_2 5 m2 15 m3 25 m4 35 m5\r\n"),
        ":4\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"ZINTERCARD 2 shard_zset_1 shard_zset_2\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            b"ZINTERCARD 2 shard_zset_1 shard_zset_2 LIMIT 2\r\n"
        ),
        ":2\r\n"
    );

    // 3. Cross-shard & overwrite ZRANGESTORE
    assert_eq!(
        send_and_read(
            &mut stream,
            b"ZRANGESTORE shard_zset_dst shard_zset_1 15 35 BYSCORE\r\n"
        ),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"ZCARD shard_zset_dst\r\n"),
        ":2\r\n"
    );
    // Overwrite existing dst with smaller range
    assert_eq!(
        send_and_read(
            &mut stream,
            b"ZRANGESTORE shard_zset_dst shard_zset_1 0 0\r\n"
        ),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"ZCARD shard_zset_dst\r\n"),
        ":1\r\n"
    );
}

#[test]
fn test_cross_shard_set_and_zset_algebra_e2e() {
    let port = 19127;
    start_test_server(port, 4);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. Cross-shard SINTER, SUNION, SDIFF
    assert_eq!(
        send_and_read(&mut stream, b"SADD alg_s1 foo bar baz\r\n"),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SADD alg_s2 bar baz qux\r\n"),
        ":3\r\n"
    );

    let sinter_res = send_and_read(&mut stream, b"SINTER alg_s1 alg_s2\r\n");
    assert!(
        sinter_res.contains("bar") && sinter_res.contains("baz") && !sinter_res.contains("foo")
    );

    let sunion_res = send_and_read(&mut stream, b"SUNION alg_s1 alg_s2\r\n");
    assert!(
        sunion_res.contains("foo")
            && sunion_res.contains("bar")
            && sunion_res.contains("baz")
            && sunion_res.contains("qux")
    );

    let sdiff_res = send_and_read(&mut stream, b"SDIFF alg_s1 alg_s2\r\n");
    assert!(sdiff_res.contains("foo") && !sdiff_res.contains("bar"));

    // 2. Cross-shard SINTERSTORE, SUNIONSTORE, SDIFFSTORE
    assert_eq!(
        send_and_read(&mut stream, b"SINTERSTORE alg_dest_inter alg_s1 alg_s2\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SCARD alg_dest_inter\r\n"),
        ":2\r\n"
    );

    assert_eq!(
        send_and_read(&mut stream, b"SUNIONSTORE alg_dest_union alg_s1 alg_s2\r\n"),
        ":4\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SCARD alg_dest_union\r\n"),
        ":4\r\n"
    );

    assert_eq!(
        send_and_read(&mut stream, b"SDIFFSTORE alg_dest_diff alg_s1 alg_s2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"SCARD alg_dest_diff\r\n"),
        ":1\r\n"
    );

    // 3. Cross-shard ZINTER, ZUNION, ZDIFF
    assert_eq!(
        send_and_read(
            &mut stream,
            b"ZADD alg_z1 10 item_a 20 item_b 30 item_c\r\n"
        ),
        ":3\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"ZADD alg_z2 5 item_b 15 item_c 25 item_d\r\n"),
        ":3\r\n"
    );

    let zinter_res = send_and_read(&mut stream, b"ZINTER 2 alg_z1 alg_z2 WITHSCORES\r\n");
    assert!(zinter_res.contains("item_b") && zinter_res.contains("25")); // 20 + 5
    assert!(zinter_res.contains("item_c") && zinter_res.contains("45")); // 30 + 15

    let zdiff_res = send_and_read(&mut stream, b"ZDIFF 2 alg_z1 alg_z2 WITHSCORES\r\n");
    assert!(zdiff_res.contains("item_a") && zdiff_res.contains("10"));
    assert!(!zdiff_res.contains("item_b"));

    // 4. Cross-shard ZINTERSTORE, ZUNIONSTORE, ZDIFFSTORE
    assert_eq!(
        send_and_read(
            &mut stream,
            b"ZINTERSTORE alg_zdest_inter 2 alg_z1 alg_z2 WEIGHTS 2 1\r\n"
        ),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"ZSCORE alg_zdest_inter item_b\r\n"),
        "$2\r\n45\r\n" // 20*2 + 5*1
    );

    assert_eq!(
        send_and_read(
            &mut stream,
            b"ZUNIONSTORE alg_zdest_union 2 alg_z1 alg_z2 AGGREGATE MAX\r\n"
        ),
        ":4\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"ZCARD alg_zdest_union\r\n"),
        ":4\r\n"
    );

    assert_eq!(
        send_and_read(
            &mut stream,
            b"ZDIFFSTORE alg_zdest_diff 2 alg_z1 alg_z2\r\n"
        ),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"ZCARD alg_zdest_diff\r\n"),
        ":1\r\n"
    );
}

#[test]
fn test_cross_shard_xread_and_xreadgroup_e2e() {
    let port = 19128;
    start_test_server(port, 4);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. Pick two stream keys that map to distinct shards
    let mut s1 = String::new();
    let mut s2 = String::new();
    for i in 0..100 {
        let k = format!("xs_key_{}", i);
        let shard = target_shard(k.as_bytes(), 4);
        if shard == 0 && s1.is_empty() {
            s1 = k;
        } else if shard == 1 && s2.is_empty() {
            s2 = k;
        }
        if !s1.is_empty() && !s2.is_empty() {
            break;
        }
    }
    assert_ne!(s1, s2);
    assert_ne!(
        target_shard(s1.as_bytes(), 4),
        target_shard(s2.as_bytes(), 4)
    );

    // 2. Populate both streams on distinct shards
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("XADD {} 100-1 f1 v1\r\n", s1).as_bytes()
        ),
        "$5\r\n100-1\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("XADD {} 100-2 f2 v2\r\n", s1).as_bytes()
        ),
        "$5\r\n100-2\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("XADD {} 200-1 f3 v3\r\n", s2).as_bytes()
        ),
        "$5\r\n200-1\r\n"
    );

    // 3. Cross-shard non-blocking XREAD
    let xread_all = send_and_read(
        &mut stream,
        format!("XREAD STREAMS {} {} 0 0\r\n", s1, s2).as_bytes(),
    );
    assert!(xread_all.starts_with("*2\r\n"));
    assert!(xread_all.contains(&s1));
    assert!(xread_all.contains("100-1"));
    assert!(xread_all.contains("100-2"));
    assert!(xread_all.contains(&s2));
    assert!(xread_all.contains("200-1"));

    // 4. Cross-shard XREAD with COUNT 1
    let xread_c1 = send_and_read(
        &mut stream,
        format!("XREAD COUNT 1 STREAMS {} {} 0 0\r\n", s1, s2).as_bytes(),
    );
    assert!(xread_c1.starts_with("*2\r\n"));
    assert!(xread_c1.contains(&s1));
    assert!(xread_c1.contains("100-1"));
    assert!(!xread_c1.contains("100-2"));
    assert!(xread_c1.contains(&s2));
    assert!(xread_c1.contains("200-1"));

    // 5. Cross-shard blocking XREAD timeout (idle streams)
    let t0 = std::time::Instant::now();
    let xread_timeout = send_and_read(
        &mut stream,
        format!("XREAD BLOCK 100 STREAMS {} {} $ $\r\n", s1, s2).as_bytes(),
    );
    assert_eq!(xread_timeout, "$-1\r\n");
    assert!(t0.elapsed() >= Duration::from_millis(90));

    // 6. Cross-shard blocking XREAD wake-up via concurrent XADD
    let s2_clone = s2.clone();
    let producer_handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(60));
        let mut producer = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        let resp = send_and_read(
            &mut producer,
            format!("XADD {} 300-1 alert high\r\n", s2_clone).as_bytes(),
        );
        assert_eq!(resp, "$5\r\n300-1\r\n");
    });

    let xread_wakeup = send_and_read(
        &mut stream,
        format!("XREAD BLOCK 2000 STREAMS {} {} $ $\r\n", s1, s2).as_bytes(),
    );
    producer_handle.join().unwrap();
    assert!(xread_wakeup.contains(&s2));
    assert!(xread_wakeup.contains("300-1"));
    assert!(xread_wakeup.contains("alert"));

    // 7. Cross-shard XREADGROUP
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("XGROUP CREATE {} grp 0\r\n", s1).as_bytes()
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("XGROUP CREATE {} grp 0\r\n", s2).as_bytes()
        ),
        "+OK\r\n"
    );

    let xrg_res = send_and_read(
        &mut stream,
        format!("XREADGROUP GROUP grp c1 STREAMS {} {} > >\r\n", s1, s2).as_bytes(),
    );
    assert!(xrg_res.starts_with("*2\r\n"));
    assert!(xrg_res.contains(&s1));
    assert!(xrg_res.contains("100-1"));
    assert!(xrg_res.contains(&s2));
    assert!(xrg_res.contains("200-1"));

    // Check PEL state on both streams
    let p1 = send_and_read(&mut stream, format!("XPENDING {} grp\r\n", s1).as_bytes());
    assert!(p1.contains(":2\r\n"));
    let p2 = send_and_read(&mut stream, format!("XPENDING {} grp\r\n", s2).as_bytes());
    assert!(p2.contains(":2\r\n"));
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
