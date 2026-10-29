//! End-to-end tests: commands (see `tests/common` for the helpers).

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
    assert!(client_list_has_id(&list_resp, client_id));
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
    assert!(client_list_has_id(&list_resp2, client_id));
    assert!(client_list_has_id(&list_resp2, client_id2));
    assert!(list_resp2.contains("name=test_client_1"));
    assert!(list_resp2.contains("name=test_client_2"));

    // Drop second client and verify cleanup. The server unregisters a client
    // once its shard reads the EOF, which a loaded machine can delay.
    drop(stream2);
    let mut list_resp3;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        list_resp3 = send_and_read(&mut stream, b"CLIENT LIST\r\n");
        if !client_list_has_id(&list_resp3, client_id2) || std::time::Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(client_list_has_id(&list_resp3, client_id));
    assert!(
        !client_list_has_id(&list_resp3, client_id2),
        "Disconnected client should be removed (id={}): {}",
        client_id2,
        list_resp3
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
    assert_eq!(resp, "*2\r\n$8\r\n{t}:list\r\n$9\r\nwoken_val\r\n");
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

    // SMOVE across shards works in standalone mode, like Redis.
    let smove_cross = send_and_read(&mut client, b"SMOVE {s1} cross_slot_dest b\r\n");
    assert_eq!(smove_cross, ":1\r\n");

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

    // LMOVE across shards works in standalone mode, like Redis.
    let lmove_cross = send_and_read(&mut client, b"LMOVE {l1} cross_slot LEFT RIGHT\r\n");
    assert_eq!(lmove_cross, "$8\r\ninserted\r\n");

    // BLMOVE immediate
    let blmove_immediate = send_and_read(&mut client, b"BLMOVE {l1}_dst {l1} LEFT LEFT 0.1\r\n");
    assert_eq!(blmove_immediate, "$3\r\none\r\n");

    // BLMOVE timeout
    let blmove_timeout = send_and_read(
        &mut client,
        b"BLMOVE {empty_q} {empty_q}_dst LEFT RIGHT 0.1\r\n",
    );
    assert_eq!(blmove_timeout, "$-1\r\n");

    // BLMOVE blocking cross-thread notification: push from another
    // connection once the client is blocked.
    client
        .write_all(b"BLMOVE {blmove_q} {blmove_q}_dst LEFT RIGHT 2.0\r\n")
        .unwrap();
    let mut pusher = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !send_and_read(&mut pusher, b"INFO clients\r\n").contains("blocked_clients:1\r\n") {
        assert!(std::time::Instant::now() < deadline, "BLMOVE never blocked");
        thread::sleep(Duration::from_millis(5));
    }
    // Another server in this process (one block hub per port; no test
    // listens on port + 1) gaining and losing a waiter must not hide this
    // one from the push.
    {
        let other_hub = rudis::block::get_block_hub_for_port(port + 1);
        let mut other_hub = other_hub.lock();
        let (tx, _rx) = flume::unbounded();
        other_hub.register_blocked_client(1, tx);
        other_hub.unregister_blocked_client(1);
    }
    send_and_read(&mut pusher, b"LPUSH {blmove_q} blocked_item\r\n");
    let mut buf = [0u8; 64];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&buf[..n]),
        "$12\r\nblocked_item\r\n"
    );
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
    // SAFETY: below PAGE_SIZE, so the kernel copies the payload.
    let sent = unsafe { engine.send_zc(fd1, test_msg) }.expect("zero-copy send");
    assert_eq!(sent, test_msg.len());

    let mut recv_buf = [0u8; 32];
    // SAFETY: `recv_buf` is a local 32-byte array and the length passed
    // matches; `fd2` is owned by `s2`, which is alive.
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
    assert!(hash_resp.contains("$11\r\nsqc8b49rny0"));
    assert!(hash_resp.contains("$11\r\nsqdtr74hyu0"));

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

    // Multi-key memcached `get` is ambiguous with Redis GET on a shared port;
    // Redis semantics win (GET takes exactly one key).
    let mc_get = b"get mc_fruit non_existent\r\n";
    assert_eq!(
        send_and_read(&mut client, mc_get),
        "-ERR wrong number of arguments for 'get' command\r\n"
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
        "-ERR No such client\r\n"
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
            b"JSON.SET jext $ '{\"num\":10,\"str\":\"hello\",\"arr\":[1,2,3]}'\r\n"
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
    assert!(docker_content.contains("COPY vendor ./vendor"));
    assert!(docker_content.contains("HEALTHCHECK"));
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

    // 9. Reset slowlog. With log-slower-than 0 the RESET itself is logged,
    // exactly as in Valkey, so the log holds one entry afterwards.
    assert_eq!(send_and_read(&mut client, b"SLOWLOG RESET\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"SLOWLOG LEN\r\n"), ":1\r\n");
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
    *rudis::config::ACTIVE_CONFIG_FILE.write() = Some(cfg_path.clone());

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

    // A blocking XREADGROUP CLAIM that times out must wait at least its BLOCK
    // duration, never cut the last millisecond short across poll steps.
    for block_ms in [55u64, 100] {
        assert_eq!(
            send_and_read(
                &mut stream,
                b"XCLAIM events:1 workers w3 0 100-0 200-0 JUSTID\r\n",
            ),
            "*2\r\n$5\r\n100-0\r\n$5\r\n200-0\r\n"
        );
        let t0 = std::time::Instant::now();
        let cmd = format!(
            "XREADGROUP GROUP workers w4 BLOCK {block_ms} CLAIM 500 STREAMS events:1 >\r\n"
        );
        let res = send_and_read(&mut stream, cmd.as_bytes());
        let elapsed = t0.elapsed();
        assert_eq!(res, "$-1\r\n");
        assert!(
            elapsed >= Duration::from_millis(block_ms),
            "XREADGROUP BLOCK {block_ms} CLAIM timed out early after {elapsed:?}"
        );
    }
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
fn test_hpexpire_hgetex_hsetex_e2e() {
    let port = 19133;
    start_test_server(port, 4);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. HSETEX with FNX and PX
    assert_eq!(
        send_and_read(
            &mut stream,
            b"HSETEX h_prof FNX PX 60000 FIELDS 2 name alice role admin\r\n"
        ),
        ":1\r\n"
    );
    // FNX fails if any field exists
    assert_eq!(
        send_and_read(
            &mut stream,
            b"HSETEX h_prof FNX PX 60000 FIELDS 2 name bob extra val\r\n"
        ),
        ":0\r\n"
    );
    // FXX succeeds when all fields exist, and KEEPTTL preserves TTL
    assert_eq!(
        send_and_read(
            &mut stream,
            b"HSETEX h_prof FXX KEEPTTL FIELDS 1 name alice_updated\r\n"
        ),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"HGET h_prof name\r\n"),
        "$13\r\nalice_updated\r\n"
    );

    // 2. Verify HPTTL > 0 for both fields
    let hpttl_res = send_and_read(&mut stream, b"HPTTL h_prof FIELDS 2 name role\r\n");
    assert!(hpttl_res.starts_with("*2\r\n:"));
    assert!(!hpttl_res.contains(":-1\r\n") && !hpttl_res.contains(":-2\r\n"));

    // 3. HGETEX with PERSIST
    let hgetex_res = send_and_read(
        &mut stream,
        b"HGETEX h_prof PERSIST FIELDS 2 name missing\r\n",
    );
    assert_eq!(hgetex_res, "*2\r\n$13\r\nalice_updated\r\n$-1\r\n");
    assert_eq!(
        send_and_read(&mut stream, b"HPTTL h_prof FIELDS 1 name\r\n"),
        "*1\r\n:-1\r\n"
    );

    // 4. HGETEX with EX sets new TTL
    assert_eq!(
        send_and_read(&mut stream, b"HGETEX h_prof EX 30 FIELDS 1 name\r\n"),
        "*1\r\n$13\r\nalice_updated\r\n"
    );
    let httl_after = send_and_read(&mut stream, b"HTTL h_prof FIELDS 1 name\r\n");
    assert!(!httl_after.contains(":-1\r\n") && !httl_after.contains(":-2\r\n"));

    // 5. HGETEX with PX 0 returns current value and immediately expires/deletes the field
    assert_eq!(
        send_and_read(&mut stream, b"HGETEX h_prof PX 0 FIELDS 1 role\r\n"),
        "*1\r\n$5\r\nadmin\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"HGET h_prof role\r\n"),
        "$-1\r\n"
    );
}

#[test]
fn test_distributed_multi_exec_and_stream_observability_e2e() {
    let port = 19134;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Find 4 keys on 4 distinct shards
    let mut shard_keys = vec![String::new(); num_shards];
    for i in 0..1000 {
        let candidate = format!("dtx_k_{}", i);
        let s = rudis::router::target_shard(candidate.as_bytes(), num_shards);
        if shard_keys[s].is_empty() {
            shard_keys[s] = candidate;
        }
        if shard_keys.iter().all(|k| !k.is_empty()) {
            break;
        }
    }
    let k0 = &shard_keys[0];
    let k1 = &shard_keys[1];
    let k2 = &shard_keys[2];
    let k3 = &shard_keys[3];

    // 1. Multi-shard MULTI/EXEC with cross-shard ZRANGESTORE, SINTERSTORE, HSETEX, and non-blocking XREAD BLOCK
    assert_eq!(send_and_read(&mut stream, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("ZADD {} 10 a 20 b 30 c\r\n", k0).as_bytes()
        ),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("ZRANGESTORE {} {} 1 2\r\n", k1, k0).as_bytes()
        ),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, format!("ZRANGE {} 0 -1\r\n", k1).as_bytes()),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("HSETEX {} FNX PX 60000 FIELDS 1 f1 v1\r\n", k2).as_bytes()
        ),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("XREAD BLOCK 5000 STREAMS {} 0-0\r\n", k3).as_bytes()
        ),
        "+QUEUED\r\n"
    );
    let exec_res = send_and_read(&mut stream, b"EXEC\r\n");
    assert_eq!(
        exec_res,
        "*5\r\n:3\r\n:2\r\n*2\r\n$1\r\nb\r\n$1\r\nc\r\n:1\r\n$-1\r\n"
    );

    // 2. XADD inside MULTI/EXEC wakes blocked XREAD waiter on commit
    let k3_clone = k3.clone();
    let waiter_handle = std::thread::spawn(move || {
        let mut w_stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        w_stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        send_and_read(
            &mut w_stream,
            format!("XREAD BLOCK 3000 STREAMS {} $\r\n", k3_clone).as_bytes(),
        )
    });
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(send_and_read(&mut stream, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(
            &mut stream,
            format!("XADD {} 1-1 msg tx_stream\r\n", k3).as_bytes()
        ),
        "+QUEUED\r\n"
    );
    let tx_stream_exec = send_and_read(&mut stream, b"EXEC\r\n");
    assert_eq!(tx_stream_exec, "*1\r\n$3\r\n1-1\r\n");
    let waiter_res = waiter_handle.join().unwrap();
    assert!(waiter_res.contains("1-1") && waiter_res.contains("tx_stream"));

    // 3. Redis 7.0+ / 7.4 Stream Observability (XINFO STREAM, XINFO GROUPS, XINFO CONSUMERS)
    assert_eq!(
        send_and_read(&mut stream, b"XADD obs_s 10-1 f a\r\n"),
        "$4\r\n10-1\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"XADD obs_s 10-2 f b\r\n"),
        "$4\r\n10-2\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"XADD obs_s 10-3 f c\r\n"),
        "$4\r\n10-3\r\n"
    );
    assert_eq!(
        send_and_read(&mut stream, b"XGROUP CREATE obs_s grp1 0\r\n"),
        "+OK\r\n"
    );

    let ginfo_initial = send_and_read(&mut stream, b"XINFO GROUPS obs_s\r\n");
    assert!(
        ginfo_initial.contains("entries-read\r\n$-1\r\n")
            && ginfo_initial.contains("lag\r\n:3\r\n"),
        "unexpected initial XINFO GROUPS: {}",
        ginfo_initial
    );

    assert_eq!(
        send_and_read(&mut stream, b"XGROUP CREATECONSUMER obs_s grp1 c_idle\r\n"),
        ":1\r\n"
    );
    let cinfo_idle = send_and_read(&mut stream, b"XINFO CONSUMERS obs_s grp1\r\n");
    assert!(
        cinfo_idle.contains("c_idle") && cinfo_idle.contains("inactive\r\n:-1\r\n"),
        "unexpected idle XINFO CONSUMERS: {}",
        cinfo_idle
    );

    let read_res = send_and_read(
        &mut stream,
        b"XREADGROUP GROUP grp1 c_active COUNT 2 STREAMS obs_s >\r\n",
    );
    assert!(read_res.contains("10-1") && read_res.contains("10-2"));

    let ginfo_after = send_and_read(&mut stream, b"XINFO GROUPS obs_s\r\n");
    assert!(
        ginfo_after.contains("entries-read\r\n:2\r\n") && ginfo_after.contains("lag\r\n:1\r\n"),
        "unexpected post-read XINFO GROUPS: {}",
        ginfo_after
    );

    let cinfo_active = send_and_read(&mut stream, b"XINFO CONSUMERS obs_s grp1\r\n");
    assert!(
        cinfo_active.contains("c_active") && cinfo_active.contains("pending\r\n:2\r\n"),
        "unexpected active XINFO CONSUMERS: {}",
        cinfo_active
    );

    assert_eq!(send_and_read(&mut stream, b"XDEL obs_s 10-2\r\n"), ":1\r\n");
    let sinfo = send_and_read(&mut stream, b"XINFO STREAM obs_s\r\n");
    assert!(
        sinfo.contains("length\r\n:2\r\n")
            && sinfo.contains("entries-added\r\n:3\r\n")
            && sinfo.contains("max-deleted-entry-id\r\n$4\r\n10-2\r\n")
            && sinfo.contains("recorded-first-entry-id\r\n$4\r\n10-1\r\n")
            && sinfo.contains("first-entry")
            && sinfo.contains("last-entry"),
        "unexpected XINFO STREAM: {}",
        sinfo
    );
}

#[test]
fn test_bzmpop_bzpopmin_zmpop_sort_unwatch_zrangebylex_e2e() {
    let port = 20100;
    let num_shards = 4;
    start_test_server(port, num_shards);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Failed to connect to rudis server");
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 1. SORT & SORT_RO
    send_and_read(&mut client, b"RPUSH {sort1} 10 3 5 1 20 8\r\n");
    let sort_asc = send_and_read(&mut client, b"SORT {sort1}\r\n");
    assert!(
        sort_asc.contains("$1\r\n1\r\n$1\r\n3\r\n$1\r\n5\r\n$1\r\n8\r\n$2\r\n10\r\n$2\r\n20\r\n"),
        "SORT asc failed: {}",
        sort_asc
    );

    let sort_desc = send_and_read(&mut client, b"SORT {sort1} DESC\r\n");
    assert!(
        sort_desc.contains("$2\r\n20\r\n$2\r\n10\r\n$1\r\n8\r\n$1\r\n5\r\n$1\r\n3\r\n$1\r\n1\r\n"),
        "SORT desc failed: {}",
        sort_desc
    );

    let sort_limit = send_and_read(&mut client, b"SORT {sort1} LIMIT 1 3\r\n");
    assert!(
        sort_limit.contains("$1\r\n3\r\n$1\r\n5\r\n$1\r\n8\r\n"),
        "SORT limit failed: {}",
        sort_limit
    );

    let sort_store = send_and_read(&mut client, b"SORT {sort1} STORE {sort1}:dst\r\n");
    assert_eq!(sort_store, ":6\r\n");
    let dst_len = send_and_read(&mut client, b"LLEN {sort1}:dst\r\n");
    assert_eq!(dst_len, ":6\r\n");

    send_and_read(
        &mut client,
        b"RPUSH {sortalpha} banana apple cherry date\r\n",
    );
    let sort_alpha = send_and_read(&mut client, b"SORT {sortalpha} ALPHA\r\n");
    assert!(
        sort_alpha.contains("apple")
            && sort_alpha.contains("banana")
            && sort_alpha.contains("cherry"),
        "SORT ALPHA failed: {}",
        sort_alpha
    );

    // 2. UNWATCH
    send_and_read(&mut client, b"SET {watchkey} val1\r\n");
    let watch_resp = send_and_read(&mut client, b"WATCH {watchkey}\r\n");
    assert_eq!(watch_resp, "+OK\r\n");
    let unwatch_resp = send_and_read(&mut client, b"UNWATCH\r\n");
    assert_eq!(unwatch_resp, "+OK\r\n");
    let mut other_client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    send_and_read(&mut other_client, b"SET {watchkey} val2\r\n");
    send_and_read(&mut client, b"MULTI\r\n");
    send_and_read(&mut client, b"SET {watchkey} val3\r\n");
    let exec_resp = send_and_read(&mut client, b"EXEC\r\n");
    assert_eq!(exec_resp, "*1\r\n+OK\r\n");
    let final_val = send_and_read(&mut client, b"GET {watchkey}\r\n");
    assert_eq!(final_val, "$4\r\nval3\r\n");

    // 3. ZRANGEBYLEX & ZREVRANGEBYLEX
    send_and_read(&mut client, b"ZADD {lexz} 0 a 0 b 0 c 0 d 0 e 0 f 0 g\r\n");
    let zrangebylex_resp = send_and_read(&mut client, b"ZRANGEBYLEX {lexz} [b [e\r\n");
    assert_eq!(
        zrangebylex_resp,
        "*4\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"
    );

    let zrangebylex_limit = send_and_read(&mut client, b"ZRANGEBYLEX {lexz} - + LIMIT 2 3\r\n");
    assert_eq!(zrangebylex_limit, "*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n");

    let zrevrangebylex_resp = send_and_read(&mut client, b"ZREVRANGEBYLEX {lexz} [e [b\r\n");
    assert_eq!(
        zrevrangebylex_resp,
        "*4\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n"
    );

    let zrevrangebylex_limit =
        send_and_read(&mut client, b"ZREVRANGEBYLEX {lexz} + - LIMIT 1 2\r\n");
    assert_eq!(zrevrangebylex_limit, "*2\r\n$1\r\nf\r\n$1\r\ne\r\n");

    // 4. ZMPOP
    send_and_read(&mut client, b"ZADD {zmpop_k} 10 m1 20 m2 30 m3 40 m4\r\n");
    let zmpop_min = send_and_read(&mut client, b"ZMPOP 1 {zmpop_k} MIN COUNT 2\r\n");
    assert!(
        zmpop_min.contains("{zmpop_k}") && zmpop_min.contains("m1") && zmpop_min.contains("m2"),
        "ZMPOP MIN failed: {}",
        zmpop_min
    );

    let zmpop_max = send_and_read(&mut client, b"ZMPOP 1 {zmpop_k} MAX COUNT 1\r\n");
    assert!(
        zmpop_max.contains("{zmpop_k}") && zmpop_max.contains("m4"),
        "ZMPOP MAX failed: {}",
        zmpop_max
    );

    let zcard_after = send_and_read(&mut client, b"ZCARD {zmpop_k}\r\n");
    assert_eq!(zcard_after, ":1\r\n");

    // 5. BZPOPMIN
    send_and_read(&mut client, b"ZADD {bzpop_k} 50 first 60 second\r\n");
    let bzpop_resp = send_and_read(&mut client, b"BZPOPMIN {bzpop_k} 0.5\r\n");
    assert!(
        bzpop_resp.contains("{bzpop_k}") && bzpop_resp.contains("first"),
        "BZPOPMIN failed: {}",
        bzpop_resp
    );

    let bzpop_empty = send_and_read(&mut client, b"BZPOPMIN {empty_bzpop} 0.02\r\n");
    assert_eq!(bzpop_empty, "*-1\r\n");

    // 6. BZMPOP
    send_and_read(&mut client, b"ZADD {bzmpop_k} 100 x 200 y 300 z\r\n");
    let bzmpop_resp = send_and_read(&mut client, b"BZMPOP 0.5 1 {bzmpop_k} MIN COUNT 2\r\n");
    assert!(
        bzmpop_resp.contains("{bzmpop_k}")
            && bzmpop_resp.contains("x")
            && bzmpop_resp.contains("y"),
        "BZMPOP failed: {}",
        bzmpop_resp
    );

    let bzmpop_empty = send_and_read(&mut client, b"BZMPOP 0.02 1 {empty_bzmpop} MIN COUNT 1\r\n");
    assert_eq!(bzmpop_empty, "*-1\r\n");
}

#[test]
fn test_pipeline_keeps_order_across_shards_e2e() {
    // A write to another shard followed by an unbatched command (MGET,
    // multi-key EXISTS) in the same pipeline must see the write.
    let port = 16979;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "4", "--no-pin"];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let enc = |args: &[&str]| {
        let mut b = format!("*{}\r\n", args.len());
        for a in args {
            b.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        b
    };
    let mut req = String::new();
    let mut want = String::new();
    for i in 0..200 {
        let (k, e, v) = (
            format!("po:k{}", i),
            format!("po:e{}", i),
            format!("new{}", i),
        );
        req += &enc(&["SET", &k, "old"]);
        want += "+OK\r\n";
        req += &enc(&["SET", &k, &v]);
        want += "+OK\r\n";
        req += &enc(&["MGET", &k, "po:missing"]);
        want += &format!("*2\r\n${}\r\n{}\r\n$-1\r\n", v.len(), v);
        req += &enc(&["SET", &e, "1"]);
        want += "+OK\r\n";
        req += &enc(&["EXISTS", &e, "po:missing"]);
        want += ":1\r\n";
    }
    c.write_all(req.as_bytes()).unwrap();
    let mut got = vec![0u8; want.len()];
    c.read_exact(&mut got).unwrap();
    let got = String::from_utf8_lossy(&got);
    if got != want {
        let pos = got
            .bytes()
            .zip(want.bytes())
            .position(|(a, b)| a != b)
            .unwrap_or(0);
        panic!(
            "pipeline reply diverges at byte {pos}: got {:?}, want {:?}",
            &got[pos.saturating_sub(40)..(pos + 40).min(got.len())],
            &want[pos.saturating_sub(40)..(pos + 40).min(want.len())]
        );
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_cross_shard_spin_config_and_replies_with_or_without_polling_e2e() {
    let port = 16974;
    let dir = std::env::temp_dir().join(format!("rudis-spin-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("spin.conf");
    std::fs::write(&conf, "cross-shard-spin 32\n").unwrap();
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "4",
        "--no-pin",
        "-c",
        conf.to_str().unwrap(),
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let get = |c: &mut TcpStream| resp_cmd(c, &["CONFIG", "GET", "cross-shard-spin"]);
    assert_eq!(get(&mut c), "*2\r\n$16\r\ncross-shard-spin\r\n$2\r\n32\r\n");

    for spin in ["32", "0"] {
        assert_eq!(
            resp_cmd(&mut c, &["CONFIG", "SET", "cross-shard-spin", spin]),
            "+OK\r\n"
        );
        // 40 keys spread over all 4 shards: single-key hops, MSET/MGET
        // scatter and a pipelined batch must all answer with spin 32 or 0.
        let keys: Vec<String> = (0..40).map(|i| format!("spin{spin}:k{i}")).collect();
        for (i, k) in keys.iter().enumerate().take(10) {
            assert_eq!(resp_cmd(&mut c, &["SET", k, &format!("v{i}")]), "+OK\r\n");
            assert_eq!(
                resp_cmd(&mut c, &["GET", k]),
                format!("${}\r\nv{}\r\n", i.to_string().len() + 1, i)
            );
        }
        let mut mset = vec!["MSET".to_string()];
        for (i, k) in keys.iter().enumerate() {
            mset.push(k.clone());
            mset.push(format!("v{i}"));
        }
        let mset: Vec<&str> = mset.iter().map(String::as_str).collect();
        assert_eq!(resp_cmd(&mut c, &mset), "+OK\r\n");
        let mut mget = vec!["MGET"];
        mget.extend(keys.iter().map(String::as_str));
        let mut expected = format!("*{}\r\n", keys.len());
        for i in 0..keys.len() {
            expected.push_str(&format!("${}\r\nv{}\r\n", i.to_string().len() + 1, i));
        }
        assert_eq!(resp_cmd(&mut c, &mget), expected);

        let mut pipeline = String::new();
        let mut expected = String::new();
        for (i, k) in keys.iter().enumerate() {
            pipeline.push_str(&format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", k.len(), k));
            expected.push_str(&format!("${}\r\nv{}\r\n", i.to_string().len() + 1, i));
        }
        c.write_all(pipeline.as_bytes()).unwrap();
        let mut got = vec![0u8; expected.len()];
        c.read_exact(&mut got).unwrap();
        assert_eq!(String::from_utf8_lossy(&got), expected);
    }
    assert_eq!(get(&mut c), "*2\r\n$16\r\ncross-shard-spin\r\n$1\r\n0\r\n");

    for bad in ["-1", "lots"] {
        let r = resp_cmd(&mut c, &["CONFIG", "SET", "cross-shard-spin", bad]);
        assert!(r.starts_with("-ERR"), "{r}");
    }
    assert_eq!(get(&mut c), "*2\r\n$16\r\ncross-shard-spin\r\n$1\r\n0\r\n");

    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_pipelined_get_of_integer_values_is_bulk_framed_e2e() {
    let port = 16973;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Integer-looking values are stored integer-encoded; a pipelined GET of
    // them used to come back as bare digits without the `$len` header.
    let vals = ["7", "12345", "-3", "9223372036854775807", "plain"];
    let keys: Vec<String> = (0..20).map(|i| format!("intget:{i}")).collect();
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(
            resp_cmd(&mut c, &["SET", k, vals[i % vals.len()]]),
            "+OK\r\n"
        );
    }
    let mut pipeline = String::new();
    let mut expected = String::new();
    for (i, k) in keys.iter().enumerate() {
        pipeline.push_str(&format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", k.len(), k));
        let v = vals[i % vals.len()];
        expected.push_str(&format!("${}\r\n{}\r\n", v.len(), v));
    }
    pipeline.push_str("*2\r\n$4\r\nINCR\r\n$8\r\nintget:0\r\n");
    expected.push_str(":8\r\n");
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = vec![0u8; expected.len()];
    c.read_exact(&mut got).unwrap();
    assert_eq!(String::from_utf8_lossy(&got), expected);
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");

    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_unlink_exists_touch_count_keys_across_all_shards_e2e() {
    let port = 16972;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // 40 keys spread over all 4 shards, sent as one request per command.
    let keys: Vec<String> = (0..40).map(|i| format!("ux:{i}")).collect();
    for k in &keys {
        assert_eq!(resp_cmd(&mut c, &["SET", k, "v"]), "+OK\r\n");
    }
    let with = |cmd: &str, extra: &[&str]| {
        let mut args = vec![cmd];
        args.extend(keys.iter().map(String::as_str));
        args.extend_from_slice(extra);
        args.iter().map(|s| s.to_string()).collect::<Vec<_>>()
    };
    let run = |c: &mut TcpStream, args: Vec<String>| {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        resp_cmd(c, &args)
    };
    // Duplicates are counted per occurrence, missing keys are not.
    assert_eq!(
        run(&mut c, with("EXISTS", &["ux:0", "ux:39", "nope"])),
        ":42\r\n"
    );
    assert_eq!(run(&mut c, with("TOUCH", &["nope"])), ":40\r\n");
    assert_eq!(run(&mut c, with("UNLINK", &["nope"])), ":40\r\n");
    assert_eq!(run(&mut c, with("EXISTS", &[])), ":0\r\n");
    assert_eq!(run(&mut c, with("UNLINK", &[])), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":0\r\n");

    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_pipelined_mget_folds_across_shards_in_order_e2e() {
    let port = 16971;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let enc = |args: &[&str]| {
        let mut s = format!("*{}\r\n", args.len());
        for a in args {
            s.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        s
    };
    let bulk = |v: &str| format!("${}\r\n{}\r\n", v.len(), v);
    let keys: Vec<String> = (0..12).map(|i| format!("pm:{i}")).collect();
    let k: Vec<&str> = keys.iter().map(String::as_str).collect();
    assert_eq!(resp_cmd(&mut c, &["HSET", "pm:hash", "f", "v"]), ":1\r\n");

    // Writes earlier in the pipeline must be visible to a later MGET on every
    // shard, a later write must not be, a hash reads as nil, and replies stay
    // in pipeline order around single-key commands.
    let mut pipeline = String::new();
    let mut expected = String::new();
    for (i, key) in k.iter().enumerate() {
        pipeline.push_str(&enc(&["SET", key, &format!("a{i}")]));
        expected.push_str("+OK\r\n");
    }
    let mut mget = vec!["MGET"];
    mget.extend(&k);
    mget.extend(["pm:missing", "pm:hash"]);
    pipeline.push_str(&enc(&mget));
    expected.push_str(&format!("*{}\r\n", k.len() + 2));
    for i in 0..k.len() {
        expected.push_str(&bulk(&format!("a{i}")));
    }
    expected.push_str("$-1\r\n$-1\r\n");
    pipeline.push_str(&enc(&["SET", k[3], "b3"]));
    expected.push_str("+OK\r\n");
    pipeline.push_str(&enc(&["GET", k[3]]));
    expected.push_str(&bulk("b3"));
    pipeline.push_str(&enc(&["MGET", k[3], k[7]]));
    expected.push_str(&format!("*2\r\n{}{}", bulk("b3"), bulk("a7")));
    pipeline.push_str(&enc(&["DEL", k[7], k[8]]));
    expected.push_str(":2\r\n");
    pipeline.push_str(&enc(&["MGET", k[7], k[8], k[9]]));
    expected.push_str(&format!("*3\r\n$-1\r\n$-1\r\n{}", bulk("a9")));
    for _ in 0..5 {
        pipeline.push_str(&enc(&["MGET", k[0], k[1], k[2]]));
        expected.push_str(&format!("*3\r\n{}{}{}", bulk("a0"), bulk("a1"), bulk("a2")));
    }
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = vec![0u8; expected.len()];
    c.read_exact(&mut got).unwrap();
    assert_eq!(String::from_utf8_lossy(&got), expected);
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");

    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_keyspace_events_match_when_pipeline_is_batched_e2e() {
    let port = 17107;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        "+OK\r\n"
    );
    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let r = resp_cmd(&mut sub, &["PSUBSCRIBE", "__keyevent@0__:*"]);
    assert!(r.contains("psubscribe"), "{r}");

    let enc = |args: &[&str]| {
        let mut s = format!("*{}\r\n", args.len());
        for a in args {
            s.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        s
    };
    // Keys on every shard; one pipeline of batched writes and reads.
    let keys: Vec<String> = (0..8).map(|i| format!("ev:{i}")).collect();
    let mut pipeline = String::new();
    let mut expected_replies = String::new();
    let mut expected_events: Vec<(String, String)> = Vec::new();
    for k in &keys {
        pipeline.push_str(&enc(&["SET", k, "v"]));
        expected_replies.push_str("+OK\r\n");
        expected_events.push(("set".into(), k.clone()));
        pipeline.push_str(&enc(&["GET", k]));
        expected_replies.push_str("$1\r\nv\r\n");
        pipeline.push_str(&enc(&["EXPIRE", k, "100"]));
        expected_replies.push_str(":1\r\n");
        expected_events.push(("expire".into(), k.clone()));
        pipeline.push_str(&enc(&["DEL", k]));
        expected_replies.push_str(":1\r\n");
        expected_events.push(("del".into(), k.clone()));
        pipeline.push_str(&enc(&["LPUSH", k, "a"]));
        expected_replies.push_str(":1\r\n");
        expected_events.push(("lpush".into(), k.clone()));
    }
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = vec![0u8; expected_replies.len()];
    c.read_exact(&mut got).unwrap();
    assert_eq!(String::from_utf8_lossy(&got), expected_replies);

    // Collect pmessage frames: event name from the channel, key as payload.
    let mut raw = Vec::new();
    let mut buf = [0u8; 65536];
    while let Ok(n) = sub.read(&mut buf) {
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&raw).to_string();
    let parts: Vec<&str> = text.split("\r\n").collect();
    let mut events = Vec::new();
    let mut i = 0;
    while i + 8 < parts.len() {
        if parts[i] == "*4" && parts[i + 2] == "pmessage" {
            let event = parts[i + 6].trim_start_matches("__keyevent@0__:");
            events.push((event.to_string(), parts[i + 8].to_string()));
            i += 9;
        } else {
            i += 1;
        }
    }
    // Same events, and in command order for each key.
    for k in &keys {
        let want: Vec<_> = expected_events.iter().filter(|(_, key)| key == k).collect();
        let have: Vec<_> = events.iter().filter(|(_, key)| key == k).collect();
        assert_eq!(have, want, "events for {k}: {text}");
    }
    assert_eq!(events.len(), expected_events.len(), "{text}");

    drop(sub);
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_string_commands_emit_keyspace_events_e2e() {
    let port = 16969;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let got = keyevents_for(
        port,
        &[
            &["SET", "s:getset", "v"],
            &["SET", "s:getdel", "v"],
            &["SET", "s:getex", "v"],
        ],
        &[
            &["INCR", "s:incr"],
            &["DECRBY", "s:incr", "3"],
            &["INCRBYFLOAT", "s:float", "1.5"],
            &["APPEND", "s:append", "x"],
            &["SETNX", "s:nx", "v"],
            &["SETNX", "s:nx", "w"],
            &["GETSET", "s:getset", "w"],
            &["GETDEL", "s:getdel"],
            &["GETDEL", "s:missing"],
            &["GETEX", "s:getex", "EX", "100"],
            &["GETEX", "s:getex", "PERSIST"],
            &["SETRANGE", "s:range", "2", "xy"],
            &["SETRANGE", "s:range", "0", ""],
            &["SETBIT", "s:bit", "7", "1"],
            &["PFADD", "s:hll", "a"],
            &["PFADD", "s:hll", "a"],
            &["MSET", "s:m1", "1", "s:m2", "2", "s:m3", "3"],
            &["MSETNX", "{s}n1", "1", "{s}n2", "2"],
        ],
    );
    let want = sorted_events(&[
        ("incrby", "s:incr"),
        ("incrby", "s:incr"),
        ("incrbyfloat", "s:float"),
        ("append", "s:append"),
        ("set", "s:nx"),
        ("set", "s:getset"),
        ("del", "s:getdel"),
        ("expire", "s:getex"),
        ("persist", "s:getex"),
        ("setrange", "s:range"),
        ("setbit", "s:bit"),
        ("pfadd", "s:hll"),
        ("set", "s:m1"),
        ("set", "s:m2"),
        ("set", "s:m3"),
        ("set", "{s}n1"),
        ("set", "{s}n2"),
    ]);
    assert_eq!(got, want);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_multi_key_del_emits_del_per_key_e2e() {
    let port = 16968;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let keys: Vec<String> = (0..8).map(|i| format!("mdel:{i}")).collect();
    let setup: Vec<Vec<&str>> = keys.iter().map(|k| vec!["SET", k.as_str(), "v"]).collect();
    let setup_refs: Vec<&[&str]> = setup.iter().map(|c| c.as_slice()).collect();
    let mut del = vec!["DEL"];
    del.extend(keys.iter().map(|k| k.as_str()));
    del.push("mdel:missing");
    let got = keyevents_for(port, &setup_refs, &[&del]);
    let want: Vec<(&str, &str)> = keys.iter().map(|k| ("del", k.as_str())).collect();
    assert_eq!(got, sorted_events(&want));
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_collection_commands_emit_keyspace_events_e2e() {
    let port = 16967;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let got = keyevents_for(
        port,
        &[
            &["RPUSH", "c:list", "a", "b", "c", "b"],
            &["RPUSH", "c:trim", "a"],
            &["SADD", "c:set", "a"],
            &["ZADD", "c:zset", "1", "a", "2", "b"],
        ],
        &[
            &["HSETNX", "c:hash", "f", "1"],
            &["HSETNX", "c:hash", "f", "2"],
            &["HMSET", "c:hash", "g", "1"],
            &["HINCRBY", "c:hash", "f", "2"],
            &["HINCRBYFLOAT", "c:hash", "g", "0.5"],
            &["LSET", "c:list", "0", "z"],
            &["LINSERT", "c:list", "BEFORE", "c", "y"],
            &["LREM", "c:list", "0", "b"],
            &["LTRIM", "c:trim", "1", "0"],
            &["SPOP", "c:set"],
            &["ZPOPMIN", "c:zset"],
            &["ZPOPMAX", "c:zset"],
        ],
    );
    let want = sorted_events(&[
        ("hset", "c:hash"),
        ("hset", "c:hash"),
        ("hincrby", "c:hash"),
        ("hincrbyfloat", "c:hash"),
        ("lset", "c:list"),
        ("linsert", "c:list"),
        ("lrem", "c:list"),
        ("ltrim", "c:trim"),
        ("del", "c:trim"),
        ("spop", "c:set"),
        ("del", "c:set"),
        ("zpopmin", "c:zset"),
        ("zpopmax", "c:zset"),
        ("del", "c:zset"),
    ]);
    assert_eq!(got, want);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_pipelined_commands_counted_in_commandstats_e2e() {
    let port = 16965;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // One write so every command runs through the pipeline batching path.
    const N: usize = 3000;
    let mut pipeline = String::new();
    let mut push = |args: &[&str]| {
        pipeline.push_str(&format!("*{}\r\n", args.len()));
        for a in args {
            pipeline.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
    };
    for i in 0..N {
        let k = format!("cs:{i}");
        push(&["SET", &k, "v"]);
        push(&["GET", &k]);
    }
    push(&["MGET", "cs:0", "cs:1", "cs:2"]);
    push(&["PING"]);
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }

    let info = info_section(&mut c, "commandstats");
    for (cmd, calls) in [("set", N), ("get", N), ("mget", 1)] {
        let want = format!("cmdstat_{cmd}:calls={calls},");
        assert!(info.contains(&want), "missing {want:?} in:\n{info}");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_commandstats_reports_measured_usec_e2e() {
    let port = 16964;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    assert_eq!(resp_cmd(&mut c, &["DEBUG", "SLEEP", "0.05"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "us:k", "v"]), "+OK\r\n");

    // Pipelined commands take the batched path.
    let mut pipeline = String::new();
    for i in 0..2000 {
        let k = format!("us:{i}");
        pipeline.push_str(&format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\nv\r\n",
            k.len(),
            k
        ));
        pipeline.push_str(&format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", k.len(), k));
    }
    pipeline.push_str("*1\r\n$4\r\nPING\r\n");
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }

    let info = info_section(&mut c, "commandstats");
    let debug_usec = cmdstat_field(&info, "debug", "usec");
    assert!(
        debug_usec >= 50_000.0,
        "DEBUG SLEEP 0.05 took {debug_usec}us:\n{info}"
    );
    assert!(
        cmdstat_field(&info, "debug", "usec_per_call") >= 50_000.0,
        "{info}"
    );
    for cmd in ["set", "get"] {
        let usec = cmdstat_field(&info, cmd, "usec");
        let per_call = cmdstat_field(&info, cmd, "usec_per_call");
        assert!(usec > 0.0, "{cmd} usec {usec}:\n{info}");
        assert!(per_call < 1000.0, "{cmd} usec_per_call {per_call}:\n{info}");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_incr_decr_keep_their_own_command_name_e2e() {
    let port = 16963;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let setuser = [
        "ACL", "SETUSER", "u", "on", "nopass", "~*", "+@all", "-incr",
    ];
    assert_eq!(resp_cmd(&mut admin, &setuser), "+OK\r\n");

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["AUTH", "u", "x"]), "+OK\r\n");
    // `-incr` must deny INCR without denying INCRBY, DECR or DECRBY.
    let denied = resp_cmd(&mut c, &["INCR", "ctr"]);
    assert!(denied.starts_with("-NOPERM"), "INCR: {denied:?}");
    assert_eq!(resp_cmd(&mut c, &["INCRBY", "ctr", "1"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["DECR", "ctr"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["DECRBY", "ctr", "2"]), ":-2\r\n");

    let info = info_section(&mut admin, "commandstats");
    for cmd in ["incrby", "decr", "decrby"] {
        assert_eq!(cmdstat_field(&info, cmd, "calls"), 1.0, "{cmd}:\n{info}");
    }
    drop(c);
    drop(admin);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_latency_histogram_reports_real_buckets_e2e() {
    let port = 16962;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    resp_cmd_full(&mut c, &["SET", "warm", "v"]);
    assert_eq!(resp_cmd(&mut c, &["CONFIG", "RESETSTAT"]), "+OK\r\n");
    // After a reset only CONFIG RESETSTAT itself has been recorded.
    let all = resp_cmd_full(&mut c, &["LATENCY", "HISTOGRAM"]);
    assert!(
        all.starts_with("*2\r\n$16\r\nconfig|resetstat\r\n"),
        "{all:?}"
    );
    assert!(all.contains("calls\r\n:1\r\n"), "{all:?}");

    for i in 0..5 {
        let k = format!("lh:{i}");
        assert_eq!(resp_cmd(&mut c, &["SET", &k, "v"]), "+OK\r\n");
    }
    // Pipelined commands go through the batching path and must count too.
    let mut pipeline = String::new();
    for i in 0..100 {
        let k = format!("lhp:{i}");
        pipeline.push_str(&format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n",
            k.len()
        ));
    }
    pipeline.push_str("*1\r\n$4\r\nPING\r\n");
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(resp_cmd(&mut c, &["DEBUG", "SLEEP", "0.02"]), "+OK\r\n");
    assert!(resp_cmd(&mut c, &["CONFIG", "GET", "maxmemory"]).starts_with("*2\r\n"));

    let set = resp_cmd_full(&mut c, &["LATENCY", "HISTOGRAM", "set"]);
    assert!(set.starts_with("*2\r\n$3\r\nset\r\n"), "{set:?}");
    assert!(set.contains("calls\r\n:105\r\n"), "{set:?}");
    let pairs = histogram_pairs(&set);
    assert_eq!(pairs.last().unwrap().1, 105, "{set:?}");
    assert!(pairs.iter().all(|&(b, _)| b.is_power_of_two()), "{set:?}");

    // A 20ms command must land in a bucket of at least 16384 usec.
    let debug = resp_cmd_full(&mut c, &["LATENCY", "HISTOGRAM", "debug"]);
    let pairs = histogram_pairs(&debug);
    assert_eq!(pairs.len(), 1, "{debug:?}");
    assert!(pairs[0].0 >= 16384 && pairs[0].1 == 1, "{debug:?}");

    // A parent name selects all of its subcommands.
    let config = resp_cmd_full(&mut c, &["LATENCY", "HISTOGRAM", "config"]);
    assert!(config.starts_with("*4\r\n"), "{config:?}");
    assert!(config.contains("config|get") && config.contains("config|resetstat"));
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_info_stats_counts_commands_bytes_and_connections_e2e() {
    let port = 16961;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["CONFIG", "RESETSTAT"]), "+OK\r\n");

    let mut sent = 0usize;
    let mut received = 0usize;
    for i in 0..10 {
        let k = format!("st:{i}");
        let req = format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len());
        sent += req.len();
        received += send_and_read(&mut c, req.as_bytes()).len();
    }
    // 200 pipelined GETs plus a PING: 201 commands in one write.
    let mut pipeline = String::new();
    for i in 0..200 {
        let k = format!("st:{}", i % 10);
        pipeline.push_str(&format!("*2\r\n$3\r\nGET\r\n${}\r\n{k}\r\n", k.len()));
    }
    pipeline.push_str("*1\r\n$4\r\nPING\r\n");
    sent += pipeline.len();
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    received += got.len();

    let info = info_section(&mut c, "stats");
    // CONFIG RESETSTAT itself is counted after the reset, like in Redis.
    assert_eq!(info_num(&info, "total_commands_processed"), 212.0, "{info}");
    let net_in = info_num(&info, "total_net_input_bytes") as usize;
    assert!(
        net_in >= sent && net_in < sent + 200,
        "{net_in} vs {sent}:\n{info}"
    );
    let net_out = info_num(&info, "total_net_output_bytes") as usize;
    assert!(net_out >= received, "{net_out} vs {received}:\n{info}");
    assert_eq!(info_num(&info, "total_connections_received"), 0.0, "{info}");

    // Accepted and rejected connections.
    let extra: Vec<TcpStream> = (0..2)
        .map(|_| {
            let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            assert_eq!(resp_cmd(&mut s, &["PING"]), "+PONG\r\n");
            s
        })
        .collect();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxclients", "3"]),
        "+OK\r\n"
    );
    let mut over = TcpStream::connect(("127.0.0.1", port)).unwrap();
    over.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut reply = [0u8; 128];
    let n = over.read(&mut reply).unwrap();
    assert!(reply[..n].starts_with(b"-ERR max number of clients"));
    drop(over);
    drop(extra);

    // Keep a steady load for a while so the 100ms rate samples see it.
    let deadline = std::time::Instant::now() + Duration::from_millis(800);
    while std::time::Instant::now() < deadline {
        assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    }
    let info = info_section(&mut c, "stats");
    assert_eq!(info_num(&info, "total_connections_received"), 2.0, "{info}");
    assert_eq!(info_num(&info, "rejected_connections"), 1.0, "{info}");
    assert!(info_num(&info, "instantaneous_ops_per_sec") > 0.0, "{info}");
    assert!(info_num(&info, "instantaneous_input_kbps") > 0.0, "{info}");
    assert!(info_num(&info, "instantaneous_output_kbps") > 0.0, "{info}");
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_info_stats_counts_keyspace_hits_and_misses_e2e() {
    let port = 16959;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Writes do not count.
    assert_eq!(resp_cmd(&mut c, &["SET", "k1", "v"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["HSET", "h", "f", "v"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["RPUSH", "l", "a"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["SADD", "st", "a"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["ZADD", "z", "1", "a"]), ":1\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["SET", "gone", "v", "PX", "1"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["CONFIG", "RESETSTAT"]), "+OK\r\n");
    std::thread::sleep(Duration::from_millis(5));

    let hits_misses = |c: &mut TcpStream| {
        let info = info_section(c, "stats");
        (
            info_num(&info, "keyspace_hits"),
            info_num(&info, "keyspace_misses"),
        )
    };
    assert_eq!(hits_misses(&mut c), (0.0, 0.0));

    assert_eq!(resp_cmd(&mut c, &["GET", "k1"]), "$1\r\nv\r\n"); // hit
    assert_eq!(resp_cmd(&mut c, &["GET", "nokey"]), "$-1\r\n"); // miss
    assert_eq!(resp_cmd(&mut c, &["GET", "gone"]), "$-1\r\n"); // expired: miss
    resp_cmd(&mut c, &["GET", "l"]); // wrong type, but the key exists: hit
    // 2 hits, 1 miss.
    assert_eq!(
        resp_cmd(&mut c, &["MGET", "k1", "nokey", "k1"]),
        "*3\r\n$1\r\nv\r\n$-1\r\n$1\r\nv\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["HGET", "h", "f"]), "$1\r\nv\r\n"); // hit
    assert_eq!(resp_cmd(&mut c, &["HGET", "h", "nofield"]), "$-1\r\n"); // key exists: hit
    assert_eq!(resp_cmd(&mut c, &["HGET", "noh", "f"]), "$-1\r\n"); // miss
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "k1", "nokey"]), ":1\r\n"); // 1 hit, 1 miss
    assert_eq!(
        resp_cmd(&mut c, &["LRANGE", "l", "0", "-1"]),
        "*1\r\n$1\r\na\r\n"
    ); // hit
    assert_eq!(hits_misses(&mut c), (8.0, 5.0));

    // Pipelined reads take the batched path (the fast paths of each shard).
    assert_eq!(resp_cmd_full(&mut c, &["GET", "k1"]), "$1\r\nv\r\n"); // hit
    let batch: &[(&[&str], bool)] = &[
        (&["GET", "nokey"], false),
        (&["HGET", "h", "f"], true),
        (&["HGET", "noh", "f"], false),
        (&["EXISTS", "k1"], true),
        (&["EXISTS", "nokey"], false),
        (&["SISMEMBER", "st", "a"], true),
        (&["SISMEMBER", "nost", "a"], false),
        (&["LRANGE", "l", "0", "-1"], true),
        (&["LRANGE", "nol", "0", "-1"], false),
        (&["ZRANGE", "z", "0", "-1"], true),
        (&["ZRANGE", "noz", "0", "-1"], false),
    ];
    let mut pipeline = String::new();
    for (args, _) in batch {
        pipeline.push_str(&format!("*{}\r\n", args.len()));
        for a in *args {
            pipeline.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
    }
    pipeline.push_str("*1\r\n$4\r\nPING\r\n");
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    let hits = batch.iter().filter(|(_, hit)| *hit).count() as f64;
    let misses = batch.len() as f64 - hits;
    assert_eq!(hits_misses(&mut c), (9.0 + hits, 5.0 + misses));

    assert_eq!(resp_cmd(&mut c, &["CONFIG", "RESETSTAT"]), "+OK\r\n");
    assert_eq!(hits_misses(&mut c), (0.0, 0.0));
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_get_getex_lcs_on_non_string_key_return_wrongtype_e2e() {
    let port = 16955;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    const WRONGTYPE: &str =
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    // Enough keys to land on the connection's own shard and on the other one.
    let keys: Vec<String> = (0..8).map(|i| format!("wt:{i}")).collect();
    for k in &keys {
        assert_eq!(resp_cmd(&mut c, &["RPUSH", k, "a"]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["GET", k]), WRONGTYPE, "GET {k}");
        assert_eq!(resp_cmd(&mut c, &["GETEX", k]), WRONGTYPE, "GETEX {k}");
        assert_eq!(resp_cmd(&mut c, &["LCS", k, "nokey"]), WRONGTYPE, "LCS {k}");
        assert_eq!(resp_cmd(&mut c, &["GETEX", k, "EX", "100"]), WRONGTYPE);
        // The failed GETEX must not have set a TTL.
        assert_eq!(resp_cmd(&mut c, &["TTL", k]), ":-1\r\n");
    }
    // Pipelined GETs take the batched path.
    let mut pipeline = String::new();
    for k in &keys {
        pipeline.push_str(&format!("*2\r\n$3\r\nGET\r\n${}\r\n{k}\r\n", k.len()));
    }
    assert_eq!(resp_cmd_full(&mut c, &["GET", "wt:0"]), WRONGTYPE);
    pipeline.push_str("*1\r\n$4\r\nPING\r\n");
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(
        String::from_utf8(got).unwrap(),
        WRONGTYPE.repeat(keys.len()) + "+PONG\r\n"
    );
    // MGET reports non-string keys as nil, like Redis.
    assert_eq!(
        resp_cmd(&mut c, &["MGET", "wt:0", "wt:1"]),
        "*2\r\n$-1\r\n$-1\r\n"
    );
    // Strings and missing keys are unaffected.
    assert_eq!(resp_cmd(&mut c, &["SET", "s", "v"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "s"]), "$1\r\nv\r\n");
    assert_eq!(resp_cmd(&mut c, &["GETEX", "s"]), "$1\r\nv\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "nokey"]), "$-1\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_dbsize_flushdb_fan_out_to_all_shards_e2e() {
    let port = 16954;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // 200 keys land on all four shards.
    for i in 0..200 {
        assert_eq!(
            resp_cmd(&mut c, &["SET", &format!("fan:{i}"), "v"]),
            "+OK\r\n"
        );
    }
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":200\r\n");
    assert_eq!(resp_cmd(&mut c, &["FLUSHDB"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":0\r\n");
    for i in 0..200 {
        assert_eq!(resp_cmd(&mut c, &["EXISTS", &format!("fan:{i}")]), ":0\r\n");
    }
    for i in 0..50 {
        assert_eq!(
            resp_cmd(&mut c, &["SET", &format!("fan:{i}"), "v"]),
            "+OK\r\n"
        );
    }
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":50\r\n");
    assert_eq!(resp_cmd(&mut c, &["FLUSHALL"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":0\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_rename_and_copy_across_shards_e2e() {
    let port = 16951;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // With 4 shards most of these pairs land on different shards, which used
    // to fail with CROSSSLOT in standalone mode.
    for i in 0..16 {
        let (a, b, cp) = (format!("ra:{i}"), format!("rb:{i}"), format!("rc:{i}"));
        assert_eq!(resp_cmd(&mut c, &["RPUSH", &a, "x", "y"]), ":2\r\n");
        assert_eq!(resp_cmd(&mut c, &["PEXPIRE", &a, "100000"]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["RENAME", &a, &b]), "+OK\r\n", "{a}");
        assert_eq!(resp_cmd(&mut c, &["EXISTS", &a]), ":0\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["LRANGE", &b, "0", "-1"]),
            "*2\r\n$1\r\nx\r\n$1\r\ny\r\n"
        );
        let ttl: i64 = resp_cmd(&mut c, &["PTTL", &b])[1..].trim().parse().unwrap();
        assert!(ttl > 90_000 && ttl <= 100_000, "TTL kept: {ttl}");
        assert_eq!(resp_cmd(&mut c, &["COPY", &b, &cp]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["COPY", &b, &cp]), ":0\r\n");
        assert_eq!(resp_cmd(&mut c, &["LLEN", &cp]), ":2\r\n");
        assert_eq!(resp_cmd(&mut c, &["SET", &a, "s"]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut c, &["RENAMENX", &a, &b]), ":0\r\n");
        assert_eq!(resp_cmd(&mut c, &["COPY", &a, &cp, "REPLACE"]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["GET", &cp]), "$1\r\ns\r\n");
        assert_eq!(resp_cmd(&mut c, &["RENAME", &a, &b]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut c, &["GET", &b]), "$1\r\ns\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["RENAME", &a, &b]),
            "-ERR no such key\r\n"
        );
    }
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":32\r\n");

    // Inside MULTI/EXEC, where the transaction already holds the shard locks.
    assert_eq!(resp_cmd(&mut c, &["SET", "tx:a", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
    for (from, to) in [("tx:a", "tx:b"), ("tx:b", "tx:c"), ("tx:c", "tx:d")] {
        assert_eq!(resp_cmd(&mut c, &["RENAME", from, to]), "+QUEUED\r\n");
    }
    assert_eq!(resp_cmd(&mut c, &["EXEC"]), "*3\r\n+OK\r\n+OK\r\n+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "tx:d"]), "$1\r\n1\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["EXISTS", "tx:a", "tx:b", "tx:c"]),
        ":0\r\n"
    );

    // Concurrent renames of one key must not duplicate it. The targets share
    // a hash tag, so for each source the renames are either all local to one
    // shard or all cross-shard (serialized by the shard locks).
    for j in 0..4 {
        let src = format!("rs:{j}");
        assert_eq!(resp_cmd(&mut c, &["SET", &src, "v"]), "+OK\r\n");
        let racers: Vec<_> = (0..8)
            .map(|i| {
                let src = src.clone();
                std::thread::spawn(move || {
                    let mut r = TcpStream::connect(("127.0.0.1", port)).unwrap();
                    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    resp_cmd(&mut r, &["RENAME", &src, &format!("{{rdst}}:{j}:{i}")])
                })
            })
            .collect();
        let oks = racers
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|r| r == "+OK\r\n")
            .count();
        assert_eq!(oks, 1, "{src}");
        let keys = resp_cmd(&mut c, &["KEYS", &format!("{{rdst}}:{j}:*")]);
        assert!(keys.starts_with("*1\r\n"), "{src}: {keys}");
        assert_eq!(resp_cmd(&mut c, &["EXISTS", &src]), ":0\r\n");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_lmove_and_smove_across_shards_e2e() {
    let port = 17105; // 16950 is taken by an in-process server for the whole run
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // With 4 shards most of these pairs land on different shards, which used
    // to fail with CROSSSLOT in standalone mode.
    for i in 0..16 {
        let (la, lb, sa, sb, st) = (
            format!("la:{i}"),
            format!("lb:{i}"),
            format!("sa:{i}"),
            format!("sb:{i}"),
            format!("st:{i}"),
        );
        assert_eq!(resp_cmd(&mut c, &["RPUSH", &la, "a", "b", "c"]), ":3\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["LMOVE", &la, &lb, "LEFT", "RIGHT"]),
            "$1\r\na\r\n",
            "{la}"
        );
        assert_eq!(
            resp_cmd(&mut c, &["LMOVE", &la, &lb, "RIGHT", "LEFT"]),
            "$1\r\nc\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["RPOPLPUSH", &la, &lb]), "$1\r\nb\r\n");
        assert_eq!(resp_cmd(&mut c, &["RPOPLPUSH", &la, &lb]), "$-1\r\n");
        assert_eq!(resp_cmd(&mut c, &["EXISTS", &la]), ":0\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["LRANGE", &lb, "0", "-1"]),
            "*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\na\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["SET", &st, "x"]), "+OK\r\n");
        let wrongtype = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        assert_eq!(
            resp_cmd(&mut c, &["LMOVE", &la, &st, "LEFT", "LEFT"]),
            "$-1\r\n"
        );
        assert_eq!(
            resp_cmd(&mut c, &["LMOVE", &lb, &st, "LEFT", "LEFT"]),
            wrongtype
        );
        assert_eq!(
            resp_cmd(&mut c, &["LMOVE", &st, &lb, "LEFT", "LEFT"]),
            wrongtype
        );
        assert_eq!(resp_cmd(&mut c, &["LLEN", &lb]), ":3\r\n");

        assert_eq!(resp_cmd(&mut c, &["SADD", &sa, "m", "n"]), ":2\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["SMOVE", &sa, &sb, "m"]),
            ":1\r\n",
            "{sa}"
        );
        assert_eq!(resp_cmd(&mut c, &["SMOVE", &sa, &sb, "zz"]), ":0\r\n");
        assert_eq!(resp_cmd(&mut c, &["SADD", &sb, "n"]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["SMOVE", &sa, &sb, "n"]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["EXISTS", &sa]), ":0\r\n");
        assert_eq!(resp_cmd(&mut c, &["SCARD", &sb]), ":2\r\n");
        assert_eq!(resp_cmd(&mut c, &["SMOVE", &sa, &st, "m"]), ":0\r\n");
        assert_eq!(resp_cmd(&mut c, &["SMOVE", &sb, &st, "m"]), wrongtype);
        assert_eq!(resp_cmd(&mut c, &["SMOVE", &st, &sb, "m"]), wrongtype);
        assert_eq!(resp_cmd(&mut c, &["SMOVE", &sb, &lb, "m"]), wrongtype);
        assert_eq!(resp_cmd(&mut c, &["SCARD", &sb]), ":2\r\n");
    }
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":48\r\n");

    // Inside MULTI/EXEC, where the transaction already holds the shard locks.
    assert_eq!(resp_cmd(&mut c, &["RPUSH", "tx:a", "1", "2"]), ":2\r\n");
    assert_eq!(resp_cmd(&mut c, &["SADD", "tx:s1", "m"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
    for (from, to) in [("tx:a", "tx:b"), ("tx:b", "tx:c"), ("tx:a", "tx:d")] {
        assert_eq!(resp_cmd(&mut c, &["RPOPLPUSH", from, to]), "+QUEUED\r\n");
    }
    assert_eq!(
        resp_cmd(&mut c, &["SMOVE", "tx:s1", "tx:s2", "m"]),
        "+QUEUED\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["EXEC"]),
        "*4\r\n$1\r\n2\r\n$1\r\n2\r\n$1\r\n1\r\n:1\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["LRANGE", "tx:c", "0", "-1"]),
        "*1\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["LRANGE", "tx:d", "0", "-1"]),
        "*1\r\n$1\r\n1\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["SISMEMBER", "tx:s2", "m"]), ":1\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["EXISTS", "tx:a", "tx:b", "tx:s1"]),
        ":0\r\n"
    );

    // Concurrent moves out of one list must neither lose nor duplicate
    // elements. Each racer moves into its own list; the targets share a hash
    // tag, so the moves are all local to one shard or all cross-shard.
    for j in 0..4 {
        let src = format!("ls:{j}");
        let elems: Vec<String> = (0..64).map(|e| e.to_string()).collect();
        let mut args = vec!["RPUSH", &src];
        args.extend(elems.iter().map(String::as_str));
        assert_eq!(resp_cmd(&mut c, &args), ":64\r\n");
        let racers: Vec<_> = (0..8)
            .map(|i| {
                let src = src.clone();
                std::thread::spawn(move || {
                    let mut r = TcpStream::connect(("127.0.0.1", port)).unwrap();
                    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    let dst = format!("{{ldst}}:{j}:{i}");
                    (0..16)
                        .filter(|_| resp_cmd(&mut r, &["RPOPLPUSH", &src, &dst]) != "$-1\r\n")
                        .count()
                })
            })
            .collect();
        let moved: usize = racers.into_iter().map(|t| t.join().unwrap()).sum();
        assert_eq!(moved, 64, "{src}");
        assert_eq!(resp_cmd(&mut c, &["EXISTS", &src]), ":0\r\n");
        let total: usize = (0..8)
            .map(|i| {
                let n = resp_cmd(&mut c, &["LLEN", &format!("{{ldst}}:{j}:{i}")]);
                n[1..].trim().parse::<usize>().unwrap()
            })
            .sum();
        assert_eq!(total, 64, "{src}");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_blmove_across_shards_e2e() {
    let port = 16937;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let connect = move || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c
    };
    let mut c = connect();
    // Pairs of keys on different shards: these used to fail with CROSSSLOT.
    let pair = |prefix: &str| -> (String, String) {
        let a = format!("{prefix}:src");
        let b = (0..)
            .map(|i| format!("{prefix}:dst{i}"))
            .find(|b| target_shard(b.as_bytes(), 4) != target_shard(a.as_bytes(), 4))
            .unwrap();
        (a, b)
    };
    let wait_blocked = |c: &mut TcpStream, n: usize| {
        for _ in 0..200 {
            if resp_cmd(c, &["INFO", "clients"]).contains(&format!("blocked_clients:{n}\r\n")) {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("client never blocked");
    };
    let is_nil = |r: &str| r == "$-1\r\n" || r == "*-1\r\n";

    // Source has an element: moved right away.
    let (src, dst) = pair("now");
    resp_cmd(&mut c, &["RPUSH", &src, "a", "b"]);
    assert_eq!(
        resp_cmd(&mut c, &["BLMOVE", &src, &dst, "LEFT", "RIGHT", "1"]),
        "$1\r\na\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["BRPOPLPUSH", &src, &dst, "1"]),
        "$1\r\nb\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["LRANGE", &dst, "0", "-1"]),
        "*2\r\n$1\r\nb\r\n$1\r\na\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["EXISTS", &src]), ":0\r\n");

    // Empty source: times out.
    let r = resp_cmd(&mut c, &["BLMOVE", &src, &dst, "LEFT", "LEFT", "0.1"]);
    assert!(is_nil(&r), "{r}");

    // Empty source: blocks until a push, then moves the element.
    for (cmd, from) in [
        (vec!["BLMOVE", "RIGHT", "LEFT"], "RIGHT"),
        (vec!["BRPOPLPUSH"], "RIGHT"),
        (vec!["BLMOVE", "LEFT", "RIGHT"], "LEFT"),
    ] {
        let (src, dst) = pair(&format!("blk{}{}", cmd.join(""), from));
        let (s2, d2) = (src.clone(), dst.clone());
        let waiter = thread::spawn(move || {
            let mut w = connect();
            let mut args = vec![cmd[0], &s2, &d2];
            args.extend(&cmd[1..]);
            args.push("5");
            resp_cmd(&mut w, &args)
        });
        wait_blocked(&mut c, 1);
        assert_eq!(resp_cmd(&mut c, &["RPUSH", &src, "x", "y"]), ":2\r\n");
        let (moved, kept) = if from == "RIGHT" {
            ("y", "x")
        } else {
            ("x", "y")
        };
        assert_eq!(waiter.join().unwrap(), format!("$1\r\n{moved}\r\n"));
        assert_eq!(
            resp_cmd(&mut c, &["LRANGE", &src, "0", "-1"]),
            format!("*1\r\n$1\r\n{kept}\r\n")
        );
        assert_eq!(
            resp_cmd(&mut c, &["LRANGE", &dst, "0", "-1"]),
            format!("*1\r\n$1\r\n{moved}\r\n")
        );
    }

    // The destination turned into another type while blocked: the element
    // stays in the source and the client gets WRONGTYPE.
    let (src, dst) = pair("wt");
    let (s2, d2) = (src.clone(), dst.clone());
    let waiter = thread::spawn(move || {
        let mut w = connect();
        resp_cmd(&mut w, &["BLMOVE", &s2, &d2, "LEFT", "LEFT", "5"])
    });
    wait_blocked(&mut c, 1);
    assert_eq!(resp_cmd(&mut c, &["SET", &dst, "str"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["RPUSH", &src, "p", "q"]), ":2\r\n");
    assert_eq!(
        waiter.join().unwrap(),
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["LRANGE", &src, "0", "-1"]),
        "*2\r\n$1\r\np\r\n$1\r\nq\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["GET", &dst]), "$3\r\nstr\r\n");

    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_msetnx_across_shards_e2e() {
    let port = 16949;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // 8 keys span all 4 shards, which used to fail with CROSSSLOT in
    // standalone mode.
    let keys: Vec<String> = (0..8).map(|i| format!("nx:{i}")).collect();
    let mut args = vec!["MSETNX".to_string()];
    for k in &keys {
        args.extend([k.clone(), format!("v-{k}")]);
    }
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    assert_eq!(resp_cmd(&mut c, &args), ":1\r\n");
    for k in &keys {
        assert_eq!(
            resp_cmd(&mut c, &["GET", k]),
            format!("${}\r\nv-{k}\r\n", k.len() + 2)
        );
    }
    // One existing key anywhere blocks all of them.
    assert_eq!(resp_cmd(&mut c, &args), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["DEL", "nx:0", "nx:1", "nx:2"]), ":3\r\n");
    assert_eq!(resp_cmd(&mut c, &args), ":0\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["EXISTS", "nx:0", "nx:1", "nx:2"]),
        ":0\r\n"
    );
    // Duplicate keys: the last value wins, like MSET.
    assert_eq!(
        resp_cmd(&mut c, &["MSETNX", "d:a", "1", "d:b", "2", "d:a", "3"]),
        ":1\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["MGET", "d:a", "d:b"]),
        "*2\r\n$1\r\n3\r\n$1\r\n2\r\n"
    );
    // Inside MULTI/EXEC, where the transaction already holds the locks.
    assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["MSETNX", "t:a", "1", "t:b", "2", "t:c", "3"]),
        "+QUEUED\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["MSETNX", "t:c", "x", "t:d", "4"]),
        "+QUEUED\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["EXEC"]), "*2\r\n:1\r\n:0\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["MGET", "t:a", "t:b", "t:c", "t:d"]),
        "*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$-1\r\n"
    );
    // Racing MSETNX calls over overlapping keys: exactly one wins and its
    // values are the ones set, never a mix.
    for j in 0..4 {
        let racers: Vec<_> = (0..8)
            .map(|i| {
                std::thread::spawn(move || {
                    let mut r = TcpStream::connect(("127.0.0.1", port)).unwrap();
                    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    let mut a = vec!["MSETNX".to_string()];
                    for k in 0..6 {
                        a.extend([format!("race:{j}:{k}"), i.to_string()]);
                    }
                    let a: Vec<&str> = a.iter().map(String::as_str).collect();
                    resp_cmd(&mut r, &a)
                })
            })
            .collect();
        let wins = racers
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|r| r == ":1\r\n")
            .count();
        assert_eq!(wins, 1, "round {j}");
        let vals: Vec<String> = (0..6)
            .map(|k| resp_cmd(&mut c, &["GET", &format!("race:{j}:{k}")]))
            .collect();
        assert!(vals.iter().all(|v| v == &vals[0]), "round {j}: {vals:?}");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_bitop_across_shards_e2e() {
    let port = 16948;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // With 4 shards most of these key triples span shards, which used to
    // fail with CROSSSLOT in standalone mode.
    for i in 0..16 {
        let (a, b, d) = (format!("ba:{i}"), format!("bb:{i}"), format!("bd:{i}"));
        assert_eq!(resp_cmd(&mut c, &["SET", &a, "\x0f\x0f"]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut c, &["SET", &b, "\x33"]), "+OK\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["BITOP", "AND", &d, &a, &b]),
            ":2\r\n",
            "{d}"
        );
        assert_eq!(resp_cmd(&mut c, &["GET", &d]), "$2\r\n\x03\x00\r\n");
        assert_eq!(resp_cmd(&mut c, &["BITOP", "or", &d, &a, &b]), ":2\r\n");
        assert_eq!(resp_cmd(&mut c, &["GET", &d]), "$2\r\n\x3f\x0f\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["BITOP", "XOR", &d, &a, &b, "nokey"]),
            ":2\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["GET", &d]), "$2\r\n\x3c\x0f\r\n");
        assert_eq!(resp_cmd(&mut c, &["BITOP", "NOT", &d, &b]), ":1\r\n");
        // !0x33 == 0xcc: 4 bits set.
        assert_eq!(resp_cmd(&mut c, &["BITCOUNT", &d]), ":4\r\n");
        // An empty result deletes the destination, like Redis.
        assert_eq!(
            resp_cmd(&mut c, &["BITOP", "AND", &d, "n1", "n2"]),
            ":0\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["EXISTS", &d]), ":0\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["RPUSH", &b, "x"]),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["DEL", &b]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["RPUSH", &b, "x"]), ":1\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["BITOP", "OR", &d, &a, &b]),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["EXISTS", &d]), ":0\r\n");
    }
    // Inside MULTI/EXEC, where the transaction already holds the locks.
    assert_eq!(resp_cmd(&mut c, &["SET", "tx:x", "\x01"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "tx:y", "\x02"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["BITOP", "OR", "tx:z", "tx:x", "tx:y"]),
        "+QUEUED\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["BITOP", "XOR", "tx:w", "tx:z", "tx:x"]),
        "+QUEUED\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["EXEC"]), "*2\r\n:1\r\n:1\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "tx:z"]), "$1\r\n\x03\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "tx:w"]), "$1\r\n\x02\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_writes_missing_from_hand_list_are_writes_e2e() {
    let port = 16947;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // A no-writes script must not be able to modify data with any write
    // command, including ones like SETBIT and SETRANGE.
    for call in [
        "redis.call('setbit',KEYS[1],0,1)",
        "redis.call('setrange',KEYS[1],0,'x')",
    ] {
        let script = format!("#!lua flags=no-writes\nreturn {call}");
        let r = resp_cmd(&mut c, &["EVAL", &script, "1", "ro:k"]);
        assert!(
            r.contains("Write commands are not allowed from read-only scripts"),
            "{call}: {r}"
        );
    }
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "ro:k"]), ":0\r\n");
    // SETBIT counts toward `save <secs> <changes>`.
    let changes = |c: &mut TcpStream| -> u64 {
        let info = resp_cmd(c, &["INFO", "persistence"]);
        let line = info
            .lines()
            .find(|l| l.starts_with("rdb_changes_since_last_save:"))
            .unwrap()
            .to_string();
        line.split(':').nth(1).unwrap().trim().parse().unwrap()
    };
    let before = changes(&mut c);
    assert_eq!(resp_cmd(&mut c, &["SETBIT", "bits", "3", "1"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["SETRANGE", "str", "0", "abc"]), ":3\r\n");
    assert_eq!(changes(&mut c), before + 2);
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_dirty_counts_only_real_changes_e2e() {
    let port = 16935u16;
    let ps = port.to_string();
    let mut server = spawn_rudis_listening(&["--port", &ps, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let dirty = |c: &mut TcpStream| -> u64 {
        let info = resp_cmd(c, &["INFO", "persistence"]);
        info.lines()
            .find_map(|l| l.strip_prefix("rdb_changes_since_last_save:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let d0 = dirty(&mut c);
    resp_cmd(&mut c, &["SETBIT", "bits", "0", "1"]);
    resp_cmd(&mut c, &["BITFIELD", "bf", "SET", "i5", "0", "1"]);
    assert_eq!(dirty(&mut c), d0 + 2);
    // Same values again: nothing changes.
    resp_cmd(&mut c, &["SETBIT", "bits", "0", "1"]);
    resp_cmd(&mut c, &["BITFIELD", "bf", "SET", "i5", "0", "1"]);
    assert_eq!(dirty(&mut c), d0 + 2);

    resp_cmd(&mut c, &["XADD", "s", "1-0", "f", "v"]);
    resp_cmd(&mut c, &["XGROUP", "CREATE", "s", "g", "0"]);
    resp_cmd(&mut c, &["XGROUP", "CREATECONSUMER", "s", "g", "alice"]);
    let d1 = dirty(&mut c);
    let r = resp_cmd(
        &mut c,
        &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", ">"],
    );
    assert!(r.contains("1-0"), "{r}");
    assert_eq!(dirty(&mut c), d1 + 1);
    // Re-reading the pending list is not a change.
    let r = resp_cmd(
        &mut c,
        &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "0"],
    );
    assert!(r.contains("1-0"), "{r}");
    assert_eq!(dirty(&mut c), d1 + 1);
    let _ = server.kill();
    let _ = server.wait();
}

#[test]
fn test_experimental_commands_off_by_default_e2e() {
    let connect = |port: u16| {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c
    };
    let probes: [&[&str]; 5] = [
        &["JSON.SET", "j", "$", "{\"a\":1}"],
        &["bf.add", "b", "x"],
        &["CMS.INITBYDIM", "c", "10", "5"],
        &["FT._LIST"],
        &["CRDT.SET", "k", "v"],
    ];

    let port = 16939u16;
    let ps = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &ps, "--threads", "2", "--no-pin"], port);
    let mut c = connect(port);
    for args in probes {
        let reply = resp_cmd(&mut c, args);
        assert!(
            reply.starts_with(&format!("-ERR unknown command '{}'", args[0])),
            "{args:?}: {reply}"
        );
    }
    // Core commands are unaffected.
    assert_eq!(resp_cmd(&mut c, &["SET", "a.b", "1"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);

    let port = 16938u16;
    let ps = port.to_string();
    let mut child = spawn_rudis_listening(
        &[
            "--port",
            &ps,
            "--threads",
            "2",
            "--no-pin",
            "--enable-experimental-commands",
            "yes",
        ],
        port,
    );
    let mut c = connect(port);
    assert_eq!(
        resp_cmd(&mut c, &["JSON.SET", "j", "$", "{\"a\":1}"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["BF.ADD", "b", "x"]), ":1\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_config_file_directives_are_applied_or_rejected_e2e() {
    let port = 16946;
    let dir = std::env::temp_dir().join(format!("rudis-conf-apply-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let included = dir.join("included.conf");
    std::fs::write(
        &included,
        "slowlog-log-slower-than 12345\n\
         client-output-buffer-limit normal 0 0 0\n\
         client-output-buffer-limit pubsub 1mb 512kb 30\n\
         notify-keyspace-events \"\"\n\
         databases 16\n",
    )
    .unwrap();
    let conf = dir.join("main.conf");
    std::fs::write(
        &conf,
        format!(
            "include {}\nuser alice on >alicepw ~* &* +@all\n",
            included.display()
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
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Directives from the included file reach the running server...
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "slowlog-log-slower-than"]),
        "*2\r\n$23\r\nslowlog-log-slower-than\r\n$5\r\n12345\r\n"
    );
    // ...including every line of a repeated directive.
    let obl = resp_cmd(&mut c, &["CONFIG", "GET", "client-output-buffer-limit"]);
    assert!(obl.contains("pubsub 1048576 524288 30"), "{obl}");
    // `user` lines create ACL users.
    assert_eq!(resp_cmd(&mut c, &["AUTH", "alice", "alicepw"]), "+OK\r\n");
    shutdown_and_wait(port, &mut child);

    // A typo or an unsupported security-relevant directive must stop startup
    // instead of being silently ignored.
    for bad in [
        "slowlog-log-slower-thn 10\n",
        "rename-command FLUSHALL \"\"\n",
        "maxclients lots\n",
    ] {
        std::fs::write(&conf, bad).unwrap();
        let status = rudis_bin()
            .args(args)
            .args(DEBUG_ON)
            .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn rudis");
        assert!(!status.success(), "rudis started with config {bad:?}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_metrics_port_serves_prometheus_over_http_e2e() {
    let (port, metrics_port) = (17077, 17078);
    let (port_s, metrics_s) = (port.to_string(), metrics_port.to_string());
    let mut child = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "2",
            "--no-pin",
            "--metrics-port",
            &metrics_s,
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "m:k", "v"]), "+OK\r\n");

    let res = http_get(metrics_port, "/metrics");
    assert!(res.starts_with("HTTP/1.1 200 OK\r\n"), "{res}");
    assert!(res.contains("Content-Type: text/plain; version=0.0.4"));
    let value = |name: &str| -> u64 {
        let line = res
            .lines()
            .find(|l| l.starts_with(&format!("{name} ")))
            .unwrap_or_else(|| panic!("{name} missing in {res}"));
        line.split(' ').nth(1).unwrap().parse().unwrap()
    };
    assert!(value("rudis_commands_processed_total") >= 1);
    assert!(value("rudis_used_memory_bytes") > 0);
    assert_eq!(value("rudis_connected_clients"), 1);
    // Per-command counters and latency histograms, persistence and
    // replication state.
    assert!(
        value("rudis_command_calls_total{cmd=\"set\"}") >= 1,
        "{res}"
    );
    assert!(value("rudis_command_latency_seconds_count{cmd=\"set\"}") >= 1);
    assert!(res.contains("rudis_command_latency_seconds_bucket{cmd=\"set\",le=\"+Inf\"}"));
    assert_eq!(value("rudis_rdb_bgsave_in_progress"), 0);
    assert_eq!(value("rudis_replication_connected_slaves"), 0);
    assert!(res.contains("rudis_replication_role{role=\"master\"} 1"));
    assert!(http_get(metrics_port, "/").starts_with("HTTP/1.1 404 "));

    // A metrics port that can't be bound is a startup error, not a silent
    // missing endpoint.
    let status = rudis_bin()
        .args(["--port", "17079", "--threads", "1", "--no-pin"])
        .args(["--metrics-port", &metrics_s])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(1));

    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_second_server_on_a_port_in_use_refuses_to_start_e2e() {
    let port = 17096;
    let port_s = port.to_string();
    let free_s = 17097.to_string();
    let args = ["--port", &port_s, "--threads", "2", "--no-pin"];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(send_and_read(&mut c, b"SET owner first\r\n"), "+OK\r\n");

    // Listeners use SO_REUSEPORT, so without a check the second server
    // would bind too and take a share of the connections.
    let err = rudis_startup_error(&args);
    assert!(
        err.contains(&format!(
            "Could not create server TCP listening socket 0.0.0.0:{port}: bind: Address already in use"
        )),
        "{err}"
    );
    assert!(
        err.contains(&format!("Failed listening on port {port} (tcp), aborting.")),
        "{err}"
    );
    // The TLS port is checked the same way.
    let err = rudis_startup_error(&[
        "--port",
        &free_s,
        "--tls-port",
        &port_s,
        "--threads",
        "1",
        "--no-pin",
    ]);
    assert!(
        err.contains(&format!("Failed listening on port {port} (tcp), aborting.")),
        "{err}"
    );

    // Every new connection still reaches the first server.
    for _ in 0..20 {
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        assert_eq!(send_and_read(&mut c, b"GET owner\r\n"), "$5\r\nfirst\r\n");
    }

    // The server closes `c` as it exits, leaving that socket on its port
    // (FIN_WAIT/TIME_WAIT); it must not block a restart.
    shutdown_and_wait(port, &mut child);
    drop(c);
    let mut child = spawn_rudis_listening(&args, port);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_sigterm_drains_clients_and_keeps_every_acked_write_e2e() {
    let port = 17073;
    let dir = std::env::temp_dir().join(format!("rudis-drain-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "4",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let mut idle = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut idle, &["PING"]), "+PONG\r\n");

    // Writers keep going until the server closes them; each records the
    // last key it got +OK for.
    let writers: Vec<_> = (0..4)
        .map(|w| {
            thread::spawn(move || {
                let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
                c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                let mut acked = 0usize;
                loop {
                    let key = format!("drain:{w}:{}", acked + 1);
                    let cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len());
                    if c.write_all(cmd.as_bytes()).is_err() {
                        return acked;
                    }
                    let mut buf = [0u8; 5];
                    match c.read_exact(&mut buf) {
                        Ok(()) if &buf == b"+OK\r\n" => acked += 1,
                        _ => return acked,
                    }
                }
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(300));
    // A command still running when the signal arrives must get its reply.
    let mut blocked = TcpStream::connect(("127.0.0.1", port)).unwrap();
    blocked
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    blocked
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$12\r\ndrain:nolist\r\n$3\r\n0.8\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    let started = std::time::Instant::now();
    // SAFETY: kill(2) takes no pointers; the pid is our own child, not yet
    // reaped, so it cannot have been reused.
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };

    idle.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut buf = [0u8; 16];
    assert_eq!(
        idle.read(&mut buf).unwrap_or(0),
        0,
        "idle client not closed"
    );
    let mut reply = [0u8; 5];
    blocked
        .read_exact(&mut reply)
        .expect("in-flight BLPOP lost its reply");
    assert_eq!(&reply, b"*-1\r\n");
    let acked: Vec<usize> = writers.into_iter().map(|w| w.join().unwrap()).collect();
    assert!(
        acked.iter().all(|&n| n > 0),
        "writers made no progress: {acked:?}"
    );
    let status = wait_exit(&mut child, Duration::from_secs(10)).expect("SIGTERM must stop rudis");
    assert!(status.success(), "{status}");
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "shutdown took {:?}",
        started.elapsed()
    );

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for (w, &n) in acked.iter().enumerate() {
        for i in [1, n / 2 + 1, n] {
            let key = format!("drain:{w}:{i}");
            assert_eq!(resp_cmd(&mut c, &["EXISTS", &key]), ":1\r\n", "{key} lost");
        }
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_blocking_pops_never_miss_a_concurrent_push_e2e() {
    // The pusher's command can reach the key's owner shard while the blocked
    // client is between "all keys empty" and "registered as a waiter". That
    // push must still wake it; it used to be lost about half of the time
    // with 4 shards, leaving the client blocked until its timeout.
    let port = 16987;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "4", "--no-pin"], port);
    let cases: [(&str, &[&str], &[&str]); 5] = [
        ("BLPOP", &["BLPOP", "{k}", "1"], &["RPUSH", "{k}", "x"]),
        ("BRPOP", &["BRPOP", "{k}", "1"], &["RPUSH", "{k}", "x"]),
        (
            "BLMPOP",
            &["BLMPOP", "1", "1", "{k}", "LEFT"],
            &["RPUSH", "{k}", "x"],
        ),
        (
            "BLMOVE",
            &["BLMOVE", "{k}", "{k}:dst", "LEFT", "RIGHT", "1"],
            &["RPUSH", "{k}", "x"],
        ),
        (
            "BZPOPMIN",
            &["BZPOPMIN", "{k}", "1"],
            &["ZADD", "{k}", "1", "x"],
        ),
    ];
    for (name, block, push) in cases {
        let mut slow = 0;
        for i in 0..40 {
            let key = format!("bwake:{name}:{i}");
            let sub = |args: &[&str]| -> Vec<String> {
                args.iter().map(|a| a.replace("{k}", &key)).collect()
            };
            let block = sub(block);
            let push = sub(push);
            let mut rd = TcpStream::connect(("127.0.0.1", port)).unwrap();
            let mut w = TcpStream::connect(("127.0.0.1", port)).unwrap();
            rd.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            w.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let encode = |args: &[String]| {
                let mut out = format!("*{}\r\n", args.len());
                for a in args {
                    out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
                }
                out
            };
            let started = std::time::Instant::now();
            rd.write_all(encode(&block).as_bytes()).unwrap();
            w.write_all(encode(&push).as_bytes()).unwrap();
            let mut buf = [0u8; 256];
            let n = w.read(&mut buf).unwrap();
            assert!(
                buf[..n].starts_with(b":") || buf[..n].starts_with(b"+"),
                "{name}"
            );
            let n = rd.read(&mut buf).unwrap();
            let reply = String::from_utf8_lossy(&buf[..n]).to_string();
            assert!(
                !reply.starts_with("*-1") && !reply.starts_with("$-1") && !reply.starts_with("_"),
                "{name} missed the push and timed out: {reply:?}"
            );
            if started.elapsed() > Duration::from_millis(500) {
                slow += 1;
            }
        }
        assert_eq!(slow, 0, "{name}: wakeups waited for the timeout");
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn test_command_introspection_matches_valkey_e2e() {
    let port = 17103u16;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Byte-for-byte what Valkey replies.
    let get_info = "*10\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
*3\r\n+@read\r\n+@string\r\n+@fast\r\n*0\r\n*1\r\n*6\r\n$5\r\nflags\r\n*2\r\n+RO\r\n+access\r\n\
$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n\
$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n\
$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n";
    assert_eq!(
        resp_cmd_full(&mut c, &["COMMAND", "INFO", "get", "no-such-command"]),
        format!("*2\r\n{get_info}$-1\r\n")
    );
    // Movable-key commands carry the flag cluster clients key off.
    let zu = resp_cmd_full(&mut c, &["COMMAND", "INFO", "zunionstore"]);
    assert!(zu.contains("+movablekeys\r\n"), "{zu}");
    assert!(zu.contains("$6\r\nkeynum\r\n"), "{zu}");

    // COMMAND lists every command COMMAND COUNT counts.
    let count = resp_cmd(&mut c, &["COMMAND", "COUNT"]);
    let count: usize = count.trim().trim_start_matches(':').parse().unwrap();
    assert!(count > 200, "COMMAND COUNT {count}");
    let all = resp_cmd_full(&mut c, &["COMMAND"]);
    assert_eq!(resp2_top_level_len(&all), count);
    assert!(all.contains(get_info));

    // Containers list only the subcommands that exist.
    let client = resp_cmd_full(&mut c, &["COMMAND", "INFO", "client"]);
    assert!(client.contains("$11\r\nclient|list\r\n"), "{client}");
    let list = resp_cmd_full(&mut c, &["COMMAND", "LIST"]);
    assert!(list.contains("$11\r\nclient|list\r\n"));
    assert!(!list.contains("sentinel"));
    // Experimental families are off by default and never listed.
    assert!(!list.to_ascii_lowercase().contains("json."));
    assert_eq!(
        resp_cmd_full(&mut c, &["COMMAND", "INFO", "json.set"]),
        "*1\r\n$-1\r\n"
    );
    let pat = resp_cmd_full(
        &mut c,
        &["COMMAND", "LIST", "FILTERBY", "PATTERN", "ZUNION*"],
    );
    assert_eq!(pat, "*2\r\n$6\r\nzunion\r\n$11\r\nzunionstore\r\n");

    // DOCS: a map, flattened for RESP2; unknown names are skipped.
    let docs = resp_cmd_full(&mut c, &["COMMAND", "DOCS", "get", "no-such-command"]);
    assert!(
        docs.starts_with("*2\r\n$3\r\nget\r\n*10\r\n$7\r\nsummary\r\n$34\r\nReturns the string value of a key.\r\n"),
        "{docs}"
    );
    assert!(resp_cmd(&mut c, &["COMMAND", "FOO"]).starts_with("-ERR unknown subcommand 'FOO'"));
    let scripting = resp_cmd_full(
        &mut c,
        &["COMMAND", "LIST", "FILTERBY", "ACLCAT", "scripting"],
    );
    assert!(scripting.contains("$11\r\nscript|kill\r\n"), "{scripting}");
    assert!(!scripting.contains("$3\r\nset\r\n"), "{scripting}");
    assert_eq!(
        resp_cmd(&mut c, &["SCRIPT", "KILL"]),
        "-NOTBUSY No scripts in execution right now.\r\n"
    );

    // RESP3 uses sets and maps.
    resp_cmd(&mut c, &["HELLO", "3"]);
    let info3 = resp_cmd_full(&mut c, &["COMMAND", "INFO", "get"]);
    assert!(
        info3.starts_with("*1\r\n*10\r\n$3\r\nget\r\n:2\r\n~2\r\n+readonly\r\n"),
        "{info3}"
    );
    assert!(
        info3.contains("%3\r\n$5\r\nflags\r\n~2\r\n+RO\r\n"),
        "{info3}"
    );
    let docs3 = resp_cmd_full(&mut c, &["COMMAND", "DOCS", "get"]);
    assert!(docs3.starts_with("%1\r\n$3\r\nget\r\n%5\r\n"), "{docs3}");

    let _ = child.kill();
    let _ = child.wait();
}

/// Untrusted counts in command arguments must produce an error reply, not
/// hang the connection's worker thread (FT.CREATE PREFIX used to loop 2^63
/// times), panic or abort on a huge preallocation.
#[test]
fn test_hostile_argument_counts_get_error_replies_e2e() {
    let port = 17083;
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--enable-experimental-commands",
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
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    let huge = "9223372036854775807";
    let max = "18446744073709551615";
    for cmd in [
        &["FT.CREATE", "x", "0x10", "PREFIX", huge, "ARGS", "inf", "*"][..],
        &["FT.SEARCH", "i", "*", "RETURN", huge, "f"],
        &["FT.AGGREGATE", "i", "*", "LOAD", huge, "@f"],
        &["HSETEX", "k", "FIELDS", huge, "f", "v"],
        &["EVAL", "return 1", max, "k"],
        &["MSETEX", "2147483647", "k", "v"],
    ] {
        let reply = resp_cmd(&mut c, cmd);
        assert!(reply.starts_with("-ERR"), "{cmd:?}: {reply}");
        assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n", "after {cmd:?}");
    }

    // A malformed memcached data block is answered and skipped instead of
    // being re-parsed forever.
    let reply = send_and_read(&mut c, b"set k 0 0 1\r\nab\r\n");
    assert!(reply.contains("bad data chunk"), "{reply}");
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    drop(c);

    shutdown_and_wait(port, &mut child.0);
}

/// CONFIG REWRITE updates the `--config` file (even when given relative to a
/// working directory the server no longer resolves against), refuses without
/// one instead of writing `rudis.conf` into the working directory, and a bad
/// config file is a clean startup error.
#[test]
fn test_config_rewrite_targets_the_startup_config_file_e2e() {
    let port = 17095;
    let root = std::env::temp_dir().join(format!("rudis-e2e-cfgfile-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let conf_dir = root.join("conf");
    let cwd = root.join("cwd");
    std::fs::create_dir_all(&conf_dir).unwrap();
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::write(conf_dir.join("server.conf"), "# mine\nmaxclients 1000\n").unwrap();
    let spawn = |args: &[&str], stderr: std::process::Stdio| {
        rudis_bin()
            .args(args)
            .args(DEBUG_ON)
            .current_dir(&cwd)
            .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
            .stdout(std::process::Stdio::null())
            .stderr(stderr)
            .spawn()
            .expect("spawn rudis")
    };
    let wait_listening = |child: &mut std::process::Child| {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(c) = TcpStream::connect(("127.0.0.1", port)) {
                c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                return c;
            }
            if let Some(s) = child.try_wait().unwrap() {
                panic!("rudis exited early: {s}");
            }
            assert!(std::time::Instant::now() < deadline, "rudis did not start");
            thread::sleep(Duration::from_millis(50));
        }
    };
    let stop = |c: &mut TcpStream, child: &mut std::process::Child| {
        let _ = c.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n");
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() > deadline {
                let _ = child.kill();
                panic!("rudis did not shut down");
            }
            thread::sleep(Duration::from_millis(50));
        }
    };
    let port_s = port.to_string();

    // 1. Started with a config file given relative to the working directory.
    let mut child = spawn(
        &["--config", "../conf/server.conf", "--port", &port_s],
        std::process::Stdio::null(),
    );
    let mut c = wait_listening(&mut child);
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxclients", "2345"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["CONFIG", "REWRITE"]), "+OK\r\n");
    let content = std::fs::read_to_string(conf_dir.join("server.conf")).unwrap();
    assert!(content.contains("# mine"), "{content}");
    assert!(content.contains("maxclients 2345"), "{content}");
    assert!(!cwd.join("rudis.conf").exists());
    stop(&mut c, &mut child);

    // 2. Started without a config file: nothing to rewrite.
    let mut child = spawn(&["--port", &port_s], std::process::Stdio::null());
    let mut c = wait_listening(&mut child);
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "REWRITE"]),
        "-ERR The server is running without a config file\r\n"
    );
    assert!(!cwd.join("rudis.conf").exists());
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    stop(&mut c, &mut child);

    // 3. A missing config file is a startup error with a message, not a panic.
    let out = spawn(
        &["--config", "../conf/missing.conf", "--port", &port_s],
        std::process::Stdio::piped(),
    )
    .wait_with_output()
    .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("Failed to load config file"), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");

    let _ = std::fs::remove_dir_all(&root);
}

/// Client-chosen sizes that parse fine must not reach unbounded loops or
/// allocations at execution: KNN k = 2^63 aborted the process on a heap
/// preallocation, VADD REDUCE built a 2^32 x dim projection, JSON.SET
/// `$.a[2^63]` padded the array forever, BF/CF/CMS reserves aborted on their
/// bit arrays, CRDT.INCRBY and HSCAN COUNT overflowed. Each must answer, with
/// an error where the size is refused, and the server keeps serving.
#[test]
fn test_hostile_sizes_get_bounded_replies_e2e() {
    let port = 17087;
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--enable-experimental-commands",
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
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // "AAAA" is a valid little-endian f32, so the query blob stays ASCII.
    for cmd in [
        &[
            "FT.CREATE",
            "hzi",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "hz:",
            "SCHEMA",
            "n",
            "NUMERIC",
            "v",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "L2",
        ][..],
        &["HSET", "hz:1", "n", "1", "v", "AAAAAAAA"],
        &["HSET", "hz:2", "n", "2", "v", "BBBBBBBB"],
        &["VADD", "vs", "VALUES", "2", "1", "2", "a"],
        &["HSET", "h", "f", "v"],
        &["SADD", "s", "a"],
        &["JSON.SET", "j", "$", r#"{"a":[1,2,3]}"#],
    ] {
        let reply = resp_cmd(&mut c, cmd);
        assert!(!reply.starts_with('-'), "{cmd:?}: {reply}");
    }

    let huge = "9223372036854775807";
    let knn = format!("*=>[KNN {huge} @v $B]");
    let json_idx = format!("$.a[{huge}]");
    for cmd in [
        &[
            "FT.CREATE",
            "hzx",
            "SCHEMA",
            "v",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            huge,
            "DISTANCE_METRIC",
            "L2",
        ][..],
        &[
            "FT.CREATE",
            "hzx",
            "SCHEMA",
            "v",
            "VECTOR",
            "HNSW",
            "8",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "L2",
            "M",
            huge,
        ],
        &[
            "VADD",
            "v2",
            "REDUCE",
            "4294967296",
            "VALUES",
            "2",
            "1",
            "1",
            "e",
        ],
        &["VADD", "v2", "VALUES", "2", "1", "1", "e", "M", huge],
        &["VSIM", "vs", "ELE", "a", "EF", huge],
        &["VRANDMEMBER", "vs", "-4294967296"],
        &["SRANDMEMBER", "s", "-9223372036854775807"],
        &["BF.RESERVE", "b", "0.01", huge],
        &["CF.RESERVE", "c", huge],
        &["CMS.INITBYDIM", "m", huge, huge],
        &["JSON.SET", "j", &json_idx, "1"],
    ] {
        let reply = resp_cmd(&mut c, cmd);
        assert!(reply.starts_with("-ERR"), "{cmd:?}: {reply}");
        assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n", "after {cmd:?}");
    }
    assert_eq!(
        resp_cmd(&mut c, &["CRDT.INCRBY", "cnt", huge]),
        format!(":{huge}\r\n")
    );
    let reply = resp_cmd(&mut c, &["CRDT.INCRBY", "cnt", "1"]);
    assert!(
        reply.starts_with("-ERR increment or decrement would overflow"),
        "{reply}"
    );

    // Huge k / COUNT are clamped to what exists.
    for (cmd, want) in [
        (
            &["FT.SEARCH", "hzi", &knn, "PARAMS", "2", "B", "AAAAAAAA"][..],
            "*5\r\n:2\r\n",
        ),
        (&["FT.SEARCH", "hzi", "@n:[nan 0]"], "*1\r\n:0\r\n"),
        (&["VSIM", "vs", "ELE", "a", "COUNT", huge], "*1\r\n"),
        (
            &["HSCAN", "h", "0", "COUNT", "18446744073709551615"],
            "*2\r\n",
        ),
        (
            &["BF.RESERVE", "bnan", "nan", "100"],
            "-ERR (0 < error rate range < 1)",
        ),
        (
            &["BF.RESERVE", "bnan", "0", "100"],
            "-ERR (0 < error rate range < 1)",
        ),
        (
            &["BF.RESERVE", "bnan", "1", "100"],
            "-ERR (0 < error rate range < 1)",
        ),
        (&["EXISTS", "bnan"], ":0"),
    ] {
        let reply = resp_cmd(&mut c, cmd);
        assert!(reply.starts_with(want), "{cmd:?}: {reply}");
        assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n", "after {cmd:?}");
    }
    drop(c);

    shutdown_and_wait(port, &mut child.0);
}

#[test]
fn test_client_reply_off_skip_inside_one_pipeline_e2e() {
    // CLIENT REPLY must apply to the commands after it even when they arrive
    // in the same read, i.e. in a batch that would otherwise be squashed.
    let port = 17092;
    start_test_server(port, 2);
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    stream
        .write_all(
            b"CLIENT REPLY OFF\r\nSET reply:k 1\r\nINCR reply:k\r\nCLIENT REPLY SKIP\r\n\
              INCR reply:k\r\nCLIENT REPLY ON\r\nCLIENT REPLY SKIP\r\nINCR reply:k\r\n\
              INCR reply:k\r\n",
        )
        .unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 1024];
    while !got.ends_with(b":5\r\n") {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
        }
    }
    assert_eq!(String::from_utf8_lossy(&got), "+OK\r\n:5\r\n");
}

/// Runs of empty frames (blank lines, `*0`, `*-1`) used to recurse once per
/// frame in the parser; a client sending megabytes of them overflowed the
/// shard thread's stack and the whole server died.
#[test]
fn test_empty_frame_flood_does_not_crash_e2e() {
    let port: u16 = 17089;
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
    for frame in [&b"\n"[..], b"*0\r\n", b"*-1\r\n"] {
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let flood = frame.repeat((8 << 20) / frame.len());
        c.write_all(&flood).unwrap();
        assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    }
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child.0);
}

/// `INFO keyspace` used to be an empty section; clients and exporters read
/// `db0:keys=…,expires=…` from it. Counts must be server-wide across shards.
#[test]
fn test_info_keyspace_counts_all_shards_e2e() {
    let port = 17433;
    start_test_server(port, 4);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["FLUSHALL"]), "+OK\r\n");
    let empty = resp_cmd(&mut c, &["INFO", "keyspace"]);
    assert!(empty.contains("# Keyspace"), "{empty}");
    assert!(!empty.contains("db0:"), "{empty}");
    for i in 0..20 {
        let k = format!("ks{i}");
        if i < 7 {
            assert_eq!(resp_cmd(&mut c, &["SET", &k, "v", "EX", "100"]), "+OK\r\n");
        } else {
            assert_eq!(resp_cmd(&mut c, &["SET", &k, "v"]), "+OK\r\n");
        }
    }
    let info = resp_cmd(&mut c, &["INFO", "keyspace"]);
    assert!(
        info.contains("db0:keys=20,expires=7,avg_ttl=0"),
        "INFO keyspace: {info}"
    );
    // Default INFO includes the section too.
    let mut all = resp_cmd(&mut c, &["INFO"]);
    while !all.contains("db0:keys=") && all.len() < 1 << 20 {
        let more = send_and_read(&mut c, b"");
        if more.is_empty() {
            break;
        }
        all.push_str(&more);
    }
    assert!(all.contains("db0:keys=20,expires=7"), "INFO: {all}");
}

/// `CLIENT NO-TOUCH` is per client: it used to be a process-wide flag, so one
/// client turning it on stopped every client's reads from updating LRU data.
/// Keys span all shards, so reads also go through other shards' batches.
#[test]
fn test_client_no_touch_is_per_client_e2e() {
    let port = 17936u16;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--bind",
            "127.0.0.1",
        ],
        port,
    );
    let mut a = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut b = TcpStream::connect(("127.0.0.1", port)).unwrap();
    a.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    b.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(resp_cmd(&mut a, &["CLIENT", "NO-TOUCH", "ON"]), "+OK\r\n");
    let keys: Vec<String> = (0..16).map(|i| format!("nt:{i}")).collect();
    for k in &keys {
        assert_eq!(resp_cmd(&mut b, &["SET", k, "v"]), "+OK\r\n");
    }
    thread::sleep(Duration::from_millis(2100));
    for (i, k) in keys.iter().enumerate() {
        // Even keys read by the NO-TOUCH client, odd ones by the other.
        let c = if i % 2 == 0 { &mut a } else { &mut b };
        assert_eq!(resp_cmd(c, &["GET", k]), "$1\r\nv\r\n");
    }
    for (i, k) in keys.iter().enumerate() {
        let idle: u64 = resp_cmd(&mut b, &["OBJECT", "IDLETIME", k])
            .trim()
            .trim_start_matches(':')
            .parse()
            .unwrap();
        if i % 2 == 0 {
            assert!(
                idle >= 2,
                "{k}: NO-TOUCH read touched the key (idle {idle})"
            );
        } else {
            assert!(
                idle <= 1,
                "{k}: normal read did not touch the key (idle {idle})"
            );
        }
    }
    // RESET turns NO-TOUCH off.
    assert_eq!(resp_cmd(&mut a, &["RESET"]), "+RESET\r\n");
    assert_eq!(resp_cmd(&mut a, &["GET", &keys[0]]), "$1\r\nv\r\n");
    assert_eq!(
        resp_cmd(&mut b, &["OBJECT", "IDLETIME", &keys[0]]),
        ":0\r\n"
    );
    drop(a);
    drop(b);
    shutdown_and_wait(port, &mut child);
}

/// A queued command that panics inside EXEC isolates (drops) the connection
/// mid-transaction. EXEC must still undo its side effects: the pause of
/// blocking-client notifications (else blocked clients on the port are never
/// woken again) and its cross-shard locks (else every later multi-shard
/// transaction on those shards waits forever).
#[test]
fn test_exec_dropped_by_panic_releases_pause_and_locks_e2e() {
    let port = 17937u16;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--bind",
            "127.0.0.1",
        ],
        port,
    );
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c
    };
    // Eight keys span the shards, so EXEC takes VLL locks.
    let keys: Vec<String> = (0..8).map(|i| format!("vll:{i}")).collect();

    let mut blocked = connect();
    blocked
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$9\r\nexec:list\r\n$1\r\n0\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(200));

    let mut a = connect();
    assert_eq!(resp_cmd(&mut a, &["MULTI"]), "+OK\r\n");
    for k in &keys {
        assert_eq!(resp_cmd(&mut a, &["SET", k, "1"]), "+QUEUED\r\n");
    }
    assert_eq!(resp_cmd(&mut a, &["DEBUG", "PANIC"]), "+QUEUED\r\n");
    a.write_all(b"*1\r\n$4\r\nEXEC\r\n").unwrap();
    // The connection is isolated: closed (or reset) without a full reply.
    let mut buf = [0u8; 4096];
    let mut got = Vec::new();
    loop {
        match a.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
        }
    }
    drop(a);

    let mut c = connect();
    // The blocked client is woken.
    assert_eq!(resp_cmd(&mut c, &["LPUSH", "exec:list", "x"]), ":1\r\n");
    let mut reply = Vec::new();
    while !reply.ends_with(b"x\r\n") {
        let n = blocked.read(&mut buf).expect("blocked client never woken");
        assert!(n > 0, "blocked client connection closed");
        reply.extend_from_slice(&buf[..n]);
    }
    // A later multi-shard transaction on the same shards gets the locks.
    assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
    for k in &keys {
        assert_eq!(resp_cmd(&mut c, &["SET", k, "2"]), "+QUEUED\r\n");
    }
    let exec = resp_cmd(&mut c, &["EXEC"]);
    assert!(exec.starts_with("*8\r\n"), "EXEC reply: {exec:?}");
    drop(blocked);
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_redis_log_file_format_and_loglevel_e2e() {
    let port = 18510u16;
    let dir = std::env::temp_dir().join(format!("rudis-srv-{}-{}", std::process::id(), port));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let log = dir.join("rudis.log");
    let conf = dir.join("rudis.conf");
    std::fs::write(
        &conf,
        format!(
            "dir {}\nsave \"\"\nloglevel verbose\nlogfile \"{}\"\n",
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
    let mut child = KillRudisOnDrop(spawn_rudis_listening(&args, port));
    let pid = child.0.id();

    let text = wait_for_log(&log, "Ready to accept connections tcp");
    for line in text.lines() {
        assert!(
            is_redis_log_line(line, pid),
            "not a Redis log line: {line:?}"
        );
    }
    assert!(
        text.contains(" * Ready to accept connections tcp\n"),
        "{text}"
    );
    // Verbose lines are written at `loglevel verbose`, and --no-pin shows.
    assert!(text.contains(" - [Shard 0/2] Worker started"), "{text}");
    assert!(
        text.contains("worker threads (not pinned to CPU cores)"),
        "{text}"
    );
    assert!(text.contains(" * Server initialized\n"), "{text}");
    assert!(text.contains(":M "), "{text}");

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "loglevel"]),
        "*2\r\n$8\r\nloglevel\r\n$7\r\nverbose\r\n"
    );
    let path = log.display().to_string();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "logfile"]),
        format!("*2\r\n$7\r\nlogfile\r\n${}\r\n{}\r\n", path.len(), path)
    );
    assert!(
        resp_cmd(&mut c, &["CONFIG", "SET", "logfile", "/tmp/x.log"])
            .contains("can't set immutable config")
    );
    assert!(
        resp_cmd(&mut c, &["CONFIG", "SET", "loglevel", "loud"]).contains(
            "argument(s) must be one of the following: debug, verbose, notice, warning, nothing"
        )
    );

    // Filtered by the runtime level: SAVE's notice is dropped at warning.
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "loglevel", "warning"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "loglevel", "notice"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "loglevel"]),
        "*2\r\n$8\r\nloglevel\r\n$6\r\nnotice\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["BGSAVE"]),
        "+Background saving started\r\n"
    );
    let text = wait_for_log(&log, "Background saving terminated with success");
    assert_eq!(
        text.matches("DB saved on disk").count(),
        1,
        "the SAVE at loglevel warning must not be logged:\n{text}"
    );
    assert!(text.contains(" * Background saving started\n"), "{text}");

    // logrotate: the file is renamed away; the next line recreates it.
    let rotated = dir.join("rudis.log.1");
    std::fs::rename(&log, &rotated).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    let text = wait_for_log(&log, "DB saved on disk");
    assert!(
        is_redis_log_line(text.lines().next().unwrap(), pid),
        "{text}"
    );

    // Valkey's logfmt format.
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "log-format", "logfmt"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    let text = wait_for_log(&log, "message=\"DB saved on disk\"");
    assert!(
        text.contains(&format!("pid={pid} role=primary timestamp=\"")),
        "{text}"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "log-format", "legacy"]),
        "+OK\r\n"
    );

    drop(c);
    shutdown_and_wait(port, &mut child.0);
    let text = std::fs::read_to_string(&log).unwrap();
    assert!(text.contains(" # User requested shutdown...\n"), "{text}");
    assert!(
        text.contains(" * Redis is now ready to exit, bye bye...\n"),
        "{text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_debug_digest_compares_logical_datasets_e2e() {
    let (p1, p2) = (18400, 18401);
    start_test_server(p1, 2);
    start_test_server(p2, 3);
    let mut a = TcpStream::connect(("127.0.0.1", p1)).unwrap();
    let mut b = TcpStream::connect(("127.0.0.1", p2)).unwrap();

    // Empty dataset: 40 zeros, as Redis.
    assert_eq!(debug_digest(&mut a), "0".repeat(40));

    load_digest_dataset(&mut a, false);
    load_digest_dataset(&mut b, true);
    let da = debug_digest(&mut a);
    assert_ne!(da, "0".repeat(40));
    assert_eq!(da, debug_digest(&mut b), "same data, other order/shards");

    // DIGEST-VALUE: per key, zeros for a missing key, independent of names.
    let va = resp_cmd_all(
        &mut a,
        &["DEBUG", "DIGEST-VALUE", "hash", "bigzset", "missing"],
    );
    let vb = resp_cmd_all(
        &mut b,
        &["DEBUG", "DIGEST-VALUE", "hash", "bigzset", "missing"],
    );
    assert_eq!(va, vb);
    assert!(va.starts_with("*3\r\n+"), "{va}");
    assert!(va.ends_with(&format!("+{}\r\n", "0".repeat(40))), "{va}");
    let one = resp_cmd_all(&mut a, &["DEBUG", "DIGEST-VALUE", "num"]);
    resp_cmd_all(&mut a, &["SET", "num-copy", "12345"]);
    assert_eq!(
        one,
        resp_cmd_all(&mut a, &["DEBUG", "DIGEST-VALUE", "num-copy"])
    );
    resp_cmd_all(&mut a, &["DEL", "num-copy"]);
    assert_eq!(debug_digest(&mut a), da);

    // TTL presence counts, its value does not.
    resp_cmd_all(&mut b, &["EXPIRE", "ttl", "5000"]);
    assert_eq!(debug_digest(&mut b), da);
    resp_cmd_all(&mut b, &["PERSIST", "ttl"]);
    assert_ne!(debug_digest(&mut b), da);
    resp_cmd_all(&mut b, &["EXPIRE", "ttl", "100"]);
    assert_eq!(debug_digest(&mut b), da);

    // Any change in content shows up.
    resp_cmd_all(&mut b, &["HSET", "bighash", "m7", "changed"]);
    assert_ne!(debug_digest(&mut b), da);
    resp_cmd_all(&mut b, &["HSET", "bighash", "m7", "v-m7"]);
    assert_eq!(debug_digest(&mut b), da);
    resp_cmd_all(&mut b, &["LSET", "list", "0", "z"]);
    assert_ne!(debug_digest(&mut b), da);

    // FLUSHALL brings back the empty digest.
    resp_cmd_all(&mut b, &["FLUSHALL"]);
    assert_eq!(debug_digest(&mut b), "0".repeat(40));
}

#[test]
fn test_debug_subcommands_help_and_errors_e2e() {
    let port = 18402;
    start_test_server(port, 2);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();

    let r = resp_cmd_all(&mut c, &["DEBUG", "no-such-sub"]);
    assert_eq!(
        r,
        "-ERR unknown subcommand 'no-such-sub'. Try DEBUG HELP.\r\n"
    );
    // Internal shard messages are not client subcommands.
    let r = resp_cmd_all(&mut c, &["DEBUG", "__shard-digest"]);
    assert!(r.starts_with("-ERR unknown subcommand"), "{r}");
    let help = resp_cmd_all(&mut c, &["DEBUG", "HELP"]);
    assert!(
        help.starts_with('*') && help.contains("+DIGEST-VALUE"),
        "{help}"
    );
    // Redis internals Rudis does not have are accepted as no-ops.
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "QUICKLIST-PACKED-THRESHOLD", "100"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "set-active-expire", "1"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "PROTOCOL", "true"]),
        ":1\r\n"
    );
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "ERROR", "ERR custom"]),
        "-ERR custom\r\n"
    );
    assert_eq!(resp_cmd_all(&mut c, &["DEBUG", "LOG", "hello"]), "+OK\r\n");
    assert!(resp_cmd_all(&mut c, &["DEBUG", "STRINGMATCH-LEN"]).starts_with("+Apparently"));
    assert!(
        resp_cmd_all(&mut c, &["DEBUG", "RELOAD", "bogus"]).starts_with("-ERR DEBUG RELOAD only")
    );

    // POPULATE: missing keys only, Redis' names and values, on every shard.
    resp_cmd_all(&mut c, &["SET", "key:3", "keep"]);
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "POPULATE", "100"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd_all(&mut c, &["DBSIZE"]), ":100\r\n");
    assert_eq!(
        resp_cmd_all(&mut c, &["GET", "key:57"]),
        "$8\r\nvalue:57\r\n"
    );
    assert_eq!(resp_cmd_all(&mut c, &["GET", "key:3"]), "$4\r\nkeep\r\n");
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "POPULATE", "10", "p", "3"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd_all(&mut c, &["GET", "p:9"]), "$3\r\nval\r\n");
    assert!(resp_cmd_all(&mut c, &["DEBUG", "POPULATE", "-1"]).starts_with("-ERR"));

    // SLEEP stalls the whole server, not just this connection's thread.
    let mut sleeper = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut others: Vec<TcpStream> = (0..4)
        .map(|_| TcpStream::connect(("127.0.0.1", port)).unwrap())
        .collect();
    for o in &mut others {
        assert_eq!(resp_cmd_all(o, &["PING"]), "+PONG\r\n");
    }
    sleeper
        .write_all(b"*3\r\n$5\r\nDEBUG\r\n$5\r\nSLEEP\r\n$3\r\n0.6\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    let start = std::time::Instant::now();
    for o in &mut others {
        assert_eq!(resp_cmd_all(o, &["PING"]), "+PONG\r\n");
    }
    assert!(
        start.elapsed() >= Duration::from_millis(300),
        "PINGs during DEBUG SLEEP answered after {:?}",
        start.elapsed()
    );
    assert_eq!(read_full_reply(&mut sleeper), "+OK\r\n");
}

/// `cross-shard-strict yes` refuses multi-key commands and transactions
/// whose keys live on different shards with Redis Cluster's CROSSSLOT error,
/// alone or pipelined; keys sharing a hash tag still work, and turning it
/// off lets cross-shard commands run again.
#[test]
fn test_cross_shard_strict_refuses_commands_spanning_shards_e2e() {
    let port = 18422u16;
    start_test_server(port, 4);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let (a, b) = (0..)
        .map(|i| (format!("cs:a{i}"), format!("cs:b{i}")))
        .find(|(a, b)| {
            rudis::router::target_shard(a.as_bytes(), 4)
                != rudis::router::target_shard(b.as_bytes(), 4)
        })
        .unwrap();
    const CROSSSLOT: &str = "-CROSSSLOT Keys in request don't hash to the same slot\r\n";
    assert_eq!(
        resp_cmd_all(&mut c, &["CONFIG", "GET", "cross-shard-strict"]),
        "*2\r\n$18\r\ncross-shard-strict\r\n$2\r\nno\r\n"
    );
    assert_eq!(resp_cmd_all(&mut c, &["MSET", &a, "1", &b, "2"]), "+OK\r\n");
    assert_eq!(
        resp_cmd_all(&mut c, &["CONFIG", "SET", "cross-shard-strict", "yes"]),
        "+OK\r\n"
    );
    for args in [
        vec!["MSET", &a, "1", &b, "2"],
        vec!["MGET", &a, &b],
        vec!["DEL", &a, &b],
        vec!["EXISTS", &a, &b],
        vec!["RENAME", &a, &b],
        vec!["LMOVE", &a, &b, "LEFT", "RIGHT"],
        vec!["SUNIONSTORE", &a, &b],
    ] {
        assert_eq!(resp_cmd_all(&mut c, &args), CROSSSLOT, "{args:?}");
    }
    // Pipelined (the batched paths).
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let cmds = vec![
        s(&["MSET", &a, "1", &b, "2"]),
        s(&["MGET", &a, &b]),
        s(&["DEL", &a, &b]),
        s(&["GET", &a]),
    ];
    let replies = String::from_utf8(pipeline_all(&mut c, &cmds, "strict-done")).unwrap();
    assert_eq!(replies.matches("-CROSSSLOT").count(), 3, "{replies}");
    assert!(replies.contains("$1\r\n1\r\n"), "{replies}");
    // A transaction over two shards is refused as a whole.
    assert_eq!(resp_cmd_all(&mut c, &["MULTI"]), "+OK\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["SET", &a, "x"]), "+QUEUED\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["SET", &b, "y"]), "+QUEUED\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["EXEC"]), CROSSSLOT);
    assert_eq!(resp_cmd_all(&mut c, &["GET", &a]), "$1\r\n1\r\n");
    // Keys sharing a hash tag share a shard.
    let (ta, tb) = (format!("{{{a}}}:1"), format!("{{{a}}}:2"));
    assert_eq!(
        resp_cmd_all(&mut c, &["MSET", &ta, "1", &tb, "2"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd_all(&mut c, &["CONFIG", "SET", "cross-shard-strict", "no"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd_all(&mut c, &["DEL", &a, &b]), ":2\r\n");
}

/// client-query-buffer-limit is enforced (it used to be accepted and
/// reported but ignored, so one client could grow its query buffer without
/// bound): a huge partial bulk, or one complete command larger than the
/// limit, closes that client and is counted, and the server keeps serving
/// everyone else. Runs the real binary: the limit is process-wide.
#[test]
fn test_client_query_buffer_limit_closes_client_e2e() {
    let port = 16871;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);

    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let below_min = resp_cmd(
        &mut admin,
        &["CONFIG", "SET", "client-query-buffer-limit", "1000"],
    );
    assert!(below_min.starts_with("-ERR"), "{below_min}");
    assert_eq!(
        resp_cmd(
            &mut admin,
            &["CONFIG", "SET", "client-query-buffer-limit", "1mb"]
        ),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut admin, &["CONFIG", "RESETSTAT"]), "+OK\r\n");

    // A bulk declared far beyond the limit, then more data than the limit
    // without ever completing it.
    let mut bad = TcpStream::connect(("127.0.0.1", port)).unwrap();
    bad.write_all(b"*1\r\n$100000000\r\n").unwrap();
    let chunk = vec![b'a'; 64 * 1024];
    for _ in 0..48 {
        // 3MB; once the server closes, writes fail.
        if bad.write_all(&chunk).is_err() {
            break;
        }
    }
    assert!(
        wait_for_server_close(&mut bad),
        "oversized partial command was not closed"
    );
    let info = wait_for_info_field(&mut admin, "client_query_buffer_limit_disconnections:1");
    assert!(
        info.contains("client_query_buffer_limit_disconnections:1"),
        "{info}"
    );

    // One complete command over the limit (Redis's info.tcl case).
    let mut big = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let value = "x".repeat(1024 * 1024);
    let cmd = format!(
        "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n${}\r\n{}\r\n",
        value.len(),
        value
    );
    let _ = big.write_all(cmd.as_bytes());
    assert!(
        wait_for_server_close(&mut big),
        "oversized command was not closed"
    );
    let info = wait_for_info_field(&mut admin, "client_query_buffer_limit_disconnections:2");
    assert!(
        info.contains("client_query_buffer_limit_disconnections:2"),
        "{info}"
    );
    assert_eq!(resp_cmd(&mut admin, &["EXISTS", "key"]), ":0\r\n");

    // The server and other clients are unaffected; commands under the
    // limit still work.
    let mut other = TcpStream::connect(("127.0.0.1", port)).unwrap();
    other
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    assert_eq!(resp_cmd(&mut other, &["PING"]), "+PONG\r\n");
    let ok_value = "y".repeat(512 * 1024);
    let ok_cmd = format!(
        "*3\r\n$3\r\nSET\r\n$2\r\nok\r\n${}\r\n{}\r\n",
        ok_value.len(),
        ok_value
    );
    assert_eq!(send_and_read(&mut other, ok_cmd.as_bytes()), "+OK\r\n");
    assert_eq!(resp_cmd(&mut admin, &["STRLEN", "ok"]), ":524288\r\n");

    shutdown_and_wait(port, &mut child);
}

/// A pipeline far larger in total than client-query-buffer-limit, made of
/// small commands the server drains as they arrive, is not mistaken for an
/// oversized query buffer.
#[test]
fn test_client_query_buffer_limit_allows_large_drained_pipeline_e2e() {
    let port = 16872;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    assert_eq!(
        resp_cmd(
            &mut c,
            &["CONFIG", "SET", "client-query-buffer-limit", "1mb"]
        ),
        "+OK\r\n"
    );

    const N: usize = 50_000;
    let value = "v".repeat(100);
    let mut pipeline = Vec::new();
    for i in 0..N {
        let key = format!("pk:{i}");
        pipeline.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            )
            .as_bytes(),
        );
    }
    assert!(pipeline.len() > 4 * 1024 * 1024);
    let mut writer = c.try_clone().unwrap();
    let w = thread::spawn(move || writer.write_all(&pipeline).unwrap());

    let expected = N * b"+OK\r\n".len();
    let mut got = Vec::with_capacity(expected);
    let mut buf = vec![0u8; 64 * 1024];
    while got.len() < expected {
        let n = c.read(&mut buf).expect("pipeline replies");
        assert!(n > 0, "server closed a drained pipeline");
        got.extend_from_slice(&buf[..n]);
    }
    w.join().unwrap();
    assert_eq!(got.len(), expected);
    assert!(got.chunks(5).all(|r| r == b"+OK\r\n"));
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), format!(":{N}\r\n"));
    let info = resp_cmd(&mut c, &["INFO", "stats"]);
    assert!(
        info.contains("client_query_buffer_limit_disconnections:0"),
        "{info}"
    );

    shutdown_and_wait(port, &mut child);
}

/// With only 64 KB of locked memory, io_uring probes fine but a full shard
/// ring can't be set up (the same ENOMEM as running out of the limit). Shards
/// used to panic, leaving keys on them unreachable; they now fall back to
/// the epoll driver and keep serving.
#[test]
fn test_shards_fall_back_to_epoll_when_io_uring_cannot_start_e2e() {
    let port = 17961u16;
    let ps = port.to_string();
    let cwd = std::env::temp_dir().join(format!("rudis-memlock0-{}", std::process::id()));
    std::fs::create_dir_all(&cwd).unwrap();
    let mut child = std::process::Command::new("sh")
        .args([
            "-c",
            "ulimit -l 64 && exec \"$0\" \"$@\"",
            env!("CARGO_BIN_EXE_rudis"),
            "--port",
            &ps,
            "--threads",
            "4",
            "--no-pin",
        ])
        .current_dir(&cwd)
        .env("MONOIO_FORCE_LEGACY_DRIVER", "0")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rudis");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut c = loop {
        if let Ok(c) = TcpStream::connect(("127.0.0.1", port)) {
            break c;
        }
        if let Some(s) = child.try_wait().unwrap() {
            panic!("rudis exited early: {s}");
        }
        assert!(std::time::Instant::now() < deadline, "rudis did not start");
        thread::sleep(Duration::from_millis(50));
    };
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Keys on every shard are reachable.
    for i in 0..64 {
        let k = format!("k{i}");
        assert_eq!(resp_cmd(&mut c, &["SET", &k, "v"]), "+OK\r\n");
    }
    for i in 0..64 {
        let k = format!("k{i}");
        assert_eq!(resp_cmd(&mut c, &["GET", &k]), "$1\r\nv\r\n");
    }
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":64\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn test_enable_debug_command_gates_debug_e2e() {
    const NOT_ALLOWED: &str = "-ERR DEBUG command not allowed.";
    let connect = |port: u16| {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c
    };

    // Default: refused, as in Redis.
    let port = 17962u16;
    let mut child = spawn_rudis_debug_setting(port, &[]);
    let mut c = connect(port);
    assert!(
        resp_cmd(&mut c, &["DEBUG", "SLEEP", "0"]).starts_with(NOT_ALLOWED),
        "DEBUG must be refused by default"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "enable-debug-command"]),
        "*2\r\n$20\r\nenable-debug-command\r\n$2\r\nno\r\n"
    );
    assert!(
        resp_cmd(&mut c, &["CONFIG", "SET", "enable-debug-command", "yes"])
            .contains("can't set immutable config"),
    );
    // Queued in MULTI it aborts the transaction.
    assert_eq!(resp_cmd(&mut c, &["MULTI"]), "+OK\r\n");
    assert!(resp_cmd(&mut c, &["DEBUG", "SLEEP", "0"]).starts_with(NOT_ALLOWED));
    assert!(resp_cmd(&mut c, &["EXEC"]).starts_with("-EXECABORT"));
    // Scripts can't get around it.
    assert!(
        resp_cmd(
            &mut c,
            &["EVAL", "return redis.call('DEBUG','SLEEP','0')", "0"]
        )
        .contains("DEBUG command not allowed"),
    );
    // Everything else is unaffected.
    assert_eq!(resp_cmd(&mut c, &["SET", "k", "v"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);

    // `local`: allowed from a loopback client.
    let port = 17963u16;
    let mut child = spawn_rudis_debug_setting(port, &["--enable-debug-command", "local"]);
    let mut c = connect(port);
    assert_eq!(resp_cmd(&mut c, &["DEBUG", "SLEEP", "0"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "enable-debug-command"]),
        "*2\r\n$20\r\nenable-debug-command\r\n$5\r\nlocal\r\n"
    );
    drop(c);
    shutdown_and_wait(port, &mut child);

    // `yes`.
    let port = 17964u16;
    let mut child = spawn_rudis_debug_setting(port, &["--enable-debug-command", "yes"]);
    let mut c = connect(port);
    assert_eq!(resp_cmd(&mut c, &["DEBUG", "SLEEP", "0"]), "+OK\r\n");
    assert!(
        !resp_cmd(
            &mut c,
            &["EVAL", "return redis.call('DEBUG','SLEEP','0')", "0"]
        )
        .contains("not allowed"),
    );
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_allkeys_lfu_keeps_frequently_used_keys_e2e() {
    let port = 17965u16;
    let ps = port.to_string();
    // One shard: this checks LFU ranking. With several shards, eviction
    // balance between them (a separate concern) decides which shard's keys
    // go first, whatever their frequency.
    let mut child = spawn_rudis_listening(&["--port", &ps, "--threads", "1", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    // Under LRU, OBJECT FREQ isn't tracked (Redis' error).
    assert_eq!(resp_cmd(&mut c, &["SET", "probe", "v"]), "+OK\r\n");
    assert!(
        resp_cmd(&mut c, &["OBJECT", "FREQ", "probe"])
            .starts_with("-ERR An LFU maxmemory policy is not selected")
    );

    assert_eq!(
        resp_cmd(
            &mut c,
            &["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"]
        ),
        "+OK\r\n"
    );
    // Factor 0 makes every access count, so "hot" is unambiguous.
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "lfu-log-factor", "0"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "lfu-log-factor"]),
        "*2\r\n$14\r\nlfu-log-factor\r\n$1\r\n0\r\n"
    );
    assert!(
        resp_cmd(&mut c, &["OBJECT", "IDLETIME", "probe"])
            .starts_with("-ERR An LFU maxmemory policy is selected")
    );
    assert_eq!(resp_cmd(&mut c, &["SET", "fresh", "v"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["OBJECT", "FREQ", "fresh"]), ":5\r\n");
    assert_eq!(resp_cmd(&mut c, &["OBJECT", "FREQ", "missing"]), "$-1\r\n");

    let val = "x".repeat(1000);
    for i in 0..100 {
        assert_eq!(
            resp_cmd(&mut c, &["SET", &format!("hot:{i}"), &val]),
            "+OK\r\n"
        );
    }
    for _ in 0..50 {
        for i in 0..100 {
            assert!(resp_cmd(&mut c, &["GET", &format!("hot:{i}")]).starts_with("$1000"));
        }
    }
    let freq = resp_cmd(&mut c, &["OBJECT", "FREQ", "hot:7"]);
    let f: i64 = freq.trim_start_matches(':').trim().parse().unwrap();
    assert!(f >= 50, "{freq}");

    // Cap memory near what's used, then write cold keys well past it.
    let used = info_u64(&mut c, "memory", "used_memory");
    assert_eq!(
        resp_cmd(
            &mut c,
            &[
                "CONFIG",
                "SET",
                "maxmemory",
                &(used + 2_000_000).to_string()
            ]
        ),
        "+OK\r\n"
    );
    for i in 0..10_000 {
        let r = resp_cmd(&mut c, &["SET", &format!("cold:{i}"), &val]);
        assert!(r == "+OK\r\n" || r.starts_with("-OOM"), "{r}");
    }
    assert!(info_u64(&mut c, "stats", "evicted_keys") > 1000);
    let hot_alive = (0..100)
        .filter(|i| resp_cmd(&mut c, &["EXISTS", &format!("hot:{i}")]) == ":1\r\n")
        .count();
    assert!(
        hot_alive >= 95,
        "only {hot_alive}/100 hot keys survived LFU eviction"
    );

    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "0"]),
        "+OK\r\n"
    );
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_cross_shard_single_key_pooled_ops_e2e() {
    let port = 17971;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "4", "--no-pin"];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Exercise unpipelined (p=1) cross-shard single-key paths across 32 keys
    // so multiple shards are hit repeatedly and descriptors/responders are reused.
    for round in 0..3 {
        for i in 0..32 {
            let k = format!("p1:pool:{i}");
            assert_eq!(resp_cmd(&mut c, &["SET", &k, "10"]), "+OK\r\n");
            assert_eq!(resp_cmd(&mut c, &["GET", &k]), "$2\r\n10\r\n");
            assert_eq!(resp_cmd(&mut c, &["INCRBY", &k, "5"]), ":15\r\n");
            assert_eq!(resp_cmd(&mut c, &["EXISTS", &k]), ":1\r\n");
            assert_eq!(resp_cmd(&mut c, &["EXPIRE", &k, "120"]), ":1\r\n");
            let ttl: i64 = resp_cmd(&mut c, &["TTL", &k])
                .trim_start_matches(':')
                .trim()
                .parse()
                .unwrap();
            assert!(ttl > 0, "round {round} key {k} ttl={ttl}");
            let exat: i64 = resp_cmd(&mut c, &["EXPIRETIME", &k])
                .trim_start_matches(':')
                .trim()
                .parse()
                .unwrap();
            assert!(exat > 0, "round {round} key {k} expiretime={exat}");
            assert_eq!(resp_cmd(&mut c, &["PERSIST", &k]), ":1\r\n");
            assert_eq!(resp_cmd(&mut c, &["TTL", &k]), ":-1\r\n");
            assert_eq!(resp_cmd(&mut c, &["DEL", &k]), ":1\r\n");
            assert_eq!(resp_cmd(&mut c, &["EXISTS", &k]), ":0\r\n");
        }
    }

    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_commandlog_lolwut_and_sentinel_e2e() {
    let port = 17979u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &port_s, "--threads", "1", "--no-pin"], port);

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // 1. LOLWUT & SENTINEL
    let lol = resp_cmd(&mut c, &["LOLWUT"]);
    assert!(
        lol.starts_with('$') && lol.contains("Redis ver. 7.2.4"),
        "{lol}"
    );
    let lol_v5 = resp_cmd(&mut c, &["LOLWUT", "VERSION", "5", "40"]);
    assert!(
        lol_v5.starts_with('$') && lol_v5.contains("version 5"),
        "{lol_v5}"
    );
    assert!(
        resp_cmd(&mut c, &["LOLWUT", "VERSION", "bad"]).starts_with("-ERR"),
        "expected error on non-integer LOLWUT version"
    );

    let sent = resp_cmd(&mut c, &["SENTINEL", "MASTERS"]);
    assert_eq!(sent, "-ERR This instance has sentinel support disabled\r\n");

    // 2. COMMANDLOG (SLOW, LARGE-REQUEST, LARGE-REPLY) & CONFIG SET/GET
    assert_eq!(
        resp_cmd(&mut c, &["COMMANDLOG", "RESET", "SLOW"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["COMMANDLOG", "RESET", "LARGE-REQUEST"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["COMMANDLOG", "RESET", "LARGE-REPLY"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut c,
            &[
                "CONFIG",
                "SET",
                "commandlog-request-larger-than",
                "150",
                "commandlog-reply-larger-than",
                "150",
            ],
        ),
        "+OK\r\n"
    );
    assert!(
        resp_cmd(&mut c, &["CONFIG", "GET", "commandlog-request-larger-than"])
            .contains("\r\n150\r\n")
    );

    let big_val = "x".repeat(256);
    assert_eq!(
        resp_cmd(&mut c, &["SET", "cmdlog:key", &big_val]),
        "+OK\r\n"
    );
    let got = resp_cmd(&mut c, &["GET", "cmdlog:key"]);
    assert!(got.contains(&big_val), "{got}");

    let req_len = resp_cmd(&mut c, &["COMMANDLOG", "LEN", "LARGE-REQUEST"]);
    assert_eq!(req_len, ":1\r\n");
    let rep_len = resp_cmd(&mut c, &["COMMANDLOG", "LEN", "LARGE-REPLY"]);
    assert_eq!(rep_len, ":1\r\n");

    let req_entries = resp_cmd(&mut c, &["COMMANDLOG", "GET", "10", "LARGE-REQUEST"]);
    assert!(req_entries.contains("cmdlog:key"), "{req_entries}");
    let rep_entries = resp_cmd(&mut c, &["COMMANDLOG", "GET", "10", "LARGE-REPLY"]);
    assert!(rep_entries.contains("cmdlog:key"), "{rep_entries}");

    let help = resp_cmd(&mut c, &["COMMANDLOG", "HELP"]);
    assert!(help.contains("LARGE-REQUEST"), "{help}");

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_perf_regression_gate_live_and_static_e2e() {
    let port = 17980u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &port_s, "--threads", "2", "--no-pin"], port);

    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let script = repo_root.join("scripts/check_perf_regression.py");
    let out = std::process::Command::new("python3")
        .arg(&script)
        .arg("--live")
        .arg("--attach")
        .arg("--port")
        .arg(port.to_string())
        .arg("--duration")
        .arg("0.2")
        .arg("--live-throughput-floor")
        .arg("5000")
        .output()
        .expect("run check_perf_regression.py --live --attach");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "live perf gate failed:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("LIVE-SET"), "{stdout}");
    assert!(stdout.contains("LIVE-GET"), "{stdout}");
    assert!(stdout.contains("GATE SUCCESS"), "{stdout}");

    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_pipelined_multikey_del_and_exists_folding_e2e() {
    let port = 17982u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Send a single pipeline mixing SETs, scattered multi-key EXISTS (with duplicates),
    // scattered multi-key DEL (with duplicates), and post-delete EXISTS across 4 shards.
    let mut pipe = String::new();
    for i in 0..8 {
        pipe.push_str(&format!("SET fold_k_{i} fold_v_{i}\r\n"));
    }
    pipe.push_str("EXISTS fold_k_0 fold_k_1 fold_k_2 fold_k_3 fold_k_0 fold_k_missing\r\n");
    pipe.push_str("DEL fold_k_0 fold_k_1 fold_k_2 fold_k_3 fold_k_0 fold_k_missing\r\n");
    pipe.push_str("EXISTS fold_k_0 fold_k_1 fold_k_4 fold_k_5\r\n");

    let resp = send_and_read(&mut c, pipe.as_bytes());
    let expected = concat!(
        "+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n",
        ":5\r\n",
        ":4\r\n",
        ":2\r\n",
    );
    assert_eq!(resp, expected);

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_pipelined_mset_folding_e2e() {
    let port = 17983u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Send a single pipeline mixing cross-shard MSETs (with duplicate keys within and across MSETs),
    // single-key SETs, and cross-shard MGETs across 4 shards.
    let mut pipe = String::new();
    pipe.push_str("MSET mset_k_0 v0_a mset_k_1 v1_a mset_k_2 v2_a mset_k_3 v3_a mset_k_0 v0_b\r\n");
    pipe.push_str("MSET mset_k_2 v2_b mset_k_4 v4_a mset_k_5 v5_a mset_k_6 v6_a mset_k_7 v7_a\r\n");
    pipe.push_str("SET mset_k_5 v5_override\r\n");
    pipe.push_str(
        "MGET mset_k_0 mset_k_1 mset_k_2 mset_k_3 mset_k_4 mset_k_5 mset_k_6 mset_k_7\r\n",
    );

    let resp = send_and_read(&mut c, pipe.as_bytes());
    let expected = concat!(
        "+OK\r\n",
        "+OK\r\n",
        "+OK\r\n",
        "*8\r\n",
        "$4\r\nv0_b\r\n",
        "$4\r\nv1_a\r\n",
        "$4\r\nv2_b\r\n",
        "$4\r\nv3_a\r\n",
        "$4\r\nv4_a\r\n",
        "$11\r\nv5_override\r\n",
        "$4\r\nv6_a\r\n",
        "$4\r\nv7_a\r\n",
    );
    assert_eq!(resp, expected);

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_pipelined_unlink_touch_hmget_smismember_zmscore_e2e() {
    let port = 17984u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Send a single pipeline exercising cross-shard HSET+HMGET, SADD+SMISMEMBER, ZADD+ZMSCORE,
    // multi-key TOUCH, and multi-key UNLINK across 4 shards.
    let mut pipe = String::new();
    pipe.push_str("HSET pipe_h_1 f1 v1 f2 v2\r\n");
    pipe.push_str("HSET pipe_h_2 fa va fb vb\r\n");
    pipe.push_str("HMGET pipe_h_1 f1 missing f2\r\n");
    pipe.push_str("HMGET pipe_h_2 fb missing fa\r\n");
    pipe.push_str("SADD pipe_s_1 m1 m2\r\n");
    pipe.push_str("SADD pipe_s_2 ma mb\r\n");
    pipe.push_str("SMISMEMBER pipe_s_1 m1 m_none m2\r\n");
    pipe.push_str("SMISMEMBER pipe_s_2 mb m_none ma\r\n");
    pipe.push_str("ZADD pipe_z_1 1.5 zm1 2.5 zm2\r\n");
    pipe.push_str("ZADD pipe_z_2 3.5 zma 4.5 zmb\r\n");
    pipe.push_str("ZMSCORE pipe_z_1 zm1 zm_none zm2\r\n");
    pipe.push_str("ZMSCORE pipe_z_2 zmb zm_none zma\r\n");
    pipe.push_str(
        "TOUCH pipe_h_1 pipe_h_2 pipe_s_1 pipe_s_2 pipe_z_1 pipe_z_2 pipe_missing_key\r\n",
    );
    pipe.push_str("UNLINK pipe_h_1 pipe_s_2 pipe_z_1 pipe_missing_key\r\n");
    pipe.push_str("TOUCH pipe_h_1 pipe_h_2 pipe_s_1 pipe_s_2 pipe_z_1 pipe_z_2\r\n");

    let resp = send_and_read(&mut c, pipe.as_bytes());
    let expected = concat!(
        ":2\r\n",
        ":2\r\n",
        "*3\r\n$2\r\nv1\r\n$-1\r\n$2\r\nv2\r\n",
        "*3\r\n$2\r\nvb\r\n$-1\r\n$2\r\nva\r\n",
        ":2\r\n",
        ":2\r\n",
        "*3\r\n:1\r\n:0\r\n:1\r\n",
        "*3\r\n:1\r\n:0\r\n:1\r\n",
        ":2\r\n",
        ":2\r\n",
        "*3\r\n$3\r\n1.5\r\n$-1\r\n$3\r\n2.5\r\n",
        "*3\r\n$3\r\n4.5\r\n$-1\r\n$3\r\n3.5\r\n",
        ":6\r\n",
        ":3\r\n",
        ":3\r\n",
    );
    assert_eq!(resp, expected);

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_pipelined_rpush_spop_zpopmin_e2e() {
    let port = 17985u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // One pipeline whose keys land on different shards, so both the local
    // fast paths and the cross-shard batch fast paths run.
    let mut pipe = String::new();
    for i in 0..4 {
        pipe.push_str(&format!("RPUSH rp_l_{i} a b\r\n"));
        pipe.push_str(&format!("RPUSH rp_l_{i} c\r\n"));
        pipe.push_str(&format!("LRANGE rp_l_{i} 0 -1\r\n"));
        pipe.push_str(&format!("SADD rp_s_{i} m{i}\r\n"));
        pipe.push_str(&format!("SPOP rp_s_{i}\r\n"));
        pipe.push_str(&format!("SPOP rp_s_{i}\r\n"));
        pipe.push_str(&format!("ZADD rp_z_{i} 2.5 zb 1.5 za\r\n"));
        pipe.push_str(&format!("ZPOPMIN rp_z_{i}\r\n"));
        pipe.push_str(&format!("ZPOPMIN rp_z_{i}\r\n"));
        pipe.push_str(&format!("ZPOPMIN rp_z_{i}\r\n"));
        pipe.push_str(&format!("EXISTS rp_s_{i} rp_z_{i}\r\n"));
        pipe.push_str(&format!("SPOP rp_l_{i}\r\n"));
    }

    let resp = send_and_read(&mut c, pipe.as_bytes());
    let mut expected = String::new();
    for i in 0..4 {
        expected.push_str(":2\r\n:3\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        expected.push_str(&format!(":1\r\n$2\r\nm{i}\r\n$-1\r\n"));
        expected
            .push_str(":2\r\n*2\r\n$2\r\nza\r\n$3\r\n1.5\r\n*2\r\n$2\r\nzb\r\n$3\r\n2.5\r\n*0\r\n");
        expected.push_str(":0\r\n");
        expected.push_str("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    assert_eq!(resp, expected);

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_hdel_srem_zrem_touch_single_and_multi_e2e() {
    let port = 17986u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Single- and multi-item forms, in mixed case, across several shards.
    let mut pipe = String::new();
    let mut expected = String::new();
    for i in 0..4 {
        pipe.push_str(&format!("HSET hd_{i} a 1 b 2 c 3\r\n"));
        pipe.push_str(&format!("hdel hd_{i} a\r\n"));
        pipe.push_str(&format!("HDEL hd_{i} b c missing\r\n"));
        pipe.push_str(&format!("SADD sr_{i} a b c\r\n"));
        pipe.push_str(&format!("SrEm sr_{i} a\r\n"));
        pipe.push_str(&format!("SREM sr_{i} b c missing\r\n"));
        pipe.push_str(&format!("ZADD zr_{i} 1 a 2 b 3 c\r\n"));
        pipe.push_str(&format!("zrem zr_{i} a\r\n"));
        pipe.push_str(&format!("ZREM zr_{i} b c missing\r\n"));
        pipe.push_str(&format!("SET tk_{i} v\r\n"));
        pipe.push_str(&format!("touch tk_{i}\r\n"));
        pipe.push_str(&format!("TOUCH tk_{i} tk_missing hd_{i} tk_{i}\r\n"));
        expected.push_str(":3\r\n:1\r\n:2\r\n");
        expected.push_str(":3\r\n:1\r\n:2\r\n");
        expected.push_str(":3\r\n:1\r\n:2\r\n");
        // hd_{i} was emptied (and so deleted) above; tk_{i} counts twice.
        expected.push_str("+OK\r\n:1\r\n:2\r\n");
    }

    let resp = send_and_read(&mut c, pipe.as_bytes());
    assert_eq!(resp, expected);

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_rpush_rpop_spop_zpopmin_parse_forms_e2e() {
    let port = 17987u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Key-only and counted forms, mixed case, across several shards.
    let mut pipe = String::new();
    let mut expected = String::new();
    for i in 0..4 {
        pipe.push_str(&format!("rpush pf_l_{i} a\r\n"));
        pipe.push_str(&format!("RPUSH pf_l_{i} b c d\r\n"));
        pipe.push_str(&format!("RpOp pf_l_{i}\r\n"));
        pipe.push_str(&format!("RPOP pf_l_{i} 2\r\n"));
        pipe.push_str(&format!("SADD pf_s_{i} x\r\n"));
        pipe.push_str(&format!("spop pf_s_{i}\r\n"));
        pipe.push_str(&format!("SPOP pf_s_{i} 2\r\n"));
        pipe.push_str(&format!("ZADD pf_z_{i} 1 p 2 q 3 r\r\n"));
        pipe.push_str(&format!("zpopmin pf_z_{i}\r\n"));
        pipe.push_str(&format!("ZPOPMIN pf_z_{i} 2\r\n"));
        expected.push_str(":1\r\n:4\r\n$1\r\nd\r\n*2\r\n$1\r\nc\r\n$1\r\nb\r\n");
        expected.push_str(":1\r\n$1\r\nx\r\n*0\r\n");
        expected.push_str(
            ":3\r\n*2\r\n$1\r\np\r\n$1\r\n1\r\n*4\r\n$1\r\nq\r\n$1\r\n2\r\n$1\r\nr\r\n$1\r\n3\r\n",
        );
    }

    let resp = send_and_read(&mut c, pipe.as_bytes());
    assert_eq!(resp, expected);

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_pipelined_hdel_srem_zrem_fast_paths_e2e() {
    let port = 17988u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c
    };
    let mut c = connect();

    // Counts, deletion of emptied keys and WRONGTYPE, across several shards.
    let mut pipe = String::new();
    let mut expected = String::new();
    for i in 0..4 {
        pipe.push_str(&format!(
            "HSET fr_h_{i} a 1 b 2\r\nHDEL fr_h_{i} a x\r\nHDEL fr_h_{i} b\r\n"
        ));
        pipe.push_str(&format!(
            "SADD fr_s_{i} a b\r\nSREM fr_s_{i} a x\r\nSREM fr_s_{i} b\r\n"
        ));
        pipe.push_str(&format!("ZADD fr_z_{i} 1 a 2 b\r\nZREM fr_z_{i} a b x\r\n"));
        pipe.push_str(&format!("EXISTS fr_h_{i} fr_s_{i} fr_z_{i}\r\n"));
        pipe.push_str(&format!("SET fr_str_{i} v\r\nSREM fr_str_{i} a\r\n"));
        expected.push_str(":2\r\n:1\r\n:1\r\n:2\r\n:1\r\n:1\r\n:2\r\n:2\r\n:0\r\n+OK\r\n");
        expected.push_str("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
    }
    assert_eq!(send_and_read(&mut c, pipe.as_bytes()), expected);

    // A pipelined SREM that removes a member aborts another client's WATCH.
    let mut watcher = connect();
    assert_eq!(send_and_read(&mut c, b"SADD fr_watch m n\r\n"), ":2\r\n");
    assert_eq!(
        send_and_read(&mut watcher, b"WATCH fr_watch\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut watcher, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut watcher, b"SCARD fr_watch\r\n"),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(&mut c, b"SET fr_d1 v\r\nSREM fr_watch m\r\nSET fr_d2 v\r\n"),
        "+OK\r\n:1\r\n+OK\r\n"
    );
    assert_eq!(send_and_read(&mut watcher, b"EXEC\r\n"), "*-1\r\n");

    // One that removes nothing leaves the key unmodified, as in Redis.
    assert_eq!(
        send_and_read(&mut watcher, b"WATCH fr_watch\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read(&mut watcher, b"MULTI\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut watcher, b"SCARD fr_watch\r\n"),
        "+QUEUED\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut c,
            b"SET fr_d1 v\r\nSREM fr_watch missing\r\nSET fr_d2 v\r\n"
        ),
        "+OK\r\n:0\r\n+OK\r\n"
    );
    assert_eq!(send_and_read(&mut watcher, b"EXEC\r\n"), "*1\r\n:1\r\n");

    drop(c);
    drop(watcher);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_set_members_of_every_length_across_encodings_e2e() {
    let port = 17989u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Members at the inline-storage boundaries (22/23/24 bytes), long, binary
    // and empty, in sets that stay small and sets that convert (> 64 members).
    let special: Vec<String> = vec![
        String::new(),
        "x".repeat(22),
        "y".repeat(23),
        "z".repeat(24),
        "w".repeat(300),
        "bin\0ary".to_string(),
    ];
    for (key, fill) in [("es_small", 0usize), ("es_big", 100)] {
        let mut members = special.clone();
        members.extend((0..fill).map(|i| format!("m{i:03}")));
        let mut args = vec!["SADD", key];
        args.extend(members.iter().map(String::as_str));
        assert_eq!(resp_cmd(&mut c, &args), format!(":{}\r\n", members.len()));
        for m in &special {
            assert_eq!(
                resp_cmd(&mut c, &["SISMEMBER", key, m]),
                ":1\r\n",
                "{key} {m:?}"
            );
        }
        assert_eq!(
            resp_cmd(&mut c, &["SISMEMBER", key, &"z".repeat(25)]),
            ":0\r\n"
        );
        assert_eq!(
            resp_cmd(&mut c, &["SCARD", key]),
            format!(":{}\r\n", members.len())
        );
        // SMEMBERS returns every member byte-for-byte.
        let reply = resp_cmd(&mut c, &["SMEMBERS", key]);
        for m in &members {
            assert!(
                reply.contains(&format!("${}\r\n{m}\r\n", m.len())),
                "{key} missing {m:?}"
            );
        }
        assert_eq!(resp_cmd(&mut c, &["SREM", key, &"w".repeat(300)]), ":1\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["SISMEMBER", key, &"w".repeat(300)]),
            ":0\r\n"
        );
        // Draining with SPOP returns each remaining member exactly once.
        let mut popped = Vec::new();
        loop {
            let r = resp_cmd(&mut c, &["SPOP", key]);
            if r == "$-1\r\n" {
                break;
            }
            popped.push(r);
        }
        assert_eq!(popped.len(), members.len() - 1, "{key}");
        popped.sort();
        popped.dedup();
        assert_eq!(
            popped.len(),
            members.len() - 1,
            "{key} popped a member twice"
        );
        assert_eq!(resp_cmd(&mut c, &["EXISTS", key]), ":0\r\n");
    }

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_zset_members_of_every_length_across_encodings_e2e() {
    let port = 17995u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Members at the inline-storage boundaries, long, binary and empty, in a
    // sorted set that stays small and one that converts (> 64 members).
    let special: Vec<String> = vec![
        String::new(),
        "x".repeat(22),
        "y".repeat(23),
        "z".repeat(24),
        "w".repeat(300),
        "bin\0ary".to_string(),
    ];
    for (key, fill) in [("ez_small", 0usize), ("ez_big", 100)] {
        let mut args: Vec<String> = vec!["ZADD".into(), key.into()];
        for (i, m) in special.iter().enumerate() {
            args.push(i.to_string());
            args.push(m.clone());
        }
        for i in 0..fill {
            args.push("1000".into());
            args.push(format!("m{i:03}"));
        }
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let total = special.len() + fill;
        assert_eq!(resp_cmd(&mut c, &argv), format!(":{total}\r\n"));
        for (i, m) in special.iter().enumerate() {
            assert_eq!(
                resp_cmd(&mut c, &["ZSCORE", key, m]),
                format!("${}\r\n{i}\r\n", i.to_string().len()),
                "{key} {m:?}"
            );
            assert_eq!(
                resp_cmd(&mut c, &["ZRANK", key, m]),
                format!(":{i}\r\n"),
                "{key} {m:?}"
            );
        }
        // Score order, then member order among equal scores.
        assert_eq!(
            resp_cmd(&mut c, &["ZRANGE", key, "0", "1"]),
            format!("*2\r\n$0\r\n\r\n$22\r\n{}\r\n", "x".repeat(22))
        );
        if fill > 0 {
            assert_eq!(
                resp_cmd(
                    &mut c,
                    &["ZRANGEBYSCORE", key, "1000", "1000", "LIMIT", "0", "2"]
                ),
                "*2\r\n$4\r\nm000\r\n$4\r\nm001\r\n"
            );
        }
        assert_eq!(
            resp_cmd(&mut c, &["ZRANGEBYSCORE", key, "3", "4"]),
            format!(
                "*2\r\n$24\r\n{}\r\n$300\r\n{}\r\n",
                "z".repeat(24),
                "w".repeat(300)
            )
        );
        // Re-scoring a long member moves it to the front.
        assert_eq!(
            resp_cmd(&mut c, &["ZADD", key, "-1", &"w".repeat(300)]),
            ":0\r\n"
        );
        assert_eq!(
            resp_cmd(&mut c, &["ZRANK", key, &"w".repeat(300)]),
            ":0\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["ZREM", key, "bin\0ary"]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["ZSCORE", key, "bin\0ary"]), "$-1\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["ZPOPMIN", key]),
            format!("*2\r\n$300\r\n{}\r\n$2\r\n-1\r\n", "w".repeat(300))
        );
        assert_eq!(
            resp_cmd(&mut c, &["ZCARD", key]),
            format!(":{}\r\n", total - 2)
        );
        assert_eq!(resp_cmd(&mut c, &["DEL", key]), ":1\r\n");
    }

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_slowlog_args_survive_read_buffer_reuse_e2e() {
    let port = 17997u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "2",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "slowlog-max-len", "1024"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "slowlog-log-slower-than", "0"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["SLOWLOG", "RESET"]), "+OK\r\n");

    // GET is parsed zero-copy out of the connection's read buffer. Log one,
    // then push lots of unrelated traffic through the same connection so the
    // buffer is reused, and check the logged arguments are intact.
    assert_eq!(resp_cmd(&mut c, &["GET", "slowlog_marker_key"]), "$-1\r\n");
    let big = "v".repeat(4000);
    for i in 0..200 {
        assert_eq!(
            resp_cmd(&mut c, &["SET", &format!("churn_{i}"), &big]),
            "+OK\r\n"
        );
    }
    assert_eq!(
        resp_cmd(
            &mut c,
            &["CONFIG", "SET", "slowlog-log-slower-than", "10000000"]
        ),
        "+OK\r\n"
    );

    c.write_all(b"SLOWLOG GET 1000\r\n").unwrap();
    let mut log = Vec::new();
    let mut buf = [0u8; 65536];
    while !String::from_utf8_lossy(&log).contains("slowlog_marker_key") {
        match c.read(&mut buf) {
            Ok(n) if n > 0 => log.extend_from_slice(&buf[..n]),
            _ => break,
        }
    }
    let log = String::from_utf8_lossy(&log);
    assert!(
        log.contains("$3\r\nget\r\n$18\r\nslowlog_marker_key\r\n"),
        "marker GET missing or corrupted in SLOWLOG ({} bytes): {}",
        log.len(),
        &log[..log.len().min(600)]
    );

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_list_elements_of_every_length_e2e() {
    let port = 17996u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let bulk = |s: &str| format!("${}\r\n{s}\r\n", s.len());

    // Elements at the inline-storage boundaries, long, binary and empty.
    let els: Vec<String> = vec![
        String::new(),
        "x".repeat(22),
        "y".repeat(23),
        "z".repeat(24),
        "w".repeat(300),
        "bin\0ary".to_string(),
    ];
    let mut args = vec!["RPUSH", "el"];
    args.extend(els.iter().map(String::as_str));
    assert_eq!(resp_cmd(&mut c, &args), ":6\r\n");
    for (i, e) in els.iter().enumerate() {
        assert_eq!(
            resp_cmd(&mut c, &["LINDEX", "el", &i.to_string()]),
            bulk(e),
            "{e:?}"
        );
    }
    let all: String = els.iter().map(|e| bulk(e)).collect();
    assert_eq!(
        resp_cmd(&mut c, &["LRANGE", "el", "0", "-1"]),
        format!("*6\r\n{all}")
    );
    assert_eq!(
        resp_cmd(&mut c, &["LSET", "el", "1", &"q".repeat(40)]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["LINSERT", "el", "BEFORE", &"z".repeat(24), "ins"]),
        ":7\r\n"
    );
    // Move the long element to another list (possibly another shard) and back.
    assert_eq!(
        resp_cmd(&mut c, &["LMOVE", "el", "el2", "RIGHT", "LEFT"]),
        bulk("bin\0ary")
    );
    assert_eq!(
        resp_cmd(&mut c, &["LMOVE", "el2", "el", "LEFT", "LEFT"]),
        bulk("bin\0ary")
    );
    assert_eq!(resp_cmd(&mut c, &["LPOP", "el"]), bulk("bin\0ary"));
    assert_eq!(resp_cmd(&mut c, &["RPOP", "el"]), bulk(&"w".repeat(300)));
    let expect = ["", &"q".repeat(40), &"y".repeat(23), "ins", &"z".repeat(24)];
    let rest: String = expect.iter().map(|e| bulk(e)).collect();
    assert_eq!(
        resp_cmd(&mut c, &["LPOP", "el", "10"]),
        format!("*5\r\n{rest}")
    );
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "el", "el2"]), ":0\r\n");

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_large_sorted_set_order_rank_and_removal_e2e() {
    let port = 17998u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Reads one full RESP reply (enough for these tests: arrays of bulks or
    // integers) instead of a single read() that may return part of it.
    fn read_reply(c: &mut TcpStream, want_lines: usize) -> String {
        let mut out = Vec::new();
        let mut buf = [0u8; 65536];
        while out.windows(2).filter(|w| *w == b"\r\n").count() < want_lines {
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            out.extend_from_slice(&buf[..n]);
        }
        String::from_utf8(out).unwrap()
    }

    // 1000 members, scores 0..99 so many tie and order by member bytes.
    let mut model: Vec<(i64, String)> = (0..1000)
        .map(|i| ((i * 37) % 100, format!("m{i:04}")))
        .collect();
    let mut args = vec!["ZADD".to_string(), "big".to_string()];
    for (s, m) in &model {
        args.push(s.to_string());
        args.push(m.clone());
    }
    let argv: Vec<&str> = args.iter().map(String::as_str).collect();
    assert_eq!(resp_cmd(&mut c, &argv), ":1000\r\n");
    model.sort();

    let check_order = |c: &mut TcpStream, model: &Vec<(i64, String)>| {
        c.write_all(b"ZRANGE big 0 -1\r\n").unwrap();
        let reply = read_reply(c, 1 + 2 * model.len());
        let expect: String = std::iter::once(format!("*{}\r\n", model.len()))
            .chain(
                model
                    .iter()
                    .map(|(_, m)| format!("${}\r\n{m}\r\n", m.len())),
            )
            .collect();
        assert_eq!(reply, expect);
    };
    check_order(&mut c, &model);
    for probe in [0usize, 1, 63, 64, 127, 128, 500, 999] {
        let (_, m) = &model[probe];
        assert_eq!(
            resp_cmd(&mut c, &["ZRANK", "big", m]),
            format!(":{probe}\r\n")
        );
        assert_eq!(
            resp_cmd(&mut c, &["ZREVRANK", "big", m]),
            format!(":{}\r\n", model.len() - 1 - probe)
        );
    }

    // Remove every third member, re-score some, pop both ends.
    let mut pipe = String::new();
    let mut removed = 0;
    for (i, (_, m)) in model.iter().enumerate() {
        if i % 3 == 0 {
            pipe.push_str(&format!("ZREM big {m}\r\n"));
            removed += 1;
        }
    }
    c.write_all(pipe.as_bytes()).unwrap();
    assert_eq!(read_reply(&mut c, removed), ":1\r\n".repeat(removed));
    model = model
        .into_iter()
        .enumerate()
        .filter(|(i, _)| i % 3 != 0)
        .map(|(_, x)| x)
        .collect();
    for m in ["m0001", "m0500", "m0998"] {
        if let Some(e) = model.iter_mut().find(|(_, x)| x == m) {
            e.0 = -1;
            assert_eq!(resp_cmd(&mut c, &["ZADD", "big", "-1", m]), ":0\r\n");
        }
    }
    model.sort();
    check_order(&mut c, &model);
    let (s, m) = model.remove(0);
    assert_eq!(
        resp_cmd(&mut c, &["ZPOPMIN", "big"]),
        format!(
            "*2\r\n${}\r\n{m}\r\n${}\r\n{s}\r\n",
            m.len(),
            s.to_string().len()
        )
    );
    let (s, m) = model.pop().unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["ZPOPMAX", "big"]),
        format!(
            "*2\r\n${}\r\n{m}\r\n${}\r\n{s}\r\n",
            m.len(),
            s.to_string().len()
        )
    );
    // Score range from a value with many ties starts at its first member.
    let first_50 = model.iter().find(|(s, _)| *s == 50).unwrap().1.clone();
    assert_eq!(
        resp_cmd(
            &mut c,
            &["ZRANGEBYSCORE", "big", "50", "50", "LIMIT", "0", "1"]
        ),
        format!("*1\r\n${}\r\n{first_50}\r\n", first_50.len())
    );
    assert_eq!(
        resp_cmd(&mut c, &["ZCARD", "big"]),
        format!(":{}\r\n", model.len())
    );
    check_order(&mut c, &model);

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_packed_list_across_chunk_boundaries_e2e() {
    let port = 18001u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    fn read_reply(c: &mut TcpStream, want_lines: usize) -> String {
        let mut out = Vec::new();
        let mut buf = [0u8; 65536];
        while out.windows(2).filter(|w| *w == b"\r\n").count() < want_lines {
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            out.extend_from_slice(&buf[..n]);
        }
        String::from_utf8(out).unwrap()
    }
    let bulk = |s: &str| format!("${}\r\n{s}\r\n", s.len());
    let range_of = |c: &mut TcpStream, start: i64, stop: i64, n: usize| -> String {
        c.write_all(format!("LRANGE pl {start} {stop}\r\n").as_bytes())
            .unwrap();
        read_reply(c, 1 + 2 * n)
    };

    // 2000 elements: mostly short (packed several hundred per chunk), every
    // 37th long (a chunk of its own), pushed from both ends.
    let mut model: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut pipe = String::new();
    for i in 0..2000 {
        let v = if i % 37 == 0 {
            format!("{}-{i}", "L".repeat(80))
        } else {
            format!("s{i}")
        };
        if i % 2 == 0 {
            pipe.push_str(&format!("RPUSH pl {v}\r\n"));
            model.push_back(v);
        } else {
            pipe.push_str(&format!("LPUSH pl {v}\r\n"));
            model.push_front(v);
        }
    }
    c.write_all(pipe.as_bytes()).unwrap();
    read_reply(&mut c, 2000);
    let model_range = |m: &std::collections::VecDeque<String>, a: usize, b: usize| -> String {
        std::iter::once(format!("*{}\r\n", b - a + 1))
            .chain(m.iter().skip(a).take(b - a + 1).map(|v| bulk(v)))
            .collect()
    };
    assert_eq!(
        range_of(&mut c, 0, -1, model.len()),
        model_range(&model, 0, model.len() - 1)
    );
    for i in [0usize, 1, 63, 64, 255, 256, 999, 1000, 1500, 1998, 1999] {
        assert_eq!(
            resp_cmd(&mut c, &["LINDEX", "pl", &i.to_string()]),
            bulk(&model[i]),
            "LINDEX {i}"
        );
    }
    assert_eq!(
        range_of(&mut c, 990, 1010, 21),
        model_range(&model, 990, 1010)
    );

    // Mid-list edits, short and long.
    let pivot = model[700].clone();
    assert_eq!(
        resp_cmd(&mut c, &["LINSERT", "pl", "BEFORE", &pivot, "ins-short"]),
        ":2001\r\n"
    );
    model.insert(700, "ins-short".into());
    let long = "Q".repeat(100);
    assert_eq!(
        resp_cmd(&mut c, &["LINSERT", "pl", "AFTER", &pivot, &long]),
        ":2002\r\n"
    );
    model.insert(702, long.clone());
    assert_eq!(
        resp_cmd(&mut c, &["LSET", "pl", "1234", "set-short"]),
        "+OK\r\n"
    );
    model[1234] = "set-short".into();
    assert_eq!(resp_cmd(&mut c, &["LSET", "pl", "1235", &long]), "+OK\r\n");
    model[1235] = long.clone();
    // LREM from the tail removes the last two copies of `long`.
    assert_eq!(resp_cmd(&mut c, &["LREM", "pl", "-2", &long]), ":2\r\n");
    model.remove(1235);
    model.remove(702);
    assert_eq!(
        resp_cmd(&mut c, &["LPOS", "pl", "set-short", "RANK", "-1"]),
        ":1233\r\n"
    );
    assert_eq!(
        range_of(&mut c, 0, -1, model.len()),
        model_range(&model, 0, model.len() - 1)
    );

    assert_eq!(resp_cmd(&mut c, &["LTRIM", "pl", "500", "1499"]), "+OK\r\n");
    let trimmed: std::collections::VecDeque<String> =
        model.iter().skip(500).take(1000).cloned().collect();
    assert_eq!(range_of(&mut c, 0, -1, 1000), model_range(&trimmed, 0, 999));
    assert_eq!(resp_cmd(&mut c, &["LLEN", "pl"]), ":1000\r\n");

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_packed_set_compaction_and_set_algebra_e2e() {
    use std::collections::HashSet;
    let port = 18002u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    enum V {
        Bulk(Vec<u8>),
        Arr(Vec<V>),
        Other,
    }
    // Parses one complete RESP value at `i`; None if more bytes are needed.
    fn parse(b: &[u8], i: usize) -> Option<(V, usize)> {
        let nl = i + b.get(i..)?.windows(2).position(|w| w == b"\r\n")?;
        let head = std::str::from_utf8(&b[i + 1..nl]).unwrap();
        match b[i] {
            b'$' => {
                let n: usize = head.parse().unwrap();
                let end = nl + 2 + n + 2;
                (b.len() >= end).then(|| (V::Bulk(b[nl + 2..nl + 2 + n].to_vec()), end))
            }
            b'*' => {
                let n: usize = head.parse().unwrap();
                let mut items = Vec::with_capacity(n);
                let mut j = nl + 2;
                for _ in 0..n {
                    let (v, k) = parse(b, j)?;
                    items.push(v);
                    j = k;
                }
                Some((V::Arr(items), j))
            }
            _ => Some((V::Other, nl + 2)),
        }
    }
    fn cmd(c: &mut TcpStream, args: &[&str]) -> V {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        c.write_all(out.as_bytes()).unwrap();
        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            if let Some((v, _)) = parse(&got, 0) {
                return v;
            }
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
    }
    fn members(v: V) -> HashSet<String> {
        let V::Arr(items) = v else {
            panic!("expected an array")
        };
        items
            .into_iter()
            .map(|i| match i {
                V::Bulk(b) => String::from_utf8(b).unwrap(),
                _ => panic!("expected bulk strings"),
            })
            .collect()
    }
    fn pipeline(c: &mut TcpStream, cmds: &[String]) {
        let mut out = String::new();
        for line in cmds {
            out.push_str(line);
            out.push_str("\r\n");
        }
        out.push_str("PING\r\n");
        c.write_all(out.as_bytes()).unwrap();
        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        while !got.ends_with(b"+PONG\r\n") {
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
    }

    // 3000 members: short ones share pages, every 50th is over 1 KB (a page
    // of its own).
    let name = |i: usize| {
        if i.is_multiple_of(50) {
            format!("{}{i}", "B".repeat(1500))
        } else {
            format!("m{i}")
        }
    };
    let mut model: HashSet<String> = (0..3000).map(name).collect();
    pipeline(
        &mut c,
        &(0..3000)
            .map(|i| format!("SADD ps {}", name(i)))
            .collect::<Vec<_>>(),
    );
    // Remove two of every three: most pages pass half dead and compact.
    let gone: Vec<String> = (0..3000).filter(|i| i % 3 != 0).map(name).collect();
    pipeline(
        &mut c,
        &gone
            .iter()
            .map(|m| format!("SREM ps {m}"))
            .collect::<Vec<_>>(),
    );
    for m in &gone {
        model.remove(m);
    }
    for _ in 0..150 {
        let V::Bulk(p) = cmd(&mut c, &["SPOP", "ps"]) else {
            panic!("SPOP")
        };
        assert!(model.remove(&String::from_utf8(p).unwrap()));
    }
    // Re-adding after compaction reuses space and still finds everything.
    let again: Vec<String> = (3000..3300).map(name).collect();
    pipeline(
        &mut c,
        &again
            .iter()
            .map(|m| format!("SADD ps {m}"))
            .collect::<Vec<_>>(),
    );
    model.extend(again);

    assert_eq!(
        resp_cmd(&mut c, &["SCARD", "ps"]),
        format!(":{}\r\n", model.len())
    );
    assert_eq!(members(cmd(&mut c, &["SMEMBERS", "ps"])), model);
    for i in [0usize, 3, 50, 150, 300, 1, 2, 3050, 3100, 3299] {
        let m = name(i);
        let want = if model.contains(&m) {
            ":1\r\n"
        } else {
            ":0\r\n"
        };
        assert_eq!(resp_cmd(&mut c, &["SISMEMBER", "ps", &m]), want, "{m}");
    }

    // Set algebra against a second set.
    let other: HashSet<String> = (0..3300).filter(|i| i % 5 == 0).map(name).collect();
    pipeline(
        &mut c,
        &other
            .iter()
            .map(|m| format!("SADD ps2 {m}"))
            .collect::<Vec<_>>(),
    );
    let inter: HashSet<String> = model.intersection(&other).cloned().collect();
    let union: HashSet<String> = model.union(&other).cloned().collect();
    let diff: HashSet<String> = model.difference(&other).cloned().collect();
    assert_eq!(members(cmd(&mut c, &["SINTER", "ps", "ps2"])), inter);
    assert_eq!(members(cmd(&mut c, &["SUNION", "ps", "ps2"])), union);
    assert_eq!(members(cmd(&mut c, &["SDIFF", "ps", "ps2"])), diff);

    // A full SSCAN sees every member.
    let mut seen = HashSet::new();
    let mut cursor = "0".to_string();
    loop {
        let V::Arr(mut parts) = cmd(&mut c, &["SSCAN", "ps", &cursor, "COUNT", "200"]) else {
            panic!("SSCAN")
        };
        let batch = parts.pop().unwrap();
        let V::Bulk(next) = parts.pop().unwrap() else {
            panic!("cursor")
        };
        seen.extend(members(batch));
        cursor = String::from_utf8(next).unwrap();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen, model);

    // Draining with SPOP empties and deletes the key.
    let n = model.len();
    let V::Arr(popped) = cmd(&mut c, &["SPOP", "ps", &n.to_string()]) else {
        panic!("SPOP n")
    };
    assert_eq!(popped.len(), n);
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "ps"]), ":0\r\n");

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_packed_zset_compaction_and_order_e2e() {
    use std::collections::BTreeMap;
    let port = 17999u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    enum V {
        Bulk(Vec<u8>),
        Arr(Vec<V>),
        Other,
    }
    fn parse(b: &[u8], i: usize) -> Option<(V, usize)> {
        let nl = i + b.get(i..)?.windows(2).position(|w| w == b"\r\n")?;
        let head = std::str::from_utf8(&b[i + 1..nl]).unwrap();
        match b[i] {
            b'$' => {
                let n: usize = head.parse().unwrap();
                let end = nl + 2 + n + 2;
                (b.len() >= end).then(|| (V::Bulk(b[nl + 2..nl + 2 + n].to_vec()), end))
            }
            b'*' => {
                let n: usize = head.parse().unwrap();
                let mut items = Vec::with_capacity(n);
                let mut j = nl + 2;
                for _ in 0..n {
                    let (v, k) = parse(b, j)?;
                    items.push(v);
                    j = k;
                }
                Some((V::Arr(items), j))
            }
            _ => Some((V::Other, nl + 2)),
        }
    }
    fn cmd(c: &mut TcpStream, args: &[&str]) -> V {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        c.write_all(out.as_bytes()).unwrap();
        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            if let Some((v, _)) = parse(&got, 0) {
                return v;
            }
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
    }
    fn strings(v: V) -> Vec<String> {
        let V::Arr(items) = v else {
            panic!("expected an array")
        };
        items
            .into_iter()
            .map(|i| match i {
                V::Bulk(b) => String::from_utf8(b).unwrap(),
                _ => panic!("expected bulk strings"),
            })
            .collect()
    }
    fn pipeline(c: &mut TcpStream, cmds: &[String]) {
        let mut out = String::new();
        for line in cmds {
            out.push_str(line);
            out.push_str("\r\n");
        }
        out.push_str("PING\r\n");
        c.write_all(out.as_bytes()).unwrap();
        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        while !got.ends_with(b"+PONG\r\n") {
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
    }
    // Model: member -> integer score; order is (score, member).
    fn order(m: &BTreeMap<String, i64>) -> Vec<(i64, String)> {
        let mut v: Vec<(i64, String)> = m.iter().map(|(k, s)| (*s, k.clone())).collect();
        v.sort();
        v
    }

    // 3000 members, short ones packed several hundred per page and every
    // 50th over 1 KiB (a page of its own); many score ties.
    let name = |i: usize| {
        if i.is_multiple_of(50) {
            format!("{}{i:05}", "B".repeat(1500))
        } else {
            format!("m{i:05}")
        }
    };
    let mut model: BTreeMap<String, i64> = BTreeMap::new();
    let mut adds = Vec::new();
    for i in 0..3000 {
        let s = (i % 97) as i64 - 40;
        adds.push(format!("ZADD pz {s} {}", name(i)));
        model.insert(name(i), s);
    }
    pipeline(&mut c, &adds);
    // Remove two of every three: pages pass half dead and compact.
    let gone: Vec<String> = (0..3000).filter(|i| i % 3 != 0).map(name).collect();
    pipeline(
        &mut c,
        &gone
            .iter()
            .map(|m| format!("ZREM pz {m}"))
            .collect::<Vec<_>>(),
    );
    for m in &gone {
        model.remove(m);
    }
    // Rescore survivors in place, and pop from both ends.
    let mut rescores = Vec::new();
    for i in (0..3000).step_by(9) {
        let s = -(i as i64);
        rescores.push(format!("ZADD pz {s} {}", name(i)));
        model.insert(name(i), s);
    }
    pipeline(&mut c, &rescores);
    for _ in 0..40 {
        let want = order(&model);
        let got = strings(cmd(&mut c, &["ZPOPMIN", "pz"]));
        assert_eq!(got[0], want[0].1);
        model.remove(&want[0].1);
        let want = order(&model);
        let got = strings(cmd(&mut c, &["ZPOPMAX", "pz"]));
        assert_eq!(got[0], want.last().unwrap().1);
        model.remove(&want.last().unwrap().1);
    }

    let want = order(&model);
    assert_eq!(
        resp_cmd(&mut c, &["ZCARD", "pz"]),
        format!(":{}\r\n", want.len())
    );
    let got = strings(cmd(&mut c, &["ZRANGE", "pz", "0", "-1", "WITHSCORES"]));
    let want_flat: Vec<String> = want
        .iter()
        .flat_map(|(s, m)| [m.clone(), s.to_string()])
        .collect();
    assert_eq!(got, want_flat);
    let rev = strings(cmd(&mut c, &["ZREVRANGE", "pz", "0", "9"]));
    let want_rev: Vec<String> = want.iter().rev().take(10).map(|(_, m)| m.clone()).collect();
    assert_eq!(rev, want_rev);
    for (rank, (s, m)) in want.iter().enumerate().step_by(37) {
        assert_eq!(
            resp_cmd(&mut c, &["ZRANK", "pz", m]),
            format!(":{rank}\r\n"),
            "{m}"
        );
        let V::Bulk(score) = cmd(&mut c, &["ZSCORE", "pz", m]) else {
            panic!("ZSCORE")
        };
        assert_eq!(String::from_utf8(score).unwrap(), s.to_string());
    }
    let by_score = strings(cmd(&mut c, &["ZRANGEBYSCORE", "pz", "-10", "10"]));
    let want_by: Vec<String> = want
        .iter()
        .filter(|(s, _)| (-10..=10).contains(s))
        .map(|(_, m)| m.clone())
        .collect();
    assert_eq!(by_score, want_by);
    assert_eq!(
        resp_cmd(&mut c, &["ZCOUNT", "pz", "-10", "10"]),
        format!(":{}\r\n", want_by.len())
    );

    // Draining empties and deletes the key.
    let n = want.len().to_string();
    let popped = strings(cmd(&mut c, &["ZPOPMIN", "pz", &n]));
    assert_eq!(popped.len(), want.len() * 2);
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "pz"]), ":0\r\n");

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_packed_small_hash_ops_and_promotion_e2e() {
    use std::collections::HashMap;
    let port = 17991u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Reads one RESP reply: bulk strings and arrays of them.
    fn read_reply(c: &mut TcpStream) -> Vec<u8> {
        fn complete(b: &[u8], i: usize) -> Option<usize> {
            let nl = i + b.get(i..)?.windows(2).position(|w| w == b"\r\n")?;
            let head = std::str::from_utf8(&b[i + 1..nl]).unwrap();
            match b[i] {
                b'$' => {
                    let n: i64 = head.parse().unwrap();
                    if n < 0 {
                        return Some(nl + 2);
                    }
                    let end = nl + 2 + n as usize + 2;
                    (b.len() >= end).then_some(end)
                }
                b'*' => {
                    let n: usize = head.parse().unwrap();
                    let mut j = nl + 2;
                    for _ in 0..n {
                        j = complete(b, j)?;
                    }
                    Some(j)
                }
                _ => Some(nl + 2),
            }
        }
        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            if complete(&got, 0) == Some(got.len()) && !got.is_empty() {
                return got;
            }
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
    }
    fn cmd(c: &mut TcpStream, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        c.write_all(out.as_bytes()).unwrap();
        String::from_utf8(read_reply(c)).unwrap()
    }
    fn bulk(s: &str) -> String {
        format!("${}\r\n{s}\r\n", s.len())
    }
    // HGETALL as a map.
    fn getall(c: &mut TcpStream, key: &str) -> HashMap<String, String> {
        let r = cmd(c, &["HGETALL", key]);
        let mut lines = r.split("\r\n");
        let n: usize = lines.next().unwrap()[1..].parse().unwrap();
        let mut items = Vec::new();
        for _ in 0..n {
            lines.next();
            items.push(lines.next().unwrap().to_string());
        }
        items
            .chunks(2)
            .map(|p| (p[0].clone(), p[1].clone()))
            .collect()
    }

    // Updates that change a value's length (including across the one-byte
    // length prefix boundary), deletes, and counters, against a model.
    let mut model: HashMap<String, String> = HashMap::new();
    for i in 0..40 {
        let f = format!("f{i}");
        let v = "x".repeat(i % 7);
        assert_eq!(cmd(&mut c, &["HSET", "sh", &f, &v]), ":1\r\n");
        model.insert(f, v);
    }
    for i in (0..40).step_by(3) {
        let f = format!("f{i}");
        let v = "y".repeat(60 - i);
        assert_eq!(cmd(&mut c, &["HSET", "sh", &f, &v]), ":0\r\n");
        model.insert(f, v);
    }
    for i in (0..40).step_by(5) {
        let f = format!("f{i}");
        assert_eq!(cmd(&mut c, &["HDEL", "sh", &f, "nope"]), ":1\r\n");
        model.remove(&f);
    }
    assert_eq!(cmd(&mut c, &["HSETNX", "sh", "f1", "z"]), ":0\r\n");
    assert_eq!(cmd(&mut c, &["HSETNX", "sh", "new", "z"]), ":1\r\n");
    model.insert("new".into(), "z".into());
    assert_eq!(cmd(&mut c, &["HINCRBY", "sh", "ctr", "41"]), ":41\r\n");
    assert_eq!(cmd(&mut c, &["HINCRBY", "sh", "ctr", "1"]), ":42\r\n");
    model.insert("ctr".into(), "42".into());
    assert_eq!(
        cmd(&mut c, &["HINCRBYFLOAT", "sh", "fl", "1.5"]),
        bulk("1.5")
    );
    model.insert("fl".into(), "1.5".into());
    assert_eq!(cmd(&mut c, &["OBJECT", "ENCODING", "sh"]), bulk("listpack"));
    assert_eq!(
        cmd(&mut c, &["HLEN", "sh"]),
        format!(":{}\r\n", model.len())
    );
    assert_eq!(getall(&mut c, "sh"), model);
    for (f, v) in &model {
        assert_eq!(cmd(&mut c, &["HGET", "sh", f]), bulk(v), "{f}");
        assert_eq!(
            cmd(&mut c, &["HSTRLEN", "sh", f]),
            format!(":{}\r\n", v.len())
        );
    }
    assert_eq!(cmd(&mut c, &["HGET", "sh", "f5"]), "$-1\r\n");
    assert_eq!(
        cmd(&mut c, &["HMGET", "sh", "f1", "f5", "ctr"]),
        format!("*3\r\n{}$-1\r\n{}", bulk(&model["f1"]), bulk("42"))
    );
    assert_eq!(cmd(&mut c, &["HEXISTS", "sh", "f2"]), ":1\r\n");

    // A value over hash-max-listpack-value (64) promotes to a hashtable and
    // keeps every field.
    let long = "L".repeat(65);
    assert_eq!(cmd(&mut c, &["HSET", "sh", "f1", &long]), ":0\r\n");
    model.insert("f1".into(), long);
    assert_eq!(
        cmd(&mut c, &["OBJECT", "ENCODING", "sh"]),
        bulk("hashtable")
    );
    assert_eq!(getall(&mut c, "sh"), model);

    // Past hash-max-listpack-entries (512) promotes too.
    let mut big: HashMap<String, String> = HashMap::new();
    for i in 0..512 {
        let (f, v) = (format!("k{i}"), format!("v{i}"));
        cmd(&mut c, &["HSET", "bh", &f, &v]);
        big.insert(f, v);
    }
    assert_eq!(cmd(&mut c, &["OBJECT", "ENCODING", "bh"]), bulk("listpack"));
    cmd(&mut c, &["HSET", "bh", "k512", "v512"]);
    big.insert("k512".into(), "v512".into());
    assert_eq!(
        cmd(&mut c, &["OBJECT", "ENCODING", "bh"]),
        bulk("hashtable")
    );
    assert_eq!(getall(&mut c, "bh"), big);

    // DUMP/RESTORE of a small hash round-trips.
    let mut small: HashMap<String, String> = HashMap::new();
    for i in 0..10 {
        let (f, v) = (format!("a{i}"), format!("b{i}"));
        cmd(&mut c, &["HSET", "dh", &f, &v]);
        small.insert(f, v);
    }
    let dump = {
        c.write_all(b"*2\r\n$4\r\nDUMP\r\n$2\r\ndh\r\n").unwrap();
        let r = read_reply(&mut c);
        let nl = r.windows(2).position(|w| w == b"\r\n").unwrap();
        r[nl + 2..r.len() - 2].to_vec()
    };
    let mut out = format!(
        "*4\r\n$7\r\nRESTORE\r\n$3\r\ndh2\r\n$1\r\n0\r\n${}\r\n",
        dump.len()
    )
    .into_bytes();
    out.extend_from_slice(&dump);
    out.extend_from_slice(b"\r\n");
    c.write_all(&out).unwrap();
    assert_eq!(String::from_utf8(read_reply(&mut c)).unwrap(), "+OK\r\n");
    assert_eq!(getall(&mut c, "dh2"), small);

    // Deleting every field deletes the key.
    for i in 0..10 {
        cmd(&mut c, &["HDEL", "dh", &format!("a{i}")]);
    }
    assert_eq!(cmd(&mut c, &["EXISTS", "dh"]), ":0\r\n");

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_packed_large_hash_compaction_and_ops_e2e() {
    use std::collections::HashMap;
    let port = 17992u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    fn read_reply(c: &mut TcpStream) -> Vec<u8> {
        fn complete(b: &[u8], i: usize) -> Option<usize> {
            let nl = i + b.get(i..)?.windows(2).position(|w| w == b"\r\n")?;
            let head = std::str::from_utf8(&b[i + 1..nl]).unwrap();
            match b[i] {
                b'$' => {
                    let n: i64 = head.parse().unwrap();
                    if n < 0 {
                        return Some(nl + 2);
                    }
                    let end = nl + 2 + n as usize + 2;
                    (b.len() >= end).then_some(end)
                }
                b'*' => {
                    let n: usize = head.parse().unwrap();
                    let mut j = nl + 2;
                    for _ in 0..n {
                        j = complete(b, j)?;
                    }
                    Some(j)
                }
                _ => Some(nl + 2),
            }
        }
        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            if !got.is_empty() && complete(&got, 0) == Some(got.len()) {
                return got;
            }
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
    }
    fn cmd(c: &mut TcpStream, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        c.write_all(out.as_bytes()).unwrap();
        String::from_utf8(read_reply(c)).unwrap()
    }
    fn pipeline(c: &mut TcpStream, cmds: &[Vec<String>]) {
        let mut out = String::new();
        for args in cmds {
            out.push_str(&format!("*{}\r\n", args.len()));
            for a in args {
                out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
            }
        }
        out.push_str("*1\r\n$4\r\nPING\r\n");
        c.write_all(out.as_bytes()).unwrap();
        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        while !got.ends_with(b"+PONG\r\n") {
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
    }
    fn bulk(s: &str) -> String {
        format!("${}\r\n{s}\r\n", s.len())
    }
    fn pairs_of(r: &str) -> Vec<String> {
        let mut lines = r.split("\r\n");
        let n: usize = lines.next().unwrap()[1..].parse().unwrap();
        (0..n)
            .map(|_| {
                lines.next();
                lines.next().unwrap().to_string()
            })
            .collect()
    }
    fn getall(c: &mut TcpStream, key: &str) -> HashMap<String, String> {
        pairs_of(&cmd(c, &["HGETALL", key]))
            .chunks(2)
            .map(|p| (p[0].clone(), p[1].clone()))
            .collect()
    }

    // 3000 fields with values over hash-max-listpack-value (a hashtable
    // from the first HSET), some over 1 KiB (a page of their own).
    let val = |i: usize, round: usize| {
        let len = if i.is_multiple_of(97) {
            1500
        } else {
            70 + (i + round) % 90
        };
        format!("{i}-{round}-").repeat(len / 4 + 1)[..len].to_string()
    };
    let mut model: HashMap<String, String> = HashMap::new();
    let mut cmds = Vec::new();
    for i in 0..3000 {
        let (f, v) = (format!("field:{i}"), val(i, 0));
        cmds.push(vec!["HSET".into(), "lh".into(), f.clone(), v.clone()]);
        model.insert(f, v);
    }
    pipeline(&mut c, &cmds);
    assert_eq!(
        cmd(&mut c, &["OBJECT", "ENCODING", "lh"]),
        bulk("hashtable")
    );
    // Updates: same length (in place) and different length (re-appended).
    let mut cmds = Vec::new();
    for i in (0..3000usize).step_by(4) {
        let f = format!("field:{i}");
        let v = if i.is_multiple_of(8) {
            "S".repeat(model[&f].len())
        } else {
            val(i, 1)
        };
        cmds.push(vec!["HSET".into(), "lh".into(), f.clone(), v.clone()]);
        model.insert(f, v);
    }
    pipeline(&mut c, &cmds);
    // Delete two of every three: pages pass half dead and compact.
    let mut cmds = Vec::new();
    for i in (0..3000).filter(|i| i % 3 != 0) {
        let f = format!("field:{i}");
        cmds.push(vec!["HDEL".into(), "lh".into(), f.clone()]);
        model.remove(&f);
    }
    pipeline(&mut c, &cmds);
    assert_eq!(cmd(&mut c, &["HINCRBY", "lh", "counter", "7"]), ":7\r\n");
    model.insert("counter".into(), "7".into());
    let gd = cmd(&mut c, &["HGETDEL", "lh", "FIELDS", "1", "field:3"]);
    assert_eq!(gd, format!("*1\r\n{}", bulk(&model["field:3"])));
    model.remove("field:3");

    assert_eq!(
        cmd(&mut c, &["HLEN", "lh"]),
        format!(":{}\r\n", model.len())
    );
    assert_eq!(getall(&mut c, "lh"), model);
    for i in [0usize, 6, 97, 194, 300, 2997, 1, 2] {
        let f = format!("field:{i}");
        let want = model.get(&f).map_or("$-1\r\n".to_string(), |v| bulk(v));
        assert_eq!(cmd(&mut c, &["HGET", "lh", &f]), want, "{f}");
    }
    // HSCAN sees every field.
    let mut seen: HashMap<String, String> = HashMap::new();
    let mut cursor = "0".to_string();
    loop {
        let r = cmd(&mut c, &["HSCAN", "lh", &cursor, "COUNT", "100"]);
        let rest = &r[r.find("\r\n").unwrap() + 2..];
        let (cur_line, after) = rest.split_once("\r\n").unwrap();
        assert!(cur_line.starts_with('$'));
        let (next, items) = after.split_once("\r\n").unwrap();
        for p in pairs_of(items).chunks(2) {
            seen.insert(p[0].clone(), p[1].clone());
        }
        cursor = next.to_string();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen, model);

    // DUMP/RESTORE of the large hash round-trips.
    c.write_all(b"*2\r\n$4\r\nDUMP\r\n$2\r\nlh\r\n").unwrap();
    let r = read_reply(&mut c);
    let nl = r.windows(2).position(|w| w == b"\r\n").unwrap();
    let dump = r[nl + 2..r.len() - 2].to_vec();
    let mut out = format!(
        "*4\r\n$7\r\nRESTORE\r\n$3\r\nlh2\r\n$1\r\n0\r\n${}\r\n",
        dump.len()
    )
    .into_bytes();
    out.extend_from_slice(&dump);
    out.extend_from_slice(b"\r\n");
    c.write_all(&out).unwrap();
    assert_eq!(String::from_utf8(read_reply(&mut c)).unwrap(), "+OK\r\n");
    assert_eq!(getall(&mut c, "lh2"), model);

    // Deleting every field deletes the key.
    let mut cmds = Vec::new();
    for f in model.keys() {
        cmds.push(vec!["HDEL".into(), "lh".into(), f.clone()]);
    }
    pipeline(&mut c, &cmds);
    assert_eq!(cmd(&mut c, &["EXISTS", "lh"]), ":0\r\n");

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_packed_list_medium_elements_and_queue_e2e() {
    use std::collections::VecDeque;
    let port = 17993u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    fn read_reply(c: &mut TcpStream) -> Vec<u8> {
        fn complete(b: &[u8], i: usize) -> Option<usize> {
            let nl = i + b.get(i..)?.windows(2).position(|w| w == b"\r\n")?;
            let head = std::str::from_utf8(&b[i + 1..nl]).unwrap();
            match b[i] {
                b'$' => {
                    let n: i64 = head.parse().unwrap();
                    if n < 0 {
                        return Some(nl + 2);
                    }
                    let end = nl + 2 + n as usize + 2;
                    (b.len() >= end).then_some(end)
                }
                b'*' => {
                    let n: usize = head.parse().unwrap();
                    let mut j = nl + 2;
                    for _ in 0..n {
                        j = complete(b, j)?;
                    }
                    Some(j)
                }
                _ => Some(nl + 2),
            }
        }
        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            if !got.is_empty() && complete(&got, 0) == Some(got.len()) {
                return got;
            }
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0);
            got.extend_from_slice(&buf[..n]);
        }
    }
    fn cmd(c: &mut TcpStream, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        c.write_all(out.as_bytes()).unwrap();
        String::from_utf8(read_reply(c)).unwrap()
    }
    fn bulk(s: &str) -> String {
        format!("${}\r\n{s}\r\n", s.len())
    }
    fn items(r: &str) -> Vec<String> {
        let mut lines = r.split("\r\n");
        let n: usize = lines.next().unwrap()[1..].parse().unwrap();
        (0..n)
            .map(|_| {
                lines.next();
                lines.next().unwrap().to_string()
            })
            .collect()
    }

    // Elements from short to 1 KiB+ (packed up to 1024 bytes, crossing the
    // two-byte length prefix at 128), pushed at both ends.
    let elem = |i: usize| {
        let len = match i % 7 {
            0 => 3,
            1 => 127,
            2 => 128,
            3 => 300,
            4 => 1024,
            5 => 1025,
            _ => 20,
        };
        format!("{i}:").repeat(len)[..len].to_string()
    };
    let mut model: VecDeque<String> = VecDeque::new();
    for i in 0..700 {
        let v = elem(i);
        if i % 3 == 0 {
            cmd(&mut c, &["LPUSH", "ml", &v]);
            model.push_front(v);
        } else {
            cmd(&mut c, &["RPUSH", "ml", &v]);
            model.push_back(v);
        }
    }
    let all = |c: &mut TcpStream| items(&cmd(c, &["LRANGE", "ml", "0", "-1"]));
    assert_eq!(all(&mut c), Vec::from(model.clone()));
    for i in [0usize, 1, 99, 350, 698, 699] {
        assert_eq!(
            cmd(&mut c, &["LINDEX", "ml", &i.to_string()]),
            bulk(&model[i]),
            "LINDEX {i}"
        );
    }
    // Mid-list edits with medium values, including into full chunks.
    for k in 0..40usize {
        let i = (k * 37) % model.len();
        let pivot = model[i].clone();
        let v = elem(k + 1000);
        let r = cmd(&mut c, &["LINSERT", "ml", "BEFORE", &pivot, &v]);
        assert_eq!(r, format!(":{}\r\n", model.len() + 1));
        let at = model.iter().position(|x| *x == pivot).unwrap();
        model.insert(at, v);
    }
    for k in 0..40usize {
        let i = (k * 53) % model.len();
        let v = elem(k + 2000);
        assert_eq!(cmd(&mut c, &["LSET", "ml", &i.to_string(), &v]), "+OK\r\n");
        model[i] = v;
    }
    assert_eq!(all(&mut c), Vec::from(model.clone()));
    assert_eq!(cmd(&mut c, &["LTRIM", "ml", "50", "-50"]), "+OK\r\n");
    let trimmed: Vec<String> = model
        .iter()
        .skip(50)
        .take(model.len() - 99)
        .cloned()
        .collect();
    assert_eq!(all(&mut c), trimmed);
    let rev = items(&cmd(&mut c, &["LRANGE", "ml", "-5", "-1"]));
    assert_eq!(rev, trimmed[trimmed.len() - 5..].to_vec());

    // A long-running queue: RPUSH at the back, LPOP at the front.
    let mut q: VecDeque<String> = VecDeque::new();
    for i in 0..20_000usize {
        let v = elem(i);
        cmd(&mut c, &["RPUSH", "q", &v]);
        q.push_back(v);
        if i >= 30 {
            assert_eq!(
                cmd(&mut c, &["LPOP", "q"]),
                bulk(&q.pop_front().unwrap()),
                "pop {i}"
            );
        }
    }
    assert_eq!(cmd(&mut c, &["LLEN", "q"]), format!(":{}\r\n", q.len()));
    assert_eq!(
        items(&cmd(&mut c, &["LRANGE", "q", "0", "-1"])),
        Vec::from(q)
    );

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

#[test]
fn test_slowlog_records_pipelined_commands_e2e() {
    let port = 17994u16;
    let port_s = port.to_string();
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "4",
            "--no-pin",
            "--aof",
            "false",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "slowlog-log-slower-than", "0"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "slowlog-max-len", "256"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["SLOWLOG", "RESET"]), "+OK\r\n");

    // One write carrying many commands of different kinds, on keys spread
    // over the shards: every one must be logged, not just the first or last.
    let cmds: Vec<Vec<String>> = (0..8)
        .flat_map(|i| {
            [
                vec!["SET".into(), format!("plk:s{i}"), "v".into()],
                vec!["GET".into(), format!("plk:s{i}")],
                vec!["INCR".into(), format!("plk:n{i}")],
                vec!["HSET".into(), format!("plk:h{i}"), "f".into(), "v".into()],
                vec!["LPUSH".into(), format!("plk:l{i}"), "x".into()],
                vec!["SADD".into(), format!("plk:z{i}"), "m".into()],
            ]
        })
        .collect();
    let mut out = String::new();
    for args in &cmds {
        out.push_str(&format!("*{}\r\n", args.len()));
        for a in args {
            out.push_str(&format!("${}\r\n{a}\r\n", a.len()));
        }
    }
    out.push_str("*1\r\n$4\r\nPING\r\n");
    c.write_all(out.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0);
        got.extend_from_slice(&buf[..n]);
    }

    let log = {
        c.write_all(b"*3\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n$3\r\n256\r\n")
            .unwrap();
        let mut got = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        // Read until the reply stops growing.
        c.set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        while std::time::Instant::now() < deadline {
            match c.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(_) if !got.is_empty() => break,
                Err(_) => {}
            }
        }
        String::from_utf8_lossy(&got).to_lowercase()
    };
    for args in &cmds {
        // Each logged entry carries the command name and key as bulk strings
        // (compared case-insensitively).
        let needle = format!(
            "${}\r\n{}\r\n${}\r\n{}\r\n",
            args[0].len(),
            args[0],
            args[1].len(),
            args[1]
        )
        .to_lowercase();
        assert!(
            log.contains(&needle),
            "pipelined `{} {}` missing from SLOWLOG GET",
            args[0],
            args[1]
        );
    }

    drop(c);
    shutdown_and_wait(port, &mut srv);
}

/// Remote shards hand pipelined batches back with their commands still in
/// them, and the connection empties each batch before reusing it. Many rounds
/// of mixed cross-shard pipelines on one connection must get exactly their own
/// replies: a pooled batch that kept old commands would re-run them (extra or
/// shifted replies, double INCRs).
#[test]
fn test_pipelined_cross_shard_batches_reuse_cleanly_e2e() {
    let port = 18004;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let bulk = |s: &str| format!("${}\r\n{}\r\n", s.len(), s);
    let cmd = |args: &[&str]| {
        let mut c = format!("*{}\r\n", args.len());
        for a in args {
            c.push_str(&bulk(a));
        }
        c
    };
    for round in 1..=30 {
        let mut req = String::new();
        let mut want = String::new();
        let keys: Vec<String> = (0..12).map(|i| format!("pbr_k{i}")).collect();
        for (i, k) in keys.iter().enumerate() {
            let v = format!("v{round}_{i}");
            req.push_str(&cmd(&["SET", k, &v]));
            want.push_str("+OK\r\n");
            req.push_str(&cmd(&["INCR", &format!("pbr_c{i}")]));
            want.push_str(&format!(":{round}\r\n"));
        }
        for (i, k) in keys.iter().enumerate() {
            req.push_str(&cmd(&["GET", k]));
            want.push_str(&bulk(&format!("v{round}_{i}")));
        }
        let mut mget: Vec<&str> = vec!["MGET"];
        mget.extend(keys.iter().map(|k| k.as_str()));
        mget.push("pbr_missing");
        req.push_str(&cmd(&mget));
        want.push_str(&format!("*{}\r\n", keys.len() + 1));
        for i in 0..keys.len() {
            want.push_str(&bulk(&format!("v{round}_{i}")));
        }
        want.push_str("$-1\r\n");
        req.push_str(&cmd(&["DEL", &keys[round % keys.len()]]));
        want.push_str(":1\r\n");
        req.push_str(&cmd(&["EXISTS", &keys[round % keys.len()]]));
        want.push_str(":0\r\n");

        client.write_all(req.as_bytes()).unwrap();
        let mut got = Vec::new();
        let mut buf = [0u8; 16384];
        while got.len() < want.len() {
            let n = client.read(&mut buf).unwrap();
            assert!(n > 0, "connection closed in round {round}");
            got.extend_from_slice(&buf[..n]);
        }
        assert_eq!(String::from_utf8_lossy(&got), want, "round {round}");
    }
    // Nothing left over: a PING gets exactly its own reply.
    assert_eq!(send_and_read(&mut client, b"PING\r\n"), "+PONG\r\n");
}

/// Pipelined GETs and MGETs whose values span the prefetch edge cases (empty,
/// one cache line either side, the 4 KB prefetch cap, large) come back byte
/// for byte, whether the key is local or on another shard.
#[test]
fn test_pipelined_reply_values_of_all_sizes_e2e() {
    let port = 18005;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let sizes = [0usize, 1, 21, 63, 64, 65, 1024, 4095, 4096, 4097, 70_000];
    let bulk = |v: &[u8]| {
        let mut b = format!("${}\r\n", v.len()).into_bytes();
        b.extend_from_slice(v);
        b.extend_from_slice(b"\r\n");
        b
    };
    let cmd = |args: &[&[u8]]| {
        let mut c = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            c.extend(bulk(a));
        }
        c
    };
    let keys: Vec<String> = (0..sizes.len()).map(|i| format!("pfv_{i}")).collect();
    let vals: Vec<Vec<u8>> = sizes
        .iter()
        .enumerate()
        .map(|(i, &n)| (0..n).map(|j| b'a' + ((i + j) % 26) as u8).collect())
        .collect();

    let mut req = Vec::new();
    let mut want = Vec::new();
    for (k, v) in keys.iter().zip(&vals) {
        req.extend(cmd(&[b"SET", k.as_bytes(), v]));
        want.extend_from_slice(b"+OK\r\n");
    }
    for _ in 0..3 {
        for (k, v) in keys.iter().zip(&vals) {
            req.extend(cmd(&[b"GET", k.as_bytes()]));
            want.extend(bulk(v));
        }
        let mut mget: Vec<&[u8]> = vec![b"MGET"];
        mget.extend(keys.iter().rev().map(|k| k.as_bytes()));
        req.extend(cmd(&mget));
        want.extend(format!("*{}\r\n", keys.len()).into_bytes());
        for v in vals.iter().rev() {
            want.extend(bulk(v));
        }
    }
    client.write_all(&req).unwrap();
    let mut got = Vec::new();
    let mut buf = vec![0u8; 65536];
    while got.len() < want.len() {
        let n = client.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    assert!(got == want, "pipelined replies differ");
    assert_eq!(send_and_read(&mut client, b"PING\r\n"), "+PONG\r\n");
}

/// Small hashes live in one exactly sized allocation that every write
/// reallocates. Drive one through growing, shrinking and same-size value
/// updates, field removal down to empty (key deleted), HINCRBY, and promotion
/// past the listpack limits, checking every reply.
#[test]
fn test_small_hash_single_allocation_round_trips_e2e() {
    let port = 18007;
    start_test_server(port, 4);
    let mut c = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut q = |args: &[&str]| -> String {
        let mut cmd = format!("*{}\r\n", args.len());
        for a in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        send_and_read(&mut c, cmd.as_bytes())
    };
    let bulk = |s: &str| format!("${}\r\n{}\r\n", s.len(), s);
    // Let a 200-byte value stay in the listpack so it needs a 2-byte prefix.
    assert_eq!(
        q(&["CONFIG", "SET", "hash-max-listpack-value", "1024"]),
        "+OK\r\n"
    );
    assert_eq!(
        q(&["HSET", "sh", "a", "1", "b", "22", "c", "333"]),
        ":3\r\n"
    );
    assert_eq!(q(&["OBJECT", "ENCODING", "sh"]), bulk("listpack"));
    // Grow a middle value past the 1-byte length prefix, then shrink it.
    let long = "x".repeat(200);
    assert_eq!(q(&["HSET", "sh", "b", &long]), ":0\r\n");
    assert_eq!(q(&["HGET", "sh", "b"]), bulk(&long));
    assert_eq!(q(&["HGET", "sh", "c"]), bulk("333"));
    assert_eq!(q(&["OBJECT", "ENCODING", "sh"]), bulk("listpack"));
    assert_eq!(q(&["HSET", "sh", "b", "y"]), ":0\r\n");
    assert_eq!(q(&["HSET", "sh", "b", "z"]), ":0\r\n"); // same size, in place
    assert_eq!(
        q(&["HGETALL", "sh"]),
        format!(
            "*6\r\n{}{}{}{}{}{}",
            bulk("a"),
            bulk("1"),
            bulk("b"),
            bulk("z"),
            bulk("c"),
            bulk("333")
        )
    );
    assert_eq!(q(&["HINCRBY", "sh", "a", "41"]), ":42\r\n");
    assert_eq!(q(&["HSTRLEN", "sh", "a"]), ":2\r\n");
    // Remove from the front, the back, then the last field.
    assert_eq!(q(&["HDEL", "sh", "a"]), ":1\r\n");
    assert_eq!(q(&["HDEL", "sh", "c"]), ":1\r\n");
    assert_eq!(q(&["HLEN", "sh"]), ":1\r\n");
    assert_eq!(q(&["HDEL", "sh", "b"]), ":1\r\n");
    assert_eq!(q(&["EXISTS", "sh"]), ":0\r\n");
    // Promotion past hash-max-listpack-entries keeps every pair.
    for i in 0..600 {
        let (f, v) = (format!("f{i}"), format!("v{i}"));
        assert_eq!(q(&["HSET", "big", &f, &v]), ":1\r\n");
    }
    assert_eq!(q(&["OBJECT", "ENCODING", "big"]), bulk("hashtable"));
    assert_eq!(q(&["HLEN", "big"]), ":600\r\n");
    assert_eq!(q(&["HGET", "big", "f0"]), bulk("v0"));
    assert_eq!(q(&["HGET", "big", "f599"]), bulk("v599"));
    // Copies are independent (each owns its allocation).
    assert_eq!(q(&["HSET", "src", "k", "v"]), ":1\r\n");
    assert_eq!(q(&["COPY", "src", "dst"]), ":1\r\n");
    assert_eq!(q(&["HSET", "src", "k", "changed"]), ":0\r\n");
    assert_eq!(q(&["HGET", "dst", "k"]), bulk("v"));
}

/// ASCII values of 25..=255 bytes may be stored 7 bits per byte. Every
/// string command must still see the original bytes: plain and pipelined
/// cross-shard reads, range reads and writes, APPEND, GETSET/GETDEL, DUMP and
/// RESTORE, and the edges around the packed range (24/25, 255/256 bytes,
/// non-ASCII).
#[test]
fn test_packed_heap_strings_read_back_exactly_e2e() {
    let port = 18008;
    start_test_server(port, 4);
    let mut c = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut q = |args: &[&[u8]]| -> Vec<u8> {
        let mut cmd = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            cmd.extend(format!("${}\r\n", a.len()).into_bytes());
            cmd.extend_from_slice(a);
            cmd.extend_from_slice(b"\r\n");
        }
        send_and_read_bytes(&mut c, &cmd)
    };
    let bulk = |v: &[u8]| {
        let mut b = format!("${}\r\n", v.len()).into_bytes();
        b.extend_from_slice(v);
        b.extend_from_slice(b"\r\n");
        b
    };
    let val = |n: usize, seed: usize| -> Vec<u8> {
        (0..n)
            .map(|i| b' ' + ((i * 31 + seed) % 95) as u8)
            .collect()
    };
    let mut keys = Vec::new();
    for (i, n) in [24usize, 25, 64, 97, 100, 128, 200, 255, 256]
        .into_iter()
        .enumerate()
    {
        let k = format!("ps:{n}").into_bytes();
        let v = val(n, i);
        assert_eq!(q(&[b"SET", &k, &v]), b"+OK\r\n");
        assert_eq!(q(&[b"GET", &k]), bulk(&v), "GET {n}");
        assert_eq!(q(&[b"STRLEN", &k]), format!(":{n}\r\n").into_bytes());
        keys.push((k, v));
    }
    // Non-ASCII stays exact too.
    let mut bin = val(100, 9);
    bin[50] = 0xC3;
    assert_eq!(q(&[b"SET", b"ps:bin", &bin]), b"+OK\r\n");
    assert_eq!(q(&[b"GET", b"ps:bin"]), bulk(&bin));
    // MGET across shards, one command and pipelined.
    let mut mget: Vec<&[u8]> = vec![b"MGET"];
    mget.extend(keys.iter().map(|(k, _)| k.as_slice()));
    let mut want = format!("*{}\r\n", keys.len()).into_bytes();
    for (_, v) in &keys {
        want.extend(bulk(v));
    }
    assert_eq!(q(&mget), want);
    // Range reads/writes and APPEND on a packed value.
    let (k100, v100) = (&keys[4].0, keys[4].1.clone());
    assert_eq!(q(&[b"GETRANGE", k100, b"10", b"19"]), bulk(&v100[10..20]));
    assert_eq!(q(&[b"SETRANGE", k100, b"5", b"HELLO"]), b":100\r\n");
    let mut v2 = v100.clone();
    v2[5..10].copy_from_slice(b"HELLO");
    assert_eq!(q(&[b"GET", k100]), bulk(&v2));
    assert_eq!(q(&[b"APPEND", k100, b"!!"]), b":102\r\n");
    v2.extend_from_slice(b"!!");
    assert_eq!(q(&[b"GET", k100]), bulk(&v2));
    // GETSET / GETDEL return the packed value.
    let (k200, v200) = (&keys[6].0, keys[6].1.clone());
    let new200 = val(200, 77);
    assert_eq!(q(&[b"GETSET", k200, &new200]), bulk(&v200));
    assert_eq!(q(&[b"GETDEL", k200]), bulk(&new200));
    assert_eq!(q(&[b"EXISTS", k200]), b":0\r\n");
    // DUMP / RESTORE round trip.
    let (k128, v128) = (&keys[5].0, keys[5].1.clone());
    let dump = q(&[b"DUMP", k128]);
    assert!(dump.starts_with(b"$"));
    let body_start = dump.iter().position(|&b| b == b'\n').unwrap() + 1;
    let payload = &dump[body_start..dump.len() - 2];
    assert_eq!(q(&[b"RESTORE", b"ps:restored", b"0", payload]), b"+OK\r\n");
    assert_eq!(q(&[b"GET", b"ps:restored"]), bulk(&v128));
}

/// Lists long enough to span many full chunks (4096 bytes each, header
/// included) read back exactly from both ends, by index and by range, and
/// after removals in the middle.
#[test]
fn test_long_lists_across_full_chunks_e2e() {
    let port = 18009;
    start_test_server(port, 4);
    let mut c = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut q = |args: &[String]| -> String {
        let mut cmd = format!("*{}\r\n", args.len());
        for a in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        send_and_read(&mut c, cmd.as_bytes())
    };
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let bulk = |x: &str| format!("${}\r\n{}\r\n", x.len(), x);
    // 3000 values pushed to the back in batches, 3000 to the front one by one.
    for batch in 0..30 {
        let mut args = s(&["RPUSH", "ll"]);
        args.extend((0..100).map(|i| format!("r{:05}", batch * 100 + i)));
        assert_eq!(q(&args), format!(":{}\r\n", (batch + 1) * 100));
    }
    for i in 0..3000 {
        assert_eq!(
            q(&s(&["LPUSH", "ll", &format!("l{i:05}")])),
            format!(":{}\r\n", 3001 + i)
        );
    }
    assert_eq!(q(&s(&["LLEN", "ll"])), ":6000\r\n");
    assert_eq!(q(&s(&["LINDEX", "ll", "0"])), bulk("l02999"));
    assert_eq!(q(&s(&["LINDEX", "ll", "2999"])), bulk("l00000"));
    assert_eq!(q(&s(&["LINDEX", "ll", "3000"])), bulk("r00000"));
    assert_eq!(q(&s(&["LINDEX", "ll", "-1"])), bulk("r02999"));
    assert_eq!(
        q(&s(&["LRANGE", "ll", "2998", "3001"])),
        format!(
            "*4\r\n{}{}{}{}",
            bulk("l00001"),
            bulk("l00000"),
            bulk("r00000"),
            bulk("r00001")
        )
    );
    // Remove from the middle, then pop both ends.
    assert_eq!(q(&s(&["LREM", "ll", "0", "r01500"])), ":1\r\n");
    assert_eq!(q(&s(&["LINDEX", "ll", "4500"])), bulk("r01501"));
    assert_eq!(q(&s(&["LPOP", "ll"])), bulk("l02999"));
    assert_eq!(q(&s(&["RPOP", "ll"])), bulk("r02999"));
    assert_eq!(q(&s(&["LLEN", "ll"])), ":5997\r\n");
}

/// The perf gate refuses a verdict on a contended host: its classifier flags
/// an A/B side whose spread stays above 8% CV, or a baseline build running
/// under 75% of the host's recorded live baseline, and passes quiet samples.
/// (`--self-test` exercises it on synthetic samples.)
#[test]
fn test_perf_gate_flags_noisy_or_slow_hosts_e2e() {
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let out = std::process::Command::new("python3")
        .arg(repo_root.join("scripts/check_perf_regression.py"))
        .arg("--self-test")
        .output()
        .expect("run check_perf_regression.py --self-test");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "self-test failed:\n{stdout}\n{stderr}"
    );
    assert!(stdout.contains("self-test OK"), "{stdout}");
    // The gate script itself is valid bash and wires in pipefail.
    let gate = std::fs::read_to_string(repo_root.join("scripts/gate.sh")).unwrap();
    assert!(gate.contains("set -euo pipefail"));
    let syntax = std::process::Command::new("bash")
        .arg("-n")
        .arg(repo_root.join("scripts/gate.sh"))
        .status()
        .unwrap();
    assert!(syntax.success());
}

/// With `client-output-buffer-limit normal` set, a client pipelining large
/// replies it never reads is disconnected near the limit (and counted).
/// Replies were checked only after a whole batch of up to 1024 commands, so
/// a 4 MB limit let one ~50 KB write build ~200 MB of replies first.
#[test]
fn test_normal_output_limit_stops_batch_near_limit_e2e() {
    let port = 18017u16;
    let ps = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &ps, "--threads", "1", "--no-pin"], port);
    let rss_kb = |pid: u32| -> u64 {
        std::fs::read_to_string(format!("/proc/{pid}/status"))
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))
            .unwrap()
            .trim()
            .trim_end_matches(" kB")
            .trim()
            .parse()
            .unwrap()
    };
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut req = String::from("*102\r\n$5\r\nRPUSH\r\n$3\r\nlst\r\n");
    let item = "y".repeat(1000);
    for _ in 0..100 {
        req.push_str(&format!("$1000\r\n{item}\r\n"));
    }
    assert_eq!(send_and_read_reply(&mut c, req.as_bytes()), ":100\r\n");
    assert_eq!(
        send_and_read_reply(
            &mut c,
            b"CONFIG SET client-output-buffer-limit \"normal 4mb 2mb 10\"\r\n"
        ),
        "+OK\r\n"
    );
    let base = rss_kb(srv.id());
    let bad = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let sock = socket2::SockRef::from(&bad);
    let _ = sock.set_recv_buffer_size(4096);
    // 2000 LRANGEs (~200 MB of replies) in one write, never read.
    let one = "*4\r\n$6\r\nLRANGE\r\n$3\r\nlst\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    (&bad).write_all(one.repeat(2000).as_bytes()).unwrap();
    let mut peak = base;
    for _ in 0..40 {
        peak = peak.max(rss_kb(srv.id()));
        thread::sleep(Duration::from_millis(50));
    }
    let list = send_and_read_reply(&mut c, b"CLIENT LIST\r\n");
    assert!(
        !list.contains("cmd=lrange"),
        "client not disconnected: {list}"
    );
    let stats = send_and_read_reply(&mut c, b"INFO stats\r\n");
    assert!(
        stats.contains("client_output_buffer_limit_disconnections:1"),
        "{stats}"
    );
    assert!(
        peak - base < 40 * 1024,
        "RSS grew {} MB for a 4 MB limit",
        (peak - base) / 1024
    );
    let _ = srv.kill();
    let _ = srv.wait();
}

/// Under `appendfsync everysec`, killing the process (not the OS) loses no
/// acknowledged write: like Redis, replies wait until the AOF bytes are
/// written to the kernel. They were buffered for a flusher running every
/// 5 ms, so kill -9 right after a reply lost the last acknowledged writes.
#[test]
fn test_everysec_kill9_loses_no_acknowledged_write_e2e() {
    let port = 18018u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-aofwt-{}", std::process::id()));
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
        "everysec",
    ];
    for round in 0..3 {
        let mut srv = spawn_rudis_listening(&args, port);
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        // Batches of 200 SETs over all shards; after each full reply the
        // batch counts as acknowledged.
        let mut acked = 0usize;
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_millis(300 + 200 * round) {
            let mut req = String::new();
            for i in acked..acked + 200 {
                let k = format!("r{round}:k{i}");
                let v = i.to_string();
                req.push_str(&format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n${}\r\n{v}\r\n",
                    k.len(),
                    v.len()
                ));
            }
            c.write_all(req.as_bytes()).unwrap();
            let (mut oks, mut buf) = (0, [0u8; 8192]);
            while oks < 200 {
                let n = c.read(&mut buf).unwrap();
                assert!(n > 0);
                oks += buf[..n].windows(3).filter(|w| w == b"OK\r").count();
            }
            acked += 200;
        }
        // kill -9 right after the last acknowledgement.
        let _ = srv.kill();
        let _ = srv.wait();
        let mut srv = spawn_rudis_listening(&args, port);
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut missing = Vec::new();
        for i in (0..acked).rev().take(400).chain([0, acked / 2]) {
            let k = format!("r{round}:k{i}");
            let r = send_and_read_reply(&mut c, format!("GET {k}\r\n").as_bytes());
            if r != format!("${}\r\n{i}\r\n", i.to_string().len()) {
                missing.push(i);
            }
        }
        assert!(
            missing.is_empty(),
            "round {round}: {} of {acked} acknowledged writes lost, e.g. {:?}",
            missing.len(),
            &missing[..missing.len().min(5)]
        );
        let _ = srv.kill();
        let _ = srv.wait();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// An expired hash field is logged as HDEL (AOF and replicas): the replica
/// and a restart from the AOF hold the same hash as the master. Replicas
/// and replays expired fields by their own clock instead.
#[test]
fn test_hash_field_expiry_is_logged_as_hdel_e2e() {
    let (mp, rp) = (18037u16, 18038u16);
    let (mps, rps) = (mp.to_string(), rp.to_string());
    let dir = std::env::temp_dir().join(format!("rudis-e2e-hdel-{}", std::process::id()));
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
    // The master keeps field f past its deadline (as if its expiry had not
    // run yet) and HPERSISTs it; field e it lets expire.
    for cmd in [
        &b"DEBUG SET-ACTIVE-EXPIRE 0\r\n"[..],
        b"DEBUG SET-ALLOW-ACCESS-EXPIRED 1\r\n",
    ] {
        assert_eq!(send_and_read_reply(&mut m, cmd), "+OK\r\n");
    }
    assert_eq!(send_and_read_reply(&mut m, b"HSET h f v e x\r\n"), ":2\r\n");
    assert_eq!(
        send_and_read_reply(&mut m, b"HPEXPIRE h 100 FIELDS 2 f e\r\n"),
        "*2\r\n:1\r\n:1\r\n"
    );
    thread::sleep(Duration::from_millis(400));
    assert_eq!(
        send_and_read_reply(&mut m, b"HPERSIST h FIELDS 1 f\r\n"),
        "*1\r\n:1\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut m, b"DEBUG SET-ALLOW-ACCESS-EXPIRED 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read_reply(&mut m, b"HLEN h\r\n"), ":1\r\n");
    assert_eq!(send_and_read_reply(&mut m, b"WAIT 1 2000\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read_reply(&mut r, b"HGETALL h\r\n"),
        "*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    drop(replica);
    drop(master);
    let master = ServerGuard(spawn_rudis_listening(&margs, mp));
    let mut m = TcpStream::connect(("127.0.0.1", mp)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut m, b"HGETALL h\r\n"),
        "*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    drop(master);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A server starting on a directory with the temp file of a rewrite that
/// a crash cut short removes it, and loads its AOF as usual.
#[test]
fn test_startup_removes_interrupted_rewrite_temp_e2e() {
    let port = 18048u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-staletmp-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        "1",
        "--no-pin",
        "--dir",
        &ds,
        "--appendonly",
        "yes",
    ];
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(send_and_read_reply(&mut c, b"SET k v\r\n"), "+OK\r\n");
    drop(srv);
    let stale = dir.join("appendonly-0.aof.tmp.999999_live7");
    std::fs::write(&stale, vec![b'x'; 4096]).unwrap();
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(send_and_read_reply(&mut c, b"GET k\r\n"), "$1\r\nv\r\n");
    assert!(!stale.exists(), "stale rewrite temp file removed");
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Plain SETs count no keyspace hits or misses (INFO stats), as in Redis;
/// with the AOF on they were counted as a GET's.
#[test]
fn test_plain_set_counts_no_keyspace_hits_or_misses_e2e() {
    let port = 18050u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-sethits-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let srv = ServerGuard(spawn_rudis_listening(
        &[
            "--port",
            &ps,
            "--threads",
            "1",
            "--no-pin",
            "--dir",
            &ds,
            "--appendonly",
            "yes",
        ],
        port,
    ));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut c, b"CONFIG RESETSTAT\r\n"),
        "+OK\r\n"
    );
    for cmd in [&b"SET a 1\r\n"[..], b"SET a 2\r\n", b"SET b 3\r\n"] {
        assert_eq!(send_and_read_reply(&mut c, cmd), "+OK\r\n");
    }
    let stats = send_and_read_reply(&mut c, b"INFO stats\r\n");
    assert!(stats.contains("keyspace_hits:0\r\n"), "{stats}");
    assert!(stats.contains("keyspace_misses:0\r\n"), "{stats}");
    assert_eq!(send_and_read_reply(&mut c, b"GET a\r\n"), "$1\r\n2\r\n");
    let stats = send_and_read_reply(&mut c, b"INFO stats\r\n");
    assert!(stats.contains("keyspace_hits:1\r\n"), "{stats}");
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// ASCII values of every length the server stores 7-bit packed come back
/// byte for byte (GET, GETRANGE, STRLEN).
#[test]
fn test_packed_string_values_of_every_length_round_trip_e2e() {
    let port = 18051u16;
    let ps = port.to_string();
    let srv = ServerGuard(spawn_rudis_listening(
        &["--port", &ps, "--threads", "2", "--no-pin"],
        port,
    ));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for n in 20..=300usize {
        let v: String = (0..n)
            .map(|i| (b'!' + ((i * 7 + n) % 94) as u8) as char)
            .collect();
        let set = format!("*3\r\n$3\r\nSET\r\n$4\r\nk{n:03}\r\n${n}\r\n{v}\r\n");
        assert_eq!(send_and_read_reply(&mut c, set.as_bytes()), "+OK\r\n");
        let get = format!("GET k{n:03}\r\n");
        assert_eq!(
            send_and_read_reply(&mut c, get.as_bytes()),
            format!("${n}\r\n{v}\r\n"),
            "len {n}"
        );
        let tail = format!("GETRANGE k{n:03} -9 -1\r\n");
        assert_eq!(
            send_and_read_reply(&mut c, tail.as_bytes()),
            format!("$9\r\n{}\r\n", &v[n - 9..]),
            "len {n}"
        );
    }
    drop(srv);
}

/// Pipelined GETs and MGETs of packed (25-255 byte ASCII) values across
/// shards, many more than one reply slab holds, all come back intact.
#[test]
fn test_pipelined_gets_of_packed_values_across_shards_e2e() {
    let port = 18052u16;
    let ps = port.to_string();
    let srv = ServerGuard(spawn_rudis_listening(
        &["--port", &ps, "--threads", "4", "--no-pin"],
        port,
    ));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let value = |i: usize| -> String {
        let n = 30 + i % 200;
        (0..n)
            .map(|j| (b'A' + ((i * 7 + j) % 26) as u8) as char)
            .collect()
    };
    let keys = 3000usize;
    let mut set = Vec::new();
    for i in 0..keys {
        let v = value(i);
        set.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\npk:{i}\r\n${}\r\n{v}\r\n",
                format!("pk:{i}").len(),
                v.len()
            )
            .as_bytes(),
        );
    }
    c.write_all(&set).unwrap();
    let mut want = String::new();
    for _ in 0..keys {
        want.push_str("+OK\r\n");
    }
    let mut got = vec![0u8; want.len()];
    c.read_exact(&mut got).unwrap();
    assert_eq!(String::from_utf8_lossy(&got), want);
    // GETs, then MGETs of 5 keys, all in one pipeline.
    let mut req = String::new();
    let mut want = String::new();
    for i in 0..keys {
        req.push_str(&format!("GET pk:{i}\r\n"));
        let v = value(i);
        want.push_str(&format!("${}\r\n{v}\r\n", v.len()));
    }
    for i in (0..keys).step_by(5) {
        req.push_str("MGET");
        want.push_str("*5\r\n");
        for k in i..i + 5 {
            req.push_str(&format!(" pk:{k}"));
            let v = value(k);
            want.push_str(&format!("${}\r\n{v}\r\n", v.len()));
        }
        req.push_str("\r\n");
    }
    c.write_all(req.as_bytes()).unwrap();
    let mut got = vec![0u8; want.len()];
    c.read_exact(&mut got).unwrap();
    assert!(got == want.as_bytes(), "pipelined replies differ");
    drop(srv);
}

/// Commands stay fast while a live AOF rewrite walks spilled values with
/// clients blocked. The rewrite's step loop always had itself ready to
/// run, and the runtime only processed timers when it parked, so every
/// timer on a rewriting shard waited for its whole rewrite. A write's reply
/// waits on one (the AOF write-through fallback): the first write to each
/// shard took ~200 ms.
#[test]
fn test_timers_fire_during_live_rewrite_e2e() {
    let port = 18057u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-rwtimer-{}", std::process::id()));
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
        "--enable-debug-command",
        "yes",
    ];
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        c
    };
    let mut c = connect();
    let mut pipe = String::new();
    for i in 0..30000 {
        pipe.push_str(&format!("SET s{i} {}{i}\r\n", "v".repeat(200)));
        if i % 3 == 0 {
            pipe.push_str(&format!("RPUSH l{} {}{i}\r\n", i % 500, "x".repeat(120)));
            pipe.push_str(&format!("HSET h{} f{i} {}\r\n", i % 300, "y".repeat(150)));
        }
    }
    // Drain the replies up to a trailing PING's.
    pipe.push_str("PING\r\n");
    c.write_all(pipe.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !got.ends_with(b"+PONG\r\n") {
        let r = c.read(&mut buf).unwrap();
        assert!(r > 0, "connection closed");
        got.extend_from_slice(&buf[..r]);
    }
    assert!(!got.windows(4).any(|w| w == b"\r\n-E"), "an error reply");
    assert!(send_and_read_reply(&mut c, b"TIER SPILLALL\r\n").starts_with(':'));
    let mut blocked: Vec<TcpStream> = (0..20)
        .map(|k| {
            let mut b = connect();
            b.write_all(format!("BLMOVE src{k} l{k} LEFT RIGHT 0\r\n").as_bytes())
                .unwrap();
            b
        })
        .collect();
    thread::sleep(Duration::from_millis(200));
    assert!(send_and_read_reply(&mut c, b"BGREWRITEAOF\r\n").starts_with('+'));
    let mut worst = Duration::ZERO;
    for i in 0..100 {
        let t = std::time::Instant::now();
        send_and_read_reply(&mut c, format!("LPUSH l{} new{i}\r\n", i % 500).as_bytes());
        worst = worst.max(t.elapsed());
    }
    eprintln!("worst LPUSH during the rewrite: {worst:?}");
    for (k, b) in blocked.iter_mut().enumerate() {
        send_and_read_reply(&mut c, format!("LPUSH src{k} m{k}\r\n").as_bytes());
        assert_eq!(
            read_full_reply(b),
            format!("${}\r\nm{k}\r\n", 1 + k.to_string().len())
        );
    }
    assert!(worst < Duration::from_millis(80), "an LPUSH took {worst:?}");
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONFIG SET proto-max-bulk-len takes memory units, like Redis.
#[test]
fn test_config_set_proto_max_bulk_len_with_units_e2e() {
    let port = 18303u16;
    let _srv = ServerGuard(spawn_rudis_listening(
        &["--port", &port.to_string(), "--threads", "1", "--no-pin"],
        port,
    ));
    let mut c = connect_ready(port);
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "proto-max-bulk-len", "512mb"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "proto-max-bulk-len"]),
        "*2\r\n$18\r\nproto-max-bulk-len\r\n$9\r\n536870912\r\n"
    );
}

/// A shard thread that dies takes the process down (exit 1), so a
/// supervisor restarts it, instead of the other shards serving on without
/// it. Here shard 1 panics as it starts (a fault-injection hook).
#[test]
fn test_dead_shard_thread_exits_the_process_e2e() {
    let port = 19731;
    let mut child = rudis_bin()
        .args(["--port", &port.to_string(), "--threads", "2", "--no-pin"])
        .env("RUDIS_FAILPOINT_SHARD_PANIC", "1")
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rudis");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("rudis kept running with a dead shard");
        }
        thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert_eq!(status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("FATAL: shard 1 panicked"), "{stderr}");
}

/// `--version` and `INFO server` name the version and the commit the
/// binary was built from.
#[test]
fn test_version_and_info_report_the_build_commit_e2e() {
    let out = rudis_bin()
        .arg("--version")
        .output()
        .expect("rudis --version");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        text.trim(),
        format!("rudis {}", rudis::version::long_version()),
        "{text}"
    );

    let port = 29761;
    start_test_server(port, 2);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let info = resp_cmd_full(&mut c, &["INFO", "server"]);
    assert!(
        info.contains(&format!("redis_git_sha1:{}\r\n", rudis::version::GIT_SHA)),
        "{info}"
    );
    assert!(
        info.contains(&format!("rudis_version:{}\r\n", rudis::version::VERSION)),
        "{info}"
    );
}

/// A cross-shard MGET never sees a cross-shard MSET or MULTI/EXEC half
/// applied, as in Redis: with 8 keys spread over 4 shards and a writer
/// setting all of them to one value at a time, every read (alone or in a
/// pipeline of MGETs) finds the keys equal.
#[test]
fn test_cross_shard_mget_sees_mset_and_exec_whole_e2e() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let port = 29801;
    start_test_server(port, 4);
    let keys: Vec<String> = (0..8).map(|i| format!("atomic:{i}")).collect();
    let shards: std::collections::HashSet<usize> =
        keys.iter().map(|k| target_shard(k.as_bytes(), 4)).collect();
    assert!(shards.len() > 1, "keys must span shards");
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c
    };
    let mut init = vec!["MSET".to_string()];
    for k in &keys {
        init.push(k.clone());
        init.push("0".to_string());
    }
    let init: Vec<&str> = init.iter().map(String::as_str).collect();
    assert_eq!(resp_cmd_full(&mut connect(), &init), "+OK\r\n");

    let mget: Vec<&str> = std::iter::once("MGET")
        .chain(keys.iter().map(String::as_str))
        .collect();
    let mut mget_frame = format!("*{}\r\n", mget.len());
    for a in &mget {
        mget_frame.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    let values = |reply: &str| -> Vec<String> {
        reply
            .split("\r\n")
            .skip(1)
            .filter(|l| !l.starts_with('$') && !l.is_empty())
            .map(str::to_string)
            .collect()
    };
    for use_exec in [false, true] {
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (stop, keys) = (stop.clone(), keys.clone());
            thread::spawn(move || {
                let mut w = TcpStream::connect(("127.0.0.1", port)).unwrap();
                w.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    let v = i.to_string();
                    let mut frame = Vec::new();
                    if use_exec {
                        frame.extend_from_slice(b"*1\r\n$5\r\nMULTI\r\n");
                        for k in &keys {
                            frame.extend_from_slice(
                                format!(
                                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                    k.len(),
                                    k,
                                    v.len(),
                                    v
                                )
                                .as_bytes(),
                            );
                        }
                        frame.extend_from_slice(b"*1\r\n$4\r\nEXEC\r\n");
                        let r = send_and_read_reply_n(&mut w, &frame, keys.len() + 2);
                        assert!(r.ends_with("+OK\r\n"), "{r}");
                    } else {
                        let mut args = vec!["MSET".to_string()];
                        for k in &keys {
                            args.push(k.clone());
                            args.push(v.clone());
                        }
                        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
                        assert_eq!(resp_cmd_full(&mut w, &refs), "+OK\r\n");
                    }
                }
            })
        };
        let (mut reader, mut pipelined) = (connect(), connect());
        let (mut reads, mut torn) = (0, Vec::new());
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            let mut replies = vec![resp_cmd_full(&mut reader, &mget)];
            // Four MGETs in one pipeline (folded into per-shard batches).
            let batch = send_and_read_reply_n(&mut pipelined, mget_frame.repeat(4).as_bytes(), 4);
            replies.extend(
                batch
                    .split("*8\r\n")
                    .filter(|r| !r.is_empty())
                    .map(|r| format!("*8\r\n{r}")),
            );
            for r in replies {
                let vals = values(&r);
                assert_eq!(vals.len(), keys.len(), "{r}");
                reads += 1;
                if vals.windows(2).any(|w| w[0] != w[1]) {
                    torn.push(vals);
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        let what = if use_exec { "EXEC" } else { "MSET" };
        assert!(reads > 100, "only {reads} reads against {what}");
        assert!(
            torn.is_empty(),
            "{} of {reads} reads torn against {what}, e.g. {:?}",
            torn.len(),
            torn.first()
        );
    }
}

/// DEBUG OBJECT shows a key as stored, like Redis: an expired key that is
/// not deleted yet (active expiry off) is still described, while reads
/// treat it as gone.
#[test]
fn test_debug_object_shows_expired_but_present_key_e2e() {
    let port = 29841;
    start_test_server(port, 1);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "0"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["SET", "k", "1", "PX", "20"]), "+OK\r\n");
    thread::sleep(Duration::from_millis(60));
    let obj = resp_cmd(&mut c, &["DEBUG", "OBJECT", "k"]);
    assert!(obj.starts_with("+Value at:"), "{obj}");
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "k"]), ":0\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["DEBUG", "SET-ACTIVE-EXPIRE", "1"]),
        "+OK\r\n"
    );
}

/// `repl-timeout`: a replica whose master goes silent without closing the
/// connection (SIGSTOP here; a dead host or a partition in production)
/// marks the link down and reconnects instead of reporting it up forever,
/// and a master drops a replica that stopped acking, as Redis does. The
/// master's PINGs keep an idle link alive.
#[test]
fn test_repl_timeout_drops_silent_links_e2e() {
    let (mport, rport) = (29881u16, 29882u16);
    let spawn = |port: u16| {
        let p = port.to_string();
        spawn_rudis_listening(
            &["--port", &p, "--threads", "2", "--no-pin", "--save", ""],
            port,
        )
    };
    let mut master = spawn(mport);
    let mut replica = spawn(rport);
    let connect = |port: u16| {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    };
    let (mut m, mut r) = (connect(mport), connect(rport));
    for (port, name, val) in [
        (mport, "repl-ping-replica-period", "1"),
        (mport, "repl-timeout", "3"),
        (rport, "repl-timeout", "3"),
    ] {
        let c = if port == mport { &mut m } else { &mut r };
        assert_eq!(resp_cmd(c, &["CONFIG", "SET", name, val]), "+OK\r\n");
    }
    assert_eq!(
        resp_cmd(&mut r, &["CONFIG", "GET", "repl-timeout"]),
        "*2\r\n$12\r\nrepl-timeout\r\n$1\r\n3\r\n"
    );
    let r0 = resp_cmd(&mut r, &["CONFIG", "SET", "repl-timeout", "0"]);
    assert!(r0.starts_with("-ERR"), "{r0}");
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    wait_for_master_link(&mut r);
    // Idle for longer than the timeout: the PINGs keep the link up.
    thread::sleep(Duration::from_secs(4));
    let info = resp_cmd_full(&mut r, &["INFO", "replication"]);
    assert!(info.contains("master_link_status:up"), "{info}");
    let info = resp_cmd_full(&mut m, &["INFO", "replication"]);
    assert!(info.contains("connected_slaves:1"), "{info}");

    let wait_for = |c: &mut TcpStream, what: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let info = resp_cmd_full(c, &["INFO", "replication"]);
            if info.contains(what) {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "no {what}: {info}");
            thread::sleep(Duration::from_millis(100));
        }
    };
    // The master goes silent: the replica gives up on it.
    // SAFETY: kill(2) on our own live child.
    unsafe { libc::kill(master.id() as libc::pid_t, libc::SIGSTOP) };
    wait_for(&mut r, "master_link_status:down");
    // SAFETY: as above.
    unsafe { libc::kill(master.id() as libc::pid_t, libc::SIGCONT) };
    wait_for_master_link(&mut r);

    // The replica goes silent: the master drops it.
    let mut m = connect(mport);
    wait_for(&mut m, "connected_slaves:1");
    // SAFETY: as above.
    unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGSTOP) };
    wait_for(&mut m, "connected_slaves:0");
    // SAFETY: as above.
    unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGCONT) };
    wait_for(&mut m, "connected_slaves:1");

    for c in [&mut master, &mut replica] {
        let _ = c.kill();
        let _ = c.wait();
    }
}

/// CONFIG SET appendonly yes on a running server, as in Redis: writes are
/// logged at once and a background rewrite writes the initial AOF, so a
/// restart with AOF on (even after kill -9) has every key, from before and
/// after the switch. While it runs SHUTDOWN refuses to exit. appendonly no
/// turns logging off again, and WAITAOF's local copy is then refused.
#[test]
fn test_config_set_appendonly_at_runtime_e2e() {
    let port = 29931u16;
    let dir = std::env::temp_dir().join(format!("rudis-aofrt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let d = dir.to_str().unwrap().to_string();
    let conf = dir.join("rudis.conf");
    std::fs::write(&conf, "appendonly no\n").unwrap();
    let cf = conf.to_str().unwrap().to_string();
    let p = port.to_string();
    let base = [
        "--config",
        &cf,
        "--port",
        &p,
        "--threads",
        "2",
        "--no-pin",
        "--save",
        "",
        "--dir",
        &d,
    ];
    let mut server = spawn_rudis_listening(&base, port);
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s
    };
    let mut c = connect();
    let info_field = |c: &mut TcpStream, name: &str| -> String {
        let info = resp_cmd_full(c, &["INFO", "persistence"]);
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}:")).map(str::to_string))
            .unwrap_or_else(|| panic!("no {name}: {info}"))
    };
    assert_eq!(resp_cmd(&mut c, &["DEBUG", "POPULATE", "20000"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["HSET", "h", "f", "v"]), ":1\r\n");
    assert_eq!(info_field(&mut c, "aof_enabled"), "0");
    let r = resp_cmd(&mut c, &["CONFIG", "SET", "appendonly", "maybe"]);
    assert!(r.contains("argument must be 'yes' or 'no'"), "{r}");

    // A slow initial rewrite, so SHUTDOWN comes while it runs.
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "rdb-key-save-delay", "50000"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "appendonly", "yes"]),
        "+OK\r\n"
    );
    assert_eq!(info_field(&mut c, "aof_enabled"), "1");
    // The initial rewrite starts right after the reply.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while info_field(&mut c, "aof_rewrite_in_progress") != "1" {
        assert!(std::time::Instant::now() < deadline, "no initial rewrite");
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        resp_cmd(&mut c, &["SHUTDOWN", "NOSAVE"]),
        "-ERR Errors trying to SHUTDOWN. Check logs.\r\n"
    );
    // Written during the rewrite: reaches the AOF through its tail.
    assert_eq!(resp_cmd(&mut c, &["SET", "during", "1"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "rdb-key-save-delay", "0"]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while info_field(&mut c, "aof_rewrite_in_progress") != "0" {
        assert!(
            std::time::Instant::now() < deadline,
            "initial rewrite never ended"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(info_field(&mut c, "aof_last_bgrewrite_status"), "ok");
    assert_eq!(resp_cmd(&mut c, &["SET", "after", "2"]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "appendonly"]),
        "*2\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["WAITAOF", "1", "0", "5000"]),
        "*2\r\n:1\r\n:0\r\n"
    );
    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".incr") || n.contains(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");

    // CONFIG REWRITE keeps it on; kill -9 and restart: everything is there.
    assert_eq!(resp_cmd(&mut c, &["CONFIG", "REWRITE"]), "+OK\r\n");
    let written = std::fs::read_to_string(&conf).unwrap();
    assert!(written.lines().any(|l| l == "appendonly yes"), "{written}");
    let _ = server.kill();
    let _ = server.wait();
    let mut server = spawn_rudis_listening(&base, port);
    let mut c = connect();
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":20003\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "during"]), "$1\r\n1\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "after"]), "$1\r\n2\r\n");
    assert_eq!(resp_cmd(&mut c, &["HGET", "h", "f"]), "$1\r\nv\r\n");

    // Off again: WAITAOF's local copy is refused, also for one waiting.
    let mut w = connect();
    assert_eq!(
        resp_cmd(&mut w, &["CONFIG", "SET", "appendfsync", "no"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut w, &["INCR", "n"]), ":1\r\n");
    w.write_all(b"*4\r\n$7\r\nWAITAOF\r\n$1\r\n1\r\n$1\r\n0\r\n$1\r\n0\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "appendonly", "no"]),
        "+OK\r\n"
    );
    let mut buf = [0u8; 256];
    let n = w.read(&mut buf).unwrap();
    assert!(
        String::from_utf8_lossy(&buf[..n]).contains("appendonly is disabled"),
        "{}",
        String::from_utf8_lossy(&buf[..n])
    );
    assert_eq!(info_field(&mut c, "aof_enabled"), "0");
    let r = resp_cmd(&mut c, &["WAITAOF", "1", "0", "0"]);
    assert!(r.contains("appendonly is disabled"), "{r}");

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
