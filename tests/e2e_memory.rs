//! End-to-end tests: memory (see `tests/common` for the helpers).

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

    // Set maxmemory a little above current usage. Tiered keys still keep their
    // key and pointer in RAM, so leave room for that metadata while the values
    // (100 x 8000 bytes) far exceed the headroom and must be spilled to NVMe.
    // `used_memory` is the allocator's count, which also moves by tens of KB
    // with connection buffers and replies, so the headroom covers that too.
    let limit = cur_used + 256 * 1024;
    assert_eq!(
        send_and_read(
            &mut client,
            format!("CONFIG SET maxmemory {}\r\n", limit).as_bytes()
        ),
        "+OK\r\n"
    );

    // Insert multiple keys that will exceed the threshold
    for i in 0..100 {
        let val = "Z".repeat(8000);
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
    for i in 0..100 {
        let expected = "Z".repeat(8000);
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
fn test_tls_in_memory_cert_e2e() {
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

    // Set maxmemory just above current usage and verify keys can still be
    // set under allkeys-lru (eviction succeeds). `used_memory` is the
    // allocator's count, which includes the server's fixed buffers, so an
    // absolute limit like 1mb would be below an empty server.
    let info = send_and_read(&mut client1, b"INFO memory\r\n");
    let used: u64 = info
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        send_and_read(
            &mut client1,
            format!("CONFIG SET maxmemory {}\r\n", used + (1 << 20)).as_bytes()
        ),
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
fn test_agent_memory_working_window_episodic_recall_and_compaction_e2e() {
    let port = 19152;
    start_test_server(port, 4);
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 1. Add 4 turns to session "agent:sess1" with token counts and 3D embeddings
    let add1 = format_resp_cmd(&[
        "AGENT.MEM.ADD",
        "agent:sess1",
        "user",
        "My prod cluster uses 8 shards on NVMe.",
        "TOKENS",
        "10",
        "VEC",
        "3",
        "1.0",
        "0.0",
        "0.0",
        "META",
        r#"{"turn":1}"#,
    ]);
    assert_eq!(send_and_read(&mut client, &add1), ":1\r\n");

    let add2 = format_resp_cmd(&[
        "AGENT.MEM.ADD",
        "agent:sess1",
        "assistant",
        "Noted: 8 shards with NVMe tiering.",
        "TOKENS",
        "10",
        "VEC",
        "3",
        "0.9",
        "0.1",
        "0.0",
    ]);
    assert_eq!(send_and_read(&mut client, &add2), ":2\r\n");

    let add3 = format_resp_cmd(&[
        "AGENT.MEM.ADD",
        "agent:sess1",
        "user",
        "How do I enable TLS on port 6380?",
        "TOKENS",
        "10",
        "VEC",
        "3",
        "0.0",
        "1.0",
        "0.0",
    ]);
    assert_eq!(send_and_read(&mut client, &add3), ":3\r\n");

    let add4 = format_resp_cmd(&[
        "AGENT.MEM.ADD",
        "agent:sess1",
        "assistant",
        "Use --tls-port 6380.",
        "TOKENS",
        "10",
        "VEC",
        "3",
        "0.0",
        "0.9",
        "0.1",
    ]);
    assert_eq!(send_and_read(&mut client, &add4), ":4\r\n");

    // 2. Request context with MAX_TOKENS 20 (fits turns 3 & 4) + QUERY [1,0,0] RECALL 2 (recalls turns 1 & 2)
    let ctx_cmd = format_resp_cmd(&[
        "AGENT.MEM.CONTEXT",
        "agent:sess1",
        "MAX_TOKENS",
        "20",
        "QUERY",
        "3",
        "1.0",
        "0.0",
        "0.0",
        "RECALL",
        "2",
    ]);
    let ctx_resp = send_and_read(&mut client, &ctx_cmd);
    assert!(
        ctx_resp.starts_with("*2\r\n*2\r\n")
            && ctx_resp.contains("How do I enable TLS on port 6380?")
            && ctx_resp.contains("My prod cluster uses 8 shards on NVMe."),
        "AGENT.MEM.CONTEXT: {}",
        ctx_resp
    );

    // 3. Compact older turns keeping only 1 recent turn
    let compact_cmd = format_resp_cmd(&[
        "AGENT.MEM.COMPACT",
        "agent:sess1",
        "KEEP_RECENT",
        "1",
        "SUMMARY",
        "Summary: user runs 8 NVMe shards and asked about TLS.",
        "TOKENS",
        "8",
    ]);
    assert_eq!(send_and_read(&mut client, &compact_cmd), ":3\r\n");

    // 4. Verify AGENT.MEM.INFO & AGENT.MEM.CLEAR
    let info_resp = send_and_read(&mut client, b"AGENT.MEM.INFO agent:sess1\r\n");
    assert!(
        info_resp.contains("active_turns\r\n:2\r\n")
            && info_resp.contains("total_turns\r\n:5\r\n")
            && info_resp.contains("compactions\r\n:1\r\n"),
        "AGENT.MEM.INFO: {}",
        info_resp
    );

    assert_eq!(
        send_and_read(&mut client, b"AGENT.MEM.CLEAR agent:sess1\r\n"),
        ":1\r\n"
    );
}

#[test]
fn test_nvme_tiered_vector_and_rerank_e2e() {
    let port = 19157;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Insert vectors with TIERED (spills FP32 to NVMe .vtier file, keeps SQ8 in RAM)
    assert_eq!(
        send_and_read(
            &mut client,
            &format_resp_cmd(&[
                "VADD",
                "vtier_idx",
                "doc_a",
                "1.0",
                "0.25",
                "-0.5",
                "0.75",
                "L2",
                "TIERED",
            ]),
        ),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(
            &mut client,
            &format_resp_cmd(&[
                "VADD",
                "vtier_idx",
                "doc_b",
                "0.0",
                "1.0",
                "0.5",
                "-0.25",
                "L2",
                "TIERED",
            ]),
        ),
        "+OK\r\n"
    );

    // VQUERY with RERANK reads exact FP32 vectors back from the .vtier file
    let vq = format_resp_cmd(&[
        "VQUERY",
        "vtier_idx",
        "2",
        "1.0",
        "0.25",
        "-0.5",
        "0.75",
        "RERANK",
    ]);
    let vq_resp = send_and_read(&mut client, &vq);
    assert!(
        vq_resp.contains("doc_a") && vq_resp.contains("0.000000"),
        "VQUERY RERANK on tiered vectors should return exact 0.000000 distance from disk: {}",
        vq_resp
    );
}

#[test]
fn test_tier_file_lives_under_dir_is_reset_at_startup_and_not_shared_e2e() {
    let port: u16 = 17087;
    let other_port: u16 = 17088;
    let dir = std::env::temp_dir().join(format!("rudis-tierfile-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let tier_dir = dir.join(format!("rudis_tier_{port}"));
    std::fs::create_dir_all(&tier_dir).unwrap();
    let tier_file = tier_dir.join("tier_shard_0.db");
    // Left by an earlier run; nothing references it after a restart.
    std::fs::write(&tier_file, vec![7u8; 1 << 20]).unwrap();
    let tier_len = || std::fs::metadata(&tier_file).unwrap().len();

    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "1",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];
    let mut child = KillOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(tier_len(), 0, "stale tier data kept");

    let big = "b".repeat(5000);
    let get_big = format!("${}\r\n{big}\r\n", big.len());
    assert_eq!(resp_cmd(&mut c, &["SET", "tf:big", &big]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["TIER", "SPILL", "tf:big"]), ":1\r\n");
    let spilled_len = tier_len();
    assert!(spilled_len > 0);

    // A second server pointed at the same tier file gets no tiering
    // rather than truncating or overwriting the first one's data.
    let mut other = KillOnDrop(
        rudis_bin()
            .args([
                "--port",
                &other_port.to_string(),
                "--threads",
                "1",
                "--no-pin",
            ])
            .env("RUDIS_TIER_DIR", &tier_dir)
            .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn rudis"),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut o = loop {
        if let Ok(o) = TcpStream::connect(("127.0.0.1", other_port)) {
            break o;
        }
        assert!(std::time::Instant::now() < deadline, "rudis did not start");
        thread::sleep(Duration::from_millis(50));
    };
    o.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut o, &["SET", "tf:o", &big]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut o, &["TIER", "SPILL", "tf:o"]), ":0\r\n");
    assert_eq!(tier_len(), spilled_len);
    drop(o);
    shutdown_and_wait(other_port, &mut other.0);

    // COPY and RENAME of a spilled key: deleting the copy must not punch out
    // the source's extent, and loading the renamed key must restore it under
    // its new name rather than the key recorded when it was spilled.
    assert_eq!(resp_cmd(&mut c, &["COPY", "tf:big", "tf:copy"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["DEL", "tf:copy"]), ":1\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["RENAME", "tf:big", "tf:moved"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["GET", "tf:big"]), "$-1\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "tf:moved"]), get_big);
    assert_eq!(
        resp_cmd(&mut c, &["RENAME", "tf:moved", "tf:big"]),
        "+OK\r\n"
    );

    // After a restart the AOF has the value; the tier file starts empty.
    drop(c);
    shutdown_and_wait(port, &mut child.0);
    let mut child = KillOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(tier_len(), 0, "tier data from the previous run kept");
    assert_eq!(resp_cmd(&mut c, &["GET", "tf:big"]), get_big);
    drop(c);
    shutdown_and_wait(port, &mut child.0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_tiered_cooling_mutation_and_extent_reclamation_e2e() {
    let port = 16515;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    // 1. String: SET -> COOL -> APPEND
    assert_eq!(
        send_and_read(&mut client, b"SET str_key hello\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"TIER COOL str_key\r\n"),
        ":1\r\n"
    );
    let info = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info.contains("cooled_keys:1"));

    // Mutate via APPEND: must not fail with WRONGTYPE, must uncool and drop extent
    assert_eq!(
        send_and_read(&mut client, b"APPEND str_key _world\r\n"),
        ":11\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET str_key\r\n"),
        "$11\r\nhello_world\r\n"
    );
    let info_after_append = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_append.contains("cooled_keys:0"));
    assert!(info_after_append.contains("total_deletes:1"));

    // 2. Integer: SET -> COOL -> INCRBY
    assert_eq!(send_and_read(&mut client, b"SET int_key 42\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read(&mut client, b"TIER COOL int_key\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"INCRBY int_key 8\r\n"),
        ":50\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"GET int_key\r\n"),
        "$2\r\n50\r\n"
    );
    let info_after_incr = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_incr.contains("cooled_keys:0"));
    assert!(info_after_incr.contains("total_deletes:2"));

    // 3. Hash: HSET -> COOL -> HSET (new field)
    assert_eq!(
        send_and_read(&mut client, b"HSET h_key f1 v1\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"TIER COOL h_key\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut client, b"HSET h_key f2 v2\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HGET h_key f1\r\n"),
        "$2\r\nv1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"HGET h_key f2\r\n"),
        "$2\r\nv2\r\n"
    );
    let info_after_hset = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_hset.contains("cooled_keys:0"));
    assert!(info_after_hset.contains("total_deletes:3"));

    // 4. List: RPUSH -> COOL -> LPUSH & LPOP
    assert_eq!(
        send_and_read(&mut client, b"RPUSH list_key elem1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"TIER COOL list_key\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"LPUSH list_key elem0\r\n"),
        ":2\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"LPOP list_key\r\n"),
        "$5\r\nelem0\r\n"
    );
    let info_after_list = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_list.contains("cooled_keys:0"));
    assert!(info_after_list.contains("total_deletes:4"));

    // 5. Set: SADD -> COOL -> SADD
    assert_eq!(send_and_read(&mut client, b"SADD set_key m1\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut client, b"TIER COOL set_key\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"SADD set_key m2\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut client, b"SISMEMBER set_key m1\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read(&mut client, b"SISMEMBER set_key m2\r\n"),
        ":1\r\n"
    );
    let info_after_set = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_set.contains("cooled_keys:0"));
    assert!(info_after_set.contains("total_deletes:5"));

    // 6. Sorted Set: ZADD -> COOL -> ZADD
    assert_eq!(
        send_and_read(&mut client, b"ZADD z_key 10.0 z1\r\n"),
        ":1\r\n"
    );
    assert_eq!(send_and_read(&mut client, b"TIER COOL z_key\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read(&mut client, b"ZADD z_key 20.0 z2\r\n"),
        ":1\r\n"
    );
    let zcard = send_and_read(&mut client, b"ZCARD z_key\r\n");
    assert_eq!(zcard, ":2\r\n");
    let info_after_zadd = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_after_zadd.contains("cooled_keys:0"));
    assert!(info_after_zadd.contains("total_deletes:6"));

    // 7. FlushDB resets cooled and tiered stats cleanly
    assert_eq!(send_and_read(&mut client, b"SET f_key val\r\n"), "+OK\r\n");
    assert_eq!(send_and_read(&mut client, b"TIER COOL f_key\r\n"), ":1\r\n");
    assert_eq!(send_and_read(&mut client, b"FLUSHDB\r\n"), "+OK\r\n");
    let info_flush = send_and_read(&mut client, b"TIER INFO\r\n");
    assert!(info_flush.contains("cooled_keys:0"));
    assert!(info_flush.contains("tiered_keys:0"));
}

/// A client that keeps its socket full (a `--pipe` bulk load) must not get its
/// whole stream parsed into one batch, and the connection's buffers must not
/// stay allocated on the shard after it disconnects. Before the batch limit, 1M
/// pipelined commands were parsed into one ~256 MiB `Vec<Command>` that the
/// pooled connection scratch then kept for the life of the process.
#[test]
fn test_pipeline_flood_memory_is_bounded_and_released_e2e() {
    const N: usize = 1_000_000;
    let port: u16 = 17391;
    let args = [
        "--port",
        "17391",
        "--threads",
        "1",
        "--no-pin",
        "--bind",
        "127.0.0.1",
    ];
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let child = KillOnDrop(spawn_rudis_listening(&args, port));
    let pid = child.0.id();
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    let base_alloc = info_allocated_bytes(&mut admin);
    let base_rss_kb = proc_status_kb(pid, "VmRSS:");

    // Flood: one writer streams N GETs of a missing key as fast as the socket
    // takes them while this thread drains the N `$-1\r\n` replies.
    let flood = TcpStream::connect(("127.0.0.1", port)).unwrap();
    flood
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    let mut writer = flood.try_clone().unwrap();
    let w = thread::spawn(move || {
        let chunk = b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n".repeat(4096);
        let mut sent = 0;
        while sent < N {
            let n = (N - sent).min(4096);
            writer.write_all(&chunk[..n * 26]).unwrap();
            sent += n;
        }
    });
    let mut reader = flood;
    let want = N * 5;
    let mut got = 0usize;
    let mut buf = vec![0u8; 1 << 16];
    while got < want {
        let n = reader.read(&mut buf).expect("flood reply");
        assert!(n > 0, "server closed the flood connection");
        got += n;
    }
    w.join().unwrap();
    assert_eq!(got, want, "unexpected reply bytes");

    // Peak RSS during the flood: the stream is ~26 MB, so anything near the
    // old ~256 MiB batch means the input was not processed in bounded batches.
    let peak_growth_mb = proc_status_kb(pid, "VmHWM:").saturating_sub(base_rss_kb) / 1024;
    assert!(
        peak_growth_mb < 96,
        "peak RSS grew {} MiB while serving a pipelined flood",
        peak_growth_mb
    );

    drop(reader);
    // Let the shard notice the hang-up and recycle the connection.
    let mut retained = 0;
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(100));
        retained = info_allocated_bytes(&mut admin).saturating_sub(base_alloc);
        if retained < 16 << 20 {
            break;
        }
    }
    assert!(
        retained < 16 << 20,
        "{} MiB still allocated after the flood client disconnected",
        retained >> 20
    );
}

/// `maxmemory` must bound what the process actually allocates, not just the
/// server's own estimate: loading ~5x the limit under each policy has to keep
/// jemalloc's live bytes near the limit.
#[test]
fn test_maxmemory_bounds_real_allocation_e2e() {
    const N: usize = 1_000_000;
    const LIMIT: u64 = 48 << 20;
    for (port, policy, threads) in [
        (17392u16, "allkeys-random", "1"),
        (17393u16, "noeviction", "1"),
        (17394u16, "allkeys-lru", "4"),
        (17395u16, "noeviction", "4"),
    ] {
        let port_s = port.to_string();
        let args = [
            "--port",
            port_s.as_str(),
            "--threads",
            threads,
            "--no-pin",
            "--bind",
            "127.0.0.1",
        ];
        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let _child = KillOnDrop(spawn_rudis_listening(&args, port));
        let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
        admin
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let base_alloc = info_allocated_bytes(&mut admin);
        assert_eq!(
            resp_cmd(
                &mut admin,
                &["CONFIG", "SET", "maxmemory", &LIMIT.to_string()]
            ),
            "+OK\r\n"
        );
        assert_eq!(
            resp_cmd(&mut admin, &["CONFIG", "SET", "maxmemory-policy", policy]),
            "+OK\r\n"
        );

        let replies = pipelined_set_load(port, N);
        let text = String::from_utf8_lossy(&replies);
        let oom = text.matches("-OOM").count();
        if policy == "noeviction" {
            assert!(oom > 0, "noeviction never rejected a write past maxmemory");
        } else {
            assert_eq!(oom, 0, "{policy} rejected writes instead of evicting");
        }

        let used = info_field_u64(&mut admin, "memory", "used_memory");
        let alloc = info_allocated_bytes(&mut admin).saturating_sub(base_alloc);
        assert!(
            used <= LIMIT + LIMIT / 8,
            "{policy}/{threads}: used_memory {} MiB over the {} MiB limit",
            used >> 20,
            LIMIT >> 20
        );
        // Real live bytes must stay within 1.5x the limit (allows for the
        // allocator's size-class rounding and per-connection buffers).
        assert!(
            alloc <= LIMIT + LIMIT / 2,
            "{policy}/{threads}: allocator holds {} MiB for a {} MiB maxmemory (used_memory {} MiB)",
            alloc >> 20,
            LIMIT >> 20,
            used >> 20
        );
    }
}

/// Keys the tier holds (spilled to disk or cooled) must answer every command
/// with their real value; they used to answer WRONGTYPE to all but GET/TYPE,
/// which auto-tiering makes visible on any server under memory pressure.
#[test]
fn test_tiered_keys_answer_every_command_e2e() {
    let port = 17431;
    let dir = std::env::temp_dir().join(format!("rudis-tier-cmds-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("rudis.conf");
    std::fs::write(&conf, format!("dir {}\n", dir.display())).unwrap();
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
    // Several keys of each kind so both shards (local and remote to this
    // connection) are covered.
    for i in 0..4 {
        let (s, n, h, l) = (
            format!("s{i}"),
            format!("n{i}"),
            format!("h{i}"),
            format!("l{i}"),
        );
        assert_eq!(resp_cmd(&mut c, &["SET", &s, "hello"]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut c, &["SET", &n, "10"]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut c, &["HSET", &h, "f", "v"]), ":1\r\n");
        assert_eq!(resp_cmd(&mut c, &["RPUSH", &l, "a", "b"]), ":2\r\n");
        for k in [&s, &n, &h, &l] {
            assert_eq!(resp_cmd(&mut c, &["TIER", "SPILL", k]), ":1\r\n", "{k}");
        }
        assert_eq!(resp_cmd(&mut c, &["STRLEN", &s]), ":5\r\n", "{s}");
        assert_eq!(resp_cmd(&mut c, &["APPEND", &s, "!"]), ":6\r\n", "{s}");
        assert_eq!(
            resp_cmd(&mut c, &["GETRANGE", &s, "0", "1"]),
            "$2\r\nhe\r\n",
            "{s}"
        );
        assert_eq!(resp_cmd(&mut c, &["INCR", &n]), ":11\r\n", "{n}");
        assert_eq!(resp_cmd(&mut c, &["HGET", &h, "f"]), "$1\r\nv\r\n", "{h}");
        assert_eq!(
            resp_cmd(&mut c, &["LRANGE", &l, "0", "-1"]),
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
            "{l}"
        );
        // Cooled keys too.
        let cs = format!("c{i}");
        assert_eq!(resp_cmd(&mut c, &["SET", &cs, "abc"]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut c, &["TIER", "COOL", &cs]), ":1\r\n", "{cs}");
        assert_eq!(resp_cmd(&mut c, &["STRLEN", &cs]), ":3\r\n", "{cs}");
        // Pipelined (squashed) path.
        assert_eq!(resp_cmd(&mut c, &["TIER", "SPILL", &n]), ":1\r\n", "{n}");
        let mut got = send_and_read(&mut c, format!("INCR {n}\r\nINCR {n}\r\n").as_bytes());
        while got.matches("\r\n").count() < 2 {
            got.push_str(&send_and_read(&mut c, b""));
        }
        assert_eq!(got, ":12\r\n:13\r\n", "{n}");
    }
    drop(c);
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Redis tracking.tcl "invalidation message of eviction keys should be
/// before response", from connections on every shard: a write executed for
/// another shard must republish that shard's usage (it didn't for SETBIT),
/// and the eviction's invalidation must precede the reply.
#[test]
fn test_eviction_invalidation_precedes_reply_across_shards_e2e() {
    let port = 17432;
    let dir = std::env::temp_dir().join(format!("rudis-evict-track-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("rudis.conf");
    std::fs::write(&conf, format!("dir {}\n", dir.display())).unwrap();
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
    for round in 0..8 {
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        resp_cmd(&mut c, &["HELLO", "3"]);
        resp_cmd(&mut c, &["DEL", "volatile-key", "big-key"]);
        assert_eq!(resp_cmd(&mut c, &["CLIENT", "TRACKING", "on"]), "+OK\r\n");
        let info = resp_cmd(&mut c, &["INFO", "memory"]);
        let used: u64 = info
            .lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let limit = (used + 100 * 1024).to_string();
        assert_eq!(
            resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", &limit]),
            "+OK\r\n"
        );
        assert_eq!(
            resp_cmd(
                &mut c,
                &["CONFIG", "SET", "maxmemory-policy", "volatile-random"]
            ),
            "+OK\r\n"
        );
        assert_eq!(
            resp_cmd(&mut c, &["SETEX", "volatile-key", "10000", "x"]),
            "+OK\r\n"
        );
        assert_eq!(resp_cmd(&mut c, &["GET", "volatile-key"]), "$1\r\nx\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["SETBIT", "big-key", "1600000", "0"]),
            ":0\r\n"
        );
        let mut got = resp_cmd(&mut c, &["GETBIT", "big-key", "0"]);
        while !got.ends_with(":0\r\n") {
            got.push_str(&send_and_read(&mut c, b""));
        }
        assert_eq!(
            got, ">2\r\n$10\r\ninvalidate\r\n*1\r\n$12\r\nvolatile-key\r\n:0\r\n",
            "round {round}"
        );
        assert_eq!(
            resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "0"]),
            "+OK\r\n"
        );
        assert_eq!(
            resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory-policy", "noeviction"]),
            "+OK\r\n"
        );
    }
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

/// `maxmemory` holds for collection writes too: the limit is checked against
/// the allocator's count, not the per-command estimate (which drifts for
/// hashes, lists and sorted sets), so filling the server with hashes far past
/// the limit keeps what the allocator holds near it.
#[test]
fn test_maxmemory_enforced_by_allocator_count_for_hashes() {
    let port = 17935u16;
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
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    // Let the first calibration against jemalloc's process count run.
    thread::sleep(Duration::from_millis(300));
    let base = info_u64(&mut c, "memory", "used_memory");
    let base_alloc = info_u64(&mut c, "memory", "allocator_allocated");
    let headroom = 8u64 << 20;
    let limit = base + headroom;
    assert_eq!(
        resp_cmd(
            &mut c,
            &["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"]
        ),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", &limit.to_string()]),
        "+OK\r\n"
    );
    // ~4x the headroom in hashes, pipelined 100 commands at a time. Small
    // hashes are packed (~45 bytes per 32-byte field), so this takes 32000
    // hashes of 20 fields.
    let val = "x".repeat(32);
    let mut buf = vec![0u8; 1 << 16];
    for batch in 0..320 {
        let mut pipe = String::new();
        for k in 0..100 {
            let key = format!("h:{batch}:{k}");
            pipe.push_str(&format!(
                "*{}\r\n$4\r\nHSET\r\n${}\r\n{key}\r\n",
                2 + 2 * 20,
                key.len()
            ));
            for f in 0..20 {
                let field = format!("field{f}");
                pipe.push_str(&format!(
                    "${}\r\n{field}\r\n${}\r\n{val}\r\n",
                    field.len(),
                    val.len()
                ));
            }
        }
        c.write_all(pipe.as_bytes()).unwrap();
        let mut replies = 0;
        let mut got = Vec::new();
        while replies < 100 {
            let n = c.read(&mut buf).unwrap();
            assert!(n > 0, "connection closed");
            got.extend_from_slice(&buf[..n]);
            replies = got.iter().filter(|&&b| b == b'\n').count();
        }
        assert!(
            !String::from_utf8_lossy(&got).contains("OOM"),
            "write refused"
        );
    }
    thread::sleep(Duration::from_millis(300));
    let used = info_u64(&mut c, "memory", "used_memory");
    let alloc = info_u64(&mut c, "memory", "allocator_allocated");
    let evicted = info_u64(&mut c, "stats", "evicted_keys");
    assert!(evicted > 0, "nothing evicted");
    assert!(
        used <= limit + (1 << 20),
        "used_memory {used} vs limit {limit}"
    );
    assert!(
        alloc <= base_alloc + headroom + headroom / 4,
        "allocator holds {} MiB over the base for an {} MiB headroom",
        (alloc - base_alloc) >> 20,
        headroom >> 20
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "0"]),
        "+OK\r\n"
    );
    drop(c);
    shutdown_and_wait(port, &mut child);
}

/// With several shards, eviction must come off every shard evenly. It used
/// to drain the shard running the commands (hot keys included) while other
/// shards held cold data, because each shard's share ignored memory no
/// shard owns. Checked with LFU: the balancing is the same for every
/// policy, and LRU's one-second clock (as Redis's) can't rank keys written
/// within the same second, which this whole run fits in.
#[test]
fn test_eviction_spreads_across_shards_and_keeps_hot_keys_e2e() {
    for (port, policy) in [(17966u16, "allkeys-lfu")] {
        let ps = port.to_string();
        let mut child = spawn_rudis_listening(&["--port", &ps, "--threads", "4", "--no-pin"], port);
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        assert_eq!(
            resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory-policy", policy]),
            "+OK\r\n"
        );
        assert_eq!(
            resp_cmd(&mut c, &["CONFIG", "SET", "lfu-log-factor", "0"]),
            "+OK\r\n"
        );
        let val = "x".repeat(1000);
        let mut hot_misses = 0;
        let mut touch_hot = |c: &mut TcpStream| {
            for i in 0..100 {
                if !resp_cmd(c, &["GET", &format!("hot:{i}")]).starts_with("$1000") {
                    hot_misses += 1;
                }
            }
        };
        for i in 0..100 {
            assert_eq!(
                resp_cmd(&mut c, &["SET", &format!("hot:{i}"), &val]),
                "+OK\r\n"
            );
        }
        for _ in 0..50 {
            touch_hot(&mut c);
        }
        let used = info_u64(&mut c, "memory", "used_memory");
        assert_eq!(
            resp_cmd(
                &mut c,
                &[
                    "CONFIG",
                    "SET",
                    "maxmemory",
                    &(used + 4_000_000).to_string()
                ]
            ),
            "+OK\r\n"
        );
        // Cold keys well past the limit; the hot set stays in use throughout.
        for i in 0..12_000 {
            let r = resp_cmd(&mut c, &["SET", &format!("cold:{i}"), &val]);
            assert!(r == "+OK\r\n" || r.starts_with("-OOM"), "{r}");
            if i % 500 == 499 {
                touch_hot(&mut c);
            }
        }
        assert!(info_u64(&mut c, "stats", "evicted_keys") > 4000, "{policy}");
        let hot_alive = (0..100)
            .filter(|i| resp_cmd(&mut c, &["EXISTS", &format!("hot:{i}")]) == ":1\r\n")
            .count();
        eprintln!("{policy}: {hot_alive}/100 hot keys alive, {hot_misses} hot GET misses");
        assert!(
            hot_alive >= 95,
            "{policy}: only {hot_alive}/100 hot keys survived"
        );
        assert_eq!(
            resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "0"]),
            "+OK\r\n"
        );
        drop(c);
        shutdown_and_wait(port, &mut child);
    }
}

/// `MEMORY STATS` reports real accounting (keys match DBSIZE, the
/// incremental counters match a recount), `MEMORY DOCTOR` gives a clean bill
/// for a small healthy instance, and INFO names the real allocator.
#[test]
fn test_memory_stats_and_doctor_report_real_accounting_e2e() {
    let port = 18010;
    start_test_server(port, 4);
    let mut c = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    for batch in 0..20 {
        let mut cmd = String::from("MSET");
        for i in 0..500 {
            cmd.push_str(&format!(" ms:{} value-{}", batch * 500 + i, i));
        }
        cmd.push_str("\r\n");
        assert_eq!(send_and_read_reply(&mut c, cmd.as_bytes()), "+OK\r\n");
    }
    let raw = send_and_read_reply(&mut c, b"MEMORY STATS\r\n");
    let toks: Vec<&str> = raw
        .split("\r\n")
        .filter(|t| !t.is_empty() && !t.starts_with('*') && !t.starts_with('$'))
        .collect();
    let field = |name: &str| -> String {
        let i = toks
            .iter()
            .position(|t| *t == name)
            .unwrap_or_else(|| panic!("{name} missing: {raw}"));
        toks[i + 1].trim_start_matches(':').to_string()
    };
    assert_eq!(field("keys.count"), "10000");
    assert_eq!(field("dataset.bytes"), field("rudis.dataset.exact.bytes"));
    assert_eq!(
        field("overhead.hashtable.main"),
        field("rudis.hashtable.exact.bytes")
    );
    assert!(field("used_memory").parse::<u64>().unwrap() > 0);
    let doctor = send_and_read_reply(&mut c, b"MEMORY DOCTOR\r\n");
    // Allocator figures are process-wide and this test process hosts many
    // servers, so fragmentation findings may appear here; the server's own
    // accounting must not drift.
    assert!(doctor.starts_with('$'), "{doctor}");
    assert!(
        doctor.contains("can't find any memory issues") || doctor.contains("detected a few issues"),
        "{doctor}"
    );
    assert!(!doctor.contains("accounting drift"), "{doctor}");
    let info = send_and_read_reply(&mut c, b"INFO memory\r\n");
    assert!(info.contains("mem_allocator:jemalloc"), "{info}");
}

/// Eviction splits the excess between shards by the per-segment dataset
/// estimate, not the legacy `data_bytes` counter (which most write paths do
/// not maintain: a shard full of hashes looked nearly empty and kept all its
/// keys while the shard holding strings was evicted down to ~13%). Strings
/// on one shard and same-sized hashes on another, filled past `maxmemory`:
/// both keep a similar share (within 1.6x), whichever shard the connection
/// lands on (the fair share once counted the two empty shards, so the
/// connection's own shard was evicted to a quarter of the target).
#[test]
fn test_eviction_splits_shards_fairly_across_value_types_e2e() {
    let port = 18012;
    let shards = 4;
    start_test_server(port, shards);
    let mut c = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    // Two hash tags on different shards.
    let shard = |k: &str| rudis::router::target_shard_and_hash(k.as_bytes(), shards).0;
    let tags: Vec<String> = (0..100).map(|i| format!("t{i}")).collect();
    let s_tag = &tags[0];
    let h_tag = tags
        .iter()
        .find(|t| shard(&format!("{{{t}}}")) != shard(&format!("{{{s_tag}}}")))
        .unwrap();
    assert_eq!(
        send_and_read(&mut c, b"CONFIG SET maxmemory-policy allkeys-lru\r\n"),
        "+OK\r\n"
    );
    assert_eq!(
        send_and_read(&mut c, b"CONFIG SET maxmemory 48mb\r\n"),
        "+OK\r\n"
    );
    // ~100 bytes of payload per key either way; ~2x the limit in total.
    let n = 300_000usize;
    let val = "v".repeat(100);
    let fval = "f".repeat(16);
    for batch in 0..(n / 500) {
        let mut req = String::new();
        for i in 0..500 {
            let k = batch * 500 + i;
            req.push_str(&format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{{{s_tag}}}s:{k}\r\n$100\r\n{val}\r\n",
                format!("{{{s_tag}}}s:{k}").len()
            ));
            let hk = format!("{{{h_tag}}}h:{k}");
            req.push_str(&format!(
                "*8\r\n$4\r\nHSET\r\n${}\r\n{hk}\r\n$2\r\nf1\r\n$16\r\n{fval}\r\n$2\r\nf2\r\n$16\r\n{fval}\r\n$2\r\nf3\r\n$16\r\n{fval}\r\n",
                hk.len()
            ));
        }
        c.write_all(req.as_bytes()).unwrap();
        let mut got = 0;
        let mut buf = vec![0u8; 1 << 16];
        while got < 1000 {
            let r = c.read(&mut buf).unwrap();
            assert!(r > 0);
            got += buf[..r].windows(2).filter(|w| w == b"\r\n").count();
        }
    }
    thread::sleep(Duration::from_millis(500));
    let count = |c: &mut TcpStream, pattern: &str| -> usize {
        let mut cursor = "0".to_string();
        let mut total = 0;
        loop {
            let cmd = format!("SCAN {cursor} MATCH {pattern} COUNT 10000\r\n");
            let r = send_and_read_reply(c, cmd.as_bytes());
            let parts: Vec<&str> = r.split("\r\n").collect();
            cursor = parts[2].to_string();
            total += parts.iter().skip(3).filter(|p| p.starts_with('{')).count();
            if cursor == "0" {
                break;
            }
        }
        total
    };
    let strings = count(&mut c, &format!("{{{s_tag}}}s:*"));
    let hashes = count(&mut c, &format!("{{{h_tag}}}h:*"));
    assert!(
        strings > 0 && hashes > 0,
        "strings {strings} hashes {hashes}"
    );
    let (lo, hi) = (strings.min(hashes), strings.max(hashes));
    assert!(
        hi * 5 < lo * 8,
        "eviction lopsided between shards: strings kept {strings}, hashes kept {hashes}"
    );
}

/// `--maxmemory` sets the eviction limit itself (not only a tiering
/// threshold, as its help once said) and other `--<directive> <value>`
/// flags pass through as config directives, e.g. `--maxmemory-policy`.
#[test]
fn test_cli_maxmemory_and_passthrough_policy_take_effect_e2e() {
    let port = 18013u16;
    let ps = port.to_string();
    let help = rudis_bin().arg("--help").output().unwrap();
    let help = String::from_utf8_lossy(&help.stdout);
    assert!(help.contains("enforced by eviction"), "{help}");
    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &ps,
            "--threads",
            "1",
            "--no-pin",
            "--maxmemory",
            "64mb",
            "--maxmemory-policy",
            "allkeys-lfu",
        ],
        port,
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let r = send_and_read_reply(&mut c, b"CONFIG GET maxmemory\r\n");
    assert!(r.contains("67108864"), "{r}");
    let r = send_and_read_reply(&mut c, b"CONFIG GET maxmemory-policy\r\n");
    assert!(r.contains("allkeys-lfu"), "{r}");
    let r = send_and_read_reply(&mut c, b"INFO memory\r\n");
    assert!(r.contains("maxmemory:67108864"), "{r}");
    let _ = srv.kill();
    let _ = srv.wait();
}

/// ZADD + ZPOPMIN churn on sorted sets of constant size must not grow
/// memory. Packed sorted sets store entries in pages; a compacted page kept
/// its full 4 KB buffer while a few long-lived (high-score) members pinned
/// it, so churn grew memory without bound (~5 bytes per pop per key).
#[test]
fn test_zset_churn_keeps_memory_flat_e2e() {
    let port = 18014u16;
    let ps = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &ps, "--threads", "1", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let used = |c: &mut TcpStream| -> i64 {
        let r = send_and_read_reply(c, b"INFO memory\r\n");
        r.lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let keys = 20;
    let mut rng = 0x9E37_79B9u32;
    let mut seq = 0u64;
    let mut batch = |c: &mut TcpStream, pairs: usize, pop: bool| {
        let mut req = String::new();
        for _ in 0..pairs {
            rng ^= rng << 13;
            rng ^= rng >> 17;
            rng ^= rng << 5;
            let k = format!("z{}", seq % keys);
            let m = format!("member:{seq:032}");
            let s = rng.to_string();
            seq += 1;
            req.push_str(&format!(
                "*4\r\n$4\r\nZADD\r\n${}\r\n{k}\r\n${}\r\n{s}\r\n${}\r\n{m}\r\n",
                k.len(),
                s.len(),
                m.len()
            ));
            if pop {
                req.push_str(&format!("*2\r\n$7\r\nZPOPMIN\r\n${}\r\n{k}\r\n", k.len()));
            }
        }
        c.write_all(req.as_bytes()).unwrap();
        // ZADD: 1 line; ZPOPMIN: 5 lines.
        let want = pairs * if pop { 6 } else { 1 };
        let (mut got, mut buf) = (0, vec![0u8; 1 << 16]);
        while got < want {
            let r = c.read(&mut buf).unwrap();
            assert!(r > 0);
            got += buf[..r].windows(2).filter(|w| w == b"\r\n").count();
        }
    };
    // Fill each key to 300 members, then churn to a steady state.
    batch(&mut c, keys as usize * 300, false);
    for _ in 0..100 {
        batch(&mut c, 1000, true);
    }
    let before = used(&mut c);
    for _ in 0..1000 {
        batch(&mut c, 1000, true);
    }
    let after = used(&mut c);
    let r = send_and_read_reply(&mut c, b"ZCARD z0\r\n");
    assert_eq!(r, ":300\r\n");
    assert!(
        after - before < 1 << 20,
        "zset churn grew used_memory from {before} to {after}"
    );
    let _ = srv.kill();
    let _ = srv.wait();
}

/// SADD + SREM churn on a set that fits one page must not grow memory: the
/// page taking appends was exempt from compaction, so dead members filled
/// it to a full 4 KB page however few were live (~4x for 100 short members).
#[test]
fn test_set_churn_on_one_page_keeps_memory_flat_e2e() {
    let port = 18015u16;
    let ps = port.to_string();
    let mut srv = spawn_rudis_listening(&["--port", &ps, "--threads", "1", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let used = |c: &mut TcpStream| -> i64 {
        let r = send_and_read_reply(c, b"INFO memory\r\n");
        r.lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let (sets, live) = (1000usize, 100usize);
    let send = |c: &mut TcpStream, cmds: &[(&str, String, String)]| {
        let mut req = String::new();
        for (op, k, m) in cmds {
            req.push_str(&format!(
                "*3\r\n${}\r\n{op}\r\n${}\r\n{k}\r\n${}\r\n{m}\r\n",
                op.len(),
                k.len(),
                m.len()
            ));
        }
        c.write_all(req.as_bytes()).unwrap();
        let (mut got, mut buf) = (0, vec![0u8; 1 << 16]);
        while got < cmds.len() {
            let r = c.read(&mut buf).unwrap();
            assert!(r > 0);
            got += buf[..r].windows(2).filter(|w| w == b"\r\n").count();
        }
    };
    // Set i holds members m{lo..lo+live}; a step drops the oldest and adds
    // the next, so every SREM and SADD changes the set.
    let step = |c: &mut TcpStream, lo: usize| {
        let mut cmds = Vec::with_capacity(sets * 2);
        for i in 0..sets {
            let k = format!("s{i}");
            cmds.push(("SREM", k.clone(), format!("m{lo}")));
            cmds.push(("SADD", k, format!("m{}", lo + live)));
        }
        send(c, &cmds);
    };
    for j in 0..live {
        let cmds: Vec<_> = (0..sets)
            .map(|i| ("SADD", format!("s{i}"), format!("m{j}")))
            .collect();
        send(&mut c, &cmds);
    }
    for lo in 0..20 {
        step(&mut c, lo);
    }
    let before = used(&mut c);
    for lo in 20..470 {
        step(&mut c, lo);
    }
    let after = used(&mut c);
    assert_eq!(send_and_read_reply(&mut c, b"SCARD s7\r\n"), ":100\r\n");
    assert_eq!(
        send_and_read_reply(&mut c, b"SISMEMBER s7 m569\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut c, b"SISMEMBER s7 m470\r\n"),
        ":1\r\n"
    );
    assert_eq!(
        send_and_read_reply(&mut c, b"SISMEMBER s7 m469\r\n"),
        ":0\r\n"
    );
    assert!(
        after - before < 1 << 20,
        "set churn grew used_memory from {before} to {after}"
    );
    let _ = srv.kill();
    let _ = srv.wait();
}

/// MEMORY MALLOC-STATS returns jemalloc's statistics report, as Redis does
/// (INFO says mem_allocator:jemalloc; the Redis suite checks both).
#[test]
fn test_memory_malloc_stats_reports_jemalloc_e2e() {
    let port = 18035u16;
    let ps = port.to_string();
    let srv = ServerGuard(spawn_rudis_listening(
        &["--port", &ps, "--threads", "1", "--no-pin"],
        port,
    ));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let r = send_and_read_reply(&mut c, b"MEMORY MALLOC-STATS\r\n");
    assert!(
        r.starts_with('$') && r.contains("jemalloc"),
        "{}",
        &r[..r.len().min(200)]
    );
    assert!(send_and_read_reply(&mut c, b"INFO memory\r\n").contains("mem_allocator:jemalloc"));
    drop(srv);
}

/// MEMORY USAGE sizes every type on whichever shard owns the key; on a
/// remote shard it went through GET and answered nil for non-strings.
#[test]
fn test_memory_usage_of_non_strings_on_any_shard_e2e() {
    let port = 18040u16;
    let ps = port.to_string();
    let srv = ServerGuard(spawn_rudis_listening(
        &["--port", &ps, "--threads", "4", "--no-pin"],
        port,
    ));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for i in 0..16 {
        for (ty, write) in [
            ("h", "HSET {k} f v"),
            ("l", "RPUSH {k} a"),
            ("s", "SADD {k} a"),
            ("z", "ZADD {k} 1 a"),
        ] {
            let k = format!("{ty}{i}");
            let w = write.replace("{k}", &k) + "\r\n";
            assert_eq!(send_and_read_reply(&mut c, w.as_bytes()), ":1\r\n", "{w}");
            let r = send_and_read_reply(&mut c, format!("MEMORY USAGE {k}\r\n").as_bytes());
            let n: i64 = r
                .strip_prefix(':')
                .and_then(|r| r.trim_end().parse().ok())
                .unwrap_or_else(|| panic!("MEMORY USAGE {k}: {r:?}"));
            assert!(n > 0, "{k}: {n}");
        }
    }
    assert_eq!(
        send_and_read_reply(&mut c, b"MEMORY USAGE nokey\r\n"),
        "$-1\r\n"
    );
    drop(srv);
}

/// A live AOF rewrite over spilled (tiered) values keeps the shard
/// responsive (each spilled value is read back from disk; a step stops at
/// its budget, also inside a segment), serves a blocked BLMOVE onto a
/// spilled list without falling back to a blocking pass (INFO
/// aof_rewrite_fallbacks stays 0), and loses nothing: the restart from the
/// AOF matches DEBUG DIGEST.
#[test]
fn test_live_rewrite_over_spilled_values_e2e() {
    let port = 18053u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-tierrw-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        "2",
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
    for i in 0..20000 {
        pipe.push_str(&format!(
            "RPUSH l{i} {}{i}\r\nSET s{i} {}{i}\r\n",
            "x".repeat(100),
            "v".repeat(200)
        ));
    }
    c.write_all(pipe.as_bytes()).unwrap();
    let mut replies = vec![0u8; 20000 * (4 + 5)];
    c.read_exact(&mut replies).unwrap();
    assert!(send_and_read_reply(&mut c, b"TIER SPILLALL\r\n").starts_with(':'));
    // Slow the walk so the rewrite is still building its image below.
    assert_eq!(
        send_and_read_reply(&mut c, b"CONFIG SET rdb-key-save-delay 2000\r\n"),
        "+OK\r\n"
    );
    let mut blocked = connect();
    blocked
        .write_all(b"BLMOVE src l19999 LEFT RIGHT 0\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    assert!(send_and_read_reply(&mut c, b"BGREWRITEAOF\r\n").starts_with('+'));
    thread::sleep(Duration::from_millis(50));
    assert!(
        send_and_read_reply(&mut c, b"INFO persistence\r\n").contains("aof_rewrite_in_progress:1")
    );
    assert_eq!(
        send_and_read_reply(&mut c, b"LPUSH src moved\r\n"),
        ":1\r\n"
    );
    assert_eq!(read_full_reply(&mut blocked), "$5\r\nmoved\r\n");
    let mut worst = Duration::ZERO;
    for i in 0..200 {
        let cmd = format!(
            "LPUSH l{} new{i}\r\nAPPEND s{} z\r\n",
            i * 13 % 20000,
            i * 7 % 20000
        );
        for line in cmd.split_inclusive("\r\n") {
            let t = std::time::Instant::now();
            send_and_read_reply(&mut c, line.as_bytes());
            worst = worst.max(t.elapsed());
        }
    }
    eprintln!("worst command during the rewrite: {worst:?}");
    // A whole segment of spilled values read back in one step took ~50 ms
    // here (debug build); now a step stops at its budget.
    assert!(
        worst < Duration::from_millis(25),
        "a command took {worst:?} during the rewrite"
    );
    assert_eq!(
        send_and_read_reply(&mut c, b"CONFIG SET rdb-key-save-delay 0\r\n"),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let info = send_and_read_reply(&mut c, b"INFO persistence\r\n");
        if info.contains("aof_rewrite_in_progress:0") {
            assert!(info.contains("aof_last_bgrewrite_status:ok"), "{info}");
            assert!(info.contains("aof_rewrite_fallbacks:0\r\n"), "{info}");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rewrite never finished"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let digest = send_and_read_reply(&mut c, b"DEBUG DIGEST\r\n");
    drop(srv);
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = connect();
    assert_eq!(
        send_and_read_reply(&mut c, b"DEBUG DIGEST\r\n"),
        digest,
        "restart from AOF"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `maxmemory-samples` is validated like Redis (1..=64) and read back.
#[test]
fn test_maxmemory_samples_config_e2e() {
    let port = 29741;
    start_test_server(port, 2);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    for bad in ["0", "65", "x"] {
        let r = resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory-samples", bad]);
        assert!(
            r.starts_with("-ERR") && r.contains("between 1 and 64"),
            "{bad}: {r}"
        );
    }
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory-samples", "64"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "maxmemory-samples"]),
        "*2\r\n$17\r\nmaxmemory-samples\r\n$2\r\n64\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory-samples", "5"]),
        "+OK\r\n"
    );
}

/// `oom-score-adj` sets the process's `/proc/<pid>/oom_score_adj` from
/// `oom-score-adj-values` (master value here), at startup and on CONFIG
/// SET, as in Redis.
#[test]
fn test_oom_score_adj_sets_the_process_score_e2e() {
    let port = 29751;
    let mut child = rudis_bin()
        .args([
            "--port",
            &port.to_string(),
            "--threads",
            "1",
            "--no-pin",
            "--oom-score-adj",
            "absolute",
            "--oom-score-adj-values",
            "300 400 800",
        ])
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rudis");
    let score = || {
        std::fs::read_to_string(format!("/proc/{}/oom_score_adj", child.id()))
            .unwrap()
            .trim()
            .to_string()
    };
    let mut c = None;
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            c = Some(s);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let mut c = c.expect("server never listened");
    assert_eq!(score(), "300");
    // Raising the score needs no privilege (lowering it may).
    assert_eq!(
        resp_cmd(
            &mut c,
            &["CONFIG", "SET", "oom-score-adj-values", "500 600 800"]
        ),
        "+OK\r\n"
    );
    assert_eq!(score(), "500");
    let r = resp_cmd(&mut c, &["CONFIG", "SET", "oom-score-adj-values", "1 2"]);
    assert!(r.starts_with("-ERR"), "{r}");
    let r = resp_cmd(&mut c, &["CONFIG", "SET", "oom-score-adj", "sometimes"]);
    assert!(r.starts_with("-ERR"), "{r}");
    assert_eq!(score(), "500");
    let _ = child.kill();
    let _ = child.wait();
}

/// Client eviction isn't implemented, so `maxmemory-clients` only accepts
/// 0 (off): a limit that would be silently ignored is refused.
#[test]
fn test_maxmemory_clients_refuses_a_limit_e2e() {
    let port = 29771;
    start_test_server(port, 2);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    for limit in ["1gb", "10%"] {
        let r = resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory-clients", limit]);
        assert!(
            r.contains("client eviction is not supported"),
            "{limit}: {r}"
        );
    }
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory-clients", "0"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "maxmemory-clients"]),
        "*2\r\n$17\r\nmaxmemory-clients\r\n$1\r\n0\r\n"
    );
}

/// `/metrics` breaks keys and memory down by shard and reports the
/// process RSS and mailbox overflows.
#[test]
fn test_metrics_report_per_shard_keys_and_memory_e2e() {
    let (port, metrics_port) = (29791u16, 29792u16);
    let mut child = rudis_bin()
        .args(["--port", &port.to_string(), "--threads", "2", "--no-pin"])
        .args(["--metrics-port", &metrics_port.to_string(), "--save", ""])
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rudis");
    let mut c = None;
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            c = Some(s);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let mut c = c.expect("server never listened");
    for i in 0..200 {
        assert_eq!(resp_cmd(&mut c, &["SET", &format!("k{i}"), "v"]), "+OK\r\n");
    }
    let mut m = TcpStream::connect(("127.0.0.1", metrics_port)).unwrap();
    m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    m.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();
    let mut body = String::new();
    let _ = m.read_to_string(&mut body);
    let _ = child.kill();
    let _ = child.wait();
    let gauge = |name: &str| -> u64 {
        body.lines()
            .find_map(|l| l.strip_prefix(name)?.trim().parse().ok())
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
    };
    let (k0, k1) = (
        gauge("rudis_shard_keys{shard=\"0\"}"),
        gauge("rudis_shard_keys{shard=\"1\"}"),
    );
    assert_eq!(k0 + k1, 200);
    assert!(k0 > 0 && k1 > 0, "keys spread over both shards: {k0} {k1}");
    assert!(gauge("rudis_shard_used_memory_bytes{shard=\"0\"}") > 0);
    assert!(gauge("rudis_process_resident_memory_bytes") > 0);
    gauge("rudis_mailbox_overflows_total");
}
