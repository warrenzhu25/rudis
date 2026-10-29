//! End-to-end tests: persistence (see `tests/common` for the helpers).

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
        "$13\r\nrudis_is_fast\r\n"
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
fn test_graceful_shutdown_command_and_worker_exit_e2e() {
    let port = 16711;
    rudis::shutdown::reset_shutdown();
    sweep_dead_test_server_dirs();
    let dir = std::env::temp_dir().join(format!("rudis-srv-{}-{}", std::process::id(), port));
    std::fs::create_dir_all(&dir).unwrap();
    let aof_config = rudis::aof::AofConfig {
        dir,
        ..Default::default()
    };

    // The real binary defaults to `no`, as Redis; tests use DEBUG.

    rudis::netsec::set_enable_debug_command(port, rudis::netsec::ProtectedAction::Yes);

    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(2);
    let mut handles = Vec::new();

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
        let shard_aof_config = aof_config.clone();
        let handle = thread::Builder::new()
            .stack_size(rudis::server::SHARD_THREAD_STACK_SIZE)
            .spawn(move || {
                run_shard_worker(
                    shard_id,
                    2,
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
            b"JSON.SET doc:ext_1 $ '{\"title\":\"test\",\"rating\":5}'\r\n"
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
        send_and_read(
            &mut client1,
            b"JSON.SET doc1 $ '{\"val\":\"compacted\"}'\r\n"
        ),
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
            b"JSON.SET doc_snap $ '{\"status\":\"persisted\",\"tier\":1}'\r\n"
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
fn test_semantic_cache_llm_workload_e2e() {
    let port = 19145;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    let format_resp_cmd = |args: &[&str]| -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        out
    };

    // 1. Insert semantic cache entries into namespace "llm:gpt4o"
    let set1 = format_resp_cmd(&[
        "SEMANTIC.SET",
        "llm:gpt4o",
        "entry:1",
        "How does Rudis achieve sub-millisecond tail latency?",
        "Rudis uses a thread-per-core shared-nothing architecture on io_uring.",
        "VECTOR",
        "4",
        "1.0",
        "0.0",
        "0.0",
        "0.0",
        "SCOPE",
        "tenant:alpha",
        "QUANTIZE",
        "TOKENS",
        "64",
    ]);
    assert_eq!(send_and_read(&mut client, &set1), "+OK\r\n");

    let set2 = format_resp_cmd(&[
        "SEMANTIC.SET",
        "llm:gpt4o",
        "entry:2",
        "Short-lived prompt",
        "Ephemeral LLM completion",
        "VECTOR",
        "4",
        "0.0",
        "1.0",
        "0.0",
        "0.0",
        "PX",
        "30",
        "SCOPE",
        "tenant:alpha",
        "TOKENS",
        "20",
    ]);
    assert_eq!(send_and_read(&mut client, &set2), "+OK\r\n");

    // Wait for entry:2 to expire
    thread::sleep(Duration::from_millis(50));

    // 2. Query near entry:1 with matching scope -> direct bulk string response hit
    let get_hit = format_resp_cmd(&[
        "SEMANTIC.GET",
        "llm:gpt4o",
        "VECTOR",
        "4",
        "0.99",
        "0.05",
        "0.0",
        "0.0",
        "THRESHOLD",
        "0.95",
        "SCOPE",
        "tenant:alpha",
    ]);
    assert_eq!(
        send_and_read(&mut client, &get_hit),
        "$69\r\nRudis uses a thread-per-core shared-nothing architecture on io_uring.\r\n"
    );

    // 3. Query near entry:1 with WITHSCORE WITHPROMPT WITHID -> 4-element array hit
    let get_rich = format_resp_cmd(&[
        "SEMANTIC.GET",
        "llm:gpt4o",
        "VECTOR",
        "4",
        "0.99",
        "0.05",
        "0.0",
        "0.0",
        "THRESHOLD",
        "0.95",
        "SCOPE",
        "tenant:alpha",
        "WITHSCORE",
        "WITHPROMPT",
        "WITHID",
    ]);
    let rich_resp = send_and_read(&mut client, &get_rich);
    assert!(
        rich_resp.starts_with(
            "*4\r\n$69\r\nRudis uses a thread-per-core shared-nothing architecture on io_uring.\r\n"
        ) && rich_resp.contains("How does Rudis achieve sub-millisecond tail latency?")
            && rich_resp.contains("entry:1"),
        "Unexpected rich SEMANTIC.GET response: {}",
        rich_resp
    );

    // 4. Query with wrong scope -> miss ($-1)
    let get_wrong_scope = format_resp_cmd(&[
        "SEMANTIC.GET",
        "llm:gpt4o",
        "VECTOR",
        "4",
        "0.99",
        "0.05",
        "0.0",
        "0.0",
        "THRESHOLD",
        "0.95",
        "SCOPE",
        "tenant:beta",
    ]);
    assert_eq!(send_and_read(&mut client, &get_wrong_scope), "$-1\r\n");

    // 5. Query expired entry:2 -> miss ($-1)
    let get_expired = format_resp_cmd(&[
        "SEMANTIC.GET",
        "llm:gpt4o",
        "VECTOR",
        "4",
        "0.0",
        "1.0",
        "0.0",
        "0.0",
        "THRESHOLD",
        "0.90",
        "SCOPE",
        "tenant:alpha",
    ]);
    assert_eq!(send_and_read(&mut client, &get_expired), "$-1\r\n");

    // 6. Inspect SEMANTIC.INFO telemetry (2 hits, 2 misses, 128 tokens_saved, 1 evicted_expired)
    let info_cmd = format_resp_cmd(&["SEMANTIC.INFO", "llm:gpt4o"]);
    let info_resp = send_and_read(&mut client, &info_cmd);
    assert!(
        info_resp.contains("entries\r\n:1\r\n")
            && info_resp.contains("hits\r\n:2\r\n")
            && info_resp.contains("misses\r\n:2\r\n")
            && info_resp.contains("tokens_saved\r\n:128\r\n")
            && info_resp.contains("evicted_expired\r\n:1\r\n"),
        "Unexpected SEMANTIC.INFO response: {}",
        info_resp
    );

    // 7. Delete entry:1 and flush namespace
    let del_cmd = format_resp_cmd(&["SEMANTIC.DEL", "llm:gpt4o", "entry:1", "nonexistent"]);
    assert_eq!(send_and_read(&mut client, &del_cmd), ":1\r\n");

    let flush_cmd = format_resp_cmd(&["SEMANTIC.FLUSH", "llm:gpt4o"]);
    assert_eq!(send_and_read(&mut client, &flush_cmd), "+OK\r\n");
}

#[test]
fn test_startup_refuses_unloadable_rdb_and_aof_e2e() {
    let dir = std::env::temp_dir().join(format!("rudis-badload-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap();

    // 1. An RDB rudis cannot load (valid checksum; a Redis module value)
    // must not load as empty.
    let mut rdb = b"REDIS0011\xFA\x09redis-ver\x058.1.0\xFE\x00\x07\x01k\x00\xFF".to_vec();
    let crc = rudis::table::crc64(&rdb);
    rdb.extend_from_slice(&crc.to_le_bytes());
    let rdb_path = dir.join("dump.rdb");
    std::fs::write(&rdb_path, &rdb).unwrap();
    let (status, stderr) = run_rudis_until_exit(&[
        "--port",
        "16988",
        "--threads",
        "2",
        "--no-pin",
        "--aof-dir",
        dir_s,
    ]);
    assert_eq!(status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("FATAL") && stderr.contains("RDB"),
        "{stderr}"
    );
    assert_eq!(
        std::fs::read(&rdb_path).unwrap(),
        rdb,
        "dump.rdb must be untouched"
    );

    // 2. An unreadable AOF (here: a directory) must not start an empty server.
    std::fs::create_dir_all(dir.join("appendonly-0.aof")).unwrap();
    let (status, stderr) = run_rudis_until_exit(&[
        "--port",
        "16988",
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir_s,
    ]);
    assert_eq!(status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("FATAL") && stderr.contains("AOF"),
        "{stderr}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_aof_truncated_tail_is_repaired_and_later_writes_survive_e2e() {
    let port = 16986;
    let dir = std::env::temp_dir().join(format!("rudis-aoftail-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap();
    let aof = dir.join("appendonly-0.aof");
    // A complete SET followed by a command cut off by a crash.
    let mut content = b"*3\r\n$3\r\nSET\r\n$6\r\nbefore\r\n$1\r\n1\r\n".to_vec();
    content.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\nlost");
    std::fs::write(&aof, &content).unwrap();
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
        dir_s,
    ];

    // aof-load-truncated no: refuse to start and leave the file alone.
    let conf = dir.join("strict.conf");
    std::fs::write(&conf, "aof-load-truncated no\n").unwrap();
    let mut strict = args.to_vec();
    strict.extend_from_slice(&["-c", conf.to_str().unwrap()]);
    let (status, stderr) = run_rudis_until_exit(&strict);
    assert_eq!(status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("aof-load-truncated"), "{stderr}");
    assert_eq!(std::fs::read(&aof).unwrap(), content);

    // Default (yes): load, cut the tail, and keep appending readable commands.
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["GET", "before"]), "$1\r\n1\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "after", "2"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["GET", "before"]), "$1\r\n1\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "after"]), "$1\r\n2\r\n");
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "lost"]), ":0\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_aof_ttls_do_not_survive_downtime_e2e() {
    let port = 16985;
    let dir = std::env::temp_dir().join(format!("rudis-aofttl-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "keep", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "e1", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["PEXPIRE", "e1", "400"]), ":1\r\n");
    let r = resp_cmd(&mut c, &["MSETEX", "1", "e2", "v", "PX", "400"]);
    assert!(r == "+OK\r\n" || r == ":1\r\n", "{r}");
    assert_eq!(
        resp_cmd(&mut c, &["HSET", "h", "f", "v", "g", "w"]),
        ":2\r\n"
    );
    assert_eq!(
        resp_cmd(&mut c, &["HPEXPIRE", "h", "400", "FIELDS", "1", "f"]),
        "*1\r\n:1\r\n"
    );
    drop(c);
    shutdown_and_wait(port, &mut child);

    // Downtime longer than every TTL: nothing may come back with a fresh TTL.
    thread::sleep(Duration::from_millis(800));

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "keep"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "e1"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "e2"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["HEXISTS", "h", "f"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["HEXISTS", "h", "g"]), ":1\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_aof_survives_thread_count_changes_e2e() {
    let port = 16984;
    let dir = std::env::temp_dir().join(format!("rudis-aofreshard-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let dir_s = dir.to_str().unwrap().to_string();
    let args = |threads: &'static str| {
        vec![
            "--port".to_string(),
            port_s.clone(),
            "--threads".to_string(),
            threads.to_string(),
            "--no-pin".to_string(),
            "--aof".to_string(),
            "true".to_string(),
            "--aof-dir".to_string(),
            dir_s.clone(),
        ]
    };
    let start = |threads: &'static str| {
        let a = args(threads);
        let a: Vec<&str> = a.iter().map(String::as_str).collect();
        spawn_rudis_listening(&a, port)
    };
    let check_all = |c: &mut TcpStream, n: usize| {
        assert_eq!(resp_cmd(c, &["DBSIZE"]), format!(":{}\r\n", n + 2));
        for k in 0..n {
            let key = format!("key:{}", k);
            let want = format!("v{}", k);
            assert_eq!(
                resp_cmd(c, &["GET", &key]),
                format!("${}\r\n{}\r\n", want.len(), want),
                "{key}"
            );
        }
        assert_eq!(resp_cmd(c, &["LLEN", "list"]), ":3\r\n");
        assert_eq!(resp_cmd(c, &["HGET", "hash", "f"]), "$1\r\nv\r\n");
    };

    let mut child = start("4");
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    for k in 0..200 {
        let (key, val) = (format!("key:{}", k), format!("v{}", k));
        assert_eq!(resp_cmd(&mut c, &["SET", &key, &val]), "+OK\r\n");
    }
    assert_eq!(
        resp_cmd(&mut c, &["RPUSH", "list", "a", "b", "c"]),
        ":3\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["HSET", "hash", "f", "v"]), ":1\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);

    // Fewer threads: keys from the dropped shard files must not vanish.
    let mut child = start("2");
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    check_all(&mut c, 200);
    drop(c);
    shutdown_and_wait(port, &mut child);

    // More threads: keys must be found in their new owning shards.
    let mut child = start("8");
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    check_all(&mut c, 200);
    for k in 200..250 {
        let (key, val) = (format!("key:{}", k), format!("v{}", k));
        assert_eq!(resp_cmd(&mut c, &["SET", &key, &val]), "+OK\r\n");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);

    // Same count again: plain replay, including writes made after the reshard.
    let mut child = start("8");
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    check_all(&mut c, 250);
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_failed_save_does_not_block_later_saves_e2e() {
    let port = 16983;
    let dir = std::env::temp_dir().join(format!("rudis-savefail-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "k", "v"]), "+OK\r\n");
    // dump.rdb is a directory, so publishing the snapshot fails.
    std::fs::create_dir_all(dir.join("dump.rdb")).unwrap();
    for _ in 0..2 {
        let r = resp_cmd(&mut c, &["SAVE"]);
        assert!(r.starts_with("-ERR") && !r.contains("in progress"), "{r}");
    }
    // The dir also holds the tier files; only temp RDBs matter here.
    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("temp-"))
        .collect();
    assert!(leftovers.is_empty(), "temp file left: {leftovers:?}");
    std::fs::remove_dir(dir.join("dump.rdb")).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    assert!(dir.join("dump.rdb").is_file());
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_sigterm_and_shutdown_save_when_save_points_configured_e2e() {
    let port = 16982;
    let dir = std::env::temp_dir().join(format!("rudis-shutsave-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("save.conf");
    std::fs::write(&conf, "save 3600 1\n").unwrap();
    let port_s = port.to_string();
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
    let rdb = dir.join("dump.rdb");
    // SAFETY: (for the closure's unsafe block) kill(2) takes no pointers; the pid
    // is our own child, not yet reaped, so it cannot have been reused.
    let sigterm = |child: &std::process::Child| unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    };

    // 1. SIGTERM snapshots before exiting, and the data comes back.
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "a", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "b", "2"]), "+OK\r\n");
    drop(c);
    sigterm(&child);
    let status = wait_exit(&mut child, Duration::from_secs(20)).expect("SIGTERM must stop rudis");
    assert!(status.success(), "{status}");
    assert!(rdb.is_file(), "SIGTERM must save when save points are set");

    // 2. Plain SHUTDOWN saves too; SHUTDOWN NOSAVE does not.
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["MGET", "a", "b"]),
        "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["SET", "c", "3"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["GET", "c"]), "$1\r\n3\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "d", "4"]), "+OK\r\n");
    let _ = c.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n");
    drop(c);
    wait_exit(&mut child, Duration::from_secs(20)).expect("SHUTDOWN NOSAVE must stop rudis");
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "d"]), ":0\r\n");

    // 3. If the save fails, neither SHUTDOWN nor SIGTERM may exit.
    std::fs::remove_file(&rdb).unwrap();
    std::fs::create_dir_all(&rdb).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["SHUTDOWN"]),
        "-ERR Errors trying to SHUTDOWN. Check logs.\r\n"
    );
    sigterm(&child);
    assert!(
        wait_exit(&mut child, Duration::from_secs(1)).is_none(),
        "exited without saving"
    );
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    std::fs::remove_dir(&rdb).unwrap();
    // Once the problem is fixed, a new signal saves and exits.
    sigterm(&child);
    let status = wait_exit(&mut child, Duration::from_secs(20)).expect("SIGTERM must stop rudis");
    assert!(status.success(), "{status}");
    assert!(rdb.is_file());
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_shutdown_without_save_points_writes_no_rdb_e2e() {
    let port = 16981;
    let dir = std::env::temp_dir().join(format!("rudis-shutnosave-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof-dir",
        dir.to_str().unwrap(),
        // Autosave off: by default (Redis's save points) shutdown saves.
        "--save",
        "",
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "a", "1"]), "+OK\r\n");
    drop(c);
    // SAFETY: kill(2) takes no pointers; the pid is our own child, not yet reaped
    // (`wait_exit` below), so it cannot have been reused.
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    wait_exit(&mut child, Duration::from_secs(20)).expect("SIGTERM must stop rudis");
    assert!(!dir.join("dump.rdb").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_save_points_trigger_background_saves_and_config_is_truthful_e2e() {
    let port = 17108;
    let dir = std::env::temp_dir().join(format!("rudis-savesched-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("save.conf");
    std::fs::write(&conf, "save 1 2\n").unwrap();
    let port_s = port.to_string();
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
    let rdb = dir.join("dump.rdb");
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "save"]),
        "*2\r\n$4\r\nsave\r\n$3\r\n1 2\r\n"
    );
    assert_eq!(info_field(&mut c, "rdb_changes_since_last_save"), "0");
    assert_ne!(info_field(&mut c, "rdb_last_save_time"), "0");

    // One change is below the threshold: no save.
    assert_eq!(resp_cmd(&mut c, &["SET", "a", "1"]), "+OK\r\n");
    thread::sleep(Duration::from_millis(1500));
    assert!(!rdb.exists());
    assert_eq!(info_field(&mut c, "rdb_changes_since_last_save"), "1");
    // The second change reaches "save 1 2".
    assert_eq!(resp_cmd(&mut c, &["SET", "b", "2"]), "+OK\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !rdb.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "scheduled save never ran"
        );
        thread::sleep(Duration::from_millis(50));
    }
    thread::sleep(Duration::from_millis(200));
    assert_eq!(info_field(&mut c, "rdb_changes_since_last_save"), "0");
    assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "ok");

    // CONFIG SET save: invalid values are rejected, "" disables saving.
    assert!(resp_cmd(&mut c, &["CONFIG", "SET", "save", "1"]).starts_with("-ERR"));
    assert!(resp_cmd(&mut c, &["CONFIG", "SET", "save", "1 x"]).starts_with("-ERR"));
    assert_eq!(resp_cmd(&mut c, &["CONFIG", "SET", "save", ""]), "+OK\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "save"]),
        "*2\r\n$4\r\nsave\r\n$0\r\n\r\n"
    );
    let mtime = std::fs::metadata(&rdb).unwrap().modified().unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "c", "3"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "d", "4"]), "+OK\r\n");
    thread::sleep(Duration::from_millis(1500));
    assert_eq!(std::fs::metadata(&rdb).unwrap().modified().unwrap(), mtime);
    assert_eq!(info_field(&mut c, "rdb_changes_since_last_save"), "2");
    drop(c);
    let _ = child.kill();
    let _ = child.wait();

    // The scheduled snapshot is loadable.
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(
        resp_cmd(&mut c, &["MGET", "a", "b"]),
        "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"
    );
    drop(c);
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_bgrewriteaof_under_load_neither_loses_nor_duplicates_writes_e2e() {
    // INCR is not idempotent, so after a restart every counter must equal the
    // last value the client saw: lower means writes were lost during a
    // rewrite, higher means some were replayed twice.
    let port = 16976;
    let dir = std::env::temp_dir().join(format!("rudis-rewrite-e2e-{}", std::process::id()));
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
    const KEYS: usize = 16;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut batch = String::new();
            for k in 0..KEYS {
                let key = format!("rw:{}", k);
                batch.push_str(&format!(
                    "*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n",
                    key.len(),
                    key
                ));
            }
            let mut last = vec![0i64; KEYS];
            let mut buf = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                c.write_all(batch.as_bytes()).unwrap();
                buf.clear();
                let mut chunk = [0u8; 4096];
                while buf.iter().filter(|&&b| b == b'\n').count() < KEYS {
                    let n = c.read(&mut chunk).unwrap();
                    assert!(n > 0, "connection closed");
                    buf.extend_from_slice(&chunk[..n]);
                }
                let text = String::from_utf8_lossy(&buf);
                for (k, line) in text.lines().enumerate() {
                    last[k] = line.strip_prefix(':').expect(line).parse().unwrap();
                }
            }
            last
        })
    };
    let mut admin = TcpStream::connect(("127.0.0.1", port)).unwrap();
    admin
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    for _ in 0..25 {
        thread::sleep(Duration::from_millis(60));
        let r = resp_cmd(&mut admin, &["BGREWRITEAOF"]);
        assert!(r.starts_with('+') || r.contains("in progress"), "{r}");
    }
    thread::sleep(Duration::from_millis(200));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let last = writer.join().unwrap();
    assert!(last.iter().all(|&v| v > 0));
    // Let any in-flight rewrite finish before shutting down.
    thread::sleep(Duration::from_millis(300));
    drop(admin);
    shutdown_and_wait(port, &mut child);

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    for (k, want) in last.iter().enumerate() {
        let got = resp_cmd(&mut c, &["GET", &format!("rw:{}", k)]);
        let want = want.to_string();
        assert_eq!(
            got,
            format!("${}\r\n{}\r\n", want.len(), want),
            "counter rw:{} after restart",
            k
        );
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_failed_bgrewriteaof_is_reported_and_does_not_block_later_ones_e2e() {
    let port = 16975;
    let dir = std::env::temp_dir().join(format!("rudis-rewritefail-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
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
    // Killed even if an assertion fails, so a failed run does not leave a
    // server on the port for the next one.
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
    assert_eq!(resp_cmd(&mut c, &["SET", "rwf:k", "v"]), "+OK\r\n");
    assert_eq!(info_field(&mut c, "aof_enabled"), "1");
    // BGREWRITEAOF marks the rewrite in progress before it replies, and a
    // finished rewrite has its status set and accepts the next one.
    let wait_rewrite = |c: &mut TcpStream| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while info_field(c, "aof_rewrite_in_progress") != "0" {
            assert!(
                std::time::Instant::now() < deadline,
                "rewrite never finished"
            );
            thread::sleep(Duration::from_millis(20));
        }
        info_field(c, "aof_last_bgrewrite_status")
    };

    // The AOF path is now a directory, so publishing the rewrite fails.
    let aof = dir.join("appendonly-0.aof");
    std::fs::remove_file(&aof).unwrap();
    std::fs::create_dir(&aof).unwrap();
    for _ in 0..2 {
        let r = resp_cmd(&mut c, &["BGREWRITEAOF"]);
        assert!(
            r.starts_with('+'),
            "a failed rewrite must not block the next: {r}"
        );
        assert_eq!(wait_rewrite(&mut c), "err");
    }
    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "temp files left: {leftovers:?}");

    std::fs::remove_dir(&aof).unwrap();
    assert!(resp_cmd(&mut c, &["BGREWRITEAOF"]).starts_with('+'));
    assert_eq!(wait_rewrite(&mut c), "ok");
    assert_eq!(resp_cmd(&mut c, &["SET", "rwf:after", "2"]), "+OK\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child.0);

    let mut child = KillOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    assert_eq!(resp_cmd(&mut c, &["GET", "rwf:k"]), "$1\r\nv\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "rwf:after"]), "$1\r\n2\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child.0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_getex_ttl_change_reaches_aof_e2e() {
    let port = 16966;
    let dir = std::env::temp_dir().join(format!("rudis-getex-aof-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];
    let expiring: Vec<String> = (0..6).map(|i| format!("gx:e{i}")).collect();
    let persisting: Vec<String> = (0..6).map(|i| format!("gx:p{i}")).collect();

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for k in &expiring {
        assert_eq!(resp_cmd(&mut c, &["SET", k, "v"]), "+OK\r\n");
    }
    for k in &persisting {
        assert_eq!(resp_cmd(&mut c, &["SET", k, "v", "EX", "1000"]), "+OK\r\n");
    }
    // One write so the GETEXs run through the pipeline batching path.
    let mut pipeline = String::new();
    let mut push = |args: &[&str]| {
        pipeline.push_str(&format!("*{}\r\n", args.len()));
        for a in args {
            pipeline.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
    };
    for k in &expiring {
        push(&["GETEX", k, "EX", "1000"]);
    }
    for k in &persisting {
        push(&["GETEX", k, "PERSIST"]);
    }
    push(&["PING"]);
    c.write_all(pipeline.as_bytes()).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while !got.ends_with(b"+PONG\r\n") {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    drop(c);
    shutdown_and_wait(port, &mut child);

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    for k in &expiring {
        let ttl = resp_cmd(&mut c, &["TTL", k]);
        let secs: i64 = ttl.trim_start_matches(':').trim().parse().unwrap();
        assert!(secs > 900, "{k}: TTL {ttl:?}");
    }
    for k in &persisting {
        assert_eq!(resp_cmd(&mut c, &["TTL", k]), ":-1\r\n", "{k}");
    }
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_dbfilename_is_used_for_save_and_load_e2e() {
    let port = 16958;
    let dir = std::env::temp_dir().join(format!("rudis-dbfilename-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("rudis.conf");
    std::fs::write(
        &conf,
        format!("dir {}\ndbfilename cache.rdb\n", dir.display()),
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
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "dbfilename"]),
        "*2\r\n$10\r\ndbfilename\r\n$9\r\ncache.rdb\r\n"
    );
    let canon = std::fs::canonicalize(&dir).unwrap().display().to_string();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "dir"]),
        format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", canon.len(), canon)
    );
    assert_eq!(resp_cmd(&mut c, &["SET", "dbf:k", "v"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    assert!(dir.join("cache.rdb").is_file());
    assert!(!dir.join("dump.rdb").exists());
    assert!(
        resp_cmd(&mut c, &["CONFIG", "SET", "dbfilename", "../escape.rdb"]).starts_with("-ERR")
    );
    drop(c);
    let _ = child.kill();
    let _ = child.wait();

    // The snapshot is loaded back from the configured file name.
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["GET", "dbf:k"]), "$1\r\nv\r\n");
    drop(c);
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_save_restores_tiered_keys_with_their_types_e2e() {
    let port = 17071;
    let dir = std::env::temp_dir().join(format!("rudis-tier-rdb-e2e-{}", std::process::id()));
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
    assert_eq!(
        resp_cmd(&mut c, &["SET", "t:s", "hello", "EX", "1000"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["HSET", "t:h", "f", "v"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["ZADD", "t:z", "2.5", "m"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["RPUSH", "t:l", "a", "b"]), ":2\r\n");
    for k in ["t:s", "t:h", "t:z", "t:l"] {
        assert_eq!(resp_cmd(&mut c, &["TIER", "SPILL", k]), ":1\r\n", "{k}");
    }
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    drop(c);
    let _ = child.kill();
    let _ = child.wait();

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["GET", "t:s"]), "$5\r\nhello\r\n");
    let ttl = resp_cmd(&mut c, &["TTL", "t:s"]);
    let ttl: i64 = ttl.trim_start_matches(':').trim().parse().unwrap();
    assert!(ttl > 900, "ttl {ttl}");
    assert_eq!(resp_cmd(&mut c, &["HGET", "t:h", "f"]), "$1\r\nv\r\n");
    assert_eq!(resp_cmd(&mut c, &["ZSCORE", "t:z", "m"]), "$3\r\n2.5\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["LRANGE", "t:l", "0", "-1"]),
        "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
    drop(c);
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_bgrewriteaof_keeps_tiered_keys_with_their_types_e2e() {
    let port = 17072;
    let dir = std::env::temp_dir().join(format!("rudis-tier-aof-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "ta:s", "hello"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["HSET", "ta:h", "f", "v"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["ZADD", "ta:z", "2.5", "m"]), ":1\r\n");
    for k in ["ta:s", "ta:h", "ta:z"] {
        assert_eq!(resp_cmd(&mut c, &["TIER", "SPILL", k]), ":1\r\n", "{k}");
    }
    assert!(resp_cmd(&mut c, &["BGREWRITEAOF"]).starts_with('+'));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while info_field(&mut c, "aof_rewrite_in_progress") != "0" {
        assert!(
            std::time::Instant::now() < deadline,
            "rewrite never finished"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(info_field(&mut c, "aof_last_bgrewrite_status"), "ok");
    drop(c);
    shutdown_and_wait(port, &mut child);

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["GET", "ta:s"]), "$5\r\nhello\r\n");
    assert_eq!(resp_cmd(&mut c, &["HGET", "ta:h", "f"]), "$1\r\nv\r\n");
    assert_eq!(resp_cmd(&mut c, &["ZSCORE", "ta:z", "m"]), "$3\r\n2.5\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_unreadable_tiered_value_fails_saves_and_keeps_old_files_e2e() {
    let port = 17190;
    let dir = std::env::temp_dir().join(format!("rudis-tier-savefail-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
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
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _child = KillOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Bigger than the tier's small-value bins, so it is read from the file.
    let big = "v".repeat(64 * 1024);
    assert_eq!(resp_cmd(&mut c, &["SET", "sf:big", &big]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SET", "sf:small", "s"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["TIER", "SPILL", "sf:big"]), ":1\r\n");

    // Complete files from before the fault.
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    let wait_rewrite = |c: &mut TcpStream| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while info_field(c, "aof_rewrite_in_progress") != "0" {
            assert!(
                std::time::Instant::now() < deadline,
                "rewrite never finished"
            );
            thread::sleep(Duration::from_millis(20));
        }
        info_field(c, "aof_last_bgrewrite_status")
    };
    assert!(resp_cmd(&mut c, &["BGREWRITEAOF"]).starts_with('+'));
    assert_eq!(wait_rewrite(&mut c), "ok");
    let rdb = dir.join("dump.rdb");
    let aof = dir.join("appendonly-0.aof");
    let rdb_before = std::fs::read(&rdb).unwrap();
    let aof_before = std::fs::read(&aof).unwrap();
    assert!(rdb_before.len() > 64 * 1024);
    assert!(aof_before.len() > 64 * 1024);

    // A real fault: the tier backing file loses its data.
    let tier_file = dir
        .join(format!("rudis_tier_{}", port))
        .join("tier_shard_0.db");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&tier_file)
        .unwrap()
        .set_len(0)
        .unwrap();

    let reply = resp_cmd(&mut c, &["SAVE"]);
    assert!(reply.starts_with("-ERR"), "{reply}");
    assert!(reply.contains("sf:big"), "{reply}");
    assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "err");
    assert_eq!(std::fs::read(&rdb).unwrap(), rdb_before);

    assert!(resp_cmd(&mut c, &["BGSAVE"]).starts_with('+'));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while info_field(&mut c, "rdb_bgsave_in_progress") != "0" {
        assert!(
            std::time::Instant::now() < deadline,
            "bgsave never finished"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "err");
    assert_eq!(std::fs::read(&rdb).unwrap(), rdb_before);

    assert!(resp_cmd(&mut c, &["BGREWRITEAOF"]).starts_with('+'));
    assert_eq!(wait_rewrite(&mut c), "err");
    assert_eq!(std::fs::read(&aof).unwrap(), aof_before);

    // A replica's full sync fails rather than ship a partial dataset.
    let mut r = TcpStream::connect(("127.0.0.1", port)).unwrap();
    r.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let reply = send_and_read(&mut r, b"SYNC\r\n");
    assert!(reply.starts_with("-ERR full sync failed"), "{reply}");
    drop(r);

    // The server stays up and still serves the in-memory key.
    assert_eq!(resp_cmd(&mut c, &["GET", "sf:small"]), "$1\r\ns\r\n");
    drop(c);
    drop(_child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_dump_of_tiered_key_over_maxmemory_is_restorable_e2e() {
    let port = 17074;
    let port_s = port.to_string();
    let mut child = spawn_rudis_listening(&["--port", &port_s, "--threads", "1", "--no-pin"], port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["HSET", "td:h", "f", "v"]), ":1\r\n");
    assert_eq!(resp_cmd(&mut c, &["TIER", "SPILL", "td:h"]), ":1\r\n");
    // Over maxmemory the key can't be loaded back, so DUMP must work from
    // the tier record itself.
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "1"]),
        "+OK\r\n"
    );
    let dump = send_and_read_bytes(&mut c, b"*2\r\n$4\r\nDUMP\r\n$4\r\ntd:h\r\n");
    assert_eq!(dump[0], b'$', "{:?}", String::from_utf8_lossy(&dump));
    let nl = dump.iter().position(|&b| b == b'\n').unwrap();
    let len: usize = std::str::from_utf8(&dump[1..nl - 1])
        .unwrap()
        .parse()
        .unwrap();
    let payload = &dump[nl + 1..nl + 1 + len];
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "SET", "maxmemory", "0"]),
        "+OK\r\n"
    );
    let mut restore =
        format!("*4\r\n$7\r\nRESTORE\r\n$5\r\ntd:h2\r\n$1\r\n0\r\n${len}\r\n").into_bytes();
    restore.extend_from_slice(payload);
    restore.extend_from_slice(b"\r\n");
    assert_eq!(send_and_read(&mut c, &restore), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["HGET", "td:h2", "f"]), "$1\r\nv\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_json_writes_survive_restart_through_the_aof_e2e() {
    let port = 17080;
    let port_s = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-json-aof-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir_s,
        "--enable-experimental-commands",
        "yes",
    ];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let doc = r#"{"n":1,"a":[1,2,3],"s":"x","b":false,"gone":1}"#;
    assert_eq!(resp_cmd(&mut c, &["JSON.SET", "jd", "$", doc]), "+OK\r\n");
    for cmd in [
        &["JSON.NUMINCRBY", "jd", "$.n", "2"][..],
        &["JSON.NUMMULTBY", "jd", "$.n", "10"],
        &["JSON.ARRAPPEND", "jd", "$.a", "4"],
        &["JSON.ARRPOP", "jd", "$.a", "0"],
        &["JSON.STRAPPEND", "jd", "$.s", r#""y""#],
        &["JSON.TOGGLE", "jd", "$.b"],
        &["JSON.DEL", "jd", "$.gone"],
        &["JSON.SET", "jd2", "$", "[1]"],
        &["JSON.CLEAR", "jd2", "$"],
    ] {
        let reply = resp_cmd(&mut c, cmd);
        assert!(!reply.starts_with('-'), "{cmd:?}: {reply}");
    }
    let before = resp_cmd(&mut c, &["JSON.GET", "jd"]);
    let before2 = resp_cmd(&mut c, &["JSON.GET", "jd2"]);
    assert!(before.contains(r#""n":30"#), "{before}");
    drop(c);
    {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
            .unwrap();
    }
    let _ = child.wait();
    // Nothing but the AOF can bring the documents back.
    assert!(!dir.join("dump.rdb").exists());

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["JSON.GET", "jd"]), before);
    assert_eq!(resp_cmd(&mut c, &["JSON.GET", "jd2"]), before2);
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_probabilistic_writes_survive_restart_through_the_aof_e2e() {
    probabilistic_writes_survive_restart(17081, false);
}

#[test]
fn test_probabilistic_writes_survive_aof_rewrite_and_restart_e2e() {
    probabilistic_writes_survive_restart(17082, true);
}

#[test]
fn test_crdt_writes_survive_restart_through_the_aof_e2e() {
    crdt_writes_survive_restart(17085, false);
    // Same port, run after the first server has stopped.
    crdt_keys_stay_on_their_shard(17085);
}

#[test]
fn test_crdt_writes_survive_aof_rewrite_and_restart_e2e() {
    crdt_writes_survive_restart(17086, true);
}

#[test]
fn test_bgsave_does_not_stall_shards_e2e() {
    // BGSAVE used to serialize each shard in one go and CRC/write the whole
    // dump on the coordinating shard, stalling its clients for the entire
    // save (~1.4 s for 2M keys). Clients must keep being served, and keys
    // written meanwhile must not break the snapshot.
    let port = 17104;
    let dir = std::env::temp_dir().join(format!("rudis-bgsave-stall-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("rudis.conf");
    std::fs::write(&conf, format!("dir {}\nsave \"\"\n", dir.display())).unwrap();
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
    // Killed even if an assertion fails: with SO_REUSEPORT a leaked server
    // would silently share the port with the next run's.
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let child = KillOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();

    const KEYS: usize = 400_000;
    const BATCH: usize = 1000;
    let val = "v".repeat(100);
    let mut out = Vec::new();
    for start in (0..KEYS).step_by(BATCH) {
        out.extend_from_slice(format!("*{}\r\n$4\r\nMSET\r\n", 1 + 2 * BATCH).as_bytes());
        for i in start..start + BATCH {
            let k = format!("base:{i}");
            out.extend_from_slice(format!("${}\r\n{k}\r\n$100\r\n{val}\r\n", k.len()).as_bytes());
        }
    }
    c.write_all(&out).unwrap();
    let (mut oks, mut buf) = (0, [0u8; 65536]);
    while oks < KEYS / BATCH {
        let n = c.read(&mut buf).unwrap();
        assert!(n > 0, "server closed during fill");
        oks += buf[..n].iter().filter(|&&b| b == b'+').count();
    }
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), format!(":{KEYS}\r\n"));

    let rdb = dir.join("dump.rdb");
    let started = std::time::Instant::now();
    assert_eq!(
        resp_cmd(&mut c, &["BGSAVE"]),
        "+Background saving started\r\n"
    );
    let (mut worst, mut during) = (Duration::ZERO, 0usize);
    while !rdb.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "BGSAVE did not finish"
        );
        let t = std::time::Instant::now();
        assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
        assert_eq!(
            resp_cmd(&mut c, &["SET", &format!("during:{during}"), "x"]),
            "+OK\r\n"
        );
        worst = worst.max(t.elapsed());
        during += 1;
    }
    let save_time = started.elapsed();
    // Only meaningful when the save takes a while; on a stalled shard the
    // worst round trip is about as long as the whole save.
    if save_time > Duration::from_millis(150) {
        assert!(
            worst < save_time / 3,
            "a round trip took {worst:?} during a {save_time:?} BGSAVE"
        );
    }
    drop(c);
    drop(child);

    // Every key that existed for the whole save is in the snapshot; keys
    // written during it may or may not be.
    let child = KillOnDrop(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let dbsize = loop {
        let r = resp_cmd(&mut c, &["DBSIZE"]);
        if !r.starts_with("-LOADING") {
            break r;
        }
        assert!(std::time::Instant::now() < deadline, "still loading");
        thread::sleep(Duration::from_millis(100));
    };
    let n: usize = dbsize.trim_start_matches(':').trim().parse().unwrap();
    assert!(
        (KEYS..=KEYS + during).contains(&n),
        "dbsize {n}, during {during}"
    );
    let mut base = 0;
    for i in (0..KEYS).step_by(997) {
        assert_eq!(
            resp_cmd(&mut c, &["EXISTS", &format!("base:{i}")]),
            ":1\r\n",
            "base:{i}"
        );
        base += 1;
    }
    assert!(base > 400);
    drop(c);
    drop(child);
    let _ = std::fs::remove_dir_all(&dir);
}

/// RESTORE payloads come from clients: forged element counts and corrupt
/// probabilistic state must be refused without taking the server down.
#[test]
fn test_restore_forged_payloads_are_refused_e2e() {
    let port: u16 = 17087;
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
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let send = |c: &mut TcpStream, args: &[&[u8]]| {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        send_and_read(c, &out)
    };
    // A list, set and hash claiming 2^32 - 1 elements with none present,
    // sealed with version 10 and a zero (unchecked) CRC.
    for ty in [1u8, 2, 4] {
        let mut payload = vec![ty, 0xFF, 0xFF, 0xFF, 0xFF, 10, 0];
        payload.extend_from_slice(&[0; 8]);
        let reply = send(&mut c, &[b"RESTORE", b"k", b"0", &payload]);
        assert!(reply.starts_with('-'), "type {ty}: {reply:?}");
    }
    // A Bloom filter whose every lookup would run 2^31 hash functions.
    let mut bloom = Vec::new();
    bloom.extend_from_slice(&100u64.to_le_bytes());
    bloom.extend_from_slice(&0.01f64.to_bits().to_le_bytes());
    bloom.extend_from_slice(&64u64.to_le_bytes());
    bloom.extend_from_slice(&(1u32 << 31).to_le_bytes());
    bloom.extend_from_slice(&0u64.to_le_bytes());
    bloom.extend_from_slice(&1u32.to_le_bytes());
    bloom.extend_from_slice(&0u64.to_le_bytes());
    let reply = send(&mut c, &[b"BF.RESTORE", b"bf", &bloom]);
    assert!(reply.starts_with('-'), "{reply:?}");
    assert_eq!(send(&mut c, &[b"BF.EXISTS", b"bf", b"x"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child.0);
}

#[test]
fn test_restore_accepts_a_zero_crc_and_rejects_a_wrong_one_e2e() {
    let port: u16 = 17089;
    let port_s = port.to_string();
    let args = ["--port", &port_s, "--threads", "2", "--no-pin"];
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(resp_cmd(&mut c, &["SET", "crc:src", "v"]), "+OK\r\n");
    let reply = send_and_read_bytes(&mut c, &format_resp_cmd(&["DUMP", "crc:src"]));
    let header_end = reply.iter().position(|&b| b == b'\n').unwrap() + 1;
    let dumped = reply[header_end..reply.len() - 2].to_vec();
    let restore = |c: &mut TcpStream, key: &str, payload: &[u8]| {
        let mut cmd = format!(
            "*4\r\n$7\r\nRESTORE\r\n${}\r\n{key}\r\n$1\r\n0\r\n",
            key.len()
        )
        .into_bytes();
        cmd.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
        cmd.extend_from_slice(payload);
        cmd.extend_from_slice(b"\r\n");
        send_and_read(c, &cmd)
    };
    let crc_at = dumped.len() - 8;

    let mut no_crc = dumped.clone();
    no_crc[crc_at..].fill(0);
    assert_eq!(restore(&mut c, "crc:zero", &no_crc), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "crc:zero"]), "$1\r\nv\r\n");

    let mut bad_crc = dumped;
    bad_crc[crc_at] ^= 1;
    assert_eq!(
        restore(&mut c, "crc:bad", &bad_crc),
        "-ERR DUMP payload version or checksum are wrong\r\n"
    );
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "crc:bad"]), ":0\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
}

#[test]
fn test_flushall_clears_every_key_family_and_stays_flushed_after_aof_replay_e2e() {
    let port: u16 = 17087;
    let dir = std::env::temp_dir().join(format!("rudis-flushfam-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port_s = port.to_string();
    let args = [
        "--port",
        &port_s,
        "--threads",
        "2",
        "--no-pin",
        "--enable-experimental-commands",
        "yes",
        "--aof",
        "true",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];
    let present = [
        (&["GET", "fam:s"][..], "$1\r\n1\r\n"),
        (&["JSON.GET", "fam:j"][..], "$7\r\n{\"a\":1}\r\n"),
        (&["BF.EXISTS", "fam:bf", "x"][..], ":1\r\n"),
        (&["CRDT.GET", "fam:r"][..], "$1\r\nv\r\n"),
        (&["CRDT.SMEMBERS", "fam:cs"][..], "*1\r\n$1\r\nm\r\n"),
        (&["CRDT.INCRBY", "fam:c", "0"][..], ":5\r\n"),
    ];
    let gone = [
        (&["GET", "fam:s"][..], "$-1\r\n"),
        (&["JSON.GET", "fam:j"][..], "$-1\r\n"),
        (&["BF.EXISTS", "fam:bf", "x"][..], ":0\r\n"),
        (&["CRDT.GET", "fam:r"][..], "$-1\r\n"),
        (&["CRDT.SMEMBERS", "fam:cs"][..], "*0\r\n"),
        (&["CRDT.INCRBY", "fam:c", "0"][..], ":0\r\n"),
    ];
    let check = |c: &mut TcpStream, want: &[(&[&str], &str)], when: &str| {
        for (cmd, reply) in want {
            assert_eq!(resp_cmd(c, cmd), *reply, "{when}: {cmd:?}");
        }
    };

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for cmd in [
        &["SET", "fam:s", "1"][..],
        &["JSON.SET", "fam:j", "$", "{\"a\":1}"],
        &["BF.ADD", "fam:bf", "x"],
        &["CRDT.SET", "fam:r", "v"],
        &["CRDT.SADD", "fam:cs", "m"],
        &["CRDT.INCRBY", "fam:c", "5"],
    ] {
        let r = resp_cmd(&mut c, cmd);
        assert!(!r.starts_with('-'), "{cmd:?}: {r}");
    }
    check(&mut c, &present, "written");
    drop(c);
    shutdown_and_wait(port, &mut child);

    // The AOF carries every family, so the flush below has something to undo.
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    check(&mut c, &present, "replayed");
    assert_eq!(resp_cmd(&mut c, &["FLUSHALL", "ASYNC"]), "+OK\r\n");
    check(&mut c, &gone, "flushed");
    drop(c);
    shutdown_and_wait(port, &mut child);

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    check(&mut c, &gone, "flushed then replayed");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_bgsave_and_bgrewriteaof_exclusive_across_shards_e2e() {
    let port = 17098;
    let dir = std::env::temp_dir().join(format!("rudis-bgsave-lock-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let args = [
        "--port",
        "17098",
        "--threads",
        "2",
        "--no-pin",
        "--aof",
        "true",
        "--aof-dir",
        dir.to_str().unwrap(),
    ];
    let mut child = spawn_rudis_listening(&args, port);

    let mut c1 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut c2 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c1.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    // Populate enough keys and delay saves so the background operation stays
    // in flight long enough to test exclusivity across both shard threads.
    for i in 0..100 {
        assert_eq!(
            resp_cmd(&mut c1, &["SET", &format!("k:{i}"), "val"]),
            "+OK\r\n"
        );
    }
    assert_eq!(
        resp_cmd(&mut c1, &["CONFIG", "SET", "rdb-key-save-delay", "10"]),
        "+OK\r\n"
    );

    assert_eq!(
        resp_cmd(&mut c1, &["BGSAVE"]),
        "+Background saving started\r\n"
    );

    // Another client (even on another shard thread) must be refused
    let err_save = resp_cmd(&mut c2, &["BGSAVE"]);
    assert_eq!(
        err_save, "-ERR Background save already in progress\r\n",
        "{err_save}"
    );
    let err_rw = resp_cmd(&mut c2, &["BGREWRITEAOF"]);
    assert_eq!(
        err_rw, "-ERR Background save or rewrite already in progress\r\n",
        "{err_rw}"
    );

    // Clear the artificial delay and wait for the save to finish
    assert_eq!(
        resp_cmd(&mut c1, &["CONFIG", "SET", "rdb-key-save-delay", "0"]),
        "+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while resp_cmd(&mut c1, &["INFO", "persistence"]).contains("rdb_bgsave_in_progress:1") {
        assert!(std::time::Instant::now() < deadline, "save did not finish");
        thread::sleep(Duration::from_millis(20));
    }

    // Now a rewrite can start without conflict
    assert_eq!(
        resp_cmd(&mut c2, &["BGREWRITEAOF"]),
        "+Background append only file rewriting started\r\n"
    );
    while resp_cmd(&mut c1, &["INFO", "persistence"]).contains("aof_rewrite_in_progress:1") {
        assert!(
            std::time::Instant::now() < deadline,
            "rewrite did not finish"
        );
        thread::sleep(Duration::from_millis(20));
    }

    drop(c1);
    drop(c2);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Regression: each accept loop re-checks for shutdown every 200 ms. It used
/// to drop the in-flight io_uring accept on that tick, and a connection the
/// kernel accepted in the same instant was discarded: it stayed established
/// but was never served, so the client hung on its first command. The race
/// needs the shards busy (completions reaped late), so half the workers keep
/// them busy with cross-shard pipelines while the other half open short-lived
/// connections across many ticks. Every one of those must be answered.
#[test]
fn test_connections_accepted_at_shutdown_poll_tick_are_served_e2e() {
    let port = 17990;
    start_test_server(port, 4);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    // Reads until `lines` reply lines have arrived (each SET is one, each GET two).
    fn read_lines(c: &mut TcpStream, lines: usize, what: &str) {
        let mut got = Vec::new();
        let mut buf = [0u8; 16384];
        while got.iter().filter(|&&b| b == b'\n').count() < lines {
            let n = c
                .read(&mut buf)
                .unwrap_or_else(|e| panic!("{what}: no reply on {:?} ({e})", c.local_addr()));
            assert!(n > 0, "{what}: closed");
            got.extend_from_slice(&buf[..n]);
        }
    }
    let workers: Vec<_> = (0..16)
        .map(|w| {
            thread::spawn(move || {
                let mut pipeline = Vec::new();
                for j in 0..32 {
                    pipeline.extend_from_slice(
                        format!("SET tick:{w}:{j} v\r\nGET tick:{w}:{j}\r\n").as_bytes(),
                    );
                }
                let connect = || {
                    let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
                    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    c
                };
                let mut c = connect();
                let mut fresh = 0u64;
                while std::time::Instant::now() < deadline {
                    if w % 2 == 0 {
                        c = connect();
                        c.write_all(b"PING\r\n").unwrap();
                        read_lines(
                            &mut c,
                            1,
                            &format!("worker {w}: fresh connection after {fresh} served"),
                        );
                        fresh += 1;
                    }
                    c.write_all(&pipeline).unwrap();
                    read_lines(&mut c, 32 * 3, &format!("worker {w}: pipeline"));
                }
                fresh
            })
        })
        .collect();
    let fresh: u64 = workers.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(fresh > 0);
}

#[test]
fn test_debug_reload_and_loadaof_keep_data_e2e() {
    // RDB: DEBUG RELOAD saves, flushes and loads back the same data.
    let port = 18403;
    start_test_server(port, 2);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    load_digest_dataset(&mut c, false);
    let before = debug_digest(&mut c);
    let size = resp_cmd_all(&mut c, &["DBSIZE"]);
    assert_eq!(resp_cmd_all(&mut c, &["DEBUG", "RELOAD"]), "+OK\r\n");
    assert_eq!(debug_digest(&mut c), before);
    assert_eq!(resp_cmd_all(&mut c, &["DBSIZE"]), size);
    // NOSAVE reloads the file saved above, dropping later changes.
    resp_cmd_all(&mut c, &["SET", "after-save", "x"]);
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "RELOAD", "NOSAVE"]),
        "+OK\r\n"
    );
    assert_eq!(debug_digest(&mut c), before);

    // AOF: DEBUG LOADAOF replays the AOF into the same data.
    let port = 18404;
    let dir = std::env::temp_dir().join(format!("rudis-srv-{}-{}", std::process::id(), port));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    start_test_server_with_aof(
        port,
        2,
        rudis::aof::AofConfig {
            enabled: true,
            dir,
            fsync_every_sec: false,
        },
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    load_digest_dataset(&mut c, true);
    let before = debug_digest(&mut c);
    let size = resp_cmd_all(&mut c, &["DBSIZE"]);
    assert_eq!(resp_cmd_all(&mut c, &["DEBUG", "LOADAOF"]), "+OK\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["DBSIZE"]), size);
    assert_eq!(debug_digest(&mut c), before);
    // Replaying did not append to the AOF: a second load is identical.
    assert_eq!(resp_cmd_all(&mut c, &["DEBUG", "LOADAOF"]), "+OK\r\n");
    assert_eq!(debug_digest(&mut c), before);
    assert_eq!(
        resp_cmd_all(&mut c, &["LRANGE", "list", "0", "-1"]),
        "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
}

#[test]
fn test_redis7_multipart_aof_is_imported_once_e2e() {
    let port = 17960u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-redis7-aof-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let aof_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&aof_dir).unwrap();

    // Base: a Redis-format RDB. Incr: what Redis 7 appends after it.
    let mut base = rudis::shard::ShardDb::new(0);
    base.set(
        bytes::Bytes::from_static(b"base:k"),
        bytes::Bytes::from_static(b"base:v"),
        None,
    );
    let mut rdb = Vec::new();
    rudis::redis_rdb::write_file_header(&mut rdb, 0);
    base.save_rdb_chunk(&mut rdb).unwrap();
    rdb.push(rudis::redis_rdb::OP_EOF);
    let crc = rudis::table::crc64(&rdb);
    rdb.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(aof_dir.join("appendonly.aof.1.base.rdb"), &rdb).unwrap();
    let mut incr = b"#TS:1700000000\r\n".to_vec();
    for cmd in [
        &["MULTI"][..],
        &["SET", "tx:a", "1"],
        &["INCR", "tx:a"],
        &["EXEC"],
        &["SELECT", "3"],
        &["SET", "db3:k", "v3"],
        &["SELECT", "0"],
        &["RPUSH", "gone", "a"],
        &["PEXPIREAT", "gone", "1000"],
        &["RPUSH", "gone", "b"],
    ] {
        incr.extend_from_slice(&resp_argv_bytes(cmd));
    }
    std::fs::write(aof_dir.join("appendonly.aof.1.incr.aof"), &incr).unwrap();
    std::fs::write(
        aof_dir.join("appendonly.aof.manifest"),
        "file appendonly.aof.1.base.rdb seq 1 type b\nfile appendonly.aof.1.incr.aof seq 1 type i\n",
    )
    .unwrap();

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
    ];
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c
    };

    let mut child = spawn_rudis_listening(&args, port);
    let mut c = connect();
    assert_eq!(resp_cmd(&mut c, &["GET", "base:k"]), "$6\r\nbase:v\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "tx:a"]), "$1\r\n2\r\n");
    assert_eq!(resp_cmd(&mut c, &["EXISTS", "gone"]), ":0\r\n");
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":2\r\n");
    assert_eq!(resp_cmd(&mut c, &["SELECT", "3"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "db3:k"]), "$2\r\nv3\r\n");
    assert_eq!(resp_cmd(&mut c, &["SELECT", "0"]), "+OK\r\n");
    // Written by rudis after the import: must survive restarts.
    assert_eq!(resp_cmd(&mut c, &["SET", "after", "x"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["INCR", "tx:a"]), ":3\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_file(dir.join("dump.rdb"));

    // Restart: the unchanged Redis files are not imported again.
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = connect();
    assert_eq!(resp_cmd(&mut c, &["GET", "after"]), "$1\r\nx\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "tx:a"]), "$1\r\n3\r\n");
    assert_eq!(resp_cmd(&mut c, &["GET", "base:k"]), "$6\r\nbase:v\r\n");
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_auto_aof_rewrite_triggers_on_growth_e2e() {
    let port = 17968u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-auto-aofrw-e2e-{}", std::process::id()));
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
        "--save",
        "",
    ];
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        c
    };
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = connect();
    assert_eq!(
        resp_cmd(&mut c, &["CONFIG", "GET", "auto-aof-rewrite-percentage"]),
        "*2\r\n$27\r\nauto-aof-rewrite-percentage\r\n$3\r\n100\r\n"
    );
    assert_eq!(
        resp_cmd(
            &mut c,
            &["CONFIG", "SET", "auto-aof-rewrite-min-size", "200kb"]
        ),
        "+OK\r\n"
    );
    assert_eq!(info_u64(&mut c, "persistence", "aof_rewrites"), 0);
    // Overwrite a few keys many times: the AOF grows, its rewrite is tiny.
    let val = "v".repeat(100);
    for chunk in 0..50 {
        let mut pipe = Vec::new();
        for j in 0..100 {
            let i = chunk * 100 + j;
            let k = format!("k{}", i % 10);
            pipe.extend_from_slice(&format_resp_cmd(&["SET", &k, &format!("{val}{i}")]));
        }
        c.write_all(&pipe).unwrap();
        let mut got = Vec::new();
        while got.windows(5).filter(|w| *w == b"+OK\r\n").count() < 100 {
            let mut b = [0u8; 4096];
            let n = c.read(&mut b).unwrap();
            got.extend_from_slice(&b[..n]);
        }
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while info_u64(&mut c, "persistence", "aof_rewrites") == 0
        || info_u64(&mut c, "persistence", "aof_rewrite_in_progress") == 1
    {
        assert!(
            std::time::Instant::now() < deadline,
            "no automatic AOF rewrite"
        );
        thread::sleep(Duration::from_millis(100));
    }
    let current = info_u64(&mut c, "persistence", "aof_current_size");
    let base = info_u64(&mut c, "persistence", "aof_base_size");
    // The rewrite left ~10 keys (writes after it, if it ran mid-loop,
    // regrow the file, but never back past min-size).
    assert!(base < 10_000, "the rewrite compacted the AOF: base {base}");
    assert!(current < 200 * 1024, "current {current}");
    assert!(base > 0 && base <= current, "base {base} current {current}");
    assert_eq!(
        info_u64(&mut c, "persistence", "aof_rewrites_consecutive_failures"),
        0
    );
    // Below min-size again: no more rewrites.
    thread::sleep(Duration::from_millis(1500));
    assert_eq!(info_u64(&mut c, "persistence", "aof_rewrites"), 1);
    drop(c);
    shutdown_and_wait(port, &mut child);

    // The rewritten AOF replays to the same data.
    let _ = std::fs::remove_file(dir.join("dump.rdb"));
    let mut child = spawn_rudis_listening(&args, port);
    let mut c = connect();
    assert_eq!(resp_cmd(&mut c, &["DBSIZE"]), ":10\r\n");
    assert_eq!(
        resp_cmd(&mut c, &["GET", "k9"]),
        format!("${}\r\n{val}4999\r\n", val.len() + 4)
    );
    drop(c);
    shutdown_and_wait(port, &mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_ops_maxmemory_policy_validation_sd_notify_and_llm_quota_persistence_e2e() {
    use std::os::unix::net::UnixDatagram;

    let port = 17977;
    let port_s = port.to_string();
    let dir = std::env::temp_dir().join(format!(
        "rudis_ops_e2e_{}_{}",
        port,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap().to_string();

    // 1. Bind a UnixDatagram socket for systemd NOTIFY_SOCKET
    let notify_sock_path = dir.join("notify.sock");
    let notify_rx = UnixDatagram::bind(&notify_sock_path).unwrap();
    notify_rx
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 2. Startup with an invalid maxmemory-policy in config file must fail fast
    let bad_conf = dir.join("bad.conf");
    std::fs::write(&bad_conf, "maxmemory-policy definitely-invalid-policy\n").unwrap();
    let bad_status = rudis_bin()
        .args([bad_conf.to_str().unwrap(), "--port", &port_s, "--no-pin"])
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("spawn rudis with invalid config");
    assert!(
        !bad_status.success(),
        "server should reject invalid maxmemory-policy at startup"
    );

    // 3. Start valid server with NOTIFY_SOCKET and experimental commands enabled
    let mut srv = rudis_bin()
        .args([
            "--port",
            &port_s,
            "--dir",
            &dir_s,
            "--threads",
            "1",
            "--no-pin",
            "--enable-experimental-commands",
            "yes",
        ])
        .env("NOTIFY_SOCKET", &notify_sock_path)
        .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rudis");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut c = loop {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            break s;
        }
        assert!(std::time::Instant::now() < deadline, "server did not start");
        thread::sleep(Duration::from_millis(25));
    };
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Verify systemd READY=1 notification was received
    let mut notify_buf = [0u8; 64];
    let n = notify_rx
        .recv(&mut notify_buf)
        .expect("expected READY=1 on NOTIFY_SOCKET");
    assert_eq!(&notify_buf[..n], b"READY=1\n");

    // 4. Runtime CONFIG SET maxmemory-policy validation
    let bad_policy_resp = resp_cmd(
        &mut c,
        &["CONFIG", "SET", "maxmemory-policy", "not-a-valid-policy"],
    );
    assert!(bad_policy_resp.starts_with("-ERR "), "{bad_policy_resp}");
    assert_eq!(
        resp_cmd(
            &mut c,
            &["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"]
        ),
        "+OK\r\n"
    );
    assert!(
        resp_cmd(&mut c, &["CONFIG", "GET", "maxmemory-policy"]).contains("\r\nvolatile-ttl\r\n")
    );

    // 5. Populate LLM.QUOTA state and persist via SAVE
    let reserve_resp = resp_cmd(
        &mut c,
        &[
            "LLM.QUOTA.RESERVE",
            "org:prod:quota",
            "RPM",
            "100",
            "TPM",
            "1000",
            "EST_TOKENS",
            "300",
            "WINDOW",
            "60000",
        ],
    );
    assert!(reserve_resp.contains(":1\r\n"), "{reserve_resp}");
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");

    drop(c);
    shutdown_and_wait(port, &mut srv);

    // Verify systemd STOPPING=1 notification was sent during shutdown
    let n_stop = notify_rx
        .recv(&mut notify_buf)
        .expect("expected STOPPING=1 on NOTIFY_SOCKET");
    assert_eq!(&notify_buf[..n_stop], b"STOPPING=1\n");

    // 6. Restart server from RDB and verify LLM.QUOTA state survived
    let mut srv2 = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--dir",
            &dir_s,
            "--threads",
            "1",
            "--no-pin",
            "--enable-experimental-commands",
            "yes",
        ],
        port,
    );
    let mut c2 = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    let quota_info = resp_cmd(&mut c2, &["LLM.QUOTA.INFO", "org:prod:quota"]);
    assert!(
        quota_info.contains("reserved_tokens\r\n:300\r\n")
            && quota_info.contains("active_reservations\r\n:1\r\n"),
        "LLM.QUOTA state did not survive RDB reload: {quota_info}"
    );

    drop(c2);
    shutdown_and_wait(port, &mut srv2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_documented_config_and_persistence_invariants_e2e() {
    let port = 17981u16;
    let port_s = port.to_string();
    let dir = std::env::temp_dir().join(format!(
        "rudis_doc_invariants_{}_{}",
        std::process::id(),
        port
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_str().unwrap().to_string();

    let mut srv = spawn_rudis_listening(
        &[
            "--port",
            &port_s,
            "--threads",
            "1",
            "--no-pin",
            "--aof",
            "false",
            "--aof-dir",
            &dir_s,
        ],
        port,
    );

    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    // Verify runtime CONFIG SET for dbfilename, repl-backlog-size, and proto-max-bulk-len
    assert_eq!(
        resp_cmd(
            &mut c,
            &[
                "CONFIG",
                "SET",
                "dbfilename",
                "custom_snap.rdb",
                "repl-backlog-size",
                "2097152",
                "proto-max-bulk-len",
                "16777216",
            ]
        ),
        "+OK\r\n"
    );
    assert!(resp_cmd(&mut c, &["CONFIG", "GET", "dbfilename"]).contains("custom_snap.rdb"));
    assert!(resp_cmd(&mut c, &["CONFIG", "GET", "repl-backlog-size"]).contains("2097152"));
    assert!(resp_cmd(&mut c, &["CONFIG", "GET", "proto-max-bulk-len"]).contains("16777216"));

    assert_eq!(resp_cmd(&mut c, &["SET", "doc:key", "doc:val"]), "+OK\r\n");
    assert_eq!(resp_cmd(&mut c, &["SAVE"]), "+OK\r\n");
    assert!(dir.join("custom_snap.rdb").exists());

    let info_p = resp_cmd(&mut c, &["INFO", "persistence"]);
    assert!(
        info_p.contains("rdb_last_bgsave_status:ok")
            && !info_p.contains("rdb_last_save_time:0\r\n"),
        "{info_p}"
    );

    drop(c);
    shutdown_and_wait(port, &mut srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Randomized persistence round trips over every packed encoding: sets
/// (PackedSet), sorted sets (ZIndex), small and large hashes (SmallHash,
/// PackedHash) and lists (PackedList), in states only random operations
/// reach (compacted pages, own-page members over 1 KiB, small hashes promoted,
/// chunks split by inserts). After each phase the data must survive an RDB
/// round trip (DEBUG RELOAD), an AOF rewrite and replay (BGREWRITEAOF +
/// DEBUG LOADAOF) and DUMP/RESTORE, matching both its digest and an
/// in-test model; then mutation continues on the reloaded encodings.
#[test]
fn test_persistence_round_trips_of_packed_encodings_e2e() {
    use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
    let port = 18003u16;
    let dir = std::env::temp_dir().join(format!("rudis-pfuzz-{}-{}", std::process::id(), port));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    start_test_server_with_aof(
        port,
        4,
        rudis::aof::AofConfig {
            enabled: true,
            dir: dir.clone(),
            fsync_every_sec: false,
        },
    );
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();

    #[derive(Debug)]
    enum V {
        Simple(String),
        Int(i64),
        Bulk(Option<Vec<u8>>),
        Arr(Vec<V>),
    }
    fn parse(b: &[u8], i: usize) -> Option<(V, usize)> {
        let nl = i + b.get(i..)?.windows(2).position(|w| w == b"\r\n")?;
        let head = std::str::from_utf8(&b[i + 1..nl]).unwrap();
        match b[i] {
            b'+' | b'-' => Some((V::Simple(head.to_string()), nl + 2)),
            b':' => Some((V::Int(head.parse().unwrap()), nl + 2)),
            b'$' => {
                let n: i64 = head.parse().unwrap();
                if n < 0 {
                    return Some((V::Bulk(None), nl + 2));
                }
                let end = nl + 2 + n as usize + 2;
                (b.len() >= end).then(|| (V::Bulk(Some(b[nl + 2..end - 2].to_vec())), end))
            }
            b'*' => {
                let n: i64 = head.parse().unwrap();
                let mut items = Vec::new();
                let mut j = nl + 2;
                for _ in 0..n.max(0) {
                    let (v, k) = parse(b, j)?;
                    items.push(v);
                    j = k;
                }
                Some((V::Arr(items), j))
            }
            other => panic!("unexpected RESP type {:?}", other as char),
        }
    }
    // Sends commands pipelined and returns their replies.
    fn pipe(c: &mut TcpStream, cmds: &[Vec<Vec<u8>>]) -> Vec<V> {
        let mut out = Vec::new();
        for args in cmds {
            out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                out.extend_from_slice(a);
                out.extend_from_slice(b"\r\n");
            }
        }
        c.write_all(&out).unwrap();
        let (mut got, mut buf, mut replies, mut pos) =
            (Vec::new(), vec![0u8; 1 << 16], Vec::new(), 0);
        while replies.len() < cmds.len() {
            match parse(&got, pos) {
                Some((v, next)) => {
                    replies.push(v);
                    pos = next;
                }
                None => {
                    let n = c.read(&mut buf).unwrap();
                    assert!(n > 0, "connection closed");
                    got.extend_from_slice(&buf[..n]);
                }
            }
        }
        replies
    }
    fn one(c: &mut TcpStream, args: &[&[u8]]) -> V {
        pipe(c, &[args.iter().map(|a| a.to_vec()).collect()])
            .pop()
            .unwrap()
    }
    fn bulks(v: V) -> Vec<Vec<u8>> {
        let V::Arr(items) = v else {
            panic!("expected array, got {v:?}")
        };
        items
            .into_iter()
            .map(|i| match i {
                V::Bulk(Some(b)) => b,
                other => panic!("expected bulk, got {other:?}"),
            })
            .collect()
    }

    #[derive(Default)]
    struct Model {
        sets: HashMap<String, HashSet<Vec<u8>>>,
        zsets: HashMap<String, BTreeMap<Vec<u8>, i64>>,
        hashes: HashMap<String, HashMap<Vec<u8>, Vec<u8>>>,
        lists: HashMap<String, VecDeque<Vec<u8>>>,
    }
    // Member/value bytes: mostly short, some medium, some over 1 KiB.
    fn blob(r: u64, tag: &str) -> Vec<u8> {
        let len = match r % 20 {
            0 => 1100 + (r >> 8) as usize % 400,
            1..=3 => 65 + (r >> 8) as usize % 200,
            _ => 1 + (r >> 8) as usize % 30,
        };
        format!("{tag}{}-", r % 100_000)
            .into_bytes()
            .into_iter()
            .cycle()
            .take(len)
            .collect()
    }
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    // Applies `n` random operations, keeping the model in step.
    let mut mutate = |c: &mut TcpStream, m: &mut Model, n: usize| {
        let mut batch: Vec<Vec<Vec<u8>>> = Vec::new();
        for _ in 0..n {
            let r = next();
            let k = (r >> 40) % 6;
            let b = |s: &str| s.as_bytes().to_vec();
            match r % 4 {
                0 => {
                    // Sets: a few large (packed) and some small.
                    let key = format!("pf:s{}", k);
                    let set = m.sets.entry(key.clone()).or_default();
                    let mem = blob(next() % 3000, "m");
                    if (r >> 8) % 3 == 0 && !set.is_empty() {
                        set.remove(&mem);
                        batch.push(vec![b("SREM"), b(&key), mem]);
                    } else {
                        set.insert(mem.clone());
                        batch.push(vec![b("SADD"), b(&key), mem]);
                    }
                }
                1 => {
                    let key = format!("pf:z{}", k);
                    let z = m.zsets.entry(key.clone()).or_default();
                    let mem = blob(next() % 3000, "z");
                    if (r >> 8) % 4 == 0 {
                        z.remove(&mem);
                        batch.push(vec![b("ZREM"), b(&key), mem]);
                    } else {
                        let score = ((r >> 16) % 200) as i64 - 100;
                        z.insert(mem.clone(), score);
                        batch.push(vec![b("ZADD"), b(&key), b(&score.to_string()), mem]);
                    }
                }
                2 => {
                    // k 0-2: small hashes (5..30-byte values, <=40 fields);
                    // k 3-5: large (long values, many fields).
                    let key = format!("pf:h{}", k);
                    let h = m.hashes.entry(key.clone()).or_default();
                    let (f, v) = if k < 3 {
                        (
                            format!("f{}", next() % 40).into_bytes(),
                            format!("v{}-", next())
                                .into_bytes()
                                .into_iter()
                                .cycle()
                                .take(5 + (r >> 8) as usize % 25)
                                .collect(),
                        )
                    } else {
                        (format!("f{}", next() % 900).into_bytes(), blob(next(), "v"))
                    };
                    if (r >> 8) % 5 == 0 {
                        h.remove(&f);
                        batch.push(vec![b("HDEL"), b(&key), f]);
                    } else {
                        h.insert(f.clone(), v.clone());
                        batch.push(vec![b("HSET"), b(&key), f, v]);
                    }
                }
                _ => {
                    let key = format!("pf:l{}", k);
                    let l = m.lists.entry(key.clone()).or_default();
                    let v = blob(next(), "e");
                    match (r >> 8) % 8 {
                        0..=2 => {
                            l.push_back(v.clone());
                            batch.push(vec![b("RPUSH"), b(&key), v]);
                        }
                        3 | 4 => {
                            l.push_front(v.clone());
                            batch.push(vec![b("LPUSH"), b(&key), v]);
                        }
                        5 if !l.is_empty() => {
                            l.pop_front();
                            batch.push(vec![b("LPOP"), b(&key)]);
                        }
                        6 if !l.is_empty() => {
                            let i = (next() % l.len() as u64) as usize;
                            l[i] = v.clone();
                            batch.push(vec![b("LSET"), b(&key), b(&i.to_string()), v]);
                        }
                        _ if !l.is_empty() => {
                            l.pop_back();
                            batch.push(vec![b("RPOP"), b(&key)]);
                        }
                        _ => {}
                    }
                }
            }
            if batch.len() == 200 {
                pipe(c, &batch);
                batch.clear();
            }
        }
        if !batch.is_empty() {
            pipe(c, &batch);
        }
        m.sets.retain(|_, s| !s.is_empty());
        m.zsets.retain(|_, z| !z.is_empty());
        m.hashes.retain(|_, h| !h.is_empty());
        m.lists.retain(|_, l| !l.is_empty());
    };
    // Full contents of `key` read back under `name` must match the model.
    fn check(c: &mut TcpStream, m: &Model, rename: &dyn Fn(&str) -> String, phase: &str) {
        for (k, want) in &m.sets {
            let got: HashSet<Vec<u8>> = bulks(one(c, &[b"SMEMBERS", rename(k).as_bytes()]))
                .into_iter()
                .collect();
            assert_eq!(&got, want, "{phase}: set {k}");
        }
        for (k, want) in &m.zsets {
            let got = bulks(one(
                c,
                &[b"ZRANGE", rename(k).as_bytes(), b"0", b"-1", b"WITHSCORES"],
            ));
            let mut exp: Vec<(i64, Vec<u8>)> = want.iter().map(|(m, s)| (*s, m.clone())).collect();
            exp.sort();
            let exp: Vec<Vec<u8>> = exp
                .into_iter()
                .flat_map(|(s, m)| [m, s.to_string().into_bytes()])
                .collect();
            assert_eq!(got, exp, "{phase}: zset {k}");
        }
        for (k, want) in &m.hashes {
            let got: HashMap<Vec<u8>, Vec<u8>> = bulks(one(c, &[b"HGETALL", rename(k).as_bytes()]))
                .chunks(2)
                .map(|p| (p[0].clone(), p[1].clone()))
                .collect();
            assert_eq!(&got, want, "{phase}: hash {k}");
        }
        for (k, want) in &m.lists {
            let got = bulks(one(c, &[b"LRANGE", rename(k).as_bytes(), b"0", b"-1"]));
            assert_eq!(got, Vec::from(want.clone()), "{phase}: list {k}");
        }
    }
    let same = |k: &str| k.to_string();
    let digest = |c: &mut TcpStream| debug_digest(c);
    let size = |c: &mut TcpStream| match one(c, &[b"DBSIZE"]) {
        V::Int(n) => n,
        other => panic!("DBSIZE: {other:?}"),
    };

    let mut m = Model::default();
    for phase in 0..3 {
        mutate(&mut c, &mut m, 12_000);
        check(&mut c, &m, &same, &format!("phase {phase} live"));
        if phase == 0 {
            // The workload must reach the packed large forms and keep small
            // hashes small, or the round trips below prove less.
            let enc = |c: &mut TcpStream, k: &str| match one(
                c,
                &[b"OBJECT", b"ENCODING", k.as_bytes()],
            ) {
                V::Bulk(Some(e)) => String::from_utf8(e).unwrap(),
                other => panic!("OBJECT ENCODING {k}: {other:?}"),
            };
            assert!(m.sets.values().all(|s| s.len() > 64), "sets stay small");
            assert_eq!(enc(&mut c, "pf:s0"), "hashtable");
            assert_eq!(enc(&mut c, "pf:z0"), "skiplist");
            assert_eq!(enc(&mut c, "pf:h0"), "listpack");
            assert_eq!(enc(&mut c, "pf:h4"), "hashtable");
            assert!(m.lists.values().any(|l| l.len() > 100), "lists stay short");
        }
        let before = digest(&mut c);
        let n = size(&mut c);
        assert_eq!(
            n as usize,
            m.sets.len() + m.zsets.len() + m.hashes.len() + m.lists.len()
        );

        // RDB: save, flush, load.
        assert!(matches!(one(&mut c, &[b"DEBUG", b"RELOAD"]), V::Simple(ref s) if s == "OK"));
        assert_eq!(
            digest(&mut c),
            before,
            "phase {phase}: digest after DEBUG RELOAD"
        );
        check(
            &mut c,
            &m,
            &same,
            &format!("phase {phase} after RDB reload"),
        );

        // AOF: rewrite, then replay into a flushed keyspace.
        assert!(matches!(one(&mut c, &[b"BGREWRITEAOF"]), V::Simple(_)));
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let V::Bulk(Some(info)) = one(&mut c, &[b"INFO", b"persistence"]) else {
                panic!("INFO persistence")
            };
            if String::from_utf8_lossy(&info).contains("aof_rewrite_in_progress:0") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "AOF rewrite did not finish"
            );
            thread::sleep(Duration::from_millis(50));
        }
        assert!(matches!(one(&mut c, &[b"DEBUG", b"LOADAOF"]), V::Simple(ref s) if s == "OK"));
        assert_eq!(
            digest(&mut c),
            before,
            "phase {phase}: digest after AOF replay"
        );
        check(
            &mut c,
            &m,
            &same,
            &format!("phase {phase} after AOF replay"),
        );

        // DUMP/RESTORE every key under a new name.
        let keys: Vec<String> = m
            .sets
            .keys()
            .chain(m.zsets.keys())
            .chain(m.hashes.keys())
            .chain(m.lists.keys())
            .cloned()
            .collect();
        for k in &keys {
            let V::Bulk(Some(dump)) = one(&mut c, &[b"DUMP", k.as_bytes()]) else {
                panic!("DUMP {k}")
            };
            let copy = format!("{k}:copy");
            let r = one(
                &mut c,
                &[b"RESTORE", copy.as_bytes(), b"0", &dump, b"REPLACE"],
            );
            assert!(
                matches!(r, V::Simple(ref s) if s == "OK"),
                "RESTORE {k}: {r:?}"
            );
        }
        check(
            &mut c,
            &m,
            &|k: &str| format!("{k}:copy"),
            &format!("phase {phase} restored copies"),
        );
        for k in &keys {
            one(&mut c, &[b"DEL", format!("{k}:copy").as_bytes()]);
        }
        assert_eq!(
            digest(&mut c),
            before,
            "phase {phase}: digest after deleting copies"
        );
    }

    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

/// SHUTDOWN (and SIGTERM) under write load with RDB save points: every
/// acknowledged write is in the final snapshot. The snapshot was taken while
/// other clients' writes kept being executed and acknowledged until they were
/// drained (~200 ms later), and those were lost on restart.
#[test]
fn test_shutdown_under_load_keeps_every_acknowledged_write_e2e() {
    let port = 18019u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-quiesce-{}", std::process::id()));
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
        "no",
        "--save",
        "3600 1",
    ];
    let mut srv = spawn_rudis_listening(&args, port);
    // Four writers, each SET w<i>:<n> one batch of 50 at a time, counting
    // fully acknowledged batches.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let stop = stop.clone();
            thread::spawn(move || {
                let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
                c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                let mut acked = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut req = String::new();
                    for n in acked..acked + 50 {
                        let k = format!("w{w}:{n}");
                        req.push_str(&format!(
                            "*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\n1\r\n",
                            k.len()
                        ));
                    }
                    if c.write_all(req.as_bytes()).is_err() {
                        break;
                    }
                    let (mut oks, mut buf) = (0, [0u8; 4096]);
                    while oks < 50 {
                        match c.read(&mut buf) {
                            Ok(n) if n > 0 => {
                                oks += buf[..n].windows(3).filter(|x| x == b"OK\r").count()
                            }
                            _ => return acked,
                        }
                    }
                    acked += 50;
                }
                acked
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(400));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    c.write_all(b"SHUTDOWN\r\n").unwrap();
    let mut buf = [0u8; 64];
    let _ = c.read(&mut buf);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while srv.try_wait().unwrap().is_none() {
        if std::time::Instant::now() > deadline {
            let _ = srv.kill();
            panic!("server did not exit within 20 s of SHUTDOWN");
        }
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let acked: Vec<usize> = writers.into_iter().map(|h| h.join().unwrap()).collect();
    let mut srv = spawn_rudis_listening(&args, port);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    for (w, &n) in acked.iter().enumerate() {
        assert!(n > 0, "writer {w} got nothing acknowledged");
        for i in [0, n / 2, n - 1] {
            let r = send_and_read_reply(&mut c, format!("EXISTS w{w}:{i}\r\n").as_bytes());
            assert_eq!(r, ":1\r\n", "writer {w}: acknowledged w{w}:{i} of {n} lost");
        }
    }
    let _ = srv.kill();
    let _ = srv.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

/// When AOF writes fail (here: a file size limit, as a full disk would),
/// write commands get -MISCONF and INFO says aof_last_write_status:err, like
/// Redis; once writes succeed again, so do commands. The server kept
/// acknowledging writes it could not persist and reported "ok".
#[test]
fn test_aof_write_failure_refuses_writes_with_misconf_e2e() {
    use std::os::unix::process::CommandExt;
    let port = 18020u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-misconf-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cmd = rudis_bin();
    cmd.args([
        "--port",
        &ps,
        "--threads",
        "1",
        "--no-pin",
        "--dir",
        dir.to_str().unwrap(),
        "--appendonly",
        "yes",
        "--appendfsync",
        "everysec",
        "--metrics-port",
        "18120",
    ])
    .stdout(std::process::Stdio::null())
    .stderr(std::process::Stdio::null());
    // SAFETY: only async-signal-safe calls between fork and exec. Files are
    // capped at 256 KB; SIGXFSZ is ignored (kept across exec) so a write past
    // the cap fails with EFBIG instead of killing the server.
    unsafe {
        cmd.pre_exec(|| {
            let lim = libc::rlimit {
                rlim_cur: 256 * 1024,
                rlim_max: libc::RLIM_INFINITY,
            };
            libc::setrlimit(libc::RLIMIT_FSIZE, &lim);
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            Ok(())
        });
    }
    let srv = ServerGuard(cmd.spawn().unwrap());
    let mut c = None;
    for _ in 0..200 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            c = Some(s);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let mut c = c.expect("server up");
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let val = "x".repeat(1000);
    let mut first_err = None;
    for i in 0..600 {
        let r = send_and_read_reply(&mut c, format!("SET k{i} {val}\r\n").as_bytes());
        if r != "+OK\r\n" {
            first_err = Some((i, r));
            break;
        }
    }
    let (at, err) = first_err.expect("writes past the size cap still acknowledged");
    assert!(at > 100, "refused too early, at write {at}");
    assert!(
        err.starts_with("-MISCONF Errors writing to the AOF file"),
        "{err}"
    );
    let info = send_and_read_reply(&mut c, b"INFO persistence\r\n");
    assert!(info.contains("aof_last_write_status:err"), "{info}");
    let metrics = || -> String {
        let mut m = TcpStream::connect(("127.0.0.1", 18120)).unwrap();
        m.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        m.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let mut body = String::new();
        let _ = m.read_to_string(&mut body);
        body
    };
    assert!(
        metrics().contains("rudis_aof_last_write_ok 0"),
        "operators can alert on it"
    );
    // Reads still work.
    assert_eq!(send_and_read_reply(&mut c, b"EXISTS k0\r\n"), ":1\r\n");
    // Lift the cap: the flusher's retry succeeds and writes are accepted.
    let unlimited = libc::rlimit {
        rlim_cur: libc::RLIM_INFINITY,
        rlim_max: libc::RLIM_INFINITY,
    };
    // SAFETY: plain syscall on our own child's pid with valid pointers.
    let rc = unsafe {
        libc::prlimit(
            srv.id() as libc::pid_t,
            libc::RLIMIT_FSIZE,
            &unlimited,
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, 0);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let r = send_and_read_reply(&mut c, b"SET after ok\r\n");
        if r == "+OK\r\n" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "writes still refused: {r}"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let info = send_and_read_reply(&mut c, b"INFO persistence\r\n");
    assert!(info.contains("aof_last_write_status:ok"), "{info}");
    assert!(metrics().contains("rudis_aof_last_write_ok 1"));
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// With save points set and `stop-writes-on-bgsave-error yes` (the default),
/// a failed RDB save makes write commands fail with -MISCONF until a save
/// succeeds, like Redis; `no` lets them through. The option was accepted
/// but ignored: writes kept being acknowledged with snapshots failing.
#[test]
fn test_failed_bgsave_stops_writes_with_misconf_e2e() {
    use std::os::unix::process::CommandExt;
    let port = 18021u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-rdbmisconf-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cmd = rudis_bin();
    cmd.args([
        "--port",
        &ps,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        dir.to_str().unwrap(),
        "--appendonly",
        "no",
        "--save",
        "3600 1000000",
    ])
    .stdout(std::process::Stdio::null())
    .stderr(std::process::Stdio::null());
    // SAFETY: only async-signal-safe calls between fork and exec. Files are
    // capped at 64 KB (SIGXFSZ ignored), so the snapshot below cannot be
    // written.
    unsafe {
        cmd.pre_exec(|| {
            let lim = libc::rlimit {
                rlim_cur: 64 * 1024,
                rlim_max: libc::RLIM_INFINITY,
            };
            libc::setrlimit(libc::RLIMIT_FSIZE, &lim);
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            Ok(())
        });
    }
    let srv = ServerGuard(cmd.spawn().unwrap());
    let mut c = None;
    for _ in 0..200 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            c = Some(s);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let mut c = c.expect("server up");
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let val = "x".repeat(1000);
    for i in 0..300 {
        assert_eq!(
            send_and_read_reply(&mut c, format!("SET k{i} {val}\r\n").as_bytes()),
            "+OK\r\n"
        );
    }
    let wait_status = |c: &mut TcpStream, want: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let info = send_and_read_reply(c, b"INFO persistence\r\n");
            if info.contains(want) && info.contains("rdb_bgsave_in_progress:0") {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "no {want}: {info}");
            thread::sleep(Duration::from_millis(50));
        }
    };
    send_and_read_reply(&mut c, b"BGSAVE\r\n");
    wait_status(&mut c, "rdb_last_bgsave_status:err");
    let r = send_and_read_reply(&mut c, b"SET blocked 1\r\n");
    assert!(
        r.starts_with("-MISCONF Redis is configured to save RDB snapshots"),
        "{r}"
    );
    // Pipelined writes are refused too; reads still work.
    let r = send_and_read_reply(&mut c, b"SET a 1\r\nGET k1\r\n");
    assert!(r.starts_with("-MISCONF"), "{r}");
    assert_eq!(send_and_read_reply(&mut c, b"EXISTS k0\r\n"), ":1\r\n");
    assert_eq!(
        send_and_read_reply(&mut c, b"CONFIG SET stop-writes-on-bgsave-error no\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read_reply(&mut c, b"SET allowed 1\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read_reply(&mut c, b"CONFIG SET stop-writes-on-bgsave-error yes\r\n"),
        "+OK\r\n"
    );
    assert!(send_and_read_reply(&mut c, b"SET blocked 1\r\n").starts_with("-MISCONF"));
    // Lift the cap: a successful save lets writes through again.
    let unlimited = libc::rlimit {
        rlim_cur: libc::RLIM_INFINITY,
        rlim_max: libc::RLIM_INFINITY,
    };
    // SAFETY: plain syscall on our own child's pid with valid pointers.
    let rc = unsafe {
        libc::prlimit(
            srv.id() as libc::pid_t,
            libc::RLIMIT_FSIZE,
            &unlimited,
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, 0);
    send_and_read_reply(&mut c, b"BGSAVE\r\n");
    wait_status(&mut c, "rdb_last_bgsave_status:ok");
    assert_eq!(send_and_read_reply(&mut c, b"SET after 1\r\n"), "+OK\r\n");
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A rewritten AOF stores a TTL as SET ... PXAT <deadline>. Replayed after
/// that deadline it keeps the key with its passed deadline (nothing expires
/// while loading, as in Redis), so a later PERSIST in the AOF still saves
/// it. The SET dropped it outright, and the PERSISTed key was lost.
#[test]
fn test_rewritten_ttl_then_persist_survives_restart_e2e() {
    let port = 18030u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-pxat-{}", std::process::id()));
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
        "--appendfsync",
        "everysec",
    ];
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut c, b"SET k v PX 1000\r\n"),
        "+OK\r\n"
    );
    send_and_read_reply(&mut c, b"BGREWRITEAOF\r\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !send_and_read_reply(&mut c, b"INFO persistence\r\n")
        .contains("aof_rewrite_in_progress:0")
    {
        assert!(std::time::Instant::now() < deadline);
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(send_and_read_reply(&mut c, b"PERSIST k\r\n"), ":1\r\n");
    // Past the deadline stored in the rewritten AOF, then kill -9 (the AOF
    // is written before replies) and restart.
    thread::sleep(Duration::from_millis(1200));
    drop(srv);
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(send_and_read_reply(&mut c, b"GET k\r\n"), "$1\r\nv\r\n");
    assert_eq!(send_and_read_reply(&mut c, b"TTL k\r\n"), ":-1\r\n");
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A hash field past its deadline but not purged yet when a snapshot is
/// taken does not come back as a permanent field after a restart.
#[test]
fn test_expired_hash_field_stays_expired_across_rdb_restart_e2e() {
    let port = 18047u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-hfesave-{}", std::process::id()));
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
        "--enable-debug-command",
        "yes",
    ];
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut c, b"DEBUG SET-ACTIVE-EXPIRE 0\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read_reply(&mut c, b"HSET h f v g w\r\n"), ":2\r\n");
    assert_eq!(
        send_and_read_reply(&mut c, b"HPEXPIRE h 100 FIELDS 1 f\r\n"),
        "*1\r\n:1\r\n"
    );
    thread::sleep(Duration::from_millis(300));
    assert_eq!(send_and_read_reply(&mut c, b"SAVE\r\n"), "+OK\r\n");
    drop(srv);
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut c, b"HGETALL h\r\n"),
        "*2\r\n$1\r\ng\r\n$1\r\nw\r\n"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Function libraries survive BGREWRITEAOF and a restart from the AOF
/// (the rewritten AOF is an RDB image that holds them); before, FUNCTION
/// LIST came back empty and FCALL failed.
#[test]
fn test_functions_survive_aof_rewrite_and_restart_e2e() {
    let port = 18045u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-fnrw-{}", std::process::id()));
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
    ];
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let code =
        "#!lua name=rwlib\nredis.register_function('rwfn', function(keys, args) return 42 end)";
    let load = format!(
        "*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n",
        code.len(),
        code
    );
    assert_eq!(
        send_and_read_reply(&mut c, load.as_bytes()),
        "$5\r\nrwlib\r\n"
    );
    for i in 0..20 {
        let cmd = format!("SET key{i} v{i}\r\n");
        assert_eq!(send_and_read_reply(&mut c, cmd.as_bytes()), "+OK\r\n");
    }
    assert!(send_and_read_reply(&mut c, b"BGREWRITEAOF\r\n").starts_with('+'));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while send_and_read_reply(&mut c, b"INFO persistence\r\n").contains("aof_rewrite_in_progress:1")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "rewrite never finished"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        send_and_read_reply(&mut c, b"SET after rewrite\r\n"),
        "+OK\r\n"
    );
    drop(srv);
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(send_and_read_reply(&mut c, b"FCALL rwfn 0\r\n"), ":42\r\n");
    assert_eq!(send_and_read_reply(&mut c, b"DBSIZE\r\n"), ":21\r\n");
    assert_eq!(
        send_and_read_reply(&mut c, b"GET after\r\n"),
        "$7\r\nrewrite\r\n"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// BGREWRITEAOF no longer stops the shards: while it rewrites ~1M keys,
/// PING keeps answering quickly, and writes of every kind (INCR, LPUSH,
/// MULTI/EXEC, EVAL, a served BLMOVE, RENAME, another DB) keep going; the
/// rewritten AOF then restarts into exactly the live dataset.
#[test]
fn test_bgrewriteaof_does_not_block_and_keeps_every_write_e2e() {
    let port = 18046u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-liverw-{}", std::process::id()));
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
    assert_eq!(
        send_and_read_reply(&mut c, b"DEBUG POPULATE 1000000 key 64\r\n"),
        "+OK\r\n"
    );
    for i in 0..200 {
        let cmd = format!("RPUSH list{i} a b c\r\nSET cnt{i} 0\r\n");
        send_and_read_reply(&mut c, cmd.as_bytes());
    }
    // A client blocked on BLMOVE, served by an LPUSH during the rewrite.
    let mut blocked = connect();
    blocked
        .write_all(b"BLMOVE src dst LEFT RIGHT 0\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    assert!(send_and_read_reply(&mut c, b"BGREWRITEAOF\r\n").starts_with('+'));
    let start = std::time::Instant::now();
    let mut worst = Duration::ZERO;
    let mut i = 0u64;
    let mut ping = connect();
    loop {
        let t = std::time::Instant::now();
        assert_eq!(send_and_read_reply(&mut ping, b"PING\r\n"), "+PONG\r\n");
        worst = worst.max(t.elapsed());
        let k = i % 200;
        let cmds = format!(
            "INCR cnt{k}\r\nLPUSH list{k} x{i}\r\nRENAME key:{i} renamed:{i}\r\nSELECT 1\r\nINCR db1:{k}\r\nSELECT 0\r\nMULTI\r\nINCR cnt{k}\r\nRPOP list{k}\r\nEXEC\r\nEVAL \"return redis.call('INCRBY', KEYS[1], 2)\" 1 cnt{k}\r\n"
        );
        for line in cmds.split_inclusive("\r\n") {
            send_and_read_reply(&mut c, line.as_bytes());
        }
        if i == 50 {
            assert_eq!(
                send_and_read_reply(&mut c, b"LPUSH src moved\r\n"),
                ":1\r\n"
            );
        }
        i += 1;
        if i.is_multiple_of(20)
            && !send_and_read_reply(&mut c, b"INFO persistence\r\n")
                .contains("aof_rewrite_in_progress:1")
        {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(120),
            "rewrite never finished"
        );
    }
    eprintln!("live rewrite: {i} write rounds, worst PING {worst:?}");
    assert!(i > 50, "writes during the rewrite: {i}");
    assert!(
        worst < Duration::from_millis(500),
        "PING took {worst:?} during the rewrite"
    );
    let mut reply = [0u8; 64];
    let n = blocked.read(&mut reply).unwrap();
    assert!(String::from_utf8_lossy(&reply[..n]).contains("moved"));
    assert_eq!(send_and_read_reply(&mut c, b"INCR after\r\n"), ":1\r\n");
    let digest = send_and_read_reply(&mut c, b"DEBUG DIGEST\r\n");
    drop(srv);
    let aof0 = std::fs::read(dir.join("appendonly-0.aof")).unwrap();
    assert!(aof0.starts_with(b"REDIS"), "rewritten as an RDB image");
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

/// BGREWRITEAOF does not block on DBs other than 0 or on probabilistic
/// structures either (they were serialized in one step at its start): with
/// 1M keys in DB 1, PING stays fast while writes to DB 1 (INCR, MOVE),
/// CMS.INCRBY and CF.ADD keep going, and the restart matches.
#[test]
fn test_bgrewriteaof_does_not_block_on_other_dbs_e2e() {
    let port = 18049u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-liverw-db1-{}", std::process::id()));
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
    assert_eq!(send_and_read_reply(&mut c, b"SELECT 1\r\n"), "+OK\r\n");
    assert_eq!(
        send_and_read_reply(&mut c, b"DEBUG POPULATE 1000000 k1 64\r\n"),
        "+OK\r\n"
    );
    assert_eq!(send_and_read_reply(&mut c, b"SELECT 0\r\n"), "+OK\r\n");
    for i in 0..100 {
        let cmds =
            format!("CMS.INITBYDIM cms{i} 1000 5\r\nCF.RESERVE cf{i} 10000\r\nSET cnt{i} 0\r\n");
        for line in cmds.split_inclusive("\r\n") {
            send_and_read_reply(&mut c, line.as_bytes());
        }
    }
    assert!(send_and_read_reply(&mut c, b"BGREWRITEAOF\r\n").starts_with('+'));
    let start = std::time::Instant::now();
    let mut worst = Duration::ZERO;
    let mut ping = connect();
    let mut i = 0u64;
    loop {
        let t = std::time::Instant::now();
        assert_eq!(send_and_read_reply(&mut ping, b"PING\r\n"), "+PONG\r\n");
        worst = worst.max(t.elapsed());
        let k = i % 100;
        let cmds = format!(
            "SELECT 1\r\nINCR k1:{i}\r\nSELECT 0\r\nMOVE cnt{k} 1\r\nSET cnt{k} {i}\r\nCMS.INCRBY cms{k} x 1\r\nCF.ADD cf{k} x{i}\r\n"
        );
        // Every command is timed: a client on the shard a blocking rewrite
        // stops waits in whichever command it sent.
        for line in cmds.split_inclusive("\r\n") {
            let t = std::time::Instant::now();
            send_and_read_reply(&mut c, line.as_bytes());
            worst = worst.max(t.elapsed());
        }
        i += 1;
        if i.is_multiple_of(20)
            && !send_and_read_reply(&mut c, b"INFO persistence\r\n")
                .contains("aof_rewrite_in_progress:1")
        {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(120),
            "rewrite never finished"
        );
    }
    eprintln!("live rewrite (DB 1): {i} write rounds, worst command {worst:?}");
    assert!(i > 40, "writes during the rewrite: {i}");
    assert!(
        worst < Duration::from_millis(100),
        "a command took {worst:?} during the rewrite"
    );
    let digest = send_and_read_reply(&mut c, b"DEBUG DIGEST\r\n");
    let probe = b"CMS.QUERY cms7 x\r\n";
    let cms = send_and_read_reply(&mut c, probe);
    drop(srv);
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = connect();
    assert_eq!(
        send_and_read_reply(&mut c, b"DEBUG DIGEST\r\n"),
        digest,
        "restart from AOF"
    );
    assert_eq!(send_and_read_reply(&mut c, probe), cms);
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A restart doesn't push a dataset that fit under maxmemory over it: the
/// buffers each shard's AOF replay freed used to stay in its allocator
/// cache (~1.5 MB a thread), counted in used_memory, so a restart near the
/// limit evicted keys.
#[test]
fn test_restart_near_maxmemory_keeps_keys_e2e() {
    let port = 18054u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-maxmem-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let base = [
        "--port",
        &ps,
        "--threads",
        "4",
        "--no-pin",
        "--dir",
        &ds,
        "--appendonly",
        "yes",
    ];
    let connect = || {
        let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        c
    };
    let keys = 120_000usize;
    let srv = ServerGuard(spawn_rudis_listening(&base, port));
    let mut c = connect();
    let value = "v".repeat(200);
    for chunk in (0..keys).collect::<Vec<_>>().chunks(10_000) {
        let mut pipe = String::new();
        for i in chunk {
            pipe.push_str(&format!("SET key:{i} {value}\r\n"));
        }
        c.write_all(pipe.as_bytes()).unwrap();
        let mut replies = vec![0u8; chunk.len() * 5];
        c.read_exact(&mut replies).unwrap();
    }
    thread::sleep(Duration::from_millis(500));
    let live_used = info_field_u64(&mut c, "memory", "used_memory");
    drop(c);
    drop(srv);

    // A limit 2.5 MB above what the data used when written live.
    let limit = (live_used + (5 << 19)).to_string();
    let mut args = base.to_vec();
    args.extend(["--maxmemory", &limit, "--maxmemory-policy", "allkeys-lru"]);
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = connect();
    thread::sleep(Duration::from_millis(1500));
    let loaded_used = info_field_u64(&mut c, "memory", "used_memory");
    eprintln!("used_memory live {live_used}, after restart {loaded_used}, limit {limit}");
    assert_eq!(info_field_u64(&mut c, "stats", "evicted_keys"), 0);
    assert_eq!(
        send_and_read_reply(&mut c, b"DBSIZE\r\n"),
        format!(":{keys}\r\n")
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A restart maps the AOF instead of reading it into memory: right after
/// loading ~200 MB, the server's anonymous memory is about the dataset, not
/// the dataset plus the files (freed read buffers lingered for ~10 s, so a
/// restart needed about twice the dataset's memory).
#[test]
fn test_restart_maps_the_aof_e2e() {
    let port = 18064u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-mmap-{}", std::process::id()));
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
    ];
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    // 200 incompressible 1 MB values (as the RDB preamble stores them).
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
        let mut cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\nbig{i}\r\n${}\r\n",
            format!("big{i}").len(),
            value.len()
        )
        .into_bytes();
        cmd.extend_from_slice(&value);
        cmd.extend_from_slice(b"\r\n");
        assert_eq!(send_and_read_reply(&mut c, &cmd), "+OK\r\n");
    }
    // A rewrite (the RDB preamble restarts load from); 200 MB of writes
    // may already have started one on their own.
    let rewriting = |c: &mut TcpStream| {
        send_and_read_reply(c, b"INFO persistence\r\n").contains("aof_rewrite_in_progress:1")
    };
    while rewriting(&mut c) {
        thread::sleep(Duration::from_millis(100));
    }
    assert!(send_and_read_reply(&mut c, b"BGREWRITEAOF\r\n").starts_with('+'));
    while rewriting(&mut c) {
        thread::sleep(Duration::from_millis(100));
    }
    drop(c);
    drop(srv);
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let status = std::fs::read_to_string(format!("/proc/{}/status", srv.0.id())).unwrap();
    let anon_mb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("RssAnon:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .unwrap()
        / 1024;
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(send_and_read_reply(&mut c, b"DBSIZE\r\n"), ":200\r\n");
    eprintln!("anonymous memory right after loading 200 MB: {anon_mb} MB");
    assert!(
        anon_mb < 320,
        "{anon_mb} MB anonymous memory after loading 200 MB"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A restart parses the commands after the AOF's base a chunk at a time:
/// with ~300 MB of commands that keep overwriting one key (and no rewrite
/// to compact them), the server's anonymous memory right after loading is
/// small. The whole tail was copied into memory first.
#[test]
fn test_restart_parses_the_aof_tail_in_chunks_e2e() {
    let port = 18068u16;
    let ps = port.to_string();
    let dir = std::env::temp_dir().join(format!("rudis-e2e-aoftail-{}", std::process::id()));
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
    ];
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut c, b"CONFIG SET auto-aof-rewrite-percentage 0\r\n"),
        "+OK\r\n"
    );
    let value = "v".repeat(1 << 20);
    let cmd = format!(
        "*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${}\r\n{value}\r\n",
        value.len()
    );
    for _ in 0..300 {
        assert_eq!(send_and_read_reply(&mut c, cmd.as_bytes()), "+OK\r\n");
    }
    drop(c);
    drop(srv);
    let srv = ServerGuard(spawn_rudis_listening(&args, port));
    let status = std::fs::read_to_string(format!("/proc/{}/status", srv.0.id())).unwrap();
    let anon_mb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("RssAnon:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .unwrap()
        / 1024;
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    assert_eq!(
        send_and_read_reply(&mut c, b"STRLEN big\r\n"),
        format!(":{}\r\n", 1 << 20)
    );
    eprintln!("anonymous memory right after replaying a 300 MB AOF tail: {anon_mb} MB");
    assert!(
        anon_mb < 120,
        "{anon_mb} MB anonymous memory after replaying a 300 MB tail"
    );
    drop(srv);
    let _ = std::fs::remove_dir_all(&dir);
}

/// With no `save` setting the binary uses Redis's default save points;
/// `--save ""` turns autosave off.
#[test]
fn test_default_save_points_match_redis_e2e() {
    for (port, extra, want) in [
        (29781u16, None, "3600 1 300 100 60 10000"),
        (29782u16, Some(""), ""),
    ] {
        let dir = std::env::temp_dir().join(format!("rudis-srv-{}-{}", std::process::id(), port));
        std::fs::create_dir_all(&dir).unwrap();
        let mut cmd = rudis_bin();
        cmd.args(["--port", &port.to_string(), "--threads", "1", "--no-pin"])
            .args(["--dir", dir.to_str().unwrap()])
            .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if let Some(save) = extra {
            cmd.args(["--save", save]);
        }
        let mut child = cmd.spawn().expect("spawn rudis");
        let mut c = None;
        for _ in 0..100 {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                c = Some(s);
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let mut c = c.expect("server never listened");
        let got = resp_cmd(&mut c, &["CONFIG", "GET", "save"]);
        let _ = child.kill();
        let _ = child.wait();
        assert_eq!(
            got,
            format!("*2\r\n$4\r\nsave\r\n${}\r\n{}\r\n", want.len(), want)
        );
    }
}

/// Corrupt DUMP payloads from Redis's corrupt-dump suite (a ziplist with a
/// wrong prevlen or count, an empty quicklist node, hash field TTLs out of
/// range or out of order) are refused with "Bad data format" rather than
/// stored, and the server keeps serving.
#[test]
fn test_restore_refuses_corrupt_payloads_e2e() {
    // Redis TCL string escapes: \xHH and \t.
    fn unescape(s: &str) -> Vec<u8> {
        let b = s.as_bytes();
        let (mut out, mut i) = (Vec::new(), 0);
        while i < b.len() {
            if b[i] == b'\\' && b[i + 1] == b'x' {
                out.push(u8::from_str_radix(&s[i + 2..i + 4], 16).unwrap());
                i += 4;
            } else if b[i] == b'\\' && b[i + 1] == b't' {
                out.push(b'\t');
                i += 2;
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
        out
    }
    let payloads = [
        // quicklist big ziplist prev len
        r"\x0E\x01\x13\x13\x00\x00\x00\x0E\x00\x00\x00\x02\x00\x00\x02\x61\x00\x0E\x02\x62\x00\xFF\x09\x00\x49\x97\x30\xB2\x0D\xA1\xED\xAA",
        // quicklist small ziplist prev len
        r"\x0E\x01\x13\x13\x00\x00\x00\x0E\x00\x00\x00\x02\x00\x00\x02\x61\x00\x02\x02\x62\x00\xFF\x09\x00\xC7\x71\x03\x97\x07\x75\xB0\x63",
        // quicklist ziplist wrong count
        r"\x0E\x01\x13\x13\x00\x00\x00\x0E\x00\x00\x00\x03\x00\x00\x02\x61\x00\x04\x02\x62\x00\xFF\x09\x00\x4D\xE2\x0A\x2F\x08\x25\xDF\x91",
        // quicklist encoded_len is 0
        r"\x12\x01\x01\x00\x0a\x00\x8f\xc6\xc0\x57\x1c\x0a\xb3\x3c",
        // hash listpackex with TTL larger than EB_EXPIRE_TIME_MAX
        r"\x17\x33\x33\x00\x00\x00\x09\x00\x00\x01\x00\x01\xf4\x01\xc5\x89\x95\x8f\x01\x00\x00\x09\x01\x01\x82\x5f\x31\x03\xf4\x29\x94\x97\x95\x8f\x01\x00\x00\x09\x02\x01\x02\x01\xf4\x01\x5e\xaf\x95\x8f\x01\x33\x00\x09\xff\x0c\x00\x7e\x4f\xf4\x33\xe9\xc5\x3e\x56",
        // hash listpackex with unordered TTL fields
        r"\x17\xc3\x30\x35\x14\x35\x00\x00\x00\t\x00\x82\x66\x32\x03\x82\x76\x32\x03\xf4\x80\x73\x16\xd1\x8f\x01\x20\x12\x02\x82\x66\x31\x20\x11\x03\x31\x03\xf4\x7f\xe0\x01\x11\x00\x33\x20\x11\x04\x33\x03\x00\x01\xff\x0c\x00\xf6\x70\x29\x57\x11\x68\x9d\xe5",
        // hash listpackex: a field without TTL followed by one with TTL
        r"\x17\x2d\x2d\x00\x00\x00\x09\x00\x82\x66\x31\x03\x82\x76\x31\x03\x00\x01\x82\x66\x32\x03\x82\x76\x32\x03\xf4\xe0\x59\x7a\x96\x00\x00\x00\x00\x09\x82\x66\x33\x03\x82\x76\x33\x03\x00\x01\xff\x0c\x00\x42\x66\xd4\xbe\x17\xc3\x96\x72",
        // hash hashtable with TTL larger than EB_EXPIRE_TIME_MAX
        r"\x16\x02\x81\x00\x01\x00\x00\x00\x00\x00\x00\x02\x66\x31\x02\x76\x31\x81\x00\x01\x00\x00\x00\x00\x00\x00\x02\x66\x32\x02\x76\x32\x0c\x00\xb9\x3c\x65\x28\x40\x94\x58\x36",
    ];
    let port = 29821;
    start_test_server(port, 2);
    let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    for (i, p) in payloads.iter().enumerate() {
        let payload = unescape(p);
        let mut frame = b"*5\r\n$7\r\nRESTORE\r\n$3\r\nkey\r\n$1\r\n0\r\n".to_vec();
        frame.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(b"\r\n$7\r\nREPLACE\r\n");
        let r = send_and_read_reply(&mut c, &frame);
        assert!(r.contains("Bad data format"), "payload {i}: {r}");
        assert_eq!(
            resp_cmd(&mut c, &["EXISTS", "key"]),
            ":0\r\n",
            "payload {i}"
        );
    }
    assert_eq!(resp_cmd(&mut c, &["PING"]), "+PONG\r\n");
}

/// SHUTDOWN waiting for a lagging replica can be cancelled with SHUTDOWN
/// ABORT, as in Redis: every client waiting in SHUTDOWN (counted in
/// blocked_clients meanwhile) gets an error and the server keeps serving.
/// SHUTDOWN NOW does not wait for replicas.
#[test]
fn test_shutdown_abort_and_now_e2e() {
    let (mport, rport) = (29911u16, 29912u16);
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
    for bad in [
        &["SHUTDOWN", "ABORT", "NOW"][..],
        &["SHUTDOWN", "SAVE", "NOSAVE"],
    ] {
        assert!(resp_cmd(&mut m, bad).contains("syntax error"), "{bad:?}");
    }
    assert_eq!(
        resp_cmd(&mut m, &["SHUTDOWN", "ABORT"]),
        "-ERR No shutdown in progress.\r\n"
    );
    assert_eq!(
        resp_cmd(&mut m, &["CONFIG", "SET", "shutdown-timeout", "60"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    wait_for_master_link(&mut r);
    // SAFETY: kill(2) on our own live child.
    unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGSTOP) };
    assert_eq!(resp_cmd(&mut m, &["INCR", "k"]), ":1\r\n");
    let (mut s1, mut s2) = (connect(mport), connect(mport));
    for s in [&mut s1, &mut s2] {
        s.write_all(b"*1\r\n$8\r\nSHUTDOWN\r\n").unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let info = resp_cmd_full(&mut m, &["INFO", "clients"]);
        if info.contains("blocked_clients:2\r\n") {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{info}");
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(resp_cmd(&mut m, &["SHUTDOWN", "ABORT"]), "+OK\r\n");
    for s in [&mut s1, &mut s2] {
        let mut buf = [0u8; 128];
        let n = s.read(&mut buf).unwrap();
        assert_eq!(
            &buf[..n],
            b"-ERR Errors trying to SHUTDOWN. Check logs.\r\n"
        );
    }
    // Still serving, writes resumed.
    assert_eq!(resp_cmd(&mut m, &["INCR", "k"]), ":2\r\n");
    assert!(resp_cmd_full(&mut m, &["INFO", "clients"]).contains("blocked_clients:0\r\n"));

    // NOW: no waiting for the stopped replica, nor for its link, which is
    // not a command batch the drain waits for (it once sat out the whole
    // 2 s drain timeout, so a test harness gave up and sent SIGTERM).
    let t = std::time::Instant::now();
    m.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$3\r\nNOW\r\n")
        .unwrap();
    let status = master.wait().unwrap();
    assert!(status.success(), "{status}");
    assert!(
        t.elapsed() < Duration::from_millis(1500),
        "{:?}",
        t.elapsed()
    );
    // SAFETY: as above.
    unsafe { libc::kill(replica.id() as libc::pid_t, libc::SIGCONT) };
    let _ = replica.kill();
    let _ = replica.wait();
}

/// A signal arriving while a SHUTDOWN NOW runs, or once it is exiting,
/// doesn't start a second shutdown: that one waited for replicas, sent them a GETACK
/// (moving the offset) and saved again over the first RDB, so the master
/// could restart at an offset its replicas never had.
#[test]
fn test_signal_during_shutdown_now_does_not_save_again_e2e() {
    let (mport, rport) = (29931u16, 29932u16);
    let dir = std::env::temp_dir().join(format!("rudis-sigshut-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let ms = mport.to_string();
    let args = ["--port", &ms, "--threads", "2", "--no-pin", "--dir", &ds];
    start_test_server(rport, 2);
    let connect = |port: u16| {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    };
    let mut r = connect(rport);
    for round in 0..5 {
        let log_path = dir.join(format!("master-{round}.log"));
        let mut master = rudis_bin()
            .args(args)
            .args(DEBUG_ON)
            .env("MONOIO_FORCE_LEGACY_DRIVER", legacy_driver_env())
            .stdout(std::fs::File::create(&log_path).unwrap())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn rudis");
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut m = loop {
            if let Ok(mut c) = TcpStream::connect(("127.0.0.1", mport)) {
                c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                if resp_cmd(&mut c, &["PING"]) == "+PONG\r\n" {
                    break c;
                }
            }
            assert!(std::time::Instant::now() < deadline, "rudis did not start");
            thread::sleep(Duration::from_millis(50));
        };
        // From round 1 on the replica already follows this address.
        assert!(resp_cmd(&mut r, &["REPLICAOF", "127.0.0.1", &ms]).starts_with("+OK"));
        wait_for_master_link(&mut r);
        assert_eq!(resp_cmd(&mut m, &["SET", "k", "v"]), "+OK\r\n");
        assert_eq!(resp_cmd(&mut m, &["WAIT", "1", "5000"]), ":1\r\n");
        m.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$3\r\nNOW\r\n")
            .unwrap();
        // As Redis's test harness does: SIGTERM right behind it, once the
        // SHUTDOWN runs (at different moments of it, and after it).
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !std::fs::read_to_string(&log_path)
            .unwrap_or_default()
            .contains("User requested shutdown")
        {
            assert!(std::time::Instant::now() < deadline, "SHUTDOWN never ran");
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(4 * round));
        // SAFETY: kill(2) on our own live child.
        unsafe { libc::kill(master.id() as libc::pid_t, libc::SIGTERM) };
        assert!(master.wait().unwrap().success());
        let log = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            !log.contains("Received SIGTERM scheduling shutdown"),
            "round {round}: a second shutdown ran:\n{log}"
        );
    }
    resp_cmd(&mut r, &["REPLICAOF", "NO", "ONE"]);
}

/// BGSAVE writes one point in time across shards, as Redis's forked child
/// does: an MSET spanning every shard that runs while the save does is in
/// the file whole or not at all (a fuzzy per-key scan saved the shards
/// streamed first with older values than the last).
#[test]
fn test_bgsave_is_one_point_in_time_across_shards_e2e() {
    let port = 29951u16;
    let dir = std::env::temp_dir().join(format!("rudis-pitsave-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let ps = port.to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        "4",
        "--no-pin",
        "--dir",
        &ds,
        "--save",
        "",
    ];
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s
    };
    let keys: Vec<String> = (0..16).map(|i| format!("pit:{i}")).collect();
    for round in 0..3 {
        let mut server = spawn_rudis_listening(&args, port);
        let mut c = connect();
        // Enough data for the save to take many steps.
        assert_eq!(
            resp_cmd_all(&mut c, &["DEBUG", "POPULATE", "300000"]),
            "+OK\r\n"
        );
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let stop = stop.clone();
            let keys = keys.clone();
            let mut w = connect();
            thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let v = i.to_string();
                    let mut cmd = vec!["MSET"];
                    for k in &keys {
                        cmd.push(k);
                        cmd.push(&v);
                    }
                    assert_eq!(resp_cmd_all(&mut w, &cmd), "+OK\r\n");
                    i += 1;
                }
                i
            })
        };
        // And transactions of several kinds of writes over every shard,
        // pipelined (each EXEC's writes are one point in time too).
        let tx_writer = {
            let stop = stop.clone();
            let mut w = connect();
            thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let v = i.to_string();
                    let mut pipe = String::new();
                    let mut push = |args: &[&str]| {
                        pipe.push_str(&format!("*{}\r\n", args.len()));
                        for a in args {
                            pipe.push_str(&format!("${}\r\n{a}\r\n", a.len()));
                        }
                    };
                    push(&["MULTI"]);
                    for j in 0..8 {
                        push(&["SET", &format!("tx:s{j}"), &v]);
                        push(&["INCR", &format!("tx:n{j}")]);
                        push(&["HSET", &format!("tx:h{j}"), "f", &v]);
                        push(&["RPUSH", &format!("tx:l{j}"), &v]);
                    }
                    push(&["EXEC"]);
                    w.write_all(pipe.as_bytes()).unwrap();
                    // +OK, 32 +QUEUED, then the EXEC reply's header and 32
                    // entries (each one line: +OK or :n).
                    let mut got = Vec::new();
                    let mut buf = [0u8; 8192];
                    while got.windows(2).filter(|w| *w == b"\r\n").count() < 1 + 32 + 1 + 32 {
                        let n = w.read(&mut buf).unwrap();
                        assert!(n > 0);
                        got.extend_from_slice(&buf[..n]);
                    }
                    i += 1;
                }
            })
        };
        thread::sleep(Duration::from_millis(20));
        assert!(resp_cmd_all(&mut c, &["BGSAVE"]).starts_with("+Background saving started"));
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while info_field_u64(&mut c, "persistence", "rdb_bgsave_in_progress") != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "BGSAVE never finished"
            );
            thread::sleep(Duration::from_millis(10));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let writes = writer.join().unwrap();
        tx_writer.join().unwrap();
        assert_eq!(
            info_field(&mut c, "rdb_last_bgsave_status"),
            "ok",
            "round {round}"
        );
        server.kill().unwrap();
        server.wait().unwrap();

        // What the file holds.
        let mut server = spawn_rudis_listening(&args, port);
        let mut c = connect();
        let mut mget = vec!["MGET"];
        mget.extend(keys.iter().map(String::as_str));
        let got = resp_cmd_all(&mut c, &mget);
        let values: Vec<&str> = got
            .lines()
            .skip(1)
            .filter(|l| !l.starts_with('$'))
            .collect();
        assert_eq!(values.len(), keys.len(), "round {round}: {got}");
        assert!(
            values.iter().all(|v| *v == values[0]),
            "round {round}: the file holds parts of different MSETs \
             ({writes} ran during the save): {values:?}"
        );
        assert!(
            resp_cmd_all(&mut c, &["DBSIZE"]).starts_with(":3000"),
            "round {round}"
        );
        // Every transaction is in the file whole or not at all.
        let n0 = resp_cmd_all(&mut c, &["GET", "tx:n0"]);
        if n0 != "$-1\r\n" {
            let n: u64 = n0.lines().nth(1).unwrap().parse().unwrap();
            let last = (n - 1).to_string();
            let bulk = format!("${}\r\n{last}\r\n", last.len());
            for j in 0..8 {
                let get = |c: &mut TcpStream, args: &[&str]| resp_cmd_all(c, args);
                assert_eq!(
                    get(&mut c, &["GET", &format!("tx:n{j}")]),
                    n0,
                    "round {round}: tx:n{j}"
                );
                assert_eq!(
                    get(&mut c, &["GET", &format!("tx:s{j}")]),
                    bulk,
                    "round {round}: tx:s{j}"
                );
                assert_eq!(
                    get(&mut c, &["HGET", &format!("tx:h{j}"), "f"]),
                    bulk,
                    "round {round}: tx:h{j}"
                );
                assert_eq!(
                    get(&mut c, &["LLEN", &format!("tx:l{j}")]),
                    format!(":{n}\r\n"),
                    "round {round}: tx:l{j}"
                );
            }
        }
        server.kill().unwrap();
        server.wait().unwrap();
        let _ = std::fs::remove_file(dir.join("dump.rdb"));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Keys evicted while a BGSAVE runs are in the file, as in the snapshot
/// Redis's forked child writes: the save holds the keys as they were when
/// it started, whatever eviction does meanwhile.
#[test]
fn test_bgsave_keeps_keys_evicted_while_it_runs_e2e() {
    let port = 29996u16;
    let dir = std::env::temp_dir().join(format!("rudis-pitevict-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let ps = port.to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        &ds,
        "--save",
        "",
    ];
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s
    };
    let server = Kill(spawn_rudis_listening(&args, port));
    let mut c = connect();
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "POPULATE", "200000", "key", "100"]),
        "+OK\r\n"
    );
    let used = info_field_u64(&mut c, "memory", "used_memory");
    assert_eq!(
        resp_cmd_all(
            &mut c,
            &["CONFIG", "SET", "maxmemory-policy", "allkeys-random"]
        ),
        "+OK\r\n"
    );
    assert!(resp_cmd_all(&mut c, &["BGSAVE"]).starts_with("+Background saving started"));
    // Now well under what the data takes: new writes evict old keys.
    let limit = (used / 2).to_string();
    assert_eq!(
        resp_cmd_all(&mut c, &["CONFIG", "SET", "maxmemory", &limit]),
        "+OK\r\n"
    );
    let val = "n".repeat(100);
    let mut i = 0;
    while info_field_u64(&mut c, "persistence", "rdb_bgsave_in_progress") != 0 {
        for _ in 0..200 {
            resp_cmd_all(&mut c, &["SET", &format!("new:{i}"), &val]);
            i += 1;
        }
    }
    assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "ok");
    let evicted = info_field_u64(&mut c, "stats", "evicted_keys");
    assert!(
        evicted > 0,
        "nothing was evicted during the save ({i} writes)"
    );
    drop(server);

    let _server = Kill(spawn_rudis_listening(&args, port));
    let mut c = connect();
    assert_eq!(
        resp_cmd_all(&mut c, &["DBSIZE"]),
        ":200000\r\n",
        "the file lost keys evicted during the save ({evicted} evicted)"
    );
    assert_eq!(resp_cmd_all(&mut c, &["EXISTS", "new:0"]), ":0\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A SWAPDB while a BGSAVE runs no longer fails the save: the file holds
/// the databases as they were when it started.
#[test]
fn test_bgsave_survives_swapdb_while_it_runs_e2e() {
    let port = 29997u16;
    let dir = std::env::temp_dir().join(format!("rudis-pitswap-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ds = dir.to_str().unwrap().to_string();
    let ps = port.to_string();
    let args = [
        "--port",
        &ps,
        "--threads",
        "2",
        "--no-pin",
        "--dir",
        &ds,
        "--save",
        "",
    ];
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s
    };
    let server = Kill(spawn_rudis_listening(&args, port));
    let mut c = connect();
    assert_eq!(
        resp_cmd_all(&mut c, &["DEBUG", "POPULATE", "300000"]),
        "+OK\r\n"
    );
    assert_eq!(resp_cmd_all(&mut c, &["SELECT", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["SET", "one", "1"]), "+OK\r\n");
    assert!(resp_cmd_all(&mut c, &["BGSAVE"]).starts_with("+Background saving started"));
    // The save starts its images in the background (within a few ms).
    thread::sleep(Duration::from_millis(50));
    assert_eq!(resp_cmd_all(&mut c, &["SWAPDB", "0", "1"]), "+OK\r\n");
    assert_eq!(
        info_field_u64(&mut c, "persistence", "rdb_bgsave_in_progress"),
        1,
        "the save ended before the swap: the test proves nothing"
    );
    while info_field_u64(&mut c, "persistence", "rdb_bgsave_in_progress") != 0 {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(info_field(&mut c, "rdb_last_bgsave_status"), "ok");
    drop(server);

    let _server = Kill(spawn_rudis_listening(&args, port));
    let mut c = connect();
    assert_eq!(resp_cmd_all(&mut c, &["DBSIZE"]), ":300000\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["EXISTS", "one"]), ":0\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["SELECT", "1"]), "+OK\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["DBSIZE"]), ":1\r\n");
    assert_eq!(resp_cmd_all(&mut c, &["GET", "one"]), "$1\r\n1\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}
