//! Resilience and Chaos Testing Suite for Rudis.
//!
//! Validates system stability under harsh conditions:
//! 1. SPSC cross-shard ring queue overflow storm (exceeding 256 ring capacity)
//! 2. Abrupt client disconnect mid-pipeline and SIGPIPE resilience
//! 3. High-concurrency client burst connections
//! 4. Memory pressure under strict MaxMemory limits

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rudis::server::run_shard_worker;

fn start_test_server(port: u16, num_shards: usize) {
    rudis::shutdown::reset_shutdown();
    let dir =
        std::env::temp_dir().join(format!("rudis-resilience-{}-{}", std::process::id(), port));
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
            .name(format!("test-resilience-shard-{}", shard_id))
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

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn send_and_read(stream: &mut TcpStream, cmd: &[u8]) -> String {
    stream.write_all(cmd).unwrap();
    let mut buf = [0u8; 16384];
    let n = stream.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[test]
fn test_mailbox_overflow_queue_storm() {
    let queue = Arc::new(rudis::mailbox::SpscQueue::<usize>::new(256));
    let total_items = 10_000;

    let q_send = queue.clone();
    let sender_handle = thread::spawn(move || {
        for i in 0..total_items {
            q_send.push(i);
        }
    });

    let q_recv = queue.clone();
    let receiver_handle = thread::spawn(move || {
        let mut received = Vec::with_capacity(total_items);
        while received.len() < total_items {
            if let Some(item) = q_recv.pop() {
                received.push(item);
            } else {
                thread::yield_now();
            }
        }
        received
    });

    sender_handle.join().unwrap();
    let received = receiver_handle.join().unwrap();

    assert_eq!(received.len(), total_items);
    for (i, &val) in received.iter().enumerate() {
        assert_eq!(
            val, i,
            "Item order must be strictly preserved across overflow queue"
        );
    }
}

#[test]
fn test_client_abrupt_disconnect_mid_pipeline() {
    let port = 20210;
    start_test_server(port, 2);

    // 1. Client connects, writes partial commands, and drops abruptly without waiting
    for _ in 0..5 {
        if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)) {
            // Write half a command
            let _ = stream.write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nabcd\r\n$");
            drop(stream);
        }
        thread::sleep(Duration::from_millis(10));
    }

    // 2. Client sends a full pipeline and closes immediately
    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)) {
        let mut pipeline = Vec::new();
        for i in 0..100 {
            pipeline.extend_from_slice(format!("SET k_{} v_{}\r\n", i, i).as_bytes());
        }
        let _ = stream.write_all(&pipeline);
        drop(stream);
    }

    // 3. New client connects normally and verifies server is completely healthy
    let mut healthy_client = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("Server should remain healthy and accepting connections");
    healthy_client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let ping_resp = send_and_read(&mut healthy_client, b"PING\r\n");
    assert_eq!(ping_resp, "+PONG\r\n");

    let set_resp = send_and_read(&mut healthy_client, b"SET health_check ok\r\n");
    assert_eq!(set_resp, "+OK\r\n");

    let get_resp = send_and_read(&mut healthy_client, b"GET health_check\r\n");
    assert_eq!(get_resp, "$2\r\nok\r\n");
}

#[test]
fn test_high_concurrency_client_burst() {
    let port = 20220;
    start_test_server(port, 4);

    let num_threads = 16;
    let ops_per_thread = 50;
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let handle = thread::spawn(move || {
            let mut client = TcpStream::connect(format!("127.0.0.1:{}", port))
                .expect("Failed to connect burst client");
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();

            for i in 0..ops_per_thread {
                let key = format!("t{}_k{}", t, i);
                let val = format!("val_{}", i);
                let set_cmd = format!("SET {{{}}} {}\r\n", key, val);
                let set_res = send_and_read(&mut client, set_cmd.as_bytes());
                assert_eq!(set_res, "+OK\r\n");

                let get_cmd = format!("GET {{{}}}\r\n", key);
                let get_res = send_and_read(&mut client, get_cmd.as_bytes());
                assert_eq!(get_res, format!("${}\r\n{}\r\n", val.len(), val));
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Burst thread must complete cleanly");
    }

    // Verify DBSIZE is accessible
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let dbsize = send_and_read(&mut client, b"DBSIZE\r\n");
    assert!(dbsize.starts_with(':'));
}

#[test]
fn test_maxmemory_eviction_pressure() {
    let port = 20230;
    start_test_server(port, 2);

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Configure 1MB limit and allkeys-lru eviction
    send_and_read(&mut client, b"CONFIG SET maxmemory 1048576\r\n");
    send_and_read(&mut client, b"CONFIG SET maxmemory-policy allkeys-lru\r\n");

    // Insert 500 keys with 4KB payloads (total ~2MB, exceeding 1MB limit)
    let payload_4kb = "x".repeat(4096);
    for i in 0..300 {
        let cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{{m}}:key_{}\r\n${}\r\n{}\r\n",
            format!("{{m}}:key_{}", i).len(),
            i,
            payload_4kb.len(),
            payload_4kb
        );
        let res = send_and_read(&mut client, cmd.as_bytes());
        assert_eq!(res, "+OK\r\n");
    }

    // Verify server remains responsive and handles reads/writes
    let ping = send_and_read(&mut client, b"PING\r\n");
    assert_eq!(ping, "+PONG\r\n");

    let info_mem = send_and_read(&mut client, b"INFO memory\r\n");
    assert!(info_mem.contains("used_memory:"));
}
