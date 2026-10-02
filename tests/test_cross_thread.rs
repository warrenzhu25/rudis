use std::thread;
use std::time::Duration;

#[test]
fn test_cross_thread_flume() {
    let (tx, rx) = flume::bounded::<u32>(10);

    let h1 = thread::spawn(move || {
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let val = rx.recv_async().await.unwrap();
            assert_eq!(val, 42);
        });
    });

    let h2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        tx.send(42).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();
}

#[test]
fn test_lock_free_spsc_queue_and_mesh() {
    let queue = rudis::mailbox::SpscQueue::<u64>::new(1024);
    assert!(queue.is_empty());
    for i in 0..500 {
        queue.push(i);
    }
    assert!(!queue.is_empty());
    for i in 0..500 {
        assert_eq!(queue.pop(), Some(i));
    }
    assert!(queue.is_empty());
    assert_eq!(queue.pop(), None);

    // Test cross-shard mesh communication across monoio threads
    let (mesh, receivers) = rudis::mailbox::create_shard_mesh(2);
    let sender_0_to_1 = mesh[0][1].clone();
    let rx_1 = receivers.into_iter().nth(1).unwrap();

    let h_consumer = thread::spawn(move || {
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let mut count = 0;
            for expected in 0..1000 {
                let msg = rx_1.recv_async().await.unwrap();
                if let rudis::shard::ShardMessage::NotifyList { keys } = msg {
                    assert_eq!(keys[0], bytes::Bytes::from(format!("key_{}", expected)));
                    count += 1;
                }
            }
            assert_eq!(count, 1000);
        });
    });

    let h_producer = thread::spawn(move || {
        for i in 0..1000 {
            let msg = rudis::shard::ShardMessage::NotifyList {
                keys: vec![bytes::Bytes::from(format!("key_{}", i))],
            };
            sender_0_to_1.send(msg).unwrap();
        }
    });

    h_producer.join().unwrap();
    h_consumer.join().unwrap();
}

#[test]
fn test_mesh_async_ping_pong_never_loses_wakeup() {
    // Two monoio shards bounce a message back and forth through
    // `recv_async`, so each one parks on every round. A lost wakeup leaves a
    // shard parked with a message queued and the watchdog fires.
    const ROUNDS: usize = 50_000;
    let (mesh, receivers) = rudis::mailbox::create_shard_mesh(2);
    let mut receivers = receivers.into_iter();
    let (rx_0, rx_1) = (receivers.next().unwrap(), receivers.next().unwrap());
    let (tx_0, tx_1) = (mesh[0][1].clone(), mesh[1][0].clone());
    let msg = || rudis::shard::ShardMessage::NotifyList { keys: Vec::new() };
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let done_1 = done_tx.clone();
    let run = |f: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>| {
        thread::spawn(move || {
            monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f)
        })
    };
    run(Box::pin(async move {
        for _ in 0..ROUNDS {
            tx_0.send(msg()).unwrap();
            rx_0.recv_async().await.unwrap();
        }
        let _ = done_tx.send(());
    }));
    run(Box::pin(async move {
        for _ in 0..ROUNDS {
            rx_1.recv_async().await.unwrap();
            tx_1.send(msg()).unwrap();
        }
        let _ = done_1.send(());
    }));
    for _ in 0..2 {
        done_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("a shard stayed parked with a queued message (lost wakeup)");
    }
}

#[test]
fn test_mesh_flood_past_ring_capacity_keeps_per_sender_order() {
    // Three shards flood shard 0 far past the 256-slot rings, so messages
    // spill into each ring's overflow while shard 0 drains concurrently.
    // Every message must arrive exactly once and in send order per sender.
    const PER_SENDER: usize = 100_000;
    let (mesh, receivers) = rudis::mailbox::create_shard_mesh(4);
    let rx = receivers.into_iter().next().unwrap();
    let mut producers = Vec::new();
    for (sender_id, row) in mesh.iter().enumerate().skip(1) {
        let tx = row[0].clone();
        producers.push(thread::spawn(move || {
            for i in 0..PER_SENDER {
                let key = bytes::Bytes::from(format!("{}:{}", sender_id, i));
                tx.send(rudis::shard::ShardMessage::NotifyList { keys: vec![key] })
                    .unwrap();
            }
        }));
    }
    let mut next = [0usize; 4];
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut received = 0;
    while received < 3 * PER_SENDER {
        match rx.try_recv() {
            Ok(rudis::shard::ShardMessage::NotifyList { keys }) => {
                let s = std::str::from_utf8(&keys[0]).unwrap();
                let (sender, seq) = s.split_once(':').unwrap();
                let (sender, seq): (usize, usize) = (sender.parse().unwrap(), seq.parse().unwrap());
                assert_eq!(seq, next[sender], "sender {} lost or reordered", sender);
                next[sender] += 1;
                received += 1;
            }
            Ok(_) => panic!("unexpected message"),
            Err(_) => {
                assert!(std::time::Instant::now() < deadline, "stuck at {:?}", next);
                std::hint::spin_loop();
            }
        }
    }
    for p in producers {
        p.join().unwrap();
    }
    assert!(rx.try_recv().is_err());
}

#[test]
fn test_spsc_queue_bursts_at_capacity_lose_nothing() {
    // Bursts just past the ring capacity fill and empty the overflow on
    // every round while the consumer drains concurrently. The consumer used
    // to move overflow items into the ring itself, racing the producer for
    // the same slot and losing or reordering messages.
    const RING: u64 = 4;
    const BURSTS: u64 = 100_000;
    let queue = std::sync::Arc::new(rudis::mailbox::SpscQueue::<u64>::new(RING as usize));
    let consumed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let total: u64 = (0..BURSTS).map(|b| RING + 1 + b % 3).sum();
    let producer = {
        let (queue, consumed) = (queue.clone(), consumed.clone());
        thread::spawn(move || {
            let mut next = 0u64;
            for b in 0..BURSTS {
                for _ in 0..RING + 1 + b % 3 {
                    queue.push(next);
                    next += 1;
                }
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while consumed.load(std::sync::atomic::Ordering::Acquire) != next {
                    assert!(std::time::Instant::now() < deadline, "consumer stuck");
                    std::hint::spin_loop();
                }
            }
        })
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut expected = 0u64;
    while expected < total {
        match queue.pop() {
            Some(v) => {
                assert_eq!(v, expected, "item lost or reordered");
                expected += 1;
                consumed.store(expected, std::sync::atomic::Ordering::Release);
            }
            None => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "stuck at {}",
                    expected
                );
                std::hint::spin_loop();
            }
        }
    }
    producer.join().unwrap();
    assert_eq!(queue.pop(), None);
}
