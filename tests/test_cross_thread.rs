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
