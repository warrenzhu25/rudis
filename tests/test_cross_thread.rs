use std::thread;
use std::time::Duration;

#[test]
fn test_cross_thread_flume() {
    let (tx, rx) = flume::bounded::<u32>(10);

    let h1 = thread::spawn(move || {
        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
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
