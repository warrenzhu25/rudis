use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use socket2::{Domain, Protocol, Socket, Type};

use crate::connection::handle_connection;
use crate::router::Router;
use crate::shard::{ShardDb, ShardMessage};

pub fn run_shard_worker(
    shard_id: usize,
    num_shards: usize,
    port: u16,
    senders: Vec<flume::Sender<ShardMessage>>,
    rx: flume::Receiver<ShardMessage>,
    core_id: Option<core_affinity::CoreId>,
) {
    if let Some(core) = core_id {
        core_affinity::set_for_current(core);
    }

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_all()
        .build()
        .expect("Failed to initialize Monoio io_uring runtime");

    rt.block_on(async move {
        // 1. Configure socket with SO_REUSEPORT and SO_REUSEADDR
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
            .expect("Failed to create socket");
        socket.set_reuse_port(true).expect("Failed to set SO_REUSEPORT");
        socket.set_reuse_address(true).expect("Failed to set SO_REUSEADDR");
        socket.set_nonblocking(true).expect("Failed to set non-blocking");
        let _ = socket.set_recv_buffer_size(512 * 1024);
        let _ = socket.set_send_buffer_size(512 * 1024);

        let addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .expect("Invalid address");
        socket.bind(&addr.into()).expect("Failed to bind socket");
        socket.listen(4096).expect("Failed to listen on socket");

        let listener = monoio::net::TcpListener::from_std(socket.into())
            .expect("Failed to convert socket into Monoio TcpListener");

        // 2. Pure thread-local Shard DB (no Mutex, no Arc)
        let local_db = Rc::new(RefCell::new(ShardDb::new()));

        // 3. Spawn background worker to handle incoming cross-shard messages from peer cores
        let cross_shard_db = local_db.clone();
        monoio::spawn(async move {
            while let Ok(msg) = rx.recv_async().await {
                match msg {
                    ShardMessage::Get { key, responder } => {
                        let val = cross_shard_db.borrow().get(&key);
                        let _ = responder.send(val);
                    }
                    ShardMessage::Set {
                        key,
                        value,
                        responder,
                    } => {
                        cross_shard_db.borrow_mut().set(key, value);
                        let _ = responder.send(());
                    }
                    ShardMessage::Del { key, responder } => {
                        let deleted = cross_shard_db.borrow_mut().del(&key);
                        let _ = responder.send(deleted);
                    }
                    ShardMessage::Exists { key, responder } => {
                        let exists = cross_shard_db.borrow().exists(&key);
                        let _ = responder.send(exists);
                    }
                    ShardMessage::IncrBy {
                        key,
                        delta,
                        responder,
                    } => {
                        let res = cross_shard_db.borrow_mut().incr_by(key, delta);
                        let _ = responder.send(res);
                    }
                }
            }
        });

        // 4. Create router
        let router = Rc::new(Router::new(shard_id, num_shards, local_db, senders));

        println!(
            "[Shard {}/{}] Worker started and listening on {} via io_uring",
            shard_id, num_shards, addr
        );

        // 5. Accept loop
        loop {
            match listener.accept().await {
                Ok((stream, _client_addr)) => {
                    let _ = stream.set_nodelay(true);
                    let r = router.clone();
                    monoio::spawn(async move {
                        handle_connection(stream, r).await;
                    });
                }
                Err(e) => {
                    eprintln!("[Shard {}] Accept error: {}", shard_id, e);
                }
            }
        }
    });
}
