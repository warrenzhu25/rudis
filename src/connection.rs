use bytes::{Bytes, BytesMut};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt, Splitable};
use monoio::net::TcpStream;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Instant;

use crate::resp::{ClientSubcommand, ClusterSubcommand, Command, SetSlotSubcommand, parse_command};
use crate::router::{Router, key_slot, target_shard};
use crate::shard::{CompactResp, ShardDb, ShardMessage};

const READ_BUFFER_SIZE: usize = 65536;

pub type ResponderChannel = (
    flume::Sender<Vec<(usize, CompactResp)>>,
    flume::Receiver<Vec<(usize, CompactResp)>>,
);

#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: Option<String>,
    pub connected_at: Instant,
    pub last_active: Instant,
    pub last_cmd: String,
}

#[inline(always)]
pub fn write_resp_integer(out: &mut Vec<u8>, val: i64) {
    match val {
        0 => out.extend_from_slice(b":0\r\n"),
        1 => out.extend_from_slice(b":1\r\n"),
        2 => out.extend_from_slice(b":2\r\n"),
        3 => out.extend_from_slice(b":3\r\n"),
        4 => out.extend_from_slice(b":4\r\n"),
        5 => out.extend_from_slice(b":5\r\n"),
        -1 => out.extend_from_slice(b":-1\r\n"),
        _ => {
            let mut buf = [0u8; 24];
            let mut i = buf.len();
            let neg = val < 0;
            let mut uval = if neg {
                if val == i64::MIN {
                    out.extend_from_slice(b":-9223372036854775808\r\n");
                    return;
                }
                (-val) as u64
            } else {
                val as u64
            };
            while uval > 0 {
                i -= 1;
                buf[i] = b'0' + (uval % 10) as u8;
                uval /= 10;
            }
            if neg {
                i -= 1;
                buf[i] = b'-';
            }
            out.push(b':');
            out.extend_from_slice(&buf[i..]);
            out.extend_from_slice(b"\r\n");
        }
    }
}

#[inline(always)]
pub fn write_resp_bulk(out: &mut Vec<u8>, val: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
    out.extend_from_slice(val);
    out.extend_from_slice(b"\r\n");
}


pub async fn handle_connection(
    mut stream: TcpStream,
    client_addr: SocketAddr,
    client_id: u64,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
) {
    let now = Instant::now();
    client_registry.borrow_mut().insert(
        client_id,
        ClientInfo {
            id: client_id,
            addr: client_addr,
            name: None,
            connected_at: now,
            last_active: now,
            last_cmd: "NONE".to_string(),
        },
    );

    struct ClientCleanup {
        client_id: u64,
        registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
        pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
    }
    impl Drop for ClientCleanup {
        fn drop(&mut self) {
            self.registry.borrow_mut().remove(&self.client_id);
            self.pubsub.borrow_mut().remove_client(self.client_id);
        }
    }
    let _cleanup = ClientCleanup {
        client_id,
        registry: client_registry.clone(),
        pubsub: router.pubsub.clone(),
    };

    let mut buf = BytesMut::with_capacity(131072);
    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
    let mut out_buf = Vec::with_capacity(65536);

    // Pre-allocated reusable channel responders (1 per shard, 0 allocations per hop in steady-state)
    let responders: Vec<ResponderChannel> =
        (0..router.num_shards).map(|_| flume::bounded(1)).collect();
    let mut remote_batches: Vec<Vec<(usize, Command)>> = (0..router.num_shards)
        .map(|_| Vec::with_capacity(64))
        .collect();

    let mut asking = false;
    let mut in_multi = false;
    let mut tx_queue: Vec<Command> = Vec::new();
    let mut tx_has_error = false;
    let mut authenticated = !crate::acl::get_acl_for_port(router.port)
        .read()
        .unwrap()
        .is_auth_required_for_default();
    let mut auth_user = "default".to_string();

    loop {
        // Rent buffer to monoio's io_uring driver
        let (res, returned_buf) = stream.read(read_buf).await;
        read_buf = returned_buf;

        match res {
            Ok(0) => {
                // Client disconnected
                break;
            }
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);

                // 1. Parse all complete commands currently in the buffer
                let mut commands = Vec::new();
                let mut should_quit = false;
                while !buf.is_empty() {
                    match parse_command(&mut buf) {
                        Ok(Some(cmd)) => {
                            commands.push(cmd);
                        }
                        Ok(None) => {
                            // Incomplete frame, need more data
                            break;
                        }
                        Err(err) => {
                            let err_resp = format!("-ERR {}\r\n", err).into_bytes();
                            out_buf.extend_from_slice(&err_resp);
                            if in_multi {
                                tx_has_error = true;
                            } else {
                                should_quit = true;
                                break;
                            }
                        }
                    }
                }

                // 2. Transition to Pub/Sub mode if SUBSCRIBE or PSUBSCRIBE is received
                if let Some(sub_idx) = commands
                    .iter()
                    .position(|c| matches!(c, Command::Subscribe(_) | Command::Psubscribe(_)))
                {
                    for c in commands.drain(..sub_idx) {
                        let _ = execute_command(
                            c,
                            &router,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                    }
                    if !out_buf.is_empty() {
                        let write_chunk = std::mem::replace(&mut out_buf, Vec::new());
                        let _ = stream.write_all(write_chunk).await.0;
                    }
                    let initial_sub = commands.remove(0);
                    run_pubsub_loop(
                        stream,
                        client_id,
                        client_registry,
                        router,
                        initial_sub,
                        commands,
                        buf,
                    )
                    .await;
                    return;
                }

                // 2.5 Transition to Replica Stream mode if PSYNC is received
                if let Some(psync_idx) = commands
                    .iter()
                    .position(|c| matches!(c, Command::Psync { .. }))
                {
                    for c in commands.drain(..psync_idx) {
                        let _ = execute_command(
                            c,
                            &router,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                    }
                    if !out_buf.is_empty() {
                        let write_chunk = std::mem::replace(&mut out_buf, Vec::new());
                        let _ = stream.write_all(write_chunk).await.0;
                    }
                    let psync_cmd = commands.remove(0);
                    run_master_replica_stream(
                        stream,
                        client_id,
                        client_registry,
                        router,
                        psync_cmd,
                    )
                    .await;
                    return;
                }

                // 3. Execute parsed commands with transaction support and pipeline squashing
                if !commands.is_empty() {
                    let has_tx = in_multi
                        || commands.iter().any(|c| {
                            matches!(c, Command::Multi | Command::Exec | Command::Discard)
                        });

                    if has_tx {
                        for cmd in commands {
                            if in_multi {
                                match cmd {
                                    Command::Multi => {
                                        out_buf.extend_from_slice(
                                            b"-ERR MULTI calls can not be nested\r\n",
                                        );
                                    }
                                    Command::Discard => {
                                        in_multi = false;
                                        tx_queue.clear();
                                        tx_has_error = false;
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                    }
                                    Command::Reset => {
                                        in_multi = false;
                                        tx_queue.clear();
                                        tx_has_error = false;
                                        out_buf.extend_from_slice(b"+RESET\r\n");
                                    }
                                    Command::Exec => {
                                        in_multi = false;
                                        if tx_has_error {
                                            tx_queue.clear();
                                            tx_has_error = false;
                                            out_buf.extend_from_slice(
                                                b"-EXECABORT Transaction discarded because of previous errors.\r\n",
                                            );
                                        } else {
                                            let mut shards = hashbrown::HashSet::new();
                                            for cmd in &tx_queue {
                                                for k in cmd_keys(cmd) {
                                                    shards.insert(router.target_shard(k));
                                                }
                                            }
                                            let mut sorted_shards: Vec<usize> =
                                                shards.into_iter().collect();
                                            sorted_shards.sort_unstable();

                                            let use_vll = sorted_shards.len() > 1;
                                            let tx_id = if use_vll {
                                                static NEXT_TX: std::sync::atomic::AtomicU64 =
                                                    std::sync::atomic::AtomicU64::new(1);
                                                let id = NEXT_TX
                                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                router.acquire_tx_locks(&sorted_shards, id).await;
                                                id
                                            } else {
                                                0
                                            };

                                            let count = tx_queue.len();
                                            out_buf.extend_from_slice(
                                                format!("*{}\r\n", count).as_bytes(),
                                            );
                                            let queued = std::mem::take(&mut tx_queue);
                                            for q_cmd in queued {
                                                let quit = execute_command(
                                                    q_cmd,
                                                    &router,
                                                    client_id,
                                                    &client_registry,
                                                    &mut out_buf,
                                                    &mut asking,
                                                    &mut authenticated,
                                                    &mut auth_user,
                                                )
                                                .await;
                                                if quit {
                                                    should_quit = true;
                                                    break;
                                                }
                                            }

                                            if use_vll {
                                                router.release_tx_locks(&sorted_shards, tx_id).await;
                                            }
                                        }
                                    }
                                    Command::Quit => {
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                        should_quit = true;
                                        break;
                                    }
                                    _ => {
                                        tx_queue.push(cmd);
                                        out_buf.extend_from_slice(b"+QUEUED\r\n");
                                    }
                                }
                            } else {
                                match cmd {
                                    Command::Multi => {
                                        in_multi = true;
                                        tx_queue.clear();
                                        tx_has_error = false;
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                    }
                                    Command::Discard => {
                                        out_buf.extend_from_slice(
                                            b"-ERR DISCARD without MULTI\r\n",
                                        );
                                    }
                                    Command::Exec => {
                                        out_buf
                                            .extend_from_slice(b"-ERR EXEC without MULTI\r\n");
                                    }
                                    _ => {
                                        let quit = execute_command(
                                            cmd,
                                            &router,
                                            client_id,
                                            &client_registry,
                                            &mut out_buf,
                                            &mut asking,
                                            &mut authenticated,
                                            &mut auth_user,
                                        )
                                        .await;
                                        if quit {
                                            should_quit = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    } else if commands.len() == 1 {
                        let quit = execute_command(
                            commands.pop().unwrap(),
                            &router,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                        if quit {
                            should_quit = true;
                        }
                    } else {
                        let quit = execute_commands_squashed(
                            commands,
                            &router,
                            &responders,
                            &mut remote_batches,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                        if quit {
                            should_quit = true;
                        }
                    }
                }

                // 4. Batch flush all accumulated responses in one io_uring write
                if !out_buf.is_empty() {
                    let (write_res, returned_buf) = stream.write_all(out_buf).await;
                    out_buf = returned_buf;
                    out_buf.clear();
                    if write_res.is_err() {
                        return;
                    }
                }

                if should_quit {
                    break;
                }
            }
            Err(_) => {
                // Connection read error
                break;
            }
        }
    }
}

async fn run_pubsub_loop(
    stream: TcpStream,
    client_id: u64,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
    initial_sub: Command,
    pending_cmds: Vec<Command>,
    mut buf: BytesMut,
) {
    let (mut reader, mut writer) = stream.into_split();
    let (write_tx, write_rx) = flume::unbounded::<Vec<u8>>();

    monoio::spawn(async move {
        while let Ok(data) = write_rx.recv_async().await {
            if let Err(_) = writer.write_all(data).await.0 {
                break;
            }
        }
    });

    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];

    let handle_cmd = |cmd: Command, out: &mut Vec<u8>| -> bool {
        let cmd_name = match &cmd {
            Command::Subscribe(_) => "SUBSCRIBE",
            Command::Unsubscribe(_) => "UNSUBSCRIBE",
            Command::Psubscribe(_) => "PSUBSCRIBE",
            Command::Punsubscribe(_) => "PUNSUBSCRIBE",
            Command::Ping(_) => "PING",
            Command::Quit => "QUIT",
            _ => "OTHER",
        };
        if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
            c.last_active = Instant::now();
            c.last_cmd = cmd_name.to_string();
        }
        match cmd {
            Command::Subscribe(channels) => {
                let mut hub = router.pubsub.borrow_mut();
                for ch in channels {
                    let count = hub.subscribe(client_id, ch.clone(), write_tx.clone());
                    out.extend_from_slice(b"*3\r\n$9\r\nsubscribe\r\n$");
                    out.extend_from_slice(ch.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&ch);
                    out.extend_from_slice(b"\r\n:");
                    out.extend_from_slice(count.to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                false
            }
            Command::Unsubscribe(channels) => {
                let mut hub = router.pubsub.borrow_mut();
                if channels.is_empty() {
                    let unsubs = hub.unsubscribe_all(client_id);
                    if unsubs.is_empty() {
                        let total = hub.total_subscriptions(client_id);
                        out.extend_from_slice(
                            format!("*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:{}\r\n", total).as_bytes(),
                        );
                    } else {
                        for (ch, remaining) in unsubs {
                            out.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$");
                            out.extend_from_slice(ch.len().to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(&ch);
                            out.extend_from_slice(b"\r\n:");
                            out.extend_from_slice(remaining.to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                } else {
                    for ch in channels {
                        let remaining = hub.unsubscribe(client_id, &ch);
                        out.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$");
                        out.extend_from_slice(ch.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&ch);
                        out.extend_from_slice(b"\r\n:");
                        out.extend_from_slice(remaining.to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                    }
                }
                false
            }
            Command::Psubscribe(patterns) => {
                let mut hub = router.pubsub.borrow_mut();
                for pat in patterns {
                    let count = hub.psubscribe(client_id, pat.clone(), write_tx.clone());
                    out.extend_from_slice(b"*3\r\n$10\r\npsubscribe\r\n$");
                    out.extend_from_slice(pat.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&pat);
                    out.extend_from_slice(b"\r\n:");
                    out.extend_from_slice(count.to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                false
            }
            Command::Punsubscribe(patterns) => {
                let mut hub = router.pubsub.borrow_mut();
                if patterns.is_empty() {
                    let unsubs = hub.punsubscribe_all(client_id);
                    if unsubs.is_empty() {
                        let total = hub.total_subscriptions(client_id);
                        out.extend_from_slice(
                            format!("*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:{}\r\n", total)
                                .as_bytes(),
                        );
                    } else {
                        for (pat, remaining) in unsubs {
                            out.extend_from_slice(b"*3\r\n$12\r\npunsubscribe\r\n$");
                            out.extend_from_slice(pat.len().to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(&pat);
                            out.extend_from_slice(b"\r\n:");
                            out.extend_from_slice(remaining.to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                } else {
                    for pat in patterns {
                        let remaining = hub.punsubscribe(client_id, &pat);
                        out.extend_from_slice(b"*3\r\n$12\r\npunsubscribe\r\n$");
                        out.extend_from_slice(pat.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&pat);
                        out.extend_from_slice(b"\r\n:");
                        out.extend_from_slice(remaining.to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                    }
                }
                false
            }
            Command::Ping(msg) => {
                match msg {
                    Some(m) => {
                        out.extend_from_slice(
                            format!("*2\r\n$4\r\npong\r\n${}\r\n", m.len()).as_bytes(),
                        );
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        out.extend_from_slice(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n");
                    }
                }
                false
            }
            Command::Quit => {
                out.extend_from_slice(b"+OK\r\n");
                true
            }
            other => {
                let name = match &other {
                    Command::Publish { .. } => "PUBLISH",
                    Command::Get(_) => "GET",
                    Command::Set { .. } => "SET",
                    _ => "UNKNOWN",
                };
                out.extend_from_slice(
                    format!("-ERR Can't execute '{}' in subscribed mode\r\n", name).as_bytes(),
                );
                false
            }
        }
    };

    let mut out = Vec::new();
    let q = handle_cmd(initial_sub, &mut out);
    if !out.is_empty() {
        let _ = write_tx.send(out);
    }
    if q {
        return;
    }

    for cmd in pending_cmds {
        let mut out = Vec::new();
        let q = handle_cmd(cmd, &mut out);
        if !out.is_empty() {
            let _ = write_tx.send(out);
        }
        if q {
            return;
        }
    }

    while !buf.is_empty() {
        match parse_command(&mut buf) {
            Ok(Some(cmd)) => {
                let mut out = Vec::new();
                let q = handle_cmd(cmd, &mut out);
                if !out.is_empty() {
                    let _ = write_tx.send(out);
                }
                if q {
                    return;
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = write_tx.send(format!("-ERR {}\r\n", e).into_bytes());
                return;
            }
        }
    }

    loop {
        let (res, returned_buf) = reader.read(read_buf).await;
        read_buf = returned_buf;
        match res {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);
                while !buf.is_empty() {
                    match parse_command(&mut buf) {
                        Ok(Some(cmd)) => {
                            let mut out = Vec::new();
                            let q = handle_cmd(cmd, &mut out);
                            if !out.is_empty() {
                                let _ = write_tx.send(out);
                            }
                            if q {
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let _ = write_tx.send(format!("-ERR {}\r\n", e).into_bytes());
                            return;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
}

async fn run_master_replica_stream(
    stream: TcpStream,
    client_id: u64,
    _client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
    _psync_cmd: Command,
) {
    let hub = crate::replication::get_replication_hub(router.port);
    let (mut reader, mut writer) = stream.into_split();
    let (write_tx, write_rx) = flume::unbounded::<Vec<u8>>();

    let rdb = router.generate_full_rdb().await;
    let _repl = hub.register_replica(client_id, write_tx.clone());

    let replid = hub.master_replid.clone();
    let offset = hub.master_repl_offset.load(std::sync::atomic::Ordering::SeqCst);
    let mut initial_msg = format!("+FULLRESYNC {} {}\r\n${}\r\n", replid, offset, rdb.len()).into_bytes();
    initial_msg.extend_from_slice(&rdb);
    if let Err(_) = writer.write_all(initial_msg).await.0 {
        hub.unregister_replica(client_id);
        return;
    }

    let writer_hub = hub.clone();
    monoio::spawn(async move {
        while let Ok(data) = write_rx.recv_async().await {
            if let Err(_) = writer.write_all(data).await.0 {
                break;
            }
        }
        writer_hub.unregister_replica(client_id);
    });

    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
    let mut buf = BytesMut::with_capacity(32768);
    loop {
        let (res, returned_buf) = reader.read(read_buf).await;
        read_buf = returned_buf;
        match res {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);
                while !buf.is_empty() {
                    match crate::resp::parse_command(&mut buf) {
                        Ok(Some(cmd)) => {
                            if let Command::Replconf(args) = cmd {
                                if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"ack") {
                                    if let Ok(s) = std::str::from_utf8(&args[1]) {
                                        if let Ok(ack_off) = s.parse::<u64>() {
                                            hub.update_replica_ack(client_id, ack_off);
                                        }
                                    }
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            buf.clear();
                            break;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
    hub.unregister_replica(client_id);
}

pub fn cmd_primary_key(cmd: &Command) -> Option<&bytes::Bytes> {
    match cmd {
        Command::Get(key)
        | Command::Set { key, .. }
        | Command::IncrBy(key, _)
        | Command::Expire(key, _)
        | Command::Persist(key)
        | Command::Ttl(key, _)
        | Command::Hset { key, .. }
        | Command::Hmset { key, .. }
        | Command::Hget { key, .. }
        | Command::Hmget { key, .. }
        | Command::Hdel { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hlen(key)
        | Command::Hgetall(key)
        | Command::Hkeys(key)
        | Command::Hvals(key)
        | Command::Lpush { key, .. }
        | Command::Rpush { key, .. }
        | Command::Lpop { key, .. }
        | Command::Rpop { key, .. }
        | Command::Lrange { key, .. }
        | Command::Llen(key)
        | Command::Lindex { key, .. }
        | Command::Sadd { key, .. }
        | Command::Srem { key, .. }
        | Command::Smembers(key)
        | Command::Sismember { key, .. }
        | Command::Scard(key)
        | Command::Spop { key, .. }
        | Command::Zadd { key, .. }
        | Command::Zrem { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zcard(key)
        | Command::Zrank { key, .. }
        | Command::Zrevrank { key, .. }
        | Command::Zcount { key, .. }
        | Command::Zincrby { key, .. }
        | Command::Zrange { key, .. }
        | Command::Zpopmin { key, .. }
        | Command::Zpopmax { key, .. }
        | Command::Type(key)
        | Command::Setnx { key, .. }
        | Command::Getset { key, .. }
        | Command::Getdel(key)
        | Command::Append { key, .. }
        | Command::Strlen(key)
        | Command::Expiretime(key, _)
        | Command::Rename { key, .. }
        | Command::Setbit { key, .. }
        | Command::Getbit { key, .. }
        | Command::Bitcount { key, .. }
        | Command::Bitpos { key, .. }
        | Command::Pfadd { key, .. }
        | Command::Dump(key)
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xlen(key)
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::Xack { key, .. }
        | Command::Xpending { key, .. }
        | Command::Hincrby { key, .. }
        | Command::Hincrbyfloat { key, .. }
        | Command::Hrandfield { key, .. }
        | Command::Hscan { key, .. }
        | Command::Smismember { key, .. }
        | Command::Srandmember { key, .. }
        | Command::Sscan { key, .. }
        | Command::Zmscore { key, .. }
        | Command::Zrandmember { key, .. }
        | Command::Zremrangebyrank { key, .. }
        | Command::Zremrangebyscore { key, .. }
        | Command::Zremrangebylex { key, .. }
        | Command::Zlexcount { key, .. }
        | Command::Zscan { key, .. }
        | Command::Ltrim { key, .. }
        | Command::Lset { key, .. }
        | Command::Lrem { key, .. }
        | Command::Lpos { key, .. }
        | Command::Linsert { key, .. }
        | Command::Incrbyfloat { key, .. }
        | Command::Setrange { key, .. }
        | Command::Getrange { key, .. } => Some(key),
        Command::Smove { source, .. }
        | Command::Lmove { source, .. }
        | Command::Blmove { source, .. } => Some(source),
        Command::Touch(keys) | Command::Del(keys) | Command::Exists(keys) | Command::Mget(keys) => {
            keys.first()
        }
        Command::Pfcount { keys } => keys.first(),
        Command::Xread { keys, .. } | Command::Xreadgroup { keys, .. } => keys.first(),
        Command::Blpop { keys, .. } | Command::Brpop { keys, .. } => keys.first(),
        Command::Sinter(keys) | Command::Sunion(keys) | Command::Sdiff(keys) => keys.first(),
        Command::Sinterstore { destination, .. }
        | Command::Sunionstore { destination, .. }
        | Command::Sdiffstore { destination, .. } => Some(destination),
        Command::Zunionstore { destination, .. }
        | Command::Zinterstore { destination, .. }
        | Command::Zdiffstore { destination, .. } => Some(destination),
        Command::Zdiff { keys, .. } | Command::Zinter { keys, .. } | Command::Zunion { keys, .. } => {
            keys.first()
        }
        Command::Mset(pairs) | Command::Msetnx(pairs) => pairs.first().map(|(k, _)| k),
        Command::Bitop { destkey, .. } | Command::Pfmerge { destkey, .. } => Some(destkey),
        Command::Eval { keys, .. } | Command::Evalsha { keys, .. } => keys.first(),
        _ => None,
    }
}

pub fn cmd_keys<'a>(cmd: &'a Command) -> Vec<&'a [u8]> {
    match cmd {
        Command::Get(k)
        | Command::IncrBy(k, _)
        | Command::Expire(k, _)
        | Command::Persist(k)
        | Command::Ttl(k, _)
        | Command::Hlen(k)
        | Command::Hgetall(k)
        | Command::Hkeys(k)
        | Command::Hvals(k)
        | Command::Llen(k)
        | Command::Smembers(k)
        | Command::Scard(k)
        | Command::Zcard(k)
        | Command::Type(k)
        | Command::Getdel(k)
        | Command::Strlen(k)
        | Command::Expiretime(k, _)
        | Command::Dump(k)
        | Command::Xlen(k) => vec![k.as_ref()],

        Command::Set { key, .. }
        | Command::Hget { key, .. }
        | Command::Lpop { key, .. }
        | Command::Rpop { key, .. }
        | Command::Lrange { key, .. }
        | Command::Lindex { key, .. }
        | Command::Sismember { key, .. }
        | Command::Spop { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zrank { key, .. }
        | Command::Zrevrank { key, .. }
        | Command::Zcount { key, .. }
        | Command::Zincrby { key, .. }
        | Command::Zrange { key, .. }
        | Command::Zpopmin { key, .. }
        | Command::Zpopmax { key, .. }
        | Command::Setnx { key, .. }
        | Command::Getset { key, .. }
        | Command::Append { key, .. }
        | Command::Hset { key, .. }
        | Command::Hmset { key, .. }
        | Command::Hexists { key, .. }
        | Command::Lpush { key, .. }
        | Command::Rpush { key, .. }
        | Command::Sadd { key, .. }
        | Command::Srem { key, .. }
        | Command::Zadd { key, .. }
        | Command::Zrem { key, .. }
        | Command::Setbit { key, .. }
        | Command::Getbit { key, .. }
        | Command::Bitcount { key, .. }
        | Command::Bitpos { key, .. }
        | Command::Pfadd { key, .. }
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::Xack { key, .. }
        | Command::Xpending { key, .. }
        | Command::Hincrby { key, .. }
        | Command::Hincrbyfloat { key, .. }
        | Command::Hrandfield { key, .. }
        | Command::Hscan { key, .. }
        | Command::Smismember { key, .. }
        | Command::Srandmember { key, .. }
        | Command::Sscan { key, .. }
        | Command::Zmscore { key, .. }
        | Command::Zrandmember { key, .. }
        | Command::Zremrangebyrank { key, .. }
        | Command::Zremrangebyscore { key, .. }
        | Command::Zremrangebylex { key, .. }
        | Command::Zlexcount { key, .. }
        | Command::Zscan { key, .. }
        | Command::Ltrim { key, .. }
        | Command::Lset { key, .. }
        | Command::Lrem { key, .. }
        | Command::Lpos { key, .. }
        | Command::Linsert { key, .. }
        | Command::Incrbyfloat { key, .. }
        | Command::Setrange { key, .. }
        | Command::Getrange { key, .. } => vec![key.as_ref()],

        Command::Smove { source, destination, .. }
        | Command::Lmove { source, destination, .. }
        | Command::Blmove { source, destination, .. } => {
            vec![source.as_ref(), destination.as_ref()]
        }

        Command::Mget(keys) | Command::Del(keys) | Command::Exists(keys) | Command::Touch(keys) => {
            keys.iter().map(|k| k.as_ref()).collect()
        }
        Command::Pfcount { keys } => keys.iter().map(|k| k.as_ref()).collect(),
        Command::Xread { keys, .. } | Command::Xreadgroup { keys, .. } => keys.iter().map(|k| k.as_ref()).collect(),
        Command::Blpop { keys, .. } | Command::Brpop { keys, .. } => keys.iter().map(|k| k.as_ref()).collect(),
        Command::Sinter(keys) | Command::Sunion(keys) | Command::Sdiff(keys) => keys.iter().map(|k| k.as_ref()).collect(),
        Command::Sinterstore { destination, keys }
        | Command::Sunionstore { destination, keys }
        | Command::Sdiffstore { destination, keys } => {
            let mut v = vec![destination.as_ref()];
            v.extend(keys.iter().map(|k| k.as_ref()));
            v
        }
        Command::Zunionstore { destination, keys, .. }
        | Command::Zinterstore { destination, keys, .. }
        | Command::Zdiffstore { destination, keys } => {
            let mut v = vec![destination.as_ref()];
            v.extend(keys.iter().map(|k| k.as_ref()));
            v
        }
        Command::Zdiff { keys, .. } | Command::Zinter { keys, .. } | Command::Zunion { keys, .. } => {
            keys.iter().map(|k| k.as_ref()).collect()
        }

        Command::Mset(pairs) | Command::Msetnx(pairs) => {
            pairs.iter().map(|(k, _)| k.as_ref()).collect()
        }

        Command::Rename { key, newkey, .. } => {
            vec![key.as_ref(), newkey.as_ref()]
        }

        Command::Bitop { destkey, srckeys, .. } | Command::Pfmerge { destkey, srckeys } => {
            let mut v = vec![destkey.as_ref()];
            v.extend(srckeys.iter().map(|k| k.as_ref()));
            v
        }

        Command::Hmget { key, .. } | Command::Hdel { key, .. } => vec![key.as_ref()],
        Command::Eval { keys, .. } | Command::Evalsha { keys, .. } => {
            keys.iter().map(|k| k.as_ref()).collect()
        }

        _ => Vec::new(),
    }
}

async fn migrate_keys_to_node(
    router: &Router,
    keys: &[Bytes],
    host: &str,
    port: u16,
    copy: bool,
) -> Result<usize, String> {
    let mut dumps = Vec::new();
    for k in keys {
        if let Some(entry) = router.dump_key(k.clone()).await {
            dumps.push((k.clone(), entry));
        }
    }

    if dumps.is_empty() {
        return Ok(0);
    }

    let target_addr = format!("{}:{}", host, port);
    let socket_addr = match std::net::ToSocketAddrs::to_socket_addrs(&target_addr) {
        Ok(mut iter) => match iter.next() {
            Some(a) => a,
            None => return Err("cannot resolve destination host".to_string()),
        },
        Err(e) => return Err(format!("invalid destination address: {}", e)),
    };

    let mut stream = match TcpStream::connect(socket_addr).await {
        Ok(s) => s,
        Err(err) => return Err(format!("IOERR error connecting to destination: {}", err)),
    };

    let mut tx_buf = Vec::new();
    tx_buf.extend_from_slice(b"*1\r\n$6\r\nASKING\r\n");
    for (k, (val, ttl)) in &dumps {
        match val {
            crate::table::RudisValue::String(s) => {
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*5\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                    tx_buf.extend_from_slice(s);
                    tx_buf.extend_from_slice(
                        format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                } else {
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                    tx_buf.extend_from_slice(s);
                    tx_buf.extend_from_slice(b"\r\n");
                }
            }
            crate::table::RudisValue::Int(n) => {
                let s = crate::table::RudisTable::format_i64(*n);
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*5\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                    tx_buf.extend_from_slice(&s);
                    tx_buf.extend_from_slice(
                        format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                } else {
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                    tx_buf.extend_from_slice(&s);
                    tx_buf.extend_from_slice(b"\r\n");
                }
            }
            crate::table::RudisValue::SmallHash(entries) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$4\r\nHSET\r\n${}\r\n", 2 + entries.len() * 2, k.len())
                        .as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                for (f, v) in entries {
                    tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                    tx_buf.extend_from_slice(f);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", v.len()).as_bytes());
                    tx_buf.extend_from_slice(v);
                    tx_buf.extend_from_slice(b"\r\n");
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::Hash(h) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$4\r\nHSET\r\n${}\r\n", 2 + h.len() * 2, k.len()).as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                for (f, v) in h {
                    tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                    tx_buf.extend_from_slice(f);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", v.len()).as_bytes());
                    tx_buf.extend_from_slice(v);
                    tx_buf.extend_from_slice(b"\r\n");
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::List(l) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$5\r\nRPUSH\r\n${}\r\n", 2 + l.len(), k.len()).as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                for item in l {
                    tx_buf.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
                    tx_buf.extend_from_slice(item);
                    tx_buf.extend_from_slice(b"\r\n");
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::Set(s) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$4\r\nSADD\r\n${}\r\n", 2 + s.len(), k.len()).as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                for item in s {
                    tx_buf.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
                    tx_buf.extend_from_slice(item);
                    tx_buf.extend_from_slice(b"\r\n");
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::ZSet(zset) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$4\r\nZADD\r\n${}\r\n", 2 + zset.len() * 2, k.len())
                        .as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                zset.for_each(|m, score| {
                    let s = score.to_string();
                    tx_buf.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                    tx_buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                    tx_buf.extend_from_slice(m);
                    tx_buf.extend_from_slice(b"\r\n");
                });
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::HyperLogLog(regs) => {
                tx_buf.extend_from_slice(
                    format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n$16384\r\n");
                tx_buf.extend_from_slice(regs.as_ref());
                tx_buf.extend_from_slice(b"\r\n");
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::Stream(stream) => {
                for (id, fields) in &stream.entries {
                    tx_buf.extend_from_slice(
                        format!(
                            "*{}\r\n$4\r\nXADD\r\n${}\r\n",
                            3 + fields.len() * 2,
                            k.len()
                        )
                        .as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    let id_str = id.to_string();
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", id_str.len(), id_str).as_bytes(),
                    );
                    for (f, v) in fields {
                        tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                        tx_buf.extend_from_slice(f);
                        tx_buf.extend_from_slice(format!("\r\n${}\r\n", v.len()).as_bytes());
                        tx_buf.extend_from_slice(v);
                        tx_buf.extend_from_slice(b"\r\n");
                    }
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::Tiered(_) | crate::table::RudisValue::Cooled { .. } => {}
        }
    }

    if let Err(e) = stream.write_all(tx_buf).await.0 {
        return Err(format!("IOERR error sending to destination: {}", e));
    }

    let resp_buf = vec![0u8; 1024];
    let (read_res, _) = stream.read(resp_buf).await;
    if let Err(e) = read_res {
        return Err(format!("IOERR error reading from destination: {}", e));
    }

    let count = dumps.len();
    if !copy {
        for (k, _) in &dumps {
            let _ = router.del(k.clone()).await;
        }
    }

    Ok(count)
}

fn parse_bulk_str_from_resp(res: &[u8]) -> Option<Bytes> {
    if res.starts_with(b"$") && !res.starts_with(b"$-1") {
        let mut parts = res[1..].splitn(2, |&b| b == b'\r');
        let len_str = std::str::from_utf8(parts.next()?).ok()?;
        let len: usize = len_str.parse().ok()?;
        let rest = parts.next()?;
        let val_slice = rest.strip_prefix(b"\n")?;
        if val_slice.len() >= len {
            return Some(Bytes::copy_from_slice(&val_slice[..len]));
        }
    }
    None
}

pub fn get_cmd_name(cmd: &Command) -> &'static str {
    match cmd {
        Command::Auth { .. } => "AUTH",
        Command::Acl(_) => "ACL",
        Command::Blpop { .. } => "BLPOP",
        Command::Brpop { .. } => "BRPOP",
        Command::Sinter(_) => "SINTER",
        Command::Sunion(_) => "SUNION",
        Command::Sdiff(_) => "SDIFF",
        Command::Sinterstore { .. } => "SINTERSTORE",
        Command::Sunionstore { .. } => "SUNIONSTORE",
        Command::Sdiffstore { .. } => "SDIFFSTORE",
        Command::Zunionstore { .. } => "ZUNIONSTORE",
        Command::Zinterstore { .. } => "ZINTERSTORE",
        Command::Zdiffstore { .. } => "ZDIFFSTORE",
        Command::Zdiff { .. } => "ZDIFF",
        Command::Zinter { .. } => "ZINTER",
        Command::Zunion { .. } => "ZUNION",
        Command::Get(_) => "GET",
        Command::Set { .. } => "SET",
        Command::Mget(_) => "MGET",
        Command::Mset(_) => "MSET",
        Command::Del(_) => "DEL",
        Command::Exists(_) => "EXISTS",
        Command::IncrBy(_, _) => "INCRBY",
        Command::Expire(_, _) => "EXPIRE",
        Command::Persist(_) => "PERSIST",
        Command::Ttl(_, _) => "TTL",
        Command::Cluster(_) => "CLUSTER",
        Command::Client(_) => "CLIENT",
        Command::Asking => "ASKING",
        Command::Migrate { .. } => "MIGRATE",
        Command::Hset { .. } => "HSET",
        Command::Hmset { .. } => "HMSET",
        Command::Hget { .. } => "HGET",
        Command::Hmget { .. } => "HMGET",
        Command::Hdel { .. } => "HDEL",
        Command::Hexists { .. } => "HEXISTS",
        Command::Hlen(_) => "HLEN",
        Command::Hgetall(_) => "HGETALL",
        Command::Hkeys(_) => "HKEYS",
        Command::Hvals(_) => "HVALS",
        Command::Hincrby { .. } => "HINCRBY",
        Command::Hincrbyfloat { .. } => "HINCRBYFLOAT",
        Command::Hrandfield { .. } => "HRANDFIELD",
        Command::Hscan { .. } => "HSCAN",
        Command::Lpush { .. } => "LPUSH",
        Command::Rpush { .. } => "RPUSH",
        Command::Lpop { .. } => "LPOP",
        Command::Rpop { .. } => "RPOP",
        Command::Lrange { .. } => "LRANGE",
        Command::Llen(_) => "LLEN",
        Command::Lindex { .. } => "LINDEX",
        Command::Ltrim { .. } => "LTRIM",
        Command::Lset { .. } => "LSET",
        Command::Lrem { .. } => "LREM",
        Command::Lpos { .. } => "LPOS",
        Command::Linsert { .. } => "LINSERT",
        Command::Lmove { .. } => "LMOVE",
        Command::Blmove { .. } => "BLMOVE",
        Command::Sadd { .. } => "SADD",
        Command::Srem { .. } => "SREM",
        Command::Smembers(_) => "SMEMBERS",
        Command::Sismember { .. } => "SISMEMBER",
        Command::Smismember { .. } => "SMISMEMBER",
        Command::Scard(_) => "SCARD",
        Command::Spop { .. } => "SPOP",
        Command::Srandmember { .. } => "SRANDMEMBER",
        Command::Smove { .. } => "SMOVE",
        Command::Sscan { .. } => "SSCAN",
        Command::Zadd { .. } => "ZADD",
        Command::Zrem { .. } => "ZREM",
        Command::Zscore { .. } => "ZSCORE",
        Command::Zmscore { .. } => "ZMSCORE",
        Command::Zcard(_) => "ZCARD",
        Command::Zrank { .. } => "ZRANK",
        Command::Zrevrank { .. } => "ZREVRANK",
        Command::Zcount { .. } => "ZCOUNT",
        Command::Zlexcount { .. } => "ZLEXCOUNT",
        Command::Zincrby { .. } => "ZINCRBY",
        Command::Zrange { .. } => "ZRANGE",
        Command::Zpopmin { .. } => "ZPOPMIN",
        Command::Zpopmax { .. } => "ZPOPMAX",
        Command::Zrandmember { .. } => "ZRANDMEMBER",
        Command::Zremrangebyrank { .. } => "ZREMRANGEBYRANK",
        Command::Zremrangebyscore { .. } => "ZREMRANGEBYSCORE",
        Command::Zremrangebylex { .. } => "ZREMRANGEBYLEX",
        Command::Zscan { .. } => "ZSCAN",
        Command::Incrbyfloat { .. } => "INCRBYFLOAT",
        Command::Setrange { .. } => "SETRANGE",
        Command::Getrange { .. } => "GETRANGE",
        Command::Hello { .. } => "HELLO",
        Command::Reset => "RESET",
        Command::Time => "TIME",
        Command::Echo(_) => "ECHO",
        Command::Type(_) => "TYPE",
        Command::Dbsize => "DBSIZE",
        Command::Flushdb => "FLUSHDB",
        Command::Flushall => "FLUSHALL",
        Command::Touch(_) => "TOUCH",
        Command::Rename { nx: false, .. } => "RENAME",
        Command::Rename { nx: true, .. } => "RENAMENX",
        Command::Setnx { .. } => "SETNX",
        Command::Getset { .. } => "GETSET",
        Command::Getdel(_) => "GETDEL",
        Command::Append { .. } => "APPEND",
        Command::Strlen(_) => "STRLEN",
        Command::Msetnx(_) => "MSETNX",
        Command::Save => "SAVE",
        Command::Bgsave => "BGSAVE",
        Command::Lastsave => "LASTSAVE",
        Command::Ping(_) => "PING",
        Command::CommandDocs => "COMMAND",
        Command::Info(_) => "INFO",
        Command::Replicaof { .. } => "REPLICAOF",
        Command::Psync { .. } => "PSYNC",
        Command::Replconf(_) => "REPLCONF",
        Command::Role => "ROLE",
        Command::Tier(_) => "TIER",
        Command::ConfigGet(_) | Command::ConfigSet(_, _) => "CONFIG",
        Command::Quit => "QUIT",
        Command::Subscribe(_) => "SUBSCRIBE",
        Command::Unsubscribe(_) => "UNSUBSCRIBE",
        Command::Psubscribe(_) => "PSUBSCRIBE",
        Command::Punsubscribe(_) => "PUNSUBSCRIBE",
        Command::Publish { .. } => "PUBLISH",
        Command::PubsubChannels(_) => "PUBSUB CHANNELS",
        Command::PubsubNumsub(_) => "PUBSUB NUMSUB",
        Command::PubsubNumpat => "PUBSUB NUMPAT",
        Command::Keys(_) => "KEYS",
        Command::Scan { .. } => "SCAN",
        Command::Randomkey => "RANDOMKEY",
        Command::Expiretime(_, false) => "EXPIRETIME",
        Command::Expiretime(_, true) => "PEXPIRETIME",
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::Setbit { .. } => "SETBIT",
        Command::Getbit { .. } => "GETBIT",
        Command::Bitcount { .. } => "BITCOUNT",
        Command::Bitpos { .. } => "BITPOS",
        Command::Bitop { .. } => "BITOP",
        Command::Pfadd { .. } => "PFADD",
        Command::Pfcount { .. } => "PFCOUNT",
        Command::Pfmerge { .. } => "PFMERGE",
        Command::Dump(_) => "DUMP",
        Command::Restore { .. } => "RESTORE",
        Command::Xadd { .. } => "XADD",
        Command::Xlen(_) => "XLEN",
        Command::Xrange { .. } => "XRANGE",
        Command::Xrevrange { .. } => "XREVRANGE",
        Command::Xread { .. } => "XREAD",
        Command::Xdel { .. } => "XDEL",
        Command::Xtrim { .. } => "XTRIM",
        Command::XgroupCreate { .. }
        | Command::XgroupDestroy { .. }
        | Command::XgroupCreateConsumer { .. }
        | Command::XgroupDelConsumer { .. } => "XGROUP",
        Command::Xreadgroup { .. } => "XREADGROUP",
        Command::Xack { .. } => "XACK",
        Command::Xpending { .. } => "XPENDING",
        Command::Eval { .. } => "EVAL",
        Command::Evalsha { .. } => "EVALSHA",
        Command::ScriptLoad(_) | Command::ScriptExists(_) | Command::ScriptFlush => "SCRIPT",
        Command::Unknown(_) => "UNKNOWN",
    }
}

async fn execute_command(
    cmd: Command,
    router: &Router,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
) -> bool {
    let cmd_name = get_cmd_name(&cmd);
    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
        c.last_active = Instant::now();
        c.last_cmd = cmd_name.to_string();
    }

    if !*authenticated && !matches!(cmd, Command::Auth { .. } | Command::Hello { .. } | Command::Quit) {
        out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
        return false;
    }

    if let Command::Asking = cmd {
        *asking = true;
        out.extend_from_slice(b"+OK\r\n");
        return false;
    }

    let is_asking = *asking;
    *asking = false;

    if let Some(key) = cmd_primary_key(&cmd) {
        let slot = key_slot(key);
        let state = router.slot_states.borrow()[slot as usize].clone();
        match state {
            crate::shard::SlotState::Moved(target) => {
                out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, target).as_bytes());
                return false;
            }
            crate::shard::SlotState::Importing(source) => {
                if !is_asking {
                    out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, source).as_bytes());
                    return false;
                }
            }
            crate::shard::SlotState::Migrating(target) => {
                let key_exists = router.exists(key.clone()).await;
                if !key_exists {
                    out.extend_from_slice(format!("-ASK {} {}\r\n", slot, target).as_bytes());
                    return false;
                }
            }
            crate::shard::SlotState::Stable => {}
        }
    }

    if crate::replication::get_replication_hub(router.port).is_slave()
        && crate::aof::command_to_resp(&cmd).is_some()
    {
        out.extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
        return false;
    }

    match cmd {
        Command::Get(key) => {
            let val = router.get(key).await;
            match val {
                Some(v) => {
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Set {
            key,
            value,
            expire_in,
        } => {
            if crate::replication::has_connected_replicas(router.port) {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Set {
                    key: key.clone(),
                    value: value.clone(),
                    expire_in,
                }) {
                    crate::replication::propagate_bytes(router.port, &bytes);
                }
            }
            router.set(key, value, expire_in).await;
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Mget(keys) => {
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for key in keys {
                match router.get(key).await {
                    Some(v) => {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
            false
        }
        Command::Mset(pairs) => {
            if crate::replication::has_connected_replicas(router.port) {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Mset(pairs.clone())) {
                    crate::replication::propagate_bytes(router.port, &bytes);
                }
            }
            for (key, val) in pairs {
                router.set(key, val, None).await;
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Del(keys) => {
            let mut count = 0usize;
            for key in keys.clone() {
                if router.del(key).await {
                    count += 1;
                }
            }
            if count > 0 && crate::replication::has_connected_replicas(router.port) {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Del(keys)) {
                    crate::replication::propagate_bytes(router.port, &bytes);
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::Exists(keys) => {
            let mut count = 0usize;
            for key in keys {
                if router.exists(key).await {
                    count += 1;
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::IncrBy(key, delta) => {
            match router.incr_by(key.clone(), delta).await {
                Ok(val) => {
                    if let Some(bytes) = crate::aof::command_to_resp(&Command::IncrBy(key, delta)) {
                        crate::replication::propagate_bytes(router.port, &bytes);
                    }
                    write_resp_integer(out, val);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Expire(key, duration) => {
            let res = router.expire(key.clone(), duration).await;
            if res {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Expire(key, duration)) {
                    crate::replication::propagate_bytes(router.port, &bytes);
                }
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Persist(key) => {
            let res = router.persist(key.clone()).await;
            if res {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Persist(key)) {
                    crate::replication::propagate_bytes(router.port, &bytes);
                }
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Ttl(key, in_millis) => {
            let res = router.ttl(key, in_millis).await;
            out.extend_from_slice(format!(":{}\r\n", res).as_bytes());
            false
        }
        Command::Ping(msg) => {
            match msg {
                Some(m) => {
                    out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                    out.extend_from_slice(&m);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"+PONG\r\n");
                }
            }
            false
        }
        Command::CommandDocs => {
            out.extend_from_slice(b"*0\r\n");
            false
        }
        Command::Info(section) => {
            let hub = crate::replication::get_replication_hub(router.port);
            let stats = crate::tiering::get_tier_stats(router.port);
            let max_mem = crate::tiering::get_max_memory(router.port);
            let used_mem = router.get_total_used_memory().await;
            let memory_str = format!(
                "# Memory\r\nused_memory:{}\r\nused_memory_human:{}\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\ncooled_keys:{}\r\ntiered_keys:{}\r\n",
                used_mem,
                crate::tiering::format_bytes_human(used_mem as u64),
                max_mem,
                crate::tiering::format_bytes_human(max_mem),
                stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed),
                stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed),
            );
            let storage_str = format!(
                "# Storage\r\ntier_enabled:1\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\nused_memory:{}\r\nused_memory_human:{}\r\ncooled_keys:{}\r\ntiered_keys:{}\r\ntiered_bytes:{}\r\nram_saved_bytes:{}\r\ndisk_reads:{}\r\ndisk_writes:{}\r\ndead_bytes:{}\r\ndecommit_count:{}\r\ncoalesced_reads:{}\r\nbin_pages:{}\r\ntotal_stashes:{}\r\ntotal_fetches:{}\r\ntotal_deletes:{}\r\nram_hits:{}\r\nram_misses:{}\r\nstreaming_reads:{}\r\noffload_threshold_pct:{}\r\nupload_threshold_pct:{}\r\n",
                max_mem,
                crate::tiering::format_bytes_human(max_mem),
                used_mem,
                crate::tiering::format_bytes_human(used_mem as u64),
                stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed),
                stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed),
                stats.tiered_bytes.load(std::sync::atomic::Ordering::Relaxed),
                stats.ram_saved_bytes.load(std::sync::atomic::Ordering::Relaxed),
                stats.disk_reads.load(std::sync::atomic::Ordering::Relaxed),
                stats.disk_writes.load(std::sync::atomic::Ordering::Relaxed),
                stats.dead_bytes.load(std::sync::atomic::Ordering::Relaxed),
                stats.decommit_count.load(std::sync::atomic::Ordering::Relaxed),
                stats.coalesced_reads.load(std::sync::atomic::Ordering::Relaxed),
                stats.bin_pages.load(std::sync::atomic::Ordering::Relaxed),
                stats.total_stashes.load(std::sync::atomic::Ordering::Relaxed),
                stats.total_fetches.load(std::sync::atomic::Ordering::Relaxed),
                stats.total_deletes.load(std::sync::atomic::Ordering::Relaxed),
                stats.ram_hits.load(std::sync::atomic::Ordering::Relaxed),
                stats.ram_misses.load(std::sync::atomic::Ordering::Relaxed),
                stats.streaming_reads.load(std::sync::atomic::Ordering::Relaxed),
                stats.offload_threshold_pct.load(std::sync::atomic::Ordering::Relaxed),
                stats.upload_threshold_pct.load(std::sync::atomic::Ordering::Relaxed),
            );
            let info_str = match section.as_deref() {
                Some(b"replication") | Some(b"REPLICATION") => {
                    hub.format_info_replication()
                }
                Some(b"storage") | Some(b"STORAGE") | Some(b"tiered") | Some(b"TIERED") => {
                    storage_str
                }
                Some(b"memory") | Some(b"MEMORY") => {
                    memory_str
                }
                _ => {
                    format!(
                        "# Server\r\nrudis_version:0.1.0\r\narch:shared-nothing-io_uring\r\nshard_id:{}\r\nnum_shards:{}\r\n\
                         # Replication\r\n{}\
                         {}\
                         {}",
                        router.shard_id, router.num_shards, hub.format_info_replication(), memory_str, storage_str
                    )
                }
            };
            out.extend_from_slice(format!("${}\r\n", info_str.len()).as_bytes());
            out.extend_from_slice(info_str.as_bytes());
            out.extend_from_slice(b"\r\n");
            false
        }
        Command::Role => {
            let hub = crate::replication::get_replication_hub(router.port);
            out.extend_from_slice(&hub.format_role_resp());
            false
        }
        Command::Replconf(args) => {
            if args.is_empty() {
                out.extend_from_slice(b"-ERR wrong number of arguments for 'replconf' command\r\n");
            } else if args[0].eq_ignore_ascii_case(b"listening-port") {
                if args.len() >= 2 {
                    if let Ok(s) = std::str::from_utf8(&args[1]) {
                        if let Ok(rport) = s.parse::<u16>() {
                            let hub = crate::replication::get_replication_hub(router.port);
                            hub.set_replica_port(client_id, rport);
                        }
                    }
                }
                out.extend_from_slice(b"+OK\r\n");
            } else if args[0].eq_ignore_ascii_case(b"capa") {
                out.extend_from_slice(b"+OK\r\n");
            } else if args[0].eq_ignore_ascii_case(b"ack") {
                if args.len() >= 2 {
                    if let Ok(s) = std::str::from_utf8(&args[1]) {
                        if let Ok(off) = s.parse::<u64>() {
                            let hub = crate::replication::get_replication_hub(router.port);
                            hub.update_replica_ack(client_id, off);
                        }
                    }
                }
            } else if args[0].eq_ignore_ascii_case(b"getack") {
                let hub = crate::replication::get_replication_hub(router.port);
                let off = hub.master_repl_offset.load(std::sync::atomic::Ordering::SeqCst).to_string();
                out.extend_from_slice(
                    format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", off.len(), off)
                        .as_bytes(),
                );
            } else {
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::Replicaof { host, port } => {
            let hub = crate::replication::get_replication_hub(router.port);
            if host.eq_ignore_ascii_case(b"no") && port.eq_ignore_ascii_case(b"one") {
                hub.stop_sync();
                hub.make_master();
                out.extend_from_slice(b"+OK\r\n");
            } else {
                let host_str = String::from_utf8_lossy(&host).to_string();
                let port_str = String::from_utf8_lossy(&port);
                if let Ok(mport) = port_str.parse::<u16>() {
                    crate::replication::start_replica_sync(
                        router.port,
                        host_str,
                        mport,
                        router.clone(),
                    );
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR invalid port\r\n");
                }
            }
            false
        }
        Command::Psync { .. } => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Tier(sub) => {
            match sub {
                crate::resp::TierSubcommand::Spill(key) => {
                    let ok = router.spill_key(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::Cool(key) => {
                    let ok = router.cool_key(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::Decommit(key) => {
                    let count = router.decommit(key.as_deref()).await;
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                crate::resp::TierSubcommand::Load(key) => {
                    let ok = router.ensure_loaded(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::SpillAll => {
                    let mut total = 0;
                    for s in 0..router.num_shards {
                        if s == router.shard_id {
                            total += router.spill_all().await;
                        } else {
                            let (tx, rx) = flume::bounded(1);
                            if router.senders[s].send(crate::shard::ShardMessage::TierSpillAll { responder: tx }).is_ok() {
                                total += rx.recv_async().await.unwrap_or(0);
                            }
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", total).as_bytes());
                }
                crate::resp::TierSubcommand::Info => {
                    let stats = crate::tiering::get_tier_stats(router.port);
                    let max_mem = crate::tiering::get_max_memory(router.port);
                    let used_mem = router.get_total_used_memory().await;
                    let info = format!(
                        "# Tiered Storage (io_uring NVMe)\r\ntier_enabled:1\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\nused_memory:{}\r\nused_memory_human:{}\r\ncooled_keys:{}\r\ntiered_keys:{}\r\ntiered_bytes:{}\r\nram_saved_bytes:{}\r\ndisk_reads:{}\r\ndisk_writes:{}\r\ndead_bytes:{}\r\ndecommit_count:{}\r\ncoalesced_reads:{}\r\nbin_pages:{}\r\ntotal_stashes:{}\r\ntotal_fetches:{}\r\ntotal_deletes:{}\r\nram_hits:{}\r\nram_misses:{}\r\nstreaming_reads:{}\r\noffload_threshold_pct:{}\r\nupload_threshold_pct:{}\r\n",
                        max_mem,
                        crate::tiering::format_bytes_human(max_mem),
                        used_mem,
                        crate::tiering::format_bytes_human(used_mem as u64),
                        stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed),
                        stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed),
                        stats.tiered_bytes.load(std::sync::atomic::Ordering::Relaxed),
                        stats.ram_saved_bytes.load(std::sync::atomic::Ordering::Relaxed),
                        stats.disk_reads.load(std::sync::atomic::Ordering::Relaxed),
                        stats.disk_writes.load(std::sync::atomic::Ordering::Relaxed),
                        stats.dead_bytes.load(std::sync::atomic::Ordering::Relaxed),
                        stats.decommit_count.load(std::sync::atomic::Ordering::Relaxed),
                        stats.coalesced_reads.load(std::sync::atomic::Ordering::Relaxed),
                        stats.bin_pages.load(std::sync::atomic::Ordering::Relaxed),
                        stats.total_stashes.load(std::sync::atomic::Ordering::Relaxed),
                        stats.total_fetches.load(std::sync::atomic::Ordering::Relaxed),
                        stats.total_deletes.load(std::sync::atomic::Ordering::Relaxed),
                        stats.ram_hits.load(std::sync::atomic::Ordering::Relaxed),
                        stats.ram_misses.load(std::sync::atomic::Ordering::Relaxed),
                        stats.streaming_reads.load(std::sync::atomic::Ordering::Relaxed),
                        stats.offload_threshold_pct.load(std::sync::atomic::Ordering::Relaxed),
                        stats.upload_threshold_pct.load(std::sync::atomic::Ordering::Relaxed),
                    );
                    out.extend_from_slice(format!("${}\r\n{}\r\n", info.len(), info).as_bytes());
                }
            }
            false
        }
        Command::ConfigGet(param) => {
            let p_str = String::from_utf8_lossy(&param).to_lowercase();
            if p_str == "maxmemory" {
                let max_mem = crate::tiering::get_max_memory(router.port).to_string();
                let resp = format!(
                    "*2\r\n$9\r\nmaxmemory\r\n${}\r\n{}\r\n",
                    max_mem.len(),
                    max_mem
                );
                out.extend_from_slice(resp.as_bytes());
            } else if p_str == "tiered-offload-threshold" {
                let val = crate::tiering::get_offload_threshold_pct(router.port).to_string();
                let resp = format!(
                    "*2\r\n$24\r\ntiered-offload-threshold\r\n${}\r\n{}\r\n",
                    val.len(),
                    val
                );
                out.extend_from_slice(resp.as_bytes());
            } else if p_str == "tiered-upload-threshold" {
                let val = crate::tiering::get_upload_threshold_pct(router.port).to_string();
                let resp = format!(
                    "*2\r\n$23\r\ntiered-upload-threshold\r\n${}\r\n{}\r\n",
                    val.len(),
                    val
                );
                out.extend_from_slice(resp.as_bytes());
            } else if p_str == "*" {
                let max_mem = crate::tiering::get_max_memory(router.port).to_string();
                let offload = crate::tiering::get_offload_threshold_pct(router.port).to_string();
                let upload = crate::tiering::get_upload_threshold_pct(router.port).to_string();
                let resp = format!(
                    "*6\r\n$9\r\nmaxmemory\r\n${}\r\n{}\r\n$24\r\ntiered-offload-threshold\r\n${}\r\n{}\r\n$23\r\ntiered-upload-threshold\r\n${}\r\n{}\r\n",
                    max_mem.len(),
                    max_mem,
                    offload.len(),
                    offload,
                    upload.len(),
                    upload
                );
                out.extend_from_slice(resp.as_bytes());
            } else {
                out.extend_from_slice(b"*0\r\n");
            }
            false
        }
        Command::ConfigSet(param, val) => {
            let p_str = String::from_utf8_lossy(&param).to_lowercase();
            let val_str = String::from_utf8_lossy(&val);
            if p_str == "maxmemory" {
                if let Some(bytes) = crate::tiering::parse_memory_bytes(&val_str) {
                    crate::tiering::set_max_memory(router.port, bytes);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET maxmemory\r\n");
                }
            } else if p_str == "tiered-offload-threshold" {
                if let Ok(pct) = val_str.parse::<u64>() {
                    crate::tiering::set_offload_threshold_pct(router.port, pct);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET tiered-offload-threshold\r\n");
                }
            } else if p_str == "tiered-upload-threshold" {
                if let Ok(pct) = val_str.parse::<u64>() {
                    crate::tiering::set_upload_threshold_pct(router.port, pct);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET tiered-upload-threshold\r\n");
                }
            } else {
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::Cluster(sub) => {
            match sub {
                ClusterSubcommand::KeySlot(key) => {
                    let slot = key_slot(&key);
                    out.extend_from_slice(format!(":{}\r\n", slot).as_bytes());
                }
                ClusterSubcommand::CountKeysInSlot(slot) => {
                    let count = router.count_keys_in_slot(slot).await;
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                ClusterSubcommand::GetKeysInSlot(slot, count) => {
                    let keys = router.get_keys_in_slot(slot, count).await;
                    out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                    for k in keys {
                        out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                ClusterSubcommand::Slots => {
                    out.extend_from_slice(format!("*{}\r\n", router.num_shards).as_bytes());
                    for s in 0..router.num_shards {
                        let start_slot = s * 16384 / router.num_shards;
                        let end_slot = if s == router.num_shards - 1 {
                            16383
                        } else {
                            (s + 1) * 16384 / router.num_shards - 1
                        };
                        let node_id = format!("{:040x}", s + 1);
                        out.extend_from_slice(b"*3\r\n");
                        out.extend_from_slice(format!(":{}\r\n", start_slot).as_bytes());
                        out.extend_from_slice(format!(":{}\r\n", end_slot).as_bytes());
                        out.extend_from_slice(
                            format!(
                                "*3\r\n$9\r\n127.0.0.1\r\n:{}\r\n${}\r\n{}\r\n",
                                router.port,
                                node_id.len(),
                                node_id
                            )
                            .as_bytes(),
                        );
                    }
                }
                ClusterSubcommand::Nodes => {
                    let nodes = router.cluster_nodes();
                    out.extend_from_slice(format!("${}\r\n", nodes.len()).as_bytes());
                    out.extend_from_slice(nodes.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::Info => {
                    let info = router.cluster_info();
                    out.extend_from_slice(format!("${}\r\n", info.len()).as_bytes());
                    out.extend_from_slice(info.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::MyId => {
                    let id = router.my_id();
                    out.extend_from_slice(format!("${}\r\n", id.len()).as_bytes());
                    out.extend_from_slice(id.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::Meet { ip, port } => {
                    match router.cluster_meet(ip, port) {
                        Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => {
                            let resp = format!("-ERR {}\r\n", e);
                            out.extend_from_slice(resp.as_bytes());
                        }
                    }
                }
                ClusterSubcommand::SetSlot(slot, sub_cmd) => match sub_cmd {
                    SetSlotSubcommand::Migrating(node) => {
                        router.set_slot_state(slot, crate::shard::SlotState::Migrating(node));
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Importing(node) => {
                        router.set_slot_state(slot, crate::shard::SlotState::Importing(node));
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Stable => {
                        router.set_slot_state(slot, crate::shard::SlotState::Stable);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Node(node) => {
                        let is_myself = node == "myself"
                            || (0..router.num_shards).any(|s| node == format!("{:040x}", s + 1));
                        if is_myself {
                            let shard = (0..router.num_shards)
                                .find(|&s| node == format!("{:040x}", s + 1))
                                .unwrap_or_else(|| {
                                    crate::router::slot_to_shard(slot, router.num_shards)
                                });
                            router.set_slot_owner(slot, shard);
                        } else {
                            router.set_slot_state(slot, crate::shard::SlotState::Moved(node));
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                },
                ClusterSubcommand::MigrateSlot { slot, host, port } => {
                    let target_addr = format!("{}:{}", host, port);
                    // 1. Mark local slot as Migrating
                    router.set_slot_state(slot, crate::shard::SlotState::Migrating(target_addr.clone()));

                    // 2. Notify remote node: CLUSTER SETSLOT <slot> IMPORTING <my_id>
                    let my_id = router.my_id();
                    let target_sock = match std::net::ToSocketAddrs::to_socket_addrs(&target_addr) {
                        Ok(mut iter) => iter.next(),
                        Err(_) => None,
                    };
                    if let Some(sock_addr) = target_sock {
                        if let Ok(mut stream) = monoio::net::TcpStream::connect(sock_addr).await {
                            let slot_str = slot.to_string();
                            let setslot_import = format!(
                                "*4\r\n$7\r\nCLUSTER\r\n$7\r\nSETSLOT\r\n${}\r\n{}\r\n$9\r\nIMPORTING\r\n${}\r\n{}\r\n",
                                slot_str.len(), slot_str, my_id.len(), my_id
                            );
                            let (write_res, _) = stream.write_all(setslot_import.into_bytes()).await;
                            if write_res.is_ok() {
                                let buf = vec![0u8; 64];
                                let _ = stream.read(buf).await;
                            }
                        }
                    }

                    // 3. Migrate keys belonging to this slot in batches
                    loop {
                        let keys = router.get_keys_in_slot(slot, 100).await;
                        if keys.is_empty() {
                            break;
                        }
                        if let Err(e) = migrate_keys_to_node(router, &keys, &host, port, false).await {
                            out.extend_from_slice(format!("-ERR migration failed: {}\r\n", e).as_bytes());
                            return false;
                        }
                    }

                    // 4. Notify remote node to take final ownership: CLUSTER SETSLOT <slot> NODE myself
                    if let Some(sock_addr) = target_sock {
                        if let Ok(mut stream) = monoio::net::TcpStream::connect(sock_addr).await {
                            let slot_str = slot.to_string();
                            let setslot_node = format!(
                                "*4\r\n$7\r\nCLUSTER\r\n$7\r\nSETSLOT\r\n${}\r\n{}\r\n$4\r\nNODE\r\n$6\r\nmyself\r\n",
                                slot_str.len(), slot_str
                            );
                            let (write_res, _) = stream.write_all(setslot_node.into_bytes()).await;
                            if write_res.is_ok() {
                                let buf = vec![0u8; 64];
                                let _ = stream.read(buf).await;
                            }
                        }
                    }

                    // 5. Update local state to Moved
                    router.set_slot_state(slot, crate::shard::SlotState::Moved(target_addr));
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClusterSubcommand::Rebalance { host, port, slots } => {
                    let num_slots = slots.unwrap_or(1);
                    let mut migrated_count = 0;
                    for s in 0..16384u16 {
                        if migrated_count >= num_slots {
                            break;
                        }
                        let is_stable = matches!(
                            router.slot_states.borrow()[s as usize],
                            crate::shard::SlotState::Stable
                        );
                        if is_stable {
                            let keys = router.get_keys_in_slot(s, 100).await;
                            if !keys.is_empty() {
                                let _ = migrate_keys_to_node(router, &keys, &host, port, false).await;
                            }
                            let target_addr = format!("{}:{}", host, port);
                            router.set_slot_state(s, crate::shard::SlotState::Moved(target_addr));
                            migrated_count += 1;
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", migrated_count).as_bytes());
                }
                ClusterSubcommand::Failover { force } => {
                    match router.cluster_failover(force) {
                        Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => {
                            let resp = format!("-ERR {}\r\n", e);
                            out.extend_from_slice(resp.as_bytes());
                        }
                    }
                }
                ClusterSubcommand::Reset { hard } => {
                    match router.cluster_reset(hard) {
                        Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => {
                            let resp = format!("-ERR {}\r\n", e);
                            out.extend_from_slice(resp.as_bytes());
                        }
                    }
                }
                ClusterSubcommand::Forget(node_id) => {
                    match router.cluster_forget(&node_id) {
                        Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => {
                            let resp = format!("-ERR {}\r\n", e);
                            out.extend_from_slice(resp.as_bytes());
                        }
                    }
                }
                ClusterSubcommand::Replicate(node_id) => {
                    match router.cluster_replicate(&node_id) {
                        Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => {
                            let resp = format!("-ERR {}\r\n", e);
                            out.extend_from_slice(resp.as_bytes());
                        }
                    }
                }
                ClusterSubcommand::SaveConfig => {
                    out.extend_from_slice(b"+OK\r\n");
                }
            }
            false
        }
        Command::Client(sub) => {
            match sub {
                ClientSubcommand::List => {
                    let list = router.client_list(client_registry).await;
                    out.extend_from_slice(format!("${}\r\n", list.len()).as_bytes());
                    out.extend_from_slice(list.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClientSubcommand::SetName(name) => {
                    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                        c.name = Some(name);
                    }
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::GetName => {
                    let name = client_registry
                        .borrow()
                        .get(&client_id)
                        .and_then(|c| c.name.clone());
                    match name {
                        Some(n) => {
                            out.extend_from_slice(format!("${}\r\n", n.len()).as_bytes());
                            out.extend_from_slice(n.as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                        None => {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    }
                }
                ClientSubcommand::Id => {
                    out.extend_from_slice(format!(":{}\r\n", client_id).as_bytes());
                }
            }
            false
        }
        Command::Hset { .. }
        | Command::Hmset { .. }
        | Command::Hget { .. }
        | Command::Hmget { .. }
        | Command::Hdel { .. }
        | Command::Hexists { .. }
        | Command::Hlen(_)
        | Command::Hgetall(_)
        | Command::Hkeys(_)
        | Command::Hvals(_)
        | Command::Lpush { .. }
        | Command::Rpush { .. }
        | Command::Lpop { .. }
        | Command::Rpop { .. }
        | Command::Lrange { .. }
        | Command::Llen(_)
        | Command::Lindex { .. }
        | Command::Sadd { .. }
        | Command::Srem { .. }
        | Command::Smembers(_)
        | Command::Sismember { .. }
        | Command::Scard(_)
        | Command::Spop { .. }
        | Command::Zadd { .. }
        | Command::Zrem { .. }
        | Command::Zscore { .. }
        | Command::Zcard(_)
        | Command::Zrank { .. }
        | Command::Zrevrank { .. }
        | Command::Zcount { .. }
        | Command::Zincrby { .. }
        | Command::Zrange { .. }
        | Command::Zpopmin { .. }
        | Command::Zpopmax { .. }
        | Command::Type(_)
        | Command::Setnx { .. }
        | Command::Getset { .. }
        | Command::Getdel(_)
        | Command::Append { .. }
        | Command::Strlen(_)
        | Command::Setbit { .. }
        | Command::Getbit { .. }
        | Command::Bitcount { .. }
        | Command::Bitpos { .. }
        | Command::Pfadd { .. }
        | Command::Dump(_)
        | Command::Restore { .. }
        | Command::Xadd { .. }
        | Command::Xlen(_)
        | Command::Xrange { .. }
        | Command::Xrevrange { .. }
        | Command::Xdel { .. }
        | Command::Xtrim { .. }
        | Command::XgroupCreate { .. }
        | Command::XgroupDestroy { .. }
        | Command::XgroupCreateConsumer { .. }
        | Command::XgroupDelConsumer { .. }
        | Command::Xack { .. }
        | Command::Xpending { .. }
        | Command::Hincrby { .. }
        | Command::Hincrbyfloat { .. }
        | Command::Hrandfield { .. }
        | Command::Hscan { .. }
        | Command::Ltrim { .. }
        | Command::Lset { .. }
        | Command::Lrem { .. }
        | Command::Lpos { .. }
        | Command::Linsert { .. }
        | Command::Smismember { .. }
        | Command::Srandmember { .. }
        | Command::Sscan { .. }
        | Command::Zmscore { .. }
        | Command::Zlexcount { .. }
        | Command::Zrandmember { .. }
        | Command::Zremrangebyrank { .. }
        | Command::Zremrangebyscore { .. }
        | Command::Zremrangebylex { .. }
        | Command::Zscan { .. }
        | Command::Incrbyfloat { .. }
        | Command::Setrange { .. }
        | Command::Getrange { .. } => {
            if let Some(target) = target_shard_of_cmd(&cmd, router.num_shards) {
                if target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof.as_deref(),
                    );
                } else {
                    let res = router.execute_remote(target, cmd).await;
                    out.extend_from_slice(&res);
                }
            }
            false
        }
        Command::Xread {
            ref keys,
            block_ms,
            ..
        }
        | Command::Xreadgroup {
            ref keys,
            block_ms,
            ..
        } => {
            if keys.is_empty() {
                out.extend_from_slice(b"$-1\r\n");
                return false;
            }
            let first_target = router.target_shard(&keys[0]);
            for k in &keys[1..] {
                if router.target_shard(k) != first_target {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
            }

            let start_len = out.len();
            if first_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_target, cmd.clone()).await;
                out.extend_from_slice(&res);
            }

            let produced_empty = &out[start_len..] == b"$-1\r\n" || &out[start_len..] == b"*0\r\n";
            if let Some(wait_ms) = block_ms {
                if produced_empty {
                    out.truncate(start_len);
                    let (tx, rx) = flume::bounded(1);
                    {
                        let hub_arc = crate::block::get_block_hub_for_port(router.port);
                        let mut hub = hub_arc.lock().unwrap();
                        for k in keys {
                            hub.register_stream_waiter(k.clone(), tx.clone());
                        }
                    }
                    let wait_res = if wait_ms > 0 {
                        let dur = std::time::Duration::from_millis(wait_ms);
                        monoio::time::timeout(dur, rx.recv_async()).await.is_ok()
                    } else {
                        rx.recv_async().await.is_ok()
                    };
                    if wait_res {
                        let mut unblocked_cmd = cmd.clone();
                        match &mut unblocked_cmd {
                            Command::Xread { block_ms: b, .. }
                            | Command::Xreadgroup { block_ms: b, .. } => {
                                *b = None;
                            }
                            _ => {}
                        }
                        if first_target == router.shard_id {
                            execute_local_command(
                                &unblocked_cmd,
                                &mut router.local_db.borrow_mut(),
                                out,
                                router.aof.as_deref(),
                            );
                        } else {
                            let res = router.execute_remote(first_target, unblocked_cmd).await;
                            out.extend_from_slice(&res);
                        }
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
            false
        }
        Command::Auth { username, password } => {
            let uname = username.as_deref().unwrap_or("default");
            let pass = password.as_str();
            let acl = crate::acl::get_acl_for_port(router.port);
            let acl_guard = acl.read().unwrap();
            if let Ok(authed_user) = acl_guard.check_auth(Some(uname), pass) {
                *authenticated = true;
                *auth_user = authed_user;
                out.extend_from_slice(b"+OK\r\n");
            } else {
                out.extend_from_slice(
                    b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
                );
            }
            false
        }
        Command::Acl(subcmd) => {
            let acl = crate::acl::get_acl_for_port(router.port);
            match subcmd {
                crate::resp::AclSubcommand::WhoAmI => {
                    out.extend_from_slice(
                        format!("${}\r\n{}\r\n", auth_user.len(), auth_user).as_bytes(),
                    );
                }
                crate::resp::AclSubcommand::Users => {
                    let users = acl.read().unwrap().users();
                    out.extend_from_slice(format!("*{}\r\n", users.len()).as_bytes());
                    for u in users {
                        out.extend_from_slice(format!("${}\r\n{}\r\n", u.len(), u).as_bytes());
                    }
                }
                crate::resp::AclSubcommand::List => {
                    let list = acl.read().unwrap().list();
                    out.extend_from_slice(format!("*{}\r\n", list.len()).as_bytes());
                    for line in list {
                        out.extend_from_slice(format!("${}\r\n{}\r\n", line.len(), line).as_bytes());
                    }
                }
                crate::resp::AclSubcommand::GetUser(username) => {
                    if let Some(user) = acl.read().unwrap().get_user(&username) {
                        out.extend_from_slice(b"*8\r\n");
                        out.extend_from_slice(b"$5\r\nflags\r\n");
                        let flags = user.flags();
                        out.extend_from_slice(format!("*{}\r\n", flags.len()).as_bytes());
                        for f in flags {
                            out.extend_from_slice(format!("${}\r\n{}\r\n", f.len(), f).as_bytes());
                        }
                        out.extend_from_slice(b"$9\r\npasswords\r\n");
                        out.extend_from_slice(format!("*{}\r\n", user.passwords.len()).as_bytes());
                        for p in &user.passwords {
                            out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
                        }
                        out.extend_from_slice(b"$8\r\ncommands\r\n");
                        let cmd_str = if user.all_commands { "+@all" } else { "-@all" };
                        out.extend_from_slice(format!("${}\r\n{}\r\n", cmd_str.len(), cmd_str).as_bytes());
                        out.extend_from_slice(b"$4\r\nkeys\r\n");
                        let key_str = if user.all_keys { "~*" } else { "" };
                        out.extend_from_slice(format!("${}\r\n{}\r\n", key_str.len(), key_str).as_bytes());
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                crate::resp::AclSubcommand::SetUser { username, rules } => {
                    match acl.write().unwrap().set_user(&username, &rules) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                    }
                }
                crate::resp::AclSubcommand::DelUser(usernames) => {
                    let count = acl.write().unwrap().del_user(&usernames);
                    write_resp_integer(out, count as i64);
                }
                crate::resp::AclSubcommand::Cat => {
                    let cats = [
                        "keyspace", "read", "write", "set", "sortedset", "list", "hash",
                        "string", "bitmap", "hyperloglog", "geo", "stream", "pubsub",
                        "admin", "fast", "slow", "blocking", "dangerous", "connection",
                        "transaction", "scripting",
                    ];
                    out.extend_from_slice(format!("*{}\r\n", cats.len()).as_bytes());
                    for cat in cats {
                        out.extend_from_slice(format!("${}\r\n{}\r\n", cat.len(), cat).as_bytes());
                    }
                }
            }
            false
        }
        Command::Blpop { keys, timeout } => {
            let mut popped: Option<(Bytes, Bytes)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    if let Ok(mut vals) = router.local_db.borrow_mut().lpop(k, 1) {
                        if let Some(v) = vals.pop() {
                            popped = Some((k.clone(), v));
                            break;
                        }
                    }
                } else {
                    let remote_res = router
                        .execute_remote(target, Command::Lpop { key: k.clone(), count: None })
                        .await;
                    if let Some(v) = parse_bulk_str_from_resp(&remote_res) {
                        popped = Some((k.clone(), v));
                        break;
                    }
                }
            }

            if let Some((k, v)) = popped {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n$");
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&v);
                out.extend_from_slice(b"\r\n");
                return false;
            }

            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock().unwrap();
                for k in &keys {
                    hub.register_list_waiter(k.clone(), crate::block::ListPopType::Left, tx.clone());
                }
            }

            let recv_res = if timeout > 0.0 {
                let dur = std::time::Duration::from_secs_f64(timeout);
                match monoio::time::timeout(dur, rx.recv_async()).await {
                    Ok(Ok((k, v))) => Some((k, v)),
                    _ => None,
                }
            } else {
                rx.recv_async().await.ok()
            };

            if let Some((k, v)) = recv_res {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n$");
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&v);
                out.extend_from_slice(b"\r\n");
            } else {
                out.extend_from_slice(b"*-1\r\n");
            }
            false
        }
        Command::Brpop { keys, timeout } => {
            let mut popped: Option<(Bytes, Bytes)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    if let Ok(mut vals) = router.local_db.borrow_mut().rpop(k, 1) {
                        if let Some(v) = vals.pop() {
                            popped = Some((k.clone(), v));
                            break;
                        }
                    }
                } else {
                    let remote_res = router
                        .execute_remote(target, Command::Rpop { key: k.clone(), count: None })
                        .await;
                    if let Some(v) = parse_bulk_str_from_resp(&remote_res) {
                        popped = Some((k.clone(), v));
                        break;
                    }
                }
            }

            if let Some((k, v)) = popped {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n$");
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&v);
                out.extend_from_slice(b"\r\n");
                return false;
            }

            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock().unwrap();
                for k in &keys {
                    hub.register_list_waiter(k.clone(), crate::block::ListPopType::Right, tx.clone());
                }
            }

            let recv_res = if timeout > 0.0 {
                let dur = std::time::Duration::from_secs_f64(timeout);
                match monoio::time::timeout(dur, rx.recv_async()).await {
                    Ok(Ok((k, v))) => Some((k, v)),
                    _ => None,
                }
            } else {
                rx.recv_async().await.ok()
            };

            if let Some((k, v)) = recv_res {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n$");
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&v);
                out.extend_from_slice(b"\r\n");
            } else {
                out.extend_from_slice(b"*-1\r\n");
            }
            false
        }
        Command::Smove {
            ref source,
            ref destination,
            ..
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                out.extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
                return false;
            }
            if s_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Lmove {
            ref source,
            ref destination,
            ..
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                out.extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
                return false;
            }
            if s_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Blmove {
            ref source,
            ref destination,
            where_from,
            where_to,
            timeout,
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                out.extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
                return false;
            }
            let start_len = out.len();
            if s_target == router.shard_id {
                execute_local_command(
                    &Command::Lmove {
                        source: source.clone(),
                        destination: destination.clone(),
                        where_from,
                        where_to,
                    },
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router
                    .execute_remote(
                        s_target,
                        Command::Lmove {
                            source: source.clone(),
                            destination: destination.clone(),
                            where_from,
                            where_to,
                        },
                    )
                    .await;
                out.extend_from_slice(&res);
            }

            if &out[start_len..] != b"$-1\r\n" {
                return false;
            }
            out.truncate(start_len);

            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock().unwrap();
                let pop_type = match where_from {
                    crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                    crate::table::ListDirection::Right => crate::block::ListPopType::Right,
                };
                hub.register_list_waiter(source.clone(), pop_type, tx);
            }

            let recv_res = if timeout > 0.0 {
                let dur = std::time::Duration::from_secs_f64(timeout);
                match monoio::time::timeout(dur, rx.recv_async()).await {
                    Ok(Ok((_, val))) => Some(val),
                    _ => None,
                }
            } else {
                rx.recv_async().await.ok().map(|(_, val)| val)
            };

            if let Some(val) = recv_res {
                let push_cmd = match where_to {
                    crate::table::ListDirection::Left => Command::Lpush {
                        key: destination.clone(),
                        values: vec![val.clone()],
                    },
                    crate::table::ListDirection::Right => Command::Rpush {
                        key: destination.clone(),
                        values: vec![val.clone()],
                    },
                };
                if d_target == router.shard_id {
                    let mut dummy = Vec::new();
                    execute_local_command(
                        &push_cmd,
                        &mut router.local_db.borrow_mut(),
                        &mut dummy,
                        router.aof.as_deref(),
                    );
                } else {
                    router.execute_remote(d_target, push_cmd).await;
                }
                write_resp_bulk(out, &val);
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
            false
        }
        Command::Hello {
            proto,
            ref auth,
            ref setname,
        } => {
            let acl = crate::acl::get_acl_for_port(router.port);
            let default_requires_auth = acl
                .read()
                .unwrap()
                .get_user("default")
                .map(|u| !u.passwords.is_empty())
                .unwrap_or(false);

            if let Some((uname, pass)) = auth {
                if let Ok(user) = acl.read().unwrap().check_auth(Some(uname), pass) {
                    *authenticated = true;
                    *auth_user = user;
                } else {
                    out.extend_from_slice(
                        b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
                    );
                    return false;
                }
            } else if !*authenticated && default_requires_auth {
                out.extend_from_slice(
                    b"-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and set the protocol.\r\n",
                );
                return false;
            }

            if let Some(name) = setname {
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.name = Some(name.clone());
                }
            }

            let proto_ver = proto.unwrap_or(2);
            out.extend_from_slice(b"*14\r\n");
            out.extend_from_slice(b"$6\r\nserver\r\n$6\r\nvalkey\r\n");
            out.extend_from_slice(b"$7\r\nversion\r\n$5\r\n7.2.0\r\n");
            out.extend_from_slice(format!("$5\r\nproto\r\n:{}\r\n", proto_ver).as_bytes());
            out.extend_from_slice(format!("$2\r\nid\r\n:{}\r\n", client_id).as_bytes());
            out.extend_from_slice(b"$4\r\nmode\r\n$10\r\nstandalone\r\n");
            out.extend_from_slice(b"$4\r\nrole\r\n$6\r\nmaster\r\n");
            out.extend_from_slice(b"$7\r\nmodules\r\n*0\r\n");
            false
        }
        Command::Reset => {
            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                c.name = None;
            }
            *asking = false;
            let acl = crate::acl::get_acl_for_port(router.port);
            let default_requires_auth = acl
                .read()
                .unwrap()
                .get_user("default")
                .map(|u| !u.passwords.is_empty())
                .unwrap_or(false);
            *authenticated = !default_requires_auth;
            *auth_user = "default".to_string();
            out.extend_from_slice(b"+RESET\r\n");
            false
        }
        Command::Time => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = now.as_secs();
            let micros = now.subsec_micros();
            out.extend_from_slice(
                format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    secs.to_string().len(),
                    secs,
                    micros.to_string().len(),
                    micros
                )
                .as_bytes(),
            );
            false
        }
        Command::Echo(ref msg) => {
            write_resp_bulk(out, msg);
            false
        }
        Command::Sinter(ref keys)
        | Command::Sunion(ref keys)
        | Command::Sdiff(ref keys)
        | Command::Zdiff { ref keys, .. }
        | Command::Zinter { ref keys, .. }
        | Command::Zunion { ref keys, .. } => {
            if keys.is_empty() {
                out.extend_from_slice(b"*0\r\n");
                return false;
            }
            let first_target = router.target_shard(&keys[0]);
            for k in &keys[1..] {
                if router.target_shard(k) != first_target {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
            }
            if first_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Sinterstore {
            ref destination,
            ref keys,
        }
        | Command::Sunionstore {
            ref destination,
            ref keys,
        }
        | Command::Sdiffstore {
            ref destination,
            ref keys,
        }
        | Command::Zunionstore {
            ref destination,
            ref keys,
            ..
        }
        | Command::Zinterstore {
            ref destination,
            ref keys,
            ..
        }
        | Command::Zdiffstore {
            ref destination,
            ref keys,
        } => {
            let dest_target = router.target_shard(destination);
            for k in keys {
                if router.target_shard(k) != dest_target {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
            }
            if dest_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(dest_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Dbsize => {
            let count = router.dbsize().await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Flushdb | Command::Flushall => {
            router.flushdb().await;
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Touch(keys) => {
            let mut count = 0usize;
            for k in keys {
                let target = router.target_shard(&k);
                if target == router.shard_id {
                    count += router.local_db.borrow_mut().touch(&[k]);
                } else {
                    let res = router.execute_remote(target, Command::Touch(vec![k])).await;
                    if res == b":1\r\n" {
                        count += 1;
                    }
                }
            }
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Rename {
            ref key,
            ref newkey,
            ..
        } => {
            let target_src = router.target_shard(key);
            let target_dst = router.target_shard(newkey);
            if target_src != target_dst {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if target_src == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(target_src, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Msetnx(ref pairs) => {
            if pairs.is_empty() {
                out.extend_from_slice(b":0\r\n");
                return false;
            }
            let first_shard = router.target_shard(&pairs[0].0);
            let all_same_shard = pairs
                .iter()
                .all(|(k, _)| router.target_shard(k) == first_shard);
            if !all_same_shard {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Pfcount { ref keys } => {
            if keys.is_empty() {
                out.extend_from_slice(b":0\r\n");
                return false;
            }
            let first_shard = router.target_shard(&keys[0]);
            let all_same = keys.iter().all(|k| router.target_shard(k) == first_shard);
            if !all_same {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Bitop {
            ref destkey,
            ref srckeys,
            ..
        } => {
            let first_shard = router.target_shard(destkey);
            let all_same = srckeys
                .iter()
                .all(|k| router.target_shard(k) == first_shard);
            if !all_same {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Pfmerge {
            ref destkey,
            ref srckeys,
        } => {
            let first_shard = router.target_shard(destkey);
            let all_same = srckeys
                .iter()
                .all(|k| router.target_shard(k) == first_shard);
            if !all_same {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Save => {
            match router.save_rdb().await {
                Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => {
                    let err = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err.as_bytes());
                }
            }
            false
        }
        Command::Bgsave => {
            match router.bgsave().await {
                Ok(_) => {
                    out.extend_from_slice(b"+Background saving started\r\n");
                }
                Err(e) => {
                    let err = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err.as_bytes());
                }
            }
            false
        }
        Command::Lastsave => {
            let ts = router.lastsave();
            out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
            false
        }
        Command::Asking => {
            // Already handled at start of execute_command
            false
        }
        Command::Migrate {
            host,
            port,
            key,
            keys,
            destination_db: _,
            timeout_ms: _,
            copy,
            replace: _,
        } => {
            let mut migrate_keys = Vec::new();
            if let Some(k) = key {
                if !k.is_empty() {
                    migrate_keys.push(k);
                }
            }
            for k in keys {
                if !migrate_keys.contains(&k) {
                    migrate_keys.push(k);
                }
            }
            if migrate_keys.is_empty() {
                out.extend_from_slice(b"+NOKEY\r\n");
                return false;
            }

            let mut dumps = Vec::new();
            for k in &migrate_keys {
                if let Some(entry) = router.dump_key(k.clone()).await {
                    dumps.push((k.clone(), entry));
                }
            }

            if dumps.is_empty() {
                out.extend_from_slice(b"+NOKEY\r\n");
                return false;
            }

            let target_addr = format!("{}:{}", host, port);
            let socket_addr = match std::net::ToSocketAddrs::to_socket_addrs(&target_addr) {
                Ok(mut iter) => match iter.next() {
                    Some(a) => a,
                    None => {
                        out.extend_from_slice(b"-ERR cannot resolve destination host\r\n");
                        return false;
                    }
                },
                Err(_) => {
                    out.extend_from_slice(b"-ERR invalid destination address\r\n");
                    return false;
                }
            };

            let mut stream = match monoio::net::TcpStream::connect(socket_addr).await {
                Ok(s) => s,
                Err(err) => {
                    out.extend_from_slice(
                        format!("-IOERR error connecting to destination: {}\r\n", err).as_bytes(),
                    );
                    return false;
                }
            };

            // Send ASKING first to allow import if destination slot is in IMPORTING state
            let mut tx_buf = Vec::new();
            tx_buf.extend_from_slice(b"*1\r\n$6\r\nASKING\r\n");
            for (k, (val, ttl)) in &dumps {
                match val {
                    crate::table::RudisValue::String(s) => {
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*5\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                            tx_buf.extend_from_slice(s);
                            tx_buf.extend_from_slice(
                                format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str)
                                    .as_bytes(),
                            );
                        } else {
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                            tx_buf.extend_from_slice(s);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                    }
                    crate::table::RudisValue::Int(n) => {
                        let s = crate::table::RudisTable::format_i64(*n);
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*5\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                            tx_buf.extend_from_slice(&s);
                            tx_buf.extend_from_slice(
                                format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str)
                                    .as_bytes(),
                            );
                        } else {
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                            tx_buf.extend_from_slice(&s);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                    }
                    crate::table::RudisValue::SmallHash(fields) => {
                        tx_buf.extend_from_slice(
                            format!(
                                "*{}\r\n$4\r\nHSET\r\n${}\r\n",
                                2 + fields.len() * 2,
                                k.len()
                            )
                            .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        for (f, v) in fields {
                            tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            tx_buf.extend_from_slice(f);
                            tx_buf.extend_from_slice(b"\r\n");
                            tx_buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            tx_buf.extend_from_slice(v);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::Hash(fields) => {
                        tx_buf.extend_from_slice(
                            format!(
                                "*{}\r\n$4\r\nHSET\r\n${}\r\n",
                                2 + fields.len() * 2,
                                k.len()
                            )
                            .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        for (f, v) in fields {
                            tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            tx_buf.extend_from_slice(f);
                            tx_buf.extend_from_slice(b"\r\n");
                            tx_buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            tx_buf.extend_from_slice(v);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::List(deque) => {
                        tx_buf.extend_from_slice(
                            format!("*{}\r\n$5\r\nRPUSH\r\n${}\r\n", 2 + deque.len(), k.len())
                                .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        for v in deque {
                            tx_buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            tx_buf.extend_from_slice(v);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::Set(set) => {
                        tx_buf.extend_from_slice(
                            format!("*{}\r\n$4\r\nSADD\r\n${}\r\n", 2 + set.len(), k.len())
                                .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        for m in set {
                            tx_buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            tx_buf.extend_from_slice(m);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::ZSet(zset) => {
                        tx_buf.extend_from_slice(
                            format!("*{}\r\n$4\r\nZADD\r\n${}\r\n", 2 + zset.len() * 2, k.len())
                                .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        zset.for_each(|m, score| {
                            let s = score.to_string();
                            tx_buf
                                .extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                            tx_buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            tx_buf.extend_from_slice(m);
                            tx_buf.extend_from_slice(b"\r\n");
                        });
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::HyperLogLog(regs) => {
                        tx_buf.extend_from_slice(
                            format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n$16384\r\n");
                        tx_buf.extend_from_slice(regs.as_ref());
                        tx_buf.extend_from_slice(b"\r\n");
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::Stream(stream) => {
                        for (id, fields) in &stream.entries {
                            tx_buf.extend_from_slice(
                                format!(
                                    "*{}\r\n$4\r\nXADD\r\n${}\r\n",
                                    3 + fields.len() * 2,
                                    k.len()
                                )
                                .as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(b"\r\n");
                            let id_str = id.to_string();
                            tx_buf.extend_from_slice(
                                format!("${}\r\n{}\r\n", id_str.len(), id_str).as_bytes(),
                            );
                            for (f, v) in fields {
                                tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                                tx_buf.extend_from_slice(f);
                                tx_buf.extend_from_slice(b"\r\n");
                                tx_buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                tx_buf.extend_from_slice(v);
                                tx_buf.extend_from_slice(b"\r\n");
                            }
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::Tiered(_) | crate::table::RudisValue::Cooled { .. } => {}
                }
            }

            if let Err(e) = stream.write_all(tx_buf).await.0 {
                out.extend_from_slice(
                    format!("-IOERR error sending to destination: {}\r\n", e).as_bytes(),
                );
                return false;
            }

            let resp_buf = vec![0u8; 1024];
            let (read_res, _) = stream.read(resp_buf).await;
            if let Err(e) = read_res {
                out.extend_from_slice(
                    format!("-IOERR error reading from destination: {}\r\n", e).as_bytes(),
                );
                return false;
            }

            if !copy {
                for (k, _) in dumps {
                    let _ = router.del(k).await;
                }
            }

            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Subscribe(_)
        | Command::Unsubscribe(_)
        | Command::Psubscribe(_)
        | Command::Punsubscribe(_) => false,
        Command::Publish { channel, message } => {
            let count = router.publish(channel, message).await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::PubsubChannels(pattern) => {
            let channels = router.pubsub_channels(pattern).await;
            out.extend_from_slice(format!("*{}\r\n", channels.len()).as_bytes());
            for ch in channels {
                out.extend_from_slice(format!("${}\r\n", ch.len()).as_bytes());
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::PubsubNumsub(channels) => {
            let counts = router.pubsub_numsub(channels).await;
            out.extend_from_slice(format!("*{}\r\n", counts.len() * 2).as_bytes());
            for (ch, cnt) in counts {
                out.extend_from_slice(format!("${}\r\n", ch.len()).as_bytes());
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(format!(":{}\r\n", cnt).as_bytes());
            }
            false
        }
        Command::PubsubNumpat => {
            let count = router.pubsub_numpat().await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Keys(pattern) => {
            let keys = router.keys(&pattern).await;
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            let cnt = count.unwrap_or(10);
            let (next_cursor, keys) = router.scan(cursor, pattern.as_deref(), cnt).await;
            let cursor_str = next_cursor.to_string();
            out.extend_from_slice(b"*2\r\n$");
            out.extend_from_slice(cursor_str.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(cursor_str.as_bytes());
            out.extend_from_slice(b"\r\n*");
            out.extend_from_slice(keys.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Randomkey => {
            match router.random_key().await {
                Some(k) => {
                    out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Expiretime(key, in_millis) => {
            let ts = router.expiretime(key, in_millis).await;
            out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
            false
        }
        Command::Multi => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Exec => {
            out.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
            false
        }
        Command::Discard => {
            out.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
            false
        }
        Command::Eval { script, keys, args } => {
            let script_str = String::from_utf8_lossy(&script);
            crate::scripting::load_script(&script);
            match crate::scripting::eval_script(
                &script_str,
                &keys,
                &args,
                &router.local_db,
                router.aof.as_deref(),
            ) {
                Ok(resp) => out.extend_from_slice(&resp),
                Err(e) => {
                    let err_resp = format!("-{}\r\n", e);
                    out.extend_from_slice(err_resp.as_bytes());
                }
            }
            false
        }
        Command::Evalsha { sha, keys, args } => {
            let sha_str = String::from_utf8_lossy(&sha);
            if let Some(script) = crate::scripting::get_script(&sha_str) {
                match crate::scripting::eval_script(
                    &script,
                    &keys,
                    &args,
                    &router.local_db,
                    router.aof.as_deref(),
                ) {
                    Ok(resp) => out.extend_from_slice(&resp),
                    Err(e) => {
                        let err_resp = format!("-{}\r\n", e);
                        out.extend_from_slice(err_resp.as_bytes());
                    }
                }
            } else {
                out.extend_from_slice(b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
            }
            false
        }
        Command::ScriptLoad(script) => {
            let sha = crate::scripting::load_script(&script);
            write_resp_bulk(out, sha.as_bytes());
            false
        }
        Command::ScriptExists(shas) => {
            let exists = crate::scripting::script_exists(&shas);
            out.extend_from_slice(format!("*{}\r\n", exists.len()).as_bytes());
            for b in exists {
                if b {
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
            }
            false
        }
        Command::ScriptFlush => {
            crate::scripting::flush_scripts();
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Quit => {
            out.extend_from_slice(b"+OK\r\n");
            true
        }
        Command::Unknown(cmd_name) => {
            let resp = format!("-ERR unknown command '{}'\r\n", cmd_name);
            out.extend_from_slice(resp.as_bytes());
            false
        }
    }
}

pub fn target_shard_of_cmd(cmd: &Command, num_shards: usize) -> Option<usize> {
    match cmd {
        Command::Get(key)
        | Command::Set { key, .. }
        | Command::IncrBy(key, _)
        | Command::Expire(key, _)
        | Command::Persist(key)
        | Command::Ttl(key, _)
        | Command::Hset { key, .. }
        | Command::Hmset { key, .. }
        | Command::Hget { key, .. }
        | Command::Hmget { key, .. }
        | Command::Hdel { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hlen(key)
        | Command::Hgetall(key)
        | Command::Hkeys(key)
        | Command::Hvals(key)
        | Command::Lpush { key, .. }
        | Command::Rpush { key, .. }
        | Command::Lpop { key, .. }
        | Command::Rpop { key, .. }
        | Command::Lrange { key, .. }
        | Command::Llen(key)
        | Command::Lindex { key, .. }
        | Command::Sadd { key, .. }
        | Command::Srem { key, .. }
        | Command::Smembers(key)
        | Command::Sismember { key, .. }
        | Command::Scard(key)
        | Command::Spop { key, .. }
        | Command::Zadd { key, .. }
        | Command::Zrem { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zcard(key)
        | Command::Zrank { key, .. }
        | Command::Zrevrank { key, .. }
        | Command::Zcount { key, .. }
        | Command::Zincrby { key, .. }
        | Command::Zrange { key, .. }
        | Command::Zpopmin { key, .. }
        | Command::Zpopmax { key, .. }
        | Command::Type(key)
        | Command::Setnx { key, .. }
        | Command::Getset { key, .. }
        | Command::Getdel(key)
        | Command::Append { key, .. }
        | Command::Strlen(key)
        | Command::Expiretime(key, _)
        | Command::Setbit { key, .. }
        | Command::Getbit { key, .. }
        | Command::Bitcount { key, .. }
        | Command::Bitpos { key, .. }
        | Command::Pfadd { key, .. }
        | Command::Dump(key)
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xlen(key)
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::Xack { key, .. }
        | Command::Xpending { key, .. }
        | Command::Hincrby { key, .. }
        | Command::Hincrbyfloat { key, .. }
        | Command::Hrandfield { key, .. }
        | Command::Hscan { key, .. }
        | Command::Smismember { key, .. }
        | Command::Srandmember { key, .. }
        | Command::Sscan { key, .. }
        | Command::Zmscore { key, .. }
        | Command::Zrandmember { key, .. }
        | Command::Zremrangebyrank { key, .. }
        | Command::Zremrangebyscore { key, .. }
        | Command::Zremrangebylex { key, .. }
        | Command::Zlexcount { key, .. }
        | Command::Zscan { key, .. }
        | Command::Ltrim { key, .. }
        | Command::Lset { key, .. }
        | Command::Lrem { key, .. }
        | Command::Lpos { key, .. }
        | Command::Linsert { key, .. }
        | Command::Incrbyfloat { key, .. }
        | Command::Setrange { key, .. }
        | Command::Getrange { key, .. } => Some(target_shard(key, num_shards)),
        Command::Smove { source, destination, .. } => {
            let s1 = target_shard(source, num_shards);
            let s2 = target_shard(destination, num_shards);
            if s1 == s2 { Some(s1) } else { None }
        }
        Command::Lmove { source, destination, .. } => {
            let s1 = target_shard(source, num_shards);
            let s2 = target_shard(destination, num_shards);
            if s1 == s2 { Some(s1) } else { None }
        }
        Command::Pfcount { keys } if keys.len() == 1 => Some(target_shard(&keys[0], num_shards)),
        Command::Rename { key, newkey, .. } => {
            let s1 = target_shard(key, num_shards);
            let s2 = target_shard(newkey, num_shards);
            if s1 == s2 { Some(s1) } else { None }
        }
        Command::Pfmerge { destkey, srckeys }
            if srckeys
                .iter()
                .all(|k| target_shard(k, num_shards) == target_shard(destkey, num_shards)) =>
        {
            Some(target_shard(destkey, num_shards))
        }
        Command::Bitop { destkey, srckeys, .. }
            if srckeys
                .iter()
                .all(|k| target_shard(k, num_shards) == target_shard(destkey, num_shards)) =>
        {
            Some(target_shard(destkey, num_shards))
        }
        Command::Touch(keys) | Command::Del(keys) | Command::Exists(keys) if keys.len() == 1 => {
            Some(target_shard(&keys[0], num_shards))
        }
        Command::Xread { keys, .. } | Command::Xreadgroup { keys, .. }
            if !keys.is_empty()
                && keys
                    .iter()
                    .all(|k| target_shard(k, num_shards) == target_shard(&keys[0], num_shards)) =>
        {
            Some(target_shard(&keys[0], num_shards))
        }
        Command::Sinter(keys)
        | Command::Sunion(keys)
        | Command::Sdiff(keys)
        | Command::Zdiff { keys, .. }
        | Command::Zinter { keys, .. }
        | Command::Zunion { keys, .. }
            if !keys.is_empty()
                && keys
                    .iter()
                    .all(|k| target_shard(k, num_shards) == target_shard(&keys[0], num_shards)) =>
        {
            Some(target_shard(&keys[0], num_shards))
        }
        Command::Sinterstore { destination, keys }
        | Command::Sunionstore { destination, keys }
        | Command::Sdiffstore { destination, keys }
        | Command::Zunionstore { destination, keys, .. }
        | Command::Zinterstore { destination, keys, .. }
        | Command::Zdiffstore { destination, keys }
            if keys
                .iter()
                .all(|k| target_shard(k, num_shards) == target_shard(destination, num_shards)) =>
        {
            Some(target_shard(destination, num_shards))
        }
        _ => None,
    }
}

pub fn execute_local_command(
    cmd: &Command,
    db: &mut ShardDb,
    out: &mut Vec<u8>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
) -> bool {
    macro_rules! record_change {
        ($cmd_expr:expr) => {
            let need_aof = aof.is_some();
            let need_rep = crate::replication::has_connected_replicas(db.port);
            if need_aof || need_rep {
                if let Some(bytes) = crate::aof::command_to_resp($cmd_expr) {
                    if let Some(aof_w) = aof {
                        aof_w.borrow_mut().append(&bytes);
                    }
                    if need_rep {
                        crate::replication::propagate_bytes(db.port, &bytes);
                    }
                }
            }
        };
    }
    match cmd {
        Command::Get(key) => {
            match db.get(key) {
                Some(v) => {
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Mget(keys) => {
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for k in keys {
                match db.get(k) {
                    Some(v) => {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
            false
        }
        Command::Set {
            key,
            value,
            expire_in,
        } => {
            db.set(key.clone(), value.clone(), *expire_in);
            record_change!(cmd);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Mset(pairs) => {
            for (k, v) in pairs {
                db.set(k.clone(), v.clone(), None);
            }
            record_change!(cmd);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Del(keys) => {
            let mut count = 0usize;
            for k in keys {
                if db.del(k) {
                    count += 1;
                }
            }
            if count > 0 {
                record_change!(cmd);
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::Exists(keys) => {
            let mut count = 0usize;
            for k in keys {
                if db.exists(k) {
                    count += 1;
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::IncrBy(key, delta) => {
            match db.incr_by(key.clone(), *delta) {
                Ok(val) => {
                    record_change!(cmd);
                    write_resp_integer(out, val);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Expire(key, duration) => {
            let res = db.expire(key, *duration);
            if res {
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Persist(key) => {
            let res = db.persist(key);
            if res {
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Ttl(key, in_millis) => {
            let res = db.ttl(key, *in_millis);
            write_resp_integer(out, res);
            false
        }
        Command::Hset { key, fields } => {
            match db.hset(key.clone(), fields.clone()) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hmset { key, fields } => {
            match db.hset(key.clone(), fields.clone()) {
                Ok(_) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hget { key, field } => {
            match db.hget(key, field) {
                Ok(Some(v)) => {
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hmget { key, fields } => {
            match db.hmget(key, fields) {
                Ok(vals) => {
                    out.extend_from_slice(format!("*{}\r\n", vals.len()).as_bytes());
                    for v in vals {
                        match v {
                            Some(val) => {
                                out.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
                                out.extend_from_slice(&val);
                                out.extend_from_slice(b"\r\n");
                            }
                            None => {
                                out.extend_from_slice(b"$-1\r\n");
                            }
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hdel { key, fields } => {
            match db.hdel(key, fields) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hexists { key, field } => {
            match db.hexists(key, field) {
                Ok(exists) => {
                    if exists {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hlen(key) => {
            match db.hlen(key) {
                Ok(len) => {
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hgetall(key) => {
            match db.hgetall(key) {
                Ok(pairs) => {
                    out.extend_from_slice(format!("*{}\r\n", pairs.len() * 2).as_bytes());
                    for (k, v) in pairs {
                        out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hkeys(key) => {
            match db.hkeys(key) {
                Ok(keys) => {
                    out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                    for k in keys {
                        out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Hvals(key) => {
            match db.hvals(key) {
                Ok(vals) => {
                    out.extend_from_slice(format!("*{}\r\n", vals.len()).as_bytes());
                    for v in vals {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        // LIST COMMANDS
        Command::Lpush { key, values } => {
            match db.lpush(key.clone(), values.clone()) {
                Ok(len) => {
                    record_change!(cmd);
                    crate::block::get_block_hub_for_port(db.port)
                        .lock()
                        .unwrap()
                        .notify_list(&mut db.table, key);
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Rpush { key, values } => {
            match db.rpush(key.clone(), values.clone()) {
                Ok(len) => {
                    record_change!(cmd);
                    crate::block::get_block_hub_for_port(db.port)
                        .lock()
                        .unwrap()
                        .notify_list(&mut db.table, key);
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Lpop { key, count } => {
            let n = count.unwrap_or(1);
            match db.lpop(key, n) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        record_change!(cmd);
                    }
                    if count.is_some() {
                        out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                        for v in popped {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    } else if let Some(first) = popped.into_iter().next() {
                        out.extend_from_slice(format!("${}\r\n", first.len()).as_bytes());
                        out.extend_from_slice(&first);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Rpop { key, count } => {
            let n = count.unwrap_or(1);
            match db.rpop(key, n) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        record_change!(cmd);
                    }
                    if count.is_some() {
                        out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                        for v in popped {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    } else if let Some(first) = popped.into_iter().next() {
                        out.extend_from_slice(format!("${}\r\n", first.len()).as_bytes());
                        out.extend_from_slice(&first);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Llen(key) => {
            match db.llen(key) {
                Ok(len) => {
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Lindex { key, index } => {
            match db.lindex(key, *index) {
                Ok(Some(v)) => {
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Lrange { key, start, stop } => {
            match db.lrange(key, *start, *stop) {
                Ok(items) => {
                    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                    for v in items {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        // SET COMMANDS
        Command::Sadd { key, members } => {
            match db.sadd(key.clone(), members.clone()) {
                Ok(added) => {
                    if added > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", added).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Srem { key, members } => {
            match db.srem(key, members) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Smembers(key) => {
            match db.smembers(key) {
                Ok(members) => {
                    out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                    for m in members {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Sismember { key, member } => {
            match db.sismember(key, member) {
                Ok(is_mem) => {
                    if is_mem {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Scard(key) => {
            match db.scard(key) {
                Ok(card) => {
                    out.extend_from_slice(format!(":{}\r\n", card).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Spop { key, count } => {
            let n = count.unwrap_or(1);
            match db.spop(key, n) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        let srem_cmd = Command::Srem {
                            key: key.clone(),
                            members: popped.clone(),
                        };
                        record_change!(&srem_cmd);
                    }
                    if count.is_some() {
                        out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                        for m in popped {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    } else if let Some(first) = popped.into_iter().next() {
                        out.extend_from_slice(format!("${}\r\n", first.len()).as_bytes());
                        out.extend_from_slice(&first);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Sinter(keys) => {
            match db.sinter(keys) {
                Ok(members) => {
                    out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                    for m in members {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Sunion(keys) => {
            match db.sunion(keys) {
                Ok(members) => {
                    out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                    for m in members {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Sdiff(keys) => {
            match db.sdiff(keys) {
                Ok(members) => {
                    out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                    for m in members {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Sinterstore { destination, keys } => {
            match db.sinterstore(destination.clone(), keys) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Sunionstore { destination, keys } => {
            match db.sunionstore(destination.clone(), keys) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Sdiffstore { destination, keys } => {
            match db.sdiffstore(destination.clone(), keys) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        // ZSET COMMANDS
        Command::Zadd {
            key,
            elements,
            flags,
        } => {
            match db.zadd(key.clone(), elements.clone(), *flags) {
                Ok((count, incr_score)) => {
                    if flags.incr {
                        if incr_score.is_some() {
                            record_change!(cmd);
                        }
                    } else if count > 0 {
                        record_change!(cmd);
                    }
                    if flags.incr {
                        if let Some(score) = incr_score {
                            let s = score.to_string();
                            out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        write_resp_integer(out, count as i64);
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zrem { key, members } => {
            match db.zrem(key, members) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zscore { key, member } => {
            match db.zscore(key, member) {
                Ok(Some(score)) => {
                    let s = score.to_string();
                    out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zcard(key) => {
            match db.zcard(key) {
                Ok(card) => {
                    out.extend_from_slice(format!(":{}\r\n", card).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zrank { key, member } => {
            match db.zrank(key, member, false) {
                Ok(Some(rank)) => {
                    out.extend_from_slice(format!(":{}\r\n", rank).as_bytes());
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zrevrank { key, member } => {
            match db.zrank(key, member, true) {
                Ok(Some(rank)) => {
                    out.extend_from_slice(format!(":{}\r\n", rank).as_bytes());
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zcount {
            key,
            min,
            min_inc,
            max,
            max_inc,
        } => {
            match db.zcount(key, *min, *min_inc, *max, *max_inc) {
                Ok(count) => {
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zincrby { key, delta, member } => {
            match db.zincrby(key.clone(), *delta, member.clone()) {
                Ok(score) => {
                    record_change!(cmd);
                    let s = score.to_string();
                    out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zrange { key, opts } => {
            match db.zrange(key, opts) {
                Ok(items) => {
                    if opts.with_scores {
                        out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                        for (m, s) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                            let s_str = s.to_string();
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", s_str.len(), s_str).as_bytes(),
                            );
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zpopmin { key, count } => {
            match db.zpopmin(key, *count) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        let zrem_cmd = Command::Zrem {
                            key: key.clone(),
                            members: popped.iter().map(|(m, _)| m.clone()).collect(),
                        };
                        record_change!(&zrem_cmd);
                    }
                    out.extend_from_slice(format!("*{}\r\n", popped.len() * 2).as_bytes());
                    for (m, s) in popped {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                        let s_str = s.to_string();
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", s_str.len(), s_str).as_bytes(),
                        );
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zpopmax { key, count } => {
            match db.zpopmax(key, *count) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        let zrem_cmd = Command::Zrem {
                            key: key.clone(),
                            members: popped.iter().map(|(m, _)| m.clone()).collect(),
                        };
                        record_change!(&zrem_cmd);
                    }
                    out.extend_from_slice(format!("*{}\r\n", popped.len() * 2).as_bytes());
                    for (m, s) in popped {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                        let s_str = s.to_string();
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", s_str.len(), s_str).as_bytes(),
                        );
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zunionstore {
            destination,
            keys,
            weights,
            aggregate,
        } => {
            match db.zunionstore(destination.clone(), keys, weights, *aggregate) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zinterstore {
            destination,
            keys,
            weights,
            aggregate,
        } => {
            match db.zinterstore(destination.clone(), keys, weights, *aggregate) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zdiffstore { destination, keys } => {
            match db.zdiffstore(destination.clone(), keys) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zdiff { keys, with_scores } => {
            match db.zdiff(keys, *with_scores) {
                Ok(items) => {
                    if *with_scores {
                        out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                        for (m, s) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                            let s_str = s.to_string();
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", s_str.len(), s_str).as_bytes(),
                            );
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zinter {
            keys,
            weights,
            aggregate,
            with_scores,
        } => {
            match db.zinter(keys, weights, *aggregate, *with_scores) {
                Ok(items) => {
                    if *with_scores {
                        out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                        for (m, s) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                            let s_str = s.to_string();
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", s_str.len(), s_str).as_bytes(),
                            );
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Zunion {
            keys,
            weights,
            aggregate,
            with_scores,
        } => {
            match db.zunion(keys, weights, *aggregate, *with_scores) {
                Ok(items) => {
                    if *with_scores {
                        out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                        for (m, s) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                            let s_str = s.to_string();
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", s_str.len(), s_str).as_bytes(),
                            );
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        // GENERIC & DATABASE COMMANDS
        Command::Type(key) => {
            let t = db.type_of(key);
            out.extend_from_slice(format!("+{}\r\n", t).as_bytes());
            false
        }
        Command::Dbsize => {
            let n = db.dbsize();
            out.extend_from_slice(format!(":{}\r\n", n).as_bytes());
            false
        }
        Command::Flushdb | Command::Flushall => {
            db.flushdb();
            record_change!(cmd);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Touch(keys) => {
            let n = db.touch(keys);
            out.extend_from_slice(format!(":{}\r\n", n).as_bytes());
            false
        }
        Command::Rename { key, newkey, nx } => {
            match db.rename(key, newkey.clone(), *nx) {
                Ok(success) => {
                    if success {
                        record_change!(cmd);
                        if *nx {
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b"+OK\r\n");
                        }
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        // EXTENDED STRING COMMANDS
        Command::Setnx { key, value } => {
            let set = db.setnx(key.clone(), value.clone());
            if set {
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Getset { key, value } => {
            match db.getset(key.clone(), value.clone()) {
                Ok(old) => {
                    record_change!(cmd);
                    match old {
                        Some(v) => {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Getdel(key) => {
            match db.getdel(key) {
                Ok(old) => {
                    if old.is_some() {
                        record_change!(cmd);
                    }
                    match old {
                        Some(v) => {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Append { key, value } => {
            match db.append(key.clone(), value) {
                Ok(new_len) => {
                    record_change!(cmd);
                    out.extend_from_slice(format!(":{}\r\n", new_len).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Strlen(key) => {
            match db.strlen(key) {
                Ok(len) => {
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Msetnx(pairs) => {
            let any_exists = pairs.iter().any(|(k, _)| db.exists(k));
            if any_exists {
                out.extend_from_slice(b":0\r\n");
            } else {
                for (k, v) in pairs {
                    db.set(k.clone(), v.clone(), None);
                }
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            }
            false
        }
        Command::Ping(msg) => {
            match msg {
                Some(m) => {
                    out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                    out.extend_from_slice(m);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"+PONG\r\n");
                }
            }
            false
        }
        Command::CommandDocs => {
            out.extend_from_slice(b"*0\r\n");
            false
        }
        Command::Keys(pattern) => {
            let keys = db.keys(pattern);
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            let cnt = count.unwrap_or(10);
            let (next_cursor, keys) = db.scan(*cursor as usize, pattern.as_deref(), cnt);
            let cursor_str = next_cursor.to_string();
            out.extend_from_slice(b"*2\r\n$");
            out.extend_from_slice(cursor_str.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(cursor_str.as_bytes());
            out.extend_from_slice(b"\r\n*");
            out.extend_from_slice(keys.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Randomkey => {
            match db.random_key() {
                Some(k) => {
                    out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Expiretime(key, in_millis) => {
            let ts = db.expiretime(key, *in_millis);
            out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
            false
        }
        Command::Multi => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Exec => {
            out.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
            false
        }
        Command::Discard => {
            out.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
            false
        }
        Command::Setbit { key, offset, value } => {
            match db.setbit(key.clone(), *offset, *value) {
                Ok(old) => {
                    record_change!(cmd);
                    out.extend_from_slice(format!(":{}\r\n", old).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Getbit { key, offset } => {
            match db.getbit(key, *offset) {
                Ok(bit) => {
                    out.extend_from_slice(format!(":{}\r\n", bit).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Bitcount { key, start, end } => {
            match db.bitcount(key, *start, *end) {
                Ok(count) => {
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Bitpos {
            key,
            bit,
            start,
            end,
        } => {
            match db.bitpos(key, *bit, *start, *end) {
                Ok(pos) => {
                    out.extend_from_slice(format!(":{}\r\n", pos).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Bitop {
            op,
            destkey,
            srckeys,
        } => {
            match db.bitop(op, destkey.clone(), srckeys) {
                Ok(len) => {
                    record_change!(cmd);
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Pfadd { key, elements } => {
            match db.pfadd(key.clone(), elements) {
                Ok(updated) => {
                    if updated {
                        record_change!(cmd);
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Pfcount { keys } => {
            match db.pfcount(keys) {
                Ok(count) => {
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Pfmerge { destkey, srckeys } => {
            match db.pfmerge(destkey.clone(), srckeys) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Dump(key) => {
            match db.dump(key) {
                Some(bytes) => {
                    out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
                    out.extend_from_slice(&bytes);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Restore {
            key,
            ttl_ms,
            serialized,
            replace,
            absttl,
        } => {
            match db.restore(key.clone(), *ttl_ms, serialized, *replace, *absttl) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    if err.starts_with("BUSYKEY") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Xadd {
            key,
            nomkstream,
            maxlen,
            minid,
            id,
            fields,
        } => {
            match db.xadd(
                key.clone(),
                id.clone(),
                fields.clone(),
                *nomkstream,
                *maxlen,
                *minid,
            ) {
                Ok(Some(generated_id)) => {
                    let explicit_cmd = Command::Xadd {
                        key: key.clone(),
                        nomkstream: *nomkstream,
                        maxlen: *maxlen,
                        minid: *minid,
                        id: crate::table::StreamAddId::Explicit(generated_id),
                        fields: fields.clone(),
                    };
                    record_change!(&explicit_cmd);
                    let s = generated_id.to_string();
                    crate::block::get_block_hub_for_port(db.port)
                        .lock()
                        .unwrap()
                        .notify_stream(key);
                    out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Xlen(key) => {
            match db.xlen(key) {
                Ok(len) => {
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xrange {
            key,
            start,
            end,
            count,
        } => {
            match db.xrange(key, start, end, *count) {
                Ok(items) => {
                    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                    for (id, fields) in items {
                        let id_str = id.to_string();
                        out.extend_from_slice(
                            format!(
                                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                id_str.len(),
                                id_str,
                                fields.len() * 2
                            )
                            .as_bytes(),
                        );
                        for (f, v) in fields {
                            out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            out.extend_from_slice(&f);
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xrevrange {
            key,
            end,
            start,
            count,
        } => {
            match db.xrevrange(key, end, start, *count) {
                Ok(items) => {
                    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                    for (id, fields) in items {
                        let id_str = id.to_string();
                        out.extend_from_slice(
                            format!(
                                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                id_str.len(),
                                id_str,
                                fields.len() * 2
                            )
                            .as_bytes(),
                        );
                        for (f, v) in fields {
                            out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            out.extend_from_slice(&f);
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xread {
            count,
            keys,
            ids,
            ..
        } => {
            match db.xread(keys, ids, *count) {
                Ok(streams) => {
                    if streams.is_empty() {
                        out.extend_from_slice(b"$-1\r\n");
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", streams.len()).as_bytes());
                        for (stream_key, entries) in streams {
                            out.extend_from_slice(
                                format!(
                                    "*2\r\n${}\r\n",
                                    stream_key.len()
                                )
                                .as_bytes(),
                            );
                            out.extend_from_slice(&stream_key);
                            out.extend_from_slice(
                                format!("\r\n*{}\r\n", entries.len()).as_bytes(),
                            );
                            for (id, fields) in entries {
                                let id_str = id.to_string();
                                out.extend_from_slice(
                                    format!(
                                        "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                        id_str.len(),
                                        id_str,
                                        fields.len() * 2
                                    )
                                    .as_bytes(),
                                );
                                for (f, v) in fields {
                                    out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                                    out.extend_from_slice(&f);
                                    out.extend_from_slice(b"\r\n");
                                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                    out.extend_from_slice(&v);
                                    out.extend_from_slice(b"\r\n");
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xdel { key, ids } => {
            match db.xdel(key, ids) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xtrim {
            key,
            maxlen,
            minid,
        } => {
            match db.xtrim(key, *maxlen, *minid) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::XgroupCreate { key, group, id, mkstream } => {
            match db.xgroup_create(key.clone(), group.clone(), id, *mkstream) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    if err.starts_with("BUSYGROUP") || err.starts_with("ERR") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::XgroupDestroy { key, group } => {
            match db.xgroup_destroy(key, group) {
                Ok(destroyed) => {
                    if destroyed {
                        record_change!(cmd);
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::XgroupCreateConsumer { key, group, consumer } => {
            match db.xgroup_createconsumer(key, group, consumer.clone()) {
                Ok(created) => {
                    if created {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::XgroupDelConsumer { key, group, consumer } => {
            match db.xgroup_delconsumer(key, group, consumer) {
                Ok(pending_count) => {
                    out.extend_from_slice(format!(":{}\r\n", pending_count).as_bytes());
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Xreadgroup {
            group,
            consumer,
            count,
            noack,
            keys,
            ids,
            ..
        } => {
            let mut all_results = Vec::new();
            let mut err = None;
            for (k, id_str) in keys.iter().zip(ids.iter()) {
                match db.xreadgroup(k, group, consumer.clone(), id_str, *count, *noack) {
                    Ok(entries) => {
                        if !entries.is_empty() {
                            all_results.push((k.clone(), entries));
                        }
                    }
                    Err(e) => {
                        err = Some(e);
                        break;
                    }
                }
            }
            if let Some(err) = err {
                if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                } else {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            } else if all_results.is_empty() {
                out.extend_from_slice(b"$-1\r\n");
            } else {
                out.extend_from_slice(format!("*{}\r\n", all_results.len()).as_bytes());
                for (stream_key, entries) in all_results {
                    out.extend_from_slice(
                        format!(
                            "*2\r\n${}\r\n",
                            stream_key.len()
                        )
                        .as_bytes(),
                    );
                    out.extend_from_slice(&stream_key);
                    out.extend_from_slice(
                        format!("\r\n*{}\r\n", entries.len()).as_bytes(),
                    );
                    for (id, fields) in entries {
                        let id_str = id.to_string();
                        out.extend_from_slice(
                            format!(
                                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                id_str.len(),
                                id_str,
                                fields.len() * 2
                            )
                            .as_bytes(),
                        );
                        for (f, v) in fields {
                            out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            out.extend_from_slice(&f);
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
            }
            false
        }
        Command::Xack { key, group, ids } => {
            match db.xack(key, group, ids) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Xpending { key, group, range } => {
            match range {
                None => match db.xpending_summary(key, group) {
                    Ok((count, min_id, max_id, consumers)) => {
                        out.extend_from_slice(b"*4\r\n");
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                        if let Some(min) = min_id {
                            let s = min.to_string();
                            out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                        if let Some(max) = max_id {
                            let s = max.to_string();
                            out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                        out.extend_from_slice(format!("*{}\r\n", consumers.len()).as_bytes());
                        for (c_name, c_cnt) in consumers {
                            out.extend_from_slice(
                                format!("*2\r\n${}\r\n", c_name.len()).as_bytes(),
                            );
                            out.extend_from_slice(&c_name);
                            out.extend_from_slice(format!("\r\n:{}\r\n", c_cnt).as_bytes());
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                        }
                    }
                },
                Some((start, end, count, consumer)) => {
                    match db.xpending_range(key, group, *start, *end, *count, consumer.as_deref()) {
                        Ok(entries) => {
                            out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                            for (id, c_name, idle, delivery_cnt) in entries {
                                let id_str = id.to_string();
                                out.extend_from_slice(
                                    format!(
                                        "*4\r\n${}\r\n{}\r\n${}\r\n",
                                        id_str.len(),
                                        id_str,
                                        c_name.len(),
                                    )
                                    .as_bytes(),
                                );
                                out.extend_from_slice(&c_name);
                                out.extend_from_slice(
                                    format!("\r\n:{}\r\n:{}\r\n", idle, delivery_cnt).as_bytes(),
                                );
                            }
                        }
                        Err(err) => {
                            if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                                out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                            } else {
                                out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                            }
                        }
                    }
                }
            }
            false
        }
        Command::Hincrby { key, field, increment } => {
            match db.hincrby(key.clone(), field.clone(), *increment) {
                Ok(val) => {
                    record_change!(cmd);
                    write_resp_integer(out, val);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Hincrbyfloat { key, field, increment } => {
            match db.hincrbyfloat(key.clone(), field.clone(), *increment) {
                Ok(val) => {
                    record_change!(cmd);
                    write_resp_bulk(out, val.to_string().as_bytes());
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Hrandfield { key, count, with_values } => {
            match db.hrandfield(key, *count, *with_values) {
                Ok(items) => {
                    if count.is_none() {
                        if let Some(f) = items.first() {
                            write_resp_bulk(out, f);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for item in items {
                            write_resp_bulk(out, &item);
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Hscan { key, cursor, pattern, count } => {
            match db.hscan(key, *cursor, pattern.as_deref().map(|p| p.as_ref()), count.unwrap_or(10)) {
                Ok((next_cursor, entries)) => {
                    out.extend_from_slice(b"*2\r\n");
                    let cur_str = next_cursor.to_string();
                    write_resp_bulk(out, cur_str.as_bytes());
                    out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                    for item in entries {
                        write_resp_bulk(out, &item);
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Smismember { key, members } => {
            match db.smismember(key, members) {
                Ok(bools) => {
                    out.extend_from_slice(format!("*{}\r\n", bools.len()).as_bytes());
                    for b in bools {
                        write_resp_integer(out, if b { 1 } else { 0 });
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Srandmember { key, count } => {
            match db.srandmember(key, *count) {
                Ok(items) => {
                    if count.is_none() {
                        if let Some(item) = items.first() {
                            write_resp_bulk(out, item);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for item in items {
                            write_resp_bulk(out, &item);
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Smove { source, destination, member } => {
            match db.smove(source, destination.clone(), member.clone()) {
                Ok(moved) => {
                    if moved {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, if moved { 1 } else { 0 });
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Sscan { key, cursor, pattern, count } => {
            match db.sscan(key, *cursor, pattern.as_deref().map(|p| p.as_ref()), count.unwrap_or(10)) {
                Ok((next_cursor, entries)) => {
                    out.extend_from_slice(b"*2\r\n");
                    let cur_str = next_cursor.to_string();
                    write_resp_bulk(out, cur_str.as_bytes());
                    out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                    for item in entries {
                        write_resp_bulk(out, &item);
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Zmscore { key, members } => {
            match db.zmscore(key, members) {
                Ok(scores) => {
                    out.extend_from_slice(format!("*{}\r\n", scores.len()).as_bytes());
                    for s in scores {
                        match s {
                            Some(val) => write_resp_bulk(out, val.to_string().as_bytes()),
                            None => out.extend_from_slice(b"$-1\r\n"),
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Zrandmember { key, count, with_scores } => {
            match db.zrandmember(key, *count, *with_scores) {
                Ok(items) => {
                    if count.is_none() {
                        if let Some((m, _)) = items.first() {
                            write_resp_bulk(out, m);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else if *with_scores {
                        out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                        for (m, s) in items {
                            write_resp_bulk(out, &m);
                            write_resp_bulk(out, s.to_string().as_bytes());
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            write_resp_bulk(out, &m);
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Zremrangebyrank { key, start, stop } => {
            match db.zremrangebyrank(key, *start, *stop) {
                Ok(removed) => {
                    if removed > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, removed as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Zremrangebyscore { key, min_score, min_inc, max_score, max_inc } => {
            match db.zremrangebyscore(key, *min_score, *min_inc, *max_score, *max_inc) {
                Ok(removed) => {
                    if removed > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, removed as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Zremrangebylex { key, min, max } => {
            match db.zremrangebylex(key, min, max) {
                Ok(removed) => {
                    if removed > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, removed as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Zlexcount { key, min, max } => {
            match db.zlexcount(key, min, max) {
                Ok(count) => {
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Zscan { key, cursor, pattern, count } => {
            match db.zscan(key, *cursor, pattern.as_deref().map(|p| p.as_ref()), count.unwrap_or(10)) {
                Ok((next_cursor, entries)) => {
                    out.extend_from_slice(b"*2\r\n");
                    let cur_str = next_cursor.to_string();
                    write_resp_bulk(out, cur_str.as_bytes());
                    out.extend_from_slice(format!("*{}\r\n", entries.len() * 2).as_bytes());
                    for (m, s) in entries {
                        write_resp_bulk(out, &m);
                        write_resp_bulk(out, s.to_string().as_bytes());
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Ltrim { key, start, stop } => {
            match db.ltrim(key, *start, *stop) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Lset { key, index, element } => {
            match db.lset(key, *index, element.clone()) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Lrem { key, count, element } => {
            match db.lrem(key, *count, element) {
                Ok(removed) => {
                    if removed > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, removed as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Lpos { key, element, rank, count, maxlen } => {
            match db.lpos(key, element, rank.unwrap_or(1), *count, *maxlen) {
                Ok(indices) => {
                    if count.is_none() {
                        if let Some(idx) = indices.first() {
                            write_resp_integer(out, *idx as i64);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", indices.len()).as_bytes());
                        for idx in indices {
                            write_resp_integer(out, idx as i64);
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Linsert { key, before, pivot, element } => {
            match db.linsert(key.clone(), *before, pivot, element.clone()) {
                Ok(len) => {
                    if len > 0 {
                        record_change!(cmd);
                        crate::block::get_block_hub_for_port(db.port)
                            .lock()
                            .unwrap()
                            .notify_list(&mut db.table, key);
                    }
                    write_resp_integer(out, len);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Lmove { source, destination, where_from, where_to } => {
            match db.lmove(source, destination.clone(), *where_from, *where_to) {
                Ok(Some(val)) => {
                    record_change!(cmd);
                    crate::block::get_block_hub_for_port(db.port)
                        .lock()
                        .unwrap()
                        .notify_list(&mut db.table, destination);
                    write_resp_bulk(out, &val);
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Incrbyfloat { key, increment } => {
            match db.incrbyfloat(key.clone(), *increment) {
                Ok(val) => {
                    record_change!(cmd);
                    write_resp_bulk(out, val.to_string().as_bytes());
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Setrange { key, offset, value } => {
            match db.setrange(key.clone(), *offset, value) {
                Ok(len) => {
                    record_change!(cmd);
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Getrange { key, start, end } => {
            match db.getrange(key, *start, *end) {
                Ok(slice) => {
                    write_resp_bulk(out, &slice);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                    }
                }
            }
            false
        }
        Command::Time => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = now.as_secs();
            let micros = now.subsec_micros();
            out.extend_from_slice(
                format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    secs.to_string().len(),
                    secs,
                    micros.to_string().len(),
                    micros
                )
                .as_bytes(),
            );
            false
        }
        Command::Echo(msg) => {
            write_resp_bulk(out, msg);
            false
        }
        Command::Quit => {
            out.extend_from_slice(b"+OK\r\n");
            true
        }
        _ => false,
    }
}

async fn execute_commands_squashed(
    commands: Vec<Command>,
    router: &Router,
    responders: &[ResponderChannel],
    remote_batches: &mut [Vec<(usize, Command)>],
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
) -> bool {
    let mut can_squash = *authenticated;
    if can_squash {
        for cmd in &commands {
            if matches!(
                cmd,
                Command::Blpop { .. }
                    | Command::Brpop { .. }
                    | Command::Blmove { .. }
                    | Command::Hello { .. }
                    | Command::Reset
                    | Command::Auth { .. }
                    | Command::Acl(_)
                    | Command::Tier(_)
                    | Command::ConfigGet(_)
                    | Command::ConfigSet(_, _)
                    | Command::Xread { block_ms: Some(_), .. }
                    | Command::Xreadgroup { block_ms: Some(_), .. }
            ) {
                can_squash = false;
                break;
            }
            if let Some(k) = cmd_primary_key(cmd) {
                let slot = key_slot(k);
                if router.slot_states.borrow()[slot as usize] != crate::shard::SlotState::Stable {
                    can_squash = false;
                    break;
                }
                if router.local_db.borrow_mut().table.is_tiered(k).is_some() {
                    can_squash = false;
                    break;
                }
            } else if !matches!(cmd, Command::Ping(_) | Command::CommandDocs | Command::Quit | Command::Time | Command::Echo(_)) {
                can_squash = false;
                break;
            }
        }
    }

    if !can_squash {
        let mut should_close = false;
        for cmd in commands {
            if execute_command(
                cmd,
                router,
                client_id,
                client_registry,
                out,
                asking,
                authenticated,
                auth_user,
            )
            .await
            {
                should_close = true;
            }
        }
        return should_close;
    }

    *asking = false;

    let n = commands.len();
    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
        c.last_active = Instant::now();
        if let Some(last_cmd) = commands.last() {
            let cmd_name = get_cmd_name(last_cmd);
            c.last_cmd = cmd_name.to_string();
        }
    }
    let mut responses: Vec<CompactResp> = vec![CompactResp::empty(); n];
    let mut local_buf = Vec::with_capacity(128);
    let mut should_close = false;

    for batch in remote_batches.iter_mut() {
        batch.clear();
    }

    // 1. Process local shard commands immediately; bucket remote commands by shard
    for (idx, cmd) in commands.into_iter().enumerate() {
        if let Some(target) = target_shard_of_cmd(&cmd, router.num_shards) {
            if target == router.shard_id {
                local_buf.clear();
                if execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    &mut local_buf,
                    router.aof.as_deref(),
                ) {
                    should_close = true;
                }
                responses[idx] = CompactResp::from_slice(&local_buf);
            } else {
                remote_batches[target].push((idx, cmd));
            }
        } else {
            // Non-sharded simple commands (PING, QUIT, COMMAND DOCS) run locally
            local_buf.clear();
            if execute_local_command(
                &cmd,
                &mut router.local_db.borrow_mut(),
                &mut local_buf,
                None,
            ) {
                should_close = true;
            }
            responses[idx] = CompactResp::from_slice(&local_buf);
        }
    }

    // 2. Dispatch batched hops to all remote shards in parallel using pre-allocated channels
    let mut pending = Vec::new();
    for (target_shard, items) in remote_batches.iter_mut().enumerate() {
        if !items.is_empty() {
            let (tx, rx) = &responders[target_shard];
            let msg = ShardMessage::Batch {
                items: std::mem::take(items),
                responder: tx.clone(),
            };
            if router.senders[target_shard].send(msg).is_ok() {
                pending.push(rx);
            }
        }
    }

    // 3. Await parallel responses from all remote shards
    for rx in pending {
        if let Ok(results) = rx.recv_async().await {
            for (idx, resp) in results {
                responses[idx] = resp;
            }
        }
    }

    // 4. Append responses in exact FIFO pipeline order
    for resp in &responses {
        out.extend_from_slice(resp.as_slice());
    }

    should_close
}
