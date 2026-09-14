use bytes::BytesMut;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt, Splitable};
use monoio::net::TcpStream;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Instant;

use crate::resp::{ClientSubcommand, ClusterSubcommand, Command, SetSlotSubcommand, parse_command};
use crate::router::{Router, key_slot, target_shard};
use crate::shard::{ShardDb, ShardMessage};

const READ_BUFFER_SIZE: usize = 65536;

pub type ResponderChannel = (
    flume::Sender<Vec<(usize, Vec<u8>)>>,
    flume::Receiver<Vec<(usize, Vec<u8>)>>,
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
                            should_quit = true;
                            break;
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

                // 3. Execute parsed commands with pipeline squashing when pipelined
                if !commands.is_empty() {
                    if commands.len() == 1 {
                        let quit = execute_command(
                            commands.pop().unwrap(),
                            &router,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
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
                        )
                        .await;
                        if quit {
                            should_quit = true;
                        }
                    }
                }

                // 4. Batch flush all accumulated responses in one io_uring write
                if !out_buf.is_empty() {
                    let write_chunk = std::mem::replace(&mut out_buf, Vec::with_capacity(65536));
                    if let Err(_e) = stream.write_all(write_chunk).await.0 {
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
        | Command::Rename { key, .. } => Some(key),
        Command::Touch(keys) | Command::Del(keys) | Command::Exists(keys) | Command::Mget(keys) => {
            keys.first()
        }
        Command::Mset(pairs) | Command::Msetnx(pairs) => pairs.first().map(|(k, _)| k),
        _ => None,
    }
}

async fn execute_command(
    cmd: Command,
    router: &Router,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    asking: &mut bool,
) -> bool {
    let cmd_name = match &cmd {
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
        Command::Lpush { .. } => "LPUSH",
        Command::Rpush { .. } => "RPUSH",
        Command::Lpop { .. } => "LPOP",
        Command::Rpop { .. } => "RPOP",
        Command::Lrange { .. } => "LRANGE",
        Command::Llen(_) => "LLEN",
        Command::Lindex { .. } => "LINDEX",
        Command::Sadd { .. } => "SADD",
        Command::Srem { .. } => "SREM",
        Command::Smembers(_) => "SMEMBERS",
        Command::Sismember { .. } => "SISMEMBER",
        Command::Scard(_) => "SCARD",
        Command::Spop { .. } => "SPOP",
        Command::Zadd { .. } => "ZADD",
        Command::Zrem { .. } => "ZREM",
        Command::Zscore { .. } => "ZSCORE",
        Command::Zcard(_) => "ZCARD",
        Command::Zrank { .. } => "ZRANK",
        Command::Zrevrank { .. } => "ZREVRANK",
        Command::Zcount { .. } => "ZCOUNT",
        Command::Zincrby { .. } => "ZINCRBY",
        Command::Zrange { .. } => "ZRANGE",
        Command::Zpopmin { .. } => "ZPOPMIN",
        Command::Zpopmax { .. } => "ZPOPMAX",
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
        Command::Ping(_) => "PING",
        Command::CommandDocs => "COMMAND",
        Command::Info => "INFO",
        Command::Quit => "QUIT",
        Command::Subscribe(_) => "SUBSCRIBE",
        Command::Unsubscribe(_) => "UNSUBSCRIBE",
        Command::Psubscribe(_) => "PSUBSCRIBE",
        Command::Punsubscribe(_) => "PUNSUBSCRIBE",
        Command::Publish { .. } => "PUBLISH",
        Command::PubsubChannels(_) => "PUBSUB CHANNELS",
        Command::PubsubNumsub(_) => "PUBSUB NUMSUB",
        Command::PubsubNumpat => "PUBSUB NUMPAT",
        Command::Unknown(_) => "UNKNOWN",
    };
    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
        c.last_active = Instant::now();
        c.last_cmd = cmd_name.to_string();
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
            for (key, val) in pairs {
                router.set(key, val, None).await;
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Del(keys) => {
            let mut count = 0usize;
            for key in keys {
                if router.del(key).await {
                    count += 1;
                }
            }
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Exists(keys) => {
            let mut count = 0usize;
            for key in keys {
                if router.exists(key).await {
                    count += 1;
                }
            }
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::IncrBy(key, delta) => {
            match router.incr_by(key, delta).await {
                Ok(val) => {
                    out.extend_from_slice(format!(":{}\r\n", val).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Expire(key, duration) => {
            let res = router.expire(key, duration).await;
            if res {
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Persist(key) => {
            let res = router.persist(key).await;
            if res {
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
        Command::Info => {
            let info_str = format!(
                "# Server\r\nrudis_version:0.1.0\r\narch:shared-nothing-io_uring\r\nshard_id:{}\r\nnum_shards:{}\r\n",
                router.shard_id, router.num_shards
            );
            out.extend_from_slice(format!("${}\r\n", info_str.len()).as_bytes());
            out.extend_from_slice(info_str.as_bytes());
            out.extend_from_slice(b"\r\n");
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
                    let mut nodes = String::new();
                    for s in 0..router.num_shards {
                        let start_slot = s * 16384 / router.num_shards;
                        let end_slot = if s == router.num_shards - 1 {
                            16383
                        } else {
                            (s + 1) * 16384 / router.num_shards - 1
                        };
                        let node_id = format!("{:040x}", s + 1);
                        let myself = if s == router.shard_id { "myself," } else { "" };
                        nodes.push_str(&format!(
                            "{} 127.0.0.1:{}@{} {}master - 0 0 {} connected {}-{}\n",
                            node_id,
                            router.port,
                            router.port + 10000,
                            myself,
                            s + 1,
                            start_slot,
                            end_slot
                        ));
                    }
                    out.extend_from_slice(format!("${}\r\n", nodes.len()).as_bytes());
                    out.extend_from_slice(nodes.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::Info => {
                    let info = format!(
                        "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\n",
                        router.num_shards, router.num_shards
                    );
                    out.extend_from_slice(format!("${}\r\n", info.len()).as_bytes());
                    out.extend_from_slice(info.as_bytes());
                    out.extend_from_slice(b"\r\n");
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
        | Command::Strlen(_) => {
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
        Command::Save | Command::Bgsave => {
            router.sync_aof().await;
            out.extend_from_slice(b"+OK\r\n");
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
                        for (m, score) in &zset.dict {
                            let s = score.to_string();
                            tx_buf
                                .extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
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
        | Command::Strlen(key) => Some(target_shard(key, num_shards)),
        Command::Rename { key, newkey, .. } => {
            let s1 = target_shard(key, num_shards);
            let s2 = target_shard(newkey, num_shards);
            if s1 == s2 { Some(s1) } else { None }
        }
        Command::Touch(keys) | Command::Del(keys) | Command::Exists(keys) if keys.len() == 1 => {
            Some(target_shard(&keys[0], num_shards))
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
            if let Some(aof) = aof {
                if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                    aof.borrow_mut().append(&bytes);
                }
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Mset(pairs) => {
            for (k, v) in pairs {
                db.set(k.clone(), v.clone(), None);
            }
            if let Some(aof) = aof {
                if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                    aof.borrow_mut().append(&bytes);
                }
            }
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
                if let Some(aof) = aof {
                    if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                        aof.borrow_mut().append(&bytes);
                    }
                }
            }
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Exists(keys) => {
            let mut count = 0usize;
            for k in keys {
                if db.exists(k) {
                    count += 1;
                }
            }
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::IncrBy(key, delta) => {
            match db.incr_by(key.clone(), *delta) {
                Ok(val) => {
                    if let Some(aof) = aof {
                        if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                            aof.borrow_mut().append(&bytes);
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", val).as_bytes());
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
                if let Some(aof) = aof {
                    if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                        aof.borrow_mut().append(&bytes);
                    }
                }
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Persist(key) => {
            let res = db.persist(key);
            if res {
                if let Some(aof) = aof {
                    if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                        aof.borrow_mut().append(&bytes);
                    }
                }
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Ttl(key, in_millis) => {
            let res = db.ttl(key, *in_millis);
            out.extend_from_slice(format!(":{}\r\n", res).as_bytes());
            false
        }
        Command::Hset { key, fields } => {
            match db.hset(key.clone(), fields.clone()) {
                Ok(count) => {
                    if let Some(aof) = aof {
                        if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                            aof.borrow_mut().append(&bytes);
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
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
                    if let Some(aof) = aof {
                        if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                            aof.borrow_mut().append(&bytes);
                        }
                    }
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
                        if let Some(aof) = aof {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
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
                    if let Some(aof) = aof {
                        if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                            aof.borrow_mut().append(&bytes);
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
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
                    if let Some(aof) = aof {
                        if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                            aof.borrow_mut().append(&bytes);
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
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
                        if let Some(aof) = aof {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                        if let Some(aof) = aof {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                        if let Some(aof) = aof {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                        if let Some(aof) = aof {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                        if let Some(aof) = aof {
                            let srem_cmd = Command::Srem {
                                key: key.clone(),
                                members: popped.clone(),
                            };
                            if let Some(bytes) = crate::aof::command_to_resp(&srem_cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
        // ZSET COMMANDS
        Command::Zadd {
            key,
            elements,
            flags,
        } => {
            match db.zadd(key.clone(), elements.clone(), *flags) {
                Ok((count, incr_score)) => {
                    if let Some(aof) = aof {
                        if flags.incr {
                            if incr_score.is_some() {
                                if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                    aof.borrow_mut().append(&bytes);
                                }
                            }
                        } else if count > 0 {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
                    }
                    if flags.incr {
                        if let Some(score) = incr_score {
                            let s = score.to_string();
                            out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
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
                        if let Some(aof) = aof {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                    if let Some(aof) = aof {
                        if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                            aof.borrow_mut().append(&bytes);
                        }
                    }
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
                        if let Some(aof) = aof {
                            let zrem_cmd = Command::Zrem {
                                key: key.clone(),
                                members: popped.iter().map(|(m, _)| m.clone()).collect(),
                            };
                            if let Some(bytes) = crate::aof::command_to_resp(&zrem_cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                        if let Some(aof) = aof {
                            let zrem_cmd = Command::Zrem {
                                key: key.clone(),
                                members: popped.iter().map(|(m, _)| m.clone()).collect(),
                            };
                            if let Some(bytes) = crate::aof::command_to_resp(&zrem_cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
            if let Some(aof) = aof {
                if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                    aof.borrow_mut().append(&bytes);
                }
            }
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
                        if let Some(aof) = aof {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                if let Some(aof) = aof {
                    if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                        aof.borrow_mut().append(&bytes);
                    }
                }
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Getset { key, value } => {
            match db.getset(key.clone(), value.clone()) {
                Ok(old) => {
                    if let Some(aof) = aof {
                        if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                            aof.borrow_mut().append(&bytes);
                        }
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
        Command::Getdel(key) => {
            match db.getdel(key) {
                Ok(old) => {
                    if old.is_some() {
                        if let Some(aof) = aof {
                            if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
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
                    if let Some(aof) = aof {
                        if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                            aof.borrow_mut().append(&bytes);
                        }
                    }
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
                if let Some(aof) = aof {
                    if let Some(bytes) = crate::aof::command_to_resp(cmd) {
                        aof.borrow_mut().append(&bytes);
                    }
                }
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
) -> bool {
    let mut can_squash = true;
    for cmd in &commands {
        if let Some(k) = cmd_primary_key(cmd) {
            let slot = key_slot(k);
            if router.slot_states.borrow()[slot as usize] != crate::shard::SlotState::Stable {
                can_squash = false;
                break;
            }
        } else if !matches!(cmd, Command::Ping(_) | Command::CommandDocs | Command::Quit) {
            can_squash = false;
            break;
        }
    }

    if !can_squash {
        let mut should_close = false;
        for cmd in commands {
            if execute_command(cmd, router, client_id, client_registry, out, asking).await {
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
            let cmd_name = match last_cmd {
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
                Command::Lpush { .. } => "LPUSH",
                Command::Rpush { .. } => "RPUSH",
                Command::Lpop { .. } => "LPOP",
                Command::Rpop { .. } => "RPOP",
                Command::Lrange { .. } => "LRANGE",
                Command::Llen(_) => "LLEN",
                Command::Lindex { .. } => "LINDEX",
                Command::Sadd { .. } => "SADD",
                Command::Srem { .. } => "SREM",
                Command::Smembers(_) => "SMEMBERS",
                Command::Sismember { .. } => "SISMEMBER",
                Command::Scard(_) => "SCARD",
                Command::Spop { .. } => "SPOP",
                Command::Zadd { .. } => "ZADD",
                Command::Zrem { .. } => "ZREM",
                Command::Zscore { .. } => "ZSCORE",
                Command::Zcard(_) => "ZCARD",
                Command::Zrank { .. } => "ZRANK",
                Command::Zrevrank { .. } => "ZREVRANK",
                Command::Zcount { .. } => "ZCOUNT",
                Command::Zincrby { .. } => "ZINCRBY",
                Command::Zrange { .. } => "ZRANGE",
                Command::Zpopmin { .. } => "ZPOPMIN",
                Command::Zpopmax { .. } => "ZPOPMAX",
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
                Command::Ping(_) => "PING",
                Command::CommandDocs => "COMMAND",
                Command::Info => "INFO",
                Command::Quit => "QUIT",
                Command::Subscribe(_) => "SUBSCRIBE",
                Command::Unsubscribe(_) => "UNSUBSCRIBE",
                Command::Psubscribe(_) => "PSUBSCRIBE",
                Command::Punsubscribe(_) => "PUNSUBSCRIBE",
                Command::Publish { .. } => "PUBLISH",
                Command::PubsubChannels(_) => "PUBSUB CHANNELS",
                Command::PubsubNumsub(_) => "PUBSUB NUMSUB",
                Command::PubsubNumpat => "PUBSUB NUMPAT",
                Command::Unknown(_) => "UNKNOWN",
            };
            c.last_cmd = cmd_name.to_string();
        }
    }
    let mut responses: Vec<Vec<u8>> = vec![Vec::new(); n];
    let mut should_close = false;

    for batch in remote_batches.iter_mut() {
        batch.clear();
    }

    // 1. Process local shard commands immediately; bucket remote commands by shard
    for (idx, cmd) in commands.into_iter().enumerate() {
        if let Some(target) = target_shard_of_cmd(&cmd, router.num_shards) {
            if target == router.shard_id {
                if execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    &mut responses[idx],
                    router.aof.as_deref(),
                ) {
                    should_close = true;
                }
            } else {
                remote_batches[target].push((idx, cmd));
            }
        } else {
            // Non-sharded simple commands (PING, QUIT, COMMAND DOCS) run locally
            if execute_local_command(
                &cmd,
                &mut router.local_db.borrow_mut(),
                &mut responses[idx],
                None,
            ) {
                should_close = true;
            }
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
    for resp in responses {
        out.extend_from_slice(&resp);
    }

    should_close
}
