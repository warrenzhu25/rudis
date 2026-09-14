use std::rc::Rc;
use bytes::BytesMut;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use crate::resp::{parse_command, Command};
use crate::router::{target_shard, Router};
use crate::shard::{ShardDb, ShardMessage};

const READ_BUFFER_SIZE: usize = 65536;

pub async fn handle_connection(mut stream: TcpStream, router: Rc<Router>) {
    let mut buf = BytesMut::with_capacity(131072);
    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
    let mut out_buf = Vec::with_capacity(65536);

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

                // 2. Execute parsed commands with pipeline squashing when pipelined
                if !commands.is_empty() {
                    if commands.len() == 1 {
                        let quit = execute_command(commands.pop().unwrap(), &router, &mut out_buf).await;
                        if quit {
                            should_quit = true;
                        }
                    } else {
                        let quit = execute_commands_squashed(commands, &router, &mut out_buf).await;
                        if quit {
                            should_quit = true;
                        }
                    }
                }

                // 3. Batch flush all accumulated responses in one io_uring write
                if !out_buf.is_empty() {
                    let write_chunk =
                        std::mem::replace(&mut out_buf, Vec::with_capacity(65536));
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

async fn execute_command(cmd: Command, router: &Router, out: &mut Vec<u8>) -> bool {
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
        | Command::Ttl(key, _) => Some(target_shard(key, num_shards)),
        Command::Del(keys) | Command::Exists(keys) if keys.len() == 1 => {
            Some(target_shard(&keys[0], num_shards))
        }
        _ => None,
    }
}

pub fn execute_local_command(cmd: &Command, db: &mut ShardDb, out: &mut Vec<u8>) -> bool {
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
        Command::Set {
            key,
            value,
            expire_in,
        } => {
            db.set(key.clone(), value.clone(), *expire_in);
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
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Persist(key) => {
            let res = db.persist(key);
            if res {
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
    out: &mut Vec<u8>,
) -> bool {
    let mut can_squash = true;
    for cmd in &commands {
        if target_shard_of_cmd(cmd, router.num_shards).is_none()
            && !matches!(cmd, Command::Ping(_) | Command::CommandDocs | Command::Quit)
        {
            can_squash = false;
            break;
        }
    }

    if !can_squash {
        let mut should_close = false;
        for cmd in commands {
            if execute_command(cmd, router, out).await {
                should_close = true;
            }
        }
        return should_close;
    }

    let n = commands.len();
    let mut responses: Vec<Vec<u8>> = vec![Vec::new(); n];
    let mut remote_batches: Vec<Vec<(usize, Command)>> = vec![Vec::new(); router.num_shards];
    let mut should_close = false;

    // 1. Process local shard commands immediately; bucket remote commands by shard
    for (idx, cmd) in commands.into_iter().enumerate() {
        if let Some(target) = target_shard_of_cmd(&cmd, router.num_shards) {
            if target == router.shard_id {
                if execute_local_command(&cmd, &mut router.local_db.borrow_mut(), &mut responses[idx]) {
                    should_close = true;
                }
            } else {
                remote_batches[target].push((idx, cmd));
            }
        } else {
            // Non-sharded simple commands (PING, QUIT, COMMAND DOCS) run locally
            if execute_local_command(&cmd, &mut router.local_db.borrow_mut(), &mut responses[idx]) {
                should_close = true;
            }
        }
    }

    // 2. Dispatch batched hops to all remote shards in parallel
    let mut pending = Vec::new();
    for (target_shard, items) in remote_batches.into_iter().enumerate() {
        if !items.is_empty() {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Batch {
                items,
                responder: tx,
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
