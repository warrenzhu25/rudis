use std::rc::Rc;
use bytes::BytesMut;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use crate::resp::{parse_command, Command};
use crate::router::Router;

const READ_BUFFER_SIZE: usize = 65536;
const MAX_BATCH_WRITE: usize = 65536;

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

                // Process all complete commands currently in the buffer
                let mut should_quit = false;
                while !buf.is_empty() {
                    match parse_command(&mut buf) {
                        Ok(Some(cmd)) => {
                            let quit = execute_command(cmd, &router, &mut out_buf).await;
                            if quit {
                                should_quit = true;
                                break;
                            }

                            // Flush early if write batch buffer exceeds threshold
                            if out_buf.len() >= MAX_BATCH_WRITE {
                                let write_chunk =
                                    std::mem::replace(&mut out_buf, Vec::with_capacity(65536));
                                if let Err(_e) = stream.write_all(write_chunk).await.0 {
                                    return;
                                }
                            }
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

                // Batch flush all accumulated responses in one io_uring write
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
        Command::Set(key, value) => {
            router.set(key, value).await;
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
