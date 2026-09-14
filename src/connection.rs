use std::rc::Rc;
use bytes::BytesMut;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use crate::resp::{parse_command, Command};
use crate::router::Router;

const READ_BUFFER_SIZE: usize = 4096;

pub async fn handle_connection(mut stream: TcpStream, router: Rc<Router>) {
    let mut buf = BytesMut::with_capacity(8192);
    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];

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
                            let (response, quit) = execute_command(cmd, &router).await;
                            if let Err(_e) = stream.write_all(response).await.0 {
                                return;
                            }
                            if quit {
                                should_quit = true;
                                break;
                            }
                        }
                        Ok(None) => {
                            // Incomplete frame, need more data
                            break;
                        }
                        Err(err) => {
                            let err_resp = format!("-ERR {}\r\n", err).into_bytes();
                            let _ = stream.write_all(err_resp).await;
                            return;
                        }
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

async fn execute_command(cmd: Command, router: &Router) -> (Vec<u8>, bool) {
    match cmd {
        Command::Get(key) => {
            let val = router.get(key).await;
            match val {
                Some(v) => {
                    let mut resp = Vec::with_capacity(32 + v.len());
                    resp.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    resp.extend_from_slice(&v);
                    resp.extend_from_slice(b"\r\n");
                    (resp, false)
                }
                None => (b"$-1\r\n".to_vec(), false),
            }
        }
        Command::Set(key, value) => {
            router.set(key, value).await;
            (b"+OK\r\n".to_vec(), false)
        }
        Command::Ping(msg) => match msg {
            Some(m) => {
                let mut resp = Vec::with_capacity(32 + m.len());
                resp.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                resp.extend_from_slice(&m);
                resp.extend_from_slice(b"\r\n");
                (resp, false)
            }
            None => (b"+PONG\r\n".to_vec(), false),
        },
        Command::CommandDocs => (b"*0\r\n".to_vec(), false),
        Command::Info => {
            let info_str = format!(
                "# Server\r\nrudis_version:0.1.0\r\narch:shared-nothing-io_uring\r\nshard_id:{}\r\nnum_shards:{}\r\n",
                router.shard_id, router.num_shards
            );
            let mut resp = Vec::with_capacity(32 + info_str.len());
            resp.extend_from_slice(format!("${}\r\n", info_str.len()).as_bytes());
            resp.extend_from_slice(info_str.as_bytes());
            resp.extend_from_slice(b"\r\n");
            (resp, false)
        }
        Command::Quit => (b"+OK\r\n".to_vec(), true),
        Command::Unknown(cmd_name) => {
            let resp = format!("-ERR unknown command '{}'\r\n", cmd_name).into_bytes();
            (resp, false)
        }
    }
}
