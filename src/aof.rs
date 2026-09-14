use std::path::{Path, PathBuf};
use bytes::BytesMut;

use crate::resp::Command;
use crate::shard::ShardDb;

#[derive(Clone, Debug)]
pub struct AofConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub fsync_every_sec: bool,
}

impl Default for AofConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: PathBuf::from("."),
            fsync_every_sec: true,
        }
    }
}

pub struct AofWriter {
    buffer: Vec<u8>,
    file: Option<monoio::fs::File>,
    path: PathBuf,
    offset: u64,
}

impl AofWriter {
    pub async fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let file = monoio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await?;
        let offset = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            buffer: Vec::with_capacity(65536),
            file: Some(file),
            path,
            offset,
        })
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline]
    pub fn append(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if let Some(file) = &self.file {
            let chunk = std::mem::replace(&mut self.buffer, Vec::with_capacity(65536));
            let len = chunk.len() as u64;
            let (res, _) = file.write_all_at(chunk, self.offset).await;
            res?;
            self.offset += len;
        }
        Ok(())
    }

    pub async fn sync(&mut self) -> std::io::Result<()> {
        self.flush().await?;
        if let Some(file) = &self.file {
            file.sync_data().await?;
        }
        Ok(())
    }
}

pub fn command_to_resp(cmd: &Command) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    match cmd {
        Command::Set {
            key,
            value,
            expire_in,
        } => {
            if let Some(dur) = expire_in {
                let ms = dur.as_millis().max(1);
                let ms_str = ms.to_string();
                buf.extend_from_slice(
                    format!("*5\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes(),
                );
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n", value.len()).as_bytes());
                buf.extend_from_slice(value);
                buf.extend_from_slice(
                    format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                );
            } else {
                buf.extend_from_slice(
                    format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes(),
                );
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n", value.len()).as_bytes());
                buf.extend_from_slice(value);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Mset(pairs) => {
            buf.extend_from_slice(format!("*{}\r\n$4\r\nMSET\r\n", 1 + pairs.len() * 2).as_bytes());
            for (k, v) in pairs {
                buf.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Del(keys) => {
            buf.extend_from_slice(format!("*{}\r\n$3\r\nDEL\r\n", 1 + keys.len()).as_bytes());
            for k in keys {
                buf.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::IncrBy(key, delta) => {
            let d_str = delta.to_string();
            buf.extend_from_slice(format!("*3\r\n$6\r\nINCRBY\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", d_str.len(), d_str).as_bytes());
            Some(buf)
        }
        Command::Expire(key, dur) => {
            let ms = dur.as_millis().max(1);
            let ms_str = ms.to_string();
            buf.extend_from_slice(
                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes());
            Some(buf)
        }
        Command::Persist(key) => {
            buf.extend_from_slice(format!("*2\r\n$7\r\nPERSIST\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Hset { key, fields } | Command::Hmset { key, fields } => {
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nHSET\r\n${}\r\n", 2 + fields.len() * 2, key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for (f, v) in fields {
                buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                buf.extend_from_slice(f);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Hdel { key, fields } => {
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nHDEL\r\n${}\r\n", 2 + fields.len(), key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for f in fields {
                buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                buf.extend_from_slice(f);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Lpush { key, values } => {
            buf.extend_from_slice(
                format!("*{}\r\n$5\r\nLPUSH\r\n${}\r\n", 2 + values.len(), key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for v in values {
                buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Rpush { key, values } => {
            buf.extend_from_slice(
                format!("*{}\r\n$5\r\nRPUSH\r\n${}\r\n", 2 + values.len(), key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for v in values {
                buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Lpop { key, count } => {
            if let Some(c) = count {
                let c_str = c.to_string();
                buf.extend_from_slice(
                    format!("*3\r\n$4\r\nLPOP\r\n${}\r\n", key.len()).as_bytes(),
                );
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", c_str.len(), c_str).as_bytes());
            } else {
                buf.extend_from_slice(
                    format!("*2\r\n$4\r\nLPOP\r\n${}\r\n", key.len()).as_bytes(),
                );
                buf.extend_from_slice(key);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Rpop { key, count } => {
            if let Some(c) = count {
                let c_str = c.to_string();
                buf.extend_from_slice(
                    format!("*3\r\n$4\r\nRPOP\r\n${}\r\n", key.len()).as_bytes(),
                );
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", c_str.len(), c_str).as_bytes());
            } else {
                buf.extend_from_slice(
                    format!("*2\r\n$4\r\nRPOP\r\n${}\r\n", key.len()).as_bytes(),
                );
                buf.extend_from_slice(key);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Sadd { key, members } => {
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nSADD\r\n${}\r\n", 2 + members.len(), key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for m in members {
                buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                buf.extend_from_slice(m);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Srem { key, members } => {
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nSREM\r\n${}\r\n", 2 + members.len(), key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for m in members {
                buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                buf.extend_from_slice(m);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Spop { key, count } => {
            if let Some(c) = count {
                let c_str = c.to_string();
                buf.extend_from_slice(
                    format!("*3\r\n$4\r\nSPOP\r\n${}\r\n", key.len()).as_bytes(),
                );
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", c_str.len(), c_str).as_bytes());
            } else {
                buf.extend_from_slice(
                    format!("*2\r\n$4\r\nSPOP\r\n${}\r\n", key.len()).as_bytes(),
                );
                buf.extend_from_slice(key);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Zadd { key, elements, flags } => {
            let mut num_args = 2 + elements.len() * 2;
            if flags.nx { num_args += 1; }
            if flags.xx { num_args += 1; }
            if flags.gt { num_args += 1; }
            if flags.lt { num_args += 1; }
            if flags.ch { num_args += 1; }
            if flags.incr { num_args += 1; }
            buf.extend_from_slice(format!("*{}\r\n$4\r\nZADD\r\n${}\r\n", num_args, key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            if flags.nx { buf.extend_from_slice(b"$2\r\nNX\r\n"); }
            if flags.xx { buf.extend_from_slice(b"$2\r\nXX\r\n"); }
            if flags.gt { buf.extend_from_slice(b"$2\r\nGT\r\n"); }
            if flags.lt { buf.extend_from_slice(b"$2\r\nLT\r\n"); }
            if flags.ch { buf.extend_from_slice(b"$2\r\nCH\r\n"); }
            if flags.incr { buf.extend_from_slice(b"$4\r\nINCR\r\n"); }
            for (s, m) in elements {
                let s_str = s.to_string();
                buf.extend_from_slice(format!("${}\r\n{}\r\n", s_str.len(), s_str).as_bytes());
                buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                buf.extend_from_slice(m);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Zrem { key, members } => {
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nZREM\r\n${}\r\n", 2 + members.len(), key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for m in members {
                buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                buf.extend_from_slice(m);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Zincrby { key, delta, member } => {
            let d_str = delta.to_string();
            buf.extend_from_slice(format!("*4\r\n$7\r\nZINCRBY\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", d_str.len(), d_str).as_bytes());
            buf.extend_from_slice(format!("${}\r\n", member.len()).as_bytes());
            buf.extend_from_slice(member);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        _ => None,
    }
}

pub fn replay_aof(path: &Path, db: &mut ShardDb) -> std::io::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let data = std::fs::read(path)?;
    if data.is_empty() {
        return Ok(0);
    }
    let mut buf = BytesMut::from(&data[..]);
    let mut count = 0;
    let mut dummy_out = Vec::new();
    while !buf.is_empty() {
        match crate::resp::parse_command(&mut buf) {
            Ok(Some(cmd)) => {
                crate::connection::execute_local_command(&cmd, db, &mut dummy_out, None);
                dummy_out.clear();
                count += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    Ok(count)
}
