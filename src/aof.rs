use bytes::BytesMut;
use std::path::{Path, PathBuf};

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
    spare_buffer: Option<Vec<u8>>,
    file: Option<std::rc::Rc<monoio::fs::File>>,
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
            spare_buffer: None,
            file: Some(std::rc::Rc::new(file)),
            path,
            offset,
        })
    }

    pub fn new_in_memory() -> Self {
        Self {
            buffer: Vec::with_capacity(65536),
            spare_buffer: None,
            file: None,
            path: PathBuf::new(),
            offset: 0,
        }
    }

    #[inline]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline]
    pub fn append(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    #[inline]
    pub fn recycle_chunk(&mut self, mut chunk: Vec<u8>) {
        if chunk.capacity() <= 4 * 1024 * 1024 {
            chunk.clear();
            self.spare_buffer = Some(chunk);
        }
    }

    #[inline]
    pub fn take_flush_chunk(&mut self) -> Option<(std::rc::Rc<monoio::fs::File>, Vec<u8>, u64)> {
        if self.buffer.is_empty() {
            return None;
        }
        let file = self.file.clone()?;
        let next_buf = self
            .spare_buffer
            .take()
            .unwrap_or_else(|| Vec::with_capacity(65536));
        let chunk = std::mem::replace(&mut self.buffer, next_buf);
        let off = self.offset;
        self.offset += chunk.len() as u64;
        Some((file, chunk, off))
    }

    #[inline]
    pub fn get_file(&self) -> Option<std::rc::Rc<monoio::fs::File>> {
        self.file.clone()
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        if let Some((file, chunk, offset)) = self.take_flush_chunk() {
            let (res, _) = file.write_all_at(chunk, offset).await;
            res?;
        }
        Ok(())
    }

    pub async fn flush_rc(aof: &std::rc::Rc<std::cell::RefCell<Self>>) -> std::io::Result<()> {
        let chunk_and_off = aof.borrow_mut().take_flush_chunk();
        if let Some((file, chunk, offset)) = chunk_and_off {
            let (res, _) = file.write_all_at(chunk, offset).await;
            res?;
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

    pub async fn reopen_after_rewrite(
        aof: &std::rc::Rc<std::cell::RefCell<Self>>,
    ) -> std::io::Result<u64> {
        let path = aof.borrow().path.clone();
        if path.as_os_str().is_empty() {
            return Ok(0);
        }
        let file = monoio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await?;
        let new_size = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        let rc_file = std::rc::Rc::new(file);
        let chunk_and_off = {
            let mut writer = aof.borrow_mut();
            writer.file = Some(rc_file.clone());
            writer.offset = new_size;
            if !writer.buffer.is_empty() {
                let chunk = std::mem::replace(&mut writer.buffer, Vec::with_capacity(65536));
                let off = writer.offset;
                writer.offset += chunk.len() as u64;
                Some((chunk, off))
            } else {
                None
            }
        };
        if let Some((chunk, off)) = chunk_and_off {
            let (res, _) = rc_file.write_all_at(chunk, off).await;
            res?;
        }
        Ok(new_size)
    }
}

pub fn command_to_resp(cmd: &Command) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    match cmd {
        Command::Set {
            key,
            value,
            expire_in,
            ..
        } => {
            if let Some(dur) = expire_in {
                let ms = dur.as_millis().max(1);
                let ms_str = ms.to_string();
                buf.extend_from_slice(format!("*5\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n", value.len()).as_bytes());
                buf.extend_from_slice(value);
                buf.extend_from_slice(
                    format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                );
            } else {
                buf.extend_from_slice(format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes());
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
        Command::Msetex {
            pairs,
            condition,
            expiry,
        } => {
            let mut num_args = 2 + pairs.len() * 2;
            if *condition != crate::resp::MsetexCondition::None {
                num_args += 1;
            }
            match expiry {
                crate::resp::MsetexExpiry::None => {}
                crate::resp::MsetexExpiry::KeepTtl => num_args += 1,
                crate::resp::MsetexExpiry::ExpireIn(_) => num_args += 2,
            }
            buf.extend_from_slice(format!("*{}\r\n$6\r\nMSETEX\r\n", num_args).as_bytes());
            let numkeys_str = pairs.len().to_string();
            buf.extend_from_slice(
                format!("${}\r\n{}\r\n", numkeys_str.len(), numkeys_str).as_bytes(),
            );
            for (k, v) in pairs {
                buf.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            match condition {
                crate::resp::MsetexCondition::Nx => buf.extend_from_slice(b"$2\r\nNX\r\n"),
                crate::resp::MsetexCondition::Xx => buf.extend_from_slice(b"$2\r\nXX\r\n"),
                crate::resp::MsetexCondition::None => {}
            }
            match expiry {
                crate::resp::MsetexExpiry::KeepTtl => buf.extend_from_slice(b"$7\r\nKEEPTTL\r\n"),
                crate::resp::MsetexExpiry::ExpireIn(d) => {
                    buf.extend_from_slice(b"$2\r\nPX\r\n");
                    let ms_str = d.as_millis().to_string();
                    buf.extend_from_slice(
                        format!("${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
                crate::resp::MsetexExpiry::None => {}
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
        Command::Expire { key, duration, .. } => {
            let ms = duration.as_millis().max(1);
            let ms_str = ms.to_string();
            buf.extend_from_slice(format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", key.len()).as_bytes());
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
                format!(
                    "*{}\r\n$4\r\nHSET\r\n${}\r\n",
                    2 + fields.len() * 2,
                    key.len()
                )
                .as_bytes(),
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
        Command::Hsetnx { key, field, value } => {
            buf.extend_from_slice(format!("*4\r\n$6\r\nHSETNX\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(format!("${}\r\n", field.len()).as_bytes());
            buf.extend_from_slice(field);
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Hdel { key, fields } | Command::Hgetdel { key, fields } => {
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
                buf.extend_from_slice(format!("*3\r\n$4\r\nLPOP\r\n${}\r\n", key.len()).as_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", c_str.len(), c_str).as_bytes());
            } else {
                buf.extend_from_slice(format!("*2\r\n$4\r\nLPOP\r\n${}\r\n", key.len()).as_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Rpop { key, count } => {
            if let Some(c) = count {
                let c_str = c.to_string();
                buf.extend_from_slice(format!("*3\r\n$4\r\nRPOP\r\n${}\r\n", key.len()).as_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", c_str.len(), c_str).as_bytes());
            } else {
                buf.extend_from_slice(format!("*2\r\n$4\r\nRPOP\r\n${}\r\n", key.len()).as_bytes());
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
                buf.extend_from_slice(format!("*3\r\n$4\r\nSPOP\r\n${}\r\n", key.len()).as_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", c_str.len(), c_str).as_bytes());
            } else {
                buf.extend_from_slice(format!("*2\r\n$4\r\nSPOP\r\n${}\r\n", key.len()).as_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Zadd {
            key,
            elements,
            flags,
        } => {
            let mut num_args = 2 + elements.len() * 2;
            if flags.nx {
                num_args += 1;
            }
            if flags.xx {
                num_args += 1;
            }
            if flags.gt {
                num_args += 1;
            }
            if flags.lt {
                num_args += 1;
            }
            if flags.ch {
                num_args += 1;
            }
            if flags.incr {
                num_args += 1;
            }
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nZADD\r\n${}\r\n", num_args, key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            if flags.nx {
                buf.extend_from_slice(b"$2\r\nNX\r\n");
            }
            if flags.xx {
                buf.extend_from_slice(b"$2\r\nXX\r\n");
            }
            if flags.gt {
                buf.extend_from_slice(b"$2\r\nGT\r\n");
            }
            if flags.lt {
                buf.extend_from_slice(b"$2\r\nLT\r\n");
            }
            if flags.ch {
                buf.extend_from_slice(b"$2\r\nCH\r\n");
            }
            if flags.incr {
                buf.extend_from_slice(b"$4\r\nINCR\r\n");
            }
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
        Command::Setnx { key, value } => {
            buf.extend_from_slice(format!("*3\r\n$5\r\nSETNX\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", value.len()).as_bytes());
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Getset { key, value } => {
            buf.extend_from_slice(format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", value.len()).as_bytes());
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Getdel(key) => {
            buf.extend_from_slice(format!("*2\r\n$3\r\nDEL\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Append { key, value } => {
            buf.extend_from_slice(format!("*3\r\n$6\r\nAPPEND\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", value.len()).as_bytes());
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Rename { key, newkey, .. } => {
            buf.extend_from_slice(format!("*3\r\n$6\r\nRENAME\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", newkey.len()).as_bytes());
            buf.extend_from_slice(newkey);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Flushdb | Command::Flushall => {
            buf.extend_from_slice(b"*1\r\n$7\r\nFLUSHDB\r\n");
            Some(buf)
        }
        Command::Msetnx(pairs) => {
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
        Command::Setbit { key, offset, value } => {
            let off_str = offset.to_string();
            let val_str = value.to_string();
            buf.extend_from_slice(format!("*4\r\n$6\r\nSETBIT\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", off_str.len(), off_str).as_bytes());
            buf.extend_from_slice(format!("${}\r\n{}\r\n", val_str.len(), val_str).as_bytes());
            Some(buf)
        }
        Command::Bitop {
            op,
            destkey,
            srckeys,
        } => {
            buf.extend_from_slice(
                format!(
                    "*{}\r\n$5\r\nBITOP\r\n${}\r\n{}\r\n${}\r\n",
                    3 + srckeys.len(),
                    op.len(),
                    op,
                    destkey.len()
                )
                .as_bytes(),
            );
            buf.extend_from_slice(destkey);
            buf.extend_from_slice(b"\r\n");
            for k in srckeys {
                buf.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Pfadd { key, elements } => {
            buf.extend_from_slice(
                format!(
                    "*{}\r\n$5\r\nPFADD\r\n${}\r\n",
                    2 + elements.len(),
                    key.len()
                )
                .as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for e in elements {
                buf.extend_from_slice(format!("${}\r\n", e.len()).as_bytes());
                buf.extend_from_slice(e);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Pfmerge { destkey, srckeys } => {
            buf.extend_from_slice(
                format!(
                    "*{}\r\n$7\r\nPFMERGE\r\n${}\r\n",
                    2 + srckeys.len(),
                    destkey.len()
                )
                .as_bytes(),
            );
            buf.extend_from_slice(destkey);
            buf.extend_from_slice(b"\r\n");
            for k in srckeys {
                buf.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Restore {
            key,
            ttl_ms,
            serialized,
            replace,
            absttl,
        } => {
            let mut num_args = 4;
            if *replace {
                num_args += 1;
            }
            if *absttl {
                num_args += 1;
            }
            let ttl_str = ttl_ms.to_string();
            buf.extend_from_slice(
                format!("*{}\r\n$7\r\nRESTORE\r\n${}\r\n", num_args, key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!(
                    "\r\n${}\r\n{}\r\n${}\r\n",
                    ttl_str.len(),
                    ttl_str,
                    serialized.len()
                )
                .as_bytes(),
            );
            buf.extend_from_slice(serialized);
            buf.extend_from_slice(b"\r\n");
            if *replace {
                buf.extend_from_slice(b"$7\r\nREPLACE\r\n");
            }
            if *absttl {
                buf.extend_from_slice(b"$6\r\nABSTTL\r\n");
            }
            Some(buf)
        }
        Command::Xadd {
            key,
            nomkstream,
            maxlen,
            minid,
            id,
            fields,
        } => {
            let mut num_args = 2 + 1 + fields.len() * 2;
            if *nomkstream {
                num_args += 1;
            }
            if maxlen.is_some() {
                num_args += 2;
            }
            if minid.is_some() {
                num_args += 2;
            }
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nXADD\r\n${}\r\n", num_args, key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            if *nomkstream {
                buf.extend_from_slice(b"$10\r\nNOMKSTREAM\r\n");
            }
            if let Some(max) = maxlen {
                let max_str = max.to_string();
                buf.extend_from_slice(b"$6\r\nMAXLEN\r\n");
                buf.extend_from_slice(format!("${}\r\n{}\r\n", max_str.len(), max_str).as_bytes());
            }
            if let Some(min) = minid {
                let min_str = min.to_string();
                buf.extend_from_slice(b"$5\r\nMINID\r\n");
                buf.extend_from_slice(format!("${}\r\n{}\r\n", min_str.len(), min_str).as_bytes());
            }
            let id_str = match id {
                crate::table::StreamAddId::Explicit(sid) => sid.to_string(),
                crate::table::StreamAddId::AutoSeq(ms) => format!("{}-*", ms),
                crate::table::StreamAddId::Auto => "*".to_string(),
            };
            buf.extend_from_slice(format!("${}\r\n{}\r\n", id_str.len(), id_str).as_bytes());
            for (k, v) in fields {
                buf.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Xdel { key, ids } => {
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nXDEL\r\n${}\r\n", 2 + ids.len(), key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            for id in ids {
                let s = id.to_string();
                buf.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
            }
            Some(buf)
        }
        Command::Xtrim { key, maxlen, minid } => {
            let mut num_args = 2;
            if maxlen.is_some() {
                num_args += 2;
            }
            if minid.is_some() {
                num_args += 2;
            }
            buf.extend_from_slice(
                format!("*{}\r\n$5\r\nXTRIM\r\n${}\r\n", num_args, key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
            if let Some(max) = maxlen {
                let max_str = max.to_string();
                buf.extend_from_slice(b"$6\r\nMAXLEN\r\n");
                buf.extend_from_slice(format!("${}\r\n{}\r\n", max_str.len(), max_str).as_bytes());
            }
            if let Some(min) = minid {
                let min_str = min.to_string();
                buf.extend_from_slice(b"$5\r\nMINID\r\n");
                buf.extend_from_slice(format!("${}\r\n{}\r\n", min_str.len(), min_str).as_bytes());
            }
            Some(buf)
        }
        Command::XgroupCreate {
            key,
            group,
            id,
            mkstream,
        } => {
            let id_str = id.to_string();
            let mut num_args = 5;
            if *mkstream {
                num_args += 1;
            }
            buf.extend_from_slice(
                format!(
                    "*{}\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n",
                    num_args,
                    key.len()
                )
                .as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", group.len()).as_bytes());
            buf.extend_from_slice(group);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", id_str.len(), id_str).as_bytes());
            if *mkstream {
                buf.extend_from_slice(b"$8\r\nMKSTREAM\r\n");
            }
            Some(buf)
        }
        Command::XgroupDestroy { key, group } => {
            buf.extend_from_slice(
                format!("*4\r\n$6\r\nXGROUP\r\n$7\r\nDESTROY\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", group.len()).as_bytes());
            buf.extend_from_slice(group);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Xack { key, group, ids } => {
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nXACK\r\n${}\r\n", 3 + ids.len(), key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", group.len()).as_bytes());
            buf.extend_from_slice(group);
            buf.extend_from_slice(b"\r\n");
            for id in ids {
                let s = id.to_string();
                buf.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
            }
            Some(buf)
        }
        Command::Hincrby {
            key,
            field,
            increment,
        } => {
            let s = increment.to_string();
            buf.extend_from_slice(format!("*4\r\n$7\r\nHINCRBY\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", field.len()).as_bytes());
            buf.extend_from_slice(field);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", s.len(), s).as_bytes());
            Some(buf)
        }
        Command::Hincrbyfloat {
            key,
            field,
            increment,
        } => {
            let s = increment.to_string();
            buf.extend_from_slice(
                format!("*4\r\n$12\r\nHINCRBYFLOAT\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", field.len()).as_bytes());
            buf.extend_from_slice(field);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", s.len(), s).as_bytes());
            Some(buf)
        }
        Command::Smove {
            source,
            destination,
            member,
        } => {
            buf.extend_from_slice(format!("*4\r\n$5\r\nSMOVE\r\n${}\r\n", source.len()).as_bytes());
            buf.extend_from_slice(source);
            buf.extend_from_slice(format!("\r\n${}\r\n", destination.len()).as_bytes());
            buf.extend_from_slice(destination);
            buf.extend_from_slice(format!("\r\n${}\r\n", member.len()).as_bytes());
            buf.extend_from_slice(member);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Zremrangebyrank { key, start, stop } => {
            let st = start.to_string();
            let sp = stop.to_string();
            buf.extend_from_slice(
                format!("*4\r\n$16\r\nZREMRANGEBYRANK\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!("\r\n${}\r\n{}\r\n${}\r\n{}\r\n", st.len(), st, sp.len(), sp).as_bytes(),
            );
            Some(buf)
        }
        Command::Zremrangebyscore {
            key,
            min_score,
            min_inc,
            max_score,
            max_inc,
        } => {
            let min_s = if *min_inc {
                min_score.to_string()
            } else {
                format!("({}", min_score)
            };
            let max_s = if *max_inc {
                max_score.to_string()
            } else {
                format!("({}", max_score)
            };
            buf.extend_from_slice(
                format!("*4\r\n$17\r\nZREMRANGEBYSCORE\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!(
                    "\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    min_s.len(),
                    min_s,
                    max_s.len(),
                    max_s
                )
                .as_bytes(),
            );
            Some(buf)
        }
        Command::Zremrangebylex { key, min, max } => {
            let min_s = match min {
                crate::table::LexBound::UnboundedMin => "-".to_string(),
                crate::table::LexBound::UnboundedMax => "+".to_string(),
                crate::table::LexBound::Inclusive(b) => format!("[{}", String::from_utf8_lossy(b)),
                crate::table::LexBound::Exclusive(b) => format!("({}", String::from_utf8_lossy(b)),
            };
            let max_s = match max {
                crate::table::LexBound::UnboundedMin => "-".to_string(),
                crate::table::LexBound::UnboundedMax => "+".to_string(),
                crate::table::LexBound::Inclusive(b) => format!("[{}", String::from_utf8_lossy(b)),
                crate::table::LexBound::Exclusive(b) => format!("({}", String::from_utf8_lossy(b)),
            };
            buf.extend_from_slice(
                format!("*4\r\n$15\r\nZREMRANGEBYLEX\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!(
                    "\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    min_s.len(),
                    min_s,
                    max_s.len(),
                    max_s
                )
                .as_bytes(),
            );
            Some(buf)
        }
        Command::Ltrim { key, start, stop } => {
            let st = start.to_string();
            let sp = stop.to_string();
            buf.extend_from_slice(format!("*4\r\n$5\r\nLTRIM\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!("\r\n${}\r\n{}\r\n${}\r\n{}\r\n", st.len(), st, sp.len(), sp).as_bytes(),
            );
            Some(buf)
        }
        Command::Lset {
            key,
            index,
            element,
        } => {
            let idx_s = index.to_string();
            buf.extend_from_slice(format!("*4\r\n$4\r\nLSET\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!(
                    "\r\n${}\r\n{}\r\n${}\r\n",
                    idx_s.len(),
                    idx_s,
                    element.len()
                )
                .as_bytes(),
            );
            buf.extend_from_slice(element);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Lrem {
            key,
            count,
            element,
        } => {
            let cnt_s = count.to_string();
            buf.extend_from_slice(format!("*4\r\n$4\r\nLREM\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!(
                    "\r\n${}\r\n{}\r\n${}\r\n",
                    cnt_s.len(),
                    cnt_s,
                    element.len()
                )
                .as_bytes(),
            );
            buf.extend_from_slice(element);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Linsert {
            key,
            before,
            pivot,
            element,
        } => {
            let dir = if *before { "BEFORE" } else { "AFTER" };
            buf.extend_from_slice(format!("*5\r\n$7\r\nLINSERT\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!("\r\n${}\r\n{}\r\n${}\r\n", dir.len(), dir, pivot.len()).as_bytes(),
            );
            buf.extend_from_slice(pivot);
            buf.extend_from_slice(format!("\r\n${}\r\n", element.len()).as_bytes());
            buf.extend_from_slice(element);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Lmove {
            source,
            destination,
            where_from,
            where_to,
        } => {
            let from_s = match where_from {
                crate::table::ListDirection::Left => "LEFT",
                crate::table::ListDirection::Right => "RIGHT",
            };
            let to_s = match where_to {
                crate::table::ListDirection::Left => "LEFT",
                crate::table::ListDirection::Right => "RIGHT",
            };
            buf.extend_from_slice(format!("*5\r\n$5\r\nLMOVE\r\n${}\r\n", source.len()).as_bytes());
            buf.extend_from_slice(source);
            buf.extend_from_slice(format!("\r\n${}\r\n", destination.len()).as_bytes());
            buf.extend_from_slice(destination);
            buf.extend_from_slice(
                format!(
                    "\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    from_s.len(),
                    from_s,
                    to_s.len(),
                    to_s
                )
                .as_bytes(),
            );
            Some(buf)
        }
        Command::Incrbyfloat { key, increment } => {
            let s = increment.to_string();
            buf.extend_from_slice(
                format!("*3\r\n$11\r\nINCRBYFLOAT\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", s.len(), s).as_bytes());
            Some(buf)
        }
        Command::Setrange { key, offset, value } => {
            let off_s = offset.to_string();
            buf.extend_from_slice(format!("*4\r\n$8\r\nSETRANGE\r\n${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(
                format!("\r\n${}\r\n{}\r\n${}\r\n", off_s.len(), off_s, value.len()).as_bytes(),
            );
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Sort {
            key,
            desc,
            alpha,
            store: Some(dest),
            limit,
        } => {
            let mut args: Vec<Vec<u8>> = vec![b"SORT".to_vec(), key.to_vec()];
            if let Some((offset, count)) = limit {
                args.push(b"LIMIT".to_vec());
                args.push(offset.to_string().into_bytes());
                args.push(count.to_string().into_bytes());
            }
            if *desc {
                args.push(b"DESC".to_vec());
            }
            if *alpha {
                args.push(b"ALPHA".to_vec());
            }
            args.push(b"STORE".to_vec());
            args.push(dest.to_vec());
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Hexpire {
            key,
            expire_ms,
            is_at,
            condition,
            fields,
        } => {
            let cmd_name = if *is_at { "HPEXPIREAT" } else { "HPEXPIRE" };
            let exp_s = expire_ms.to_string();
            let numfields_s = fields.len().to_string();
            let mut args: Vec<&[u8]> = vec![cmd_name.as_bytes(), key.as_ref(), exp_s.as_bytes()];
            match condition {
                crate::resp::HexpireCondition::None => {}
                crate::resp::HexpireCondition::Nx => args.push(b"NX"),
                crate::resp::HexpireCondition::Xx => args.push(b"XX"),
                crate::resp::HexpireCondition::Gt => args.push(b"GT"),
                crate::resp::HexpireCondition::Lt => args.push(b"LT"),
            }
            args.push(b"FIELDS");
            args.push(numfields_s.as_bytes());
            for f in fields {
                args.push(f.as_ref());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Hpersist { key, fields } => {
            let numfields_s = fields.len().to_string();
            let mut args: Vec<&[u8]> =
                vec![b"HPERSIST", key.as_ref(), b"FIELDS", numfields_s.as_bytes()];
            for f in fields {
                args.push(f.as_ref());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Hgetex {
            key,
            expire,
            fields,
        } => {
            if matches!(
                expire,
                crate::resp::HFieldExpireOpt::None | crate::resp::HFieldExpireOpt::KeepTtl
            ) {
                return None;
            }
            let numfields_s = fields.len().to_string();
            let val_s;
            let mut args: Vec<&[u8]> = vec![b"HGETEX", key.as_ref()];
            match expire {
                crate::resp::HFieldExpireOpt::Persist => {
                    args.push(b"PERSIST");
                }
                crate::resp::HFieldExpireOpt::ExMs(ms) => {
                    val_s = ms.to_string();
                    args.push(b"PX");
                    args.push(val_s.as_bytes());
                }
                crate::resp::HFieldExpireOpt::ExAtMs(ms) => {
                    val_s = ms.to_string();
                    args.push(b"PXAT");
                    args.push(val_s.as_bytes());
                }
                _ => {}
            }
            args.push(b"FIELDS");
            args.push(numfields_s.as_bytes());
            for f in fields {
                args.push(f.as_ref());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Hsetex {
            key,
            condition,
            expire,
            pairs,
        } => {
            let numfields_s = pairs.len().to_string();
            let val_s;
            let mut args: Vec<&[u8]> = vec![b"HSETEX", key.as_ref()];
            match condition {
                crate::resp::HsetexCondition::None => {}
                crate::resp::HsetexCondition::Fnx => args.push(b"FNX"),
                crate::resp::HsetexCondition::Fxx => args.push(b"FXX"),
            }
            match expire {
                crate::resp::HFieldExpireOpt::None | crate::resp::HFieldExpireOpt::Persist => {}
                crate::resp::HFieldExpireOpt::KeepTtl => args.push(b"KEEPTTL"),
                crate::resp::HFieldExpireOpt::ExMs(ms) => {
                    val_s = ms.to_string();
                    args.push(b"PX");
                    args.push(val_s.as_bytes());
                }
                crate::resp::HFieldExpireOpt::ExAtMs(ms) => {
                    val_s = ms.to_string();
                    args.push(b"PXAT");
                    args.push(val_s.as_bytes());
                }
            }
            args.push(b"FIELDS");
            args.push(numfields_s.as_bytes());
            for (f, v) in pairs {
                args.push(f.as_ref());
                args.push(v.as_ref());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Xclaim {
            key,
            group,
            consumer,
            min_idle_time,
            ids,
            idle,
            time,
            retrycount,
            force,
            justid,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"XCLAIM".to_vec(),
                key.to_vec(),
                group.to_vec(),
                consumer.to_vec(),
                min_idle_time.to_string().into_bytes(),
            ];
            for id in ids {
                args.push(id.to_vec());
            }
            if let Some(i) = idle {
                args.push(b"IDLE".to_vec());
                args.push(i.to_string().into_bytes());
            }
            if let Some(t) = time {
                args.push(b"TIME".to_vec());
                args.push(t.to_string().into_bytes());
            }
            if let Some(r) = retrycount {
                args.push(b"RETRYCOUNT".to_vec());
                args.push(r.to_string().into_bytes());
            }
            if *force {
                args.push(b"FORCE".to_vec());
            }
            if *justid {
                args.push(b"JUSTID".to_vec());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Xautoclaim {
            key,
            group,
            consumer,
            min_idle_time,
            start,
            count,
            justid,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"XAUTOCLAIM".to_vec(),
                key.to_vec(),
                group.to_vec(),
                consumer.to_vec(),
                min_idle_time.to_string().into_bytes(),
                start.to_vec(),
            ];
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
            if *justid {
                args.push(b"JUSTID".to_vec());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Zrangestore { dst, src, opts } => {
            let (start_s, stop_s) = if opts.by_score {
                let min_s = if opts.min_inc {
                    opts.min_score.to_string()
                } else {
                    format!("({}", opts.min_score)
                };
                let max_s = if opts.max_inc {
                    opts.max_score.to_string()
                } else {
                    format!("({}", opts.max_score)
                };
                (min_s.into_bytes(), max_s.into_bytes())
            } else {
                (
                    opts.start.to_string().into_bytes(),
                    opts.stop.to_string().into_bytes(),
                )
            };
            let mut args: Vec<Vec<u8>> = vec![
                b"ZRANGESTORE".to_vec(),
                dst.to_vec(),
                src.to_vec(),
                start_s,
                stop_s,
            ];
            if opts.by_score {
                args.push(b"BYSCORE".to_vec());
            } else if opts.by_lex {
                args.push(b"BYLEX".to_vec());
            }
            if opts.rev {
                args.push(b"REV".to_vec());
            }
            if let Some(count) = opts.count {
                args.push(b"LIMIT".to_vec());
                args.push(opts.offset.to_string().into_bytes());
                args.push(count.to_string().into_bytes());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::SemanticSet {
            namespace,
            id,
            prompt,
            response,
            vector,
            ttl,
            scope,
            quantize,
            tokens,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"SEMANTIC.SET".to_vec(),
                namespace.to_vec(),
                id.to_vec(),
                prompt.to_vec(),
                response.to_vec(),
                b"VECTOR".to_vec(),
                vector.len().to_string().into_bytes(),
            ];
            for v in vector {
                args.push(v.to_string().into_bytes());
            }
            if let Some(d) = ttl {
                args.push(b"PX".to_vec());
                args.push(d.as_millis().max(1).to_string().into_bytes());
            }
            if let Some(s) = scope {
                args.push(b"SCOPE".to_vec());
                args.push(s.to_vec());
            }
            if *quantize {
                args.push(b"QUANTIZE".to_vec());
            }
            if let Some(t) = tokens {
                args.push(b"TOKENS".to_vec());
                args.push(t.to_string().into_bytes());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::SemanticDel { namespace, ids } => {
            buf.extend_from_slice(
                format!("*{}\r\n$12\r\nSEMANTIC.DEL\r\n", 2 + ids.len()).as_bytes(),
            );
            buf.extend_from_slice(format!("${}\r\n", namespace.len()).as_bytes());
            buf.extend_from_slice(namespace);
            buf.extend_from_slice(b"\r\n");
            for id in ids {
                buf.extend_from_slice(format!("${}\r\n", id.len()).as_bytes());
                buf.extend_from_slice(id);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::SemanticFlush(namespace) => {
            buf.extend_from_slice(
                format!("*2\r\n$14\r\nSEMANTIC.FLUSH\r\n${}\r\n", namespace.len()).as_bytes(),
            );
            buf.extend_from_slice(namespace);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::AgentMemAdd {
            session,
            role,
            content,
            tokens,
            vector,
            meta,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.MEM.ADD".to_vec(),
                session.to_vec(),
                role.to_vec(),
                content.to_vec(),
            ];
            if let Some(t) = tokens {
                args.push(b"TOKENS".to_vec());
                args.push(t.to_string().into_bytes());
            }
            if let Some(v) = vector {
                args.push(b"VEC".to_vec());
                args.push(v.len().to_string().into_bytes());
                for &f in v {
                    args.push(f.to_string().into_bytes());
                }
            }
            if let Some(m) = meta {
                args.push(b"META".to_vec());
                args.push(m.to_vec());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::AgentMemCompact {
            session,
            keep_recent,
            summary,
            tokens,
            vector,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.MEM.COMPACT".to_vec(),
                session.to_vec(),
                b"KEEP_RECENT".to_vec(),
                keep_recent.to_string().into_bytes(),
                b"SUMMARY".to_vec(),
                summary.to_vec(),
            ];
            if let Some(t) = tokens {
                args.push(b"TOKENS".to_vec());
                args.push(t.to_string().into_bytes());
            }
            if let Some(v) = vector {
                args.push(b"VEC".to_vec());
                args.push(v.len().to_string().into_bytes());
                for &f in v {
                    args.push(f.to_string().into_bytes());
                }
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::AgentMemClear(session) => {
            buf.extend_from_slice(
                format!("*2\r\n$15\r\nAGENT.MEM.CLEAR\r\n${}\r\n", session.len()).as_bytes(),
            );
            buf.extend_from_slice(session);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::AgentCheckpointPut {
            key,
            step_id,
            parent_id,
            state,
            meta,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.CHECKPOINT.PUT".to_vec(),
                key.to_vec(),
                step_id.to_vec(),
            ];
            if let Some(p) = parent_id {
                args.push(b"PARENT".to_vec());
                args.push(p.to_vec());
            }
            args.push(b"STATE".to_vec());
            args.push(state.to_vec());
            if let Some(m) = meta {
                args.push(b"META".to_vec());
                args.push(m.to_vec());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::AgentToolClaim {
            key,
            call_id,
            ttl_ms,
            input,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.TOOL.CLAIM".to_vec(),
                key.to_vec(),
                call_id.to_vec(),
                b"TTL".to_vec(),
                ttl_ms.to_string().into_bytes(),
            ];
            if let Some(inp) = input {
                args.push(b"INPUT".to_vec());
                args.push(inp.to_vec());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::AgentToolComplete {
            key,
            call_id,
            output,
            ttl_ms,
        } => {
            let mut args: Vec<Vec<u8>> = vec![
                b"AGENT.TOOL.COMPLETE".to_vec(),
                key.to_vec(),
                call_id.to_vec(),
                b"OUTPUT".to_vec(),
                output.to_vec(),
            ];
            if let Some(ttl) = ttl_ms {
                args.push(b"TTL".to_vec());
                args.push(ttl.to_string().into_bytes());
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Vadd {
            key,
            element,
            vector,
            metric,
            quantize,
            pq,
            tiered,
            reduce,
            quant,
            ef,
            setattr,
            m,
            cas,
            is_redis_vset,
        } => {
            let mut args: Vec<Vec<u8>> = Vec::with_capacity(14);
            args.push(b"VADD".to_vec());
            args.push(key.to_vec());
            if *is_redis_vset {
                if let Some(r) = reduce {
                    args.push(b"REDUCE".to_vec());
                    args.push(r.to_string().into_bytes());
                }
                args.push(b"FP32".to_vec());
                let mut blob = Vec::with_capacity(vector.len() * 4);
                for &v in vector {
                    blob.extend_from_slice(&v.to_le_bytes());
                }
                args.push(blob);
                args.push(element.to_vec());
                if *cas {
                    args.push(b"CAS".to_vec());
                }
                if let Some(q) = quant {
                    args.push(match q {
                        crate::vector::VQuant::NoQuant => b"NOQUANT".to_vec(),
                        crate::vector::VQuant::Q8 => b"Q8".to_vec(),
                        crate::vector::VQuant::Bin => b"BIN".to_vec(),
                    });
                }
                if let Some(e) = ef {
                    args.push(b"EF".to_vec());
                    args.push(e.to_string().into_bytes());
                }
                if let Some(attr) = setattr {
                    args.push(b"SETATTR".to_vec());
                    args.push(attr.as_bytes().to_vec());
                }
                if let Some(m_val) = m {
                    args.push(b"M".to_vec());
                    args.push(m_val.to_string().into_bytes());
                }
            } else {
                args.push(element.to_vec());
                for &v in vector {
                    args.push(v.to_string().into_bytes());
                }
                if let Some(m_type) = metric {
                    args.push(m_type.as_str().as_bytes().to_vec());
                }
                if *quantize {
                    args.push(b"QUANTIZE".to_vec());
                }
                if *pq {
                    args.push(b"PQ".to_vec());
                }
                if *tiered {
                    args.push(b"TIERED".to_vec());
                }
            }
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(&a);
                buf.extend_from_slice(b"\r\n");
            }
            Some(buf)
        }
        Command::Vdel { key, element } => {
            buf.extend_from_slice(b"*3\r\n$4\r\nVREM\r\n");
            buf.extend_from_slice(format!("${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", element.len()).as_bytes());
            buf.extend_from_slice(element);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Vsetattr { key, element, attr } => {
            buf.extend_from_slice(b"*4\r\n$8\r\nVSETATTR\r\n");
            buf.extend_from_slice(format!("${}\r\n", key.len()).as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", element.len()).as_bytes());
            buf.extend_from_slice(element);
            buf.extend_from_slice(format!("\r\n${}\r\n", attr.len()).as_bytes());
            buf.extend_from_slice(attr.as_bytes());
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

pub fn rewrite_shard_aof(db: &mut ShardDb, dir: &Path, shard_id: usize) -> std::io::Result<usize> {
    use std::io::Write;
    let mut count = 0;
    let now = std::time::Instant::now();
    let unix_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    if !db.table.hash_field_expires.is_empty() {
        let keys_with_field_exp: Vec<bytes::Bytes> =
            db.table.hash_field_expires.keys().cloned().collect();
        for k in keys_with_field_exp {
            db.table.purge_expired_hash_fields(&k);
        }
    }

    static TMP_REWRITE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let tmp_id = TMP_REWRITE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp_path = dir.join(format!(
        "appendonly-{}.aof.tmp.{}_{}",
        shard_id,
        std::process::id(),
        tmp_id
    ));
    let target_path = dir.join(format!("appendonly-{}.aof", shard_id));

    let file = std::fs::File::create(&tmp_path)?;
    let mut writer = std::io::BufWriter::with_capacity(65536, file);

    // 1. Snapshot all non-expired entries in RudisTable as canonical RESP commands
    for entry in db.table.entries() {
        if let Some(exp) = entry.expire_at
            && exp <= now
        {
            continue;
        }
        let k = entry.key.as_ref();
        let hydrated_val;
        let val_ref = match &entry.val {
            crate::table::RudisValue::Cooled { val, .. } => val.as_ref(),
            crate::table::RudisValue::Tiered(ptr) => {
                if let Some(ref tm) = db.tier_manager
                    && let Ok((_, raw)) = tm.read_ptr_sync(*ptr)
                {
                    hydrated_val = crate::table::RudisValue::String(bytes::Bytes::from(raw));
                    &hydrated_val
                } else {
                    continue;
                }
            }
            other => other,
        };
        match val_ref {
            crate::table::RudisValue::String(val) => {
                if let Some(exp) = entry.expire_at {
                    let rem_ms = exp.duration_since(now).as_millis() as u64;
                    let rem_ms_str = rem_ms.to_string();
                    writer.write_all(b"*5\r\n$3\r\nSET\r\n$")?;
                    writer.write_all(k.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n$")?;
                    writer.write_all(val.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(val.as_ref())?;
                    writer.write_all(b"\r\n$2\r\nPX\r\n$")?;
                    writer.write_all(rem_ms_str.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(rem_ms_str.as_bytes())?;
                    writer.write_all(b"\r\n")?;
                } else {
                    writer.write_all(b"*3\r\n$3\r\nSET\r\n$")?;
                    writer.write_all(k.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n$")?;
                    writer.write_all(val.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(val.as_ref())?;
                    writer.write_all(b"\r\n")?;
                }
                count += 1;
            }
            crate::table::RudisValue::Int(val) => {
                let val_str = val.to_string();
                writer.write_all(b"*3\r\n$3\r\nSET\r\n$")?;
                writer.write_all(k.len().to_string().as_bytes())?;
                writer.write_all(b"\r\n")?;
                writer.write_all(k)?;
                writer.write_all(b"\r\n$")?;
                writer.write_all(val_str.len().to_string().as_bytes())?;
                writer.write_all(b"\r\n")?;
                writer.write_all(val_str.as_bytes())?;
                writer.write_all(b"\r\n")?;
                count += 1;
            }
            crate::table::RudisValue::SmallHash(pairs) if !pairs.is_empty() => {
                writer.write_all(
                    format!("*{}\r\n$4\r\nHSET\r\n${}\r\n", pairs.len() * 2 + 2, k.len())
                        .as_bytes(),
                )?;
                writer.write_all(k)?;
                writer.write_all(b"\r\n")?;
                for (field, v) in pairs {
                    writer.write_all(format!("${}\r\n", field.len()).as_bytes())?;
                    writer.write_all(field.as_ref())?;
                    writer.write_all(format!("\r\n${}\r\n", v.len()).as_bytes())?;
                    writer.write_all(v.as_ref())?;
                    writer.write_all(b"\r\n")?;
                }
                count += 1;
            }
            crate::table::RudisValue::Hash(map) if !map.is_empty() => {
                writer.write_all(
                    format!("*{}\r\n$4\r\nHSET\r\n${}\r\n", map.len() * 2 + 2, k.len()).as_bytes(),
                )?;
                writer.write_all(k)?;
                writer.write_all(b"\r\n")?;
                for (field, v) in map.as_ref() {
                    writer.write_all(format!("${}\r\n", field.len()).as_bytes())?;
                    writer.write_all(field.as_ref())?;
                    writer.write_all(format!("\r\n${}\r\n", v.len()).as_bytes())?;
                    writer.write_all(v.as_ref())?;
                    writer.write_all(b"\r\n")?;
                }
                count += 1;
            }
            crate::table::RudisValue::List(list) if !list.is_empty() => {
                writer.write_all(
                    format!("*{}\r\n$5\r\nRPUSH\r\n${}\r\n", list.len() + 2, k.len()).as_bytes(),
                )?;
                writer.write_all(k)?;
                writer.write_all(b"\r\n")?;
                for item in list {
                    writer.write_all(format!("${}\r\n", item.len()).as_bytes())?;
                    writer.write_all(item.as_ref())?;
                    writer.write_all(b"\r\n")?;
                }
                count += 1;
            }
            crate::table::RudisValue::Set(set) if !set.is_empty() => {
                let members: Vec<bytes::Bytes> = set.to_vec();
                writer.write_all(
                    format!("*{}\r\n$4\r\nSADD\r\n${}\r\n", members.len() + 2, k.len()).as_bytes(),
                )?;
                writer.write_all(k)?;
                writer.write_all(b"\r\n")?;
                for m in members {
                    writer.write_all(format!("${}\r\n", m.len()).as_bytes())?;
                    writer.write_all(m.as_ref())?;
                    writer.write_all(b"\r\n")?;
                }
                count += 1;
            }
            crate::table::RudisValue::ZSet(zset) if !zset.is_empty() => {
                let elements = zset.to_vec();
                writer.write_all(
                    format!(
                        "*{}\r\n$4\r\nZADD\r\n${}\r\n",
                        elements.len() * 2 + 2,
                        k.len()
                    )
                    .as_bytes(),
                )?;
                writer.write_all(k)?;
                writer.write_all(b"\r\n")?;
                for (member, score) in elements {
                    let score_str = score.to_string();
                    writer.write_all(
                        format!(
                            "${}\r\n{}\r\n${}\r\n",
                            score_str.len(),
                            score_str,
                            member.len()
                        )
                        .as_bytes(),
                    )?;
                    writer.write_all(member.as_ref())?;
                    writer.write_all(b"\r\n")?;
                }
                count += 1;
            }
            crate::table::RudisValue::HyperLogLog(hll) => {
                writer.write_all(b"*3\r\n$3\r\nSET\r\n$")?;
                writer.write_all(k.len().to_string().as_bytes())?;
                writer.write_all(b"\r\n")?;
                writer.write_all(k)?;
                writer.write_all(b"\r\n$")?;
                writer.write_all(hll.len().to_string().as_bytes())?;
                writer.write_all(b"\r\n")?;
                writer.write_all(hll.as_ref())?;
                writer.write_all(b"\r\n")?;
                count += 1;
            }
            crate::table::RudisValue::Stream(stream) if !stream.entries.is_empty() => {
                for (sid, fields) in &stream.entries {
                    let id_str = sid.to_string();
                    let num_args = 2 + 1 + fields.len() * 2;
                    writer.write_all(
                        format!("*{}\r\n$4\r\nXADD\r\n${}\r\n", num_args, k.len()).as_bytes(),
                    )?;
                    writer.write_all(k)?;
                    writer
                        .write_all(format!("\r\n${}\r\n{}\r\n", id_str.len(), id_str).as_bytes())?;
                    for (f, v) in fields {
                        writer.write_all(format!("${}\r\n", f.len()).as_bytes())?;
                        writer.write_all(f.as_ref())?;
                        writer.write_all(format!("\r\n${}\r\n", v.len()).as_bytes())?;
                        writer.write_all(v.as_ref())?;
                        writer.write_all(b"\r\n")?;
                    }
                    count += 1;
                }
            }
            _ => {}
        }

        if !matches!(val_ref, crate::table::RudisValue::String(_))
            && let Some(exp) = entry.expire_at
        {
            let rem_ms = exp.duration_since(now).as_millis() as u64;
            let rem_ms_str = rem_ms.to_string();
            writer.write_all(b"*3\r\n$7\r\nPEXPIRE\r\n$")?;
            writer.write_all(k.len().to_string().as_bytes())?;
            writer.write_all(b"\r\n")?;
            writer.write_all(k)?;
            writer.write_all(b"\r\n$")?;
            writer.write_all(rem_ms_str.len().to_string().as_bytes())?;
            writer.write_all(b"\r\n")?;
            writer.write_all(rem_ms_str.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }

        if !db.table.hash_field_expires.is_empty()
            && let Some(fmap) = db.table.hash_field_expires.get(k)
        {
            for (field, &exp) in fmap {
                if exp > now {
                    let rem_ms = exp.duration_since(now).as_millis() as u64;
                    let exp_unix_ms_str = (unix_now + rem_ms).to_string();
                    writer.write_all(b"*6\r\n$10\r\nHPEXPIREAT\r\n$")?;
                    writer.write_all(k.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n$")?;
                    writer.write_all(exp_unix_ms_str.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(exp_unix_ms_str.as_bytes())?;
                    writer.write_all(b"\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$")?;
                    writer.write_all(field.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(field.as_ref())?;
                    writer.write_all(b"\r\n")?;
                }
            }
        }
    }

    // 2. Snapshot JSON documents
    for (k, doc) in db.json_store.iter() {
        let doc_str = serde_json::to_string(doc).unwrap_or_default();
        writer.write_all(format!("*4\r\n$8\r\nJSON.SET\r\n${}\r\n", k.len()).as_bytes())?;
        writer.write_all(k.as_ref())?;
        writer.write_all(
            format!("\r\n$1\r\n$\r\n${}\r\n{}\r\n", doc_str.len(), doc_str).as_bytes(),
        )?;
        count += 1;
    }

    // 3. Snapshot Vector sets / HNSW indexes
    for (name, index) in &db.vector_indexes {
        for (elem, &node_id) in &index.key_to_id {
            if let Some(Some(node)) = index.nodes.get(node_id) {
                let cmd = Command::Vadd {
                    key: bytes::Bytes::from(name.clone()),
                    element: elem.clone(),
                    vector: index.node_vector_cow(node).into_owned(),
                    metric: Some(index.metric),
                    quantize: node.quantized.is_some(),
                    pq: node.pq.is_some(),
                    tiered: node.is_tiered,
                    reduce: None,
                    quant: Some(index.quant),
                    ef: None,
                    setattr: index.attributes.get(elem).cloned(),
                    m: Some(index.m),
                    cas: false,
                    is_redis_vset: index.is_redis_vset,
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 4. Snapshot Semantic Caches (SEMANTIC.*)
    for (ns, cache) in &db.semantic_caches {
        for (id, entry) in &cache.entries {
            if let Some(exp) = entry.expire_at
                && exp <= now
            {
                continue;
            }
            let ttl = entry
                .expire_at
                .map(|exp| exp.saturating_duration_since(now));
            if let Some(&node_id) = cache.index.key_to_id.get(id)
                && let Some(Some(node)) = cache.index.nodes.get(node_id)
            {
                let cmd = Command::SemanticSet {
                    namespace: ns.clone(),
                    id: id.clone(),
                    prompt: entry.prompt.clone(),
                    response: entry.response.clone(),
                    vector: cache.index.node_vector_cow(node).into_owned(),
                    ttl,
                    scope: entry.scope.clone(),
                    quantize: node.quantized.is_some(),
                    tokens: Some(entry.tokens),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 5. Snapshot Agent Memory Sessions (AGENT.MEM.*)
    for (sess_key, session) in &db.agent_memories {
        let get_turn_vec = |turn_id: u64| -> Option<Vec<f32>> {
            session
                .index
                .as_ref()
                .and_then(|idx| idx.get_vector_cow(&bytes::Bytes::from(turn_id.to_string())))
                .map(|c| c.into_owned())
        };
        let has_compacted = session.turns.iter().any(|t| t.compacted);
        if has_compacted {
            for turn in session.turns.iter().filter(|t| t.compacted) {
                let cmd = Command::AgentMemAdd {
                    session: sess_key.clone(),
                    role: turn.role.clone(),
                    content: turn.content.clone(),
                    tokens: Some(turn.tokens),
                    vector: get_turn_vec(turn.id),
                    meta: turn.meta.clone(),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
            let mut active_iter = session.turns.iter().filter(|t| !t.compacted);
            if let Some(summary_turn) = active_iter.next() {
                let compact_cmd = Command::AgentMemCompact {
                    session: sess_key.clone(),
                    keep_recent: 0,
                    summary: summary_turn.content.clone(),
                    tokens: Some(summary_turn.tokens),
                    vector: get_turn_vec(summary_turn.id),
                };
                if let Some(resp) = command_to_resp(&compact_cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
                for turn in active_iter {
                    let cmd = Command::AgentMemAdd {
                        session: sess_key.clone(),
                        role: turn.role.clone(),
                        content: turn.content.clone(),
                        tokens: Some(turn.tokens),
                        vector: get_turn_vec(turn.id),
                        meta: turn.meta.clone(),
                    };
                    if let Some(resp) = command_to_resp(&cmd) {
                        writer.write_all(&resp)?;
                        count += 1;
                    }
                }
            }
        } else {
            for turn in &session.turns {
                let cmd = Command::AgentMemAdd {
                    session: sess_key.clone(),
                    role: turn.role.clone(),
                    content: turn.content.clone(),
                    tokens: Some(turn.tokens),
                    vector: get_turn_vec(turn.id),
                    meta: turn.meta.clone(),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 6. Snapshot Agent Checkpoints (AGENT.CHECKPOINT.*)
    for (ck_key, thread) in &db.agent_checkpoints {
        for step_id in &thread.order {
            if let Some(node) = thread.nodes.get(step_id) {
                let cmd = Command::AgentCheckpointPut {
                    key: ck_key.clone(),
                    step_id: node.step_id.clone(),
                    parent_id: node.parent_id.clone(),
                    state: node.state.clone(),
                    meta: node.metadata.clone(),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    // 7. Snapshot Agent Tool Registries (AGENT.TOOL.*)
    for (tool_key, reg) in &db.agent_tools {
        for (call_id, entry) in &reg.calls {
            if let Some(exp) = entry.expire_at
                && exp <= now
            {
                continue;
            }
            if let Some(ref output) = entry.output {
                let ttl_ms = entry
                    .expire_at
                    .map(|exp| exp.saturating_duration_since(now).as_millis().max(1) as u64);
                let cmd = Command::AgentToolComplete {
                    key: tool_key.clone(),
                    call_id: call_id.clone(),
                    output: output.clone(),
                    ttl_ms,
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            } else if let Some(lease_until) = entry.lease_until
                && lease_until > now
            {
                let rem_ms = lease_until.duration_since(now).as_millis().max(1) as u64;
                let cmd = Command::AgentToolClaim {
                    key: tool_key.clone(),
                    call_id: call_id.clone(),
                    ttl_ms: rem_ms,
                    input: entry.input.clone(),
                };
                if let Some(resp) = command_to_resp(&cmd) {
                    writer.write_all(&resp)?;
                    count += 1;
                }
            }
        }
    }

    writer.flush()?;
    let file = writer.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()?;
    std::fs::rename(&tmp_path, &target_path)?;
    let _ = sync_parent_dir(&target_path);

    Ok(count)
}

/// Flushes directory metadata so that recently renamed files are durable across sudden power loss
pub fn sync_parent_dir(path: &std::path::Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        let dir_path = if parent.as_os_str().is_empty() {
            std::path::Path::new(".")
        } else {
            parent
        };
        if let Ok(dir_file) = std::fs::File::open(dir_path) {
            dir_file.sync_all()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    #[test]
    fn test_aof_compaction_and_rewrite() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-aof-rewrite-unit-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let mut db = ShardDb::new(6379);

        // 1. Populate various data types in ShardDb
        db.set(Bytes::from("str1"), Bytes::from("val1"), None);
        db.set(
            Bytes::from("str2"),
            Bytes::from("val2"),
            Some(Duration::from_secs(3600)),
        );
        db.hset(
            Bytes::from("hash1"),
            vec![
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        )
        .unwrap();
        db.rpush(
            Bytes::from("list1"),
            vec![Bytes::from("elem1"), Bytes::from("elem2")],
        )
        .unwrap();
        db.sadd(
            Bytes::from("set1"),
            vec![Bytes::from("m1"), Bytes::from("m2")],
        )
        .unwrap();
        db.zadd(
            Bytes::from("zset1"),
            vec![(10.5, Bytes::from("z1")), (20.0, Bytes::from("z2"))],
            crate::table::ZAddFlags::default(),
        )
        .unwrap();
        db.json_store
            .json_set(b"doc1", "$", r#"{"title":"rewrite"}"#, false, false)
            .unwrap();

        // 2. Perform AOF rewrite
        let rewritten_count =
            rewrite_shard_aof(&mut db, &temp_dir, 0).expect("rewrite should succeed");
        assert_eq!(rewritten_count, 7);

        let aof_file = temp_dir.join("appendonly-0.aof");
        assert!(aof_file.exists());

        // 3. Replay rewritten AOF into fresh ShardDb and verify all state is restored
        let mut new_db = ShardDb::new(6379);
        let replayed = replay_aof(&aof_file, &mut new_db).expect("replay should succeed");
        assert_eq!(replayed, 7);

        assert_eq!(new_db.get(b"str1"), Some(Bytes::from("val1")));
        assert_eq!(new_db.get(b"str2"), Some(Bytes::from("val2")));
        assert_eq!(
            new_db.hget(b"hash1", b"f1").unwrap(),
            Some(Bytes::from("v1"))
        );
        assert_eq!(
            new_db.lrange(b"list1", 0, -1).unwrap(),
            vec![Bytes::from("elem1"), Bytes::from("elem2")]
        );
        assert!(new_db.sismember(b"set1", b"m1").unwrap());
        assert_eq!(new_db.zscore(b"zset1", b"z1").unwrap(), Some(10.5));
        assert!(
            new_db
                .json_store
                .json_get(b"doc1", &["$"])
                .unwrap()
                .contains("rewrite")
        );

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_aof_writer_reopen_and_continuous_logging() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-aof-reopen-unit-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let aof_file = temp_dir.join("appendonly-0.aof");

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .build()
            .unwrap();

        rt.block_on(async {
            let writer = std::rc::Rc::new(std::cell::RefCell::new(
                AofWriter::open(aof_file.clone()).await.unwrap(),
            ));
            writer
                .borrow_mut()
                .append(b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n");
            AofWriter::flush_rc(&writer).await.unwrap();

            // Populate db with k1 and rewrite
            let mut db = ShardDb::new(6379);
            db.set(Bytes::from("k1"), Bytes::from("v1"), None);
            let count = rewrite_shard_aof(&mut db, &temp_dir, 0).unwrap();
            assert_eq!(count, 1);

            // Reopen writer after rewrite and log k2
            let new_size = AofWriter::reopen_after_rewrite(&writer).await.unwrap();
            assert!(new_size > 0);
            writer
                .borrow_mut()
                .append(b"*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
            AofWriter::flush_rc(&writer).await.unwrap();

            // Replay and verify both k1 and k2
            let mut new_db = ShardDb::new(6379);
            let replayed = replay_aof(&aof_file, &mut new_db).unwrap();
            assert_eq!(replayed, 2);
            assert_eq!(new_db.get(b"k1"), Some(Bytes::from("v1")));
            assert_eq!(new_db.get(b"k2"), Some(Bytes::from("v2")));
        });

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_sync_parent_dir_durability() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-sync-dir-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);
        let test_file = temp_dir.join("test.txt");
        std::fs::write(&test_file, b"data").unwrap();

        assert!(sync_parent_dir(&test_file).is_ok());

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_rdb_and_aof_hash_field_expiration_persistence() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-hexpire-persist-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let mut db = ShardDb::new(6379);
        db.hset(
            Bytes::from("session_h"),
            vec![
                (Bytes::from("token"), Bytes::from("abc123")),
                (Bytes::from("perm"), Bytes::from("admin")),
            ],
        )
        .unwrap();
        let res = db
            .table
            .hexpire(
                b"session_h",
                60_000,
                false,
                crate::resp::HexpireCondition::None,
                &[Bytes::from("token")],
            )
            .unwrap();
        assert_eq!(res, vec![1]);

        // 1. Verify RDB chunk round-trip preserves field TTL
        let mut rdb_chunk = Vec::new();
        db.save_rdb_chunk(&mut rdb_chunk);

        let mut restored_rdb_db = ShardDb::new(6379);
        restored_rdb_db.restore_rdb_chunk(&rdb_chunk).unwrap();
        let ttls = restored_rdb_db
            .table
            .httl(
                b"session_h",
                false,
                false,
                &[Bytes::from("token"), Bytes::from("perm")],
            )
            .unwrap();
        assert!(ttls[0] > 0 && ttls[0] <= 60);
        assert_eq!(ttls[1], -1);

        // 2. Verify AOF rewrite & replay preserves field TTL
        rewrite_shard_aof(&mut db, &temp_dir, 0).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");
        let mut restored_aof_db = ShardDb::new(6379);
        replay_aof(&aof_file, &mut restored_aof_db).unwrap();
        let aof_ttls = restored_aof_db
            .table
            .httl(
                b"session_h",
                false,
                false,
                &[Bytes::from("token"), Bytes::from("perm")],
            )
            .unwrap();
        assert!(aof_ttls[0] > 0 && aof_ttls[0] <= 60);
        assert_eq!(aof_ttls[1], -1);

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_rdb_and_aof_vector_set_persistence() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-vset-persist-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let mut db = ShardDb::new(6379);
        db.vadd_ext(
            "movies",
            Bytes::from("m1"),
            vec![1.0, 0.0, 0.0],
            None,
            false,
            false,
            false,
            None,
            Some(crate::vector::VQuant::Q8),
            Some(64),
            Some(r#"{"genre":"sci-fi","year":1999}"#.to_string()),
            Some(24),
            true,
        )
        .unwrap();
        db.vadd_ext(
            "movies",
            Bytes::from("m2"),
            vec![0.0, 1.0, 0.0],
            None,
            false,
            false,
            false,
            None,
            Some(crate::vector::VQuant::Q8),
            None,
            Some(r#"{"genre":"drama","year":2020}"#.to_string()),
            Some(24),
            true,
        )
        .unwrap();

        // 1. Extended RDB round-trip
        let mut rdb_chunk = Vec::new();
        db.save_extended_rdb_chunk(&mut rdb_chunk);
        let mut restored_rdb = ShardDb::new(6379);
        restored_rdb.restore_rdb_chunk(&rdb_chunk).unwrap();
        let idx = restored_rdb.vector_indexes.get("movies").unwrap();
        assert!(idx.is_redis_vset);
        assert_eq!(idx.quant, crate::vector::VQuant::Q8);
        assert_eq!(idx.m, 24);
        assert_eq!(idx.len(), 2);
        assert_eq!(
            restored_rdb.vgetattr("movies", &Bytes::from("m1")).unwrap(),
            Some(r#"{"genre":"sci-fi","year":1999}"#.to_string())
        );

        // 2. AOF rewrite & replay round-trip
        rewrite_shard_aof(&mut db, &temp_dir, 0).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");
        let mut restored_aof = ShardDb::new(6379);
        replay_aof(&aof_file, &mut restored_aof).unwrap();
        let aof_idx = restored_aof.vector_indexes.get("movies").unwrap();
        assert!(aof_idx.is_redis_vset);
        assert_eq!(aof_idx.quant, crate::vector::VQuant::Q8);
        assert_eq!(aof_idx.m, 24);
        assert_eq!(aof_idx.len(), 2);
        assert_eq!(
            restored_aof.vgetattr("movies", &Bytes::from("m2")).unwrap(),
            Some(r#"{"genre":"drama","year":2020}"#.to_string())
        );

        // 3. Incremental VSETATTR and VREM command_to_resp replay
        let setattr_cmd = Command::Vsetattr {
            key: Bytes::from("movies"),
            element: Bytes::from("m1"),
            attr: r#"{"genre":"action","year":2001}"#.to_string(),
        };
        let vrem_cmd = Command::Vdel {
            key: Bytes::from("movies"),
            element: Bytes::from("m2"),
        };
        let mut stream = command_to_resp(&setattr_cmd).unwrap();
        stream.extend_from_slice(&command_to_resp(&vrem_cmd).unwrap());
        std::fs::write(&aof_file, &stream).unwrap();
        replay_aof(&aof_file, &mut restored_aof).unwrap();
        assert_eq!(restored_aof.vector_indexes.get("movies").unwrap().len(), 1);
        assert_eq!(
            restored_aof.vgetattr("movies", &Bytes::from("m1")).unwrap(),
            Some(r#"{"genre":"action","year":2001}"#.to_string())
        );

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_rdb_and_aof_ai_native_state_persistence() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-ai-persist-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&temp_dir);

        let mut db = ShardDb::new(6379);

        // 1. Populate Semantic Cache
        db.semantic_set(
            Bytes::from("llm:prod"),
            Bytes::from("q1"),
            Bytes::from("What is Rudis?"),
            Bytes::from("Rudis is a thread-per-core AI-native data platform."),
            vec![1.0, 0.0, 0.0],
            Some(Duration::from_secs(3600)),
            Some(Bytes::from("tenant:acme")),
            false,
            Some(42),
        )
        .unwrap();

        // 2. Populate Agent Memory (with compaction + episodic vectors)
        db.agent_mem_add(
            Bytes::from("sess:agent1"),
            Bytes::from("user"),
            Bytes::from(" Secret code is 7788."),
            Some(8),
            Some(vec![1.0, 0.0, 0.0]),
            Some(Bytes::from(r#"{"turn":1}"#)),
        )
        .unwrap();
        db.agent_mem_add(
            Bytes::from("sess:agent1"),
            Bytes::from("assistant"),
            Bytes::from("Understood, I stored 7788."),
            Some(8),
            Some(vec![0.9, 0.1, 0.0]),
            None,
        )
        .unwrap();
        let compacted = db
            .agent_mem_compact(
                &Bytes::from("sess:agent1"),
                0,
                Bytes::from("User shared secret code."),
                Some(6),
                Some(vec![0.5, 0.5, 0.0]),
            )
            .unwrap();
        assert_eq!(compacted, 2);
        db.agent_mem_add(
            Bytes::from("sess:agent1"),
            Bytes::from("user"),
            Bytes::from("Now help me deploy."),
            Some(5),
            Some(vec![0.0, 1.0, 0.0]),
            None,
        )
        .unwrap();

        // 3. Populate Agent Checkpoints (DAG lineage)
        db.agent_checkpoint_put(
            Bytes::from("thread:wf1"),
            Bytes::from("step1"),
            None,
            Bytes::from(r#"{"phase":"plan"}"#),
            Some(Bytes::from(r#"{"agent":"planner"}"#)),
        );
        db.agent_checkpoint_put(
            Bytes::from("thread:wf1"),
            Bytes::from("step2"),
            Some(Bytes::from("step1")),
            Bytes::from(r#"{"phase":"act"}"#),
            Some(Bytes::from(r#"{"agent":"executor"}"#)),
        );

        // 4. Populate Agent Tool Registry (one completed, one in-flight lease)
        let claim_done = db.agent_tool_claim(
            Bytes::from("tools:wf1"),
            Bytes::from("call:done"),
            60_000,
            Some(Bytes::from(r#"{"q":"weather"}"#)),
        );
        assert_eq!(claim_done.state, crate::agent::ToolClaimState::Claimed);
        db.agent_tool_complete(
            Bytes::from("tools:wf1"),
            Bytes::from("call:done"),
            Bytes::from(r#"{"temp":72}"#),
            Some(3_600_000),
        );
        let claim_inflight = db.agent_tool_claim(
            Bytes::from("tools:wf1"),
            Bytes::from("call:inflight"),
            60_000,
            Some(Bytes::from(r#"{"q":"search"}"#)),
        );
        assert_eq!(claim_inflight.state, crate::agent::ToolClaimState::Claimed);

        let verify_restored = |target: &mut ShardDb| {
            // Verify Semantic Cache
            let hit = target
                .semantic_get(
                    &Bytes::from("llm:prod"),
                    &[1.0, 0.0, 0.0],
                    0.95,
                    Some(b"tenant:acme"),
                )
                .unwrap()
                .expect("semantic cache hit expected");
            assert_eq!(hit.id, Bytes::from("q1"));
            assert_eq!(
                hit.response,
                Bytes::from("Rudis is a thread-per-core AI-native data platform.")
            );

            // Verify Agent Memory working window + episodic recall
            let ctx = target
                .agent_mem_context(&Bytes::from("sess:agent1"), 100, Some(&[1.0, 0.0, 0.0]), 2)
                .unwrap();
            assert_eq!(ctx.recent_turns.len(), 2);
            assert_eq!(
                ctx.recent_turns[0].content,
                Bytes::from("User shared secret code.")
            );
            assert_eq!(
                ctx.recent_turns[1].content,
                Bytes::from("Now help me deploy.")
            );
            assert!(!ctx.recalled_episodes.is_empty());
            assert_eq!(
                ctx.recalled_episodes[0].content,
                Bytes::from(" Secret code is 7788.")
            );

            // Verify Agent Checkpoint DAG history
            let history = target.agent_checkpoint_history(&Bytes::from("thread:wf1"), None, 10);
            assert_eq!(history.len(), 2);
            assert_eq!(history[0].step_id, Bytes::from("step2"));
            assert_eq!(history[0].parent_id, Some(Bytes::from("step1")));
            assert_eq!(history[1].step_id, Bytes::from("step1"));

            // Verify Agent Tool Registry (completed & in-flight)
            let check_done = target.agent_tool_claim(
                Bytes::from("tools:wf1"),
                Bytes::from("call:done"),
                30_000,
                None,
            );
            assert_eq!(check_done.state, crate::agent::ToolClaimState::Completed);
            assert_eq!(check_done.output, Some(Bytes::from(r#"{"temp":72}"#)));

            let check_inflight = target.agent_tool_claim(
                Bytes::from("tools:wf1"),
                Bytes::from("call:inflight"),
                30_000,
                None,
            );
            assert_eq!(
                check_inflight.state,
                crate::agent::ToolClaimState::InProgress
            );
        };

        // Test 1: RDB chunk save & restore round-trip
        let mut rdb_chunk = Vec::new();
        db.save_rdb_chunk(&mut rdb_chunk);
        let mut rdb_restored = ShardDb::new(6379);
        rdb_restored.restore_rdb_chunk(&rdb_chunk).unwrap();
        verify_restored(&mut rdb_restored);

        // Test 2: AOF rewrite & replay round-trip
        rewrite_shard_aof(&mut db, &temp_dir, 0).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");
        let mut aof_restored = ShardDb::new(6379);
        replay_aof(&aof_file, &mut aof_restored).unwrap();
        verify_restored(&mut aof_restored);

        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
