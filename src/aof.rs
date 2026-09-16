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
            file: Some(std::rc::Rc::new(file)),
            path,
            offset,
        })
    }

    pub fn new_in_memory() -> Self {
        Self {
            buffer: Vec::with_capacity(65536),
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
    pub fn take_flush_chunk(&mut self) -> Option<(std::rc::Rc<monoio::fs::File>, Vec<u8>, u64)> {
        if self.buffer.is_empty() {
            return None;
        }
        let file = self.file.clone()?;
        let chunk = std::mem::replace(&mut self.buffer, Vec::with_capacity(65536));
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
        Command::Expire(key, dur) => {
            let ms = dur.as_millis().max(1);
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
