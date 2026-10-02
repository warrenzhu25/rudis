use bytes::BytesMut;
use std::path::{Path, PathBuf};

use crate::resp::Command;
use crate::shard::ShardDb;

/// Absolute unix-ms deadline `d` from now. Expiries are written to the AOF as
/// absolute times (like Redis' PEXPIREAT/PXAT propagation) so that downtime
/// between write and replay does not extend a key's lifetime.
fn unix_ms_after(d: std::time::Duration) -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        + d.as_millis()
}

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

    /// Points the writer at the freshly rewritten AOF at `self.path`.
    ///
    /// Must run synchronously right after the rewrite, with no `.await` in
    /// between, so no command can run in the gap: anything appended before is
    /// part of the rewritten snapshot (the pending buffer is dropped rather
    /// than appended twice) and anything appended after goes to the new file.
    /// A flush already in flight still targets the old, replaced file, which
    /// is harmless because its commands are in the snapshot too.
    pub fn swap_after_rewrite(&mut self) -> std::io::Result<u64> {
        if self.path.as_os_str().is_empty() {
            return Ok(0);
        }
        // The rewrite just renamed the new file into place.
        let std_file = std::fs::OpenOptions::new().write(true).open(&self.path)?;
        let new_size = std_file.metadata()?.len();
        self.file = Some(std::rc::Rc::new(monoio::fs::File::from_std(std_file)?));
        self.offset = new_size;
        self.buffer.clear();
        Ok(new_size)
    }
}

/// Rewrites shard `shard_id`'s AOF from `db` and, if `aof` is the live
/// writer, switches it to the new file in the same synchronous step.
pub fn rewrite_and_swap_shard_aof(
    db: &mut ShardDb,
    dir: &Path,
    shard_id: usize,
    aof: Option<&std::rc::Rc<std::cell::RefCell<AofWriter>>>,
) -> std::io::Result<usize> {
    let count = rewrite_shard_aof(db, dir, shard_id)?;
    if let Some(aof) = aof {
        aof.borrow_mut().swap_after_rewrite()?;
    }
    Ok(count)
}

pub fn command_to_resp(cmd: &Command) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    match cmd {
        Command::Set {
            key,
            value,
            expire_in,
            keepttl,
            ..
        } => {
            if *keepttl {
                buf.extend_from_slice(format!("*4\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n", value.len()).as_bytes());
                buf.extend_from_slice(value);
                buf.extend_from_slice(b"\r\n$7\r\nKEEPTTL\r\n");
            } else if let Some(dur) = expire_in {
                let ms = dur.as_millis().max(1);
                let abs_ms = if ms < 1_000_000_000_000 {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    now_ms + ms
                } else {
                    ms
                };
                let ms_str = abs_ms.to_string();
                buf.extend_from_slice(format!("*5\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(format!("\r\n${}\r\n", value.len()).as_bytes());
                buf.extend_from_slice(value);
                buf.extend_from_slice(
                    format!("\r\n$4\r\nPXAT\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
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
                    buf.extend_from_slice(b"$4\r\nPXAT\r\n");
                    let ms_str = unix_ms_after(*d).to_string();
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
        Command::Unlink(keys) => {
            buf.extend_from_slice(format!("*{}\r\n$6\r\nUNLINK\r\n", 1 + keys.len()).as_bytes());
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
            let ms_str = unix_ms_after(*duration).to_string();
            buf.extend_from_slice(
                format!("*3\r\n$9\r\nPEXPIREAT\r\n${}\r\n", key.len()).as_bytes(),
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
        Command::Copy {
            source,
            destination,
            destination_db,
            replace,
        } => {
            let mut count = 3;
            if destination_db.is_some() {
                count += 2;
            }
            if *replace {
                count += 1;
            }
            buf.extend_from_slice(
                format!("*{}\r\n$4\r\nCOPY\r\n${}\r\n", count, source.len()).as_bytes(),
            );
            buf.extend_from_slice(source);
            buf.extend_from_slice(format!("\r\n${}\r\n", destination.len()).as_bytes());
            buf.extend_from_slice(destination);
            if let Some(db_id) = destination_db {
                buf.extend_from_slice(
                    format!("\r\n$2\r\nDB\r\n${}\r\n{}", db_id.to_string().len(), db_id).as_bytes(),
                );
            }
            if *replace {
                buf.extend_from_slice(b"\r\n$7\r\nREPLACE");
            }
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
        Command::Bitfield { key, ops, readonly } => {
            if *readonly {
                return None;
            }
            let mut parts: Vec<Vec<u8>> = Vec::new();
            parts.push(b"BITFIELD".to_vec());
            parts.push(key.to_vec());
            let mut last_overflow = crate::resp::BitfieldOverflow::Wrap;
            for op in ops {
                if op.overflow != last_overflow {
                    parts.push(b"OVERFLOW".to_vec());
                    match op.overflow {
                        crate::resp::BitfieldOverflow::Wrap => parts.push(b"WRAP".to_vec()),
                        crate::resp::BitfieldOverflow::Sat => parts.push(b"SAT".to_vec()),
                        crate::resp::BitfieldOverflow::Fail => parts.push(b"FAIL".to_vec()),
                    }
                    last_overflow = op.overflow;
                }
                let type_str = format!("{}{}", if op.sign { "i" } else { "u" }, op.bits);
                let off_str = op.offset.to_string();
                match op.op_type {
                    crate::resp::BitfieldOpType::Get => {
                        parts.push(b"GET".to_vec());
                        parts.push(type_str.into_bytes());
                        parts.push(off_str.into_bytes());
                    }
                    crate::resp::BitfieldOpType::Set(val) => {
                        parts.push(b"SET".to_vec());
                        parts.push(type_str.into_bytes());
                        parts.push(off_str.into_bytes());
                        parts.push(val.to_string().into_bytes());
                    }
                    crate::resp::BitfieldOpType::Incrby(incr) => {
                        parts.push(b"INCRBY".to_vec());
                        parts.push(type_str.into_bytes());
                        parts.push(off_str.into_bytes());
                        parts.push(incr.to_string().into_bytes());
                    }
                }
            }
            buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
            for p in parts {
                buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
                buf.extend_from_slice(&p);
                buf.extend_from_slice(b"\r\n");
            }
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
            approx: _,
            trim_strategy: _,
            idmp: _,
            id,
            fields,
            limit: _,
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
        Command::Xidmprecord {
            key,
            pid,
            iid,
            id_raw,
        } => {
            buf.extend_from_slice(
                format!("*5\r\n$11\r\nXIDMPRECORD\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", pid.len()).as_bytes());
            buf.extend_from_slice(pid);
            buf.extend_from_slice(format!("\r\n${}\r\n", iid.len()).as_bytes());
            buf.extend_from_slice(iid);
            buf.extend_from_slice(format!("\r\n${}\r\n", id_raw.len()).as_bytes());
            buf.extend_from_slice(id_raw);
            buf.extend_from_slice(b"\r\n");
            Some(buf)
        }
        Command::Xtrim {
            key,
            maxlen,
            minid,
            approx: _,
            trim_strategy: _,
            limit: _,
        } => {
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
            entries_read,
        } => {
            let id_str = id.to_string();
            let mut num_args = 5;
            if *mkstream {
                num_args += 1;
            }
            if entries_read.is_some() {
                num_args += 2;
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
            if let Some(er) = entries_read {
                let er_str = er.to_string();
                buf.extend_from_slice(b"$11\r\nENTRIESREAD\r\n");
                buf.extend_from_slice(format!("${}\r\n{}\r\n", er_str.len(), er_str).as_bytes());
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
        Command::XgroupSetId {
            key,
            group,
            id,
            entries_read,
        } => {
            let mut num_args = 5;
            if entries_read.is_some() {
                num_args += 2;
            }
            buf.extend_from_slice(
                format!(
                    "*{}\r\n$6\r\nXGROUP\r\n$5\r\nSETID\r\n${}\r\n",
                    num_args,
                    key.len()
                )
                .as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", group.len()).as_bytes());
            buf.extend_from_slice(group);
            buf.extend_from_slice(format!("\r\n${}\r\n{}\r\n", id.len(), id).as_bytes());
            if let Some(er) = entries_read {
                let er_str = er.to_string();
                buf.extend_from_slice(
                    format!("$11\r\nENTRIESREAD\r\n${}\r\n{}\r\n", er_str.len(), er_str).as_bytes(),
                );
            }
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
        Command::Xnack {
            key,
            group,
            mode,
            ids,
            retrycount,
            force,
        } => {
            let mut num_args = 3 + 1 + 2 + ids.len();
            if retrycount.is_some() {
                num_args += 2;
            }
            if *force {
                num_args += 1;
            }
            buf.extend_from_slice(
                format!("*{}\r\n$5\r\nXNACK\r\n${}\r\n", num_args, key.len()).as_bytes(),
            );
            buf.extend_from_slice(key);
            buf.extend_from_slice(format!("\r\n${}\r\n", group.len()).as_bytes());
            buf.extend_from_slice(group);
            buf.extend_from_slice(b"\r\n");
            let mode_str = match mode {
                crate::resp::XnackMode::Silent => "SILENT",
                crate::resp::XnackMode::Fail => "FAIL",
                crate::resp::XnackMode::Fatal => "FATAL",
            };
            buf.extend_from_slice(format!("${}\r\n{}\r\n", mode_str.len(), mode_str).as_bytes());
            buf.extend_from_slice(b"$3\r\nIDS\r\n");
            let numids_s = ids.len().to_string();
            buf.extend_from_slice(format!("${}\r\n{}\r\n", numids_s.len(), numids_s).as_bytes());
            for id in ids {
                let id_s = id.to_string();
                buf.extend_from_slice(format!("${}\r\n{}\r\n", id_s.len(), id_s).as_bytes());
            }
            if let Some(rc) = retrycount {
                let rc_s = rc.to_string();
                buf.extend_from_slice(b"$10\r\nRETRYCOUNT\r\n");
                buf.extend_from_slice(format!("${}\r\n{}\r\n", rc_s.len(), rc_s).as_bytes());
            }
            if *force {
                buf.extend_from_slice(b"$5\r\nFORCE\r\n");
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
        Command::Lmovem {
            source,
            destination,
            where_from,
            where_to,
            mode,
            count,
            ordering,
            raw_tokens,
        } => {
            let (from_s, to_s, mode_s, order_s) = if let Some(tokens) = raw_tokens {
                (
                    &tokens[0][..],
                    &tokens[1][..],
                    &tokens[2][..],
                    &tokens[3][..],
                )
            } else {
                let from_s = match where_from {
                    crate::table::ListDirection::Left => b"left".as_slice(),
                    crate::table::ListDirection::Right => b"right".as_slice(),
                };
                let to_s = match where_to {
                    crate::table::ListDirection::Left => b"left".as_slice(),
                    crate::table::ListDirection::Right => b"right".as_slice(),
                };
                let mode_s = match mode {
                    crate::resp::LmovemMode::Count => b"COUNT".as_slice(),
                    crate::resp::LmovemMode::Exactly => b"EXACTLY".as_slice(),
                };
                let order_s = match ordering {
                    crate::resp::LmovemOrdering::Obo => b"OBO".as_slice(),
                    crate::resp::LmovemOrdering::Bulk => b"BULK".as_slice(),
                };
                (from_s, to_s, mode_s, order_s)
            };
            let count_s = count.to_string();
            buf.extend_from_slice(
                format!("*8\r\n$6\r\nLMOVEM\r\n${}\r\n", source.len()).as_bytes(),
            );
            buf.extend_from_slice(source);
            buf.extend_from_slice(format!("\r\n${}\r\n", destination.len()).as_bytes());
            buf.extend_from_slice(destination);
            buf.extend_from_slice(format!("\r\n${}\r\n", from_s.len(),).as_bytes());
            buf.extend_from_slice(from_s);
            buf.extend_from_slice(format!("\r\n${}\r\n", to_s.len(),).as_bytes());
            buf.extend_from_slice(to_s);
            buf.extend_from_slice(format!("\r\n${}\r\n", mode_s.len(),).as_bytes());
            buf.extend_from_slice(mode_s);
            buf.extend_from_slice(
                format!(
                    "\r\n${}\r\n{}\r\n${}\r\n",
                    count_s.len(),
                    count_s,
                    order_s.len(),
                )
                .as_bytes(),
            );
            buf.extend_from_slice(order_s);
            buf.extend_from_slice(b"\r\n");
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
            by,
            get,
            readonly: _,
        } => {
            let mut args: Vec<Vec<u8>> = vec![b"SORT".to_vec(), key.to_vec()];
            if let Some(by_pat) = by {
                args.push(b"BY".to_vec());
                args.push(by_pat.to_vec());
            }
            if let Some((offset, count)) = limit {
                args.push(b"LIMIT".to_vec());
                args.push(offset.to_string().into_bytes());
                args.push(count.to_string().into_bytes());
            }
            for get_pat in get {
                args.push(b"GET".to_vec());
                args.push(get_pat.to_vec());
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
            // Always propagate as an absolute deadline (see `unix_ms_after`).
            let cmd_name = "HPEXPIREAT";
            let exp_s = if *is_at {
                expire_ms.to_string()
            } else {
                unix_ms_after(std::time::Duration::from_millis((*expire_ms).max(0) as u64))
                    .to_string()
            };
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
                    val_s = unix_ms_after(std::time::Duration::from_millis((*ms).max(0) as u64))
                        .to_string();
                    args.push(b"PXAT");
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
                    val_s = unix_ms_after(std::time::Duration::from_millis((*ms).max(0) as u64))
                        .to_string();
                    args.push(b"PXAT");
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

static AOF_LOAD_TRUNCATED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// `aof-load-truncated`: when true (Redis default), an AOF whose last command
/// is incomplete is truncated to the last complete command and loaded.
pub fn set_aof_load_truncated(v: bool) {
    AOF_LOAD_TRUNCATED.store(v, std::sync::atomic::Ordering::Relaxed);
}

pub fn aof_load_truncated() -> bool {
    AOF_LOAD_TRUNCATED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Parses an `appendfsync` value into "fsync every second?".
///
/// `always` is rejected rather than silently downgraded: writes reach the AOF
/// through a background flusher, so a reply can't wait for its fsync.
pub fn parse_appendfsync(v: &str) -> Result<bool, String> {
    match v.to_ascii_lowercase().as_str() {
        "everysec" => Ok(true),
        "no" => Ok(false),
        "always" => Err(
            "appendfsync always is not supported (the AOF is flushed in the background); use everysec or no"
                .to_string(),
        ),
        other => Err(format!(
            "invalid appendfsync '{}' (expected everysec or no)",
            other
        )),
    }
}

type FsyncFlags = std::collections::HashMap<u16, std::sync::Arc<std::sync::atomic::AtomicBool>>;
static FSYNC_EVERY_SEC: std::sync::Mutex<Option<FsyncFlags>> = std::sync::Mutex::new(None);

/// Live `appendfsync` policy for the server on `port` (true = everysec). The
/// AOF flusher reads it on every tick, so `CONFIG SET appendfsync` applies
/// without a restart. Defaults to everysec.
pub fn fsync_every_sec_flag(port: u16) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    let mut map = FSYNC_EVERY_SEC.lock().unwrap_or_else(|e| e.into_inner());
    map.get_or_insert_with(Default::default)
        .entry(port)
        .or_insert_with(|| std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)))
        .clone()
}

pub fn set_fsync_every_sec(port: u16, every_sec: bool) {
    fsync_every_sec_flag(port).store(every_sec, std::sync::atomic::Ordering::Relaxed);
}

/// The `appendfsync` value reported by CONFIG GET.
pub fn appendfsync_name(port: u16) -> &'static str {
    if fsync_every_sec_flag(port).load(std::sync::atomic::Ordering::Relaxed) {
        "everysec"
    } else {
        "no"
    }
}

/// Replays an AOF into `db`, returning the number of commands applied.
///
/// Like Redis: an incomplete final command (e.g. a crash mid-write) is cut
/// off and the file truncated to the last complete command when
/// `aof-load-truncated` is on, otherwise it is an error. Any malformed
/// command before the end of the file is always an error, so the caller can
/// refuse to start rather than silently dropping everything after it.
pub fn replay_aof(path: &Path, db: &mut ShardDb) -> std::io::Result<usize> {
    replay_aof_with(path, db, aof_load_truncated())
}

pub fn replay_aof_with(
    path: &Path,
    db: &mut ShardDb,
    load_truncated: bool,
) -> std::io::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let data = std::fs::read(path)?;
    if data.is_empty() {
        return Ok(0);
    }
    let bad = |offset: usize, why: &str| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "bad AOF format at offset {} of {}: {}",
                offset,
                data.len(),
                why
            ),
        )
    };
    let mut buf = BytesMut::from(&data[..]);
    let mut count = 0;
    let mut dummy_out = Vec::new();
    while !buf.is_empty() {
        let offset = data.len() - buf.len();
        // rudis only ever appends RESP arrays; anything else is corruption
        // (the inline parser would otherwise accept garbage as a command).
        if buf[0] != b'*' {
            return Err(bad(offset, "expected a RESP array"));
        }
        match crate::resp::parse_command(&mut buf) {
            Ok(Some(cmd)) => {
                crate::connection::execute_local_command(&cmd, db, &mut dummy_out, None);
                dummy_out.clear();
                count += 1;
            }
            Ok(None) if buf.is_empty() => break,
            Ok(None) => {
                if !load_truncated {
                    return Err(bad(
                        offset,
                        "truncated final command (set aof-load-truncated yes to load anyway)",
                    ));
                }
                eprintln!(
                    "!!! Warning: short read while loading the AOF file {:?}: truncating it from {} to {} bytes (last complete command).",
                    path,
                    data.len(),
                    offset
                );
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(path)?
                    .set_len(offset as u64)?;
                break;
            }
            Err(e) => return Err(bad(offset, &e)),
        }
    }
    Ok(count)
}

/// Records how many shards wrote the `appendonly-{i}.aof` files in a dir.
pub const AOF_SHARDS_MANIFEST: &str = "appendonly.shards";
/// Present only while [`reshard_aof_dir`] is swapping files in; if it is
/// found at startup the swap was interrupted and needs a human.
pub const AOF_RESHARD_MARKER: &str = "appendonly.reshard-in-progress";
const AOF_RESHARD_STAGING: &str = ".aof-reshard-staging";

fn aof_shard_file_index(name: &str) -> Option<usize> {
    name.strip_prefix("appendonly-")?
        .strip_suffix(".aof")?
        .parse()
        .ok()
}

fn write_file_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    f.write_all(data)?;
    f.sync_all()
}

/// How keys were spread over the per-shard AOF files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AofLayout {
    shards: usize,
    /// Cluster mode routes by CRC16 slot, standalone by key hash.
    cluster_slots: bool,
}

impl AofLayout {
    fn current(shards: usize) -> Self {
        Self {
            shards,
            cluster_slots: crate::cluster::HAS_ACTIVE_CLUSTER
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    fn routing_name(&self) -> &'static str {
        if self.cluster_slots { "slots" } else { "hash" }
    }
}

fn write_aof_shards_manifest(dir: &Path, layout: AofLayout) -> std::io::Result<()> {
    let tmp = dir.join(format!("{}.tmp", AOF_SHARDS_MANIFEST));
    let text = format!(
        "shards {}\nrouting {}\n",
        layout.shards,
        layout.routing_name()
    );
    write_file_synced(&tmp, text.as_bytes())?;
    let target = dir.join(AOF_SHARDS_MANIFEST);
    std::fs::rename(&tmp, &target)?;
    sync_parent_dir(&target)
}

fn read_aof_shards_manifest(dir: &Path) -> std::io::Result<Option<AofLayout>> {
    let path = dir.join(AOF_SHARDS_MANIFEST);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut shards = None;
    let mut cluster_slots = None;
    for line in text.lines() {
        match line.trim().split_once(' ') {
            Some(("shards", n)) => shards = n.trim().parse::<usize>().ok().filter(|n| *n > 0),
            Some(("routing", "hash")) => cluster_slots = Some(false),
            Some(("routing", "slots")) => cluster_slots = Some(true),
            _ => {}
        }
    }
    match (shards, cluster_slots) {
        (Some(shards), Some(cluster_slots)) => Ok(Some(AofLayout {
            shards,
            cluster_slots,
        })),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("bad AOF shard manifest {:?}: {:?}", path, text),
        )),
    }
}

/// Returns argument `idx` of a raw RESP array command.
fn raw_resp_arg(raw: &[u8], idx: usize) -> Option<&[u8]> {
    fn line(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
        let end = pos + buf.get(pos..)?.windows(2).position(|w| w == b"\r\n")?;
        Some((&buf[pos..end], end + 2))
    }
    let (hdr, mut pos) = line(raw, 0)?;
    let argc: usize = std::str::from_utf8(hdr.strip_prefix(b"*")?)
        .ok()?
        .parse()
        .ok()?;
    if idx >= argc {
        return None;
    }
    for i in 0..=idx {
        let (len_line, next) = line(raw, pos)?;
        let len: usize = std::str::from_utf8(len_line.strip_prefix(b"$")?)
            .ok()?
            .parse()
            .ok()?;
        let arg = raw.get(next..next + len)?;
        if i == idx {
            return Some(arg);
        }
        pos = next + len + 2;
    }
    None
}

/// Makes the per-shard AOF files in `dir` match `num_shards`.
///
/// Each shard replays only `appendonly-{shard}.aof`, and keys are owned by
/// `hash(key) % num_shards`, so restarting with a different `--threads` would
/// load keys into shards that never look them up (or drop whole files). When
/// the count changed, this replays every old file into one in-memory db,
/// rewrites it, and splits the rewrite by key owner into new per-shard files.
/// The old files are kept in a backup dir. The shard count and routing scheme
/// (key hash, or cluster slots) are recorded in [`AOF_SHARDS_MANIFEST`]; dirs
/// from before the manifest existed are assumed to have one file per shard
/// (the writer creates every shard's file) and the current routing scheme.
///
/// Returns the old shard count if a reshard happened.
pub fn reshard_aof_dir(dir: &Path, num_shards: usize, port: u16) -> std::io::Result<Option<usize>> {
    let invalid = |msg: String| std::io::Error::new(std::io::ErrorKind::InvalidData, msg);
    let marker = dir.join(AOF_RESHARD_MARKER);
    if marker.exists() {
        let backup = std::fs::read_to_string(&marker).unwrap_or_default();
        return Err(invalid(format!(
            "a previous AOF reshard in {:?} was interrupted. The original AOF files are in {:?}. \
             Restore them into {:?} (removing any appendonly-*.aof there), delete {:?}, and restart",
            dir,
            backup.trim(),
            dir,
            marker
        )));
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut max_index: Option<usize> = None;
    for entry in entries {
        let name = entry?.file_name();
        if let Some(i) = name.to_str().and_then(aof_shard_file_index) {
            max_index = Some(max_index.map_or(i, |m| m.max(i)));
        }
    }
    // Leftovers from a reshard that crashed before touching the real files.
    let staging = dir.join(AOF_RESHARD_STAGING);
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }

    let new_layout = AofLayout::current(num_shards);
    let old_layout = match (read_aof_shards_manifest(dir)?, max_index) {
        (Some(l), Some(i)) if i >= l.shards => {
            return Err(invalid(format!(
                "{:?} says {} shards but appendonly-{}.aof exists",
                dir.join(AOF_SHARDS_MANIFEST),
                l.shards,
                i
            )));
        }
        (Some(l), _) => l,
        (None, Some(i)) => AofLayout {
            shards: i + 1,
            ..new_layout
        },
        (None, None) => new_layout,
    };
    let old_shards = old_layout.shards;
    if old_layout == new_layout {
        if !dir.join(AOF_SHARDS_MANIFEST).exists() {
            write_aof_shards_manifest(dir, new_layout)?;
        }
        return Ok(None);
    }

    // 1. Load everything. Each key only ever lived in one file, so its
    //    history replays in order.
    let mut combined = ShardDb::new(port);
    for i in 0..old_shards {
        replay_aof(&dir.join(format!("appendonly-{}.aof", i)), &mut combined)?;
    }

    // 2. Rewrite to canonical per-key commands and split them by owner.
    std::fs::create_dir_all(&staging)?;
    rewrite_shard_aof(&mut combined, &staging, 0)?;
    drop(combined);
    let rewritten = std::fs::read(staging.join("appendonly-0.aof"))?;
    let mut outputs: Vec<Vec<u8>> = vec![Vec::new(); num_shards];
    let mut buf = BytesMut::from(&rewritten[..]);
    while !buf.is_empty() {
        let start = rewritten.len() - buf.len();
        let cmd = match crate::resp::parse_command(&mut buf) {
            Ok(Some(cmd)) => cmd,
            _ => return Err(invalid(format!("bad rewritten AOF at offset {}", start))),
        };
        let raw = &rewritten[start..rewritten.len() - buf.len()];
        let shard = crate::connection::target_shard_of_cmd(&cmd, num_shards)
            .or_else(|| raw_resp_arg(raw, 1).map(|k| crate::router::target_shard(k, num_shards)))
            .unwrap_or(0);
        outputs[shard].extend_from_slice(raw);
    }
    for (i, out) in outputs.iter().enumerate() {
        write_file_synced(&staging.join(format!("appendonly-{}.aof", i)), out)?;
    }
    sync_parent_dir(&staging.join("appendonly-0.aof"))?;

    // 3. Swap: old files to a backup dir, new files in, manifest last.
    let unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let backup = dir.join(format!(
        "aof-reshard-backup-{}to{}-{}",
        old_shards, num_shards, unix_ms
    ));
    write_file_synced(&marker, backup.to_string_lossy().as_bytes())?;
    sync_parent_dir(&marker)?;
    std::fs::create_dir_all(&backup)?;
    for i in 0..old_shards {
        let name = format!("appendonly-{}.aof", i);
        let old = dir.join(&name);
        if old.exists() {
            std::fs::rename(&old, backup.join(&name))?;
        }
    }
    sync_parent_dir(&backup.join("x"))?;
    for i in 0..num_shards {
        let name = format!("appendonly-{}.aof", i);
        std::fs::rename(staging.join(&name), dir.join(&name))?;
    }
    write_aof_shards_manifest(dir, new_layout)?;
    std::fs::remove_file(&marker)?;
    sync_parent_dir(&marker)?;
    let _ = std::fs::remove_dir_all(&staging);
    Ok(Some(old_shards))
}

pub fn rewrite_shard_aof(db: &mut ShardDb, dir: &Path, shard_id: usize) -> std::io::Result<usize> {
    static TMP_REWRITE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let tmp_id = TMP_REWRITE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp_path = dir.join(format!(
        "appendonly-{}.aof.tmp.{}_{}",
        shard_id,
        std::process::id(),
        tmp_id
    ));
    let target_path = dir.join(format!("appendonly-{}.aof", shard_id));
    let res = write_rewritten_aof(db, &tmp_path, &target_path);
    if res.is_err() {
        // Don't leave a partial rewrite behind.
        let _ = std::fs::remove_file(&tmp_path);
    }
    res
}

/// Writes `db` as an AOF to `tmp_path`, fsyncs it and renames it to `target_path`.
fn write_rewritten_aof(
    db: &mut ShardDb,
    tmp_path: &Path,
    target_path: &Path,
) -> std::io::Result<usize> {
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

    let file = std::fs::File::create(tmp_path)?;
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
                    let rem_ms_str = unix_ms_after(exp.saturating_duration_since(now)).to_string();
                    writer.write_all(b"*5\r\n$3\r\nSET\r\n$")?;
                    writer.write_all(k.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(k)?;
                    writer.write_all(b"\r\n$")?;
                    writer.write_all(val.len().to_string().as_bytes())?;
                    writer.write_all(b"\r\n")?;
                    writer.write_all(val.as_ref())?;
                    writer.write_all(b"\r\n$4\r\nPXAT\r\n$")?;
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
            crate::table::RudisValue::Stream(stream) => {
                if !stream.entries.is_empty() {
                    for (sid, fields) in &stream.entries {
                        let id_str = sid.to_string();
                        let num_args = 2 + 1 + fields.len() * 2;
                        writer.write_all(
                            format!("*{}\r\n$4\r\nXADD\r\n${}\r\n", num_args, k.len()).as_bytes(),
                        )?;
                        writer.write_all(k)?;
                        writer.write_all(
                            format!("\r\n${}\r\n{}\r\n", id_str.len(), id_str).as_bytes(),
                        )?;
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
                if stream.last_id != crate::table::StreamId::default() || stream.entries_added > 0 {
                    let last_id_str = stream.last_id.to_string();
                    let ea_str = stream.entries_added.to_string();
                    let md_str = stream.max_deleted_entry_id.to_string();
                    writer
                        .write_all(format!("*7\r\n$6\r\nXSETID\r\n${}\r\n", k.len()).as_bytes())?;
                    writer.write_all(k)?;
                    writer.write_all(
                        format!(
                            "\r\n${}\r\n{}\r\n$12\r\nENTRIESADDED\r\n${}\r\n{}\r\n$12\r\nMAXDELETEDID\r\n${}\r\n{}\r\n",
                            last_id_str.len(), last_id_str,
                            ea_str.len(), ea_str,
                            md_str.len(), md_str
                        ).as_bytes()
                    )?;
                    count += 1;
                }
                for grp in stream.groups.values() {
                    let last_deliv_str = grp.last_delivered_id.to_string();
                    if let Some(er) = grp.entries_read {
                        let er_str = er.to_string();
                        writer.write_all(
                            format!("*7\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n", k.len())
                                .as_bytes(),
                        )?;
                        writer.write_all(k)?;
                        writer.write_all(format!("\r\n${}\r\n", grp.name.len()).as_bytes())?;
                        writer.write_all(&grp.name)?;
                        writer.write_all(
                            format!(
                                "\r\n${}\r\n{}\r\n$11\r\nENTRIESREAD\r\n${}\r\n{}\r\n",
                                last_deliv_str.len(),
                                last_deliv_str,
                                er_str.len(),
                                er_str
                            )
                            .as_bytes(),
                        )?;
                    } else {
                        writer.write_all(
                            format!("*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n", k.len())
                                .as_bytes(),
                        )?;
                        writer.write_all(k)?;
                        writer.write_all(format!("\r\n${}\r\n", grp.name.len()).as_bytes())?;
                        writer.write_all(&grp.name)?;
                        writer.write_all(
                            format!("\r\n${}\r\n{}\r\n", last_deliv_str.len(), last_deliv_str)
                                .as_bytes(),
                        )?;
                    }
                    count += 1;

                    for (c_name, cons) in &grp.consumers {
                        for (&sid, &deliv_time) in &cons.pel {
                            let pe = match grp.pel.get(&sid) {
                                Some(p) => p,
                                None => continue,
                            };
                            let sid_str = sid.to_string();
                            let dt_str = deliv_time.to_string();
                            let rc_str = pe.delivery_count.to_string();
                            writer.write_all(
                                format!("*10\r\n$6\r\nXCLAIM\r\n${}\r\n", k.len()).as_bytes(),
                            )?;
                            writer.write_all(k)?;
                            writer.write_all(format!("\r\n${}\r\n", grp.name.len()).as_bytes())?;
                            writer.write_all(&grp.name)?;
                            writer.write_all(format!("\r\n${}\r\n", c_name.len()).as_bytes())?;
                            writer.write_all(c_name)?;
                            writer.write_all(
                                format!(
                                    "\r\n$1\r\n0\r\n${}\r\n{}\r\n$4\r\nTIME\r\n${}\r\n{}\r\n$10\r\nRETRYCOUNT\r\n${}\r\n{}\r\n$6\r\nJUSTID\r\n",
                                    sid_str.len(), sid_str,
                                    dt_str.len(), dt_str,
                                    rc_str.len(), rc_str,
                                ).as_bytes()
                            )?;
                            count += 1;
                        }
                    }

                    for (&sid, pe) in &grp.pel {
                        if pe.consumer.is_empty() {
                            let sid_str = sid.to_string();
                            let rc_str = pe.delivery_count.to_string();
                            writer.write_all(
                                format!("*10\r\n$5\r\nXNACK\r\n${}\r\n", k.len()).as_bytes(),
                            )?;
                            writer.write_all(k)?;
                            writer.write_all(format!("\r\n${}\r\n", grp.name.len()).as_bytes())?;
                            writer.write_all(&grp.name)?;
                            writer.write_all(
                                format!(
                                    "\r\n$4\r\nFAIL\r\n$3\r\nIDS\r\n$1\r\n1\r\n${}\r\n{}\r\n$10\r\nRETRYCOUNT\r\n${}\r\n{}\r\n$5\r\nFORCE\r\n",
                                    sid_str.len(), sid_str,
                                    rc_str.len(), rc_str,
                                ).as_bytes()
                            )?;
                            count += 1;
                        }
                    }
                }
            }
            _ => {}
        }

        if !matches!(val_ref, crate::table::RudisValue::String(_))
            && let Some(exp) = entry.expire_at
        {
            let rem_ms_str = unix_ms_after(exp.saturating_duration_since(now)).to_string();
            writer.write_all(b"*3\r\n$9\r\nPEXPIREAT\r\n$")?;
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
    std::fs::rename(tmp_path, target_path)?;
    let _ = sync_parent_dir(target_path);

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

    fn aof_tmp(name: &str, content: &[u8]) -> PathBuf {
        let p = std::env::temp_dir().join(format!("rudis-aoftail-{}-{}", std::process::id(), name));
        std::fs::write(&p, content).unwrap();
        p
    }

    #[test]
    fn test_appendfsync_parse_and_live_policy() {
        assert_eq!(parse_appendfsync("everysec"), Ok(true));
        assert_eq!(parse_appendfsync("No"), Ok(false));
        assert!(parse_appendfsync("always").is_err());
        assert!(parse_appendfsync("").is_err());

        // Per-port, defaults to everysec, and the flusher's handle sees updates.
        let port = 59_101;
        assert_eq!(appendfsync_name(port), "everysec");
        let flag = fsync_every_sec_flag(port);
        set_fsync_every_sec(port, false);
        assert!(!flag.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(appendfsync_name(port), "no");
        assert_eq!(appendfsync_name(59_102), "everysec");
        set_fsync_every_sec(port, true);
        assert!(flag.load(std::sync::atomic::Ordering::Relaxed));
    }

    const SET_A: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";

    /// Replays each `appendonly-{i}.aof` and checks every key sits in the
    /// shard that owns it. Returns the total key count.
    fn assert_aof_keys_owned(dir: &Path, num_shards: usize) -> usize {
        let mut total = 0;
        for i in 0..num_shards {
            let mut db = ShardDb::new(0);
            replay_aof(&dir.join(format!("appendonly-{}.aof", i)), &mut db).unwrap();
            for e in db.table.entries() {
                assert_eq!(
                    crate::router::target_shard(&e.key, num_shards),
                    i,
                    "key {:?} in wrong shard file",
                    e.key
                );
            }
            total += db.table.dbsize();
        }
        assert!(!dir.join(format!("appendonly-{}.aof", num_shards)).exists());
        total
    }

    #[test]
    fn test_reshard_aof_dir_moves_keys_to_owning_shards() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-reshard-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Legacy layout (no manifest), written by 4 shards.
        let mut dbs: Vec<ShardDb> = (0..4).map(|_| ShardDb::new(0)).collect();
        for k in 0..100 {
            let key = Bytes::from(format!("k{}", k));
            let ttl = (k % 10 == 0).then(|| Duration::from_secs(1000));
            dbs[crate::router::target_shard(&key, 4)].set(key, Bytes::from("v"), ttl);
        }
        for (i, db) in dbs.iter_mut().enumerate() {
            rewrite_shard_aof(db, &dir, i).unwrap();
        }

        assert_eq!(reshard_aof_dir(&dir, 2, 0).unwrap(), Some(4));
        assert_eq!(assert_aof_keys_owned(&dir, 2), 100);
        assert_eq!(
            read_aof_shards_manifest(&dir).unwrap(),
            Some(AofLayout::current(2))
        );
        assert!(!dir.join(AOF_RESHARD_MARKER).exists());
        assert!(!dir.join(AOF_RESHARD_STAGING).exists());
        let backups: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("aof-reshard-backup-4to2-")
            })
            .collect();
        assert_eq!(backups.len(), 1);
        assert_eq!(std::fs::read_dir(backups[0].path()).unwrap().count(), 4);

        // Same layout again: nothing to do.
        assert_eq!(reshard_aof_dir(&dir, 2, 0).unwrap(), None);
        // Grow, and TTLs survive the round trips.
        assert_eq!(reshard_aof_dir(&dir, 8, 0).unwrap(), Some(2));
        assert_eq!(assert_aof_keys_owned(&dir, 8), 100);
        let key = Bytes::from("k10");
        let mut db = ShardDb::new(0);
        let owner = crate::router::target_shard(&key, 8);
        replay_aof(&dir.join(format!("appendonly-{}.aof", owner)), &mut db).unwrap();
        assert!(
            db.table
                .entries()
                .any(|e| e.key == key && e.expire_at.is_some())
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_reshard_aof_dir_refuses_after_interrupted_swap_or_bad_manifest() {
        let dir =
            std::env::temp_dir().join(format!("rudis-aof-reshard-bad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("appendonly-0.aof"), SET_A).unwrap();

        std::fs::write(dir.join(AOF_RESHARD_MARKER), "/backup/here").unwrap();
        let err = reshard_aof_dir(&dir, 1, 0).unwrap_err();
        assert!(err.to_string().contains("/backup/here"), "{err}");
        std::fs::remove_file(dir.join(AOF_RESHARD_MARKER)).unwrap();

        // Manifest says 1 shard but a second shard file exists.
        std::fs::write(dir.join(AOF_SHARDS_MANIFEST), "shards 1\nrouting hash\n").unwrap();
        std::fs::write(dir.join("appendonly-1.aof"), b"").unwrap();
        assert!(reshard_aof_dir(&dir, 2, 0).is_err());
        std::fs::remove_file(dir.join("appendonly-1.aof")).unwrap();

        // Staging leftovers from a crash before the swap are discarded.
        std::fs::create_dir_all(dir.join(AOF_RESHARD_STAGING)).unwrap();
        assert_eq!(reshard_aof_dir(&dir, 1, 0).unwrap(), None);
        assert!(!dir.join(AOF_RESHARD_STAGING).exists());

        std::fs::write(dir.join(AOF_SHARDS_MANIFEST), "garbage").unwrap();
        assert!(reshard_aof_dir(&dir, 1, 0).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_raw_resp_arg() {
        assert_eq!(raw_resp_arg(SET_A, 0), Some(&b"SET"[..]));
        assert_eq!(raw_resp_arg(SET_A, 1), Some(&b"a"[..]));
        assert_eq!(raw_resp_arg(SET_A, 2), Some(&b"1"[..]));
        assert_eq!(raw_resp_arg(SET_A, 3), None);
        assert_eq!(raw_resp_arg(b"*2\r\n$3\r\nGET", 1), None);
    }

    #[test]
    fn test_replay_aof_truncated_tail_is_cut_when_allowed() {
        let mut content = SET_A.to_vec();
        content.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1"); // crash mid-write
        let p = aof_tmp("trunc-yes", &content);
        let mut db = ShardDb::new(0);
        assert_eq!(replay_aof_with(&p, &mut db, true).unwrap(), 1);
        assert_eq!(db.table.dbsize(), 1);
        // File now ends at the last complete command, so new appends are readable.
        assert_eq!(std::fs::read(&p).unwrap(), SET_A);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn test_replay_aof_truncated_tail_is_fatal_when_disallowed() {
        let mut content = SET_A.to_vec();
        content.extend_from_slice(b"*3\r\n$3\r\nSET");
        let p = aof_tmp("trunc-no", &content);
        let mut db = ShardDb::new(0);
        let err = replay_aof_with(&p, &mut db, false).unwrap_err();
        assert!(err.to_string().contains("aof-load-truncated"), "{err}");
        assert_eq!(
            std::fs::read(&p).unwrap(),
            content,
            "file must be untouched"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn test_replay_aof_mid_file_corruption_is_fatal() {
        let mut content = SET_A.to_vec();
        content.extend_from_slice(b"garbage\r\n");
        content.extend_from_slice(SET_A);
        let p = aof_tmp("corrupt", &content);
        let mut db = ShardDb::new(0);
        let err = replay_aof_with(&p, &mut db, true).unwrap_err();
        assert!(
            err.to_string().contains(&format!("offset {}", SET_A.len())),
            "{err}"
        );
        // Bad multibulk header is corruption too, even with aof-load-truncated.
        let p2 = aof_tmp("corrupt2", b"*x\r\n");
        assert!(replay_aof_with(&p2, &mut db, true).is_err());
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(&p2);
    }

    #[test]
    fn test_aof_expiries_are_absolute_so_downtime_does_not_extend_ttl() {
        let dir = std::env::temp_dir().join(format!("rudis-aof-abs-exp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Live AOF: SET, then EXPIRE/PEXPIRE propagated as an absolute deadline.
        let expire = Command::Expire {
            key: Bytes::from_static(b"live"),
            duration: Duration::from_millis(50),
            opts: Default::default(),
        };
        let resp = command_to_resp(&expire).unwrap();
        assert!(
            resp.starts_with(b"*3\r\n$9\r\nPEXPIREAT\r\n"),
            "{:?}",
            String::from_utf8_lossy(&resp)
        );
        let mut live = SET_A.to_vec(); // SET a 1 (no TTL)
        live.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\nlive\r\n$1\r\nx\r\n");
        live.extend_from_slice(&resp);
        let live_path = dir.join("live.aof");
        std::fs::write(&live_path, &live).unwrap();

        // Rewrite output for keys with TTLs (string and non-string).
        let mut src = ShardDb::new(0);
        src.set(
            Bytes::from("rw_str"),
            Bytes::from("v"),
            Some(Duration::from_millis(50)),
        );
        src.rpush(Bytes::from("rw_list"), vec![Bytes::from("e")])
            .unwrap();
        src.table
            .expire(b"rw_list", Duration::from_millis(50), Default::default());
        rewrite_shard_aof(&mut src, &dir, 0).unwrap();

        // "Downtime" longer than every TTL before replaying.
        std::thread::sleep(Duration::from_millis(120));

        let mut db = ShardDb::new(0);
        replay_aof_with(&live_path, &mut db, true).unwrap();
        assert_eq!(db.table.dbsize(), 1, "only the key without TTL may survive");

        let mut db = ShardDb::new(0);
        replay_aof_with(&dir.join("appendonly-0.aof"), &mut db, true).unwrap();
        assert_eq!(
            db.table.dbsize(),
            0,
            "rewritten keys must not outlive their TTL"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

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

            // Switch the writer to the rewritten file and log k2
            let new_size = writer.borrow_mut().swap_after_rewrite().unwrap();
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
    fn test_rewrite_swap_neither_duplicates_nor_loses_writes() {
        let temp_dir =
            std::env::temp_dir().join(format!("rudis-aof-swap-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        let aof_file = temp_dir.join("appendonly-0.aof");
        const INCR: &[u8] = b"*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n";

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .build()
            .unwrap();
        rt.block_on(async {
            let writer = std::rc::Rc::new(std::cell::RefCell::new(
                AofWriter::open(aof_file.clone()).await.unwrap(),
            ));
            let mut db = ShardDb::new(6379);
            // Two INCRs applied; the second is still only in the AOF buffer
            // when the rewrite snapshots the db.
            for flush in [true, false] {
                db.incr_by(Bytes::from("n"), 1).unwrap();
                writer.borrow_mut().append(INCR);
                if flush {
                    AofWriter::flush_rc(&writer).await.unwrap();
                }
            }
            rewrite_and_swap_shard_aof(&mut db, &temp_dir, 0, Some(&writer)).unwrap();
            // A write after the swap must land in the new file.
            db.incr_by(Bytes::from("n"), 1).unwrap();
            writer.borrow_mut().append(INCR);
            AofWriter::flush_rc(&writer).await.unwrap();

            let mut replayed = ShardDb::new(6379);
            replay_aof(&aof_file, &mut replayed).unwrap();
            assert_eq!(replayed.get(b"n"), Some(Bytes::from("3")));
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
