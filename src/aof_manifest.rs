//! Redis 7's multi-part AOF manifest (`<appendfilename>.manifest` in
//! `appenddirname`): which files make up the AOF, in what order. One line
//! per file, `file <name> seq <n> type <b|h|i>`: at most one base (the
//! dataset when the last rewrite started, as RDB or as commands), history
//! files (replaced, kept only until deleted) and incr files (the writes
//! since, replayed in `seq` order after the base).
//!
//! rudis writes one incr file per shard and tags its line `shard <i>`: Redis
//! and Valkey ignore keys they don't know, so they still load the directory
//! (each key lives in one shard, so its whole history is in one incr file
//! and replaying the files one after another keeps every key's order),
//! while rudis replays each shard's file on that shard, in parallel.

use std::io::Write;
use std::path::{Path, PathBuf};

/// What an AOF file is to the manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AofFileType {
    Base,
    History,
    Incr,
}

impl AofFileType {
    fn letter(self) -> char {
        match self {
            AofFileType::Base => 'b',
            AofFileType::History => 'h',
            AofFileType::Incr => 'i',
        }
    }
}

/// One manifest line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AofFileInfo {
    pub name: String,
    pub seq: u64,
    pub kind: AofFileType,
    /// The rudis shard whose writes an incr file holds (`shard <i>`).
    pub shard: Option<usize>,
}

/// A parsed manifest, in the order Redis keeps it: base, history, incr.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AofManifest {
    pub base: Option<AofFileInfo>,
    pub history: Vec<AofFileInfo>,
    pub incr: Vec<AofFileInfo>,
}

/// Why a manifest was refused, worded as Redis words it where it has a
/// message for the case.
pub type ManifestError = String;

/// The manifest's file name for `appendfilename`.
pub fn manifest_name(appendfilename: &str) -> String {
    format!("{appendfilename}.manifest")
}

/// A base file's name: `<appendfilename>.<seq>.base.rdb` or `.base.aof`.
pub fn base_name(appendfilename: &str, seq: u64, rdb: bool) -> String {
    format!(
        "{appendfilename}.{seq}.base.{}",
        if rdb { "rdb" } else { "aof" }
    )
}

/// An incr file's name: `<appendfilename>.<seq>.incr.aof`.
pub fn incr_name(appendfilename: &str, seq: u64) -> String {
    format!("{appendfilename}.{seq}.incr.aof")
}

/// Splits a manifest line into words, with Redis's quoting rules
/// (sdssplitargs: `"..."` with C escapes and `\xHH`, or `'...'`).
pub fn split_args(line: &str) -> Option<Vec<String>> {
    let b = line.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == b.len() {
            return Some(out);
        }
        let mut tok: Vec<u8> = Vec::new();
        match b[i] {
            b'"' => {
                i += 1;
                loop {
                    match *b.get(i)? {
                        b'"' => break,
                        b'\\' => {
                            let c = *b.get(i + 1)?;
                            if c == b'x'
                                && let Some(hex) = b.get(i + 2..i + 4)
                                && let Ok(v) =
                                    u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16)
                            {
                                tok.push(v);
                                i += 4;
                                continue;
                            }
                            tok.push(match c {
                                b'n' => b'\n',
                                b'r' => b'\r',
                                b't' => b'\t',
                                b'b' => 8,
                                b'a' => 7,
                                other => other,
                            });
                            i += 2;
                        }
                        c => {
                            tok.push(c);
                            i += 1;
                        }
                    }
                }
                i += 1;
                if b.get(i).is_some_and(|c| !c.is_ascii_whitespace()) {
                    return None;
                }
            }
            b'\'' => {
                i += 1;
                loop {
                    match *b.get(i)? {
                        b'\'' => break,
                        b'\\' if b.get(i + 1) == Some(&b'\'') => {
                            tok.push(b'\'');
                            i += 2;
                        }
                        c => {
                            tok.push(c);
                            i += 1;
                        }
                    }
                }
                i += 1;
                if b.get(i).is_some_and(|c| !c.is_ascii_whitespace()) {
                    return None;
                }
            }
            _ => {
                while i < b.len() && !b[i].is_ascii_whitespace() {
                    tok.push(b[i]);
                    i += 1;
                }
            }
        }
        out.push(String::from_utf8(tok).ok()?);
    }
}

/// `name` as Redis writes it in a manifest: as is, or quoted with escapes
/// (sdscatrepr) when it has spaces, quotes or unprintable bytes.
fn repr_name(name: &str) -> String {
    // sdsneedsrepr: backslash, double quote, whitespace or unprintable.
    let plain = !name.is_empty()
        && name
            .bytes()
            .all(|c| c.is_ascii_graphic() && c != b'"' && c != b'\\');
    if plain {
        return name.to_string();
    }
    let mut s = String::from("\"");
    for c in name.bytes() {
        match c {
            b'\\' => s.push_str("\\\\"),
            b'"' => s.push_str("\\\""),
            b'\n' => s.push_str("\\n"),
            b'\r' => s.push_str("\\r"),
            b'\t' => s.push_str("\\t"),
            7 => s.push_str("\\a"),
            8 => s.push_str("\\b"),
            c if c.is_ascii_graphic() || c == b' ' => s.push(c as char),
            c => s.push_str(&format!("\\x{c:02x}")),
        }
    }
    s.push('"');
    s
}

impl AofManifest {
    /// Parses a manifest's text, with Redis's checks: one base at most,
    /// incr `seq`s increasing, file names (not paths), no blank lines.
    pub fn parse(text: &str) -> Result<Self, ManifestError> {
        if text.is_empty() {
            return Err("Found an empty AOF manifest".into());
        }
        let mut m = AofManifest::default();
        let mut any = false;
        for raw in text.lines() {
            if raw.len() > 1024 {
                return Err("The AOF manifest file contains too long line".into());
            }
            let line = raw.trim();
            if line.is_empty() {
                return Err("Invalid AOF manifest file format".into());
            }
            if line.starts_with('#') {
                continue;
            }
            let bad = || format!("Invalid AOF manifest file format: {line:?}");
            let parts = split_args(line).ok_or_else(bad)?;
            if !parts.len().is_multiple_of(2) {
                return Err(bad());
            }
            // `key value` pairs in any order; unknown keys are ignored, as in
            // Redis (rudis reads `shard`).
            let (mut name, mut seq, mut kind, mut shard) = (None, None, None, None);
            for pair in parts.chunks(2) {
                match pair[0].as_str() {
                    "file" => name = Some(pair[1].clone()),
                    "seq" => seq = Some(pair[1].as_str()),
                    "type" => kind = Some(pair[1].as_str()),
                    "shard" => shard = pair[1].parse::<usize>().ok(),
                    _ => {}
                }
            }
            let (Some(name), Some(seq), Some(kind)) = (name, seq, kind) else {
                return Err(bad());
            };
            let seq = match seq.parse::<u64>() {
                Ok(s) if s > 0 => s,
                _ => return Err(bad()),
            };
            if name.is_empty()
                || name == "."
                || name == ".."
                || name.contains('/')
                || name.contains('\\')
            {
                return Err(format!("File can't be a path, just a filename: {name:?}"));
            }
            any = true;
            let kind = match kind {
                "b" => AofFileType::Base,
                "h" => AofFileType::History,
                "i" => AofFileType::Incr,
                other => return Err(format!("Unknown AOF file type '{other}'")),
            };
            let info = AofFileInfo {
                name,
                seq,
                kind,
                shard,
            };
            match kind {
                AofFileType::Base => {
                    if m.base.is_some() {
                        return Err("Found duplicate base file information".into());
                    }
                    m.base = Some(info);
                }
                AofFileType::History => m.history.push(info),
                AofFileType::Incr => {
                    if m.incr.last().is_some_and(|p| seq <= p.seq) {
                        return Err(format!("Found a non-monotonic sequence number: {line:?}"));
                    }
                    m.incr.push(info);
                }
            }
        }
        if !any {
            return Err("Found an empty AOF manifest".into());
        }
        Ok(m)
    }

    /// The manifest's text, as Redis writes it (base, history, incr), with
    /// `shard <i>` after the fields Redis knows.
    pub fn to_text(&self) -> String {
        let mut s = String::new();
        for f in self.base.iter().chain(&self.history).chain(&self.incr) {
            s.push_str(&format!(
                "file {} seq {} type {}",
                repr_name(&f.name),
                f.seq,
                f.kind.letter()
            ));
            if let Some(shard) = f.shard {
                s.push_str(&format!(" shard {shard}"));
            }
            s.push('\n');
        }
        s
    }

    /// The files to load, in order: the base, then the incr files.
    pub fn load_order(&self) -> impl Iterator<Item = &AofFileInfo> {
        self.base.iter().chain(&self.incr)
    }

    /// The highest `seq` in use among base, history and incr files.
    pub fn max_seq(&self) -> u64 {
        self.base
            .iter()
            .chain(&self.history)
            .chain(&self.incr)
            .map(|f| f.seq)
            .max()
            .unwrap_or(0)
    }

    /// Reads `<dir>/<appendfilename>.manifest`: `None` if there is none.
    pub fn load(dir: &Path, appendfilename: &str) -> std::io::Result<Option<Self>> {
        let path = dir.join(manifest_name(appendfilename));
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        Self::parse(&text).map(Some).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e} in {path:?}"))
        })
    }

    /// Writes the manifest into `dir` crash-safely, as Redis does: a temp
    /// file (`temp-<name>`), fsynced, renamed over the manifest, then the
    /// directory fsynced so the rename itself survives a crash.
    pub fn persist(&self, dir: &Path, appendfilename: &str) -> std::io::Result<()> {
        let name = manifest_name(appendfilename);
        let tmp: PathBuf = dir.join(format!("temp-{name}"));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(self.to_text().as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, dir.join(&name))?;
        std::fs::File::open(dir)?.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incr(name: &str, seq: u64, shard: Option<usize>) -> AofFileInfo {
        AofFileInfo {
            name: name.into(),
            seq,
            kind: AofFileType::Incr,
            shard,
        }
    }

    #[test]
    fn test_reads_what_redis_writes() {
        // Valkey 9.2's getAofManifestAsString output for a rewritten AOF.
        let text = "file appendonly.aof.2.base.rdb seq 2 type b\n\
                    file appendonly.aof.1.incr.aof seq 1 type h\n\
                    file appendonly.aof.2.incr.aof seq 2 type i\n\
                    file appendonly.aof.3.incr.aof seq 3 type i\n";
        let m = AofManifest::parse(text).unwrap();
        assert_eq!(m.base.as_ref().unwrap().name, "appendonly.aof.2.base.rdb");
        assert_eq!(m.history.len(), 1);
        assert_eq!(
            m.load_order().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            [
                "appendonly.aof.2.base.rdb",
                "appendonly.aof.2.incr.aof",
                "appendonly.aof.3.incr.aof"
            ]
        );
        assert_eq!(m.max_seq(), 3);
        // And writes it back byte for byte.
        assert_eq!(m.to_text(), text);
    }

    #[test]
    fn test_round_trips_shards_and_quoted_names() {
        let m = AofManifest {
            base: Some(AofFileInfo {
                name: base_name("appendonly.aof", 4, false),
                seq: 4,
                kind: AofFileType::Base,
                shard: None,
            }),
            history: vec![],
            incr: vec![
                incr(&incr_name("appendonly.aof", 7), 7, Some(0)),
                incr(&incr_name("appendonly.aof", 8), 8, Some(1)),
                incr("odd name\t\"x\".aof", 9, None),
            ],
        };
        let text = m.to_text();
        assert!(text.contains("file appendonly.aof.4.base.aof seq 4 type b\n"));
        assert!(text.contains("file appendonly.aof.7.incr.aof seq 7 type i shard 0\n"));
        assert!(text.contains("file \"odd name\\t\\\"x\\\".aof\" seq 9 type i\n"));
        assert_eq!(AofManifest::parse(&text).unwrap(), m);
    }

    #[test]
    fn test_refuses_what_redis_refuses() {
        for (text, why) in [
            ("", "empty"),
            (
                "file a seq 1 type b\nfile b seq 2 type b\n",
                "duplicate base",
            ),
            (
                "file a seq 2 type i\nfile b seq 2 type i\n",
                "non-monotonic",
            ),
            ("file ../a seq 1 type i\n", "path"),
            ("file a seq 0 type i\n", "zero seq"),
            ("file a seq 1 type x\n", "unknown type"),
            ("file a seq 1\n", "missing type"),
            ("file a seq 1 type i\n\nfile b seq 2 type i\n", "blank line"),
            ("# only a comment\n", "no entries"),
        ] {
            assert!(AofManifest::parse(text).is_err(), "{why}: {text:?}");
        }
        // Unknown keys are ignored, as in Redis.
        let m = AofManifest::parse("file a seq 1 type i future 42 shard 3\n").unwrap();
        assert_eq!(m.incr[0].shard, Some(3));
    }

    #[test]
    fn test_persists_atomically() {
        let dir = std::env::temp_dir().join(format!("rudis-manifest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(AofManifest::load(&dir, "appendonly.aof").unwrap(), None);
        let m = AofManifest {
            incr: vec![incr(&incr_name("appendonly.aof", 1), 1, Some(0))],
            ..Default::default()
        };
        m.persist(&dir, "appendonly.aof").unwrap();
        assert!(!dir.join("temp-appendonly.aof.manifest").exists());
        assert_eq!(AofManifest::load(&dir, "appendonly.aof").unwrap(), Some(m));
        std::fs::write(dir.join("appendonly.aof.manifest"), "garbage\n").unwrap();
        assert!(AofManifest::load(&dir, "appendonly.aof").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
