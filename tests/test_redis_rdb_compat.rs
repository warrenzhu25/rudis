//! Redis/Valkey RDB and DUMP compatibility, checked against a real Valkey
//! server (`~/valkey-stable/src/valkey-server`, or `$VALKEY_SERVER`). Tests
//! that need it skip (pass with a note) when it is not installed.
//!
//! Ports 18100-18199 and 18390-18399 are used.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// A minimal RESP2 client
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Reply {
    Status(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

impl Reply {
    fn bytes(&self) -> Vec<u8> {
        match self {
            Reply::Bulk(Some(b)) => b.clone(),
            Reply::Status(s) => s.as_bytes().to_vec(),
            Reply::Int(i) => i.to_string().into_bytes(),
            other => panic!("not a string reply: {other:?}"),
        }
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes()).into_owned()
    }

    fn int(&self) -> i64 {
        match self {
            Reply::Int(i) => *i,
            other => panic!("not an integer reply: {other:?}"),
        }
    }

    fn array(&self) -> Vec<Reply> {
        match self {
            Reply::Array(Some(v)) => v.clone(),
            Reply::Array(None) => Vec::new(),
            other => panic!("not an array reply: {other:?}"),
        }
    }

    /// A field/value array as a map.
    fn map(&self) -> BTreeMap<String, Reply> {
        self.array()
            .chunks(2)
            .map(|c| (c[0].text(), c[1].clone()))
            .collect()
    }

    /// A canonical, comparable rendering.
    fn render(&self) -> String {
        match self {
            Reply::Status(s) => format!("+{s}"),
            Reply::Error(e) => format!("-{e}"),
            Reply::Int(i) => format!(":{i}"),
            Reply::Bulk(None) => "nil".into(),
            Reply::Bulk(Some(b)) => format!("{:?}", String::from_utf8_lossy(b)),
            Reply::Array(None) => "nil[]".into(),
            Reply::Array(Some(v)) => {
                format!(
                    "[{}]",
                    v.iter().map(Reply::render).collect::<Vec<_>>().join(",")
                )
            }
        }
    }
}

struct Client {
    r: BufReader<TcpStream>,
}

impl Client {
    fn connect(port: u16) -> Client {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        Client {
            r: BufReader::new(s),
        }
    }

    fn cmd<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Reply {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            let a = a.as_ref();
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        self.r.get_mut().write_all(&out).unwrap();
        self.read()
    }

    fn c(&mut self, args: &str) -> Reply {
        let v: Vec<&str> = args.split(' ').collect();
        self.cmd(&v)
    }

    fn ok<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Reply {
        let r = self.cmd(args);
        if let Reply::Error(e) = &r {
            let shown: Vec<String> = args
                .iter()
                .map(|a| {
                    String::from_utf8_lossy(a.as_ref())
                        .chars()
                        .take(40)
                        .collect()
                })
                .collect();
            panic!("{shown:?} failed: {e}");
        }
        r
    }

    fn read(&mut self) -> Reply {
        let mut line = Vec::new();
        self.r.read_until(b'\n', &mut line).unwrap();
        assert!(line.ends_with(b"\r\n"), "bad reply line {line:?}");
        let body = String::from_utf8_lossy(&line[1..line.len() - 2]).into_owned();
        match line[0] {
            b'+' => Reply::Status(body),
            b'-' => Reply::Error(body),
            b':' => Reply::Int(body.parse().unwrap()),
            b'$' => {
                let n: i64 = body.parse().unwrap();
                if n < 0 {
                    return Reply::Bulk(None);
                }
                let mut b = vec![0u8; n as usize + 2];
                self.r.read_exact(&mut b).unwrap();
                b.truncate(n as usize);
                Reply::Bulk(Some(b))
            }
            b'*' => {
                let n: i64 = body.parse().unwrap();
                if n < 0 {
                    return Reply::Array(None);
                }
                Reply::Array(Some((0..n).map(|_| self.read()).collect()))
            }
            other => panic!("unexpected reply type {}", other as char),
        }
    }
}

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

fn valkey_bin() -> Option<PathBuf> {
    let p = std::env::var_os("VALKEY_SERVER")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| Path::new(&h).join("valkey-stable/src/valkey-server"))
        })?;
    p.is_file().then_some(p)
}

macro_rules! require_valkey {
    () => {
        match valkey_bin() {
            Some(p) => p,
            None => {
                eprintln!("valkey-server not found (set VALKEY_SERVER); skipping");
                return;
            }
        }
    };
}

struct Server {
    child: Child,
    port: u16,
    dir: PathBuf,
    name: &'static str,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn client(&self) -> Client {
        Client::connect(self.port)
    }

    fn wait_ready(mut self) -> Server {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                let log = std::fs::read_to_string(self.dir.join(format!("{}.log", self.name)))
                    .unwrap_or_default();
                panic!("{} exited ({status}) during startup:\n{log}", self.name);
            }
            if ping(self.port) {
                return self;
            }
            assert!(Instant::now() < deadline, "{} did not start", self.name);
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Kills the server without letting it save.
    fn kill(self) -> PathBuf {
        let dir = self.dir.clone();
        drop(self);
        dir
    }

    /// The server's own exit status after it was asked to stop.
    fn exit_status(mut self) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(Some(s)) = self.child.try_wait() {
                return Some(s);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        None
    }
}

/// PONG from the server (Valkey answers -LOADING until its RDB is in).
fn ping(port: u16) -> bool {
    let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    if s.write_all(b"*1\r\n$4\r\nPING\r\n").is_err() {
        return false;
    }
    let mut buf = [0u8; 7];
    s.read_exact(&mut buf).is_ok() && &buf == b"+PONG\r\n"
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rudis-rdbcompat-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn start_valkey(port: u16, dir: &Path, extra: &[&str]) -> Server {
    let bin = valkey_bin().expect("valkey-server");
    let log = std::fs::File::create(dir.join("valkey.log")).unwrap();
    let mut cmd = Command::new(bin);
    cmd.args([
        "--port",
        &port.to_string(),
        "--bind",
        "127.0.0.1",
        "--dir",
        dir.to_str().unwrap(),
        "--dbfilename",
        "dump.rdb",
        "--save",
        "",
        "--appendonly",
        "no",
        "--enable-debug-command",
        "yes",
        "--daemonize",
        "no",
    ]);
    cmd.args(extra);
    let child = cmd
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    Server {
        child,
        port,
        dir: dir.to_path_buf(),
        name: "valkey",
    }
    .wait_ready()
}

fn start_rudis(port: u16, dir: &Path, threads: usize) -> Server {
    let log = std::fs::File::create(dir.join("rudis.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_rudis"))
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--threads",
            &threads.to_string(),
            "--no-pin",
            "--aof-dir",
            dir.to_str().unwrap(),
        ])
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    Server {
        child,
        port,
        dir: dir.to_path_buf(),
        name: "rudis",
    }
    .wait_ready()
}

// ---------------------------------------------------------------------------
// Data set and comparison
// ---------------------------------------------------------------------------

fn lzf_friendly() -> Vec<u8> {
    b"abcdefgh".iter().cycle().take(3000).copied().collect()
}

/// Fills a server with every type, in several encodings.
fn populate(c: &mut Client) {
    c.ok(&["SET", "s:plain", "hello world"]);
    c.ok(&["SET", "s:int", "12345"]);
    c.ok(&["SET", "s:neg", "-42"]);
    c.ok(&["SET", "s:i8", "-128"]);
    c.ok(&["SET", "s:i32", "2147483647"]);
    c.ok(&["SET", "s:big", "9223372036854775807"]);
    c.ok(&["SET", "s:lead0", "007"]);
    c.ok(&["SET", "s:float", "3.14"]);
    c.ok(&["SET", "s:empty", ""]);
    c.ok(&[&b"SET"[..], b"s:lzf", &lzf_friendly()]);
    let bin: Vec<u8> = (0..=255u8).collect();
    c.ok(&[&b"SET"[..], b"s:bin", &bin]);
    c.ok(&["SET", "s:ttl", "soon", "PX", "600000"]);
    c.ok(&["SET", "12345", "integer key"]);

    c.ok(&["RPUSH", "l:small", "a", "b", "c", "1", "-2", "300000"]);
    let big: Vec<String> = (0..2000)
        .map(|i| format!("item-{i}-{}", "x".repeat(i % 50)))
        .collect();
    let mut args = vec!["RPUSH".to_string(), "l:big".into()];
    args.extend(big);
    c.ok(&args);
    c.ok(&["RPUSH", "l:ttl", "x"]);
    c.ok(&["PEXPIRE", "l:ttl", "600000"]);

    c.ok(&[
        "SADD",
        "set:int",
        "1",
        "2",
        "3",
        "-5",
        "100000",
        "-9000000000",
    ]);
    c.ok(&["SADD", "set:small", "a", "b", "c", "1"]);
    let mut args = vec!["SADD".to_string(), "set:big".into()];
    args.extend((0..600).map(|i| format!("member:{i}")));
    c.ok(&args);
    let mut args = vec!["SADD".to_string(), "set:bigint".into()];
    args.extend((0..700).map(|i| (i * 3 - 1000).to_string()));
    c.ok(&args);

    c.ok(&[
        "ZADD", "z:small", "1", "a", "2.5", "b", "-inf", "c", "inf", "d", "-0.125", "e",
    ]);
    let mut args = vec!["ZADD".to_string(), "z:big".into()];
    for i in 0..400 {
        args.push(format!("{}", i as f64 / 7.0));
        args.push(format!("zm{i}"));
    }
    c.ok(&args);

    c.ok(&["HSET", "h:small", "f1", "v1", "f2", "2", "f3", ""]);
    let mut args = vec!["HSET".to_string(), "h:big".into()];
    for i in 0..300 {
        args.push(format!("field{i}"));
        args.push(format!("value{i}"));
    }
    c.ok(&args);
    let long = "L".repeat(200);
    c.ok(&["HSET", "h:longval", "f", &long]);

    c.ok(&["PFADD", "hll:sparse", "a", "b", "c"]);
    let mut args = vec!["PFADD".to_string(), "hll:dense".into()];
    args.extend((0..5000).map(|i| format!("e{i}")));
    c.ok(&args);

    // Streams: varying fields, deletions, groups, PEL, consumers.
    for i in 1..=250 {
        let id = format!("{}-{}", 1_000_000 + i / 3, i % 3);
        if i % 10 == 0 {
            c.ok(&["XADD", "x:s", &id, "other", &i.to_string()]);
        } else {
            c.ok(&[
                "XADD",
                "x:s",
                &id,
                "name",
                &format!("n{i}"),
                "n",
                &(i * 7).to_string(),
            ]);
        }
    }
    c.ok(&["XDEL", "x:s", "1000001-0", "1000010-1"]);
    c.ok(&["XGROUP", "CREATE", "x:s", "g1", "0"]);
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g1",
        "alice",
        "COUNT",
        "5",
        "STREAMS",
        "x:s",
        ">",
    ]);
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g1",
        "bob",
        "COUNT",
        "3",
        "STREAMS",
        "x:s",
        ">",
    ]);
    c.ok(&["XACK", "x:s", "g1", "1000002-0"]);
    c.ok(&["XGROUP", "CREATE", "x:s", "g2", "$"]);
    c.ok(&["XGROUP", "CREATECONSUMER", "x:s", "g2", "carol"]);
    c.ok(&["XADD", "x:empty", "5-5", "f", "v"]);
    c.ok(&["XDEL", "x:empty", "5-5"]);
}

/// Everything observable about a key, rendered for comparison.
fn describe(c: &mut Client, key: &[u8]) -> String {
    let t = c.cmd(&[&b"TYPE"[..], key]).text();
    let mut d = format!("type={t};");
    let pttl = c.cmd(&[&b"PTTL"[..], key]).int();
    d += if pttl > 0 { "ttl;" } else { "persist;" };
    match t.as_str() {
        "string" => {
            let v = c.cmd(&[&b"GET"[..], key]).bytes();
            if v.starts_with(b"HYLL") {
                // PFCOUNT may refresh the cached cardinality in the string.
                d += &format!("hll={}", c.cmd(&[&b"PFCOUNT"[..], key]).int());
            } else {
                d += &format!("{:?}", String::from_utf8_lossy(&v));
            }
        }
        "list" => d += &c.cmd(&[&b"LRANGE"[..], key, b"0", b"-1"]).render(),
        "set" => {
            let mut m: Vec<String> = c
                .cmd(&[&b"SMEMBERS"[..], key])
                .array()
                .iter()
                .map(Reply::render)
                .collect();
            m.sort();
            d += &m.join(",");
        }
        "zset" => {
            // Scores compared as doubles: servers may print the same double
            // with different digit counts.
            let items = c
                .cmd(&[&b"ZRANGE"[..], key, b"0", b"-1", b"WITHSCORES"])
                .array();
            for p in items.chunks(2) {
                let score: f64 = p[1].text().parse().unwrap();
                d += &format!("{}={score:?},", p[0].render());
            }
        }
        "hash" => {
            let mut m: Vec<String> = c
                .cmd(&[&b"HGETALL"[..], key])
                .array()
                .chunks(2)
                .map(|p| format!("{}={}", p[0].render(), p[1].render()))
                .collect();
            m.sort();
            d += &m.join(",");
        }
        "stream" => {
            d += &c.cmd(&[&b"XRANGE"[..], key, b"-", b"+"]).render();
            let info = c.cmd(&[&b"XINFO"[..], b"STREAM", key]).map();
            for f in [
                "length",
                "last-generated-id",
                "max-deleted-entry-id",
                "entries-added",
                "groups",
            ] {
                d += &format!(
                    ";{f}={}",
                    info.get(f).map(Reply::render).unwrap_or_default()
                );
            }
            let mut groups = c.cmd(&[&b"XINFO"[..], b"GROUPS", key]).array();
            groups.sort_by_key(|g| g.map().get("name").map(Reply::text));
            for g in groups {
                let g = g.map();
                let name = g["name"].bytes();
                for f in [
                    "name",
                    "consumers",
                    "pending",
                    "last-delivered-id",
                    "entries-read",
                    "lag",
                ] {
                    d += &format!(";{f}={}", g.get(f).map(Reply::render).unwrap_or_default());
                }
                let pending = c.cmd(&[&b"XPENDING"[..], key, &name, b"-", b"+", b"1000"]);
                for p in pending.array() {
                    let p = p.array();
                    d += &format!(";pel={},{},{}", p[0].text(), p[1].text(), p[3].int());
                }
                let mut cons = c.cmd(&[&b"XINFO"[..], b"CONSUMERS", key, &name]).array();
                cons.sort_by_key(|x| x.map().get("name").map(Reply::text));
                for x in cons {
                    let x = x.map();
                    d += &format!(";consumer={},{}", x["name"].text(), x["pending"].int());
                }
            }
        }
        other => panic!("unexpected type {other}"),
    }
    d
}

fn snapshot(c: &mut Client) -> BTreeMap<Vec<u8>, String> {
    let keys = c.c("KEYS *").array();
    keys.iter()
        .map(|k| {
            let k = k.bytes();
            let d = describe(c, &k);
            (k, d)
        })
        .collect()
}

fn assert_same(expected: &BTreeMap<Vec<u8>, String>, got: &BTreeMap<Vec<u8>, String>) {
    let ek: Vec<_> = expected
        .keys()
        .map(|k| String::from_utf8_lossy(k).into_owned())
        .collect();
    let gk: Vec<_> = got
        .keys()
        .map(|k| String::from_utf8_lossy(k).into_owned())
        .collect();
    assert_eq!(ek, gk, "key sets differ");
    for (k, e) in expected {
        let g = got.get(k).cloned().unwrap_or_default();
        if *e != g {
            let ea: Vec<&str> = e.split(',').collect();
            let ga: Vec<&str> = g.split(',').collect();
            let i = ea
                .iter()
                .zip(ga.iter())
                .position(|(a, b)| a != b)
                .unwrap_or(0);
            panic!(
                "key {:?} differs at item {i}: expected {:?}, got {:?}",
                String::from_utf8_lossy(k),
                &ea[i.saturating_sub(2)..(i + 3).min(ea.len())],
                &ga[i.saturating_sub(2)..(i + 3).min(ga.len())]
            );
        }
    }
}

fn save(c: &mut Client) {
    assert_eq!(c.c("SAVE"), Reply::Status("OK".into()));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// (a) Valkey writes an RDB with every type; Rudis loads it.
#[test]
fn valkey_rdb_loads_in_rudis() {
    require_valkey!();
    for threads in [1, 4] {
        let dir = scratch_dir(&format!("v2r-{threads}"));
        let valkey = start_valkey(18110, &dir, &[]);
        let mut v = valkey.client();
        // Small node limits to get multi-node quicklists, plain nodes and
        // every compact encoding.
        v.ok(&["CONFIG", "SET", "list-max-listpack-size", "4"]);
        v.ok(&["DEBUG", "QUICKLIST-PACKED-THRESHOLD", "100"]);
        populate(&mut v);
        let plain = "P".repeat(500);
        v.ok(&["RPUSH", "l:plain", "small", &plain, "tail"]);
        v.ok(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=rdblib\nredis.register_function('rdbfn', function(keys, args) return 'from-rdb' end)",
        ]);
        assert_eq!(v.c("OBJECT ENCODING set:int").text(), "intset");
        assert_eq!(v.c("OBJECT ENCODING h:small").text(), "listpack");
        assert_eq!(v.c("OBJECT ENCODING z:small").text(), "listpack");
        assert_eq!(v.c("OBJECT ENCODING set:small").text(), "listpack");
        let expected = snapshot(&mut v);
        save(&mut v);
        drop(v);
        valkey.kill();

        let rudis = start_rudis(18111, &dir, threads);
        let mut r = rudis.client();
        let got = snapshot(&mut r);
        assert_same(&expected, &got);
        assert_eq!(r.c("FCALL rdbfn 0").text(), "from-rdb");
        assert_eq!(r.c("GET s:int").text(), "12345");
        assert_eq!(r.c("INCR s:int").int(), 12346);
    }
}

/// (b) DUMP payloads cross over in both directions, every type.
#[test]
fn dump_restore_both_directions() {
    require_valkey!();
    let vdir = scratch_dir("dump-v");
    let rdir = scratch_dir("dump-r");
    let valkey = start_valkey(18120, &vdir, &[]);
    let rudis = start_rudis(18121, &rdir, 2);
    let mut v = valkey.client();
    let mut r = rudis.client();
    v.ok(&["CONFIG", "SET", "list-max-listpack-size", "4"]);
    populate(&mut v);

    // Valkey -> Rudis.
    let expected = snapshot(&mut v);
    for k in expected.keys() {
        let payload = v.cmd(&[&b"DUMP"[..], k]).bytes();
        let pttl = v.cmd(&[&b"PTTL"[..], k]).int().max(0).to_string();
        let res = r.cmd(&[&b"RESTORE"[..], k, pttl.as_bytes(), &payload]);
        assert_eq!(
            res,
            Reply::Status("OK".into()),
            "RESTORE {:?}",
            String::from_utf8_lossy(k)
        );
    }
    assert_same(&expected, &snapshot(&mut r));

    // Rudis -> Valkey (into a flushed Valkey).
    v.ok(&["FLUSHALL"]);
    for k in expected.keys() {
        let payload = r.cmd(&[&b"DUMP"[..], k]).bytes();
        let pttl = r.cmd(&[&b"PTTL"[..], k]).int().max(0).to_string();
        let res = v.cmd(&[&b"RESTORE"[..], k, pttl.as_bytes(), &payload]);
        assert_eq!(
            res,
            Reply::Status("OK".into()),
            "RESTORE {:?}",
            String::from_utf8_lossy(k)
        );
    }
    assert_same(&expected, &snapshot(&mut v));

    // Corrupt payloads are refused by both.
    let mut bad = r.c("DUMP s:plain").bytes();
    let n = bad.len();
    bad[n - 1] ^= 0xFF;
    assert!(matches!(
        r.cmd(&[&b"RESTORE"[..], b"bad", b"0", &bad]),
        Reply::Error(_)
    ));
    assert!(matches!(
        v.cmd(&[&b"RESTORE"[..], b"bad", b"0", &bad]),
        Reply::Error(_)
    ));
}

/// (c) Rudis SAVE output loads in Valkey with identical contents.
#[test]
fn rudis_rdb_loads_in_valkey() {
    require_valkey!();
    for threads in [1, 4] {
        let dir = scratch_dir(&format!("r2v-{threads}"));
        let rudis = start_rudis(18130, &dir, threads);
        let mut r = rudis.client();
        populate(&mut r);
        r.ok(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=rudislib\nredis.register_function('rudisfn', function(keys, args) return 'from-rudis' end)",
        ]);
        let expected = snapshot(&mut r);
        save(&mut r);
        drop(r);
        rudis.kill();

        let valkey = start_valkey(18131, &dir, &[]);
        let mut v = valkey.client();
        assert_same(&expected, &snapshot(&mut v));
        assert_eq!(v.c("FCALL rudisfn 0").text(), "from-rudis");
        // Valkey writes it back; Rudis reads Valkey's re-save.
        save(&mut v);
        drop(v);
        valkey.kill();
        let rudis = start_rudis(18130, &dir, threads);
        assert_same(&expected, &snapshot(&mut rudis.client()));
    }
}

/// (d) A file in the legacy Rudis encoding still loads.
#[test]
fn legacy_rudis_rdb_still_loads() {
    use rudis::table::{RudisTable, RudisValue};
    let dir = scratch_dir("legacy");
    let mut file = b"REDIS0011\xFE\x00".to_vec();
    let mut record = |key: &[u8], val: RudisValue, expire_ms: Option<u64>| {
        if let Some(ms) = expire_ms {
            file.push(0xFC);
            file.extend_from_slice(&ms.to_le_bytes());
        }
        file.extend_from_slice(&(key.len() as u32).to_le_bytes());
        file.extend_from_slice(key);
        RudisTable::serialize_val_payload(&val, &mut file);
    };
    record(
        b"legacy:str",
        RudisValue::String(rudis::compact::CompactStr::new(b"old value")),
        None,
    );
    record(b"legacy:int", RudisValue::Int(77), None);
    let in_an_hour = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 3_600_000;
    record(
        b"legacy:list",
        RudisValue::List(Box::new(
            ["a", "b"].iter().map(|s| bytes::Bytes::from(*s)).collect(),
        )),
        Some(in_an_hour),
    );
    record(
        b"legacy:hash",
        RudisValue::SmallHash(Box::new(vec![(
            bytes::Bytes::from_static(b"f"),
            bytes::Bytes::from_static(b"v"),
        )])),
        None,
    );
    let mut regs = Box::new([0u8; 16384]);
    regs[1] = 1;
    record(b"legacy:hll", RudisValue::HyperLogLog(regs), None);
    file.push(0xFF);
    let crc = rudis::table::crc64(&file);
    file.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(dir.join("dump.rdb"), &file).unwrap();

    let rudis = start_rudis(18140, &dir, 2);
    let mut r = rudis.client();
    assert_eq!(r.c("GET legacy:str").text(), "old value");
    assert_eq!(r.c("GET legacy:int").text(), "77");
    assert_eq!(r.c("LRANGE legacy:list 0 -1").render(), r#"["a","b"]"#);
    assert!(r.c("PTTL legacy:list").int() > 3_500_000);
    assert_eq!(r.c("HGET legacy:hash f").text(), "v");
    assert_eq!(r.c("PFCOUNT legacy:hll").int(), 1);
    // The next save is in the Redis encoding and loads again.
    save(&mut r);
    let saved = std::fs::read(dir.join("dump.rdb")).unwrap();
    assert_eq!(saved[9], 0xFA, "SAVE writes AUX fields after the header");
    drop(r);
    rudis.kill();
    let rudis = start_rudis(18140, &dir, 3);
    let mut r = rudis.client();
    assert_eq!(r.c("GET legacy:str").text(), "old value");
    assert_eq!(r.c("HGET legacy:hash f").text(), "v");
    assert_eq!(r.c("PFCOUNT legacy:hll").int(), 1);
    assert_eq!(r.c("DBSIZE").int(), 5);
}

/// Rudis-only state survives a Redis-format save: hash field TTLs and
/// stream IDMP settings ride in `rudis-ext` AUX fields, which Valkey skips.
#[test]
fn rudis_extensions_survive_and_valkey_skips_them() {
    let dir = scratch_dir("ext");
    let rudis = start_rudis(18150, &dir, 2);
    let mut r = rudis.client();
    r.ok(&["HSET", "hx", "a", "1", "b", "2"]);
    r.ok(&["HPEXPIRE", "hx", "600000", "FIELDS", "1", "a"]);
    r.ok(&["XADD", "xs", "1-1", "f", "v"]);
    r.ok(&["XCFGSET", "xs", "IDMP-DURATION", "100"]);
    let ttl_before = r.c("HPTTL hx FIELDS 1 a").array()[0].int();
    assert!(ttl_before > 0);
    let xinfo = r.c("XINFO STREAM xs").render();
    save(&mut r);
    drop(r);
    rudis.kill();

    let rudis = start_rudis(18150, &dir, 2);
    let mut r = rudis.client();
    let ttl = r.c("HPTTL hx FIELDS 1 a").array()[0].int();
    assert!(ttl > 0 && ttl <= ttl_before, "{ttl} vs {ttl_before}");
    assert_eq!(r.c("HPTTL hx FIELDS 1 b").array()[0].int(), -1);
    assert_eq!(r.c("XINFO STREAM xs").render(), xinfo);
    drop(r);
    rudis.kill();

    if valkey_bin().is_some() {
        let valkey = start_valkey(18151, &dir, &[]);
        let mut v = valkey.client();
        assert_eq!(v.c("HGET hx a").text(), "1");
        assert_eq!(v.c("XLEN xs").int(), 1);
    }
}

/// Rudis refuses (instead of silently emptying) data it cannot represent.
#[test]
fn unsupported_rdb_contents_fail_loudly() {
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("module", {
            let mut b = b"REDIS0011\xFE\x00\x07\x01k".to_vec();
            b.extend_from_slice(&[0x81, 0, 0, 0, 0, 0, 0, 0, 1]);
            b
        }),
        (
            "db 1",
            b"REDIS0011\xFA\x09redis-ver\x057.2.4\xFE\x01\x00\x01k\x01v".to_vec(),
        ),
        ("version", b"REDIS0099\xFF".to_vec()),
    ];
    for (name, mut body) in cases {
        if !body.ends_with(b"\xFF") {
            body.push(0xFF);
        }
        let crc = rudis::table::crc64(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        let mut db = rudis::shard::ShardDb::new(0);
        let err = rudis::table::load_rdb_bytes(&body, &mut db, 0, 1).unwrap_err();
        eprintln!("{name}: {err}");
        match name {
            "module" => assert!(err.to_string().contains("module"), "{err}"),
            "db 1" => assert!(err.to_string().contains("database 1"), "{err}"),
            _ => assert!(err.to_string().contains("version"), "{err}"),
        }
    }
}

/// Hand-built RDB v6 file with the pre-Redis-7 encodings (zipmap,
/// ziplists, quicklist of ziplists, intset, ascii zset scores, seconds
/// expiry) loads with the right contents.
#[test]
fn old_encodings_load() {
    fn put_len(out: &mut Vec<u8>, n: usize) {
        if n < 64 {
            out.push(n as u8);
        } else {
            out.push(0x80);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        }
    }
    fn put_str(out: &mut Vec<u8>, s: &[u8]) {
        put_len(out, s.len());
        out.extend_from_slice(s);
    }
    fn ziplist(items: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        let mut prev = 0usize;
        for it in items {
            let start = body.len();
            body.push(prev as u8);
            if let Ok(v) = std::str::from_utf8(it).unwrap().parse::<i16>() {
                body.push(0xC0);
                body.extend_from_slice(&v.to_le_bytes());
            } else {
                body.push(it.len() as u8);
                body.extend_from_slice(it);
            }
            prev = body.len() - start;
        }
        let mut zl = ((10 + body.len() + 1) as u32).to_le_bytes().to_vec();
        zl.extend_from_slice(&0u32.to_le_bytes());
        zl.extend_from_slice(&(items.len() as u16).to_le_bytes());
        zl.extend_from_slice(&body);
        zl.push(0xFF);
        zl
    }
    let mut f = b"REDIS0006".to_vec();
    f.extend_from_slice(&[0xFE, 0x00]);
    // 9 HASH_ZIPMAP
    f.push(9);
    put_str(&mut f, b"zm");
    put_str(&mut f, &[1, 1, b'a', 1, 0, b'1', 0xFF]);
    // 10 LIST_ZIPLIST
    f.push(10);
    put_str(&mut f, b"lz");
    put_str(&mut f, &ziplist(&[b"x", b"12", b"y"]));
    // 11 SET_INTSET with a seconds expiry far in the future
    f.push(0xFD);
    f.extend_from_slice(&2_000_000_000i32.to_le_bytes());
    f.push(11);
    put_str(&mut f, b"si");
    let mut is = 2u32.to_le_bytes().to_vec();
    is.extend_from_slice(&2u32.to_le_bytes());
    is.extend_from_slice(&(-3i16).to_le_bytes());
    is.extend_from_slice(&7i16.to_le_bytes());
    put_str(&mut f, &is);
    // 12 ZSET_ZIPLIST
    f.push(12);
    put_str(&mut f, b"zz");
    put_str(&mut f, &ziplist(&[b"m1", b"5", b"m2", b"1.5"]));
    // 13 HASH_ZIPLIST
    f.push(13);
    put_str(&mut f, b"hz");
    put_str(&mut f, &ziplist(&[b"f", b"v", b"n", b"9"]));
    // 14 LIST_QUICKLIST (two ziplist nodes)
    f.push(14);
    put_str(&mut f, b"lq");
    put_len(&mut f, 2);
    put_str(&mut f, &ziplist(&[b"q1", b"q2"]));
    put_str(&mut f, &ziplist(&[b"q3"]));
    // 3 ZSET with ascii doubles
    f.push(3);
    put_str(&mut f, b"za");
    put_len(&mut f, 2);
    put_str(&mut f, b"p");
    f.extend_from_slice(b"\x04-2.5");
    put_str(&mut f, b"q");
    f.push(254);
    // A key that expired long ago (ms) is skipped.
    f.push(0xFC);
    f.extend_from_slice(&1000u64.to_le_bytes());
    f.push(0);
    put_str(&mut f, b"gone");
    put_str(&mut f, b"x");
    f.push(0xFF);
    let crc = rudis::table::crc64(&f);
    f.extend_from_slice(&crc.to_le_bytes());

    let dir = scratch_dir("old");
    std::fs::write(dir.join("dump.rdb"), &f).unwrap();
    let rudis = start_rudis(18160, &dir, 2);
    let mut r = rudis.client();
    assert_eq!(r.c("DBSIZE").int(), 7);
    assert_eq!(r.c("HGETALL zm").render(), r#"["a","1"]"#);
    assert_eq!(r.c("LRANGE lz 0 -1").render(), r#"["x","12","y"]"#);
    let mut si: Vec<String> = r.c("SMEMBERS si").array().iter().map(Reply::text).collect();
    si.sort();
    assert_eq!(si, vec!["-3", "7"]);
    assert!(r.c("TTL si").int() > 0);
    assert_eq!(
        r.c("ZRANGE zz 0 -1 WITHSCORES").render(),
        r#"["m2","1.5","m1","5"]"#
    );
    assert_eq!(r.c("HGET hz n").text(), "9");
    assert_eq!(r.c("LRANGE lq 0 -1").render(), r#"["q1","q2","q3"]"#);
    assert_eq!(
        r.c("ZRANGE za 0 -1 WITHSCORES").render(),
        r#"["p","-2.5","q","inf"]"#
    );
    assert_eq!(r.c("EXISTS gone").int(), 0);
}

fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Full sync in both directions: Rudis replicating from Valkey, and Valkey
/// replicating from Rudis.
#[test]
fn full_sync_with_valkey_both_directions() {
    require_valkey!();
    // Valkey master -> Rudis replica.
    let vdir = scratch_dir("repl-vm");
    let rdir = scratch_dir("repl-rr");
    let valkey = start_valkey(18170, &vdir, &["--repl-diskless-sync", "no"]);
    let mut v = valkey.client();
    populate(&mut v);
    let expected = snapshot(&mut v);
    let rudis = start_rudis(18171, &rdir, 2);
    let mut r = rudis.client();
    r.ok(&["REPLICAOF", "127.0.0.1", "18170"]);
    wait_for("Rudis to sync from Valkey", || {
        r.c("DBSIZE").int() == expected.len() as i64
    });
    assert_same(&expected, &snapshot(&mut r));
    r.ok(&["REPLICAOF", "NO", "ONE"]);
    drop(v);
    valkey.kill();

    // Rudis master -> Valkey replica.
    let vdir = scratch_dir("repl-vr");
    let valkey = start_valkey(18172, &vdir, &["--replicaof", "127.0.0.1 18171"]);
    let mut v = valkey.client();
    wait_for("Valkey to sync from Rudis", || {
        v.c("INFO replication")
            .text()
            .contains("master_link_status:up")
            && v.c("DBSIZE").int() == expected.len() as i64
    });
    assert_same(&expected, &snapshot(&mut v));
}

/// After a broken link, a replica resumes with a partial resync in both
/// directions; a PSYNC offset off by one would skip or repeat a byte.
#[test]
fn partial_resync_with_valkey_both_directions() {
    require_valkey!();
    let info_field = |c: &mut Client, section: &str, field: &str| -> String {
        let text = c.c(&format!("INFO {section}")).text();
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("{field} missing in {text}"))
            .trim()
            .to_string()
    };
    for (rudis_is_master, mport, rport) in [(false, 18173u16, 18174u16), (true, 18175, 18176)] {
        let mdir = scratch_dir(&format!("psync-m{mport}"));
        let rdir = scratch_dir(&format!("psync-r{rport}"));
        let (master, replica) = if rudis_is_master {
            let m = start_rudis(mport, &mdir, 2);
            let r = start_valkey(rport, &rdir, &[]);
            (m, r)
        } else {
            let m = start_valkey(mport, &mdir, &["--repl-diskless-sync", "no"]);
            let r = start_rudis(rport, &rdir, 2);
            (m, r)
        };
        let mut m = master.client();
        let mut r = replica.client();
        populate(&mut m);
        r.ok(&["REPLICAOF", "127.0.0.1", &mport.to_string()]);
        wait_for("initial sync", || {
            info_field(&mut r, "replication", "master_link_status") == "up"
                && info_field(&mut r, "replication", "slave_repl_offset")
                    == info_field(&mut m, "replication", "master_repl_offset")
        });
        let partial_before = info_field(&mut m, "stats", "sync_partial_ok");
        assert!(m.c("CLIENT KILL TYPE replica").int() >= 1);
        for i in 0..200 {
            m.ok(&["SET", &format!("after:{i}"), &format!("v{i}")]);
            m.c("INCR psync:ctr");
        }
        wait_for("partial resync", || {
            info_field(&mut m, "stats", "sync_partial_ok") != partial_before
                && info_field(&mut r, "replication", "master_link_status") == "up"
                && info_field(&mut r, "replication", "slave_repl_offset")
                    == info_field(&mut m, "replication", "master_repl_offset")
        });
        assert_eq!(info_field(&mut m, "stats", "sync_full"), "1");
        assert_same(&snapshot(&mut m), &snapshot(&mut r));
        drop(m);
        drop(r);
        master.kill();
        replica.kill();
    }
}

/// A server asked to SHUTDOWN after loading a Valkey file exits cleanly.
#[test]
fn rudis_shutdown_after_valkey_load() {
    require_valkey!();
    let dir = scratch_dir("shutdown");
    let valkey = start_valkey(18180, &dir, &[]);
    let mut v = valkey.client();
    v.ok(&["SET", "k", "v"]);
    save(&mut v);
    drop(v);
    valkey.kill();
    let rudis = start_rudis(18181, &dir, 1);
    let mut r = rudis.client();
    assert_eq!(r.c("GET k").text(), "v");
    let _ = r.cmd(&["SHUTDOWN", "SAVE"]);
    assert!(rudis.exit_status().is_some());
    // The file Rudis wrote on the way out loads in Valkey.
    let valkey = start_valkey(18180, &dir, &[]);
    assert_eq!(valkey.client().c("GET k").text(), "v");
}

/// A Valkey replica of a Rudis master follows consumer-group traffic:
/// Rudis propagates XREADGROUP, XCLAIM and XAUTOCLAIM as Redis does (XCLAIM
/// ... FORCE JUSTID LASTID, XGROUP SETID/CREATECONSUMER) and approximate
/// trims as exact ones, so Valkey ends with the same entries and PEL.
#[test]
fn valkey_replica_follows_rudis_stream_groups() {
    require_valkey!();
    let (mport, rport) = (18390u16, 18391u16);
    let mdir = scratch_dir("stream-repl-m");
    let rdir = scratch_dir("stream-repl-r");
    let master = start_rudis(mport, &mdir, 2);
    let replica = start_valkey(rport, &rdir, &[]);
    let mut m = master.client();
    let mut v = replica.client();
    let field = |c: &mut Client, section: &str, name: &str| -> String {
        let text = c.c(&format!("INFO {section}")).text();
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("{name} missing in {text}"))
            .trim()
            .to_string()
    };
    v.ok(&["REPLICAOF", "127.0.0.1", &mport.to_string()]);
    wait_for("Valkey to sync from Rudis", || {
        field(&mut v, "replication", "master_link_status") == "up"
    });

    for i in 1..=250 {
        m.ok(&["XADD", "s", &format!("{i}-0"), "f", &i.to_string()]);
    }
    m.ok(&["XGROUP", "CREATE", "s", "g", "0"]);
    m.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "COUNT",
        "5",
        "STREAMS",
        "s",
        ">",
    ]);
    m.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "bob",
        "NOACK",
        "COUNT",
        "3",
        "STREAMS",
        "s",
        ">",
    ]);
    m.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "COUNT",
        "2",
        "STREAMS",
        "s",
        "0",
    ]);
    m.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "carol",
        "COUNT",
        "4",
        "STREAMS",
        "s",
        ">",
    ]);
    m.ok(&["XCLAIM", "s", "g", "dave", "0", "1-0", "2-0"]);
    m.ok(&[
        "XCLAIM", "s", "g", "dave", "0", "3-0", "JUSTID", "LASTID", "20-0",
    ]);
    m.ok(&[
        "XCLAIM",
        "s",
        "g",
        "erin",
        "0",
        "30-0",
        "FORCE",
        "RETRYCOUNT",
        "4",
    ]);
    m.ok(&["XDEL", "s", "10-0"]);
    m.ok(&["XAUTOCLAIM", "s", "g", "frank", "0", "0-0", "COUNT", "100"]);
    m.ok(&["XACK", "s", "g", "1-0"]);
    m.ok(&["XADD", "s", "MAXLEN", "~", "100", "*", "f", "v"]);
    m.ok(&["XTRIM", "s", "MINID", "~", "180-0"]);
    m.ok(&["XGROUP", "CREATECONSUMER", "s", "g", "gina"]);
    m.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "hank",
        "COUNT",
        "2",
        "STREAMS",
        "s",
        ">",
    ]);
    m.ok(&["SET", "done", "1"]);

    wait_for("Valkey to apply the stream traffic", || {
        field(&mut v, "replication", "slave_repl_offset")
            == field(&mut m, "replication", "master_repl_offset")
            && v.c("EXISTS done").int() == 1
    });
    assert_eq!(m.c("XLEN s"), v.c("XLEN s"));
    assert_eq!(m.c("XRANGE s - +").render(), v.c("XRANGE s - +").render());
    // XPENDING rows: id, owner, delivery count, and an idle time showing
    // the replica has the master's delivery time.
    let pending = |c: &mut Client| -> Vec<(String, String, i64, i64)> {
        c.c("XPENDING s g - + 1000")
            .array()
            .iter()
            .map(|row| {
                let row = row.array();
                (row[0].text(), row[1].text(), row[2].int(), row[3].int())
            })
            .collect()
    };
    let (pm, pv) = (pending(&mut m), pending(&mut v));
    assert_eq!(pm.len(), pv.len(), "{pm:?} vs {pv:?}");
    assert!(!pm.is_empty());
    for (a, b) in pm.iter().zip(&pv) {
        assert_eq!((&a.0, &a.1, a.3), (&b.0, &b.1, b.3), "{pm:?} vs {pv:?}");
        assert!(
            (a.2 - b.2).abs() < 2_000,
            "delivery time differs: {a:?} vs {b:?}"
        );
    }
    let group = |c: &mut Client| {
        let g = c.c("XINFO GROUPS s").array()[0].map();
        [
            "name",
            "consumers",
            "pending",
            "last-delivered-id",
            "entries-read",
        ]
        .map(|k| g[k].render())
    };
    assert_eq!(group(&mut m), group(&mut v));
    let consumers = |c: &mut Client| -> Vec<(String, i64)> {
        let mut out: Vec<(String, i64)> = c
            .c("XINFO CONSUMERS s g")
            .array()
            .iter()
            .map(|x| {
                let x = x.map();
                (x["name"].text(), x["pending"].int())
            })
            .collect();
        out.sort();
        out
    };
    assert_eq!(consumers(&mut m), consumers(&mut v));
}
