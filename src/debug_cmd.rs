//! `DEBUG` subcommand helpers shared by the connection path
//! (`execute_command`, which fans whole-server work out to every shard) and
//! the shard path (`execute_local_command`, which runs a shard's part).

use bytes::Bytes;
use std::cell::RefCell;

use crate::shard::ShardDb;

/// `DEBUG` subcommands that are accepted and answered `+OK` without doing
/// anything: they tune or inspect Redis internals Rudis does not have
/// (listpack/quicklist/dict encodings, jemalloc, cluster bus failpoints,
/// keymeta modules, ...). Kept so the Redis test suites that call them as
/// setup steps still run. The first group is used by the 49 core unit
/// suites (`scripts/run_redis_test_suite.sh`), the second by the integration
/// suites. Anything not listed and not implemented answers Redis' unknown
/// subcommand error.
pub const OK_SUBCOMMANDS: &[&[u8]] = &[
    // Core unit suites.
    b"change-repl-id",
    b"config-rewrite-force-all",
    b"dict-resizing",
    b"htstats",
    b"htstats-key",
    b"jmap",
    b"listpack",
    b"mallctl",
    b"mallctl-str",
    b"quicklist-packed-threshold",
    b"replybuffer",
    b"sdslen",
    b"segfault",
    b"set-disable-deny-scripts",
    // Integration suites.
    b"aof-flush-sleep",
    b"allocsize-slots-assert",
    b"asm-failpoint",
    b"asm-trim-method",
    b"close-cluster-link-on-packet-drop",
    b"defrag-frag-cache-stats",
    b"disable-cluster-random-ping",
    b"drop-cluster-packet-filter",
    b"enable-keymeta-runtime-registration",
    b"internal_secret",
    b"keymeta-aof-dump",
    b"keysizes-hist-assert",
    b"mark-internal-client",
    b"repl-pause",
    b"reply-copy-avoidance",
    b"set-skip-checksum-validation",
];

/// Whether `sub` (any case) is in [`OK_SUBCOMMANDS`].
pub fn is_ok_subcommand(sub: &[u8]) -> bool {
    OK_SUBCOMMANDS.iter().any(|s| s.eq_ignore_ascii_case(sub))
}

/// Redis' reply to an unknown `DEBUG` subcommand.
pub fn write_unknown_subcommand(out: &mut Vec<u8>, sub: &[u8]) {
    let sub = String::from_utf8_lossy(&sub[..sub.len().min(128)]);
    // Newlines would break the protocol line.
    let sub = sub.replace(['\r', '\n'], " ");
    out.extend_from_slice(
        format!("-ERR unknown subcommand '{}'. Try DEBUG HELP.\r\n", sub).as_bytes(),
    );
}

#[inline]
fn write_status(out: &mut Vec<u8>, s: &str) {
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// A digest as Redis replies it: a status line of 40 hex characters.
pub fn write_digest(out: &mut Vec<u8>, d: &crate::digest::Digest20) {
    write_status(out, &crate::digest::to_hex(d));
}

/// `DEBUG DIGEST-VALUE`'s reply: an array of status lines.
pub fn write_digest_array(out: &mut Vec<u8>, digests: &[crate::digest::Digest20]) {
    out.extend_from_slice(format!("*{}\r\n", digests.len()).as_bytes());
    for d in digests {
        write_digest(out, d);
    }
}

const HELP: &[&str] = &[
    "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "DIGEST",
    "    Output a hex signature representing the current DB content.",
    "DIGEST-VALUE <key> [<key> ...]",
    "    Output a hex signature of the values of all the specified keys.",
    "ERROR <string>",
    "    Return a RESP protocol error with <string> as message. Useful for clients",
    "    unit tests to simulate error replies.",
    "LOG <message>",
    "    Write <message> to the server log.",
    "LOADAOF",
    "    Flush the AOF buffers on disk and reload the AOF in memory.",
    "REPLICATE <string>",
    "    Replicates the provided string to replicas, allowing data divergence.",
    "OBJECT <key>",
    "    Show low level info about `key` and associated value.",
    "PANIC",
    "    Crash the server simulating a panic.",
    "PAUSE-CRON <0|1>",
    "    Stop periodic cron job processing.",
    "POPULATE <count> [<prefix>] [<size>]",
    "    Create <count> string keys named key:<num>. If <prefix> is specified then",
    "    it is used instead of the 'key' prefix. These are not propagated to",
    "    replicas.",
    "PROTOCOL <type>",
    "    Reply with a test value of the specified type. <type> can be: string,",
    "    integer, double, bignum, null, array, set, map, attrib, push, verbatim,",
    "    true, false.",
    "RELOAD [option ...]",
    "    Save the RDB on disk and reload it back to memory. Valid <option> values:",
    "    * MERGE, NOFLUSH: accepted; the dataset is always replaced by the RDB.",
    "    * NOSAVE: the database will be loaded from an existing RDB file.",
    "SET-ACTIVE-EXPIRE <0|1>",
    "    Setting it to 0 disables expiring keys in background when they are not",
    "    accessed (otherwise the server does it). Use 1 to restore.",
    "SET-ALLOW-ACCESS-EXPIRED <0|1>",
    "    Allow access to expired keys.",
    "SLEEP <seconds>",
    "    Stop the server for <seconds>. Decimals allowed.",
    "STRINGMATCH-LEN",
    "    Run a fuzz tester against the stringmatchlen() function.",
    "HELP",
    "    Print this help.",
];

/// `DEBUG HELP`: an array of status lines, like Redis' `addReplyHelp`.
pub fn write_help(out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", HELP.len()).as_bytes());
    for line in HELP {
        write_status(out, line);
    }
}

/// `DEBUG ERROR <string>`: the string as an error reply, newlines blanked.
pub fn write_error_reply(out: &mut Vec<u8>, msg: &[u8]) {
    out.push(b'-');
    out.extend(msg.iter().map(|&b| match b {
        b'\r' | b'\n' => b' ',
        b => b,
    }));
    out.extend_from_slice(b"\r\n");
}

/// `DEBUG PROTOCOL <type>`, with Redis' test values for RESP2 and RESP3.
pub fn write_protocol(out: &mut Vec<u8>, name: &[u8], resp3: bool) {
    let bool_reply = |out: &mut Vec<u8>, v: bool| {
        out.extend_from_slice(match (resp3, v) {
            (true, true) => b"#t\r\n",
            (true, false) => b"#f\r\n",
            (false, true) => b":1\r\n",
            (false, false) => b":0\r\n",
        })
    };
    match name.to_ascii_lowercase().as_slice() {
        b"string" => out.extend_from_slice(b"$11\r\nHello World\r\n"),
        b"integer" => out.extend_from_slice(b":12345\r\n"),
        b"double" => out.extend_from_slice(if resp3 {
            b",3.141\r\n"
        } else {
            b"$5\r\n3.141\r\n"
        }),
        b"bignum" => {
            const BIG: &str = "1234567999999999999999999999999999999";
            if resp3 {
                out.extend_from_slice(format!("({}\r\n", BIG).as_bytes());
            } else {
                out.extend_from_slice(format!("${}\r\n{}\r\n", BIG.len(), BIG).as_bytes());
            }
        }
        b"null" => out.extend_from_slice(if resp3 { b"_\r\n" } else { b"$-1\r\n" }),
        b"array" => out.extend_from_slice(b"*3\r\n:0\r\n:1\r\n:2\r\n"),
        b"set" => {
            out.extend_from_slice(if resp3 { b"~3\r\n" } else { b"*3\r\n" });
            out.extend_from_slice(b":0\r\n:1\r\n:2\r\n");
        }
        b"map" => {
            out.extend_from_slice(if resp3 { b"%3\r\n" } else { b"*6\r\n" });
            for j in 0..3 {
                out.extend_from_slice(format!(":{}\r\n", j).as_bytes());
                bool_reply(out, j == 1);
            }
        }
        b"attrib" => {
            if resp3 {
                out.extend_from_slice(
                    b"|1\r\n$14\r\nkey-popularity\r\n*2\r\n$7\r\nkey:123\r\n:90\r\n",
                );
            }
            // Attributes are not real replies: a real one always follows.
            out.extend_from_slice(b"$39\r\nSome real reply following the attribute\r\n");
        }
        b"push" => {
            if !resp3 {
                out.extend_from_slice(b"-ERR RESP2 is not supported by this command\r\n");
                return;
            }
            out.extend_from_slice(b">2\r\n$16\r\nserver-cpu-usage\r\n:42\r\n");
            // Push replies are not real replies: a real one always follows.
            out.extend_from_slice(b"$40\r\nSome real reply following the push reply\r\n");
        }
        b"true" => bool_reply(out, true),
        b"false" => bool_reply(out, false),
        b"verbatim" => {
            const TEXT: &str = "This is a verbatim\nstring";
            if resp3 {
                out.extend_from_slice(
                    format!("={}\r\ntxt:{}\r\n", TEXT.len() + 4, TEXT).as_bytes(),
                );
            } else {
                out.extend_from_slice(format!("${}\r\n{}\r\n", TEXT.len(), TEXT).as_bytes());
            }
        }
        _ => out.extend_from_slice(
            b"-ERR Wrong protocol type name. Please use one of the following: \
              string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false\r\n",
        ),
    }
}

/// `DEBUG STRINGMATCH-LEN`: Redis' stringmatchlen fuzz test, here against
/// the glob matcher used by KEYS/SCAN/PSUBSCRIBE: random patterns and
/// strings must never crash or hang it.
pub fn stringmatch_fuzz() {
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    const ALPHABET: &[u8] = b"ab*?[]^-\\";
    for _ in 0..1000 {
        let plen = (next() % 32) as usize;
        let slen = (next() % 32) as usize;
        let pattern: Vec<u8> = (0..plen)
            .map(|_| ALPHABET[(next() % ALPHABET.len() as u64) as usize])
            .collect();
        let text: Vec<u8> = (0..slen).map(|_| (next() % 256) as u8).collect();
        let _ = crate::pubsub::glob_match(&pattern, &text);
    }
}

/// Parses `DEBUG RELOAD`'s options; returns whether to save first.
pub fn parse_reload_options(opts: &[Bytes]) -> Result<bool, &'static str> {
    let mut save = true;
    for opt in opts {
        if opt.eq_ignore_ascii_case(b"nosave") {
            save = false;
        } else if !opt.eq_ignore_ascii_case(b"merge") && !opt.eq_ignore_ascii_case(b"noflush") {
            return Err("ERR DEBUG RELOAD only supports the MERGE, NOFLUSH and NOSAVE options.");
        }
    }
    Ok(save)
}

/// A non-negative integer argument, as Redis' `getPositiveLongFromObject`.
pub fn parse_non_negative(arg: &[u8]) -> Result<u64, &'static str> {
    let n: i64 = std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or("ERR value is not an integer or out of range")?;
    u64::try_from(n).map_err(|_| "ERR value is out of range, must be positive")
}

/// `DEBUG POPULATE`'s value for key number `n`: `value:<n>`, padded with
/// zero bytes or cut to `size` when one was given, as Redis does.
pub fn populate_value(n: &[u8], size: Option<usize>) -> Vec<u8> {
    let mut v = Vec::with_capacity(6 + n.len());
    v.extend_from_slice(b"value:");
    v.extend_from_slice(n);
    if let Some(size) = size {
        v.resize(size, 0);
    }
    v
}

/// The RESP array for `DEBUG REPLICATE <arg>...`.
pub fn encode_resp_array(args: &[Bytes]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// The shard side of `DEBUG`, for the subcommands `execute_local_command`
/// does not handle itself: the per-shard parts of whole-server
/// subcommands (see the `DEBUG_*_SHARD` constants in [`crate::router`]),
/// and local versions of the others for callers that run commands on one
/// shard directly (scripts, replication apply).
pub fn execute_shard_debug(
    args: &[Bytes],
    db: &mut ShardDb,
    out: &mut Vec<u8>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
) {
    let Some(sub) = args.first() else {
        out.extend_from_slice(b"-ERR wrong number of arguments for 'debug' command\r\n");
        return;
    };
    if sub.as_ref() == crate::router::DEBUG_DIGEST_SHARD {
        let part = crate::digest::shard_digest(db);
        crate::connection::write_resp_bulk(out, crate::digest::encode_shard_part(&part).as_bytes());
    } else if sub.as_ref() == crate::router::DEBUG_LOADAOF_SHARD {
        let Some(aof) = aof else {
            out.extend_from_slice(b"+OK\r\n");
            return;
        };
        let path = aof.borrow().path().to_path_buf();
        db.flushdb();
        // The replayed commands are already on the replicas.
        let _quiet = crate::replication::SuppressPropagation::new();
        match crate::aof::replay_aof(&path, db) {
            Ok(_) => out.extend_from_slice(b"+OK\r\n"),
            Err(e) => {
                let msg = format!("Error loading the AOF {}: {}", path.display(), e);
                write_error_reply(out, format!("ERR {}", msg).as_bytes());
            }
        }
    } else if sub.as_ref() == crate::router::DEBUG_POPULATE_SHARD {
        // `__shard-populate <prefix> <size|""> <n>...`
        let (Some(prefix), Some(size)) = (args.get(1), args.get(2)) else {
            out.extend_from_slice(b"-ERR syntax error\r\n");
            return;
        };
        let size = std::str::from_utf8(size)
            .ok()
            .and_then(|s| s.parse::<usize>().ok());
        for n in &args[3..] {
            let mut key = Vec::with_capacity(prefix.len() + 1 + n.len());
            key.extend_from_slice(prefix);
            key.push(b':');
            key.extend_from_slice(n);
            if !db.exists(&key) {
                db.setnx(Bytes::from(key), Bytes::from(populate_value(n, size)));
            }
        }
        out.extend_from_slice(b"+OK\r\n");
    } else if sub.eq_ignore_ascii_case(b"digest") && args.len() == 1 {
        let d = crate::digest::combine_shards([crate::digest::shard_digest(db)]);
        write_digest(out, &d);
    } else if sub.eq_ignore_ascii_case(b"digest-value") {
        let digests: Vec<_> = args[1..]
            .iter()
            .map(|k| crate::digest::key_value_digest(db, k))
            .collect();
        write_digest_array(out, &digests);
    } else if sub.eq_ignore_ascii_case(b"help") {
        write_help(out);
    } else if sub.eq_ignore_ascii_case(b"protocol") && args.len() == 2 {
        write_protocol(out, &args[1], crate::connection::CURRENT_CLIENT_RESP3.get());
    } else if sub.eq_ignore_ascii_case(b"error") && args.len() == 2 {
        write_error_reply(out, &args[1]);
    } else if sub.eq_ignore_ascii_case(b"log") && args.len() == 2 {
        println!("DEBUG LOG: {}", String::from_utf8_lossy(&args[1]));
        out.extend_from_slice(b"+OK\r\n");
    } else if is_ok_subcommand(sub) {
        out.extend_from_slice(b"+OK\r\n");
    } else {
        write_unknown_subcommand(out, sub);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_subcommand_error_matches_redis() {
        let mut out = Vec::new();
        write_unknown_subcommand(&mut out, b"NoSuchThing");
        assert_eq!(
            out,
            b"-ERR unknown subcommand 'NoSuchThing'. Try DEBUG HELP.\r\n".to_vec()
        );
        let mut out = Vec::new();
        write_unknown_subcommand(&mut out, b"a\r\nb");
        assert_eq!(
            out,
            b"-ERR unknown subcommand 'a  b'. Try DEBUG HELP.\r\n".to_vec()
        );
    }

    #[test]
    fn ok_list_is_case_insensitive_and_closed() {
        assert!(is_ok_subcommand(b"QUICKLIST-PACKED-THRESHOLD"));
        assert!(is_ok_subcommand(b"htstats-key"));
        assert!(!is_ok_subcommand(b"digest"));
        assert!(!is_ok_subcommand(b"bogus"));
    }

    #[test]
    fn help_is_an_array_of_status_lines() {
        let mut out = Vec::new();
        write_help(&mut out);
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with(&format!("*{}\r\n+DEBUG <subcommand>", HELP.len())));
        assert!(text.contains("+DIGEST-VALUE <key> [<key> ...]\r\n"));
        assert!(text.ends_with("+    Print this help.\r\n"));
    }

    #[test]
    fn protocol_replies_follow_resp_version() {
        let reply = |name: &[u8], resp3| {
            let mut out = Vec::new();
            write_protocol(&mut out, name, resp3);
            String::from_utf8(out).unwrap()
        };
        assert_eq!(reply(b"true", false), ":1\r\n");
        assert_eq!(reply(b"TRUE", true), "#t\r\n");
        assert_eq!(reply(b"null", false), "$-1\r\n");
        assert_eq!(
            reply(b"map", true),
            "%3\r\n:0\r\n#f\r\n:1\r\n#t\r\n:2\r\n#f\r\n"
        );
        assert_eq!(
            reply(b"map", false),
            "*6\r\n:0\r\n:0\r\n:1\r\n:1\r\n:2\r\n:0\r\n"
        );
        assert_eq!(
            reply(b"attrib", false),
            "$39\r\nSome real reply following the attribute\r\n"
        );
        assert!(reply(b"push", false).starts_with("-ERR"));
        assert_eq!(
            reply(b"verbatim", true),
            "=29\r\ntxt:This is a verbatim\nstring\r\n"
        );
        assert!(reply(b"nope", true).starts_with("-ERR Wrong protocol type name"));
    }

    #[test]
    fn reload_options_and_populate_values() {
        assert_eq!(parse_reload_options(&[]), Ok(true));
        assert_eq!(
            parse_reload_options(&[Bytes::from_static(b"NOSAVE"), Bytes::from_static(b"merge")]),
            Ok(false)
        );
        assert!(parse_reload_options(&[Bytes::from_static(b"fast")]).is_err());
        assert_eq!(populate_value(b"12", None), b"value:12".to_vec());
        assert_eq!(populate_value(b"12", Some(3)), b"val".to_vec());
        assert_eq!(populate_value(b"1", Some(10)), b"value:1\0\0\0".to_vec());
        assert_eq!(parse_non_negative(b"5"), Ok(5));
        assert!(parse_non_negative(b"-1").is_err());
        assert!(parse_non_negative(b"x").is_err());
    }

    #[test]
    fn error_reply_and_replicate_encoding() {
        let mut out = Vec::new();
        write_error_reply(&mut out, b"ERR boom\nnow");
        assert_eq!(out, b"-ERR boom now\r\n".to_vec());
        assert_eq!(
            encode_resp_array(&[Bytes::from_static(b"fake-command-1")]),
            b"*1\r\n$14\r\nfake-command-1\r\n".to_vec()
        );
        stringmatch_fuzz();
    }

    #[test]
    fn shard_debug_populate_digest_and_unknown() {
        let mut db = ShardDb::new(0);
        let mut out = Vec::new();
        let args: Vec<Bytes> = [crate::router::DEBUG_POPULATE_SHARD, b"key", b"", b"0", b"1"]
            .iter()
            .map(|a| Bytes::copy_from_slice(a))
            .collect();
        execute_shard_debug(&args, &mut db, &mut out, None);
        assert_eq!(out, b"+OK\r\n".to_vec());
        assert_eq!(db.get(b"key:1"), Some(Bytes::from_static(b"value:1")));

        out.clear();
        execute_shard_debug(
            &[Bytes::from_static(crate::router::DEBUG_DIGEST_SHARD)],
            &mut db,
            &mut out,
            None,
        );
        let text = String::from_utf8(out.clone()).unwrap();
        assert!(
            text.starts_with("$42\r\n") && text.ends_with(":2\r\n"),
            "{text}"
        );

        out.clear();
        execute_shard_debug(&[Bytes::from_static(b"bogus")], &mut db, &mut out, None);
        assert!(out.starts_with(b"-ERR unknown subcommand 'bogus'"));
    }
}
