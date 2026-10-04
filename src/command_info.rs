//! COMMAND, COMMAND COUNT / LIST / INFO / DOCS.
//!
//! The replies come from the Valkey command table, pre-encoded as RESP3 by
//! `scripts/gen_command_docs.py` (see [`crate::command_docs`]). Only
//! commands and subcommands that rudis actually parses are listed: that is
//! decided once, by asking the command parser about every table entry.

use crate::acl_categories::{CATEGORIES, COMMANDS, NUM_COMMANDS};
use crate::command_docs::DOCS;
use crate::resp::Command;
use bytes::Bytes;
use std::sync::OnceLock;

/// Whether the parser knows `name` (`cmd` or `container|sub`). Parsing has
/// no side effects. Some parsers report a wrong argument count and an
/// unknown subcommand with the same error, so it is asked with zero to three
/// dummy arguments; any answer other than "unknown" means it is known.
fn parser_knows(name: &str) -> bool {
    let parts: Vec<Bytes> = name
        .split('|')
        .map(|p| Bytes::from(p.to_ascii_uppercase()))
        .collect();
    (0..4).any(|extra| {
        let mut args = parts.clone();
        args.extend(std::iter::repeat_n(Bytes::from_static(b"0"), extra));
        match crate::resp::build_command(args) {
            // `<container> HELP` is answered generically.
            Ok(Some(Command::Unknown(s))) => parts.len() == 2 && s.ends_with(" HELP"),
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(e) => !e.to_ascii_lowercase().contains("unknown"),
        }
    })
}

/// `listed()[i]`: `COMMANDS[i]` is shown by COMMAND and friends.
fn listed() -> &'static [bool] {
    static LISTED: OnceLock<Vec<bool>> = OnceLock::new();
    LISTED.get_or_init(|| {
        let mut v = vec![false; NUM_COMMANDS];
        // Containers sort before their subcommands, so parents are decided first.
        for i in 0..NUM_COMMANDS {
            let c = &COMMANDS[i];
            v[i] =
                DOCS[i].is_some() && c.parent.is_none_or(|p| v[p as usize]) && parser_knows(c.name);
        }
        v
    })
}

fn top_level() -> impl Iterator<Item = usize> {
    let l = listed();
    (0..NUM_COMMANDS).filter(move |&i| l[i] && COMMANDS[i].parent.is_none())
}

fn subcommands(i: usize) -> impl Iterator<Item = usize> {
    let l = listed();
    let (a, b) = COMMANDS[i].subs;
    (a as usize..b as usize).filter(move |&j| l[j])
}

fn lookup(name: &str) -> Option<usize> {
    crate::acl::command_index(name).filter(|&i| listed()[i])
}

fn bulk(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", s.len()).as_bytes());
    out.extend_from_slice(s);
    out.extend_from_slice(b"\r\n");
}

fn header(out: &mut Vec<u8>, kind: u8, n: usize) {
    out.push(kind);
    out.extend_from_slice(n.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn info3(out: &mut Vec<u8>, i: usize) {
    let Some(doc) = &DOCS[i] else { return };
    header(out, b'*', 10);
    out.extend_from_slice(doc.info);
    let subs: Vec<usize> = subcommands(i).collect();
    header(out, b'*', subs.len());
    for j in subs {
        info3(out, j);
    }
}

fn docs3(out: &mut Vec<u8>, i: usize) {
    let Some(doc) = &DOCS[i] else { return };
    let subs: Vec<usize> = subcommands(i).collect();
    header(
        out,
        b'%',
        doc.docs_fields as usize + usize::from(!subs.is_empty()),
    );
    out.extend_from_slice(doc.docs);
    if !subs.is_empty() {
        bulk(out, b"subcommands");
        header(out, b'%', subs.len());
        for j in subs {
            bulk(out, COMMANDS[j].name.as_bytes());
            docs3(out, j);
        }
    }
}

/// Rewrites a RESP3 reply for a RESP2 client: maps become flat arrays,
/// sets become arrays and nulls become null bulk strings.
fn resp3_to_resp2(src: &[u8], out: &mut Vec<u8>) {
    let mut i = 0;
    while i < src.len() {
        let Some(len) = src[i..].windows(2).position(|w| w == b"\r\n") else {
            out.extend_from_slice(&src[i..]);
            return;
        };
        let line = &src[i..i + len + 2];
        let num = || {
            std::str::from_utf8(&line[1..len])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0)
        };
        i += len + 2;
        match line[0] {
            b'%' => header(out, b'*', 2 * num()),
            b'~' => header(out, b'*', num()),
            b'_' => out.extend_from_slice(b"$-1\r\n"),
            b'$' => {
                out.extend_from_slice(line);
                let end = (i + num() + 2).min(src.len());
                out.extend_from_slice(&src[i..end]);
                i = end;
            }
            _ => out.extend_from_slice(line),
        }
    }
}

fn emit(out: &mut Vec<u8>, resp3: bool, reply: &[u8]) {
    if resp3 {
        out.extend_from_slice(reply);
    } else {
        resp3_to_resp2(reply, out);
    }
}

/// Builds a reply once per protocol and keeps it.
fn cached(
    cell: &'static OnceLock<[Vec<u8>; 2]>,
    resp3: bool,
    build: fn() -> Vec<u8>,
) -> &'static [u8] {
    let both = cell.get_or_init(|| {
        let r3 = build();
        let mut r2 = Vec::with_capacity(r3.len());
        resp3_to_resp2(&r3, &mut r2);
        [r2, r3]
    });
    &both[usize::from(resp3)]
}

fn all_info() -> Vec<u8> {
    let top: Vec<usize> = top_level().collect();
    let mut out = Vec::new();
    header(&mut out, b'*', top.len());
    for i in top {
        info3(&mut out, i);
    }
    out
}

fn all_docs() -> Vec<u8> {
    let top: Vec<usize> = top_level().collect();
    let mut out = Vec::new();
    header(&mut out, b'%', top.len());
    for i in top {
        bulk(&mut out, COMMANDS[i].name.as_bytes());
        docs3(&mut out, i);
    }
    out
}

/// COMMAND COUNT.
pub fn write_count(out: &mut Vec<u8>) {
    out.extend_from_slice(format!(":{}\r\n", top_level().count()).as_bytes());
}

/// COMMAND (no `names`) and COMMAND INFO [name ...].
pub fn write_info(out: &mut Vec<u8>, names: &[String], resp3: bool) {
    static ALL: OnceLock<[Vec<u8>; 2]> = OnceLock::new();
    if names.is_empty() {
        out.extend_from_slice(cached(&ALL, resp3, all_info));
        return;
    }
    let mut reply = Vec::new();
    header(&mut reply, b'*', names.len());
    for name in names {
        match lookup(name) {
            Some(i) => info3(&mut reply, i),
            None => reply.extend_from_slice(b"_\r\n"),
        }
    }
    emit(out, resp3, &reply);
}

/// COMMAND DOCS [name ...]. Unknown names are skipped.
pub fn write_docs(out: &mut Vec<u8>, names: &[String], resp3: bool) {
    static ALL: OnceLock<[Vec<u8>; 2]> = OnceLock::new();
    if names.is_empty() {
        out.extend_from_slice(cached(&ALL, resp3, all_docs));
        return;
    }
    let found: Vec<usize> = names.iter().filter_map(|n| lookup(n)).collect();
    let mut reply = Vec::new();
    header(&mut reply, b'%', found.len());
    for i in found {
        bulk(&mut reply, COMMANDS[i].name.as_bytes());
        docs3(&mut reply, i);
    }
    emit(out, resp3, &reply);
}

/// COMMAND LIST [FILTERBY MODULE name | ACLCAT category | PATTERN pattern].
/// `filter` is `(type, value)` with the type upper case.
pub fn write_list(out: &mut Vec<u8>, filter: Option<(&str, &str)>) {
    let l = listed();
    let keep: Box<dyn Fn(usize) -> bool> = match filter {
        None => Box::new(|_| true),
        Some(("ACLCAT", cat)) => {
            match CATEGORIES.iter().position(|c| c.eq_ignore_ascii_case(cat)) {
                Some(bit) => Box::new(move |i| COMMANDS[i].cats & (1 << bit) != 0),
                None => Box::new(|_| false),
            }
        }
        Some(("PATTERN", pat)) => {
            let pat = pat.to_ascii_lowercase();
            Box::new(move |i| {
                crate::pubsub::glob_match(pat.as_bytes(), COMMANDS[i].name.as_bytes())
            })
        }
        // No modules.
        Some(_) => Box::new(|_| false),
    };
    let names: Vec<&str> = (0..NUM_COMMANDS)
        .filter(|&i| l[i] && keep(i))
        .map(|i| COMMANDS[i].name)
        .collect();
    header(out, b'*', names.len());
    for n in names {
        bulk(out, n.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name_at(i: usize) -> &'static str {
        COMMANDS[i].name
    }

    #[test]
    fn docs_table_matches_acl_table() {
        assert_eq!(DOCS.len(), NUM_COMMANDS);
        for (i, d) in DOCS.iter().enumerate() {
            if let Some(d) = d {
                assert_eq!(d.name, name_at(i));
            }
        }
    }

    #[test]
    fn lists_implemented_commands_only() {
        let l = listed();
        let is = |n: &str| crate::acl::command_index(n).is_some_and(|i| l[i]);
        for n in [
            "get",
            "set",
            "client",
            "client|list",
            "config|get",
            "command|docs",
        ] {
            assert!(is(n), "{n} should be listed");
        }
        // Sentinel-only and coarse rudis families have no entry.
        for n in ["sentinel", "json", "bf", "agent"] {
            assert!(!is(n), "{n} should not be listed");
        }
        let count = top_level().count();
        assert!(count > 200, "only {count} commands listed");
    }

    #[test]
    fn resp2_downgrade() {
        let mut out = Vec::new();
        resp3_to_resp2(
            b"%1\r\n$1\r\na\r\n~2\r\n+x\r\n:1\r\n_\r\n$4\r\n%~\r\n\r\n",
            &mut out,
        );
        assert_eq!(
            out,
            b"*2\r\n$1\r\na\r\n*2\r\n+x\r\n:1\r\n$-1\r\n$4\r\n%~\r\n\r\n".to_vec()
        );
    }

    #[test]
    fn get_info_matches_valkey() {
        let mut out = Vec::new();
        write_info(&mut out, &["get".to_string()], false);
        let expected =
            b"*1\r\n*10\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
*3\r\n+@read\r\n+@string\r\n+@fast\r\n*0\r\n*1\r\n*6\r\n$5\r\nflags\r\n*2\r\n+RO\r\n+access\r\n\
$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n\
$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n\
$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n";
        assert_eq!(
            String::from_utf8_lossy(&out),
            String::from_utf8_lossy(expected)
        );
    }
}
