#!/usr/bin/env python3
"""Generate src/command_docs.rs: the COMMAND / COMMAND INFO / COMMAND DOCS data.

Source of truth is the Valkey command table (src/commands/*.json). Each entry
is pre-encoded as RESP3, exactly as Valkey's addReplyCommandInfo /
addReplyCommandDocs emit it, minus the subcommand list: the server appends
only the subcommands rudis implements (see src/command_info.rs) and
downgrades to RESP2 for RESP2 clients.

Entries are indexed like `acl_categories::COMMANDS` (same names, same
order). Commands that only exist in Sentinel, and the coarse rudis-only
command families in gen_acl_categories.EXTRA (e.g. `json` for JSON.*), have
no entry.

Usage:
    scripts/gen_command_docs.py [VALKEY_SRC_DIR] > src/command_docs.rs
VALKEY_SRC_DIR defaults to ~/valkey.
"""
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_acl_categories as acl  # noqa: E402

# Reply order of command flags (server.c addReplyFlagsForCommand). Flags
# not listed (MAY_REPLICATE, SENTINEL, ONLY_SENTINEL, PROTECTED,
# TOUCHES_ARBITRARY_KEYS, ALL_DBS) are hidden on purpose.
COMMAND_FLAGS = [
    ("WRITE", "write"),
    ("READONLY", "readonly"),
    ("DENYOOM", "denyoom"),
    ("MODULE", "module"),
    ("ADMIN", "admin"),
    ("PUBSUB", "pubsub"),
    ("NOSCRIPT", "noscript"),
    ("BLOCKING", "blocking"),
    ("LOADING", "loading"),
    ("STALE", "stale"),
    ("SKIP_MONITOR", "skip_monitor"),
    ("SKIP_COMMANDLOG", "skip_commandlog"),
    ("ASKING", "asking"),
    ("FAST", "fast"),
    ("NO_AUTH", "no_auth"),
    ("NO_MANDATORY_KEYS", "no_mandatory_keys"),
    ("NO_ASYNC_LOADING", "no_async_loading"),
    ("NO_MULTI", "no_multi"),
    ("MOVABLE_KEYS", "movablekeys"),
    ("ALLOW_BUSY", "allow_busy"),
]
KEY_FLAGS = [
    ("RO", "RO"),
    ("RW", "RW"),
    ("OW", "OW"),
    ("RM", "RM"),
    ("ACCESS", "access"),
    ("UPDATE", "update"),
    ("INSERT", "insert"),
    ("DELETE", "delete"),
    ("NOT_KEY", "not_key"),
    ("INCOMPLETE", "incomplete"),
    ("VARIABLE_FLAGS", "variable_flags"),
]
DOC_FLAGS = [("DEPRECATED", "deprecated"), ("SYSCMD", "syscmd")]
GROUPS = {"sorted_set": "sorted-set"}

# Rudis-only commands that are real, single commands (not the coarse
# families) get a minimal synthesized entry. Fields: arity, extra command
# flags, key spec as (first, last, step) or None, key spec flags, summary.
RUDIS_ONLY = {
    "blmovem": (-6, ["BLOCKING"], (1, 2, 1), ["RW", "ACCESS", "DELETE", "INSERT"],
                "Pops elements from a list and pushes them to another, blocking until available."),
    "lmovem": (-5, [], (1, 2, 1), ["RW", "ACCESS", "DELETE", "INSERT"],
               "Pops elements from a list and pushes them to another."),
    "delex": (-2, [], (1, 1, 1), ["RW", "DELETE"],
              "Deletes a key, optionally only if its value matches."),
    "increx": (-2, [], (1, 1, 1), ["RW", "ACCESS", "UPDATE"],
               "Increments the integer value of a key and optionally sets its expiration."),
    "digest": (2, [], (1, 1, 1), ["RO", "ACCESS"], "Returns a hash digest of a key's value."),
    "sdiffcard": (-3, [], None, ["RO", "ACCESS"],
                  "Returns the number of members of the difference of multiple sets."),
    "sunioncard": (-3, [], None, ["RO", "ACCESS"],
                   "Returns the number of members of the union of multiple sets."),
    "xackdel": (-5, [], (1, 1, 1), ["RW", "DELETE"],
                "Acknowledges and deletes stream entries."),
    "xcfgset": (-2, [], (1, 1, 1), ["RW", "UPDATE"], "Sets stream configuration."),
    "xdelex": (-4, [], (1, 1, 1), ["RW", "DELETE"], "Deletes stream entries."),
    "xidmprecord": (5, [], (1, 1, 1), ["RW", "UPDATE"],
                    "Records an idempotent producer id for a stream."),
    "xnack": (-7, [], (1, 1, 1), ["RW", "UPDATE"],
              "Negatively acknowledges stream entries."),
    "vadd": (-2, [], (1, 1, 1), ["RW", "INSERT"], "Adds an element to a vector set."),
    "vrem": (-3, [], (1, 1, 1), ["RW", "DELETE"], "Removes an element from a vector set."),
    "vsetattr": (-4, [], (1, 1, 1), ["RW", "UPDATE"],
                 "Sets the attributes of a vector set element."),
    "vcard": (2, [], (1, 1, 1), ["RO", "ACCESS"], "Returns the number of elements in a vector set."),
    "vdim": (2, [], (1, 1, 1), ["RO", "ACCESS"], "Returns the dimension of a vector set."),
    "vemb": (-3, [], (1, 1, 1), ["RO", "ACCESS"], "Returns the vector of an element."),
    "vgetattr": (3, [], (1, 1, 1), ["RO", "ACCESS"],
                 "Returns the attributes of a vector set element."),
    "vinfo": (2, [], (1, 1, 1), ["RO", "ACCESS"], "Returns information about a vector set."),
    "vismember": (3, [], (1, 1, 1), ["RO", "ACCESS"],
                  "Determines whether an element is in a vector set."),
    "vlinks": (-3, [], (1, 1, 1), ["RO", "ACCESS"],
               "Returns the HNSW neighbours of an element."),
    "vquery": (-3, [], (1, 1, 1), ["RO", "ACCESS"], "Queries a vector set."),
    "vrandmember": (-2, [], (1, 1, 1), ["RO", "ACCESS"],
                    "Returns random elements of a vector set."),
    "vsim": (-4, [], (1, 1, 1), ["RO", "ACCESS"],
             "Returns the elements most similar to a vector or element."),
}


def c_literal(s):
    """Valkey's generator pastes JSON strings into C string literals, so an
    unescaped '"' splits the literal (MIGRATE's `""` token is empty) and
    backslash escapes are decoded."""
    out, i = [], 0
    while i < len(s):
        ch = s[i]
        if ch == "\\" and i + 1 < len(s):
            out.append({"n": "\n", "t": "\t", "r": "\r"}.get(s[i + 1], s[i + 1]))
            i += 2
            continue
        if ch != '"':
            out.append(ch)
        i += 1
    return "".join(out)


def bulk(s):
    b = c_literal(s).encode() if isinstance(s, str) else s
    return b"$%d\r\n%s\r\n" % (len(b), b)


def status(s):
    return b"+" + s.encode() + b"\r\n"


def integer(n):
    return b":%d\r\n" % n


def array(n):
    return b"*%d\r\n" % n


def rmap(n):
    return b"%%%d\r\n" % n


def rset(n):
    return b"~%d\r\n" % n


def flag_set(names, table, reply=status):
    present = [out for (flag, out) in table if flag in names]
    return rset(len(present)) + b"".join(reply(p) for p in present)


def key_spec(spec):
    out = b""
    n = 3
    if spec.get("notes"):
        n += 1
        out += bulk("notes") + bulk(spec["notes"])
    out += bulk("flags") + flag_set(set(spec.get("flags", [])), KEY_FLAGS)
    bs = spec["begin_search"]
    out += bulk("begin_search")
    if bs.get("index"):
        out += rmap(2) + bulk("type") + bulk("index")
        out += bulk("spec") + rmap(1) + bulk("index") + integer(bs["index"]["pos"])
    elif bs.get("keyword"):
        kw = bs["keyword"]
        out += rmap(2) + bulk("type") + bulk("keyword")
        out += bulk("spec") + rmap(2) + bulk("keyword") + bulk(kw["keyword"])
        out += bulk("startfrom") + integer(kw["startfrom"])
    else:
        out += rmap(2) + bulk("type") + bulk("unknown") + bulk("spec") + rmap(0)
    fk = spec["find_keys"]
    out += bulk("find_keys")
    if fk.get("range"):
        r = fk["range"]
        out += rmap(2) + bulk("type") + bulk("range") + bulk("spec") + rmap(3)
        out += bulk("lastkey") + integer(r["lastkey"])
        out += bulk("keystep") + integer(r["step"])
        out += bulk("limit") + integer(r["limit"])
    elif fk.get("keynum"):
        k = fk["keynum"]
        out += rmap(2) + bulk("type") + bulk("keynum") + bulk("spec") + rmap(3)
        out += bulk("keynumidx") + integer(k["keynumidx"])
        out += bulk("firstkey") + integer(k["firstkey"])
        out += bulk("keystep") + integer(k["step"])
    else:
        out += rmap(2) + bulk("type") + bulk("unknown") + bulk("spec") + rmap(0)
    return rmap(n) + out


def legacy_range(specs, flags):
    """(first, last, step) and the movablekeys flag, as
    server.c populateCommandLegacyRangeSpec computes them."""
    if not specs:
        return (0, 0, 0)

    def is_range(s):
        return bool(s["begin_search"].get("index")) and bool(s["find_keys"].get("range"))

    if len(specs) == 1 and is_range(specs[0]):
        s = specs[0]
        if "INCOMPLETE" in s.get("flags", []):
            flags.add("MOVABLE_KEYS")
        pos = s["begin_search"]["index"]["pos"]
        last = s["find_keys"]["range"]["lastkey"]
        return (pos, last + pos if last >= 0 else last, s["find_keys"]["range"]["step"])
    u32 = lambda v: v & 0xFFFFFFFF  # noqa: E731 (C's unsigned comparison)
    first, last, prev_last = None, 0, 0
    for s in specs:
        if not is_range(s):
            flags.add("MOVABLE_KEYS")
            continue
        pos = s["begin_search"]["index"]["pos"]
        r = s["find_keys"]["range"]
        if r["step"] != 1 or (prev_last and prev_last != pos - 1):
            flags.add("MOVABLE_KEYS")
            continue
        if "INCOMPLETE" in s.get("flags", []):
            flags.add("MOVABLE_KEYS")
        first = pos if first is None else min(first, pos)
        abs_last = r["lastkey"] + pos if r["lastkey"] >= 0 else r["lastkey"]
        last = last if u32(last) >= u32(abs_last) else abs_last
        prev_last = last
    if first is None:
        flags.add("MOVABLE_KEYS")
        return (0, 0, 0)
    rel = last if last < 0 else last - first
    return (first, rel + first if rel >= 0 else rel, 1)


def info_head(fullname, v, cats_mask):
    """The first nine COMMAND INFO fields."""
    flags = set(v.get("command_flags", []))
    first, last, step = legacy_range(v.get("key_specs", []), flags)
    out = bulk(fullname) + integer(v["arity"])
    out += flag_set(flags, COMMAND_FLAGS)
    out += integer(first) + integer(last) + integer(step)
    cats = [c for i, c in enumerate(acl.CATEGORIES) if cats_mask & (1 << i)]
    out += rset(len(cats)) + b"".join(status("@" + c) for c in cats)
    tips = v.get("command_tips", [])
    out += rset(len(tips)) + b"".join(bulk(t.lower()) for t in tips)
    specs = v.get("key_specs", [])
    out += rset(len(specs)) + b"".join(key_spec(s) for s in specs)
    return out


def arg_list(args):
    out = array(len(args))
    for a in args:
        typ = a["type"]
        nested = typ in ("oneof", "block")
        fields = [(b"name", bulk(a["name"].lower())), (b"type", bulk(typ))]
        if not nested:
            fields.append((b"display_text", bulk(a.get("display", a["name"]).lower())))
        if "key_spec_index" in a:
            fields.append((b"key_spec_index", integer(a["key_spec_index"])))
        if a.get("token"):
            fields.append((b"token", bulk(a["token"].upper())))
        if a.get("summary"):
            fields.append((b"summary", bulk(a["summary"])))
        if a.get("since"):
            fields.append((b"since", bulk(a["since"])))
        if a.get("deprecated_since"):
            fields.append((b"deprecated_since", bulk(a["deprecated_since"])))
        aflags = [f for f in ("optional", "multiple", "multiple_token") if a.get(f)]
        if aflags:
            fields.append((b"flags", rset(len(aflags)) + b"".join(status(f) for f in aflags)))
        if nested:
            fields.append((b"arguments", arg_list(a["arguments"])))
        out += rmap(len(fields)) + b"".join(bulk(k) + val for k, val in fields)
    return out


def docs_fields(v):
    """COMMAND DOCS map fields, without "subcommands"."""
    fields = []
    if v.get("summary"):
        fields.append(("summary", bulk(v["summary"])))
    if v.get("since"):
        fields.append(("since", bulk(v["since"])))
    fields.append(("group", bulk(GROUPS.get(v["group"], v["group"]))))
    if v.get("complexity"):
        fields.append(("complexity", bulk(v["complexity"])))
    if v.get("doc_flags"):
        fields.append(("doc_flags", flag_set(set(v["doc_flags"]), DOC_FLAGS)))
    if v.get("deprecated_since"):
        fields.append(("deprecated_since", bulk(v["deprecated_since"])))
    if v.get("replaced_by"):
        fields.append(("replaced_by", bulk(v["replaced_by"])))
    if v.get("history"):
        h = v["history"]
        fields.append(
            ("history", rset(len(h)) + b"".join(array(2) + bulk(s) + bulk(c) for s, c in h))
        )
    if v.get("arguments"):
        fields.append(("arguments", arg_list(v["arguments"])))
    return len(fields), b"".join(bulk(k) + val for k, val in fields)


def synthesize(name, cats):
    arity, extra_flags, rng, kflags, summary = RUDIS_ONLY[name]
    flags = list(extra_flags)
    if "write" in cats:
        flags.append("WRITE")
    elif "read" in cats:
        flags.append("READONLY")
    if "fast" in cats:
        flags.append("FAST")
    if rng is None:
        # numkeys key [key ...], like SINTERCARD.
        specs = [{"flags": kflags, "begin_search": {"index": {"pos": 1}},
                  "find_keys": {"keynum": {"keynumidx": 0, "firstkey": 1, "step": 1}}}]
    else:
        first, last, step = rng
        specs = [{"flags": kflags, "begin_search": {"index": {"pos": first}},
                  "find_keys": {"range": {"lastkey": last - first, "step": step, "limit": 0}}}]
    return {"summary": summary, "since": "rudis", "group": "generic", "arity": arity,
            "command_flags": flags, "key_specs": specs}


def rust_bytes(b):
    out = ['b"']
    for c in b:
        if c == 0x0D:
            out.append("\\r")
        elif c == 0x0A:
            out.append("\\n")
        elif c == 0x22:
            out.append('\\"')
        elif c == 0x5C:
            out.append("\\\\")
        elif 0x20 <= c < 0x7F:
            out.append(chr(c))
        else:
            out.append("\\x%02x" % c)
    out.append('"')
    return "".join(out)


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/valkey")
    descs = {}
    for fn in sorted(glob.glob(os.path.join(src, "src", "commands", "*.json"))):
        with open(fn) as f:
            for name, v in json.load(f).items():
                full = name.lower()
                if "container" in v:
                    full = v["container"].lower() + "|" + full
                descs[full] = v
    if not descs:
        sys.exit(f"no command json found under {src}/src/commands")
    names = sorted(set(descs) | set(acl.EXTRA))
    for n in RUDIS_ONLY:
        assert n in acl.EXTRA, n

    lines = []
    w = lines.append
    w("// @generated by scripts/gen_command_docs.py from the Valkey command table")
    w("// (src/commands/*.json) plus rudis-only commands. Do not edit by hand.")
    w("")
    w("/// Pre-encoded (RESP3) COMMAND data for one command.")
    w("pub struct CmdDoc {")
    w("    /// Same as `acl_categories::COMMANDS[i].name`.")
    w("    pub name: &'static str,")
    w("    /// The first nine COMMAND INFO fields (everything but subcommands).")
    w("    pub info: &'static [u8],")
    w("    /// Number of COMMAND DOCS map fields in `docs`.")
    w("    pub docs_fields: u8,")
    w("    /// COMMAND DOCS map fields, without \"subcommands\".")
    w("    pub docs: &'static [u8],")
    w("}")
    w("")
    w(f"pub static DOCS: [Option<CmdDoc>; {len(names)}] = [")
    for n in names:
        v = descs.get(n)
        if v is None:
            cats = set(acl.EXTRA[n])
            if n not in RUDIS_ONLY:
                w("    None,")
                continue
            v = synthesize(n, cats)
        else:
            cats = acl.implicit(v)[0]
            if "ONLY_SENTINEL" in v.get("command_flags", []):
                w("    None,")
                continue
        mask = 0
        for c in cats:
            mask |= 1 << acl.CATEGORIES.index(c)
        nfields, docs = docs_fields(v)
        w(
            f'    Some(CmdDoc {{ name: "{n}", info: {rust_bytes(info_head(n, v, mask))}, '
            f"docs_fields: {nfields}, docs: {rust_bytes(docs)} }}),"
        )
    w("];")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
