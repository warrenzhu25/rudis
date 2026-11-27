---
paths:
  - "src/**/*.rs"
  - "docs/differences.md"
---

# Redis Compatibility

Clients, tools and Redis's own TCL suites are the users. They see error strings,
reply shapes, log lines, INFO fields and config names, so all of these are API.

## Rules

- **Match Redis exactly where it's observable**: error text (`-ERR Unknown
  client type 'x'`), reply type and shape in RESP2 and RESP3, INFO field names
  and units, the log lines tests grep for ("Replica generated a reply to
  command '…'"), and command names in logs (`slowlog|get`, lowercase, with
  the subcommand).
- **Cite the source of the behavior** in a comment or the commit message:
  the Redis function (`getClientType`, `afterErrorReply`), config default, or
  TCL test that fixes it. Never infer Redis behavior from memory when the test
  file or the Redis source is at hand.
- **Config names are Redis's first.** When rudis already has the feature
  under another name (Valkey's, or its own), add Redis's name as an alias
  rather than renaming. Defaults follow Redis unless there's a documented
  reason (`repl-diskless-sync-delay` is 0, not 5).
- **Every deliberate difference goes in `docs/differences.md`**, in the same
  commit, with what rudis does instead, the reason, and any consequence for
  operators (a cost per GB, a stall, a missing field).
- **Parse what Redis parses, reject what Redis rejects.** Unknown options get
  Redis's error (`syntax error`), not silence. `CLIENT LIST TYPE` was parsed
  and ignored for months.
- **New commands and subcommands get their COMMAND/ACL entries.** Add them
  to `scripts/gen_acl_categories.py` / `gen_command_docs.py`, regenerate the
  tables, and check `COMMAND INFO` (key specs: `NO_KEYS` for keyless
  commands, never a made-up `numkeys` spec).
- **`CHANGELOG.md` `[Unreleased]`** gets an entry for every user-visible
  change: Added / Changed / Fixed, with the visible symptom for fixes.
