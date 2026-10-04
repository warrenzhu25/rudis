# Component 15: Security, Memory Allocator & TLS (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/acl.rs` (443 lines), `src/allocator.rs` (344 lines), `src/tls.rs` (346 lines)
> **High-Level Design Spec**: [`docs/design/15_security_tls.md`](../design/15_security_tls.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)
>
> This revision re-verifies every claim against current source. The previously-documented
> `requirepass` enforcement gap is **now fixed**. The old kTLS plaintext-bypass note is
> **obsolete**: kTLS is not implemented and its partial scaffolding (`enable_ktls`,
> `is_ktls_active`, kTLS read/write branches) has been removed (§5.3). TLS clients now run the
> same generic client loop as plaintext clients (§5.4). Several
> new findings are documented below (§6) that were not present in the prior pass.

---

## 1. Source Module Map & Responsibilities

| File | Role | Key Items |
| :--- | :--- | :--- |
| `src/acl.rs` | Per-port ACL registry, password hashing, `AUTH`/`ACL *` logic | `AclUser`, `AclManager`, `PORT_ACLS`, `HAS_CUSTOM_ACL`, `hash_password*` |
| `src/allocator.rs` | jemalloc telemetry for `INFO`, plus an unrelated small-collection object-pool arena | `AllocatorStats`, `get_allocator_stats`, `format_memory_info`, `SmallCollectionArena` |
| `src/tls.rs` | rustls-backed TLS (userspace only — no kTLS), self-signed cert generation, `TlsSession` handshake, `TlsTransport` (the TLS `ClientTransport` impl) | `TlsSession`, `TlsTransport`, `generate_self_signed_cert`, `create_server_config` |
| `src/transport.rs` | Transport trait the generic client loop is monomorphized over; plaintext impl; out-of-band push target | `ClientTransport`, `PlainTransport`, `PushTarget` |

`src/allocator.rs` is really two unrelated modules sharing a file: lines 1–89 are jemalloc
telemetry; lines 91–344 are `SmallCollectionArena`, a per-shard free-list pool for `List`/`Hash`/
`Set`/`ZSet` collections used by `src/table.rs` (Component 05) — it has nothing to do with the
system allocator and is not mentioned in the module's own doc comment grouping, but it lives in
this file and is covered in §4.

---

## 2. Component Architecture & Data Flow

```
AUTH user pass  /  HELLO ... AUTH user pass  /  ACL SETUSER|GETUSER|LIST|USERS|DELUSER|WHOAMI|CAT
                                   │
     PORT_ACLS: Mutex<HashMap<u16 port, Arc<RwLock<AclManager>>>>   (acl.rs:7-8, process-global)
                                   │
          get_acl_for_port(port) → creates-on-first-use, same Arc shared by every shard
          thread and by both the plain and TLS accept loops for that port (same `router.port`)
                                   │
                    AclManager { users: HashMap<String, AclUser> }
                                   │
     check_auth(username, password) -> Result<String, &'static str>   (§3.3)
     nopass ⇒ instant success; else plaintext match OR any of 4 hash-comparison forms
                                   │
     connection bootstraps `authenticated` from is_auth_required_for_default() (§3.4);
     on every subsequent command: NOAUTH gate, then (if HAS_CUSTOM_ACL or non-default user)
     can_execute_command(name) && can_access_key(key) for EVERY key the command touches (§3.5)


requirepass (config file OR CONFIG SET) ──► directly primes the "default" AclUser (§3.6)
     main.rs:137-153 (startup)  and  connection.rs CONFIG SET requirepass (~6686-6703)
     both: clear passwords/hashes, push plaintext + SHA-256 hash, nopass=false,
     HAS_CUSTOM_ACL=true  — FIXED: no longer silently ignored (§6 "Status" notes)


INFO command (memory section)
                                   │
     allocator::format_memory_info(used_mem, max_mem, cooled_keys, tiered_keys)
                                   │
     allocator::get_allocator_stats()  →  tikv_jemalloc_ctl::stats::{allocated,active,
     resident,metadata,mapped}  (real reads against the process's global jemalloc allocator)


--tls-port listener (server.rs, spawned per shard, parallel SO_REUSEPORT socket)
                                   │
     TlsSession::new(rustls ServerConfig) → handshake_monoio()  (10 s handshake timeout)
     (genuine async rustls handshake loop driven over monoio AsyncReadRent/AsyncWriteRentExt)
                                   │
     no kTLS: userspace rustls only (enable_ktls / is_ktls_active removed, §5.3)
                                   │
     TlsTransport → handle_client<TlsTransport>(): the SAME generic client loop as
     handle_connection() → handle_client<PlainTransport>() (squashing, MULTI/EXEC, Pub/Sub,
     CLIENT KILL/LIST, limits, stats); MONITOR/tracking pushes arrive via PushTarget::Queue
     and are encrypted by the owning connection (§5.4)
```

---

## 3. ACL System — Exact Structures & Algorithms (`src/acl.rs`)

### 3.1 Module-level statics (acl.rs:1-15)

```rust
pub static HAS_CUSTOM_ACL: AtomicBool = AtomicBool::new(false);

pub static PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn get_acl_for_port(port: u16) -> Arc<RwLock<AclManager>> {
    let mut map = PORT_ACLS.lock().unwrap();
    map.entry(port)
        .or_insert_with(|| Arc::new(RwLock::new(AclManager::new())))
        .clone()
}
```

`HAS_CUSTOM_ACL` is a single process-wide flag (not per-port) — it is set `true` the first time
*any* port's `AclManager::set_user`/`del_user` runs, or the first time `requirepass` is primed
(main.rs or `CONFIG SET requirepass`). It is read as a fast-path short-circuit before taking the
per-port `RwLock` on the hot command-execution path (§3.5) — once true, it stays true for the
life of the process (never reset to `false`).

`PORT_ACLS` is keyed by listening **port number**, not by "plain vs. TLS". Since the TLS accept
loop on a shard reuses the *same* `Router` (and therefore the same `router.port`, the shard's
plain port — see server.rs:228-240) as the plain accept loop, `get_acl_for_port(router.port)`
resolves to the **identical** `AclManager` for TLS and plain connections on that shard. There is
no separate "TLS ACL" — `requirepass`/ACL rules protect both listeners uniformly.

### 3.2 Password hashing — three functions, one dead branch (acl.rs:17-58)

```rust
pub fn hash_password(password: &str) -> String {               // legacy
    // SHA1("rudis_acl_salt_v1:" + password), formatted "#<40-hex>"
}

/// Standard Redis SHA-256 hash (#<64-hex>)
pub fn hash_password_sha256(password: &str) -> String {
    // ring::digest::digest(SHA256, password.as_bytes()), formatted "#<64-hex>"
    // UNSALTED — matches real Redis's ACL password-hash format exactly.
}

/// Modern per-user salted SHA-256 password hash
pub fn hash_password_salted(username: &str, password: &str) -> String {
    // ring::digest::digest(SHA256, "<username>:<password>"), formatted "#<64-hex>"
}
```

Three distinct schemes exist:
1. **`hash_password`** — legacy SHA1 (160-bit / 40 hex chars) with one hardcoded global salt
   string `"rudis_acl_salt_v1:"` shared by every user and every Rudis instance. Still computed
   and still checked in `check_auth` for backward compatibility, and still one of the two hashes
   `set_user`'s `>password` rule writes into `password_hashes` (acl.rs:304-307).
2. **`hash_password_sha256`** — **this is the real Redis-compatible scheme**: unsalted
   `SHA256(password)`, 256-bit / 64 hex chars, `#`-prefixed. Re-verified: Redis's actual ACL
   password hash is unsalted SHA-256 for exactly this reason (so `ACL GETUSER`'s hash output is
   portable/comparable across installs), and this implementation matches it precisely. This is
   the hash `set_user`'s `>password` rule writes first (acl.rs:300-303) and the one `main.rs`'s
   `requirepass` priming and `CONFIG SET requirepass` both use.
3. **`hash_password_salted`** — a third, per-user-salted SHA-256 variant
   (`SHA256(username + ":" + password)`). **This function is dead on the write side**: it is
   `grep`-confirmed to have exactly two references in the whole codebase — its own definition
   (acl.rs:45) and a single call site inside `check_auth` (acl.rs:216) that computes it on every
   auth attempt and compares it against stored hashes. Nothing anywhere (`set_user`, `main.rs`,
   `CONFIG SET requirepass`) ever *stores* a hash in this format, so the comparison can never
   succeed — it is pure wasted work on every `AUTH`/`HELLO AUTH`/pipeline auth check.

### 3.3 `AclUser` / `AclManager` — exact struct layout (acl.rs:60-199)

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    pub passwords: Vec<String>,                           // plaintext, still populated
    pub password_hashes: Vec<String>,                      // "#<hex>" entries, multiple schemes mixed
    pub nopass: bool,
    pub all_commands: bool,
    pub allowed_commands: hashbrown::HashSet<String>,       // used only when all_commands == false
    pub disallowed_commands: hashbrown::HashSet<String>,    // used only when all_commands == true
    pub all_keys: bool,
    pub allowed_key_patterns: Vec<String>,                  // used only when all_keys == false
}

pub struct AclManager {
    pub users: HashMap<String, AclUser>,
}
```

`AclUser::new_default()` (acl.rs:75-88) seeds the `"default"` user with `enabled: true,
nopass: true, all_commands: true, all_keys: true` and all collections empty — a fully
unrestricted user. `AclManager::new()` (acl.rs:195-199) inserts exactly this one `"default"`
entry; it is the *only* user that exists until an `ACL SETUSER` call (or `requirepass` priming,
which mutates this same default user in place) adds more.

`set_user`'s fallback-insert path for a brand-new non-default username (acl.rs:271-282) seeds
the opposite defaults: `enabled: false, nopass: false, all_commands: false, all_keys: false` —
i.e. a freshly-created user via `ACL SETUSER newuser <rules>` starts fully locked down
(disabled, no commands, no keys) until the rule tokens in the same call grant something.

### 3.4 `check_auth` — exact matching algorithm (acl.rs:201-233)

```rust
pub fn check_auth(&self, username: Option<&str>, password: &str) -> Result<String, &'static str> {
    let user_name = username.unwrap_or("default");
    if let Some(user) = self.users.get(user_name) {
        if !user.enabled { return Err("WRONGPASS User is disabled"); }
        if user.nopass { return Ok(user_name.to_string()); }          // short-circuit, no hashing at all
        let legacy_sha1 = hash_password(password);
        let sha256 = hash_password_sha256(password);
        let salted = hash_password_salted(user_name, password);       // computed but never matches (§3.2)
        if user.passwords.iter().any(|p| p == password)
            || user.password_hashes.iter().any(|h| {
                h == &sha256
                    || h == &salted
                    || h == &legacy_sha1
                    || (h.starts_with('#') && h[1..] == sha256[1..])  // tolerates a bare-hex stored hash
                    || h == password                                   // lets a pre-hashed `#...` ACL entry
            })                                                         // be compared verbatim against input
        {
            Ok(user_name.to_string())
        } else {
            Err("WRONGPASS invalid username-password pair or user is disabled.")
        }
    } else {
        Err("WRONGPASS invalid username-password pair or user is disabled.")
    }
}
```

The `h == password` arm is what makes Redis's real `ACL SETUSER user #<precomputed-sha256-hash>`
syntax work: the client's `AUTH` password argument is compared directly against the stored
`#<hex>` string without the server needing to know in advance whether the stored value is a
hash or literal text.

`is_auth_required_for_default` (acl.rs:235-241):

```rust
pub fn is_auth_required_for_default(&self) -> bool {
    if let Some(user) = self.users.get("default") {
        !user.nopass && (!user.passwords.is_empty() || !user.password_hashes.is_empty())
    } else {
        false
    }
}
```

### 3.5 Connection-level enforcement

**Bootstrap** (the generic `handle_client` loop in `connection.rs`, shared by plaintext and TLS
connections — one code site for both):

```rust
let mut authenticated = !crate::acl::get_acl_for_port(router.port)
    .read().unwrap().is_auth_required_for_default();
let mut auth_user = "default".to_string();
```

**Per-command gate** (`execute_command`, connection.rs:4990-5026):

```rust
if !*authenticated && !matches!(cmd, Command::Auth{..} | Command::Hello{..} | Command::Quit) {
    out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
    return false;
}

if *authenticated
    && (crate::acl::HAS_CUSTOM_ACL.load(Ordering::Relaxed) || auth_user != "default")
{
    let acl = crate::acl::get_acl_for_port(router.port);
    let acl_guard = acl.read().unwrap();
    if let Some(user) = acl_guard.get_user(auth_user) {
        if !user.can_execute_command(cmd_name) { /* -NOPERM, return false */ }
        for key in cmd_keys(&cmd) {                 // ALL keys the command touches, not just one
            if !user.can_access_key(key) { /* -NOPERM, return false */ }
        }
    }
}
```

The ACL check is skipped entirely (fast path) unless `HAS_CUSTOM_ACL` is set process-wide or the
current connection authenticated as a non-default user — this means a deployment that never
touches ACL/`requirepass` pays zero `RwLock`/hashing cost per command. `cmd_keys` (connection.rs:
3443-3448) walks `for_each_cmd_key` and collects **every** key referenced by the command (not
just a single "primary" key — this supersedes an older single-key `cmd_primary_key` helper, which
still exists at connection.rs:2778 but is now used only for routing/hashing, not for ACL).

`AclUser::can_execute_command`/`can_access_key` (acl.rs:90-121):

```rust
pub fn can_execute_command(&self, cmd_name: &str) -> bool {
    let name = cmd_name.to_lowercase();
    if matches!(name.as_str(), "ping"|"reset"|"quit"|"auth"|"hello") { return true; }  // always allowed
    if self.all_commands { !self.disallowed_commands.contains(&name) }
    else { self.allowed_commands.contains(&name) }
}

pub fn can_access_key(&self, key: &[u8]) -> bool {
    if self.all_keys { return true; }
    let key_str = String::from_utf8_lossy(key);
    for pat in &self.allowed_key_patterns {
        if pat == "*" { return true; }
        if let Some(prefix) = pat.strip_suffix('*') {
            if key_str.starts_with(prefix) { return true; }
        } else if key_str == *pat { return true; }
    }
    false
}
```

Only bare prefix-glob (`foo*`) or exact-match key patterns are real; no general glob engine
(no mid-string `*`, no `?`/`[abc]` classes as real Redis ACL supports).

**Pipeline/squash path** (`execute_commands_squashed`-style batch entry, connection.rs:19120-19166)
performs the *identical* `can_execute_command`/`for_each_cmd_key`+`can_access_key` checks per
queued command while deciding squash-eligibility; any command an ACL would deny forces
`can_squash = false` for the whole batch, and that command then falls through to the ordinary
sequential `execute_command` path where the real `-NOPERM` fires — pipelining cannot bypass ACL.

**`Command::Reset`** (connection.rs:9405-9424) recomputes `authenticated` with a **different,
narrower** formula than `is_auth_required_for_default`:

```rust
let default_requires_auth = acl.read().unwrap().get_user("default")
    .map(|u| !u.passwords.is_empty())
    .unwrap_or(false);
*authenticated = !default_requires_auth;
```

This checks only `passwords.is_empty()` — it ignores `nopass` and `password_hashes` entirely. A
default user configured with only a pre-hashed password (`ACL SETUSER default #<hash>`, leaving
`passwords` empty while `password_hashes` is non-empty) would, per `is_auth_required_for_default`,
require re-authentication after `RESET`, but this inline check instead sets `authenticated = true`
unconditionally in that case — see §6 for the implication.

### 3.6 `requirepass` integration — FIXED, no longer silent

**Startup** (`main.rs:137-153`, runs once before any shard thread is spawned):

```rust
if let Some(ref pass) = server_config.requirepass {
    let acl = rudis::acl::get_acl_for_port(port);
    let mut acl_guard = acl.write().unwrap();
    if let Some(user) = acl_guard.get_user_mut("default") {
        user.passwords.clear();
        user.password_hashes.clear();
        if !pass.is_empty() {
            user.passwords.push(pass.clone());
            let h = rudis::acl::hash_password_sha256(pass);
            user.password_hashes.push(h);
            user.nopass = false;
            rudis::acl::HAS_CUSTOM_ACL.store(true, Ordering::Release);
        } else {
            user.nopass = true;
        }
    }
}
```

**Runtime** (`CONFIG SET requirepass`, connection.rs:6686-6703) performs the byte-for-byte
identical sequence against the live `AclManager` for `router.port`. Because
`get_acl_for_port(port)` is called *before* any shard worker starts (main.rs runs this prior to
spawning shard threads at line 155+), and `PORT_ACLS` is a single process-global map, every
shard's later `get_acl_for_port(router.port)` call resolves to this same already-primed
`AclManager` — the default user's password and `HAS_CUSTOM_ACL=true` are visible to every shard
from the first connection onward. `CONFIG GET requirepass` / `CONFIG GET *` (connection.rs:6412-
6425, 6477-6483) read back `user.passwords.first()` (plaintext) for display, defaulting to `""`
when unset.

**This closes the gap the previous doc pass documented**: setting `requirepass` (config file or
`CONFIG SET`) now reliably flips `nopass = false` and `HAS_CUSTOM_ACL = true` on the default user,
so `is_auth_required_for_default()` returns `true` and every new connection (plain or TLS, same
`router.port`) starts with `authenticated = false` and must `AUTH`/`HELLO AUTH`. See §6 "Status
of previously-reported issues" for the verification trail.

### 3.7 `ACL SETUSER` rule-token parser (acl.rs:266-356)

Full token table, in parse order:

| Token | Effect |
| :--- | :--- |
| `on` / `off` | `enabled = true` / `false` |
| `nopass` | `nopass=true`, clears `passwords` and `password_hashes` |
| `-nopass` | `nopass=false` only (does not touch stored passwords) |
| `>password` | `nopass=false`; pushes plaintext to `passwords` (dedup) **and** pushes both `hash_password_sha256(p)` and `hash_password(p)` (legacy SHA1) to `password_hashes` (dedup each) |
| `#hexhash` | `nopass=false`; pushes `"#<hexhash>"` verbatim to `password_hashes` (dedup) — for pre-hashed credentials |
| `<password` | removes `password` from `passwords`; also removes its legacy-SHA1 hash from `password_hashes` (does **not** remove a matching SHA-256 hash) |
| `!hexhash` | removes `"#<hexhash>"` (or bare `hexhash`) from `password_hashes` |
| `+@all` / `+all` | `all_commands=true`, clears `disallowed_commands` |
| `-@all` / `-all` | `all_commands=false`, clears `allowed_commands` |
| `+cmd` | if `all_commands`: remove `cmd` from `disallowed_commands`; else: add to `allowed_commands` |
| `-cmd` | if `all_commands`: add `cmd` to `disallowed_commands`; else: remove from `allowed_commands` |
| `~*` / `allkeys` | `all_keys=true`, clears `allowed_key_patterns` |
| `resetkeys` | `all_keys=false`, clears `allowed_key_patterns` |
| `~pattern` | `all_keys=false`; pushes `pattern` to `allowed_key_patterns` (dedup) |
| anything else (e.g. `+@read`, `-@write`, `&channel:*`) | **silently accepted and ignored** — no error returned, no state change |

So `ACL SETUSER bob on >pw -@all +get ~user:*` genuinely produces a user restricted to `GET`
(plus the always-allowed `PING`/`RESET`/`QUIT`/`AUTH`/`HELLO`) against `user:*`-prefixed keys —
verified by `test_acl_command_and_key_enforcement` (acl.rs:417-442). But `+@read`/`-@write`
category tokens and `&channel:*` pub/sub ACL tokens remain unimplemented and are dropped without
any error surfaced to the caller — a deployment can issue `ACL SETUSER x +@read` expecting
category-level read access and get a user that can execute **nothing** (since `all_commands`
defaults `false` for new users and no bare command names were ever added).

### 3.8 `ACL` subcommand mechanics (connection.rs:7815-7906, `resp.rs:202-213`)

```rust
pub enum AclSubcommand { List, Users, GetUser(String), SetUser{username,rules}, DelUser(Vec<String>), WhoAmI, Cat }
```

- **`WHOAMI`** — returns the connection's `auth_user` string (not re-derived from the ACL table).
- **`USERS`** — `AclManager::users()`: sorted list of all usernames.
- **`LIST`** — `AclManager::list()`: each user rendered via `to_acl_list_line` (acl.rs:142-181),
  sorted. Format: `user <name> on|off nopass|>pw1 >pw2 ... #hash1 ...  +@all -cmd1 -cmd2 | -@all +cmd1 ...  ~* | ~pat1 ~pat2 ...  &*` — note the line **always** ends with a hardcoded `&*` regardless of actual channel permissions (there are none), and when not `nopass`, both plaintext (`>pw`) and any hash not identical to a plaintext entry are listed.
- **`GETUSER <name>`** — returns a flat 8-element RESP array (4 key/value pairs: `flags`,
  `passwords`, `commands`, `keys`). `commands` is reduced to just `"+@all"` or `"-@all"` (the
  actual `allowed_commands`/`disallowed_commands` sets are **not** exposed), and `keys` is either
  `"~*"` or an **empty string** (the actual `allowed_key_patterns` list is likewise not exposed).
  There is no `channels` or `selectors` field at all (real Redis `ACL GETUSER` includes both).
- **`SETUSER <name> <rules...>`** — see §3.7; always returns `+OK` (rules are parsed best-effort,
  `Result<(), String>` is in practice always `Ok(())` — no token ever produces an `Err`).
- **`DELUSER <names...>`** — `AclManager::del_user` (acl.rs:358-367): explicitly refuses to
  remove `"default"` (silently skipped, not counted), removes every other named user, returns the
  count actually removed. Also sets `HAS_CUSTOM_ACL = true`.
- **`CAT`** — returns a **hardcoded static list of 21 category name strings**
  (`keyspace, read, write, set, sortedset, list, hash, string, bitmap, hyperloglog, geo, stream,
  pubsub, admin, fast, slow, blocking, dangerous, connection, transaction, scripting`). These
  names are not derived from any real per-command category metadata and have no relationship to
  `can_execute_command`'s enforcement (which only understands bare command names) — `ACL CAT`
  exists purely for client-compatibility discovery, not as a basis for `+@category` rules.

---

## 4. Memory Allocator (`src/allocator.rs`)

### 4.1 Confirmed: jemalloc is the real global allocator

```rust
// src/lib.rs:39-40
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

This is the **only** `#[global_allocator]` attribute in the crate (`rg` found exactly one match).
`Cargo.toml` also lists `mimalloc = { version = "0.1.52", default-features = false }` as a
dependency (line 15) and `tikv-jemallocator`/`tikv-jemalloc-ctl` (lines 21-22, both with
`"stats"` feature). **`mimalloc` is never referenced anywhere in `src/`** (`rg -n mimalloc src/`
finds zero hits outside `Cargo.toml`) — it is a dead/vestigial dependency, pulled in but unused.
This re-confirms (correcting an earlier stale doc claim) that **jemalloc, via
`tikv-jemallocator::Jemalloc`, is the actual process-wide allocator**; mimalloc is not wired up
anywhere.

### 4.2 `AllocatorStats` and `get_allocator_stats` (allocator.rs:3-36)

```rust
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocatorStats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub metadata: usize,
    pub mapped: usize,
    pub fragmentation_ratio: f64,
}

pub fn get_allocator_stats() -> AllocatorStats {
    let _ = tikv_jemalloc_ctl::epoch::advance();                 // refresh jemalloc's cached stats epoch
    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0);
    let active    = tikv_jemalloc_ctl::stats::active::read().unwrap_or(0);
    let resident  = tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0);
    let metadata  = tikv_jemalloc_ctl::stats::metadata::read().unwrap_or(0);
    let mapped    = tikv_jemalloc_ctl::stats::mapped::read().unwrap_or(0);
    let fragmentation_ratio = if allocated > 0 { resident as f64 / allocated as f64 } else { 1.0 };
    AllocatorStats { allocated, active, resident, metadata, mapped, fragmentation_ratio }
}
```

All five byte counters are real `tikv_jemalloc_ctl::stats::*` MIB reads against the live
jemalloc instance (not estimates). `fragmentation_ratio` is **not** a native jemalloc stat — it
is locally derived as `resident / allocated` (capped-below at `1.0` when nothing is allocated
yet; it can legitimately be `< 1.0` is impossible by construction since `resident >= allocated`
in jemalloc's accounting, so this ratio is always `>= 1.0` in practice, "1.00" meaning no
fragmentation overhead).

### 4.3 `format_memory_info` → `INFO` memory section field mapping (allocator.rs:39-89)

| `INFO` field | Source |
| :--- | :--- |
| `used_memory` / `used_memory_human` | the `used_mem` parameter passed in by the caller (a separately tracked estimate, see Component 05), **not** a jemalloc stat |
| `used_memory_rss` / `used_memory_rss_human` | `stats.resident` if nonzero, else falls back to `used_mem` |
| `maxmemory` / `maxmemory_human` | the `max_mem` parameter (config-driven, not allocator-driven) |
| `mem_fragmentation_ratio` | `stats.fragmentation_ratio` (resident/allocated) if `stats.allocated > 0`, else hardcoded `1.00` |
| `mem_allocator` | **hardcoded literal `"libc"`** — see §6, this is stale/incorrect given jemalloc is the real allocator |
| `allocator_allocated` / `allocator_active` / `allocator_resident` / `allocator_metadata` / `allocator_mapped` | direct `stats.*` passthrough, i.e. real jemalloc numbers |
| `cooled_keys` / `tiered_keys` | passed-through parameters from the tiering subsystem (Component 07), unrelated to the allocator |

`format_memory_info`'s only real call site is `crate::allocator::format_memory_info(...)` inside
`INFO`'s handler in `connection.rs`. There is no heap-profiling/dump capability exposed anywhere
(`tikv-jemalloc-ctl`'s profiling hooks beyond the `stats` feature are not used) — this module is
read-only aggregate-stats, not a diagnostic/dump tool.

### 4.4 `SmallCollectionArena` — per-shard object pool, unrelated to jemalloc (allocator.rs:91-344)

```rust
pub const MAX_ARENA_POOLED: usize = 1024;

#[derive(Debug, Default)]
pub struct SmallCollectionArena {
    list_pool: Vec<VecDeque<Bytes>>,
    hash_pool: Vec<Vec<(Bytes, Bytes)>>,
    set_pool: Vec<Vec<crate::table::SmallSetEntry>>,
    zset_pool: Vec<Vec<(crate::table::OrderedScore, Bytes)>>,
    pub allocations_saved: u64,
    pub recycles_count: u64,
}
```

Each collection type gets its own free-list `Vec` pool, pre-allocated with `Vec::with_capacity(128)`
slots in `SmallCollectionArena::new()`. The pattern is identical across all four types:

- `acquire_*(min_cap)`: pop a pooled container if one exists (increment `allocations_saved`),
  `reserve` it up to `min_cap` if its existing capacity is smaller; otherwise allocate fresh with
  `Vec/VecDeque::with_capacity(min_cap.max(16 or 8))`.
- `recycle_*(container)`: `clear()` the container's contents, then push it back onto the pool
  **only if** its capacity is `<= 512` *and* the pool is below `MAX_ARENA_POOLED` (1024) entries
  (increment `recycles_count`); otherwise the container is dropped normally (oversized or
  pool-full containers are not retained, bounding worst-case memory held by the pool).
- `pool_stats() -> (usize, usize, usize, usize)`: current `(list, hash, set, zset)` pool lengths,
  for introspection/tests.

**Integration**: `RudisTable` (Component 05, `table.rs:2423`) embeds
`arena: crate::allocator::SmallCollectionArena` as a plain field (one arena per shard's table,
not shared/locked). Call sites recycle a collection's backing storage when it empties out to zero
members (e.g. `LPOP` draining a list to empty, `SREM` draining a set to empty) and re-acquire from
the pool on the next `LPUSH`/`HSET`/`SADD`/`ZADD` that recreates the same key or a different key
of the same shape — e.g. `table.rs:6614` (`LPUSH`-family `acquire_list`), `table.rs:6692`
(another list push path), `table.rs:4834`/`4982` (`HSET`-family `acquire_small_hash`),
`table.rs:7950`/`8013` (`SADD`-family `acquire_small_set`), `table.rs:9506` (`ZADD`-family
`acquire_small_zset`), and `table.rs:2677-2686` (the four `recycle_*` calls on delete-to-empty).
The goal is eliminating allocator round-trips for the classic "push one item, pop one item"
churn pattern on small collections — it has no interaction with jemalloc stats or `INFO` at all.

---

## 5. TLS (`src/tls.rs`) — rustls handshake, self-signed certs, and `TlsTransport` (no kTLS)

### 5.1 Certificate provisioning

```rust
pub fn generate_self_signed_cert(subject_alt_names: Vec<String>) -> Result<(Vec<u8>, Vec<u8>), String>
```
Uses `rcgen` (`rcgen = "0.14.10"`): builds `CertificateParams` from the given SANs, sets CN
`"Rudis In-Memory Dev Cert"` and O `"Rudis Server"`, generates an `rcgen::KeyPair`, self-signs,
and returns `(cert_der, key_der)` as raw DER bytes. Called from `main.rs:189-193` with
`vec!["localhost", "127.0.0.1"]` whenever `--tls-port` is set but `--tls-cert-file`/
`--tls-key-file` are **not both** provided (main.rs:182-197) — i.e. this is the automatic
dev-cert fallback, generated fresh in memory on every process start (not cached to disk).

```rust
pub fn create_server_config(cert_der: &[u8], key_der: &[u8]) -> Result<Arc<ServerConfig>, String>
```
Wraps the DER bytes in `rustls::pki_types::{CertificateDer, PrivateKeyDer}` and builds a
`rustls::ServerConfig` via `.with_no_client_auth().with_single_cert(...)` — no mTLS/client-cert
verification is supported (server-only TLS).

```rust
pub fn load_certs_and_key_from_files(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>, String>
```
Reads both files and decodes them with `rustls::pki_types::pem::PemObject` when they contain a
`-----BEGIN ` marker: every `CERTIFICATE` block of the cert file becomes the served chain (leaf
first, e.g. certbot's `fullchain.pem`), and the key file may hold a PKCS#8, PKCS#1 (RSA) or SEC1
(EC) key. Files without a PEM marker are still accepted as one raw DER certificate and a DER key,
which was the only format that worked before (see §6). Errors name the offending file, and
`main.rs` aborts startup on them.

### 5.2 `TlsSession` and the handshake

`TlsSession` wraps a `rustls::ServerConnection`. (It used to also carry an `is_ktls_active: bool`
that was always `false`; that field is gone — see §5.3.)

The async handshake driver `handshake_monoio` is the one wired into `server.rs`'s TLS accept loop,
bounded by a 10 s handshake timeout. It runs the standard rustls handshake loop — `while
conn.is_handshaking() { write_tls while wants_write; read_tls + process_new_packets when
wants_read }` — then flushes any trailing handshake/ticket frames, driven over
`monoio::net::TcpStream` using `AsyncReadRent`/`AsyncWriteRentExt`. It is a correct async
adaptation of the loop — the handshake itself is real and correctly negotiates a session.

### 5.3 kTLS — not implemented (scaffolding removed)

Earlier revisions of `tls.rs` carried partial kTLS scaffolding: an `enable_ktls(raw_fd)` that only
issued `setsockopt(IPPROTO_TCP, TCP_ULP, "tls")` (attaching the kernel TLS ULP, with **no**
`setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` key install), called after every handshake with its
result discarded; an `is_ktls_active` flag hard-forced to `false`; and raw-socket kTLS branches in
`read_plaintext`/`write_plaintext` that could therefore never run. (Before commit `8c39a2f` the flag
was set from `enable_ktls`'s success, which produced the old plaintext-bypass bug; that bug note is
now obsolete because the whole code path is gone.)

All of that has been **deleted**. TLS is userspace `rustls` only. Real kTLS was judged not worth it
for Rudis: it would need `rustls`'s `dangerous_extract_secrets` plus per-cipher-suite
`SOL_TLS` `TLS_TX`/`TLS_RX` key installation, RX-side handling of TLS 1.3 control records
(`recvmsg` with a record-type cmsg for KeyUpdate / NewSessionTicket / alerts), and the `tls` kernel
module — and since Rudis does not use `sendfile`, it would mostly just move the crypto cost into the
kernel. It is also not testable in this project's CI. Removing it is preferable to keeping inert
scaffolding that reads like a working feature.

### 5.4 Wiring into the server (server.rs / main.rs / connection.rs / transport.rs)

- `main.rs:182-204` builds the `rustls::ServerConfig` once at startup (via
  `load_certs_and_key_from_files` or `generate_self_signed_cert`+`create_server_config`) and
  wraps it in `TlsWorkerConfig { tls_port, server_config }`, passed down to every
  shard worker.
- `server.rs:101-127` conditionally opens a **second** `SO_REUSEPORT`/`SO_REUSEADDR` socket on
  `tls_cfg.tls_port`, parallel to the shard's plain listener, when `tls_config` is `Some`.
- `server.rs` spawns a dedicated accept loop for that listener (polled with a 200ms
  timeout against `crate::shutdown::is_shutting_down()`, same pattern as the plain accept loop).
  Each accepted TLS client gets `client_id = ((shard_id as u64) << 48) | 0x8000_0000_0000 + n` —
  the `0x8000_0000_0000` bit flags it as a TLS-origin client ID, disjoint from plain client IDs on
  the same shard. Per connection: `TlsSession::new(server_config.clone())` →
  `session.handshake_monoio(&mut stream)` (10 s timeout) → on success the session is wrapped in a
  `TlsTransport` and handed to the generic client loop.
- There is no separate TLS client loop any more (the old `handle_tls_connection` was deleted).
  `handle_client<T: ClientTransport>` in `connection.rs` is generic over the trait in
  `src/transport.rs` and monomorphized for `PlainTransport` and `TlsTransport` (no `dyn`, so the
  plaintext path is unchanged). TLS clients therefore get the full plaintext feature set: pipeline
  squashing (`execute_commands_squashed`), `MULTI`/`EXEC` pipelining, Pub/Sub mode (generic
  `run_pubsub_loop` over a split transport; the rustls session is shared between reader and writer
  task via `Rc<RefCell>`), `CLIENT KILL`/`CLIENT LIST` via the global client registry,
  output-buffer limits, `omem`/`qbuf`/pipeline stats, protocol-error close, shutdown drain, idle
  timeout, max-clients, and the same `authenticated`/`auth_user` bootstrap (§3.5).
- Out-of-band pushes (`MONITOR` lines, client-tracking invalidations) produced on other threads go
  through `transport::PushTarget`: `Fd(RawFd)` for plaintext (non-blocking `libc::send` on the fd,
  as before) and `Queue` (bounded flume channel, 4096) for TLS, drained and encrypted by the owning
  connection. Previously these were raw `libc::send`s on the fd, which injected plaintext into a
  TLS `MONITOR` client's stream.
- Two rustls usage bugs were fixed along the way: ciphertext left over beyond one `read_tls` call
  (~4 KiB) used to be silently dropped (pipelines larger than ~4 KiB per read lost commands), and
  replies larger than 64 KiB failed on rustls's default send-buffer limit.
- Replication links (`PSYNC`/`SYNC`/`DFLY FLOW`) remain plaintext-port only; on the TLS port they get
  `-ERR replication links are only supported on the plaintext port` and the connection is closed.

---

## 6. Status of Previously-Reported Issues (Re-Verified Against Current Source)

| Issue | Prior status | Current status |
| :--- | :--- | :--- |
| `requirepass` config/`CONFIG SET` not enforced (default user stayed `nopass: true`, `HAS_CUSTOM_ACL` never set) | Verified gap | **FIXED.** `main.rs:137-153` and `CONFIG SET requirepass` (connection.rs:6686-6703) both now clear+repopulate `passwords`/`password_hashes`, flip `nopass=false`, and set `HAS_CUSTOM_ACL=true`. `is_auth_required_for_default()` correctly returns `true` afterward, and both plain and TLS connections (same `router.port` → same `AclManager`) bootstrap `authenticated=false`. Traced to commit `002086a feat(security): synchronize requirepass ACL enforcement and implement salted SHA-256 password hashing`. |
| kTLS `TCP_ULP`-success treated as "encryption active", causing silent plaintext after a real handshake | Verified CRITICAL bug | **OBSOLETE — code removed.** First neutralized in commit `8c39a2f` (result discarded, `is_ktls_active` forced `false`); now `enable_ktls`, `is_ktls_active`, and the kTLS read/write branches are deleted entirely. kTLS is not implemented; all `--tls-port` traffic goes through userspace rustls (§5.3). |
| TLS clients ran a separate, feature-poor loop (`handle_tls_connection`: no pipeline squashing, no output-buffer limits, etc.); MONITOR/tracking pushes were written as plaintext onto TLS sockets; >~4 KiB leftover ciphertext per read dropped; >64 KiB replies failed | Gap / bugs | **FIXED.** TLS clients run the generic `handle_client<TlsTransport>` loop; pushes go through `PushTarget::Queue` and are encrypted by the owning connection; both rustls buffering bugs fixed (§5.4). |
| Password hashing scheme / unsalted-SHA-256 compatibility with real Redis | To re-verify | **Confirmed**: `hash_password_sha256` (acl.rs:33-42) is unsalted `SHA256(password)`, `#`-prefixed 64-hex — matches real Redis's ACL hash format exactly. (A separate legacy fixed-salt SHA1 scheme and a dead per-user-salted SHA-256 scheme also exist in the same file — see §3.2 — but the SHA-256 unsalted form is the one actually written by `requirepass`/`ACL SETUSER >password`.) |
| Allocator is jemalloc, not mimalloc | To re-verify | **Confirmed**: `src/lib.rs:39-40` sets `#[global_allocator] = tikv_jemallocator::Jemalloc`. `mimalloc` remains in `Cargo.toml` as a dependency but has zero references anywhere in `src/` — dead/unused. |

### New findings from this pass

- **`load_certs_and_key_from_files` failed on real PEM certs** (tls.rs:54-74). **FIXED:** PEM
  chains and keys are now decoded (§5.1), covered by unit tests and an e2e test that starts the
  binary with PEM files. Original finding:
  The function reads the raw bytes of `--tls-cert-file`/`--tls-key-file` and passes them straight
  to `create_server_config` as DER — there is no PEM decoding anywhere in the crate (`rg` for
  `pem`/`rustls-pemfile` in `Cargo.toml`/`src/tls.rs` finds nothing). Real-world cert/key files
  (from `certbot`, `openssl`, etc.) are almost always PEM-encoded text
  (`-----BEGIN CERTIFICATE-----...`), which rustls will reject as invalid DER when
  `ServerConfig::builder()...with_single_cert(...)` tries to parse it, surfacing as
  `Err("Failed to create rustls ServerConfig: ...")` from `create_server_config`, which `main.rs:
  185-187`'s `.expect("Failed to load TLS cert/key files")` turns into a startup panic. In
  practice, `--tls-cert-file`/`--tls-key-file` only work today if the files happen to contain raw
  DER bytes, not the PEM format the flag names and doc comments imply; the self-signed in-memory
  fallback path (no cert/key files given) is unaffected since it already produces raw DER.
- **`mem_allocator:libc` is a hardcoded, incorrect `INFO` field** (allocator.rs:66). Despite
  every `allocator_*` field in the same `INFO` block being real `tikv-jemalloc-ctl` data, the
  `mem_allocator` field itself is a string literal `"libc"`, not `"jemalloc"` — any tooling that
  branches on this field (e.g. Redis-compatible monitoring that picks jemalloc-specific behavior
  based on `mem_allocator`) will be misled.
- **`hash_password_salted` is dead on the write path** (acl.rs:45-58, 216). Computed on every
  `check_auth` call but never stored by any code path — wasted SHA-256 computation per
  authentication attempt with zero behavioral effect (see §3.2).
- **`Command::Reset`'s re-auth check diverges from `is_auth_required_for_default`** (connection.rs:
  9414-9421). It only inspects `passwords.is_empty()`, ignoring `nopass` and `password_hashes`.
  A default user secured solely via a pre-hashed credential (`ACL SETUSER default #<hash>`, empty
  `passwords`) would be required to re-authenticate by `is_auth_required_for_default()`'s logic,
  but `RESET` would instead leave the connection `authenticated = true` as `"default"` — a minor
  but real inconsistency between two "is the default user open" computations that should agree.
- **`ACL GETUSER` exposes far less detail than it stores** (connection.rs:7839-7865). The real
  `allowed_commands`/`disallowed_commands` sets and `allowed_key_patterns` list are collapsed to
  just `"+@all"`/`"-@all"` and `"~*"`/`""` respectively — an operator cannot see which specific
  commands/key-patterns a restricted user actually has via `GETUSER`; `ACL LIST`'s
  `to_acl_list_line` output (§3.8) is the only place the granular rule set is visible.
- **`ACL CAT`'s category list is cosmetic** (connection.rs:7877-7905) — 21 hardcoded strings with
  no link to real command metadata or to the `+@category`/`-@category` tokens that `ACL SETUSER`
  silently ignores (§3.7). A client that lists categories via `ACL CAT` and then tries to use one
  in `ACL SETUSER +@read` will see the rule accepted (`+OK`) but have no actual effect.

---

## 7. Cross-Component Interactions

- **`src/connection.rs`**: hosts essentially all ACL/TLS *behavior* — `AUTH`/`HELLO AUTH`/`RESET`/
  `ACL *` command handlers, the per-command and squash-path enforcement gates (§3.5), `CONFIG
  GET/SET requirepass` (§3.6), and the single generic client loop `handle_client<T:
  ClientTransport>` that serves both plain and TLS connections (§5.4). `allocator::format_memory_info`
  is called from `INFO`'s handler.
- **`src/server.rs`** (Component 01): conditionally binds the second `SO_REUSEPORT` TLS listener
  per shard and spawns its dedicated accept loop (§5.4), parallel to everything it already does
  for the plain listener.
- **`src/main.rs`**: parses `--tls-port`/`--tls-cert-file`/`--tls-key-file`, builds the shared
  `rustls::ServerConfig` once before any shard thread starts, and primes the default ACL user
  from `--requirepass`/config-file `requirepass` (§3.6) — also before any shard thread starts, so
  every shard observes a consistently-initialized `AclManager` from its very first connection.
- **`src/table.rs`** (Component 05): embeds `SmallCollectionArena` as `RudisTable.arena` (§4.4) —
  the only consumer of that half of `allocator.rs`; unrelated to jemalloc stats.
- **`src/tiering.rs`** (Component 07): does **not** call `allocator::get_allocator_stats()` —
  tiering's memory-pressure decisions use a separately tracked `used_memory` estimate on
  `RudisTable`, not live jemalloc RSS figures.

---

## 8. Future Improvements

- **Medium — remove plaintext password storage now that hashing exists** (§3.2, §3.7).
  `ACL SETUSER user >password` (and `requirepass`) still push the plaintext into
  `AclUser.passwords` in addition to hashing it — the hashing machinery exists but the plaintext
  exposure it should close (credentials sitting in process memory / reachable via a core dump,
  echoed back verbatim by `CONFIG GET requirepass`/`ACL LIST`) remains.
- **Medium — delete or wire up `hash_password_salted`** (§3.2, §6). It is pure dead weight on the
  hot `check_auth` path today; either start writing hashes in this format from `set_user`/
  `requirepass`, or remove the function and its per-call computation entirely.
- **Medium — replace the legacy fixed-salt SHA1 scheme with per-user-salted work-factored
  hashing.** `hash_password`'s hardcoded global salt (`"rudis_acl_salt_v1:"`) plus a fast,
  non-work-factored hash (SHA1) offers little resistance to offline brute force; real Redis
  accepts this tradeoff by documenting SHA-256 as fast-but-standard, but a slow KDF
  (Argon2id/bcrypt/scrypt) would be meaningfully stronger if credential confidentiality under a
  memory/disk compromise matters for a given deployment.
- **Low — reconcile `Command::Reset`'s auth-required check with `is_auth_required_for_default`**
  (§6) so both computations agree in every configuration, not just the common
  plaintext-password-set case.
- **Low — fix `mem_allocator:libc`** to report `"jemalloc"` (§6), and consider removing the
  unused `mimalloc` dependency from `Cargo.toml` to avoid confusing future readers the way the
  prior documentation pass was apparently misled.
- **Low — extend `ACL SETUSER`/`ACL CAT`/`ACL GETUSER` to be internally consistent**: either
  implement `+@category`/`&channel` tokens for real, or have `ACL SETUSER` reject unrecognized
  rule tokens with an error instead of silently accepting them (§3.7, §6); and have `GETUSER`
  expose the real `allowed_commands`/`disallowed_commands`/`allowed_key_patterns` detail that
  `ACL LIST` already renders, rather than the collapsed all-or-nothing summary it returns today.
- **Low — expose jemalloc heap-profiling/dump capability**, not just aggregate stats (§4.2), if
  production memory-leak/fragmentation debugging ever becomes a need —
  `tikv-jemalloc-ctl` supports profiling hooks beyond the `stats` feature currently used.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: `AclManager` is keyed by **port number** in a single process-global `PORT_ACLS`
  map, not per-connection-type — a shard's plain and TLS listeners share one `AclManager` because
  they share one `Router`/`router.port`. There is no way today to give the TLS listener a
  different ACL policy than the plain listener on the same port pair.
* **Gotcha 2**: `check_auth` short-circuits to `Ok` immediately when `user.nopass` is `true`,
  before touching `passwords`/`password_hashes` at all — a user with `nopass` set has those
  fields effectively ignored for authentication purposes even if populated.
* **Gotcha 3**: `HAS_CUSTOM_ACL` is a one-way latch for the whole process (never reset to
  `false`) — once any port primes `requirepass` or runs `ACL SETUSER`/`DELUSER`, the per-command
  ACL check (§3.5) stays "on" for every port for the rest of the process lifetime, even a port
  that never configured its own ACL.
* **Gotcha 4**: `SmallCollectionArena` (allocator.rs §4.4) is unrelated to jemalloc — don't
  expect `pool_stats()`/`allocations_saved` to show up in jemalloc `INFO` fields; it is a
  hand-rolled free-list pool that sits *above* jemalloc, reducing calls into it.
* **Gotcha 5**: kTLS is deliberately **not implemented** (§5.3) — the old `enable_ktls`/
  `is_ktls_active` scaffolding was removed rather than left inert. Re-adding kTLS means a full
  implementation (`dangerous_extract_secrets`, `SOL_TLS` `TLS_TX`/`TLS_RX` key install per cipher
  suite, RX control-record handling via `recvmsg` cmsgs), not just a `TCP_ULP` attach — a partial
  version is exactly what produced the old plaintext-bypass bug. Likewise, never `libc::send`
  directly on a TLS client's fd: deliver out-of-band data through `PushTarget` (§5.4).
* **Gotcha 6**: `--tls-cert-file`/`--tls-key-file` take PEM (a chain is allowed) or raw DER. Any
  other content is a startup error, not a fallback to the self-signed certificate.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests (acl.rs: test_acl_salted_password_hashing, test_acl_command_and_key_enforcement;
#    allocator.rs: test_small_collection_arena_lifecycle, test_collection_arena_rudis_table_integration;
#    tls.rs: test_tls_cert_generation_and_config, test_tls_worker_config)
cargo test --lib -- --test-threads=1
```
