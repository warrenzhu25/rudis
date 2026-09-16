# Component 15: Security, Memory Allocator & TLS (`src/acl.rs`, `src/allocator.rs`, `src/tls.rs`)

## 1. Architectural Purpose & Scope

> **Update note**: since this doc was last verified, a real batch of fixes landed — salted
> password hashing, real per-command/per-key ACL enforcement, and a genuinely-wired `--tls-port`
> listener. This revision re-verifies all three against the current source. Two of the three are
> improvements as advertised; the TLS wiring introduced a new, more severe problem than the
> "dead code" state it replaced — see §2.4/§4.4's ⚠️ for a real plaintext-over-the-wire bug in the
> kTLS path. Read that section before treating `--tls-port` as safe to enable.

Three unrelated system-services modules bundled under one doc:
1. **Access control (`src/acl.rs`)**: a per-port, multi-user authentication *and, as of this update, real authorization* store (`AUTH user pass`, `ACL SETUSER/GETUSER/LIST/USERS/DELUSER/WHOAMI`) modeled loosely on Redis ACL syntax. Per-command and per-key checks are now genuinely enforced — see §2.2 — though password storage still has real weaknesses, see §2.3.
2. **Jemalloc telemetry (`src/allocator.rs`)**: read-only statistics via `tikv-jemalloc-ctl`, surfaced through `INFO`'s memory section. No profiling/heap-dump capability. Unchanged by this update.
3. **TLS certificate/handshake plumbing (`src/tls.rs`)**: a real `rustls` handshake wrapper with genuine in-memory self-signed cert generation (`rcgen`), now genuinely wired to a `--tls-port` listener (§4.4) — but the `kTLS` fast-path it also wires up has a real bug that causes it to silently transmit **unencrypted** application data once activated (§2.4). This is worse than the previous "dead code" state, not better, for anyone who enables `--tls-port` on Linux with the kernel `tls` module available.

---

## 2. Key Invariants & Concurrency Constraints

1. **One `AclManager` per listening port, not global**: `PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>>`, looked up via `get_acl_for_port(port)`. Every shard thread serving the same port shares the same `Arc<RwLock<AclManager>>` — this is, like `BlockHub` (Component 06), a deliberate exception to the shared-nothing/zero-lock architecture, needed because auth state has to be consistent across every shard's independently-accepted connections on that port.
2. **Authentication is enforced, and authorization is now real too (updated).** `execute_command` gates on `authenticated: bool` exactly as before (`-NOAUTH` for anything but `AUTH`/`HELLO`/`QUIT` while unauthenticated), but immediately after that gate it now looks up the authenticated user's `AclUser` by `auth_user` and calls two new real methods before dispatching: `can_execute_command(cmd_name)` (checks `disallowed_commands`/`allowed_commands` depending on `all_commands`, always allowing `PING`/`RESET`/`QUIT`/`AUTH`/`HELLO`) and, if the command has a primary key, `can_access_key(key)` (checks `allowed_key_patterns`, a `prefix*`/exact-match list, unless `all_keys`). A denied command gets a real `-NOPERM` reply and returns without executing (§4.1). The pipelined squash-eligibility check (`execute_commands_squashed`) also consults these two methods per command — a command an ACL would deny just falls back to the sequential path, where the real `-NOPERM` denial happens.
3. **Passwords are now hashed, but plaintext storage was not removed — this is a real gap, not full resolution.** `AclUser` gained a `password_hashes: Vec<String>` field, and `hash_password` computes `SHA1("rudis_acl_salt_v1:" + password)`. But `ACL SETUSER user >password` still pushes the plaintext into `passwords` *and* the hash into `password_hashes` (§4.2) — the plaintext field was never removed, so anyone who could previously read plaintext passwords from memory/a core dump still can. The hash itself is also weak by password-hashing standards: SHA1 is a fast general-purpose hash (not a slow KDF like Argon2/bcrypt/scrypt, so no work-factor resistance to offline brute force), and the salt (`"rudis_acl_salt_v1:"`) is a single hardcoded constant shared by every user and every deployment, not a per-user random salt — identical passwords across users or across a fleet of Rudis instances produce identical hashes, and the fixed salt is trivially precomputable into a rainbow table once known. `check_auth` accepts a match against either the plaintext or the hash (`§4.1`), so both weaknesses are live simultaneously.
4. **TLS is now genuinely wired up — and its kTLS fast-path has a real, severe bug: it silently transmits plaintext.** A `--tls-port` listener now exists (§4.4) and performs a real `rustls` handshake via `TlsSession::handshake_monoio`. After a successful handshake, it unconditionally attempts `enable_ktls`, which only calls `setsockopt(IPPROTO_TCP, TCP_ULP, "tls")` — attaching the kernel's TLS upper-layer-protocol module — and, if that syscall merely *succeeds* (which it will on any Linux host with the `tls` kernel module loadable, regardless of whether any key material was ever installed), sets `is_ktls_active = true`. Both `TlsSession::read_plaintext` and `write_plaintext` then branch on `is_ktls_active`: when true, they read/write **raw socket bytes directly, with no rustls encryption/decryption at all**, on the assumption the kernel is doing it. But the second, actually-required `setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` call that installs the negotiated key into the kernel socket is still missing (this exact gap was already flagged when the code was dead — see §4.4) — so the kernel never encrypts anything, and **every byte of application data after the handshake goes out on the wire in cleartext**, while both client and server believe they completed a real TLS session. This is worse than TLS simply not existing: a client connecting to `--tls-port` gets a real, correctly-negotiated handshake and then a false sense of security for the rest of the connection.
5. **Allocator stats are read-only telemetry**, gated behind no lock beyond jemalloc's own internal epoch counter (`tikv_jemalloc_ctl::epoch::advance()`), safely callable from any thread without coordination with the rest of Rudis.

---

## 3. Component Architecture & Data Structures

```
                 AUTH user pass  /  ACL SETUSER|GETUSER|LIST|USERS|DELUSER|WHOAMI
                                     │
                     PORT_ACLS: Mutex<HashMap<port, Arc<RwLock<AclManager>>>>
                                     │
                          AclManager { users: HashMap<String, AclUser> }
                                     │
                     check_auth(username, password) -> Result<String, &str>
                     (accepts a plaintext OR password_hashes match — §2.3)
                                     │
                     sets `authenticated = true`, then on EVERY subsequent command:
                     user.can_execute_command(name) && user.can_access_key(key)?
                     -NOPERM if either check fails (§2.2/§4.1) — real enforcement now


                 INFO command (memory section)                    (unchanged)
                                     │
                     allocator::format_memory_info(used_mem, max_mem, ...)
                                     │
                     allocator::get_allocator_stats()  →  tikv_jemalloc_ctl::stats::*


                 --tls-port listener (server.rs, new) ──► TlsSession::handshake_monoio
                                                            (real rustls handshake)
                                                                     │
                                                            enable_ktls(TCP_ULP) "succeeds"
                                                                     │
                                                  is_ktls_active = true, NO key installed
                                                                     │
                                            ⚠️ read_plaintext/write_plaintext skip rustls
                                               entirely and touch the raw socket — every
                                               byte after the handshake is sent in the
                                               clear (§2.4/§4.4)
```

### The real `AclUser` / `AclManager` (`src/acl.rs`, updated)

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    pub passwords: Vec<String>,          // still plaintext, still populated (§2.3)
    pub password_hashes: Vec<String>,    // new: SHA1 with a fixed global salt (§2.3)
    pub nopass: bool,
    pub all_commands: bool,
    pub allowed_commands: hashbrown::HashSet<String>,   // new
    pub disallowed_commands: hashbrown::HashSet<String>, // new
    pub all_keys: bool,
    pub allowed_key_patterns: Vec<String>,               // new
}
```

`allowed_commands`/`disallowed_commands`/`allowed_key_patterns` now exist and are genuinely
read by `can_execute_command`/`can_access_key` (§2.2/§4.1) — the old doc's "no such fields
exist" finding no longer holds. `AclManager` is unchanged:

```rust
pub struct AclManager {
    pub users: HashMap<String, AclUser>,
}
```

### The real allocator stats (`src/allocator.rs`)

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
```

(`fragmentation_ratio` is derived as `resident / allocated`, not a jemalloc-native field.)

---

## 4. Execution Algorithms & Code Logic

### 4.1 Authentication + real authorization (updated) — `check_auth`, `can_execute_command`, `can_access_key`

```rust
pub fn check_auth(&self, username: Option<&str>, password: &str) -> Result<String, &'static str> {
    let user_name = username.unwrap_or("default");
    if let Some(user) = self.users.get(user_name) {
        if !user.enabled { return Err("WRONGPASS User is disabled"); }
        let hashed = hash_password(password);
        if user.nopass
            || user.passwords.iter().any(|p| p == password)
            || user.password_hashes.iter().any(|h| h == &hashed || h == password)
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

Note `check_auth` accepts a match against `password_hashes` via **either** the freshly-computed
hash **or** the raw password string itself (`h == password`) — this lets `ACL SETUSER user
#<precomputed-hash>` (Redis's real syntax for pre-hashed passwords) work by comparing the
stored value directly against whatever the client sent, without knowing in advance whether
that stored value is a hash or plaintext.

The real new enforcement, in `connection.rs`'s `execute_command`, runs immediately after the
existing `-NOAUTH` gate and before the command's match arm:

```rust
if !*authenticated && !matches!(cmd, Command::Auth { .. } | Command::Hello { .. } | Command::Quit) {
    out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
    return false;
}

if *authenticated {
    let acl = crate::acl::get_acl_for_port(router.port);
    let acl_guard = acl.read().unwrap();
    if let Some(user) = acl_guard.get_user(auth_user) {
        if !user.can_execute_command(cmd_name) {
            out.extend_from_slice(format!(
                "-NOPERM this user has no permissions to run the '{}' command\r\n",
                cmd_name.to_lowercase()
            ).as_bytes());
            return false;
        }
        if let Some(key) = cmd_primary_key(&cmd)
            && !user.can_access_key(key.as_ref())
        {
            out.extend_from_slice(b"-NOPERM this user has no permissions to access one of the keys used as arguments\r\n");
            return false;
        }
    }
}
```

with the real check methods on `AclUser`:

```rust
pub fn can_execute_command(&self, cmd_name: &str) -> bool {
    let name = cmd_name.to_lowercase();
    if matches!(name.as_str(), "ping" | "reset" | "quit" | "auth" | "hello") { return true; }
    if self.all_commands { !self.disallowed_commands.contains(&name) }
    else { self.allowed_commands.contains(&name) }
}

pub fn can_access_key(&self, key: &[u8]) -> bool {
    if self.all_keys { return true; }
    let key_str = String::from_utf8_lossy(key);
    self.allowed_key_patterns.iter().any(|pat| {
        pat == "*"
            || pat.strip_suffix('*').is_some_and(|prefix| key_str.starts_with(prefix))
            || key_str == *pat
    })
}
```

A denied user genuinely gets `-NOPERM`, not a silent allow. `execute_commands_squashed`'s
squash-eligibility loop calls the same two methods per queued command; a command an ACL
would deny simply falls back to the sequential path, where the real denial above fires.

### 4.2 `ACL SETUSER` — now recognizes command/key rules too, but still silently drops the rest

```rust
for rule in rules {
    if rule == "on" { user.enabled = true; }
    else if rule == "off" { user.enabled = false; }
    else if rule == "nopass" { user.nopass = true; user.passwords.clear(); user.password_hashes.clear(); }
    else if let Some(p) = rule.strip_prefix('>') {
        user.nopass = false;
        user.passwords.push(p.to_string());              // plaintext still stored — §2.3
        user.password_hashes.push(hash_password(p));       // hash added alongside it
    }
    else if let Some(h) = rule.strip_prefix('#') { user.password_hashes.push(format!("#{}", h)); }
    else if let Some(p) = rule.strip_prefix('<') { user.passwords.retain(|pass| pass != p); }
    else if rule == "+@all" || rule == "+all" { user.all_commands = true; user.disallowed_commands.clear(); }
    else if rule == "-@all" || rule == "-all" { user.all_commands = false; user.allowed_commands.clear(); }
    else if let Some(cmd) = rule.strip_prefix('+') {       // new: per-command allow
        let c = cmd.to_lowercase();
        if user.all_commands { user.disallowed_commands.remove(&c); } else { user.allowed_commands.insert(c); }
    }
    else if let Some(cmd) = rule.strip_prefix('-') {       // new: per-command deny
        let c = cmd.to_lowercase();
        if user.all_commands { user.disallowed_commands.insert(c); } else { user.allowed_commands.remove(&c); }
    }
    else if rule == "~*" || rule == "allkeys" { user.all_keys = true; user.allowed_key_patterns.clear(); }
    else if rule == "resetkeys" { user.all_keys = false; user.allowed_key_patterns.clear(); }
    else if let Some(pat) = rule.strip_prefix('~') { user.all_keys = false; user.allowed_key_patterns.push(pat.to_string()); }
}
```

`ACL SETUSER bob on >pw -@all +get ~user:*` now genuinely produces a user who can only run
`GET` (plus the always-allowed `PING`/`RESET`/`QUIT`/`AUTH`/`HELLO`) against keys matching
`user:*` — verified by a real unit test (`test_acl_command_and_key_enforcement`). The old
doc's finding that `+@category` tokens (e.g. `+@read`/`-@write`) and other glob forms like
`&channel:*` are silently accepted-but-ignored still holds — only bare per-command `+cmd`/
`-cmd` tokens and simple `prefix*`/exact-match key patterns are real; category-level and
pub/sub-channel ACL rules are not implemented.

### 4.3 Allocator telemetry — real jemalloc reads, exposed through `INFO`

```rust
pub fn get_allocator_stats() -> AllocatorStats {
    let _ = tikv_jemalloc_ctl::epoch::advance();
    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0);
    let active = tikv_jemalloc_ctl::stats::active::read().unwrap_or(0);
    let resident = tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0);
    let metadata = tikv_jemalloc_ctl::stats::metadata::read().unwrap_or(0);
    let mapped = tikv_jemalloc_ctl::stats::mapped::read().unwrap_or(0);
    let fragmentation_ratio = if allocated > 0 { resident as f64 / allocated as f64 } else { 1.0 };
    AllocatorStats { allocated, active, resident, metadata, mapped, fragmentation_ratio }
}
```

`format_memory_info` (also in `allocator.rs`) wraps this into the RESP bulk string returned by `INFO`'s memory section — confirmed by its single real call site in `connection.rs` (`crate::allocator::format_memory_info(...)`). There is no heap-profiling/dump capability (no `jemalloc_pprof`-style export) — this module is stats-only.

### 4.4 TLS is now wired end-to-end — and its kTLS fast-path has a live plaintext bug

`main.rs` gained `--tls-port`/`--tls-cert-file`/`--tls-key-file` flags; when `--tls-port` is
set, each shard's `run_shard_worker` (Component 01) now binds a **second** `SO_REUSEPORT`
listener on that port, parallel to the plain one, and spawns a dedicated accept loop for it:

```rust
// server.rs — spawned only if tls_config is Some
match tls_listener.accept().await {
    Ok((mut stream, client_addr)) => {
        let mut session = crate::tls::TlsSession::new(s_cfg)?;
        session.handshake_monoio(&mut stream).await?;          // real rustls handshake
        crate::connection::handle_tls_connection(stream, session, client_addr, client_id, reg_clone, router_clone).await;
    }
    ...
}
```

`handshake_monoio` is a genuine, correctly-written async adaptation of the `rustls` handshake
loop (`wants_write`/`write_tls`/`wants_read`/`read_tls`/`process_new_packets`, driven through
`monoio`'s `AsyncReadRent`/`AsyncWriteRentExt` instead of blocking I/O) — the handshake itself
is real and correctly negotiates a TLS session. `handle_tls_connection` (new, in
`connection.rs`) then runs the same command-execution machinery as `handle_connection`, but
reads/writes through `TlsSession::read_plaintext`/`write_plaintext` instead of the raw socket.

**⚠️ The bug**: at the end of a successful handshake, both `complete_handshake` (still unused
directly) and `handshake_monoio` unconditionally call `enable_ktls`, and `enable_ktls` — same
as before — only performs the *first* of the two Linux kTLS setup calls:

```rust
pub fn enable_ktls(raw_fd: RawFd) -> io::Result<()> {
    let ret = unsafe { libc::setsockopt(raw_fd, IPPROTO_TCP, TCP_ULP, b"tls\0".as_ptr() as *const _, 4) };
    if ret == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}
// handshake_monoio, after a successful handshake:
if enable_ktls(raw_fd).is_ok() { self.is_ktls_active = true; }
```

`setsockopt(IPPROTO_TCP, TCP_ULP, "tls")` only attaches the kernel's TLS upper-layer-protocol
module to the socket — it succeeds regardless of whether any key material is ever installed,
and the Linux kernel does not encrypt anything from this call alone. The second, actually
required call — `setsockopt(SOL_TLS, TLS_TX, ...)`/`TLS_RX` installing the negotiated
cipher/key/IV — still does not exist anywhere in this file, exactly as before this update.
The difference now is that `is_ktls_active` gates real I/O behavior:

```rust
// TlsSession::read_plaintext / write_plaintext
if self.is_ktls_active {
    // reads/writes the RAW socket directly — no rustls encrypt/decrypt at all
    let (res, returned) = stream.read(std::mem::take(read_buf)).await;
    ...
} else {
    // real rustls-mediated encrypt/decrypt path
}
```

Since `enable_ktls` "succeeds" on any Linux host where the `tls` kernel module is loadable
(common on modern distros) regardless of key installation, `is_ktls_active` becomes `true` on
essentially every real Linux deployment, and **every byte of application data sent after the
handshake goes out on the wire completely unencrypted** — while the client and server both
believe they completed a real TLS session, because the handshake itself genuinely succeeded.
This is strictly worse than the previous "TLS code exists but nothing calls it" state: before,
no one could accidentally rely on TLS that wasn't there; now, enabling `--tls-port` produces a
working handshake followed by silent plaintext, which is the worst version of this failure
mode for anyone who trusts it. Treat `--tls-port` as unsafe to use until either `enable_ktls`
performs the real key-install `setsockopt` call, or (much simpler and lower-risk) `is_ktls_active`
is just never set to `true` by a bare `TCP_ULP` success, and the code always takes the real
`rustls`-mediated encrypt/decrypt path.

---

## 5. Cross-Component Interactions

- **`src/connection.rs`**: `get_acl_for_port` backs both the `AUTH`/`ACL *` command handlers and the new per-command/per-key enforcement in `execute_command`/`execute_commands_squashed` (§4.1); `allocator::format_memory_info` is called via `INFO`; and, new, `handle_tls_connection` is a second connection-handling entry point (alongside plain `handle_connection`) that routes all I/O through a `TlsSession` (§4.4).
- **`src/server.rs`** (Component 01): now conditionally binds a second `SO_REUSEPORT` listener on `--tls-port` and spawns a dedicated TLS accept loop per shard (§4.4), in addition to everything it did before.
- **`src/main.rs`**: parses the new `--tls-port`/`--tls-cert-file`/`--tls-key-file` CLI flags, builds a `rustls::ServerConfig` once (via `tls::create_server_config` or `tls::generate_self_signed_cert` if no cert/key files are given) before spawning any shard thread, and passes it down as `TlsWorkerConfig`.
- **`src/tiering.rs`**: does not call `allocator::get_allocator_stats()` — memory-pressure decisions for auto-tiering are driven by a separately tracked `used_memory` estimate on `RudisTable` (see Component 05), not by live jemalloc RSS figures. Unchanged by this update.

---

## 6. Performance Characteristics

- **Auth check cost**: one `RwLock::read()` acquisition plus a linear scan over `passwords`/`password_hashes` (typically 0-1 entries each) plus one SHA1 computation per `AUTH` call — negligible, and only paid once per connection lifetime in the common case.
- **Real per-command ACL overhead now exists (updated)**: every command after authentication takes an `AclManager` read-lock and a `HashMap`/`HashSet` lookup via `can_execute_command`/`can_access_key` (§4.1) — small, but no longer zero as the old doc stated; this is a real, permanent per-command cost on every connection now, not just at `AUTH` time.
- **Allocator stats are cheap but not free**: unchanged — `epoch::advance()` triggers jemalloc to refresh its internal counters, more than a simple atomic load; only invoked from `INFO`, not a hot-path command.
- **TLS has real handshake and per-byte I/O cost now that it's wired up**: the `rustls` handshake (§4.4) is genuine CPU work paid once per TLS connection; ongoing traffic either goes through real `rustls` encrypt/decrypt (the safe, intended path) or — per the §4.4 bug — bypasses encryption entirely once `is_ktls_active` is (incorrectly) set, which is *faster* than real encryption precisely because it isn't doing any. Do not read that speed as a feature.

---

## 7. Future Improvements

- **CRITICAL, was Medium — fix or disable the kTLS plaintext-bypass bug before `--tls-port` is used anywhere (§2.4/§4.4).** This is now the single most urgent item in this entire document: enabling `--tls-port` produces connections that complete a real TLS handshake and then silently send all application data unencrypted, because `is_ktls_active` is set from a `TCP_ULP` `setsockopt` success alone, with no actual key-install call ever made. The fastest safe fix is the smallest one: stop setting `is_ktls_active = true` from `enable_ktls`'s current (incomplete) implementation — always take the real `rustls` encrypt/decrypt path in `read_plaintext`/`write_plaintext` until `enable_ktls` is extended to also perform the `setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` key-install call. Until one of these lands, `--tls-port` should not be documented or offered as a secure option.
- **High — remove plaintext password storage now that hashing exists (§2.3).** `ACL SETUSER user >password` still pushes the plaintext into `AclUser.passwords` in addition to hashing it into `password_hashes` — the hashing work was done but the vulnerability it was meant to close (plaintext credentials sitting in process memory / reachable via a core dump) was not actually closed. Stop populating `passwords` from the `>` rule (keep it only for backward-compatible reads of already-stored plaintext, if any migration path requires that), and have `check_auth` compare only against `password_hashes`.
- **High — replace the fixed-salt SHA1 scheme with a real per-user-salted slow hash (§2.3).** `hash_password` uses one hardcoded global salt (`"rudis_acl_salt_v1:"`) shared across every user and every Rudis instance, with a fast general-purpose hash (SHA1) that has no work-factor resistance to offline brute force. A per-user random salt plus Argon2id (or at minimum bcrypt/scrypt/PBKDF2 with a real iteration count) closes both weaknesses — the fixed-salt SHA1 hash is barely better than plaintext against a determined offline attacker.
- **Medium — extend `ACL SETUSER` to support command categories and pub/sub channel patterns (§4.2).** `+@read`/`-@write`-style category tokens and `&channel:*` pub/sub ACL rules are still silently accepted and ignored, exactly as before this update — only bare per-command and per-key-prefix rules are real. Either implement categories/channels or make `ACL SETUSER` reject unrecognized rule tokens with an error, so a deployment can't believe it applied a restriction that was silently dropped.
- **Low — expose jemalloc heap-profiling/dump capability, not just aggregate stats (§4.3)**, if deep memory-leak/fragmentation debugging in production ever becomes a need — `tikv-jemalloc-ctl` supports profiling hooks beyond the stats-only reads currently used. Unchanged by this update.
