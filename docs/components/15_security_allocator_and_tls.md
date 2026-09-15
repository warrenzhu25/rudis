# Component 15: Security, Memory Allocator & TLS (`src/acl.rs`, `src/allocator.rs`, `src/tls.rs`)

## 1. Architectural Purpose & Scope

Three unrelated system-services modules bundled under one doc:
1. **Access control (`src/acl.rs`)**: a per-port, multi-user *authentication* store (`AUTH user pass`, `ACL SETUSER/GETUSER/LIST/USERS/DELUSER/WHOAMI`) modeled loosely on Redis ACL syntax. It is **authentication only** — see §2.2, there is no per-command or per-key authorization enforcement anywhere in the codebase.
2. **Jemalloc telemetry (`src/allocator.rs`)**: read-only statistics via `tikv-jemalloc-ctl`, surfaced through `INFO`'s memory section. No profiling/heap-dump capability.
3. **TLS certificate/handshake plumbing (`src/tls.rs`)**: a real, correctly-written `rustls` handshake wrapper with genuine in-memory self-signed cert generation (`rcgen`) and a genuine `kTLS` `TCP_ULP` `setsockopt` call — but see §2.4, **none of it is called from anywhere else in the codebase.** It is dead code with no connection-handling call site.

---

## 2. Key Invariants & Concurrency Constraints

1. **One `AclManager` per listening port, not global**: `PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>>`, looked up via `get_acl_for_port(port)`. Every shard thread serving the same port shares the same `Arc<RwLock<AclManager>>` — this is, like `BlockHub` (Component 06), a deliberate exception to the shared-nothing/zero-lock architecture, needed because auth state has to be consistent across every shard's independently-accepted connections on that port.
2. **Authentication is enforced; authorization is not.** `handle_connection` tracks one `authenticated: bool` per connection (initialized to `true` only if the default user has `nopass`) and rejects every command except `AUTH`/`HELLO`/`QUIT` with `-NOAUTH` while `false`. Once a user authenticates as *any* user, **every command is allowed** — `AclUser::all_commands` and `all_keys` are stored and rendered back by `ACL GETUSER`/`ACL LIST`, but grepping the whole codebase confirms they are never read anywhere except those two display paths. A user created with `ACL SETUSER bob on >pw -@all` still gets `+@all` treatment operationally.
3. **Passwords are stored and compared in plaintext**, not hashed. `AclUser.passwords: Vec<String>` holds exactly what was passed to `ACL SETUSER user >password`; `check_auth` does a plain `==` string comparison. There is no SHA-256 (or any) hashing anywhere in `acl.rs`.
4. **TLS/kTLS code exists but is unreachable.** `src/tls.rs` defines `generate_self_signed_cert`, `create_server_config`, `load_certs_and_key_from_files`, `enable_ktls`, and `TlsSession`, all fully implemented — but grepping `main.rs`, `server.rs`, and `connection.rs` for `tls`/`TlsSession`/`enable_ktls` turns up zero matches. There is no `--tls-port` CLI flag, no TLS listener, nothing that ever constructs a `TlsSession`. Every client connection is plain TCP.
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
                                     │
                     (sets `authenticated = true`; no further gate exists —
                      every subsequent command runs regardless of all_commands/all_keys)


                 INFO command (memory section)
                                     │
                     allocator::format_memory_info(used_mem, max_mem, ...)
                                     │
                     allocator::get_allocator_stats()  →  tikv_jemalloc_ctl::stats::*


                 tls.rs: generate_self_signed_cert / create_server_config /
                         TlsSession::complete_handshake / enable_ktls(TCP_ULP)
                                     │
                              (no caller anywhere — dead code)
```

### The real `AclUser` / `AclManager` (`src/acl.rs`)

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    pub passwords: Vec<String>,
    pub nopass: bool,
    pub all_commands: bool,
    pub all_keys: bool,
}
```

No `allowed_commands: HashSet<String>`, no `allowed_key_patterns: Vec<String>`, no command-category (`+@read`/`-@write`) support, no key-glob (`~cached:*`) support — this is the entire struct. `AclManager` is just:

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

### 4.1 Authentication (`AclManager::check_auth`) — the entire enforcement surface

```rust
pub fn check_auth(&self, username: Option<&str>, password: &str) -> Result<String, &'static str> {
    let user_name = username.unwrap_or("default");
    if let Some(user) = self.users.get(user_name) {
        if !user.enabled {
            return Err("WRONGPASS User is disabled");
        }
        if user.nopass || user.passwords.iter().any(|p| p == password) {
            Ok(user_name.to_string())
        } else {
            Err("WRONGPASS invalid username-password pair or user is disabled.")
        }
    } else {
        Err("WRONGPASS invalid username-password pair or user is disabled.")
    }
}
```

Called from `connection.rs`'s `Command::Auth` handler:

```rust
Command::Auth { username, password } => {
    let uname = username.as_deref().unwrap_or("default");
    let pass = password.as_str();
    let acl = crate::acl::get_acl_for_port(router.port);
    let acl_guard = acl.read().unwrap();
    if let Ok(authed_user) = acl_guard.check_auth(Some(uname), pass) {
        *authenticated = true;
        *auth_user = authed_user;
        out.extend_from_slice(b"+OK\r\n");
    } else {
        out.extend_from_slice(b"-WRONGPASS invalid username-password pair or user is disabled.\r\n");
    }
    false
}
```

And gated at the very top of command dispatch, before the match on command type:

```rust
if !*authenticated && !matches!(cmd, Command::Auth { .. } | Command::Hello { .. } | Command::Quit) {
    out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
    return false;
}
```

That's the entirety of the security gate: pass or fail authentication once, then run anything. There is no second check anywhere that consults `auth_user`'s `AclUser` record again during command dispatch.

### 4.2 `ACL SETUSER` — parses real Redis ACL rule tokens, but only recognizes a handful

```rust
for rule in rules {
    if rule == "on" { user.enabled = true; }
    else if rule == "off" { user.enabled = false; }
    else if rule == "nopass" { user.nopass = true; user.passwords.clear(); }
    else if let Some(p) = rule.strip_prefix('>') { user.nopass = false; user.passwords.push(p.to_string()); }
    else if let Some(p) = rule.strip_prefix('<') { user.passwords.retain(|pass| pass != p); }
    else if rule == "+@all" || rule == "+all" { user.all_commands = true; }
    else if rule == "-@all" || rule == "-all" { user.all_commands = false; }
    else if rule == "~*" || rule == "allkeys" { user.all_keys = true; }
}
```

A rule like `+@read`, `-@write`, `~cached:*`, or `&channel:*` is silently accepted as a token but matches none of these branches and does **nothing** — it neither errors nor changes any state. `ACL SETUSER bob on >pw +@read ~cached:*` produces a user identical to `ACL SETUSER bob on >pw` (both `all_commands: false, all_keys: false` by the `or_insert_with` default), and — per §2.2 — even that has no runtime effect on what `bob` can execute once authenticated.

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

### 4.4 TLS/kTLS — real cryptographic plumbing, zero callers

```rust
pub fn generate_self_signed_cert(subject_alt_names: Vec<String>) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut params = rcgen::CertificateParams::new(subject_alt_names)...;
    let key_pair = rcgen::KeyPair::generate()...;
    let cert = params.self_signed(&key_pair)...;
    Ok((cert.der().to_vec(), key_pair.serialize_der()))
}

pub fn enable_ktls(raw_fd: RawFd) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        const IPPROTO_TCP: libc::c_int = 6;
        const TCP_ULP: libc::c_int = 31;
        let ulp_name = b"tls\0";
        let ret = unsafe {
            libc::setsockopt(raw_fd, IPPROTO_TCP, TCP_ULP, ulp_name.as_ptr() as *const libc::c_void, ulp_name.len() as libc::socklen_t)
        };
        if ret == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }
    ...
}
```

This is genuinely correct as far as it goes: `TlsSession::complete_handshake` drives a real `rustls::ServerConnection` handshake loop (`wants_write`/`write_tls`/`wants_read`/`read_tls`/`process_new_packets`), and on success attempts `enable_ktls`. **One real gap even within this unused code**: setting `TCP_ULP` to `"tls"` only loads the kernel's TLS ULP module and prepares the socket to accept key material — actual kTLS offload additionally requires a second `setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` call installing the negotiated cipher/key/IV extracted from the completed `rustls` session. That second call does not exist anywhere in this file. So even if this code *were* wired up, `enable_ktls` succeeding would not actually hand encryption over to the kernel — `is_ktls_active = true` would be set, but no keys would ever reach the kernel socket, meaning `rustls` would still be doing all the encryption in userspace regardless of the flag's value. Combined with there being no caller at all, TLS support should be considered entirely non-functional today, not merely "userspace-only."

---

## 5. Cross-Component Interactions

- **`src/connection.rs`**: the only real caller of anything in this trio — `get_acl_for_port` (auth gate + `AUTH`/`ACL *` command handlers) and `allocator::format_memory_info` (via `INFO`). Nothing in `connection.rs` references `src/tls.rs` at all.
- **`src/server.rs` / `src/main.rs`**: no references to `acl`, `allocator`, or `tls` modules at all beyond what's reachable transitively through `connection.rs`'s own imports — there is no TLS listener setup and no separate allocator-stats background task.
- **`src/tiering.rs`**: does not call `allocator::get_allocator_stats()` — memory-pressure decisions for auto-tiering are driven by a separately tracked `used_memory` estimate on `RudisTable` (see Component 05), not by live jemalloc RSS figures.

---

## 6. Performance Characteristics

- **Auth check cost**: one `RwLock::read()` acquisition plus a linear scan over `passwords: Vec<String>` (typically 0-1 entries) per `AUTH` call — negligible, and only paid once per connection lifetime in the common case.
- **Zero enforcement cost elsewhere**: because there is no per-command authorization check (§2.2), there is also no per-command ACL overhead — every command after authentication runs at full speed with no additional gate.
- **Allocator stats are cheap but not free**: `epoch::advance()` triggers jemalloc to refresh its internal counters, which is more than a simple atomic load; calling `get_allocator_stats()` in a hot loop would be measurably more expensive than reading a plain counter, though it's only actually invoked from `INFO`, which is not a hot-path command.
- **TLS/kTLS have no runtime performance characteristics to report** — the code is never executed.
