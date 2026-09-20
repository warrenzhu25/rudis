# Component 15: Security, Memory Allocator & TLS (Design)

> **Source Files**: `src/acl.rs`, `src/allocator.rs`, `src/tls.rs`


---

### 1. Architectural Purpose & Scope

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

### 2. Key Invariants & Concurrency Constraints

1. **One `AclManager` per listening port, not global**: `PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>>`, looked up via `get_acl_for_port(port)`. Every shard thread serving the same port shares the same `Arc<RwLock<AclManager>>` — this is, like `BlockHub` (Component 06), a deliberate exception to the shared-nothing/zero-lock architecture, needed because auth state has to be consistent across every shard's independently-accepted connections on that port.
2. **Authentication is enforced, and authorization is now real too (updated).** `execute_command` gates on `authenticated: bool` exactly as before (`-NOAUTH` for anything but `AUTH`/`HELLO`/`QUIT` while unauthenticated), but immediately after that gate it now looks up the authenticated user's `AclUser` by `auth_user` and calls two new real methods before dispatching: `can_execute_command(cmd_name)` (checks `disallowed_commands`/`allowed_commands` depending on `all_commands`, always allowing `PING`/`RESET`/`QUIT`/`AUTH`/`HELLO`) and, if the command has a primary key, `can_access_key(key)` (checks `allowed_key_patterns`, a `prefix*`/exact-match list, unless `all_keys`). A denied command gets a real `-NOPERM` reply and returns without executing (§4.1). The pipelined squash-eligibility check (`execute_commands_squashed`) also consults these two methods per command — a command an ACL would deny just falls back to the sequential path, where the real `-NOPERM` denial happens.
3. **Passwords are now hashed, but plaintext storage was not removed — this is a real gap, not full resolution.** `AclUser` gained a `password_hashes: Vec<String>` field, and `hash_password` computes `SHA1("rudis_acl_salt_v1:" + password)`. But `ACL SETUSER user >password` still pushes the plaintext into `passwords` *and* the hash into `password_hashes` (§4.2) — the plaintext field was never removed, so anyone who could previously read plaintext passwords from memory/a core dump still can. The hash itself is also weak by password-hashing standards: SHA1 is a fast general-purpose hash (not a slow KDF like Argon2/bcrypt/scrypt, so no work-factor resistance to offline brute force), and the salt (`"rudis_acl_salt_v1:"`) is a single hardcoded constant shared by every user and every deployment, not a per-user random salt — identical passwords across users or across a fleet of Rudis instances produce identical hashes, and the fixed salt is trivially precomputable into a rainbow table once known. `check_auth` accepts a match against either the plaintext or the hash (`§4.1`), so both weaknesses are live simultaneously.
4. **TLS is now genuinely wired up — and its kTLS fast-path has a real, severe bug: it silently transmits plaintext.** A `--tls-port` listener now exists (§4.4) and performs a real `rustls` handshake via `TlsSession::handshake_monoio`. After a successful handshake, it unconditionally attempts `enable_ktls`, which only calls `setsockopt(IPPROTO_TCP, TCP_ULP, "tls")` — attaching the kernel's TLS upper-layer-protocol module — and, if that syscall merely *succeeds* (which it will on any Linux host with the `tls` kernel module loadable, regardless of whether any key material was ever installed), sets `is_ktls_active = true`. Both `TlsSession::read_plaintext` and `write_plaintext` then branch on `is_ktls_active`: when true, they read/write **raw socket bytes directly, with no rustls encryption/decryption at all**, on the assumption the kernel is doing it. But the second, actually-required `setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` call that installs the negotiated key into the kernel socket is still missing (this exact gap was already flagged when the code was dead — see §4.4) — so the kernel never encrypts anything, and **every byte of application data after the handshake goes out on the wire in cleartext**, while both client and server believe they completed a real TLS session. This is worse than TLS simply not existing: a client connecting to `--tls-port` gets a real, correctly-negotiated handshake and then a false sense of security for the rest of the connection.
5. **Allocator stats are read-only telemetry**, gated behind no lock beyond jemalloc's own internal epoch counter (`tikv_jemalloc_ctl::epoch::advance()`), safely callable from any thread without coordination with the rest of Rudis.

---

### 3. Performance Characteristics

- **Auth check cost**: one `RwLock::read()` acquisition plus a linear scan over `passwords`/`password_hashes` (typically 0-1 entries each) plus one SHA1 computation per `AUTH` call — negligible, and only paid once per connection lifetime in the common case.
- **Real per-command ACL overhead now exists (updated)**: every command after authentication takes an `AclManager` read-lock and a `HashMap`/`HashSet` lookup via `can_execute_command`/`can_access_key` (§4.1) — small, but no longer zero as the old doc stated; this is a real, permanent per-command cost on every connection now, not just at `AUTH` time.
- **Allocator stats are cheap but not free**: unchanged — `epoch::advance()` triggers jemalloc to refresh its internal counters, more than a simple atomic load; only invoked from `INFO`, not a hot-path command.
- **TLS has real handshake and per-byte I/O cost now that it's wired up**: the `rustls` handshake (§4.4) is genuine CPU work paid once per TLS connection; ongoing traffic either goes through real `rustls` encrypt/decrypt (the safe, intended path) or — per the §4.4 bug — bypasses encryption entirely once `is_ktls_active` is (incorrectly) set, which is *faster* than real encryption precisely because it isn't doing any. Do not read that speed as a feature.

---
