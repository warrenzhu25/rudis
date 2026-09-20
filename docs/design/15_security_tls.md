# Component 15: Security, Memory Allocator & TLS (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/acl.rs, src/allocator.rs, src/tls.rs`  
> **Implementation Reference**: [`docs/internal/15_security_tls.md`](../internal/15_security_tls.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Traditional in-memory datastores encounter severe scalability barriers on modern multi-core, high-throughput cloud hardware. Single-threaded architectures (such as Redis) saturate a single CPU core while leaving the remaining 95%+ of server cores idle. Multi-threaded mutex architectures (such as Memcached) suffer from heavy spinlock contention, CPU cache line bouncing, and global memory allocator lock bottlenecks.

### 1.2 The Rudis Solution
Rudis implements the **Thread-Per-Core (Shared-Nothing)** architectural paradigm natively on Linux `io_uring` via Monoio. Each physical CPU core owns its own isolated event loop, its own thread-local memory database, and its own kernel `SO_REUSEPORT` listener. Operations on local keys execute in nanoseconds with zero locks, zero atomic operations, and zero cross-core cache invalidations.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model
Three loosely related concerns bundled under one component because each is a cross-cutting,
port- or process-wide facility rather than a per-shard data type: per-user/per-command Access
Control Lists (ACLs) gating command and key access, `jemalloc` as the process-wide global
allocator with a thread-local small-object recycling pool layered on top, and `rustls`-based
TLS termination on an optional dedicated listener port.

### 2.2 Design Rationale (The "Why")

**ACLs.** A production deployment needs to run more than one credential and more than one
privilege level against the same server — an application user that can only touch its own key
prefix, an operations user that can run `INFO`/`CLIENT`, and so on. Rudis implements a
Redis-compatible subset of this model: named users (`AclUser`) with an enabled/disabled flag,
password credentials, a command allow/deny set, and a key-pattern allow list, managed through
`AUTH` and the `ACL SETUSER`/`GETUSER`/`LIST`/`USERS`/`DELUSER`/`WHOAMI`/`CAT` subcommands
(internal document §2). This is deliberately a *subset* of real Redis ACLs — see §2.3.3 for
exactly which parts are real versus not yet implemented.

**Why `jemalloc` as the global allocator.** A thread-per-core, high-throughput server issues a
very large number of small, short-lived allocations (RESP argument buffers, per-command
temporary `Vec`s, list/hash/set/zset element storage) concurrently across every core. The
platform default allocator (glibc `malloc` on Linux) is a reasonable general-purpose allocator
but is not tuned for this access pattern: it is more prone to heap fragmentation under many
small alternating alloc/free cycles, and its internal locking is not designed around a
one-thread-per-core workload. `jemalloc` (via `tikv-jemallocator`, set as `#[global_allocator]`
in `src/lib.rs`) uses per-thread arenas and size-class segregation that fit a thread-per-core
process well, and it exposes rich, cheap-to-read runtime statistics (`tikv-jemalloc-ctl`) that
Rudis surfaces through `INFO`'s memory section — visibility a default system allocator does not
provide without extra tooling. Rudis additionally layers a small, application-level object pool
(`SmallCollectionArena`, in `allocator.rs`) *on top of* `jemalloc`: for the hottest collection
commands (`LPUSH`/`LPOP`, `HSET`/`HDEL`, `SADD`/`SREM`, `ZADD`/`ZREM`), it recycles the backing
`Vec`/`VecDeque` containers themselves across calls rather than relying on the allocator to
service each container churn — a request satisfied entirely by an in-process pool never reaches
`jemalloc` at all. This is not a *replacement* allocator; it is a per-shard, thread-local cache
that reduces how often the global allocator is invoked for a specific, high-frequency pattern.

**Why TLS via `rustls`, and why attempt kTLS.** `rustls` is a memory-safe, pure-Rust TLS
implementation with no OpenSSL dependency, a good fit for a Rust codebase that otherwise avoids
C dependencies where practical, and it integrates cleanly with `monoio`'s async I/O model via
manual handshake driving (§4.4 of the internal document). Encrypting and decrypting every byte
of a connection in userspace is real, recurring CPU work; Linux kernel TLS (kTLS, via
`TCP_ULP`) exists specifically to let the kernel (or, on supporting NICs, hardware) perform that
work instead, avoiding a userspace copy/crypto pass per byte. Rudis's `tls.rs` module attempts
to promote a socket to kTLS after every handshake for exactly this reason — but as documented
in §2.3.4, that promotion is not currently wired to take effect, so today the performance
benefit kTLS is meant to provide is aspirational, not realized.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **One `AclManager` per listening port, not global**: `PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>>`, looked up via `get_acl_for_port(port)`. Every shard thread serving the same port shares the same `Arc<RwLock<AclManager>>` — this is, like `BlockHub` (Component 06), a deliberate exception to the shared-nothing/zero-lock architecture, needed because auth state has to be consistent across every shard's independently-accepted connections on that port.
2. **Authentication is enforced, and per-command/per-key authorization is real.** `execute_command` gates on `authenticated: bool` (`-NOAUTH` for anything but `AUTH`/`HELLO`/`QUIT` while unauthenticated); once authenticated, it looks up the connection's `AclUser` by `auth_user` and calls two methods before dispatching: `can_execute_command(cmd_name)` (checks `disallowed_commands`/`allowed_commands` depending on `all_commands`, always allowing `PING`/`RESET`/`QUIT`/`AUTH`/`HELLO`) and, if the command has a primary key, `can_access_key(key)` (checks `allowed_key_patterns`, a `prefix*`/exact-match list, unless `all_keys`). A denied command gets a real `-NOPERM` reply and returns without executing (§3.1). The pipelined squash-eligibility check (`execute_commands_squashed`) also consults these two methods per command — a command an ACL would deny falls back to the sequential path, where the real `-NOPERM` denial happens.
3. **Passwords are hashed, but plaintext storage was not removed — a real, live gap.** `AclUser` carries a `password_hashes: Vec<String>` field alongside `passwords`, and `hash_password` computes `SHA1("rudis_acl_salt_v1:" + password)`. `ACL SETUSER user >password` pushes the plaintext into `passwords` *and* the hash into `password_hashes` (§3.2) — the plaintext field is never cleared, so anything that could read plaintext passwords from process memory or a core dump still can. The hash itself is also weak by password-hashing standards: SHA1 is a fast general-purpose hash (not a slow KDF like Argon2/bcrypt/scrypt, so it offers no work-factor resistance to offline brute force), and the salt (`"rudis_acl_salt_v1:"`) is a single hardcoded constant shared by every user and every deployment rather than a per-user random salt — identical passwords across users or across a fleet of Rudis instances produce identical hashes, and the fixed salt is trivially precomputable into a rainbow table once known. `check_auth` accepts a match against either the plaintext or the hash (§3.1), so both weaknesses are live simultaneously.
4. **TLS is wired up end-to-end for the safe (userspace `rustls`) path; kTLS offload is attempted but never actually activated.** A `--tls-port` listener performs a real `rustls` handshake via `TlsSession::handshake_monoio` (§3.4). After a successful handshake, it unconditionally calls `enable_ktls`, which issues `setsockopt(IPPROTO_TCP, TCP_ULP, "tls")` — attaching the kernel's TLS upper-layer-protocol module, and succeeding on any Linux host where that kernel module is loadable, independent of whether any key material was ever installed. However, the *result* of `enable_ktls` is intentionally discarded (`let _ = enable_ktls(raw_fd);`), and `TlsSession::is_ktls_active` is then set unconditionally to `false` regardless of outcome. The second, actually-required call that would install the negotiated cipher/key/IV into the kernel socket (`setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)`) is not implemented at all. Net effect: `read_plaintext`/`write_plaintext` always take the real `rustls`-mediated encrypt/decrypt branch — every byte of application data over `--tls-port` is genuinely encrypted — but the "line-rate zero-copy kTLS" behavior described in `tls.rs`'s own module documentation comment is not live in the current build; the `enable_ktls` call and the `is_ktls_active` field are present but functionally inert scaffolding for a feature that is not yet wired to actually engage.
5. **Allocator stats are read-only telemetry**, gated behind no lock beyond jemalloc's own internal epoch counter (`tikv_jemalloc_ctl::epoch::advance()`), safely callable from any thread without coordination with the rest of Rudis.
6. **`requirepass` (config-file directive or `CONFIG SET requirepass`) does not by itself enable authentication enforcement — a verified, real gap.** Per-connection auth gating is decided once, at accept time, from `HAS_CUSTOM_ACL` (an atomic flag set only by a real `ACL SETUSER`/`ACL DELUSER` call) and the default user's `nopass` flag. Neither `CONFIG SET requirepass` nor the `requirepass` config-file directive at startup goes through `ACL SETUSER`, so neither one flips `HAS_CUSTOM_ACL` to `true` or clears the default user's `nopass` flag — both stay in their permissive initial state, and new connections continue to start pre-authenticated regardless of a configured `requirepass` value. See internal document §2 for the exact code path. The only mechanism that reliably enforces authentication today is an explicit `ACL SETUSER default ... >password` (or `off`).
7. **ACL rules are enforced only at the granularity of individual commands and simple key prefixes — command *categories* (`+@read`/`-@write`/...) and pub/sub channel patterns (`&channel:*`) are parsed as opaque tokens but silently have no effect.** `ACL CAT` returns a fixed, hardcoded list of category name strings for introspection purposes only; it is not backed by a real per-command category table, and `ACL SETUSER`'s rule parser has no branch that recognizes a `@category` or `&pattern` token, so such a rule is accepted (no error) but never changes what the user can do. A deployment that issues `ACL SETUSER app -@all +@read` believes it granted read-only access; in the current implementation, that user retains whatever command set it had before the unrecognized token was silently skipped.

---

## 3. High-Level Architecture & Workflow Diagram

```
Client TLS Handshake ──► rustls (Userspace, monoio-driven async handshake)
                                    │
                          enable_ktls() attempted, best-effort
                          (TCP_ULP attach only; result discarded)
                                    │
                          is_ktls_active = false  (always, current build)
                                    │
                          All application data ──► rustls encrypt/decrypt ──► Wire
                          (the kTLS zero-copy path exists as inert scaffolding, not
                           yet reachable in the current build — see §2.3.4)
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Auth check cost**: one `RwLock::read()` acquisition plus a linear scan over `passwords`/`password_hashes` (typically 0-1 entries each) plus one SHA1 computation per `AUTH` call — negligible, and only paid once per connection lifetime in the common case.
- **Real per-command ACL overhead now exists (updated)**: every command after authentication takes an `AclManager` read-lock and a `HashMap`/`HashSet` lookup via `can_execute_command`/`can_access_key` (§4.1) — small, but no longer zero as the old doc stated; this is a real, permanent per-command cost on every connection now, not just at `AUTH` time.
- **Allocator stats are cheap but not free**: unchanged — `epoch::advance()` triggers jemalloc to refresh its internal counters, more than a simple atomic load; only invoked from `INFO`, not a hot-path command.
- **TLS pays a real handshake cost, then a real per-byte encrypt/decrypt cost on every connection**: the `rustls` handshake (§4.4) is genuine CPU work paid once per TLS connection; because `is_ktls_active` never becomes `true` in the current build (§2.3.4), all ongoing traffic over `--tls-port` is encrypted/decrypted in userspace by `rustls` on every read and write — there is currently no faster kTLS path to fall back to, correctly or otherwise.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/15_security_tls.md`**](../internal/15_security_tls.md): Low-level implementation and code reference.
* **Source Files**: `src/acl.rs, src/allocator.rs, src/tls.rs`
