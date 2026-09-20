# Component 10: Kernel Bypass & Zero-Copy Networking (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/xdp.rs, src/zerocopy.rs`  
> **High-Level Design Spec**: [`docs/design/10_kernel_bypass_xdp.md`](../design/10_kernel_bypass_xdp.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/xdp.rs` | Core implementation and logic | Primary data structures and algorithms |
| `src/zerocopy.rs` | Core implementation and logic | Primary data structures and algorithms |

---

### 3. Component Architecture

```
                    XDP.* Redis commands (client-issued, e.g. redis-cli)
                                        │
                                        ▼
                     crate::xdp::get_xdp_engine()  (global Arc<XdpEngine>)
                                        │
                ┌───────────────────────┼───────────────────────┐
                ▼                       ▼                       ▼
        XDP.RULEADD/DEL/LIST    XDP.STATS / XDP.INFO      XDP.PACKET <bytes>
       (CIDR allow/drop table)   (atomic counters)      (manually parses the
                                                          given bytes as an
                                                          Ethernet/IPv4 frame
                                                          and runs the same
                                                          filter+rate-limit
                                                          pipeline used above)

        src/zerocopy.rs: RegisteredBufferPool + ZeroCopyEngine
        — fully implemented, fully unit-tested, ZERO callers anywhere
          else in the codebase.
```

#### `XdpEngine` (`src/xdp.rs`) — the real struct

```rust
pub struct XdpEngine {
    pub ifname: String,
    pub mode: XdpMode,
    pub frame_size: usize,          // 2048, cosmetic (reported by XDP.INFO only)
    pub num_frames: usize,          // 4096, cosmetic
    pub rules: RwLock<Vec<XdpRule>>,
    pub next_rule_id: AtomicU32,
    pub rate_limiters: RwLock<HashMap<u32, TokenBucket>>,  // keyed by source IPv4, as u32
    pub default_rate_limit: f64,     // 100_000.0 tokens/sec
    pub default_rate_capacity: f64,  // 50_000.0 burst capacity
    pub rx_packets: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub dropped_packets: AtomicU64,
    pub redirected_packets: AtomicU64,
    pub pass_packets: AtomicU64,
    pub rate_limit_drops: AtomicU64,
}
```

There is no `XdpSocket`, `XdpUmem`, `XdpRxRing`/`XdpTxRing`/`XdpFillRing`/`XdpCompRing`, and no
`xsk_fd: RawFd` anywhere in the file — those were invented in the prior version of this
document. The real per-rule type is:

```rust
pub struct XdpRule {
    pub id: u32,
    pub action: XdpAction,   // Pass | Drop | Redirect | Tx
    pub cidr: String,
    pub network: u32,        // pre-computed via parse_cidr for fast masking
    pub netmask: u32,
}
```

#### `TokenBucket` (`src/xdp.rs`) — the real rate limiter, one instance per source IP

```rust
pub struct TokenBucket {
    pub tokens: f64,
    pub capacity: f64,
    pub refill_rate: f64,   // tokens per second
    pub last_update: Instant,
}

impl TokenBucket {
    pub fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        if self.tokens >= 1.0 { self.tokens -= 1.0; true } else { false }
    }
}
```

Standard continuous-refill token bucket, costing exactly 1 token per packet regardless of
packet size (the prior doc's `TokenBucketLimiter::allow_packet(&mut self, packet_len: u64)`,
which spent tokens proportional to byte length, does not exist — the real bucket is
per-*packet*, not per-*byte*). Buckets are created lazily per source IP the first time that IP
is seen (`rate_limiters.entry(ip).or_insert_with(...)`), never evicted — a real, if minor,
unbounded-memory-growth characteristic worth knowing (one `TokenBucket` per distinct source IP
ever seen, for the lifetime of the process).

#### `src/zerocopy.rs` — the real (but unused) types

```rust
pub struct RegisteredBufferPool {
    ptr: *mut u8,
    layout: Layout,
    slot_size: usize,
    slot_count: usize,
    free_slots: Vec<usize>,
    iovecs: Vec<libc::iovec>,
    stats: Arc<ZeroCopyStats>,
}

pub struct ZeroCopyEngine {
    stats: Arc<ZeroCopyStats>,
}
```

Note these are **separate** structs — `ZeroCopyEngine` does not own a `buffer_pool` field the
way the prior doc claimed; a caller would need to wire the two together manually (and no caller
does).

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `XdpEngine::process_packet` — manual, CPU-side Ethernet/IPv4 parsing

```rust
pub fn process_packet(&self, packet: &[u8]) -> XdpAction {
    self.rx_packets.fetch_add(1, Ordering::Relaxed);
    self.rx_bytes.fetch_add(packet.len() as u64, Ordering::Relaxed);
    if packet.is_empty() {
        self.dropped_packets.fetch_add(1, Ordering::Relaxed);
        return XdpAction::Drop;
    }
    // Detects either a 14-byte Ethernet header (checking bytes[12..14] == 0x0800 for IPv4)
    // followed by an IPv4 packet, OR a bare IPv4 packet (checking the top nibble of byte 0 == 4).
    // Extracts source IP (bytes 12..16 of the IPv4 header) and, for TCP (protocol byte == 6),
    // the destination port — computed but currently unused (`let _ = payload_offset;`).
    ...
    if let Some(ip) = src_ip {
        // 1. Linear scan of CIDR rules (first match wins: Drop/Pass/Redirect/Tx)
        // 2. Per-source-IP TokenBucket rate limit (lazily created)
    }
    // 3. Anything not matched by a rule or rate-limited is unconditionally Redirect'd
    //    (the doc comment says "Redis port 6379 or cluster bus", but the code does not
    //    actually inspect the destination port to decide this — it's an unconditional default)
    self.redirected_packets.fetch_add(1, Ordering::Relaxed);
    XdpAction::Redirect
}
```

The byte-offset parsing itself is real and reasonably careful (bounds-checked slice access,
handles both frame shapes), but it operates on whatever byte slice was handed to it — there is
no code anywhere that reads this slice from an actual NIC, a raw socket, or an XDP program.

#### 4.2 The only caller: `Command::XdpPacket` in `connection.rs`

```rust
Command::XdpPacket(payload) => {
    let action = crate::xdp::get_xdp_engine().process_packet(&payload);
    let s = format!("+{}\r\n", action);
    out.extend_from_slice(s.as_bytes());
    false
}
```

`payload` is a `Bytes` argument taken directly from the client's `XDP.PACKET` command — a
Redis client can construct an arbitrary byte string and ask the server to run it through the
simulated filter/rate-limit pipeline and report back `+PASS`, `+DROP`, `+REDIRECT`, or `+TX`.
This confirms the design: it's a **testable simulation of what an XDP filter's logic would do**,
reachable as an ordinary command, not a hook into real packet ingress. The companion commands
(`XDP.INFO`, `XDP.RULEADD`, `XDP.RULEDEL`, `XDP.RULELIST`, `XDP.STATS`) manage the same global
rule table and read back the same atomic counters — all of it is only ever exercised by
whatever a client explicitly sends to `XDP.PACKET`, never by this server's own real 6379
listener traffic.

#### 4.3 `ZeroCopyEngine::send_zc` — real syscall usage, verified unreachable

```rust
pub fn send_zc(&self, fd: RawFd, data: &[u8]) -> io::Result<usize> {
    if data.is_empty() { return Ok(0); }
    let flags = if data.len() >= PAGE_SIZE {
        libc::MSG_NOSIGNAL | MSG_ZEROCOPY
    } else {
        libc::MSG_NOSIGNAL
    };
    let ret = unsafe { libc::send(fd, data.as_ptr() as *const libc::c_void, data.len(), flags) };
    if ret >= 0 {
        // record zc_send_calls / zc_bytes_sent, return Ok(sent)
    } else {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOBUFS) || flags & MSG_ZEROCOPY != 0 {
            // fall back to a plain blocking libc::send with just MSG_NOSIGNAL
        }
        Err(err)
    }
}
```

This is correct, idiomatic use of Linux's real zero-copy send path (including the documented
requirement to poll `MSG_ERRQUEUE` for completion notifications in a full implementation — which
this code does *not* do; there is no `MSG_ERRQUEUE`/`recvmsg` polling anywhere in the file, so
even if this were wired up, the caller would have no way to know when the kernel has actually
finished the zero-copy DMA and it's safe to reuse/free the source buffer). `fd: RawFd` is a raw
file descriptor — but `connection.rs`'s actual sockets are `monoio::net::TcpStream` objects
whose file descriptors aren't exposed or passed to this function anywhere. There is no
integration point today.

`build_io_uring_send_zc` similarly constructs a real `io_uring::opcode::SendZc` entry using the
`io-uring` crate (a genuine dependency in `Cargo.toml`, used elsewhere for direct-I/O tiering —
see Component 07), but nothing ever calls it or submits the resulting entry to a ring.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: the only real integration point — six `Xdp*` `Command` variants
  (`XdpInfo`, `XdpRuleAdd`, `XdpRuleDel`, `XdpRuleList`, `XdpStats`, `XdpPacket`) are dispatched
  here, each just forwarding to `crate::xdp::get_xdp_engine()`. **Not connected**: the real
  `handle_connection`/accept-loop path in `server.rs` (Component 01) always uses `monoio`'s
  `SO_REUSEPORT` `TcpListener`, regardless of `XdpEngine`'s state.
- **`src/resp.rs`**: parses the six `XDP.*` command names/arguments into the `Command` enum
  variants above (including mapping `"DROP"`/`"PASS"`/`"REDIRECT"`/`"TX"` strings to
  `XdpAction`).
- **`src/zerocopy.rs`**: no cross-component interactions to document — verified zero callers
  outside its own `#[cfg(test)]` module.

---

### 7. Future Improvements

- **High-priority decision, not a fix: decide whether either file has a real future, and act accordingly.** Both are currently fully-implemented-but-disconnected code with real maintenance cost (they compile, they have tests, they need to keep compiling as the rest of the codebase changes) and zero runtime value. Concretely: (a) wire `zerocopy.rs`'s `send_zc`/`RegisteredBufferPool` into `connection.rs`'s large-reply write path (e.g. big `HGETALL`/`SMEMBERS`/`FT.SEARCH` responses above a size threshold) where zero-copy send could plausibly help, since `monoio`'s socket file descriptors would need to be exposed for this to even be possible — or (b) delete it and its tests if there's no near-term plan to integrate it. The same either/or applies to `xdp.rs`, except the honest path there is narrower: real AF_XDP kernel bypass is a large undertaking (a genuine `aya`/`xsk` dependency, root/capabilities, NIC driver support) — if that's not the actual goal, rename this module to reflect what it really is (a testable packet-filter/rate-limiter simulation) rather than implying kernel bypass.
- **Medium — if `zerocopy.rs` is kept, add the missing `MSG_ERRQUEUE` completion polling (§4.3).** `send_zc` submits `MSG_ZEROCOPY` sends but never polls `MSG_ERRQUEUE` for the kernel's completion notification, so even a wired-up caller would have no correct way to know when the source buffer is safe to reuse or free — a real correctness gap in the zero-copy contract, not just "unused code."
- **Low — bound `rate_limiters`' unbounded growth (§3's `TokenBucket` note).** A `TokenBucket` is created and kept forever for every distinct source IP `XDP.PACKET` has ever been asked to evaluate, with no eviction — low risk given it's only reachable via an explicit client command today, but worth a periodic sweep (evict buckets untouched for N minutes) if this is ever exposed more broadly.
- **Low — make `process_packet`'s default-Redirect fallback actually inspect the destination port**, matching what its own doc comment claims ("Redis port 6379 or cluster bus") rather than unconditionally redirecting everything unmatched (§4.1) — cheap to fix and removes a doc-vs-code mismatch inside the file itself.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Simulated XDP mode uses 128 UMEM frames on boot to avoid pre-allocating 128MB per shard.
* **Gotcha 2**: Pre-registered io_uring buffers eliminate get_user_pages and page table walks.
* **Gotcha 3**: Linux SO_ZEROCOPY uses page-flipping for egress frames larger than 4KB.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
