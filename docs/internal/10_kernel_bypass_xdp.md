# Component 10: Kernel Bypass & Zero-Copy Networking (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/xdp.rs` (706 lines), `src/zerocopy.rs` (352 lines)
> **High-Level Design Spec**: [`docs/design/10_kernel_bypass_xdp.md`](../design/10_kernel_bypass_xdp.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

> This document was re-verified line-by-line against the current `src/xdp.rs` and
> `src/zerocopy.rs`. It corrects two errors present in an earlier revision of this document
> (which itself claimed to be correcting an *even earlier* revision): the earlier revision
> asserted that `XdpEngine` has no socket/ring/UMEM types (`XskSocket`, `XskUmem`, `XskRing`,
> a `sockets` registry field). **That assertion was wrong** — those types exist in the current
> source, are instantiated once per shard at boot, and are driven by a real background polling
> loop in `src/server.rs`. See §3 and §4.2 for the corrected, verified picture.

---

## 1. Source Module Map & Responsibilities

| File | Lines | Responsibility |
| :--- | :--- | :--- |
| `src/xdp.rs` | 706 | In-process simulation of AF_XDP data structures (UMEM, rings, XSK sockets), a CIDR rule engine, a per-source-IP token-bucket rate limiter, and manual Ethernet/IPv4/TCP header parsing. Exposed via eight `XDP.*` commands. |
| `src/zerocopy.rs` | 352 | Real, standalone primitives for Linux `MSG_ZEROCOPY` socket sends and `io_uring`-registered fixed buffers. Not wired into the server's connection I/O path. |

**Build configuration**: `Cargo.toml` (workspace root) defines no `[features]` table at all —
neither module is behind a Cargo feature flag or a `#[cfg(...)]` attribute. Both compile
unconditionally into every build, on every platform Rudis targets. There is no way to build
Rudis *without* this code, and no way to opt into "real" AF_XDP behavior — the module always
runs in its current, self-contained form.

---

## 2. Verified Status Summary

Consistent with `docs/design/10_kernel_bypass_xdp.md`: **neither module performs real Linux
kernel bypass, and neither is on the path that serves the server's actual `6379` (or configured
port) client traffic.** `server.rs`'s real accept loop binds an ordinary `SO_REUSEPORT`
`monoio::net::TcpListener` per shard (Component 01) and is entirely independent of `XdpEngine`.

What *is* real, beyond what the design doc's high-level framing covers, is that `src/xdp.rs`'s
ring/socket types are not merely declared — they are instantiated and driven by a live,
always-running background task per shard (§4.2). That task can only ever see packets that a
client injects via the `XDP.INJECT` command; there is no code path from a physical NIC, a raw
socket, or a real eBPF/XDP program into it. So the corrected picture is: **the simulation is
more completely wired end-to-end (rule/rate-limit filtering → command parsing → real command
execution against the shard's data → a simulated response ring) than a purely
"disconnected code" framing would suggest, while still being entirely reachable only through an
explicit admin command and never through real network ingress.**

---

## 3. Data Structures (`src/xdp.rs`)

### 3.1 `XdpAction` / `XdpMode` — enums

```rust
pub enum XdpAction { Pass, Drop, Redirect, Tx }     // xdp.rs:8
pub enum XdpMode { Driver, Skb, Simulated }          // xdp.rs:67
```

`XdpMode::Driver` is a real enum variant but is **never constructed anywhere in the codebase** —
grepping the crate for `XdpMode::Driver` finds only its `Display` arm (xdp.rs:76). There is no
constructor path that selects it. The mode a running instance actually uses is decided once, at
process startup, by the global engine initializer (xdp.rs:586-593):

```rust
static GLOBAL_XDP_ENGINE: LazyLock<Arc<XdpEngine>> = LazyLock::new(|| {
    let mode = if std::path::Path::new("/sys/class/net").exists() {
        XdpMode::Skb
    } else {
        XdpMode::Simulated
    };
    Arc::new(XdpEngine::new("eth0", mode))
});
```

On essentially every real Linux host `/sys/class/net` exists, so **`XdpMode::Skb` is the mode
selected in practice**, not `Simulated` — `Simulated` is the fallback for hosts without a
`sysfs` network directory (e.g. some containers). This distinction matters because it changes
`num_frames` (§3.5) and therefore how much memory the subsystem allocates on boot. Critically,
selecting `Skb` here is purely cosmetic (it only changes what `XDP.INFO` prints) — it does not
attach an eBPF program, open an `AF_XDP` socket, or bind to `eth0` (which may not even exist as
a real interface on the host).

### 3.2 `XdpRule` — CIDR filter entry

```rust
pub struct XdpRule {                 // xdp.rs:27
    pub id: u32,
    pub action: XdpAction,
    pub cidr: String,
    pub network: u32,   // pre-masked network address, from parse_cidr
    pub netmask: u32,
}
```

`parse_cidr` (xdp.rs:35) parses an IPv4 CIDR string (`a.b.c.d/n`, `n` optional and defaulting to
`/32`) into a `(network, netmask)` pair with standard bitwise masking (`!0u32 << (32 - n)`).
Rules are stored as `RwLock<Vec<XdpRule>>` on `XdpEngine` and evaluated by **linear scan,
first match wins** — acceptable given rule tables are expected to stay small; there is no CIDR
trie or interval structure.

### 3.3 `TokenBucket` — per-source-IP rate limiter

```rust
pub struct TokenBucket {             // xdp.rs:83
    pub tokens: f64,
    pub capacity: f64,
    pub refill_rate: f64,   // tokens/sec
    pub last_update: Instant,
}

impl TokenBucket {
    pub fn allow(&mut self) -> bool {   // xdp.rs:100
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        if self.tokens >= 1.0 { self.tokens -= 1.0; true } else { false }
    }
}
```

Standard continuous-refill token bucket costing exactly **1 token per packet**, regardless of
packet size (there is no byte-proportional variant in this file). `XdpEngine::rate_limiters` is
a `RwLock<HashMap<u32, TokenBucket>>` keyed by source IPv4 (as `u32`); buckets are created lazily
on first sight of an IP (`entry(ip).or_insert_with(...)`, xdp.rs:477) and **are never evicted** —
one `TokenBucket` per distinct source IP the process has ever classified a packet for, for the
lifetime of the process. This is a real, if minor, unbounded-growth characteristic. The default
bucket, applied to every newly seen IP, has `capacity = 50_000.0` and `refill_rate = 100_000.0`
tokens/sec (`XdpEngine::default_rate_capacity` / `default_rate_limit`, xdp.rs:360-361).

### 3.4 `XskRing<T>` — generic ring buffer

```rust
pub struct XskRing<T> {              // xdp.rs:129
    pub producer: AtomicU32,
    pub consumer: AtomicU32,
    pub mask: u32,               // capacity - 1; capacity is rounded up to a power of two
    pub entries: Vec<RwLock<T>>,
}
```

`produce`/`consume` (xdp.rs:151-175) implement a classic index-based circular buffer: `produce`
checks `prod.wrapping_sub(cons) > mask` to detect a full ring, writes into
`entries[prod & mask]` under a per-slot `RwLock`, then advances the producer index with
`Ordering::Release`; `consume` mirrors this with `Ordering::Acquire` on the producer load. This
is **not lock-free in the strict sense** — each slot is protected by its own `RwLock<T>`, not a
truly wait-free CAS-based SPSC design — but the index bookkeeping is atomic and the algorithm is
correct for single-producer/single-consumer use within one process. Four ring instances back
each `XskSocket` (§3.6): `rx_ring: XskRing<XdpDesc>`, `fill_ring: XskRing<u64>`,
`tx_ring: XskRing<XdpDesc>`, `comp_ring: XskRing<u64>` — mirroring the four rings a real AF_XDP
socket exposes (Rx, Fill, Tx, Completion).

```rust
pub struct XdpDesc { pub addr: u64, pub len: u32, pub options: u32 }  // xdp.rs:122
```

### 3.5 `XskUmem` — simulated UMEM

```rust
pub struct XskUmem {                 // xdp.rs:191
    pub frame_size: usize,
    pub num_frames: usize,
    pub frames: RwLock<Vec<Vec<u8>>>,   // num_frames * frame_size bytes, fully allocated up front
}
```

`XskEngine::new` fixes `frame_size = 2048` unconditionally and sets `num_frames` based on mode
(xdp.rs:347-351): **128 frames for `XdpMode::Simulated`, 4096 frames otherwise** (i.e. for
`Skb`, the mode actually selected on a normal Linux host — see §3.1). Since every shard creates
its own `XskSocket`, and each `XskSocket` owns an independently allocated `XskUmem`
(`Vec<Vec<u8>>` of `num_frames` buffers of `frame_size` bytes each, `XskUmem::new`,
xdp.rs:198-208), the **per-shard UMEM footprint is `num_frames * frame_size` bytes**:
- `Simulated` mode: 128 × 2048 B = 256 KiB per shard.
- `Skb` mode (the practical default on Linux): 4096 × 2048 B = **8 MiB per shard**, i.e. 8 MiB ×
  (number of shards) process-wide just for this otherwise-idle subsystem — e.g. 128 MiB total on
  a 16-shard instance. This number is derived directly from the constants above, not measured;
  actual RSS impact depends on allocator behavior and is not separately benchmarked here.

`write_frame`/`read_frame` (xdp.rs:210-226) are plain bounds-checked `copy_from_slice`/`to_vec`
operations against a `frame_idx`-selected element of `frames`.

### 3.6 `XskSocket` — simulated AF_XDP socket

```rust
pub struct XskSocket {               // xdp.rs:230
    pub queue_id: u32,
    pub rx_ring: XskRing<XdpDesc>,
    pub fill_ring: XskRing<u64>,      // pre-filled with all frame indices on construction
    pub tx_ring: XskRing<XdpDesc>,
    pub comp_ring: XskRing<u64>,
    pub umem: Arc<XskUmem>,
    pub rx_packets: AtomicU64,
    pub tx_packets: AtomicU64,
}
```

Key methods:
- **`inject_rx(&self, packet: &[u8]) -> bool`** (xdp.rs:265): pops a free frame index from
  `fill_ring`, copies `packet` into that UMEM frame, builds an `XdpDesc { addr, len, options: 0 }`
  and pushes it onto `rx_ring`. Returns the frame to `fill_ring` and `false` if `rx_ring` is
  full. This is how a byte payload gets *into* the simulated pipeline (§4.2).
- **`rx_burst(&self, out: &mut Vec<Vec<u8>>, max_batch: usize) -> usize`** (xdp.rs:286): pops up
  to `max_batch` descriptors off `rx_ring`, reads each corresponding UMEM frame's bytes into
  `out`, and replenishes `fill_ring` with the freed frame index.
- **`tx_burst(&self, packets: &[&[u8]]) -> usize`** (xdp.rs:303): writes each packet into a UMEM
  frame, pushes a descriptor onto `tx_ring`, and immediately also pushes the same frame index
  onto `comp_ring` (simulating instantaneous send completion — there is no actual transmission).

`XdpEngine::sockets: RwLock<HashMap<(u16, u32), Arc<XskSocket>>>` (xdp.rs:335) is a registry
keyed by `(port, queue_id)`. `get_or_create_socket(port, queue_id)` (xdp.rs:494) lazily
constructs one `XskSocket` per key.

### 3.7 `XdpEngine` — top-level struct

```rust
pub struct XdpEngine {               // xdp.rs:325
    pub ifname: String,
    pub mode: XdpMode,
    pub frame_size: usize,                              // 2048, fixed
    pub num_frames: usize,                               // 128 (Simulated) or 4096 (Skb/Driver)
    pub rules: RwLock<Vec<XdpRule>>,
    pub next_rule_id: AtomicU32,
    pub rate_limiters: RwLock<HashMap<u32, TokenBucket>>, // keyed by source IPv4
    pub default_rate_limit: f64,                          // 100_000.0 tokens/sec
    pub default_rate_capacity: f64,                       // 50_000.0 burst capacity
    pub sockets: RwLock<HashMap<(u16, u32), Arc<XskSocket>>>,
    pub rx_packets: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub dropped_packets: AtomicU64,
    pub redirected_packets: AtomicU64,
    pub pass_packets: AtomicU64,
    pub rate_limit_drops: AtomicU64,
}
```

`get_xdp_engine()` (xdp.rs:595) returns a clone of the process-wide `Arc<XdpEngine>` singleton —
every shard, and every client connection, shares exactly one `XdpEngine` (but each shard owns
its *own* `XskSocket` within that engine's shared `sockets` map, keyed by its own `queue_id`).

### 3.8 `src/zerocopy.rs` — real, standalone, unused types

```rust
pub struct RegisteredBufferPool {    // zerocopy.rs:37
    ptr: *mut u8,
    layout: Layout,
    slot_size: usize,
    slot_count: usize,
    free_slots: Vec<usize>,
    iovecs: Vec<libc::iovec>,
    stats: Arc<ZeroCopyStats>,
}

pub struct ZeroCopyEngine {          // zerocopy.rs:170
    stats: Arc<ZeroCopyStats>,
}
```

These are **separate** structs — `ZeroCopyEngine` does not own a `buffer_pool` field; a caller
would need to wire the two together manually, and none does. `RegisteredBufferPool` page-aligns
a single `alloc_zeroed` allocation (`PAGE_SIZE = 4096`) into `slot_count` slots of `slot_size`
bytes each (defaults: 16 slots × 64 KiB = 1 MiB, `DEFAULT_SLOT_COUNT`/`DEFAULT_SLOT_SIZE`,
zerocopy.rs:11-14), exposes each slot's `libc::iovec` for `io_uring::Submitter::register_buffers`,
and tracks free/acquired slots with a plain `Vec<usize>` free-list plus `ZeroCopyStats` hit/miss
counters.

---

## 4. Execution Algorithms

### 4.1 `XdpEngine::process_packet` — manual Ethernet/IPv4/TCP parsing + filter pipeline

```rust
pub fn process_packet(&self, packet: &[u8]) -> XdpAction {   // xdp.rs:404
    self.rx_packets.fetch_add(1, Ordering::Relaxed);
    self.rx_bytes.fetch_add(packet.len() as u64, Ordering::Relaxed);
    if packet.is_empty() { return XdpAction::Drop; }         // + dropped_packets++
    // ...
}
```

Step by step:
1. **Frame-shape detection** (xdp.rs:416-450): tries a 14-byte Ethernet header first
   (`packet[12..14] == [0x08, 0x00]` for EtherType IPv4), falling back to treating `packet` as a
   bare IPv4 datagram if `packet[0] >> 4 == 4`. Both branches bounds-check before indexing.
2. **Field extraction**: source IPv4 address from bytes 12..16 of the IP header; if the IP
   protocol byte is 6 (TCP) and the packet is long enough, the destination port is also parsed
   out — but it is **computed and discarded** (`let _ = payload_offset;`, xdp.rs:489). No code
   path in this function currently branches on destination port.
3. **CIDR rule scan** (xdp.rs:452-473): if a source IP was extracted, `self.rules` is scanned
   linearly; the first rule whose `(ip & netmask) == network` decides the action
   (`Drop`/`Pass`/`Redirect`/`Tx`), incrementing the matching atomic counter and returning
   immediately.
4. **Rate limiting** (xdp.rs:475-486): if no rule matched, the per-source-IP `TokenBucket` is
   consulted (created lazily); a failed `allow()` returns `Drop` and increments both
   `rate_limit_drops` and `dropped_packets`.
5. **Default fallback** (xdp.rs:488-491): anything not matched or rate-limited is
   **unconditionally `Redirect`ed** — this is a real, documented gap: the function's own comment
   says "Redis port 6379 or cluster bus", but the code never actually inspects the destination
   port it parsed in step 2 to make that decision; every otherwise-unfiltered packet is
   redirected regardless of port.
6. **Non-IP input** (empty `src_ip`, e.g. a plain RESP/inline-text payload with no recognizable
   Ethernet/IPv4 header): skips rule/rate-limit evaluation entirely and falls straight through to
   the same unconditional `Redirect` in step 5.

### 4.2 The real, wired ingestion loop — `src/server.rs`, per shard, at boot

This is the part missing from earlier revisions of this document. Every shard's startup sequence
(`server.rs:269-313`) spawns a `monoio` background task that continuously polls that shard's own
`XskSocket`:

```rust
let xdp_engine = crate::xdp::get_xdp_engine();
let xsk_socket = xdp_engine.get_or_create_socket(shard_port, shard_id as u32); // queue_id = shard_id
monoio::spawn(async move {
    let mut frames = Vec::with_capacity(32);
    loop {
        let count = xsk_socket.rx_burst(&mut frames, 32);
        if count > 0 {
            for frame in frames.drain(..) {
                let action = xdp_engine.process_packet(&frame);
                if (action == Pass || action == Redirect)
                    && let Some(cmd_payload) = crate::xdp::extract_transport_payload(&frame)
                {
                    // parse cmd_payload as a RESP command, route to the owning shard
                    // (locally or via router.execute_remote), execute it against real
                    // shard state, and tx_burst() the RESP response back onto the same
                    // XskSocket's tx_ring/comp_ring.
                }
            }
        } else {
            monoio::time::sleep(Duration::from_millis(5)).await;
        }
    }
});
```

So the pipeline **is** fully wired end to end: a byte payload injected into `rx_ring` is
classified (CIDR/rate-limit), has its RESP command extracted, is **actually executed against the
shard's real database** (`execute_local_command` or a cross-shard `execute_remote` call — the
same code path a normal client connection uses), and the real response is written back into the
socket's simulated `tx_ring`/`comp_ring`. What is *not* real is how a payload gets into
`rx_ring` in the first place: the only producer is `XskSocket::inject_rx`, called exclusively
from the `XDP.INJECT` command (§4.3) — there is still no eBPF program, raw socket, or NIC handing
frames to this ring. Nothing consumes `tx_ring`/`comp_ring` to actually transmit bytes over a
network; `XDP.SOCKET` merely reports their lengths.

### 4.3 The `XDP.*` command family — eight variants, not six

`src/resp.rs` parses eight `XDP.*` subcommands into eight `Command` enum variants, each
dispatched in `src/connection.rs` (~line 8423):

| Command | `Command` variant | Effect |
| :--- | :--- | :--- |
| `XDP.INFO` | `XdpInfo` | Formats `XdpEngine::info()` — ifname, mode, frame size/count, rule/socket counts, all atomic counters. |
| `XDP.RULEADD <action> <cidr>` | `XdpRuleAdd` | `add_rule` — pushes a new `XdpRule`. |
| `XDP.RULEDEL <id>` | `XdpRuleDel` | `del_rule` — removes by id. |
| `XDP.RULELIST` | `XdpRuleList` | `list_rules` — clones and returns the rule vector. |
| `XDP.STATS` | `XdpStats` | Returns all six atomic counters as a RESP array. |
| `XDP.PACKET <bytes>` | `XdpPacket` | Calls `process_packet` **directly**, bypassing the ring entirely — a synchronous classify-and-reply request/response, not a ring injection. |
| `XDP.SOCKET <queue_id>` | `XdpSocket` | `get_or_create_socket(port, queue_id)`, reports `rx_ring`/`fill_ring`/`tx_ring` lengths. |
| `XDP.INJECT <queue_id> <bytes>` | `XdpInject` | `get_or_create_socket(port, queue_id).inject_rx(payload)` — the **only** producer for the background loop in §4.2. |

`extract_transport_payload` (xdp.rs:544-583) mirrors `process_packet`'s frame-shape detection to
strip Ethernet+IPv4+TCP headers (computing the TCP data offset from the header's IHL/data-offset
nibbles) and return the remaining bytes as the RESP/inline command payload; if the input isn't a
recognizable Ethernet/IP/TCP frame it falls back to treating the entire input as a direct RESP or
inline-text payload.

### 4.4 `ZeroCopyEngine::send_zc` — real syscall usage, verified unreachable

```rust
pub fn send_zc(&self, fd: RawFd, data: &[u8]) -> io::Result<usize> {   // zerocopy.rs:200
    let flags = if data.len() >= PAGE_SIZE {
        libc::MSG_NOSIGNAL | MSG_ZEROCOPY
    } else {
        libc::MSG_NOSIGNAL
    };
    let ret = unsafe { libc::send(fd, data.as_ptr() as *const _, data.len(), flags) };
    // on success: record zc_send_calls / zc_bytes_sent
    // on ENOBUFS or when MSG_ZEROCOPY was requested: fall back to a plain blocking send()
}
```

Correct, idiomatic use of Linux's zero-copy send path — including the size threshold
(`PAGE_SIZE`, 4 KiB) below which plain `send()` is used, since `MSG_ZEROCOPY` incurs page-pinning
overhead not worth paying for small payloads. **What's missing**: Linux's `MSG_ZEROCOPY` contract
requires the caller to poll `MSG_ERRQUEUE` via `recvmsg` for a completion notification before the
source buffer can be safely reused or freed — `send_zc` does not do this anywhere in the file.
Even a hypothetically wired-up caller would have no correct way to know when the kernel has
finished the zero-copy DMA. `fd: RawFd` is a raw file descriptor; `connection.rs`'s real sockets
are `monoio::net::TcpStream` objects whose underlying fd is never extracted or passed to this
function anywhere in the codebase — **zero callers outside `zerocopy.rs`'s own `#[cfg(test)]`
module**, verified by grep across `src/*.rs`.

`build_io_uring_send_zc` (zerocopy.rs:249) similarly constructs a real
`io_uring::opcode::SendZc` entry via the genuine `io-uring` crate dependency (also used, for
unrelated purposes, by Component 07's tiered-storage direct I/O) — but nothing ever submits the
resulting entry to a ring.

---

## 5. Cross-Component Interactions

- **`src/server.rs`** (Component 01): spawns the per-shard `XskSocket` polling task at boot
  (§4.2) — this is real integration, not merely a comment. It runs alongside, and independently
  of, the shard's real `monoio` `TcpListener` accept loop; the two never interact.
- **`src/connection.rs`**: dispatches all eight `Xdp*` `Command` variants (§4.3), each forwarding
  to `crate::xdp::get_xdp_engine()`.
- **`src/resp.rs`**: parses the eight `XDP.*` command names/arguments into the `Command` enum
  variants above (including mapping `"DROP"`/`"PASS"`/`"REDIRECT"`/`"TX"` strings to
  `XdpAction`).
- **`src/router.rs`**: `execute_remote` is reused, unmodified, by the §4.2 ingestion loop to
  route a command extracted from an injected packet to its owning shard when that shard differs
  from the one polling the socket — the same cross-shard mailbox mechanism (Component 04) an
  ordinary client connection uses.
- **`src/zerocopy.rs`**: no cross-component interactions — verified zero callers outside its own
  test module.

---

## 6. Future Improvements

- **High-priority decision, not a fix: decide whether either file has a real future, and act
  accordingly.** The §4.2 loop means `xdp.rs` is more thoroughly wired than "dead code with
  tests," but it is still only reachable via an explicit `XDP.INJECT` admin command, never by
  real network traffic. Either invest in real `AF_XDP`/`XSK` syscalls (a genuine `aya`/`libbpf`
  dependency, root/`CAP_NET_ADMIN`/`CAP_BPF`, NIC driver support) and a real eBPF program, or
  rename/document this module clearly as a packet-classification simulation harness rather than
  implying kernel bypass. `zerocopy.rs` has no callers at all; either wire `send_zc`/
  `RegisteredBufferPool` into `connection.rs`'s large-reply write path (which requires exposing
  `monoio` socket file descriptors, not currently done anywhere) or remove it.
- **Medium — add the missing `MSG_ERRQUEUE` completion polling** (§4.4) if `zerocopy.rs` is kept
  — a real correctness gap in the zero-copy contract, independent of whether it ever gets a
  caller.
- **Medium — make `process_packet`'s default-Redirect fallback actually inspect the destination
  port** (§4.1), matching its own doc comment, rather than unconditionally redirecting everything
  unmatched.
- **Low — bound `rate_limiters`' unbounded growth** (§3.3): one `TokenBucket` per distinct source
  IP ever classified, never evicted. Low risk while only reachable via explicit commands, but
  worth a periodic sweep if ever exposed more broadly.

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1 — UMEM footprint is per-shard, and depends on which `XdpMode` is actually selected.**
  Every shard creates its own `XskSocket`/`XskUmem` at boot (§4.2), and `num_frames` is decided
  once, process-wide, by whether `/sys/class/net` exists (§3.1) — true on essentially all real
  Linux hosts, which selects `Skb` mode with **4096 frames × 2048 B = 8 MiB of UMEM per shard**
  (not the 128-frame/256 KiB `Simulated` figure, which only applies on hosts without `sysfs`).
  On a 16-shard instance that is roughly 128 MiB allocated at boot for a subsystem that, absent
  an explicit `XDP.INJECT` call, never receives a packet.
* **Gotcha 2 — `XDP.INJECT` is not a diagnostic no-op.** Because of the §4.2 background loop, an
  injected payload that parses as a valid RESP command is **actually executed against real
  shard state** (same code path as a normal client command) and can mutate the keyspace. Treat
  `XDP.INJECT` as equivalent to sending a command on an ordinary connection, not as an inert test
  hook.
* **Gotcha 3 — `XDP.PACKET` and `XDP.INJECT` exercise different code paths.** `XDP.PACKET` calls
  `process_packet` directly and only ever returns a classification (`PASS`/`DROP`/`REDIRECT`/
  `TX`); it never touches a ring and never executes a command. `XDP.INJECT` goes through the ring
  and, if the payload parses as RESP and is not filtered, does execute a command. Don't conflate
  the two when writing tests or reasoning about side effects.
* **Gotcha 4 — `RegisteredBufferPool`/`ZeroCopyEngine` are correct but fully inert.** `libc::send`
  with `MSG_ZEROCOPY` is real, page-alignment is real, `io_uring::opcode::SendZc` construction is
  real — but there is no caller anywhere outside `zerocopy.rs`'s own tests, and no completion
  (`MSG_ERRQUEUE`) polling even if one were added.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
