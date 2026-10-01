# Component 10: Kernel Bypass & Zero-Copy Networking (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/xdp.rs` (706 lines), `src/zerocopy.rs` (352 lines)
> **High-Level Design Spec**: [`docs/design/10_kernel_bypass_xdp.md`](../design/10_kernel_bypass_xdp.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

> Re-verified line-by-line against the current `src/xdp.rs` and `src/zerocopy.rs` (both
> unchanged in size since the previous pass — still exactly 706 and 352 lines — but re-read in
> full rather than assumed unchanged), plus `src/server.rs`, `src/connection.rs`, `src/resp.rs`,
> `src/router.rs` and `src/main.rs`/`src/lib.rs`. All findings from the prior revision were
> re-checked against current line numbers (which shifted because `connection.rs` and `resp.rs`
> have both grown substantially). One correction from the prior revision: the three rule-table
> sub-operations are **not** separate top-level command names (`XDP.RULEADD`/`XDP.RULEDEL`/
> `XDP.RULELIST`) — they are subcommands of a single `XDP.RULE ADD|DEL|LIST` command, parsed by
> one `"XDP.RULE"` match arm in `resp.rs`. See §4.3.

---

## 1. Source Module Map & Responsibilities

| File | Lines | Responsibility |
| :--- | :--- | :--- |
| `src/xdp.rs` | 706 | In-process simulation of AF_XDP data structures (UMEM, rings, XSK sockets), a CIDR rule engine, a per-source-IP token-bucket rate limiter, and manual Ethernet/IPv4/TCP header parsing. Exposed via six `XDP.*` command names (eight `Command` enum variants, since `XDP.RULE` fans out to three). |
| `src/zerocopy.rs` | 352 | Real, standalone primitives for Linux `MSG_ZEROCOPY` socket sends and `io_uring`-registered fixed buffers. Not wired into the server's connection I/O path. |

**Module registration**: `src/lib.rs:36-37` declares `pub mod xdp;` and `pub mod zerocopy;`
unconditionally — no `#[cfg(...)]` gate on either `mod` statement. `src/main.rs` contains **zero**
references to either module (grep confirms); it is not involved in process bootstrap beyond
`server.rs` pulling in the `XdpEngine` singleton itself (§4.2).

**Build configuration**: `Cargo.toml` (workspace root) defines no `[features]` table at all —
neither module is behind a Cargo feature flag. Both compile unconditionally into every build, on
every platform Rudis targets. There is no way to build Rudis *without* this code, and no way to
opt into "real" AF_XDP behavior — the module always runs in its current, self-contained form.

---

## 2. Verified Status Summary

**Neither module performs real Linux kernel bypass, and neither is on the path that serves the
server's actual `6379` (or configured port) client traffic.** `server.rs`'s real accept loop binds
an ordinary `SO_REUSEPORT` `monoio::net::TcpListener` per shard (Component 01) and is entirely
independent of `XdpEngine`.

What *is* real: `src/xdp.rs`'s ring/socket types are not merely declared — they are instantiated
and driven by a live, always-running background task per shard, spawned inside
`run_shard_worker` (`src/server.rs:47`, loop body at lines 270-314; §4.2). That task can only ever
see packets that a client injects via the `XDP.INJECT` command; there is no code path from a
physical NIC, a raw socket, or a real eBPF/XDP program into it. The pipeline is wired end-to-end:
rule/rate-limit filtering → transport-header stripping → RESP command parsing → **real command
execution against the shard's actual keyspace** (the same `execute_local_command` /
`Router::execute_remote` machinery an ordinary client connection uses) → a response written back
onto a simulated `tx_ring`/`comp_ring`. It is entirely reachable only through the explicit
`XDP.INJECT` admin command and never through real network ingress.

---

## 3. Data Structures (`src/xdp.rs`)

### 3.1 `XdpAction` / `XdpMode` — enums

```rust
pub enum XdpAction { Pass, Drop, Redirect, Tx }     // xdp.rs:8-13
pub enum XdpMode { Driver, Skb, Simulated }          // xdp.rs:67-71
```

`XdpAction` implements `Display` (xdp.rs:15-24), printing `"PASS"`/`"DROP"`/`"REDIRECT"`/`"TX"` —
these are also the exact strings `XDP.RULE ADD` parses back in (§4.3), and what `XDP.PACKET`
returns as a RESP simple string.

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
attach an eBPF program, open an `AF_XDP` socket, or bind to `eth0` (which may not even exist as a
real interface on the host). `GLOBAL_XDP_ENGINE` is a process-wide `LazyLock<Arc<XdpEngine>>`
(xdp.rs:586); `get_xdp_engine()` (xdp.rs:595-597) just clones the `Arc`. Every shard and every
client connection shares exactly one `XdpEngine` instance.

### 3.2 `XdpRule` — CIDR filter entry

```rust
pub struct XdpRule {                 // xdp.rs:27-33
    pub id: u32,
    pub action: XdpAction,
    pub cidr: String,
    pub network: u32,   // pre-masked network address, from parse_cidr
    pub netmask: u32,
}
```

`parse_cidr` (xdp.rs:35-64) parses an IPv4 CIDR string (`a.b.c.d/n`, `n` optional and defaulting
to `/32`) into a `(network, netmask)` pair with standard bitwise masking
(`netmask = !0u32 << (32 - prefix_len)`, with an explicit `prefix_len == 0 → netmask = 0` special
case to avoid a shift-by-32 panic). `prefix_len > 32` is rejected with an `Err`. Rules are stored
as `RwLock<Vec<XdpRule>>` on `XdpEngine` and evaluated by **linear scan, first match wins**
(xdp.rs:454-473) — acceptable given rule tables are expected to stay small; there is no CIDR trie
or interval structure. `next_rule_id: AtomicU32` starts at `1` and is incremented with
`fetch_add(.., Ordering::SeqCst)` per `add_rule` call (xdp.rs:372-385); IDs are never reused after
a `del_rule`.

### 3.3 `TokenBucket` — per-source-IP rate limiter

```rust
pub struct TokenBucket {             // xdp.rs:83-88
    pub tokens: f64,
    pub capacity: f64,
    pub refill_rate: f64,   // tokens/sec
    pub last_update: Instant,
}

impl TokenBucket {
    pub fn new(capacity: f64, refill_rate: f64) -> Self { .. }   // xdp.rs:91-98, tokens starts full
    pub fn allow(&mut self) -> bool {                            // xdp.rs:100-111
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        if self.tokens >= 1.0 { self.tokens -= 1.0; true } else { false }
    }
}
```

Standard continuous-refill token bucket costing exactly **1 token per packet**, regardless of
packet size (there is no byte-proportional variant in this file). `XdpEngine::rate_limiters` is a
`RwLock<HashMap<u32, TokenBucket>>` keyed by source IPv4 (as `u32`); buckets are created lazily on
first sight of an IP via `entry(ip).or_insert_with(..)` (xdp.rs:476-479) and **are never evicted**
— one `TokenBucket` per distinct source IP the process has ever classified a packet for, for the
lifetime of the process. This is a real, if minor, unbounded-growth characteristic. The default
bucket, applied to every newly seen IP, has `capacity = 50_000.0` and `refill_rate = 100_000.0`
tokens/sec (`XdpEngine::default_rate_capacity` / `default_rate_limit`, set in `XdpEngine::new`,
xdp.rs:360-361).

### 3.4 `XdpDesc` and `XskRing<T>` — descriptor and generic ring buffer

```rust
pub struct XdpDesc { pub addr: u64, pub len: u32, pub options: u32 }  // xdp.rs:122-126

pub struct XskRing<T> {              // xdp.rs:129-134
    pub producer: AtomicU32,
    pub consumer: AtomicU32,
    pub mask: u32,               // capacity - 1; capacity rounded up to next power of two
    pub entries: Vec<RwLock<T>>,
}
```

`XskRing::new(size)` (xdp.rs:137-149) rounds `size` up with `next_power_of_two()` and
pre-allocates one `RwLock<T>` slot per entry via `T::default()` — so `T` must implement
`Clone + Default`. `produce`/`consume` (xdp.rs:151-175) implement a classic index-based circular
buffer: `produce` checks `prod.wrapping_sub(cons) > mask` to detect a full ring, writes into
`entries[prod & mask]` under a per-slot `RwLock` write guard, then advances the producer index
with `Ordering::Release`; `consume` mirrors this with `Ordering::Acquire` on the producer load and
clones the slot's value out. This is **not lock-free in the strict sense** — each slot is
protected by its own `RwLock<T>`, not a truly wait-free CAS-based SPSC design — but the index
bookkeeping is atomic and the algorithm is correct for single-producer/single-consumer use within
one process. `len()`/`is_empty()` (xdp.rs:177-187) are plain wrapping-subtract reads of the two
atomics (`Ordering::Relaxed`). Four ring instances back each `XskSocket` (§3.6): `rx_ring:
XskRing<XdpDesc>`, `fill_ring: XskRing<u64>`, `tx_ring: XskRing<XdpDesc>`, `comp_ring:
XskRing<u64>` — mirroring the four rings a real AF_XDP socket exposes (Rx, Fill, Tx, Completion).

### 3.5 `XskUmem` — simulated UMEM, and the unused `UmemFrame` struct

```rust
pub struct XskUmem {                 // xdp.rs:191-195
    pub frame_size: usize,
    pub num_frames: usize,
    pub frames: RwLock<Vec<Vec<u8>>>,   // num_frames * frame_size bytes, fully allocated up front
}
```

`XdpEngine::new` fixes `frame_size = 2048` unconditionally and sets `num_frames` based on mode
(xdp.rs:347-351): **128 frames for `XdpMode::Simulated`, 4096 frames for every other mode**
(i.e. `Skb` and the never-constructed `Driver` — see §3.1). `XskUmem::new` (xdp.rs:198-208)
eagerly allocates `num_frames` separate `Vec<u8>` buffers, each zero-initialized to `frame_size`
bytes (`vec![0u8; frame_size]`), stored in a `Vec<Vec<u8>>` behind one `RwLock`. Since every shard
creates its own `XskSocket` at boot (§4.2), and each `XskSocket` owns an independently allocated
`XskUmem`, the **per-shard UMEM footprint is `num_frames * frame_size` bytes**:
- `Simulated` mode: 128 × 2048 B = 256 KiB per shard.
- `Skb` mode (the practical default on Linux): 4096 × 2048 B = **8 MiB per shard**, i.e. 8 MiB ×
  (number of shards) process-wide just for this otherwise-idle subsystem — e.g. 128 MiB total on
  a 16-shard instance. This number is derived directly from the constants above, not measured;
  actual RSS impact depends on allocator behavior and is not separately benchmarked here.

`write_frame`/`read_frame` (xdp.rs:210-226) are plain bounds-checked `copy_from_slice`/`to_vec`
operations against a `frame_idx`-selected element of `frames`; `write_frame` silently truncates
data longer than `frame_size` (`len = data.len().min(self.frame_size)`) rather than erroring, and
both methods silently no-op/return-empty on an out-of-range `frame_idx` rather than panicking.

**`UmemFrame`** (xdp.rs:114-118):

```rust
pub struct UmemFrame {
    pub addr: u64,
    pub len: u32,
    pub buffer: Vec<u8>,
}
```

This struct is declared but **never constructed or referenced anywhere else in the crate** —
`rg -n "UmemFrame" src/` returns exactly one hit, its own definition. It appears to be a leftover
or planned-but-unused per-frame metadata type; `XskUmem` instead stores raw `Vec<u8>` buffers
directly and `XdpDesc` (not `UmemFrame`) is what actually flows through the rings. Pure dead code
with no runtime effect — safe to ignore, or a candidate for removal.

### 3.6 `XskSocket` — simulated AF_XDP socket

```rust
pub struct XskSocket {               // xdp.rs:230-239
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

`XskSocket::new(queue_id, frame_size, num_frames)` (xdp.rs:242-262): the ring capacity is
`(num_frames.max(64)).next_power_of_two()` (so at least 64 slots even for small `num_frames`), and
all four rings share this same capacity. At construction, `fill_ring` is pre-loaded with every
frame index `0..num_frames` via repeated `produce()` calls — this is what makes `inject_rx`
immediately usable without a separate "allocate frame" step.

Key methods:
- **`inject_rx(&self, packet: &[u8]) -> bool`** (xdp.rs:265-283): pops a free frame index from
  `fill_ring`; if none is free, returns `false` immediately (packet dropped, no counter
  incremented for this case). Otherwise copies `packet` into that UMEM frame via `write_frame`,
  builds an `XdpDesc { addr: frame_idx * frame_size, len: packet.len() as u32, options: 0 }` and
  pushes it onto `rx_ring`. If `rx_ring.produce` fails (ring full), the frame index is returned to
  `fill_ring` and `false` is returned — no leak, but also no `rx_packets` increment on this
  branch; `rx_packets` is only incremented on the success path. This is how a byte payload gets
  *into* the simulated pipeline (§4.2) — the **only** producer of `rx_ring` entries in the whole
  codebase is this method, called exclusively by the `XDP.INJECT` command handler (§4.3).
- **`rx_burst(&self, out: &mut Vec<Vec<u8>>, max_batch: usize) -> usize`** (xdp.rs:286-300): pops
  up to `max_batch` descriptors off `rx_ring` in a loop, reads each corresponding UMEM frame's
  bytes into `out` via `read_frame`, and replenishes `fill_ring` with the freed frame index.
  Returns the number of packets drained. Note: this method does **not** increment
  `rx_packets`/`tx_packets` itself (those are updated in `inject_rx`/`tx_burst`).
- **`tx_burst(&self, packets: &[&[u8]]) -> usize`** (xdp.rs:303-322): for each packet, writes it
  into UMEM frame index `sent` (i.e. **frame indices are simply the 0-based position within the
  batch**, not pulled from any free-list — this diverges from how `rx_burst`/`inject_rx` manage
  frames, but is harmless here since nothing ever reads these tx frames back by address), pushes a
  descriptor onto `tx_ring`, and immediately *also* pushes the same frame index onto `comp_ring`
  (simulating instantaneous send completion — there is no actual transmission, so completion is
  trivially "immediate").

`XdpEngine::sockets: RwLock<HashMap<(u16, u32), Arc<XskSocket>>>` (xdp.rs:335) is a registry keyed
by `(port, queue_id)` — **not** per-shard-exclusive: any shard can look up or create a socket for
*any* `(port, queue_id)` pair, because `XdpEngine` itself is one process-wide singleton shared by
every shard (§3.1). `get_or_create_socket(port, queue_id)` (xdp.rs:494-500) lazily constructs one
`XskSocket` per key via `HashMap::entry(..).or_insert_with(..)`. `get_socket` (xdp.rs:502-504,
read-only lookup, returns `None` if absent) and `list_sockets` (xdp.rs:506-508, returns all
`(port, queue_id)` keys) are also provided but have no `XDP.*` command currently wired to them —
`XDP.SOCKET` always calls `get_or_create_socket`, never `get_socket` (§4.3).

### 3.7 `XdpEngine` — top-level struct

```rust
pub struct XdpEngine {               // xdp.rs:325-344
    pub ifname: String,
    pub mode: XdpMode,
    pub frame_size: usize,                                // 2048, fixed
    pub num_frames: usize,                                 // 128 (Simulated) or 4096 (Skb/Driver)
    pub rules: RwLock<Vec<XdpRule>>,
    pub next_rule_id: AtomicU32,
    pub rate_limiters: RwLock<HashMap<u32, TokenBucket>>,  // keyed by source IPv4
    pub default_rate_limit: f64,                           // 100_000.0 tokens/sec
    pub default_rate_capacity: f64,                        // 50_000.0 burst capacity
    pub sockets: RwLock<HashMap<(u16, u32), Arc<XskSocket>>>,
    pub rx_packets: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub dropped_packets: AtomicU64,
    pub redirected_packets: AtomicU64,
    pub pass_packets: AtomicU64,
    pub rate_limit_drops: AtomicU64,
}
```

`get_xdp_engine()` (xdp.rs:595-597) returns a clone of the process-wide `Arc<XdpEngine>` singleton
— every shard, and every client connection, shares exactly one `XdpEngine`, including its
`sockets` registry. A consequence worth calling out explicitly: **a connection accepted on shard
B can inject a packet into the ring polled by shard A's background task**, simply by calling
`XDP.INJECT <queue_id=A> <payload>` — nothing ties the issuing connection's shard to the
`queue_id` argument (§4.2, §4.3).

### 3.8 `src/zerocopy.rs` — real, standalone, unused types

```rust
pub struct ZeroCopyStats {           // zerocopy.rs:21-27
    pub zc_send_calls: AtomicU64,
    pub zc_bytes_sent: AtomicU64,
    pub registered_buffer_hits: AtomicU64,
    pub registered_buffer_misses: AtomicU64,
    pub fallback_sends: AtomicU64,
}

pub struct RegisteredBufferPool {    // zerocopy.rs:37-45
    ptr: *mut u8,
    layout: Layout,
    slot_size: usize,
    slot_count: usize,
    free_slots: Vec<usize>,
    iovecs: Vec<libc::iovec>,
    stats: Arc<ZeroCopyStats>,
}

pub struct ZeroCopyEngine {          // zerocopy.rs:170-172
    stats: Arc<ZeroCopyStats>,
}
```

These are **separate** structs — `ZeroCopyEngine` does not own a `buffer_pool` field; a caller
would need to wire the two together manually, and none does (both take their own
`Arc<ZeroCopyStats>` independently). `RegisteredBufferPool::new(slot_count, slot_size, stats)`
(zerocopy.rs:52-87) does a single `alloc_zeroed` allocation of `slot_count * slot_size` bytes
page-aligned to `PAGE_SIZE = 4096` (`Layout::from_size_align(total_size, PAGE_SIZE)`), then slices
it into `slot_count` contiguous `libc::iovec`s (one per slot, computed via pointer arithmetic
`ptr.add(i * slot_size)`) for `io_uring::Submitter::register_buffers`, and seeds a `Vec<usize>`
free-list `[0, 1, .., slot_count-1]`. Defaults: `DEFAULT_SLOT_COUNT = 16`, `DEFAULT_SLOT_SIZE =
64 * 1024` (zerocopy.rs:11-14) → 16 × 64 KiB = **1 MiB total pool size** if constructed with the
defaults (no code currently constructs it with any arguments — it has zero callers, §4.4).
`acquire_slot`/`release_slot` (zerocopy.rs:95-114) pop/push the free-list and bump
`registered_buffer_hits`/`misses` in `ZeroCopyStats`; `release_slot` guards against double-release
via `free_slots.contains(&slot_idx)`. `get_slot`/`get_slot_mut` (zerocopy.rs:117-144) hand out
raw-pointer-derived slices via `unsafe { std::slice::from_raw_parts[_mut] }`. `Drop for
RegisteredBufferPool` (zerocopy.rs:159-167) calls `dealloc` with the stored `Layout`, matching the
original allocation exactly (correct pairing). `unsafe impl Send + Sync` is declared explicitly
(zerocopy.rs:47-48) since the struct holds a raw `*mut u8`.

---

## 4. Execution Algorithms

### 4.1 `XdpEngine::process_packet` — manual Ethernet/IPv4/TCP parsing + filter pipeline

```rust
pub fn process_packet(&self, packet: &[u8]) -> XdpAction {   // xdp.rs:404-492
```

Step by step:
1. **Counters first** (xdp.rs:405-407): `rx_packets` and `rx_bytes` are incremented
   unconditionally, before any validation — even an empty packet counts toward `rx_packets`.
2. **Empty-packet short-circuit** (xdp.rs:409-412): `packet.is_empty()` → increments
   `dropped_packets` and returns `XdpAction::Drop` immediately.
3. **Frame-shape detection** (xdp.rs:416-450): tries a 14-byte Ethernet header first — if
   `packet.len() >= 14 && packet[12] == 0x08 && packet[13] == 0x00` (EtherType IPv4), it treats
   bytes `14..` as an IPv4 header (requires `>= 20` more bytes, else `(None, None, 14)`). Otherwise,
   if `packet.len() >= 20 && (packet[0] >> 4) == 4`, it treats the whole packet as a bare IPv4
   datagram. If neither matches, `(src_ip, dst_port, payload_offset) = (None, None, 0)` — a
   "plain text or diagnostic payload" case, e.g. raw RESP bytes with no network headers at all.
   Both IP branches bounds-check length before indexing.
4. **Field extraction**: source IPv4 address from bytes 12..16 of the IP header (big-endian
   `u32`); if the IP protocol byte (offset 9) is `6` (TCP) and the packet is long enough, the
   destination port is also parsed from the TCP header — but it is **computed and discarded**
   (`let _ = payload_offset;`, xdp.rs:489, and `_dst_port` is never read at all). No code path in
   this function currently branches on destination port.
5. **CIDR rule scan** (xdp.rs:452-473): only runs if a source IP was extracted (`if let Some(ip) =
   src_ip`). `self.rules` is scanned linearly; the first rule whose `(ip & netmask) == network`
   decides the action (`Drop`/`Pass`/`Redirect`/`Tx`), increments the matching atomic counter
   (`Tx` increments none), and returns immediately. Packets with no extractable source IP (the
   "plain text" case) skip this step entirely.
6. **Rate limiting** (xdp.rs:475-486): only reached if a source IP was extracted and no rule
   matched. The per-source-IP `TokenBucket` is consulted (created lazily with the default
   capacity/rate, §3.3); a failed `allow()` increments both `rate_limit_drops` and
   `dropped_packets` and returns `XdpAction::Drop`.
7. **Default fallback** (xdp.rs:488-491): anything not matched by a rule or rate-limited is
   **unconditionally `Redirect`ed**, incrementing `redirected_packets`. This is a real, documented
   gap: the inline comment above this line says "Redis port 6379 or cluster bus", but the code
   never actually inspects the destination port it parsed in step 4 to make that decision — every
   otherwise-unfiltered packet is redirected regardless of port.
8. **Non-IP input** (empty `src_ip`, e.g. a plain RESP/inline-text payload with no recognizable
   Ethernet/IPv4 header): skips rule/rate-limit evaluation entirely (steps 5-6) and falls straight
   through to the same unconditional `Redirect` in step 7. In practice this is the common case for
   `XDP.INJECT`-driven traffic, since injected payloads are typically raw RESP bytes, not
   wire-format Ethernet frames (though `extract_transport_payload`, §4.3, can also strip real
   Ethernet/IP/TCP headers if a caller does inject full wire-format frames).

### 4.2 The real, wired ingestion loop — `src/server.rs`, per shard, at boot

Every shard's `run_shard_worker` (`src/server.rs:47`) spawns a `monoio` background task
(`server.rs:270-314`) that continuously polls that shard's own `XskSocket`:

```rust
// server.rs:270-314 (annotated)
let xdp_engine = crate::xdp::get_xdp_engine();                              // shared singleton
let xsk_socket = xdp_engine.get_or_create_socket(shard_port, shard_id as u32); // queue_id = shard_id
let xdp_db = local_db.clone();
let xdp_aof = aof_writer.clone();
let xdp_router = router.clone();
monoio::spawn(async move {
    let mut frames = Vec::with_capacity(32);
    loop {
        let count = xsk_socket.rx_burst(&mut frames, 32);
        if count > 0 {
            for frame in frames.drain(..) {
                let action = xdp_engine.process_packet(&frame);
                if (action == XdpAction::Pass || action == XdpAction::Redirect)
                    && let Some(cmd_payload) = crate::xdp::extract_transport_payload(&frame)
                {
                    let mut b_mut = bytes::BytesMut::from(&cmd_payload[..]);
                    if let Ok(Some(cmd)) = crate::resp::parse_command(&mut b_mut) {
                        let target_sid = crate::connection::target_shard_of_cmd(&cmd, xdp_router.num_shards)
                            .unwrap_or(xdp_router.shard_id);
                        let mut out = Vec::new();
                        if target_sid == xdp_router.shard_id {
                            let mut db = xdp_db.borrow_mut();
                            let _ = crate::connection::execute_local_command(
                                &cmd, &mut db, &mut out, xdp_aof.as_ref().map(|a| a.as_ref()),
                            );
                        } else {
                            let resp = xdp_router.execute_remote(target_sid, cmd).await;
                            out.extend_from_slice(&resp);
                        }
                        if !out.is_empty() {
                            xsk_socket.tx_burst(&[&out]);
                        }
                    }
                }
            }
        } else {
            monoio::time::sleep(Duration::from_millis(5)).await;
        }
    }
});
```

Walkthrough:
1. **Socket identity**: `get_or_create_socket(shard_port, shard_id as u32)` — the `queue_id` for
   each shard's *polled* socket is exactly that shard's own `shard_id`, and `shard_port` is the
   same port value passed to `Router::new` and stored as `Router::port` (`src/router.rs:159`), so
   `XDP.SOCKET`/`XDP.INJECT` issued from *any* connection on *any* shard use the same `port` value
   and can reach any shard's polling ring by matching `queue_id` (§3.7's cross-shard-injection
   point).
2. **Poll loop cadence**: drains up to 32 frames per `rx_burst` call; if the burst was empty,
   sleeps 5 ms before retrying. Busy shards process a full batch with no sleep; idle shards poll
   at ~200 Hz.
3. **Action filter**: only `Pass` and `Redirect` actions proceed to command extraction; `Drop` and
   `Tx` classifications are discarded with no further effect (there is no code that acts on `Tx`
   specially here — it is simply excluded from this `if`).
4. **Header stripping**: `extract_transport_payload` (§4.3) turns the raw frame into a presumed
   RESP/inline command payload.
5. **RESP parsing**: `crate::resp::parse_command` is called directly on a `BytesMut` wrapping the
   extracted payload — the same parser real client connections use (Component 03). A parse
   failure (`Err` or `Ok(None)`, e.g. incomplete/malformed input) silently drops the frame with no
   counter or log.
6. **Routing decision**: `target_shard_of_cmd(&cmd, num_shards)` (`src/connection.rs:12338`)
   contains an explicit `match` over a few hundred lines covering single-key commands (`GET`,
   `SET`, `HSET`, `SADD`, `ZADD`, `LPUSH`, etc. — each mapped to `target_shard(key, num_shards)`)
   and ends in a catch-all `_ => None` (confirmed at `connection.rs:12687`). Any command **not**
   covered by that explicit list — including multi-key commands, admin commands, and even the
   `XDP.*` commands themselves if somehow injected — falls through to `None`, which
   `.unwrap_or(xdp_router.shard_id)` resolves to **the polling shard's own id**, i.e. it executes
   locally regardless of true key ownership. This means routing correctness for the injection path
   depends entirely on whether the parsed command happens to be one of the explicitly-listed
   single-key variants.
7. **Execution**: if `target_sid == xdp_router.shard_id`, the command runs through
   `execute_local_command` (`connection.rs:12734`) directly against `local_db` — **the real
   shard state**, with AOF logging applied the same as any client-issued write. Otherwise it is
   shipped via `Router::execute_remote(target_sid, cmd)` (`router.rs:2209`, `async fn ... ->
   Vec<u8>`), the same cross-shard mailbox mechanism (Component 04) an ordinary client connection
   uses for cross-shard commands.
8. **Response**: the raw RESP response bytes are written back via `xsk_socket.tx_burst(&[&out])`
   — into the same socket's `tx_ring`/`comp_ring` the inbound frame arrived from. Nothing ever
   consumes `tx_ring`/`comp_ring` to transmit bytes over a real network; `XDP.SOCKET` merely
   reports their lengths (§4.3).

So the pipeline **is** fully wired end to end: a byte payload injected into `rx_ring` is
classified (CIDR/rate-limit), has its RESP command extracted, is **actually executed against the
shard's real database** (locally or via a genuine cross-shard call), and the real response is
written back into the socket's simulated `tx_ring`/`comp_ring`. What is *not* real is how a
payload gets into `rx_ring` in the first place: the only producer is `XskSocket::inject_rx`
(§3.6), called exclusively from the `XDP.INJECT` command handler (§4.3) — there is still no eBPF
program, raw socket, or NIC handing frames to this ring.

### 4.3 The `XDP.*` command family — six command names, eight `Command` variants

`src/resp.rs` parses the command strings at lines 12968-13030. Correction from a prior revision of
this document: rule management is **one** command name, `XDP.RULE`, with `ADD`/`DEL`/`LIST`
subcommands — not three separate top-level commands.

| Command | Parse site (`resp.rs`) | `Command` variant (`resp.rs:1706-1720`) | Dispatch site (`connection.rs`) | Effect |
| :--- | :--- | :--- | :--- | :--- |
| `XDP.INFO` | 12968 | `XdpInfo` | 11717 | Formats `XdpEngine::info()` as a bulk string — ifname, mode, frame size/count, rule/socket counts, all six atomic counters. |
| `XDP.RULE ADD <action> <cidr>` | 12969-12991 | `XdpRuleAdd { action, cidr }` | 11722 | `add_rule` — pushes a new `XdpRule`; replies with the new rule's integer id. `<action>` must be one of `DROP`/`PASS`/`REDIRECT`/`TX` (case-insensitive via `.to_uppercase()`), else `-ERR Unknown XDP action: ..`. |
| `XDP.RULE DEL <id>` | 12992-13001 | `XdpRuleDel(u32)` | 11729 | `del_rule` — removes by id; `-ERR` if not found. |
| `XDP.RULE LIST` | 13003 | `XdpRuleList` | 11736 | `list_rules` — clones and returns the rule vector as a RESP array of `"id:N action:X cidr:Y"` bulk strings. |
| `XDP.STATS` | 13007 | `XdpStats` | 11745 | Returns 6 key/value pairs (12-element RESP array) for `rx_packets`, `rx_bytes`, `dropped_packets`, `redirected_packets`, `pass_packets`, `rate_limit_drops`. |
| `XDP.PACKET <bytes>` | 13008-13013 | `XdpPacket(Bytes)` | 11766 | Calls `process_packet` **directly**, bypassing the ring entirely — a synchronous classify-and-reply request/response (`+PASS`/`+DROP`/`+REDIRECT`/`+TX`), not a ring injection. Never touches a command execution path. |
| `XDP.SOCKET <queue_id>` | 13014-13020 | `XdpSocket(u32)` | 11772 | `get_or_create_socket(router.port, qid)` (creates if absent — **not** a read-only lookup), reports `queue_id`, `rx_len`, `fill_len`, `tx_len` as a 4-pair/8-element RESP array. |
| `XDP.INJECT <queue_id> <bytes>` | 13021-13030 | `XdpInject { queue_id, payload }` | 11786 | `get_or_create_socket(router.port, queue_id).inject_rx(payload)` — the **only** producer for the background loop in §4.2. Replies `+OK` on success, `-ERR ring full` if `inject_rx` returned `false` (either no free UMEM frame or `rx_ring` already full). |

All eight variants are tagged under the ACL/stats category `"XDP"` by the command-name classifier
at `connection.rs:4306-4313`.

`extract_transport_payload` (xdp.rs:544-583) mirrors `process_packet`'s frame-shape detection to
strip Ethernet+IPv4+TCP headers:
- **Ethernet+IPv4+TCP** (`packet.len() >= 54 && packet[12..14] == [0x08,0x00] && packet[14+9] ==
  6`): computes the IPv4 header length from the IHL nibble (`(packet[14] & 0x0F) * 4`), then the
  TCP header's data offset from its own nibble, and returns everything after both headers.
- **Raw IPv4+TCP** (`packet.len() >= 40 && (packet[0] >> 4) == 4 && packet[9] == 6`): same
  computation without the 14-byte Ethernet prefix.
- **Direct RESP** (first byte is `*`, `+`, `$`, `:`, or `-`): returns the entire input unchanged.
- **Anything else** (including too-short inputs that don't match either header shape): falls
  through to the final `else` arm and is still returned unchanged as an "inline text command" —
  the only way to get `None` back is an **empty** input packet (checked at function entry) or a
  TCP/IP frame whose declared header lengths overrun the actual buffer (the bounds checks inside
  the two header branches return early without hitting any `return Some(..)`, falling through to
  the implicit `None` at the end of the function).

### 4.4 `ZeroCopyEngine::send_zc` — real syscall usage, verified unreachable

```rust
pub fn send_zc(&self, fd: RawFd, data: &[u8]) -> io::Result<usize> {   // zerocopy.rs:200-246
    let flags = if data.len() >= PAGE_SIZE {
        libc::MSG_NOSIGNAL | MSG_ZEROCOPY
    } else {
        libc::MSG_NOSIGNAL
    };
    let ret = unsafe { libc::send(fd, data.as_ptr() as *const _, data.len(), flags) };
    // on success: record zc_send_calls / zc_bytes_sent
    // on ENOBUFS, or whenever MSG_ZEROCOPY was requested: fall back to a plain blocking send()
}
```

Correct, idiomatic use of Linux's zero-copy send path — including the size threshold
(`PAGE_SIZE`, 4 KiB) below which plain `send()` is used, since `MSG_ZEROCOPY` incurs page-pinning
overhead not worth paying for small payloads. An empty `data` short-circuits to `Ok(0)` with no
syscall. The fallback branch (zerocopy.rs:225-243) triggers on `ENOBUFS` *or* whenever
`MSG_ZEROCOPY` was in the flags at all — i.e. it unconditionally retries with a plain blocking
`send()` whenever the zero-copy attempt didn't cleanly succeed, not only on the documented
"kernel buffer full" case; `fallback_sends` is incremented whenever this retry path runs.

**What's missing**: Linux's `MSG_ZEROCOPY` contract requires the caller to poll `MSG_ERRQUEUE` via
`recvmsg` for a completion notification before the source buffer can be safely reused or freed —
`send_zc` does not do this anywhere in the file. Even a hypothetically wired-up caller would have
no correct way to know when the kernel has finished the zero-copy DMA. `fd: RawFd` is a raw file
descriptor; `connection.rs`'s real sockets are `monoio::net::TcpStream` objects whose underlying
fd is never extracted or passed to this function anywhere in the codebase — **confirmed by grep
(`rg -n "zerocopy::" src/*.rs`): zero callers outside `zerocopy.rs`'s own `#[cfg(test)]` module.**

`build_io_uring_send_zc` (zerocopy.rs:249-260) similarly constructs a real
`io_uring::opcode::SendZc` entry via the genuine `io-uring` crate dependency (also used, for
unrelated purposes, by Component 07's tiered-storage direct I/O) — but nothing ever submits the
resulting entry to a ring; `enable_so_zerocopy` (zerocopy.rs:180-196) is a correct
`setsockopt(SOL_SOCKET, SO_ZEROCOPY, 1)` wrapper, also with zero callers outside its own tests.

---

## 5. Cross-Component Interactions

- **`src/server.rs`** (Component 01): spawns the per-shard `XskSocket` polling task inside
  `run_shard_worker` at boot (§4.2) — real integration, not merely a comment. It runs alongside,
  and independently of, the shard's real `monoio` `TcpListener` accept loop; the two never
  interact or share any state beyond both ultimately calling into the same `execute_local_command`
  / `Router` machinery.
- **`src/connection.rs`**: dispatches all eight `Xdp*` `Command` variants (`connection.rs:11717-
  11795`, §4.3), each forwarding to `crate::xdp::get_xdp_engine()`. Also supplies
  `target_shard_of_cmd` (12338) and `execute_local_command` (12734), both reused unmodified by the
  §4.2 ingestion loop.
- **`src/resp.rs`**: parses the six `XDP.*` command name strings into the eight `Command` enum
  variants above (`resp.rs:12968-13030`), including mapping `"DROP"`/`"PASS"`/`"REDIRECT"`/`"TX"`
  strings to `XdpAction` for `XDP.RULE ADD`.
- **`src/router.rs`**: `Router::execute_remote` (`router.rs:2209`) is reused, unmodified, by the
  §4.2 ingestion loop to route a command extracted from an injected packet to its owning shard
  when `target_shard_of_cmd` resolves to a different shard than the one polling the socket — the
  same cross-shard mailbox mechanism (Component 04) an ordinary client connection uses. `Router`
  also supplies the `port: u16` field (`router.rs:159`) that both the boot-time socket
  registration and the `XDP.SOCKET`/`XDP.INJECT` handlers key their sockets on.
- **`src/main.rs`** / **`src/lib.rs`**: `lib.rs:36-37` declares both modules unconditionally;
  `main.rs` has zero direct references to either — all wiring happens inside `server.rs`.
- **`src/zerocopy.rs`**: no cross-component interactions — verified zero callers outside its own
  test module, across the entire `src/*.rs` tree.

---

## 6. Future Improvements

- **High-priority decision, not a fix: decide whether either file has a real future, and act
  accordingly.** The §4.2 loop means `xdp.rs` is more thoroughly wired than "dead code with
  tests," but it is still only reachable via an explicit `XDP.INJECT` admin command, never by real
  network traffic. Either invest in real `AF_XDP`/`XSK` syscalls (a genuine `aya`/`libbpf`
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
- **Medium — `target_shard_of_cmd`'s silent fallback to local execution** (§4.2 step 6) means any
  command not in its explicit single-key whitelist (multi-key commands, admin commands, etc.)
  injected via `XDP.INJECT` executes on whichever shard happened to be polling the ring it was
  injected into, not necessarily the shard that actually owns the affected key(s) — a correctness
  trap for anyone using `XDP.INJECT` to drive real traffic against a multi-shard keyspace.
- **Low — bound `rate_limiters`' unbounded growth** (§3.3): one `TokenBucket` per distinct source
  IP ever classified, never evicted. Low risk while only reachable via explicit commands, but
  worth a periodic sweep if ever exposed more broadly.
- **Low — remove the dead `UmemFrame` struct** (§3.5): declared, never constructed or referenced
  anywhere else in the crate.

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1 — UMEM footprint is per-shard, and depends on which `XdpMode` is actually selected.**
  Every shard creates its own `XskSocket`/`XskUmem` at boot (§4.2), and `num_frames` is decided
  once, process-wide, by whether `/sys/class/net` exists (§3.1) — true on essentially all real
  Linux hosts, which selects `Skb` mode with **4096 frames × 2048 B = 8 MiB of UMEM per shard**
  (not the 128-frame/256 KiB `Simulated` figure, which only applies on hosts without `sysfs`). On
  a 16-shard instance that is roughly 128 MiB allocated at boot for a subsystem that, absent an
  explicit `XDP.INJECT` call, never receives a packet.
* **Gotcha 2 — `XDP.INJECT` is not a diagnostic no-op.** Because of the §4.2 background loop, an
  injected payload that parses as a valid RESP command is **actually executed against real shard
  state** (same code path as a normal client command, including AOF logging) and can mutate the
  keyspace. Treat `XDP.INJECT` as equivalent to sending a command on an ordinary connection, not
  as an inert test hook.
* **Gotcha 3 — `XDP.PACKET` and `XDP.INJECT` exercise different code paths.** `XDP.PACKET` calls
  `process_packet` directly and only ever returns a classification (`PASS`/`DROP`/`REDIRECT`/
  `TX`); it never touches a ring and never executes a command. `XDP.INJECT` goes through the ring
  and, if the payload parses as RESP/inline and is not filtered (classified `Pass` or `Redirect`),
  does execute a command. Don't conflate the two when writing tests or reasoning about side
  effects.
* **Gotcha 4 — the `queue_id` argument to `XDP.INJECT`/`XDP.SOCKET` is not scoped to the issuing
  connection's shard.** Because `XdpEngine` and its `sockets` map are one process-wide singleton
  (§3.1, §3.7), `XDP.INJECT <queue_id> <payload>` on *any* connection, on *any* shard, reaches the
  background polling loop of **whichever shard's `shard_id` equals `queue_id`** — including a
  shard other than the one handling the current connection. Using a `queue_id` that doesn't match
  any shard's id creates an orphaned socket that nothing ever polls; injected packets there sit in
  `rx_ring` forever (bounded by ring capacity, after which further injects simply fail with `-ERR
  ring full`).
* **Gotcha 5 — command routing inside the ingestion loop is a partial whitelist, not a general
  router.** `target_shard_of_cmd` (`connection.rs:12338`) only resolves ~a few hundred lines worth
  of explicitly-matched single-key command variants to their owning shard; everything else
  (`_ => None`, confirmed at `connection.rs:12687`) defaults to local execution on the polling
  shard via `.unwrap_or(xdp_router.shard_id)`. Don't assume an injected multi-key or
  non-key-addressed command gets routed correctly.
* **Gotcha 6 — `RegisteredBufferPool`/`ZeroCopyEngine` are correct but fully inert.** `libc::send`
  with `MSG_ZEROCOPY` is real, page-alignment is real, `io_uring::opcode::SendZc` construction is
  real — but there is no caller anywhere outside `zerocopy.rs`'s own tests, and no completion
  (`MSG_ERRQUEUE`) polling even if one were added.
* **Gotcha 7 — the dead `UmemFrame` struct (xdp.rs:114-118) is not part of any live data path.**
  `XskUmem` stores raw `Vec<u8>` buffers directly; `UmemFrame` is declared but never constructed.
  Don't spend time tracing it expecting to find a call site.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
