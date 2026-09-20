# Component 10: Kernel Bypass & Zero-Copy Networking — Design & Architecture

> **Subsystem Scope**: `src/xdp.rs`, `src/zerocopy.rs`
> **Implementation Reference**: [`docs/internal/10_kernel_bypass_xdp.md`](../internal/10_kernel_bypass_xdp.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Purpose and Current Status

`src/xdp.rs` and `src/zerocopy.rs` model two Linux kernel-bypass networking techniques —
AF_XDP (`XSK`) zero-copy packet rings and `MSG_ZEROCOPY`/registered `io_uring` buffers — as a
self-contained, pure-userspace subsystem, exposed to clients as an `XDP.*` command family.

**This subsystem does not perform real kernel bypass today, and is not on the server's live
network path.** It is not gated behind a Cargo feature flag or a `#[cfg(...)]` attribute —
`src/xdp.rs` and `src/zerocopy.rs` are unconditionally compiled into every build — but neither
module makes an `AF_XDP` socket syscall, loads an eBPF program, or binds to a real network
interface. Concretely:

- **`src/xdp.rs`** implements the AF_XDP data structures (UMEM, fill/completion/Rx/Tx rings,
  socket) entirely as in-process Rust collections (`Vec`, atomics, `RwLock`), and reaches them
  only through explicit `XDP.*` admin commands or a background polling task (Component 01's
  server loop) that drains a ring nothing external ever populates. The only way a real packet
  payload enters this pipeline is a client explicitly calling `XDP.PACKET <bytes>` or
  `XDP.INJECT <queue> <bytes>` — there is no code path from an actual NIC, raw socket, or a real
  XDP/eBPF program into these rings.
- **`src/zerocopy.rs`** (`RegisteredBufferPool`, `ZeroCopyEngine`) implements real, correct
  low-level primitives — page-aligned buffer allocation, `SO_ZEROCOPY`/`MSG_ZEROCOPY` socket
  send with fallback, and `io_uring::opcode::SendZc` entry construction — but nothing in the
  codebase outside its own unit tests constructs a `ZeroCopyEngine` or calls `send_zc`. The
  server's actual connection write path (`src/connection.rs`, built on `monoio`) does not use
  this module.

The `XdpMode` a running instance reports (`Driver`, `Skb`, or `Simulated`) is cosmetic: the
global engine picks `Skb` if `/sys/class/net` exists on the host and `Simulated` otherwise, but
this only changes what `XDP.INFO` prints as a string — it does not attach to a real interface,
does not change how `process_packet` behaves, and `Driver` mode is never actually selected by
any code path (it exists as an enum variant with no constructor reaching it).

## 2. Why Kernel Bypass Exists as a Design Direction

### 2.1 The problem it targets

Rudis's default networking path (Component 01) already uses Linux `io_uring` via `monoio` for
asynchronous, batched I/O — a substantial improvement over classic blocking or `epoll`-based
socket I/O. At sufficiently high packet rates, however, even `io_uring` still routes every
packet through the kernel's general network stack: `sk_buff` allocation, netfilter hooks, and
the TCP/IP stack's own processing overhead are paid per packet regardless of how efficiently
userspace consumes the result. **AF_XDP** is Linux's mechanism for bypassing that stack
entirely for a specific NIC queue: an eBPF program attached at the driver (or generic/`SKB`)
layer redirects raw frames directly into a userspace-visible ring buffer (the UMEM), skipping
`sk_buff` construction and the general stack for that traffic. **`MSG_ZEROCOPY`/registered
buffers** address a related but distinct cost — avoiding a kernel-to-userspace memory copy on
the *send* path by letting the kernel DMA directly from pinned/registered userspace buffers.

### 2.2 The trade-off against the default `io_uring` path

Real AF_XDP kernel bypass is a substantially larger undertaking than the `io_uring` path Rudis
already runs on, with real costs: it requires an eBPF program (typically via a crate such as
`aya` or the `libbpf` C bindings), root or `CAP_NET_ADMIN`/`CAP_BPF` privileges, and depends on
the NIC driver's own AF_XDP support (driver-mode bypass is not universally available; many
deployments would fall back to slower `SKB`/generic mode, which re-enters much of the kernel
stack it was meant to avoid). It also forgoes the kernel's own protocol handling — a raw AF_XDP
consumer must parse Ethernet/IP/TCP framing itself and reimplement whatever the kernel would
otherwise have done, which is real ongoing engineering surface, not a one-time cost.
`io_uring`-based sockets, by contrast, still benefit from the kernel's TCP stack (congestion
control, retransmission, connection state) while getting most of the syscall-batching and
zero-copy-friendly buffer registration benefit `monoio` already exploits. The honest framing of
this trade-off is: kernel bypass buys lower per-packet CPU cost at very high, sustained packet
rates, at the price of reimplementing transport-layer concerns Rudis currently gets for free
from the kernel, plus deployment requirements (root/capabilities, driver support) the default
`io_uring` path does not have.

### 2.3 Present state relative to that trade-off

Because `src/xdp.rs` does not yet make the real AF_XDP syscalls or attach a real eBPF program,
Rudis today has not actually made that trade — it has built and unit-tested the *userspace data
structures and packet-classification logic* (CIDR allow/drop rules, per-source-IP token-bucket
rate limiting, ring-buffer bookkeeping) that a real AF_XDP integration would eventually sit
behind, exercised through an admin-command interface rather than live traffic. This should be
read as a testable simulation and a foundation for a future integration, not as a currently
available performance feature. Anyone evaluating Rudis for kernel-bypass networking should treat
this subsystem as experimental/aspirational until it is wired to real `AF_XDP`/`XSK` sockets and
a real network interface.

## 3. What Is Real and Useful Today

Independent of the "kernel bypass" framing, the packet-classification logic itself is real,
correct, and unit-tested userspace code:

- A CIDR-based rule table (`XDP.RULE ADD/DEL/LIST`) supporting `DROP`/`PASS`/`REDIRECT`/`TX`
  actions per source-IP prefix, evaluated as a linear scan (acceptable given rule tables are
  expected to be small).
- A per-source-IP token-bucket rate limiter, created lazily on first sight of an IP and never
  evicted (see the internal doc for the resulting unbounded-growth characteristic).
- Byte-accurate Ethernet/IPv4/TCP header parsing sufficient to extract a source IP and, for TCP,
  a destination port, from either a raw Ethernet frame or a bare IPv4 packet.

These are genuinely useful as a standalone, testable packet-filtering/rate-limiting engine; they
simply are not, today, interposed on real traffic.

## 4. No Performance Claims

Because neither module is on the live network path, **no line-rate, packets-per-second, or CPU
overhead numbers are claimed for this subsystem** — any such figures would necessarily be
invented, since there is no real traffic flowing through it to measure. The only performance
statement that can be made honestly is algorithmic: the CIDR rule scan and rate-limiter lookup
are cheap, small-N operations, and the ring-buffer data structures are lock-free single-producer/
single-consumer designs appropriate for a real AF_XDP integration if one is built — but their
cost under real line-rate traffic has not been measured because they do not yet see any.

## 5. Implementation Reference

For concrete struct/field layouts, the packet-classification algorithm, the ring-buffer
implementation, and exactly which code paths are and are not connected to anything, see
[`docs/internal/10_kernel_bypass_xdp.md`](../internal/10_kernel_bypass_xdp.md).
