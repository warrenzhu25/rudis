# Component 10: Kernel Bypass & Zero-Copy Networking (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/xdp.rs, src/zerocopy.rs`  
> **Implementation Reference**: [`docs/internal/10_kernel_bypass_xdp.md`](../internal/10_kernel_bypass_xdp.md)  
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
Hardware kernel bypass for line-rate networking. Ingress network frames bypass standard kernel socket stacks using AF_XDP (XSK) driver UMEM rings, pre-registered io_uring fixed buffers, and Linux SO_ZEROCOPY.

### 2.2 Design Rationale (The "Why")
At millions of QPS, Linux kernel network stack overhead (sk_buff allocations, netfilter, page table walks) consumes up to 40% of CPU cycles. AF_XDP reads raw packets directly into userspace driver rings.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **`XdpEngine` is a single global, not per-shard**: `get_xdp_engine()` returns a clone of an
   `Arc<XdpEngine>` behind a process-wide `static GLOBAL_XDP_ENGINE: LazyLock<Arc<XdpEngine>>`
   — every shard thread that calls `XDP.*` commands shares the exact same instance, coordinated
   via `RwLock<Vec<XdpRule>>` and `RwLock<HashMap<u32, TokenBucket>>` (a real, if narrow,
   exception to the shared-nothing model, similar in shape to `BlockHub` in Component 06).
2. **`XdpMode` is cosmetic, not functional**: the global engine picks `XdpMode::Skb` if
   `/sys/class/net` exists on the host, else `XdpMode::Simulated` — this only changes what
   `XDP.INFO` reports as a string; it does not change `process_packet`'s behavior or attach
   anything to a real interface in either mode.
3. **`RegisteredBufferPool` owns raw allocated memory directly** (`alloc_zeroed`/`dealloc` via
   `std::alloc`, not a `Vec`), page-aligned via `Layout::from_size_align(total_size, PAGE_SIZE)`,
   with a manual `unsafe impl Send + Sync` — correct in isolation, but again: nothing in the
   codebase actually constructs one outside its own test.
4. **`ZeroCopyEngine::send_zc` degrades gracefully**: only applies `MSG_ZEROCOPY` for payloads
   `>= PAGE_SIZE` (4KB, per a comment citing page-locking overhead for smaller sends), and on
   `ENOBUFS` (kernel zero-copy completion queue full) or general zero-copy failure, retries once
   with a plain blocking `send()` — this fallback logic is real and correct, it's just never
   invoked by anything.

---

## 3. High-Level Architecture & Workflow Diagram

```
NIC Hardware ──► eBPF XDP Driver ──► AF_XDP UMEM Ring ──► Worker Thread
       (Bypasses standard Linux kernel network stack & socket buffers)
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **No measured network-layer performance benefit exists from either file.** `xdp.rs`'s cost is
  whatever it costs to run `process_packet` once per `XDP.PACKET` command a client explicitly
  sends — i.e., it's exercised at whatever rate a test or admin script chooses to call it, not
  at line rate against real traffic. `zerocopy.rs` is entirely inert in the running server.
- **The token-bucket rate limiter and CIDR rule table are real, correct, O(1)-per-packet
  (rules are a linear scan, but the rule list is expected to be small) userspace logic** — useful
  as a testable filter-policy engine, just not connected to anything that would make it a DDoS
  defense in practice.
- Any performance claims in the previous version of this document (100GbE line-rate, "28 million
  packets/sec", "&lt;5% CPU utilization" for `MSG_ZEROCOPY`) were invented and have been removed;
  none of it has ever been benchmarked because none of it runs on the real request path.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/10_kernel_bypass_xdp.md`**](../internal/10_kernel_bypass_xdp.md): Low-level implementation and code reference.
* **Source Files**: `src/xdp.rs, src/zerocopy.rs`
