# Component 03: RESP Protocol Engine & Command Parser (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/resp.rs`  
> **Implementation Reference**: [`docs/internal/03_resp_engine.md`](../internal/03_resp_engine.md)  
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
Zero-copy parsing over borrowed byte slices. Converts RESP2 arrays (*3\r\n...), RESP3 types, and inline space-separated commands into strongly-typed Command enums without intermediate string copies.

### 2.2 Design Rationale (The "Why")
Memory allocation and string copying during command parsing dominate CPU profiles in high-QPS benchmarks. Rudis parses command frames in place using bytes::Bytes slices, achieving zero-allocation parsing for all hot-path commands.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **Zero-Copy RESP Array Parsing**: Bulk string arguments inside a `*N\r\n...` frame are
   extracted via `BytesMut::split_to(len).freeze()` — a reference-count bump on the
   underlying buffer, never a byte-for-byte copy.
2. **All-or-Nothing Frame Consumption**: A partially buffered command (still waiting on more
   bytes from the socket) leaves the input buffer completely untouched (`Ok(None)`); a
   complete command is parsed and fully consumed in one call. There's no partial-consumption
   state to track between calls.
3. **Three Independent Input Grammars, One Entry Point**: `parse_command` recognizes RESP
   arrays (`*...`), plain space-separated inline text (`GET foo\r\n`), and Memcached's ASCII
   storage-command grammar (`set key flags exptime bytes\r\n<data>\r\n`) — dispatched purely
   by the first byte of the buffer, or by trial-parsing for the Memcached case (see §4.1).
4. **No RESP3 wire-format parsing in this file.** `HELLO` is recognized and parsed as a
   `Command` (so a client can request protocol v3), but nothing in `resp.rs` parses RESP3
   input types (maps `%`, sets `~`, doubles `,`, booleans `#`, nulls `_`, pushes `>`) — every
   incoming command is still a flat array of `$`-prefixed bulk strings. RESP3 is purely an
   *output*-side concern implemented in `connection.rs` (a per-client `is_resp3` flag and a
   thread-local `CURRENT_CLIENT_RESP3` cell gate which reply format gets written).

---

## 3. High-Level Architecture & Workflow Diagram

```
Raw Ingress Bytes: *3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nalice\r\n
                                      │
                         Zero-Copy Group Probing
                                      │
                         Command::Set { key: Bytes("user"), val: Bytes("alice") }
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Zero-copy on the hot (RESP array) path**: every bulk-string argument is a `Bytes` slice
  sharing the original read buffer's allocation, not a fresh heap copy.
- **The inline and Memcached paths copy**: both exist for compatibility/interactive use, not
  throughput, and neither is on the benchmarked pipeline path.
- **`build_command`'s dispatch is a single string match, not a lookup table**: with 200+ arms,
  this is a large `match` the compiler is left to optimize (typically into some mix of
  length-bucketed comparisons/jump tables); no bespoke perfect-hash or trie dispatch was
  built for it.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/03_resp_engine.md`**](../internal/03_resp_engine.md): Low-level implementation and code reference.
* **Source Files**: `src/resp.rs`
