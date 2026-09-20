# Component 19: Pub/Sub Messaging Hub (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/pubsub.rs`  
> **Implementation Reference**: [`docs/internal/19_pubsub.md`](../internal/19_pubsub.md)  
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
Shared-nothing Pub/Sub messaging. Uses a 16-stripe atomic presence bitmask (ShardedPresenceTable) to eliminate cross-shard broadcast storms, and Redis 7 slot-bound sharded pub/sub (SPUBLISH) for point-to-point routing.

### 2.2 Design Rationale (The "Why")
Global PUBLISH in shared-nothing architectures causes broadcast storms across all worker cores. The striped presence bitmask lets publishers bypass uninterested shards entirely, cutting cross-core hops by up to 90%.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **Genuinely per-shard, not a `BlockHub`/`ACL`/search-registry-style global exception.**
   `Router.pubsub: Rc<RefCell<PubSubHub>>` — a plain `Rc`/`RefCell`, exactly like `ShardDb`
   itself, with no `Arc`/`Mutex` anywhere. A subscriber's registration (`channels`,
   `patterns`, `clients`, `client_channels`, `client_patterns`) only ever exists in the
   `PubSubHub` of the one shard that accepted that subscriber's connection.
2. **Cross-shard delivery is a real, parallel fan-out — send-then-await, matching the
   squashed-pipeline pattern.** `Router::publish` (Component 04) delivers to local
   subscribers immediately, then dispatches one `ShardMessage::Publish` to every *other*
   shard (all sends issued before any await), and sums each shard's returned delivery count —
   the same "dispatch all, then await all" shape used throughout the codebase for
   parallelism (Components 02/04).
3. **A hand-written glob matcher, not a regex or a crate.** `glob_match` implements `*`/`?`
   wildcard matching via an explicit backtracking scan (tracking the last `*` position and
   resuming from there on a mismatch) rather than compiling a regex or pulling in a glob
   crate — used both for `PSUBSCRIBE` pattern matching against published channels and for
   `PUBSUB CHANNELS <pattern>`'s filtering.
4. **No RESP3 push-type framing anywhere in this file — verified.** Every delivered message
   (`message`/`pmessage`) and every subscribe/unsubscribe confirmation is hard-coded RESP2
   array framing (`*3\r\n$7\r\nmessage\r\n...`); grepping this file for `is_resp3`/`resp3`
   finds zero matches. A RESP3-negotiated client (Component 02's `CURRENT_CLIENT_RESP3`
   machinery) still receives ordinary array-type frames for pub/sub messages instead of the
   RESP3 push type (`>3\r\n...`) real Redis sends once a client has opted into RESP3 — see §7.
5. **A subscriber count of zero triggers cleanup, not a lingering empty entry.** Every
   `unsubscribe`/`punsubscribe`/`unsubscribe_all`/`punsubscribe_all` path removes the
   channel/pattern's `HashSet` entirely once it's empty (not left as an empty set), and
   removes the client's `flume::Sender` from `clients` once its last subscription anywhere
   drops to zero (`total_subscriptions(client_id) == 0`) — no unbounded growth from
   subscribe/unsubscribe churn.

---

## 3. High-Level Architecture & Workflow Diagram

```
PUBLISH "news" "hello" ──► Check ShardedPresenceTable Bitmask
                                       │
                        ┌──────────────┴──────────────┐
                        ▼                             ▼
                 Shard 0 (Present)             Shard 2 (Present)
                 (Deliver to Clients)          (Deliver to Clients)
                 [Shards 1, 3..15 bypassed with ZERO channel messages]
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Direct-channel publish is O(subscribers to that channel)** — no overhead from unrelated
  channels or patterns.
- **Pattern publish is O(total registered patterns) per publish**, not O(matching patterns)
  (§4.2) — a deployment with many distinct active `PSUBSCRIBE` patterns pays a glob-match per
  pattern on every single `PUBLISH`, regardless of how many (if any) actually match.
- **Cross-shard fan-out cost is O(num_shards) per `PUBLISH`, done in parallel** (§2.2) — every
  publish touches every other shard's `PubSubHub` once via a `flume` message, dispatched
  concurrently rather than sequentially, matching the codebase's general cross-shard fan-out
  pattern.
- **One frame is built once and cloned per subscriber**, not re-serialized per recipient —
  the `Vec<u8>` frame construction cost is paid once regardless of subscriber count.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/19_pubsub.md`**](../internal/19_pubsub.md): Low-level implementation and code reference.
* **Source Files**: `src/pubsub.rs`
