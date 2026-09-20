# Component 19: Pub/Sub Messaging Hub — Design & Architecture

> **Subsystem Scope**: `src/pubsub.rs` (cross-shard routing lives in `src/router.rs`)
> **Implementation Reference**: [`docs/internal/19_pubsub.md`](../internal/19_pubsub.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Purpose

Rudis implements Redis's Publish/Subscribe messaging model: clients register interest in
named **channels** (`SUBSCRIBE`) or **glob patterns** over channel names (`PSUBSCRIBE`), and a
`PUBLISH` on a channel fans a message out to every matching subscriber, with at-most-once,
best-effort delivery semantics identical to upstream Redis (no persistence, no acknowledgment,
no replay — a subscriber that is not connected when a message is published never sees it).
Redis 7 sharded Pub/Sub (`SPUBLISH`/`SSUBSCRIBE`/`SUNSUBSCRIBE`) is also implemented, giving
cluster deployments a channel class whose messages are routed point-to-point instead of
broadcast.

## 2. Design Rationale ("Why")

### 2.1 The core tension: shared-nothing shards vs. a globally addressable namespace

Rudis is a thread-per-core, shared-nothing server (Component 01/04): each shard owns its own
event loop and its own thread-local state, communicating with other shards only through
explicit message passing over `flume` channels, never through shared mutable memory. Pub/Sub
subscriptions, however, are fundamentally global: a client subscribed on shard 0 must receive a
message published by a client connected to shard 7, even though the two connections share
nothing. A naive implementation has only two choices, both bad at scale:

- **Broadcast every `PUBLISH` to every shard.** Correct, but wasteful: on a 16-shard node, a
  channel with a single subscriber on one shard still costs 15 wasted cross-shard messages per
  publish, and the cost scales linearly with shard count regardless of how few shards actually
  care.
- **Centralize subscription state in one global, lock-protected hub.** Avoids the fan-out
  waste but reintroduces exactly the lock contention and cache-line bouncing the thread-per-core
  design exists to eliminate (see Component 01 §1 for the general argument), turning every
  `PUBLISH`/`SUBSCRIBE` into a point of cross-core synchronization.

### 2.2 Rudis's answer: per-shard hubs plus a shared presence bitmask

Rudis keeps subscription state **fully local** to the shard that accepted the subscribing
connection — each shard owns its own `PubSubHub` (channels, patterns, and per-client delivery
queues), a plain `Rc<RefCell<..>>` with no cross-thread synchronization primitive at all. To
avoid unconditional broadcast, a small piece of *summary* state — which shards currently have
at least one subscriber possibly relevant to a channel — is shared across shards via a
`ShardedPresenceTable`: a fixed array of atomic bitmasks, one bit per shard, updated whenever a
shard's local subscriber count for a channel's hash stripe transitions to or from zero. A
publisher consults this bitmask before deciding which other shards are even worth contacting,
turning most publishes on channels with narrow interest into local-only operations plus a
handful of targeted cross-shard messages, instead of an unconditional broadcast to every shard.
This is a deliberate precision/cost trade-off, not an exact index: because the bitmask is
striped by a hash of the channel name (16 stripes) rather than keyed by the exact channel, two
different channel names can collide into the same stripe, causing an occasional unnecessary
cross-shard hop (a false positive) — but the presence table can never cause a *missed* delivery,
because the receiving shard's own `PubSubHub` still does the exact, authoritative
channel/pattern match before actually delivering anything.

### 2.3 Why sharded Pub/Sub (`SPUBLISH`) exists as a separate channel class

Standard `PUBLISH` must, in the worst case, reach any shard, because any shard could hold a
subscriber for any channel. Redis Cluster's sharded Pub/Sub trades that flexibility for
predictable routing: a shard-channel's name is hashed with the same CRC16 slot function used
for key routing (Component 04), so both `SSUBSCRIBE` and `SPUBLISH` resolve to exactly one
target shard, deterministically, without consulting any presence state. This turns "which
shards might care" from a runtime question into a compile-time-obvious one for callers who are
willing to accept the trade-off Redis Cluster made for sharded Pub/Sub: messages on a shard
channel are only guaranteed to reach subscribers connected to the shard that owns that channel's
slot, not to every replica, but the cost is now O(1) regardless of cluster or shard count,
rather than O(shards with possible interest).

## 3. Delivery Model & Guarantees

- **At-most-once, best-effort.** `PUBLISH`/`SPUBLISH` do not persist messages; a subscriber must
  already be registered when the message is published to receive it. There is no queue replay
  on reconnect.
- **Exact-match channels vs. glob-pattern channels are independent delivery paths.** A message
  published to channel `C` is delivered to (a) every client that issued `SUBSCRIBE C`, and (b)
  every client whose `PSUBSCRIBE <pattern>` glob-matches `C`, evaluated as two separate lookups
  per publish (see the internal doc for the matching algorithm and its cost model).
- **RESP2 vs. RESP3 framing is negotiated per subscriber, not per channel.** A client that has
  upgraded to RESP3 (`HELLO 3`) receives out-of-band push-type frames (`>3`/`>4`); a RESP2
  client receives ordinary array replies (`*3`/`*4`) for the same message — the hub tracks each
  subscriber's protocol version and builds the appropriate frame once per publish, reusing it
  across every subscriber that shares that protocol version.
- **Slow-consumer protection, not unbounded buffering.** Each subscriber's delivery path is a
  bounded queue; a subscriber that cannot keep up is subject to the server's general client
  output-buffer-limit enforcement for the Pub/Sub client class (Component 15/resilience), rather
  than being allowed to grow its outstanding message backlog without limit. A message that
  cannot be enqueued because the subscriber's queue is full is simply not delivered to that
  subscriber and is not counted in `PUBLISH`'s returned receiver count.
- **No cross-shard publish ordering guarantee.** Two `PUBLISH` calls to the same channel issued
  against different shards in close succession have no defined relative ordering as observed by
  a subscriber; ordering is only guaranteed among messages that traverse the same shard's
  delivery queue for the same subscriber.
- **Sharded Pub/Sub trades scope for routing cost.** A shard channel's subscribers are only ever
  the clients connected to (or forwarded through) the single shard owning that channel's CRC16
  slot; there is intentionally no fan-out for `SPUBLISH`.

## 4. Key Invariants

1. **Subscription state never crosses a shard boundary implicitly.** A `PubSubHub` is owned by
   exactly one shard's `Router`; the only way another shard learns about it is the explicit
   `ShardMessage::Publish`/`Spublish`/`Ssubscribe`/`Sunsubscribe` messages sent over the
   existing inter-shard channel infrastructure (Component 04).
2. **The presence bitmask is a hint, never a source of truth.** It may over-approximate (causing
   a wasted but harmless remote lookup); it must never under-approximate in a way that drops a
   real delivery, because each shard still performs the exact match locally.
3. **A client's registration is torn down completely on disconnect.** Every subscription type
   (channel, pattern, shard channel) held by a client is released when its connection closes,
   including updating the shared presence bitmask if that was the channel's last local
   subscriber — no dangling per-client or per-channel state survives a dropped connection.

## 5. Implementation Reference

For concrete struct layouts, the glob-matching algorithm, the RESP frame formats, and the
exact cross-shard fan-out and slot-routing code paths, see
[`docs/internal/19_pubsub.md`](../internal/19_pubsub.md).
