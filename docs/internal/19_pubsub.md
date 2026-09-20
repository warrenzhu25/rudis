# Component 19: Pub/Sub Messaging Hub — Implementation Reference

> **Source Files**: `src/pubsub.rs` (hub and presence table); cross-shard routing and sharded
> Pub/Sub slot resolution live in `src/router.rs`.
> **High-Level Design Spec**: [`docs/design/19_pubsub.md`](../design/19_pubsub.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Module Responsibilities

| File | Responsibility |
| :--- | :--- |
| `src/pubsub.rs` | `PubSubHub` (per-shard subscriber registry), `ShardedPresenceTable` (cross-shard presence bitmask), RESP frame builders, glob matcher. |
| `src/router.rs` | `Router::publish`/`pubsub_channels`/`pubsub_numsub`/`pubsub_numpat` (cross-shard fan-out for standard/pattern Pub/Sub); `Router::spublish`/`ssubscribe`/`sunsubscribe` (CRC16 slot routing for sharded Pub/Sub). |
| `src/connection.rs` | `run_pubsub_loop`/`handle_pubsub_cmd` (the dedicated per-connection task a client enters after `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`). |
| `src/shard.rs` | `ShardMessage::Publish`/`Spublish`/`Ssubscribe`/`Sunsubscribe` — the inter-shard message variants that carry Pub/Sub operations across the `flume` channel mesh. |

---

## 2. Data Structures (verbatim from `src/pubsub.rs`)

### 2.1 `PubSubHub` — one instance per shard, owned by `Router.pubsub: Rc<RefCell<PubSubHub>>`

```rust
#[derive(Default)]
pub struct PubSubHub {
    pub channels: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,          // channel -> subscriber client IDs
    pub patterns: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,          // pattern -> subscriber client IDs
    pub shard_channels: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,    // SPUBLISH channel -> subscriber client IDs
    pub clients: hashbrown::HashMap<u64, flume::Sender<Bytes>>,                // client ID -> delivery queue
    pub client_channels: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,   // reverse index: client -> its channels
    pub client_patterns: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,   // reverse index: client -> its patterns
    pub client_shard_channels: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>, // reverse index: client -> its shard channels
    pub client_resp3: hashbrown::HashSet<u64>,                                 // clients that negotiated RESP3
    pub stripe_counts: [usize; PUBSUB_STRIPES],                                // local subscriber count per presence stripe
    pub total_patterns: usize,                                                 // local pattern-subscription count
}
```

All maps use `hashbrown::HashMap`/`HashSet` (the crate `hashbrown`, not `std`), consistent with
the rest of the codebase's storage layer. The reverse indices (`client_channels`,
`client_patterns`, `client_shard_channels`) exist so that `unsubscribe_all`/`punsubscribe_all`/
`sunsubscribe_all`/connection-drop cleanup are O(subscriptions held by this client), not O(every
channel/pattern registered anywhere in the hub). `client_resp3` is a `HashSet<u64>` of client
IDs, checked per subscriber at publish time to decide RESP2 vs. RESP3 framing; it is populated
and cleared by every `subscribe*`/`psubscribe*`/`ssubscribe` call based on the `is_resp3`
argument the caller passes in (sourced from the connection's negotiated protocol version —
Component 02), and removed once a client has zero total subscriptions.

### 2.2 `ShardedPresenceTable` — cross-shard hint state, one instance per listening port

```rust
pub const PUBSUB_STRIPES: usize = 16;

pub struct ShardedPresenceTable {
    pub channel_stripes: [AtomicU64; PUBSUB_STRIPES],
    pub pattern_presence: AtomicU64,
}
```

Each element of `channel_stripes` is a bitmask (bit `i` = "shard `i` has at least one local
subscriber for some channel hashing into this stripe"). `stripe_for(channel)` computes the
stripe as `hash_key(channel) as usize % PUBSUB_STRIPES` using the same `hash_key` function the
storage engine uses for its own hash table (`src/table.rs`), so a table with only 16 stripes for
potentially millions of distinct channel names necessarily has hash collisions across unrelated
channels — this is intentional (§2.2 of the design doc): a collision only produces a spurious
remote lookup, never a missed delivery, because the exact match happens locally on the target
shard. `pattern_presence` is a *single* bitmask, not striped — because a pattern can match any
channel name, there is no way to narrow "which shards might have a matching pattern" any further
than "which shards have any pattern subscription at all."

`get_presence_table(port: u16) -> Arc<ShardedPresenceTable>` looks the table up (or lazily
creates it) in a process-wide `static PRESENCE_TABLES: LazyLock<RwLock<hashbrown::HashMap<u16,
Arc<ShardedPresenceTable>>>>`, keyed by the server's listening port so that multiple Rudis
instances or test harnesses in the same process do not share presence state. Each `Router`
holds its own clone of the `Arc<ShardedPresenceTable>` for its port (`Router.presence_table`),
obtained once at construction (`router.rs`).

`add_subscriber`/`remove_subscriber`/`add_pattern_subscriber`/`remove_pattern_subscriber` all
use `fetch_or`/`fetch_and` with `Ordering::Relaxed` — the bitmask only needs to be an
eventually-visible hint, not a linearizable source of truth, consistent with §2 of the design
doc. `interested_shards(channel) -> u64` returns `channel_stripes[stripe_for(channel)] |
pattern_presence`, i.e. the OR of "any shard with a subscriber whose channel hashes to this
stripe" and "any shard with any active pattern subscription."

### 2.3 RESP frame builders

```rust
pub fn build_pubsub_frame(kind: &[u8], channel: &[u8], message: &[u8], is_resp3: bool) -> Bytes
pub fn build_pubsub_pframe(pattern: &[u8], channel: &[u8], message: &[u8], is_resp3: bool) -> Bytes
```

Both build a `bytes::Bytes` frame directly (no intermediate `Vec<Value>`/generic RESP encoder):
`build_pubsub_frame` emits a 3-element reply — `*3\r\n$<len>\r\n<kind>\r\n$<len>\r\n<channel>\r\n
$<len>\r\n<message>\r\n` for RESP2, or the same three elements under a `>3\r\n` push-type prefix
for RESP3 — used for both `message` (standard/pattern-matched-as-exact... see below) and
`smessage` (sharded) delivery, with `kind` supplying the literal reply-type bulk string.
`build_pubsub_pframe` is the 4-element `pmessage` variant (`pattern`, `channel`, `message`,
plus the fixed `pmessage` kind), prefixed `*4`/`>4`. Each frame is built exactly once per publish
per protocol version actually in use among that publish's subscribers (see §4.1), then
`Bytes::clone()`d (a reference-count bump, not a data copy) to every subscriber sharing that
protocol version.

---

## 3. Execution Algorithms

### 3.1 `glob_match` — backtracking `*`/`?` wildcard matcher

```rust
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0; let mut t = 0;
    let mut star_p = None; let mut match_t = 0;
    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) { p += 1; t += 1; }
        else if p < pattern.len() && pattern[p] == b'*' { star_p = Some(p); p += 1; match_t = t; }
        else if let Some(sp) = star_p { p = sp + 1; match_t += 1; t = match_t; }
        else { return false; }
    }
    while p < pattern.len() && pattern[p] == b'*' { p += 1; }
    p == pattern.len()
}
```

Standard greedy-with-backtracking wildcard matching (linear-time in practice, not a naive
exponential recursive matcher): on a literal mismatch, if a `*` was seen earlier, the matcher
retries by letting that `*` consume one additional character of `text` and re-scanning the
pattern from just past it. `?` matches exactly one character. There is **no character-class
support** (`[abc]`, `[a-z]`, `[^abc]`) — only `*` and `?` are recognized, which is a narrower
wildcard grammar than real Redis's `PSUBSCRIBE`/`KEYS` glob syntax (which supports bracket
classes). A pattern relying on `[...]` will not behave as it would against real Redis; the
bracket characters are matched only literally. Used both for `PSUBSCRIBE` pattern evaluation
against published channels (§3.2) and for filtering `PUBSUB CHANNELS <pattern>`/`PUBSUB
SHARDCHANNELS <pattern>` results.

### 3.2 `PubSubHub::publish` — local delivery, two independent passes

```rust
pub fn publish(&self, channel: &[u8], message: &[u8]) -> usize {
    let mut count = 0;
    // 1. Exact-channel subscribers: one frame per protocol version, built lazily, cloned per recipient
    if let Some(subscribers) = self.channels.get(channel) {
        for client_id in subscribers {
            let frame = /* RESP2 or RESP3 build_pubsub_frame(b"message", ...), memoized per call */;
            if let Some(tx) = self.clients.get(client_id) && tx.try_send(frame.clone()).is_ok() {
                count += 1;
            }
        }
    }
    // 2. Pattern subscribers: glob-match every registered pattern against this channel
    for (pattern, subscribers) in &self.patterns {
        if glob_match(pattern, channel) {
            for client_id in subscribers {
                let frame = /* RESP2 or RESP3 build_pubsub_pframe(pattern, channel, ...), memoized per call */;
                if let Some(tx) = self.clients.get(client_id) && tx.try_send(frame.clone()).is_ok() {
                    count += 1;
                }
            }
        }
    }
    count
}
```

Exact-channel delivery is **O(subscribers to that specific channel)**. Pattern delivery is
**O(total registered patterns in this shard's hub)**, because every registered pattern must be
glob-matched against the published channel — there is no prefix index or trie to skip patterns
that cannot possibly match; a deployment with many distinct active `PSUBSCRIBE` patterns pays a
glob-match per pattern on every single local `publish` call regardless of how many actually
match. Delivery uses `flume::Sender::try_send` (non-blocking): a full queue causes the send to
fail silently for that one subscriber, and that subscriber is not counted toward the returned
total — this is the mechanism by which a slow consumer is skipped rather than allowed to block
the publisher or grow its backlog unboundedly (see the resilience/client-output-buffer-limit
subsystem for how a persistently full queue eventually leads to that connection being closed).
`count` reflects only messages that were actually enqueued, per client per publish.

### 3.3 Cross-shard fan-out: `Router::publish` (`src/router.rs`)

```rust
pub async fn publish(&self, channel: Bytes, message: Bytes) -> usize {
    let mut total = self.pubsub.borrow().publish(&channel, &message);   // 1. deliver locally first
    let mask = self.presence_table.interested_shards(&channel);         // 2. consult the hint
    let mut pending = Vec::new();
    for (sid, sender) in self.senders.iter().enumerate() {
        if sid != self.shard_id && (mask & (1u64 << sid)) != 0 {
            let (tx, rx) = self.acquire_pubsub_responder();
            let msg = ShardMessage::Publish { channel: channel.clone(), message: message.clone(), responder: tx.clone() };
            if sender.send(msg).is_ok() { pending.push((tx, rx)); } else { self.release_pubsub_responder(tx, rx); }
        }
    }
    for (tx, rx) in pending {                                            // 3. await every dispatched shard
        if let Ok(count) = rx.recv_async().await { total += count; }
        self.release_pubsub_responder(tx, rx);
    }
    total
}
```

The sequence is: (1) deliver to this shard's own local subscribers synchronously; (2) load the
presence bitmask for the channel; (3) send one `ShardMessage::Publish` to every *other* shard
whose bit is set — all sends are issued before any `.await`, so the fan-out is dispatched in
parallel rather than sequentially, matching the "dispatch all, then await all" pattern used
elsewhere in the router (Component 04); (4) await each dispatched shard's reply and sum its
reported delivery count into the running total. `acquire_pubsub_responder`/
`release_pubsub_responder` pool the one-shot `flume::bounded(1)` response channels used for this
round trip rather than allocating a fresh pair per target shard per publish. A shard whose bit
was set purely due to a stripe collision (§2.2) still receives a message, runs its own exact
`PubSubHub::publish`, finds no real matches, and correctly replies with `0` — the cost of a
false positive is one wasted round trip, not an incorrect count.

`Router::pubsub_channels`/`pubsub_numsub`/`pubsub_numpat` follow the same shape: query the local
hub first, then fan out to every other shard unconditionally (there is no presence-bitmask
shortcut for these introspection commands, since they must enumerate global state regardless of
which shards currently have subscribers) and merge results.

### 3.4 Sharded Pub/Sub: CRC16 slot routing, not presence-based fan-out

```rust
pub async fn spublish(&self, channel: Bytes, message: Bytes) -> usize {
    let slot = key_slot(&channel);                       // CRC16(XMODEM) % 16384, same as key routing
    let target = slot_to_shard(slot, self.num_shards);
    if target == self.shard_id {
        self.pubsub.borrow().spublish(&channel, &message)
    } else {
        // send one ShardMessage::Spublish to `target` and await its reply — no other shard is contacted
    }
}
```

`key_slot`/`slot_to_shard` are the exact same functions `src/router.rs` uses for ordinary key
routing (Component 04): `key_slot` computes `crc16::State::<crc16::XMODEM>::calculate(hash_tag)
% 16384` (respecting `{hash tag}` syntax, identical to Redis Cluster key routing), and
`slot_to_shard` maps the 16384-slot space evenly across `num_shards` via `(slot * num_shards) /
16384`. `ssubscribe`/`sunsubscribe` resolve the same way: if the channel's slot belongs to this
shard, the local `PubSubHub`'s `shard_channels`/`client_shard_channels` maps are updated
directly; otherwise a `ShardMessage::Ssubscribe`/`Sunsubscribe` is sent to the exact owning
shard, and (for `ssubscribe`) the local hub's `client_shard_channels` reverse index is also
updated so that `SUNSUBSCRIBE` issued with no arguments (meaning "all") and connection-drop
cleanup on *this* shard can find and release the subscription without needing to ask the
remote shard which channels a client is on. Unlike `Router::publish`, there is no presence
bitmask consultation at all for sharded Pub/Sub — routing is a pure function of the channel name,
by design (see design doc §2.3).

`PubSubHub::spublish` (the local delivery function) mirrors §3.2's exact-channel pass only —
shard channels are never glob-matched against patterns; `PSUBSCRIBE` subscribers do not receive
`SPUBLISH` traffic, confirmed by `test_sharded_pubsub_hub_ssubscribe_spublish_sunsubscribe`.

### 3.5 Cleanup on disconnect: `remove_client_with_presence`

```rust
pub fn remove_client_with_presence(&mut self, client_id: u64, presence_info: Option<(usize, &ShardedPresenceTable)>) {
    self.unsubscribe_all_with_presence(client_id, presence_info);
    self.punsubscribe_all_with_presence(client_id, presence_info);
    self.sunsubscribe_all(client_id);
    self.client_resp3.remove(&client_id);
    self.clients.remove(&client_id);
}
```

Called once a Pub/Sub-mode connection's loop exits (disconnect, `QUIT`, protocol error, or a
client output-buffer-limit breach). Tears down every channel, pattern, and shard-channel
subscription for the client, clearing the shared presence bitmask's bit for this shard whenever
a channel's or the pattern class's local subscriber count drops to zero as a result (via the
`_with_presence` variants — see §3.2's callers in `connection.rs`), and removes the client's
delivery-queue sender and RESP3 flag. No dangling per-client or per-channel entries survive a
disconnect.

---

## 4. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): `run_pubsub_loop` spawns a dedicated writer task per
  Pub/Sub connection (a `flume::bounded::<Bytes>(4096)` queue) that drains delivered frames onto
  the socket independently of the loop reading new `SUBSCRIBE`/`UNSUBSCRIBE`/`PING`/`QUIT`
  commands from the client; `handle_pubsub_cmd` is the sole caller of
  `subscribe_with_presence`/`unsubscribe_with_presence`/`psubscribe_with_presence`/
  `punsubscribe_with_presence` (standard/pattern) and `Router::ssubscribe`/`sunsubscribe`
  (sharded). The writer task also enforces the Pub/Sub client class's output-buffer limits
  (Component 15) against `queued_bytes`, closing the connection if a subscriber falls
  persistently behind.
- **`src/router.rs`** (Component 04): owns all cross-shard Pub/Sub entry points — `publish`,
  `pubsub_channels`, `pubsub_numsub`, `pubsub_numpat` (presence-bitmask-filtered fan-out), and
  `spublish`/`ssubscribe`/`sunsubscribe` (CRC16 slot routing, §3.4).
- **`src/shard.rs`** (Component 04): defines the `ShardMessage::Publish`/`Spublish`/
  `Ssubscribe`/`Sunsubscribe` variants carried over the inter-shard `flume` channel mesh.
- **`src/table.rs`**: no relationship. A channel or pattern name is never a Redis key; Pub/Sub
  state does not participate in expiration, eviction, or the storage engine at all.

---

## Contributor Gotchas & Debugging Guide

* **Gotcha 1**: `ShardedPresenceTable` uses 16 `AtomicU64` stripes for exact-channel presence
  plus one unstriped bitmask for pattern presence; a stripe hit does not mean an exact-channel
  match — only the target shard's own hub lookup is authoritative.
* **Gotcha 2**: `SPUBLISH`/`SSUBSCRIBE` route by CRC16 slot exactly like key commands, and never
  consult the presence table — a shard-channel subscriber only ever receives messages routed
  through the one shard that owns that channel's slot.
* **Gotcha 3**: RESP2/RESP3 framing is chosen per subscriber at delivery time based on
  `PubSubHub.client_resp3`, not fixed at compile time; both frame variants are built at most
  once per publish and shared via `Bytes` cloning.
* **Gotcha 4**: Delivery uses non-blocking `try_send`; a full subscriber queue silently drops
  that one message for that one subscriber rather than blocking the publisher.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib pubsub -- --test-threads=1
```
