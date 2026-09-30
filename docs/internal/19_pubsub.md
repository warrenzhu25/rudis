# Component 19: Pub/Sub Messaging Hub — Implementation Reference

> **Source Files**: `src/pubsub.rs` (987 lines — hub, presence table, glob matcher, RESP frame
> builders); cross-shard routing and CRC16 sharded-channel slot resolution live in `src/router.rs`;
> the per-connection state machine lives in `src/connection.rs`; the inter-shard wire protocol is
> defined in `src/shard.rs`.
> **High-Level Design Spec**: [`docs/design/19_pubsub.md`](../design/19_pubsub.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

This revision re-verifies every claim in the previous pass directly against current
`src/pubsub.rs` (987 lines, up from ~849). Three things changed materially since the last pass:

| Commit | What changed |
| :--- | :--- |
| `3ffffce` | `glob_match` was rewritten from a simplified two-pointer `*`/`?` matcher into a full port of Redis's recursive `stringmatchlen`, adding `[...]` bracket character classes, `[^...]` negation, range (`a-z`), `\`-escapes, and a nesting-depth abuse guard. **The previous doc's claim that bracket classes are unsupported is now false.** |
| `3adc495` | `ShardedPresenceTable`'s bitmask was widened from a single `AtomicU64` per stripe (64-shard ceiling) to `[AtomicU64; 4]` per stripe (256-shard ceiling), and `Router::publish`'s fan-out loop was changed from "fetch one mask, test bits" to "call `is_shard_interested(shard_id, channel)` once per candidate shard." |
| `a196abd`, `ab5555f` | Introduced `SPUBLISH`/`SSUBSCRIBE`/`SUNSUBSCRIBE` (CRC16 slot-routed sharded Pub/Sub) and the presence bitmask itself — both already covered by the prior pass and reconfirmed unchanged in mechanics below. |

---

## 1. Module Responsibilities

| File | Responsibility |
| :--- | :--- |
| `src/pubsub.rs` | `PubSubHub` (per-shard subscriber registry), `ShardedPresenceTable` (cross-shard presence bitmask), RESP frame builders, the recursive glob matcher. |
| `src/router.rs` | `Router::publish`/`pubsub_channels`/`pubsub_numsub`/`pubsub_numpat` (cross-shard fan-out for standard/pattern Pub/Sub, presence-filtered); `Router::spublish`/`ssubscribe`/`sunsubscribe`/`pubsub_shardnumsub` (CRC16 slot routing for sharded Pub/Sub); `key_slot`/`slot_to_shard` (shared with ordinary key routing, Component 04). |
| `src/connection.rs` | `run_pubsub_loop`/`handle_pubsub_cmd` — the dedicated per-connection task a client is handed off to, permanently, the first time it issues `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`; `ClientCleanup`/`TlsClientCleanup` Drop guards that call `remove_client_with_presence` on disconnect. |
| `src/shard.rs` | The 10 `ShardMessage` variants that carry Pub/Sub operations across the per-shard `flume` mailbox mesh: `Publish`, `Spublish`, `Ssubscribe`, `Sunsubscribe`, `PubsubChannels`, `PubsubShardchannels`, `PubsubNumsub`, `PubsubShardnumsub`, `PubsubNumpat`, `RemoveClientPubSub`. |

Pub/Sub state (`PubSubHub`) is **not** shared across shards — each shard's `run_shard_worker`
creates its own `Rc<RefCell<PubSubHub>>` (`src/server.rs:227`) and hands it into that shard's
`Router`. There is no global registry of subscriptions; every cross-shard operation below exists
specifically to bridge that shared-nothing boundary.

---

## 2. Data Structures (verbatim from `src/pubsub.rs`)

### 2.1 `PubSubHub` — one instance per shard, owned by `Router.pubsub: Rc<RefCell<PubSubHub>>`

```rust
#[derive(Default)]
pub struct PubSubHub {
    pub channels: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,              // channel -> subscriber client IDs
    pub patterns: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,              // pattern -> subscriber client IDs
    pub shard_channels: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,        // SPUBLISH channel -> subscriber client IDs
    pub clients: hashbrown::HashMap<u64, flume::Sender<Bytes>>,                    // client ID -> delivery queue
    pub client_channels: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,       // reverse index: client -> its channels
    pub client_patterns: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,       // reverse index: client -> its patterns
    pub client_shard_channels: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>, // reverse index: client -> its shard channels
    pub client_resp3: hashbrown::HashSet<u64>,                                     // clients that negotiated RESP3
    pub stripe_counts: [usize; PUBSUB_STRIPES],                                    // local subscriber count per presence stripe (ref-count)
    pub total_patterns: usize,                                                     // local pattern-subscription count (ref-count)
}
```

Unchanged since the prior pass — `src/pubsub.rs:296-307`. All maps use `hashbrown`, not `std`.
The reverse indices exist so `unsubscribe_all`/`punsubscribe_all`/`sunsubscribe_all`/
connection-drop cleanup are `O(subscriptions held by this client)`, not `O(every channel/pattern
registered anywhere in the hub)`. `stripe_counts`/`total_patterns` are **local, per-shard
ref-counts**, not global state — they exist purely so that `subscribe_with_presence`/
`unsubscribe_with_presence`/etc. only touch the (cross-shard, atomic) `ShardedPresenceTable` on a
`0 -> 1` or `1 -> 0` transition of the local count, not on every individual client's
subscribe/unsubscribe (§3.5).

### 2.2 `ShardedPresenceTable` — cross-shard hint state, one instance per listening port

```rust
pub const PUBSUB_STRIPES: usize = 16;
pub const PUBSUB_WORDS_PER_STRIPE: usize = 4; // Scales cleanly up to 256 shards

pub struct ShardedPresenceTable {
    pub channel_stripes: [[AtomicU64; PUBSUB_WORDS_PER_STRIPE]; PUBSUB_STRIPES],
    pub pattern_presence: [AtomicU64; PUBSUB_WORDS_PER_STRIPE],
}
```

**This layout changed since the prior pass** (`3adc495`): each stripe used to be a single
`AtomicU64` (64-shard ceiling); it is now `[AtomicU64; 4]` — a 256-bit bitmask per stripe, one
bit per shard ID, addressed as `word = shard_id / 64`, `bit = shard_id % 64`. Concrete size:
`channel_stripes` is `16 * 4 * 8 = 512` bytes of atomics; `pattern_presence` adds another `4 * 8 =
32` bytes; the whole struct is `544` bytes, heap-allocated once per port inside an `Arc`.

`stripe_for(channel: &[u8]) -> usize` computes `(crate::table::hash_key(channel) as usize) %
PUBSUB_STRIPES` — `hash_key` is `fxhash::hash64` (`src/table.rs:1484-1486`, the same function the
storage engine's own hash table uses). 16 stripes for a potentially unbounded number of distinct
channel names necessarily means hash collisions across unrelated channels; this is intentional —
a collision only produces a spurious remote lookup, never a missed delivery, because the exact
match always happens locally on the target shard (§3.3).

Per-shard-ID operations (`src/pubsub.rs:220-269`):

```rust
pub fn add_subscriber(&self, shard_id: usize, channel: &[u8]) {
    let word = shard_id / 64; let bit = shard_id % 64;
    if word < PUBSUB_WORDS_PER_STRIPE {
        let stripe = Self::stripe_for(channel);
        self.channel_stripes[stripe][word].fetch_or(1u64 << bit, Ordering::Relaxed);
    }
}
// remove_subscriber: mirror, fetch_and(!(1 << bit))
// add_pattern_subscriber / remove_pattern_subscriber: same word/bit split, operate on pattern_presence[word]

pub fn is_shard_interested(&self, shard_id: usize, channel: &[u8]) -> bool {
    let word = shard_id / 64; let bit = shard_id % 64;
    if word < PUBSUB_WORDS_PER_STRIPE {
        let stripe = Self::stripe_for(channel);
        let mask = self.channel_stripes[stripe][word].load(Ordering::Relaxed)
            | self.pattern_presence[word].load(Ordering::Relaxed);
        (mask & (1u64 << bit)) != 0
    } else {
        true // fail open: shard IDs >= 256 are always treated as "interested"
    }
}
```

All operations use `Ordering::Relaxed` — the bitmask is only an eventually-visible optimization
hint, never a linearizable source of truth (the real check is always the target shard's own hub
lookup). **Verified limitation**: for `shard_id >= 256` (`word >= PUBSUB_WORDS_PER_STRIPE`),
`add_subscriber`/`remove_subscriber`/`add_pattern_subscriber`/`remove_pattern_subscriber` are
silent no-ops (the bit is simply never recorded), while `is_shard_interested` unconditionally
returns `true` for the same range. This is a safe fail-open (no delivery is ever skipped because
of it) but it means **presence filtering provides zero pruning benefit for shard IDs 256 and
above** — every publish always fans out to those shards regardless of actual interest. With the
current 16-stripe/4-word layout this only matters for deployments with more than 256
shards/cores, far beyond realistic single-host thread-per-core counts today.

`interested_shards(channel: &[u8]) -> u64` (`src/pubsub.rs:271-276`) returns only
`channel_stripes[stripe][0] | pattern_presence[0]` — **word 0 only**, i.e. it can only report
presence for shard IDs 0-63. Grepping the whole tree shows this method is called **exclusively
from the unit tests** (`test_sharded_presence_table_bitmask_tracking`,
`src/pubsub.rs:869-925`); no production code path uses it. `Router::publish` (§3.3) instead calls
`is_shard_interested(shard_id, channel)` once per candidate shard, which is the only path that
correctly covers the full 256-shard range.

`get_presence_table(port: u16) -> Arc<ShardedPresenceTable>` (`src/pubsub.rs:279-293`) looks the
table up, or lazily creates it, in a process-wide `static PRESENCE_TABLES:
LazyLock<RwLock<hashbrown::HashMap<u16, Arc<ShardedPresenceTable>>>>`, keyed by listening port so
multiple Rudis instances/test harnesses in one process don't share presence state. Each `Router`
clones the `Arc` once at construction (`router.rs:226`, `Router.presence_table`).

### 2.3 RESP frame builders (unchanged from prior pass)

```rust
pub fn build_pubsub_frame(kind: &[u8], channel: &[u8], message: &[u8], is_resp3: bool) -> Bytes
pub fn build_pubsub_pframe(pattern: &[u8], channel: &[u8], message: &[u8], is_resp3: bool) -> Bytes
```

Both hand-build a `bytes::Bytes` frame directly (no intermediate `Vec<Value>`/generic RESP
encoder). `build_pubsub_frame` emits `*3\r\n$<len>\r\n<kind>\r\n$<len>\r\n<channel>\r\n
$<len>\r\n<message>\r\n` for RESP2, or the same three elements under a `>3\r\n` push-type prefix
for RESP3 — reused for both `message` (exact-channel and pattern... no, see below) and
`smessage` (sharded) delivery, with `kind` supplying the literal reply-type bulk string
(`"message"` or `"smessage"`). `build_pubsub_pframe` is the 4-element `pmessage` variant
(`pattern`, `channel`, `message`), prefixed `*4`/`>4`. Each frame is built at most once per
matching subscriber-set per protocol version actually in use among that set's subscribers
(§3.2), then `Bytes::clone()`d — a reference-count bump on the underlying buffer, not a data
copy — to every subscriber sharing that protocol version.

---

## 3. Execution Algorithms

### 3.1 `glob_match` — recursive backtracking matcher, ported from Redis `stringmatchlen` (rewritten in `3ffffce`)

```rust
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut skip_longer_matches = false;
    stringmatchlen_impl(pattern, text, false, &mut skip_longer_matches, 0)
}

fn stringmatchlen_impl(mut pattern: &[u8], mut text: &[u8], nocase: bool,
                        skip_longer_matches: &mut bool, nesting: usize) -> bool {
    if nesting > 1000 { return false; }              // abuse guard, not a general length cap
    while !pattern.is_empty() && !text.is_empty() {
        match pattern[0] {
            b'*' => { /* collapse consecutive '*'; if pattern is now just "*", match;
                         otherwise try consuming 0..N chars of text, recursing on each,
                         nesting + 1; `skip_longer_matches` short-circuits further retries
                         once a deeper recursion has already proven no match is possible */ }
            b'?' => { text = &text[1..]; }
            b'[' => { /* character class: optional leading '^' negation, '\' escapes inside
                         the class, 'a-z' range matching (auto-swapped if reversed), else
                         literal membership; consumes exactly one char of `text` */ }
            b'\\' => { /* escape: match the next pattern byte literally against text[0] */ }
            _ => { /* literal byte compare */ }
        }
        pattern = &pattern[1..];
        if text.is_empty() {
            while !pattern.is_empty() && pattern[0] == b'*' { pattern = &pattern[1..]; }
            break;
        }
    }
    pattern.is_empty() && text.is_empty()
}
```

**This is a materially different — and more capable — matcher than the prior doc pass
described.** The previous revision's matcher was a simplified two-pointer `*`/`?`-only
implementation with **no** `[...]` bracket support; `3ffffce` replaced it wholesale with a
recursive backtracking port of Redis's own `stringmatchlen`, which **does** support `[abc]`
membership, `[^abc]` negation, `[a-z]` ranges, and `\`-escaping — i.e. the full glob grammar real
Redis `PSUBSCRIBE`/`KEYS` use. The public `glob_match` entry point always passes `nocase = false`
(case-sensitive only — there is no case-insensitive variant reachable from Pub/Sub). The
`nesting > 1000` guard bounds the *recursion depth from `*` backtracking specifically* (each `*`
that requires a retry increments `nesting`), protecting against stack overflow on pathological
patterns with many wildcards — it does not cap overall pattern or text length. Used both for
`PSUBSCRIBE` pattern evaluation against published channels (§3.2) and for filtering `PUBSUB
CHANNELS <pattern>`/`PUBSUB SHARDCHANNELS <pattern>` results (`PubSubHub::channels`/
`shard_channels`, `src/pubsub.rs:735-761`).

### 3.2 `PubSubHub::publish` — local delivery, two independent passes (`src/pubsub.rs:570-624`)

```rust
pub fn publish(&self, channel: &[u8], message: &[u8]) -> usize {
    let mut count = 0;
    // 1. Exact-channel subscribers: one frame pair (RESP2/RESP3) built lazily, memoized for this pass
    if let Some(subscribers) = self.channels.get(channel) {
        let mut frame_resp2: Option<Bytes> = None;
        let mut frame_resp3: Option<Bytes> = None;
        for client_id in subscribers {
            let frame = /* RESP2 or RESP3 build_pubsub_frame(b"message", ...), get_or_insert_with */;
            if let Some(tx) = self.clients.get(client_id) && tx.try_send(frame.clone()).is_ok() {
                count += 1;
            }
        }
    }
    // 2. Pattern subscribers: glob-match every registered pattern against this channel
    for (pattern, subscribers) in &self.patterns {
        if glob_match(pattern, channel) {
            let mut frame_resp2: Option<Bytes> = None;   // fresh memo cell PER MATCHING PATTERN
            let mut frame_resp3: Option<Bytes> = None;    // (the frame embeds `pattern`, so it can't be shared across patterns)
            for client_id in subscribers {
                let frame = /* RESP2 or RESP3 build_pubsub_pframe(pattern, channel, ...), get_or_insert_with */;
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
**O(total registered patterns in this shard's hub)** — every registered pattern is glob-matched
against the published channel on every single `publish` call; there is no prefix index or trie
to skip patterns that cannot possibly match. The RESP2/RESP3 frame is memoized **once per
matching pattern**, not once globally across all patterns (each frame embeds the pattern bytes,
so patterns can't share a frame instance the way exact-channel subscribers do). Delivery uses
`flume::Sender::try_send` — **non-blocking**: a full queue makes the send fail silently for that
one subscriber, and that subscriber is not counted toward the returned total. This is the entire
backpressure mechanism at the hub level: a slow consumer is skipped for that one message rather
than blocking the publisher or growing the hub's own memory unboundedly. `count` reflects only
messages actually enqueued, per client per publish — **delivery is at-most-once**, confirmed by
`test_pubsub_slow_consumer_backpressure_and_zero_copy` (`src/pubsub.rs:827-866`), which shows a
2-slot channel silently dropping a 3rd message for one subscriber while a 10-slot channel
receives all 3.

### 3.3 Cross-shard fan-out: `Router::publish` (`src/router.rs:2276-2301`)

```rust
pub async fn publish(&self, channel: Bytes, message: Bytes) -> usize {
    let mut total = self.pubsub.borrow().publish(&channel, &message);  // 1. deliver locally first
    let mut pending = Vec::new();
    for (sid, sender) in self.senders.iter().enumerate() {
        if sid != self.shard_id && self.presence_table.is_shard_interested(sid, &channel) {  // 2. per-shard check
            let (tx, rx) = self.acquire_pubsub_responder();
            let msg = ShardMessage::Publish { channel: channel.clone(), message: message.clone(), responder: tx.clone() };
            if sender.send(msg).is_ok() { pending.push((tx, rx)); } else { self.release_pubsub_responder(tx, rx); }
        }
    }
    for (tx, rx) in pending {                                          // 3. await every dispatched shard
        if let Ok(count) = rx.recv_async().await { total += count; }
        self.release_pubsub_responder(tx, rx);
    }
    total
}
```

**Changed since the prior pass**: the old implementation fetched a single `u64` mask via
`interested_shards(&channel)` once, then tested a bit per shard. The current implementation
instead calls `is_shard_interested(sid, &channel)` — which internally recomputes `stripe_for`
(i.e. `fxhash::hash64(channel) % 16`) — **once per candidate remote shard**, i.e. `num_shards - 1`
times per `publish` call, rather than hashing the channel name once and reusing the result. This
is the necessary tradeoff for supporting the full 256-shard address space (§2.2), since
`interested_shards()`'s single-mask return value can't. All `ShardMessage::Publish` sends are
issued in the loop before any `.await` — the fan-out is dispatched to every interested shard in
parallel, not sequentially, matching the "dispatch all, then await all" pattern used elsewhere in
the router (Component 04). `acquire_pubsub_responder`/`release_pubsub_responder`
(`src/router.rs:1074-1086`) pool the one-shot `flume::bounded(1)` response channels used for this
round trip out of `Router.pubsub_responder_pool` rather than allocating a fresh pair per target
shard per publish. A shard whose bit was set purely due to a stripe collision (§2.2) still
receives a `ShardMessage::Publish`, runs its own exact `PubSubHub::publish`, finds no real
matches, and correctly replies `0` — the cost of a false positive is one wasted round trip, not
an incorrect count.

`Router::pubsub_channels`/`pubsub_numsub`/`pubsub_numpat` follow the same "local first, then fan
out" shape but **unconditionally** query every other shard (no presence-bitmask shortcut — they
must enumerate global state regardless of which shards currently have subscribers) and merge
results with a `hashbrown::HashSet`/`HashMap` before returning.

### 3.4 Sharded Pub/Sub: CRC16 slot routing, not presence-based fan-out (`src/router.rs:2390-2468`)

```rust
pub async fn spublish(&self, channel: Bytes, message: Bytes) -> usize {
    let slot = key_slot(&channel);                       // crc16::State::<crc16::XMODEM>::calculate(hash_tag) % 16384
    let target = slot_to_shard(slot, self.num_shards);    // (slot * num_shards) / 16384
    if target == self.shard_id {
        self.pubsub.borrow().spublish(&channel, &message)
    } else {
        // single ShardMessage::Spublish to `target`, await its reply — no other shard is ever contacted
    }
}
```

`key_slot`/`slot_to_shard` are the *exact same functions* `src/router.rs` uses for ordinary key
routing (Component 04, reconfirmed unchanged): `key_slot` respects `{hash tag}` syntax (extracted
by `extract_hash_tag`) and computes CRC16/XMODEM mod 16384, identical to Redis Cluster key
routing; `slot_to_shard` divides the 16384-slot space evenly across `num_shards` via `(slot *
num_shards) / 16384` — this is purely a local-node sharding function, *not* a Redis Cluster
slot-ownership table (no relationship to `src/cluster.rs`'s gossip-derived slot map). `ssubscribe`
resolves the same way: if the channel's slot belongs to this shard, `PubSubHub::ssubscribe`
inserts directly into the local `shard_channels`/`client_shard_channels` maps; otherwise a
`ShardMessage::Ssubscribe { client_id, channel, sender: <the client's flume::Sender<Bytes> clone>,
is_resp3, responder }` is sent to the exact owning shard — **the subscriber's actual delivery
channel handle is transmitted across the shard mesh**, so the remote shard can later deliver
frames directly via its own local `try_send`, with zero additional hop through the client's home
shard. After the remote registration completes, the client's *home* shard also records the
channel in its own `client_shard_channels` reverse index (but not in `shard_channels`/`clients`),
purely so `SUNSUBSCRIBE` with no arguments and connection-drop cleanup can enumerate "what shard
channels does this client hold" locally without an extra round trip to ask the remote shard.
`sunsubscribe` mirrors this. Unlike `Router::publish`, **there is no presence-bitmask
consultation anywhere in the `spublish`/`ssubscribe`/`sunsubscribe` path** — routing is a pure
function of the channel name; a shard-channel message either reaches the one shard that
deterministically owns its slot, or (if that shard is unreachable) is dropped, there is no
broadcast fallback.

`PubSubHub::spublish` (`src/pubsub.rs:692-717`, the local delivery function) mirrors §3.2's
exact-channel pass only — shard channels are never glob-matched against patterns; `PSUBSCRIBE`
subscribers never receive `SPUBLISH` traffic, confirmed by
`test_sharded_pubsub_hub_ssubscribe_spublish_sunsubscribe` (`src/pubsub.rs:928-986`), which
asserts a pattern-subscribed client receives nothing from an `SPUBLISH`.

`Router::pubsub_shardnumsub` (`src/router.rs:2516-2545`) is the one shard-scoped introspection
command that *does* route deterministically rather than broadcasting: each requested channel name
is hashed to its slot/owning shard individually, grouped into one `ShardMessage::PubsubShardnumsub`
batch per target shard, and only those shards are contacted — `PUBSUB SHARDCHANNELS` (no specific
channel names) still has to broadcast to all shards since it must enumerate an unbounded set.

### 3.5 Cleanup on disconnect: `remove_client_with_presence` (`src/pubsub.rs:719-733`)

```rust
pub fn remove_client_with_presence(&mut self, client_id: u64, presence_info: Option<(usize, &ShardedPresenceTable)>) {
    self.unsubscribe_all_with_presence(client_id, presence_info);
    self.punsubscribe_all_with_presence(client_id, presence_info);
    self.sunsubscribe_all(client_id);   // note: no presence_info parameter — shard channels never touch the presence table
    self.client_resp3.remove(&client_id);
    self.clients.remove(&client_id);
}
```

Called from two places (`src/connection.rs:1573-1604`'s `ClientCleanup::drop` for plaintext
connections, and the TLS equivalent at `src/connection.rs:1200-1216`, plus the writer-task path
inside `run_pubsub_loop` itself is *not* where cleanup happens — it's the outer connection-level
Drop guard that runs regardless of which branch inside `run_pubsub_loop` returned). On the
client's own (home) shard, cleanup passes `presence_info = Some((shard_id, &presence_table))` so
that a channel's or the pattern class's local subscriber count dropping to zero correctly clears
that shard's presence bit. The home-shard `Drop` guard also unconditionally broadcasts
`ShardMessage::RemoveClientPubSub { client_id }` to every *other* shard (guarded only by "is this
shard's hub non-empty at all," `src/connection.rs:1584` — a cheap, coarse check, not a check of
whether *this specific client* has any cross-shard state) so that any `shard_channels` entries
this client registered on a remote shard via `SSUBSCRIBE` (§3.4) get cleaned up too. Those remote
cleanups pass `presence_info = None` (`src/server.rs:1631-1635`) — correct, since `shard_channels`
cleanup (`sunsubscribe_all`) never touches the presence table in the first place, and a client's
*regular* channel/pattern subscriptions only ever exist on its home shard (`SUBSCRIBE`/
`PSUBSCRIBE` are never routed cross-shard, unlike `SSUBSCRIBE`).

---

## 4. Connection-Level State Machine (`src/connection.rs`)

A connection starts in the normal command-execution loop (Component 02). The **first** time it
parses a `SUBSCRIBE`, `PSUBSCRIBE`, or `SSUBSCRIBE` command (`src/connection.rs:1729-1735`), any
commands queued ahead of it in the same read are executed normally, then the TCP stream (split
into `reader`/`writer` halves) and the remaining parsed commands are handed to
`run_pubsub_loop` (`src/connection.rs:2178-2595`) — **permanently, for the rest of the
connection's life.** There is no path back to the normal `execute_command` dispatcher, even after
the client unsubscribes from everything; only a reader EOF/error, a protocol parse error, or an
explicit `QUIT` ends the loop. This differs from real Redis, where subscribe-mode restrictions are
dynamic (based on current subscription count, not sticky for the connection's lifetime).

`run_pubsub_loop` spawns a dedicated writer task (`monoio::spawn`, `src/connection.rs:2191-2226`)
that owns the socket's write half and drains a **`flume::bounded::<Bytes>(4096)`** channel
(`write_tx`/`write_rx`, `src/connection.rs:2188`) — this is the per-client delivery queue whose
`Sender<Bytes>` clone is what gets stored in `PubSubHub.clients` and (for `SSUBSCRIBE`) shipped
across the shard mesh. The writer task tracks `queued_bytes` and enforces the `ClientClass::Pubsub`
output-buffer limit (`src/connection.rs:73-74`, **default hard 32 MiB / soft 8 MiB with a 60s
grace period** — `PUBSUB_BUFFER_LIMIT: BufferLimit::new(33554432, 8388608, 60)`, matching real
Redis's default `client-output-buffer-limit pubsub 32mb 8mb 60`); exceeding the hard limit, or
staying above the soft limit for 60 continuous seconds, breaks the writer loop and ends the
connection. This is the second, independent backpressure layer on top of the 4096-slot channel
itself: the bounded channel drops individual messages silently when full (§3.2); the output-buffer
limit instead kills a subscriber connection outright once its *backlog of already-accepted-but-
unwritten* bytes grows too large (e.g. the socket write side is slow, not the channel).

`handle_pubsub_cmd` (`src/connection.rs:2230-2487`) is the sole command dispatcher while in this
mode. It explicitly handles `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`SSUBSCRIBE`/
`SUNSUBSCRIBE`/`PING`/`CLIENT REPLY`/`QUIT`; **every other command — including `PUBLISH` and
`SPUBLISH` — falls into a catch-all branch that replies `-ERR Can't execute '<CMD>' in subscribed
mode\r\n` and is rejected unconditionally**, regardless of whether the client negotiated RESP3.
Real Redis relaxes this restriction for RESP3 clients (which receive push-type frames
out-of-band and can therefore safely run arbitrary commands while subscribed); Rudis does not —
this is a verified behavioral gap, not a stripe-collision-style approximation.

---

## 5. Cross-Shard Wire Protocol (`src/shard.rs:450-493`)

The 10 `ShardMessage` variants Pub/Sub uses, all carried over the same per-shard-pair `flume`
mailbox used for every other cross-shard operation (Component 04):

| Variant | Payload | Purpose |
| :--- | :--- | :--- |
| `Publish` | `channel, message, responder: Sender<usize>` | Deliver to one remote shard's local subscribers; reply is that shard's match count. |
| `Spublish` | `channel, message, responder: Sender<usize>` | Deliver `SPUBLISH` to the one shard that owns the channel's slot. |
| `Ssubscribe` | `client_id, channel, sender: Sender<Bytes>, is_resp3, responder: Sender<()>` | Register a subscriber directly into a remote shard's hub — carries the actual delivery-channel handle. |
| `Sunsubscribe` | `client_id, channel, responder: Sender<()>` | Remove a shard-channel subscription on its owning (remote) shard. |
| `PubsubChannels` / `PubsubShardchannels` | `pattern: Option<Bytes>, responder: Sender<Vec<Bytes>>` | Enumerate channel names on a remote shard for `PUBSUB CHANNELS`/`SHARDCHANNELS`. |
| `PubsubNumsub` / `PubsubShardnumsub` | `channels: Vec<Bytes>, responder: Sender<Vec<(Bytes, usize)>>` | Per-channel subscriber counts; `PubsubShardnumsub` is routed per-channel to the owning shard only, `PubsubNumsub` is broadcast. |
| `PubsubNumpat` | `responder: Sender<usize>` | Remote pattern-subscription count for `PUBSUB NUMPAT`. |
| `RemoveClientPubSub` | `client_id` (no responder — fire-and-forget) | Disconnect cleanup broadcast to every other shard (§3.5). |

---

## 6. Delivery Guarantees (verified, not assumed)

- **At-most-once, never at-least-once.** A message that doesn't fit in a subscriber's 4096-slot
  `flume::bounded` channel is silently dropped for that subscriber (`try_send` failure, §3.2); the
  publisher receives no error and no indication which subscribers, if any, missed the message —
  only an aggregate count of successful enqueues.
- **No persistence, no replay.** There is no backlog, WAL, or stream-like buffer anywhere in
  `pubsub.rs`; a message not currently deliverable (channel full, or no subscriber registered at
  publish time) is gone.
- **No cross-shard ordering guarantee beyond per-shard-pair FIFO.** `Router::publish` dispatches
  to all interested shards in parallel and awaits them in the order they were sent, not the order
  replies arrive — the returned aggregate count is exact, but nothing here orders concurrent
  publishes to the same channel from different shards relative to each other.
- **Presence-table false positives are possible and harmless; false negatives are not possible**
  for shard IDs < 256 (they are also not possible, but for a different reason — fail-open — for
  shard IDs >= 256, §2.2).

---

## 7. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): owns the entire per-connection Pub/Sub state machine
  (§4) and both disconnect-cleanup Drop guards (§3.5).
- **`src/router.rs`** (Component 04): owns every cross-shard Pub/Sub entry point — presence-
  filtered fan-out for standard/pattern Pub/Sub (§3.3), CRC16 slot routing for sharded Pub/Sub
  (§3.4), and the responder-channel pool (`pubsub_responder_pool`).
- **`src/shard.rs`** (Component 04): defines the wire protocol (§5).
- **`src/table.rs`**: supplies `hash_key` (`fxhash::hash64`), reused unmodified by
  `ShardedPresenceTable::stripe_for`; otherwise no relationship — a channel/pattern name is never
  a Redis key and Pub/Sub state never participates in expiration, eviction, or the storage engine.
- **`src/cluster.rs`**: **no relationship** for slot *ownership* — `spublish`'s `slot_to_shard`
  is a local, even 16384/`num_shards` split, independent of `src/cluster.rs`'s gossip-derived
  cluster slot map. `key_slot` (the CRC16 computation itself) is shared code, not shared state.

---

## Contributor Gotchas & Debugging Guide

* **Gotcha 1**: `glob_match` now supports `[...]`/`[^...]`/ranges/escapes (since `3ffffce`) — do
  not assume the narrower `*`/`?`-only grammar described in older documentation or design notes.
* **Gotcha 2**: `ShardedPresenceTable` is now `[[AtomicU64; 4]; 16]` (256-shard capacity), not
  `[AtomicU64; 16]` (64-shard capacity). `interested_shards()` only ever sees word 0 (shards
  0-63) and is dead code outside unit tests — don't add a production call site expecting it to
  cover higher shard IDs; use `is_shard_interested(shard_id, channel)` per shard instead.
* **Gotcha 3**: `Router::publish` re-hashes the channel name once per remote shard candidate
  (via `is_shard_interested`'s internal `stripe_for` call), not once total — a latent
  micro-inefficiency introduced by the multi-word refactor, worth revisiting if channel hashing
  ever shows up in a publish-heavy profile.
* **Gotcha 4**: `SPUBLISH`/`SSUBSCRIBE` route by CRC16 slot exactly like key commands and never
  consult the presence table at all — a shard-channel subscriber only ever receives messages
  routed through the one shard that owns that channel's slot.
* **Gotcha 5**: once a connection issues any of `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`, it can
  never return to normal command execution for the rest of its life, even after unsubscribing
  from everything — and RESP3 does not relax the subscribed-mode command allow-list the way it
  does in real Redis (§4).
* **Gotcha 6**: delivery is `try_send`-based and silently drops on a full per-client queue; a
  persistently slow subscriber is eventually disconnected by the pubsub output-buffer limit
  (default 32 MiB hard / 8 MiB soft for 60s), not by any signal from the hub itself.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib pubsub -- --test-threads=1
```
