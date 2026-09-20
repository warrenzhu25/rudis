# Component 19: Pub/Sub Messaging Hub (Implementation)

## Component 19: Pub/Sub Messaging Hub — Code Reference & Implementation

> **Source Files**: ``src/pubsub.rs``


---

### 3. Component Architecture & Data Structures

```
Client A (shard 0)              Client B (shard 2)
   SUBSCRIBE news                  PUBLISH news "hello"
        │                                  │
        ▼                                  ▼
shard 0's PubSubHub              shard 2's Router::publish
  .channels["news"] = {A}                  │
  .clients[A] = write_tx_A          1. shard 2's own PubSubHub.publish("news", "hello")
                                        (0 local subscribers on shard 2 itself)
                                     2. ShardMessage::Publish sent to every OTHER shard,
                                        including shard 0, in parallel
                                     3. shard 0's PubSubHub.publish("news", "hello") finds A,
                                        sends the RESP frame on write_tx_A
                                     4. shard 0's dedicated pubsub writer task (run_pubsub_loop,
                                        Component 02) drains write_tx_A and writes the socket
```

---

### Real data structures (verbatim from `src/pubsub.rs`)

```rust
pub struct PubSubHub {
    pub channels: HashMap<Bytes, HashSet<u64>>,          // channel -> subscriber client IDs
    pub patterns: HashMap<Bytes, HashSet<u64>>,          // pattern -> subscriber client IDs
    pub clients: HashMap<u64, flume::Sender<Vec<u8>>>,   // client ID -> its delivery channel
    pub client_channels: HashMap<u64, HashSet<Bytes>>,   // reverse index: client -> its channels
    pub client_patterns: HashMap<u64, HashSet<Bytes>>,   // reverse index: client -> its patterns
}
```

Two reverse indices (`client_channels`/`client_patterns`) exist purely so
`unsubscribe_all`/`punsubscribe_all`/`total_subscriptions`/connection-drop cleanup don't have
to scan every channel/pattern in the hub looking for a given client — a real, deliberate
O(subscriptions for this client) design instead of O(all channels/patterns).

---

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `glob_match`: backtracking `*`/`?` matcher

```rust
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t, mut star_p, mut match_t) = (0, 0, None, 0);
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

This is the standard greedy-with-backtracking wildcard algorithm: on a literal mismatch, if a
`*` was seen earlier, the matcher "backtracks" by advancing `match_t` (trying to let the `*`
consume one more character of `text`) and resuming pattern matching from just after that `*` —
rather than a recursive/exponential-worst-case naive implementation. `?` matches exactly one
character; there's no character-class (`[abc]`) support, matching real Redis's own
`PSUBSCRIBE` glob semantics (which also lack character classes... actually real Redis glob
*does* support `[...]` classes — this implementation's `?`/`*`-only subset is narrower than
real Redis's pattern language, worth knowing if a client relies on bracket-class patterns).

#### 4.2 `publish`: two independent passes, direct channels then pattern channels

```rust
pub fn publish(&self, channel: &[u8], message: &[u8]) -> usize {
    let mut count = 0;
    // 1. Direct channel subscribers — one frame built once, cloned per subscriber
    if let Some(subscribers) = self.channels.get(channel) {
        let frame = /* build "*3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<message>\r\n" once */;
        for client_id in subscribers {
            if let Some(tx) = self.clients.get(client_id) && tx.send(frame.clone()).is_ok() { count += 1; }
        }
    }
    // 2. Pattern subscribers — iterate EVERY registered pattern, glob-match against this channel
    for (pattern, subscribers) in &self.patterns {
        if glob_match(pattern, channel) { /* build "*4\r\n$8\r\npmessage\r\n..." once, send to each */ }
    }
    count
}
```

Direct-channel delivery is O(subscribers to this exact channel); pattern delivery is
**O(total registered patterns)**, since every pattern has to be glob-matched against the
published channel name — there's no pattern index (e.g. a trie or prefix grouping) to narrow
the set of patterns actually worth checking. `send(frame.clone())` is a `flume` channel send
(non-blocking, delivers to the subscriber's dedicated writer task — Component 02's
`run_pubsub_loop`); a `count` increment happens only if the send actually succeeds, so a
subscriber whose connection has already dropped (channel disconnected) doesn't count as
delivered even though it's still technically registered until the next cleanup path runs.

#### 4.3 Cleanup: `remove_client` is the single exit-path hook

```rust
pub fn remove_client(&mut self, client_id: u64) {
    self.unsubscribe_all(client_id);
    self.punsubscribe_all(client_id);
}
```

Called when a pub/sub-mode connection's loop exits (disconnect, `QUIT`, or a hard error) —
tears down every channel and pattern subscription for that client in one call, relying on
`unsubscribe_all`/`punsubscribe_all`'s own per-entry cleanup (§2.5) to leave no dangling
`HashSet`/`clients` entries behind.

---

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): `run_pubsub_loop` is the sole caller of
  `subscribe`/`unsubscribe`/`psubscribe`/`punsubscribe`/`remove_client` — a permanent
  mode-switch entered on `SUBSCRIBE`/`PSUBSCRIBE`, with its own split reader/writer tasks (a
  dedicated `flume::unbounded` channel feeds a background writer task that drains published
  messages onto the socket, independent of the loop that reads new `SUBSCRIBE`/`UNSUBSCRIBE`/
  `PING`/`QUIT` commands from the client).
- **`src/router.rs`** (Component 04): `Router::publish`/`pubsub_channels`/`pubsub_numsub`/
  `pubsub_numpat` are the only entry points that reach across shards — each publishes/queries
  the local `PubSubHub` first, then fans out to every other shard's `ShardMessage::Publish`/
  equivalent and merges results (§2.2).
- **`src/shard.rs`** (Component 04): `ShardMessage::Publish { channel, message, responder }`
  is the wire message a remote shard's `PubSubHub.publish` call is delivered through.
- **`src/table.rs`**: no relationship — pub/sub state is entirely separate from `RudisTable`;
  a channel name is never a Redis key and never interacts with expiration/eviction.

---

---

### 7. Future Improvements

- **Medium — send RESP3 push-type frames (`>`) to RESP3-negotiated subscribers instead of always RESP2 arrays (§2.4).** This is a real, verified protocol-compliance gap: a client that sent `HELLO 3` and is tracked as `is_resp3` elsewhere in the codebase (Component 02) still receives plain `*3\r\n...`/`*4\r\n...` array frames for `message`/`pmessage` delivery here, not the RESP3 push type real Redis switches to. Since one frame is currently built once and cloned to every subscriber (§4.2, a real performance benefit), fixing this requires either building two frame variants up front (RESP2 and RESP3) and picking per-subscriber based on a tracked protocol flag, or moving per-subscriber protocol awareness into `PubSubHub` itself (currently it has none).
- **Medium — index patterns to avoid an O(total patterns) scan per publish (§4.2/§6).** A simple first step: group patterns by their literal (non-wildcard) prefix so a publish only glob-matches against patterns whose prefix could plausibly match the channel, rather than every registered pattern unconditionally.
- **Low — add character-class (`[abc]`/`[a-z]`) support to `glob_match` (§4.1)** if closer compatibility with real Redis's full glob pattern language (which does support bracket classes) becomes a goal — today `?`/`*` are the only wildcard forms recognized.
- **Low — consider whether `PubSubHub` being per-shard (rather than a single process-wide hub like `BlockHub`) has any subtle ordering implications worth documenting** — e.g. two publishes to the same channel from different shards in quick succession have no cross-shard ordering guarantee relative to each other, only FIFO delivery within whichever shard's `flume` channel a given subscriber is registered on.

---
---
