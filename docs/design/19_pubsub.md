# Component 19: Pub/Sub Messaging Hub (Design)

## Component 19: Pub/Sub Messaging Hub

> **Source Files**: ``src/pubsub.rs``


---

### 1. Architectural Purpose & Scope

`src/pubsub.rs` implements `PubSubHub`, the per-shard channel/pattern subscription registry
backing `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`PUBLISH`/`PUBSUB CHANNELS`/
`NUMSUB`/`NUMPAT`. Like `BlockHub` (Component 06), a client that issues `SUBSCRIBE` hands its
connection off to a dedicated, permanent mode-switch loop (`run_pubsub_loop` in
`connection.rs`) that never returns to ordinary command processing for the lifetime of that
TCP connection. Unlike `BlockHub`, `PubSubHub` is genuinely **per-shard** (one instance per
shard, owned by `Router.pubsub`, not a process-wide `Arc<Mutex<_>>`) — cross-shard delivery
(a publisher on shard A reaching a subscriber connected via shard B) is handled by `Router::
publish` fanning the message out to every other shard's own `PubSubHub`, not by sharing one
hub across shards.

---

---

### 2. Key Invariants & Concurrency Constraints

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

---

### 6. Performance Characteristics

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
