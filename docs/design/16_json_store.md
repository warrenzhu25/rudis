# Component 16: JSON Document Store & JSONPath Engine (Design)

## Component 16: JSON Document Store & JSONPath Engine

> **Source Files**: ``src/json.rs``


---

### 1. Architectural Purpose & Scope

`src/json.rs` implements a RedisJSON-compatible document store: a hand-written JSONPath
parser/evaluator operating directly on `serde_json::Value` trees, plus `JsonStore`, the
per-shard map of key → JSON document that backs `JSON.SET`/`GET`/`DEL`/`TYPE`/`NUMINCRBY`/
`STRAPPEND`/`STRLEN`/`ARRAPPEND`/`ARRLEN`/`ARRPOP`/`OBJKEYS`/`OBJLEN`/`TOGGLE`/`CLEAR`/`MGET`.
Unlike `src/vector.rs` (Component 08) and unlike `src/crdt.rs` before its fix (Component 12),
single-key JSON commands are **genuinely routed per-key across shards** — verified directly
in `connection.rs`: every `Command::Json*` variant (except `JsonMget`, see §4.5) appears in
the same `target_shard_of_cmd`/local-vs-`execute_remote` dispatch arm as ordinary string/hash/
list commands, so a `JSON.SET`/`GET` on a given key always lands on the one shard that key
actually hashes to, regardless of which shard's connection issued it.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **A real, but partial, JSONPath implementation.** `parse_json_path` hand-parses `$`, bare
   `.field` traversal, `[idx]` (including negative indices), `[*]` wildcards, `[start:end]`
   slices (including negative/omitted bounds), and `["quoted"]`/`['quoted']` field names. There
   is **no recursive descent (`$..field`) and no filter-expression syntax (`?(@.price < 10)`)**
   — both real RedisJSON/JSONPath features. A path using either silently fails to match
   anything (parses as a literal field name containing those characters) rather than erroring.
2. **Whole-document storage, no incremental structure.** `JsonStore.docs: HashMap<Bytes,
   Value>` stores one complete `serde_json::Value` tree per key. A `JSON.SET`/`NUMINCRBY`/etc.
   on a deeply nested path still has to parse the target's own sub-value in place (via
   `query_json_path_mut`, no full-document re-parse), but `JSON.GET` always calls
   `serde_json::to_string` fresh on whatever subtree matched — there's no cached serialized
   form, and a `JSON.GET key $` on a huge document re-serializes the entire thing every call.
3. **`query_json_path`/`query_json_path_mut` are structurally identical, hand-duplicated for
   `&`/`&mut`.** Every match arm in the immutable traversal (§4.2) has a corresponding
   `_mut` arm doing the identical navigation logic against `.get`/`.get_mut`,
   `.values()`/`.values_mut()`, `&arr[i]`/`&mut arr[i]`. This is a real, verified
   duplication (not a design choice with a stated rationale) — a bugfix to one traversal
   rule (e.g. how negative slice bounds clamp) has to be applied to both copies by hand.
4. **Auto-vivification on `SET`, not on read.** `set_json_path` creates intermediate
   `Object`/`Array` containers as needed when writing to a path whose parents don't exist yet
   (§4.3) — real Redis JSON has the same behavior. `NX`/`XX` are checked once, up front,
   against whether the *target* path already resolves to something, before any mutation.

---

---

### 6. Performance Characteristics

- **`JSON.GET` cost scales with matched-subtree size, not query specificity** — every call
  does a fresh `serde_json::to_string` of whatever `query_json_path` returned, with no
  memoization; repeatedly reading the same small field from a large sibling-heavy document is
  cheap, but repeatedly reading `$` on a large document is not.
- **Path traversal is O(document breadth) per segment, not indexed** — `Field` lookups on an
  `Object` are O(1) (backed by `serde_json`'s own map), but `Wildcard`/`Slice` segments
  necessarily visit every child at that level; there's no precomputed path index.
- **`JSON.MGET`'s sequential fan-out (§4.5) is the single biggest addressable cost** on
  multi-key JSON reads spread across shards — see Future Improvements.

---
