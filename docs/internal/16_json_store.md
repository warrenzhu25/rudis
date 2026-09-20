# Component 16: JSON Document Store & JSONPath Engine (Implementation)

## Component 16: JSON Document Store & JSONPath Engine — Code Reference & Implementation

> **Source Files**: ``src/json.rs``


---

### 3. Component Architecture & Data Structures

```
JSON.SET doc:1 $.user.name "\"Alice\""
              │
              ▼
   parse_json_path("$.user.name") -> [Root, Field("user"), Field("name")]
              │
              ▼
   set_json_path: walk parent segments (Root, Field("user")),
   auto-vivifying an empty Object at "user" if it doesn't exist yet,
   then insert "name" -> Value::String("Alice") into that object
              │
              ▼
   JsonStore.docs[doc:1] = { "user": { "name": "Alice" } }
```

---

### Real data structures (verbatim from `src/json.rs`)

```rust
pub enum PathSegment {
    Root,
    Field(String),
    Index(isize),
    Wildcard,
    Slice { start: Option<isize>, end: Option<isize> },
}

pub struct JsonStore {
    docs: HashMap<Bytes, Value>,   // Value = serde_json::Value
}
```

There is no bespoke JSON representation — every stored document is a plain `serde_json::Value`
(the same enum `Value::{Null, Bool, Number, String, Array, Object}` any `serde_json` consumer
would use), not a Redis-specific compact encoding.

---

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `parse_json_path`: character-by-character, no grammar/lexer library

The parser is a single hand-rolled `while let Some(&ch) = chars.peek()` loop over a
`Peekable<Chars>` iterator, branching on `.`, `*`, `[`, or "anything else starts a bare field
name" — no `nom`/`pest`/regex dependency. Bracket contents (`[...]`) are further classified by
trying, in order: `*` (wildcard), a quoted string, a `:`-containing slice, a parseable integer
(index), then falling back to an unquoted field name. This ordering matters: `[0:2]` is
recognized as a slice before the parser ever tries to parse it as an integer.

#### 4.2 `query_json_path` / `query_json_path_mut`: breadth-first accumulation per segment

Both functions maintain a `Vec` of "current matches" and, for each `PathSegment` in turn,
build a new `Vec` of every match's children that satisfy that segment — so `$.items[*].id`
naturally fans out to multiple simultaneous matches (one per array element) by the time it
reaches the final `Field("id")` segment. A `Slice`'s `start`/`end` are each independently
clamped: negative values count from the end (`len + s`), values are `.max(0)`/`.min(len)`
bounded, and `s_idx < e_idx` is required for anything to be included — an empty or
backward-ordered slice range simply matches nothing rather than erroring.

#### 4.3 `set_json_path`: parent traversal with auto-vivification, then a final insert

```rust
for seg in parent_segments {
    match seg {
        PathSegment::Field(name) => {
            if !curr.is_object() { *curr = Value::Object(serde_json::Map::new()); }
            let map = curr.as_object_mut().unwrap();
            if !map.contains_key(name) { map.insert(name.clone(), Value::Object(...)); }
            curr = map.get_mut(name).unwrap();
        }
        PathSegment::Index(idx) => {
            if !curr.is_array() { *curr = Value::Array(Vec::new()); }
            let arr = curr.as_array_mut().unwrap();
            while arr.len() <= actual_idx { arr.push(Value::Null); }
            curr = &mut arr[actual_idx];
        }
        _ => return Err("ERR wildcards not supported as parent path for SET"),
    }
}
```

If an intermediate path element exists but is the *wrong type* (e.g. `$.user.name` where
`user` is currently a string, not an object), it's silently **overwritten** with a fresh empty
container (`*curr = Value::Object(...)`) rather than erroring — a real, permissive behavior
worth knowing: `JSON.SET` can silently destroy a differently-typed intermediate value on the
way to setting a deep path. Setting through a `Wildcard`/`Slice` parent segment is the one
case that does return a real error (`"ERR wildcards not supported as parent path for SET"`).

#### 4.4 `delete_json_path`: wildcard deletes clear whole containers

A `Field`/`Index` last-segment delete removes one entry; a `Wildcard` last segment instead
clears the entire matched `Object`/`Array` in place (`map.clear()`/`arr.clear()`) and counts
every removed entry — so `JSON.DEL key $.items[*]` empties the `items` array (leaving an empty
array behind, not removing the array itself) rather than deleting each element one at a time.

#### 4.5 `JSON.MGET`: sequential per-key, not fanned out — the same gap `MGET`/`MSET` had before their fix

```rust
Command::JsonMget { keys, path } => {
    for k in keys {
        let single_cmd = Command::JsonGet { key: k.clone(), paths: vec![path.clone()] };
        if let Some(target) = target_shard_of_cmd(&single_cmd, router.num_shards) {
            if target == router.shard_id { /* local execute_local_command */ }
            else { let res = router.execute_remote(target, single_cmd).await; /* ... */ }
        } else { out.extend_from_slice(b"$-1\r\n"); }
    }
}
```

Each key in a `JSON.MGET` is routed and awaited **one at a time** — exactly the shape
`MGET`/`MSET` had before the fix documented in Component 02 §4.6/Component 04 §4.3. Unlike
plain `MGET`, `JsonMget` was never given the bucket-by-shard-then-fan-out treatment; a
`JSON.MGET` spanning several remote shards still pays one serialized round-trip per key.

---

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): dispatches every single-key `Command::Json*` through
  the shared `target_shard_of_cmd`/local-vs-`execute_remote` fork (§1); `JsonMget` is a
  standalone arm with its own sequential per-key loop (§4.5).
- **`src/search.rs`** (Component 09): `JSON.SET` on the root path (`$`) triggers
  `index_document_hook` for auto-indexing after a successful write, flattening top-level
  scalar fields into the search engine's document representation — nested objects/arrays are
  stringified, not recursively flattened (already documented in Component 09 §5).
- **`src/table.rs`** (Component 05): no relationship — JSON documents live entirely in
  `ShardDb.json_store: JsonStore`, a separate per-shard map alongside `RudisTable`, never as a
  `RudisValue` variant.
- **RDB persistence: no relationship, verified.** Grepping `table.rs`/`router.rs`'s RDB
  save/restore chunk logic for any reference to `json_store` finds none — `JsonStore` is not
  included in `save_rdb_chunk`/`load_rdb`, so JSON documents do **not** survive a restart via
  RDB. This is the same gap Component 08 §7 flagged for vector indexes.

---

---

### 7. Future Improvements

- **Medium — give `JSON.MGET` the same bucket-and-fan-out treatment `MGET`/`MSET` already got (§4.5).** The building blocks are identical to Component 02/04's fix: bucket the requested keys by target shard, dispatch one batched request per remote shard, await all in parallel, reassemble in original order. Until then, `JSON.MGET` is the one JSON command that doesn't benefit from the cross-shard parallelism the rest of this subsystem already has.
- **Medium — include `JsonStore` in RDB save/restore (§5).** A shared gap with vector indexes (Component 08) and, before its own fix, CRDT state (Component 12) — any of these auxiliary per-shard stores silently losing all data on restart is a real durability surprise for a feature that otherwise looks fully persistent (ordinary keys in the same process do survive a restart).
- **Low — deduplicate `query_json_path`/`query_json_path_mut` (§2.3)**, e.g. via a macro or a trait abstracting over `&`/`&mut` child access, to remove the risk of the two traversal implementations drifting apart on a future bugfix.
- **Low — extend JSONPath coverage** (recursive descent `$..field`, filter expressions `?(@.price < N)`) if closer RedisJSON/JSONPath-spec compatibility becomes a goal (§2.1) — the current subset covers the common cases (field access, indexing, wildcards, slices) but silently no-ops on anything more advanced rather than erroring, which could surprise a client library that assumes full JSONPath support.

---
---
