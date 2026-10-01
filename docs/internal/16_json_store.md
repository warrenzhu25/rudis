# Component 16: JSON Document Store & JSONPath Engine (Implementation Deep-Dive & Code Reference)

> **Source File**: `src/json.rs` (991 lines)
> **Command dispatch**: `src/resp.rs` (`Command::Json*` variants + parser), `src/connection.rs` (`execute_local_command` arms)
> **Cross-shard fan-out**: `src/router.rs` (`Router::json_mget`), `src/shard.rs` (`ShardMessage::JsonMget`), `src/server.rs` (cross-shard mailbox loop)
> **Persistence**: `src/shard.rs` (`ShardDb::save_rdb_chunk`/`save_extended_rdb_chunk`/`restore_rdb_chunk`), `src/table.rs` (`load_rdb`/`load_rdb_bytes`)
> **AOF/replication encoding**: `src/aof.rs` (`command_to_resp`)
> **RediSearch integration**: `src/search.rs` (`index_json_document_hook`, `extract_json_fields`), `src/shard.rs` (`ShardDb::index_json_document_local`)
> **High-Level Design Spec**: [`docs/design/16_json_store.md`](../design/16_json_store.md)

---

## 1. Source Module Map

| File | Role |
| :--- | :--- |
| `src/json.rs` | `PathSegment` enum, JSONPath parser/evaluator (`parse_json_path`, `query_json_path[_mut]`, `set_json_path`, `delete_json_path`), and `JsonStore` (the per-shard document map + every `json_*` command method). |
| `src/resp.rs` | `Command::Json*` enum variants (lines 1437-1508) and the `"JSON.*" =>` parse arms (lines 10213-10418). |
| `src/connection.rs` | `execute_local_command` match arms that call into `JsonStore` (lines 16783-16996), plus the `JsonMget` dispatch arm (7507-7517), the `record_change!` macro that drives dirty-tracking/AOF/replication (12740-12761), and the single-key extraction lists (`cmd_primary_key`, `for_each_cmd_key`, `get_cmd_name`) that include every `Json*` variant. |
| `src/router.rs` | `Router::json_mget` (1543-1616): the only JSON command with its own bucket-by-shard, parallel-fan-out implementation. |
| `src/shard.rs` | `ShardDb.json_store: JsonStore` field (618); `ShardMessage::JsonMget` wire struct (402-406); `ShardDb::save_rdb_chunk`/`save_extended_rdb_chunk`/`restore_rdb_chunk` (2274-2394, 2659+); `index_json_document_local` (773-792). |
| `src/table.rs` | `load_rdb`/`load_rdb_bytes` file-load path that restores JSON documents via `type_byte == 7` (14566-14600). |
| `src/aof.rs` | `command_to_resp` (166-1936): has **no** `Command::Json*` arm — every JSON write falls through to the final `_ => None`. |
| `src/search.rs` | `index_json_document_hook`/`extract_json_fields` — the global (cross-shard) RediSearch JSON indexing path. |

---

## 2. Storage Model

```rust
// src/json.rs:437-440
#[derive(Default, Debug)]
pub struct JsonStore {
    docs: HashMap<Bytes, Value>,   // Value = serde_json::Value (hashbrown::HashMap)
}
```

There is **no bespoke JSON representation**. Every document is stored as a plain `serde_json::Value`
(`Null | Bool | Number | String | Array | Object`) — the exact same enum any generic `serde_json`
consumer uses, not a Redis-specific compact encoding, not an arena, not a DOM with parent pointers.
`JsonStore` lives at `ShardDb.json_store: crate::json::JsonStore` (`src/shard.rs:618`), a field
sitting alongside `table: RudisTable`, `crdt_store`, `probabilistic_store`, etc. — **JSON documents
never become a `RudisValue` variant** and are invisible to ordinary `GET`/`TYPE`/`OBJECT ENCODING`.

`JsonStore`'s only other state-bearing methods are `new()` (443), `iter()` (450, used by RDB save
and by `FT.CREATE`'s backfill scan), `insert_raw(key, val)` (455, used only by RDB/replication
restore paths — bypasses path logic entirely), `get(key)` (460), `len()`/`is_empty()` (874-880).
There is no eviction, no TTL, no per-document size cap, and no limit on nesting depth — the only
ceiling is `proto-max-bulk-len` on the inbound `JSON.SET` payload and available process memory.

---

## 3. JSONPath Engine

### 3.1 Grammar actually supported

`PathSegment` (`src/json.rs:7-16`):

```rust
pub enum PathSegment {
    Root,
    Field(String),
    Index(isize),
    Wildcard,
    Slice { start: Option<isize>, end: Option<isize> },
}
```

Supported syntax, enumerated from what `parse_json_path` actually recognizes: `$` / `.` (root),
`.field` / bare `field`, `[0]` / `[-1]` (negative index, resolved relative to array length at
query time, not at parse time), `[*]` / bare `*` (wildcard — matches all object values or all
array elements), `[0:2]` / `[:3]` / `[1:]` / `[:]` (slice, each side independently optional),
`["key"]` / `['key']` (quoted bracket field access — lets a field name contain `.` or `[`), and a
bare unquoted bracket name `[key]` (falls through to the same `Field` branch as a quoted name).

**No filter expressions** (`?(@.price < N)`) exist anywhere in `src/json.rs` — there is no
`@`-token handling, no comparison-operator parsing, nothing. A path containing `?(...)` gets
parsed field-by-field as ordinary (nonexistent) field names and simply matches nothing.

### 3.2 Recursive descent (`$..field`) — re-verified, NOT implemented

The parser's main loop (`src/json.rs:43-118`) has a comment that is misleading on its own:

```rust
// src/json.rs:43-52
while let Some(&ch) = chars.peek() {
    if ch == '.' {
        chars.next();
        // Could be another dot for recursive or wildcard
        if chars.peek() == Some(&'*') {
            chars.next();
            segments.push(PathSegment::Wildcard);
        }
        continue;
    }
    ...
```

Tracing `"$..name"` character by character: the pre-loop block consumes `$` (pushes `Root`) and
then opportunistically consumes one trailing `.`, leaving `.name` (one dot + "name"). The main
loop then sees `ch == '.'`, consumes *that* second dot, checks whether the next char is `*`
(it's `n`, so no), and `continue`s **without pushing any segment at all** — the dot is simply
discarded. The next iteration parses `"name"` as an ordinary `Field`. Final result:
`[Root, Field("name")]` — byte-for-byte identical to parsing `"$.name"`.

**There is no `PathSegment::RecursiveDescent` variant and no code path that walks more than one
level down for a double dot.** `$..name` does not search `name` at every nesting depth; it
silently degrades to a single-level `$.name` lookup, matching only a direct child of the root
named `name`. This matches what `docs/design/16_json_store.md:31` already states ("no recursive
descent"), and contradicts any claim that recursive descent is supported — there is no supporting
code for that claim anywhere in `src/json.rs`.

### 3.3 `query_json_path` / `query_json_path_mut`: breadth-first, segment-by-segment fan-out

Both functions (124-201 immutable, 204-285 mutable — hand-duplicated, not shared via a generic/
trait) maintain a `Vec` of "current matches" and replace it with a new `Vec` after each segment:

- `Field(name)`: for each current match that is an `Object`, look up `name`; non-matches drop out.
- `Index(idx)`: negative indices are resolved as `len + idx` (as `usize`, so an index past the
  start wraps rather than erroring — see §7); out-of-range indices silently drop the match.
- `Wildcard`: fans out to every object value or every array element.
- `Slice { start, end }`: `start`/`end` each independently default (`0` / array length) and are
  clamped: negative counts from the end via `(len + s).max(0)`, positive is `.min(len)`; if the
  resulting `s_idx >= e_idx` the slice contributes **zero** matches for that branch (no error).

If `current` becomes empty after any segment, the loop `break`s early (124/204: `if current.is_empty() { break; }`). The two functions are textually near-identical except `&'a Value`/`map.get`/`&arr[i]` in the immutable version vs `&'a mut Value`/`map.get_mut`/`&mut arr[i]` in the mutable one — a real duplication-drift risk flagged again in §7.

### 3.4 `set_json_path`: parent-segment walk with silent-overwrite auto-vivification

`src/json.rs:290-381`. Segments are split into `parent_segments` (all but last) and `last_seg`.
Walking the parents (321-351):

```rust
PathSegment::Field(name) => {
    if !curr.is_object() { *curr = Value::Object(serde_json::Map::new()); }
    let map = curr.as_object_mut().unwrap();
    if !map.contains_key(name) { map.insert(name.clone(), Value::Object(Map::new())); }
    curr = map.get_mut(name).unwrap();
}
PathSegment::Index(idx) => {
    if !curr.is_array() { *curr = Value::Array(Vec::new()); }
    let arr = curr.as_array_mut().unwrap();
    while arr.len() <= actual_idx { arr.push(Value::Null); }   // pads with Null
    curr = &mut arr[actual_idx];
}
_ => return Err("ERR wildcards not supported as parent path for SET"),
```

**If an intermediate node exists but is the wrong type it is silently destroyed and replaced**
with a fresh empty container (`*curr = Value::Object(...)` / `Value::Array(...)`) — there is no
type-check error. `JSON.SET doc $.user.name "..."` where `user` currently holds a string will
overwrite that string with `{}` and continue. The *only* error path for a parent segment is
`Wildcard`/`Slice` (`"ERR wildcards not supported as parent path for SET"`); `last_seg` being
anything other than `Field`/`Index` gives `"ERR invalid target path for SET"`.

`nx`/`xx` are checked once, up front, against `query_json_path(root, &segments).is_empty()`
(308-314) — not per-match, since `set_json_path` always writes exactly one target (the last
segment can't be a `Wildcard`/`Slice`, so there's never more than one write site). Passing both
`nx: true, xx: true` always returns `Ok(false)`: either the target exists (nx fails) or it
doesn't (xx fails) — there is no third outcome.

### 3.5 `delete_json_path`: wildcard-last-segment clears the whole container

`src/json.rs:385-434`. Root deletion special-cases to `*root = Value::Null` (391, count 1 — the
key itself is removed one level up, in `JsonStore::json_del`, not here). For a non-root path, it
calls `query_json_path_mut` on the **parent** segments and, per parent:
- `Field`/`Index` last segment: removes exactly one entry if present.
- `Wildcard` last segment: `map.clear()`/`arr.clear()` — removes every entry in that one container
  and counts `map.len()`/`arr.len()` as the deleted count — so `JSON.DEL key $.items[*]` empties
  the `items` array in place (array itself survives, now `[]`) rather than deleting one element
  at a time.
- `Slice` as last segment is unhandled (`_ => {}` at 429) — contributes 0 deletions silently.

---

## 4. `JsonStore` command methods — exact behavior

All take `key: &[u8]`, internally doing `Bytes::copy_from_slice(key)` to look up `self.docs`
(a `HashMap<Bytes, Value>` keyed by owned `Bytes` — each call allocates a fresh `Bytes` copy of
the key for the lookup, even on a pure read).

| Command | Method (lines) | Path default | Multi-match behavior |
| :--- | :--- | :--- | :--- |
| `JSON.SET` | `json_set` (465-499) | required | single write site only (§3.4) |
| `JSON.GET` | `json_get` (502-531) | `$` | returns raw single value **only if** exactly 1 match **and** the path string contains neither `*` nor `:`; otherwise wraps in a JSON array |
| `JSON.DEL`/`JSON.FORGET` | `json_del` (534-552) | none (root) | wildcard-last clears container (§3.5) |
| `JSON.TYPE` | `json_type` (555-574) | `$` | **first match only** (`matches.first()`) |
| `JSON.NUMINCRBY` | `json_numincrby` (577-612) | required | **all matches** incremented; single result → bare number string, multi-match → `"[v1,v2,...]"` |
| `JSON.NUMMULTBY` | `json_nummultby` (615-655) | required | **all matches** multiplied (own method — see §5) |
| `JSON.STRAPPEND` | `json_strappend` (658-686) | `$` | **all matches** appended to; returns the length of the *last* one processed |
| `JSON.STRLEN` | `json_strlen` (689-698) | `$` | **first match only** |
| `JSON.ARRAPPEND` | `json_arrappend` (701-737) | required | **all matches** appended to; returns length of the *last* one processed |
| `JSON.ARRLEN` | `json_arrlen` (740-749) | `$` | **first match only** |
| `JSON.ARRPOP` | `json_arrpop` (752-780) | `$` | **first match only** (`matches.into_iter().next()`), even for a wildcard path matching several arrays |
| `JSON.OBJKEYS` | `json_objkeys` (783-792) | `$` | **first match only** |
| `JSON.OBJLEN` | `json_objlen` (795-804) | `$` | **first match only** |
| `JSON.TOGGLE` | `json_toggle` (807-833) | **required**, no default | **all matches** toggled |
| `JSON.CLEAR` | `json_clear` (837-872) | `$` | **all matches** cleared/zeroed |
| `JSON.MGET` | not a `JsonStore` method — see §6 | required | handled entirely in `router.rs` |

Note the split: incrementing/multiplying/appending/toggling/clearing act on **every** matched
node (loop `for val in matches`), while the length/introspection getters (`TYPE`, `STRLEN`,
`ARRLEN`, `OBJKEYS`, `OBJLEN`) and `ARRPOP` act on **only the first** match — a wildcard path like
`$.items[*]` behaves completely differently depending on which command you run against it.

`JSON.TOGGLE`'s path parameter is **mandatory** at the parser level (`src/resp.rs:10391-10397`
requires `args.len() >= 3`) — unlike real RedisJSON where path defaults to root — so
`JSON.TOGGLE key` (no path) is a parse-time "wrong number of arguments" error here, not a
root-level toggle.

### 4.1 Numeric mutation detail (`json_numincrby`/`json_nummultby`, nearly identical bodies)

```rust
// src/json.rs:589-605 (numincrby) — nummultby (632-648) is the same shape with `*` not `+`
for val in matches {
    if let Value::Number(num) = val {
        let cur = num.as_f64().unwrap_or(0.0);
        let new_num = cur + delta;                      // or cur * factor
        if let Some(n) = Number::from_f64(new_num) {
            *val = Value::Number(n);
            results.push(new_num.to_string());
        } else {
            let int_val = new_num.round() as i64;        // NaN/Infinity fallback
            *val = json!(int_val);
            results.push(int_val.to_string());
        }
    } else {
        return Err("ERR value at path is not a number".to_string());
    }
}
```

If **any** matched node is a non-number, the whole call errors out with
`"ERR value at path is not a number"` — but nodes processed earlier in the `matches` iteration
order have **already been mutated in place** before the error is returned, since the loop doesn't
pre-validate all matches before writing any of them. A wildcard `NUMINCRBY` over a mixed-type
array can partially apply.

### 4.2 `JSON.NUMMULTBY` now has its own `JsonStore` method — fixed since the last docs pass

An earlier version of this subsystem (and the previous revision of this doc) had
`Command::JsonNumMultBy` composed in `connection.rs` from two separate `json_numincrby` calls
(`json_numincrby(key, path, 0.0)` to read, then a second call with the computed delta), which
broke on any multi-match wildcard path because the intermediate read returned a bracketed
multi-value string that couldn't `parse::<f64>()`. **That composition no longer exists.**
`JsonStore::json_nummultby` (615-655) is now a standalone method, structurally identical to
`json_numincrby` but multiplying instead of adding, and it correctly handles multi-match paths —
confirmed by the in-file unit test:

```rust
// src/json.rs:976-989
let mult_res = store.json_nummultby(key, "$.items[*].price", 2.0).unwrap();
assert_eq!(mult_res, "[20,50,100]");   // doc had prices 10, 25, 50
```

`connection.rs`'s `Command::JsonNumMultBy` arm (16863-16877) is now a single call:
`db.json_store.json_nummultby(key, path, *factor)`, mirroring `JsonNumIncrBy`'s arm exactly.
This was part of commit `784ccb4` ("multi-match json_nummultby and HNSW vector index integration
in RediSearch" — `git log --oneline -- src/json.rs`).

### 4.3 `JSON.GET`'s raw-vs-wrapped heuristic (502-531)

```rust
// src/json.rs:510-519
if paths.len() == 1 {
    let matches = query_json_path(doc, &segments);
    if matches.is_empty() {
        Some("[]".to_string())
    } else if matches.len() == 1 && !paths[0].contains('*') && !paths[0].contains(':') {
        Some(serde_json::to_string(matches[0]).unwrap_or_default())   // raw value
    } else {
        Some(serde_json::to_string(&matches).unwrap_or_default())      // JSON array of matches
    }
}
```

The raw-vs-array decision is driven by the **path string's syntax** (does it literally contain
`*` or `:`), not purely by match count: `$.items[0]` with one match returns the raw value;
`$.items[*]` with exactly one match (e.g. a single-element array) still returns it wrapped in an
array, because the path text contains `*`. Multiple `paths` (`JSON.GET key $.a $.b`) always
returns a JSON object mapping each path string to its (always array-wrapped, via `json!(matches)`)
result set (521-528), regardless of match count per path.

---

## 5. In-place mutation mechanics (whole-document clone vs subtree mutation)

Every `JsonStore` mutator obtains `&mut Value` references directly into the stored document via
`query_json_path_mut` and writes through them (`*val = ...`, `s.push_str(...)`, `arr.push(...)`,
`map.clear()`) — **no document is ever deep-cloned to be mutated.** The one exception, and it's
not inside `JsonStore` at all: `connection.rs`'s `JsonSet`/`JsonDel` arms need to call
`db.index_json_document_local(...)` (a `&mut self` method on `ShardDb`) while also needing a
`&Value` of the just-written document, and the borrow checker won't let `db.json_store.get(key)`
stay borrowed across a `db.index_json_document_local(...)` call. So those two arms do:

```rust
// src/connection.rs:16790-16798 (JsonSet) — JsonDel (16821-16833) does the same for the clear-or-delete case
if let Some(doc) = db.json_store.get(key).cloned() {   // full Value clone, every successful SET/DEL
    let k_str = String::from_utf8_lossy(key);
    db.index_json_document_local(&k_str, &doc);
    crate::search::index_json_document_hook(&k_str, &doc);
}
```

Every other mutating command (`NumIncrBy`, `NumMultBy`, `StrAppend`, `ArrAppend`, `ArrPop`,
`Toggle`, `Clear`) only calls the global `crate::search::index_json_document_hook`, passing a
plain `&Value` borrow (`db.json_store.get(key)`, no `.cloned()`) — so **only `JSON.SET` and
`JSON.DEL` pay a full-document clone cost**, and only because they also update the *shard-local*
`search_indices` map (`index_json_document_local`), which the incremental mutators skip entirely.
That means a RediSearch index with `ON JSON` schema sees shard-local index updates from
`JSON.SET`/`JSON.DEL` but **not** from `JSON.NUMINCRBY`/`ARRAPPEND`/etc. against
`ShardDb.search_indices` — only the global `SEARCH_INDICES` registry (via
`index_json_document_hook`) is kept current for those. (Full dual-registry architecture is
Component 09's territory; noted here only for the JSON-write code path.)

---

## 6. Command dispatch end-to-end

### 6.1 `Command` enum & parser (`src/resp.rs`)

16 `Json*` variants exist (1437-1508): `JsonSet{key,path,json_val,nx,xx}`, `JsonGet{key,paths}`,
`JsonDel{key,path:Option}`, `JsonType{key,path:Option}`, `JsonNumIncrBy{key,path,delta}`,
`JsonNumMultBy{key,path,factor}`, `JsonStrAppend{key,path:Option,value}`,
`JsonStrLen{key,path:Option}`, `JsonArrAppend{key,path,values}`, `JsonArrLen{key,path:Option}`,
`JsonArrPop{key,path:Option,index:Option<isize>}`, `JsonObjKeys{key,path:Option}`,
`JsonObjLen{key,path:Option}`, `JsonToggle{key,path}` (mandatory path), `JsonClear{key,path:Option}`,
`JsonMget{keys:Vec<Bytes>,path}`.

The parse arms (`src/resp.rs:10213-10418`) are plain string-command dispatch — no grammar/parser
library. Two things worth flagging precisely:
- **`"JSON.DEL" | "JSON.FORGET" =>` is a single shared arm** (10253) — `JSON.FORGET` is a true
  alias of `JSON.DEL`, not a distinct command.
- **`JSON.NUMMULTBY`/`JSON.NUMINCRBY`/`JSON.ARRAPPEND`/`JSON.TOGGLE` all require their path
  argument** (`args.len() < 4`/`< 3` checks) — no "defaults to root" fallback for these four,
  unlike `GET`/`DEL`/`TYPE`/`STRAPPEND`/`STRLEN`/`ARRLEN`/`ARRPOP`/`OBJKEYS`/`OBJLEN`/`CLEAR`
  which do default an absent path argument to `$`/`None`.
- `JSON.SET`'s `NX`/`XX` flags are parsed case-insensitively from any trailing args
  (`to_uppercase()`, 10222-10228) with no validation against unrecognized trailing tokens — a
  typo'd modifier is silently ignored rather than erroring.

**Commands that do not exist anywhere in the codebase** (not in the `Command` enum, not parsed,
not implemented in `JsonStore`): `JSON.ARRINSERT`, `JSON.ARRTRIM`, `JSON.MERGE`, `JSON.DEBUG`,
`JSON.RESP`. Sending any of these to the server hits the normal "unknown command" path — there is
no stub, no partial implementation, no `unimplemented!()` marker; grepping `resp.rs`/`json.rs`/
`connection.rs` for `ArrInsert`/`ArrTrim`/`JsonMerge`/`JsonDebug`/`JsonResp` finds zero matches.

### 6.2 `execute_local_command` arms (`src/connection.rs:16783-16996`)

Each arm (`JsonSet` through `JsonClear`) follows the same shape: call the matching `JsonStore`
method, and on a mutation-that-actually-changed-something, call `record_change!(cmd)` (§6.3) and
re-index via `crate::search::index_json_document_hook` (and, for `JsonSet`/`JsonDel` only,
`db.index_json_document_local` — §5). Response encoding is plain hand-rolled RESP: `+OK\r\n`,
`$-1\r\n` for not-found/no-op, `:N\r\n` for integer-returning commands, `$len\r\n...\r\n` bulk for
string-returning ones, and a manual `*N\r\n` + per-element bulk loop for `JSON.OBJKEYS` (16947-16956).

Every single-key `Json*` command is included in all three of `connection.rs`'s shared
single-key-command lists: `cmd_primary_key` (2778, Json arms at 2902-2916, used for sharding
target resolution), `for_each_cmd_key` (3041, Json arms at 3158-3169+, used by `record_change!`'s
`WATCH`-key-touch logic and by `cmd_keys` at 3443), and `get_cmd_name` (3965, Json arms map to the
literal string `"JSON"` for stats/`MONITOR`/ACL-category display, ~4250-4265). `JsonMget` is
**not** in `cmd_primary_key`/`for_each_cmd_key` (it has `keys: Vec<Bytes>`, not a single `key`) —
it gets its own standalone dispatch arm instead (next section).

### 6.3 `record_change!` macro — dirty tracking, AOF, and replication fan-out

```rust
// src/connection.rs:12740-12761
macro_rules! record_change {
    ($cmd_expr:expr) => {
        DIRTY_CHANGES.fetch_add(1, Ordering::Relaxed);
        if HAS_WATCHED_KEYS.load(Ordering::Relaxed) {
            for_each_cmd_key($cmd_expr, |k| touch_watched_key(db.port, k));
        }
        let need_aof = aof.is_some();
        let need_rep = crate::replication::has_connected_replicas(db.port);
        if need_aof || need_rep {
            if let Some(bytes) = crate::aof::command_to_resp($cmd_expr) {
                if let Some(aof_w) = aof { aof_w.borrow_mut().append(&bytes); }
                if need_rep { crate::replication::propagate_shard_bytes(db.port, db.shard_id, &bytes); }
            }
        }
    };
}
```

Every mutating `Json*` arm calls this (so `DIRTY_CHANGES` increments and `WATCH`ed-key
invalidation fires correctly for JSON writes — that part works), **but** the AOF-append and
replica-propagate both happen only `if let Some(bytes) = crate::aof::command_to_resp($cmd_expr)` —
and that is where JSON falls through to nothing (§8).

---

## 7. `JSON.MGET` — now bucketed and fanned out in parallel (fixed since the last docs pass)

The previous revision of this subsystem issued one `Command::JsonGet` per key, sequentially
awaited, one round trip at a time for remote shards — the same gap `MGET`/`MSET` originally had.
**That is no longer the implementation.** `Command::JsonMget` now dispatches to a dedicated router
method:

```rust
// src/connection.rs:7507-7517
Command::JsonMget { keys, path } => {
    let results = router.json_mget(keys, &path).await;
    ...
}
```

`Router::json_mget` (`src/router.rs:1543-1616`):
1. Single-shard fast path (`num_shards <= 1`, 1550-1557): straight local loop, no fan-out needed.
2. Otherwise, partitions every key by `self.target_shard(&key)` into `local_keys` and one
   `Vec<(idx, Bytes)>` per remote shard (`remote_batches`, 1559-1572), preserving each key's
   original result-index.
3. Local keys are resolved immediately in-place (1577-1582).
4. One `ShardMessage::JsonMget { keys, path, responder }` is sent **per remote shard that has at
   least one key** (not per key) over a `flume::bounded(1)` responder channel (1589-1604) —
   this is the batched cross-shard IPC message, defined at `src/shard.rs:402-406`:
   `JsonMget { keys: Vec<(usize, Bytes)>, path: String, responder: flume::Sender<Vec<(usize, Option<String>)>> }`.
5. All remote responders are awaited concurrently (`for rx in responders { rx.recv_async().await }`,
   1606-1613) — one round trip per *shard*, run in parallel, not one round trip per *key*.

The actual remote-shard-side handler lives in the cross-shard mailbox receive loop,
`src/server.rs:1470-1479`: for each `(idx, key)` in the batch it calls
`db.json_store.json_get(&key, &[path_ref])` and sends back `Vec<(idx, Option<String>)>`, which
`json_mget` scatters back into the correctly-ordered `results` vector by `idx`. (A second,
near-identical handler exists at `src/router.rs:4188-4204` inside a `#[cfg(test)]`-style
in-process test harness spawned with a dummy `ShardDb::new(9996)` — same logic, test-only.)

---

## 8. Persistence & Replication — precise status (re-verified against current source)

### 8.1 RDB save/restore: JSON documents **do** survive, via extended type tag `7`

`ShardDb::save_rdb_chunk` (`src/shard.rs:2274-2318`) serializes the normal `RudisTable` entries
first, then **unconditionally** calls `self.save_extended_rdb_chunk(buf)` at line 2317 — there is
no feature flag or config gate skipping it. `save_extended_rdb_chunk` (2320-2394+) writes JSON
documents as its *first* extended-record category:

```rust
// src/shard.rs:2321-2329
for (key, val) in self.json_store.iter() {
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.push(7u8);                                            // extended-record type tag
    let json_str = val.to_string();
    buf.extend_from_slice(&(json_str.len() as u32).to_le_bytes());
    buf.extend_from_slice(json_str.as_bytes());
}
```

Wire format per record: `u32 LE key_len | key_bytes | 0x07 | u32 LE json_len | json_utf8_bytes`.
(Bloom filters use tag `8`, vector-index nodes use tag `9|0x80`, etc. — JSON is simply the first
of several extended record kinds appended after the ordinary table.)

**Two independent restore paths both handle tag 7 identically:**
- File-based load, `src/table.rs::load_rdb`/`load_rdb_bytes` (14566-14600): reads `type_byte == 7`,
  UTF-8-decodes and `serde_json::from_str`s the payload, checks
  `crate::router::target_shard(&key, num_shards) == shard_id` (so each shard only claims the keys
  it owns on a multi-shard reload), then `db.json_store.insert_raw(key.clone(), val)`.
- In-memory chunk restore, `src/shard.rs::ShardDb::restore_rdb_chunk` (2659+, tag-7 branch at
  2705-2722): same decode, same `insert_raw`, no shard-ownership filter (caller already knows
  which shard's chunk it is).

**Where each path is wired in:**
- `Router::generate_full_rdb` (`src/router.rs:3099-3120+`) calls `save_rdb_chunk` on the local
  shard and fans a `ShardMessage::SaveRdbChunk` out to every other shard, concatenating all chunks
  behind a `"REDIS0011"` header — this is what `SAVE`/`BGSAVE` writes to disk, and what
  `table.rs::load_rdb`/`load_rdb_bytes` reads back on process startup. **JSON documents fully
  survive a `SAVE`/`BGSAVE` + restart cycle.**
- The per-shard replication full-sync handler in `src/connection.rs` (~2700-2723, the DFLY-FLOW-
  style stream setup that registers a shard flow and fetches `save_rdb_chunk`) sends this same
  chunk — including JSON — to a newly-attaching replica as its initial snapshot, consumed via
  `restore_rdb_chunk`. **A replica's initial full resync also receives all JSON documents that
  existed at snapshot time.**
- This was added by commit `63437cb` ("feat(rdb): add snapshot persistence and restore for JSON,
  probabilistic, and vector data types") — confirming the fix is real and recent, not speculative.

This **reverses** an earlier finding (carried in a prior revision of this document) that JSON had
"no relationship" to RDB and did not survive a restart. That claim is now false; verify-before-
trusting it was the entire point of this pass, and the current code unambiguously persists JSON
through both on-disk RDB and in-memory full-resync snapshot paths.

### 8.2 AOF append / incremental replication: still zero coverage — confirmed precisely

Despite every mutating `Json*` arm calling `record_change!(cmd)` (§6.3), which *does* attempt an
AOF append and replica propagation, the actual encoding step fails silently for JSON:
`crate::aof::command_to_resp` (`src/aof.rs:166-1936`) is one large `match cmd { ... }` with a
dedicated arm per `Command` variant family (`Set`, `Incrbyfloat`, `Vsetattr`, geo, stream, CRDT,
etc.) ending in a catch-all `_ => None` at line 1936. **Grepping the full body of this match for
`Json` finds zero arms.** Every `Command::Json*` value — mutating or not — falls through to
`_ => None`.

Consequence, traced through `record_change!`: `command_to_resp($cmd_expr)` returns `None` →
the `if let Some(bytes) = ...` body never executes → `aof_w.borrow_mut().append(&bytes)` never
runs and `crate::replication::propagate_shard_bytes(...)` never runs. So:
- `JSON.SET`/`DEL`/`NUMINCRBY`/`NUMMULTBY`/`STRAPPEND`/`ARRAPPEND`/`ARRPOP`/`TOGGLE`/`CLEAR` are
  **never written to the AOF file** — an `appendonly yes` server that crashes and replays its AOF
  will **not** recover any JSON mutation (only the RDB-snapshot state the AOF was based on).
- They are **never incrementally propagated to connected replicas** — a replica only has the JSON
  state captured at its last full resync (§8.1); every JSON write issued on the primary after that
  moment silently never reaches it. (The same connected-replicas check and `DIRTY_CHANGES` counter
  do still get updated — monitoring/`WATCH` semantics for JSON keys are otherwise normal; it is
  specifically the wire-encoding step that is missing.)

**Net persistence picture**: JSON documents are durable across `SAVE`/`BGSAVE`/restart and across
a replica's *initial* full sync, but are **not** covered by continuous AOF durability or
continuous replica propagation — a gap that sits between "fully persistent" and "ephemeral,"
and is easy to miss because the RDB snapshot behavior alone looks like full persistence.

---

## 9. RediSearch auto-indexing hook

`JSON.SET` (on any successful write, any path — not limited to root `$`) and `JSON.DEL` (on any
successful deletion) call both `db.index_json_document_local(&k_str, &doc)` (shard-local
`search_indices`, `src/shard.rs:773-792`) and `crate::search::index_json_document_hook(&k_str, &doc)`
(global `SEARCH_INDICES` registry, `src/search.rs:1240-1260`). Both walk every registered index
whose `schema.on_type` is `"JSON"` (case-insensitively), skip it if the key doesn't match any of
the index's declared key prefixes, then call `extract_json_fields(schema, root)` and
`idx.add_document(key, extracted_fields, extracted_vectors)`. `JSON.DEL` additionally calls
`db.delete_document_local`/`crate::search::delete_document_hook` when the delete removed the
*entire* document (key no longer present in `json_store` after the call) rather than a subtree.
The six incremental mutators (`NUMINCRBY`/`NUMMULTBY`/`STRAPPEND`/`ARRAPPEND`/`ARRPOP`/`TOGGLE`/
`CLEAR`) only call the global hook, not the shard-local one — see §5 for why. Field-extraction
details (flattening rules, vector-field handling) are Component 09's territory
(`docs/internal/09_redisearch.md`); this section only documents which JSON write paths trigger it.

---

## 10. Known bugs, gaps, and gotchas (verified against current `src/json.rs`/`connection.rs`/`aof.rs`)

1. **No recursive descent, despite the misleading in-code comment** (§3.2) — `$..field` silently
   degrades to `$.field` (single-level lookup), it does not search every nesting depth. No filter
   expressions (`?(@.price < N)`) either — unsupported syntax matches nothing, never errors.
2. **Auto-vivification silently destroys wrong-typed intermediates** on `JSON.SET` (§3.4) — no
   type-check error, the existing value at a parent segment is overwritten with an empty
   container if it isn't already the right container type.
3. **Read-only introspection commands only look at the first JSONPath match** (`TYPE`, `STRLEN`,
   `ARRLEN`, `OBJKEYS`, `OBJLEN`) while the numeric/string/array/bool mutators act on *every*
   match — a wildcard path behaves inconsistently depending on which command you issue against it.
   `JSON.ARRPOP` is a mutator that nonetheless only touches the first match.
4. **Partial mutation on type-mismatch mid-loop**: `NUMINCRBY`/`NUMMULTBY`/`STRAPPEND`/
   `ARRAPPEND`/`TOGGLE` error out on hitting a wrong-typed node, but matches processed earlier in
   the same call have already been mutated in place — there's no pre-validation pass or rollback.
5. **`JSON.GET`'s raw-vs-array-wrapped response depends on the literal path string** containing
   `*`/`:`, not purely on match count (§4.3) — `$.items[*]` with one match still returns `[value]`,
   not `value`.
6. **AOF/replication propagation is completely absent for every `Json*` command** (§8.2) — this is
   independent of (and worse than) the RDB-snapshot persistence story in §8.1; both facts need to
   be understood together to get an accurate picture of JSON durability.
7. **`JSON.TOGGLE`'s path argument is mandatory** (no default-to-root), diverging from the
   optional-path pattern every other read-modify command in this file uses.
8. **`JSON.ARRINSERT`, `JSON.ARRTRIM`, `JSON.MERGE`, `JSON.DEBUG`, `JSON.RESP` do not exist** in
   any form — not in the `Command` enum, not parsed, not implemented. Sending them is an unknown-
   command error, with no partial/stub behavior to be aware of.
9. **Two near-duplicate traversal implementations** (`query_json_path`/`query_json_path_mut`,
   §3.3) that must be kept manually in sync on any future JSONPath semantic change.
10. **Full-document clone on every successful `JSON.SET`/`JSON.DEL`** (§5), solely to satisfy the
    borrow checker around the shard-local search-index update — the six incremental mutators avoid
    this cost since they skip the shard-local index.

---

## How to Verify Changes

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --lib -- --test-threads=1      # includes src/json.rs's 3 in-module tests
```

Relevant existing tests: `src/json.rs::tests::test_json_crud_and_jsonpath` (888-954),
`test_json_wildcards_and_slices` (957-974), `test_json_nummultby_multi_match` (977-990 — the
regression test proving §4.2's fix). JSONPath parsing also has fuzz coverage added by commit
`f9b879c` ("add adversarial fuzz testing for RESP, JSONPath, and search query parsers").
