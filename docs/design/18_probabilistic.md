# Component 18: Probabilistic Data Structures (Design)

> **Source Files**: `src/probabilistic.rs`


---

### 1. Architectural Purpose & Scope

`src/probabilistic.rs` implements four independent approximate-membership/frequency data
structures — a **Bloom Filter**, a **Cuckoo Filter**, a **Count-Min Sketch**, and a **Top-K
frequency tracker** (Space-Saving algorithm) — exposed via RedisBloom-compatible commands
(`BF.*`, `CF.*`, `CMS.*`, `TOPK.*`). Each structure type has its own per-key map inside
`ProbabilisticStore`, which lives in `ShardDb` alongside `vector_indexes`/`crdt_store`/
`json_store`. Like `src/json.rs` (Component 16) and `src/geo.rs` (Component 17) and unlike
`src/vector.rs` (Component 08), every single-key command here is genuinely routed per-key
across shards — confirmed directly in `connection.rs`: `BfAdd`/`CfAdd`/`CmsIncrby`/`TopkAdd`/
etc. all appear in the same `target_shard_of_cmd`/local-vs-`execute_remote` dispatch arm as
ordinary keyed commands.

---

### 2. Key Invariants & Concurrency Constraints

1. **A single custom hash function underlies all four structures.** `fnv1a_hash` (a
   seeded 64-bit FNV-1a) and `double_hash` (two independent FNV-1a calls with different fixed
   seeds, used for Kirsch-Mitzenmacher double-hashing) are shared by the Bloom filter, Cuckoo
   filter, and Count-Min Sketch — there is no per-structure hash family, and no cryptographic
   hash anywhere in this file (not a concern for these structures' intended use, unlike an
   auth-adjacent context).
2. **Bloom filter sizing follows the standard formulas, computed once at creation.**
   `BloomFilter::new(capacity, error_rate)` derives bit-array size via
   $m = \lceil -n \ln(p) / (\ln 2)^2 \rceil$ and hash count via $k = \text{round}((m/n)\ln 2)$,
   clamped to `[1, 30]` hashes — real, textbook Bloom filter parameter derivation, not
   hardcoded constants.
3. **The Cuckoo filter is a real, complete implementation including eviction ("cuckoo
   kicks").** `add` tries both candidate buckets first, and only falls back to the
   randomized-kick relocation loop (`MAX_KICKS = 500`) if both are full — a genuine cuckoo
   hashing insert, not a simplified always-fails-when-full variant. `delete` is also real
   (removes a matching fingerprint from either candidate bucket), which is one of the
   Cuckoo filter's actual advantages over a Bloom filter (Bloom filters can't support
   deletion at all without a counting variant, which isn't implemented here).
4. **The Top-K tracker is a real Space-Saving algorithm, not an exact top-K.** Once at
   capacity, `TopK::add` evicts the *minimum-count* tracked item and gives the new item that
   evicted item's count plus the increment — the standard Space-Saving guarantee (every
   tracked count is an overestimate, bounded by the true frequency of whatever was evicted
   last), not an exact frequency count.
5. **No structure ever shrinks or is auto-resized.** A Bloom/Cuckoo filter's bit array or
   bucket count is fixed at creation time (`BF.RESERVE`/`CF.RESERVE`'s capacity argument); a
   Count-Min Sketch's width/depth are likewise fixed at `CMS.INITBYDIM`/`INITBYPROB` time.
   There is no `BF.INSERT ... EXPANSION` auto-scaling behavior — once a filter created with a
   given capacity is over-inserted, its false-positive rate silently degrades rather than the
   structure growing.

---

### 3. Performance Characteristics

- **Bloom/Cuckoo `add`/`contains` are O(num_hashes) / O(1)** respectively — a Bloom filter
  check costs up to 30 bit-array probes (bounded, per §2.2's clamp), a Cuckoo filter check is
  two fixed-size (4-slot) bucket scans regardless of fill level.
- **Cuckoo insertion degrades under high load factor** — the `MAX_KICKS = 500` eviction chain
  only triggers once both candidate buckets are full, and a filter approaching its rated
  capacity will trigger it increasingly often before either succeeding or returning
  `"ERR Cuckoo filter is full"` — a real, expected cuckoo-hashing characteristic, not a bug.
- **Count-Min Sketch `incr_by`/`query` are O(depth)**, independent of how many distinct items
  have been tracked — the whole point of a fixed-size sketch over an exact per-item counter map.
- **Top-K's eviction is O(k) per new distinct item once at capacity** (§4.4) — negligible at
  small `k`, would matter if `TOPK.RESERVE` were ever used with a very large `k`.

---
