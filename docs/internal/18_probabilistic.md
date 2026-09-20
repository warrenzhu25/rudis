# Component 18: Probabilistic Data Structures (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/probabilistic.rs`  
> **High-Level Design Spec**: [`docs/design/18_probabilistic.md`](../design/18_probabilistic.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/probabilistic.rs` | Core implementation and logic | Primary data structures and algorithms |

---

### 2. Component Architecture & Data Structures

```
BF.RESERVE myfilter 0.01 1000        CF.ADD mycuckoo item
        │                                     │
        ▼                                     ▼
BloomFilter::new(1000, 0.01)          CuckooFilter::add("item")
  m,k derived from formula                    │
        │                          fingerprint(item) -> u16
        ▼                          indices(item, fp) -> (i1, i2)
ProbabilisticStore                  try buckets[i1]/[i2], else
  .bloom_filters["myfilter"]        cuckoo-kick up to 500 times
```

---

### Real data structures (verbatim from `src/probabilistic.rs`)

```rust
pub struct BloomFilter { pub capacity: usize, pub error_rate: f64, pub num_bits: usize,
                          pub num_hashes: usize, pub count: usize, pub bits: Vec<u64> }

pub struct CuckooFilter { pub capacity: usize, pub num_buckets: usize, pub count: usize,
                           pub buckets: Vec<[u16; 4]> }   // BUCKET_SIZE = 4

pub struct CountMinSketch { pub width: usize, pub depth: usize, pub total_count: u64,
                             pub table: Vec<Vec<u64>> }

pub struct TopK { pub k: usize, pub items: HashMap<Bytes, u64> }   // Space-Saving

pub struct ProbabilisticStore {
    pub bloom_filters: HashMap<Bytes, BloomFilter>,
    pub cuckoo_filters: HashMap<Bytes, CuckooFilter>,
    pub cms_sketches: HashMap<Bytes, CountMinSketch>,
    pub topk_trackers: HashMap<Bytes, TopK>,
}
```

Each structure type gets its own separate `HashMap` inside `ProbabilisticStore` — a key used
for a Bloom filter and a key of the same name used for a Cuckoo filter would be two completely
independent entries (in different maps), not a naming collision, since the command layer
(`connection.rs`) dispatches to the right map by command family (`BF.*` vs `CF.*` vs...), not
by inspecting what's already stored under that key.

---

### 3. Execution Algorithms & Code Logic

#### 3.1 Bloom filter: bit array indexed by `h1 + i*h2 mod num_bits`

```rust
pub fn add(&mut self, item: &[u8]) -> bool {
    let (h1, h2) = double_hash(item);
    let mut was_present = true;
    for i in 0..self.num_hashes {
        let bit = (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) % self.num_bits;
        if !self.get_bit(bit) { was_present = false; self.set_bit(bit); }
    }
    if !was_present { self.count += 1; true } else { false }
}
```

The classic Kirsch-Mitzenmacher trick: instead of computing `num_hashes` independent hash
functions, only two real hashes (`h1`, `h2`) are computed, and the `i`-th "hash" is derived
cheaply as `h1 + i*h2` — mathematically indistinguishable from independent hashing for Bloom
filter purposes, and far cheaper than running up to 30 real hash functions per `add`/`contains`
call. `add`'s return value (and the `count` increment) reflect whether the item was *probably
already present* (all bits already set) before this call — a real, if approximate,
already-seen signal, not just "operation succeeded."

#### 3.2 Cuckoo filter: two candidate buckets via XOR, eviction via random kicks

```rust
fn indices(&self, item: &[u8], fp: u16) -> (usize, usize) {
    let h = fnv1a_hash(item, SEED) as usize;
    let i1 = h % self.num_buckets;
    let i2 = (i1 ^ fnv1a_hash(&fp.to_le_bytes(), OTHER_SEED) as usize) % self.num_buckets;
    (i1, i2)
}
```

This is the standard partial-key cuckoo hashing trick: `i2` is derived from `i1` XORed with a
hash of the *fingerprint itself* (`alt_index`, §below), which is what makes `alt_index(alt_index(i,
fp), fp) == i` — applying the same XOR twice cancels out — so an item's alternate bucket can
always be recomputed from its current bucket and fingerprint alone, without needing to
re-hash the original item during a kick chain. The kick loop swaps a random existing
fingerprint out of a full bucket, relocates it to its own alternate bucket, and repeats up to
`MAX_KICKS = 500` times before giving up with `Err("ERR Cuckoo filter is full")` — real
cuckoo-hashing eviction, not a stub.

#### 3.3 Count-Min Sketch: `depth` independent hash rows, `min` across them as the estimate

```rust
pub fn incr_by(&mut self, item: &[u8], delta: u64) -> u64 {
    let (h1, h2) = double_hash(item);
    let mut min_val = u64::MAX;
    for r in 0..self.depth {
        let col = (h1.wrapping_add((r as u64).wrapping_mul(h2)) as usize) % self.width;
        self.table[r][col] = self.table[r][col].saturating_add(delta);
        min_val = min_val.min(self.table[r][col]);
    }
    self.total_count = self.total_count.saturating_add(delta);
    min_val
}
```

Same Kirsch-Mitzenmacher double-hash reuse as the Bloom filter (§3.1) to derive `depth`
independent-enough row hashes from two real hash computations. Taking the **minimum** across
rows after incrementing is the standard Count-Min Sketch estimator: any single row can only
*overestimate* a true count (due to hash collisions with other items sharing that row's
column), so the minimum across independent rows is the tightest available overestimate.
`CMS.INITBYPROB`'s width/depth derivation (`from_prob`) uses the textbook formulas
$w = \lceil e/\epsilon \rceil$, $d = \lceil \ln(1/(1-\delta)) \rceil$.

`incr_by` is the **standard (non-conservative) update rule**: every one of the `depth` row
counters is unconditionally `saturating_add`-ed by `delta` on every call
(`self.table[r][col] = self.table[r][col].saturating_add(delta)`), regardless of what the
running minimum across rows is. This is a real, deliberate distinction from the
*conservative update* variant of Count-Min Sketch (which only raises a row's counter up to
`max(current, new_min_estimate)`, skipping rows whose counter is already at or above the
post-update minimum) — conservative update tightens the sketch's over-estimation bound at the
cost of extra per-row comparisons on every increment; this implementation does not do that
extra work and accepts the correspondingly looser (but still correct, still one-sided)
over-estimation bound of the textbook algorithm.

#### 3.4 Top-K: Space-Saving eviction of the current minimum

```rust
pub fn add(&mut self, item: Bytes, increment: u64) -> Option<Bytes> {
    if let Some(count) = self.items.get_mut(&item) { *count += increment; return None; }
    if self.items.len() < self.k { self.items.insert(item, increment); return None; }
    // find (Bytes, u64) with minimum count, evict it, insert new item with min_val + increment
}
```

Finding the minimum-count tracked item is a **linear scan over all `k` tracked items** on
every eviction (`for (k, &v) in &self.items { if v < min_val { ... } }`) — fine for the small
`k` values `TOPK.RESERVE` is realistically used with, but not the heap-based O(log k) eviction
a larger-scale Space-Saving implementation would use.

---

### 4. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): dispatches every `BF.*`/`CF.*`/`CMS.*`/`TOPK.*`
  command through the shared `target_shard_of_cmd`/local-vs-`execute_remote` fork (§1),
  identical in shape to how `Json*`/`Geo*` commands are routed (Components 16/17). The command
  handlers themselves live in `src/table.rs` (e.g. `BF.RESERVE`'s handler inserts directly into
  `db.probabilistic_store.bloom_filters`), not in `probabilistic.rs`, which contains only the
  data structures and their pure algorithms.
- **`src/resp.rs`** (Component 03): parses each command family's arguments (e.g.
  `BF.RESERVE`'s error-rate/capacity, `CMS.INITBYPROB`'s error/confidence, `TOPK.RESERVE`'s
  `k`) into the matching `Command::Bf*`/`Cf*`/`Cms*`/`Topk*` variants.
- **`src/table.rs`**: none of these four structures are `RudisValue` variants — they live
  entirely in `ShardDb.probabilistic_store` (a `ProbabilisticStore`, a separate per-shard field
  alongside `table: RudisTable`), not inside the main keyspace `RudisTable` itself.
- **RDB persistence: real, verified.** `ShardDb::save_extended_rdb_chunk` (`src/shard.rs`)
  serializes every entry of `bloom_filters` (type tag `8`), `cuckoo_filters` (tag `10`),
  `cms_sketches` (tag `11`), and `topk_trackers` (tag `12`) into the RDB chunk stream —
  capacity/error-rate/bit-array for Bloom, bucket table for Cuckoo, width/depth/table for CMS,
  and the full `k`/item-count map for Top-K — and the matching branch in the RDB load path
  (`src/shard.rs`) reconstructs each structure from those bytes and re-inserts it into
  `probabilistic_store` on startup. A Bloom/Cuckoo/CMS/Top-K key survives `SAVE`/restart exactly
  like an ordinary key; there is no persistence gap for this subsystem.

---

### 5. Future Improvements

- **Low — replace Top-K's O(k) linear-scan eviction with a min-heap for O(log k) (§3.4)** — only matters if `TOPK.RESERVE` is used with a large `k`; negligible at the small-k values this structure is typically used for.
- **Low — implement conservative update for the Count-Min Sketch (§3.3)** — the current `incr_by` always updates all `depth` rows unconditionally; switching to conservative update (only raising a row's counter up to the post-increment minimum) would tighten the sketch's over-estimation bound at a small extra per-increment cost, a well-known refinement of the base algorithm this implementation does not currently apply.
- **Low — support counting Bloom filters or scalable/auto-expanding variants** if `BF.INSERT ... EXPANSION`-style auto-growth or deletion-capable Bloom semantics become a compatibility target — today, over-inserting a fixed-capacity Bloom filter just silently raises its real false-positive rate above the configured target with no signal to the caller.
- **Low — document the false-positive-rate implications of Cuckoo filter fingerprint collisions explicitly** (§3.2) — `contains` can return a false positive if two different items hash to the same 16-bit fingerprint in the same bucket, same as any Cuckoo filter design; not a bug, but worth stating plainly given the structure's "no false negatives" framing can otherwise be read as "always exact."

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Cuckoo filters use 4-slot bucket tables with fingerprint-based partial-key cuckoo hashing (§3.2); `MAX_KICKS = 500` bounds the eviction chain before `CF.ADD` fails with `"ERR Cuckoo filter is full"`.
* **Gotcha 2**: The Count-Min Sketch uses the standard (non-conservative) update rule — every row is unconditionally incremented on `incr_by`, not just the rows needed to raise the minimum (§3.3).
* **Gotcha 3**: Top-K's Space-Saving algorithm gives O(1) updates only for items already tracked; once at capacity, admitting a new distinct item costs an O(k) linear scan to find the current minimum to evict (§3.4).

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
