# Component 18: Probabilistic Data Structures (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/probabilistic.rs`, `src/hll.rs`
> **High-Level Design Spec**: [`docs/design/18_probabilistic.md`](../design/18_probabilistic.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/probabilistic.rs` (417 lines) | Bloom/Cuckoo/CMS/Top-K data structures and algorithms | `BloomFilter`, `CuckooFilter`, `CountMinSketch`, `TopK`, `ProbabilisticStore`, `fnv1a_hash`, `double_hash` |
| `src/hll.rs` (348 lines) | Standalone Redis-compatible HyperLogLog codec (dense + sparse) | `murmur_hash_64a`, `hll_pat_len`, `hll_validate`, `hll_decode_registers`, `hll_encode_sparse`, `hll_encode_dense`, `hll_create_from_regs`, `hll_compute_card`, `hll_count`, `hll_add`, `hll_merge` |

Both modules are declared at crate root (`src/lib.rs`: `pub mod probabilistic;` / `pub mod hll;`) and are invoked almost exclusively from `src/table.rs` (per-shard command handlers) and `src/connection.rs` (cross-shard fan-out for multi-key `PFCOUNT`/`PFMERGE` and RESP response encoding). `src/hll.rs` is a **brand-new standalone module** (added in commit `b85b6a7`, "implement Redis-compatible dense/sparse HyperLogLog") — it did not exist in the prior HLL implementation, which stored decoded registers directly as a dedicated `RudisValue::HyperLogLog` enum variant. That variant still exists in `src/table.rs` (`RudisValue::HyperLogLog(Box<[u8; 16384]>)`, line 1424) but is now **legacy-only**, reachable only via `RESTORE` of an old `DUMP` payload tagged type `5` (§5.4). Every `PFADD`-created key today is a plain `RudisValue::String` holding the real Redis `"HYLL"` byte format produced by `src/hll.rs`.

---

### 2. Component Architecture & Data Structures

```
BF.RESERVE myfilter 0.01 1000        CF.ADD mycuckoo item         PFADD hll a b c
        │                                     │                           │
        ▼                                     ▼                           ▼
BloomFilter::new(1000, 0.01)          CuckooFilter::add("item")   db.pfadd(key, elems)
  m,k derived from formula                    │                 RudisValue::String holding
        │                          fingerprint(item) -> u16      "HYLL"-magic byte blob
        ▼                          indices(item, fp) -> (i1, i2)         │
ProbabilisticStore                  try buckets[i1]/[i2], else    hll::hll_add() ->
  .bloom_filters["myfilter"]        cuckoo-kick up to 500 times   decode all 16384 regs,
                                                                   bump rank, re-encode
                                                                   sparse (or dense if it
                                                                   no longer fits/exceeds
                                                                   HLL_SPARSE_MAX_BYTES)
```

The Bloom/Cuckoo/CMS/Top-K structures live in a dedicated per-shard `ProbabilisticStore` field (outside the main keyspace). HyperLogLog, by contrast, is **not** a separate store — a `PFADD`ed key is an ordinary keyspace entry (`RudisValue::String`) whose byte payload happens to follow the Redis HLL wire format, so it is fully subject to `EXPIRE`, `DEL`, `RENAME`, `DUMP`/`RESTORE`, replication, and generic string commands (`APPEND`, `GETRANGE`, `SETRANGE`, `STRLEN`) like any other string key.

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

### HyperLogLog byte format (`src/hll.rs`, no dedicated Rust struct — a raw `Vec<u8>`/`Bytes` wire format)

```rust
pub const HLL_HDR_SIZE: usize = 16;          // 4("HYLL") + 1(encoding) + 3(unused) + 8(cached card)
pub const HLL_DENSE: u8 = 0;
pub const HLL_SPARSE: u8 = 1;
pub const HLL_REGISTERS: usize = 16384;      // 2^14 registers
pub const HLL_DENSE_SIZE: usize = HLL_HDR_SIZE + 12288;  // 16304 bytes total (verified)
pub const HLL_SPARSE_MAX_BYTES: usize = 3000;            // promote sparse->dense past this
pub const ERR_WRONGTYPE: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";
pub const ERR_INVALIDOBJ: &str = "INVALIDOBJ Corrupted HLL object detected";
```

16384 registers × 6 bits/register = 98304 bits = 12288 bytes exactly — the dense payload has
no padding (`src/hll.rs:9`). Header layout (byte offsets, little-endian, matches real Redis'
`hllhdr`):

| Bytes | Field | Notes |
| :--- | :--- | :--- |
| `0..4` | magic `"HYLL"` | `hll_validate` rejects anything else as `ERR_WRONGTYPE` |
| `4` | encoding | `0` = dense, `1` = sparse |
| `5..8` | unused | always written as `0,0,0` |
| `8..16` | cached cardinality (u64 LE) | bit 63 (top bit of byte 15) = 0 means "cache valid", 1 means "recompute" |
| `16..` | payload | 12288 raw packed-register bytes (dense) or variable-length opcode stream (sparse) |

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

Sizing (`BloomFilter::new`, `src/probabilistic.rs:38-60`, re-verified unchanged): `capacity`
is floored at 1, `error_rate` clamped to `[0.00001, 0.5]`. Bit-array size is the textbook
`m = ceil(-n * ln(p) / ln(2)^2)`, floored at 64 bits; hash count is `k = round((m/n) * ln(2))`,
clamped to `[1, 30]`. Backing storage is `Vec<u64>` sized `m.div_ceil(64)` words — e.g. the
auto-create default (`capacity=1000, error_rate=0.01`, used by `BF.ADD`/`BF.MADD` on a
non-existent key) yields `m ≈ 9586` bits (~1199 bytes, 150 × `u64`) and `k = 7` hashes.

#### 3.2 Cuckoo filter: two candidate buckets via XOR, eviction via fingerprint-derived (not RNG) kicks

```rust
fn indices(&self, item: &[u8], fp: u16) -> (usize, usize) {
    let h = fnv1a_hash(item, SEED) as usize;
    let i1 = h % self.num_buckets;
    let i2 = (i1 ^ fnv1a_hash(&fp.to_le_bytes(), OTHER_SEED) as usize) % self.num_buckets;
    (i1, i2)
}
```

This is the standard partial-key cuckoo hashing trick: `i2` is derived from `i1` XORed with a
hash of the *fingerprint itself* (`alt_index`, `src/probabilistic.rs:151-154`), which is what
makes `alt_index(alt_index(i, fp), fp) == i` — applying the same XOR twice cancels out — so an
item's alternate bucket can always be recomputed from its current bucket and fingerprint alone,
without needing to re-hash the original item during a kick chain.

**Re-verified nuance**: the kick chain is **fully deterministic given the fingerprint**, not
RNG-driven, despite informally being called "random kicks." The starting bucket is chosen by
fingerprint parity (`src/probabilistic.rs:177-181`: `cur_i = if (fp as usize).is_multiple_of(2)
{ i1 } else { i2 }`), and the victim slot inside a full bucket is `slot_idx = (cur_fp as usize)
% BUCKET_SIZE` (line 185) — no `rand()`/RNG call anywhere in the eviction loop. Two `CF.ADD`
calls on the same fingerprint value, against the same filter state, always produce the exact
same kick sequence. The loop swaps the victim fingerprint out, relocates `cur_i` to the
victim's alternate bucket, and repeats up to `MAX_KICKS = 500` times before giving up with
`Err("ERR Cuckoo filter is full")` — real cuckoo-hashing eviction, not a stub, just not
randomized.

Sizing (`CuckooFilter::new`, line 124): `capacity` floored at 1; `num_buckets =
(capacity / BUCKET_SIZE).next_power_of_two().max(4)` — so the auto-create default
(`capacity=1000`, used by `CF.ADD`/`CF.ADDNX` on a miss) yields `num_buckets = 256`
(1000/4=250 → next power of two 256), i.e. 1024 total 2-byte fingerprint slots (2048 bytes),
reported by `CF.INFO`'s `Size` field as `num_buckets * BUCKET_SIZE(4) * 2` bytes
(`src/connection.rs:17567`). Fingerprint 0 is reserved as "empty slot" sentinel — `fingerprint()`
remaps a real hash of 0 to 1 (`src/probabilistic.rs:137-139`), so a fingerprint collision with 0
can never falsely read as occupied/empty.

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

Sizing (`src/probabilistic.rs:243-258`, re-verified unchanged): `CountMinSketch::new(width,
depth)` floors `width` at 4 and clamps `depth` to `[1, 16]`. `CMS.INITBYPROB`'s width/depth
derivation (`from_prob`) uses the textbook formulas `w = ceil(e / err)`,
`d = ceil(ln(1 / (1 - conf)))`. The auto-create default (used by `CMS.INCRBY` on a
non-existent key, `src/connection.rs:17604`) is `width=2000, depth=5` — 10000 `u64` cells
(80000 bytes).

`incr_by` is the **standard (non-conservative) update rule**: every one of the `depth` row
counters is unconditionally `saturating_add`-ed by `delta` on every call, regardless of what
the running minimum across rows is. This is a real, deliberate distinction from the
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
a larger-scale Space-Saving implementation would use. Auto-create default (`TOPK.ADD` on a
non-existent key, `src/connection.rs:17654`) is `k=50`.

**Re-verified**: `TopK::new` floors `k` at 1, and `add`'s `increment` is floored at 1
(`let inc = increment.max(1)`). `TOPK.RESERVE` in this codebase takes **only** `key` and
`topk` (`src/resp.rs:11158-11168`, exactly 3 args) — it does **not** accept the optional
`width`/`depth`/`decay` arguments real RedisBloom's `TOPK.RESERVE` supports for its internal
Count-Min sketch and decay factor; this implementation's Top-K has no internal CMS at all, it
is pure Space-Saving over a `HashMap<Bytes, u64>`.

---

## 4. HyperLogLog (`src/hll.rs`) — Dense & Sparse Codec, Full `PF*` Mechanics

### 4.1 `murmur_hash_64a` — exact MurmurHash64A port (`src/hll.rs:16-50`)

64-bit MurmurHash2 variant (`M = 0xc6a4a7935bd1e995`, `R = 47`), seeded per call. Processes
the key in 8-byte little-endian blocks; the final partial block (`len % 8` leftover bytes) is
folded in byte-by-byte via `k |= (b as u64) << (i*8)` before the final mix — a faithful,
bit-exact port of Redis' `MurmurHash64A`, required for cross-compatibility with real Redis HLL
payloads if they were ever migrated in (the hash must match exactly or register
indices/ranks would silently differ from genuine Redis-computed HLLs).

### 4.2 `hll_pat_len` — register index + rank computation (`src/hll.rs:53-60`)

```rust
pub fn hll_pat_len(ele: &[u8]) -> (usize, u8) {
    let hash = murmur_hash_64a(ele, 0xadc83b19);          // HLL_HASH_SEED, matches real Redis
    let index = (hash & 0x3FFF) as usize;                  // low 14 bits -> 1 of 16384 registers
    let pat = (hash >> 14) | (1u64 << 50);                 // remaining 50 bits + guard bit
    let count = (pat.trailing_zeros() + 1) as u8;           // rank = position of first 1-bit
    (index, count)
}
```

Standard HyperLogLog register-update primitive. The low 14 bits of the hash select the
register (`2^14 = 16384 = HLL_REGISTERS`). The remaining 50 bits are treated as a bit pattern
whose *rank* (position of the first set bit, 1-indexed) is the candidate register value; the
explicit `| (1u64 << 50)` guard bit bounds `trailing_zeros()` to at most 50, so `count` is
bounded at `51` even if every one of the 50 real bits happens to be zero (prevents an
unbounded/overflowing rank on a degenerate all-zero pattern) — this exactly matches real
Redis' `hllPatLen`.

### 4.3 Dense register packing — 6-bit fields across byte boundaries (`src/hll.rs:62-96`)

`hll_dense_get_register`/`hll_dense_set_register` read/write a register's 6-bit value by
building a 16-bit little-endian window from the register's home byte and the following byte
(`b0 | (b1 << 8)`), shifting by the bit offset within the byte (`bit % 8`, 0-7), and
masking/merging with `0x3F`. Since a 6-bit field starting at offset 7 spans bits 7-12 (2 bytes),
a 2-byte window is always sufficient — this is the standard technique for packing values that
don't align to byte boundaries, equivalent to real Redis' `HLL_DENSE_GET_REGISTER`/
`HLL_DENSE_SET_REGISTER` macros.

### 4.4 `hll_validate` — structural corruption checks (`src/hll.rs:98-149`)

Returns `Ok(encoding_byte)` or an error. Checks, in order:
1. `bytes.len() >= HLL_HDR_SIZE` (16), else `ERR_WRONGTYPE`.
2. `bytes[0..4] == b"HYLL"`, else `ERR_WRONGTYPE`.
3. If `encoding == HLL_DENSE`: `bytes.len() == HLL_DENSE_SIZE` (16304) exactly, else
   `ERR_WRONGTYPE`.
4. If `encoding == HLL_SPARSE`: walks the opcode stream (same 3-way VAL/XZERO/ZERO decode as
   §4.6 below), accumulating a running register count via `checked_add` (overflow →
   `ERR_INVALIDOBJ`, not a panic) and rejecting as soon as the running count exceeds
   `HLL_REGISTERS` (`ERR_INVALIDOBJ`). After the full walk, the running count must equal
   `HLL_REGISTERS` **exactly** — an opcode stream that covers fewer or more than 16384
   registers is `ERR_INVALIDOBJ`.
5. Any other encoding byte → `ERR_WRONGTYPE`.

This is the only place corrupted/truncated/adversarial HLL payloads are caught; every other
`hll_*` function assumes its input already passed `hll_validate` (several call it internally
first — e.g. `hll_decode_registers`, line 152).

### 4.5 Sparse opcode format (3 opcodes, re-verified bit-exact against real Redis)

Decoded in `hll_decode_registers` (`src/hll.rs:159-181`) and `hll_validate` (§4.4), encoded in
`hll_encode_sparse` (§4.6):

| Opcode | Top bits of byte | Layout | Meaning |
| :--- | :--- | :--- | :--- |
| **VAL** | `1` (bit 7 set) | `1vvvvvll` (1 byte) | `len = (ll)+1` (1-4) consecutive registers set to `val = (vvvvv)+1` (1-32) |
| **XZERO** | `01` (bit 6 set, bit 7 clear) | `01xxxxxx yyyyyyyy` (2 bytes) | `len = (((xxxxxx)<<8)\|yyyyyyyy)+1` (1-16384) consecutive registers set to 0 |
| **ZERO** | `00` (bits 6,7 clear) | `00llllll` (1 byte) | `len = (llllll)+1` (1-64) consecutive registers set to 0 |

Decode dispatch checks `b & 0x80` first (VAL), then `b & 0x40` (XZERO), else ZERO — matching
the bit layout exactly. A register value of 0 that is covered by neither an VAL run containing
it nor explicitly means the decoder just advances `reg_idx` without writing (the output array
starts fully zeroed, `[0u8; 16384]`, so ZERO/XZERO runs are no-ops against the initial state).

### 4.6 `hll_encode_sparse` — greedy opcode emission with inline dense-promotion checks (`src/hll.rs:186-225`)

```rust
pub fn hll_encode_sparse(regs: &[u8; 16384]) -> Option<Vec<u8>> {
    if regs.iter().any(|&r| r > 32) { return None; }   // VAL's 5-bit field maxes at 32
    // ... greedy run-length emission of ZERO/XZERO/VAL opcodes ...
    // returns None early if buf.len() > HLL_SPARSE_MAX_BYTES at any point
}
```

**Sparse-to-dense promotion trigger** (re-verified): encoding bails out to `None` — meaning
the caller (`hll_create_from_regs`, §4.7) falls back to dense — under exactly two conditions:
(a) any single register value exceeds `32` (the VAL opcode's 5-bit value field, biased by +1,
tops out at 32), or (b) the growing sparse buffer exceeds `HLL_SPARSE_MAX_BYTES = 3000` bytes
at any point during emission. Both match real Redis' sparse representation limits
(`HLL_SPARSE_VAL_MAX_VALUE = 32`, configurable `hll-sparse-max-bytes` defaulting to 3000).

For a run of identical non-zero registers, at most 4 are folded into a single VAL opcode
(`run_len < 4` loop bound, line 214) since VAL's length field is only 2 bits. For zero runs,
runs of ≤64 become a single ZERO opcode; longer runs are chunked into ≤16384-length XZERO
opcodes in a `while run_len > 0` loop (lines 199-210).

### 4.7 `hll_create_from_regs` — register array → full HYLL byte string (`src/hll.rs:248-272`)

Always tries sparse first (`hll_encode_sparse`); on `None`, falls back to `hll_encode_dense`
(unconditional success, fixed 12288-byte output, §4.3). Builds the 16-byte header (`"HYLL"` +
encoding byte + 3 zero bytes) then the cardinality cache field: if `card_cache: Some(c)` is
passed, writes `c` with bit 63 of the last byte forced to `0` (cache **valid**); if `None`,
writes all-zero bytes with the last byte's top bit forced to `1` (cache **invalid** —
`PFCOUNT` must recompute). Every call site that actually mutates registers
(`hll_add`, the manual merge loops in `table.rs`/`connection.rs`) passes `None`, correctly
invalidating the cardinality cache on every structural change, matching real Redis' "any
PFADD that changes a register dirties the cached cardinality" rule.

### 4.8 `hll_compute_card` — classic Flajolet HLL estimator (`src/hll.rs:274-296`)

```rust
const M: f64 = 16384.0;
const ALPHA: f64 = 0.7213475204444817;   // HyperLogLog bias-correction constant for m=16384
let raw_estimate = ALPHA * M * M / sum;   // sum = Σ 2^-register[i]
if raw_estimate <= 2.5 * M && zeros > 0 {
    (M * (M / zeros as f64).ln()).round() as u64          // linear counting (small cardinalities)
} else if raw_estimate <= (1.0/30.0) * 4294967296.0 {
    raw_estimate.round() as u64                            // raw HLL estimate (mid range)
} else {
    (-4294967296.0 * (1.0 - raw_estimate / 4294967296.0).ln()).round() as u64   // large-range correction
}
```

**Re-verified finding**: this is the **original/classic Flajolet et al. (2007) HyperLogLog
estimator** — raw harmonic-mean estimate with the standard `α·m²/Σ2^-M[i]` formula, linear
counting for the small-cardinality regime (`raw_estimate ≤ 2.5m` and at least one zero
register), and the `-2^32·ln(1 - E/2^32)` large-range correction near the 32-bit counter
overflow boundary (`raw_estimate > m/30 · 2^32`... here checked as `≤ (1/30)·2^32`, i.e. the
correction applies above that threshold). **This is not the "new" histogram/τ-σ
bias-corrected cardinality algorithm Redis adopted in Redis 4.0+** (which replaces linear
counting and the large-range correction with a different, empirically bias-corrected
estimator based on register-value histograms). For the same register state, this
implementation's `PFCOUNT` will produce a numerically different (though still ~0.81%
std-error-class) estimate than a real modern Redis server — a genuine behavioral divergence
worth flagging for anyone cross-validating cardinalities against real Redis.

### 4.9 `hll_count` — cache-aware cardinality read (`src/hll.rs:298-312`)

Validates the blob, reads the 8-byte cache field at `bytes[8..16]`; if the top bit of the last
byte is `0`, returns the cached `u64` directly (no decode). Otherwise decodes all 16384
registers, computes cardinality via §4.8, **writes the freshly-computed value back into the
caller's byte buffer** (clearing the invalid bit) — so repeated `PFCOUNT` calls on a key whose
registers haven't changed since the last count are O(1) after the first call, not O(16384).

### 4.10 `hll_add` / `hll_merge` — mutation primitives (`src/hll.rs:314-348`)

```rust
pub fn hll_add(bytes: &mut Vec<u8>, elements: &[Bytes]) -> Result<bool, &'static str> {
    let mut regs = hll_decode_registers(bytes)?;           // full 16384-byte decode
    let mut modified = false;
    for elem in elements {
        let (index, count) = hll_pat_len(elem.as_ref());
        if count > regs[index] { regs[index] = count; modified = true; }
    }
    if modified { *bytes = hll_create_from_regs(&regs, None); }   // full re-encode
    Ok(modified)
}
```

**Re-verified cost characteristic**: every `PFADD` call that changes at least one register
pays a **full decode of all 16384 registers plus a full re-encode** (greedy sparse-opcode
re-emission or dense repack) of the entire object, regardless of how many elements were added
or how small the sparse representation currently is. Real Redis instead patches the sparse
opcode stream in place for the common case (`hllSparseSet`'s in-place splice), only falling
back to full decode/re-encode when a promotion to dense is actually triggered. This
implementation always pays the O(16384) cost — correct, but not the constant-factor-optimized
path real Redis uses for sparse HLLs under light load.

`hll_merge(dest, sources)` decodes `dest` (or starts from all-zero registers if `dest` is
empty) and each source, taking the per-register max across all of them, then re-encodes via
`hll_create_from_regs` — the textbook HLL merge (registers are monotonic under max, so merging
two sketches is exact, not approximate, at the register level). **Dead-code finding**:
`hll_merge` is never called from anywhere else in the crate (`rg` across `src/*.rs` finds only
its own definition) — `table.rs::pfmerge` (§4.12) and `connection.rs`'s cross-shard `PFMERGE`
path (§4.13) both reimplement the identical decode/max/re-encode logic manually inline instead
of calling this function. Functionally harmless (the logic is duplicated correctly in both
places) but a maintenance wart: a future bug fix to the merge algorithm would need to be
applied in three places, not one.

### 4.11 `PFADD` (`Db::pfadd`, `src/table.rs:11123-11181`)

1. Look up `key`. If absent, create via `hll_create_sparse_empty()` (§4.11a) and immediately
   `hll_add` the elements into it (or just store the empty sparse HLL if `elements` is empty),
   then `self.set(...)`. Always returns `true` (key creation counts as "cardinality may have
   changed").
2. If present and `RudisValue::String`: `hll_validate` first (propagates `ERR_WRONGTYPE`/
   `ERR_INVALIDOBJ` on a non-HLL or corrupted string) — if `elements` is empty, short-circuits
   `Ok(false)` without even decoding. Otherwise decode-mutate-reencode via `hll_add` (§4.10) and
   write back only if `updated`.
3. If present and legacy `RudisValue::HyperLogLog(regs)` (§5.4): mutates the boxed register
   array **in place** via the same `hll_pat_len`/rank-compare logic, without ever constructing
   a byte-format blob — no `hll_add`/`hll_create_from_regs` call at all for this branch.
4. Any other `RudisValue` variant → `WRONGTYPE Operation against a key holding the wrong kind
   of value` (the *generic* WRONGTYPE message, not the HLL-specific one — that one only comes
   from `hll_validate`).

`hll_create_sparse_empty()` (`src/hll.rs:237-246`): builds an 18-byte blob — 16-byte header
(`"HYLL"`, encoding=sparse, cache=0/valid since cardinality of an empty HLL is provably 0) plus
a single 2-byte **XZERO** opcode (`0x7F, 0xFF` → `len = (((0x7F & 0x3F)<<8)|0xFF)+1 = 16384`)
covering all 16384 registers in one run. This is the minimum-size valid sparse HLL.

### 4.12 `PFCOUNT` (`Db::pfcount`, `src/table.rs:11183-11253`)

- **Zero keys**: `Ok(0)` immediately.
- **Single key**: looks up the entry; `RudisValue::String` path validates + calls `hll_count`
  (§4.9, cache-aware) and persists the possibly-updated cache byte back into the stored
  `Bytes`; legacy `RudisValue::HyperLogLog` path calls `hll_compute_card` directly on the
  boxed register array (no cache field exists for the legacy variant — every legacy-path
  `PFCOUNT` fully recomputes, there is nowhere to cache the result). Missing key → `Ok(0)`.
  Wrong type → the generic `WRONGTYPE` string.
- **Multiple keys**: builds a `[0u8; 16384]` accumulator, decodes every key's registers (via
  `hll_decode_registers`, which internally validates), takes the per-register max across all
  of them (an implicit, throwaway merge — never cached, never written back anywhere), then
  calls `hll_compute_card` once on the merged array. Missing keys are silently skipped (not an
  error); if none of the keys existed, returns `Ok(0)`.

### 4.13 `PFMERGE` (`Db::pfmerge`, `src/table.rs:11255-11347`)

Pre-validates every source **and** the destination (if it already exists) before doing any
merging — so a `WRONGTYPE`/`INVALIDOBJ` on any single source key aborts the whole operation
with nothing written, rather than partially merging. Then re-reads dest + each source a
*second* time (the validation pass and the merge pass are two separate loops over the same
keys) to build the `[0u8; 16384]` max-accumulator, and finally writes
`hll_create_from_regs(&merged, None)` (cache explicitly invalidated) into `destkey` via
`self.set(...)`. Always returns `Ok(())` / `+OK` — unlike real Redis, there's no distinct
"dest changed vs. unchanged" signal (not applicable here since `PFMERGE` has no boolean RESP
reply anyway).

### 4.14 `PFDEBUG` subcommands (parsed `src/resp.rs:6579-6614`, dispatched `src/connection.rs:15182-15226` and `src/table.rs:11349-11417`)

| Subcommand | Behavior |
| :--- | :--- |
| `PFDEBUG GETREG key` | `pfdebug_getreg` decodes (or copies, for the legacy variant) all 16384 registers and the handler writes them as a RESP array of **16384 individual integers** (`*16384\r\n` + one `:<val>\r\n` per register) — not a bulk string, an actual multi-bulk array (`src/connection.rs:15182-15193`). |
| `PFDEBUG ENCODING key` | Returns `+dense\r\n` or `+sparse\r\n` (simple string) via `hll_validate`'s returned encoding byte; legacy variant always reports `"dense"` (it's conceptually always "the whole register array materialized"). |
| `PFDEBUG TODENSE key` | Forces sparse→dense conversion in place: re-decodes, re-encodes dense (§4.3), rebuilds the header reusing the **original** cached-cardinality bytes (`hdr[8..16].copy_from_slice(&s[8..16])` — cache validity is preserved across this conversion, unlike a real register mutation) via `src/table.rs:11388-11417`. Returns `:1` if it actually converted, `:0` if the key was already dense (or is the legacy variant, which is a no-op `:0`). |
| `PFDEBUG SIMD on/off/1/0/yes` | Parsed into a `bool` (`src/resp.rs:6603-6611`) but **completely ignored** — the handler (both in `execute_local_command` and the identical arm in the cross-shard dispatcher) unconditionally replies `+enabled\r\n` regardless of the argument or any actual SIMD code path. There is no SIMD implementation anywhere in `src/hll.rs` — this is a pure RESP-compatibility stub for clients/tests that probe `PFDEBUG SIMD`. |
| `PFSELFTEST` | Unconditionally replies `+OK\r\n` — also a pure stub; no self-test logic runs. |

### 4.15 Cross-shard `PFCOUNT`/`PFMERGE` fan-out (`src/connection.rs:10374-10525`)

When a multi-key `PFCOUNT`/`PFMERGE` has keys that don't all hash to the router's current
shard: in cluster mode this is a hard `-CROSSSLOT` error (same policy as `BITOP`/generic
multi-key commands); outside cluster mode, the router fetches each key's raw value across
shards via `router.get(k)` (async, goes over the shard mailbox), calls
`crate::hll::hll_decode_registers` on each fetched blob directly (bypassing `table.rs`'s
`pfcount`/`pfmerge` entirely — this path has **no legacy `RudisValue::HyperLogLog` handling**,
since `router.get` only ever returns the byte-string view), accumulates the per-register max,
and for `PFMERGE` writes the result back with `router.set(destkey, ..., None)`. A decode error
(corrupted/non-HLL value) on any fetched key aborts the whole command with that error.

---

## 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): dispatches every `BF.*`/`CF.*`/`CMS.*`/`TOPK.*`/
  `PF*` command through the shared `target_shard_of_cmd`/local-vs-`execute_remote` fork (§1),
  identical in shape to how `Json*`/`Geo*` commands are routed (Components 16/17), **plus** a
  bespoke multi-key fan-out specifically for `PFCOUNT`/`PFMERGE` when the keys span shards
  (§4.15) — no other probabilistic command family has this cross-shard merge path, because
  Bloom/Cuckoo/CMS/Top-K commands are always single-key.
- **`src/resp.rs`** (Component 03): parses each command family's arguments (e.g.
  `BF.RESERVE`'s error-rate/capacity, `CMS.INITBYPROB`'s error/confidence, `TOPK.RESERVE`'s
  `k`, `PFADD`'s variadic elements, `PFDEBUG`'s 4 subcommands) into the matching
  `Command::Bf*`/`Cf*`/`Cms*`/`Topk*`/`Pf*` variants (`src/resp.rs:876-891` for the `Pf*`
  variants specifically). None of the `BF.RESERVE`/`CF.RESERVE`/`TOPK.RESERVE` parsers accept
  real RedisBloom's optional tuning arguments (`EXPANSION`, `NONSCALING`, `BUCKETSIZE`,
  `MAXITERATIONS`, Top-K's `width`/`depth`/`decay`) — argument counts are checked for strict
  equality against the minimal required set.
- **`src/table.rs`**: `BloomFilter`/`CuckooFilter`/`CountMinSketch`/`TopK` are **not**
  `RudisValue` variants — they live entirely in `ShardDb.probabilistic_store` (a
  `ProbabilisticStore`, a separate per-shard field alongside `table: RudisTable`), not inside
  the main keyspace `RudisTable` itself. HyperLogLog is the opposite: a `PFADD`ed key **is** a
  normal keyspace entry (`RudisValue::String`), so it participates in `EXPIRE`/`TTL`/`RENAME`/
  generic string commands; `APPEND`/`SETRANGE`/`GETRANGE`/`STRLEN` on the **legacy**
  `RudisValue::HyperLogLog` variant specifically convert it to the real byte format via
  `hll_create_from_regs` on the fly before doing the string operation (`src/table.rs:4027-4033,
  4060-4063, 4118-4121, 4152`), and `APPEND`/`SETRANGE` downgrade the entry to a plain
  `RudisValue::String` in the process (it can never become `HyperLogLog` again from a string
  operation).
- **RDB persistence (Bloom/Cuckoo/CMS/Top-K): real, verified, unchanged.**
  `ShardDb::save_extended_rdb_chunk` (`src/shard.rs:2320-2439`) serializes every entry of
  `bloom_filters` (type tag `8`), `cuckoo_filters` (tag `10`), `cms_sketches` (tag `11`), and
  `topk_trackers` (tag `12`) into the RDB chunk stream — capacity/error-rate/bit-array for
  Bloom, bucket table for Cuckoo, width/depth/table for CMS, and the full `k`/item-count map
  for Top-K — and the matching branch in the RDB load path (`src/shard.rs`, lines 2746/2879/
  2912/2947) reconstructs each structure and re-inserts it into `probabilistic_store` on
  startup. A Bloom/Cuckoo/CMS/Top-K key survives `SAVE`/restart exactly like an ordinary key.
- **RDB persistence (HyperLogLog): no special chunk type.** A `PFADD`-created HLL is a
  `RudisValue::String` under the hood, so it rides the **generic** string-key RDB path
  (`save_rdb_chunk`'s normal key/value serialization, type tag `0`) — there is no HLL-specific
  RDB tag in `save_extended_rdb_chunk`'s list, because none is needed.

### 5.4 Legacy `RudisValue::HyperLogLog` and the `DUMP`/`RESTORE` byte-format mismatch (re-verified, real finding)

`RudisValue::HyperLogLog(Box<[u8; 16384]>)` (`src/table.rs:1424`) is only ever *constructed* in
one place in the entire crate: `Db::deserialize_val_payload`'s `5 =>` arm
(`src/table.rs:12717-12725`), reached by `RESTORE` on a `DUMP` payload whose type byte is `5`.
`Db::serialize_val_payload`'s `RudisValue::HyperLogLog(regs) =>` arm (`src/table.rs:12446-
12449`) is the only place that *writes* such a payload — and it writes the raw 16384-byte
**decoded register array**, not a real `"HYLL"`-prefixed byte string. So type tag `5` in this
codebase's `DUMP` format is a self-referential legacy format: it round-trips correctly only
between this server's own `DUMP`/`RESTORE`, and does **not** match real Redis' actual
`RDB_TYPE_STRING`-wrapped HLL dump format at all.

**Confirmed real bug / incompatibility**: once a legacy `RudisValue::HyperLogLog` value exists
(via `RESTORE`), several code paths read it out as **raw, unwrapped 16384-byte register bytes**
instead of converting it to the proper `"HYLL"` byte format first:
- `Db::get_with_hash`/`get_compact_with_hash`/`write_get_resp` (`src/table.rs:2893, 2936,
  2973`) — plain `GET key` on such a key returns `Bytes::copy_from_slice(&regs[..])`, 16384 raw
  bytes with **no `"HYLL"` magic header at all**.
- Replication/`MIGRATE` propagation (`src/connection.rs:3658-3675, 10851-10870`) encodes it as
  `SET key <16384 raw register bytes>\r\n` — the exact same unwrapped payload, now propagated
  to a replica or another cluster node as an ordinary string.

The result: if a legacy HLL is `GET` by a client (or replicated/migrated) and then written back
or re-sent to any `PF*` command (locally or on the receiving replica/node), `hll_validate` will
reject it immediately with `ERR_WRONGTYPE` (`"WRONGTYPE Key is not a valid HyperLogLog string
value."`) — because the leaked bytes don't start with `"HYLL"`. In other words, a legacy HLL
only behaves correctly as long as it is accessed exclusively through `PFADD`/`PFCOUNT`/
`PFMERGE`/`PFDEBUG` (all of which handle the `RudisValue::HyperLogLog` branch specially,
§4.11-4.14); any generic string read-then-elsewhere-write path silently produces a value that
no longer round-trips as a valid HLL. By contrast, `APPEND`/`STRLEN`/`GETRANGE`/`SETRANGE`
(§5 above) correctly call `hll_create_from_regs` first and so do *not* exhibit this bug — only
the plain `GET`-family and replication/migration encoders skip that conversion.

---

## 6. Future Improvements

- **Low — replace Top-K's O(k) linear-scan eviction with a min-heap for O(log k) (§3.4)** — only matters if `TOPK.RESERVE` is used with a large `k`; negligible at the small-k values this structure is typically used for.
- **Low — implement conservative update for the Count-Min Sketch (§3.3)** — the current `incr_by` always updates all `depth` rows unconditionally; switching to conservative update (only raising a row's counter up to the post-increment minimum) would tighten the sketch's over-estimation bound at a small extra per-increment cost, a well-known refinement of the base algorithm this implementation does not currently apply.
- **Low — support counting Bloom filters or scalable/auto-expanding variants** if `BF.INSERT ... EXPANSION`-style auto-growth or deletion-capable Bloom semantics become a compatibility target — today, over-inserting a fixed-capacity Bloom filter just silently raises its real false-positive rate above the configured target with no signal to the caller, and `BF.RESERVE`/`CF.RESERVE`/`TOPK.RESERVE` don't parse any of RedisBloom's optional tuning arguments at all (§5).
- **Low — document the false-positive-rate implications of Cuckoo filter fingerprint collisions explicitly** (§3.2) — `contains` can return a false positive if two different items hash to the same 16-bit fingerprint in the same bucket, same as any Cuckoo filter design; not a bug, but worth stating plainly given the structure's "no false negatives" framing can otherwise be read as "always exact." Also worth noting the kick eviction is fingerprint-deterministic, not RNG-based (§3.2).
- **Medium — fix the legacy `RudisValue::HyperLogLog` byte-leak on `GET`/replication/`MIGRATE`** (§5.4) — `GET`, replication propagation, and `MIGRATE` all serialize a legacy HLL as raw, unwrapped 16384-byte register bytes instead of calling `hll_create_from_regs` first (as `APPEND`/`STRLEN`/`GETRANGE`/`SETRANGE` correctly already do); the result is a non-`"HYLL"`-prefixed string that any subsequent `PF*` command rejects as `WRONGTYPE`. The fix is mechanical: route those three call sites through `hll_create_from_regs(regs, None)` the same way the string-op call sites already do.
- **Low — `hll::hll_merge` is dead code** (§4.10) — `Db::pfmerge` and the cross-shard `PFMERGE` fan-out in `connection.rs` both duplicate its decode/max/re-encode logic manually instead of calling it; either wire them up to call the shared function, or delete it.
- **Low — every mutating `PFADD`/`PFMERGE` pays a full O(16384) decode+re-encode** (§4.10) — real Redis patches the sparse opcode stream in place for the common case and only falls back to full decode/re-encode on a dense promotion; this implementation always takes the full-rebuild path. Fine at current scale/usage, but a real throughput difference under heavy per-element `PFADD` traffic on large sparse HLLs.
- **Low — `hll_compute_card` implements the classic/original Flajolet HLL estimator, not Redis 4.0+'s histogram/bias-corrected estimator** (§4.8) — `PFCOUNT` results will diverge numerically from a real Redis server computing cardinality over the identical register state; both are valid ~0.81%-std-error HLL estimators, but they are not bit-for-bit reproductions of each other.
- **Low — `PFDEBUG SIMD`/`PFSELFTEST` are pure RESP stubs** (§4.14) — `PFDEBUG SIMD` ignores its argument and always replies `+enabled`; there is no SIMD code path anywhere in `src/hll.rs`.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Cuckoo filters use 4-slot bucket tables with fingerprint-based partial-key cuckoo hashing (§3.2); `MAX_KICKS = 500` bounds the eviction chain before `CF.ADD` fails with `"ERR Cuckoo filter is full"`. The kick chain is deterministic from the fingerprint value, not RNG-seeded.
* **Gotcha 2**: The Count-Min Sketch uses the standard (non-conservative) update rule — every row is unconditionally incremented on `incr_by`, not just the rows needed to raise the minimum (§3.3).
* **Gotcha 3**: Top-K's Space-Saving algorithm gives O(1) updates only for items already tracked; once at capacity, admitting a new distinct item costs an O(k) linear scan to find the current minimum to evict (§3.4).
* **Gotcha 4**: A `PFADD`-created key is a plain `RudisValue::String` holding real `"HYLL"`-magic bytes (dense or sparse) — `TYPE key` and `SCAN ... TYPE` both report it as `"string"` (`src/table.rs:3531, 3630`), and `OBJECT ENCODING` reports `"raw"` like any other string (line 1459). There is no `"hyperloglog"` type or encoding string anywhere in this codebase.
* **Gotcha 5**: The legacy `RudisValue::HyperLogLog` variant only comes into existence via `RESTORE` of a `DUMP` payload with type tag `5` (this server's own, non-Redis-compatible legacy tag). `PF*` commands handle it transparently, but plain `GET`, replication, and `MIGRATE` leak its *raw register bytes* without the `"HYLL"` header — do not assume `GET` on a restored legacy HLL round-trips through `PFADD` elsewhere (§5.4).
* **Gotcha 6**: Sparse HLL encoding silently falls back to dense (12288-byte fixed payload) the moment any register exceeds value 32, or the growing sparse opcode stream exceeds `HLL_SPARSE_MAX_BYTES = 3000` bytes (§4.6) — there is no server-side config knob for this threshold (unlike real Redis' `hll-sparse-max-bytes`), it's a hardcoded constant.
* **Gotcha 7**: `hll_compute_card`'s cardinality formula is the classic 2007 Flajolet HLL estimator (linear counting + raw + large-range correction), not real Redis' modern histogram-based bias-corrected estimator — don't expect bit-identical `PFCOUNT` results against a real Redis instance holding logically-equivalent data (§4.8).

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1

# 4. HyperLogLog coverage: src/hll.rs itself has no #[cfg(test)] module (unlike
#    src/probabilistic.rs, which does, src/probabilistic.rs:368-417) — HLL behavior is
#    exercised via the ported Redis TCL suite and/or integration tests that call PFADD/
#    PFCOUNT/PFMERGE/PFDEBUG through the RESP layer.
tclsh tests/redis-tests/unit/hyperloglog.tcl   # if the test harness is wired up for TCL suites
```
