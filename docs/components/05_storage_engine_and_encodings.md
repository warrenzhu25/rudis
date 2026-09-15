# Component 05: Storage Engine & Compact Encodings (`src/table.rs`)

## 1. Architectural Purpose & Scope

`src/table.rs` is Rudis's core in-memory associative storage engine. It provides the dictionary implementation (`RudisTable`), manages the lifecycle of `RudisValue` data structures, enforces compact memory encodings (Listpack, Intset, Skiplist), and coordinates active/passive key expiration.

---

## 2. Key Invariants & Concurrency Constraints

1. **Thread-Isolation**: Each `RudisTable` belongs to a single shard thread. It contains **no mutexes, atomic operations, or lock-free concurrency wrappers**.
2. **Inlined Metadata & Expiration**: Expiration timestamps (`expire_at`) are stored directly inside `RudisEntry`. Checking key validity does not require secondary hash lookups.
3. **Adaptive Promotion & Demotion**: Small collections are stored as dense continuous byte arrays (`Listpack`, `Intset`). When size or length thresholds are crossed, collections are automatically promoted to indexed structures (`FlatHash`, `SkipList`).
4. **Passive Inline Eviction**: Any operation encountering an expired key immediately frees it and returns `None` without waiting for the background cleaner.

---

## 3. Data Structures & Memory Layouts

```
                          RudisTable (SwissTable SIMD Probing)
                                       │
                         ┌─────────────┴─────────────┐
                         ▼                           ▼
                   "session:100"               "leaderboard"
                  RudisEntry                  RudisEntry
                  ├── expire_at: None         ├── expire_at: Some(t + 3600)
                  └── val: RudisValue::Hash   └── val: RudisValue::ZSet
                           │                           │
                   [ Listpack Buffer ]         [ Augmented Skiplist + Hash ]
                   • field1 -> val1            • member -> score
                   • field2 -> val2            • Level 0..16 spans
```

### Core Memory Definitions

```rust
pub struct RudisTable {
    pub entries: hashbrown::HashMap<Bytes, RudisEntry, FxBuildHasher>,
}

pub struct RudisEntry {
    pub key: Bytes,
    pub val: RudisValue,
    pub expire_at: Option<Instant>,
}

pub enum RudisValue {
    String(Bytes),
    Hash(RudisHash),
    List(RudisList),
    Set(RudisSet),
    ZSet(RudisZSet),
    Stream(RudisStream),
    Json(RudisJson),
    Bitmap(RudisBitmap),
    HyperLogLog(RudisHll),
    External(RudisExternalPtr), // Stored on NVMe SSD
}
```

---

## 4. Compact Encodings Deep-Dive

### 4.1 Hash Encoding: Listpack vs FlatHash

- **Listpack Encoding**: A contiguous array of entries formatted as `[field_len, field_data, val_len, val_data]`. Linear scans require no pointer dereferencing and fit entirely into L1/L2 cache lines.
- **Thresholds**: If field count exceeds `hash-max-listpack-entries` (512) or any field/value exceeds `hash-max-listpack-value` (64 bytes), the Listpack is unfolded into a `FlatHash`.

```rust
pub enum RudisHash {
    Listpack(Vec<u8>),
    Table(hashbrown::HashMap<Bytes, Bytes, FxBuildHasher>),
}

impl RudisHash {
    pub fn set(&mut self, field: Bytes, val: Bytes) {
        match self {
            RudisHash::Listpack(lp) => {
                if lp_entries(lp) < 512 && field.len() <= 64 && val.len() <= 64 {
                    lp_insert_or_replace(lp, &field, &val);
                } else {
                    // Promote to Table
                    let mut table = lp_to_table(lp);
                    table.insert(field, val);
                    *self = RudisHash::Table(table);
                }
            }
            RudisHash::Table(table) => {
                table.insert(field, val);
            }
        }
    }
}
```

### 4.2 Set Encoding: Intset vs FlatSet

- **Intset Encoding**: An array of sorted integers stored using the minimal required byte width (16-bit, 32-bit, or 64-bit).
- **Search**: Employs $O(\log N)$ binary search.
- **Auto-Promotion**: If an integer cannot fit in 64 bits or a non-integer string is added, the Intset converts to `FlatSet`.

```rust
pub enum RudisSet {
    Intset(Intset),
    Table(hashbrown::HashSet<Bytes, FxBuildHasher>),
}

pub struct Intset {
    pub encoding: IntsetEncoding, // Int16, Int32, or Int64
    pub data: Vec<u8>,
}
```

### 4.3 Sorted Set (ZSet): Augmented Skiplist with Span Ranks

To support $O(\log N)$ range lookups and rank calculations (`ZRANK`, `ZREVRANK`), Rudis pairs a `hashbrown::HashMap<Bytes, f64>` with a multi-level **SkipList**:

```rust
pub struct SkipListNode {
    pub member: Bytes,
    pub score: f64,
    pub backward: *mut SkipListNode,
    pub levels: Vec<SkipListLevel>,
}

pub struct SkipListLevel {
    pub forward: *mut SkipListNode,
    pub span: usize, // Number of elements skipped at Level 0
}
```

#### Rank Calculation Algorithm ($O(\log N)$)
```rust
pub fn get_rank(&self, member: &[u8], score: f64) -> Option<usize> {
    let mut rank = 0;
    let mut curr = self.head;

    for i in (0..self.max_level).rev() {
        unsafe {
            while !(*curr).levels[i].forward.is_null()
                && ((*(*curr).levels[i].forward).score < score
                    || ((*(*curr).levels[i].forward).score == score
                        && (*(*curr).levels[i].forward).member.as_ref() <= member))
            {
                rank += (*curr).levels[i].span;
                curr = (*curr).levels[i].forward;
            }
            if !curr.is_null() && (*curr).member.as_ref() == member {
                return Some(rank);
            }
        }
    }
    None
}
```

---

## 5. Expiration Management

Rudis combines **passive** (inline) and **active** (background probabilistic) expiration:

### 5.1 Passive Expiration on Read
```rust
impl RudisTable {
    pub fn get(&mut self, key: &[u8]) -> Option<&RudisValue> {
        if let Some(entry) = self.entries.get(key) {
            if let Some(expire) = entry.expire_at {
                if Instant::now() >= expire {
                    self.entries.remove(key); // Evict inline
                    return None;
                }
            }
            return Some(&entry.val);
        }
        None
    }
}
```

### 5.2 Active Expiration Sampling (`active_expire_sample`)
Every 100ms, the reactor randomly samples up to `sample_size` keys with expirations:
```rust
pub fn active_expire_sample(&mut self, sample_size: usize) {
    let now = Instant::now();
    let expired_keys: Vec<Bytes> = self.entries.iter()
        .filter_map(|(k, entry)| {
            if let Some(exp) = entry.expire_at {
                if now >= exp { Some(k.clone()) } else { None }
            } else {
                None
            }
        })
        .take(sample_size)
        .collect();

    for k in expired_keys {
        self.entries.remove(&k);
    }
}
```

---

## 6. Performance Characteristics

- **SIMD Accelerations**: `hashbrown::HashMap` processes 16 bucket control bytes in parallel via 128-bit vector instructions (`_mm_cmpeq_epi8`).
- **Memory Footprint**: Small hashes and sets achieve up to **$5\times$ smaller RAM footprint** than uncompressed pointer-based hash tables.
