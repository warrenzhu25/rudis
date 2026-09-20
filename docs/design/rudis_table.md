# Design Document: `RudisTable` - Custom In-Memory Storage Engine

## 1. Background & Motivation

In-memory data stores like Redis and Dragonfly depend fundamentally on their core associative hash table implementation:
- **Redis (`dict.c`)**: Uses chaining with linked lists (`dictEntry`). While it supports progressive incremental rehashing (`dictRehash`), it suffers from heavy pointer chasing, cache misses, and significant memory overhead (24–32 bytes per entry).
- **Dragonfly (`DashTable`)**: Uses a custom hash table inspired by the VLDB 2020 paper *"DASH: Scalable and Write-Efficient In-Memory Hashing"*. It organizes entries into 64-byte cache-line-sized buckets with 1-byte hash fingerprints, using segmented directory splits to eliminate latency spikes. However, its implementation carries complex concurrency locks and versioning headers designed for fiber-level preemption and migration.
- **Rudis (Current)**: Relies on `hashbrown::HashMap` (Google SwissTable). While it provides high single-thread performance (940k Ops/sec) and SIMD SSE2/NEON group probing, it has two key limitations:
  1. **Monolithic Reallocation**: Resizes the entire backing table at once when load factor exceeds 87.5%, introducing tail-latency (p999) spikes on large key spaces.
  2. **Decoupled TTL & Multi-Map Overhead**: Values (`entries`), TTL timestamps (`expirations`), and cluster slots (`slot_to_keys`) are stored in distinct hash tables. A read or write operation frequently requires multiple independent hash lookups.

`RudisTable` is a custom storage engine tailored specifically for the thread-per-core, shared-nothing, `io_uring` architecture of `rudis`.

---

## 2. Core Architectural Pillars

### Pillar 1: Cache-Line Aligned SIMD Bucket Groups
Following the principles of SwissTable and DASH:
- The table is organized into discrete **Buckets** aligned to 64-byte boundaries (the standard CPU L1 cache line size).
- Each bucket contains a **Metadata Control Header** consisting of 14–16 one-byte slots:
  - `0xFF`: Empty slot
  - `0xFE`: Deleted slot (Tombstone)
  - `0x00..=0x7F`: 7-bit hash fingerprint (top 7 bits of hash)
- **SIMD Probing**: Using 128-bit vector instructions (`_mm_cmpeq_epi8` / `movemask` on x86_64, or NEON `vceqq_u8` on ARM), a lookup evaluates all 16 slots in parallel in a single CPU clock cycle before dereferencing any key or value payload.

### Pillar 2: Inlined Entry & TTL Representation
Instead of maintaining separate tables for values and expirations:
- Each slot stores a unified `RudisEntry`:
  ```rust
  pub enum RudisValue {
      String(Bytes),
      Hash(FlatHash),
  }

  pub struct RudisEntry {
      pub key: Bytes,
      pub val: RudisValue,
      pub expire_at: Option<Instant>, // or compact relative timestamp
  }
  ```
- **Single-Probe Resolution**:
  - `GET`, `SET`, `TTL`, `EXPIRE`, and passive expiration checks are evaluated within the same cache line.
  - Expired keys encountered during normal probes are passively evicted inline with zero secondary table lookups.

### Pillar 3: Segmented Incremental Rehashing (Zero Latency Spikes)
To eliminate monolithic resize pauses:
- The table utilizes **Segmented Directory Resizing**:
  - A top-level directory points to independent **Segments** (contiguous arrays of buckets).
  - When an individual segment exceeds its target load factor (e.g., 85%), only that segment is split and rehashed into two new segments.
  - The top-level directory updates its pointers using standard extendible hashing prefix masks.
  - Maximum pause time per operation remains strictly bounded ($O(1)$ constant time per split), preserving sub-millisecond p99.9 latency under high ingestion rates.

### Pillar 4: Pure Thread-Local / Shared-Nothing
- Because Rudis strictly enforces thread-per-core isolation via Monoio, every `RudisTable` instance belongs exclusively to one thread on its dedicated CPU core.
- **Zero Synchronization**:
  - NO mutexes
  - NO read-write locks
  - NO atomic CAS loops
  - NO versioning headers in bucket metadata

---

## 3. Detailed Data Layout

```text
+-------------------------------------------------------------------------+
|                               Segment                                   |
+-------------------------------------------------------------------------+
| Bucket 0: [16B Control Bytes] [Slot 0 .. Slot 13 Payload Pointers/Data] |
| Bucket 1: [16B Control Bytes] [Slot 0 .. Slot 13 Payload Pointers/Data] |
| Bucket 2: [16B Control Bytes] [Slot 0 .. Slot 13 Payload Pointers/Data] |
| ...                                                                     |
+-------------------------------------------------------------------------+
```

### 3.1 Control Byte States
```rust
const EMPTY: u8 = 0xFF;
const DELETED: u8 = 0xFE;

#[inline(always)]
fn fingerprint(hash: u64) -> u8 {
    ((hash >> 57) & 0x7F) as u8
}
```

### 3.2 Lookup Algorithm
1. Compute 64-bit hash $H = \text{hash}(key)$ using high-speed `foldhash` or `fxhash`.
2. Extract segment index and bucket index:
   $$\text{seg\_idx} = (H \gg 32) \ \& \ \text{segment\_mask}$$
   $$\text{bucket\_idx} = H \ \& \ \text{bucket\_mask}$$
3. Load 16-byte control array into 128-bit SIMD register.
4. Compare against broadcasted target fingerprint $F = \text{fingerprint}(H)$.
5. Extract match bitmask. For each matched bit:
   - Check if key in slot matches target key.
   - If match found, check `expire_at`:
     - If expired, mark slot as `DELETED`, drop entry, return `None`.
     - If valid, return reference to value.
6. If empty slot is encountered during probe chain, terminate search (key does not exist).

---

## 4. Phased Implementation Plan

1. **Phase 1: Flat SIMD Bucket Engine (`RudisBucketTable`)**:
   - Implement the 64-byte bucket layout with 16-slot SIMD control bytes and unified inlined entry (`key`, `val`, `expire_at`).
   - Benchmark vs `hashbrown` on fixed sizes to verify equal or higher raw probe speed.

2. **Phase 2: Segmented Directory & Incremental Splitting**:
   - Implement directory-based extendible hashing over segment arrays.
   - Verify bounded pause times during continuous heavy insertions.

3. **Phase 3: Integration into `ShardDb`**:
   - Replace separate `entries` and `expirations` in `ShardDb` with `RudisTable`.
   - Update passive expiration and active sampling cycle to leverage inlined timestamps.
   - Validate against full test suite (`cargo test`) and ensure zero benchmark regressions.
