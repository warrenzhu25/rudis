# Component 17: Geospatial Commands (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/geo.rs`  
> **Implementation Reference**: [`docs/internal/17_geospatial.md`](../internal/17_geospatial.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Traditional in-memory datastores encounter severe scalability barriers on modern multi-core, high-throughput cloud hardware. Single-threaded architectures (such as Redis) saturate a single CPU core while leaving the remaining 95%+ of server cores idle. Multi-threaded mutex architectures (such as Memcached) suffer from heavy spinlock contention, CPU cache line bouncing, and global memory allocator lock bottlenecks.

### 1.2 The Rudis Solution
Rudis implements the **Thread-Per-Core (Shared-Nothing)** architectural paradigm natively on Linux `io_uring` via Monoio. Each physical CPU core owns its own isolated event loop, its own thread-local memory database, and its own kernel `SO_REUSEPORT` listener. Operations on local keys execute in nanoseconds with zero locks, zero atomic operations, and zero cross-core cache invalidations.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model
Geospatial indexing using 52-bit integer geohashes. Coordinates (longitude, latitude) map to 52-bit integers stored as scores in Sorted Sets (ZSet). Distance queries use the Haversine spherical formula.

### 2.2 Design Rationale (The "Why")
Geohashes map 2D coordinates into 1D space, enabling standard B-tree / skip-list range queries to find nearby entities with zero specialized spatial index overhead.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **Encoding is 52-bit interleaved (26 bits longitude + 26 bits latitude), matching real
   Redis's internal encoding** — not the 5-bit-alphabet, 11-character textual geohash;
   that (`geohash_to_base32`) is only computed on demand for the `GEOHASH` command's text
   output, never used as the stored representation.
2. **Latitude is clamped to the real Web Mercator-projectable range**
   (`GEO_LAT_MIN`/`MAX` = ±85.05112878°, not ±90°) — this matches real Redis's own
   documented limitation exactly (values interleave cleanly only within this range); a
   `GEOADD` outside it is rejected with `"ERR invalid latitude"`.
3. **Distance is Haversine (great-circle on a sphere), not an ellipsoidal (Vincenty) model** —
   using `EARTH_RADIUS_METERS = 6372797.560856`, the same constant real Redis's own Haversine
   implementation uses. This is an approximation (Earth isn't a perfect sphere) but is exactly
   what real Redis does too, so behavior matches rather than diverges.
4. **Geo commands route per-key across shards exactly like ordinary ZSET commands** (verified:
   `Geoadd`/`Geodist`/`Geopos`/`Geosearch`/etc. all appear in the same `target_shard_of_cmd`
   dispatch arm as other keyed commands, Component 02 §4.5) — there is no geo-specific routing
   concern; a "geo set" is routed by the same CRC16 key-slot mechanism as any other key.
5. **Radius/box search is pruned by geohash interval decomposition, not a full-set scan.**
   `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` all compute a small set of contiguous 1D geohash
   `[min, max]` intervals that cover the query's bounding box (`geohash_search_ranges`), then
   issue one `ZRANGEBYSCORE`-style range query per interval (`execute_geo_query`) instead of
   decoding and distance-checking every member of the set — see §3.4/§3.5 of the internal
   document for the interval-derivation algorithm.

---

## 3. High-Level Architecture & Workflow Diagram

```
(Longitude, Latitude) ──► 52-Bit Integer Geohash ──► ZSet Score (B-Tree)
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **`GEOADD`/`GEODIST`/`GEOPOS` are O(1)-ish**, bounded by the underlying `ZADD`/`ZSCORE` cost
  (Component 05) plus a fixed amount of bit-interleaving/Haversine math — no scan involved.
- **`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` prune via geohash-interval decomposition
  before doing any distance math** (internal document §3.4/§3.5): the query's bounding box is
  covered by a small number of contiguous geohash score intervals, and each interval is served
  by the underlying ZSET's score-ordered range query. Against the `Full` (skip-list/B-tree-backed)
  ZSet representation (Component 05), each interval query costs roughly O(log N + k) where `k`
  is the number of members that fall in that interval; against the `Small` (linear-vector) ZSet
  representation used for sets below the compaction threshold, a range query is a linear scan of
  that small vector regardless. This is materially better than a naive full-set scan for large,
  geographically dispersed sets, though it is still not an exact-neighborhood search: a search
  radius near the geohash "cell" boundary can require multiple intervals, and the interval
  decomposition (not real Redis's literal "9 neighboring cells" technique) is a close but
  independently-derived approximation of the same idea.
- **No caching of decoded coordinates** — every scan re-runs `decode_geohash` (cheap bit
  extraction) per candidate member; not a measurable cost relative to the Haversine
  trigonometry, which dominates the per-member cost once a candidate is selected.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/17_geospatial.md`**](../internal/17_geospatial.md): Low-level implementation and code reference.
* **Source Files**: `src/geo.rs`
