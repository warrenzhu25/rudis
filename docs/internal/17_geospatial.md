# Component 17: Geospatial Commands (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/geo.rs`  
> **High-Level Design Spec**: [`docs/design/17_geospatial.md`](../design/17_geospatial.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/geo.rs` | Core implementation and logic | Primary data structures and algorithms |

---

### 2. Component Architecture & Data Structures

```
GEOADD geo:cities -122.4194 37.7749 "San Francisco"
              │
              ▼
   encode_geohash(-122.4194, 37.7749) -> u64 (52-bit interleaved)
              │
              ▼
   db.zadd("geo:cities", [(hash as f64, "San Francisco")], flags)
              │
              ▼
   RudisZSet (Component 05) — "San Francisco" -> score = hash as f64

GEODIST geo:cities "San Francisco" "Oakland" km
              │
              ▼
   db.zscore() twice -> decode_geohash() twice -> haversine_distance() -> GeoUnit::from_meters()
```

---

### Real supporting types (`src/geo.rs`)

```rust
pub enum GeoUnit { Meters, Kilometers, Miles, Feet }   // to_meters/from_meters conversion factors:
                                                         // km=1000, mi=1609.344, ft=0.3048 — match real Redis

pub struct GeoItemResult {
    pub member: Bytes,
    pub dist: Option<f64>,
    pub hash: Option<u64>,
    pub coord: Option<(f64, f64)>,
}
```

`GeoItemResult` + `format_geo_results` unify the reply-formatting for every command that can
return `WITHCOORD`/`WITHDIST`/`WITHHASH` options (`GEORADIUS`, `GEORADIUSBYMEMBER`,
`GEOSEARCH`) — one shared formatter rather than three separately hand-written reply encoders.

```rust
pub enum GeoSearchShape {
    Radius { radius_m: f64 },
    Box { width_m: f64, height_m: f64 },
}
```

`GeoSearchShape` is the shared query-shape type consumed by `GEORADIUS`/`GEORADIUSBYMEMBER`
(always `Radius`) and `GEOSEARCH` (`Radius` for `BYRADIUS`, `Box` for `BYBOX`) — one execution
path (`execute_geo_query`, §3.5) serves all three commands regardless of shape.

---

### 3. Execution Algorithms & Code Logic

#### 3.1 `encode_geohash`/`decode_geohash`: bit interleaving, not a lookup table

```rust
for i in 0..26 {
    let bit_x = (x >> i) & 1;
    let bit_y = (y >> i) & 1;
    hash |= bit_x << (2 * i);
    hash |= bit_y << (2 * i + 1);
}
```

Longitude and latitude are each linearly normalized into a 26-bit integer, then their bits are
interleaved (x at even positions, y at odd) into one 52-bit value — the standard Z-order/Morton
encoding real geohashing uses, decoded by the exact inverse bit-extraction in `decode_geohash`.
Because this is lossy quantization (26 bits per axis over the real coordinate range), a
decode-then-re-encode round trip returns a coordinate close to, but not bit-identical to, the
original input — `decode_geohash` returns the *center* of the encoded cell
(`(x + 0.5) / max_val`), which is why the round-trip test in this file asserts closeness
(`< 1e-5`) rather than exact equality.

#### 3.2 `GEOADD`'s real implementation: encode, then delegate entirely to `ZADD`

```rust
for (lon, lat, member) in items {
    match crate::geo::encode_geohash(*lon, *lat) {
        Ok(hash) => elements.push((hash as f64, member.clone())),
        Err(e) => { out.extend_from_slice(...); return false; }
    }
}
let flags = crate::table::ZAddFlags { nx: *nx, xx: *xx, ch: *ch, gt: false, lt: false, incr: false };
match db.zadd(key.clone(), elements, flags) { ... }
```

`NX`/`XX`/`CH` flags pass straight through to the underlying `ZADD` semantics (Component 05) —
`GEOADD` adds no geo-specific conflict handling of its own beyond the encoding step.

#### 3.3 `GEODIST`: two `zscore` lookups, decode, Haversine, unit conversion

Straightforward: `db.zscore(key, m1)`/`db.zscore(key, m2)` fetch the two raw geohash scores,
`decode_geohash` turns each back into coordinates, `haversine_distance` computes meters, and
the requested `GeoUnit` (default meters) converts the final figure — a member with no score
(never added, or added via a non-geo `ZADD` with a score that happens to decode to garbage
coordinates) returns `$-1\r\n` only if the `zscore` lookup itself misses, not if the decoded
"coordinates" are nonsensical.

#### 3.4 Geohash-interval pruning: `geohash_search_ranges`

All three radius/box-search commands (`GEORADIUS`, `GEORADIUSBYMEMBER`, `GEOSEARCH`) funnel
through one shared execution path (§3.5), fronted by `geohash_search_ranges`, which computes a
small set of contiguous 1D geohash score intervals covering the query's bounding box **before**
any ZSET access happens:

```rust
pub fn geohash_search_ranges(lon: f64, lat: f64, radius_meters: f64) -> Vec<(f64, f64)> {
    // 1. Degenerate/global cases handled directly:
    //    radius <= 0   -> a single exact-point interval [hash, hash]
    //    radius >= 20_000_000m -> the full 52-bit range [0, 2^52 - 1] (no pruning possible)

    // 2. Otherwise, convert the radius to a local lat/lon bounding box using the
    //    111,320 m/degree flat-Earth approximation (same approximation GEOSEARCH's
    //    BY BOX shape uses, §3.5), clamped to the valid lon/lat ranges.

    // 3. Normalize the box's four corners into 26-bit x/y grid coordinates, then find the
    //    largest power-of-two cell size `k` (via `delta_max.leading_zeros()`) such that the
    //    box's x-span and y-span each fit within one or two cells of that size.

    // 4. Enumerate every k-sized grid cell the box overlaps (cx in [cell_min_x, cell_max_x],
    //    cy in [cell_min_y, cell_max_y]); for each cell, interleave its low and high corner
    //    into a [h_min, h_max] geohash interval exactly like `encode_geohash` does.

    // 5. Sort the resulting per-cell intervals by lower bound and merge adjacent/overlapping
    //    ones (start <= last.1 + 1.0) into the final, minimal set of contiguous ranges.
}
```

This is a self-contained interval decomposition, not a port of real Redis's literal "9
neighboring geohash cells at a fixed resolution" technique — it instead picks the coarsest
common cell size `k` that covers the query box in at most a handful of cells, then always
merges adjacent output intervals, so the number of ranges returned adapts to the query's actual
size rather than being fixed at 9. `radius_meters >= 20_000_000.0` (roughly the antipodal
distance on Earth) short-circuits to the single all-encompassing interval `[0, 2^52 - 1]`,
which is honest about the fact that a search radius that large cannot be pruned at all.

#### 3.5 `execute_geo_query`: one range query per interval, not a scan of the whole set

```rust
pub fn execute_geo_query(db, key, center_lon, center_lat, shape, unit,
                          withdist, withhash, withcoord, count, asc) -> Vec<GeoItemResult> {
    let ranges = shape.search_ranges(center_lon, center_lat);   // §3.4, or a box-diagonal radius
    for (min_score, max_score) in ranges {
        let z_opts = ZRangeOpts { by_score: true, min_score, max_score, with_scores: true, .. };
        for (member, score) in db.zrange(key, &z_opts).unwrap_or_default() {
            if !seen.insert(member.clone()) { continue; }             // interval de-dup
            let (m_lon, m_lat) = decode_geohash(score as u64);
            let dist_m = haversine_distance(center_lon, center_lat, m_lon, m_lat);
            if shape.is_inside(center_lon, center_lat, m_lon, m_lat, dist_m) {
                results.push(GeoItemResult { member, dist, hash, coord });
                // early break once `count` is satisfied, only when no ASC/DESC sort requested
            }
        }
    }
    // if asc/desc requested: sort by distance; then truncate to `count`
}
```

Each interval from `geohash_search_ranges` is served by `RudisTable::zrange` with
`ZRangeOpts { by_score: true, .. }` (Component 05), which for the `Full` (B-tree-backed) ZSet
representation performs a real ordered range query (`BTreeMap::range`, `Bound::Included(min)..`)
rather than a linear scan of the whole set — cost is roughly O(log N) to seek plus O(k) to walk
the `k` members inside that interval. For the `Small` (linear-vector) ZSet representation used
below the compaction threshold, the same `ZRangeOpts` request degrades to a linear filter over
that (small, bounded-size) vector, which is the expected and acceptable cost at that scale.

A `seen: hashbrown::HashSet<Bytes>` guards against double-counting a member that falls inside
more than one merged interval (possible near a cell boundary, since `geohash_search_ranges`'s
merge step only coalesces intervals that are contiguous or overlapping, not ones that are
merely nearby). `GeoSearchShape::is_inside` performs the final exact-radius or exact-box test
on every interval-matched candidate — the interval pruning narrows *which* members are decoded
and distance-checked, but the final radius/box test is still exact, so pruning cannot introduce
false positives, only reduce the candidate set the exact test has to run against. `GEOSEARCH`'s
`BY BOX` variant approximates the box test itself as
`dlat_m = Δlat° × 111,320` / `dlon_m = Δlon° × 111,320 × cos(center_lat)` — a flat-Earth-local
approximation, not exact geodesic box math; reasonable at the box sizes these commands are
typically used for but not precise at very large box dimensions.

`Command::Georadiusbymember` and the `FROMMEMBER` form of `GEOSEARCH` resolve their center point
via a `zscore` lookup and `decode_geohash` before calling `execute_geo_query`, and `GEOSEARCH`
with neither `BYRADIUS` nor `BYBOX` falls back to `GeoSearchShape::Radius { radius_m: 0.0 }`
(matched only by an exact-hash point, via `geohash_search_ranges`'s degenerate-radius case).

---

### 4. Cross-Component Interactions

- **`src/table.rs`** (Component 05): every geo command is built entirely on `RudisTable`'s
  `zadd`/`zscore`/`zrange` — there is no geo-specific storage anywhere; a "geo set" *is* a
  `RudisZSet`, and `execute_geo_query`'s per-interval pruning (§3.5) relies directly on
  `ZRangeOpts { by_score: true, .. }` being a real ordered range query against that ZSet.
- **`src/connection.rs`** (Component 02): owns every `Command::Geo*` match arm (`geo.rs` itself
  contains no command dispatch, only the math/formatting helpers those arms call, plus
  `execute_geo_query` which takes `&mut ShardDb` directly); routes all of them through the
  standard `target_shard_of_cmd` keyed-command path (§2.4).
- **`src/resp.rs`** (Component 03): parses `GEOADD`/`GEODIST`/etc.'s arguments (including unit
  strings `m`/`km`/`mi`/`ft` and `BYRADIUS`/`BYBOX`/`ASC`/`DESC`/`WITHCOORD`/`WITHDIST`/
  `WITHHASH` option flags) into the `Command::Geo*` variants this file's helpers consume.

---

### 5. Future Improvements

- **Low — factor `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`'s command-handling glue in `connection.rs` into fewer near-duplicate call sites.** All three already share the same core search execution (`execute_geo_query`, §3.5), but the surrounding argument-resolution and reply-formatting code at each call site is still hand-duplicated three times; a bugfix to the shared shape needs to be re-verified at each site.
- **Low — replace the `BY BOX` flat-Earth approximation with exact geodesic box math** (§3.5) if very large box searches (spanning enough latitude/longitude to make the flat approximation's error non-negligible) become a real use case — not a concern at typical city-scale search radii.
- **Low — tighten `geohash_search_ranges`'s interval count for very eccentric boxes** (§3.4): the algorithm picks one common cell size `k` for both axes, so a very wide, thin bounding box can still enumerate more cells (and thus intervals) than a two-axis-aware decomposition would; not a concern at the aspect ratios typical `GEOSEARCH BYBOX` calls use.
- **Low — reject non-geo `ZADD`s against a key already used as a geo set, or document the compatibility footgun explicitly (§1)** — since a geo set is just a `ZSET`, nothing stops an ordinary `ZADD member not-a-geohash-score` from corrupting later `GEODIST`/`GEOPOS`/`GEOSEARCH` calls against that key with silently-nonsensical decoded coordinates.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Longitude is bound to `[-180, 180]`, latitude to `[-85.05112878, 85.05112878]` (`GEO_LAT_MIN`/`GEO_LAT_MAX`) — not `[-90, 90]`; a `GEOADD` outside this range returns `"ERR invalid latitude"`.
* **Gotcha 2**: `GEOSEARCH` supports `BYRADIUS` and `BYBOX` bounding shapes, both served by the same `execute_geo_query`/`geohash_search_ranges` pruning path (§3.4/§3.5) — `BYBOX` is not a separate, unpruned code path.
* **Gotcha 3**: The Haversine formula uses a fixed spherical Earth radius constant, `EARTH_RADIUS_METERS = 6372797.560856` — the same literal constant value real Redis's own Haversine implementation hardcodes, not a value pulled from the WGS84 ellipsoid parameters (whose semi-major axis is 6378137.0m) or a simple mean-radius approximation.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
