# Component 17: Geospatial Commands (Implementation Deep-Dive & Code Reference)

> **Source File**: `src/geo.rs` (569 lines)
> **Command dispatch / glue**: `src/connection.rs` (`Command::Geo*` match arms, ~lines 16997–17386)
> **Argument parsing**: `src/resp.rs` (`Command::Geo*` struct defs ~lines 1509–1584; parsers ~lines 10419–10975)
> **High-Level Design Spec**: [`docs/design/17_geospatial.md`](../design/17_geospatial.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 0. What changed since the previous pass of this document

The previous version of this document described a **custom, non-Redis** search-pruning
algorithm (`geohash_search_ranges`: a power-of-two-cell interval decomposition with interval
merging) that explicitly was *not* a port of real Redis's 9-neighbor-cell technique.

That algorithm **no longer exists**. Commit `375b237` ("implement Redis-compatible geospatial
engine and pass unit/geo") replaced it with a close line-for-line port of real Redis's actual
`geohash.c`/`geohash_helper.c` algorithm:

- `GeoHashBits { bits: u64, step: u8 }` — a variable-precision interleaved geohash, distinct from
  the fixed-26-bit-per-axis hash stored in the ZSET.
- `geohash_move_x` / `geohash_move_y` — the real Redis bit-mask trick for moving one grid cell in
  interleaved-bit space without a decode/re-encode round trip.
- `geohash_estimate_steps_by_radius` — the real Redis radius→precision estimator (doubling-radius
  loop + high-latitude correction at 66°/80°).
- `calculate_search_areas` — the real Redis "center cell + up to 8 neighbors, trimmed to the ones
  that actually overlap the query bounding box" algorithm, replacing the old merged-interval
  approach.

`tests/redis-tests/unit/geo.tcl` (real Redis's own Tcl geospatial test suite) is now listed in
`CORE_SUITES` in `scripts/run_redis_test_suite.sh:69`, i.e. it is part of the standard CI run —
this rewrite's purpose was Redis test-suite compatibility, not just an internal refactor.

Also corrected in this pass: the mile-conversion constant is `1609.34`, not `1609.344` as the
prior document claimed (§2 below) — it matches real Redis's own (slightly non-standard) constant.
And the `GEOSEARCH BYBOX` exact-match test is **not** a flat-Earth approximation as previously
documented — it uses `geohash_lat_distance` (exact spherical meridian arc) and `haversine_distance`
(exact great-circle distance) for the final filter; flat-Earth-style trigonometric approximation is
used only to size the *search* bounding box (§3.5), never to decide final shape membership (§3.4).

A new, previously-undocumented correctness issue was found during this pass: **`GEORADIUS ...
STORE`/`STOREDIST` and `GEOSEARCHSTORE` write their destination key on the shard that owns the
*source* key, not the shard that owns the destination's hash slot** — see §6.1, Known Bug #1.

---

## 1. Source Module Map & Responsibilities

| File | Role |
| :--- | :--- |
| `src/geo.rs` | All geohash math (fixed-precision encode/decode for storage, variable-precision `GeoHashBits` grid for search), Haversine distance, unit conversion, the 9-cell search-area algorithm, the shared query executor (`execute_geo_query`), and reply formatting (`format_geo_results`). Contains **no command dispatch** — it is a pure helper/algorithm module. |
| `src/connection.rs` | Owns every `Command::Geo*` match arm: argument validation against the live ZSET (`WRONGTYPE`, "key does not exist"), calling into `geo.rs`'s helpers, and writing the RESP reply. |
| `src/resp.rs` | Defines the eight `Command::Geo*` enum variants and parses `GEOADD`/`GEODIST`/`GEOPOS`/`GEOHASH`/`GEORADIUS[_RO]`/`GEORADIUSBYMEMBER[_RO]`/`GEOSEARCH`/`GEOSEARCHSTORE` wire arguments into them. |
| `src/table.rs` | `RudisZSet` (`Small`/`Full` variants) and `ZRangeOpts`-driven `range()` — every geo command's storage substrate; a "geo set" is literally a `ZSET` whose scores are 52-bit geohash integers reinterpreted as `f64`. |

---

## 2. Constants (all in `src/geo.rs`, lines 3–11)

| Constant | Value | Line | Meaning |
| :--- | :--- | :--- | :--- |
| `EARTH_RADIUS_IN_METERS` | `6372797.560856` | 3 | Fixed spherical Earth radius used by Haversine. Identical literal to real Redis's own `D_R` constant — **not** WGS84's semi-major axis (6378137.0m) and **not** a simple mean-radius approximation. |
| `MERCATOR_MAX` | `20037726.37` | 4 | Half of Earth's circumference (≈ Mercator-projection maximum extent); used only in `geohash_estimate_steps_by_radius` as the loop-termination bound. |
| `GEO_LAT_MIN` / `GEO_LAT_MAX` | `-85.05112878` / `85.05112878` | 5–6 | Valid latitude range for the internal encoding — the standard Mercator-projectable latitude bound, **not** `[-90, 90]`. |
| `GEO_LON_MIN` / `GEO_LON_MAX` | `-180.0` / `180.0` | 7–8 | Valid longitude range. |
| `GEO_STEP_MAX` | `26` | 9 | Bits per axis for the ZSET-stored geohash (52 bits total). Declared but not referenced directly in the fixed-precision `encode_geohash`/`decode_geohash` path below — those hardcode the loop bound `26` instead of reading this constant (cosmetic inconsistency, not a bug, since the two values are kept in sync by construction). |
| `GEOHASH_ALPHABET` | `b"0123456789bcdefghjkmnpqrstuvwxyz"` | 11 | Standard 32-character geohash base32 alphabet, used only by `GEOHASH`'s string output (§3.3). |

---

## 3. Geohash Encoding — Two Separate Encoders, One Purpose Each

There are **two independent interleaving implementations** in this file, used for two different
purposes. They share the same bit-interleaving *idea* but are separate code paths:

1. **Fixed 26-bit-per-axis encoder** (`encode_geohash`/`decode_geohash`, §3.1) — used to compute
   the **score stored in the ZSET** by `GEOADD` and to decode that score back to coordinates
   everywhere else (`GEODIST`, `GEOPOS`, `GEOHASH`, and the center-point resolution for
   `GEORADIUSBYMEMBER`/`GEOSEARCH ... FROMMEMBER`). Always exactly 26 bits per axis (52 bits
   total) — precision is fixed, not query-dependent.
2. **Variable-precision `GeoHashBits` grid** (`encode_with_step`/`decode_area`, §3.4) — used only
   internally by `calculate_search_areas` to build a small set of geohash grid cells to scan. Step
   count (precision) varies per query, chosen by `geohash_estimate_steps_by_radius` based on the
   search radius.

### 3.1 `encode_geohash`/`decode_geohash` (`src/geo.rs:53–89`) — storage-score codec

```rust
pub fn encode_geohash(lon: f64, lat: f64) -> Result<u64, String> {
    // range check against GEO_LON_MIN/MAX, GEO_LAT_MIN/MAX -> "ERR invalid longitude,latitude pair {lon},{lat}"
    let lat_int = normalize(lat, GEO_LAT_MIN, GEO_LAT_MAX, 1u64 << 26);  // clamped to [0, 2^26 - 1]
    let lon_int = normalize(lon, GEO_LON_MIN, GEO_LON_MAX, 1u64 << 26);
    for i in 0..26 {
        hash |= ((lat_int >> i) & 1) << (2 * i);       // latitude bits at EVEN positions
        hash |= ((lon_int >> i) & 1) << (2 * i + 1);   // longitude bits at ODD positions
    }
    Ok(hash)   // 52-bit Morton/Z-order code
}
```

`lon`/`lat` are each linearly normalized into a 26-bit unsigned integer over their respective
valid ranges, then the two 26-bit integers are bit-interleaved (latitude in even bit positions,
longitude in odd) into a single 52-bit value. This is standard Z-order/Morton encoding.

`decode_geohash` (lines 74–89) does the exact inverse bit-extraction, then reconstructs
coordinates as the **center** of the decoded grid cell:
`lat = GEO_LAT_MIN + ((ilat + 0.5) / 2^26) * (GEO_LAT_MAX - GEO_LAT_MIN)` (and the longitude
analogue), each clamped back into range at the end. Because this is lossy quantization, a
decode-then-re-encode round trip returns a coordinate close to — but not bit-identical to — the
original input; the round-trip unit test (`test_geohash_encode_decode_roundtrip`, line 547)
asserts `< 1e-5` closeness, not exact equality.

**Storage format**: `GEOADD`'s handler (`src/connection.rs:17005–17016`) calls `encode_geohash`
and pushes `(hash as f64, member)` into the same `Vec<(f64, Bytes)>` that `ZADD` consumes — the
52-bit integer is reinterpreted (lossily, since `f64` has a 52-bit mantissa, which happens to
exactly fit) as the member's ZSET score. There is no separate geo-indexed storage structure
anywhere in the codebase; a "geo set" *is* a `RudisZSet` (`src/table.rs:147–153`).

### 3.2 `geohash_to_base32` (`src/geo.rs:94–115`) — `GEOHASH` command's string output

```rust
pub fn geohash_to_base32(score: u64) -> String {
    let (lon, lat) = decode_geohash(score);                 // decode using INTERNAL range (-85.05.../85.05...)
    // re-encode using the STANDARD geohash range lat in [-90, 90], lon in [-180, 180]
    for i in 0..26 { hash |= ... }                           // same interleave as §3.1, standard range
    for i in 0..11 {
        let idx = if i == 10 { 0 } else { (hash >> (52 - (i+1)*5)) & 0x1f };
        res.push(GEOHASH_ALPHABET[idx]);
    }
}
```

Matches real Redis's own behavior: the internally-stored geohash uses the narrower Mercator
latitude range, but `GEOHASH`'s textual output must match the public geohash.org standard (full
`[-90, 90]` latitude range), so the coordinate is decoded with the internal range and *re-encoded*
with the standard range before being sliced into 5-bit base32 groups. 11 characters are emitted
(55 bits sliced from the 52-bit value, 5 bits short — Redis pads the missing bits as zero), and the
11th character is **hardcoded to index 0** (`'0'`) rather than reading past the 52-bit value.
Verified against `tests/test_server_e2e.rs:4483–4487`, which asserts the literal strings
`tc1q585vb58` (Palermo) and `tc26yj70z7h` (Catania) for the test fixture's coordinates.

### 3.3 `GeoUnit` (`src/geo.rs:14–49`) — unit parsing and conversion

```rust
pub enum GeoUnit { Meters, Kilometers, Miles, Feet }
```

| Method | `m` | `km` | `mi` | `ft` |
| :--- | :--- | :--- | :--- | :--- |
| `to_meters(val)` | `val` | `val * 1000.0` | `val * 1609.34` | `val * 0.3048` |
| `from_meters(m)` | `m` | `m / 1000.0` | `m / 1609.34` | `m / 0.3048` |

`GeoUnit::parse` (lines 22–30) lowercases the input and matches `"m"|"km"|"mi"|"ft"`, erroring
`"unsupported unit provided. please use M, KM, FT, MI"` otherwise. **The mile constant is
`1609.34`, not the geometrically exact `1609.344`** — this is intentional and matches real Redis's
own `geohash.c` unit table (`GEO_UNIT` array `{1, 1000, 1609.34, 0.3048}`) exactly, not a bug.

### 3.4 `haversine_distance` / `geohash_lat_distance` (`src/geo.rs:118–135`)

```rust
pub fn geohash_lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (lat2d.to_radians() - lat1d.to_radians()).abs()
}

pub fn haversine_distance(lon1d, lat1d, lon2d, lat2d) -> f64 {
    let v = ((lon2r - lon1r) / 2.0).sin();
    if v == 0.0 { return geohash_lat_distance(lat1d, lat2d); }   // same-longitude fast path
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u*u + lat1r.cos() * lat2r.cos() * v*v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().clamp(0.0, 1.0).asin()
}
```

Standard Haversine great-circle distance on a sphere of radius `EARTH_RADIUS_IN_METERS`. The
`a.sqrt().clamp(0.0, 1.0)` guards against floating-point values marginally above `1.0` (which
would make `asin` return `NaN`) for near-antipodal or identical points. `geohash_lat_distance` is
both a same-longitude fast path for `haversine_distance` *and* reused directly by `geo_within_shape`
(§4) for the latitude-only half of a box test.

---

## 4. The Variable-Precision Grid: `GeoHashBits`/`GeoHashArea` (real-Redis port)

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GeoHashBits { pub bits: u64, pub step: u8 }   // geo.rs:138-141

#[derive(Debug, Clone, Copy, Default)]
pub struct GeoHashArea { pub min_lon: f64, pub max_lon: f64, pub min_lat: f64, pub max_lat: f64 }  // geo.rs:195-200
```

Unlike `encode_geohash`'s fixed 26-bit-per-axis hash, `GeoHashBits` carries its own `step` (bits
per axis, 1–26) — a search can be computed at coarser precision than the storage encoding.

### 4.1 `encode_with_step` (`geo.rs:177–192`) / `decode_area` (`geo.rs:202–220`)

`encode_with_step(lon, lat, step)` is the same normalize-and-interleave as `encode_geohash` (§3.1)
but parameterized by `step` instead of hardcoded `26`, and it **clamps** out-of-range coordinates
instead of erroring (`lat.clamp(GEO_LAT_MIN, GEO_LAT_MAX)`). `decode_area` is the inverse: given a
`GeoHashBits`, it reconstructs the **full bounding box** `[min_lon, max_lon] × [min_lat, max_lat]`
of that grid cell (not just its center point, unlike `decode_geohash`).

### 4.2 `geohash_move_x` / `geohash_move_y` (`geo.rs:144–175`) — bit-twiddling neighbor step

```rust
pub fn geohash_move_x(hash: &mut GeoHashBits, d: i8) {
    let mut x = hash.bits & 0xaaaaaaaaaaaaaaaa_u64;   // isolate the interleaved X (longitude) bits
    let y = hash.bits & 0x5555555555555555_u64;       // isolate Y (latitude) bits, untouched
    let zz = 0x5555555555555555_u64 >> (64 - hash.step * 2);
    if d > 0 { x = x.wrapping_add(zz + 1); } else { x = (x | zz).wrapping_sub(zz + 1); }
    x &= 0xaaaaaaaaaaaaaaaa_u64 >> (64 - hash.step * 2);
    hash.bits = x | y;
}
```

This is the classic Redis geohash trick for moving exactly one grid cell in the ±X (or, in
`geohash_move_y`, ±Y) direction **without decoding to coordinates and re-encoding** — it operates
directly on the interleaved bit pattern using carry-propagation through a masked add/subtract
(`0xaaaa...`/`0x5555...` are the alternating-bit masks that isolate even/odd bit positions; `zz` is
a step-width-scaled "all target-axis bits set" value used as a carry seed). `d == 0` is a no-op
short-circuit. This exactly mirrors real Redis's `geohashMoveX`/`geohashMoveY` in `geohash.c`.

### 4.3 `geohash_estimate_steps_by_radius` (`geo.rs:222–240`) — radius → precision

```rust
pub fn geohash_estimate_steps_by_radius(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 { return 26; }                // degenerate point search: max precision
    let mut step = 1;
    let mut rm = range_meters;
    while rm < MERCATOR_MAX { rm *= 2.0; step += 1; }     // double radius until it covers ~half Earth
    step -= 2;
    if lat > 66.0 || lat < -66.0 {                        // high-latitude correction (grid cells
        step -= 1;                                        // get narrower in longitude near the poles)
        if lat > 80.0 || lat < -80.0 { step -= 1; }
    }
    step.clamp(1, 26) as u8
}
```

Larger search radii produce *smaller* `step` (coarser grid cells, fewer/bigger cells to cover the
query); smaller radii produce larger `step` (finer cells). This exactly mirrors real Redis's
`geohashEstimateStepsByRadius`, including the empirically-tuned 66°/80° latitude cutoffs that
compensate for geohash cells becoming longitude-narrower near the poles.

---

## 5. Search Shape, Area Enumeration, and Exact Filtering

### 5.1 `GeoSearchShape` (`geo.rs:243–246`)

```rust
pub enum GeoSearchShape { Radius { radius_m: f64 }, Box { width_m: f64, height_m: f64 } }
```

`Radius` serves `GEORADIUS`/`GEORADIUSBYMEMBER` (always) and `GEOSEARCH ... BYRADIUS`. `Box`
serves `GEOSEARCH ... BYBOX`. Both funnel through the same `calculate_search_areas` →
`execute_geo_query` pipeline (§5.3, §5.4) — there is no separate, unpruned code path for boxes.

### 5.2 `geohash_bounding_box` (`geo.rs:248–276`) — approximate query-sizing box

```rust
pub fn geohash_bounding_box(shape: &GeoSearchShape, center_lon: f64, center_lat: f64) -> [f64; 4] {
    let (width, height) = match shape { Radius{r} => (r, r), Box{w,h} => (w/2.0, h/2.0) };
    let lat_delta = (height / EARTH_RADIUS_IN_METERS).to_degrees();
    let lat_top = (center_lat + lat_delta).to_radians().cos().abs();
    let lat_bot = (center_lat - lat_delta).to_radians().cos().abs();
    // longitude delta computed separately at the box's top and bottom latitude, since a degree of
    // longitude covers fewer meters farther from the equator (cos(lat) correction)
    let long_delta_top = width / EARTH_RADIUS_IN_METERS / lat_top (in degrees), or 180.0 if lat_top ~ 0
    let long_delta_bottom = ... analogous ...
    // southern hemisphere uses the bottom-latitude (wider, since it's farther from the pole) delta
    // for BOTH min_lon and max_lon; northern hemisphere uses the top-latitude delta for both.
    [min_lon, min_lat, max_lon, max_lat]
}
```

This produces an approximate lon/lat bounding box used **only** to decide which grid cell
precision (`step`) and which of the 9 candidate cells actually overlap the query (§5.3) — it is
*not* used to decide final shape membership. The `cos(lat)`-correction trigonometry introduces
some flat-Earth-style approximation error, but since it only controls which (possibly slightly too
many or too few, always re-verified) candidate cells get scanned, this approximation cannot cause
incorrect results — only, in pathological cases, a slightly larger or smaller candidate set.

### 5.3 `calculate_search_areas` (`geo.rs:309–392`) — the real Redis 9-cell algorithm

```rust
pub fn calculate_search_areas(shape, center_lon, center_lat) -> Vec<GeoHashBits> {
    let bounds = geohash_bounding_box(shape, center_lon, center_lat);            // §5.2
    let radius_meters = match shape {                                            // Box -> box diagonal/2
        Radius{r} => r,
        Box{w,h} => ((w/2.0).powi(2) + (h/2.0).powi(2)).sqrt(),
    };
    let mut steps = geohash_estimate_steps_by_radius(radius_meters, center_lat);  // §4.3
    let mut hash = encode_with_step(center_lon, center_lat, steps);              // center cell

    // 1. Compute all 8 neighbor cells at this precision via geohash_move_x/y (§4.2):
    //    N, S, E, W directly; NE/NW/SE/SW as E/W moved N/S (i.e. diagonal = two single-axis moves).

    // 2. If the N/S/E/W neighbor cells' decoded areas (§4.1) don't fully cover the query bounding
    //    box on any side (north_area.max_lat < bounds[3], etc.), and steps > 1, DECREASE steps by
    //    1 and recompute center + all 8 neighbors at the coarser precision. This runs at most once
    //    (not a loop) -- a single one-step coarsening pass.

    // 3. At steps >= 2, compare the (possibly-recomputed) CENTER cell's own area against the query
    //    bounding box on each of the 4 sides independently, and mark the 3 neighbor directions on
    //    the excluded side as invalid (e.g. center.min_lat < bounds[1] (south of the query box)
    //    invalidates south, south_west, AND south_east -- not just south).

    // 4. Return: center cell always first, followed by each of the 8 neighbors still marked valid,
    //    in fixed N,S,E,W,NE,NW,SE,SW order.  Vec::with_capacity(9) -- AT MOST 9 cells, ever.
}
```

This produces **at most 9** `GeoHashBits` cells (center + up to 8 neighbors) regardless of query
size — unlike the previous document's merged-interval approach, the candidate count does not scale
with query size; it only ever shrinks (coarsening the precision once if the immediate neighbors
don't cover the query, then dropping neighbors on sides that overshoot the query box).

### 5.4 `scores_of_geohash_box` (`geo.rs:395–399`) — cell → ZSET score range

```rust
pub fn scores_of_geohash_box(hash: GeoHashBits) -> (f64, f64) {
    let min_bits = hash.bits << (52 - hash.step * 2);
    let max_bits = (hash.bits + 1) << (52 - hash.step * 2);
    (min_bits as f64, max_bits as f64)
}
```

Left-shifts a `step`-precision cell's bit pattern up into the full 52-bit storage-score space,
turning a grid cell into a contiguous `[min_score, max_score)` range directly comparable against
the 52-bit scores `GEOADD` stores (§3.1) — this is what lets `execute_geo_query` (§5.5) issue a
real ZSET range query per cell instead of a full scan.

### 5.5 `geo_within_shape` (`geo.rs:278–307`) — the exact, final membership test

```rust
pub fn geo_within_shape(shape, score, center_lon, center_lat) -> Option<((f64,f64), f64)> {
    let (m_lon, m_lat) = decode_geohash(score);                     // fixed-26-bit decode, §3.1
    match shape {
        Radius { radius_m } => {
            let dist = haversine_distance(center_lon, center_lat, m_lon, m_lat);   // exact
            if dist <= radius_m { Some(((m_lon,m_lat), dist)) } else { None }
        }
        Box { width_m, height_m } => {
            let lat_dist = geohash_lat_distance(m_lat, center_lat);               // exact meridian arc
            if lat_dist > height_m / 2.0 { return None; }
            let lon_dist = haversine_distance(m_lon, m_lat, center_lon, m_lat);   // exact, same-lat row
            if lon_dist > width_m / 2.0 { return None; }
            let dist = haversine_distance(center_lon, center_lat, m_lon, m_lat);  // exact diagonal dist
            Some(((m_lon, m_lat), dist))
        }
    }
}
```

**Both branches are exact spherical-geometry tests**, not flat-Earth approximations — the box
test's lat half uses `geohash_lat_distance` (exact arc length along a meridian) and its lon half
uses `haversine_distance` evaluated at a shared latitude (exact great-circle distance along that
latitude row). The only approximation anywhere in the search path is `geohash_bounding_box` (§5.2),
and that is used exclusively to decide which of ≤9 cells to scan, never to decide membership —
candidate-set pruning (§5.3) therefore **cannot introduce false positives**, only (in principle,
rarely) a slightly larger candidate set than strictly necessary.

---

## 6. `execute_geo_query` (`geo.rs:449–540`) — the shared executor for all 4 search commands

```rust
pub fn execute_geo_query(db, key, center_lon, center_lat, shape, unit,
                          withdist, withhash, withcoord, count, any, asc) -> Vec<GeoItemResult> {
    let mut results = Vec::new();
    let mut seen = hashbrown::HashSet::new();
    let areas = calculate_search_areas(&shape, center_lon, center_lat);   // ≤ 9 cells, §5.3
    let mut last_processed: Option<GeoHashBits> = None;

    for hash_box in areas {
        if last_processed == Some(hash_box) { continue; }   // skip consecutive duplicate cells
        last_processed = Some(hash_box);
        let (min_score, max_score) = scores_of_geohash_box(hash_box);     // §5.4
        let z_opts = ZRangeOpts { by_score: true, min_score, max_score, max_inc: false, with_scores: true, .. };
        for (member, score) in db.zrange(key, &z_opts).unwrap_or_default() {
            if !seen.insert(member.clone()) { continue; }                 // cross-cell de-dup
            if let Some(((m_lon,m_lat), dist_m)) = geo_within_shape(&shape, score as u64, center_lon, center_lat) {
                results.push(GeoItemResult { member, dist, hash, coord, dist_m, score: score as u64 });
                if any && count.is_some_and(|c| results.len() >= c) { break; }   // early-exit (inner)
            }
        }
        if any && count.is_some_and(|c| results.len() >= c) { break; }          // early-exit (outer)
    }

    let effective_asc = if asc.is_none() && count.is_some() && !any { Some(true) } else { asc };
    if let Some(is_asc) = effective_asc {
        results.sort_by(|a,b| a.dist_m.partial_cmp(&b.dist_m)... );              // ASC or DESC by distance
    }
    if let Some(c) = count { results.truncate(c); }
    results
}
```

Key behaviors, verified line-by-line:

- **Range-query, not full scan.** Each of the ≤9 cells issues one `ZRangeOpts{by_score: true, ..}`
  query (§7 below covers the underlying complexity). This is **not** an `O(N)` scan of the whole
  ZSET as the previous document's investigation found for the old implementation — see §7 for the
  corrected complexity analysis, which matters for the common case, with a caveat for the `Small`
  ZSet representation.
- **`max_inc: false`**: cell score ranges are half-open `[min_score, max_score)`, matching
  `scores_of_geohash_box`'s `[min_bits, max_bits)` construction — consistent, no off-by-one gap or
  overlap between adjacent cells' score ranges.
- **De-duplication** happens at two independent levels: `last_processed` skips re-querying an
  *identical* cell (`bits` and `step` both equal to the previous iteration's, which can happen when
  neighbor-direction moves collapse at grid edges), and the `seen: HashSet<Bytes>` guards against
  counting the same *member* twice if it falls inside more than one of the (generally
  non-overlapping, but not guaranteed disjoint at boundaries) cell score ranges.
- **`ANY` short-circuits eagerly**, breaking out of both the inner member loop and the outer cell
  loop the moment `count` is satisfied — results are **not** sorted by distance in this case
  (`effective_asc` stays whatever `asc` was, which is `None` unless the caller passed explicit
  `ASC`/`DESC`), matching real Redis's "first N matches found, unordered" `ANY` semantics.
- **Implicit ASC**: if `COUNT` was given, `ANY` was not, and the caller didn't request `ASC`/`DESC`
  explicitly, the results are sorted ascending by distance before truncation — otherwise truncating
  to `count` without a defined order would arbitrarily drop members depending on cell visitation
  order (fixed N,S,E,W,NE,NW,SE,SW, §5.3) rather than picking the nearest ones.
- **`GeoItemResult`** (`geo.rs:402–409`) always carries `dist_m: f64` and `score: u64` (both
  used internally for sorting/storing) regardless of `WITHDIST`/`WITHHASH`; the `dist`/`hash`/
  `coord` `Option` fields are only populated when the corresponding flag is set, and
  `format_geo_results` (`geo.rs:411–447`) is the single shared reply formatter for all of
  `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`'s `WITHCOORD`/`WITHDIST`/`WITHHASH` combinations —
  one formatter, not three hand-written ones.

### 6.1 Known Bug #1: `STORE`/`STOREDIST`/`GEOSEARCHSTORE` destination can land on the wrong shard

Rudis is thread-per-core: by default (`src/main.rs:164`, `num_shards =
server_config.threads.unwrap_or_else(|| num_cores.min(8))`), the keyspace is partitioned across
multiple shard threads by CRC16 hash slot (`src/router.rs:86–106`, `slot_to_shard`/`target_shard`)
**regardless of whether `CLUSTER` mode is enabled** — this partitioning is an always-on
implementation detail of the threading model, not a user-visible cluster feature.

- `for_each_cmd_key` (`src/connection.rs:3041`, GEO arms at `3173–3191`) correctly registers
  **both** the source `key` and the `store`/`storedist`/`dest` key(s) for `GEORADIUS`,
  `GEORADIUSBYMEMBER`, and `GEOSEARCHSTORE`.
- Those registered keys are used for the `CROSSSLOT` check at `src/connection.rs:5037–5050` — but
  that check is gated behind `if router.cluster_enabled`, which is **`false` by default** in
  standalone mode.
- Separately, **shard dispatch** for these commands uses `cmd_primary_key`
  (`src/connection.rs:2778`, GEO arms at `2917–2924`) and `target_shard_of_cmd`
  (`src/connection.rs:12338`, GEO arms at `12466–12473`) — both of which extract **only the source
  `key`**, ignoring `store`/`storedist`/`dest` entirely.
- The command handler itself then calls `db.zadd(store_dest, ...)` (`src/connection.rs:17158`,
  `17226`, `17381`) directly against the **local** `ShardDb` of whichever shard owns the *source*
  key — there is no cross-shard forwarding anywhere in this path.

**Net effect**: in the default standalone (non-`CLUSTER`) multi-shard configuration — which is the
default on any multi-core machine — if `STORE`/`STOREDIST`'s destination key or
`GEOSEARCHSTORE`'s `dest` hashes to a different shard than the source geo key, the write silently
lands in the source-key shard's local keyspace partition. Any later command addressing that
destination key directly (e.g. `ZRANGE dest ...`) routes by the destination's *own* hash slot to a
different shard, which never received the write — the stored result becomes invisible except via
further `STORE`-based commands lucky enough to also route through the source key's shard. In
`CLUSTER` mode, this is masked by the `CROSSSLOT` rejection (the command never executes at all if
source and dest are on different slots), so the bug is specific to standalone multi-shard
operation. A fix would need either `target_shard_of_cmd`/`cmd_primary_key` to consider the
destination key, or explicit cross-shard redirection of the final `zadd`, mirroring how
`ZRANGESTORE` avoids the equivalent issue by keying its shard routing on `dst` instead of `src`
(`src/connection.rs:2826`, `Command::Zrangestore { dst: key, .. }`).

### 6.2 Other findings from this pass

- **Shared error-message wart**: both `GEORADIUS` and `GEORADIUSBYMEMBER`'s `STORE`/`WITH*`
  incompatibility error is the literal string `"STORE option in GEORADIUS is not compatible with
  WITHDIST, WITHHASH and WITHCOORD options"` (`src/resp.rs:10596-10597` and reused verbatim at
  `10690-10691`) — a `GEORADIUSBYMEMBER ... STORE ... WITHDIST` call gets an error message that
  says "GEORADIUS", not "GEORADIUSBYMEMBER". Cosmetic, matches a real quirk in upstream Redis's own
  error strings, not a Rudis-specific bug.
- **Dead fallback branches**: `connection.rs`'s `Geosearch`/`Geosearchstore` handlers
  (`17289–17294`, `17357–17362`) both have an `else` branch defaulting to
  `GeoSearchShape::Radius { radius_m: 0.0 }` when neither `BYRADIUS` nor `BYBOX` was given — but
  `resp.rs`'s parser for both commands (`10823-10825`, `10958-10960`) already rejects that input at
  parse time with `"exactly one of BYRADIUS and BYBOX can be specified"`, so this branch in
  `connection.rs` is unreachable in practice.
- **`GEOADD`'s own type-check is delegated entirely to `ZADD`**: unlike `GEODIST`/`GEOPOS`/
  `GEOHASH`/`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`/`GEOSEARCHSTORE`, which all do an explicit
  `db.type_of(key)` check up front and return `WRONGTYPE` themselves, `GEOADD`
  (`connection.rs:16998–17035`) has no such check — it relies on `db.zadd`'s own internal
  `WRONGTYPE` detection. Functionally equivalent (same error text), just structurally inconsistent
  with the other seven commands' handlers.
- **No protection against non-geo `ZADD` corrupting a geo set.** Since a geo set is just a `ZSET`,
  nothing stops `ZADD geo:cities not-a-geohash member`; later `GEODIST`/`GEOPOS`/`GEOHASH`/
  `GEOSEARCH` calls against that key will silently decode nonsense coordinates from that score
  rather than erroring — unchanged from the previous pass's finding, still present in the
  current implementation.
- **Coordinate string formatting uses Rust's default `f64` Display**, not a `%.17g`-equivalent
  (`connection.rs:17073-17076` for `GEOPOS`, `geo.rs:440-443` for `WITHCOORD`): `format!("{}", lon)`
  prints the shortest string that round-trips to the same `f64`, which generally differs in digit
  count/trailing digits from real Redis's C `%.17g` formatting for the same underlying value,
  though both represent the same number.

---

## 7. Complexity: Range Query vs. Linear Scan (per-cell, depends on ZSET representation)

`db.zrange(key, &ZRangeOpts{by_score: true, ..})` (`src/table.rs:9690–9711`) dispatches to
`RudisZSet::range` (`src/table.rs:297–409`), whose `by_score` branch (`349–380`) differs by
representation:

```rust
RudisZSet::Small(v) => Box::new(v.iter().filter(filter_fn)),                 // O(n) linear scan
RudisZSet::Full { tree, .. } => Box::new(
    tree.range((Bound::Included((OrderedScore(min), Bytes::new())), Bound::Unbounded))
        .take_while(|item| if max_inc { item.0.0 <= max } else { item.0.0 < max })
        .filter(filter_fn)
),   // BTreeSet::range -> O(log n) seek + O(k) walk, k = matches in range
```

- **`Full`** (`BTreeSet<(OrderedScore, Bytes)>`, promoted past the small-collection threshold —
  see Component 05, `05_storage_engine.md`): a true ordered range seek, `O(log n)` to find the
  start plus `O(k)` to walk the `k` members whose score falls in `[min_score, max_score)`.
- **`Small`** (`Vec<(OrderedScore, Bytes)>`, below the threshold): a linear filter over the whole
  (small, bounded) vector — acceptable at that scale, same as every other ZSET range operation on
  `Small` sets.

Since `execute_geo_query` issues at most 9 such range queries per search command (§5.3, §6), total
search cost for a `Full`-backed geo set is roughly `O(9 * (log n + k))` where `k` is the number of
members actually inside the ≤9 candidate cells — **not** `O(n)` over the whole set. This
supersedes the earlier document's finding of an `O(N)` full scan, which described the
now-removed `geohash_search_ranges`-era implementation; that finding no longer applies to the
current `calculate_search_areas`/`execute_geo_query` pipeline for `Full`-backed sets. It still
applies, as an accepted/expected cost (same as any other ZSET op at that scale), to `Small`-backed
sets.

---

## 8. Command-by-Command Reference

All eight commands share `WRONGTYPE Operation against a key holding the wrong kind of value` when
the key exists and isn't a ZSET (except `GEOADD`, §6.2), and all are keyed/sharded by their single
primary geo key (`key` for all except `GEOSEARCHSTORE`'s pair; see §6.1 for the destination-key
caveat).

### 8.1 `GEOADD key [NX|XX] [CH] lon lat member [lon lat member ...]`

- Parser: `resp.rs:10419–10474`. Accepts the `NX`/`XX`/`CH` flag prefix (only one of NX/XX; `nx &&
  xx` is a `syntax error`), then requires the remaining args to be a non-empty multiple of 3.
  Validates every `(lon, lat)` pair against `GEO_LON_MIN/MAX`/`GEO_LAT_MIN/MAX` **at parse time**
  (error: `"invalid longitude,latitude pair {lon:.6},{lat:.6}"`).
- Handler: `connection.rs:16998–17035`. Re-encodes every `(lon, lat)` via `encode_geohash` (§3.1)
  — a **second**, redundant range check happens here too, returning a RESP error (note: lowercase
  `"ERR invalid longitude,latitude pair ..."` message text, distinct from the parser's version)
  if somehow still out of range. Builds `ZAddFlags { nx, xx, ch, gt: false, lt: false, incr: false
  }` and delegates entirely to `db.zadd` — reply is the ZADD-standard added-count integer.

### 8.2 `GEODIST key member1 member2 [unit]`

- Parser: `resp.rs:10475–10490`. `unit` optional, defaults handled downstream (not in the parse).
- Handler: `connection.rs:17036–17060`. Explicit `WRONGTYPE` guard. Two independent `db.zscore`
  calls; if **either** misses, replies `$-1\r\n` (nil bulk) — a member with no score (never added,
  or whose score happens to decode to garbage) is indistinguishable from a wrongly-typed lookup
  here; only an actual `zscore` miss triggers the nil reply. On both hits: `decode_geohash` both
  scores, `haversine_distance`, convert via `unit.from_meters` if a unit was given (default is
  meters, i.e. no conversion), format to 4 decimal places (`"{:.4}"`).

### 8.3 `GEOPOS key [member ...]`

- Parser: `resp.rs:10491–10498`. `members` may be empty (`args[2..]`, not validated non-empty).
- Handler: `connection.rs:17061–17084`. Per member: `zscore` hit → `decode_geohash` → 2-element
  bulk-string array `[lon, lat]` (Rust default `Display` formatting, §6.2); miss → `*-1\r\n`.

### 8.4 `GEOHASH key [member ...]`

- Parser: `resp.rs:10499–10506`.
- Handler: `connection.rs:17085–17104`. Per member: `zscore` hit → `geohash_to_base32` (§3.2) → 11
  character bulk string; miss → `$-1\r\n`.

### 8.5 `GEORADIUS key lon lat radius unit [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT n [ANY]] [ASC|DESC] [STORE key | STOREDIST key]` (and `GEORADIUS_RO`)

- Parser: `resp.rs:10507–10614`. `is_ro = args[0] == "GEORADIUS_RO"`; when `is_ro`, `STORE`/
  `STOREDIST` tokens are rejected as `syntax error` rather than silently ignored
  (`10576`, `10583`). `COUNT n` optionally followed immediately by `ANY` sets both `count` and
  `any` in one option. Mutual-exclusion checks: `ANY` requires `COUNT`; `STORE`/`STOREDIST` are
  incompatible with any of `WITHDIST`/`WITHHASH`/`WITHCOORD` (§6.2's shared-message wart).
- Handler: `connection.rs:17105–17166`. `WRONGTYPE` guard; if the key doesn't exist: `del`s the
  store destination and replies `:0` if `STORE`/`STOREDIST` was requested, else replies `*0\r\n`.
  Otherwise builds `GeoSearchShape::Radius { radius_m: unit.to_meters(radius) }` and calls
  `execute_geo_query` (§6). If `STORE`/`STOREDIST`: deletes the destination key first, then
  `zadd`s results in with score = `unit.from_meters(dist_m)` for `STOREDIST` or the raw geohash
  `score` for `STORE` (i.e. `STOREDIST` stores a *distance-ranked* ZSET, `STORE` stores a
  *geohash-ranked* one re-usable as a geo set) — `ZAddFlags::default()` (no NX/XX/CH/GT/LT/INCR).
  Otherwise formats via `format_geo_results` (§6).

### 8.6 `GEORADIUSBYMEMBER key member radius unit [...]` (and `GEORADIUSBYMEMBER_RO`)

- Parser: `resp.rs:10615–10707`, identical option grammar to `GEORADIUS` minus the explicit
  `lon`/`lat` (uses `member` instead).
- Handler: `connection.rs:17167–17234`. Same shape as `GEORADIUS` except the center point comes
  from `db.zscore(key, member)` → `decode_geohash`; a missing/undecodable member replies
  `-ERR could not decode requested zset member\r\n` (not `WRONGTYPE`, not nil).

### 8.7 `GEOSEARCH key (FROMMEMBER member | FROMLONLAT lon lat) (BYRADIUS r unit | BYBOX w h unit) [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]`

- Parser: `resp.rs:10708–10842`. Enforces **exactly one** of `FROMMEMBER`/`FROMLONLAT` and
  **exactly one** of `BYRADIUS`/`BYBOX` (both required, both mutually exclusive within their pair)
  — errors use real Redis's exact wording (`"exactly one of FROMMEMBER or FROMLONLAT can be
  specified for GEOSEARCH"`, etc.).
- Handler: `connection.rs:17235–17302`. No `STORE` option (that's the separate `GEOSEARCHSTORE`
  command, §8.8). Resolves center via `from_lonlat` directly or `from_member`'s `zscore`+decode
  (missing member → `-ERR could not decode requested zset member\r\n`, same as §8.6). Builds
  `GeoSearchShape::Radius` or `::Box` depending on which of `by_radius`/`by_box` is `Some`
  (`unit.to_meters` applied to the raw radius/width/height first) — the `else` fallback to
  `Radius{radius_m: 0.0}` (§6.2) is unreachable given the parser's exactly-one-required check.
  Calls `execute_geo_query`, formats via `format_geo_results`.

### 8.8 `GEOSEARCHSTORE dest key (FROMMEMBER|FROMLONLAT) (BYRADIUS|BYBOX) [ASC|DESC] [COUNT n [ANY]] [STOREDIST]`

- Parser: `resp.rs:10843–~10975`. Same `FROMMEMBER`/`FROMLONLAT` and `BYRADIUS`/`BYBOX`
  exactly-one enforcement as `GEOSEARCH`; explicitly **rejects** `WITHCOORD`/`WITHDIST`/`WITHHASH`
  (`"GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options"`) since those
  options are meaningless for a command whose output is a stored ZSET, not a reply array.
- Handler: `connection.rs:17303–17386`. If source `key` doesn't exist: `del`s `dest`, replies `:0`.
  Otherwise resolves the center point identically to `GEOSEARCH`, calls `execute_geo_query` with
  `withdist=withhash=withcoord=false` (irrelevant for storage), then stores exactly like
  `GEORADIUS ... STORE`/`STOREDIST` (§8.5): `del(dest)` first, then `zadd` with either the raw
  geohash score or `unit.from_meters(dist_m)` depending on `storedist`. **Shares Known Bug #1**
  (§6.1): routes/shards on `key`, not `dest`.

---

## 9. Cross-Component Interactions

- **`src/table.rs`** (Component 05): every geo command is built entirely on `RudisTable`'s
  `zadd`/`zscore`/`zrange` — there is no geo-specific storage anywhere; a "geo set" *is* a
  `RudisZSet`, and `execute_geo_query`'s per-cell queries (§6) rely directly on
  `ZRangeOpts { by_score: true, .. }` being a real ordered range query (§7) against that ZSET's
  `Full` representation.
- **`src/connection.rs`** (Component 02): owns every `Command::Geo*` match arm (`geo.rs` itself
  contains no command dispatch, only the math/formatting helpers those arms call, plus
  `execute_geo_query` which takes `&mut ShardDb` directly); routes all of them through
  `cmd_primary_key`/`target_shard_of_cmd` by the single geo `key` field (§6.1 documents the
  resulting destination-key shard bug for the three `STORE`-capable commands).
- **`src/resp.rs`** (Component 03): parses `GEOADD`/`GEODIST`/etc.'s arguments (including unit
  strings `m`/`km`/`mi`/`ft` and `BYRADIUS`/`BYBOX`/`ASC`/`DESC`/`WITHCOORD`/`WITHDIST`/
  `WITHHASH`/`ANY` option flags) into the `Command::Geo*` variants this file's helpers consume; also
  performs the bulk of coordinate range validation (duplicated, more defensively, in `geo.rs`
  itself for `GEOADD`'s `encode_geohash` call).
- **`src/router.rs`** (Component 04): `target_shard`/`slot_to_shard` define the always-on CRC16
  hash-slot-to-shard partitioning (§6.1) that the geo `STORE` commands' destination keys can
  silently violate when `CLUSTER` mode is off.

---

## 10. Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Longitude is bound to `[-180, 180]`, latitude to `[-85.05112878, 85.05112878]`
  (`GEO_LAT_MIN`/`GEO_LAT_MAX`, `geo.rs:5-6`) — not `[-90, 90]`; a `GEOADD` outside this range
  returns `invalid longitude,latitude pair` (checked twice: once in `resp.rs` at parse time, once
  redundantly in `geo.rs::encode_geohash`).
* **Gotcha 2**: `GEOSEARCH`/`GEOSEARCHSTORE` support `BYRADIUS` and `BYBOX`, both served by the
  same `calculate_search_areas`/`execute_geo_query` pruning path (§5.3/§6) — `BYBOX` is not a
  separate, unpruned code path, and its final membership test is exact spherical geometry, not a
  flat-Earth approximation (§5.5).
* **Gotcha 3**: The Haversine formula uses the fixed spherical constant
  `EARTH_RADIUS_IN_METERS = 6372797.560856` (`geo.rs:3`) — the same literal real Redis hardcodes,
  not WGS84's 6378137.0m semi-major axis or a simple mean-radius approximation.
* **Gotcha 4**: The mile conversion factor is `1609.34` (`geo.rs:36,45`), matching real Redis's own
  slightly-non-exact constant — don't "fix" it to the geometrically precise `1609.344` without
  checking Redis compatibility test expectations first.
* **Gotcha 5 (cross-shard STORE bug, §6.1)**: `GEORADIUS ... STORE dest` / `STOREDIST dest` /
  `GEOSEARCHSTORE dest ...` route and shard purely by the *source* geo key. In standalone
  multi-shard mode (the default on multi-core hosts), if `dest` hashes to a different shard than
  the source key, the write is silently invisible to subsequent direct lookups of `dest`. Reproduce
  with `num_shards > 1`, `CLUSTER` disabled, and a `dest` key name chosen (e.g. by brute-force
  hashing) to land on a different shard than the source.

### How to Verify Changes

```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run geo.rs's own unit tests (encode/decode round-trip, Haversine, unit conversion)
cargo test --lib geo:: -- --test-threads=1

# 4. Run the e2e geo integration test
cargo test --test test_server_e2e test_geospatial_engine_e2e

# 5. Run real Redis's own geo.tcl compatibility suite (part of CORE_SUITES)
#    args are: PORT SUITE_ARG CLIENTS
./scripts/run_redis_test_suite.sh 16379 unit/geo
```
