# Component 17: Geospatial Commands (Implementation)

> **Source Files**: `src/geo.rs`


---

### 3. Component Architecture & Data Structures

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

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `encode_geohash`/`decode_geohash`: bit interleaving, not a lookup table

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

#### 4.2 `GEOADD`'s real implementation: encode, then delegate entirely to `ZADD`

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

#### 4.3 `GEODIST`: two `zscore` lookups, decode, Haversine, unit conversion

Straightforward: `db.zscore(key, m1)`/`db.zscore(key, m2)` fetch the two raw geohash scores,
`decode_geohash` turns each back into coordinates, `haversine_distance` computes meters, and
the requested `GeoUnit` (default meters) converts the final figure — a member with no score
(never added, or added via a non-geo `ZADD` with a score that happens to decode to garbage
coordinates) returns `$-1\r\n` only if the `zscore` lookup itself misses, not if the decoded
"coordinates" are nonsensical.

#### 4.4 `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`: three independent, near-identical brute-force scans

All three commands share the exact same shape, implemented as three separate, hand-duplicated
blocks (not a shared helper function):

```rust
let z_opts = crate::table::ZRangeOpts { start: 0, stop: -1, with_scores: true, ..Default::default() };
if let Ok(pairs) = db.zrange(key, &z_opts) {
    for (member, score) in pairs {
        let (m_lon, m_lat) = crate::geo::decode_geohash(score as u64);
        let dist = crate::geo::haversine_distance(center_lon, center_lat, m_lon, m_lat);
        if dist <= radius_meters { /* collect into results */ }
    }
}
```

**This is a full O(N) scan of every member in the geo set for every search**, decoding and
computing Haversine distance against each one, regardless of the search radius or how many
members actually match. Real Redis's own `GEORADIUS`/`GEOSEARCH` implementation instead uses
the sorted-by-geohash structure of the underlying skiplist to narrow the scan to a small
neighborhood of 52-bit-geohash-adjacent score ranges (the "9 neighboring geohash cells"
technique) before doing exact distance filtering — this implementation does none of that
narrowing; it always decodes and distance-checks the entire set. `GEOSEARCH`'s `BY BOX` variant
additionally approximates a box as `dlat_m = Δlat° × 111,320` /
`dlon_m = Δlon° × 111,320 × cos(center_lat)` — a flat-Earth-locally approximation, not exact
geodesic box math, reasonable at the box sizes these commands are typically used for but not
precise at very large box dimensions.

---

### 5. Cross-Component Interactions

- **`src/table.rs`** (Component 05): every geo command is built entirely on `RudisTable`'s
  `zadd`/`zscore`/`zrange` — there is no geo-specific storage anywhere; a "geo set" *is* a
  `RudisZSet`.
- **`src/connection.rs`** (Component 02): owns every `Command::Geo*` match arm (`geo.rs` itself
  contains no command dispatch, only the math/formatting helpers those arms call); routes all
  of them through the standard `target_shard_of_cmd` keyed-command path (§2.4).
- **`src/resp.rs`** (Component 03): parses `GEOADD`/`GEODIST`/etc.'s arguments (including unit
  strings `m`/`km`/`mi`/`ft` and `BYRADIUS`/`BYBOX`/`ASC`/`DESC`/`WITHCOORD`/`WITHDIST`/
  `WITHHASH` option flags) into the `Command::Geo*` variants this file's helpers consume.

---

### 7. Future Improvements

- **Medium — narrow `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`'s scan using geohash-neighborhood pruning instead of a full O(N) scan (§4.4).** Since scores are already geohash-ordered when read via a range on the sorted structure, computing the target radius/box's covering geohash cell(s) and querying only score ranges near them (real Redis's approach) would turn this into roughly O(log N + matches) instead of O(N) — the highest-value fix here for any geo set large enough to matter.
- **Low — factor `GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`'s shared scan-and-filter logic into one helper (§4.4).** Three independently hand-written copies of the same loop is a real duplication-drift risk — a bugfix or the geohash-neighborhood optimization above would otherwise need to be applied three times.
- **Low — replace the `BY BOX` flat-Earth approximation with exact geodesic box math** (§4.4) if very large box searches (spanning enough latitude/longitude to make the flat approximation's error non-negligible) become a real use case — not a concern at typical city-scale search radii.
- **Low — reject non-geo `ZADD`s against a key already used as a geo set, or document the compatibility footgun explicitly (§1)** — since a geo set is just a `ZSET`, nothing stops an ordinary `ZADD member not-a-geohash-score` from corrupting later `GEODIST`/`GEOPOS`/`GEOSEARCH` calls against that key with silently-nonsensical decoded coordinates.

---
---
