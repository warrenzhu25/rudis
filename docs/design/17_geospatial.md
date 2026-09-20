# Component 17: Geospatial Commands (Design)

## Component 17: Geospatial Commands

> **Source Files**: ``src/geo.rs``


---

### 1. Architectural Purpose & Scope

`src/geo.rs` is pure math and reply-formatting — it owns **no storage of its own**. Every
`GEOADD`/`GEODIST`/`GEOPOS`/`GEOHASH`/`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` command is
implemented directly in `src/connection.rs` on top of the existing sorted-set (`RudisZSet`,
Component 05) API — `GEOADD` is a `ZADD` whose "score" is a 52-bit interleaved geohash encoding
of (longitude, latitude), and every other geo command decodes that score back into
coordinates. This is architecturally identical to how real Redis implements its own `GEO*`
command family as a thin layer over `ZSET`, and it means `ZRANGE`/`ZSCORE`/any other ZSET
command works unmodified against a "geo set" key too — a real compatibility feature and a real
footgun (an arbitrary `ZADD` against a geo key can insert a member with a score that isn't a
valid geohash at all, and nothing rejects it).

---

---

### 2. Key Invariants & Concurrency Constraints

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

---

---

### 6. Performance Characteristics

- **`GEOADD`/`GEODIST`/`GEOPOS` are O(1)-ish**, bounded by the underlying `ZADD`/`ZSCORE` cost
  (Component 05) plus a fixed amount of bit-interleaving/Haversine math — no scan involved.
- **`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` are all O(N) in the size of the geo set**
  (§4.4), not O(matches) or O(log N + matches) the way a geohash-neighborhood-aware
  implementation would be — a large geo set with a small-radius search still decodes and
  distance-checks every member.
- **No caching of decoded coordinates** — every scan re-runs `decode_geohash` (cheap bit
  extraction) per member per call; not a measurable cost relative to the Haversine
  trigonometry, which dominates.

---
