use bytes::Bytes;

pub const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
pub const MERCATOR_MAX: f64 = 20037726.37;
pub const GEO_LAT_MIN: f64 = -85.05112878;
pub const GEO_LAT_MAX: f64 = 85.05112878;
pub const GEO_LON_MIN: f64 = -180.0;
pub const GEO_LON_MAX: f64 = 180.0;
pub const GEO_STEP_MAX: u8 = 26;

const GEOHASH_ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoUnit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl GeoUnit {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "m" => Ok(GeoUnit::Meters),
            "km" => Ok(GeoUnit::Kilometers),
            "mi" => Ok(GeoUnit::Miles),
            "ft" => Ok(GeoUnit::Feet),
            _ => Err("unsupported unit provided. please use M, KM, FT, MI".to_string()),
        }
    }

    pub fn to_meters(&self, val: f64) -> f64 {
        match self {
            GeoUnit::Meters => val,
            GeoUnit::Kilometers => val * 1000.0,
            GeoUnit::Miles => val * 1609.34,
            GeoUnit::Feet => val * 0.3048,
        }
    }

    pub fn from_meters(&self, meters: f64) -> f64 {
        match self {
            GeoUnit::Meters => meters,
            GeoUnit::Kilometers => meters / 1000.0,
            GeoUnit::Miles => meters / 1609.34,
            GeoUnit::Feet => meters / 0.3048,
        }
    }
}

/// Encodes (longitude, latitude) into Redis 52-bit integer geohash.
/// Latitude bits are in even positions (0, 2, 4...) and longitude bits are in odd positions (1, 3, 5...).
pub fn encode_geohash(lon: f64, lat: f64) -> Result<u64, String> {
    if lon < GEO_LON_MIN || lon > GEO_LON_MAX || lat < GEO_LAT_MIN || lat > GEO_LAT_MAX {
        return Err(format!("ERR invalid longitude,latitude pair {:.6},{:.6}", lon, lat));
    }

    let lat_offset = ((lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN)) * ((1u64 << 26) as f64);
    let lon_offset = ((lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN)) * ((1u64 << 26) as f64);
    let lat_int = (lat_offset as u64).min((1u64 << 26) - 1);
    let lon_int = (lon_offset as u64).min((1u64 << 26) - 1);

    let mut hash = 0u64;
    for i in 0..26 {
        let bit_lat = (lat_int >> i) & 1;
        let bit_lon = (lon_int >> i) & 1;
        hash |= bit_lat << (2 * i);
        hash |= bit_lon << (2 * i + 1);
    }
    Ok(hash)
}

/// Decodes Redis 52-bit integer geohash into (longitude, latitude).
pub fn decode_geohash(hash: u64) -> (f64, f64) {
    let mut ilat = 0u64;
    let mut ilon = 0u64;

    for i in 0..26 {
        let bit_lat = (hash >> (2 * i)) & 1;
        let bit_lon = (hash >> (2 * i + 1)) & 1;
        ilat |= bit_lat << i;
        ilon |= bit_lon << i;
    }

    let max_val = (1u64 << 26) as f64;
    let lat = GEO_LAT_MIN + ((ilat as f64 + 0.5) / max_val) * (GEO_LAT_MAX - GEO_LAT_MIN);
    let lon = GEO_LON_MIN + ((ilon as f64 + 0.5) / max_val) * (GEO_LON_MAX - GEO_LON_MIN);
    (lon.clamp(GEO_LON_MIN, GEO_LON_MAX), lat.clamp(GEO_LAT_MIN, GEO_LAT_MAX))
}

/// Formats a 52-bit geohash into an 11-character Redis-standard base32 string.
/// In Redis, the internal geohash (with latitude in [-85.05112878, 85.05112878]) is decoded
/// and re-encoded using standard geohash latitude range [-90.0, 90.0] before emitting base32.
pub fn geohash_to_base32(score: u64) -> String {
    let (lon, lat) = decode_geohash(score);
    let lat_offset = (((lat - (-90.0)) / 180.0) * ((1u64 << 26) as f64)).clamp(0.0, ((1u64 << 26) - 1) as f64) as u64;
    let lon_offset = (((lon - (-180.0)) / 360.0) * ((1u64 << 26) as f64)).clamp(0.0, ((1u64 << 26) - 1) as f64) as u64;
    let mut hash = 0u64;
    for i in 0..26 {
        let bit_lat = (lat_offset >> i) & 1;
        let bit_lon = (lon_offset >> i) & 1;
        hash |= bit_lat << (2 * i);
        hash |= bit_lon << (2 * i + 1);
    }
    let mut res = Vec::with_capacity(11);
    for i in 0..11 {
        let idx = if i == 10 {
            0
        } else {
            ((hash >> (52 - (i + 1) * 5)) & 0x1f) as usize
        };
        res.push(GEOHASH_ALPHABET[idx]);
    }
    String::from_utf8(res).unwrap_or_default()
}

#[inline]
pub fn geohash_lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (lat2d.to_radians() - lat1d.to_radians()).abs()
}

/// Computes Great-Circle (Haversine) distance between two points in meters.
pub fn haversine_distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lat1r = lat1d.to_radians();
    let lat2r = lat2d.to_radians();
    let lon1r = lon1d.to_radians();
    let lon2r = lon2d.to_radians();
    let v = ((lon2r - lon1r) / 2.0).sin();
    if v == 0.0 {
        return geohash_lat_distance(lat1d, lat2d);
    }
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().clamp(0.0, 1.0).asin()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

#[inline]
pub fn geohash_move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }
    let mut x = hash.bits & 0xaaaaaaaaaaaaaaaa_u64;
    let y = hash.bits & 0x5555555555555555_u64;
    let zz = 0x5555555555555555_u64 >> (64 - hash.step * 2);
    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x = (x | zz).wrapping_sub(zz + 1);
    }
    x &= 0xaaaaaaaaaaaaaaaa_u64 >> (64 - hash.step * 2);
    hash.bits = x | y;
}

#[inline]
pub fn geohash_move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }
    let x = hash.bits & 0xaaaaaaaaaaaaaaaa_u64;
    let mut y = hash.bits & 0x5555555555555555_u64;
    let zz = 0xaaaaaaaaaaaaaaaa_u64 >> (64 - hash.step * 2);
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y = (y | zz).wrapping_sub(zz + 1);
    }
    y &= 0x5555555555555555_u64 >> (64 - hash.step * 2);
    hash.bits = x | y;
}

pub fn encode_with_step(lon: f64, lat: f64, step: u8) -> GeoHashBits {
    let lat_clamped = lat.clamp(GEO_LAT_MIN, GEO_LAT_MAX);
    let lon_clamped = lon.clamp(GEO_LON_MIN, GEO_LON_MAX);
    let lat_offset = ((lat_clamped - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN)) * ((1u64 << step) as f64);
    let lon_offset = ((lon_clamped - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN)) * ((1u64 << step) as f64);
    let lat_int = (lat_offset as u64).min((1u64 << step) - 1);
    let lon_int = (lon_offset as u64).min((1u64 << step) - 1);
    let mut bits = 0u64;
    for i in 0..step {
        let bit_lat = (lat_int >> i) & 1;
        let bit_lon = (lon_int >> i) & 1;
        bits |= bit_lat << (2 * i);
        bits |= bit_lon << (2 * i + 1);
    }
    GeoHashBits { bits, step }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GeoHashArea {
    pub min_lon: f64,
    pub max_lon: f64,
    pub min_lat: f64,
    pub max_lat: f64,
}

pub fn decode_area(hash: GeoHashBits) -> GeoHashArea {
    let mut ilat = 0u64;
    let mut ilon = 0u64;
    for i in 0..hash.step {
        let bit_lat = (hash.bits >> (2 * i)) & 1;
        let bit_lon = (hash.bits >> (2 * i + 1)) & 1;
        ilat |= bit_lat << i;
        ilon |= bit_lon << i;
    }
    let total = (1u64 << hash.step) as f64;
    let lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
    let lon_scale = GEO_LON_MAX - GEO_LON_MIN;
    GeoHashArea {
        min_lat: GEO_LAT_MIN + (ilat as f64 / total) * lat_scale,
        max_lat: GEO_LAT_MIN + ((ilat + 1) as f64 / total) * lat_scale,
        min_lon: GEO_LON_MIN + (ilon as f64 / total) * lon_scale,
        max_lon: GEO_LON_MIN + ((ilon + 1) as f64 / total) * lon_scale,
    }
}

pub fn geohash_estimate_steps_by_radius(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut step = 1;
    let mut rm = range_meters;
    while rm < MERCATOR_MAX {
        rm *= 2.0;
        step += 1;
    }
    step -= 2;
    if lat > 66.0 || lat < -66.0 {
        step -= 1;
        if lat > 80.0 || lat < -80.0 {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

#[derive(Debug, Clone, Copy)]
pub enum GeoSearchShape {
    Radius { radius_m: f64 },
    Box { width_m: f64, height_m: f64 },
}

pub fn geohash_bounding_box(
    shape: &GeoSearchShape,
    center_lon: f64,
    center_lat: f64,
) -> [f64; 4] {
    let (width, height) = match *shape {
        GeoSearchShape::Radius { radius_m } => (radius_m, radius_m),
        GeoSearchShape::Box { width_m, height_m } => (width_m / 2.0, height_m / 2.0),
    };
    let lat_delta = (height / EARTH_RADIUS_IN_METERS).to_degrees();
    let lat_top = (center_lat + lat_delta).to_radians().cos().abs();
    let lat_bot = (center_lat - lat_delta).to_radians().cos().abs();
    let long_delta_top = if lat_top > 1e-6 {
        (width / EARTH_RADIUS_IN_METERS / lat_top).to_degrees()
    } else {
        180.0
    };
    let long_delta_bottom = if lat_bot > 1e-6 {
        (width / EARTH_RADIUS_IN_METERS / lat_bot).to_degrees()
    } else {
        180.0
    };
    let southern = center_lat < 0.0;
    let min_lon = if southern { center_lon - long_delta_bottom } else { center_lon - long_delta_top };
    let max_lon = if southern { center_lon + long_delta_bottom } else { center_lon + long_delta_top };
    let min_lat = center_lat - lat_delta;
    let max_lat = center_lat + lat_delta;
    [min_lon, min_lat, max_lon, max_lat]
}

pub fn geo_within_shape(
    shape: &GeoSearchShape,
    score: u64,
    center_lon: f64,
    center_lat: f64,
) -> Option<((f64, f64), f64)> {
    let (m_lon, m_lat) = decode_geohash(score);
    match *shape {
        GeoSearchShape::Radius { radius_m } => {
            let dist = haversine_distance(center_lon, center_lat, m_lon, m_lat);
            if dist <= radius_m {
                Some(((m_lon, m_lat), dist))
            } else {
                None
            }
        }
        GeoSearchShape::Box { width_m, height_m } => {
            let lat_dist = geohash_lat_distance(m_lat, center_lat);
            if lat_dist > height_m / 2.0 {
                return None;
            }
            let lon_dist = haversine_distance(m_lon, m_lat, center_lon, m_lat);
            if lon_dist > width_m / 2.0 {
                return None;
            }
            let dist = haversine_distance(center_lon, center_lat, m_lon, m_lat);
            Some(((m_lon, m_lat), dist))
        }
    }
}

pub fn calculate_search_areas(
    shape: &GeoSearchShape,
    center_lon: f64,
    center_lat: f64,
) -> Vec<GeoHashBits> {
    let bounds = geohash_bounding_box(shape, center_lon, center_lat);
    let radius_meters = match *shape {
        GeoSearchShape::Radius { radius_m } => radius_m,
        GeoSearchShape::Box { width_m, height_m } => {
            ((width_m / 2.0).powi(2) + (height_m / 2.0).powi(2)).sqrt()
        }
    };
    let mut steps = geohash_estimate_steps_by_radius(radius_meters, center_lat);
    let mut hash = encode_with_step(center_lon, center_lat, steps);

    let get_neighbors = |h: GeoHashBits| -> [GeoHashBits; 8] {
        let mut n = h; geohash_move_y(&mut n, 1);
        let mut s = h; geohash_move_y(&mut s, -1);
        let mut e = h; geohash_move_x(&mut e, 1);
        let mut w = h; geohash_move_x(&mut w, -1);
        let mut ne = e; geohash_move_y(&mut ne, 1);
        let mut nw = w; geohash_move_y(&mut nw, 1);
        let mut se = e; geohash_move_y(&mut se, -1);
        let mut sw = w; geohash_move_y(&mut sw, -1);
        [n, s, e, w, ne, nw, se, sw]
    };

    let mut neigh = get_neighbors(hash);
    let mut area = decode_area(hash);

    let north_area = decode_area(neigh[0]);
    let south_area = decode_area(neigh[1]);
    let east_area = decode_area(neigh[2]);
    let west_area = decode_area(neigh[3]);

    let mut decrease_step = false;
    if north_area.max_lat < bounds[3]
        || south_area.min_lat > bounds[1]
        || east_area.max_lon < bounds[2]
        || west_area.min_lon > bounds[0]
    {
        decrease_step = true;
    }

    if steps > 1 && decrease_step {
        steps -= 1;
        hash = encode_with_step(center_lon, center_lat, steps);
        neigh = get_neighbors(hash);
        area = decode_area(hash);
    }

    let mut n_valid = [true; 8];
    if steps >= 2 {
        if area.min_lat < bounds[1] {
            n_valid[1] = false; // south
            n_valid[7] = false; // south_west
            n_valid[6] = false; // south_east
        }
        if area.max_lat > bounds[3] {
            n_valid[0] = false; // north
            n_valid[4] = false; // north_east
            n_valid[5] = false; // north_west
        }
        if area.min_lon < bounds[0] {
            n_valid[3] = false; // west
            n_valid[7] = false; // south_west
            n_valid[5] = false; // north_west
        }
        if area.max_lon > bounds[2] {
            n_valid[2] = false; // east
            n_valid[6] = false; // south_east
            n_valid[4] = false; // north_east
        }
    }

    let mut result = Vec::with_capacity(9);
    result.push(hash);
    for i in 0..8 {
        if n_valid[i] {
            result.push(neigh[i]);
        }
    }
    result
}

#[inline]
pub fn scores_of_geohash_box(hash: GeoHashBits) -> (f64, f64) {
    let min_bits = hash.bits << (52 - hash.step * 2);
    let max_bits = (hash.bits + 1) << (52 - hash.step * 2);
    (min_bits as f64, max_bits as f64)
}

#[derive(Debug, Clone)]
pub struct GeoItemResult {
    pub member: Bytes,
    pub dist: Option<f64>,
    pub hash: Option<u64>,
    pub coord: Option<(f64, f64)>,
    pub dist_m: f64,
    pub score: u64,
}

pub fn format_geo_results(out: &mut Vec<u8>, results: &[GeoItemResult], has_options: bool) {
    out.extend_from_slice(format!("*{}\r\n", results.len()).as_bytes());
    for item in results {
        if !has_options {
            crate::connection::write_resp_bulk(out, &item.member);
        } else {
            let mut sub_count = 1;
            if item.dist.is_some() {
                sub_count += 1;
            }
            if item.hash.is_some() {
                sub_count += 1;
            }
            if item.coord.is_some() {
                sub_count += 1;
            }

            out.extend_from_slice(format!("*{}\r\n", sub_count).as_bytes());
            crate::connection::write_resp_bulk(out, &item.member);

            if let Some(d) = item.dist {
                let d_str = format!("{:.4}", d);
                crate::connection::write_resp_bulk(out, d_str.as_bytes());
            }
            if let Some(h) = item.hash {
                crate::connection::write_resp_integer(out, h as i64);
            }
            if let Some((lon, lat)) = item.coord {
                out.extend_from_slice(b"*2\r\n");
                let lon_str = format!("{}", lon);
                let lat_str = format!("{}", lat);
                crate::connection::write_resp_bulk(out, lon_str.as_bytes());
                crate::connection::write_resp_bulk(out, lat_str.as_bytes());
            }
        }
    }
}

pub fn execute_geo_query(
    db: &mut crate::shard::ShardDb,
    key: &[u8],
    center_lon: f64,
    center_lat: f64,
    shape: GeoSearchShape,
    unit: GeoUnit,
    withdist: bool,
    withhash: bool,
    withcoord: bool,
    count: Option<usize>,
    any: bool,
    asc: Option<bool>,
) -> Vec<GeoItemResult> {
    let mut results = Vec::new();
    let mut seen = hashbrown::HashSet::new();

    let areas = calculate_search_areas(&shape, center_lon, center_lat);
    let mut last_processed: Option<GeoHashBits> = None;

    for hash_box in areas {
        if let Some(last) = last_processed {
            if last.bits == hash_box.bits && last.step == hash_box.step {
                continue;
            }
        }
        last_processed = Some(hash_box);

        let (min_score, max_score) = scores_of_geohash_box(hash_box);
        let z_opts = crate::table::ZRangeOpts {
            by_score: true,
            min_score,
            max_score,
            max_inc: false,
            with_scores: true,
            ..Default::default()
        };
        if let Ok(pairs) = db.zrange(key, &z_opts) {
            for (member, score) in pairs {
                if !seen.insert(member.clone()) {
                    continue;
                }
                if let Some(((m_lon, m_lat), dist_m)) =
                    geo_within_shape(&shape, score as u64, center_lon, center_lat)
                {
                    results.push(GeoItemResult {
                        member,
                        dist: if withdist {
                            Some(unit.from_meters(dist_m))
                        } else {
                            None
                        },
                        hash: if withhash { Some(score as u64) } else { None },
                        coord: if withcoord {
                            Some((m_lon, m_lat))
                        } else {
                            None
                        },
                        dist_m,
                        score: score as u64,
                    });
                    if any && count.is_some_and(|c| results.len() >= c) {
                        break;
                    }
                }
            }
        }
        if any && count.is_some_and(|c| results.len() >= c) {
            break;
        }
    }

    let effective_asc = if asc.is_none() && count.is_some() && !any {
        Some(true)
    } else {
        asc
    };

    if let Some(is_asc) = effective_asc {
        if is_asc {
            results.sort_by(|a, b| a.dist_m.partial_cmp(&b.dist_m).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            results.sort_by(|a, b| b.dist_m.partial_cmp(&a.dist_m).unwrap_or(std::cmp::Ordering::Equal));
        }
    }

    if let Some(c) = count {
        results.truncate(c);
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geohash_encode_decode_roundtrip() {
        let lon = 13.361389;
        let lat = 38.115556;
        let hash = encode_geohash(lon, lat).expect("encode");
        let (d_lon, d_lat) = decode_geohash(hash);
        assert!((lon - d_lon).abs() < 1e-5);
        assert!((lat - d_lat).abs() < 1e-5);
    }

    #[test]
    fn test_haversine_palermo_catania() {
        let dist = haversine_distance(13.361389, 38.115556, 15.087269, 37.502669);
        assert!((dist - 166274.0).abs() < 500.0);
    }

    #[test]
    fn test_unit_conversions() {
        let m = GeoUnit::Kilometers.to_meters(2.5);
        assert_eq!(m, 2500.0);
        let km = GeoUnit::Kilometers.from_meters(2500.0);
        assert_eq!(km, 2.5);
    }
}
