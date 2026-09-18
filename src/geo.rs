use bytes::Bytes;

pub const EARTH_RADIUS_METERS: f64 = 6372797.560856;
pub const GEO_LAT_MIN: f64 = -85.05112878;
pub const GEO_LAT_MAX: f64 = 85.05112878;
pub const GEO_LON_MIN: f64 = -180.0;
pub const GEO_LON_MAX: f64 = 180.0;

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
            other => Err(format!("Unsupported unit '{}', need m, km, mi, ft", other)),
        }
    }

    pub fn to_meters(&self, val: f64) -> f64 {
        match self {
            GeoUnit::Meters => val,
            GeoUnit::Kilometers => val * 1000.0,
            GeoUnit::Miles => val * 1609.344,
            GeoUnit::Feet => val * 0.3048,
        }
    }

    pub fn from_meters(&self, meters: f64) -> f64 {
        match self {
            GeoUnit::Meters => meters,
            GeoUnit::Kilometers => meters / 1000.0,
            GeoUnit::Miles => meters / 1609.344,
            GeoUnit::Feet => meters / 0.3048,
        }
    }
}

/// Encodes (longitude, latitude) into a 52-bit integer geohash.
pub fn encode_geohash(lon: f64, lat: f64) -> Result<u64, &'static str> {
    if lon < GEO_LON_MIN || lon > GEO_LON_MAX {
        return Err("ERR invalid longitude");
    }
    if lat < GEO_LAT_MIN || lat > GEO_LAT_MAX {
        return Err("ERR invalid latitude");
    }

    let norm_lon = (lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN);
    let norm_lat = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);

    let max_val = (1u64 << 26) as f64;
    let x = (norm_lon * max_val).clamp(0.0, max_val - 1.0) as u64;
    let y = (norm_lat * max_val).clamp(0.0, max_val - 1.0) as u64;

    // Interleave bits of x and y: x at even bit positions, y at odd bit positions
    let mut hash = 0u64;
    for i in 0..26 {
        let bit_x = (x >> i) & 1;
        let bit_y = (y >> i) & 1;
        hash |= bit_x << (2 * i);
        hash |= bit_y << (2 * i + 1);
    }
    Ok(hash)
}

/// Decodes a 52-bit integer geohash into (longitude, latitude).
pub fn decode_geohash(hash: u64) -> (f64, f64) {
    let mut x = 0u64;
    let mut y = 0u64;

    for i in 0..26 {
        let bit_x = (hash >> (2 * i)) & 1;
        let bit_y = (hash >> (2 * i + 1)) & 1;
        x |= bit_x << i;
        y |= bit_y << i;
    }

    let max_val = (1u64 << 26) as f64;
    let lon = GEO_LON_MIN + ((x as f64 + 0.5) / max_val) * (GEO_LON_MAX - GEO_LON_MIN);
    let lat = GEO_LAT_MIN + ((y as f64 + 0.5) / max_val) * (GEO_LAT_MAX - GEO_LAT_MIN);
    (lon, lat)
}

/// Formats a 52-bit geohash into an 11-character Redis-standard base32 string.
pub fn geohash_to_base32(hash: u64) -> String {
    // Left-shift 52-bit hash to 55 bits so 11 5-bit chunks can be extracted from MSB
    let shifted = hash << 3;
    let mut result = Vec::with_capacity(11);
    for i in (0..11).rev() {
        let idx = ((shifted >> (i * 5)) & 0x1F) as usize;
        result.push(GEOHASH_ALPHABET[idx]);
    }
    String::from_utf8(result).unwrap_or_default()
}

/// Computes Great-Circle (Haversine) distance between two points in meters.
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();

    let a =
        (dlat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().clamp(0.0, 1.0).asin();
    EARTH_RADIUS_METERS * c
}

#[derive(Debug, Clone)]
pub struct GeoItemResult {
    pub member: Bytes,
    pub dist: Option<f64>,
    pub hash: Option<u64>,
    pub coord: Option<(f64, f64)>,
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
                let lon_str = format!("{:.6}", lon);
                let lat_str = format!("{:.6}", lat);
                crate::connection::write_resp_bulk(out, lon_str.as_bytes());
                crate::connection::write_resp_bulk(out, lat_str.as_bytes());
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum GeoSearchShape {
    Radius { radius_m: f64 },
    Box { width_m: f64, height_m: f64 },
}

impl GeoSearchShape {
    pub fn search_ranges(&self, lon: f64, lat: f64) -> Vec<(f64, f64)> {
        match *self {
            GeoSearchShape::Radius { radius_m } => geohash_search_ranges(lon, lat, radius_m),
            GeoSearchShape::Box { width_m, height_m } => {
                let diag = (width_m * width_m + height_m * height_m).sqrt() / 2.0;
                geohash_search_ranges(lon, lat, diag)
            }
        }
    }

    pub fn is_inside(
        &self,
        center_lon: f64,
        center_lat: f64,
        m_lon: f64,
        m_lat: f64,
        dist_m: f64,
    ) -> bool {
        match *self {
            GeoSearchShape::Radius { radius_m } => dist_m <= radius_m,
            GeoSearchShape::Box { width_m, height_m } => {
                let w_m = width_m / 2.0;
                let h_m = height_m / 2.0;
                let dlat_m = (m_lat - center_lat).abs() * 111_320.0;
                let dlon_m =
                    (m_lon - center_lon).abs() * 111_320.0 * (center_lat.to_radians().cos().abs());
                dlat_m <= h_m && dlon_m <= w_m
            }
        }
    }
}

/// Computes contiguous 1D geohash intervals [min_hash, max_hash] that completely cover
/// the bounding box of radius `radius_meters` around (lon, lat).
pub fn geohash_search_ranges(lon: f64, lat: f64, radius_meters: f64) -> Vec<(f64, f64)> {
    if radius_meters <= 0.0 {
        if let Ok(hash) = encode_geohash(lon, lat) {
            return vec![(hash as f64, hash as f64)];
        }
        return Vec::new();
    }

    if radius_meters >= 20_000_000.0 {
        return vec![(0.0, ((1u64 << 52) - 1) as f64)];
    }

    let lat_rad = lat.to_radians();
    let dlat = radius_meters / 111_320.0;
    let cos_lat = lat_rad.cos().abs().max(0.0001);
    let dlon = radius_meters / (111_320.0 * cos_lat);

    let min_lat = (lat - dlat).clamp(GEO_LAT_MIN, GEO_LAT_MAX);
    let max_lat = (lat + dlat).clamp(GEO_LAT_MIN, GEO_LAT_MAX);
    let min_lon = (lon - dlon).clamp(GEO_LON_MIN, GEO_LON_MAX);
    let max_lon = (lon + dlon).clamp(GEO_LON_MIN, GEO_LON_MAX);

    let max_coord = (1u64 << 26) as f64;
    let norm_lon_min = ((min_lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN)).clamp(0.0, 1.0);
    let norm_lon_max = ((max_lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN)).clamp(0.0, 1.0);
    let norm_lat_min = ((min_lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN)).clamp(0.0, 1.0);
    let norm_lat_max = ((max_lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN)).clamp(0.0, 1.0);

    let x_min = (norm_lon_min * max_coord).clamp(0.0, max_coord - 1.0) as u64;
    let x_max = (norm_lon_max * max_coord).clamp(0.0, max_coord - 1.0) as u64;
    let y_min = (norm_lat_min * max_coord).clamp(0.0, max_coord - 1.0) as u64;
    let y_max = (norm_lat_max * max_coord).clamp(0.0, max_coord - 1.0) as u64;

    let delta_x = (x_max - x_min).max(1);
    let delta_y = (y_max - y_min).max(1);
    let delta_max = delta_x.max(delta_y);

    let k = (64 - delta_max.leading_zeros()).min(26);

    let cell_min_x = x_min >> k;
    let cell_max_x = x_max >> k;
    let cell_min_y = y_min >> k;
    let cell_max_y = y_max >> k;

    let mut ranges = Vec::new();

    for cx in cell_min_x..=cell_max_x {
        for cy in cell_min_y..=cell_max_y {
            let x0 = cx << k;
            let y0 = cy << k;
            let x1 = x0 | ((1u64 << k) - 1);
            let y1 = y0 | ((1u64 << k) - 1);

            let mut h_min = 0u64;
            let mut h_max = 0u64;
            for i in 0..26 {
                let bx0 = (x0 >> i) & 1;
                let by0 = (y0 >> i) & 1;
                h_min |= (bx0 << (2 * i)) | (by0 << (2 * i + 1));

                let bx1 = (x1 >> i) & 1;
                let by1 = (y1 >> i) & 1;
                h_max |= (bx1 << (2 * i)) | (by1 << (2 * i + 1));
            }
            ranges.push((h_min as f64, h_max as f64));
        }
    }

    ranges.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut merged: Vec<(f64, f64)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some(last) = merged.last_mut()
            && start <= last.1 + 1.0
        {
            if end > last.1 {
                last.1 = end;
            }
            continue;
        }
        merged.push((start, end));
    }

    merged
}

/// Unified spatial range query executor using geohash interval pruning.
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
    asc: Option<bool>,
) -> Vec<GeoItemResult> {
    let mut results = Vec::new();
    let mut seen = hashbrown::HashSet::new();

    let ranges = shape.search_ranges(center_lon, center_lat);
    for (min_score, max_score) in ranges {
        let z_opts = crate::table::ZRangeOpts {
            by_score: true,
            min_score,
            max_score,
            with_scores: true,
            ..Default::default()
        };
        if let Ok(pairs) = db.zrange(key, &z_opts) {
            for (member, score) in pairs {
                if !seen.insert(member.clone()) {
                    continue;
                }
                let (m_lon, m_lat) = decode_geohash(score as u64);
                let dist_m = haversine_distance(center_lon, center_lat, m_lon, m_lat);
                if shape.is_inside(center_lon, center_lat, m_lon, m_lat, dist_m) {
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
                    });
                    if asc.is_none() && count.is_some_and(|c| results.len() >= c) {
                        break;
                    }
                }
            }
        }
        if asc.is_none() && count.is_some_and(|c| results.len() >= c) {
            break;
        }
    }

    if let Some(is_asc) = asc {
        if is_asc {
            results.sort_by(|a, b| {
                a.dist
                    .unwrap_or(0.0)
                    .partial_cmp(&b.dist.unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        } else {
            results.sort_by(|a, b| {
                b.dist
                    .unwrap_or(0.0)
                    .partial_cmp(&a.dist.unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
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

        let base32 = geohash_to_base32(hash);
        assert_eq!(base32.len(), 11);
    }

    #[test]
    fn test_haversine_palermo_catania() {
        // Palermo: 13.361389, 38.115556
        // Catania: 15.087269, 37.502669
        let dist = haversine_distance(13.361389, 38.115556, 15.087269, 37.502669);
        // Distance is approx 166.27 km
        assert!((dist - 166274.0).abs() < 500.0);
    }

    #[test]
    fn test_unit_conversions() {
        let m = GeoUnit::Kilometers.to_meters(2.5);
        assert_eq!(m, 2500.0);
        let km = GeoUnit::Kilometers.from_meters(2500.0);
        assert_eq!(km, 2.5);
    }

    #[test]
    fn test_geohash_search_ranges_pruning() {
        let lon = 13.361389;
        let lat = 38.115556;
        let ranges = geohash_search_ranges(lon, lat, 5000.0);
        assert!(!ranges.is_empty());
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].1 < ranges[i + 1].0);
        }
        let center_hash = encode_geohash(lon, lat).unwrap() as f64;
        let covered = ranges
            .iter()
            .any(|&(min_s, max_s)| center_hash >= min_s && center_hash <= max_s);
        assert!(covered);
    }

    #[test]
    fn test_execute_geo_query_pruned_search() {
        let mut db = crate::shard::ShardDb::new(0);
        let key = b"Sicily";

        let palermo_hash = encode_geohash(13.361389, 38.115556).unwrap() as f64;
        db.zadd(
            Bytes::from_static(key),
            vec![(palermo_hash, Bytes::from("Palermo"))],
            crate::table::ZAddFlags::default(),
        )
        .unwrap();

        let catania_hash = encode_geohash(15.087269, 37.502669).unwrap() as f64;
        db.zadd(
            Bytes::from_static(key),
            vec![(catania_hash, Bytes::from("Catania"))],
            crate::table::ZAddFlags::default(),
        )
        .unwrap();

        let res = execute_geo_query(
            &mut db,
            key,
            13.361389,
            38.115556,
            GeoSearchShape::Radius {
                radius_m: 100_000.0,
            },
            GeoUnit::Kilometers,
            true,
            true,
            true,
            None,
            Some(true),
        );
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].member, Bytes::from("Palermo"));
        assert!(res[0].dist.unwrap() < 1.0);

        let res2 = execute_geo_query(
            &mut db,
            key,
            13.361389,
            38.115556,
            GeoSearchShape::Radius {
                radius_m: 200_000.0,
            },
            GeoUnit::Kilometers,
            true,
            false,
            false,
            None,
            Some(true),
        );
        assert_eq!(res2.len(), 2);
        assert_eq!(res2[0].member, Bytes::from("Palermo"));
        assert_eq!(res2[1].member, Bytes::from("Catania"));
    }
}
