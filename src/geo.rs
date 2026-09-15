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

    let a = (dlat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
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
            if item.dist.is_some() { sub_count += 1; }
            if item.hash.is_some() { sub_count += 1; }
            if item.coord.is_some() { sub_count += 1; }

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
}
