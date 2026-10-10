//! `DEBUG DIGEST` and `DEBUG DIGEST-VALUE`: a SHA1 signature of the logical
//! dataset, modelled on Redis' `computeDatasetDigest` / `xorObjectDigest`
//! (debug.c), so two servers holding the same data report the same digest
//! (for example a master and its replica, or a server before and after an
//! RDB/AOF reload).
//!
//! The digest depends only on the logical content, never on how it is held:
//!
//! * Keys are combined with XOR, so neither the iteration order of a table
//!   nor which shard owns a key matters; each shard returns its XOR and the
//!   connection XORs them together.
//! * A key's digest mixes (order-dependent SHA1 chaining) its name, a type
//!   tag and its value, then XORs in a marker if it has a TTL. Only the
//!   presence of a TTL counts, not the deadline: a replica's absolute
//!   deadlines differ from its master's.
//! * Strings digest their bytes, so an `Int` digests like its decimal
//!   string and an HLL like its register bytes. Lists and stream entries
//!   are mixed in order; set members, hash fields and sorted set
//!   `(member, score)` pairs are XORed, so the small (vector) and large
//!   (hash table) encodings agree. Scores use Redis' `%.17g` form
//!   ([`crate::connection::format_score`]), so `-0` and `0` agree too.
//! * Hash fields with a TTL get a `!!hexpire!!` marker (presence only).
//! * Tiered values are read from the tier without promoting them; cooled
//!   values digest their in-memory copy.
//! * Keys (and hash fields) whose deadline has passed but which have not
//!   been reclaimed yet are skipped: they are logically gone.
//! * Rudis' extended types, kept outside the main table (JSON, Bloom,
//!   Cuckoo, Count-Min, Top-K and vector sets), are included through their
//!   persistence encodings, made order-independent where those iterate a
//!   hash map.
//!
//! The digest is not bit-compatible with Redis' (Rudis encodes some
//! fields differently); it only has to agree between Rudis servers.

use bytes::Bytes;
use sha1::{Digest as _, Sha1};
use std::time::Instant;

use crate::shard::ShardDb;
use crate::table::{RudisEntry, RudisSet, RudisStream, RudisValue, RudisZSet, TieredPointer};

/// A 20-byte SHA1 digest.
pub type Digest20 = [u8; 20];

/// The digest of an empty dataset (and of a missing key).
pub const ZERO: Digest20 = [0; 20];

/// Redis' `xorDigest`: `d ^= SHA1(data)`.
#[inline]
pub fn xor_digest(d: &mut Digest20, data: &[u8]) {
    let h: Digest20 = Sha1::digest(data).into();
    xor_into(d, &h);
}

/// Redis' `mixDigest`: `d = SHA1(d || data)`.
#[inline]
pub fn mix_digest(d: &mut Digest20, data: &[u8]) {
    let mut hasher = Sha1::new();
    hasher.update(&d[..]);
    hasher.update(data);
    *d = hasher.finalize().into();
}

/// `d ^= other`.
#[inline]
pub fn xor_into(d: &mut Digest20, other: &Digest20) {
    for (a, b) in d.iter_mut().zip(other) {
        *a ^= b;
    }
}

/// Lowercase hex, 40 characters.
pub fn to_hex(d: &Digest20) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(40);
    for b in d {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Parses [`to_hex`]'s output.
pub fn from_hex(s: &[u8]) -> Option<Digest20> {
    if s.len() != 40 {
        return None;
    }
    let mut d = ZERO;
    for (i, pair) in s.as_chunks::<2>().0.iter().enumerate() {
        let hex = std::str::from_utf8(pair).ok()?;
        d[i] = u8::from_str_radix(hex, 16).ok()?;
    }
    Some(d)
}

// Type tags mixed into every key digest. The core types use Redis' OBJ_*
// numbering; Rudis' extended types use their own range.
const TYPE_STRING: u32 = 0;
const TYPE_LIST: u32 = 1;
const TYPE_SET: u32 = 2;
const TYPE_ZSET: u32 = 3;
const TYPE_HASH: u32 = 4;
const TYPE_STREAM: u32 = 6;
const TYPE_JSON: u32 = 100;
const TYPE_BLOOM: u32 = 101;
const TYPE_CUCKOO: u32 = 102;
const TYPE_CMS: u32 = 103;
const TYPE_TOPK: u32 = 104;
const TYPE_VECTORSET: u32 = 105;
/// A tiered value whose record could not be read back.
const TYPE_UNREADABLE: u32 = 0xFFFF_FFFF;

const EXPIRE_MARK: &[u8] = b"!!expire!!";
const HEXPIRE_MARK: &[u8] = b"!!hexpire!!";

#[inline]
fn mix_type(d: &mut Digest20, t: u32) {
    mix_digest(d, &t.to_be_bytes());
}

fn stream_id_str(id: &crate::table::StreamId) -> String {
    format!("{}-{}", id.ms, id.seq)
}

/// Per-field TTLs of one hash, as kept in `RudisTable::hash_field_expires`.
pub type FieldTtls = hashbrown::HashMap<Bytes, Instant>;

/// What a value digest needs besides the value itself.
pub struct DigestCtx<'a> {
    pub now: Instant,
    /// Reads a tiered value back without promoting it.
    pub load_tiered: &'a dyn Fn(TieredPointer) -> Option<RudisValue>,
}

/// Redis' `xorObjectDigest` for a main-table value: mixes the type tag and
/// value into `d`, then XORs in the TTL marker if `has_expire`.
/// `field_ttls` are the hash's per-field deadlines, if any.
pub fn object_digest(
    d: &mut Digest20,
    val: &RudisValue,
    has_expire: bool,
    field_ttls: Option<&FieldTtls>,
    ctx: &DigestCtx<'_>,
) {
    value_digest(d, val, field_ttls, ctx);
    if has_expire {
        xor_digest(d, EXPIRE_MARK);
    }
}

fn value_digest(
    d: &mut Digest20,
    val: &RudisValue,
    field_ttls: Option<&FieldTtls>,
    ctx: &DigestCtx<'_>,
) {
    match val {
        RudisValue::String(s) => {
            mix_type(d, TYPE_STRING);
            mix_digest(d, &s.view());
        }
        RudisValue::Int(n) => {
            mix_type(d, TYPE_STRING);
            mix_digest(d, itoa_buf(*n).as_bytes());
        }
        RudisValue::HyperLogLog(regs) => {
            mix_type(d, TYPE_STRING);
            mix_digest(d, &regs[..]);
        }
        RudisValue::List(list) => {
            mix_type(d, TYPE_LIST);
            for item in list.iter() {
                mix_digest(d, item);
            }
        }
        RudisValue::Set(set) => {
            mix_type(d, TYPE_SET);
            set_digest(d, set);
        }
        RudisValue::ZSet(z) => {
            mix_type(d, TYPE_ZSET);
            zset_digest(d, z);
        }
        RudisValue::SmallHash(pairs) => {
            mix_type(d, TYPE_HASH);
            for (f, v) in pairs.iter() {
                hash_field_digest(d, f, v, field_ttls, ctx.now);
            }
        }
        RudisValue::Hash(map) => {
            mix_type(d, TYPE_HASH);
            for (f, v) in map.iter() {
                hash_field_digest(d, f, v, field_ttls, ctx.now);
            }
        }
        RudisValue::Stream(s) => {
            mix_type(d, TYPE_STREAM);
            stream_digest(d, s);
        }
        RudisValue::Cooled(cv) => value_digest(d, &cv.val, field_ttls, ctx),
        RudisValue::Tiered(ptr) => match (ctx.load_tiered)(**ptr) {
            Some(loaded) if !matches!(loaded, RudisValue::Tiered(_)) => {
                value_digest(d, &loaded, field_ttls, ctx)
            }
            _ => {
                // Unreadable: digest the pointer so the failure shows up as a
                // mismatch rather than as a silently missing value.
                mix_type(d, TYPE_UNREADABLE);
                mix_digest(d, &ptr.file_id.to_be_bytes());
                mix_digest(d, &ptr.offset.to_be_bytes());
            }
        },
    }
}

/// The decimal form of an integer, without allocating.
struct ItoaBuf {
    buf: [u8; 20],
    start: usize,
}

impl ItoaBuf {
    fn as_bytes(&self) -> &[u8] {
        &self.buf[self.start..]
    }
}

fn itoa_buf(n: i64) -> ItoaBuf {
    let mut buf = [0u8; 20];
    let mut pos = buf.len();
    let neg = n < 0;
    let mut v = n.unsigned_abs();
    loop {
        pos -= 1;
        buf[pos] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if neg {
        pos -= 1;
        buf[pos] = b'-';
    }
    ItoaBuf { buf, start: pos }
}

fn set_digest(d: &mut Digest20, set: &RudisSet) {
    for member in set.iter() {
        xor_digest(d, member);
    }
}

fn zset_digest(d: &mut Digest20, z: &RudisZSet) {
    let mut add = |member: &[u8], score: f64| {
        let mut ele = ZERO;
        mix_digest(&mut ele, member);
        mix_digest(&mut ele, crate::connection::format_score(score).as_bytes());
        xor_into(d, &ele);
    };
    match z {
        RudisZSet::Small(v) => {
            for (score, member) in v {
                add(member, score.0);
            }
        }
        RudisZSet::Full { dict, .. } => {
            for (member, &score) in dict {
                add(member, score);
            }
        }
    }
}

fn hash_field_digest(
    d: &mut Digest20,
    field: &[u8],
    value: &[u8],
    field_ttls: Option<&FieldTtls>,
    now: Instant,
) {
    let ttl = field_ttls.and_then(|m| m.get(field));
    if ttl.is_some_and(|&at| at <= now) {
        // Expired but not reclaimed yet: logically gone.
        return;
    }
    let mut ele = ZERO;
    mix_digest(&mut ele, field);
    mix_digest(&mut ele, value);
    if ttl.is_some() {
        mix_digest(&mut ele, HEXPIRE_MARK);
    }
    xor_into(d, &ele);
}

/// Entries in order (as Redis does), then the last id and the consumer
/// groups: each group's name, last-delivered id, PEL (entry id and owning
/// consumer, in id order) and consumers (name and pending ids). Groups and
/// consumers are XORed, as they live in hash maps. Delivery times, delivery
/// counts and seen times are left out: they are wall-clock dependent.
fn stream_digest(d: &mut Digest20, s: &RudisStream) {
    for (id, fields) in &s.entries {
        mix_digest(d, stream_id_str(id).as_bytes());
        for (f, v) in fields {
            mix_digest(d, f);
            mix_digest(d, v);
        }
    }
    mix_digest(d, b"!!last-id!!");
    mix_digest(d, stream_id_str(&s.last_id).as_bytes());
    if s.groups.is_empty() {
        return;
    }
    let mut groups = ZERO;
    for group in s.groups.values() {
        let mut gd = ZERO;
        mix_digest(&mut gd, &group.name);
        mix_digest(&mut gd, stream_id_str(&group.last_delivered_id).as_bytes());
        for (id, pel) in &group.pel {
            mix_digest(&mut gd, stream_id_str(id).as_bytes());
            mix_digest(&mut gd, &pel.consumer);
        }
        let mut consumers = ZERO;
        for consumer in group.consumers.values() {
            let mut cd = ZERO;
            mix_digest(&mut cd, &consumer.name);
            for id in consumer.pel.keys() {
                mix_digest(&mut cd, stream_id_str(id).as_bytes());
            }
            xor_into(&mut consumers, &cd);
        }
        mix_digest(&mut gd, &consumers);
        xor_into(&mut groups, &gd);
    }
    mix_digest(d, b"!!groups!!");
    mix_digest(d, &groups);
}

/// Whether a main-table entry is logically present at `now`.
#[inline]
fn is_live(entry: &RudisEntry, now: Instant) -> bool {
    entry.expire_at().is_none_or(|at| at > now)
}

fn main_entry_digest(db: &ShardDb, entry: &RudisEntry, d: &mut Digest20, ctx: &DigestCtx<'_>) {
    let field_ttls = if db.table.hash_field_expires.is_empty() {
        None
    } else {
        db.table.hash_field_expires.get(entry.key.as_slice())
    };
    object_digest(d, &entry.val, entry.expire_at().is_some(), field_ttls, ctx);
}

fn json_digest(d: &mut Digest20, doc: &serde_json::Value) {
    mix_type(d, TYPE_JSON);
    mix_digest(d, doc.to_string().as_bytes());
}

fn encoded_digest(d: &mut Digest20, t: u32, encode: impl FnOnce(&mut Vec<u8>)) {
    mix_type(d, t);
    let mut buf = Vec::new();
    encode(&mut buf);
    mix_digest(d, &buf);
}

fn topk_digest(d: &mut Digest20, tk: &crate::probabilistic::TopK) {
    mix_type(d, TYPE_TOPK);
    mix_digest(d, &(tk.k as u64).to_be_bytes());
    let mut items = ZERO;
    for (item, count) in &tk.items {
        let mut ele = ZERO;
        mix_digest(&mut ele, item);
        mix_digest(&mut ele, &count.to_be_bytes());
        xor_into(&mut items, &ele);
    }
    mix_digest(d, &items);
}

/// A vector set: its parameters, then each element's name, full-precision
/// vector and attributes, XORed. The HNSW graph is left out: its shape
/// depends on insertion order and random levels.
fn vectorset_digest(d: &mut Digest20, index: &crate::vector::HnswIndex) {
    mix_type(d, TYPE_VECTORSET);
    let quant = match index.quant {
        crate::vector::VQuant::NoQuant => 0u8,
        crate::vector::VQuant::Q8 => 1,
        crate::vector::VQuant::Bin => 2,
    };
    mix_digest(
        d,
        &[index.metric as u8, quant, u8::from(index.is_redis_vset)],
    );
    mix_digest(d, &(index.dim as u64).to_be_bytes());
    let mut elements = ZERO;
    for (key, &node_id) in &index.key_to_id {
        let Some(Some(node)) = index.nodes.get(node_id) else {
            continue;
        };
        let mut ele = ZERO;
        mix_digest(&mut ele, key);
        let vector = index.node_vector_cow(node);
        let mut raw = Vec::with_capacity(vector.len() * 4);
        for coord in vector.iter() {
            raw.extend_from_slice(&coord.to_bits().to_le_bytes());
        }
        mix_digest(&mut ele, &raw);
        match index.attributes.get(key) {
            Some(attr) => {
                mix_digest(&mut ele, b"!!attr!!");
                mix_digest(&mut ele, attr.as_bytes());
            }
            None => mix_digest(&mut ele, b"!!noattr!!"),
        }
        xor_into(&mut elements, &ele);
    }
    mix_digest(d, &elements);
}

/// Calls `f(key, digest_fn)` for every extended-type key in `db`, where
/// `digest_fn` mixes that key's type and value into a digest.
fn for_each_ext_key(db: &ShardDb, mut f: impl FnMut(&[u8], &dyn Fn(&mut Digest20))) {
    for (key, doc) in db.json_store.iter() {
        f(key, &|d| json_digest(d, doc));
    }
    let prob = &db.probabilistic_store;
    for (key, bf) in &prob.bloom_filters {
        f(key, &|d| encoded_digest(d, TYPE_BLOOM, |b| bf.encode(b)));
    }
    for (key, cf) in &prob.cuckoo_filters {
        f(key, &|d| encoded_digest(d, TYPE_CUCKOO, |b| cf.encode(b)));
    }
    for (key, cms) in &prob.cms_sketches {
        f(key, &|d| encoded_digest(d, TYPE_CMS, |b| cms.encode(b)));
    }
    for (key, tk) in &prob.topk_trackers {
        f(key, &|d| topk_digest(d, tk));
    }
    for (name, index) in &db.vector_indexes {
        if index.is_empty() {
            continue;
        }
        f(name.as_bytes(), &|d| vectorset_digest(d, index));
    }
}

/// Mixes the extended-type value stored at `key` into `d`; false if there
/// is none.
fn ext_value_digest(db: &ShardDb, key: &[u8], d: &mut Digest20) -> bool {
    if let Some(doc) = db.json_store.get(key) {
        json_digest(d, doc);
        return true;
    }
    let prob = &db.probabilistic_store;
    if let Some(bf) = prob.bloom_filters.get(key) {
        encoded_digest(d, TYPE_BLOOM, |b| bf.encode(b));
        return true;
    }
    if let Some(cf) = prob.cuckoo_filters.get(key) {
        encoded_digest(d, TYPE_CUCKOO, |b| cf.encode(b));
        return true;
    }
    if let Some(cms) = prob.cms_sketches.get(key) {
        encoded_digest(d, TYPE_CMS, |b| cms.encode(b));
        return true;
    }
    if let Some(tk) = prob.topk_trackers.get(key) {
        topk_digest(d, tk);
        return true;
    }
    if let Ok(name) = std::str::from_utf8(key)
        && let Some(index) = db.vector_indexes.get(name)
        && !index.is_empty()
    {
        vectorset_digest(d, index);
        return true;
    }
    false
}

/// This shard's part of `DEBUG DIGEST`: the XOR of every live key's digest
/// (name, type, value, TTL presence) and the number of keys included.
/// Read-only: nothing is expired, promoted or warmed.
pub fn shard_digest(db: &ShardDb) -> (Digest20, u64) {
    let now = Instant::now();
    let load = |ptr| db.hydrate_tiered(ptr);
    let ctx = DigestCtx {
        now,
        load_tiered: &load,
    };
    let mut acc = ZERO;
    let mut count = 0u64;
    for entry in db.table.entries() {
        if !is_live(entry, now) {
            continue;
        }
        let mut kd = ZERO;
        mix_digest(&mut kd, entry.key.as_slice());
        main_entry_digest(db, entry, &mut kd, &ctx);
        xor_into(&mut acc, &kd);
        count += 1;
    }
    for_each_ext_key(db, |key, digest| {
        let mut kd = ZERO;
        mix_digest(&mut kd, key);
        digest(&mut kd);
        xor_into(&mut acc, &kd);
        count += 1;
    });
    (acc, count)
}

/// `DEBUG DIGEST-VALUE` for one key owned by this shard: the digest of its
/// type, value and TTL presence (not its name), or [`ZERO`] if it does not
/// exist.
pub fn key_value_digest(db: &ShardDb, key: &[u8]) -> Digest20 {
    let now = Instant::now();
    let mut d = ZERO;
    if let Some(entry) = db.table.peek_entry(key)
        && is_live(entry, now)
    {
        let load = |ptr| db.hydrate_tiered(ptr);
        let ctx = DigestCtx {
            now,
            load_tiered: &load,
        };
        main_entry_digest(db, entry, &mut d, &ctx);
        return d;
    }
    ext_value_digest(db, key, &mut d);
    d
}

/// The `DEBUG DIGEST` reply for a dataset whose shards returned
/// `(xor, count)` parts: 40 zeros when empty, as Redis does.
pub fn combine_shards(parts: impl IntoIterator<Item = (Digest20, u64)>) -> Digest20 {
    let mut acc = ZERO;
    let mut count = 0u64;
    for (d, n) in parts {
        xor_into(&mut acc, &d);
        count += n;
    }
    if count == 0 { ZERO } else { acc }
}

/// Encodes a shard part for the cross-shard reply: `<40 hex>:<count>`.
pub fn encode_shard_part(part: &(Digest20, u64)) -> String {
    format!("{}:{}", to_hex(&part.0), part.1)
}

/// Decodes [`encode_shard_part`]'s output.
pub fn decode_shard_part(s: &[u8]) -> Option<(Digest20, u64)> {
    let colon = s.iter().position(|&b| b == b':')?;
    let d = from_hex(&s[..colon])?;
    let n = std::str::from_utf8(&s[colon + 1..]).ok()?.parse().ok()?;
    Some((d, n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::CompactKey;
    use crate::table::{Expiry, RudisHashMap, SmallSetEntry, StreamId};
    use std::time::Duration;

    fn no_tier(_: TieredPointer) -> Option<RudisValue> {
        None
    }

    fn vdigest(val: &RudisValue) -> Digest20 {
        let ctx = DigestCtx {
            now: Instant::now(),
            load_tiered: &no_tier,
        };
        let mut d = ZERO;
        object_digest(&mut d, val, false, None, &ctx);
        d
    }

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn insert(db: &mut ShardDb, key: &str, val: RudisValue, ttl: Option<Duration>) {
        let exp = Expiry::from(ttl.map(|t| Instant::now() + t));
        db.table
            .insert_entry(RudisEntry::new(CompactKey::from(key.as_bytes()), val, exp));
    }

    #[test]
    fn hex_round_trip() {
        let mut d = ZERO;
        mix_digest(&mut d, b"abc");
        let hex = to_hex(&d);
        assert_eq!(hex.len(), 40);
        assert_eq!(from_hex(hex.as_bytes()), Some(d));
        assert_eq!(from_hex(b"zz"), None);
        let part = (d, 7);
        assert_eq!(
            decode_shard_part(encode_shard_part(&part).as_bytes()),
            Some(part)
        );
    }

    #[test]
    fn int_digests_like_its_decimal_string() {
        for n in [0i64, 7, -1, 1234567890, i64::MIN, i64::MAX] {
            assert_eq!(
                vdigest(&RudisValue::Int(n)),
                vdigest(&RudisValue::String(n.to_string().as_str().into())),
                "{n}"
            );
        }
        assert_ne!(
            vdigest(&RudisValue::Int(1)),
            vdigest(&RudisValue::String("01".into()))
        );
    }

    #[test]
    fn small_and_full_hash_agree_and_are_order_independent() {
        let pairs = vec![(b("a"), b("1")), (b("b"), b("2")), (b("c"), b("3"))];
        let small = RudisValue::SmallHash(Box::new(pairs.clone()));
        let mut rev = pairs.clone();
        rev.reverse();
        let small_rev = RudisValue::SmallHash(Box::new(rev));
        let mut map = RudisHashMap::default();
        for (f, v) in &pairs {
            map.insert(f.clone(), v.clone());
        }
        let full = RudisValue::Hash(Box::new(map));
        assert_eq!(vdigest(&small), vdigest(&small_rev));
        assert_eq!(vdigest(&small), vdigest(&full));
        let other = RudisValue::SmallHash(Box::new(vec![(b("a"), b("1")), (b("b"), b("3"))]));
        assert_ne!(vdigest(&small), vdigest(&other));
    }

    #[test]
    fn hash_field_ttl_presence_counts_but_not_its_value() {
        let val = RudisValue::SmallHash(Box::new(vec![(b("f"), b("v")), (b("g"), b("w"))]));
        let now = Instant::now();
        let ctx = DigestCtx {
            now,
            load_tiered: &no_tier,
        };
        let digest_with = |ttls: Option<&FieldTtls>| {
            let mut d = ZERO;
            object_digest(&mut d, &val, false, ttls, &ctx);
            d
        };
        let mut t1 = FieldTtls::new();
        t1.insert(b("f"), now + Duration::from_secs(10));
        let mut t2 = FieldTtls::new();
        t2.insert(b("f"), now + Duration::from_secs(5000));
        assert_eq!(digest_with(Some(&t1)), digest_with(Some(&t2)));
        assert_ne!(digest_with(Some(&t1)), digest_with(None));
        // An expired field is skipped entirely.
        let mut gone = FieldTtls::new();
        gone.insert(b("g"), now - Duration::from_millis(1));
        let only_f = RudisValue::SmallHash(Box::new(vec![(b("f"), b("v"))]));
        let mut d = ZERO;
        object_digest(&mut d, &val, false, Some(&gone), &ctx);
        assert_eq!(d, vdigest(&only_f));
    }

    #[test]
    fn set_encodings_and_orders_agree() {
        let members = ["x", "y", "z", "123"];
        let small = RudisSet::Small(
            members
                .iter()
                .map(|m| SmallSetEntry {
                    hash: crate::table::hash_key(m.as_bytes()),
                    member: b(m),
                })
                .collect(),
        );
        let mut full_set = hashbrown::HashSet::with_hasher(Default::default());
        for m in members.iter().rev() {
            full_set.insert(b(m));
        }
        let full = RudisSet::Full(full_set);
        assert_eq!(
            vdigest(&RudisValue::Set(Box::new(small))),
            vdigest(&RudisValue::Set(Box::new(full)))
        );
    }

    #[test]
    fn zset_encodings_orders_and_zero_scores_agree() {
        let items = [(1.5, "a"), (-0.0, "b"), (1e300, "c")];
        let mut small = RudisZSet::new();
        for (s, m) in items {
            small.insert(s, b(m));
        }
        let mut dict = hashbrown::HashMap::new();
        let mut tree = std::collections::BTreeSet::new();
        for (s, m) in items.iter().rev() {
            let s = if *s == 0.0 { 0.0 } else { *s };
            dict.insert(b(m), s);
            tree.insert((crate::table::OrderedScore(s), b(m)));
        }
        let full = RudisZSet::Full { dict, tree };
        assert_eq!(
            vdigest(&RudisValue::ZSet(Box::new(small.clone()))),
            vdigest(&RudisValue::ZSet(Box::new(full)))
        );
        let mut changed = small.clone();
        changed.insert(1.25, b("a"));
        assert_ne!(
            vdigest(&RudisValue::ZSet(Box::new(small))),
            vdigest(&RudisValue::ZSet(Box::new(changed)))
        );
    }

    #[test]
    fn list_order_matters_and_types_differ() {
        let l1 = RudisValue::List(Box::new([b("a"), b("b")].into_iter().collect()));
        let l2 = RudisValue::List(Box::new([b("b"), b("a")].into_iter().collect()));
        assert_ne!(vdigest(&l1), vdigest(&l2));
        // A one-member set and a one-element list differ by type.
        let set = RudisValue::Set(Box::new(RudisSet::Small(vec![SmallSetEntry {
            hash: crate::table::hash_key(b"a"),
            member: b("a"),
        }])));
        let list = RudisValue::List(Box::new([b("a")].into_iter().collect()));
        assert_ne!(vdigest(&set), vdigest(&list));
    }

    #[test]
    fn stream_entries_and_groups_are_digested() {
        let mut s = RudisStream::new();
        let id = StreamId { ms: 1, seq: 0 };
        s.entries.insert(id, vec![(b("f"), b("v"))]);
        s.last_id = id;
        let base = vdigest(&RudisValue::Stream(Box::new(s.clone())));
        let mut with_group = s.clone();
        with_group.groups.insert(
            b("g"),
            crate::table::StreamGroup {
                name: b("g"),
                last_delivered_id: StreamId::default(),
                entries_read: None,
                consumers: hashbrown::HashMap::new(),
                pel: Default::default(),
                next_nack_seq: 0,
            },
        );
        assert_ne!(base, vdigest(&RudisValue::Stream(Box::new(with_group))));
        let mut other_entry = s.clone();
        other_entry.entries.insert(id, vec![(b("f"), b("w"))]);
        assert_ne!(base, vdigest(&RudisValue::Stream(Box::new(other_entry))));
    }

    #[test]
    fn cooled_digests_like_its_value_and_tiered_loads_it() {
        let val = RudisValue::String("hello world, a value".into());
        let ptr = TieredPointer {
            file_id: 1,
            offset: 2,
            length: 3,
            value_type: 0,
        };
        let cooled = RudisValue::Cooled(Box::new(crate::table::CooledValue {
            ptr,
            val: val.clone(),
        }));
        assert_eq!(vdigest(&val), vdigest(&cooled));

        let loaded = val.clone();
        let load = move |_| Some(loaded.clone());
        let ctx = DigestCtx {
            now: Instant::now(),
            load_tiered: &load,
        };
        let mut d = ZERO;
        object_digest(
            &mut d,
            &RudisValue::Tiered(Box::new(ptr)),
            false,
            None,
            &ctx,
        );
        assert_eq!(d, vdigest(&val));
        // Unreadable tiered values do not digest like the real value.
        assert_ne!(vdigest(&RudisValue::Tiered(Box::new(ptr))), vdigest(&val));
    }

    #[test]
    fn dataset_digest_is_order_independent_and_ttl_presence_only() {
        let mut a = ShardDb::new(0);
        let mut b_db = ShardDb::new(0);
        insert(&mut a, "k1", RudisValue::Int(5), None);
        insert(
            &mut a,
            "k2",
            RudisValue::String("v2".into()),
            Some(Duration::from_secs(100)),
        );
        insert(
            &mut b_db,
            "k2",
            RudisValue::String("v2".into()),
            Some(Duration::from_secs(9000)),
        );
        insert(&mut b_db, "k1", RudisValue::String("5".into()), None);
        assert_eq!(shard_digest(&a), shard_digest(&b_db));
        assert_eq!(shard_digest(&a).1, 2);

        // Dropping the TTL changes the digest.
        let mut c = ShardDb::new(0);
        insert(&mut c, "k1", RudisValue::Int(5), None);
        insert(&mut c, "k2", RudisValue::String("v2".into()), None);
        assert_ne!(shard_digest(&a).0, shard_digest(&c).0);

        // Key names count: same values under other names differ.
        let mut d = ShardDb::new(0);
        insert(&mut d, "k1", RudisValue::String("v2".into()), None);
        insert(&mut d, "k2", RudisValue::Int(5), None);
        assert_ne!(shard_digest(&c).0, shard_digest(&d).0);
    }

    #[test]
    fn split_across_shards_equals_single_shard() {
        let mut whole = ShardDb::new(0);
        let mut s0 = ShardDb::new(0);
        let mut s1 = ShardDb::new(0);
        for i in 0..20 {
            let k = format!("key:{i}");
            insert(&mut whole, &k, RudisValue::Int(i), None);
            let part = if i % 2 == 0 { &mut s0 } else { &mut s1 };
            insert(part, &k, RudisValue::Int(i), None);
        }
        assert_eq!(
            combine_shards([shard_digest(&whole)]),
            combine_shards([shard_digest(&s0), shard_digest(&s1)])
        );
        assert_eq!(combine_shards([(ZERO, 0), (ZERO, 0)]), ZERO);
        assert_ne!(combine_shards([shard_digest(&whole)]), ZERO);
    }

    #[test]
    fn expired_keys_are_skipped_and_missing_keys_are_zero() {
        let mut db = ShardDb::new(0);
        insert(&mut db, "live", RudisValue::Int(1), None);
        let mut only_live = ShardDb::new(0);
        insert(&mut only_live, "live", RudisValue::Int(1), None);
        db.table.insert_entry(RudisEntry::new(
            CompactKey::from(&b"dead"[..]),
            RudisValue::Int(2),
            Expiry::from(Instant::now() - Duration::from_millis(5)),
        ));
        assert_eq!(shard_digest(&db), shard_digest(&only_live));
        assert_eq!(key_value_digest(&db, b"dead"), ZERO);
        assert_eq!(key_value_digest(&db, b"missing"), ZERO);
        assert_eq!(
            key_value_digest(&db, b"live"),
            vdigest(&RudisValue::String("1".into()))
        );
    }

    #[test]
    fn extended_types_are_included() {
        let mut db = ShardDb::new(0);
        let empty = shard_digest(&db);
        db.json_store
            .json_set(b"doc", "$", r#"{"a":1}"#, false, false)
            .unwrap();
        let with_json = shard_digest(&db);
        assert_eq!(with_json.1, empty.1 + 1);
        assert_ne!(with_json.0, empty.0);
        assert_ne!(key_value_digest(&db, b"doc"), ZERO);

        let mut other = ShardDb::new(0);
        other
            .json_store
            .json_set(b"doc", "$", r#"{"a":2}"#, false, false)
            .unwrap();
        assert_ne!(shard_digest(&other).0, with_json.0);
    }

    #[test]
    fn tiered_value_digests_like_in_memory() {
        let dir = std::env::temp_dir().join(format!("rudis_tier_digest_{}", std::process::id()));
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut db = ShardDb::new(0);
            db.tier_manager = Some(std::rc::Rc::new(
                crate::tiering::ShardTierManager::open(0, 55571, &dir)
                    .await
                    .unwrap(),
            ));
            db.hset(
                b("h"),
                vec![(b("f1"), b("value-one")), (b("f2"), b("value-two"))],
            )
            .unwrap();
            db.set(b("s"), b("a string value long enough to spill"), None);
            let before = shard_digest(&db);
            let h_before = key_value_digest(&db, b"h");

            for key in [&b"h"[..], b"s"] {
                let (payload, val_type) = db.table.get_value_for_spill(key).unwrap();
                let tm = db.tier_manager.clone().unwrap();
                let ptr = tm
                    .stash_record(&Bytes::copy_from_slice(key), &payload, val_type)
                    .await
                    .unwrap();
                assert!(db.table.set_tiered_pointer(key, ptr));
            }
            assert!(
                db.table
                    .peek_entry(b"h")
                    .is_some_and(|e| matches!(e.val, RudisValue::Tiered(_)))
            );
            assert_eq!(shard_digest(&db), before);
            assert_eq!(key_value_digest(&db, b"h"), h_before);
            // Digesting did not promote the values.
            assert!(
                db.table
                    .peek_entry(b"s")
                    .is_some_and(|e| matches!(e.val, RudisValue::Tiered(_)))
            );
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
