use bytes::Bytes;
use fxhash::hash64;
use hashbrown::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const GROUP_SIZE: usize = 16;
pub const EMPTY: u8 = 0xFF;
pub const DELETED: u8 = 0xFE;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OrderedScore(pub f64);

impl Eq for OrderedScore {}

impl PartialOrd for OrderedScore {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedScore {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ZAddFlags {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
    pub incr: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ZRangeOpts {
    pub start: i64,
    pub stop: i64,
    pub min_score: f64,
    pub min_inc: bool,
    pub max_score: f64,
    pub max_inc: bool,
    pub by_score: bool,
    pub rev: bool,
    pub with_scores: bool,
    pub offset: usize,
    pub count: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Aggregate {
    Sum,
    Min,
    Max,
}

impl Default for Aggregate {
    fn default() -> Self {
        Aggregate::Sum
    }
}

const SMALL_ZSET_LIMIT: usize = 64;

#[derive(Clone, Debug)]
pub enum RudisZSet {
    Small(Vec<(OrderedScore, Bytes)>),
    Full {
        dict: hashbrown::HashMap<Bytes, f64>,
        tree: std::collections::BTreeSet<(OrderedScore, Bytes)>,
    },
}

impl Default for RudisZSet {
    fn default() -> Self {
        Self::new()
    }
}

impl RudisZSet {
    pub fn new() -> Self {
        RudisZSet::Small(Vec::new())
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            RudisZSet::Small(v) => v.len(),
            RudisZSet::Full { dict, .. } => dict.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        match self {
            RudisZSet::Small(v) => v.iter().find(|(_, m)| m.as_ref() == member).map(|(s, _)| s.0),
            RudisZSet::Full { dict, .. } => dict.get(member).copied(),
        }
    }

    pub fn insert(&mut self, score: f64, member: Bytes) {
        match self {
            RudisZSet::Small(v) => {
                if let Some(pos) = v.iter().position(|(_, m)| m == &member) {
                    v.remove(pos);
                }
                let ord = (OrderedScore(score), member);
                let insert_idx = match v.binary_search(&ord) {
                    Ok(idx) | Err(idx) => idx,
                };
                v.insert(insert_idx, ord);

                if v.len() > SMALL_ZSET_LIMIT {
                    let mut dict = hashbrown::HashMap::with_capacity(v.len());
                    let mut tree = std::collections::BTreeSet::new();
                    for (s, m) in v.drain(..) {
                        dict.insert(m.clone(), s.0);
                        tree.insert((s, m));
                    }
                    *self = RudisZSet::Full { dict, tree };
                }
            }
            RudisZSet::Full { dict, tree } => {
                if let Some(old_score) = dict.insert(member.clone(), score) {
                    tree.remove(&(OrderedScore(old_score), member.clone()));
                }
                tree.insert((OrderedScore(score), member));
            }
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        match self {
            RudisZSet::Small(v) => {
                if let Some(pos) = v.iter().position(|(_, m)| m.as_ref() == member) {
                    Some(v.remove(pos).0.0)
                } else {
                    None
                }
            }
            RudisZSet::Full { dict, tree } => {
                if let Some(old_score) = dict.remove(member) {
                    tree.remove(&(OrderedScore(old_score), Bytes::copy_from_slice(member)));
                    Some(old_score)
                } else {
                    None
                }
            }
        }
    }

    pub fn rank(&self, member: &[u8], rev: bool) -> Option<usize> {
        match self {
            RudisZSet::Small(v) => {
                if rev {
                    v.iter().rev().position(|(_, m)| m.as_ref() == member)
                } else {
                    v.iter().position(|(_, m)| m.as_ref() == member)
                }
            }
            RudisZSet::Full { dict, tree } => {
                if !dict.contains_key(member) {
                    return None;
                }
                if rev {
                    tree.iter().rev().position(|(_, m)| m.as_ref() == member)
                } else {
                    tree.iter().position(|(_, m)| m.as_ref() == member)
                }
            }
        }
    }

    pub fn count(&self, min: f64, min_inc: bool, max: f64, max_inc: bool) -> usize {
        match self {
            RudisZSet::Small(v) => v.iter().filter(|(OrderedScore(s), _)| {
                let ge_min = if min_inc { *s >= min } else { *s > min };
                let le_max = if max_inc { *s <= max } else { *s < max };
                ge_min && le_max
            }).count(),
            RudisZSet::Full { tree, .. } => tree.iter().filter(|(OrderedScore(s), _)| {
                let ge_min = if min_inc { *s >= min } else { *s > min };
                let le_max = if max_inc { *s <= max } else { *s < max };
                ge_min && le_max
            }).count(),
        }
    }

    pub fn range(&self, opts: &ZRangeOpts) -> Vec<(Bytes, f64)> {
        let n = self.len();
        if n == 0 {
            return Vec::new();
        }

        if opts.by_score {
            let min = opts.min_score;
            let min_inc = opts.min_inc;
            let max = opts.max_score;
            let max_inc = opts.max_inc;
            let filter_fn = move |item: &&(OrderedScore, Bytes)| {
                let s = item.0.0;
                let ge_min = if min_inc { s >= min } else { s > min };
                let le_max = if max_inc { s <= max } else { s < max };
                ge_min && le_max
            };

            let get_items: Box<dyn Iterator<Item = &(OrderedScore, Bytes)>> = match self {
                RudisZSet::Small(v) => Box::new(v.iter().filter(filter_fn)),
                RudisZSet::Full { tree, .. } => Box::new(tree.iter().filter(filter_fn)),
            };

            if opts.rev {
                let rev_items: Vec<_> = get_items.collect();
                let skipped = rev_items.into_iter().rev().skip(opts.offset);
                if let Some(c) = opts.count {
                    skipped.take(c).map(|(OrderedScore(s), m)| (m.clone(), *s)).collect()
                } else {
                    skipped.map(|(OrderedScore(s), m)| (m.clone(), *s)).collect()
                }
            } else {
                let skipped = get_items.skip(opts.offset);
                if let Some(c) = opts.count {
                    skipped.take(c).map(|(OrderedScore(s), m)| (m.clone(), *s)).collect()
                } else {
                    skipped.map(|(OrderedScore(s), m)| (m.clone(), *s)).collect()
                }
            }
        } else {
            let mut start = opts.start;
            let mut stop = opts.stop;
            let n_i = n as i64;
            if start < 0 { start = (n_i + start).max(0); }
            if stop < 0 { stop = n_i + stop; }
            if start > stop || start >= n_i { return Vec::new(); }
            let start_u = start.max(0) as usize;
            let stop_u = (stop.min(n_i - 1) as usize).max(start_u);
            let limit = stop_u - start_u + 1;

            match self {
                RudisZSet::Small(v) => {
                    if opts.rev {
                        v.iter().rev().skip(start_u).take(limit).map(|(OrderedScore(s), m)| (m.clone(), *s)).collect()
                    } else {
                        v.iter().skip(start_u).take(limit).map(|(OrderedScore(s), m)| (m.clone(), *s)).collect()
                    }
                }
                RudisZSet::Full { tree, .. } => {
                    if opts.rev {
                        tree.iter().rev().skip(start_u).take(limit).map(|(OrderedScore(s), m)| (m.clone(), *s)).collect()
                    } else {
                        tree.iter().skip(start_u).take(limit).map(|(OrderedScore(s), m)| (m.clone(), *s)).collect()
                    }
                }
            }
        }
    }

    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let n = count.min(self.len());
        let mut popped = Vec::with_capacity(n);
        match self {
            RudisZSet::Small(v) => {
                for _ in 0..n {
                    let (OrderedScore(s), m) = v.remove(0);
                    popped.push((m, s));
                }
            }
            RudisZSet::Full { dict, tree } => {
                for _ in 0..n {
                    if let Some((OrderedScore(s), m)) = tree.pop_first() {
                        dict.remove(&m);
                        popped.push((m, s));
                    }
                }
            }
        }
        popped
    }

    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let n = count.min(self.len());
        let mut popped = Vec::with_capacity(n);
        match self {
            RudisZSet::Small(v) => {
                for _ in 0..n {
                    let (OrderedScore(s), m) = v.pop().unwrap();
                    popped.push((m, s));
                }
            }
            RudisZSet::Full { dict, tree } => {
                for _ in 0..n {
                    if let Some((OrderedScore(s), m)) = tree.pop_last() {
                        dict.remove(&m);
                        popped.push((m, s));
                    }
                }
            }
        }
        popped
    }

    pub fn for_each<F: FnMut(&Bytes, f64)>(&self, mut f: F) {
        match self {
            RudisZSet::Small(v) => {
                for (OrderedScore(s), m) in v {
                    f(m, *s);
                }
            }
            RudisZSet::Full { dict, .. } => {
                for (m, s) in dict {
                    f(m, *s);
                }
            }
        }
    }
}

impl PartialEq for RudisZSet {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        match (self, other) {
            (RudisZSet::Small(a), RudisZSet::Small(b)) => a == b,
            _ => {
                let mut a_items: Vec<(OrderedScore, Bytes)> = Vec::with_capacity(self.len());
                self.for_each(|m, s| a_items.push((OrderedScore(s), m.clone())));
                a_items.sort();
                let mut b_items: Vec<(OrderedScore, Bytes)> = Vec::with_capacity(other.len());
                other.for_each(|m, s| b_items.push((OrderedScore(s), m.clone())));
                b_items.sort();
                a_items == b_items
            }
        }
    }
}

impl Eq for RudisZSet {}

const SMALL_SET_LIMIT: usize = 64;

#[derive(Clone, Debug)]
pub enum RudisSet {
    Small(Vec<Bytes>),
    Full(hashbrown::HashSet<Bytes>),
}

impl Default for RudisSet {
    fn default() -> Self {
        Self::new()
    }
}

pub enum RudisSetIter<'a> {
    Small(std::slice::Iter<'a, Bytes>),
    Full(hashbrown::hash_set::Iter<'a, Bytes>),
}

impl<'a> Iterator for RudisSetIter<'a> {
    type Item = &'a Bytes;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RudisSetIter::Small(it) => it.next(),
            RudisSetIter::Full(it) => it.next(),
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            RudisSetIter::Small(it) => it.size_hint(),
            RudisSetIter::Full(it) => it.size_hint(),
        }
    }
}

impl<'a> ExactSizeIterator for RudisSetIter<'a> {}

impl<'a> IntoIterator for &'a RudisSet {
    type Item = &'a Bytes;
    type IntoIter = RudisSetIter<'a>;
    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl RudisSet {
    #[inline(always)]
    pub fn new() -> Self {
        RudisSet::Small(Vec::new())
    }

    #[inline(always)]
    pub fn with_capacity(cap: usize) -> Self {
        if cap <= SMALL_SET_LIMIT {
            RudisSet::Small(Vec::with_capacity(cap))
        } else {
            RudisSet::Full(hashbrown::HashSet::with_capacity(cap))
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            RudisSet::Small(v) => v.len(),
            RudisSet::Full(s) => s.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        match self {
            RudisSet::Small(v) => v.is_empty(),
            RudisSet::Full(s) => s.is_empty(),
        }
    }

    #[inline(always)]
    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            RudisSet::Small(v) => {
                for m in v {
                    if m.as_ref() == member {
                        return true;
                    }
                }
                false
            }
            RudisSet::Full(s) => s.contains(member),
        }
    }

    pub fn insert(&mut self, member: Bytes) -> bool {
        match self {
            RudisSet::Small(v) => {
                let m_bytes = member.as_ref();
                for m in v.iter() {
                    if m.as_ref() == m_bytes {
                        return false;
                    }
                }
                v.push(member);
                if v.len() > SMALL_SET_LIMIT {
                    let mut set = hashbrown::HashSet::with_capacity(v.len());
                    for m in v.drain(..) {
                        set.insert(m);
                    }
                    *self = RudisSet::Full(set);
                }
                true
            }
            RudisSet::Full(s) => s.insert(member),
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            RudisSet::Small(v) => {
                if let Some(pos) = v.iter().position(|m| m.as_ref() == member) {
                    v.swap_remove(pos);
                    true
                } else {
                    false
                }
            }
            RudisSet::Full(s) => s.remove(member),
        }
    }

    pub fn to_vec(&self) -> Vec<Bytes> {
        match self {
            RudisSet::Small(v) => v.clone(),
            RudisSet::Full(s) => s.iter().cloned().collect(),
        }
    }

    pub fn pop(&mut self) -> Option<Bytes> {
        match self {
            RudisSet::Small(v) => v.pop(),
            RudisSet::Full(s) => {
                if let Some(elem) = s.iter().next().cloned() {
                    s.remove(&elem);
                    Some(elem)
                } else {
                    None
                }
            }
        }
    }

    #[inline(always)]
    pub fn iter(&self) -> RudisSetIter<'_> {
        match self {
            RudisSet::Small(v) => RudisSetIter::Small(v.iter()),
            RudisSet::Full(s) => RudisSetIter::Full(s.iter()),
        }
    }
}

impl PartialEq for RudisSet {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        match (self, other) {
            (RudisSet::Small(a), RudisSet::Small(b)) => {
                a.iter().all(|m| b.iter().any(|x| x == m))
            }
            (RudisSet::Full(a), RudisSet::Full(b)) => a == b,
            _ => {
                for m in self.iter() {
                    if !other.contains(m.as_ref()) {
                        return false;
                    }
                }
                true
            }
        }
    }
}

impl Eq for RudisSet {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    pub fn to_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    pub fn parse_exact(s: &str) -> Result<Self, &'static str> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err("Invalid stream ID specified as stream command argument");
        }
        let ms: u64 = parts[0]
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        let seq: u64 = parts[1]
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        Ok(Self { ms, seq })
    }

    pub fn parse(s: &str) -> Result<Self, &'static str> {
        if s == "0" || s == "0-0" {
            return Ok(Self::new(0, 0));
        }
        if let Some((ms_s, seq_s)) = s.split_once('-') {
            let ms: u64 = ms_s.parse().map_err(|_| "Invalid stream ID specified as stream command argument")?;
            let seq: u64 = seq_s.parse().map_err(|_| "Invalid stream ID specified as stream command argument")?;
            Ok(Self::new(ms, seq))
        } else {
            let ms: u64 = s.parse().map_err(|_| "Invalid stream ID specified as stream command argument")?;
            Ok(Self::new(ms, 0))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamAddId {
    Auto,
    AutoSeq(u64),
    Explicit(StreamId),
}

impl StreamAddId {
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        if s == "*" {
            return Ok(StreamAddId::Auto);
        }
        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms: u64 = ms_str
                .parse()
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            if seq_str == "*" {
                return Ok(StreamAddId::AutoSeq(ms));
            }
            let seq: u64 = seq_str
                .parse()
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            return Ok(StreamAddId::Explicit(StreamId::new(ms, seq)));
        }
        Err("Invalid stream ID specified as stream command argument")
    }
}

pub fn parse_range_bound(
    s: &str,
    is_start: bool,
) -> Result<std::ops::Bound<StreamId>, &'static str> {
    if s == "-" {
        return Ok(std::ops::Bound::Unbounded);
    }
    if s == "+" {
        return Ok(std::ops::Bound::Unbounded);
    }
    let (s_trim, exclusive) = if let Some(stripped) = s.strip_prefix('(') {
        (stripped, true)
    } else {
        (s, false)
    };
    let id = if let Some((ms_str, seq_str)) = s_trim.split_once('-') {
        let ms: u64 = ms_str
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        let seq: u64 = seq_str
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        StreamId::new(ms, seq)
    } else {
        let ms: u64 = s_trim
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        let seq = if is_start { 0 } else { u64::MAX };
        StreamId::new(ms, seq)
    };
    if exclusive {
        Ok(std::ops::Bound::Excluded(id))
    } else {
        Ok(std::ops::Bound::Included(id))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPelEntry {
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConsumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub pel: std::collections::BTreeMap<StreamId, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub consumers: HashMap<Bytes, StreamConsumer>,
    pub pel: std::collections::BTreeMap<StreamId, StreamPelEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RudisStream {
    pub entries: std::collections::BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub groups: HashMap<Bytes, StreamGroup>,
}

impl RudisStream {
    pub fn new() -> Self {
        Self {
            entries: std::collections::BTreeMap::new(),
            last_id: StreamId::default(),
            groups: HashMap::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RudisValue {
    String(Bytes),
    Int(i64),
    SmallHash(Vec<(Bytes, Bytes)>),
    Hash(HashMap<Bytes, Bytes>),
    List(std::collections::VecDeque<Bytes>),
    Set(RudisSet),
    ZSet(RudisZSet),
    HyperLogLog(Box<[u8; 16384]>),
    Stream(RudisStream),
}

#[derive(Clone, Debug)]
pub struct RudisEntry {
    pub key: Bytes,
    pub val: RudisValue,
    pub expire_at: Option<Instant>,
}

#[inline(always)]
pub fn hash_key(key: &[u8]) -> u64 {
    hash64(key)
}

#[inline(always)]
pub fn fingerprint(hash: u64) -> u8 {
    (hash >> 57) as u8 & 0x7F
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn match_control_bytes_simd(ptr: *const u8, target: u8) -> u16 {
    use std::arch::x86_64::*;
    unsafe {
        let group = _mm_loadu_si128(ptr as *const __m128i);
        let match_target = _mm_set1_epi8(target as i8);
        let cmp = _mm_cmpeq_epi8(group, match_target);
        _mm_movemask_epi8(cmp) as u16
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
unsafe fn match_control_bytes_simd(ptr: *const u8, target: u8) -> u16 {
    let mut mask = 0u16;
    for i in 0..16 {
        unsafe {
            if *ptr.add(i) == target {
                mask |= 1 << i;
            }
        }
    }
    mask
}

#[inline(always)]
fn match_control_bytes(ctrl: &[u8], offset: usize, target: u8) -> u16 {
    unsafe { match_control_bytes_simd(ctrl.as_ptr().add(offset), target) }
}

/// A high-performance flat hash table utilizing 16-slot SIMD group probing
/// with inlined values and expiration metadata.
pub struct RudisFlatTable {
    ctrl: Vec<u8>,
    pub slots: Vec<Option<RudisEntry>>,
    pub capacity: usize,
    mask: usize,
    items: usize,
    growth_left: usize,
    pub slot_counts: Box<[u32; 16384]>,
}

impl RudisFlatTable {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two().max(GROUP_SIZE);
        let mut ctrl = vec![EMPTY; cap + GROUP_SIZE];
        ctrl.copy_within(0..GROUP_SIZE, cap);

        let mut slots = Vec::with_capacity(cap);
        for _ in 0..cap {
            slots.push(None);
        }

        Self {
            ctrl,
            slots,
            capacity: cap,
            mask: cap - 1,
            items: 0,
            growth_left: cap * 7 / 8,
            slot_counts: vec![0u32; 16384].into_boxed_slice().try_into().unwrap(),
        }
    }

    #[inline(always)]
    fn set_ctrl(&mut self, idx: usize, byte: u8) {
        self.ctrl[idx] = byte;
        if idx < GROUP_SIZE {
            self.ctrl[self.capacity + idx] = byte;
        }
    }

    /// Finds the index of a matching key, if present.
    pub fn find(&self, key: &[u8], hash: u64) -> Option<usize> {
        let tag = fingerprint(hash);
        let mut idx = (hash as usize) & self.mask;
        let mut step = 0;

        loop {
            let match_mask = match_control_bytes(&self.ctrl, idx, tag);
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                if let Some(ref entry) = self.slots[slot_idx] {
                    if entry.key.as_ref() == key {
                        return Some(slot_idx);
                    }
                }
                bits &= bits - 1;
            }

            let empty_mask = match_control_bytes(&self.ctrl, idx, EMPTY);
            if empty_mask != 0 {
                return None;
            }

            step += GROUP_SIZE;
            idx = (idx + step) & self.mask;
        }
    }

    /// Searches for key and returns either `(Some(existing_slot_idx), candidate_insert_idx)`
    /// or `(None, candidate_insert_idx)`.
    fn find_or_prepare_insert(&self, key: &[u8], hash: u64) -> (Option<usize>, usize) {
        let tag = fingerprint(hash);
        let mut idx = (hash as usize) & self.mask;
        let mut step = 0;
        let mut first_free: Option<usize> = None;

        loop {
            let match_mask = match_control_bytes(&self.ctrl, idx, tag);
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                if let Some(ref entry) = self.slots[slot_idx] {
                    if entry.key.as_ref() == key {
                        return (Some(slot_idx), slot_idx);
                    }
                }
                bits &= bits - 1;
            }

            if first_free.is_none() {
                let del_mask = match_control_bytes(&self.ctrl, idx, DELETED);
                if del_mask != 0 {
                    let offset = del_mask.trailing_zeros() as usize;
                    first_free = Some((idx + offset) & self.mask);
                }
            }

            let empty_mask = match_control_bytes(&self.ctrl, idx, EMPTY);
            if empty_mask != 0 {
                let free_idx = match first_free {
                    Some(f) => f,
                    None => {
                        let offset = empty_mask.trailing_zeros() as usize;
                        (idx + offset) & self.mask
                    }
                };
                return (None, free_idx);
            }

            step += GROUP_SIZE;
            idx = (idx + step) & self.mask;
        }
    }

    fn resize(&mut self, new_cap: usize) {
        let mut new_table = RudisFlatTable::new(new_cap);
        for opt in self.slots.drain(..) {
            if let Some(entry) = opt {
                let h = hash_key(&entry.key);
                let (_, insert_idx) = new_table.find_or_prepare_insert(&entry.key, h);
                let tag = fingerprint(h);
                new_table.set_ctrl(insert_idx, tag);
                new_table.slots[insert_idx] = Some(entry);
                new_table.items += 1;
                new_table.growth_left = new_table.growth_left.saturating_sub(1);
            }
        }
        new_table.slot_counts = self.slot_counts.clone();
        *self = new_table;
    }

    pub fn insert(&mut self, entry: RudisEntry) -> Option<RudisEntry> {
        if self.growth_left == 0 {
            self.resize(self.capacity * 2);
        }

        let h = hash_key(&entry.key);
        let (existing, insert_idx) = self.find_or_prepare_insert(&entry.key, h);

        if let Some(idx) = existing {
            let old = self.slots[idx].replace(entry);
            old
        } else {
            let tag = fingerprint(h);
            self.set_ctrl(insert_idx, tag);
            let slot = crate::router::key_slot(&entry.key) as usize;
            self.slot_counts[slot] += 1;
            self.slots[insert_idx] = Some(entry);
            self.items += 1;
            self.growth_left = self.growth_left.saturating_sub(1);
            None
        }
    }

    pub fn remove(&mut self, slot_idx: usize) -> Option<RudisEntry> {
        self.set_ctrl(slot_idx, DELETED);
        self.items -= 1;
        let entry = self.slots[slot_idx].take();
        if let Some(ref e) = entry {
            let slot = crate::router::key_slot(&e.key) as usize;
            self.slot_counts[slot] = self.slot_counts[slot].saturating_sub(1);
        }
        entry
    }


    #[inline(always)]
    pub fn get_slot(&self, idx: usize) -> Option<&RudisEntry> {
        self.slots[idx].as_ref()
    }

    #[inline(always)]
    pub fn get_slot_mut(&mut self, idx: usize) -> Option<&mut RudisEntry> {
        self.slots[idx].as_mut()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.items
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        let cap = self.capacity;
        self.ctrl.fill(EMPTY);
        self.ctrl.copy_within(0..GROUP_SIZE, cap);
        self.slots.fill(None);
        self.items = 0;
        self.growth_left = (cap * 7) / 8;
        self.slot_counts.fill(0);
    }
}

/// The complete thread-local Rudis storage engine unifying:
/// 1. Flat SIMD-accelerated hash table (`RudisFlatTable`)
/// 2. Inlined TTL expiration
/// 3. Secondary cluster slot index
pub struct RudisTable {
    table: RudisFlatTable,
    sample_cursor: usize,
}

impl RudisTable {
    pub fn new() -> Self {
        Self {
            table: RudisFlatTable::new(64),
            sample_cursor: 0,
        }
    }

    #[inline]
    fn check_expired_slot(&mut self, slot_idx: usize) -> bool {
        let is_exp = if let Some(entry) = self.table.get_slot(slot_idx) {
            if let Some(expire_at) = entry.expire_at {
                Instant::now() >= expire_at
            } else {
                false
            }
        } else {
            false
        };

        if is_exp {
            self.table.remove(slot_idx);
            true
        } else {
            false
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::String(b) => Ok(Some(b.clone())),
                    RudisValue::Int(n) => Ok(Some(Self::format_i64(*n))),
                    RudisValue::HyperLogLog(regs) => Ok(Some(Bytes::copy_from_slice(&regs[..]))),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_entry(&mut self, key: &[u8]) -> Option<(RudisValue, Option<Duration>)> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return None;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let ttl = entry.expire_at.and_then(|exp| {
                    let now = Instant::now();
                    if exp > now {
                        Some(exp.duration_since(now))
                    } else {
                        None
                    }
                });
                return Some((entry.val.clone(), ttl));
            }
        }
        None
    }

    #[inline(always)]
    pub fn format_i64(n: i64) -> Bytes {
        match n {
            0 => Bytes::from_static(b"0"),
            1 => Bytes::from_static(b"1"),
            2 => Bytes::from_static(b"2"),
            3 => Bytes::from_static(b"3"),
            4 => Bytes::from_static(b"4"),
            5 => Bytes::from_static(b"5"),
            6 => Bytes::from_static(b"6"),
            7 => Bytes::from_static(b"7"),
            8 => Bytes::from_static(b"8"),
            9 => Bytes::from_static(b"9"),
            10 => Bytes::from_static(b"10"),
            -1 => Bytes::from_static(b"-1"),
            _ => {
                let mut buf = [0u8; 24];
                let mut i = buf.len();
                let val = n;
                let neg = val < 0;
                let mut uval = if neg {
                    if val == i64::MIN {
                        return Bytes::from_static(b"-9223372036854775808");
                    }
                    (-val) as u64
                } else {
                    val as u64
                };
                while uval > 0 {
                    i -= 1;
                    buf[i] = b'0' + (uval % 10) as u8;
                    uval /= 10;
                }
                if neg {
                    i -= 1;
                    buf[i] = b'-';
                }
                Bytes::copy_from_slice(&buf[i..])
            }
        }
    }

    #[inline(always)]
    pub fn parse_i64_bytes(bytes: &[u8]) -> Option<i64> {
        if bytes.is_empty() {
            return None;
        }
        let (neg, s) = match bytes[0] {
            b'-' => (true, &bytes[1..]),
            b'+' => (false, &bytes[1..]),
            _ => (false, bytes),
        };
        if s.is_empty() {
            return None;
        }
        let mut val: u64 = 0;
        for &b in s {
            if !b.is_ascii_digit() {
                return None;
            }
            val = val.checked_mul(10)?.checked_add((b - b'0') as u64)?;
        }
        if neg {
            if val > (i64::MIN.unsigned_abs()) {
                return None;
            }
            Some(-(val as i64))
        } else {
            if val > (i64::MAX as u64) {
                return None;
            }
            Some(val as i64)
        }
    }

    pub fn set(&mut self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        let h = hash_key(&key);
        let expire_at = expire_in.map(|d| Instant::now() + d);
        let val = if let Some(int_val) = Self::parse_i64_bytes(&value) {
            RudisValue::Int(int_val)
        } else {
            RudisValue::String(value)
        };
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing {
            if let Some(entry) = self.table.get_slot_mut(idx) {
                entry.val = val;
                entry.expire_at = expire_at;
                return;
            }
        }

        let entry = RudisEntry {
            key,
            val,
            expire_at,
        };
        self.table.insert(entry);
    }

    pub fn del(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            let was_exp = self.check_expired_slot(idx);
            if was_exp {
                return false;
            }
            if self.table.remove(idx).is_some() {
                return true;
            }
        }
        false
    }

    pub fn exists(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                false
            } else {
                true
            }
        } else {
            false
        }
    }

    pub fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<i64, String> {
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing {
            let was_exp = self.check_expired_slot(idx);
            if !was_exp {
                if let Some(entry) = self.table.get_slot_mut(idx) {
                    match &mut entry.val {
                        RudisValue::Int(n) => {
                            let nv = n.checked_add(delta).ok_or_else(|| {
                                "increment or decrement would overflow".to_string()
                            })?;
                            *n = nv;
                            return Ok(nv);
                        }
                        RudisValue::String(b) => {
                            let current = Self::parse_i64_bytes(b).ok_or_else(|| {
                                "value is not an integer or out of range".to_string()
                            })?;
                            let nv = current.checked_add(delta).ok_or_else(|| {
                                "increment or decrement would overflow".to_string()
                            })?;
                            entry.val = RudisValue::Int(nv);
                            return Ok(nv);
                        }
                        _ => {
                            return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
                        }
                    }
                }
            }
        }

        let new_val = delta;
        let entry = RudisEntry {
            key,
            val: RudisValue::Int(new_val),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(new_val)
    }

    pub fn expire(&mut self, key: &[u8], duration: Duration) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return false;
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                entry.expire_at = Some(Instant::now() + duration);
                return true;
            }
        }
        false
    }

    pub fn persist(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return false;
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                if entry.expire_at.is_some() {
                    entry.expire_at = None;
                    return true;
                }
            }
        }
        false
    }

    pub fn ttl(&mut self, key: &[u8], in_millis: bool) -> i64 {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return -2;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match entry.expire_at {
                    Some(expire_at) => {
                        let now = Instant::now();
                        if now >= expire_at {
                            self.check_expired_slot(idx);
                            -2
                        } else {
                            let diff = expire_at.duration_since(now);
                            if in_millis {
                                diff.as_millis() as i64
                            } else {
                                diff.as_secs() as i64
                            }
                        }
                    }
                    None => -1,
                }
            } else {
                -2
            }
        } else {
            -2
        }
    }

    pub fn expiretime(&mut self, key: &[u8], in_millis: bool) -> i64 {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return -2;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match entry.expire_at {
                    Some(expire_at) => {
                        let now = Instant::now();
                        if now >= expire_at {
                            self.check_expired_slot(idx);
                            -2
                        } else {
                            let diff = expire_at.duration_since(now);
                            let now_epoch = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or(Duration::ZERO);
                            let target_epoch = now_epoch + diff;
                            if in_millis {
                                target_epoch.as_millis() as i64
                            } else {
                                target_epoch.as_secs() as i64
                            }
                        }
                    }
                    None => -1,
                }
            } else {
                -2
            }
        } else {
            -2
        }
    }

    pub fn keys(&mut self, pattern: &[u8]) -> Vec<Bytes> {
        let mut res = Vec::new();
        let cap = self.table.capacity();
        for i in 0..cap {
            if self.check_expired_slot(i) {
                continue;
            }
            if let Some(entry) = self.table.get_slot(i) {
                if crate::pubsub::glob_match(pattern, &entry.key) {
                    res.push(entry.key.clone());
                }
            }
        }
        res
    }

    pub fn scan(
        &mut self,
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> (usize, Vec<Bytes>) {
        let cap = self.table.capacity();
        if cursor >= cap || cap == 0 {
            return (0, Vec::new());
        }
        let mut res = Vec::new();
        let mut idx = cursor;
        while idx < cap {
            if !self.check_expired_slot(idx) {
                if let Some(entry) = self.table.get_slot(idx) {
                    let matches = match pattern {
                        Some(pat) => crate::pubsub::glob_match(pat, &entry.key),
                        None => true,
                    };
                    if matches {
                        res.push(entry.key.clone());
                    }
                }
            }
            idx += 1;
            if res.len() >= count {
                break;
            }
        }
        let next_cursor = if idx >= cap { 0 } else { idx };
        (next_cursor, res)
    }

    pub fn random_key(&mut self) -> Option<Bytes> {
        let cap = self.table.capacity();
        if self.table.len() == 0 || cap == 0 {
            return None;
        }
        self.sample_cursor = (self.sample_cursor + 17) & (cap - 1);
        let start = self.sample_cursor;
        for i in 0..cap {
            let idx = (start + i) & (cap - 1);
            if self.check_expired_slot(idx) {
                continue;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                return Some(entry.key.clone());
            }
        }
        None
    }

    pub fn flushdb(&mut self) {
        self.table.clear();
    }

    pub fn dbsize(&mut self) -> usize {
        self.table.len()
    }

    pub fn type_of(&mut self, key: &[u8]) -> &'static str {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return "none";
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::String(_) | RudisValue::Int(_) => "string",
                    RudisValue::Hash(_) | RudisValue::SmallHash(_) => "hash",
                    RudisValue::List(_) => "list",
                    RudisValue::Set(_) => "set",
                    RudisValue::ZSet(_) => "zset",
                    RudisValue::HyperLogLog(_) => "string",
                    RudisValue::Stream(_) => "stream",
                }
            } else {
                "none"
            }
        } else {
            "none"
        }
    }

    pub fn touch(&mut self, keys: &[Bytes]) -> usize {
        let mut count = 0;
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if !self.check_expired_slot(idx) {
                    count += 1;
                }
            }
        }
        count
    }

    pub fn rename(&mut self, src: &[u8], dst: Bytes, nx: bool) -> Result<bool, &'static str> {
        if src == dst.as_ref() {
            let h = hash_key(src);
            if let Some(idx) = self.table.find(src, h) {
                if !self.check_expired_slot(idx) {
                    return if nx { Ok(false) } else { Ok(true) };
                }
            }
            return Err("no such key");
        }

        let h_src = hash_key(src);
        let src_idx = match self.table.find(src, h_src) {
            Some(idx) => {
                if self.check_expired_slot(idx) {
                    return Err("no such key");
                }
                idx
            }
            None => return Err("no such key"),
        };

        if nx {
            let h_dst = hash_key(&dst);
            if let Some(dst_idx) = self.table.find(&dst, h_dst) {
                if !self.check_expired_slot(dst_idx) {
                    return Ok(false);
                }
            }
        }

        // Remove src
        let mut entry = self.table.remove(src_idx).unwrap();

        // If dst exists, remove it first
        let h_dst = hash_key(&dst);
        if let Some(dst_idx) = self.table.find(&dst, h_dst) {
            self.table.remove(dst_idx);
        }

        // Update entry key to dst and insert
        entry.key = dst;
        self.table.insert(entry);

        Ok(true)
    }

    pub fn setnx(&mut self, key: Bytes, value: Bytes) -> bool {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if !self.check_expired_slot(idx) {
                return false;
            }
        }
        self.set(key, value, None);
        true
    }

    pub fn getset(&mut self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                self.set(key, value, None);
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::String(old) => {
                        let prev = old.clone();
                        *old = value;
                        entry.expire_at = None;
                        Ok(Some(prev))
                    }
                    RudisValue::Int(n) => {
                        let prev = Self::format_i64(*n);
                        if let Some(int_val) = Self::parse_i64_bytes(&value) {
                            entry.val = RudisValue::Int(int_val);
                        } else {
                            entry.val = RudisValue::String(value);
                        }
                        entry.expire_at = None;
                        Ok(Some(prev))
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            self.set(key, value, None);
            Ok(None)
        }
    }

    pub fn getdel(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::String(s) => {
                        let val = s.clone();
                        self.table.remove(idx);
                        Ok(Some(val))
                    }
                    RudisValue::Int(n) => {
                        let val = Self::format_i64(*n);
                        self.table.remove(idx);
                        Ok(Some(val))
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn append(&mut self, key: Bytes, val_to_append: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                let new_val = Bytes::copy_from_slice(val_to_append);
                let len = new_val.len();
                self.set(key, new_val, None);
                return Ok(len);
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::String(s) => {
                        let mut combined = Vec::with_capacity(s.len() + val_to_append.len());
                        combined.extend_from_slice(s);
                        combined.extend_from_slice(val_to_append);
                        let len = combined.len();
                        *s = Bytes::from(combined);
                        Ok(len)
                    }
                    RudisValue::Int(n) => {
                        let s = Self::format_i64(*n);
                        let mut combined = Vec::with_capacity(s.len() + val_to_append.len());
                        combined.extend_from_slice(&s);
                        combined.extend_from_slice(val_to_append);
                        let len = combined.len();
                        entry.val = RudisValue::String(Bytes::from(combined));
                        Ok(len)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                let new_val = Bytes::copy_from_slice(val_to_append);
                let len = new_val.len();
                self.set(key, new_val, None);
                Ok(len)
            }
        } else {
            let new_val = Bytes::copy_from_slice(val_to_append);
            let len = new_val.len();
            self.set(key, new_val, None);
            Ok(len)
        }
    }

    pub fn strlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::String(s) => Ok(s.len()),
                    RudisValue::Int(n) => Ok(Self::format_i64(*n).len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn hset(&mut self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<usize, &'static str> {
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing {
            if !self.check_expired_slot(idx) {
                if let Some(entry) = self.table.get_slot_mut(idx) {
                    match &mut entry.val {
                        RudisValue::SmallHash(pairs) => {
                            let mut added = 0;
                            for (f, v) in fields {
                                if let Some(pos) = pairs.iter().position(|(k, _)| *k == f) {
                                    pairs[pos].1 = v;
                                } else {
                                    pairs.push((f, v));
                                    added += 1;
                                }
                            }
                            if pairs.len() > 64 {
                                let map: HashMap<Bytes, Bytes> = pairs.drain(..).collect();
                                entry.val = RudisValue::Hash(map);
                            }
                            return Ok(added);
                        }
                        RudisValue::Hash(map) => {
                            let mut added = 0;
                            for (f, v) in fields {
                                if map.insert(f, v).is_none() {
                                    added += 1;
                                }
                            }
                            return Ok(added);
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                }
            }
        }

        let (val, added) = if fields.len() <= 64 {
            let added = fields.len();
            (RudisValue::SmallHash(fields), added)
        } else {
            let mut map = HashMap::new();
            let mut added = 0;
            for (f, v) in fields {
                if map.insert(f, v).is_none() {
                    added += 1;
                }
            }
            (RudisValue::Hash(map), added)
        };
        let entry = RudisEntry {
            key,
            val,
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(added)
    }

    pub fn hget(&mut self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => {
                        Ok(pairs.iter().find(|(k, _)| k == field).map(|(_, v)| v.clone()))
                    }
                    RudisValue::Hash(map) => Ok(map.get(field).cloned()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn hmget(
        &mut self,
        key: &[u8],
        fields: &[Bytes],
    ) -> Result<Vec<Option<Bytes>>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(vec![None; fields.len()]);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => {
                        Ok(fields.iter().map(|f| pairs.iter().find(|(k, _)| k == f).map(|(_, v)| v.clone())).collect())
                    }
                    RudisValue::Hash(map) => {
                        Ok(fields.iter().map(|f| map.get(f).cloned()).collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(vec![None; fields.len()])
            }
        } else {
            Ok(vec![None; fields.len()])
        }
    }

    pub fn hdel(&mut self, key: &[u8], fields: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (count, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::SmallHash(pairs) => {
                        let mut c = 0;
                        for f in fields {
                            if let Some(pos) = pairs.iter().position(|(k, _)| k == f) {
                                pairs.swap_remove(pos);
                                c += 1;
                            }
                        }
                        (c, pairs.is_empty())
                    }
                    RudisValue::Hash(map) => {
                        let mut c = 0;
                        for f in fields {
                            if map.remove(f).is_some() {
                                c += 1;
                            }
                        }
                        (c, map.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (0, false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(count)
        } else {
            Ok(0)
        }
    }

    pub fn hexists(&mut self, key: &[u8], field: &[u8]) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(false);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.iter().any(|(k, _)| k == field)),
                    RudisValue::Hash(map) => Ok(map.contains_key(field)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    pub fn hlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.len()),
                    RudisValue::Hash(map) => Ok(map.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn hgetall(&mut self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.clone()),
                    RudisValue::Hash(map) => {
                        Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn hkeys(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.iter().map(|(k, _)| k.clone()).collect()),
                    RudisValue::Hash(map) => Ok(map.keys().cloned().collect()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn hvals(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.iter().map(|(_, v)| v.clone()).collect()),
                    RudisValue::Hash(map) => Ok(map.values().cloned().collect()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    // LIST METHODS
    pub fn lpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                // Key was expired and removed
            } else if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        for v in values {
                            deque.push_front(v);
                        }
                        return Ok(deque.len());
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let mut deque = std::collections::VecDeque::with_capacity(values.len());
        for v in values {
            deque.push_front(v);
        }
        let len = deque.len();
        let entry = RudisEntry {
            key,
            val: RudisValue::List(deque),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(len)
    }

    pub fn rpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                // Key was expired and removed
            } else if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        for v in values {
                            deque.push_back(v);
                        }
                        return Ok(deque.len());
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let mut deque = std::collections::VecDeque::with_capacity(values.len());
        for v in values {
            deque.push_back(v);
        }
        let len = deque.len();
        let entry = RudisEntry {
            key,
            val: RudisValue::List(deque),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(len)
    }

    pub fn lpop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (popped, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        let mut res = Vec::new();
                        for _ in 0..count {
                            if let Some(val) = deque.pop_front() {
                                res.push(val);
                            } else {
                                break;
                            }
                        }
                        let empty = deque.is_empty();
                        (res, empty)
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(popped)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn rpop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (popped, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        let mut res = Vec::new();
                        for _ in 0..count {
                            if let Some(val) = deque.pop_back() {
                                res.push(val);
                            } else {
                                break;
                            }
                        }
                        let empty = deque.is_empty();
                        (res, empty)
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(popped)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn llen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::List(deque) => Ok(deque.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn lindex(&mut self, key: &[u8], index: i64) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::List(deque) => {
                        let n = deque.len() as i64;
                        let actual_idx = if index < 0 { n + index } else { index };
                        if actual_idx >= 0 && actual_idx < n {
                            Ok(deque.get(actual_idx as usize).cloned())
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn lrange(
        &mut self,
        key: &[u8],
        mut start: i64,
        mut stop: i64,
    ) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::List(deque) => {
                        let n = deque.len() as i64;
                        if n == 0 {
                            return Ok(Vec::new());
                        }
                        if start < 0 {
                            start = (n + start).max(0);
                        }
                        if stop < 0 {
                            stop = n + stop;
                        }
                        if start > stop || start >= n {
                            return Ok(Vec::new());
                        }
                        let start_u = start.max(0) as usize;
                        let stop_u = (stop.min(n - 1) as usize).max(start_u);
                        let mut res = Vec::with_capacity(stop_u - start_u + 1);
                        for i in start_u..=stop_u {
                            if let Some(v) = deque.get(i) {
                                res.push(v.clone());
                            }
                        }
                        Ok(res)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    // SET METHODS
    pub fn sadd(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<usize, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                // Key was expired, re-create below
            } else if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Set(set) => {
                        let mut added = 0;
                        for m in members {
                            if set.insert(m) {
                                added += 1;
                            }
                        }
                        return Ok(added);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let mut set = RudisSet::with_capacity(members.len());
        let mut added = 0;
        for m in members {
            if set.insert(m) {
                added += 1;
            }
        }
        let entry = RudisEntry {
            key,
            val: RudisValue::Set(set),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(added)
    }

    pub fn srem(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (removed_count, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Set(set) => {
                        let mut c = 0;
                        for m in members {
                            if set.remove(m) {
                                c += 1;
                            }
                        }
                        let empty = set.is_empty();
                        (c, empty)
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (0, false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(removed_count)
        } else {
            Ok(0)
        }
    }

    pub fn smembers(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Set(set) => Ok(set.to_vec()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn sismember(&mut self, key: &[u8], member: &[u8]) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if let Some(entry) = self.table.get_slot(idx) {
                if let Some(expire_at) = entry.expire_at {
                    if Instant::now() >= expire_at {
                        self.table.remove(idx);
                        return Ok(false);
                    }
                }
                match &entry.val {
                    RudisValue::Set(set) => Ok(set.contains(member)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    pub fn scard(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Set(set) => Ok(set.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn spop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (popped, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Set(set) => {
                        let mut res = Vec::new();
                        for _ in 0..count {
                            if let Some(elem) = set.pop() {
                                res.push(elem);
                            } else {
                                break;
                            }
                        }
                        let empty = set.is_empty();
                        (res, empty)
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(popped)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn sinter(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut sets: Vec<RudisSet> = Vec::new();
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    return Ok(Vec::new());
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => sets.push(s.clone()),
                        _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    }
                }
            } else {
                return Ok(Vec::new());
            }
        }
        if sets.is_empty() {
            return Ok(Vec::new());
        }
        sets.sort_by_key(|s| s.len());
        let first = &sets[0];
        let mut result = Vec::new();
        for m in first.iter() {
            if sets[1..].iter().all(|s| s.contains(m.as_ref())) {
                result.push(m.clone());
            }
        }
        Ok(result)
    }

    pub fn sunion(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        let mut union_set = hashbrown::HashSet::new();
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => {
                            for m in s.iter() {
                                union_set.insert(m.clone());
                            }
                        }
                        _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    }
                }
            }
        }
        Ok(union_set.into_iter().collect())
    }

    pub fn sdiff(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let first_set = {
            let h = hash_key(&keys[0]);
            if let Some(idx) = self.table.find(&keys[0], h) {
                if self.check_expired_slot(idx) {
                    return Ok(Vec::new());
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => s.clone(),
                        _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    }
                } else {
                    return Ok(Vec::new());
                }
            } else {
                return Ok(Vec::new());
            }
        };

        let mut other_sets: Vec<RudisSet> = Vec::new();
        for k in &keys[1..] {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => other_sets.push(s.clone()),
                        _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    }
                }
            }
        }

        let mut diff = Vec::new();
        for m in first_set.iter() {
            if !other_sets.iter().any(|s| s.contains(m.as_ref())) {
                diff.push(m.clone());
            }
        }
        Ok(diff)
    }

    pub fn sinterstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        let members = self.sinter(keys)?;
        let count = members.len();
        self.del(dest.as_ref());
        if count > 0 {
            self.sadd(dest, members)?;
        }
        Ok(count)
    }

    pub fn sunionstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        let members = self.sunion(keys)?;
        let count = members.len();
        self.del(dest.as_ref());
        if count > 0 {
            self.sadd(dest, members)?;
        }
        Ok(count)
    }

    pub fn sdiffstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        let members = self.sdiff(keys)?;
        let count = members.len();
        self.del(dest.as_ref());
        if count > 0 {
            self.sadd(dest, members)?;
        }
        Ok(count)
    }

    pub fn zunionstore(
        &mut self,
        dest: Bytes,
        keys: &[Bytes],
        weights: &[f64],
        agg: Aggregate,
    ) -> Result<usize, &'static str> {
        let items = self.zunion(keys, weights, agg, true)?;
        let count = items.len();
        self.del(dest.as_ref());
        if count > 0 {
            let mut zset = RudisZSet::new();
            for (m, s) in items {
                zset.insert(s, m);
            }
            let entry = RudisEntry {
                key: dest,
                val: RudisValue::ZSet(zset),
                expire_at: None,
            };
            self.table.insert(entry);
        }
        Ok(count)
    }

    pub fn zinterstore(
        &mut self,
        dest: Bytes,
        keys: &[Bytes],
        weights: &[f64],
        agg: Aggregate,
    ) -> Result<usize, &'static str> {
        let items = self.zinter(keys, weights, agg, true)?;
        let count = items.len();
        self.del(dest.as_ref());
        if count > 0 {
            let mut zset = RudisZSet::new();
            for (m, s) in items {
                zset.insert(s, m);
            }
            let entry = RudisEntry {
                key: dest,
                val: RudisValue::ZSet(zset),
                expire_at: None,
            };
            self.table.insert(entry);
        }
        Ok(count)
    }

    pub fn zdiffstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        let items = self.zdiff(keys, true)?;
        let count = items.len();
        self.del(dest.as_ref());
        if count > 0 {
            let mut zset = RudisZSet::new();
            for (m, s) in items {
                zset.insert(s, m);
            }
            let entry = RudisEntry {
                key: dest,
                val: RudisValue::ZSet(zset),
                expire_at: None,
            };
            self.table.insert(entry);
        }
        Ok(count)
    }

    pub fn zunion(
        &mut self,
        keys: &[Bytes],
        weights: &[f64],
        agg: Aggregate,
        _with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let mut acc: hashbrown::HashMap<Bytes, f64> = hashbrown::HashMap::new();
        for (i, k) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::ZSet(zs) => {
                            zs.for_each(|m, s| {
                                let val = s * weight;
                                acc.entry(m.clone())
                                    .and_modify(|cur| {
                                        *cur = match agg {
                                            Aggregate::Sum => *cur + val,
                                            Aggregate::Min => cur.min(val),
                                            Aggregate::Max => cur.max(val),
                                        };
                                    })
                                    .or_insert(val);
                            });
                        }
                        RudisValue::Set(s) => {
                            for m in s.iter() {
                                let val = 1.0 * weight;
                                acc.entry(m.clone())
                                    .and_modify(|cur| {
                                        *cur = match agg {
                                            Aggregate::Sum => *cur + val,
                                            Aggregate::Min => cur.min(val),
                                            Aggregate::Max => cur.max(val),
                                        };
                                    })
                                    .or_insert(val);
                            }
                        }
                        _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    }
                }
            }
        }
        let mut res: Vec<(Bytes, f64)> = acc.into_iter().collect();
        res.sort_by(|(m1, s1), (m2, s2)| {
            s1.partial_cmp(s2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| m1.cmp(m2))
        });
        Ok(res)
    }

    pub fn zinter(
        &mut self,
        keys: &[Bytes],
        weights: &[f64],
        agg: Aggregate,
        _with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut key_maps: Vec<hashbrown::HashMap<Bytes, f64>> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    return Ok(Vec::new());
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    let mut map = hashbrown::HashMap::new();
                    match &entry.val {
                        RudisValue::ZSet(zs) => {
                            zs.for_each(|m, s| {
                                map.insert(m.clone(), s * weight);
                            });
                        }
                        RudisValue::Set(s) => {
                            for m in s.iter() {
                                map.insert(m.clone(), 1.0 * weight);
                            }
                        }
                        _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    }
                    key_maps.push(map);
                } else {
                    return Ok(Vec::new());
                }
            } else {
                return Ok(Vec::new());
            }
        }
        key_maps.sort_by_key(|m| m.len());
        let first = &key_maps[0];
        let mut result = Vec::new();
        for (m, initial_score) in first {
            let mut score = *initial_score;
            let mut present = true;
            for other in &key_maps[1..] {
                if let Some(other_score) = other.get(m) {
                    score = match agg {
                        Aggregate::Sum => score + *other_score,
                        Aggregate::Min => score.min(*other_score),
                        Aggregate::Max => score.max(*other_score),
                    };
                } else {
                    present = false;
                    break;
                }
            }
            if present {
                result.push((m.clone(), score));
            }
        }
        result.sort_by(|(m1, s1), (m2, s2)| {
            s1.partial_cmp(s2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| m1.cmp(m2))
        });
        Ok(result)
    }

    pub fn zdiff(&mut self, keys: &[Bytes], _with_scores: bool) -> Result<Vec<(Bytes, f64)>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let first = {
            let h = hash_key(&keys[0]);
            if let Some(idx) = self.table.find(&keys[0], h) {
                if self.check_expired_slot(idx) {
                    return Ok(Vec::new());
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    let mut items = Vec::new();
                    match &entry.val {
                        RudisValue::ZSet(zs) => {
                            zs.for_each(|m, s| items.push((m.clone(), s)));
                        }
                        RudisValue::Set(s) => {
                            for m in s.iter() {
                                items.push((m.clone(), 1.0));
                            }
                        }
                        _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    }
                    items
                } else {
                    return Ok(Vec::new());
                }
            } else {
                return Ok(Vec::new());
            }
        };

        let mut other_members = hashbrown::HashSet::new();
        for k in &keys[1..] {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::ZSet(zs) => {
                            zs.for_each(|m, _| {
                                other_members.insert(m.clone());
                            });
                        }
                        RudisValue::Set(s) => {
                            for m in s.iter() {
                                other_members.insert(m.clone());
                            }
                        }
                        _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                    }
                }
            }
        }

        let mut diff: Vec<(Bytes, f64)> = first
            .into_iter()
            .filter(|(m, _)| !other_members.contains(m))
            .collect();
        diff.sort_by(|(m1, s1), (m2, s2)| {
            s1.partial_cmp(s2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| m1.cmp(m2))
        });
        Ok(diff)
    }

    pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
        if self.table.slot_counts[slot as usize] == 0 {
            return 0;
        }
        let now = Instant::now();
        let mut count = 0;
        let mut expired_indices = Vec::new();
        for (idx, opt) in self.table.slots.iter().enumerate() {
            if let Some(entry) = opt {
                if crate::router::key_slot(&entry.key) == slot {
                    if let Some(exp) = entry.expire_at {
                        if now >= exp {
                            expired_indices.push(idx);
                            continue;
                        }
                    }
                    count += 1;
                }
            }
        }
        for idx in expired_indices {
            self.table.remove(idx);
        }
        count
    }

    pub fn get_keys_in_slot(&mut self, slot: u16, count: usize) -> Vec<Bytes> {
        let now = Instant::now();
        let mut result = Vec::new();
        let mut expired_indices = Vec::new();
        for (idx, opt) in self.table.slots.iter().enumerate() {
            if let Some(entry) = opt {
                if crate::router::key_slot(&entry.key) == slot {
                    if let Some(exp) = entry.expire_at {
                        if now >= exp {
                            expired_indices.push(idx);
                            continue;
                        }
                    }
                    result.push(entry.key.clone());
                    if result.len() >= count {
                        break;
                    }
                }
            }
        }
        for idx in expired_indices {
            self.table.remove(idx);
        }
        result
    }

    // =========================================================================
    // SORTED SET (ZSET) OPERATIONS
    // =========================================================================

    pub fn zadd(
        &mut self,
        key: Bytes,
        elements: Vec<(f64, Bytes)>,
        flags: ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                // Expired slot has been cleaned up, will insert as new below
            } else if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let mut added_count = 0usize;
                        let mut changed_count = 0usize;
                        let mut new_score_incr = None;

                        for (score, member) in elements {
                            if let Some(old_score) = zset.get_score(&member) {
                                if flags.nx {
                                    continue;
                                }
                                let new_score = if flags.incr { old_score + score } else { score };
                                if flags.gt && new_score <= old_score {
                                    continue;
                                }
                                if flags.lt && new_score >= old_score {
                                    continue;
                                }
                                if new_score != old_score {
                                    zset.insert(new_score, member);
                                    changed_count += 1;
                                }
                                if flags.incr {
                                    new_score_incr = Some(new_score);
                                }
                            } else {
                                if flags.xx {
                                    continue;
                                }
                                zset.insert(score, member);
                                added_count += 1;
                                changed_count += 1;
                                if flags.incr {
                                    new_score_incr = Some(score);
                                }
                            }
                        }

                        let ret_count = if flags.ch { changed_count } else { added_count };
                        return Ok((ret_count, new_score_incr));
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        // Key does not exist
        if flags.xx {
            return Ok((0, None));
        }

        let mut zset = RudisZSet::new();
        let mut added_count = 0usize;
        let mut new_score_incr = None;

        for (score, member) in elements {
            zset.insert(score, member);
            added_count += 1;
            if flags.incr {
                new_score_incr = Some(score);
            }
        }

        let entry = RudisEntry {
            key,
            val: RudisValue::ZSet(zset),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok((added_count, new_score_incr))
    }

    pub fn zscore(&mut self, key: &[u8], member: &[u8]) -> Result<Option<f64>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.get_score(member)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn zcard(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn zrank(
        &mut self,
        key: &[u8],
        member: &[u8],
        rev: bool,
    ) -> Result<Option<usize>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.rank(member, rev)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn zcount(
        &mut self,
        key: &[u8],
        min: f64,
        min_inc: bool,
        max: f64,
        max_inc: bool,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.count(min, min_inc, max, max_inc)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn zincrby(&mut self, key: Bytes, delta: f64, member: Bytes) -> Result<f64, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                // Expired slot has been cleaned up, will insert as new below
            } else if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let new_score = if let Some(old_score) = zset.get_score(&member) {
                            let s = old_score + delta;
                            zset.insert(s, member);
                            s
                        } else {
                            zset.insert(delta, member);
                            delta
                        };
                        return Ok(new_score);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let mut zset = RudisZSet::new();
        zset.insert(delta, member);
        let entry = RudisEntry {
            key,
            val: RudisValue::ZSet(zset),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(delta)
    }

    pub fn zrange(
        &mut self,
        key: &[u8],
        opts: &ZRangeOpts,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.range(opts)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn zrem(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (removed_count, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let mut count = 0usize;
                        for m in members {
                            if zset.remove(m).is_some() {
                                count += 1;
                            }
                        }
                        (count, zset.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (0, false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(removed_count)
        } else {
            Ok(0)
        }
    }

    pub fn zpopmin(&mut self, key: &[u8], count: usize) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (res, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let popped = zset.pop_min(count);
                        (popped, zset.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(res)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn zpopmax(&mut self, key: &[u8], count: usize) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (res, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let popped = zset.pop_max(count);
                        (popped, zset.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(res)
        } else {
            Ok(Vec::new())
        }
    }

    /// Active sampling cycle: samples up to 20 slots starting from cursor and evicts expired keys.
    pub fn active_expire_cycle(&mut self) -> usize {
        let cap = self.table.capacity();
        if cap == 0 || self.table.len() == 0 {
            return 0;
        }

        let mut expired_count = 0;
        let mut checked = 0;
        while checked < 20 {
            let idx = self.sample_cursor % cap;
            self.sample_cursor = (self.sample_cursor + 1) % cap;
            if self.check_expired_slot(idx) {
                expired_count += 1;
            }
            checked += 1;
        }
        expired_count
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.len() == 0
    }

    // BITMAP OPERATIONS
    pub fn setbit(&mut self, key: Bytes, offset: usize, value: u8) -> Result<u8, &'static str> {
        if value > 1 {
            return Err("bit is not an integer or out of range");
        }
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);

        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            let was_exp = self.check_expired_slot(idx);
            if !was_exp {
                if let Some(entry) = self.table.get_slot_mut(idx) {
                    match &mut entry.val {
                        RudisValue::String(b) => {
                            let mut vec = b.to_vec();
                            if vec.len() <= byte_idx {
                                vec.resize(byte_idx + 1, 0);
                            }
                            let old_byte = vec[byte_idx];
                            let old_bit = (old_byte >> bit_idx) & 1;
                            if value == 1 {
                                vec[byte_idx] |= 1 << bit_idx;
                            } else {
                                vec[byte_idx] &= !(1 << bit_idx);
                            }
                            *b = Bytes::from(vec);
                            return Ok(old_bit);
                        }
                        RudisValue::Int(n) => {
                            let mut vec = Self::format_i64(*n).to_vec();
                            if vec.len() <= byte_idx {
                                vec.resize(byte_idx + 1, 0);
                            }
                            let old_byte = vec[byte_idx];
                            let old_bit = (old_byte >> bit_idx) & 1;
                            if value == 1 {
                                vec[byte_idx] |= 1 << bit_idx;
                            } else {
                                vec[byte_idx] &= !(1 << bit_idx);
                            }
                            entry.val = RudisValue::String(Bytes::from(vec));
                            return Ok(old_bit);
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )
                        }
                    }
                }
            }
        }

        let mut vec = vec![0u8; byte_idx + 1];
        if value == 1 {
            vec[byte_idx] |= 1 << bit_idx;
        }
        let entry = RudisEntry {
            key,
            val: RudisValue::String(Bytes::from(vec)),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(0)
    }

    pub fn getbit(&mut self, key: &[u8], offset: usize) -> Result<u8, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::String(b) => {
                        let byte_idx = offset / 8;
                        if byte_idx >= b.len() {
                            return Ok(0);
                        }
                        let bit_idx = 7 - (offset % 8);
                        let bit = (b[byte_idx] >> bit_idx) & 1;
                        return Ok(bit);
                    }
                    RudisValue::Int(n) => {
                        let s = Self::format_i64(*n);
                        let b = &s[..];
                        let byte_idx = offset / 8;
                        if byte_idx >= b.len() {
                            return Ok(0);
                        }
                        let bit_idx = 7 - (offset % 8);
                        let bit = (b[byte_idx] >> bit_idx) & 1;
                        return Ok(bit);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        )
                    }
                }
            }
        }
        Ok(0)
    }

    pub fn bitcount(
        &mut self,
        key: &[u8],
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let bytes_data: Option<Vec<u8>> = match &entry.val {
                    RudisValue::String(b) => Some(b.to_vec()),
                    RudisValue::Int(n) => Some(Self::format_i64(*n).to_vec()),
                    _ => None,
                };
                if let Some(b) = bytes_data {
                    let len = b.len() as i64;
                    if len == 0 {
                        return Ok(0);
                    }
                    let s = match start {
                        Some(v) => {
                            if v < 0 {
                                (len + v).max(0) as usize
                            } else {
                                v.min(len) as usize
                            }
                        }
                        None => 0,
                    };
                    let e = match end {
                        Some(v) => {
                            if v < 0 {
                                (len + v).max(0) as usize
                            } else {
                                v.min(len - 1) as usize
                            }
                        }
                        None => (len - 1) as usize,
                    };
                    if s > e || s >= b.len() {
                        return Ok(0);
                    }
                    let slice = &b[s..=e.min(b.len() - 1)];
                    let count: usize =
                        slice.iter().map(|byte| byte.count_ones() as usize).sum();
                    return Ok(count);
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        Ok(0)
    }

    pub fn bitpos(
        &mut self,
        key: &[u8],
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<i64, &'static str> {
        if bit > 1 {
            return Err("The bit argument must be 1 or 0.");
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(if bit == 0 { 0 } else { -1 });
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let bytes_data: Option<Vec<u8>> = match &entry.val {
                    RudisValue::String(b) => Some(b.to_vec()),
                    RudisValue::Int(n) => Some(Self::format_i64(*n).to_vec()),
                    _ => None,
                };
                if let Some(b) = bytes_data {
                    let len = b.len() as i64;
                    if len == 0 {
                        return Ok(if bit == 0 { 0 } else { -1 });
                    }
                    let s = match start {
                        Some(v) => {
                            if v < 0 {
                                (len + v).max(0) as usize
                            } else {
                                v.min(len) as usize
                            }
                        }
                        None => 0,
                    };
                    let e = match end {
                        Some(v) => {
                            if v < 0 {
                                (len + v).max(0) as usize
                            } else {
                                v.min(len - 1) as usize
                            }
                        }
                        None => (len - 1) as usize,
                    };
                    if s > e || s >= b.len() {
                        return Ok(-1);
                    }
                    for (i, &byte) in b[s..=e.min(b.len() - 1)].iter().enumerate() {
                        let byte_offset = s + i;
                        for bit_idx in 0..8 {
                            let curr_bit = (byte >> (7 - bit_idx)) & 1;
                            if curr_bit == bit {
                                return Ok((byte_offset * 8 + bit_idx) as i64);
                            }
                        }
                    }
                    if bit == 0 && end.is_none() {
                        return Ok((b.len() * 8) as i64);
                    }
                    return Ok(-1);
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        Ok(if bit == 0 { 0 } else { -1 })
    }

    pub fn bitop(
        &mut self,
        op: &str,
        destkey: Bytes,
        srckeys: &[Bytes],
    ) -> Result<usize, &'static str> {
        let op = op.to_uppercase();
        if srckeys.is_empty() {
            return Err("wrong number of arguments for 'bitop' command");
        }
        let mut buffers = Vec::with_capacity(srckeys.len());
        let mut max_len = 0;
        for k in srckeys {
            let h = hash_key(k);
            let b = if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    Vec::new()
                } else if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::String(bytes) => bytes.to_vec(),
                        RudisValue::Int(n) => Self::format_i64(*n).to_vec(),
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )
                        }
                    }
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            max_len = max_len.max(b.len());
            buffers.push(b);
        }

        let mut result = vec![0u8; max_len];
        match op.as_str() {
            "AND" => {
                for i in 0..max_len {
                    let mut b = 0xFF;
                    for buf in &buffers {
                        b &= buf.get(i).copied().unwrap_or(0);
                    }
                    result[i] = b;
                }
            }
            "OR" => {
                for i in 0..max_len {
                    let mut b = 0;
                    for buf in &buffers {
                        b |= buf.get(i).copied().unwrap_or(0);
                    }
                    result[i] = b;
                }
            }
            "XOR" => {
                for i in 0..max_len {
                    let mut b = 0;
                    for buf in &buffers {
                        b ^= buf.get(i).copied().unwrap_or(0);
                    }
                    result[i] = b;
                }
            }
            "NOT" => {
                if buffers.len() != 1 {
                    return Err("BITOP NOT takes only one source key");
                }
                for i in 0..max_len {
                    result[i] = !buffers[0].get(i).copied().unwrap_or(0);
                }
            }
            _ => return Err("syntax error"),
        }

        let len = result.len();
        self.set(destkey, Bytes::from(result), None);
        Ok(len)
    }

    // HYPERLOGLOG OPERATIONS
    pub fn pfadd(&mut self, key: Bytes, elements: &[Bytes]) -> Result<bool, &'static str> {
        let mut updated = false;
        let h = hash_key(&key);
        let mut existing_registers = if let Some(idx) = self.table.find(&key, h) {
            let was_exp = self.check_expired_slot(idx);
            if !was_exp {
                if let Some(entry) = self.table.get_slot_mut(idx) {
                    match &mut entry.val {
                        RudisValue::HyperLogLog(regs) => Some(regs),
                        RudisValue::String(s) if s.len() == 16384 => {
                            let mut arr = Box::new([0u8; 16384]);
                            arr.copy_from_slice(s);
                            entry.val = RudisValue::HyperLogLog(arr);
                            match &mut entry.val {
                                RudisValue::HyperLogLog(regs) => Some(regs),
                                _ => unreachable!(),
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(regs) = existing_registers.as_deref_mut() {
            for elem in elements {
                let h = hash_key(elem);
                let reg_idx = (h & 0x3FFF) as usize; // 14 bits (0..16383)
                let rho = ((h >> 14).leading_zeros() as u8 + 1).min(51);
                if rho > regs[reg_idx] {
                    regs[reg_idx] = rho;
                    updated = true;
                }
            }
            return Ok(updated);
        }

        let mut regs = Box::new([0u8; 16384]);
        for elem in elements {
            let h = hash_key(elem);
            let reg_idx = (h & 0x3FFF) as usize;
            let rho = ((h >> 14).leading_zeros() as u8 + 1).min(51);
            if rho > regs[reg_idx] {
                regs[reg_idx] = rho;
                updated = true;
            }
        }
        let entry = RudisEntry {
            key,
            val: RudisValue::HyperLogLog(regs),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(updated)
    }

    pub fn pfcount(&mut self, keys: &[Bytes]) -> Result<u64, &'static str> {
        let mut merged = [0u8; 16384];
        let mut has_hll = false;

        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if !self.check_expired_slot(idx) {
                    if let Some(entry) = self.table.get_slot(idx) {
                        match &entry.val {
                            RudisValue::HyperLogLog(regs) => {
                                has_hll = true;
                                for i in 0..16384 {
                                    merged[i] = merged[i].max(regs[i]);
                                }
                            }
                            RudisValue::String(s) if s.len() == 16384 => {
                                has_hll = true;
                                for i in 0..16384 {
                                    merged[i] = merged[i].max(s[i]);
                                }
                            }
                            _ => {
                                return Err(
                                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                                )
                            }
                        }
                    }
                }
            }
        }

        if !has_hll {
            return Ok(0);
        }

        const M: f64 = 16384.0;
        const ALPHA: f64 = 0.7213475;
        let mut sum = 0.0;
        let mut zeros = 0;
        for &val in merged.iter() {
            sum += 2.0_f64.powi(-(val as i32));
            if val == 0 {
                zeros += 1;
            }
        }

        let raw_estimate = ALPHA * M * M / sum;
        if raw_estimate <= 2.5 * M && zeros > 0 {
            let count = M * (M / zeros as f64).ln();
            Ok(count.round() as u64)
        } else {
            Ok(raw_estimate.round() as u64)
        }
    }

    pub fn pfmerge(&mut self, destkey: Bytes, srckeys: &[Bytes]) -> Result<(), &'static str> {
        let mut merged = [0u8; 16384];
        let h = hash_key(&destkey);
        if let Some(idx) = self.table.find(&destkey, h) {
            if !self.check_expired_slot(idx) {
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::HyperLogLog(regs) => {
                            for i in 0..16384 {
                                merged[i] = merged[i].max(regs[i]);
                            }
                        }
                        RudisValue::String(s) if s.len() == 16384 => {
                            for i in 0..16384 {
                                merged[i] = merged[i].max(s[i]);
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )
                        }
                    }
                }
            }
        }

        for k in srckeys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if !self.check_expired_slot(idx) {
                    if let Some(entry) = self.table.get_slot(idx) {
                        match &entry.val {
                            RudisValue::HyperLogLog(regs) => {
                                for i in 0..16384 {
                                    merged[i] = merged[i].max(regs[i]);
                                }
                            }
                            RudisValue::String(s) if s.len() == 16384 => {
                                for i in 0..16384 {
                                    merged[i] = merged[i].max(s[i]);
                                }
                            }
                            _ => {
                                return Err(
                                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                                )
                            }
                        }
                    }
                }
            }
        }

        let h = hash_key(&destkey);
        if let Some(idx) = self.table.find(&destkey, h) {
            if let Some(entry) = self.table.get_slot_mut(idx) {
                entry.val = RudisValue::HyperLogLog(Box::new(merged));
                return Ok(());
            }
        }

        let entry = RudisEntry {
            key: destkey,
            val: RudisValue::HyperLogLog(Box::new(merged)),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(())
    }

    fn compute_stream_id(
        stream: &mut RudisStream,
        add_id: StreamAddId,
    ) -> Result<StreamId, &'static str> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let final_id = match add_id {
            StreamAddId::Auto => {
                let ms = if now_ms > stream.last_id.ms {
                    now_ms
                } else {
                    stream.last_id.ms
                };
                let seq = if ms == stream.last_id.ms {
                    if stream.last_id == StreamId::default() && stream.entries.is_empty() {
                        if ms == 0 { 1 } else { 0 }
                    } else {
                        stream.last_id.seq + 1
                    }
                } else {
                    if ms == 0 { 1 } else { 0 }
                };
                StreamId::new(ms, seq)
            }
            StreamAddId::AutoSeq(ms) => {
                if ms < stream.last_id.ms {
                    return Err(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                let seq = if ms == stream.last_id.ms {
                    if stream.last_id == StreamId::default() && stream.entries.is_empty() {
                        if ms == 0 { 1 } else { 0 }
                    } else {
                        stream.last_id.seq + 1
                    }
                } else {
                    if ms == 0 { 1 } else { 0 }
                };
                StreamId::new(ms, seq)
            }
            StreamAddId::Explicit(id) => {
                if id.ms == 0 && id.seq == 0 {
                    return Err("ERR The ID specified in XADD must be greater than 0-0");
                }
                if !stream.entries.is_empty() || stream.last_id != StreamId::default() {
                    if id <= stream.last_id {
                        return Err(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                        );
                    }
                }
                id
            }
        };
        Ok(final_id)
    }

    fn apply_stream_trim(
        stream: &mut RudisStream,
        maxlen: Option<usize>,
        minid: Option<StreamId>,
    ) -> usize {
        let mut trimmed = 0;
        if let Some(max) = maxlen {
            while stream.entries.len() > max {
                if let Some(first_key) = stream.entries.keys().next().copied() {
                    stream.entries.remove(&first_key);
                    trimmed += 1;
                } else {
                    break;
                }
            }
        }
        if let Some(min_id) = minid {
            while let Some(first_key) = stream.entries.keys().next().copied() {
                if first_key < min_id {
                    stream.entries.remove(&first_key);
                    trimmed += 1;
                } else {
                    break;
                }
            }
        }
        trimmed
    }

    pub fn xadd(
        &mut self,
        key: Bytes,
        add_id: StreamAddId,
        fields: Vec<(Bytes, Bytes)>,
        nomkstream: bool,
        maxlen: Option<usize>,
        minid: Option<StreamId>,
    ) -> Result<Option<StreamId>, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                if nomkstream {
                    return Ok(None);
                }
                let mut stream = RudisStream::new();
                let final_id = Self::compute_stream_id(&mut stream, add_id)?;
                stream.last_id = final_id;
                stream.entries.insert(final_id, fields);
                Self::apply_stream_trim(&mut stream, maxlen, minid);

                let entry = RudisEntry {
                    key,
                    val: RudisValue::Stream(stream),
                    expire_at: None,
                };
                self.table.insert(entry);
                return Ok(Some(final_id));
            }

            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let final_id = Self::compute_stream_id(stream, add_id)?;
                        stream.last_id = final_id;
                        stream.entries.insert(final_id, fields);
                        Self::apply_stream_trim(stream, maxlen, minid);
                        return Ok(Some(final_id));
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        if nomkstream {
            return Ok(None);
        }

        let mut stream = RudisStream::new();
        let final_id = Self::compute_stream_id(&mut stream, add_id)?;
        stream.last_id = final_id;
        stream.entries.insert(final_id, fields);
        Self::apply_stream_trim(&mut stream, maxlen, minid);

        let entry = RudisEntry {
            key,
            val: RudisValue::Stream(stream),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(Some(final_id))
    }

    pub fn xlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Stream(stream) => Ok(stream.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn xrange(
        &mut self,
        key: &[u8],
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let start_bound = parse_range_bound(start, true)?;
        let end_bound = parse_range_bound(end, false)?;

        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Stream(stream) => {
                        let limit = count.unwrap_or(usize::MAX);
                        let mut items = Vec::new();
                        for (id, fields) in stream.entries.range((start_bound, end_bound)) {
                            items.push((*id, fields.clone()));
                            if items.len() >= limit {
                                break;
                            }
                        }
                        Ok(items)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn xrevrange(
        &mut self,
        key: &[u8],
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let start_bound = parse_range_bound(start, true)?;
        let end_bound = parse_range_bound(end, false)?;

        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Stream(stream) => {
                        let limit = count.unwrap_or(usize::MAX);
                        let mut items = Vec::new();
                        for (id, fields) in stream.entries.range((start_bound, end_bound)).rev() {
                            items.push((*id, fields.clone()));
                            if items.len() >= limit {
                                break;
                            }
                        }
                        Ok(items)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn xread(
        &mut self,
        keys: &[Bytes],
        ids: &[String],
        count: Option<usize>,
    ) -> Result<Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)>, &'static str> {
        if keys.len() != ids.len() {
            return Err("ERR Unbalanced XREAD list of streams and IDs");
        }
        let mut results = Vec::new();
        let limit = count.unwrap_or(usize::MAX);

        for (k, id_str) in keys.iter().zip(ids.iter()) {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Stream(stream) => {
                            let lower_bound = if id_str == "$" {
                                std::ops::Bound::Excluded(stream.last_id)
                            } else if id_str == "+" {
                                std::ops::Bound::Excluded(StreamId::new(u64::MAX, u64::MAX))
                            } else {
                                let bound_id = if let Some((ms_s, seq_s)) = id_str.split_once('-') {
                                    let ms: u64 = ms_s.parse().map_err(|_| {
                                        "Invalid stream ID specified as stream command argument"
                                    })?;
                                    let seq: u64 = seq_s.parse().map_err(|_| {
                                        "Invalid stream ID specified as stream command argument"
                                    })?;
                                    StreamId::new(ms, seq)
                                } else {
                                    let ms: u64 = id_str.parse().map_err(|_| {
                                        "Invalid stream ID specified as stream command argument"
                                    })?;
                                    StreamId::new(ms, 0)
                                };
                                std::ops::Bound::Excluded(bound_id)
                            };

                            let mut entries = Vec::new();
                            for (id, fields) in stream
                                .entries
                                .range((lower_bound, std::ops::Bound::Unbounded))
                            {
                                entries.push((*id, fields.clone()));
                                if entries.len() >= limit {
                                    break;
                                }
                            }
                            if !entries.is_empty() {
                                results.push((k.clone(), entries));
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn xdel(&mut self, key: &[u8], ids: &[StreamId]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let mut count = 0;
                        for id in ids {
                            if stream.entries.remove(id).is_some() {
                                count += 1;
                            }
                        }
                        Ok(count)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn xtrim(
        &mut self,
        key: &[u8],
        maxlen: Option<usize>,
        minid: Option<StreamId>,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let trimmed = Self::apply_stream_trim(stream, maxlen, minid);
                        Ok(trimmed)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    // RDB SERIALIZATION & DUMP / RESTORE
    pub fn serialize_val_payload(val: &RudisValue, payload: &mut Vec<u8>) {
        match val {
            RudisValue::String(b) => {
                payload.push(0u8);
                payload.extend_from_slice(&(b.len() as u32).to_le_bytes());
                payload.extend_from_slice(b);
            }
            RudisValue::Int(n) => {
                payload.push(0u8);
                let s = Self::format_i64(*n);
                payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
                payload.extend_from_slice(&s);
            }
            RudisValue::List(l) => {
                payload.push(1u8);
                payload.extend_from_slice(&(l.len() as u32).to_le_bytes());
                for item in l {
                    payload.extend_from_slice(&(item.len() as u32).to_le_bytes());
                    payload.extend_from_slice(item);
                }
            }
            RudisValue::Set(s) => {
                payload.push(2u8);
                payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
                for item in s {
                    payload.extend_from_slice(&(item.len() as u32).to_le_bytes());
                    payload.extend_from_slice(item);
                }
            }
            RudisValue::ZSet(z) => {
                payload.push(3u8);
                payload.extend_from_slice(&(z.len() as u32).to_le_bytes());
                z.for_each(|m, score| {
                    payload.extend_from_slice(&(m.len() as u32).to_le_bytes());
                    payload.extend_from_slice(m);
                    payload.extend_from_slice(&score.to_bits().to_le_bytes());
                });
            }
            RudisValue::SmallHash(pairs) => {
                payload.push(4u8);
                payload.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
                for (f, v) in pairs {
                    payload.extend_from_slice(&(f.len() as u32).to_le_bytes());
                    payload.extend_from_slice(f);
                    payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    payload.extend_from_slice(v);
                }
            }
            RudisValue::Hash(h) => {
                payload.push(4u8);
                payload.extend_from_slice(&(h.len() as u32).to_le_bytes());
                for (f, v) in h {
                    payload.extend_from_slice(&(f.len() as u32).to_le_bytes());
                    payload.extend_from_slice(f);
                    payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    payload.extend_from_slice(v);
                }
            }
            RudisValue::HyperLogLog(regs) => {
                payload.push(5u8);
                payload.extend_from_slice(regs.as_ref());
            }
            RudisValue::Stream(stream) => {
                payload.push(6u8);
                payload.extend_from_slice(&(stream.entries.len() as u32).to_le_bytes());
                payload.extend_from_slice(&stream.last_id.ms.to_le_bytes());
                payload.extend_from_slice(&stream.last_id.seq.to_le_bytes());
                for (id, fields) in &stream.entries {
                    payload.extend_from_slice(&id.ms.to_le_bytes());
                    payload.extend_from_slice(&id.seq.to_le_bytes());
                    payload.extend_from_slice(&(fields.len() as u32).to_le_bytes());
                    for (k, v) in fields {
                        payload.extend_from_slice(&(k.len() as u32).to_le_bytes());
                        payload.extend_from_slice(k);
                        payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
                        payload.extend_from_slice(v);
                    }
                }
            }
        }
    }

    pub fn deserialize_val_payload(data: &[u8]) -> Result<(RudisValue, usize), &'static str> {
        if data.is_empty() {
            return Err("DUMP payload version or checksum are wrong");
        }
        let type_byte = data[0];
        let mut cursor = 1;
        let val = match type_byte {
            0 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + len > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let val = Bytes::copy_from_slice(&data[cursor..cursor + len]);
                cursor += len;
                RudisValue::String(val)
            }
            1 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut list = std::collections::VecDeque::with_capacity(count);
                for _ in 0..count {
                    if cursor + 4 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    list.push_back(Bytes::copy_from_slice(&data[cursor..cursor + len]));
                    cursor += len;
                }
                RudisValue::List(list)
            }
            2 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut set = RudisSet::with_capacity(count);
                for _ in 0..count {
                    if cursor + 4 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    set.insert(Bytes::copy_from_slice(&data[cursor..cursor + len]));
                    cursor += len;
                }
                RudisValue::Set(set)
            }
            3 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut zset = RudisZSet::new();
                for _ in 0..count {
                    if cursor + 4 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let member = Bytes::copy_from_slice(&data[cursor..cursor + len]);
                    cursor += len;
                    if cursor + 8 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let score_bits = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    let score = f64::from_bits(score_bits);
                    zset.insert(score, member);
                }
                RudisValue::ZSet(zset)
            }
            4 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if count <= 64 {
                    let mut pairs = Vec::with_capacity(count);
                    for _ in 0..count {
                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let f_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                        cursor += 4;
                        if cursor + f_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let f = Bytes::copy_from_slice(&data[cursor..cursor + f_len]);
                        cursor += f_len;

                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                        cursor += 4;
                        if cursor + v_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v = Bytes::copy_from_slice(&data[cursor..cursor + v_len]);
                        cursor += v_len;

                        pairs.push((f, v));
                    }
                    RudisValue::SmallHash(pairs)
                } else {
                    let mut hash = hashbrown::HashMap::with_capacity(count);
                    for _ in 0..count {
                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let f_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                        cursor += 4;
                        if cursor + f_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let f = Bytes::copy_from_slice(&data[cursor..cursor + f_len]);
                        cursor += f_len;

                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                        cursor += 4;
                        if cursor + v_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v = Bytes::copy_from_slice(&data[cursor..cursor + v_len]);
                        cursor += v_len;

                        hash.insert(f, v);
                    }
                    RudisValue::Hash(hash)
                }
            }
            5 => {
                if cursor + 16384 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let mut regs = Box::new([0u8; 16384]);
                regs.copy_from_slice(&data[cursor..cursor + 16384]);
                cursor += 16384;
                RudisValue::HyperLogLog(regs)
            }
            6 => {
                if cursor + 4 + 8 + 8 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(
                    data[cursor..cursor + 4].try_into().unwrap(),
                ) as usize;
                cursor += 4;
                let last_ms = u64::from_le_bytes(
                    data[cursor..cursor + 8].try_into().unwrap(),
                );
                cursor += 8;
                let last_seq = u64::from_le_bytes(
                    data[cursor..cursor + 8].try_into().unwrap(),
                );
                cursor += 8;
                let mut entries = std::collections::BTreeMap::new();
                for _ in 0..count {
                    if cursor + 8 + 8 + 4 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let ms = u64::from_le_bytes(
                        data[cursor..cursor + 8].try_into().unwrap(),
                    );
                    cursor += 8;
                    let seq = u64::from_le_bytes(
                        data[cursor..cursor + 8].try_into().unwrap(),
                    );
                    cursor += 8;
                    let f_count = u32::from_le_bytes(
                        data[cursor..cursor + 4].try_into().unwrap(),
                    ) as usize;
                    cursor += 4;
                    let mut fields = Vec::with_capacity(f_count);
                    for _ in 0..f_count {
                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let k_len = u32::from_le_bytes(
                            data[cursor..cursor + 4].try_into().unwrap(),
                        ) as usize;
                        cursor += 4;
                        if cursor + k_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let k = Bytes::copy_from_slice(&data[cursor..cursor + k_len]);
                        cursor += k_len;
                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v_len = u32::from_le_bytes(
                            data[cursor..cursor + 4].try_into().unwrap(),
                        ) as usize;
                        cursor += 4;
                        if cursor + v_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v = Bytes::copy_from_slice(&data[cursor..cursor + v_len]);
                        cursor += v_len;
                        fields.push((k, v));
                    }
                    entries.insert(StreamId::new(ms, seq), fields);
                }
                RudisValue::Stream(RudisStream {
                    entries,
                    last_id: StreamId::new(last_ms, last_seq),
                    groups: HashMap::new(),
                })
            }
            _ => return Err("DUMP payload version or checksum are wrong"),
        };
        Ok((val, cursor))
    }

    pub fn dump(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        if self.check_expired_slot(idx) {
            return None;
        }
        let entry = self.table.get_slot(idx)?;
        let mut payload = Vec::new();
        Self::serialize_val_payload(&entry.val, &mut payload);
        // 2-byte RDB version: 10
        payload.extend_from_slice(&10u16.to_le_bytes());
        // 8-byte CRC64
        let crc = crc64(&payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        Some(payload)
    }

    pub fn restore(
        &mut self,
        key: Bytes,
        ttl_ms: u64,
        serialized: &[u8],
        replace: bool,
        absttl: bool,
    ) -> Result<(), &'static str> {
        if serialized.len() < 10 {
            return Err("DUMP payload version or checksum are wrong");
        }
        let data_len = serialized.len() - 8;
        let expected_crc = u64::from_le_bytes(
            serialized[data_len..]
                .try_into()
                .map_err(|_| "DUMP payload version or checksum are wrong")?,
        );
        let actual_crc = crc64(&serialized[..data_len]);
        if expected_crc != actual_crc {
            return Err("DUMP payload version or checksum are wrong");
        }

        let rdb_ver = u16::from_le_bytes(
            serialized[data_len - 2..data_len]
                .try_into()
                .map_err(|_| "DUMP payload version or checksum are wrong")?,
        );
        if rdb_ver > 15 {
            return Err("DUMP payload version or checksum are wrong");
        }

        if self.exists(&key) {
            if !replace {
                return Err("BUSYKEY Target key name already exists.");
            }
            self.del(&key);
        }

        let payload_len = data_len - 2;
        let (decoded_value, _) = Self::deserialize_val_payload(&serialized[..payload_len])?;

        let expire_at = if ttl_ms == 0 {
            None
        } else if absttl {
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if ttl_ms <= now_unix {
                return Ok(());
            } else {
                let remaining_ms = ttl_ms - now_unix;
                Some(Instant::now() + Duration::from_millis(remaining_ms))
            }
        } else {
            Some(Instant::now() + Duration::from_millis(ttl_ms))
        };

        let entry = RudisEntry {
            key,
            val: decoded_value,
            expire_at,
        };
        self.table.insert(entry);
        Ok(())
    }

    pub fn save_rdb_chunk(&mut self, buf: &mut Vec<u8>) {
        let now = Instant::now();
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for idx in 0..self.table.capacity() {
            if self.table.get_slot(idx).is_some() {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    if let Some(exp) = entry.expire_at {
                        if exp <= now {
                            continue;
                        }
                        let rem_ms = exp.duration_since(now).as_millis() as u64;
                        let expire_unix_ms = unix_now + rem_ms;
                        buf.push(0xFC); // EXPIRETIME_MS opcode
                        buf.extend_from_slice(&expire_unix_ms.to_le_bytes());
                    }
                    buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&entry.key);
                    Self::serialize_val_payload(&entry.val, buf);
                }
            }
        }
    }

    pub fn restore_rdb_chunk(&mut self, mut data: &[u8]) -> Result<(), &'static str> {
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        while !data.is_empty() {
            let mut expire_at = None;
            if data[0] == 0xFC {
                if data.len() < 9 {
                    return Err("Truncated RDB expire");
                }
                let exp_unix_ms = u64::from_le_bytes(data[1..9].try_into().unwrap());
                data = &data[9..];
                if exp_unix_ms <= unix_now {
                    // Already expired - skip key and value
                    if data.len() < 4 { return Err("Truncated RDB key"); }
                    let k_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                    data = &data[4..];
                    if data.len() < k_len { return Err("Truncated RDB key"); }
                    data = &data[k_len..];
                    let (_, consumed) = Self::deserialize_val_payload(data)?;
                    data = &data[consumed..];
                    continue;
                }
                let rem_ms = exp_unix_ms - unix_now;
                expire_at = Some(Instant::now() + Duration::from_millis(rem_ms));
            }
            if data.len() < 4 { return Err("Truncated RDB key"); }
            let k_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
            data = &data[4..];
            if data.len() < k_len { return Err("Truncated RDB key"); }
            let key = Bytes::copy_from_slice(&data[..k_len]);
            data = &data[k_len..];
            let (val, consumed) = Self::deserialize_val_payload(data)?;
            data = &data[consumed..];

            self.del(&key);
            self.table.insert(RudisEntry {
                key,
                val,
                expire_at,
            });
        }
        Ok(())
    }

    // REDIS STREAMS CONSUMER GROUPS
    pub fn xgroup_create(
        &mut self,
        key: Bytes,
        group: Bytes,
        id_str: &str,
        mkstream: bool,
    ) -> Result<(), &'static str> {
        let h = hash_key(&key);
        let idx_opt = self.table.find(&key, h);
        let stream_slot = if let Some(idx) = idx_opt {
            if self.check_expired_slot(idx) {
                None
            } else {
                Some(idx)
            }
        } else {
            None
        };

        if stream_slot.is_none() {
            if !mkstream {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            let stream = RudisStream::new();
            let entry = RudisEntry {
                key: key.clone(),
                val: RudisValue::Stream(stream),
                expire_at: None,
            };
            self.table.insert(entry);
        }

        let idx = self.table.find(&key, h).unwrap();
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(stream) => {
                if stream.groups.contains_key(&group) {
                    return Err("BUSYGROUP Consumer Group name already exists");
                }
                let last_delivered_id = if id_str == "$" {
                    stream.last_id
                } else {
                    StreamId::parse(id_str)?
                };
                let grp = StreamGroup {
                    name: group.clone(),
                    last_delivered_id,
                    consumers: HashMap::new(),
                    pel: std::collections::BTreeMap::new(),
                };
                stream.groups.insert(group, grp);
                Ok(())
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xgroup_destroy(&mut self, key: &[u8], group: &[u8]) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        Ok(stream.groups.remove(group).is_some())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Err("ERR The XGROUP subcommand requires the key to exist")
            }
        } else {
            Err("ERR The XGROUP subcommand requires the key to exist")
        }
    }

    pub fn xgroup_createconsumer(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
    ) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let grp = stream.groups.get_mut(group).ok_or("NOGROUP No such consumer group for key name")?;
                        if grp.consumers.contains_key(&consumer) {
                            Ok(false)
                        } else {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                            grp.consumers.insert(consumer.clone(), StreamConsumer {
                                name: consumer,
                                seen_time_ms: now,
                                pel: std::collections::BTreeMap::new(),
                            });
                            Ok(true)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Err("ERR The XGROUP subcommand requires the key to exist")
            }
        } else {
            Err("ERR The XGROUP subcommand requires the key to exist")
        }
    }

    pub fn xgroup_delconsumer(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: &[u8],
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let grp = stream.groups.get_mut(group).ok_or("NOGROUP No such consumer group for key name")?;
                        if let Some(cons) = grp.consumers.remove(consumer) {
                            let pending = cons.pel.len();
                            for id in cons.pel.keys() {
                                grp.pel.remove(id);
                            }
                            Ok(pending)
                        } else {
                            Ok(0)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Err("ERR The XGROUP subcommand requires the key to exist")
            }
        } else {
            Err("ERR The XGROUP subcommand requires the key to exist")
        }
    }

    pub fn xreadgroup(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
        id_str: &str,
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Ok(Vec::new());
                }
                i
            }
            None => return Ok(Vec::new()),
        };

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let limit = count.unwrap_or(usize::MAX);

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(stream) => {
                let grp = stream.groups.get_mut(group).ok_or("NOGROUP No such key or consumer group")?;
                let cons = grp.consumers.entry(consumer.clone()).or_insert_with(|| StreamConsumer {
                    name: consumer.clone(),
                    seen_time_ms: now,
                    pel: std::collections::BTreeMap::new(),
                });
                cons.seen_time_ms = now;

                if id_str == ">" {
                    let mut results = Vec::new();
                    let range = stream.entries.range((std::ops::Bound::Excluded(grp.last_delivered_id), std::ops::Bound::Unbounded));
                    for (&id, fields) in range {
                        if results.len() >= limit {
                            break;
                        }
                        results.push((id, fields.clone()));
                    }

                    for (id, _) in &results {
                        grp.last_delivered_id = std::cmp::max(grp.last_delivered_id, *id);
                        if !noack {
                            grp.pel.insert(*id, StreamPelEntry {
                                consumer: consumer.clone(),
                                delivery_time_ms: now,
                                delivery_count: 1,
                            });
                            let cons = grp.consumers.get_mut(&consumer).unwrap();
                            cons.pel.insert(*id, now);
                        }
                    }
                    Ok(results)
                } else {
                    let start_id = StreamId::parse(id_str)?;
                    let mut results = Vec::new();
                    for (&id, _) in cons.pel.range((std::ops::Bound::Excluded(start_id), std::ops::Bound::Unbounded)) {
                        if results.len() >= limit {
                            break;
                        }
                        if let Some(fields) = stream.entries.get(&id) {
                            results.push((id, fields.clone()));
                        }
                    }
                    for (id, _) in &results {
                        if let Some(pel_entry) = grp.pel.get_mut(id) {
                            pel_entry.delivery_time_ms = now;
                            pel_entry.delivery_count += 1;
                        }
                        if let Some(cons) = grp.consumers.get_mut(&consumer) {
                            cons.pel.insert(*id, now);
                        }
                    }
                    Ok(results)
                }
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xack(
        &mut self,
        key: &[u8],
        group: &[u8],
        ids: &[StreamId],
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Ok(0);
                }
                i
            }
            None => return Ok(0),
        };

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(stream) => {
                let grp = match stream.groups.get_mut(group) {
                    Some(g) => g,
                    None => return Ok(0),
                };
                let mut acked = 0;
                for id in ids {
                    if let Some(pel_entry) = grp.pel.remove(id) {
                        if let Some(cons) = grp.consumers.get_mut(&pel_entry.consumer) {
                            cons.pel.remove(id);
                        }
                        acked += 1;
                    }
                }
                Ok(acked)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xpending_summary(
        &mut self,
        key: &[u8],
        group: &[u8],
    ) -> Result<(usize, Option<StreamId>, Option<StreamId>, Vec<(Bytes, usize)>), &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("NOGROUP No such key or consumer group");
                }
                i
            }
            None => return Err("NOGROUP No such key or consumer group"),
        };

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &entry.val {
            RudisValue::Stream(stream) => {
                let grp = stream.groups.get(group).ok_or("NOGROUP No such key or consumer group")?;
                let count = grp.pel.len();
                let min_id = grp.pel.keys().next().copied();
                let max_id = grp.pel.keys().next_back().copied();
                let mut consumer_counts = Vec::new();
                for (name, cons) in &grp.consumers {
                    if !cons.pel.is_empty() {
                        consumer_counts.push((name.clone(), cons.pel.len()));
                    }
                }
                Ok((count, min_id, max_id, consumer_counts))
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xpending_range(
        &mut self,
        key: &[u8],
        group: &[u8],
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<&[u8]>,
    ) -> Result<Vec<(StreamId, Bytes, u64, usize)>, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("NOGROUP No such key or consumer group");
                }
                i
            }
            None => return Err("NOGROUP No such key or consumer group"),
        };

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &entry.val {
            RudisValue::Stream(stream) => {
                let grp = stream.groups.get(group).ok_or("NOGROUP No such key or consumer group")?;
                let mut results = Vec::new();
                for (&id, pel_entry) in grp.pel.range(start..=end) {
                    if let Some(c) = consumer {
                        if pel_entry.consumer.as_ref() != c {
                            continue;
                        }
                    }
                    let idle = now.saturating_sub(pel_entry.delivery_time_ms);
                    results.push((id, pel_entry.consumer.clone(), idle, pel_entry.delivery_count));
                    if results.len() >= count {
                        break;
                    }
                }
                Ok(results)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }
}

pub fn crc64(data: &[u8]) -> u64 {
    let mut crc: u64 = 0;
    for &b in data {
        crc ^= (b as u64) << 56;
        for _ in 0..8 {
            if (crc & 0x8000_0000_0000_0000) != 0 {
                crc = (crc << 1) ^ 0x42F0_E1EB_A9EA_3693;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

pub fn load_rdb(
    path: &std::path::Path,
    db: &mut crate::shard::ShardDb,
    shard_id: usize,
    num_shards: usize,
) -> std::io::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let data = std::fs::read(path)?;
    if data.len() < 18 {
        return Ok(0);
    }
    if !data.starts_with(b"REDIS") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid RDB magic header",
        ));
    }
    let content_len = data.len() - 8;
    let expected_crc = u64::from_le_bytes(data[content_len..].try_into().unwrap());
    let actual_crc = crc64(&data[..content_len]);
    if expected_crc != actual_crc {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "CRC64 checksum mismatch in RDB file",
        ));
    }

    let unix_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut cursor = 9; // Skip REDIS0011
    let mut count = 0;

    while cursor < content_len {
        let op = data[cursor];
        if op == 0xFF {
            // EOF
            break;
        }
        if op == 0xFE {
            // SELECTDB
            cursor += 1;
            if cursor < content_len {
                cursor += 1; // DB number
            }
            continue;
        }

        let mut expire_at = None;
        if op == 0xFC {
            // EXPIRETIME_MS
            cursor += 1;
            if cursor + 8 > content_len {
                break;
            }
            let exp_unix_ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            if exp_unix_ms <= unix_now {
                // Expired - skip key and value
                if cursor + 4 > content_len { break; }
                let k_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + k_len > content_len { break; }
                cursor += k_len;
                if let Ok((_, consumed)) = RudisTable::deserialize_val_payload(&data[cursor..content_len]) {
                    cursor += consumed;
                } else {
                    break;
                }
                continue;
            }
            expire_at = Some(Instant::now() + Duration::from_millis(exp_unix_ms - unix_now));
        } else if op == 0xFD {
            // EXPIRETIME_SEC
            cursor += 1;
            if cursor + 4 > content_len {
                break;
            }
            let exp_unix_sec = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as u64;
            cursor += 4;
            let exp_unix_ms = exp_unix_sec * 1000;
            if exp_unix_ms <= unix_now {
                if cursor + 4 > content_len { break; }
                let k_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + k_len > content_len { break; }
                cursor += k_len;
                if let Ok((_, consumed)) = RudisTable::deserialize_val_payload(&data[cursor..content_len]) {
                    cursor += consumed;
                } else {
                    break;
                }
                continue;
            }
            expire_at = Some(Instant::now() + Duration::from_millis(exp_unix_ms - unix_now));
        }

        if cursor + 4 > content_len {
            break;
        }
        let k_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + k_len > content_len {
            break;
        }
        let key = Bytes::copy_from_slice(&data[cursor..cursor + k_len]);
        cursor += k_len;

        let (val, consumed) = match RudisTable::deserialize_val_payload(&data[cursor..content_len]) {
            Ok(res) => res,
            Err(_) => break,
        };
        cursor += consumed;

        if crate::router::target_shard(&key, num_shards) == shard_id {
            db.table.del(&key);
            db.table.table.insert(RudisEntry {
                key,
                val,
                expire_at,
            });
            count += 1;
        }
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_flat_table_crud_and_growth() {
        let mut table = RudisFlatTable::new(16);
        assert_eq!(table.capacity(), 16);

        // Insert 100 items to force multiple resizes
        for i in 0..100 {
            let key = Bytes::from(format!("key_{}", i));
            let val = Bytes::from(format!("val_{}", i));
            let entry = RudisEntry {
                key,
                val: RudisValue::String(val),
                expire_at: None,
            };
            table.insert(entry);
        }
        assert_eq!(table.len(), 100);
        assert!(table.capacity() >= 128);

        // Verify all items are present
        for i in 0..100 {
            let key = format!("key_{}", i);
            let h = hash_key(key.as_bytes());
            let idx = table.find(key.as_bytes(), h).expect("key must be found");
            let entry = table.get_slot(idx).unwrap();
            assert_eq!(entry.key, key.as_bytes());
            if let RudisValue::String(ref b) = entry.val {
                assert_eq!(b, format!("val_{}", i).as_bytes());
            } else {
                panic!("expected String value");
            }
        }

        // Test removal
        let h = hash_key(b"key_42");
        let idx = table.find(b"key_42", h).unwrap();
        let removed = table.remove(idx).unwrap();
        assert_eq!(removed.key, b"key_42".as_slice());
        assert_eq!(table.len(), 99);
        assert!(table.find(b"key_42", h).is_none());
    }

    #[test]
    fn test_rudis_table_redis_operations() {
        let mut table = RudisTable::new();

        // 1. SET & GET
        table.set(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"), None);
        assert_eq!(table.get(b"k1").unwrap(), Some(Bytes::from_static(b"v1")));
        assert_eq!(table.get(b"nonexistent").unwrap(), None);

        // 2. INCRBY
        assert_eq!(
            table.incr_by(Bytes::from_static(b"counter"), 10).unwrap(),
            10
        );
        assert_eq!(
            table.incr_by(Bytes::from_static(b"counter"), -3).unwrap(),
            7
        );

        // 3. TTL & Expiration
        table.set(
            Bytes::from_static(b"exp_key"),
            Bytes::from_static(b"temp"),
            Some(Duration::from_millis(50)),
        );
        assert_eq!(
            table.get(b"exp_key").unwrap(),
            Some(Bytes::from_static(b"temp"))
        );
        assert!(table.ttl(b"exp_key", false) >= 0);

        thread::sleep(Duration::from_millis(60));
        assert_eq!(table.get(b"exp_key").unwrap(), None, "Key must be expired");
        assert_eq!(table.ttl(b"exp_key", false), -2);

        // 4. Hashes
        let added = table
            .hset(
                Bytes::from_static(b"user:1"),
                vec![
                    (Bytes::from_static(b"name"), Bytes::from_static(b"alice")),
                    (Bytes::from_static(b"age"), Bytes::from_static(b"30")),
                ],
            )
            .unwrap();
        assert_eq!(added, 2);

        assert_eq!(
            table.hget(b"user:1", b"name").unwrap(),
            Some(Bytes::from_static(b"alice"))
        );
        assert_eq!(table.hlen(b"user:1").unwrap(), 2);
        assert!(table.hexists(b"user:1", b"name").unwrap());
        assert!(!table.hexists(b"user:1", b"missing").unwrap());

        // WRONGTYPE test
        assert!(table.get(b"user:1").is_err());
        assert!(table.hget(b"k1", b"field").is_err());
    }

    #[test]
    fn test_rudis_table_zset_operations() {
        let mut table = RudisTable::new();

        // 1. ZADD
        let (added, _) = table
            .zadd(
                Bytes::from_static(b"myzset"),
                vec![
                    (10.0, Bytes::from_static(b"m1")),
                    (20.5, Bytes::from_static(b"m2")),
                    (5.0, Bytes::from_static(b"m3")),
                ],
                ZAddFlags::default(),
            )
            .unwrap();
        assert_eq!(added, 3);
        assert_eq!(table.zcard(b"myzset").unwrap(), 3);

        // 2. ZSCORE & ZRANK
        assert_eq!(table.zscore(b"myzset", b"m2").unwrap(), Some(20.5));
        assert_eq!(table.zscore(b"myzset", b"nonexistent").unwrap(), None);
        assert_eq!(table.zrank(b"myzset", b"m3", false).unwrap(), Some(0)); // 5.0
        assert_eq!(table.zrank(b"myzset", b"m1", false).unwrap(), Some(1)); // 10.0
        assert_eq!(table.zrank(b"myzset", b"m2", false).unwrap(), Some(2)); // 20.5
        assert_eq!(table.zrank(b"myzset", b"m3", true).unwrap(), Some(2)); // rev rank

        // 3. ZRANGE by index
        let opts = ZRangeOpts {
            start: 0,
            stop: -1,
            min_score: 0.0,
            min_inc: true,
            max_score: 0.0,
            max_inc: true,
            by_score: false,
            rev: false,
            with_scores: true,
            offset: 0,
            count: None,
        };
        let res = table.zrange(b"myzset", &opts).unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].0, b"m3".as_slice());
        assert_eq!(res[1].0, b"m1".as_slice());
        assert_eq!(res[2].0, b"m2".as_slice());

        // 4. ZCOUNT
        assert_eq!(table.zcount(b"myzset", 5.0, true, 15.0, true).unwrap(), 2);
        assert_eq!(table.zcount(b"myzset", 5.0, false, 15.0, true).unwrap(), 1);

        // 5. ZINCRBY
        let new_score = table
            .zincrby(
                Bytes::from_static(b"myzset"),
                15.0,
                Bytes::from_static(b"m3"),
            )
            .unwrap();
        assert_eq!(new_score, 20.0);
        assert_eq!(table.zscore(b"myzset", b"m3").unwrap(), Some(20.0));

        // 6. ZPOPMIN & ZPOPMAX
        let popped_min = table.zpopmin(b"myzset", 1).unwrap();
        assert_eq!(popped_min.len(), 1);
        assert_eq!(popped_min[0].0, b"m1".as_slice()); // 10.0

        let popped_max = table.zpopmax(b"myzset", 1).unwrap();
        assert_eq!(popped_max.len(), 1);
        assert_eq!(popped_max[0].0, b"m2".as_slice()); // 20.5

        // Remaining should be m3 (20.0)
        assert_eq!(table.zcard(b"myzset").unwrap(), 1);

        // 7. ZREM
        let removed = table.zrem(b"myzset", &[Bytes::from_static(b"m3")]).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(table.zcard(b"myzset").unwrap(), 0);

        // WRONGTYPE test
        table.set(
            Bytes::from_static(b"str_key"),
            Bytes::from_static(b"val"),
            None,
        );
        assert!(table.zcard(b"str_key").is_err());
    }

    #[test]
    fn test_rudis_table_keyspace_inspection() {
        let mut table = RudisTable::new();
        assert_eq!(table.random_key(), None);
        assert_eq!(table.keys(b"*").len(), 0);

        table.set(Bytes::from_static(b"alpha:1"), Bytes::from_static(b"a"), None);
        table.set(Bytes::from_static(b"alpha:2"), Bytes::from_static(b"b"), None);
        table.set(Bytes::from_static(b"beta:1"), Bytes::from_static(b"c"), None);

        // KEYS
        let matched = table.keys(b"alpha:*");
        assert_eq!(matched.len(), 2);
        assert!(matched.contains(&Bytes::from_static(b"alpha:1")));
        assert!(matched.contains(&Bytes::from_static(b"alpha:2")));

        // RANDOMKEY
        let rk = table.random_key();
        assert!(rk.is_some());

        // SCAN
        let (next_cursor, scanned) = table.scan(0, Some(b"alpha:*"), 10);
        assert_eq!(next_cursor, 0); // Scanned whole small table
        assert_eq!(scanned.len(), 2);

        // EXPIRETIME
        assert_eq!(table.expiretime(b"non_exist", false), -2);
        assert_eq!(table.expiretime(b"alpha:1", false), -1);
        table.expire(b"alpha:1", Duration::from_secs(50));
        let exp = table.expiretime(b"alpha:1", false);
        assert!(exp > 0);
    }

    #[test]
    fn test_rudis_table_bitmaps_and_hll() {
        let mut table = RudisTable::new();

        // 1. SETBIT & GETBIT
        // 'a' in ASCII is 0b01100001 (byte 0: bit 1, 2, 7 are 1)
        assert_eq!(table.setbit(Bytes::from_static(b"bm"), 1, 1).unwrap(), 0);
        assert_eq!(table.setbit(Bytes::from_static(b"bm"), 2, 1).unwrap(), 0);
        assert_eq!(table.setbit(Bytes::from_static(b"bm"), 7, 1).unwrap(), 0);
        assert_eq!(table.getbit(b"bm", 1).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 2).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 3).unwrap(), 0);
        assert_eq!(table.getbit(b"bm", 7).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 100).unwrap(), 0);
        assert_eq!(table.get(b"bm").unwrap(), Some(Bytes::from_static(b"a")));

        // 2. BITCOUNT
        assert_eq!(table.bitcount(b"bm", None, None).unwrap(), 3);
        // Set bit in byte 1 (offset 15 = bit 7 of byte 1)
        assert_eq!(table.setbit(Bytes::from_static(b"bm"), 15, 1).unwrap(), 0);
        assert_eq!(table.bitcount(b"bm", None, None).unwrap(), 4);
        assert_eq!(table.bitcount(b"bm", Some(0), Some(0)).unwrap(), 3);
        assert_eq!(table.bitcount(b"bm", Some(1), Some(1)).unwrap(), 1);

        // 3. BITPOS
        assert_eq!(table.bitpos(b"bm", 1, None, None).unwrap(), 1);
        assert_eq!(table.bitpos(b"bm", 0, None, None).unwrap(), 0);
        assert_eq!(table.bitpos(b"bm", 1, Some(1), None).unwrap(), 15);

        // 4. BITOP
        table.set(Bytes::from_static(b"k1"), Bytes::from_static(b"\x0f"), None); // 00001111
        table.set(Bytes::from_static(b"k2"), Bytes::from_static(b"\x33"), None); // 00110011
        assert_eq!(
            table
                .bitop("AND", Bytes::from_static(b"kand"), &[Bytes::from_static(b"k1"), Bytes::from_static(b"k2")])
                .unwrap(),
            1
        );
        assert_eq!(table.get(b"kand").unwrap(), Some(Bytes::from_static(b"\x03")));

        assert_eq!(
            table
                .bitop("OR", Bytes::from_static(b"kor"), &[Bytes::from_static(b"k1"), Bytes::from_static(b"k2")])
                .unwrap(),
            1
        );
        assert_eq!(table.get(b"kor").unwrap(), Some(Bytes::from_static(b"\x3f")));

        assert_eq!(
            table
                .bitop("XOR", Bytes::from_static(b"kxor"), &[Bytes::from_static(b"k1"), Bytes::from_static(b"k2")])
                .unwrap(),
            1
        );
        assert_eq!(table.get(b"kxor").unwrap(), Some(Bytes::from_static(b"\x3c")));

        assert_eq!(
            table
                .bitop("NOT", Bytes::from_static(b"knot"), &[Bytes::from_static(b"k1")])
                .unwrap(),
            1
        );
        assert_eq!(table.get(b"knot").unwrap(), Some(Bytes::from_static(b"\xf0")));

        // 5. HYPERLOGLOG: PFADD & PFCOUNT
        let elements_a = vec![
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"zap"),
            Bytes::from_static(b"a"),
        ];
        assert_eq!(table.pfadd(Bytes::from_static(b"hll1"), &elements_a).unwrap(), true);
        assert_eq!(table.pfadd(Bytes::from_static(b"hll1"), &elements_a).unwrap(), false); // No updates
        let c1 = table.pfcount(&[Bytes::from_static(b"hll1")]).unwrap();
        assert_eq!(c1, 4);

        let elements_b = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
            Bytes::from_static(b"foo"),
        ];
        assert_eq!(table.pfadd(Bytes::from_static(b"hll2"), &elements_b).unwrap(), true);
        let c2 = table.pfcount(&[Bytes::from_static(b"hll2")]).unwrap();
        assert_eq!(c2, 4);

        // Multiple keys pfcount (union)
        let c_union = table
            .pfcount(&[Bytes::from_static(b"hll1"), Bytes::from_static(b"hll2")])
            .unwrap();
        assert_eq!(c_union, 6); // foo, bar, zap, a, b, c = 6 distinct elements

        // 6. PFMERGE
        table
            .pfmerge(
                Bytes::from_static(b"hll_merged"),
                &[Bytes::from_static(b"hll1"), Bytes::from_static(b"hll2")],
            )
            .unwrap();
        let c_merged = table.pfcount(&[Bytes::from_static(b"hll_merged")]).unwrap();
        assert_eq!(c_merged, 6);
    }

    #[test]
    fn test_rudis_table_dump_and_restore() {
        let mut table = RudisTable::new();

        // 1. Non-existent key
        assert_eq!(table.dump(b"non_exist"), None);

        // 2. String dump and restore
        table.set(
            Bytes::from_static(b"str_key"),
            Bytes::from_static(b"hello world"),
            None,
        );
        let dumped = table.dump(b"str_key").expect("dump must succeed");
        assert!(dumped.len() >= 10);

        // Restore to target key
        table
            .restore(
                Bytes::from_static(b"restored_str"),
                0,
                &dumped,
                false,
                false,
            )
            .expect("restore must succeed");
        assert_eq!(
            table.get(b"restored_str").unwrap(),
            Some(Bytes::from_static(b"hello world"))
        );

        // BUSYKEY error
        let err = table.restore(
            Bytes::from_static(b"restored_str"),
            0,
            &dumped,
            false,
            false,
        );
        assert_eq!(err, Err("BUSYKEY Target key name already exists."));

        // Replace success
        table
            .restore(
                Bytes::from_static(b"restored_str"),
                0,
                &dumped,
                true,
                false,
            )
            .expect("replace must succeed");

        // Checksum verification failure
        let mut corrupt = dumped.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xFF;
        assert_eq!(
            table.restore(
                Bytes::from_static(b"corrupt"),
                0,
                &corrupt,
                false,
                false,
            ),
            Err("DUMP payload version or checksum are wrong")
        );

        // 3. Hash dump and restore
        table
            .hset(
                Bytes::from_static(b"myhash"),
                vec![
                    (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                ],
            )
            .unwrap();
        let hash_dump = table.dump(b"myhash").unwrap();
        table
            .restore(
                Bytes::from_static(b"restored_hash"),
                0,
                &hash_dump,
                false,
                false,
            )
            .unwrap();
        assert_eq!(
            table.hget(b"restored_hash", b"f1").unwrap(),
            Some(Bytes::from_static(b"v1"))
        );
        assert_eq!(
            table.hget(b"restored_hash", b"f2").unwrap(),
            Some(Bytes::from_static(b"v2"))
        );

        // 4. ZSet dump and restore
        table
            .zadd(
                Bytes::from_static(b"myz"),
                vec![
                    (10.5, Bytes::from_static(b"m1")),
                    (20.0, Bytes::from_static(b"m2")),
                ],
                ZAddFlags::default(),
            )
            .unwrap();
        let zset_dump = table.dump(b"myz").unwrap();
        table
            .restore(
                Bytes::from_static(b"restored_z"),
                0,
                &zset_dump,
                false,
                false,
            )
            .unwrap();
        assert_eq!(table.zscore(b"restored_z", b"m1").unwrap(), Some(10.5));
        assert_eq!(table.zscore(b"restored_z", b"m2").unwrap(), Some(20.0));
    }

    #[test]
    fn test_streams_table() {
        let mut table = RudisTable::new();

        // 1. Invalid ID checks
        let err0 = table.xadd(
            Bytes::from_static(b"mystream"),
            StreamAddId::Explicit(StreamId::new(0, 0)),
            vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            false,
            None,
            None,
        );
        assert_eq!(
            err0,
            Err("ERR The ID specified in XADD must be greater than 0-0")
        );

        // 2. NOMKSTREAM when stream doesn't exist
        let res_nomk = table
            .xadd(
                Bytes::from_static(b"nonexistent"),
                StreamAddId::Auto,
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                true,
                None,
                None,
            )
            .unwrap();
        assert_eq!(res_nomk, None);

        // 3. XADD with explicit ID
        let id1 = table
            .xadd(
                Bytes::from_static(b"s1"),
                StreamAddId::Explicit(StreamId::new(1000, 1)),
                vec![
                    (Bytes::from_static(b"sensor"), Bytes::from_static(b"temp")),
                    (Bytes::from_static(b"val"), Bytes::from_static(b"25")),
                ],
                false,
                None,
                None,
            )
            .unwrap()
            .unwrap();
        assert_eq!(id1, StreamId::new(1000, 1));
        assert_eq!(table.xlen(b"s1").unwrap(), 1);

        // Monotonicity error
        let err_mono = table.xadd(
            Bytes::from_static(b"s1"),
            StreamAddId::Explicit(StreamId::new(1000, 1)),
            vec![(Bytes::from_static(b"a"), Bytes::from_static(b"b"))],
            false,
            None,
            None,
        );
        assert_eq!(
            err_mono,
            Err("ERR The ID specified in XADD is equal or smaller than the target stream top item")
        );

        // 4. AutoSeq
        let id2 = table
            .xadd(
                Bytes::from_static(b"s1"),
                StreamAddId::AutoSeq(1000),
                vec![(Bytes::from_static(b"val"), Bytes::from_static(b"26"))],
                false,
                None,
                None,
            )
            .unwrap()
            .unwrap();
        assert_eq!(id2, StreamId::new(1000, 2));

        let id3 = table
            .xadd(
                Bytes::from_static(b"s1"),
                StreamAddId::AutoSeq(1001),
                vec![(Bytes::from_static(b"val"), Bytes::from_static(b"27"))],
                false,
                None,
                None,
            )
            .unwrap()
            .unwrap();
        assert_eq!(id3, StreamId::new(1001, 0));
        assert_eq!(table.xlen(b"s1").unwrap(), 3);

        // 5. XRANGE
        let range = table.xrange(b"s1", "-", "+", None).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, id1);
        assert_eq!(range[1].0, id2);
        assert_eq!(range[2].0, id3);

        // XRANGE with count
        let range_limited = table.xrange(b"s1", "-", "+", Some(2)).unwrap();
        assert_eq!(range_limited.len(), 2);
        assert_eq!(range_limited[0].0, id1);
        assert_eq!(range_limited[1].0, id2);

        // XRANGE exclusive prefix
        let range_ex = table.xrange(b"s1", "(1000-1", "+", None).unwrap();
        assert_eq!(range_ex.len(), 2);
        assert_eq!(range_ex[0].0, id2);

        // 6. XREVRANGE
        let rev = table.xrevrange(b"s1", "+", "-", None).unwrap();
        assert_eq!(rev.len(), 3);
        assert_eq!(rev[0].0, id3);
        assert_eq!(rev[1].0, id2);
        assert_eq!(rev[2].0, id1);

        // 7. XREAD
        let read_res = table
            .xread(&[Bytes::from_static(b"s1")], &[id1.to_string()], None)
            .unwrap();
        assert_eq!(read_res.len(), 1);
        assert_eq!(read_res[0].1.len(), 2);
        assert_eq!(read_res[0].1[0].0, id2);
        assert_eq!(read_res[0].1[1].0, id3);

        // XREAD with $
        let read_dollar = table
            .xread(&[Bytes::from_static(b"s1")], &[String::from("$")], None)
            .unwrap();
        assert_eq!(read_dollar.len(), 0);

        // 8. XDEL
        let del_cnt = table.xdel(b"s1", &[id2]).unwrap();
        assert_eq!(del_cnt, 1);
        assert_eq!(table.xlen(b"s1").unwrap(), 2);

        // 9. XTRIM
        // Add more entries
        for i in 10..20 {
            table
                .xadd(
                    Bytes::from_static(b"s1"),
                    StreamAddId::Explicit(StreamId::new(2000 + i, 0)),
                    vec![(Bytes::from_static(b"i"), Bytes::from(i.to_string()))],
                    false,
                    None,
                    None,
                )
                .unwrap();
        }
        assert_eq!(table.xlen(b"s1").unwrap(), 12);
        let trimmed = table.xtrim(b"s1", Some(5), None).unwrap();
        assert_eq!(trimmed, 7);
        assert_eq!(table.xlen(b"s1").unwrap(), 5);

        // 10. DUMP and RESTORE stream
        let stream_dump = table.dump(b"s1").unwrap();
        table
            .restore(
                Bytes::from_static(b"restored_stream"),
                0,
                &stream_dump,
                false,
                false,
            )
            .unwrap();
        assert_eq!(table.xlen(b"restored_stream").unwrap(), 5);
        let restored_range = table
            .xrange(b"restored_stream", "-", "+", None)
            .unwrap();
        assert_eq!(restored_range.len(), 5);
    }
}
