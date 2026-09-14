use bytes::Bytes;
use fxhash::hash64;
use hashbrown::HashMap;
use std::time::{Duration, Instant};

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

#[derive(Clone, Debug, PartialEq)]
pub struct RudisZSet {
    pub dict: hashbrown::HashMap<Bytes, f64>,
    pub tree: std::collections::BTreeSet<(OrderedScore, Bytes)>,
}

impl Eq for RudisZSet {}

impl Default for RudisZSet {
    fn default() -> Self {
        Self::new()
    }
}

impl RudisZSet {
    pub fn new() -> Self {
        Self {
            dict: hashbrown::HashMap::new(),
            tree: std::collections::BTreeSet::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.dict.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RudisValue {
    String(Bytes),
    Hash(HashMap<Bytes, Bytes>),
    List(std::collections::VecDeque<Bytes>),
    Set(hashbrown::HashSet<Bytes>),
    ZSet(RudisZSet),
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
    slots: Vec<Option<RudisEntry>>,
    capacity: usize,
    mask: usize,
    items: usize,
    growth_left: usize,
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
            self.slots[insert_idx] = Some(entry);
            self.items += 1;
            self.growth_left = self.growth_left.saturating_sub(1);
            None
        }
    }

    pub fn remove(&mut self, slot_idx: usize) -> Option<RudisEntry> {
        self.set_ctrl(slot_idx, DELETED);
        self.items -= 1;
        self.slots[slot_idx].take()
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

    pub fn clear(&mut self) {
        let cap = self.capacity;
        self.ctrl.fill(EMPTY);
        self.ctrl.copy_within(0..GROUP_SIZE, cap);
        self.slots.fill(None);
        self.items = 0;
        self.growth_left = (cap * 7) / 8;
    }
}

/// The complete thread-local Rudis storage engine unifying:
/// 1. Flat SIMD-accelerated hash table (`RudisFlatTable`)
/// 2. Inlined TTL expiration
/// 3. Secondary cluster slot index
pub struct RudisTable {
    table: RudisFlatTable,
    slot_to_keys: HashMap<u16, hashbrown::HashSet<Bytes>>,
    sample_cursor: usize,
}

impl RudisTable {
    pub fn new() -> Self {
        Self {
            table: RudisFlatTable::new(64),
            slot_to_keys: HashMap::new(),
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
            if let Some(removed) = self.table.remove(slot_idx) {
                let slot = crate::router::key_slot(&removed.key);
                if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                    set.remove(&removed.key);
                }
            }
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

    pub fn set(&mut self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(key.clone());

        let expire_at = expire_in.map(|d| Instant::now() + d);
        let entry = RudisEntry {
            key,
            val: RudisValue::String(value),
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
            if let Some(removed) = self.table.remove(idx) {
                let slot = crate::router::key_slot(&removed.key);
                if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                    set.remove(&removed.key);
                }
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
        if let Some(idx) = self.table.find(&key, h) {
            let was_exp = self.check_expired_slot(idx);
            if !was_exp {
                let (current, old_expire) = match self.table.get_slot(idx) {
                    Some(entry) => {
                        match &entry.val {
                            RudisValue::String(b) => {
                                let s = std::str::from_utf8(b).map_err(|_| {
                                    "value is not an integer or out of range".to_string()
                                })?;
                                let val = s.parse::<i64>().map_err(|_| {
                                    "value is not an integer or out of range".to_string()
                                })?;
                                (val, entry.expire_at)
                            }
                            _ => {
                                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
                            }
                        }
                    }
                    None => (0, None),
                };

                let new_val = current
                    .checked_add(delta)
                    .ok_or_else(|| "increment or decrement would overflow".to_string())?;

                let entry = RudisEntry {
                    key: key.clone(),
                    val: RudisValue::String(Bytes::from(new_val.to_string())),
                    expire_at: old_expire,
                };
                self.table.insert(entry);
                return Ok(new_val);
            }
        }

        let new_val = delta;
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(key.clone());
        let entry = RudisEntry {
            key,
            val: RudisValue::String(Bytes::from(new_val.to_string())),
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

    pub fn flushdb(&mut self) {
        self.table.clear();
        self.slot_to_keys.clear();
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
                    RudisValue::String(_) => "string",
                    RudisValue::Hash(_) => "hash",
                    RudisValue::List(_) => "list",
                    RudisValue::Set(_) => "set",
                    RudisValue::ZSet(_) => "zset",
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
        let src_slot = crate::router::key_slot(&entry.key);
        if let Some(set) = self.slot_to_keys.get_mut(&src_slot) {
            set.remove(&entry.key);
        }

        // If dst exists, remove it from slot_to_keys first
        let h_dst = hash_key(&dst);
        if let Some(dst_idx) = self.table.find(&dst, h_dst) {
            if let Some(old_dst) = self.table.remove(dst_idx) {
                let dst_slot = crate::router::key_slot(&old_dst.key);
                if let Some(set) = self.slot_to_keys.get_mut(&dst_slot) {
                    set.remove(&old_dst.key);
                }
            }
        }

        // Update entry key to dst and insert
        entry.key = dst.clone();
        let dst_slot = crate::router::key_slot(&dst);
        self.slot_to_keys.entry(dst_slot).or_default().insert(dst);
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
                        let slot = crate::router::key_slot(key);
                        if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                            set.remove(key);
                        }
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
        if let Some(idx) = self.table.find(&key, h) {
            let _ = self.check_expired_slot(idx);
        }

        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing {
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
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

        let slot = crate::router::key_slot(&key);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(key.clone());

        let mut map = HashMap::new();
        let mut added = 0;
        for (f, v) in fields {
            if map.insert(f, v).is_none() {
                added += 1;
            }
        }
        let entry = RudisEntry {
            key,
            val: RudisValue::Hash(map),
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
                if let Some(removed) = self.table.remove(idx) {
                    let slot = crate::router::key_slot(&removed.key);
                    if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                        set.remove(&removed.key);
                    }
                }
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
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(key.clone());
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
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(key.clone());
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
                if let Some(removed) = self.table.remove(idx) {
                    let slot = crate::router::key_slot(&removed.key);
                    if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                        set.remove(&removed.key);
                    }
                }
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
                if let Some(removed) = self.table.remove(idx) {
                    let slot = crate::router::key_slot(&removed.key);
                    if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                        set.remove(&removed.key);
                    }
                }
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

        let mut set = hashbrown::HashSet::with_capacity(members.len());
        let mut added = 0;
        for m in members {
            if set.insert(m) {
                added += 1;
            }
        }
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(key.clone());
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
                if let Some(removed) = self.table.remove(idx) {
                    let slot = crate::router::key_slot(&removed.key);
                    if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                        set.remove(&removed.key);
                    }
                }
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
                    RudisValue::Set(set) => Ok(set.iter().cloned().collect()),
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
            if self.check_expired_slot(idx) {
                return Ok(false);
            }
            if let Some(entry) = self.table.get_slot(idx) {
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
                            if let Some(elem) = set.iter().next().cloned() {
                                set.remove(&elem);
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
                if let Some(removed) = self.table.remove(idx) {
                    let slot = crate::router::key_slot(&removed.key);
                    if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                        set.remove(&removed.key);
                    }
                }
            }
            Ok(popped)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
        if let Some(keys) = self.slot_to_keys.get_mut(&slot) {
            let mut expired = Vec::new();
            for k in keys.iter() {
                let h = hash_key(k);
                if let Some(idx) = self.table.find(k, h) {
                    if let Some(entry) = self.table.get_slot(idx) {
                        if let Some(exp) = entry.expire_at {
                            if Instant::now() >= exp {
                                expired.push(k.clone());
                            }
                        }
                    }
                }
            }
            for k in expired {
                keys.remove(&k);
                let h = hash_key(&k);
                if let Some(idx) = self.table.find(&k, h) {
                    self.table.remove(idx);
                }
            }
            keys.len()
        } else {
            0
        }
    }

    pub fn get_keys_in_slot(&mut self, slot: u16, count: usize) -> Vec<Bytes> {
        if let Some(keys) = self.slot_to_keys.get_mut(&slot) {
            let mut expired = Vec::new();
            for k in keys.iter() {
                let h = hash_key(k);
                if let Some(idx) = self.table.find(k, h) {
                    if let Some(entry) = self.table.get_slot(idx) {
                        if let Some(exp) = entry.expire_at {
                            if Instant::now() >= exp {
                                expired.push(k.clone());
                            }
                        }
                    }
                }
            }
            for k in expired {
                keys.remove(&k);
                let h = hash_key(&k);
                if let Some(idx) = self.table.find(&k, h) {
                    self.table.remove(idx);
                }
            }
            keys.iter().take(count).cloned().collect()
        } else {
            Vec::new()
        }
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
                            if let Some(&old_score) = zset.dict.get(&member) {
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
                                    zset.tree.remove(&(OrderedScore(old_score), member.clone()));
                                    zset.tree.insert((OrderedScore(new_score), member.clone()));
                                    zset.dict.insert(member, new_score);
                                    changed_count += 1;
                                }
                                if flags.incr {
                                    new_score_incr = Some(new_score);
                                }
                            } else {
                                if flags.xx {
                                    continue;
                                }
                                zset.tree.insert((OrderedScore(score), member.clone()));
                                zset.dict.insert(member, score);
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
            zset.tree.insert((OrderedScore(score), member.clone()));
            zset.dict.insert(member, score);
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
                    RudisValue::ZSet(zset) => Ok(zset.dict.get(member).copied()),
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
                    RudisValue::ZSet(zset) => {
                        if !zset.dict.contains_key(member) {
                            return Ok(None);
                        }
                        if rev {
                            let rank = zset.tree.iter().rev().position(|(_, m)| m == member);
                            Ok(rank)
                        } else {
                            let rank = zset.tree.iter().position(|(_, m)| m == member);
                            Ok(rank)
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
                    RudisValue::ZSet(zset) => {
                        let count = zset
                            .tree
                            .iter()
                            .filter(|(OrderedScore(s), _)| {
                                let ge_min = if min_inc { *s >= min } else { *s > min };
                                let le_max = if max_inc { *s <= max } else { *s < max };
                                ge_min && le_max
                            })
                            .count();
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

    pub fn zincrby(&mut self, key: Bytes, delta: f64, member: Bytes) -> Result<f64, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                // Expired slot has been cleaned up, will insert as new below
            } else if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let new_score = if let Some(&old_score) = zset.dict.get(&member) {
                            zset.tree.remove(&(OrderedScore(old_score), member.clone()));
                            let s = old_score + delta;
                            zset.tree.insert((OrderedScore(s), member.clone()));
                            zset.dict.insert(member, s);
                            s
                        } else {
                            zset.tree.insert((OrderedScore(delta), member.clone()));
                            zset.dict.insert(member, delta);
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
        zset.tree.insert((OrderedScore(delta), member.clone()));
        zset.dict.insert(member, delta);
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
                    RudisValue::ZSet(zset) => {
                        let n = zset.len();
                        if n == 0 {
                            return Ok(Vec::new());
                        }

                        if opts.by_score {
                            let min = opts.min_score;
                            let min_inc = opts.min_inc;
                            let max = opts.max_score;
                            let max_inc = opts.max_inc;

                            let make_iter = || {
                                zset.tree.iter().filter(move |(OrderedScore(s), _)| {
                                    let ge_min = if min_inc { *s >= min } else { *s > min };
                                    let le_max = if max_inc { *s <= max } else { *s < max };
                                    ge_min && le_max
                                })
                            };

                            let res: Vec<(Bytes, f64)> = if opts.rev {
                                let rev_items: Vec<_> = make_iter().collect();
                                let skipped = rev_items.into_iter().rev().skip(opts.offset);
                                if let Some(c) = opts.count {
                                    skipped
                                        .take(c)
                                        .map(|(OrderedScore(s), m)| (m.clone(), *s))
                                        .collect()
                                } else {
                                    skipped
                                        .map(|(OrderedScore(s), m)| (m.clone(), *s))
                                        .collect()
                                }
                            } else {
                                let skipped = make_iter().skip(opts.offset);
                                if let Some(c) = opts.count {
                                    skipped
                                        .take(c)
                                        .map(|(OrderedScore(s), m)| (m.clone(), *s))
                                        .collect()
                                } else {
                                    skipped
                                        .map(|(OrderedScore(s), m)| (m.clone(), *s))
                                        .collect()
                                }
                            };
                            Ok(res)
                        } else {
                            let mut start = opts.start;
                            let mut stop = opts.stop;
                            let n_i = n as i64;
                            if start < 0 {
                                start = (n_i + start).max(0);
                            }
                            if stop < 0 {
                                stop = n_i + stop;
                            }
                            if start > stop || start >= n_i {
                                return Ok(Vec::new());
                            }
                            let start_u = start.max(0) as usize;
                            let stop_u = (stop.min(n_i - 1) as usize).max(start_u);
                            let limit = stop_u - start_u + 1;

                            let res: Vec<(Bytes, f64)> = if opts.rev {
                                zset.tree
                                    .iter()
                                    .rev()
                                    .skip(start_u)
                                    .take(limit)
                                    .map(|(OrderedScore(s), m)| (m.clone(), *s))
                                    .collect()
                            } else {
                                zset.tree
                                    .iter()
                                    .skip(start_u)
                                    .take(limit)
                                    .map(|(OrderedScore(s), m)| (m.clone(), *s))
                                    .collect()
                            };
                            Ok(res)
                        }
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
                            if let Some(old_score) = zset.dict.remove(m) {
                                zset.tree.remove(&(OrderedScore(old_score), m.clone()));
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
                        let n = count.min(zset.len());
                        let mut popped = Vec::with_capacity(n);
                        for _ in 0..n {
                            if let Some((OrderedScore(s), m)) = zset.tree.pop_first() {
                                zset.dict.remove(&m);
                                popped.push((m, s));
                            }
                        }
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
                        let n = count.min(zset.len());
                        let mut popped = Vec::with_capacity(n);
                        for _ in 0..n {
                            if let Some((OrderedScore(s), m)) = zset.tree.pop_last() {
                                zset.dict.remove(&m);
                                popped.push((m, s));
                            }
                        }
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
}
