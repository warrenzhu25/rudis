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

    #[inline]
    pub fn insert(&mut self, score: f64, member: Bytes) {
        if let Some(old_score) = self.dict.insert(member.clone(), score) {
            self.tree.remove(&(OrderedScore(old_score), member.clone()));
        }
        self.tree.insert((OrderedScore(score), member));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RudisValue {
    String(Bytes),
    Hash(HashMap<Bytes, Bytes>),
    List(std::collections::VecDeque<Bytes>),
    Set(hashbrown::HashSet<Bytes>),
    ZSet(RudisZSet),
    HyperLogLog(Box<[u8; 16384]>),
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
                    RudisValue::HyperLogLog(_) => "string",
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
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(key.clone());
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
                match &entry.val {
                    RudisValue::String(b) => {
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
                match &entry.val {
                    RudisValue::String(b) => {
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
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        )
                    }
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
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(key.clone());
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

        let slot = crate::router::key_slot(&destkey);
        self.slot_to_keys
            .entry(slot)
            .or_default()
            .insert(destkey.clone());
        let entry = RudisEntry {
            key: destkey,
            val: RudisValue::HyperLogLog(Box::new(merged)),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(())
    }

    // RDB SERIALIZATION & DUMP / RESTORE
    pub fn dump(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        if self.check_expired_slot(idx) {
            return None;
        }
        let entry = self.table.get_slot(idx)?;
        let mut payload = Vec::new();
        match &entry.val {
            RudisValue::String(b) => {
                payload.push(0u8);
                payload.extend_from_slice(&(b.len() as u32).to_le_bytes());
                payload.extend_from_slice(b);
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
                for (m, score) in &z.dict {
                    payload.extend_from_slice(&(m.len() as u32).to_le_bytes());
                    payload.extend_from_slice(m);
                    payload.extend_from_slice(&score.to_bits().to_le_bytes());
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
        }

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
        let mut cursor = 0;
        if cursor >= payload_len {
            return Err("DUMP payload version or checksum are wrong");
        }
        let type_byte = serialized[cursor];
        cursor += 1;

        let decoded_value = match type_byte {
            0 => {
                // String
                if cursor + 4 > payload_len {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let len = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + len > payload_len {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let val = Bytes::copy_from_slice(&serialized[cursor..cursor + len]);
                RudisValue::String(val)
            }
            1 => {
                // List
                if cursor + 4 > payload_len {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut list = std::collections::VecDeque::with_capacity(count);
                for _ in 0..count {
                    if cursor + 4 > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    list.push_back(Bytes::copy_from_slice(&serialized[cursor..cursor + len]));
                    cursor += len;
                }
                RudisValue::List(list)
            }
            2 => {
                // Set
                if cursor + 4 > payload_len {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut set = hashbrown::HashSet::with_capacity(count);
                for _ in 0..count {
                    if cursor + 4 > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    set.insert(Bytes::copy_from_slice(&serialized[cursor..cursor + len]));
                    cursor += len;
                }
                RudisValue::Set(set)
            }
            3 => {
                // ZSet
                if cursor + 4 > payload_len {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut zset = RudisZSet::new();
                for _ in 0..count {
                    if cursor + 4 > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let member = Bytes::copy_from_slice(&serialized[cursor..cursor + len]);
                    cursor += len;
                    if cursor + 8 > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let score_bits = u64::from_le_bytes(serialized[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    let score = f64::from_bits(score_bits);
                    zset.insert(score, member);
                }
                RudisValue::ZSet(zset)
            }
            4 => {
                // Hash
                if cursor + 4 > payload_len {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut hash = hashbrown::HashMap::with_capacity(count);
                for _ in 0..count {
                    if cursor + 4 > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let f_len = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + f_len > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let f = Bytes::copy_from_slice(&serialized[cursor..cursor + f_len]);
                    cursor += f_len;

                    if cursor + 4 > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let v_len = u32::from_le_bytes(serialized[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + v_len > payload_len {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let v = Bytes::copy_from_slice(&serialized[cursor..cursor + v_len]);
                    cursor += v_len;

                    hash.insert(f, v);
                }
                RudisValue::Hash(hash)
            }
            5 => {
                // HyperLogLog
                if cursor + 16384 > payload_len {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let mut regs = Box::new([0u8; 16384]);
                regs.copy_from_slice(&serialized[cursor..cursor + 16384]);
                RudisValue::HyperLogLog(regs)
            }
            _ => return Err("DUMP payload version or checksum are wrong"),
        };

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

        let slot = crate::router::key_slot(&key);
        self.slot_to_keys.entry(slot).or_default().insert(key.clone());
        let entry = RudisEntry {
            key,
            val: decoded_value,
            expire_at,
        };
        self.table.insert(entry);
        Ok(())
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
}
