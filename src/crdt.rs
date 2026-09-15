//! Active-Active Multi-Region Replication with Conflict-Free Replicated Data Types (CRDTs).
//!
//! Provides mathematically sound, deterministic conflict resolution across independent
//! Rudis clusters/regions without central coordination or two-phase commit:
//! - **Hybrid Logical Clocks (HLC)**: Monotonically increasing causal timestamps.
//! - **LWW-Register (Last-Write-Wins)**: Deterministic register convergence for string keys.
//! - **ORSet (Observed-Remove Set)**: Causally consistent sets with add-wins semantics.
//! - **PN-Counter (Positive-Negative Counter)**: Distributed counters supporting atomic increment/decrement.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

/// Hybrid Logical Clock (HLC) Timestamp.
/// Provides causal ordering across multiple independent nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HlcTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
    pub node_id: u16,
}

impl HlcTimestamp {
    pub fn new(physical_ms: u64, logical: u32, node_id: u16) -> Self {
        Self {
            physical_ms,
            logical,
            node_id,
        }
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.physical_ms
            .cmp(&other.physical_ms)
            .then_with(|| self.logical.cmp(&other.logical))
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

/// Hybrid Logical Clock generator.
pub struct HybridLogicalClock {
    pub node_id: u16,
    latest_physical_ms: AtomicU64,
    latest_logical: AtomicU32,
}

impl HybridLogicalClock {
    pub fn new(node_id: u16) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            node_id,
            latest_physical_ms: AtomicU64::new(now),
            latest_logical: AtomicU32::new(0),
        }
    }

    /// Generates a new unique, monotonically increasing HLC timestamp for a local write.
    pub fn now(&self) -> HlcTimestamp {
        let phys_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        loop {
            let cur_phys = self.latest_physical_ms.load(AtomicOrdering::Acquire);
            let cur_log = self.latest_logical.load(AtomicOrdering::Acquire);

            let (next_phys, next_log) = if phys_now > cur_phys {
                (phys_now, 0)
            } else {
                (cur_phys, cur_log + 1)
            };

            if self
                .latest_physical_ms
                .compare_exchange(
                    cur_phys,
                    next_phys,
                    AtomicOrdering::Release,
                    AtomicOrdering::Relaxed,
                )
                .is_ok()
            {
                self.latest_logical.store(next_log, AtomicOrdering::Release);
                return HlcTimestamp::new(next_phys, next_log, self.node_id);
            }
        }
    }

    /// Advances the local clock upon receiving a remote HLC timestamp from another region.
    pub fn update(&self, remote: &HlcTimestamp) -> HlcTimestamp {
        let phys_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        loop {
            let cur_phys = self.latest_physical_ms.load(AtomicOrdering::Acquire);
            let cur_log = self.latest_logical.load(AtomicOrdering::Acquire);

            let max_phys = phys_now.max(cur_phys).max(remote.physical_ms);
            let next_log = if max_phys == cur_phys && max_phys == remote.physical_ms {
                cur_log.max(remote.logical) + 1
            } else if max_phys == cur_phys {
                cur_log + 1
            } else if max_phys == remote.physical_ms {
                remote.logical + 1
            } else {
                0
            };

            if self
                .latest_physical_ms
                .compare_exchange(
                    cur_phys,
                    max_phys,
                    AtomicOrdering::Release,
                    AtomicOrdering::Relaxed,
                )
                .is_ok()
            {
                self.latest_logical.store(next_log, AtomicOrdering::Release);
                return HlcTimestamp::new(max_phys, next_log, self.node_id);
            }
        }
    }
}

/// Last-Write-Wins Register (LWW-Register).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LwwRegister {
    pub value: Bytes,
    pub timestamp: HlcTimestamp,
    pub tombstone: bool,
}

impl LwwRegister {
    pub fn new(value: Bytes, timestamp: HlcTimestamp) -> Self {
        Self {
            value,
            timestamp,
            tombstone: false,
        }
    }

    pub fn tombstone(timestamp: HlcTimestamp) -> Self {
        Self {
            value: Bytes::new(),
            timestamp,
            tombstone: true,
        }
    }

    /// Merges another LWW-Register into self. Returns `true` if self was modified.
    pub fn merge(&mut self, other: &LwwRegister) -> bool {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.tombstone = other.tombstone;
            true
        } else {
            false
        }
    }
}

/// Observed-Remove Set (ORSet) with add-wins semantics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrSet {
    /// Maps each member to the set of unique add timestamps currently observing it.
    pub elements: HashMap<Bytes, HashSet<HlcTimestamp>>,
    /// Tombstone timestamps of removed instances.
    pub tombstones: HashSet<HlcTimestamp>,
}

impl OrSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a member with a unique HLC timestamp.
    pub fn add(&mut self, element: Bytes, ts: HlcTimestamp) {
        self.elements.entry(element).or_default().insert(ts);
    }

    /// Removes a member by moving all its current add timestamps to the tombstone set.
    pub fn remove(&mut self, element: &Bytes) -> bool {
        if let Some(tags) = self.elements.remove(element) {
            let count = tags.len();
            for tag in tags {
                self.tombstones.insert(tag);
            }
            count > 0
        } else {
            false
        }
    }

    /// Reads active elements in the set (where at least one add tag has not been tombstoned).
    pub fn read(&self) -> Vec<Bytes> {
        let mut res = Vec::new();
        for (elem, tags) in &self.elements {
            if tags.iter().any(|tag| !self.tombstones.contains(tag)) {
                res.push(elem.clone());
            }
        }
        res
    }

    /// Checks if an element exists in the set.
    pub fn contains(&self, element: &Bytes) -> bool {
        if let Some(tags) = self.elements.get(element) {
            tags.iter().any(|tag| !self.tombstones.contains(tag))
        } else {
            false
        }
    }

    /// Merges another ORSet into self using state-based CRDT join.
    pub fn merge(&mut self, other: &OrSet) {
        // Union tombstones
        for ts in &other.tombstones {
            self.tombstones.insert(*ts);
        }

        // Union elements
        for (elem, other_tags) in &other.elements {
            let my_tags = self.elements.entry(elem.clone()).or_default();
            for tag in other_tags {
                my_tags.insert(*tag);
            }
        }

        // Clean up tombstoned tags
        self.elements.retain(|_, tags| {
            tags.retain(|tag| !self.tombstones.contains(tag));
            !tags.is_empty()
        });
    }

    /// Prunes tombstones older than `cutoff_physical_ms`.
    /// Returns the number of tombstones pruned.
    pub fn prune_tombstones(&mut self, cutoff_physical_ms: u64) -> usize {
        let before_len = self.tombstones.len();
        self.tombstones
            .retain(|ts| ts.physical_ms >= cutoff_physical_ms);
        before_len - self.tombstones.len()
    }
}

/// Positive-Negative Counter (PN-Counter).
/// Allows distributed atomic increments and decrements with commutative state merges.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PnCounter {
    pub p: HashMap<u16, i64>,
    pub n: HashMap<u16, i64>,
}

impl PnCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn value(&self) -> i64 {
        let pos_sum: i64 = self.p.values().sum();
        let neg_sum: i64 = self.n.values().sum();
        pos_sum - neg_sum
    }

    pub fn inc(&mut self, node_id: u16, delta: i64) {
        if delta >= 0 {
            *self.p.entry(node_id).or_default() += delta;
        } else {
            *self.n.entry(node_id).or_default() += -delta;
        }
    }

    pub fn dec(&mut self, node_id: u16, delta: i64) {
        self.inc(node_id, -delta);
    }

    /// Merges another PN-Counter using component-wise maximum.
    pub fn merge(&mut self, other: &PnCounter) {
        for (&node_id, &val) in &other.p {
            let entry = self.p.entry(node_id).or_default();
            *entry = (*entry).max(val);
        }
        for (&node_id, &val) in &other.n {
            let entry = self.n.entry(node_id).or_default();
            *entry = (*entry).max(val);
        }
    }
}

/// Multi-region CRDT Store per Rudis instance.
pub struct CrdtStore {
    pub clock: HybridLogicalClock,
    pub registers: HashMap<Bytes, LwwRegister>,
    pub sets: HashMap<Bytes, OrSet>,
    pub counters: HashMap<Bytes, PnCounter>,
}

impl CrdtStore {
    pub fn new(node_id: u16) -> Self {
        Self {
            clock: HybridLogicalClock::new(node_id),
            registers: HashMap::new(),
            sets: HashMap::new(),
            counters: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Bytes, val: Bytes) -> HlcTimestamp {
        let ts = self.clock.now();
        self.registers.insert(key, LwwRegister::new(val, ts));
        ts
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.registers.get(key).and_then(|r| {
            if r.tombstone {
                None
            } else {
                Some(r.value.clone())
            }
        })
    }

    pub fn del(&mut self, key: &Bytes) -> bool {
        let ts = self.clock.now();
        if let Some(r) = self.registers.get_mut(key)
            && !r.tombstone
        {
            r.tombstone = true;
            r.timestamp = ts;
            return true;
        }
        false
    }

    pub fn counter_incr(&mut self, key: Bytes, delta: i64) -> i64 {
        let node_id = self.clock.node_id;
        let c = self.counters.entry(key).or_default();
        c.inc(node_id, delta);
        c.value()
    }

    pub fn counter_get(&self, key: &Bytes) -> i64 {
        self.counters.get(key).map(|c| c.value()).unwrap_or(0)
    }

    pub fn set_add(&mut self, key: Bytes, member: Bytes) -> bool {
        let ts = self.clock.now();
        let s = self.sets.entry(key).or_default();
        let was_present = s.contains(&member);
        s.add(member, ts);
        !was_present
    }

    pub fn set_members(&self, key: &Bytes) -> Vec<Bytes> {
        self.sets.get(key).map(|s| s.read()).unwrap_or_default()
    }

    pub fn set_rem(&mut self, key: &Bytes, member: &Bytes) -> bool {
        if let Some(s) = self.sets.get_mut(key) {
            s.remove(member)
        } else {
            false
        }
    }

    /// Exports full CRDT state payload for replication sync across regions.
    pub fn export_sync_payload(&self) -> Vec<u8> {
        // Simple fast binary serialization: [type: u8][key_len: u32][key]...
        let mut buf = Vec::new();
        // Registers
        for (k, r) in &self.registers {
            buf.push(1u8); // type: Register
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(r.value.len() as u32).to_le_bytes());
            buf.extend_from_slice(&r.value);
            buf.extend_from_slice(&r.timestamp.physical_ms.to_le_bytes());
            buf.extend_from_slice(&r.timestamp.logical.to_le_bytes());
            buf.extend_from_slice(&r.timestamp.node_id.to_le_bytes());
            buf.push(if r.tombstone { 1 } else { 0 });
        }
        // Counters
        for (k, c) in &self.counters {
            buf.push(2u8); // type: Counter
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(c.p.len() as u32).to_le_bytes());
            for (&nid, &v) in &c.p {
                buf.extend_from_slice(&nid.to_le_bytes());
                buf.extend_from_slice(&v.to_le_bytes());
            }
            buf.extend_from_slice(&(c.n.len() as u32).to_le_bytes());
            for (&nid, &v) in &c.n {
                buf.extend_from_slice(&nid.to_le_bytes());
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        // Sets
        for (k, s) in &self.sets {
            buf.push(3u8); // type: Set
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(s.elements.len() as u32).to_le_bytes());
            for (elem, tags) in &s.elements {
                buf.extend_from_slice(&(elem.len() as u32).to_le_bytes());
                buf.extend_from_slice(elem);
                buf.extend_from_slice(&(tags.len() as u32).to_le_bytes());
                for tag in tags {
                    buf.extend_from_slice(&tag.physical_ms.to_le_bytes());
                    buf.extend_from_slice(&tag.logical.to_le_bytes());
                    buf.extend_from_slice(&tag.node_id.to_le_bytes());
                }
            }
            buf.extend_from_slice(&(s.tombstones.len() as u32).to_le_bytes());
            for tag in &s.tombstones {
                buf.extend_from_slice(&tag.physical_ms.to_le_bytes());
                buf.extend_from_slice(&tag.logical.to_le_bytes());
                buf.extend_from_slice(&tag.node_id.to_le_bytes());
            }
        }
        buf
    }

    /// Merges remote CRDT state payload into local store.
    pub fn merge_sync_payload(&mut self, data: &[u8]) -> Result<usize, String> {
        let mut offset = 0;
        let mut merged_items = 0;

        while offset < data.len() {
            let item_type = data[offset];
            offset += 1;

            match item_type {
                1 => {
                    // Register
                    if offset + 4 > data.len() {
                        break;
                    }
                    let k_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    let k = Bytes::copy_from_slice(&data[offset..offset + k_len]);
                    offset += k_len;

                    let v_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    let v = Bytes::copy_from_slice(&data[offset..offset + v_len]);
                    offset += v_len;

                    let phys = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let log = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    let nid = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                    offset += 2;
                    let tombstone = data[offset] == 1;
                    offset += 1;

                    let ts = HlcTimestamp::new(phys, log, nid);
                    self.clock.update(&ts);

                    let remote_reg = LwwRegister {
                        value: v,
                        timestamp: ts,
                        tombstone,
                    };
                    self.registers
                        .entry(k)
                        .or_insert_with(|| remote_reg.clone())
                        .merge(&remote_reg);
                    merged_items += 1;
                }
                2 => {
                    // Counter
                    if offset + 4 > data.len() {
                        break;
                    }
                    let k_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    let k = Bytes::copy_from_slice(&data[offset..offset + k_len]);
                    offset += k_len;

                    let mut p = HashMap::new();
                    let p_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    for _ in 0..p_len {
                        let nid = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                        offset += 2;
                        let val = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                        p.insert(nid, val);
                    }

                    let mut n = HashMap::new();
                    let n_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    for _ in 0..n_len {
                        let nid = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                        offset += 2;
                        let val = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                        n.insert(nid, val);
                    }

                    let remote_counter = PnCounter { p, n };
                    self.counters.entry(k).or_default().merge(&remote_counter);
                    merged_items += 1;
                }
                3 => {
                    // Set
                    if offset + 4 > data.len() {
                        break;
                    }
                    let k_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    let k = Bytes::copy_from_slice(&data[offset..offset + k_len]);
                    offset += k_len;

                    let mut elements = HashMap::new();
                    let elem_count =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    for _ in 0..elem_count {
                        let e_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
                            as usize;
                        offset += 4;
                        let elem = Bytes::copy_from_slice(&data[offset..offset + e_len]);
                        offset += e_len;

                        let tag_count =
                            u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
                                as usize;
                        offset += 4;
                        let mut tags = HashSet::new();
                        for _ in 0..tag_count {
                            let phys =
                                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                            offset += 8;
                            let log =
                                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                            offset += 4;
                            let nid =
                                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                            offset += 2;
                            let ts = HlcTimestamp::new(phys, log, nid);
                            self.clock.update(&ts);
                            tags.insert(ts);
                        }
                        elements.insert(elem, tags);
                    }

                    let mut tombstones = HashSet::new();
                    let tomb_count =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    for _ in 0..tomb_count {
                        let phys = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                        let log = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                        offset += 4;
                        let nid = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                        offset += 2;
                        let ts = HlcTimestamp::new(phys, log, nid);
                        self.clock.update(&ts);
                        tombstones.insert(ts);
                    }

                    let remote_set = OrSet {
                        elements,
                        tombstones,
                    };
                    self.sets.entry(k).or_default().merge(&remote_set);
                    merged_items += 1;
                }
                _ => return Err(format!("Unknown CRDT item type: {}", item_type)),
            }
        }

        Ok(merged_items)
    }

    /// Automated garbage collection for all tombstones older than `ttl_ms` (default: 86_400_000 ms / 24h).
    pub fn gc_tombstones(&mut self, ttl_ms: u64) -> (usize, usize) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now.saturating_sub(ttl_ms);

        let before_regs = self.registers.len();
        self.registers
            .retain(|_, reg| !reg.tombstone || reg.timestamp.physical_ms >= cutoff);
        let reg_pruned = before_regs - self.registers.len();

        let mut set_pruned = 0;
        for s in self.sets.values_mut() {
            set_pruned += s.prune_tombstones(cutoff);
        }
        (reg_pruned, set_pruned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_monotonicity() {
        let clock = HybridLogicalClock::new(1);
        let t1 = clock.now();
        let t2 = clock.now();
        assert!(t2 > t1);
    }

    #[test]
    fn test_lww_register_convergence() {
        let mut r1 = LwwRegister::new(Bytes::from_static(b"val1"), HlcTimestamp::new(100, 0, 1));
        let r2 = LwwRegister::new(Bytes::from_static(b"val2"), HlcTimestamp::new(105, 0, 2));

        r1.merge(&r2);
        assert_eq!(r1.value.as_ref(), b"val2");

        let r_older = LwwRegister::new(Bytes::from_static(b"val0"), HlcTimestamp::new(90, 0, 3));
        r1.merge(&r_older);
        assert_eq!(r1.value.as_ref(), b"val2");
    }

    #[test]
    fn test_pn_counter_convergence() {
        let mut c1 = PnCounter::new();
        c1.inc(1, 10);
        c1.dec(1, 2);

        let mut c2 = PnCounter::new();
        c2.inc(2, 5);
        c2.dec(2, 1);

        c1.merge(&c2);
        assert_eq!(c1.value(), (10 - 2) + (5 - 1)); // 12
    }

    #[test]
    fn test_orset_add_wins() {
        let mut s1 = OrSet::new();
        let ts1 = HlcTimestamp::new(100, 0, 1);
        s1.add(Bytes::from_static(b"apple"), ts1);

        let mut s2 = s1.clone();
        s2.remove(&Bytes::from_static(b"apple"));

        // Concurrent add on s1 with higher timestamp
        let ts2 = HlcTimestamp::new(101, 0, 1);
        s1.add(Bytes::from_static(b"apple"), ts2);

        // Merge s2 into s1
        s1.merge(&s2);
        // Add-wins: ts2 was not tombstoned by s2, so apple remains!
        assert!(s1.contains(&Bytes::from_static(b"apple")));
    }
}
