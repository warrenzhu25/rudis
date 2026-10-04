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
    /// Returns the tombstoned timestamps (empty if the member was absent).
    pub fn remove(&mut self, element: &Bytes) -> Vec<HlcTimestamp> {
        let tags: Vec<HlcTimestamp> = self
            .elements
            .remove(element)
            .map(|tags| tags.into_iter().collect())
            .unwrap_or_default();
        self.tombstones.extend(tags.iter().copied());
        tags
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

const PN_OVERFLOW: &str = "increment or decrement would overflow";

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

    /// Exact counter value; summed in i128 so merged per-node maxima cannot overflow.
    fn total(&self) -> i128 {
        let pos_sum: i128 = self.p.values().map(|&v| v as i128).sum();
        let neg_sum: i128 = self.n.values().map(|&v| v as i128).sum();
        pos_sum - neg_sum
    }

    pub fn value(&self) -> i64 {
        self.total().clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }

    /// Adds `delta` to this node's component. Like INCRBY, a change that would
    /// overflow the value or a component is refused and leaves the counter as is.
    pub fn inc(&mut self, node_id: u16, delta: i64) -> Result<(), &'static str> {
        i64::try_from(self.total() + delta as i128).map_err(|_| PN_OVERFLOW)?;
        let side = if delta >= 0 { &mut self.p } else { &mut self.n };
        let cur = side.get(&node_id).copied().unwrap_or(0);
        let next =
            i64::try_from(cur as i128 + delta.unsigned_abs() as i128).map_err(|_| PN_OVERFLOW)?;
        side.insert(node_id, next);
        Ok(())
    }

    pub fn dec(&mut self, node_id: u16, delta: i64) -> Result<(), &'static str> {
        self.inc(node_id, delta.checked_neg().ok_or(PN_OVERFLOW)?)
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

    pub fn counter_incr(&mut self, key: Bytes, delta: i64) -> Result<i64, &'static str> {
        let node_id = self.clock.node_id;
        if let Some(c) = self.counters.get_mut(&key) {
            c.inc(node_id, delta)?;
            return Ok(c.value());
        }
        // Built aside so a refused increment does not leave an empty counter.
        let mut c = PnCounter::new();
        c.inc(node_id, delta)?;
        let value = c.value();
        self.counters.insert(key, c);
        Ok(value)
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

    /// Removes `member` from the set at `key`. Returns the add timestamps it
    /// tombstoned (empty if `member` was not in the set).
    pub fn set_rem(&mut self, key: &Bytes, member: &Bytes) -> Vec<HlcTimestamp> {
        self.sets
            .get_mut(key)
            .map(|s| s.remove(member))
            .unwrap_or_default()
    }

    /// Exports full CRDT state payload for replication sync across regions.
    pub fn export_sync_payload(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (k, r) in &self.registers {
            encode_register(&mut buf, k, r);
        }
        for (k, c) in &self.counters {
            encode_counter(&mut buf, k, c);
        }
        for (k, s) in &self.sets {
            encode_set(&mut buf, k, s.elements.iter(), s.tombstones.iter());
        }
        buf
    }

    /// The same state as `export_sync_payload`, as one payload per register,
    /// counter and set. The AOF rewrite logs each as a `CRDT.MERGE`.
    pub fn export_entry_payloads(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        let registers = self.registers.iter().map(|(k, r)| {
            let mut buf = Vec::new();
            encode_register(&mut buf, k, r);
            buf
        });
        let counters = self.counters.iter().map(|(k, c)| {
            let mut buf = Vec::new();
            encode_counter(&mut buf, k, c);
            buf
        });
        let sets = self.sets.iter().map(|(k, s)| {
            let mut buf = Vec::new();
            encode_set(&mut buf, k, s.elements.iter(), s.tombstones.iter());
            buf
        });
        registers.chain(counters).chain(sets)
    }

    /// The register at `key` (including a deletion tombstone) as a sync
    /// payload; empty if there is none.
    pub fn register_payload(&self, key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(r) = self.registers.get(key) {
            encode_register(&mut buf, key, r);
        }
        buf
    }

    /// The counter at `key` as a sync payload; empty if there is none.
    pub fn counter_payload(&self, key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(c) = self.counters.get(key) {
            encode_counter(&mut buf, key, c);
        }
        buf
    }

    /// `member`'s add timestamps in the set at `key` as a sync payload, so
    /// merging it adds `member` exactly as this store holds it; empty if
    /// `member` is not in the set.
    pub fn set_member_payload(&self, key: &[u8], member: &Bytes) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(tags) = self.sets.get(key).and_then(|s| s.elements.get(member)) {
            encode_set(
                &mut buf,
                key,
                std::iter::once((member, tags)),
                std::iter::empty(),
            );
        }
        buf
    }

    /// A sync payload that tombstones `tags` in the set at `key`: merging it
    /// removes what the `set_rem` that returned `tags` removed.
    pub fn set_removal_payload(key: &[u8], tags: &[HlcTimestamp]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_set(&mut buf, key, std::iter::empty(), tags.iter());
        buf
    }

    /// Merges remote CRDT state payload into local store.
    ///
    /// The whole payload is decoded before anything is applied, so a
    /// malformed one is an error that changes nothing.
    pub fn merge_sync_payload(&mut self, data: &[u8]) -> Result<usize, String> {
        Ok(self.merge_entries(decode_sync_payload(data)?))
    }

    /// Merges decoded entries; returns how many.
    pub fn merge_entries(&mut self, entries: impl IntoIterator<Item = CrdtEntry>) -> usize {
        let mut merged_items = 0;
        for entry in entries {
            self.merge_entry(entry);
            merged_items += 1;
        }
        merged_items
    }

    fn merge_entry(&mut self, entry: CrdtEntry) {
        match entry {
            CrdtEntry::Register(k, remote_reg) => {
                self.clock.update(&remote_reg.timestamp);
                self.registers
                    .entry(k)
                    .or_insert_with(|| remote_reg.clone())
                    .merge(&remote_reg);
            }
            CrdtEntry::Counter(k, remote_counter) => {
                self.counters.entry(k).or_default().merge(&remote_counter);
            }
            CrdtEntry::Set(k, remote_set) => {
                for ts in remote_set
                    .elements
                    .values()
                    .flatten()
                    .chain(&remote_set.tombstones)
                {
                    self.clock.update(ts);
                }
                self.sets.entry(k).or_default().merge(&remote_set);
            }
        }
    }

    /// Garbage collection for all tombstones from before `cutoff_physical_ms`
    /// (unix ms; see `GcHorizon`): deleted registers are dropped and removed
    /// set tags forgotten. Returns how many of each were pruned.
    pub fn gc_tombstones(&mut self, cutoff_physical_ms: u64) -> (usize, usize) {
        let before_regs = self.registers.len();
        self.registers
            .retain(|_, reg| !reg.tombstone || reg.timestamp.physical_ms >= cutoff_physical_ms);
        let reg_pruned = before_regs - self.registers.len();

        let mut set_pruned = 0;
        for s in self.sets.values_mut() {
            set_pruned += s.prune_tombstones(cutoff_physical_ms);
        }
        (reg_pruned, set_pruned)
    }
}

/// Which tombstones `CRDT.GC` prunes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcHorizon {
    /// `CRDT.GC [ttl-ms]`: those older than `ttl-ms` (default 24 hours).
    Ttl(Option<u64>),
    /// `CRDT.GC BEFORE <unix-ms>`: those from before an absolute time. A GC
    /// is logged in this form, so replaying it later prunes exactly what the
    /// original did.
    Before(u64),
}

impl GcHorizon {
    pub const DEFAULT_TTL_MS: u64 = 86_400_000;

    /// The absolute cutoff, in unix milliseconds.
    pub fn cutoff_ms(self) -> u64 {
        match self {
            GcHorizon::Ttl(ttl) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                now.saturating_sub(ttl.unwrap_or(Self::DEFAULT_TTL_MS))
            }
            GcHorizon::Before(cutoff) => cutoff,
        }
    }
}

/// One keyed entry of a sync payload (`CRDT.DUMP` / `CRDT.MERGE`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CrdtEntry {
    Register(Bytes, LwwRegister),
    Counter(Bytes, PnCounter),
    Set(Bytes, OrSet),
}

impl CrdtEntry {
    pub fn key(&self) -> &Bytes {
        match self {
            CrdtEntry::Register(k, _) | CrdtEntry::Counter(k, _) | CrdtEntry::Set(k, _) => k,
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            CrdtEntry::Register(k, r) => encode_register(buf, k, r),
            CrdtEntry::Counter(k, c) => encode_counter(buf, k, c),
            CrdtEntry::Set(k, s) => encode_set(buf, k, s.elements.iter(), s.tombstones.iter()),
        }
    }
}

// Sync payload wire format: a sequence of entries, each
//   [type: u8][key_len: u32][key] followed by
//   1 (register): [value_len: u32][value][ts][tombstone: u8]
//   2 (counter):  [p_len: u32]([node_id: u16][count: i64])* [n_len: u32]([node_id: u16][count: i64])*
//   3 (set):      [elem_count: u32]([elem_len: u32][elem][tag_count: u32][ts]*)* [tomb_count: u32][ts]*
// where ts = [physical_ms: u64][logical: u32][node_id: u16], all little-endian.

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

fn put_ts(buf: &mut Vec<u8>, ts: &HlcTimestamp) {
    buf.extend_from_slice(&ts.physical_ms.to_le_bytes());
    buf.extend_from_slice(&ts.logical.to_le_bytes());
    buf.extend_from_slice(&ts.node_id.to_le_bytes());
}

fn encode_register(buf: &mut Vec<u8>, key: &[u8], r: &LwwRegister) {
    buf.push(1u8);
    put_bytes(buf, key);
    put_bytes(buf, &r.value);
    put_ts(buf, &r.timestamp);
    buf.push(if r.tombstone { 1 } else { 0 });
}

fn encode_counter(buf: &mut Vec<u8>, key: &[u8], c: &PnCounter) {
    buf.push(2u8);
    put_bytes(buf, key);
    for side in [&c.p, &c.n] {
        buf.extend_from_slice(&(side.len() as u32).to_le_bytes());
        for (&nid, &v) in side {
            buf.extend_from_slice(&nid.to_le_bytes());
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }
}

fn encode_set<'a>(
    buf: &mut Vec<u8>,
    key: &[u8],
    elements: impl ExactSizeIterator<Item = (&'a Bytes, &'a HashSet<HlcTimestamp>)>,
    tombstones: impl ExactSizeIterator<Item = &'a HlcTimestamp>,
) {
    buf.push(3u8);
    put_bytes(buf, key);
    buf.extend_from_slice(&(elements.len() as u32).to_le_bytes());
    for (elem, tags) in elements {
        put_bytes(buf, elem);
        buf.extend_from_slice(&(tags.len() as u32).to_le_bytes());
        for tag in tags {
            put_ts(buf, tag);
        }
    }
    buf.extend_from_slice(&(tombstones.len() as u32).to_le_bytes());
    for tag in tombstones {
        put_ts(buf, tag);
    }
}

/// Bounds-checked cursor over an untrusted payload.
struct Reader<'a> {
    data: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.data.len() < n {
            return Err("truncated CRDT payload".to_string());
        }
        let (head, rest) = self.data.split_at(n);
        self.data = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.array::<1>()?[0])
    }

    fn len(&mut self) -> Result<usize, String> {
        Ok(u32::from_le_bytes(self.array()?) as usize)
    }

    fn bytes(&mut self) -> Result<Bytes, String> {
        let n = self.len()?;
        Ok(Bytes::copy_from_slice(self.take(n)?))
    }

    fn ts(&mut self) -> Result<HlcTimestamp, String> {
        let physical_ms = u64::from_le_bytes(self.array()?);
        let logical = u32::from_le_bytes(self.array()?);
        let node_id = u16::from_le_bytes(self.array()?);
        Ok(HlcTimestamp::new(physical_ms, logical, node_id))
    }

    fn counter_side(&mut self) -> Result<HashMap<u16, i64>, String> {
        let mut side = HashMap::new();
        for _ in 0..self.len()? {
            let nid = u16::from_le_bytes(self.array()?);
            let v = i64::from_le_bytes(self.array()?);
            if v < 0 {
                return Err("negative CRDT counter component".to_string());
            }
            let e = side.entry(nid).or_insert(0);
            *e = v.max(*e);
        }
        Ok(side)
    }
}

/// Calls `f` with the key of each entry of a sync payload, as a slice of
/// `data`, without decoding the entries. Stops at the first malformed entry
/// (merging such a payload fails anyway).
pub fn for_each_payload_key<'a>(data: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    const TS_LEN: usize = 14;
    let mut r = Reader { data };
    let mut next_key = || -> Result<&'a [u8], String> {
        let item_type = r.u8()?;
        let n = r.len()?;
        let key = r.take(n)?;
        match item_type {
            1 => {
                let n = r.len()?;
                r.take(n)?;
                r.take(TS_LEN + 1)?;
            }
            2 => {
                for _ in 0..2 {
                    let n = r.len()?;
                    r.take(n.saturating_mul(10))?;
                }
            }
            3 => {
                for _ in 0..r.len()? {
                    let n = r.len()?;
                    r.take(n)?;
                    let tags = r.len()?;
                    r.take(tags.saturating_mul(TS_LEN))?;
                }
                let n = r.len()?;
                r.take(n.saturating_mul(TS_LEN))?;
            }
            _ => return Err(format!("Unknown CRDT item type: {}", item_type)),
        }
        Ok(key)
    };
    while let Ok(key) = next_key() {
        f(key);
    }
}

/// Decodes a sync payload. Never panics: malformed input is an error.
pub fn decode_sync_payload(data: &[u8]) -> Result<Vec<CrdtEntry>, String> {
    let mut r = Reader { data };
    let mut entries = Vec::new();
    while !r.data.is_empty() {
        let item_type = r.u8()?;
        if !(1..=3).contains(&item_type) {
            return Err(format!("Unknown CRDT item type: {}", item_type));
        }
        let k = r.bytes()?;
        entries.push(match item_type {
            1 => {
                let value = r.bytes()?;
                let timestamp = r.ts()?;
                let tombstone = match r.u8()? {
                    0 => false,
                    1 => true,
                    b => return Err(format!("invalid CRDT register tombstone flag: {}", b)),
                };
                CrdtEntry::Register(
                    k,
                    LwwRegister {
                        value,
                        timestamp,
                        tombstone,
                    },
                )
            }
            2 => {
                let p = r.counter_side()?;
                let n = r.counter_side()?;
                CrdtEntry::Counter(k, PnCounter { p, n })
            }
            _ => {
                let mut elements: HashMap<Bytes, HashSet<HlcTimestamp>> = HashMap::new();
                for _ in 0..r.len()? {
                    let elem = r.bytes()?;
                    let tags = elements.entry(elem).or_default();
                    for _ in 0..r.len()? {
                        tags.insert(r.ts()?);
                    }
                }
                let mut tombstones = HashSet::new();
                for _ in 0..r.len()? {
                    tombstones.insert(r.ts()?);
                }
                CrdtEntry::Set(
                    k,
                    OrSet {
                        elements,
                        tombstones,
                    },
                )
            }
        });
    }
    Ok(entries)
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
        c1.inc(1, 10).unwrap();
        c1.dec(1, 2).unwrap();

        let mut c2 = PnCounter::new();
        c2.inc(2, 5).unwrap();
        c2.dec(2, 1).unwrap();

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

    fn assert_same_state(a: &CrdtStore, b: &CrdtStore) {
        assert_eq!(a.registers, b.registers);
        assert_eq!(a.counters, b.counters);
        assert_eq!(a.sets, b.sets);
    }

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    /// Merging the payload logged for each write, in order, rebuilds the
    /// writer's state exactly, deletions and removals included.
    #[test]
    fn test_write_payloads_replay_exactly() {
        let mut src = CrdtStore::new(1);
        let mut log: Vec<Vec<u8>> = Vec::new();
        src.set(b("r"), b("v1"));
        log.push(src.register_payload(b"r"));
        src.set(b("r"), b("v2"));
        log.push(src.register_payload(b"r"));
        src.set(b("gone"), b("x"));
        log.push(src.register_payload(b"gone"));
        assert!(src.del(&b("gone")));
        log.push(src.register_payload(b"gone"));
        src.counter_incr(b("c"), 10).unwrap();
        log.push(src.counter_payload(b"c"));
        src.counter_incr(b("c"), -3).unwrap();
        log.push(src.counter_payload(b"c"));
        for m in ["a", "b", "a"] {
            src.set_add(b("s"), b(m));
            log.push(src.set_member_payload(b"s", &b(m)));
        }
        let removed = src.set_rem(&b("s"), &b("a"));
        assert_eq!(removed.len(), 2, "both adds of 'a' are tombstoned");
        log.push(CrdtStore::set_removal_payload(b"s", &removed));
        assert!(src.set_rem(&b("s"), &b("zz")).is_empty());

        let mut dst = CrdtStore::new(2);
        for p in &log {
            dst.merge_sync_payload(p).unwrap();
        }
        assert_same_state(&src, &dst);
        assert_eq!(dst.get(&b("r")), Some(b("v2")));
        assert_eq!(dst.get(&b("gone")), None);
        assert_eq!(dst.counter_get(&b("c")), 7);
        assert_eq!(dst.set_members(&b("s")), vec![b("b")]);
        // Replaying twice changes nothing: merge is idempotent.
        for p in &log {
            dst.merge_sync_payload(p).unwrap();
        }
        assert_same_state(&src, &dst);
        // The replica's clock moved past every merged timestamp, so its own
        // next write wins.
        dst.set(b("r"), b("v3"));
        assert!(dst.registers[&b("r")].timestamp > src.registers[&b("r")].timestamp);

        let mut rebuilt = CrdtStore::new(3);
        for p in src.export_entry_payloads() {
            rebuilt.merge_sync_payload(&p).unwrap();
        }
        assert_same_state(&src, &rebuilt);
    }

    #[test]
    fn test_malformed_payloads_are_errors_not_panics() {
        let mut src = CrdtStore::new(1);
        src.set(b("r"), b("v"));
        src.set(b("d"), b("v"));
        src.del(&b("d"));
        src.counter_incr(b("c"), 5).unwrap();
        src.counter_incr(b("c"), -2).unwrap();
        src.set_add(b("s"), b("m1"));
        src.set_add(b("s"), b("m2"));
        src.set_rem(&b("s"), &b("m1"));
        let good = src.export_sync_payload();

        let keys_of = |data: &[u8]| {
            let mut keys = Vec::new();
            for_each_payload_key(data, |k| keys.push(k.to_vec()));
            keys
        };
        let all_keys: Vec<Vec<u8>> = decode_sync_payload(&good)
            .unwrap()
            .iter()
            .map(|e| e.key().to_vec())
            .collect();
        assert_eq!(keys_of(&good), all_keys);
        // Every strict prefix is truncated mid-entry or ends cleanly between
        // entries; either way nothing panics, and an error applies nothing.
        // The key walker yields exactly the complete entries.
        for len in 0..good.len() {
            let mut dst = CrdtStore::new(2);
            match dst.merge_sync_payload(&good[..len]) {
                Ok(n) => assert_eq!(keys_of(&good[..len]), all_keys[..n]),
                Err(_) => {
                    assert!(dst.registers.is_empty() && dst.counters.is_empty());
                    assert!(dst.sets.is_empty());
                    assert!(keys_of(&good[..len]).len() < all_keys.len());
                }
            }
        }
        // Flipping any byte must not panic either.
        for i in 0..good.len() {
            for flip in [0x01u8, 0x80, 0xff] {
                let mut bad = good.clone();
                bad[i] ^= flip;
                let _ = CrdtStore::new(2).merge_sync_payload(&bad);
                keys_of(&bad);
            }
        }

        let huge = u32::MAX.to_le_bytes();
        let mut cases: Vec<Vec<u8>> = vec![
            vec![9],
            vec![1],
            [&[1u8][..], &huge].concat(),
            [&[1u8, 1, 0, 0, 0, b'k'][..], &huge].concat(),
            [&[2u8, 1, 0, 0, 0, b'k'][..], &huge].concat(),
            [&[3u8, 1, 0, 0, 0, b'k'][..], &huge].concat(),
            [
                &[3u8, 1, 0, 0, 0, b'k', 1, 0, 0, 0, 1, 0, 0, 0, b'e'][..],
                &huge,
            ]
            .concat(),
        ];
        // A register with a tombstone flag other than 0/1.
        let mut reg = Vec::new();
        encode_register(
            &mut reg,
            b"k",
            &LwwRegister::new(b("v"), HlcTimestamp::new(1, 0, 1)),
        );
        *reg.last_mut().unwrap() = 7;
        cases.push(reg);
        // A counter with a negative component.
        let mut neg = PnCounter::new();
        neg.p.insert(1, -1);
        let mut ctr = Vec::new();
        encode_counter(&mut ctr, b"k", &neg);
        cases.push(ctr);
        for case in &cases {
            keys_of(case);
            let mut dst = CrdtStore::new(2);
            assert!(dst.merge_sync_payload(case).is_err(), "{case:?}");
            assert!(dst.registers.is_empty() && dst.counters.is_empty() && dst.sets.is_empty());
        }

        // A good entry followed by garbage applies nothing.
        let mut dst = CrdtStore::new(2);
        let mut mixed = src.register_payload(b"r");
        mixed.push(42);
        assert!(dst.merge_sync_payload(&mixed).is_err());
        assert!(dst.registers.is_empty());
    }

    /// Like INCRBY, an increment that would overflow is refused and leaves the
    /// counter unchanged (it used to panic, or wrap in release builds).
    #[test]
    fn test_pn_counter_refuses_overflow() {
        let mut c = PnCounter::new();
        c.inc(1, i64::MAX).unwrap();
        assert_eq!(c.inc(1, 1), Err(PN_OVERFLOW));
        assert_eq!(c.value(), i64::MAX);
        c.inc(1, i64::MIN + 1).unwrap();
        assert_eq!(c.value(), 0);
        // The value is back in range, but this node's positive side is full.
        assert_eq!(c.inc(1, 1), Err(PN_OVERFLOW));
        assert_eq!(c.dec(1, i64::MIN), Err(PN_OVERFLOW));
        // A component holds a magnitude, and 2^63 does not fit in one.
        assert_eq!(PnCounter::new().inc(1, i64::MIN), Err(PN_OVERFLOW));
        assert_eq!(c.value(), 0);

        // Merged per-node maxima may exceed i64 together; value() saturates.
        let mut a = PnCounter::new();
        a.inc(1, i64::MAX).unwrap();
        let mut b = PnCounter::new();
        b.inc(2, i64::MAX).unwrap();
        a.merge(&b);
        assert_eq!(a.value(), i64::MAX);

        // Refused store increments leave no trace, not even an empty counter.
        let mut store = CrdtStore::new(1);
        assert_eq!(
            store.counter_incr(Bytes::from_static(b"k"), i64::MIN),
            Err(PN_OVERFLOW)
        );
        assert!(store.counters.is_empty());
        store
            .counter_incr(Bytes::from_static(b"k"), i64::MAX)
            .unwrap();
        let before = store.counters.clone();
        assert_eq!(
            store.counter_incr(Bytes::from_static(b"k"), 1),
            Err(PN_OVERFLOW)
        );
        assert_eq!(store.counters, before);
    }
}
