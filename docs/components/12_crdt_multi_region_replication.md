# Component 12: Active-Active Multi-Region CRDT Engine (`src/crdt.rs`)

## 1. Architectural Purpose & Scope

`src/crdt.rs` implements Rudis's active-active multi-region replication engine. Built for geo-distributed deployments spanning continents, it eliminates cross-datacenter write latency by allowing clients to write locally to any region. Concurrent mutations converge automatically and deterministically into identical states using **Conflict-Free Replicated Data Types (CRDTs)** synchronized by **Hybrid Logical Clocks (HLC)**.

---

## 2. Key Invariants & Concurrency Constraints

1. **Local Write Latency ($< 1$ ms)**: Writes complete immediately against the local datacenter; synchronization across regions occurs asynchronously via delta streams.
2. **Deterministic Convergence**: State merge functions are mathematically **associative**, **commutative**, and **idempotent**:
   $$\text{merge}(A, B) = \text{merge}(B, A)$$
   $$\text{merge}(\text{merge}(A, B), C) = \text{merge}(A, \text{merge}(B, C))$$
   $$\text{merge}(A, A) = A$$
3. **Causal Monotonicity via HLC**: Overcomes physical clock drift across datacenters by combining physical UNIX epoch timestamps with logical event counters.
4. **Add-Wins Semantics for Sets**: Concurrent additions and removals of the same element resolve in favor of the addition (OR-Set).

---

## 3. Component Architecture & Data Structures

```
  Client (US-East)                         Client (EU-West)
         │                                        │
         ▼ (Write locally)                        ▼ (Write locally)
 [ Update HLC: 100.1 ]                    [ Update HLC: 102.1 ]
 [ Local RudisTable ]                     [ Local RudisTable ]
         │                                        │
         └───────────── Asynchronous WAN ─────────┘
                              │
                              ▼
                Conflict Resolution via HLC:
             102.1 > 100.1 ──► EU-West Wins!
             Both Datacenters Converge to Same State!
```

### Core Data Structures

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HybridLogicalClock {
    pub physical_ms: u64,
    pub logical_counter: u32,
    pub node_id: u16,
}

// 1. Last-Write-Wins Register
pub struct LwwRegister<T> {
    pub value: T,
    pub clock: HybridLogicalClock,
}

// 2. Positive-Negative Counter
pub struct PnCounter {
    pub increments: HashMap<u16, u64>, // NodeId -> Positive Counts
    pub decrements: HashMap<u16, u64>, // NodeId -> Negative Counts
}

// 3. Observed-Remove Set (Add-Wins)
pub struct OrSet<T: Hash + Eq + Clone> {
    // Element -> Set of active addition UUIDs with clocks
    pub elements: HashMap<T, HashMap<Uuid, HybridLogicalClock>>,
    // Set of tombstone UUIDs that have been removed
    pub tombstones: HashMap<Uuid, HybridLogicalClock>,
}
```

---

## 4. Execution Algorithms & Code Logic

### 4.1 Hybrid Logical Clock Updates

An HLC updates on local events and when receiving remote messages:

```rust
impl HybridLogicalClock {
    pub fn update_with_remote(&mut self, remote: &HybridLogicalClock) {
        let physical_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let max_physical = self.physical_ms.max(remote.physical_ms).max(physical_now);

        if max_physical == self.physical_ms && max_physical == remote.physical_ms {
            self.logical_counter = self.logical_counter.max(remote.logical_counter) + 1;
        } else if max_physical == self.physical_ms {
            self.logical_counter += 1;
        } else if max_physical == remote.physical_ms {
            self.logical_counter = remote.logical_counter + 1;
        } else {
            self.logical_counter = 0;
        }

        self.physical_ms = max_physical;
    }
}
```

### 4.2 LWW-Register Merge Logic

```rust
impl<T: Clone> LwwRegister<T> {
    pub fn merge(&mut self, remote: LwwRegister<T>) {
        if remote.clock > self.clock {
            self.value = remote.value;
            self.clock = remote.clock;
        }
    }
}
```

### 4.3 PN-Counter Merge Logic

Each node independently increments its own entry in `increments` and `decrements`. Merging computes the element-wise maximum across all node vectors:

```rust
impl PnCounter {
    pub fn merge(&mut self, remote: &PnCounter) {
        for (&node, &remote_val) in &remote.increments {
            let local_val = self.increments.entry(node).or_default();
            *local_val = (*local_val).max(remote_val);
        }
        for (&node, &remote_val) in &remote.decrements {
            let local_val = self.decrements.entry(node).or_default();
            *local_val = (*local_val).max(remote_val);
        }
    }

    pub fn value(&self) -> i64 {
        let pos: u64 = self.increments.values().sum();
        let neg: u64 = self.decrements.values().sum();
        pos as i64 - neg as i64
    }
}
```

### 4.4 Add-Wins Observed-Remove Set (OR-Set)

```rust
impl<T: Hash + Eq + Clone> OrSet<T> {
    pub fn add(&mut self, element: T, clock: HybridLogicalClock) -> Uuid {
        let tag = Uuid::new_v4();
        self.elements
            .entry(element)
            .or_default()
            .insert(tag, clock);
        tag
    }

    pub fn remove(&mut self, element: &T, clock: HybridLogicalClock) {
        if let Some(tags) = self.elements.remove(element) {
            for (tag, _) in tags {
                self.tombstones.insert(tag, clock);
            }
        }
    }

    pub fn merge(&mut self, remote: OrSet<T>) {
        // 1. Merge tombstones (keeping newest clock for each tag)
        for (tag, r_clock) in remote.tombstones {
            let l_clock = self.tombstones.entry(tag).or_insert(r_clock);
            if r_clock > *l_clock { *l_clock = r_clock; }
        }

        // 2. Merge elements (preserving tags not marked in tombstones)
        for (elem, r_tags) in remote.elements {
            let l_tags = self.elements.entry(elem).or_default();
            for (tag, clock) in r_tags {
                if !self.tombstones.contains_key(&tag) {
                    l_tags.entry(tag).or_insert(clock);
                }
            }
        }

        // 3. Purge any elements whose tags are now in tombstones
        self.elements.retain(|_, tags| {
            tags.retain(|tag, _| !self.tombstones.contains_key(tag));
            !tags.is_empty()
        });
    }
}
```

---

## 5. Cross-Component Interactions

- **`src/replication.rs`**: Transmits CRDT delta packets between peer regional clusters.
- **`src/table.rs`**: Stores CRDT data types directly as `RudisValue` representations.
- **`src/connection.rs`**: Exposes CRDT-specific Redis commands (`CRDT.GET`, `CRDT.SET`, `CRDT.INCR`).

---

## 6. Performance Characteristics

- **Zero Consensus Overhead**: Requires no Paxos or Raft voting rounds. Writes complete in $< 1$ ms locally.
- **Minimal Delta Footprint**: Delta synchronization sends only updated tags and values rather than entire datasets.
