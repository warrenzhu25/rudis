# Rudis Internals: The Comprehensive Architecture & Contributor Learning Guide

Welcome to the internal engineering guide for **Rudis**. This document provides an in-depth, self-contained explanation of Rudis's architecture, subsystems, data structures, concurrency primitives, and execution flows. 

**This guide is designed so that contributors can understand every major system and its core logic without needing to read the source code directly.**

---

## Table of Contents

1. [Architectural Philosophy: Shared-Nothing & Thread-per-Core](#1-architectural-philosophy-shared-nothing--thread-per-core)
2. [End-to-End Request Lifecycle & Zero-Copy RESP Engine](#2-end-to-end-request-lifecycle--zero-copy-resp-engine)
3. [Memory Engine: `RudisTable` and Compact Encodings](#3-memory-engine-rudistable-and-compact-encodings)
4. [Blocking Operations & The Reactive Event Hub (`BlockHub`)](#4-blocking-operations--the-reactive-event-hub-blockhub)
5. [Transactions (`MULTI`/`EXEC`) & Virtual Lock-Free Sharding (VLL)](#5-transactions-multiexec--virtual-lock-free-sharding-vll)
6. [NVMe SSD Tiered Storage Engine](#6-nvme-ssd-tiered-storage-engine)
7. [Vector Search Engine: HNSW, SQ8, PQ & ADC](#7-vector-search-engine-hnsw-sq8-pq--adc)
8. [RediSearch & Hybrid Reciprocal Rank Fusion (RRF)](#8-redisearch--hybrid-reciprocal-rank-fusion-rrf)
9. [Kernel Bypass Networking: AF_XDP & Zero-Copy TCP](#9-kernel-bypass-networking-af_xdp--zero-copy-tcp)
10. [Cluster Topology, Gossip & Multi-Region CRDT Engine](#10-cluster-topology-gossip--multi-region-crdt-engine)
11. [Contributor Quick Reference: Subsystem Cheat Sheet](#11-contributor-quick-reference-subsystem-cheat-sheet)

---

## 1. Architectural Philosophy: Shared-Nothing & Thread-per-Core

### 1.1 Why Shared-Nothing?

Traditional multi-threaded in-memory databases (e.g., Memcached, KeyDB) rely on shared memory protected by fine-grained mutexes or lock-free concurrent hash maps (such as Java's `ConcurrentHashMap` or C++'s `folly::ConcurrentHashMap`). While this allows any worker thread to touch any key, it hits a severe scalability wall on modern multi-core servers (32–128 physical cores) due to:
- **Cache Line Bouncing (False Sharing)**: Cores continually invalidate each other's L1/L2 caches when updating shared memory addresses or atomic refcounts.
- **Lock Contention**: Hot keys create serial bottlenecks where threads stall waiting on synchronization primitives.
- **Cross-NUMA Memory Latency**: Accessing memory allocated on a remote socket takes $2\times$ to $3\times$ longer than local memory.

Redis avoided these issues by remaining strictly **single-threaded** for its execution engine. However, a single thread can only saturate one CPU core, leaving modern 64-core processors largely idle unless multiple separate Redis instances are deployed and managed.

**Rudis adopts the Shared-Nothing, Thread-per-Core Multi-Reactor Model** (pioneered by engines like Seastar and Dragonfly):

```
+-----------------------------------------------------------------------------------------------------+
|                                            RUDIS PROCESS                                            |
|                                                                                                     |
|  +--------------------------------+  +--------------------------------+                             |
|  |     CPU Core 0 (Pinned)        |  |     CPU Core 1 (Pinned)        |                             |
|  |  +--------------------------+  |  |  +--------------------------+  |                             |
|  |  |   Monoio io_uring Loop   |  |  |  |   Monoio io_uring Loop   |  |                             |
|  |  +--------------------------+  |  |  +--------------------------+  |                             |
|  |  | Shard 0 Table (DRAM)     |  |  |  | Shard 1 Table (DRAM)     |  |   ... Up to N Cores        |
|  |  | Shard 0 Tiering (NVMe)   |  |  |  | Shard 1 Tiering (NVMe)   |  |                             |
|  |  | Shard 0 BlockHub         |  |  |  | Shard 1 BlockHub         |  |                             |
|  |  +--------------------------+  |  |  +--------------------------+  |                             |
|  +--------------------------------+  +--------------------------------+                             |
|                  ^                                   ^                                              |
|                  |     Lock-Free Channel Mesh        |                                              |
|                  +===================================+                                              |
|                                                                                                     |
|  TCP Listen Socket with SO_REUSEPORT (Kernel balances connections directly across cores)            |
+-----------------------------------------------------------------------------------------------------+
```

### 1.2 Core Invariants
1. **Zero Locks on Storage**: Every `RudisTable` instance belongs exclusively to one thread on its assigned CPU core. There are **no `Mutex`, `RwLock`, or atomic CAS operations** on normal read/write hot paths.
2. **Core Affinity Pinning**: Each shard worker thread is pinned to its designated CPU core using `core_affinity::set_for_current`. This maximizes L1/L2 data and instruction cache hits and eliminates OS thread migration overhead.
3. **`SO_REUSEPORT` Kernel Load Balancing**: Every shard binds its own TCP listener to the same port. The Linux kernel distributes incoming client connections across all shard rings without any userspace proxy or dispatcher thread.
4. **Lock-Free Cross-Thread Communication**: When a request touches keys belonging to another shard, communication occurs through bounded, lock-free message queues (`flume` channels), maintaining asynchronous non-blocking operation.

---

## 2. End-to-End Request Lifecycle & Zero-Copy RESP Engine

### 2.1 From Socket Read to Command Parse

Rudis runs on top of `monoio`, a pure Rust asynchronous runtime driven by Linux's `io_uring`:

```
 Client TCP Stream
        │
        ▼ (monoio io_uring read)
 [ Connection Buffer: 64KB Slab ]
        │
        ▼ (resp::parse_command)
 [ Zero-Copy Command AST ] ───► Key Extraction (crc16 / xxHash)
        │
        ├──► Local Shard Key?  ───► Direct RudisTable Mutation ──► Write Buffer
        │
        └──► Remote Shard Key? ───► ShardMessage::ExecuteViaRouter ──► Remote Channel
```

#### Code Logic: The Read Loop (`src/connection.rs`)
```rust
pub async fn run_client_loop<S: AsyncReadRent + AsyncWriteRent>(
    mut stream: S,
    db: &mut RudisDb,
    router: &RouterHandle,
    client_id: u64,
) {
    let mut buf = vec![0u8; 64 * 1024]; // 64KB read slab
    let mut read_pos = 0;
    let mut out = Vec::with_capacity(16 * 1024);

    loop {
        // Asynchronously read from socket using io_uring submission/completion
        let (res, slice) = stream.read(buf.slice(read_pos..)).await;
        let n = res.unwrap();
        if n == 0 { break; } // EOF

        read_pos += n;
        let mut consumed = 0;

        // Fast-path zero-copy RESP parse
        while let Some((cmd, bytes_read)) = parse_command(&slice[consumed..read_pos]) {
            consumed += bytes_read;
            
            // Execute command locally or forward via router
            execute_command(cmd, db, router, &mut out).await;
        }

        // Flush output buffer to client socket
        if !out.is_empty() {
            let (w_res, _) = stream.write_all(out.split_off(0)).await;
            w_res.unwrap();
        }

        // Compact remaining unprocessed bytes to front of buffer
        buf.copy_within(consumed..read_pos, 0);
        read_pos -= consumed;
    }
}
```

### 2.2 Zero-Copy RESP2 & RESP3 Parser (`src/resp.rs`)

Rudis parses Redis protocol frames using reference slices (`&[u8]`) that reference the underlying socket buffer directly. Strings are instantiated as `bytes::Bytes` (atomic reference-counted byte slices) without allocating heap memory for payload copies.

#### Protocol Framing:
- **Arrays (`*<count>\r\n`)**: Read count, recursively parse elements into argument vector.
- **Bulk Strings (`$<len>\r\n<data>\r\n`)**: Read payload length, extract slice `[pos..pos+len]`.
- **Inline Commands**: Support legacy text clients (`PING\r\n`, `SET k v\r\n`).
- **RESP3 Type Support**: Nulls (`_\r\n`), Booleans (`#t\r\n`), Doubles (`,1.23\r\n`), Maps (`%<count>\r\n`), Sets (`~<count>\r\n`), and Pushes (`><count>\r\n`).

### 2.3 Shard Routing Algorithm (`src/router.rs`)

Keys are assigned to shards using a two-stage deterministic hashing strategy:
1. **Cluster Mode (Slot Hash)**: `slot = crc16(hash_tag(key)) % 16384`. Each shard manages a contiguous or assigned slice of slots.
2. **Standalone Multi-Threaded Mode**: `shard_id = (xxh3(hash_tag(key)) % num_shards)`.

```rust
// Hash tag extraction: if key contains "{...}", only the content inside braces is hashed
pub fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(s) = key.iter().position(|&b| b == b'{') {
        if let Some(e) = key[s + 1..].iter().position(|&b| b == b'}') {
            if e > 0 {
                return &key[s + 1..s + 1 + e];
            }
        }
    }
    key
}

pub fn key_to_shard(key: &[u8], num_shards: usize) -> usize {
    let tag = hash_tag(key);
    (xxhash_rust::xxh3::xxh3_64(tag) as usize) % num_shards
}
```

---

## 3. Memory Engine: `RudisTable` and Compact Encodings

### 3.1 Architecture of `RudisTable` (`src/table.rs`)

`RudisTable` is the primary associative storage engine for each shard. Unlike standard Redis (which maintains separate hash tables for keys, values, and expiration timestamps), `RudisTable` inlines metadata and timestamps directly into each dictionary entry.

```rust
pub struct RudisEntry {
    pub key: Bytes,
    pub val: RudisValue,
    pub expire_at: Option<Instant>, // Inlined expiration
}

pub enum RudisValue {
    String(Bytes),
    Hash(RudisHash),
    List(RudisList),
    Set(RudisSet),
    ZSet(RudisZSet),
    Stream(RudisStream),
    Json(RudisJson),
    Bitmap(RudisBitmap),
    Cuckoo(CuckooFilter),
    Bloom(BloomFilter),
    Cms(CountMinSketch),
    TopK(TopK),
    External(RudisExternalPtr), // Stored on NVMe SSD
}
```

### 3.2 Compact Encodings & Memory Optimization

To match and exceed Redis's memory efficiency, Rudis uses adaptive compact encodings that dynamically convert between dense continuous byte representations for small payloads and fast indexed structures for large datasets:

```
Data Type   Compact Encoding (< threshold)         Expanded Encoding (>= threshold)
──────────────────────────────────────────────────────────────────────────────────────────
Hash        Listpack (Contiguous key-val bytes)   FlatHash (SIMD Hash Table)
List        Listpack (Contiguous elements)         Quicklist (Doubly-linked Listpacks)
Set         Intset (Sorted 16/32/64-bit array)    FlatSet (SIMD Hash Set)
Sorted Set  Listpack (Contiguous member-score)    Augmented Skiplist + Hash Table
```

#### 1. Hashes: Listpack to FlatHash Promotion
- Under `hash-max-listpack-entries` (default: 512) and `hash-max-listpack-value` (default: 64 bytes), fields and values are stored in a contiguous binary buffer (`Listpack`).
- When an insertion causes entries to exceed the threshold or a value length exceeds 64 bytes, Rudis transparently unfolds the Listpack into a `FlatHash`.

#### 2. Sets: Intset Dual Encoding
- If all elements added to a Set are integers that fit in 16, 32, or 64 bits, the set is encoded as an **`Intset`**.
- An `Intset` is a sorted array of integers. Lookups, additions, and removals use $O(\log N)$ binary search.
- When a non-integer string is added, the `Intset` automatically promotes to a `FlatSet`.

#### 3. Sorted Sets (ZSets): Augmented Skiplist with Rank Calculation
A sorted set must support $O(\log N)$ score lookups (`ZRANGEBYSCORE`), rank lookups (`ZRANK`, `ZREVRANK`), and member updates (`ZADD`, `ZINCRBY`).
- **Memory Layout**: A combined `FlatHash<Bytes, f64>` (for $O(1)$ member-to-score lookup) paired with a **SkipList** (for ordered traversals).
- **Augmented Span Count**: Every skiplist forward pointer contains a `span` integer indicating how many elements at level 0 are traversed by this jump. This enables **$O(\log N)$ rank computation** without scanning intermediate nodes:

```
Level 2: [Head] ---------------- (span: 4) ---------------> [Node D]
Level 1: [Head] ------- (span: 2) -------> [Node B] - (2) -> [Node D]
Level 0: [Head] - (1) -> [Node A] - (1) -> [Node B] - (1) -> [Node C] - (1) -> [Node D]
```

#### 4. Scientific Score Precision Formatting (`%.17g`)
Redis requires exact float score representations when outputting to RESP clients. Standard Rust `Display` formats floats differently than C's standard library. Rudis uses direct `libc::snprintf` with format `%.17g`:

```rust
#[inline]
pub fn format_score(val: f64) -> String {
    if val.is_nan() { "nan".to_string() }
    else if val.is_infinite() {
        if val.is_sign_positive() { "inf".to_string() } else { "-inf".to_string() }
    } else if val == 0.0 {
        "0".to_string() // Neutralizes signed zeros (-0.0 -> "0")
    } else {
        let mut buf = [0u8; 64];
        let len = unsafe {
            libc::snprintf(
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
                b"%.17g\0".as_ptr() as *const libc::c_char,
                val,
            )
        };
        unsafe { std::str::from_utf8_unchecked(&buf[..len as usize]) }.to_string()
    }
}
```

---

## 4. Blocking Operations & The Reactive Event Hub (`BlockHub`)

### 4.1 The Challenge of Blocking in a Shared-Nothing Architecture

Commands like `BLPOP`, `BRPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, and `BLMPOP` specify keys to wait on and a timeout. If none of the keys exist, the client must sleep until either:
1. An element is pushed to one of the keys.
2. The timeout expires.

In a shared-nothing system, **Key A may reside on Shard 0 while a client waiting for Key A is connected to Shard 1**, and a writer pushing to Key A may be on Shard 2! Furthermore, **reactor threads must never block or sleep**, otherwise all other concurrent clients on that shard stall.

### 4.2 `BlockHub` Implementation (`src/block.rs`)

Every shard maintains a thread-local `BlockHub`. When a blocking command finds all target keys empty, it registers an asynchronous waiter:

```rust
pub struct Waiter {
    pub client_id: u64,
    pub port: u16,
    pub keys: Vec<Bytes>,
    pub target_type: WaiterType, // List or ZSet
    pub is_min: bool,            // For ZSet pops (Min vs Max)
    pub count: usize,
    pub sender: oneshot::Sender<BlockedResult>,
}

pub struct BlockHub {
    // Map of Key -> List of Waiters waiting on that specific key
    pub list_waiters: HashMap<Bytes, Vec<Waiter>>,
    pub zset_waiters: HashMap<Bytes, Vec<Waiter>>,
}
```

```
Client Connection (Shard 1) ──► Issues BLPOP key1 0 (key1 is empty)
        │
        ▼ (Registers Waiter with oneshot channel in Shard 1 BlockHub)
   [ Client yields control to event loop; socket remains suspended ]
        │
        ▲
(Another Client pushes to key1 on Shard 0 via RPUSH key1 "val")
        │
        ▼
   Shard 0 Router broadcast: ShardMessage::NotifyList { key: "key1" }
        │
        ▼ (Received by Shard 1)
   Shard 1 BlockHub matches Waiter for "key1"
        │
        ├── Extracts item from remote/local key
        ├── Sends item over oneshot channel to suspended client
        └── Client wakes up, writes RESP response, and resumes read loop!
```

### 4.3 Preventing Duplicate Notifications
When a client waits on multiple keys (e.g., `BLPOP key1 key2 0`), if keys belong to different shards, multiple shards could attempt to satisfy the client simultaneously. Rudis guards against this with `satisfied_clients: HashSet<u64>`:
```rust
if satisfied_clients.contains(&waiter.client_id) {
    // Client has already been satisfied by an earlier key in this batch
    continue;
}
if waiter.sender.send(result).is_ok() {
    satisfied_clients.insert(waiter.client_id);
}
```

---

## 5. Transactions (`MULTI`/`EXEC`) & Virtual Lock-Free Sharding (VLL)

### 5.1 Transaction State Model

A Redis transaction guarantees isolated, sequential execution:
- `MULTI`: Enters transaction state. Subsequent commands return `+QUEUED\r\n`.
- `DISCARD`: Aborts queued commands and clears transaction state.
- `WATCH key [key ...]`: Monitors keys for optimistic concurrency control. If another client modifies a watched key before `EXEC`, the transaction aborts and returns `*-1\r\n`.
- `EXEC`: Executes all queued commands atomically.

```rust
pub struct ClientTxState {
    pub in_tx: bool,
    pub queue: Vec<Command>,
    pub watched_keys: HashMap<Bytes, u64>, // Key -> Version at time of WATCH
    pub dirty_cas: bool,                   // Set to true if any watched key changes
}
```

### 5.2 Cross-Shard Transaction Execution

When `EXEC` is invoked, the transaction may contain commands that access keys on multiple distinct shards. Rudis uses **Virtual Lock-Free Sharding (VLL)** principles:
1. **Dependency Analysis**: Identify all target shards involved in the queued commands.
2. **Deterministic Locking Sequence**: Shard locks (or execution phases) are acquired in strictly ascending order of Shard ID (`Shard 0 -> Shard 1 -> ...`), mathematically preventing deadlocks.
3. **Execution & Batch Reply**: Commands are executed against their respective local shards. Results are assembled into a single RESP array reply and written back to the client socket atomically.

---

## 6. NVMe SSD Tiered Storage Engine

### 6.1 Purpose & Economics
Keeping 100% of large datasets in DRAM is expensive. In typical production workloads, 80% of data is cold or cooling. Rudis includes an embedded NVMe tiering engine (`src/tiering.rs`) that expands storage capacity beyond physical RAM limits by up to $5\times$, keeping hot keys in DRAM and offloading cold values to NVMe SSDs.

```
+-------------------------------------------------------------------------------+
|                                DRAM (HOT TIER)                                |
|                                                                               |
|  RudisTable                                                                   |
|   ├── "user:1001" ──► RudisValue::String("active_session_data")  [Hot]        |
|   └── "user:9999" ──► RudisValue::External(RudisExternalPtr)      [Cold]      |
+--------------------------------------┬----------------------------------------+
                                       │ Pointer dereference (16 bytes)
                                       ▼
+-------------------------------------------------------------------------------+
|                               NVMe SSD STORAGE                                |
|                                                                               |
|  File: shard-0.tier (Direct I/O O_DIRECT)                                      |
|  [Page 0: 4KB SmallBins] [Page 1: 4KB SmallBins] [Segment 1: Large Blobs]     |
|  Offset: 0x004000, Size: 128 bytes ──► Unpack: "historical_profile_archive"    |
+-------------------------------------------------------------------------------+
```

### 6.2 Three-State Value Lifecycle
```
 [ Ingest ] ──► Hot (in DRAM)
                  │ (Memory pressure exceeds high watermark)
                  ▼
                Staged / Cooled (Pushed to 4KB SmallBins buffer)
                  │ (Buffer flushed via io_uring O_DIRECT)
                  ▼
                Cold (Value freed from RAM; replaced by 16-byte RudisExternalPtr)
                  │
                  ▼ (Client reads cold key via GET)
                Promoted back to Hot!
```

### 6.3 `SmallBins` Packing & Hole Punching
- **Small Value Packing (`SmallBins`)**: Writing individual 100-byte values directly to NVMe SSD causes massive write amplification. Rudis batches small values into aligned 4 KB pages before issuing a single write.
- **Hole Punching (`fallocate`)**: When a cold key is deleted or rewritten, its disk space is freed without rewriting the entire file by invoking `libc::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, len)`. The underlying filesystem (ext4/XFS) deallocates the physical SSD blocks instantly.

---

## 7. Vector Search Engine: HNSW, SQ8, PQ & ADC

Rudis features a built-in vector search engine (`src/vector.rs`) compatible with RediSearch vector syntax for semantic search, retrieval-augmented generation (RAG), and similarity matching.

### 7.1 Hierarchical Navigable Small World (HNSW)

HNSW builds a multi-layer graph where lower layers contain dense connections and higher layers contain sparse "expressways" for fast navigation across high-dimensional vector spaces:

```
Layer 2 (Sparse):    (Node A) -------------------------> (Node Z)
                        │                                  │
Layer 1 (Medium):    (Node A) ---------> (Node M) -----> (Node Z)
                        │                   │              │
Layer 0 (All Nodes): (Node A) -> (Node B) -> (Node M) -> (Node X) -> (Node Z)
```

### 7.2 Vector Quantization

High-dimensional float vectors (e.g., 1536 dimensions for OpenAI `text-embedding-3-large`) consume 6,144 bytes per vector. Rudis implements two levels of vector quantization:

#### 1. Scalar Quantization (SQ8)
- Projects 32-bit floating point numbers into 8-bit unsigned integers:
  $$\tilde{x}_i = \left\lfloor \frac{x_i - \min}{\max - \min} \times 255 \right\rfloor$$
- **Compression**: $4\times$ memory reduction with $> 98\%$ recall retention.

#### 2. Product Quantization (PQ) & Asymmetric Distance Computation (ADC)
- Splits a $D$-dimensional vector into $M$ sub-vectors of dimension $D/M$.
- Each sub-vector is clustered into $K=256$ centroids using k-means.
- A 1536-dimensional vector is compressed into a compact array of $M$ 1-byte indices!
- **Asymmetric Distance Computation (ADC)** precomputes distance tables between query sub-vectors and the 256 centroids, turning query evaluations into blazing-fast **table lookups with zero floating point multiplications**:

$$\text{Dist}(Q, X) \approx \sum_{m=1}^M \text{LookupTable}[m][X_m]$$

---

## 8. RediSearch & Hybrid Reciprocal Rank Fusion (RRF)

### 8.1 Inverted Index & BM25 Scoring (`src/search.rs`)

Rudis implements an in-memory full-text search index matching RediSearch specifications (`FT.CREATE`, `FT.SEARCH`):
- **Tokenization & Stemming**: Normalizes text into lowercased tokens.
- **Posting Lists**: Maps tokens to document IDs with term frequencies.
- **BM25 Relevance Scoring**:
  $$\text{Score}(D, Q) = \sum_{t \in Q} \text{IDF}(t) \cdot \frac{f(t, D) \cdot (k_1 + 1)}{f(t, D) + k_1 \cdot \left(1 - b + b \cdot \frac{|D|}{\text{avgdl}}\right)}$$

### 8.2 Hybrid Search with Reciprocal Rank Fusion (RRF)

When querying text and vectors simultaneously (e.g., finding articles relevant to "distributed systems" with vector similarity to an input query), Rudis combines both rankings using **Reciprocal Rank Fusion (RRF)**:

```
 Text Query ("distributed systems")  ──► BM25 Ranking: [Doc A (1), Doc B (2), Doc C (3)]
                                                              │
 Vector KNN Query (<vector_data>)    ──► HNSW Ranking: [Doc B (1), Doc D (2), Doc A (3)]
                                                              │
                                                              ▼
               RRF Score = Sum( 1.0 / (60 + Rank_i) ) across all result lists
                                                              │
                                                              ▼
                     Merged Top Results: [Doc B, Doc A, Doc D, Doc C]
```

---

## 9. Kernel Bypass Networking: AF_XDP & Zero-Copy TCP

### 9.1 AF_XDP / eBPF Kernel Bypass (`src/xdp.rs`)

For extreme networking environments, standard Linux TCP stack socket processing incurs kernel context switches, softirq scheduling, and socket buffer (`sk_buff`) allocations.

Rudis implements an **AF_XDP (XSK)** driver:
- **eBPF Filter in Kernel Driver**: An eBPF program attaches to the network interface card (NIC) RX driver hook (`XDP_DRV`).
- **Zero-Copy UMEM Ring**: Ethernet frames are directed into memory-mapped userspace buffers (`UMEM`), completely bypassing the Linux network stack for sub-microsecond packet ingestion.
- **Hardware Rate Limiting**: Malicious requests or DDoS floods are dropped directly in the NIC driver via `XDP_DROP` before touching userspace CPU cycles.

### 9.2 Linux TCP Zero-Copy Engine (`src/zerocopy.rs`)

When transmitting large payloads (e.g., `MGET`, bulk strings, RDB snapshots, vector matrices), copying bytes from application buffers to kernel socket buffers wastes memory bandwidth.

Rudis utilizes the Linux kernel's `MSG_ZEROCOPY` interface:
1. Sockets are initialized with `setsockopt(fd, SOL_SOCKET, SO_ZEROCOPY, 1)`.
2. Outbound buffers are transmitted with `libc::send(fd, buf, len, MSG_ZEROCOPY)`.
3. The kernel maps the userspace pages directly to the network interface card via DMA without copying.
4. Completion notifications are harvested from the socket error queue (`MSG_ERRQUEUE`).

---

## 10. Cluster Topology, Gossip & Multi-Region CRDT Engine

### 10.1 Redis Cluster Bus & Gossip Protocol (`src/cluster.rs`)

Rudis implements standard Redis Cluster specifications:
- **16384 Virtual Hash Slots**: Distributed among cluster nodes.
- **Cluster Bus (Port + 10000)**: Binary gossip protocol exchanged between nodes every 100ms.
- **Slot Redirection (`MOVED` and `ASK`)**:
  - `MOVED <slot> <ip>:<port>`: Informs client the key belongs permanently to another node.
  - `ASK <slot> <ip>:<port>`: Used during active slot migrations when a key has already moved.

### 10.2 Multi-Region Active-Active CRDTs (`src/crdt.rs`)

For cross-datacenter multi-region active-active replication, standard primary-replica replication suffers from cross-WAN latency and write collisions. Rudis provides conflict-free replicated data types:

```
 Region US-East (Writes)                   Region EU-West (Writes)
         │                                          │
         ▼                                          ▼
   [ HLC: 100.1, Val: "v1" ]                  [ HLC: 102.1, Val: "v2" ]
         │                                          │
         +──────────────────── WAN ─────────────────+
                               │
                               ▼
        Conflict Resolution: HLC(EU) > HLC(US) -> Converges to "v2" everywhere!
```

- **Hybrid Logical Clock (HLC)**: Combines physical clock time with logical sequence counters to achieve monotonic causal ordering without NTP drift anomalies.
- **LWW-Register (Last-Write-Wins)**: Highest HLC wins during concurrent writes.
- **PN-Counter (Positive-Negative Counter)**: Independent increment and decrement state vectors that converge deterministically under commutative addition.
- **OR-Set (Observed-Remove Set)**: Adds are tagged with unique UUIDs. An element exists if any added tag has not been removed, guaranteeing that concurrent add/remove operations resolve predictably (Add-Wins).

---

## 11. Contributor Quick Reference: Subsystem Cheat Sheet

| Subsystem | Component Doc | Primary Source | Core Responsibility |
| :--- | :--- | :--- | :--- |
| **Server Loop** | [**01_reactor_and_server.md**](components/01_reactor_and_server.md) | `src/server.rs`, `src/main.rs` | `monoio` event loop, listener setup, core affinity pinning, signal handling. |
| **Connection** | [**02_connection_and_execution.md**](components/02_connection_and_execution.md) | `src/connection.rs` | Socket read/write, client lifecycle, command execution dispatch, pipeline squashing. |
| **RESP Engine** | [**03_resp_protocol_engine.md**](components/03_resp_protocol_engine.md) | `src/resp.rs` | Zero-copy RESP2 & RESP3 parsing, AST command variants, exact Redis error strings. |
| **Routing** | [**04_sharding_and_router_mesh.md**](components/04_sharding_and_router_mesh.md) | `src/router.rs`, `src/shard.rs` | Slot and shard hashing, remote shard message dispatching, multi-key fanout. |
| **Storage Table**| [**05_storage_engine_and_encodings.md**](components/05_storage_engine_and_encodings.md) | `src/table.rs` | `RudisTable`, `RudisValue`, Listpack/Intset/Skiplist compact encodings, inlined TTL. |
| **Event Hub** | [**06_blocking_hub_and_waiters.md**](components/06_blocking_hub_and_waiters.md) | `src/block.rs` | Asynchronous wait/notify hub for `BLPOP`, `BZPOPMIN`, `BZMPOP`, duplicate suppression. |
| **SSD Tiering** | [**07_nvme_tiered_storage.md**](components/07_nvme_tiered_storage.md) | `src/tiering.rs` | NVMe direct I/O, `SmallBins` 4KB packing, hole punching (`fallocate`), cold offload. |
| **Vector Engine**| [**08_vector_search_hnsw_quantization.md**](components/08_vector_search_hnsw_quantization.md) | `src/vector.rs` | HNSW graph, SQ8 quantization, PQ with ADC distance tables, tiered top-K reranking. |
| **Full-Text** | [**09_redisearch_fulltext_and_rrf.md**](components/09_redisearch_fulltext_and_rrf.md) | `src/search.rs` | RediSearch inverted index, BM25 ranking, Reciprocal Rank Fusion (RRF) hybrid search. |
| **Zero-Copy & Bypass** | [**10_kernel_bypass_and_zerocopy.md**](components/10_kernel_bypass_and_zerocopy.md) | `src/xdp.rs`, `src/zerocopy.rs` | AF_XDP eBPF network bypass driver, UMEM packet rings, Linux `MSG_ZEROCOPY` TCP. |
| **Cluster** | [**11_cluster_bus_and_gossip.md**](components/11_cluster_bus_and_gossip.md) | `src/cluster.rs` | 16,384 slots, Gossip bus (port + 10000), epoch consensus, `MOVED`/`ASK` redirection. |
| **CRDT Engine** | [**12_crdt_multi_region_replication.md**](components/12_crdt_multi_region_replication.md) | `src/crdt.rs` | Multi-region active-active synchronization, HLC, LWW-Register, PN-Counter, OR-Set. |
| **Lua Engine** | [**13_lua_scripting_and_functions.md**](components/13_lua_scripting_and_functions.md) | `src/scripting.rs` | Sandboxed `mlua` execution for `EVAL`, `EVALSHA`, SHA1 bytecode caching, and `FCALL`. |
| **Persistence** | [**14_persistence_and_replication.md**](components/14_persistence_and_replication.md) | `src/replication.rs`, `src/aof.rs`| Forkless in-process AOF rewrites, RDB snapshots, `PSYNC` circular backlog stream. |
| **Security & Allocator** | [**15_security_allocator_and_tls.md**](components/15_security_allocator_and_tls.md) | `src/acl.rs`, `src/allocator.rs`, `src/tls.rs`| Redis ACL v2 user permissions, Jemalloc memory telemetry, in-memory TLS & Linux kTLS. |

---
*Maintained by the Rudis Core Team. For questions or architecture reviews, consult the team issue tracker.*
