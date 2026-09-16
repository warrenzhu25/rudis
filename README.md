# rudis (Redis in Rust)

A ultra-high-performance, **multi-threaded**, **shared-nothing** Redis-compatible in-memory and NVMe-tiered database implemented in Rust, built natively on Linux **`io_uring`** via **Monoio**.

---

## Architecture Overview

```
                      Client Connections
                              │
               (SO_REUSEPORT Kernel Balancing)
                 ┌────────────┴────────────┐
                 ▼                         ▼
         ┌───────────────┐         ┌───────────────┐
         │    Core 0     │         │    Core 1     │
         │ Monoio Runtime│         │ Monoio Runtime│
         │ (io_uring)    │         │ (io_uring)    │
         ├───────────────┤         ├───────────────┤
         │    Shard 0    │         │    Shard 1    │
         │ Local HashMap │         │ Local HashMap │
         └───────┬───────┘         └───────┬───────┘
                 │     Cross-Shard Mesh    │
                 └────────◄ Channels ►─────┘
```

`rudis` employs a **Thread-Per-Core (Shared-Nothing)** model inspired by modern high-throughput architectures like Dragonfly and ScyllaDB/Seastar:

1. **Thread-per-Core Pinning**: Worker threads are pinned to physical CPU cores using `core_affinity`. Each thread runs its own isolated `monoio` event loop driving an independent Linux `io_uring` instance.
2. **Ingress with `SO_REUSEPORT`**: Every worker thread binds its own TCP listener to the same port. The Linux kernel distributes incoming client connections across the worker threads with zero user-space coordination.
3. **Partitioned In-Memory Storage**: State is strictly thread-local (`ShardDb`). Local operations execute in nanoseconds against a thread-local `HashMap` with **zero mutexes, zero atomic operations, and zero cross-core cache invalidation**.
4. **CRC16 Key Routing & Cross-Shard Mesh**:
   - Keys are mapped to 16,384 cluster slots using CRC16: `slot = crc16(tag) % 16384`.
   - If a connection receives a command for a key on its local shard, it executes immediately.
   - If the key resides on another shard, it dispatches the request through a lock-free cross-core channel mesh, where Monoio utilizes an `eventfd` waker to resume the peer core's `io_uring` ring.
5. **Multi-Protocol Gateway**: Supports standard RESP2, RESP3 (`HELLO 3`), inline text commands, and a built-in **Dual-Protocol Memcached Gateway** sharing database 0 with zero configuration.

> **Contributor Learning Guides**:
> - [**Rudis Internals & Architecture Guide**](docs/rudis_internals_guide.md): Exhaustive, self-contained walkthrough of the engine, data structures, and lock-free routing.
> - [**Component Architecture Documentation**](docs/components.md): Dedicated, deep-dive specifications for all 19 individual subsystems (`RudisTable`, `BlockHub`, Tiering, Vector, Search, XDP, CRDTs, JSON, Geo, Probabilistic structures, Pub/Sub, etc.), in one file.

---

## Quick Start

### 1. Build
```bash
cargo build --release
```

### 2. Run
By default, `rudis` auto-detects CPU cores and runs worker threads on port 6379:
```bash
./target/release/rudis --port 6379 --threads 4
```

### 3. Connect with `redis-cli` (RESP2 / RESP3)
```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET user:1 alice
OK
127.0.0.1:6379> GET user:1
"alice"
```

### 4. Connect with `nc` / `telnet` (Inline Text Protocol)
```bash
$ nc 127.0.0.1 6379
SET foo bar
+OK
GET foo
$3
bar
QUIT
+OK
```

### 5. Connect with Memcached Clients (Dual-Protocol Gateway)
```bash
$ nc 127.0.0.1 6379
set session:1 0 3600 5
admin
STORED
get session:1
VALUE session:1 0 5
admin
END
stats
STAT version 1.0.0-rudis
STAT curr_connections 1
END
quit
```

---

## Subsystem & Feature Matrix

| Subsystem / Module | Status | Description |
| :--- | :---: | :--- |
| **Thread-per-Core Engine** | Complete | Shared-nothing architecture on Monoio / `io_uring` with lock-free cross-shard channel mesh. |
| **Core Redis Data Structures** | Complete | Strings, Hashes, Lists, Sets, Sorted Sets (ZSets), Bitmaps, HyperLogLog. |
| **Geospatial Engine** | Complete | 52-bit integer geohashes, Haversine spherical distance, Redis 6.2+ `GEOSEARCH`. |
| **Streams & Consumer Groups** | Complete | Append-only log with radix tree indexing, consumer groups, PEL, and non-blocking / blocking `XREAD`. |
| **Transactions & Multi-Key** | Complete | `MULTI`/`EXEC`/`DISCARD` with Very Lightweight Locking (VLL) multi-shard distributed isolation. |
| **Pub/Sub Messaging** | Complete | High-throughput channels, glob pattern subscriptions (`PSUBSCRIBE`), and introspection. |
| **Scripting & Functions** | Complete | Redis 7 Function libraries (`FUNCTION LOAD`, `FCALL`) and standard Lua scripting (`EVAL`, `EVALSHA`). |
| **ACL & Security** | Complete | Granular user permissions, category selectors (`+@all`, `-@admin`), passwords, and `AUTH`. |
| **Replication & Persistence** | Complete | Point-in-time RDB snapshots, streaming AOF with background rewrite replay, and `PSYNC` master-replica streaming. |
| **Redis Cluster & Gossip** | Complete | Dedicated cluster bus (`port + 10000`), `-MOVED` / `-ASK` routing, dynamic slot migration, and Raft-like consensus failover. |
| **Dragonfly Compatibility Suite** | Complete | `DFLYCLUSTER`, `DFLYMIGRATE`, cache key pinning (`STICK`/`UNSTICK`), `DELEX`, and Dual-Protocol Memcached Gateway. |
| **RedisJSON Document Store** | Complete | RFC 8259 document store with recursive JSONPath parsing, array slices, and in-place atomic mutations. |
| **RediSearch & Hybrid Fusion** | Complete | Multi-field schema index, Okapi BM25 scoring, inverted token index, and Reciprocal Rank Fusion (RRF). |
| **RedisBloom Probabilistic Engine** | Complete | Bloom (`BF.*`), Cuckoo (`CF.*`), Count-Min Sketch (`CMS.*`), and Top-K (`TOPK.*`) heavy-hitter trackers. |
| **HNSW Vector Search Engine** | Complete | Cosine, L2, IP metrics, AVX2 SIMD kernels, SQ8 scalar quantization, Product Quantization (PQ), and ADC. |
| **NVMe Tiered Storage** | Complete | 3-state value lifecycle (Hot/Cooled/Cold), SmallBins 4KB bin packing, Direct I/O (`O_DIRECT`), and `fallocate` hole punching. |
| **Zero-Copy Snapshots** | Complete | Linux `ioctl(FICLONE)` reflink snapshotting (<1ms point-in-time checkpointing without stopping traffic). |
| **Multi-Region CRDTs** | Complete | Leaderless active-active replication with 16-byte Hybrid Logical Clocks, LWW, OR-Set, PN-Counter, and tombstone GC. |
| **Hardware Zero-Copy I/O** | Complete | AF_XDP (XSK) kernel bypass, eBPF wire-speed packet filtering, `io_uring` fixed registered buffers, and Linux `SO_ZEROCOPY`. |
| **Hardware Kernel TLS (kTLS)** | Complete | `rustls` user-space TLS 1.2/1.3 handshake offloaded to Linux `TCP_ULP` symmetric AES-GCM kernel cipher pipelines. |
| **Jemalloc Per-Core Telemetry** | Complete | `tikv-jemallocator` integration with live memory metrics via `tikv-jemalloc-ctl` in `INFO memory`. |

---

## Complete Command Reference

### 1. Strings & Basic Keyspace
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SET` | `SET key value [EX seconds \| PX ms] [NX \| XX]` | Sets value with optional TTL expiration and conditional creation flags. |
| `GET` | `GET key` | Returns string value or nil if non-existent or expired. Transparently loads tiered cold values. |
| `PUT` | `PUT key value` | High-throughput inline alias for `SET`. |
| `MSET` | `MSET key value [key value ...]` | Atomically sets multiple key-value pairs across single or multiple shards. |
| `MGET` | `MGET key [key ...]` | Retrieves values for multiple keys across shards via parallel scatter-gather dispatch. |
| `SETNX` | `SETNX key value` | Sets key only if it does not already exist. |
| `MSETNX` | `MSETNX key value [key value ...]` | Sets multiple keys only if none of the specified keys exist. |
| `GETSET` | `GETSET key value` | Atomically sets new value and returns previous value. |
| `GETDEL` | `GETDEL key` | Atomically retrieves value and removes key from the database. |
| `APPEND` | `APPEND key value` | Appends a string value to a key, returning the resulting length. |
| `STRLEN` | `STRLEN key` | Returns byte length of string stored at key. |
| `SETRANGE` | `SETRANGE key offset value` | Overwrites part of the string stored at key starting at specified offset. |
| `GETRANGE` | `GETRANGE key start end` | Returns substring of value stored at key within 0-based signed offset bounds. |
| `INCR` / `DECR` | `INCR key` / `DECR key` | Increments or decrements 64-bit integer value by 1. |
| `INCRBY` / `DECRBY` | `INCRBY key delta` / `DECRBY key delta` | Increments or decrements 64-bit integer value by specified delta. |
| `INCRBYFLOAT` | `INCRBYFLOAT key delta` | Increments floating-point value stored at key by specified float delta. |

### 2. Hashes
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `HSET` / `HMSET` | `HSET key field value [field value ...]` | Sets one or more field-value pairs in hash. |
| `HGET` | `HGET key field` | Returns value of specified field in hash. |
| `HMGET` | `HMGET key field [field ...]` | Returns values of multiple specified fields in hash. |
| `HDEL` | `HDEL key field [field ...]` | Deletes one or more fields from hash. |
| `HEXISTS` | `HEXISTS key field` | Checks if field exists in hash (returns 1 or 0). |
| `HLEN` | `HLEN key` | Returns number of fields contained within hash. |
| `HGETALL` | `HGETALL key` | Returns all fields and values stored in hash. |
| `HKEYS` / `HVALS` | `HKEYS key` / `HVALS key` | Returns all field names or all field values in hash. |
| `HINCRBY` | `HINCRBY key field delta` | Increments integer value of hash field by specified integer. |
| `HINCRBYFLOAT` | `HINCRBYFLOAT key field delta` | Increments floating-point value of hash field by specified float. |
| `HRANDFIELD` | `HRANDFIELD key [count [WITHVALUES]]` | Returns random field(s) from hash, optionally including values. |
| `HSCAN` | `HSCAN key cursor [MATCH pat] [COUNT n]` | Iterates incrementally over hash fields using cursor-based pagination. |

### 3. Lists
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `LPUSH` / `RPUSH` | `LPUSH key val [...]` / `RPUSH key val [...]` | Inserts elements at head or tail of list. |
| `LPOP` / `RPOP` | `LPOP key [count]` / `RPOP key [count]` | Removes and returns element(s) from head or tail of list. |
| `LRANGE` | `LRANGE key start stop` | Returns elements of list within specified slice bounds. |
| `LLEN` | `LLEN key` | Returns number of elements in list. |
| `LINDEX` | `LINDEX key index` | Returns element at specified 0-based or negative index. |
| `LTRIM` | `LTRIM key start stop` | Trims list to specified range in place. |
| `LSET` | `LSET key index element` | Overwrites element at index with new value. |
| `LREM` | `LREM key count element` | Removes occurrences of element from list based on count direction. |
| `LPOS` | `LPOS key element [RANK r] [COUNT c]` | Returns index of matching element in list. |
| `LINSERT` | `LINSERT key BEFORE\|AFTER pivot val` | Inserts value immediately before or after pivot element. |
| `LMOVE` | `LMOVE src dst LEFT\|RIGHT LEFT\|RIGHT` | Atomically pops from source list and pushes to destination list. |
| `BLMOVE` | `BLMOVE src dst LEFT\|RIGHT LEFT\|RIGHT timeout` | Blocking version of `LMOVE` with floating-point timeout in seconds. |
| `BLPOP` / `BRPOP` | `BLPOP key [...] timeout` / `BRPOP key [...] timeout` | Blocking pop from head or tail of first non-empty list. |

### 4. Sets
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SADD` | `SADD key member [member ...]` | Adds one or more members to set. |
| `SREM` | `SREM key member [member ...]` | Removes one or more members from set. |
| `SMEMBERS` | `SMEMBERS key` | Returns all members of set. |
| `SISMEMBER` | `SISMEMBER key member` | Returns 1 if member exists in set, else 0. |
| `SMISMEMBER` | `SMISMEMBER key member [member ...]` | Batch checks existence of multiple members in set. |
| `SCARD` | `SCARD key` | Returns cardinality (number of elements) of set. |
| `SPOP` | `SPOP key [count]` | Removes and returns one or more random members from set. |
| `SRANDMEMBER` | `SRANDMEMBER key [count]` | Returns one or more random members without removing them. |
| `SMOVE` | `SMOVE source destination member` | Atomically moves member from source set to destination set. |
| `SSCAN` | `SSCAN key cursor [MATCH pat] [COUNT n]` | Iterates incrementally over elements of set. |
| `SINTER` / `SUNION` / `SDIFF` | `SINTER key [key ...]` / `SUNION ...` / `SDIFF ...` | Computes set intersection, union, or difference across multiple keys. |
| `SINTERSTORE` / `SUNIONSTORE` / `SDIFFSTORE` | `SINTERSTORE dst key [key ...]` | Computes set algebraic operation and stores result into destination key. |

### 5. Sorted Sets (ZSets)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `ZADD` | `ZADD key [NX\|XX] [GT\|LT] [CH] score member [...]` | Adds members with scores or updates existing scores. |
| `ZREM` | `ZREM key member [member ...]` | Removes one or more members from sorted set. |
| `ZSCORE` | `ZSCORE key member` | Returns score of member as a floating-point string. |
| `ZMSCORE` | `ZMSCORE key member [member ...]` | Returns scores for multiple members in single roundtrip. |
| `ZCARD` | `ZCARD key` | Returns cardinality of sorted set. |
| `ZRANK` / `ZREVRANK` | `ZRANK key member` / `ZREVRANK key member` | Returns 0-based rank ordered ascending or descending by score. |
| `ZCOUNT` | `ZCOUNT key min max` | Returns number of elements with scores within `[min, max]`. |
| `ZLEXCOUNT` | `ZLEXCOUNT key min max` | Returns number of elements within lexicographical interval. |
| `ZINCRBY` | `ZINCRBY key delta member` | Increments member score by specified float delta. |
| `ZRANGE` | `ZRANGE key min max [BYSCORE\|BYLEX] [REV] [LIMIT o c] [WITHSCORES]` | Flexible range queries by index, score, or lexicographical bounds. |
| `ZPOPMIN` / `ZPOPMAX` | `ZPOPMIN key [count]` / `ZPOPMAX key [count]` | Removes and returns member(s) with lowest or highest scores. |
| `ZRANDMEMBER` | `ZRANDMEMBER key [count [WITHSCORES]]` | Returns random member(s) from sorted set. |
| `ZREMRANGEBYRANK` | `ZREMRANGEBYRANK key start stop` | Removes members within 0-based rank range. |
| `ZREMRANGEBYSCORE`| `ZREMRANGEBYSCORE key min max` | Removes members with scores within `[min, max]`. |
| `ZREMRANGEBYLEX` | `ZREMRANGEBYLEX key min max` | Removes members within lexicographical range. |
| `ZSCAN` | `ZSCAN key cursor [MATCH pat] [COUNT n]` | Iterates incrementally over members and scores. |
| `ZINTER` / `ZUNION` / `ZDIFF` | `ZINTER numkeys key [...] [WEIGHTS ...] [AGGREGATE ...]` | Computes intersection, union, or difference across sorted sets. |
| `ZINTERSTORE` / `ZUNIONSTORE` / `ZDIFFSTORE` | `ZINTERSTORE dst numkeys key [...]` | Performs set algebra and stores result into destination key. |

### 6. Bitmaps & Bitfields
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SETBIT` | `SETBIT key offset value` | Sets or clears bit at offset (0 or 1), returning previous bit value. |
| `GETBIT` | `GETBIT key offset` | Returns bit value stored at offset (0 or 1). |
| `BITCOUNT` | `BITCOUNT key [start end [BYTE\|BIT]]` | Counts number of set bits (population count) in byte range. |
| `BITPOS` | `BITPOS key bit [start [end]]` | Finds first bit set to 0 or 1 in string. |
| `BITOP` | `BITOP AND\|OR\|XOR\|NOT destkey srckey [...]` | Bitwise logical operations across multiple source strings stored into destkey. |

### 7. HyperLogLog
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `PFADD` | `PFADD key element [element ...]` | Adds elements to HyperLogLog approximate cardinality register. |
| `PFCOUNT` | `PFCOUNT key [key ...]` | Returns approximated cardinality of single or merged HyperLogLogs. |
| `PFMERGE` | `PFMERGE destkey sourcekey [sourcekey ...]`| Merges multiple HyperLogLog registers into destination register. |

### 8. Geospatial (52-Bit Geohash & Haversine)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `GEOADD` | `GEOADD key [NX\|XX\|CH] lon lat member [...]` | Stores geospatial coordinates as 52-bit geohash integer scores. |
| `GEODIST` | `GEODIST key m1 m2 [m\|km\|mi\|ft]` | Computes Haversine great-circle distance between two members. |
| `GEOPOS` | `GEOPOS key member [member ...]` | Returns normalized longitude and latitude coordinates for members. |
| `GEOHASH` | `GEOHASH key member [member ...]` | Returns 11-character standard Base32 geohash strings for members. |
| `GEORADIUS` | `GEORADIUS key lon lat r u [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT n] [ASC\|DESC]` | Queries elements within spherical radius of coordinate. |
| `GEORADIUSBYMEMBER` | `GEORADIUSBYMEMBER key member r u [...]` | Radius query centered at location of existing set member. |
| `GEOSEARCH` | `GEOSEARCH key [FROMMEMBER m \| FROMLONLAT lon lat] [BYRADIUS r u \| BYBOX w h u] [...]` | Redis 6.2+ multi-criterion geospatial search by circular radius or bounding box. |

### 9. Streams & Consumer Groups
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `XADD` | `XADD key [NOMKSTREAM] [MAXLEN\|MINID threshold] ID field val [...]` | Appends new entry to stream with auto-generated (`*`) or explicit timestamp ID. |
| `XLEN` | `XLEN key` | Returns number of entries in stream. |
| `XRANGE` / `XREVRANGE` | `XRANGE key start end [COUNT n]` | Iterates through stream entries in ascending or descending ID order. |
| `XDEL` | `XDEL key id [id ...]` | Deletes specific entry IDs from stream. |
| `XTRIM` | `XTRIM key MAXLEN\|MINID threshold` | Trims stream length or drops entries older than specified ID. |
| `XREAD` | `XREAD [COUNT n] [BLOCK ms] STREAMS key [...] id [...]` | Reads newer entries from one or more streams with optional async blocking. |
| `XGROUP CREATE` | `XGROUP CREATE key group id [MKSTREAM]` | Creates consumer group anchored at stream ID or `$` (stream tail). |
| `XGROUP DESTROY` | `XGROUP DESTROY key group` | Destroys consumer group and drops associated Pending Entries List (PEL). |
| `XGROUP CREATECONSUMER` | `XGROUP CREATECONSUMER key group consumer` | Explicitly registers named consumer within consumer group. |
| `XGROUP DELCONSUMER` | `XGROUP DELCONSUMER key group consumer` | Removes named consumer and reclaims consumer pending state. |
| `XREADGROUP` | `XREADGROUP GROUP grp csn [COUNT n] [BLOCK ms] [NOACK] STREAMS key id` | Reads stream entries on behalf of consumer group, updating PEL state. |
| `XACK` | `XACK key group id [id ...]` | Acknowledges successfully processed entries, removing them from group PEL. |
| `XPENDING` | `XPENDING key group [start end count [consumer]]` | Inspects unacknowledged pending messages in consumer group. |

### 10. Transactions & Multi-Key Isolation
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `MULTI` | `MULTI` | Enters transactional buffering mode for current client connection. |
| `EXEC` | `EXEC` | Atomically executes buffered commands across shards using distributed lock ordering (VLL). |
| `DISCARD` | `DISCARD` | Flushes transaction buffer and exits transactional mode. |

### 11. Pub/Sub Messaging
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | Subscribes client connection to one or more pub/sub channels. |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel ...]` | Unsubscribes client from specific channels or all channels. |
| `PSUBSCRIBE` | `PSUBSCRIBE pattern [pattern ...]` | Subscribes client to glob-style channel patterns (e.g. `news.*`). |
| `PUNSUBSCRIBE` | `PUNSUBSCRIBE [pattern ...]` | Unsubscribes client from glob-style channel patterns. |
| `PUBLISH` | `PUBLISH channel message` | Broadcasts message to all subscribers across all shards, returning receiver count. |
| `PUBSUB CHANNELS` | `PUBSUB CHANNELS [pattern]` | Lists active channels matching optional pattern. |
| `PUBSUB NUMSUB` | `PUBSUB NUMSUB [channel ...]` | Returns subscriber counts for specified channels. |
| `PUBSUB NUMPAT` | `PUBSUB NUMPAT` | Returns total number of active pattern subscriptions across the node. |

### 12. Scripting & Redis 7 Functions
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `EVAL` | `EVAL script numkeys [key ...] [arg ...]` | Executes Lua script synchronously with keys mapped to local or mesh dispatchers. |
| `EVALSHA` | `EVALSHA sha1 numkeys [key ...] [arg ...]` | Executes pre-cached script by its SHA1 hexadecimal digest. |
| `SCRIPT LOAD` | `SCRIPT LOAD script` | Compiles Lua script into cache without execution, returning its SHA1 digest. |
| `SCRIPT EXISTS` | `SCRIPT EXISTS sha1 [sha1 ...]` | Queries existence of script digests in the execution cache. |
| `SCRIPT FLUSH` | `SCRIPT FLUSH` | Flushes script cache. |
| `FUNCTION LOAD` | `FUNCTION LOAD [REPLACE] #!lua name=lib ...` | Compiles and registers persistent Redis 7 function library routines. |
| `FCALL` | `FCALL function numkeys [key ...] [arg ...]` | Invokes registered function routine. |
| `FUNCTION LIST` | `FUNCTION LIST` | Inspects loaded library names, descriptions, and exported routines. |
| `FUNCTION DELETE` | `FUNCTION DELETE lib` | Purges registered function library from memory. |

### 13. Access Control Lists (ACL) & Security
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `AUTH` | `AUTH [username] password` | Authenticates connection against configured ACL users. |
| `ACL LIST` | `ACL LIST` | Dumps all configured users and their active permission rules. |
| `ACL USERS` | `ACL USERS` | Returns list of all defined usernames. |
| `ACL WHOAMI` | `ACL WHOAMI` | Returns username of current connection. |
| `ACL CAT` | `ACL CAT` | Lists supported command categories (`read`, `write`, `admin`, `fast`, `slow`, etc.). |
| `ACL GETUSER` | `ACL GETUSER username` | Returns granular map of rules, allowed commands, passwords, and flags for user. |
| `ACL SETUSER` | `ACL SETUSER username [rules ...]` | Creates or updates user rules (`on`, `off`, `>password`, `+@all`, `+get`, etc.). |
| `ACL DELUSER` | `ACL DELUSER username [username ...]` | Deletes user accounts. |

### 14. Server, Keyspace & Client Management
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `PING` | `PING [message]` | Tests connection liveness, returning `PONG` or echoed message. |
| `ECHO` | `ECHO message` | Echoes back supplied string argument. |
| `TIME` | `TIME` | Returns server time as two-element array: `[unix_timestamp_seconds, microseconds]`. |
| `RESET` | `RESET` | Resets client connection state (clears auth, multi, and tracking). |
| `DBSIZE` | `DBSIZE` | Returns total number of active keys across all shards. |
| `FLUSHDB` / `FLUSHALL` | `FLUSHDB` / `FLUSHALL` | Purges keys across all shards. |
| `KEYS` | `KEYS pattern` | Returns all keys matching glob pattern across all shards. |
| `SCAN` | `SCAN cursor [MATCH pat] [COUNT n]` | Cursor-based keyspace pagination across shards. |
| `RANDOMKEY` | `RANDOMKEY` | Returns random key from keyspace or nil if empty. |
| `TYPE` | `TYPE key` | Returns string representation of key type (`string`, `list`, `set`, `zset`, `hash`, `stream`, etc.). |
| `EXPIRE` / `PEXPIRE` | `EXPIRE key sec` / `PEXPIRE key ms` | Sets TTL expiration in seconds or milliseconds. |
| `EXPIREAT` / `PEXPIREAT` | `EXPIREAT key timestamp` | Sets absolute UNIX expiration timestamp in seconds or milliseconds. |
| `EXPIRETIME` / `PEXPIRETIME` | `EXPIRETIME key` | Returns expiration UNIX timestamp in seconds or milliseconds. |
| `PERSIST` | `PERSIST key` | Clears expiration timer, making key persistent. |
| `TTL` / `PTTL` | `TTL key` / `PTTL key` | Returns remaining time-to-live in seconds or milliseconds (-2 if missing, -1 if no TTL). |
| `TOUCH` | `TOUCH key [key ...]` | Updates last-access timestamp for key(s) to influence LRU eviction. |
| `RENAME` / `RENAMENX` | `RENAME key newkey` / `RENAMENX ...` | Renames key, optionally failing if target exists (`NX`). |
| `DUMP` / `RESTORE` | `DUMP key` / `RESTORE key ttl serialized` | Exports and imports binary serialized key representations. |
| `INFO` | `INFO [section]` | Returns server status, memory stats, tiering info, replication offset, and shard health. |
| `COMMAND` / `COMMAND DOCS` | `COMMAND` / `COMMAND DOCS` | Returns Redis command metadata for client driver introspection. |
| `CONFIG GET` / `CONFIG SET` | `CONFIG GET param` / `CONFIG SET param val` | Inspects and mutates server configuration parameters at runtime. |
| `HELLO` | `HELLO [2\|3] [AUTH user pass] [SETNAME name]` | Protocol handshake negotiating RESP2 or RESP3 mode and client naming. |
| `CLIENT LIST` | `CLIENT LIST` | Returns detailed list of active client connections, file descriptors, and idle times. |
| `CLIENT ID` | `CLIENT ID` | Returns unique 64-bit client connection ID. |
| `CLIENT SETNAME` / `GETNAME` | `CLIENT SETNAME name` / `CLIENT GETNAME` | Sets or retrieves connection handle name. |
| `CLIENT TRACKING` | `CLIENT TRACKING on\|off [BCAST] [PREFIX pre]` | Enables RESP3 client-side caching invalidation push notifications. |
| `CLIENT CACHING` | `CLIENT CACHING yes\|no` | Opts in or out of tracking for next executed command. |

### 15. Persistence (RDB & AOF) & Replication
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SAVE` | `SAVE` | Synchronously writes point-in-time RDB snapshot to disk. |
| `BGSAVE` | `BGSAVE` | Asynchronously dispatches RDB serialization without blocking client traffic. |
| `LASTSAVE` | `LASTSAVE` | Returns UNIX timestamp of most recent successful snapshot. |
| `REPLICAOF` / `SLAVEOF` | `REPLICAOF host port` / `REPLICAOF NO ONE` | Sets master replication target or promotes node to master. |
| `PSYNC` | `PSYNC replid offset` | Initiates partial or full resynchronization stream. |
| `REPLCONF` | `REPLCONF [listening-port port] [ack offset]` | Replication configuration negotiation and periodic ACK heartbeat. |
| `ROLE` | `ROLE` | Reports replication role (`master` or `slave`), offset, and connected replicas. |

### 16. Redis Cluster & Gossip Protocol
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `CLUSTER SLOTS` | `CLUSTER SLOTS` | Returns array of slot ranges mapped to master and replica endpoint addresses. |
| `CLUSTER SHARDS` | `CLUSTER SHARDS` | Redis 7 specification reporting nested slot ranges and detailed node attributes. |
| `CLUSTER LINKS` | `CLUSTER LINKS` | Telemetry on active cluster bus peer connections, direction, and buffer allocations. |
| `CLUSTER NODES` | `CLUSTER NODES` | Returns standard space-delimited cluster topology string. |
| `CLUSTER INFO` | `CLUSTER INFO` | Returns cluster state, assigned slots, current epoch, and peer stats. |
| `CLUSTER MEET` | `CLUSTER MEET ip port` | Connects to peer on cluster bus (`port + 10000`) and joins gossip network. |
| `CLUSTER MYID` | `CLUSTER MYID` | Returns 40-character hexadecimal node identifier. |
| `CLUSTER COUNTKEYSINSLOT` | `CLUSTER COUNTKEYSINSLOT slot` | Returns number of keys currently assigned to hash slot. |
| `CLUSTER GETKEYSINSLOT` | `CLUSTER GETKEYSINSLOT slot count` | Returns sample of keys belonging to hash slot. |
| `CLUSTER SETSLOT` | `CLUSTER SETSLOT slot IMPORTING\|MIGRATING\|NODE\|STABLE` | Sets slot migration and ownership state. |
| `CLUSTER ADDSLOTS` / `DELSLOTS` | `CLUSTER ADDSLOTS slot [...]` | Dynamically assigns or strips discrete slot IDs. |
| `CLUSTER ADDSLOTSRANGE` / `DELSLOTSRANGE` | `CLUSTER ADDSLOTSRANGE start end [...]` | Bulk assigns or strips contiguous slot ranges. |
| `CLUSTER FAILOVER` | `CLUSTER FAILOVER [FORCE]` | Initiates manual coordinated or forced failover election. |
| `CLUSTER RESET` | `CLUSTER RESET [HARD\|SOFT]` | Resets cluster state, clearing slots and epochs. |
| `CLUSTER FORGET` | `CLUSTER FORGET node_id` | Removes node from gossip peer tables. |
| `CLUSTER REPLICATE` | `CLUSTER REPLICATE master_id` | Configures node as replica of specified master. |
| `CLUSTER SAVECONFIG` | `CLUSTER SAVECONFIG` | Forces persistence of cluster topology state to disk. |
| `ASKING` | `ASKING` | Flags client connection to accept next request targeting an `-ASK` slot. |
| `MIGRATE` | `MIGRATE host port key\|"" dest_db timeout [COPY] [REPLACE] [KEYS ...]` | Transfers key(s) to destination node. |

### 17. Dragonfly Compatibility Suite & Memcached Gateway
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `DFLYCLUSTER MYID` | `DFLYCLUSTER MYID` | Returns Dragonfly unique cluster node identifier. |
| `DFLYCLUSTER CONFIG` | `DFLYCLUSTER CONFIG json_string` | Atomically configures slot ownership and node roles using JSON manifest. |
| `DFLYCLUSTER GETSLOTINFO` | `DFLYCLUSTER GETSLOTINFO SLOTS s1 [s2 ...]` | Granular metadata, key count, and memory footprint per slot. |
| `DFLYCLUSTER FLUSHSLOTS` | `DFLYCLUSTER FLUSHSLOTS s1 e1 [s2 e2 ...]` | Flushes all keys belonging to slot range(s) without wiping entire DB. |
| `DFLYCLUSTER SLOT-MIGRATION-STATUS` | `DFLYCLUSTER SLOT-MIGRATION-STATUS` | Live telemetry on Dragonfly slot migration flows and transfer rates. |
| `DFLYMIGRATE INIT` | `DFLYMIGRATE INIT source_id shards [slots...]` | Initializes multi-shard Dragonfly migration session. |
| `DFLYMIGRATE FLOW` | `DFLYMIGRATE FLOW source_id flow_id` | Streams slot migration flow chunks. |
| `DFLYMIGRATE ACK` | `DFLYMIGRATE ACK flow_id` | Acknowledges receipt of migration flow chunks. |
| `STICK` | `STICK key [key ...]` | Pins key(s) in DRAM to prevent LRU eviction or NVMe tiering under memory pressure. |
| `UNSTICK` | `UNSTICK key [key ...]` | Removes cache pinning flag, allowing normal tiering and eviction. |
| `STICKY` | `STICKY key` | Returns 1 if key is pinned in memory, else 0. |
| `DELEX` | `DELEX key [IFEQ val \| IFNE val \| IFGT val \| IFLT val]` | Atomic conditional deletion based on value equality or numerical comparison. |
| **Memcached Gateway** | `set`, `add`, `replace`, `get`, `delete`, `incr`, `decr`, `stats`, `version`, `quit` | Seamless text-based Memcached protocol sharing database 0 with standard Redis clients. |

### 18. RedisJSON Document Store (RFC 8259)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `JSON.SET` | `JSON.SET key path json [NX\|XX]` | Stores JSON document or updates sub-tree with JSONPath selectors. |
| `JSON.GET` | `JSON.GET key [path ...]` | Serializes document or sub-paths into JSON strings. |
| `JSON.DEL` | `JSON.DEL key [path]` | Deletes entire JSON document or removes selected sub-path in place. |
| `JSON.TYPE` | `JSON.TYPE key [path]` | Returns data type (`object`, `array`, `string`, `integer`, `number`, `boolean`, `null`). |
| `JSON.NUMINCRBY` | `JSON.NUMINCRBY key path delta` | Increments numeric value at path in place without re-serialization. |
| `JSON.NUMMULTBY` | `JSON.NUMMULTBY key path factor` | Multiplies numeric value at path by specified factor. |
| `JSON.STRAPPEND` | `JSON.STRAPPEND key [path] json_string` | Appends string to existing string value at path. |
| `JSON.STRLEN` | `JSON.STRLEN key [path]` | Returns character length of string located at path. |
| `JSON.ARRAPPEND` | `JSON.ARRAPPEND key path val [val ...]` | Appends values to array container located at path. |
| `JSON.ARRLEN` | `JSON.ARRLEN key [path]` | Returns length of array located at path. |
| `JSON.ARRPOP` | `JSON.ARRPOP key [path [index]]` | Pops and returns element from array at index (default -1). |
| `JSON.OBJKEYS` | `JSON.OBJKEYS key [path]` | Returns list of keys in JSON object located at path. |
| `JSON.OBJLEN` | `JSON.OBJLEN key [path]` | Returns number of key-value attributes in JSON object. |
| `JSON.TOGGLE` | `JSON.TOGGLE key path` | Toggles boolean value located at path (`true` $\leftrightarrow$ `false`). |
| `JSON.CLEAR` | `JSON.CLEAR key [path]` | Clears container elements (empties array or object). |
| `JSON.MGET` | `JSON.MGET key [key ...] path` | Scatter-gather query retrieving JSON sub-paths across multiple keys. |

### 19. RediSearch & Hybrid Fusion (BM25 + Vector RRF)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `FT.CREATE` | `FT.CREATE idx ON HASH\|JSON PREFIX 1 p SCHEMA field TYPE [...]` | Creates secondary index over Hashes or JSON docs (`TEXT`, `NUMERIC`, `TAG`, `VECTOR`). |
| `FT.SEARCH` | `FT.SEARCH idx query [LIMIT o c] [RETURN n f...] [SORTBY f] [PARAMS ...]` | Full-text search with BM25 ranking, numeric filters, and tag matching. |
| `FT.INFO` | `FT.INFO idx` | Inspects index statistics, field schemas, document count, and memory consumption. |
| `FT.DROPINDEX` | `FT.DROPINDEX idx [DD]` | Drops index, optionally deleting underlying document keys (`DD`). |
| `FT.EXPLAIN` | `FT.EXPLAIN idx query` | Returns parsed query execution plan and filter syntax tree. |
| `FT.ADD` | `FT.ADD idx doc_id score FIELDS f v [...]` | Manually indexes document fields into search index. |

### 20. RedisBloom Probabilistic Engine
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `BF.RESERVE` | `BF.RESERVE key error_rate capacity` | Initializes Bloom filter with optimal bit array sizing and hash counts. |
| `BF.ADD` / `BF.MADD` | `BF.ADD key item` / `BF.MADD key item [...]` | Inserts one or more items into Bloom filter. |
| `BF.EXISTS` / `BF.MEXISTS` | `BF.EXISTS key item` / `BF.MEXISTS ...` | Queries membership of item(s) in Bloom filter. |
| `BF.INFO` | `BF.INFO key` | Inspects Bloom filter capacity, size, filter count, and items added. |
| `CF.RESERVE` | `CF.RESERVE key capacity` | Initializes Cuckoo filter with 4-slot bucket tables and 16-bit fingerprints. |
| `CF.ADD` / `CF.ADDNX` | `CF.ADD key item` / `CF.ADDNX key item` | Adds item to Cuckoo filter, optionally ensuring uniqueness (`ADDNX`). |
| `CF.EXISTS` | `CF.EXISTS key item` | Queries presence of item in Cuckoo filter. |
| `CF.DEL` | `CF.DEL key item` | Deletes item fingerprint from Cuckoo filter bucket. |
| `CF.INFO` | `CF.INFO key` | Inspects Cuckoo filter bucket count, filters, and items stored. |
| `CMS.INITBYDIM` | `CMS.INITBYDIM key width depth` | Initializes Count-Min Sketch with explicit width and depth. |
| `CMS.INITBYPROB` | `CMS.INITBYPROB key error probability` | Initializes Count-Min Sketch derived from error tolerance and confidence. |
| `CMS.INCRBY` | `CMS.INCRBY key item count [item count ...]`| Increments frequency counters for items in sketch. |
| `CMS.QUERY` | `CMS.QUERY key item [item ...]` | Estimates point frequency for items using minimum across hash rows. |
| `CMS.INFO` | `CMS.INFO key` | Returns Count-Min Sketch width, depth, and total count. |
| `TOPK.RESERVE` | `TOPK.RESERVE key topk` | Initializes Space-Saving Top-K streaming heavy-hitter tracker. |
| `TOPK.ADD` | `TOPK.ADD key item [item ...]` | Increments frequency of item(s) and adjusts dynamic top-$k$ ranks. |
| `TOPK.QUERY` | `TOPK.QUERY key item [item ...]` | Checks if item(s) are currently in top-$k$ frequent element list. |
| `TOPK.LIST` | `TOPK.LIST key` | Returns ordered list of current top-$k$ heavy hitters. |
| `TOPK.INFO` | `TOPK.INFO key` | Inspects Top-K tracking capacity and decay parameters. |

### 21. Vector Search Engine (HNSW, SQ8, PQ & ADC)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `VADD` | `VADD idx key d0 d1 ... [METRIC cos\|l2\|ip] [QUANTIZE/SQ8] [PQ] [TIERED]` | Inserts embedding into HNSW graph with optional SQ8 or Product Quantization. |
| `VQUERY` | `VQUERY idx k q0 q1 ... [RERANK]` | Approximate Nearest Neighbor (ANN) search with optional exact float reranking. |
| `VSIM` | `VSIM idx key1 key2 [METRIC ...]` | Computes similarity distance between two indexed vectors in memory. |
| `VDEL` | `VDEL idx key` | Removes vector from index and rewires neighboring graph edges. |
| `VINFO` | `VINFO idx` | Returns index dimension, metric, element count, and layer distribution. |

### 22. NVMe Tiered Storage & Zero-Copy Snapshots
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `TIER SPILL` | `TIER SPILL key` | Offloads key to NVMe disk, freeing DRAM value allocation. |
| `TIER LOAD` | `TIER LOAD key` | Pre-fetches cold NVMe key back into DRAM memory. |
| `TIER COOL` | `TIER COOL key` | Transitions key to Cooled state (persisted to disk, cached in DRAM). |
| `TIER DECOMMIT` | `TIER DECOMMIT [key]` | Instantly drops DRAM copies of cooled keys without disk I/O. |
| `TIER SPILLALL` | `TIER SPILLALL` | Spills all eligible in-memory keys across shards to NVMe storage. |
| `TIER GC` | `TIER GC` | Triggers online zero-copy hole punching (`fallocate`) to reclaim freed disk bins. |
| `TIER SNAPSHOT` | `TIER SNAPSHOT dir` | Parallel `<1ms` zero-copy reflink snapshot using Linux `ioctl(FICLONE)`. |
| `TIER BACKUP` | `TIER BACKUP dir` | Alias for zero-copy point-in-time tiered backup. |
| `TIER INFO` | `TIER INFO` | Reports live disk footprint, SmallBins count, and DRAM bytes saved. |

### 23. Multi-Region Active-Active CRDTs
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `CRDT.SET` | `CRDT.SET key val` | Updates Last-Write-Wins (LWW) register tagged with 16-byte Hybrid Logical Clock. |
| `CRDT.GET` | `CRDT.GET key` | Reads register value if not deleted by an active tombstone. |
| `CRDT.DEL` | `CRDT.DEL key` | Emits deletion tombstone tagged with monotonic HLC timestamp. |
| `CRDT.INCRBY` | `CRDT.INCRBY key delta` | Atomically updates positive or negative counter in distributed PN-Counter. |
| `CRDT.SADD` | `CRDT.SADD key member` | Adds member to Observed-Remove Set (OR-Set) with unique dot tag. |
| `CRDT.SMEMBERS` | `CRDT.SMEMBERS key` | Returns set members whose additions have not been observed as removed. |
| `CRDT.SREM` | `CRDT.SREM key member` | Removes member from OR-Set by tombstining observed tags. |
| `CRDT.DUMP` | `CRDT.DUMP` | Serializes state of all CRDT structures for multi-region replication replication. |
| `CRDT.MERGE` | `CRDT.MERGE payload` | Deterministically merges remote datacenter state using causal HLC comparison. |
| `CRDT.GC` | `CRDT.GC [ttl_ms]` | Prunes expired deletion tombstones to prevent metadata growth. |

### 24. Zero-Copy Networking, AF_XDP & eBPF
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `XDP.INFO` | `XDP.INFO` | Reports active AF_XDP driver mode, NIC interface, and UMEM ring fill levels. |
| `XDP.RULE ADD` | `XDP.RULE ADD DROP\|PASS cidr` | Injects wire-speed CIDR packet filter rules evaluated at driver/NIC speed. |
| `XDP.RULE DEL` | `XDP.RULE DEL rule_id` | Deletes eBPF packet filtering rule by ID. |
| `XDP.RULE LIST` | `XDP.RULE LIST` | Lists all active wire-speed eBPF rules and drop counters. |
| `XDP.STATS` | `XDP.STATS` | Live telemetry on RX/TX packets, bytes, dropped flood packets, and rate limits. |
| `XDP.PACKET` | `XDP.PACKET hex_payload` | Diagnoses and tests packet path through eBPF filter pipeline. |

---

## Technical Deep Dives

### 1. NVMe Cold-Storage Tiering (`io_uring`)
Rudis features a thread-per-core asynchronous storage tiering engine built natively on `io_uring`:
- **Three-State Value Lifecycle**: Values transition through `Hot` (in DRAM) $\to$ `Cooled` (persisted to NVMe, cached in DRAM) $\to$ `Cold` (persisted to NVMe, pointer in DRAM).
- **Zero-I/O Decommit**: `TIER DECOMMIT` drops in-memory copies of cooled keys without performing disk writes.
- **SmallBins 4KB Page Packing**: Values $<2$ KB are packed into aligned 4KB disk bins to eliminate NVMe space amplification.
- **Direct I/O (`O_DIRECT`)**: Bypasses Linux Page Cache overhead with hardware-aligned DMA writes and reads (`RUDIS_DIRECT_IO=1`).
- **Zero-Copy Online GC / Hole-Punching**: Reclaims NVMe storage on deletion via Linux `fallocate(FALLOC_FL_PUNCH_HOLE)` (`TIER GC`).
- **Transparent Async Retrieval**: Any access (`GET`, `DUMP`, etc.) to a tiered key transparently reads from disk via `io_uring` without blocking worker event loops.

### 2. Zero-Copy Tiered Snapshots (`FICLONE` / Reflink CoW)
Rudis supports sub-millisecond, zero-copy snapshots of tiered storage on NVMe filesystems supporting copy-on-write (Btrfs, XFS reflink, ZFS, OCFS2):
- **`TIER SNAPSHOT <dir>` / `TIER BACKUP <dir>`**: Dispatches parallel snapshot commands across all thread shards.
- Uses kernel `ioctl(FICLONE)` reflink cloning with automatic fallbacks to `copy_file_range` and streaming copy.
- Atomically creates point-in-time storage checkpoints with metadata manifests in $<1$ ms without stopping traffic or locking workers.

### 3. Vector Search & Quantization Engine (HNSW, SQ8, PQ & ADC)
Rudis includes an integrated Hierarchical Navigable Small World (HNSW) vector index:
- **Metrics**: Cosine distance, Euclidean $L_2$ distance, and Inner Product (IP) with AVX2 SIMD acceleration.
- **8-Bit Scalar Quantization (SQ8)**: Compresses 32-bit floating-point embeddings by **75%** (512 bytes $\to$ 128 bytes per 128-dim vector) with asymmetric distance scoring and $O(1)$ cosine norm expansion.
- **Product Quantization (PQ) & Asymmetric Distance Computation (ADC)**: Decomposes embeddings into $M$ sub-vectors mapped to centroid codebooks (**up to 96.9% memory reduction**) with precomputed $M \times 256$ ADC lookup tables.
- **Tiered Vector Storage & Rerank**: Keeps quantized codes in memory while storing full-precision vectors on tiered NVMe storage, reranking top candidates pool for exact precision.

### 4. Modern Redis 7 / Valkey Parity & Client Tracking
- **RESP3 Protocol**: Full protocol negotiation via `HELLO 3`, native RESP3 maps (`%`), sets (`~`), and push frames (`>`).
- **Client-Side Caching (`CLIENT TRACKING`)**: Invalidation broadcasts (`CLIENT TRACKING on [BCAST] [PREFIX ...]`). Subscribed clients receive asynchronous push notifications (`>2 invalidate ...`) whenever tracked keys are updated or deleted.
- **Redis 7 Functions Engine**: Standalone, persistent Lua library routines loaded via `FUNCTION LOAD #!lua name=<lib>`, invoked with `FCALL`, queried via `FUNCTION LIST`, and purged via `FUNCTION DELETE`.

### 5. Hardware-Accelerated Linux Kernel TLS (kTLS)
Rudis supports zero-copy Transport Layer Security powered by `rustls` and Linux Kernel TLS (`kTLS`):
- **User-Space Handshake**: Completes standard TLS 1.2/1.3 handshakes in user space via `rustls`.
- **Kernel-Level Offload**: Once negotiated, symmetric cipher states (`TCP_ULP` $\to$ `tls`) offload encryption/decryption directly to the Linux kernel.
- **Zero-Copy `io_uring` Pipelines**: Ingress and egress payloads bypass user-space encryption buffers, allowing direct DMA data transfers to and from NICs with hardware AES-GCM acceleration.

### 6. Active-Active Multi-Region Replication (CRDTs & Tombstone GC)
Rudis features a conflict-free replicated data type (CRDT) engine for leaderless, multi-datacenter active-active clusters:
- **16-Byte Hybrid Logical Clocks (HLC)**: Monotonic physical time + logical counter guaranteeing causal ordering across asynchronous distributed nodes.
- **Data Types**: LWW-Register, Observed-Remove Set (OR-Set), and Positive-Negative Counter (PN-Counter).
- **Automated Tombstone TTL Garbage Collection**: Prunes deletion tombstones (`CRDT.GC [ttl_ms]`) to prevent metadata bloat without sacrificing convergence.

### 7. Hardware Zero-Copy Network I/O (`io_uring` Fixed Buffers & `SO_ZEROCOPY`)
- **Registered Fixed Buffers (`IORING_REGISTER_BUFFERS`)**: Memory pages (4KB-aligned) are pre-registered with the kernel during startup via `RegisteredBufferPool`. The kernel pins page frames directly, avoiding `get_user_pages` and page table walks during high-throughput `io_uring` reads and writes.
- **Linux `SO_ZEROCOPY` & `send_zc` (`MSG_ZEROCOPY`)**: Bypasses kernel skb socket buffer allocations by allowing network interface cards (NICs) to perform direct DMA reads from user-space memory buffers.

### 8. Dragonfly Compatibility Suite
- **Cluster Control Plane**: Full support for Dragonfly's cluster management commands (`DFLYCLUSTER MYID`, `CONFIG`, `GETSLOTINFO`, `FLUSHSLOTS`, `SLOT-MIGRATION-STATUS`) and migration orchestration (`DFLYMIGRATE INIT`, `FLOW`, `ACK`).
- **Cache Eviction Protection (`STICK`, `UNSTICK`, `STICKY`)**: Pins hot keys in DRAM, preventing them from being evicted or offloaded to NVMe tiering during memory pressure.
- **Conditional Deletion (`DELEX`)**: Atomic conditional deletions based on value equality or numerical comparisons (`IFEQ`, `IFNE`, `IFGT`, `IFLT`).
- **Dual-Protocol Memcached Gateway**: Unified server supporting the standard text-based Memcached protocol (`set`, `add`, `replace`, `get`, `delete`, `incr`, `decr`, `stats`, `version`, `quit`), sharing database 0 with standard Redis clients with zero overhead.

---

## Testing

Run unit tests and end-to-end multi-threaded integration tests:
```bash
cargo test
```

---

## Benchmarks & Performance Whitepapers

> **Master Documentation**:
> - [**Comprehensive Performance Guide & Benchmark Whitepaper**](docs/benchmarks/comprehensive_performance_guide.md): Master report covering payload sensitivity (64B-64KB), pipeline depth sensitivity (1-200), tail latency distributions ($p50$ to $p99.9$), and specialized engines.
> - [**Benchmark Index & Directory**](docs/benchmarks/README.md): Central catalog linking all benchmark evaluations, comparative analyses against Dragonfly, and reproduction scripts.

### 1. High-Concurrency Multi-Engine Throughput (16 Cores, Pipeline 50–100)

Benchmarked on **AMD EPYC 7B13 (64 vCPUs, 117 GiB RAM)** with server pinned to cores `0-15` and `memtier_benchmark` on cores `32-63` (32 client threads):

| Engine / Workload | Command | Throughput (Ops/sec) | Bandwidth (MB/s) | p50 Latency (ms) | p99 Latency (ms) | Speedup vs. Dragonfly |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Counter Primitive** | `INCR` | **4,175,571** | 157.8 MB/s | **0.66** | **1.58** | **+4.4% (1.04x)** |
| **Key-Value Read** | `GET (1KB)` | **3,698,356** | 2,364.3 MB/s | **0.60** | **1.71** | **+534.2% (6.34x)** |
| **Sorted Sets** | `ZADD` | **3,468,035** | 194.6 MB/s | **0.81** | **1.96** | 0.90x |
| **Key-Value Write** | `SET (1KB)` | **3,415,601** | 3,491.4 MB/s | **0.69** | **2.01** | **+72.4% (1.72x)** |
| **Hash Table Read** | `HGET (1KB)` | **2,892,417** | 1,354.6 MB/s | **0.93** | **3.07** | **+450.2% (5.50x)** |
| **Probabilistic Bloom** | `BF.ADD` | **2,806,122** | 178.7 MB/s | **0.51** | **1.18** | *N/A (Rudis Native)* |
| **Lists** | `LPUSH (1KB)` | **2,774,462** | 2,840.5 MB/s | **1.02** | **2.58** | **+67.3% (1.67x)** |
| **RedisJSON Read** | `JSON.GET` | **2,690,696** | 170.2 MB/s | **0.54** | **1.18** | *N/A (Rudis Native)* |
| **RedisJSON Write** | `JSON.SET` | **2,656,600** | 186.1 MB/s | **0.54** | **1.26** | *N/A (Rudis Native)* |
| **Hash Table Write** | `HSET (1KB)` | **2,636,209** | 2,722.4 MB/s | **0.99** | **2.93** | **+20.9% (1.21x)** |
| **Probabilistic Cuckoo**| `CF.EXISTS` | **2,606,122** | 346.9 MB/s | **0.52** | **1.40** | *N/A (Rudis Native)* |
| **Geospatial Distance**| `GEODIST` | **2,533,758** | 214.3 MB/s | **0.55** | **1.29** | *N/A (Rudis Native)* |
| **Count-Min Sketch** | `CMS.QUERY` | **2,492,478** | 146.9 MB/s | **0.58** | **1.25** | *N/A (Rudis Native)* |
| **Top-K Heavy Hitters**| `TOPK.ADD` | **2,551,940** | 153.1 MB/s | **0.54** | **1.33** | *N/A (Rudis Native)* |
| **Geospatial Indexing**| `GEOADD` | **2,402,303** | 297.3 MB/s | **0.57** | **1.46** | *N/A (Rudis Native)* |
| **Vector Search (HNSW)**| `VQUERY` | **1,687,465** | 139.7 MB/s | **0.74** | **2.13** | *N/A (Rudis Native)* |
| **Streams Ingestion** | `XADD` | **1,560,679** | 161.9 MB/s | **0.90** | **2.35** | *N/A (Rudis Native)* |
| **Socket Saturation** | `SET (64KB)` | 134,326 | **8,401.5 MB/s (67.2 Gbps)** | 10.18 | 34.05 | *N/A (Network Bound)* |

---

### 2. Multi-Core In-Memory Scaling & CPU Profiling (1 to 32 Cores, 1KB Payloads)

Detailed Benchmark & Profiling report: [docs/benchmarks/scaling_and_profiling_report.md](docs/benchmarks/scaling_and_profiling_report.md)

| Cores | 100% SET (1KB) | SET Bandwidth | 100% GET (1KB) | 50/50 SET/GET | p50 Latency | p99 Latency |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 867,251 ops/s | 886.29 MB/s | 1,707,230 ops/s | 448,088 ops/s | 1.48 ms | 4.25 ms |
| **2** | 1,181,227 ops/s | 1,207.27 MB/s | 2,410,932 ops/s | 578,853 ops/s | 1.04 ms | 4.32 ms |
| **4** | 1,689,026 ops/s | 1,726.42 MB/s | 3,546,696 ops/s | 757,423 ops/s | 0.71 ms | 2.81 ms |
| **8** | 2,190,518 ops/s | 2,239.09 MB/s | **4,251,151 ops/s** | 1,057,310 ops/s | **0.59 ms** | **2.24 ms** |
| **16** | **2,795,856 ops/s** | **2,857.88 MB/s** | 3,260,984 ops/s | **1,401,317 ops/s** | 0.86 ms | 2.67 ms |
| **32** | 2,534,120 ops/s | 2,590.36 MB/s | 3,278,615 ops/s | 1,341,148 ops/s | 0.83 ms | 2.64 ms |

---

### Vector Search & SQ8 Quantization (10,000 Vectors, 128 Dimensions, Cosine)

Detailed Vector Search benchmark report: [docs/benchmarks/vector_search.md](docs/benchmarks/vector_search.md)

| Index Mode | Vector Payload RAM | RAM Savings | Ingestion Rate | Search QPS | Latency p50 | Latency p99 | Recall@10 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Float32 HNSW (AVX2)** | 5.12 MB | Baseline (0%) | **3,556 vec/s** | **5,880 QPS** | **147 µs** | **338 µs** | **54.8%** |
| **SQ8 Quantized (AVX2)** | **1.28 MB** | **-75.0%** | 2,080 vec/s | 3,752 QPS | 254 µs | 427 µs | 53.0% |
| **SQ8 + Exact Rerank (AVX2)** | 1.28 MB | **-75.0%** | 2,080 vec/s | 3,901 QPS | 243 µs | 448 µs | 53.0% |

---

### NVMe Tiered Storage: Rudis vs. Dragonfly (4 Worker Cores, 1KB Payloads)

Detailed Tiered Storage benchmark report: [docs/benchmarks/tiered_storage.md](docs/benchmarks/tiered_storage.md)

Rudis was benchmarked against **Dragonfly v1.39.0** on 4 physical worker cores (`taskset -c 0-3`) with `--maxmemory 1024mb` on NVMe storage using `memtier_benchmark` (4 threads, 4 connections/thread, pipeline depth 50, 1.5M keys):

| Workload | Payload | Rudis (Ops/sec) | Dragonfly (Ops/sec) | Rudis Speedup | Winner |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET** | 1KB | **1,279,856** | 286,351 | **+347.0% (4.47x)** | **Rudis** |
| **GET** | 1KB | **670,695** | 202,615 | **+231.0% (3.31x)** | **Rudis** |
| **SET/GET 1:1** | 1KB | **543,150** | 254,241 | **+113.6% (2.14x)** | **Rudis** |

### Multi-Core NVMe Tiered Storage Scaling (4, 8, 16 Cores)

| Worker Cores | SET Throughput | GET Throughput | SET/GET 1:1 Throughput | Peak Bandwidth | Median Latency (p50) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **4 Cores** | 1,351,053 ops/s | 528,145 ops/s | 948,435 ops/s | 1.41 GB/s | **0.54 ms** |
| **8 Cores** | **2,303,685 ops/s** | **1,099,510 ops/s** | **1,750,906 ops/s** | **2.41 GB/s** | **0.61 ms** |
| **16 Cores** | 1,933,960 ops/s | 763,735 ops/s | 1,236,764 ops/s | 2.02 GB/s | **0.71 ms** |

---

### Write-Batched Optimization (1 to 32 Threads, 100% SET, 1KB Payload, Pipeline 100)

Detailed write-batching benchmark document: [docs/benchmarks/write_batching.md](docs/benchmarks/write_batching.md)

| Server Threads | Baseline Ops/sec | **Batched Ops/sec** | Baseline Bandwidth | **Batched Bandwidth** | Baseline Avg Lat | **Batched Avg Lat** | Speedup |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 60,487.24 | **816,936.81** | 63.3 MB/s | **855.1 MB/s** | 52.87 ms | **3.90 ms** | **13.5x** |
| **2** | 88,727.36 | **593,237.78** | 92.8 MB/s | **620.9 MB/s** | 36.05 ms | **5.38 ms** | **6.7x** |
| **4** | 159,224.13 | **857,550.64** | 166.6 MB/s | **897.6 MB/s** | 20.09 ms | **3.71 ms** | **5.4x** |
| **8** | 295,665.11 | **794,211.64** | 309.5 MB/s | **831.3 MB/s** | 10.82 ms | **4.01 ms** | **2.7x** |
| **16** | 297,048.94 | **705,995.32** | 310.9 MB/s | **739.0 MB/s** | 10.77 ms | **4.51 ms** | **2.4x** |
| **32** | 273,831.18 | **704,537.20** | 286.6 MB/s | **737.4 MB/s** | 11.69 ms | **4.52 ms** | **2.6x** |

### Baseline Scaling (Pre-Optimization)

Detailed baseline benchmark document: [docs/benchmarks/baseline.md](docs/benchmarks/baseline.md)

| Server Threads | Throughput (Ops/sec) | Bandwidth (MB/s) | Avg Latency (ms) | p50 (ms) | p99 (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 60,487.24 | 63.26 | 52.87 | 53.50 | 80.90 |
| **2** | 88,727.36 | 92.83 | 36.05 | 37.12 | 66.56 |
| **4** | 159,224.13 | 166.63 | 20.09 | 20.86 | 41.47 |
| **8** | 295,665.11 | 309.47 | 10.82 | 9.86 | 26.75 |
| **16** | 297,048.94 | 310.92 | 10.77 | 10.18 | 30.72 |
| **32** | 273,831.18 | 286.61 | 11.69 | 10.56 | 34.82 |

---

## Project Structure

```
rudis/
├── Cargo.toml
├── docs/
│   ├── rudis_internals_guide.md # Comprehensive internal architecture & contributor learning guide
│   ├── components.md          # Detailed documentation for all 19 core subsystems
│   └── benchmarks/
│       ├── README.md          # Benchmark index and directory catalog
│       ├── comprehensive_performance_guide.md # Master performance whitepaper
│       ├── scaling_and_profiling_report.md # 1-32 core scaling and perf report
│       ├── multi_command_comparison.md # Multi-command head-to-head vs Dragonfly
│       ├── baseline.md        # Detailed 1-32 thread baseline results
│       ├── tiered_storage.md  # NVMe tiered storage benchmark vs Dragonfly
│       ├── vector_search.md   # HNSW vector search and SQ8 quantization benchmark
│       └── write_batching.md  # High-depth write-batching benchmark
├── src/
│   ├── main.rs          # CLI argument parsing, thread pinning, mesh setup
│   ├── lib.rs           # Library root exporting modules
│   ├── acl.rs           # Access Control List (ACL) engine and user rules
│   ├── allocator.rs     # jemalloc profiling and memory statistics
│   ├── aof.rs           # Append-Only File (AOF) persistence and rewrite engine
│   ├── block.rs         # Blocking commands manager (BLPOP, BRPOP, BLMOVE, XREAD BLOCK)
│   ├── bin/
│   │   └── vector_bench.rs # Standalone vector benchmark suite
│   ├── cluster.rs       # Redis Cluster bus (port + 10000), gossip, consensus failover, Dragonfly cluster
│   ├── connection.rs    # TCP connection handler, RESP3 push, command batching, Memcached parser
│   ├── crdt.rs          # Active-Active multi-region CRDT engine (HLC, LWW, OR-Set, PN-Counter)
│   ├── geo.rs           # 52-bit geohash encoding, Haversine distance, and geospatial queries
│   ├── json.rs          # RFC 8259 RedisJSON engine with deep JSONPath navigation and mutations
│   ├── probabilistic.rs # Bloom, Cuckoo, Count-Min Sketch, and Top-K probabilistic structures
│   ├── pubsub.rs        # Pub/Sub hub, channel and pattern dispatching
│   ├── replication.rs   # Master-replica replication hub, PSYNC, and replication offset tracking
│   ├── resp.rs          # RESP2/RESP3 & inline frame parser and serializer
│   ├── router.rs        # CRC16 key partitioner and cross-core message dispatcher
│   ├── scripting.rs     # Lua scripting and Redis 7 Function engine
│   ├── search.rs        # RediSearch full-text engine, BM25 scoring, inverted index, RRF
│   ├── server.rs        # Shared server context, background tasks, and signal handling
│   ├── shard.rs         # Thread-local in-memory key-value database and message types
│   ├── table.rs         # Data structures (String, Hash, List, Set, SortedSet, Stream)
│   ├── tiering.rs       # NVMe tiered storage, io_uring Direct I/O, zero-copy snapshots
│   ├── tls.rs           # Hardware-accelerated Linux Kernel TLS (kTLS) and rustls integration
│   ├── vector.rs        # HNSW vector search engine, AVX2 SIMD acceleration, SQ8 & PQ quantization
│   ├── xdp.rs           # AF_XDP kernel bypass, eBPF wire-speed packet filter, UMEM rings
│   └── zerocopy.rs      # SO_ZEROCOPY and io_uring fixed registered buffer pool
└── tests/
    ├── test_cross_thread.rs # Validates cross-core eventfd waker with Monoio
    └── test_server_e2e.rs   # 47 comprehensive end-to-end integration tests
```
