# Component 14: Persistence & Replication Engines (`src/replication.rs`, `src/aof.rs`)

## 1. Architectural Purpose & Scope

The **Persistence & Replication** subsystem guarantees durability and high availability across node failures. It comprises:
1. **Append-Only File (AOF) Engine (`src/aof.rs`)**: Logs mutating commands sequentially, featuring synchronous/asynchronous sync policies and a **forkless background AOF rewrite** mechanism.
2. **Replication Hub (`src/replication.rs`)**: Synchronizes state between primary nodes and read-replicas using the **`PSYNC`** protocol, backed by a circular in-memory replication backlog.

---

## 2. Key Invariants & Concurrency Constraints

1. **Forkless In-Process Rewrites**: Redis relies on Linux `fork()`, which can induce massive memory doubling, copy-on-write stalls, and kernel page allocation latency spikes. Rudis performs AOF rewrites **entirely in-process** using non-blocking asynchronous iterators.
2. **Deterministic Replication Stream**: Mutating commands are written to the replication stream in identical execution order across all shards, timestamped by a monotonically increasing 64-bit replication offset.
3. **Circular Replication Backlog**: Replicas that momentarily disconnect can resume with a Partial Resynchronization (`PSYNC`) as long as their acknowledged offset remains inside the backlog window.
4. **Crash-Safe Swapping**: Rewritten AOF and RDB files are flushed via `fsync` to a temporary path before being atomically renamed to the primary target file (`renameat2`).

---

## 3. Component Architecture & Data Structures

```
+─────────────────────────────────────────────────────────────────────────────+
|                         AOF PERSISTENCE ENGINE (aof.rs)                     |
|                                                                             |
|      Write Command (SET k v)                                                |
|             │                                                               |
|             ├──► Shard RudisTable (Instant In-Memory Update)                |
|             │                                                               |
|             ▼                                                               |
|      AOF Buffer (RESP Bytes) ──► fsync according to appendfsync policy      |
|             │                    (always | everysec | no)                   |
|             ▼                                                               |
|      [ appendonly.aof File ]                                                |
+─────────────────────────────────────────────────────────────────────────────+
                                       │
                                       ▼ (Replication Stream)
+─────────────────────────────────────────────────────────────────────────────+
|                          REPLICATION HUB (replication.rs)                   |
|                                                                             |
|      Replication Backlog: Circular Ring Buffer (e.g. 64MB)                  |
|      [ ... | Offset: 100400 | Offset: 100450 | Offset: 100500 | ... ]       |
|             │                                                               |
|             ├──► Replica A (In Sync: streaming live delta commands)         |
|             │                                                               |
|             └──► Replica B (Reconnected: PSYNC <repl_id> 100400             |
|                                └──► Partial Resync: sends delta!)           |
+─────────────────────────────────────────────────────────────────────────────+
```

### Core Replication Structures

```rust
pub struct ReplicationHub {
    pub repl_id: String,           // 40-char hex identifier
    pub master_offset: u64,        // Current write offset
    pub backlog: CircularBacklog,  // In-memory ring buffer
    pub replicas: Vec<ReplicaClient>,
}

pub struct CircularBacklog {
    pub buffer: Vec<u8>,
    pub capacity: usize,
    pub base_offset: u64,
    pub write_pos: usize,
}

pub struct AofManager {
    pub file: std::fs::File,
    pub sync_policy: AofSyncPolicy, // Always, EverySec, No
    pub pending_bytes: Vec<u8>,
    pub is_rewriting: bool,
    pub rewrite_buffer: Vec<u8>,
}
```

---

## 4. Execution Algorithms & Code Logic

### 4.1 Forkless AOF Rewrite Algorithm

Instead of invoking `fork()`, Rudis streams existing key-value state to a new file while buffering new live writes:

```rust
impl AofManager {
    pub async fn start_forkless_rewrite(&mut self, table: &RudisTable) -> std::io::Result<()> {
        let temp_path = PathBuf::from("appendonly.aof.tmp");
        let mut temp_file = std::fs::File::create(&temp_path)?;

        self.is_rewriting = true;
        self.rewrite_buffer.clear();

        // 1. Snapshot all live entries to temp file using minimal reconstruction commands
        for (key, entry) in &table.entries {
            let restore_cmd = entry_to_resp_command(key, &entry.val, entry.expire_at);
            temp_file.write_all(&restore_cmd)?;
        }

        // 2. Append all mutating commands that arrived during the iteration
        temp_file.write_all(&self.rewrite_buffer)?;
        temp_file.sync_all()?;

        // 3. Atomically replace active file
        std::fs::rename(temp_path, "appendonly.aof")?;
        self.is_rewriting = false;
        self.rewrite_buffer.clear();

        Ok(())
    }
}
```

### 4.2 Partial Resynchronization (`PSYNC`)

When a replica reconnects, it sends `PSYNC <repl_id> <offset>`:

```rust
impl ReplicationHub {
    pub fn handle_psync(
        &mut self,
        req_repl_id: &str,
        req_offset: u64,
        out: &mut Vec<u8>,
    ) -> PsyncResult {
        // 1. Validate replication ID
        if req_repl_id == self.repl_id && self.backlog.contains_offset(req_offset) {
            // Partial Resync: Send +CONTINUE and stream missing backlog range
            out.extend_from_slice(format!("+CONTINUE {}\r\n", self.repl_id).as_bytes());
            let delta = self.backlog.read_range(req_offset, self.master_offset);
            out.extend_from_slice(&delta);
            PsyncResult::Partial
        } else {
            // Full Resync: Send +FULLRESYNC <repl_id> <offset> and transfer full RDB
            out.extend_from_slice(
                format!("+FULLRESYNC {} {}\r\n", self.repl_id, self.master_offset).as_bytes(),
            );
            PsyncResult::Full
        }
    }
}
```

---

## 5. Cross-Component Interactions

- **`src/connection.rs`**: Invokes `record_change!(cmd)` on every mutating command, feeding both the AOF write buffer and the replication backlog.
- **`src/table.rs`**: Supplies key iterators for RDB serialization and AOF background rewrites.
- **`src/server.rs`**: Triggers periodic 1-second `fsync` flushes under `appendfsync everysec`.

---

## 6. Performance Characteristics

- **Zero Fork Stalls**: Because no Linux `fork()` is called, memory usage never doubles, and p999 tail latency remains flat during background persistence rewrites.
- **Zero Space Leaking**: Backlog ring buffers operate within a fixed memory bound (default: 64 MB), overwriting oldest offsets automatically.
