# Component 06: Blocking Operations & The Reactive Event Hub (`src/block.rs`)

## 1. Architectural Purpose & Scope

`src/block.rs` implements Rudis's asynchronous waiter registration and notification engine (**`BlockHub`**). It powers Redis blocking commands—including `BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, and `BLMPOP`—allowing clients to wait for elements to appear in lists or sorted sets across shards without ever stalling or sleeping reactor threads.

---

## 2. Key Invariants & Concurrency Constraints

1. **Non-Blocking Reactor Execution**: Reactor threads must never sleep or block. When a client blocks, its socket read loop yields execution, and an asynchronous waiter future is registered.
2. **Cross-Shard Awareness**: A client connected to Shard A waiting on Key K can be satisfied by a write operation (`LPUSH`, `ZADD`) executed on Shard B.
3. **Duplicate Waiter Suppression**: When a client waits on multiple keys (e.g., `BLPOP k1 k2 0`), only the first available key satisfies the request; subsequent notifications for the other keys in the same transaction or batch are ignored.
4. **Immediate Clean-Up on Timeout or Disconnect**: When a blocked client times out or disconnects, its waiter registration is cleanly purged from `BlockHub` via RAII drop guards.

---

## 3. Component Architecture & Data Structures

```
  Client issues BLPOP k1 0 (k1 is empty)
                     │
                     ▼
             Register Waiter
  ┌────────────────────────────────────────┐
  │ Waiter                                 │
  │ ├── client_id: u64                     │
  │ ├── keys: [k1]                         │
  │ ├── target_type: List                  │
  │ └── sender: oneshot::Sender<PopResult> │
  └──────────────────┬─────────────────────┘
                     │
                     ▼
           Store in Shard BlockHub
  list_waiters: HashMap<"k1", [Waiter1, Waiter2]>
                     │
                     ▼ (Client yields; event loop processes other sockets)
                     ...
                     ▲ (Another client writes LPUSH k1 "val" on any shard)
                     │
         Shard Router Broadcasts:
     ShardMessage::NotifyList { key: "k1" }
                     │
                     ▼
           BlockHub::notify_list
     ├── Matches Waiter1 for "k1"
     ├── Pops item from k1
     └── Sends result via oneshot::Sender
                     │
                     ▼
       Waiter1 wakes up, writes RESP,
       and resumes read loop!
```

### Core Waiter Data Structures

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WaiterType {
    List,
    ZSet,
}

pub struct Waiter {
    pub client_id: u64,
    pub port: u16,
    pub keys: Vec<Bytes>,
    pub target_type: WaiterType,
    pub is_min: bool,            // For ZSet pop direction
    pub count: usize,            // For MPOP variants
    pub sender: oneshot::Sender<BlockedPopResult>,
}

pub enum BlockedPopResult {
    List { key: Bytes, element: Bytes },
    ZSet { key: Bytes, member: Bytes, score: f64 },
    MultiList { key: Bytes, elements: Vec<Bytes> },
    MultiZSet { key: Bytes, items: Vec<(Bytes, f64)> },
    Timeout,
}

pub struct BlockHub {
    pub list_waiters: HashMap<Bytes, Vec<Waiter>>,
    pub zset_waiters: HashMap<Bytes, Vec<Waiter>>,
}
```

---

## 4. Execution Algorithms & Code Logic

### 4.1 Client Blocking & Registration Flow

When `BLPOP` or `BZPOPMIN` finds all keys empty, it registers in `BlockHub`:

```rust
pub async fn handle_blocking_list_pop(
    client_id: u64,
    port: u16,
    keys: Vec<Bytes>,
    timeout: Duration,
    hub: &mut BlockHub,
) -> Option<(Bytes, Bytes)> {
    let (tx, rx) = oneshot::channel();
    let waiter = Waiter {
        client_id,
        port,
        keys: keys.clone(),
        target_type: WaiterType::List,
        is_min: true,
        count: 1,
        sender: tx,
    };

    // Register waiter for all specified keys
    for k in &keys {
        hub.list_waiters.entry(k.clone()).or_default().push(waiter.clone());
    }

    // Await notification or timeout asynchronously
    if timeout.is_zero() {
        // Block indefinitely until awakened
        match rx.await {
            Ok(BlockedPopResult::List { key, element }) => Some((key, element)),
            _ => None,
        }
    } else {
        // Block with timeout
        monoio::select! {
            res = rx => {
                match res {
                    Ok(BlockedPopResult::List { key, element }) => Some((key, element)),
                    _ => None,
                }
            }
            _ = monoio::time::sleep(timeout) => {
                None // Timed out, return null
            }
        }
    }
}
```

### 4.2 Waiter Notification Flow (`notify_list` and `notify_zset`)

When a write command (`LPUSH`, `RPUSH`, `ZADD`) mutates a key, `BlockHub` is notified immediately:

```rust
impl BlockHub {
    pub fn notify_list(
        &mut self,
        table: &mut RudisTable,
        key: &Bytes,
    ) {
        let waiters = match self.list_waiters.remove(key) {
            Some(w) => w,
            None => return,
        };

        let mut satisfied_clients = HashSet::new();

        for waiter in waiters {
            // Duplicate waiter check
            if satisfied_clients.contains(&waiter.client_id) {
                continue;
            }

            // Pop item from list
            if let Some(RudisValue::List(list)) = table.get_mut(key) {
                if let Some(elem) = list.pop_left() {
                    let res = BlockedPopResult::List {
                        key: key.clone(),
                        element: elem,
                    };
                    if waiter.sender.send(res).is_ok() {
                        satisfied_clients.insert(waiter.client_id);
                    }
                } else {
                    // List is empty again; re-register remaining waiters
                    self.list_waiters.entry(key.clone()).or_default().push(waiter);
                    break;
                }
            }
        }
    }
}
```

### 4.3 Multi-Key ZSet Pop (`notify_zset`)

For `BZPOPMIN`, `BZPOPMAX`, and `BZMPOP`, the notification logic respects pop counts and order:

```rust
impl BlockHub {
    pub fn notify_zset(
        &mut self,
        table: &mut RudisTable,
        key: &Bytes,
    ) {
        let waiters = match self.zset_waiters.remove(key) {
            Some(w) => w,
            None => return,
        };

        let mut satisfied_clients = HashSet::new();

        for waiter in waiters {
            if satisfied_clients.contains(&waiter.client_id) {
                continue;
            }

            if let Some(RudisValue::ZSet(zset)) = table.get_mut(key) {
                let popped = if waiter.is_min {
                    zset.pop_min(waiter.count)
                } else {
                    zset.pop_max(waiter.count)
                };

                if !popped.is_empty() {
                    let res = BlockedPopResult::MultiZSet {
                        key: key.clone(),
                        items: popped,
                    };
                    if waiter.sender.send(res).is_ok() {
                        satisfied_clients.insert(waiter.client_id);
                    }
                }
            }
        }
    }
}
```

---

## 5. Cross-Component Interactions

- **`src/server.rs`**: Invokes `hub.notify_list` and `hub.notify_zset` upon receiving `ShardMessage::NotifyList` from peer shards.
- **`src/connection.rs`**: Invokes `handle_blocking_list_pop` and `handle_bzpop` when parsing blocking commands.
- **`src/router.rs`**: Dispatches notification broadcasts across the shard channel mesh whenever a list or zset mutation occurs.

---

## 6. Performance Characteristics

- **Zero Polling Overhead**: Waiters consume zero CPU while sleeping. Waking up a client is an $O(1)$ channel signal.
- **High Concurrency**: Supports tens of thousands of concurrently blocked clients with minimal RAM overhead (~128 bytes per registered waiter).
