use bytes::Bytes;
use flume::Sender;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListPopType {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientUnblockType {
    Timeout,
    Error,
    WrongType,
}

#[derive(Clone, Debug)]
pub enum BlockedListResult {
    Popped(Bytes, Vec<Bytes>),
    Unblocked(ClientUnblockType),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZSetPopType {
    Min,
    Max,
}

#[derive(Clone, Debug)]
pub enum BlockedZSetResult {
    Popped {
        key: Bytes,
        items: Vec<(Bytes, f64)>,
        is_zmpop: bool,
    },
    Unblocked(ClientUnblockType),
}

pub struct ZSetWaiter {
    pub client_id: u64,
    pub key: Bytes,
    pub pop_type: ZSetPopType,
    pub count: usize,
    pub is_zmpop: bool,
    pub sender: Sender<BlockedZSetResult>,
}

#[derive(Clone, Debug)]
pub enum WaiterOp {
    Pop {
        pop_type: ListPopType,
        count: usize,
    },
    Move {
        where_from: ListPopType,
        where_to: ListPopType,
        destination: Bytes,
    },
}

pub struct ListWaiter {
    pub client_id: u64,
    pub key: Bytes,
    pub op: WaiterOp,
    pub sender: Sender<BlockedListResult>,
}

pub struct StreamWaiter {
    pub key: Bytes,
    pub sender: Sender<()>,
}

pub struct BlockHub {
    pub port: u16,
    list_waiters: HashMap<Bytes, VecDeque<ListWaiter>>,
    zset_waiters: HashMap<Bytes, VecDeque<ZSetWaiter>>,
    stream_waiters: HashMap<Bytes, Vec<StreamWaiter>>,
    blocked_clients: HashMap<u64, Sender<BlockedListResult>>,
    blocked_zset_clients: HashMap<u64, Sender<BlockedZSetResult>>,
    paused_count: usize,
    pending_notifies: Vec<Bytes>,
}

pub static PORT_BLOCK_HUBS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static TOTAL_BLOCKED_WAITERS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[inline(always)]
pub fn has_blocked_waiters(_port: u16) -> bool {
    TOTAL_BLOCKED_WAITERS.load(std::sync::atomic::Ordering::Relaxed) > 0
}

pub fn get_block_hub_for_port(port: u16) -> Arc<Mutex<BlockHub>> {
    let mut map = PORT_BLOCK_HUBS.lock().unwrap();
    map.entry(port)
        .or_insert_with(|| Arc::new(Mutex::new(BlockHub::new(port))))
        .clone()
}

impl Default for BlockHub {
    fn default() -> Self {
        Self::new(0)
    }
}

impl BlockHub {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            list_waiters: HashMap::new(),
            zset_waiters: HashMap::new(),
            stream_waiters: HashMap::new(),
            blocked_clients: HashMap::new(),
            blocked_zset_clients: HashMap::new(),
            paused_count: 0,
            pending_notifies: Vec::new(),
        }
    }

    #[inline(always)]
    pub fn sync_atomic_waiters_count(&self) {
        let count = self.paused_count
            + self.list_waiters.values().map(|w| w.len()).sum::<usize>()
            + self.zset_waiters.values().map(|w| w.len()).sum::<usize>()
            + self.stream_waiters.values().map(|w| w.len()).sum::<usize>()
            + self.blocked_clients.len()
            + self.blocked_zset_clients.len();
        TOTAL_BLOCKED_WAITERS.store(count, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn is_paused(&self) -> bool {
        self.paused_count > 0
    }

    pub fn pause(&mut self) {
        self.paused_count += 1;
        self.sync_atomic_waiters_count();
    }

    pub fn add_pending_notify(&mut self, key: Bytes) {
        if !self.pending_notifies.iter().any(|k| k == &key) {
            self.pending_notifies.push(key);
        }
    }

    pub fn resume(&mut self) -> Vec<Bytes> {
        if self.paused_count > 0 {
            self.paused_count -= 1;
        }
        self.sync_atomic_waiters_count();
        if self.paused_count == 0 {
            std::mem::take(&mut self.pending_notifies)
        } else {
            Vec::new()
        }
    }

    pub fn clear_pending_notifies(&mut self) {
        if self.paused_count > 0 {
            self.paused_count -= 1;
        }
        self.pending_notifies.clear();
        self.sync_atomic_waiters_count();
    }

    pub fn blocked_clients_count(&self) -> usize {
        self.blocked_clients.len() + self.blocked_zset_clients.len()
    }

    pub fn register_blocked_client(&mut self, client_id: u64, sender: Sender<BlockedListResult>) {
        self.blocked_clients.insert(client_id, sender);
        self.sync_atomic_waiters_count();
    }

    pub fn register_blocked_zset_client(
        &mut self,
        client_id: u64,
        sender: Sender<BlockedZSetResult>,
    ) {
        self.blocked_zset_clients.insert(client_id, sender);
        self.sync_atomic_waiters_count();
    }

    pub fn is_blocked(&self, client_id: u64) -> bool {
        self.blocked_clients.contains_key(&client_id)
            || self.blocked_zset_clients.contains_key(&client_id)
    }

    pub fn remove_waiters_for_client(&mut self, client_id: u64) {
        for waiters in self.list_waiters.values_mut() {
            waiters.retain(|w| w.client_id != client_id);
        }
        for waiters in self.zset_waiters.values_mut() {
            waiters.retain(|w| w.client_id != client_id);
        }
        self.blocked_clients.remove(&client_id);
        self.blocked_zset_clients.remove(&client_id);
        self.sync_atomic_waiters_count();
    }

    pub fn unregister_blocked_client(&mut self, client_id: u64) {
        self.remove_waiters_for_client(client_id);
    }

    pub fn unblock_client(&mut self, client_id: u64, unblock_type: ClientUnblockType) -> bool {
        let mut unblocked = false;
        if let Some(sender) = self.blocked_clients.remove(&client_id) {
            let _ = sender.send(BlockedListResult::Unblocked(unblock_type));
            for waiters in self.list_waiters.values_mut() {
                waiters.retain(|w| w.client_id != client_id);
            }
            unblocked = true;
        }
        if let Some(sender) = self.blocked_zset_clients.remove(&client_id) {
            let _ = sender.send(BlockedZSetResult::Unblocked(unblock_type));
            for waiters in self.zset_waiters.values_mut() {
                waiters.retain(|w| w.client_id != client_id);
            }
            unblocked = true;
        }
        if unblocked {
            self.sync_atomic_waiters_count();
        }
        unblocked
    }

    pub fn register_list_waiter(
        &mut self,
        client_id: u64,
        key: Bytes,
        pop_type: ListPopType,
        count: usize,
        sender: Sender<BlockedListResult>,
    ) {
        self.list_waiters
            .entry(key.clone())
            .or_default()
            .push_back(ListWaiter {
                client_id,
                key,
                op: WaiterOp::Pop { pop_type, count },
                sender,
            });
        self.sync_atomic_waiters_count();
    }

    pub fn register_move_waiter(
        &mut self,
        client_id: u64,
        source: Bytes,
        where_from: ListPopType,
        where_to: ListPopType,
        destination: Bytes,
        sender: Sender<BlockedListResult>,
    ) {
        self.list_waiters
            .entry(source.clone())
            .or_default()
            .push_back(ListWaiter {
                client_id,
                key: source,
                op: WaiterOp::Move {
                    where_from,
                    where_to,
                    destination,
                },
                sender,
            });
        self.sync_atomic_waiters_count();
    }

    /// Called when LPUSH or RPUSH adds values to a list.
    /// If there is an active waiter, pop from table directly and deliver to the waiter.
    pub fn notify_list(&mut self, table: &mut crate::table::RudisTable, key: &Bytes) {
        crate::connection::touch_watched_key(self.port, key.as_ref());
        if table.is_key_expired(key.as_ref()) {
            return;
        }
        let mut dest_to_notify: Option<Bytes> = None;
        let mut satisfied_clients = Vec::new();
        if let Some(waiters) = self.list_waiters.get_mut(key) {
            while let Some(waiter) = waiters.pop_front() {
                if waiter.sender.is_disconnected() {
                    continue;
                }
                match waiter.op {
                    WaiterOp::Pop { pop_type, count } => {
                        let popped = match pop_type {
                            ListPopType::Left => table.lpop(key.as_ref(), count).ok(),
                            ListPopType::Right => table.rpop(key.as_ref(), count).ok(),
                        };
                        if let Some(vals) = popped {
                            if !vals.is_empty() {
                                crate::connection::DIRTY_CHANGES
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let _ = waiter
                                    .sender
                                    .send(BlockedListResult::Popped(key.clone(), vals));
                                satisfied_clients.push(waiter.client_id);
                                if !table.exists(key.as_ref()) {
                                    break;
                                }
                            } else {
                                // No elements available to satisfy waiter, put it back!
                                waiters.push_front(waiter);
                                break;
                            }
                        } else {
                            waiters.push_front(waiter);
                            break;
                        }
                    }
                    WaiterOp::Move {
                        where_from,
                        where_to,
                        ref destination,
                    } => {
                        let dst_type = table.type_of(destination.as_ref());
                        if dst_type != "none" && dst_type != "list" {
                            let _ = waiter
                                .sender
                                .send(BlockedListResult::Unblocked(ClientUnblockType::WrongType));
                            satisfied_clients.push(waiter.client_id);
                            continue;
                        }
                        let popped = match where_from {
                            ListPopType::Left => {
                                table.lpop(key.as_ref(), 1).ok().and_then(|mut v| v.pop())
                            }
                            ListPopType::Right => {
                                table.rpop(key.as_ref(), 1).ok().and_then(|mut v| v.pop())
                            }
                        };
                        if let Some(val) = popped {
                            crate::connection::DIRTY_CHANGES
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            match where_to {
                                ListPopType::Left => {
                                    let _ = table.lpush(destination.clone(), vec![val.clone()]);
                                }
                                ListPopType::Right => {
                                    let _ = table.rpush(destination.clone(), vec![val.clone()]);
                                }
                            }
                            let dest_clone = destination.clone();
                            crate::connection::touch_watched_key(self.port, destination.as_ref());
                            let _ = waiter
                                .sender
                                .send(BlockedListResult::Popped(key.clone(), vec![val]));
                            satisfied_clients.push(waiter.client_id);
                            dest_to_notify = Some(dest_clone);
                            break;
                        } else {
                            waiters.push_front(waiter);
                            break;
                        }
                    }
                }
            }
        }
        for cid in satisfied_clients {
            self.remove_waiters_for_client(cid);
        }
        if let Some(dst) = dest_to_notify {
            self.notify_list(table, &dst);
        }
    }

    pub fn register_zset_waiter(
        &mut self,
        client_id: u64,
        key: Bytes,
        pop_type: ZSetPopType,
        count: usize,
        is_zmpop: bool,
        sender: Sender<BlockedZSetResult>,
    ) {
        self.zset_waiters
            .entry(key.clone())
            .or_default()
            .push_back(ZSetWaiter {
                client_id,
                key,
                pop_type,
                count,
                is_zmpop,
                sender,
            });
        self.sync_atomic_waiters_count();
    }

    /// Called when elements are added to a sorted set.
    pub fn notify_zset(&mut self, table: &mut crate::table::RudisTable, key: &Bytes) {
        crate::connection::touch_watched_key(self.port, key.as_ref());
        if table.is_key_expired(key.as_ref()) {
            return;
        }
        let mut satisfied_clients = Vec::new();
        if let Some(waiters) = self.zset_waiters.get_mut(key) {
            while let Some(waiter) = waiters.pop_front() {
                if waiter.sender.is_disconnected() || satisfied_clients.contains(&waiter.client_id)
                {
                    continue;
                }
                let popped = match waiter.pop_type {
                    ZSetPopType::Min => table.zpopmin(key.as_ref(), waiter.count).ok(),
                    ZSetPopType::Max => table.zpopmax(key.as_ref(), waiter.count).ok(),
                };
                if let Some(items) = popped {
                    if !items.is_empty() {
                        crate::connection::DIRTY_CHANGES
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = waiter.sender.send(BlockedZSetResult::Popped {
                            key: key.clone(),
                            items,
                            is_zmpop: waiter.is_zmpop,
                        });
                        satisfied_clients.push(waiter.client_id);
                        if !table.exists(key.as_ref()) {
                            break;
                        }
                    } else {
                        waiters.push_front(waiter);
                        break;
                    }
                } else {
                    waiters.push_front(waiter);
                    break;
                }
            }
        }
        for cid in satisfied_clients {
            self.remove_waiters_for_client(cid);
        }
    }

    pub fn register_stream_waiter(&mut self, key: Bytes, sender: Sender<()>) {
        self.stream_waiters
            .entry(key.clone())
            .or_default()
            .push(StreamWaiter { key, sender });
        self.sync_atomic_waiters_count();
    }

    /// Called when XADD adds an entry to a stream.
    pub fn notify_stream(&mut self, key: &Bytes) {
        if let Some(waiters) = self.stream_waiters.remove(key) {
            for waiter in waiters {
                let _ = waiter.sender.send(());
            }
            self.sync_atomic_waiters_count();
        }
    }
}
