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
    Movem {
        where_from: ListPopType,
        where_to: ListPopType,
        destination: Bytes,
        mode: crate::resp::LmovemMode,
        count: usize,
        ordering: crate::resp::LmovemOrdering,
    },
}

pub struct ListWaiter {
    pub client_id: u64,
    pub key: Bytes,
    pub op: WaiterOp,
    pub sender: Sender<BlockedListResult>,
}

#[derive(Clone, Debug)]
pub enum BlockedStreamResult {
    Data(Vec<u8>),
    Error(Vec<u8>),
    CrossShard,
    Unblocked(ClientUnblockType),
}

pub struct StreamWaiter {
    pub client_id: u64,
    pub key: Bytes,
    pub cmd: crate::resp::Command,
    pub is_resp3: bool,
    pub is_cross_shard: bool,
    pub sender: Sender<BlockedStreamResult>,
}

pub struct BlockHub {
    pub port: u16,
    list_waiters: HashMap<Bytes, VecDeque<ListWaiter>>,
    zset_waiters: HashMap<Bytes, VecDeque<ZSetWaiter>>,
    stream_waiters: HashMap<Bytes, Vec<StreamWaiter>>,
    blocked_clients: HashMap<u64, Sender<BlockedListResult>>,
    blocked_zset_clients: HashMap<u64, Sender<BlockedZSetResult>>,
    blocked_stream_clients: HashMap<u64, Sender<BlockedStreamResult>>,
    paused_count: usize,
    pending_notifies: Vec<Bytes>,
    /// This hub's share of `TOTAL_BLOCKED_WAITERS`.
    published_waiters: usize,
}

pub static PORT_BLOCK_HUBS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
/// Sum of every hub's waiters. Each hub adds and removes only its own share:
/// with several servers in one process (one hub per port), a hub storing its
/// own count would hide another hub's waiters, and a push there would skip
/// the wakeup.
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

impl Drop for BlockHub {
    fn drop(&mut self) {
        self.publish_waiters(0);
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
            blocked_stream_clients: HashMap::new(),
            paused_count: 0,
            pending_notifies: Vec::new(),
            published_waiters: 0,
        }
    }

    #[inline(always)]
    pub fn sync_atomic_waiters_count(&mut self) {
        let count = self.paused_count
            + self.list_waiters.values().map(|w| w.len()).sum::<usize>()
            + self.zset_waiters.values().map(|w| w.len()).sum::<usize>()
            + self.stream_waiters.values().map(|w| w.len()).sum::<usize>()
            + self.blocked_clients.len()
            + self.blocked_zset_clients.len()
            + self.blocked_stream_clients.len();
        self.publish_waiters(count);
    }

    fn publish_waiters(&mut self, count: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        if count > self.published_waiters {
            TOTAL_BLOCKED_WAITERS.fetch_add(count - self.published_waiters, Relaxed);
        } else {
            TOTAL_BLOCKED_WAITERS.fetch_sub(self.published_waiters - count, Relaxed);
        }
        self.published_waiters = count;
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
        self.blocked_clients.len()
            + self.blocked_zset_clients.len()
            + self.blocked_stream_clients.len()
    }

    pub fn blocking_keys_count(&self) -> usize {
        let mut keys: hashbrown::HashSet<&Bytes> = hashbrown::HashSet::new();
        for (k, v) in &self.list_waiters {
            if !v.is_empty() {
                keys.insert(k);
            }
        }
        for (k, v) in &self.zset_waiters {
            if !v.is_empty() {
                keys.insert(k);
            }
        }
        for (k, v) in &self.stream_waiters {
            if !v.is_empty() {
                keys.insert(k);
            }
        }
        keys.len()
    }

    pub fn blocking_keys_on_nokey_count(&self) -> usize {
        self.stream_waiters
            .values()
            .filter(|v| !v.is_empty())
            .count()
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
            || self.blocked_stream_clients.contains_key(&client_id)
    }

    pub fn remove_waiters_for_client(&mut self, client_id: u64) {
        for waiters in self.list_waiters.values_mut() {
            waiters.retain(|w| w.client_id != client_id);
        }
        self.list_waiters.retain(|_, v| !v.is_empty());
        for waiters in self.zset_waiters.values_mut() {
            waiters.retain(|w| w.client_id != client_id);
        }
        self.zset_waiters.retain(|_, v| !v.is_empty());
        for waiters in self.stream_waiters.values_mut() {
            waiters.retain(|w| w.client_id != client_id);
        }
        self.stream_waiters.retain(|_, v| !v.is_empty());
        self.blocked_clients.remove(&client_id);
        self.blocked_zset_clients.remove(&client_id);
        self.blocked_stream_clients.remove(&client_id);
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
        if let Some(sender) = self.blocked_stream_clients.remove(&client_id) {
            let _ = sender.try_send(BlockedStreamResult::Unblocked(unblock_type));
            for waiters in self.stream_waiters.values_mut() {
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

    pub fn register_movem_waiter(
        &mut self,
        client_id: u64,
        source: Bytes,
        where_from: ListPopType,
        where_to: ListPopType,
        destination: Bytes,
        mode: crate::resp::LmovemMode,
        count: usize,
        ordering: crate::resp::LmovemOrdering,
        sender: Sender<BlockedListResult>,
    ) {
        self.list_waiters
            .entry(source.clone())
            .or_default()
            .push_back(ListWaiter {
                client_id,
                key: source,
                op: WaiterOp::Movem {
                    where_from,
                    where_to,
                    destination,
                    mode,
                    count,
                    ordering,
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
                // Nothing to serve yet: stay blocked. Without this a move
                // waiter whose destination has the wrong type would fail
                // with WRONGTYPE on a wakeup check before any element
                // arrived; Redis only fails once an element is there.
                if !table.exists(key.as_ref()) {
                    waiters.push_front(waiter);
                    break;
                }
                match waiter.op {
                    WaiterOp::Pop { pop_type, count } => {
                        let popped = match pop_type {
                            ListPopType::Left => table.lpop(key.as_ref(), count).ok(),
                            ListPopType::Right => table.rpop(key.as_ref(), count).ok(),
                        };
                        if let Some(vals) = popped {
                            if !vals.is_empty() {
                                let count = Some(vals.len());
                                propagate_served(
                                    self.port,
                                    &match pop_type {
                                        ListPopType::Left => crate::resp::Command::Lpop {
                                            key: key.clone(),
                                            count,
                                        },
                                        ListPopType::Right => crate::resp::Command::Rpop {
                                            key: key.clone(),
                                            count,
                                        },
                                    },
                                );
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
                            match where_to {
                                ListPopType::Left => {
                                    let _ = table.lpush(destination.clone(), vec![val.clone()]);
                                }
                                ListPopType::Right => {
                                    let _ = table.rpush(destination.clone(), vec![val.clone()]);
                                }
                            }
                            let dir = |t: ListPopType| match t {
                                ListPopType::Left => crate::table::ListDirection::Left,
                                ListPopType::Right => crate::table::ListDirection::Right,
                            };
                            propagate_served(
                                self.port,
                                &crate::resp::Command::Lmove {
                                    source: key.clone(),
                                    destination: destination.clone(),
                                    where_from: dir(where_from),
                                    where_to: dir(where_to),
                                },
                            );
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
                    WaiterOp::Movem {
                        where_from,
                        where_to,
                        ref destination,
                        mode,
                        count,
                        ordering,
                    } => {
                        let dst_type = table.type_of(destination.as_ref());
                        if dst_type != "none" && dst_type != "list" {
                            let _ = waiter
                                .sender
                                .send(BlockedListResult::Unblocked(ClientUnblockType::WrongType));
                            satisfied_clients.push(waiter.client_id);
                            continue;
                        }
                        let from_dir = match where_from {
                            ListPopType::Left => crate::table::ListDirection::Left,
                            ListPopType::Right => crate::table::ListDirection::Right,
                        };
                        let to_dir = match where_to {
                            ListPopType::Left => crate::table::ListDirection::Left,
                            ListPopType::Right => crate::table::ListDirection::Right,
                        };
                        match table.lmovem(
                            key.as_ref(),
                            destination.clone(),
                            from_dir,
                            to_dir,
                            mode,
                            count,
                            ordering,
                        ) {
                            Ok(Some(vals)) => {
                                propagate_served(
                                    self.port,
                                    &crate::resp::Command::Lmovem {
                                        source: key.clone(),
                                        destination: destination.clone(),
                                        where_from: from_dir,
                                        where_to: to_dir,
                                        mode: crate::resp::LmovemMode::Exactly,
                                        count: vals.len(),
                                        ordering,
                                        raw_tokens: None,
                                    },
                                );
                                crate::connection::touch_watched_key(self.port, key.as_ref());
                                crate::connection::touch_watched_key(
                                    self.port,
                                    destination.as_ref(),
                                );
                                let dest_clone = destination.clone();
                                let _ = waiter
                                    .sender
                                    .send(BlockedListResult::Popped(key.clone(), vals));
                                satisfied_clients.push(waiter.client_id);
                                dest_to_notify = Some(dest_clone);
                                break;
                            }
                            Ok(None) => {
                                waiters.push_front(waiter);
                                break;
                            }
                            Err(_) => {
                                let _ = waiter.sender.send(BlockedListResult::Unblocked(
                                    ClientUnblockType::WrongType,
                                ));
                                satisfied_clients.push(waiter.client_id);
                                continue;
                            }
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
                        let count = Some(items.len());
                        propagate_served(
                            self.port,
                            &match waiter.pop_type {
                                ZSetPopType::Min => crate::resp::Command::Zpopmin {
                                    key: key.clone(),
                                    count,
                                },
                                ZSetPopType::Max => crate::resp::Command::Zpopmax {
                                    key: key.clone(),
                                    count,
                                },
                            },
                        );
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

    pub fn register_stream_waiter(
        &mut self,
        client_id: u64,
        key: Bytes,
        cmd: crate::resp::Command,
        is_resp3: bool,
        is_cross_shard: bool,
        sender: Sender<BlockedStreamResult>,
    ) {
        self.blocked_stream_clients
            .insert(client_id, sender.clone());
        let entry = self.stream_waiters.entry(key.clone()).or_default();
        if !entry.iter().any(|w| w.client_id == client_id) {
            entry.push(StreamWaiter {
                client_id,
                key,
                cmd,
                is_resp3,
                is_cross_shard,
                sender,
            });
        }
        self.sync_atomic_waiters_count();
    }

    /// Called when XADD adds an entry to a stream.
    pub fn notify_stream(&mut self, db: &mut crate::shard::ShardDb, key: &Bytes) {
        if let Some(waiters) = self.stream_waiters.get_mut(key) {
            let mut satisfied = Vec::new();
            for (idx, waiter) in waiters.iter().enumerate() {
                if waiter.sender.is_disconnected() {
                    satisfied.push(idx);
                    continue;
                }
                if waiter.is_cross_shard {
                    let _ = waiter.sender.send(BlockedStreamResult::CrossShard);
                    satisfied.push(idx);
                    continue;
                }
                let mut out = Vec::new();
                crate::connection::CURRENT_CLIENT_RESP3.set(waiter.is_resp3);
                crate::connection::execute_local_command(&waiter.cmd, db, &mut out, None);
                let is_xread = matches!(waiter.cmd, crate::resp::Command::Xread { .. });
                let empty = out == b"$-1\r\n"
                    || out == b"*0\r\n"
                    || (is_xread && out.starts_with(b"-WRONGTYPE"));
                if !empty {
                    let res = if out.starts_with(b"-") {
                        BlockedStreamResult::Error(out)
                    } else {
                        BlockedStreamResult::Data(out)
                    };
                    let _ = waiter.sender.send(res);
                    satisfied.push(idx);
                }
            }
            let mut removed_cids = Vec::new();
            for idx in satisfied.into_iter().rev() {
                let w = waiters.remove(idx);
                self.blocked_stream_clients.remove(&w.client_id);
                removed_cids.push(w.client_id);
            }
            if waiters.is_empty() {
                self.stream_waiters.remove(key);
            }
            if !removed_cids.is_empty() {
                for other_waiters in self.stream_waiters.values_mut() {
                    other_waiters.retain(|ow| !removed_cids.contains(&ow.client_id));
                }
            }
            self.sync_atomic_waiters_count();
        }
    }

    pub fn notify_all_streams(&mut self, db: &mut crate::shard::ShardDb) {
        let keys: Vec<Bytes> = self.stream_waiters.keys().cloned().collect();
        for k in keys {
            self.notify_stream(db, &k);
        }
    }
}

/// Logs and replicates a mutation made while serving a blocked client. It
/// runs on the key owner's shard thread right after the write that woke the
/// client, so the AOF and the replicas see the pop in the same order, and in
/// the same shard's AOF, as the data change itself.
fn propagate_served(port: u16, cmd: &crate::resp::Command) {
    let Some(bytes) = crate::aof::command_to_resp(cmd) else {
        return;
    };
    let mut shard_id = None;
    crate::connection::CURRENT_ROUTER.with(|cr| {
        if let Some(router) = cr.borrow().as_ref() {
            shard_id = Some(router.shard_id);
            if let Some(aof) = &router.aof {
                aof.borrow_mut().append(&bytes);
            }
        }
    });
    if crate::replication::has_connected_replicas(port) {
        match shard_id {
            Some(id) => crate::replication::propagate_shard_bytes(port, id, &bytes),
            None => crate::replication::propagate_bytes(port, &bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use flume::unbounded;

    #[test]
    fn test_block_hub_lifecycle_and_pause() {
        let mut hub = BlockHub::new(12345);
        assert!(!hub.is_paused());

        hub.pause();
        assert!(hub.is_paused());
        hub.add_pending_notify(Bytes::from_static(b"k1"));

        let pending = hub.resume();
        assert!(!hub.is_paused());
        assert_eq!(pending, vec![Bytes::from_static(b"k1")]);

        let (tx, _rx) = unbounded();
        hub.register_list_waiter(100, Bytes::from_static(b"k1"), ListPopType::Left, 1, tx);
        assert_eq!(hub.list_waiters.len(), 1);
        assert!(has_blocked_waiters(12345));

        hub.unregister_blocked_client(100);
        assert_eq!(
            hub.list_waiters
                .get(&Bytes::from_static(b"k1"))
                .map_or(0, |v| v.len()),
            0
        );
        assert_eq!(hub.published_waiters, 0);
    }

    #[test]
    fn test_one_hub_emptying_does_not_hide_another_hubs_waiters() {
        let mut busy = BlockHub::new(12348);
        let (tx, _rx) = unbounded();
        busy.register_list_waiter(400, Bytes::from_static(b"k"), ListPopType::Left, 1, tx);

        let mut other = BlockHub::new(12349);
        let (tx2, _rx2) = unbounded();
        other.register_blocked_client(401, tx2);
        other.unregister_blocked_client(401);
        assert!(
            has_blocked_waiters(12348),
            "a push on the busy hub's port must still look for waiters"
        );
    }

    #[test]
    fn test_block_hub_notify_list_and_move() {
        let mut hub = BlockHub::new(12346);
        let mut table = crate::table::RudisTable::new();
        let key = Bytes::from_static(b"mylist");

        let (tx, rx) = unbounded();
        hub.register_list_waiter(201, key.clone(), ListPopType::Left, 1, tx);

        // Initially empty, notify does not satisfy
        hub.notify_list(&mut table, &key);
        assert!(rx.try_recv().is_err());

        // Push data to table and notify
        table
            .rpush(key.clone(), vec![Bytes::from_static(b"item1")])
            .unwrap();
        hub.notify_list(&mut table, &key);

        let res = rx.try_recv().expect("should receive popped element");
        match res {
            BlockedListResult::Popped(k, items) => {
                assert_eq!(k, key);
                assert_eq!(items, vec![Bytes::from_static(b"item1")]);
            }
            _ => panic!("unexpected result: {:?}", res),
        }

        // Test move waiter
        let src = Bytes::from_static(b"src_list");
        let dst = Bytes::from_static(b"dst_list");
        let (tx_m, rx_m) = unbounded();
        hub.register_move_waiter(
            202,
            src.clone(),
            ListPopType::Right,
            ListPopType::Left,
            dst.clone(),
            tx_m,
        );

        table
            .rpush(src.clone(), vec![Bytes::from_static(b"moved_val")])
            .unwrap();
        hub.notify_list(&mut table, &src);

        let res_m = rx_m.try_recv().expect("should receive moved element");
        match res_m {
            BlockedListResult::Popped(s, items) => {
                assert_eq!(s, src);
                assert_eq!(items, vec![Bytes::from_static(b"moved_val")]);
            }
            _ => panic!("unexpected move result: {:?}", res_m),
        }
        assert_eq!(table.llen(dst.as_ref()), Ok(1));
    }

    #[test]
    fn test_block_hub_zset_and_stream_notifications() {
        let mut hub = BlockHub::new(12347);
        let mut table = crate::table::RudisTable::new();
        let zk = Bytes::from_static(b"zkey");

        let (tx_z, rx_z) = unbounded();
        hub.register_zset_waiter(301, zk.clone(), ZSetPopType::Min, 1, false, tx_z);

        table
            .zadd(
                zk.clone(),
                vec![(10.0, Bytes::from_static(b"m10"))],
                crate::table::ZAddFlags::default(),
            )
            .unwrap();
        hub.notify_zset(&mut table, &zk);

        let res_z = rx_z.try_recv().expect("should receive zset pop");
        match res_z {
            BlockedZSetResult::Popped {
                key,
                items,
                is_zmpop,
            } => {
                assert_eq!(key, zk);
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].0, Bytes::from_static(b"m10"));
                assert!(!is_zmpop);
            }
            _ => panic!("unexpected zset pop result: {:?}", res_z),
        }

        // Test Stream waiter
        let sk = Bytes::from_static(b"skey");
        let (tx_s, rx_s) = unbounded();
        let cmd = crate::resp::Command::Xread {
            count: None,
            maxcount: None,
            maxsize: None,
            block_ms: None,
            keys: vec![sk.clone()],
            ids: vec!["0-0".to_string()],
        };
        let mut sdb = crate::shard::ShardDb::new(12346);
        let xadd_cmd = crate::resp::Command::Xadd {
            key: sk.clone(),
            nomkstream: false,
            maxlen: None,
            minid: None,
            approx: false,
            trim_strategy: crate::table::StreamTrimStrategy::KeepRef,
            idmp: None,
            id: crate::table::StreamAddId::Explicit(crate::table::StreamId::new(1, 0)),
            fields: vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            limit: None,
        };
        let mut dummy = Vec::new();
        crate::connection::execute_local_command(&xadd_cmd, &mut sdb, &mut dummy, None);
        hub.register_stream_waiter(1001, sk.clone(), cmd, false, false, tx_s);
        assert_eq!(hub.stream_waiters.len(), 1);

        hub.notify_stream(&mut sdb, &sk);
        assert!(rx_s.try_recv().is_ok());
        assert_eq!(hub.stream_waiters.len(), 0);

        // Test unblock client
        let (tx_u, rx_u) = unbounded();
        hub.blocked_clients.insert(999, tx_u);
        assert!(hub.unblock_client(999, ClientUnblockType::Timeout));
        match rx_u.try_recv().unwrap() {
            BlockedListResult::Unblocked(ClientUnblockType::Timeout) => {}
            other => panic!("unexpected unblocked result: {:?}", other),
        }
    }
}
