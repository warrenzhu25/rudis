use bytes::Bytes;
use flume::Sender;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListPopType {
    Left,
    Right,
}

pub struct ListWaiter {
    pub key: Bytes,
    pub pop_type: ListPopType,
    pub sender: Sender<(Bytes, Bytes)>, // (key, value)
}

pub struct StreamWaiter {
    pub key: Bytes,
    pub sender: Sender<()>,
}

pub struct BlockHub {
    list_waiters: HashMap<Bytes, VecDeque<ListWaiter>>,
    stream_waiters: HashMap<Bytes, Vec<StreamWaiter>>,
}

pub static PORT_BLOCK_HUBS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn get_block_hub_for_port(port: u16) -> Arc<Mutex<BlockHub>> {
    let mut map = PORT_BLOCK_HUBS.lock().unwrap();
    map.entry(port)
        .or_insert_with(|| Arc::new(Mutex::new(BlockHub::new())))
        .clone()
}

impl Default for BlockHub {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockHub {
    pub fn new() -> Self {
        Self {
            list_waiters: HashMap::new(),
            stream_waiters: HashMap::new(),
        }
    }

    pub fn register_list_waiter(
        &mut self,
        key: Bytes,
        pop_type: ListPopType,
        sender: Sender<(Bytes, Bytes)>,
    ) {
        self.list_waiters
            .entry(key.clone())
            .or_default()
            .push_back(ListWaiter {
                key,
                pop_type,
                sender,
            });
    }

    /// Called when LPUSH or RPUSH adds values to a list.
    /// If there is an active waiter, pop from table directly and deliver to the waiter.
    pub fn notify_list(&mut self, table: &mut crate::table::RudisTable, key: &Bytes) {
        if let Some(waiters) = self.list_waiters.get_mut(key) {
            while let Some(waiter) = waiters.pop_front() {
                if waiter.sender.is_disconnected() {
                    continue;
                }
                // Try popping from the list
                let popped = match waiter.pop_type {
                    ListPopType::Left => table.lpop(key.as_ref(), 1).ok().and_then(|mut v| v.pop()),
                    ListPopType::Right => table.rpop(key.as_ref(), 1).ok().and_then(|mut v| v.pop()),
                };
                if let Some(val) = popped {
                    let _ = waiter.sender.send((key.clone(), val));
                    break;
                }
            }
        }
    }

    pub fn register_stream_waiter(&mut self, key: Bytes, sender: Sender<()>) {
        self.stream_waiters
            .entry(key.clone())
            .or_default()
            .push(StreamWaiter { key, sender });
    }

    /// Called when XADD adds an entry to a stream.
    pub fn notify_stream(&mut self, key: &Bytes) {
        if let Some(waiters) = self.stream_waiters.remove(key) {
            for waiter in waiters {
                let _ = waiter.sender.send(());
            }
        }
    }
}
