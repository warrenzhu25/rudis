use bytes::Bytes;
use hashbrown::HashMap;

/// Messages passed across CPU cores to access or mutate a shard's data.
pub enum ShardMessage {
    Get {
        key: Bytes,
        responder: flume::Sender<Option<Bytes>>,
    },
    Set {
        key: Bytes,
        value: Bytes,
        responder: flume::Sender<()>,
    },
    Del {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    Exists {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
}

/// A purely thread-local key-value store for one shard.
/// Because this shard is accessed only by the thread running on its assigned CPU core,
/// it requires NO Mutex and NO cross-thread synchronization.
pub struct ShardDb {
    entries: HashMap<Bytes, Bytes>,
}

impl ShardDb {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.entries.get(key).cloned()
    }

    #[inline]
    pub fn set(&mut self, key: Bytes, value: Bytes) {
        self.entries.insert(key, value);
    }

    #[inline]
    pub fn del(&mut self, key: &[u8]) -> bool {
        self.entries.remove(key).is_some()
    }

    #[inline]
    pub fn exists(&self, key: &[u8]) -> bool {
        self.entries.contains_key(key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
