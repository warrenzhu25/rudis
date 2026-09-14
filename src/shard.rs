use hashbrown::HashMap;

/// Messages passed across CPU cores to access or mutate a shard's data.
pub enum ShardMessage {
    Get {
        key: Vec<u8>,
        responder: flume::Sender<Option<Vec<u8>>>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        responder: flume::Sender<()>,
    },
}

/// A purely thread-local key-value store for one shard.
/// Because this shard is accessed only by the thread running on its assigned CPU core,
/// it requires NO Mutex and NO cross-thread synchronization.
pub struct ShardDb {
    entries: HashMap<Vec<u8>, Vec<u8>>,
}

impl ShardDb {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.entries.get(key).cloned()
    }

    #[inline]
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.entries.insert(key, value);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
