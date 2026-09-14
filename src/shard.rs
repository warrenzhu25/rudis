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
    IncrBy {
        key: Bytes,
        delta: i64,
        responder: flume::Sender<Result<i64, String>>,
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

    pub fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<i64, String> {
        let current = match self.entries.get(&key) {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| "value is not an integer or out of range".to_string())?;
                s.parse::<i64>()
                    .map_err(|_| "value is not an integer or out of range".to_string())?
            }
            None => 0,
        };

        let new_val = current
            .checked_add(delta)
            .ok_or_else(|| "increment or decrement would overflow".to_string())?;
        self.entries.insert(key, Bytes::from(new_val.to_string()));
        Ok(new_val)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
