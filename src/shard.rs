use std::time::{Duration, Instant};
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
        expire_in: Option<Duration>,
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
    Expire {
        key: Bytes,
        duration: Duration,
        responder: flume::Sender<bool>,
    },
    Persist {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    Ttl {
        key: Bytes,
        in_millis: bool,
        responder: flume::Sender<i64>,
    },
}

/// A purely thread-local key-value store for one shard.
/// Because this shard is accessed only by the thread running on its assigned CPU core,
/// it requires NO Mutex and NO cross-thread synchronization.
pub struct ShardDb {
    entries: HashMap<Bytes, Bytes>,
    expirations: HashMap<Bytes, Instant>,
}

impl ShardDb {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            expirations: HashMap::new(),
        }
    }

    /// Check if a key is expired. If so, passively remove it.
    /// Returns true if the key was expired and deleted.
    #[inline]
    fn check_expired(&mut self, key: &[u8]) -> bool {
        if let Some(&expire_at) = self.expirations.get(key) {
            if Instant::now() >= expire_at {
                self.entries.remove(key);
                self.expirations.remove(key);
                return true;
            }
        }
        false
    }

    #[inline]
    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        if self.check_expired(key) {
            return None;
        }
        self.entries.get(key).cloned()
    }

    #[inline]
    pub fn set(&mut self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        if let Some(d) = expire_in {
            self.expirations.insert(key.clone(), Instant::now() + d);
        } else {
            self.expirations.remove(&key);
        }
        self.entries.insert(key, value);
    }

    #[inline]
    pub fn del(&mut self, key: &[u8]) -> bool {
        self.expirations.remove(key);
        self.entries.remove(key).is_some()
    }

    #[inline]
    pub fn exists(&mut self, key: &[u8]) -> bool {
        if self.check_expired(key) {
            return false;
        }
        self.entries.contains_key(key)
    }

    pub fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<i64, String> {
        let _ = self.check_expired(&key);
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

    pub fn expire(&mut self, key: &[u8], duration: Duration) -> bool {
        if self.check_expired(key) || !self.entries.contains_key(key) {
            return false;
        }
        if let Some((k, _)) = self.entries.get_key_value(key) {
            self.expirations.insert(k.clone(), Instant::now() + duration);
            true
        } else {
            false
        }
    }

    pub fn persist(&mut self, key: &[u8]) -> bool {
        if self.check_expired(key) || !self.entries.contains_key(key) {
            return false;
        }
        self.expirations.remove(key).is_some()
    }

    pub fn ttl(&mut self, key: &[u8], in_millis: bool) -> i64 {
        if self.check_expired(key) || !self.entries.contains_key(key) {
            return -2; // Key does not exist
        }
        match self.expirations.get(key) {
            Some(&expire_at) => {
                let now = Instant::now();
                if now >= expire_at {
                    self.entries.remove(key);
                    self.expirations.remove(key);
                    -2
                } else {
                    let diff = expire_at.duration_since(now);
                    if in_millis {
                        diff.as_millis() as i64
                    } else {
                        diff.as_secs() as i64
                    }
                }
            }
            None => -1, // Key exists with no expiration
        }
    }

    /// Active expiration cycle: samples up to 20 keys with expiration and evicts expired ones.
    pub fn active_expire_cycle(&mut self) -> usize {
        if self.expirations.is_empty() {
            return 0;
        }
        let now = Instant::now();
        let mut expired_keys = Vec::new();

        for (k, &expire_at) in self.expirations.iter().take(20) {
            if now >= expire_at {
                expired_keys.push(k.clone());
            }
        }

        let count = expired_keys.len();
        for k in expired_keys {
            self.entries.remove(&k);
            self.expirations.remove(&k);
        }
        count
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
