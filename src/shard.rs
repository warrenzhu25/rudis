use std::time::{Duration, Instant};
use bytes::Bytes;
use hashbrown::{HashMap, HashSet};

use crate::resp::Command;

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
    CountKeysInSlot {
        slot: u16,
        responder: flume::Sender<usize>,
    },
    GetKeysInSlot {
        slot: u16,
        count: usize,
        responder: flume::Sender<Vec<Bytes>>,
    },
    ClientList {
        responder: flume::Sender<String>,
    },
    Batch {
        items: Vec<(usize, Command)>,
        responder: flume::Sender<Vec<(usize, Vec<u8>)>>,
    },
}

/// A purely thread-local key-value store for one shard.
/// Because this shard is accessed only by the thread running on its assigned CPU core,
/// it requires NO Mutex and NO cross-thread synchronization.
pub struct ShardDb {
    entries: HashMap<Bytes, Bytes>,
    hashes: HashMap<Bytes, HashMap<Bytes, Bytes>>,
    expirations: HashMap<Bytes, Instant>,
    slot_to_keys: HashMap<u16, HashSet<Bytes>>,
}

impl ShardDb {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            hashes: HashMap::new(),
            expirations: HashMap::new(),
            slot_to_keys: HashMap::new(),
        }
    }

    /// Check if a key is expired. If so, passively remove it.
    /// Returns true if the key was expired and deleted.
    #[inline]
    fn check_expired(&mut self, key: &[u8]) -> bool {
        if let Some(&expire_at) = self.expirations.get(key) {
            if Instant::now() >= expire_at {
                let slot = crate::router::key_slot(key);
                if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                    set.remove(key);
                }
                self.entries.remove(key);
                self.hashes.remove(key);
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
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys.entry(slot).or_default().insert(key.clone());
        self.hashes.remove(&key);
        if let Some(d) = expire_in {
            self.expirations.insert(key.clone(), Instant::now() + d);
        } else {
            self.expirations.remove(&key);
        }
        self.entries.insert(key, value);
    }

    #[inline]
    pub fn del(&mut self, key: &[u8]) -> bool {
        let slot = crate::router::key_slot(key);
        if let Some(set) = self.slot_to_keys.get_mut(&slot) {
            set.remove(key);
        }
        self.expirations.remove(key);
        let removed_str = self.entries.remove(key).is_some();
        let removed_hash = self.hashes.remove(key).is_some();
        removed_str || removed_hash
    }

    #[inline]
    pub fn exists(&mut self, key: &[u8]) -> bool {
        if self.check_expired(key) {
            return false;
        }
        self.entries.contains_key(key) || self.hashes.contains_key(key)
    }

    pub fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<i64, String> {
        let _ = self.check_expired(&key);
        if self.hashes.contains_key(&key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
        }
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
        self.set(key, Bytes::from(new_val.to_string()), None);
        Ok(new_val)
    }

    pub fn expire(&mut self, key: &[u8], duration: Duration) -> bool {
        if self.check_expired(key) {
            return false;
        }
        let exists = self.entries.contains_key(key) || self.hashes.contains_key(key);
        if !exists {
            return false;
        }
        let k = self
            .entries
            .get_key_value(key)
            .map(|(k, _)| k.clone())
            .or_else(|| self.hashes.get_key_value(key).map(|(k, _)| k.clone()));
        if let Some(k) = k {
            self.expirations.insert(k, Instant::now() + duration);
            true
        } else {
            false
        }
    }

    pub fn persist(&mut self, key: &[u8]) -> bool {
        if self.check_expired(key) {
            return false;
        }
        let exists = self.entries.contains_key(key) || self.hashes.contains_key(key);
        if !exists {
            return false;
        }
        self.expirations.remove(key).is_some()
    }

    pub fn ttl(&mut self, key: &[u8], in_millis: bool) -> i64 {
        if self.check_expired(key) {
            return -2;
        }
        let exists = self.entries.contains_key(key) || self.hashes.contains_key(key);
        if !exists {
            return -2; // Key does not exist
        }
        match self.expirations.get(key) {
            Some(&expire_at) => {
                let now = Instant::now();
                if now >= expire_at {
                    let slot = crate::router::key_slot(key);
                    if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                        set.remove(key);
                    }
                    self.entries.remove(key);
                    self.hashes.remove(key);
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

    pub fn hset(&mut self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<usize, &'static str> {
        let _ = self.check_expired(&key);
        if self.entries.contains_key(&key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        let slot = crate::router::key_slot(&key);
        self.slot_to_keys.entry(slot).or_default().insert(key.clone());

        let map = self.hashes.entry(key).or_default();
        let mut added = 0;
        for (f, v) in fields {
            if map.insert(f, v).is_none() {
                added += 1;
            }
        }
        Ok(added)
    }

    pub fn hget(&mut self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, &'static str> {
        if self.check_expired(key) {
            return Ok(None);
        }
        if self.entries.contains_key(key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        Ok(self.hashes.get(key).and_then(|m| m.get(field).cloned()))
    }

    pub fn hmget(&mut self, key: &[u8], fields: &[Bytes]) -> Result<Vec<Option<Bytes>>, &'static str> {
        if self.check_expired(key) {
            return Ok(vec![None; fields.len()]);
        }
        if self.entries.contains_key(key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if let Some(map) = self.hashes.get(key) {
            Ok(fields.iter().map(|f| map.get(f).cloned()).collect())
        } else {
            Ok(vec![None; fields.len()])
        }
    }

    pub fn hdel(&mut self, key: &[u8], fields: &[Bytes]) -> Result<usize, &'static str> {
        if self.check_expired(key) {
            return Ok(0);
        }
        if self.entries.contains_key(key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if let Some(map) = self.hashes.get_mut(key) {
            let mut count = 0;
            for f in fields {
                if map.remove(f).is_some() {
                    count += 1;
                }
            }
            if map.is_empty() {
                self.hashes.remove(key);
                self.expirations.remove(key);
                let slot = crate::router::key_slot(key);
                if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                    set.remove(key);
                }
            }
            Ok(count)
        } else {
            Ok(0)
        }
    }

    pub fn hexists(&mut self, key: &[u8], field: &[u8]) -> Result<bool, &'static str> {
        if self.check_expired(key) {
            return Ok(false);
        }
        if self.entries.contains_key(key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        Ok(self.hashes.get(key).map_or(false, |m| m.contains_key(field)))
    }

    pub fn hlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        if self.check_expired(key) {
            return Ok(0);
        }
        if self.entries.contains_key(key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        Ok(self.hashes.get(key).map_or(0, |m| m.len()))
    }

    pub fn hgetall(&mut self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, &'static str> {
        if self.check_expired(key) {
            return Ok(Vec::new());
        }
        if self.entries.contains_key(key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if let Some(map) = self.hashes.get(key) {
            Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        } else {
            Ok(Vec::new())
        }
    }

    pub fn hkeys(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        if self.check_expired(key) {
            return Ok(Vec::new());
        }
        if self.entries.contains_key(key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if let Some(map) = self.hashes.get(key) {
            Ok(map.keys().cloned().collect())
        } else {
            Ok(Vec::new())
        }
    }

    pub fn hvals(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        if self.check_expired(key) {
            return Ok(Vec::new());
        }
        if self.entries.contains_key(key) {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if let Some(map) = self.hashes.get(key) {
            Ok(map.values().cloned().collect())
        } else {
            Ok(Vec::new())
        }
    }

    pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
        if let Some(keys) = self.slot_to_keys.get_mut(&slot) {
            let now = Instant::now();
            let exp = &self.expirations;
            keys.retain(|k| {
                if let Some(&expire_at) = exp.get(k) {
                    now < expire_at
                } else {
                    true
                }
            });
            keys.len()
        } else {
            0
        }
    }

    pub fn get_keys_in_slot(&mut self, slot: u16, count: usize) -> Vec<Bytes> {
        if let Some(keys) = self.slot_to_keys.get_mut(&slot) {
            let now = Instant::now();
            let exp = &self.expirations;
            keys.retain(|k| {
                if let Some(&expire_at) = exp.get(k) {
                    now < expire_at
                } else {
                    true
                }
            });
            keys.iter().take(count).cloned().collect()
        } else {
            Vec::new()
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
            let slot = crate::router::key_slot(&k);
            if let Some(set) = self.slot_to_keys.get_mut(&slot) {
                set.remove(&k);
            }
            self.entries.remove(&k);
            self.hashes.remove(&k);
            self.expirations.remove(&k);
        }
        count
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len() + self.hashes.len()
    }
}

