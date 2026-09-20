use bytes::Bytes;

pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_p = None;
    let mut match_t = 0;

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star_p = Some(p);
            p += 1;
            match_t = t;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            match_t += 1;
            t = match_t;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

#[inline]
pub fn build_pubsub_frame(kind: &[u8], channel: &[u8], message: &[u8], is_resp3: bool) -> Bytes {
    let mut buf = Vec::with_capacity(32 + kind.len() + channel.len() + message.len());
    let prefix = if is_resp3 { b">3\r\n" } else { b"*3\r\n" };
    buf.extend_from_slice(prefix);
    buf.extend_from_slice(b"$");
    buf.extend_from_slice(kind.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(kind);
    buf.extend_from_slice(b"\r\n$");
    buf.extend_from_slice(channel.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(channel);
    buf.extend_from_slice(b"\r\n$");
    buf.extend_from_slice(message.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(message);
    buf.extend_from_slice(b"\r\n");
    Bytes::from(buf)
}

#[inline]
pub fn build_pubsub_pframe(
    pattern: &[u8],
    channel: &[u8],
    message: &[u8],
    is_resp3: bool,
) -> Bytes {
    let mut buf = Vec::with_capacity(40 + pattern.len() + channel.len() + message.len());
    let prefix = if is_resp3 { b">4\r\n" } else { b"*4\r\n" };
    buf.extend_from_slice(prefix);
    buf.extend_from_slice(b"$8\r\npmessage\r\n$");
    buf.extend_from_slice(pattern.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(pattern);
    buf.extend_from_slice(b"\r\n$");
    buf.extend_from_slice(channel.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(channel);
    buf.extend_from_slice(b"\r\n$");
    buf.extend_from_slice(message.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(message);
    buf.extend_from_slice(b"\r\n");
    Bytes::from(buf)
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};

pub const PUBSUB_STRIPES: usize = 16;

pub struct ShardedPresenceTable {
    pub channel_stripes: [AtomicU64; PUBSUB_STRIPES],
    pub pattern_presence: AtomicU64,
}

impl Default for ShardedPresenceTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedPresenceTable {
    pub const fn new() -> Self {
        Self {
            channel_stripes: [const { AtomicU64::new(0) }; PUBSUB_STRIPES],
            pattern_presence: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn stripe_for(channel: &[u8]) -> usize {
        (crate::table::hash_key(channel) as usize) % PUBSUB_STRIPES
    }

    #[inline(always)]
    pub fn add_subscriber(&self, shard_id: usize, channel: &[u8]) {
        let stripe = Self::stripe_for(channel);
        self.channel_stripes[stripe].fetch_or(1u64 << shard_id, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn remove_subscriber(&self, shard_id: usize, channel: &[u8]) {
        let stripe = Self::stripe_for(channel);
        self.channel_stripes[stripe].fetch_and(!(1u64 << shard_id), Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn add_pattern_subscriber(&self, shard_id: usize) {
        self.pattern_presence
            .fetch_or(1u64 << shard_id, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn remove_pattern_subscriber(&self, shard_id: usize) {
        self.pattern_presence
            .fetch_and(!(1u64 << shard_id), Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn interested_shards(&self, channel: &[u8]) -> u64 {
        let stripe = Self::stripe_for(channel);
        self.channel_stripes[stripe].load(Ordering::Relaxed)
            | self.pattern_presence.load(Ordering::Relaxed)
    }
}

static PRESENCE_TABLES: LazyLock<RwLock<hashbrown::HashMap<u16, Arc<ShardedPresenceTable>>>> =
    LazyLock::new(|| RwLock::new(hashbrown::HashMap::new()));

pub fn get_presence_table(port: u16) -> Arc<ShardedPresenceTable> {
    let read = PRESENCE_TABLES.read().unwrap();
    if let Some(t) = read.get(&port) {
        return t.clone();
    }
    drop(read);
    let mut write = PRESENCE_TABLES.write().unwrap();
    write
        .entry(port)
        .or_insert_with(|| Arc::new(ShardedPresenceTable::new()))
        .clone()
}

#[derive(Default)]
pub struct PubSubHub {
    pub channels: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,
    pub patterns: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,
    pub shard_channels: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,
    pub clients: hashbrown::HashMap<u64, flume::Sender<Bytes>>,
    pub client_channels: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,
    pub client_patterns: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,
    pub client_shard_channels: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,
    pub client_resp3: hashbrown::HashSet<u64>,
    pub stripe_counts: [usize; PUBSUB_STRIPES],
    pub total_patterns: usize,
}

impl PubSubHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn total_subscriptions(&self, client_id: u64) -> usize {
        let ch_count = self
            .client_channels
            .get(&client_id)
            .map(|s| s.len())
            .unwrap_or(0);
        let pat_count = self
            .client_patterns
            .get(&client_id)
            .map(|s| s.len())
            .unwrap_or(0);
        let shard_count = self
            .client_shard_channels
            .get(&client_id)
            .map(|s| s.len())
            .unwrap_or(0);
        ch_count + pat_count + shard_count
    }

    pub fn subscribe_with_presence(
        &mut self,
        client_id: u64,
        channel: Bytes,
        tx: flume::Sender<Bytes>,
        is_resp3: bool,
        presence_info: Option<(usize, &ShardedPresenceTable)>,
    ) -> usize {
        self.clients.insert(client_id, tx);
        if is_resp3 {
            self.client_resp3.insert(client_id);
        } else {
            self.client_resp3.remove(&client_id);
        }
        let newly_inserted = self
            .channels
            .entry(channel.clone())
            .or_default()
            .insert(client_id);
        if newly_inserted {
            let stripe = ShardedPresenceTable::stripe_for(&channel);
            if self.stripe_counts[stripe] == 0
                && let Some((shard_id, presence)) = presence_info
            {
                presence.add_subscriber(shard_id, &channel);
            }
            self.stripe_counts[stripe] += 1;
        }
        self.client_channels
            .entry(client_id)
            .or_default()
            .insert(channel);
        self.total_subscriptions(client_id)
    }

    pub fn subscribe(
        &mut self,
        client_id: u64,
        channel: Bytes,
        tx: flume::Sender<Bytes>,
        is_resp3: bool,
    ) -> usize {
        self.subscribe_with_presence(client_id, channel, tx, is_resp3, None)
    }

    pub fn unsubscribe_with_presence(
        &mut self,
        client_id: u64,
        channel: &[u8],
        presence_info: Option<(usize, &ShardedPresenceTable)>,
    ) -> usize {
        if let Some(set) = self.channels.get_mut(channel) {
            if set.remove(&client_id) {
                let stripe = ShardedPresenceTable::stripe_for(channel);
                self.stripe_counts[stripe] = self.stripe_counts[stripe].saturating_sub(1);
                if self.stripe_counts[stripe] == 0
                    && let Some((shard_id, presence)) = presence_info
                {
                    presence.remove_subscriber(shard_id, channel);
                }
            }
            if set.is_empty() {
                self.channels.remove(channel);
            }
        }
        if let Some(ch_set) = self.client_channels.get_mut(&client_id) {
            ch_set.remove(channel);
            if ch_set.is_empty() {
                self.client_channels.remove(&client_id);
            }
        }
        let total = self.total_subscriptions(client_id);
        if total == 0 {
            self.clients.remove(&client_id);
            self.client_resp3.remove(&client_id);
        }
        total
    }

    pub fn unsubscribe(&mut self, client_id: u64, channel: &[u8]) -> usize {
        self.unsubscribe_with_presence(client_id, channel, None)
    }

    pub fn unsubscribe_all_with_presence(
        &mut self,
        client_id: u64,
        presence_info: Option<(usize, &ShardedPresenceTable)>,
    ) -> Vec<(Bytes, usize)> {
        let mut res = Vec::new();
        if let Some(ch_set) = self.client_channels.remove(&client_id) {
            for ch in ch_set {
                if let Some(set) = self.channels.get_mut(&ch) {
                    if set.remove(&client_id) {
                        let stripe = ShardedPresenceTable::stripe_for(&ch);
                        self.stripe_counts[stripe] = self.stripe_counts[stripe].saturating_sub(1);
                        if self.stripe_counts[stripe] == 0
                            && let Some((shard_id, presence)) = presence_info
                        {
                            presence.remove_subscriber(shard_id, &ch);
                        }
                    }
                    if set.is_empty() {
                        self.channels.remove(&ch);
                    }
                }
                let total = self.total_subscriptions(client_id);
                res.push((ch, total));
            }
        }
        if self.total_subscriptions(client_id) == 0 {
            self.clients.remove(&client_id);
            self.client_resp3.remove(&client_id);
        }
        res
    }

    pub fn unsubscribe_all(&mut self, client_id: u64) -> Vec<(Bytes, usize)> {
        self.unsubscribe_all_with_presence(client_id, None)
    }

    pub fn psubscribe_with_presence(
        &mut self,
        client_id: u64,
        pattern: Bytes,
        tx: flume::Sender<Bytes>,
        is_resp3: bool,
        presence_info: Option<(usize, &ShardedPresenceTable)>,
    ) -> usize {
        self.clients.insert(client_id, tx);
        if is_resp3 {
            self.client_resp3.insert(client_id);
        } else {
            self.client_resp3.remove(&client_id);
        }
        let newly_inserted = self
            .patterns
            .entry(pattern.clone())
            .or_default()
            .insert(client_id);
        if newly_inserted {
            if self.total_patterns == 0
                && let Some((shard_id, presence)) = presence_info
            {
                presence.add_pattern_subscriber(shard_id);
            }
            self.total_patterns += 1;
        }
        self.client_patterns
            .entry(client_id)
            .or_default()
            .insert(pattern);
        self.total_subscriptions(client_id)
    }

    pub fn psubscribe(
        &mut self,
        client_id: u64,
        pattern: Bytes,
        tx: flume::Sender<Bytes>,
        is_resp3: bool,
    ) -> usize {
        self.psubscribe_with_presence(client_id, pattern, tx, is_resp3, None)
    }

    pub fn punsubscribe_with_presence(
        &mut self,
        client_id: u64,
        pattern: &[u8],
        presence_info: Option<(usize, &ShardedPresenceTable)>,
    ) -> usize {
        if let Some(set) = self.patterns.get_mut(pattern) {
            if set.remove(&client_id) {
                self.total_patterns = self.total_patterns.saturating_sub(1);
                if self.total_patterns == 0
                    && let Some((shard_id, presence)) = presence_info
                {
                    presence.remove_pattern_subscriber(shard_id);
                }
            }
            if set.is_empty() {
                self.patterns.remove(pattern);
            }
        }
        if let Some(pat_set) = self.client_patterns.get_mut(&client_id) {
            pat_set.remove(pattern);
            if pat_set.is_empty() {
                self.client_patterns.remove(&client_id);
            }
        }
        let total = self.total_subscriptions(client_id);
        if total == 0 {
            self.clients.remove(&client_id);
            self.client_resp3.remove(&client_id);
        }
        total
    }

    pub fn punsubscribe(&mut self, client_id: u64, pattern: &[u8]) -> usize {
        self.punsubscribe_with_presence(client_id, pattern, None)
    }

    pub fn punsubscribe_all_with_presence(
        &mut self,
        client_id: u64,
        presence_info: Option<(usize, &ShardedPresenceTable)>,
    ) -> Vec<(Bytes, usize)> {
        let mut res = Vec::new();
        if let Some(pat_set) = self.client_patterns.remove(&client_id) {
            for pat in pat_set {
                if let Some(set) = self.patterns.get_mut(&pat) {
                    if set.remove(&client_id) {
                        self.total_patterns = self.total_patterns.saturating_sub(1);
                        if self.total_patterns == 0
                            && let Some((shard_id, presence)) = presence_info
                        {
                            presence.remove_pattern_subscriber(shard_id);
                        }
                    }
                    if set.is_empty() {
                        self.patterns.remove(&pat);
                    }
                }
                let total = self.total_subscriptions(client_id);
                res.push((pat, total));
            }
        }
        if self.total_subscriptions(client_id) == 0 {
            self.clients.remove(&client_id);
            self.client_resp3.remove(&client_id);
        }
        res
    }

    pub fn punsubscribe_all(&mut self, client_id: u64) -> Vec<(Bytes, usize)> {
        self.punsubscribe_all_with_presence(client_id, None)
    }

    pub fn publish(&self, channel: &[u8], message: &[u8]) -> usize {
        let mut count = 0;

        // 1. Direct channel subscribers
        if let Some(subscribers) = self.channels.get(channel) {
            let mut frame_resp2: Option<Bytes> = None;
            let mut frame_resp3: Option<Bytes> = None;

            for client_id in subscribers {
                let is_resp3 = self.client_resp3.contains(client_id);
                let frame = if is_resp3 {
                    frame_resp3.get_or_insert_with(|| {
                        build_pubsub_frame(b"message", channel, message, true)
                    })
                } else {
                    frame_resp2.get_or_insert_with(|| {
                        build_pubsub_frame(b"message", channel, message, false)
                    })
                };
                if let Some(tx) = self.clients.get(client_id)
                    && tx.try_send(frame.clone()).is_ok()
                {
                    count += 1;
                }
            }
        }

        // 2. Pattern subscribers
        for (pattern, subscribers) in &self.patterns {
            if glob_match(pattern, channel) {
                let mut frame_resp2: Option<Bytes> = None;
                let mut frame_resp3: Option<Bytes> = None;

                for client_id in subscribers {
                    let is_resp3 = self.client_resp3.contains(client_id);
                    let frame = if is_resp3 {
                        frame_resp3.get_or_insert_with(|| {
                            build_pubsub_pframe(pattern, channel, message, true)
                        })
                    } else {
                        frame_resp2.get_or_insert_with(|| {
                            build_pubsub_pframe(pattern, channel, message, false)
                        })
                    };
                    if let Some(tx) = self.clients.get(client_id)
                        && tx.try_send(frame.clone()).is_ok()
                    {
                        count += 1;
                    }
                }
            }
        }

        count
    }

    pub fn ssubscribe(
        &mut self,
        client_id: u64,
        channel: Bytes,
        tx: flume::Sender<Bytes>,
        is_resp3: bool,
    ) -> usize {
        self.clients.insert(client_id, tx);
        if is_resp3 {
            self.client_resp3.insert(client_id);
        } else {
            self.client_resp3.remove(&client_id);
        }
        self.shard_channels
            .entry(channel.clone())
            .or_default()
            .insert(client_id);
        self.client_shard_channels
            .entry(client_id)
            .or_default()
            .insert(channel);
        self.total_subscriptions(client_id)
    }

    pub fn sunsubscribe(&mut self, client_id: u64, channel: &[u8]) -> usize {
        if let Some(set) = self.shard_channels.get_mut(channel) {
            set.remove(&client_id);
            if set.is_empty() {
                self.shard_channels.remove(channel);
            }
        }
        if let Some(ch_set) = self.client_shard_channels.get_mut(&client_id) {
            ch_set.remove(channel);
            if ch_set.is_empty() {
                self.client_shard_channels.remove(&client_id);
            }
        }
        let total = self.total_subscriptions(client_id);
        if total == 0 {
            self.clients.remove(&client_id);
            self.client_resp3.remove(&client_id);
        }
        total
    }

    pub fn sunsubscribe_all(&mut self, client_id: u64) -> Vec<(Bytes, usize)> {
        let mut res = Vec::new();
        if let Some(ch_set) = self.client_shard_channels.remove(&client_id) {
            for ch in ch_set {
                if let Some(set) = self.shard_channels.get_mut(&ch) {
                    set.remove(&client_id);
                    if set.is_empty() {
                        self.shard_channels.remove(&ch);
                    }
                }
                let total = self.total_subscriptions(client_id);
                res.push((ch, total));
            }
        }
        if self.total_subscriptions(client_id) == 0 {
            self.clients.remove(&client_id);
            self.client_resp3.remove(&client_id);
        }
        res
    }

    pub fn spublish(&self, channel: &[u8], message: &[u8]) -> usize {
        let mut count = 0;
        if let Some(subscribers) = self.shard_channels.get(channel) {
            let mut frame_resp2: Option<Bytes> = None;
            let mut frame_resp3: Option<Bytes> = None;

            for client_id in subscribers {
                let is_resp3 = self.client_resp3.contains(client_id);
                let frame = if is_resp3 {
                    frame_resp3.get_or_insert_with(|| {
                        build_pubsub_frame(b"smessage", channel, message, true)
                    })
                } else {
                    frame_resp2.get_or_insert_with(|| {
                        build_pubsub_frame(b"smessage", channel, message, false)
                    })
                };
                if let Some(tx) = self.clients.get(client_id)
                    && tx.try_send(frame.clone()).is_ok()
                {
                    count += 1;
                }
            }
        }
        count
    }

    pub fn remove_client_with_presence(
        &mut self,
        client_id: u64,
        presence_info: Option<(usize, &ShardedPresenceTable)>,
    ) {
        self.unsubscribe_all_with_presence(client_id, presence_info);
        self.punsubscribe_all_with_presence(client_id, presence_info);
        self.sunsubscribe_all(client_id);
        self.client_resp3.remove(&client_id);
        self.clients.remove(&client_id);
    }

    pub fn remove_client(&mut self, client_id: u64) {
        self.remove_client_with_presence(client_id, None);
    }

    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        let mut list = Vec::new();
        for ch in self.channels.keys() {
            if let Some(pat) = pattern {
                if glob_match(pat, ch) {
                    list.push(ch.clone());
                }
            } else {
                list.push(ch.clone());
            }
        }
        list
    }

    pub fn shard_channels(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        let mut list = Vec::new();
        for ch in self.shard_channels.keys() {
            if let Some(pat) = pattern {
                if glob_match(pat, ch) {
                    list.push(ch.clone());
                }
            } else {
                list.push(ch.clone());
            }
        }
        list
    }

    pub fn numsub(&self, channel: &[u8]) -> usize {
        self.channels.get(channel).map(|s| s.len()).unwrap_or(0)
    }

    pub fn shard_numsub(&self, channel: &[u8]) -> usize {
        self.shard_channels
            .get(channel)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    pub fn numpat(&self) -> usize {
        self.patterns.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pubsub_resp2_and_resp3_push_frames() {
        let mut hub = PubSubHub::new();
        let (tx1, rx1) = flume::unbounded();
        let (tx2, rx2) = flume::unbounded();
        let (tx3, rx3) = flume::unbounded();
        let (tx4, rx4) = flume::unbounded();

        // Client 1: RESP2 subscriber
        hub.subscribe(1, Bytes::from_static(b"news"), tx1, false);
        // Client 2: RESP3 subscriber
        hub.subscribe(2, Bytes::from_static(b"news"), tx2, true);
        // Client 3: RESP3 pattern subscriber
        hub.psubscribe(3, Bytes::from_static(b"news*"), tx3, true);
        // Client 4: RESP2 pattern subscriber
        hub.psubscribe(4, Bytes::from_static(b"news*"), tx4, false);

        let count = hub.publish(b"news", b"breaking");
        assert_eq!(count, 4);

        // Client 1 receives RESP2 array: *3
        let msg1 = rx1.try_recv().unwrap();
        assert!(msg1.starts_with(b"*3\r\n$7\r\nmessage\r\n"));

        // Client 2 receives RESP3 push: >3
        let msg2 = rx2.try_recv().unwrap();
        assert!(msg2.starts_with(b">3\r\n$7\r\nmessage\r\n"));

        // Client 3 receives RESP3 pattern push: >4
        let msg3 = rx3.try_recv().unwrap();
        assert!(msg3.starts_with(b">4\r\n$8\r\npmessage\r\n"));

        // Client 4 receives RESP2 pattern array: *4
        let msg4 = rx4.try_recv().unwrap();
        assert!(msg4.starts_with(b"*4\r\n$8\r\npmessage\r\n"));

        // Unsubscribe cleans up client_resp3
        hub.unsubscribe(2, b"news");
        assert!(!hub.client_resp3.contains(&2));
        hub.punsubscribe(3, b"news*");
        assert!(!hub.client_resp3.contains(&3));
    }

    #[test]
    fn test_pubsub_slow_consumer_backpressure_and_zero_copy() {
        let mut hub = PubSubHub::new();
        let (tx1, rx1) = flume::bounded(2);
        let (tx2, rx2) = flume::bounded(10);

        hub.subscribe(1, Bytes::from_static(b"fast_lane"), tx1, false);
        hub.subscribe(2, Bytes::from_static(b"fast_lane"), tx2, false);

        // First 2 publishes fill client 1's queue
        assert_eq!(hub.publish(b"fast_lane", b"msg1"), 2);
        assert_eq!(hub.publish(b"fast_lane", b"msg2"), 2);

        // Third publish: client 1 queue is full (backpressure), client 2 accepts
        assert_eq!(hub.publish(b"fast_lane", b"msg3"), 1);

        // Verify client 1 has msg1 and msg2, but msg3 was dropped due to backpressure
        assert_eq!(
            rx1.try_recv().unwrap(),
            Bytes::from_static(b"*3\r\n$7\r\nmessage\r\n$9\r\nfast_lane\r\n$4\r\nmsg1\r\n")
        );
        assert_eq!(
            rx1.try_recv().unwrap(),
            Bytes::from_static(b"*3\r\n$7\r\nmessage\r\n$9\r\nfast_lane\r\n$4\r\nmsg2\r\n")
        );
        assert!(rx1.try_recv().is_err());

        // Verify client 2 received all three messages
        assert_eq!(
            rx2.try_recv().unwrap(),
            Bytes::from_static(b"*3\r\n$7\r\nmessage\r\n$9\r\nfast_lane\r\n$4\r\nmsg1\r\n")
        );
        assert_eq!(
            rx2.try_recv().unwrap(),
            Bytes::from_static(b"*3\r\n$7\r\nmessage\r\n$9\r\nfast_lane\r\n$4\r\nmsg2\r\n")
        );
        assert_eq!(
            rx2.try_recv().unwrap(),
            Bytes::from_static(b"*3\r\n$7\r\nmessage\r\n$9\r\nfast_lane\r\n$4\r\nmsg3\r\n")
        );
    }

    #[test]
    fn test_sharded_presence_table_bitmask_tracking() {
        let presence = ShardedPresenceTable::new();
        let mut hub_shard0 = PubSubHub::new();
        let mut hub_shard1 = PubSubHub::new();

        let (tx0, _rx0) = flume::bounded(10);
        let (tx1, _rx1) = flume::bounded(10);

        // Before any subscriptions, no shards are interested
        assert_eq!(presence.interested_shards(b"sports"), 0);

        // Shard 0 subscribes to "sports"
        hub_shard0.subscribe_with_presence(
            1,
            Bytes::from_static(b"sports"),
            tx0.clone(),
            false,
            Some((0, &presence)),
        );
        let mask = presence.interested_shards(b"sports");
        assert_eq!(mask & (1 << 0), 1 << 0, "Shard 0 should be marked present");
        assert_eq!(mask & (1 << 1), 0, "Shard 1 should not be marked present");

        // Shard 1 subscribes to "sports" as well
        hub_shard1.subscribe_with_presence(
            2,
            Bytes::from_static(b"sports"),
            tx1,
            false,
            Some((1, &presence)),
        );
        let mask = presence.interested_shards(b"sports");
        assert_eq!(mask & ((1 << 0) | (1 << 1)), (1 << 0) | (1 << 1));

        // Shard 0 unsubscribes
        hub_shard0.unsubscribe_with_presence(1, b"sports", Some((0, &presence)));
        let mask = presence.interested_shards(b"sports");
        assert_eq!(mask & (1 << 0), 0, "Shard 0 bit should be cleared");
        assert_eq!(mask & (1 << 1), 1 << 1, "Shard 1 bit should remain");

        // Pattern subscription on shard 0 marks pattern presence for all channels
        hub_shard0.psubscribe_with_presence(
            3,
            Bytes::from_static(b"news.*"),
            tx0,
            false,
            Some((0, &presence)),
        );
        assert_eq!(
            presence.interested_shards(b"any_channel") & (1 << 0),
            1 << 0
        );

        // Unsubscribe pattern clears pattern presence
        hub_shard0.punsubscribe_with_presence(3, b"news.*", Some((0, &presence)));
        assert_eq!(presence.interested_shards(b"any_channel") & (1 << 0), 0);
    }

    #[test]
    fn test_sharded_pubsub_hub_ssubscribe_spublish_sunsubscribe() {
        let mut hub = PubSubHub::new();
        let (tx1, rx1) = flume::unbounded();
        let (tx2, rx2) = flume::unbounded();
        let (tx_pat, rx_pat) = flume::unbounded();

        // Client 1: RESP2 subscriber to shard channel "orders:eu"
        let count1 = hub.ssubscribe(1, Bytes::from_static(b"orders:eu"), tx1, false);
        assert_eq!(count1, 1);

        // Client 2: RESP3 subscriber to shard channel "orders:eu"
        let count2 = hub.ssubscribe(2, Bytes::from_static(b"orders:eu"), tx2, true);
        assert_eq!(count2, 1);

        // Client 3: pattern subscriber to "orders:*"
        hub.psubscribe(3, Bytes::from_static(b"orders:*"), tx_pat, false);

        // SPUBLISH to "orders:eu"
        let recv_count = hub.spublish(b"orders:eu", b"payload_123");
        // Only the 2 direct shard subscribers receive it; pattern subscriber does not!
        assert_eq!(recv_count, 2);

        // Check client 1 received RESP2 smessage frame: *3\r\n$8\r\nsmessage\r\n...
        let msg1 = rx1.try_recv().unwrap();
        assert_eq!(
            msg1,
            Bytes::from_static(
                b"*3\r\n$8\r\nsmessage\r\n$9\r\norders:eu\r\n$11\r\npayload_123\r\n"
            )
        );

        // Check client 2 received RESP3 smessage frame: >3\r\n$8\r\nsmessage\r\n...
        let msg2 = rx2.try_recv().unwrap();
        assert_eq!(
            msg2,
            Bytes::from_static(
                b">3\r\n$8\r\nsmessage\r\n$9\r\norders:eu\r\n$11\r\npayload_123\r\n"
            )
        );

        // Check pattern subscriber received nothing
        assert!(rx_pat.try_recv().is_err());

        // Test shard_channels and shard_numsub
        let active = hub.shard_channels(None);
        assert_eq!(active, vec![Bytes::from_static(b"orders:eu")]);
        assert_eq!(hub.shard_numsub(b"orders:eu"), 2);
        assert_eq!(hub.shard_numsub(b"orders:us"), 0);

        // SUNSUBSCRIBE
        let remaining1 = hub.sunsubscribe(1, b"orders:eu");
        assert_eq!(remaining1, 0);
        assert_eq!(hub.shard_numsub(b"orders:eu"), 1);

        let remaining2 = hub.sunsubscribe(2, b"orders:eu");
        assert_eq!(remaining2, 0);
        assert_eq!(hub.shard_numsub(b"orders:eu"), 0);
        assert!(hub.shard_channels(None).is_empty());
    }
}
