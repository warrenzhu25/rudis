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

#[derive(Default)]
pub struct PubSubHub {
    pub channels: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,
    pub patterns: hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>,
    pub clients: hashbrown::HashMap<u64, flume::Sender<Vec<u8>>>,
    pub client_channels: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,
    pub client_patterns: hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,
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
        ch_count + pat_count
    }

    pub fn subscribe(
        &mut self,
        client_id: u64,
        channel: Bytes,
        tx: flume::Sender<Vec<u8>>,
    ) -> usize {
        self.clients.insert(client_id, tx);
        self.channels
            .entry(channel.clone())
            .or_default()
            .insert(client_id);
        self.client_channels
            .entry(client_id)
            .or_default()
            .insert(channel);
        self.total_subscriptions(client_id)
    }

    pub fn unsubscribe(&mut self, client_id: u64, channel: &[u8]) -> usize {
        if let Some(set) = self.channels.get_mut(channel) {
            set.remove(&client_id);
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
        }
        total
    }

    pub fn unsubscribe_all(&mut self, client_id: u64) -> Vec<(Bytes, usize)> {
        let mut res = Vec::new();
        if let Some(ch_set) = self.client_channels.remove(&client_id) {
            for ch in ch_set {
                if let Some(set) = self.channels.get_mut(&ch) {
                    set.remove(&client_id);
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
        }
        res
    }

    pub fn psubscribe(
        &mut self,
        client_id: u64,
        pattern: Bytes,
        tx: flume::Sender<Vec<u8>>,
    ) -> usize {
        self.clients.insert(client_id, tx);
        self.patterns
            .entry(pattern.clone())
            .or_default()
            .insert(client_id);
        self.client_patterns
            .entry(client_id)
            .or_default()
            .insert(pattern);
        self.total_subscriptions(client_id)
    }

    pub fn punsubscribe(&mut self, client_id: u64, pattern: &[u8]) -> usize {
        if let Some(set) = self.patterns.get_mut(pattern) {
            set.remove(&client_id);
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
        }
        total
    }

    pub fn punsubscribe_all(&mut self, client_id: u64) -> Vec<(Bytes, usize)> {
        let mut res = Vec::new();
        if let Some(pat_set) = self.client_patterns.remove(&client_id) {
            for pat in pat_set {
                if let Some(set) = self.patterns.get_mut(&pat) {
                    set.remove(&client_id);
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
        }
        res
    }

    pub fn publish(&self, channel: &[u8], message: &[u8]) -> usize {
        let mut count = 0;

        // 1. Direct channel subscribers
        if let Some(subscribers) = self.channels.get(channel) {
            let mut frame = Vec::new();
            frame.extend_from_slice(b"*3\r\n$7\r\nmessage\r\n$");
            frame.extend_from_slice(channel.len().to_string().as_bytes());
            frame.extend_from_slice(b"\r\n");
            frame.extend_from_slice(channel);
            frame.extend_from_slice(b"\r\n$");
            frame.extend_from_slice(message.len().to_string().as_bytes());
            frame.extend_from_slice(b"\r\n");
            frame.extend_from_slice(message);
            frame.extend_from_slice(b"\r\n");

            for client_id in subscribers {
                if let Some(tx) = self.clients.get(client_id) {
                    if tx.send(frame.clone()).is_ok() {
                        count += 1;
                    }
                }
            }
        }

        // 2. Pattern subscribers
        for (pattern, subscribers) in &self.patterns {
            if glob_match(pattern, channel) {
                let mut frame = Vec::new();
                frame.extend_from_slice(b"*4\r\n$8\r\npmessage\r\n$");
                frame.extend_from_slice(pattern.len().to_string().as_bytes());
                frame.extend_from_slice(b"\r\n");
                frame.extend_from_slice(pattern);
                frame.extend_from_slice(b"\r\n$");
                frame.extend_from_slice(channel.len().to_string().as_bytes());
                frame.extend_from_slice(b"\r\n");
                frame.extend_from_slice(channel);
                frame.extend_from_slice(b"\r\n$");
                frame.extend_from_slice(message.len().to_string().as_bytes());
                frame.extend_from_slice(b"\r\n");
                frame.extend_from_slice(message);
                frame.extend_from_slice(b"\r\n");

                for client_id in subscribers {
                    if let Some(tx) = self.clients.get(client_id) {
                        if tx.send(frame.clone()).is_ok() {
                            count += 1;
                        }
                    }
                }
            }
        }

        count
    }

    pub fn remove_client(&mut self, client_id: u64) {
        self.unsubscribe_all(client_id);
        self.punsubscribe_all(client_id);
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

    pub fn numsub(&self, channel: &[u8]) -> usize {
        self.channels.get(channel).map(|s| s.len()).unwrap_or(0)
    }

    pub fn numpat(&self) -> usize {
        self.patterns.len()
    }
}
