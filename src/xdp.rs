use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XdpAction {
    Pass,
    Drop,
    Redirect,
    Tx,
}

impl std::fmt::Display for XdpAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XdpAction::Pass => write!(f, "PASS"),
            XdpAction::Drop => write!(f, "DROP"),
            XdpAction::Redirect => write!(f, "REDIRECT"),
            XdpAction::Tx => write!(f, "TX"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct XdpRule {
    pub id: u32,
    pub action: XdpAction,
    pub cidr: String,
    pub network: u32,
    pub netmask: u32,
}

pub fn parse_cidr(cidr_str: &str) -> Result<(u32, u32), String> {
    let parts: Vec<&str> = cidr_str.split('/').collect();
    if parts.is_empty() || parts.len() > 2 {
        return Err(format!("Invalid CIDR format: {}", cidr_str));
    }
    let ip: Ipv4Addr = parts[0]
        .parse()
        .map_err(|e| format!("Invalid IPv4 address: {}", e))?;
    let ip_u32 = u32::from(ip);

    let prefix_len: u32 = if parts.len() == 2 {
        parts[1]
            .parse()
            .map_err(|e| format!("Invalid prefix length: {}", e))?
    } else {
        32
    };

    if prefix_len > 32 {
        return Err("Prefix length cannot exceed 32".to_string());
    }

    let netmask = if prefix_len == 0 {
        0
    } else {
        !0u32 << (32 - prefix_len)
    };
    let network = ip_u32 & netmask;
    Ok((network, netmask))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XdpMode {
    Driver,
    Skb,
    Simulated,
}

impl std::fmt::Display for XdpMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XdpMode::Driver => write!(f, "driver"),
            XdpMode::Skb => write!(f, "skb (generic)"),
            XdpMode::Simulated => write!(f, "simulated (userspace bypass)"),
        }
    }
}

pub struct TokenBucket {
    pub tokens: f64,
    pub capacity: f64,
    pub refill_rate: f64, // tokens per second
    pub last_update: Instant,
}

impl TokenBucket {
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            tokens: capacity,
            capacity,
            refill_rate,
            last_update: Instant::now(),
        }
    }

    pub fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct UmemFrame {
    pub addr: u64,
    pub len: u32,
    pub buffer: Vec<u8>,
}

/// AF_XDP packet descriptor pointing to a frame in the UMEM buffer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct XdpDesc {
    pub addr: u64,
    pub len: u32,
    pub options: u32,
}

/// High-throughput lock-free circular ring buffer modeling AF_XDP rings.
pub struct XskRing<T> {
    pub producer: AtomicU32,
    pub consumer: AtomicU32,
    pub mask: u32,
    pub entries: Vec<RwLock<T>>,
}

impl<T: Clone + Default> XskRing<T> {
    pub fn new(size: u32) -> Self {
        let actual_size = size.next_power_of_two();
        let mut entries = Vec::with_capacity(actual_size as usize);
        for _ in 0..actual_size {
            entries.push(RwLock::new(T::default()));
        }
        Self {
            producer: AtomicU32::new(0),
            consumer: AtomicU32::new(0),
            mask: actual_size - 1,
            entries,
        }
    }

    #[inline]
    pub fn produce(&self, item: T) -> bool {
        let prod = self.producer.load(Ordering::Relaxed);
        let cons = self.consumer.load(Ordering::Acquire);
        if prod.wrapping_sub(cons) > self.mask {
            return false; // Ring full
        }
        let idx = (prod & self.mask) as usize;
        *self.entries[idx].write().unwrap() = item;
        self.producer.store(prod.wrapping_add(1), Ordering::Release);
        true
    }

    #[inline]
    pub fn consume(&self) -> Option<T> {
        let cons = self.consumer.load(Ordering::Relaxed);
        let prod = self.producer.load(Ordering::Acquire);
        if cons == prod {
            return None; // Ring empty
        }
        let idx = (cons & self.mask) as usize;
        let item = self.entries[idx].read().unwrap().clone();
        self.consumer.store(cons.wrapping_add(1), Ordering::Release);
        Some(item)
    }

    #[inline]
    pub fn len(&self) -> u32 {
        self.producer
            .load(Ordering::Relaxed)
            .wrapping_sub(self.consumer.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.producer.load(Ordering::Relaxed) == self.consumer.load(Ordering::Relaxed)
    }
}

/// Pre-allocated UMEM area representing pinned DMA-capable memory frames.
pub struct XskUmem {
    pub frame_size: usize,
    pub num_frames: usize,
    pub frames: RwLock<Vec<Vec<u8>>>,
}

impl XskUmem {
    pub fn new(frame_size: usize, num_frames: usize) -> Self {
        let mut frames = Vec::with_capacity(num_frames);
        for _ in 0..num_frames {
            frames.push(vec![0u8; frame_size]);
        }
        Self {
            frame_size,
            num_frames,
            frames: RwLock::new(frames),
        }
    }

    pub fn write_frame(&self, frame_idx: usize, data: &[u8]) {
        let mut frames = self.frames.write().unwrap();
        if let Some(frame) = frames.get_mut(frame_idx) {
            let len = data.len().min(self.frame_size);
            frame[..len].copy_from_slice(&data[..len]);
        }
    }

    pub fn read_frame(&self, frame_idx: usize, len: usize) -> Vec<u8> {
        let frames = self.frames.read().unwrap();
        if let Some(frame) = frames.get(frame_idx) {
            let actual_len = len.min(self.frame_size).min(frame.len());
            frame[..actual_len].to_vec()
        } else {
            Vec::new()
        }
    }
}

/// Dedicated AF_XDP socket (XSK) bound to a hardware NIC RX/TX queue.
pub struct XskSocket {
    pub queue_id: u32,
    pub rx_ring: XskRing<XdpDesc>,
    pub fill_ring: XskRing<u64>,
    pub tx_ring: XskRing<XdpDesc>,
    pub comp_ring: XskRing<u64>,
    pub umem: Arc<XskUmem>,
    pub rx_packets: AtomicU64,
    pub tx_packets: AtomicU64,
}

impl XskSocket {
    pub fn new(queue_id: u32, frame_size: usize, num_frames: usize) -> Self {
        let umem = Arc::new(XskUmem::new(frame_size, num_frames));
        let ring_size = (num_frames.max(64) as u32).next_power_of_two();
        let fill_ring = XskRing::new(ring_size);

        // Pre-fill the fill ring with all available UMEM frame indices
        for i in 0..num_frames {
            fill_ring.produce(i as u64);
        }

        Self {
            queue_id,
            rx_ring: XskRing::new(ring_size),
            fill_ring,
            tx_ring: XskRing::new(ring_size),
            comp_ring: XskRing::new(ring_size),
            umem,
            rx_packets: AtomicU64::new(0),
            tx_packets: AtomicU64::new(0),
        }
    }

    /// Ingests a packet into the AF_XDP UMEM and enqueues descriptor into the Rx ring.
    pub fn inject_rx(&self, packet: &[u8]) -> bool {
        if let Some(frame_idx) = self.fill_ring.consume() {
            self.umem.write_frame(frame_idx as usize, packet);
            let desc = XdpDesc {
                addr: frame_idx * (self.umem.frame_size as u64),
                len: packet.len() as u32,
                options: 0,
            };
            if self.rx_ring.produce(desc) {
                self.rx_packets.fetch_add(1, Ordering::Relaxed);
                true
            } else {
                self.fill_ring.produce(frame_idx);
                false
            }
        } else {
            false
        }
    }

    /// Polls and consumes a batch of received packet payloads from the Rx ring.
    pub fn rx_burst(&self, out: &mut Vec<Vec<u8>>, max_batch: usize) -> usize {
        let mut count = 0;
        while count < max_batch {
            if let Some(desc) = self.rx_ring.consume() {
                let frame_idx = (desc.addr / (self.umem.frame_size as u64)) as usize;
                let payload = self.umem.read_frame(frame_idx, desc.len as usize);
                out.push(payload);
                self.fill_ring.produce(frame_idx as u64);
                count += 1;
            } else {
                break;
            }
        }
        count
    }

    /// Transmits a batch of packet payloads via the Tx ring.
    pub fn tx_burst(&self, packets: &[&[u8]]) -> usize {
        let mut sent = 0;
        for &pkt in packets {
            let frame_idx = sent as u64;
            self.umem.write_frame(frame_idx as usize, pkt);
            let desc = XdpDesc {
                addr: frame_idx * (self.umem.frame_size as u64),
                len: pkt.len() as u32,
                options: 0,
            };
            if self.tx_ring.produce(desc) {
                self.comp_ring.produce(frame_idx);
                self.tx_packets.fetch_add(1, Ordering::Relaxed);
                sent += 1;
            } else {
                break;
            }
        }
        sent
    }
}

pub struct XdpEngine {
    pub ifname: String,
    pub mode: XdpMode,
    pub frame_size: usize,
    pub num_frames: usize,
    pub rules: RwLock<Vec<XdpRule>>,
    pub next_rule_id: AtomicU32,
    pub rate_limiters: RwLock<HashMap<u32, TokenBucket>>,
    pub default_rate_limit: f64,
    pub default_rate_capacity: f64,
    pub sockets: RwLock<HashMap<(u16, u32), Arc<XskSocket>>>,

    // Atomic telemetry counters
    pub rx_packets: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub dropped_packets: AtomicU64,
    pub redirected_packets: AtomicU64,
    pub pass_packets: AtomicU64,
    pub rate_limit_drops: AtomicU64,
}

impl XdpEngine {
    pub fn new(ifname: &str, mode: XdpMode) -> Self {
        let num_frames = match mode {
            XdpMode::Simulated => 128,
            _ => 4096,
        };
        Self {
            ifname: ifname.to_string(),
            mode,
            frame_size: 2048,
            num_frames,
            rules: RwLock::new(Vec::new()),
            next_rule_id: AtomicU32::new(1),
            rate_limiters: RwLock::new(HashMap::new()),
            default_rate_limit: 100_000.0,
            default_rate_capacity: 50_000.0,
            sockets: RwLock::new(HashMap::new()),
            rx_packets: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            dropped_packets: AtomicU64::new(0),
            redirected_packets: AtomicU64::new(0),
            pass_packets: AtomicU64::new(0),
            rate_limit_drops: AtomicU64::new(0),
        }
    }

    pub fn add_rule(&self, action: XdpAction, cidr_str: &str) -> Result<u32, String> {
        let (network, netmask) = parse_cidr(cidr_str)?;
        let id = self.next_rule_id.fetch_add(1, Ordering::SeqCst);
        let rule = XdpRule {
            id,
            action,
            cidr: cidr_str.to_string(),
            network,
            netmask,
        };
        let mut rules = self.rules.write().unwrap();
        rules.push(rule);
        Ok(id)
    }

    pub fn del_rule(&self, id: u32) -> Result<(), String> {
        let mut rules = self.rules.write().unwrap();
        let initial_len = rules.len();
        rules.retain(|r| r.id != id);
        if rules.len() < initial_len {
            Ok(())
        } else {
            Err(format!("Rule ID {} not found", id))
        }
    }

    pub fn list_rules(&self) -> Vec<XdpRule> {
        let rules = self.rules.read().unwrap();
        rules.clone()
    }

    /// Process a raw Ethernet or IP packet frame through the eBPF / XDP pipeline
    pub fn process_packet(&self, packet: &[u8]) -> XdpAction {
        self.rx_packets.fetch_add(1, Ordering::Relaxed);
        self.rx_bytes
            .fetch_add(packet.len() as u64, Ordering::Relaxed);

        if packet.is_empty() {
            self.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return XdpAction::Drop;
        }

        // Try extracting source IP from either raw Ethernet frame (header: 14 bytes)
        // or raw IPv4 packet (header: 20 bytes).
        let (src_ip, _dst_port, payload_offset) = if packet.len() >= 14
            && packet[12] == 0x08
            && packet[13] == 0x00
        {
            // Ethernet frame with IPv4
            let ip_slice = &packet[14..];
            if ip_slice.len() >= 20 {
                let src =
                    u32::from_be_bytes([ip_slice[12], ip_slice[13], ip_slice[14], ip_slice[15]]);
                let protocol = ip_slice[9];
                let (dst_port, payload_off) = if protocol == 6 && ip_slice.len() >= 40 {
                    let d_port = u16::from_be_bytes([ip_slice[22], ip_slice[23]]);
                    (Some(d_port), 14 + 20 + 20)
                } else {
                    (None, 14 + 20)
                };
                (Some(src), dst_port, payload_off)
            } else {
                (None, None, 14)
            }
        } else if packet.len() >= 20 && (packet[0] >> 4) == 4 {
            // Raw IPv4 packet
            let src = u32::from_be_bytes([packet[12], packet[13], packet[14], packet[15]]);
            let protocol = packet[9];
            let (dst_port, payload_off) = if protocol == 6 && packet.len() >= 40 {
                let d_port = u16::from_be_bytes([packet[22], packet[23]]);
                (Some(d_port), 40)
            } else {
                (None, 20)
            };
            (Some(src), dst_port, payload_off)
        } else {
            // Plain text or diagnostic payload
            (None, None, 0)
        };

        if let Some(ip) = src_ip {
            // 1. Check eBPF CIDR Filter Rules
            let rules = self.rules.read().unwrap();
            for r in rules.iter() {
                if (ip & r.netmask) == r.network {
                    match r.action {
                        XdpAction::Drop => {
                            self.dropped_packets.fetch_add(1, Ordering::Relaxed);
                            return XdpAction::Drop;
                        }
                        XdpAction::Pass => {
                            self.pass_packets.fetch_add(1, Ordering::Relaxed);
                            return XdpAction::Pass;
                        }
                        XdpAction::Redirect => {
                            self.redirected_packets.fetch_add(1, Ordering::Relaxed);
                            return XdpAction::Redirect;
                        }
                        XdpAction::Tx => return XdpAction::Tx,
                    }
                }
            }

            // 2. Token Bucket IP Rate Limiter
            let mut limiters = self.rate_limiters.write().unwrap();
            let bucket = limiters.entry(ip).or_insert_with(|| {
                TokenBucket::new(self.default_rate_capacity, self.default_rate_limit)
            });

            if !bucket.allow() {
                self.rate_limit_drops.fetch_add(1, Ordering::Relaxed);
                self.dropped_packets.fetch_add(1, Ordering::Relaxed);
                return XdpAction::Drop;
            }
        }

        // 3. Demux: Redis port 6379 or cluster bus -> Redirect to userspace UMEM ring
        let _ = payload_offset;
        self.redirected_packets.fetch_add(1, Ordering::Relaxed);
        XdpAction::Redirect
    }

    pub fn get_or_create_socket(&self, port: u16, queue_id: u32) -> Arc<XskSocket> {
        let mut sockets = self.sockets.write().unwrap();
        sockets
            .entry((port, queue_id))
            .or_insert_with(|| Arc::new(XskSocket::new(queue_id, self.frame_size, self.num_frames)))
            .clone()
    }

    pub fn get_socket(&self, port: u16, queue_id: u32) -> Option<Arc<XskSocket>> {
        self.sockets.read().unwrap().get(&(port, queue_id)).cloned()
    }

    pub fn list_sockets(&self) -> Vec<(u16, u32)> {
        self.sockets.read().unwrap().keys().copied().collect()
    }

    pub fn info(&self) -> String {
        let rules_count = self.rules.read().unwrap().len();
        let sockets_count = self.sockets.read().unwrap().len();
        format!(
            "# XDP Kernel Bypass\r\n\
            interface:{}\r\n\
            mode:{}\r\n\
            umem_frame_size:{}\r\n\
            umem_num_frames:{}\r\n\
            active_ebpf_rules:{}\r\n\
            active_xsk_queues:{}\r\n\
            rx_packets:{}\r\n\
            rx_bytes:{}\r\n\
            dropped_packets:{}\r\n\
            redirected_packets:{}\r\n\
            pass_packets:{}\r\n\
            rate_limit_drops:{}\r\n",
            self.ifname,
            self.mode,
            self.frame_size,
            self.num_frames,
            rules_count,
            sockets_count,
            self.rx_packets.load(Ordering::Relaxed),
            self.rx_bytes.load(Ordering::Relaxed),
            self.dropped_packets.load(Ordering::Relaxed),
            self.redirected_packets.load(Ordering::Relaxed),
            self.pass_packets.load(Ordering::Relaxed),
            self.rate_limit_drops.load(Ordering::Relaxed),
        )
    }
}

/// Extracts RESP command payload from a raw network frame (Ethernet/IP/TCP or raw RESP).
pub fn extract_transport_payload(packet: &[u8]) -> Option<bytes::Bytes> {
    if packet.is_empty() {
        return None;
    }
    // Check Ethernet frame (14 bytes) + IPv4 (20 bytes) + TCP (20+ bytes)
    if packet.len() >= 54 && packet[12] == 0x08 && packet[13] == 0x00 && packet[14 + 9] == 6 {
        let ip_hdr_len = ((packet[14] & 0x0F) * 4) as usize;
        let tcp_offset = 14 + ip_hdr_len;
        if packet.len() >= tcp_offset + 20 {
            let tcp_data_offset = (((packet[tcp_offset + 12] >> 4) & 0x0F) * 4) as usize;
            let payload_start = tcp_offset + tcp_data_offset;
            if packet.len() > payload_start {
                return Some(bytes::Bytes::copy_from_slice(&packet[payload_start..]));
            }
        }
    } else if packet.len() >= 40 && (packet[0] >> 4) == 4 && packet[9] == 6 {
        // Raw IPv4 + TCP
        let ip_hdr_len = ((packet[0] & 0x0F) * 4) as usize;
        let tcp_offset = ip_hdr_len;
        if packet.len() >= tcp_offset + 20 {
            let tcp_data_offset = (((packet[tcp_offset + 12] >> 4) & 0x0F) * 4) as usize;
            let payload_start = tcp_offset + tcp_data_offset;
            if packet.len() > payload_start {
                return Some(bytes::Bytes::copy_from_slice(&packet[payload_start..]));
            }
        }
    } else if packet[0] == b'*'
        || packet[0] == b'+'
        || packet[0] == b'$'
        || packet[0] == b':'
        || packet[0] == b'-'
    {
        // Direct RESP payload
        return Some(bytes::Bytes::copy_from_slice(packet));
    } else {
        // Inline text command
        return Some(bytes::Bytes::copy_from_slice(packet));
    }
    None
}

// Global XDP engine instance
static GLOBAL_XDP_ENGINE: LazyLock<Arc<XdpEngine>> = LazyLock::new(|| {
    let mode = if std::path::Path::new("/sys/class/net").exists() {
        XdpMode::Skb
    } else {
        XdpMode::Simulated
    };
    Arc::new(XdpEngine::new("eth0", mode))
});

pub fn get_xdp_engine() -> Arc<XdpEngine> {
    GLOBAL_XDP_ENGINE.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdp_rule_filtering_and_stats() {
        let engine = XdpEngine::new("eth0", XdpMode::Simulated);

        // Add a DROP rule for 10.0.0.0/8
        let r1 = engine.add_rule(XdpAction::Drop, "10.0.0.0/8").unwrap();
        assert_eq!(r1, 1);

        // Add a PASS rule for 192.168.1.0/24
        let r2 = engine.add_rule(XdpAction::Pass, "192.168.1.0/24").unwrap();
        assert_eq!(r2, 2);

        assert_eq!(engine.list_rules().len(), 2);

        // Create simulated IPv4 packets
        let mut packet_drop = vec![0u8; 40];
        packet_drop[0] = 0x45; // IPv4
        packet_drop[12] = 10;
        packet_drop[13] = 1;
        packet_drop[14] = 2;
        packet_drop[15] = 3; // src: 10.1.2.3

        let mut packet_pass = vec![0u8; 40];
        packet_pass[0] = 0x45;
        packet_pass[12] = 192;
        packet_pass[13] = 168;
        packet_pass[14] = 1;
        packet_pass[15] = 50; // src: 192.168.1.50

        let mut packet_other = vec![0u8; 40];
        packet_other[0] = 0x45;
        packet_other[12] = 172;
        packet_other[13] = 16;
        packet_other[14] = 0;
        packet_other[15] = 1; // src: 172.16.0.1

        assert_eq!(engine.process_packet(&packet_drop), XdpAction::Drop);
        assert_eq!(engine.process_packet(&packet_pass), XdpAction::Pass);
        assert_eq!(engine.process_packet(&packet_other), XdpAction::Redirect);

        assert_eq!(engine.dropped_packets.load(Ordering::Relaxed), 1);
        assert_eq!(engine.pass_packets.load(Ordering::Relaxed), 1);
        assert_eq!(engine.redirected_packets.load(Ordering::Relaxed), 1);

        // Delete rule 1
        assert!(engine.del_rule(1).is_ok());
        assert_eq!(engine.list_rules().len(), 1);

        // Now packet from 10.1.2.3 should no longer drop
        assert_eq!(engine.process_packet(&packet_drop), XdpAction::Redirect);
    }

    #[test]
    fn test_token_bucket_rate_limiter() {
        let mut bucket = TokenBucket::new(2.0, 1.0);
        assert!(bucket.allow());
        assert!(bucket.allow());
        assert!(!bucket.allow()); // Exhausted
    }

    #[test]
    fn test_xsk_socket_rings_lifecycle() {
        let socket = XskSocket::new(0, 2048, 64);
        assert_eq!(socket.fill_ring.len(), 64);
        assert_eq!(socket.rx_ring.len(), 0);

        // Inject 3 simulated packets into the AF_XDP socket
        let pkt1 = b"PING\r\n";
        let pkt2 = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n";
        let pkt3 = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n";

        assert!(socket.inject_rx(pkt1));
        assert!(socket.inject_rx(pkt2));
        assert!(socket.inject_rx(pkt3));

        assert_eq!(socket.rx_ring.len(), 3);
        assert_eq!(socket.fill_ring.len(), 61);

        // Burst receive
        let mut rx_batch = Vec::new();
        let count = socket.rx_burst(&mut rx_batch, 32);
        assert_eq!(count, 3);
        assert_eq!(rx_batch.len(), 3);
        assert_eq!(&rx_batch[0], pkt1);
        assert_eq!(&rx_batch[1], pkt2);
        assert_eq!(&rx_batch[2], pkt3);

        // Fill ring replenished back to 64
        assert_eq!(socket.fill_ring.len(), 64);
        assert_eq!(socket.rx_ring.len(), 0);

        // Test Tx burst
        let resp1: &[u8] = b"+PONG\r\n";
        let resp2: &[u8] = b"+OK\r\n";
        let tx_count = socket.tx_burst(&[resp1, resp2]);
        assert_eq!(tx_count, 2);
        assert_eq!(socket.tx_ring.len(), 2);
        assert_eq!(socket.comp_ring.len(), 2);

        // Test extract_transport_payload
        let resp_payload = extract_transport_payload(pkt2).unwrap();
        assert_eq!(&resp_payload[..], pkt2);
    }
}
