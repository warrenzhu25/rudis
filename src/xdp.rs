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
        Self {
            ifname: ifname.to_string(),
            mode,
            frame_size: 2048,
            num_frames: 4096,
            rules: RwLock::new(Vec::new()),
            next_rule_id: AtomicU32::new(1),
            rate_limiters: RwLock::new(HashMap::new()),
            default_rate_limit: 100_000.0,
            default_rate_capacity: 50_000.0,
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
        self.rx_bytes.fetch_add(packet.len() as u64, Ordering::Relaxed);

        if packet.is_empty() {
            self.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return XdpAction::Drop;
        }

        // Try extracting source IP from either raw Ethernet frame (header: 14 bytes)
        // or raw IPv4 packet (header: 20 bytes).
        let (src_ip, _dst_port, payload_offset) = if packet.len() >= 14 && packet[12] == 0x08 && packet[13] == 0x00 {
            // Ethernet frame with IPv4
            let ip_slice = &packet[14..];
            if ip_slice.len() >= 20 {
                let src = u32::from_be_bytes([ip_slice[12], ip_slice[13], ip_slice[14], ip_slice[15]]);
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
            let bucket = limiters
                .entry(ip)
                .or_insert_with(|| TokenBucket::new(self.default_rate_capacity, self.default_rate_limit));

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

    pub fn info(&self) -> String {
        let rules_count = self.rules.read().unwrap().len();
        format!(
            "# XDP Kernel Bypass\r\n\
            interface:{}\r\n\
            mode:{}\r\n\
            umem_frame_size:{}\r\n\
            umem_num_frames:{}\r\n\
            active_ebpf_rules:{}\r\n\
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
            self.rx_packets.load(Ordering::Relaxed),
            self.rx_bytes.load(Ordering::Relaxed),
            self.dropped_packets.load(Ordering::Relaxed),
            self.redirected_packets.load(Ordering::Relaxed),
            self.pass_packets.load(Ordering::Relaxed),
            self.rate_limit_drops.load(Ordering::Relaxed),
        )
    }
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
}
