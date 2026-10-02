//! Network listener configuration: `bind` address parsing and per-port
//! listener creation.
//!
//! Mirrors Redis/Valkey `bind` semantics:
//! - a whitespace-separated list of IPv4/IPv6 addresses,
//! - `*` means every IPv4 interface, `::*` every IPv6 interface,
//! - a leading `-` marks an address as optional: failing to bind it (e.g. no
//!   IPv6 on the host) is logged and skipped instead of aborting startup.
//!
//! With no `bind` directive Redis listens on `* -::*`; we use the same default.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{LazyLock, RwLock};

use socket2::{Domain, Protocol, Socket, Type};

/// Redis default when no `bind` directive is given.
pub const DEFAULT_BIND: &str = "* -::*";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindAddr {
    pub ip: IpAddr,
    /// `-` prefix: skip silently if the address can't be bound.
    pub optional: bool,
}

/// Parses a Redis-style `bind` value.
pub fn parse_bind_spec(spec: &str) -> Result<Vec<BindAddr>, String> {
    let mut out: Vec<BindAddr> = Vec::new();
    for tok in spec.split_whitespace() {
        let (optional, t) = match tok.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, tok),
        };
        let ip = match t {
            "*" => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            "::*" => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            other => other
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .map_err(|_| format!("Invalid bind address '{}'", tok))?,
        };
        if !out.iter().any(|b| b.ip == ip) {
            out.push(BindAddr { ip, optional });
        }
    }
    if out.is_empty() {
        return Err("bind requires at least one address".to_string());
    }
    Ok(out)
}

/// Renders addresses back to the `bind` form used by `CONFIG GET bind`.
pub fn format_bind_spec(addrs: &[BindAddr]) -> String {
    addrs
        .iter()
        .map(|b| {
            let s = match b.ip {
                IpAddr::V4(v4) if v4.is_unspecified() => "*".to_string(),
                IpAddr::V6(v6) if v6.is_unspecified() => "::*".to_string(),
                ip => ip.to_string(),
            };
            if b.optional { format!("-{}", s) } else { s }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

static BIND_ADDRS: LazyLock<RwLock<HashMap<u16, Vec<BindAddr>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Registers the bind addresses for the server whose base port is `port`.
pub fn set_bind_addrs(port: u16, addrs: Vec<BindAddr>) {
    BIND_ADDRS.write().unwrap().insert(port, addrs);
}

/// Bind addresses for the server on base port `port` (Redis default if unset).
pub fn bind_addrs(port: u16) -> Vec<BindAddr> {
    if let Some(v) = BIND_ADDRS.read().unwrap().get(&port) {
        return v.clone();
    }
    parse_bind_spec(DEFAULT_BIND).expect("default bind spec is valid")
}

/// Creates one non-blocking `SO_REUSEPORT` listener on `ip:port`.
pub fn bind_reuseport_listener(
    ip: IpAddr,
    port: u16,
    backlog: i32,
) -> std::io::Result<std::net::TcpListener> {
    let domain = if ip.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    if ip.is_ipv6() {
        // Keep `::` from also claiming IPv4, which would collide with `*`.
        socket.set_only_v6(true)?;
    }
    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    let _ = socket.set_recv_buffer_size(512 * 1024);
    let _ = socket.set_send_buffer_size(512 * 1024);
    socket.bind(&SocketAddr::new(ip, port).into())?;
    socket.listen(backlog)?;
    Ok(socket.into())
}

/// Binds every address in `addrs` on `port`. Optional (`-`) addresses that
/// fail are skipped; a required address failing, or nothing bound at all, is
/// an error.
pub fn bind_all(
    addrs: &[BindAddr],
    port: u16,
    backlog: i32,
) -> Result<Vec<(SocketAddr, std::net::TcpListener)>, String> {
    let mut out = Vec::with_capacity(addrs.len());
    for b in addrs {
        match bind_reuseport_listener(b.ip, port, backlog) {
            Ok(l) => out.push((SocketAddr::new(b.ip, port), l)),
            Err(e) if b.optional => {
                tracing::debug!("skipping optional bind address {}:{}: {}", b.ip, port, e);
            }
            Err(e) => {
                return Err(format!(
                    "Failed to bind {}: {}",
                    SocketAddr::new(b.ip, port),
                    e
                ));
            }
        }
    }
    if out.is_empty() {
        return Err(format!("No bind address could be bound on port {}", port));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bind_spec_variants() {
        let v = parse_bind_spec("127.0.0.1 -::1").unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].ip, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert!(!v[0].optional);
        assert_eq!(v[1].ip, "::1".parse::<IpAddr>().unwrap());
        assert!(v[1].optional);

        let d = parse_bind_spec(DEFAULT_BIND).unwrap();
        assert_eq!(d[0].ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(d[1].ip, IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert!(d[1].optional);

        // duplicates collapse, brackets accepted
        let b = parse_bind_spec("[::1] ::1 0.0.0.0").unwrap();
        assert_eq!(b.len(), 2);

        assert!(parse_bind_spec("not-an-ip").is_err());
        assert!(parse_bind_spec("   ").is_err());
    }

    #[test]
    fn test_format_bind_spec_roundtrip() {
        for spec in ["* -::*", "127.0.0.1 -::1", "10.0.0.5"] {
            assert_eq!(format_bind_spec(&parse_bind_spec(spec).unwrap()), spec);
        }
    }

    #[test]
    fn test_bind_all_respects_address_and_optional() {
        let addrs = parse_bind_spec("127.0.0.1").unwrap();
        let ls = bind_all(&addrs, 0, 16).unwrap();
        assert_eq!(ls.len(), 1);
        assert!(ls[0].1.local_addr().unwrap().ip().is_loopback());

        // An unassignable optional address is skipped, required one errors.
        let opt = parse_bind_spec("127.0.0.1 -192.0.2.123").unwrap();
        assert_eq!(bind_all(&opt, 0, 16).unwrap().len(), 1);
        let req = parse_bind_spec("192.0.2.123").unwrap();
        assert!(bind_all(&req, 0, 16).is_err());
    }

    #[test]
    fn test_bind_addrs_registry_default_and_override() {
        let port = 65011;
        assert_eq!(bind_addrs(port), parse_bind_spec(DEFAULT_BIND).unwrap());
        set_bind_addrs(port, parse_bind_spec("127.0.0.1").unwrap());
        assert_eq!(format_bind_spec(&bind_addrs(port)), "127.0.0.1");
    }
}
