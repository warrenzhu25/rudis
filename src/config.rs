use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Production configuration for Rudis, compatible with redis.conf / rudis.conf
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RudisConfig {
    pub bind: String,
    pub port: u16,
    pub threads: Option<usize>,
    pub maxclients: usize,
    pub maxmemory: Option<String>,
    pub maxmemory_bytes: Option<u64>,
    pub maxmemory_policy: String,
    pub appendonly: bool,
    pub dir: PathBuf,
    pub requirepass: Option<String>,
    pub tls_port: Option<u16>,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub cluster_enabled: bool,
    pub tiered_offload_threshold: u64,
    pub tiered_upload_threshold: u64,
    pub extra_directives: HashMap<String, String>,
}

impl Default for RudisConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            threads: None,
            maxclients: 10000,
            maxmemory: None,
            maxmemory_bytes: None,
            maxmemory_policy: "noeviction".to_string(),
            appendonly: false,
            dir: PathBuf::from("."),
            requirepass: None,
            tls_port: None,
            tls_cert_file: None,
            tls_key_file: None,
            cluster_enabled: false,
            tiered_offload_threshold: 60,
            tiered_upload_threshold: 80,
            extra_directives: HashMap::new(),
        }
    }
}

impl RudisConfig {
    /// Loads configuration from a string formatted like redis.conf
    pub fn parse_str(content: &str) -> Result<Self, String> {
        let mut config = Self::default();

        for (line_num, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            // Ignore empty lines and comment lines starting with #
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Directives are whitespace-separated
            let mut parts = line.split_whitespace();
            let directive = match parts.next() {
                Some(d) => d.to_lowercase(),
                None => continue,
            };

            let rest: Vec<&str> = parts.collect();
            if rest.is_empty() {
                continue;
            }

            match directive.as_str() {
                "bind" => {
                    config.bind = rest.join(" ");
                }
                "port" => {
                    config.port = rest[0]
                        .parse::<u16>()
                        .map_err(|e| format!("Invalid port at line {}: {}", line_num + 1, e))?;
                }
                "threads" | "io-threads" => {
                    let n = rest[0]
                        .parse::<usize>()
                        .map_err(|e| format!("Invalid threads at line {}: {}", line_num + 1, e))?;
                    config.threads = Some(n);
                }
                "maxclients" => {
                    config.maxclients = rest[0].parse::<usize>().map_err(|e| {
                        format!("Invalid maxclients at line {}: {}", line_num + 1, e)
                    })?;
                }
                "maxmemory" => {
                    let raw_mem = rest[0].to_string();
                    let bytes = crate::tiering::parse_memory_bytes(&raw_mem);
                    config.maxmemory = Some(raw_mem);
                    config.maxmemory_bytes = bytes;
                }
                "maxmemory-policy" => {
                    config.maxmemory_policy = rest[0].to_lowercase();
                }
                "appendonly" => {
                    config.appendonly =
                        matches!(rest[0].to_lowercase().as_str(), "yes" | "true" | "1");
                }
                "dir" => {
                    // Strip quotes if present
                    let mut p = rest.join(" ");
                    if (p.starts_with('"') && p.ends_with('"'))
                        || (p.starts_with('\'') && p.ends_with('\''))
                    {
                        p = p[1..p.len() - 1].to_string();
                    }
                    config.dir = PathBuf::from(p);
                }
                "requirepass" => {
                    let mut pass = rest.join(" ");
                    if (pass.starts_with('"') && pass.ends_with('"'))
                        || (pass.starts_with('\'') && pass.ends_with('\''))
                    {
                        pass = pass[1..pass.len() - 1].to_string();
                    }
                    config.requirepass = Some(pass);
                }
                "tls-port" => {
                    let p = rest[0]
                        .parse::<u16>()
                        .map_err(|e| format!("Invalid tls-port at line {}: {}", line_num + 1, e))?;
                    config.tls_port = Some(p);
                }
                "tls-cert-file" => {
                    config.tls_cert_file = Some(PathBuf::from(rest.join(" ")));
                }
                "tls-key-file" => {
                    config.tls_key_file = Some(PathBuf::from(rest.join(" ")));
                }
                "cluster-enabled" => {
                    config.cluster_enabled =
                        matches!(rest[0].to_lowercase().as_str(), "yes" | "true" | "1");
                }
                "tiered-offload-threshold" => {
                    let val = rest[0].parse::<u64>().map_err(|e| {
                        format!(
                            "Invalid tiered-offload-threshold at line {}: {}",
                            line_num + 1,
                            e
                        )
                    })?;
                    config.tiered_offload_threshold = val;
                }
                "tiered-upload-threshold" => {
                    let val = rest[0].parse::<u64>().map_err(|e| {
                        format!(
                            "Invalid tiered-upload-threshold at line {}: {}",
                            line_num + 1,
                            e
                        )
                    })?;
                    config.tiered_upload_threshold = val;
                }
                other => {
                    config
                        .extra_directives
                        .insert(other.to_string(), rest.join(" "));
                }
            }
        }

        Ok(config)
    }

    /// Loads configuration from a file path
    pub fn load_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| format!("Failed to read config file {:?}: {}", path.as_ref(), e))?;
        Self::parse_str(&content)
    }

    /// Overrides configuration settings with explicitly provided CLI arguments
    pub fn merge_cli(
        &mut self,
        port: Option<u16>,
        threads: Option<usize>,
        aof: Option<bool>,
        aof_dir: Option<PathBuf>,
        maxmemory: Option<String>,
        tiered_offload_threshold: Option<u64>,
        tiered_upload_threshold: Option<u64>,
        tls_port: Option<u16>,
        tls_cert_file: Option<PathBuf>,
        tls_key_file: Option<PathBuf>,
        cluster_enabled: Option<bool>,
    ) {
        if let Some(p) = port {
            self.port = p;
        }
        if let Some(t) = threads {
            self.threads = Some(t);
        }
        if let Some(a) = aof {
            self.appendonly = a;
        }
        if let Some(d) = aof_dir {
            self.dir = d;
        }
        if let Some(m) = maxmemory {
            let bytes = crate::tiering::parse_memory_bytes(&m);
            self.maxmemory = Some(m);
            self.maxmemory_bytes = bytes;
        }
        if let Some(o) = tiered_offload_threshold {
            self.tiered_offload_threshold = o;
        }
        if let Some(u) = tiered_upload_threshold {
            self.tiered_upload_threshold = u;
        }
        if let Some(tp) = tls_port {
            self.tls_port = Some(tp);
        }
        if let Some(tc) = tls_cert_file {
            self.tls_cert_file = Some(tc);
        }
        if let Some(tk) = tls_key_file {
            self.tls_key_file = Some(tk);
        }
        if let Some(c) = cluster_enabled {
            self.cluster_enabled = c;
        }
    }
}

pub static ACTIVE_CONFIG_FILE: std::sync::RwLock<Option<PathBuf>> = std::sync::RwLock::new(None);

/// Rewrites the active configuration file atomically with current runtime settings
pub fn rewrite_config_file(port: u16) -> Result<(), String> {
    let path = ACTIVE_CONFIG_FILE
        .read()
        .unwrap()
        .clone()
        .unwrap_or_else(|| PathBuf::from("rudis.conf"));

    // 1. Read existing file if it exists, or create a new template
    let mut lines: Vec<String> = if path.exists() {
        fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read config file: {}", e))?
            .lines()
            .map(|s| s.to_string())
            .collect()
    } else {
        vec!["# Rudis configuration file (auto-generated by CONFIG REWRITE)".to_string()]
    };

    // 2. Prepare current dynamic directives
    let mut directives: HashMap<String, String> = HashMap::new();
    directives.insert("port".to_string(), port.to_string());
    directives.insert(
        "maxclients".to_string(),
        crate::connection::get_max_clients().to_string(),
    );
    directives.insert(
        "maxmemory-policy".to_string(),
        crate::connection::get_max_memory_policy(),
    );
    let max_mem = crate::tiering::get_max_memory(port);
    if max_mem > 0 {
        directives.insert("maxmemory".to_string(), max_mem.to_string());
    }
    directives.insert(
        "tiered-offload-threshold".to_string(),
        crate::tiering::get_offload_threshold_pct(port).to_string(),
    );
    directives.insert(
        "tiered-upload-threshold".to_string(),
        crate::tiering::get_upload_threshold_pct(port).to_string(),
    );
    directives.insert(
        "slowlog-log-slower-than".to_string(),
        crate::slowlog::SLOWLOG_LOG_SLOWER_THAN
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
    );
    directives.insert(
        "slowlog-max-len".to_string(),
        crate::slowlog::SLOWLOG_MAX_LEN
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
    );
    directives.insert(
        "slowlog-entry-max-argc".to_string(),
        crate::slowlog::SLOWLOG_ENTRY_MAX_ARGC
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
    );
    directives.insert(
        "slowlog-entry-max-string-len".to_string(),
        crate::slowlog::SLOWLOG_ENTRY_MAX_STRING_LEN
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
    );
    directives.insert(
        "client-output-buffer-limit".to_string(),
        crate::connection::format_client_output_buffer_limit_config(),
    );

    // If requirepass is set
    let acl = crate::acl::get_acl_for_port(port);
    if let Some(user) = acl.read().unwrap().get_user("default")
        && let Some(pass) = user.passwords.first()
    {
        directives.insert("requirepass".to_string(), pass.clone());
    }

    // 3. Update existing lines or track which ones were updated
    let mut updated_keys = std::collections::HashSet::new();
    for line in &mut lines {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        if let Some(key) = trimmed.split_whitespace().next() {
            let key_lower = key.to_lowercase();
            if let Some(val) = directives.get(&key_lower) {
                *line = format!("{} {}", key_lower, val);
                updated_keys.insert(key_lower);
            }
        }
    }

    // 4. Append remaining directives that were not present in the original file
    for (k, v) in directives {
        if !updated_keys.contains(&k) {
            lines.push(format!("{} {}", k, v));
        }
    }

    // 5. Write to temp file in the same directory, sync, rename, and sync parent dir!
    let tmp_path = format!("{}.tmp.{}", path.display(), std::process::id());
    {
        use std::io::Write;
        let mut tmp_file = fs::File::create(&tmp_path)
            .map_err(|e| format!("Failed to create temp config file: {}", e))?;
        for l in lines {
            writeln!(tmp_file, "{}", l)
                .map_err(|e| format!("Failed to write config line: {}", e))?;
        }
        tmp_file.flush().map_err(|e| e.to_string())?;
        tmp_file.sync_all().map_err(|e| e.to_string())?;
    }

    fs::rename(&tmp_path, &path)
        .map_err(|e| format!("Failed to atomically rename config file: {}", e))?;

    // Directory fsync for crash-durability!
    if let Some(parent) = path.parent() {
        let dir_path = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        if let Ok(dir_file) = fs::File::open(dir_path) {
            let _ = dir_file.sync_all();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_and_comments() {
        let text = r#"
        # This is a sample comment
        # Another line

        "#;
        let cfg = RudisConfig::parse_str(text).unwrap();
        assert_eq!(cfg.port, 6379);
        assert_eq!(cfg.maxclients, 10000);
        assert!(!cfg.appendonly);
    }

    #[test]
    fn test_parse_standard_redis_conf_options() {
        let text = r#"
        port 7777
        bind 0.0.0.0
        threads 4
        maxclients 5000
        maxmemory 2gb
        maxmemory-policy allkeys-lru
        appendonly yes
        dir /tmp/rudis-data
        requirepass "secret_pwd_123"
        tls-port 7778
        tls-cert-file /etc/ssl/cert.pem
        tls-key-file /etc/ssl/key.pem
        cluster-enabled yes
        tiered-offload-threshold 65
        tiered-upload-threshold 85
        save 900 1
        "#;
        let cfg = RudisConfig::parse_str(text).unwrap();
        assert_eq!(cfg.port, 7777);
        assert_eq!(cfg.bind, "0.0.0.0");
        assert_eq!(cfg.threads, Some(4));
        assert_eq!(cfg.maxclients, 5000);
        assert_eq!(cfg.maxmemory.as_deref(), Some("2gb"));
        assert_eq!(cfg.maxmemory_bytes, Some(2 * 1024 * 1024 * 1024));
        assert_eq!(cfg.maxmemory_policy, "allkeys-lru");
        assert!(cfg.appendonly);
        assert_eq!(cfg.dir, PathBuf::from("/tmp/rudis-data"));
        assert_eq!(cfg.requirepass.as_deref(), Some("secret_pwd_123"));
        assert_eq!(cfg.tls_port, Some(7778));
        assert_eq!(cfg.tls_cert_file, Some(PathBuf::from("/etc/ssl/cert.pem")));
        assert_eq!(cfg.tls_key_file, Some(PathBuf::from("/etc/ssl/key.pem")));
        assert!(cfg.cluster_enabled);
        assert_eq!(cfg.tiered_offload_threshold, 65);
        assert_eq!(cfg.tiered_upload_threshold, 85);
        assert_eq!(
            cfg.extra_directives.get("save").map(|s| s.as_str()),
            Some("900 1")
        );
    }

    #[test]
    fn test_parse_invalid_number_error() {
        let text = "port not_a_number\n";
        assert!(RudisConfig::parse_str(text).is_err());
    }

    #[test]
    fn test_config_merge_cli() {
        let mut cfg = RudisConfig::default();
        assert_eq!(cfg.port, 6379);
        assert!(!cfg.appendonly);

        cfg.merge_cli(
            Some(8888),
            Some(2),
            Some(true),
            Some(PathBuf::from("/data/aof")),
            Some("1gb".to_string()),
            Some(75),
            Some(90),
            Some(8889),
            None,
            None,
            Some(true),
        );

        assert_eq!(cfg.port, 8888);
        assert_eq!(cfg.threads, Some(2));
        assert!(cfg.appendonly);
        assert_eq!(cfg.dir, PathBuf::from("/data/aof"));
        assert_eq!(cfg.maxmemory.as_deref(), Some("1gb"));
        assert_eq!(cfg.maxmemory_bytes, Some(1024 * 1024 * 1024));
        assert_eq!(cfg.tiered_offload_threshold, 75);
        assert_eq!(cfg.tiered_upload_threshold, 90);
        assert_eq!(cfg.tls_port, Some(8889));
        assert!(cfg.cluster_enabled);
    }

    #[test]
    fn test_rewrite_config_file_atomic_and_durable() {
        let temp_dir = std::env::temp_dir().join(format!("rudis-cfg-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let cfg_path = temp_dir.join("test_rudis.conf");

        // Initial config file
        fs::write(
            &cfg_path,
            "# Sample config\nport 6379\nmaxclients 1000\n# Custom comment\n",
        )
        .unwrap();

        *ACTIVE_CONFIG_FILE.write().unwrap() = Some(cfg_path.clone());

        // Rewrite with port 9999
        let res = rewrite_config_file(9999);
        assert!(res.is_ok());

        // Verify content
        let content = fs::read_to_string(&cfg_path).unwrap();
        assert!(content.contains("port 9999"));
        assert!(content.contains("# Custom comment"));
        assert!(content.contains("slowlog-log-slower-than"));
        assert!(content.contains("client-output-buffer-limit"));

        let _ = fs::remove_file(&cfg_path);
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
