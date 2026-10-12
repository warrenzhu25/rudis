use crate::acl_categories::{CATEGORIES, COMMANDS, NUM_COMMANDS, WORDS};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

pub static HAS_CUSTOM_ACL: AtomicBool = AtomicBool::new(false);

pub static PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn get_acl_for_port(port: u16) -> Arc<RwLock<AclManager>> {
    let mut map = PORT_ACLS.lock();
    map.entry(port)
        .or_insert_with(|| Arc::new(RwLock::new(AclManager::new())))
        .clone()
}

/// Makes `port` resolve to the same ACL table as `base_port`.
///
/// In cluster mode every shard listens on `base_port + shard_id`, but users,
/// `requirepass` and ACL SETUSER are server-wide in Redis. Without aliasing,
/// connections to any shard port other than the base one would see a fresh,
/// unauthenticated `default nopass` user.
pub fn share_acl_with_port(port: u16, base_port: u16) {
    if port == base_port {
        return;
    }
    let mut map = PORT_ACLS.lock();
    let base = map
        .entry(base_port)
        .or_insert_with(|| Arc::new(RwLock::new(AclManager::new())))
        .clone();
    map.insert(port, base);
}

/// Redis ACL password hash: `#` + lowercase hex SHA-256 of the password.
pub fn hash_password_sha256(password: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, password.as_bytes());
    let mut s = String::with_capacity(65);
    s.push('#');
    for b in digest.as_ref() {
        use std::fmt::Write;
        let _ = write!(&mut s, "{:02x}", b);
    }
    s
}

/// Validates and normalizes a `#<hash>` ACL rule body (64 hex chars).
fn parse_password_hash(hex: &str) -> Result<String, String> {
    if hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(format!("#{}", hex.to_ascii_lowercase()))
    } else {
        Err(format!(
            "Error in ACL SETUSER modifier '#{}': The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters",
            hex
        ))
    }
}

/// Constant-time equality so hash comparison timing doesn't leak a prefix match.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Index of a command (`name` or `container|sub`, case-insensitive) in
/// [`COMMANDS`].
pub fn command_index(name: &str) -> Option<usize> {
    COMMANDS
        .binary_search_by(|c| {
            c.name
                .bytes()
                .cmp(name.bytes().map(|b| b.to_ascii_lowercase()))
        })
        .ok()
}

fn category_bit(name: &str) -> Option<u32> {
    CATEGORIES
        .iter()
        .position(|c| c.eq_ignore_ascii_case(name))
        .map(|i| 1u32 << i)
}

/// Commands in `category` (for `ACL CAT <category>`), or None if unknown.
pub fn commands_in_category(category: &str) -> Option<Vec<&'static str>> {
    let bit = category_bit(category)?;
    Some(
        COMMANDS
            .iter()
            .filter(|c| c.cats & bit != 0)
            .map(|c| c.name)
            .collect(),
    )
}

/// One bit per entry of [`COMMANDS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandSet([u64; WORDS]);

impl CommandSet {
    const fn empty() -> Self {
        Self([0; WORDS])
    }

    fn full() -> Self {
        let mut s = Self::empty();
        for i in 0..NUM_COMMANDS {
            s.set(i, true);
        }
        s
    }

    fn get(&self, i: usize) -> bool {
        (self.0[i / 64] >> (i % 64)) & 1 == 1
    }

    fn set(&mut self, i: usize, on: bool) {
        if on {
            self.0[i / 64] |= 1 << (i % 64);
        } else {
            self.0[i / 64] &= !(1 << (i % 64));
        }
    }
}

/// Key permission bits (Redis ACL_READ_PERMISSION / ACL_WRITE_PERMISSION).
pub const KEY_READ: u8 = 1;
pub const KEY_WRITE: u8 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPattern {
    pub pattern: String,
    pub perm: u8,
}

/// Parses `~pat` (read+write) or `%<R|W|RW>~pat`.
fn parse_key_rule(rule: &str) -> Result<(u8, &str), String> {
    if let Some(pat) = rule.strip_prefix('~') {
        return Ok((KEY_READ | KEY_WRITE, pat));
    }
    let body = &rule[1..];
    let (flags, pat) = body
        .split_once('~')
        .ok_or_else(|| setuser_err(rule, "Syntax error"))?;
    let mut perm = 0;
    for c in flags.chars() {
        let bit = match c.to_ascii_uppercase() {
            'R' => KEY_READ,
            'W' => KEY_WRITE,
            _ => return Err(setuser_err(rule, "Syntax error")),
        };
        if perm & bit != 0 {
            return Err(setuser_err(rule, "Syntax error"));
        }
        perm |= bit;
    }
    if perm == 0 {
        return Err(setuser_err(rule, "Syntax error"));
    }
    Ok((perm, pat))
}

fn setuser_err(rule: &str, msg: &str) -> String {
    format!("Error in ACL SETUSER modifier '{}': {}", rule, msg)
}

/// Reason for an ACL access denial (used by `ACL LOG` and `INFO stats`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclDeniedReason {
    Auth,
    Command,
    Key,
    Channel,
}

impl AclDeniedReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Command => "command",
            Self::Key => "key",
            Self::Channel => "channel",
        }
    }
}

/// One entry in the server's `ACL LOG` ring buffer.
#[derive(Clone, Debug, PartialEq)]
pub struct AclLogEntry {
    pub entry_id: u64,
    pub count: u64,
    pub reason: AclDeniedReason,
    pub context: String,
    pub object: String,
    pub username: String,
    pub age_created: std::time::Instant,
    pub timestamp_created: f64,
    pub timestamp_last_updated: f64,
    pub client_info: String,
}

fn unix_now_secs_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CmdRuleEntry {
    Category { allow: bool, name: &'static str },
    Command { allow: bool, idx: usize },
    FirstArg { parent_idx: usize, arg: String },
}

fn canonical_category_name(name: &str) -> Option<(&'static str, u32)> {
    CATEGORIES
        .iter()
        .position(|c| c.eq_ignore_ascii_case(name))
        .map(|i| (CATEGORIES[i], 1u32 << i))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    /// `#<sha256 hex>` entries only; plaintext passwords are never stored.
    pub password_hashes: Vec<String>,
    pub nopass: bool,
    /// Redis `allcommands`: set by `+@all`, cleared by any later removal. Also
    /// grants commands that are not in the category table.
    pub all_commands: bool,
    commands: CommandSet,
    /// Base command rule (`true` = `+@all`, `false` = `-@all`) and ordered,
    /// compacted command rule modifiers as tracked by Redis `ACL GETUSER`.
    cmd_base_all: bool,
    cmd_rules: Vec<CmdRuleEntry>,
    /// Allowed first-argument restrictions `(parent_cmd_idx, lowercase_arg)`
    /// such as `+select|0` or `+debug|object`.
    first_arg_rules: Vec<(usize, String)>,
    pub all_keys: bool,
    /// `~pat` / `%R~pat` / `%W~pat` key patterns.
    pub key_patterns: Vec<KeyPattern>,
    /// `&*` / `allchannels`.
    pub all_channels: bool,
    /// `&<glob>` Pub/Sub channel patterns.
    pub channel_patterns: Vec<String>,
    /// `sanitize-payload` (default) / `skip-sanitize-payload`: whether
    /// RESTORE payloads get deep sanitization for this user.
    pub sanitize_payload: bool,
}

impl AclUser {
    pub fn new_default() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            password_hashes: Vec::new(),
            nopass: true,
            all_commands: true,
            commands: CommandSet::full(),
            cmd_base_all: true,
            cmd_rules: Vec::new(),
            first_arg_rules: Vec::new(),
            all_keys: true,
            key_patterns: Vec::new(),
            all_channels: true,
            channel_patterns: Vec::new(),
            sanitize_payload: true,
        }
    }

    /// A newly created user. Starts as `off resetpass resetkeys -@all`, with
    /// channel access governed by `acl-pubsub-default` (`all_channels_default`).
    pub fn new_empty_with_pubsub(name: &str, all_channels_default: bool) -> Self {
        Self {
            name: name.to_string(),
            enabled: false,
            password_hashes: Vec::new(),
            nopass: false,
            all_commands: false,
            commands: CommandSet::empty(),
            cmd_base_all: false,
            cmd_rules: Vec::new(),
            first_arg_rules: Vec::new(),
            all_keys: false,
            key_patterns: Vec::new(),
            all_channels: all_channels_default,
            channel_patterns: Vec::new(),
            sanitize_payload: true,
        }
    }

    /// `cmd_name` is a command name or `container|sub` (any case). A bare
    /// container name (subcommand unknown to the caller) is permitted only if
    /// every non-help subcommand is.
    pub fn can_execute_command(&self, cmd_name: &str) -> bool {
        self.can_execute_command_with_first_arg(cmd_name, None)
    }

    /// Checks command permission, additionally consulting first-arg rules
    /// (`+select|0`, `+debug|object`) when `first_arg` is provided and the
    /// full command is not unconditionally permitted.
    pub fn can_execute_command_with_first_arg(
        &self,
        cmd_name: &str,
        first_arg: Option<&[u8]>,
    ) -> bool {
        let Some(i) = command_index(cmd_name) else {
            return self.all_commands;
        };
        let c = &COMMANDS[i];
        if c.no_auth || self.all_commands {
            return true;
        }
        if c.subs.0 == c.subs.1 {
            if self.commands.get(i) {
                return true;
            }
            if let Some(arg) = first_arg
                && !self.first_arg_rules.is_empty()
            {
                return self
                    .first_arg_rules
                    .iter()
                    .any(|(idx, a)| *idx == i && a.as_bytes().eq_ignore_ascii_case(arg));
            }
            return false;
        }
        (c.subs.0..c.subs.1).all(|j| {
            let j = j as usize;
            self.commands.get(j) || COMMANDS[j].name.ends_with("|help")
        })
    }

    /// Redis ACLChangeSelectorPerm: a container rule also covers its
    /// subcommands.
    fn change_perm(&mut self, i: usize, allow: bool) {
        self.commands.set(i, allow);
        let (a, b) = COMMANDS[i].subs;
        for j in a..b {
            self.commands.set(j as usize, allow);
        }
        self.first_arg_rules.retain(|(idx, _)| *idx != i);
        if !allow {
            self.all_commands = false;
        }
    }

    fn change_category(&mut self, bit: u32, allow: bool) {
        for (i, c) in COMMANDS.iter().enumerate() {
            if c.cats & bit != 0 {
                self.change_perm(i, allow);
            }
        }
        if !allow {
            self.all_commands = false;
        }
    }

    pub fn apply_rule(&mut self, rule: &str) -> Result<(), String> {
        self.apply_rule_with_pubsub(rule, false)
    }

    pub fn apply_rule_with_pubsub(
        &mut self,
        rule: &str,
        all_channels_default: bool,
    ) -> Result<(), String> {
        let lower = rule.to_ascii_lowercase();
        match lower.as_str() {
            "on" => self.enabled = true,
            "off" => self.enabled = false,
            "nopass" => {
                self.nopass = true;
                self.password_hashes.clear();
            }
            "resetpass" => {
                self.nopass = false;
                self.password_hashes.clear();
            }
            "allkeys" | "~*" => {
                self.all_keys = true;
                self.key_patterns.clear();
            }
            "resetkeys" => {
                self.all_keys = false;
                self.key_patterns.clear();
            }
            "allcommands" | "+@all" => {
                self.commands = CommandSet::full();
                self.all_commands = true;
                self.cmd_base_all = true;
                self.cmd_rules.clear();
                self.first_arg_rules.clear();
            }
            "nocommands" | "-@all" => {
                self.commands = CommandSet::empty();
                self.all_commands = false;
                self.cmd_base_all = false;
                self.cmd_rules.clear();
                self.first_arg_rules.clear();
            }
            "allchannels" | "&*" => {
                self.all_channels = true;
                self.channel_patterns.clear();
            }
            "resetchannels" => {
                self.all_channels = false;
                self.channel_patterns.clear();
            }
            "sanitize-payload" => self.sanitize_payload = true,
            "skip-sanitize-payload" => self.sanitize_payload = false,
            // No selectors exist, so there is nothing to clear.
            "clearselectors" => {}
            "reset" => {
                let name = std::mem::take(&mut self.name);
                *self = Self::new_empty_with_pubsub(&name, all_channels_default);
            }
            _ => return self.apply_prefixed_rule(rule),
        }
        Ok(())
    }

    fn apply_prefixed_rule(&mut self, rule: &str) -> Result<(), String> {
        const UNKNOWN: &str = "Unknown command or category name in ACL";
        if let Some(p) = rule.strip_prefix('>') {
            self.nopass = false;
            let h = hash_password_sha256(p);
            if !self.password_hashes.contains(&h) {
                self.password_hashes.push(h);
            }
        } else if let Some(h) = rule.strip_prefix('#') {
            let full_hash = parse_password_hash(h)?;
            self.nopass = false;
            if !self.password_hashes.contains(&full_hash) {
                self.password_hashes.push(full_hash);
            }
        } else if let Some(p) = rule.strip_prefix('<') {
            let h = hash_password_sha256(p);
            self.password_hashes.retain(|x| *x != h);
        } else if let Some(h) = rule.strip_prefix('!') {
            let full_hash = parse_password_hash(h)?;
            self.password_hashes.retain(|x| *x != full_hash);
        } else if rule.starts_with('~') || rule.starts_with('%') {
            let (perm, pat) = parse_key_rule(rule)?;
            if self.all_keys {
                return Err(setuser_err(
                    rule,
                    "Adding a pattern after the * pattern (or the 'allkeys' flag) is not valid and does not have any effect. Try 'resetkeys' to start with an empty list of patterns",
                ));
            }
            // Same pattern again widens its permissions, as in Redis.
            match self.key_patterns.iter_mut().find(|p| p.pattern == pat) {
                Some(p) => p.perm |= perm,
                None => self.key_patterns.push(KeyPattern {
                    pattern: pat.to_string(),
                    perm,
                }),
            }
        } else if let Some(pat) = rule.strip_prefix('&') {
            if self.all_channels {
                return Err(setuser_err(
                    rule,
                    "Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' to start with an empty list of channels",
                ));
            }
            if !self.channel_patterns.iter().any(|p| p == pat) {
                self.channel_patterns.push(pat.to_string());
            }
        } else if rule.starts_with('(') {
            return Err(setuser_err(rule, "Selectors are not supported"));
        } else if let Some(rest) = rule.strip_prefix('+').or_else(|| rule.strip_prefix('-')) {
            let allow = rule.starts_with('+');
            if let Some(cat) = rest.strip_prefix('@') {
                let (cat_name, bit) =
                    canonical_category_name(cat).ok_or_else(|| setuser_err(rule, UNKNOWN))?;
                self.change_category(bit, allow);
                self.cmd_rules.retain(
                    |e| !matches!(e, CmdRuleEntry::Category { name, .. } if *name == cat_name),
                );
                self.cmd_rules.push(CmdRuleEntry::Category {
                    allow,
                    name: cat_name,
                });
            } else if let Some(i) = command_index(rest) {
                self.change_perm(i, allow);
                let (a, b) = COMMANDS[i].subs;
                self.cmd_rules.retain(|e| match e {
                    CmdRuleEntry::Command { idx, .. } => {
                        *idx != i && !(*idx >= a as usize && *idx < b as usize)
                    }
                    CmdRuleEntry::FirstArg { parent_idx, .. } => *parent_idx != i,
                    _ => true,
                });
                self.cmd_rules.push(CmdRuleEntry::Command { allow, idx: i });
            } else if rest.contains('|') {
                let parts: Vec<&str> = rest.split('|').collect();
                if parts.len() == 3 {
                    let sub_name = format!("{}|{}", parts[0], parts[1]);
                    if command_index(&sub_name).is_some() {
                        return Err(setuser_err(
                            rule,
                            "Allowing first-arg of a subcommand is not supported",
                        ));
                    }
                    return Err(setuser_err(rule, UNKNOWN));
                }
                if parts.len() == 2
                    && allow
                    && !parts[1].is_empty()
                    && let Some(parent_idx) = command_index(parts[0])
                {
                    let c = &COMMANDS[parent_idx];
                    // Only plain commands (no subcommands in table, e.g.
                    // SELECT or DEBUG) accept first-arg restrictions; an
                    // unknown subcommand of a container (e.g. +config|asdf)
                    // is rejected as unknown.
                    if c.subs.0 == c.subs.1 && c.parent.is_none() {
                        let arg_lower = parts[1].to_ascii_lowercase();
                        if !self
                            .first_arg_rules
                            .iter()
                            .any(|(idx, a)| *idx == parent_idx && *a == arg_lower)
                        {
                            self.first_arg_rules.push((parent_idx, arg_lower.clone()));
                        }
                        self.cmd_rules.retain(|e| {
                            !matches!(
                                e,
                                CmdRuleEntry::FirstArg { parent_idx: p, arg }
                                    if *p == parent_idx && *arg == arg_lower
                            )
                        });
                        self.cmd_rules.push(CmdRuleEntry::FirstArg {
                            parent_idx,
                            arg: arg_lower,
                        });
                        return Ok(());
                    }
                }
                return Err(setuser_err(rule, UNKNOWN));
            } else {
                return Err(setuser_err(rule, UNKNOWN));
            }
        } else {
            return Err(setuser_err(rule, "Syntax error"));
        }
        Ok(())
    }

    /// The command part of `ACL LIST` / `ACL GETUSER`, replayable through
    /// `ACL SETUSER`.
    pub fn commands_rule_string(&self) -> String {
        let mut parts = vec![if self.cmd_base_all { "+@all" } else { "-@all" }.to_string()];
        let sign = |on: bool| if on { '+' } else { '-' };
        for entry in &self.cmd_rules {
            match entry {
                CmdRuleEntry::Category { allow, name } => {
                    parts.push(format!("{}@{}", sign(*allow), name));
                }
                CmdRuleEntry::Command { allow, idx } => {
                    parts.push(format!("{}{}", sign(*allow), COMMANDS[*idx].name));
                }
                CmdRuleEntry::FirstArg { parent_idx, arg } => {
                    parts.push(format!("+{}|{}", COMMANDS[*parent_idx].name, arg));
                }
            }
        }
        parts.join(" ")
    }

    pub fn keys_rule_string(&self) -> String {
        if self.all_keys {
            "~*".to_string()
        } else {
            self.key_patterns
                .iter()
                .map(|p| match p.perm {
                    KEY_READ => format!("%R~{}", p.pattern),
                    KEY_WRITE => format!("%W~{}", p.pattern),
                    _ => format!("~{}", p.pattern),
                })
                .collect::<Vec<_>>()
                .join(" ")
        }
    }

    /// Channel string for `ACL GETUSER <user> channels`: `"&*"` when
    /// `all_channels`, else `&pat` entries joined by space (`""` when empty).
    pub fn channels_rule_string(&self) -> String {
        if self.all_channels {
            "&*".to_string()
        } else {
            self.channel_patterns
                .iter()
                .map(|p| format!("&{}", p))
                .collect::<Vec<_>>()
                .join(" ")
        }
    }

    /// Redis ACLCheckChannelAgainstList: channels (PUBLISH/SUBSCRIBE) are glob
    /// matched against the user's patterns; a PSUBSCRIBE pattern must equal
    /// one of them literally.
    pub fn can_access_channel(&self, channel: &[u8], is_pattern: bool) -> bool {
        self.all_channels
            || self.channel_patterns.iter().any(|p| {
                if is_pattern {
                    p.as_bytes() == channel
                } else {
                    crate::pubsub::glob_match(p.as_bytes(), channel)
                }
            })
    }

    /// Redis ACLSelectorCheckKey: some single pattern must glob-match `key`
    /// and grant every permission in `need` ([`KEY_READ`] | [`KEY_WRITE`]; 0
    /// for commands that neither read nor modify the value, e.g. EXISTS).
    pub fn can_access_key(&self, key: &[u8], need: u8) -> bool {
        self.all_keys
            || self.key_patterns.iter().any(|p| {
                p.perm & need == need && crate::pubsub::glob_match(p.pattern.as_bytes(), key)
            })
    }

    pub fn flags(&self) -> Vec<String> {
        let mut flags = Vec::new();
        if self.enabled {
            flags.push("on".to_string());
        } else {
            flags.push("off".to_string());
        }
        if self.nopass {
            flags.push("nopass".to_string());
        }
        if self.all_keys {
            flags.push("allkeys".to_string());
        }
        if self.all_channels {
            flags.push("allchannels".to_string());
        }
        if self.all_commands {
            flags.push("allcommands".to_string());
        }
        flags.push(
            if self.sanitize_payload {
                "sanitize-payload"
            } else {
                "skip-sanitize-payload"
            }
            .to_string(),
        );
        flags
    }

    pub fn to_acl_list_line(&self) -> String {
        let mut parts = vec![format!("user {}", self.name)];
        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }
        if self.nopass {
            parts.push("nopass".to_string());
        } else {
            for h in &self.password_hashes {
                parts.push(h.clone());
            }
        }
        parts.push(
            if self.sanitize_payload {
                "sanitize-payload"
            } else {
                "skip-sanitize-payload"
            }
            .to_string(),
        );
        // Like Redis' ACLDescribeSelector: no key patterns means nothing to
        // list (a new user has none).
        let keys = self.keys_rule_string();
        if !keys.is_empty() {
            parts.push(keys);
        }
        if self.all_channels {
            parts.push("&*".to_string());
        } else {
            parts.push("resetchannels".to_string());
            for pat in &self.channel_patterns {
                parts.push(format!("&{}", pat));
            }
        }
        parts.push(self.commands_rule_string());
        parts.join(" ")
    }
}

pub struct AclManager {
    pub users: HashMap<String, AclUser>,
    /// Value of the `requirepass` config, kept only so CONFIG GET/REWRITE can
    /// report it like Redis does. ACL commands never expose it.
    pub requirepass: Option<String>,
    /// `acl-pubsub-default`: `true` = `allchannels`, `false` = `resetchannels`
    /// (Redis 7 default).
    pub acl_pubsub_default_all: bool,
    /// `aclfile`: path to external ACL file (empty if not configured).
    pub aclfile: Option<std::path::PathBuf>,
    /// `acllog-max-len` (default 128).
    pub acllog_max_len: usize,
    pub acl_log: std::collections::VecDeque<AclLogEntry>,
    pub next_log_entry_id: u64,
}

impl Default for AclManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AclManager {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), AclUser::new_default());
        Self {
            users,
            requirepass: None,
            acl_pubsub_default_all: false,
            aclfile: None,
            acllog_max_len: 128,
            acl_log: std::collections::VecDeque::new(),
            next_log_entry_id: 0,
        }
    }

    /// Records a security event in `ACL LOG` (aggregating matching entries as
    /// Redis `ACLAddLogEntry` does).
    pub fn add_log_entry(
        &mut self,
        reason: AclDeniedReason,
        context: &str,
        object: &str,
        username: &str,
        client_info: String,
    ) {
        if self.acllog_max_len == 0 {
            return;
        }
        let now_ts = unix_now_secs_f64();
        if let Some(pos) = self.acl_log.iter().position(|e| {
            e.reason == reason
                && e.context == context
                && e.object == object
                && e.username == username
        }) {
            let mut entry = self.acl_log.remove(pos).unwrap();
            entry.count += 1;
            entry.timestamp_last_updated = now_ts;
            entry.client_info = client_info;
            self.acl_log.push_front(entry);
            return;
        }
        while self.acl_log.len() >= self.acllog_max_len {
            self.acl_log.pop_back();
        }
        let entry_id = self.next_log_entry_id;
        self.next_log_entry_id += 1;
        self.acl_log.push_front(AclLogEntry {
            entry_id,
            count: 1,
            reason,
            context: context.to_string(),
            object: object.to_string(),
            username: username.to_string(),
            age_created: std::time::Instant::now(),
            timestamp_created: now_ts,
            timestamp_last_updated: now_ts,
            client_info,
        });
    }

    pub fn check_auth(
        &self,
        username: Option<&str>,
        password: &str,
    ) -> Result<String, &'static str> {
        const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";
        let user_name = username.unwrap_or("default");
        let Some(user) = self.users.get(user_name) else {
            return Err(WRONGPASS);
        };
        if !user.enabled {
            return Err(WRONGPASS);
        }
        if user.nopass {
            return Ok(user_name.to_string());
        }
        // Only the SHA-256 of the supplied password is compared; a stored hash
        // is never accepted as a password itself.
        let candidate = hash_password_sha256(password);
        if user
            .password_hashes
            .iter()
            .any(|h| ct_eq(h.as_bytes(), candidate.as_bytes()))
        {
            Ok(user_name.to_string())
        } else {
            Err(WRONGPASS)
        }
    }

    /// Redis `authRequired()`: new connections start unauthenticated unless the
    /// default user is enabled and `nopass`.
    pub fn is_auth_required_for_default(&self) -> bool {
        match self.users.get("default") {
            Some(user) => !(user.nopass && user.enabled),
            None => true,
        }
    }

    /// Applies `requirepass`: the default user gets exactly this password
    /// (empty string = nopass), as Redis does.
    pub fn set_requirepass(&mut self, pass: &str) {
        if let Some(user) = self.users.get_mut("default") {
            user.password_hashes.clear();
            if pass.is_empty() {
                user.nopass = true;
            } else {
                user.password_hashes.push(hash_password_sha256(pass));
                user.nopass = false;
                HAS_CUSTOM_ACL.store(true, Ordering::Release);
            }
        }
        self.requirepass = if pass.is_empty() {
            None
        } else {
            Some(pass.to_string())
        };
    }

    pub fn list(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for user in self.users.values() {
            lines.push(user.to_acl_list_line());
        }
        lines.sort();
        lines
    }

    pub fn users(&self) -> Vec<String> {
        let mut u: Vec<String> = self.users.keys().cloned().collect();
        u.sort();
        u
    }

    pub fn get_user(&self, username: &str) -> Option<AclUser> {
        self.users.get(username).cloned()
    }

    pub fn get_user_mut(&mut self, username: &str) -> Option<&mut AclUser> {
        self.users.get_mut(username)
    }

    /// Applies `rules` atomically: on error the user is left unchanged (or not
    /// created), as in Redis.
    pub fn set_user(&mut self, username: &str, rules: &[String]) -> Result<(), String> {
        if username.contains(' ') || username.contains('\0') {
            return Err("Usernames can't contain spaces or null characters".to_string());
        }
        let default_all_chan = self.acl_pubsub_default_all;
        let mut user = self
            .users
            .get(username)
            .cloned()
            .unwrap_or_else(|| AclUser::new_empty_with_pubsub(username, default_all_chan));
        for rule in rules {
            user.apply_rule_with_pubsub(rule, default_all_chan)?;
        }
        HAS_CUSTOM_ACL.store(true, Ordering::Release);
        self.users.insert(username.to_string(), user);
        Ok(())
    }

    /// Applies a `user <name> [rules...]` declaration from a config file or
    /// `aclfile`, where a fresh user (including `default`) starts from
    /// `new_empty_with_pubsub` before its rules are applied (except `default`
    /// when not mentioned in an `aclfile`, which stays `new_default()`).
    pub fn load_user_from_declaration(
        &mut self,
        username: &str,
        rules: &[String],
    ) -> Result<(), String> {
        if username.contains(' ') || username.contains('\0') {
            return Err("Usernames can't contain spaces or null characters".to_string());
        }
        let default_all_chan = self.acl_pubsub_default_all;
        let mut user = AclUser::new_empty_with_pubsub(username, default_all_chan);
        for rule in rules {
            user.apply_rule_with_pubsub(rule, default_all_chan)?;
        }
        HAS_CUSTOM_ACL.store(true, Ordering::Release);
        self.users.insert(username.to_string(), user);
        Ok(())
    }

    /// Loads users from the configured `aclfile` atomically (leaving existing
    /// users untouched on any syntax or duplicate-user error).
    pub fn load_from_aclfile(&mut self) -> Result<(), String> {
        let Some(ref path) = self.aclfile else {
            return Err("This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a configuration file set) in order to store users in the Redis configuration.".to_string());
        };
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Loading ACL file {:?} failed: {}", path, e))?;
        let default_all_chan = self.acl_pubsub_default_all;
        let mut new_users: HashMap<String, AclUser> = HashMap::new();
        for (line_idx, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 || !parts[0].eq_ignore_ascii_case("user") {
                return Err(format!(
                    "{}:{}: Unrecognized ACL file line starting with '{}'",
                    path.display(),
                    line_idx + 1,
                    parts.first().copied().unwrap_or("")
                ));
            }
            let username = parts[1];
            if new_users.contains_key(username) {
                return Err(format!(
                    "{}:{}: Duplicate user '{}' found",
                    path.display(),
                    line_idx + 1,
                    username
                ));
            }
            let mut user = AclUser::new_empty_with_pubsub(username, default_all_chan);
            for rule in &parts[2..] {
                user.apply_rule_with_pubsub(rule, default_all_chan)
                    .map_err(|e| format!("{}:{}: {}", path.display(), line_idx + 1, e))?;
            }
            new_users.insert(username.to_string(), user);
        }
        // As in Redis, if the ACL file omits `default`, keep the standard
        // default user (`on nopass ~* &* +@all`).
        new_users
            .entry("default".to_string())
            .or_insert_with(AclUser::new_default);
        HAS_CUSTOM_ACL.store(true, Ordering::Release);
        self.users = new_users;
        Ok(())
    }

    /// Saves all current users to the configured `aclfile`.
    pub fn save_to_aclfile(&self) -> Result<(), String> {
        let Some(ref path) = self.aclfile else {
            return Err("This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a configuration file set) in order to store users in the Redis configuration.".to_string());
        };
        let mut lines = self.list();
        lines.push(String::new());
        let body = lines.join("\n");
        std::fs::write(path, body).map_err(|e| {
            format!(
                "Wait, impossible to open the ACL file {:?} for writing: {}",
                path, e
            )
        })
    }

    pub fn del_user(&mut self, usernames: &[String]) -> usize {
        HAS_CUSTOM_ACL.store(true, Ordering::Release);
        let mut count = 0;
        for u in usernames {
            if u != "default" && self.users.remove(u).is_some() {
                count += 1;
            }
        }
        count
    }
}

/// Generates a random hex password for `ACL GENPASS [bits]` (1..=4096 bits,
/// default 256 -> 64 hex chars).
pub fn genpass_hex(bits: Option<i64>) -> Result<String, &'static str> {
    let bits = bits.unwrap_or(256);
    if !(1..=4096).contains(&bits) {
        return Err(
            "ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096",
        );
    }
    let hex_len = (bits as usize).div_ceil(4);
    let byte_len = hex_len.div_ceil(2);
    let mut random_bytes = vec![0u8; byte_len];
    use ring::rand::{SecureRandom, SystemRandom};
    let rng = SystemRandom::new();
    if rng.fill(&mut random_bytes).is_err() {
        for (i, b) in random_bytes.iter_mut().enumerate() {
            *b = ((unix_now_secs_f64().to_bits() as usize)
                .wrapping_mul(31)
                .wrapping_add(i)) as u8;
        }
    }
    let mut s = String::with_capacity(byte_len * 2);
    for b in random_bytes {
        use std::fmt::Write;
        let _ = write!(&mut s, "{:02x}", b);
    }
    s.truncate(hex_len);
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl_passwords_stored_hashed_and_hash_not_accepted() {
        let mut mgr = AclManager::new();
        let pass = "super_secret_pw";
        let expected_hash = hash_password_sha256(pass);
        assert_eq!(expected_hash.len(), 65);

        mgr.set_user(
            "alice",
            &["on", &format!(">{}", pass), "+@all", "~*"].map(String::from),
        )
        .unwrap();
        let alice = mgr.get_user("alice").unwrap();
        assert_eq!(alice.password_hashes, vec![expected_hash.clone()]);
        // Plaintext never appears in ACL LIST.
        let line = alice.to_acl_list_line();
        assert!(!line.contains(pass), "{}", line);
        assert!(line.contains(&expected_hash));

        assert_eq!(mgr.check_auth(Some("alice"), pass), Ok("alice".to_string()));
        assert!(mgr.check_auth(Some("alice"), "wrong_pw").is_err());
        // The stored hash (with or without '#') must not work as a password.
        assert!(mgr.check_auth(Some("alice"), &expected_hash).is_err());
        assert!(mgr.check_auth(Some("alice"), &expected_hash[1..]).is_err());

        // Precomputed '#<sha256>' rule (Redis format) authenticates the plaintext.
        mgr.set_user(
            "bob",
            &["on", &expected_hash.to_uppercase(), "+@all"].map(String::from),
        )
        .unwrap();
        assert_eq!(mgr.check_auth(Some("bob"), pass), Ok("bob".to_string()));
        // Malformed hashes are rejected instead of being stored.
        assert!(mgr.set_user("carol", &["#abc".to_string()]).is_err());

        // '<' removes a password.
        mgr.set_user("alice", &[format!("<{}", pass)]).unwrap();
        assert!(mgr.check_auth(Some("alice"), pass).is_err());
    }

    #[test]
    fn test_cluster_shard_ports_share_one_acl_table() {
        let (base, shard1) = (65031u16, 65032u16);
        // A stale per-shard table created before aliasing must be replaced.
        let _ = get_acl_for_port(shard1);
        share_acl_with_port(shard1, base);
        share_acl_with_port(base, base); // no-op
        get_acl_for_port(base).write().set_requirepass("pw");
        let shard_acl = get_acl_for_port(shard1);
        assert!(Arc::ptr_eq(&shard_acl, &get_acl_for_port(base)));
        assert!(shard_acl.read().is_auth_required_for_default());
        shard_acl
            .write()
            .set_user("zed", &["on".to_string(), ">z".to_string()])
            .unwrap();
        assert!(get_acl_for_port(base).read().get_user("zed").is_some());
    }

    #[test]
    fn test_default_user_auth_required_semantics() {
        let mut mgr = AclManager::new();
        assert!(!mgr.is_auth_required_for_default());
        // A hash-only password still requires auth (no plaintext stored).
        mgr.set_user("default", &[hash_password_sha256("x")])
            .unwrap();
        assert!(mgr.is_auth_required_for_default());
        mgr.set_requirepass("");
        assert!(!mgr.is_auth_required_for_default());
        assert_eq!(mgr.requirepass, None);
        mgr.set_requirepass("pw");
        assert!(mgr.is_auth_required_for_default());
        assert_eq!(mgr.requirepass.as_deref(), Some("pw"));
        assert!(mgr.check_auth(None, "pw").is_ok());
        // Disabled default user: auth required even if nopass.
        mgr.set_user("default", &["nopass".to_string(), "off".to_string()])
            .unwrap();
        assert!(mgr.is_auth_required_for_default());
    }

    #[test]
    fn test_acl_command_and_key_enforcement() {
        let mut mgr = AclManager::new();
        // User with restricted commands (-@all +get) and restricted keys (~user:*)
        mgr.set_user(
            "restricted",
            &[
                "on".to_string(),
                "nopass".to_string(),
                "-@all".to_string(),
                "+get".to_string(),
                "~user:*".to_string(),
            ],
        )
        .unwrap();

        let user = mgr.get_user("restricted").unwrap();
        assert!(user.can_execute_command("get"));
        assert!(user.can_execute_command("GET"));
        // Only NO_AUTH commands bypass command ACLs (as in Redis); PING does not.
        assert!(user.can_execute_command("auth"));
        assert!(user.can_execute_command("hello"));
        assert!(!user.can_execute_command("ping"));
        assert!(!user.can_execute_command("set"));
        assert!(!user.can_execute_command("del"));

        for need in [0, KEY_READ, KEY_WRITE, KEY_READ | KEY_WRITE] {
            assert!(user.can_access_key(b"user:12345", need));
            assert!(user.can_access_key(b"user:profile", need));
            assert!(!user.can_access_key(b"cache:12345", need));
            assert!(!user.can_access_key(b"admin:root", need));
        }
    }

    fn user_with(rules: &[&str]) -> Result<AclUser, String> {
        let mut mgr = AclManager::new();
        mgr.set_user(
            "u",
            &rules.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
        )?;
        Ok(mgr.get_user("u").unwrap())
    }

    #[test]
    fn test_acl_categories_follow_redis_command_table() {
        // +@all -@dangerous: data commands OK, admin/dangerous ones denied.
        let u = user_with(&["+@all", "-@dangerous"]).unwrap();
        for ok in [
            "get",
            "set",
            "ping",
            "client|setname",
            "acl|whoami",
            "config|help",
        ] {
            assert!(u.can_execute_command(ok), "{ok}");
        }
        for denied in [
            "flushall",
            "flushdb",
            "keys",
            "config|set",
            "config|get",
            "client|kill",
            "acl|setuser",
            "debug",
            "shutdown",
            "mcp",
            "xdp",
        ] {
            assert!(!u.can_execute_command(denied), "{denied}");
        }
        // Bare container with a dangerous subcommand is not fully permitted.
        assert!(!u.can_execute_command("config"));
        assert!(!u.all_commands);

        // +@read grants reads only.
        let u = user_with(&["+@read"]).unwrap();
        assert!(u.can_execute_command("get"));
        assert!(u.can_execute_command("zrange"));
        assert!(u.can_execute_command("memory|usage"));
        assert!(!u.can_execute_command("set"));
        assert!(!u.can_execute_command("json")); // mixed read/write family is @write
        assert!(!u.can_execute_command("memory|stats"));

        // Container rules cascade; subcommand rules are precise.
        let u = user_with(&["+config", "-config|set"]).unwrap();
        assert!(u.can_execute_command("config|get"));
        assert!(!u.can_execute_command("config|set"));
        let u = user_with(&["-@all", "+client|setname"]).unwrap();
        assert!(u.can_execute_command("client|setname"));
        assert!(!u.can_execute_command("client|kill"));

        // Commands outside the table are covered only by allcommands.
        assert!(
            user_with(&["+@all"])
                .unwrap()
                .can_execute_command("not-a-command")
        );
        assert!(
            !user_with(&["+@read"])
                .unwrap()
                .can_execute_command("not-a-command")
        );

        // reset == off resetpass resetkeys -@all
        let u = user_with(&["on", "nopass", "+@all", "~*", "reset"]).unwrap();
        assert!(!u.enabled && !u.nopass && !u.all_keys);
        assert!(!u.can_execute_command("get"));
    }

    #[test]
    fn test_acl_setuser_rejects_unknown_rules_atomically() {
        let mut mgr = AclManager::new();
        for (rule, msg) in [
            ("+notacommand", "Unknown command or category name in ACL"),
            ("+@notacategory", "Unknown command or category name in ACL"),
            ("bogus", "Syntax error"),
            ("-nopass", "Unknown command or category name in ACL"),
            ("(+get ~x)", "Selectors are not supported"),
            ("%X~x", "Syntax error"),
            ("%RR~x", "Syntax error"),
            ("%~x", "Syntax error"),
            ("%Rx", "Syntax error"),
        ] {
            let err = mgr
                .set_user("u", &["on".to_string(), rule.to_string()])
                .unwrap_err();
            assert!(
                err.starts_with(&format!("Error in ACL SETUSER modifier '{}': ", rule))
                    && err.contains(msg),
                "{rule}: {err}"
            );
        }
        // A failed SETUSER neither creates nor partially modifies the user.
        assert!(mgr.get_user("u").is_none());
        mgr.set_user("u", &["on".to_string(), "+get".to_string()])
            .unwrap();
        assert!(
            mgr.set_user("u", &["+set".to_string(), "+nope".to_string()])
                .is_err()
        );
        assert!(!mgr.get_user("u").unwrap().can_execute_command("set"));
        // A key pattern after allkeys is rejected like in Redis.
        assert!(
            mgr.set_user("u", &["~*".to_string(), "~foo".to_string()])
                .is_err()
        );
    }

    #[test]
    fn test_acl_channel_permissions() {
        // New users have no channel access (Redis 7 resetchannels default);
        // the default user keeps &*.
        let u = user_with(&["on"]).unwrap();
        assert!(!u.can_access_channel(b"news", false));
        assert!(AclUser::new_default().can_access_channel(b"anything", false));

        let u = user_with(&["&news.*", "&alerts"]).unwrap();
        assert!(u.can_access_channel(b"news.sport", false));
        assert!(u.can_access_channel(b"alerts", false));
        assert!(!u.can_access_channel(b"alerts2", false));
        // PSUBSCRIBE patterns must match a user pattern literally.
        assert!(u.can_access_channel(b"news.*", true));
        assert!(!u.can_access_channel(b"news.s*", true));
        assert!(!u.can_access_channel(b"*", true));
        assert_eq!(u.channels_rule_string(), "&news.* &alerts");

        let u = user_with(&["&a", "allchannels"]).unwrap();
        assert!(u.all_channels && u.channel_patterns.is_empty());
        let err = user_with(&["&*", "&x"]).unwrap_err();
        assert!(err.contains("allchannels"), "{err}");
        let u = user_with(&["&*", "resetchannels"]).unwrap();
        assert_eq!(u.channels_rule_string(), "");
        assert!(u.to_acl_list_line().contains("resetchannels"));
        let u = user_with(&["&*", "reset"]).unwrap();
        assert!(!u.all_channels);
    }

    #[test]
    fn test_acl_read_write_key_permissions() {
        let u = user_with(&["%R~r:*", "%W~w:*", "~rw:[ab]?"]).unwrap();
        assert!(u.can_access_key(b"r:1", KEY_READ));
        assert!(!u.can_access_key(b"r:1", KEY_WRITE));
        assert!(!u.can_access_key(b"r:1", KEY_READ | KEY_WRITE));
        assert!(u.can_access_key(b"w:1", KEY_WRITE));
        assert!(!u.can_access_key(b"w:1", KEY_READ));
        // Commands needing no permission bits (EXISTS) only need a match.
        assert!(u.can_access_key(b"w:1", 0));
        assert!(!u.can_access_key(b"z", 0));
        // Full glob syntax, not just trailing '*'.
        assert!(u.can_access_key(b"rw:a1", KEY_READ | KEY_WRITE));
        assert!(!u.can_access_key(b"rw:c1", KEY_READ));
        assert!(!u.can_access_key(b"rw:a12", KEY_READ));
        // Repeating a pattern widens it.
        let u = user_with(&["%R~k", "%W~k"]).unwrap();
        assert_eq!(u.key_patterns.len(), 1);
        assert!(u.can_access_key(b"k", KEY_READ | KEY_WRITE));
        assert_eq!(u.keys_rule_string(), "~k");
        let u = user_with(&["%W~k", "%r~j"]).unwrap();
        assert_eq!(u.keys_rule_string(), "%W~k %R~j");
        assert!(user_with(&["allkeys", "%R~x"]).is_err());
    }

    #[test]
    fn test_acl_list_line_round_trips() {
        for rules in [
            vec!["on", "nopass", "+@all", "~*"],
            vec![
                "on",
                ">pw",
                "+@all",
                "-@dangerous",
                "+config|get",
                "~a:*",
                "~b",
            ],
            vec!["off", "-@all", "+@read", "-memory|usage", "+client|setname"],
            vec!["on", "nopass", "+@all", "&news.*", "&alerts"],
            vec![
                "on", "nopass", "+@all", "%R~r:*", "%W~w:*", "%RW~rw", "~x", "%W~r:*",
            ],
        ] {
            let u = user_with(&rules).unwrap();
            let line = u.to_acl_list_line();
            let replay: Vec<&str> = line.split(' ').skip(2).collect();
            let u2 = user_with(&replay).unwrap();
            assert_eq!(u.commands, u2.commands, "{line}");
            assert_eq!(u.all_commands, u2.all_commands, "{line}");
            assert_eq!(u.key_patterns, u2.key_patterns, "{line}");
            assert_eq!(u.password_hashes, u2.password_hashes, "{line}");
            assert_eq!(u.enabled, u2.enabled, "{line}");
            assert_eq!(u.all_channels, u2.all_channels, "{line}");
            assert_eq!(u.channel_patterns, u2.channel_patterns, "{line}");
        }
    }

    #[test]
    fn test_acl_cat_genpass_first_arg_log_and_aclfile() {
        // ACL CAT
        let tx_cmds = commands_in_category("transaction").unwrap();
        assert!(tx_cmds.contains(&"multi"));
        assert!(commands_in_category("nonexistent").is_none());

        // ACL GENPASS
        assert_eq!(genpass_hex(None).unwrap().len(), 64);
        assert_eq!(genpass_hex(Some(5)).unwrap().len(), 2);
        assert!(genpass_hex(Some(0)).is_err());
        assert!(genpass_hex(Some(5000)).is_err());

        // First-arg rules (+select|0, +debug|object)
        let u = user_with(&["on", "nopass", "-@all", "+select|0", "+debug|object"]).unwrap();
        assert!(!u.can_execute_command("select"));
        assert!(u.can_execute_command_with_first_arg("select", Some(b"0")));
        assert!(!u.can_execute_command_with_first_arg("select", Some(b"1")));
        assert!(u.can_execute_command_with_first_arg("debug", Some(b"OBJECT")));
        assert!(!u.can_execute_command_with_first_arg("debug", Some(b"SEGFAULT")));
        assert!(u.commands_rule_string().contains("+select|0"));

        // ACL LOG aggregation and max len
        let mut mgr = AclManager::new();
        mgr.acllog_max_len = 2;
        mgr.add_log_entry(
            AclDeniedReason::Command,
            "toplevel",
            "get",
            "alice",
            "id=1".to_string(),
        );
        mgr.add_log_entry(
            AclDeniedReason::Command,
            "toplevel",
            "get",
            "alice",
            "id=1".to_string(),
        );
        assert_eq!(mgr.acl_log.len(), 1);
        assert_eq!(mgr.acl_log[0].count, 2);
        assert_eq!(mgr.acl_log[0].entry_id, 0);
        mgr.add_log_entry(
            AclDeniedReason::Key,
            "toplevel",
            "k1",
            "alice",
            "id=1".to_string(),
        );
        mgr.add_log_entry(
            AclDeniedReason::Channel,
            "toplevel",
            "c1",
            "alice",
            "id=1".to_string(),
        );
        assert_eq!(mgr.acl_log.len(), 2);
        assert_eq!(mgr.acl_log[0].object, "c1");
        assert_eq!(mgr.acl_log[1].object, "k1");

        // ACL SAVE / LOAD round-trip and duplicate rejection
        let path = std::env::temp_dir().join(format!("rudis-acl-unit-{}.acl", std::process::id()));
        mgr.aclfile = Some(path.clone());
        mgr.set_user(
            "alice",
            &[
                "on".to_string(),
                ">secret".to_string(),
                "~k:*".to_string(),
                "&ch:*".to_string(),
                "+get".to_string(),
            ],
        )
        .unwrap();
        mgr.save_to_aclfile().unwrap();
        let mut mgr2 = AclManager::new();
        mgr2.aclfile = Some(path.clone());
        mgr2.load_from_aclfile().unwrap();
        let alice = mgr2.get_user("alice").unwrap();
        assert!(alice.enabled);
        assert!(alice.can_execute_command("get"));
        assert!(!alice.can_execute_command("set"));
        assert!(alice.can_access_key(b"k:1", KEY_READ));
        assert!(alice.can_access_channel(b"ch:1", false));

        // Duplicate user in ACL file fails atomically without clobbering existing users
        std::fs::write(
            &path,
            "user alice on nopass ~* +@all\nuser alice on nopass ~* -@all\n",
        )
        .unwrap();
        let err = mgr2.load_from_aclfile().unwrap_err();
        assert!(err.contains("Duplicate user 'alice'"), "{err}");
        assert!(!mgr2.get_user("alice").unwrap().can_execute_command("set"));
        let _ = std::fs::remove_file(&path);
    }
}
