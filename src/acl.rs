use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};

pub static HAS_CUSTOM_ACL: AtomicBool = AtomicBool::new(false);

pub static PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn get_acl_for_port(port: u16) -> Arc<RwLock<AclManager>> {
    let mut map = PORT_ACLS.lock().unwrap();
    map.entry(port)
        .or_insert_with(|| Arc::new(RwLock::new(AclManager::new())))
        .clone()
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    /// `#<sha256 hex>` entries only; plaintext passwords are never stored.
    pub password_hashes: Vec<String>,
    pub nopass: bool,
    pub all_commands: bool,
    pub allowed_commands: hashbrown::HashSet<String>,
    pub disallowed_commands: hashbrown::HashSet<String>,
    pub all_keys: bool,
    pub allowed_key_patterns: Vec<String>,
}

impl AclUser {
    pub fn new_default() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            password_hashes: Vec::new(),
            nopass: true,
            all_commands: true,
            allowed_commands: hashbrown::HashSet::new(),
            disallowed_commands: hashbrown::HashSet::new(),
            all_keys: true,
            allowed_key_patterns: Vec::new(),
        }
    }

    pub fn can_execute_command(&self, cmd_name: &str) -> bool {
        let name = cmd_name.to_lowercase();
        if name == "ping" || name == "reset" || name == "quit" || name == "auth" || name == "hello"
        {
            return true;
        }
        if self.all_commands {
            !self.disallowed_commands.contains(&name)
        } else {
            self.allowed_commands.contains(&name)
        }
    }

    pub fn can_access_key(&self, key: &[u8]) -> bool {
        if self.all_keys {
            return true;
        }
        let key_str = String::from_utf8_lossy(key);
        for pat in &self.allowed_key_patterns {
            if pat == "*" {
                return true;
            }
            if let Some(prefix) = pat.strip_suffix('*') {
                if key_str.starts_with(prefix) {
                    return true;
                }
            } else if key_str == *pat {
                return true;
            }
        }
        false
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
        if self.all_commands {
            flags.push("allcommands".to_string());
        }
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
        if self.all_commands {
            parts.push("+@all".to_string());
            for d in &self.disallowed_commands {
                parts.push(format!("-{}", d));
            }
        } else {
            parts.push("-@all".to_string());
            for a in &self.allowed_commands {
                parts.push(format!("+{}", a));
            }
        }
        if self.all_keys {
            parts.push("~*".to_string());
        } else {
            for pat in &self.allowed_key_patterns {
                parts.push(format!("~{}", pat));
            }
        }
        parts.push("&*".to_string());
        parts.join(" ")
    }
}

pub struct AclManager {
    pub users: HashMap<String, AclUser>,
    /// Value of the `requirepass` config, kept only so CONFIG GET/REWRITE can
    /// report it like Redis does. ACL commands never expose it.
    pub requirepass: Option<String>,
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
        }
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

    pub fn set_user(&mut self, username: &str, rules: &[String]) -> Result<(), String> {
        HAS_CUSTOM_ACL.store(true, Ordering::Release);
        let user = self
            .users
            .entry(username.to_string())
            .or_insert_with(|| AclUser {
                name: username.to_string(),
                enabled: false,
                password_hashes: Vec::new(),
                nopass: false,
                all_commands: false,
                allowed_commands: hashbrown::HashSet::new(),
                disallowed_commands: hashbrown::HashSet::new(),
                all_keys: false,
                allowed_key_patterns: Vec::new(),
            });

        for rule in rules {
            if rule == "on" {
                user.enabled = true;
            } else if rule == "off" {
                user.enabled = false;
            } else if rule == "nopass" {
                user.nopass = true;
                user.password_hashes.clear();
            } else if rule == "-nopass" {
                user.nopass = false;
            } else if rule == "resetpass" {
                user.nopass = false;
                user.password_hashes.clear();
            } else if let Some(p) = rule.strip_prefix('>') {
                user.nopass = false;
                let h = hash_password_sha256(p);
                if !user.password_hashes.contains(&h) {
                    user.password_hashes.push(h);
                }
            } else if let Some(h) = rule.strip_prefix('#') {
                let full_hash = parse_password_hash(h)?;
                user.nopass = false;
                if !user.password_hashes.contains(&full_hash) {
                    user.password_hashes.push(full_hash);
                }
            } else if let Some(p) = rule.strip_prefix('<') {
                let h = hash_password_sha256(p);
                user.password_hashes.retain(|x| *x != h);
            } else if let Some(h) = rule.strip_prefix('!') {
                let full_hash = parse_password_hash(h)?;
                user.password_hashes.retain(|x| *x != full_hash);
            } else if let Some(cat) = rule.strip_prefix("+@") {
                let c = cat.to_lowercase();
                if c == "all" {
                    user.all_commands = true;
                    user.disallowed_commands.clear();
                } else if c == "scripting" {
                    for cmd in &[
                        "eval",
                        "evalsha",
                        "eval_ro",
                        "evalsha_ro",
                        "function",
                        "fcall",
                        "fcall_ro",
                        "script",
                    ] {
                        if user.all_commands {
                            user.disallowed_commands.remove(*cmd);
                        } else {
                            user.allowed_commands.insert(cmd.to_string());
                        }
                    }
                } else if c == "string" {
                    for cmd in &[
                        "get",
                        "set",
                        "mget",
                        "mset",
                        "incr",
                        "decr",
                        "incrby",
                        "decrby",
                        "incrbyfloat",
                        "append",
                        "strlen",
                        "getset",
                        "getdel",
                        "getex",
                        "setnx",
                        "setex",
                        "psetex",
                        "msetnx",
                    ] {
                        if user.all_commands {
                            user.disallowed_commands.remove(*cmd);
                        } else {
                            user.allowed_commands.insert(cmd.to_string());
                        }
                    }
                }
            } else if let Some(cat) = rule.strip_prefix("-@") {
                let c = cat.to_lowercase();
                if c == "all" {
                    user.all_commands = false;
                    user.allowed_commands.clear();
                } else if c == "scripting" {
                    for cmd in &[
                        "eval",
                        "evalsha",
                        "eval_ro",
                        "evalsha_ro",
                        "function",
                        "fcall",
                        "fcall_ro",
                        "script",
                    ] {
                        if user.all_commands {
                            user.disallowed_commands.insert(cmd.to_string());
                        } else {
                            user.allowed_commands.remove(*cmd);
                        }
                    }
                }
            } else if rule == "+all" {
                user.all_commands = true;
                user.disallowed_commands.clear();
            } else if rule == "-all" {
                user.all_commands = false;
                user.allowed_commands.clear();
            } else if let Some(cmd) = rule.strip_prefix('+') {
                let c = cmd.to_lowercase();
                if user.all_commands {
                    user.disallowed_commands.remove(&c);
                } else {
                    user.allowed_commands.insert(c);
                }
            } else if let Some(cmd) = rule.strip_prefix('-') {
                let c = cmd.to_lowercase();
                if user.all_commands {
                    user.disallowed_commands.insert(c);
                } else {
                    user.allowed_commands.remove(&c);
                }
            } else if rule == "~*" || rule == "allkeys" {
                user.all_keys = true;
                user.allowed_key_patterns.clear();
            } else if rule == "resetkeys" {
                user.all_keys = false;
                user.allowed_key_patterns.clear();
            } else if let Some(pat) = rule.strip_prefix('~') {
                user.all_keys = false;
                if !user.allowed_key_patterns.contains(&pat.to_string()) {
                    user.allowed_key_patterns.push(pat.to_string());
                }
            }
        }
        Ok(())
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
        assert!(user.can_execute_command("ping")); // Builtin allowed
        assert!(!user.can_execute_command("set"));
        assert!(!user.can_execute_command("del"));

        assert!(user.can_access_key(b"user:12345"));
        assert!(user.can_access_key(b"user:profile"));
        assert!(!user.can_access_key(b"cache:12345"));
        assert!(!user.can_access_key(b"admin:root"));
    }
}
