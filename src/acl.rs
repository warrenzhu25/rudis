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

pub fn hash_password(password: &str) -> String {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(b"rudis_acl_salt_v1:");
    hasher.update(password.as_bytes());
    let res = hasher.finalize();
    let mut s = String::with_capacity(res.len() * 2 + 1);
    s.push('#');
    for b in res {
        use std::fmt::Write;
        let _ = write!(&mut s, "{:02x}", b);
    }
    s
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    pub passwords: Vec<String>,
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
            passwords: Vec::new(),
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
            for p in &self.passwords {
                parts.push(format!(">{}", p));
            }
            for h in &self.password_hashes {
                if !self.passwords.iter().any(|p| p == h) {
                    parts.push(h.clone());
                }
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
        Self { users }
    }

    pub fn check_auth(
        &self,
        username: Option<&str>,
        password: &str,
    ) -> Result<String, &'static str> {
        let user_name = username.unwrap_or("default");
        if let Some(user) = self.users.get(user_name) {
            if !user.enabled {
                return Err("WRONGPASS User is disabled");
            }
            let hashed = hash_password(password);
            if user.nopass
                || user.passwords.iter().any(|p| p == password)
                || user
                    .password_hashes
                    .iter()
                    .any(|h| h == &hashed || h == password)
            {
                Ok(user_name.to_string())
            } else {
                Err("WRONGPASS invalid username-password pair or user is disabled.")
            }
        } else {
            Err("WRONGPASS invalid username-password pair or user is disabled.")
        }
    }

    pub fn is_auth_required_for_default(&self) -> bool {
        if let Some(user) = self.users.get("default") {
            !user.nopass && (!user.passwords.is_empty() || !user.password_hashes.is_empty())
        } else {
            false
        }
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

    pub fn set_user(&mut self, username: &str, rules: &[String]) -> Result<(), String> {
        HAS_CUSTOM_ACL.store(true, Ordering::Release);
        let user = self
            .users
            .entry(username.to_string())
            .or_insert_with(|| AclUser {
                name: username.to_string(),
                enabled: false,
                passwords: Vec::new(),
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
                user.passwords.clear();
                user.password_hashes.clear();
            } else if rule == "-nopass" {
                user.nopass = false;
            } else if let Some(p) = rule.strip_prefix('>') {
                user.nopass = false;
                if !user.passwords.contains(&p.to_string()) {
                    user.passwords.push(p.to_string());
                }
                let h = hash_password(p);
                if !user.password_hashes.contains(&h) {
                    user.password_hashes.push(h);
                }
            } else if let Some(h) = rule.strip_prefix('#') {
                user.nopass = false;
                let full_hash = format!("#{}", h);
                if !user.password_hashes.contains(&full_hash) {
                    user.password_hashes.push(full_hash);
                }
            } else if let Some(p) = rule.strip_prefix('<') {
                user.passwords.retain(|pass| pass != p);
                let h = hash_password(p);
                user.password_hashes.retain(|pass| pass != &h && pass != p);
            } else if let Some(h) = rule.strip_prefix('!') {
                let full_hash = format!("#{}", h);
                user.password_hashes
                    .retain(|pass| pass != &full_hash && pass != h);
            } else if rule == "+@all" || rule == "+all" {
                user.all_commands = true;
                user.disallowed_commands.clear();
            } else if rule == "-@all" || rule == "-all" {
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
    fn test_acl_salted_password_hashing() {
        let mut mgr = AclManager::new();
        let pass = "super_secret_pw";
        let expected_hash = hash_password(pass);
        assert!(expected_hash.starts_with('#'));

        // Set user with plaintext password: should auto-hash
        mgr.set_user(
            "alice",
            &[
                "on".to_string(),
                format!(">{}", pass),
                "+@all".to_string(),
                "~*".to_string(),
            ],
        )
        .unwrap();

        let alice = mgr.get_user("alice").unwrap();
        assert!(alice.password_hashes.contains(&expected_hash));

        // Authenticate with plaintext password
        assert_eq!(mgr.check_auth(Some("alice"), pass), Ok("alice".to_string()));
        assert!(mgr.check_auth(Some("alice"), "wrong_pw").is_err());

        // Set user directly with precomputed hash
        mgr.set_user(
            "bob",
            &[
                "on".to_string(),
                expected_hash.clone(),
                "+@all".to_string(),
                "~*".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(mgr.check_auth(Some("bob"), pass), Ok("bob".to_string()));
        assert!(mgr.check_auth(Some("bob"), "wrong_pw").is_err());
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
