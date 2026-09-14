use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, RwLock};

pub static PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn get_acl_for_port(port: u16) -> Arc<RwLock<AclManager>> {
    let mut map = PORT_ACLS.lock().unwrap();
    map.entry(port)
        .or_insert_with(|| Arc::new(RwLock::new(AclManager::new())))
        .clone()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    pub passwords: Vec<String>,
    pub nopass: bool,
    pub all_commands: bool,
    pub all_keys: bool,
}

impl AclUser {
    pub fn new_default() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            passwords: Vec::new(),
            nopass: true,
            all_commands: true,
            all_keys: true,
        }
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
        }
        if self.all_commands {
            parts.push("+@all".to_string());
        } else {
            parts.push("-@all".to_string());
        }
        if self.all_keys {
            parts.push("~*".to_string());
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

    pub fn check_auth(&self, username: Option<&str>, password: &str) -> Result<String, &'static str> {
        let user_name = username.unwrap_or("default");
        if let Some(user) = self.users.get(user_name) {
            if !user.enabled {
                return Err("WRONGPASS User is disabled");
            }
            if user.nopass || user.passwords.iter().any(|p| p == password) {
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
            !user.nopass && !user.passwords.is_empty()
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
        let user = self.users.entry(username.to_string()).or_insert_with(|| AclUser {
            name: username.to_string(),
            enabled: false,
            passwords: Vec::new(),
            nopass: false,
            all_commands: false,
            all_keys: false,
        });

        for rule in rules {
            if rule == "on" {
                user.enabled = true;
            } else if rule == "off" {
                user.enabled = false;
            } else if rule == "nopass" {
                user.nopass = true;
                user.passwords.clear();
            } else if let Some(p) = rule.strip_prefix('>') {
                user.nopass = false;
                if !user.passwords.contains(&p.to_string()) {
                    user.passwords.push(p.to_string());
                }
            } else if let Some(p) = rule.strip_prefix('<') {
                user.passwords.retain(|pass| pass != p);
            } else if rule == "+@all" || rule == "+all" {
                user.all_commands = true;
            } else if rule == "-@all" || rule == "-all" {
                user.all_commands = false;
            } else if rule == "~*" || rule == "allkeys" {
                user.all_keys = true;
            }
        }
        Ok(())
    }

    pub fn del_user(&mut self, usernames: &[String]) -> usize {
        let mut count = 0;
        for u in usernames {
            if u != "default" && self.users.remove(u).is_some() {
                count += 1;
            }
        }
        count
    }
}
