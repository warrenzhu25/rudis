use bytes::Bytes;
use hashbrown::HashMap;
use serde_json::{Number, Value, json};

/// Represents a parsed segment in a JSONPath expression (e.g. `$.users[0].name`).
#[derive(Clone, Debug, PartialEq)]
pub enum PathSegment {
    Root,
    Field(String),
    Index(isize),
    Wildcard,
    Slice {
        start: Option<isize>,
        end: Option<isize>,
    },
}

/// Parses a JSONPath string into individual segments.
/// Supports standard RedisJSON syntax: `$`, `.foo`, `$.foo.bar`, `$.items[0]`, `$.items[*]`, `$.items[0:2]`, `["key"]`.
pub fn parse_json_path(path_str: &str) -> Result<Vec<PathSegment>, String> {
    let s = path_str.trim();
    if s.is_empty() || s == "$" || s == "." {
        return Ok(vec![PathSegment::Root]);
    }

    let mut segments = Vec::new();
    let mut chars = s.chars().peekable();

    // Consume leading '$' if present
    if chars.peek() == Some(&'$') {
        chars.next();
        segments.push(PathSegment::Root);
        if chars.peek() == Some(&'.') {
            chars.next();
        }
    } else if chars.peek() == Some(&'.') {
        chars.next();
        segments.push(PathSegment::Root);
    } else {
        segments.push(PathSegment::Root);
    }

    while let Some(&ch) = chars.peek() {
        if ch == '.' {
            chars.next();
            // Could be another dot for recursive or wildcard
            if chars.peek() == Some(&'*') {
                chars.next();
                segments.push(PathSegment::Wildcard);
            }
            continue;
        }

        if ch == '*' {
            chars.next();
            segments.push(PathSegment::Wildcard);
            continue;
        }

        if ch == '[' {
            chars.next(); // consume '['
            let mut inner = String::new();
            while let Some(&c) = chars.peek() {
                if c == ']' {
                    chars.next();
                    break;
                }
                inner.push(c);
                chars.next();
            }

            let inner = inner.trim();
            if inner == "*" {
                segments.push(PathSegment::Wildcard);
            } else if (inner.starts_with('"') && inner.ends_with('"'))
                || (inner.starts_with('\'') && inner.ends_with('\''))
            {
                // Quoted field name: ["foo"]
                let field_name = &inner[1..inner.len() - 1];
                segments.push(PathSegment::Field(field_name.to_string()));
            } else if inner.contains(':') {
                // Slice: [0:2] or [:] or [1:] or [:3]
                let parts: Vec<&str> = inner.split(':').collect();
                let start = if parts.first().is_none_or(|p| p.trim().is_empty()) {
                    None
                } else {
                    parts[0].trim().parse::<isize>().ok()
                };
                let end = if parts.get(1).is_none_or(|p| p.trim().is_empty()) {
                    None
                } else {
                    parts[1].trim().parse::<isize>().ok()
                };
                segments.push(PathSegment::Slice { start, end });
            } else if let Ok(idx) = inner.parse::<isize>() {
                segments.push(PathSegment::Index(idx));
            } else {
                // Unquoted string field inside brackets
                segments.push(PathSegment::Field(inner.to_string()));
            }
            continue;
        }

        // Regular identifier field name
        let mut field = String::new();
        while let Some(&c) = chars.peek() {
            if c == '.' || c == '[' {
                break;
            }
            field.push(c);
            chars.next();
        }

        if !field.is_empty() {
            segments.push(PathSegment::Field(field));
        }
    }

    Ok(segments)
}

/// Evaluates a JSONPath query against a JSON `Value`, returning references to matching nodes.
pub fn query_json_path<'a>(root: &'a Value, segments: &[PathSegment]) -> Vec<&'a Value> {
    if segments.is_empty() || (segments.len() == 1 && segments[0] == PathSegment::Root) {
        return vec![root];
    }

    let mut current = vec![root];

    for seg in segments {
        if *seg == PathSegment::Root {
            continue;
        }

        let mut next = Vec::new();
        for val in current {
            match seg {
                PathSegment::Root => next.push(val),
                PathSegment::Field(name) => {
                    if let Value::Object(map) = val
                        && let Some(child) = map.get(name)
                    {
                        next.push(child);
                    }
                }
                PathSegment::Index(idx) => {
                    if let Value::Array(arr) = val {
                        let actual_idx = if *idx < 0 {
                            (arr.len() as isize + idx) as usize
                        } else {
                            *idx as usize
                        };
                        if actual_idx < arr.len() {
                            next.push(&arr[actual_idx]);
                        }
                    }
                }
                PathSegment::Wildcard => {
                    if let Value::Object(map) = val {
                        for child in map.values() {
                            next.push(child);
                        }
                    } else if let Value::Array(arr) = val {
                        for child in arr {
                            next.push(child);
                        }
                    }
                }
                PathSegment::Slice { start, end } => {
                    if let Value::Array(arr) = val {
                        let len = arr.len() as isize;
                        let s = start.unwrap_or(0);
                        let s_idx = if s < 0 {
                            (len + s).max(0) as usize
                        } else {
                            s.min(len) as usize
                        };
                        let e = end.unwrap_or(len);
                        let e_idx = if e < 0 {
                            (len + e).max(0) as usize
                        } else {
                            e.min(len) as usize
                        };
                        if s_idx < e_idx {
                            for item in &arr[s_idx..e_idx] {
                                next.push(item);
                            }
                        }
                    }
                }
            }
        }
        current = next;
        if current.is_empty() {
            break;
        }
    }

    current
}

/// Evaluates a JSONPath query against a mutable JSON `Value`, returning mutable references to matching nodes.
pub fn query_json_path_mut<'a>(
    root: &'a mut Value,
    segments: &[PathSegment],
) -> Vec<&'a mut Value> {
    if segments.is_empty() || (segments.len() == 1 && segments[0] == PathSegment::Root) {
        return vec![root];
    }

    let mut current = vec![root];

    for seg in segments {
        if *seg == PathSegment::Root {
            continue;
        }

        let mut next = Vec::new();
        for val in current {
            match seg {
                PathSegment::Root => next.push(val),
                PathSegment::Field(name) => {
                    if let Value::Object(map) = val
                        && let Some(child) = map.get_mut(name)
                    {
                        next.push(child);
                    }
                }
                PathSegment::Index(idx) => {
                    if let Value::Array(arr) = val {
                        let len = arr.len() as isize;
                        let actual_idx = if *idx < 0 {
                            (len + idx) as usize
                        } else {
                            *idx as usize
                        };
                        if actual_idx < arr.len() {
                            next.push(&mut arr[actual_idx]);
                        }
                    }
                }
                PathSegment::Wildcard => {
                    if let Value::Object(map) = val {
                        for child in map.values_mut() {
                            next.push(child);
                        }
                    } else if let Value::Array(arr) = val {
                        for child in arr.iter_mut() {
                            next.push(child);
                        }
                    }
                }
                PathSegment::Slice { start, end } => {
                    if let Value::Array(arr) = val {
                        let len = arr.len() as isize;
                        let s = start.unwrap_or(0);
                        let s_idx = if s < 0 {
                            (len + s).max(0) as usize
                        } else {
                            s.min(len) as usize
                        };
                        let e = end.unwrap_or(len);
                        let e_idx = if e < 0 {
                            (len + e).max(0) as usize
                        } else {
                            e.min(len) as usize
                        };
                        if s_idx < e_idx {
                            for item in &mut arr[s_idx..e_idx] {
                                next.push(item);
                            }
                        }
                    }
                }
            }
        }
        current = next;
        if current.is_empty() {
            break;
        }
    }

    current
}

/// Sets a JSON value at the specified JSONPath.
/// Creates intermediate objects if necessary.
/// Respects `nx` (only set if target does not exist) and `xx` (only set if target exists).
pub fn set_json_path(
    root: &mut Value,
    path: &str,
    new_value: Value,
    nx: bool,
    xx: bool,
) -> Result<bool, &'static str> {
    let segments = parse_json_path(path).map_err(|_| "ERR invalid path")?;
    if segments.is_empty() || (segments.len() == 1 && segments[0] == PathSegment::Root) {
        if nx {
            // Root already exists
            return Ok(false);
        }
        *root = new_value;
        return Ok(true);
    }

    // Check target existence if nx or xx specified
    let target_exists = !query_json_path(root, &segments).is_empty();
    if nx && target_exists {
        return Ok(false);
    }
    if xx && !target_exists {
        return Ok(false);
    }

    // Traverse up to parent segment
    let parent_segments = &segments[..segments.len() - 1];
    let last_seg = &segments[segments.len() - 1];

    let mut curr = root;
    for seg in parent_segments {
        match seg {
            PathSegment::Root => {}
            PathSegment::Field(name) => {
                if !curr.is_object() {
                    *curr = Value::Object(serde_json::Map::new());
                }
                let map = curr.as_object_mut().unwrap();
                if !map.contains_key(name) {
                    map.insert(name.clone(), Value::Object(serde_json::Map::new()));
                }
                curr = map.get_mut(name).unwrap();
            }
            PathSegment::Index(idx) => {
                if !curr.is_array() {
                    *curr = Value::Array(Vec::new());
                }
                let arr = curr.as_array_mut().unwrap();
                let actual_idx = if *idx < 0 {
                    (arr.len() as isize + idx) as usize
                } else {
                    *idx as usize
                };
                while arr.len() <= actual_idx {
                    arr.push(Value::Null);
                }
                curr = &mut arr[actual_idx];
            }
            _ => return Err("ERR wildcards not supported as parent path for SET"),
        }
    }

    match last_seg {
        PathSegment::Field(name) => {
            if !curr.is_object() {
                *curr = Value::Object(serde_json::Map::new());
            }
            curr.as_object_mut()
                .unwrap()
                .insert(name.clone(), new_value);
            Ok(true)
        }
        PathSegment::Index(idx) => {
            if !curr.is_array() {
                *curr = Value::Array(Vec::new());
            }
            let arr = curr.as_array_mut().unwrap();
            let actual_idx = if *idx < 0 {
                (arr.len() as isize + idx) as usize
            } else {
                *idx as usize
            };
            while arr.len() <= actual_idx {
                arr.push(Value::Null);
            }
            arr[actual_idx] = new_value;
            Ok(true)
        }
        _ => Err("ERR invalid target path for SET"),
    }
}

/// Deletes matching paths from a JSON value.
/// Returns the number of paths deleted.
pub fn delete_json_path(root: &mut Value, path: &str) -> usize {
    let segments = match parse_json_path(path) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    if segments.is_empty() || (segments.len() == 1 && segments[0] == PathSegment::Root) {
        *root = Value::Null;
        return 1;
    }

    let parent_segments = &segments[..segments.len() - 1];
    let last_seg = &segments[segments.len() - 1];

    let parents = query_json_path_mut(root, parent_segments);
    let mut deleted = 0;

    for parent in parents {
        match last_seg {
            PathSegment::Field(name) => {
                if let Value::Object(map) = parent
                    && map.remove(name).is_some()
                {
                    deleted += 1;
                }
            }
            PathSegment::Index(idx) => {
                if let Value::Array(arr) = parent {
                    let len = arr.len() as isize;
                    let actual_idx = if *idx < 0 { len + idx } else { *idx };
                    if actual_idx >= 0 && (actual_idx as usize) < arr.len() {
                        arr.remove(actual_idx as usize);
                        deleted += 1;
                    }
                }
            }
            PathSegment::Wildcard => {
                if let Value::Object(map) = parent {
                    deleted += map.len();
                    map.clear();
                } else if let Value::Array(arr) = parent {
                    deleted += arr.len();
                    arr.clear();
                }
            }
            _ => {}
        }
    }

    deleted
}

/// In-memory storage for JSON documents with RedisJSON command execution.
#[derive(Default, Debug)]
pub struct JsonStore {
    docs: HashMap<Bytes, Value>,
}

impl JsonStore {
    pub fn new() -> Self {
        Self {
            docs: HashMap::new(),
        }
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Value)> {
        self.docs.iter()
    }

    #[inline]
    pub fn insert_raw(&mut self, key: Bytes, val: Value) {
        self.docs.insert(key, val);
    }

    /// JSON.SET <key> <path> <json_value> [NX|XX]
    pub fn json_set(
        &mut self,
        key: &[u8],
        path: &str,
        json_str: &str,
        nx: bool,
        xx: bool,
    ) -> Result<bool, String> {
        let new_val: Value =
            serde_json::from_str(json_str).map_err(|e| format!("ERR invalid JSON value: {}", e))?;

        let key_bytes = Bytes::copy_from_slice(key);

        if let Some(existing) = self.docs.get_mut(&key_bytes) {
            match set_json_path(existing, path, new_val, nx, xx) {
                Ok(success) => Ok(success),
                Err(e) => Err(e.to_string()),
            }
        } else {
            // Key does not exist
            if xx {
                return Ok(false);
            }
            let segments = parse_json_path(path)?;
            if segments.is_empty() || (segments.len() == 1 && segments[0] == PathSegment::Root) {
                self.docs.insert(key_bytes, new_val);
                Ok(true)
            } else {
                let mut root = Value::Object(serde_json::Map::new());
                set_json_path(&mut root, path, new_val, false, false).map_err(|e| e.to_string())?;
                self.docs.insert(key_bytes, root);
                Ok(true)
            }
        }
    }

    /// JSON.GET <key> [paths...]
    pub fn json_get(&self, key: &[u8], paths: &[&str]) -> Option<String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self.docs.get(&key_bytes)?;

        if paths.is_empty() || (paths.len() == 1 && (paths[0] == "$" || paths[0] == ".")) {
            return Some(serde_json::to_string(doc).unwrap_or_default());
        }

        if paths.len() == 1 {
            let segments = parse_json_path(paths[0]).ok()?;
            let matches = query_json_path(doc, &segments);
            if matches.is_empty() {
                Some("[]".to_string())
            } else if matches.len() == 1 && !paths[0].contains('*') && !paths[0].contains(':') {
                Some(serde_json::to_string(matches[0]).unwrap_or_default())
            } else {
                Some(serde_json::to_string(&matches).unwrap_or_default())
            }
        } else {
            // Multiple paths: return JSON object mapping each path to its result
            let mut obj = serde_json::Map::new();
            for &p in paths {
                if let Ok(segments) = parse_json_path(p) {
                    let matches = query_json_path(doc, &segments);
                    obj.insert(p.to_string(), json!(matches));
                }
            }
            Some(serde_json::to_string(&Value::Object(obj)).unwrap_or_default())
        }
    }

    /// JSON.DEL <key> [path] (or JSON.FORGET)
    pub fn json_del(&mut self, key: &[u8], path: Option<&str>) -> usize {
        let key_bytes = Bytes::copy_from_slice(key);
        match path {
            None | Some("$") | Some(".") => {
                if self.docs.remove(&key_bytes).is_some() {
                    1
                } else {
                    0
                }
            }
            Some(p) => {
                if let Some(doc) = self.docs.get_mut(&key_bytes) {
                    delete_json_path(doc, p)
                } else {
                    0
                }
            }
        }
    }

    /// JSON.TYPE <key> [path]
    pub fn json_type(&self, key: &[u8], path: Option<&str>) -> Option<String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self.docs.get(&key_bytes)?;
        let p = path.unwrap_or("$");
        let segments = parse_json_path(p).ok()?;
        let matches = query_json_path(doc, &segments);
        if let Some(target) = matches.first() {
            let type_str = match target {
                Value::Null => "null",
                Value::Bool(_) => "boolean",
                Value::Number(_) => "number",
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            Some(type_str.to_string())
        } else {
            None
        }
    }

    /// JSON.NUMINCRBY <key> <path> <number>
    pub fn json_numincrby(&mut self, key: &[u8], path: &str, delta: f64) -> Result<String, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self
            .docs
            .get_mut(&key_bytes)
            .ok_or("ERR could not find key")?;
        let segments = parse_json_path(path)?;
        let matches = query_json_path_mut(doc, &segments);
        if matches.is_empty() {
            return Err("ERR path does not exist".to_string());
        }

        let mut results = Vec::new();
        for val in matches {
            if let Value::Number(num) = val {
                let cur = num.as_f64().unwrap_or(0.0);
                let new_num = cur + delta;
                if let Some(n) = Number::from_f64(new_num) {
                    *val = Value::Number(n);
                    results.push(new_num.to_string());
                } else {
                    let int_val = new_num.round() as i64;
                    *val = json!(int_val);
                    results.push(int_val.to_string());
                }
            } else {
                return Err("ERR value at path is not a number".to_string());
            }
        }

        if results.len() == 1 {
            Ok(results[0].clone())
        } else {
            Ok(format!("[{}]", results.join(",")))
        }
    }

    /// JSON.STRAPPEND <key> [path] <string>
    pub fn json_strappend(
        &mut self,
        key: &[u8],
        path: Option<&str>,
        append_str: &str,
    ) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self
            .docs
            .get_mut(&key_bytes)
            .ok_or("ERR could not find key")?;
        let p = path.unwrap_or("$");
        let segments = parse_json_path(p)?;
        let matches = query_json_path_mut(doc, &segments);
        if matches.is_empty() {
            return Err("ERR path does not exist".to_string());
        }

        let mut last_len = 0;
        for val in matches {
            if let Value::String(s) = val {
                s.push_str(append_str);
                last_len = s.len();
            } else {
                return Err("ERR value at path is not a string".to_string());
            }
        }
        Ok(last_len)
    }

    /// JSON.STRLEN <key> [path]
    pub fn json_strlen(&self, key: &[u8], path: Option<&str>) -> Option<usize> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self.docs.get(&key_bytes)?;
        let p = path.unwrap_or("$");
        let segments = parse_json_path(p).ok()?;
        let matches = query_json_path(doc, &segments);
        matches
            .first()
            .and_then(|val| val.as_str().map(|s| s.len()))
    }

    /// JSON.ARRAPPEND <key> <path> <values...>
    pub fn json_arrappend(
        &mut self,
        key: &[u8],
        path: &str,
        values_json: &[&str],
    ) -> Result<usize, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self
            .docs
            .get_mut(&key_bytes)
            .ok_or("ERR could not find key")?;
        let segments = parse_json_path(path)?;
        let matches = query_json_path_mut(doc, &segments);
        if matches.is_empty() {
            return Err("ERR path does not exist".to_string());
        }

        let mut parsed_values = Vec::with_capacity(values_json.len());
        for &v in values_json {
            let parsed: Value =
                serde_json::from_str(v).map_err(|e| format!("ERR invalid JSON element: {}", e))?;
            parsed_values.push(parsed);
        }

        let mut last_len = 0;
        for val in matches {
            if let Value::Array(arr) = val {
                for item in &parsed_values {
                    arr.push(item.clone());
                }
                last_len = arr.len();
            } else {
                return Err("ERR value at path is not an array".to_string());
            }
        }
        Ok(last_len)
    }

    /// JSON.ARRLEN <key> [path]
    pub fn json_arrlen(&self, key: &[u8], path: Option<&str>) -> Option<usize> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self.docs.get(&key_bytes)?;
        let p = path.unwrap_or("$");
        let segments = parse_json_path(p).ok()?;
        let matches = query_json_path(doc, &segments);
        matches
            .first()
            .and_then(|val| val.as_array().map(|a| a.len()))
    }

    /// JSON.ARRPOP <key> [path] [index]
    pub fn json_arrpop(
        &mut self,
        key: &[u8],
        path: Option<&str>,
        index: Option<isize>,
    ) -> Option<String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self.docs.get_mut(&key_bytes)?;
        let p = path.unwrap_or("$");
        let segments = parse_json_path(p).ok()?;
        let matches = query_json_path_mut(doc, &segments);
        let target = matches.into_iter().next()?;
        if let Value::Array(arr) = target {
            if arr.is_empty() {
                return None;
            }
            let idx = index.unwrap_or(-1);
            let len = arr.len() as isize;
            let actual = if idx < 0 { len + idx } else { idx };
            if actual >= 0 && (actual as usize) < arr.len() {
                let popped = arr.remove(actual as usize);
                Some(serde_json::to_string(&popped).unwrap_or_default())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// JSON.OBJKEYS <key> [path]
    pub fn json_objkeys(&self, key: &[u8], path: Option<&str>) -> Option<Vec<String>> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self.docs.get(&key_bytes)?;
        let p = path.unwrap_or("$");
        let segments = parse_json_path(p).ok()?;
        let matches = query_json_path(doc, &segments);
        matches
            .first()
            .and_then(|val| val.as_object().map(|m| m.keys().cloned().collect()))
    }

    /// JSON.OBJLEN <key> [path]
    pub fn json_objlen(&self, key: &[u8], path: Option<&str>) -> Option<usize> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self.docs.get(&key_bytes)?;
        let p = path.unwrap_or("$");
        let segments = parse_json_path(p).ok()?;
        let matches = query_json_path(doc, &segments);
        matches
            .first()
            .and_then(|val| val.as_object().map(|m| m.len()))
    }

    /// JSON.TOGGLE <key> [path]
    pub fn json_toggle(&mut self, key: &[u8], path: &str) -> Result<String, String> {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = self
            .docs
            .get_mut(&key_bytes)
            .ok_or("ERR could not find key")?;
        let segments = parse_json_path(path)?;
        let matches = query_json_path_mut(doc, &segments);
        if matches.is_empty() {
            return Err("ERR path does not exist".to_string());
        }

        let mut results = Vec::new();
        for val in matches {
            if let Value::Bool(b) = val {
                *b = !*b;
                results.push((*b).to_string());
            } else {
                return Err("ERR value at path is not a boolean".to_string());
            }
        }
        if results.len() == 1 {
            Ok(results[0].clone())
        } else {
            Ok(format!("[{}]", results.join(",")))
        }
    }

    /// JSON.CLEAR <key> [path]
    /// Clears container values (arrays/objects) or sets numeric values to 0.
    pub fn json_clear(&mut self, key: &[u8], path: Option<&str>) -> usize {
        let key_bytes = Bytes::copy_from_slice(key);
        let doc = match self.docs.get_mut(&key_bytes) {
            Some(d) => d,
            None => return 0,
        };
        let p = path.unwrap_or("$");
        let segments = match parse_json_path(p) {
            Ok(s) => s,
            Err(_) => return 0,
        };
        let matches = query_json_path_mut(doc, &segments);
        let mut cleared = 0;
        for val in matches {
            match val {
                Value::Array(arr) => {
                    if !arr.is_empty() {
                        arr.clear();
                        cleared += 1;
                    }
                }
                Value::Object(map) => {
                    if !map.is_empty() {
                        map.clear();
                        cleared += 1;
                    }
                }
                Value::Number(num) if num.as_f64() != Some(0.0) => {
                    *val = json!(0);
                    cleared += 1;
                }
                _ => {}
            }
        }
        cleared
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_crud_and_jsonpath() {
        let mut store = JsonStore::new();
        let key = b"doc:1";

        // JSON.SET root
        let res = store.json_set(
            key,
            "$",
            r#"{"name":"Alice","age":30,"tags":["rust","database"],"active":true}"#,
            false,
            false,
        );
        assert_eq!(res, Ok(true));

        // JSON.GET root
        let json_str = store.json_get(key, &["$"]).unwrap();
        assert!(json_str.contains("Alice"));

        // JSON.GET nested field
        let name_str = store.json_get(key, &["$.name"]).unwrap();
        assert_eq!(name_str, "\"Alice\"");

        // JSON.TYPE
        assert_eq!(
            store.json_type(key, Some("$.age")),
            Some("number".to_string())
        );
        assert_eq!(
            store.json_type(key, Some("$.tags")),
            Some("array".to_string())
        );

        // JSON.NUMINCRBY
        let new_age = store.json_numincrby(key, "$.age", 2.0).unwrap();
        assert_eq!(new_age, "32");

        // JSON.ARRAPPEND
        let new_len = store
            .json_arrappend(key, "$.tags", &[r#""redis""#, r#""performance""#])
            .unwrap();
        assert_eq!(new_len, 4);
        assert_eq!(store.json_arrlen(key, Some("$.tags")), Some(4));

        // JSON.ARRPOP
        let popped = store.json_arrpop(key, Some("$.tags"), None).unwrap();
        assert_eq!(popped, "\"performance\"");
        assert_eq!(store.json_arrlen(key, Some("$.tags")), Some(3));

        // JSON.TOGGLE
        let toggled = store.json_toggle(key, "$.active").unwrap();
        assert_eq!(toggled, "false");

        // JSON.OBJKEYS
        let keys = store.json_objkeys(key, Some("$")).unwrap();
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));

        // JSON.DEL nested
        let deleted = store.json_del(key, Some("$.active"));
        assert_eq!(deleted, 1);
        assert_eq!(store.json_type(key, Some("$.active")), None);

        // JSON.DEL root
        let del_root = store.json_del(key, None);
        assert_eq!(del_root, 1);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_json_wildcards_and_slices() {
        let mut store = JsonStore::new();
        let key = b"users";
        let doc = r#"{"items":[{"id":1,"score":10},{"id":2,"score":20},{"id":3,"score":30}]}"#;
        store.json_set(key, "$", doc, false, false).unwrap();

        // Wildcard query: $.items[*].id
        let res = store.json_get(key, &["$.items[*].id"]).unwrap();
        assert!(res.contains("1"));
        assert!(res.contains("2"));
        assert!(res.contains("3"));

        // Slice query: $.items[0:2]
        let res_slice = store.json_get(key, &["$.items[0:2]"]).unwrap();
        assert!(res_slice.contains("10"));
        assert!(res_slice.contains("20"));
        assert!(!res_slice.contains("30"));
    }
}
