//! Built-in Model Context Protocol (MCP) server engine (`MCP.TOOLS`, `MCP.CALL`, `MCP.RPC`).
//!
//! Exposes Rudis KV, Semantic Cache, Vector Sets, Agent Memory/Checkpoints, and RediSearch
//! as native MCP tools speaking both RESP (`MCP.TOOLS`, `MCP.CALL`) and JSON-RPC 2.0 (`MCP.RPC`).

use bytes::Bytes;
use serde_json::{Value, json};
use std::time::Duration;

use crate::resp::{Command, SetCondition, VsimTarget};
use crate::search::SearchOptions;

#[derive(Debug, Clone)]
pub struct McpToolDef {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

pub fn builtin_mcp_tools() -> Vec<McpToolDef> {
    vec![
        McpToolDef {
            name: "rudis_kv_get",
            description: "Get the value of a key from Rudis KV store",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string", "description": "Key to fetch" }
                },
                "required": ["key"]
            }),
        },
        McpToolDef {
            name: "rudis_kv_set",
            description: "Set a string key in Rudis KV store with optional TTL in milliseconds",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" },
                    "value": { "type": "string" },
                    "px": { "type": "integer", "description": "Optional expiration in milliseconds" }
                },
                "required": ["key", "value"]
            }),
        },
        McpToolDef {
            name: "rudis_semantic_set",
            description: "Cache an LLM prompt and response with its embedding vector",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "namespace": { "type": "string" },
                    "id": { "type": "string" },
                    "prompt": { "type": "string" },
                    "response": { "type": "string" },
                    "vector": { "type": "array", "items": { "type": "number" } },
                    "px": { "type": "integer" },
                    "scope": { "type": "string" }
                },
                "required": ["namespace", "id", "prompt", "response", "vector"]
            }),
        },
        McpToolDef {
            name: "rudis_semantic_get",
            description: "Lookup a semantically similar cached LLM response by embedding vector",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "namespace": { "type": "string" },
                    "vector": { "type": "array", "items": { "type": "number" } },
                    "threshold": { "type": "number", "default": 0.15 },
                    "k": { "type": "integer", "default": 1 },
                    "scope": { "type": "string" }
                },
                "required": ["namespace", "vector"]
            }),
        },
        McpToolDef {
            name: "rudis_vector_add",
            description: "Add an element and embedding vector with optional JSON attributes to a Vector Set",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" },
                    "element": { "type": "string" },
                    "vector": { "type": "array", "items": { "type": "number" } },
                    "attr": { "type": "string", "description": "Optional JSON attribute object string" }
                },
                "required": ["key", "element", "vector"]
            }),
        },
        McpToolDef {
            name: "rudis_vector_search",
            description: "Search a Vector Set by embedding vector with optional attribute filter expression",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" },
                    "vector": { "type": "array", "items": { "type": "number" } },
                    "count": { "type": "integer", "default": 5 },
                    "filter": { "type": "string" },
                    "with_scores": { "type": "boolean", "default": true },
                    "with_attribs": { "type": "boolean", "default": false }
                },
                "required": ["key", "vector"]
            }),
        },
        McpToolDef {
            name: "rudis_agent_memory_add",
            description: "Append a conversation turn to an AI agent's token-budgeted working and episodic memory",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "session": { "type": "string" },
                    "role": { "type": "string" },
                    "content": { "type": "string" },
                    "tokens": { "type": "integer" },
                    "vector": { "type": "array", "items": { "type": "number" } },
                    "meta": { "type": "string" }
                },
                "required": ["session", "role", "content"]
            }),
        },
        McpToolDef {
            name: "rudis_agent_memory_context",
            description: "Retrieve token-budgeted recent turns and HNSW episodic recall for an AI agent session",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "session": { "type": "string" },
                    "max_tokens": { "type": "integer", "default": 2048 },
                    "query_vector": { "type": "array", "items": { "type": "number" } },
                    "recall_k": { "type": "integer", "default": 3 }
                },
                "required": ["session"]
            }),
        },
        McpToolDef {
            name: "rudis_agent_checkpoint_put",
            description: "Save a DAG checkpoint node for an AI agent execution thread",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" },
                    "step_id": { "type": "string" },
                    "parent_id": { "type": "string" },
                    "state": { "type": "string" },
                    "meta": { "type": "string" }
                },
                "required": ["key", "step_id", "state"]
            }),
        },
        McpToolDef {
            name: "rudis_agent_checkpoint_get",
            description: "Get the latest or a specific DAG checkpoint node for an AI agent thread",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" },
                    "step_id": { "type": "string" }
                },
                "required": ["key"]
            }),
        },
        McpToolDef {
            name: "rudis_ft_search",
            description: "Execute a RediSearch full-text, tag, or numeric query on an index",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "index": { "type": "string" },
                    "query": { "type": "string" },
                    "limit": { "type": "integer", "default": 10 }
                },
                "required": ["index", "query"]
            }),
        },
    ]
}

pub fn tools_list_json() -> Value {
    let tools: Vec<Value> = builtin_mcp_tools()
        .into_iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema,
            })
        })
        .collect();
    json!({ "tools": tools })
}

fn parse_f32_vec(val: &Value, field_name: &str) -> Result<Vec<f32>, String> {
    let arr = val
        .as_array()
        .ok_or_else(|| format!("'{}' must be an array of numbers", field_name))?;
    if arr.is_empty() {
        return Err(format!("'{}' cannot be empty", field_name));
    }
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let f = item
            .as_f64()
            .ok_or_else(|| format!("'{}' elements must be numbers", field_name))?;
        out.push(f as f32);
    }
    Ok(out)
}

fn get_req_str(args: &Value, field: &str) -> Result<String, String> {
    args.get(field)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing required string argument '{}'", field))
}

/// Maps an MCP tool invocation `(tool_name, arguments_json)` into a native Rudis `Command`.
pub fn plan_tool_command(tool: &str, args: &Value) -> Result<Command, String> {
    match tool {
        "rudis_kv_get" => {
            let key = get_req_str(args, "key")?;
            Ok(Command::Get(Bytes::from(key)))
        }
        "rudis_kv_set" => {
            let key = get_req_str(args, "key")?;
            let value = get_req_str(args, "value")?;
            let expire_in = args
                .get("px")
                .and_then(|v| v.as_u64())
                .map(|ms| Duration::from_millis(ms.max(1)));
            Ok(Command::Set {
                key: Bytes::from(key),
                value: Bytes::from(value),
                expire_in,
                condition: SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            })
        }
        "rudis_semantic_set" => {
            let namespace = get_req_str(args, "namespace")?;
            let id = get_req_str(args, "id")?;
            let prompt = get_req_str(args, "prompt")?;
            let response = get_req_str(args, "response")?;
            let vec_val = args
                .get("vector")
                .ok_or_else(|| "missing required argument 'vector'".to_string())?;
            let vector = parse_f32_vec(vec_val, "vector")?;
            let ttl = args
                .get("px")
                .and_then(|v| v.as_u64())
                .map(|ms| Duration::from_millis(ms.max(1)));
            let scope = args
                .get("scope")
                .and_then(|v| v.as_str())
                .map(|s| Bytes::from(s.to_string()));
            let tokens = args.get("tokens").and_then(|v| v.as_u64());
            Ok(Command::SemanticSet {
                namespace: Bytes::from(namespace),
                id: Bytes::from(id),
                prompt: Bytes::from(prompt),
                response: Bytes::from(response),
                vector,
                ttl,
                scope,
                quantize: false,
                tokens,
            })
        }
        "rudis_semantic_get" => {
            let namespace = get_req_str(args, "namespace")?;
            let vec_val = args
                .get("vector")
                .ok_or_else(|| "missing required argument 'vector'".to_string())?;
            let query = parse_f32_vec(vec_val, "vector")?;
            let threshold = args
                .get("threshold")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.15) as f32;
            let scope = args
                .get("scope")
                .and_then(|v| v.as_str())
                .map(|s| Bytes::from(s.to_string()));
            Ok(Command::SemanticGet {
                namespace: Bytes::from(namespace),
                query,
                threshold,
                scope,
                with_score: true,
                with_prompt: true,
                with_id: true,
            })
        }
        "rudis_vector_add" => {
            let key = get_req_str(args, "key")?;
            let element = get_req_str(args, "element")?;
            let vec_val = args
                .get("vector")
                .ok_or_else(|| "missing required argument 'vector'".to_string())?;
            let vector = parse_f32_vec(vec_val, "vector")?;
            let setattr = args
                .get("attr")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            Ok(Command::Vadd {
                key: Bytes::from(key),
                element: Bytes::from(element),
                vector,
                metric: None,
                quantize: false,
                pq: false,
                tiered: false,
                reduce: None,
                quant: None,
                ef: None,
                setattr,
                m: None,
                cas: false,
                is_redis_vset: true,
            })
        }
        "rudis_vector_search" => {
            let key = get_req_str(args, "key")?;
            let vec_val = args
                .get("vector")
                .ok_or_else(|| "missing required argument 'vector'".to_string())?;
            let vector = parse_f32_vec(vec_val, "vector")?;
            let count = args.get("count").and_then(|v| v.as_u64()).unwrap_or(5) as usize;
            let filter = args
                .get("filter")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let with_scores = args
                .get("with_scores")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let with_attribs = args
                .get("with_attribs")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok(Command::Vsim {
                key: Bytes::from(key),
                target: VsimTarget::Vector(vector),
                with_scores,
                with_attribs,
                count: count.max(1),
                epsilon: None,
                ef: None,
                filter,
                filter_ef: None,
                truth: false,
                no_thread: false,
            })
        }
        "rudis_agent_memory_add" => {
            let session = get_req_str(args, "session")?;
            let role = get_req_str(args, "role")?;
            let content = get_req_str(args, "content")?;
            let tokens = args.get("tokens").and_then(|v| v.as_u64());
            let vector = match args.get("vector") {
                Some(v) if !v.is_null() => Some(parse_f32_vec(v, "vector")?),
                _ => None,
            };
            let meta = args
                .get("meta")
                .and_then(|v| v.as_str())
                .map(|s| Bytes::from(s.to_string()));
            Ok(Command::AgentMemAdd {
                session: Bytes::from(session),
                role: Bytes::from(role),
                content: Bytes::from(content),
                tokens,
                vector,
                meta,
            })
        }
        "rudis_agent_memory_context" => {
            let session = get_req_str(args, "session")?;
            let max_tokens = args
                .get("max_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(2048);
            let query = match args.get("query_vector") {
                Some(v) if !v.is_null() => Some(parse_f32_vec(v, "query_vector")?),
                _ => None,
            };
            let recall_k = args.get("recall_k").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
            Ok(Command::AgentMemContext {
                session: Bytes::from(session),
                max_tokens,
                query,
                recall_k,
            })
        }
        "rudis_agent_checkpoint_put" => {
            let key = get_req_str(args, "key")?;
            let step_id = get_req_str(args, "step_id")?;
            let state = get_req_str(args, "state")?;
            let parent_id = args
                .get("parent_id")
                .and_then(|v| v.as_str())
                .map(|s| Bytes::from(s.to_string()));
            let meta = args
                .get("meta")
                .and_then(|v| v.as_str())
                .map(|s| Bytes::from(s.to_string()));
            Ok(Command::AgentCheckpointPut {
                key: Bytes::from(key),
                step_id: Bytes::from(step_id),
                parent_id,
                state: Bytes::from(state),
                meta,
            })
        }
        "rudis_agent_checkpoint_get" => {
            let key = get_req_str(args, "key")?;
            let step_id = args
                .get("step_id")
                .and_then(|v| v.as_str())
                .map(|s| Bytes::from(s.to_string()));
            Ok(Command::AgentCheckpointGet {
                key: Bytes::from(key),
                step_id,
            })
        }
        "rudis_ft_search" => {
            let index = get_req_str(args, "index")?;
            let query = get_req_str(args, "query")?;
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
            let options = SearchOptions {
                limit,
                ..SearchOptions::default()
            };
            Ok(Command::FtSearch {
                index,
                query,
                options,
            })
        }
        _ => Err(format!("Unknown MCP tool: {}", tool)),
    }
}

/// Parses a single RESP2/RESP3 frame from `buf` into a `serde_json::Value` and returns `(val, next_pos, is_error)`.
fn parse_resp_frame(buf: &[u8], pos: usize) -> Option<(Value, usize, bool)> {
    if pos >= buf.len() {
        return None;
    }
    let prefix = buf[pos];
    let line_end = buf[pos + 1..].windows(2).position(|w| w == b"\r\n")? + pos + 1;
    let header = std::str::from_utf8(&buf[pos + 1..line_end]).ok()?;
    let next = line_end + 2;

    match prefix {
        b'+' => Some((Value::String(header.to_string()), next, false)),
        b'-' => Some((Value::String(header.to_string()), next, true)),
        b':' => {
            let n: i64 = header.parse().unwrap_or(0);
            Some((json!(n), next, false))
        }
        b',' => {
            let f: f64 = header.parse().unwrap_or(0.0);
            Some((json!(f), next, false))
        }
        b'_' => Some((Value::Null, next, false)),
        b'$' => {
            let len: isize = header.parse().ok()?;
            if len < 0 {
                return Some((Value::Null, next, false));
            }
            let ulen = len as usize;
            if next + ulen + 2 > buf.len() {
                return None;
            }
            let data = &buf[next..next + ulen];
            let s = String::from_utf8_lossy(data).to_string();
            Some((Value::String(s), next + ulen + 2, false))
        }
        b'*' => {
            let count: isize = header.parse().ok()?;
            if count < 0 {
                return Some((Value::Null, next, false));
            }
            let ucount = count as usize;
            let mut items = Vec::with_capacity(ucount);
            let mut cur = next;
            let mut any_err = false;
            for _ in 0..ucount {
                let (v, npos, is_err) = parse_resp_frame(buf, cur)?;
                if is_err {
                    any_err = true;
                }
                items.push(v);
                cur = npos;
            }
            Some((Value::Array(items), cur, any_err))
        }
        b'%' => {
            let count: isize = header.parse().ok()?;
            if count < 0 {
                return Some((Value::Null, next, false));
            }
            let ucount = count as usize;
            let mut map = serde_json::Map::with_capacity(ucount);
            let mut cur = next;
            let mut any_err = false;
            for _ in 0..ucount {
                let (k, kpos, kerr) = parse_resp_frame(buf, cur)?;
                let (v, vpos, verr) = parse_resp_frame(buf, kpos)?;
                if kerr || verr {
                    any_err = true;
                }
                let k_str = match k {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                map.insert(k_str, v);
                cur = vpos;
            }
            Some((Value::Object(map), cur, any_err))
        }
        _ => None,
    }
}

pub fn resp_bytes_to_json(buf: &[u8]) -> (Value, bool) {
    match parse_resp_frame(buf, 0) {
        Some((val, _, is_err)) => (val, is_err),
        None => (
            Value::String(String::from_utf8_lossy(buf).to_string()),
            false,
        ),
    }
}

pub fn format_mcp_call_result(val: Value, is_error: bool) -> Value {
    let text = match &val {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": val,
        "isError": is_error
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_tools_list_and_command_planning() {
        let list = tools_list_json();
        let tools = list["tools"].as_array().unwrap();
        assert!(tools.len() >= 10);
        assert!(tools.iter().any(|t| t["name"] == "rudis_semantic_get"));
        assert!(
            tools
                .iter()
                .any(|t| t["name"] == "rudis_agent_memory_context")
        );

        // rudis_kv_get
        let cmd_get = plan_tool_command("rudis_kv_get", &json!({"key": "mykey"})).unwrap();
        assert!(matches!(cmd_get, Command::Get(k) if k == "mykey"));

        // rudis_kv_set
        let cmd_set = plan_tool_command(
            "rudis_kv_set",
            &json!({"key": "k1", "value": "v1", "px": 1000}),
        )
        .unwrap();
        assert!(matches!(cmd_set, Command::Set { .. }));

        // rudis_semantic_set
        let cmd_sem_set = plan_tool_command(
            "rudis_semantic_set",
            &json!({
                "namespace": "ns",
                "id": "doc1",
                "prompt": "what is rust?",
                "response": "a language",
                "vector": [0.1, 0.2, 0.3],
                "px": 5000,
                "scope": "tenantA"
            }),
        )
        .unwrap();
        assert!(matches!(cmd_sem_set, Command::SemanticSet { .. }));

        // rudis_semantic_get
        let cmd_sem_get = plan_tool_command(
            "rudis_semantic_get",
            &json!({
                "namespace": "ns",
                "vector": [0.1, 0.2, 0.3],
                "threshold": 0.2,
                "scope": "tenantA"
            }),
        )
        .unwrap();
        assert!(matches!(cmd_sem_get, Command::SemanticGet { .. }));

        // rudis_vector_add
        let cmd_vadd = plan_tool_command(
            "rudis_vector_add",
            &json!({
                "key": "vkey",
                "element": "elem1",
                "vector": [1.0, 0.0],
                "attr": "{\"category\":\"tech\"}"
            }),
        )
        .unwrap();
        assert!(matches!(cmd_vadd, Command::Vadd { .. }));

        // rudis_vector_search
        let cmd_vsearch = plan_tool_command(
            "rudis_vector_search",
            &json!({
                "key": "vkey",
                "vector": [1.0, 0.0],
                "count": 10,
                "filter": "@category == 'tech'"
            }),
        )
        .unwrap();
        assert!(matches!(cmd_vsearch, Command::Vsim { .. }));

        // rudis_agent_memory_add
        let cmd_mem_add = plan_tool_command(
            "rudis_agent_memory_add",
            &json!({
                "session": "sess1",
                "role": "user",
                "content": "hello world",
                "tokens": 2,
                "vector": [0.5, 0.5]
            }),
        )
        .unwrap();
        assert!(matches!(cmd_mem_add, Command::AgentMemAdd { .. }));

        // rudis_agent_memory_context
        let cmd_mem_ctx = plan_tool_command(
            "rudis_agent_memory_context",
            &json!({
                "session": "sess1",
                "max_tokens": 1000,
                "query_vector": [0.5, 0.5],
                "recall_k": 5
            }),
        )
        .unwrap();
        assert!(matches!(cmd_mem_ctx, Command::AgentMemContext { .. }));

        // rudis_agent_checkpoint_put & get
        let cmd_chk_put = plan_tool_command(
            "rudis_agent_checkpoint_put",
            &json!({
                "key": "chk_key",
                "step_id": "step_1",
                "parent_id": "step_0",
                "state": "active",
                "meta": "{}"
            }),
        )
        .unwrap();
        assert!(matches!(cmd_chk_put, Command::AgentCheckpointPut { .. }));

        let cmd_chk_get = plan_tool_command(
            "rudis_agent_checkpoint_get",
            &json!({
                "key": "chk_key",
                "step_id": "step_1"
            }),
        )
        .unwrap();
        assert!(matches!(cmd_chk_get, Command::AgentCheckpointGet { .. }));

        // rudis_ft_search
        let cmd_ft = plan_tool_command(
            "rudis_ft_search",
            &json!({
                "index": "idx",
                "query": "rust speed",
                "limit": 5
            }),
        )
        .unwrap();
        assert!(matches!(cmd_ft, Command::FtSearch { .. }));

        // Error cases
        assert!(plan_tool_command("nonexistent_tool", &json!({})).is_err());
        assert!(plan_tool_command("rudis_kv_get", &json!({})).is_err());
        assert!(plan_tool_command("rudis_semantic_set", &json!({"namespace": "ns", "id": "1", "prompt": "p", "response": "r", "vector": ["invalid"]})).is_err());
    }

    #[test]
    fn test_resp_bytes_to_json_full_spectrum() {
        let (val, is_err) = resp_bytes_to_json(b"+OK\r\n");
        assert!(!is_err);
        assert_eq!(val, json!("OK"));

        let (val, is_err) = resp_bytes_to_json(b"-ERR unknown command\r\n");
        assert!(is_err);
        assert_eq!(val, json!("ERR unknown command"));

        let (val, is_err) = resp_bytes_to_json(b":-123\r\n");
        assert!(!is_err);
        assert_eq!(val, json!(-123));

        let (val, is_err) = resp_bytes_to_json(b",12.5\r\n");
        assert!(!is_err);
        assert_eq!(val, json!(12.5));

        let (val, is_err) = resp_bytes_to_json(b"_\r\n");
        assert!(!is_err);
        assert_eq!(val, Value::Null);

        let (val, is_err) = resp_bytes_to_json(b"$-1\r\n");
        assert!(!is_err);
        assert_eq!(val, Value::Null);

        let (val, is_err) = resp_bytes_to_json(b"*2\r\n$5\r\nhello\r\n:42\r\n");
        assert!(!is_err);
        assert_eq!(val, json!(["hello", 42]));

        // RESP3 Map
        let (val, is_err) = resp_bytes_to_json(b"%1\r\n+status\r\n+healthy\r\n");
        assert!(!is_err);
        assert_eq!(val, json!({"status": "healthy"}));

        // Format mcp call result
        let res = format_mcp_call_result(json!({"done": true}), false);
        assert_eq!(res["isError"], false);
        assert_eq!(res["content"][0]["type"], "text");
    }
}
