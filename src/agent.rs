use bytes::Bytes;
use hashbrown::{HashMap, HashSet};

use crate::vector::{HnswIndex, VectorMetric};

/// A single conversation turn or compacted summary in an [`AgentMemorySession`].
#[derive(Debug, Clone, PartialEq)]
pub struct AgentTurn {
    pub id: u64,
    pub role: Bytes,
    pub content: Bytes,
    pub tokens: u64,
    pub meta: Option<Bytes>,
    pub compacted: bool,
}

/// A semantically recalled past episode outside the active recent context window.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentEpisodeHit {
    pub id: u64,
    pub role: Bytes,
    pub content: Bytes,
    pub score: f32,
    pub meta: Option<Bytes>,
}

/// Combined working-memory window + episodic recall returned by `AGENT.MEM.CONTEXT`.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentContextResult {
    pub recent_turns: Vec<AgentTurn>,
    pub recalled_episodes: Vec<AgentEpisodeHit>,
}

/// Token-budgeted working memory and HNSW-backed episodic memory for an AI agent session.
#[derive(Debug, Clone)]
pub struct AgentMemorySession {
    pub session_id: String,
    pub turns: Vec<AgentTurn>,
    pub id_to_pos: HashMap<u64, usize>,
    pub index: Option<HnswIndex>,
    pub next_turn_id: u64,
    pub active_tokens: u64,
    pub compactions: u64,
}

impl AgentMemorySession {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            turns: Vec::new(),
            id_to_pos: HashMap::new(),
            index: None,
            next_turn_id: 1,
            active_tokens: 0,
            compactions: 0,
        }
    }

    #[inline]
    fn estimate_tokens(content: &[u8]) -> u64 {
        (content.len().div_ceil(4)).max(1) as u64
    }

    /// Appends a turn to the session working memory and optionally indexes its embedding into
    /// the session's episodic [`HnswIndex`].
    pub fn add(
        &mut self,
        role: Bytes,
        content: Bytes,
        tokens: Option<u64>,
        vector: Option<Vec<f32>>,
        meta: Option<Bytes>,
    ) -> Result<u64, String> {
        let id = self.next_turn_id;
        if let Some(vec) = vector {
            if vec.is_empty() {
                return Err("ERR vector dimension must be greater than 0".to_string());
            }
            let idx = self.index.get_or_insert_with(|| {
                HnswIndex::new(self.session_id.clone(), vec.len(), VectorMetric::Cosine)
            });
            if vec.len() != idx.dim {
                return Err("ERR vector dimension mismatch".to_string());
            }
            idx.add(Bytes::from(id.to_string()), vec)
                .map_err(|e| format!("ERR {}", e))?;
        }
        self.next_turn_id += 1;
        let tok = tokens.unwrap_or_else(|| Self::estimate_tokens(&content));
        self.active_tokens = self.active_tokens.saturating_add(tok);
        let pos = self.turns.len();
        self.turns.push(AgentTurn {
            id,
            role,
            content,
            tokens: tok,
            meta,
            compacted: false,
        });
        self.id_to_pos.insert(id, pos);
        Ok(id)
    }

    /// Assembles a prompt context containing:
    /// 1. The newest active turns fitting within `max_tokens` (in chronological order).
    /// 2. Up to `recall_k` semantically closest past episodes not already in the recent window.
    pub fn context(
        &self,
        max_tokens: u64,
        query: Option<&[f32]>,
        recall_k: usize,
    ) -> Result<AgentContextResult, String> {
        if let (Some(q), Some(idx)) = (query, &self.index)
            && q.len() != idx.dim
        {
            return Err("ERR vector dimension mismatch".to_string());
        }

        let mut recent_turns = Vec::new();
        let mut recent_ids = HashSet::new();
        let mut used_tokens = 0u64;

        for turn in self.turns.iter().rev() {
            if turn.compacted {
                continue;
            }
            if used_tokens.saturating_add(turn.tokens) > max_tokens {
                break;
            }
            used_tokens += turn.tokens;
            recent_ids.insert(turn.id);
            recent_turns.push(turn.clone());
        }
        recent_turns.reverse();

        let mut recalled_episodes = Vec::new();
        if let (Some(q), Some(idx)) = (query, &self.index)
            && recall_k > 0
        {
            let filter_fn = |k: &Bytes| -> bool {
                std::str::from_utf8(k)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|id| !recent_ids.contains(&id))
            };
            let candidates = idx.search_filtered(q, recall_k, None, false, Some(&filter_fn));
            for (k, dist) in candidates {
                if let Some(id) = std::str::from_utf8(&k)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    && let Some(&pos) = self.id_to_pos.get(&id)
                    && let Some(turn) = self.turns.get(pos)
                {
                    let sim = (1.0 - dist).clamp(0.0, 1.0);
                    recalled_episodes.push(AgentEpisodeHit {
                        id: turn.id,
                        role: turn.role.clone(),
                        content: turn.content.clone(),
                        score: sim,
                        meta: turn.meta.clone(),
                    });
                }
            }
        }

        Ok(AgentContextResult {
            recent_turns,
            recalled_episodes,
        })
    }

    /// Compacts all active turns older than the most recent `keep_recent` active turns into a
    /// single `"system"` summary turn, while preserving all older episodic embeddings in HNSW.
    pub fn compact(
        &mut self,
        keep_recent: usize,
        summary: Bytes,
        tokens: Option<u64>,
        vector: Option<Vec<f32>>,
    ) -> Result<usize, String> {
        let active_positions: Vec<usize> = self
            .turns
            .iter()
            .enumerate()
            .filter_map(|(i, t)| if !t.compacted { Some(i) } else { None })
            .collect();

        if active_positions.len() <= keep_recent {
            return Ok(0);
        }

        let summary_id = self.next_turn_id;
        if let Some(vec) = vector {
            if vec.is_empty() {
                return Err("ERR vector dimension must be greater than 0".to_string());
            }
            let idx = self.index.get_or_insert_with(|| {
                HnswIndex::new(self.session_id.clone(), vec.len(), VectorMetric::Cosine)
            });
            if vec.len() != idx.dim {
                return Err("ERR vector dimension mismatch".to_string());
            }
            idx.add(Bytes::from(summary_id.to_string()), vec)
                .map_err(|e| format!("ERR {}", e))?;
        }
        self.next_turn_id += 1;

        let compact_count = active_positions.len() - keep_recent;
        for &pos in &active_positions[..compact_count] {
            self.active_tokens = self.active_tokens.saturating_sub(self.turns[pos].tokens);
            self.turns[pos].compacted = true;
        }

        let summary_tokens = tokens.unwrap_or_else(|| Self::estimate_tokens(&summary));
        self.active_tokens = self.active_tokens.saturating_add(summary_tokens);
        let insert_pos = if keep_recent == 0 {
            self.turns.len()
        } else {
            active_positions[compact_count]
        };
        self.turns.insert(
            insert_pos,
            AgentTurn {
                id: summary_id,
                role: Bytes::from_static(b"system"),
                content: summary,
                tokens: summary_tokens,
                meta: None,
                compacted: false,
            },
        );
        self.id_to_pos.clear();
        for (idx, t) in self.turns.iter().enumerate() {
            self.id_to_pos.insert(t.id, idx);
        }
        self.compactions += 1;
        Ok(compact_count)
    }

    pub fn info(&self) -> (usize, usize, u64, usize, usize, u64) {
        let active_turns = self.turns.iter().filter(|t| !t.compacted).count();
        let total_turns = self.turns.len();
        let dim = self.index.as_ref().map(|i| i.dim).unwrap_or(0);
        let vecs = self.index.as_ref().map(|i| i.len()).unwrap_or(0);
        (
            active_turns,
            total_turns,
            self.active_tokens,
            dim,
            vecs,
            self.compactions,
        )
    }
}

/// Decision returned by `LLM.QUOTA.RESERVE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmReserveResult {
    pub allowed: bool,
    pub reservation_id: Option<u64>,
    pub remaining_tokens: u64,
    pub retry_after_ms: u64,
}

/// Dual RPM (Requests Per Window) + TPM (Tokens Per Window) quota governor with
/// pre-inference token reservation and post-stream actual token settlement.
#[derive(Debug, Clone)]
pub struct LlmQuotaBucket {
    pub requests: std::collections::VecDeque<(std::time::Instant, u64)>,
    pub reservations: HashMap<u64, (std::time::Instant, u64)>,
    pub next_reservation_id: u64,
    pub window_ms: u64,
}

impl Default for LlmQuotaBucket {
    fn default() -> Self {
        Self::new(60_000)
    }
}

impl LlmQuotaBucket {
    pub fn new(window_ms: u64) -> Self {
        Self {
            requests: std::collections::VecDeque::new(),
            reservations: HashMap::new(),
            next_reservation_id: 1,
            window_ms: window_ms.max(1),
        }
    }

    fn evict_expired(&mut self, now: std::time::Instant) {
        let window = std::time::Duration::from_millis(self.window_ms);
        while let Some(&(ts, _)) = self.requests.front() {
            if now.duration_since(ts) >= window {
                self.requests.pop_front();
            } else {
                break;
            }
        }
        self.reservations
            .retain(|_, (ts, _)| now.duration_since(*ts) < window);
    }

    /// Atomically checks both `rpm` and `tpm` limits and reserves `est_tokens` if within quota.
    pub fn reserve(
        &mut self,
        rpm: usize,
        tpm: u64,
        est_tokens: u64,
        window_ms: Option<u64>,
    ) -> LlmReserveResult {
        if let Some(w) = window_ms {
            self.window_ms = w.max(1);
        }
        let now = std::time::Instant::now();
        self.evict_expired(now);

        let active_reqs = self.requests.len() + self.reservations.len();
        let used_tokens: u64 = self.requests.iter().map(|(_, t)| *t).sum();
        let reserved_tokens: u64 = self.reservations.values().map(|(_, t)| *t).sum();
        let committed = used_tokens.saturating_add(reserved_tokens);

        if active_reqs < rpm && committed.saturating_add(est_tokens) <= tpm {
            let res_id = self.next_reservation_id;
            self.next_reservation_id += 1;
            self.reservations.insert(res_id, (now, est_tokens));
            let remaining = tpm.saturating_sub(committed.saturating_add(est_tokens));
            LlmReserveResult {
                allowed: true,
                reservation_id: Some(res_id),
                remaining_tokens: remaining,
                retry_after_ms: 0,
            }
        } else {
            let oldest_req = self.requests.front().map(|(ts, _)| *ts);
            let oldest_res = self.reservations.values().map(|(ts, _)| *ts).min();
            let oldest = match (oldest_req, oldest_res) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            let retry_after_ms = oldest
                .map(|ts| {
                    let elapsed = now.duration_since(ts).as_millis() as u64;
                    self.window_ms.saturating_sub(elapsed).max(1)
                })
                .unwrap_or(self.window_ms);
            LlmReserveResult {
                allowed: false,
                reservation_id: None,
                remaining_tokens: tpm.saturating_sub(committed),
                retry_after_ms,
            }
        }
    }

    /// Settles a reservation with the actual token count consumed by the LLM call.
    /// Returns `(found, net_token_delta)` where negative delta indicates refunded tokens.
    pub fn settle(&mut self, reservation_id: u64, actual_tokens: u64) -> (bool, i64) {
        let now = std::time::Instant::now();
        self.evict_expired(now);
        if let Some((created_at, est_tokens)) = self.reservations.remove(&reservation_id) {
            self.requests.push_back((created_at, actual_tokens));
            let delta = (actual_tokens as i64).saturating_sub(est_tokens as i64);
            (true, delta)
        } else {
            (false, 0)
        }
    }

    pub fn info(&mut self) -> (usize, u64, u64, usize, u64) {
        let now = std::time::Instant::now();
        self.evict_expired(now);
        let active_reqs = self.requests.len() + self.reservations.len();
        let used_tokens: u64 = self.requests.iter().map(|(_, t)| *t).sum();
        let reserved_tokens: u64 = self.reservations.values().map(|(_, t)| *t).sum();
        (
            active_reqs,
            used_tokens,
            reserved_tokens,
            self.reservations.len(),
            self.window_ms,
        )
    }
}

/// A single node in an [`AgentCheckpointThread`] execution DAG.
#[derive(Debug, Clone)]
pub struct AgentCheckpointNode {
    pub step_id: Bytes,
    pub parent_id: Option<Bytes>,
    pub seq: u64,
    pub timestamp_ms: u64,
    pub state: Bytes,
    pub metadata: Option<Bytes>,
}

/// DAG-aware durable agent state checkpoint store for a thread (`AGENT.CHECKPOINT.*`).
#[derive(Debug, Clone)]
pub struct AgentCheckpointThread {
    pub nodes: hashbrown::HashMap<Bytes, AgentCheckpointNode>,
    pub order: Vec<Bytes>,
    pub head_step_id: Option<Bytes>,
    pub next_seq: u64,
}

impl Default for AgentCheckpointThread {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentCheckpointThread {
    pub fn new() -> Self {
        Self {
            nodes: hashbrown::HashMap::new(),
            order: Vec::new(),
            head_step_id: None,
            next_seq: 1,
        }
    }

    pub fn put(
        &mut self,
        step_id: Bytes,
        parent_id: Option<Bytes>,
        state: Bytes,
        metadata: Option<Bytes>,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let node = AgentCheckpointNode {
            step_id: step_id.clone(),
            parent_id,
            seq,
            timestamp_ms,
            state,
            metadata,
        };
        if !self.nodes.contains_key(&step_id) {
            self.order.push(step_id.clone());
        }
        self.nodes.insert(step_id.clone(), node);
        self.head_step_id = Some(step_id);
        seq
    }

    pub fn get(&self, step_id: Option<&Bytes>) -> Option<&AgentCheckpointNode> {
        let target = step_id.or(self.head_step_id.as_ref())?;
        self.nodes.get(target)
    }

    /// Walks the parent-pointer DAG lineage starting from `from_step` (or `head_step_id` when `None`),
    /// returning up to `limit` ancestor checkpoints in reverse-lineage order (leaf -> root).
    pub fn history(&self, from_step: Option<&Bytes>, limit: usize) -> Vec<AgentCheckpointNode> {
        let mut out = Vec::new();
        if limit == 0 {
            return out;
        }
        let mut cur = from_step.or(self.head_step_id.as_ref()).cloned();
        let mut visited = hashbrown::HashSet::new();
        while let Some(step) = cur {
            if !visited.insert(step.clone()) {
                break;
            }
            let Some(node) = self.nodes.get(&step) else {
                break;
            };
            out.push(node.clone());
            if out.len() >= limit {
                break;
            }
            cur = node.parent_id.clone();
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolClaimState {
    Claimed,
    InProgress,
    Completed,
}

#[derive(Debug, Clone)]
pub struct ToolClaimResult {
    pub state: ToolClaimState,
    pub output: Option<Bytes>,
    pub meta_int: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolCallEntry {
    pub(crate) input: Option<Bytes>,
    pub(crate) output: Option<Bytes>,
    pub(crate) attempt: u64,
    pub(crate) lease_until: Option<std::time::Instant>,
    pub(crate) expire_at: Option<std::time::Instant>,
}

/// Idempotent tool-call lease & result deduplication registry (`AGENT.TOOL.*`).
#[derive(Debug, Clone, Default)]
pub struct AgentToolRegistry {
    pub(crate) calls: hashbrown::HashMap<Bytes, ToolCallEntry>,
}

impl AgentToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn claim(&mut self, call_id: Bytes, ttl_ms: u64, input: Option<Bytes>) -> ToolClaimResult {
        let now = std::time::Instant::now();
        let lease_dur = std::time::Duration::from_millis(ttl_ms.max(1));

        if let Some(entry) = self.calls.get_mut(&call_id) {
            if entry.expire_at.is_some_and(|exp| now >= exp) {
                self.calls.remove(&call_id);
            } else if let Some(ref out) = entry.output {
                return ToolClaimResult {
                    state: ToolClaimState::Completed,
                    output: Some(out.clone()),
                    meta_int: 0,
                };
            } else if let Some(lease_until) = entry.lease_until
                && now < lease_until
            {
                let rem_ms = lease_until.duration_since(now).as_millis() as u64;
                return ToolClaimResult {
                    state: ToolClaimState::InProgress,
                    output: None,
                    meta_int: rem_ms.max(1),
                };
            } else {
                entry.attempt += 1;
                entry.lease_until = Some(now + lease_dur);
                if input.is_some() {
                    entry.input = input;
                }
                return ToolClaimResult {
                    state: ToolClaimState::Claimed,
                    output: None,
                    meta_int: entry.attempt,
                };
            }
        }

        self.calls.insert(
            call_id,
            ToolCallEntry {
                input,
                output: None,
                attempt: 1,
                lease_until: Some(now + lease_dur),
                expire_at: None,
            },
        );
        ToolClaimResult {
            state: ToolClaimState::Claimed,
            output: None,
            meta_int: 1,
        }
    }

    pub fn complete(&mut self, call_id: Bytes, output: Bytes, ttl_ms: Option<u64>) -> bool {
        let now = std::time::Instant::now();
        let expire_at = ttl_ms.map(|ms| now + std::time::Duration::from_millis(ms.max(1)));
        if let Some(entry) = self.calls.get_mut(&call_id) {
            entry.output = Some(output);
            entry.lease_until = None;
            entry.expire_at = expire_at;
            true
        } else {
            self.calls.insert(
                call_id,
                ToolCallEntry {
                    input: None,
                    output: Some(output),
                    attempt: 1,
                    lease_until: None,
                    expire_at,
                },
            );
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_memory_working_window_episodic_recall_and_compaction() {
        let mut session = AgentMemorySession::new("sess:1".to_string());

        // Add 4 turns with embeddings
        let id1 = session
            .add(
                Bytes::from("user"),
                Bytes::from("My favorite database is Rudis and my dog is named Rex."),
                Some(12),
                Some(vec![1.0, 0.0, 0.0]),
                Some(Bytes::from(r#"{"topic":"bio"}"#)),
            )
            .unwrap();
        assert_eq!(id1, 1);

        let id2 = session
            .add(
                Bytes::from("assistant"),
                Bytes::from("Got it! I will remember Rex and Rudis."),
                Some(10),
                Some(vec![0.9, 0.1, 0.0]),
                None,
            )
            .unwrap();
        assert_eq!(id2, 2);

        let id3 = session
            .add(
                Bytes::from("user"),
                Bytes::from("How do I configure io_uring thread pinning?"),
                Some(10),
                Some(vec![0.0, 1.0, 0.0]),
                None,
            )
            .unwrap();
        assert_eq!(id3, 3);

        let id4 = session
            .add(
                Bytes::from("assistant"),
                Bytes::from("Pass --threads N and keep --no-pin unset."),
                Some(10),
                Some(vec![0.0, 0.9, 0.1]),
                None,
            )
            .unwrap();
        assert_eq!(id4, 4);

        // Request context with max_tokens=20 (fits only turns 3 and 4) and query about dog Rex ([1, 0, 0])
        let ctx = session.context(20, Some(&[1.0, 0.0, 0.0]), 2).unwrap();
        assert_eq!(ctx.recent_turns.len(), 2);
        assert_eq!(ctx.recent_turns[0].id, 3);
        assert_eq!(ctx.recent_turns[1].id, 4);

        // Episodic recall should surface turns 1 and 2 (and NOT duplicate 3 or 4)
        assert_eq!(ctx.recalled_episodes.len(), 2);
        assert_eq!(ctx.recalled_episodes[0].id, 1);
        assert_eq!(ctx.recalled_episodes[1].id, 2);

        // Compact older turns keeping only 1 recent turn
        let compacted = session
            .compact(
                1,
                Bytes::from("Summary: user loves Rudis, has dog Rex, asked about io_uring."),
                Some(8),
                Some(vec![0.7, 0.7, 0.0]),
            )
            .unwrap();
        assert_eq!(compacted, 3);

        let (active_turns, total_turns, active_tokens, dim, vecs, compactions) = session.info();
        assert_eq!(active_turns, 2); // summary + turn 4
        assert_eq!(total_turns, 5);
        assert_eq!(active_tokens, 18); // 8 + 10
        assert_eq!(dim, 3);
        assert_eq!(vecs, 5);
        assert_eq!(compactions, 1);

        // Context after compaction has [summary (id 5), turn 4], and can still recall compacted turn 1!
        let ctx2 = session.context(50, Some(&[1.0, 0.0, 0.0]), 1).unwrap();
        assert_eq!(ctx2.recent_turns.len(), 2);
        assert_eq!(ctx2.recent_turns[0].id, 5);
        assert_eq!(ctx2.recent_turns[0].role, Bytes::from("system"));
        assert_eq!(ctx2.recent_turns[1].id, 4);
        assert_eq!(ctx2.recalled_episodes.len(), 1);
        assert_eq!(ctx2.recalled_episodes[0].id, 1);
    }

    #[test]
    fn test_llm_quota_reserve_and_settle() {
        let mut bucket = LlmQuotaBucket::new(60_000);

        // RPM=2, TPM=1000: reserve 700 tokens -> allowed (remaining=300)
        let r1 = bucket.reserve(2, 1000, 700, None);
        assert!(r1.allowed);
        assert_eq!(r1.reservation_id, Some(1));
        assert_eq!(r1.remaining_tokens, 300);

        // Second request tries to reserve 500 tokens -> exceeds TPM (700 + 500 > 1000), denied!
        let r2 = bucket.reserve(2, 1000, 500, None);
        assert!(!r2.allowed);
        assert_eq!(r2.remaining_tokens, 300);
        assert!(r2.retry_after_ms > 0);

        // Settle reservation 1 with actual_tokens=400 (refunds 300 tokens -> delta = -300)
        let (found, delta) = bucket.settle(1, 400);
        assert!(found);
        assert_eq!(delta, -300);

        // Now 600 tokens are available (1000 - 400), so reserving 500 succeeds!
        let r3 = bucket.reserve(2, 1000, 500, None);
        assert!(r3.allowed);
        assert_eq!(r3.reservation_id, Some(2));
        assert_eq!(r3.remaining_tokens, 100);

        // Third request with 50 tokens -> denied due to RPM=2 (1 settled + 1 reserved = 2 active requests)
        let r4 = bucket.reserve(2, 1000, 50, None);
        assert!(!r4.allowed);
    }

    #[test]
    fn test_agent_checkpoint_dag_and_tool_idempotency() {
        let mut thread = AgentCheckpointThread::new();
        assert_eq!(
            thread.put(
                Bytes::from("step_1"),
                None,
                Bytes::from(r#"{"node":"plan"}"#),
                Some(Bytes::from(r#"{"actor":"planner"}"#)),
            ),
            1
        );
        // Branch A: step_1 -> step_2a
        assert_eq!(
            thread.put(
                Bytes::from("step_2a"),
                Some(Bytes::from("step_1")),
                Bytes::from(r#"{"node":"tool_a"}"#),
                None,
            ),
            2
        );
        // Branch B (forked from step_1): step_1 -> step_2b -> step_3b
        assert_eq!(
            thread.put(
                Bytes::from("step_2b"),
                Some(Bytes::from("step_1")),
                Bytes::from(r#"{"node":"tool_b"}"#),
                None,
            ),
            3
        );
        assert_eq!(
            thread.put(
                Bytes::from("step_3b"),
                Some(Bytes::from("step_2b")),
                Bytes::from(r#"{"node":"final_b"}"#),
                None,
            ),
            4
        );

        // Head is step_3b; lineage from head is [step_3b, step_2b, step_1]
        let head = thread.get(None).unwrap();
        assert_eq!(head.step_id, Bytes::from("step_3b"));
        let hist_b = thread.history(None, 10);
        let ids_b: Vec<&[u8]> = hist_b.iter().map(|n| n.step_id.as_ref()).collect();
        assert_eq!(
            ids_b,
            vec![&b"step_3b"[..], &b"step_2b"[..], &b"step_1"[..]]
        );

        // Time-travel lineage from forked branch step_2a is [step_2a, step_1]
        let hist_a = thread.history(Some(&Bytes::from("step_2a")), 10);
        let ids_a: Vec<&[u8]> = hist_a.iter().map(|n| n.step_id.as_ref()).collect();
        assert_eq!(ids_a, vec![&b"step_2a"[..], &b"step_1"[..]]);

        // Tool idempotency lease lifecycle
        let mut tools = AgentToolRegistry::new();
        let c1 = tools.claim(
            Bytes::from("call_99"),
            30_000,
            Some(Bytes::from(r#"{"sql":"SELECT 1"}"#)),
        );
        assert_eq!(c1.state, ToolClaimState::Claimed);
        assert_eq!(c1.meta_int, 1);

        // Concurrent duplicate claim while lease is active -> IN_PROGRESS
        let c2 = tools.claim(Bytes::from("call_99"), 30_000, None);
        assert_eq!(c2.state, ToolClaimState::InProgress);
        assert!(c2.meta_int > 0);

        // Complete the tool call -> subsequent claims return cached COMPLETED output
        assert!(tools.complete(
            Bytes::from("call_99"),
            Bytes::from(r#"{"rows":[1]}"#),
            Some(60_000),
        ));
        let c3 = tools.claim(Bytes::from("call_99"), 30_000, None);
        assert_eq!(c3.state, ToolClaimState::Completed);
        assert_eq!(c3.output, Some(Bytes::from(r#"{"rows":[1]}"#)));
    }
}
