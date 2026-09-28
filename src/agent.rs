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
}
