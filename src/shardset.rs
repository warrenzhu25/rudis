//! A fixed-size set of shard ids, for tracking which shards a pipeline is
//! still waiting on.
//!
//! It replaces a single `u64` bitmask, which aliased (or, in debug builds,
//! panicked on) shard numbers of 64 and above. The set holds up to
//! [`MAX_SHARDS`] shards inline, so it never allocates on the hot path; the
//! server refuses a larger `threads` setting at startup.

/// The most shards a server may run. Also the reach of the pub/sub presence
/// bitmasks (`pubsub::PUBSUB_WORDS_PER_STRIPE` words of 64 bits).
pub const MAX_SHARDS: usize = 256;

const WORDS: usize = MAX_SHARDS / 64;

const _: () = assert!(WORDS == crate::pubsub::PUBSUB_WORDS_PER_STRIPE);

/// Set of shard ids below [`MAX_SHARDS`].
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ShardSet {
    words: [u64; WORDS],
}

impl ShardSet {
    pub const fn new() -> Self {
        Self { words: [0; WORDS] }
    }

    /// Adds `shard`. Panics if `shard >= MAX_SHARDS`.
    #[inline(always)]
    pub fn insert(&mut self, shard: usize) {
        self.words[shard / 64] |= 1u64 << (shard % 64);
    }

    /// Removes `shard` (a no-op if absent). Panics if `shard >= MAX_SHARDS`.
    #[inline(always)]
    pub fn remove(&mut self, shard: usize) {
        self.words[shard / 64] &= !(1u64 << (shard % 64));
    }

    #[inline(always)]
    pub fn contains(&self, shard: usize) -> bool {
        shard < MAX_SHARDS && self.words[shard / 64] & (1u64 << (shard % 64)) != 0
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|&w| w == 0)
    }

    /// The lowest shard in the set.
    #[inline(always)]
    pub fn first(&self) -> Option<usize> {
        self.words
            .iter()
            .enumerate()
            .find(|(_, w)| **w != 0)
            .map(|(i, w)| i * 64 + w.trailing_zeros() as usize)
    }

    /// The shards in ascending order (a snapshot: the set may be changed
    /// while iterating over the copy).
    #[inline(always)]
    pub fn iter(&self) -> impl Iterator<Item = usize> + use<> {
        let words = self.words;
        (0..WORDS).flat_map(move |i| {
            let mut w = words[i];
            std::iter::from_fn(move || {
                if w == 0 {
                    return None;
                }
                let bit = w.trailing_zeros() as usize;
                w &= w - 1;
                Some(i * 64 + bit)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_past_64_do_not_alias_low_shards() {
        let mut s = ShardSet::new();
        assert!(s.is_empty());
        assert_eq!(s.first(), None);
        s.insert(64);
        assert!(!s.contains(0));
        assert!(s.contains(64));
        assert_eq!(s.first(), Some(64));
        s.insert(0);
        s.remove(64);
        assert!(s.contains(0));
        assert!(!s.contains(64));
        s.remove(0);
        assert!(s.is_empty());
    }

    #[test]
    fn iterates_every_member_in_order_across_words() {
        let members = [0, 1, 63, 64, 65, 127, 128, 200, MAX_SHARDS - 1];
        let mut s = ShardSet::new();
        for &m in members.iter().rev() {
            s.insert(m);
        }
        assert_eq!(s.iter().collect::<Vec<_>>(), members);
        assert!(!s.contains(MAX_SHARDS));
        // Draining by `first` visits the same shards.
        let mut drained = Vec::new();
        while let Some(f) = s.first() {
            drained.push(f);
            s.remove(f);
        }
        assert_eq!(drained, members);
    }

    #[test]
    #[should_panic]
    fn inserting_past_the_cap_panics() {
        ShardSet::new().insert(MAX_SHARDS);
    }
}
