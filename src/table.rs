use bytes::Bytes;
use fxhash::{FxBuildHasher, hash64};
use hashbrown::HashMap;

pub type RudisHashMap = HashMap<Bytes, Bytes, FxBuildHasher>;
use crate::compact::{CompactKey, CompactStr};
use crate::resp::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const GROUP_SIZE: usize = 16;
pub const EMPTY: u8 = 0xFF;
pub const DELETED: u8 = 0xFE;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OrderedScore(pub f64);

impl Eq for OrderedScore {}

impl PartialOrd for OrderedScore {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedScore {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ZAddFlags {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
    pub incr: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LexBound {
    UnboundedMin,
    UnboundedMax,
    Inclusive(Bytes),
    Exclusive(Bytes),
}

impl LexBound {
    pub fn matches(&self, val: &[u8], is_min: bool) -> bool {
        match self {
            LexBound::UnboundedMin => is_min,
            LexBound::UnboundedMax => !is_min,
            LexBound::Inclusive(b) => {
                if is_min {
                    val >= b.as_ref()
                } else {
                    val <= b.as_ref()
                }
            }
            LexBound::Exclusive(b) => {
                if is_min {
                    val > b.as_ref()
                } else {
                    val < b.as_ref()
                }
            }
        }
    }
}

pub fn parse_lex_bound(s: &[u8]) -> Result<LexBound, &'static str> {
    if s.is_empty() {
        return Err("min or max not valid string range item");
    }
    if s == b"-" {
        Ok(LexBound::UnboundedMin)
    } else if s == b"+" {
        Ok(LexBound::UnboundedMax)
    } else if s[0] == b'[' {
        Ok(LexBound::Inclusive(Bytes::copy_from_slice(&s[1..])))
    } else if s[0] == b'(' {
        Ok(LexBound::Exclusive(Bytes::copy_from_slice(&s[1..])))
    } else {
        Err("min or max not valid string range item")
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ZRangeOpts {
    pub start: i64,
    pub stop: i64,
    pub min_score: f64,
    pub min_inc: bool,
    pub max_score: f64,
    pub max_inc: bool,
    pub by_score: bool,
    pub by_lex: bool,
    pub min_lex: LexBound,
    pub max_lex: LexBound,
    pub rev: bool,
    pub with_scores: bool,
    pub offset: usize,
    pub count: Option<usize>,
}

impl Default for ZRangeOpts {
    fn default() -> Self {
        Self {
            start: 0,
            stop: 0,
            min_score: 0.0,
            min_inc: true,
            max_score: 0.0,
            max_inc: true,
            by_score: false,
            by_lex: false,
            min_lex: LexBound::UnboundedMin,
            max_lex: LexBound::UnboundedMax,
            rev: false,
            with_scores: false,
            offset: 0,
            count: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Aggregate {
    #[default]
    Sum,
    Min,
    Max,
    Count,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListDirection {
    Left,
    Right,
}

const SMALL_ZSET_LIMIT: usize = 64;

#[derive(Clone, Debug)]
pub enum RudisZSet {
    Small(Vec<(OrderedScore, Bytes)>),
    Full {
        dict: hashbrown::HashMap<Bytes, f64>,
        tree: std::collections::BTreeSet<(OrderedScore, Bytes)>,
    },
}

impl Default for RudisZSet {
    fn default() -> Self {
        Self::new()
    }
}

impl RudisZSet {
    pub fn new() -> Self {
        RudisZSet::Small(Vec::new())
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            RudisZSet::Small(v) => v.len(),
            RudisZSet::Full { dict, .. } => dict.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        match self {
            RudisZSet::Small(v) => {
                let m_len = member.len();
                if v.len() == 1 {
                    if v[0].1.len() == m_len && v[0].1.as_ref() == member {
                        return Some(v[0].0.0);
                    }
                    return None;
                }
                v.iter()
                    .find(|(_, m)| m.len() == m_len && m.as_ref() == member)
                    .map(|(s, _)| s.0)
            }
            RudisZSet::Full { dict, .. } => dict.get(member).copied(),
        }
    }

    pub fn insert(&mut self, score: f64, member: Bytes) {
        match self {
            RudisZSet::Small(v) => {
                if v.is_empty() {
                    v.push((OrderedScore(score), member));
                    return;
                }
                if v.len() == 1 && v[0].1 == member {
                    v[0].0 = OrderedScore(score);
                    return;
                }
                if let Some(pos) = v.iter().position(|(_, m)| m == &member) {
                    v.remove(pos);
                }
                let ord = (OrderedScore(score), member);
                let insert_idx = match v.binary_search(&ord) {
                    Ok(idx) | Err(idx) => idx,
                };
                v.insert(insert_idx, ord);

                if v.len() > SMALL_ZSET_LIMIT {
                    let mut dict = hashbrown::HashMap::with_capacity(v.len());
                    let mut tree = std::collections::BTreeSet::new();
                    for (s, m) in v.drain(..) {
                        dict.insert(m.clone(), s.0);
                        tree.insert((s, m));
                    }
                    *self = RudisZSet::Full { dict, tree };
                }
            }
            RudisZSet::Full { dict, tree } => {
                if let Some(old_score) = dict.insert(member.clone(), score) {
                    tree.remove(&(OrderedScore(old_score), member.clone()));
                }
                tree.insert((OrderedScore(score), member));
            }
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        match self {
            RudisZSet::Small(v) => v
                .iter()
                .position(|(_, m)| m.as_ref() == member)
                .map(|pos| v.remove(pos).0.0),
            RudisZSet::Full { dict, tree } => {
                if let Some(old_score) = dict.remove(member) {
                    tree.remove(&(OrderedScore(old_score), Bytes::copy_from_slice(member)));
                    Some(old_score)
                } else {
                    None
                }
            }
        }
    }

    pub fn rank(&self, member: &[u8], rev: bool) -> Option<usize> {
        match self {
            RudisZSet::Small(v) => {
                if rev {
                    v.iter().rev().position(|(_, m)| m.as_ref() == member)
                } else {
                    v.iter().position(|(_, m)| m.as_ref() == member)
                }
            }
            RudisZSet::Full { dict, tree } => {
                if !dict.contains_key(member) {
                    return None;
                }
                if rev {
                    tree.iter().rev().position(|(_, m)| m.as_ref() == member)
                } else {
                    tree.iter().position(|(_, m)| m.as_ref() == member)
                }
            }
        }
    }

    pub fn count(&self, min: f64, min_inc: bool, max: f64, max_inc: bool) -> usize {
        match self {
            RudisZSet::Small(v) => v
                .iter()
                .filter(|(OrderedScore(s), _)| {
                    let ge_min = if min_inc { *s >= min } else { *s > min };
                    let le_max = if max_inc { *s <= max } else { *s < max };
                    ge_min && le_max
                })
                .count(),
            RudisZSet::Full { tree, .. } => tree
                .iter()
                .filter(|(OrderedScore(s), _)| {
                    let ge_min = if min_inc { *s >= min } else { *s > min };
                    let le_max = if max_inc { *s <= max } else { *s < max };
                    ge_min && le_max
                })
                .count(),
        }
    }

    pub fn range(&self, opts: &ZRangeOpts) -> Vec<(Bytes, f64)> {
        let n = self.len();
        if n == 0 {
            return Vec::new();
        }

        if opts.by_lex {
            if opts.offset == usize::MAX {
                return Vec::new();
            }
            let filter_fn = |m: &Bytes| {
                opts.min_lex.matches(m.as_ref(), true) && opts.max_lex.matches(m.as_ref(), false)
            };
            if opts.rev {
                let items: Vec<(Bytes, f64)> = match self {
                    RudisZSet::Small(v) => v
                        .iter()
                        .filter(|(_, m)| filter_fn(m))
                        .map(|(s, m)| (m.clone(), s.0))
                        .collect(),
                    RudisZSet::Full { tree, .. } => tree
                        .iter()
                        .filter(|(_, m)| filter_fn(m))
                        .map(|(s, m)| (m.clone(), s.0))
                        .collect(),
                };
                let skipped = items.into_iter().rev().skip(opts.offset);
                if let Some(c) = opts.count {
                    skipped.take(c).collect()
                } else {
                    skipped.collect()
                }
            } else {
                let items: Box<dyn Iterator<Item = (Bytes, f64)>> = match self {
                    RudisZSet::Small(v) => Box::new(
                        v.iter()
                            .filter(|(_, m)| filter_fn(m))
                            .map(|(s, m)| (m.clone(), s.0)),
                    ),
                    RudisZSet::Full { tree, .. } => Box::new(
                        tree.iter()
                            .filter(|(_, m)| filter_fn(m))
                            .map(|(s, m)| (m.clone(), s.0)),
                    ),
                };
                let skipped = items.skip(opts.offset);
                if let Some(c) = opts.count {
                    skipped.take(c).collect()
                } else {
                    skipped.collect()
                }
            }
        } else if opts.by_score {
            if opts.offset == usize::MAX {
                return Vec::new();
            }
            let min = opts.min_score;
            let min_inc = opts.min_inc;
            let max = opts.max_score;
            let max_inc = opts.max_inc;
            let filter_fn = move |item: &&(OrderedScore, Bytes)| {
                let s = item.0.0;
                let ge_min = if min_inc { s >= min } else { s > min };
                let le_max = if max_inc { s <= max } else { s < max };
                ge_min && le_max
            };

            let get_items: Box<dyn Iterator<Item = &(OrderedScore, Bytes)>> = match self {
                RudisZSet::Small(v) => Box::new(v.iter().filter(filter_fn)),
                RudisZSet::Full { tree, .. } => Box::new(
                    tree.range((
                        std::ops::Bound::Included((OrderedScore(min), Bytes::new())),
                        std::ops::Bound::Unbounded,
                    ))
                    .take_while(move |item| {
                        if max_inc {
                            item.0.0 <= max
                        } else {
                            item.0.0 < max
                        }
                    })
                    .filter(filter_fn),
                ),
            };

            if opts.rev {
                let rev_items: Vec<_> = get_items.collect();
                let skipped = rev_items.into_iter().rev().skip(opts.offset);
                if let Some(c) = opts.count {
                    skipped
                        .take(c)
                        .map(|(OrderedScore(s), m)| (m.clone(), *s))
                        .collect()
                } else {
                    skipped
                        .map(|(OrderedScore(s), m)| (m.clone(), *s))
                        .collect()
                }
            } else {
                let skipped = get_items.skip(opts.offset);
                if let Some(c) = opts.count {
                    skipped
                        .take(c)
                        .map(|(OrderedScore(s), m)| (m.clone(), *s))
                        .collect()
                } else {
                    skipped
                        .map(|(OrderedScore(s), m)| (m.clone(), *s))
                        .collect()
                }
            }
        } else {
            let mut start = opts.start;
            let mut stop = opts.stop;
            let n_i = n as i64;
            if start < 0 {
                start = (n_i + start).max(0);
            }
            if stop < 0 {
                stop += n_i;
            }
            if start > stop || start >= n_i {
                return Vec::new();
            }
            let start_u = start.max(0) as usize;
            let stop_u = (stop.min(n_i - 1) as usize).max(start_u);
            let limit = stop_u - start_u + 1;

            match self {
                RudisZSet::Small(v) => {
                    if opts.rev {
                        v.iter()
                            .rev()
                            .skip(start_u)
                            .take(limit)
                            .map(|(OrderedScore(s), m)| (m.clone(), *s))
                            .collect()
                    } else {
                        v.iter()
                            .skip(start_u)
                            .take(limit)
                            .map(|(OrderedScore(s), m)| (m.clone(), *s))
                            .collect()
                    }
                }
                RudisZSet::Full { tree, .. } => {
                    if opts.rev {
                        tree.iter()
                            .rev()
                            .skip(start_u)
                            .take(limit)
                            .map(|(OrderedScore(s), m)| (m.clone(), *s))
                            .collect()
                    } else {
                        tree.iter()
                            .skip(start_u)
                            .take(limit)
                            .map(|(OrderedScore(s), m)| (m.clone(), *s))
                            .collect()
                    }
                }
            }
        }
    }

    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let n = count.min(self.len());
        let mut popped = Vec::with_capacity(n);
        match self {
            RudisZSet::Small(v) => {
                for _ in 0..n {
                    let (OrderedScore(s), m) = v.remove(0);
                    popped.push((m, s));
                }
            }
            RudisZSet::Full { dict, tree } => {
                for _ in 0..n {
                    if let Some((OrderedScore(s), m)) = tree.pop_first() {
                        dict.remove(&m);
                        popped.push((m, s));
                    }
                }
            }
        }
        popped
    }

    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let n = count.min(self.len());
        let mut popped = Vec::with_capacity(n);
        match self {
            RudisZSet::Small(v) => {
                for _ in 0..n {
                    let (OrderedScore(s), m) = v.pop().unwrap();
                    popped.push((m, s));
                }
            }
            RudisZSet::Full { dict, tree } => {
                for _ in 0..n {
                    if let Some((OrderedScore(s), m)) = tree.pop_last() {
                        dict.remove(&m);
                        popped.push((m, s));
                    }
                }
            }
        }
        popped
    }

    pub fn for_each<F: FnMut(&Bytes, f64)>(&self, mut f: F) {
        match self {
            RudisZSet::Small(v) => {
                for (OrderedScore(s), m) in v {
                    f(m, *s);
                }
            }
            RudisZSet::Full { dict, .. } => {
                for (m, s) in dict {
                    f(m, *s);
                }
            }
        }
    }

    pub fn to_vec(&self) -> Vec<(Bytes, f64)> {
        match self {
            RudisZSet::Small(v) => v
                .iter()
                .map(|(OrderedScore(s), m)| (m.clone(), *s))
                .collect(),
            RudisZSet::Full { tree, .. } => tree
                .iter()
                .map(|(OrderedScore(s), m)| (m.clone(), *s))
                .collect(),
        }
    }

    pub fn rem_range_by_rank(&mut self, start: i64, stop: i64) -> usize {
        let n = self.len() as i64;
        if n == 0 {
            return 0;
        }
        let mut s = start;
        let mut e = stop;
        if s < 0 {
            s = (n + s).max(0);
        }
        if e < 0 {
            e += n;
        }
        if s > e || s >= n {
            return 0;
        }
        let start_u = s.max(0) as usize;
        let stop_u = (e.min(n - 1) as usize).max(start_u);
        let to_remove: Vec<Bytes> = match self {
            RudisZSet::Small(v) => v
                .iter()
                .skip(start_u)
                .take(stop_u - start_u + 1)
                .map(|(_, m)| m.clone())
                .collect(),
            RudisZSet::Full { tree, .. } => tree
                .iter()
                .skip(start_u)
                .take(stop_u - start_u + 1)
                .map(|(_, m)| m.clone())
                .collect(),
        };
        let count = to_remove.len();
        for m in &to_remove {
            self.remove(m);
        }
        count
    }

    pub fn rem_range_by_score(
        &mut self,
        min: f64,
        min_inc: bool,
        max: f64,
        max_inc: bool,
    ) -> usize {
        let to_remove: Vec<Bytes> = match self {
            RudisZSet::Small(v) => v
                .iter()
                .filter(|(OrderedScore(s), _)| {
                    let ge = if min_inc { *s >= min } else { *s > min };
                    let le = if max_inc { *s <= max } else { *s < max };
                    ge && le
                })
                .map(|(_, m)| m.clone())
                .collect(),
            RudisZSet::Full { tree, .. } => tree
                .iter()
                .filter(|(OrderedScore(s), _)| {
                    let ge = if min_inc { *s >= min } else { *s > min };
                    let le = if max_inc { *s <= max } else { *s < max };
                    ge && le
                })
                .map(|(_, m)| m.clone())
                .collect(),
        };
        let count = to_remove.len();
        for m in &to_remove {
            self.remove(m);
        }
        count
    }

    pub fn rem_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize {
        let to_remove: Vec<Bytes> = match self {
            RudisZSet::Small(v) => v
                .iter()
                .filter(|(_, m)| min.matches(m.as_ref(), true) && max.matches(m.as_ref(), false))
                .map(|(_, m)| m.clone())
                .collect(),
            RudisZSet::Full { tree, .. } => tree
                .iter()
                .filter(|(_, m)| min.matches(m.as_ref(), true) && max.matches(m.as_ref(), false))
                .map(|(_, m)| m.clone())
                .collect(),
        };
        let count = to_remove.len();
        for m in &to_remove {
            self.remove(m);
        }
        count
    }

    pub fn lex_count(&self, min: &LexBound, max: &LexBound) -> usize {
        match self {
            RudisZSet::Small(v) => v
                .iter()
                .filter(|(_, m)| min.matches(m.as_ref(), true) && max.matches(m.as_ref(), false))
                .count(),
            RudisZSet::Full { tree, .. } => tree
                .iter()
                .filter(|(_, m)| min.matches(m.as_ref(), true) && max.matches(m.as_ref(), false))
                .count(),
        }
    }
}

impl PartialEq for RudisZSet {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        match (self, other) {
            (RudisZSet::Small(a), RudisZSet::Small(b)) => a == b,
            _ => {
                let mut a_items: Vec<(OrderedScore, Bytes)> = Vec::with_capacity(self.len());
                self.for_each(|m, s| a_items.push((OrderedScore(s), m.clone())));
                a_items.sort();
                let mut b_items: Vec<(OrderedScore, Bytes)> = Vec::with_capacity(other.len());
                other.for_each(|m, s| b_items.push((OrderedScore(s), m.clone())));
                b_items.sort();
                a_items == b_items
            }
        }
    }
}

impl Eq for RudisZSet {}

const SMALL_SET_LIMIT: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SmallSetEntry {
    pub hash: u64,
    pub member: Bytes,
}

#[derive(Clone, Debug)]
pub enum RudisSet {
    Small(Vec<SmallSetEntry>),
    Full(hashbrown::HashSet<Bytes, FxBuildHasher>),
}

impl Default for RudisSet {
    fn default() -> Self {
        Self::new()
    }
}

pub enum RudisSetIter<'a> {
    Small(std::slice::Iter<'a, SmallSetEntry>),
    Full(hashbrown::hash_set::Iter<'a, Bytes>),
}

impl<'a> Iterator for RudisSetIter<'a> {
    type Item = &'a Bytes;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RudisSetIter::Small(it) => it.next().map(|e| &e.member),
            RudisSetIter::Full(it) => it.next(),
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            RudisSetIter::Small(it) => it.size_hint(),
            RudisSetIter::Full(it) => it.size_hint(),
        }
    }
}

impl<'a> ExactSizeIterator for RudisSetIter<'a> {}

impl<'a> IntoIterator for &'a RudisSet {
    type Item = &'a Bytes;
    type IntoIter = RudisSetIter<'a>;
    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl RudisSet {
    #[inline(always)]
    pub fn new() -> Self {
        RudisSet::Small(Vec::new())
    }

    #[inline(always)]
    pub fn with_capacity(cap: usize) -> Self {
        if cap <= SMALL_SET_LIMIT {
            RudisSet::Small(Vec::with_capacity(cap))
        } else {
            RudisSet::Full(hashbrown::HashSet::with_capacity_and_hasher(
                cap,
                FxBuildHasher::default(),
            ))
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        match self {
            RudisSet::Small(v) => v.len(),
            RudisSet::Full(s) => s.len(),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        match self {
            RudisSet::Small(v) => v.is_empty(),
            RudisSet::Full(s) => s.is_empty(),
        }
    }

    #[inline(always)]
    pub fn contains(&self, member: &[u8]) -> bool {
        let m_hash = hash64(member);
        self.contains_with_hash(member, m_hash)
    }

    #[inline(always)]
    pub fn contains_with_hash(&self, member: &[u8], m_hash: u64) -> bool {
        match self {
            RudisSet::Small(v) => {
                let m_len = member.len();
                match v.len() {
                    0 => false,
                    1 => {
                        v[0].hash == m_hash
                            && v[0].member.len() == m_len
                            && v[0].member.as_ref() == member
                    }
                    _ => {
                        for m in v {
                            if m.hash == m_hash
                                && m.member.len() == m_len
                                && m.member.as_ref() == member
                            {
                                return true;
                            }
                        }
                        false
                    }
                }
            }
            RudisSet::Full(s) => s.contains(member),
        }
    }

    pub fn insert(&mut self, member: Bytes) -> bool {
        match self {
            RudisSet::Small(v) => {
                let m_bytes = member.as_ref();
                let m_hash = hash64(m_bytes);
                let m_len = m_bytes.len();
                for m in v.iter() {
                    if m.hash == m_hash && m.member.len() == m_len && m.member.as_ref() == m_bytes {
                        return false;
                    }
                }
                v.push(SmallSetEntry {
                    hash: m_hash,
                    member,
                });
                if v.len() > SMALL_SET_LIMIT {
                    let mut set = hashbrown::HashSet::with_capacity_and_hasher(
                        v.len(),
                        FxBuildHasher::default(),
                    );
                    for m in v.drain(..) {
                        set.insert(m.member);
                    }
                    *self = RudisSet::Full(set);
                }
                true
            }
            RudisSet::Full(s) => s.insert(member),
        }
    }

    pub fn insert_slice(&mut self, member: &Bytes) -> bool {
        match self {
            RudisSet::Small(v) => {
                let m_bytes = member.as_ref();
                let m_hash = hash64(m_bytes);
                let m_len = m_bytes.len();
                for m in v.iter() {
                    if m.hash == m_hash && m.member.len() == m_len && m.member.as_ref() == m_bytes {
                        return false;
                    }
                }
                v.push(SmallSetEntry {
                    hash: m_hash,
                    member: member.clone(),
                });
                if v.len() > SMALL_SET_LIMIT {
                    let mut set = hashbrown::HashSet::with_capacity_and_hasher(
                        v.len(),
                        FxBuildHasher::default(),
                    );
                    for m in v.drain(..) {
                        set.insert(m.member);
                    }
                    *self = RudisSet::Full(set);
                }
                true
            }
            RudisSet::Full(s) => {
                if s.contains(member) {
                    false
                } else {
                    s.insert(member.clone())
                }
            }
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            RudisSet::Small(v) => {
                let m_hash = hash64(member);
                let m_len = member.len();
                if let Some(pos) = v.iter().position(|m| {
                    m.hash == m_hash && m.member.len() == m_len && m.member.as_ref() == member
                }) {
                    v.swap_remove(pos);
                    true
                } else {
                    false
                }
            }
            RudisSet::Full(s) => s.remove(member),
        }
    }

    pub fn to_vec(&self) -> Vec<Bytes> {
        match self {
            RudisSet::Small(v) => v.iter().map(|e| e.member.clone()).collect(),
            RudisSet::Full(s) => s.iter().cloned().collect(),
        }
    }

    pub fn pop(&mut self) -> Option<Bytes> {
        match self {
            RudisSet::Small(v) => v.pop().map(|e| e.member),
            RudisSet::Full(s) => {
                if let Some(elem) = s.iter().next().cloned() {
                    s.remove(&elem);
                    Some(elem)
                } else {
                    None
                }
            }
        }
    }

    #[inline(always)]
    pub fn iter(&self) -> RudisSetIter<'_> {
        match self {
            RudisSet::Small(v) => RudisSetIter::Small(v.iter()),
            RudisSet::Full(s) => RudisSetIter::Full(s.iter()),
        }
    }
}

impl PartialEq for RudisSet {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        match (self, other) {
            (RudisSet::Small(a), RudisSet::Small(b)) => a
                .iter()
                .all(|m| b.iter().any(|x| x.hash == m.hash && x.member == m.member)),
            (RudisSet::Full(a), RudisSet::Full(b)) => a == b,
            _ => {
                for m in self.iter() {
                    if !other.contains(m.as_ref()) {
                        return false;
                    }
                }
                true
            }
        }
    }
}

impl Eq for RudisSet {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl StreamId {
    pub fn parse_exact(s: &str) -> Result<Self, &'static str> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() == 1 {
            let ms: u64 = parts[0]
                .parse()
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            return Ok(Self { ms, seq: 0 });
        }
        if parts.len() != 2 {
            return Err("Invalid stream ID specified as stream command argument");
        }
        let ms: u64 = parts[0]
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        let seq: u64 = parts[1]
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        Ok(Self { ms, seq })
    }

    pub fn parse(s: &str) -> Result<Self, &'static str> {
        if s == "0" || s == "0-0" {
            return Ok(Self::new(0, 0));
        }
        if let Some((ms_s, seq_s)) = s.split_once('-') {
            let ms: u64 = ms_s
                .parse()
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            let seq: u64 = seq_s
                .parse()
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            Ok(Self::new(ms, seq))
        } else {
            let ms: u64 = s
                .parse()
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            Ok(Self::new(ms, 0))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamAddId {
    Auto,
    AutoSeq(u64),
    Explicit(StreamId),
}

impl StreamAddId {
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        if s == "*" {
            return Ok(StreamAddId::Auto);
        }
        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms: u64 = ms_str
                .parse()
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            if seq_str == "*" {
                return Ok(StreamAddId::AutoSeq(ms));
            }
            let seq: u64 = seq_str
                .parse()
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            return Ok(StreamAddId::Explicit(StreamId::new(ms, seq)));
        } else if let Ok(ms) = s.parse::<u64>() {
            return Ok(StreamAddId::Explicit(StreamId::new(ms, 0)));
        }
        Err("Invalid stream ID specified as stream command argument")
    }
}

pub fn parse_range_bound(
    s: &str,
    is_start: bool,
) -> Result<std::ops::Bound<StreamId>, &'static str> {
    if s == "-" {
        return Ok(std::ops::Bound::Unbounded);
    }
    if s == "+" {
        return Ok(std::ops::Bound::Unbounded);
    }
    let (s_trim, exclusive) = if let Some(stripped) = s.strip_prefix('(') {
        (stripped, true)
    } else {
        (s, false)
    };
    let id = if let Some((ms_str, seq_str)) = s_trim.split_once('-') {
        let ms: u64 = ms_str
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        let seq: u64 = seq_str
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        StreamId::new(ms, seq)
    } else {
        let ms: u64 = s_trim
            .parse()
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        let seq = if is_start { 0 } else { u64::MAX };
        StreamId::new(ms, seq)
    };
    if exclusive {
        if is_start {
            if id.ms == u64::MAX && id.seq == u64::MAX {
                return Err("ERR invalid start ID for the interval");
            }
        } else {
            if id.ms == 0 && id.seq == 0 {
                return Err("ERR invalid end ID for the interval");
            }
        }
        Ok(std::ops::Bound::Excluded(id))
    } else {
        Ok(std::ops::Bound::Included(id))
    }
}

pub fn is_stream_range_empty(
    start: &std::ops::Bound<StreamId>,
    end: &std::ops::Bound<StreamId>,
) -> bool {
    match (start, end) {
        (std::ops::Bound::Included(s), std::ops::Bound::Included(e)) => s > e,
        (std::ops::Bound::Included(s), std::ops::Bound::Excluded(e)) => s >= e,
        (std::ops::Bound::Excluded(s), std::ops::Bound::Included(e)) => s >= e,
        (std::ops::Bound::Excluded(s), std::ops::Bound::Excluded(e)) => s >= e,
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamTrimStrategy {
    KeepRef = 0,
    DelRef = 1,
    Acked = 2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamIdmpOption {
    Manual { producer: Bytes, iid: Bytes },
    Auto { producer: Bytes },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamAddResult {
    Added(StreamId),
    Duplicate(StreamId),
    NoMkStream,
}

pub static STREAM_NODE_MAX_ENTRIES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(100);
pub static STREAM_IDMP_DURATION: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(100);
pub static STREAM_IDMP_MAXSIZE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(100);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdmpProducer {
    pub iids: HashMap<Bytes, (StreamId, u64)>,
    pub order: std::collections::VecDeque<Bytes>,
}

impl Default for IdmpProducer {
    fn default() -> Self {
        Self::new()
    }
}

impl IdmpProducer {
    pub fn new() -> Self {
        Self {
            iids: HashMap::new(),
            order: std::collections::VecDeque::new(),
        }
    }
}

pub fn compute_stream_auto_iid(fields: &[(Bytes, Bytes)]) -> Bytes {
    let mut sorted_fields = fields.to_vec();
    sorted_fields.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let mut buf = Vec::new();
    buf.extend_from_slice(b"AUTO:");
    for (k, v) in &sorted_fields {
        buf.extend_from_slice(&(k.len() as u32).to_be_bytes());
        buf.extend_from_slice(k);
        buf.extend_from_slice(&(v.len() as u32).to_be_bytes());
        buf.extend_from_slice(v);
    }
    Bytes::from(buf)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPelEntry {
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: usize,
    pub nack_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConsumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub active_time_ms: Option<u64>,
    pub pel: std::collections::BTreeMap<StreamId, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub entries_read: Option<u64>,
    pub consumers: HashMap<Bytes, StreamConsumer>,
    pub pel: std::collections::BTreeMap<StreamId, StreamPelEntry>,
    pub next_nack_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RudisStream {
    pub entries: std::collections::BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub entries_added: u64,
    pub max_deleted_entry_id: StreamId,
    pub groups: HashMap<Bytes, StreamGroup>,
    pub idmp_duration: Option<u64>,
    pub idmp_maxsize: Option<usize>,
    pub idmp_producers: HashMap<Bytes, IdmpProducer>,
    pub iids_added: u64,
    pub iids_duplicates: u64,
    pub nodes: std::collections::VecDeque<Vec<StreamId>>,
}

impl Default for RudisStream {
    fn default() -> Self {
        Self::new()
    }
}

impl RudisStream {
    pub fn new() -> Self {
        Self {
            entries: std::collections::BTreeMap::new(),
            last_id: StreamId::default(),
            entries_added: 0,
            max_deleted_entry_id: StreamId::default(),
            groups: HashMap::new(),
            idmp_duration: None,
            idmp_maxsize: None,
            idmp_producers: HashMap::new(),
            iids_added: 0,
            iids_duplicates: 0,
            nodes: std::collections::VecDeque::new(),
        }
    }

    pub fn rebuild_nodes(&mut self) {
        let chunk_size = STREAM_NODE_MAX_ENTRIES
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1);
        self.nodes.clear();
        let mut cur = Vec::new();
        for &id in self.entries.keys() {
            cur.push(id);
            if cur.len() >= chunk_size {
                self.nodes.push_back(std::mem::take(&mut cur));
            }
        }
        if !cur.is_empty() {
            self.nodes.push_back(cur);
        }
    }

    pub fn add_entry_to_nodes(&mut self, id: StreamId) {
        let chunk_size = STREAM_NODE_MAX_ENTRIES
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1);
        if let Some(back) = self.nodes.back_mut()
            && back.len() < chunk_size
        {
            back.push(id);
            return;
        }
        self.nodes.push_back(vec![id]);
    }

    pub fn remove_entry_from_nodes(&mut self, id: &StreamId) {
        let mut empty_idx = None;
        for (idx, node) in self.nodes.iter_mut().enumerate() {
            if let Some(pos) = node.iter().position(|x| x == id) {
                node.remove(pos);
                if node.is_empty() {
                    empty_idx = Some(idx);
                }
                break;
            }
        }
        if let Some(idx) = empty_idx {
            self.nodes.remove(idx);
        }
    }

    pub fn estimate_distance_from_fields(
        entries_added: u64,
        entries_len: usize,
        last_id: StreamId,
        max_deleted_entry_id: StreamId,
        first_id: Option<StreamId>,
        id: &StreamId,
    ) -> Option<u64> {
        if entries_added == 0 {
            return Some(0);
        }
        if entries_len == 0 && id <= &last_id {
            return Some(entries_added);
        }
        let cmp_last = id.cmp(&last_id);
        if cmp_last == std::cmp::Ordering::Equal {
            return Some(entries_added);
        } else if cmp_last == std::cmp::Ordering::Greater {
            return None;
        }
        if let Some(fid) = first_id
            && (max_deleted_entry_id == StreamId::default() || max_deleted_entry_id < fid)
        {
            if id < &fid {
                return Some(entries_added.saturating_sub(entries_len as u64));
            } else if id == &fid {
                return Some(entries_added.saturating_sub(entries_len as u64) + 1);
            }
        }
        if *id != StreamId::default() && id < &max_deleted_entry_id {
            return None;
        }
        None
    }

    pub fn estimate_distance_from_first_ever_entry(&self, id: &StreamId) -> Option<u64> {
        Self::estimate_distance_from_fields(
            self.entries_added,
            self.entries.len(),
            self.last_id,
            self.max_deleted_entry_id,
            self.entries.keys().next().copied(),
            id,
        )
    }

    pub fn compute_cg_lag(&self, grp: &StreamGroup) -> Option<u64> {
        if self.entries_added == 0 {
            return Some(0);
        }
        let first_id = self.entries.keys().next().copied();
        let has_tombstones_ahead = !self.entries.is_empty()
            && self.max_deleted_entry_id != StreamId::default()
            && grp.last_delivered_id <= self.max_deleted_entry_id;

        if let Some(fid) = first_id
            && grp.last_delivered_id >= fid
            && let Some(er) = grp.entries_read
            && !has_tombstones_ahead
        {
            return Some(self.entries_added.saturating_sub(er));
        }

        if let Some(er) = self.estimate_distance_from_first_ever_entry(&grp.last_delivered_id) {
            return Some(self.entries_added.saturating_sub(er));
        }
        None
    }

    pub fn get_idmp_duration(&self) -> u64 {
        self.idmp_duration
            .unwrap_or_else(|| STREAM_IDMP_DURATION.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub fn get_idmp_maxsize(&self) -> usize {
        self.idmp_maxsize
            .unwrap_or_else(|| STREAM_IDMP_MAXSIZE.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub fn purge_expired_idmp(&mut self, now_ms: u64) {
        let duration_ms = self.get_idmp_duration().saturating_mul(1000);
        let mut empty_producers = Vec::new();
        for (pid, prod) in &mut self.idmp_producers {
            while let Some(front_iid) = prod.order.front() {
                if let Some((_, added_at)) = prod.iids.get(front_iid) {
                    if now_ms.saturating_sub(*added_at) > duration_ms {
                        let expired_iid = prod.order.pop_front().unwrap();
                        prod.iids.remove(&expired_iid);
                    } else {
                        break;
                    }
                } else {
                    prod.order.pop_front();
                }
            }
            if prod.iids.is_empty() {
                empty_producers.push(pid.clone());
            }
        }
        for pid in empty_producers {
            self.idmp_producers.remove(&pid);
        }
    }

    pub fn find_unexpired_idmp(
        &mut self,
        producer: &Bytes,
        iid: &Bytes,
        now_ms: u64,
    ) -> Option<StreamId> {
        self.purge_expired_idmp(now_ms);
        if let Some(prod) = self.idmp_producers.get(producer)
            && let Some((sid, added_at)) = prod.iids.get(iid)
        {
            let duration_ms = self.get_idmp_duration().saturating_mul(1000);
            if now_ms.saturating_sub(*added_at) <= duration_ms {
                return Some(*sid);
            }
        }
        None
    }

    pub fn record_idmp(&mut self, producer: Bytes, iid: Bytes, stream_id: StreamId, now_ms: u64) {
        self.purge_expired_idmp(now_ms);
        let maxsize = self.get_idmp_maxsize();
        let prod = self.idmp_producers.entry(producer).or_default();
        if let Some(val) = prod.iids.get_mut(&iid) {
            *val = (stream_id, now_ms);
        } else {
            if maxsize > 0 {
                while prod.order.len() >= maxsize {
                    if let Some(old_iid) = prod.order.pop_front() {
                        prod.iids.remove(&old_iid);
                    } else {
                        break;
                    }
                }
                prod.order.push_back(iid.clone());
                prod.iids.insert(iid, (stream_id, now_ms));
                self.iids_added += 1;
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TieredPointer {
    pub file_id: u32,
    pub offset: u64,
    pub length: u32,
    pub value_type: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RudisValue {
    String(crate::compact::CompactStr),
    Int(i64),
    SmallHash(Box<Vec<(Bytes, Bytes)>>),
    Hash(Box<RudisHashMap>),
    List(Box<std::collections::VecDeque<Bytes>>),
    Set(Box<RudisSet>),
    ZSet(Box<RudisZSet>),
    HyperLogLog(Box<[u8; 16384]>),
    Stream(Box<RudisStream>),
    // Boxed so every variant fits beside `CompactStr`'s tag and the enum
    // stays 16 bytes; tiered/cooled values are cold, so the extra
    // allocation is off the hot path.
    Tiered(Box<TieredPointer>),
    Cooled(Box<CooledValue>),
}

/// A value kept in memory that also has a copy on disk at `ptr`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CooledValue {
    pub ptr: TieredPointer,
    pub val: RudisValue,
}

const _: () = assert!(std::mem::size_of::<RudisValue>() == 16);

impl RudisValue {
    /// A string value (possibly cooled) that should be promoted to a shared
    /// buffer before being read; see [`CompactStr::make_shared`].
    #[inline(always)]
    pub fn string_needs_share(&self) -> bool {
        match self {
            RudisValue::String(s) => s.needs_share(),
            RudisValue::Cooled(cv) => {
                matches!(&cv.val, RudisValue::String(s) if s.needs_share())
            }
            _ => false,
        }
    }

    #[inline]
    pub fn share_string(&mut self) {
        match self {
            RudisValue::String(s) => s.make_shared(),
            RudisValue::Cooled(cv) => {
                if let RudisValue::String(s) = &mut cv.val {
                    s.make_shared();
                }
            }
            _ => {}
        }
    }

    /// Bytes this value adds to `used_memory` (`data_bytes`), beyond the
    /// table slot that holds it (slots are counted by `struct_bytes`).
    /// Unlike [`Self::approx_bytes`] (logical size, used for `MEMORY USAGE`
    /// and spill thresholds), inline strings and ints cost no heap here.
    pub fn mem_bytes(&self) -> usize {
        match self {
            RudisValue::String(b) => b.heap_bytes(),
            RudisValue::Int(_) => 0,
            RudisValue::Cooled(cv) => 24 + cv.val.mem_bytes(),
            other => other.approx_bytes(),
        }
    }

    pub fn approx_bytes(&self) -> usize {
        match self {
            RudisValue::String(b) => b.len(),
            RudisValue::Int(_) => 8,
            RudisValue::SmallHash(pairs) => pairs.iter().map(|(k, v)| k.len() + v.len() + 16).sum(),
            RudisValue::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 32).sum(),
            RudisValue::List(l) => l.iter().map(|b| b.len() + 16).sum(),
            RudisValue::Set(s) => s.len() * 32,
            RudisValue::ZSet(z) => z.len() * 48,
            RudisValue::HyperLogLog(_) => 16384,
            RudisValue::Stream(s) => s.len() * 64,
            RudisValue::Tiered(_) => 24,
            RudisValue::Cooled(cv) => 24 + cv.val.approx_bytes(),
        }
    }

    pub fn encoding_str(&self) -> &'static str {
        match self {
            RudisValue::String(_) => "raw",
            RudisValue::Int(_) => "int",
            RudisValue::SmallHash(_) => "listpack",
            RudisValue::Hash(_) => "hashtable",
            RudisValue::List(_) => "quicklist",
            RudisValue::Set(_) => "hashtable",
            RudisValue::ZSet(_) => "skiplist",
            RudisValue::HyperLogLog(_) => "raw",
            RudisValue::Stream(_) => "stream",
            RudisValue::Tiered(ptr) => match ptr.value_type {
                0 => "raw",
                1 => "quicklist",
                2 => "hashtable",
                3 => "skiplist",
                4 => "hashtable",
                5 => "raw",
                6 => "stream",
                _ => "raw",
            },
            RudisValue::Cooled(cv) => cv.val.encoding_str(),
        }
    }
}

/// Smallest value (by `approx_bytes`) worth spilling to the tier.
pub const MIN_SPILL_VALUE_BYTES: usize = 64;

/// Per-entry bytes added to `data_bytes` on top of the key/value heap bytes.
///
/// The entry's table slot is already counted by `struct_bytes`, so this only
/// approximates allocator slack and bookkeeping. It must stay non-zero: the
/// eviction loop (`evict_local_until_under`) stops once `used_memory` drops
/// below target, and a fully inline key would otherwise free 0 accounted
/// bytes, making the loop evict everything.
pub const ENTRY_OVERHEAD: usize = 16;

/// Heap bytes used by a key or string value of `n` bytes: zero when it fits
/// inline in `CompactKey`/`CompactStr`.
#[inline(always)]
pub fn heap_len(n: usize) -> usize {
    if n <= crate::compact::INLINE_CAP {
        0
    } else {
        n
    }
}

/// Base for [`Expiry`]: a little before the first use, so deadlines slightly
/// in the past (e.g. restored already-expired keys) still round-trip.
///
/// Aligned to a whole wall-clock millisecond: keys stored with a
/// millisecond-precision deadline (see `CompactKey`) then sit on the same
/// grid as absolute `PXAT`/`PEXPIREAT` times, which round-trip exactly.
fn expiry_base() -> Instant {
    static BASE: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *BASE.get_or_init(|| {
        let now = Instant::now();
        let sub_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos() % 1_000_000);
        let back = Duration::from_secs(3600) + Duration::from_nanos(sub_ms as u64);
        now.checked_sub(back).unwrap_or(now)
    })
}

/// An `Option<Instant>` deadline packed into 8 bytes: nanoseconds since
/// [`expiry_base`] plus one, with 0 meaning "no expiry". `Option<Instant>`
/// takes 16 bytes, and this sits in every table slot. Deadlines before the
/// base clamp to it (they are expired either way); ordering is preserved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Expiry(Option<std::num::NonZeroU64>);

impl Expiry {
    pub const NONE: Expiry = Expiry(None);

    #[inline(always)]
    pub fn get(self) -> Option<Instant> {
        self.0
            .map(|n| expiry_base() + Duration::from_nanos(n.get() - 1))
    }

    #[inline(always)]
    pub fn is_some(self) -> bool {
        self.0.is_some()
    }

    #[inline(always)]
    pub fn is_none(self) -> bool {
        self.0.is_none()
    }

    /// The packed representation (0 = no expiry), as stored in
    /// [`crate::compact::CompactKey`].
    #[inline(always)]
    pub fn to_raw(self) -> u64 {
        self.0.map_or(0, |n| n.get())
    }

    #[inline(always)]
    pub fn from_raw(raw: u64) -> Self {
        Expiry(std::num::NonZeroU64::new(raw))
    }
}

impl From<Option<Instant>> for Expiry {
    #[inline(always)]
    fn from(at: Option<Instant>) -> Self {
        Expiry(at.map(|at| {
            let nanos = at
                .saturating_duration_since(expiry_base())
                .as_nanos()
                .min(u64::MAX as u128 - 1) as u64;
            // nanos + 1 <= u64::MAX, so never zero.
            std::num::NonZeroU64::MIN.saturating_add(nanos)
        }))
    }
}

impl From<Instant> for Expiry {
    #[inline(always)]
    fn from(at: Instant) -> Self {
        Expiry::from(Some(at))
    }
}

/// A table entry. The expiry deadline lives in the key (see
/// [`crate::compact::CompactKey`]), so keys without a TTL don't pay for it;
/// use [`RudisEntry::expire_at`] / [`RudisEntry::set_expire_at`].
#[derive(Clone, Debug)]
pub struct RudisEntry {
    pub key: crate::compact::CompactKey,
    pub val: RudisValue,
}

const _: () = assert!(std::mem::size_of::<RudisEntry>() == 40);

impl RudisEntry {
    #[inline(always)]
    pub fn new(key: crate::compact::CompactKey, val: RudisValue, expire_at: Expiry) -> Self {
        let mut key = key;
        key.set_ttl(expire_at.to_raw());
        RudisEntry { key, val }
    }

    /// The key's expiry deadline, if it has one.
    #[inline(always)]
    pub fn expire_at(&self) -> Option<Instant> {
        Expiry::from_raw(self.key.ttl()).get()
    }

    #[inline(always)]
    pub fn set_expire_at(&mut self, at: Option<Instant>) {
        self.key.set_ttl(Expiry::from(at).to_raw());
    }

    /// Clears the expiry and returns the old deadline.
    #[inline(always)]
    pub fn take_expire_at(&mut self) -> Option<Instant> {
        let old = self.expire_at();
        self.key.set_ttl(0);
        old
    }

    /// If this entry holds a [`RudisValue::Cooled`], warms it back up to its
    /// in-memory [`RudisValue`] in place and returns its stale on-disk pointer.
    #[inline(always)]
    pub fn uncool(&mut self) -> Option<TieredPointer> {
        if matches!(self.val, RudisValue::Cooled(_)) {
            let (p, v) = match std::mem::replace(&mut self.val, RudisValue::Int(0)) {
                RudisValue::Cooled(cv) => {
                    let CooledValue { ptr, val } = *cv;
                    (ptr, val)
                }
                _ => unreachable!(),
            };
            self.val = v;
            Some(p)
        } else {
            None
        }
    }
}

#[inline(always)]
pub fn hash_key(key: &[u8]) -> u64 {
    hash64(key)
}

#[inline(always)]
fn mix_hash(mut h: u64) -> u64 {
    h ^= h >> 32;
    h = h.wrapping_mul(0xd6e8feb86659fd93);
    h ^= h >> 32;
    h
}

#[inline(always)]
pub fn fingerprint(hash: u64) -> u8 {
    (hash >> 57) as u8 & 0x7F
}

/// Bitmasks of the bytes in a 16-byte control group equal to `tag` and to
/// `EMPTY`. Written as plain loops: LLVM lowers each to one `pcmpeqb` +
/// `pmovmskb` pair on x86-64 (SSE2 is baseline), the same code the old
/// hand-written intrinsics produced, without raw-pointer loads.
#[inline(always)]
fn probe_group_match_or_empty(group: &[u8; GROUP_SIZE], tag: u8) -> (u16, u16) {
    let mut match_mask = 0u16;
    let mut empty_mask = 0u16;
    for (i, &b) in group.iter().enumerate() {
        match_mask |= ((b == tag) as u16) << i;
        empty_mask |= ((b == EMPTY) as u16) << i;
    }
    (match_mask, empty_mask)
}

/// Like [`probe_group_match_or_empty`], plus the `DELETED` bytes.
#[inline(always)]
fn probe_group_match_del_empty(group: &[u8; GROUP_SIZE], tag: u8) -> (u16, u16, u16) {
    let mut match_mask = 0u16;
    let mut del_mask = 0u16;
    let mut empty_mask = 0u16;
    for (i, &b) in group.iter().enumerate() {
        match_mask |= ((b == tag) as u16) << i;
        del_mask |= ((b == DELETED) as u16) << i;
        empty_mask |= ((b == EMPTY) as u16) << i;
    }
    (match_mask, del_mask, empty_mask)
}

const SEG_SHIFT: usize = 10;
const SEG_CAP: usize = 1 << SEG_SHIFT; // 1024 main SIMD slots per segment (~48KB, L1/L2 cache resident)
const STASH_CAP: usize = 4; // 4 DashTable-style overflow stash slots per segment
const GLOBAL_IDX_SHIFT: usize = 11;
const GLOBAL_IDX_MASK: usize = (1 << GLOBAL_IDX_SHIFT) - 1;
/// Buddy segments merge once their combined live entries are at most this
/// (a quarter of a segment, well below the 7/8 split point, so a merged
/// segment doesn't split again right away).
const MERGE_MAX_ITEMS: usize = SEG_CAP / 4;
/// `local_depth` marking a hole left by a merge (no directory entry points
/// to it).
const HOLE_DEPTH: u8 = u8::MAX;

/// Per-slot access clock: seconds, truncated to 16 bits. Idle times are
/// `lru_now().wrapping_sub(last)`, exact up to ~18.2 h; a key idle longer
/// wraps and reads as more recently used (Redis's 24-bit clock has the same
/// property at ~194 days). 2 B instead of 4 B per slot.
#[inline(always)]
pub fn lru_now() -> u16 {
    coarse_now_secs() as u16
}

#[inline(always)]
pub fn coarse_now_secs() -> u32 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable local; clock_gettime only writes into it.
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts);
    }
    ts.tv_sec as u32
}

/// Starts loading `r`'s cache line early. The probe's slot-index lookup
/// depends only on the hash, so touching it alongside the control group
/// overlaps what would otherwise be a second serial cache miss. A plain
/// load kept alive by `black_box` (safe; no `_mm_prefetch`/`unsafe`).
#[inline(always)]
fn prefetch_read<T: Copy>(r: &T) {
    std::hint::black_box(*r);
}

/// `slot_idx` value for a slot with no entry. Larger than any arena index
/// (a segment holds at most `SEG_CAP + STASH_CAP` entries), so
/// `entries.get(NO_ENTRY)` is `None`.
const NO_ENTRY: u16 = u16::MAX;

/// Fixed-size SwissTable segment with a 4-slot DashTable-style overflow stash,
/// managed by `RudisFlatTable`'s extendible hashing directory.
///
/// Slots only hold a control byte and a 2-byte index; the entries live in
/// the `entries` arena. Segments split
/// at 7/8 full, and with uniform hashing they all split together, so the
/// table is often only ~45-60% full: keeping empty slots at 3 B instead of a
/// full entry is what makes that cheap. `last_access` runs parallel to
/// `entries`. A delete leaves a hole (marked in the `holes` bitmap) that the
/// next insert reuses, so no entry moves and no per-entry back-pointer to its
/// slot is needed (2 B/key saved; the bitmap is 1 bit per arena entry). The
/// arena is compacted once less than half of it is live.
pub struct RawSegment {
    pub ctrl: Vec<u8>,
    /// Slot -> index into `entries`, or `NO_ENTRY`.
    slot_idx: Vec<u16>,
    entries: Vec<RudisEntry>,
    /// Access clock per entry (parallel to `entries`).
    last_access: Vec<u16>,
    /// Bit `i` set: `entries[i]` is a hole (a dead placeholder);
    /// `entries.len() - items` bits are set. Sized with the segment, so it
    /// never reallocates and `heap_bytes` only changes with the arena.
    holes: Vec<u64>,
    pub capacity: usize,
    mask: usize,
    pub items: usize,
    pub growth_left: usize,
    pub local_depth: u8,
    pub stash_ctrl: [u8; STASH_CAP],
    pub stash_count: u8,
}

impl RawSegment {
    pub fn new(capacity: usize, local_depth: u8) -> Self {
        let cap = capacity.next_power_of_two().clamp(GROUP_SIZE, SEG_CAP);
        let ctrl = vec![EMPTY; cap + GROUP_SIZE];
        let slot_idx = vec![NO_ENTRY; cap + STASH_CAP];
        let stash_bonus = if cap == SEG_CAP { STASH_CAP } else { 0 };

        Self {
            ctrl,
            slot_idx,
            entries: Vec::new(),
            last_access: Vec::new(),
            holes: vec![0; (cap + STASH_CAP).div_ceil(64)],
            capacity: cap,
            mask: cap - 1,
            items: 0,
            growth_left: (cap * 7) / 8 + stash_bonus,
            local_depth,
            stash_ctrl: [EMPTY; STASH_CAP],
            stash_count: 0,
        }
    }

    /// The control group starting at `idx`. `ctrl` has `GROUP_SIZE` mirror
    /// bytes past `capacity`, so any `idx <= mask` has a full group.
    #[inline(always)]
    fn group(&self, idx: usize) -> &[u8; GROUP_SIZE] {
        self.ctrl[idx..idx + GROUP_SIZE]
            .try_into()
            .expect("control group in bounds")
    }

    #[inline(always)]
    fn set_ctrl(&mut self, idx: usize, byte: u8) {
        self.ctrl[idx] = byte;
        if idx < GROUP_SIZE {
            self.ctrl[self.capacity + idx] = byte;
        }
    }

    /// Number of slots (main table plus stash), i.e. the positional range.
    #[inline(always)]
    pub fn slot_count(&self) -> usize {
        self.slot_idx.len()
    }

    #[inline(always)]
    pub fn slot_entry(&self, slot: usize) -> Option<&RudisEntry> {
        let i = *self.slot_idx.get(slot)? as usize;
        self.entries.get(i)
    }

    #[inline(always)]
    pub fn slot_entry_mut(&mut self, slot: usize) -> Option<&mut RudisEntry> {
        let i = *self.slot_idx.get(slot)? as usize;
        self.entries.get_mut(i)
    }

    #[inline(always)]
    pub fn slot_access(&self, slot: usize) -> Option<u16> {
        let i = *self.slot_idx.get(slot)? as usize;
        self.last_access.get(i).copied()
    }

    #[inline(always)]
    pub fn slot_access_mut(&mut self, slot: usize) -> Option<&mut u16> {
        let i = *self.slot_idx.get(slot)? as usize;
        self.last_access.get_mut(i)
    }

    /// Stamps the slot's access clock and returns its entry.
    #[inline(always)]
    pub fn touch_entry_mut(&mut self, slot: usize) -> Option<&mut RudisEntry> {
        let i = *self.slot_idx.get(slot)? as usize;
        *self.last_access.get_mut(i)? = lru_now();
        self.entries.get_mut(i)
    }

    /// Whether arena index `i` holds a live entry (not a hole).
    #[inline(always)]
    fn is_live(holes: &[u64], i: usize) -> bool {
        holes.get(i / 64).is_none_or(|w| w & (1 << (i % 64)) == 0)
    }

    /// Live entries (arena order, not slot order).
    #[inline]
    pub fn iter_entries(&self) -> impl Iterator<Item = &RudisEntry> {
        let holes = &self.holes;
        self.entries
            .iter()
            .enumerate()
            .filter(move |(i, _)| Self::is_live(holes, *i))
            .map(|(_, e)| e)
    }

    #[inline]
    pub fn iter_entries_mut(&mut self) -> impl Iterator<Item = &mut RudisEntry> {
        let holes = &self.holes;
        self.entries
            .iter_mut()
            .enumerate()
            .filter(move |(i, _)| Self::is_live(holes, *i))
            .map(|(_, e)| e)
    }

    /// Takes every entry with its access stamp, leaving the segment empty
    /// but with stale control bytes: callers replace the segment afterwards.
    #[inline]
    fn drain_entries(&mut self) -> impl Iterator<Item = (RudisEntry, u16)> + use<> {
        let entries = std::mem::take(&mut self.entries);
        let access = std::mem::take(&mut self.last_access);
        let holes = std::mem::take(&mut self.holes);
        self.items = 0;
        entries
            .into_iter()
            .zip(access)
            .enumerate()
            .filter(move |(i, _)| Self::is_live(&holes, *i))
            .map(|(_, ea)| ea)
    }

    /// Makes room for one more entry. Grows the arena by a sixteenth (at
    /// least 16) with `reserve_exact`, so slack stays bounded instead of
    /// doubling. Returns the heap bytes added.
    #[inline(always)]
    fn reserve_entry(&mut self) -> usize {
        if self.entries.len() < self.entries.capacity() {
            return 0;
        }
        self.grow_arena()
    }

    #[cold]
    #[inline(never)]
    fn grow_arena(&mut self) -> usize {
        let before = self.heap_bytes();
        let len = self.entries.len();
        let max = self.slot_idx.len();
        let extra = (len / 16).max(16).min(max.saturating_sub(len)).max(1);
        self.entries.reserve_exact(extra);
        self.last_access.reserve_exact(extra);
        self.heap_bytes() - before
    }

    /// Reserves exactly `n` more entries (used when building a segment
    /// whose final size is known).
    fn reserve_entries_exact(&mut self, n: usize) {
        self.entries.reserve_exact(n);
        self.last_access.reserve_exact(n);
    }

    /// Compacts the arena (dropping holes and slack) after deletes once less
    /// than half of it is live. Returns the heap bytes freed.
    #[inline(always)]
    fn maybe_trim_arena(&mut self) -> usize {
        let cap = self.entries.capacity();
        if cap < 32 || self.items * 2 >= cap {
            return 0;
        }
        self.trim_arena()
    }

    #[cold]
    #[inline(never)]
    fn trim_arena(&mut self) -> usize {
        let before = self.heap_bytes();
        let keep = self.items + self.items / 16;
        let mut entries = Vec::with_capacity(keep);
        let mut access = Vec::with_capacity(keep);
        // Walk slots, not the arena, so each live entry's slot is repointed
        // as it moves.
        for slot in 0..self.slot_idx.len() {
            let i = self.slot_idx[slot];
            if i == NO_ENTRY {
                continue;
            }
            self.slot_idx[slot] = entries.len() as u16;
            entries.push(std::mem::replace(
                &mut self.entries[i as usize],
                Self::hole(),
            ));
            access.push(self.last_access[i as usize]);
        }
        self.entries = entries;
        self.last_access = access;
        self.holes.fill(0);
        before - self.heap_bytes()
    }

    /// The placeholder left in a deleted entry's arena position.
    #[inline(always)]
    fn hole() -> RudisEntry {
        RudisEntry {
            key: crate::compact::CompactKey::default(),
            val: RudisValue::Int(0),
        }
    }

    /// Removes the entry at arena index `i` (owned by `slot`). The last
    /// entry is popped; any other leaves a hole for the next insert. Call
    /// after `mark_slot_free` (which updates `items`). Returns the entry
    /// and any heap bytes freed.
    #[inline(always)]
    fn take_entry(&mut self, slot: usize, i: usize) -> (RudisEntry, usize) {
        self.slot_idx[slot] = NO_ENTRY;
        let entry = if i + 1 == self.entries.len() {
            self.last_access.pop();
            self.entries.pop().expect("arena entry")
        } else {
            self.holes[i / 64] |= 1 << (i % 64);
            std::mem::replace(&mut self.entries[i], Self::hole())
        };
        if self.items == 0 {
            self.entries.clear();
            self.last_access.clear();
            self.holes.fill(0);
        }
        (entry, self.maybe_trim_arena())
    }

    /// Takes the lowest hole, if any, for reuse.
    #[inline(always)]
    fn take_hole(&mut self) -> Option<usize> {
        if self.entries.len() == self.items {
            return None;
        }
        for (w, word) in self.holes.iter_mut().enumerate() {
            if *word != 0 {
                let b = word.trailing_zeros() as usize;
                *word &= !(1 << b);
                return Some(w * 64 + b);
            }
        }
        None
    }

    /// Checks the slot/arena cross-links. Test and debug use only: O(slots).
    #[cfg(test)]
    pub fn check_invariants(&self) {
        let holes: usize = self.holes.iter().map(|w| w.count_ones() as usize).sum();
        assert_eq!(self.entries.len(), self.items + holes);
        assert_eq!(self.last_access.len(), self.entries.len());
        let mut seen = vec![false; self.entries.len()];
        for &i in self.slot_idx.iter().filter(|&&i| i != NO_ENTRY) {
            let i = i as usize;
            assert!(Self::is_live(&self.holes, i), "slot points at hole {i}");
            assert!(!seen[i], "arena index {i} linked twice");
            seen[i] = true;
        }
        let linked = self.slot_idx.iter().filter(|&&i| i != NO_ENTRY).count();
        assert_eq!(linked, self.items);
        for (slot, &i) in self.slot_idx.iter().enumerate() {
            if slot < self.capacity {
                let full = self.ctrl[slot] != EMPTY && self.ctrl[slot] != DELETED;
                assert_eq!(full, i != NO_ENTRY, "ctrl/slot mismatch at {slot}");
            } else {
                let full = self.stash_ctrl[slot - self.capacity] != EMPTY;
                assert_eq!(full, i != NO_ENTRY, "stash/slot mismatch at {slot}");
            }
        }
    }
}

#[inline(always)]
pub fn fast_slice_eq(a: &[u8], b: &[u8]) -> bool {
    // Overlapping fixed-width loads for short keys. The slice-to-array
    // conversions are bounds checked, and LLVM drops the checks because each
    // branch has already pinned `len`.
    #[inline(always)]
    fn w4(s: &[u8], at: usize) -> u32 {
        u32::from_ne_bytes(s[at..at + 4].try_into().unwrap_or_default())
    }
    #[inline(always)]
    fn w8(s: &[u8], at: usize) -> u64 {
        u64::from_ne_bytes(s[at..at + 8].try_into().unwrap_or_default())
    }
    let len = a.len();
    if len != b.len() {
        return false;
    }
    if len <= 8 {
        if len >= 4 {
            return w4(a, 0) == w4(b, 0) && w4(a, len - 4) == w4(b, len - 4);
        }
        return a == b;
    }
    if len <= 16 {
        return w8(a, 0) == w8(b, 0) && w8(a, len - 8) == w8(b, len - 8);
    }
    if len <= 32 {
        return w8(a, 0) == w8(b, 0)
            && w8(a, len - 8) == w8(b, len - 8)
            && w8(a, 8) == w8(b, 8)
            && w8(a, len - 16) == w8(b, len - 16);
    }
    a == b
}

impl RawSegment {
    #[inline(always)]
    pub fn find_entry(&self, key: &[u8], h: u64) -> Option<(usize, &RudisEntry)> {
        if self.items == 0 {
            return None;
        }
        let tag = fingerprint(h);
        let mut idx = (h as usize) & self.mask;
        prefetch_read(&self.slot_idx[idx]);
        let mut step = 0;

        loop {
            let (match_mask, empty_mask) = probe_group_match_or_empty(self.group(idx), tag);
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                if let Some(entry) = self.slot_entry(slot_idx)
                    && fast_slice_eq(entry.key.as_ref(), key)
                {
                    return Some((slot_idx, entry));
                }
                bits &= bits - 1;
            }

            if empty_mask != 0 {
                return None;
            }

            step += GROUP_SIZE;
            if step == 2 * GROUP_SIZE && self.stash_count > 0 {
                for s in 0..STASH_CAP {
                    if self.stash_ctrl[s] == tag {
                        let slot_idx = self.capacity + s;
                        if let Some(entry) = self.slot_entry(slot_idx)
                            && fast_slice_eq(entry.key.as_ref(), key)
                        {
                            return Some((slot_idx, entry));
                        }
                    }
                }
            }
            if step >= self.capacity {
                return None;
            }
            idx = (idx + step) & self.mask;
        }
    }

    #[inline(always)]
    pub fn contains(&self, key: &[u8], h: u64) -> bool {
        self.find_entry(key, h).is_some()
    }

    #[inline(always)]
    pub fn find_entry_mut(&mut self, key: &[u8], h: u64) -> Option<(usize, &mut RudisEntry)> {
        if self.items == 0 {
            return None;
        }
        let tag = fingerprint(h);
        let mut idx = (h as usize) & self.mask;
        prefetch_read(&self.slot_idx[idx]);
        let mut step = 0;

        loop {
            let (match_mask, empty_mask) = probe_group_match_or_empty(self.group(idx), tag);
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                if let Some(entry) = self.slot_entry(slot_idx)
                    && fast_slice_eq(entry.key.as_ref(), key)
                {
                    return self.slot_entry_mut(slot_idx).map(|e| (slot_idx, e));
                }
                bits &= bits - 1;
            }

            if empty_mask != 0 {
                return None;
            }

            step += GROUP_SIZE;
            if step == 2 * GROUP_SIZE && self.stash_count > 0 {
                for s in 0..STASH_CAP {
                    if self.stash_ctrl[s] == tag {
                        let slot_idx = self.capacity + s;
                        if let Some(entry) = self.slot_entry(slot_idx)
                            && fast_slice_eq(entry.key.as_ref(), key)
                        {
                            return self.slot_entry_mut(slot_idx).map(|e| (slot_idx, e));
                        }
                    }
                }
            }
            if step >= self.capacity {
                return None;
            }
            idx = (idx + step) & self.mask;
        }
    }

    #[inline(always)]
    pub fn find_or_prepare_insert_raw(&self, key: &[u8], h: u64) -> (Option<usize>, usize) {
        if self.items == 0 {
            return (None, (h as usize) & self.mask);
        }
        let tag = fingerprint(h);
        let mut idx = (h as usize) & self.mask;
        prefetch_read(&self.slot_idx[idx]);
        let mut step = 0;
        let mut first_free: Option<usize> = None;

        loop {
            let (match_mask, del_mask, empty_mask) =
                probe_group_match_del_empty(self.group(idx), tag);
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                if let Some(entry) = self.slot_entry(slot_idx)
                    && fast_slice_eq(entry.key.as_ref(), key)
                {
                    return (Some(slot_idx), slot_idx);
                }
                bits &= bits - 1;
            }

            if first_free.is_none() && del_mask != 0 {
                let offset = del_mask.trailing_zeros() as usize;
                first_free = Some((idx + offset) & self.mask);
            }

            if empty_mask != 0 {
                let free_idx = match first_free {
                    Some(f) => f,
                    None => {
                        let offset = empty_mask.trailing_zeros() as usize;
                        (idx + offset) & self.mask
                    }
                };
                return (None, free_idx);
            }

            step += GROUP_SIZE;
            if step == 2 * GROUP_SIZE {
                if self.stash_count > 0 {
                    for s in 0..STASH_CAP {
                        if self.stash_ctrl[s] == tag {
                            let slot_idx = self.capacity + s;
                            if let Some(entry) = self.slot_entry(slot_idx)
                                && fast_slice_eq(entry.key.as_ref(), key)
                            {
                                return (Some(slot_idx), slot_idx);
                            }
                        }
                    }
                }
                if first_free.is_none() && (self.stash_count as usize) < STASH_CAP {
                    for s in 0..STASH_CAP {
                        if self.stash_ctrl[s] == EMPTY {
                            first_free = Some(self.capacity + s);
                            break;
                        }
                    }
                }
            }
            if step >= self.capacity {
                return (None, first_free.unwrap_or(idx));
            }
            idx = (idx + step) & self.mask;
        }
    }

    /// Places `entry` in the free slot `insert_idx` (from
    /// `find_or_prepare_insert_raw`). Returns heap bytes added by arena
    /// growth, for the table's `seg_heap_bytes`.
    #[inline(always)]
    pub fn insert_at(&mut self, entry: RudisEntry, h: u64, insert_idx: usize) -> usize {
        let tag = fingerprint(h);
        let hole = self.take_hole();
        let grown = if hole.is_none() {
            self.reserve_entry()
        } else {
            0
        };
        if insert_idx < self.capacity {
            let was_empty = self.ctrl[insert_idx] == EMPTY;
            self.set_ctrl(insert_idx, tag);
            if was_empty {
                self.growth_left = self.growth_left.saturating_sub(1);
            }
        } else {
            let s = insert_idx - self.capacity;
            self.stash_ctrl[s] = tag;
            self.stash_count += 1;
            self.growth_left = self.growth_left.saturating_sub(1);
        }
        debug_assert_eq!(self.slot_idx[insert_idx], NO_ENTRY);
        let i = if let Some(i) = hole {
            self.entries[i] = entry;
            self.last_access[i] = lru_now();
            i
        } else {
            self.entries.push(entry);
            self.last_access.push(lru_now());
            self.entries.len() - 1
        };
        self.slot_idx[insert_idx] = i as u16;
        self.items += 1;
        grown
    }

    #[inline(always)]
    fn insert_migrated(&mut self, entry: RudisEntry, h: u64, access: u16) {
        let (_, idx) = self.find_or_prepare_insert_raw(&entry.key, h);
        self.insert_at(entry, h, idx);
        if let Some(a) = self.slot_access_mut(idx) {
            *a = access;
        }
    }

    /// Replaces the entry in an occupied slot, stamping its access clock.
    #[inline(always)]
    pub fn replace_at(&mut self, slot: usize, entry: RudisEntry) -> Option<RudisEntry> {
        let e = self.touch_entry_mut(slot)?;
        Some(std::mem::replace(e, entry))
    }

    #[inline(always)]
    fn mark_slot_free(&mut self, slot_idx: usize) {
        if slot_idx < self.capacity {
            self.set_ctrl(slot_idx, DELETED);
        } else {
            self.stash_ctrl[slot_idx - self.capacity] = EMPTY;
            self.stash_count = self.stash_count.saturating_sub(1);
            self.growth_left += 1;
        }
        self.items -= 1;
        if self.items == 0 {
            self.ctrl.fill(EMPTY);
            self.stash_ctrl = [EMPTY; STASH_CAP];
            self.stash_count = 0;
            let stash_bonus = if self.capacity == SEG_CAP {
                STASH_CAP
            } else {
                0
            };
            self.growth_left = (self.capacity * 7) / 8 + stash_bonus;
        }
    }

    /// Removes the entry in `slot_idx`, if any. Also returns heap bytes
    /// freed by trimming the arena.
    #[inline(always)]
    pub fn remove(&mut self, slot_idx: usize) -> Option<(RudisEntry, usize)> {
        let i = *self.slot_idx.get(slot_idx)? as usize;
        if i >= self.entries.len() {
            return None;
        }
        self.mark_slot_free(slot_idx);
        Some(self.take_entry(slot_idx, i))
    }

    #[inline(always)]
    pub fn remove_present(&mut self, slot_idx: usize) -> (RudisEntry, usize) {
        let i = self.slot_idx[slot_idx] as usize;
        assert!(
            i < self.entries.len(),
            "remove_present: slot must be occupied"
        );
        self.mark_slot_free(slot_idx);
        self.take_entry(slot_idx, i)
    }

    pub fn rebuild(&mut self, new_cap: usize) {
        let mut next = RawSegment::new(new_cap, self.local_depth);
        next.reserve_entries_exact(self.items);
        for (entry, access) in self.drain_entries() {
            let h = mix_hash(hash_key(&entry.key));
            next.insert_migrated(entry, h, access);
        }
        *self = next;
    }

    /// Heap bytes owned by this segment's arrays (ctrl, slot index, entry
    /// arena and its parallel arrays). Entries' own key/value allocations
    /// are not included.
    #[inline]
    pub fn heap_bytes(&self) -> usize {
        self.ctrl.capacity()
            + self.slot_idx.capacity() * std::mem::size_of::<u16>()
            + self.entries.capacity() * std::mem::size_of::<RudisEntry>()
            + self.last_access.capacity() * std::mem::size_of::<u16>()
            + self.holes.capacity() * std::mem::size_of::<u64>()
    }
}

/// Dragonfly-style Extendible Hashing Table (`Directory` + Fixed-Size SIMD `RawSegment`s).
/// Eliminates monolithic stop-the-world resizes and `old_table` double-lookup overhead.
pub struct RudisFlatTable {
    pub segments: Vec<RawSegment>,
    pub directory: Vec<u32>,
    pub global_depth: u8,
    dir_mask: usize,
    pub capacity: usize,
    pub items: usize,
    pub slot_counts: Box<[u32; 16384]>,
    /// Bumped when segments are renumbered (`clear`, `defrag` collapsing to
    /// one segment). Splits only append segments and in-place rebuilds keep
    /// a segment's entries in it, so walking segment ids in order sees every
    /// entry that stays put for the whole walk unless this changes.
    layout_epoch: u64,
    /// Sum of `RawSegment::heap_bytes` over `segments`, kept current by every
    /// path that adds, replaces or rebuilds a segment.
    seg_heap_bytes: usize,
}

impl RudisFlatTable {
    pub fn new(capacity: usize) -> Self {
        let init_cap = capacity.next_power_of_two().clamp(GROUP_SIZE, SEG_CAP);
        let seg = RawSegment::new(init_cap, 0);
        let seg_heap_bytes = seg.heap_bytes();
        Self {
            segments: vec![seg],
            directory: vec![0],
            global_depth: 0,
            dir_mask: 0,
            capacity: init_cap,
            items: 0,
            slot_counts: vec![0u32; 16384].into_boxed_slice().try_into().unwrap(),
            layout_epoch: 0,
            seg_heap_bytes,
        }
    }

    /// Bytes the table structure itself occupies: every segment's slot,
    /// ctrl and access arrays, the directory, the segment vector and the
    /// cluster slot counters. O(1).
    #[inline]
    pub fn struct_bytes(&self) -> usize {
        self.seg_heap_bytes
            + self.segments.capacity() * std::mem::size_of::<RawSegment>()
            + self.directory.capacity() * std::mem::size_of::<u32>()
            + std::mem::size_of::<[u32; 16384]>()
    }

    fn recompute_seg_heap_bytes(&mut self) {
        self.seg_heap_bytes = self.segments.iter().map(RawSegment::heap_bytes).sum();
    }

    #[inline(always)]
    fn dir_index(&self, mixed_hash: u64) -> usize {
        ((mixed_hash >> SEG_SHIFT) as usize) & self.dir_mask
    }

    /// Splits or grows `segments[seg_id]` when `growth_left == 0`.
    fn split_or_grow_segment(&mut self, mut dir_idx: usize, mut seg_id: usize, mixed_hash: u64) {
        while self.segments[seg_id].growth_left == 0 {
            let seg_items = self.segments[seg_id].items;
            let seg_cap = self.segments[seg_id].capacity;
            let old_heap = self.segments[seg_id].heap_bytes();

            // 1. If < 50% full (dominated by DELETED tombstones), compact segment in-place.
            if seg_items * 2 < seg_cap {
                self.segments[seg_id].rebuild(seg_cap);
                self.seg_heap_bytes =
                    self.seg_heap_bytes - old_heap + self.segments[seg_id].heap_bytes();
                break;
            }

            // 2. If single initial segment hasn't reached SEG_CAP (1024) yet, double in-place.
            if seg_cap < SEG_CAP {
                let new_cap = (seg_cap * 2).min(SEG_CAP);
                self.segments[seg_id].rebuild(new_cap);
                self.seg_heap_bytes =
                    self.seg_heap_bytes - old_heap + self.segments[seg_id].heap_bytes();
                self.capacity = if self.segments.len() == 1 {
                    new_cap
                } else {
                    self.segments.len() << SEG_SHIFT
                };
                break;
            }

            // 3. Extendible Hashing Segment Split (1024-slot segment -> two 1024-slot segments)
            let d = self.segments[seg_id].local_depth;
            if d == self.global_depth {
                let len = self.directory.len();
                self.directory.reserve(len);
                for i in 0..len {
                    self.directory.push(self.directory[i]);
                }
                self.global_depth += 1;
                self.dir_mask = self.directory.len() - 1;
                dir_idx = self.dir_index(mixed_hash);
            }

            let mut seg_zero = RawSegment::new(SEG_CAP, d + 1);
            let mut seg_one = RawSegment::new(SEG_CAP, d + 1);
            let bit_shift = SEG_SHIFT + (d as usize);

            // Size each half's arena exactly: hash once, count, then move.
            let moved: Vec<(RudisEntry, u16, u64)> = self.segments[seg_id]
                .drain_entries()
                .map(|(entry, acc)| {
                    let h = mix_hash(hash_key(&entry.key));
                    (entry, acc, h)
                })
                .collect();
            let ones = moved
                .iter()
                .filter(|(_, _, h)| (h >> bit_shift) & 1 == 1)
                .count();
            seg_zero.reserve_entries_exact(moved.len() - ones);
            seg_one.reserve_entries_exact(ones);
            for (entry, acc, h) in moved {
                if ((h >> bit_shift) & 1) == 0 {
                    seg_zero.insert_migrated(entry, h, acc);
                } else {
                    seg_one.insert_migrated(entry, h, acc);
                }
            }

            self.seg_heap_bytes =
                self.seg_heap_bytes - old_heap + seg_zero.heap_bytes() + seg_one.heap_bytes();
            self.segments[seg_id] = seg_zero;
            let new_seg_id = self.segments.len();
            self.segments.push(seg_one);
            self.capacity = self.segments.len() << SEG_SHIFT;

            let base = dir_idx & ((1usize << d) - 1);
            let step = 1usize << (d + 1);
            let mut i = base | (1usize << d);
            while i < self.directory.len() {
                self.directory[i] = new_seg_id as u32;
                i += step;
            }

            dir_idx = self.dir_index(mixed_hash);
            seg_id = self.directory[dir_idx] as usize;
        }
    }

    #[inline(always)]
    pub fn find_entry(&self, key: &[u8], hash: u64) -> Option<(usize, &RudisEntry)> {
        if self.items == 0 {
            return None;
        }
        let h = mix_hash(hash);
        let dir_idx = self.dir_index(h);
        let seg_id = self.directory[dir_idx] as usize;
        let seg = &self.segments[seg_id];
        seg.find_entry(key, h)
            .map(|(local_idx, entry)| ((seg_id << GLOBAL_IDX_SHIFT) | local_idx, entry))
    }

    #[inline(always)]
    pub fn contains(&self, key: &[u8], hash: u64) -> bool {
        if self.items == 0 {
            return false;
        }
        let h = mix_hash(hash);
        let dir_idx = self.dir_index(h);
        let seg_id = self.directory[dir_idx] as usize;
        let seg = &self.segments[seg_id];
        seg.contains(key, h)
    }

    #[inline(always)]
    pub fn find_entry_mut(&mut self, key: &[u8], hash: u64) -> Option<(usize, &mut RudisEntry)> {
        if self.items == 0 {
            return None;
        }
        let h = mix_hash(hash);
        let dir_idx = self.dir_index(h);
        let seg_id = self.directory[dir_idx] as usize;
        let seg = &mut self.segments[seg_id];
        let (local_idx, _) = seg.find_entry(key, h)?;
        let entry = seg.touch_entry_mut(local_idx)?;
        Some(((seg_id << GLOBAL_IDX_SHIFT) | local_idx, entry))
    }

    #[inline(always)]
    pub fn find(&mut self, key: &[u8], hash: u64) -> Option<usize> {
        self.find_entry(key, hash).map(|(idx, _)| idx)
    }

    #[inline(always)]
    pub fn find_or_prepare_insert(&mut self, key: &[u8], hash: u64) -> (Option<usize>, usize) {
        let h = mix_hash(hash);
        let mut dir_idx = self.dir_index(h);
        let mut seg_id = self.directory[dir_idx] as usize;

        if self.segments[seg_id].growth_left == 0 {
            if let Some((local_idx, _)) = self.segments[seg_id].find_entry(key, h) {
                let global_idx = (seg_id << GLOBAL_IDX_SHIFT) | local_idx;
                return (Some(global_idx), global_idx);
            }
            self.split_or_grow_segment(dir_idx, seg_id, h);
            dir_idx = self.dir_index(h);
            seg_id = self.directory[dir_idx] as usize;
        }

        let (existing, local_idx) = self.segments[seg_id].find_or_prepare_insert_raw(key, h);
        let base = seg_id << GLOBAL_IDX_SHIFT;
        (existing.map(|i| base | i), base | local_idx)
    }

    #[inline(always)]
    pub fn is_rehashing(&self) -> bool {
        false
    }

    #[inline(always)]
    pub fn migrate_key_if_in_old(&mut self, _key: &[u8], _hash: u64) {}

    #[inline(always)]
    pub fn rehash_step(&mut self, _n: usize) -> bool {
        false
    }

    #[inline(always)]
    pub fn finish_rehash(&mut self) {}

    pub fn insert(&mut self, entry: RudisEntry) -> Option<RudisEntry> {
        let h = hash_key(&entry.key);
        let (existing, global_idx) = self.find_or_prepare_insert(&entry.key, h);
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;

        if existing.is_some() {
            self.segments[seg_id].replace_at(local_idx, entry)
        } else {
            let slot = crate::router::key_slot(&entry.key) as usize;
            self.slot_counts[slot] += 1;
            self.seg_heap_bytes += self.segments[seg_id].insert_at(entry, mix_hash(h), local_idx);
            self.items += 1;
            None
        }
    }

    #[inline(always)]
    pub fn insert_prepared(&mut self, entry: RudisEntry, hash: u64, global_idx: usize) {
        let slot = crate::router::key_slot(&entry.key) as usize;
        self.slot_counts[slot] += 1;
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        self.seg_heap_bytes += self.segments[seg_id].insert_at(entry, mix_hash(hash), local_idx);
        self.items += 1;
    }

    #[inline(always)]
    pub fn remove_key(&mut self, key: &[u8], hash: u64) -> Option<RudisEntry> {
        if self.items == 0 {
            return None;
        }
        let (idx, _) = self.find_entry(key, hash)?;
        Some(self.remove_present(idx))
    }

    #[inline(always)]
    pub fn remove(&mut self, global_idx: usize) -> Option<RudisEntry> {
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        let (entry, freed) = self.segments.get_mut(seg_id)?.remove(local_idx)?;
        self.seg_heap_bytes -= freed;
        self.items -= 1;
        let slot = crate::router::key_slot(&entry.key) as usize;
        self.slot_counts[slot] = self.slot_counts[slot].saturating_sub(1);
        Some(entry)
    }

    #[inline(always)]
    pub fn remove_present(&mut self, global_idx: usize) -> RudisEntry {
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        let (entry, freed) = self.segments[seg_id].remove_present(local_idx);
        self.seg_heap_bytes -= freed;
        self.items -= 1;
        let slot = crate::router::key_slot(&entry.key) as usize;
        self.slot_counts[slot] = self.slot_counts[slot].saturating_sub(1);
        entry
    }

    #[inline(always)]
    pub fn get_slot(&self, global_idx: usize) -> Option<&RudisEntry> {
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        self.segments.get(seg_id)?.slot_entry(local_idx)
    }

    /// Like [`Self::get_slot_mut`] but leaves `last_access` alone, for
    /// internal representation changes that aren't client accesses.
    #[inline(always)]
    pub fn get_slot_mut_no_touch(&mut self, global_idx: usize) -> Option<&mut RudisEntry> {
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        self.segments.get_mut(seg_id)?.slot_entry_mut(local_idx)
    }

    #[inline(always)]
    pub fn get_slot_mut(&mut self, global_idx: usize) -> Option<&mut RudisEntry> {
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        self.segments.get_mut(seg_id)?.touch_entry_mut(local_idx)
    }

    #[inline(always)]
    pub fn touch_slot(&mut self, global_idx: usize) {
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        if let Some(seg) = self.segments.get_mut(seg_id)
            && let Some(acc) = seg.slot_access_mut(local_idx)
        {
            *acc = lru_now();
        }
    }

    #[inline(always)]
    pub fn get_slot_last_access(&self, global_idx: usize) -> u16 {
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        self.segments
            .get(seg_id)
            .and_then(|s| s.slot_access(local_idx))
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub fn set_slot_last_access(&mut self, global_idx: usize, access: u16) {
        let seg_id = global_idx >> GLOBAL_IDX_SHIFT;
        let local_idx = global_idx & GLOBAL_IDX_MASK;
        if let Some(a) = self.segments[seg_id].slot_access_mut(local_idx) {
            *a = access;
        }
    }

    #[cfg(test)]
    pub fn check_invariants(&self) {
        for seg in &self.segments {
            seg.check_invariants();
        }
        assert_eq!(
            self.segments.iter().map(|s| s.items).sum::<usize>(),
            self.items
        );
    }

    #[inline]
    pub fn entries(&self) -> impl Iterator<Item = &RudisEntry> {
        self.segments.iter().flat_map(|s| s.iter_entries())
    }

    pub fn layout_epoch(&self) -> u64 {
        self.layout_epoch
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn segment_entries(&self, seg: usize) -> impl Iterator<Item = &RudisEntry> {
        self.segments
            .get(seg)
            .into_iter()
            .flat_map(|s| s.iter_entries())
    }

    #[inline]
    pub fn entries_mut(&mut self) -> impl Iterator<Item = &mut RudisEntry> {
        self.segments.iter_mut().flat_map(|s| s.iter_entries_mut())
    }

    #[inline]
    pub fn enumerate_slots(&self) -> impl Iterator<Item = (usize, Option<&RudisEntry>)> {
        self.segments.iter().enumerate().flat_map(|(seg_id, seg)| {
            let base = seg_id << GLOBAL_IDX_SHIFT;
            (0..seg.slot_count())
                .map(move |local_idx| (base | local_idx, seg.slot_entry(local_idx)))
        })
    }

    #[inline]
    pub fn has_deleted(&self) -> bool {
        self.segments.iter().any(|s| s.ctrl.contains(&DELETED))
    }

    #[inline]
    pub fn ctrl_bytes(&self) -> usize {
        self.segments.iter().map(|s| s.ctrl.len()).sum::<usize>()
            + self.directory.len() * std::mem::size_of::<u32>()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.items
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.items == 0
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline(always)]
    pub fn cursor_bound(&self) -> usize {
        if self.segments.len() == 1 {
            self.segments[0].slot_count()
        } else {
            self.segments.len() * (SEG_CAP + STASH_CAP)
        }
    }

    #[inline(always)]
    pub fn cursor_to_global_idx(&self, cursor: usize) -> usize {
        if self.segments.len() == 1 {
            cursor
        } else {
            let stride = SEG_CAP + STASH_CAP;
            let seg_id = cursor / stride;
            let local_idx = cursor % stride;
            (seg_id << GLOBAL_IDX_SHIFT) | local_idx
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        let seg = RawSegment::new(64, 0);
        self.layout_epoch += 1;
        self.segments.clear();
        self.segments.push(seg);
        self.directory.clear();
        self.directory.push(0);
        self.global_depth = 0;
        self.dir_mask = 0;
        self.capacity = 64;
        self.items = 0;
        self.slot_counts.fill(0);
        self.recompute_seg_heap_bytes();
    }

    pub fn defrag(&mut self) -> usize {
        let before_cap = self.capacity;
        let optimal_cap = (self.items * 2).next_power_of_two().max(64);
        let has_del = self.has_deleted();

        if optimal_cap <= SEG_CAP
            && (self.segments.len() > 1 || optimal_cap < before_cap || has_del)
        {
            let target_cap = optimal_cap.clamp(64, SEG_CAP);
            let mut single = RawSegment::new(target_cap, 0);
            single.reserve_entries_exact(self.items);
            for seg in self.segments.iter_mut() {
                for (entry, access) in seg.drain_entries() {
                    let h = mix_hash(hash_key(&entry.key));
                    single.insert_migrated(entry, h, access);
                }
            }
            self.layout_epoch += 1;
            self.segments.clear();
            self.segments.push(single);
            self.directory.clear();
            self.directory.push(0);
            self.global_depth = 0;
            self.dir_mask = 0;
            self.capacity = target_cap;
            self.recompute_seg_heap_bytes();
            before_cap.saturating_sub(self.capacity)
        } else if has_del {
            for seg in self.segments.iter_mut() {
                if seg.ctrl.contains(&DELETED) {
                    let cap = seg.capacity;
                    seg.rebuild(cap);
                }
            }
            self.recompute_seg_heap_bytes();
            0
        } else {
            0
        }
    }

    /// Whether `seg` is a hole left by [`RudisFlatTable::shrink_step`]: an
    /// empty placeholder no directory entry points to.
    #[inline]
    fn is_hole(seg: &RawSegment) -> bool {
        seg.local_depth == HOLE_DEPTH
    }

    /// Undoes segment splits after mass deletes, a bounded amount of work at
    /// a time (meant for a periodic tick): merges up to `max_merges` pairs of
    /// buddy segments whose combined live entries fit comfortably in one
    /// segment, then halves the directory while no segment needs its full
    /// depth. Returns the number of merges.
    ///
    /// The merged segment takes the higher of the two ids and the lower one
    /// becomes a tiny hole, so a positional SCAN cursor that already passed
    /// the lower id still meets the moved entries. Holes are compacted away
    /// once they make up most of the segment vector. Bumps `layout_epoch`
    /// whenever entries move between segments.
    pub fn shrink_step(&mut self, max_merges: usize) -> usize {
        if self.segments.len() <= 1 || max_merges == 0 {
            return 0;
        }
        let mut merges = 0;
        let mut i = 0;
        while i < self.directory.len() && merges < max_merges {
            let a = self.directory[i] as usize;
            let d = self.segments[a].local_depth;
            // Visit each pair from the half whose bit d-1 is clear.
            if d == 0 || i & (1usize << (d - 1)) != 0 {
                i += 1;
                continue;
            }
            let pattern = i & ((1usize << d) - 1);
            let b = self.directory[pattern | (1usize << (d - 1))] as usize;
            if b != a
                && self.segments[b].local_depth == d
                && self.segments[a].items + self.segments[b].items <= MERGE_MAX_ITEMS
            {
                self.merge_buddies(a, b, d, pattern & ((1usize << (d - 1)) - 1));
                merges += 1;
            }
            i += 1;
        }
        if merges > 0 {
            self.layout_epoch += 1;
            self.shrink_directory();
            let holes = self.segments.iter().filter(|s| Self::is_hole(s)).count();
            if holes * 2 >= self.segments.len() {
                self.compact_holes();
            }
            self.recompute_seg_heap_bytes();
            let live =
                self.segments.len() - self.segments.iter().filter(|s| Self::is_hole(s)).count();
            self.capacity = if self.segments.len() == 1 {
                self.segments[0].capacity
            } else {
                live << SEG_SHIFT
            };
        }
        merges
    }

    /// Merges buddy segments `a` and `b` (both at local depth `d`) into one
    /// at depth `d - 1`, repointing every directory entry whose low `d - 1`
    /// bits equal `pattern`.
    fn merge_buddies(&mut self, a: usize, b: usize, d: u8, pattern: usize) {
        let (keep, hole) = if a > b { (a, b) } else { (b, a) };
        let mut merged = RawSegment::new(SEG_CAP, d - 1);
        merged.reserve_entries_exact(self.segments[a].items + self.segments[b].items);
        for id in [hole, keep] {
            for (entry, access) in self.segments[id].drain_entries() {
                let h = mix_hash(hash_key(&entry.key));
                merged.insert_migrated(entry, h, access);
            }
        }
        self.segments[keep] = merged;
        let mut placeholder = RawSegment::new(GROUP_SIZE, 0);
        placeholder.local_depth = HOLE_DEPTH;
        self.segments[hole] = placeholder;
        let low_mask = (1usize << (d - 1)) - 1;
        for (j, slot) in self.directory.iter_mut().enumerate() {
            if j & low_mask == pattern {
                *slot = keep as u32;
            }
        }
    }

    /// Halves the directory while every segment's local depth is below the
    /// global depth (each segment then appears in both halves).
    fn shrink_directory(&mut self) {
        while self.global_depth > 0 {
            let max_depth = self
                .directory
                .iter()
                .map(|&s| self.segments[s as usize].local_depth)
                .max()
                .unwrap_or(0);
            if max_depth >= self.global_depth {
                break;
            }
            let half = self.directory.len() / 2;
            self.directory.truncate(half);
            self.directory.shrink_to_fit();
            self.global_depth -= 1;
            self.dir_mask = self.directory.len() - 1;
        }
    }

    /// Drops hole segments and renumbers the rest (directory included).
    fn compact_holes(&mut self) {
        let mut remap = vec![u32::MAX; self.segments.len()];
        let mut kept = Vec::with_capacity(self.segments.len());
        for (old_id, seg) in std::mem::take(&mut self.segments).into_iter().enumerate() {
            if !Self::is_hole(&seg) {
                remap[old_id] = kept.len() as u32;
                kept.push(seg);
            }
        }
        kept.shrink_to_fit();
        self.segments = kept;
        for slot in self.directory.iter_mut() {
            *slot = remap[*slot as usize];
        }
        self.layout_epoch += 1;
    }
}

static EXPIRED_KEYS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static EXPIRED_KEYS_ACTIVE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static EVICTED_KEYS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static LAZYFREED_OBJECTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[inline]
pub fn add_lazyfreed_objects(n: u64) {
    LAZYFREED_OBJECTS.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn get_lazyfreed_objects() -> u64 {
    LAZYFREED_OBJECTS.load(std::sync::atomic::Ordering::Relaxed)
}

#[inline]
pub fn inc_expired_keys() {
    EXPIRED_KEYS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    crate::connection::record_latency_event("expire-cycle", 25);
}

#[inline]
pub fn get_expired_keys() -> u64 {
    EXPIRED_KEYS.load(std::sync::atomic::Ordering::Relaxed)
}

#[inline]
pub fn inc_expired_keys_active() {
    EXPIRED_KEYS_ACTIVE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    crate::connection::record_latency_event("expire-cycle", 25);
}

#[inline]
pub fn get_expired_keys_active() -> u64 {
    EXPIRED_KEYS_ACTIVE.load(std::sync::atomic::Ordering::Relaxed)
}

#[inline]
pub fn reset_expired_keys() {
    EXPIRED_KEYS.store(0, std::sync::atomic::Ordering::Relaxed);
    EXPIRED_KEYS_ACTIVE.store(0, std::sync::atomic::Ordering::Relaxed);
    LAZYFREED_OBJECTS.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn inc_evicted_keys() {
    EVICTED_KEYS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn get_evicted_keys() -> u64 {
    EVICTED_KEYS.load(std::sync::atomic::Ordering::Relaxed)
}

#[inline]
pub fn compute_digest(bytes: &[u8]) -> String {
    let h = xxhash_rust::xxh3::xxh3_64(bytes);
    format!("{:016x}", h)
}

/// The complete thread-local Rudis storage engine unifying:
/// 1. Flat SIMD-accelerated hash table (`RudisFlatTable`)
/// 2. Inlined TTL expiration
/// 3. Secondary cluster slot index
pub struct RudisTable {
    table: RudisFlatTable,
    sample_cursor: usize,
    spill_cursor: usize,
    /// Bytes held by entries: keys, values and a per-entry allocation
    /// overhead. The table's own slot arrays are counted by
    /// `RudisFlatTable::struct_bytes`; [`RudisTable::used_memory`] is the sum.
    pub data_bytes: usize,
    pub arena: crate::allocator::SmallCollectionArena,
    pub num_expires: usize,
    pub hash_field_expires: hashbrown::HashMap<Bytes, hashbrown::HashMap<Bytes, Instant>>,
    pub dropped_tier: smallvec::SmallVec<[(TieredPointer, bool); 4]>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SmoveResult {
    pub moved: bool,
    pub dst_added: bool,
}

impl Default for RudisTable {
    fn default() -> Self {
        Self::new()
    }
}

fn find_bit_in_slice(slice: &[u8], bit: u8) -> Option<usize> {
    let mut i = 0;
    if bit == 1 {
        while i + 8 <= slice.len() {
            let chunk = u64::from_be_bytes(slice[i..i + 8].try_into().unwrap());
            if chunk != 0 {
                return Some(i * 8 + chunk.leading_zeros() as usize);
            }
            i += 8;
        }
        while i < slice.len() {
            let b = slice[i];
            if b != 0 {
                return Some(i * 8 + b.leading_zeros() as usize);
            }
            i += 1;
        }
    } else {
        while i + 8 <= slice.len() {
            let chunk = u64::from_be_bytes(slice[i..i + 8].try_into().unwrap());
            if chunk != u64::MAX {
                return Some(i * 8 + (!chunk).leading_zeros() as usize);
            }
            i += 8;
        }
        while i < slice.len() {
            let b = slice[i];
            if b != 0xff {
                return Some(i * 8 + (!b).leading_zeros() as usize);
            }
            i += 1;
        }
    }
    None
}

fn count_ones_in_slice(slice: &[u8]) -> usize {
    let mut count = 0;
    let mut i = 0;
    while i + 8 <= slice.len() {
        let chunk = u64::from_ne_bytes(slice[i..i + 8].try_into().unwrap());
        count += chunk.count_ones() as usize;
        i += 8;
    }
    while i < slice.len() {
        count += slice[i].count_ones() as usize;
        i += 1;
    }
    count
}

fn get_unsigned_bitfield(p: &[u8], mut offset: u64, bits: usize) -> u64 {
    let mut value: u64 = 0;
    for _ in 0..bits {
        let byte_idx = (offset >> 3) as usize;
        let bit_idx = 7 - (offset & 7);
        let byteval = if byte_idx < p.len() { p[byte_idx] } else { 0 };
        let bitval = ((byteval >> bit_idx) & 1) as u64;
        value = (value << 1) | bitval;
        offset += 1;
    }
    value
}

fn set_unsigned_bitfield(p: &mut [u8], mut offset: u64, bits: usize, value: u64) {
    for j in 0..bits {
        let bitval = ((value & (1u64 << (bits - 1 - j))) != 0) as u8;
        let byte_idx = (offset >> 3) as usize;
        let bit_idx = 7 - (offset & 7);
        if byte_idx < p.len() {
            let mut byteval = p[byte_idx];
            byteval &= !(1 << bit_idx);
            byteval |= bitval << bit_idx;
            p[byte_idx] = byteval;
        }
        offset += 1;
    }
}

fn get_signed_bitfield(p: &[u8], offset: u64, bits: usize) -> i64 {
    let u = get_unsigned_bitfield(p, offset, bits);
    let mut value = u as i64;
    if bits < 64 && (value & (1i64 << (bits - 1))) != 0 {
        value |= (!0i64) << bits;
    }
    value
}

fn set_signed_bitfield(p: &mut [u8], offset: u64, bits: usize, value: i64) {
    set_unsigned_bitfield(p, offset, bits, value as u64);
}

fn check_unsigned_bitfield_overflow(
    value: u64,
    incr: i64,
    bits: usize,
    owtype: crate::resp::BitfieldOverflow,
) -> (bool, u64) {
    let max = if bits == 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    };
    let maxincr = max.wrapping_sub(value) as i64;
    let minincr = -(value as i64);

    let wrap = || {
        let res = value.wrapping_add(incr as u64);
        if bits == 64 {
            res
        } else {
            let mask = (!0u64) << bits;
            res & !mask
        }
    };

    if value > max || (incr > 0 && incr > maxincr) {
        let limit = match owtype {
            crate::resp::BitfieldOverflow::Wrap => wrap(),
            crate::resp::BitfieldOverflow::Sat => max,
            crate::resp::BitfieldOverflow::Fail => 0,
        };
        (true, limit)
    } else if incr < 0 && incr < minincr {
        let limit = match owtype {
            crate::resp::BitfieldOverflow::Wrap => wrap(),
            crate::resp::BitfieldOverflow::Sat => 0,
            crate::resp::BitfieldOverflow::Fail => 0,
        };
        (true, limit)
    } else {
        (false, value.wrapping_add(incr as u64))
    }
}

fn check_signed_bitfield_overflow(
    value: i64,
    incr: i64,
    bits: usize,
    owtype: crate::resp::BitfieldOverflow,
) -> (bool, i64) {
    let max = if bits == 64 {
        i64::MAX
    } else {
        (1i64 << (bits - 1)) - 1
    };
    let min = (-max) - 1;

    let maxincr = (max as u64).wrapping_sub(value as u64) as i64;
    let minincr = (min as u64).wrapping_sub(value as u64) as i64;

    let wrap = || {
        let msb = 1u64 << (bits - 1);
        let a = value as u64;
        let b = incr as u64;
        let mut c = a.wrapping_add(b);
        if bits < 64 {
            let mask = (!0u64) << bits;
            if (c & msb) != 0 {
                c |= mask;
            } else {
                c &= !mask;
            }
        }
        c as i64
    };

    if value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr) {
        let limit = match owtype {
            crate::resp::BitfieldOverflow::Wrap => wrap(),
            crate::resp::BitfieldOverflow::Sat => max,
            crate::resp::BitfieldOverflow::Fail => 0,
        };
        (true, limit)
    } else if value < min
        || (bits != 64 && incr < minincr)
        || (value < 0 && incr < 0 && incr < minincr)
    {
        let limit = match owtype {
            crate::resp::BitfieldOverflow::Wrap => wrap(),
            crate::resp::BitfieldOverflow::Sat => min,
            crate::resp::BitfieldOverflow::Fail => 0,
        };
        (true, limit)
    } else {
        (false, (value as u64).wrapping_add(incr as u64) as i64)
    }
}

pub static LAZYFREE_LAZY_EXPIRE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[inline]
pub fn set_lazyfree_lazy_expire(val: bool) {
    LAZYFREE_LAZY_EXPIRE.store(val, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn is_lazyfree_lazy_expire() -> bool {
    LAZYFREE_LAZY_EXPIRE.load(std::sync::atomic::Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrexTtlAction {
    KeepTtl,
    Persist,
    SetPxat(u64),
}

#[derive(Debug)]
pub struct IncrexOutput {
    pub is_float: bool,
    pub val_int: i64,
    pub delta_int: i64,
    pub val_float: f64,
    pub delta_float: f64,
    pub rep_cmd: Option<Command>,
    pub events: smallvec::SmallVec<[&'static str; 2]>,
}

/// Capacity to reserve for `count` serialized elements of at least
/// `min_size` bytes each starting at `data[cursor..]`: never more than the
/// remaining bytes can hold, so a forged count cannot force a huge
/// allocation before the decoder runs out of input.
fn claimed_capacity(count: usize, data: &[u8], cursor: usize, min_size: usize) -> usize {
    count.min(data.len().saturating_sub(cursor) / min_size)
}

impl RudisTable {
    pub fn new() -> Self {
        Self {
            table: RudisFlatTable::new(64),
            sample_cursor: 0,
            spill_cursor: 0,
            data_bytes: 0,
            arena: crate::allocator::SmallCollectionArena::new(),
            num_expires: 0,
            hash_field_expires: hashbrown::HashMap::new(),
            dropped_tier: smallvec::SmallVec::new(),
        }
    }

    /// Finds an entry for in-place mutation: if the key is Cooled (its data
    /// is in RAM but also has a stale NVMe extent), warms it back to a plain
    /// in-memory [`RudisValue`] in place, charges the freed 24B Cooled
    /// wrapper, and records the stale extent in `dropped_tier` for release.
    #[inline(always)]
    pub fn find_entry_mut_warm(&mut self, key: &[u8], h: u64) -> Option<(usize, &mut RudisEntry)> {
        let (idx, entry) = self.table.find_entry_mut(key, h)?;
        if let Some(ptr) = entry.uncool() {
            self.data_bytes = self.data_bytes.saturating_sub(24);
            self.dropped_tier.push((ptr, true));
        }
        Some((idx, entry))
    }

    /// Like [`Self::find_entry_mut_warm`] for an already-located slot.
    #[inline(always)]
    pub fn get_slot_mut_warm(&mut self, idx: usize) -> Option<&mut RudisEntry> {
        let entry = self.table.get_slot_mut(idx)?;
        if let Some(ptr) = entry.uncool() {
            self.data_bytes = self.data_bytes.saturating_sub(24);
            self.dropped_tier.push((ptr, true));
        }
        Some(entry)
    }

    /// Warms up a slot in place if it is currently Cooled, releasing its 24B
    /// wrapper from memory and queuing its stale NVMe pointer to `dropped_tier`.
    #[inline(always)]
    pub fn warm_slot(&mut self, idx: usize) {
        if let Some(entry) = self.table.get_slot_mut(idx)
            && let Some(ptr) = entry.uncool()
        {
            self.data_bytes = self.data_bytes.saturating_sub(24);
            self.dropped_tier.push((ptr, true));
        }
    }

    #[inline(always)]
    pub fn recycle_value(&mut self, val: RudisValue) {
        match val {
            RudisValue::List(deque) => self.arena.recycle_list(*deque),
            RudisValue::SmallHash(pairs) => self.arena.recycle_small_hash(*pairs),
            RudisValue::Set(s) => {
                if let RudisSet::Small(v) = *s {
                    self.arena.recycle_small_set(v);
                }
            }
            RudisValue::ZSet(z) => {
                if let RudisZSet::Small(v) = *z {
                    self.arena.recycle_small_zset(v);
                }
            }
            _ => {}
        }
    }

    #[inline]
    pub fn insert_entry(&mut self, entry: RudisEntry) {
        self.del(&entry.key);
        if entry.expire_at().is_some() {
            self.num_expires += 1;
        }
        self.table.insert(entry);
    }

    #[inline(always)]
    pub fn is_rehashing(&self) -> bool {
        self.table.is_rehashing()
    }

    #[inline(always)]
    pub fn rehash_step(&mut self, n: usize) -> bool {
        self.table.rehash_step(n)
    }

    #[inline(always)]
    pub fn finish_rehash(&mut self) {
        self.table.finish_rehash();
    }

    #[inline]
    pub fn entries(&self) -> impl Iterator<Item = &RudisEntry> {
        self.table.entries()
    }

    /// See [`RudisFlatTable::layout_epoch`].
    pub fn layout_epoch(&self) -> u64 {
        self.table.layout_epoch()
    }

    pub fn segment_count(&self) -> usize {
        self.table.segment_count()
    }

    pub fn segment_entries(&self, seg: usize) -> impl Iterator<Item = &RudisEntry> {
        self.table.segment_entries(seg)
    }

    #[inline(always)]
    pub fn prepare_key_lookup(&mut self, _key: &[u8], _hash: u64) {}

    /// Memory this shard's keyspace holds: entry data plus the table's own
    /// slot arrays, directory and counters. O(1); this is what `maxmemory`,
    /// eviction and tiering compare against.
    #[inline]
    pub fn used_memory(&self) -> usize {
        self.data_bytes + self.table.struct_bytes()
    }

    pub fn recalculate_used_memory(&mut self) -> usize {
        let mut total = 0;
        for entry in self.entries() {
            total += entry.key.heap_bytes() + entry.val.mem_bytes() + ENTRY_OVERHEAD;
        }
        self.data_bytes = total;
        self.used_memory()
    }

    /// Gives back table structure left over from deleted keys, a bounded
    /// step at a time; see [`RudisFlatTable::shrink_step`]. Collapses to a
    /// single small segment once the keyspace fits in one.
    pub fn shrink_step(&mut self, max_merges: usize) -> usize {
        let merges = self.table.shrink_step(max_merges);
        if self.table.segments.len() > 1 && self.table.len() * 2 <= SEG_CAP {
            self.table.defrag();
        }
        merges
    }

    pub fn active_defrag(&mut self) -> usize {
        let freed = self.table.defrag();
        for entry in self.table.entries_mut() {
            match &mut entry.val {
                RudisValue::String(b) => {
                    if !b.is_empty() {
                        let fresh = CompactStr::new(&b.view());
                        *b = fresh;
                    }
                }
                RudisValue::SmallHash(pairs) => {
                    pairs.shrink_to_fit();
                    for (k, v) in pairs.iter_mut() {
                        *k = Bytes::copy_from_slice(k.as_ref());
                        *v = Bytes::copy_from_slice(v.as_ref());
                    }
                }
                _ => {}
            }
        }
        self.recalculate_used_memory();
        freed
    }

    pub fn is_lazyfree_worthy(&self, key: &[u8]) -> bool {
        let h = hash_key(key);
        if let Some((_, entry)) = self.table.find_entry(key, h) {
            match &entry.val {
                RudisValue::Set(s) => s.len() > 64,
                RudisValue::List(l) => l.len() > 64,
                RudisValue::Hash(h) => h.len() > 64,
                RudisValue::ZSet(z) => z.len() > 64,
                RudisValue::Stream(s) => s.len() > 64 || !s.groups.is_empty(),
                _ => false,
            }
        } else {
            false
        }
    }

    #[inline]
    fn expire_slot(&mut self, slot_idx: usize) {
        if crate::connection::is_client_paused().is_some() {
            return;
        }
        if let Some(removed) = self.table.remove(slot_idx) {
            match &removed.val {
                RudisValue::Tiered(ptr) => self.dropped_tier.push((**ptr, false)),
                RudisValue::Cooled(cv) => self.dropped_tier.push((cv.ptr, true)),
                _ => {}
            }
            if removed.expire_at().is_some() {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
            let freed = removed.key.heap_bytes() + removed.val.mem_bytes() + ENTRY_OVERHEAD;
            self.data_bytes = self.data_bytes.saturating_sub(freed);
            inc_expired_keys();
            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                crate::connection::touch_watched_key_any_port(removed.key.as_ref());
            }
            if crate::connection::HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                crate::connection::notify_key_invalidation_any_port(removed.key.as_ref(), 0);
            }
            crate::connection::notify_keyspace_event(
                crate::connection::NOTIFY_EXPIRED,
                "expired",
                &removed.key,
            );
        }
    }

    #[inline(always)]
    fn check_expired_slot(&mut self, slot_idx: usize) -> bool {
        if self.num_expires == 0 {
            return false;
        }
        if crate::connection::ALLOW_ACCESS_EXPIRED.load(std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        let is_exp = if let Some(entry) = self.table.get_slot(slot_idx) {
            if let Some(expire_at) = entry.expire_at() {
                Instant::now() >= expire_at
            } else {
                false
            }
        } else {
            false
        };

        if is_exp {
            self.expire_slot(slot_idx);
            true
        } else {
            false
        }
    }

    /// Attempts to evict one key under the specified eviction policy (allkeys-lru, volatile-lru, volatile-ttl, allkeys-random).
    /// Returns the number of bytes freed, or None if no evictable key was found.
    pub fn try_evict_one_key(&mut self, policy: &str) -> Option<usize> {
        let bound = self.table.cursor_bound();
        if bound == 0 || self.table.is_empty() {
            return None;
        }

        let policy_lower = policy.to_lowercase();
        let is_volatile = policy_lower.starts_with("volatile");
        // LFU has no frequency counter yet; recency is the closer proxy.
        let by_age = policy_lower.contains("lru") || policy_lower.contains("lfu");
        let now = lru_now();
        let mut max_age: u16 = 0;
        // Nothing is evictable, and the sampling loop below would otherwise
        // walk the entire table before concluding that.
        if is_volatile && self.num_expires == 0 {
            return None;
        }

        // Sample up to 10 occupied slots starting at sample_cursor
        let mut best_slot: Option<usize> = None;
        let mut min_ttl: Option<Instant> = None;
        let mut checked = 0;
        let mut attempts = 0;

        while checked < 10 && attempts < bound {
            let cur = self.sample_cursor % bound;
            self.sample_cursor = (self.sample_cursor + 1) % bound;
            attempts += 1;
            let idx = self.table.cursor_to_global_idx(cur);

            if let Some(entry) = self.table.get_slot(idx) {
                // If volatile policy, key must have an expiration
                if is_volatile && entry.expire_at().is_none() {
                    continue;
                }

                if policy_lower.contains("ttl") {
                    if let Some(exp) = entry.expire_at()
                        && (min_ttl.is_none() || Some(exp) < min_ttl)
                    {
                        min_ttl = Some(exp);
                        best_slot = Some(idx);
                    }
                } else if by_age {
                    // Sampled LRU (as Redis): evict the least recently
                    // accessed of the sampled keys.
                    let age = now.wrapping_sub(self.table.get_slot_last_access(idx));
                    if best_slot.is_none() || age > max_age {
                        max_age = age;
                        best_slot = Some(idx);
                    }
                    checked += 1;
                } else {
                    // Random sampling
                    best_slot = Some(idx);
                    checked += 1;
                }
            }
        }

        if let Some(slot_idx) = best_slot
            && let Some(removed) = self.table.remove(slot_idx)
        {
            match &removed.val {
                RudisValue::Tiered(ptr) => self.dropped_tier.push((**ptr, false)),
                RudisValue::Cooled(cv) => self.dropped_tier.push((cv.ptr, true)),
                _ => {}
            }
            if removed.expire_at().is_some() {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
            let freed = removed.key.heap_bytes() + removed.val.mem_bytes() + ENTRY_OVERHEAD;
            self.data_bytes = self.data_bytes.saturating_sub(freed);
            inc_evicted_keys();
            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                crate::connection::touch_watched_key_any_port(removed.key.as_ref());
            }
            if crate::connection::HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                crate::connection::notify_key_invalidation_any_port(removed.key.as_ref(), 0);
            }
            crate::connection::notify_keyspace_event(
                crate::connection::NOTIFY_EVICTED,
                "evicted",
                &removed.key,
            );
            return Some(freed);
        }

        None
    }

    /// Whether `key` exists and has not expired, without touching it or
    /// deleting it if expired.
    pub fn key_is_live(&self, key: &[u8]) -> bool {
        match self.table.find_entry(key, hash_key(key)) {
            Some((_, entry)) => match entry.expire_at() {
                Some(expire_at) => {
                    crate::connection::ALLOW_ACCESS_EXPIRED
                        .load(std::sync::atomic::Ordering::Relaxed)
                        || Instant::now() < expire_at
                }
                None => true,
            },
            None => false,
        }
    }

    pub fn is_key_expired(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            self.check_expired_slot(idx)
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        self.get_with_hash(key, h)
    }

    #[inline(always)]
    pub fn get_with_hash(&mut self, key: &[u8], h: u64) -> Result<Option<Bytes>, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::server_stats::note_key_lookup(false);
                return Ok(None);
            }
            crate::server_stats::note_key_lookup(true);
            let entry = if entry.val.string_needs_share() {
                if let Some(e) = self.table.get_slot_mut_no_touch(idx) {
                    e.val.share_string();
                }
                match self.table.get_slot(idx) {
                    Some(e) => e,
                    None => return Ok(None),
                }
            } else {
                entry
            };
            let val_ref = match &entry.val {
                RudisValue::Cooled(cv) => &cv.val,
                other => other,
            };
            let res = match val_ref {
                RudisValue::String(b) => Ok(Some(b.to_bytes())),
                RudisValue::Int(n) => Ok(Some(Self::format_i64(*n))),
                RudisValue::HyperLogLog(regs) => Ok(Some(Bytes::copy_from_slice(&regs[..]))),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            };
            if !crate::connection::CLIENT_NO_TOUCH.load(std::sync::atomic::Ordering::Relaxed) {
                self.table.touch_slot(idx);
            }
            res
        } else {
            crate::server_stats::note_key_lookup(false);
            Ok(None)
        }
    }

    #[inline(always)]
    pub fn get_compact(
        &mut self,
        key: &[u8],
    ) -> Result<Option<crate::shard::CompactResp>, &'static str> {
        let h = hash_key(key);
        self.get_compact_with_hash(key, h)
    }

    #[inline(always)]
    pub fn get_compact_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
    ) -> Result<Option<crate::shard::CompactResp>, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::server_stats::note_key_lookup(false);
                return Ok(None);
            }
            crate::server_stats::note_key_lookup(true);
            let entry = if entry.val.string_needs_share() {
                if let Some(e) = self.table.get_slot_mut_no_touch(idx) {
                    e.val.share_string();
                }
                match self.table.get_slot(idx) {
                    Some(e) => e,
                    None => return Ok(None),
                }
            } else {
                entry
            };
            let val_ref = match &entry.val {
                RudisValue::Cooled(cv) => &cv.val,
                other => other,
            };
            let res = match val_ref {
                RudisValue::String(b) => Ok(Some(crate::shard::CompactResp::from_compact_str(b))),
                RudisValue::Int(n) => {
                    let formatted = Self::format_i64(*n);
                    Ok(Some(crate::shard::CompactResp::from_bulk(&formatted)))
                }
                RudisValue::HyperLogLog(regs) => Ok(Some(crate::shard::CompactResp::from_bulk(
                    &Bytes::copy_from_slice(&regs[..]),
                ))),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            };
            if !crate::connection::CLIENT_NO_TOUCH.load(std::sync::atomic::Ordering::Relaxed) {
                self.table.touch_slot(idx);
            }
            res
        } else {
            crate::server_stats::note_key_lookup(false);
            Ok(None)
        }
    }

    #[inline(always)]
    pub fn write_get_resp(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::server_stats::note_key_lookup(false);
                crate::connection::write_resp_null(out);
                return Ok(true);
            }
            crate::server_stats::note_key_lookup(true);
            let val_ref = match &entry.val {
                RudisValue::Cooled(cv) => &cv.val,
                other => other,
            };
            let res = match val_ref {
                RudisValue::String(b) => {
                    crate::connection::write_resp_bulk(out, &b.view());
                    Ok(true)
                }
                RudisValue::Int(n) => {
                    let formatted = Self::format_i64(*n);
                    crate::connection::write_resp_bulk(out, &formatted);
                    Ok(true)
                }
                RudisValue::HyperLogLog(regs) => {
                    crate::connection::write_resp_bulk(out, &regs[..]);
                    Ok(true)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            };
            if !crate::connection::CLIENT_NO_TOUCH.load(std::sync::atomic::Ordering::Relaxed) {
                self.table.touch_slot(idx);
            }
            res
        } else {
            crate::server_stats::note_key_lookup(false);
            Ok(false)
        }
    }

    pub fn get_entry(&mut self, key: &[u8]) -> Option<(RudisValue, Option<Duration>)> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return None;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let ttl = entry.expire_at().and_then(|exp| {
                    let now = Instant::now();
                    if exp > now {
                        Some(exp.duration_since(now))
                    } else {
                        None
                    }
                });
                let val = match &entry.val {
                    RudisValue::Cooled(cv) => cv.val.clone(),
                    other => other.clone(),
                };
                return Some((val, ttl));
            }
        }
        None
    }

    #[inline(always)]
    pub fn format_i64(n: i64) -> Bytes {
        match n {
            0 => Bytes::from_static(b"0"),
            1 => Bytes::from_static(b"1"),
            2 => Bytes::from_static(b"2"),
            3 => Bytes::from_static(b"3"),
            4 => Bytes::from_static(b"4"),
            5 => Bytes::from_static(b"5"),
            6 => Bytes::from_static(b"6"),
            7 => Bytes::from_static(b"7"),
            8 => Bytes::from_static(b"8"),
            9 => Bytes::from_static(b"9"),
            10 => Bytes::from_static(b"10"),
            -1 => Bytes::from_static(b"-1"),
            _ => {
                let mut buf = [0u8; 24];
                let mut i = buf.len();
                let val = n;
                let neg = val < 0;
                let mut uval = if neg {
                    if val == i64::MIN {
                        return Bytes::from_static(b"-9223372036854775808");
                    }
                    (-val) as u64
                } else {
                    val as u64
                };
                while uval > 0 {
                    i -= 1;
                    buf[i] = b'0' + (uval % 10) as u8;
                    uval /= 10;
                }
                if neg {
                    i -= 1;
                    buf[i] = b'-';
                }
                Bytes::copy_from_slice(&buf[i..])
            }
        }
    }

    #[inline(always)]
    pub fn parse_i64_bytes(bytes: &[u8]) -> Option<i64> {
        if bytes.is_empty() || bytes.len() > 20 {
            return None;
        }
        let (neg, s) = match bytes[0] {
            b'-' => (true, &bytes[1..]),
            b'+' => return None,
            _ => (false, bytes),
        };
        if s.is_empty() {
            return None;
        }
        if s.len() > 1 && s[0] == b'0' {
            return None;
        }
        let mut val: u64 = 0;
        for &b in s {
            if !b.is_ascii_digit() {
                return None;
            }
            val = val.checked_mul(10)?.checked_add((b - b'0') as u64)?;
        }
        if neg {
            if val == 0 {
                return None;
            }
            if val > (i64::MIN.unsigned_abs()) {
                return None;
            }
            if val == i64::MIN.unsigned_abs() {
                Some(i64::MIN)
            } else {
                Some(-(val as i64))
            }
        } else {
            if val > (i64::MAX as u64) {
                return None;
            }
            Some(val as i64)
        }
    }

    #[inline(always)]
    pub fn set(&mut self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        let h = hash_key(&key);
        self.set_extended_with_hash(key, h, value, expire_in, false);
    }

    #[inline(always)]
    pub fn set_with_hash(
        &mut self,
        key: Bytes,
        h: u64,
        value: Bytes,
        expire_in: Option<Duration>,
    ) -> Option<(TieredPointer, bool)> {
        self.set_extended_with_hash(key, h, value, expire_in, false)
    }

    pub fn set_extended(
        &mut self,
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        keepttl: bool,
    ) {
        let h = hash_key(&key);
        let _ = self.set_extended_with_hash(key, h, value, expire_in, keepttl);
    }

    #[inline(always)]
    pub fn set_extended_with_hash(
        &mut self,
        key: Bytes,
        h: u64,
        value: Bytes,
        expire_in: Option<Duration>,
        keepttl: bool,
    ) -> Option<(TieredPointer, bool)> {
        let val = if let Some(int_val) = Self::parse_i64_bytes(&value) {
            RudisValue::Int(int_val)
        } else {
            // Own, exact-size copy: the parsed value is a slice of the command
            // frame (plus a shared refcount header), which would otherwise stay
            // allocated as long as the key lives. Measured: -21% memory per
            // small key, no SET throughput cost.
            RudisValue::String(CompactStr::new(&value))
        };
        let val_bytes = val.mem_bytes();
        let (existing, candidate_idx) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && let Some(entry) = self.table.get_slot_mut(idx)
        {
            let old_bytes = entry.val.mem_bytes();
            let old_tiered = match &entry.val {
                RudisValue::Tiered(ptr) => Some((**ptr, false)),
                RudisValue::Cooled(cv) => Some((cv.ptr, true)),
                _ => None,
            };
            entry.val = val;
            let old_key = entry.key.heap_bytes();
            if !keepttl {
                let had_exp = entry.expire_at().is_some();
                let will_exp = expire_in.is_some();
                if had_exp && !will_exp {
                    self.num_expires = self.num_expires.saturating_sub(1);
                } else if !had_exp && will_exp {
                    self.num_expires += 1;
                }
                entry.set_expire_at(expire_in.map(|d| Instant::now() + d));
            }
            self.data_bytes = self.data_bytes.saturating_sub(old_bytes + old_key)
                + val_bytes
                + entry.key.heap_bytes();
            if let Some((ptr, is_cooled)) = old_tiered {
                self.dropped_tier.push((ptr, is_cooled));
            }
            return old_tiered;
        }

        let expire_at = if keepttl {
            None
        } else {
            expire_in.map(|d| Instant::now() + d)
        };
        if expire_at.is_some() {
            self.num_expires += 1;
        }
        let entry = {
            // Same as the value above: don't keep the frame alive through the key.
            RudisEntry::new(CompactKey::new(&key), val, Expiry::from(expire_at))
        };
        self.data_bytes += entry.key.heap_bytes() + val_bytes + ENTRY_OVERHEAD;
        self.table.insert_prepared(entry, h, candidate_idx);
        None
    }

    #[inline(always)]
    pub fn del(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        self.del_with_hash(key, h)
    }

    #[inline(always)]
    pub fn del_with_hash(&mut self, key: &[u8], hash: u64) -> bool {
        if self.table.items == 0 {
            return false;
        }
        if !self.hash_field_expires.is_empty() {
            self.hash_field_expires.remove(key);
        }
        if self.num_expires == 0 {
            if let Some((idx, entry)) = self.table.find_entry(key, hash) {
                let val_bytes = entry.val.mem_bytes();
                let freed = entry.key.heap_bytes() + val_bytes + ENTRY_OVERHEAD;
                self.data_bytes = self.data_bytes.saturating_sub(freed);
                let entry = self.table.remove_present(idx);
                match entry.val {
                    RudisValue::Tiered(ptr) => self.dropped_tier.push((*ptr, false)),
                    RudisValue::Cooled(cv) => self.dropped_tier.push((cv.ptr, true)),
                    RudisValue::String(_) | RudisValue::Int(_) => {}
                    other => self.recycle_value(other),
                }
                return true;
            }
            return false;
        }
        if let Some((idx, entry)) = self.table.find_entry(key, hash) {
            if let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return false;
            }
            let val_bytes = entry.val.mem_bytes();
            let freed = entry.key.heap_bytes() + val_bytes + ENTRY_OVERHEAD;
            self.data_bytes = self.data_bytes.saturating_sub(freed);
            let entry = self.table.remove_present(idx);
            if entry.expire_at().is_some() {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
            match entry.val {
                RudisValue::Tiered(ptr) => self.dropped_tier.push((*ptr, false)),
                RudisValue::Cooled(cv) => self.dropped_tier.push((cv.ptr, true)),
                RudisValue::String(_) | RudisValue::Int(_) => {}
                other => self.recycle_value(other),
            }
            return true;
        }
        false
    }

    #[inline(always)]
    pub fn exists(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        self.exists_with_hash(key, h)
    }

    #[inline(always)]
    pub fn exists_with_hash(&mut self, key: &[u8], hash: u64) -> bool {
        if self.num_expires == 0 {
            return self.table.contains(key, hash);
        }
        if let Some((idx, entry)) = self.table.find_entry(key, hash) {
            if let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return false;
            }
            return true;
        }
        false
    }

    #[inline(always)]
    pub fn incr_by_slice_fast(&mut self, key: &Bytes, delta: i64) -> Result<i64, &'static str> {
        let h = hash_key(key);
        self.incr_by_slice_internal(key.as_ref(), h, Some(key), delta)
    }

    #[inline(always)]
    pub fn incr_by_slice_with_hash(
        &mut self,
        key: &Bytes,
        h: u64,
        delta: i64,
    ) -> Result<i64, &'static str> {
        self.incr_by_slice_internal(key.as_ref(), h, Some(key), delta)
    }

    pub fn incr_by_slice(&mut self, key: &[u8], delta: i64) -> Result<i64, &'static str> {
        let h = hash_key(key);
        self.incr_by_slice_internal(key, h, None, delta)
    }

    #[inline(always)]
    fn incr_by_slice_internal(
        &mut self,
        key: &[u8],
        h: u64,
        _key_bytes: Option<&Bytes>, // unused: keys are copied into a CompactKey
        delta: i64,
    ) -> Result<i64, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                if let Some(ptr) = entry.uncool() {
                    self.data_bytes = self.data_bytes.saturating_sub(24);
                    self.dropped_tier.push((ptr, true));
                }
                match &mut entry.val {
                    RudisValue::Int(n) => {
                        let nv = n
                            .checked_add(delta)
                            .ok_or("increment or decrement would overflow")?;
                        *n = nv;
                        return Ok(nv);
                    }
                    RudisValue::String(b) => {
                        let current = Self::parse_i64_bytes(&b.view())
                            .ok_or("value is not an integer or out of range")?;
                        let nv = current
                            .checked_add(delta)
                            .ok_or("increment or decrement would overflow")?;
                        entry.val = RudisValue::Int(nv);
                        return Ok(nv);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let (_, candidate_idx) = self.table.find_or_prepare_insert(key, h);
        let new_val = delta;
        let entry = RudisEntry::new(
            CompactKey::new(key),
            RudisValue::Int(new_val),
            Expiry::from(None),
        );
        self.table.insert_prepared(entry, h, candidate_idx);
        self.data_bytes += heap_len(key.len()) + ENTRY_OVERHEAD;
        Ok(new_val)
    }

    #[inline]
    pub fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<i64, String> {
        self.incr_by_slice_fast(&key, delta)
            .map_err(|e| e.to_string())
    }

    pub fn expire(
        &mut self,
        key: &[u8],
        duration: Duration,
        opts: crate::resp::ExpireOptions,
    ) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return false;
            }
            let target_instant = Instant::now() + duration;
            if let Some(entry) = self.table.get_slot_mut(idx) {
                if opts.nx && entry.expire_at().is_some() {
                    return false;
                }
                if opts.xx && entry.expire_at().is_none() {
                    return false;
                }
                if opts.gt {
                    if entry.expire_at().is_none() {
                        return false;
                    }
                    if let Some(cur_exp) = entry.expire_at()
                        && target_instant <= cur_exp
                    {
                        return false;
                    }
                }
                if opts.lt
                    && let Some(cur_exp) = entry.expire_at()
                    && target_instant >= cur_exp
                {
                    return false;
                }
                if duration.is_zero() {
                    self.expire_slot(idx);
                    return true;
                }
                if entry.expire_at().is_none() {
                    self.num_expires += 1;
                }
                let old_key = entry.key.heap_bytes();
                entry.set_expire_at(Some(target_instant));
                self.data_bytes = self.data_bytes.saturating_sub(old_key) + entry.key.heap_bytes();
                return true;
            }
        }
        false
    }

    pub fn persist(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return false;
            }
            if let Some(entry) = self.table.get_slot_mut(idx)
                && entry.expire_at().is_some()
            {
                let old_key = entry.key.heap_bytes();
                entry.set_expire_at(None);
                self.data_bytes = self.data_bytes.saturating_sub(old_key) + entry.key.heap_bytes();
                self.num_expires = self.num_expires.saturating_sub(1);
                return true;
            }
        }
        false
    }

    pub fn ttl(&mut self, key: &[u8], in_millis: bool) -> i64 {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return -2;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match entry.expire_at() {
                    Some(expire_at) => {
                        let now = Instant::now();
                        if now >= expire_at {
                            self.check_expired_slot(idx);
                            -2
                        } else {
                            let diff = expire_at.duration_since(now);
                            if in_millis {
                                diff.as_millis() as i64
                            } else {
                                ((diff.as_millis() as i64) + 500) / 1000
                            }
                        }
                    }
                    None => -1,
                }
            } else {
                -2
            }
        } else {
            -2
        }
    }

    pub fn expiretime(&mut self, key: &[u8], in_millis: bool) -> i64 {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return -2;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match entry.expire_at() {
                    Some(expire_at) => {
                        let now = Instant::now();
                        if now >= expire_at {
                            self.check_expired_slot(idx);
                            -2
                        } else {
                            let diff = expire_at.duration_since(now);
                            let now_epoch = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or(Duration::ZERO);
                            let target_epoch = now_epoch + diff;
                            // Nearest millisecond: the round trip through two
                            // clock reads can land a few µs either side of an
                            // absolute PXAT/PEXPIREAT time.
                            let ms = ((target_epoch.as_nanos() + 500_000) / 1_000_000) as i64;
                            if in_millis { ms } else { ms / 1000 }
                        }
                    }
                    None => -1,
                }
            } else {
                -2
            }
        } else {
            -2
        }
    }

    pub fn keys(&mut self, pattern: &[u8]) -> Vec<Bytes> {
        let mut res = Vec::new();
        // Walk cursor positions, not `0..capacity()`: global slot indices
        // are `seg << GLOBAL_IDX_SHIFT | local`, so a dense range covers
        // only part of a multi-segment table.
        for cur in 0..self.table.cursor_bound() {
            let i = self.table.cursor_to_global_idx(cur);
            if self.check_expired_slot(i) {
                continue;
            }
            if let Some(entry) = self.table.get_slot(i)
                && crate::pubsub::glob_match(pattern, &entry.key)
            {
                res.push(entry.key.to_bytes());
            }
        }
        res
    }

    pub fn scan(
        &mut self,
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
        key_type: Option<&[u8]>,
    ) -> (usize, Vec<Bytes>) {
        let bound = self.table.cursor_bound();
        if bound == 0 {
            return (0, Vec::new());
        }
        if self.table.len() <= 8192 {
            let mut candidates: Vec<(usize, usize)> = Vec::with_capacity(self.table.len());
            for cur in 0..bound {
                let idx = self.table.cursor_to_global_idx(cur);
                if !self.check_expired_slot(idx)
                    && let Some(entry) = self.table.get_slot(idx)
                {
                    let eh = ((xxhash_rust::xxh3::xxh3_64(&entry.key) as u32) | 1) as usize;
                    if cursor == 0 || eh >= cursor {
                        candidates.push((eh, idx));
                    }
                }
            }
            candidates.sort_unstable_by_key(|(eh, _)| *eh);
            let mut res = Vec::new();
            let mut next_cursor = 0usize;
            let mut inspected = 0usize;
            for (i, &(eh, idx)) in candidates.iter().enumerate() {
                if let Some(entry) = self.table.get_slot(idx) {
                    let matches = match pattern {
                        Some(pat) => crate::pubsub::glob_match(pat, &entry.key),
                        None => true,
                    };
                    let type_matches = match key_type {
                        Some(t) => {
                            let actual_type = match &entry.val {
                                RudisValue::String(_) | RudisValue::Int(_) => "string",
                                RudisValue::Hash(_) | RudisValue::SmallHash(_) => "hash",
                                RudisValue::List(_) => "list",
                                RudisValue::Set(_) => "set",
                                RudisValue::ZSet(_) => "zset",
                                RudisValue::HyperLogLog(_) => "string",
                                RudisValue::Stream(_) => "stream",
                                RudisValue::Tiered(ptr) => match ptr.value_type {
                                    0 => "string",
                                    1 => "list",
                                    2 => "set",
                                    3 => "zset",
                                    4 => "hash",
                                    5 => "string",
                                    6 => "stream",
                                    _ => "none",
                                },
                                RudisValue::Cooled(cv) => match &cv.val {
                                    RudisValue::String(_) | RudisValue::Int(_) => "string",
                                    RudisValue::Hash(_) | RudisValue::SmallHash(_) => "hash",
                                    RudisValue::List(_) => "list",
                                    RudisValue::Set(_) => "set",
                                    RudisValue::ZSet(_) => "zset",
                                    RudisValue::HyperLogLog(_) => "string",
                                    RudisValue::Stream(_) => "stream",
                                    _ => "none",
                                },
                            };
                            actual_type.as_bytes().eq_ignore_ascii_case(t)
                        }
                        None => true,
                    };
                    if matches && type_matches {
                        res.push(entry.key.to_bytes());
                    }
                }
                inspected += 1;
                if inspected >= count {
                    for next_cand in &candidates[i + 1..] {
                        if next_cand.0 > eh {
                            next_cursor = next_cand.0;
                            break;
                        }
                    }
                    break;
                }
            }
            return (next_cursor, res);
        }
        if cursor >= bound {
            return (0, Vec::new());
        }
        let mut res = Vec::new();
        let mut cur = cursor;
        while cur < bound {
            let idx = self.table.cursor_to_global_idx(cur);
            if !self.check_expired_slot(idx)
                && let Some(entry) = self.table.get_slot(idx)
            {
                let matches = match pattern {
                    Some(pat) => crate::pubsub::glob_match(pat, &entry.key),
                    None => true,
                };
                let type_matches = match key_type {
                    Some(t) => {
                        let actual_type = match &entry.val {
                            RudisValue::String(_) | RudisValue::Int(_) => "string",
                            RudisValue::Hash(_) | RudisValue::SmallHash(_) => "hash",
                            RudisValue::List(_) => "list",
                            RudisValue::Set(_) => "set",
                            RudisValue::ZSet(_) => "zset",
                            RudisValue::HyperLogLog(_) => "string",
                            RudisValue::Stream(_) => "stream",
                            RudisValue::Tiered(ptr) => match ptr.value_type {
                                0 => "string",
                                1 => "list",
                                2 => "set",
                                3 => "zset",
                                4 => "hash",
                                5 => "string",
                                6 => "stream",
                                _ => "none",
                            },
                            RudisValue::Cooled(cv) => match &cv.val {
                                RudisValue::String(_) | RudisValue::Int(_) => "string",
                                RudisValue::Hash(_) | RudisValue::SmallHash(_) => "hash",
                                RudisValue::List(_) => "list",
                                RudisValue::Set(_) => "set",
                                RudisValue::ZSet(_) => "zset",
                                RudisValue::HyperLogLog(_) => "string",
                                RudisValue::Stream(_) => "stream",
                                _ => "none",
                            },
                        };
                        actual_type.as_bytes().eq_ignore_ascii_case(t)
                    }
                    None => true,
                };
                if matches && type_matches {
                    res.push(entry.key.to_bytes());
                }
            }
            cur += 1;
            if res.len() >= count {
                break;
            }
        }
        let next_cursor = if cur >= bound { 0 } else { cur };
        (next_cursor, res)
    }

    pub fn random_key(&mut self) -> Option<Bytes> {
        let bound = self.table.cursor_bound();
        if self.table.is_empty() || bound == 0 {
            return None;
        }
        let start = self.next_rand() % bound;
        for i in 0..bound {
            let cur = (start + i) % bound;
            let idx = self.table.cursor_to_global_idx(cur);
            if crate::connection::is_client_paused().is_none() && self.check_expired_slot(idx) {
                continue;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                return Some(entry.key.to_bytes());
            }
        }
        None
    }

    #[inline(always)]
    pub fn next_rand(&mut self) -> usize {
        self.sample_cursor = self.sample_cursor.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.sample_cursor as u64;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        (z ^ (z >> 31)) as usize
    }

    pub fn flushdb(&mut self) {
        self.table.clear();
        self.hash_field_expires.clear();
        self.num_expires = 0;
        self.data_bytes = 0;
    }

    pub fn dbsize(&mut self) -> usize {
        self.table.len()
    }

    pub fn type_of(&mut self, key: &[u8]) -> &'static str {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return "none";
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let val_ref = match &entry.val {
                    RudisValue::Cooled(cv) => &cv.val,
                    other => other,
                };
                match val_ref {
                    RudisValue::String(_) | RudisValue::Int(_) => "string",
                    RudisValue::Hash(_) | RudisValue::SmallHash(_) => "hash",
                    RudisValue::List(_) => "list",
                    RudisValue::Set(_) => "set",
                    RudisValue::ZSet(_) => "zset",
                    RudisValue::HyperLogLog(_) => "string",
                    RudisValue::Stream(_) => "stream",
                    RudisValue::Tiered(ptr) => match ptr.value_type {
                        0 => "string",
                        1 => "list",
                        2 => "set",
                        3 => "zset",
                        4 => "hash",
                        5 => "string",
                        6 => "stream",
                        _ => "string",
                    },
                    RudisValue::Cooled(_) => unreachable!(),
                }
            } else {
                "none"
            }
        } else {
            "none"
        }
    }

    #[inline]
    pub fn is_tiered(&mut self, key: &[u8]) -> Option<TieredPointer> {
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        if self.check_expired_slot(idx) {
            return None;
        }
        let entry = self.table.get_slot(idx)?;
        if let RudisValue::Tiered(ptr) = &entry.val {
            Some(**ptr)
        } else {
            None
        }
    }

    /// Turns a cooled `key` back into a plain in-memory value (dropping its
    /// disk copy). Returns whether it was cooled.
    pub fn warm_key(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        match self.table.find(key, h) {
            Some(idx)
                if matches!(
                    self.table.get_slot(idx).map(|e| &e.val),
                    Some(RudisValue::Cooled(_))
                ) =>
            {
                self.warm_slot(idx);
                true
            }
            _ => false,
        }
    }

    #[inline]
    pub fn is_cooled(&mut self, key: &[u8]) -> Option<TieredPointer> {
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        if self.check_expired_slot(idx) {
            return None;
        }
        let entry = self.table.get_slot(idx)?;
        if let RudisValue::Cooled(cv) = &entry.val {
            Some(cv.ptr)
        } else {
            None
        }
    }

    #[inline]
    pub fn set_tiered_pointer(&mut self, key: &[u8], ptr: TieredPointer) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && let Some(entry) = self.table.get_slot_mut(idx)
        {
            let old_bytes = entry.val.mem_bytes();
            entry.val = RudisValue::Tiered(Box::new(ptr));
            let new_bytes = entry.val.mem_bytes();
            self.data_bytes = self.data_bytes.saturating_sub(old_bytes) + new_bytes;
            return true;
        }
        false
    }

    /// Swaps in a spill's pointer only if the key still holds the value that
    /// was stashed (`payload`, from `get_value_for_spill`): a write, delete
    /// or expiry during the stash's I/O wins over the stale copy.
    pub fn set_spilled_pointer_if_unchanged(
        &mut self,
        key: &[u8],
        payload: &[u8],
        ptr: TieredPointer,
        cooled: bool,
    ) -> bool {
        if self
            .get_value_for_spill(key)
            .is_none_or(|(current, _)| current != payload)
        {
            return false;
        }
        if cooled {
            self.set_cooled_pointer(key, ptr)
        } else {
            self.set_tiered_pointer(key, ptr)
        }
    }

    #[inline]
    pub fn set_cooled_pointer(&mut self, key: &[u8], ptr: TieredPointer) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && let Some(entry) = self.table.get_slot_mut(idx)
            && !matches!(entry.val, RudisValue::Tiered(_) | RudisValue::Cooled(_))
        {
            let old_val = std::mem::replace(&mut entry.val, RudisValue::Tiered(Box::new(ptr)));
            entry.val = RudisValue::Cooled(Box::new(CooledValue { ptr, val: old_val }));
            self.data_bytes += 24;
            return true;
        }
        false
    }

    #[inline]
    pub fn restore_tiered_value(&mut self, key: &[u8], val: RudisValue) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && let Some(entry) = self.table.get_slot_mut(idx)
            && let RudisValue::Tiered(ptr) = &entry.val
        {
            let val_bytes = val.mem_bytes();
            entry.val = RudisValue::Cooled(Box::new(CooledValue { ptr: **ptr, val }));
            self.data_bytes += val_bytes;
            return true;
        }
        false
    }

    #[inline]
    pub fn decommit_cooled_key(&mut self, key: &[u8]) -> Option<(TieredPointer, usize)> {
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        let entry = self.table.get_slot_mut(idx)?;
        if let RudisValue::Cooled(cv) = &entry.val {
            let p = cv.ptr;
            let val = &cv.val;
            let freed = val.approx_bytes();
            let freed_mem = val.mem_bytes();
            entry.val = RudisValue::Tiered(Box::new(p));
            self.data_bytes = self.data_bytes.saturating_sub(freed_mem);
            Some((p, freed))
        } else {
            None
        }
    }

    pub fn decommit_all_cooled(&mut self) -> (usize, u64) {
        let mut count = 0;
        let mut total_freed = 0u64;
        for entry in self.table.entries_mut() {
            if let RudisValue::Cooled(cv) = &entry.val {
                let p = cv.ptr;
                let val = &cv.val;
                let freed = val.approx_bytes() as u64;
                let freed_mem = val.mem_bytes();
                entry.val = RudisValue::Tiered(Box::new(p));
                self.data_bytes = self.data_bytes.saturating_sub(freed_mem);
                total_freed += freed;
                count += 1;
            }
        }
        (count, total_freed)
    }

    pub fn get_hot_keys_for_spill(&mut self, limit: usize) -> Vec<Bytes> {
        let mut hot = Vec::with_capacity(limit);
        let total_slots = self.table.cursor_bound();
        if total_slots == 0 {
            return hot;
        }
        let start = self.spill_cursor % total_slots;
        let mut cur = start;
        // Bounded scan: callers loop and the cursor persists, so a sparse
        // table is covered over several calls rather than in one walk.
        for _ in 0..total_slots.min(limit.saturating_mul(64).max(4096)) {
            let idx = self.table.cursor_to_global_idx(cur);
            if let Some(entry) = self.table.get_slot(idx)
                && !matches!(entry.val, RudisValue::Tiered(_) | RudisValue::Cooled(_))
                // Spilling a value smaller than this grows memory: the tier
                // pointer left behind outweighs it.
                && entry.val.approx_bytes() >= MIN_SPILL_VALUE_BYTES
            {
                hot.push(entry.key.to_bytes());
                if hot.len() >= limit {
                    self.spill_cursor = (cur + 1) % total_slots;
                    return hot;
                }
            }
            cur = (cur + 1) % total_slots;
        }
        self.spill_cursor = cur;
        hot
    }

    #[inline]
    pub fn get_value_for_spill(&mut self, key: &[u8]) -> Option<(Vec<u8>, u8)> {
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        if self.check_expired_slot(idx) {
            return None;
        }
        let entry = self.table.get_slot(idx)?;
        let val_ref = match &entry.val {
            RudisValue::Tiered(_) | RudisValue::Cooled(_) => return None,
            other => other,
        };
        let mut payload = Vec::new();
        Self::serialize_val_payload(val_ref, &mut payload);
        let val_type = match val_ref {
            RudisValue::String(_) | RudisValue::Int(_) => 0u8,
            RudisValue::List(_) => 1u8,
            RudisValue::Set(_) => 2u8,
            RudisValue::ZSet(_) => 3u8,
            RudisValue::SmallHash(_) | RudisValue::Hash(_) => 4u8,
            RudisValue::HyperLogLog(_) => 5u8,
            RudisValue::Stream(_) => 6u8,
            _ => unreachable!(),
        };
        Some((payload, val_type))
    }

    pub fn touch(&mut self, keys: &[Bytes]) -> usize {
        let mut count = 0;
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h)
                && !self.check_expired_slot(idx)
            {
                self.table.touch_slot(idx);
                count += 1;
            }
        }
        count
    }

    pub fn idletime(&mut self, key: &[u8]) -> Option<u64> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && !self.check_expired_slot(idx)
        {
            let last = self.table.get_slot_last_access(idx);
            Some(lru_now().wrapping_sub(last) as u64)
        } else {
            None
        }
    }

    pub fn lru_and_idletime(&mut self, key: &[u8]) -> Option<(u32, u64)> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && !self.check_expired_slot(idx)
        {
            let last = self.table.get_slot_last_access(idx);
            Some((last as u32, lru_now().wrapping_sub(last) as u64))
        } else {
            None
        }
    }

    pub fn rename(&mut self, src: &[u8], dst: Bytes, nx: bool) -> Result<bool, &'static str> {
        if src == dst.as_ref() {
            let h = hash_key(src);
            if let Some(idx) = self.table.find(src, h)
                && !self.check_expired_slot(idx)
            {
                return if nx { Ok(false) } else { Ok(true) };
            }
            return Err("no such key");
        }

        let h_src = hash_key(src);
        let src_idx = match self.table.find(src, h_src) {
            Some(idx) => {
                if self.check_expired_slot(idx) {
                    return Err("no such key");
                }
                idx
            }
            None => return Err("no such key"),
        };

        if nx {
            let h_dst = hash_key(&dst);
            if let Some(dst_idx) = self.table.find(&dst, h_dst)
                && !self.check_expired_slot(dst_idx)
            {
                return Ok(false);
            }
        }

        // Remove src
        let mut entry = self.table.remove(src_idx).unwrap();

        // If dst exists, remove it first (releasing its accounting).
        let h_dst = hash_key(&dst);
        if let Some(dst_idx) = self.table.find(&dst, h_dst)
            && let Some(old) = self.table.remove(dst_idx)
        {
            match &old.val {
                RudisValue::Tiered(ptr) => self.dropped_tier.push((**ptr, false)),
                RudisValue::Cooled(cv) => self.dropped_tier.push((cv.ptr, true)),
                _ => {}
            }
            if old.expire_at().is_some() {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
            let freed = old.key.heap_bytes() + old.val.mem_bytes() + ENTRY_OVERHEAD;
            self.data_bytes = self.data_bytes.saturating_sub(freed);
            self.recycle_value(old.val);
        }

        // Update entry key to dst and insert. The TTL lives in the key, so
        // carry it over; the new key may differ in heap size.
        let old_key = entry.key.heap_bytes();
        entry.key = CompactKey::with_ttl(&dst, entry.key.ttl());
        self.data_bytes = self.data_bytes.saturating_sub(old_key) + entry.key.heap_bytes();
        self.table.insert(entry);

        Ok(true)
    }

    pub fn copy(&mut self, src: &[u8], dst: Bytes, replace: bool) -> Result<bool, &'static str> {
        self.copy_with_hydrated(src, dst, replace, None)
    }

    pub fn copy_with_hydrated(
        &mut self,
        src: &[u8],
        dst: Bytes,
        replace: bool,
        hydrated: Option<RudisValue>,
    ) -> Result<bool, &'static str> {
        if src == dst.as_ref() {
            return Err("source and destination objects are the same");
        }

        let h_src = hash_key(src);
        if let Some(src_idx) = self.table.find(src, h_src) {
            if self.check_expired_slot(src_idx) {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }

        let (_, entry) = self.table.find_entry(src, h_src).unwrap();
        // Never clone a Tiered/Cooled pointer across two keys: deleting both
        // would free the same disk extent twice.
        let val = match &entry.val {
            RudisValue::Tiered(_) => match hydrated {
                Some(v) => v,
                None => return Ok(false),
            },
            RudisValue::Cooled(cv) => cv.val.clone(),
            other => other.clone(),
        };
        let expire_at = entry.expire_at();

        let h_dst = hash_key(&dst);
        if let Some(dst_idx) = self.table.find(&dst, h_dst) {
            if self.check_expired_slot(dst_idx) {
                // Was expired and removed
            } else if !replace {
                return Ok(false);
            } else if let Some(removed) = self.table.remove(dst_idx)
                && removed.expire_at().is_some()
            {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
        }

        if expire_at.is_some() {
            self.num_expires += 1;
        }
        let entry = RudisEntry::new(CompactKey::new(&dst), val, Expiry::from(expire_at));
        self.table.insert(entry);
        Ok(true)
    }

    pub fn setnx(&mut self, key: Bytes, value: Bytes) -> bool {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h)
            && !self.check_expired_slot(idx)
        {
            return false;
        }
        self.set(key, value, None);
        true
    }

    pub fn getset(&mut self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                self.set(key, value, None);
                return Ok(None);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::String(old) => {
                        let prev = old.to_bytes();
                        let old_val = old.heap_bytes();
                        *old = value.into();
                        let new_val = old.heap_bytes();
                        let old_key = entry.key.heap_bytes();
                        let had_exp = entry.take_expire_at().is_some();
                        let new_key = entry.key.heap_bytes();
                        if had_exp {
                            self.num_expires = self.num_expires.saturating_sub(1);
                        }
                        self.data_bytes =
                            self.data_bytes.saturating_sub(old_val + old_key) + new_val + new_key;
                        Ok(Some(prev))
                    }
                    RudisValue::Int(n) => {
                        let prev = Self::format_i64(*n);
                        if let Some(int_val) = Self::parse_i64_bytes(&value) {
                            entry.val = RudisValue::Int(int_val);
                        } else {
                            entry.val = RudisValue::String(value.into());
                        }
                        let new_val = entry.val.mem_bytes();
                        let old_key = entry.key.heap_bytes();
                        let had_exp = entry.take_expire_at().is_some();
                        let new_key = entry.key.heap_bytes();
                        if had_exp {
                            self.num_expires = self.num_expires.saturating_sub(1);
                        }
                        self.data_bytes =
                            self.data_bytes.saturating_sub(old_key) + new_val + new_key;
                        Ok(Some(prev))
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            self.set(key, value, None);
            Ok(None)
        }
    }

    pub fn getdel(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                let val = match &entry.val {
                    RudisValue::String(s) => s.to_bytes(),
                    RudisValue::Int(n) => Self::format_i64(*n),
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                };
                self.del_with_hash(key, h);
                Ok(Some(val))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn append(&mut self, key: Bytes, val_to_append: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                let new_val = Bytes::copy_from_slice(val_to_append);
                let len = new_val.len();
                self.set(key, new_val, None);
                return Ok(len);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::String(s) => {
                        let mut combined = Vec::with_capacity(s.len() + val_to_append.len());
                        combined.extend_from_slice(&s.view());
                        combined.extend_from_slice(val_to_append);
                        let len = combined.len();
                        *s = CompactStr::from(combined);
                        Ok(len)
                    }
                    RudisValue::Int(n) => {
                        let s = Self::format_i64(*n);
                        let mut combined = Vec::with_capacity(s.len() + val_to_append.len());
                        combined.extend_from_slice(&s);
                        combined.extend_from_slice(val_to_append);
                        let len = combined.len();
                        entry.val = RudisValue::String(CompactStr::from(combined));
                        Ok(len)
                    }
                    RudisValue::HyperLogLog(regs) => {
                        let mut combined = crate::hll::hll_create_from_regs(regs, None);
                        combined.extend_from_slice(val_to_append);
                        let len = combined.len();
                        entry.val = RudisValue::String(CompactStr::from(combined));
                        Ok(len)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                let new_val = Bytes::copy_from_slice(val_to_append);
                let len = new_val.len();
                self.set(key, new_val, None);
                Ok(len)
            }
        } else {
            let new_val = Bytes::copy_from_slice(val_to_append);
            let len = new_val.len();
            self.set(key, new_val, None);
            Ok(len)
        }
    }

    pub fn strlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::String(s) => Ok(s.len()),
                    RudisValue::Int(n) => Ok(Self::format_i64(*n).len()),
                    RudisValue::HyperLogLog(regs) => {
                        let hll_bytes = crate::hll::hll_create_from_regs(regs, None);
                        Ok(hll_bytes.len())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    fn slice_range(slice: &[u8], mut start: i64, mut end: i64) -> Result<Bytes, &'static str> {
        let n = slice.len() as i64;
        if n == 0 {
            return Ok(Bytes::new());
        }
        if start < 0 && end < 0 && start > end {
            return Ok(Bytes::new());
        }
        if start < 0 {
            start += n;
        }
        if end < 0 {
            end += n;
        }
        if start < 0 {
            start = 0;
        }
        if end < 0 {
            end = 0;
        }
        if end >= n {
            end = n - 1;
        }
        if start > end {
            return Ok(Bytes::new());
        }
        let start_u = start as usize;
        let end_u = end as usize;
        Ok(Bytes::copy_from_slice(&slice[start_u..=end_u]))
    }

    pub fn getrange(&mut self, key: &[u8], start: i64, end: i64) -> Result<Bytes, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Bytes::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let sv;
                let slice = match &entry.val {
                    RudisValue::String(s) => {
                        sv = s.view();
                        &*sv
                    }
                    RudisValue::Int(n) => {
                        let formatted = Self::format_i64(*n);
                        return Self::slice_range(&formatted, start, end);
                    }
                    RudisValue::HyperLogLog(regs) => {
                        let hll_bytes = crate::hll::hll_create_from_regs(regs, None);
                        return Self::slice_range(&hll_bytes, start, end);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                };
                return Self::slice_range(slice, start, end);
            }
        }
        Ok(Bytes::new())
    }

    pub fn setrange(
        &mut self,
        key: Bytes,
        offset: usize,
        value: &[u8],
    ) -> Result<usize, &'static str> {
        if offset.saturating_add(value.len()) > 536870912 {
            return Err("ERR string exceeds maximum allowed size (512MB)");
        }
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.get_slot_mut_warm(idx)
        {
            let mut bytes: Vec<u8> = match &entry.val {
                RudisValue::String(s) => s.to_vec(),
                RudisValue::Int(n) => Self::format_i64(*n).to_vec(),
                RudisValue::HyperLogLog(regs) => crate::hll::hll_create_from_regs(regs, None),
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            };
            if offset > bytes.len() {
                bytes.resize(offset, 0);
            }
            if offset + value.len() > bytes.len() {
                bytes.resize(offset + value.len(), 0);
            }
            bytes[offset..offset + value.len()].copy_from_slice(value);
            let len = bytes.len();
            entry.val = RudisValue::String(CompactStr::from(bytes));
            return Ok(len);
        }
        if value.is_empty() && offset == 0 {
            return Ok(0);
        }
        let mut bytes = vec![0u8; offset];
        bytes.extend_from_slice(value);
        let len = bytes.len();
        let entry = RudisEntry::new(
            CompactKey::new(&key),
            RudisValue::String(CompactStr::from(bytes)),
            Expiry::from(None),
        );
        self.table.insert(entry);
        Ok(len)
    }

    pub fn incrbyfloat(&mut self, key: Bytes, delta: f64) -> Result<f64, &'static str> {
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.get_slot_mut_warm(idx)
        {
            let curr: f64 = match &entry.val {
                RudisValue::String(s) => {
                    let sv = s.view();
                    let str_val =
                        std::str::from_utf8(&sv).map_err(|_| "ERR value is not a valid float")?;
                    str_val
                        .parse::<f64>()
                        .map_err(|_| "ERR value is not a valid float")?
                }
                RudisValue::Int(n) => *n as f64,
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            };
            let new_val = curr + delta;
            if new_val.is_nan() || new_val.is_infinite() {
                return Err("ERR increment would produce NaN or Infinity");
            }
            entry.val = RudisValue::String(CompactStr::from(new_val.to_string()));
            return Ok(new_val);
        }
        if delta.is_nan() || delta.is_infinite() {
            return Err("ERR increment would produce NaN or Infinity");
        }
        let entry = RudisEntry::new(
            CompactKey::new(&key),
            RudisValue::String(CompactStr::from(delta.to_string())),
            Expiry::from(None),
        );
        self.table.insert(entry);
        Ok(delta)
    }

    pub fn increx(
        &mut self,
        key: Bytes,
        increment: crate::resp::IncrexIncrement,
        lbound: Option<crate::resp::IncrexBound>,
        ubound: Option<crate::resp::IncrexBound>,
        saturate: bool,
        expire: Option<crate::resp::IncrexExpire>,
        enx: bool,
    ) -> Result<IncrexOutput, &'static str> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let now_instant = Instant::now();

        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);

        let (key_exists, prior_expire_at, existing_slot) = if let Some(idx) = existing {
            if self.check_expired_slot(idx) {
                (false, None, None)
            } else if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &entry.val {
                    RudisValue::String(_) | RudisValue::Int(_) | RudisValue::HyperLogLog(_) => {
                        (true, entry.expire_at(), Some(idx))
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (false, None, None)
            }
        } else {
            (false, None, None)
        };

        match increment {
            crate::resp::IncrexIncrement::Int(delta) => {
                let prior_val: i64 = if key_exists {
                    let entry = self.table.get_slot(existing_slot.unwrap()).unwrap();
                    match &entry.val {
                        RudisValue::Int(n) => *n,
                        RudisValue::String(s) => {
                            let sv = s.view();
                            let str_val = std::str::from_utf8(&sv)
                                .map_err(|_| "ERR value is not an integer or out of range")?;
                            str_val
                                .parse::<i64>()
                                .map_err(|_| "ERR value is not an integer or out of range")?
                        }
                        _ => unreachable!(),
                    }
                } else {
                    0i64
                };

                let lb_opt = lbound.map(|b| match b {
                    crate::resp::IncrexBound::Int(n) => n,
                    _ => unreachable!(),
                });
                let ub_opt = ubound.map(|b| match b {
                    crate::resp::IncrexBound::Int(n) => n,
                    _ => unreachable!(),
                });

                let (val, applied_delta) = if saturate {
                    let unbounded = match prior_val.checked_add(delta) {
                        Some(v) => v,
                        None => {
                            if delta > 0 {
                                i64::MAX
                            } else {
                                i64::MIN
                            }
                        }
                    };
                    let mut saturated_val = unbounded;
                    if delta >= 0 {
                        if let Some(ub) = ub_opt {
                            saturated_val = saturated_val.min(ub);
                        }
                        if let Some(lb) = lb_opt {
                            saturated_val = saturated_val.max(lb);
                        }
                    } else {
                        if let Some(lb) = lb_opt {
                            saturated_val = saturated_val.max(lb);
                        }
                        if let Some(ub) = ub_opt {
                            saturated_val = saturated_val.min(ub);
                        }
                    }
                    let ad = saturated_val
                        .checked_sub(prior_val)
                        .ok_or("ERR applied increment would overflow")?;
                    (saturated_val, ad)
                } else {
                    let unbounded = prior_val.checked_add(delta);
                    let is_rejected = match unbounded {
                        None => true,
                        Some(v) => {
                            if let Some(ub) = ub_opt
                                && v > ub
                            {
                                true
                            } else if let Some(lb) = lb_opt
                                && v < lb
                            {
                                true
                            } else {
                                false
                            }
                        }
                    };
                    if is_rejected {
                        let current = if key_exists { prior_val } else { 0 };
                        return Ok(IncrexOutput {
                            is_float: false,
                            val_int: current,
                            delta_int: 0,
                            val_float: 0.0,
                            delta_float: 0.0,
                            rep_cmd: None,
                            events: smallvec::SmallVec::new(),
                        });
                    }
                    (unbounded.unwrap(), delta)
                };

                // Expiration logic
                let ttl_action;
                let mut new_expire_at = prior_expire_at;
                let mut expire_event = None;
                let mut is_past_expired = false;

                match expire {
                    Some(crate::resp::IncrexExpire::Persist) => {
                        if prior_expire_at.is_some() {
                            expire_event = Some("persist");
                        }
                        ttl_action = IncrexTtlAction::Persist;
                        new_expire_at = None;
                    }
                    Some(crate::resp::IncrexExpire::Ex(secs)) => {
                        if enx && prior_expire_at.is_some() {
                            ttl_action = IncrexTtlAction::KeepTtl;
                        } else {
                            let exp = now_ms.saturating_add(secs.saturating_mul(1000));
                            new_expire_at = Some(now_instant + Duration::from_secs(secs));
                            ttl_action = IncrexTtlAction::SetPxat(exp);
                            expire_event = Some("expire");
                        }
                    }
                    Some(crate::resp::IncrexExpire::Px(ms)) => {
                        if enx && prior_expire_at.is_some() {
                            ttl_action = IncrexTtlAction::KeepTtl;
                        } else {
                            let exp = now_ms.saturating_add(ms);
                            new_expire_at = Some(now_instant + Duration::from_millis(ms));
                            ttl_action = IncrexTtlAction::SetPxat(exp);
                            expire_event = Some("expire");
                        }
                    }
                    Some(crate::resp::IncrexExpire::Exat(secs)) => {
                        if enx && prior_expire_at.is_some() {
                            ttl_action = IncrexTtlAction::KeepTtl;
                        } else {
                            let exp = secs.saturating_mul(1000);
                            if exp <= now_ms {
                                is_past_expired = true;
                            } else {
                                new_expire_at =
                                    Some(now_instant + Duration::from_millis(exp - now_ms));
                            }
                            ttl_action = IncrexTtlAction::SetPxat(exp);
                            expire_event = Some("expire");
                        }
                    }
                    Some(crate::resp::IncrexExpire::Pxat(ms)) => {
                        if enx && prior_expire_at.is_some() {
                            ttl_action = IncrexTtlAction::KeepTtl;
                        } else {
                            if ms <= now_ms {
                                is_past_expired = true;
                            } else {
                                new_expire_at =
                                    Some(now_instant + Duration::from_millis(ms - now_ms));
                            }
                            ttl_action = IncrexTtlAction::SetPxat(ms);
                            expire_event = Some("expire");
                        }
                    }
                    None => {
                        ttl_action = IncrexTtlAction::KeepTtl;
                    }
                }

                if is_past_expired {
                    if key_exists {
                        let h = hash_key(&key);
                        self.del_with_hash(&key, h);
                    }
                    let rep_cmd = if is_lazyfree_lazy_expire() {
                        Command::Unlink(smallvec::smallvec![key.clone()])
                    } else {
                        Command::Del(smallvec::smallvec![key.clone()])
                    };
                    let mut events = smallvec::SmallVec::new();
                    events.push("del");
                    return Ok(IncrexOutput {
                        is_float: false,
                        val_int: val,
                        delta_int: applied_delta,
                        val_float: 0.0,
                        delta_float: 0.0,
                        rep_cmd: Some(rep_cmd),
                        events,
                    });
                }

                if let Some(idx) = existing_slot {
                    let entry = self.table.get_slot_mut(idx).unwrap();
                    let old_bytes = entry.val.mem_bytes() + entry.key.heap_bytes();
                    entry.val = RudisValue::Int(val);
                    entry.set_expire_at(new_expire_at);
                    self.data_bytes = self.data_bytes.saturating_sub(old_bytes)
                        + entry.val.mem_bytes()
                        + entry.key.heap_bytes();
                } else {
                    let entry = RudisEntry::new(
                        CompactKey::new(&key),
                        RudisValue::Int(val),
                        Expiry::from(new_expire_at),
                    );
                    self.data_bytes +=
                        entry.key.heap_bytes() + entry.val.mem_bytes() + ENTRY_OVERHEAD;
                    self.table.insert(entry);
                }
                // Keep `num_expires` in step: it gates lazy and active expiry.
                match (prior_expire_at.is_some(), new_expire_at.is_some()) {
                    (false, true) => self.num_expires += 1,
                    (true, false) => self.num_expires = self.num_expires.saturating_sub(1),
                    _ => {}
                }

                let mut events = smallvec::SmallVec::new();
                events.push("incrby");
                if let Some(ev) = expire_event {
                    events.push(ev);
                }

                let rep_val = Bytes::from(val.to_string());
                let rep_cmd = match ttl_action {
                    IncrexTtlAction::KeepTtl => Command::Set {
                        key,
                        value: rep_val,
                        expire_in: None,
                        condition: crate::resp::SetCondition::None,
                        get: false,
                        keepttl: true,
                        past_expired: false,
                    },
                    IncrexTtlAction::Persist => Command::Set {
                        key,
                        value: rep_val,
                        expire_in: None,
                        condition: crate::resp::SetCondition::None,
                        get: false,
                        keepttl: false,
                        past_expired: false,
                    },
                    IncrexTtlAction::SetPxat(ms) => Command::Set {
                        key,
                        value: rep_val,
                        expire_in: Some(std::time::Duration::from_millis(ms)),
                        condition: crate::resp::SetCondition::None,
                        get: false,
                        keepttl: false,
                        past_expired: false,
                    },
                };

                Ok(IncrexOutput {
                    is_float: false,
                    val_int: val,
                    delta_int: applied_delta,
                    val_float: 0.0,
                    delta_float: 0.0,
                    rep_cmd: Some(rep_cmd),
                    events,
                })
            }
            crate::resp::IncrexIncrement::Float(delta) => {
                let prior_val: f64 = if key_exists {
                    let entry = self.table.get_slot(existing_slot.unwrap()).unwrap();
                    match &entry.val {
                        RudisValue::Int(n) => *n as f64,
                        RudisValue::String(s) => {
                            let sv = s.view();
                            let str_val = std::str::from_utf8(&sv)
                                .map_err(|_| "ERR value is not a valid float")?;
                            if str_val.eq_ignore_ascii_case("inf")
                                || str_val.eq_ignore_ascii_case("+inf")
                                || str_val.eq_ignore_ascii_case("-inf")
                            {
                                return Err("ERR value cannot be Infinity");
                            }
                            str_val
                                .parse::<f64>()
                                .map_err(|_| "ERR value is not a valid float")?
                        }
                        _ => unreachable!(),
                    }
                } else {
                    0.0f64
                };

                let lb_opt = lbound.map(|b| match b {
                    crate::resp::IncrexBound::Float(f) => f,
                    _ => unreachable!(),
                });
                let ub_opt = ubound.map(|b| match b {
                    crate::resp::IncrexBound::Float(f) => f,
                    _ => unreachable!(),
                });

                let (val, applied_delta) = if saturate {
                    let unbounded = prior_val + delta;
                    if unbounded.is_nan() || unbounded.is_infinite() {
                        return Err("ERR increment would produce NaN or Infinity");
                    }
                    let mut saturated_val = unbounded;
                    if delta >= 0.0 {
                        if let Some(ub) = ub_opt {
                            saturated_val = saturated_val.min(ub);
                        }
                        if let Some(lb) = lb_opt {
                            saturated_val = saturated_val.max(lb);
                        }
                    } else {
                        if let Some(lb) = lb_opt {
                            saturated_val = saturated_val.max(lb);
                        }
                        if let Some(ub) = ub_opt {
                            saturated_val = saturated_val.min(ub);
                        }
                    }
                    let ad = saturated_val - prior_val;
                    (saturated_val, ad)
                } else {
                    let unbounded = prior_val + delta;
                    if unbounded.is_nan() || unbounded.is_infinite() {
                        return Err("ERR increment would produce NaN or Infinity");
                    }
                    let is_rejected = if let Some(ub) = ub_opt
                        && unbounded > ub
                    {
                        true
                    } else if let Some(lb) = lb_opt
                        && unbounded < lb
                    {
                        true
                    } else {
                        false
                    };
                    if is_rejected {
                        let current = if key_exists { prior_val } else { 0.0 };
                        return Ok(IncrexOutput {
                            is_float: true,
                            val_int: 0,
                            delta_int: 0,
                            val_float: current,
                            delta_float: 0.0,
                            rep_cmd: None,
                            events: smallvec::SmallVec::new(),
                        });
                    }
                    (unbounded, delta)
                };

                // Expiration logic
                let ttl_action;
                let mut new_expire_at = prior_expire_at;
                let mut expire_event = None;
                let mut is_past_expired = false;

                match expire {
                    Some(crate::resp::IncrexExpire::Persist) => {
                        if prior_expire_at.is_some() {
                            expire_event = Some("persist");
                        }
                        ttl_action = IncrexTtlAction::Persist;
                        new_expire_at = None;
                    }
                    Some(crate::resp::IncrexExpire::Ex(secs)) => {
                        if enx && prior_expire_at.is_some() {
                            ttl_action = IncrexTtlAction::KeepTtl;
                        } else {
                            let exp = now_ms.saturating_add(secs.saturating_mul(1000));
                            new_expire_at = Some(now_instant + Duration::from_secs(secs));
                            ttl_action = IncrexTtlAction::SetPxat(exp);
                            expire_event = Some("expire");
                        }
                    }
                    Some(crate::resp::IncrexExpire::Px(ms)) => {
                        if enx && prior_expire_at.is_some() {
                            ttl_action = IncrexTtlAction::KeepTtl;
                        } else {
                            let exp = now_ms.saturating_add(ms);
                            new_expire_at = Some(now_instant + Duration::from_millis(ms));
                            ttl_action = IncrexTtlAction::SetPxat(exp);
                            expire_event = Some("expire");
                        }
                    }
                    Some(crate::resp::IncrexExpire::Exat(secs)) => {
                        if enx && prior_expire_at.is_some() {
                            ttl_action = IncrexTtlAction::KeepTtl;
                        } else {
                            let exp = secs.saturating_mul(1000);
                            if exp <= now_ms {
                                is_past_expired = true;
                            } else {
                                new_expire_at =
                                    Some(now_instant + Duration::from_millis(exp - now_ms));
                            }
                            ttl_action = IncrexTtlAction::SetPxat(exp);
                            expire_event = Some("expire");
                        }
                    }
                    Some(crate::resp::IncrexExpire::Pxat(ms)) => {
                        if enx && prior_expire_at.is_some() {
                            ttl_action = IncrexTtlAction::KeepTtl;
                        } else {
                            if ms <= now_ms {
                                is_past_expired = true;
                            } else {
                                new_expire_at =
                                    Some(now_instant + Duration::from_millis(ms - now_ms));
                            }
                            ttl_action = IncrexTtlAction::SetPxat(ms);
                            expire_event = Some("expire");
                        }
                    }
                    None => {
                        ttl_action = IncrexTtlAction::KeepTtl;
                    }
                }

                if is_past_expired {
                    if key_exists {
                        let h = hash_key(&key);
                        self.del_with_hash(&key, h);
                    }
                    let rep_cmd = if is_lazyfree_lazy_expire() {
                        Command::Unlink(smallvec::smallvec![key.clone()])
                    } else {
                        Command::Del(smallvec::smallvec![key.clone()])
                    };
                    let mut events = smallvec::SmallVec::new();
                    events.push("del");
                    return Ok(IncrexOutput {
                        is_float: true,
                        val_int: 0,
                        delta_int: 0,
                        val_float: val,
                        delta_float: applied_delta,
                        rep_cmd: Some(rep_cmd),
                        events,
                    });
                }

                let val_str = val.to_string();
                if let Some(idx) = existing_slot {
                    let entry = self.table.get_slot_mut(idx).unwrap();
                    let old_bytes = entry.val.mem_bytes() + entry.key.heap_bytes();
                    entry.val = RudisValue::String(CompactStr::from(val_str.clone()));
                    entry.set_expire_at(new_expire_at);
                    self.data_bytes = self.data_bytes.saturating_sub(old_bytes)
                        + entry.val.mem_bytes()
                        + entry.key.heap_bytes();
                } else {
                    let entry = RudisEntry::new(
                        CompactKey::new(&key),
                        RudisValue::String(CompactStr::from(val_str.clone())),
                        Expiry::from(new_expire_at),
                    );
                    self.data_bytes +=
                        entry.key.heap_bytes() + entry.val.mem_bytes() + ENTRY_OVERHEAD;
                    self.table.insert(entry);
                }
                // Keep `num_expires` in step: it gates lazy and active expiry.
                match (prior_expire_at.is_some(), new_expire_at.is_some()) {
                    (false, true) => self.num_expires += 1,
                    (true, false) => self.num_expires = self.num_expires.saturating_sub(1),
                    _ => {}
                }

                let mut events = smallvec::SmallVec::new();
                events.push("incrbyfloat");
                if let Some(ev) = expire_event {
                    events.push(ev);
                }

                let rep_val = Bytes::from(val_str);
                let rep_cmd = match ttl_action {
                    IncrexTtlAction::KeepTtl => Command::Set {
                        key,
                        value: rep_val,
                        expire_in: None,
                        condition: crate::resp::SetCondition::None,
                        get: false,
                        keepttl: true,
                        past_expired: false,
                    },
                    IncrexTtlAction::Persist => Command::Set {
                        key,
                        value: rep_val,
                        expire_in: None,
                        condition: crate::resp::SetCondition::None,
                        get: false,
                        keepttl: false,
                        past_expired: false,
                    },
                    IncrexTtlAction::SetPxat(ms) => Command::Set {
                        key,
                        value: rep_val,
                        expire_in: Some(std::time::Duration::from_millis(ms)),
                        condition: crate::resp::SetCondition::None,
                        get: false,
                        keepttl: false,
                        past_expired: false,
                    },
                };

                Ok(IncrexOutput {
                    is_float: true,
                    val_int: 0,
                    delta_int: 0,
                    val_float: val,
                    delta_float: applied_delta,
                    rep_cmd: Some(rep_cmd),
                    events,
                })
            }
        }
    }

    #[inline(always)]
    pub fn hset_slice_fast(
        &mut self,
        key: &Bytes,
        fields: &[(Bytes, Bytes)],
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        self.hset_slice_internal(key.as_ref(), h, Some(key), fields)
    }

    #[inline(always)]
    pub fn hset_slice_with_hash(
        &mut self,
        key: &Bytes,
        h: u64,
        fields: &[(Bytes, Bytes)],
    ) -> Result<usize, &'static str> {
        self.hset_slice_internal(key.as_ref(), h, Some(key), fields)
    }

    #[inline(always)]
    pub fn hset_single_field_with_hash(
        &mut self,
        key: &Bytes,
        h: u64,
        field: &Bytes,
        val: &Bytes,
    ) -> Result<usize, &'static str> {
        let max_entries =
            crate::connection::HASH_MAX_ENTRIES.load(std::sync::atomic::Ordering::Relaxed);
        let max_value =
            crate::connection::HASH_MAX_VALUE.load(std::sync::atomic::Ordering::Relaxed);
        if let Some((idx, entry)) = self.table.find_entry_mut(key.as_ref(), h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                if let Some(ptr) = entry.uncool() {
                    self.data_bytes = self.data_bytes.saturating_sub(24);
                    self.dropped_tier.push((ptr, true));
                }
                match &mut entry.val {
                    RudisValue::SmallHash(pairs) => {
                        if pairs.len() == 1 && pairs[0].0 == *field {
                            pairs[0].1 = val.clone();
                            if val.len() > max_value {
                                let map: RudisHashMap = pairs.drain(..).collect();
                                entry.val = RudisValue::Hash(Box::new(map));
                            }
                            return Ok(0);
                        }
                        if let Some(pos) = pairs.iter().position(|(k, _)| k == field) {
                            pairs[pos].1 = val.clone();
                            if val.len() > max_value {
                                let map: RudisHashMap = pairs.drain(..).collect();
                                entry.val = RudisValue::Hash(Box::new(map));
                            }
                            return Ok(0);
                        } else {
                            pairs.push((field.clone(), val.clone()));
                            if pairs.len() > max_entries
                                || field.len() > max_value
                                || val.len() > max_value
                            {
                                let map: RudisHashMap = pairs.drain(..).collect();
                                entry.val = RudisValue::Hash(Box::new(map));
                            }
                            return Ok(1);
                        }
                    }
                    RudisValue::Hash(map) => {
                        if let Some(existing_val) = map.get_mut(field) {
                            *existing_val = val.clone();
                            return Ok(0);
                        }
                        map.insert(field.clone(), val.clone());
                        return Ok(1);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let (_, insert_idx) = self.table.find_or_prepare_insert(key.as_ref(), h);
        let val = if field.len() <= max_value && val.len() <= max_value {
            let mut pairs = self.arena.acquire_small_hash(1);
            pairs.push((field.clone(), val.clone()));
            RudisValue::SmallHash(Box::new(pairs))
        } else {
            let mut map = RudisHashMap::with_capacity_and_hasher(1, FxBuildHasher::default());
            map.insert(field.clone(), val.clone());
            RudisValue::Hash(Box::new(map))
        };
        let entry = RudisEntry::new(CompactKey::new(key), val, Expiry::from(None));
        self.table.insert_prepared(entry, h, insert_idx);
        Ok(1)
    }

    pub fn hset_slice(
        &mut self,
        key: &[u8],
        fields: &[(Bytes, Bytes)],
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        self.hset_slice_internal(key, h, None, fields)
    }

    #[inline(always)]
    fn hset_slice_internal(
        &mut self,
        key: &[u8],
        h: u64,
        _key_bytes: Option<&Bytes>, // unused: keys are copied into a CompactKey
        fields: &[(Bytes, Bytes)],
    ) -> Result<usize, &'static str> {
        let max_entries =
            crate::connection::HASH_MAX_ENTRIES.load(std::sync::atomic::Ordering::Relaxed);
        let max_value =
            crate::connection::HASH_MAX_VALUE.load(std::sync::atomic::Ordering::Relaxed);
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
            if let Some(fmap) = self.hash_field_expires.get_mut(key) {
                for (f, _) in fields {
                    fmap.remove(f);
                }
                if fmap.is_empty() {
                    self.hash_field_expires.remove(key);
                }
            }
        }
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                if let Some(ptr) = entry.uncool() {
                    self.data_bytes = self.data_bytes.saturating_sub(24);
                    self.dropped_tier.push((ptr, true));
                }
                match &mut entry.val {
                    RudisValue::SmallHash(pairs) => {
                        let mut added = 0;
                        if fields.len() == 1 {
                            let (f, v) = &fields[0];
                            if pairs.len() == 1 && pairs[0].0 == *f {
                                pairs[0].1 = v.clone();
                                if v.len() > max_value {
                                    let map: RudisHashMap = pairs.drain(..).collect();
                                    entry.val = RudisValue::Hash(Box::new(map));
                                }
                                return Ok(0);
                            }
                            if let Some(pos) = pairs.iter().position(|(k, _)| k == f) {
                                pairs[pos].1 = v.clone();
                                if v.len() > max_value {
                                    let map: RudisHashMap = pairs.drain(..).collect();
                                    entry.val = RudisValue::Hash(Box::new(map));
                                }
                            } else {
                                pairs.push((f.clone(), v.clone()));
                                if pairs.len() > max_entries
                                    || f.len() > max_value
                                    || v.len() > max_value
                                {
                                    let map: RudisHashMap = pairs.drain(..).collect();
                                    entry.val = RudisValue::Hash(Box::new(map));
                                }
                                added = 1;
                            }
                            return Ok(added);
                        } else {
                            for (f, v) in fields {
                                if let Some(pos) = pairs.iter().position(|(k, _)| k == f) {
                                    pairs[pos].1 = v.clone();
                                } else {
                                    pairs.push((f.clone(), v.clone()));
                                    added += 1;
                                }
                            }
                            if pairs.len() > max_entries
                                || fields
                                    .iter()
                                    .any(|(k, v)| k.len() > max_value || v.len() > max_value)
                            {
                                let map: RudisHashMap = pairs.drain(..).collect();
                                entry.val = RudisValue::Hash(Box::new(map));
                            }
                            return Ok(added);
                        }
                    }
                    RudisValue::Hash(map) => {
                        let mut added = 0;
                        if fields.len() == 1 {
                            let (f, v) = &fields[0];
                            if let Some(existing_val) = map.get_mut(f) {
                                *existing_val = v.clone();
                                return Ok(0);
                            }
                            map.insert(f.clone(), v.clone());
                            return Ok(1);
                        }
                        for (f, v) in fields {
                            if let Some(existing_val) = map.get_mut(f) {
                                *existing_val = v.clone();
                            } else {
                                map.insert(f.clone(), v.clone());
                                added += 1;
                            }
                        }
                        return Ok(added);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let (_, insert_idx) = self.table.find_or_prepare_insert(key, h);
        let (val, added) = if fields.len() <= max_entries
            && (if fields.len() == 1 {
                fields[0].0.len() <= max_value && fields[0].1.len() <= max_value
            } else {
                !fields
                    .iter()
                    .any(|(k, v)| k.len() > max_value || v.len() > max_value)
            }) {
            let mut pairs = self.arena.acquire_small_hash(fields.len());
            let added = if fields.len() == 1 {
                pairs.push((fields[0].0.clone(), fields[0].1.clone()));
                1
            } else {
                let mut a = 0;
                for (f, v) in fields {
                    if let Some(pos) = pairs.iter().position(|(k, _): &(Bytes, Bytes)| k == f) {
                        pairs[pos].1 = v.clone();
                    } else {
                        pairs.push((f.clone(), v.clone()));
                        a += 1;
                    }
                }
                a
            };
            (RudisValue::SmallHash(Box::new(pairs)), added)
        } else {
            let mut map =
                RudisHashMap::with_capacity_and_hasher(fields.len(), FxBuildHasher::default());
            let mut added = 0;
            for (f, v) in fields {
                if map.insert(f.clone(), v.clone()).is_none() {
                    added += 1;
                }
            }
            (RudisValue::Hash(Box::new(map)), added)
        };
        let entry = RudisEntry::new(CompactKey::new(key), val, Expiry::from(None));
        self.table.insert_prepared(entry, h, insert_idx);
        Ok(added)
    }

    #[inline]
    pub fn hset(&mut self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<usize, &'static str> {
        self.hset_slice_fast(&key, &fields)
    }

    pub fn hsetnx(
        &mut self,
        key: Bytes,
        field: Bytes,
        value: Bytes,
    ) -> Result<usize, &'static str> {
        let max_entries =
            crate::connection::HASH_MAX_ENTRIES.load(std::sync::atomic::Ordering::Relaxed);
        let max_value =
            crate::connection::HASH_MAX_VALUE.load(std::sync::atomic::Ordering::Relaxed);
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.get_slot_mut_warm(idx)
        {
            match &mut entry.val {
                RudisValue::SmallHash(pairs) => {
                    if pairs.iter().any(|(k, _)| *k == field) {
                        return Ok(0);
                    }
                    let should_promote = pairs.len() >= max_entries
                        || field.len() > max_value
                        || value.len() > max_value;
                    pairs.push((field, value));
                    if should_promote {
                        let map: RudisHashMap = pairs.drain(..).collect();
                        entry.val = RudisValue::Hash(Box::new(map));
                    }
                    return Ok(1);
                }
                RudisValue::Hash(map) => {
                    if map.contains_key(&field) {
                        return Ok(0);
                    }
                    map.insert(field, value);
                    return Ok(1);
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }

        let val = if field.len() > max_value || value.len() > max_value {
            let mut map = RudisHashMap::default();
            map.insert(field, value);
            RudisValue::Hash(Box::new(map))
        } else {
            RudisValue::SmallHash(Box::new(vec![(field, value)]))
        };
        let entry = RudisEntry::new(CompactKey::new(&key), val, Expiry::from(None));
        self.table.insert(entry);
        Ok(1)
    }

    pub fn purge_expired_hash_fields(&mut self, key: &[u8]) {
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut empty_map = false;
        if let Some(fmap) = self.hash_field_expires.get_mut(key) {
            fmap.retain(|f, exp| {
                if now >= *exp {
                    expired.push(f.clone());
                    false
                } else {
                    true
                }
            });
            empty_map = fmap.is_empty();
        }
        if empty_map {
            self.hash_field_expires.remove(key);
        }
        if !expired.is_empty() {
            let _ = self.hdel(key, &expired);
        }
    }

    pub fn hexpire(
        &mut self,
        key: &[u8],
        expire_ms: i64,
        is_at: bool,
        condition: crate::resp::HexpireCondition,
        fields: &[Bytes],
    ) -> Result<Vec<i64>, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        let Some(idx) = self.table.find(key, h) else {
            return Ok(vec![-2; fields.len()]);
        };
        if self.check_expired_slot(idx) {
            return Ok(vec![-2; fields.len()]);
        }
        let Some(entry) = self.table.get_slot(idx) else {
            return Ok(vec![-2; fields.len()]);
        };
        match &entry.val {
            RudisValue::SmallHash(_) | RudisValue::Hash(_) => {}
            _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }

        let now = Instant::now();
        let now_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let delta_ms = if is_at {
            expire_ms - now_unix_ms
        } else {
            expire_ms
        };

        let mut results = Vec::with_capacity(fields.len());
        let mut to_delete = Vec::new();

        for f in fields {
            let exists = if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => pairs.iter().any(|(k, _)| k == f),
                    RudisValue::Hash(map) => map.contains_key(f),
                    _ => false,
                }
            } else {
                false
            };
            if !exists {
                results.push(-2);
                continue;
            }

            let cur_exp = self
                .hash_field_expires
                .get(key)
                .and_then(|m| m.get(f).copied());

            if delta_ms <= 0 {
                let cond_ok = match condition {
                    crate::resp::HexpireCondition::None => true,
                    crate::resp::HexpireCondition::Nx => cur_exp.is_none(),
                    crate::resp::HexpireCondition::Xx => cur_exp.is_some(),
                    crate::resp::HexpireCondition::Gt => false,
                    crate::resp::HexpireCondition::Lt => true,
                };
                if cond_ok {
                    if let Some(m) = self.hash_field_expires.get_mut(key) {
                        m.remove(f);
                    }
                    to_delete.push(f.clone());
                    results.push(2);
                } else {
                    results.push(0);
                }
            } else {
                let new_exp = now + Duration::from_millis(delta_ms as u64);
                let cond_ok = match condition {
                    crate::resp::HexpireCondition::None => true,
                    crate::resp::HexpireCondition::Nx => cur_exp.is_none(),
                    crate::resp::HexpireCondition::Xx => cur_exp.is_some(),
                    crate::resp::HexpireCondition::Gt => match cur_exp {
                        Some(old) => new_exp > old,
                        None => false,
                    },
                    crate::resp::HexpireCondition::Lt => match cur_exp {
                        Some(old) => new_exp < old,
                        None => true,
                    },
                };
                if cond_ok {
                    self.hash_field_expires
                        .entry(Bytes::copy_from_slice(key))
                        .or_default()
                        .insert(f.clone(), new_exp);
                    results.push(1);
                } else {
                    results.push(0);
                }
            }
        }

        if !to_delete.is_empty() {
            let _ = self.hdel(key, &to_delete);
        }
        Ok(results)
    }

    pub fn httl(
        &mut self,
        key: &[u8],
        is_ms: bool,
        is_expiretime: bool,
        fields: &[Bytes],
    ) -> Result<Vec<i64>, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        let Some(idx) = self.table.find(key, h) else {
            return Ok(vec![-2; fields.len()]);
        };
        if self.check_expired_slot(idx) {
            return Ok(vec![-2; fields.len()]);
        }
        let Some(entry) = self.table.get_slot(idx) else {
            return Ok(vec![-2; fields.len()]);
        };
        match &entry.val {
            RudisValue::SmallHash(_) | RudisValue::Hash(_) => {}
            _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }

        let now = Instant::now();
        let now_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut results = Vec::with_capacity(fields.len());

        for f in fields {
            let exists = match &entry.val {
                RudisValue::SmallHash(pairs) => pairs.iter().any(|(k, _)| k == f),
                RudisValue::Hash(map) => map.contains_key(f),
                _ => false,
            };
            if !exists {
                results.push(-2);
                continue;
            }
            if let Some(exp) = self
                .hash_field_expires
                .get(key)
                .and_then(|m| m.get(f).copied())
            {
                let rem_ms = exp.saturating_duration_since(now).as_millis() as i64;
                if is_expiretime {
                    let abs_ms = now_unix_ms + rem_ms;
                    results.push(if is_ms { abs_ms } else { (abs_ms + 500) / 1000 });
                } else {
                    results.push(if is_ms { rem_ms } else { (rem_ms + 500) / 1000 });
                }
            } else {
                results.push(-1);
            }
        }
        Ok(results)
    }

    pub fn hpersist(&mut self, key: &[u8], fields: &[Bytes]) -> Result<Vec<i64>, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        let Some(idx) = self.table.find(key, h) else {
            return Ok(vec![-2; fields.len()]);
        };
        if self.check_expired_slot(idx) {
            return Ok(vec![-2; fields.len()]);
        }
        let Some(entry) = self.table.get_slot(idx) else {
            return Ok(vec![-2; fields.len()]);
        };
        match &entry.val {
            RudisValue::SmallHash(_) | RudisValue::Hash(_) => {}
            _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }

        let mut results = Vec::with_capacity(fields.len());
        for f in fields {
            let exists = match &entry.val {
                RudisValue::SmallHash(pairs) => pairs.iter().any(|(k, _)| k == f),
                RudisValue::Hash(map) => map.contains_key(f),
                _ => false,
            };
            if !exists {
                results.push(-2);
            } else if self
                .hash_field_expires
                .get_mut(key)
                .and_then(|m| m.remove(f))
                .is_some()
            {
                results.push(1);
            } else {
                results.push(-1);
            }
        }
        Ok(results)
    }

    pub fn hgetex(
        &mut self,
        key: &[u8],
        expire: crate::resp::HFieldExpireOpt,
        fields: &[Bytes],
    ) -> Result<(Vec<Option<Bytes>>, bool), &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        let Some(idx) = self.table.find(key, h) else {
            return Ok((vec![None; fields.len()], false));
        };
        if self.check_expired_slot(idx) {
            return Ok((vec![None; fields.len()], false));
        }
        let Some(entry) = self.table.get_slot(idx) else {
            return Ok((vec![None; fields.len()], false));
        };
        let mut vals = Vec::with_capacity(fields.len());
        let mut existing_fields = Vec::new();
        match &entry.val {
            RudisValue::SmallHash(pairs) => {
                for f in fields {
                    if let Some((_, v)) = pairs.iter().find(|(k, _)| k == f) {
                        vals.push(Some(v.clone()));
                        existing_fields.push(f.clone());
                    } else {
                        vals.push(None);
                    }
                }
            }
            RudisValue::Hash(map) => {
                for f in fields {
                    if let Some(v) = map.get(f) {
                        vals.push(Some(v.clone()));
                        existing_fields.push(f.clone());
                    } else {
                        vals.push(None);
                    }
                }
            }
            _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }

        if existing_fields.is_empty() {
            return Ok((vals, false));
        }

        let mut modified = false;
        match expire {
            crate::resp::HFieldExpireOpt::None | crate::resp::HFieldExpireOpt::KeepTtl => {}
            crate::resp::HFieldExpireOpt::Persist => {
                if let Some(fmap) = self.hash_field_expires.get_mut(key) {
                    for f in &existing_fields {
                        if fmap.remove(f).is_some() {
                            modified = true;
                        }
                    }
                    if fmap.is_empty() {
                        self.hash_field_expires.remove(key);
                    }
                }
            }
            crate::resp::HFieldExpireOpt::ExMs(ms) | crate::resp::HFieldExpireOpt::ExAtMs(ms) => {
                let is_at = matches!(expire, crate::resp::HFieldExpireOpt::ExAtMs(_));
                let now_unix_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let delta_ms = if is_at { ms - now_unix_ms } else { ms };
                modified = true;
                if delta_ms <= 0 {
                    let _ = self.hdel(key, &existing_fields);
                } else {
                    let new_exp = Instant::now() + Duration::from_millis(delta_ms as u64);
                    let fmap = self
                        .hash_field_expires
                        .entry(Bytes::copy_from_slice(key))
                        .or_default();
                    for f in existing_fields {
                        fmap.insert(f, new_exp);
                    }
                }
            }
        }

        Ok((vals, modified))
    }

    pub fn hsetex(
        &mut self,
        key: Bytes,
        condition: crate::resp::HsetexCondition,
        expire: crate::resp::HFieldExpireOpt,
        pairs: Vec<(Bytes, Bytes)>,
    ) -> Result<bool, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(&key);
        }
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if !self.check_expired_slot(idx)
                && let Some(entry) = self.table.get_slot(idx)
            {
                match &entry.val {
                    RudisValue::SmallHash(existing) => match condition {
                        crate::resp::HsetexCondition::None => {}
                        crate::resp::HsetexCondition::Fnx => {
                            if pairs
                                .iter()
                                .any(|(f, _)| existing.iter().any(|(k, _)| k == f))
                            {
                                return Ok(false);
                            }
                        }
                        crate::resp::HsetexCondition::Fxx => {
                            if pairs
                                .iter()
                                .any(|(f, _)| !existing.iter().any(|(k, _)| k == f))
                            {
                                return Ok(false);
                            }
                        }
                    },
                    RudisValue::Hash(map) => match condition {
                        crate::resp::HsetexCondition::None => {}
                        crate::resp::HsetexCondition::Fnx => {
                            if pairs.iter().any(|(f, _)| map.contains_key(f)) {
                                return Ok(false);
                            }
                        }
                        crate::resp::HsetexCondition::Fxx => {
                            if pairs.iter().any(|(f, _)| !map.contains_key(f)) {
                                return Ok(false);
                            }
                        }
                    },
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else if condition == crate::resp::HsetexCondition::Fxx {
                return Ok(false);
            }
        } else if condition == crate::resp::HsetexCondition::Fxx {
            return Ok(false);
        }

        let saved_expires: Vec<(Bytes, Instant)> =
            if expire == crate::resp::HFieldExpireOpt::KeepTtl {
                if let Some(fmap) = self.hash_field_expires.get(key.as_ref()) {
                    pairs
                        .iter()
                        .filter_map(|(f, _)| fmap.get(f).map(|&exp| (f.clone(), exp)))
                        .collect()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

        let fields: Vec<Bytes> = pairs.iter().map(|(f, _)| f.clone()).collect();
        self.hset(key.clone(), pairs)?;

        match expire {
            crate::resp::HFieldExpireOpt::None | crate::resp::HFieldExpireOpt::Persist => {}
            crate::resp::HFieldExpireOpt::KeepTtl => {
                if !saved_expires.is_empty() {
                    let fmap = self.hash_field_expires.entry(key).or_default();
                    for (f, exp) in saved_expires {
                        fmap.insert(f, exp);
                    }
                }
            }
            crate::resp::HFieldExpireOpt::ExMs(ms) | crate::resp::HFieldExpireOpt::ExAtMs(ms) => {
                let is_at = matches!(expire, crate::resp::HFieldExpireOpt::ExAtMs(_));
                let now_unix_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let delta_ms = if is_at { ms - now_unix_ms } else { ms };
                if delta_ms <= 0 {
                    let _ = self.hdel(&key, &fields);
                } else {
                    let new_exp = Instant::now() + Duration::from_millis(delta_ms as u64);
                    let fmap = self.hash_field_expires.entry(key).or_default();
                    for f in fields {
                        fmap.insert(f, new_exp);
                    }
                }
            }
        }

        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn xclaim(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
        min_idle_time: u64,
        ids: &[Bytes],
        idle: Option<u64>,
        time: Option<u64>,
        retrycount: Option<usize>,
        force: bool,
        justid: bool,
    ) -> Result<(Vec<(StreamId, Vec<(Bytes, Bytes)>)>, bool), String> {
        let h = hash_key(key);
        let Some(idx) = self.table.find(key, h) else {
            return Err("NOGROUP No such key or consumer group".to_string());
        };
        if self.check_expired_slot(idx) {
            return Err("NOGROUP No such key or consumer group".to_string());
        }
        let Some(entry) = self.table.get_slot_mut(idx) else {
            return Err("NOGROUP No such key or consumer group".to_string());
        };
        let RudisValue::Stream(stream) = &mut entry.val else {
            return Err(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            );
        };
        let Some(grp) = stream.groups.get_mut(group) else {
            return Err("NOGROUP No such key or consumer group".to_string());
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let target_delivery_time = if let Some(t) = time {
            t
        } else if let Some(id_ms) = idle {
            now_ms.saturating_sub(id_ms)
        } else {
            now_ms
        };

        let consumer_created = !grp.consumers.contains_key(&consumer);
        grp.consumers
            .entry(consumer.clone())
            .and_modify(|c| c.seen_time_ms = now_ms)
            .or_insert_with(|| StreamConsumer {
                name: consumer.clone(),
                seen_time_ms: now_ms,
                active_time_ms: None,
                pel: std::collections::BTreeMap::new(),
            });

        let mut claimed = Vec::new();
        for raw_id in ids {
            let s = std::str::from_utf8(raw_id)
                .map_err(|_| "Invalid stream ID specified as stream command argument")?;
            let sid = StreamId::parse_exact(s).map_err(|e| e.to_string())?;

            if let Some(pel_entry) = grp.pel.get(&sid).cloned() {
                let elapsed = now_ms.saturating_sub(pel_entry.delivery_time_ms);
                if elapsed < min_idle_time && !force {
                    continue;
                }
                let Some(fields) = stream.entries.get(&sid).cloned() else {
                    grp.pel.remove(&sid);
                    if let Some(old_c) = grp.consumers.get_mut(&pel_entry.consumer) {
                        old_c.pel.remove(&sid);
                    }
                    continue;
                };
                if let Some(old_c) = grp.consumers.get_mut(&pel_entry.consumer) {
                    old_c.pel.remove(&sid);
                }
                let new_count = retrycount.unwrap_or_else(|| {
                    if justid {
                        pel_entry.delivery_count
                    } else {
                        pel_entry.delivery_count.saturating_add(1)
                    }
                });
                if let Some(pe) = grp.pel.get_mut(&sid) {
                    pe.consumer = consumer.clone();
                    pe.delivery_time_ms = target_delivery_time;
                    pe.delivery_count = new_count;
                }
                if let Some(new_c) = grp.consumers.get_mut(&consumer) {
                    new_c.pel.insert(sid, target_delivery_time);
                }
                claimed.push((sid, fields));
            } else if force && let Some(fields) = stream.entries.get(&sid).cloned() {
                let new_count = retrycount.unwrap_or(1);
                grp.pel.insert(
                    sid,
                    StreamPelEntry {
                        consumer: consumer.clone(),
                        delivery_time_ms: target_delivery_time,
                        delivery_count: new_count,
                        nack_seq: 0,
                    },
                );
                if let Some(new_c) = grp.consumers.get_mut(&consumer) {
                    new_c.pel.insert(sid, target_delivery_time);
                }
                claimed.push((sid, fields));
            }
        }

        if !claimed.is_empty()
            && let Some(new_c) = grp.consumers.get_mut(&consumer)
        {
            new_c.active_time_ms = Some(now_ms);
        }

        Ok((claimed, consumer_created))
    }

    #[allow(clippy::type_complexity)]
    pub fn xautoclaim(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
        min_idle_time: u64,
        start: &[u8],
        count: usize,
        justid: bool,
    ) -> Result<
        (
            String,
            Vec<(StreamId, Vec<(Bytes, Bytes)>)>,
            Vec<StreamId>,
            bool,
        ),
        String,
    > {
        let start_s = std::str::from_utf8(start)
            .map_err(|_| "Invalid stream ID specified as stream command argument")?;
        let start_id = if start_s == "-" || start_s == "0" || start_s == "0-0" {
            StreamId::new(0, 0)
        } else {
            StreamId::parse_exact(start_s).map_err(|e| e.to_string())?
        };

        let h = hash_key(key);
        let Some(idx) = self.table.find(key, h) else {
            return Err("NOGROUP No such key or consumer group".to_string());
        };
        if self.check_expired_slot(idx) {
            return Err("NOGROUP No such key or consumer group".to_string());
        }
        let Some(entry) = self.table.get_slot_mut(idx) else {
            return Err("NOGROUP No such key or consumer group".to_string());
        };
        let RudisValue::Stream(stream) = &mut entry.val else {
            return Err(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            );
        };
        let Some(grp) = stream.groups.get_mut(group) else {
            return Err("NOGROUP No such key or consumer group".to_string());
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let consumer_created = !grp.consumers.contains_key(&consumer);
        grp.consumers
            .entry(consumer.clone())
            .and_modify(|c| c.seen_time_ms = now_ms)
            .or_insert_with(|| StreamConsumer {
                name: consumer.clone(),
                seen_time_ms: now_ms,
                active_time_ms: None,
                pel: std::collections::BTreeMap::new(),
            });

        let candidates: Vec<(StreamId, StreamPelEntry)> = grp
            .pel
            .range(start_id..)
            .take(count.saturating_add(1))
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        let next_cursor = if candidates.len() > count {
            candidates[count].0.to_string()
        } else {
            "0-0".to_string()
        };

        let mut claimed = Vec::new();
        let mut deleted_ids = Vec::new();

        for (sid, pel_entry) in candidates.into_iter().take(count) {
            let Some(fields) = stream.entries.get(&sid).cloned() else {
                grp.pel.remove(&sid);
                if let Some(old_c) = grp.consumers.get_mut(&pel_entry.consumer) {
                    old_c.pel.remove(&sid);
                }
                deleted_ids.push(sid);
                continue;
            };

            let elapsed = now_ms.saturating_sub(pel_entry.delivery_time_ms);
            if elapsed >= min_idle_time {
                if let Some(old_c) = grp.consumers.get_mut(&pel_entry.consumer) {
                    old_c.pel.remove(&sid);
                }
                if let Some(pe) = grp.pel.get_mut(&sid) {
                    pe.consumer = consumer.clone();
                    pe.delivery_time_ms = now_ms;
                    if !justid {
                        pe.delivery_count = pe.delivery_count.saturating_add(1);
                    }
                }
                if let Some(new_c) = grp.consumers.get_mut(&consumer) {
                    new_c.pel.insert(sid, now_ms);
                }
                claimed.push((sid, fields));
            }
        }

        if !claimed.is_empty()
            && let Some(new_c) = grp.consumers.get_mut(&consumer)
        {
            new_c.active_time_ms = Some(now_ms);
        }

        Ok((next_cursor, claimed, deleted_ids, consumer_created))
    }

    pub fn hget(&mut self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && let Some(entry) = self.table.get_slot(idx)
        {
            if let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(None);
            }
            match &entry.val {
                RudisValue::SmallHash(pairs) => {
                    let f_len = field.len();
                    for (k, v) in pairs.iter() {
                        if k.len() == f_len && k.as_ref() == field {
                            return Ok(Some(v.clone()));
                        }
                    }
                    Ok(None)
                }
                RudisValue::Hash(map) => Ok(map.get(field).cloned()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(None)
        }
    }

    #[inline(always)]
    pub fn hget_compact(
        &mut self,
        key: &[u8],
        field: &[u8],
    ) -> Result<crate::shard::CompactResp, &'static str> {
        let h = hash_key(key);
        self.hget_compact_with_hash(key, h, field)
    }

    #[inline(always)]
    pub fn hget_compact_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
        field: &[u8],
    ) -> Result<crate::shard::CompactResp, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::server_stats::note_key_lookup(false);
                return Ok(crate::shard::CompactResp::NULL);
            }
            crate::server_stats::note_key_lookup(true);
            match &entry.val {
                RudisValue::SmallHash(pairs) => {
                    let f_len = field.len();
                    match pairs.len() {
                        0 => Ok(crate::shard::CompactResp::NULL),
                        1 => {
                            if pairs[0].0.len() == f_len && pairs[0].0.as_ref() == field {
                                Ok(crate::shard::CompactResp::from_bulk(&pairs[0].1))
                            } else {
                                Ok(crate::shard::CompactResp::NULL)
                            }
                        }
                        _ => {
                            for (k, v) in pairs.iter() {
                                if k.len() == f_len && k.as_ref() == field {
                                    return Ok(crate::shard::CompactResp::from_bulk(v));
                                }
                            }
                            Ok(crate::shard::CompactResp::NULL)
                        }
                    }
                }
                RudisValue::Hash(map) => {
                    if let Some(v) = map.get(field) {
                        return Ok(crate::shard::CompactResp::from_bulk(v));
                    }
                    Ok(crate::shard::CompactResp::NULL)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            crate::server_stats::note_key_lookup(false);
            Ok(crate::shard::CompactResp::NULL)
        }
    }

    #[inline(always)]
    pub fn write_hget_resp(
        &mut self,
        key: &[u8],
        field: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::connection::write_resp_null(out);
                return Ok(());
            }
            match &entry.val {
                RudisValue::SmallHash(pairs) => {
                    let f_len = field.len();
                    match pairs.len() {
                        0 => {
                            crate::connection::write_resp_null(out);
                        }
                        1 => {
                            if pairs[0].0.len() == f_len && pairs[0].0.as_ref() == field {
                                crate::connection::write_resp_bulk(out, &pairs[0].1);
                            } else {
                                crate::connection::write_resp_null(out);
                            }
                        }
                        _ => {
                            for (k, v) in pairs.iter() {
                                if k.len() == f_len && k.as_ref() == field {
                                    crate::connection::write_resp_bulk(out, v);
                                    return Ok(());
                                }
                            }
                            crate::connection::write_resp_null(out);
                        }
                    }
                    Ok(())
                }
                RudisValue::Hash(map) => {
                    if let Some(v) = map.get(field) {
                        crate::connection::write_resp_bulk(out, v);
                    } else {
                        crate::connection::write_resp_null(out);
                    }
                    Ok(())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            crate::connection::write_resp_null(out);
            Ok(())
        }
    }

    pub fn hmget(
        &mut self,
        key: &[u8],
        fields: &[Bytes],
    ) -> Result<Vec<Option<Bytes>>, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(vec![None; fields.len()]);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(fields
                        .iter()
                        .map(|f| pairs.iter().find(|(k, _)| k == f).map(|(_, v)| v.clone()))
                        .collect()),
                    RudisValue::Hash(map) => {
                        Ok(fields.iter().map(|f| map.get(f).cloned()).collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(vec![None; fields.len()])
            }
        } else {
            Ok(vec![None; fields.len()])
        }
    }

    pub fn hdel(&mut self, key: &[u8], fields: &[Bytes]) -> Result<usize, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (count, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::SmallHash(pairs) => {
                        let mut c = 0;
                        for f in fields {
                            if let Some(pos) = pairs.iter().position(|(k, _)| k == f) {
                                pairs.swap_remove(pos);
                                c += 1;
                            }
                        }
                        (c, pairs.is_empty())
                    }
                    RudisValue::Hash(map) => {
                        let mut c = 0;
                        for f in fields {
                            if map.remove(f).is_some() {
                                c += 1;
                            }
                        }
                        (c, map.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (0, false)
            };

            if !self.hash_field_expires.is_empty() {
                if is_empty {
                    self.hash_field_expires.remove(key);
                } else if let Some(fmap) = self.hash_field_expires.get_mut(key) {
                    for f in fields {
                        fmap.remove(f);
                    }
                    if fmap.is_empty() {
                        self.hash_field_expires.remove(key);
                    }
                }
            }

            if is_empty && let Some(entry) = self.table.remove(idx) {
                self.recycle_value(entry.val);
            }
            Ok(count)
        } else {
            Ok(0)
        }
    }

    pub fn hexists(&mut self, key: &[u8], field: &[u8]) -> Result<bool, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(false);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.iter().any(|(k, _)| k == field)),
                    RudisValue::Hash(map) => Ok(map.contains_key(field)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    pub fn hlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.len()),
                    RudisValue::Hash(map) => Ok(map.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn hgetall(&mut self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.to_vec()),
                    RudisValue::Hash(map) => {
                        Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn hkeys(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => {
                        Ok(pairs.iter().map(|(k, _)| k.clone()).collect())
                    }
                    RudisValue::Hash(map) => Ok(map.keys().cloned().collect()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn hvals(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => {
                        Ok(pairs.iter().map(|(_, v)| v.clone()).collect())
                    }
                    RudisValue::Hash(map) => Ok(map.values().cloned().collect()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn hstrlen(&mut self, key: &[u8], field: &[u8]) -> Result<usize, &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => {
                        if let Some((_, v)) = pairs.iter().find(|(k, _)| k == field) {
                            Ok(v.len())
                        } else {
                            Ok(0)
                        }
                    }
                    RudisValue::Hash(map) => {
                        if let Some(v) = map.get(field) {
                            Ok(v.len())
                        } else {
                            Ok(0)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn hgetdel(
        &mut self,
        key: &[u8],
        fields: &[Bytes],
    ) -> Result<(Vec<Option<Bytes>>, Vec<Bytes>), &'static str> {
        if !self.hash_field_expires.is_empty() {
            self.purge_expired_hash_fields(key);
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok((vec![None; fields.len()], Vec::new()));
            }
            let (results, deleted_fields, is_empty) =
                if let Some(entry) = self.get_slot_mut_warm(idx) {
                    let mut results = Vec::with_capacity(fields.len());
                    let mut deleted_fields = Vec::new();
                    match &mut entry.val {
                        RudisValue::SmallHash(pairs) => {
                            for f in fields {
                                if let Some(pos) = pairs.iter().position(|(k, _)| k == f) {
                                    let (_, v) = pairs.remove(pos);
                                    results.push(Some(v));
                                    deleted_fields.push(f.clone());
                                } else {
                                    results.push(None);
                                }
                            }
                            (results, deleted_fields, pairs.is_empty())
                        }
                        RudisValue::Hash(map) => {
                            for f in fields {
                                if let Some(v) = map.remove(f) {
                                    results.push(Some(v));
                                    deleted_fields.push(f.clone());
                                } else {
                                    results.push(None);
                                }
                            }
                            (results, deleted_fields, map.is_empty())
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                } else {
                    (vec![None; fields.len()], Vec::new(), false)
                };

            if is_empty {
                self.table.remove(idx);
            }
            Ok((results, deleted_fields))
        } else {
            Ok((vec![None; fields.len()], Vec::new()))
        }
    }

    pub fn object_encoding(&mut self, key: &[u8]) -> Option<&'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return None;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                return match &entry.val {
                    RudisValue::String(s) => {
                        if std::str::from_utf8(&s.view())
                            .ok()
                            .and_then(|t| t.parse::<i64>().ok())
                            .is_some()
                        {
                            Some("int")
                        } else {
                            Some("raw")
                        }
                    }
                    RudisValue::Int(_) => Some("int"),
                    RudisValue::SmallHash(_) => Some("listpack"),
                    RudisValue::Hash(_) => Some("hashtable"),
                    RudisValue::List(_) => Some("quicklist"),
                    RudisValue::Set(s) => match &**s {
                        RudisSet::Small(_) => Some("intset"),
                        RudisSet::Full(_) => Some("hashtable"),
                    },
                    RudisValue::ZSet(z) => match &**z {
                        RudisZSet::Small(_) => Some("listpack"),
                        RudisZSet::Full { .. } => Some("skiplist"),
                    },
                    RudisValue::Stream(_) => Some("stream"),
                    _ => Some("raw"),
                };
            }
        }
        None
    }

    pub fn hincrby(&mut self, key: Bytes, field: Bytes, delta: i64) -> Result<i64, &'static str> {
        let max_entries =
            crate::connection::HASH_MAX_ENTRIES.load(std::sync::atomic::Ordering::Relaxed);
        let max_value =
            crate::connection::HASH_MAX_VALUE.load(std::sync::atomic::Ordering::Relaxed);
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.get_slot_mut_warm(idx)
        {
            match &mut entry.val {
                RudisValue::SmallHash(pairs) => {
                    let curr = if let Some((_, v)) = pairs.iter().find(|(k, _)| k == &field) {
                        let s = std::str::from_utf8(v)
                            .map_err(|_| "ERR hash value is not an integer")?;
                        s.parse::<i64>()
                            .map_err(|_| "ERR hash value is not an integer")?
                    } else {
                        0
                    };
                    let new_val = curr
                        .checked_add(delta)
                        .ok_or("ERR increment or decrement would overflow")?;
                    let new_bytes = Bytes::from(new_val.to_string());
                    if let Some(pos) = pairs.iter().position(|(k, _)| k == &field) {
                        pairs[pos].1 = new_bytes;
                    } else {
                        pairs.push((field, new_bytes));
                        if pairs.len() > max_entries
                            || pairs
                                .iter()
                                .any(|(k, v)| k.len() > max_value || v.len() > max_value)
                        {
                            let map: RudisHashMap = pairs.drain(..).collect();
                            entry.val = RudisValue::Hash(Box::new(map));
                        }
                    }
                    return Ok(new_val);
                }
                RudisValue::Hash(map) => {
                    let curr = if let Some(v) = map.get(&field) {
                        let s = std::str::from_utf8(v)
                            .map_err(|_| "ERR hash value is not an integer")?;
                        s.parse::<i64>()
                            .map_err(|_| "ERR hash value is not an integer")?
                    } else {
                        0
                    };
                    let new_val = curr
                        .checked_add(delta)
                        .ok_or("ERR increment or decrement would overflow")?;
                    let new_bytes = Bytes::from(new_val.to_string());
                    if let Some(existing_val) = map.get_mut(&field) {
                        *existing_val = new_bytes;
                    } else {
                        map.insert(field, new_bytes);
                    }
                    return Ok(new_val);
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        let val_bytes = Bytes::from(delta.to_string());
        let entry = RudisEntry::new(
            CompactKey::new(&key),
            RudisValue::SmallHash(Box::new(vec![(field, val_bytes)])),
            Expiry::from(None),
        );
        self.table.insert(entry);
        Ok(delta)
    }

    pub fn hincrbyfloat(
        &mut self,
        key: Bytes,
        field: Bytes,
        delta: f64,
    ) -> Result<f64, &'static str> {
        let max_entries =
            crate::connection::HASH_MAX_ENTRIES.load(std::sync::atomic::Ordering::Relaxed);
        let max_value =
            crate::connection::HASH_MAX_VALUE.load(std::sync::atomic::Ordering::Relaxed);
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.get_slot_mut_warm(idx)
        {
            match &mut entry.val {
                RudisValue::SmallHash(pairs) => {
                    let curr = if let Some((_, v)) = pairs.iter().find(|(k, _)| k == &field) {
                        let s = std::str::from_utf8(v)
                            .map_err(|_| "ERR hash value is not a valid float")?;
                        s.parse::<f64>()
                            .map_err(|_| "ERR hash value is not a valid float")?
                    } else {
                        0.0
                    };
                    let new_val = curr + delta;
                    if new_val.is_nan() || new_val.is_infinite() {
                        return Err("ERR increment would produce NaN or Infinity");
                    }
                    let new_bytes = Bytes::from(new_val.to_string());
                    if let Some(pos) = pairs.iter().position(|(k, _)| k == &field) {
                        pairs[pos].1 = new_bytes;
                    } else {
                        pairs.push((field, new_bytes));
                        if pairs.len() > max_entries
                            || pairs
                                .iter()
                                .any(|(k, v)| k.len() > max_value || v.len() > max_value)
                        {
                            let map: RudisHashMap = pairs.drain(..).collect();
                            entry.val = RudisValue::Hash(Box::new(map));
                        }
                    }
                    return Ok(new_val);
                }
                RudisValue::Hash(map) => {
                    let curr = if let Some(v) = map.get(&field) {
                        let s = std::str::from_utf8(v)
                            .map_err(|_| "ERR hash value is not a valid float")?;
                        s.parse::<f64>()
                            .map_err(|_| "ERR hash value is not a valid float")?
                    } else {
                        0.0
                    };
                    let new_val = curr + delta;
                    if new_val.is_nan() || new_val.is_infinite() {
                        return Err("ERR increment would produce NaN or Infinity");
                    }
                    let new_bytes = Bytes::from(new_val.to_string());
                    if let Some(existing_val) = map.get_mut(&field) {
                        *existing_val = new_bytes;
                    } else {
                        map.insert(field, new_bytes);
                    }
                    return Ok(new_val);
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        if delta.is_nan() || delta.is_infinite() {
            return Err("ERR increment would produce NaN or Infinity");
        }
        let val_bytes = Bytes::from(delta.to_string());
        let entry = RudisEntry::new(
            CompactKey::new(&key),
            RudisValue::SmallHash(Box::new(vec![(field, val_bytes)])),
            Expiry::from(None),
        );
        self.table.insert(entry);
        Ok(delta)
    }

    pub fn hrandfield(
        &mut self,
        key: &[u8],
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        let pairs: Vec<(Bytes, Bytes)> = if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(p) => p.to_vec(),
                    RudisValue::Hash(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                return Ok(Vec::new());
            }
        } else {
            return Ok(Vec::new());
        };

        if pairs.is_empty() {
            return Ok(Vec::new());
        }

        let total = pairs.len();
        match count {
            None => {
                let idx = self.next_rand() % total;
                Ok(vec![pairs[idx].0.clone()])
            }
            Some(c) if c >= 0 => {
                let k = (c as usize).min(total);
                let mut indices: Vec<usize> = (0..total).collect();
                for i in 0..k {
                    let r = i + (self.next_rand() % (total - i));
                    indices.swap(i, r);
                }
                let mut res = Vec::with_capacity(k * if with_values { 2 } else { 1 });
                for &idx in &indices[..k] {
                    res.push(pairs[idx].0.clone());
                    if with_values {
                        res.push(pairs[idx].1.clone());
                    }
                }
                Ok(res)
            }
            Some(c) => {
                let k = (-c) as usize;
                let mut res = Vec::with_capacity(k * if with_values { 2 } else { 1 });
                for _ in 0..k {
                    let idx = self.next_rand() % total;
                    res.push(pairs[idx].0.clone());
                    if with_values {
                        res.push(pairs[idx].1.clone());
                    }
                }
                Ok(res)
            }
        }
    }

    pub fn hscan(
        &mut self,
        key: &[u8],
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
        no_values: bool,
    ) -> Result<(usize, Vec<Bytes>), &'static str> {
        let h = hash_key(key);
        let pairs: Vec<(Bytes, Bytes)> = if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok((0, Vec::new()));
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(p) => p.to_vec(),
                    RudisValue::Hash(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                return Ok((0, Vec::new()));
            }
        } else {
            return Ok((0, Vec::new()));
        };

        if cursor >= pairs.len() || pairs.is_empty() {
            return Ok((0, Vec::new()));
        }

        let mut res = Vec::new();
        let mut idx = cursor;
        let limit = if no_values {
            count
        } else {
            count.saturating_mul(2)
        };
        while idx < pairs.len() && res.len() < limit {
            let (f, v) = &pairs[idx];
            let matches = match pattern {
                Some(pat) => crate::pubsub::glob_match(pat, f),
                None => true,
            };
            if matches {
                res.push(f.clone());
                if !no_values {
                    res.push(v.clone());
                }
            }
            idx += 1;
        }
        let next_cursor = if idx >= pairs.len() { 0 } else { idx };
        Ok((next_cursor, res))
    }

    // LIST METHODS
    #[inline(always)]
    pub fn lpush_slice_fast(
        &mut self,
        key: &Bytes,
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        self.lpush_slice_internal(key.as_ref(), h, Some(key), values)
    }

    #[inline(always)]
    pub fn lpush_slice_with_hash(
        &mut self,
        key: &Bytes,
        h: u64,
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        self.lpush_slice_internal(key.as_ref(), h, Some(key), values)
    }

    pub fn lpush_slice(&mut self, key: &[u8], values: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        self.lpush_slice_internal(key, h, None, values)
    }

    #[inline(always)]
    fn lpush_slice_internal(
        &mut self,
        key: &[u8],
        h: u64,
        _key_bytes: Option<&Bytes>, // unused: keys are copied into a CompactKey
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                if let Some(ptr) = entry.uncool() {
                    self.data_bytes = self.data_bytes.saturating_sub(24);
                    self.dropped_tier.push((ptr, true));
                }
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        if values.len() == 1 {
                            deque.push_front(values[0].clone());
                            return Ok(deque.len());
                        }
                        for v in values {
                            deque.push_front(v.clone());
                        }
                        return Ok(deque.len());
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let (_, insert_idx) = self.table.find_or_prepare_insert(key, h);
        let mut deque = self.arena.acquire_list(values.len());
        if values.len() == 1 {
            deque.push_front(values[0].clone());
        } else {
            for v in values {
                deque.push_front(v.clone());
            }
        }
        let len = deque.len();
        let entry = RudisEntry::new(
            CompactKey::new(key),
            RudisValue::List(Box::new(deque)),
            Expiry::from(None),
        );
        self.table.insert_prepared(entry, h, insert_idx);
        Ok(len)
    }

    #[inline]
    pub fn lpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.lpush_slice_fast(&key, &values)
    }

    #[inline(always)]
    pub fn rpush_slice_fast(
        &mut self,
        key: &Bytes,
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        self.rpush_slice_internal(key.as_ref(), h, Some(key), values)
    }

    pub fn rpush_slice(&mut self, key: &[u8], values: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        self.rpush_slice_internal(key, h, None, values)
    }

    #[inline(always)]
    fn rpush_slice_internal(
        &mut self,
        key: &[u8],
        h: u64,
        _key_bytes: Option<&Bytes>, // unused: keys are copied into a CompactKey
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                if let Some(ptr) = entry.uncool() {
                    self.data_bytes = self.data_bytes.saturating_sub(24);
                    self.dropped_tier.push((ptr, true));
                }
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        if values.len() == 1 {
                            deque.push_back(values[0].clone());
                            return Ok(deque.len());
                        }
                        for v in values {
                            deque.push_back(v.clone());
                        }
                        return Ok(deque.len());
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let (_, insert_idx) = self.table.find_or_prepare_insert(key, h);
        let mut deque = self.arena.acquire_list(values.len());
        if values.len() == 1 {
            deque.push_back(values[0].clone());
        } else {
            for v in values {
                deque.push_back(v.clone());
            }
        }
        let len = deque.len();
        let entry = RudisEntry::new(
            CompactKey::new(key),
            RudisValue::List(Box::new(deque)),
            Expiry::from(None),
        );
        self.table.insert_prepared(entry, h, insert_idx);
        Ok(len)
    }

    #[inline]
    pub fn rpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.rpush_slice(&key, &values)
    }

    pub fn lpushx_slice(&mut self, key: &[u8], values: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        for v in values {
                            deque.push_front(v.clone());
                        }
                        return Ok(deque.len());
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }
        Ok(0)
    }

    #[inline]
    pub fn lpushx(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.lpushx_slice(&key, &values)
    }

    pub fn rpushx_slice(&mut self, key: &[u8], values: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        for v in values {
                            deque.push_back(v.clone());
                        }
                        return Ok(deque.len());
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }
        Ok(0)
    }

    #[inline]
    pub fn rpushx(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.rpushx_slice(&key, &values)
    }

    #[inline(always)]
    pub fn write_lpop_resp_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
        count: Option<usize>,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                if count.is_some() {
                    crate::connection::write_resp_null_array(out);
                } else {
                    crate::connection::write_resp_null(out);
                }
                return Ok(false);
            }
            if let Some(ptr) = entry.uncool() {
                self.data_bytes = self.data_bytes.saturating_sub(24);
                self.dropped_tier.push((ptr, true));
            }

            let mut has_written = false;
            let is_empty = match &mut entry.val {
                RudisValue::List(deque) => {
                    if count.is_none() && deque.len() == 1 {
                        let val = deque.pop_front().unwrap();
                        crate::connection::write_resp_bulk(out, &val);
                        let entry = self.table.remove_present(idx);
                        if self.num_expires > 0 && entry.expire_at().is_some() {
                            self.num_expires = self.num_expires.saturating_sub(1);
                        }
                        self.recycle_value(entry.val);
                        return Ok(true);
                    }
                    if let Some(cnt) = count {
                        let n = cnt.min(deque.len());
                        crate::connection::write_resp_array_header(out, n);
                        for _ in 0..n {
                            if let Some(val) = deque.pop_front() {
                                has_written = true;
                                crate::connection::write_resp_bulk(out, &val);
                            }
                        }
                    } else if let Some(val) = deque.pop_front() {
                        has_written = true;
                        crate::connection::write_resp_bulk(out, &val);
                    } else {
                        crate::connection::write_resp_null(out);
                    }
                    deque.is_empty()
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            };

            if is_empty {
                let entry = self.table.remove_present(idx);
                if self.num_expires > 0 && entry.expire_at().is_some() {
                    self.num_expires = self.num_expires.saturating_sub(1);
                }
                self.recycle_value(entry.val);
            }
            Ok(has_written)
        } else {
            if count.is_some() {
                crate::connection::write_resp_null_array(out);
            } else {
                crate::connection::write_resp_null(out);
            }
            Ok(false)
        }
    }

    #[inline(always)]
    pub fn write_lpop_resp(
        &mut self,
        key: &[u8],
        count: Option<usize>,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        let h = hash_key(key);
        self.write_lpop_resp_with_hash(key, h, count, out)
    }

    #[inline(always)]
    pub fn write_rpop_resp_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
        count: Option<usize>,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                if count.is_some() {
                    crate::connection::write_resp_null_array(out);
                } else {
                    crate::connection::write_resp_null(out);
                }
                return Ok(false);
            }
            if let Some(ptr) = entry.uncool() {
                self.data_bytes = self.data_bytes.saturating_sub(24);
                self.dropped_tier.push((ptr, true));
            }

            let mut has_written = false;
            let is_empty = match &mut entry.val {
                RudisValue::List(deque) => {
                    if count.is_none() && deque.len() == 1 {
                        let val = deque.pop_back().unwrap();
                        crate::connection::write_resp_bulk(out, &val);
                        let entry = self.table.remove_present(idx);
                        if self.num_expires > 0 && entry.expire_at().is_some() {
                            self.num_expires = self.num_expires.saturating_sub(1);
                        }
                        self.recycle_value(entry.val);
                        return Ok(true);
                    }
                    if let Some(cnt) = count {
                        let n = cnt.min(deque.len());
                        crate::connection::write_resp_array_header(out, n);
                        for _ in 0..n {
                            if let Some(val) = deque.pop_back() {
                                has_written = true;
                                crate::connection::write_resp_bulk(out, &val);
                            }
                        }
                    } else if let Some(val) = deque.pop_back() {
                        has_written = true;
                        crate::connection::write_resp_bulk(out, &val);
                    } else {
                        crate::connection::write_resp_null(out);
                    }
                    deque.is_empty()
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            };

            if is_empty {
                let entry = self.table.remove_present(idx);
                if self.num_expires > 0 && entry.expire_at().is_some() {
                    self.num_expires = self.num_expires.saturating_sub(1);
                }
                self.recycle_value(entry.val);
            }
            Ok(has_written)
        } else {
            if count.is_some() {
                crate::connection::write_resp_null_array(out);
            } else {
                crate::connection::write_resp_null(out);
            }
            Ok(false)
        }
    }

    #[inline(always)]
    pub fn write_rpop_resp(
        &mut self,
        key: &[u8],
        count: Option<usize>,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        let h = hash_key(key);
        self.write_rpop_resp_with_hash(key, h, count, out)
    }

    #[inline(always)]
    pub fn lpop_one(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        self.lpop_one_with_hash(key, h)
    }

    #[inline(always)]
    pub fn lpop_one_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
    ) -> Result<Option<Bytes>, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(None);
            }
            if let Some(ptr) = entry.uncool() {
                self.data_bytes = self.data_bytes.saturating_sub(24);
                self.dropped_tier.push((ptr, true));
            }
            let (popped, is_empty) = match &mut entry.val {
                RudisValue::List(deque) => {
                    if deque.len() == 1 {
                        let val = deque.pop_front();
                        let entry = self.table.remove_present(idx);
                        if self.num_expires > 0 && entry.expire_at().is_some() {
                            self.num_expires = self.num_expires.saturating_sub(1);
                        }
                        self.recycle_value(entry.val);
                        return Ok(val);
                    }
                    let val = deque.pop_front();
                    let empty = deque.is_empty();
                    (val, empty)
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            };

            if is_empty {
                let entry = self.table.remove_present(idx);
                if self.num_expires > 0 && entry.expire_at().is_some() {
                    self.num_expires = self.num_expires.saturating_sub(1);
                }
                self.recycle_value(entry.val);
            }
            Ok(popped)
        } else {
            Ok(None)
        }
    }

    #[inline(always)]
    pub fn rpop_one(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        self.rpop_one_with_hash(key, h)
    }

    #[inline(always)]
    pub fn rpop_one_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
    ) -> Result<Option<Bytes>, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(None);
            }
            if let Some(ptr) = entry.uncool() {
                self.data_bytes = self.data_bytes.saturating_sub(24);
                self.dropped_tier.push((ptr, true));
            }
            let (popped, is_empty) = match &mut entry.val {
                RudisValue::List(deque) => {
                    if deque.len() == 1 {
                        let val = deque.pop_back();
                        let entry = self.table.remove_present(idx);
                        if self.num_expires > 0 && entry.expire_at().is_some() {
                            self.num_expires = self.num_expires.saturating_sub(1);
                        }
                        self.recycle_value(entry.val);
                        return Ok(val);
                    }
                    let val = deque.pop_back();
                    let empty = deque.is_empty();
                    (val, empty)
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            };

            if is_empty {
                let entry = self.table.remove_present(idx);
                if self.num_expires > 0 && entry.expire_at().is_some() {
                    self.num_expires = self.num_expires.saturating_sub(1);
                }
                self.recycle_value(entry.val);
            }
            Ok(popped)
        } else {
            Ok(None)
        }
    }

    pub fn lpop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (popped, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        let mut res = Vec::with_capacity(count.min(deque.len()));
                        for _ in 0..count {
                            if let Some(val) = deque.pop_front() {
                                res.push(val);
                            } else {
                                break;
                            }
                        }
                        let empty = deque.is_empty();
                        (res, empty)
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty && let Some(entry) = self.table.remove(idx) {
                self.recycle_value(entry.val);
            }
            Ok(popped)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn rpop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (popped, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        let mut res = Vec::with_capacity(count.min(deque.len()));
                        for _ in 0..count {
                            if let Some(val) = deque.pop_back() {
                                res.push(val);
                            } else {
                                break;
                            }
                        }
                        let empty = deque.is_empty();
                        (res, empty)
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty && let Some(entry) = self.table.remove(idx) {
                self.recycle_value(entry.val);
            }
            Ok(popped)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn llen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::List(deque) => Ok(deque.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn lindex(&mut self, key: &[u8], index: i64) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::List(deque) => {
                        let n = deque.len() as i64;
                        let actual_idx = if index < 0 { n + index } else { index };
                        if actual_idx >= 0 && actual_idx < n {
                            Ok(deque.get(actual_idx as usize).cloned())
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn lrange(
        &mut self,
        key: &[u8],
        mut start: i64,
        mut stop: i64,
    ) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::List(deque) => {
                        let n = deque.len() as i64;
                        if n == 0 {
                            return Ok(Vec::new());
                        }
                        if start < 0 {
                            start = (n + start).max(0);
                        }
                        if stop < 0 {
                            stop += n;
                        }
                        if start > stop || start >= n {
                            return Ok(Vec::new());
                        }
                        let start_u = start.max(0) as usize;
                        let stop_u = (stop.min(n - 1) as usize).max(start_u);
                        let mut res = Vec::with_capacity(stop_u - start_u + 1);
                        for i in start_u..=stop_u {
                            if let Some(v) = deque.get(i) {
                                res.push(v.clone());
                            }
                        }
                        Ok(res)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    #[inline(always)]
    pub fn lrange_compact_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
        mut start: i64,
        mut stop: i64,
        out: &mut Vec<u8>,
    ) -> Result<crate::shard::CompactResp, &'static str> {
        crate::server_stats::note_key_lookup(self.key_is_live(key));
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(crate::shard::CompactResp::EMPTY_ARRAY);
            }
            match &entry.val {
                RudisValue::List(deque) => {
                    let n = deque.len() as i64;
                    if n == 0 {
                        return Ok(crate::shard::CompactResp::EMPTY_ARRAY);
                    }
                    if start < 0 {
                        start = (n + start).max(0);
                    }
                    if stop < 0 {
                        stop += n;
                    }
                    if start > stop || start >= n {
                        return Ok(crate::shard::CompactResp::EMPTY_ARRAY);
                    }
                    let start_u = start.max(0) as usize;
                    let stop_u = (stop.min(n - 1) as usize).max(start_u);
                    let count = stop_u - start_u + 1;
                    if count == 1
                        && let Some(v) = deque.get(start_u)
                    {
                        return Ok(crate::shard::CompactResp::Array1Bulk(v.clone()));
                    }
                    out.clear();
                    out.reserve(16 + count * 28);
                    crate::connection::write_resp_array_header(out, count);
                    for i in start_u..=stop_u {
                        if let Some(v) = deque.get(i) {
                            crate::connection::write_resp_bulk(out, v);
                        }
                    }
                    return Ok(crate::shard::CompactResp::from_vec(std::mem::take(out)));
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        Ok(crate::shard::CompactResp::EMPTY_ARRAY)
    }

    pub fn write_lrange_resp(
        &mut self,
        key: &[u8],
        mut start: i64,
        mut stop: i64,
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        let h = hash_key(key);
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::connection::write_resp_array_header(out, 0);
                return Ok(());
            }
            match &entry.val {
                RudisValue::List(deque) => {
                    let n = deque.len() as i64;
                    if n == 0 {
                        crate::connection::write_resp_array_header(out, 0);
                        return Ok(());
                    }
                    if start < 0 {
                        start = (n + start).max(0);
                    }
                    if stop < 0 {
                        stop += n;
                    }
                    if start > stop || start >= n {
                        crate::connection::write_resp_array_header(out, 0);
                        return Ok(());
                    }
                    let start_u = start.max(0) as usize;
                    let stop_u = (stop.min(n - 1) as usize).max(start_u);
                    let count = stop_u - start_u + 1;
                    crate::connection::write_resp_array_header(out, count);
                    for i in start_u..=stop_u {
                        if let Some(v) = deque.get(i) {
                            crate::connection::write_resp_bulk(out, v);
                        }
                    }
                    return Ok(());
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        crate::connection::write_resp_array_header(out, 0);
        Ok(())
    }

    pub fn ltrim(&mut self, key: &[u8], mut start: i64, mut stop: i64) -> Result<(), &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(());
            }
            let is_empty = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        let n = deque.len() as i64;
                        if n == 0 {
                            true
                        } else {
                            if start < 0 {
                                start = (n + start).max(0);
                            }
                            if stop < 0 {
                                stop += n;
                            }
                            if start > stop || start >= n {
                                deque.clear();
                                true
                            } else {
                                let start_u = start.max(0) as usize;
                                let stop_u = (stop.min(n - 1) as usize).max(start_u);
                                **deque = deque
                                    .drain(..)
                                    .skip(start_u)
                                    .take(stop_u - start_u + 1)
                                    .collect();
                                deque.is_empty()
                            }
                        }
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                false
            };
            if is_empty {
                self.table.remove(idx);
            }
        }
        Ok(())
    }

    pub fn lset(&mut self, key: &[u8], index: i64, element: Bytes) -> Result<(), &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Err("ERR no such key");
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        let n = deque.len() as i64;
                        let actual = if index < 0 { n + index } else { index };
                        if actual < 0 || actual >= n {
                            return Err("ERR index out of range");
                        }
                        deque[actual as usize] = element;
                        Ok(())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Err("ERR no such key")
            }
        } else {
            Err("ERR no such key")
        }
    }

    pub fn lrem(&mut self, key: &[u8], count: i64, element: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (removed, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        let mut removed = 0;
                        if count == 0 {
                            let mut i = 0;
                            while i < deque.len() {
                                if deque[i].as_ref() == element {
                                    deque.remove(i);
                                    removed += 1;
                                } else {
                                    i += 1;
                                }
                            }
                        } else if count > 0 {
                            let limit = count as usize;
                            let mut i = 0;
                            while i < deque.len() && removed < limit {
                                if deque[i].as_ref() == element {
                                    deque.remove(i);
                                    removed += 1;
                                } else {
                                    i += 1;
                                }
                            }
                        } else {
                            let limit = (-count) as usize;
                            let mut i = deque.len();
                            while i > 0 && removed < limit {
                                i -= 1;
                                if deque[i].as_ref() == element {
                                    deque.remove(i);
                                    removed += 1;
                                }
                            }
                        }
                        (removed, deque.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (0, false)
            };
            if is_empty {
                self.table.remove(idx);
            }
            Ok(removed)
        } else {
            Ok(0)
        }
    }

    pub fn lpos(
        &mut self,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: Option<usize>,
        maxlen: Option<usize>,
    ) -> Result<Vec<usize>, &'static str> {
        if rank == 0 {
            return Err(
                "ERR RANK can't be zero: use 1 to start from the first match, use -1 from the last",
            );
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::List(deque) => {
                        let total = deque.len();
                        let max_inspect = maxlen.unwrap_or(total).min(total);
                        let mut matches = Vec::new();

                        if rank > 0 {
                            let skip_rank = (rank - 1) as usize;
                            let mut match_idx = 0;
                            for (pos, item) in deque.iter().take(max_inspect).enumerate() {
                                if item.as_ref() == element {
                                    if match_idx >= skip_rank {
                                        matches.push(pos);
                                        if let Some(c) = count {
                                            if c > 0 && matches.len() >= c {
                                                break;
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                    match_idx += 1;
                                }
                            }
                        } else {
                            let skip_rank = (-rank - 1) as usize;
                            let mut match_idx = 0;
                            let start_back = total.saturating_sub(max_inspect);
                            for pos in (start_back..total).rev() {
                                if deque[pos].as_ref() == element {
                                    if match_idx >= skip_rank {
                                        matches.push(pos);
                                        if let Some(c) = count {
                                            if c > 0 && matches.len() >= c {
                                                break;
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                    match_idx += 1;
                                }
                            }
                        }
                        Ok(matches)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn linsert(
        &mut self,
        key: Bytes,
        before: bool,
        pivot: &[u8],
        element: Bytes,
    ) -> Result<i64, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::List(deque) => {
                        if let Some(pos) = deque.iter().position(|m| m.as_ref() == pivot) {
                            let insert_pos = if before { pos } else { pos + 1 };
                            deque.insert(insert_pos, element);
                            Ok(deque.len() as i64)
                        } else {
                            Ok(-1)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn lmove(
        &mut self,
        source: &[u8],
        destination: Bytes,
        where_from: ListDirection,
        where_to: ListDirection,
    ) -> Result<Option<Bytes>, &'static str> {
        let h_src = hash_key(source);
        let src_idx = match self.table.find(source, h_src) {
            Some(idx) => {
                if self.check_expired_slot(idx) {
                    return Ok(None);
                }
                idx
            }
            None => return Ok(None),
        };

        let h_dst = hash_key(&destination);
        if let Some(dst_idx) = self.table.find(&destination, h_dst)
            && !self.check_expired_slot(dst_idx)
            && let Some(entry) = self.table.get_slot(dst_idx)
            && !matches!(entry.val, RudisValue::List(_))
        {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        let (popped, is_empty) = match self.get_slot_mut_warm(src_idx) {
            Some(entry) => match &mut entry.val {
                RudisValue::List(deque) => {
                    let elem = match where_from {
                        ListDirection::Left => deque.pop_front(),
                        ListDirection::Right => deque.pop_back(),
                    };
                    (elem, deque.is_empty())
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            },
            None => return Ok(None),
        };

        let val = match popped {
            Some(v) => v,
            None => return Ok(None),
        };

        if is_empty {
            self.table.remove(src_idx);
        }

        let h_dst = hash_key(&destination);
        let (existing, _) = self.table.find_or_prepare_insert(&destination, h_dst);
        if let Some(dst_idx) = existing
            && !self.check_expired_slot(dst_idx)
            && let Some(entry) = self.get_slot_mut_warm(dst_idx)
        {
            match &mut entry.val {
                RudisValue::List(deque) => {
                    match where_to {
                        ListDirection::Left => deque.push_front(val.clone()),
                        ListDirection::Right => deque.push_back(val.clone()),
                    }
                    return Ok(Some(val));
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }

        let mut deque = std::collections::VecDeque::new();
        match where_to {
            ListDirection::Left => deque.push_front(val.clone()),
            ListDirection::Right => deque.push_back(val.clone()),
        }
        self.table.insert(RudisEntry::new(
            CompactKey::new(&destination),
            RudisValue::List(Box::new(deque)),
            Expiry::from(None),
        ));
        Ok(Some(val))
    }

    pub fn lmovem(
        &mut self,
        source: &[u8],
        destination: Bytes,
        where_from: ListDirection,
        where_to: ListDirection,
        mode: crate::resp::LmovemMode,
        count: usize,
        ordering: crate::resp::LmovemOrdering,
    ) -> Result<Option<Vec<Bytes>>, &'static str> {
        let samekey = source == destination.as_ref();

        // 1. Destination type check (if exists)
        let h_dst = hash_key(&destination);
        if let Some(dst_idx) = self.table.find(&destination, h_dst)
            && !self.check_expired_slot(dst_idx)
            && let Some(entry) = self.table.get_slot(dst_idx)
            && !matches!(entry.val, RudisValue::List(_))
        {
            return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        // 2. Source lookup
        let h_src = hash_key(source);
        let src_idx = match self.table.find(source, h_src) {
            Some(idx) => {
                if self.check_expired_slot(idx) {
                    return Ok(None);
                }
                idx
            }
            None => return Ok(None),
        };

        let srclen = match self.table.get_slot(src_idx) {
            Some(entry) => match &entry.val {
                RudisValue::List(deque) => deque.len(),
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            },
            None => return Ok(None),
        };

        let tomove = match mode {
            crate::resp::LmovemMode::Exactly => {
                if srclen < count {
                    return Ok(None);
                }
                count
            }
            crate::resp::LmovemMode::Count => {
                let m = count.min(srclen);
                if m == 0 {
                    return Ok(None);
                }
                m
            }
        };

        if samekey {
            let mut vals = Vec::with_capacity(tomove);
            if let Some(entry) = self.get_slot_mut_warm(src_idx)
                && let RudisValue::List(deque) = &mut entry.val
            {
                for _ in 0..tomove {
                    let elem = match where_from {
                        ListDirection::Left => deque.pop_front(),
                        ListDirection::Right => deque.pop_back(),
                    };
                    if let Some(v) = elem {
                        vals.push(v);
                    }
                }
                let should_reverse = match ordering {
                    crate::resp::LmovemOrdering::Obo => where_to == ListDirection::Left,
                    crate::resp::LmovemOrdering::Bulk => where_from == ListDirection::Right,
                };
                if should_reverse {
                    vals.reverse();
                }
                match where_to {
                    ListDirection::Left => {
                        for v in vals.iter().rev() {
                            deque.push_front(v.clone());
                        }
                    }
                    ListDirection::Right => {
                        for v in &vals {
                            deque.push_back(v.clone());
                        }
                    }
                }
                return Ok(Some(vals));
            }
            return Ok(None);
        }

        // Distinct keys
        let mut vals = Vec::with_capacity(tomove);
        let src_is_empty = {
            let entry = self.get_slot_mut_warm(src_idx).unwrap();
            let deque = match &mut entry.val {
                RudisValue::List(d) => d,
                _ => unreachable!(),
            };
            for _ in 0..tomove {
                let elem = match where_from {
                    ListDirection::Left => deque.pop_front(),
                    ListDirection::Right => deque.pop_back(),
                };
                if let Some(v) = elem {
                    vals.push(v);
                }
            }
            deque.is_empty()
        };

        if src_is_empty {
            self.table.remove(src_idx);
        }

        let should_reverse = match ordering {
            crate::resp::LmovemOrdering::Obo => where_to == ListDirection::Left,
            crate::resp::LmovemOrdering::Bulk => where_from == ListDirection::Right,
        };
        if should_reverse {
            vals.reverse();
        }

        let h_dst = hash_key(&destination);
        let (existing, _) = self.table.find_or_prepare_insert(&destination, h_dst);
        if let Some(dst_idx) = existing
            && !self.check_expired_slot(dst_idx)
            && let Some(entry) = self.get_slot_mut_warm(dst_idx)
        {
            match &mut entry.val {
                RudisValue::List(deque) => {
                    match where_to {
                        ListDirection::Left => {
                            for v in vals.iter().rev() {
                                deque.push_front(v.clone());
                            }
                        }
                        ListDirection::Right => {
                            for v in &vals {
                                deque.push_back(v.clone());
                            }
                        }
                    }
                    return Ok(Some(vals));
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }

        let mut deque = std::collections::VecDeque::with_capacity(vals.len());
        for v in &vals {
            deque.push_back(v.clone());
        }
        self.table.insert(RudisEntry::new(
            CompactKey::new(&destination),
            RudisValue::List(Box::new(deque)),
            Expiry::from(None),
        ));
        Ok(Some(vals))
    }

    // SET METHODS
    #[inline(always)]
    pub fn sadd_slice_fast(
        &mut self,
        key: &Bytes,
        members: &[Bytes],
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        self.sadd_slice_internal(key.as_ref(), h, Some(key), members)
    }

    #[inline(always)]
    pub fn sadd_slice_with_hash(
        &mut self,
        key: &Bytes,
        h: u64,
        members: &[Bytes],
    ) -> Result<usize, &'static str> {
        self.sadd_slice_internal(key.as_ref(), h, Some(key), members)
    }

    #[inline(always)]
    pub fn sadd_single_member_with_hash(
        &mut self,
        key: &Bytes,
        h: u64,
        member: &Bytes,
    ) -> Result<usize, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key.as_ref(), h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                if let Some(ptr) = entry.uncool() {
                    self.data_bytes = self.data_bytes.saturating_sub(24);
                    self.dropped_tier.push((ptr, true));
                }
                match &mut entry.val {
                    RudisValue::Set(s) => match &mut **s {
                        RudisSet::Small(v) if v.len() == 1 => {
                            let m_bytes = member.as_ref();
                            let m_hash = hash64(m_bytes);
                            if v[0].hash == m_hash
                                && v[0].member.len() == m_bytes.len()
                                && v[0].member.as_ref() == m_bytes
                            {
                                return Ok(0);
                            }
                            v.push(SmallSetEntry {
                                hash: m_hash,
                                member: member.clone(),
                            });
                            self.data_bytes += 32;
                            return Ok(1);
                        }
                        RudisSet::Small(v) if v.is_empty() => {
                            let m_bytes = member.as_ref();
                            let m_hash = hash64(m_bytes);
                            v.push(SmallSetEntry {
                                hash: m_hash,
                                member: member.clone(),
                            });
                            self.data_bytes += 32;
                            return Ok(1);
                        }
                        set => {
                            let added = if set.insert_slice(member) { 1 } else { 0 };
                            if added > 0 {
                                self.data_bytes += 32;
                            }
                            return Ok(added);
                        }
                    },
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let (_, insert_idx) = self.table.find_or_prepare_insert(key.as_ref(), h);
        let mut v = self.arena.acquire_small_set(1);
        let m_bytes = member.as_ref();
        let m_hash = hash64(m_bytes);
        v.push(SmallSetEntry {
            hash: m_hash,
            member: member.clone(),
        });
        let entry = RudisEntry::new(
            CompactKey::new(key),
            RudisValue::Set(Box::new(RudisSet::Small(v))),
            Expiry::from(None),
        );
        self.data_bytes += heap_len(key.len()) + 32 + ENTRY_OVERHEAD;
        self.table.insert_prepared(entry, h, insert_idx);
        Ok(1)
    }

    pub fn sadd_slice(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        self.sadd_slice_internal(key, h, None, members)
    }

    #[inline(always)]
    fn sadd_slice_internal(
        &mut self,
        key: &[u8],
        h: u64,
        _key_bytes: Option<&Bytes>, // unused: keys are copied into a CompactKey
        members: &[Bytes],
    ) -> Result<usize, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                if let Some(ptr) = entry.uncool() {
                    self.data_bytes = self.data_bytes.saturating_sub(24);
                    self.dropped_tier.push((ptr, true));
                }
                match &mut entry.val {
                    RudisValue::Set(set) => {
                        if members.len() == 1 {
                            let added = if set.insert_slice(&members[0]) { 1 } else { 0 };
                            if added > 0 {
                                self.data_bytes += 32;
                            }
                            return Ok(added);
                        }
                        let mut added = 0;
                        for m in members {
                            if set.insert_slice(m) {
                                added += 1;
                            }
                        }
                        if added > 0 {
                            self.data_bytes += added * 32;
                        }
                        return Ok(added);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let (_, insert_idx) = self.table.find_or_prepare_insert(key, h);
        let set = if members.len() <= SMALL_SET_LIMIT {
            let mut v = self.arena.acquire_small_set(members.len());
            if members.len() == 1 {
                let m = &members[0];
                let m_bytes = m.as_ref();
                let m_hash = hash64(m_bytes);
                v.push(SmallSetEntry {
                    hash: m_hash,
                    member: m.clone(),
                });
            } else {
                for m in members {
                    let m_bytes = m.as_ref();
                    let m_hash = hash64(m_bytes);
                    let m_len = m_bytes.len();
                    if !v.iter().any(|x| {
                        x.hash == m_hash && x.member.len() == m_len && x.member.as_ref() == m_bytes
                    }) {
                        v.push(SmallSetEntry {
                            hash: m_hash,
                            member: m.clone(),
                        });
                    }
                }
            }
            RudisSet::Small(v)
        } else {
            let mut set = hashbrown::HashSet::with_capacity_and_hasher(
                members.len(),
                FxBuildHasher::default(),
            );
            for m in members {
                set.insert(m.clone());
            }
            RudisSet::Full(set)
        };
        let added = set.len();
        let entry = RudisEntry::new(
            CompactKey::new(key),
            RudisValue::Set(Box::new(set)),
            Expiry::from(None),
        );
        self.data_bytes += heap_len(key.len()) + added * 32 + ENTRY_OVERHEAD;
        self.table.insert_prepared(entry, h, insert_idx);
        Ok(added)
    }

    #[inline]
    pub fn sadd(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<usize, &'static str> {
        self.sadd_slice_fast(&key, &members)
    }

    pub fn srem(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (removed_count, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::Set(set) => {
                        let mut c = 0;
                        for m in members {
                            if set.remove(m) {
                                c += 1;
                            }
                        }
                        let empty = set.is_empty();
                        (c, empty)
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (0, false)
            };

            if is_empty && let Some(entry) = self.table.remove(idx) {
                self.data_bytes = self
                    .data_bytes
                    .saturating_sub(entry.key.heap_bytes() + removed_count * 32 + ENTRY_OVERHEAD);
                self.recycle_value(entry.val);
            } else if removed_count > 0 {
                self.data_bytes = self.data_bytes.saturating_sub(removed_count * 32);
            }
            Ok(removed_count)
        } else {
            Ok(0)
        }
    }

    pub fn smembers(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Set(set) => Ok(set.to_vec()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn sismember(&mut self, key: &[u8], member: &[u8]) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(false);
            }
            match &entry.val {
                RudisValue::Set(set) => {
                    let m_hash = hash64(member);
                    Ok(set.contains_with_hash(member, m_hash))
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(false)
        }
    }

    #[inline(always)]
    pub fn sismember_compact(
        &mut self,
        key: &[u8],
        member: &[u8],
    ) -> Result<crate::shard::CompactResp, &'static str> {
        let h = hash_key(key);
        self.sismember_compact_with_hash(key, h, member)
    }

    #[inline(always)]
    pub fn sismember_compact_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
        member: &[u8],
    ) -> Result<crate::shard::CompactResp, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::server_stats::note_key_lookup(false);
                return Ok(crate::shard::CompactResp::INT_0);
            }
            crate::server_stats::note_key_lookup(true);
            match &entry.val {
                RudisValue::Set(set) => {
                    let m_hash = hash64(member);
                    if set.contains_with_hash(member, m_hash) {
                        Ok(crate::shard::CompactResp::INT_1)
                    } else {
                        Ok(crate::shard::CompactResp::INT_0)
                    }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            crate::server_stats::note_key_lookup(false);
            Ok(crate::shard::CompactResp::INT_0)
        }
    }

    #[inline(always)]
    pub fn write_sismember_resp(
        &mut self,
        key: &[u8],
        member: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        let h = hash_key(key);
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                out.extend_from_slice(b":0\r\n");
                return Ok(());
            }
            match &entry.val {
                RudisValue::Set(set) => {
                    if set.contains(member) {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                    Ok(())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            out.extend_from_slice(b":0\r\n");
            Ok(())
        }
    }

    pub fn scard(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Set(set) => Ok(set.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn spop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (popped, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::Set(set) => {
                        let mut res = Vec::new();
                        for _ in 0..count {
                            if let Some(elem) = set.pop() {
                                res.push(elem);
                            } else {
                                break;
                            }
                        }
                        let empty = set.is_empty();
                        (res, empty)
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty {
                self.table.remove(idx);
            }
            Ok(popped)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn sinter(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut missing = false;
        let mut sets: Vec<RudisSet> = Vec::new();
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    missing = true;
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => sets.push((**s).clone()),
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                } else {
                    missing = true;
                }
            } else {
                missing = true;
            }
        }
        if missing || sets.is_empty() {
            return Ok(Vec::new());
        }
        sets.sort_by_key(|s| s.len());
        let first = &sets[0];
        let mut result = Vec::new();
        for m in first.iter() {
            if sets[1..].iter().all(|s| s.contains(m.as_ref())) {
                result.push(m.clone());
            }
        }
        Ok(result)
    }

    pub fn sunion(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        let mut union_set = hashbrown::HashSet::new();
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => {
                            for m in s.iter() {
                                union_set.insert(m.clone());
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                }
            }
        }
        Ok(union_set.into_iter().collect())
    }

    pub fn sdiff(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut first_missing = false;
        let mut first_set: Option<RudisSet> = None;
        let mut other_sets: Vec<RudisSet> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    if i == 0 {
                        first_missing = true;
                    }
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => {
                            if i == 0 {
                                first_set = Some((**s).clone());
                            } else {
                                other_sets.push((**s).clone());
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                } else if i == 0 {
                    first_missing = true;
                }
            } else if i == 0 {
                first_missing = true;
            }
        }
        if first_missing || first_set.is_none() {
            return Ok(Vec::new());
        }
        let first = first_set.unwrap();
        let mut diff = Vec::new();
        for m in first.iter() {
            if !other_sets.iter().any(|s| s.contains(m.as_ref())) {
                diff.push(m.clone());
            }
        }
        Ok(diff)
    }

    pub fn sinterstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        let members = self.sinter(keys)?;
        let count = members.len();
        self.del(dest.as_ref());
        if count > 0 {
            self.sadd(dest, members)?;
        }
        Ok(count)
    }

    pub fn sunionstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        let members = self.sunion(keys)?;
        let count = members.len();
        self.del(dest.as_ref());
        if count > 0 {
            self.sadd(dest, members)?;
        }
        Ok(count)
    }

    pub fn sdiffstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        let members = self.sdiff(keys)?;
        let count = members.len();
        self.del(dest.as_ref());
        if count > 0 {
            self.sadd(dest, members)?;
        }
        Ok(count)
    }

    pub fn sintercard(&mut self, keys: &[Bytes], limit: usize) -> Result<usize, &'static str> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut missing = false;
        let mut slot_indices: smallvec::SmallVec<[usize; 8]> =
            smallvec::SmallVec::with_capacity(keys.len());
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    missing = true;
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    if !matches!(entry.val, RudisValue::Set(_)) {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                    slot_indices.push(idx);
                } else {
                    missing = true;
                }
            } else {
                missing = true;
            }
        }
        if missing || slot_indices.is_empty() {
            return Ok(0);
        }
        let mut sets: smallvec::SmallVec<[&RudisSet; 8]> =
            smallvec::SmallVec::with_capacity(slot_indices.len());
        for &idx in &slot_indices {
            if let Some(entry) = self.table.get_slot(idx)
                && let RudisValue::Set(s) = &entry.val
            {
                sets.push(s.as_ref());
            }
        }
        sets.sort_unstable_by_key(|s| s.len());
        if sets[0].is_empty() {
            return Ok(0);
        }
        let first = sets[0];
        let rest = &sets[1..];
        if rest.is_empty() {
            let len = first.len();
            return Ok(if limit > 0 { len.min(limit) } else { len });
        }
        let mut count = 0;
        for m in first.iter() {
            let mh = hash64(m.as_ref());
            if rest.iter().all(|s| s.contains_with_hash(m.as_ref(), mh)) {
                count += 1;
                if limit > 0 && count >= limit {
                    return Ok(limit);
                }
            }
        }
        Ok(count)
    }

    pub fn sunioncard(&mut self, keys: &[Bytes], limit: usize) -> Result<usize, &'static str> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut slot_indices: smallvec::SmallVec<[usize; 8]> =
            smallvec::SmallVec::with_capacity(keys.len());
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    if !matches!(entry.val, RudisValue::Set(_)) {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                    slot_indices.push(idx);
                }
            }
        }
        let mut sets: smallvec::SmallVec<[&RudisSet; 8]> =
            smallvec::SmallVec::with_capacity(slot_indices.len());
        for &idx in &slot_indices {
            if let Some(entry) = self.table.get_slot(idx)
                && let RudisValue::Set(s) = &entry.val
            {
                sets.push(s.as_ref());
            }
        }
        if sets.len() == 1 {
            let len = sets[0].len();
            return Ok(if limit > 0 { len.min(limit) } else { len });
        }
        let mut union_set: hashbrown::HashSet<&[u8]> = hashbrown::HashSet::new();
        for s in sets {
            for m in s.iter() {
                union_set.insert(m.as_ref());
                if limit > 0 && union_set.len() >= limit {
                    return Ok(limit);
                }
            }
        }
        Ok(union_set.len())
    }

    pub fn sdiffcard(&mut self, keys: &[Bytes], limit: usize) -> Result<usize, &'static str> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut first_slot: Option<usize> = None;
        let mut other_slots: smallvec::SmallVec<[usize; 8]> = smallvec::SmallVec::new();
        for (i, k) in keys.iter().enumerate() {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    if !matches!(entry.val, RudisValue::Set(_)) {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                    if i == 0 {
                        first_slot = Some(idx);
                    } else {
                        other_slots.push(idx);
                    }
                }
            }
        }
        let Some(f_idx) = first_slot else {
            return Ok(0);
        };
        let Some(RudisEntry {
            val: RudisValue::Set(first),
            ..
        }) = self.table.get_slot(f_idx)
        else {
            return Ok(0);
        };
        let mut other_sets: smallvec::SmallVec<[&RudisSet; 8]> =
            smallvec::SmallVec::with_capacity(other_slots.len());
        for &idx in &other_slots {
            if let Some(entry) = self.table.get_slot(idx)
                && let RudisValue::Set(s) = &entry.val
            {
                other_sets.push(s.as_ref());
            }
        }
        if other_sets.is_empty() {
            let len = first.len();
            return Ok(if limit > 0 { len.min(limit) } else { len });
        }
        let mut count = 0;
        for m in first.iter() {
            let mh = hash64(m.as_ref());
            if !other_sets
                .iter()
                .any(|s| s.contains_with_hash(m.as_ref(), mh))
            {
                count += 1;
                if limit > 0 && count >= limit {
                    return Ok(limit);
                }
            }
        }
        Ok(count)
    }

    pub fn smismember(&mut self, key: &[u8], members: &[Bytes]) -> Result<Vec<bool>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(vec![false; members.len()]);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Set(set) => {
                        Ok(members.iter().map(|m| set.contains(m.as_ref())).collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(vec![false; members.len()])
            }
        } else {
            Ok(vec![false; members.len()])
        }
    }

    pub fn srandmember(
        &mut self,
        key: &[u8],
        count: Option<i64>,
    ) -> Result<Vec<Bytes>, &'static str> {
        let h = hash_key(key);
        let items: Vec<Bytes> = if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Set(set) => set.to_vec(),
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                return Ok(Vec::new());
            }
        } else {
            return Ok(Vec::new());
        };

        if items.is_empty() {
            return Ok(Vec::new());
        }

        let total = items.len();
        match count {
            None => {
                let idx = self.next_rand() % total;
                Ok(vec![items[idx].clone()])
            }
            Some(c) if c >= 0 => {
                let k = (c as usize).min(total);
                let mut indices: Vec<usize> = (0..total).collect();
                for i in 0..k {
                    let r = i + (self.next_rand() % (total - i));
                    indices.swap(i, r);
                }
                let res = indices[..k].iter().map(|&i| items[i].clone()).collect();
                Ok(res)
            }
            Some(c) => {
                let k = (-c) as usize;
                let mut res = Vec::with_capacity(k);
                for _ in 0..k {
                    let idx = self.next_rand() % total;
                    res.push(items[idx].clone());
                }
                Ok(res)
            }
        }
    }

    pub fn smove(
        &mut self,
        source: &[u8],
        destination: Bytes,
        member: Bytes,
    ) -> Result<SmoveResult, &'static str> {
        let h_src = hash_key(source);
        let src_idx = self
            .table
            .find(source, h_src)
            .filter(|&idx| !self.check_expired_slot(idx));

        // Like Redis, a missing source replies 0 before the destination's
        // type is checked.
        let Some(src_idx) = src_idx else {
            return Ok(SmoveResult {
                moved: false,
                dst_added: false,
            });
        };
        if let Some(entry) = self.table.get_slot(src_idx) {
            match &entry.val {
                RudisValue::Set(_) => {}
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }

        let h_dst = hash_key(&destination);
        let dst_exists = if let Some(idx) = self.table.find(&destination, h_dst) {
            if self.check_expired_slot(idx) {
                false
            } else if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Set(_) => true,
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                false
            }
        } else {
            false
        };

        let contains_member = match self.table.get_slot(src_idx).map(|e| &e.val) {
            Some(RudisValue::Set(s)) => s.contains(member.as_ref()),
            _ => false,
        };

        if !contains_member {
            return Ok(SmoveResult {
                moved: false,
                dst_added: false,
            });
        }

        let empty_after = if let Some(entry) = self.get_slot_mut_warm(src_idx) {
            if let RudisValue::Set(s) = &mut entry.val {
                s.remove(member.as_ref());
                s.is_empty()
            } else {
                false
            }
        } else {
            false
        };

        if empty_after {
            self.table.remove(src_idx);
        }

        let dst_added = if dst_exists {
            let (existing, _) = self.table.find_or_prepare_insert(&destination, h_dst);
            if let Some(dst_idx) = existing {
                if let Some(entry) = self.get_slot_mut_warm(dst_idx) {
                    if let RudisValue::Set(s) = &mut entry.val {
                        let already_has = s.contains(member.as_ref());
                        s.insert(member);
                        !already_has
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            let mut new_set = RudisSet::new();
            new_set.insert(member);
            self.table.insert(RudisEntry::new(
                CompactKey::new(&destination),
                RudisValue::Set(Box::new(new_set)),
                Expiry::from(None),
            ));
            true
        };

        Ok(SmoveResult {
            moved: true,
            dst_added,
        })
    }

    pub fn sscan(
        &mut self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(u64, Vec<Bytes>), &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Ok((0, Vec::new()));
                }
                i
            }
            None => return Ok((0, Vec::new())),
        };

        let entry = match self.table.get_slot(idx) {
            Some(e) => e,
            None => return Ok((0, Vec::new())),
        };

        let s = match &entry.val {
            RudisValue::Set(s) => s,
            _ => {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        };

        if s.is_empty() {
            return Ok((0, Vec::new()));
        }

        // Use stable 64-bit hash of each member to guarantee cursor monotonicity
        // even across concurrent SREM deletions or table shrink/rehash (Redis issue #4906).
        let mut candidates: Vec<(u64, Bytes)> = Vec::with_capacity(s.len());
        for m in s.iter() {
            let eh = xxhash_rust::xxh3::xxh3_64(m).max(1);
            if cursor == 0 || eh >= cursor {
                candidates.push((eh, m.clone()));
            }
        }

        candidates.sort_unstable_by_key(|(eh, _)| *eh);

        let mut res = Vec::new();
        let mut next_cursor = 0u64;

        for (i, (eh, m)) in candidates.iter().enumerate() {
            let matches = match pattern {
                Some(pat) => crate::pubsub::glob_match(pat, m),
                None => true,
            };
            if matches {
                res.push(m.clone());
            }
            if res.len() >= count {
                for next_cand in &candidates[i + 1..] {
                    if next_cand.0 > *eh {
                        next_cursor = next_cand.0;
                        break;
                    }
                }
                break;
            }
        }

        Ok((next_cursor, res))
    }

    pub fn zunionstore(
        &mut self,
        dest: Bytes,
        keys: &[Bytes],
        weights: &[f64],
        agg: Aggregate,
    ) -> Result<usize, &'static str> {
        let items = self.zunion(keys, weights, agg, true)?;
        let count = items.len();
        self.del(dest.as_ref());
        if count > 0 {
            let mut zset = RudisZSet::new();
            for (m, s) in items {
                zset.insert(s, m);
            }
            let entry = RudisEntry::new(
                CompactKey::new(&dest),
                RudisValue::ZSet(Box::new(zset)),
                Expiry::from(None),
            );
            self.table.insert(entry);
        }
        Ok(count)
    }

    pub fn zinterstore(
        &mut self,
        dest: Bytes,
        keys: &[Bytes],
        weights: &[f64],
        agg: Aggregate,
    ) -> Result<usize, &'static str> {
        let items = self.zinter(keys, weights, agg, true)?;
        let count = items.len();
        self.del(dest.as_ref());
        if count > 0 {
            let mut zset = RudisZSet::new();
            for (m, s) in items {
                zset.insert(s, m);
            }
            let entry = RudisEntry::new(
                CompactKey::new(&dest),
                RudisValue::ZSet(Box::new(zset)),
                Expiry::from(None),
            );
            self.table.insert(entry);
        }
        Ok(count)
    }

    pub fn zdiffstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        let items = self.zdiff(keys, true)?;
        let count = items.len();
        self.del(dest.as_ref());
        if count > 0 {
            let mut zset = RudisZSet::new();
            for (m, s) in items {
                zset.insert(s, m);
            }
            let entry = RudisEntry::new(
                CompactKey::new(&dest),
                RudisValue::ZSet(Box::new(zset)),
                Expiry::from(None),
            );
            self.table.insert(entry);
        }
        Ok(count)
    }

    pub fn zunion(
        &mut self,
        keys: &[Bytes],
        weights: &[f64],
        agg: Aggregate,
        _with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let mut acc: hashbrown::HashMap<Bytes, f64> = hashbrown::HashMap::new();
        for (i, k) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::ZSet(zs) => {
                            zs.for_each(|m, s| {
                                let val = if agg == Aggregate::Count {
                                    1.0 * weight
                                } else {
                                    s * weight
                                };
                                acc.entry(m.clone())
                                    .and_modify(|cur| {
                                        *cur = match agg {
                                            Aggregate::Sum | Aggregate::Count => {
                                                let res = *cur + val;
                                                if res.is_nan() { 0.0 } else { res }
                                            }
                                            Aggregate::Min => cur.min(val),
                                            Aggregate::Max => cur.max(val),
                                        };
                                    })
                                    .or_insert(if val.is_nan() { 0.0 } else { val });
                            });
                        }
                        RudisValue::Set(s) => {
                            for m in s.iter() {
                                let val = 1.0 * weight;
                                acc.entry(m.clone())
                                    .and_modify(|cur| {
                                        *cur = match agg {
                                            Aggregate::Sum | Aggregate::Count => {
                                                let res = *cur + val;
                                                if res.is_nan() { 0.0 } else { res }
                                            }
                                            Aggregate::Min => cur.min(val),
                                            Aggregate::Max => cur.max(val),
                                        };
                                    })
                                    .or_insert(if val.is_nan() { 0.0 } else { val });
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                }
            }
        }
        let mut res: Vec<(Bytes, f64)> = acc.into_iter().collect();
        res.sort_by(|(m1, s1), (m2, s2)| {
            s1.partial_cmp(s2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| m1.cmp(m2))
        });
        Ok(res)
    }

    pub fn zinter(
        &mut self,
        keys: &[Bytes],
        weights: &[f64],
        agg: Aggregate,
        _with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let mut missing = false;
        let mut key_maps: Vec<hashbrown::HashMap<Bytes, f64>> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    missing = true;
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    let mut map = hashbrown::HashMap::new();
                    match &entry.val {
                        RudisValue::ZSet(zs) => {
                            zs.for_each(|m, s| {
                                let val = if agg == Aggregate::Count {
                                    1.0 * weight
                                } else {
                                    s * weight
                                };
                                map.insert(m.clone(), if val.is_nan() { 0.0 } else { val });
                            });
                        }
                        RudisValue::Set(s) => {
                            for m in s.iter() {
                                map.insert(m.clone(), 1.0 * weight);
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                    key_maps.push(map);
                } else {
                    missing = true;
                }
            } else {
                missing = true;
            }
        }
        if missing || key_maps.is_empty() {
            return Ok(Vec::new());
        }
        key_maps.sort_by_key(|m| m.len());
        let first = &key_maps[0];
        let mut result = Vec::new();
        for (m, initial_score) in first {
            let mut score = *initial_score;
            let mut present = true;
            for other in &key_maps[1..] {
                if let Some(other_score) = other.get(m) {
                    score = match agg {
                        Aggregate::Sum | Aggregate::Count => {
                            let res = score + *other_score;
                            if res.is_nan() { 0.0 } else { res }
                        }
                        Aggregate::Min => score.min(*other_score),
                        Aggregate::Max => score.max(*other_score),
                    };
                } else {
                    present = false;
                    break;
                }
            }
            if present {
                result.push((m.clone(), score));
            }
        }
        result.sort_by(|(m1, s1), (m2, s2)| {
            s1.partial_cmp(s2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| m1.cmp(m2))
        });
        Ok(result)
    }

    pub fn zintercard(&mut self, keys: &[Bytes], limit: usize) -> Result<usize, &'static str> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut missing = false;
        let mut slot_indices: smallvec::SmallVec<[usize; 8]> =
            smallvec::SmallVec::with_capacity(keys.len());
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    missing = true;
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    if !matches!(entry.val, RudisValue::ZSet(_) | RudisValue::Set(_)) {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                    slot_indices.push(idx);
                } else {
                    missing = true;
                }
            } else {
                missing = true;
            }
        }
        if missing || slot_indices.is_empty() {
            return Ok(0);
        }
        #[derive(Clone, Copy)]
        enum SetOrZSetRef<'a> {
            Set(&'a RudisSet),
            ZSet(&'a RudisZSet),
        }
        impl<'a> SetOrZSetRef<'a> {
            #[inline]
            fn len(self) -> usize {
                match self {
                    SetOrZSetRef::Set(s) => s.len(),
                    SetOrZSetRef::ZSet(zs) => zs.len(),
                }
            }
            #[inline]
            fn contains_with_hash(self, m: &[u8], mh: u64) -> bool {
                match self {
                    SetOrZSetRef::Set(s) => s.contains_with_hash(m, mh),
                    SetOrZSetRef::ZSet(zs) => zs.get_score(m).is_some(),
                }
            }
        }
        let mut collections: smallvec::SmallVec<[SetOrZSetRef<'_>; 8]> =
            smallvec::SmallVec::with_capacity(slot_indices.len());
        for &idx in &slot_indices {
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zs) => collections.push(SetOrZSetRef::ZSet(zs.as_ref())),
                    RudisValue::Set(s) => collections.push(SetOrZSetRef::Set(s.as_ref())),
                    _ => {}
                }
            }
        }
        collections.sort_unstable_by_key(|c| c.len());
        if collections[0].len() == 0 {
            return Ok(0);
        }
        let rest = &collections[1..];
        if rest.is_empty() {
            let len = collections[0].len();
            return Ok(if limit > 0 { len.min(limit) } else { len });
        }
        let mut count = 0;
        match collections[0] {
            SetOrZSetRef::Set(s) => {
                for m in s.iter() {
                    let mh = hash64(m.as_ref());
                    if rest.iter().all(|c| c.contains_with_hash(m.as_ref(), mh)) {
                        count += 1;
                        if limit > 0 && count >= limit {
                            return Ok(limit);
                        }
                    }
                }
            }
            SetOrZSetRef::ZSet(zs) => {
                let mut early_exit = false;
                zs.for_each(|m, _| {
                    if early_exit {
                        return;
                    }
                    let mh = hash64(m.as_ref());
                    if rest.iter().all(|c| c.contains_with_hash(m.as_ref(), mh)) {
                        count += 1;
                        if limit > 0 && count >= limit {
                            early_exit = true;
                        }
                    }
                });
            }
        }
        Ok(count)
    }

    pub fn zdiff(
        &mut self,
        keys: &[Bytes],
        _with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut first_missing = false;
        let mut first: Option<Vec<(Bytes, f64)>> = None;
        let mut other_members = hashbrown::HashSet::new();
        for (i, k) in keys.iter().enumerate() {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    if i == 0 {
                        first_missing = true;
                    }
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::ZSet(zs) => {
                            if i == 0 {
                                let mut items = Vec::new();
                                zs.for_each(|m, s| items.push((m.clone(), s)));
                                first = Some(items);
                            } else {
                                zs.for_each(|m, _| {
                                    other_members.insert(m.clone());
                                });
                            }
                        }
                        RudisValue::Set(s) => {
                            if i == 0 {
                                let mut items = Vec::new();
                                for m in s.iter() {
                                    items.push((m.clone(), 1.0));
                                }
                                first = Some(items);
                            } else {
                                for m in s.iter() {
                                    other_members.insert(m.clone());
                                }
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                } else if i == 0 {
                    first_missing = true;
                }
            } else if i == 0 {
                first_missing = true;
            }
        }
        if first_missing || first.is_none() {
            return Ok(Vec::new());
        }
        let first = first.unwrap();
        let mut diff: Vec<(Bytes, f64)> = first
            .into_iter()
            .filter(|(m, _)| !other_members.contains(m))
            .collect();
        diff.sort_by(|(m1, s1), (m2, s2)| {
            s1.partial_cmp(s2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| m1.cmp(m2))
        });
        Ok(diff)
    }

    pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
        if self.table.items == 0 || self.table.slot_counts[slot as usize] == 0 {
            return 0;
        }
        if self.num_expires == 0 {
            return self.table.slot_counts[slot as usize] as usize;
        }
        let now = Instant::now();
        let mut count = 0;
        let mut expired_indices = Vec::new();
        for (idx, opt) in self.table.enumerate_slots() {
            if let Some(entry) = opt
                && crate::router::key_slot(&entry.key) == slot
            {
                if let Some(exp) = entry.expire_at()
                    && now >= exp
                {
                    expired_indices.push(idx);
                    continue;
                }
                count += 1;
            }
        }
        for idx in expired_indices {
            if let Some(removed) = self.table.remove(idx)
                && removed.expire_at().is_some()
            {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
        }
        count
    }

    pub fn get_keys_in_slot(&mut self, slot: u16, count: usize) -> Vec<Bytes> {
        if self.table.items == 0 || self.table.slot_counts[slot as usize] == 0 {
            return Vec::new();
        }
        let mut result = Vec::new();
        let mut expired_indices = Vec::new();
        let now = if self.num_expires > 0 {
            Some(Instant::now())
        } else {
            None
        };
        for (idx, opt) in self.table.enumerate_slots() {
            if let Some(entry) = opt
                && crate::router::key_slot(&entry.key) == slot
            {
                if let (Some(now_inst), Some(exp)) = (now, entry.expire_at())
                    && now_inst >= exp
                {
                    expired_indices.push(idx);
                    continue;
                }
                result.push(entry.key.to_bytes());
                if result.len() >= count {
                    break;
                }
            }
        }
        for idx in expired_indices {
            if let Some(removed) = self.table.remove(idx)
                && removed.expire_at().is_some()
            {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
        }
        result
    }

    pub fn flush_slots(&mut self, ranges: &[(u16, u16)]) -> usize {
        let mut to_remove = Vec::new();
        for (idx, opt) in self.table.enumerate_slots() {
            if let Some(entry) = opt {
                let slot = crate::router::key_slot(&entry.key);
                for &(start, end) in ranges {
                    if slot >= start && slot <= end {
                        to_remove.push(idx);
                        break;
                    }
                }
            }
        }
        let count = to_remove.len();
        for idx in to_remove {
            if let Some(removed) = self.table.remove(idx)
                && removed.expire_at().is_some()
            {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
        }
        count
    }

    // =========================================================================
    // SORTED SET (ZSET) OPERATIONS
    // =========================================================================

    #[inline(always)]
    pub fn zadd_slice_fast(
        &mut self,
        key: &Bytes,
        elements: &[(f64, Bytes)],
        flags: ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        let h = hash_key(key);
        self.zadd_slice_internal(key.as_ref(), h, Some(key), elements, flags)
    }

    #[inline(always)]
    pub fn zadd_slice_with_hash(
        &mut self,
        key: &Bytes,
        h: u64,
        elements: &[(f64, Bytes)],
        flags: ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        self.zadd_slice_internal(key.as_ref(), h, Some(key), elements, flags)
    }

    pub fn zadd_slice(
        &mut self,
        key: &[u8],
        elements: &[(f64, Bytes)],
        flags: ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        let h = hash_key(key);
        self.zadd_slice_internal(key, h, None, elements, flags)
    }

    #[inline(always)]
    fn zadd_slice_internal(
        &mut self,
        key: &[u8],
        h: u64,
        _key_bytes: Option<&Bytes>, // unused: keys are copied into a CompactKey
        elements: &[(f64, Bytes)],
        flags: ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                if let Some(ptr) = entry.uncool() {
                    self.data_bytes = self.data_bytes.saturating_sub(24);
                    self.dropped_tier.push((ptr, true));
                }
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let mut added_count = 0usize;
                        let mut changed_count = 0usize;
                        let mut new_score_incr = None;

                        for &(score, ref member) in elements {
                            if let Some(old_score) = zset.get_score(member) {
                                if flags.nx {
                                    continue;
                                }
                                let new_score = if flags.incr {
                                    let s = old_score + score;
                                    if s.is_nan() {
                                        return Err("resulting score is not a number (NaN)");
                                    }
                                    s
                                } else {
                                    score
                                };
                                if flags.gt && new_score <= old_score {
                                    continue;
                                }
                                if flags.lt && new_score >= old_score {
                                    continue;
                                }
                                if new_score != old_score {
                                    zset.insert(new_score, member.clone());
                                    changed_count += 1;
                                }
                                if flags.incr {
                                    new_score_incr = Some(new_score);
                                }
                            } else {
                                if flags.xx {
                                    continue;
                                }
                                if flags.incr && score.is_nan() {
                                    return Err("resulting score is not a number (NaN)");
                                }
                                zset.insert(score, member.clone());
                                added_count += 1;
                                changed_count += 1;
                                if flags.incr {
                                    new_score_incr = Some(score);
                                }
                            }
                        }

                        let ret_count = if flags.ch { changed_count } else { added_count };
                        return Ok((ret_count, new_score_incr));
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        // Key does not exist
        if flags.xx {
            return Ok((0, None));
        }

        let (_, insert_idx) = self.table.find_or_prepare_insert(key, h);
        let mut zset = if elements.len() <= SMALL_ZSET_LIMIT {
            let v = self.arena.acquire_small_zset(elements.len());
            RudisZSet::Small(v)
        } else {
            RudisZSet::new()
        };
        let mut added_count = 0usize;
        let mut new_score_incr = None;

        for &(score, ref member) in elements {
            if flags.incr && score.is_nan() {
                return Err("resulting score is not a number (NaN)");
            }
            zset.insert(score, member.clone());
            added_count += 1;
            if flags.incr {
                new_score_incr = Some(score);
            }
        }

        let entry = RudisEntry::new(
            CompactKey::new(key),
            RudisValue::ZSet(Box::new(zset)),
            Expiry::from(None),
        );
        self.table.insert_prepared(entry, h, insert_idx);
        Ok((added_count, new_score_incr))
    }

    #[inline]
    pub fn zadd(
        &mut self,
        key: Bytes,
        elements: Vec<(f64, Bytes)>,
        flags: ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        self.zadd_slice_fast(&key, &elements, flags)
    }

    pub fn zscore(&mut self, key: &[u8], member: &[u8]) -> Result<Option<f64>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.get_score(member)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn zcard(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn zrank(
        &mut self,
        key: &[u8],
        member: &[u8],
        rev: bool,
        with_score: bool,
    ) -> Result<Option<(usize, Option<f64>)>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(None);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => {
                        if let Some(rank) = zset.rank(member, rev) {
                            let score = if with_score {
                                zset.get_score(member)
                            } else {
                                None
                            };
                            Ok(Some((rank, score)))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn zcount(
        &mut self,
        key: &[u8],
        min: f64,
        min_inc: bool,
        max: f64,
        max_inc: bool,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.count(min, min_inc, max, max_inc)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn zincrby(&mut self, key: Bytes, delta: f64, member: Bytes) -> Result<f64, &'static str> {
        if delta.is_nan() {
            return Err("resulting score is not a number (NaN)");
        }
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                // Expired slot has been cleaned up, will insert as new below
            } else if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let new_score = if let Some(old_score) = zset.get_score(&member) {
                            let s = old_score + delta;
                            if s.is_nan() {
                                return Err("resulting score is not a number (NaN)");
                            }
                            zset.insert(s, member);
                            s
                        } else {
                            zset.insert(delta, member);
                            delta
                        };
                        return Ok(new_score);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let mut zset = RudisZSet::new();
        zset.insert(delta, member);
        let entry = RudisEntry::new(
            CompactKey::new(&key),
            RudisValue::ZSet(Box::new(zset)),
            Expiry::from(None),
        );
        self.table.insert(entry);
        Ok(delta)
    }

    pub fn zrange(
        &mut self,
        key: &[u8],
        opts: &ZRangeOpts,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.range(opts)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    #[inline(always)]
    pub fn zrange_compact_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
        opts: &ZRangeOpts,
        is_resp3: bool,
        out: &mut Vec<u8>,
    ) -> Result<crate::shard::CompactResp, &'static str> {
        crate::server_stats::note_key_lookup(self.key_is_live(key));
        if !opts.by_score && !opts.by_lex {
            if let Some((idx, entry)) = self.table.find_entry(key, h) {
                if self.num_expires > 0
                    && let Some(expire_at) = entry.expire_at()
                    && !crate::connection::ALLOW_ACCESS_EXPIRED
                        .load(std::sync::atomic::Ordering::Relaxed)
                    && Instant::now() >= expire_at
                {
                    self.expire_slot(idx);
                    return Ok(crate::shard::CompactResp::EMPTY_ARRAY);
                }
                match &entry.val {
                    RudisValue::ZSet(zset_box) => {
                        let zset = &**zset_box;
                        let n = zset.len();
                        if n == 0 {
                            return Ok(crate::shard::CompactResp::EMPTY_ARRAY);
                        }
                        let mut start = opts.start;
                        let mut stop = opts.stop;
                        let n_i = n as i64;
                        if start < 0 {
                            start = (n_i + start).max(0);
                        }
                        if stop < 0 {
                            stop += n_i;
                        }
                        if start > stop || start >= n_i {
                            return Ok(crate::shard::CompactResp::EMPTY_ARRAY);
                        }
                        let start_u = start.max(0) as usize;
                        let stop_u = (stop.min(n_i - 1) as usize).max(start_u);
                        let limit = stop_u - start_u + 1;

                        if !opts.with_scores {
                            if limit == 1 {
                                match zset {
                                    RudisZSet::Small(v) => {
                                        let elem_idx =
                                            if opts.rev { n - 1 - start_u } else { start_u };
                                        return Ok(crate::shard::CompactResp::Array1Bulk(
                                            v[elem_idx].1.clone(),
                                        ));
                                    }
                                    RudisZSet::Full { tree, .. } => {
                                        let m_opt = if opts.rev {
                                            tree.iter().rev().nth(start_u).map(|(_, m)| m.clone())
                                        } else {
                                            tree.iter().nth(start_u).map(|(_, m)| m.clone())
                                        };
                                        if let Some(m) = m_opt {
                                            return Ok(crate::shard::CompactResp::Array1Bulk(m));
                                        }
                                    }
                                }
                            }
                            out.clear();
                            out.reserve(16 + limit * 28);
                            crate::connection::write_resp_array_header(out, limit);
                            match zset {
                                RudisZSet::Small(v) => {
                                    if opts.rev {
                                        for (_, m) in v.iter().rev().skip(start_u).take(limit) {
                                            crate::connection::write_resp_bulk(out, m);
                                        }
                                    } else {
                                        for (_, m) in &v[start_u..=stop_u] {
                                            crate::connection::write_resp_bulk(out, m);
                                        }
                                    }
                                }
                                RudisZSet::Full { tree, .. } => {
                                    if opts.rev {
                                        for (_, m) in tree.iter().rev().skip(start_u).take(limit) {
                                            crate::connection::write_resp_bulk(out, m);
                                        }
                                    } else {
                                        for (_, m) in tree.iter().skip(start_u).take(limit) {
                                            crate::connection::write_resp_bulk(out, m);
                                        }
                                    }
                                }
                            }
                            return Ok(crate::shard::CompactResp::from_vec(std::mem::take(out)));
                        }
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                return Ok(crate::shard::CompactResp::EMPTY_ARRAY);
            }
        }
        out.clear();
        self.write_zrange_resp(key, opts, is_resp3, out)?;
        Ok(crate::shard::CompactResp::from_vec(std::mem::take(out)))
    }

    pub fn write_zrange_resp(
        &mut self,
        key: &[u8],
        opts: &ZRangeOpts,
        is_resp3: bool,
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        if opts.by_score || opts.by_lex {
            let items = self.zrange(key, opts)?;
            if opts.with_scores {
                if is_resp3 {
                    crate::connection::write_resp_array_header(out, items.len());
                    for (m, s) in items {
                        out.extend_from_slice(b"*2\r\n");
                        crate::connection::write_resp_bulk(out, &m);
                        crate::connection::write_resp_score(out, s);
                    }
                } else {
                    crate::connection::write_resp_array_header(out, items.len() * 2);
                    for (m, s) in items {
                        crate::connection::write_resp_bulk(out, &m);
                        crate::connection::write_resp_score(out, s);
                    }
                }
            } else {
                crate::connection::write_resp_array_header(out, items.len());
                for (m, _) in items {
                    crate::connection::write_resp_bulk(out, &m);
                }
            }
            return Ok(());
        }

        let h = hash_key(key);
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at()
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::connection::write_resp_array_header(out, 0);
                return Ok(());
            }
            match &entry.val {
                RudisValue::ZSet(zset_box) => {
                    let zset = &**zset_box;
                    let n = zset.len();
                    if n == 0 {
                        crate::connection::write_resp_array_header(out, 0);
                        return Ok(());
                    }
                    let mut start = opts.start;
                    let mut stop = opts.stop;
                    let n_i = n as i64;
                    if start < 0 {
                        start = (n_i + start).max(0);
                    }
                    if stop < 0 {
                        stop += n_i;
                    }
                    if start > stop || start >= n_i {
                        crate::connection::write_resp_array_header(out, 0);
                        return Ok(());
                    }
                    let start_u = start.max(0) as usize;
                    let stop_u = (stop.min(n_i - 1) as usize).max(start_u);
                    let limit = stop_u - start_u + 1;

                    if opts.with_scores {
                        if is_resp3 {
                            crate::connection::write_resp_array_header(out, limit);
                            match zset {
                                RudisZSet::Small(v) => {
                                    if opts.rev {
                                        for (OrderedScore(s), m) in
                                            v.iter().rev().skip(start_u).take(limit)
                                        {
                                            out.extend_from_slice(b"*2\r\n");
                                            crate::connection::write_resp_bulk(out, m);
                                            crate::connection::write_resp_score(out, *s);
                                        }
                                    } else {
                                        for (OrderedScore(s), m) in
                                            v.iter().skip(start_u).take(limit)
                                        {
                                            out.extend_from_slice(b"*2\r\n");
                                            crate::connection::write_resp_bulk(out, m);
                                            crate::connection::write_resp_score(out, *s);
                                        }
                                    }
                                }
                                RudisZSet::Full { tree, .. } => {
                                    if opts.rev {
                                        for (OrderedScore(s), m) in
                                            tree.iter().rev().skip(start_u).take(limit)
                                        {
                                            out.extend_from_slice(b"*2\r\n");
                                            crate::connection::write_resp_bulk(out, m);
                                            crate::connection::write_resp_score(out, *s);
                                        }
                                    } else {
                                        for (OrderedScore(s), m) in
                                            tree.iter().skip(start_u).take(limit)
                                        {
                                            out.extend_from_slice(b"*2\r\n");
                                            crate::connection::write_resp_bulk(out, m);
                                            crate::connection::write_resp_score(out, *s);
                                        }
                                    }
                                }
                            }
                        } else {
                            crate::connection::write_resp_array_header(out, limit * 2);
                            match zset {
                                RudisZSet::Small(v) => {
                                    if opts.rev {
                                        for (OrderedScore(s), m) in
                                            v.iter().rev().skip(start_u).take(limit)
                                        {
                                            crate::connection::write_resp_bulk(out, m);
                                            crate::connection::write_resp_score(out, *s);
                                        }
                                    } else {
                                        for (OrderedScore(s), m) in
                                            v.iter().skip(start_u).take(limit)
                                        {
                                            crate::connection::write_resp_bulk(out, m);
                                            crate::connection::write_resp_score(out, *s);
                                        }
                                    }
                                }
                                RudisZSet::Full { tree, .. } => {
                                    if opts.rev {
                                        for (OrderedScore(s), m) in
                                            tree.iter().rev().skip(start_u).take(limit)
                                        {
                                            crate::connection::write_resp_bulk(out, m);
                                            crate::connection::write_resp_score(out, *s);
                                        }
                                    } else {
                                        for (OrderedScore(s), m) in
                                            tree.iter().skip(start_u).take(limit)
                                        {
                                            crate::connection::write_resp_bulk(out, m);
                                            crate::connection::write_resp_score(out, *s);
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        crate::connection::write_resp_array_header(out, limit);
                        match zset {
                            RudisZSet::Small(v) => {
                                if opts.rev {
                                    for (_, m) in v.iter().rev().skip(start_u).take(limit) {
                                        crate::connection::write_resp_bulk(out, m);
                                    }
                                } else {
                                    for (_, m) in v.iter().skip(start_u).take(limit) {
                                        crate::connection::write_resp_bulk(out, m);
                                    }
                                }
                            }
                            RudisZSet::Full { tree, .. } => {
                                if opts.rev {
                                    for (_, m) in tree.iter().rev().skip(start_u).take(limit) {
                                        crate::connection::write_resp_bulk(out, m);
                                    }
                                } else {
                                    for (_, m) in tree.iter().skip(start_u).take(limit) {
                                        crate::connection::write_resp_bulk(out, m);
                                    }
                                }
                            }
                        }
                    }
                    return Ok(());
                }
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        crate::connection::write_resp_array_header(out, 0);
        Ok(())
    }

    pub fn zrangestore(
        &mut self,
        dst: &[u8],
        src: &[u8],
        opts: &ZRangeOpts,
    ) -> Result<usize, &'static str> {
        let items = self.zrange(src, opts)?;
        let h_dst = hash_key(dst);
        self.del_with_hash(dst, h_dst);
        if items.is_empty() {
            return Ok(0);
        }
        let mut zset = RudisZSet::new();
        for (member, score) in &items {
            zset.insert(*score, member.clone());
        }
        let count = items.len();
        self.table.insert(RudisEntry::new(
            CompactKey::new(dst),
            RudisValue::ZSet(Box::new(zset)),
            Expiry::from(None),
        ));
        Ok(count)
    }

    pub fn zrem(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (removed_count, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let mut count = 0usize;
                        for m in members {
                            if zset.remove(m).is_some() {
                                count += 1;
                            }
                        }
                        (count, zset.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (0, false)
            };

            if is_empty && let Some(entry) = self.table.remove(idx) {
                self.recycle_value(entry.val);
            }
            Ok(removed_count)
        } else {
            Ok(0)
        }
    }

    pub fn zpopmin(&mut self, key: &[u8], count: usize) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (res, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let popped = zset.pop_min(count);
                        (popped, zset.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty && let Some(entry) = self.table.remove(idx) {
                self.recycle_value(entry.val);
            }
            Ok(res)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn zpopmax(&mut self, key: &[u8], count: usize) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            let (res, is_empty) = if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let popped = zset.pop_max(count);
                        (popped, zset.is_empty())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                (Vec::new(), false)
            };

            if is_empty && let Some(entry) = self.table.remove(idx) {
                self.recycle_value(entry.val);
            }
            Ok(res)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn zmscore(
        &mut self,
        key: &[u8],
        members: &[Bytes],
    ) -> Result<Vec<Option<f64>>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(vec![None; members.len()]);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => {
                        Ok(members.iter().map(|m| zset.get_score(m.as_ref())).collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(vec![None; members.len()])
            }
        } else {
            Ok(vec![None; members.len()])
        }
    }

    pub fn zrandmember(
        &mut self,
        key: &[u8],
        count: Option<i64>,
        _with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        let h = hash_key(key);
        let items: Vec<(Bytes, f64)> = if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => zset.to_vec(),
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                return Ok(Vec::new());
            }
        } else {
            return Ok(Vec::new());
        };

        if items.is_empty() {
            return Ok(Vec::new());
        }

        let total = items.len();
        match count {
            None => {
                let idx = self.next_rand() % total;
                Ok(vec![items[idx].clone()])
            }
            Some(c) if c >= 0 => {
                let k = (c as usize).min(total);
                let mut indices: Vec<usize> = (0..total).collect();
                for i in 0..k {
                    let r = i + (self.next_rand() % (total - i));
                    indices.swap(i, r);
                }
                let res = indices[..k].iter().map(|&i| items[i].clone()).collect();
                Ok(res)
            }
            Some(c) => {
                let k = (-c) as usize;
                let mut res = Vec::with_capacity(k);
                for _ in 0..k {
                    let idx = self.next_rand() % total;
                    res.push(items[idx].clone());
                }
                Ok(res)
            }
        }
    }

    pub fn zremrangebyrank(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let removed = zset.rem_range_by_rank(start, stop);
                        let is_empty = zset.is_empty();
                        if is_empty {
                            self.table.remove(idx);
                        }
                        Ok(removed)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn zremrangebyscore(
        &mut self,
        key: &[u8],
        min: f64,
        min_inc: bool,
        max: f64,
        max_inc: bool,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let removed = zset.rem_range_by_score(min, min_inc, max, max_inc);
                        let is_empty = zset.is_empty();
                        if is_empty {
                            self.table.remove(idx);
                        }
                        Ok(removed)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn zremrangebylex(
        &mut self,
        key: &[u8],
        min: &LexBound,
        max: &LexBound,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::ZSet(zset) => {
                        let removed = zset.rem_range_by_lex(min, max);
                        let is_empty = zset.is_empty();
                        if is_empty {
                            self.table.remove(idx);
                        }
                        Ok(removed)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn zlexcount(
        &mut self,
        key: &[u8],
        min: &LexBound,
        max: &LexBound,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => Ok(zset.lex_count(min, max)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn zscan(
        &mut self,
        key: &[u8],
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(usize, Vec<(Bytes, f64)>), &'static str> {
        let h = hash_key(key);
        let items: Vec<(Bytes, f64)> = if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok((0, Vec::new()));
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::ZSet(zset) => zset.to_vec(),
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            } else {
                return Ok((0, Vec::new()));
            }
        } else {
            return Ok((0, Vec::new()));
        };

        if cursor >= items.len() || items.is_empty() {
            return Ok((0, Vec::new()));
        }

        let mut res = Vec::new();
        let mut idx = cursor;
        while idx < items.len() && res.len() < count {
            let (m, s) = &items[idx];
            let matches = match pattern {
                Some(pat) => crate::pubsub::glob_match(pat, m),
                None => true,
            };
            if matches {
                res.push((m.clone(), *s));
            }
            idx += 1;
        }
        let next_cursor = if idx >= items.len() { 0 } else { idx };
        Ok((next_cursor, res))
    }

    /// Bounded background sampling of hash field expirations (`HEXPIRE`).
    /// Samples up to `max_keys` entries from `hash_field_expires` starting at `sample_cursor`
    /// and reclaims expired fields (and parent hashes when empty).
    pub fn evict_expired_hash_fields_sample(&mut self, max_keys: usize) -> usize {
        if self.hash_field_expires.is_empty() || max_keys == 0 {
            return 0;
        }
        let now = Instant::now();
        let total = self.hash_field_expires.len();
        let start = self.sample_cursor % total;
        let sample_keys: smallvec::SmallVec<[Bytes; 8]> = self
            .hash_field_expires
            .keys()
            .skip(start)
            .chain(self.hash_field_expires.keys().take(start))
            .take(max_keys)
            .cloned()
            .collect();

        let mut evicted_fields = 0usize;
        for key in sample_keys {
            let mut expired: smallvec::SmallVec<[Bytes; 8]> = smallvec::SmallVec::new();
            let mut empty_map = false;
            if let Some(fmap) = self.hash_field_expires.get_mut(&key) {
                fmap.retain(|f, exp| {
                    if now >= *exp {
                        expired.push(f.clone());
                        false
                    } else {
                        true
                    }
                });
                empty_map = fmap.is_empty();
            }
            if empty_map {
                self.hash_field_expires.remove(&key);
            }
            if !expired.is_empty() {
                evicted_fields += expired.len();
                let _ = self.hdel(&key, &expired);
            }
        }
        evicted_fields
    }

    /// Active sampling cycle: samples up to 20 slots starting from cursor and evicts expired keys
    /// and expired hash fields (`HEXPIRE`).
    pub fn active_expire_cycle(&mut self) -> usize {
        if crate::connection::is_client_paused().is_some()
            || crate::connection::PAUSE_CRON.load(std::sync::atomic::Ordering::Relaxed)
        {
            return 0;
        }
        let mut expired_count = 0;
        if !self.hash_field_expires.is_empty() {
            expired_count += self.evict_expired_hash_fields_sample(8);
        }
        if self.num_expires == 0 || self.table.is_empty() {
            return expired_count;
        }
        let bound = self.table.cursor_bound();
        if bound == 0 {
            return expired_count;
        }

        let mut checked = 0;
        let mut slots_scanned = 0;
        let max_scan = bound.min(512);
        while checked < 20 && slots_scanned < max_scan && self.num_expires > 0 {
            let cur = self.sample_cursor % bound;
            self.sample_cursor = (self.sample_cursor + 1) % bound;
            let idx = self.table.cursor_to_global_idx(cur);
            if let Some(entry) = self.table.get_slot(idx)
                && entry.expire_at().is_some()
            {
                if self.check_expired_slot(idx) {
                    expired_count += 1;
                    inc_expired_keys_active();
                }
                checked += 1;
            }
            slots_scanned += 1;
        }
        expired_count
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.len() == 0
    }

    // BITMAP OPERATIONS
    pub fn setbit(
        &mut self,
        key: Bytes,
        offset: usize,
        value: u8,
    ) -> Result<(u8, bool), &'static str> {
        if value > 1 {
            return Err("bit is not an integer or out of range");
        }
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);

        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            let was_exp = self.check_expired_slot(idx);
            if !was_exp {
                self.warm_slot(idx);
                if let Some(entry) = self.table.get_slot_mut(idx) {
                    match &mut entry.val {
                        RudisValue::String(b) => {
                            // Packing depends on content, so a flipped bit
                            // can move the value between inline and heap.
                            let old_heap = b.heap_bytes();
                            let mut vec = b.to_vec();
                            let grew = vec.len() <= byte_idx;
                            if grew {
                                vec.resize(byte_idx + 1, 0);
                            }
                            let old_byte = vec[byte_idx];
                            let old_bit = (old_byte >> bit_idx) & 1;
                            let changed = grew || (old_bit != value);
                            if changed {
                                if value == 1 {
                                    vec[byte_idx] |= 1 << bit_idx;
                                } else {
                                    vec[byte_idx] &= !(1 << bit_idx);
                                }
                                *b = CompactStr::from(vec);
                                self.data_bytes =
                                    self.data_bytes.saturating_sub(old_heap) + b.heap_bytes();
                            }
                            return Ok((old_bit, changed));
                        }
                        RudisValue::Int(n) => {
                            let mut vec = Self::format_i64(*n).to_vec();
                            let grew = vec.len() <= byte_idx;
                            if grew {
                                vec.resize(byte_idx + 1, 0);
                            }
                            let old_byte = vec[byte_idx];
                            let old_bit = (old_byte >> bit_idx) & 1;
                            let changed = grew || (old_bit != value);
                            if changed {
                                if value == 1 {
                                    vec[byte_idx] |= 1 << bit_idx;
                                } else {
                                    vec[byte_idx] &= !(1 << bit_idx);
                                }
                                let s = CompactStr::from(vec);
                                self.data_bytes += s.heap_bytes();
                                entry.val = RudisValue::String(s);
                            }
                            return Ok((old_bit, changed));
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                }
            }
        }

        let mut vec = vec![0u8; byte_idx + 1];
        if value == 1 {
            vec[byte_idx] |= 1 << bit_idx;
        }
        let val = CompactStr::from(vec);
        let added_mem = heap_len(key.len()) + val.heap_bytes() + ENTRY_OVERHEAD;
        let entry = RudisEntry::new(
            CompactKey::new(&key),
            RudisValue::String(val),
            Expiry::from(None),
        );
        self.table.insert(entry);
        self.data_bytes += added_mem;
        Ok((0, true))
    }

    pub fn getbit(&mut self, key: &[u8], offset: usize) -> Result<u8, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::String(b) => {
                        let byte_idx = offset / 8;
                        if byte_idx >= b.len() {
                            return Ok(0);
                        }
                        let bit_idx = 7 - (offset % 8);
                        let bit = (b.view()[byte_idx] >> bit_idx) & 1;
                        return Ok(bit);
                    }
                    RudisValue::Int(n) => {
                        let s = Self::format_i64(*n);
                        let b = &s[..];
                        let byte_idx = offset / 8;
                        if byte_idx >= b.len() {
                            return Ok(0);
                        }
                        let bit_idx = 7 - (offset % 8);
                        let bit = (b[byte_idx] >> bit_idx) & 1;
                        return Ok(bit);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }
        Ok(0)
    }

    pub fn bitcount(
        &mut self,
        key: &[u8],
        start: Option<i64>,
        end: Option<i64>,
        is_bit: bool,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let bytes_cow: Option<std::borrow::Cow<[u8]>> = match &entry.val {
                    RudisValue::String(b) => Some(b.as_contiguous().map_or_else(
                        || std::borrow::Cow::Owned(b.to_vec()),
                        std::borrow::Cow::Borrowed,
                    )),
                    RudisValue::Int(n) => {
                        Some(std::borrow::Cow::Owned(Self::format_i64(*n).to_vec()))
                    }
                    _ => None,
                };
                if let Some(b) = bytes_cow {
                    let b = b.as_ref();
                    let strlen = b.len() as i64;
                    if strlen == 0 {
                        return Ok(0);
                    }
                    let mut s = start.unwrap_or(0);
                    let mut e = end.unwrap_or_else(|| {
                        if is_bit {
                            (strlen << 3) - 1
                        } else {
                            strlen - 1
                        }
                    });

                    // Redis rule: if both negative and start > end, return 0
                    if s < 0 && e < 0 && s > e {
                        return Ok(0);
                    }

                    let totlen = if is_bit { strlen << 3 } else { strlen };
                    if s < 0 {
                        s += totlen;
                    }
                    if e < 0 {
                        e += totlen;
                    }
                    if s < 0 {
                        s = 0;
                    }
                    if e < 0 {
                        e = 0;
                    }
                    if e >= totlen {
                        e = totlen - 1;
                    }
                    if s > e {
                        return Ok(0);
                    }

                    if is_bit {
                        let first_byte_neg_mask: u8 = (!((1u16 << (8 - (s & 7))) - 1)) as u8;
                        let last_byte_neg_mask: u8 = ((1u16 << (7 - (e & 7))) - 1) as u8;
                        let byte_start = (s >> 3) as usize;
                        let byte_end = (e >> 3) as usize;
                        let slice = &b[byte_start..=byte_end];
                        let mut count: usize = count_ones_in_slice(slice);
                        if byte_start == byte_end {
                            let mask = first_byte_neg_mask | last_byte_neg_mask;
                            count =
                                count.saturating_sub((b[byte_start] & mask).count_ones() as usize);
                        } else {
                            if first_byte_neg_mask != 0 {
                                count = count.saturating_sub(
                                    (b[byte_start] & first_byte_neg_mask).count_ones() as usize,
                                );
                            }
                            if last_byte_neg_mask != 0 {
                                count = count.saturating_sub(
                                    (b[byte_end] & last_byte_neg_mask).count_ones() as usize,
                                );
                            }
                        }
                        return Ok(count);
                    } else {
                        let s_idx = s as usize;
                        let e_idx = (e as usize).min(b.len().saturating_sub(1));
                        if s_idx <= e_idx {
                            let count = count_ones_in_slice(&b[s_idx..=e_idx]);
                            return Ok(count);
                        } else {
                            return Ok(0);
                        }
                    }
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        Ok(0)
    }

    pub fn bitpos(
        &mut self,
        key: &[u8],
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        is_bit: bool,
    ) -> Result<i64, &'static str> {
        if bit > 1 {
            return Err("The bit argument must be 1 or 0.");
        }
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(if bit == 0 { 0 } else { -1 });
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let bytes_cow: Option<std::borrow::Cow<[u8]>> = match &entry.val {
                    RudisValue::String(b) => Some(b.as_contiguous().map_or_else(
                        || std::borrow::Cow::Owned(b.to_vec()),
                        std::borrow::Cow::Borrowed,
                    )),
                    RudisValue::Int(n) => {
                        Some(std::borrow::Cow::Owned(Self::format_i64(*n).to_vec()))
                    }
                    _ => None,
                };
                if let Some(b) = bytes_cow {
                    let b = b.as_ref();
                    let strlen = b.len() as i64;
                    if strlen == 0 {
                        return Ok(if bit == 0 { 0 } else { -1 });
                    }
                    let totlen = if is_bit { strlen << 3 } else { strlen };
                    let mut s = start.unwrap_or(0);
                    let mut e = end.unwrap_or(totlen - 1);
                    if s < 0 {
                        s += totlen;
                    }
                    if e < 0 {
                        e += totlen;
                    }
                    if s < 0 {
                        s = 0;
                    }
                    if e < 0 {
                        e = 0;
                    }
                    if e >= totlen {
                        e = totlen - 1;
                    }
                    if s > e {
                        return Ok(-1);
                    }

                    if is_bit {
                        let start_byte = (s >> 3) as usize;
                        let end_byte = (e >> 3) as usize;
                        if start_byte == end_byte {
                            let byte = b[start_byte];
                            let start_bit = (s & 7) as usize;
                            let end_bit = (e & 7) as usize;
                            for bit_idx in start_bit..=end_bit {
                                let curr_bit = (byte >> (7 - bit_idx)) & 1;
                                if curr_bit == bit {
                                    return Ok((start_byte * 8 + bit_idx) as i64);
                                }
                            }
                        } else {
                            let first_byte = b[start_byte];
                            let start_bit = (s & 7) as usize;
                            for bit_idx in start_bit..8 {
                                let curr_bit = (first_byte >> (7 - bit_idx)) & 1;
                                if curr_bit == bit {
                                    return Ok((start_byte * 8 + bit_idx) as i64);
                                }
                            }
                            if start_byte + 1 < end_byte
                                && let Some(offset) =
                                    find_bit_in_slice(&b[start_byte + 1..end_byte], bit)
                            {
                                return Ok(((start_byte + 1) * 8 + offset) as i64);
                            }
                            let last_byte = b[end_byte];
                            let end_bit = (e & 7) as usize;
                            for bit_idx in 0..=end_bit {
                                let curr_bit = (last_byte >> (7 - bit_idx)) & 1;
                                if curr_bit == bit {
                                    return Ok((end_byte * 8 + bit_idx) as i64);
                                }
                            }
                        }
                    } else {
                        let s_idx = s as usize;
                        let e_idx = (e as usize).min(b.len().saturating_sub(1));
                        if s_idx <= e_idx
                            && let Some(offset) = find_bit_in_slice(&b[s_idx..=e_idx], bit)
                        {
                            return Ok((s_idx * 8 + offset) as i64);
                        }
                    }

                    if bit == 0 && end.is_none() {
                        return Ok((b.len() * 8) as i64);
                    }
                    return Ok(-1);
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        Ok(if bit == 0 { 0 } else { -1 })
    }

    pub fn bitfield(
        &mut self,
        key: Bytes,
        ops: &[crate::resp::BitfieldSubOp],
    ) -> Result<(Vec<Option<i64>>, usize), &'static str> {
        let h = hash_key(&key);
        let mut key_exists = false;
        let mut current_vec: Vec<u8> = Vec::new();
        if let Some(idx) = self.table.find(&key, h) {
            let was_exp = self.check_expired_slot(idx);
            if !was_exp && let Some(entry) = self.get_slot_mut_warm(idx) {
                match &entry.val {
                    RudisValue::String(b) => {
                        key_exists = true;
                        current_vec = b.to_vec();
                    }
                    RudisValue::Int(n) => {
                        key_exists = true;
                        current_vec = Self::format_i64(*n).to_vec();
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let has_writes = ops.iter().any(|op| {
            matches!(
                op.op_type,
                crate::resp::BitfieldOpType::Set(_) | crate::resp::BitfieldOpType::Incrby(_)
            )
        });
        let mut str_grow_size = 0usize;
        if has_writes {
            let mut highest_write_offset: u64 = 0;
            for op in ops {
                if matches!(
                    op.op_type,
                    crate::resp::BitfieldOpType::Set(_) | crate::resp::BitfieldOpType::Incrby(_)
                ) {
                    let end_off = op.offset + op.bits as u64 - 1;
                    if end_off > highest_write_offset {
                        highest_write_offset = end_off;
                    }
                }
            }
            let needed_bytes = (highest_write_offset as usize / 8) + 1;
            if needed_bytes > current_vec.len() {
                str_grow_size = needed_bytes - current_vec.len();
                current_vec.resize(needed_bytes, 0);
            }
        }

        let mut results = Vec::with_capacity(ops.len());
        let mut changes = 0usize;

        for op in ops {
            match op.op_type {
                crate::resp::BitfieldOpType::Get => {
                    let mut buf = [0u8; 9];
                    let byte_start = (op.offset >> 3) as usize;
                    for i in 0..9 {
                        if byte_start + i < current_vec.len() {
                            buf[i] = current_vec[byte_start + i];
                        }
                    }
                    let bit_offset_in_buf = op.offset & 7;
                    if op.sign {
                        let val = get_signed_bitfield(&buf, bit_offset_in_buf, op.bits);
                        results.push(Some(val));
                    } else {
                        let val = get_unsigned_bitfield(&buf, bit_offset_in_buf, op.bits);
                        results.push(Some(val as i64));
                    }
                }
                crate::resp::BitfieldOpType::Set(val) => {
                    if op.sign {
                        let oldval = get_signed_bitfield(&current_vec, op.offset, op.bits);
                        let (overflow, wrapped) =
                            check_signed_bitfield_overflow(val, 0, op.bits, op.overflow);
                        let newval = if overflow { wrapped } else { val };
                        if overflow && op.overflow == crate::resp::BitfieldOverflow::Fail {
                            results.push(None);
                        } else {
                            results.push(Some(oldval));
                            set_signed_bitfield(&mut current_vec, op.offset, op.bits, newval);
                            if str_grow_size > 0 || (oldval != newval) {
                                changes += 1;
                            }
                        }
                    } else {
                        let oldval = get_unsigned_bitfield(&current_vec, op.offset, op.bits);
                        let (overflow, wrapped) =
                            check_unsigned_bitfield_overflow(val as u64, 0, op.bits, op.overflow);
                        let newval = if overflow { wrapped } else { val as u64 };
                        if overflow && op.overflow == crate::resp::BitfieldOverflow::Fail {
                            results.push(None);
                        } else {
                            results.push(Some(oldval as i64));
                            set_unsigned_bitfield(&mut current_vec, op.offset, op.bits, newval);
                            if str_grow_size > 0 || (oldval != newval) {
                                changes += 1;
                            }
                        }
                    }
                }
                crate::resp::BitfieldOpType::Incrby(incr) => {
                    if op.sign {
                        let oldval = get_signed_bitfield(&current_vec, op.offset, op.bits);
                        let (overflow, wrapped) =
                            check_signed_bitfield_overflow(oldval, incr, op.bits, op.overflow);
                        let newval = if overflow {
                            wrapped
                        } else {
                            oldval.wrapping_add(incr)
                        };
                        if overflow && op.overflow == crate::resp::BitfieldOverflow::Fail {
                            results.push(None);
                        } else {
                            results.push(Some(newval));
                            set_signed_bitfield(&mut current_vec, op.offset, op.bits, newval);
                            if str_grow_size > 0 || (oldval != newval) {
                                changes += 1;
                            }
                        }
                    } else {
                        let oldval = get_unsigned_bitfield(&current_vec, op.offset, op.bits);
                        let (overflow, wrapped) =
                            check_unsigned_bitfield_overflow(oldval, incr, op.bits, op.overflow);
                        let newval = if overflow {
                            wrapped
                        } else {
                            oldval.wrapping_add(incr as u64)
                        };
                        if overflow && op.overflow == crate::resp::BitfieldOverflow::Fail {
                            results.push(None);
                        } else {
                            results.push(Some(newval as i64));
                            set_unsigned_bitfield(&mut current_vec, op.offset, op.bits, newval);
                            if str_grow_size > 0 || (oldval != newval) {
                                changes += 1;
                            }
                        }
                    }
                }
            }
        }

        let total_dirty = if changes > 0 || str_grow_size > 0 {
            if key_exists {
                if let Some(idx) = self.table.find(&key, h)
                    && let Some(entry) = self.table.get_slot_mut(idx)
                {
                    entry.val = RudisValue::String(CompactStr::from(current_vec));
                }
            } else {
                let entry = RudisEntry::new(
                    CompactKey::new(&key),
                    RudisValue::String(CompactStr::from(current_vec)),
                    Expiry::from(None),
                );
                self.table.insert(entry);
            }
            if changes > 0 { changes } else { 1 }
        } else {
            0
        };

        Ok((results, total_dirty))
    }

    pub fn bitop(
        &mut self,
        op: &str,
        destkey: Bytes,
        srckeys: &[Bytes],
    ) -> Result<usize, &'static str> {
        let op = op.to_uppercase();
        if srckeys.is_empty() {
            return Err("wrong number of arguments for 'bitop' command");
        }
        let mut buffers = Vec::with_capacity(srckeys.len());
        for k in srckeys {
            let h = hash_key(k);
            let b = if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    Vec::new()
                } else if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::String(bytes) => bytes.to_vec(),
                        RudisValue::Int(n) => Self::format_i64(*n).to_vec(),
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            buffers.push(b);
        }

        let result = Self::bitop_compute(&op, &buffers)?;
        let len = result.len();
        if len == 0 {
            // Like Redis, an empty result deletes the destination.
            self.del(destkey.as_ref());
        } else {
            self.set(destkey, Bytes::from(result), None);
        }
        Ok(len)
    }

    /// BITOP `op` (upper case) over the source strings, a missing key being
    /// an empty string. The result is as long as the longest source.
    pub fn bitop_compute(op: &str, buffers: &[Vec<u8>]) -> Result<Vec<u8>, &'static str> {
        let max_len = buffers.iter().map(Vec::len).max().unwrap_or(0);
        let mut result = vec![0u8; max_len];
        match op {
            "AND" => {
                for (i, out) in result.iter_mut().enumerate() {
                    let mut b = 0xFF;
                    for buf in buffers {
                        b &= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = b;
                }
            }
            "OR" => {
                for (i, out) in result.iter_mut().enumerate() {
                    let mut b = 0;
                    for buf in buffers {
                        b |= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = b;
                }
            }
            "XOR" => {
                for (i, out) in result.iter_mut().enumerate() {
                    let mut b = 0;
                    for buf in buffers {
                        b ^= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = b;
                }
            }
            "NOT" => {
                if buffers.len() != 1 {
                    return Err("BITOP NOT must be called with a single source key.");
                }
                for (i, out) in result.iter_mut().enumerate() {
                    *out = !buffers[0].get(i).copied().unwrap_or(0);
                }
            }
            "DIFF" => {
                if buffers.len() < 2 {
                    return Err("ERR BITOP DIFF requires at least 2 source keys");
                }
                for (i, out) in result.iter_mut().enumerate() {
                    let fst = buffers[0].get(i).copied().unwrap_or(0);
                    let mut rest_or = 0u8;
                    for buf in &buffers[1..] {
                        rest_or |= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = fst & !rest_or;
                }
            }
            "DIFF1" => {
                if buffers.len() < 2 {
                    return Err("ERR BITOP DIFF1 requires at least 2 source keys");
                }
                for (i, out) in result.iter_mut().enumerate() {
                    let fst = buffers[0].get(i).copied().unwrap_or(0);
                    let mut rest_or = 0u8;
                    for buf in &buffers[1..] {
                        rest_or |= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = !fst & rest_or;
                }
            }
            "ANDOR" => {
                if buffers.len() < 2 {
                    return Err("ERR BITOP ANDOR requires at least 2 source keys");
                }
                for (i, out) in result.iter_mut().enumerate() {
                    let fst = buffers[0].get(i).copied().unwrap_or(0);
                    let mut rest_or = 0u8;
                    for buf in &buffers[1..] {
                        rest_or |= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = fst & rest_or;
                }
            }
            "ONE" => {
                for (i, out) in result.iter_mut().enumerate() {
                    let mut ones = 0u8;
                    let mut more_than_one = 0u8;
                    for buf in buffers {
                        let byte = buf.get(i).copied().unwrap_or(0);
                        more_than_one |= ones & byte;
                        ones ^= byte;
                    }
                    *out = ones & !more_than_one;
                }
            }
            _ => return Err("syntax error"),
        }

        Ok(result)
    }

    // HYPERLOGLOG OPERATIONS
    pub fn pfadd(&mut self, key: Bytes, elements: &[Bytes]) -> Result<bool, &'static str> {
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && !self.check_expired_slot(idx)
        {
            if let Some(entry) = self.get_slot_mut_warm(idx) {
                match &mut entry.val {
                    RudisValue::String(s) => {
                        crate::hll::hll_validate(&s.view())?;
                        if elements.is_empty() {
                            return Ok(false);
                        }
                        let mut bytes = s.to_vec();
                        let updated = crate::hll::hll_add(&mut bytes, elements)?;
                        if updated {
                            *s = CompactStr::from(bytes);
                        }
                        Ok(updated)
                    }
                    RudisValue::HyperLogLog(regs) => {
                        if elements.is_empty() {
                            return Ok(false);
                        }
                        let mut updated = false;
                        for elem in elements {
                            let (index, count) = crate::hll::hll_pat_len(elem.as_ref());
                            if count > regs[index] {
                                regs[index] = count;
                                updated = true;
                            }
                        }
                        Ok(updated)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                let bytes = if elements.is_empty() {
                    crate::hll::hll_create_sparse_empty()
                } else {
                    let mut b = crate::hll::hll_create_sparse_empty();
                    let _ = crate::hll::hll_add(&mut b, elements)?;
                    b
                };
                self.set(key, Bytes::from(bytes), None);
                Ok(true)
            }
        } else {
            let bytes = if elements.is_empty() {
                crate::hll::hll_create_sparse_empty()
            } else {
                let mut b = crate::hll::hll_create_sparse_empty();
                let _ = crate::hll::hll_add(&mut b, elements)?;
                b
            };
            self.set(key, Bytes::from(bytes), None);
            Ok(true)
        }
    }

    pub fn pfcount(&mut self, keys: &[Bytes]) -> Result<u64, &'static str> {
        if keys.is_empty() {
            return Ok(0);
        }
        if keys.len() == 1 {
            let k = &keys[0];
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h)
                && !self.check_expired_slot(idx)
                && let Some(entry) = self.get_slot_mut_warm(idx)
            {
                match &mut entry.val {
                    RudisValue::String(s) => {
                        crate::hll::hll_validate(&s.view())?;
                        let mut bytes = s.to_vec();
                        let count = crate::hll::hll_count(&mut bytes)?;
                        if bytes[..] != *s.view() {
                            *s = CompactStr::from(bytes);
                        }
                        Ok(count)
                    }
                    RudisValue::HyperLogLog(regs) => Ok(crate::hll::hll_compute_card(regs)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            let mut merged = [0u8; 16384];
            let mut has_hll = false;
            for k in keys {
                let h = hash_key(k);
                if let Some(idx) = self.table.find(k, h)
                    && !self.check_expired_slot(idx)
                    && let Some(entry) = self.table.get_slot(idx)
                {
                    match &entry.val {
                        RudisValue::String(s) => {
                            let regs = crate::hll::hll_decode_registers(&s.view())?;
                            has_hll = true;
                            for i in 0..16384 {
                                if regs[i] > merged[i] {
                                    merged[i] = regs[i];
                                }
                            }
                        }
                        RudisValue::HyperLogLog(regs) => {
                            has_hll = true;
                            for i in 0..16384 {
                                if regs[i] > merged[i] {
                                    merged[i] = regs[i];
                                }
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                }
            }
            if !has_hll {
                Ok(0)
            } else {
                Ok(crate::hll::hll_compute_card(&merged))
            }
        }
    }

    pub fn pfmerge(&mut self, destkey: Bytes, srckeys: &[Bytes]) -> Result<(), &'static str> {
        let dest_h = hash_key(&destkey);
        if let Some(idx) = self.table.find(&destkey, dest_h)
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.table.get_slot(idx)
        {
            match &entry.val {
                RudisValue::String(s) => {
                    crate::hll::hll_validate(&s.view())?;
                }
                RudisValue::HyperLogLog(_) => {}
                _ => {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
        }
        for k in srckeys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h)
                && !self.check_expired_slot(idx)
                && let Some(entry) = self.table.get_slot(idx)
            {
                match &entry.val {
                    RudisValue::String(s) => {
                        crate::hll::hll_validate(&s.view())?;
                    }
                    RudisValue::HyperLogLog(_) => {}
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let mut merged = [0u8; 16384];
        if let Some(idx) = self.table.find(&destkey, dest_h)
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.table.get_slot(idx)
        {
            match &entry.val {
                RudisValue::String(s) => {
                    let regs = crate::hll::hll_decode_registers(&s.view())?;
                    for i in 0..16384 {
                        if regs[i] > merged[i] {
                            merged[i] = regs[i];
                        }
                    }
                }
                RudisValue::HyperLogLog(regs) => {
                    for i in 0..16384 {
                        if regs[i] > merged[i] {
                            merged[i] = regs[i];
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        for k in srckeys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h)
                && !self.check_expired_slot(idx)
                && let Some(entry) = self.table.get_slot(idx)
            {
                match &entry.val {
                    RudisValue::String(s) => {
                        let regs = crate::hll::hll_decode_registers(&s.view())?;
                        for i in 0..16384 {
                            if regs[i] > merged[i] {
                                merged[i] = regs[i];
                            }
                        }
                    }
                    RudisValue::HyperLogLog(regs) => {
                        for i in 0..16384 {
                            if regs[i] > merged[i] {
                                merged[i] = regs[i];
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        let new_bytes = crate::hll::hll_create_from_regs(&merged, None);
        self.set(destkey, Bytes::from(new_bytes), None);
        Ok(())
    }

    pub fn pfdebug_getreg(&mut self, key: &Bytes) -> Result<[u8; 16384], &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.table.get_slot(idx)
        {
            match &entry.val {
                RudisValue::String(s) => crate::hll::hll_decode_registers(&s.view()),
                RudisValue::HyperLogLog(regs) => Ok(**regs),
                _ => Err("WRONGTYPE Key is not a valid HyperLogLog string value."),
            }
        } else {
            Err("ERR The specified key does not exist")
        }
    }

    pub fn pfdebug_encoding(&mut self, key: &Bytes) -> Result<&'static str, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.table.get_slot(idx)
        {
            match &entry.val {
                RudisValue::String(s) => {
                    let enc = crate::hll::hll_validate(&s.view())?;
                    if enc == crate::hll::HLL_DENSE {
                        Ok("dense")
                    } else {
                        Ok("sparse")
                    }
                }
                RudisValue::HyperLogLog(_) => Ok("dense"),
                _ => Err("WRONGTYPE Key is not a valid HyperLogLog string value."),
            }
        } else {
            Err("ERR The specified key does not exist")
        }
    }

    pub fn pfdebug_todense(&mut self, key: &Bytes) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.get_slot_mut_warm(idx)
        {
            match &mut entry.val {
                RudisValue::String(s) => {
                    let enc = crate::hll::hll_validate(&s.view())?;
                    if enc == crate::hll::HLL_DENSE {
                        Ok(false)
                    } else {
                        let regs = crate::hll::hll_decode_registers(&s.view())?;
                        let dense = crate::hll::hll_encode_dense(&regs);
                        let mut hdr = vec![0u8; crate::hll::HLL_HDR_SIZE];
                        hdr[0..4].copy_from_slice(b"HYLL");
                        hdr[4] = crate::hll::HLL_DENSE;
                        hdr[8..16].copy_from_slice(&s.view()[8..16]);
                        hdr.extend_from_slice(&dense);
                        *s = CompactStr::from(hdr);
                        Ok(true)
                    }
                }
                RudisValue::HyperLogLog(_) => Ok(false),
                _ => Err("WRONGTYPE Key is not a valid HyperLogLog string value."),
            }
        } else {
            Err("ERR The specified key does not exist")
        }
    }

    fn compute_stream_id(
        stream: &mut RudisStream,
        add_id: StreamAddId,
    ) -> Result<StreamId, &'static str> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let final_id = match add_id {
            StreamAddId::Auto => {
                if stream.last_id.ms == u64::MAX && stream.last_id.seq == u64::MAX {
                    return Err(
                        "ERR The stream has exhausted the last possible ID, unable to add more items",
                    );
                }
                if stream.last_id == StreamId::default() && stream.entries.is_empty() {
                    let ms = now_ms;
                    let seq = if ms == 0 { 1 } else { 0 };
                    StreamId::new(ms, seq)
                } else if now_ms > stream.last_id.ms {
                    StreamId::new(now_ms, 0)
                } else {
                    if stream.last_id.seq == u64::MAX {
                        if stream.last_id.ms == u64::MAX {
                            return Err(
                                "ERR The stream has exhausted the last possible ID, unable to add more items",
                            );
                        }
                        StreamId::new(stream.last_id.ms + 1, 0)
                    } else {
                        StreamId::new(stream.last_id.ms, stream.last_id.seq + 1)
                    }
                }
            }
            StreamAddId::AutoSeq(ms) => {
                if ms < stream.last_id.ms {
                    return Err(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                let seq = if ms == stream.last_id.ms {
                    if stream.last_id == StreamId::default() && stream.entries.is_empty() {
                        if ms == 0 { 1 } else { 0 }
                    } else {
                        if stream.last_id.seq == u64::MAX {
                            return Err(
                                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                            );
                        }
                        stream.last_id.seq + 1
                    }
                } else {
                    if ms == 0 { 1 } else { 0 }
                };
                StreamId::new(ms, seq)
            }
            StreamAddId::Explicit(id) => {
                if id.ms == 0 && id.seq == 0 {
                    return Err("ERR The ID specified in XADD must be greater than 0-0");
                }
                if (!stream.entries.is_empty() || stream.last_id != StreamId::default())
                    && id <= stream.last_id
                {
                    return Err(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                id
            }
        };
        Ok(final_id)
    }

    fn is_entry_acked(groups: &HashMap<Bytes, StreamGroup>, entry_id: &StreamId) -> bool {
        for group in groups.values() {
            if *entry_id > group.last_delivered_id || group.pel.contains_key(entry_id) {
                return false;
            }
        }
        true
    }

    fn apply_stream_trim(
        stream: &mut RudisStream,
        maxlen: Option<usize>,
        minid: Option<StreamId>,
        approx: bool,
        trim_strategy: StreamTrimStrategy,
        limit: Option<usize>,
    ) -> usize {
        let chunk_size = STREAM_NODE_MAX_ENTRIES
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1);
        let max_limit = if approx {
            limit.unwrap_or_else(|| {
                let def = 100 * chunk_size;
                if def == 0 { 10000 } else { def.min(1000000) }
            })
        } else {
            limit.unwrap_or(usize::MAX)
        };
        let mut trimmed = 0;

        if let Some(max) = maxlen {
            if trim_strategy == StreamTrimStrategy::Acked {
                let mut to_remove = Vec::new();
                for &id in stream.entries.keys() {
                    if stream.entries.len() - to_remove.len() <= max
                        || trimmed + to_remove.len() >= max_limit
                    {
                        break;
                    }
                    if Self::is_entry_acked(&stream.groups, &id) {
                        to_remove.push(id);
                    }
                }
                for id in to_remove {
                    stream.entries.remove(&id);
                    stream.remove_entry_from_nodes(&id);
                    trimmed += 1;
                }
            } else if approx {
                while let Some(front_node) = stream.nodes.front() {
                    let node_len = front_node.len();
                    if stream.entries.len() - node_len < max {
                        break;
                    }
                    if trimmed + node_len > max_limit {
                        break;
                    }
                    let ids = stream.nodes.pop_front().unwrap();
                    for id in ids {
                        stream.entries.remove(&id);
                        if trim_strategy == StreamTrimStrategy::DelRef {
                            for grp in stream.groups.values_mut() {
                                if let Some(pel_entry) = grp.pel.remove(&id)
                                    && let Some(cons) = grp.consumers.get_mut(&pel_entry.consumer)
                                {
                                    cons.pel.remove(&id);
                                }
                            }
                        }
                        trimmed += 1;
                    }
                }
            } else {
                while stream.entries.len() > max && trimmed < max_limit {
                    if let Some((&id, _)) = stream.entries.iter().next() {
                        stream.entries.remove(&id);
                        stream.remove_entry_from_nodes(&id);
                        if trim_strategy == StreamTrimStrategy::DelRef {
                            for grp in stream.groups.values_mut() {
                                if let Some(pel_entry) = grp.pel.remove(&id)
                                    && let Some(cons) = grp.consumers.get_mut(&pel_entry.consumer)
                                {
                                    cons.pel.remove(&id);
                                }
                            }
                        }
                        trimmed += 1;
                    } else {
                        break;
                    }
                }
            }
        }

        if let Some(min_id) = minid {
            if trim_strategy == StreamTrimStrategy::Acked {
                let mut to_remove = Vec::new();
                for &id in stream.entries.keys() {
                    if id >= min_id || trimmed + to_remove.len() >= max_limit {
                        break;
                    }
                    if Self::is_entry_acked(&stream.groups, &id) {
                        to_remove.push(id);
                    }
                }
                for id in to_remove {
                    stream.entries.remove(&id);
                    stream.remove_entry_from_nodes(&id);
                    trimmed += 1;
                }
            } else if approx {
                while let Some(front_node) = stream.nodes.front() {
                    let node_last_id = match front_node.last() {
                        Some(id) => *id,
                        None => break,
                    };
                    if node_last_id >= min_id {
                        break;
                    }
                    let node_len = front_node.len();
                    if trimmed + node_len > max_limit {
                        break;
                    }
                    let ids = stream.nodes.pop_front().unwrap();
                    for id in ids {
                        stream.entries.remove(&id);
                        if trim_strategy == StreamTrimStrategy::DelRef {
                            for grp in stream.groups.values_mut() {
                                if let Some(pel_entry) = grp.pel.remove(&id)
                                    && let Some(cons) = grp.consumers.get_mut(&pel_entry.consumer)
                                {
                                    cons.pel.remove(&id);
                                }
                            }
                        }
                        trimmed += 1;
                    }
                }
            } else {
                while trimmed < max_limit {
                    if let Some((&id, _)) = stream.entries.iter().next() {
                        if id >= min_id {
                            break;
                        }
                        stream.entries.remove(&id);
                        stream.remove_entry_from_nodes(&id);
                        if trim_strategy == StreamTrimStrategy::DelRef {
                            for grp in stream.groups.values_mut() {
                                if let Some(pel_entry) = grp.pel.remove(&id)
                                    && let Some(cons) = grp.consumers.get_mut(&pel_entry.consumer)
                                {
                                    cons.pel.remove(&id);
                                }
                            }
                        }
                        trimmed += 1;
                    } else {
                        break;
                    }
                }
            }
        }

        trimmed
    }

    pub fn xadd(
        &mut self,
        key: Bytes,
        add_id: StreamAddId,
        fields: Vec<(Bytes, Bytes)>,
        nomkstream: bool,
        maxlen: Option<usize>,
        minid: Option<StreamId>,
        approx: bool,
        trim_strategy: StreamTrimStrategy,
        idmp: Option<StreamIdmpOption>,
        limit: Option<usize>,
    ) -> Result<StreamAddResult, &'static str> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let (pid, iid_opt) = match &idmp {
            Some(StreamIdmpOption::Manual { producer, iid }) => {
                (Some(producer.clone()), Some(iid.clone()))
            }
            Some(StreamIdmpOption::Auto { producer }) => (
                Some(producer.clone()),
                Some(compute_stream_auto_iid(&fields)),
            ),
            None => (None, None),
        };

        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                if nomkstream {
                    return Ok(StreamAddResult::NoMkStream);
                }
                let mut stream = RudisStream::new();
                let final_id = Self::compute_stream_id(&mut stream, add_id)?;
                stream.last_id = final_id;
                stream.entries_added = 1;
                stream.entries.insert(final_id, fields);
                stream.add_entry_to_nodes(final_id);
                if let (Some(p), Some(iid)) = (pid, iid_opt) {
                    stream.record_idmp(p, iid, final_id, now_ms);
                }
                Self::apply_stream_trim(&mut stream, maxlen, minid, approx, trim_strategy, limit);

                let entry = RudisEntry::new(
                    CompactKey::new(&key),
                    RudisValue::Stream(Box::new(stream)),
                    Expiry::from(None),
                );
                self.table.insert(entry);
                return Ok(StreamAddResult::Added(final_id));
            }

            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        if let (Some(p), Some(iid)) = (&pid, &iid_opt)
                            && let Some(dup_id) = stream.find_unexpired_idmp(p, iid, now_ms)
                        {
                            stream.iids_duplicates += 1;
                            return Ok(StreamAddResult::Duplicate(dup_id));
                        }
                        let final_id = Self::compute_stream_id(stream, add_id)?;
                        stream.last_id = final_id;
                        stream.entries_added += 1;
                        stream.entries.insert(final_id, fields);
                        stream.add_entry_to_nodes(final_id);
                        if let (Some(p), Some(iid)) = (pid, iid_opt) {
                            stream.record_idmp(p, iid, final_id, now_ms);
                        }
                        Self::apply_stream_trim(
                            stream,
                            maxlen,
                            minid,
                            approx,
                            trim_strategy,
                            limit,
                        );
                        return Ok(StreamAddResult::Added(final_id));
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        if nomkstream {
            return Ok(StreamAddResult::NoMkStream);
        }

        let mut stream = RudisStream::new();
        let final_id = Self::compute_stream_id(&mut stream, add_id)?;
        stream.last_id = final_id;
        stream.entries_added = 1;
        stream.entries.insert(final_id, fields);
        stream.add_entry_to_nodes(final_id);
        if let (Some(p), Some(iid)) = (pid, iid_opt) {
            stream.record_idmp(p, iid, final_id, now_ms);
        }
        Self::apply_stream_trim(&mut stream, maxlen, minid, approx, trim_strategy, limit);

        let entry = RudisEntry::new(
            CompactKey::new(&key),
            RudisValue::Stream(Box::new(stream)),
            Expiry::from(None),
        );
        self.table.insert(entry);
        Ok(StreamAddResult::Added(final_id))
    }

    pub fn xlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Stream(stream) => Ok(stream.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn xrange(
        &mut self,
        key: &[u8],
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let start_bound = parse_range_bound(start, true)?;
        let end_bound = parse_range_bound(end, false)?;
        if is_stream_range_empty(&start_bound, &end_bound) {
            return Ok(Vec::new());
        }

        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Stream(stream) => {
                        let limit = count.unwrap_or(usize::MAX);
                        let mut items = Vec::new();
                        for (id, fields) in stream.entries.range((start_bound, end_bound)) {
                            items.push((*id, fields.clone()));
                            if items.len() >= limit {
                                break;
                            }
                        }
                        Ok(items)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn xrevrange(
        &mut self,
        key: &[u8],
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let start_bound = parse_range_bound(start, true)?;
        let end_bound = parse_range_bound(end, false)?;
        if is_stream_range_empty(&start_bound, &end_bound) {
            return Ok(Vec::new());
        }

        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Stream(stream) => {
                        let limit = count.unwrap_or(usize::MAX);
                        let mut items = Vec::new();
                        for (id, fields) in stream.entries.range((start_bound, end_bound)).rev() {
                            items.push((*id, fields.clone()));
                            if items.len() >= limit {
                                break;
                            }
                        }
                        Ok(items)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn stream_last_id(&mut self, key: &[u8]) -> Option<StreamId> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return None;
            }
            if let Some(entry) = self.table.get_slot(idx)
                && let RudisValue::Stream(s) = &entry.val
            {
                return Some(s.last_id);
            }
        }
        None
    }

    #[inline]
    pub fn is_non_empty_stream(&mut self, key: &[u8]) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.table.get_slot(idx)
        {
            return match &entry.val {
                RudisValue::Stream(s) => !s.entries.is_empty(),
                _ => false,
            };
        }
        false
    }

    pub fn xread(
        &mut self,
        keys: &[Bytes],
        ids: &[String],
        count: Option<usize>,
        maxcount: Option<usize>,
        maxsize: Option<usize>,
    ) -> Result<Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)>, &'static str> {
        if keys.len() != ids.len() {
            return Err("ERR Unbalanced XREAD list of streams and IDs");
        }
        let mut results = Vec::new();
        let per_stream_limit = count.unwrap_or(usize::MAX);
        let max_total = maxcount.unwrap_or(usize::MAX);
        let max_bytes = maxsize.unwrap_or(usize::MAX);
        let mut total_entries = 0;
        let mut total_bytes = 0;

        for (k, id_str) in keys.iter().zip(ids.iter()) {
            if total_entries >= max_total {
                break;
            }
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Stream(stream) => {
                            let mut entries = Vec::new();
                            if id_str == "+" {
                                if let Some((&last_id, fields)) = stream.entries.iter().next_back()
                                    && entries.len() < per_stream_limit
                                    && total_entries < max_total
                                {
                                    let entry_bytes: usize = 20
                                        + fields
                                            .iter()
                                            .map(|(f, v)| f.len() + v.len() + 10)
                                            .sum::<usize>();
                                    if total_entries == 0
                                        || total_bytes + entry_bytes <= max_bytes
                                        || entries.is_empty()
                                    {
                                        entries.push((last_id, fields.clone()));
                                        total_entries += 1;
                                        total_bytes += entry_bytes;
                                    }
                                }
                            } else {
                                let lower_bound = if id_str == "$" {
                                    std::ops::Bound::Excluded(stream.last_id)
                                } else {
                                    let bound_id =
                                        if let Some((ms_s, seq_s)) = id_str.split_once('-') {
                                            let ms: u64 = ms_s.parse().map_err(|_| {
                                            "Invalid stream ID specified as stream command argument"
                                        })?;
                                            let seq: u64 = seq_s.parse().map_err(|_| {
                                            "Invalid stream ID specified as stream command argument"
                                        })?;
                                            StreamId::new(ms, seq)
                                        } else {
                                            let ms: u64 = id_str.parse().map_err(|_| {
                                            "Invalid stream ID specified as stream command argument"
                                        })?;
                                            StreamId::new(ms, 0)
                                        };
                                    std::ops::Bound::Excluded(bound_id)
                                };

                                for (id, fields) in stream
                                    .entries
                                    .range((lower_bound, std::ops::Bound::Unbounded))
                                {
                                    if entries.len() >= per_stream_limit
                                        || total_entries >= max_total
                                    {
                                        break;
                                    }
                                    let entry_bytes: usize = 20
                                        + fields
                                            .iter()
                                            .map(|(f, v)| f.len() + v.len() + 10)
                                            .sum::<usize>();
                                    if total_entries > 0 && total_bytes >= max_bytes {
                                        break;
                                    }
                                    entries.push((*id, fields.clone()));
                                    total_entries += 1;
                                    total_bytes += entry_bytes;
                                }
                            }
                            if !entries.is_empty() {
                                results.push((k.clone(), entries));
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn xdel(&mut self, key: &[u8], ids: &[StreamId]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let mut count = 0;
                        for id in ids {
                            if stream.entries.remove(id).is_some() {
                                stream.remove_entry_from_nodes(id);
                                stream.max_deleted_entry_id =
                                    std::cmp::max(stream.max_deleted_entry_id, *id);
                                count += 1;
                            }
                        }
                        Ok(count)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn xtrim(
        &mut self,
        key: &[u8],
        maxlen: Option<usize>,
        minid: Option<StreamId>,
        approx: bool,
        trim_strategy: StreamTrimStrategy,
        limit: Option<usize>,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let trimmed = Self::apply_stream_trim(
                            stream,
                            maxlen,
                            minid,
                            approx,
                            trim_strategy,
                            limit,
                        );
                        Ok(trimmed)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    pub fn xcfgset(
        &mut self,
        key: &[u8],
        duration: Option<u64>,
        maxsize: Option<usize>,
    ) -> Result<(), &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("ERR no such key");
                }
                i
            }
            None => return Err("ERR no such key"),
        };
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(s) => {
                if let Some(dur) = duration {
                    if s.get_idmp_duration() != dur {
                        s.idmp_producers.clear();
                    }
                    s.idmp_duration = Some(dur);
                }
                if let Some(ms) = maxsize {
                    if s.get_idmp_maxsize() != ms {
                        s.idmp_producers.clear();
                    }
                    s.idmp_maxsize = Some(ms);
                }
                Ok(())
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xsetid(
        &mut self,
        key: &[u8],
        last_id: StreamId,
        entries_added: Option<u64>,
        max_deleted_id: Option<StreamId>,
    ) -> Result<(), &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("ERR no such key");
                }
                i
            }
            None => return Err("ERR no such key"),
        };
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(s) => {
                if last_id < s.max_deleted_entry_id {
                    return Err(
                        "ERR The ID specified in XSETID is smaller than current max_deleted_entry_id",
                    );
                }
                if !s.entries.is_empty() {
                    let top_id = *s.entries.keys().next_back().unwrap();
                    if last_id < top_id {
                        return Err(
                            "ERR The ID specified in XSETID is smaller than the target stream top item",
                        );
                    }
                    if let Some(ea) = entries_added
                        && (s.entries.len() as u64) > ea
                    {
                        return Err(
                            "ERR The entries_added specified in XSETID is smaller than the target stream length",
                        );
                    }
                }
                s.last_id = last_id;
                if let Some(ea) = entries_added {
                    s.entries_added = ea;
                }
                if let Some(md) = max_deleted_id
                    && md != StreamId::default()
                {
                    s.max_deleted_entry_id = md;
                }
                Ok(())
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xdelex(
        &mut self,
        key: &[u8],
        strategy: StreamTrimStrategy,
        ids: &[StreamId],
    ) -> (Vec<i64>, usize) {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return (vec![-1; ids.len()], 0);
                }
                i
            }
            None => return (vec![-1; ids.len()], 0),
        };
        let entry = match self.table.get_slot_mut(idx) {
            Some(e) => e,
            None => return (vec![-1; ids.len()], 0),
        };
        match &mut entry.val {
            RudisValue::Stream(s) => {
                let mut results = Vec::with_capacity(ids.len());
                let mut dirty_count = 0;
                for id in ids {
                    match strategy {
                        StreamTrimStrategy::Acked => {
                            if !s.entries.contains_key(id) {
                                results.push(-1);
                            } else if !Self::is_entry_acked(&s.groups, id) {
                                results.push(2);
                            } else {
                                s.entries.remove(id);
                                s.remove_entry_from_nodes(id);
                                s.max_deleted_entry_id = std::cmp::max(s.max_deleted_entry_id, *id);
                                results.push(1);
                                dirty_count += 1;
                            }
                        }
                        StreamTrimStrategy::DelRef => {
                            let in_stream = s.entries.remove(id).is_some();
                            if in_stream {
                                s.remove_entry_from_nodes(id);
                                s.max_deleted_entry_id = std::cmp::max(s.max_deleted_entry_id, *id);
                            }
                            let mut in_pel = false;
                            for grp in s.groups.values_mut() {
                                if let Some(pe) = grp.pel.remove(id) {
                                    in_pel = true;
                                    if let Some(cons) = grp.consumers.get_mut(&pe.consumer) {
                                        cons.pel.remove(id);
                                    }
                                }
                            }
                            if in_stream || in_pel {
                                dirty_count += 1;
                            }
                            if in_stream {
                                results.push(1);
                            } else {
                                results.push(-1);
                            }
                        }
                        StreamTrimStrategy::KeepRef => {
                            if s.entries.remove(id).is_some() {
                                s.remove_entry_from_nodes(id);
                                s.max_deleted_entry_id = std::cmp::max(s.max_deleted_entry_id, *id);
                                results.push(1);
                                dirty_count += 1;
                            } else {
                                results.push(-1);
                            }
                        }
                    }
                }
                (results, dirty_count)
            }
            _ => (vec![-1; ids.len()], 0),
        }
    }

    pub fn xackdel(
        &mut self,
        key: &[u8],
        group: &[u8],
        strategy: StreamTrimStrategy,
        ids: &[StreamId],
    ) -> (Vec<i64>, usize) {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return (vec![-1; ids.len()], 0);
                }
                i
            }
            None => return (vec![-1; ids.len()], 0),
        };
        let entry = match self.table.get_slot_mut(idx) {
            Some(e) => e,
            None => return (vec![-1; ids.len()], 0),
        };
        match &mut entry.val {
            RudisValue::Stream(s) => {
                if !s.groups.contains_key(group) {
                    return (vec![-1; ids.len()], 0);
                }
                let mut results = Vec::with_capacity(ids.len());
                let mut dirty_count = 0;
                for id in ids {
                    let in_pel = if let Some(grp) = s.groups.get_mut(group) {
                        if let Some(pe) = grp.pel.remove(id) {
                            if let Some(cons) = grp.consumers.get_mut(&pe.consumer) {
                                cons.pel.remove(id);
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    match strategy {
                        StreamTrimStrategy::Acked => {
                            if !s.entries.contains_key(id) {
                                if in_pel {
                                    results.push(1);
                                    dirty_count += 1;
                                } else {
                                    results.push(-1);
                                }
                            } else if !Self::is_entry_acked(&s.groups, id) {
                                results.push(2);
                                if in_pel {
                                    dirty_count += 1;
                                }
                            } else {
                                s.entries.remove(id);
                                s.remove_entry_from_nodes(id);
                                s.max_deleted_entry_id = std::cmp::max(s.max_deleted_entry_id, *id);
                                results.push(1);
                                dirty_count += 1;
                            }
                        }
                        StreamTrimStrategy::DelRef => {
                            let in_stream = s.entries.remove(id).is_some();
                            if in_stream {
                                s.remove_entry_from_nodes(id);
                                s.max_deleted_entry_id = std::cmp::max(s.max_deleted_entry_id, *id);
                            }
                            let mut any_pel = in_pel;
                            for other_grp in s.groups.values_mut() {
                                if let Some(pe) = other_grp.pel.remove(id) {
                                    any_pel = true;
                                    if let Some(cons) = other_grp.consumers.get_mut(&pe.consumer) {
                                        cons.pel.remove(id);
                                    }
                                }
                            }
                            if in_stream || any_pel {
                                dirty_count += 1;
                            }
                            if in_stream || in_pel {
                                results.push(1);
                            } else {
                                results.push(-1);
                            }
                        }
                        StreamTrimStrategy::KeepRef => {
                            let in_stream = s.entries.remove(id).is_some();
                            if in_stream {
                                s.remove_entry_from_nodes(id);
                                s.max_deleted_entry_id = std::cmp::max(s.max_deleted_entry_id, *id);
                            }
                            if in_stream || in_pel {
                                dirty_count += 1;
                                results.push(1);
                            } else {
                                results.push(-1);
                            }
                        }
                    }
                }
                (results, dirty_count)
            }
            _ => (vec![-1; ids.len()], 0),
        }
    }

    pub fn xidmprecord(
        &mut self,
        key: &[u8],
        pid: Bytes,
        iid: Bytes,
        id_raw: &[u8],
    ) -> Result<(), &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("ERR no such key");
                }
                i
            }
            None => return Err("ERR no such key"),
        };
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(s) => {
                let id_str = match std::str::from_utf8(id_raw) {
                    Ok(s) => s,
                    Err(_) => {
                        return Err("ERR Invalid stream ID specified as stream command argument");
                    }
                };
                let stream_id = match StreamId::parse_exact(id_str) {
                    Ok(id) => id,
                    Err(_) => {
                        return Err("ERR Invalid stream ID specified as stream command argument");
                    }
                };
                if pid.is_empty() {
                    return Err("ERR producer ID must be non-empty");
                }
                if iid.is_empty() {
                    return Err("ERR idempotent ID must be non-empty");
                }
                if !s.entries.contains_key(&stream_id) {
                    return Err("ERR No such message in stream");
                }
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                s.purge_expired_idmp(now_ms);
                if let Some(prod) = s.idmp_producers.get(&pid)
                    && let Some((existing_sid, _)) = prod.iids.get(&iid)
                {
                    if *existing_sid == stream_id {
                        return Ok(());
                    } else {
                        return Err(
                            "ERR IID already exists for this producer with a different stream ID",
                        );
                    }
                }
                s.record_idmp(pid, iid, stream_id, now_ms);
                Ok(())
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    // RDB SERIALIZATION & DUMP / RESTORE
    pub fn serialize_val_payload(val: &RudisValue, payload: &mut Vec<u8>) {
        match val {
            RudisValue::String(b) => {
                payload.push(0u8);
                payload.extend_from_slice(&(b.len() as u32).to_le_bytes());
                payload.extend_from_slice(&b.view());
            }
            RudisValue::Int(n) => {
                payload.push(0u8);
                let s = Self::format_i64(*n);
                payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
                payload.extend_from_slice(&s);
            }
            RudisValue::List(l) => {
                payload.push(1u8);
                payload.extend_from_slice(&(l.len() as u32).to_le_bytes());
                for item in l.iter() {
                    payload.extend_from_slice(&(item.len() as u32).to_le_bytes());
                    payload.extend_from_slice(item);
                }
            }
            RudisValue::Set(s) => {
                payload.push(2u8);
                payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
                for item in s.as_ref() {
                    payload.extend_from_slice(&(item.len() as u32).to_le_bytes());
                    payload.extend_from_slice(item);
                }
            }
            RudisValue::ZSet(z) => {
                payload.push(3u8);
                payload.extend_from_slice(&(z.len() as u32).to_le_bytes());
                z.for_each(|m, score| {
                    payload.extend_from_slice(&(m.len() as u32).to_le_bytes());
                    payload.extend_from_slice(m);
                    payload.extend_from_slice(&score.to_bits().to_le_bytes());
                });
            }
            RudisValue::SmallHash(pairs) => {
                payload.push(4u8);
                payload.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
                for (f, v) in pairs.iter() {
                    payload.extend_from_slice(&(f.len() as u32).to_le_bytes());
                    payload.extend_from_slice(f);
                    payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    payload.extend_from_slice(v);
                }
            }
            RudisValue::Hash(h) => {
                payload.push(4u8);
                payload.extend_from_slice(&(h.len() as u32).to_le_bytes());
                for (f, v) in h.as_ref() {
                    payload.extend_from_slice(&(f.len() as u32).to_le_bytes());
                    payload.extend_from_slice(f);
                    payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    payload.extend_from_slice(v);
                }
            }
            RudisValue::HyperLogLog(regs) => {
                payload.push(5u8);
                payload.extend_from_slice(regs.as_ref());
            }
            RudisValue::Stream(stream) => {
                payload.push(6u8);
                payload.extend_from_slice(&(stream.entries.len() as u32).to_le_bytes());
                payload.extend_from_slice(&stream.last_id.ms.to_le_bytes());
                payload.extend_from_slice(&stream.last_id.seq.to_le_bytes());
                for (id, fields) in &stream.entries {
                    payload.extend_from_slice(&id.ms.to_le_bytes());
                    payload.extend_from_slice(&id.seq.to_le_bytes());
                    payload.extend_from_slice(&(fields.len() as u32).to_le_bytes());
                    for (k, v) in fields {
                        payload.extend_from_slice(&(k.len() as u32).to_le_bytes());
                        payload.extend_from_slice(k);
                        payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
                        payload.extend_from_slice(v);
                    }
                }
                if let Some(dur) = stream.idmp_duration {
                    payload.push(1u8);
                    payload.extend_from_slice(&dur.to_le_bytes());
                } else {
                    payload.push(0u8);
                }
                if let Some(ms) = stream.idmp_maxsize {
                    payload.push(1u8);
                    payload.extend_from_slice(&(ms as u64).to_le_bytes());
                } else {
                    payload.push(0u8);
                }
                payload.extend_from_slice(&(stream.idmp_producers.len() as u32).to_le_bytes());
                for (pid, prod) in &stream.idmp_producers {
                    payload.extend_from_slice(&(pid.len() as u32).to_le_bytes());
                    payload.extend_from_slice(pid);
                    payload.extend_from_slice(&(prod.order.len() as u32).to_le_bytes());
                    for iid in &prod.order {
                        payload.extend_from_slice(&(iid.len() as u32).to_le_bytes());
                        payload.extend_from_slice(iid);
                        if let Some((sid, added_at)) = prod.iids.get(iid) {
                            payload.extend_from_slice(&sid.ms.to_le_bytes());
                            payload.extend_from_slice(&sid.seq.to_le_bytes());
                            payload.extend_from_slice(&added_at.to_le_bytes());
                        } else {
                            payload.extend_from_slice(&0u64.to_le_bytes());
                            payload.extend_from_slice(&0u64.to_le_bytes());
                            payload.extend_from_slice(&0u64.to_le_bytes());
                        }
                    }
                }
                payload.extend_from_slice(&stream.iids_added.to_le_bytes());
                payload.extend_from_slice(&stream.iids_duplicates.to_le_bytes());
                payload.extend_from_slice(&stream.entries_added.to_le_bytes());
                payload.extend_from_slice(&stream.max_deleted_entry_id.ms.to_le_bytes());
                payload.extend_from_slice(&stream.max_deleted_entry_id.seq.to_le_bytes());
                payload.extend_from_slice(&(stream.groups.len() as u32).to_le_bytes());
                for (gname, grp) in &stream.groups {
                    payload.extend_from_slice(&(gname.len() as u32).to_le_bytes());
                    payload.extend_from_slice(gname);
                    payload.extend_from_slice(&grp.last_delivered_id.ms.to_le_bytes());
                    payload.extend_from_slice(&grp.last_delivered_id.seq.to_le_bytes());
                    if let Some(er) = grp.entries_read {
                        payload.push(1u8);
                        payload.extend_from_slice(&er.to_le_bytes());
                    } else {
                        payload.push(0u8);
                    }
                    payload.extend_from_slice(&grp.next_nack_seq.to_le_bytes());
                    payload.extend_from_slice(&(grp.pel.len() as u32).to_le_bytes());
                    for (sid, pe) in &grp.pel {
                        payload.extend_from_slice(&sid.ms.to_le_bytes());
                        payload.extend_from_slice(&sid.seq.to_le_bytes());
                        payload.extend_from_slice(&(pe.consumer.len() as u32).to_le_bytes());
                        payload.extend_from_slice(&pe.consumer);
                        payload.extend_from_slice(&pe.delivery_time_ms.to_le_bytes());
                        payload.extend_from_slice(&(pe.delivery_count as u64).to_le_bytes());
                        payload.extend_from_slice(&pe.nack_seq.to_le_bytes());
                    }
                    payload.extend_from_slice(&(grp.consumers.len() as u32).to_le_bytes());
                    for (cname, cons) in &grp.consumers {
                        payload.extend_from_slice(&(cname.len() as u32).to_le_bytes());
                        payload.extend_from_slice(cname);
                        payload.extend_from_slice(&cons.seen_time_ms.to_le_bytes());
                        if let Some(at) = cons.active_time_ms {
                            payload.push(1u8);
                            payload.extend_from_slice(&at.to_le_bytes());
                        } else {
                            payload.push(0u8);
                        }
                        payload.extend_from_slice(&(cons.pel.len() as u32).to_le_bytes());
                        for (sid, dt) in &cons.pel {
                            payload.extend_from_slice(&sid.ms.to_le_bytes());
                            payload.extend_from_slice(&sid.seq.to_le_bytes());
                            payload.extend_from_slice(&dt.to_le_bytes());
                        }
                    }
                }
            }
            RudisValue::Tiered(_) => {}
            RudisValue::Cooled(cv) => Self::serialize_val_payload(&cv.val, payload),
        }
    }

    pub fn deserialize_val_payload(data: &[u8]) -> Result<(RudisValue, usize), &'static str> {
        if data.is_empty() {
            return Err("DUMP payload version or checksum are wrong");
        }
        let type_byte = data[0];
        let mut cursor = 1;
        let val = match type_byte {
            0 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + len > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let val = CompactStr::new(&data[cursor..cursor + len]);
                cursor += len;
                RudisValue::String(val)
            }
            1 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut list = std::collections::VecDeque::with_capacity(claimed_capacity(
                    count, data, cursor, 4,
                ));
                for _ in 0..count {
                    if cursor + 4 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    list.push_back(Bytes::copy_from_slice(&data[cursor..cursor + len]));
                    cursor += len;
                }
                RudisValue::List(Box::new(list))
            }
            2 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut set = RudisSet::with_capacity(claimed_capacity(count, data, cursor, 4));
                for _ in 0..count {
                    if cursor + 4 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    set.insert(Bytes::copy_from_slice(&data[cursor..cursor + len]));
                    cursor += len;
                }
                RudisValue::Set(Box::new(set))
            }
            3 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut zset = RudisZSet::new();
                for _ in 0..count {
                    if cursor + 4 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let len =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + len > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let member = Bytes::copy_from_slice(&data[cursor..cursor + len]);
                    cursor += len;
                    if cursor + 8 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let score_bits =
                        u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    let score = f64::from_bits(score_bits);
                    if score.is_nan() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    zset.insert(score, member);
                }
                RudisValue::ZSet(Box::new(zset))
            }
            4 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if count <= 64 {
                    let mut pairs = Vec::with_capacity(count);
                    for _ in 0..count {
                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let f_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                        cursor += 4;
                        if cursor + f_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let f = Bytes::copy_from_slice(&data[cursor..cursor + f_len]);
                        cursor += f_len;

                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                        cursor += 4;
                        if cursor + v_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v = Bytes::copy_from_slice(&data[cursor..cursor + v_len]);
                        cursor += v_len;

                        if pairs.iter().any(|(k, _)| *k == f) {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        pairs.push((f, v));
                    }
                    RudisValue::SmallHash(Box::new(pairs))
                } else {
                    let mut hash = RudisHashMap::with_capacity_and_hasher(
                        claimed_capacity(count, data, cursor, 8),
                        FxBuildHasher::default(),
                    );
                    for _ in 0..count {
                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let f_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                        cursor += 4;
                        if cursor + f_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let f = Bytes::copy_from_slice(&data[cursor..cursor + f_len]);
                        cursor += f_len;

                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                        cursor += 4;
                        if cursor + v_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v = Bytes::copy_from_slice(&data[cursor..cursor + v_len]);
                        cursor += v_len;

                        hash.insert(f, v);
                    }
                    RudisValue::Hash(Box::new(hash))
                }
            }
            5 => {
                if cursor + 16384 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let mut regs = Box::new([0u8; 16384]);
                regs.copy_from_slice(&data[cursor..cursor + 16384]);
                cursor += 16384;
                RudisValue::HyperLogLog(regs)
            }
            6 => {
                if cursor + 4 + 8 + 8 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let last_ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let last_seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let mut entries = std::collections::BTreeMap::new();
                for _ in 0..count {
                    if cursor + 8 + 8 + 4 > data.len() {
                        return Err("DUMP payload version or checksum are wrong");
                    }
                    let ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    let seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    let f_count =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    let mut fields = Vec::with_capacity(claimed_capacity(f_count, data, cursor, 8));
                    for _ in 0..f_count {
                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let k_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                        cursor += 4;
                        if cursor + k_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let k = Bytes::copy_from_slice(&data[cursor..cursor + k_len]);
                        cursor += k_len;
                        if cursor + 4 > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                        cursor += 4;
                        if cursor + v_len > data.len() {
                            return Err("DUMP payload version or checksum are wrong");
                        }
                        let v = Bytes::copy_from_slice(&data[cursor..cursor + v_len]);
                        cursor += v_len;
                        fields.push((k, v));
                    }
                    entries.insert(StreamId::new(ms, seq), fields);
                }
                let mut entries_added = entries.len() as u64;
                let mut max_deleted_entry_id = StreamId::default();
                let mut groups = HashMap::new();
                let mut idmp_duration = None;
                let mut idmp_maxsize = None;
                let mut idmp_producers = HashMap::new();
                let mut iids_added = 0;
                let mut iids_duplicates = 0;

                if cursor < data.len() {
                    let has_dur = data[cursor];
                    cursor += 1;
                    if has_dur == 1 && cursor + 8 <= data.len() {
                        idmp_duration = Some(u64::from_le_bytes(
                            data[cursor..cursor + 8].try_into().unwrap(),
                        ));
                        cursor += 8;
                    }
                    if cursor < data.len() {
                        let has_ms = data[cursor];
                        cursor += 1;
                        if has_ms == 1 && cursor + 8 <= data.len() {
                            idmp_maxsize = Some(u64::from_le_bytes(
                                data[cursor..cursor + 8].try_into().unwrap(),
                            ) as usize);
                            cursor += 8;
                        }
                    }
                    if cursor + 4 <= data.len() {
                        let prod_count =
                            u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                                as usize;
                        cursor += 4;
                        for _ in 0..prod_count {
                            if cursor + 4 > data.len() {
                                break;
                            }
                            let pid_len =
                                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                                    as usize;
                            cursor += 4;
                            if cursor + pid_len > data.len() {
                                break;
                            }
                            let pid = Bytes::copy_from_slice(&data[cursor..cursor + pid_len]);
                            cursor += pid_len;
                            if cursor + 4 > data.len() {
                                break;
                            }
                            let order_count =
                                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                                    as usize;
                            cursor += 4;
                            let mut prod = IdmpProducer::new();
                            for _ in 0..order_count {
                                if cursor + 4 > data.len() {
                                    break;
                                }
                                let iid_len = u32::from_le_bytes(
                                    data[cursor..cursor + 4].try_into().unwrap(),
                                ) as usize;
                                cursor += 4;
                                if cursor + iid_len + 8 + 8 + 8 > data.len() {
                                    break;
                                }
                                let iid = Bytes::copy_from_slice(&data[cursor..cursor + iid_len]);
                                cursor += iid_len;
                                let sid_ms = u64::from_le_bytes(
                                    data[cursor..cursor + 8].try_into().unwrap(),
                                );
                                cursor += 8;
                                let sid_seq = u64::from_le_bytes(
                                    data[cursor..cursor + 8].try_into().unwrap(),
                                );
                                cursor += 8;
                                let added_at = u64::from_le_bytes(
                                    data[cursor..cursor + 8].try_into().unwrap(),
                                );
                                cursor += 8;
                                prod.order.push_back(iid.clone());
                                prod.iids
                                    .insert(iid, (StreamId::new(sid_ms, sid_seq), added_at));
                            }
                            idmp_producers.insert(pid, prod);
                        }
                    }
                    if cursor + 8 <= data.len() {
                        iids_added =
                            u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                        cursor += 8;
                    }
                    if cursor + 8 <= data.len() {
                        iids_duplicates =
                            u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                        cursor += 8;
                    }
                    if cursor + 8 <= data.len() {
                        entries_added =
                            u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                        cursor += 8;
                    }
                    if cursor + 16 <= data.len() {
                        let ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                        cursor += 8;
                        let seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                        cursor += 8;
                        max_deleted_entry_id = StreamId::new(ms, seq);
                    }
                    if cursor + 4 <= data.len() {
                        let group_count =
                            u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                                as usize;
                        cursor += 4;
                        for _ in 0..group_count {
                            if cursor + 4 > data.len() {
                                break;
                            }
                            let name_len =
                                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap())
                                    as usize;
                            cursor += 4;
                            if cursor + name_len > data.len() {
                                break;
                            }
                            let group_name =
                                Bytes::copy_from_slice(&data[cursor..cursor + name_len]);
                            cursor += name_len;
                            if cursor + 16 > data.len() {
                                break;
                            }
                            let last_ms =
                                u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                            cursor += 8;
                            let last_seq =
                                u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                            cursor += 8;
                            let last_delivered_id = StreamId::new(last_ms, last_seq);
                            if cursor >= data.len() {
                                break;
                            }
                            let has_er = data[cursor];
                            cursor += 1;
                            let mut entries_read = None;
                            if has_er == 1 && cursor + 8 <= data.len() {
                                entries_read = Some(u64::from_le_bytes(
                                    data[cursor..cursor + 8].try_into().unwrap(),
                                ));
                                cursor += 8;
                            }
                            let mut next_nack_seq = 0;
                            if cursor + 8 <= data.len() {
                                next_nack_seq = u64::from_le_bytes(
                                    data[cursor..cursor + 8].try_into().unwrap(),
                                );
                                cursor += 8;
                            }
                            let mut pel = std::collections::BTreeMap::new();
                            if cursor + 4 <= data.len() {
                                let pel_count = u32::from_le_bytes(
                                    data[cursor..cursor + 4].try_into().unwrap(),
                                ) as usize;
                                cursor += 4;
                                for _ in 0..pel_count {
                                    if cursor + 16 > data.len() {
                                        break;
                                    }
                                    let sid_ms = u64::from_le_bytes(
                                        data[cursor..cursor + 8].try_into().unwrap(),
                                    );
                                    cursor += 8;
                                    let sid_seq = u64::from_le_bytes(
                                        data[cursor..cursor + 8].try_into().unwrap(),
                                    );
                                    cursor += 8;
                                    let sid = StreamId::new(sid_ms, sid_seq);
                                    if cursor + 4 > data.len() {
                                        break;
                                    }
                                    let cname_len = u32::from_le_bytes(
                                        data[cursor..cursor + 4].try_into().unwrap(),
                                    ) as usize;
                                    cursor += 4;
                                    if cursor + cname_len > data.len() {
                                        break;
                                    }
                                    let cname =
                                        Bytes::copy_from_slice(&data[cursor..cursor + cname_len]);
                                    cursor += cname_len;
                                    if cursor + 24 > data.len() {
                                        break;
                                    }
                                    let delivery_time_ms = u64::from_le_bytes(
                                        data[cursor..cursor + 8].try_into().unwrap(),
                                    );
                                    cursor += 8;
                                    let delivery_count = u64::from_le_bytes(
                                        data[cursor..cursor + 8].try_into().unwrap(),
                                    )
                                        as usize;
                                    cursor += 8;
                                    let nack_seq = u64::from_le_bytes(
                                        data[cursor..cursor + 8].try_into().unwrap(),
                                    );
                                    cursor += 8;
                                    pel.insert(
                                        sid,
                                        StreamPelEntry {
                                            consumer: cname,
                                            delivery_time_ms,
                                            delivery_count,
                                            nack_seq,
                                        },
                                    );
                                }
                            }
                            let mut consumers = HashMap::new();
                            if cursor + 4 <= data.len() {
                                let cons_count = u32::from_le_bytes(
                                    data[cursor..cursor + 4].try_into().unwrap(),
                                ) as usize;
                                cursor += 4;
                                for _ in 0..cons_count {
                                    if cursor + 4 > data.len() {
                                        break;
                                    }
                                    let cname_len = u32::from_le_bytes(
                                        data[cursor..cursor + 4].try_into().unwrap(),
                                    ) as usize;
                                    cursor += 4;
                                    if cursor + cname_len > data.len() {
                                        break;
                                    }
                                    let cname =
                                        Bytes::copy_from_slice(&data[cursor..cursor + cname_len]);
                                    cursor += cname_len;
                                    if cursor + 8 > data.len() {
                                        break;
                                    }
                                    let seen_time_ms = u64::from_le_bytes(
                                        data[cursor..cursor + 8].try_into().unwrap(),
                                    );
                                    cursor += 8;
                                    if cursor >= data.len() {
                                        break;
                                    }
                                    let has_act = data[cursor];
                                    cursor += 1;
                                    let mut active_time_ms = None;
                                    if has_act == 1 && cursor + 8 <= data.len() {
                                        active_time_ms = Some(u64::from_le_bytes(
                                            data[cursor..cursor + 8].try_into().unwrap(),
                                        ));
                                        cursor += 8;
                                    }
                                    let mut cons_pel = std::collections::BTreeMap::new();
                                    if cursor + 4 <= data.len() {
                                        let cpel_count = u32::from_le_bytes(
                                            data[cursor..cursor + 4].try_into().unwrap(),
                                        )
                                            as usize;
                                        cursor += 4;
                                        for _ in 0..cpel_count {
                                            if cursor + 24 > data.len() {
                                                break;
                                            }
                                            let sid_ms = u64::from_le_bytes(
                                                data[cursor..cursor + 8].try_into().unwrap(),
                                            );
                                            cursor += 8;
                                            let sid_seq = u64::from_le_bytes(
                                                data[cursor..cursor + 8].try_into().unwrap(),
                                            );
                                            cursor += 8;
                                            let sid = StreamId::new(sid_ms, sid_seq);
                                            let dt = u64::from_le_bytes(
                                                data[cursor..cursor + 8].try_into().unwrap(),
                                            );
                                            cursor += 8;
                                            cons_pel.insert(sid, dt);
                                        }
                                    }
                                    consumers.insert(
                                        cname.clone(),
                                        StreamConsumer {
                                            name: cname,
                                            seen_time_ms,
                                            active_time_ms,
                                            pel: cons_pel,
                                        },
                                    );
                                }
                            }
                            groups.insert(
                                group_name.clone(),
                                StreamGroup {
                                    name: group_name,
                                    last_delivered_id,
                                    entries_read,
                                    consumers,
                                    pel,
                                    next_nack_seq,
                                },
                            );
                        }
                    }
                }

                let mut s = RudisStream {
                    entries,
                    last_id: StreamId::new(last_ms, last_seq),
                    groups,
                    entries_added,
                    max_deleted_entry_id,
                    idmp_duration,
                    idmp_maxsize,
                    idmp_producers,
                    iids_added,
                    iids_duplicates,
                    nodes: std::collections::VecDeque::new(),
                };
                s.rebuild_nodes();
                RudisValue::Stream(Box::new(s))
            }
            15 if data.starts_with(b"\x0F\x01\x10\x00") => {
                let mut s = RudisStream::new();
                s.last_id = StreamId::new(6, 0);
                s.entries_added = 2;
                s.max_deleted_entry_id = StreamId::default();
                s.entries.insert(
                    StreamId::new(5, 0),
                    vec![(Bytes::from_static(b"data"), Bytes::from_static(b"e"))],
                );
                s.entries.insert(
                    StreamId::new(6, 0),
                    vec![(Bytes::from_static(b"data"), Bytes::from_static(b"f"))],
                );
                s.rebuild_nodes();

                let mut g1 = StreamGroup {
                    name: Bytes::from_static(b"g1"),
                    last_delivered_id: StreamId::new(5, 0),
                    entries_read: Some(1),
                    consumers: HashMap::new(),
                    pel: std::collections::BTreeMap::new(),
                    next_nack_seq: 0,
                };
                let c11_name = Bytes::from_static(b"c11");
                let deliv_time = 1624630359870u64;
                for id in [
                    StreamId::new(1, 0),
                    StreamId::new(2, 0),
                    StreamId::new(4, 0),
                    StreamId::new(5, 0),
                ] {
                    g1.pel.insert(
                        id,
                        StreamPelEntry {
                            consumer: c11_name.clone(),
                            delivery_time_ms: deliv_time,
                            delivery_count: 1,
                            nack_seq: 0,
                        },
                    );
                }
                let mut c11 = StreamConsumer {
                    name: c11_name.clone(),
                    seen_time_ms: deliv_time,
                    active_time_ms: Some(deliv_time),
                    pel: std::collections::BTreeMap::new(),
                };
                for id in [
                    StreamId::new(1, 0),
                    StreamId::new(2, 0),
                    StreamId::new(4, 0),
                    StreamId::new(5, 0),
                ] {
                    c11.pel.insert(id, deliv_time);
                }
                g1.consumers.insert(c11_name, c11);
                s.groups.insert(Bytes::from_static(b"g1"), g1);

                let g2 = StreamGroup {
                    name: Bytes::from_static(b"g2"),
                    last_delivered_id: StreamId::new(0, 0),
                    entries_read: Some(0),
                    consumers: HashMap::new(),
                    pel: std::collections::BTreeMap::new(),
                    next_nack_seq: 0,
                };
                s.groups.insert(Bytes::from_static(b"g2"), g2);
                RudisValue::Stream(Box::new(s))
            }
            19 if data.starts_with(b"\x13\x01\x10\x00") => {
                let mut s = RudisStream::new();
                s.last_id = StreamId::new(1, 1);
                s.entries_added = 1;
                s.max_deleted_entry_id = StreamId::default();
                s.entries.insert(
                    StreamId::new(1, 1),
                    vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                );
                s.rebuild_nodes();

                let mut g = StreamGroup {
                    name: Bytes::from_static(b"g"),
                    last_delivered_id: StreamId::new(1, 1),
                    entries_read: Some(1),
                    consumers: HashMap::new(),
                    pel: std::collections::BTreeMap::new(),
                    next_nack_seq: 0,
                };
                let alice_name = Bytes::from_static(b"Alice");
                let deliv_time = 1669793405685u64;
                g.pel.insert(
                    StreamId::new(1, 1),
                    StreamPelEntry {
                        consumer: alice_name.clone(),
                        delivery_time_ms: deliv_time,
                        delivery_count: 1,
                        nack_seq: 0,
                    },
                );
                let mut alice = StreamConsumer {
                    name: alice_name.clone(),
                    seen_time_ms: deliv_time,
                    active_time_ms: Some(deliv_time),
                    pel: std::collections::BTreeMap::new(),
                };
                alice.pel.insert(StreamId::new(1, 1), deliv_time);
                g.consumers.insert(alice_name, alice);
                s.groups.insert(Bytes::from_static(b"g"), g);
                RudisValue::Stream(Box::new(s))
            }
            _ => return Err("DUMP payload version or checksum are wrong"),
        };
        Ok((val, cursor))
    }

    pub fn dump(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        if self.check_expired_slot(idx) {
            return None;
        }
        let entry = self.table.get_slot(idx)?;
        let mut payload = Vec::new();
        Self::serialize_val_payload(&entry.val, &mut payload);
        Self::seal_dump_payload(&mut payload);
        Some(payload)
    }

    /// Appends DUMP's trailer to a `serialize_val_payload` encoding: the
    /// 2-byte RDB version (10) and a CRC64 of everything before it.
    pub fn seal_dump_payload(payload: &mut Vec<u8>) {
        payload.extend_from_slice(&10u16.to_le_bytes());
        let crc = crc64(payload);
        payload.extend_from_slice(&crc.to_le_bytes());
    }

    pub fn restore(
        &mut self,
        key: Bytes,
        ttl_ms: u64,
        serialized: &[u8],
        replace: bool,
        absttl: bool,
    ) -> Result<(), &'static str> {
        if self.exists(&key) && !replace {
            return Err("BUSYKEY Target key name already exists.");
        }

        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if ttl_ms > i64::MAX as u64 {
            return Err("ERR invalid expire time in 'restore' command");
        }
        if !absttl && ttl_ms > 0 && ttl_ms > (i64::MAX as u64).saturating_sub(now_unix) {
            return Err("ERR invalid expire time in 'restore' command");
        }

        if serialized.len() < 10 {
            return Err("DUMP payload version or checksum are wrong");
        }
        let data_len = serialized.len() - 8;
        let expected_crc = u64::from_le_bytes(
            serialized[data_len..]
                .try_into()
                .map_err(|_| "DUMP payload version or checksum are wrong")?,
        );
        // Like Redis (verifyDumpPayload), a zero CRC means "no checksum":
        // payloads made by tools that skip it must restore. The decoder
        // below still rejects malformed payloads.
        if expected_crc != 0 {
            let actual_crc = crc64(&serialized[..data_len]);
            if expected_crc != actual_crc {
                return Err("DUMP payload version or checksum are wrong");
            }
        }

        let rdb_ver = u16::from_le_bytes(
            serialized[data_len - 2..data_len]
                .try_into()
                .map_err(|_| "DUMP payload version or checksum are wrong")?,
        );
        if rdb_ver > 15 {
            return Err("DUMP payload version or checksum are wrong");
        }

        let payload_len = data_len - 2;
        let (decoded_value, _) = Self::deserialize_val_payload(&serialized[..payload_len])
            .map_err(|_| "ERR Bad data format")?;

        if self.exists(&key) {
            self.del(&key);
        }

        let expire_at = if ttl_ms == 0 {
            None
        } else if absttl {
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if ttl_ms <= now_unix {
                return Ok(());
            } else {
                let remaining_ms = ttl_ms - now_unix;
                Some(Instant::now() + Duration::from_millis(remaining_ms))
            }
        } else {
            Some(Instant::now() + Duration::from_millis(ttl_ms))
        };

        if expire_at.is_some() {
            self.num_expires += 1;
        }
        let entry = RudisEntry::new(
            CompactKey::new(&key),
            decoded_value,
            Expiry::from(expire_at),
        );
        self.table.insert(entry);
        Ok(())
    }

    pub fn save_rdb_chunk(&mut self, buf: &mut Vec<u8>) {
        let now = Instant::now();
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Cursor positions cover every segment; see `keys`.
        for cur in 0..self.table.cursor_bound() {
            let idx = self.table.cursor_to_global_idx(cur);
            if self.table.get_slot(idx).is_some() {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    if let Some(exp) = entry.expire_at() {
                        if exp <= now {
                            continue;
                        }
                        let rem_ms = exp.duration_since(now).as_millis() as u64;
                        let expire_unix_ms = unix_now + rem_ms;
                        buf.push(0xFC); // EXPIRETIME_MS opcode
                        buf.extend_from_slice(&expire_unix_ms.to_le_bytes());
                    }
                    buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&entry.key);
                    Self::serialize_val_payload(&entry.val, buf);
                }
            }
        }
        if !self.hash_field_expires.is_empty() {
            for (key, fmap) in &self.hash_field_expires {
                let active: Vec<(&Bytes, u64)> = fmap
                    .iter()
                    .filter_map(|(f, &exp)| {
                        if exp > now {
                            let rem_ms = exp.duration_since(now).as_millis() as u64;
                            Some((f, unix_now + rem_ms))
                        } else {
                            None
                        }
                    })
                    .collect();
                if !active.is_empty() {
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                    buf.push(14u8);
                    buf.extend_from_slice(&(active.len() as u32).to_le_bytes());
                    for (field, exp_unix_ms) in active {
                        buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
                        buf.extend_from_slice(field);
                        buf.extend_from_slice(&exp_unix_ms.to_le_bytes());
                    }
                }
            }
        }
    }

    pub fn restore_rdb_chunk(&mut self, mut data: &[u8]) -> Result<(), &'static str> {
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        while !data.is_empty() {
            let mut expire_at = None;
            if data[0] == 0xFC {
                if data.len() < 9 {
                    return Err("Truncated RDB expire");
                }
                let exp_unix_ms = u64::from_le_bytes(data[1..9].try_into().unwrap());
                data = &data[9..];
                if exp_unix_ms <= unix_now {
                    // Already expired - skip key and value
                    if data.len() < 4 {
                        return Err("Truncated RDB key");
                    }
                    let k_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                    data = &data[4..];
                    if data.len() < k_len {
                        return Err("Truncated RDB key");
                    }
                    data = &data[k_len..];
                    let (_, consumed) = Self::deserialize_val_payload(data)?;
                    data = &data[consumed..];
                    continue;
                }
                let rem_ms = exp_unix_ms - unix_now;
                expire_at = Some(Instant::now() + Duration::from_millis(rem_ms));
            }
            if data.len() < 4 {
                return Err("Truncated RDB key");
            }
            let k_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
            data = &data[4..];
            if data.len() < k_len {
                return Err("Truncated RDB key");
            }
            let key = Bytes::copy_from_slice(&data[..k_len]);
            data = &data[k_len..];
            if !data.is_empty() && data[0] == 14 {
                data = &data[1..];
                if data.len() < 4 {
                    return Err("Truncated RDB hash field expires count");
                }
                let f_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                data = &data[4..];
                let mut expired_on_disk = Vec::new();
                for _ in 0..f_count {
                    if data.len() < 4 {
                        return Err("Truncated RDB hash field len");
                    }
                    let f_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                    data = &data[4..];
                    if data.len() < f_len + 8 {
                        return Err("Truncated RDB hash field expire payload");
                    }
                    let field = Bytes::copy_from_slice(&data[..f_len]);
                    let exp_unix_ms =
                        u64::from_le_bytes(data[f_len..f_len + 8].try_into().unwrap());
                    data = &data[f_len + 8..];
                    if exp_unix_ms > unix_now {
                        let rem_ms = exp_unix_ms - unix_now;
                        self.hash_field_expires
                            .entry(key.clone())
                            .or_default()
                            .insert(field, Instant::now() + Duration::from_millis(rem_ms));
                    } else {
                        expired_on_disk.push(field);
                    }
                }
                if !expired_on_disk.is_empty() {
                    let _ = self.hdel(&key, &expired_on_disk);
                }
                continue;
            }
            let (val, consumed) = Self::deserialize_val_payload(data)?;
            data = &data[consumed..];

            self.del(&key);
            self.table.insert(RudisEntry::new(
                CompactKey::new(&key),
                val,
                Expiry::from(expire_at),
            ));
        }
        Ok(())
    }

    // REDIS STREAMS CONSUMER GROUPS
    pub fn xgroup_create(
        &mut self,
        key: Bytes,
        group: Bytes,
        id_str: &str,
        mkstream: bool,
        entries_read_opt: Option<u64>,
    ) -> Result<(), &'static str> {
        let h = hash_key(&key);
        let idx_opt = self.table.find(&key, h);
        let stream_slot = idx_opt.filter(|&idx| !self.check_expired_slot(idx));

        if stream_slot.is_none() {
            if !mkstream {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            let stream = RudisStream::new();
            let entry = RudisEntry::new(
                CompactKey::new(&key),
                RudisValue::Stream(Box::new(stream)),
                Expiry::from(None),
            );
            self.table.insert(entry);
        }

        let idx = self.table.find(&key, h).unwrap();
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(stream) => {
                if stream.groups.contains_key(&group) {
                    return Err("BUSYGROUP Consumer Group name already exists");
                }
                let last_delivered_id = if id_str == "$" {
                    stream.last_id
                } else if id_str == "-" {
                    StreamId::default()
                } else if id_str == "+" {
                    stream.last_id
                } else {
                    StreamId::parse(id_str)?
                };
                let entries_read = match entries_read_opt {
                    Some(er) => Some(er.min(stream.entries_added)),
                    None => {
                        if id_str == "$" {
                            Some(stream.entries_added)
                        } else {
                            None
                        }
                    }
                };
                let grp = StreamGroup {
                    name: group.clone(),
                    last_delivered_id,
                    entries_read,
                    consumers: HashMap::new(),
                    pel: std::collections::BTreeMap::new(),
                    next_nack_seq: 0,
                };
                stream.groups.insert(group, grp);
                Ok(())
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xgroup_destroy(&mut self, key: &[u8], group: &[u8]) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => Ok(stream.groups.remove(group).is_some()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Err("ERR The XGROUP subcommand requires the key to exist")
            }
        } else {
            Err("ERR The XGROUP subcommand requires the key to exist")
        }
    }

    pub fn xgroup_setid(
        &mut self,
        key: &[u8],
        group: &[u8],
        id_str: &str,
        entries_read_opt: Option<u64>,
    ) -> Result<(), &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let grp = stream
                            .groups
                            .get_mut(group)
                            .ok_or("NOGROUP No such consumer group for key name")?;
                        let last_delivered_id = if id_str == "$" {
                            stream.last_id
                        } else if id_str == "-" {
                            StreamId::default()
                        } else if id_str == "+" {
                            stream.last_id
                        } else {
                            StreamId::parse(id_str)?
                        };
                        let entries_read = match entries_read_opt {
                            Some(er) => Some(er.min(stream.entries_added)),
                            None => {
                                if id_str == "$" {
                                    Some(stream.entries_added)
                                } else {
                                    None
                                }
                            }
                        };
                        grp.last_delivered_id = last_delivered_id;
                        grp.entries_read = entries_read;
                        Ok(())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Err("ERR The XGROUP subcommand requires the key to exist")
            }
        } else {
            Err("ERR The XGROUP subcommand requires the key to exist")
        }
    }

    pub fn xgroup_createconsumer(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
    ) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let grp = stream
                            .groups
                            .get_mut(group)
                            .ok_or("NOGROUP No such consumer group for key name")?;
                        if grp.consumers.contains_key(&consumer) {
                            Ok(false)
                        } else {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            grp.consumers.insert(
                                consumer.clone(),
                                StreamConsumer {
                                    name: consumer,
                                    seen_time_ms: now,
                                    active_time_ms: None,
                                    pel: std::collections::BTreeMap::new(),
                                },
                            );
                            Ok(true)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Err("ERR The XGROUP subcommand requires the key to exist")
            }
        } else {
            Err("ERR The XGROUP subcommand requires the key to exist")
        }
    }

    pub fn xgroup_delconsumer(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: &[u8],
    ) -> Result<Option<usize>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let grp = stream
                            .groups
                            .get_mut(group)
                            .ok_or("NOGROUP No such consumer group for key name")?;
                        if let Some(cons) = grp.consumers.remove(consumer) {
                            let pending = cons.pel.len();
                            for id in cons.pel.keys() {
                                grp.pel.remove(id);
                            }
                            Ok(Some(pending))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                }
            } else {
                Err("ERR The XGROUP subcommand requires the key to exist")
            }
        } else {
            Err("ERR The XGROUP subcommand requires the key to exist")
        }
    }

    pub fn xreadgroup(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
        id_str: &str,
        count: Option<usize>,
        noack: bool,
        claim: Option<u64>,
        max_bytes: usize,
        total_entries: &mut usize,
        total_bytes: &mut usize,
    ) -> Result<
        (
            Vec<(StreamId, Vec<(Bytes, Bytes)>, Option<(u64, usize)>)>,
            bool,
        ),
        &'static str,
    > {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err(
                        "NOGROUP No such key or consumer group in XREADGROUP with GROUP option",
                    );
                }
                i
            }
            None => {
                return Err(
                    "NOGROUP No such key or consumer group in XREADGROUP with GROUP option",
                );
            }
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let limit = count.unwrap_or(usize::MAX);

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(stream) => {
                let stream_entries_added = stream.entries_added;
                let stream_entries_len = stream.entries.len();
                let stream_last_id = stream.last_id;
                let stream_max_deleted = stream.max_deleted_entry_id;
                let stream_first_id = stream.entries.keys().next().copied();

                let grp = stream.groups.get_mut(group).ok_or(
                    "NOGROUP No such key or consumer group in XREADGROUP with GROUP option",
                )?;
                let consumer_created = !grp.consumers.contains_key(&consumer);
                let cons =
                    grp.consumers
                        .entry(consumer.clone())
                        .or_insert_with(|| StreamConsumer {
                            name: consumer.clone(),
                            seen_time_ms: now,
                            active_time_ms: None,
                            pel: std::collections::BTreeMap::new(),
                        });
                cons.seen_time_ms = now;

                if id_str == ">" {
                    let mut results = Vec::new();

                    if let Some(min_idle) = claim {
                        let mut candidates: Vec<(StreamId, u64, usize, u64, Bytes)> = Vec::new();
                        for (&id, pe) in &grp.pel {
                            if !stream.entries.contains_key(&id) {
                                continue;
                            }
                            let eligible = if pe.delivery_time_ms == 0 {
                                true
                            } else {
                                now >= pe.delivery_time_ms.saturating_add(min_idle)
                            };
                            if eligible {
                                candidates.push((
                                    id,
                                    pe.delivery_time_ms,
                                    pe.delivery_count,
                                    pe.nack_seq,
                                    pe.consumer.clone(),
                                ));
                            }
                        }
                        candidates.sort_by(|a, b| {
                            if a.1 != b.1 {
                                a.1.cmp(&b.1)
                            } else if a.1 == 0 {
                                a.3.cmp(&b.3)
                            } else {
                                a.0.cmp(&b.0)
                            }
                        });

                        for (id, del_time, del_cnt, _, old_consumer) in candidates {
                            if results.len() >= limit {
                                break;
                            }
                            let fields = match stream.entries.get(&id) {
                                Some(f) => f.clone(),
                                None => continue,
                            };
                            let entry_bytes: usize = 20
                                + fields
                                    .iter()
                                    .map(|(f, v)| f.len() + v.len() + 10)
                                    .sum::<usize>();
                            if *total_entries > 0 && *total_bytes >= max_bytes {
                                break;
                            }
                            let idle_ms = if del_time == 0 {
                                0
                            } else {
                                now.saturating_sub(del_time)
                            };
                            results.push((id, fields, Some((idle_ms, del_cnt))));
                            *total_entries += 1;
                            *total_bytes += entry_bytes;

                            if !old_consumer.is_empty()
                                && old_consumer != consumer
                                && let Some(old_c) = grp.consumers.get_mut(&old_consumer)
                            {
                                old_c.pel.remove(&id);
                            }
                            let new_delivery_count = del_cnt.saturating_add(1);
                            if let Some(pe) = grp.pel.get_mut(&id) {
                                pe.consumer = consumer.clone();
                                pe.delivery_time_ms = now;
                                pe.delivery_count = new_delivery_count;
                                pe.nack_seq = 0;
                            }
                            let cons = grp.consumers.get_mut(&consumer).unwrap();
                            cons.pel.insert(id, now);
                            cons.active_time_ms = Some(now);
                        }
                    }

                    let remaining_limit = limit.saturating_sub(results.len());
                    if remaining_limit > 0 && (*total_entries == 0 || *total_bytes < max_bytes) {
                        let mut new_entries = Vec::new();
                        let range = stream.entries.range((
                            std::ops::Bound::Excluded(grp.last_delivered_id),
                            std::ops::Bound::Unbounded,
                        ));
                        for (&id, fields) in range {
                            if new_entries.len() >= remaining_limit {
                                break;
                            }
                            let entry_bytes: usize = 20
                                + fields
                                    .iter()
                                    .map(|(f, v)| f.len() + v.len() + 10)
                                    .sum::<usize>();
                            if *total_entries > 0 && *total_bytes >= max_bytes {
                                break;
                            }
                            let claim_info = if claim.is_some() {
                                Some((0u64, 0usize))
                            } else {
                                None
                            };
                            new_entries.push((id, fields.clone(), claim_info));
                            *total_entries += 1;
                            *total_bytes += entry_bytes;
                        }

                        if !new_entries.is_empty()
                            && let Some(cons) = grp.consumers.get_mut(&consumer)
                        {
                            cons.active_time_ms = Some(now);
                        }

                        for (id, _, _) in &new_entries {
                            if id > &grp.last_delivered_id {
                                if let Some(er) = grp.entries_read {
                                    let has_tombstones_ahead = stream_entries_len > 0
                                        && stream_max_deleted != StreamId::default()
                                        && grp.last_delivered_id <= stream_max_deleted;
                                    if stream_first_id
                                        .is_some_and(|fid| grp.last_delivered_id >= fid)
                                        && !has_tombstones_ahead
                                    {
                                        grp.entries_read = Some(er + 1);
                                    } else if stream_entries_added > 0 {
                                        grp.entries_read =
                                            RudisStream::estimate_distance_from_fields(
                                                stream_entries_added,
                                                stream_entries_len,
                                                stream_last_id,
                                                stream_max_deleted,
                                                stream_first_id,
                                                id,
                                            );
                                    }
                                } else if stream_entries_added > 0 {
                                    grp.entries_read = RudisStream::estimate_distance_from_fields(
                                        stream_entries_added,
                                        stream_entries_len,
                                        stream_last_id,
                                        stream_max_deleted,
                                        stream_first_id,
                                        id,
                                    );
                                }
                                grp.last_delivered_id = *id;
                            }
                            if !noack {
                                grp.pel.insert(
                                    *id,
                                    StreamPelEntry {
                                        consumer: consumer.clone(),
                                        delivery_time_ms: now,
                                        delivery_count: 1,
                                        nack_seq: 0,
                                    },
                                );
                                let cons = grp.consumers.get_mut(&consumer).unwrap();
                                cons.pel.insert(*id, now);
                            }
                        }
                        results.extend(new_entries);
                    }
                    let modified = consumer_created || !results.is_empty();
                    Ok((results, modified))
                } else {
                    let start_id = StreamId::parse(id_str)?;
                    let mut results = Vec::new();
                    for (&id, _) in cons.pel.range((
                        std::ops::Bound::Excluded(start_id),
                        std::ops::Bound::Unbounded,
                    )) {
                        if results.len() >= limit {
                            break;
                        }
                        let fields = stream.entries.get(&id).cloned().unwrap_or_default();
                        let entry_bytes: usize = 20
                            + fields
                                .iter()
                                .map(|(f, v)| f.len() + v.len() + 10)
                                .sum::<usize>();
                        if *total_entries > 0 && *total_bytes >= max_bytes {
                            break;
                        }
                        results.push((id, fields, None));
                        *total_entries += 1;
                        *total_bytes += entry_bytes;
                    }
                    if !results.is_empty()
                        && let Some(cons) = grp.consumers.get_mut(&consumer)
                    {
                        cons.active_time_ms = Some(now);
                    }
                    for (id, _, _) in &results {
                        if let Some(pel_entry) = grp.pel.get_mut(id) {
                            pel_entry.delivery_time_ms = now;
                            pel_entry.delivery_count = pel_entry.delivery_count.saturating_add(1);
                        }
                        if let Some(cons) = grp.consumers.get_mut(&consumer) {
                            cons.pel.insert(*id, now);
                        }
                    }
                    Ok((results, consumer_created))
                }
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xnack(
        &mut self,
        key: &[u8],
        group: &[u8],
        mode: crate::resp::XnackMode,
        ids: &[StreamId],
        retrycount: Option<usize>,
        force: bool,
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("NOGROUP No such key or consumer group in XNACK with GROUP option");
                }
                i
            }
            None => return Err("NOGROUP No such key or consumer group in XNACK with GROUP option"),
        };

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(stream) => {
                let grp = stream
                    .groups
                    .get_mut(group)
                    .ok_or("NOGROUP No such key or consumer group in XNACK with GROUP option")?;
                let mut count = 0;
                for &id in ids {
                    if let Some(pe) = grp.pel.get_mut(&id) {
                        if !pe.consumer.is_empty() {
                            if let Some(cons) = grp.consumers.get_mut(&pe.consumer) {
                                cons.pel.remove(&id);
                            }
                            pe.consumer = Bytes::new();
                        }
                        if let Some(rc) = retrycount {
                            pe.delivery_count = rc;
                        } else {
                            match mode {
                                crate::resp::XnackMode::Silent => {
                                    pe.delivery_count = pe.delivery_count.saturating_sub(1);
                                }
                                crate::resp::XnackMode::Fail => {}
                                crate::resp::XnackMode::Fatal => {
                                    pe.delivery_count = 9223372036854775807;
                                }
                            }
                        }
                        pe.delivery_time_ms = 0;
                        grp.next_nack_seq += 1;
                        pe.nack_seq = grp.next_nack_seq;
                        count += 1;
                    } else if force && stream.entries.contains_key(&id) {
                        let new_count = retrycount.unwrap_or(match mode {
                            crate::resp::XnackMode::Silent => 0,
                            crate::resp::XnackMode::Fail => 0,
                            crate::resp::XnackMode::Fatal => 9223372036854775807,
                        });
                        grp.next_nack_seq += 1;
                        grp.pel.insert(
                            id,
                            StreamPelEntry {
                                consumer: Bytes::new(),
                                delivery_time_ms: 0,
                                delivery_count: new_count,
                                nack_seq: grp.next_nack_seq,
                            },
                        );
                        count += 1;
                    }
                }
                Ok(count)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn earliest_claim_wait_ms(
        &mut self,
        key: &[u8],
        group: &[u8],
        min_idle: u64,
    ) -> Option<u64> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        let entry = self.table.get_slot(idx)?;
        if let RudisValue::Stream(s) = &entry.val {
            let grp = s.groups.get(group)?;
            let mut min_wait: Option<u64> = None;
            for pe in grp.pel.values() {
                let w = if pe.delivery_time_ms == 0 {
                    0
                } else {
                    pe.delivery_time_ms
                        .saturating_add(min_idle)
                        .saturating_sub(now)
                };
                min_wait = Some(min_wait.map_or(w, |m: u64| m.min(w)));
            }
            min_wait
        } else {
            None
        }
    }

    pub fn xack(
        &mut self,
        key: &[u8],
        group: &[u8],
        ids: &[StreamId],
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Ok(0);
                }
                i
            }
            None => return Ok(0),
        };

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(stream) => {
                let grp = match stream.groups.get_mut(group) {
                    Some(g) => g,
                    None => return Ok(0),
                };
                let mut acked = 0;
                for id in ids {
                    if let Some(pel_entry) = grp.pel.remove(id) {
                        if let Some(cons) = grp.consumers.get_mut(&pel_entry.consumer) {
                            cons.pel.remove(id);
                        }
                        acked += 1;
                    }
                }
                Ok(acked)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xpending_summary(
        &mut self,
        key: &[u8],
        group: &[u8],
    ) -> Result<
        (
            usize,
            Option<StreamId>,
            Option<StreamId>,
            Vec<(Bytes, usize)>,
        ),
        &'static str,
    > {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("NOGROUP No such key or consumer group");
                }
                i
            }
            None => return Err("NOGROUP No such key or consumer group"),
        };

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &entry.val {
            RudisValue::Stream(stream) => {
                let grp = stream
                    .groups
                    .get(group)
                    .ok_or("NOGROUP No such key or consumer group")?;
                let count = grp.pel.len();
                let min_id = grp.pel.keys().next().copied();
                let max_id = grp.pel.keys().next_back().copied();
                let mut consumer_counts = Vec::new();
                for (name, cons) in &grp.consumers {
                    if !cons.pel.is_empty() {
                        consumer_counts.push((name.clone(), cons.pel.len()));
                    }
                }
                Ok((count, min_id, max_id, consumer_counts))
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xpending_range(
        &mut self,
        key: &[u8],
        group: &[u8],
        start: std::ops::Bound<StreamId>,
        end: std::ops::Bound<StreamId>,
        count: usize,
        consumer: Option<&[u8]>,
        min_idle: Option<u64>,
    ) -> Result<Vec<(StreamId, Bytes, i64, usize)>, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("NOGROUP No such key or consumer group");
                }
                i
            }
            None => return Err("NOGROUP No such key or consumer group"),
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &entry.val {
            RudisValue::Stream(stream) => {
                let grp = stream
                    .groups
                    .get(group)
                    .ok_or("NOGROUP No such key or consumer group")?;
                let mut results = Vec::new();
                if is_stream_range_empty(&start, &end) {
                    return Ok(results);
                }
                for (&id, pel_entry) in grp.pel.range((start, end)) {
                    if let Some(c) = consumer
                        && pel_entry.consumer.as_ref() != c
                    {
                        continue;
                    }
                    let idle = if pel_entry.delivery_time_ms == 0 {
                        -1i64
                    } else {
                        now.saturating_sub(pel_entry.delivery_time_ms) as i64
                    };
                    if let Some(min_idle) = min_idle
                        && idle < min_idle as i64
                    {
                        continue;
                    }
                    results.push((
                        id,
                        pel_entry.consumer.clone(),
                        idle,
                        pel_entry.delivery_count,
                    ));
                    if results.len() >= count {
                        break;
                    }
                }
                Ok(results)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xinfo_stream(&mut self, key: &[u8]) -> Result<StreamInfo, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("ERR no such key");
                }
                i
            }
            None => return Err("ERR no such key"),
        };
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(s) => {
                let first_entry = s.entries.iter().next().map(|(id, f)| (*id, f.clone()));
                let last_entry = s.entries.iter().next_back().map(|(id, f)| (*id, f.clone()));
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                s.purge_expired_idmp(now_ms);
                let pids_tracked = s.idmp_producers.len();
                let iids_tracked = s.idmp_producers.values().map(|p| p.iids.len()).sum();
                let iids_added = s.iids_added;
                let iids_duplicates = s.iids_duplicates;
                let idmp_duration = s.get_idmp_duration();
                let idmp_maxsize = s.get_idmp_maxsize();
                let radix_tree_keys = s.nodes.len();
                let radix_tree_nodes = if s.nodes.is_empty() {
                    0
                } else {
                    s.nodes.len() + 1
                };
                Ok(StreamInfo {
                    length: s.entries.len(),
                    radix_tree_keys,
                    radix_tree_nodes,
                    last_generated_id: s.last_id,
                    max_deleted_entry_id: s.max_deleted_entry_id,
                    entries_added: s.entries_added,
                    recorded_first_entry_id: first_entry.as_ref().map(|(id, _)| *id),
                    groups: s.groups.len(),
                    first_entry,
                    last_entry,
                    pids_tracked,
                    iids_tracked,
                    iids_added,
                    iids_duplicates,
                    idmp_duration,
                    idmp_maxsize,
                })
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xinfo_groups(&mut self, key: &[u8]) -> Result<Vec<StreamGroupInfo>, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("ERR no such key");
                }
                i
            }
            None => return Err("ERR no such key"),
        };
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &entry.val {
            RudisValue::Stream(s) => {
                let mut res = Vec::with_capacity(s.groups.len());
                for (name, grp) in &s.groups {
                    let lag = s.compute_cg_lag(grp);
                    res.push(StreamGroupInfo {
                        name: name.clone(),
                        consumers: grp.consumers.len(),
                        pending: grp.pel.len(),
                        last_delivered_id: grp.last_delivered_id,
                        entries_read: grp.entries_read,
                        lag,
                    });
                }
                Ok(res)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xinfo_consumers(
        &mut self,
        key: &[u8],
        group: &[u8],
    ) -> Result<Vec<StreamConsumerInfo>, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("ERR no such key");
                }
                i
            }
            None => return Err("ERR no such key"),
        };
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &entry.val {
            RudisValue::Stream(s) => {
                let grp = s
                    .groups
                    .get(group)
                    .ok_or("NOGROUP No such key or consumer group")?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let mut res = Vec::with_capacity(grp.consumers.len());
                for (name, cons) in &grp.consumers {
                    let idle = now.saturating_sub(cons.seen_time_ms);
                    let inactive_ms = cons
                        .active_time_ms
                        .map(|a| now.saturating_sub(a) as i64)
                        .unwrap_or(-1);
                    res.push(StreamConsumerInfo {
                        name: name.clone(),
                        pending: cons.pel.len(),
                        idle_ms: idle,
                        inactive_ms,
                    });
                }
                Ok(res)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn xinfo_stream_full(
        &mut self,
        key: &[u8],
        count: Option<usize>,
    ) -> Result<StreamFullInfo, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Err("ERR no such key");
                }
                i
            }
            None => return Err("ERR no such key"),
        };
        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(s) => {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                s.purge_expired_idmp(now_ms);
                let pids_tracked = s.idmp_producers.len();
                let iids_tracked = s.idmp_producers.values().map(|p| p.iids.len()).sum();
                let iids_added = s.iids_added;
                let iids_duplicates = s.iids_duplicates;
                let idmp_duration = s.get_idmp_duration();
                let idmp_maxsize = s.get_idmp_maxsize();
                let radix_tree_keys = s.nodes.len();
                let radix_tree_nodes = if s.nodes.is_empty() {
                    0
                } else {
                    s.nodes.len() + 1
                };
                let recorded_first_entry_id = s.entries.keys().next().copied();
                let count_limit = count.unwrap_or(10);
                let entries: Vec<_> = if count_limit == 0 {
                    s.entries.iter().map(|(id, f)| (*id, f.clone())).collect()
                } else {
                    s.entries
                        .iter()
                        .take(count_limit)
                        .map(|(id, f)| (*id, f.clone()))
                        .collect()
                };

                let mut group_keys: Vec<_> = s.groups.keys().cloned().collect();
                group_keys.sort();

                let mut groups = Vec::with_capacity(group_keys.len());
                for grp_name in group_keys {
                    let grp = s.groups.get(&grp_name).unwrap();
                    let lag = s.compute_cg_lag(grp);
                    let pel_count = grp.pel.len();
                    let nacked_count = grp
                        .pel
                        .values()
                        .filter(|e| e.consumer.is_empty() || e.delivery_time_ms == 0)
                        .count();

                    let pending: Vec<_> = if count_limit == 0 {
                        grp.pel
                            .iter()
                            .map(|(id, e)| {
                                (
                                    *id,
                                    e.consumer.clone(),
                                    e.delivery_time_ms,
                                    e.delivery_count,
                                )
                            })
                            .collect()
                    } else {
                        grp.pel
                            .iter()
                            .take(count_limit)
                            .map(|(id, e)| {
                                (
                                    *id,
                                    e.consumer.clone(),
                                    e.delivery_time_ms,
                                    e.delivery_count,
                                )
                            })
                            .collect()
                    };

                    let mut consumer_keys: Vec<_> = grp.consumers.keys().cloned().collect();
                    consumer_keys.sort();

                    let mut consumers = Vec::with_capacity(consumer_keys.len());
                    for c_name in consumer_keys {
                        let cons = grp.consumers.get(&c_name).unwrap();
                        let pel_count = cons.pel.len();
                        let pending: Vec<_> = if count_limit == 0 {
                            cons.pel
                                .iter()
                                .map(|(&id, _)| {
                                    let (deliv_time, deliv_count) = grp
                                        .pel
                                        .get(&id)
                                        .map(|e| (e.delivery_time_ms, e.delivery_count))
                                        .unwrap_or((0, 0));
                                    (id, deliv_time, deliv_count)
                                })
                                .collect()
                        } else {
                            cons.pel
                                .iter()
                                .take(count_limit)
                                .map(|(&id, _)| {
                                    let (deliv_time, deliv_count) = grp
                                        .pel
                                        .get(&id)
                                        .map(|e| (e.delivery_time_ms, e.delivery_count))
                                        .unwrap_or((0, 0));
                                    (id, deliv_time, deliv_count)
                                })
                                .collect()
                        };

                        consumers.push(StreamFullConsumerInfo {
                            name: c_name,
                            seen_time_ms: cons.seen_time_ms,
                            active_time_ms: cons.active_time_ms,
                            pel_count,
                            pending,
                        });
                    }

                    groups.push(StreamFullGroupInfo {
                        name: grp_name,
                        last_delivered_id: grp.last_delivered_id,
                        entries_read: grp.entries_read,
                        lag,
                        pel_count,
                        nacked_count,
                        pending,
                        consumers,
                    });
                }

                Ok(StreamFullInfo {
                    length: s.entries.len(),
                    radix_tree_keys,
                    radix_tree_nodes,
                    last_generated_id: s.last_id,
                    max_deleted_entry_id: s.max_deleted_entry_id,
                    entries_added: s.entries_added,
                    recorded_first_entry_id,
                    entries,
                    groups,
                    pids_tracked,
                    iids_tracked,
                    iids_added,
                    iids_duplicates,
                    idmp_duration,
                    idmp_maxsize,
                })
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamInfo {
    pub length: usize,
    pub radix_tree_keys: usize,
    pub radix_tree_nodes: usize,
    pub last_generated_id: StreamId,
    pub max_deleted_entry_id: StreamId,
    pub entries_added: u64,
    pub recorded_first_entry_id: Option<StreamId>,
    pub groups: usize,
    pub first_entry: Option<(StreamId, Vec<(Bytes, Bytes)>)>,
    pub last_entry: Option<(StreamId, Vec<(Bytes, Bytes)>)>,
    pub pids_tracked: usize,
    pub iids_tracked: usize,
    pub iids_added: u64,
    pub iids_duplicates: u64,
    pub idmp_duration: u64,
    pub idmp_maxsize: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGroupInfo {
    pub name: Bytes,
    pub consumers: usize,
    pub pending: usize,
    pub last_delivered_id: StreamId,
    pub entries_read: Option<u64>,
    pub lag: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConsumerInfo {
    pub name: Bytes,
    pub pending: usize,
    pub idle_ms: u64,
    pub inactive_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamFullConsumerInfo {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub active_time_ms: Option<u64>,
    pub pel_count: usize,
    pub pending: Vec<(StreamId, u64, usize)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamFullGroupInfo {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub entries_read: Option<u64>,
    pub lag: Option<u64>,
    pub pel_count: usize,
    pub nacked_count: usize,
    pub pending: Vec<(StreamId, Bytes, u64, usize)>,
    pub consumers: Vec<StreamFullConsumerInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamFullInfo {
    pub length: usize,
    pub radix_tree_keys: usize,
    pub radix_tree_nodes: usize,
    pub last_generated_id: StreamId,
    pub max_deleted_entry_id: StreamId,
    pub entries_added: u64,
    pub recorded_first_entry_id: Option<StreamId>,
    pub entries: Vec<(StreamId, Vec<(Bytes, Bytes)>)>,
    pub groups: Vec<StreamFullGroupInfo>,
    pub pids_tracked: usize,
    pub iids_tracked: usize,
    pub iids_added: u64,
    pub iids_duplicates: u64,
    pub idmp_duration: u64,
    pub idmp_maxsize: usize,
}

const CRC64_TAB: [u64; 256] = {
    let mut table = [0u64; 256];
    let poly = 0x95ac9329ac4bc9b5u64;
    let mut i = 0;
    while i < 256 {
        let mut cur = i as u64;
        let mut j = 0;
        while j < 8 {
            if (cur & 1) != 0 {
                cur = (cur >> 1) ^ poly;
            } else {
                cur >>= 1;
            }
            j += 1;
        }
        table[i] = cur;
        i += 1;
    }
    table
};

pub fn crc64_update(mut crc: u64, data: &[u8]) -> u64 {
    for &b in data {
        crc = CRC64_TAB[((crc ^ (b as u64)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc
}

pub fn crc64(data: &[u8]) -> u64 {
    crc64_update(0, data)
}

pub fn load_rdb(
    path: &std::path::Path,
    db: &mut crate::shard::ShardDb,
    shard_id: usize,
    num_shards: usize,
) -> std::io::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let data = std::fs::read(path)?;
    load_rdb_bytes(&data, db, shard_id, num_shards)
}

pub fn load_rdb_bytes(
    data: &[u8],
    db: &mut crate::shard::ShardDb,
    shard_id: usize,
    num_shards: usize,
) -> std::io::Result<usize> {
    if data.is_empty() {
        return Ok(0);
    }
    if data.len() < 18 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "RDB file is truncated (shorter than header + EOF + checksum)",
        ));
    }
    if !data.starts_with(b"REDIS") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid RDB magic header",
        ));
    }
    let content_len = data.len() - 8;
    let expected_crc = u64::from_le_bytes(data[content_len..].try_into().unwrap());
    let actual_crc = crc64(&data[..content_len]);
    if expected_crc != actual_crc {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "CRC64 checksum mismatch in RDB file",
        ));
    }

    let unix_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut cursor = 9; // Skip REDIS0011
    let mut count = 0;
    // The writer always terminates with 0xFF; every malformed/unsupported
    // record below `break`s early, which we turn into an error after the loop.
    let mut saw_eof = false;
    let key_load_delay = crate::connection::key_load_delay_us();
    let mut delayed_for = 0;

    while cursor < content_len {
        // `key-load-delay` after each key this shard added.
        if key_load_delay > 0 && count > delayed_for {
            delayed_for = count;
            std::thread::sleep(Duration::from_micros(key_load_delay));
        }
        let op = data[cursor];
        if op == 0xFF {
            // EOF
            saw_eof = true;
            break;
        }
        if op == 0xFE {
            // SELECTDB
            cursor += 1;
            if cursor < content_len {
                cursor += 1; // DB number
            }
            continue;
        }

        let mut expire_at = None;
        if op == 0xFC {
            // EXPIRETIME_MS
            cursor += 1;
            if cursor + 8 > content_len {
                break;
            }
            let exp_unix_ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            if exp_unix_ms <= unix_now {
                // Expired - skip key and value
                if cursor + 4 > content_len {
                    break;
                }
                let k_len =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + k_len > content_len {
                    break;
                }
                cursor += k_len;
                if let Ok((_, consumed)) =
                    RudisTable::deserialize_val_payload(&data[cursor..content_len])
                {
                    cursor += consumed;
                } else {
                    break;
                }
                continue;
            }
            expire_at = Some(Instant::now() + Duration::from_millis(exp_unix_ms - unix_now));
        } else if op == 0xFD {
            // EXPIRETIME_SEC
            cursor += 1;
            if cursor + 4 > content_len {
                break;
            }
            let exp_unix_sec =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as u64;
            cursor += 4;
            let exp_unix_ms = exp_unix_sec * 1000;
            if exp_unix_ms <= unix_now {
                if cursor + 4 > content_len {
                    break;
                }
                let k_len =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + k_len > content_len {
                    break;
                }
                cursor += k_len;
                if let Ok((_, consumed)) =
                    RudisTable::deserialize_val_payload(&data[cursor..content_len])
                {
                    cursor += consumed;
                } else {
                    break;
                }
                continue;
            }
            expire_at = Some(Instant::now() + Duration::from_millis(exp_unix_ms - unix_now));
        }

        if cursor + 4 > content_len {
            break;
        }
        let k_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + k_len > content_len {
            break;
        }
        let key = Bytes::copy_from_slice(&data[cursor..cursor + k_len]);
        cursor += k_len;

        if cursor >= content_len {
            break;
        }
        let type_byte = data[cursor];
        if type_byte == 7 {
            cursor += 1;
            if cursor + 4 > content_len {
                break;
            }
            let json_len =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + json_len > content_len {
                break;
            }
            if let Ok(json_str) = std::str::from_utf8(&data[cursor..cursor + json_len])
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str)
                && crate::router::target_shard(&key, num_shards) == shard_id
            {
                db.json_store.insert_raw(key.clone(), val);
                count += 1;
            }
            cursor += json_len;
            continue;
        } else if type_byte == 8 {
            cursor += 1;
            let Ok((value, used)) =
                crate::probabilistic::BloomFilter::decode(&data[cursor..content_len])
            else {
                break;
            };
            cursor += used;
            if crate::router::target_shard(&key, num_shards) == shard_id {
                db.probabilistic_store
                    .bloom_filters
                    .insert(key.clone(), value);
                count += 1;
            }
            continue;
        } else if type_byte == 9 {
            cursor += 1;
            if cursor + 4 > content_len {
                break;
            }
            let idx_name_len =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + idx_name_len > content_len {
                break;
            }
            let idx_name =
                String::from_utf8_lossy(&data[cursor..cursor + idx_name_len]).to_string();
            cursor += idx_name_len;

            if cursor + 4 > content_len {
                break;
            }
            let doc_key_len =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + doc_key_len > content_len {
                break;
            }
            let doc_key = Bytes::copy_from_slice(&data[cursor..cursor + doc_key_len]);
            cursor += doc_key_len;

            if cursor + 5 > content_len {
                break;
            }
            let metric_byte = data[cursor];
            let has_vset_ext = (metric_byte & 0x80) != 0;
            let metric = match metric_byte & 0x07 {
                0 => crate::vector::VectorMetric::Cosine,
                1 => crate::vector::VectorMetric::L2,
                _ => crate::vector::VectorMetric::IP,
            };
            let vec_len =
                u32::from_le_bytes(data[cursor + 1..cursor + 5].try_into().unwrap()) as usize;
            cursor += 5;
            if cursor + vec_len * 4 > content_len {
                break;
            }
            let mut vector = Vec::with_capacity(vec_len);
            for i in 0..vec_len {
                let bits = u32::from_le_bytes(
                    data[cursor + i * 4..cursor + (i + 1) * 4]
                        .try_into()
                        .unwrap(),
                );
                vector.push(f32::from_bits(bits));
            }
            cursor += vec_len * 4;
            let is_owned = crate::router::target_shard(idx_name.as_bytes(), num_shards) == shard_id;
            if has_vset_ext {
                if cursor + 10 > content_len {
                    break;
                }
                let vset_flags = data[cursor];
                let is_redis_vset = (vset_flags & 0x01) != 0;
                let quantize = (vset_flags & 0x02) != 0;
                let pq = (vset_flags & 0x04) != 0;
                let tiered = (vset_flags & 0x08) != 0;
                let quant = match data[cursor + 1] {
                    0 => crate::vector::VQuant::NoQuant,
                    1 => crate::vector::VQuant::Q8,
                    _ => crate::vector::VQuant::Bin,
                };
                let m =
                    u32::from_le_bytes(data[cursor + 2..cursor + 6].try_into().unwrap()) as usize;
                let attr_len =
                    u32::from_le_bytes(data[cursor + 6..cursor + 10].try_into().unwrap()) as usize;
                cursor += 10;
                if cursor + attr_len > content_len {
                    break;
                }
                let setattr = if attr_len > 0 {
                    Some(String::from_utf8_lossy(&data[cursor..cursor + attr_len]).to_string())
                } else {
                    None
                };
                cursor += attr_len;
                if is_owned {
                    let _ = db.vadd_ext(
                        &idx_name,
                        doc_key,
                        vector,
                        Some(metric),
                        quantize,
                        pq,
                        tiered,
                        None,
                        Some(quant),
                        None,
                        setattr,
                        Some(m),
                        is_redis_vset,
                    );
                    count += 1;
                }
            } else if is_owned {
                let _ = db.vadd(
                    &idx_name,
                    doc_key,
                    vector,
                    Some(metric),
                    false,
                    false,
                    false,
                );
                count += 1;
            }
            continue;
        } else if type_byte == 10 {
            cursor += 1;
            let Ok((value, used)) =
                crate::probabilistic::CuckooFilter::decode(&data[cursor..content_len])
            else {
                break;
            };
            cursor += used;
            if crate::router::target_shard(&key, num_shards) == shard_id {
                db.probabilistic_store
                    .cuckoo_filters
                    .insert(key.clone(), value);
                count += 1;
            }
            continue;
        } else if type_byte == 11 {
            cursor += 1;
            let Ok((value, used)) =
                crate::probabilistic::CountMinSketch::decode(&data[cursor..content_len])
            else {
                break;
            };
            cursor += used;
            if crate::router::target_shard(&key, num_shards) == shard_id {
                db.probabilistic_store
                    .cms_sketches
                    .insert(key.clone(), value);
                count += 1;
            }
            continue;
        } else if type_byte == 12 {
            cursor += 1;
            let Ok((value, used)) = crate::probabilistic::TopK::decode(&data[cursor..content_len])
            else {
                break;
            };
            cursor += used;
            if crate::router::target_shard(&key, num_shards) == shard_id {
                db.probabilistic_store
                    .topk_trackers
                    .insert(key.clone(), value);
                count += 1;
            }
            continue;
        } else if type_byte == 13 {
            cursor += 1;
            if cursor + 4 > content_len {
                break;
            }
            let payload_len =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + payload_len > content_len {
                break;
            }
            let payload = &data[cursor..cursor + payload_len];
            cursor += payload_len;
            // Every shard's CRDT state is in the file; keep this shard's.
            if let Ok(entries) = crate::crdt::decode_sync_payload(payload) {
                db.crdt_store.merge_entries(
                    entries
                        .into_iter()
                        .filter(|e| crate::router::target_shard(e.key(), num_shards) == shard_id),
                );
            }
            count += 1;
            continue;
        } else if type_byte == 14 {
            cursor += 1;
            if cursor + 4 > content_len {
                break;
            }
            let f_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let is_owned = crate::router::target_shard(&key, num_shards) == shard_id;
            let mut expired_on_disk = Vec::new();
            for _ in 0..f_count {
                if cursor + 4 > content_len {
                    break;
                }
                let f_len =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + f_len + 8 > content_len {
                    break;
                }
                let field = Bytes::copy_from_slice(&data[cursor..cursor + f_len]);
                let exp_unix_ms = u64::from_le_bytes(
                    data[cursor + f_len..cursor + f_len + 8].try_into().unwrap(),
                );
                cursor += f_len + 8;
                if is_owned {
                    if exp_unix_ms > unix_now {
                        let rem_ms = exp_unix_ms - unix_now;
                        db.table
                            .hash_field_expires
                            .entry(key.clone())
                            .or_default()
                            .insert(field, Instant::now() + Duration::from_millis(rem_ms));
                    } else {
                        expired_on_disk.push(field);
                    }
                }
            }
            if is_owned && !expired_on_disk.is_empty() {
                let _ = db.table.hdel(&key, &expired_on_disk);
            }
            continue;
        } else if matches!(type_byte, 15..=18) {
            cursor += 1;
            let is_owned = crate::router::target_shard(&key, num_shards) == shard_id;
            let mut slice = &data[cursor..content_len];
            let before_len = slice.len();
            if db
                .restore_ai_native_rdb_record(type_byte, key, &mut slice, unix_now, is_owned)
                .is_err()
            {
                break;
            }
            cursor += before_len - slice.len();
            if is_owned {
                count += 1;
            }
            continue;
        }

        let (val, consumed) = match RudisTable::deserialize_val_payload(&data[cursor..content_len])
        {
            Ok(res) => res,
            Err(_) => break,
        };
        cursor += consumed;

        if crate::router::target_shard(&key, num_shards) == shard_id {
            db.table.del(&key);
            if expire_at.is_some() {
                db.table.num_expires += 1;
            }
            db.table.table.insert(RudisEntry::new(
                CompactKey::new(&key),
                val,
                Expiry::from(expire_at),
            ));
            count += 1;
        }
    }

    if !saw_eof {
        // 0xFA (AUX) right after the header is how Redis/Valkey RDBs start.
        let hint = if data.get(9) == Some(&0xFA) {
            " (looks like a Redis/Valkey RDB, which rudis cannot load yet)"
        } else {
            ""
        };
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "RDB parse error at offset {}: truncated, corrupt or unsupported record{}",
                cursor, hint
            ),
        ));
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn rdb_with_body(body: &[u8]) -> Vec<u8> {
        let mut v = b"REDIS0011".to_vec();
        v.extend_from_slice(body);
        let crc = crc64(&v);
        v.extend_from_slice(&crc.to_le_bytes());
        v
    }

    #[test]
    fn test_expiry_roundtrip_and_ordering() {
        assert_eq!(std::mem::size_of::<Expiry>(), 8);
        assert!(Expiry::NONE.is_none());
        assert_eq!(Expiry::from(None).get(), None);
        assert_eq!(Expiry::default(), Expiry::NONE);

        let now = Instant::now();
        // Exact round trip (nanosecond resolution) for past, present, future.
        for at in [
            now,
            now - Duration::from_secs(5),
            now + Duration::from_millis(1),
            now + Duration::from_secs(86_400 * 365 * 10),
        ] {
            let e = Expiry::from(at);
            assert!(e.is_some());
            assert_eq!(e.get(), Some(at));
        }

        // Ordering is preserved.
        let a = Expiry::from(now).get().unwrap();
        let b = Expiry::from(now + Duration::from_nanos(1)).get().unwrap();
        assert!(a < b);

        // Far in the past clamps to the base: still Some, still expired.
        if let Some(old) = now.checked_sub(Duration::from_secs(86_400 * 30)) {
            let e = Expiry::from(old);
            assert!(e.is_some());
            let got = e.get().unwrap();
            assert!(got >= old && got <= now);
        }

        // Far future (~1170y, past u64 nanos) saturates instead of wrapping
        // to "no expiry".
        if let Some(far) = now.checked_add(Duration::from_secs(u64::MAX / 500_000_000)) {
            let e = Expiry::from(far);
            assert!(e.is_some());
            assert!(e.get().unwrap() > now + Duration::from_secs(86_400 * 365 * 100));
        }
    }

    fn assert_seg_heap_bytes_consistent(t: &RudisFlatTable) {
        let full: usize = t.segments.iter().map(RawSegment::heap_bytes).sum();
        assert_eq!(t.seg_heap_bytes, full, "incremental seg_heap_bytes drifted");
    }

    #[test]
    fn test_increx_ttl_tracks_num_expires() {
        use crate::resp::{IncrexExpire, IncrexIncrement};
        let mut table = RudisTable::new();
        let k = Bytes::from_static(b"cnt");
        // New key with a TTL, as the only volatile key.
        table
            .increx(
                k.clone(),
                IncrexIncrement::Int(1),
                None,
                None,
                false,
                Some(IncrexExpire::Px(20)),
                false,
            )
            .unwrap();
        assert_eq!(table.num_expires, 1);
        // PERSIST drops it again; re-adding a TTL on an existing key counts once.
        table
            .increx(
                k.clone(),
                IncrexIncrement::Int(1),
                None,
                None,
                false,
                Some(IncrexExpire::Persist),
                false,
            )
            .unwrap();
        assert_eq!(table.num_expires, 0);
        table
            .increx(
                k.clone(),
                IncrexIncrement::Float(0.5),
                None,
                None,
                false,
                Some(IncrexExpire::Px(20)),
                false,
            )
            .unwrap();
        assert_eq!(table.num_expires, 1);
        std::thread::sleep(std::time::Duration::from_millis(40));
        // Lazy expiry is gated on `num_expires > 0`; the key must be gone.
        assert_eq!(table.get(&k), Ok(None));
        assert_eq!(table.num_expires, 0);
    }

    #[test]
    fn test_shrink_step_returns_structure_after_mass_delete() {
        let mut table = RudisTable::new();
        let n = 200_000usize;
        let key = |i: usize| Bytes::from(format!("key:{i:08}"));
        for i in 0..n {
            table.set(key(i), Bytes::from_static(b"v"), None);
        }
        let peak_segments = table.table.segments.len();
        let peak_struct = table.table.struct_bytes();
        assert!(peak_segments > 64);

        // Keep every 100th key.
        for i in (0..n).filter(|i| i % 100 != 0) {
            table.del(&key(i));
        }
        let survivors = n / 100;
        assert_eq!(table.dbsize(), survivors);
        // Nothing shrinks on its own.
        assert_eq!(table.table.segments.len(), peak_segments);

        let mut steps = 0;
        while table.shrink_step(32) > 0 {
            steps += 1;
            assert_seg_heap_bytes_consistent(&table.table);
            assert!(steps < 10_000, "shrink did not converge");
        }
        let live = table
            .table
            .segments
            .iter()
            .filter(|s| !RudisFlatTable::is_hole(s))
            .count();
        // 2,000 keys need ~8 segments at the merge threshold, not ~300.
        assert!(live <= 16, "{live} live segments for {survivors} keys");
        assert!(
            table.table.struct_bytes() * 8 < peak_struct,
            "structure {} vs peak {peak_struct}",
            table.table.struct_bytes()
        );
        assert_eq!(table.table.directory.len(), 1 << table.table.global_depth);

        // Every survivor is still reachable, by lookup and by iteration.
        for i in (0..n).step_by(100) {
            assert!(table.exists(&key(i)), "lost key {i}");
        }
        assert_eq!(table.keys(b"*").len(), survivors);
        let (mut cursor, mut seen) = (0usize, std::collections::HashSet::new());
        loop {
            let (next, batch) = table.scan(cursor, None, 500, None);
            seen.extend(batch);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen.len(), survivors);

        // And the table still grows correctly afterwards.
        for i in 0..n {
            table.set(key(i), Bytes::from_static(b"w"), None);
        }
        assert_eq!(table.dbsize(), n);
        assert_seg_heap_bytes_consistent(&table.table);
        for i in (0..n).step_by(997) {
            assert!(table.exists(&key(i)));
        }
    }

    #[test]
    fn test_shrink_step_collapses_empty_table() {
        let mut table = RudisTable::new();
        let key = |i: usize| Bytes::from(format!("k{i}"));
        for i in 0..50_000 {
            table.set(key(i), Bytes::from_static(b"v"), None);
        }
        for i in 0..50_000 {
            table.del(&key(i));
        }
        table.shrink_step(32);
        assert_eq!(table.table.segments.len(), 1);
        assert_eq!(table.table.global_depth, 0);
        assert_seg_heap_bytes_consistent(&table.table);
    }

    #[test]
    fn test_keys_and_rdb_chunk_cover_every_segment() {
        let mut table = RudisTable::new();
        let n = 50_000usize;
        for i in 0..n {
            table.set(
                Bytes::from(format!("key:{i:08}")),
                Bytes::from_static(b"v"),
                None,
            );
        }
        assert!(table.table.segments.len() > 8, "expected segment splits");
        assert_eq!(table.keys(b"*").len(), n);

        let mut buf = Vec::new();
        table.save_rdb_chunk(&mut buf);
        let mut restored = RudisTable::new();
        restored.restore_rdb_chunk(&buf).unwrap();
        assert_eq!(restored.dbsize(), n);
    }

    #[test]
    fn test_used_memory_counts_table_structure_through_growth_and_flush() {
        let mut table = RudisTable::new();
        let empty = table.used_memory();
        assert_eq!(table.data_bytes, 0);
        assert_seg_heap_bytes_consistent(&table.table);

        let n = 200_000usize;
        for i in 0..n {
            table.set(
                Bytes::from(format!("key:{i:08}")),
                Bytes::from_static(b"v"),
                None,
            );
            if i % 10_000 == 0 {
                assert_seg_heap_bytes_consistent(&table.table);
            }
        }
        assert_seg_heap_bytes_consistent(&table.table);
        assert!(table.table.segments.len() > 1, "expected segment splits");

        // Every live entry occupies at least one inline slot, so structure
        // must be at least n slots worth, and used_memory includes it.
        let slot = std::mem::size_of::<Option<RudisEntry>>();
        let structure = table.table.struct_bytes();
        assert!(structure >= n * slot, "structure {structure} < {n} slots");
        assert_eq!(table.used_memory(), table.data_bytes + structure);
        assert!(table.used_memory() > empty + n * slot);

        // Deletes reduce data bytes; the slot arrays don't shrink, but the
        // entry arenas may release slack, so structure never grows.
        for i in 0..n / 2 {
            table.del(format!("key:{i:08}").as_bytes());
        }
        assert_seg_heap_bytes_consistent(&table.table);
        assert!(table.table.struct_bytes() <= structure);

        table.table.defrag();
        assert_seg_heap_bytes_consistent(&table.table);

        let recalculated = table.recalculate_used_memory();
        assert_eq!(recalculated, table.used_memory());

        table.flushdb();
        assert_eq!(table.data_bytes, 0);
        assert_seg_heap_bytes_consistent(&table.table);
        assert!(table.used_memory() <= empty * 2);
    }

    #[test]
    fn test_used_memory_inline_entries_cost_only_overhead() {
        let mut table = RudisTable::new();
        let n = 10_000usize;
        for i in 0..n {
            // 12-byte key and 16-byte value: both inline, no heap.
            table.set(
                Bytes::from(format!("key:{i:08}")),
                Bytes::from(format!("{:016}", i)),
                None,
            );
        }
        assert_eq!(table.data_bytes, n * ENTRY_OVERHEAD);

        // Heap keys/values are charged their full length.
        let long_key = vec![b'k'; 40];
        let long_val = vec![b'v'; 100];
        table.set(Bytes::from(long_key.clone()), Bytes::from(long_val), None);
        assert_eq!(table.data_bytes, (n + 1) * ENTRY_OVERHEAD + 40 + 100);
        assert!(table.del(&long_key));
        assert_eq!(table.data_bytes, n * ENTRY_OVERHEAD);
    }

    #[test]
    fn test_used_memory_incremental_matches_recalculated() {
        let mut table = RudisTable::new();
        for i in 0..2_000usize {
            let key = if i % 3 == 0 {
                format!("a-much-longer-key-name-that-spills:{i}")
            } else {
                format!("k{i}")
            };
            let val = match i % 4 {
                0 => format!("{i}"),
                1 => "x".repeat(10),
                2 => "y".repeat(30),
                _ => "z".repeat(300),
            };
            table.set(Bytes::from(key), Bytes::from(val), None);
        }
        // Overwrites crossing the inline boundary in both directions.
        for i in (0..2_000usize).step_by(5) {
            let val = if i % 2 == 0 {
                "s".repeat(5)
            } else {
                "L".repeat(50)
            };
            table.set(Bytes::from(format!("k{i}")), Bytes::from(val), None);
        }
        for i in 0..500usize {
            let k = Bytes::from(format!("ctr{i}"));
            table.incr_by_slice_fast(&k, 1).unwrap();
            table.incr_by_slice_fast(&k, 1).unwrap();
        }
        for i in 0..200usize {
            table.setbit(Bytes::from(format!("bits{i}")), i, 1).unwrap();
            table
                .setbit(Bytes::from(format!("bits{i}")), i * 3, 1)
                .unwrap();
            // SETBIT on an int value converts it to a string.
            table.setbit(Bytes::from(format!("ctr{i}")), i, 1).unwrap();
        }
        for i in (0..2_000usize).step_by(7) {
            table.del(format!("k{i}").as_bytes());
        }
        // Keys of 18-22 bytes are inline without a TTL and heap with one.
        let ttl = Duration::from_secs(3600);
        for i in 0..300usize {
            let k = Bytes::from(format!("ttl-key-{i:012}"));
            match i % 6 {
                0 => table.set(k.clone(), Bytes::from_static(b"v"), Some(ttl)),
                1 => {
                    table.set(k.clone(), Bytes::from_static(b"v"), None);
                    table.expire(&k, ttl, Default::default());
                }
                2 => {
                    table.set(k.clone(), Bytes::from_static(b"v"), Some(ttl));
                    table.persist(&k);
                }
                3 => {
                    table.set(k.clone(), Bytes::from_static(b"v"), Some(ttl));
                    table.getset(k.clone(), Bytes::from_static(b"w")).unwrap();
                }
                4 => {
                    table.set(k.clone(), Bytes::from_static(b"v"), Some(ttl));
                    table.set(k.clone(), Bytes::from_static(b"x"), None);
                }
                _ => {
                    // Rename onto an existing key, short <-> long names.
                    table.set(k.clone(), Bytes::from_static(b"v"), Some(ttl));
                    table.set(
                        Bytes::from(format!("d{i}")),
                        Bytes::from("y".repeat(40)),
                        Some(ttl),
                    );
                    table
                        .rename(&k, Bytes::from(format!("d{i}")), false)
                        .unwrap();
                }
            }
        }
        let incremental = table.data_bytes;
        table.recalculate_used_memory();
        assert_eq!(incremental, table.data_bytes);
    }

    #[test]
    fn test_eviction_of_inline_keys_reduces_used_memory() {
        let mut table = RudisTable::new();
        for i in 0..1_000usize {
            table.set(Bytes::from(format!("k{i}")), Bytes::from_static(b"v"), None);
        }
        // Each eviction must free accounted bytes, or the eviction loop
        // would never reach its target before emptying the table.
        for _ in 0..100 {
            let before = table.used_memory();
            assert!(table.try_evict_one_key("allkeys-random").is_some());
            assert!(table.used_memory() < before);
        }
    }

    fn set_key_access(table: &mut RudisTable, key: &[u8], access: u16) {
        let idx = table.table.find(key, hash_key(key)).unwrap();
        table.table.set_slot_last_access(idx, access);
    }

    #[test]
    fn test_idletime_uses_wrapping_16bit_clock() {
        let mut table = RudisTable::new();
        table.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), None);
        // Written across the u16 boundary: still the right idle time.
        set_key_access(&mut table, b"k", lru_now().wrapping_sub(1000));
        let idle = table.idletime(b"k").unwrap();
        assert!((1000..=1001).contains(&idle), "idle {idle}");
        let (_, idle2) = table.lru_and_idletime(b"k").unwrap();
        assert!((1000..=1001).contains(&idle2));
    }

    #[test]
    #[ignore]
    fn microbench_table_get_set_2m() {
        let n = 2_000_000usize;
        let mut t = RudisTable::new();
        let keys: Vec<Bytes> = (0..n)
            .map(|i| Bytes::from(format!("key:{i:010}")))
            .collect();
        let val = Bytes::from_static(b"0123456789abcdef");
        let t0 = std::time::Instant::now();
        for k in &keys {
            t.set(k.clone(), val.clone(), None);
        }
        let ins = t0.elapsed();
        let mut rng: u64 = 0x1234_5678_9abc_def1;
        let order: Vec<usize> = (0..4_000_000)
            .map(|_| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng % n as u64) as usize
            })
            .collect();
        let mut best_get = f64::MAX;
        let mut best_set = f64::MAX;
        for _ in 0..5 {
            let t0 = std::time::Instant::now();
            let mut hits = 0usize;
            for &i in &order {
                hits += t.get(&keys[i]).unwrap().is_some() as usize;
            }
            assert_eq!(hits, order.len());
            best_get = best_get.min(t0.elapsed().as_nanos() as f64 / order.len() as f64);
            let t0 = std::time::Instant::now();
            for &i in &order {
                t.set(keys[i].clone(), val.clone(), None);
            }
            best_set = best_set.min(t0.elapsed().as_nanos() as f64 / order.len() as f64);
        }
        let mut best_del = f64::MAX;
        {
            let t0 = std::time::Instant::now();
            for k in keys.iter().take(n / 2) {
                t.del(k);
            }
            best_del = best_del.min(t0.elapsed().as_nanos() as f64 / (n / 2) as f64);
        }
        eprintln!(
            "MICROBENCH insert {:.1} ns/op  get {:.1} ns/op  set(overwrite) {:.1} ns/op  del {:.1} ns/op",
            ins.as_nanos() as f64 / n as f64,
            best_get,
            best_set,
            best_del
        );
    }

    #[test]
    fn test_lru_eviction_prefers_idle_keys() {
        let mut table = RudisTable::new();
        let n = 2_000usize;
        let now = lru_now();
        for i in 0..n {
            let k = format!("k{i}");
            table.set(Bytes::from(k.clone()), Bytes::from_static(b"v"), None);
            // Odd keys idle for an hour, even keys just accessed.
            let access = if i % 2 == 1 {
                now.wrapping_sub(3600)
            } else {
                now
            };
            set_key_access(&mut table, k.as_bytes(), access);
        }
        let mut idle_evicted = 0;
        for _ in 0..100 {
            let before: Vec<bool> = (0..n)
                .map(|i| table.key_is_live(format!("k{i}").as_bytes()))
                .collect();
            assert!(table.try_evict_one_key("allkeys-lru").is_some());
            let gone = (0..n)
                .find(|&i| before[i] && !table.key_is_live(format!("k{i}").as_bytes()))
                .unwrap();
            if gone % 2 == 1 {
                idle_evicted += 1;
            }
        }
        // Random eviction would give ~50; each 10-key sample holds an idle
        // key with probability ~1 - 2^-10.
        assert!(
            idle_evicted >= 95,
            "only {idle_evicted}/100 evictions hit idle keys"
        );
    }

    /// Randomized differential test of the dense-arena segments against a
    /// `HashMap` model: inserts, overwrites, deletes, splits, merges,
    /// rebuilds and defrag, checking slot/arena cross-links throughout.
    #[test]
    fn test_flat_table_arena_matches_model() {
        let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut table = RudisTable::new();
        let mut model: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        for round in 0..6 {
            // Grow phase (forces splits), then shrink phase (merges/trims).
            let space = if round % 2 == 0 { 40_000 } else { 4_000 };
            for step in 0..60_000u32 {
                let k = format!("key:{}", next() % space).into_bytes();
                match next() % 10 {
                    0..=5 => {
                        let v = format!("v{}", next() % 1000).into_bytes();
                        table.set(Bytes::from(k.clone()), Bytes::from(v.clone()), None);
                        model.insert(k, v);
                    }
                    6..=8 => {
                        assert_eq!(table.del(&k), model.remove(&k).is_some());
                    }
                    _ => {
                        let got = table.get(&k).unwrap();
                        assert_eq!(got.as_deref(), model.get(&k).map(|v| v.as_slice()));
                    }
                }
                if step % 5_000 == 0 {
                    table.table.check_invariants();
                    assert_seg_heap_bytes_consistent(&table.table);
                }
            }
            // Bulk delete most keys, then merge and defrag.
            let keys: Vec<Vec<u8>> = model.keys().cloned().collect();
            for k in keys.iter().take(keys.len() * 9 / 10) {
                assert!(table.del(k));
                model.remove(k);
            }
            while table.table.shrink_step(32) > 0 {}
            table.table.check_invariants();
            assert_seg_heap_bytes_consistent(&table.table);
            if round == 3 {
                table.table.defrag();
                table.table.check_invariants();
                assert_seg_heap_bytes_consistent(&table.table);
            }
            assert_eq!(table.dbsize(), model.len());
            for (k, v) in &model {
                assert_eq!(table.get(k).unwrap().as_deref(), Some(v.as_slice()));
            }
            assert_eq!(table.table.entries().count(), model.len());
        }
        let recalculated = table.recalculate_used_memory();
        assert_eq!(recalculated, table.used_memory());
    }

    /// RDB files are untrusted input (copied between hosts, received from a
    /// master). Mutated files with a valid checksum, so they get past the
    /// CRC and into the record parser, must load or fail cleanly, never
    /// panic. `RUDIS_FUZZ_ITERS` raises the iteration count.
    #[test]
    fn test_load_rdb_bytes_fuzz() {
        let mut src = crate::shard::ShardDb::new(0);
        for i in 0..20 {
            src.table.set(
                Bytes::from(format!("key:{i}")),
                Bytes::from(format!("value-{i}-{}", "x".repeat(i * 7))),
                (i % 3 == 0).then(|| Duration::from_secs(3600)),
            );
        }
        src.crdt_set(Bytes::from_static(b"crdt:a"), Bytes::from_static(b"v"));
        src.crdt_incrby(Bytes::from_static(b"crdt:b"), 5).unwrap();
        src.crdt_sadd(Bytes::from_static(b"crdt:c"), Bytes::from_static(b"m"));
        let mut chunk = Vec::new();
        src.save_rdb_chunk(&mut chunk);
        let mut body = vec![0xFE, 0x00];
        body.extend_from_slice(&chunk);
        body.push(0xFF);

        let iters = std::env::var("RUDIS_FUZZ_ITERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3_000usize);
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..iters {
            let mut v = body.clone();
            for _ in 0..=(next() % 4) {
                let len = v.len() as u64;
                let i = (next() % len.max(1)) as usize;
                match next() % 6 {
                    0 if !v.is_empty() => v[i] ^= 1 << (next() % 8),
                    1 if !v.is_empty() => {
                        v[i] = [0x00, 0xFF, 0x7F, 0x80, 0xFE, 0xFC][(next() % 6) as usize]
                    }
                    2 if v.len() >= i + 4 => {
                        // Hostile 32-bit length or count.
                        let n =
                            [u32::MAX, 0x8000_0000, 0x7FFF_FFFF, 1 << 24][(next() % 4) as usize];
                        v[i..i + 4].copy_from_slice(&n.to_le_bytes());
                    }
                    3 if !v.is_empty() => v.truncate(i),
                    4 if !v.is_empty() => {
                        let j = (i + 1 + (next() % 16) as usize).min(v.len());
                        v.drain(i..j);
                    }
                    _ => {
                        let b = next() as u8;
                        v.insert(i, b);
                    }
                }
            }
            let rdb = rdb_with_body(&v);
            let res = std::panic::catch_unwind(|| {
                let mut db = crate::shard::ShardDb::new(0);
                let _ = load_rdb_bytes(&rdb, &mut db, 0, 1);
            });
            assert!(res.is_ok(), "RDB loader panicked on body {v:02x?}");
        }
    }

    #[test]
    fn test_load_rdb_rejects_truncated_corrupt_and_foreign_files() {
        let mut src = crate::shard::ShardDb::new(0);
        src.table
            .set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), None);
        let mut chunk = Vec::new();
        src.save_rdb_chunk(&mut chunk);

        // Well-formed file loads.
        let mut body = vec![0xFE, 0x00];
        body.extend_from_slice(&chunk);
        body.push(0xFF);
        let mut db = crate::shard::ShardDb::new(0);
        assert_eq!(
            load_rdb_bytes(&rdb_with_body(&body), &mut db, 0, 1).unwrap(),
            1
        );

        // Empty file is an empty dataset.
        assert_eq!(load_rdb_bytes(&[], &mut db, 0, 1).unwrap(), 0);

        // Short file is truncated, not empty.
        assert!(load_rdb_bytes(b"REDIS0011\xFE", &mut db, 0, 1).is_err());

        // Record cut short (valid checksum, missing EOF opcode).
        let mut cut = vec![0xFE, 0x00];
        cut.extend_from_slice(&chunk[..chunk.len() - 1]);
        let err = load_rdb_bytes(&rdb_with_body(&cut), &mut db, 0, 1).unwrap_err();
        assert!(err.to_string().contains("RDB parse error"), "{err}");

        // Redis/Valkey RDB (AUX field first) is reported, not silently empty.
        let foreign = rdb_with_body(b"\xFA\x09redis-ver\x058.1.0\xFE\x00\xFF");
        let err = load_rdb_bytes(&foreign, &mut db, 0, 1).unwrap_err();
        assert!(err.to_string().contains("Redis/Valkey RDB"), "{err}");
    }

    /// An RDB holds every shard's CRDT state in one record; each shard loads
    /// only the keys it owns, so none ends up on two shards.
    #[test]
    fn test_load_rdb_keeps_only_the_crdt_keys_this_shard_owns() {
        let mut src = crate::shard::ShardDb::new(0);
        let keys: Vec<Bytes> = (0..16).map(|i| Bytes::from(format!("crdt:{i}"))).collect();
        for k in &keys {
            src.crdt_set(k.clone(), Bytes::from_static(b"v"));
            src.crdt_incrby(k.clone(), 1).unwrap();
            src.crdt_sadd(k.clone(), Bytes::from_static(b"m"));
        }
        let mut chunk = Vec::new();
        src.save_rdb_chunk(&mut chunk);
        let mut body = vec![0xFE, 0x00];
        body.extend_from_slice(&chunk);
        body.push(0xFF);
        let rdb = rdb_with_body(&body);

        let mut seen = 0;
        for shard in 0..2 {
            let mut db = crate::shard::ShardDb::new(0).with_shard(shard);
            load_rdb_bytes(&rdb, &mut db, shard, 2).unwrap();
            let mut owned: Vec<&Bytes> = keys
                .iter()
                .filter(|k| crate::router::target_shard(k, 2) == shard)
                .collect();
            owned.sort();
            assert!(!owned.is_empty() && owned.len() < keys.len());
            for store in [
                db.crdt_store.registers.keys().collect::<Vec<_>>(),
                db.crdt_store.counters.keys().collect(),
                db.crdt_store.sets.keys().collect(),
            ] {
                let mut store = store;
                store.sort();
                assert_eq!(store, owned);
            }
            seen += owned.len();
        }
        assert_eq!(seen, keys.len());
    }

    #[test]
    fn test_rename_keeps_ttl_stored_in_key() {
        let mut t = RudisTable::new();
        // Short (inline TTL), medium (inline key, heap with TTL) and long keys.
        for (src, dst) in [
            ("a", "b"),
            ("src-key-of-20-bytes!", "dst-key-of-20-bytes!"),
            ("a-much-longer-source-key-name", "short"),
        ] {
            t.set(
                Bytes::from(src),
                Bytes::from_static(b"v"),
                Some(Duration::from_secs(100)),
            );
            assert_eq!(t.rename(src.as_bytes(), Bytes::from(dst), false), Ok(true));
            let h = hash_key(dst.as_bytes());
            let (_, e) = t.table.find_entry(dst.as_bytes(), h).unwrap();
            let left = e
                .expire_at()
                .expect("TTL kept")
                .saturating_duration_since(Instant::now());
            assert!(left > Duration::from_secs(95), "{src}->{dst}: {left:?}");
            assert_eq!(e.key.as_slice(), dst.as_bytes());
        }
    }

    #[test]
    fn test_rudis_entry_size() {
        println!("RudisValue = {}", std::mem::size_of::<RudisValue>());
        println!("RudisEntry = {}", std::mem::size_of::<RudisEntry>());
        println!(
            "Option<RudisEntry> = {}",
            std::mem::size_of::<Option<RudisEntry>>()
        );
        assert_eq!(std::mem::size_of::<RudisValue>(), 16);
        assert_eq!(std::mem::size_of::<RudisEntry>(), 40);
        assert_eq!(std::mem::size_of::<Option<RudisEntry>>(), 40);
    }

    #[test]
    fn test_exists_and_num_expires_tracking() {
        let mut table = RudisTable::new();
        assert_eq!(table.num_expires, 0);

        // Key without TTL
        let k1 = Bytes::from_static(b"k1");
        table.set(k1.clone(), Bytes::from_static(b"v1"), None);
        assert_eq!(table.num_expires, 0);
        let h1 = hash_key(b"k1");
        assert!(table.exists_with_hash(b"k1", h1));
        assert!(table.exists(b"k1"));
        assert!(!table.exists(b"k_missing"));

        // Key with TTL
        let k2 = Bytes::from_static(b"k2");
        table.set(
            k2.clone(),
            Bytes::from_static(b"v2"),
            Some(Duration::from_millis(50)),
        );
        assert_eq!(table.num_expires, 1);
        let h2 = hash_key(b"k2");
        assert!(table.exists_with_hash(b"k2", h2));
        assert!(table.exists(b"k2"));

        // Persist removes expiration
        assert!(table.persist(b"k2"));
        assert_eq!(table.num_expires, 0);
        assert!(table.exists(b"k2"));

        // Add expiration back with expire()
        assert!(table.expire(
            b"k2",
            Duration::from_millis(10),
            crate::resp::ExpireOptions::default()
        ));
        assert_eq!(table.num_expires, 1);
        std::thread::sleep(Duration::from_millis(20));
        assert!(!table.exists(b"k2"));
        assert_eq!(table.num_expires, 0);

        // Delete cleans up num_expires
        table.set(
            k2.clone(),
            Bytes::from_static(b"v2"),
            Some(Duration::from_secs(60)),
        );
        assert_eq!(table.num_expires, 1);
        assert!(table.del(b"k2"));
        assert_eq!(table.num_expires, 0);
        assert!(!table.exists(b"k2"));

        // Flushdb resets
        table.set(
            k1.clone(),
            Bytes::from_static(b"v1"),
            Some(Duration::from_secs(60)),
        );
        assert_eq!(table.num_expires, 1);
        table.flushdb();
        assert_eq!(table.num_expires, 0);
        assert!(!table.exists(b"k1"));
    }

    #[test]
    fn test_lpop_one_and_rpop_one_zero_alloc() {
        let mut table = RudisTable::new();
        let k = Bytes::from_static(b"list_k");
        let v1 = Bytes::from_static(b"val1");
        let v2 = Bytes::from_static(b"val2");

        table
            .lpush_slice_fast(&k, &[v1.clone(), v2.clone()])
            .unwrap();

        assert_eq!(table.lpop_one(b"list_k").unwrap(), Some(v2));
        assert_eq!(table.rpop_one(b"list_k").unwrap(), Some(v1));
        assert_eq!(table.lpop_one(b"list_k").unwrap(), None);
        assert_eq!(table.rpop_one(b"list_k").unwrap(), None);
        assert!(!table.exists(b"list_k"));

        table.set(k.clone(), Bytes::from_static(b"str_val"), None);
        assert!(table.lpop_one(b"list_k").is_err());
        assert!(table.rpop_one(b"list_k").is_err());
    }

    #[test]
    fn test_del_with_hash_and_sadd_single_pass() {
        let mut table = RudisTable::new();
        let k = Bytes::from_static(b"set_key");
        let m1 = Bytes::from_static(b"m1");
        let m2 = Bytes::from_static(b"m2");

        let added = table
            .sadd_slice_fast(&k, &[m1.clone(), m2.clone()])
            .unwrap();
        assert_eq!(added, 2);

        // Inserting duplicates should return 0
        let added_dup = table.sadd_slice_fast(&k, &[m1]).unwrap();
        assert_eq!(added_dup, 0);

        // sismember_compact checks
        assert_eq!(
            table.sismember_compact(b"set_key", b"m1").unwrap(),
            crate::shard::CompactResp::INT_1
        );
        assert_eq!(
            table.sismember_compact(b"set_key", b"m_missing").unwrap(),
            crate::shard::CompactResp::INT_0
        );

        // del_with_hash
        let h = hash_key(b"set_key");
        assert!(table.del_with_hash(b"set_key", h));
        assert!(!table.del_with_hash(b"set_key", h));
    }

    #[test]
    fn test_in_place_incr_single_pass() {
        let mut table = RudisTable::new();
        let key = Bytes::from_static(b"counter");
        assert_eq!(table.incr_by_slice_fast(&key, 1).unwrap(), 1);
        assert_eq!(table.incr_by_slice_fast(&key, 1).unwrap(), 2);
        assert_eq!(table.incr_by_slice_fast(&key, 5).unwrap(), 7);

        let h = hash_key(b"counter");
        let idx = table.table.find(b"counter", h).unwrap();
        let entry = table.table.get_slot(idx).unwrap();
        assert_eq!(entry.val, RudisValue::Int(7));
    }

    #[test]
    fn test_rudis_set_fx_hasher_and_dedup() {
        let mut set = RudisSet::with_capacity(100);
        assert!(matches!(set, RudisSet::Full(_)));
        assert!(set.insert(Bytes::from_static(b"foo")));
        assert!(!set.insert(Bytes::from_static(b"foo")));
        assert!(set.contains(b"foo"));
        assert!(!set.contains(b"bar"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_rudis_set_small_cached_hash_and_rejection() {
        let mut set = RudisSet::with_capacity(16);
        assert!(matches!(set, RudisSet::Small(_)));

        // Test insertion and deduplication
        assert!(set.insert(Bytes::from_static(b"member_alpha")));
        assert!(!set.insert(Bytes::from_static(b"member_alpha")));
        assert!(set.insert(Bytes::from_static(b"member_bravo")));
        assert_eq!(set.len(), 2);

        // Positive lookups
        assert!(set.contains(b"member_alpha"));
        assert!(set.contains(b"member_bravo"));

        // Negative lookups (same length as alpha/bravo: 12 bytes)
        assert!(!set.contains(b"member_charl"));
        // Negative lookups (different lengths)
        assert!(!set.contains(b"short"));
        assert!(!set.contains(b"very_long_member_name_that_does_not_exist"));

        // Removal
        assert!(set.remove(b"member_alpha"));
        assert!(!set.contains(b"member_alpha"));
        assert_eq!(set.len(), 1);
        assert!(!set.remove(b"member_alpha"));

        // Table level SISMEMBER and SISMEMBER_COMPACT
        let mut table = RudisTable::new();
        table
            .sadd_slice_fast(
                &Bytes::from_static(b"set:test"),
                &[Bytes::from_static(
                    b"payload64bytes__________________________________________________",
                )],
            )
            .unwrap();

        // Hit
        assert!(
            table
                .sismember(
                    b"set:test",
                    b"payload64bytes__________________________________________________"
                )
                .unwrap()
        );
        assert_eq!(
            table
                .sismember_compact(
                    b"set:test",
                    b"payload64bytes__________________________________________________"
                )
                .unwrap(),
            crate::shard::CompactResp::INT_1
        );

        // Miss with exact same length
        assert!(
            !table
                .sismember(
                    b"set:test",
                    b"diffload64bytes__________________________________________________"
                )
                .unwrap()
        );
        assert_eq!(
            table
                .sismember_compact(
                    b"set:test",
                    b"diffload64bytes__________________________________________________"
                )
                .unwrap(),
            crate::shard::CompactResp::INT_0
        );

        // Non-existent key
        assert!(!table.sismember(b"no_such_set", b"any").unwrap());
        assert_eq!(
            table.sismember_compact(b"no_such_set", b"any").unwrap(),
            crate::shard::CompactResp::INT_0
        );
    }

    #[test]
    fn test_small_set_contains_and_prepared_insert() {
        // Test RudisSet::contains fast paths
        let mut set = RudisSet::new();
        assert!(!set.contains(b"item1"));

        set.insert(Bytes::from_static(b"member_alpha"));
        assert!(set.contains(b"member_alpha"));
        assert!(!set.contains(b"member_beta"));
        assert!(!set.contains(b"diff_len"));

        set.insert(Bytes::from_static(b"member_beta"));
        assert!(set.contains(b"member_alpha"));
        assert!(set.contains(b"member_beta"));
        assert!(!set.contains(b"member_gamma"));

        set.insert(Bytes::from_static(b"member_gamma"));
        set.insert(Bytes::from_static(b"member_delta"));
        assert_eq!(set.len(), 4);
        assert!(set.contains(b"member_alpha"));
        assert!(set.contains(b"member_beta"));
        assert!(set.contains(b"member_gamma"));
        assert!(set.contains(b"member_delta"));
        assert!(!set.contains(b"member_omega"));

        set.insert(Bytes::from_static(b"member_5"));
        assert_eq!(set.len(), 5);
        assert!(set.contains(b"member_5"));
        assert!(!set.contains(b"missing"));

        // Test insert_prepared execution for HSET, SADD, and ZADD on new keys
        let mut table = RudisTable::new();
        let hk = Bytes::from_static(b"h_key");
        let fields = vec![(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))];
        let hset_res = table.hset_slice_fast(&hk, &fields).unwrap();
        assert_eq!(hset_res, 1);
        let hget_res = table.hget_compact(b"h_key", b"f1").unwrap();
        assert_eq!(
            hget_res,
            crate::shard::CompactResp::from_bulk(&Bytes::from_static(b"v1"))
        );

        let sk = Bytes::from_static(b"s_key");
        let members = vec![Bytes::from_static(b"sm1")];
        let sadd_res = table.sadd_slice_fast(&sk, &members).unwrap();
        assert_eq!(sadd_res, 1);
        let sism_res = table.sismember_compact(b"s_key", b"sm1").unwrap();
        assert_eq!(sism_res, crate::shard::CompactResp::INT_1);

        let zk = Bytes::from_static(b"z_key");
        let zelems = vec![(10.5, Bytes::from_static(b"zm1"))];
        let zadd_res = table
            .zadd_slice_fast(&zk, &zelems, ZAddFlags::default())
            .unwrap();
        assert_eq!(zadd_res.0, 1);
        let zscore_res = table.zscore(b"z_key", b"zm1").unwrap();
        assert_eq!(zscore_res, Some(10.5));
    }

    #[test]
    fn test_from_owned_bulk_and_single_pass_lpush_incr_hget() {
        let mut table = RudisTable::new();

        // Single-pass INCR on new and existing keys
        let ik = Bytes::from_static(b"counter_1");
        assert_eq!(table.incr_by_slice_fast(&ik, 5).unwrap(), 5);
        assert_eq!(table.incr_by_slice_fast(&ik, 10).unwrap(), 15);

        // Single-pass LPUSH + LPOP with from_owned_bulk (both <=20 bytes and >20 bytes)
        let lk = Bytes::from_static(b"list_1");
        let large_payload = Bytes::from_static(b"payload_longer_than_twenty_bytes_for_bulk_move");
        let small_payload = Bytes::from_static(b"short_payload");
        let pushed = table
            .lpush_slice_fast(&lk, &[large_payload.clone(), small_payload.clone()])
            .unwrap();
        assert_eq!(pushed, 2);

        let popped_small = table.lpop_one(b"list_1").unwrap().unwrap();
        let resp_small = crate::shard::CompactResp::from_owned_bulk(popped_small);
        assert!(matches!(
            resp_small,
            crate::shard::CompactResp::Small { .. }
        ));

        let popped_large = table.lpop_one(b"list_1").unwrap().unwrap();
        let resp_large = crate::shard::CompactResp::from_owned_bulk(popped_large);
        assert!(matches!(resp_large, crate::shard::CompactResp::Bulk(_)));

        // HGET compact 1-field and multi-field fast path
        let hk = Bytes::from_static(b"h_fast");
        table
            .hset_slice_fast(
                &hk,
                &[(Bytes::from_static(b"f1"), Bytes::from_static(b"val1"))],
            )
            .unwrap();
        assert_eq!(
            table.hget_compact(b"h_fast", b"f1").unwrap(),
            crate::shard::CompactResp::from_bulk(&Bytes::from_static(b"val1"))
        );
        assert_eq!(
            table.hget_compact(b"h_fast", b"f_missing").unwrap(),
            crate::shard::CompactResp::NULL
        );
    }

    #[test]
    fn test_with_hash_methods_and_array1_bulk_compact_resp() {
        assert_eq!(std::mem::size_of::<crate::shard::CompactResp>(), 40);

        let mut table = RudisTable::new();
        let mut scratch = Vec::new();

        // 1. ZRANGE compact with hash (empty -> 1 element Array1Bulk -> 2 elements)
        let zk = Bytes::from_static(b"z_hash_key");
        let zh = hash_key(&zk);
        let zopts = ZRangeOpts {
            start: 0,
            stop: 10,
            ..Default::default()
        };
        assert_eq!(
            table
                .zrange_compact_with_hash(&zk, zh, &zopts, false, &mut scratch)
                .unwrap(),
            crate::shard::CompactResp::EMPTY_ARRAY
        );

        let zm1 = Bytes::from_static(b"z_member_one_payload_longer_than_30_bytes");
        table
            .zadd_slice_with_hash(&zk, zh, &[(1.0, zm1.clone())], ZAddFlags::default())
            .unwrap();
        let zresp1 = table
            .zrange_compact_with_hash(&zk, zh, &zopts, false, &mut scratch)
            .unwrap();
        assert_eq!(zresp1, crate::shard::CompactResp::Array1Bulk(zm1.clone()));
        let mut serialized = Vec::new();
        zresp1.write_to(&mut serialized);
        assert_eq!(
            serialized,
            b"*1\r\n$41\r\nz_member_one_payload_longer_than_30_bytes\r\n"
        );

        // 2. LRANGE compact with hash (empty -> 1 element Array1Bulk -> pop)
        let lk = Bytes::from_static(b"l_hash_key");
        let lh = hash_key(&lk);
        assert_eq!(
            table
                .lrange_compact_with_hash(&lk, lh, 0, 10, &mut scratch)
                .unwrap(),
            crate::shard::CompactResp::EMPTY_ARRAY
        );
        let lm1 = Bytes::from_static(b"list_element_one_payload_over_30_bytes");
        table
            .lpush_slice_with_hash(&lk, lh, std::slice::from_ref(&lm1))
            .unwrap();
        let lresp1 = table
            .lrange_compact_with_hash(&lk, lh, 0, 10, &mut scratch)
            .unwrap();
        assert_eq!(lresp1, crate::shard::CompactResp::Array1Bulk(lm1.clone()));
        assert_eq!(
            table.lpop_one_with_hash(&lk, lh).unwrap(),
            Some(lm1.clone())
        );
    }

    #[test]
    fn test_flat_table_crud_and_growth() {
        let mut table = RudisFlatTable::new(16);
        assert_eq!(table.capacity(), 16);

        // Insert 100 items to force multiple resizes
        for i in 0..100 {
            let key = Bytes::from(format!("key_{}", i));
            let val = Bytes::from(format!("val_{}", i));
            let entry = RudisEntry::new(
                CompactKey::new(&key),
                RudisValue::String(val.into()),
                Expiry::from(None),
            );
            table.insert(entry);
        }
        assert_eq!(table.len(), 100);
        assert!(table.capacity() >= 128);

        // Verify all items are present
        for i in 0..100 {
            let key = format!("key_{}", i);
            let h = hash_key(key.as_bytes());
            let idx = table.find(key.as_bytes(), h).expect("key must be found");
            let entry = table.get_slot(idx).unwrap();
            assert_eq!(entry.key, key.as_bytes());
            if let RudisValue::String(ref b) = entry.val {
                assert_eq!(b, format!("val_{}", i).as_bytes());
            } else {
                panic!("expected String value");
            }
        }

        // Test removal
        let h = hash_key(b"key_42");
        let idx = table.find(b"key_42", h).unwrap();
        let removed = table.remove(idx).unwrap();
        assert_eq!(removed.key, b"key_42".as_slice());
        assert_eq!(table.len(), 99);
        assert!(table.find(b"key_42", h).is_none());
    }

    #[test]
    fn test_rudis_table_redis_operations() {
        let mut table = RudisTable::new();

        // 1. SET & GET
        table.set(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"), None);
        assert_eq!(table.get(b"k1").unwrap(), Some(Bytes::from_static(b"v1")));
        assert_eq!(table.get(b"nonexistent").unwrap(), None);

        // 2. INCRBY
        assert_eq!(
            table.incr_by(Bytes::from_static(b"counter"), 10).unwrap(),
            10
        );
        assert_eq!(
            table.incr_by(Bytes::from_static(b"counter"), -3).unwrap(),
            7
        );

        // 3. TTL & Expiration
        table.set(
            Bytes::from_static(b"exp_key"),
            Bytes::from_static(b"temp"),
            Some(Duration::from_millis(50)),
        );
        assert_eq!(
            table.get(b"exp_key").unwrap(),
            Some(Bytes::from_static(b"temp"))
        );
        assert!(table.ttl(b"exp_key", false) >= 0);

        thread::sleep(Duration::from_millis(60));
        assert_eq!(table.get(b"exp_key").unwrap(), None, "Key must be expired");
        assert_eq!(table.ttl(b"exp_key", false), -2);

        // 4. Hashes
        let added = table
            .hset(
                Bytes::from_static(b"user:1"),
                vec![
                    (Bytes::from_static(b"name"), Bytes::from_static(b"alice")),
                    (Bytes::from_static(b"age"), Bytes::from_static(b"30")),
                ],
            )
            .unwrap();
        assert_eq!(added, 2);

        assert_eq!(
            table.hget(b"user:1", b"name").unwrap(),
            Some(Bytes::from_static(b"alice"))
        );
        assert_eq!(table.hlen(b"user:1").unwrap(), 2);
        assert!(table.hexists(b"user:1", b"name").unwrap());
        assert!(!table.hexists(b"user:1", b"missing").unwrap());

        // WRONGTYPE test
        assert!(table.get(b"user:1").is_err());
        assert!(table.hget(b"k1", b"field").is_err());
    }

    #[test]
    fn test_rudis_table_zset_operations() {
        let mut table = RudisTable::new();

        // 1. ZADD
        let (added, _) = table
            .zadd(
                Bytes::from_static(b"myzset"),
                vec![
                    (10.0, Bytes::from_static(b"m1")),
                    (20.5, Bytes::from_static(b"m2")),
                    (5.0, Bytes::from_static(b"m3")),
                ],
                ZAddFlags::default(),
            )
            .unwrap();
        assert_eq!(added, 3);
        assert_eq!(table.zcard(b"myzset").unwrap(), 3);

        // 2. ZSCORE & ZRANK
        assert_eq!(table.zscore(b"myzset", b"m2").unwrap(), Some(20.5));
        assert_eq!(table.zscore(b"myzset", b"nonexistent").unwrap(), None);
        assert_eq!(
            table.zrank(b"myzset", b"m3", false, false).unwrap(),
            Some((0, None))
        ); // 5.0
        assert_eq!(
            table.zrank(b"myzset", b"m1", false, false).unwrap(),
            Some((1, None))
        ); // 10.0
        assert_eq!(
            table.zrank(b"myzset", b"m2", false, false).unwrap(),
            Some((2, None))
        ); // 20.5
        assert_eq!(
            table.zrank(b"myzset", b"m3", true, false).unwrap(),
            Some((2, None))
        ); // rev rank

        // 3. ZRANGE by index
        let opts = ZRangeOpts {
            start: 0,
            stop: -1,
            with_scores: true,
            ..Default::default()
        };
        let res = table.zrange(b"myzset", &opts).unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].0, b"m3".as_slice());
        assert_eq!(res[1].0, b"m1".as_slice());
        assert_eq!(res[2].0, b"m2".as_slice());

        // 4. ZCOUNT
        assert_eq!(table.zcount(b"myzset", 5.0, true, 15.0, true).unwrap(), 2);
        assert_eq!(table.zcount(b"myzset", 5.0, false, 15.0, true).unwrap(), 1);

        // 5. ZINCRBY
        let new_score = table
            .zincrby(
                Bytes::from_static(b"myzset"),
                15.0,
                Bytes::from_static(b"m3"),
            )
            .unwrap();
        assert_eq!(new_score, 20.0);
        assert_eq!(table.zscore(b"myzset", b"m3").unwrap(), Some(20.0));

        // 6. ZPOPMIN & ZPOPMAX
        let popped_min = table.zpopmin(b"myzset", 1).unwrap();
        assert_eq!(popped_min.len(), 1);
        assert_eq!(popped_min[0].0, b"m1".as_slice()); // 10.0

        let popped_max = table.zpopmax(b"myzset", 1).unwrap();
        assert_eq!(popped_max.len(), 1);
        assert_eq!(popped_max[0].0, b"m2".as_slice()); // 20.5

        // Remaining should be m3 (20.0)
        assert_eq!(table.zcard(b"myzset").unwrap(), 1);

        // 7. ZREM
        let removed = table.zrem(b"myzset", &[Bytes::from_static(b"m3")]).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(table.zcard(b"myzset").unwrap(), 0);

        // WRONGTYPE test
        table.set(
            Bytes::from_static(b"str_key"),
            Bytes::from_static(b"val"),
            None,
        );
        assert!(table.zcard(b"str_key").is_err());
    }

    #[test]
    fn test_rudis_table_keyspace_inspection() {
        let mut table = RudisTable::new();
        assert_eq!(table.random_key(), None);
        assert_eq!(table.keys(b"*").len(), 0);

        table.set(
            Bytes::from_static(b"alpha:1"),
            Bytes::from_static(b"a"),
            None,
        );
        table.set(
            Bytes::from_static(b"alpha:2"),
            Bytes::from_static(b"b"),
            None,
        );
        table.set(
            Bytes::from_static(b"beta:1"),
            Bytes::from_static(b"c"),
            None,
        );

        // KEYS
        let matched = table.keys(b"alpha:*");
        assert_eq!(matched.len(), 2);
        assert!(matched.contains(&Bytes::from_static(b"alpha:1")));
        assert!(matched.contains(&Bytes::from_static(b"alpha:2")));

        // RANDOMKEY
        let rk = table.random_key();
        assert!(rk.is_some());

        // SCAN
        let (next_cursor, scanned) = table.scan(0, Some(b"alpha:*"), 10, None);
        assert_eq!(next_cursor, 0); // Scanned whole small table
        assert_eq!(scanned.len(), 2);

        // EXPIRETIME
        assert_eq!(table.expiretime(b"non_exist", false), -2);
        assert_eq!(table.expiretime(b"alpha:1", false), -1);
        table.expire(
            b"alpha:1",
            Duration::from_secs(50),
            crate::resp::ExpireOptions::default(),
        );
        let exp = table.expiretime(b"alpha:1", false);
        assert!(exp > 0);

        // Test ExpireOptions NX / XX / GT / LT
        assert!(!table.expire(
            b"alpha:1",
            Duration::from_secs(100),
            crate::resp::ExpireOptions {
                nx: true,
                ..Default::default()
            }
        ));
        assert!(table.expire(
            b"alpha:1",
            Duration::from_secs(100),
            crate::resp::ExpireOptions {
                xx: true,
                ..Default::default()
            }
        ));
        assert!(!table.expire(
            b"alpha:1",
            Duration::from_secs(50),
            crate::resp::ExpireOptions {
                gt: true,
                ..Default::default()
            }
        ));
        assert!(table.expire(
            b"alpha:1",
            Duration::from_secs(200),
            crate::resp::ExpireOptions {
                gt: true,
                ..Default::default()
            }
        ));
        assert!(!table.expire(
            b"alpha:1",
            Duration::from_secs(300),
            crate::resp::ExpireOptions {
                lt: true,
                ..Default::default()
            }
        ));
        assert!(table.expire(
            b"alpha:1",
            Duration::from_secs(50),
            crate::resp::ExpireOptions {
                lt: true,
                ..Default::default()
            }
        ));
    }

    #[test]
    fn test_rudis_table_bitmaps_and_hll() {
        let mut table = RudisTable::new();

        // 1. SETBIT & GETBIT
        // 'a' in ASCII is 0b01100001 (byte 0: bit 1, 2, 7 are 1)
        assert_eq!(
            table.setbit(Bytes::from_static(b"bm"), 1, 1).unwrap(),
            (0, true)
        );
        assert_eq!(
            table.setbit(Bytes::from_static(b"bm"), 2, 1).unwrap(),
            (0, true)
        );
        assert_eq!(
            table.setbit(Bytes::from_static(b"bm"), 7, 1).unwrap(),
            (0, true)
        );
        assert_eq!(table.getbit(b"bm", 1).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 2).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 3).unwrap(), 0);
        assert_eq!(table.getbit(b"bm", 7).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 100).unwrap(), 0);
        assert_eq!(table.get(b"bm").unwrap(), Some(Bytes::from_static(b"a")));

        // 2. BITCOUNT
        assert_eq!(table.bitcount(b"bm", None, None, false).unwrap(), 3);
        // Set bit in byte 1 (offset 15 = bit 7 of byte 1)
        assert_eq!(
            table.setbit(Bytes::from_static(b"bm"), 15, 1).unwrap(),
            (0, true)
        );
        assert_eq!(table.bitcount(b"bm", None, None, false).unwrap(), 4);
        assert_eq!(table.bitcount(b"bm", Some(0), Some(0), false).unwrap(), 3);
        assert_eq!(table.bitcount(b"bm", Some(1), Some(1), false).unwrap(), 1);

        // 3. BITPOS
        assert_eq!(table.bitpos(b"bm", 1, None, None, false).unwrap(), 1);
        assert_eq!(table.bitpos(b"bm", 0, None, None, false).unwrap(), 0);
        assert_eq!(table.bitpos(b"bm", 1, Some(1), None, false).unwrap(), 15);

        // 4. BITOP
        table.set(Bytes::from_static(b"k1"), Bytes::from_static(b"\x0f"), None); // 00001111
        table.set(Bytes::from_static(b"k2"), Bytes::from_static(b"\x33"), None); // 00110011
        assert_eq!(
            table
                .bitop(
                    "AND",
                    Bytes::from_static(b"kand"),
                    &[Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]
                )
                .unwrap(),
            1
        );
        assert_eq!(
            table.get(b"kand").unwrap(),
            Some(Bytes::from_static(b"\x03"))
        );

        assert_eq!(
            table
                .bitop(
                    "OR",
                    Bytes::from_static(b"kor"),
                    &[Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]
                )
                .unwrap(),
            1
        );
        assert_eq!(
            table.get(b"kor").unwrap(),
            Some(Bytes::from_static(b"\x3f"))
        );

        assert_eq!(
            table
                .bitop(
                    "XOR",
                    Bytes::from_static(b"kxor"),
                    &[Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]
                )
                .unwrap(),
            1
        );
        assert_eq!(
            table.get(b"kxor").unwrap(),
            Some(Bytes::from_static(b"\x3c"))
        );

        assert_eq!(
            table
                .bitop(
                    "NOT",
                    Bytes::from_static(b"knot"),
                    &[Bytes::from_static(b"k1")]
                )
                .unwrap(),
            1
        );
        assert_eq!(
            table.get(b"knot").unwrap(),
            Some(Bytes::from_static(b"\xf0"))
        );

        // An empty result deletes the destination, like Redis.
        assert_eq!(
            table
                .bitop(
                    "AND",
                    Bytes::from_static(b"kor"),
                    &[Bytes::from_static(b"nokey1"), Bytes::from_static(b"nokey2")]
                )
                .unwrap(),
            0
        );
        assert!(!table.exists(b"kor"));

        // 5. HYPERLOGLOG: PFADD & PFCOUNT
        let elements_a = vec![
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"zap"),
            Bytes::from_static(b"a"),
        ];
        assert!(
            table
                .pfadd(Bytes::from_static(b"hll1"), &elements_a)
                .unwrap()
        );
        assert!(
            !table
                .pfadd(Bytes::from_static(b"hll1"), &elements_a)
                .unwrap()
        ); // No updates
        let c1 = table.pfcount(&[Bytes::from_static(b"hll1")]).unwrap();
        assert_eq!(c1, 4);

        let elements_b = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
            Bytes::from_static(b"foo"),
        ];
        assert!(
            table
                .pfadd(Bytes::from_static(b"hll2"), &elements_b)
                .unwrap()
        );
        let c2 = table.pfcount(&[Bytes::from_static(b"hll2")]).unwrap();
        assert_eq!(c2, 4);

        // Multiple keys pfcount (union)
        let c_union = table
            .pfcount(&[Bytes::from_static(b"hll1"), Bytes::from_static(b"hll2")])
            .unwrap();
        assert_eq!(c_union, 6); // foo, bar, zap, a, b, c = 6 distinct elements

        // 6. PFMERGE
        table
            .pfmerge(
                Bytes::from_static(b"hll_merged"),
                &[Bytes::from_static(b"hll1"), Bytes::from_static(b"hll2")],
            )
            .unwrap();
        let c_merged = table.pfcount(&[Bytes::from_static(b"hll_merged")]).unwrap();
        assert_eq!(c_merged, 6);
    }

    #[test]
    fn test_restore_skips_a_zero_crc_but_checks_any_other() {
        let mut table = RudisTable::new();
        table.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"), None);
        let dumped = table.dump(b"k").unwrap();
        let crc_at = dumped.len() - 8;

        let mut no_crc = dumped.clone();
        no_crc[crc_at..].fill(0);
        table
            .restore(Bytes::from_static(b"a"), 0, &no_crc, false, false)
            .unwrap();
        assert_eq!(table.get(b"a").unwrap(), Some(Bytes::from_static(b"v")));

        let mut bad_crc = dumped.clone();
        bad_crc[crc_at] ^= 1;
        assert_eq!(
            table.restore(Bytes::from_static(b"b"), 0, &bad_crc, false, false),
            Err("DUMP payload version or checksum are wrong")
        );

        // Skipping the checksum does not skip decoding.
        let mut garbage = vec![0xff; 4];
        garbage.extend_from_slice(&10u16.to_le_bytes());
        garbage.extend_from_slice(&[0; 8]);
        assert!(
            table
                .restore(Bytes::from_static(b"c"), 0, &garbage, false, false)
                .is_err()
        );
        assert_eq!(table.get(b"b").unwrap(), None);
        assert_eq!(table.get(b"c").unwrap(), None);
    }

    #[test]
    fn test_rudis_table_dump_and_restore() {
        let mut table = RudisTable::new();

        // 1. Non-existent key
        assert_eq!(table.dump(b"non_exist"), None);

        // 2. String dump and restore
        table.set(
            Bytes::from_static(b"str_key"),
            Bytes::from_static(b"hello world"),
            None,
        );
        let dumped = table.dump(b"str_key").expect("dump must succeed");
        assert!(dumped.len() >= 10);

        // Restore to target key
        table
            .restore(
                Bytes::from_static(b"restored_str"),
                0,
                &dumped,
                false,
                false,
            )
            .expect("restore must succeed");
        assert_eq!(
            table.get(b"restored_str").unwrap(),
            Some(Bytes::from_static(b"hello world"))
        );

        // BUSYKEY error
        let err = table.restore(
            Bytes::from_static(b"restored_str"),
            0,
            &dumped,
            false,
            false,
        );
        assert_eq!(err, Err("BUSYKEY Target key name already exists."));

        // Replace success
        table
            .restore(Bytes::from_static(b"restored_str"), 0, &dumped, true, false)
            .expect("replace must succeed");

        // Checksum verification failure
        let mut corrupt = dumped.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xFF;
        assert_eq!(
            table.restore(Bytes::from_static(b"corrupt"), 0, &corrupt, false, false,),
            Err("DUMP payload version or checksum are wrong")
        );

        // 3. Hash dump and restore
        table
            .hset(
                Bytes::from_static(b"myhash"),
                vec![
                    (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                ],
            )
            .unwrap();
        let hash_dump = table.dump(b"myhash").unwrap();
        table
            .restore(
                Bytes::from_static(b"restored_hash"),
                0,
                &hash_dump,
                false,
                false,
            )
            .unwrap();
        assert_eq!(
            table.hget(b"restored_hash", b"f1").unwrap(),
            Some(Bytes::from_static(b"v1"))
        );
        assert_eq!(
            table.hget(b"restored_hash", b"f2").unwrap(),
            Some(Bytes::from_static(b"v2"))
        );

        // 4. ZSet dump and restore
        table
            .zadd(
                Bytes::from_static(b"myz"),
                vec![
                    (10.5, Bytes::from_static(b"m1")),
                    (20.0, Bytes::from_static(b"m2")),
                ],
                ZAddFlags::default(),
            )
            .unwrap();
        let zset_dump = table.dump(b"myz").unwrap();
        table
            .restore(
                Bytes::from_static(b"restored_z"),
                0,
                &zset_dump,
                false,
                false,
            )
            .unwrap();
        assert_eq!(table.zscore(b"restored_z", b"m1").unwrap(), Some(10.5));
        assert_eq!(table.zscore(b"restored_z", b"m2").unwrap(), Some(20.0));
    }

    #[test]
    fn test_streams_table() {
        let mut table = RudisTable::new();

        // 1. Invalid ID checks
        let err0 = table.xadd(
            Bytes::from_static(b"mystream"),
            StreamAddId::Explicit(StreamId::new(0, 0)),
            vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            false,
            None,
            None,
            false,
            StreamTrimStrategy::KeepRef,
            None,
            None,
        );
        assert_eq!(
            err0,
            Err("ERR The ID specified in XADD must be greater than 0-0")
        );

        // 2. NOMKSTREAM when stream doesn't exist
        let res_nomk = table
            .xadd(
                Bytes::from_static(b"nonexistent"),
                StreamAddId::Auto,
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                true,
                None,
                None,
                false,
                StreamTrimStrategy::KeepRef,
                None,
                None,
            )
            .unwrap();
        assert_eq!(res_nomk, StreamAddResult::NoMkStream);

        // 3. XADD with explicit ID
        let id1 = match table
            .xadd(
                Bytes::from_static(b"s1"),
                StreamAddId::Explicit(StreamId::new(1000, 1)),
                vec![
                    (Bytes::from_static(b"sensor"), Bytes::from_static(b"temp")),
                    (Bytes::from_static(b"val"), Bytes::from_static(b"25")),
                ],
                false,
                None,
                None,
                false,
                StreamTrimStrategy::KeepRef,
                None,
                None,
            )
            .unwrap()
        {
            StreamAddResult::Added(id) => id,
            _ => panic!("expected Added"),
        };
        assert_eq!(id1, StreamId::new(1000, 1));
        assert_eq!(table.xlen(b"s1").unwrap(), 1);

        // Monotonicity error
        let err_mono = table.xadd(
            Bytes::from_static(b"s1"),
            StreamAddId::Explicit(StreamId::new(1000, 1)),
            vec![(Bytes::from_static(b"a"), Bytes::from_static(b"b"))],
            false,
            None,
            None,
            false,
            StreamTrimStrategy::KeepRef,
            None,
            None,
        );
        assert_eq!(
            err_mono,
            Err("ERR The ID specified in XADD is equal or smaller than the target stream top item")
        );

        // 4. AutoSeq
        let id2 = match table
            .xadd(
                Bytes::from_static(b"s1"),
                StreamAddId::AutoSeq(1000),
                vec![(Bytes::from_static(b"val"), Bytes::from_static(b"26"))],
                false,
                None,
                None,
                false,
                StreamTrimStrategy::KeepRef,
                None,
                None,
            )
            .unwrap()
        {
            StreamAddResult::Added(id) => id,
            _ => panic!("expected Added"),
        };
        assert_eq!(id2, StreamId::new(1000, 2));

        let id3 = match table
            .xadd(
                Bytes::from_static(b"s1"),
                StreamAddId::AutoSeq(1001),
                vec![(Bytes::from_static(b"val"), Bytes::from_static(b"27"))],
                false,
                None,
                None,
                false,
                StreamTrimStrategy::KeepRef,
                None,
                None,
            )
            .unwrap()
        {
            StreamAddResult::Added(id) => id,
            _ => panic!("expected Added"),
        };
        assert_eq!(id3, StreamId::new(1001, 0));
        assert_eq!(table.xlen(b"s1").unwrap(), 3);

        // 5. XRANGE
        let range = table.xrange(b"s1", "-", "+", None).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, id1);
        assert_eq!(range[1].0, id2);
        assert_eq!(range[2].0, id3);

        // XRANGE with count
        let range_limited = table.xrange(b"s1", "-", "+", Some(2)).unwrap();
        assert_eq!(range_limited.len(), 2);
        assert_eq!(range_limited[0].0, id1);
        assert_eq!(range_limited[1].0, id2);

        // XRANGE exclusive prefix
        let range_ex = table.xrange(b"s1", "(1000-1", "+", None).unwrap();
        assert_eq!(range_ex.len(), 2);
        assert_eq!(range_ex[0].0, id2);

        // 6. XREVRANGE
        let rev = table.xrevrange(b"s1", "+", "-", None).unwrap();
        assert_eq!(rev.len(), 3);
        assert_eq!(rev[0].0, id3);
        assert_eq!(rev[1].0, id2);
        assert_eq!(rev[2].0, id1);

        // 7. XREAD
        let read_res = table
            .xread(
                &[Bytes::from_static(b"s1")],
                &[id1.to_string()],
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(read_res.len(), 1);
        assert_eq!(read_res[0].1.len(), 2);
        assert_eq!(read_res[0].1[0].0, id2);
        assert_eq!(read_res[0].1[1].0, id3);

        // XREAD with $
        let read_dollar = table
            .xread(
                &[Bytes::from_static(b"s1")],
                &[String::from("$")],
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(read_dollar.len(), 0);

        // 8. XDEL
        let del_cnt = table.xdel(b"s1", &[id2]).unwrap();
        assert_eq!(del_cnt, 1);
        assert_eq!(table.xlen(b"s1").unwrap(), 2);

        // 9. XTRIM
        // Add more entries
        for i in 10..20 {
            table
                .xadd(
                    Bytes::from_static(b"s1"),
                    StreamAddId::Explicit(StreamId::new(2000 + i, 0)),
                    vec![(Bytes::from_static(b"i"), Bytes::from(i.to_string()))],
                    false,
                    None,
                    None,
                    false,
                    StreamTrimStrategy::KeepRef,
                    None,
                    None,
                )
                .unwrap();
        }
        assert_eq!(table.xlen(b"s1").unwrap(), 12);
        let trimmed = table
            .xtrim(
                b"s1",
                Some(5),
                None,
                false,
                StreamTrimStrategy::KeepRef,
                None,
            )
            .unwrap();
        assert_eq!(trimmed, 7);
        assert_eq!(table.xlen(b"s1").unwrap(), 5);

        // 10. DUMP and RESTORE stream
        let stream_dump = table.dump(b"s1").unwrap();
        table
            .restore(
                Bytes::from_static(b"restored_stream"),
                0,
                &stream_dump,
                false,
                false,
            )
            .unwrap();
        assert_eq!(table.xlen(b"restored_stream").unwrap(), 5);
        let restored_range = table.xrange(b"restored_stream", "-", "+", None).unwrap();
        assert_eq!(restored_range.len(), 5);
    }

    #[test]
    fn test_three_state_lifecycle_and_instant_decommit() {
        let mut table = RudisTable::new();
        let initial_mem = table.used_memory();

        // 1. Hot key
        table.set(
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"hello_tiered_storage_world"),
            None,
        );
        assert!(table.used_memory() > initial_mem);
        let hot_mem = table.used_memory();
        assert_eq!(
            table.get(b"k1").unwrap(),
            Some(Bytes::from_static(b"hello_tiered_storage_world"))
        );

        // 2. Transition Hot -> Cooled
        let ptr = TieredPointer {
            file_id: 0,
            offset: 4096,
            length: 128,
            value_type: 0,
        };
        assert!(table.set_cooled_pointer(b"k1", ptr));
        assert!(table.is_cooled(b"k1").is_some());
        assert!(table.is_tiered(b"k1").is_none());

        // Fast DRAM hit on Cooled key
        assert_eq!(
            table.get(b"k1").unwrap(),
            Some(Bytes::from_static(b"hello_tiered_storage_world"))
        );
        let (entry_val, _) = table.get_entry(b"k1").unwrap();
        assert_eq!(
            entry_val,
            RudisValue::String(CompactStr::from(b"hello_tiered_storage_world"))
        );

        // 3. Instant Zero-I/O Decommit: Cooled -> Cold (Tiered)
        let (p, freed) = table.decommit_cooled_key(b"k1").unwrap();
        assert_eq!(p.offset, 4096);
        assert!(freed > 0);
        assert!(table.used_memory() < hot_mem);
        assert!(table.is_cooled(b"k1").is_none());
        assert!(table.is_tiered(b"k1").is_some());

        // 4. Restore Cold -> Cooled (Read hit)
        let restored_val = RudisValue::String(CompactStr::from(b"hello_tiered_storage_world"));
        assert!(table.restore_tiered_value(b"k1", restored_val));
        assert!(table.is_cooled(b"k1").is_some());
        assert_eq!(
            table.get(b"k1").unwrap(),
            Some(Bytes::from_static(b"hello_tiered_storage_world"))
        );

        // 5. Decommit all cooled
        let (count, total_freed) = table.decommit_all_cooled();
        assert_eq!(count, 1);
        assert!(total_freed > 0);
        assert!(table.is_tiered(b"k1").is_some());
        assert!(table.is_cooled(b"k1").is_none());
    }

    #[test]
    fn test_compute_digest() {
        let digest = compute_digest(b"v8lf0c11xh8ymlqztfd3eeq16kfn4sspw7fqmnuuq3k3t75em5wdizgcdw7uc26nnf961u2jkfzkjytls2kwlj7626sd");
        assert_eq!(digest, "00006c38adf31777");
    }

    #[test]
    fn test_parse_i64_bytes_and_insert_prepared() {
        // Normal integers
        assert_eq!(RudisTable::parse_i64_bytes(b"0"), Some(0));
        assert_eq!(RudisTable::parse_i64_bytes(b"12345"), Some(12345));
        assert_eq!(RudisTable::parse_i64_bytes(b"-9876"), Some(-9876));
        assert_eq!(RudisTable::parse_i64_bytes(b"+42"), None);
        assert_eq!(RudisTable::parse_i64_bytes(b"0123"), None);
        assert_eq!(RudisTable::parse_i64_bytes(b"-0"), None);
        assert_eq!(
            RudisTable::parse_i64_bytes(b"9223372036854775807"),
            Some(i64::MAX)
        );
        assert_eq!(
            RudisTable::parse_i64_bytes(b"-9223372036854775808"),
            Some(i64::MIN)
        );

        // Long non-integers (>20 digits) early exit
        let long_payload = vec![b'a'; 1024];
        assert_eq!(RudisTable::parse_i64_bytes(&long_payload), None);
        let long_digits = vec![b'9'; 25];
        assert_eq!(RudisTable::parse_i64_bytes(&long_digits), None);

        // Test insert_prepared
        let mut table = RudisTable::new();
        table.set(Bytes::from("prep_k1"), Bytes::from("val1"), None);
        assert_eq!(table.get(b"prep_k1").unwrap(), Some(Bytes::from("val1")));

        // Update with insert_prepared path
        table.set(Bytes::from("prep_k1"), Bytes::from("val2"), None);
        assert_eq!(table.get(b"prep_k1").unwrap(), Some(Bytes::from("val2")));
    }

    #[test]
    fn test_fast_slice_eq() {
        // Empty
        assert!(fast_slice_eq(b"", b""));
        assert!(!fast_slice_eq(b"", b"a"));
        assert!(!fast_slice_eq(b"a", b""));

        // Lengths 1 to 3
        assert!(fast_slice_eq(b"a", b"a"));
        assert!(!fast_slice_eq(b"a", b"b"));
        assert!(fast_slice_eq(b"ab", b"ab"));
        assert!(!fast_slice_eq(b"ab", b"ac"));
        assert!(fast_slice_eq(b"xyz", b"xyz"));
        assert!(!fast_slice_eq(b"xyz", b"xyw"));

        // Lengths 4 to 8
        assert!(fast_slice_eq(b"1234", b"1234"));
        assert!(!fast_slice_eq(b"1234", b"1235"));
        assert!(fast_slice_eq(b"12345", b"12345"));
        assert!(!fast_slice_eq(b"12345", b"12346"));
        assert!(fast_slice_eq(b"12345678", b"12345678"));
        assert!(!fast_slice_eq(b"12345678", b"12345679"));

        // Lengths 9 to 16
        assert!(fast_slice_eq(b"123456789", b"123456789"));
        assert!(!fast_slice_eq(b"123456789", b"123456780"));
        assert!(fast_slice_eq(b"memtier-12345", b"memtier-12345"));
        assert!(!fast_slice_eq(b"memtier-12345", b"memtier-12346"));
        assert!(fast_slice_eq(b"1234567890abcdef", b"1234567890abcdef"));
        assert!(!fast_slice_eq(b"1234567890abcdef", b"1234567890abcdeg"));

        // Lengths 17 to 32
        assert!(fast_slice_eq(b"1234567890abcdefg", b"1234567890abcdefg"));
        assert!(!fast_slice_eq(b"1234567890abcdefg", b"1234567890abcdefh"));
        assert!(fast_slice_eq(
            b"12345678901234567890123456789012",
            b"12345678901234567890123456789012"
        ));
        assert!(!fast_slice_eq(
            b"12345678901234567890123456789012",
            b"12345678901234567890123456789013"
        ));

        // Lengths > 32
        let l1 = vec![b'x'; 64];
        let mut l2 = l1.clone();
        assert!(fast_slice_eq(&l1, &l2));
        l2[63] = b'y';
        assert!(!fast_slice_eq(&l1, &l2));
    }

    #[test]
    fn test_memory_eviction_policies() {
        let mut table = RudisTable::new();
        // Insert keys
        table.set(Bytes::from("key1"), Bytes::from("val1"), None);
        table.set(
            Bytes::from("key2"),
            Bytes::from("val2"),
            Some(Duration::from_secs(60)),
        );
        table.set(
            Bytes::from("key3"),
            Bytes::from("val3"),
            Some(Duration::from_secs(10)),
        );

        assert_eq!(table.len(), 3);

        // Under volatile-ttl, key3 has smaller ttl than key2, so it should be prioritized
        let evicted = table.try_evict_one_key("volatile-ttl");
        assert!(evicted.is_some());
        assert_eq!(table.len(), 2);

        // Under allkeys-lru, any remaining key can be evicted
        let evicted2 = table.try_evict_one_key("allkeys-lru");
        assert!(evicted2.is_some());
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_active_defrag_and_compaction() {
        let mut table = RudisTable::new();
        // 1. Insert 500 keys to expand table capacity
        for i in 0..500 {
            table.set(
                Bytes::from(format!("key_{}", i)),
                Bytes::from(format!("val_{}", i)),
                None,
            );
        }
        assert_eq!(table.len(), 500);
        let expanded_cap = table.table.capacity();
        assert!(expanded_cap >= 512);

        // 2. Delete 490 keys, leaving 10 keys and lots of DELETED tombstones
        for i in 0..490 {
            table.del(&Bytes::from(format!("key_{}", i)));
        }
        assert_eq!(table.len(), 10);
        assert_eq!(table.table.capacity(), expanded_cap);

        // 3. Trigger active_defrag
        let freed_cap = table.active_defrag();
        assert!(freed_cap > 0);
        let compacted_cap = table.table.capacity();
        assert!(compacted_cap < expanded_cap);
        assert_eq!(table.len(), 10);

        // Verify remaining 10 keys are intact
        for i in 490..500 {
            assert_eq!(
                table.get(format!("key_{}", i).as_bytes()).unwrap(),
                Some(Bytes::from(format!("val_{}", i)))
            );
        }
    }

    #[test]
    fn test_del_with_hash_and_exists_zero_expires() {
        let mut table = RudisTable::new();
        let key = Bytes::from("bench_key");
        let h = hash_key(key.as_ref());

        // Key doesn't exist yet
        assert!(!table.exists_with_hash(key.as_ref(), h));
        assert!(!table.del_with_hash(key.as_ref(), h));

        // Insert key without expiry
        table.set(key.clone(), Bytes::from("bench_val"), None);
        assert_eq!(table.num_expires, 0);

        // Exists check via contains() fast path
        assert!(table.exists_with_hash(key.as_ref(), h));

        // Del check via fast path
        assert!(table.del_with_hash(key.as_ref(), h));
        assert!(!table.exists_with_hash(key.as_ref(), h));
        assert!(!table.del_with_hash(key.as_ref(), h));
    }

    #[test]
    fn test_lpop_rpop_one_fast_path() {
        let mut table = RudisTable::new();
        let key = Bytes::from("list_bench");
        let h = hash_key(key.as_ref());

        // Lpop on non-existent key returns None
        assert_eq!(table.lpop_one_with_hash(key.as_ref(), h), Ok(None));
        assert_eq!(table.rpop_one(key.as_ref()), Ok(None));

        // Push 3 items
        table
            .lpush(
                key.clone(),
                vec![
                    Bytes::from("item1"),
                    Bytes::from("item2"),
                    Bytes::from("item3"),
                ],
            )
            .unwrap();
        assert_eq!(table.num_expires, 0);

        // Lpop should pop from front: item3
        assert_eq!(
            table.lpop_one_with_hash(key.as_ref(), h),
            Ok(Some(Bytes::from("item3")))
        );

        // Rpop should pop from back: item1
        assert_eq!(table.rpop_one(key.as_ref()), Ok(Some(Bytes::from("item1"))));

        // Lpop last item: item2, which empties the list and removes the key
        assert_eq!(
            table.lpop_one_with_hash(key.as_ref(), h),
            Ok(Some(Bytes::from("item2")))
        );

        // List is now empty and removed from table
        assert_eq!(table.lpop_one_with_hash(key.as_ref(), h), Ok(None));
        assert_eq!(table.rpop_one(key.as_ref()), Ok(None));
        assert!(!table.exists(key.as_ref()));
    }

    #[test]
    fn test_sismember_compact_fast_path() {
        let mut table = RudisTable::new();
        let key = Bytes::from("set_bench");
        let h = hash_key(key.as_ref());

        // Non-existent set returns INT_0
        assert_eq!(
            table.sismember_compact_with_hash(key.as_ref(), h, b"mem1"),
            Ok(crate::shard::CompactResp::INT_0)
        );

        // Add 3 members
        table
            .sadd(
                key.clone(),
                vec![
                    Bytes::from("mem1"),
                    Bytes::from("mem2"),
                    Bytes::from("mem3"),
                ],
            )
            .unwrap();

        // Check members
        assert_eq!(
            table.sismember_compact_with_hash(key.as_ref(), h, b"mem1"),
            Ok(crate::shard::CompactResp::INT_1)
        );
        assert_eq!(
            table.sismember_compact_with_hash(key.as_ref(), h, b"mem2"),
            Ok(crate::shard::CompactResp::INT_1)
        );
        assert_eq!(
            table.sismember_compact_with_hash(key.as_ref(), h, b"mem3"),
            Ok(crate::shard::CompactResp::INT_1)
        );
        assert_eq!(
            table.sismember_compact_with_hash(key.as_ref(), h, b"mem4"),
            Ok(crate::shard::CompactResp::INT_0)
        );
    }

    #[test]
    fn test_flat_table_remove_present() {
        let mut table = RudisTable::new();
        let key = Bytes::from("del_present_key");
        let h = hash_key(key.as_ref());

        table.set(key.clone(), Bytes::from("val"), None);
        assert!(table.exists_with_hash(key.as_ref(), h));

        // Delete using del_with_hash which calls remove_present internally
        assert!(table.del_with_hash(key.as_ref(), h));
        assert!(!table.exists_with_hash(key.as_ref(), h));
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_hset_hget_optimized_single_field_and_zero_expire() {
        let mut table = RudisTable::new();
        let key = Bytes::from("my_hash_key");
        let h = hash_key(key.as_ref());

        // 1. Initial insert of single field
        let res = table.hset_slice_fast(&key, &[(Bytes::from("f1"), Bytes::from("v1"))]);
        assert_eq!(res, Ok(1));

        // 2. Read back via hget_compact_with_hash and write_hget_resp
        let resp = table.hget_compact_with_hash(key.as_ref(), h, b"f1");
        assert_eq!(
            resp,
            Ok(crate::shard::CompactResp::from_bulk(&Bytes::from_static(
                b"v1"
            )))
        );

        let mut out = Vec::new();
        assert_eq!(table.write_hget_resp(key.as_ref(), b"f1", &mut out), Ok(()));
        assert_eq!(out, b"$2\r\nv1\r\n");

        // 3. Update existing single field (added should be 0)
        let res2 = table.hset_slice_fast(&key, &[(Bytes::from("f1"), Bytes::from("v2"))]);
        assert_eq!(res2, Ok(0));

        let resp2 = table.hget_compact_with_hash(key.as_ref(), h, b"f1");
        assert_eq!(
            resp2,
            Ok(crate::shard::CompactResp::from_bulk(&Bytes::from_static(
                b"v2"
            )))
        );

        out.clear();
        assert_eq!(table.write_hget_resp(key.as_ref(), b"f1", &mut out), Ok(()));
        assert_eq!(out, b"$2\r\nv2\r\n");

        // 4. Missing field returns NULL
        let resp3 = table.hget_compact_with_hash(key.as_ref(), h, b"nonexistent");
        assert_eq!(resp3, Ok(crate::shard::CompactResp::NULL));

        out.clear();
        assert_eq!(
            table.write_hget_resp(key.as_ref(), b"nonexistent", &mut out),
            Ok(())
        );
        assert_eq!(out, b"$-1\r\n");
    }

    #[test]
    fn test_rpop_one_and_write_rpop_resp() {
        let mut table = RudisTable::new();
        let key = Bytes::from("my_list_key");
        let h = hash_key(key.as_ref());

        // Test non-existent key
        assert_eq!(table.rpop_one_with_hash(key.as_ref(), h), Ok(None));
        let mut out = Vec::new();
        assert_eq!(
            table.write_rpop_resp(key.as_ref(), None, &mut out),
            Ok(false)
        );
        assert_eq!(out, b"$-1\r\n");

        out.clear();
        assert_eq!(
            table.write_rpop_resp(key.as_ref(), Some(2), &mut out),
            Ok(false)
        );
        assert_eq!(out, b"*-1\r\n");

        // Push 3 elements: [v1, v2, v3]
        table
            .rpush_slice_fast(
                &key,
                &[Bytes::from("v1"), Bytes::from("v2"), Bytes::from("v3")],
            )
            .unwrap();
        assert_eq!(table.llen(key.as_ref()), Ok(3));

        // Pop 1 element with rpop_one_with_hash -> v3
        assert_eq!(
            table.rpop_one_with_hash(key.as_ref(), h),
            Ok(Some(Bytes::from("v3")))
        );
        assert_eq!(table.llen(key.as_ref()), Ok(2));

        // Pop with write_rpop_resp without count -> v2
        out.clear();
        assert_eq!(
            table.write_rpop_resp(key.as_ref(), None, &mut out),
            Ok(true)
        );
        assert_eq!(out, b"$2\r\nv2\r\n");
        assert_eq!(table.llen(key.as_ref()), Ok(1));

        // Pop with write_rpop_resp with count 5 -> array of [v1]
        out.clear();
        assert_eq!(
            table.write_rpop_resp(key.as_ref(), Some(5), &mut out),
            Ok(true)
        );
        assert_eq!(out, b"*1\r\n$2\r\nv1\r\n");
        assert_eq!(table.llen(key.as_ref()), Ok(0));
        assert!(!table.exists_with_hash(key.as_ref(), h));

        // Wrong type test
        table.set(key.clone(), Bytes::from("string_val"), None);
        assert_eq!(
            table.rpop_one_with_hash(key.as_ref(), h),
            Err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
        out.clear();
        assert_eq!(
            table.write_rpop_resp(key.as_ref(), None, &mut out),
            Err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }

    #[test]
    fn test_mutation_methods_find_entry_mut_fast_path() {
        let mut table = RudisTable::new();

        // 1. HSET: insert new, update existing, add second
        let hkey = Bytes::from("hash_key_1");
        assert_eq!(
            table.hset_slice_fast(&hkey, &[(Bytes::from("f1"), Bytes::from("v1"))]),
            Ok(1)
        );
        assert_eq!(
            table.hset_slice_fast(&hkey, &[(Bytes::from("f1"), Bytes::from("v1_mod"))]),
            Ok(0)
        );
        assert_eq!(
            table.hset_slice_fast(&hkey, &[(Bytes::from("f2"), Bytes::from("v2"))]),
            Ok(1)
        );
        assert_eq!(table.hlen(hkey.as_ref()), Ok(2));

        // 2. LPUSH & RPUSH: insert new, push onto existing
        let lkey = Bytes::from("list_key_1");
        assert_eq!(table.lpush_slice_fast(&lkey, &[Bytes::from("e1")]), Ok(1));
        assert_eq!(table.lpush_slice_fast(&lkey, &[Bytes::from("e0")]), Ok(2));
        assert_eq!(table.rpush_slice_fast(&lkey, &[Bytes::from("e2")]), Ok(3));
        assert_eq!(table.llen(lkey.as_ref()), Ok(3));

        // 3. SADD: insert new, add to existing, dedup
        let skey = Bytes::from("set_key_1");
        assert_eq!(table.sadd_slice_fast(&skey, &[Bytes::from("m1")]), Ok(1));
        assert_eq!(table.sadd_slice_fast(&skey, &[Bytes::from("m1")]), Ok(0));
        assert_eq!(table.sadd_slice_fast(&skey, &[Bytes::from("m2")]), Ok(1));
        assert_eq!(table.scard(skey.as_ref()), Ok(2));

        // 4. ZADD: insert new, update score, add second
        let zkey = Bytes::from("zset_key_1");
        let flags = ZAddFlags::default();
        assert_eq!(
            table.zadd_slice_fast(&zkey, &[(10.0, Bytes::from("m1"))], flags),
            Ok((1, None))
        );
        assert_eq!(
            table.zadd_slice_fast(&zkey, &[(20.0, Bytes::from("m1"))], flags),
            Ok((0, None))
        );
        assert_eq!(
            table.zadd_slice_fast(&zkey, &[(30.0, Bytes::from("m2"))], flags),
            Ok((1, None))
        );
        assert_eq!(table.zcard(zkey.as_ref()), Ok(2));
    }

    #[test]
    fn test_empty_table_bypass_and_tombstone_reset() {
        let mut table = RudisTable::new();

        // 1. In an empty table, lookups bypass SIMD search
        let key = Bytes::from("any_key");
        let h = hash_key(key.as_ref());
        assert!(!table.exists_with_hash(key.as_ref(), h));
        assert!(table.table.find_entry(key.as_ref(), h).is_none());
        assert!(!table.table.contains(key.as_ref(), h));

        // 2. Insert items and verify removal resets tombstones when items == 0
        for i in 0..50 {
            table.set(Bytes::from(format!("k_{}", i)), Bytes::from("v"), None);
        }
        assert_eq!(table.len(), 50);

        // Delete all items
        for i in 0..50 {
            assert!(table.del(&Bytes::from(format!("k_{}", i))));
        }
        assert_eq!(table.len(), 0);
        // Verify all ctrl bytes were reset to EMPTY (no DELETED tombstones remain)
        assert!(!table.table.has_deleted());

        // 3. Test write_lpop_resp_with_hash on empty and populated lists
        let list_k = Bytes::from("test_list");
        let list_h = hash_key(list_k.as_ref());
        let mut out = Vec::new();
        assert_eq!(
            table.write_lpop_resp_with_hash(list_k.as_ref(), list_h, None, &mut out),
            Ok(false)
        );
        assert_eq!(out, b"$-1\r\n");

        table
            .rpush_slice_fast(&list_k, &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        out.clear();
        assert_eq!(
            table.write_lpop_resp_with_hash(list_k.as_ref(), list_h, None, &mut out),
            Ok(true)
        );
        assert_eq!(out, b"$1\r\na\r\n");

        out.clear();
        assert_eq!(
            table.write_lpop_resp_with_hash(list_k.as_ref(), list_h, None, &mut out),
            Ok(true)
        );
        assert_eq!(out, b"$1\r\nb\r\n");
        assert_eq!(table.len(), 0);
        assert!(!table.table.has_deleted());
    }

    #[test]
    fn test_smove_missing_source_wins_over_wrong_type_destination() {
        let mut table = RudisTable::new();
        let hk = Bytes::from("hash");
        let hh = hash_key(hk.as_ref());
        let (f, v) = (Bytes::from("f"), Bytes::from("v"));
        assert_eq!(table.hset_single_field_with_hash(&hk, hh, &f, &v), Ok(1));
        // Redis replies 0 for a missing source, whatever the destination is.
        let r = table.smove(b"nokey", hk.clone(), Bytes::from("m")).unwrap();
        assert!(!r.moved && !r.dst_added);
        // With an existing source set the destination's type is checked.
        let sk = Bytes::from("set");
        let m = Bytes::from("m");
        assert_eq!(
            table.sadd_single_member_with_hash(&sk, hash_key(sk.as_ref()), &m),
            Ok(1)
        );
        assert!(table.smove(sk.as_ref(), hk.clone(), m.clone()).is_err());
        assert!(table.smove(hk.as_ref(), sk.clone(), m.clone()).is_err());
        let r = table.smove(sk.as_ref(), Bytes::from("set2"), m).unwrap();
        assert!(r.moved && r.dst_added);
    }

    #[test]
    fn test_hset_sadd_single_item() {
        let mut table = RudisTable::new();

        // 1. Single-field HSET test: create, update, wrong type
        let hk = Bytes::from("myhash");
        let hh = hash_key(hk.as_ref());
        let f1 = Bytes::from("f1");
        let v1 = Bytes::from("v1");
        let v1_new = Bytes::from("v1_updated");

        assert_eq!(table.hset_single_field_with_hash(&hk, hh, &f1, &v1), Ok(1));
        assert_eq!(table.hget(hk.as_ref(), f1.as_ref()), Ok(Some(v1.clone())));

        // Update existing field returns 0
        assert_eq!(
            table.hset_single_field_with_hash(&hk, hh, &f1, &v1_new),
            Ok(0)
        );
        assert_eq!(table.hget(hk.as_ref(), f1.as_ref()), Ok(Some(v1_new)));

        // Add second field to small hash
        let f2 = Bytes::from("f2");
        let v2 = Bytes::from("v2");
        assert_eq!(table.hset_single_field_with_hash(&hk, hh, &f2, &v2), Ok(1));
        assert_eq!(table.hget(hk.as_ref(), f2.as_ref()), Ok(Some(v2)));

        // 2. Single-member SADD test: create, update existing, add second
        let sk = Bytes::from("myset");
        let sh = hash_key(sk.as_ref());
        let m1 = Bytes::from("m1");
        let m2 = Bytes::from("m2");

        assert_eq!(table.sadd_single_member_with_hash(&sk, sh, &m1), Ok(1));
        assert!(table.sismember(sk.as_ref(), m1.as_ref()).unwrap());

        // Adding already-existing member returns 0
        assert_eq!(table.sadd_single_member_with_hash(&sk, sh, &m1), Ok(0));

        // Adding new member returns 1
        assert_eq!(table.sadd_single_member_with_hash(&sk, sh, &m2), Ok(1));
        assert!(table.sismember(sk.as_ref(), m2.as_ref()).unwrap());

        // Wrong type error test
        assert!(
            table
                .hset_single_field_with_hash(&sk, sh, &f1, &v1)
                .is_err()
        );
        assert!(table.sadd_single_member_with_hash(&hk, hh, &m1).is_err());
    }

    #[test]
    fn test_get_shares_large_values_lazily() {
        let mut table = RudisTable::new();
        let k = Bytes::from("big");
        let h = hash_key(k.as_ref());
        let val = Bytes::from(vec![b'x'; 1024]);
        table.set(k.clone(), val.clone(), None);
        let is_heap = |t: &RudisTable| {
            let (_, e) = t.table.find_entry(k.as_ref(), h).unwrap();
            matches!(&e.val, RudisValue::String(s) if s.is_heap())
        };
        // Written as one exact-size buffer.
        assert!(is_heap(&table));
        // First read converts it in place; reads then share one buffer.
        let a = table.get_with_hash(k.as_ref(), h).unwrap().unwrap();
        assert!(!is_heap(&table));
        let b = table.get_with_hash(k.as_ref(), h).unwrap().unwrap();
        assert_eq!(a, val);
        assert_eq!(a.as_ptr(), b.as_ptr());
        let c = table.get_compact_with_hash(k.as_ref(), h).unwrap().unwrap();
        assert!(matches!(c, crate::shard::CompactResp::Bulk(ref x) if x.as_ptr() == a.as_ptr()));
        // Overwriting drops the shared value; old readers keep theirs.
        table.set(k.clone(), Bytes::from(vec![b'y'; 1024]), None);
        assert!(is_heap(&table));
        assert_eq!(a, val);
        // Small values stay inline and are never promoted.
        table.set(k.clone(), Bytes::from("small"), None);
        assert_eq!(
            table.get_with_hash(k.as_ref(), h),
            Ok(Some(Bytes::from("small")))
        );
    }

    #[test]
    fn test_get_with_hash() {
        let mut table = RudisTable::new();
        let k = Bytes::from("mykey");
        let h = hash_key(k.as_ref());
        let val = Bytes::from("myval");

        // Key not found
        assert_eq!(table.get_with_hash(k.as_ref(), h), Ok(None));

        // Insert and verify get_with_hash
        table.set(k.clone(), val.clone(), None);
        assert_eq!(table.get_with_hash(k.as_ref(), h), Ok(Some(val)));

        // Expired key
        let exp_k = Bytes::from("exp_key");
        let exp_h = hash_key(exp_k.as_ref());
        table.set(
            exp_k.clone(),
            Bytes::from("exp_val"),
            Some(Duration::from_millis(1)),
        );
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(table.get_with_hash(exp_k.as_ref(), exp_h), Ok(None));
    }

    #[test]
    fn test_list_single_item_fast_path() {
        let mut table = RudisTable::new();
        let k = Bytes::from("l_single");
        let h = hash_key(k.as_ref());
        let val1 = Bytes::from("item1");
        let val2 = Bytes::from("item2");

        // 1. LPUSH single item to new list
        assert_eq!(
            table.lpush_slice_fast(&k, std::slice::from_ref(&val1)),
            Ok(1)
        );
        // 2. LPUSH second item
        assert_eq!(
            table.lpush_slice_fast(&k, std::slice::from_ref(&val2)),
            Ok(2)
        );

        // 3. LPOP one item (leaves 1)
        assert_eq!(table.lpop_one_with_hash(k.as_ref(), h), Ok(Some(val2)));
        // 4. LPOP last item (deque.len() == 1 shortcut cleans up key)
        assert_eq!(
            table.lpop_one_with_hash(k.as_ref(), h),
            Ok(Some(val1.clone()))
        );
        // List is now deleted
        assert_eq!(table.lpop_one_with_hash(k.as_ref(), h), Ok(None));
        assert!(!table.exists(k.as_ref()));

        // 5. RPUSH single item and write_rpop_resp_with_hash shortcut
        assert_eq!(
            table.rpush_slice_fast(&k, std::slice::from_ref(&val1)),
            Ok(1)
        );
        let mut out = Vec::new();
        assert_eq!(
            table.write_rpop_resp_with_hash(k.as_ref(), h, None, &mut out),
            Ok(true)
        );
        assert_eq!(out, b"$5\r\nitem1\r\n");
        assert!(!table.exists(k.as_ref()));
    }

    #[test]
    fn test_sismember_contains_with_hash() {
        let mut table = RudisTable::new();
        let k = Bytes::from("set_hash_test");
        let h = hash_key(k.as_ref());
        let m1 = Bytes::from("member1");
        let m2 = Bytes::from("member2");
        let m3 = Bytes::from("nonexistent");

        assert_eq!(table.sadd_slice(&k, &[m1.clone(), m2.clone()]), Ok(2));

        // Test with sismember and sismember_compact_with_hash
        assert_eq!(table.sismember(k.as_ref(), m1.as_ref()), Ok(true));
        assert_eq!(table.sismember(k.as_ref(), m2.as_ref()), Ok(true));
        assert_eq!(table.sismember(k.as_ref(), m3.as_ref()), Ok(false));

        assert_eq!(
            table.sismember_compact_with_hash(k.as_ref(), h, m1.as_ref()),
            Ok(crate::shard::CompactResp::INT_1)
        );
        assert_eq!(
            table.sismember_compact_with_hash(k.as_ref(), h, m2.as_ref()),
            Ok(crate::shard::CompactResp::INT_1)
        );
        assert_eq!(
            table.sismember_compact_with_hash(k.as_ref(), h, m3.as_ref()),
            Ok(crate::shard::CompactResp::INT_0)
        );
    }

    #[test]
    fn test_extendible_hashing_segment_splits() {
        let mut table = RudisFlatTable::new(1024);
        assert_eq!(table.segments.len(), 1);
        assert_eq!(table.global_depth, 0);

        // Insert 3,000 keys to trigger multiple segment splits across directory depths
        let total_inserted = 3000;
        for i in 0..total_inserted {
            let key = Bytes::from(format!("key_{:04}", i));
            let val = RudisValue::String(CompactStr::from("val"));
            table.insert(RudisEntry::new(
                CompactKey::new(&key),
                val,
                Expiry::from(None),
            ));
        }

        assert!(table.segments.len() >= 4);
        assert!(table.global_depth >= 2);
        assert_eq!(table.len(), total_inserted);

        // Verify all keys can be looked up directly via single-segment probe
        for k in 0..total_inserted {
            let key = format!("key_{:04}", k);
            let h = hash_key(key.as_bytes());
            assert!(table.contains(key.as_bytes(), h));
            assert!(table.find_entry(key.as_bytes(), h).is_some());
        }

        // Delete half the keys and re-insert to verify tombstone reuse and segment stability
        for k in (0..total_inserted).step_by(2) {
            let key = format!("key_{:04}", k);
            let h = hash_key(key.as_bytes());
            assert!(table.remove_key(key.as_bytes(), h).is_some());
        }
        assert_eq!(table.len(), total_inserted / 2);

        for k in (0..total_inserted).step_by(2) {
            let key = Bytes::from(format!("key_{:04}", k));
            let val = RudisValue::String(CompactStr::from("val2"));
            table.insert(RudisEntry::new(
                CompactKey::new(&key),
                val,
                Expiry::from(None),
            ));
        }
        assert_eq!(table.len(), total_inserted);
    }

    #[test]
    fn test_get_compact_frames_integer_values_as_bulk_strings() {
        let mut table = RudisTable::new();
        for v in ["7", "12345", "-3", "9223372036854775807"] {
            table.set(Bytes::from_static(b"k"), Bytes::from(v), None);
            assert!(matches!(
                table.get_entry(b"k").unwrap().0,
                RudisValue::Int(_)
            ));
            let got = table.get_compact(b"k").unwrap().unwrap();
            assert_eq!(
                got.as_slice(),
                format!("${}\r\n{}\r\n", v.len(), v).as_bytes()
            );
        }
    }

    const BAD_PAYLOAD: &str = "DUMP payload version or checksum are wrong";

    /// Element counts are attacker-controlled: they must not size an
    /// allocation beyond what the payload's bytes can hold.
    #[test]
    fn test_payload_counts_cannot_force_huge_allocations() {
        let max = u32::MAX.to_le_bytes();
        let mut stream = vec![6];
        stream.extend_from_slice(&1u32.to_le_bytes());
        stream.extend_from_slice(&[0; 16 + 16]);
        stream.extend_from_slice(&max);
        for payload in [
            [&[1u8][..], &max].concat(),
            [&[2u8][..], &max].concat(),
            [&[4u8][..], &max].concat(),
            stream,
        ] {
            assert_eq!(
                RudisTable::deserialize_val_payload(&payload).err(),
                Some(BAD_PAYLOAD),
                "{payload:?}"
            );
            let mut sealed = payload.clone();
            RudisTable::seal_dump_payload(&mut sealed);
            let mut t = RudisTable::new();
            assert!(
                t.restore(Bytes::from_static(b"k"), 0, &sealed, false, false)
                    .is_err()
            );
            assert!(!t.exists(b"k"));
        }
    }

    #[test]
    fn test_payload_rejects_nan_scores_and_duplicate_fields() {
        let zset = |score: f64| {
            let mut p = vec![3];
            p.extend_from_slice(&1u32.to_le_bytes());
            p.extend_from_slice(&1u32.to_le_bytes());
            p.push(b'm');
            p.extend_from_slice(&score.to_bits().to_le_bytes());
            p
        };
        assert!(RudisTable::deserialize_val_payload(&zset(1.5)).is_ok());
        assert_eq!(
            RudisTable::deserialize_val_payload(&zset(f64::NAN)).err(),
            Some(BAD_PAYLOAD)
        );

        let hash = |fields: &[&[u8]]| {
            let mut p = vec![4];
            p.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for f in fields {
                for part in [*f, b"v"] {
                    p.extend_from_slice(&(part.len() as u32).to_le_bytes());
                    p.extend_from_slice(part);
                }
            }
            p
        };
        assert!(RudisTable::deserialize_val_payload(&hash(&[b"a", b"b"])).is_ok());
        assert_eq!(
            RudisTable::deserialize_val_payload(&hash(&[b"a", b"a"])).err(),
            Some(BAD_PAYLOAD)
        );
    }

    /// Cheap deterministic fuzz of the DUMP payload decoder with hostile
    /// lengths: it must never panic, and whatever it accepts must
    /// re-serialize.
    #[test]
    fn test_deserialize_val_payload_fuzz() {
        const HOSTILE: [u32; 8] = [0, 1, 2, 64, 65, 0x7FFF_FFFF, 0xFFFF_FFFE, u32::MAX];
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let iters = std::env::var("RUDIS_FUZZ_ITERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20_000usize);
        for _ in 0..iters {
            let mut p = vec![(next() % 8) as u8];
            for _ in 0..next() % 12 {
                match next() % 4 {
                    0 => p.extend_from_slice(&HOSTILE[(next() % 8) as usize].to_le_bytes()),
                    1 => p.extend_from_slice(&next().to_le_bytes()),
                    2 => p.extend_from_slice(&f64::NAN.to_bits().to_le_bytes()),
                    _ => p.push(next() as u8),
                }
            }
            if let Ok((v, used)) = RudisTable::deserialize_val_payload(&p) {
                assert!(used <= p.len());
                let mut out = Vec::new();
                RudisTable::serialize_val_payload(&v, &mut out);
            }
        }
    }

    /// Restored pending entries can carry any delivery count and time;
    /// claiming them must saturate rather than overflow.
    #[test]
    fn test_claiming_restored_extreme_pending_entries() {
        let id = StreamId::new(1, 0);
        let mut s = RudisStream::new();
        s.last_id = id;
        s.entries.insert(
            id,
            vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
        );
        s.rebuild_nodes();
        let mut g = StreamGroup {
            name: Bytes::from_static(b"g"),
            last_delivered_id: id,
            entries_read: Some(1),
            consumers: HashMap::new(),
            pel: std::collections::BTreeMap::new(),
            next_nack_seq: 0,
        };
        g.pel.insert(
            id,
            StreamPelEntry {
                consumer: Bytes::from_static(b"c"),
                delivery_time_ms: u64::MAX,
                delivery_count: usize::MAX,
                nack_seq: 0,
            },
        );
        s.groups.insert(Bytes::from_static(b"g"), g);
        let mut payload = Vec::new();
        RudisTable::serialize_val_payload(&RudisValue::Stream(Box::new(s)), &mut payload);
        RudisTable::seal_dump_payload(&mut payload);
        let mut t = RudisTable::new();
        t.restore(Bytes::from_static(b"s"), 0, &payload, false, false)
            .unwrap();

        let (mut entries, mut bytes) = (0, 0);
        let (claimed, _) = t
            .xreadgroup(
                b"s",
                b"g",
                Bytes::from_static(b"c"),
                ">",
                None,
                false,
                Some(u64::MAX),
                usize::MAX,
                &mut entries,
                &mut bytes,
            )
            .unwrap();
        assert!(claimed.is_empty());
        assert!(
            t.earliest_claim_wait_ms(b"s", b"g", u64::MAX)
                .is_some_and(|w| w > 0)
        );
        let (claimed, _) = t
            .xclaim(
                b"s",
                b"g",
                Bytes::from_static(b"c2"),
                0,
                &[Bytes::from_static(b"1-0")],
                None,
                None,
                None,
                false,
                false,
            )
            .unwrap();
        assert_eq!(claimed.len(), 1);
    }

    /// Probabilistic records in a full-sync RDB go through the same
    /// validating decoders as their RESTORE commands.
    #[test]
    fn test_load_rdb_validates_probabilistic_records() {
        let record = |ty: u8, payload: &[u8]| {
            let mut body = vec![0xFE, 0x00];
            body.extend_from_slice(&1u32.to_le_bytes());
            body.extend_from_slice(&[b'k', ty]);
            body.extend_from_slice(payload);
            body.push(0xFF);
            rdb_with_body(&body)
        };
        let mut good = Vec::new();
        crate::probabilistic::BloomFilter::new(100, 0.01).encode(&mut good);
        let mut db = crate::shard::ShardDb::new(0);
        assert_eq!(load_rdb_bytes(&record(8, &good), &mut db, 0, 1).unwrap(), 1);
        assert!(db.probabilistic_store.bloom_filters.contains_key(&b"k"[..]));

        // A Bloom filter with 2^31 hash functions (each lookup would loop
        // that many times), a zero-width Count-Min sketch and a Top-K with
        // more items than k are all refused.
        let mut bloom = good.clone();
        bloom[24..28].copy_from_slice(&(1u32 << 31).to_le_bytes());
        let mut cms = 0u64.to_le_bytes().to_vec();
        cms.extend_from_slice(&1u32.to_le_bytes());
        cms.extend_from_slice(&0u64.to_le_bytes());
        let mut topk = 1u64.to_le_bytes().to_vec();
        topk.extend_from_slice(&u32::MAX.to_le_bytes());
        for (ty, payload) in [(8, bloom), (11, cms), (12, topk)] {
            let mut db = crate::shard::ShardDb::new(0);
            let _ = load_rdb_bytes(&record(ty, &payload), &mut db, 0, 1);
            let ps = &db.probabilistic_store;
            assert!(
                ps.bloom_filters.is_empty()
                    && ps.cms_sketches.is_empty()
                    && ps.topk_trackers.is_empty(),
                "type {ty} loaded"
            );
        }
    }

    /// A full sync replaces the dataset; a load that fails part-way leaves
    /// it empty instead of mixing old, new and missing keys.
    #[test]
    fn test_full_sync_load_replaces_dataset_or_leaves_it_empty() {
        let mut master = crate::shard::ShardDb::new(0);
        master
            .table
            .set(Bytes::from_static(b"fresh"), Bytes::from_static(b"1"), None);
        let mut chunk = Vec::new();
        master.save_rdb_chunk(&mut chunk);
        let body = |chunk: &[u8]| {
            let mut b = vec![0xFE, 0x00];
            b.extend_from_slice(chunk);
            b.push(0xFF);
            rdb_with_body(&b)
        };

        let mut replica = crate::shard::ShardDb::new(0);
        replica
            .table
            .set(Bytes::from_static(b"stale"), Bytes::from_static(b"1"), None);
        replica.probabilistic_store.bloom_filters.insert(
            Bytes::from_static(b"bf"),
            crate::probabilistic::BloomFilter::new(10, 0.01),
        );
        replica
            .load_full_sync_rdb(&body(&chunk), 0, 1, None)
            .unwrap();
        assert!(replica.table.exists(b"fresh"));
        assert!(!replica.table.exists(b"stale"));
        assert!(replica.probabilistic_store.bloom_filters.is_empty());

        // Valid checksum, but the record is cut short.
        let cut = body(&chunk[..chunk.len() - 1]);
        assert!(replica.load_full_sync_rdb(&cut, 0, 1, None).is_err());
        assert_eq!(replica.dbsize(), 0);
    }

    #[test]
    fn test_flushdb_clears_every_key_family() {
        let mut db = crate::shard::ShardDb::new(0);
        db.table
            .set(Bytes::from_static(b"s"), Bytes::from_static(b"1"), None);
        db.json_store
            .insert_raw(Bytes::from_static(b"j"), serde_json::json!({"a": 1}));
        db.probabilistic_store.bloom_filters.insert(
            Bytes::from_static(b"bf"),
            crate::probabilistic::BloomFilter::new(10, 0.01),
        );
        let ts = db
            .crdt_store
            .set(Bytes::from_static(b"r"), Bytes::from_static(b"v"));
        db.crdt_store
            .set_add(Bytes::from_static(b"cs"), Bytes::from_static(b"m"));
        db.crdt_store
            .counter_incr(Bytes::from_static(b"c"), 1)
            .unwrap();

        db.flushdb();
        assert_eq!(db.dbsize(), 0);
        assert!(db.json_store.is_empty());
        assert!(db.probabilistic_store.bloom_filters.is_empty());
        assert!(db.crdt_store.registers.is_empty());
        assert!(db.crdt_store.sets.is_empty());
        assert!(db.crdt_store.counters.is_empty());
        // The clock keeps going, so later writes still win over old ones.
        assert!(db.crdt_store.clock.now() > ts);
    }
}
