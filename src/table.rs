use bytes::Bytes;
use fxhash::{FxBuildHasher, hash64};
use hashbrown::HashMap;

pub type RudisHashMap = HashMap<Bytes, Bytes, FxBuildHasher>;
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
        Ok(std::ops::Bound::Excluded(id))
    } else {
        Ok(std::ops::Bound::Included(id))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPelEntry {
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamConsumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub pel: std::collections::BTreeMap<StreamId, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub consumers: HashMap<Bytes, StreamConsumer>,
    pub pel: std::collections::BTreeMap<StreamId, StreamPelEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RudisStream {
    pub entries: std::collections::BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub groups: HashMap<Bytes, StreamGroup>,
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
            groups: HashMap::new(),
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
    String(Bytes),
    Int(i64),
    SmallHash(Vec<(Bytes, Bytes)>),
    Hash(RudisHashMap),
    List(std::collections::VecDeque<Bytes>),
    Set(RudisSet),
    ZSet(RudisZSet),
    HyperLogLog(Box<[u8; 16384]>),
    Stream(RudisStream),
    Tiered(TieredPointer),
    Cooled {
        ptr: TieredPointer,
        val: Box<RudisValue>,
    },
}

impl RudisValue {
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
            RudisValue::Cooled { val, .. } => 24 + val.approx_bytes(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RudisEntry {
    pub key: Bytes,
    pub val: RudisValue,
    pub expire_at: Option<Instant>,
}

#[inline(always)]
pub fn hash_key(key: &[u8]) -> u64 {
    hash64(key)
}

#[inline(always)]
pub fn fingerprint(hash: u64) -> u8 {
    (hash >> 57) as u8 & 0x7F
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn probe_group_match_or_empty(ptr: *const u8, tag: u8) -> (u16, u16) {
    use std::arch::x86_64::*;
    unsafe {
        let group = _mm_loadu_si128(ptr as *const __m128i);
        let tag_target = _mm_set1_epi8(tag as i8);
        let empty_target = _mm_set1_epi8(EMPTY as i8);
        let match_cmp = _mm_cmpeq_epi8(group, tag_target);
        let empty_cmp = _mm_cmpeq_epi8(group, empty_target);
        (
            _mm_movemask_epi8(match_cmp) as u16,
            _mm_movemask_epi8(empty_cmp) as u16,
        )
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn probe_group_match_del_empty(ptr: *const u8, tag: u8) -> (u16, u16, u16) {
    use std::arch::x86_64::*;
    unsafe {
        let group = _mm_loadu_si128(ptr as *const __m128i);
        let tag_target = _mm_set1_epi8(tag as i8);
        let del_target = _mm_set1_epi8(DELETED as i8);
        let empty_target = _mm_set1_epi8(EMPTY as i8);
        let match_cmp = _mm_cmpeq_epi8(group, tag_target);
        let del_cmp = _mm_cmpeq_epi8(group, del_target);
        let empty_cmp = _mm_cmpeq_epi8(group, empty_target);
        (
            _mm_movemask_epi8(match_cmp) as u16,
            _mm_movemask_epi8(del_cmp) as u16,
            _mm_movemask_epi8(empty_cmp) as u16,
        )
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
unsafe fn probe_group_match_or_empty(ptr: *const u8, tag: u8) -> (u16, u16) {
    let mut match_mask = 0u16;
    let mut empty_mask = 0u16;
    for i in 0..16 {
        let b = *ptr.add(i);
        if b == tag {
            match_mask |= 1 << i;
        }
        if b == EMPTY {
            empty_mask |= 1 << i;
        }
    }
    (match_mask, empty_mask)
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
unsafe fn probe_group_match_del_empty(ptr: *const u8, tag: u8) -> (u16, u16, u16) {
    let mut match_mask = 0u16;
    let mut del_mask = 0u16;
    let mut empty_mask = 0u16;
    for i in 0..16 {
        let b = *ptr.add(i);
        if b == tag {
            match_mask |= 1 << i;
        }
        if b == DELETED {
            del_mask |= 1 << i;
        }
        if b == EMPTY {
            empty_mask |= 1 << i;
        }
    }
    (match_mask, del_mask, empty_mask)
}

/// A high-performance flat hash table utilizing 16-slot SIMD group probing
/// with inlined values and expiration metadata.
pub struct RudisFlatTable {
    ctrl: Vec<u8>,
    pub slots: Vec<Option<RudisEntry>>,
    pub capacity: usize,
    mask: usize,
    items: usize,
    growth_left: usize,
    pub slot_counts: Box<[u32; 16384]>,
}

impl RudisFlatTable {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two().max(GROUP_SIZE);
        let mut ctrl = vec![EMPTY; cap + GROUP_SIZE];
        ctrl.copy_within(0..GROUP_SIZE, cap);

        let mut slots = Vec::with_capacity(cap);
        for _ in 0..cap {
            slots.push(None);
        }

        Self {
            ctrl,
            slots,
            capacity: cap,
            mask: cap - 1,
            items: 0,
            growth_left: cap * 7 / 8,
            slot_counts: vec![0u32; 16384].into_boxed_slice().try_into().unwrap(),
        }
    }

    #[inline(always)]
    fn set_ctrl(&mut self, idx: usize, byte: u8) {
        self.ctrl[idx] = byte;
        if idx < GROUP_SIZE {
            self.ctrl[self.capacity + idx] = byte;
        }
    }

    /// Finds the index and entry reference of a matching key, if present.
    #[inline(always)]
    pub fn find_entry(&self, key: &[u8], hash: u64) -> Option<(usize, &RudisEntry)> {
        if self.items == 0 {
            return None;
        }
        let tag = fingerprint(hash);
        let mut idx = (hash as usize) & self.mask;
        let mut step = 0;

        loop {
            let (match_mask, empty_mask) =
                unsafe { probe_group_match_or_empty(self.ctrl.as_ptr().add(idx), tag) };
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                // SAFETY: A SIMD tag match strictly matches 0..=127, never EMPTY (0xFF) or DELETED (0xFE).
                let entry = unsafe {
                    self.slots
                        .get_unchecked(slot_idx)
                        .as_ref()
                        .unwrap_unchecked()
                };
                if entry.key.len() == key.len() && entry.key.as_ref() == key {
                    return Some((slot_idx, entry));
                }
                bits &= bits - 1;
            }

            if empty_mask != 0 {
                return None;
            }

            step += GROUP_SIZE;
            idx = (idx + step) & self.mask;
        }
    }

    /// Checks whether a key exists in the flat table without returning the entry reference.
    #[inline(always)]
    pub fn contains(&self, key: &[u8], hash: u64) -> bool {
        if self.items == 0 {
            return false;
        }
        let tag = fingerprint(hash);
        let mut idx = (hash as usize) & self.mask;
        let mut step = 0;

        loop {
            let (match_mask, empty_mask) =
                unsafe { probe_group_match_or_empty(self.ctrl.as_ptr().add(idx), tag) };
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                // SAFETY: A SIMD tag match strictly matches 0..=127, never EMPTY or DELETED.
                let entry = unsafe {
                    self.slots
                        .get_unchecked(slot_idx)
                        .as_ref()
                        .unwrap_unchecked()
                };
                if entry.key.len() == key.len() && entry.key.as_ref() == key {
                    return true;
                }
                bits &= bits - 1;
            }

            if empty_mask != 0 {
                return false;
            }

            step += GROUP_SIZE;
            idx = (idx + step) & self.mask;
        }
    }

    /// Finds the index and mutable entry reference of a matching key, if present.
    #[inline(always)]
    pub fn find_entry_mut(&mut self, key: &[u8], hash: u64) -> Option<(usize, &mut RudisEntry)> {
        if self.items == 0 {
            return None;
        }
        let tag = fingerprint(hash);
        let mut idx = (hash as usize) & self.mask;
        let mut step = 0;

        loop {
            let (match_mask, empty_mask) =
                unsafe { probe_group_match_or_empty(self.ctrl.as_ptr().add(idx), tag) };
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                // SAFETY: A SIMD tag match strictly matches 0..=127, never EMPTY or DELETED.
                let entry = unsafe {
                    self.slots
                        .get_unchecked(slot_idx)
                        .as_ref()
                        .unwrap_unchecked()
                };
                if entry.key.len() == key.len() && entry.key.as_ref() == key {
                    // SAFETY: slot_idx is valid and within bounds
                    let entry_mut = unsafe {
                        self.slots
                            .get_unchecked_mut(slot_idx)
                            .as_mut()
                            .unwrap_unchecked()
                    };
                    return Some((slot_idx, entry_mut));
                }
                bits &= bits - 1;
            }

            if empty_mask != 0 {
                return None;
            }

            step += GROUP_SIZE;
            idx = (idx + step) & self.mask;
        }
    }

    /// Finds the index of a matching key, if present.
    #[inline(always)]
    pub fn find(&self, key: &[u8], hash: u64) -> Option<usize> {
        self.find_entry(key, hash).map(|(idx, _)| idx)
    }

    /// Searches for key and returns either `(Some(existing_slot_idx), candidate_insert_idx)`
    /// or `(None, candidate_insert_idx)`.
    fn find_or_prepare_insert(&self, key: &[u8], hash: u64) -> (Option<usize>, usize) {
        if self.items == 0 {
            return (None, (hash as usize) & self.mask);
        }
        let tag = fingerprint(hash);
        let mut idx = (hash as usize) & self.mask;
        let mut step = 0;
        let mut first_free: Option<usize> = None;

        loop {
            let (match_mask, del_mask, empty_mask) =
                unsafe { probe_group_match_del_empty(self.ctrl.as_ptr().add(idx), tag) };
            let mut bits = match_mask;
            while bits != 0 {
                let offset = bits.trailing_zeros() as usize;
                let slot_idx = (idx + offset) & self.mask;
                // SAFETY: A SIMD tag match strictly matches 0..=127, never EMPTY or DELETED.
                let entry = unsafe {
                    self.slots
                        .get_unchecked(slot_idx)
                        .as_ref()
                        .unwrap_unchecked()
                };
                if entry.key.len() == key.len() && entry.key.as_ref() == key {
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
            idx = (idx + step) & self.mask;
        }
    }

    fn resize(&mut self, new_cap: usize) {
        let mut new_table = RudisFlatTable::new(new_cap);
        for entry in self.slots.drain(..).flatten() {
            let h = hash_key(&entry.key);
            let (_, insert_idx) = new_table.find_or_prepare_insert(&entry.key, h);
            let tag = fingerprint(h);
            new_table.set_ctrl(insert_idx, tag);
            new_table.slots[insert_idx] = Some(entry);
            new_table.items += 1;
            new_table.growth_left = new_table.growth_left.saturating_sub(1);
        }
        new_table.slot_counts = self.slot_counts.clone();
        *self = new_table;
    }

    pub fn insert(&mut self, entry: RudisEntry) -> Option<RudisEntry> {
        if self.growth_left == 0 {
            let new_cap = if self.items * 2 < self.capacity && self.capacity > GROUP_SIZE {
                self.capacity
            } else {
                self.capacity * 2
            };
            self.resize(new_cap);
        }

        let h = hash_key(&entry.key);
        let (existing, insert_idx) = self.find_or_prepare_insert(&entry.key, h);

        if let Some(idx) = existing {
            self.slots[idx].replace(entry)
        } else {
            let tag = fingerprint(h);
            self.set_ctrl(insert_idx, tag);
            if crate::cluster::HAS_ACTIVE_CLUSTER.load(std::sync::atomic::Ordering::Relaxed) {
                let slot = crate::router::key_slot(&entry.key) as usize;
                self.slot_counts[slot] += 1;
            }
            self.slots[insert_idx] = Some(entry);
            self.items += 1;
            self.growth_left = self.growth_left.saturating_sub(1);
            None
        }
    }

    #[inline(always)]
    pub fn insert_prepared(&mut self, entry: RudisEntry, hash: u64, insert_idx: usize) {
        if self.growth_left == 0 {
            self.insert(entry);
            return;
        }
        let tag = fingerprint(hash);
        self.set_ctrl(insert_idx, tag);
        if crate::cluster::HAS_ACTIVE_CLUSTER.load(std::sync::atomic::Ordering::Relaxed) {
            let slot = crate::router::key_slot(&entry.key) as usize;
            self.slot_counts[slot] += 1;
        }
        self.slots[insert_idx] = Some(entry);
        self.items += 1;
        self.growth_left = self.growth_left.saturating_sub(1);
    }

    #[inline(always)]
    pub fn remove(&mut self, slot_idx: usize) -> Option<RudisEntry> {
        self.set_ctrl(slot_idx, DELETED);
        self.items -= 1;
        if self.items == 0 {
            self.ctrl.fill(EMPTY);
            self.growth_left = self.capacity * 7 / 8;
        }
        let entry = self.slots[slot_idx].take();
        if let Some(ref e) = entry
            && crate::cluster::HAS_ACTIVE_CLUSTER.load(std::sync::atomic::Ordering::Relaxed)
        {
            let slot = crate::router::key_slot(&e.key) as usize;
            self.slot_counts[slot] = self.slot_counts[slot].saturating_sub(1);
        }
        entry
    }

    /// Optimized removal when the caller already verified `self.slots[slot_idx]` is `Some`.
    /// Avoids outer Option checks and inlines cleanly into `del_with_hash`.
    #[inline(always)]
    pub fn remove_present(&mut self, slot_idx: usize) -> RudisEntry {
        self.set_ctrl(slot_idx, DELETED);
        self.items -= 1;
        if self.items == 0 {
            self.ctrl.fill(EMPTY);
            self.growth_left = self.capacity * 7 / 8;
        }
        // SAFETY: Caller verified presence via find_entry
        let entry = unsafe {
            self.slots
                .get_unchecked_mut(slot_idx)
                .take()
                .unwrap_unchecked()
        };
        if crate::cluster::HAS_ACTIVE_CLUSTER.load(std::sync::atomic::Ordering::Relaxed) {
            let slot = crate::router::key_slot(&entry.key) as usize;
            self.slot_counts[slot] = self.slot_counts[slot].saturating_sub(1);
        }
        entry
    }

    #[inline(always)]
    pub fn get_slot(&self, idx: usize) -> Option<&RudisEntry> {
        self.slots[idx].as_ref()
    }

    #[inline(always)]
    pub fn get_slot_mut(&mut self, idx: usize) -> Option<&mut RudisEntry> {
        self.slots[idx].as_mut()
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
    pub fn clear(&mut self) {
        let cap = self.capacity;
        self.ctrl.fill(EMPTY);
        self.ctrl.copy_within(0..GROUP_SIZE, cap);
        self.slots.fill(None);
        self.items = 0;
        self.growth_left = (cap * 7) / 8;
        self.slot_counts.fill(0);
    }

    pub fn defrag(&mut self) -> usize {
        let optimal_cap = (self.items * 2).next_power_of_two().max(GROUP_SIZE).max(64);
        let before_cap = self.capacity;
        let has_deleted = self.ctrl.contains(&DELETED);
        if optimal_cap < self.capacity || has_deleted {
            self.resize(optimal_cap);
            before_cap.saturating_sub(self.capacity)
        } else {
            0
        }
    }
}

static EXPIRED_KEYS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static EVICTED_KEYS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[inline]
pub fn inc_expired_keys() {
    EXPIRED_KEYS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn get_expired_keys() -> u64 {
    EXPIRED_KEYS.load(std::sync::atomic::Ordering::Relaxed)
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
    pub used_memory: usize,
    pub arena: crate::allocator::SmallCollectionArena,
    pub num_expires: usize,
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

impl RudisTable {
    pub fn new() -> Self {
        let base_mem = 64 * std::mem::size_of::<Option<RudisEntry>>() + 64 + GROUP_SIZE + 16384 * 4;
        Self {
            table: RudisFlatTable::new(64),
            sample_cursor: 0,
            spill_cursor: 0,
            used_memory: base_mem,
            arena: crate::allocator::SmallCollectionArena::new(),
            num_expires: 0,
        }
    }

    #[inline(always)]
    pub fn recycle_value(&mut self, val: RudisValue) {
        match val {
            RudisValue::List(deque) => self.arena.recycle_list(deque),
            RudisValue::SmallHash(pairs) => self.arena.recycle_small_hash(pairs),
            RudisValue::Set(RudisSet::Small(v)) => self.arena.recycle_small_set(v),
            RudisValue::ZSet(RudisZSet::Small(v)) => self.arena.recycle_small_zset(v),
            _ => {}
        }
    }

    #[inline]
    pub fn insert_entry(&mut self, entry: RudisEntry) {
        self.del(&entry.key);
        if entry.expire_at.is_some() {
            self.num_expires += 1;
        }
        self.table.insert(entry);
    }

    #[inline]
    pub fn entries(&self) -> impl Iterator<Item = &RudisEntry> {
        self.table.slots.iter().flatten()
    }

    pub fn recalculate_used_memory(&mut self) -> usize {
        let mut total = self.table.capacity * std::mem::size_of::<Option<RudisEntry>>()
            + self.table.ctrl.len()
            + 16384 * 4;
        for entry in self.table.slots.iter().flatten() {
            total += entry.key.len() + entry.val.approx_bytes() + 64;
        }
        self.used_memory = total;
        total
    }

    pub fn active_defrag(&mut self) -> usize {
        let freed = self.table.defrag();
        for entry in self.table.slots.iter_mut().flatten() {
            match &mut entry.val {
                RudisValue::String(b) => {
                    if !b.is_empty() {
                        *b = Bytes::copy_from_slice(b.as_ref());
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

    #[inline]
    fn expire_slot(&mut self, slot_idx: usize) {
        if let Some(removed) = self.table.remove(slot_idx) {
            if removed.expire_at.is_some() {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
            let freed = removed.key.len() + removed.val.approx_bytes() + 64;
            self.used_memory = self.used_memory.saturating_sub(freed);
            inc_expired_keys();
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
            if let Some(expire_at) = entry.expire_at {
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
        let cap = self.table.capacity();
        if cap == 0 || self.table.is_empty() {
            return None;
        }

        let policy_lower = policy.to_lowercase();
        let is_volatile = policy_lower.starts_with("volatile");

        // Sample up to 10 occupied slots starting at sample_cursor
        let mut best_slot: Option<usize> = None;
        let mut min_ttl: Option<Instant> = None;
        let mut checked = 0;
        let mut attempts = 0;

        while checked < 10 && attempts < cap {
            let idx = self.sample_cursor % cap;
            self.sample_cursor = (self.sample_cursor + 1) % cap;
            attempts += 1;

            if let Some(entry) = self.table.get_slot(idx) {
                // If volatile policy, key must have an expiration
                if is_volatile && entry.expire_at.is_none() {
                    continue;
                }

                if policy_lower.contains("ttl") {
                    if let Some(exp) = entry.expire_at
                        && (min_ttl.is_none() || Some(exp) < min_ttl)
                    {
                        min_ttl = Some(exp);
                        best_slot = Some(idx);
                    }
                } else {
                    // LRU / random sampling
                    best_slot = Some(idx);
                    checked += 1;
                }
            }
        }

        if let Some(slot_idx) = best_slot
            && let Some(removed) = self.table.remove(slot_idx)
        {
            let freed = removed.key.len() + removed.val.approx_bytes() + 64;
            self.used_memory = self.used_memory.saturating_sub(freed);
            inc_evicted_keys();
            return Some(freed);
        }

        None
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(None);
            }
            let val_ref = match &entry.val {
                RudisValue::Cooled { val, .. } => val.as_ref(),
                other => other,
            };
            match val_ref {
                RudisValue::String(b) => Ok(Some(b.clone())),
                RudisValue::Int(n) => Ok(Some(Self::format_i64(*n))),
                RudisValue::HyperLogLog(regs) => Ok(Some(Bytes::copy_from_slice(&regs[..]))),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(None);
            }
            let val_ref = match &entry.val {
                RudisValue::Cooled { val, .. } => val.as_ref(),
                other => other,
            };
            match val_ref {
                RudisValue::String(b) => Ok(Some(crate::shard::CompactResp::from_bulk(b))),
                RudisValue::Int(n) => {
                    let formatted = Self::format_i64(*n);
                    Ok(Some(crate::shard::CompactResp::from_slice(&formatted)))
                }
                RudisValue::HyperLogLog(regs) => Ok(Some(crate::shard::CompactResp::from_bulk(
                    &Bytes::copy_from_slice(&regs[..]),
                ))),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            Ok(None)
        }
    }

    #[inline(always)]
    pub fn write_get_resp(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool, &'static str> {
        let h = hash_key(key);
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::connection::write_resp_null(out);
                return Ok(true);
            }
            let val_ref = match &entry.val {
                RudisValue::Cooled { val, .. } => val.as_ref(),
                other => other,
            };
            match val_ref {
                RudisValue::String(b) => {
                    crate::connection::write_resp_bulk(out, b);
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
            }
        } else {
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
                let ttl = entry.expire_at.and_then(|exp| {
                    let now = Instant::now();
                    if exp > now {
                        Some(exp.duration_since(now))
                    } else {
                        None
                    }
                });
                let val = match &entry.val {
                    RudisValue::Cooled { val, .. } => (**val).clone(),
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
        if bytes.is_empty() {
            return None;
        }
        let (neg, s) = match bytes[0] {
            b'-' => (true, &bytes[1..]),
            b'+' => (false, &bytes[1..]),
            _ => (false, bytes),
        };
        if s.is_empty() || s.len() > 20 {
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
    pub fn set_with_hash(&mut self, key: Bytes, h: u64, value: Bytes, expire_in: Option<Duration>) {
        self.set_extended_with_hash(key, h, value, expire_in, false);
    }

    pub fn set_extended(
        &mut self,
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        keepttl: bool,
    ) {
        let h = hash_key(&key);
        self.set_extended_with_hash(key, h, value, expire_in, keepttl);
    }

    #[inline(always)]
    pub fn set_extended_with_hash(
        &mut self,
        key: Bytes,
        h: u64,
        value: Bytes,
        expire_in: Option<Duration>,
        keepttl: bool,
    ) {
        let val = if let Some(int_val) = Self::parse_i64_bytes(&value) {
            RudisValue::Int(int_val)
        } else {
            RudisValue::String(Bytes::copy_from_slice(&value))
        };
        let val_bytes = val.approx_bytes();
        let (existing, candidate_idx) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && let Some(entry) = self.table.get_slot_mut(idx)
        {
            let old_bytes = entry.val.approx_bytes();
            entry.val = val;
            if !keepttl {
                let had_exp = entry.expire_at.is_some();
                let will_exp = expire_in.is_some();
                if had_exp && !will_exp {
                    self.num_expires = self.num_expires.saturating_sub(1);
                } else if !had_exp && will_exp {
                    self.num_expires += 1;
                }
                entry.expire_at = expire_in.map(|d| Instant::now() + d);
            }
            self.used_memory = self.used_memory.saturating_sub(old_bytes) + val_bytes;
            return;
        }

        let expire_at = if keepttl {
            None
        } else {
            expire_in.map(|d| Instant::now() + d)
        };
        if expire_at.is_some() {
            self.num_expires += 1;
        }
        let entry_mem = key.len() + val_bytes + 64;
        let entry = RudisEntry {
            key: Bytes::copy_from_slice(&key),
            val,
            expire_at,
        };
        self.table.insert_prepared(entry, h, candidate_idx);
        self.used_memory += entry_mem;
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
        if self.num_expires == 0 {
            if let Some((idx, entry)) = self.table.find_entry(key, hash) {
                let val_bytes = match &entry.val {
                    RudisValue::String(b) => b.len(),
                    RudisValue::Int(_) => 8,
                    other => other.approx_bytes(),
                };
                let freed = entry.key.len() + val_bytes + 64;
                self.used_memory = self.used_memory.saturating_sub(freed);
                let entry = self.table.remove_present(idx);
                match entry.val {
                    RudisValue::String(_) | RudisValue::Int(_) => {}
                    other => self.recycle_value(other),
                }
                return true;
            }
            return false;
        }
        if let Some((idx, entry)) = self.table.find_entry(key, hash) {
            if let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return false;
            }
            let val_bytes = match &entry.val {
                RudisValue::String(b) => b.len(),
                RudisValue::Int(_) => 8,
                other => other.approx_bytes(),
            };
            let freed = entry.key.len() + val_bytes + 64;
            self.used_memory = self.used_memory.saturating_sub(freed);
            let entry = self.table.remove_present(idx);
            if entry.expire_at.is_some() {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
            match entry.val {
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
            if let Some(expire_at) = entry.expire_at
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
        key_bytes: Option<&Bytes>,
        delta: i64,
    ) -> Result<i64, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                match &mut entry.val {
                    RudisValue::Int(n) => {
                        let nv = n
                            .checked_add(delta)
                            .ok_or("increment or decrement would overflow")?;
                        *n = nv;
                        return Ok(nv);
                    }
                    RudisValue::String(b) => {
                        let current = Self::parse_i64_bytes(b)
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
        let entry = RudisEntry {
            key: key_bytes
                .cloned()
                .unwrap_or_else(|| Bytes::copy_from_slice(key)),
            val: RudisValue::Int(new_val),
            expire_at: None,
        };
        self.table.insert_prepared(entry, h, candidate_idx);
        self.used_memory += key.len() + 8 + 64;
        Ok(new_val)
    }

    #[inline]
    pub fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<i64, String> {
        self.incr_by_slice_fast(&key, delta)
            .map_err(|e| e.to_string())
    }

    pub fn expire(&mut self, key: &[u8], duration: Duration) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return false;
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                if entry.expire_at.is_none() {
                    self.num_expires += 1;
                }
                entry.expire_at = Some(Instant::now() + duration);
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
                && entry.expire_at.is_some()
            {
                entry.expire_at = None;
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
                match entry.expire_at {
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
                                diff.as_secs() as i64
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
                match entry.expire_at {
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
                            if in_millis {
                                target_epoch.as_millis() as i64
                            } else {
                                target_epoch.as_secs() as i64
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

    pub fn keys(&mut self, pattern: &[u8]) -> Vec<Bytes> {
        let mut res = Vec::new();
        let cap = self.table.capacity();
        for i in 0..cap {
            if self.check_expired_slot(i) {
                continue;
            }
            if let Some(entry) = self.table.get_slot(i)
                && crate::pubsub::glob_match(pattern, &entry.key)
            {
                res.push(entry.key.clone());
            }
        }
        res
    }

    pub fn scan(
        &mut self,
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> (usize, Vec<Bytes>) {
        let cap = self.table.capacity();
        if cursor >= cap || cap == 0 {
            return (0, Vec::new());
        }
        let mut res = Vec::new();
        let mut idx = cursor;
        while idx < cap {
            if !self.check_expired_slot(idx)
                && let Some(entry) = self.table.get_slot(idx)
            {
                let matches = match pattern {
                    Some(pat) => crate::pubsub::glob_match(pat, &entry.key),
                    None => true,
                };
                if matches {
                    res.push(entry.key.clone());
                }
            }
            idx += 1;
            if res.len() >= count {
                break;
            }
        }
        let next_cursor = if idx >= cap { 0 } else { idx };
        (next_cursor, res)
    }

    pub fn random_key(&mut self) -> Option<Bytes> {
        let cap = self.table.capacity();
        if self.table.is_empty() || cap == 0 {
            return None;
        }
        self.sample_cursor = (self.sample_cursor + 17) & (cap - 1);
        let start = self.sample_cursor;
        for i in 0..cap {
            let idx = (start + i) & (cap - 1);
            if self.check_expired_slot(idx) {
                continue;
            }
            if let Some(entry) = self.table.get_slot(idx) {
                return Some(entry.key.clone());
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
        self.num_expires = 0;
        let base_mem = self.table.capacity * std::mem::size_of::<Option<RudisEntry>>()
            + self.table.ctrl.len()
            + 16384 * 4;
        self.used_memory = base_mem;
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
                    RudisValue::Cooled { val, .. } => val.as_ref(),
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
                    RudisValue::Cooled { .. } => unreachable!(),
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
        if let RudisValue::Tiered(ptr) = entry.val {
            Some(ptr)
        } else {
            None
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
        if let RudisValue::Cooled { ptr, .. } = entry.val {
            Some(ptr)
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
            let old_bytes = entry.val.approx_bytes();
            entry.val = RudisValue::Tiered(ptr);
            let new_bytes = entry.val.approx_bytes();
            self.used_memory = self.used_memory.saturating_sub(old_bytes) + new_bytes;
            return true;
        }
        false
    }

    #[inline]
    pub fn set_cooled_pointer(&mut self, key: &[u8], ptr: TieredPointer) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && let Some(entry) = self.table.get_slot_mut(idx)
            && !matches!(entry.val, RudisValue::Tiered(_) | RudisValue::Cooled { .. })
        {
            let old_val = std::mem::replace(&mut entry.val, RudisValue::Tiered(ptr));
            entry.val = RudisValue::Cooled {
                ptr,
                val: Box::new(old_val),
            };
            self.used_memory += 24;
            return true;
        }
        false
    }

    #[inline]
    pub fn restore_tiered_value(&mut self, key: &[u8], val: RudisValue) -> bool {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && let Some(entry) = self.table.get_slot_mut(idx)
            && let RudisValue::Tiered(ptr) = entry.val
        {
            let val_bytes = val.approx_bytes();
            entry.val = RudisValue::Cooled {
                ptr,
                val: Box::new(val),
            };
            self.used_memory += val_bytes;
            return true;
        }
        false
    }

    #[inline]
    pub fn decommit_cooled_key(&mut self, key: &[u8]) -> Option<(TieredPointer, usize)> {
        let h = hash_key(key);
        let idx = self.table.find(key, h)?;
        let entry = self.table.get_slot_mut(idx)?;
        if let RudisValue::Cooled { ptr, val } = &entry.val {
            let p = *ptr;
            let freed = val.approx_bytes();
            entry.val = RudisValue::Tiered(p);
            self.used_memory = self.used_memory.saturating_sub(freed);
            Some((p, freed))
        } else {
            None
        }
    }

    pub fn decommit_all_cooled(&mut self) -> (usize, u64) {
        let mut count = 0;
        let mut total_freed = 0u64;
        for opt in self.table.slots.iter_mut() {
            if let Some(entry) = opt
                && let RudisValue::Cooled { ptr, val } = &entry.val
            {
                let p = *ptr;
                let freed = val.approx_bytes() as u64;
                entry.val = RudisValue::Tiered(p);
                self.used_memory = self.used_memory.saturating_sub(freed as usize);
                total_freed += freed;
                count += 1;
            }
        }
        (count, total_freed)
    }

    pub fn get_hot_keys_for_spill(&mut self, limit: usize) -> Vec<Bytes> {
        let mut hot = Vec::with_capacity(limit);
        let total_slots = self.table.slots.len();
        if total_slots == 0 {
            return hot;
        }
        let start = self.spill_cursor % total_slots;
        let mut idx = start;
        for _ in 0..total_slots {
            if let Some(entry) = &self.table.slots[idx]
                && !matches!(entry.val, RudisValue::Tiered(_) | RudisValue::Cooled { .. })
            {
                hot.push(entry.key.clone());
                if hot.len() >= limit {
                    self.spill_cursor = (idx + 1) % total_slots;
                    return hot;
                }
            }
            idx = (idx + 1) % total_slots;
        }
        self.spill_cursor = idx;
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
            RudisValue::Tiered(_) | RudisValue::Cooled { .. } => return None,
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
                count += 1;
            }
        }
        count
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

        // If dst exists, remove it first
        let h_dst = hash_key(&dst);
        if let Some(dst_idx) = self.table.find(&dst, h_dst) {
            self.table.remove(dst_idx);
        }

        // Update entry key to dst and insert
        entry.key = dst;
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
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::String(old) => {
                        let prev = old.clone();
                        *old = value;
                        if entry.expire_at.take().is_some() {
                            self.num_expires = self.num_expires.saturating_sub(1);
                        }
                        Ok(Some(prev))
                    }
                    RudisValue::Int(n) => {
                        let prev = Self::format_i64(*n);
                        if let Some(int_val) = Self::parse_i64_bytes(&value) {
                            entry.val = RudisValue::Int(int_val);
                        } else {
                            entry.val = RudisValue::String(value);
                        }
                        if entry.expire_at.take().is_some() {
                            self.num_expires = self.num_expires.saturating_sub(1);
                        }
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
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::String(s) => {
                        let val = s.clone();
                        self.table.remove(idx);
                        Ok(Some(val))
                    }
                    RudisValue::Int(n) => {
                        let val = Self::format_i64(*n);
                        self.table.remove(idx);
                        Ok(Some(val))
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

    pub fn append(&mut self, key: Bytes, val_to_append: &[u8]) -> Result<usize, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                let new_val = Bytes::copy_from_slice(val_to_append);
                let len = new_val.len();
                self.set(key, new_val, None);
                return Ok(len);
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::String(s) => {
                        let mut combined = Vec::with_capacity(s.len() + val_to_append.len());
                        combined.extend_from_slice(s);
                        combined.extend_from_slice(val_to_append);
                        let len = combined.len();
                        *s = Bytes::from(combined);
                        Ok(len)
                    }
                    RudisValue::Int(n) => {
                        let s = Self::format_i64(*n);
                        let mut combined = Vec::with_capacity(s.len() + val_to_append.len());
                        combined.extend_from_slice(&s);
                        combined.extend_from_slice(val_to_append);
                        let len = combined.len();
                        entry.val = RudisValue::String(Bytes::from(combined));
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
                let slice = match &entry.val {
                    RudisValue::String(s) => s.as_ref(),
                    RudisValue::Int(n) => {
                        let formatted = Self::format_i64(*n);
                        return Self::slice_range(&formatted, start, end);
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
            && let Some(entry) = self.table.get_slot_mut(idx)
        {
            let mut bytes: Vec<u8> = match &entry.val {
                RudisValue::String(s) => s.to_vec(),
                RudisValue::Int(n) => Self::format_i64(*n).to_vec(),
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
            entry.val = RudisValue::String(Bytes::from(bytes));
            return Ok(len);
        }
        if value.is_empty() && offset == 0 {
            return Ok(0);
        }
        let mut bytes = vec![0u8; offset];
        bytes.extend_from_slice(value);
        let len = bytes.len();
        let entry = RudisEntry {
            key,
            val: RudisValue::String(Bytes::from(bytes)),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(len)
    }

    pub fn incrbyfloat(&mut self, key: Bytes, delta: f64) -> Result<f64, &'static str> {
        let h = hash_key(&key);
        let (existing, _) = self.table.find_or_prepare_insert(&key, h);
        if let Some(idx) = existing
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.table.get_slot_mut(idx)
        {
            let curr: f64 = match &entry.val {
                RudisValue::String(s) => {
                    let str_val =
                        std::str::from_utf8(s).map_err(|_| "ERR value is not a valid float")?;
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
            entry.val = RudisValue::String(Bytes::from(new_val.to_string()));
            return Ok(new_val);
        }
        if delta.is_nan() || delta.is_infinite() {
            return Err("ERR increment would produce NaN or Infinity");
        }
        let entry = RudisEntry {
            key,
            val: RudisValue::String(Bytes::from(delta.to_string())),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(delta)
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                match &mut entry.val {
                    RudisValue::SmallHash(pairs) => {
                        if pairs.len() == 1 && pairs[0].0 == *field {
                            pairs[0].1 = val.clone();
                            if val.len() > max_value {
                                let map: RudisHashMap = pairs.drain(..).collect();
                                entry.val = RudisValue::Hash(map);
                            }
                            return Ok(0);
                        }
                        if let Some(pos) = pairs.iter().position(|(k, _)| k == field) {
                            pairs[pos].1 = val.clone();
                            if val.len() > max_value {
                                let map: RudisHashMap = pairs.drain(..).collect();
                                entry.val = RudisValue::Hash(map);
                            }
                            return Ok(0);
                        } else {
                            pairs.push((field.clone(), val.clone()));
                            if pairs.len() > max_entries
                                || field.len() > max_value
                                || val.len() > max_value
                            {
                                let map: RudisHashMap = pairs.drain(..).collect();
                                entry.val = RudisValue::Hash(map);
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
            RudisValue::SmallHash(pairs)
        } else {
            let mut map = RudisHashMap::with_capacity_and_hasher(1, FxBuildHasher::default());
            map.insert(field.clone(), val.clone());
            RudisValue::Hash(map)
        };
        let entry = RudisEntry {
            key: key.clone(),
            val,
            expire_at: None,
        };
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
        key_bytes: Option<&Bytes>,
        fields: &[(Bytes, Bytes)],
    ) -> Result<usize, &'static str> {
        let max_entries =
            crate::connection::HASH_MAX_ENTRIES.load(std::sync::atomic::Ordering::Relaxed);
        let max_value =
            crate::connection::HASH_MAX_VALUE.load(std::sync::atomic::Ordering::Relaxed);
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                match &mut entry.val {
                    RudisValue::SmallHash(pairs) => {
                        let mut added = 0;
                        if fields.len() == 1 {
                            let (f, v) = &fields[0];
                            if pairs.len() == 1 && pairs[0].0 == *f {
                                pairs[0].1 = v.clone();
                                if v.len() > max_value {
                                    let map: RudisHashMap = pairs.drain(..).collect();
                                    entry.val = RudisValue::Hash(map);
                                }
                                return Ok(0);
                            }
                            if let Some(pos) = pairs.iter().position(|(k, _)| k == f) {
                                pairs[pos].1 = v.clone();
                                if v.len() > max_value {
                                    let map: RudisHashMap = pairs.drain(..).collect();
                                    entry.val = RudisValue::Hash(map);
                                }
                            } else {
                                pairs.push((f.clone(), v.clone()));
                                if pairs.len() > max_entries
                                    || f.len() > max_value
                                    || v.len() > max_value
                                {
                                    let map: RudisHashMap = pairs.drain(..).collect();
                                    entry.val = RudisValue::Hash(map);
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
                                entry.val = RudisValue::Hash(map);
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
            (RudisValue::SmallHash(pairs), added)
        } else {
            let mut map =
                RudisHashMap::with_capacity_and_hasher(fields.len(), FxBuildHasher::default());
            let mut added = 0;
            for (f, v) in fields {
                if map.insert(f.clone(), v.clone()).is_none() {
                    added += 1;
                }
            }
            (RudisValue::Hash(map), added)
        };
        let entry = RudisEntry {
            key: key_bytes
                .cloned()
                .unwrap_or_else(|| Bytes::copy_from_slice(key)),
            val,
            expire_at: None,
        };
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
            && let Some(entry) = self.table.get_slot_mut(idx)
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
                        entry.val = RudisValue::Hash(map);
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
            RudisValue::Hash(map)
        } else {
            RudisValue::SmallHash(vec![(field, value)])
        };
        let entry = RudisEntry {
            key,
            val,
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(1)
    }

    pub fn hget(&mut self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h)
            && let Some(entry) = self.table.get_slot(idx)
        {
            if let Some(expire_at) = entry.expire_at
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
                    for (k, v) in pairs {
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
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(crate::shard::CompactResp::NULL);
            }
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
                            for (k, v) in pairs {
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
        let h = hash_key(key);
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
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
                            for (k, v) in pairs {
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
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (count, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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

            if is_empty && let Some(entry) = self.table.remove(idx) {
                self.recycle_value(entry.val);
            }
            Ok(count)
        } else {
            Ok(0)
        }
    }

    pub fn hexists(&mut self, key: &[u8], field: &[u8]) -> Result<bool, &'static str> {
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
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(Vec::new());
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(pairs) => Ok(pairs.clone()),
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
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok((vec![None; fields.len()], Vec::new()));
            }
            let (results, deleted_fields, is_empty) =
                if let Some(entry) = self.table.get_slot_mut(idx) {
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
                        if std::str::from_utf8(s)
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
                    RudisValue::Set(RudisSet::Small(_)) => Some("intset"),
                    RudisValue::Set(RudisSet::Full(_)) => Some("hashtable"),
                    RudisValue::ZSet(RudisZSet::Small(_)) => Some("listpack"),
                    RudisValue::ZSet(RudisZSet::Full { .. }) => Some("skiplist"),
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
            && let Some(entry) = self.table.get_slot_mut(idx)
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
                            entry.val = RudisValue::Hash(map);
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
        let entry = RudisEntry {
            key,
            val: RudisValue::SmallHash(vec![(field, val_bytes)]),
            expire_at: None,
        };
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
            && let Some(entry) = self.table.get_slot_mut(idx)
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
                            entry.val = RudisValue::Hash(map);
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
        let entry = RudisEntry {
            key,
            val: RudisValue::SmallHash(vec![(field, val_bytes)]),
            expire_at: None,
        };
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
                    RudisValue::SmallHash(p) => p.clone(),
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
    ) -> Result<(usize, Vec<Bytes>), &'static str> {
        let h = hash_key(key);
        let pairs: Vec<(Bytes, Bytes)> = if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok((0, Vec::new()));
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::SmallHash(p) => p.clone(),
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
        while idx < pairs.len() && res.len() < count * 2 {
            let (f, v) = &pairs[idx];
            let matches = match pattern {
                Some(pat) => crate::pubsub::glob_match(pat, f),
                None => true,
            };
            if matches {
                res.push(f.clone());
                res.push(v.clone());
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
        key_bytes: Option<&Bytes>,
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
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
        let entry = RudisEntry {
            key: key_bytes
                .cloned()
                .unwrap_or_else(|| Bytes::copy_from_slice(key)),
            val: RudisValue::List(deque),
            expire_at: None,
        };
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
        key_bytes: Option<&Bytes>,
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
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
        let entry = RudisEntry {
            key: key_bytes
                .cloned()
                .unwrap_or_else(|| Bytes::copy_from_slice(key)),
            val: RudisValue::List(deque),
            expire_at: None,
        };
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
            if let Some(entry) = self.table.get_slot_mut(idx) {
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
            if let Some(entry) = self.table.get_slot_mut(idx) {
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                if count.is_some() {
                    crate::connection::write_resp_array_header(out, 0);
                } else {
                    crate::connection::write_resp_null(out);
                }
                return Ok(false);
            }

            let mut has_written = false;
            let is_empty = match &mut entry.val {
                RudisValue::List(deque) => {
                    if count.is_none() && deque.len() == 1 {
                        let val = deque.pop_front().unwrap();
                        crate::connection::write_resp_bulk(out, &val);
                        let entry = self.table.remove_present(idx);
                        if self.num_expires > 0 && entry.expire_at.is_some() {
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
                if self.num_expires > 0 && entry.expire_at.is_some() {
                    self.num_expires = self.num_expires.saturating_sub(1);
                }
                self.recycle_value(entry.val);
            }
            Ok(has_written)
        } else {
            if count.is_some() {
                crate::connection::write_resp_array_header(out, 0);
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                if count.is_some() {
                    crate::connection::write_resp_array_header(out, 0);
                } else {
                    crate::connection::write_resp_null(out);
                }
                return Ok(false);
            }

            let mut has_written = false;
            let is_empty = match &mut entry.val {
                RudisValue::List(deque) => {
                    if count.is_none() && deque.len() == 1 {
                        let val = deque.pop_back().unwrap();
                        crate::connection::write_resp_bulk(out, &val);
                        let entry = self.table.remove_present(idx);
                        if self.num_expires > 0 && entry.expire_at.is_some() {
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
                if self.num_expires > 0 && entry.expire_at.is_some() {
                    self.num_expires = self.num_expires.saturating_sub(1);
                }
                self.recycle_value(entry.val);
            }
            Ok(has_written)
        } else {
            if count.is_some() {
                crate::connection::write_resp_array_header(out, 0);
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(None);
            }
            let (popped, is_empty) = match &mut entry.val {
                RudisValue::List(deque) => {
                    if deque.len() == 1 {
                        let val = deque.pop_front();
                        let entry = self.table.remove_present(idx);
                        if self.num_expires > 0 && entry.expire_at.is_some() {
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
                if self.num_expires > 0 && entry.expire_at.is_some() {
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(None);
            }
            let (popped, is_empty) = match &mut entry.val {
                RudisValue::List(deque) => {
                    if deque.len() == 1 {
                        let val = deque.pop_back();
                        let entry = self.table.remove_present(idx);
                        if self.num_expires > 0 && entry.expire_at.is_some() {
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
                if self.num_expires > 0 && entry.expire_at.is_some() {
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
            let (popped, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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
            let (popped, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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
        if let Some((idx, entry)) = self.table.find_entry(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
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
                && let Some(expire_at) = entry.expire_at
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
            let is_empty = if let Some(entry) = self.table.get_slot_mut(idx) {
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
                                *deque = deque
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
            if let Some(entry) = self.table.get_slot_mut(idx) {
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
            let (removed, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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
            if let Some(entry) = self.table.get_slot_mut(idx) {
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

        let (popped, is_empty) = match self.table.get_slot_mut(src_idx) {
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
            && let Some(entry) = self.table.get_slot_mut(dst_idx)
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
        self.table.insert(RudisEntry {
            key: destination,
            val: RudisValue::List(deque),
            expire_at: None,
        });
        Ok(Some(val))
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                match &mut entry.val {
                    RudisValue::Set(RudisSet::Small(v)) if v.len() == 1 => {
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
                        return Ok(1);
                    }
                    RudisValue::Set(RudisSet::Small(v)) if v.is_empty() => {
                        let m_bytes = member.as_ref();
                        let m_hash = hash64(m_bytes);
                        v.push(SmallSetEntry {
                            hash: m_hash,
                            member: member.clone(),
                        });
                        return Ok(1);
                    }
                    RudisValue::Set(set) => {
                        let added = if set.insert_slice(member) { 1 } else { 0 };
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

        let (_, insert_idx) = self.table.find_or_prepare_insert(key.as_ref(), h);
        let mut v = self.arena.acquire_small_set(1);
        let m_bytes = member.as_ref();
        let m_hash = hash64(m_bytes);
        v.push(SmallSetEntry {
            hash: m_hash,
            member: member.clone(),
        });
        let entry = RudisEntry {
            key: key.clone(),
            val: RudisValue::Set(RudisSet::Small(v)),
            expire_at: None,
        };
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
        key_bytes: Option<&Bytes>,
        members: &[Bytes],
    ) -> Result<usize, &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
                match &mut entry.val {
                    RudisValue::Set(set) => {
                        if members.len() == 1 {
                            let added = if set.insert_slice(&members[0]) { 1 } else { 0 };
                            return Ok(added);
                        }
                        let mut added = 0;
                        for m in members {
                            if set.insert_slice(m) {
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
        let entry = RudisEntry {
            key: key_bytes
                .cloned()
                .unwrap_or_else(|| Bytes::copy_from_slice(key)),
            val: RudisValue::Set(set),
            expire_at: None,
        };
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
            let (removed_count, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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
                self.recycle_value(entry.val);
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
                && let Some(expire_at) = entry.expire_at
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                return Ok(crate::shard::CompactResp::INT_0);
            }
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
                && let Some(expire_at) = entry.expire_at
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
            let (popped, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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
                        RudisValue::Set(s) => sets.push(s.clone()),
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
                                first_set = Some(s.clone());
                            } else {
                                other_sets.push(s.clone());
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
        let mut sets: Vec<RudisSet> = Vec::with_capacity(keys.len());
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    missing = true;
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => sets.push(s.clone()),
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
            return Ok(0);
        }
        sets.sort_by_key(|s| s.len());
        let first = &sets[0];
        let mut count = 0;
        for m in first.iter() {
            if sets[1..].iter().all(|s| s.contains(m.as_ref())) {
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
        let mut sets: Vec<RudisSet> = Vec::with_capacity(keys.len());
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Set(s) => sets.push(s.clone()),
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                }
            }
        }
        let mut union_set = hashbrown::HashSet::new();
        for s in sets {
            for m in s.iter() {
                union_set.insert(m.clone());
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
                                first_set = Some(s.clone());
                            } else {
                                other_sets.push(s.clone());
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
            return Ok(0);
        }
        let first = first_set.unwrap();
        let mut count = 0;
        for m in first.iter() {
            if !other_sets.iter().any(|s| s.contains(m.as_ref())) {
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

        if let Some(idx) = src_idx
            && let Some(entry) = self.table.get_slot(idx)
        {
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

        let src_idx = match src_idx {
            Some(idx) => idx,
            None => {
                return Ok(SmoveResult {
                    moved: false,
                    dst_added: false,
                });
            }
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

        let empty_after = if let Some(entry) = self.table.get_slot_mut(src_idx) {
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
                if let Some(entry) = self.table.get_slot_mut(dst_idx) {
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
            self.table.insert(RudisEntry {
                key: destination,
                val: RudisValue::Set(new_set),
                expire_at: None,
            });
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
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(usize, Vec<Bytes>), &'static str> {
        let h = hash_key(key);
        let items: Vec<Bytes> = if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok((0, Vec::new()));
            }
            if let Some(entry) = self.table.get_slot(idx) {
                match &entry.val {
                    RudisValue::Set(s) => s.to_vec(),
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
            let m = &items[idx];
            let matches = match pattern {
                Some(pat) => crate::pubsub::glob_match(pat, m),
                None => true,
            };
            if matches {
                res.push(m.clone());
            }
            idx += 1;
        }
        let next_cursor = if idx >= items.len() { 0 } else { idx };
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
            let entry = RudisEntry {
                key: dest,
                val: RudisValue::ZSet(zset),
                expire_at: None,
            };
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
            let entry = RudisEntry {
                key: dest,
                val: RudisValue::ZSet(zset),
                expire_at: None,
            };
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
            let entry = RudisEntry {
                key: dest,
                val: RudisValue::ZSet(zset),
                expire_at: None,
            };
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
        enum SetOrZSet {
            Set(RudisSet),
            ZSet(RudisZSet),
        }
        impl SetOrZSet {
            fn len(&self) -> usize {
                match self {
                    SetOrZSet::Set(s) => s.len(),
                    SetOrZSet::ZSet(zs) => zs.len(),
                }
            }
            fn contains(&self, m: &[u8]) -> bool {
                match self {
                    SetOrZSet::Set(s) => s.contains(m),
                    SetOrZSet::ZSet(zs) => zs.get_score(m).is_some(),
                }
            }
        }
        let mut collections: Vec<SetOrZSet> = Vec::with_capacity(keys.len());
        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    missing = true;
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::ZSet(zs) => collections.push(SetOrZSet::ZSet(zs.clone())),
                        RudisValue::Set(s) => collections.push(SetOrZSet::Set(s.clone())),
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
        if missing || collections.is_empty() {
            return Ok(0);
        }
        collections.sort_by_key(|c| c.len());
        let mut count = 0;
        let check_other =
            |m: &[u8], cols: &[SetOrZSet]| -> bool { cols.iter().all(|c| c.contains(m)) };
        match &collections[0] {
            SetOrZSet::Set(s) => {
                for m in s.iter() {
                    if check_other(m.as_ref(), &collections[1..]) {
                        count += 1;
                        if limit > 0 && count >= limit {
                            return Ok(limit);
                        }
                    }
                }
            }
            SetOrZSet::ZSet(zs) => {
                let mut early_exit = false;
                zs.for_each(|m, _| {
                    if early_exit {
                        return;
                    }
                    if check_other(m.as_ref(), &collections[1..]) {
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
        if self.table.slot_counts[slot as usize] == 0 {
            return 0;
        }
        if self.num_expires == 0 {
            return self.table.slot_counts[slot as usize] as usize;
        }
        let now = Instant::now();
        let mut count = 0;
        let mut expired_indices = Vec::new();
        for (idx, opt) in self.table.slots.iter().enumerate() {
            if let Some(entry) = opt
                && crate::router::key_slot(&entry.key) == slot
            {
                if let Some(exp) = entry.expire_at
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
                && removed.expire_at.is_some()
            {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
        }
        count
    }

    pub fn get_keys_in_slot(&mut self, slot: u16, count: usize) -> Vec<Bytes> {
        let mut result = Vec::new();
        let mut expired_indices = Vec::new();
        let now = if self.num_expires > 0 {
            Some(Instant::now())
        } else {
            None
        };
        for (idx, opt) in self.table.slots.iter().enumerate() {
            if let Some(entry) = opt
                && crate::router::key_slot(&entry.key) == slot
            {
                if let (Some(now_inst), Some(exp)) = (now, entry.expire_at)
                    && now_inst >= exp
                {
                    expired_indices.push(idx);
                    continue;
                }
                result.push(entry.key.clone());
                if result.len() >= count {
                    break;
                }
            }
        }
        for idx in expired_indices {
            if let Some(removed) = self.table.remove(idx)
                && removed.expire_at.is_some()
            {
                self.num_expires = self.num_expires.saturating_sub(1);
            }
        }
        result
    }

    pub fn flush_slots(&mut self, ranges: &[(u16, u16)]) -> usize {
        let mut to_remove = Vec::new();
        for (idx, opt) in self.table.slots.iter().enumerate() {
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
                && removed.expire_at.is_some()
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
        key_bytes: Option<&Bytes>,
        elements: &[(f64, Bytes)],
        flags: ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        if let Some((idx, entry)) = self.table.find_entry_mut(key, h) {
            if self.num_expires > 0
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
            } else {
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

        let entry = RudisEntry {
            key: key_bytes
                .cloned()
                .unwrap_or_else(|| Bytes::copy_from_slice(key)),
            val: RudisValue::ZSet(zset),
            expire_at: None,
        };
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
            } else if let Some(entry) = self.table.get_slot_mut(idx) {
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
        let entry = RudisEntry {
            key,
            val: RudisValue::ZSet(zset),
            expire_at: None,
        };
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
        if !opts.by_score && !opts.by_lex {
            if let Some((idx, entry)) = self.table.find_entry(key, h) {
                if self.num_expires > 0
                    && let Some(expire_at) = entry.expire_at
                    && !crate::connection::ALLOW_ACCESS_EXPIRED
                        .load(std::sync::atomic::Ordering::Relaxed)
                    && Instant::now() >= expire_at
                {
                    self.expire_slot(idx);
                    return Ok(crate::shard::CompactResp::EMPTY_ARRAY);
                }
                match &entry.val {
                    RudisValue::ZSet(zset) => {
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
                && let Some(expire_at) = entry.expire_at
                && !crate::connection::ALLOW_ACCESS_EXPIRED
                    .load(std::sync::atomic::Ordering::Relaxed)
                && Instant::now() >= expire_at
            {
                self.expire_slot(idx);
                crate::connection::write_resp_array_header(out, 0);
                return Ok(());
            }
            match &entry.val {
                RudisValue::ZSet(zset) => {
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
        if items.is_empty() {
            if let Some(idx) = self.table.find(dst, h_dst) {
                self.table.remove(idx);
            }
            return Ok(0);
        }
        let mut zset = RudisZSet::new();
        for (member, score) in &items {
            zset.insert(*score, member.clone());
        }
        let count = items.len();
        self.table.insert(RudisEntry {
            key: Bytes::copy_from_slice(dst),
            val: RudisValue::ZSet(zset),
            expire_at: None,
        });
        Ok(count)
    }

    pub fn zrem(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            let (removed_count, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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
            let (res, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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
            let (res, is_empty) = if let Some(entry) = self.table.get_slot_mut(idx) {
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
            if let Some(entry) = self.table.get_slot_mut(idx) {
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
            if let Some(entry) = self.table.get_slot_mut(idx) {
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
            if let Some(entry) = self.table.get_slot_mut(idx) {
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

    /// Active sampling cycle: samples up to 20 slots starting from cursor and evicts expired keys.
    pub fn active_expire_cycle(&mut self) -> usize {
        let cap = self.table.capacity();
        if cap == 0 || self.table.is_empty() {
            return 0;
        }

        let mut expired_count = 0;
        let mut checked = 0;
        while checked < 20 {
            let idx = self.sample_cursor % cap;
            self.sample_cursor = (self.sample_cursor + 1) % cap;
            if self.check_expired_slot(idx) {
                expired_count += 1;
            }
            checked += 1;
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
    pub fn setbit(&mut self, key: Bytes, offset: usize, value: u8) -> Result<u8, &'static str> {
        if value > 1 {
            return Err("bit is not an integer or out of range");
        }
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);

        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            let was_exp = self.check_expired_slot(idx);
            if !was_exp && let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::String(b) => {
                        let mut vec = b.to_vec();
                        if vec.len() <= byte_idx {
                            vec.resize(byte_idx + 1, 0);
                        }
                        let old_byte = vec[byte_idx];
                        let old_bit = (old_byte >> bit_idx) & 1;
                        if value == 1 {
                            vec[byte_idx] |= 1 << bit_idx;
                        } else {
                            vec[byte_idx] &= !(1 << bit_idx);
                        }
                        *b = Bytes::from(vec);
                        return Ok(old_bit);
                    }
                    RudisValue::Int(n) => {
                        let mut vec = Self::format_i64(*n).to_vec();
                        if vec.len() <= byte_idx {
                            vec.resize(byte_idx + 1, 0);
                        }
                        let old_byte = vec[byte_idx];
                        let old_bit = (old_byte >> bit_idx) & 1;
                        if value == 1 {
                            vec[byte_idx] |= 1 << bit_idx;
                        } else {
                            vec[byte_idx] &= !(1 << bit_idx);
                        }
                        entry.val = RudisValue::String(Bytes::from(vec));
                        return Ok(old_bit);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                }
            }
        }

        let mut vec = vec![0u8; byte_idx + 1];
        if value == 1 {
            vec[byte_idx] |= 1 << bit_idx;
        }
        let entry = RudisEntry {
            key,
            val: RudisValue::String(Bytes::from(vec)),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(0)
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
                        let bit = (b[byte_idx] >> bit_idx) & 1;
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
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot(idx) {
                let bytes_data: Option<Vec<u8>> = match &entry.val {
                    RudisValue::String(b) => Some(b.to_vec()),
                    RudisValue::Int(n) => Some(Self::format_i64(*n).to_vec()),
                    _ => None,
                };
                if let Some(b) = bytes_data {
                    let len = b.len() as i64;
                    if len == 0 {
                        return Ok(0);
                    }
                    let s = match start {
                        Some(v) => {
                            if v < 0 {
                                (len + v).max(0) as usize
                            } else {
                                v.min(len) as usize
                            }
                        }
                        None => 0,
                    };
                    let e = match end {
                        Some(v) => {
                            if v < 0 {
                                (len + v).max(0) as usize
                            } else {
                                v.min(len - 1) as usize
                            }
                        }
                        None => (len - 1) as usize,
                    };
                    if s > e || s >= b.len() {
                        return Ok(0);
                    }
                    let slice = &b[s..=e.min(b.len() - 1)];
                    let count: usize = slice.iter().map(|byte| byte.count_ones() as usize).sum();
                    return Ok(count);
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
                let bytes_data: Option<Vec<u8>> = match &entry.val {
                    RudisValue::String(b) => Some(b.to_vec()),
                    RudisValue::Int(n) => Some(Self::format_i64(*n).to_vec()),
                    _ => None,
                };
                if let Some(b) = bytes_data {
                    let len = b.len() as i64;
                    if len == 0 {
                        return Ok(if bit == 0 { 0 } else { -1 });
                    }
                    let s = match start {
                        Some(v) => {
                            if v < 0 {
                                (len + v).max(0) as usize
                            } else {
                                v.min(len) as usize
                            }
                        }
                        None => 0,
                    };
                    let e = match end {
                        Some(v) => {
                            if v < 0 {
                                (len + v).max(0) as usize
                            } else {
                                v.min(len - 1) as usize
                            }
                        }
                        None => (len - 1) as usize,
                    };
                    if s > e || s >= b.len() {
                        return Ok(-1);
                    }
                    for (i, &byte) in b[s..=e.min(b.len() - 1)].iter().enumerate() {
                        let byte_offset = s + i;
                        for bit_idx in 0..8 {
                            let curr_bit = (byte >> (7 - bit_idx)) & 1;
                            if curr_bit == bit {
                                return Ok((byte_offset * 8 + bit_idx) as i64);
                            }
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
        let mut max_len = 0;
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
            max_len = max_len.max(b.len());
            buffers.push(b);
        }

        let mut result = vec![0u8; max_len];
        match op.as_str() {
            "AND" => {
                for (i, out) in result.iter_mut().enumerate() {
                    let mut b = 0xFF;
                    for buf in &buffers {
                        b &= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = b;
                }
            }
            "OR" => {
                for (i, out) in result.iter_mut().enumerate() {
                    let mut b = 0;
                    for buf in &buffers {
                        b |= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = b;
                }
            }
            "XOR" => {
                for (i, out) in result.iter_mut().enumerate() {
                    let mut b = 0;
                    for buf in &buffers {
                        b ^= buf.get(i).copied().unwrap_or(0);
                    }
                    *out = b;
                }
            }
            "NOT" => {
                if buffers.len() != 1 {
                    return Err("BITOP NOT takes only one source key");
                }
                for (i, out) in result.iter_mut().enumerate() {
                    *out = !buffers[0].get(i).copied().unwrap_or(0);
                }
            }
            _ => return Err("syntax error"),
        }

        let len = result.len();
        self.set(destkey, Bytes::from(result), None);
        Ok(len)
    }

    // HYPERLOGLOG OPERATIONS
    pub fn pfadd(&mut self, key: Bytes, elements: &[Bytes]) -> Result<bool, &'static str> {
        let mut updated = false;
        let h = hash_key(&key);
        let existing_registers = if let Some(idx) = self.table.find(&key, h) {
            let was_exp = self.check_expired_slot(idx);
            if !was_exp {
                if let Some(entry) = self.table.get_slot_mut(idx) {
                    match &mut entry.val {
                        RudisValue::HyperLogLog(regs) => Some(regs),
                        RudisValue::String(s) if s.len() == 16384 => {
                            let mut arr = Box::new([0u8; 16384]);
                            arr.copy_from_slice(s);
                            entry.val = RudisValue::HyperLogLog(arr);
                            match &mut entry.val {
                                RudisValue::HyperLogLog(regs) => Some(regs),
                                _ => unreachable!(),
                            }
                        }
                        _ => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(regs) = existing_registers {
            for elem in elements {
                let h = hash_key(elem);
                let reg_idx = (h & 0x3FFF) as usize; // 14 bits (0..16383)
                let rho = ((h >> 14).leading_zeros() as u8 + 1).min(51);
                if rho > regs[reg_idx] {
                    regs[reg_idx] = rho;
                    updated = true;
                }
            }
            return Ok(updated);
        }

        let mut regs = Box::new([0u8; 16384]);
        for elem in elements {
            let h = hash_key(elem);
            let reg_idx = (h & 0x3FFF) as usize;
            let rho = ((h >> 14).leading_zeros() as u8 + 1).min(51);
            if rho > regs[reg_idx] {
                regs[reg_idx] = rho;
                updated = true;
            }
        }
        let entry = RudisEntry {
            key,
            val: RudisValue::HyperLogLog(regs),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(updated)
    }

    pub fn pfcount(&mut self, keys: &[Bytes]) -> Result<u64, &'static str> {
        let mut merged = [0u8; 16384];
        let mut has_hll = false;

        for k in keys {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h)
                && !self.check_expired_slot(idx)
                && let Some(entry) = self.table.get_slot(idx)
            {
                match &entry.val {
                    RudisValue::HyperLogLog(regs) => {
                        has_hll = true;
                        for i in 0..16384 {
                            merged[i] = merged[i].max(regs[i]);
                        }
                    }
                    RudisValue::String(s) if s.len() == 16384 => {
                        has_hll = true;
                        for i in 0..16384 {
                            merged[i] = merged[i].max(s[i]);
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
            return Ok(0);
        }

        const M: f64 = 16384.0;
        const ALPHA: f64 = 0.7213475;
        let mut sum = 0.0;
        let mut zeros = 0;
        for &val in merged.iter() {
            sum += 2.0_f64.powi(-(val as i32));
            if val == 0 {
                zeros += 1;
            }
        }

        let raw_estimate = ALPHA * M * M / sum;
        if raw_estimate <= 2.5 * M && zeros > 0 {
            let count = M * (M / zeros as f64).ln();
            Ok(count.round() as u64)
        } else {
            Ok(raw_estimate.round() as u64)
        }
    }

    pub fn pfmerge(&mut self, destkey: Bytes, srckeys: &[Bytes]) -> Result<(), &'static str> {
        let mut merged = [0u8; 16384];
        let h = hash_key(&destkey);
        if let Some(idx) = self.table.find(&destkey, h)
            && !self.check_expired_slot(idx)
            && let Some(entry) = self.table.get_slot(idx)
        {
            match &entry.val {
                RudisValue::HyperLogLog(regs) => {
                    for i in 0..16384 {
                        merged[i] = merged[i].max(regs[i]);
                    }
                }
                RudisValue::String(s) if s.len() == 16384 => {
                    for i in 0..16384 {
                        merged[i] = merged[i].max(s[i]);
                    }
                }
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
                    RudisValue::HyperLogLog(regs) => {
                        for i in 0..16384 {
                            merged[i] = merged[i].max(regs[i]);
                        }
                    }
                    RudisValue::String(s) if s.len() == 16384 => {
                        for i in 0..16384 {
                            merged[i] = merged[i].max(s[i]);
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

        let h = hash_key(&destkey);
        if let Some(idx) = self.table.find(&destkey, h)
            && let Some(entry) = self.table.get_slot_mut(idx)
        {
            entry.val = RudisValue::HyperLogLog(Box::new(merged));
            return Ok(());
        }

        let entry = RudisEntry {
            key: destkey,
            val: RudisValue::HyperLogLog(Box::new(merged)),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(())
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
                let ms = if now_ms > stream.last_id.ms {
                    now_ms
                } else {
                    stream.last_id.ms
                };
                let seq = if ms == stream.last_id.ms {
                    if stream.last_id == StreamId::default() && stream.entries.is_empty() {
                        if ms == 0 { 1 } else { 0 }
                    } else {
                        stream.last_id.seq + 1
                    }
                } else {
                    if ms == 0 { 1 } else { 0 }
                };
                StreamId::new(ms, seq)
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

    fn apply_stream_trim(
        stream: &mut RudisStream,
        maxlen: Option<usize>,
        minid: Option<StreamId>,
    ) -> usize {
        let mut trimmed = 0;
        if let Some(max) = maxlen {
            while stream.entries.len() > max {
                if let Some(first_key) = stream.entries.keys().next().copied() {
                    stream.entries.remove(&first_key);
                    trimmed += 1;
                } else {
                    break;
                }
            }
        }
        if let Some(min_id) = minid {
            while let Some(first_key) = stream.entries.keys().next().copied() {
                if first_key < min_id {
                    stream.entries.remove(&first_key);
                    trimmed += 1;
                } else {
                    break;
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
    ) -> Result<Option<StreamId>, &'static str> {
        let h = hash_key(&key);
        if let Some(idx) = self.table.find(&key, h) {
            if self.check_expired_slot(idx) {
                if nomkstream {
                    return Ok(None);
                }
                let mut stream = RudisStream::new();
                let final_id = Self::compute_stream_id(&mut stream, add_id)?;
                stream.last_id = final_id;
                stream.entries.insert(final_id, fields);
                Self::apply_stream_trim(&mut stream, maxlen, minid);

                let entry = RudisEntry {
                    key,
                    val: RudisValue::Stream(stream),
                    expire_at: None,
                };
                self.table.insert(entry);
                return Ok(Some(final_id));
            }

            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let final_id = Self::compute_stream_id(stream, add_id)?;
                        stream.last_id = final_id;
                        stream.entries.insert(final_id, fields);
                        Self::apply_stream_trim(stream, maxlen, minid);
                        return Ok(Some(final_id));
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
            return Ok(None);
        }

        let mut stream = RudisStream::new();
        let final_id = Self::compute_stream_id(&mut stream, add_id)?;
        stream.last_id = final_id;
        stream.entries.insert(final_id, fields);
        Self::apply_stream_trim(&mut stream, maxlen, minid);

        let entry = RudisEntry {
            key,
            val: RudisValue::Stream(stream),
            expire_at: None,
        };
        self.table.insert(entry);
        Ok(Some(final_id))
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

    pub fn xread(
        &mut self,
        keys: &[Bytes],
        ids: &[String],
        count: Option<usize>,
    ) -> Result<Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)>, &'static str> {
        if keys.len() != ids.len() {
            return Err("ERR Unbalanced XREAD list of streams and IDs");
        }
        let mut results = Vec::new();
        let limit = count.unwrap_or(usize::MAX);

        for (k, id_str) in keys.iter().zip(ids.iter()) {
            let h = hash_key(k);
            if let Some(idx) = self.table.find(k, h) {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    match &entry.val {
                        RudisValue::Stream(stream) => {
                            let lower_bound = if id_str == "$" {
                                std::ops::Bound::Excluded(stream.last_id)
                            } else if id_str == "+" {
                                std::ops::Bound::Excluded(StreamId::new(u64::MAX, u64::MAX))
                            } else {
                                let bound_id = if let Some((ms_s, seq_s)) = id_str.split_once('-') {
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

                            let mut entries = Vec::new();
                            for (id, fields) in stream
                                .entries
                                .range((lower_bound, std::ops::Bound::Unbounded))
                            {
                                entries.push((*id, fields.clone()));
                                if entries.len() >= limit {
                                    break;
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
    ) -> Result<usize, &'static str> {
        let h = hash_key(key);
        if let Some(idx) = self.table.find(key, h) {
            if self.check_expired_slot(idx) {
                return Ok(0);
            }
            if let Some(entry) = self.table.get_slot_mut(idx) {
                match &mut entry.val {
                    RudisValue::Stream(stream) => {
                        let trimmed = Self::apply_stream_trim(stream, maxlen, minid);
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

    // RDB SERIALIZATION & DUMP / RESTORE
    pub fn serialize_val_payload(val: &RudisValue, payload: &mut Vec<u8>) {
        match val {
            RudisValue::String(b) => {
                payload.push(0u8);
                payload.extend_from_slice(&(b.len() as u32).to_le_bytes());
                payload.extend_from_slice(b);
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
                for item in l {
                    payload.extend_from_slice(&(item.len() as u32).to_le_bytes());
                    payload.extend_from_slice(item);
                }
            }
            RudisValue::Set(s) => {
                payload.push(2u8);
                payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
                for item in s {
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
                for (f, v) in pairs {
                    payload.extend_from_slice(&(f.len() as u32).to_le_bytes());
                    payload.extend_from_slice(f);
                    payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    payload.extend_from_slice(v);
                }
            }
            RudisValue::Hash(h) => {
                payload.push(4u8);
                payload.extend_from_slice(&(h.len() as u32).to_le_bytes());
                for (f, v) in h {
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
            }
            RudisValue::Tiered(_) => {}
            RudisValue::Cooled { val, .. } => Self::serialize_val_payload(val, payload),
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
                let val = Bytes::copy_from_slice(&data[cursor..cursor + len]);
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
                let mut list = std::collections::VecDeque::with_capacity(count);
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
                RudisValue::List(list)
            }
            2 => {
                if cursor + 4 > data.len() {
                    return Err("DUMP payload version or checksum are wrong");
                }
                let count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let mut set = RudisSet::with_capacity(count);
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
                RudisValue::Set(set)
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
                    zset.insert(score, member);
                }
                RudisValue::ZSet(zset)
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

                        pairs.push((f, v));
                    }
                    RudisValue::SmallHash(pairs)
                } else {
                    let mut hash =
                        RudisHashMap::with_capacity_and_hasher(count, FxBuildHasher::default());
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
                    RudisValue::Hash(hash)
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
                    let mut fields = Vec::with_capacity(f_count);
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
                RudisValue::Stream(RudisStream {
                    entries,
                    last_id: StreamId::new(last_ms, last_seq),
                    groups: HashMap::new(),
                })
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
        // 2-byte RDB version: 10
        payload.extend_from_slice(&10u16.to_le_bytes());
        // 8-byte CRC64
        let crc = crc64(&payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        Some(payload)
    }

    pub fn restore(
        &mut self,
        key: Bytes,
        ttl_ms: u64,
        serialized: &[u8],
        replace: bool,
        absttl: bool,
    ) -> Result<(), &'static str> {
        if serialized.len() < 10 {
            return Err("DUMP payload version or checksum are wrong");
        }
        let data_len = serialized.len() - 8;
        let expected_crc = u64::from_le_bytes(
            serialized[data_len..]
                .try_into()
                .map_err(|_| "DUMP payload version or checksum are wrong")?,
        );
        let actual_crc = crc64(&serialized[..data_len]);
        if expected_crc != actual_crc {
            return Err("DUMP payload version or checksum are wrong");
        }

        let rdb_ver = u16::from_le_bytes(
            serialized[data_len - 2..data_len]
                .try_into()
                .map_err(|_| "DUMP payload version or checksum are wrong")?,
        );
        if rdb_ver > 15 {
            return Err("DUMP payload version or checksum are wrong");
        }

        if self.exists(&key) {
            if !replace {
                return Err("BUSYKEY Target key name already exists.");
            }
            self.del(&key);
        }

        let payload_len = data_len - 2;
        let (decoded_value, _) = Self::deserialize_val_payload(&serialized[..payload_len])?;

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
        let entry = RudisEntry {
            key,
            val: decoded_value,
            expire_at,
        };
        self.table.insert(entry);
        Ok(())
    }

    pub fn save_rdb_chunk(&mut self, buf: &mut Vec<u8>) {
        let now = Instant::now();
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for idx in 0..self.table.capacity() {
            if self.table.get_slot(idx).is_some() {
                if self.check_expired_slot(idx) {
                    continue;
                }
                if let Some(entry) = self.table.get_slot(idx) {
                    if let Some(exp) = entry.expire_at {
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
            let (val, consumed) = Self::deserialize_val_payload(data)?;
            data = &data[consumed..];

            self.del(&key);
            self.table.insert(RudisEntry {
                key,
                val,
                expire_at,
            });
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
    ) -> Result<(), &'static str> {
        let h = hash_key(&key);
        let idx_opt = self.table.find(&key, h);
        let stream_slot = idx_opt.filter(|&idx| !self.check_expired_slot(idx));

        if stream_slot.is_none() {
            if !mkstream {
                return Err("ERR The XGROUP subcommand requires the key to exist");
            }
            let stream = RudisStream::new();
            let entry = RudisEntry {
                key: key.clone(),
                val: RudisValue::Stream(stream),
                expire_at: None,
            };
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
                } else {
                    StreamId::parse(id_str)?
                };
                let grp = StreamGroup {
                    name: group.clone(),
                    last_delivered_id,
                    consumers: HashMap::new(),
                    pel: std::collections::BTreeMap::new(),
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
    ) -> Result<usize, &'static str> {
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
                            Ok(pending)
                        } else {
                            Ok(0)
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
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        let h = hash_key(key);
        let idx = match self.table.find(key, h) {
            Some(i) => {
                if self.check_expired_slot(i) {
                    return Ok(Vec::new());
                }
                i
            }
            None => return Ok(Vec::new()),
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let limit = count.unwrap_or(usize::MAX);

        let entry = self.table.get_slot_mut(idx).unwrap();
        match &mut entry.val {
            RudisValue::Stream(stream) => {
                let grp = stream
                    .groups
                    .get_mut(group)
                    .ok_or("NOGROUP No such key or consumer group")?;
                let cons =
                    grp.consumers
                        .entry(consumer.clone())
                        .or_insert_with(|| StreamConsumer {
                            name: consumer.clone(),
                            seen_time_ms: now,
                            pel: std::collections::BTreeMap::new(),
                        });
                cons.seen_time_ms = now;

                if id_str == ">" {
                    let mut results = Vec::new();
                    let range = stream.entries.range((
                        std::ops::Bound::Excluded(grp.last_delivered_id),
                        std::ops::Bound::Unbounded,
                    ));
                    for (&id, fields) in range {
                        if results.len() >= limit {
                            break;
                        }
                        results.push((id, fields.clone()));
                    }

                    for (id, _) in &results {
                        grp.last_delivered_id = std::cmp::max(grp.last_delivered_id, *id);
                        if !noack {
                            grp.pel.insert(
                                *id,
                                StreamPelEntry {
                                    consumer: consumer.clone(),
                                    delivery_time_ms: now,
                                    delivery_count: 1,
                                },
                            );
                            let cons = grp.consumers.get_mut(&consumer).unwrap();
                            cons.pel.insert(*id, now);
                        }
                    }
                    Ok(results)
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
                        if let Some(fields) = stream.entries.get(&id) {
                            results.push((id, fields.clone()));
                        }
                    }
                    for (id, _) in &results {
                        if let Some(pel_entry) = grp.pel.get_mut(id) {
                            pel_entry.delivery_time_ms = now;
                            pel_entry.delivery_count += 1;
                        }
                        if let Some(cons) = grp.consumers.get_mut(&consumer) {
                            cons.pel.insert(*id, now);
                        }
                    }
                    Ok(results)
                }
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
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
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<&[u8]>,
    ) -> Result<Vec<(StreamId, Bytes, u64, usize)>, &'static str> {
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
                for (&id, pel_entry) in grp.pel.range(start..=end) {
                    if let Some(c) = consumer
                        && pel_entry.consumer.as_ref() != c
                    {
                        continue;
                    }
                    let idle = now.saturating_sub(pel_entry.delivery_time_ms);
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
}

pub fn crc64(data: &[u8]) -> u64 {
    let mut crc: u64 = 0;
    for &b in data {
        crc ^= (b as u64) << 56;
        for _ in 0..8 {
            if (crc & 0x8000_0000_0000_0000) != 0 {
                crc = (crc << 1) ^ 0x42F0_E1EB_A9EA_3693;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
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
    if data.len() < 18 {
        return Ok(0);
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

    while cursor < content_len {
        let op = data[cursor];
        if op == 0xFF {
            // EOF
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
            if cursor + 40 > content_len {
                break;
            }
            let capacity =
                u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap()) as usize;
            let error_rate = f64::from_bits(u64::from_le_bytes(
                data[cursor + 8..cursor + 16].try_into().unwrap(),
            ));
            let num_bits =
                u64::from_le_bytes(data[cursor + 16..cursor + 24].try_into().unwrap()) as usize;
            let num_hashes =
                u32::from_le_bytes(data[cursor + 24..cursor + 28].try_into().unwrap()) as usize;
            let count_val =
                u64::from_le_bytes(data[cursor + 28..cursor + 36].try_into().unwrap()) as usize;
            let bits_len =
                u32::from_le_bytes(data[cursor + 36..cursor + 40].try_into().unwrap()) as usize;
            cursor += 40;
            if cursor + bits_len * 8 > content_len {
                break;
            }
            let mut bits = Vec::with_capacity(bits_len);
            for i in 0..bits_len {
                bits.push(u64::from_le_bytes(
                    data[cursor + i * 8..cursor + (i + 1) * 8]
                        .try_into()
                        .unwrap(),
                ));
            }
            cursor += bits_len * 8;
            if crate::router::target_shard(&key, num_shards) == shard_id {
                db.probabilistic_store.bloom_filters.insert(
                    key.clone(),
                    crate::probabilistic::BloomFilter {
                        capacity,
                        error_rate,
                        num_bits,
                        num_hashes,
                        count: count_val,
                        bits,
                    },
                );
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
            let metric = match metric_byte {
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
            if crate::router::target_shard(&doc_key, num_shards) == shard_id {
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
            if cursor + 28 > content_len {
                break;
            }
            let capacity =
                u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap()) as usize;
            let num_buckets =
                u64::from_le_bytes(data[cursor + 8..cursor + 16].try_into().unwrap()) as usize;
            let count_val =
                u64::from_le_bytes(data[cursor + 16..cursor + 24].try_into().unwrap()) as usize;
            let buckets_len =
                u32::from_le_bytes(data[cursor + 24..cursor + 28].try_into().unwrap()) as usize;
            cursor += 28;
            if cursor + buckets_len * 8 > content_len {
                break;
            }
            let mut buckets = Vec::with_capacity(buckets_len);
            for i in 0..buckets_len {
                let base = cursor + i * 8;
                let fp0 = u16::from_le_bytes(data[base..base + 2].try_into().unwrap());
                let fp1 = u16::from_le_bytes(data[base + 2..base + 4].try_into().unwrap());
                let fp2 = u16::from_le_bytes(data[base + 4..base + 6].try_into().unwrap());
                let fp3 = u16::from_le_bytes(data[base + 6..base + 8].try_into().unwrap());
                buckets.push([fp0, fp1, fp2, fp3]);
            }
            cursor += buckets_len * 8;
            if crate::router::target_shard(&key, num_shards) == shard_id {
                db.probabilistic_store.cuckoo_filters.insert(
                    key.clone(),
                    crate::probabilistic::CuckooFilter {
                        capacity,
                        num_buckets,
                        count: count_val,
                        buckets,
                    },
                );
                count += 1;
            }
            continue;
        } else if type_byte == 11 {
            cursor += 1;
            if cursor + 20 > content_len {
                break;
            }
            let width = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap()) as usize;
            let depth =
                u32::from_le_bytes(data[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
            let total_count =
                u64::from_le_bytes(data[cursor + 12..cursor + 20].try_into().unwrap());
            cursor += 20;
            let total_cells = width * depth;
            if cursor + total_cells * 8 > content_len {
                break;
            }
            let mut table = Vec::with_capacity(depth);
            for _ in 0..depth {
                let mut row = Vec::with_capacity(width);
                for _ in 0..width {
                    let cell = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    row.push(cell);
                }
                table.push(row);
            }
            if crate::router::target_shard(&key, num_shards) == shard_id {
                db.probabilistic_store.cms_sketches.insert(
                    key.clone(),
                    crate::probabilistic::CountMinSketch {
                        width,
                        depth,
                        total_count,
                        table,
                    },
                );
                count += 1;
            }
            continue;
        } else if type_byte == 12 {
            cursor += 1;
            if cursor + 12 > content_len {
                break;
            }
            let k = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap()) as usize;
            let items_len =
                u32::from_le_bytes(data[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
            cursor += 12;
            let mut items = hashbrown::HashMap::with_capacity(items_len);
            for _ in 0..items_len {
                if cursor + 4 > content_len {
                    break;
                }
                let item_len =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if cursor + item_len + 8 > content_len {
                    break;
                }
                let item_key = Bytes::copy_from_slice(&data[cursor..cursor + item_len]);
                cursor += item_len;
                let count_val = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                items.insert(item_key, count_val);
            }
            if crate::router::target_shard(&key, num_shards) == shard_id {
                db.probabilistic_store
                    .topk_trackers
                    .insert(key.clone(), crate::probabilistic::TopK { k, items });
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
            let _ = db.crdt_store.merge_sync_payload(payload);
            count += 1;
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
            db.table.table.insert(RudisEntry {
                key,
                val,
                expire_at,
            });
            count += 1;
        }
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_rudis_entry_size() {
        assert_eq!(std::mem::size_of::<RudisStream>(), 80);
        assert_eq!(std::mem::size_of::<RudisZSet>(), 64);
        assert_eq!(std::mem::size_of::<RudisValue>(), 88);
        assert_eq!(std::mem::size_of::<RudisEntry>(), 136);
        assert_eq!(std::mem::size_of::<Option<RudisEntry>>(), 136);
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
        assert!(table.expire(b"k2", Duration::from_millis(10)));
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
            let entry = RudisEntry {
                key,
                val: RudisValue::String(val),
                expire_at: None,
            };
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
        let (next_cursor, scanned) = table.scan(0, Some(b"alpha:*"), 10);
        assert_eq!(next_cursor, 0); // Scanned whole small table
        assert_eq!(scanned.len(), 2);

        // EXPIRETIME
        assert_eq!(table.expiretime(b"non_exist", false), -2);
        assert_eq!(table.expiretime(b"alpha:1", false), -1);
        table.expire(b"alpha:1", Duration::from_secs(50));
        let exp = table.expiretime(b"alpha:1", false);
        assert!(exp > 0);
    }

    #[test]
    fn test_rudis_table_bitmaps_and_hll() {
        let mut table = RudisTable::new();

        // 1. SETBIT & GETBIT
        // 'a' in ASCII is 0b01100001 (byte 0: bit 1, 2, 7 are 1)
        assert_eq!(table.setbit(Bytes::from_static(b"bm"), 1, 1).unwrap(), 0);
        assert_eq!(table.setbit(Bytes::from_static(b"bm"), 2, 1).unwrap(), 0);
        assert_eq!(table.setbit(Bytes::from_static(b"bm"), 7, 1).unwrap(), 0);
        assert_eq!(table.getbit(b"bm", 1).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 2).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 3).unwrap(), 0);
        assert_eq!(table.getbit(b"bm", 7).unwrap(), 1);
        assert_eq!(table.getbit(b"bm", 100).unwrap(), 0);
        assert_eq!(table.get(b"bm").unwrap(), Some(Bytes::from_static(b"a")));

        // 2. BITCOUNT
        assert_eq!(table.bitcount(b"bm", None, None).unwrap(), 3);
        // Set bit in byte 1 (offset 15 = bit 7 of byte 1)
        assert_eq!(table.setbit(Bytes::from_static(b"bm"), 15, 1).unwrap(), 0);
        assert_eq!(table.bitcount(b"bm", None, None).unwrap(), 4);
        assert_eq!(table.bitcount(b"bm", Some(0), Some(0)).unwrap(), 3);
        assert_eq!(table.bitcount(b"bm", Some(1), Some(1)).unwrap(), 1);

        // 3. BITPOS
        assert_eq!(table.bitpos(b"bm", 1, None, None).unwrap(), 1);
        assert_eq!(table.bitpos(b"bm", 0, None, None).unwrap(), 0);
        assert_eq!(table.bitpos(b"bm", 1, Some(1), None).unwrap(), 15);

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
            )
            .unwrap();
        assert_eq!(res_nomk, None);

        // 3. XADD with explicit ID
        let id1 = table
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
            )
            .unwrap()
            .unwrap();
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
        );
        assert_eq!(
            err_mono,
            Err("ERR The ID specified in XADD is equal or smaller than the target stream top item")
        );

        // 4. AutoSeq
        let id2 = table
            .xadd(
                Bytes::from_static(b"s1"),
                StreamAddId::AutoSeq(1000),
                vec![(Bytes::from_static(b"val"), Bytes::from_static(b"26"))],
                false,
                None,
                None,
            )
            .unwrap()
            .unwrap();
        assert_eq!(id2, StreamId::new(1000, 2));

        let id3 = table
            .xadd(
                Bytes::from_static(b"s1"),
                StreamAddId::AutoSeq(1001),
                vec![(Bytes::from_static(b"val"), Bytes::from_static(b"27"))],
                false,
                None,
                None,
            )
            .unwrap()
            .unwrap();
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
            .xread(&[Bytes::from_static(b"s1")], &[id1.to_string()], None)
            .unwrap();
        assert_eq!(read_res.len(), 1);
        assert_eq!(read_res[0].1.len(), 2);
        assert_eq!(read_res[0].1[0].0, id2);
        assert_eq!(read_res[0].1[1].0, id3);

        // XREAD with $
        let read_dollar = table
            .xread(&[Bytes::from_static(b"s1")], &[String::from("$")], None)
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
                )
                .unwrap();
        }
        assert_eq!(table.xlen(b"s1").unwrap(), 12);
        let trimmed = table.xtrim(b"s1", Some(5), None).unwrap();
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
        let initial_mem = table.used_memory;

        // 1. Hot key
        table.set(
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"hello_tiered_storage_world"),
            None,
        );
        assert!(table.used_memory > initial_mem);
        let hot_mem = table.used_memory;
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
            RudisValue::String(Bytes::from_static(b"hello_tiered_storage_world"))
        );

        // 3. Instant Zero-I/O Decommit: Cooled -> Cold (Tiered)
        let (p, freed) = table.decommit_cooled_key(b"k1").unwrap();
        assert_eq!(p.offset, 4096);
        assert!(freed > 0);
        assert!(table.used_memory < hot_mem);
        assert!(table.is_cooled(b"k1").is_none());
        assert!(table.is_tiered(b"k1").is_some());

        // 4. Restore Cold -> Cooled (Read hit)
        let restored_val = RudisValue::String(Bytes::from_static(b"hello_tiered_storage_world"));
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
        assert_eq!(RudisTable::parse_i64_bytes(b"+42"), Some(42));
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
        assert_eq!(out, b"*0\r\n");

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
        assert!(!table.table.ctrl.contains(&DELETED));

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
        assert!(!table.table.ctrl.contains(&DELETED));
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
}
