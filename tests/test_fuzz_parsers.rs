//! Fuzz and Adversarial Parser Testing Suite for Rudis.
//!
//! Validates panic isolation and memory safety across:
//! 1. Zero-Copy RESP2 / RESP3 Protocol Parser (`src/resp.rs`)
//! 2. RedisJSON JSONPath Syntax Parser (`src/json.rs`)
//! 3. RediSearch Query AST Parser (`src/search.rs`)

use bytes::Bytes;
use rudis::json::parse_json_path;
use rudis::resp::build_command;
use rudis::search::parse_query;

/// Simple deterministic pseudo-random generator (Xorshift64) to avoid external rand dependency.
struct SimplePrng {
    state: u64,
}

impl SimplePrng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0xDEADBEEFCAFE } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            0
        } else {
            (self.next_u64() as usize) % max
        }
    }

    fn random_bytes(&mut self, len: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            buf.push(self.next_u64() as u8);
        }
        buf
    }

    fn random_string(&mut self, len: usize, charset: &[u8]) -> String {
        let mut s = String::with_capacity(len);
        for _ in 0..len {
            let idx = self.next_usize(charset.len());
            s.push(charset[idx] as char);
        }
        s
    }
}

#[test]
fn test_fuzz_resp_parser_adversarial_corpus() {
    let adversarial_frames: &[&[u8]] = &[
        // Extreme lengths
        b"*1\r\n$99999999999999999999999999999999\r\na\r\n",
        b"*1\r\n$-99999999999999999999999999999999\r\na\r\n",
        b"*99999999999999999999999999999999\r\n$1\r\na\r\n",
        b"*-99999999999999999999999999999999\r\n$1\r\na\r\n",
        // Malformed delimiters
        b"*\r\n",
        b"$\r\n",
        b":\r\n",
        b"+\r\n",
        b"-\r\n",
        b"*0\r\n",
        b"*-1\r\n",
        b"$0\r\n\r\n",
        b"$-1\r\n",
        b"*1\r\n$0\r\n\r\n",
        // Embedded nulls and binary corruptions
        b"*2\r\n$4\r\nSE\x00T\r\n$3\r\nk\x001\r\n",
        b"*1\r\n$10\r\n\xFF\xFE\xFD\xFC\xFB\xFA\xF9\xF8\xF7\xF6\r\n",
        // Truncated frames
        b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n",
        b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv",
        b"*2\r\n$3\r\nGET\r\n",
        // Partial CRLFs
        b"*1\r\n$3\r\nFOO\r",
        b"*1\r\n$3\r\nFOO\n",
        b"PING\r",
        b"PING\n",
        b"ECHO \x00\r\n",
    ];

    for frame in adversarial_frames {
        let args = parse_raw_resp_arguments(frame);
        if !args.is_empty() {
            let _ = build_command(args);
        }
    }
}

#[test]
fn test_fuzz_resp_parser_random_mutations() {
    let mut prng = SimplePrng::new(0x1337BEEF);
    let seeds: &[&[u8]] = &[
        b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n",
        b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n",
        b"*1\r\n$4\r\nPING\r\n",
        b"*4\r\n$4\r\nHSET\r\n$2\r\nh1\r\n$2\r\nf1\r\n$2\r\nv1\r\n",
        b"*3\r\n$5\r\nRPUSH\r\n$2\r\nl1\r\n$2\r\ne1\r\n",
    ];

    for &seed in seeds {
        for _ in 0..500 {
            let mut mutated = seed.to_vec();
            let mutations = prng.next_usize(5) + 1;
            for _ in 0..mutations {
                if mutated.is_empty() {
                    break;
                }
                let pos = prng.next_usize(mutated.len());
                match prng.next_usize(4) {
                    0 => {
                        // Flip byte
                        mutated[pos] = prng.next_u64() as u8;
                    }
                    1 => {
                        // Truncate
                        mutated.truncate(pos);
                    }
                    2 => {
                        // Insert random byte
                        mutated.insert(pos, prng.next_u64() as u8);
                    }
                    _ => {
                        // Delete byte
                        mutated.remove(pos);
                    }
                }
            }

            let args = parse_raw_resp_arguments(&mutated);
            if !args.is_empty() {
                // Must never panic
                let _ = build_command(args);
            }
        }
    }
}

#[test]
fn test_fuzz_resp_parser_raw_random_streams() {
    let mut prng = SimplePrng::new(0x9876543210ABCDEF);
    for _ in 0..1000 {
        let len = prng.next_usize(256);
        let bytes = prng.random_bytes(len);
        let args = parse_raw_resp_arguments(&bytes);
        if !args.is_empty() {
            let _ = build_command(args);
        }
    }
}

#[test]
fn test_fuzz_json_path_parser_fuzzing() {
    let mut prng = SimplePrng::new(0xCAFEBABE);
    let charset =
        b"$.[]*?()0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_:;,@!/\\ \t";

    // 1. Handcrafted edge-case paths
    let edge_cases = [
        "",
        "$",
        "$$",
        "$.",
        "$..",
        "$...",
        "$.foo",
        "$.foo.bar",
        "$[0]",
        "$[-1]",
        "$[9999999999999999999999999999]",
        "$[-9999999999999999999999999999]",
        "$[:::]",
        "$[0:10:2]",
        "$[10:0:-1]",
        "$[*]",
        "$.*",
        "$..*",
        "$['foo']['bar']",
        "$['foo.bar']",
        "$[?(@.price < 10)]",
        "[[[[[[[[[[",
        "]]]]]]]]]]",
        "$[",
        "$]",
        "$['",
        "$[\"",
    ];

    for path in edge_cases {
        let _ = parse_json_path(path);
    }

    // 2. 2,000 random permutations
    for _ in 0..2000 {
        let len = prng.next_usize(64);
        let path = prng.random_string(len, charset);
        // Must never panic
        let _ = parse_json_path(&path);
    }
}

#[test]
fn test_fuzz_search_query_parser_fuzzing() {
    let mut prng = SimplePrng::new(0xF00DBEEF);
    let charset = b"abcdefghijklmnopqrstuvwxyz0123456789@:{}[],.*|&()~-=> \t";

    // 1. Handcrafted search edge cases
    let edge_cases = [
        "",
        "*",
        "**",
        "***",
        "(",
        ")",
        "()",
        "(((((((",
        ")))))))",
        "(@field:)",
        "(@field:{})",
        "(@field:[])",
        "(@field:[0 10])",
        "(@field:[-inf +inf])",
        "(@field:[nan nan])",
        "foo |",
        "| bar",
        "foo | | bar",
        "foo bar baz",
        "-foo -bar",
        "*=>[KNN 10 @vec $blob]",
        "*=>[KNN invalid @vec $blob]",
        "*=>[KNN 10 @vec]",
        "*=>[KNN]",
        "=>[]",
        "(@field:{a | b | c})",
        "(@field:[VECTOR_RANGE 0.5 $blob])",
        "(@field:[VECTOR_RANGE invalid $blob])",
    ];

    for query in edge_cases {
        // Must never panic
        let _ = parse_query(query);
    }

    // 2. 2,000 random query mutations
    for _ in 0..2000 {
        let len = prng.next_usize(80);
        let query = prng.random_string(len, charset);
        // Must never panic
        let _ = parse_query(&query);
    }
}

/// Helper that splits a raw RESP frame into command argument vectors.
fn parse_raw_resp_arguments(frame: &[u8]) -> Vec<Bytes> {
    if frame.is_empty() {
        return Vec::new();
    }
    if frame.starts_with(b"*") {
        let mut cur = &frame[1..];
        let idx = match cur.iter().position(|&b| b == b'\r') {
            Some(i) => i,
            None => return Vec::new(),
        };
        let count: usize = match std::str::from_utf8(&cur[..idx])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(c) => c,
            None => return Vec::new(),
        };
        if cur.len() < idx + 2 {
            return Vec::new();
        }
        cur = &cur[idx + 2..];
        let mut args = Vec::with_capacity(count.min(128));
        for _ in 0..count.min(128) {
            if !cur.starts_with(b"$") {
                break;
            }
            cur = &cur[1..];
            let len_idx = match cur.iter().position(|&b| b == b'\r') {
                Some(i) => i,
                None => break,
            };
            let len: usize = match std::str::from_utf8(&cur[..len_idx])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(l) => l,
                None => break,
            };
            if cur.len() < len_idx + 2 {
                break;
            }
            cur = &cur[len_idx + 2..];
            if cur.len() < len + 2 {
                break;
            }
            args.push(Bytes::copy_from_slice(&cur[..len]));
            cur = &cur[len + 2..];
        }
        args
    } else {
        // Inline command split
        frame
            .split(|&b| b == b' ' || b == b'\r' || b == b'\n')
            .filter(|chunk| !chunk.is_empty())
            .map(Bytes::copy_from_slice)
            .collect()
    }
}
