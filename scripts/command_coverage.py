#!/usr/bin/env python3
"""
Rudis Command & Test Coverage Analyzer
Scans Rudis command definitions and evaluates test coverage across
integration tests (tests/) and unit tests (src/).
"""

import os
import re
import sys
import glob
from collections import defaultdict

CATEGORIES = {
    "Strings & Basic Keyspace": [
        "GET", "SET", "PUT", "MGET", "MSET", "MSETEX", "SETNX", "MSETNX", "GETSET",
        "GETDEL", "APPEND", "STRLEN", "SETRANGE", "GETRANGE", "INCR", "DECR",
        "INCRBY", "DECRBY", "INCRBYFLOAT", "DEL", "EXISTS", "SETEX", "PSETEX"
    ],
    "Hashes": [
        "HSET", "HMSET", "HGET", "HMGET", "HDEL", "HEXISTS", "HLEN", "HGETALL",
        "HKEYS", "HVALS", "HINCRBY", "HINCRBYFLOAT", "HRANDFIELD", "HSCAN"
    ],
    "Lists": [
        "LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN", "LINDEX", "LTRIM",
        "LSET", "LREM", "LPOS", "LINSERT", "LMOVE", "BLMOVE", "BLPOP", "BRPOP"
    ],
    "Sets": [
        "SADD", "SREM", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD", "SPOP",
        "SRANDMEMBER", "SMOVE", "SSCAN", "SINTER", "SUNION", "SDIFF",
        "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE"
    ],
    "Sorted Sets (ZSets)": [
        "ZADD", "ZREM", "ZSCORE", "ZMSCORE", "ZCARD", "ZRANK", "ZREVRANK",
        "ZCOUNT", "ZLEXCOUNT", "ZINCRBY", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE",
        "ZREVRANGEBYSCORE", "ZPOPMIN", "ZPOPMAX", "ZRANDMEMBER", "ZREMRANGEBYRANK",
        "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX", "ZSCAN", "ZINTER", "ZUNION",
        "ZDIFF", "ZINTERSTORE", "ZUNIONSTORE", "ZDIFFSTORE"
    ],
    "Bitmaps & Bitfields": [
        "SETBIT", "GETBIT", "BITCOUNT", "BITPOS", "BITOP"
    ],
    "HyperLogLog": [
        "PFADD", "PFCOUNT", "PFMERGE"
    ],
    "Geospatial": [
        "GEOADD", "GEODIST", "GEOPOS", "GEOHASH", "GEORADIUS",
        "GEORADIUSBYMEMBER", "GEOSEARCH"
    ],
    "Streams & Consumer Groups": [
        "XADD", "XLEN", "XRANGE", "XREVRANGE", "XDEL", "XTRIM", "XREAD",
        "XGROUP", "XREADGROUP", "XACK", "XPENDING"
    ],
    "Transactions": [
        "MULTI", "EXEC", "DISCARD"
    ],
    "Pub/Sub": [
        "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH", "PUBSUB"
    ],
    "Scripting & Functions": [
        "EVAL", "EVALSHA", "SCRIPT", "FUNCTION", "FCALL"
    ],
    "ACL & Security": [
        "AUTH", "ACL"
    ],
    "Server, Keyspace & Client": [
        "PING", "ECHO", "TIME", "RESET", "DBSIZE", "FLUSHDB", "FLUSHALL",
        "KEYS", "SCAN", "RANDOMKEY", "TYPE", "EXPIRE", "PEXPIRE", "EXPIREAT",
        "PEXPIREAT", "EXPIRETIME", "PEXPIRETIME", "PERSIST", "TTL", "PTTL",
        "TOUCH", "RENAME", "RENAMENX", "DUMP", "RESTORE", "INFO", "COMMAND",
        "CONFIG", "HELLO", "CLIENT", "QUIT", "MEMORY"
    ],
    "Persistence & Replication": [
        "SAVE", "BGSAVE", "LASTSAVE", "REPLICAOF", "PSYNC", "REPLCONF", "ROLE"
    ],
    "Cluster & Gossip": [
        "CLUSTER", "ASKING", "MIGRATE"
    ],
    "Dragonfly Parity & Memcached": [
        "DFLYCLUSTER", "DFLYMIGRATE", "STICK", "UNSTICK", "STICKY", "DELEX",
        "STATS", "VERSION"
    ],
    "RedisJSON": [
        "JSON.SET", "JSON.GET", "JSON.DEL", "JSON.TYPE", "JSON.NUMINCRBY",
        "JSON.NUMMULTBY", "JSON.STRAPPEND", "JSON.STRLEN", "JSON.ARRAPPEND",
        "JSON.ARRLEN", "JSON.ARRPOP", "JSON.OBJKEYS", "JSON.OBJLEN",
        "JSON.TOGGLE", "JSON.CLEAR", "JSON.MGET"
    ],
    "RediSearch & Hybrid Fusion": [
        "FT.CREATE", "FT.SEARCH", "FT.INFO", "FT.DROPINDEX", "FT.EXPLAIN", "FT.ADD"
    ],
    "RedisBloom Probabilistic": [
        "BF.RESERVE", "BF.ADD", "BF.MADD", "BF.EXISTS", "BF.MEXISTS", "BF.INFO",
        "CF.RESERVE", "CF.ADD", "CF.ADDNX", "CF.EXISTS", "CF.DEL", "CF.INFO",
        "CMS.INITBYDIM", "CMS.INITBYPROB", "CMS.INCRBY", "CMS.QUERY", "CMS.INFO",
        "TOPK.RESERVE", "TOPK.ADD", "TOPK.QUERY", "TOPK.LIST", "TOPK.INFO"
    ],
    "Vector Search (HNSW)": [
        "VADD", "VQUERY", "VSIM", "VDEL", "VINFO"
    ],
    "NVMe Tiered Storage": [
        "TIER"
    ],
    "Multi-Region CRDTs": [
        "CRDT.SET", "CRDT.GET", "CRDT.DEL", "CRDT.INCRBY", "CRDT.SADD",
        "CRDT.SMEMBERS", "CRDT.SREM", "CRDT.DUMP", "CRDT.MERGE", "CRDT.GC"
    ],
    "AF_XDP & eBPF Networking": [
        "XDP.INFO", "XDP.RULE", "XDP.STATS", "XDP.PACKET"
    ],
}

def extract_defined_commands(resp_path):
    with open(resp_path) as f:
        lines = f.readlines()
    start = False
    top_level_cmds = []
    for line in lines:
        if 'pub fn build_command(' in line:
            start = True
            continue
        if start and 'fn parse_memcached_storage_command' in line:
            break
        if start:
            if line.startswith('        "') and '" =>' in line:
                cmd = line.strip().split('"')[1]
                top_level_cmds.append(cmd)
    return sorted(list(set(top_level_cmds)))

def is_command_tested(cmd, corpus):
    cmd_lower = cmd.lower()
    patterns = [
        rf'\b{re.escape(cmd)}\b',
        rf'\b{re.escape(cmd_lower)}\b',
        rf'\${len(cmd)}\\r\\n{re.escape(cmd_lower)}\\r\\n',
        rf'\${len(cmd)}\\r\\n{re.escape(cmd)}\\r\\n',
    ]
    return any(re.search(p, corpus, re.IGNORECASE) for p in patterns)

def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    resp_rs = os.path.join(repo_root, "src", "resp.rs")
    
    defined_cmds = extract_defined_commands(resp_rs)
    
    # Load E2E integration test source
    e2e_files = glob.glob(os.path.join(repo_root, "tests", "*.rs"))
    e2e_corpus = ""
    for f in e2e_files:
        with open(f) as fh:
            e2e_corpus += "\n" + fh.read()
            
    # Load Unit test source in src/
    unit_files = glob.glob(os.path.join(repo_root, "src", "*.rs"))
    unit_corpus = ""
    for f in unit_files:
        with open(f) as fh:
            unit_corpus += "\n" + fh.read()
            
    total_corpus = e2e_corpus + "\n" + unit_corpus

    print("================================================================================")
    print("                      RUDIS COMMAND COVERAGE REPORT                             ")
    print("================================================================================\n")
    
    total_defined = len(defined_cmds)
    total_e2e_covered = 0
    total_all_covered = 0

    print(f"{'Category':<32} | {'Commands':<10} | {'E2E Covered':<12} | {'Overall':<10}")
    print("-" * 75)

    covered_set = set()

    for cat_name, cmd_list in CATEGORIES.items():
        valid_cmds = [c for c in cmd_list if c in defined_cmds]
        e2e_count = sum(1 for c in valid_cmds if is_command_tested(c, e2e_corpus))
        all_count = sum(1 for c in valid_cmds if is_command_tested(c, total_corpus))
        
        for c in valid_cmds:
            covered_set.add(c)
            
        e2e_pct = (e2e_count / len(valid_cmds) * 100) if valid_cmds else 100.0
        all_pct = (all_count / len(valid_cmds) * 100) if valid_cmds else 100.0
        
        print(f"{cat_name:<32} | {len(valid_cmds):<10} | {e2e_count}/{len(valid_cmds)} ({e2e_pct:4.1f}%) | {all_count}/{len(valid_cmds)} ({all_pct:4.1f}%)")

    # Check for unmapped commands
    unmapped = [c for c in defined_cmds if c not in covered_set]
    if unmapped:
        e2e_count = sum(1 for c in unmapped if is_command_tested(c, e2e_corpus))
        all_count = sum(1 for c in unmapped if is_command_tested(c, total_corpus))
        e2e_pct = (e2e_count / len(unmapped) * 100)
        all_pct = (all_count / len(unmapped) * 100)
        print(f"{'Other Extended Commands':<32} | {len(unmapped):<10} | {e2e_count}/{len(unmapped)} ({e2e_pct:4.1f}%) | {all_count}/{len(unmapped)} ({all_pct:4.1f}%)")

    # Global summary
    all_e2e_hits = sum(1 for c in defined_cmds if is_command_tested(c, e2e_corpus))
    all_total_hits = sum(1 for c in defined_cmds if is_command_tested(c, total_corpus))
    
    print("=" * 75)
    print(f"Total Defined Commands in Rudis:    {total_defined}")
    print(f"End-to-End Test (E2E) Coverage:    {all_e2e_hits} / {total_defined} ({all_e2e_hits/total_defined*100:.1f}%)")
    print(f"Overall (E2E + Unit Tests):        {all_total_hits} / {total_defined} ({all_total_hits/total_defined*100:.1f}%)")
    print("================================================================================\n")

    if "--uncovered" in sys.argv or "--details" in sys.argv:
        e2e_uncovered = [c for c in defined_cmds if not is_command_tested(c, e2e_corpus)]
        if e2e_uncovered:
            print("Commands lacking End-to-End (E2E TCP) integration tests:")
            for cmd in sorted(e2e_uncovered):
                print(f"  - {cmd}")
            print()


if __name__ == "__main__":
    main()
