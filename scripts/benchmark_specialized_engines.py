#!/usr/bin/env python3
"""
Rudis Specialized Engines Benchmark Suite
Evaluates throughput and latency percentiles across:
- RedisJSON (JSON.SET, JSON.GET)
- Streams (XADD, XRANGE)
- Probabilistic Structures (BF.ADD, BF.EXISTS, CF.ADD, CF.EXISTS, CMS.INCRBY, CMS.QUERY, TOPK.ADD, TOPK.QUERY)
- Geospatial (GEOADD, GEODIST)
- Vector Search (VADD, VQUERY)
- Transactions (MULTI/EXEC)
"""

import subprocess
import time
import os
import sys
import json

SERVER_BIN = "/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"
PORT = 6379
SERVER_THREADS = 16
SERVER_CORES = f"0-{SERVER_THREADS - 1}"
CLIENT_CORES = "32-63"
DURATION = 5
PIPELINE = 50

os.makedirs("/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs", exist_ok=True)

def kill_server():
    subprocess.run(["pkill", "-9", "-x", "rudis"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)

def parse_memtier_output(output, metric_key="Totals"):
    # Look for the target row or Totals
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 10:
            if parts[0].lower() == metric_key.lower() or parts[0] == "Totals":
                try:
                    return {
                        "ops_sec": float(parts[1]),
                        "avg_lat": float(parts[4]),
                        "p50": float(parts[5]),
                        "p90": float(parts[6]),
                        "p95": float(parts[7]),
                        "p99": float(parts[8]),
                        "p999": float(parts[9]),
                        "kb_sec": float(parts[10]),
                    }
                except ValueError:
                    continue
    return None

def run_test(name, memtier_cmd_args, warmup_args=None, metric_key="Totals"):
    kill_server()
    server_cmd = ["taskset", "-c", SERVER_CORES, SERVER_BIN, "--threads", str(SERVER_THREADS), "--port", str(PORT)]
    server_proc = subprocess.Popen(server_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.2)

    if warmup_args:
        w_cmd = [
            "taskset", "-c", CLIENT_CORES, MEMTIER_BIN,
            "--server", "127.0.0.1", "--port", str(PORT),
            "--clients", "1", "--threads", "16",
            "--pipeline", str(PIPELINE),
            "--test-time", "2", "--hide-histogram"
        ] + warmup_args
        subprocess.run(w_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    base_cmd = [
        "taskset", "-c", CLIENT_CORES, MEMTIER_BIN,
        "--server", "127.0.0.1", "--port", str(PORT),
        "--clients", "1", "--threads", "32",
        "--pipeline", str(PIPELINE),
        "--print-percentiles", "50,90,95,99,99.9",
        "--test-time", str(DURATION),
        "--hide-histogram"
    ]
    full_cmd = base_cmd + memtier_cmd_args

    try:
        res = subprocess.run(full_cmd, capture_output=True, text=True, timeout=DURATION + 20)
        output = res.stdout
    except Exception as e:
        print(f"Error: {e}", flush=True)
        output = ""

    server_proc.terminate()
    try:
        server_proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        server_proc.kill()
    time.sleep(0.3)

    return parse_memtier_output(output, metric_key)

def main():
    print("================================================================================")
    print("              RUDIS SPECIALIZED ENGINES BENCHMARK SUITE                         ")
    print("================================================================================")
    print(f"Config: 16 Server Threads, Pipeline {PIPELINE}, 32 Client Threads, Duration {DURATION}s\n")

    test_groups = [
        {
            "category": "RedisJSON Engine",
            "tests": [
                {
                    "name": "JSON.SET",
                    "cmd": ["--command=JSON.SET json:__key__ $ [1,2,3,4,5]", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=200000"],
                    "warmup": None,
                    "key": "Json.sets"
                },
                {
                    "name": "JSON.GET",
                    "cmd": ["--command=JSON.GET json:__key__ $", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=200000"],
                    "warmup": ["--command=JSON.SET json:__key__ $ [1,2,3,4,5]", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=200000"],
                    "key": "Json.gets"
                }
            ]
        },
        {
            "category": "Streams Engine",
            "tests": [
                {
                    "name": "XADD",
                    "cmd": ["--command=XADD stream:__key__ * sensor temp val 25", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "warmup": None,
                    "key": "Xadds"
                },
                {
                    "name": "XRANGE",
                    "cmd": ["--command=XRANGE stream:__key__ - + COUNT 10", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "warmup": ["--command=XADD stream:__key__ * sensor temp val 25", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "key": "Xranges"
                }
            ]
        },
        {
            "category": "Probabilistic Engine (RedisBloom)",
            "tests": [
                {
                    "name": "BF.ADD",
                    "cmd": ["--command=BF.ADD bf:__key__ item__key__", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "warmup": None,
                    "key": "Bf.adds"
                },
                {
                    "name": "BF.EXISTS",
                    "cmd": ["--command=BF.EXISTS bf:__key__ item__key__", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "warmup": ["--command=BF.ADD bf:__key__ item__key__", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "key": "Bf.exists"
                },
                {
                    "name": "CF.ADD",
                    "cmd": ["--command=CF.ADD cf:__key__ item__key__", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "warmup": None,
                    "key": "Cf.adds"
                },
                {
                    "name": "CF.EXISTS",
                    "cmd": ["--command=CF.EXISTS cf:__key__ item__key__", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "warmup": ["--command=CF.ADD cf:__key__ item__key__", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "key": "Cf.exists"
                },
                {
                    "name": "CMS.INCRBY",
                    "cmd": ["--command=CMS.INCRBY cms:__key__ item 1", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "warmup": None,
                    "key": "Cms.incrbys"
                },
                {
                    "name": "CMS.QUERY",
                    "cmd": ["--command=CMS.QUERY cms:__key__ item", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "warmup": ["--command=CMS.INCRBY cms:__key__ item 1", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "key": "Cms.querys"
                },
                {
                    "name": "TOPK.ADD",
                    "cmd": ["--command=TOPK.ADD topk:__key__ alpha", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "warmup": None,
                    "key": "Topk.adds"
                },
                {
                    "name": "TOPK.QUERY",
                    "cmd": ["--command=TOPK.QUERY topk:__key__ alpha", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "warmup": ["--command=TOPK.ADD topk:__key__ alpha", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=100000"],
                    "key": "Topk.querys"
                }
            ]
        },
        {
            "category": "Geospatial Engine",
            "tests": [
                {
                    "name": "GEOADD",
                    "cmd": ["--command=GEOADD geo:__key__ 13.361389 38.115556 Palermo 15.087269 37.502669 Catania", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "warmup": None,
                    "key": "Geoadds"
                },
                {
                    "name": "GEODIST",
                    "cmd": ["--command=GEODIST geo:__key__ Palermo Catania km", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "warmup": ["--command=GEOADD geo:__key__ 13.361389 38.115556 Palermo 15.087269 37.502669 Catania", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "key": "Geodists"
                }
            ]
        },
        {
            "category": "Vector Search Engine (HNSW)",
            "tests": [
                {
                    "name": "VADD",
                    "cmd": ["--command=VADD vec:__key__ doc1 1.0 0.0 0.5 0.2", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "warmup": None,
                    "key": "Vadds"
                },
                {
                    "name": "VQUERY",
                    "cmd": ["--command=VQUERY vec:__key__ 5 1.0 0.0 0.5 0.2", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "warmup": ["--command=VADD vec:__key__ doc1 1.0 0.0 0.5 0.2", "--command-ratio=1", "--command-key-pattern=R", "--key-minimum=1", "--key-maximum=50000"],
                    "key": "Vquerys"
                }
            ]
        },
        {
            "category": "Transactions (MULTI/EXEC)",
            "tests": [
                {
                    "name": "MULTI/EXEC Batch",
                    "cmd": [
                        "--command=MULTI", "--command-ratio=1",
                        "--command=SET tx:__key__ data_val", "--command-ratio=5",
                        "--command=EXEC", "--command-ratio=1",
                        "--key-minimum=1", "--key-maximum=100000"
                    ],
                    "warmup": None,
                    "key": "Totals"
                }
            ]
        }
    ]

    all_results = {}

    for group in test_groups:
        cat = group["category"]
        print(f"--- Group: {cat} ---")
        all_results[cat] = []
        for t in group["tests"]:
            t_name = t["name"]
            stats = run_test(t_name, t["cmd"], warmup_args=t["warmup"], metric_key=t["key"])
            if stats:
                stats["name"] = t_name
                all_results[cat].append(stats)
                mb_s = stats["kb_sec"] / 1024.0
                print(f"  {t_name:<16} -> {stats['ops_sec']:>10,.1f} Ops/s | {mb_s:>7,.1f} MB/s | Avg: {stats['avg_lat']:>5.2f}ms | p50: {stats['p50']:>5.2f}ms | p99: {stats['p99']:>5.2f}ms | p99.9: {stats['p999']:>5.2f}ms", flush=True)
            else:
                print(f"  {t_name:<16} -> FAILED to parse output", flush=True)
        print()

    out_file = "/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs/specialized_engines_results.json"
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"Saved raw JSON results to: {out_file}")

if __name__ == "__main__":
    main()
