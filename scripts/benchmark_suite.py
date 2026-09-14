#!/usr/bin/env python3
import subprocess
import time
import os
import sys
import json

MEMTIER = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"
VALKEY_CLI = "/usr/local/google/home/warrenzhu/valkey/src/valkey-cli"

ENGINES = {
    "Rudis": {
        "cmd": ["taskset", "-c", "0-15", "./target/release/rudis", "--threads", "16", "--port", "6379"],
        "cwd": "/usr/local/google/home/warrenzhu/github/rudis",
    },
    "Dragonfly": {
        "cmd": ["taskset", "-c", "0-15", "/usr/local/google/home/warrenzhu/dragonfly", "--proactor_threads=16", "--port", "6379"],
        "cwd": "/usr/local/google/home/warrenzhu",
    },
}

COMMAND_TESTS = [
    {
        "name": "SET (1KB)",
        "warmup": None,
        "memtier_args": [
            "--ratio", "1:0", "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--key-pattern", "S:S",
        ],
        "type_prefix": "Sets",
    },
    {
        "name": "GET (1KB)",
        "warmup": [
            "--ratio", "1:0", "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--key-pattern", "S:S", "--test-time", "3", "--hide-histogram",
        ],
        "memtier_args": [
            "--ratio", "0:1", "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--key-pattern", "S:S",
        ],
        "type_prefix": "Gets",
    },
    {
        "name": "SET/GET 1:1 (1KB)",
        "warmup": [
            "--ratio", "1:0", "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--key-pattern", "S:S", "--test-time", "3", "--hide-histogram",
        ],
        "memtier_args": [
            "--ratio", "1:1", "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--key-pattern", "S:S",
        ],
        "type_prefix": "Totals",
    },
    {
        "name": "LPUSH (1KB)",
        "warmup": None,
        "memtier_args": [
            '--command=LPUSH __key__ __data__', '--command-ratio=1', '--command-key-pattern=S',
            "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Lpushs",
    },
    {
        "name": "LRANGE",
        "warmup": [
            '--command=LPUSH __key__ __data__', '--command-ratio=1', '--command-key-pattern=S',
            "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--test-time", "3", "--hide-histogram",
        ],
        "memtier_args": [
            '--command=LRANGE __key__ 0 10', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Lranges",
    },
    {
        "name": "HSET (1KB)",
        "warmup": None,
        "memtier_args": [
            '--command=HSET __key__ field __data__', '--command-ratio=1', '--command-key-pattern=S',
            "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Hsets",
    },
    {
        "name": "HGET (1KB)",
        "warmup": [
            '--command=HSET __key__ field __data__', '--command-ratio=1', '--command-key-pattern=S',
            "--data-size", "1024",
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--test-time", "3", "--hide-histogram",
        ],
        "memtier_args": [
            '--command=HGET __key__ field', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Hgets",
    },
    {
        "name": "SADD",
        "warmup": None,
        "memtier_args": [
            '--command=SADD __key__ member', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Sadds",
    },
    {
        "name": "SISMEMBER",
        "warmup": [
            '--command=SADD __key__ member', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--test-time", "3", "--hide-histogram",
        ],
        "memtier_args": [
            '--command=SISMEMBER __key__ member', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Sismembers",
    },
    {
        "name": "ZADD",
        "warmup": None,
        "memtier_args": [
            '--command=ZADD __key__ 10 member', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Zadds",
    },
    {
        "name": "ZRANGE",
        "warmup": [
            '--command=ZADD __key__ 10 member', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
            "--test-time", "3", "--hide-histogram",
        ],
        "memtier_args": [
            '--command=ZRANGE __key__ 0 10', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Zranges",
    },
    {
        "name": "INCR",
        "warmup": None,
        "memtier_args": [
            '--command=INCR __key__', '--command-ratio=1', '--command-key-pattern=S',
            "--pipeline", "100", "--key-minimum", "1", "--key-maximum", "1000000",
        ],
        "type_prefix": "Incrs",
    },
]

def wait_for_server(port=6379, timeout=10.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            res = subprocess.run([VALKEY_CLI, "-p", str(port), "PING"],
                                 capture_output=True, text=True, timeout=1.0)
            if "PONG" in res.stdout:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False

def flushall(port=6379):
    try:
        subprocess.run([VALKEY_CLI, "-p", str(port), "FLUSHALL"],
                       capture_output=True, text=True, timeout=5.0)
    except Exception as e:
        print(f"Warning: FLUSHALL failed: {e}", file=sys.stderr)

def parse_memtier_output(output, target_type):
    lines = output.splitlines()
    in_stats = False
    row_data = None
    totals_data = None
    for line in lines:
        if "ALL STATS" in line:
            in_stats = True
            continue
        if in_stats and line.startswith("---"):
            continue
        if in_stats:
            tokens = line.split()
            if not tokens:
                continue
            name = tokens[0].lower()
            if name == target_type.lower():
                row_data = tokens
            elif name == "totals":
                totals_data = tokens

    chosen = row_data if row_data is not None else totals_data
    if not chosen:
        print(f"Error parsing output for {target_type} in:\n{output}", file=sys.stderr)
        return None

    try:
        return {
            "type": chosen[0],
            "ops_sec": float(chosen[1]),
            "avg_latency": float(chosen[4]),
            "p50": float(chosen[5]),
            "p90": float(chosen[6]),
            "p95": float(chosen[7]),
            "p99": float(chosen[8]),
            "p99_9": float(chosen[9]),
            "kb_sec": float(chosen[10]),
        }
    except Exception as e:
        print(f"Error extracting tokens from {chosen}: {e}", file=sys.stderr)
        return None

def run_memtier(args, test_time=10):
    base_cmd = [
        "taskset", "-c", "32-63", MEMTIER,
        "--server", "127.0.0.1", "--port", "6379",
        "--clients", "1", "--threads", "32",
        "--test-time", str(test_time),
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    full_cmd = base_cmd + args
    res = subprocess.run(full_cmd, capture_output=True, text=True)
    return res.stdout

def main():
    duration = 10
    if len(sys.argv) > 1:
        duration = int(sys.argv[1])

    results = {}

    for engine_name, engine_cfg in ENGINES.items():
        print(f"\n==========================================")
        print(f"Starting {engine_name}...")
        print(f"==========================================")

        subprocess.run(["pkill", "-9", "-f", "rudis"], capture_output=True)
        subprocess.run(["pkill", "-9", "-f", "dragonfly"], capture_output=True)
        time.sleep(1.0)

        proc = subprocess.Popen(engine_cfg["cmd"], cwd=engine_cfg["cwd"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not wait_for_server():
            print(f"Failed to start {engine_name}!", file=sys.stderr)
            proc.kill()
            continue

        print(f"{engine_name} is running and responsive.")
        results[engine_name] = {}

        for test in COMMAND_TESTS:
            test_name = test["name"]
            print(f"  Running test: {test_name} ({duration}s)...", end="", flush=True)

            flushall()
            time.sleep(1.0)

            if test["warmup"]:
                run_memtier(test["warmup"], test_time=3)
                time.sleep(0.5)

            out = run_memtier(test["memtier_args"], test_time=duration)
            stats = parse_memtier_output(out, test["type_prefix"])
            if stats:
                results[engine_name][test_name] = stats
                print(f" {stats['ops_sec']:,.0f} Ops/s, {stats['kb_sec']/1024:.1f} MB/s, p50={stats['p50']:.2f}ms, p99={stats['p99']:.2f}ms")
            else:
                print(" FAILED to parse output")

        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

        time.sleep(1.0)

    with open("multi_command_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nSaved raw results to multi_command_results.json")

if __name__ == "__main__":
    main()
