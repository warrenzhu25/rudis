#!/usr/bin/env python3
"""
Comprehensive Common Commands Benchmark Suite: Rudis vs. Dragonfly
Measures the most common Redis commands across all core data structures on 16 and 32 Cores:
  - Strings: SET, GET, INCR, MSET, MGET
  - Hashes: HSET, HGET
  - Lists: LPUSH, LPOP, LRANGE
  - Sets: SADD, SISMEMBER
  - Sorted Sets (ZSets): ZADD, ZRANGE
  - Keyspace: DEL, EXISTS
"""

import json
import os
import socket
import statistics
import subprocess
import sys
import time

RUDIS_BIN = "/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
DRAGONFLY_BIN = "/usr/local/google/home/warrenzhu/dragonfly"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"

CLIENT_CPUS = "32-63"
CLIENT_THREADS = 16
CLIENT_CONNS = 4  # 64 concurrent connections total
ITERATIONS = 2
KEY_MAX = 64000

WORKLOADS = [
    # 1. Strings
    {
        "id": "SET",
        "category": "Strings",
        "name": "SET (1KB)",
        "pipeline": 16,
        "requests": 2000,
        "data_size": 1024,
        "ratio": "1:0",
        "populate_type": None,
        "command": None,
    },
    {
        "id": "GET",
        "category": "Strings",
        "name": "GET (1KB)",
        "pipeline": 16,
        "requests": 2000,
        "data_size": 1024,
        "ratio": "0:1",
        "populate_type": "string",
        "command": None,
    },
    {
        "id": "INCR",
        "category": "Strings",
        "name": "INCR",
        "pipeline": 16,
        "requests": 2000,
        "data_size": None,
        "ratio": None,
        "populate_type": None,
        "command": "INCR __key__",
    },
    {
        "id": "MSET_5K",
        "category": "Strings",
        "name": "MSET 5-Keys (Cross-Shard)",
        "pipeline": 4,
        "requests": 1000,
        "data_size": 128,
        "ratio": None,
        "populate_type": None,
        "command": "MSET " + " ".join("__key__ __data__" for _ in range(5)),
    },
    {
        "id": "MGET_5K",
        "category": "Strings",
        "name": "MGET 5-Keys (Cross-Shard)",
        "pipeline": 4,
        "requests": 1000,
        "data_size": 128,
        "ratio": None,
        "populate_type": "string",
        "command": "MGET " + " ".join("__key__" for _ in range(5)),
    },

    # 2. Hashes
    {
        "id": "HSET",
        "category": "Hashes",
        "name": "HSET",
        "pipeline": 16,
        "requests": 2000,
        "data_size": 128,
        "ratio": None,
        "populate_type": None,
        "command": "HSET __key__ field1 __data__",
    },
    {
        "id": "HGET",
        "category": "Hashes",
        "name": "HGET",
        "pipeline": 16,
        "requests": 2000,
        "data_size": None,
        "ratio": None,
        "populate_type": "hash",
        "command": "HGET __key__ field1",
    },

    # 3. Lists
    {
        "id": "LPUSH",
        "category": "Lists",
        "name": "LPUSH",
        "pipeline": 16,
        "requests": 2000,
        "data_size": 128,
        "ratio": None,
        "populate_type": None,
        "command": "LPUSH __key__ __data__",
    },
    {
        "id": "LPOP",
        "category": "Lists",
        "name": "LPOP",
        "pipeline": 16,
        "requests": 2000,
        "data_size": None,
        "ratio": None,
        "populate_type": "list",
        "command": "LPOP __key__",
    },
    {
        "id": "LRANGE",
        "category": "Lists",
        "name": "LRANGE (0-10)",
        "pipeline": 16,
        "requests": 2000,
        "data_size": None,
        "ratio": None,
        "populate_type": "list",
        "command": "LRANGE __key__ 0 10",
    },

    # 4. Sets
    {
        "id": "SADD",
        "category": "Sets",
        "name": "SADD",
        "pipeline": 16,
        "requests": 2000,
        "data_size": 64,
        "ratio": None,
        "populate_type": None,
        "command": "SADD __key__ __data__",
    },
    {
        "id": "SISMEMBER",
        "category": "Sets",
        "name": "SISMEMBER",
        "pipeline": 16,
        "requests": 2000,
        "data_size": 64,
        "ratio": None,
        "populate_type": "set",
        "command": "SISMEMBER __key__ __data__",
    },

    # 5. Sorted Sets (ZSets)
    {
        "id": "ZADD",
        "category": "Sorted Sets",
        "name": "ZADD",
        "pipeline": 16,
        "requests": 2000,
        "data_size": 64,
        "ratio": None,
        "populate_type": None,
        "command": "ZADD __key__ 100 __data__",
    },
    {
        "id": "ZRANGE",
        "category": "Sorted Sets",
        "name": "ZRANGE (0-10)",
        "pipeline": 16,
        "requests": 2000,
        "data_size": None,
        "ratio": None,
        "populate_type": "zset",
        "command": "ZRANGE __key__ 0 10",
    },

    # 6. Keyspace / Generic
    {
        "id": "DEL",
        "category": "Keyspace",
        "name": "DEL",
        "pipeline": 16,
        "requests": 2000,
        "data_size": None,
        "ratio": None,
        "populate_type": "string",
        "command": "DEL __key__",
    },
    {
        "id": "EXISTS",
        "category": "Keyspace",
        "name": "EXISTS",
        "pipeline": 16,
        "requests": 2000,
        "data_size": None,
        "ratio": None,
        "populate_type": "string",
        "command": "EXISTS __key__",
    },
]

def wait_ping(port, timeout=8.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.sendall(b"*1\r\n$4\r\nPING\r\n")
            resp = s.recv(1024)
            s.close()
            if b"+PONG" in resp:
                return True
        except Exception:
            time.sleep(0.1)
    return False

def flushall(port):
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=1.0)
        s.sendall(b"*1\r\n$8\r\nFLUSHALL\r\n")
        s.recv(1024)
        s.close()
    except Exception as e:
        print(f"Flushall error on port {port}: {e}")

def populate(port, pop_type):
    if not pop_type:
        return

    json_tmp = f"/tmp/pop_{port}_{pop_type}.json"
    cmd = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", "16",
        "-c", "4",
        "-n", "500",  # 32,000 keys
        "--key-maximum", str(KEY_MAX),
        "--pipeline", "16",
        "--hide-histogram",
        "--json-out-file", json_tmp,
    ]

    if pop_type == "string":
        cmd.extend(["--ratio", "1:0", "-d", "1024", "--key-pattern", "S:S"])
    elif pop_type == "hash":
        cmd.extend([
            "--command=HSET __key__ field1 __data__",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "-d", "128",
        ])
    elif pop_type == "list":
        cmd.extend([
            "--command=LPUSH __key__ __data__",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "-d", "128",
        ])
    elif pop_type == "set":
        cmd.extend([
            "--command=SADD __key__ __data__",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "-d", "64",
        ])
    elif pop_type == "zset":
        cmd.extend([
            "--command=ZADD __key__ 100 __data__",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "-d", "64",
        ])

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(json_tmp):
        os.remove(json_tmp)

def run_single_memtier(port, workload, run_idx):
    json_path = f"/tmp/memtier_{port}_{workload['id']}_{run_idx}.json"
    if os.path.exists(json_path):
        os.remove(json_path)

    cmd = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", str(CLIENT_THREADS),
        "-c", str(CLIENT_CONNS),
        "-n", str(workload["requests"]),
        "--pipeline", str(workload["pipeline"]),
        "--hide-histogram",
        "--json-out-file", json_path,
    ]

    if workload.get("data_size"):
        cmd.extend(["-d", str(workload["data_size"])])

    if workload.get("ratio"):
        cmd.extend(["--ratio", workload["ratio"]])
        cmd.extend(["--key-pattern", "R:R"])
        cmd.extend(["--key-maximum", str(KEY_MAX)])

    if workload.get("command"):
        cmd.extend([
            f"--command={workload['command']}",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "--key-maximum", str(KEY_MAX),
        ])

    res = subprocess.run(cmd, capture_output=True, text=True)
    if not os.path.exists(json_path):
        print(f"       [!] Error running memtier for {workload['id']}: {res.stderr[:200]}")
        return None

    try:
        with open(json_path, "r") as f:
            data = json.load(f)
        os.remove(json_path)
        totals = data["ALL STATS"]["Totals"]
        percentiles = totals.get("Percentile Latencies", {})
        return {
            "ops_sec": float(totals.get("Ops/sec", 0.0)),
            "avg_latency": float(totals.get("Latency", 0.0)),
            "p50": float(percentiles.get("50.000", totals.get("Latency", 0.0))),
            "p99": float(percentiles.get("99.000", totals.get("Latency", 0.0))),
            "p99_9": float(percentiles.get("99.900", totals.get("Latency", 0.0))),
        }
    except Exception as e:
        print(f"       [!] Error parsing JSON {json_path}: {e}")
        return None

def run_workload_benchmark(engine_name, port, workload):
    runs = []
    for r in range(ITERATIONS):
        flushall(port)
        if workload.get("populate_type"):
            populate(port, workload["populate_type"])
        time.sleep(0.2)

        metrics = run_single_memtier(port, workload, r)
        if metrics:
            runs.append(metrics)
            print(f"      Run {r+1}/{ITERATIONS}: {metrics['ops_sec']:,.0f} ops/sec, "
                  f"avg: {metrics['avg_latency']:.2f}ms, p99: {metrics['p99']:.2f}ms")

    if not runs:
        return None

    ops_list = [x["ops_sec"] for x in runs]
    avg_lat_list = [x["avg_latency"] for x in runs]
    p99_list = [x["p99"] for x in runs]

    return {
        "ops_sec_mean": statistics.mean(ops_list),
        "avg_latency_mean": statistics.mean(avg_lat_list),
        "p99_latency_mean": statistics.mean(p99_list),
    }

def benchmark_at_core_count(cores):
    server_cpus = f"0-{cores-1}"
    print(f"\n=================================================================")
    print(f"       BENCHMARKING ON {cores} CORES (CPUs {server_cpus})       ")
    print(f"=================================================================")

    results = {"Dragonfly": {}, "Rudis": {}}

    # 1. Dragonfly
    print(f"\n>>> Launching Dragonfly v1.39 ({cores} threads)...")
    dfly_cmd = [
        "taskset", "-c", server_cpus,
        DRAGONFLY_BIN,
        "--port", "6381",
        f"--proactor_threads={cores}",
        "--cache_mode=false",
        "--dbfilename=",
    ]
    dfly_proc = subprocess.Popen(dfly_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_ping(6381):
        print("Failed to start Dragonfly on port 6381")
        dfly_proc.kill()
    else:
        for wl in WORKLOADS:
            print(f"    [Dragonfly] {wl['name']} ({wl['category']}):")
            m = run_workload_benchmark("Dragonfly", 6381, wl)
            if m:
                results["Dragonfly"][wl["id"]] = m
        dfly_proc.terminate()
        try:
            dfly_proc.wait(timeout=5)
        except Exception:
            dfly_proc.kill()

    time.sleep(1.0)

    # 2. Rudis
    print(f"\n>>> Launching Rudis ({cores} threads)...")
    rudis_cmd = [
        "taskset", "-c", server_cpus,
        RUDIS_BIN,
        "--port", "6379",
        "--threads", str(cores),
    ]
    rudis_proc = subprocess.Popen(rudis_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_ping(6379):
        print("Failed to start Rudis on port 6379")
        rudis_proc.kill()
    else:
        for wl in WORKLOADS:
            print(f"    [Rudis] {wl['name']} ({wl['category']}):")
            m = run_workload_benchmark("Rudis", 6379, wl)
            if m:
                results["Rudis"][wl["id"]] = m
        rudis_proc.terminate()
        try:
            rudis_proc.wait(timeout=5)
        except Exception:
            rudis_proc.kill()

    # Print summary table for this core count
    print(f"\n-------------------------------------------------------------------------------------------------------------")
    print(f"                                   SUMMARY: {cores} CORES HEAD-TO-HEAD                                      ")
    print(f"-------------------------------------------------------------------------------------------------------------")
    print(f"{'Command / Workload':<28} | {'Dragonfly (ops/s)':<18} | {'Rudis (ops/s)':<18} | {'Rudis vs DF':<14} | {'p99 Latency (DF / Rudis)'}")
    print(f"-----------------------------+--------------------+--------------------+----------------+-------------------------")
    for wl in WORKLOADS:
        wl_id = wl["id"]
        df_res = results["Dragonfly"].get(wl_id)
        ru_res = results["Rudis"].get(wl_id)
        if df_res and ru_res:
            ratio = ru_res["ops_sec_mean"] / df_res["ops_sec_mean"]
            diff_pct = (ratio - 1.0) * 100
            diff_str = f"{ratio:.2f}x ({diff_pct:+.1f}%)"
            p99_str = f"{df_res['p99_latency_mean']:.2f}ms / {ru_res['p99_latency_mean']:.2f}ms"
            print(f"{wl['name']:<28} | {df_res['ops_sec_mean']:>15,.0f} ops/s | {ru_res['ops_sec_mean']:>15,.0f} ops/s | {diff_str:<14} | {p99_str}")
    print(f"-------------------------------------------------------------------------------------------------------------\n")

    return results

def main():
    print("=================================================================")
    print("  RUDIS VS DRAGONFLY: MOST COMMON REDIS COMMANDS BENCHMARK SUITE ")
    print("  Cores Tested: 16 Physical Cores & 32 Physical Cores            ")
    print("  Data Types: Strings, Hashes, Lists, Sets, ZSets, Keyspace      ")
    print("=================================================================")

    out_file = "/usr/local/google/home/warrenzhu/github/rudis/benchmark_common_commands_results.json"
    final_results = {}
    if os.path.exists(out_file):
        try:
            with open(out_file, "r") as f:
                final_results = json.load(f)
        except Exception:
            final_results = {}

    core_list = [16]
    if len(sys.argv) > 1:
        core_list = [int(x) for x in sys.argv[1].split(",")]
    for cores in core_list:
        final_results[f"{cores}_cores"] = benchmark_at_core_count(cores)

    with open(out_file, "w") as f:
        json.dump(final_results, f, indent=2)
    print(f"\nBenchmark completed successfully! Results written to {out_file}")

if __name__ == "__main__":
    main()
