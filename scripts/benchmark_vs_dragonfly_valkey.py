#!/usr/bin/env python3
"""
Comprehensive 5-run benchmark suite comparing Rudis vs Dragonfly vs Valkey.
Covers:
  - Single-key 100% SET (Pipeline 1)
  - Single-key 100% GET (Pipeline 1)
  - Single-key 50:50 Mixed (Pipeline 1)
  - High-throughput 100% SET (Pipeline 16)
  - High-throughput 100% GET (Pipeline 16)
  - High-throughput 50:50 Mixed (Pipeline 16)
  - Multi-key MSET (10 keys scattered)
  - Multi-key MGET (10 keys scattered)
  - Multi-key MGET (10 keys co-located with {hashtag})
"""

import json
import math
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time

RUDIS_BIN = "/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
DRAGONFLY_BIN = "/usr/local/google/home/warrenzhu/dragonfly"
VALKEY_BIN = "/usr/local/google/home/warrenzhu/valkey-stable/src/valkey-server"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"

SERVER_CPUS = "0-7"
CLIENT_CPUS = "32-47"
CLIENT_THREADS = 8
CLIENT_CONNS = 8  # 64 concurrent clients total
ITERATIONS = 5

ENGINES = {
    "Valkey 8.1.9": {
        "bin": VALKEY_BIN,
        "port": 6380,
        "cmd": [
            "taskset", "-c", SERVER_CPUS,
            VALKEY_BIN,
            "--port", "6380",
            "--io-threads", "8",
            "--io-threads-do-reads", "yes",
            "--protected-mode", "no",
            "--save", "",
            "--appendonly", "no",
        ],
    },
    "Dragonfly v1.39.0": {
        "bin": DRAGONFLY_BIN,
        "port": 6381,
        "cmd": [
            "taskset", "-c", SERVER_CPUS,
            DRAGONFLY_BIN,
            "--port", "6381",
            "--proactor_threads=8",
            "--cache_mode=false",
            "--dbfilename=",
        ],
    },
    "Rudis (Thread-per-core)": {
        "bin": RUDIS_BIN,
        "port": 6379,
        "cmd": [
            "taskset", "-c", SERVER_CPUS,
            RUDIS_BIN,
            "--port", "6379",
            "--threads", "8",
        ],
    },
}

WORKLOADS = [
    {
        "id": "set_p1",
        "name": "SET 100% (Pipeline 1, 1KB)",
        "pipeline": 1,
        "requests": 2000,
        "data_size": 1024,
        "ratio": "1:0",
        "needs_populate": False,
        "command": None,
    },
    {
        "id": "get_p1",
        "name": "GET 100% (Pipeline 1, 1KB)",
        "pipeline": 1,
        "requests": 2000,
        "data_size": 1024,
        "ratio": "0:1",
        "needs_populate": True,
        "command": None,
    },
    {
        "id": "mixed_p1",
        "name": "Mixed 50:50 (Pipeline 1, 1KB)",
        "pipeline": 1,
        "requests": 2000,
        "data_size": 1024,
        "ratio": "1:1",
        "needs_populate": True,
        "command": None,
    },
    {
        "id": "set_p16",
        "name": "SET 100% (Pipeline 16, 1KB)",
        "pipeline": 16,
        "requests": 10000,
        "data_size": 1024,
        "ratio": "1:0",
        "needs_populate": False,
        "command": None,
    },
    {
        "id": "get_p16",
        "name": "GET 100% (Pipeline 16, 1KB)",
        "pipeline": 16,
        "requests": 10000,
        "data_size": 1024,
        "ratio": "0:1",
        "needs_populate": True,
        "command": None,
    },
    {
        "id": "mixed_p16",
        "name": "Mixed 50:50 (Pipeline 16, 1KB)",
        "pipeline": 16,
        "requests": 10000,
        "data_size": 1024,
        "ratio": "1:1",
        "needs_populate": True,
        "command": None,
    },
    {
        "id": "mset_10k_scattered",
        "name": "MSET 10 Keys (Scattered)",
        "pipeline": 1,
        "requests": 1000,
        "data_size": 100,
        "ratio": None,
        "needs_populate": False,
        "command": "MSET " + " ".join("__key__ __data__" for _ in range(10)),
    },
    {
        "id": "mget_10k_scattered",
        "name": "MGET 10 Keys (Scattered)",
        "pipeline": 1,
        "requests": 1000,
        "data_size": 100,
        "ratio": None,
        "needs_populate": True,
        "command": "MGET " + " ".join("__key__" for _ in range(10)),
    },
    {
        "id": "mget_10k_colocated",
        "name": "MGET 10 Keys (Co-located {tag})",
        "pipeline": 1,
        "requests": 1000,
        "data_size": 100,
        "ratio": None,
        "needs_populate": True,
        "command": "MGET " + " ".join(f"{{user:tag}}key_{i}" for i in range(10)),
    },
]

def check_vm_load():
    """Verify load is low before benchmarking."""
    load1, load5, load15 = os.getloadavg()
    print(f"[VM Load Check] 1-min load: {load1:.2f}, 5-min: {load5:.2f}, 15-min: {load15:.2f}")
    if load1 > 3.0:
        print(f"Warning: load {load1:.2f} > 3.0. Waiting 5 seconds to quiet down...")
        time.sleep(5.0)

def wait_ping(port, timeout=5.0):
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

def populate_keyspace(port, num_keys=64000, data_size=1024):
    """Pre-populate keyspace for GET and mixed workloads."""
    json_tmp = f"/tmp/populate_{port}.json"
    cmd = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", "8",
        "-c", "8",
        "-n", str(num_keys // 64),
        "--ratio", "1:0",
        "-d", str(data_size),
        "--key-pattern", "S:S",
        "--key-maximum", str(num_keys),
        "--pipeline", "16",
        "--hide-histogram",
        "--json-out-file", json_tmp,
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(json_tmp):
        os.remove(json_tmp)

    # Also populate co-located keys
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=2.0)
        for i in range(10):
            k = f"{{user:tag}}key_{i}"
            v = "x" * data_size
            cmd_raw = f"*3\r\n$3\r\nSET\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n".encode()
            s.sendall(cmd_raw)
            s.recv(1024)
        s.close()
    except Exception as e:
        print(f"Error populating co-located keys: {e}")

def run_single_memtier(port, workload, run_idx):
    """Execute one memtier run and parse JSON output."""
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
        cmd.extend(["--key-maximum", "64000"])

    if workload.get("command"):
        cmd.extend([
            f"--command={workload['command']}",
            "--command-ratio=1",
            "--command-key-pattern=R",
        ])

    res = subprocess.run(cmd, capture_output=True, text=True)
    if not os.path.exists(json_path):
        print(f"Error: JSON output file {json_path} not found. Stderr: {res.stderr[:200]}")
        return None

    try:
        with open(json_path, "r") as f:
            data = json.load(f)
        os.remove(json_path)
        totals = data["ALL STATS"]["Totals"]
        percentiles = totals.get("Percentile Latencies", {})
        ops_sec = float(totals["Ops/sec"])
        avg_lat = float(totals["Average Latency"])
        p99_lat = float(percentiles.get("p99.00", 0.0))
        p50_lat = float(percentiles.get("p50.00", 0.0))
        return {
            "ops_sec": ops_sec,
            "avg_lat_ms": avg_lat,
            "p50_lat_ms": p50_lat,
            "p99_lat_ms": p99_lat,
        }
    except Exception as e:
        print(f"Error parsing JSON {json_path}: {e}")
        return None

def compute_stats(values):
    if not values:
        return {"mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0}
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "mean": mean,
        "std": std,
        "min": min(values),
        "max": max(values),
    }

def main():
    global ITERATIONS
    df_only = "--df-only" in sys.argv or "--dragonfly-only" in sys.argv
    rudis_only = "--rudis-only" in sys.argv
    for i, arg in enumerate(sys.argv):
        if arg in ("-i", "--iterations") and i + 1 < len(sys.argv):
            ITERATIONS = int(sys.argv[i + 1])

    print("=" * 70)
    print(f"   AUTOMATED {ITERATIONS}-RUN COMPARATIVE BENCHMARK")
    if rudis_only:
        print("   Rudis Only")
    elif df_only:
        print("   Rudis vs Dragonfly v1.39.0 (Valkey skipped)")
    else:
        print("   Rudis vs Dragonfly v1.39.0 vs Valkey 8.1.9")
    print("=" * 70)

    # Clean up any lingering dump files or processes
    if os.path.exists("dump.rdb"):
        os.remove("dump.rdb")

    all_results = {}
    engines_to_run = {
        k: v for k, v in ENGINES.items()
        if not (df_only and "Valkey" in k) and not (rudis_only and "Rudis" not in k)
    }

    for engine_name, engine_info in engines_to_run.items():
        print(f"\n>>>>>>>> Starting Engine: {engine_name} (Port {engine_info['port']}) <<<<<<<<")
        check_vm_load()

        # Start server process
        server_proc = subprocess.Popen(
            engine_info["cmd"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if not wait_ping(engine_info["port"], timeout=5.0):
            print(f"Failed to connect to {engine_name} on port {engine_info['port']}!")
            server_proc.kill()
            continue

        print(f"Engine {engine_name} online and responsive.")
        all_results[engine_name] = {}

        try:
            for w in WORKLOADS:
                print(f"  --> Workload: {w['name']} ({ITERATIONS} runs)...", end="", flush=True)
                flushall(engine_info["port"])
                time.sleep(0.3)

                if w["needs_populate"]:
                    populate_keyspace(engine_info["port"], data_size=w.get("data_size", 1024))

                run_metrics = []
                for r in range(ITERATIONS):
                    res = run_single_memtier(engine_info["port"], w, r)
                    if res:
                        run_metrics.append(res)
                    time.sleep(0.2)

                ops = [m["ops_sec"] for m in run_metrics]
                avg_lats = [m["avg_lat_ms"] for m in run_metrics]
                p99_lats = [m["p99_lat_ms"] for m in run_metrics]

                stats_ops = compute_stats(ops)
                stats_lat = compute_stats(avg_lats)
                stats_p99 = compute_stats(p99_lats)

                all_results[engine_name][w["id"]] = {
                    "workload_name": w["name"],
                    "ops_sec": stats_ops,
                    "avg_lat_ms": stats_lat,
                    "p99_lat_ms": stats_p99,
                    "raw_runs": run_metrics,
                }
                print(f" Done! Mean: {stats_ops['mean']:,.1f} ops/s (±{stats_ops['std']:,.1f}), p99: {stats_p99['mean']:.3f} ms")

        finally:
            server_proc.terminate()
            try:
                server_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server_proc.kill()
            time.sleep(1.0)
            if os.path.exists("dump.rdb"):
                os.remove("dump.rdb")

    # Save results to json
    results_path = "docs/benchmarks/benchmark_comparison_results.json"
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    with open(results_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nRaw results successfully saved to {results_path}")

    # Generate Markdown Summary Table
    print("\n" + "=" * 80)
    print(f"{'Workload':<32} | {'Rudis (Mean ops/s)':<18} | {'Dragonfly (Mean)':<18} | {'Valkey 8.1.9':<16}")
    print("-" * 80)
    for w in WORKLOADS:
        w_id = w["id"]
        rudis_val = all_results.get("Rudis (Thread-per-core)", {}).get(w_id, {}).get("ops_sec", {}).get("mean", 0.0)
        df_val = all_results.get("Dragonfly v1.39.0", {}).get(w_id, {}).get("ops_sec", {}).get("mean", 0.0)
        vk_val = all_results.get("Valkey 8.1.9", {}).get(w_id, {}).get("ops_sec", {}).get("mean", 0.0)
        print(f"{w['name']:<32} | {rudis_val:>18,.1f} | {df_val:>18,.1f} | {vk_val:>16,.1f}")
    print("=" * 80)

if __name__ == "__main__":
    main()
