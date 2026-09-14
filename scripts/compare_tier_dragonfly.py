#!/usr/bin/env python3
import subprocess
import time
import os
import sys
import json
import shutil

MEMTIER = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"
VALKEY_CLI = "/usr/local/google/home/warrenzhu/valkey/src/valkey-cli"
RUDIS_BIN = "./target/release/rudis"
DRAGONFLY_BIN = "/usr/local/google/home/warrenzhu/dragonfly"

def wait_for_server(port, timeout=10.0):
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

def run_workload(port, test_name, ratio, duration=10):
    cmd = [
        "taskset", "-c", "8-23", MEMTIER,
        "-s", "127.0.0.1", "-p", str(port),
        "--threads", "4", "--clients", "4",
        "--pipeline", "50",
        "--ratio", ratio, "--data-size", "1024",
        "--key-minimum", "1", "--key-maximum", "1500000",
        "--key-pattern", "S:S",
        "--test-time", str(duration),
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    target_type = "Sets" if ratio == "1:0" else ("Gets" if ratio == "0:1" else "Totals")
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    res = parse_memtier_output(out, target_type)
    if res:
        print(f"    {test_name}: {res['ops_sec']:.0f} ops/sec, {res['avg_latency']:.2f} ms (p50: {res['p50']:.2f}ms, p99: {res['p99']:.2f}ms)")
    else:
        print(f"    {test_name}: FAILED to parse output")
    return res

def run_benchmark():
    results = {}
    port = 6395

    # -----------------------------------------------------------------
    # 1. Benchmark Dragonfly Tiered Storage (4 threads, 1024MB maxmemory)
    # -----------------------------------------------------------------
    print("==================================================================")
    print("Benchmarking Dragonfly Tiered Storage (4 threads, 1024MB maxmemory)")
    print("==================================================================")
    df_dir = "/tmp/df_tier_bench"
    if os.path.exists(df_dir):
        shutil.rmtree(df_dir)
    os.makedirs(df_dir, exist_ok=True)

    df_proc = subprocess.Popen([
        "taskset", "-c", "0-3",
        DRAGONFLY_BIN,
        "--proactor_threads=4",
        f"--port={port}",
        "--maxmemory=1024mb",
        f"--tiered_prefix={df_dir}/tier",
        "--tiered_experimental_cooling=true",
        "--pipeline_squash=0",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if not wait_for_server(port):
        print("Failed to start Dragonfly", file=sys.stderr)
        df_proc.kill()
        return

    print("  Running SET (1KB, 10s)...")
    df_set = run_workload(port, "SET", "1:0", duration=10)
    print("  Running GET (1KB, 10s)...")
    df_get = run_workload(port, "GET", "0:1", duration=10)
    print("  Running SET/GET 1:1 (1KB, 10s)...")
    df_mix = run_workload(port, "SET/GET 1:1", "1:1", duration=10)

    df_proc.kill()
    df_proc.wait()
    shutil.rmtree(df_dir, ignore_errors=True)

    results["Dragonfly"] = {
        "set": df_set,
        "get": df_get,
        "mix": df_mix,
    }

    # -----------------------------------------------------------------
    # 2. Benchmark Rudis Tiered Storage (4 threads, 1024MB maxmemory)
    # -----------------------------------------------------------------
    print("\n==================================================================")
    print("Benchmarking Rudis Tiered Storage (4 threads, 1024MB maxmemory)")
    print("==================================================================")
    rudis_dir = "/tmp/rudis_tier_bench"
    if os.path.exists(rudis_dir):
        shutil.rmtree(rudis_dir)
    os.makedirs(rudis_dir, exist_ok=True)

    env_rudis = os.environ.copy()
    env_rudis["RUDIS_TIER_DIR"] = rudis_dir

    rudis_proc = subprocess.Popen([
        "taskset", "-c", "0-3",
        RUDIS_BIN,
        "--threads", "4",
        "--port", str(port),
        "--maxmemory", "1024mb",
        "--tiered-offload-threshold", "60",
        "--tiered-upload-threshold", "80",
    ], env=env_rudis, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if not wait_for_server(port):
        print("Failed to start Rudis", file=sys.stderr)
        rudis_proc.kill()
        return

    print("  Running SET (1KB, 10s)...")
    rudis_set = run_workload(port, "SET", "1:0", duration=10)
    print("  Running GET (1KB, 10s)...")
    rudis_get = run_workload(port, "GET", "0:1", duration=10)
    print("  Running SET/GET 1:1 (1KB, 10s)...")
    rudis_mix = run_workload(port, "SET/GET 1:1", "1:1", duration=10)

    rudis_proc.kill()
    rudis_proc.wait()
    shutil.rmtree(rudis_dir, ignore_errors=True)

    results["Rudis"] = {
        "set": rudis_set,
        "get": rudis_get,
        "mix": rudis_mix,
    }

    with open("tier_dragonfly_comparison.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nBenchmark results saved to tier_dragonfly_comparison.json")

if __name__ == "__main__":
    run_benchmark()
