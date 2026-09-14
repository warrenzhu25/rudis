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

def get_tier_info(port):
    try:
        res = subprocess.run([VALKEY_CLI, "-p", str(port), "TIER", "INFO"],
                             capture_output=True, text=True, timeout=2.0)
        info = {}
        for line in res.stdout.splitlines():
            if ":" in line and not line.startswith("#"):
                k, v = line.split(":", 1)
                info[k.strip()] = v.strip()
        return info
    except Exception as e:
        print(f"Error fetching TIER INFO: {e}", file=sys.stderr)
        return {}

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
        # tokens: [Type, Ops/sec, Hits/sec, Misses/sec, Avg. Latency, p50, p90, p95, p99, p99.9, KB/sec]
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

def run_test():
    tier_dir = "/tmp/rudis_tier_bench"
    if os.path.exists(tier_dir):
        shutil.rmtree(tier_dir)
    os.makedirs(tier_dir, exist_ok=True)

    results = {}

    # -------------------------------------------------------------
    # 1. Baseline: Pure In-Memory (No MaxMemory Limit)
    # -------------------------------------------------------------
    print("=========================================================")
    print("Running Baseline: In-Memory (8 threads, no tiering limit)")
    print("=========================================================")
    port_dram = 6381
    env_dram = os.environ.copy()
    env_dram["RUDIS_TIER_DIR"] = tier_dir

    p_dram = subprocess.Popen([
        "taskset", "-c", "0-7",
        RUDIS_BIN,
        "--threads", "8",
        "--port", str(port_dram),
    ], env=env_dram)

    if not wait_for_server(port_dram):
        print("Failed to start DRAM server", file=sys.stderr)
        p_dram.kill()
        return

    # SET benchmark (populate 100,000 keys, 512B)
    print("  Running In-Memory SET...")
    cmd_set = [
        "taskset", "-c", "8-23", MEMTIER,
        "-s", "127.0.0.1", "-p", str(port_dram),
        "--threads", "8", "--clients", "4",
        "--pipeline", "50",
        "--ratio", "1:0", "--data-size", "512",
        "--key-minimum", "1", "--key-maximum", "100000",
        "--key-pattern", "S:S",
        "--test-time", "10",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    out_set = subprocess.run(cmd_set, capture_output=True, text=True).stdout
    metrics_set_dram = parse_memtier_output(out_set, "Sets")
    print(f"  SET: {metrics_set_dram['ops_sec']:.0f} ops/sec, {metrics_set_dram['avg_latency']:.2f} ms")

    # GET benchmark
    print("  Running In-Memory GET...")
    cmd_get = [
        "taskset", "-c", "8-23", MEMTIER,
        "-s", "127.0.0.1", "-p", str(port_dram),
        "--threads", "8", "--clients", "4",
        "--pipeline", "50",
        "--ratio", "0:1", "--data-size", "512",
        "--key-minimum", "1", "--key-maximum", "100000",
        "--key-pattern", "S:S",
        "--test-time", "10",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    out_get = subprocess.run(cmd_get, capture_output=True, text=True).stdout
    metrics_get_dram = parse_memtier_output(out_get, "Gets")
    print(f"  GET: {metrics_get_dram['ops_sec']:.0f} ops/sec, {metrics_get_dram['avg_latency']:.2f} ms")

    # SET/GET 1:1 benchmark
    print("  Running In-Memory SET/GET 1:1...")
    cmd_mix = [
        "taskset", "-c", "8-23", MEMTIER,
        "-s", "127.0.0.1", "-p", str(port_dram),
        "--threads", "8", "--clients", "4",
        "--pipeline", "50",
        "--ratio", "1:1", "--data-size", "512",
        "--key-minimum", "1", "--key-maximum", "100000",
        "--key-pattern", "S:S",
        "--test-time", "10",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    out_mix = subprocess.run(cmd_mix, capture_output=True, text=True).stdout
    metrics_mix_dram = parse_memtier_output(out_mix, "Totals")
    print(f"  SET/GET: {metrics_mix_dram['ops_sec']:.0f} ops/sec, {metrics_mix_dram['avg_latency']:.2f} ms")

    p_dram.kill()
    p_dram.wait()

    results["in_memory"] = {
        "set": metrics_set_dram,
        "get": metrics_get_dram,
        "mix": metrics_mix_dram,
    }

    # -------------------------------------------------------------
    # 2. Tiered Storage: NVMe with SmallBins & OpManager
    # -------------------------------------------------------------
    print("\n=========================================================")
    print("Running Tiered Storage (8 threads, maxmemory 32MB, 512B SmallBins)")
    print("=========================================================")
    shutil.rmtree(tier_dir)
    os.makedirs(tier_dir, exist_ok=True)

    port_tier = 6382
    env_tier = os.environ.copy()
    env_tier["RUDIS_TIER_DIR"] = tier_dir

    p_tier = subprocess.Popen([
        "taskset", "-c", "0-7",
        RUDIS_BIN,
        "--threads", "8",
        "--port", str(port_tier),
        "--maxmemory", "32mb",
        "--tiered-offload-threshold", "60",
        "--tiered-upload-threshold", "80",
    ], env=env_tier)

    if not wait_for_server(port_tier):
        print("Failed to start Tiered server", file=sys.stderr)
        p_tier.kill()
        return

    # SET benchmark (populate 100,000 keys of 512B -> ~60MB dataset exceeding 32MB limit)
    print("  Running Tiered Storage SET (Spill & SmallBins packing)...")
    cmd_set_t = [
        "taskset", "-c", "8-23", MEMTIER,
        "-s", "127.0.0.1", "-p", str(port_tier),
        "--threads", "8", "--clients", "4",
        "--pipeline", "50",
        "--ratio", "1:0", "--data-size", "512",
        "--key-minimum", "1", "--key-maximum", "100000",
        "--key-pattern", "S:S",
        "--test-time", "10",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    out_set_t = subprocess.run(cmd_set_t, capture_output=True, text=True).stdout
    metrics_set_tier = parse_memtier_output(out_set_t, "Sets")
    print(f"  SET: {metrics_set_tier['ops_sec']:.0f} ops/sec, {metrics_set_tier['avg_latency']:.2f} ms")

    info_after_set = get_tier_info(port_tier)
    print("  TIER INFO after SET:")
    for k in ["cooled_keys", "tiered_keys", "tiered_bytes", "ram_saved_bytes", "bin_pages", "total_stashes"]:
        print(f"    {k}: {info_after_set.get(k, 'N/A')}")

    # Instant Decommit to push cooled items to cold storage for maximum read tier test
    subprocess.run([VALKEY_CLI, "-p", str(port_tier), "TIER", "DECOMMIT"], capture_output=True)

    # GET benchmark (reads tiered data via OpManager and io_uring)
    print("  Running Tiered Storage GET (OpManager read coalescing & disk fetch)...")
    cmd_get_t = [
        "taskset", "-c", "8-23", MEMTIER,
        "-s", "127.0.0.1", "-p", str(port_tier),
        "--threads", "8", "--clients", "4",
        "--pipeline", "50",
        "--ratio", "0:1", "--data-size", "512",
        "--key-minimum", "1", "--key-maximum", "100000",
        "--key-pattern", "S:S",
        "--test-time", "10",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    out_get_t = subprocess.run(cmd_get_t, capture_output=True, text=True).stdout
    metrics_get_tier = parse_memtier_output(out_get_t, "Gets")
    print(f"  GET: {metrics_get_tier['ops_sec']:.0f} ops/sec, {metrics_get_tier['avg_latency']:.2f} ms")

    # SET/GET 1:1 benchmark
    print("  Running Tiered Storage SET/GET 1:1 (concurrent stashing & reading)...")
    cmd_mix_t = [
        "taskset", "-c", "8-23", MEMTIER,
        "-s", "127.0.0.1", "-p", str(port_tier),
        "--threads", "8", "--clients", "4",
        "--pipeline", "50",
        "--ratio", "1:1", "--data-size", "512",
        "--key-minimum", "1", "--key-maximum", "100000",
        "--key-pattern", "S:S",
        "--test-time", "10",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    out_mix_t = subprocess.run(cmd_mix_t, capture_output=True, text=True).stdout
    metrics_mix_tier = parse_memtier_output(out_mix_t, "Totals")
    print(f"  SET/GET: {metrics_mix_tier['ops_sec']:.0f} ops/sec, {metrics_mix_tier['avg_latency']:.2f} ms")

    info_final = get_tier_info(port_tier)
    print("  Final TIER INFO:")
    for k in ["cooled_keys", "tiered_keys", "tiered_bytes", "ram_saved_bytes", "bin_pages",
              "coalesced_reads", "total_stashes", "total_fetches", "ram_hits", "ram_misses", "streaming_reads"]:
        print(f"    {k}: {info_final.get(k, 'N/A')}")

    p_tier.kill()
    p_tier.wait()

    results["tiered"] = {
        "set": metrics_set_tier,
        "get": metrics_get_tier,
        "mix": metrics_mix_tier,
        "info_after_set": info_after_set,
        "info_final": info_final,
    }

    with open("tiered_benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nBenchmark results written to tiered_benchmark_results.json")

if __name__ == "__main__":
    run_test()
