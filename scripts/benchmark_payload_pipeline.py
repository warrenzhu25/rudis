#!/usr/bin/env python3
"""
Rudis Benchmark: Payload Size & Pipeline Depth Sensitivity Sweeps
Evaluates throughput, network saturation, and high-percentile tail latencies.
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

PAYLOAD_SIZES = [64, 256, 1024, 4096, 16384, 65536]
PIPELINE_DEPTHS = [1, 5, 10, 25, 50, 100, 200]

os.makedirs("/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs", exist_ok=True)

def kill_server():
    subprocess.run(["pkill", "-9", "-x", "rudis"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)

def parse_memtier_output(output, metric_key="Totals"):
    target_prefixes = [metric_key, "Totals", "Sets", "Gets"]
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 10:
            for prefix in target_prefixes:
                if parts[0] == prefix:
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

def run_test(args, metric_key, warmup_args=None):
    kill_server()
    server_cmd = ["taskset", "-c", SERVER_CORES, SERVER_BIN, "--threads", str(SERVER_THREADS), "--port", str(PORT)]
    server_proc = subprocess.Popen(server_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.2)

    if warmup_args:
        w_cmd = [
            "taskset", "-c", CLIENT_CORES, MEMTIER_BIN,
            "--server", "127.0.0.1", "--port", str(PORT),
            "--clients", "1", "--threads", "16",
            "--test-time", "2", "--hide-histogram"
        ] + warmup_args
        subprocess.run(w_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    base_cmd = [
        "taskset", "-c", CLIENT_CORES, MEMTIER_BIN,
        "--server", "127.0.0.1", "--port", str(PORT),
        "--clients", "1", "--threads", "32",
        "--print-percentiles", "50,90,95,99,99.9",
        "--test-time", str(DURATION),
        "--hide-histogram"
    ]
    full_cmd = base_cmd + args

    try:
        res = subprocess.run(full_cmd, capture_output=True, text=True, timeout=DURATION + 20)
        output = res.stdout
    except Exception as e:
        print(f"Error running memtier: {e}", flush=True)
        output = ""

    server_proc.terminate()
    try:
        server_proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        server_proc.kill()
    time.sleep(0.3)

    return parse_memtier_output(output, metric_key)

def run_payload_sweep():
    print("\n================================================================================")
    print("                    SWEEP 1: PAYLOAD SIZE SENSITIVITY                           ")
    print("================================================================================")
    print(f"Fixed Config: 16 Server Threads, Pipeline 50, 32 Client Threads, Duration {DURATION}s\n")

    results = {"SET": [], "GET": [], "MIXED": []}

    for size in PAYLOAD_SIZES:
        # 100% SET
        set_args = [
            "--ratio", "1:0", "--data-size", str(size),
            "--pipeline", "50", "--key-minimum", "1", "--key-maximum", "500000",
            "--key-pattern", "S:S"
        ]
        s_res = run_test(set_args, "Sets")
        if s_res:
            s_res["payload_bytes"] = size
            results["SET"].append(s_res)
            mb_s = s_res["kb_sec"] / 1024.0
            print(f"  [SET  {size:>5}B] -> {s_res['ops_sec']:>10,.1f} Ops/s | {mb_s:>7,.1f} MB/s | Avg: {s_res['avg_lat']:>5.2f}ms | p50: {s_res['p50']:>5.2f}ms | p99: {s_res['p99']:>5.2f}ms", flush=True)

        # 100% GET
        warm_args = [
            "--ratio", "1:0", "--data-size", str(size),
            "--pipeline", "50", "--key-minimum", "1", "--key-maximum", "500000",
            "--key-pattern", "S:S"
        ]
        get_args = [
            "--ratio", "0:1", "--data-size", str(size),
            "--pipeline", "50", "--key-minimum", "1", "--key-maximum", "500000",
            "--key-pattern", "S:S"
        ]
        g_res = run_test(get_args, "Gets", warmup_args=warm_args)
        if g_res:
            g_res["payload_bytes"] = size
            results["GET"].append(g_res)
            mb_s = g_res["kb_sec"] / 1024.0
            print(f"  [GET  {size:>5}B] -> {g_res['ops_sec']:>10,.1f} Ops/s | {mb_s:>7,.1f} MB/s | Avg: {g_res['avg_lat']:>5.2f}ms | p50: {g_res['p50']:>5.2f}ms | p99: {g_res['p99']:>5.2f}ms", flush=True)

        # 50/50 SET/GET
        mix_args = [
            "--ratio", "1:1", "--data-size", str(size),
            "--pipeline", "50", "--key-minimum", "1", "--key-maximum", "500000",
            "--key-pattern", "S:S"
        ]
        m_res = run_test(mix_args, "Totals", warmup_args=warm_args)
        if m_res:
            m_res["payload_bytes"] = size
            results["MIXED"].append(m_res)
            mb_s = m_res["kb_sec"] / 1024.0
            print(f"  [MIX  {size:>5}B] -> {m_res['ops_sec']:>10,.1f} Ops/s | {mb_s:>7,.1f} MB/s | Avg: {m_res['avg_lat']:>5.2f}ms | p50: {m_res['p50']:>5.2f}ms | p99: {m_res['p99']:>5.2f}ms", flush=True)

    return results

def run_pipeline_sweep():
    print("\n================================================================================")
    print("                    SWEEP 2: PIPELINE DEPTH SENSITIVITY                         ")
    print("================================================================================")
    print(f"Fixed Config: 16 Server Threads, 1KB Payload, 32 Client Threads, Duration {DURATION}s\n")

    results = {"SET": [], "GET": []}

    for depth in PIPELINE_DEPTHS:
        # SET
        set_args = [
            "--ratio", "1:0", "--data-size", "1024",
            "--pipeline", str(depth), "--key-minimum", "1", "--key-maximum", "500000",
            "--key-pattern", "S:S"
        ]
        s_res = run_test(set_args, "Sets")
        if s_res:
            s_res["pipeline_depth"] = depth
            results["SET"].append(s_res)
            mb_s = s_res["kb_sec"] / 1024.0
            print(f"  [SET  Pipeline {depth:>3}] -> {s_res['ops_sec']:>10,.1f} Ops/s | {mb_s:>7,.1f} MB/s | Avg: {s_res['avg_lat']:>5.2f}ms | p50: {s_res['p50']:>5.2f}ms | p99: {s_res['p99']:>5.2f}ms | p99.9: {s_res['p999']:>5.2f}ms", flush=True)

        # GET
        warm_args = [
            "--ratio", "1:0", "--data-size", "1024",
            "--pipeline", "50", "--key-minimum", "1", "--key-maximum", "500000",
            "--key-pattern", "S:S"
        ]
        get_args = [
            "--ratio", "0:1", "--data-size", "1024",
            "--pipeline", str(depth), "--key-minimum", "1", "--key-maximum", "500000",
            "--key-pattern", "S:S"
        ]
        g_res = run_test(get_args, "Gets", warmup_args=warm_args)
        if g_res:
            g_res["pipeline_depth"] = depth
            results["GET"].append(g_res)
            mb_s = g_res["kb_sec"] / 1024.0
            print(f"  [GET  Pipeline {depth:>3}] -> {g_res['ops_sec']:>10,.1f} Ops/s | {mb_s:>7,.1f} MB/s | Avg: {g_res['avg_lat']:>5.2f}ms | p50: {g_res['p50']:>5.2f}ms | p99: {g_res['p99']:>5.2f}ms | p99.9: {g_res['p999']:>5.2f}ms", flush=True)

    return results

def main():
    payload_results = run_payload_sweep()
    pipeline_results = run_pipeline_sweep()

    full_data = {
        "payload_sweep": payload_results,
        "pipeline_sweep": pipeline_results
    }

    out_file = "/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs/payload_pipeline_results.json"
    with open(out_file, "w") as f:
        json.dump(full_data, f, indent=2)
    print(f"\nSaved raw JSON results to: {out_file}")

if __name__ == "__main__":
    main()
