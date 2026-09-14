#!/usr/bin/env python3
import subprocess
import time
import re
import os
import sys

SERVER_BIN = "/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"
PORT = 6379
DURATION = 60
THREAD_COUNTS = [1, 2, 4, 8, 16, 32]

os.makedirs("/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs", exist_ok=True)
results = []

def parse_memtier_output(output):
    for line in output.splitlines():
        if line.strip().startswith("Sets") or line.strip().startswith("Totals"):
            parts = line.split()
            if len(parts) >= 10:
                ops_sec = parts[1]
                avg_lat = parts[4]
                p50 = parts[5]
                p90 = parts[6]
                p95 = parts[7]
                p99 = parts[8]
                p999 = parts[9]
                kb_sec = parts[10]
                return {
                    "ops_sec": float(ops_sec),
                    "avg_lat": float(avg_lat),
                    "p50": float(p50),
                    "p90": float(p90),
                    "p95": float(p95),
                    "p99": float(p99),
                    "p999": float(p999),
                    "kb_sec": float(kb_sec),
                }
    return None

print(f"Starting 60s Scaling Suite for threads: {THREAD_COUNTS}")

for t in THREAD_COUNTS:
    print(f"\n=======================================================", flush=True)
    print(f"  Testing rudis with {t} server thread(s) for {DURATION}s...", flush=True)
    print(f"=======================================================", flush=True)

    # Clean up any leftover processes
    subprocess.run(["pkill", "-9", "-x", "rudis"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.0)

    # 1. Start rudis server pinned to cores 0..(t-1)
    core_range = f"0-{t-1}" if t > 1 else "0"
    server_cmd = ["taskset", "-c", core_range, SERVER_BIN, "--threads", str(t), "--port", str(PORT)]
    server_proc = subprocess.Popen(server_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Wait for server to bind
    time.sleep(2.0)

    # 2. Run memtier_benchmark on cores 32-63
    memtier_cmd = [
        "taskset", "-c", "32-63",
        MEMTIER_BIN,
        "--server", "127.0.0.1",
        "--port", str(PORT),
        "--clients", "1",
        "--threads", "32",
        "--ratio", "1:0",
        "--data-size", "1024",
        "--pipeline", "100",
        "--key-minimum", "1",
        "--key-maximum", "1000000",
        "--key-pattern", "S:S",
        "--print-percentiles", "50,90,95,99,99.9",
        "--test-time", str(DURATION),
        "--hide-histogram"
    ]

    try:
        res = subprocess.run(memtier_cmd, capture_output=True, text=True, timeout=DURATION + 30)
        raw_out = res.stdout
    except Exception as e:
        print(f"Error running memtier: {e}", flush=True)
        raw_out = ""

    # Save log
    log_path = f"/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs/squashed_scaling_{t}t.log"
    with open(log_path, "w") as f:
        f.write(raw_out)

    # 3. Kill server
    server_proc.terminate()
    try:
        server_proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        server_proc.kill()
    time.sleep(1.0)

    stats = parse_memtier_output(raw_out)
    if stats:
        stats["threads"] = t
        stats["cores"] = core_range
        results.append(stats)
        print(f"  -> Ops/sec:   {stats['ops_sec']:,.2f}", flush=True)
        print(f"  -> Bandwidth: {stats['kb_sec']/1024:,.2f} MB/sec", flush=True)
        print(f"  -> Avg Lat:   {stats['avg_lat']:.3f} ms", flush=True)
        print(f"  -> p50 Lat:   {stats['p50']:.3f} ms", flush=True)
        print(f"  -> p99 Lat:   {stats['p99']:.3f} ms", flush=True)
    else:
        print("  -> Failed to parse stats. Raw output:\n", raw_out, flush=True)

print("\n\n=======================================================", flush=True)
print("FULL SCALING SUITE RESULTS (60s each):", flush=True)
print("=======================================================", flush=True)
print("| Server Threads | Pinned Cores | Throughput (Ops/sec) | Bandwidth (MB/sec) | Avg Latency (ms) | p50 Latency (ms) | p90 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) | p99.9 Latency (ms) |", flush=True)
print("| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |", flush=True)
for r in results:
    mb_sec = r['kb_sec'] / 1024.0
    print(f"| **{r['threads']}** | `{r['cores']}` | **{r['ops_sec']:,.2f}** | {mb_sec:,.2f} | {r['avg_lat']:.2f} | {r['p50']:.2f} | {r['p90']:.2f} | {r['p95']:.2f} | {r['p99']:.2f} | {r['p999']:.2f} |", flush=True)
