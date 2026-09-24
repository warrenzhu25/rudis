#!/usr/bin/env python3
"""
Head-to-Head NVMe Tiered Storage Benchmark Under Memory Pressure:
Rudis (io_uring + SmallBins + OpManager) vs. Dragonfly v1.39 (io_uring Tiered Storage)

Configuration:
  - Threads: 4 shards/threads pinned to quiet physical cores 16-19
  - Client: memtier_benchmark pinned to disjoint physical cores 0-15
  - Memory Cap (maxmemory): 1024 MB (Dragonfly minimum for 4 threads is 1.0 GiB)
  - Offload Threshold: 50% of maxmemory (starts offloading at ~512 MB)
  - Dataset: 350,000 keys x 4096B (4 KB) = ~1.43 GB raw payload (~1.4x RAM limit)
  - Backing Storage: Real ext4 NVMe filesystem
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import time

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUDIS_BIN = os.path.join(REPO_DIR, "target/release/rudis")
DRAGONFLY_BIN = "/usr/local/google/home/warrenzhu/dragonfly"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"
BENCH_BASE_DIR = os.path.join(REPO_DIR, "tier_bench_nvme")

SERVER_CPUS = "16-19"
CLIENT_CPUS = "0-15"
THREADS = 4
PORT = 6391

MAXMEMORY_MB = 1024
DATA_SIZE = 4096
KEY_MAX = 350000
PIPELINE = 32
CLIENTS_PER_THREAD = 4
CLIENT_THREADS = 8


def wait_ping(port, timeout=15.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.sendall(b"*1\r\n$4\r\nPING\r\n")
            resp = s.recv(1024)
            s.close()
            if b"PONG" in resp:
                return True
        except Exception:
            time.sleep(0.2)
    return False


def redis_cmd(port, *args):
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=5.0)
        parts = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = str(a).encode()
            parts.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
        s.sendall(b"".join(parts))
        chunks = []
        s.settimeout(2.0)
        while True:
            try:
                data = s.recv(65536)
                if not data:
                    break
                chunks.append(data)
                if len(data) < 65536:
                    break
            except socket.timeout:
                break
        s.close()
        return b"".join(chunks).decode("utf-8", errors="replace")
    except Exception as e:
        return f"ERR: {e}"


def parse_info_dict(raw_text):
    out = {}
    for line in raw_text.splitlines():
        line = line.strip()
        if ":" in line and not line.startswith("#") and not line.startswith("$"):
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def run_memtier(port, ratio, test_time=None, requests_per_client=None, key_pattern="R:R"):
    json_out = f"/tmp/tier_memtier_{port}.json"
    if os.path.exists(json_out):
        os.remove(json_out)

    cmd = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "-s", "127.0.0.1", "-p", str(port),
        "--threads", str(CLIENT_THREADS),
        "--clients", str(CLIENTS_PER_THREAD),
        "--pipeline", str(PIPELINE),
        "--ratio", ratio,
        "--data-size", str(DATA_SIZE),
        "--key-minimum", "1",
        "--key-maximum", str(KEY_MAX),
        "--key-pattern", key_pattern,
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
        "--json-out-file", json_out,
    ]
    if requests_per_client is not None:
        cmd.extend(["-n", str(requests_per_client)])
    else:
        cmd.extend(["--test-time", str(test_time or 10)])

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    with open(json_out) as f:
        data = json.load(f)

    totals = data["ALL STATS"]["Totals"]
    pcts = totals.get("Percentile Latencies", {})
    return {
        "ops_sec": float(totals.get("Ops/sec", 0.0)),
        "kb_sec": float(totals.get("KB/sec", 0.0)),
        "avg_latency_ms": float(totals.get("Average Latency", totals.get("Latency", 0.0))),
        "p50_ms": float(pcts.get("p50.00", 0.0)),
        "p90_ms": float(pcts.get("p90.00", 0.0)),
        "p99_ms": float(pcts.get("p99.00", 0.0)),
    }


def benchmark_engine(name, proc_cmd, env=None, is_rudis=False):
    print("\n" + "=" * 85)
    print(f"  BENCHMARKING: {name}")
    print(f"  4 Threads (CPUs {SERVER_CPUS}) | MaxMemory {MAXMEMORY_MB} MB | Dataset ~1.43 GB (4KB x 350K keys)")
    print("=" * 85)

    proc = subprocess.Popen(
        proc_cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    if not wait_ping(PORT):
        proc.kill()
        raise RuntimeError(f"{name} failed to start on port {PORT}")

    try:
        total_conns = CLIENT_THREADS * CLIENTS_PER_THREAD
        reqs_per_conn = (KEY_MAX + total_conns - 1) // total_conns

        # 1. Phase 1: Write & Offload (Exact 350,000 unique 4KB keys = 1.43 GB > 1.0 GB RAM cap)
        print("  [1/3] Phase 1: Write & Active NVMe Offload (350,000 unique 4KB keys = 1.43 GB)...")
        m_set = run_memtier(PORT, "1:0", requests_per_client=reqs_per_conn, key_pattern="P:P")
        print(f"        -> SET:     {m_set['ops_sec']:>10,.0f} ops/s | {m_set['kb_sec']/1024:>6.1f} MB/s | avg: {m_set['avg_latency_ms']:.2f} ms | p99: {m_set['p99_ms']:.2f} ms")

        if is_rudis:
            tier_info_1 = parse_info_dict(redis_cmd(PORT, "TIER", "INFO"))
        else:
            tier_info_1 = parse_info_dict(redis_cmd(PORT, "INFO", "ALL"))

        # 2. Phase 2: Cold & Hybrid Tiered Read (GET 0:1 across 350K keys)
        print("  [2/3] Phase 2: Tiered Read Under Memory Pressure (GET 4KB values across 350K keys)...")
        m_get = run_memtier(PORT, "0:1", test_time=10, key_pattern="R:R")
        print(f"        -> GET:     {m_get['ops_sec']:>10,.0f} ops/s | {m_get['kb_sec']/1024:>6.1f} MB/s | avg: {m_get['avg_latency_ms']:.2f} ms | p99: {m_get['p99_ms']:.2f} ms")

        # 3. Phase 3: Mixed Read/Write 1:1 Under Memory Ceiling
        print("  [3/3] Phase 3: Mixed Read/Write (1:1 SET/GET under memory ceiling)...")
        m_mix = run_memtier(PORT, "1:1", test_time=10, key_pattern="R:R")
        print(f"        -> MIX 1:1: {m_mix['ops_sec']:>10,.0f} ops/s | {m_mix['kb_sec']/1024:>6.1f} MB/s | avg: {m_mix['avg_latency_ms']:.2f} ms | p99: {m_mix['p99_ms']:.2f} ms")

        if is_rudis:
            tier_info_final = parse_info_dict(redis_cmd(PORT, "TIER", "INFO"))
        else:
            tier_info_final = parse_info_dict(redis_cmd(PORT, "INFO", "ALL"))

        return {
            "SET": m_set,
            "GET": m_get,
            "MIX": m_mix,
            "telemetry_after_set": tier_info_1,
            "telemetry_final": tier_info_final,
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        time.sleep(1.0)


def main():
    if os.path.exists(BENCH_BASE_DIR):
        shutil.rmtree(BENCH_BASE_DIR)
    os.makedirs(BENCH_BASE_DIR, exist_ok=True)

    dfly_dir = os.path.join(BENCH_BASE_DIR, "dragonfly")
    rudis_dir = os.path.join(BENCH_BASE_DIR, "rudis")
    os.makedirs(dfly_dir, exist_ok=True)
    os.makedirs(rudis_dir, exist_ok=True)

    results = {}

    # 1. Benchmark Dragonfly v1.39
    dfly_cmd = [
        "taskset", "-c", SERVER_CPUS,
        DRAGONFLY_BIN,
        f"--proactor_threads={THREADS}",
        f"--port={PORT}",
        f"--maxmemory={MAXMEMORY_MB}mb",
        "--dbfilename=",
        f"--tiered_prefix={os.path.join(dfly_dir, 'tier')}",
        "--tiered_offload_threshold=0.5",
        "--tiered_upload_threshold=0.2",
        "--tiered_experimental_cooling=true",
    ]
    results["Dragonfly"] = benchmark_engine("Dragonfly v1.39 (io_uring Tiered)", dfly_cmd, is_rudis=False)

    # Clean Dragonfly backing files before Rudis run
    shutil.rmtree(dfly_dir, ignore_errors=True)

    # 2. Benchmark Rudis
    rudis_env = os.environ.copy()
    rudis_env["RUDIS_TIER_DIR"] = rudis_dir
    rudis_cmd = [
        "taskset", "-c", SERVER_CPUS,
        RUDIS_BIN,
        "--threads", str(THREADS),
        "--port", str(PORT),
        "--maxmemory", f"{MAXMEMORY_MB}mb",
        "--tiered-offload-threshold", "50",
        "--tiered-upload-threshold", "80",
    ]
    results["Rudis"] = benchmark_engine("Rudis (io_uring + SmallBins + OpManager)", rudis_cmd, env=rudis_env, is_rudis=True)

    # Cleanup backing files
    shutil.rmtree(BENCH_BASE_DIR, ignore_errors=True)

    # Print comparison summary
    print("\n" + "=" * 95)
    print("  HEAD-TO-HEAD SUMMARY: NVME TIERED STORAGE UNDER MEMORY PRESSURE (1024 MB CAP)")
    print("=" * 95)
    print(f"{'Workload':<16} | {'Dragonfly (ops/s)':<18} | {'Rudis (ops/s)':<18} | {'Speedup':<16} | {'Rudis p99':<10}")
    print("-" * 95)
    for phase in ["SET", "GET", "MIX"]:
        d_ops = results["Dragonfly"][phase]["ops_sec"]
        r_ops = results["Rudis"][phase]["ops_sec"]
        r_p99 = results["Rudis"][phase]["p99_ms"]
        ratio = r_ops / d_ops if d_ops > 0 else 0.0
        pct = (ratio - 1.0) * 100.0
        print(f"{phase:<16} | {d_ops:>17,.0f} | {r_ops:>17,.0f} | {ratio:>5.2f}x ({pct:+.1f}%) | {r_p99:>6.2f} ms")
    print("=" * 95)

    out_path = os.path.join(REPO_DIR, "tiered_benchmark_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved full head-to-head results and telemetry to {out_path}")


if __name__ == "__main__":
    main()
