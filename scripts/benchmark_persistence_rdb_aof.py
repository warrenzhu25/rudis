#!/usr/bin/env python3
"""
Persistence Benchmark Suite: Rudis vs. Dragonfly
Measures Throughput, Latency Distribution, Memory Footprint (RSS / Peak VmHWM),
and CPU Utilization across:
  1. Pure In-Memory Baseline (No Persistence)
  2. Live Background RDB Snapshotting (BGSAVE under active load)
  3. Real-Time AOF Logging (Append-Only File streaming)
  4. Live AOF Compaction (BGREWRITEAOF under active load)
"""

import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time

RUDIS_BIN = "/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
DRAGONFLY_BIN = "/usr/local/google/home/warrenzhu/dragonfly"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"

SERVER_CPUS = "16-31"  # 16 physical cores
CLIENT_CPUS = "0-15"   # 16 disjoint physical cores
PORT = 6395
CORES = 16

KEY_COUNT = 150_000
DATA_SIZE = 1024  # 1KB per key (~180-220MB dataset in RAM)
TEST_TIME_SECS = 10
CLIENT_THREADS = 16
CLIENT_CONNS = 4


def get_proc_memory_kb(pid):
    """Returns (VmRSS, VmHWM) in KB."""
    rss, hwm = 0, 0
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                elif line.startswith("VmHWM:"):
                    hwm = int(line.split()[1])
    except Exception:
        pass
    return rss, hwm


def get_proc_cpu_ticks(pid):
    """Returns (utime, stime) in clock ticks."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split()
            return int(fields[13]), int(fields[14])
    except Exception:
        return 0, 0


def wait_ping(port, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
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


def send_cmd(port, cmd_bytes):
    s = socket.create_connection(("127.0.0.1", port), timeout=3.0)
    s.sendall(cmd_bytes)
    resp = s.recv(4096)
    s.close()
    return resp


def populate_dataset(port):
    """Populates KEY_COUNT 1KB keys."""
    print(f"    [*] Pre-populating {KEY_COUNT:,} 1KB keys...")
    cmd = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", "1",
        "-c", "1",
        "-n", str(KEY_COUNT),
        "--key-maximum", str(KEY_COUNT),
        "--key-minimum", "1",
        "--pipeline", "64",
        "--hide-histogram",
        "--ratio", "1:0",
        "-d", str(DATA_SIZE),
        "--key-pattern", "S:S",
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)


def run_benchmark_run(port, trigger_fn=None):
    """Runs a 10s SET workload via memtier while optionally executing a persistence trigger."""
    json_path = f"/tmp/bench_persist_{port}_{int(time.time()*1000)}.json"
    cmd = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", str(CLIENT_THREADS),
        "-c", str(CLIENT_CONNS),
        "--test-time", str(TEST_TIME_SECS),
        "--pipeline", "16",
        "--ratio", "1:0",
        "-d", str(DATA_SIZE),
        "--key-pattern", "R:R",
        "--key-maximum", str(KEY_COUNT),
        "--hide-histogram",
        "--json-out-file", json_path,
    ]

    memtier_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)

    # If a persistence trigger is requested (e.g. BGSAVE or BGREWRITEAOF), fire it after 2 seconds of traffic
    trigger_res = None
    if trigger_fn:
        time.sleep(2.0)
        t_start = time.time()
        trigger_res = trigger_fn(port)
        trigger_res["elapsed_sec"] = time.time() - t_start

    _, stderr = memtier_proc.communicate()
    if not os.path.exists(json_path):
        print(f"    [!] Error running memtier: {stderr[:200]}")
        return None, trigger_res

    with open(json_path, "r") as f:
        data = json.load(f)
    os.remove(json_path)

    totals = data["ALL STATS"]["Totals"]
    percentiles = totals.get("Percentile Latencies", {})
    metrics = {
        "ops_sec": float(totals.get("Ops/sec", 0.0)),
        "avg_lat": float(totals.get("Latency", 0.0)),
        "p50": float(percentiles.get("50.000", totals.get("Latency", 0.0))),
        "p99": float(percentiles.get("99.000", totals.get("Latency", 0.0))),
        "p99_9": float(percentiles.get("99.900", totals.get("Latency", 0.0))),
    }
    return metrics, trigger_res


def trigger_rudis_bgsave(port):
    send_cmd(port, b"*1\r\n$6\r\nBGSAVE\r\n")
    # Poll until bgsave completes
    while True:
        time.sleep(0.1)
        resp = send_cmd(port, b"*2\r\n$4\r\nINFO\r\n$11\r\npersistence\r\n")
        if b"rdb_bgsave_in_progress:0" in resp:
            break
    rdb_file = f"/tmp/rudis_persist_{port}/dump.rdb"
    size_mb = (os.path.getsize(rdb_file) / (1024 * 1024)) if os.path.exists(rdb_file) else 0.0
    return {"size_mb": size_mb, "type": "RDB"}


def trigger_dfly_bgsave(port):
    send_cmd(port, b"*1\r\n$6\r\nBGSAVE\r\n")
    # Poll until bgsave completes by checking LASTSAVE
    t0 = send_cmd(port, b"*1\r\n$8\r\nLASTSAVE\r\n")
    while True:
        time.sleep(0.1)
        t1 = send_cmd(port, b"*1\r\n$8\r\nLASTSAVE\r\n")
        if t1 != t0:
            break
    rdb_file = f"/tmp/dfly_persist_{port}/dump.rdb"
    size_mb = (os.path.getsize(rdb_file) / (1024 * 1024)) if os.path.exists(rdb_file) else 0.0
    return {"size_mb": size_mb, "type": "RDB"}


def trigger_rudis_bgrewriteaof(port):
    send_cmd(port, b"*1\r\n$12\r\nBGREWRITEAOF\r\n")
    # Poll until rewrite completes
    time.sleep(1.0)
    aof_dir = f"/tmp/rudis_persist_{port}"
    total_size = sum(
        os.path.getsize(os.path.join(aof_dir, f))
        for f in os.listdir(aof_dir)
        if f.endswith(".aof")
    ) if os.path.exists(aof_dir) else 0
    return {"size_mb": total_size / (1024 * 1024), "type": "AOF-Rewrite"}


def benchmark_engine(name, launch_cmd, work_dir, trigger_fn=None):
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(work_dir, exist_ok=True)

    proc = subprocess.Popen(launch_cmd, cwd=work_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_ping(PORT):
        print(f"Failed to start {name} on port {PORT}")
        proc.kill()
        return None

    pid = proc.pid
    populate_dataset(PORT)
    time.sleep(1.0)

    rss_baseline, _ = get_proc_memory_kb(pid)
    cpu_u0, cpu_s0 = get_proc_cpu_ticks(pid)
    t0 = time.time()

    print(f"    [*] Measuring 10s steady-state SET load on {name}...")
    metrics, trigger_info = run_benchmark_run(PORT, trigger_fn)

    elapsed_wall = time.time() - t0
    cpu_u1, cpu_s1 = get_proc_cpu_ticks(pid)
    rss_end, hwm_peak = get_proc_memory_kb(pid)

    # Clean up
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()

    if not metrics:
        return None

    # Calculate CPU utilization (clock ticks / ticks_per_sec / wall_sec)
    clk_tck = os.sysconf("SC_CLK_TCK")
    cpu_user_sec = (cpu_u1 - cpu_u0) / clk_tck
    cpu_sys_sec = (cpu_s1 - cpu_s0) / clk_tck
    total_cpu_sec = cpu_user_sec + cpu_sys_sec
    cores_used = total_cpu_sec / elapsed_wall if elapsed_wall else 0.0

    return {
        "ops_sec": metrics["ops_sec"],
        "avg_lat": metrics["avg_lat"],
        "p50": metrics["p50"],
        "p99": metrics["p99"],
        "p99_9": metrics["p99_9"],
        "baseline_rss_mb": rss_baseline / 1024.0,
        "peak_rss_mb": hwm_peak / 1024.0,
        "delta_rss_mb": (hwm_peak - rss_baseline) / 1024.0,
        "cpu_user_pct": (cpu_user_sec / elapsed_wall * 100) if elapsed_wall else 0.0,
        "cpu_sys_pct": (cpu_sys_sec / elapsed_wall * 100) if elapsed_wall else 0.0,
        "cores_used": cores_used,
        "trigger": trigger_info,
    }


def main():
    print("=" * 85)
    print("  PERSISTENCE BENCHMARK: RUDIS VS DRAGONFLY ON 16 CORES")
    print(f"  CPUs: Server [{SERVER_CPUS}], Client [{CLIENT_CPUS}] | Dataset: {KEY_COUNT:,} 1KB keys")
    print("=" * 85)

    results = {}

    # 1. In-Memory Baseline (No Persistence)
    print("\n--- 1. IN-MEMORY BASELINE (NO PERSISTENCE) ---")
    print(">>> Testing Dragonfly (In-Memory)...")
    df_base = benchmark_engine(
        "Dragonfly",
        [
            "taskset", "-c", SERVER_CPUS,
            DRAGONFLY_BIN,
            "--port", str(PORT),
            f"--proactor_threads={CORES}",
            "--cache_mode=false",
            "--dbfilename=",
        ],
        f"/tmp/dfly_persist_{PORT}",
    )
    results["Dragonfly_Base"] = df_base

    print(">>> Testing Rudis (In-Memory)...")
    rudis_base = benchmark_engine(
        "Rudis",
        [
            "taskset", "-c", SERVER_CPUS,
            RUDIS_BIN,
            "--port", str(PORT),
            "--threads", str(CORES),
        ],
        f"/tmp/rudis_persist_{PORT}",
    )
    results["Rudis_Base"] = rudis_base

    # 2. Live BGSAVE Snapshot under Active Write Traffic
    print("\n--- 2. LIVE BGSAVE SNAPSHOT (UNDER LOAD) ---")
    print(">>> Testing Dragonfly BGSAVE...")
    df_bgsave = benchmark_engine(
        "Dragonfly",
        [
            "taskset", "-c", SERVER_CPUS,
            DRAGONFLY_BIN,
            "--port", str(PORT),
            f"--proactor_threads={CORES}",
            "--cache_mode=false",
            "--nodf_snapshot_format",
            "--dbfilename=dump.rdb",
            f"--dir=/tmp/dfly_persist_{PORT}",
        ],
        f"/tmp/dfly_persist_{PORT}",
        trigger_fn=trigger_dfly_bgsave,
    )
    results["Dragonfly_BGSAVE"] = df_bgsave

    print(">>> Testing Rudis BGSAVE...")
    rudis_bgsave = benchmark_engine(
        "Rudis",
        [
            "taskset", "-c", SERVER_CPUS,
            RUDIS_BIN,
            "--port", str(PORT),
            "--threads", str(CORES),
        ],
        f"/tmp/rudis_persist_{PORT}",
        trigger_fn=trigger_rudis_bgsave,
    )
    results["Rudis_BGSAVE"] = rudis_bgsave

    # 3. Real-Time AOF Persistence (Rudis)
    print("\n--- 3. REAL-TIME AOF WRITE STREAMING ---")
    print(">>> Testing Rudis AOF Streaming...")
    rudis_aof = benchmark_engine(
        "Rudis",
        [
            "taskset", "-c", SERVER_CPUS,
            RUDIS_BIN,
            "--port", str(PORT),
            "--threads", str(CORES),
            "--aof", "true",
            "--aof-dir", f"/tmp/rudis_persist_{PORT}",
        ],
        f"/tmp/rudis_persist_{PORT}",
    )
    results["Rudis_AOF"] = rudis_aof

    # 4. Live AOF Rewrite (BGREWRITEAOF under Active Write Traffic)
    print("\n--- 4. LIVE AOF REWRITE / COMPACTION (UNDER LOAD) ---")
    print(">>> Testing Rudis BGREWRITEAOF...")
    rudis_bgrewrite = benchmark_engine(
        "Rudis",
        [
            "taskset", "-c", SERVER_CPUS,
            RUDIS_BIN,
            "--port", str(PORT),
            "--threads", str(CORES),
            "--aof", "true",
            "--aof-dir", f"/tmp/rudis_persist_{PORT}",
        ],
        f"/tmp/rudis_persist_{PORT}",
        trigger_fn=trigger_rudis_bgrewriteaof,
    )
    results["Rudis_BGREWRITEAOF"] = rudis_bgrewrite

    # Summary Report Table
    print("\n" + "=" * 115)
    print("                                PERSISTENCE BENCHMARK SUMMARY (16 CORES)")
    print("=" * 115)
    header = f"{'Engine & Mode':<26} | {'Throughput':>14} | {'p50 Lat':>8} | {'p99 Lat':>8} | {'Base RSS':>9} | {'Peak RSS':>9} | {'CPU Cores':>9} | {'Snapshot':>12}"
    print(header)
    print("-" * 115)

    for mode, data in results.items():
        if not data:
            print(f"{mode:<26} | {'FAILED':>14}")
            continue
        snap_str = "None"
        if data.get("trigger"):
            t = data["trigger"]
            snap_str = f"{t.get('size_mb', 0):.1f}MB/{t.get('elapsed_sec', 0):.2f}s"

        row = (
            f"{mode:<26} | "
            f"{data['ops_sec']:>12,.0f}/s | "
            f"{data['p50']:>6.2f}ms | "
            f"{data['p99']:>6.2f}ms | "
            f"{data['baseline_rss_mb']:>7.1f}MB | "
            f"{data['peak_rss_mb']:>7.1f}MB | "
            f"{data['cores_used']:>8.1f}c | "
            f"{snap_str:>12}"
        )
        print(row)
    print("=" * 115)

    # Save to JSON
    with open("benchmark_persistence_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\n[+] Full results written to benchmark_persistence_results.json")


if __name__ == "__main__":
    main()
