#!/usr/bin/env python3
"""
Rigorous comparative benchmark: Rudis vs Dragonfly on Geospatial Engine Workloads.
Compares:
  1. GEOADD (Write throughput: insertion, geohash encoding, zset indexing)
  2. GEODIST (Point-to-point distance calculations)
  3. GEORADIUS (Spatial circular query with geohash interval pruning)
  4. GEOSEARCH BYRADIUS (Redis 6.2+ spatial radius search)
  5. GEOSEARCH BYBOX (Redis 6.2+ spatial bounding box search)
"""

import json
import os
import re
import socket
import subprocess
import sys
import time

RUDIS_BIN = "/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
DRAGONFLY_BIN = "/usr/local/google/home/warrenzhu/dragonfly"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"

SERVER_CPUS = "0-7"
CLIENT_CPUS = "32-47"
CLIENT_THREADS = 8
CLIENT_CONNS = 8  # 64 concurrent client connections
PIPELINE = 16
DURATION = 10     # seconds per workload

def wait_for_port(port, timeout=5.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            return True
        except Exception:
            time.sleep(0.1)
    return False

def parse_memtier_output(output):
    """Parses Ops/sec, Avg Latency, p50, p99 from memtier_benchmark stdout."""
    res = {
        "ops_sec": 0.0,
        "avg_latency": 0.0,
        "p50_latency": 0.0,
        "p99_latency": 0.0,
    }
    for line in output.splitlines():
        if line.startswith("Totals") or line.startswith("Sets") or line.startswith("Gets"):
            parts = line.split()
            if len(parts) >= 8:
                try:
                    res["ops_sec"] = float(parts[1])
                    res["avg_latency"] = float(parts[4])
                    res["p50_latency"] = float(parts[5])
                    res["p99_latency"] = float(parts[6])
                    break
                except ValueError:
                    pass
    return res

def populate_geo_data(port, num_points=50000):
    """Populates spatial dataset around Mediterranean / Europe."""
    s = socket.create_connection(("127.0.0.1", port), timeout=10.0)
    rfile = s.makefile("rb")
    batch = []
    # Seed 50,000 locations spread across 10.0-18.0 Lon, 35.0-45.0 Lat
    for i in range(num_points):
        lon = 10.0 + (i % 800) * 0.01
        lat = 35.0 + (i // 800) * 0.015
        batch.append(f"GEOADD geo:points {lon:.6f} {lat:.6f} loc:{i}\r\n")
        if len(batch) >= 2000:
            s.sendall("".join(batch).encode())
            for _ in range(len(batch)):
                rfile.readline()
            batch = []
    if batch:
        s.sendall("".join(batch).encode())
        for _ in range(len(batch)):
            rfile.readline()
    s.close()

def run_memtier_test(port, cmd_str, key_min=1, key_max=50000, pipeline=16):
    args = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "--server", "127.0.0.1",
        "--port", str(port),
        "--protocol", "redis",
        "--threads", str(CLIENT_THREADS),
        "--clients", str(CLIENT_CONNS),
        "--pipeline", str(pipeline),
        "--test-time", str(DURATION),
        "--command", cmd_str,
        "--command-ratio", "1",
        "--command-key-pattern", "R",
        "--key-minimum", str(key_min),
        "--key-maximum", str(key_max),
        "--hide-histogram",
    ]
    p = subprocess.run(args, capture_output=True, text=True)
    return parse_memtier_output(p.stdout)

def benchmark_engine(name, start_cmd, port):
    print(f"\n>>> Starting {name} on port {port}...")
    proc = subprocess.Popen(start_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_for_port(port):
        print(f"Failed to start {name} on port {port}")
        proc.kill()
        return None

    print(f"  Populating 50,000 geospatial points into {name}...")
    populate_geo_data(port, num_points=50000)
    time.sleep(1)

    results = {}

    # 1. GEOADD
    print(f"  [1/5] Benchmarking GEOADD (pipeline {PIPELINE})...")
    geoadd_cmd = "GEOADD geo:stream 13.361389 38.115556 pt:__key__"
    results["GEOADD"] = run_memtier_test(port, geoadd_cmd, pipeline=PIPELINE)
    print(f"    -> {results['GEOADD']['ops_sec']:,.0f} ops/sec, avg: {results['GEOADD']['avg_latency']:.3f} ms, p99: {results['GEOADD']['p99_latency']:.3f} ms")

    # 2. GEODIST
    print(f"  [2/5] Benchmarking GEODIST (pipeline {PIPELINE})...")
    geodist_cmd = "GEODIST geo:points loc:__key__ loc:1 km"
    results["GEODIST"] = run_memtier_test(port, geodist_cmd, pipeline=PIPELINE)
    print(f"    -> {results['GEODIST']['ops_sec']:,.0f} ops/sec, avg: {results['GEODIST']['avg_latency']:.3f} ms, p99: {results['GEODIST']['p99_latency']:.3f} ms")

    # 3. GEORADIUS (Spatial Range Query)
    print(f"  [3/5] Benchmarking GEORADIUS (50km spatial radius, COUNT 10, pipeline {PIPELINE})...")
    georadius_cmd = "GEORADIUS geo:points 13.361389 38.115556 50 km WITHDIST COUNT 10"
    results["GEORADIUS"] = run_memtier_test(port, georadius_cmd, pipeline=PIPELINE)
    print(f"    -> {results['GEORADIUS']['ops_sec']:,.0f} ops/sec, avg: {results['GEORADIUS']['avg_latency']:.3f} ms, p99: {results['GEORADIUS']['p99_latency']:.3f} ms")

    # 4. GEOSEARCH BYRADIUS
    print(f"  [4/5] Benchmarking GEOSEARCH BYRADIUS (50km radius, COUNT 10, pipeline {PIPELINE})...")
    geosearch_rad_cmd = "GEOSEARCH geo:points FROMLONLAT 13.361389 38.115556 BYRADIUS 50 km WITHDIST COUNT 10"
    results["GEOSEARCH_RADIUS"] = run_memtier_test(port, geosearch_rad_cmd, pipeline=PIPELINE)
    print(f"    -> {results['GEOSEARCH_RADIUS']['ops_sec']:,.0f} ops/sec, avg: {results['GEOSEARCH_RADIUS']['avg_latency']:.3f} ms, p99: {results['GEOSEARCH_RADIUS']['p99_latency']:.3f} ms")

    # 5. GEOSEARCH BYBOX
    print(f"  [5/5] Benchmarking GEOSEARCH BYBOX (100km x 100km box, COUNT 10, pipeline {PIPELINE})...")
    geosearch_box_cmd = "GEOSEARCH geo:points FROMLONLAT 13.361389 38.115556 BYBOX 100 100 km WITHDIST COUNT 10"
    results["GEOSEARCH_BOX"] = run_memtier_test(port, geosearch_box_cmd, pipeline=PIPELINE)
    print(f"    -> {results['GEOSEARCH_BOX']['ops_sec']:,.0f} ops/sec, avg: {results['GEOSEARCH_BOX']['avg_latency']:.3f} ms, p99: {results['GEOSEARCH_BOX']['p99_latency']:.3f} ms")

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
    time.sleep(1)

    return results

def main():
    print("=" * 80)
    print("      GEOSPATIAL BENCHMARK: RUDIS VS. DRAGONFLY (8 CORES, PIPELINE 16)")
    print("=" * 80)

    # 1. Benchmark Dragonfly
    df_cmd = [
        "taskset", "-c", SERVER_CPUS,
        DRAGONFLY_BIN,
        "--port", "6381",
        "--proactor_threads=8",
        "--cache_mode=false",
        "--dbfilename=",
    ]
    df_results = benchmark_engine("Dragonfly v1.39.0", df_cmd, 6381)

    # 2. Benchmark Rudis
    rudis_cmd = [
        "taskset", "-c", SERVER_CPUS,
        RUDIS_BIN,
        "--threads", "8",
        "--port", "6379",
    ]
    rudis_results = benchmark_engine("Rudis v0.1.0", rudis_cmd, 6379)

    # 3. Print Final Comparison Table
    print("\n" + "=" * 90)
    print("                      FINAL GEOSPATIAL COMPARISON RESULTS")
    print("=" * 90)
    header = f"{'Workload':<20} | {'Metric':<14} | {'Dragonfly v1.39':<18} | {'Rudis v0.1.0':<18} | {'Delta (%)':<12}"
    print(header)
    print("-" * 90)

    workloads = [
        ("GEOADD", "GEOADD (Write)"),
        ("GEODIST", "GEODIST (Distance)"),
        ("GEORADIUS", "GEORADIUS (50km)"),
        ("GEOSEARCH_RADIUS", "GEOSEARCH Radius"),
        ("GEOSEARCH_BOX", "GEOSEARCH Box"),
    ]

    out_data = {
        "dragonfly": df_results,
        "rudis": rudis_results,
    }

    for key, display_name in workloads:
        df_w = df_results[key]
        ru_w = rudis_results[key]

        df_ops = df_w["ops_sec"]
        ru_ops = ru_w["ops_sec"]
        delta_pct = ((ru_ops - df_ops) / df_ops) * 100.0 if df_ops > 0 else 0.0
        delta_str = f"{delta_pct:+.1f}%"

        print(f"{display_name:<20} | {'Throughput':<14} | {df_ops:>14,.0f} ops/s | {ru_ops:>14,.0f} ops/s | {delta_str:>10}")
        print(f"{'':<20} | {'Avg Latency':<14} | {df_w['avg_latency']:>15.3f} ms | {ru_w['avg_latency']:>15.3f} ms | {'':>10}")
        print(f"{'':<20} | {'p99 Latency':<14} | {df_w['p99_latency']:>15.3f} ms | {ru_w['p99_latency']:>15.3f} ms | {'':>10}")
        print("-" * 90)

    # Save results to json
    with open("benchmark_logs/geo_rudis_vs_dragonfly.json", "w") as f:
        json.dump(out_data, f, indent=2)
    print("\nDetailed results written to benchmark_logs/geo_rudis_vs_dragonfly.json")

if __name__ == "__main__":
    main()
