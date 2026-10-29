#!/usr/bin/env python3
"""
Multi-Core Benchmark Suite: Rudis vs. Dragonfly v1.39 vs. Valkey 8.1 (1 to 16 Physical Cores).

Hardware topology on this 64-vCPU AMD EPYC 7B13 host (`lscpu -e`):
  - 32 physical cores (CORE 0..31), 2 SMT threads per core:
      * Physical core i (0..15, L3 CCX 0-1) -> vCPUs i and i+32
      * Physical core i (16..31, L3 CCX 2-3) -> vCPUs i and i+32
  - To guarantee zero physical-core, L1/L2, and L3 CCX contention between the
    datastore and the load generator, the server is pinned to physical cores
    0..15 (leaving their SMT siblings 32..47 idle) and `memtier_benchmark` is
    pinned to physical cores 16..31 (vCPUs 16-31,48-63 on CCX 2-3).

Measures (3 interleaved rounds per workload, reporting medians):
  1. Head-to-Head Comparison at 8 and 16 Physical Cores (Rudis vs. Dragonfly v1.39 vs. Valkey 8.1):
     - SET 100% (Pipeline 16, 1KB)
     - GET 100% (Pipeline 16, 1KB)
     - SET 100% Unpipelined (Pipeline 1, 1KB)
     - GET 100% Unpipelined (Pipeline 1, 1KB)
     - MGET 10-Key Scattered (Pipeline 8, 100B)
     - DEL 10-Key Scattered (Pipeline 8, 100B)
  2. Core Scaling Curve (1, 2, 4, 8, 16 Physical Cores) for SET (P16, 1KB) and GET (P16, 1KB).
"""

import json
import os
import socket
import statistics
import subprocess
import time

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUDIS_BIN = os.environ.get("RUDIS_BIN", os.path.join(REPO_DIR, "target", "release", "rudis"))
DRAGONFLY_BIN = os.environ.get("DRAGONFLY_BIN", "/usr/local/google/home/warrenzhu/dragonfly")
VALKEY_BIN = os.environ.get("VALKEY_BIN", "/usr/local/google/home/warrenzhu/valkey-stable/src/valkey-server")
MEMTIER_BIN = os.environ.get("MEMTIER_BIN", "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark")

# Disjoint physical cores (CCX 2 & 3) so server cores 0..15 (CCX 0 & 1, SMT siblings 32..47 idle)
# experience zero SMT or L1/L2/L3 cache interference from memtier_benchmark.
CLIENT_CPUS = "16-31,48-63"
CLIENT_THREADS = 16
CLIENT_CONNS = 4  # 64 concurrent client connections
ITERATIONS = int(os.environ.get("BENCH_ITERS", "3"))
TEST_TIME = int(os.environ.get("BENCH_SECS", "5"))

WORKLOADS = [
    {
        "id": "set_p16",
        "name": "SET 100% (Pipeline 16, 1KB)",
        "pipeline": 16,
        "data_size": 1024,
        "ratio": "1:0",
        "needs_populate": False,
        "command": None,
    },
    {
        "id": "get_p16",
        "name": "GET 100% (Pipeline 16, 1KB)",
        "pipeline": 16,
        "data_size": 1024,
        "ratio": "0:1",
        "needs_populate": True,
        "command": None,
    },
    {
        "id": "set_p1",
        "name": "SET 100% (Unpipelined P1, 1KB)",
        "pipeline": 1,
        "data_size": 1024,
        "ratio": "1:0",
        "needs_populate": False,
        "command": None,
    },
    {
        "id": "get_p1",
        "name": "GET 100% (Unpipelined P1, 1KB)",
        "pipeline": 1,
        "data_size": 1024,
        "ratio": "0:1",
        "needs_populate": True,
        "command": None,
    },
    {
        "id": "mget_10k",
        "name": "MGET 10-Key Scattered (P8)",
        "pipeline": 8,
        "data_size": 100,
        "ratio": None,
        "needs_populate": True,
        "command": "MGET " + " ".join("__key__" for _ in range(10)),
    },
    {
        "id": "del_10k",
        "name": "DEL 10-Key Scattered (P8)",
        "pipeline": 8,
        "data_size": 100,
        "ratio": None,
        "needs_populate": True,
        "command": "DEL " + " ".join("__key__" for _ in range(10)),
    },
]


def wait_ping(port, timeout=10.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                resp = s.recv(1024)
                if b"+PONG" in resp:
                    return True
        except Exception:
            time.sleep(0.1)
    return False


def flushall(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2.0) as s:
            s.sendall(b"*1\r\n$8\r\nFLUSHALL\r\n")
            s.recv(1024)
    except Exception as e:
        print(f"Flushall error on port {port}: {e}")


def populate_keyspace(port, num_keys=64000, data_size=1024):
    json_tmp = f"/tmp/pop_{port}.json"
    cmd = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", "16",
        "-c", "4",
        "-n", str(num_keys // 64),
        "--ratio", "1:0",
        "-d", str(data_size),
        "--key-pattern", "P:P",
        "--key-minimum", "1",
        "--key-maximum", str(num_keys),
        "--pipeline", "16",
        "--hide-histogram",
        "--json-out-file", json_tmp,
    ]
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
        "--test-time", str(TEST_TIME),
        "--pipeline", str(workload["pipeline"]),
        "--key-minimum", "1",
        "--key-maximum", "64000",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
        "--json-out-file", json_path,
    ]

    if workload.get("data_size") and not workload.get("command"):
        cmd.extend(["-d", str(workload["data_size"])])

    if workload.get("ratio"):
        cmd.extend(["--ratio", workload["ratio"], "--key-pattern", "R:R"])

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
        pcts = totals.get("Percentile Latencies", {})
        avg_lat = float(totals.get("Average Latency", totals.get("Latency", 0.0)))
        return {
            "ops_sec": float(totals.get("Ops/sec", 0.0)),
            "avg_latency": avg_lat,
            "p50": float(pcts.get("p50.00", pcts.get("50.000", avg_lat))),
            "p99": float(pcts.get("p99.00", pcts.get("99.000", avg_lat))),
            "p99_9": float(pcts.get("p99.90", pcts.get("99.900", avg_lat))),
        }
    except Exception as e:
        print(f"Error parsing {json_path}: {e}")
        return None


def start_engine(engine, cores, port):
    server_cpus = f"0-{cores - 1}" if cores > 1 else "0"
    if engine == "Rudis":
        cmd = [
            "taskset", "-c", server_cpus,
            RUDIS_BIN,
            "--port", str(port),
            "--threads", str(cores),
            "--bind", "127.0.0.1",
        ]
    elif engine == "Dragonfly":
        cmd = [
            "taskset", "-c", server_cpus,
            DRAGONFLY_BIN,
            "--port", str(port),
            f"--proactor_threads={cores}",
            "--bind", "127.0.0.1",
            "--cache_mode=false",
            "--dbfilename=",
            "--logtostderr=false",
        ]
    elif engine == "Valkey":
        io_threads = min(cores, 16)
        cmd = [
            "taskset", "-c", server_cpus,
            VALKEY_BIN,
            "--port", str(port),
            "--bind", "127.0.0.1",
            "--protected-mode", "no",
            "--save", "",
            "--appendonly", "no",
            "--io-threads", str(io_threads),
            "--io-threads-do-reads", "yes" if io_threads > 1 else "no",
        ]
    else:
        raise ValueError(f"Unknown engine {engine}")

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_ping(port):
        proc.kill()
        return None
    return proc


def stop_engine(proc, port):
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()
    tier_dir = f"rudis_tier_{port}"
    if os.path.exists(tier_dir):
        subprocess.run(["rm", "-rf", tier_dir])
    time.sleep(0.3)


def benchmark_at_core_count(cores):
    server_cpus = f"0-{cores - 1}" if cores > 1 else "0"
    print(f"\n=================================================================")
    print(f"  INTERLEAVED BENCHMARK ON {cores} PHYSICAL CORES (CPUs {server_cpus})")
    print(f"=================================================================")

    engines = ["Rudis", "Dragonfly"]
    if os.path.exists(VALKEY_BIN):
        engines.append("Valkey")

    ports = {"Rudis": 16379, "Dragonfly": 16381, "Valkey": 16383}
    raw_runs = {eng: {wl["id"]: [] for wl in WORKLOADS} for eng in engines}

    for r in range(ITERATIONS):
        print(f"\n--- Interleaved Round {r + 1}/{ITERATIONS} ({cores} cores) ---")
        round_engines = engines if r % 2 == 0 else list(reversed(engines))
        for eng in round_engines:
            port = ports[eng]
            proc = start_engine(eng, cores, port)
            if not proc:
                print(f"  [!] Failed to start {eng}")
                continue
            try:
                for wl in WORKLOADS:
                    flushall(port)
                    if wl.get("needs_populate"):
                        populate_keyspace(port, 64000, wl.get("data_size") or 100)
                    time.sleep(0.2)
                    m = run_single_memtier(port, wl, r)
                    if m:
                        raw_runs[eng][wl["id"]].append(m)
                        print(
                            f"  [{eng:<9} | {wl['id']:<8}] {m['ops_sec']:>11,.0f} ops/s | "
                            f"avg {m['avg_latency']:.2f}ms | p99 {m['p99']:.2f}ms | p99.9 {m['p99_9']:.2f}ms"
                        )
            finally:
                stop_engine(proc, port)

    results = {}
    for eng in engines:
        results[eng] = {}
        for wl in WORKLOADS:
            runs = raw_runs[eng][wl["id"]]
            if not runs:
                continue
            ops_list = [x["ops_sec"] for x in runs]
            avg_list = [x["avg_latency"] for x in runs]
            p50_list = [x["p50"] for x in runs]
            p99_list = [x["p99"] for x in runs]
            p999_list = [x["p99_9"] for x in runs]
            med_ops = statistics.median(ops_list)
            cv_pct = (statistics.stdev(ops_list) / med_ops * 100.0) if len(ops_list) > 1 and med_ops > 0 else 0.0
            results[eng][wl["id"]] = {
                "ops_sec_median": round(med_ops, 2),
                "ops_sec_mean": round(statistics.mean(ops_list), 2),
                "cv_pct": round(cv_pct, 2),
                "avg_latency_median": round(statistics.median(avg_list), 3),
                "p50_latency_median": round(statistics.median(p50_list), 3),
                "p99_latency_median": round(statistics.median(p99_list), 3),
                "p999_latency_median": round(statistics.median(p999_list), 3),
            }
    return results


def run_engine_scaling(engine="Rudis"):
    print(f"\n=================================================================")
    print(f"       {engine.upper()} CORE SCALING PROFILE (1, 2, 4, 8, 16 CORES)         ")
    print(f"=================================================================")
    scaling_data = {}
    cores_list = [1, 2, 4, 8, 16]
    set_wl = WORKLOADS[0]
    get_wl = WORKLOADS[1]
    port = 16379 if engine == "Rudis" else 16381

    for c in cores_list:
        proc = start_engine(engine, c, port)
        if not proc:
            print(f"Failed to start {engine} with {c} cores")
            continue
        try:
            set_runs = []
            get_runs = []
            for r in range(ITERATIONS):
                flushall(port)
                time.sleep(0.2)
                ms = run_single_memtier(port, set_wl, r)
                if ms:
                    set_runs.append(ms)
                populate_keyspace(port, 64000, 1024)
                time.sleep(0.2)
                mg = run_single_memtier(port, get_wl, r)
                if mg:
                    get_runs.append(mg)
            if set_runs and get_runs:
                set_med = statistics.median([x["ops_sec"] for x in set_runs])
                get_med = statistics.median([x["ops_sec"] for x in get_runs])
                set_p99 = statistics.median([x["p99"] for x in set_runs])
                get_p99 = statistics.median([x["p99"] for x in get_runs])
                scaling_data[str(c)] = {
                    "set_ops_sec": round(set_med, 2),
                    "get_ops_sec": round(get_med, 2),
                    "set_p99_ms": round(set_p99, 3),
                    "get_p99_ms": round(get_p99, 3),
                }
                print(
                    f"  {engine:<9} {c:2d} Core(s): SET {set_med:>11,.0f} ops/s (p99 {set_p99:.2f}ms) | "
                    f"GET {get_med:>11,.0f} ops/s (p99 {get_p99:.2f}ms)"
                )
        finally:
            stop_engine(proc, port)

    return scaling_data


def generate_markdown(results, out_path):
    lines = [
        "# Multi-Core Benchmark Report: Rudis vs. Dragonfly v1.39 vs. Valkey 8.1 (1–16 Physical Cores)",
        "",
        "**Hardware Platform:** AMD EPYC 7B13 (64 logical vCPUs / 32 physical cores across 4 L3 CCXs, 128 MiB L3 cache)",
        "**Host Partitioning:** Dedicated Server Physical Cores `0-15` (CCX 0–1, SMT siblings `32-47` idle) | Dedicated Client Physical Cores `16-31` (`16-31,48-63` on CCX 2–3 — zero physical-core or L3 overlap)",
        f"**Client Setup:** `memtier_benchmark`, {CLIENT_THREADS} client threads × {CLIENT_CONNS} connections/thread = {CLIENT_THREADS * CLIENT_CONNS} concurrent connections, {ITERATIONS} interleaved rounds × {TEST_TIME}s per workload (reporting medians)",
        "",
        "This report is generated by [`scripts/benchmark_multicore_suite.py`](../scripts/benchmark_multicore_suite.py), which writes both this Markdown file and [`benchmark_multicore_results.json`](benchmarks/results/benchmark_multicore_results.json).",
        "",
        "## 1. Methodology & Topology Note (Why 1–16 Physical Cores)",
        "",
        "* **SMT & CCX Isolation**: `lscpu -e` shows that this 64-vCPU VM has 32 physical cores (`CORE 0..31`), where vCPU `i` and vCPU `i+32` are SMT siblings sharing the same physical core and L1/L2 cache. Running a 32-thread server on `0-31` and a 32-thread `memtier_benchmark` client on `32-63` places a client thread on the SMT sibling of every single server core. Restricting the server to physical cores `0-15` (with `32-47` left idle) and pinning the client to `16-31,48-63` isolates server and client onto disjoint physical cores and disjoint L3 cache complexes (CCX 0–1 vs. CCX 2–3).",
        "* **Interleaved Rounds**: In each round, engines are benchmarked back-to-back in alternating order (`Rudis -> Dragonfly -> Valkey`, then reversed) so any host noise affects all engines equally.",
        "* **Multi-Key Placeholder Fix**: Scattered 10-key `MGET` and `DEL` use ten `__key__` placeholders (`--command-key-pattern=R`) so `memtier_benchmark` generates 10 independent random keys per command across all shards.",
        "",
        "---",
    ]

    workload_order = [
        ("set_p16", "SET 100% (Pipeline 16, 1KB)"),
        ("get_p16", "GET 100% (Pipeline 16, 1KB)"),
        ("set_p1", "SET 100% (Unpipelined P1, 1KB)"),
        ("get_p1", "GET 100% (Unpipelined P1, 1KB)"),
        ("mget_10k", "MGET 10-Key Scattered (P8)"),
        ("del_10k", "DEL 10-Key Scattered (P8)"),
    ]

    for sec_idx, (core_key, core_title) in enumerate([("8_cores", "8 Physical Cores"), ("16_cores", "16 Physical Cores")], start=2):
        rc = results.get(core_key, {})
        lines.extend([
            "",
            f"## {sec_idx}. Head-to-Head Comparison: {core_title}",
            "",
            "| Workload | Rudis | Dragonfly v1.39 | Valkey 8.1 | Rudis vs. Dragonfly | Rudis p99 | Dragonfly p99 |",
            "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
        ])
        for wid, label in workload_order:
            r_m = rc.get("Rudis", {}).get(wid, {})
            d_m = rc.get("Dragonfly", {}).get(wid, {})
            v_m = rc.get("Valkey", {}).get(wid, {})
            r_ops = r_m.get("ops_sec_median", 0.0)
            d_ops = d_m.get("ops_sec_median", 0.0)
            v_ops = v_m.get("ops_sec_median", 0.0)
            r_p99 = r_m.get("p99_latency_median", 0.0)
            d_p99 = d_m.get("p99_latency_median", 0.0)
            adv = f"{((r_ops / d_ops) - 1.0) * 100:+.1f}%" if d_ops > 0 else "N/A"
            v_str = f"{v_ops:,.0f} ops/s" if v_ops > 0 else "N/A"
            lines.append(
                f"| **{label}** | **{r_ops:,.0f} ops/s** | {d_ops:,.0f} ops/s | {v_str} | **{adv}** | **{r_p99:.2f} ms** | {d_p99:.2f} ms |"
            )
        lines.append("")
        lines.append("---")

    lines.extend([
        "",
        "## 4. Physical Core Scaling Profile (1 to 16 Cores: Rudis vs. Dragonfly v1.39)",
        "",
        "### Rudis (1 to 16 Physical Cores)",
        "",
        "| Physical Cores | SET (P16, 1KB) | SET Speedup | SET p99 | GET (P16, 1KB) | GET Speedup | GET p99 |",
        "| :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])

    scaling = results.get("rudis_scaling", {})
    base_set = scaling.get("1", {}).get("set_ops_sec", 1.0) or 1.0
    base_get = scaling.get("1", {}).get("get_ops_sec", 1.0) or 1.0
    for c in [1, 2, 4, 8, 16]:
        row = scaling.get(str(c), {})
        s_ops = row.get("set_ops_sec", 0.0)
        g_ops = row.get("get_ops_sec", 0.0)
        s_p99 = row.get("set_p99_ms", 0.0)
        g_p99 = row.get("get_p99_ms", 0.0)
        s_fac = s_ops / base_set if base_set > 0 else 1.0
        g_fac = g_ops / base_get if base_get > 0 else 1.0
        lines.append(
            f"| **{c:2d} Core(s)** | **{s_ops:,.0f} ops/s** | {s_fac:.2f}x | {s_p99:.2f} ms | **{g_ops:,.0f} ops/s** | {g_fac:.2f}x | {g_p99:.2f} ms |"
        )

    df_scaling = results.get("dragonfly_scaling", {})
    if df_scaling:
        lines.extend([
            "",
            "### Dragonfly v1.39 (1 to 16 Physical Cores)",
            "",
            "| Physical Cores | SET (P16, 1KB) | SET Speedup | SET p99 | GET (P16, 1KB) | GET Speedup | GET p99 |",
            "| :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
        ])
        df_base_set = df_scaling.get("1", {}).get("set_ops_sec", 1.0) or 1.0
        df_base_get = df_scaling.get("1", {}).get("get_ops_sec", 1.0) or 1.0
        for c in [1, 2, 4, 8, 16]:
            row = df_scaling.get(str(c), {})
            s_ops = row.get("set_ops_sec", 0.0)
            g_ops = row.get("get_ops_sec", 0.0)
            s_p99 = row.get("set_p99_ms", 0.0)
            g_p99 = row.get("get_p99_ms", 0.0)
            s_fac = s_ops / df_base_set if df_base_set > 0 else 1.0
            g_fac = g_ops / df_base_get if df_base_get > 0 else 1.0
            lines.append(
                f"| **{c:2d} Core(s)** | **{s_ops:,.0f} ops/s** | {s_fac:.2f}x | {s_p99:.2f} ms | **{g_ops:,.0f} ops/s** | {g_fac:.2f}x | {g_p99:.2f} ms |"
            )

    lines.extend([
        "",
        "---",
        "",
        "## 5. Reproducing This Benchmark",
        "",
        "```bash",
        "cargo build --release",
        "python3 scripts/benchmark_multicore_suite.py",
        "```",
        "",
    ])

    with open(out_path, "w") as f:
        f.write("\n".join(lines))


def main():
    final_results = {
        "8_cores": benchmark_at_core_count(8),
        "16_cores": benchmark_at_core_count(16),
        "rudis_scaling": run_engine_scaling("Rudis"),
        "dragonfly_scaling": run_engine_scaling("Dragonfly"),
    }

    json_path = os.path.join(REPO_DIR, "docs", "benchmarks", "results", "benchmark_multicore_results.json")
    with open(json_path, "w") as f:
        json.dump(final_results, f, indent=2)
    print(f"\nJSON results saved to {json_path}")

    md_path = os.path.join(REPO_DIR, "docs", "benchmark_multicore_results.md")
    generate_markdown(final_results, md_path)
    print(f"Markdown report generated at {md_path}")


if __name__ == "__main__":
    main()
