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

# ---------------------------------------------------------------------------
# CPU pinning.
#
# Host is an AMD EPYC 7B13: 32 physical cores / 64 threads, SMT sibling pairs
# are (N, N+32) -- i.e. logical CPU 32 is the second thread of physical core 0.
#
# The previous layout (server 0-15, client 32-63) put the load generator on the
# *SMT siblings of the server's own cores*, so memtier threads stole execution
# units from the shard threads they were measuring. How many collided was
# re-decided by the scheduler every run, which drove run-to-run swings of up to
# 2.05x (median CV 15.5%).
#
# New layout uses disjoint physical cores and leaves every sibling idle:
#   server -> logical 16-31  (physical cores 16-31)
#   client -> logical 0-15   (physical cores 0-15)
#   idle   -> logical 32-63  (all siblings of the above)
#
# The server takes 16-31 specifically because cpu0 carries ~120x the softirq
# load of cpu15 (loopback NET_RX + device IRQs). Cores 16-31 measure 5k-88k
# softirq ticks vs cpu0's 3.4M, and are uniform among themselves. Putting the
# client on 0-15 makes the load generator absorb that interrupt burden instead
# of handicapping shard 0.
SERVER_CPU_BASE = 16
CLIENT_CPU_BASE = 0
TOTAL_PHYSICAL_CORES = 32

CLIENT_THREADS = 16
CLIENT_CONNS = int(os.environ.get("BENCH_CONNS", "4"))  # x threads = total connections

# Time-based steady-state window. Count-based runs (-n 2000) completed in ~64ms,
# so TCP slow-start, allocator warmup, hashbrown growth and page faults all
# landed inside the measurement. Seconds of steady state instead.
TEST_TIME_SECS = int(os.environ.get("BENCH_TEST_TIME", "5"))
WARMUP_RUNS = int(os.environ.get("BENCH_WARMUP", "1"))
ITERATIONS = int(os.environ.get("BENCH_ITERATIONS", "3"))

# Workloads whose measured coefficient of variation exceeds this are reported as
# inconclusive rather than as a ratio.
CV_INCONCLUSIVE_PCT = 5.0

KEY_MAX = 64000

# Set per core-count by benchmark_at_core_count().
CLIENT_CPUS_ACTIVE = f"{CLIENT_CPU_BASE}-{CLIENT_CPU_BASE + CLIENT_THREADS - 1}"

# Optional comma-separated workload id filter, e.g. BENCH_WORKLOADS=SET,GET,INCR
WORKLOAD_FILTER = [
    w.strip() for w in os.environ.get("BENCH_WORKLOADS", "").split(",") if w.strip()
]


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

if WORKLOAD_FILTER:
    WORKLOADS = [w for w in WORKLOADS if w["id"] in WORKLOAD_FILTER]
    if not WORKLOADS:
        raise SystemExit(f"BENCH_WORKLOADS matched no workloads: {WORKLOAD_FILTER}")

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
    # Populate from ONE connection covering the whole keyspace.
    #
    # memtier gives every connection the same key sequence rather than
    # partitioning the range between them, so -n is the number of DISTINCT keys
    # created no matter how many threads and connections are used. The previous
    # "-t 16 -c 4 -n 500" wrote the same 500 keys 64 times over and produced
    # DBSIZE=500, not the 32,000 its comment claimed. Reading back randomly
    # across --key-maximum 64000 then missed ~99% of the time, so the read
    # workloads were benchmarking the null-reply path and never transferred a
    # value at all. Verified with DBSIZE:
    #   -t16 -c4 -n500  S:S -> 500        (what this used to do)
    #   -t1  -c1 -n64000 S:S -> 64000     (what it does now)
    cmd = [
        "taskset", "-c", CLIENT_CPUS_ACTIVE,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", "1",
        "-c", "1",
        "-n", str(KEY_MAX),
        "--key-maximum", str(KEY_MAX),
        "--key-minimum", "1",
        "--pipeline", "64",
        "--hide-histogram",
        "--json-out-file", json_tmp,
    ]

    if pop_type == "string":
        cmd.extend(["--ratio", "1:0", "-d", "1024", "--key-pattern", "S:S"])
    elif pop_type == "hash":
        cmd.extend([
            "--command=HSET __key__ field1 __data__",
            "--command-ratio=1",
            # Sequential so the single populate connection walks the entire
            # range exactly once. Random placement leaves ~37% of the keyspace
            # empty by the coupon-collector bound, which shows up as misses.
            "--command-key-pattern=S",
            "-d", "128",
        ])
    elif pop_type == "list":
        cmd.extend([
            "--command=LPUSH __key__ __data__",
            "--command-ratio=1",
            # Sequential so the single populate connection walks the entire
            # range exactly once. Random placement leaves ~37% of the keyspace
            # empty by the coupon-collector bound, which shows up as misses.
            "--command-key-pattern=S",
            "-d", "128",
        ])
    elif pop_type == "set":
        cmd.extend([
            "--command=SADD __key__ __data__",
            "--command-ratio=1",
            # Sequential so the single populate connection walks the entire
            # range exactly once. Random placement leaves ~37% of the keyspace
            # empty by the coupon-collector bound, which shows up as misses.
            "--command-key-pattern=S",
            "-d", "64",
        ])
    elif pop_type == "zset":
        cmd.extend([
            "--command=ZADD __key__ 100 __data__",
            "--command-ratio=1",
            # Sequential so the single populate connection walks the entire
            # range exactly once. Random placement leaves ~37% of the keyspace
            # empty by the coupon-collector bound, which shows up as misses.
            "--command-key-pattern=S",
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
        "taskset", "-c", CLIENT_CPUS_ACTIVE,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", str(CLIENT_THREADS),
        "-c", str(CLIENT_CONNS),
        "--test-time", str(TEST_TIME_SECS),
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
            # Random access for the measured workload, so the benchmark spreads
            # over the keyspace rather than walking it in cache-friendly order.
            # (The populate step uses S deliberately; that is a different call.)
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
    total = WARMUP_RUNS + ITERATIONS
    for r in range(total):
        is_warmup = r < WARMUP_RUNS
        flushall(port)
        if workload.get("populate_type"):
            populate(port, workload["populate_type"])
        time.sleep(0.2)

        metrics = run_single_memtier(port, workload, r)
        if not metrics:
            continue
        if is_warmup:
            # Discarded: absorbs TCP slow-start, allocator arena warmup,
            # hashbrown growth and first-touch page faults.
            print(f"      Warmup {r+1}/{WARMUP_RUNS}: {metrics['ops_sec']:,.0f} ops/sec (discarded)")
            continue
        runs.append(metrics)
        print(f"      Run {r-WARMUP_RUNS+1}/{ITERATIONS}: {metrics['ops_sec']:,.0f} ops/sec, "
              f"avg: {metrics['avg_latency']:.2f}ms, p99: {metrics['p99']:.2f}ms")

    if not runs:
        return None

    ops_list = [x["ops_sec"] for x in runs]
    avg_lat_list = [x["avg_latency"] for x in runs]
    p99_list = [x["p99"] for x in runs]

    median_ops = statistics.median(ops_list)
    stdev_ops = statistics.stdev(ops_list) if len(ops_list) > 1 else 0.0
    cv_pct = (stdev_ops / median_ops * 100) if median_ops else 0.0
    if cv_pct > CV_INCONCLUSIVE_PCT:
        print(f"      [!] CV {cv_pct:.1f}% exceeds {CV_INCONCLUSIVE_PCT:.0f}% -- result is noise-dominated")

    return {
        # Median is the headline statistic: unlike the mean it is not dragged by
        # a single outlier run.
        "ops_sec_median": median_ops,
        "ops_sec_mean": statistics.mean(ops_list),
        "ops_sec_min": min(ops_list),
        "ops_sec_max": max(ops_list),
        "ops_sec_cv_pct": cv_pct,
        "avg_latency_median": statistics.median(avg_lat_list),
        "p99_latency_median": statistics.median(p99_list),
        "runs": ops_list,
    }

def benchmark_at_core_count(cores):
    global CLIENT_CPUS_ACTIVE

    server_cpus = f"{SERVER_CPU_BASE}-{SERVER_CPU_BASE + cores - 1}"
    client_cores = min(cores, CLIENT_THREADS)
    CLIENT_CPUS_ACTIVE = f"{CLIENT_CPU_BASE}-{CLIENT_CPU_BASE + client_cores - 1}"

    oversubscribed = (
        SERVER_CPU_BASE + cores > TOTAL_PHYSICAL_CORES
        or CLIENT_CPU_BASE + client_cores > SERVER_CPU_BASE
    )

    print(f"\n=================================================================")
    print(f"       BENCHMARKING ON {cores} CORES       ")
    print(f"=================================================================")
    print(f"  server CPUs : {server_cpus}   (physical cores, SMT siblings idle)")
    print(f"  client CPUs : {CLIENT_CPUS_ACTIVE}   (disjoint physical cores)")
    print(f"  window      : {TEST_TIME_SECS}s x {ITERATIONS} runs "
          f"(+{WARMUP_RUNS} discarded warmup)")
    if oversubscribed:
        print(f"  [!] WARNING: {cores} server + {client_cores} client cores exceed the "
              f"{TOTAL_PHYSICAL_CORES} physical cores available.")
        print(f"  [!] Server and client will share physical cores via SMT; "
              f"results will be noise-dominated.")

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
    cargo_bin = os.path.expanduser("~/.cargo/bin/cargo")
    if os.path.exists(cargo_bin):
        repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(f"[*] Ensuring latest release build via {cargo_bin} build --release...")
        subprocess.run([cargo_bin, "build", "--release"], check=True, cwd=repo_dir)

    print(f"\n>>> Launching Rudis ({cores} threads)...")
    rudis_cmd = [
        "taskset", "-c", server_cpus,
        RUDIS_BIN,
        "--port", "6379",
        "--threads", str(cores),
    ]
    rudis_log = open("/tmp/rudis_bench.log", "w")
    rudis_proc = subprocess.Popen(rudis_cmd, stdout=rudis_log, stderr=subprocess.STDOUT)
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
    print(f"\n----------------------------------------------------------------------------------------------------------------------")
    print(f"                                   SUMMARY: {cores} CORES HEAD-TO-HEAD (median of {ITERATIONS})                        ")
    print(f"----------------------------------------------------------------------------------------------------------------------")
    print(f"{'Command / Workload':<28} | {'Dragonfly (ops/s)':>19} {'CV':>6} | {'Rudis (ops/s)':>19} {'CV':>6} | {'Rudis vs DF':<20} | {'p99 (DF/Rudis)'}")
    print(f"-----------------------------+---------------------------+---------------------------+----------------------+----------------")
    inconclusive = 0
    for wl in WORKLOADS:
        wl_id = wl["id"]
        df_res = results["Dragonfly"].get(wl_id)
        ru_res = results["Rudis"].get(wl_id)
        if not (df_res and ru_res):
            continue
        df_ops = df_res["ops_sec_median"]
        ru_ops = ru_res["ops_sec_median"]
        df_cv = df_res["ops_sec_cv_pct"]
        ru_cv = ru_res["ops_sec_cv_pct"]
        ratio = ru_ops / df_ops
        diff_pct = (ratio - 1.0) * 100

        # If either engine's run-to-run noise is comparable to the measured gap,
        # the ratio is not evidence of anything.
        noise = max(df_cv, ru_cv)
        if noise > CV_INCONCLUSIVE_PCT and abs(diff_pct) < 2 * noise:
            diff_str = f"{ratio:.2f}x INCONCLUSIVE"
            inconclusive += 1
        else:
            diff_str = f"{ratio:.2f}x ({diff_pct:+.1f}%)"

        p99_str = f"{df_res['p99_latency_median']:.2f}/{ru_res['p99_latency_median']:.2f}ms"
        print(f"{wl['name']:<28} | {df_ops:>15,.0f} ops/s {df_cv:>5.1f}% | "
              f"{ru_ops:>15,.0f} ops/s {ru_cv:>5.1f}% | {diff_str:<20} | {p99_str}")
    print(f"----------------------------------------------------------------------------------------------------------------------")
    if inconclusive:
        print(f"  [!] {inconclusive} workload(s) inconclusive: run-to-run noise exceeds the measured difference.")
    print()

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
