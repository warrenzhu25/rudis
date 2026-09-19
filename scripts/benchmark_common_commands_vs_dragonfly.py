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

# This box is 32 physical cores x 2 SMT threads, where logical CPU N and N+32 are
# siblings of the same physical core. The server is pinned to 0-{cores-1}, i.e.
# physical cores 0-{cores-1}, so the client MUST avoid 32-{cores+31} or it lands on
# the server's own physical cores and the two fight over the same execution units.
# Physical cores 16-31 (logical 16-31 and 48-63) are disjoint from a 16-core server.
CLIENT_CPUS = "16-31,48-63"
CLIENT_THREADS = 16
CLIENT_CONNS = 4  # 64 concurrent connections total
# Throughput on this box swings ~25% run to run, so a couple of samples is pure
# noise. Several full sweeps per engine, aggregated by median, is the minimum that
# produces a number worth committing. Override with --iterations.
ITERATIONS = 3
# Measure for a fixed wall-clock duration instead of a fixed request count. With
# -n 2000 x 64 connections a 2M ops/s engine finished in ~60ms, so the "result"
# was mostly connection setup, TCP slow start and allocator warmup.
TEST_TIME_SECS = 3
WARMUP_SECS = 1
KEY_MAX = 64000

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

def port_in_use(port):
    """True if something is already accepting connections on this port."""
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=0.3)
        s.close()
        return True
    except Exception:
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
    cmd = [
        "taskset", "-c", CLIENT_CPUS,
        MEMTIER_BIN,
        "-s", "127.0.0.1",
        "-p", str(port),
        "-t", "16",
        "-c", "4",
        "-n", "500",  # 32,000 keys
        "--key-maximum", str(KEY_MAX),
        "--pipeline", "16",
        "--hide-histogram",
        "--json-out-file", json_tmp,
    ]

    if pop_type == "string":
        cmd.extend(["--ratio", "1:0", "-d", "1024", "--key-pattern", "S:S"])
    elif pop_type == "hash":
        cmd.extend([
            "--command=HSET __key__ field1 __data__",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "-d", "128",
        ])
    elif pop_type == "list":
        cmd.extend([
            "--command=LPUSH __key__ __data__",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "-d", "128",
        ])
    elif pop_type == "set":
        cmd.extend([
            "--command=SADD __key__ __data__",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "-d", "64",
        ])
    elif pop_type == "zset":
        cmd.extend([
            "--command=ZADD __key__ 100 __data__",
            "--command-ratio=1",
            "--command-key-pattern=R",
            "-d", "64",
        ])

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(json_tmp):
        os.remove(json_tmp)

def run_single_memtier(port, workload, run_idx, duration=None):
    duration = duration if duration is not None else TEST_TIME_SECS
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
        "--test-time", str(duration),
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

def take_sample(port, workload, run_idx):
    """Run one flush/populate/warmup/measure cycle and return the measured metrics.

    The warmup result is thrown away on purpose: it absorbs connection setup, TCP
    slow start, allocator growth and first-touch page faults, which otherwise land
    in whichever engine happens to be measured from a colder state.
    """
    flushall(port)
    if workload.get("populate_type"):
        populate(port, workload["populate_type"])
    time.sleep(0.2)
    if WARMUP_SECS > 0:
        run_single_memtier(port, workload, f"{run_idx}_warmup", duration=WARMUP_SECS)
        # Re-establish the exact dataset the measured run expects: the warmup may
        # have deleted keys (DEL), drained lists (LPOP) or grown them (LPUSH).
        flushall(port)
        if workload.get("populate_type"):
            populate(port, workload["populate_type"])
        time.sleep(0.2)
    return run_single_memtier(port, workload, run_idx)


def aggregate(samples):
    """Summarize repeated samples by median, keeping the spread visible.

    Median (not mean) because a single scheduling hiccup in one sweep would
    otherwise drag the reported number several percent.
    """
    if not samples:
        return None
    ops = [x["ops_sec"] for x in samples]
    return {
        "ops_sec_median": statistics.median(ops),
        "ops_sec_min": min(ops),
        "ops_sec_max": max(ops),
        # Kept for backwards compatibility with previously committed telemetry.
        "ops_sec_mean": statistics.mean(ops),
        "avg_latency_mean": statistics.mean([x["avg_latency"] for x in samples]),
        "p99_latency_mean": statistics.median([x["p99"] for x in samples]),
        "samples": [round(o, 1) for o in ops],
    }


def launch_engine(engine, cores, server_cpus):
    """Start one engine and wait for it to answer PING. Returns (proc, port).

    Only ever one engine is alive at a time: a co-resident server would evict the
    measured engine from L3 (the dataset is a large fraction of this box's cache)
    and steal cycles on the same pinned cores.
    """
    if engine == "Dragonfly":
        port = 6381
        cmd = [
            "taskset", "-c", server_cpus,
            DRAGONFLY_BIN,
            "--port", str(port),
            f"--proactor_threads={cores}",
            "--cache_mode=false",
            "--dbfilename=",
        ]
    else:
        port = 6379
        cmd = [
            "taskset", "-c", server_cpus,
            RUDIS_BIN,
            "--port", str(port),
            "--threads", str(cores),
        ]

    other_port = 6379 if engine == "Dragonfly" else 6381
    if port_in_use(other_port):
        print(f"    [!] WARNING: a server is still listening on {other_port} while "
              f"benchmarking {engine}. Numbers from this sweep are contaminated.")
    if port_in_use(port):
        print(f"    [!] WARNING: port {port} is already in use before launching "
              f"{engine}; a stale server may be answering instead.")

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_ping(port):
        print(f"Failed to start {engine} on port {port}")
        proc.kill()
        return None, port
    return proc, port


def stop_engine(proc):
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()
    time.sleep(0.5)


def benchmark_at_core_count(cores):
    server_cpus = f"0-{cores-1}"
    print("\n=================================================================")
    print(f"       BENCHMARKING ON {cores} CORES (CPUs {server_cpus})       ")
    print(f"       {ITERATIONS} interleaved sweeps per engine, median reported")
    print("=================================================================")

    samples = {"Dragonfly": {wl["id"]: [] for wl in WORKLOADS},
               "Rudis": {wl["id"]: [] for wl in WORKLOADS}}

    # Alternate which engine is measured first on each sweep. Running one engine
    # to completion before starting the other (the previous behaviour) let slow
    # machine drift - page cache, thermals, whatever else lands on the box - be
    # charged entirely to whichever engine happened to run second.
    for it in range(ITERATIONS):
        order = ["Dragonfly", "Rudis"] if it % 2 == 0 else ["Rudis", "Dragonfly"]
        print(f"\n>>> Sweep {it+1}/{ITERATIONS}  (order: {' then '.join(order)})")
        for engine in order:
            proc, port = launch_engine(engine, cores, server_cpus)
            if proc is None:
                continue
            for wl in WORKLOADS:
                m = take_sample(port, wl, it)
                if m:
                    samples[engine][wl["id"]].append(m)
                    print(f"    [{engine}] {wl['name']}: {m['ops_sec']:,.0f} ops/sec, "
                          f"p99: {m['p99']:.2f}ms")
            stop_engine(proc)

    results = {"Dragonfly": {}, "Rudis": {}}
    for engine in results:
        for wl in WORKLOADS:
            agg = aggregate(samples[engine][wl["id"]])
            if agg:
                results[engine][wl["id"]] = agg

    # Print summary table for this core count
    print("\n-------------------------------------------------------------------------------------------------------------------------")
    print(f"                             SUMMARY: {cores} CORES HEAD-TO-HEAD (median of {ITERATIONS} sweeps)                          ")
    print("-------------------------------------------------------------------------------------------------------------------------")
    print(f"{'Command / Workload':<28} | {'Dragonfly (ops/s)':<18} | {'Rudis (ops/s)':<18} | {'Rudis vs DF':<16} | {'spread (DF / Rudis)'}")
    print("-----------------------------+--------------------+--------------------+------------------+----------------------------")
    noisy = []
    for wl in WORKLOADS:
        wl_id = wl["id"]
        df_res = results["Dragonfly"].get(wl_id)
        ru_res = results["Rudis"].get(wl_id)
        if not (df_res and ru_res):
            continue
        df_med = df_res["ops_sec_median"]
        ru_med = ru_res["ops_sec_median"]
        ratio = ru_med / df_med if df_med else 0.0
        # If the two sample ranges overlap, the ordering of the medians is not
        # something this many samples can actually establish.
        overlap = (df_res["ops_sec_min"] <= ru_res["ops_sec_max"]
                   and ru_res["ops_sec_min"] <= df_res["ops_sec_max"])
        flag = " ~" if overlap else "  "
        if overlap:
            noisy.append(wl["name"])
        diff_str = f"{ratio:.2f}x ({(ratio-1.0)*100:+.1f}%){flag}"
        df_spread = (df_res["ops_sec_max"] - df_res["ops_sec_min"]) / df_med * 100 if df_med else 0
        ru_spread = (ru_res["ops_sec_max"] - ru_res["ops_sec_min"]) / ru_med * 100 if ru_med else 0
        spread_str = f"{df_spread:.0f}% / {ru_spread:.0f}%"
        print(f"{wl['name']:<28} | {df_med:>15,.0f} ops/s | {ru_med:>15,.0f} ops/s | {diff_str:<16} | {spread_str}")
    print("-------------------------------------------------------------------------------------------------------------------------")
    if noisy:
        print(f"  ~ = sample ranges overlap; the gap is not resolvable at {ITERATIONS} sweeps: "
              + ", ".join(noisy))
    print()

    return results

def main():
    global ITERATIONS

    # Usage: benchmark_common_commands_vs_dragonfly.py [cores[,cores...]] [--iterations N]
    argv = sys.argv[1:]
    core_list = [16]
    positional = [a for a in argv if not a.startswith("-")]
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-i", "--iterations") and i + 1 < len(argv):
            ITERATIONS = int(argv[i + 1])
            if positional and positional[0] == argv[i + 1]:
                positional.pop(0)
            i += 1
        elif a.startswith("--iterations="):
            ITERATIONS = int(a.split("=", 1)[1])
        i += 1
    if positional:
        core_list = [int(x) for x in positional[0].split(",")]

    print("=================================================================")
    print("  RUDIS VS DRAGONFLY: MOST COMMON REDIS COMMANDS BENCHMARK SUITE ")
    print("  Cores Tested: 16 Physical Cores & 32 Physical Cores            ")
    print("  Data Types: Strings, Hashes, Lists, Sets, ZSets, Keyspace      ")
    print(f"  Sweeps per engine: {ITERATIONS} (interleaved, median reported)  ")
    print("=================================================================")

    out_file = "/usr/local/google/home/warrenzhu/github/rudis/benchmark_common_commands_results.json"
    final_results = {}
    if os.path.exists(out_file):
        try:
            with open(out_file, "r") as f:
                final_results = json.load(f)
        except Exception:
            final_results = {}

    for cores in core_list:
        final_results[f"{cores}_cores"] = benchmark_at_core_count(cores)

    with open(out_file, "w") as f:
        json.dump(final_results, f, indent=2)
    print(f"\nBenchmark completed successfully! Results written to {out_file}")

if __name__ == "__main__":
    main()
