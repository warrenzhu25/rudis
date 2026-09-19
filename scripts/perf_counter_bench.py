#!/usr/bin/env python3
"""
Deterministic per-operation counter benchmark for Rudis.

Wall-clock throughput on this host has a median coefficient of variation of
~15% (see benchmark_methodology_analysis.md), which cannot resolve the 1-5%
effects that most microarchitectural optimizations produce. Hardware counters
can: instructions-per-operation and page-faults-per-operation are very nearly
deterministic for a fixed workload, because they count *work done* rather than
*work done per unit of contended wall-clock time*.

Use this to answer "did I actually remove the allocation / memcpy / syscall?"
Use the throughput suite to answer "how fast is it end to end?".

Usage:
    python3 scripts/perf_counter_bench.py                 # all workloads
    python3 scripts/perf_counter_bench.py SET GET         # subset
    BENCH_PERF_SECS=5 python3 scripts/perf_counter_bench.py

Notes:
  * perf_event_paranoid is 2 on this host, so only user-space events are
    counted for our own process. Kernel-side syscall cost is therefore NOT
    included; that is fine for measuring allocation/copy/parse work, but means
    syscall-elimination wins show up only indirectly (as fewer instructions).
  * Server is pinned to the same quiet cores the throughput suite uses.
"""

import json
import os
import re
import signal
import socket
import statistics
import subprocess
import sys
import time

RUDIS_BIN = "/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"

# Matches the throughput harness: server on quiet physical cores 16-31,
# client on disjoint physical cores 0-15, all SMT siblings (32-63) idle.
SERVER_CPUS = "16-31"
CLIENT_CPUS = "0-15"
SHARDS = 16
PORT = 6379

PERF_SECS = int(os.environ.get("BENCH_PERF_SECS", "5"))
ITERATIONS = int(os.environ.get("BENCH_PERF_ITERS", "3"))
KEY_MAX = 64000

EVENTS = [
    "instructions",
    "cycles",
    "cache-references",
    "cache-misses",
    "branch-misses",
    "page-faults",
    "context-switches",
]

WORKLOADS = {
    "SET":       {"args": ["--ratio", "1:0", "-d", "1024", "--key-pattern", "R:R"], "populate": None},
    "GET":       {"args": ["--ratio", "0:1", "-d", "1024", "--key-pattern", "R:R"], "populate": "string"},
    "INCR":      {"args": ["--command=INCR __key__"], "populate": None},
    "HSET":      {"args": ["--command=HSET __key__ field1 __data__", "-d", "128"], "populate": None},
    "HGET":      {"args": ["--command=HGET __key__ field1"], "populate": "hash"},
    "LPUSH":     {"args": ["--command=LPUSH __key__ __data__", "-d", "128"], "populate": None},
    "LRANGE":    {"args": ["--command=LRANGE __key__ 0 10"], "populate": "list"},
    "SADD":      {"args": ["--command=SADD __key__ __data__", "-d", "64"], "populate": None},
    "ZADD":      {"args": ["--command=ZADD __key__ 100 __data__", "-d", "64"], "populate": None},
    "ZRANGE":    {"args": ["--command=ZRANGE __key__ 0 10"], "populate": "zset"},
    "DEL":       {"args": ["--command=DEL __key__"], "populate": "string"},
    "EXISTS":    {"args": ["--command=EXISTS __key__"], "populate": "string"},
}

POPULATE_ARGS = {
    "string": ["--ratio", "1:0", "-d", "1024", "--key-pattern", "S:S"],
    "hash":   ["--command=HSET __key__ field1 __data__", "-d", "128", "--command-key-pattern=R"],
    "list":   ["--command=LPUSH __key__ __data__", "-d", "128", "--command-key-pattern=R"],
    "set":    ["--command=SADD __key__ __data__", "-d", "64", "--command-key-pattern=R"],
    "zset":   ["--command=ZADD __key__ 100 __data__", "-d", "64", "--command-key-pattern=R"],
}


def wait_ping(port, timeout=10.0):
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


def send_cmd(port, payload):
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=2.0)
        s.sendall(payload)
        s.recv(4096)
        s.close()
    except Exception:
        pass


def populate(pop_type):
    if not pop_type:
        return
    cmd = [
        "taskset", "-c", CLIENT_CPUS, MEMTIER_BIN,
        "-s", "127.0.0.1", "-p", str(PORT),
        "-t", "16", "-c", "4", "-n", "500",
        "--key-maximum", str(KEY_MAX), "--pipeline", "16",
        "--hide-histogram",
    ] + POPULATE_ARGS[pop_type]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def run_client(extra_args):
    cmd = [
        "taskset", "-c", CLIENT_CPUS, MEMTIER_BIN,
        "-s", "127.0.0.1", "-p", str(PORT),
        "-t", "16", "-c", "4",
        "--test-time", str(PERF_SECS),
        "--pipeline", "16",
        "--key-maximum", str(KEY_MAX),
        "--hide-histogram",
        "--json-out-file", "/tmp/perfbench_client.json",
    ] + extra_args
    if any(a.startswith("--command=") for a in extra_args):
        cmd.extend(["--command-ratio=1", "--command-key-pattern=R"])
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        with open("/tmp/perfbench_client.json") as f:
            return float(json.load(f)["ALL STATS"]["Totals"]["Ops/sec"])
    except Exception:
        return 0.0


def parse_perf(stderr_text):
    """Parse `perf stat` output into {event: count}."""
    counters = {}
    for line in stderr_text.splitlines():
        line = line.strip()
        m = re.match(r"^([\d,]+)\s+([a-zA-Z0-9_\-\.:]+)", line)
        if not m:
            continue
        raw, event = m.group(1), m.group(2)
        try:
            counters[event.split(":")[0]] = int(raw.replace(",", ""))
        except ValueError:
            pass
    return counters


def measure(name, spec):
    """Attach perf to a running Rudis, drive one workload, return per-op counters."""
    proc = subprocess.Popen(
        ["taskset", "-c", SERVER_CPUS, RUDIS_BIN,
         "--port", str(PORT), "--threads", str(SHARDS)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if not wait_ping(PORT):
        proc.kill()
        raise SystemExit(f"Rudis failed to start for workload {name}")

    try:
        send_cmd(PORT, b"*1\r\n$8\r\nFLUSHALL\r\n")
        populate(spec["populate"])

        # Warmup, not counted: pays first-touch faults and allocator warmup.
        run_client(spec["args"])

        samples = []
        for _ in range(ITERATIONS):
            perf = subprocess.Popen(
                ["perf", "stat", "-e", ",".join(EVENTS), "-p", str(proc.pid)],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
            )
            time.sleep(0.3)  # let perf attach before load starts
            ops_sec = run_client(spec["args"])
            perf.send_signal(signal.SIGINT)
            _, err = perf.communicate(timeout=20)

            counters = parse_perf(err)
            total_ops = ops_sec * PERF_SECS
            if not counters or total_ops <= 0:
                continue
            samples.append({
                "ops_sec": ops_sec,
                "per_op": {k: v / total_ops for k, v in counters.items()},
            })
        return samples
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        time.sleep(0.5)


def main():
    selected = sys.argv[1:] or list(WORKLOADS.keys())
    unknown = [w for w in selected if w not in WORKLOADS]
    if unknown:
        raise SystemExit(f"Unknown workload(s): {unknown}. Known: {list(WORKLOADS)}")

    print("=" * 100)
    print("  RUDIS DETERMINISTIC PER-OPERATION COUNTER BENCHMARK")
    print(f"  server CPUs {SERVER_CPUS} | client CPUs {CLIENT_CPUS} | "
          f"{PERF_SECS}s x {ITERATIONS} runs (+1 warmup)")
    print("  User-space counters only (perf_event_paranoid=2).")
    print("=" * 100)

    results = {}
    for name in selected:
        print(f"\n>>> {name}")
        samples = measure(name, WORKLOADS[name])
        if not samples:
            print("    [!] no samples collected")
            continue

        agg = {}
        for event in EVENTS:
            vals = [s["per_op"][event] for s in samples if event in s["per_op"]]
            if not vals:
                continue
            med = statistics.median(vals)
            cv = (statistics.stdev(vals) / med * 100) if len(vals) > 1 and med else 0.0
            agg[event] = {"per_op_median": med, "cv_pct": cv}
            print(f"    {event:<18} {med:>14.2f} /op   (CV {cv:>5.1f}%)")

        ops = [s["ops_sec"] for s in samples]
        agg["_ops_sec_median"] = statistics.median(ops)
        print(f"    {'(throughput)':<18} {statistics.median(ops):>14,.0f} ops/s")
        results[name] = agg

    out = "/usr/local/google/home/warrenzhu/github/rudis/benchmark_perf_counters.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nWritten to {out}")
    print("\nCompare across commits with: git stash && <rebuild> && rerun && diff the json.")
    print("instructions/op and page-faults/op are the most trustworthy signals.")


if __name__ == "__main__":
    main()
