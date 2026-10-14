#!/usr/bin/env python3
"""A/B throughput comparison of two rudis builds, workload by workload.

Reuses benchmark_common_commands_vs_dragonfly.py (populate, memtier, CPU
pinning) and check_perf_regression.py's quiet-host wait and baseline builds.

- The two binaries alternate per round, the first one swapping every round
  (AB, BA, ...), so host drift and launch-order effects hit both alike.
- Before each launch it waits until other processes leave the host quiet
  (bursts from other tenants otherwise put 20-30% noise into runs).
- Rounds are added (up to --max-rounds) while any workload's spread on
  either side exceeds 5% CV.
- With --control WORKLOAD (one the change does not touch), each B/A ratio is
  also reported relative to the control's, which cancels remaining drift.

Examples:
  # this build vs HEAD, three workloads, LPUSH as the control:
  scripts/ab_bench.py --baseline-rev HEAD --only LPUSH,SADD,SPOP --control LPUSH --quick
  # two binaries:
  scripts/ab_bench.py /tmp/rudis_old target/release/rudis --only HSET,HGET

--quick screens in a few minutes (3 s runs, 2 per launch, 2 rounds): enough
for ~5% effects. Rerun without it when a result is close to the limit.
"""
import argparse
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(HERE)
CV_LIMIT_PCT = 5.0

# Workloads beyond the committed suite. memtier sends the same member every
# time, so after the first pass most removals remove nothing; that still
# exercises dispatch (routing, hashing, lookup), which is what fast paths
# change.
EXTRA_WORKLOADS = (
    ("SREM", "set_deep", "SREM __key__ m0001"),
    ("ZREM", "zset_deep", "ZREM __key__ m0001"),
    ("HDEL", "hash", "HDEL __key__ field1"),
    # 375-field hashes (fields "0".."374"): HGET scans to the middle.
    ("HGETDEEP", "hash_deep", "HGET __key__ 200"),
    # 10-field small hashes with 5-byte values (the packed small form).
    ("HGETSMALL", "hash_ten", "HGET __key__ 7"),
    ("HSETSMALL", "hash_ten", "HSET __key__ 7 m9999"),
)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("binaries", nargs="*", help="BIN_A BIN_B (or one BIN_B with --baseline-rev)")
    p.add_argument("--baseline-rev", help="Use this commit (built once, cached) as A")
    p.add_argument("--only", default="", help="Comma-separated workload ids to run")
    p.add_argument("--control", help="Workload the change does not touch; ratios are also shown relative to it")
    p.add_argument("--quick", action="store_true", help="3 s runs, 2 per launch, 1 warmup")
    p.add_argument("--cores", type=int, default=8, help="Server threads/CPUs")
    p.add_argument("--rounds", type=int, default=None, help="Minimum rounds (default 2 quick, 3 full)")
    p.add_argument("--max-rounds", type=int, default=None, help="Rounds allowed while noisy (default rounds+2)")
    p.add_argument("--max-busy-cpus", type=float, default=2.0,
                   help="Before each launch, wait until other processes keep at most this many CPUs busy")
    return p.parse_args()


def main():
    args = parse_args()
    if args.quick:
        os.environ.setdefault("BENCH_TEST_TIME", "3")
        os.environ.setdefault("BENCH_ITERATIONS", "2")
        os.environ.setdefault("BENCH_WARMUP", "1")
    sys.path.insert(0, HERE)
    import benchmark_common_commands_vs_dragonfly as s
    import check_perf_regression as gate

    if args.baseline_rev:
        if len(args.binaries) > 1:
            raise SystemExit("with --baseline-rev give at most one binary (B)")
        bin_a, sha = gate.build_baseline(args.baseline_rev)
        label_a = f"{args.baseline_rev} ({sha[:10]})"
        bin_b = args.binaries[0] if args.binaries else os.path.join(REPO_DIR, "target", "release", "rudis")
    else:
        if len(args.binaries) != 2:
            raise SystemExit("give BIN_A BIN_B, or --baseline-rev REV [BIN_B]")
        bin_a, bin_b = args.binaries
        label_a = bin_a

    # Deep hash populates: HSET key 0 m0000 1 m0001 ... (the scored deep
    # populate emits (j, member)); hash_ten is the same at depth 10.
    s.DEEP_POPULATE["hash_deep"] = ("HSET", True)
    s.DEEP_POPULATE["hash_ten"] = ("HSET", True)
    orig_populate_deep = s.populate_deep

    def populate_deep(port, pop_type):
        if pop_type != "hash_ten":
            return orig_populate_deep(port, pop_type)
        depth = s.POP_DEPTH
        s.POP_DEPTH = 10
        try:
            return orig_populate_deep(port, pop_type)
        finally:
            s.POP_DEPTH = depth

    s.populate_deep = populate_deep
    for wid, pop, cmd in EXTRA_WORKLOADS:
        s.WORKLOADS.append({
            "id": wid, "category": "extra", "name": wid, "pipeline": 16,
            "requests": 2000, "data_size": None, "ratio": None,
            "populate_type": pop, "command": cmd,
        })
    keep = [w.strip() for w in args.only.split(",") if w.strip()]
    if args.control and keep and args.control not in keep:
        keep.append(args.control)
    if keep:
        unknown = set(keep) - {w["id"] for w in s.WORKLOADS}
        if unknown:
            raise SystemExit(f"unknown workloads: {', '.join(sorted(unknown))}")
        s.WORKLOADS[:] = [w for w in s.WORKLOADS if w["id"] in keep]

    rounds = args.rounds or (2 if args.quick else 3)
    max_rounds = args.max_rounds or rounds + 2
    cores = args.cores
    server_cpus = f"{s.SERVER_CPU_BASE}-{s.SERVER_CPU_BASE + cores - 1}"
    s.CLIENT_CPUS_ACTIVE = f"{s.CLIENT_CPU_BASE}-{s.CLIENT_CPU_BASE + s.client_cpu_count(cores) - 1}"
    port = 6390

    def run_binary(binary):
        s.ensure_port_free(port, "rudis")
        proc = subprocess.Popen(
            ["taskset", "-c", server_cpus, binary, "--port", str(port), "--threads", str(cores)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        out = {}
        try:
            if not s.wait_ping(port):
                raise SystemExit(f"{binary} did not start")
            for wl in s.WORKLOADS:
                m = s.run_workload_benchmark("rudis", port, wl)
                if m:
                    out[wl["id"]] = m["runs"]
        finally:
            s.stop_and_reap(proc, port, "rudis")
            time.sleep(1.0)
        return out

    def cv(v):
        return statistics.stdev(v) / statistics.median(v) * 100 if len(v) > 1 else 0.0

    res = {"A": {}, "B": {}}
    for r in range(max_rounds):
        order = (("A", bin_a), ("B", bin_b)) if r % 2 == 0 else (("B", bin_b), ("A", bin_a))
        for tag, b in order:
            busy = gate.wait_quiet(args.max_busy_cpus)
            note = f" (host still busy: {busy:.1f} CPUs)" if busy > args.max_busy_cpus else ""
            print(f"=== round {r + 1} binary {tag} ({b}){note}", flush=True)
            for wid, runs in run_binary(b).items():
                res[tag].setdefault(wid, []).extend(runs)
        if r + 1 < rounds:
            continue
        noisy = [f"{tag}/{wid} {cv(v):.1f}%" for tag in res for wid, v in res[tag].items() if cv(v) > CV_LIMIT_PCT]
        if not noisy:
            break
        if r + 1 < max_rounds:
            print(f"=== noisy ({', '.join(noisy)}): adding a round", flush=True)

    ratios = {}
    for wl in s.WORKLOADS:
        a, b = res["A"].get(wl["id"], []), res["B"].get(wl["id"], [])
        if a and b:
            ratios[wl["id"]] = statistics.median(b) / statistics.median(a)
    ctrl = ratios.get(args.control) if args.control else None

    print(f"\nA = {label_a}\nB = {bin_b}")
    print(f"{cores} cores, server CPUs {server_cpus}, client CPUs {s.CLIENT_CPUS_ACTIVE}")
    head = f"{'workload':<11} {'A median':>13} {'A cv%':>6} {'B median':>13} {'B cv%':>6} {'B/A':>7}"
    if ctrl:
        head += f" {'vs ' + args.control:>13}"
    print(head)
    for wl in s.WORKLOADS:
        wid = wl["id"]
        if wid not in ratios:
            continue
        a, b = res["A"][wid], res["B"][wid]
        line = (f"{wid:<11} {statistics.median(a):>13,.0f} {cv(a):>6.1f} "
                f"{statistics.median(b):>13,.0f} {cv(b):>6.1f} {ratios[wid]:>7.3f}")
        if ctrl:
            line += "      control" if wid == args.control else f" {(ratios[wid] / ctrl - 1) * 100:>+12.1f}%"
        print(line)


if __name__ == "__main__":
    main()
