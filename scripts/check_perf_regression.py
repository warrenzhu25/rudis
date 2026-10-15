#!/usr/bin/env python3
"""
Automated Hardware Performance Counter, Throughput & Tail-Latency Regression Gate for Rudis.

Validates that:
1. GET hot path incurs <= 0.01 page-faults / op (zero unexpected heap allocations).
2. GET and SET hot paths incur <= 0.01 context-switches / op (lock-free thread execution).
3. GET and SET throughput meet the >= 1,000,000 ops/sec baseline floor.
4. GET and SET average latency (<= 1.2 ms), p99 tail latency (<= 2.9 ms), and
   p99.9 tail latency (<= 15.0 ms) stay within the engineering invariants in agent.md.
5. With `--live`, starts `target/release/rudis` and runs a live pipelined SET/GET
   smoke benchmark verifying live throughput, p99 latency, and zero GET page-faults/op.
6. With `--memtier`, starts `target/release/rudis` pinned like the throughput suite,
   measures SET/GET with memtier, and fails if either drops more than
   `--max-drop-pct` below `benchmark_live_baseline.json` (machine-specific; updated only
   with `--update-baseline`). Without `--live`/`--memtier`, checks 1-4 only re-read the
   stored counter file and say nothing about the current build.
7. With `--memtier --baseline-rev REV` (or `--baseline-bin PATH`), measures this build
   and a baseline build back to back instead, in alternating launches (order swapped each
   round), waiting before each launch until other processes leave the host quiet, and
   adding rounds while either side's spread exceeds 5% CV. Fails if this build is more
   than `--max-ab-drop-pct` (5%) below the baseline build. Drift on the host hits both
   sides alike, so this tracks real regressions far better than fixed numbers do. The
   baseline commit is built once in a temporary worktree and cached.

Exit code 0 indicates all hardware counter, throughput, and latency thresholds are satisfied.
"""

import argparse
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FILE = os.path.join(REPO_DIR, "benchmark_perf_counters.json")
DEFAULT_BIN = os.path.join(REPO_DIR, "target", "release", "rudis")

THROUGHPUT_FLOOR_OPS = 2_630_000.0
AVG_LATENCY_CEIL_MS = 1.2
P99_LATENCY_CEIL_MS = 2.9
P999_LATENCY_CEIL_MS = 15.0
COUNTER_CEIL_PER_OP = 0.01

# Live CI smoke thresholds (sized to pass reliably on 2-vCPU shared GitHub Actions runners
# while catching catastrophic throughput/latency/allocation regressions).
LIVE_THROUGHPUT_FLOOR_OPS = 100_000.0
LIVE_P99_LATENCY_CEIL_MS = 25.0


def validate_counter_data(data):
    """Validate counter, throughput, and latency invariants on a parsed JSON dictionary.
    Returns (regressions_list, rows_list).
    """
    regressions = []
    rows = []

    if "GET" not in data and "SET" not in data:
        regressions.append("Neither GET nor SET workload present in counter data")
        return regressions, rows

    for wl in ("GET", "SET"):
        if wl not in data:
            continue
        entry = data[wl]

        if wl == "GET":
            pf = float(entry.get("page-faults", {}).get("per_op_median", 0.0))
            st_pf = "PASS" if pf <= COUNTER_CEIL_PER_OP else "FAIL"
            if st_pf == "FAIL":
                regressions.append(
                    f"{wl} page-faults: {pf:.6f} /op exceeds threshold {COUNTER_CEIL_PER_OP}"
                )
            rows.append((wl, "page-faults", f"{pf:>12.6f}/op", f"<= {COUNTER_CEIL_PER_OP:.6f}", st_pf))

        cs = float(entry.get("context-switches", {}).get("per_op_median", 0.0))
        st_cs = "PASS" if cs <= COUNTER_CEIL_PER_OP else "FAIL"
        if st_cs == "FAIL":
            regressions.append(
                f"{wl} context-switches: {cs:.6f} /op exceeds threshold {COUNTER_CEIL_PER_OP}"
            )
        rows.append((wl, "context-switches", f"{cs:>12.6f}/op", f"<= {COUNTER_CEIL_PER_OP:.6f}", st_cs))

        ops = float(entry.get("_ops_sec_median", 0.0))
        st_ops = "PASS" if ops >= THROUGHPUT_FLOOR_OPS else "FAIL"
        if st_ops == "FAIL":
            regressions.append(
                f"{wl} throughput: {ops:,.0f} ops/s below threshold {THROUGHPUT_FLOOR_OPS:,.0f} ops/s"
            )
        rows.append((wl, "throughput", f"{ops:>12,.0f}/s", f">= {THROUGHPUT_FLOOR_OPS:,.0f}", st_ops))

        avg_ms = float(entry.get("_avg_latency_ms_median", 0.0))
        st_avg = "PASS" if 0.0 < avg_ms <= AVG_LATENCY_CEIL_MS else "FAIL"
        if st_avg == "FAIL":
            regressions.append(
                f"{wl} avg latency: {avg_ms:.3f} ms exceeds threshold {AVG_LATENCY_CEIL_MS:.3f} ms"
            )
        rows.append((wl, "avg-latency", f"{avg_ms:>11.3f} ms", f"<= {AVG_LATENCY_CEIL_MS:.3f} ms", st_avg))

        p99_ms = float(entry.get("_p99_latency_ms_median", 0.0))
        st_p99 = "PASS" if 0.0 < p99_ms <= P99_LATENCY_CEIL_MS else "FAIL"
        if st_p99 == "FAIL":
            regressions.append(
                f"{wl} p99 latency: {p99_ms:.3f} ms exceeds threshold {P99_LATENCY_CEIL_MS:.3f} ms"
            )
        rows.append((wl, "p99-latency", f"{p99_ms:>11.3f} ms", f"<= {P99_LATENCY_CEIL_MS:.3f} ms", st_p99))

        p999_ms = float(entry.get("_p999_latency_ms_median", 0.0))
        st_p999 = "PASS" if 0.0 < p999_ms <= P999_LATENCY_CEIL_MS else "FAIL"
        if st_p999 == "FAIL":
            regressions.append(
                f"{wl} p99.9 latency: {p999_ms:.3f} ms exceeds threshold {P999_LATENCY_CEIL_MS:.3f} ms"
            )
        rows.append((wl, "p99.9-latency", f"{p999_ms:>11.3f} ms", f"<= {P999_LATENCY_CEIL_MS:.3f} ms", st_p999))

    return regressions, rows


def read_proc_faults(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split(")")[-1].split()
            return int(fields[7]) + int(fields[9])
    except Exception:
        return None


def wait_ping(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                if b"+PONG" in s.recv(64):
                    return True
        except Exception:
            time.sleep(0.05)
    return False


def run_pipelined_load(port, kind, duration_sec, threads=4, pipeline=16, key_space=4096):
    """Drive pipelined SET or GET requests against 127.0.0.1:port and return (ops_sec, p99_ms, total_ops)."""
    val = b"x" * 128
    batches = []
    expected_bytes_per_batch = 0
    for b in range(64):
        buf = bytearray()
        for i in range(pipeline):
            k = f"perf:key:{(b * pipeline + i) % key_space}".encode()
            if kind == "SET":
                buf.extend(
                    f"*3\r\n$3\r\nSET\r\n${len(k)}\r\n".encode()
                    + k
                    + f"\r\n${len(val)}\r\n".encode()
                    + val
                    + b"\r\n"
                )
            else:
                buf.extend(f"*2\r\n$3\r\nGET\r\n${len(k)}\r\n".encode() + k + b"\r\n")
        batches.append(bytes(buf))
    if kind == "SET":
        expected_bytes_per_batch = pipeline * len(b"+OK\r\n")
    else:
        expected_bytes_per_batch = pipeline * (len(f"${len(val)}\r\n".encode()) + len(val) + 2)

    stop_at = time.perf_counter() + duration_sec
    counts = [0] * threads
    latencies_ms = [[] for _ in range(threads)]

    def worker(tid):
        s = socket.create_connection(("127.0.0.1", port), timeout=5.0)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        local_ops = 0
        local_lats = latencies_ms[tid]
        idx = tid
        try:
            while time.perf_counter() < stop_at:
                payload = batches[idx & 63]
                idx += 1
                t0 = time.perf_counter()
                s.sendall(payload)
                rem = expected_bytes_per_batch
                while rem > 0:
                    chunk = s.recv(min(65536, rem))
                    if not chunk:
                        break
                    rem -= len(chunk)
                dt_ms = (time.perf_counter() - t0) * 1000.0
                local_lats.append(dt_ms)
                local_ops += pipeline
        finally:
            s.close()
        counts[tid] = local_ops

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    t_start = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    elapsed = max(1e-6, time.perf_counter() - t_start)

    total_ops = sum(counts)
    all_lats = [x for sub in latencies_ms for x in sub]
    all_lats.sort()
    p99_ms = all_lats[int(len(all_lats) * 0.99)] if all_lats else 0.0
    return total_ops / elapsed, p99_ms, total_ops


def run_live_gate(rudis_bin, port, duration_sec, attach=False, throughput_floor=LIVE_THROUGHPUT_FLOOR_OPS):
    shards = min(4, max(1, (os.cpu_count() or 2) // 2))
    proc = None
    target_pid = os.getpid()
    if not attach:
        if not os.path.exists(rudis_bin):
            return [f"Rudis binary not found for --live gate: {rudis_bin}"], []
        proc = subprocess.Popen(
            [rudis_bin, "--port", str(port), "--threads", str(shards), "--no-pin"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        target_pid = proc.pid

    regressions = []
    rows = []
    try:
        if not wait_ping(port, timeout=10.0):
            return [f"Rudis server failed to start or respond on port {port}"], []

        # Warmup and populate keyspace
        run_pipelined_load(port, "SET", duration_sec=0.3, threads=shards, pipeline=16)

        set_ops, set_p99, _ = run_pipelined_load(
            port, "SET", duration_sec=duration_sec, threads=shards, pipeline=16
        )
        run_pipelined_load(port, "GET", duration_sec=0.2, threads=shards, pipeline=16)
        pf_before = read_proc_faults(target_pid) if proc is not None else None
        get_ops, get_p99, get_total = run_pipelined_load(
            port, "GET", duration_sec=duration_sec, threads=shards, pipeline=16
        )
        pf_after = read_proc_faults(target_pid) if proc is not None else None

        for wl, ops, p99 in (("LIVE-SET", set_ops, set_p99), ("LIVE-GET", get_ops, get_p99)):
            st_ops = "PASS" if ops >= throughput_floor else "FAIL"
            if st_ops == "FAIL":
                regressions.append(
                    f"{wl} throughput: {ops:,.0f} ops/s below live threshold {throughput_floor:,.0f} ops/s"
                )
            rows.append((wl, "throughput", f"{ops:>12,.0f}/s", f">= {throughput_floor:,.0f}", st_ops))

            st_p99 = "PASS" if 0.0 < p99 <= LIVE_P99_LATENCY_CEIL_MS else "FAIL"
            if st_p99 == "FAIL":
                regressions.append(
                    f"{wl} p99 latency: {p99:.3f} ms exceeds live threshold {LIVE_P99_LATENCY_CEIL_MS:.3f} ms"
                )
            rows.append((wl, "p99-latency", f"{p99:>11.3f} ms", f"<= {LIVE_P99_LATENCY_CEIL_MS:.3f} ms", st_p99))

        if pf_before is not None and pf_after is not None and get_total > 0:
            pf_per_op = max(0, pf_after - pf_before) / float(get_total)
            st_pf = "PASS" if pf_per_op <= COUNTER_CEIL_PER_OP else "FAIL"
            if st_pf == "FAIL":
                regressions.append(
                    f"LIVE-GET page-faults: {pf_per_op:.6f} /op exceeds threshold {COUNTER_CEIL_PER_OP}"
                )
            rows.append(("LIVE-GET", "page-faults", f"{pf_per_op:>12.6f}/op", f"<= {COUNTER_CEIL_PER_OP:.6f}", st_pf))
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()

    return regressions, rows


def find_memtier():
    env = os.environ.get("MEMTIER_BIN")
    if env and os.path.exists(env):
        return env
    for p in (
        os.path.expanduser("~/memtier_benchmark/memtier_benchmark"),
        "/usr/local/bin/memtier_benchmark",
        "/usr/bin/memtier_benchmark",
    ):
        if os.path.exists(p):
            return p
    return None


# memtier gate: same pinning as benchmark_common_commands_vs_dragonfly.py (server on
# physical cores 16+, client on 0+, SMT siblings idle) so numbers are comparable.
MEMTIER_WORKLOADS = {
    "SET": {"args": ["--ratio", "1:0", "-d", "1024", "--key-pattern", "R:R"], "populate": False},
    "GET": {"args": ["--ratio", "0:1", "-d", "1024", "--key-pattern", "R:R"], "populate": True},
}
MEMTIER_KEY_MAX = 64000
DEFAULT_LIVE_BASELINE = os.path.join(REPO_DIR, "benchmark_live_baseline.json")
# Launch-to-launch throughput spread on this host is 3-6% CV (see the RPUSH/SPOP A/B
# in d12ae33); 12% keeps false failures rare while catching real regressions.
DEFAULT_MAX_DROP_PCT = 12.0


def _memtier_run(memtier, port, client_cpus, client_threads, args, secs, json_path):
    cmd = [
        "taskset", "-c", client_cpus, memtier,
        "-s", "127.0.0.1", "-p", str(port),
        "-t", str(client_threads), "-c", "4",
        "--test-time", str(secs), "--pipeline", "16",
        "--key-maximum", str(MEMTIER_KEY_MAX),
        "--hide-histogram", "--json-out-file", json_path,
    ] + args
    if os.path.exists(json_path):
        os.remove(json_path)
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        with open(json_path) as f:
            return float(json.load(f)["ALL STATS"]["Totals"]["Ops/sec"])
    except Exception:
        return 0.0
    finally:
        if os.path.exists(json_path):
            os.remove(json_path)


def run_memtier_gate(rudis_bin, port, threads, launches, runs, secs):
    """Measure live SET/GET throughput with memtier. Returns ({wl: [ops...]}, errors)."""
    memtier = find_memtier()
    if memtier is None:
        return None, ["memtier_benchmark not found (set MEMTIER_BIN)"]
    if not os.path.exists(rudis_bin):
        return None, [f"Rudis binary not found: {rudis_bin} (run cargo build --release)"]
    server_cpus = f"16-{16 + threads - 1}"
    # 16 client threads, one per CPU on 0-15 (below the server's CPUs). With
    # only `threads` client CPUs (8), 1 KiB GET was client-bound at ~1.2M
    # ops/s, and its run-to-run noise read as small server regressions.
    client_threads = 16
    client_cpus = "0-15"
    json_path = f"/tmp/perf_gate_{port}.json"
    samples = {wl: [] for wl in MEMTIER_WORKLOADS}
    for _ in range(launches):
        proc = subprocess.Popen(
            ["taskset", "-c", server_cpus, rudis_bin, "--port", str(port), "--threads", str(threads)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            if not wait_ping(port, timeout=10.0):
                return None, [f"Rudis failed to start on port {port}"]
            for wl, spec in MEMTIER_WORKLOADS.items():
                if spec["populate"]:
                    # One connection walking the whole keyspace once, so reads hit.
                    subprocess.run(
                        ["taskset", "-c", client_cpus, memtier, "-s", "127.0.0.1", "-p", str(port),
                         "-t", "1", "-c", "1", "-n", str(MEMTIER_KEY_MAX),
                         "--key-minimum", "1", "--key-maximum", str(MEMTIER_KEY_MAX),
                         "--pipeline", "64", "--ratio", "1:0", "-d", "1024", "--key-pattern", "S:S",
                         "--hide-histogram"],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    )
                # Warmup (discarded): TCP slow start, allocator and table growth.
                _memtier_run(memtier, port, client_cpus, client_threads, spec["args"], 1, json_path)
                for _ in range(runs):
                    ops = _memtier_run(memtier, port, client_cpus, client_threads, spec["args"], secs, json_path)
                    if ops > 0:
                        samples[wl].append(ops)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
            time.sleep(0.5)
    return samples, []


def check_memtier_gate(samples, baseline_path, max_drop_pct):
    regressions, rows = [], []
    baseline = {}
    if os.path.exists(baseline_path):
        with open(baseline_path) as f:
            baseline = json.load(f).get("workloads", {})
    if not baseline:
        regressions.append(f"No live baseline at {baseline_path}; record one with --update-baseline")
    for wl, vals in samples.items():
        if not vals:
            regressions.append(f"MT-{wl}: no successful memtier runs")
            continue
        med = statistics.median(vals)
        base = baseline.get(wl, {}).get("ops_sec_median")
        if not base:
            rows.append((f"MT-{wl}", "throughput", f"{med:>12,.0f}/s", "no baseline", "SKIP"))
            continue
        floor = base * (1 - max_drop_pct / 100.0)
        delta = (med / base - 1) * 100
        status = "PASS" if med >= floor else "FAIL"
        if status == "FAIL":
            regressions.append(
                f"MT-{wl} throughput {med:,.0f} ops/s is {delta:+.1f}% vs baseline {base:,.0f} "
                f"(allowed -{max_drop_pct:.0f}%)"
            )
        rows.append((f"MT-{wl}", "throughput", f"{med:>12,.0f}/s", f">= {floor:,.0f} ({delta:+.1f}%)", status))
    return regressions, rows


# A/B mode (--baseline-rev/--baseline-bin): the candidate and a baseline build
# measured back to back in alternating launches, so host drift (other tenants,
# thermal state) hits both equally. Judged on the candidate/baseline ratio.
DEFAULT_MAX_AB_DROP_PCT = 5.0
# A side whose runs spread more than this (CV %) gets another round.
AB_CV_LIMIT_PCT = 5.0
BASELINE_CACHE = "/tmp/rudis_baseline_bins"


def busy_cpus(window=2.0):
    """CPUs' worth of non-idle time across the host over `window` seconds,
    from /proc/stat. (The load average would still count our own previous
    benchmark run for minutes.)"""
    def snap():
        with open("/proc/stat") as f:
            v = [int(x) for x in f.readline().split()[1:]]
        idle = v[3] + v[4]
        return sum(v) - idle, sum(v)
    b0, t0 = snap()
    time.sleep(window)
    b1, t1 = snap()
    return (b1 - b0) / max(1, t1 - t0) * (os.cpu_count() or 1)


def wait_quiet(max_busy, timeout=180.0):
    """Waits until other processes keep at most `max_busy` CPUs busy (other
    tenants' bursts otherwise put 20-30% noise into throughput runs).
    Returns the last measurement."""
    deadline = time.time() + timeout
    while True:
        busy = busy_cpus()
        if busy <= max_busy or time.time() >= deadline:
            return busy
        time.sleep(3)


def build_baseline(rev):
    """Path to a release binary of commit `rev`, built once per commit in a
    temporary worktree and cached under BASELINE_CACHE."""
    sha = subprocess.run(
        ["git", "-C", REPO_DIR, "rev-parse", rev], capture_output=True, text=True, check=True
    ).stdout.strip()
    out = os.path.join(BASELINE_CACHE, sha, "rudis")
    if os.path.exists(out):
        return out, sha
    wt = os.path.join(BASELINE_CACHE, "worktree")
    subprocess.run(["git", "-C", REPO_DIR, "worktree", "remove", "--force", wt],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["git", "-C", REPO_DIR, "worktree", "add", "--detach", wt, sha], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        env = dict(os.environ, CARGO_TARGET_DIR=os.path.join(BASELINE_CACHE, "target"))
        print(f"  Building baseline {sha[:10]} (cached for later runs)...", flush=True)
        subprocess.run(["cargo", "build", "--release", "--quiet"], cwd=wt, env=env, check=True)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        shutil.copy2(os.path.join(BASELINE_CACHE, "target", "release", "rudis"), out)
    finally:
        subprocess.run(["git", "-C", REPO_DIR, "worktree", "remove", "--force", wt],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return out, sha


def _cv(vals):
    return statistics.pstdev(vals) / statistics.mean(vals) * 100 if len(vals) > 1 else 0.0


def run_memtier_ab(cand_bin, base_bin, port, threads, rounds, max_rounds, runs, secs, max_busy):
    """Alternating launches of candidate and baseline (order swapped each
    round), waiting for a quiet host before each. Rounds continue past
    `rounds` (up to `max_rounds`) while either side's CV exceeds the limit.
    Returns ({"cand": {wl: [...]}, "base": {...}}, errors, notes)."""
    samples = {"cand": {wl: [] for wl in MEMTIER_WORKLOADS},
               "base": {wl: [] for wl in MEMTIER_WORKLOADS}}
    notes = []
    for r in range(max_rounds):
        order = (("cand", cand_bin), ("base", base_bin))
        if r % 2:
            order = order[::-1]
        for tag, binary in order:
            load = wait_quiet(max_busy)
            if load > max_busy:
                notes.append(f"round {r + 1} {tag}: host still busy ({load:.1f} CPUs)")
            got, errs = run_memtier_gate(binary, port, threads, 1, runs, secs)
            if errs:
                return None, errs, notes
            for wl, v in got.items():
                samples[tag][wl].extend(v)
        noisy = [
            f"{tag}/{wl} {_cv(v):.1f}%"
            for tag in samples for wl, v in samples[tag].items()
            if _cv(v) > AB_CV_LIMIT_PCT
        ]
        if r + 1 >= rounds and not noisy:
            break
        if r + 1 >= rounds and noisy:
            notes.append(f"round {r + 1}: noisy ({', '.join(noisy)}), adding a round")
    return samples, [], notes


def check_memtier_ab(samples, max_drop_pct):
    regressions, rows = [], []
    for wl in MEMTIER_WORKLOADS:
        cand, base = samples["cand"][wl], samples["base"][wl]
        if not cand or not base:
            regressions.append(f"MT-{wl}: no successful memtier runs")
            continue
        ratio = statistics.median(cand) / statistics.median(base)
        delta = (ratio - 1) * 100
        status = "PASS" if delta >= -max_drop_pct else "FAIL"
        if status == "FAIL":
            regressions.append(
                f"MT-{wl} throughput is {delta:+.1f}% vs the baseline build measured back to back "
                f"(allowed -{max_drop_pct:.0f}%; CV cand {_cv(cand):.1f}%, base {_cv(base):.1f}%)"
            )
        rows.append((
            f"MT-{wl}", "vs baseline build",
            f"{statistics.median(cand):>12,.0f}/s",
            f"{delta:+.1f}% (cv {_cv(cand):.0f}/{_cv(base):.0f})", status,
        ))
    return regressions, rows


def write_live_baseline(samples, baseline_path, threads, launches, runs, secs):
    data = {
        "_note": "Machine-specific memtier baseline for check_perf_regression.py --memtier. "
                 "Only update deliberately (--update-baseline), never automatically.",
        "host_cpus": os.cpu_count(),
        "server_threads": threads,
        "launches": launches,
        "runs_per_launch": runs,
        "secs_per_run": secs,
        "workloads": {
            wl: {"ops_sec_median": round(statistics.median(v), 2), "runs": [round(x, 2) for x in v]}
            for wl, v in samples.items() if v
        },
    }
    with open(baseline_path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def main():
    parser = argparse.ArgumentParser(description="Rudis performance & hardware counter regression gate")
    parser.add_argument("counter_file", nargs="?", default=DEFAULT_FILE, help="Path to benchmark_perf_counters.json")
    parser.add_argument("--live", action="store_true", help="Also run a live pipelined SET/GET check against rudis")
    parser.add_argument(
        "--memtier", action="store_true",
        help="Measure live SET/GET throughput with memtier and compare to the live baseline",
    )
    parser.add_argument("--update-baseline", action="store_true", help="With --memtier: record the live baseline")
    parser.add_argument("--live-baseline", default=DEFAULT_LIVE_BASELINE, help="Live baseline JSON for --memtier")
    parser.add_argument("--max-drop-pct", type=float, default=DEFAULT_MAX_DROP_PCT,
                        help="With --memtier: allowed throughput drop vs baseline")
    parser.add_argument("--server-threads", type=int, default=8, help="With --memtier: rudis --threads")
    parser.add_argument("--launches", type=int, default=2, help="With --memtier: server launches")
    parser.add_argument("--runs", type=int, default=2, help="With --memtier: measured runs per launch")
    parser.add_argument("--secs", type=int, default=3, help="With --memtier: seconds per run")
    parser.add_argument(
        "--baseline-rev",
        help="With --memtier: A/B against this commit (e.g. HEAD), built once and cached, "
             "instead of the stored baseline numbers",
    )
    parser.add_argument("--baseline-bin", help="With --memtier: A/B against this rudis binary")
    parser.add_argument("--max-ab-drop-pct", type=float, default=DEFAULT_MAX_AB_DROP_PCT,
                        help="With an A/B: allowed drop vs the baseline build")
    parser.add_argument("--rounds", type=int, default=3, help="With an A/B: minimum rounds")
    parser.add_argument("--max-rounds", type=int, default=4,
                        help="With an A/B: rounds allowed while results are noisy")
    parser.add_argument("--max-busy-cpus", type=float, default=2.0,
                        help="With an A/B: before each launch, wait until other processes "
                             "keep at most this many CPUs busy")
    parser.add_argument("--attach", action="store_true", help="Attach --live check to an already-running server on --port")
    parser.add_argument("--rudis-bin", default=DEFAULT_BIN, help="Path to rudis binary for --live")
    parser.add_argument("--port", type=int, default=17999, help="TCP port for --live server")
    parser.add_argument("--duration", type=float, default=1.5, help="Per-workload duration (s) for --live")
    parser.add_argument(
        "--live-throughput-floor",
        type=float,
        default=LIVE_THROUGHPUT_FLOOR_OPS,
        help="Minimum ops/sec floor for --live mode",
    )
    args = parser.parse_args()

    if not os.path.exists(args.counter_file):
        print(f"[!] Counter file not found: {args.counter_file}")
        sys.exit(1)

    with open(args.counter_file) as f:
        data = json.load(f)

    print("=" * 84)
    print("  RUDIS AUTOMATED HARDWARE COUNTER, THROUGHPUT & LATENCY REGRESSION GATE")
    print(f"  Stored results (NOT re-measured on this build): {args.counter_file}")
    if not (args.live or args.memtier):
        print("  [!] No live measurement requested: pass --memtier (or --live) to test this build.")
    print("=" * 84)

    regressions, rows = validate_counter_data(data)
    if args.memtier and (args.baseline_rev or args.baseline_bin):
        if args.baseline_bin:
            base_bin, label = args.baseline_bin, args.baseline_bin
        else:
            base_bin, sha = build_baseline(args.baseline_rev)
            label = f"{args.baseline_rev} ({sha[:10]})"
        print(f"  A/B: {args.rudis_bin} vs {label}")
        samples, errs, notes = run_memtier_ab(
            args.rudis_bin, base_bin, args.port, args.server_threads,
            args.rounds, args.max_rounds, args.runs, args.secs, args.max_busy_cpus,
        )
        for n in notes:
            print(f"  [note] {n}")
        regressions.extend(errs)
        if samples is not None:
            ab_regs, ab_rows = check_memtier_ab(samples, args.max_ab_drop_pct)
            regressions.extend(ab_regs)
            rows.extend(ab_rows)
    elif args.memtier:
        samples, errs = run_memtier_gate(
            args.rudis_bin, args.port, args.server_threads, args.launches, args.runs, args.secs
        )
        regressions.extend(errs)
        if samples is not None:
            if args.update_baseline:
                write_live_baseline(
                    samples, args.live_baseline, args.server_threads, args.launches, args.runs, args.secs
                )
                print(f"  Recorded live baseline: {args.live_baseline}")
            mt_regs, mt_rows = check_memtier_gate(samples, args.live_baseline, args.max_drop_pct)
            regressions.extend(mt_regs)
            rows.extend(mt_rows)
    if args.live:
        live_regs, live_rows = run_live_gate(
            args.rudis_bin,
            args.port,
            args.duration,
            attach=args.attach,
            throughput_floor=args.live_throughput_floor,
        )
        regressions.extend(live_regs)
        rows.extend(live_rows)

    print(f"\n{'Workload':<10} | {'Metric':<18} | {'Value':<14} | {'Threshold':<16} | {'Status':<8}")
    print("-" * 78)
    for wl, metric, val_s, thresh_s, status in rows:
        print(f"{wl:<10} | {metric:<18} | {val_s:<14} | {thresh_s:<16} | {status:<8}")
    print("-" * 78)

    if regressions:
        print("\n[!] GATE FAILURE: Regressions detected:")
        for r in regressions:
            print(f"  - {r}")
        sys.exit(1)
    else:
        print("\n[+] GATE SUCCESS: Zero hardware counter, throughput, or latency regressions.")
        sys.exit(0)


if __name__ == "__main__":
    main()
