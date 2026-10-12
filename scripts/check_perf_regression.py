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

Exit code 0 indicates all hardware counter, throughput, and latency thresholds are satisfied.
"""

import argparse
import json
import os
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


def main():
    parser = argparse.ArgumentParser(description="Rudis performance & hardware counter regression gate")
    parser.add_argument("counter_file", nargs="?", default=DEFAULT_FILE, help="Path to benchmark_perf_counters.json")
    parser.add_argument("--live", action="store_true", help="Also run a live pipelined SET/GET check against rudis")
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
    print(f"  Verifying baseline results from: {args.counter_file}")
    print("=" * 84)

    regressions, rows = validate_counter_data(data)
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
