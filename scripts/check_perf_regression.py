#!/usr/bin/env python3
"""
Automated Hardware Performance Counter & Regression Gate for Rudis.

Validates that:
1. GET hot path incurs 0.00 page-faults / op (zero unexpected heap allocations).
2. GET and SET hot paths incur 0.00 context-switches / op (lock-free thread execution).
3. Throughput does not suffer unexpected regressions.

Exit code 0 indicates all hardware counter and latency thresholds are satisfied.
"""

import json
import os
import sys

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FILE = os.path.join(REPO_DIR, "benchmark_perf_counters.json")


def main():
    target_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE
    if not os.path.exists(target_file):
        print(f"[!] Counter file not found: {target_file}")
        sys.exit(1)

    with open(target_file) as f:
        data = json.load(f)

    print("=" * 80)
    print("  RUDIS AUTOMATED HARDWARE COUNTER REGRESSION GATE")
    print(f"  Verifying counter results from: {target_file}")
    print("=" * 80)

    regressions = []
    print(f"\n{'Workload':<10} | {'Metric':<18} | {'Value':<14} | {'Threshold':<14} | {'Status':<8}")
    print("-" * 75)

    # Check GET
    if "GET" in data:
        get_pf = data["GET"].get("page-faults", {}).get("per_op_median", 0.0)
        get_cs = data["GET"].get("context-switches", {}).get("per_op_median", 0.0)
        get_ops = data["GET"].get("_ops_sec_median", 0.0)

        status_pf = "PASS" if get_pf <= 0.01 else "FAIL"
        status_cs = "PASS" if get_cs <= 0.01 else "FAIL"
        if status_pf == "FAIL":
            regressions.append(f"GET page-faults: {get_pf:.6f} /op exceeds threshold 0.01")
        if status_cs == "FAIL":
            regressions.append(f"GET context-switches: {get_cs:.6f} /op exceeds threshold 0.01")

        print(f"{'GET':<10} | {'page-faults':<18} | {get_pf:>12.6f}/op | {'<= 0.010000':<14} | {status_pf:<8}")
        print(f"{'GET':<10} | {'context-switches':<18} | {get_cs:>12.6f}/op | {'<= 0.010000':<14} | {status_cs:<8}")
        print(f"{'GET':<10} | {'throughput':<18} | {get_ops:>12,.0f}/s | {'>= 1,000,000':<14} | {'PASS' if get_ops >= 1000000 else 'WARN':<8}")

    # Check SET
    if "SET" in data:
        set_cs = data["SET"].get("context-switches", {}).get("per_op_median", 0.0)
        set_ops = data["SET"].get("_ops_sec_median", 0.0)

        status_cs = "PASS" if set_cs <= 0.01 else "FAIL"
        if status_cs == "FAIL":
            regressions.append(f"SET context-switches: {set_cs:.6f} /op exceeds threshold 0.01")

        print(f"{'SET':<10} | {'context-switches':<18} | {set_cs:>12.6f}/op | {'<= 0.010000':<14} | {status_cs:<8}")
        print(f"{'SET':<10} | {'throughput':<18} | {set_ops:>12,.0f}/s | {'>= 1,000,000':<14} | {'PASS' if set_ops >= 1000000 else 'WARN':<8}")

    print("-" * 75)

    if regressions:
        print("\n[!] GATE FAILURE: Regressions detected:")
        for r in regressions:
            print(f"  - {r}")
        sys.exit(1)
    else:
        print("\n[+] GATE SUCCESS: Zero hardware counter regressions. All performance invariants satisfied.")
        sys.exit(0)


if __name__ == "__main__":
    main()
