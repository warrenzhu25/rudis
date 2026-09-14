#!/usr/bin/env python3
import socket
import subprocess
import time
import os
import sys
import json

RUDIS_BIN = "/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
MEMTIER_BIN = "/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"
PORT = 6389

def get_vm_rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        return 0
    return 0

def run_memory_benchmarks():
    print("==================================================")
    print(" Running Memory Footprint Benchmarks")
    print("==================================================")
    results = {}

    # 1. Benchmark RudisValue::Int vs RudisValue::String
    proc = subprocess.Popen(
        ["taskset", "-c", "0-3", RUDIS_BIN, "--threads", "4", "--port", str(PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.5)
    pid = proc.pid
    baseline_rss = get_vm_rss_kb(pid)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", PORT))

    print("Populating 1,000,000 integer keys (RudisValue::Int)...")
    batch_size = 5000
    batch = []
    t0 = time.time()
    for i in range(1_000_000):
        key = f"int:{i}".encode()
        val = f"{i}".encode()
        cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n".encode() + key + f"\r\n${len(val)}\r\n".encode() + val + b"\r\n"
        batch.append(cmd)
        if len(batch) >= batch_size:
            s.sendall(b"".join(batch))
            for _ in range(batch_size):
                s.recv(5)
            batch = []
    t_int = time.time() - t0
    int_rss = get_vm_rss_kb(pid)
    int_mem_mb = (int_rss - baseline_rss) / 1024.0
    bytes_per_int_key = (int_rss - baseline_rss) * 1024 / 1_000_000.0

    print(f"  -> 1,000,000 Int keys inserted in {t_int:.2f}s")
    print(f"  -> Memory used: {int_mem_mb:.2f} MB ({bytes_per_int_key:.1f} bytes/key)")

    # Test String keys
    print("Populating 1,000,000 string keys (RudisValue::String)...")
    rss_before_str = get_vm_rss_kb(pid)
    t0 = time.time()
    for i in range(1_000_000):
        key = f"str:{i}".encode()
        val = f"val_str_payload_{i}".encode()
        cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n".encode() + key + f"\r\n${len(val)}\r\n".encode() + val + b"\r\n"
        batch.append(cmd)
        if len(batch) >= batch_size:
            s.sendall(b"".join(batch))
            for _ in range(batch_size):
                s.recv(5)
            batch = []
    t_str = time.time() - t0
    str_rss = get_vm_rss_kb(pid)
    str_mem_mb = (str_rss - rss_before_str) / 1024.0
    bytes_per_str_key = (str_rss - rss_before_str) * 1024 / 1_000_000.0

    print(f"  -> 1,000,000 String keys inserted in {t_str:.2f}s")
    print(f"  -> Memory used: {str_mem_mb:.2f} MB ({bytes_per_str_key:.1f} bytes/key)")
    mem_saving_pct = (1.0 - int_mem_mb / str_mem_mb) * 100.0
    print(f"  -> Memory reduction with RudisValue::Int: {mem_saving_pct:.1f}%")

    s.close()
    proc.kill()
    proc.wait()

    # 2. Benchmark SmallHash
    proc = subprocess.Popen(
        ["taskset", "-c", "0-3", RUDIS_BIN, "--threads", "4", "--port", str(PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.5)
    pid = proc.pid
    baseline_rss = get_vm_rss_kb(pid)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", PORT))

    print("\nPopulating 100,000 small hashes (RudisValue::SmallHash, 4 fields each)...")
    batch = []
    t0 = time.time()
    for i in range(100_000):
        key = f"sh:{i}".encode()
        cmd = f"*10\r\n$4\r\nHSET\r\n${len(key)}\r\n".encode() + key + b"\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$2\r\nf4\r\n$2\r\nv4\r\n"
        batch.append(cmd)
        if len(batch) >= batch_size:
            s.sendall(b"".join(batch))
            for _ in range(batch_size):
                s.recv(4)
            batch = []
    t_sh = time.time() - t0
    sh_rss = get_vm_rss_kb(pid)
    sh_mem_mb = (sh_rss - baseline_rss) / 1024.0
    bytes_per_hash = (sh_rss - baseline_rss) * 1024 / 100_000.0

    print(f"  -> 100,000 SmallHash inserted in {t_sh:.2f}s")
    print(f"  -> Memory used: {sh_mem_mb:.2f} MB ({bytes_per_hash:.1f} bytes/hash)")

    s.close()
    proc.kill()
    proc.wait()

    results["int_vs_string"] = {
        "int_keys_count": 1_000_000,
        "int_memory_mb": round(int_mem_mb, 2),
        "int_bytes_per_key": round(bytes_per_int_key, 1),
        "str_keys_count": 1_000_000,
        "str_memory_mb": round(str_mem_mb, 2),
        "str_bytes_per_key": round(bytes_per_str_key, 1),
        "int_memory_reduction_pct": round(mem_saving_pct, 1),
    }
    results["small_hash"] = {
        "hash_count": 100_000,
        "fields_per_hash": 4,
        "memory_mb": round(sh_mem_mb, 2),
        "bytes_per_hash": round(bytes_per_hash, 1),
    }

    return results

def run_throughput_benchmarks():
    print("\n==================================================")
    print(" Running 16-Thread Throughput Benchmarks")
    print("==================================================")
    proc = subprocess.Popen(
        ["taskset", "-c", "0-15", RUDIS_BIN, "--threads", "16", "--port", str(PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)

    results = {}

    def parse_memtier_output(out_str):
        metrics = {}
        for line in out_str.splitlines():
            line = line.strip()
            if line.startswith("Sets") or line.startswith("Gets") or line.startswith("Totals"):
                parts = line.split()
                if len(parts) >= 5:
                    try:
                        metrics["ops_sec"] = float(parts[1])
                        metrics["hits_sec"] = float(parts[2])
                        metrics["misses_sec"] = float(parts[3])
                        metrics["avg_lat_ms"] = float(parts[4])
                        metrics["p50_lat_ms"] = float(parts[5]) if len(parts) > 5 else 0.0
                        metrics["p99_lat_ms"] = float(parts[6]) if len(parts) > 6 else 0.0
                        metrics["kb_sec"] = float(parts[7]) if len(parts) > 7 else 0.0
                    except (ValueError, IndexError):
                        pass
        return metrics

    # SET Benchmark
    print("Benchmarking SET (1KB payload, pipeline 100, 32 clients on cores 16-47)...")
    cmd = [
        "taskset", "-c", "16-47",
        MEMTIER_BIN,
        "--server", "127.0.0.1", "--port", str(PORT),
        "--clients", "1", "--threads", "32",
        "--ratio", "1:0", "--data-size", "1024",
        "--pipeline", "100",
        "--key-minimum", "1", "--key-maximum", "1000000",
        "--key-pattern", "S:S",
        "--test-time", "15",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    set_metrics = parse_memtier_output(res.stdout)
    results["SET_1KB"] = set_metrics
    ops = set_metrics.get("ops_sec", 0)
    p50 = set_metrics.get("p50_lat_ms", 0)
    p99 = set_metrics.get("p99_lat_ms", 0)
    mb_s = set_metrics.get("kb_sec", 0) / 1024.0
    print(f"  -> SET Throughput: {ops:,.0f} ops/sec ({mb_s:.1f} MB/sec)")
    print(f"  -> Latency: p50={p50:.2f}ms, p99={p99:.2f}ms")

    # GET Benchmark
    print("\nBenchmarking GET (1KB payload, pipeline 100, 32 clients on cores 16-47)...")
    cmd = [
        "taskset", "-c", "16-47",
        MEMTIER_BIN,
        "--server", "127.0.0.1", "--port", str(PORT),
        "--clients", "1", "--threads", "32",
        "--ratio", "0:1", "--data-size", "1024",
        "--pipeline", "100",
        "--key-minimum", "1", "--key-maximum", "1000000",
        "--key-pattern", "S:S",
        "--test-time", "15",
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    get_metrics = parse_memtier_output(res.stdout)
    results["GET_1KB"] = get_metrics
    ops = get_metrics.get("ops_sec", 0)
    p50 = get_metrics.get("p50_lat_ms", 0)
    p99 = get_metrics.get("p99_lat_ms", 0)
    mb_s = get_metrics.get("kb_sec", 0) / 1024.0
    print(f"  -> GET Throughput: {ops:,.0f} ops/sec ({mb_s:.1f} MB/sec)")
    print(f"  -> Latency: p50={p50:.2f}ms, p99={p99:.2f}ms")

    proc.kill()
    proc.wait()

    return results

def main():
    final_report = {}
    final_report["memory"] = run_memory_benchmarks()
    final_report["throughput_16t"] = run_throughput_benchmarks()

    os.makedirs("/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs", exist_ok=True)
    out_file = "/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs/memory_and_throughput_results.json"
    with open(out_file, "w") as f:
        json.dump(final_report, f, indent=2)

    print("\n==================================================")
    print(" BENCHMARK RESULTS SUMMARY")
    print("==================================================")
    print(json.dumps(final_report, indent=2))
    print(f"\nResults successfully written to {out_file}")

if __name__ == "__main__":
    main()
