#!/usr/bin/env python3
"""Memory per key: Rudis vs Valkey vs Dragonfly.

For each server and dataset: start it fresh, note its resident memory
(VmRSS), fill it with string keys `key:<n>` holding random (incompressible)
values of the given size, sent as pipelined SETs from 8 connections, wait
for it to settle, and report the RSS growth per key and the server's own
`used_memory`. Every server gets the same cores and thread count;
persistence is off. (DEBUG POPULATE is not used: its values are one
repeated byte, which Dragonfly compresses, so it would not compare like
with like.)

    RUDIS_BIN=target/release/rudis VALKEY_BIN=... DRAGONFLY_BIN=... \\
        python3 scripts/benchmark_memory.py [--threads 8] [--cores 0-7]

Results go to stdout as a Markdown table (and --json for raw numbers).
"""

import argparse
import json
import multiprocessing
import os
import random
import socket
import subprocess
import sys
import tempfile
import time

DATASETS = [  # (keys, value bytes)
    (10_000_000, 32),
    (1_000_000, 1024),
]


def cmd(port, *args, timeout=600):
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    out = b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n%s\r\n" % (len(a), a) for a in (x.encode() if isinstance(x, str) else x for x in args))
    s.sendall(out)
    buf = b""
    while True:
        chunk = s.recv(1 << 20)
        if not chunk:
            break
        buf += chunk
        if buf.startswith((b"+", b"-", b":")) and buf.endswith(b"\r\n"):
            break
        if buf.startswith(b"$"):
            hdr, _, rest = buf.partition(b"\r\n")
            n = int(hdr[1:])
            if n < 0 or len(rest) >= n + 2:
                break
    s.close()
    return buf.decode(errors="replace")


FILL_CONNECTIONS = 8
FILL_BATCH = 1000


def fill_range(args):
    """SETs key:<lo>..key:<hi-1> to random values, pipelined."""
    port, lo, hi, size = args
    rnd = random.Random(lo)
    pool = rnd.randbytes(1 << 22)  # random values are slices of this
    s = socket.create_connection(("127.0.0.1", port), timeout=600)
    pending = 0
    for start in range(lo, hi, FILL_BATCH):
        out = []
        for n in range(start, min(start + FILL_BATCH, hi)):
            key = b"key:%d" % n
            off = rnd.randrange(len(pool) - size)
            out.append(b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n"
                       % (len(key), key, size, pool[off:off + size]))
        s.sendall(b"".join(out))
        pending += len(out)
        # Read the +OKs back (5 bytes each) so the server never stalls.
        need = pending * 5
        got = 0
        while got < need:
            chunk = s.recv(1 << 20)
            if not chunk:
                raise SystemExit(f"connection to {port} closed while filling")
            if b"-" in chunk:
                raise SystemExit(f"SET failed on {port}: {chunk[:200]!r}")
            got += len(chunk)
        pending = 0
    s.close()


def fill(port, keys, size):
    step = (keys + FILL_CONNECTIONS - 1) // FILL_CONNECTIONS
    jobs = [(port, lo, min(lo + step, keys), size) for lo in range(0, keys, step)]
    with multiprocessing.Pool(len(jobs)) as pool:
        pool.map(fill_range, jobs)


def rss_bytes(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return 0


def info_field(port, field):
    for line in cmd(port, "INFO", "memory").splitlines():
        if line.startswith(field + ":"):
            return int(line.split(":", 1)[1])
    return None


def wait_ready(port, proc, deadline=60):
    end = time.time() + deadline
    while time.time() < end:
        if proc.poll() is not None:
            raise SystemExit(f"server on {port} exited ({proc.returncode})")
        try:
            if "PONG" in cmd(port, "PING", timeout=1):
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise SystemExit(f"server on {port} never answered")


def launch(name, binary, port, threads, cores, workdir):
    if name == "rudis":
        args = [binary, "--port", str(port), "--threads", str(threads), "--save", "",
                "--dir", workdir]
    elif name == "valkey":
        args = [binary, "--port", str(port), "--save", "", "--appendonly", "no",
                "--dir", workdir]
    else:  # dragonfly
        args = [binary, "--port", str(port), f"--proactor_threads={threads}", "--dbfilename=",
                f"--dir={workdir}", "--logtostderr=false", f"--log_dir={workdir}"]
    proc = subprocess.Popen(["taskset", "-c", cores] + args,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_ready(port, proc)
    return proc


def measure(name, binary, port, threads, cores, keys, size):
    with tempfile.TemporaryDirectory(prefix=f"memb-{name}-") as d:
        proc = launch(name, binary, port, threads, cores, d)
        try:
            time.sleep(1)
            base = rss_bytes(proc.pid)
            base_used = info_field(port, "used_memory") or 0
            t = time.time()
            fill(port, keys, size)
            fill_s = time.time() - t
            dbsize = int(cmd(port, "DBSIZE").strip()[1:])
            if dbsize != keys:
                raise SystemExit(f"{name}: DBSIZE {dbsize}, wanted {keys}")
            time.sleep(3)
            rss = rss_bytes(proc.pid)
            used = info_field(port, "used_memory") or 0
            return {
                "server": name, "keys": keys, "value_bytes": size,
                "rss_base": base, "rss": rss,
                "rss_per_key": (rss - base) / keys,
                "used_memory_per_key": (used - base_used) / keys,
                "fill_seconds": round(fill_s, 1),
            }
        finally:
            proc.kill()
            proc.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--cores", default="0-7")
    ap.add_argument("--port", type=int, default=24000)
    ap.add_argument("--json", help="write raw results here")
    a = ap.parse_args()
    servers = [(n, os.environ.get(v)) for n, v in
               (("rudis", "RUDIS_BIN"), ("valkey", "VALKEY_BIN"), ("dragonfly", "DRAGONFLY_BIN"))]
    servers = [(n, b) for n, b in servers if b]
    if not servers:
        sys.exit("set RUDIS_BIN, VALKEY_BIN and/or DRAGONFLY_BIN")
    results = []
    for keys, size in DATASETS:
        for i, (name, binary) in enumerate(servers):
            r = measure(name, binary, a.port + i, a.threads, a.cores, keys, size)
            print(json.dumps(r), file=sys.stderr, flush=True)
            results.append(r)
    if a.json:
        with open(a.json, "w") as f:
            json.dump(results, f, indent=1)
    names = [n for n, _ in servers]
    print("| Dataset | " + " | ".join(f"{n} (RSS/key)" for n in names) + " |")
    print("| :--- |" + " ---: |" * len(names))
    for keys, size in DATASETS:
        row = [r for r in results if r["keys"] == keys and r["value_bytes"] == size]
        cells = []
        for n in names:
            r = next(x for x in row if x["server"] == n)
            cells.append(f"{r['rss_per_key']:.0f} B ({r['rss'] / 2**30:.2f} GiB)")
        print(f"| {keys:,} keys x {size} B | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
