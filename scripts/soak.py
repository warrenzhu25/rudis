#!/usr/bin/env python3
"""Soak test: a master and a replica under sustained mixed load for hours,
sampled every minute, with a report at the end.

  scripts/soak.py --binary target/release/rudis --hours 24 --dir /tmp/soak

What runs:
  * master (4 shards, AOF on, background saves, maxmemory with allkeys-lru so
    eviction is always active, replication backpressure and dual-channel
    full syncs on) and a replica of it (4 shards, AOF on);
  * memtier load, rate-limited: a mixed SET/GET workload over a key space
    larger than maxmemory, plus a second memtier writing keys with TTLs;
  * BGSAVE on the master every 10 minutes and BGREWRITEAOF on the replica
    every 30.

Every --interval seconds it records (samples.jsonl) memory (used and RSS),
commands processed, evictions, expirations, replication offsets and link
state, full syncs, output-buffer disconnections, persistence status,
backpressure counters, and the p50/p99/max latency of 200 GET probes on
each server. It stops early if a server exits. At the end it stops the load,
waits for the replica (WAIT), compares DEBUG DIGEST, and writes report.md:
RSS and latency trends (first hour against the last), counters, and every
anomaly seen. Processes are pinned with taskset to --cores (a range, e.g.
16-31), split between the servers and memtier, so the host stays usable.
"""

import argparse
import json
import os
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time

MEMTIER = os.path.expanduser("~/memtier_benchmark/memtier_benchmark")


def resp(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Conn:
    def __init__(self, port, timeout=10):
        self.port = port
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(1 << 16)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n):
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(1 << 16)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2 :]
        return data

    def reply(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind in (b"+", b"-", b":"):
            return line.decode(errors="replace")
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n).decode(errors="replace")
        if kind == b"*":
            return [self.reply() for _ in range(int(rest))]
        raise ValueError(f"bad reply {line!r}")

    def cmd(self, *args):
        self.sock.sendall(resp(*args))
        return self.reply()


def info(port):
    text = Conn(port).cmd("INFO", "everything")
    out = {}
    for line in (text or "").splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def latency_probe(port, n=200):
    c = Conn(port)
    times = []
    for i in range(n):
        t = time.perf_counter()
        c.cmd("GET", f"memtier-{i * 7919 % 1000000}")
        times.append((time.perf_counter() - t) * 1000)
    times.sort()
    return {
        "p50_ms": round(times[n // 2], 3),
        "p99_ms": round(times[int(n * 0.99) - 1], 3),
        "max_ms": round(times[-1], 3),
    }


def wait_port(port, proc, secs=30):
    end = time.time() + secs
    while time.time() < end:
        if proc.poll() is not None:
            raise SystemExit(f"server on {port} exited at startup ({proc.returncode})")
        try:
            if Conn(port, timeout=1).cmd("PING") == "+PONG":
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise SystemExit(f"server on {port} did not start")


def cpus(spec):
    lo, hi = (int(x) for x in spec.split("-"))
    return list(range(lo, hi + 1))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", required=True)
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--interval", type=int, default=60, help="seconds between samples")
    ap.add_argument("--dir", required=True, help="working directory (wiped)")
    ap.add_argument("--cores", default="16-31")
    ap.add_argument("--port", type=int, default=21000, help="master port; replica is +1")
    ap.add_argument("--maxmemory", default="2gb")
    ap.add_argument("--rate", type=int, default=2000, help="memtier requests/s per connection")
    a = ap.parse_args()

    shutil.rmtree(a.dir, ignore_errors=True)
    os.makedirs(a.dir)
    cores = cpus(a.cores)
    q = max(1, len(cores) // 4)
    master_cores, replica_cores, load_cores = cores[:q], cores[q : 2 * q], cores[2 * q :]
    mp, rp = a.port, a.port + 1
    binary = os.path.abspath(a.binary)
    shutil.copy(binary, os.path.join(a.dir, "rudis"))  # immune to rebuilds
    binary = os.path.join(a.dir, "rudis")

    def server(port, name, core_list, extra):
        d = os.path.join(a.dir, name)
        os.makedirs(d)
        args = [
            "taskset", "-c", ",".join(map(str, core_list)), binary,
            "--port", str(port), "--threads", "4", "--no-pin", "--dir", d,
            "--appendonly", "yes", "--save", "", "--enable-debug-command", "yes", "--logfile", os.path.join(a.dir, f"{name}.log"),
        ] + extra
        return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=open(os.path.join(a.dir, f"{name}.err"), "w"))

    master = server(mp, "master", master_cores, [
        "--maxmemory", a.maxmemory, "--maxmemory-policy", "allkeys-lru",
        "--repl-backpressure-threshold", "64mb", "--repl-backpressure-max-delay", "100",
        "--dual-channel-replication-enabled", "yes",
        "--client-output-buffer-limit", "replica 512mb 256mb 60",
    ])
    replica = server(rp, "replica", replica_cores, ["--dual-channel-replication-enabled", "yes"])
    wait_port(mp, master)
    wait_port(rp, replica)
    Conn(rp).cmd("REPLICAOF", "127.0.0.1", str(mp))

    duration = int(a.hours * 3600)
    load_cpu = ",".join(map(str, load_cores))
    mixed = subprocess.Popen([
        "taskset", "-c", load_cpu, MEMTIER, "--server", "127.0.0.1", "--port", str(mp),
        "--threads", "2", "--clients", "10", "--ratio", "1:3", "--data-size-range", "32-2048",
        "--key-maximum", "4000000", "--key-pattern", "R:R", "--rate-limiting", str(a.rate),
        "--test-time", str(duration), "--hide-histogram", "--pipeline", "1",
    ], stdout=open(os.path.join(a.dir, "memtier_mixed.out"), "w"), stderr=subprocess.STDOUT)
    ttl = subprocess.Popen([
        "taskset", "-c", load_cpu, MEMTIER, "--server", "127.0.0.1", "--port", str(mp),
        "--threads", "1", "--clients", "4", "--ratio", "1:0", "--data-size", "128",
        "--key-prefix", "ttl-", "--key-maximum", "2000000", "--expiry-range", "5-600",
        "--rate-limiting", str(a.rate // 2), "--test-time", str(duration), "--hide-histogram",
    ], stdout=open(os.path.join(a.dir, "memtier_ttl.out"), "w"), stderr=subprocess.STDOUT)

    samples_path = os.path.join(a.dir, "samples.jsonl")
    anomalies = []
    start = time.time()
    last_bgsave = last_rewrite = start
    keep = [
        "used_memory", "used_memory_rss", "total_commands_processed", "instantaneous_ops_per_sec",
        "evicted_keys", "expired_keys", "keyspace_hits", "keyspace_misses", "sync_full",
        "sync_partial_ok", "client_output_buffer_limit_disconnections", "master_repl_offset",
        "connected_slaves", "master_link_status", "slave_repl_offset", "rdb_last_bgsave_status",
        "aof_last_write_status", "aof_last_bgrewrite_status", "repl_backpressure_congested_replicas",
        "repl_backpressure_delayed_writes", "rejected_calls", "total_error_replies",
    ]

    def stop(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    try:
        with open(samples_path, "w") as out:
            while time.time() - start < duration:
                time.sleep(a.interval)
                now = time.time()
                row = {"t": round(now - start)}
                for name, port, proc in (("master", mp, master), ("replica", rp, replica)):
                    if proc.poll() is not None:
                        anomalies.append(f"{row['t']}s: {name} exited ({proc.returncode})")
                        raise KeyboardInterrupt
                    try:
                        i = info(port)
                        row[name] = {k: i.get(k) for k in keep}
                        # Which errors (e.g. errorstat_ERR:count=3), so error
                        # replies can be told apart afterwards.
                        row[name].update({k: v for k, v in i.items() if k.startswith("errorstat_")})
                        row[name].update(latency_probe(port))
                    except Exception as e:  # noqa: BLE001
                        anomalies.append(f"{row['t']}s: {name} probe failed: {e}")
                for proc, name in ((mixed, "memtier mixed"), (ttl, "memtier ttl")):
                    if proc.poll() not in (None, 0):
                        anomalies.append(f"{row['t']}s: {name} exited ({proc.returncode})")
                m, r = row.get("master", {}), row.get("replica", {})
                if r.get("master_link_status") not in (None, "up"):
                    anomalies.append(f"{row['t']}s: replica link {r.get('master_link_status')}")
                for k in ("rdb_last_bgsave_status", "aof_last_write_status", "aof_last_bgrewrite_status"):
                    for name, s in (("master", m), ("replica", r)):
                        if s.get(k) not in (None, "ok"):
                            anomalies.append(f"{row['t']}s: {name} {k}={s.get(k)}")
                # SCHEDULE: an automatic AOF rewrite may be running; the
                # save then starts after it instead of failing. Without it,
                # the harness's own BGSAVEs were the master's only error
                # replies in the first 24 h run.
                if now - last_bgsave >= 600:
                    reply = Conn(mp).cmd("BGSAVE", "SCHEDULE")
                    if str(reply).startswith("-"):
                        anomalies.append(f"{row['t']}s: master BGSAVE SCHEDULE: {reply}")
                    last_bgsave = now
                if now - last_rewrite >= 1800:
                    reply = Conn(rp).cmd("BGREWRITEAOF")
                    if str(reply).startswith("-"):
                        anomalies.append(f"{row['t']}s: replica BGREWRITEAOF: {reply}")
                    last_rewrite = now
                out.write(json.dumps(row) + "\n")
                out.flush()
    except KeyboardInterrupt:
        pass
    finally:
        for p in (mixed, ttl):
            if p.poll() is None:
                p.terminate()
        for p in (mixed, ttl):
            try:
                p.wait(30)
            except subprocess.TimeoutExpired:
                p.kill()

    # Settle and compare.
    digest = {}
    try:
        # TTL keys expire on each side's clock, so digests taken one after
        # the other would race them; delete them (replicated) first.
        mc, cur, dropped = Conn(mp, timeout=60), "0", 0
        while True:
            cur, ks = mc.cmd("SCAN", cur, "MATCH", "ttl-*", "COUNT", "10000")
            if ks:
                mc.cmd("UNLINK", *ks)
                dropped += len(ks)
            if cur == "0":
                break
        waited = Conn(mp, timeout=60).cmd("WAIT", "1", "30000")
        time.sleep(2)
        digest = {
            "ttl_keys_dropped": dropped,
            "wait": waited,
            "master": Conn(mp, timeout=120).cmd("DEBUG", "DIGEST"),
            "replica": Conn(rp, timeout=120).cmd("DEBUG", "DIGEST"),
            "master_dbsize": Conn(mp).cmd("DBSIZE"),
            "replica_dbsize": Conn(rp).cmd("DBSIZE"),
        }
    except Exception as e:  # noqa: BLE001
        anomalies.append(f"final comparison failed: {e}")
    for p in (master, replica):
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(60)
            except subprocess.TimeoutExpired:
                p.kill()
    write_report(a, samples_path, anomalies, digest)


def write_report(a, samples_path, anomalies, digest):
    rows = [json.loads(l) for l in open(samples_path)] if os.path.exists(samples_path) else []
    lines = [f"# Soak report ({len(rows)} samples, {a.hours} h planned)", ""]
    if not rows:
        lines += ["No samples."]
    else:
        hour = max(1, 3600 // a.interval)
        first, last = rows[:hour], rows[-hour:]

        def avg(rs, who, k):
            vals = [float(r[who][k]) for r in rs if who in r and r[who].get(k) not in (None, "")]
            return statistics.mean(vals) if vals else float("nan")

        lines += ["| | first hour | last hour |", "| :--- | ---: | ---: |"]
        for who in ("master", "replica"):
            for k, scale, unit in (
                ("used_memory", 2**20, "MB"), ("used_memory_rss", 2**20, "MB"),
                ("instantaneous_ops_per_sec", 1, "ops/s"), ("p50_ms", 1, "ms"), ("p99_ms", 1, "ms"),
                ("max_ms", 1, "ms"),
            ):
                lines.append(f"| {who} {k} ({unit}) | {avg(first, who, k) / scale:.2f} | {avg(last, who, k) / scale:.2f} |")
        end = rows[-1]
        lines += ["", "## Counters at the end", ""]
        for who in ("master", "replica"):
            s = end.get(who, {})
            lines.append(f"* {who}: " + ", ".join(
                f"{k}={s.get(k)}" for k in (
                    "total_commands_processed", "evicted_keys", "expired_keys", "sync_full",
                    "sync_partial_ok", "client_output_buffer_limit_disconnections",
                    "repl_backpressure_delayed_writes", "total_error_replies",
                ) if s.get(k) is not None))
        for who in ("master", "replica"):
            errs = {k: v for k, v in end.get(who, {}).items() if k.startswith("errorstat_")}
            lines.append(f"* {who} errorstats: " + (", ".join(f"{k}={v}" for k, v in sorted(errs.items())) or "none"))
        worst = max((float(r[w]["max_ms"]) for r in rows for w in ("master", "replica") if w in r), default=0)
        lines.append(f"* worst single probe latency: {worst:.1f} ms")
    lines += ["", "## Final comparison", "", f"```\n{json.dumps(digest, indent=2)}\n```", ""]
    md, rd = digest.get("master"), digest.get("replica")
    valid = isinstance(md, str) and not md.startswith("-") and isinstance(rd, str) and not rd.startswith("-")
    same = valid and md == rd
    if not same:
        anomalies = list(anomalies) + ["replica digest differs from master (or digest unavailable)"]
    lines.append(f"Replica matches master: **{'yes' if same else 'NO'}**")
    lines += ["", "## Anomalies", ""] + ([f"* {x}" for x in anomalies] or ["None."])
    open(os.path.join(a.dir, "report.md"), "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    sys.exit(main())
