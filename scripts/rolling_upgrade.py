#!/usr/bin/env python3
"""Rolling upgrade of a master + replica under write load (docs/operations.md §7).

Starts a master and a replica (AOF on) from --old, runs writers that INCR
their own counters and follow whichever node is the master, then:

  1. restarts the replica on --new (SIGTERM, start),
  2. FAILOVER on the master: the replica takes over,
  3. restarts the old master (now a replica) on --new,
  4. FAILOVER back.

Checks that no acknowledged write is lost (each counter is at least the
INCRs acknowledged, and at most those plus the ones whose reply was lost),
that the replica's DEBUG DIGEST matches the master's, and reports per step
the longest gap with no acknowledged write and the full/partial resyncs.
Exits 1 on a failed check.

usage: rolling_upgrade.py --old <bin> --new <bin> [--dir /tmp/rudis-upgrade]
"""
import argparse
import os
import shutil
import socket
import subprocess
import sys
import threading
import time


def resp(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Conn:
    def __init__(self, port, timeout=5):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            d = self.sock.recv(65536)
            if not d:
                raise ConnectionError("closed")
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n):
        while len(self.buf) < n + 2:
            d = self.sock.recv(65536)
            if not d:
                raise ConnectionError("closed")
            self.buf += d
        v, self.buf = self.buf[:n], self.buf[n + 2:]
        return v

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

    def close(self):
        self.sock.close()


def try_cmd(port, *args, timeout=5):
    try:
        c = Conn(port, timeout)
        try:
            return c.cmd(*args)
        finally:
            c.close()
    except (OSError, ConnectionError, ValueError) as e:
        return f"!{e}"


def info(port):
    text = try_cmd(port, "INFO", "everything")
    out = {}
    if isinstance(text, str) and not text.startswith("!"):
        for line in text.splitlines():
            if ":" in line and not line.startswith("#"):
                k, v = line.split(":", 1)
                out[k.strip()] = v.strip()
    return out


class Cluster:
    """Two nodes, each a (port, dir) whose process can be restarted."""

    def __init__(self, base, ports, cores):
        self.base, self.ports, self.cores = base, ports, cores
        self.procs = {}

    def start(self, port, binary, replicaof=None):
        d = os.path.join(self.base, str(port))
        os.makedirs(d, exist_ok=True)
        args = ["taskset", "-c", self.cores[port], binary, "--port", str(port), "--threads", "4",
                "--no-pin", "--dir", d, "--appendonly", "yes", "--save", "",
                "--enable-debug-command", "yes", "--logfile", os.path.join(self.base, f"{port}.log")]
        if replicaof:
            args += ["--replicaof", f"127.0.0.1 {replicaof}"]
        self.procs[port] = subprocess.Popen(args, stdout=subprocess.DEVNULL,
                                            stderr=open(os.path.join(self.base, f"{port}.err"), "a"))
        deadline = time.time() + 120
        while time.time() < deadline:
            r = try_cmd(port, "PING")
            if r == "+PONG":
                return
            if self.procs[port].poll() is not None:
                raise SystemExit(f"node {port} exited ({self.procs[port].returncode})")
            time.sleep(0.05)
        raise SystemExit(f"node {port} never answered")

    def stop(self, port):
        p = self.procs.pop(port)
        p.terminate()
        try:
            p.wait(60)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
            return "killed after 60 s"
        return f"exit {p.returncode}"

    def stop_all(self):
        for port in list(self.procs):
            self.stop(port)


def role(port):
    r = try_cmd(port, "ROLE", timeout=2)
    return r[0] if isinstance(r, list) and r else None


class Writers:
    """Threads INCRing their own keys on whichever node is the master."""

    def __init__(self, ports, n=8):
        self.ports, self.n = ports, n
        self.acked = [0] * n
        self.uncertain = [0] * n
        self.errors = {}
        self.ack_times = []
        self.lock = threading.Lock()
        self.stop = False
        self.threads = [threading.Thread(target=self.run, args=(i,), daemon=True) for i in range(n)]

    def master(self):
        for p in self.ports:
            if role(p) == "master":
                return p
        return None

    def run(self, i):
        key = f"upg:{{{i}}}:c"
        conn = None
        while not self.stop:
            if conn is None:
                port = self.master()
                if port is None:
                    time.sleep(0.05)
                    continue
                try:
                    conn = Conn(port, timeout=2)
                except OSError:
                    time.sleep(0.05)
                    continue
            try:
                r = conn.cmd("INCR", key)
            except (OSError, ConnectionError, ValueError):
                # Sent, reply lost: it may or may not have been applied.
                self.uncertain[i] += 1
                conn.close()
                conn = None
                continue
            if r.startswith(":"):
                self.acked[i] += 1
                with self.lock:
                    self.ack_times.append(time.time())
            else:
                with self.lock:
                    kind = r.split(" ", 1)[0]
                    self.errors[kind] = self.errors.get(kind, 0) + 1
                conn.close()
                conn = None
                time.sleep(0.01)
            time.sleep(0.0005)

    def longest_gap(self, start, end):
        with self.lock:
            ts = [t for t in self.ack_times if start <= t <= end]
        ts = [start] + ts + [end]
        return max(b - a for a, b in zip(ts, ts[1:]))


def wait_synced(master, replica, secs=120):
    """Until the replica's link is up and its offset reaches the master's."""
    deadline = time.time() + secs
    while time.time() < deadline:
        mi, ri = info(master), info(replica)
        if ri.get("master_link_status") == "up":
            mo, ro = int(mi.get("master_repl_offset", -1)), int(ri.get("slave_repl_offset", -2))
            if ro >= mo > 0:
                return True
        time.sleep(0.1)
    return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--dir", default="/tmp/rudis-upgrade")
    ap.add_argument("--port", type=int, default=21400)
    ap.add_argument("--keys", type=int, default=1_000_000, help="keys preloaded (DEBUG POPULATE)")
    ap.add_argument("--settle", type=float, default=5, help="seconds of load between steps")
    a = ap.parse_args()

    shutil.rmtree(a.dir, ignore_errors=True)
    os.makedirs(a.dir)
    old = shutil.copy(os.path.abspath(a.old), os.path.join(a.dir, "rudis-old"))
    new = shutil.copy(os.path.abspath(a.new), os.path.join(a.dir, "rudis-new"))
    A, B = a.port, a.port + 1
    cl = Cluster(a.dir, [A, B], {A: "16-19", B: "20-23"})
    failures, steps = [], []
    try:
        cl.start(A, old)
        print("populate", try_cmd(A, "DEBUG", "POPULATE", a.keys, "upg:pop", 100, timeout=300), flush=True)
        cl.start(B, old, replicaof=A)
        if not wait_synced(A, B):
            raise SystemExit("replica never synced")
        w = Writers([A, B])
        for t in w.threads:
            t.start()
        time.sleep(a.settle)

        def step(name, fn, master, replica):
            before = {p: info(p) for p in (A, B)}
            t0 = time.time()
            note = fn()
            synced = wait_synced(master, replica)
            t1 = time.time()
            time.sleep(a.settle)
            after = {p: info(p) for p in (A, B)}
            # Resyncs are counted by the master serving them (this step's
            # master; its counters survive the step unless it restarted).
            def delta(k):
                return int(after[master].get(k, 0)) - int(before[master].get(k, 0))
            full, partial = delta("sync_full"), delta("sync_partial_ok")
            gap = w.longest_gap(t0, t1 + a.settle)
            row = f"{name}: {t1 - t0:.1f}s to resync, longest write gap {gap * 1e3:.0f} ms, full syncs {full}, partial {partial}"
            if note:
                row += f" ({note})"
            if not synced:
                failures.append(f"{name}: replica never caught up")
            print(row, flush=True)
            steps.append(row)

        def restart(port, binary, replicaof):
            def fn():
                how = cl.stop(port)
                cl.start(port, binary, replicaof=replicaof)
                return f"old process {how}"
            return fn

        def failover(port):
            def fn():
                r = try_cmd(port, "FAILOVER", "TIMEOUT", "10000", timeout=15)
                deadline = time.time() + 30
                while time.time() < deadline and info(port).get("master_failover_state", "no-failover") != "no-failover":
                    time.sleep(0.05)
                return f"FAILOVER {r}"
            return fn

        step("1. replica restarted on new", restart(B, new, A), A, B)
        step("2. failover to replica", failover(A), B, A)
        if role(B) != "master" or role(A) != "slave":
            failures.append(f"after failover: roles A={role(A)} B={role(B)}")
        step("3. old master restarted on new", restart(A, new, B), B, A)
        step("4. failover back", failover(B), A, B)
        if role(A) != "master" or role(B) != "slave":
            failures.append(f"after failover back: roles A={role(A)} B={role(B)}")

        w.stop = True
        for t in w.threads:
            t.join(10)
        if not wait_synced(A, B):
            failures.append("final: replica never caught up")
        m = Conn(A, 30)
        for i in range(w.n):
            v = int(m.cmd("GET", f"upg:{{{i}}}:c") or 0)
            lo, hi = w.acked[i], w.acked[i] + w.uncertain[i]
            if not lo <= v <= hi:
                failures.append(f"counter {i}: {v}, expected {lo}..{hi}")
        md, rd = try_cmd(A, "DEBUG", "DIGEST", timeout=120), try_cmd(B, "DEBUG", "DIGEST", timeout=120)
        if md != rd or str(md).startswith(("-", "!")):
            failures.append(f"digest master {md} != replica {rd}")
        print(f"acknowledged INCRs {sum(w.acked)}, reply lost {sum(w.uncertain)}, error replies {w.errors}")
        print(f"digest master {md} replica {rd}")
    finally:
        cl.stop_all()
    print("\n".join(["FAILURES:"] + failures) if failures else "ALL CHECKS PASSED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
