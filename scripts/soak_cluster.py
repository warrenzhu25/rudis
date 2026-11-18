#!/usr/bin/env python3
"""Cluster soak: a 3-master, 3-replica cluster under sustained load for hours,
with manual failovers, reshards and replica restarts injected throughout,
sampled every minute, with a report at the end.

  scripts/soak_cluster.py --binary target/release/rudis --hours 24 --dir /tmp/csoak

What runs:
  * six cluster-mode nodes (2 shards each, so MOVED points at shard ports),
    AOF on, created with `valkey-cli --cluster create --cluster-replicas 1`;
  * memtier in --cluster-mode, rate-limited, mixed SET/GET over a bounded key
    space (no maxmemory, no TTLs: every key must survive, and digests of a
    master and its replica can be compared at the end);
  * a counter writer: INCR on 1000 counters spread over the slots, following
    MOVED/ASK and retrying CLUSTERDOWN/TRYAGAIN, recording the highest value
    each INCR acknowledged;
  * chaos, each on its own period: CLUSTER FAILOVER on a random master's
    replica (a manual failover: no acknowledged write may be lost), a reshard
    of a few hundred slots between random masters, kill -9 and restart of a
    random replica, BGSAVE / BGREWRITEAOF on a random node.

Every --interval seconds it records each node's INFO (memory, ops, role,
replication), checks that every node answers within 5 s (a hung node is the
failure a deadlock produces), that the cluster is ok with all 16384 slots
assigned, and probes latency through a cluster client. At the end it stops
the load, checks every counter is at least its highest acknowledged value,
compares DEBUG DIGEST of each master and its replica, and writes report.md.
"""

import argparse
import json
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from soak import MEMTIER, BigValueWriter, Conn, cpus, resp  # noqa: E402

CLI = os.path.expanduser(os.environ.get("VALKEY_CLI", "~/valkey-stable/src/valkey-cli"))
SLOTS = 16384


def crc16(data: bytes) -> int:
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def key_slot(key: str) -> int:
    k = key.encode()
    s = k.find(b"{")
    if s >= 0:
        e = k.find(b"}", s + 1)
        if e > s + 1:
            k = k[s + 1 : e]
    return crc16(k) % SLOTS


class ClusterClient:
    """Minimal cluster client: slot map from CLUSTER SLOTS, follows MOVED and
    ASK, reconnects. Errors other than redirects are returned to the caller."""

    def __init__(self, seeds):
        self.seeds = list(seeds)
        self.slots = {}
        self.conns = {}
        # Connections that failed (and were redone, after a 50 ms pause)
        # during the last cmd: its latency is then the client's, not the
        # server's.
        self.reconnects = 0
        self.refresh()

    def refresh(self):
        for p in random.sample(self.seeds, len(self.seeds)):
            try:
                rows = Conn(p, timeout=5).cmd("CLUSTER", "SLOTS")
                if not isinstance(rows, list):
                    continue
                m = {}
                for r in rows:
                    for s in range(int(r[0]), int(r[1]) + 1):
                        m[s] = int(r[2][1])
                if m:
                    self.slots = m
                    return
            except Exception:  # noqa: BLE001
                continue

    def conn(self, port):
        c = self.conns.get(port)
        if c is None:
            c = Conn(port, timeout=5)
            self.conns[port] = c
        return c

    def drop(self, port):
        c = self.conns.pop(port, None)
        if c:
            try:
                c.sock.close()
            except OSError:
                pass

    def cmd(self, key, *args, tries=8):
        port = self.slots.get(key_slot(key)) or random.choice(self.seeds)
        asking = False
        last = None
        hops = []
        self.reconnects = 0
        for _ in range(tries):
            try:
                c = self.conn(port)
                if asking:
                    c.cmd("ASKING")
                    asking = False
                r = c.cmd(*args)
            except Exception as e:  # noqa: BLE001
                self.drop(port)
                self.reconnects += 1
                last = f"-CONN {e}"
                time.sleep(0.05)
                self.refresh()
                port = self.slots.get(key_slot(key)) or random.choice(self.seeds)
                continue
            if isinstance(r, str) and r.startswith("-MOVED "):
                hops.append(f"{port}>M{r.split()[2].rsplit(':', 1)[1]}")
                port = int(r.split()[2].rsplit(":", 1)[1])
                self.slots[key_slot(key)] = port
                continue
            if isinstance(r, str) and r.startswith("-ASK "):
                hops.append(f"{port}>A{r.split()[2].rsplit(':', 1)[1]}")
                port = int(r.split()[2].rsplit(":", 1)[1])
                asking = True
                continue
            return r
        # Out of tries: the error, or the redirects that went in circles.
        return last or f"-REDIRECTLOOP slot {key_slot(key)}: {' '.join(hops)}"


def node_info(port, timeout=5):
    text = Conn(port, timeout=timeout).cmd("INFO", "everything")
    out = {}
    for line in (text or "").splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def cluster_info(port):
    text = Conn(port, timeout=5).cmd("CLUSTER", "INFO")
    return dict(l.split(":", 1) for l in (text or "").splitlines() if ":" in l)


def cluster_nodes(port):
    """[(id, port, role, master_id, slots_str)] as `port` sees the cluster."""
    out = []
    for line in (Conn(port, timeout=5).cmd("CLUSTER", "NODES") or "").splitlines():
        f = line.split()
        if len(f) < 8:
            continue
        p = int(f[1].split("@")[0].rsplit(":", 1)[1])
        role = "master" if "master" in f[2] else "slave"
        out.append((f[0], p, role, f[3], " ".join(f[8:])))
    return out


def foreign_keys(port):
    """{slot: keys} for slots holding keys on `port` that it doesn't serve
    (its master's, for a replica): stale data a node should not have."""
    ns = cluster_nodes(port)
    myid = Conn(port, timeout=5).cmd("CLUSTER", "MYID")
    mine = next(n for n in ns if n[0] == myid)
    owner_id = myid if mine[2] == "master" else mine[3]
    owned = set()
    owner_port = next(n[1] for n in ns if n[0] == owner_id)
    for n in ns:
        if n[0] == owner_id:
            for part in n[4].split():
                if part.startswith("["):
                    continue
                lo, _, hi = part.partition("-")
                owned.update(range(int(lo), int(hi or lo) + 1))
    # A slot open for migration ([slot-<-id] importing, [slot->-id]
    # migrating, shown by the owner only) legitimately has keys on both.
    for n in cluster_nodes(owner_port):
        for part in n[4].split():
            if part.startswith("["):
                owned.add(int(part[1:].split("-", 1)[0]))
    c = Conn(port, timeout=30)
    others = [s for s in range(SLOTS) if s not in owned]
    c.sock.sendall(b"".join(resp("CLUSTER", "COUNTKEYSINSLOT", s) for s in others))
    out = {}
    for s in others:
        r = c.reply()
        if isinstance(r, str) and r.startswith(":") and r != ":0":
            out[s] = int(r[1:])
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", required=True)
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--interval", type=int, default=60, help="seconds between samples")
    ap.add_argument("--dir", required=True, help="working directory (wiped)")
    ap.add_argument("--cores", default="16-31")
    # Bus ports are port + 10000: keep them below the ephemeral range
    # (32768+ on Linux), where an outgoing connection can hold one and a
    # node fails to start or restart with "Address already in use".
    ap.add_argument("--port", type=int, default=21000, help="first node's port; nodes are 10 apart")
    ap.add_argument("--threads", type=int, default=2, help="shards per node")
    ap.add_argument("--rate", type=int, default=1000, help="memtier requests/s per connection")
    ap.add_argument("--keys", type=int, default=300000, help="memtier key space")
    ap.add_argument("--big-every", type=float, default=2.0,
                    help="seconds between big-value operations (512 KB-8 MB values); 0 turns them off")
    ap.add_argument("--failover-every", type=int, default=600, help="seconds")
    ap.add_argument("--reshard-every", type=int, default=1200, help="seconds")
    ap.add_argument("--restart-every", type=int, default=1800, help="seconds")
    ap.add_argument("--save-every", type=int, default=1800, help="seconds")
    a = ap.parse_args()

    shutil.rmtree(a.dir, ignore_errors=True)
    os.makedirs(a.dir)
    cores = cpus(a.cores)
    half = max(1, len(cores) // 2)
    node_cores, load_cores = cores[:half], cores[half:]
    binary = os.path.join(a.dir, "rudis")
    shutil.copy(os.path.abspath(a.binary), binary)  # immune to rebuilds
    ports = [a.port + 10 * i for i in range(6)]
    procs = {}
    events = []  # (t, text): chaos actions, for the report
    anomalies = []
    start = time.time()

    def t():
        return round(time.time() - start)

    def note(msg):
        events.append(f"{t()}s: {msg}")
        print(events[-1], flush=True)

    def bad(msg):
        anomalies.append(f"{t()}s: {msg}")
        print("ANOMALY", anomalies[-1], flush=True)

    def launch(port):
        d = os.path.join(a.dir, f"node-{port}")
        os.makedirs(d, exist_ok=True)
        args = [
            "taskset", "-c", ",".join(map(str, node_cores)), binary,
            "--port", str(port), "--threads", str(a.threads), "--no-pin", "--dir", d,
            "--cluster-enabled", "yes", "--appendonly", "yes", "--save", "",
            "--enable-debug-command", "yes", "--logfile", os.path.join(a.dir, f"node-{port}.log"),
        ]
        procs[port] = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=open(os.path.join(a.dir, f"node-{port}.err"), "a"))
        end = time.time() + 60
        while time.time() < end:
            if procs[port].poll() is not None:
                raise SystemExit(f"node {port} exited at startup ({procs[port].returncode})")
            try:
                if Conn(port, timeout=1).cmd("PING") == "+PONG":
                    return
            except OSError:
                pass
            time.sleep(0.1)
        raise SystemExit(f"node {port} did not start")

    for p in ports:
        launch(p)
    out = subprocess.run(
        [CLI, "--cluster", "create", *[f"127.0.0.1:{p}" for p in ports], "--cluster-replicas", "1", "--cluster-yes"],
        capture_output=True, text=True, timeout=120)
    open(os.path.join(a.dir, "create.out"), "w").write(out.stdout + out.stderr)
    if out.returncode != 0:
        raise SystemExit(f"cluster create failed:\n{out.stdout}{out.stderr}")
    end = time.time() + 60
    while time.time() < end:
        if all(cluster_info(p).get("cluster_state") == "ok" for p in ports):
            break
        time.sleep(0.5)
    note("cluster created: " + ", ".join(f"{n[1]}={n[2]}" for n in sorted(cluster_nodes(ports[0]), key=lambda n: n[1])))

    duration = int(a.hours * 3600)
    load_cpu = ",".join(map(str, load_cores))
    memtier = subprocess.Popen([
        "taskset", "-c", load_cpu, MEMTIER, "--server", "127.0.0.1", "--port", str(ports[0]),
        "--cluster-mode", "--threads", "2", "--clients", "8", "--ratio", "1:3",
        "--data-size-range", "32-1024", "--key-maximum", str(a.keys), "--key-pattern", "R:R",
        "--rate-limiting", str(a.rate), "--test-time", str(duration), "--hide-histogram",
    ], stdout=open(os.path.join(a.dir, "memtier.out"), "w"), stderr=subprocess.STDOUT)

    # Counter writer: the highest value each counter's INCR acknowledged.
    acked = {}
    counter_errors = {}
    stop_counters = threading.Event()

    def counters():
        cc = ClusterClient(ports)
        keys = [f"ctr:{i}" for i in range(1000)]
        while not stop_counters.is_set():
            k = random.choice(keys)
            r = cc.cmd(k, "INCR", k)
            if isinstance(r, str) and r.startswith(":"):
                acked[k] = max(acked.get(k, 0), int(r[1:]))
            else:
                kind = (r or "-NONE").split()[0]
                counter_errors[kind] = counter_errors.get(kind, 0) + 1
                if kind == "-REDIRECTLOOP" and counter_errors[kind] <= 5:
                    note(f"counter writer: {r}")
                time.sleep(0.01)

    writer = threading.Thread(target=counters, daemon=True)
    writer.start()
    big = None
    if a.big_every > 0:
        big_cc = ClusterClient(ports)
        big = BigValueWriter(lambda key, *args: big_cc.cmd(key, *args), a.big_every)
        big.start()

    def masters():
        ns = cluster_nodes(random.choice([p for p in ports if procs[p].poll() is None]))
        return [n for n in ns if n[2] == "master" and n[4]], ns

    def failover():
        ms, ns = masters()
        m = random.choice(ms)
        reps = [n for n in ns if n[2] == "slave" and n[3] == m[0]]
        if not reps:
            bad(f"master {m[1]} has no replica to fail over to")
            return
        r = random.choice(reps)
        reply = Conn(r[1], timeout=10).cmd("CLUSTER", "FAILOVER")
        note(f"CLUSTER FAILOVER on {r[1]} (replica of {m[1]}): {reply}")
        end = time.time() + 30
        while time.time() < end:
            try:
                if Conn(r[1], timeout=5).cmd("ROLE")[0] == "master":
                    return
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.2)
        bad(f"failover to {r[1]} didn't happen within 30 s")

    def reshard():
        ms, _ = masters()
        if len(ms) < 2:
            return
        src, dst = random.sample(ms, 2)
        n = random.randint(50, 400)
        out = subprocess.run(
            [CLI, "--cluster", "reshard", f"127.0.0.1:{dst[1]}", "--cluster-from", src[0],
             "--cluster-to", dst[0], "--cluster-slots", str(n), "--cluster-yes"],
            capture_output=True, text=True, timeout=1200)
        note(f"reshard {n} slots {src[1]} -> {dst[1]}: exit {out.returncode}")
        if out.returncode != 0:
            bad(f"reshard {src[1]} -> {dst[1]} failed: {(out.stdout + out.stderr)[-500:]}")

    def restart_replica():
        _, ns = masters()
        reps = [n for n in ns if n[2] == "slave"]
        if not reps:
            return
        r = random.choice(reps)
        procs[r[1]].kill()
        procs[r[1]].wait()
        note(f"kill -9 replica {r[1]}")
        launch(r[1])
        end = time.time() + 120
        while time.time() < end:
            try:
                if node_info(r[1]).get("master_link_status") == "up":
                    note(f"replica {r[1]} back, link up")
                    return
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.5)
        bad(f"restarted replica {r[1]} didn't relink within 120 s")

    def save():
        p = random.choice(ports)
        cmd = random.choice(["BGSAVE", "BGREWRITEAOF"])
        reply = Conn(p, timeout=10).cmd(*([cmd, "SCHEDULE"] if cmd == "BGSAVE" else [cmd]))
        note(f"{cmd} on {p}: {reply}")
        if str(reply).startswith("-"):
            bad(f"{cmd} on {p}: {reply}")

    chaos = [
        (a.failover_every, failover), (a.reshard_every, reshard),
        (a.restart_every, restart_replica), (a.save_every, save),
    ]
    last_chaos = {f: start for _, f in chaos}
    probe = ClusterClient(ports)
    keep = [
        "used_memory", "used_memory_rss", "total_commands_processed", "instantaneous_ops_per_sec",
        "role", "connected_slaves", "master_link_status", "master_repl_offset", "sync_full",
        "sync_partial_ok", "aof_last_write_status", "aof_last_bgrewrite_status",
        "rdb_last_bgsave_status", "total_error_replies", "db0",
    ]
    samples_path = os.path.join(a.dir, "samples.jsonl")

    def stop(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    try:
        with open(samples_path, "w") as out_f:
            while time.time() - start < duration:
                time.sleep(a.interval)
                row = {"t": t(), "nodes": {}}
                for p in ports:
                    if procs[p].poll() is not None:
                        bad(f"node {p} exited ({procs[p].returncode})")
                        raise KeyboardInterrupt
                    try:
                        i = node_info(p)
                        ci = cluster_info(p)
                        row["nodes"][p] = {k: i.get(k) for k in keep}
                        row["nodes"][p].update({k: ci.get(k) for k in (
                            "cluster_state", "cluster_slots_assigned", "cluster_slots_ok", "cluster_known_nodes")})
                    except Exception as e:  # noqa: BLE001
                        bad(f"node {p} didn't answer within 5 s: {e}")
                        continue
                    n = row["nodes"][p]
                    if n.get("cluster_state") != "ok" or n.get("cluster_slots_assigned") != str(SLOTS):
                        bad(f"node {p}: cluster_state={n.get('cluster_state')} assigned={n.get('cluster_slots_assigned')}")
                    for k in ("aof_last_write_status", "aof_last_bgrewrite_status", "rdb_last_bgsave_status"):
                        if n.get(k) not in (None, "ok"):
                            bad(f"node {p} {k}={n.get(k)}")
                    if n.get("role") == "slave" and n.get("master_link_status") != "up":
                        bad(f"replica {p} link {n.get('master_link_status')}")
                times = []
                reconnect_times = []
                for _ in range(200):
                    k = f"memtier-{random.randrange(a.keys)}"
                    s = time.perf_counter()
                    r = probe.cmd(k, "GET", k)
                    ms = (time.perf_counter() - s) * 1000
                    # A GET that had to reconnect (its node was killed or
                    # restarted since) measures the client's 50 ms
                    # back-off: counted apart, not as server latency.
                    (reconnect_times if probe.reconnects else times).append(ms)
                    if isinstance(r, str) and r.startswith("-") and not r.startswith("-LOADING"):
                        row.setdefault("probe_errors", {}).setdefault(r.split()[0], 0)
                        row["probe_errors"][r.split()[0]] += 1
                times.sort()
                n = len(times)
                row["probe"] = {"p50_ms": round(times[n // 2], 3), "p99_ms": round(times[n * 99 // 100 - 1], 3),
                                "max_ms": round(times[-1], 3)}
                if reconnect_times:
                    row["probe"]["reconnects"] = len(reconnect_times)
                    row["probe"]["reconnect_max_ms"] = round(max(reconnect_times), 3)
                row["counters"] = {"acked_total": sum(acked.values()), "errors": dict(counter_errors)}
                if memtier.poll() not in (None, 0):
                    bad(f"memtier exited ({memtier.returncode})")
                out_f.write(json.dumps(row) + "\n")
                out_f.flush()
                now = time.time()
                for every, f in chaos:
                    if every > 0 and now - last_chaos[f] >= every:
                        last_chaos[f] = now
                        try:
                            f()
                        except Exception as e:  # noqa: BLE001
                            bad(f"{f.__name__} raised {e!r}")
                        # After each action: no node may hold keys of slots
                        # it doesn't serve (give replication a moment).
                        time.sleep(2)
                        for p in ports:
                            try:
                                fk = foreign_keys(p)
                            except Exception as e:  # noqa: BLE001
                                bad(f"foreign-key check on {p} failed: {e!r}")
                                continue
                            if fk:
                                items = sorted(fk.items())[:5]
                                bad(f"node {p} holds keys of slots it doesn't serve, after {f.__name__}: "
                                    f"{len(fk)} slots, {sum(fk.values())} keys, e.g. {items}")
    except KeyboardInterrupt:
        pass
    finally:
        stop_counters.set()
        if big:
            big.stop_event.set()
            big.join(60)
        if memtier.poll() is None:
            memtier.terminate()
        try:
            memtier.wait(30)
        except subprocess.TimeoutExpired:
            memtier.kill()
        writer.join(10)

    final = {}
    try:
        time.sleep(3)
        cc = ClusterClient(ports)
        lost = []
        for k, v in sorted(acked.items()):
            r = cc.cmd(k, "GET", k)
            got = int(r) if isinstance(r, str) and r.lstrip("-").isdigit() else None
            if got is None or got < v:
                lost.append((k, v, r))
        if big:
            final["big_values"] = big.summary()
            if big.stats["mismatches"]:
                anomalies.append(f"big values read back wrong: {big.examples}")
        final["counters"] = {"keys": len(acked), "acked_incrs": sum(acked.values()),
                             "lost": len(lost), "examples": lost[:10]}
        if lost:
            anomalies.append(f"{len(lost)} counters below their highest acknowledged INCR: {lost[:5]}")
            # Who each node thinks serves those slots (and any open
            # migration markers), to tell lost data from a routing split.
            for k, _, _ in lost[:3]:
                slot = key_slot(k)
                views = {}
                for p in ports:
                    try:
                        for n in cluster_nodes(p):
                            for part in n[4].split():
                                if part.startswith("["):
                                    if int(part[1:].split("-", 1)[0]) == slot:
                                        views.setdefault(p, []).append(f"{n[1]}{part}")
                                    continue
                                lo, _, hi = part.partition("-")
                                if int(lo) <= slot <= int(hi or lo):
                                    views.setdefault(p, []).append(f"{n[1]}({n[2]})")
                    except Exception as e:  # noqa: BLE001
                        views[p] = [repr(e)]
                anomalies.append(f"slot {slot} ({k}) as each node sees it: {views}")
        pairs = []
        ns = cluster_nodes(ports[0])
        for m in [n for n in ns if n[2] == "master"]:
            Conn(m[1], timeout=60).cmd("WAIT", "1", "30000")
        time.sleep(2)
        for m in [n for n in ns if n[2] == "master"]:
            md = Conn(m[1], timeout=120).cmd("DEBUG", "DIGEST")
            msize = Conn(m[1]).cmd("DBSIZE")
            for r in [n for n in ns if n[2] == "slave" and n[3] == m[0]]:
                rd = Conn(r[1], timeout=120).cmd("DEBUG", "DIGEST")
                rsize = Conn(r[1]).cmd("DBSIZE")
                pairs.append({"master": m[1], "replica": r[1], "same": md == rd,
                              "master_dbsize": msize, "replica_dbsize": rsize})
                if md != rd:
                    anomalies.append(f"replica {r[1]} differs from master {m[1]} ({rsize} vs {msize} keys)")
        final["pairs"] = pairs
    except Exception as e:  # noqa: BLE001
        anomalies.append(f"final checks failed: {e!r}")
    for p in ports:
        if procs[p].poll() is None:
            procs[p].terminate()
            try:
                procs[p].wait(60)
            except subprocess.TimeoutExpired:
                procs[p].kill()
                anomalies.append(f"node {p} didn't exit within 60 s of SIGTERM")
    write_report(a, samples_path, events, anomalies, final)


def write_report(a, samples_path, events, anomalies, final):
    rows = [json.loads(l) for l in open(samples_path)] if os.path.exists(samples_path) else []
    lines = [f"# Cluster soak report ({len(rows)} samples, {a.hours} h planned)", ""]
    if rows:
        hour = max(1, 3600 // a.interval)
        first, last = rows[:hour], rows[-hour:]

        def avg(rs, f):
            vals = [v for v in (f(r) for r in rs) if v is not None]
            return statistics.mean(vals) if vals else float("nan")

        def total(r, k):
            vals = [float(n[k]) for n in r["nodes"].values() if n.get(k) not in (None, "")]
            return sum(vals) if vals else None

        lines += ["| | first hour | last hour |", "| :--- | ---: | ---: |"]
        for label, f in (
            ("used_memory, all nodes (MB)", lambda r: (total(r, "used_memory") or 0) / 2**20),
            ("used_memory_rss, all nodes (MB)", lambda r: (total(r, "used_memory_rss") or 0) / 2**20),
            ("ops/s, all nodes", lambda r: total(r, "instantaneous_ops_per_sec")),
            ("probe p50 (ms)", lambda r: r["probe"]["p50_ms"]),
            ("probe p99 (ms)", lambda r: r["probe"]["p99_ms"]),
            ("probe max (ms)", lambda r: r["probe"]["max_ms"]),
        ):
            lines.append(f"| {label} | {avg(first, f):.2f} | {avg(last, f):.2f} |")
        worst = max(r["probe"]["max_ms"] for r in rows)
        recon = sum(r["probe"].get("reconnects", 0) for r in rows)
        lines += ["", f"* worst probe latency: {worst:.1f} ms",
                  f"* probe GETs that reconnected first (not counted above): {recon}",
                  f"* counter writer at the end: {rows[-1].get('counters')}"]
        perr = {}
        for r in rows:
            for k, v in r.get("probe_errors", {}).items():
                perr[k] = perr.get(k, 0) + v
        lines.append(f"* probe errors: {perr or 'none'}")
    lines += ["", "## Final checks", "", f"```\n{json.dumps(final, indent=2)}\n```", ""]
    lines += ["## Chaos events", ""] + [f"* {e}" for e in events]
    lines += ["", "## Anomalies", ""] + ([f"* {x}" for x in anomalies] or ["None."])
    open(os.path.join(a.dir, "report.md"), "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    sys.exit(main())
