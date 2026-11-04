#!/usr/bin/env python3
"""Client-library compatibility matrix for rudis.

Starts a standalone rudis and a 3-master rudis cluster from --binary, fetches
each client library into a cache directory (network needed), runs that
client's suite of shared scenarios (strings, collections, pipeline,
transaction, pubsub, scripting, blocking, streams, scan, resp3, cluster), and
prints a markdown matrix (and writes it to --out if given).

Usage:
  scripts/client_compat/run.py --binary target/release/rudis [--out docs/client_compat.md]
      [--clients redis-py,go-redis,ioredis,node-redis,jedis,lettuce] [--cores 0-7]
"""
import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SCENARIOS = ["strings", "collections", "pipeline", "transaction", "pubsub", "scripting",
             "blocking", "streams", "scan", "resp3", "cluster"]
STANDALONE_PORT = 16600
CLUSTER_PORTS = [16610, 16612, 16614]

NODE_VERSION = "v22.11.0"
JEDIS = [("redis.clients", "jedis", "5.2.0"), ("org.apache.commons", "commons-pool2", "2.12.0"),
         ("org.slf4j", "slf4j-api", "1.7.36"), ("com.google.code.gson", "gson", "2.11.0"),
         ("org.json", "json", "20240303")]
LETTUCE = [("io.lettuce", "lettuce-core", "6.5.0.RELEASE"),
           ("io.projectreactor", "reactor-core", "3.6.6"),
           ("org.reactivestreams", "reactive-streams", "1.0.4")] + [
    ("io.netty", f"netty-{m}", "4.1.113.Final")
    for m in ["common", "buffer", "transport", "resolver", "handler", "codec",
              "transport-native-unix-common"]]


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def fetch(url, dest):
    if not os.path.exists(dest):
        log("fetching", url)
        tmp = dest + ".part"
        with urllib.request.urlopen(url, timeout=120) as r, open(tmp, "wb") as f:
            shutil.copyfileobj(r, f)
        os.rename(tmp, dest)
    return dest


def maven_jar(cache, group, artifact, version):
    path = group.replace(".", "/")
    name = f"{artifact}-{version}.jar"
    return fetch(f"https://repo1.maven.org/maven2/{path}/{artifact}/{version}/{name}",
                 os.path.join(cache, name))


def wait_port(port, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as s:
                s.sendall(b"PING\r\n")
                if s.recv(64):
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"rudis on {port} did not start")


def cmd(port, *args):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        req = f"*{len(args)}\r\n" + "".join(f"${len(str(a))}\r\n{a}\r\n" for a in args)
        s.sendall(req.encode())
        return s.recv(65536).decode(errors="replace")


def start_servers(binary, cores, work):
    procs = []

    def spawn(port, extra):
        d = os.path.join(work, f"srv{port}")
        os.makedirs(d, exist_ok=True)
        procs.append(subprocess.Popen(
            ["taskset", "-c", cores, binary, "--port", str(port), "--no-pin", "--dir", d,
             "--save", "", "--enable-debug-command", "yes"] + extra,
            stdout=subprocess.DEVNULL, stderr=open(os.path.join(d, "err"), "w")))
        wait_port(port)

    spawn(STANDALONE_PORT, ["--threads", "4"])
    ranges = [(0, 5460), (5461, 10922), (10923, 16383)]
    for p in CLUSTER_PORTS:
        spawn(p, ["--threads", "1", "--cluster-enabled", "yes"])
    for p, (s, e) in zip(CLUSTER_PORTS, ranges):
        cmd(p, "CLUSTER", "DELSLOTSRANGE", 0, 16383)
        cmd(p, "CLUSTER", "ADDSLOTSRANGE", s, e)
    for p in CLUSTER_PORTS[1:]:
        cmd(CLUSTER_PORTS[0], "CLUSTER", "MEET", "127.0.0.1", p)
    deadline = time.time() + 30
    while time.time() < deadline:
        if all("cluster_state:ok" in cmd(p, "CLUSTER", "INFO")
               and cmd(p, "CLUSTER", "NODES").count("connected") >= 3 for p in CLUSTER_PORTS):
            break
        time.sleep(0.2)
    return procs


def run_suite(argv, env, cwd=None, timeout=600):
    env = {**os.environ, **env, "RUDIS_PORT": str(STANDALONE_PORT),
           "RUDIS_CLUSTER_PORT": str(CLUSTER_PORTS[0])}
    out = subprocess.run(argv, env=env, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    rows = []
    for line in out.stdout.splitlines():
        if line.startswith("{"):
            rows.append(json.loads(line))
    if not rows:
        log(out.stdout[-3000:], out.stderr[-3000:])
    return rows


def suite_redis_py(cache):
    meta = json.load(urllib.request.urlopen("https://pypi.org/pypi/redis/json", timeout=60))
    wheel = next(u["url"] for u in meta["urls"] if u["packagetype"] == "bdist_wheel")
    whl = fetch(wheel, os.path.join(cache, os.path.basename(wheel)))
    return run_suite([sys.executable, os.path.join(HERE, "redis_py_suite.py")],
                     {"PYTHONPATH": whl})


def suite_go_redis(cache):
    return run_suite(["go", "run", "."], {"GOFLAGS": "-mod=mod"},
                     cwd=os.path.join(HERE, "go_redis"))


def node_bin(cache):
    d = os.path.join(cache, f"node-{NODE_VERSION}-linux-x64")
    if not os.path.isdir(d):
        tgz = fetch(f"https://nodejs.org/dist/{NODE_VERSION}/node-{NODE_VERSION}-linux-x64.tar.xz",
                    os.path.join(cache, f"node-{NODE_VERSION}.tar.xz"))
        with tarfile.open(tgz) as t:
            t.extractall(cache, filter="data")
    return os.path.join(d, "bin")


def node_suite(cache, client):
    bindir = node_bin(cache)
    work = os.path.join(cache, "node")
    os.makedirs(work, exist_ok=True)
    shutil.copy(os.path.join(HERE, "node", "suite.js"), work)
    env = {"PATH": bindir + os.pathsep + os.environ["PATH"]}
    if not os.path.isdir(os.path.join(work, "node_modules", "ioredis")) or \
            not os.path.isdir(os.path.join(work, "node_modules", "redis")):
        subprocess.run([os.path.join(bindir, "npm"), "install", "--silent", "--no-audit",
                        "--no-fund", "ioredis", "redis"], cwd=work, env={**os.environ, **env},
                       check=True)
    return run_suite([os.path.join(bindir, "node"), "suite.js", client], env, cwd=work)


def java_suite(cache, client, deps):
    jars = [maven_jar(cache, *d) for d in deps]
    cp = os.pathsep.join(jars)
    out = os.path.join(cache, f"java-{client}")
    os.makedirs(out, exist_ok=True)
    src = os.path.join(HERE, "java", f"{client.capitalize()}Suite.java")
    subprocess.run(["javac", "-d", out, "-cp", cp, src], check=True)
    return run_suite(["java", "-cp", cp + os.pathsep + out, f"{client.capitalize()}Suite"],
                     {"CC_CLIENT_VERSION": deps[0][2]})


SUITES = {
    "redis-py": suite_redis_py,
    "go-redis": suite_go_redis,
    "ioredis": lambda c: node_suite(c, "ioredis"),
    "node-redis": lambda c: node_suite(c, "node-redis"),
    "jedis": lambda c: java_suite(c, "jedis", JEDIS),
    "lettuce": lambda c: java_suite(c, "lettuce", LETTUCE),
}


def markdown(rows, binary_rev):
    clients = []
    for r in rows:
        if (r["client"], r["version"]) not in clients:
            clients.append((r["client"], r["version"]))
    res = {(r["client"], r["scenario"]): r for r in rows}
    lines = [f"Generated by `scripts/client_compat/run.py` against rudis `{binary_rev}`.", "",
             "| Scenario | " + " | ".join(f"{c} {v}" for c, v in clients) + " |",
             "|---|" + "---|" * len(clients)]
    notes = []
    for s in SCENARIOS:
        cells = []
        for c, _ in clients:
            r = res.get((c, s))
            if r is None:
                cells.append("–")
            elif r["ok"]:
                cells.append("✅")
            else:
                notes.append(f"* **{c} / {s}**: `{r['error']}`")
                cells.append(f"❌ [{len(notes)}]")
        lines.append(f"| {s} | " + " | ".join(cells) + " |")
    if notes:
        lines += ["", "Failures:", ""] + [f"{i + 1}. {n[2:]}" for i, n in enumerate(notes)]
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True)
    ap.add_argument("--clients", default=",".join(SUITES))
    ap.add_argument("--cores", default="0-7")
    ap.add_argument("--cache", default=os.path.join(REPO, "target", "client-compat"))
    ap.add_argument("--out")
    a = ap.parse_args()
    os.makedirs(a.cache, exist_ok=True)
    work = os.path.join(a.cache, "run")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    rev = subprocess.run(["git", "-C", REPO, "rev-parse", "--short", "HEAD"],
                         capture_output=True, text=True).stdout.strip()
    procs = start_servers(os.path.abspath(a.binary), a.cores, work)
    rows = []
    try:
        for name in a.clients.split(","):
            log("== suite", name)
            try:
                got = SUITES[name](a.cache)
            except Exception as e:  # noqa: BLE001
                log(f"{name} suite could not run: {e}")
                got = []
            if not got:
                got = [{"client": name, "version": "?", "scenario": s, "ok": False,
                        "error": "suite did not run"} for s in SCENARIOS]
            rows += got
            # Each suite starts from an empty standalone server.
            cmd(STANDALONE_PORT, "FLUSHALL")
    finally:
        for p in procs:
            p.terminate()
    md = markdown(rows, rev)
    print(md)
    if a.out:
        with open(a.out, "w") as f:
            f.write(md)
    sys.exit(0 if all(r["ok"] for r in rows) else 1)


if __name__ == "__main__":
    main()
