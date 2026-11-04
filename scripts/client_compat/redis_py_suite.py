#!/usr/bin/env python3
"""redis-py compatibility suite for scripts/client_compat/run.sh.

Runs the shared scenarios against a standalone server (RUDIS_PORT) and a
cluster (RUDIS_CLUSTER_PORT, one seed node) and prints one JSON line per
scenario: {"client", "version", "scenario", "ok", "error"}.
"""
import json
import os
import threading
import time
import traceback

import redis
from redis.cluster import RedisCluster

PORT = int(os.environ["RUDIS_PORT"])
CLUSTER_PORT = int(os.environ["RUDIS_CLUSTER_PORT"])
CLIENT = "redis-py"


def report(name, fn):
    try:
        fn()
        ok, err = True, ""
    except Exception as e:  # noqa: BLE001
        ok, err = False, f"{type(e).__name__}: {e}".splitlines()[0][:300]
        if os.environ.get("CC_VERBOSE"):
            traceback.print_exc()
    print(json.dumps({"client": CLIENT, "version": redis.__version__, "scenario": name,
                      "ok": ok, "error": err}), flush=True)


def conn(**kw):
    return redis.Redis(port=PORT, decode_responses=True, **kw)


def strings():
    r = conn()
    r.flushall()
    assert r.set("s", "v", ex=100)
    assert r.get("s") == "v"
    assert 0 < r.ttl("s") <= 100
    assert r.incr("n") == 1 and r.incrby("n", 5) == 6
    assert r.mset({"a": "1", "b": "2"}) and r.mget("a", "b", "missing") == ["1", "2", None]
    assert r.set("s", "w", nx=True) is None
    assert r.getdel("s") == "v" and r.exists("s") == 0


def collections():
    r = conn()
    r.hset("h", mapping={"f1": "1", "f2": "2"})
    assert r.hgetall("h") == {"f1": "1", "f2": "2"}
    r.rpush("l", "a", "b", "c")
    assert r.lrange("l", 0, -1) == ["a", "b", "c"]
    r.sadd("st", "x", "y")
    assert r.smembers("st") == {"x", "y"}
    r.zadd("z", {"m1": 1, "m2": 2})
    assert r.zrange("z", 0, -1, withscores=True) == [("m1", 1.0), ("m2", 2.0)]


def pipeline():
    r = conn()
    p = r.pipeline(transaction=False)
    for i in range(1000):
        p.set(f"p:{i}", i)
    assert all(p.execute())
    p = r.pipeline(transaction=False)
    for i in range(1000):
        p.get(f"p:{i}")
    assert p.execute() == [str(i) for i in range(1000)]


def transaction():
    r = conn()
    r.set("tx", "0")
    p = r.pipeline()
    p.incr("tx").incr("tx")
    assert p.execute() == [1, 2]
    # WATCH: a concurrent change aborts the transaction.
    with r.pipeline() as p:
        p.watch("tx")
        conn().set("tx", "changed")
        p.multi()
        p.set("tx", "mine")
        try:
            p.execute()
            raise AssertionError("EXEC succeeded after a watched key changed")
        except redis.WatchError:
            pass
    assert r.get("tx") == "changed"


def pubsub():
    r = conn()
    ps = r.pubsub()
    ps.subscribe("ch")
    ps.psubscribe("pat.*")
    deadline = time.time() + 5
    seen = set()
    while time.time() < deadline and len(seen) < 2:
        r.publish("ch", "m1")
        r.publish("pat.x", "m2")
        msg = ps.get_message(timeout=0.2)
        while msg:
            if msg["type"] in ("message", "pmessage"):
                seen.add((msg["channel"], msg["data"]))
            msg = ps.get_message(timeout=0.05)
    ps.close()
    assert ("ch", "m1") in seen and ("pat.x", "m2") in seen, seen


def scripting():
    r = conn()
    assert r.eval("return redis.call('SET', KEYS[1], ARGV[1])", 1, "lk", "lv") == "OK"
    script = r.register_script("return redis.call('GET', KEYS[1])")
    assert script(keys=["lk"]) == "lv"  # EVALSHA, falling back to EVAL
    r.script_flush()
    assert script(keys=["lk"]) == "lv"


def blocking():
    r = conn()
    r.delete("bq")
    threading.Timer(0.2, lambda: conn().rpush("bq", "x")).start()
    assert r.blpop(["bq"], timeout=5) == ("bq", "x")


def streams():
    r = conn()
    r.delete("xs")
    r.xadd("xs", {"f": "v"})
    r.xgroup_create("xs", "g", id="0")
    got = r.xreadgroup("g", "c", {"xs": ">"}, count=10)
    assert got and got[0][0] == "xs" and len(got[0][1]) == 1
    msg_id = got[0][1][0][0]
    assert r.xack("xs", "g", msg_id) == 1
    assert r.xpending("xs", "g")["pending"] == 0


def scan():
    r = conn()
    r.flushall()
    for i in range(500):
        r.set(f"scan:{i}", 1)
    assert len(set(r.scan_iter(match="scan:*", count=50))) == 500


def resp3():
    r = conn(protocol=3)
    r.set("r3", "v")
    assert r.get("r3") == "v"
    r.hset("r3h", "f", "1")
    assert r.hgetall("r3h") == {"f": "1"}
    assert r.zadd("r3z", {"a": 1.5}) == 1 and r.zscore("r3z", "a") == 1.5


def cluster():
    rc = RedisCluster(host="127.0.0.1", port=CLUSTER_PORT, decode_responses=True)
    for i in range(200):
        rc.set(f"ck:{i}", i)
    assert all(rc.get(f"ck:{i}") == str(i) for i in range(200))
    rc.mset({"{t}a": "1", "{t}b": "2"})
    assert rc.mget("{t}a", "{t}b") == ["1", "2"]
    p = rc.pipeline()
    for i in range(100):
        p.get(f"ck:{i}")
    assert p.execute() == [str(i) for i in range(100)]
    rc.close()


for name, fn in [("strings", strings), ("collections", collections), ("pipeline", pipeline),
                 ("transaction", transaction), ("pubsub", pubsub), ("scripting", scripting),
                 ("blocking", blocking), ("streams", streams), ("scan", scan),
                 ("resp3", resp3), ("cluster", cluster)]:
    report(name, fn)
