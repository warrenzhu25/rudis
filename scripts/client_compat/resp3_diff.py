#!/usr/bin/env python3
"""RESP3 reply-shape differential check: rudis vs a reference Redis.

Sends the same commands (after HELLO 3) to both servers and compares the
*type structure* of each reply (map/set/array/double/null/bulk/int/...,
recursively), not values, which differ legitimately (ids, timings).

Usage: resp3_diff.py <reference_port> <rudis_port>
Exit 1 if any reply shape differs.
"""
import socket
import sys


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.s.recv(65536)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n):
        while len(self.buf) < n + 2:
            chunk = self.s.recv(65536)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read(self):
        line = self._line()
        t, rest = chr(line[0]), line[1:]
        if t in "+-:,#(":
            return (t, rest.decode(errors="replace"))
        if t == "_":
            return ("_", None)
        if t in "$=!":
            n = int(rest)
            if n < 0:
                return ("$-1", None)
            return (t, self._exact(n).decode(errors="replace"))
        if t in "*~>":
            n = int(rest)
            if n < 0:
                return ("*-1", None)
            return (t, [self.read() for _ in range(n)])
        if t == "%" or t == "|":
            n = int(rest)
            items = [(self.read(), self.read()) for _ in range(n)]
            if t == "|":
                return self.read()  # attribute: skip it
            return ("%", items)
        raise ValueError(f"bad reply {line!r}")

    def cmd(self, *args):
        req = f"*{len(args)}\r\n" + "".join(f"${len(str(a).encode())}\r\n{a}\r\n" for a in args)
        self.s.sendall(req.encode())
        try:
            return self.read()
        except socket.timeout:
            return ("TIMEOUT", None)


def shape(r, depth=0):
    t, v = r
    if t in ("*", "~", ">"):
        if depth > 3:
            return t
        inner = sorted({shape(x, depth + 1) for x in v})
        return f"{t}[{'|'.join(inner)}]"
    if t == "%":
        if depth > 3:
            return t
        inner = sorted({f"{shape(k, depth + 1)}:{shape(x, depth + 1)}" for k, x in v})
        return f"%{{{'|'.join(inner)}}}"
    if t in ("-",):
        return "-ERR"
    return t


SETUP = [
    ["FLUSHALL"],
    ["SET", "s", "v"], ["SET", "n", "10"], ["SET", "f", "1.5"],
    ["HSET", "h", "f1", "1", "f2", "2"],
    ["RPUSH", "l", "a", "b", "c"],
    ["SADD", "st", "x", "y"], ["SADD", "st2", "y", "z"],
    ["ZADD", "z", "1.5", "a", "2", "b"], ["ZADD", "z2", "3", "c"],
    ["XADD", "xs", "1-1", "f", "v"], ["XADD", "xs", "1-2", "f", "w"],
    ["XGROUP", "CREATE", "xs", "g", "0"],
    ["PFADD", "hll", "a", "b"],
    ["GEOADD", "geo", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"],
    ["SETBIT", "bits", "7", "1"],
    # Slow-log contents differ by timing, not shape: keep it empty.
    ["CONFIG", "SET", "slowlog-log-slower-than", "10000000"], ["SLOWLOG", "RESET"],
]

PROBES = [
    # strings
    ["GET", "s"], ["GET", "missing"], ["MGET", "s", "missing"], ["SET", "s", "w", "NX"],
    ["SET", "s", "v", "XX", "GET"], ["SET", "new1", "v", "GET"], ["GETDEL", "missing"],
    ["GETEX", "missing"], ["INCRBYFLOAT", "f", "1"], ["STRLEN", "s"], ["GETRANGE", "s", "0", "-1"],
    ["SETNX", "s", "x"], ["LCS", "s", "s"], ["SUBSTR", "s", "0", "1"],
    # keys
    ["EXISTS", "s"], ["TYPE", "s"], ["TTL", "s"], ["PTTL", "missing"], ["EXPIRETIME", "s"],
    ["RANDOMKEY"], ["SCAN", "0", "COUNT", "100"], ["KEYS", "st*"], ["OBJECT", "ENCODING", "s"],
    ["OBJECT", "ENCODING", "missing"], ["DUMP", "missing"], ["TOUCH", "s"],
    # hashes
    ["HGETALL", "h"], ["HGETALL", "missing"], ["HGET", "h", "nope"], ["HMGET", "h", "f1", "nope"],
    ["HKEYS", "h"], ["HVALS", "h"], ["HRANDFIELD", "h"], ["HRANDFIELD", "h", "2"],
    ["HRANDFIELD", "h", "2", "WITHVALUES"], ["HRANDFIELD", "missing"], ["HSCAN", "h", "0"],
    ["HINCRBYFLOAT", "h", "f1", "0.5"], ["HSTRLEN", "h", "f1"], ["HEXISTS", "h", "f1"],
    ["HTTL", "h", "FIELDS", "1", "f1"],
    # lists
    ["LRANGE", "l", "0", "-1"], ["LPOP", "missing"], ["LPOP", "l", "1"], ["LPOP", "missing", "2"],
    ["LINDEX", "l", "99"], ["LPOS", "l", "zz"], ["LMPOP", "1", "missing", "LEFT"],
    ["LMPOP", "1", "l", "LEFT"], ["BLPOP", "missing", "0.01"], ["BLMPOP", "0.01", "1", "missing", "LEFT"],
    ["RPOPLPUSH", "missing", "x"], ["LMOVE", "missing", "x", "LEFT", "LEFT"],
    # sets
    ["SMEMBERS", "st"], ["SMEMBERS", "missing"], ["SINTER", "st", "st2"], ["SUNION", "st", "st2"],
    ["SDIFF", "st", "st2"], ["SRANDMEMBER", "st"], ["SRANDMEMBER", "st", "2"], ["SRANDMEMBER", "missing"],
    ["SPOP", "missing"], ["SMISMEMBER", "st", "x", "q"], ["SSCAN", "st", "0"], ["SISMEMBER", "st", "x"],
    ["SINTERCARD", "2", "st", "st2"],
    # sorted sets
    ["ZSCORE", "z", "a"], ["ZSCORE", "z", "nope"], ["ZMSCORE", "z", "a", "nope"], ["ZINCRBY", "z", "1", "c"],
    ["ZADD", "z", "INCR", "1", "a"], ["ZADD", "z", "NX", "INCR", "1", "a"], ["ZRANGE", "z", "0", "-1"],
    ["ZRANGE", "z", "0", "-1", "WITHSCORES"], ["ZREVRANGE", "z", "0", "-1", "WITHSCORES"],
    ["ZRANGEBYSCORE", "z", "-inf", "+inf", "WITHSCORES"], ["ZRANK", "z", "a"], ["ZRANK", "z", "a", "WITHSCORE"],
    ["ZRANK", "z", "nope"], ["ZREVRANK", "z", "a", "WITHSCORE"], ["ZPOPMIN", "z2"], ["ZPOPMAX", "missing"],
    ["ZPOPMIN", "z", "1"], ["ZRANDMEMBER", "z"], ["ZRANDMEMBER", "z", "2", "WITHSCORES"],
    ["ZMPOP", "1", "z", "MIN"], ["ZMPOP", "1", "missing", "MIN"], ["BZPOPMIN", "missing", "0.01"],
    ["ZUNION", "2", "z", "z2", "WITHSCORES"], ["ZINTER", "2", "z", "z2"], ["ZDIFF", "2", "z", "z2", "WITHSCORES"],
    ["ZSCAN", "z", "0"], ["ZCARD", "z"], ["ZLEXCOUNT", "z", "-", "+"],
    # streams
    ["XRANGE", "xs", "-", "+"], ["XREVRANGE", "xs", "+", "-", "COUNT", "1"], ["XLEN", "xs"],
    ["XREAD", "STREAMS", "xs", "0"], ["XREAD", "STREAMS", "missing", "0"],
    ["XREAD", "BLOCK", "10", "STREAMS", "xs", "$"],
    ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "xs", ">"],
    ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "xs", "0"],
    ["XPENDING", "xs", "g"], ["XPENDING", "xs", "g", "-", "+", "10"],
    ["XINFO", "STREAM", "xs"], ["XINFO", "GROUPS", "xs"], ["XINFO", "CONSUMERS", "xs", "g"],
    ["XAUTOCLAIM", "xs", "g", "c2", "0", "0"], ["XCLAIM", "xs", "g", "c2", "0", "1-1"],
    # hll / geo / bits
    ["PFCOUNT", "hll"], ["GEOPOS", "geo", "Palermo", "nope"], ["GEODIST", "geo", "Palermo", "Catania"],
    ["GEODIST", "geo", "Palermo", "nope"], ["GEOHASH", "geo", "Palermo"],
    ["GEOSEARCH", "geo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "WITHCOORD", "WITHDIST"],
    ["GEORADIUS", "geo", "15", "37", "200", "km", "WITHDIST"], ["BITCOUNT", "bits"], ["BITPOS", "bits", "1"],
    ["BITFIELD", "bits", "GET", "u8", "0"],
    # server / connection
    ["CONFIG", "GET", "maxmemory"], ["CONFIG", "GET", "maxmemory*"], ["INFO", "server"], ["DBSIZE"],
    ["TIME"], ["LASTSAVE"], ["CLIENT", "ID"], ["CLIENT", "GETNAME"], ["CLIENT", "INFO"],
    ["CLIENT", "TRACKINGINFO"], ["MEMORY", "USAGE", "s"], ["MEMORY", "USAGE", "missing"],
    ["SLOWLOG", "GET", "1"], ["LATENCY", "LATEST"], ["COMMAND", "COUNT"], ["COMMAND", "INFO", "get"],
    ["COMMAND", "DOCS", "get"], ["COMMAND", "GETKEYS", "SET", "a", "b"], ["ACL", "WHOAMI"],
    ["ACL", "GETUSER", "default"], ["ACL", "LOG"], ["ACL", "CAT"], ["ROLE"], ["LOLWUT", "VERSION", "5"],
    ["ECHO", "x"], ["PING"], ["PING", "x"], ["FUNCTION", "LIST"], ["SCRIPT", "EXISTS", "abc"],
    ["PUBSUB", "NUMSUB", "ch"], ["PUBSUB", "CHANNELS"], ["PUBSUB", "NUMPAT"],
    # scripting reply conversion
    ["EVAL", "return redis.call('HGETALL', KEYS[1])", "1", "h"],
    ["EVAL", "return nil", "0"], ["EVAL", "return {1, 'a', {2}}", "0"],
    ["EVAL", "return redis.call('ZSCORE', KEYS[1], 'a')", "1", "z"],
    ["EVAL", "return {double=3.5}", "0"], ["EVAL", "return {map={a=1}}", "0"],
    # transactions
    ["MULTI"], ["HGETALL", "h"], ["ZSCORE", "z", "a"], ["GET", "missing"], ["EXEC"],
    # more strings / keys
    ["MGET", "missing", "missing2"], ["GETEX", "s", "PERSIST"], ["TYPE", "missing"],
    ["OBJECT", "FREQ", "s"], ["OBJECT", "IDLETIME", "s"], ["COPY", "s", "scopy"],
    ["SORT", "l", "ALPHA"], ["SORT_RO", "l", "ALPHA", "LIMIT", "0", "1"],
    # more lists
    ["RPUSH", "l2", "x", "y", "z"], ["LPOS", "l2", "y", "COUNT", "0"], ["LRANGE", "missing", "0", "-1"],
    ["LMPOP", "1", "l2", "RIGHT", "COUNT", "2"], ["RPUSH", "l3", "q"], ["BLPOP", "l3", "1"],
    ["RPUSH", "l3", "q"], ["BLMPOP", "1", "1", "l3", "LEFT"], ["RPUSH", "l3", "q"],
    ["BRPOPLPUSH", "l3", "l4", "1"], ["BLMOVE", "l4", "l3", "LEFT", "RIGHT", "1"],
    # more hashes / sets
    ["HRANDFIELD", "h", "-3", "WITHVALUES"], ["HSCAN", "h", "0", "NOVALUES"], ["SPOP", "st2", "1"],
    ["SRANDMEMBER", "st", "-3"], ["SINTERSTORE", "sdst", "st", "st2"], ["SMOVE", "st", "st3", "x"],
    # more sorted sets
    ["ZADD", "z3", "1", "a", "2", "b", "3", "c"], ["ZRANGEBYLEX", "z3", "-", "+"],
    ["ZRANGE", "z3", "0", "1", "BYSCORE", "LIMIT", "0", "2", "WITHSCORES"],
    ["ZREVRANGEBYSCORE", "z3", "+inf", "-inf", "WITHSCORES"], ["ZPOPMIN", "z3", "2"],
    ["ZADD", "z4", "1", "a"], ["BZPOPMIN", "z4", "1"], ["ZADD", "z4", "1", "a"],
    ["BZMPOP", "1", "1", "z4", "MIN"], ["ZINTERCARD", "2", "z", "z2"], ["ZRANDMEMBER", "missing"],
    ["ZRANDMEMBER", "z3", "-2", "WITHSCORES"], ["ZADD", "z5", "inf", "x"], ["ZSCORE", "z5", "x"],
    ["ZINTERSTORE", "zdst", "2", "z", "z2"], ["ZRANGESTORE", "zdst2", "z", "0", "-1"],
    # more streams
    ["XADD", "xs2", "1-1", "a", "1"], ["XGROUP", "CREATE", "xs2", "g2", "0"],
    ["XREADGROUP", "GROUP", "g2", "c", "COUNT", "1", "STREAMS", "xs2", ">"],
    ["XAUTOCLAIM", "xs2", "g2", "c3", "0", "0", "JUSTID"], ["XINFO", "STREAM", "xs2", "FULL"],
    ["XTRIM", "xs2", "MAXLEN", "10"], ["XDEL", "xs2", "9-9"], ["XSETID", "xs2", "5-0"],
    ["XREAD", "COUNT", "1", "STREAMS", "xs", "xs2", "0", "0"],
    # geo
    ["GEOSEARCH", "geo", "FROMMEMBER", "Palermo", "BYBOX", "400", "400", "km", "WITHHASH", "WITHCOORD"],
    ["GEORADIUSBYMEMBER", "geo", "Palermo", "200", "km", "WITHCOORD", "WITHDIST", "WITHHASH"],
    ["GEOPOS", "missing", "x"], ["BITFIELD_RO", "bits", "GET", "u8", "0"],
    # server
    ["CLIENT", "LIST"], ["MEMORY", "STATS"], ["MEMORY", "DOCTOR"], ["LATENCY", "DOCTOR"],
    ["LATENCY", "HISTOGRAM", "get"], ["CONFIG", "GET", "maxmemory", "maxmemory-policy"],
    ["FUNCTION", "LOAD", "#!lua name=lib\nredis.register_function('f', function() return 1 end)"],
    ["FUNCTION", "LIST"], ["FUNCTION", "STATS"], ["FCALL", "f", "0"], ["SCRIPT", "LOAD", "return 1"],
    ["EVALSHA", "e0e1f9fabfc9d4800c877a703b823ac0578ff8db", "0"], ["OBJECT", "HELP"],
    ["CLIENT", "NO-EVICT", "on"], ["WAIT", "0", "10"], ["COMMAND", "LIST", "FILTERBY", "PATTERN", "xa*"],
    ["MODULE", "LIST"], ["INFO", "keyspace"], ["HELLO"], ["CLUSTER", "INFO"],
    # pub/sub last: the connection is subscribed afterwards
    ["SUBSCRIBE", "ch1"], ["PSUBSCRIBE", "p*"], ["UNSUBSCRIBE", "ch1"], ["PING"],
]


def main():
    ref, rud = Conn(int(sys.argv[1])), Conn(int(sys.argv[2]))
    for c in (ref, rud):
        c.cmd("HELLO", "3")
        for s in SETUP:
            c.cmd(*s)
    diffs = 0
    for p in PROBES:
        a, b = shape(ref.cmd(*p)), shape(rud.cmd(*p))
        if a != b:
            diffs += 1
            print(f"DIFF {' '.join(p)[:60]:62s} redis={a}  rudis={b}")
    print(f"{diffs} of {len(PROBES)} replies differ in shape")
    sys.exit(1 if diffs else 0)


if __name__ == "__main__":
    main()
