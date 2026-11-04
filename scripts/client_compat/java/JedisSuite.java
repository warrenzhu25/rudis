// Jedis compatibility suite for scripts/client_compat/run.py: the shared
// scenarios against RUDIS_PORT and RUDIS_CLUSTER_PORT, one JSON line each.
import java.util.*;
import redis.clients.jedis.*;
import redis.clients.jedis.params.*;
import redis.clients.jedis.resps.*;
import redis.clients.jedis.args.*;

public class JedisSuite {
    static final int PORT = Integer.parseInt(System.getenv("RUDIS_PORT"));
    static final int CPORT = Integer.parseInt(System.getenv("RUDIS_CLUSTER_PORT"));
    static String version = "?";

    interface Body { void run() throws Exception; }

    static void check(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
    }

    static void report(String name, Body body) {
        String err = "";
        boolean ok = true;
        try { body.run(); } catch (Throwable t) { ok = false; err = String.valueOf(t); }
        if (err.length() > 300) err = err.substring(0, 300);
        System.out.println("{\"client\":\"jedis\",\"version\":\"" + version + "\",\"scenario\":\"" + name
            + "\",\"ok\":" + ok + ",\"error\":\"" + esc(err) + "\"}");
    }

    public static void main(String[] args) throws Exception {
        String clientVersion = System.getenv("CC_CLIENT_VERSION");
        if (clientVersion != null) version = clientVersion;
        Jedis r = new Jedis("127.0.0.1", PORT, 5000);
        report("strings", () -> {
            r.flushAll();
            check("OK".equals(r.set("s", "v", SetParams.setParams().ex(100))), "set");
            check("v".equals(r.get("s")), "get");
            long ttl = r.ttl("s"); check(ttl > 0 && ttl <= 100, "ttl " + ttl);
            check(r.incrBy("n", 6) == 6, "incrby");
            r.mset("a", "1", "b", "2");
            check(Arrays.asList("1", "2", null).equals(r.mget("a", "b", "missing")), "mget");
            check(r.set("s", "w", SetParams.setParams().nx()) == null, "set nx");
            check(r.get("missing") == null, "get missing");
        });
        report("collections", () -> {
            r.hset("h", Map.of("f1", "1", "f2", "2"));
            check(Map.of("f1", "1", "f2", "2").equals(r.hgetAll("h")), "hgetall");
            r.rpush("l", "a", "b", "c");
            check(List.of("a", "b", "c").equals(r.lrange("l", 0, -1)), "lrange");
            r.sadd("st", "x", "y");
            check(r.smembers("st").size() == 2, "smembers");
            r.zadd("z", 1.5, "m1");
            check(r.zscore("z", "m1") == 1.5, "zscore");
        });
        report("pipeline", () -> {
            Pipeline pl = r.pipelined();
            for (int i = 0; i < 1000; i++) pl.set("p:" + i, "" + i);
            pl.sync();
            pl = r.pipelined();
            List<Response<String>> gets = new ArrayList<>();
            for (int i = 0; i < 1000; i++) gets.add(pl.get("p:" + i));
            pl.sync();
            for (int i = 0; i < 1000; i++) check(("" + i).equals(gets.get(i).get()), "pipelined get " + i);
        });
        report("transaction", () -> {
            r.set("tx", "0");
            Transaction t = r.multi();
            t.incr("tx"); t.incr("tx");
            List<Object> res = t.exec();
            check(Long.valueOf(2).equals(res.get(1)), "multi/exec " + res);
            try (Jedis w = new Jedis("127.0.0.1", PORT)) {
                w.watch("tx");
                r.set("tx", "changed");
                Transaction t2 = w.multi();
                t2.set("tx", "mine");
                check(t2.exec() == null, "watch conflict");
            }
        });
        report("pubsub", () -> {
            Map<String, String> got = Collections.synchronizedMap(new HashMap<>());
            JedisPubSub ps = new JedisPubSub() {
                public void onMessage(String ch, String m) { got.put(ch, m); }
                public void onPMessage(String pat, String ch, String m) { got.put(ch, m); }
            };
            Thread th = new Thread(() -> {
                try (Jedis s = new Jedis("127.0.0.1", PORT)) { s.subscribe(ps, "ch"); }
            });
            th.start();
            Thread.sleep(300);
            ps.psubscribe("pat.*");
            for (int i = 0; i < 25 && got.size() < 2; i++) {
                r.publish("ch", "m1"); r.publish("pat.x", "m2"); Thread.sleep(200);
            }
            ps.unsubscribe(); ps.punsubscribe();
            th.join(5000);
            check("m1".equals(got.get("ch")) && "m2".equals(got.get("pat.x")), "messages " + got);
        });
        report("scripting", () -> {
            check("OK".equals(r.eval("return redis.call('SET', KEYS[1], ARGV[1])", 1, "lk", "lv")), "eval");
            String sha = r.scriptLoad("return redis.call('GET', KEYS[1])");
            check("lv".equals(r.evalsha(sha, 1, "lk")), "evalsha");
        });
        report("blocking", () -> {
            r.del("bq");
            new Thread(() -> {
                try { Thread.sleep(200); } catch (InterruptedException e) { }
                try (Jedis j = new Jedis("127.0.0.1", PORT)) { j.rpush("bq", "x"); }
            }).start();
            try (Jedis b = new Jedis("127.0.0.1", PORT, 10000)) {
                List<String> v = b.blpop(5, "bq");
                check(List.of("bq", "x").equals(v), "blpop " + v);
            }
        });
        report("streams", () -> {
            r.del("xs");
            r.xadd("xs", StreamEntryID.NEW_ENTRY, Map.of("f", "v"));
            r.xgroupCreate("xs", "g", new StreamEntryID(), false);
            List<Map.Entry<String, List<StreamEntry>>> got = r.xreadGroup("g", "c",
                XReadGroupParams.xReadGroupParams().count(10), Map.of("xs", StreamEntryID.UNRECEIVED_ENTRY));
            check(got != null && got.size() == 1 && got.get(0).getValue().size() == 1, "xreadgroup " + got);
            check(r.xack("xs", "g", got.get(0).getValue().get(0).getID()) == 1, "xack");
            check(r.xpending("xs", "g").getTotal() == 0, "xpending");
        });
        report("scan", () -> {
            r.flushAll();
            for (int i = 0; i < 500; i++) r.set("scan:" + i, "1");
            Set<String> seen = new HashSet<>();
            String cur = "0";
            do {
                ScanResult<String> sr = r.scan(cur, new ScanParams().match("scan:*").count(50));
                seen.addAll(sr.getResult());
                cur = sr.getCursor();
            } while (!"0".equals(cur));
            check(seen.size() == 500, "scan saw " + seen.size());
        });
        report("resp3", () -> {
            try (Jedis r3 = new Jedis(new HostAndPort("127.0.0.1", PORT),
                    DefaultJedisClientConfig.builder().protocol(RedisProtocol.RESP3).build())) {
                r3.hset("r3h", "f", "1");
                check("1".equals(r3.hgetAll("r3h").get("f")), "hgetall");
                r3.zadd("r3z", 2.5, "a");
                check(r3.zscore("r3z", "a") == 2.5, "zscore");
                check(r3.get("missing") == null, "null");
            }
        });
        report("cluster", () -> {
            try (JedisCluster rc = new JedisCluster(new HostAndPort("127.0.0.1", CPORT))) {
                for (int i = 0; i < 200; i++) rc.set("ck:" + i, "" + i);
                for (int i = 0; i < 200; i++) check(("" + i).equals(rc.get("ck:" + i)), "get ck:" + i);
                rc.mset("{t}a", "1", "{t}b", "2");
                check(List.of("1", "2").equals(rc.mget("{t}a", "{t}b")), "mget");
            }
        });
        r.close();
        System.exit(0);
    }
}
