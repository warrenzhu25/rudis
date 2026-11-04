// Lettuce compatibility suite for scripts/client_compat/run.py: the shared
// scenarios against RUDIS_PORT and RUDIS_CLUSTER_PORT, one JSON line each.
// Lettuce 6 negotiates RESP3 (HELLO 3) by default.
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class LettuceSuite {
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
        System.out.println("{\"client\":\"lettuce\",\"version\":\"" + version + "\",\"scenario\":\"" + name
            + "\",\"ok\":" + ok + ",\"error\":\"" + esc(err) + "\"}");
    }

    public static void main(String[] args) throws Exception {
        String clientVersion = System.getenv("CC_CLIENT_VERSION");
        if (clientVersion != null) version = clientVersion;
        RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", PORT));
        client.setDefaultTimeout(Duration.ofSeconds(10));
        StatefulRedisConnection<String, String> conn = client.connect();
        RedisCommands<String, String> r = conn.sync();
        report("strings", () -> {
            r.flushall();
            check("OK".equals(r.set("s", "v", SetArgs.Builder.ex(100))), "set");
            check("v".equals(r.get("s")), "get");
            long ttl = r.ttl("s"); check(ttl > 0 && ttl <= 100, "ttl " + ttl);
            check(r.incrby("n", 6) == 6, "incrby");
            r.mset(Map.of("a", "1", "b", "2"));
            List<KeyValue<String, String>> kv = r.mget("a", "b", "missing");
            check("1".equals(kv.get(0).getValue()) && "2".equals(kv.get(1).getValue()) && !kv.get(2).hasValue(), "mget " + kv);
            check(r.set("s", "w", SetArgs.Builder.nx()) == null, "set nx");
            check(r.get("missing") == null, "get missing");
        });
        report("collections", () -> {
            r.hset("h", Map.of("f1", "1", "f2", "2"));
            check(Map.of("f1", "1", "f2", "2").equals(r.hgetall("h")), "hgetall " + r.hgetall("h"));
            r.rpush("l", "a", "b", "c");
            check(List.of("a", "b", "c").equals(r.lrange("l", 0, -1)), "lrange");
            r.sadd("st", "x", "y");
            check(r.smembers("st").size() == 2, "smembers");
            r.zadd("z", 1.5, "m1");
            check(r.zscore("z", "m1") == 1.5, "zscore");
        });
        report("pipeline", () -> {
            RedisAsyncCommands<String, String> a = conn.async();
            conn.setAutoFlushCommands(false);
            List<RedisFuture<String>> futs = new ArrayList<>();
            for (int i = 0; i < 1000; i++) a.set("p:" + i, "" + i);
            for (int i = 0; i < 1000; i++) futs.add(a.get("p:" + i));
            conn.flushCommands();
            conn.setAutoFlushCommands(true);
            for (int i = 0; i < 1000; i++) check(("" + i).equals(futs.get(i).get(10, TimeUnit.SECONDS)), "pipelined get " + i);
        });
        report("transaction", () -> {
            r.set("tx", "0");
            r.multi(); r.incr("tx"); r.incr("tx");
            TransactionResult res = r.exec();
            check(Long.valueOf(2).equals(res.get(1)), "multi/exec");
            try (StatefulRedisConnection<String, String> w = client.connect()) {
                w.sync().watch("tx");
                r.set("tx", "changed");
                w.sync().multi(); w.sync().set("tx", "mine");
                check(w.sync().exec().wasDiscarded(), "watch conflict");
            }
        });
        report("pubsub", () -> {
            Map<String, String> got = new ConcurrentHashMap<>();
            StatefulRedisPubSubConnection<String, String> ps = client.connectPubSub();
            ps.addListener(new RedisPubSubAdapter<String, String>() {
                public void message(String ch, String m) { got.put(ch, m); }
                public void message(String pat, String ch, String m) { got.put(ch, m); }
            });
            ps.sync().subscribe("ch");
            ps.sync().psubscribe("pat.*");
            for (int i = 0; i < 25 && got.size() < 2; i++) {
                r.publish("ch", "m1"); r.publish("pat.x", "m2"); Thread.sleep(200);
            }
            ps.close();
            check("m1".equals(got.get("ch")) && "m2".equals(got.get("pat.x")), "messages " + got);
        });
        report("scripting", () -> {
            String ok = r.eval("return redis.call('SET', KEYS[1], ARGV[1])", ScriptOutputType.STATUS, new String[]{"lk"}, "lv");
            check("OK".equals(ok), "eval " + ok);
            String sha = r.scriptLoad("return redis.call('GET', KEYS[1])");
            String v = r.evalsha(sha, ScriptOutputType.VALUE, "lk");
            check("lv".equals(v), "evalsha " + v);
        });
        report("blocking", () -> {
            r.del("bq");
            new Thread(() -> {
                try { Thread.sleep(200); } catch (InterruptedException e) { }
                try (StatefulRedisConnection<String, String> c = client.connect()) { c.sync().rpush("bq", "x"); }
            }).start();
            try (StatefulRedisConnection<String, String> b = client.connect()) {
                KeyValue<String, String> v = b.sync().blpop(5, "bq");
                check(v != null && "bq".equals(v.getKey()) && "x".equals(v.getValue()), "blpop " + v);
            }
        });
        report("streams", () -> {
            r.del("xs");
            r.xadd("xs", Map.of("f", "v"));
            r.xgroupCreate(XReadArgs.StreamOffset.from("xs", "0"), "g");
            List<StreamMessage<String, String>> got = r.xreadgroup(Consumer.from("g", "c"),
                XReadArgs.Builder.count(10), XReadArgs.StreamOffset.lastConsumed("xs"));
            check(got.size() == 1 && "xs".equals(got.get(0).getStream()), "xreadgroup " + got);
            check(r.xack("xs", "g", got.get(0).getId()) == 1, "xack");
            check(r.xpending("xs", "g").getCount() == 0, "xpending");
        });
        report("scan", () -> {
            r.flushall();
            for (int i = 0; i < 500; i++) r.set("scan:" + i, "1");
            Set<String> seen = new HashSet<>();
            KeyScanCursor<String> cur = r.scan(ScanArgs.Builder.matches("scan:*").limit(50));
            seen.addAll(cur.getKeys());
            while (!cur.isFinished()) {
                cur = r.scan(cur, ScanArgs.Builder.matches("scan:*").limit(50));
                seen.addAll(cur.getKeys());
            }
            check(seen.size() == 500, "scan saw " + seen.size());
        });
        report("resp3", () -> {
            // The suite runs on RESP3; this checks RESP2 still works too.
            RedisClient c2 = RedisClient.create(RedisURI.create("127.0.0.1", PORT));
            c2.setOptions(ClientOptions.builder().protocolVersion(ProtocolVersion.RESP2).build());
            try (StatefulRedisConnection<String, String> conn2 = c2.connect()) {
                conn2.sync().hset("r2h", "f", "1");
                check("1".equals(conn2.sync().hgetall("r2h").get("f")), "resp2 hgetall");
                conn2.sync().zadd("r2z", 2.5, "a");
                check(conn2.sync().zscore("r2z", "a") == 2.5, "resp2 zscore");
            }
            c2.shutdown();
        });
        report("cluster", () -> {
            RedisClusterClient cc = RedisClusterClient.create(RedisURI.create("127.0.0.1", CPORT));
            try (var cconn = cc.connect()) {
                RedisAdvancedClusterCommands<String, String> rc = cconn.sync();
                for (int i = 0; i < 200; i++) rc.set("ck:" + i, "" + i);
                for (int i = 0; i < 200; i++) check(("" + i).equals(rc.get("ck:" + i)), "get ck:" + i);
                rc.mset(Map.of("{t}a", "1", "{t}b", "2"));
                List<KeyValue<String, String>> kv = rc.mget("{t}a", "{t}b");
                check("1".equals(kv.get(0).getValue()) && "2".equals(kv.get(1).getValue()), "mget " + kv);
            }
            cc.shutdown();
        });
        conn.close();
        client.shutdown();
        System.exit(0);
    }
}
