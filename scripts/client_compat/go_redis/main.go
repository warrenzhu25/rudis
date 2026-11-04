// go-redis compatibility suite for scripts/client_compat/run.py: the shared
// scenarios against a standalone rudis (RUDIS_PORT) and a cluster
// (RUDIS_CLUSTER_PORT), one JSON line per scenario. go-redis v9 speaks RESP3
// by default.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func addr(env string) string { return "127.0.0.1:" + os.Getenv(env) }

func version() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, d := range info.Deps {
			if d.Path == "github.com/redis/go-redis/v9" {
				return d.Version
			}
		}
	}
	return "?"
}

func check(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf(format, args...))
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func report(name string, fn func()) {
	ok, msg := true, ""
	func() {
		defer func() {
			if r := recover(); r != nil {
				ok, msg = false, fmt.Sprint(r)
			}
		}()
		fn()
	}()
	if len(msg) > 300 {
		msg = msg[:300]
	}
	line, _ := json.Marshal(map[string]any{"client": "go-redis", "version": version(),
		"scenario": name, "ok": ok, "error": msg})
	fmt.Println(string(line))
}

func client(proto int) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr("RUDIS_PORT"), Protocol: proto,
		ReadTimeout: 5 * time.Second})
}

func main() {
	r := client(3)
	report("strings", func() {
		must(r.FlushAll(ctx).Result())
		must(r.Set(ctx, "s", "v", 100*time.Second).Result())
		check(must(r.Get(ctx, "s").Result()) == "v", "get")
		ttl := must(r.TTL(ctx, "s").Result())
		check(ttl > 0 && ttl <= 100*time.Second, "ttl %v", ttl)
		check(must(r.IncrBy(ctx, "n", 6).Result()) == 6, "incrby")
		must(r.MSet(ctx, "a", "1", "b", "2").Result())
		vals := must(r.MGet(ctx, "a", "b", "missing").Result())
		check(vals[0] == "1" && vals[1] == "2" && vals[2] == nil, "mget %v", vals)
		ok := must(r.SetNX(ctx, "s", "w", 0).Result())
		check(!ok, "setnx on existing key")
		_, err := r.Get(ctx, "missing").Result()
		check(errors.Is(err, redis.Nil), "get missing: %v", err)
	})
	report("collections", func() {
		must(r.HSet(ctx, "h", "f1", "1", "f2", "2").Result())
		h := must(r.HGetAll(ctx, "h").Result())
		check(len(h) == 2 && h["f1"] == "1" && h["f2"] == "2", "hgetall %v", h)
		must(r.RPush(ctx, "l", "a", "b", "c").Result())
		check(fmt.Sprint(must(r.LRange(ctx, "l", 0, -1).Result())) == "[a b c]", "lrange")
		must(r.SAdd(ctx, "st", "x", "y").Result())
		check(len(must(r.SMembers(ctx, "st").Result())) == 2, "smembers")
		must(r.ZAdd(ctx, "z", redis.Z{Score: 1.5, Member: "m1"}).Result())
		check(must(r.ZScore(ctx, "z", "m1").Result()) == 1.5, "zscore")
		zs := must(r.ZRangeWithScores(ctx, "z", 0, -1).Result())
		check(len(zs) == 1 && zs[0].Score == 1.5, "zrange withscores %v", zs)
	})
	report("pipeline", func() {
		p := r.Pipeline()
		for i := 0; i < 1000; i++ {
			p.Set(ctx, fmt.Sprintf("p:%d", i), i, 0)
		}
		must(p.Exec(ctx))
		p = r.Pipeline()
		cmds := make([]*redis.StringCmd, 1000)
		for i := range cmds {
			cmds[i] = p.Get(ctx, fmt.Sprintf("p:%d", i))
		}
		must(p.Exec(ctx))
		for i, c := range cmds {
			check(c.Val() == fmt.Sprint(i), "pipelined get %d", i)
		}
	})
	report("transaction", func() {
		must(r.Set(ctx, "tx", "0", 0).Result())
		cmds := must(r.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Incr(ctx, "tx")
			p.Incr(ctx, "tx")
			return nil
		}))
		check(cmds[1].(*redis.IntCmd).Val() == 2, "multi/exec")
		err := r.Watch(ctx, func(tx *redis.Tx) error {
			must(client(3).Set(ctx, "tx", "changed", 0).Result())
			_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, "tx", "mine", 0)
				return nil
			})
			return err
		}, "tx")
		check(errors.Is(err, redis.TxFailedErr), "watch conflict: %v", err)
	})
	report("pubsub", func() {
		ps := r.PSubscribe(ctx, "pat.*")
		defer ps.Close()
		if err := ps.Subscribe(ctx, "ch"); err != nil {
			panic(err)
		}
		must(ps.Receive(ctx))
		ch := ps.Channel()
		got := map[string]string{}
		deadline := time.After(5 * time.Second)
		for len(got) < 2 {
			r.Publish(ctx, "ch", "m1")
			r.Publish(ctx, "pat.x", "m2")
			select {
			case m := <-ch:
				got[m.Channel] = m.Payload
			case <-time.After(200 * time.Millisecond):
			case <-deadline:
				panic(fmt.Sprintf("messages %v", got))
			}
		}
		check(got["ch"] == "m1" && got["pat.x"] == "m2", "messages %v", got)
	})
	report("scripting", func() {
		check(must(r.Eval(ctx, "return redis.call('SET', KEYS[1], ARGV[1])", []string{"lk"}, "lv").Result()) == "OK", "eval")
		s := redis.NewScript("return redis.call('GET', KEYS[1])")
		check(must(s.Run(ctx, r, []string{"lk"}).Result()) == "lv", "evalsha")
		must(r.ScriptFlush(ctx).Result())
		check(must(s.Run(ctx, r, []string{"lk"}).Result()) == "lv", "evalsha after flush")
	})
	report("blocking", func() {
		r.Del(ctx, "bq")
		go func() { time.Sleep(200 * time.Millisecond); client(3).RPush(ctx, "bq", "x") }()
		v := must(r.BLPop(ctx, 5*time.Second, "bq").Result())
		check(v[0] == "bq" && v[1] == "x", "blpop %v", v)
	})
	report("streams", func() {
		r.Del(ctx, "xs")
		must(r.XAdd(ctx, &redis.XAddArgs{Stream: "xs", Values: map[string]any{"f": "v"}}).Result())
		must(r.XGroupCreate(ctx, "xs", "g", "0").Result())
		got := must(r.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c",
			Streams: []string{"xs", ">"}, Count: 10}).Result())
		check(len(got) == 1 && got[0].Stream == "xs" && len(got[0].Messages) == 1, "xreadgroup %v", got)
		check(must(r.XAck(ctx, "xs", "g", got[0].Messages[0].ID).Result()) == 1, "xack")
		check(must(r.XPending(ctx, "xs", "g").Result()).Count == 0, "xpending")
	})
	report("scan", func() {
		must(r.FlushAll(ctx).Result())
		for i := 0; i < 500; i++ {
			r.Set(ctx, fmt.Sprintf("scan:%d", i), 1, 0)
		}
		seen := map[string]bool{}
		it := r.Scan(ctx, 0, "scan:*", 50).Iterator()
		for it.Next(ctx) {
			seen[it.Val()] = true
		}
		must(0, it.Err())
		check(len(seen) == 500, "scan saw %d", len(seen))
	})
	report("resp3", func() {
		// The suite runs on RESP3; this checks RESP2 still works too.
		r2 := client(2)
		must(r2.HSet(ctx, "r2h", "f", "1").Result())
		check(must(r2.HGetAll(ctx, "r2h").Result())["f"] == "1", "resp2 hgetall")
		must(r2.ZAdd(ctx, "r2z", redis.Z{Score: 2.5, Member: "a"}).Result())
		check(must(r2.ZScore(ctx, "r2z", "a").Result()) == 2.5, "resp2 zscore")
	})
	report("cluster", func() {
		rc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{addr("RUDIS_CLUSTER_PORT")}})
		defer rc.Close()
		for i := 0; i < 200; i++ {
			must(rc.Set(ctx, fmt.Sprintf("ck:%d", i), i, 0).Result())
		}
		for i := 0; i < 200; i++ {
			check(must(rc.Get(ctx, fmt.Sprintf("ck:%d", i)).Result()) == fmt.Sprint(i), "get ck:%d", i)
		}
		must(rc.MSet(ctx, "{t}a", "1", "{t}b", "2").Result())
		check(fmt.Sprint(must(rc.MGet(ctx, "{t}a", "{t}b").Result())) == "[1 2]", "mget")
	})
}
