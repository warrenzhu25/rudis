#!/usr/bin/env bash
# Interleaved Rudis vs Dragonfly, same setup as benchmark_vs_dragonfly_valkey.py
# (8 server threads on cores 0-7, memtier 8 threads x 8 conns on 32-47, 1 KB
# values), but timed runs and alternating engines every round so host drift
# hits both equally. Reports medians.
set -u
RUDIS_BIN=${RUDIS_BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release/rudis}
DRAGONFLY_BIN=${DRAGONFLY_BIN:-$HOME/dragonfly}
MT=${MEMTIER_BIN:-$HOME/memtier_benchmark/memtier_benchmark}
CLI=${CLI_BIN:-$HOME/valkey-stable/src/valkey-cli}
PORT=17862
OUT=${OUTF:-/tmp/rudis_vs_dragonfly_interleaved.txt}
ROUNDS=${ROUNDS:-5}
T=${T:-10}
: > "$OUT"
start() {
  if [ "$1" = rudis ]; then
    taskset -c 0-7 "$RUDIS_BIN" --port $PORT --threads 8 --bind 127.0.0.1 >/dev/null 2>&1 &
  else
    taskset -c 0-7 "$DRAGONFLY_BIN" --port $PORT --proactor_threads=8 --cache_mode=false --dbfilename= --logtostderr=false >/dev/null 2>&1 &
  fi
  PID=$!
  for _ in $(seq 100); do $CLI -p $PORT PING >/dev/null 2>&1 && break; sleep 0.1; done
  taskset -c 32-47 $MT -s 127.0.0.1 -p $PORT -t 8 -c 8 --ratio 1:0 --pipeline 16 -d 1024 \
    --key-minimum 1 --key-maximum 64000 --key-pattern P:P --requests allkeys --hide-histogram >/dev/null 2>&1
  for i in $(seq 0 9); do $CLI -p $PORT SET "{user:tag}key_$i" x >/dev/null; done
}
mt() { taskset -c 32-47 $MT -s 127.0.0.1 -p $PORT -t 8 -c 8 --test-time $T --hide-histogram -d 1024 \
         --key-minimum 1 --key-maximum 64000 "$@" 2>/dev/null | grep -E "^Totals" | awk '{print $2, $NF}'; }
K10='__key__ __key__ __key__ __key__ __key__ __key__ __key__ __key__ __key__ __key__'
for r in $(seq 1 $ROUNDS); do
  for eng in rudis df; do
    start $eng
    echo "$eng r$r set_p1 $(mt --ratio 1:0 --pipeline 1 --key-pattern R:R)" | tee -a "$OUT"
    echo "$eng r$r get_p1 $(mt --ratio 0:1 --pipeline 1 --key-pattern R:R)" | tee -a "$OUT"
    echo "$eng r$r mixed_p1 $(mt --ratio 1:1 --pipeline 1 --key-pattern R:R)" | tee -a "$OUT"
    echo "$eng r$r mget10_scattered $(mt --pipeline 1 --command "MGET $K10" --command-key-pattern R)" | tee -a "$OUT"
    echo "$eng r$r mset10_scattered $(mt --pipeline 1 --command "MSET __key__ __data__ __key__ __data__ __key__ __data__ __key__ __data__ __key__ __data__ __key__ __data__ __key__ __data__ __key__ __data__ __key__ __data__ __key__ __data__" --command-key-pattern R)" | tee -a "$OUT"
    echo "$eng r$r mget10_colocated $(mt --pipeline 1 --command "MGET {user:tag}key_0 {user:tag}key_1 {user:tag}key_2 {user:tag}key_3 {user:tag}key_4 {user:tag}key_5 {user:tag}key_6 {user:tag}key_7 {user:tag}key_8 {user:tag}key_9")" | tee -a "$OUT"
    kill $PID; wait $PID 2>/dev/null
  done
done
rm -rf rudis_tier_$PORT
python3 - "$OUT" <<'EOF'
import sys, statistics, collections
d = collections.defaultdict(list)
for line in open(sys.argv[1]):
    p = line.split()
    if len(p) >= 4:
        try: d[(p[2], p[0])].append(float(p[3]))
        except ValueError: pass
wls = sorted({k[0] for k in d})
print(f"{'workload':20s} {'rudis':>12s} {'df':>12s} {'delta':>8s}  spread(r/df)")
for w in wls:
    r, f = d[(w, 'rudis')], d[(w, 'df')]
    mr, mf = statistics.median(r), statistics.median(f)
    sp = lambda v: f"{(max(v)-min(v))/statistics.median(v)*100:.0f}%"
    print(f"{w:20s} {mr:12.0f} {mf:12.0f} {(mr/mf-1)*100:+7.1f}%  {sp(r)}/{sp(f)}")
EOF
