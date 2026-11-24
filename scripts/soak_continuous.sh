#!/bin/bash
# Continuous cluster soak on the latest origin/main: back-to-back segments
# of scripts/soak_cluster.py, each on the newest pushed build, so pushes
# never wait for a soak and every build gets soaked soon after it lands.
#
#   scripts/soak_continuous.sh [segment-hours] [extra soak_cluster.py args...]
#
# Defaults: 6 h segments, normal-pace chaos. Builds in its own worktree
# (never touches your working copy) and runs each segment with that
# commit's own harness. One line per segment goes to
# $ROOT/summary.log; each segment's full report stays in its directory.
# Touch $ROOT/STOP to end after the current segment.
#
# Segments run on physical cores 24-31 and their SMT siblings (CCX 3; the
# soak_cluster.py default), which local CI and the perf A/B leave alone, so
# CI no longer interrupts them. Creating $ROOT/PAUSE still holds the next
# segment back (e.g. for a full benchmark run), until it is removed.
set -uo pipefail

SEG_HOURS="${1:-6}"
shift || true
REPO="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="${SOAK_ROOT:-$HOME/soak-runs/continuous}"
WT="$ROOT/build-wt"
BIN_DIR="${SOAK_BIN_DIR:-$HOME/soak-bin}"
mkdir -p "$ROOT" "$BIN_DIR"
rm -f "$ROOT/STOP"

log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$ROOT/summary.log"; }

while [ ! -e "$ROOT/STOP" ]; do
    while [ -e "$ROOT/PAUSE" ] && [ ! -e "$ROOT/STOP" ]; do
        sleep 30
    done
    [ -e "$ROOT/STOP" ] && break
    git -C "$REPO" fetch -q origin main || { log "fetch failed; retrying in 5 min"; sleep 300; continue; }
    sha=$(git -C "$REPO" rev-parse --short origin/main)
    if [ ! -d "$WT" ]; then
        git -C "$REPO" worktree add --detach "$WT" "$sha" >/dev/null 2>&1
    fi
    git -C "$WT" checkout -q --detach "$sha"
    bin="$BIN_DIR/rudis-$sha"
    if [ ! -x "$bin" ]; then
        if ! (cd "$WT" && nice cargo build -q --release) >"$ROOT/build-$sha.log" 2>&1; then
            log "$sha: build failed (see build-$sha.log); retrying in 30 min"
            sleep 1800
            continue
        fi
        cp "$WT/target/release/rudis" "$bin"
    fi
    dir="$ROOT/$(date -u +%Y%m%dT%H%M)-$sha"
    log "$sha: segment start ($SEG_HOURS h) in $dir"
    python3 -u "$WT/scripts/soak_cluster.py" --binary "$bin" --hours "$SEG_HOURS" \
        --dir "$dir" "$@" >"$dir.out" 2>&1 &
    echo $! >"$ROOT/segment.pid"
    wait $!
    rc=$?
    rm -f "$ROOT/segment.pid"
    report="$dir/report.md"
    if [ ! -f "$report" ]; then
        log "$sha: segment ended (exit $rc) WITHOUT a report: see $dir.out"
        continue
    fi
    verdict=$(python3 - "$report" <<'PY'
import json, re, sys
text = open(sys.argv[1]).read()
anom = text.split("## Anomalies", 1)[1].split("##", 1)[0].strip() if "## Anomalies" in text else "?"
ok = anom.startswith("None")
m = re.search(r"```\n(\{.*?\})\n```", text, re.S)
final = json.loads(m.group(1)) if m else {}
lost = final.get("counters", {}).get("lost", "?")
mism = final.get("big_values", {}).get("mismatches", "-")
pairs = final.get("pairs", [])
same = all(p.get("same") for p in pairs) if pairs else "?"
mt = re.search(r"memtier restarts[^:]*: (\d+)", text)
p99 = re.search(r"\| probe p99 \(ms\) \| [\d.]+ \| ([\d.]+) \|", text)
mem = re.search(r"\| used_memory, all nodes \(MB\) \| ([\d.]+) \| ([\d.]+) \|", text)
trend = re.search(r"used_memory trend after warmup: ([+-][\d.]+ MB/h)", text)
print(("OK" if ok and lost == 0 and mism in (0, "-") and same is True else "PROBLEM")
      + f" lost={lost} big_mismatches={mism} replicas_same={same}"
      + f" memtier_restarts={mt.group(1) if mt else '?'}"
      + f" p99_last_h={p99.group(1) if p99 else '?'}ms"
      + (f" mem={mem.group(1)}->{mem.group(2)}MB" if mem else "")
      + (f" mem_trend={trend.group(1).replace(' ', '')}" if trend else "")
      + ("" if ok else f" anomalies: {anom[:200]!r}"))
PY
)
    log "$sha: segment done (exit $rc): $verdict"
done
log "stopped (STOP file)"
