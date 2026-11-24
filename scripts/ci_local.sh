#!/usr/bin/env bash
# Every job of .github/workflows/ci.yml, run locally on a clean worktree of a
# commit, so a series can be checked before it is pushed (GitHub runs CI on
# the pushed head only).
#
#   scripts/ci_local.sh <rev>               # full CI on <rev>
#   scripts/ci_local.sh --series <base> <tip>
#       per commit in base..tip: fmt, clippy (both feature sets), unit tests,
#       test targets build, and the perf A/B against its parent when it
#       changes src/; then full CI on <tip>
#
# Full CI lints and builds everything first, one step at a time, then runs
# the test groups at the same time in lanes, each on its own physical cores
# and in its own network namespace (see lane() below): release e2e on
# io_uring; the debug e2e on the legacy driver, instrumented for coverage;
# and three lanes of Redis TCL suites. Each step's time is logged.
#
# A series runs only what its changes need (agent.md, Rule 4): nothing for
# docs only, a YAML check for workflows only, everything otherwise (the
# perf A/B only for commits that change src/). CI_FULL=1 forces everything.
#
# CI_ALLOW_NOISY=1 passes --allow-noisy to the perf A/B: on a host too
# noisy for a perf verdict, the series still checks every commit's build,
# lint and tests. The perf verdict is then still owed: rerun the A/B for
# the tip on a quiet host before pushing.
#
# It never pushes: push only after it exits 0. The e2e and TCL suites are
# timing-sensitive, so keep the host otherwise quiet while it runs (builds
# or benchmarks alongside it have failed tests that pass alone).
#
# Needs: unshare (user + network namespaces), tclsh, cargo-deny,
# cargo-llvm-cov, and the memtier binary check_perf_regression.py uses.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WT="$REPO/target/ci-worktree"
LOGS="$REPO/target/ci-logs"
mkdir -p "$LOGS"
# shellcheck disable=SC1091
source "$HOME/.cargo/env"

usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

checkout() { # rev
    if [ ! -d "$WT" ]; then
        git -C "$REPO" worktree add --detach "$WT" "$1" >/dev/null
    else
        git -C "$WT" checkout -q --detach "$1"
        git -C "$WT" clean -fdq -e target
    fi
}

# Runs a step, logging to $LOG, and prints how long it took; on failure
# shows the tail and exits 1 (inside a lane, only that lane's subshell).
step() { # name, command...
    local name="$1" t=$SECONDS; shift
    printf '== %s%s\n' "${LANE:+[$LANE] }" "$name" | tee -a "$LOG"
    if ! "$@" >> "$LOG" 2>&1; then
        tail -60 "$LOG"
        echo "CI FAILED at: ${LANE:+[$LANE] }$name ($SHA, $((SECONDS - t))s)" >&2
        exit 1
    fi
    printf '   %s%s: %ds\n' "${LANE:+[$LANE] }" "$name" "$((SECONDS - t))" | tee -a "$LOG"
}

# Runs a shell snippet in a private network namespace (loopback only), so
# fixed test ports never clash with anything else on the host, nor with the
# other lanes.
netns() { unshare -rn bash -c "ip link set lo up && $*"; }

# The test executables `cargo test --no-run $@` builds, one path per line,
# longest-running first (so a worker pool finishes them soonest).
test_executables() { # cargo test args...
    cargo test --no-run --message-format=json "$@" 2>/dev/null | python3 -c '
import json, os, sys
order = ["e2e_replication", "e2e_persistence", "e2e_memory", "e2e_commands", "e2e_cluster"]
exes = []
for line in sys.stdin:
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if m.get("reason") == "compiler-artifact" and m.get("profile", {}).get("test") and m.get("executable"):
        exes.append(m["executable"])
def rank(e):
    n = os.path.basename(e).rsplit("-", 1)[0]
    return order.index(n) if n in order else len(order)
print("\n".join(sorted(set(exes), key=rank)))
'
}

# Runs test executables (paths in $1 matching $2) with $3 at a time, each in
# its own network namespace, logging each to its own file. Fails if any
# fails, with that log's tail.
run_tests() { # list-file, regex, workers
    local list="$1" re="$2" workers="$3" exe name rc=0
    local -A pid_name=()
    while read -r exe; do
        [ -n "$exe" ] || continue
        while [ "$(jobs -rp | wc -l)" -ge "$workers" ]; do wait -n || true; done
        name="$(basename "$exe")"
        # Serial inside an e2e binary (fixed ports); unit tests in parallel.
        local args=""
        case "$name" in e2e_*) args="--test-threads=1" ;; esac
        (cd "$WT" && netns "'$exe' $args") > "$LOGS/ci_$SHA.${LANE:-}.$name.log" 2>&1 &
        pid_name[$!]="$name"
    done < <(grep -E "$re" "$list")
    for pid in "${!pid_name[@]}"; do
        name="${pid_name[$pid]}"
        if wait "$pid"; then
            grep -h "^test result" "$LOGS/ci_$SHA.${LANE:-}.$name.log" | sed "s/^/   $name: /"
        else
            echo "   FAILED: $name"
            tail -40 "$LOGS/ci_$SHA.${LANE:-}.$name.log"
            rc=1
        fi
    done
    return $rc
}

# Lanes: test groups that run at the same time, each pinned to its own
# physical cores (with their SMT siblings: vCPU i and i+32 share a core on
# this 32-core / 64-vCPU host), in its own network namespace and with its
# own copy of the TCL tree. Everything is built before they start, so no
# compile competes with their timing-sensitive tests.
LANE_PIDS=()
LANE_NAMES=()
lane() { # name, cpus, function
    local name="$1" cpus="$2" fn="$3"
    (
        taskset -pc "$cpus" "$BASHPID" > /dev/null
        LANE="$name"
        LOG="$LOGS/ci_$SHA.$name.log"
        : > "$LOG"
        "$fn"
    ) &
    LANE_PIDS+=("$!")
    LANE_NAMES+=("$name")
}
wait_lanes() {
    local i failed=""
    for i in "${!LANE_PIDS[@]}"; do
        wait "${LANE_PIDS[$i]}" || failed="$failed ${LANE_NAMES[$i]}"
    done
    LANE_PIDS=()
    LANE_NAMES=()
    if [ -n "$failed" ]; then
        echo "CI FAILED in lanes:$failed ($SHA; logs: $LOGS/ci_$SHA.<lane>.log)" >&2
        exit 1
    fi
}

# Coverage instruments the debug build in target/ (cargo-llvm-cov's
# show-env): the coverage lane's run is also the debug, legacy-driver pass.
cov_env() {
    eval "$(cargo llvm-cov show-env --sh 2> /dev/null)"
    export MONOIO_FORCE_LEGACY_DRIVER=1
}

build_release_tests() {
    test_executables --release --lib --tests > "$LOGS/ci_$SHA.release-tests"
}
build_coverage() (
    cargo llvm-cov clean --workspace
    cov_env
    cargo build
    test_executables --lib --tests > "$LOGS/ci_$SHA.cov-tests"
)

# Lane: release build on the io_uring driver, as production runs.
lane_release() {
    export MONOIO_FORCE_LEGACY_DRIVER=0
    step "release unit, cross-thread and e2e tests (io_uring)" \
        run_tests "$LOGS/ci_$SHA.release-tests" '/(rudis|test_[a-z_]+|e2e_[a-z_]+)-[0-9a-f]+$' 3
}

# Lane: the debug build, instrumented, on the legacy driver; then the report.
lane_coverage() {
    cov_env
    step "debug unit, cross-thread and e2e tests (legacy driver, coverage)" \
        run_tests "$LOGS/ci_$SHA.cov-tests" '/(rudis|test_[a-z_]+|e2e_[a-z_]+)-[0-9a-f]+$' 3
    step "coverage report" bash -c 'cargo llvm-cov report --lcov --output-path coverage.lcov &&
        { cargo llvm-cov report; echo; cargo llvm-cov report --text; } > coverage.txt &&
        cargo llvm-cov report && python3 scripts/command_coverage.py --details'
}

TCL_REPL="integration/replication-2,integration/replication-psync,integration/block-repl,integration/psync2,integration/psync2-pingoff,integration/psync2-master-restart,integration/failover,integration/shutdown,integration/replication-3,integration/replication-4,integration/psync2-reg"
TCL_PERSIST="unit/aofrw,unit/client-eviction,integration/aof-race,integration/convert-ziplist-hash-on-load,integration/convert-ziplist-zset-on-load,integration/rdb,integration/corrupt-dump,integration/corrupt-dump-fuzzer,unit/wait,unit/violations,integration/logging"
TCL_CLUSTER="unit/cluster/failure-marking,unit/cluster/internal-secret,unit/cluster/multi-slot-operations,unit/cluster/misc,unit/cluster/scripting,unit/cluster/sharded-pubsub,unit/cluster/slot-ownership,unit/cluster/replica-migration,unit/cluster/human-announced-nodename,unit/cluster/hostnames,unit/cluster/announced-endpoints,integration/dismiss-mem"

lane_tcl_core() {
    export RUDIS_TCL_DIR="$WT/target/tcl-core"
    step "TCL core suites" netns "SKIP_BUILD=1 bash scripts/run_redis_test_suite.sh 16379 all"
}
lane_tcl_repl() {
    export RUDIS_TCL_DIR="$WT/target/tcl-repl"
    step "TCL replication integration suites" netns "SKIP_BUILD=1 bash scripts/run_redis_integration_suite.sh $TCL_REPL"
}
lane_tcl_persist_cluster() {
    export RUDIS_TCL_DIR="$WT/target/tcl-persist"
    step "TCL persistence integration suites" netns "SKIP_BUILD=1 bash scripts/run_redis_integration_suite.sh $TCL_PERSIST"
    export RUDIS_TCL_DIR="$WT/target/tcl-cluster"
    step "TCL cluster integration suites" netns "RUDIS_THREADS=1 SKIP_BUILD=1 bash scripts/run_redis_integration_suite.sh $TCL_CLUSTER"
}

full_ci() { # rev
    SHA="$(git -C "$REPO" rev-parse --short "$1")"
    LOG="$LOGS/ci_$SHA.log"
    : > "$LOG"
    checkout "$SHA"
    cd "$WT"
    echo "CI for $SHA (log: $LOG)"
    local t0=$SECONDS
    # Lint, the quick checks that build differently, and every build the
    # lanes need, one at a time.
    step "cargo fmt --check" cargo fmt --check
    step "clippy" cargo clippy --all-targets -- -D warnings
    step "clippy (no default features)" cargo clippy --all-targets --no-default-features -- -D warnings
    step "unit tests, no default features" \
        env MONOIO_FORCE_LEGACY_DRIVER=1 cargo test --lib --no-default-features
    step "redis rdb compat tests" cargo test --test test_redis_rdb_compat
    step "cargo deny check" cargo deny check
    step "release build" cargo build --release
    step "release test build" build_release_tests
    step "coverage build" build_coverage
    # The test groups, at the same time.
    # Physical cores 0-23 and their siblings; 24-31 (CCX 3) are the
    # continuous soak's.
    lane release 0-3,32-35 lane_release
    lane coverage 4-9,36-41 lane_coverage
    lane tcl-core 10-14,42-46 lane_tcl_core
    lane tcl-repl 15-19,47-51 lane_tcl_repl
    lane tcl-persist-cluster 20-23,52-55 lane_tcl_persist_cluster
    wait_lanes
    echo "CI PASSED ($SHA, $((SECONDS - t0))s)" | tee -a "$LOG"
}

commit_checks() { # sha
    local sha="$1" short
    short="$(git -C "$REPO" rev-parse --short "$sha")"
    echo "== commit $short: $(git -C "$REPO" log -1 --format=%s "$sha")"
    checkout "$sha"
    local log="$LOGS/checks_$short.log"
    # Each command checked explicitly: `set -e` does not apply inside a
    # function whose status is tested.
    checks() {
        cd "$WT" || return 1
        cargo fmt --check || return 1
        cargo clippy --all-targets -- -D warnings || return 1
        cargo clippy --all-targets --no-default-features -- -D warnings || return 1
        MONOIO_FORCE_LEGACY_DRIVER=1 cargo test --lib || return 1
        cargo test --no-run --tests || return 1
    }
    if ! (checks) > "$log" 2>&1; then
        tail -40 "$log"
        echo "CHECKS FAILED ($short)" >&2
        exit 1
    fi
    grep -E "^test result" "$log" | head -1
    if git -C "$REPO" diff --quiet "$sha^" "$sha" -- src; then
        echo "   (no src change: perf A/B skipped)"
        return
    fi
    local perf="$LOGS/perf_$short.log"
    if ! (cd "$WT" && cargo build --release) > "$perf" 2>&1; then
        tail -20 "$perf"
        echo "PERF BUILD FAILED ($short)" >&2
        exit 1
    fi
    # Best 2 of 3: one A/B sample swings about +-5% on this host between
    # identical builds, the gate's own limit, so a single noisy sample
    # stopped series on commits that couldn't touch the measured path. A
    # clean first run is enough; after a failure, two more runs decide (a
    # real regression fails them all).
    local run pass=0 fail=0
    for run in 1 2 3; do
        if (cd "$WT" && python3 scripts/check_perf_regression.py \
            --memtier --baseline-rev "$sha^" ${CI_ALLOW_NOISY:+--allow-noisy}) \
            > "$perf.$run" 2>&1; then
            pass=$((pass + 1))
        else
            fail=$((fail + 1))
        fi
        grep -E "MT-" "$perf.$run" | sed "s/^/   run $run: /" || true
        [ "$pass" -ge 2 ] || [ "$fail" -ge 2 ] || [ "$run" = 1 -a "$pass" = 1 ] || continue
        break
    done
    if [ "$fail" -ge 2 ]; then
        tail -20 "$perf.$run"
        echo "PERF FAILED ($short): $fail of $((pass + fail)) A/B runs failed" >&2
        exit 1
    fi
}

# What a series needs, from what it changes (see agent.md, Rule 4):
# "docs" (only *.md, docs/, LICENSE: nothing can break), "workflows" (only
# those and .github/: the YAML must parse), or "full". CI_FULL=1 forces
# "full".
change_tier() { # base, tip
    local files
    files="$(git -C "$REPO" diff --name-only "$1" "$2")"
    if [ -n "${CI_FULL:-}" ]; then
        echo full
    elif ! grep -qvE '(\.md$|^docs/|^LICENSE$)' <<< "$files"; then
        echo docs
    elif ! grep -qvE '(\.md$|^docs/|^LICENSE$|^\.github/)' <<< "$files"; then
        echo workflows
    else
        echo full
    fi
}

if [ "${1:-}" = --series ] && [ $# -eq 3 ]; then
    tip_short="$(git -C "$REPO" rev-parse --short "$3")"
    case "$(change_tier "$2" "$3")" in
        docs)
            echo "== $2..$3 changes only docs: nothing to run"
            echo "CI PASSED ($tip_short, docs only)"
            exit 0
            ;;
        workflows)
            echo "== $2..$3 changes only docs and workflows: checking the workflow YAML"
            for f in $(git -C "$REPO" diff --name-only "$2" "$3" -- .github); do
                git -C "$REPO" show "$3:$f" 2> /dev/null | python3 -c 'import sys, yaml; yaml.safe_load(sys.stdin)' \
                    || { echo "CI FAILED: $f does not parse" >&2; exit 1; }
            done
            echo "CI PASSED ($tip_short, workflows only)"
            exit 0
            ;;
    esac
fi


case "${1:-}" in
    --series)
        [ $# -eq 3 ] || usage
        for sha in $(git -C "$REPO" rev-list --reverse "$2..$3"); do
            commit_checks "$sha"
        done
        full_ci "$3"
        ;;
    -h | --help | "") usage ;;
    *)
        [ $# -eq 1 ] || usage
        full_ci "$1"
        ;;
esac
