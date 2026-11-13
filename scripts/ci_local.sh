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

# Runs a step, logging to $LOG; on failure shows the tail and exits 1.
step() { # name, command...
    local name="$1"; shift
    printf '== %s\n' "$name" | tee -a "$LOG"
    if ! "$@" >> "$LOG" 2>&1; then
        tail -60 "$LOG"
        echo "CI FAILED at: $name ($SHA)" >&2
        exit 1
    fi
}

# Runs a shell snippet in a private network namespace (loopback only), so
# fixed test ports never clash with anything else on the host.
netns() { unshare -rn bash -c "ip link set lo up && $*"; }

full_ci() { # rev
    SHA="$(git -C "$REPO" rev-parse --short "$1")"
    LOG="$LOGS/ci_$SHA.log"
    : > "$LOG"
    checkout "$SHA"
    cd "$WT"
    echo "CI for $SHA (log: $LOG)"
    # Job build_and_test, and gate.sh's both-driver e2e passes (debug).
    step "gate.sh (no perf)" env GATE_SKIP_PERF=1 scripts/gate.sh
    step "unit tests, no default features" \
        env MONOIO_FORCE_LEGACY_DRIVER=1 cargo test --lib --no-default-features
    # Job io_uring_tests: release builds on the io_uring driver.
    step "release unit tests (io_uring)" \
        env MONOIO_FORCE_LEGACY_DRIVER=0 cargo test --release --lib
    step "release cross-thread (io_uring)" \
        env MONOIO_FORCE_LEGACY_DRIVER=0 cargo test --release --test test_cross_thread
    step "release e2e (io_uring)" env MONOIO_FORCE_LEGACY_DRIVER=0 \
        unshare -rn bash -c "ip link set lo up && cargo test --release --test 'e2e_*' -- --test-threads=1"
    # Job redis_tcl_suites.
    step "release build" cargo build --release
    step "TCL core suites" netns "SKIP_BUILD=1 bash scripts/run_redis_test_suite.sh 16379 all"
    step "TCL replication integration suites" netns "SKIP_BUILD=1 bash scripts/run_redis_integration_suite.sh integration/replication-2,integration/replication-psync,integration/block-repl,integration/psync2,integration/psync2-pingoff,integration/psync2-master-restart,integration/failover,integration/shutdown,integration/replication-3,integration/replication-4,integration/psync2-reg"
    step "TCL persistence integration suites" netns "SKIP_BUILD=1 bash scripts/run_redis_integration_suite.sh integration/aof-race,integration/convert-ziplist-hash-on-load,integration/convert-ziplist-zset-on-load,integration/rdb,integration/corrupt-dump,integration/corrupt-dump-fuzzer,unit/wait,unit/violations,integration/logging"
    step "TCL cluster integration suites" netns "RUDIS_THREADS=1 SKIP_BUILD=1 bash scripts/run_redis_integration_suite.sh unit/cluster/failure-marking,unit/cluster/internal-secret,unit/cluster/multi-slot-operations,integration/dismiss-mem"
    # Job supply_chain.
    step "cargo deny check" cargo deny check
    # The coverage step of build_and_test re-runs the tests instrumented.
    step "coverage" netns "./scripts/generate_coverage.sh"
    echo "CI PASSED ($SHA)" | tee -a "$LOG"
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
    if ! (cd "$WT" && cargo build --release && python3 scripts/check_perf_regression.py \
        --memtier --baseline-rev "$sha^") > "$perf" 2>&1; then
        tail -20 "$perf"
        echo "PERF FAILED ($short): an INCONCLUSIVE verdict means the host was noisy; rerun when quiet" >&2
        exit 1
    fi
    grep -E "MT-" "$perf" || true
}

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
