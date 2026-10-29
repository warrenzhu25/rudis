#!/usr/bin/env bash
# The full pre-push quality gate (agent.md Rule 4), in one place so no step's
# failure can be hidden by a pipe: every command runs under `set -euo
# pipefail`, and output is filtered only after a step's exit code is checked.
#
#   scripts/gate.sh            # full gate, perf A/B vs HEAD
#   GATE_SKIP_PERF=1 scripts/gate.sh   # everything but the perf A/B
#   GATE_SKIP_URING=1 ...              # skip the E2E pass on io_uring
#
# The Miri checks run when their module differs from HEAD (staged or not).
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source "$HOME/.cargo/env"
LOG="$(mktemp /tmp/gate.XXXXXX)"
trap 'rm -f "$LOG"' EXIT

step() { # name, command...
    local name="$1"; shift
    printf '== %s\n' "$name"
    if ! "$@" > "$LOG" 2>&1; then
        tail -40 "$LOG"
        echo "GATE FAILED at: $name" >&2
        exit 1
    fi
    grep -E "^test result|GATE|MT-|  - |\[note\]|miri:" "$LOG" || true
}

step "cargo fmt --check" cargo fmt --check
step "clippy" cargo clippy --all-targets -- -D warnings
step "clippy (no default features)" cargo clippy --all-targets --no-default-features -- -D warnings
step "unit tests" cargo test --lib
step "cross-thread tests" cargo test --test test_cross_thread
step "redis rdb compat tests" cargo test --test test_redis_rdb_compat
step "e2e tests (serial, private netns)" unshare -rn bash -c \
    'ip link set lo up && cargo test --test test_server_e2e -- --test-threads=1'
# Again on the io_uring driver production runs: the pass above runs its
# servers on monoio's legacy (epoll) driver, which hid io_uring-only bugs
# (a full sync over 2 GiB failed with EINVAL on io_uring only).
if [ -z "${GATE_SKIP_URING:-}" ]; then
    step "e2e tests on io_uring (serial, private netns)" env MONOIO_FORCE_LEGACY_DRIVER=0 \
        unshare -rn bash -c \
        'ip link set lo up && cargo test --test test_server_e2e -- --test-threads=1'
fi
changed="$(git diff --name-only HEAD)"
if grep -qx "src/smallhash.rs" <<<"$changed"; then
    step "miri smallhash" scripts/miri_smallhash.sh
fi
if grep -qx "src/compact.rs" <<<"$changed"; then
    step "miri compact" scripts/miri_compact.sh
fi
step "release build" cargo build --release
if [ -z "${GATE_SKIP_PERF:-}" ]; then
    step "perf A/B vs HEAD" python3 scripts/check_perf_regression.py --memtier --baseline-rev HEAD
fi
echo "GATE PASSED"
