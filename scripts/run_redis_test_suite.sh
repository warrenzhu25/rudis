#!/usr/bin/env bash
set -e

# ==============================================================================
# Rudis Official Redis/Valkey TCL Test Suite Runner
# Supports running individual suites or all core data structure suites.
# ==============================================================================

PORT=${1:-16379}
SUITE_ARG=${2:-all-types}
CLIENTS=${3:-1}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Ensure binary is built (SKIP_BUILD=1 reuses target/release/rudis).
if [ "${SKIP_BUILD:-0}" != "1" ]; then
    echo "Building Rudis release binary..."
    CARGO=$(command -v cargo || echo ~/.cargo/bin/cargo)
    "$CARGO" build --release
fi

TEST_LOG="/tmp/rudis_tcl_test_${PORT}.log"
SERVER_DIR="/tmp/rudis_tcl_server_${PORT}"
rm -f "$TEST_LOG"
killall -9 rudis 2>/dev/null || true
sleep 0.5

RUDIS_PID=""
stop_server() {
    if [ -n "$RUDIS_PID" ]; then
        kill -9 "$RUDIS_PID" 2>/dev/null || true
        wait "$RUDIS_PID" 2>/dev/null || true
        RUDIS_PID=""
    fi
}
trap stop_server EXIT

# Each suite gets a fresh server, so one suite's state or crash cannot fail
# the next. unit/scripting and unit/functions run cross-key Lua that needs
# a single shard; the rest use 2 (override with the 4th argument).
start_server() {
    local suite=$1
    local threads=${THREADS_ARG:-}
    if [ -z "$threads" ]; then
        if [ "$suite" = "unit/scripting" ] || [ "$suite" = "unit/functions" ]; then
            threads=1
        else
            threads=2
        fi
    fi
    echo "Starting Rudis server on port $PORT (threads: $threads)..."
    # The previous suite's server (killed with -9) may still hold the port
    # for a moment; starting now would fail with "Address already in use".
    for _ in $(seq 1 100); do
        if ! timeout 1 bash -c "</dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    # Run in a scratch dir: suites SAVE, and a dump.rdb left in the repo
    # would be loaded by every server later started there (e.g. e2e tests).
    rm -rf "$SERVER_DIR" && mkdir -p "$SERVER_DIR"
    (cd "$SERVER_DIR" && exec "$REPO_ROOT/target/release/rudis" --port "$PORT" --threads "$threads" --no-pin) >> "$TEST_LOG" 2>&1 &
    RUDIS_PID=$!
    for _ in $(seq 1 50); do
        if timeout 1 bash -c "</dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "Rudis did not start on port $PORT"
    return 1
}
THREADS_ARG=${4:-}

CORE_SUITES=(
    "unit/type/string"
    "unit/type/hash"
    "unit/type/hash-field-expire"
    "unit/type/list"
    "unit/type/list-2"
    "unit/type/list-3"
    "unit/type/list-4"
    "unit/type/set"
    "unit/type/zset"
    "unit/expire"
    "unit/keyspace"
    "unit/type/incr"
    "unit/type/increx"
    "unit/bitops"
    "unit/bitfield"
    "unit/scan"
    "unit/type/stream"
    "unit/type/stream-cgroups"
    "unit/dump"
    "unit/pubsub"
    "unit/multi"
    "unit/quit"
    "unit/protocol"
    "unit/other"
    "unit/hyperloglog"
    "unit/sort"
    "unit/geo"
    "unit/auth"
    "unit/printver"
    "unit/limits"
    "unit/scripting"
    "unit/functions"
    "unit/pubsubshard"
    "unit/slowlog"
    "unit/latency-monitor"
    "unit/lazyfree"
    "unit/pause"
    "unit/info-command"
    "unit/introspection-2"
    "unit/obuf-limits"
    "unit/replybufsize"
    "unit/querybuf"
    "unit/tracking"
    "unit/acl-v2"
    "unit/acl"
    "unit/info"
    "unit/introspection"
    "unit/maxmemory"
    "unit/gcra"
)

if [ "$SUITE_ARG" = "all-types" ] || [ "$SUITE_ARG" = "all" ]; then
    TARGET_SUITES=("${CORE_SUITES[@]}")
else
    IFS=',' read -ra TARGET_SUITES <<< "$SUITE_ARG"
fi

echo "============================================================"
echo " Running Redis TCL Test Suite on Port $PORT (Clients: $CLIENTS)"
echo " Selected Suites: ${TARGET_SUITES[*]}"
echo "============================================================"

cd tests/redis-tests
FAILED_SUITES=()
PASSED_SUITES=()

for suite in "${TARGET_SUITES[@]}"; do
    echo ""
    echo ">>> Running TCL Suite: $suite ..."
    stop_server
    start_server "$suite" || { FAILED_SUITES+=("$suite"); continue; }
    if tclsh test_helper.tcl \
        --host 127.0.0.1 \
        --port "$PORT" \
        --singledb \
        --ignore-encoding \
        --ignore-digest \
        --tags "-needs:repl -needs:debug -needs:config-rewrite -needs:pfdebug -needs:config-maxmemory" \
        --skiptest "/.*script timeout.*" \
        --skiptest "/.*test function kill$" \
        --skiptest "/.*kill not working on.*" \
        --skiptest "/.*Subkey notifications.*" \
        --skiptest "/.*Timedout.*" \
        --skiptest "/.*OOM.*" \
        --clients "$CLIENTS" \
        --single "$suite"; then
        PASSED_SUITES+=("$suite")
    else
        echo "!!! Suite $suite failed!"
        FAILED_SUITES+=("$suite")
    fi
done

echo ""
echo "============================================================"
echo " Redis TCL Test Execution Summary"
echo "============================================================"
echo " Passed: ${#PASSED_SUITES[@]} (${PASSED_SUITES[*]})"
if [ ${#FAILED_SUITES[@]} -gt 0 ]; then
    echo " Failed: ${#FAILED_SUITES[@]} (${FAILED_SUITES[*]})"
    exit 1
else
    echo " All selected Redis TCL suites passed successfully!"
fi
