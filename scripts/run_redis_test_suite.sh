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

# Ensure binary is built
echo "Building Rudis release binary..."
~/.cargo/bin/cargo build --release

# Clean log
TEST_LOG="/tmp/rudis_tcl_test_${PORT}.log"
rm -f "$TEST_LOG"

# Start rudis server in background
echo "Starting Rudis server on port $PORT..."
./target/release/rudis --port "$PORT" --threads 2 --no-pin > "$TEST_LOG" 2>&1 &
RUDIS_PID=$!

cleanup() {
    echo "Stopping Rudis server (PID $RUDIS_PID)..."
    kill -9 "$RUDIS_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to bind
sleep 1
if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null && ! timeout 1 bash -c "</dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
    sleep 1
fi

CORE_SUITES=(
    "unit/type/string"
    "unit/type/hash"
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
    if tclsh test_helper.tcl \
        --host 127.0.0.1 \
        --port "$PORT" \
        --singledb \
        --ignore-encoding \
        --ignore-digest \
        --tags "-needs:repl -needs:debug -needs:config-rewrite -needs:pfdebug" \
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
