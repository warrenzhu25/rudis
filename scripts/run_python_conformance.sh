#!/usr/bin/env bash
set -e

# ==============================================================================
# Rudis Dragonfly-Style Python Integration Suite Runner
# Spawns an isolated Rudis instance and runs the Python conformance suite.
# ==============================================================================

PORT=${1:-16420}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Ensure binary exists
if [ ! -f "./target/release/rudis" ]; then
    echo "Building release binary..."
    ~/.cargo/bin/cargo build --release
fi

echo "============================================================"
echo " Starting Rudis on Port $PORT for Python Conformance Suite"
echo "============================================================"

TEST_LOG="/tmp/rudis_py_conformance_${PORT}.log"
rm -f "$TEST_LOG"

./target/release/rudis --port "$PORT" --threads 2 --no-pin > "$TEST_LOG" 2>&1 &
RUDIS_PID=$!

cleanup() {
    echo "Stopping Rudis server (PID $RUDIS_PID)..."
    kill -9 "$RUDIS_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to bind
sleep 1
for _ in {1..10}; do
    if timeout 1 bash -c "</dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
        break
    fi
    sleep 0.2
done

export RUDIS_TEST_HOST="127.0.0.1"
export RUDIS_TEST_PORT="$PORT"

echo "Executing Python Conformance Tests..."
python3 -m unittest discover -s tests/python -p "test_*.py" -v

echo ""
echo "============================================================"
echo " All Python Conformance Tests Passed Successfully!"
echo "============================================================"
