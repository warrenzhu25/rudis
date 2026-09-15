#!/usr/bin/env bash
set -e

PORT=${1:-16379}
SUITE=${2:-unit/type/string}
CLIENTS=${3:-1}

echo "============================================================"
echo " Starting Rudis on port $PORT for Redis TCL Test Suite"
echo " Target Suite: $SUITE (Clients: $CLIENTS)"
echo "============================================================"

# Ensure binary is built
~/.cargo/bin/cargo build --release

# Start rudis server in background
./target/release/rudis --port "$PORT" --threads 2 --no-pin > /tmp/rudis_test.log 2>&1 &
RUDIS_PID=$!

cleanup() {
    echo "Stopping Rudis server (PID $RUDIS_PID)..."
    kill -9 "$RUDIS_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to bind
sleep 1

cd tests/redis-tests
tclsh test_helper.tcl \
    --host 127.0.0.1 \
    --port "$PORT" \
    --singledb \
    --ignore-encoding \
    --ignore-digest \
    --tags "-needs:repl" \
    --clients "$CLIENTS" \
    --single "$SUITE"
