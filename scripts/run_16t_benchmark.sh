#!/bin/bash
set -e

SERVER_BIN="/usr/local/google/home/warrenzhu/github/rudis/target/release/rudis"
MEMTIER_BIN="/usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark"
PORT=6379
THREADS=16
DURATION=60
LOG_FILE="/usr/local/google/home/warrenzhu/github/rudis/benchmark_logs/squashed_16t.log"

echo "=========================================================="
echo " Starting Rudis Server: $THREADS threads on cores 0-15"
echo "=========================================================="
taskset -c 0-15 "$SERVER_BIN" --threads "$THREADS" --port "$PORT" &
SERVER_PID=$!

sleep 2

cleanup() {
    echo "Stopping Rudis server (PID $SERVER_PID)..."
    kill -9 "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "=========================================================="
echo " Running memtier_benchmark ($DURATION seconds) on cores 32-63..."
echo "=========================================================="
taskset -c 32-63 "$MEMTIER_BIN" \
    --server 127.0.0.1 --port "$PORT" \
    --clients 1 --threads 32 --ratio 1:0 --data-size 1024 \
    --pipeline 100 --key-minimum 1 --key-maximum 1000000 \
    --key-pattern S:S \
    --print-percentiles 50,90,95,99,99.9 \
    --test-time "$DURATION" \
    --hide-histogram | tee "$LOG_FILE"

echo "=========================================================="
echo " Benchmark finished. Results saved to $LOG_FILE"
echo "=========================================================="
