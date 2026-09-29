#!/usr/bin/env bash
set -e

# ==============================================================================
# Rudis Comprehensive Test Coverage Generator
# Executes Unit, Cross-Thread, and E2E Integration tests under cargo-llvm-cov
# and generates summary, detailed text, LCOV, and command coverage reports.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

export PATH="$HOME/.cargo/bin:$PATH"
export MONOIO_FORCE_LEGACY_DRIVER=1

echo "============================================================"
echo " 1. Cleaning previous coverage profile artifacts"
echo "============================================================"
cargo llvm-cov clean --workspace

echo "============================================================"
echo " 2. Running Library Unit Tests under llvm-cov"
echo "============================================================"
cargo llvm-cov --no-report --lib

echo "============================================================"
echo " 3. Running Cross-Thread Tests under llvm-cov"
echo "============================================================"
cargo llvm-cov --no-report --test test_cross_thread

echo "============================================================"
echo " 4. Running End-to-End Integration Tests (serial)"
echo "============================================================"
cargo llvm-cov --no-report --test test_server_e2e -- --test-threads=1

echo "============================================================"
echo " 5. Generating Aggregated Coverage Reports"
echo "============================================================"
# LCOV format (for CI/Codecov)
cargo llvm-cov report --lcov --output-path coverage.lcov

# Detailed text and summary table
{
    cargo llvm-cov report
    echo ""
    echo "================================================================================"
    echo "                              DETAILED COVERAGE"
    echo "================================================================================"
    cargo llvm-cov report --text
} > coverage.txt

# Print Summary Table to stdout
cargo llvm-cov report

echo "============================================================"
echo " 6. Running Redis Command Coverage Analysis"
echo "============================================================"
python3 scripts/command_coverage.py --details

echo "============================================================"
echo " Coverage generation complete!"
echo " Reports generated:"
echo "   - Summary & Detailed: coverage.txt"
echo "   - LCOV format:       coverage.lcov"
echo "============================================================"
