#!/usr/bin/env bash
set -e

# ==============================================================================
# Rudis Fuzz & Adversarial Parser Test Runner
# Exercises RESP2/RESP3, JSONPath, and RediSearch query parsers against thousands
# of mutations, extreme lengths, and malformed frames.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

export PATH="$HOME/.cargo/bin:$PATH"

echo "============================================================"
echo " Running Parser Fuzz & Adversarial Test Suite"
echo "============================================================"

cargo test --test test_fuzz_parsers -- --nocapture

echo ""
echo "============================================================"
echo " All Parser Fuzz Tests Passed (Zero Panics / Zero Crashes)!"
echo "============================================================"
