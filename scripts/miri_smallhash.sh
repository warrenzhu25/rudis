#!/usr/bin/env bash
# Runs src/smallhash.rs's unit tests under Miri, which checks its unsafe
# single-allocation code for undefined behaviour (out-of-bounds access,
# use-after-free, wrong dealloc layout, misaligned or uninitialized reads,
# leaks).
#
# Miri cannot run the whole crate (jemalloc is reached through FFI), so the
# module is copied into a throwaway crate with its one table dependency
# (`to_map`) stubbed, and the long randomized tests are shortened (Miri runs
# ~1000x slower). Takes ~5 minutes.
#
# Needs: rustup component add --toolchain nightly miri rust-src
# Run after any change to src/smallhash.rs.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d /tmp/miri_smallhash.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/src"
cat > "$WORK/Cargo.toml" <<'EOF'
[package]
name = "miri_smallhash"
version = "0.1.0"
edition = "2024"
[dependencies]
bytes = "1"
EOF
python3 - "$REPO/src/smallhash.rs" "$WORK/src/lib.rs" <<'EOF'
import re, sys
s = open(sys.argv[1]).read()
# Replace the table-map conversion with a std HashMap stand-in.
start = s.index("    /// The pairs as a full hash map")
end = s.index("\n    }\n", start) + len("\n    }\n")
s = s[:start] + """    /// Miri stand-in for the table's map conversion.
    pub fn to_map(&self) -> std::collections::HashMap<Vec<u8>, Vec<u8>> {
        self.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
    }
""" + s[end:]
# Shorten the randomized loops.
s = re.sub(r"for step in 0\.\.[0-9_]+ \{", "for step in 0..3_000 {", s)
s = re.sub(r"for round in 0\.\.[0-9_]+u64 \{", "for round in 0..400u64 {", s)
open(sys.argv[2], "w").write("#![allow(dead_code)]\n" + s)
EOF
cd "$WORK"
if cargo +nightly miri test > miri.log 2>&1; then
    grep -E "^test |test result" miri.log
    echo "miri: smallhash OK"
else
    tail -60 miri.log
    echo "miri: smallhash FAILED" >&2
    exit 1
fi
