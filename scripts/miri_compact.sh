#!/usr/bin/env bash
# Runs src/compact.rs's unit tests under Miri, which checks its unsafe code
# (the leaked `Box<[u8]>` behind `CompactStr::Heap` and `HeapPacked`, and
# `CompactKey`'s) for undefined behaviour: out-of-bounds access,
# use-after-free, wrong dealloc layouts, uninitialized reads, leaks.
#
# Miri cannot run the whole crate (jemalloc is reached through FFI), so the
# module is copied into a throwaway crate with its one outside dependency
# (`connection::write_resp_bulk`) provided by a small stand-in. Under Miri
# the BMI2 path is not taken (no CPU feature detection); the portable
# packer it must match byte for byte is.
#
# Needs: rustup component add --toolchain nightly miri rust-src
# Run after any change to src/compact.rs.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d /tmp/miri_compact.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/src"
cat > "$WORK/Cargo.toml" <<'EOF'
[package]
name = "miri_compact"
version = "0.1.0"
edition = "2024"
[dependencies]
bytes = "1"
EOF
{
    echo '#![allow(dead_code)]'
    echo 'mod compact;'
    echo 'pub mod connection {'
    echo '    /// Stand-in for the server writer: `$len\r\n<val>\r\n`.'
    echo '    pub fn write_resp_bulk(out: &mut Vec<u8>, val: &[u8]) {'
    echo '        out.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());'
    echo '        out.extend_from_slice(val);'
    echo '        out.extend_from_slice(b"\r\n");'
    echo '    }'
    echo '}'
} > "$WORK/src/lib.rs"
cp "$REPO/src/compact.rs" "$WORK/src/compact.rs"
cd "$WORK"
if cargo +nightly miri test > miri.log 2>&1; then
    grep -E "^test |test result" miri.log
    echo "miri: compact OK"
else
    tail -60 miri.log
    echo "miri: compact FAILED" >&2
    exit 1
fi
