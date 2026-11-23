# Contributing to Rudis

Thanks for your interest! Bug reports, compatibility gaps and pull requests are all welcome.

## Reporting a bug

Open an issue with:

- what you ran (commands, or a minimal client snippet), what you expected and what happened;
- the Rudis commit (`rudis --version` or `git rev-parse HEAD`), the kernel (`uname -r`) and
  the flags or config file you started Rudis with;
- for a difference from Redis, what Redis (or Valkey) replies to the same commands.

## Building and testing

Rudis builds on Linux with the toolchain pinned in `rust-toolchain.toml`:

```bash
cargo build --release
cargo test --lib                                      # unit tests
cargo test --test 'e2e_*' -- --test-threads=1         # end-to-end tests (start real servers)
```

Before opening a pull request, also run:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

The Redis/Valkey TCL suites run with `scripts/run_redis_test_suite.sh` (core suites) and
`scripts/run_redis_integration_suite.sh <suite>` (suites that start their own servers); they
need `tclsh`. `scripts/ci_local.sh` runs everything CI runs, including the performance gate.

## Pull requests

- One logical change per commit, with a conventional header:
  `fix(scope): ...`, `feat(scope): ...`, `perf(scope): ...`, `test(scope): ...`,
  `docs(scope): ...`, `chore(scope): ...`. The body explains why.
- Every commit builds, and passes `fmt`, `clippy` and the tests.
- A bug fix comes with a test that fails without it.
- Changes to the request path need a before/after benchmark
  (`scripts/check_perf_regression.py`).
- Read [`agent.md`](agent.md) for the engineering invariants (thread-per-core ownership, no
  locks on the hot path, and so on) before changing the core.

## License

By contributing you agree that your contributions are licensed under the [MIT License](LICENSE).
