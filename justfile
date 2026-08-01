# PlayOnAir workspace tasks

set shell := ["bash", "-euo", "pipefail", "-c"]

root := justfile_directory()

# Format all crates.
fmt:
    cd "{{root}}" && cargo fmt --all

# Check formatting without writing.
fmt-check:
    cd "{{root}}" && cargo fmt --all -- --check

# Clippy with workspace lints + -D warnings.
lint:
    cd "{{root}}" && cargo clippy --workspace --all-targets --all-features -- -D warnings

# Unit + integration tests (prefer nextest when installed).
test:
    #!/usr/bin/env bash
    set -euo pipefail
    cd "{{root}}"
    if command -v cargo-nextest >/dev/null 2>&1; then
      TZ=UTC cargo nextest run --workspace --all-features
    else
      TZ=UTC cargo test --workspace --all-features
    fi

# Supply-chain license / advisory / ban checks.
deny:
    cd "{{root}}" && cargo deny check

# RustSec advisory database.
audit:
    cd "{{root}}" && cargo audit

# Unused dependency detection.
machete:
    cd "{{root}}" && cargo machete --with-metadata

# Full quality gate.
check: fmt-check lint test deny audit machete

# Run the binary (optional args after `--`).
run *args:
    cd "{{root}}" && cargo run -p play-on-air -- {{args}}

# Debug build of the binary.
build:
    cd "{{root}}" && cargo build -p play-on-air

# Release build of the binary.
build-release:
    cd "{{root}}" && cargo build -p play-on-air --release
