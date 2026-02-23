#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MANIFEST_PATH="${MANIFEST_PATH:-$ROOT_DIR/gateway-rust/Cargo.toml}"

# Perf実測時にビルド条件を固定する。
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS="${CARGO_PROFILE_RELEASE_CODEGEN_UNITS:-1}"
export CARGO_PROFILE_RELEASE_LTO="${CARGO_PROFILE_RELEASE_LTO:-fat}"
if [[ "$(uname -s)" == "Linux" ]]; then
  export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}"
fi

cd "$ROOT_DIR"
cargo build --release --bin gateway-rust --manifest-path "$MANIFEST_PATH"
