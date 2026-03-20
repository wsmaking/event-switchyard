#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- "$@"
