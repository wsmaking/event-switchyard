#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

OUTPUT_DIR="contracts/fixtures/strategy_intents"
if [[ $# -ge 2 && "$1" == "--output-dir" ]]; then
  OUTPUT_DIR="$2"
  shift 2
fi

mkdir -p "$OUTPUT_DIR"

cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- \
  --profile aggressive_buy \
  --output "$OUTPUT_DIR/aggressive_buy.json" \
  "$@"

cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- \
  --profile passive_sell \
  --output "$OUTPUT_DIR/passive_sell.json" \
  "$@"

cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- \
  --profile default_compare \
  --output "$OUTPUT_DIR/default_compare.json" \
  "$@"

printf 'WROTE %s\n' "$OUTPUT_DIR"
