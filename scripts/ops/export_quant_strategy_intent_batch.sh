#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

OUTPUT_DIR="contracts/fixtures/strategy_intents"
MANIFEST_PATH=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --manifest)
      MANIFEST_PATH="$2"
      shift 2
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

mkdir -p "$OUTPUT_DIR"

if [[ -n "$MANIFEST_PATH" ]]; then
  while IFS=$'\t' read -r profile output_file; do
    cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- \
      --profile "$profile" \
      --output "$OUTPUT_DIR/$output_file" \
      "${EXTRA_ARGS[@]}"
  done < <(
    python3 - "$MANIFEST_PATH" <<'PY'
import json
import sys

manifest_path = sys.argv[1]
payload = json.load(open(manifest_path, "r", encoding="utf-8"))
for scenario in payload.get("scenarios", []):
    profile = scenario["profile"]
    output_file = scenario.get("outputFile", f"{profile}.json")
    print(f"{profile}\t{output_file}")
PY
  )
else
  cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- \
    --profile aggressive_buy \
    --output "$OUTPUT_DIR/aggressive_buy.json" \
    "${EXTRA_ARGS[@]}"

  cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- \
    --profile passive_sell \
    --output "$OUTPUT_DIR/passive_sell.json" \
    "${EXTRA_ARGS[@]}"

  cargo run --manifest-path gateway-rust/Cargo.toml --bin export_strategy_intent -- \
    --profile default_compare \
    --output "$OUTPUT_DIR/default_compare.json" \
    "${EXTRA_ARGS[@]}"
fi

printf 'WROTE %s\n' "$OUTPUT_DIR"
