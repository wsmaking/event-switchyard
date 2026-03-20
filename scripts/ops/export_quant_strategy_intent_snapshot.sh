#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

SNAPSHOT_TS="${SNAPSHOT_TS:-$(date +%Y%m%d_%H%M%S)}"
OUTPUT_ROOT="${OUTPUT_ROOT:-var/quant_strategy_intents}"
OUTPUT_DIR="$OUTPUT_ROOT/$SNAPSHOT_TS"

mkdir -p "$OUTPUT_DIR"

"$ROOT/scripts/ops/export_quant_strategy_intent_batch.sh" --output-dir "$OUTPUT_DIR" "$@"

cat > "$OUTPUT_DIR/manifest.json" <<MANIFEST
{
  "snapshotTs": "$SNAPSHOT_TS",
  "outputRoot": "$OUTPUT_ROOT",
  "outputDir": "$OUTPUT_DIR",
  "profiles": [
    "aggressive_buy",
    "passive_sell",
    "default_compare"
  ]
}
MANIFEST

printf 'WROTE %s\n' "$OUTPUT_DIR/manifest.json"
