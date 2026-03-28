#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
EXPORT_PATH="${EXPORT_PATH:-$ROOT_DIR/build/mobile-testflight/export}"
USERNAME="${APP_STORE_CONNECT_USERNAME:-}"
APP_PASSWORD="${APP_STORE_CONNECT_APP_PASSWORD:-}"

if [[ -z "$USERNAME" || -z "$APP_PASSWORD" ]]; then
  echo "APP_STORE_CONNECT_USERNAME and APP_STORE_CONNECT_APP_PASSWORD are required"
  exit 1
fi

IPA_PATH="$(find "$EXPORT_PATH" -maxdepth 1 -name '*.ipa' | head -n 1)"
if [[ -z "$IPA_PATH" ]]; then
  echo "IPA not found under $EXPORT_PATH"
  echo "Run scripts/ops/export_mobile_testflight_ipa.sh first"
  exit 1
fi

echo "[validate] $IPA_PATH"
xcrun altool --validate-app -f "$IPA_PATH" -t ios -u "$USERNAME" -p "$APP_PASSWORD" --output-format xml

echo "[upload] $IPA_PATH"
xcrun altool --upload-app -f "$IPA_PATH" -t ios -u "$USERNAME" -p "$APP_PASSWORD" --output-format xml

echo "[ok] upload submitted"
