#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_ROOT="$ROOT_DIR/build/mobile-testflight"
ARCHIVE_PATH="${ARCHIVE_PATH:-$BUILD_ROOT/archive/MobileLearningShell.xcarchive}"
EXPORT_PATH="${EXPORT_PATH:-$BUILD_ROOT/export}"
EXPORT_OPTIONS_PLIST="$BUILD_ROOT/ExportOptions-AppStore.plist"
TEAM_ID="${APP_DEVELOPMENT_TEAM:-}"

if [[ -z "$TEAM_ID" ]]; then
  echo "APP_DEVELOPMENT_TEAM is required"
  exit 1
fi

if [[ ! -d "$ARCHIVE_PATH" ]]; then
  echo "Archive not found: $ARCHIVE_PATH"
  echo "Run scripts/ops/archive_mobile_testflight.sh first"
  exit 1
fi

mkdir -p "$BUILD_ROOT" "$EXPORT_PATH"

cat > "$EXPORT_OPTIONS_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "https://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>destination</key>
  <string>export</string>
  <key>method</key>
  <string>app-store</string>
  <key>signingStyle</key>
  <string>automatic</string>
  <key>stripSwiftSymbols</key>
  <true/>
  <key>teamID</key>
  <string>${TEAM_ID}</string>
  <key>uploadSymbols</key>
  <true/>
</dict>
</plist>
EOF

echo "[export] ipa"

xcodebuild \
  -exportArchive \
  -archivePath "$ARCHIVE_PATH" \
  -exportPath "$EXPORT_PATH" \
  -exportOptionsPlist "$EXPORT_OPTIONS_PLIST" \
  -allowProvisioningUpdates

echo "[ok] ipa ready"
echo "  export: $EXPORT_PATH"
