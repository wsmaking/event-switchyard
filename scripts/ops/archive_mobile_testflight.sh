#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT_PATH="$ROOT_DIR/ios/MobileLearningShell.xcodeproj"
SCHEME="MobileLearningShell"
BUILD_ROOT="$ROOT_DIR/build/mobile-testflight"
ARCHIVE_PATH="$BUILD_ROOT/archive/MobileLearningShell.xcarchive"
DERIVED_DATA_PATH="$BUILD_ROOT/derived-data"

TEAM_ID="${APP_DEVELOPMENT_TEAM:-}"
if [[ -z "$TEAM_ID" ]]; then
  echo "APP_DEVELOPMENT_TEAM is required"
  exit 1
fi

XCODE_MAJOR="$(xcodebuild -version | awk '/Xcode/ {split($2, parts, "."); print parts[1]}')"
if [[ -z "$XCODE_MAJOR" || "$XCODE_MAJOR" -lt 16 ]]; then
  echo "Xcode 16 or later is required for TestFlight uploads"
  echo "Current: $(xcodebuild -version | head -n 1)"
  exit 1
fi

BUNDLE_ID="${APP_BUNDLE_IDENTIFIER:-io.wsmaking.eventswitchyard.mobilelearning}"
DISPLAY_NAME="${APP_DISPLAY_NAME:-Switchyard Mobile}"
MARKETING_VERSION="${APP_MARKETING_VERSION:-1.0.0}"
BUILD_NUMBER="${APP_BUILD_NUMBER:-$(date +%Y%m%d%H%M)}"

"$ROOT_DIR/scripts/ops/prepare_mobile_ios_shell.sh"

rm -rf "$ARCHIVE_PATH"
mkdir -p "$(dirname "$ARCHIVE_PATH")" "$DERIVED_DATA_PATH"

echo "[archive] MobileLearningShell"
echo "  team     : $TEAM_ID"
echo "  bundle   : $BUNDLE_ID"
echo "  version  : $MARKETING_VERSION ($BUILD_NUMBER)"

xcodebuild \
  -project "$PROJECT_PATH" \
  -scheme "$SCHEME" \
  -configuration Release \
  -destination "generic/platform=iOS" \
  -archivePath "$ARCHIVE_PATH" \
  -derivedDataPath "$DERIVED_DATA_PATH" \
  DEVELOPMENT_TEAM="$TEAM_ID" \
  PRODUCT_BUNDLE_IDENTIFIER="$BUNDLE_ID" \
  INFOPLIST_KEY_CFBundleDisplayName="$DISPLAY_NAME" \
  MARKETING_VERSION="$MARKETING_VERSION" \
  CURRENT_PROJECT_VERSION="$BUILD_NUMBER" \
  -allowProvisioningUpdates \
  archive

echo "[ok] archive ready"
echo "  archive: $ARCHIVE_PATH"
