#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

"$ROOT_DIR/scripts/ops/prepare_mobile_ios_shell.sh"

echo "[build] iOS shell for simulator"
xcodebuild \
  -project "$ROOT_DIR/ios/MobileLearningShell.xcodeproj" \
  -scheme MobileLearningShell \
  -sdk iphonesimulator \
  -destination 'generic/platform=iOS Simulator' \
  CODE_SIGNING_ALLOWED=NO \
  build
