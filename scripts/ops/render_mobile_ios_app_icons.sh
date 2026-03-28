#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SOURCE_ICON="$ROOT_DIR/frontend/public/app-icon-512.png"
TARGET_DIR="$ROOT_DIR/ios/MobileLearningShell/Assets.xcassets/AppIcon.appiconset"

mkdir -p "$TARGET_DIR"

sips -z 120 120 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-120.png" >/dev/null
sips -z 180 180 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-180.png" >/dev/null
sips -z 20 20 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-20.png" >/dev/null
sips -z 29 29 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-29.png" >/dev/null
sips -z 40 40 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-40.png" >/dev/null
sips -z 58 58 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-58.png" >/dev/null
sips -z 60 60 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-60.png" >/dev/null
sips -z 76 76 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-76.png" >/dev/null
sips -z 80 80 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-80.png" >/dev/null
sips -z 87 87 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-87.png" >/dev/null
sips -z 152 152 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-152.png" >/dev/null
sips -z 167 167 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-167.png" >/dev/null
sips -z 1024 1024 "$SOURCE_ICON" --out "$TARGET_DIR/app-icon-1024.png" >/dev/null

echo "[ok] rendered iOS app icons"
echo "  target: $TARGET_DIR"
