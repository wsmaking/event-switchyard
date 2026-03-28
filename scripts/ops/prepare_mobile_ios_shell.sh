#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
WEBAPP_DIR="$ROOT_DIR/ios/MobileLearningShell/WebApp"

echo "[icons] iOS app icons"
"$ROOT_DIR/scripts/ops/render_mobile_ios_app_icons.sh"

echo "[build] frontend mobile bundle"
(
  cd "$ROOT_DIR/frontend"
  VITE_PUBLIC_BASE=./ npm run build
)

echo "[sync] iOS WebApp resources"
mkdir -p "$WEBAPP_DIR"
rsync -a --delete --exclude '.gitignore' "$ROOT_DIR/frontend/dist/" "$WEBAPP_DIR/"
perl -0pi -e 's/href="\/([^"]+)"/href=".\/$1"/g; s/src="\/([^"]+)"/src=".\/$1"/g' "$WEBAPP_DIR/index.html"

echo "[ok] iOS shell resources prepared"
echo "  xcodeproj : $ROOT_DIR/ios/MobileLearningShell.xcodeproj"
echo "  web bundle: $WEBAPP_DIR"
