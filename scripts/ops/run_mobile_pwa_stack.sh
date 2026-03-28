#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

echo "[build] frontend PWA bundle"
(
  cd "${ROOT_DIR}/frontend"
  npm run build
)

echo "[start] business replay stack"
"${ROOT_DIR}/scripts/ops/run_business_replay_stack.sh"

cat <<EOF
[ok] Mobile PWA stack is up.
  app-java UI    : http://localhost:8080/mobile
  app-java root  : http://localhost:8080
  gateway-rust   : http://localhost:8081

iPhone:
  1. Open the app-java URL in Safari over HTTPS or a trusted private network path
  2. Use Share -> Add to Home Screen
EOF
