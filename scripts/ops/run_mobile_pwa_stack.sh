#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
LAN_IP="$(ifconfig en0 2>/dev/null | awk '/inet / {print $2; exit}')"

export APP_HTTP_HOST="${APP_HTTP_HOST:-0.0.0.0}"

echo "[build] frontend PWA bundle"
(
  cd "${ROOT_DIR}/frontend"
  npm run build
)

echo "[restart] Java replay services with APP_HTTP_HOST=${APP_HTTP_HOST}"
"${ROOT_DIR}/scripts/ops/stop_java_replay_stack.sh"

echo "[start] business replay stack"
"${ROOT_DIR}/scripts/ops/run_business_replay_stack.sh"

cat <<EOF
[ok] Mobile PWA stack is up.
  app-java UI    : http://localhost:8080/mobile
  app-java root  : http://localhost:8080
  gateway-rust   : http://localhost:8081
  app-java bind  : ${APP_HTTP_HOST}
  lan preview    : ${LAN_IP:+http://${LAN_IP}:8080/mobile}

iPhone:
  1. Open the LAN preview URL in Safari on the same Wi-Fi for a quick check
  2. Use Share -> Add to Home Screen
  3. For stable offline/PWA install, re-open once over HTTPS
EOF
