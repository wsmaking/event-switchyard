#!/usr/bin/env zsh
set -euo pipefail

wait_health() {
  local name="$1"
  local url="$2"
  local attempts="${3:-20}"

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "[ready] ${name}"
      return 0
    fi
    sleep 1
  done

  echo "[fail] ${name} is not healthy: ${url}" >&2
  exit 1
}

wait_health "app-java" "http://localhost:8080/health"

echo "[check] mobile shell"
curl -fsS "http://localhost:8080/mobile" | rg -q "Event Switchyard|Mobile Learning"
echo "[ok] /mobile served"

echo "[check] manifest"
curl -fsS "http://localhost:8080/manifest.webmanifest" | rg -q "\"display\": \"standalone\""
echo "[ok] manifest served"

echo "[check] service worker"
curl -fsS "http://localhost:8080/sw.js" | rg -q "switchyard-pwa-v2"
echo "[ok] service worker served"
