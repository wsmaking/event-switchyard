#!/usr/bin/env bash
set -euo pipefail

POSTGRES_PORT="${POSTGRES_PORT:-5433}"

echo "[info] Starting postgres on port ${POSTGRES_PORT} (if not running)"
POSTGRES_PORT="${POSTGRES_PORT}" docker compose --profile gateway up -d postgres

echo "[info] Running backoffice migrations"
POSTGRES_PORT="${POSTGRES_PORT}" docker compose --profile gateway run --rm backoffice-migrate
