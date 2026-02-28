#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.."; pwd)"
DIST_DIR="${FRONTEND_DIST:-$ROOT_DIR/frontend/dist}"
BUCKET="${MINIO_BUCKET:-frontend}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
FRONTEND_BUILD="${FRONTEND_BUILD:-1}"
FRONTEND_BASE="${FRONTEND_BASE:-/}"

if [ "$FRONTEND_BUILD" = "1" ]; then
  echo "==> Building frontend (base=${FRONTEND_BASE})..."
  (cd "$ROOT_DIR/frontend" && npm run build -- --base="$FRONTEND_BASE")
fi

if [ ! -d "$DIST_DIR" ]; then
  echo "frontend/dist not found. Run: (cd frontend && npm ci && npm run build)"
  exit 1
fi

echo "==> Starting MinIO (profile: frontend)..."
docker compose --profile frontend up -d minio >/dev/null

echo "==> Uploading dist/ to MinIO bucket: $BUCKET"
MC_HOST_LOCAL="http://$MINIO_USER:$MINIO_PASSWORD@minio:9000"
docker compose --profile frontend run --rm \
  -e "MC_HOST_local=$MC_HOST_LOCAL" \
  minio-mc mb -p local/$BUCKET >/dev/null 2>&1 || true
docker compose --profile frontend run --rm \
  -e "MC_HOST_local=$MC_HOST_LOCAL" \
  minio-mc anonymous set download local/$BUCKET >/dev/null 2>&1 || true
docker compose --profile frontend run --rm \
  -e "MC_HOST_local=$MC_HOST_LOCAL" \
  -v "$DIST_DIR":/dist:ro \
  minio-mc mirror --overwrite /dist local/$BUCKET

echo "==> Done. Start Nginx with:"
echo "==>   docker compose --profile frontend up -d nginx"
echo "==> Frontend URL: http://localhost/"
