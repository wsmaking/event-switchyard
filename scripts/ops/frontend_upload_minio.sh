#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.."; pwd)"
DIST_DIR="${FRONTEND_DIST:-$ROOT_DIR/frontend/dist}"
BUCKET="${MINIO_BUCKET:-frontend}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"

if [ ! -d "$DIST_DIR" ]; then
  echo "frontend/dist not found. Run: (cd frontend && npm ci && npm run build)"
  exit 1
fi

echo "==> Starting MinIO (profile: frontend)..."
docker compose --profile frontend up -d minio >/dev/null

echo "==> Uploading dist/ to MinIO bucket: $BUCKET"
docker compose --profile frontend run --rm \
  -e MINIO_ROOT_USER="$MINIO_USER" \
  -e MINIO_ROOT_PASSWORD="$MINIO_PASSWORD" \
  -v "$DIST_DIR":/dist:ro \
  minio-mc \
  sh -c "mc alias set local http://minio:9000 \$MINIO_ROOT_USER \$MINIO_ROOT_PASSWORD >/dev/null && mc mb -p local/$BUCKET >/dev/null 2>&1 || true; mc mirror --overwrite /dist local/$BUCKET"

echo "==> Done. MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
