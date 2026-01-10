#!/usr/bin/env bash
set -euo pipefail

# Export daily audit anchors to external storage.
# Requires AUDIT_ANCHOR_EXPORT_DIR to be set by gateway-rust.
#
# Usage:
#   AUDIT_ANCHOR_EXPORT_DIR=var/gateway/anchors \
#   ANCHOR_EXPORT_DEST="s3://my-bucket/audit/anchors" \
#   ./scripts/ops/audit_anchor_export.sh
#
# If ANCHOR_EXPORT_DEST starts with s3://, aws CLI is used.
# Otherwise the destination is treated as a local directory.

SRC_DIR="${AUDIT_ANCHOR_EXPORT_DIR:-}"
DEST="${ANCHOR_EXPORT_DEST:-}"
DRY_RUN="${DRY_RUN:-0}"

if [[ -z "${SRC_DIR}" ]]; then
  echo "AUDIT_ANCHOR_EXPORT_DIR is required." >&2
  exit 1
fi

if [[ -z "${DEST}" ]]; then
  echo "ANCHOR_EXPORT_DEST is required (s3://... or local path)." >&2
  exit 1
fi

if [[ ! -d "${SRC_DIR}" ]]; then
  echo "Source directory not found: ${SRC_DIR}" >&2
  exit 1
fi

if [[ "${DEST}" == s3://* ]]; then
  if ! command -v aws >/dev/null 2>&1; then
    echo "aws CLI is required for S3 export." >&2
    exit 1
  fi
  if [[ "${DRY_RUN}" == "1" ]]; then
    aws s3 sync "${SRC_DIR}" "${DEST}" --dryrun
  else
    aws s3 sync "${SRC_DIR}" "${DEST}"
  fi
else
  mkdir -p "${DEST}"
  if command -v rsync >/dev/null 2>&1; then
    if [[ "${DRY_RUN}" == "1" ]]; then
      rsync -avn "${SRC_DIR}/" "${DEST}/"
    else
      rsync -a "${SRC_DIR}/" "${DEST}/"
    fi
  else
    if [[ "${DRY_RUN}" == "1" ]]; then
      echo "DRY_RUN=1: would copy ${SRC_DIR}/* to ${DEST}/"
    else
      cp -a "${SRC_DIR}/." "${DEST}/"
    fi
  fi
fi
