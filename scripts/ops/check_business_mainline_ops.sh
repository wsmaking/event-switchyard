#!/usr/bin/env zsh
set -euo pipefail

APP_BASE_URL="${APP_BASE_URL:-http://localhost:8080}"

echo "[check] ops overview ${APP_BASE_URL}/api/ops/overview"
ops_json="$(curl -fsS "${APP_BASE_URL}/api/ops/overview")"
printf '%s' "${ops_json}" | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
errors = []

def expect(label, condition, detail):
    if not condition:
        errors.append(f"{label}:{detail}")

oms = payload.get("omsStats") or {}
backoffice = payload.get("backOfficeStats") or {}
oms_bus = payload.get("omsBusStats") or {}
backoffice_bus = payload.get("backOfficeBusStats") or {}
oms_reconcile = payload.get("omsReconcile") or {}
backoffice_reconcile = payload.get("backOfficeReconcile") or {}

expect("oms_state", oms.get("state") in {"RUNNING", "HTTP_ONLY"}, oms.get("state"))
expect("backoffice_state", backoffice.get("state") in {"RUNNING", "HTTP_ONLY"}, backoffice.get("state"))
expect("oms_pending", int(oms.get("pendingOrphanCount") or 0) == 0, oms.get("pendingOrphanCount"))
expect("backoffice_pending", int(backoffice.get("pendingOrphanCount") or 0) == 0, backoffice.get("pendingOrphanCount"))
expect("oms_dlq", int(oms.get("deadLetterCount") or 0) == 0, oms.get("deadLetterCount"))
expect("backoffice_dlq", int(backoffice.get("deadLetterCount") or 0) == 0, backoffice.get("deadLetterCount"))
expect("oms_sequence_gap", int(oms.get("sequenceGaps") or 0) == 0, oms.get("sequenceGaps"))
expect("backoffice_sequence_gap", int(backoffice.get("sequenceGaps") or 0) == 0, backoffice.get("sequenceGaps"))
expect("oms_reconcile", len(oms_reconcile.get("issues") or []) == 0, oms_reconcile.get("issues"))
expect("backoffice_reconcile", len(backoffice_reconcile.get("issues") or []) == 0, backoffice_reconcile.get("issues"))

if oms_bus:
    expect("oms_bus_state", oms_bus.get("state") in {"RUNNING", "HTTP_ONLY"}, oms_bus.get("state"))
if backoffice_bus:
    expect("backoffice_bus_state", backoffice_bus.get("state") in {"RUNNING", "HTTP_ONLY"}, backoffice_bus.get("state"))

summary = {
    "oms_state": oms.get("state"),
    "oms_bus_state": oms_bus.get("state"),
    "oms_processed": oms.get("processed"),
    "oms_sequence_gaps": oms.get("sequenceGaps"),
    "oms_pending": oms.get("pendingOrphanCount"),
    "oms_dlq": oms.get("deadLetterCount"),
    "backoffice_state": backoffice.get("state"),
    "backoffice_bus_state": backoffice_bus.get("state"),
    "backoffice_processed": backoffice.get("processed"),
    "backoffice_sequence_gaps": backoffice.get("sequenceGaps"),
    "backoffice_pending": backoffice.get("pendingOrphanCount"),
    "backoffice_dlq": backoffice.get("deadLetterCount"),
}

print(json.dumps(summary, ensure_ascii=False, indent=2))

if errors:
    print("[fail] business mainline ops checks failed", file=sys.stderr)
    for error in errors:
        print(f"  - {error}", file=sys.stderr)
    sys.exit(1)
'
