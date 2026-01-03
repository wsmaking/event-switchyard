#!/usr/bin/env bash
set -euo pipefail

TRANSPORT="${1:-sim}"
RUNS="${RUNS:-2000}"
ENDPOINT="${APP_ORDER_URL:-http://localhost:8080/api/orders}"
METRICS_URL="${GATEWAY_METRICS_URL:-http://localhost:8081/metrics}"

echo "==> Exchange transport smoke"
echo "  Transport: ${TRANSPORT}"
echo "  Endpoint:  ${ENDPOINT}"
echo "  Metrics:   ${METRICS_URL}"
echo "  Runs:      ${RUNS}"
echo
echo "  NOTE: Gateway must already be running with EXCHANGE_TRANSPORT=${TRANSPORT}"
echo

python3 scripts/bench_orders.py --runs "${RUNS}" --endpoint "${ENDPOINT}" --out "results/order_api_bench_${TRANSPORT}.json"

echo
echo "==> Gateway latency metrics"
curl -fsS "${METRICS_URL}" | rg "gateway_accept_to_execution_report_latency_ms_(max|sum|count)" || true
