#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

DOCKER_CONTEXT="${DOCKER_CONTEXT:-default}"
IMAGE="${IMAGE:-event-switchyard-gateway-rust:latest}"
RUNS="${RUNS:-3}"
DURATION="${DURATION:-30}"
TARGET_RPS="${TARGET_RPS:-10000}"
WORKERS="${WORKERS:-192}"
ACCOUNTS="${ACCOUNTS:-24}"
QUEUE_CAPACITY="${QUEUE_CAPACITY:-200000}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-30}"
OUT_BASE="${OUT_BASE:-var/results}"

STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${OUT_BASE}/linux_lane_verify_${STAMP}"
mkdir -p "$RUN_DIR"

RESULTS_TSV="$RUN_DIR/results.tsv"
echo -e "case\trun\tcompleted_rps\taccepted_rate\tack_accepted_p99_us\tack_accepted_p999_us\tdurable_confirm_p99_us\tdurable_confirm_p999_us\tload_path\tmetrics_path" >"$RESULTS_TSV"

case_specs=(
  "linux_8lane_w30_15:8:30:15"
  "linux_16lane_w30_15:16:30:15"
)

for spec in "${case_specs[@]}"; do
  case_name="${spec%%:*}"
  rest="${spec#*:}"
  shards="${rest%%:*}"
  rest2="${rest#*:}"
  wait_us="${rest2%%:*}"
  wait_min_us="${rest2#*:}"

  for run in $(seq 1 "$RUNS"); do
    container_name="gw_${case_name}_${run}"
    port=$((29110 + run))
    load_out="$RUN_DIR/${case_name}_run${run}.load.txt"
    metrics_out="$RUN_DIR/${case_name}_run${run}.metrics.prom"

    docker --context "$DOCKER_CONTEXT" rm -f "$container_name" >/dev/null 2>&1 || true
    docker --context "$DOCKER_CONTEXT" run -d --rm \
      --name "$container_name" \
      -p "${port}:8081" \
      -e GATEWAY_PORT=8081 \
      -e GATEWAY_TCP_PORT=0 \
      -e JWT_HS256_SECRET=secret123 \
      -e KAFKA_ENABLE=0 \
      -e FASTPATH_DRAIN_ENABLE=1 \
      -e FASTPATH_DRAIN_WORKERS=4 \
      -e V3_HTTP_ENABLE=true \
      -e V3_HTTP_INGRESS_ENABLE=true \
      -e V3_HTTP_CONFIRM_ENABLE=true \
      -e V3_TCP_ENABLE=false \
      -e V3_DURABLE_CONTROL_PRESET=hft_stable \
      -e V3_SOFT_REJECT_PCT=85 \
      -e V3_HARD_REJECT_PCT=92 \
      -e V3_KILL_REJECT_PCT=98 \
      -e V3_KILL_AUTO_RECOVER=false \
      -e V3_SESSION_LOSS_SUSPECT_THRESHOLD=4096 \
      -e V3_SHARD_LOSS_SUSPECT_THRESHOLD=32768 \
      -e V3_GLOBAL_LOSS_SUSPECT_THRESHOLD=131072 \
      -e V3_SHARD_COUNT="$shards" \
      -e V3_DURABLE_WORKER_BATCH_MAX=24 \
      -e V3_DURABLE_WORKER_BATCH_MIN=12 \
      -e V3_DURABLE_WORKER_BATCH_WAIT_US="$wait_us" \
      -e V3_DURABLE_WORKER_BATCH_WAIT_MIN_US="$wait_min_us" \
      -e V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS=8192 \
      -e V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT=50 \
      -e V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT=25 \
      -e V3_DURABLE_WORKER_DYNAMIC_INFLIGHT=true \
      -e V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MIN_CAP_PCT=5 \
      -e V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MAX_CAP_PCT=80 \
      -e V3_DURABLE_PRESSURE_EWMA_ALPHA_PCT=100 \
      -e V3_DURABLE_DYNAMIC_CAP_SLEW_STEP_PCT=100 \
      "$IMAGE" >/dev/null

    ready=0
    for _ in $(seq 1 120); do
      if curl -sS "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
        ready=1
        break
      fi
      sleep 0.2
    done
    if [[ "$ready" != "1" ]]; then
      echo "FAIL: health not ready case=${case_name} run=${run}"
      docker --context "$DOCKER_CONTEXT" logs "$container_name" | tail -n 80 || true
      docker --context "$DOCKER_CONTEXT" rm -f "$container_name" >/dev/null 2>&1 || true
      exit 1
    fi

    python3 scripts/ops/open_loop_v3_load.py \
      --host 127.0.0.1 \
      --port "$port" \
      --path /v3/orders \
      --duration-sec "$DURATION" \
      --target-rps "$TARGET_RPS" \
      --workers "$WORKERS" \
      --accounts "$ACCOUNTS" \
      --account-prefix "${case_name}_${run}" \
      --jwt-secret secret123 \
      --queue-capacity "$QUEUE_CAPACITY" \
      --request-timeout-sec "$REQUEST_TIMEOUT_SEC" \
      --drain-timeout-sec "$DRAIN_TIMEOUT_SEC" >"$load_out"

    curl -sS "http://127.0.0.1:${port}/metrics" >"$metrics_out"

    completed_rps="$(awk -F= '/^completed_rps=/{print $2}' "$load_out" | tail -n1)"
    accepted_rate="$(awk -F= '/^accepted_rate=/{print $2}' "$load_out" | tail -n1)"
    ack_p99="$(awk '/^gateway_v3_live_ack_accepted_p99_us /{print $2}' "$metrics_out" | tail -n1)"
    ack_p999="$(awk '/^gateway_v3_live_ack_accepted_p999_us /{print $2}' "$metrics_out" | tail -n1)"
    durable_p99="$(awk '/^gateway_v3_durable_confirm_p99_us /{print $2}' "$metrics_out" | tail -n1)"
    durable_p999="$(awk '/^gateway_v3_durable_confirm_p999_us /{print $2}' "$metrics_out" | tail -n1)"

    echo -e "${case_name}\t${run}\t${completed_rps:-0}\t${accepted_rate:-0}\t${ack_p99:-0}\t${ack_p999:-0}\t${durable_p99:-0}\t${durable_p999:-0}\t${load_out}\t${metrics_out}" >>"$RESULTS_TSV"
    echo "[linux-case] ${case_name} run=${run} completed_rps=${completed_rps:-0} accepted_rate=${accepted_rate:-0} ack_accepted_p99_us=${ack_p99:-0} durable_confirm_p99_us=${durable_p99:-0}"

    docker --context "$DOCKER_CONTEXT" rm -f "$container_name" >/dev/null 2>&1 || true
  done
done

echo "RESULTS_TSV=$RESULTS_TSV"
echo "RUN_DIR=$RUN_DIR"
