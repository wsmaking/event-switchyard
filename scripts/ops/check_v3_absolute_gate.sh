#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-29001}"
DURATION="${DURATION:-300}"
CONNECTIONS="${CONNECTIONS:-400}"
THREADS="${THREADS:-8}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
NOISE_GUARD_STRICT="${NOISE_GUARD_STRICT:-0}"
NOISE_CPU_THRESHOLD="${NOISE_CPU_THRESHOLD:-35}"
TARGET_RPS="${TARGET_RPS:-10000}"
TARGET_ACK_P99_US="${TARGET_ACK_P99_US:-200}"
TARGET_ACCEPTED_RATE="${TARGET_ACCEPTED_RATE:-0.95}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "FAIL: pure HFT absolute gate requires Linux (current: $(uname -s))"
  exit 2
fi

mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$OUT_DIR/v3_gate_${STAMP}.gateway.log"
WRK_OUT="$OUT_DIR/v3_gate_${STAMP}.wrk.txt"
METRICS_OUT="$OUT_DIR/v3_gate_${STAMP}.metrics.prom"
SUMMARY_OUT="$OUT_DIR/v3_gate_${STAMP}.summary.txt"
NOISE_OUT="$OUT_DIR/v3_gate_${STAMP}.noise.txt"

cd "$ROOT_DIR"

if [[ "$BUILD_RELEASE" == "1" ]]; then
  echo "[build] building release binary with fixed perf flags..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_gate_build.log 2>&1
elif [[ ! -x ./gateway-rust/target/release/gateway-rust ]]; then
  echo "[build] release binary not found. building..."
  scripts/ops/build_gateway_rust_release.sh >/tmp/v3_gate_build.log 2>&1
fi

STRICT="$NOISE_GUARD_STRICT" CPU_THRESHOLD="$NOISE_CPU_THRESHOLD" \
  scripts/ops/perf_noise_guard.sh "$NOISE_OUT"

cleanup() {
  if [[ -n "${GATEWAY_PID:-}" ]]; then
    kill "${GATEWAY_PID}" >/dev/null 2>&1 || true
    wait "${GATEWAY_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "[start] launching gateway-rust on ${HOST}:${PORT}"
V3_SOFT_REJECT_PCT="${V3_SOFT_REJECT_PCT:-70}" \
V3_HARD_REJECT_PCT="${V3_HARD_REJECT_PCT:-85}" \
V3_KILL_REJECT_PCT="${V3_KILL_REJECT_PCT:-95}" \
V3_KILL_AUTO_RECOVER="${V3_KILL_AUTO_RECOVER:-false}" \
V3_INGRESS_QUEUE_CAPACITY="${V3_INGRESS_QUEUE_CAPACITY:-65536}" \
V3_DURABILITY_QUEUE_CAPACITY="${V3_DURABILITY_QUEUE_CAPACITY:-200000}" \
V3_SHARD_COUNT="${V3_SHARD_COUNT:-4}" \
V3_LOSS_GAP_TIMEOUT_MS="${V3_LOSS_GAP_TIMEOUT_MS:-500}" \
GATEWAY_PORT="$PORT" \
GATEWAY_TCP_PORT=0 \
JWT_HS256_SECRET=secret123 \
KAFKA_ENABLE=0 \
FASTPATH_DRAIN_ENABLE=1 \
FASTPATH_DRAIN_WORKERS="${FASTPATH_DRAIN_WORKERS:-4}" \
./gateway-rust/target/release/gateway-rust >"$LOG_FILE" 2>&1 &
GATEWAY_PID=$!

for _ in $(seq 1 100); do
  if curl -sS "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

if ! curl -sS "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
  echo "FAIL: gateway failed to start"
  exit 1
fi

echo "[load] wrk /v3/orders duration=${DURATION}s threads=${THREADS} connections=${CONNECTIONS}"
HOST="$HOST" \
PORT="$PORT" \
DURATION="$DURATION" \
CONNECTIONS="$CONNECTIONS" \
THREADS="$THREADS" \
LATENCY=1 \
WRK_PATH=/v3/orders \
JWT_HS256_SECRET=secret123 \
scripts/ops/wrk_gateway_rust.sh >"$WRK_OUT" 2>&1

curl -sS "http://${HOST}:${PORT}/metrics" >"$METRICS_OUT"

rps="$(awk '/Requests\/sec:/{print $2}' "$WRK_OUT" | tail -n1)"
live_ack_p99_us="$(awk '/^gateway_live_ack_p99_us /{print $2}' "$METRICS_OUT" | tail -n1)"
live_ack_accepted_p99_us="$(awk '/^gateway_live_ack_accepted_p99_us /{print $2}' "$METRICS_OUT" | tail -n1)"
accepted_rate="$(awk '/^gateway_v3_accepted_rate /{print $2}' "$METRICS_OUT" | tail -n1)"
accepted_total="$(awk '/^gateway_v3_accepted_total /{print $2}' "$METRICS_OUT" | tail -n1)"
rejected_soft_total="$(awk '/^gateway_v3_rejected_soft_total /{print $2}' "$METRICS_OUT" | tail -n1)"
rejected_hard_total="$(awk '/^gateway_v3_rejected_hard_total /{print $2}' "$METRICS_OUT" | tail -n1)"
rejected_killed_total="$(awk '/^gateway_v3_rejected_killed_total /{print $2}' "$METRICS_OUT" | tail -n1)"

rps="${rps:-0}"
live_ack_p99_us="${live_ack_p99_us:-999999}"
live_ack_accepted_p99_us="${live_ack_accepted_p99_us:-NA}"
accepted_rate="${accepted_rate:-0}"
accepted_total="${accepted_total:-0}"
rejected_soft_total="${rejected_soft_total:-0}"
rejected_hard_total="${rejected_hard_total:-0}"
rejected_killed_total="${rejected_killed_total:-0}"

cat >"$SUMMARY_OUT" <<EOF
v3_absolute_gate
date=${STAMP}
host=${HOST}:${PORT}
duration_sec=${DURATION}
target_rps=${TARGET_RPS}
target_live_ack_p99_us=${TARGET_ACK_P99_US}
target_accepted_rate=${TARGET_ACCEPTED_RATE}
observed_rps=${rps}
observed_live_ack_p99_us=${live_ack_p99_us}
observed_live_ack_accepted_p99_us=${live_ack_accepted_p99_us}
observed_accepted_rate=${accepted_rate}
accepted_total=${accepted_total}
rejected_soft_total=${rejected_soft_total}
rejected_hard_total=${rejected_hard_total}
rejected_killed_total=${rejected_killed_total}
wrk_out=${WRK_OUT}
metrics_out=${METRICS_OUT}
gateway_log=${LOG_FILE}
noise_snapshot=${NOISE_OUT}
EOF

pass_rps="$(awk -v v="$rps" -v t="$TARGET_RPS" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"
pass_ack="$(awk -v v="$live_ack_p99_us" -v t="$TARGET_ACK_P99_US" 'BEGIN{print (v+0<=t+0) ? 1 : 0}')"
pass_rate="$(awk -v v="$accepted_rate" -v t="$TARGET_ACCEPTED_RATE" 'BEGIN{print (v+0>=t+0) ? 1 : 0}')"

echo "[summary] rps=${rps} live_ack_p99_us=${live_ack_p99_us} live_ack_accepted_p99_us=${live_ack_accepted_p99_us} accepted_rate=${accepted_rate}"
echo "[artifacts] ${SUMMARY_OUT}"

if [[ "$pass_rps" == "1" && "$pass_ack" == "1" && "$pass_rate" == "1" ]]; then
  echo "PASS: absolute gate satisfied"
  exit 0
fi

echo "FAIL: absolute gate not satisfied"
echo "  - rps >= ${TARGET_RPS}: ${pass_rps}"
echo "  - live_ack_p99_us <= ${TARGET_ACK_P99_US}: ${pass_ack}"
echo "  - accepted_rate >= ${TARGET_ACCEPTED_RATE}: ${pass_rate}"
exit 1
