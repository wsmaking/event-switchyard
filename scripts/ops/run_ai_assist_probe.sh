#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-38080}"
REQUESTS="${REQUESTS:-300}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-2}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/var/results}"
BUILD_JAR="${BUILD_JAR:-1}"
AI_ASSIST_LLM_ENABLED="${AI_ASSIST_LLM_ENABLED:-false}"
AI_ASSIST_LLM_PROVIDER="${AI_ASSIST_LLM_PROVIDER:-none}"
AI_ASSIST_LLM_TIMEOUT_MS="${AI_ASSIST_LLM_TIMEOUT_MS:-600}"
AI_ASSIST_OPENAI_API_KEY="${AI_ASSIST_OPENAI_API_KEY:-}"
AI_ASSIST_OPENAI_ENDPOINT="${AI_ASSIST_OPENAI_ENDPOINT:-https://api.openai.com/v1/chat/completions}"
AI_ASSIST_OPENAI_MODEL="${AI_ASSIST_OPENAI_MODEL:-gpt-4o-mini}"
AI_ASSIST_OPENAI_TEMPERATURE="${AI_ASSIST_OPENAI_TEMPERATURE:-0.1}"
AI_ASSIST_OPENAI_MAX_TOKENS="${AI_ASSIST_OPENAI_MAX_TOKENS:-120}"
AI_ASSIST_OPENAI_HTTP_TIMEOUT_MS="${AI_ASSIST_OPENAI_HTTP_TIMEOUT_MS:-1200}"

mkdir -p "$OUT_DIR"
cd "$ROOT_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$OUT_DIR/ai_assist_probe_${STAMP}.server.log"
LOAD_OUT="$OUT_DIR/ai_assist_probe_${STAMP}.load.txt"
SUMMARY_OUT="$OUT_DIR/ai_assist_probe_${STAMP}.summary.txt"
METRICS_OUT="$OUT_DIR/ai_assist_probe_${STAMP}.metrics.prom"

if [[ "$BUILD_JAR" == "1" ]]; then
  ./gradlew :ai-assist:bootJar >/tmp/ai_assist_bootjar.log 2>&1
fi

JAR_PATH="$ROOT_DIR/ai-assist/build/libs/ai-assist.jar"
if [[ ! -f "$JAR_PATH" ]]; then
  echo "FAIL: ai-assist jar not found at $JAR_PATH"
  exit 1
fi

cleanup() {
  if [[ -n "${AI_ASSIST_PID:-}" ]]; then
    kill "${AI_ASSIST_PID}" >/dev/null 2>&1 || true
    wait "${AI_ASSIST_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "[start] ai-assist on ${HOST}:${PORT}"
AI_ASSIST_PORT="$PORT" \
AI_ASSIST_LLM_ENABLED="$AI_ASSIST_LLM_ENABLED" \
AI_ASSIST_LLM_PROVIDER="$AI_ASSIST_LLM_PROVIDER" \
AI_ASSIST_LLM_TIMEOUT_MS="$AI_ASSIST_LLM_TIMEOUT_MS" \
AI_ASSIST_OPENAI_API_KEY="$AI_ASSIST_OPENAI_API_KEY" \
AI_ASSIST_OPENAI_ENDPOINT="$AI_ASSIST_OPENAI_ENDPOINT" \
AI_ASSIST_OPENAI_MODEL="$AI_ASSIST_OPENAI_MODEL" \
AI_ASSIST_OPENAI_TEMPERATURE="$AI_ASSIST_OPENAI_TEMPERATURE" \
AI_ASSIST_OPENAI_MAX_TOKENS="$AI_ASSIST_OPENAI_MAX_TOKENS" \
AI_ASSIST_OPENAI_HTTP_TIMEOUT_MS="$AI_ASSIST_OPENAI_HTTP_TIMEOUT_MS" \
java -jar "$JAR_PATH" >"$LOG_FILE" 2>&1 &
AI_ASSIST_PID=$!

for _ in $(seq 1 100); do
  if curl -sS "http://${HOST}:${PORT}/actuator/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

if ! curl -sS "http://${HOST}:${PORT}/actuator/health" >/dev/null 2>&1; then
  echo "FAIL: ai-assist failed to start"
  exit 1
fi

echo "[load] requests=${REQUESTS} timeout=${REQUEST_TIMEOUT_SEC}s llm_enabled=${AI_ASSIST_LLM_ENABLED} llm_provider=${AI_ASSIST_LLM_PROVIDER}"
python3 - "$HOST" "$PORT" "$REQUESTS" "$REQUEST_TIMEOUT_SEC" >"$LOAD_OUT" <<'PY'
import json
import sys
import time
import urllib.request
import urllib.error

host = sys.argv[1]
port = int(sys.argv[2])
requests = int(sys.argv[3])
timeout = float(sys.argv[4])

url = f"http://{host}:{port}/ai/risk-navigator/check"
latencies_ms = []
ok = warn = block = 0
fallback_total = 0
llm_used_total = 0
error_total = 0

for i in range(requests):
    payload = {
        "accountId": f"acc-{i % 8}",
        "symbol": "7203.T" if i % 10 else "6758.T",
        "side": "BUY" if i % 2 == 0 else "SELL",
        "qty": 100 + (i % 1000),
        "price": 1000 + (i % 2000),
        "context": {"source": "probe", "seq": i},
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)
            decision = body.get("decision", "")
            if decision == "OK":
                ok += 1
            elif decision == "WARN":
                warn += 1
            else:
                block += 1
            if body.get("fallbackUsed", False):
                fallback_total += 1
            if body.get("llmUsed", False):
                llm_used_total += 1
    except Exception:
        error_total += 1

success_total = max(0, requests - error_total)
error_rate = (error_total / requests) if requests else 0.0
fallback_rate = (fallback_total / success_total) if success_total else 0.0

latencies_ms.sort()
def pctl(p):
    if not latencies_ms:
        return 0.0
    idx = min(len(latencies_ms) - 1, int((len(latencies_ms) - 1) * p))
    return latencies_ms[idx]

print(f"requests={requests}")
print(f"success_total={success_total}")
print(f"error_total={error_total}")
print(f"error_rate={error_rate:.6f}")
print(f"decision_ok_total={ok}")
print(f"decision_warn_total={warn}")
print(f"decision_block_total={block}")
print(f"fallback_total={fallback_total}")
print(f"fallback_rate={fallback_rate:.6f}")
print(f"llm_used_total={llm_used_total}")
print(f"latency_p50_ms={pctl(0.50):.3f}")
print(f"latency_p95_ms={pctl(0.95):.3f}")
print(f"latency_p99_ms={pctl(0.99):.3f}")
PY

curl -sS "http://${HOST}:${PORT}/actuator/prometheus" >"$METRICS_OUT"

load_value() {
  local key="$1"
  awk -F= -v k="$key" '$1==k {print $2}' "$LOAD_OUT" | tail -n1
}

success_total="$(load_value success_total)"
error_total="$(load_value error_total)"
error_rate="$(load_value error_rate)"
ok_total="$(load_value decision_ok_total)"
warn_total="$(load_value decision_warn_total)"
block_total="$(load_value decision_block_total)"
fallback_total="$(load_value fallback_total)"
fallback_rate="$(load_value fallback_rate)"
llm_used_total="$(load_value llm_used_total)"
latency_p50_ms="$(load_value latency_p50_ms)"
latency_p95_ms="$(load_value latency_p95_ms)"
latency_p99_ms="$(load_value latency_p99_ms)"

cat >"$SUMMARY_OUT" <<EOF
ai_assist_probe
date=${STAMP}
host=${HOST}
port=${PORT}
requests=${REQUESTS}
request_timeout_sec=${REQUEST_TIMEOUT_SEC}
llm_enabled=${AI_ASSIST_LLM_ENABLED}
llm_provider=${AI_ASSIST_LLM_PROVIDER}
success_total=${success_total}
error_total=${error_total}
error_rate=${error_rate}
decision_ok_total=${ok_total}
decision_warn_total=${warn_total}
decision_block_total=${block_total}
fallback_total=${fallback_total}
fallback_rate=${fallback_rate}
llm_used_total=${llm_used_total}
latency_p50_ms=${latency_p50_ms}
latency_p95_ms=${latency_p95_ms}
latency_p99_ms=${latency_p99_ms}
load_out=${LOAD_OUT}
metrics_out=${METRICS_OUT}
server_log=${LOG_FILE}
EOF

echo "[summary] success=${success_total}/${REQUESTS} error_rate=${error_rate} p95_ms=${latency_p95_ms} fallback_rate=${fallback_rate}"
echo "[artifacts] summary=${SUMMARY_OUT}"
echo "[artifacts] metrics=${METRICS_OUT}"
