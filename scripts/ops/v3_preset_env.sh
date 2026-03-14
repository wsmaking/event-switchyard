#!/usr/bin/env bash
# Shared preset definitions for v3 ops scripts.
# This file is intended to be sourced.

if [[ -n "${V3_PRESET_ENV_SH_LOADED:-}" ]]; then
  return 0 2>/dev/null || exit 0
fi
V3_PRESET_ENV_SH_LOADED=1

v3_preset_default() {
  local key="$1"
  local value="$2"
  if [[ -z "${!key:-}" ]]; then
    export "${key}=${value}"
  fi
}

list_v3_ops_presets() {
  cat <<'EOF'
hft_best        : strict-quality baseline (TCP, 10k, us+ns gates)
hft_capacity    : throughput-focused capability run (45k profile)
hft_diagnostic  : diagnosis-focused run (gate enforcement off)
http_compare    : HTTP ingress comparison profile
EOF
}

apply_v3_ops_preset() {
  local preset="${1:-}"
  case "$preset" in
    hft_best|strict)
      v3_preset_default V3_INGRESS_TRANSPORT tcp
      v3_preset_default V3_TCP_PORT 39001
      v3_preset_default TARGET_RPS 10000
      v3_preset_default TARGET_COMPLETED_RPS 10000
      v3_preset_default TARGET_COMPLETED_RPS_EPSILON 0.0005
      v3_preset_default TARGET_ACK_ACCEPTED_P99_US 40
      v3_preset_default TARGET_ACK_ACCEPTED_P99_NS 40000
      v3_preset_default TARGET_ACK_P99_US 100
      v3_preset_default TARGET_ACCEPTED_RATE 0.99
      v3_preset_default TARGET_DURABLE_CONFIRM_P99_US 120000000
      v3_preset_default WARN_DURABLE_CONFIRM_P99_US 120000000
      v3_preset_default TARGET_REJECTED_KILLED_MAX 0
      v3_preset_default TARGET_LOSS_SUSPECT_MAX 0
      v3_preset_default TARGET_OFFERED_RPS_RATIO_MIN 0.99
      v3_preset_default TARGET_DROPPED_OFFER_RATIO_MAX 0.001
      v3_preset_default TARGET_UNSENT_TOTAL_MAX 0
      v3_preset_default TARGET_STRICT_LANE_TOPOLOGY 1
      v3_preset_default TARGET_STRICT_PER_LANE_CHECKS 1
      v3_preset_default V3_DURABLE_ACK_PATH_GUARD_ENABLED false
      v3_preset_default V3_TCP_BUSY_POLL_US 50
      v3_preset_default V3_TCP_AUTH_STICKY_CONTEXT true
      v3_preset_default V3_TCP_STICKY_ACCOUNT_PER_WORKER true
      v3_preset_default LOAD_WORKERS 192
      v3_preset_default LOAD_ACCOUNTS 24
      v3_preset_default LOAD_QUEUE_CAPACITY 200000
      v3_preset_default LOAD_FINAL_CATCHUP true
      v3_preset_default V3_DURABLE_CONTROL_PRESET hft_stable
      v3_preset_default V3_DURABLE_ADMISSION_FSYNC_ONLY_SOFT_SUSTAIN_TICKS 0
      v3_preset_default V3_DURABLE_ADMISSION_FSYNC_ONLY_HARD_SUSTAIN_TICKS 0
      ;;
    hft_capacity|capability)
      v3_preset_default V3_INGRESS_TRANSPORT tcp
      v3_preset_default V3_TCP_PORT 39001
      v3_preset_default DURATION 120
      v3_preset_default TARGET_RPS 45000
      v3_preset_default TARGET_COMPLETED_RPS 45000
      v3_preset_default TARGET_ACK_ACCEPTED_P99_US 40
      v3_preset_default TARGET_ACK_ACCEPTED_P99_NS 40000
      v3_preset_default TARGET_ACK_P99_US 100
      v3_preset_default TARGET_ACCEPTED_RATE 0.95
      v3_preset_default TARGET_REJECTED_KILLED_MAX 0
      v3_preset_default TARGET_LOSS_SUSPECT_MAX 0
      v3_preset_default TARGET_OFFERED_RPS_RATIO_MIN 0.99
      v3_preset_default TARGET_DROPPED_OFFER_RATIO_MAX 0.001
      v3_preset_default TARGET_UNSENT_TOTAL_MAX 0
      v3_preset_default TARGET_STRICT_LANE_TOPOLOGY 1
      v3_preset_default TARGET_STRICT_PER_LANE_CHECKS 1
      v3_preset_default V3_DURABLE_ACK_PATH_GUARD_ENABLED false
      v3_preset_default V3_TCP_BUSY_POLL_US 50
      v3_preset_default V3_TCP_AUTH_STICKY_CONTEXT true
      v3_preset_default V3_TCP_STICKY_ACCOUNT_PER_WORKER true
      v3_preset_default LOAD_WORKERS 512
      v3_preset_default LOAD_ACCOUNTS 64
      v3_preset_default LOAD_QUEUE_CAPACITY 1500000
      v3_preset_default V3_SHARD_COUNT 8
      ;;
    hft_diagnostic|diag)
      v3_preset_default V3_INGRESS_TRANSPORT tcp
      v3_preset_default V3_TCP_PORT 39001
      v3_preset_default DURATION 60
      v3_preset_default TARGET_RPS 10000
      v3_preset_default TARGET_COMPLETED_RPS 10000
      v3_preset_default TARGET_ACK_ACCEPTED_P99_US 50
      v3_preset_default TARGET_ACK_ACCEPTED_P99_NS 0
      v3_preset_default TARGET_ACK_P99_US 120
      v3_preset_default TARGET_ACCEPTED_RATE 0.95
      v3_preset_default TARGET_DURABLE_CONFIRM_P99_US 0
      v3_preset_default WARN_DURABLE_CONFIRM_P99_US 0
      v3_preset_default ENFORCE_GATE 0
      v3_preset_default V3_DURABLE_ACK_PATH_GUARD_ENABLED true
      v3_preset_default V3_TCP_BUSY_POLL_US 0
      v3_preset_default V3_TCP_AUTH_STICKY_CONTEXT true
      v3_preset_default V3_TCP_STICKY_ACCOUNT_PER_WORKER true
      v3_preset_default ENABLE_TIMESERIES 1
      v3_preset_default TIMESERIES_INTERVAL_MS 500
      v3_preset_default LOAD_WORKERS 192
      v3_preset_default LOAD_ACCOUNTS 24
      ;;
    http_compare|http)
      v3_preset_default V3_INGRESS_TRANSPORT http
      v3_preset_default V3_TCP_PORT 39001
      v3_preset_default TARGET_ACK_ACCEPTED_P99_US 40
      v3_preset_default TARGET_ACK_ACCEPTED_P99_NS 40000
      v3_preset_default V3_DURABLE_ACK_PATH_GUARD_ENABLED true
      v3_preset_default V3_TCP_BUSY_POLL_US 0
      v3_preset_default V3_TCP_AUTH_STICKY_CONTEXT false
      v3_preset_default V3_TCP_STICKY_ACCOUNT_PER_WORKER false
      ;;
    "" )
      return 0
      ;;
    *)
      echo "FAIL: unknown V3_OPS_PRESET=${preset}" >&2
      echo "available presets:" >&2
      list_v3_ops_presets >&2
      return 1
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  cmd="${1:-}"
  case "$cmd" in
    --list)
      list_v3_ops_presets
      ;;
    --help|-h|"")
      echo "Usage:"
      echo "  source scripts/ops/v3_preset_env.sh && apply_v3_ops_preset <name>"
      echo "  scripts/ops/v3_preset_env.sh --list"
      ;;
    *)
      apply_v3_ops_preset "$cmd"
      ;;
  esac
fi
