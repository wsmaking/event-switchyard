use super::*;
use crate::audit::{AuditEvent, AuditLog};
use crate::backpressure::BackpressureConfig;
use crate::bus::BusPublisher;
use crate::engine::FastPathEngine;
use crate::order::{OrderRequest, OrderType, TimeInForce};
use crate::sse::SseHub;
use crate::store::{OrderIdMap, OrderStatus, OrderStore, ShardedOrderStore};
use crate::strategy::algo::{
    AlgoExecutionPlan, AlgoExecutionSlice, STRATEGY_ALGO_PLAN_SCHEMA_VERSION,
};
use crate::strategy::config::{
    AccountRiskBudget, ExecutionConfigSnapshot, ExecutionPolicyConfig, KillSwitchPolicy,
    SymbolExecutionOverride, UrgencyOverride, VenuePreference,
};
use crate::strategy::intent::{
    AlgoExecutionSpec, ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION,
    StrategyIntent,
};
use crate::strategy::runtime::{
    AlgoChildStatus, AlgoParentExecution, AlgoParentStatus, AlgoRuntimeMode,
    STRATEGY_ALGO_RUNTIME_SNAPSHOT_EVENT_TYPE,
};
use crate::strategy::shadow::ShadowScoreComponent;
use crate::strategy::shadow::{
    SHADOW_RECORD_SCHEMA_VERSION, ShadowComparisonStatus, ShadowOutcomeView, ShadowPolicyView,
    ShadowRecord,
};
use axum::http::HeaderValue;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use gateway_core::LatencyHistogram;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

const TEST_JWT_SECRET: &str = "orders-v2-test-secret";

fn build_test_state() -> AppState {
    let wal_path =
        std::env::temp_dir().join(format!("gateway-rust-orders-test-{}.log", now_nanos()));
    let audit_log = Arc::new(AuditLog::new(wal_path).expect("create audit log"));
    build_test_state_with_audit_log(audit_log)
}

fn build_test_state_with_audit_log(audit_log: Arc<AuditLog>) -> AppState {
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let shard = super::super::V3ShardIngress::new(tx, 1024);
    let (durable_tx, _durable_rx) = tokio::sync::mpsc::channel(1024);
    let durable_lane = super::super::V3DurableLane::new(durable_tx, 1024);
    let v3_durable_ingress = Arc::new(super::super::V3DurableIngress::new(vec![durable_lane]));
    let v3_ingress = Arc::new(super::super::V3Ingress::new(
        vec![shard],
        false,
        60,
        3_000,
        60,
        1,
        3,
        6,
    ));
    audit_log.clone().start_async_writer(None);
    let audit_read_path = Arc::new(audit_log.path().to_path_buf());
    let v3_durable_audit_logs = Arc::new(vec![Arc::clone(&audit_log)]);
    let v3_confirm_rebuild_paths = Arc::new(vec![audit_log.path().to_path_buf()]);
    let shard_u64 = || Arc::new(vec![Arc::new(AtomicU64::new(0))]);
    let lane_u64 = || Arc::new(vec![Arc::new(AtomicU64::new(0))]);
    let lane_u64_with = |value: u64| Arc::new(vec![Arc::new(AtomicU64::new(value))]);
    let lane_i64 = || Arc::new(vec![Arc::new(AtomicI64::new(0))]);
    let confirm_lane_hist = || Arc::new(vec![Arc::new(LatencyHistogram::new())]);
    let v3_durable_wal_fsync_hist_per_lane = Arc::new(vec![Arc::new(LatencyHistogram::new())]);
    let v3_durable_worker_loop_hist_per_lane = Arc::new(vec![Arc::new(LatencyHistogram::new())]);

    AppState {
        engine: FastPathEngine::new(16_384),
        jwt_auth: Arc::new(crate::auth::JwtAuth::for_test(TEST_JWT_SECRET)),
        order_store: Arc::new(OrderStore::new()),
        sharded_store: Arc::new(ShardedOrderStore::new_with_ttl_and_shards(86_400_000, 64)),
        order_id_map: Arc::new(OrderIdMap::new()),
        venue_order_control: None,
        sse_hub: Arc::new(SseHub::new()),
        order_id_seq: Arc::new(AtomicU64::new(1)),
        audit_log,
        v3_durable_audit_logs,
        audit_read_path,
        v3_confirm_rebuild_paths,
        bus_publisher: Arc::new(BusPublisher::disabled_for_test()),
        bus_mode_outbox: true,
        backpressure: BackpressureConfig {
            inflight_max: None,
            soft_wal_age_ms_max: None,
            wal_bytes_max: None,
            wal_age_ms_max: None,
            disk_free_pct_min: None,
        },
        inflight_controller: crate::inflight::InflightController::spawn_from_env(),
        rate_limiter: None,
        strategy_snapshot_store: Arc::new(
            crate::store::strategy_snapshot::StrategySnapshotStore::default(),
        ),
        strategy_shadow_store: Arc::new(
            crate::store::strategy_shadow_store::StrategyShadowStore::default(),
        ),
        strategy_runtime_store: Arc::new(
            crate::store::strategy_runtime_store::StrategyRuntimeStore::default(),
        ),
        quant_feedback_exporter: Arc::new(crate::strategy::sink::FeedbackExporter::disabled()),
        ack_hist: Arc::new(LatencyHistogram::new()),
        wal_enqueue_hist: Arc::new(LatencyHistogram::new()),
        durable_ack_hist: Arc::new(LatencyHistogram::new()),
        fdatasync_hist: Arc::new(LatencyHistogram::new()),
        durable_notify_hist: Arc::new(LatencyHistogram::new()),
        v2_durable_wait_timeout_ms: 1_000,
        v2_requests_total: Arc::new(AtomicU64::new(0)),
        v2_durable_wait_timeout_total: Arc::new(AtomicU64::new(0)),
        idempotency_checked: Arc::new(AtomicU64::new(0)),
        idempotency_hits: Arc::new(AtomicU64::new(0)),
        idempotency_creates: Arc::new(AtomicU64::new(0)),
        reject_invalid_qty: Arc::new(AtomicU64::new(0)),
        reject_rate_limit: Arc::new(AtomicU64::new(0)),
        reject_risk: Arc::new(AtomicU64::new(0)),
        reject_invalid_symbol: Arc::new(AtomicU64::new(0)),
        reject_queue_full: Arc::new(AtomicU64::new(0)),
        backpressure_soft_wal_age: Arc::new(AtomicU64::new(0)),
        backpressure_soft_rate_decline: Arc::new(AtomicU64::new(0)),
        backpressure_inflight: Arc::new(AtomicU64::new(0)),
        backpressure_wal_bytes: Arc::new(AtomicU64::new(0)),
        backpressure_wal_age: Arc::new(AtomicU64::new(0)),
        backpressure_disk_free: Arc::new(AtomicU64::new(0)),
        v3_ingress: Arc::clone(&v3_ingress),
        v3_accepted_total: Arc::new(AtomicU64::new(0)),
        v3_accepted_total_per_shard: shard_u64(),
        v3_rejected_soft_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_soft_total_per_shard: shard_u64(),
        v3_rejected_hard_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_hard_total_per_shard: shard_u64(),
        v3_rejected_killed_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_killed_total_per_shard: shard_u64(),
        v3_kill_recovered_total: Arc::new(AtomicU64::new(0)),
        v3_loss_suspect_total: Arc::new(AtomicU64::new(0)),
        v3_session_killed_total: Arc::new(AtomicU64::new(0)),
        v3_shard_killed_total: Arc::new(AtomicU64::new(0)),
        v3_global_killed_total: Arc::new(AtomicU64::new(0)),
        v3_durable_ingress: Arc::clone(&v3_durable_ingress),
        v3_durable_accepted_total: Arc::new(AtomicU64::new(0)),
        v3_durable_accepted_total_per_lane: lane_u64(),
        v3_durable_rejected_total: Arc::new(AtomicU64::new(0)),
        v3_durable_rejected_total_per_lane: lane_u64(),
        v3_live_ack_hist: Arc::new(LatencyHistogram::new()),
        v3_live_ack_hist_ns: Arc::new(LatencyHistogram::new()),
        v3_live_ack_accepted_hist: Arc::new(LatencyHistogram::new()),
        v3_live_ack_accepted_hist_ns: Arc::new(LatencyHistogram::new()),
        v3_live_ack_accepted_tsc_hist_ns: Arc::new(LatencyHistogram::new()),
        v3_tsc_clock: None,
        v3_tsc_runtime_enabled: Arc::new(AtomicBool::new(false)),
        v3_tsc_invariant: false,
        v3_tsc_hz: 0,
        v3_tsc_mismatch_threshold_pct: 20,
        v3_tsc_fallback_total: Arc::new(AtomicU64::new(0)),
        v3_tsc_cross_core_total: Arc::new(AtomicU64::new(0)),
        v3_tsc_mismatch_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_append_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_fsync_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_fsync_hist_per_lane,
        v3_durable_fsync_p99_cached_us: Arc::new(AtomicU64::new(6_000)),
        v3_durable_fsync_p99_cached_us_per_lane: lane_u64(),
        v3_durable_fsync_ewma_alpha_pct: 30,
        v3_durable_worker_loop_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_worker_loop_hist_per_lane,
        v3_durable_worker_batch_min: 4,
        v3_durable_worker_batch_max: 8,
        v3_durable_worker_batch_wait_min_us: 100,
        v3_durable_worker_batch_wait_us: 200,
        v3_durable_worker_receipt_timeout_us: 20_000_000,
        v3_durable_replica_enabled: false,
        v3_durable_replica_required: false,
        v3_durable_replica_receipt_timeout_us: 20_000_000,
        v3_durable_worker_max_inflight_receipts: 16_384,
        v3_durable_worker_max_inflight_receipts_global: 65_536,
        v3_durable_worker_inflight_soft_cap_pct: 50,
        v3_durable_worker_inflight_hard_cap_pct: 25,
        v3_durable_worker_batch_adaptive: true,
        v3_durable_worker_batch_adaptive_low_util_pct: 10.0,
        v3_durable_worker_batch_adaptive_high_util_pct: 60.0,
        v3_durable_depth_last: Arc::new(AtomicU64::new(0)),
        v3_durable_depth_last_per_lane: lane_u64(),
        v3_durable_backlog_growth_per_sec: Arc::new(AtomicI64::new(0)),
        v3_durable_backlog_growth_per_sec_per_lane: lane_i64(),
        v3_durable_soft_reject_pct: 95,
        v3_durable_hard_reject_pct: 98,
        v3_durable_backlog_soft_reject_per_sec: i64::MAX,
        v3_durable_backlog_hard_reject_per_sec: i64::MAX,
        v3_durable_backlog_signal_min_queue_pct: 0.0,
        v3_durable_ack_path_guard_enabled: true,
        v3_durable_admission_controller_enabled: false,
        v3_durable_admission_sustain_ticks: 1,
        v3_durable_admission_recover_ticks: 1,
        v3_durable_admission_soft_fsync_p99_us: 6_000,
        v3_durable_admission_hard_fsync_p99_us: 12_000,
        v3_durable_admission_fsync_presignal_pct: 1.0,
        v3_durable_admission_level: Arc::new(AtomicU64::new(0)),
        v3_durable_admission_level_per_lane: lane_u64(),
        v3_durable_admission_soft_trip_total: Arc::new(AtomicU64::new(0)),
        v3_durable_admission_soft_trip_total_per_lane: lane_u64(),
        v3_durable_admission_hard_trip_total: Arc::new(AtomicU64::new(0)),
        v3_durable_admission_hard_trip_total_per_lane: lane_u64(),
        v3_durable_admission_signal_queue_soft_total_per_lane: lane_u64(),
        v3_durable_admission_signal_queue_hard_total_per_lane: lane_u64(),
        v3_durable_admission_signal_backlog_soft_total_per_lane: lane_u64(),
        v3_durable_admission_signal_backlog_hard_total_per_lane: lane_u64(),
        v3_durable_admission_signal_fsync_soft_total_per_lane: lane_u64(),
        v3_durable_admission_signal_fsync_hard_total_per_lane: lane_u64(),
        v3_durable_backpressure_soft_total: Arc::new(AtomicU64::new(0)),
        v3_durable_backpressure_soft_total_per_lane: lane_u64(),
        v3_durable_backpressure_hard_total: Arc::new(AtomicU64::new(0)),
        v3_durable_backpressure_hard_total_per_lane: lane_u64(),
        v3_durable_write_error_total: Arc::new(AtomicU64::new(0)),
        v3_durable_replica_append_total: Arc::new(AtomicU64::new(0)),
        v3_durable_replica_write_error_total: Arc::new(AtomicU64::new(0)),
        v3_durable_replica_receipt_timeout_total: Arc::new(AtomicU64::new(0)),
        v3_durable_receipt_timeout_total: Arc::new(AtomicU64::new(0)),
        v3_durable_receipt_inflight_per_lane: lane_u64(),
        v3_durable_receipt_inflight_max_per_lane: lane_u64(),
        v3_durable_receipt_inflight: Arc::new(AtomicU64::new(0)),
        v3_durable_receipt_inflight_max: Arc::new(AtomicU64::new(0)),
        v3_durable_pressure_pct_per_lane: lane_u64(),
        v3_durable_dynamic_cap_pct_per_lane: lane_u64(),
        v3_soft_reject_pct: 85,
        v3_hard_reject_pct: 90,
        v3_kill_reject_pct: 95,
        v3_thread_affinity_apply_success_total: Arc::new(AtomicU64::new(0)),
        v3_thread_affinity_apply_failure_total: Arc::new(AtomicU64::new(0)),
        v3_shard_affinity_cpu: Arc::new(vec![-1]),
        v3_durable_affinity_cpu: Arc::new(vec![-1]),
        v3_tcp_server_affinity_cpu: -1,
        v3_confirm_store: Arc::new(super::super::V3ConfirmStore::new(1, 500, 600_000)),
        v3_confirm_oldest_inflight_us: Arc::new(AtomicU64::new(0)),
        v3_confirm_oldest_inflight_us_per_lane: lane_u64(),
        v3_confirm_age_hist_per_lane: confirm_lane_hist(),
        v3_loss_gap_timeout_ms: 500,
        v3_loss_scan_interval_ms: 50,
        v3_loss_scan_batch: 128,
        v3_confirm_gc_batch: 128,
        v3_confirm_timeout_scan_cost_last: Arc::new(AtomicU64::new(0)),
        v3_confirm_timeout_scan_cost_total: Arc::new(AtomicU64::new(0)),
        v3_confirm_gc_removed_total: Arc::new(AtomicU64::new(0)),
        v3_confirm_rebuild_restored_total: Arc::new(AtomicU64::new(0)),
        v3_confirm_rebuild_elapsed_ms: Arc::new(AtomicU64::new(0)),
        v3_replay_position_applied_total: Arc::new(AtomicU64::new(0)),
        v3_replay_session_seq_seeded_total: Arc::new(AtomicU64::new(0)),
        v3_replay_session_shard_seeded_total: Arc::new(AtomicU64::new(0)),
        v3_startup_rebuild_in_progress: Arc::new(AtomicBool::new(false)),
        v3_startup_rebuild_started_at_ns: Arc::new(AtomicU64::new(0)),
        v3_startup_rebuild_completed_at_ns: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_soft_reject_age_us: 0,
        v3_durable_confirm_hard_reject_age_us: 0,
        v3_durable_confirm_guard_soft_slack_pct: 0,
        v3_durable_confirm_guard_hard_slack_pct: 0,
        v3_durable_confirm_guard_soft_requires_admission: false,
        v3_durable_confirm_guard_hard_requires_admission: false,
        v3_durable_confirm_guard_secondary_required: false,
        v3_durable_confirm_guard_min_queue_pct: 0.0,
        v3_durable_confirm_guard_min_inflight_pct: 0,
        v3_durable_confirm_guard_min_backlog_per_sec: 0,
        v3_durable_confirm_guard_soft_sustain_ticks: 2,
        v3_durable_confirm_guard_hard_sustain_ticks: 2,
        v3_durable_confirm_guard_recover_ticks: 3,
        v3_durable_confirm_guard_recover_hysteresis_pct: 85,
        v3_durable_confirm_guard_autotune_enabled: true,
        v3_durable_confirm_guard_autotune_low_pressure_pct: 35,
        v3_durable_confirm_guard_autotune_high_pressure_pct: 80,
        v3_durable_confirm_guard_soft_sustain_ticks_effective_per_lane: lane_u64_with(2),
        v3_durable_confirm_guard_hard_sustain_ticks_effective_per_lane: lane_u64_with(2),
        v3_durable_confirm_guard_recover_ticks_effective_per_lane: lane_u64_with(3),
        v3_durable_confirm_guard_soft_armed_per_lane: lane_u64_with(1),
        v3_durable_confirm_guard_hard_armed_per_lane: lane_u64_with(1),
        v3_durable_confirm_age_soft_reject_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_soft_reject_skipped_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_skipped_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_soft_reject_skipped_unarmed_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_skipped_unarmed_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_soft_reject_skipped_low_load_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_skipped_low_load_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_autotune_enabled: false,
        v3_durable_confirm_age_autotune_alpha_pct: 20,
        v3_durable_confirm_age_fsync_linked: true,
        v3_durable_confirm_age_fsync_soft_ref_us: 6_000,
        v3_durable_confirm_age_fsync_hard_ref_us: 12_000,
        v3_durable_confirm_age_fsync_max_relax_pct: 100,
        v3_durable_confirm_soft_reject_age_min_us: 0,
        v3_durable_confirm_soft_reject_age_max_us: 0,
        v3_durable_confirm_hard_reject_age_min_us: 0,
        v3_durable_confirm_hard_reject_age_max_us: 0,
        v3_durable_confirm_soft_reject_age_effective_us_per_lane: lane_u64(),
        v3_durable_confirm_hard_reject_age_effective_us_per_lane: lane_u64(),
        v3_durable_confirm_hourly_pressure_ewma_per_lane: Arc::new(
            (0..24)
                .map(|_| Arc::new(AtomicU64::new(0)))
                .collect::<Vec<_>>(),
        ),
        v3_risk_profile: super::super::V3RiskProfile::Light,
        v3_risk_margin_mode: super::super::V3RiskMarginMode::Legacy,
        v3_risk_loops: 16,
        v3_risk_strict_symbols: false,
        v3_risk_max_order_qty: 10_000,
        v3_risk_max_notional: 1_000_000_000,
        v3_risk_daily_notional_limit: 1_000_000_000_000,
        v3_risk_max_abs_position_qty: 100_000_000,
        v3_symbol_limits: Arc::new(HashMap::new()),
        v3_session_id_intern: Arc::new(dashmap::DashMap::new()),
        v3_account_id_intern: Arc::new(dashmap::DashMap::new()),
        v3_account_daily_notional: Arc::new(dashmap::DashMap::new()),
        v3_account_symbol_position: Arc::new(dashmap::DashMap::new()),
        v3_hotpath_histogram_sample_rate: 1,
        v3_hotpath_sample_cursor: Arc::new(AtomicU64::new(0)),
        v3_stage_parse_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_position_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_margin_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_limits_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_enqueue_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_serialize_hist: Arc::new(LatencyHistogram::new()),
    }
}

fn build_test_state_with_v3_pipeline(
    confirm_timeout_ms: u64,
    confirm_ttl_ms: u64,
    loss_scan_interval_ms: u64,
    durable_lane_count: usize,
    durable_lane_capacity: usize,
) -> (
    AppState,
    tokio::sync::mpsc::Receiver<super::super::V3OrderTask>,
    Vec<tokio::sync::mpsc::Receiver<super::super::V3DurableTask>>,
) {
    let wal_path =
        std::env::temp_dir().join(format!("gateway-rust-orders-v3-int-{}.log", now_nanos()));
    let audit_log = Arc::new(AuditLog::new(wal_path).expect("create audit log"));
    build_test_state_with_v3_pipeline_and_audit_log(
        audit_log,
        confirm_timeout_ms,
        confirm_ttl_ms,
        loss_scan_interval_ms,
        durable_lane_count,
        durable_lane_capacity,
    )
}

fn build_test_state_with_v3_pipeline_and_audit_log(
    audit_log: Arc<AuditLog>,
    confirm_timeout_ms: u64,
    confirm_ttl_ms: u64,
    loss_scan_interval_ms: u64,
    durable_lane_count: usize,
    durable_lane_capacity: usize,
) -> (
    AppState,
    tokio::sync::mpsc::Receiver<super::super::V3OrderTask>,
    Vec<tokio::sync::mpsc::Receiver<super::super::V3DurableTask>>,
) {
    let mut state = build_test_state_with_audit_log(audit_log);
    let (ingress_tx, ingress_rx) = tokio::sync::mpsc::channel(1024);
    let shard = super::super::V3ShardIngress::new(ingress_tx, 1024);
    state.v3_ingress = Arc::new(super::super::V3Ingress::new(
        vec![shard],
        false,
        60,
        3_000,
        60,
        1,
        3,
        6,
    ));
    let mut durable_lanes = Vec::with_capacity(durable_lane_count.max(1));
    let mut durable_rxs = Vec::with_capacity(durable_lane_count.max(1));
    for _ in 0..durable_lane_count.max(1) {
        let (durable_tx, durable_rx) = tokio::sync::mpsc::channel(durable_lane_capacity.max(1));
        durable_lanes.push(super::super::V3DurableLane::new(
            durable_tx,
            durable_lane_capacity.max(1) as u64,
        ));
        durable_rxs.push(durable_rx);
    }
    state.v3_durable_ingress = Arc::new(super::super::V3DurableIngress::new(durable_lanes));
    state.v3_confirm_store = Arc::new(super::super::V3ConfirmStore::new(
        1,
        confirm_timeout_ms,
        confirm_ttl_ms,
    ));
    state.v3_confirm_oldest_inflight_us = Arc::new(AtomicU64::new(0));
    state.v3_confirm_oldest_inflight_us_per_lane = Arc::new(vec![Arc::new(AtomicU64::new(0))]);
    state.v3_confirm_age_hist_per_lane = Arc::new(vec![Arc::new(LatencyHistogram::new())]);
    state.v3_loss_gap_timeout_ms = confirm_timeout_ms;
    state.v3_loss_scan_interval_ms = loss_scan_interval_ms;
    state.v3_loss_scan_batch = 256;
    state.v3_confirm_gc_batch = 256;
    state.v3_durable_worker_batch_max = 4;
    state.v3_durable_worker_batch_wait_us = 100;
    (state, ingress_rx, durable_rxs)
}

fn build_test_state_with_soft_queue_pressure(
    max_depth: u64,
    queued_depth: u64,
    soft_reject_pct: u64,
    hard_reject_pct: u64,
    kill_reject_pct: u64,
) -> (
    AppState,
    tokio::sync::mpsc::Receiver<super::super::V3OrderTask>,
) {
    let mut state = build_test_state();
    let capacity = max_depth.max(1) as usize;
    let (ingress_tx, ingress_rx) = tokio::sync::mpsc::channel(capacity);
    let shard = super::super::V3ShardIngress::new(ingress_tx, max_depth.max(1));
    state.v3_ingress = Arc::new(super::super::V3Ingress::new(
        vec![shard],
        false,
        95,
        10,
        5,
        32,
        64,
        128,
    ));
    state.v3_soft_reject_pct = soft_reject_pct;
    state.v3_hard_reject_pct = hard_reject_pct;
    state.v3_kill_reject_pct = kill_reject_pct;
    state.v3_durable_ack_path_guard_enabled = false;

    for seq in 0..queued_depth.min(max_depth) {
        let task = super::super::V3OrderTask {
            session_id: Arc::<str>::from(format!("seed-sess-{seq}")),
            account_id: Arc::<str>::from("seed-acc"),
            execution_run_id: None,
            decision_key: None,
            decision_attempt_seq: None,
            intent_id: None,
            model_id: None,
            effective_risk_budget_ref: None,
            actual_policy: None,
            position_symbol_key: FastPathEngine::symbol_to_bytes("AAPL"),
            position_delta_qty: 1,
            session_seq: seq + 1,
            attempt_seq: seq + 1,
            received_at_ns: now_nanos(),
            shard_id: 0,
        };
        state
            .v3_ingress
            .try_enqueue(0, task)
            .expect("seed ingress queue");
    }

    (state, ingress_rx)
}

fn make_token(account_id: &str) -> String {
    let header = r#"{"alg":"HS256","typ":"JWT"}"#;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let payload = format!(
        r#"{{"accountId":"{}","sub":"{}","iat":{},"exp":{}}}"#,
        account_id,
        account_id,
        now,
        now + 3600
    );

    let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());
    let signing_input = format!("{}.{}", header_b64, payload_b64);

    let mut mac = HmacSha256::new_from_slice(TEST_JWT_SECRET.as_bytes()).expect("hmac");
    mac.update(signing_input.as_bytes());
    let sig = mac.finalize().into_bytes();
    let sig_b64 = URL_SAFE_NO_PAD.encode(sig);

    format!("{}.{}.{}", header_b64, payload_b64, sig_b64)
}

fn headers(account_id: &str, idempotency_key: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let auth = format!("Bearer {}", make_token(account_id));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&auth).expect("authorization header"),
    );
    if let Some(key) = idempotency_key {
        headers.insert(
            "Idempotency-Key",
            HeaderValue::from_str(key).expect("idem header"),
        );
    }
    headers
}

fn request_with_client_id(client_order_id: &str) -> OrderRequest {
    OrderRequest {
        symbol: "AAPL".into(),
        side: "BUY".into(),
        order_type: OrderType::Limit,
        qty: 100,
        price: Some(15_000),
        time_in_force: TimeInForce::Gtc,
        expire_at: None,
        client_order_id: Some(client_order_id.to_string()),
        intent_id: None,
        model_id: None,
        execution_run_id: None,
        decision_key: None,
        decision_attempt_seq: None,
    }
}

fn strategy_intent_fixture() -> StrategyIntent {
    let decision_basis_at_ns = now_nanos();
    StrategyIntent {
        schema_version: STRATEGY_INTENT_SCHEMA_VERSION,
        intent_id: "intent-1".to_string(),
        account_id: "acc-1".to_string(),
        session_id: "sess-1".to_string(),
        symbol: "AAPL".to_string(),
        side: "buy".to_string(),
        order_type: OrderType::Limit,
        qty: 100,
        limit_price: Some(15_000),
        time_in_force: TimeInForce::Gtd,
        urgency: IntentUrgency::Normal,
        execution_policy: ExecutionPolicyKind::Aggressive,
        risk_budget_ref: Some(crate::strategy::intent::RiskBudgetRef {
            budget_id: "budget-42".to_string(),
            version: 3,
        }),
        model_id: Some("model-1".to_string()),
        execution_run_id: Some("run-1".to_string()),
        decision_key: Some("decision-1".to_string()),
        decision_attempt_seq: Some(1),
        decision_basis_at_ns: Some(decision_basis_at_ns),
        max_decision_age_ns: Some(60_000_000_000),
        market_snapshot_id: Some("market-1".to_string()),
        signal_id: Some("signal-1".to_string()),
        recovery_policy: None,
        algo: None,
        created_at_ns: 10,
        expires_at_ns: 123_000_000,
    }
}

fn twap_strategy_intent_fixture(start_at_ns: u64) -> StrategyIntent {
    let mut intent = strategy_intent_fixture();
    intent.execution_policy = ExecutionPolicyKind::Twap;
    intent.time_in_force = TimeInForce::Gtc;
    intent.algo = Some(AlgoExecutionSpec {
        slice_count: Some(4),
        slice_interval_ns: Some(1),
        volume_curve_bps: vec![],
        expected_market_volume: vec![],
        participation_target_bps: None,
        start_at_ns: Some(start_at_ns),
    });
    intent
}

fn replay_runtime_fixture(child_count: usize, start_at_ns: u64) -> AlgoParentExecution {
    let qty_per_child = 100 / child_count.max(1) as u64;
    let slices = (0..child_count)
        .map(|idx| AlgoExecutionSlice {
            child_intent_id: format!("intent-1::child-{:02}", idx + 1),
            sequence: idx as u32 + 1,
            qty: qty_per_child,
            send_at_ns: start_at_ns.saturating_add(idx as u64),
            weight_bps: None,
            expected_market_volume: None,
            participation_target_bps: None,
        })
        .collect::<Vec<_>>();
    AlgoParentExecution::from_plan(
        &AlgoExecutionPlan {
            schema_version: STRATEGY_ALGO_PLAN_SCHEMA_VERSION,
            parent_intent_id: "intent-1".to_string(),
            policy: ExecutionPolicyKind::Twap,
            total_qty: 100,
            child_count: slices.len() as u32,
            start_at_ns,
            slice_interval_ns: 1,
            slices,
        },
        "acc-1".to_string(),
        "sess-1".to_string(),
        "AAPL".to_string(),
        Some("model-1".to_string()),
        Some("run-1".to_string()),
        AlgoRuntimeMode::GatewayManagedResume,
        OrderRequest {
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 100,
            price: Some(15_000),
            time_in_force: TimeInForce::Gtc,
            expire_at: None,
            client_order_id: None,
            intent_id: Some("intent-1".to_string()),
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-1".to_string()),
            decision_attempt_seq: Some(1),
        },
        Some("budget-42".to_string()),
        None,
        None,
        start_at_ns.saturating_sub(10),
    )
}

#[test]
fn build_idempotency_key_falls_back_to_decision_metadata() {
    let headers = HeaderMap::new();
    let req = OrderRequest {
        symbol: "AAPL".into(),
        side: "BUY".into(),
        order_type: OrderType::Limit,
        qty: 100,
        price: Some(15_000),
        time_in_force: TimeInForce::Gtc,
        expire_at: None,
        client_order_id: None,
        intent_id: Some("intent-1".to_string()),
        model_id: Some("model-1".to_string()),
        execution_run_id: Some("run-1".to_string()),
        decision_key: Some("dec-7".to_string()),
        decision_attempt_seq: Some(2),
    };

    assert_eq!(
        build_idempotency_key(&headers, &req).as_deref(),
        Some("decision:run-1:dec-7:2")
    );
}

#[test]
fn validate_order_request_rejects_invalid_decision_metadata() {
    let mut req = request_with_client_id("cid-decision");
    req.decision_attempt_seq = Some(0);
    assert_eq!(
        validate_order_request(&req),
        Some("INVALID_DECISION_ATTEMPT")
    );

    let mut req = request_with_client_id("cid-decision");
    req.execution_run_id = Some("run-1".to_string());
    req.decision_key = Some(" ".to_string());
    assert_eq!(validate_order_request(&req), Some("INVALID_DECISION_KEY"));

    let mut req = request_with_client_id("cid-decision");
    req.decision_attempt_seq = Some(1);
    assert_eq!(validate_order_request(&req), Some("DECISION_KEY_REQUIRED"));
}

fn strategy_runtime_snapshot_line(runtime: &AlgoParentExecution, at: u64) -> String {
    serde_json::to_string(&AuditEvent {
        event_type: STRATEGY_ALGO_RUNTIME_SNAPSHOT_EVENT_TYPE.to_string(),
        at,
        account_id: runtime.account_id.clone(),
        order_id: None,
        data: serde_json::to_value(runtime).expect("serialize runtime"),
    })
    .expect("serialize runtime event")
}

fn wait_for_feedback_lines(path: &PathBuf, min_lines: usize) -> String {
    for _ in 0..100 {
        if let Ok(raw) = fs::read_to_string(path) {
            if raw.lines().count() >= min_lines {
                return raw;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("feedback lines not written in time");
}

fn v3_tcp_request(
    jwt_token: &str,
    symbol: &str,
    side: u8,
    order_type: u8,
    qty: u64,
    price: u64,
) -> [u8; V3_TCP_REQUEST_SIZE] {
    v3_tcp_request_with_metadata(jwt_token, symbol, side, order_type, qty, price, None, None)
}

fn v3_tcp_request_with_metadata(
    jwt_token: &str,
    symbol: &str,
    side: u8,
    order_type: u8,
    qty: u64,
    price: u64,
    intent_id: Option<&str>,
    model_id: Option<&str>,
) -> [u8; V3_TCP_REQUEST_SIZE] {
    let mut frame = [0u8; V3_TCP_REQUEST_SIZE];
    let token_bytes = jwt_token.as_bytes();
    let token_len = token_bytes.len().min(V3_TCP_TOKEN_MAX_LEN);
    frame[0..2].copy_from_slice(&(token_len as u16).to_le_bytes());
    frame[V3_TCP_TOKEN_OFFSET..(V3_TCP_TOKEN_OFFSET + token_len)]
        .copy_from_slice(&token_bytes[..token_len]);
    if token_len == 0 {
        let intent_bytes = intent_id.unwrap_or_default().as_bytes();
        let model_bytes = model_id.unwrap_or_default().as_bytes();
        let intent_len = intent_bytes.len().min(V3_TCP_TOKEN_MAX_LEN);
        let model_len = model_bytes
            .len()
            .min(V3_TCP_TOKEN_MAX_LEN.saturating_sub(intent_len));
        frame[V3_TCP_INTENT_LEN_OFFSET..(V3_TCP_INTENT_LEN_OFFSET + 2)]
            .copy_from_slice(&(intent_len as u16).to_le_bytes());
        frame[V3_TCP_MODEL_LEN_OFFSET..(V3_TCP_MODEL_LEN_OFFSET + 2)]
            .copy_from_slice(&(model_len as u16).to_le_bytes());
        frame[V3_TCP_TOKEN_OFFSET..(V3_TCP_TOKEN_OFFSET + intent_len)]
            .copy_from_slice(&intent_bytes[..intent_len]);
        let model_offset = V3_TCP_TOKEN_OFFSET + intent_len;
        frame[model_offset..(model_offset + model_len)].copy_from_slice(&model_bytes[..model_len]);
    }
    let symbol_bytes = symbol.as_bytes();
    let copy_len = symbol_bytes.len().min(V3_TCP_SYMBOL_LEN);
    frame[V3_TCP_SYMBOL_OFFSET..(V3_TCP_SYMBOL_OFFSET + copy_len)]
        .copy_from_slice(&symbol_bytes[..copy_len]);
    frame[V3_TCP_SIDE_OFFSET] = side;
    frame[V3_TCP_TYPE_OFFSET] = order_type;
    frame[V3_TCP_QTY_OFFSET..(V3_TCP_QTY_OFFSET + 8)].copy_from_slice(&qty.to_le_bytes());
    frame[V3_TCP_PRICE_OFFSET..(V3_TCP_PRICE_OFFSET + 8)].copy_from_slice(&price.to_le_bytes());
    frame
}

fn v3_tcp_auth_init_frame(jwt_token: &str) -> [u8; V3_TCP_REQUEST_SIZE] {
    let mut frame = [0u8; V3_TCP_REQUEST_SIZE];
    let token_bytes = jwt_token.as_bytes();
    let token_len = token_bytes.len().min(V3_TCP_TOKEN_MAX_LEN);
    frame[0..2].copy_from_slice(&(token_len as u16).to_le_bytes());
    frame[V3_TCP_TOKEN_OFFSET..(V3_TCP_TOKEN_OFFSET + token_len)]
        .copy_from_slice(&token_bytes[..token_len]);
    frame
}

fn put_order(
    state: &AppState,
    order_id: &str,
    account_id: &str,
    client_order_id: Option<&str>,
    idempotency_key: Option<&str>,
) {
    let order = OrderSnapshot::new(
        order_id.to_string(),
        account_id.to_string(),
        "AAPL".into(),
        "BUY".into(),
        OrderType::Limit,
        100,
        Some(15_000),
        TimeInForce::Gtc,
        None,
        client_order_id.map(|v| v.to_string()),
    );
    state.sharded_store.put(order, idempotency_key);
    state.order_id_map.register_with_internal(
        state.order_id_seq.fetch_add(1, Ordering::Relaxed),
        order_id.to_string(),
        account_id.to_string(),
    );
}

fn find_fixture(rel: &str) -> PathBuf {
    let mut dir = std::env::current_dir().expect("cwd");
    for _ in 0..6 {
        let candidate = dir.join(rel);
        if candidate.exists() {
            return candidate;
        }
        if !dir.pop() {
            break;
        }
    }
    panic!("fixture not found: {rel}");
}

fn load_fixture<T: DeserializeOwned>(rel: &str) -> T {
    let path = find_fixture(rel);
    let raw = fs::read_to_string(path).expect("read fixture");
    serde_json::from_str(&raw).expect("deserialize fixture")
}

#[derive(Debug, Deserialize)]
struct RiskDecisionFixture {
    cases: Vec<RiskDecisionCase>,
}

#[derive(Debug, Deserialize)]
struct RiskDecisionCase {
    id: String,
    input: RiskDecisionInput,
    expected: RiskDecisionExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RiskDecisionInput {
    symbol: String,
    side: String,
    order_type: String,
    qty: u64,
    price: u64,
    strict_symbols: bool,
    symbol_known: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RiskDecisionExpected {
    allowed: bool,
    reason: Option<String>,
    http_status: u16,
}

#[derive(Debug, Deserialize)]
struct RejectReasonFixture {
    reasons: Vec<RejectReasonCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RejectReasonCase {
    reason: String,
    tcp_reason_code: u32,
}

#[derive(Debug, Deserialize)]
struct AmendDecisionFixture {
    cases: Vec<AmendDecisionCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AmendDecisionCase {
    id: String,
    scope: String,
    from_status: String,
    input: AmendDecisionInput,
    expected: AmendDecisionExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AmendDecisionInput {
    new_qty: u64,
    new_price: u64,
    comment: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AmendDecisionExpected {
    allowed: bool,
    reason: Option<String>,
    http_status: u16,
    next_status: String,
}

#[derive(Debug, Deserialize)]
struct TifPolicyFixture {
    cases: Vec<TifPolicyCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TifPolicyCase {
    id: String,
    scope: String,
    input: TifPolicyInput,
    expected: TifPolicyExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TifPolicyInput {
    order_type: String,
    time_in_force: String,
    price: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TifPolicyExpected {
    allowed: bool,
    reason: Option<String>,
    http_status: u16,
    effective_time_in_force: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PositionCapFixture {
    cases: Vec<PositionCapCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionCapCase {
    id: String,
    scope: String,
    input: PositionCapInput,
    expected: PositionCapExpected,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionCapInput {
    symbol: String,
    side: String,
    qty: u64,
    current_position_qty: i64,
    max_abs_position_qty: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionCapExpected {
    allowed: bool,
    reason: Option<String>,
    http_status: u16,
    projected_position_qty: i64,
}

fn fixture_order_type(raw: &str) -> OrderType {
    match raw {
        "LIMIT" => OrderType::Limit,
        "MARKET" => OrderType::Market,
        other => panic!("unknown orderType in fixture: {other}"),
    }
}

fn fixture_time_in_force(raw: &str) -> TimeInForce {
    match raw {
        "GTC" => TimeInForce::Gtc,
        "GTD" => TimeInForce::Gtd,
        "IOC" => TimeInForce::Ioc,
        "FOK" => TimeInForce::Fok,
        other => panic!("unknown timeInForce in fixture: {other}"),
    }
}

fn fixture_projected_position_qty(side: &str, current_position_qty: i64, qty: u64) -> i64 {
    match side {
        "BUY" => current_position_qty.saturating_add(qty as i64),
        "SELL" => current_position_qty.saturating_sub(qty as i64),
        _ => current_position_qty,
    }
}

fn fixture_order_status(raw: &str) -> OrderStatus {
    match raw {
        "ACCEPTED" => OrderStatus::Accepted,
        "SENT" => OrderStatus::Sent,
        "AMEND_REQUESTED" => OrderStatus::AmendRequested,
        "CANCEL_REQUESTED" => OrderStatus::CancelRequested,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        other => panic!("unknown fromStatus in fixture: {other}"),
    }
}

mod classic;
mod fixtures;
mod strategy;
mod v3;
