//! 運用API（観測の入口）:
//! - 役割: FastPathの稼働状態とSLO指標を取得する。
//! - 位置: 運用監視のための読み取り専用パス。
//! - 内包: health と Prometheus metrics の出力。

use axum::{Json, extract::State};
use std::sync::atomic::Ordering;

use crate::order::HealthResponse;

use super::AppState;

// ヘルス/メトリクス出力: Prometheus向けのスナップショットを生成。

/// ヘルスチェック（GET /health）
/// - キュー長と主要レイテンシを返却
pub(super) async fn handle_health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "OK".into(),
        queue_len: state.engine.queue_len(),
        latency_p50_ns: state.engine.latency_p50(),
        latency_p99_ns: state.engine.latency_p99(),
    })
}

/// メトリクス（GET /metrics）
/// - Bus/Outbox/Idempotency/Reject/Latency をPrometheus形式で出力
pub(super) async fn handle_metrics(State(state): State<AppState>) -> String {
    let bus_metrics = state.bus_publisher.metrics();
    let outbox_metrics = crate::outbox::metrics();
    let idempotency_hits = state.idempotency_hits.load(Ordering::Relaxed);
    let idempotency_creates = state.idempotency_creates.load(Ordering::Relaxed);
    let idempotency_expired = state.sharded_store.idempotency_expired_total();
    let idempotency_checked = state.idempotency_checked.load(Ordering::Relaxed);
    let idempotency_handled = idempotency_hits + idempotency_creates + idempotency_expired;
    let idempotency_handled_ratio = if idempotency_checked == 0 {
        0.0
    } else {
        idempotency_handled as f64 / idempotency_checked as f64
    };
    let idempotency_expired_ratio = if idempotency_checked == 0 {
        0.0
    } else {
        idempotency_expired as f64 / idempotency_checked as f64
    };
    let bus_enabled = if bus_metrics.enabled { 1 } else { 0 };
    let reject_invalid_qty = state.reject_invalid_qty.load(Ordering::Relaxed);
    let reject_rate_limit = state.reject_rate_limit.load(Ordering::Relaxed);
    let reject_risk = state.reject_risk.load(Ordering::Relaxed);
    let reject_invalid_symbol = state.reject_invalid_symbol.load(Ordering::Relaxed);
    let reject_queue_full = state.reject_queue_full.load(Ordering::Relaxed);
    let v2_requests_total = state.v2_requests_total.load(Ordering::Relaxed);
    let v2_durable_wait_timeout_total = state.v2_durable_wait_timeout_total.load(Ordering::Relaxed);
    let v2_durable_wait_timeout_ratio = if v2_requests_total == 0 {
        0.0
    } else {
        v2_durable_wait_timeout_total as f64 / v2_requests_total as f64
    };
    let backpressure_soft_wal_age = state.backpressure_soft_wal_age.load(Ordering::Relaxed);
    let backpressure_soft_rate_decline =
        state.backpressure_soft_rate_decline.load(Ordering::Relaxed);
    let backpressure_inflight = state.backpressure_inflight.load(Ordering::Relaxed);
    let backpressure_wal_bytes = state.backpressure_wal_bytes.load(Ordering::Relaxed);
    let backpressure_wal_age = state.backpressure_wal_age.load(Ordering::Relaxed);
    let backpressure_disk_free = state.backpressure_disk_free.load(Ordering::Relaxed);
    let v3_accepted_total = state.v3_accepted_total.load(Ordering::Relaxed);
    let v3_rejected_soft_total = state.v3_rejected_soft_total.load(Ordering::Relaxed);
    let v3_rejected_hard_total = state.v3_rejected_hard_total.load(Ordering::Relaxed);
    let v3_rejected_killed_total = state.v3_rejected_killed_total.load(Ordering::Relaxed);
    let v3_queue_depth = state.v3_ingress.total_depth();
    let v3_queue_capacity = state
        .v3_ingress
        .max_depth_per_shard()
        .saturating_mul(state.v3_ingress.shard_count() as u64);
    let v3_soft_reject_pct = state.v3_soft_reject_pct;
    let v3_hard_reject_pct = state.v3_hard_reject_pct;
    let v3_kill_reject_pct = state.v3_kill_reject_pct;
    let v3_queue_utilization_pct = state.v3_ingress.queue_utilization_pct_max() as f64;
    let v3_kill_switch = if state.v3_ingress.is_global_killed() {
        1
    } else {
        0
    };
    let v3_shard_kill_switches = state.v3_ingress.shard_kill_switch_count();
    let v3_kill_auto_recover = if state.v3_ingress.kill_auto_recover_enabled() {
        1
    } else {
        0
    };
    let v3_kill_recover_pct = state.v3_ingress.kill_recover_pct();
    let v3_kill_recover_after_ms = state.v3_ingress.kill_recover_after_ms();
    let v3_kill_recovered_total = state.v3_kill_recovered_total.load(Ordering::Relaxed);
    let v3_loss_suspect_total = state.v3_loss_suspect_total.load(Ordering::Relaxed);
    let v3_session_loss_suspect_threshold =
        state.v3_ingress.session_loss_suspect_threshold() as u64;
    let v3_session_killed_total = state.v3_session_killed_total.load(Ordering::Relaxed);
    let v3_shard_killed_total = state.v3_shard_killed_total.load(Ordering::Relaxed);
    let v3_global_killed_total = state.v3_global_killed_total.load(Ordering::Relaxed);
    let v3_durable_accepted_total = state.v3_durable_accepted_total.load(Ordering::Relaxed);
    let v3_durable_rejected_total = state.v3_durable_rejected_total.load(Ordering::Relaxed);
    let v3_durable_queue_depth = state.v3_durable_ingress.total_depth();
    let v3_durable_queue_capacity = state.v3_durable_ingress.total_capacity();
    let v3_durable_queue_utilization_pct = if v3_durable_queue_capacity == 0 {
        0.0
    } else {
        (v3_durable_queue_depth as f64 * 100.0) / v3_durable_queue_capacity as f64
    };
    let v3_durable_queue_utilization_pct_max = state.v3_durable_ingress.queue_utilization_pct_max();
    let v3_durable_lane_skew_pct = state.v3_durable_ingress.lane_skew_pct();
    let v3_durable_lane_depths = state.v3_durable_ingress.lane_depths();
    let v3_durable_lane_utils = state.v3_durable_ingress.lane_utilization_pcts();
    let v3_durable_lanes = v3_durable_lane_depths.len() as u64;
    let v3_durable_queue_full_total = state.v3_durable_ingress.queue_full_total();
    let v3_durable_queue_closed_total = state.v3_durable_ingress.queue_closed_total();
    let v3_durable_worker_processed_total = state.v3_durable_ingress.processed_total();
    let v3_durable_backlog_growth_per_sec = state
        .v3_durable_backlog_growth_per_sec
        .load(Ordering::Relaxed);
    let v3_durable_write_error_total = state.v3_durable_write_error_total.load(Ordering::Relaxed);
    let v3_durable_receipt_timeout_total = state
        .v3_durable_receipt_timeout_total
        .load(Ordering::Relaxed);
    let v3_durable_receipt_inflight = state.v3_durable_receipt_inflight.load(Ordering::Relaxed);
    let v3_durable_receipt_inflight_max =
        state.v3_durable_receipt_inflight_max.load(Ordering::Relaxed);
    let v3_durable_worker_receipt_timeout_us = state.v3_durable_worker_receipt_timeout_us;
    let v3_durable_worker_max_inflight_receipts = state.v3_durable_worker_max_inflight_receipts;
    let v3_durable_worker_inflight_soft_cap_pct = state.v3_durable_worker_inflight_soft_cap_pct;
    let v3_durable_worker_inflight_hard_cap_pct = state.v3_durable_worker_inflight_hard_cap_pct;
    let v3_durable_backpressure_soft_total = state
        .v3_durable_backpressure_soft_total
        .load(Ordering::Relaxed);
    let v3_durable_backpressure_hard_total = state
        .v3_durable_backpressure_hard_total
        .load(Ordering::Relaxed);
    let v3_durable_admission_controller_enabled = if state.v3_durable_admission_controller_enabled {
        1
    } else {
        0
    };
    let v3_durable_admission_level = state.v3_durable_admission_level.load(Ordering::Relaxed);
    let v3_durable_admission_level_per_lane = state
        .v3_durable_admission_level_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_soft_trip_total = state
        .v3_durable_admission_soft_trip_total
        .load(Ordering::Relaxed);
    let v3_durable_admission_soft_trip_total_per_lane = state
        .v3_durable_admission_soft_trip_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_hard_trip_total = state
        .v3_durable_admission_hard_trip_total
        .load(Ordering::Relaxed);
    let v3_durable_admission_hard_trip_total_per_lane = state
        .v3_durable_admission_hard_trip_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_signal_queue_soft_total_per_lane = state
        .v3_durable_admission_signal_queue_soft_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_signal_queue_hard_total_per_lane = state
        .v3_durable_admission_signal_queue_hard_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_signal_backlog_soft_total_per_lane = state
        .v3_durable_admission_signal_backlog_soft_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_signal_backlog_hard_total_per_lane = state
        .v3_durable_admission_signal_backlog_hard_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_signal_fsync_soft_total_per_lane = state
        .v3_durable_admission_signal_fsync_soft_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_signal_fsync_hard_total_per_lane = state
        .v3_durable_admission_signal_fsync_hard_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_admission_sustain_ticks = state.v3_durable_admission_sustain_ticks;
    let v3_durable_admission_recover_ticks = state.v3_durable_admission_recover_ticks;
    let v3_durable_admission_soft_fsync_p99_us = state.v3_durable_admission_soft_fsync_p99_us;
    let v3_durable_admission_hard_fsync_p99_us = state.v3_durable_admission_hard_fsync_p99_us;
    let v3_durable_soft_reject_pct = state.v3_durable_soft_reject_pct;
    let v3_durable_hard_reject_pct = state.v3_durable_hard_reject_pct;
    let v3_durable_backlog_soft_reject_per_sec = state.v3_durable_backlog_soft_reject_per_sec;
    let v3_durable_backlog_hard_reject_per_sec = state.v3_durable_backlog_hard_reject_per_sec;
    let v3_durable_backlog_signal_min_queue_pct = state.v3_durable_backlog_signal_min_queue_pct;
    let v3_durable_admission_fsync_presignal_pct = state.v3_durable_admission_fsync_presignal_pct;
    let v3_durable_wal_append_p50 = state.v3_durable_wal_append_hist.snapshot().percentile(50.0);
    let v3_durable_wal_append_p99 = state.v3_durable_wal_append_hist.snapshot().percentile(99.0);
    let v3_durable_fsync_p50 = state.v3_durable_wal_fsync_hist.snapshot().percentile(50.0);
    let v3_durable_fsync_p99 = state.v3_durable_wal_fsync_hist.snapshot().percentile(99.0);
    let v3_durable_worker_loop_p50 = state
        .v3_durable_worker_loop_hist
        .snapshot()
        .percentile(50.0);
    let v3_durable_worker_loop_p99 = state
        .v3_durable_worker_loop_hist
        .snapshot()
        .percentile(99.0);
    let v3_durable_worker_batch_adaptive = if state.v3_durable_worker_batch_adaptive {
        1
    } else {
        0
    };
    let v3_durable_backlog_growth_per_sec_per_lane = state
        .v3_durable_backlog_growth_per_sec_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_backpressure_soft_total_per_lane = state
        .v3_durable_backpressure_soft_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_backpressure_hard_total_per_lane = state
        .v3_durable_backpressure_hard_total_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_durable_fsync_p99_per_lane = state
        .v3_durable_wal_fsync_hist_per_lane
        .iter()
        .map(|hist| hist.snapshot().percentile(99.0))
        .collect::<Vec<_>>();
    let v3_durable_worker_loop_p99_per_lane = state
        .v3_durable_worker_loop_hist_per_lane
        .iter()
        .map(|hist| hist.snapshot().percentile(99.0))
        .collect::<Vec<_>>();
    let v3_processed_total = state.v3_ingress.processed_total();
    let v3_live_ack_p99 = state.v3_live_ack_hist.snapshot().percentile(99.0);
    let v3_live_ack_accepted_p99 = state.v3_live_ack_accepted_hist.snapshot().percentile(99.0);
    let v3_durable_confirm_p99 = state.v3_durable_confirm_hist.snapshot().percentile(99.0);
    let v3_total = v3_accepted_total
        + v3_rejected_soft_total
        + v3_rejected_hard_total
        + v3_rejected_killed_total;
    let v3_accepted_rate = if v3_total == 0 {
        0.0
    } else {
        v3_accepted_total as f64 / v3_total as f64
    };
    let v3_risk_profile_level = state.v3_risk_profile.as_metric_level();
    let v3_risk_margin_mode_level = state.v3_risk_margin_mode.as_metric_level();
    let v3_risk_profile_loops = state.v3_risk_loops;
    let v3_risk_strict_symbols = if state.v3_risk_strict_symbols { 1 } else { 0 };
    let v3_risk_max_order_qty = state.v3_risk_max_order_qty;
    let v3_risk_max_notional = state.v3_risk_max_notional;
    let v3_risk_daily_notional_limit = state.v3_risk_daily_notional_limit;
    let v3_risk_max_abs_position_qty = state.v3_risk_max_abs_position_qty;
    let v3_symbol_limits_count = state.v3_symbol_limits.len();
    let v3_account_daily_notional_count = state.v3_account_daily_notional.len();
    let v3_account_symbol_position_count = state.v3_account_symbol_position.len();
    let v3_stage_parse_p50 = state.v3_stage_parse_hist.snapshot().percentile(50.0);
    let v3_stage_parse_p99 = state.v3_stage_parse_hist.snapshot().percentile(99.0);
    let v3_stage_risk_p50 = state.v3_stage_risk_hist.snapshot().percentile(50.0);
    let v3_stage_risk_p99 = state.v3_stage_risk_hist.snapshot().percentile(99.0);
    let v3_stage_risk_position_p50 = state
        .v3_stage_risk_position_hist
        .snapshot()
        .percentile(50.0);
    let v3_stage_risk_position_p99 = state
        .v3_stage_risk_position_hist
        .snapshot()
        .percentile(99.0);
    let v3_stage_risk_margin_p50 = state.v3_stage_risk_margin_hist.snapshot().percentile(50.0);
    let v3_stage_risk_margin_p99 = state.v3_stage_risk_margin_hist.snapshot().percentile(99.0);
    let v3_stage_risk_limits_p50 = state.v3_stage_risk_limits_hist.snapshot().percentile(50.0);
    let v3_stage_risk_limits_p99 = state.v3_stage_risk_limits_hist.snapshot().percentile(99.0);
    let v3_stage_enqueue_p50 = state.v3_stage_enqueue_hist.snapshot().percentile(50.0);
    let v3_stage_enqueue_p99 = state.v3_stage_enqueue_hist.snapshot().percentile(99.0);
    let v3_stage_serialize_p50 = state.v3_stage_serialize_hist.snapshot().percentile(50.0);
    let v3_stage_serialize_p99 = state.v3_stage_serialize_hist.snapshot().percentile(99.0);
    let v3_confirm_store_size = state.v3_confirm_store.total_size();
    let v3_confirm_store_lanes = state.v3_confirm_store.lane_count_metric();
    let v3_confirm_lane_skew_pct = state.v3_confirm_store.lane_skew_pct();
    let v3_confirm_oldest_inflight_us = state.v3_confirm_oldest_inflight_us.load(Ordering::Relaxed);
    let v3_confirm_oldest_inflight_us_per_lane = state
        .v3_confirm_oldest_inflight_us_per_lane
        .iter()
        .map(|v| v.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let v3_confirm_age_p99_per_lane = state
        .v3_confirm_age_hist_per_lane
        .iter()
        .map(|hist| hist.snapshot().percentile(99.0))
        .collect::<Vec<_>>();
    let v3_confirm_timeout_scan_cost_last = state
        .v3_confirm_timeout_scan_cost_last
        .load(Ordering::Relaxed);
    let v3_confirm_timeout_scan_cost_total = state
        .v3_confirm_timeout_scan_cost_total
        .load(Ordering::Relaxed);
    let v3_confirm_gc_removed_total = state.v3_confirm_gc_removed_total.load(Ordering::Relaxed);
    let v3_confirm_rebuild_restored_total = state
        .v3_confirm_rebuild_restored_total
        .load(Ordering::Relaxed);
    let v3_confirm_rebuild_elapsed_ms = state.v3_confirm_rebuild_elapsed_ms.load(Ordering::Relaxed);
    let v3_durable_confirm_soft_reject_age_us = state.v3_durable_confirm_soft_reject_age_us;
    let v3_durable_confirm_hard_reject_age_us = state.v3_durable_confirm_hard_reject_age_us;
    let v3_durable_confirm_age_soft_reject_total = state
        .v3_durable_confirm_age_soft_reject_total
        .load(Ordering::Relaxed);
    let v3_durable_confirm_age_hard_reject_total = state
        .v3_durable_confirm_age_hard_reject_total
        .load(Ordering::Relaxed);
    let inflight = state.inflight_controller.inflight();
    let durable_inflight = v3_durable_queue_depth;
    let wal_age_ms = state.audit_log.wal_age_ms();
    let inflight_limit_dynamic = state.inflight_controller.current_limit();
    let durable_commit_rate_ewma_milli = state.inflight_controller.rate_ewma_milli();
    let soft_reject_rate_ewma_milli = state.inflight_controller.soft_reject_rate_ewma_milli();
    let inflight_dynamic_enabled = if state.inflight_controller.enabled() {
        1
    } else {
        0
    };
    let ack_p50 = state.ack_hist.snapshot().percentile(50.0);
    let ack_p99 = state.ack_hist.snapshot().percentile(99.0);
    let ack_p999 = state.ack_hist.snapshot().percentile(99.9);
    let wal_enqueue_p50 = state.wal_enqueue_hist.snapshot().percentile(50.0);
    let wal_enqueue_p99 = state.wal_enqueue_hist.snapshot().percentile(99.0);
    let wal_enqueue_p999 = state.wal_enqueue_hist.snapshot().percentile(99.9);
    let durable_ack_p50 = state.durable_ack_hist.snapshot().percentile(50.0);
    let durable_ack_p99 = state.durable_ack_hist.snapshot().percentile(99.0);
    let durable_ack_p999 = state.durable_ack_hist.snapshot().percentile(99.9);
    let fdatasync_p50 = state.fdatasync_hist.snapshot().percentile(50.0);
    let fdatasync_p99 = state.fdatasync_hist.snapshot().percentile(99.0);
    let fdatasync_p999 = state.fdatasync_hist.snapshot().percentile(99.9);
    let durable_notify_p50 = state.durable_notify_hist.snapshot().percentile(50.0);
    let durable_notify_p99 = state.durable_notify_hist.snapshot().percentile(99.0);
    let durable_notify_p999 = state.durable_notify_hist.snapshot().percentile(99.9);
    let fast_path_processing_p50 = state.engine.processing_p50() / 1_000;
    let fast_path_processing_p99 = state.engine.processing_p99() / 1_000;
    let fast_path_processing_p999 = state.engine.processing_p999() / 1_000;

    let mut snapshot = format!(
        "# HELP gateway_queue_len Current queue length\n\
         # TYPE gateway_queue_len gauge\n\
         gateway_queue_len {}\n\
         # HELP gateway_order_store_shards Sharded order store shard count\n\
         # TYPE gateway_order_store_shards gauge\n\
         gateway_order_store_shards {}\n\
         # HELP gateway_kafka_enabled Kafka publish enabled (1/0)\n\
         # TYPE gateway_kafka_enabled gauge\n\
         gateway_kafka_enabled {}\n\
         # HELP gateway_kafka_publish_queued_total Total Kafka publish enqueued\n\
         # TYPE gateway_kafka_publish_queued_total counter\n\
         gateway_kafka_publish_queued_total {}\n\
         # HELP gateway_kafka_delivery_ok_total Total Kafka delivery success\n\
         # TYPE gateway_kafka_delivery_ok_total counter\n\
         gateway_kafka_delivery_ok_total {}\n\
         # HELP gateway_kafka_delivery_err_total Total Kafka delivery failures\n\
         # TYPE gateway_kafka_delivery_err_total counter\n\
         gateway_kafka_delivery_err_total {}\n\
         # HELP gateway_kafka_publish_dropped_total Total Kafka publish dropped\n\
         # TYPE gateway_kafka_publish_dropped_total counter\n\
         gateway_kafka_publish_dropped_total {}\n\
         # HELP gateway_outbox_lines_read_total Total audit lines read by outbox\n\
         # TYPE gateway_outbox_lines_read_total counter\n\
         gateway_outbox_lines_read_total {}\n\
         # HELP gateway_outbox_events_published_total Total outbox events published to bus\n\
         # TYPE gateway_outbox_events_published_total counter\n\
         gateway_outbox_events_published_total {}\n\
         # HELP gateway_outbox_events_skipped_total Total outbox events skipped\n\
         # TYPE gateway_outbox_events_skipped_total counter\n\
         gateway_outbox_events_skipped_total {}\n\
         # HELP gateway_outbox_read_errors_total Total outbox read errors\n\
         # TYPE gateway_outbox_read_errors_total counter\n\
         gateway_outbox_read_errors_total {}\n\
         # HELP gateway_outbox_publish_errors_total Total outbox publish errors\n\
         # TYPE gateway_outbox_publish_errors_total counter\n\
         gateway_outbox_publish_errors_total {}\n\
         # HELP gateway_outbox_offset_resets_total Total outbox offset resets\n\
         # TYPE gateway_outbox_offset_resets_total counter\n\
         gateway_outbox_offset_resets_total {}\n\
         # HELP gateway_outbox_backoff_base_ms Outbox backoff base milliseconds\n\
         # TYPE gateway_outbox_backoff_base_ms gauge\n\
         gateway_outbox_backoff_base_ms {}\n\
         # HELP gateway_outbox_backoff_max_ms Outbox backoff max milliseconds\n\
         # TYPE gateway_outbox_backoff_max_ms gauge\n\
         gateway_outbox_backoff_max_ms {}\n\
         # HELP gateway_outbox_backoff_current_ms Outbox current backoff milliseconds\n\
         # TYPE gateway_outbox_backoff_current_ms gauge\n\
         gateway_outbox_backoff_current_ms {}\n\
         # HELP gateway_idempotency_checked_total Total idempotency checks (requests with Idempotency-Key)\n\
         # TYPE gateway_idempotency_checked_total counter\n\
         gateway_idempotency_checked_total {}\n\
         # HELP gateway_idempotency_hits_total Total idempotency replays returning existing order\n\
         # TYPE gateway_idempotency_hits_total counter\n\
         gateway_idempotency_hits_total {}\n\
         # HELP gateway_idempotency_creates_total Total idempotent order creations\n\
         # TYPE gateway_idempotency_creates_total counter\n\
         gateway_idempotency_creates_total {}\n\
         # HELP gateway_idempotency_expired_total Total idempotency entries expired by TTL\n\
         # TYPE gateway_idempotency_expired_total counter\n\
         gateway_idempotency_expired_total {}\n\
         # HELP gateway_idempotency_handled_ratio Ratio of idempotency requests that were handled\n\
         # TYPE gateway_idempotency_handled_ratio gauge\n\
         gateway_idempotency_handled_ratio {}\n\
         # HELP gateway_idempotency_expired_ratio Ratio of expired idempotency entries\n\
         # TYPE gateway_idempotency_expired_ratio gauge\n\
         gateway_idempotency_expired_ratio {}\n\
         # HELP gateway_reject_invalid_qty_total Total rejects due to invalid quantity\n\
         # TYPE gateway_reject_invalid_qty_total counter\n\
         gateway_reject_invalid_qty_total {}\n\
         # HELP gateway_reject_rate_limit_total Total rejects due to rate limit\n\
         # TYPE gateway_reject_rate_limit_total counter\n\
         gateway_reject_rate_limit_total {}\n\
         # HELP gateway_reject_risk_total Total rejects due to risk limits\n\
         # TYPE gateway_reject_risk_total counter\n\
         gateway_reject_risk_total {}\n\
         # HELP gateway_reject_invalid_symbol_total Total rejects due to invalid symbol\n\
         # TYPE gateway_reject_invalid_symbol_total counter\n\
         gateway_reject_invalid_symbol_total {}\n\
         # HELP gateway_reject_queue_full_total Total rejects due to queue full\n\
         # TYPE gateway_reject_queue_full_total counter\n\
         gateway_reject_queue_full_total {}\n\
         # HELP gateway_v3_accepted_total Total /v3/orders volatile accepted\n\
         # TYPE gateway_v3_accepted_total counter\n\
         gateway_v3_accepted_total {}\n\
         # HELP gateway_v3_rejected_soft_total Total /v3/orders soft rejects\n\
         # TYPE gateway_v3_rejected_soft_total counter\n\
         gateway_v3_rejected_soft_total {}\n\
         # HELP gateway_v3_rejected_killed_total Total /v3/orders killed rejects\n\
         # TYPE gateway_v3_rejected_killed_total counter\n\
         gateway_v3_rejected_killed_total {}\n\
         # HELP gateway_v3_queue_depth Current /v3 ingress queue depth\n\
         # TYPE gateway_v3_queue_depth gauge\n\
         gateway_v3_queue_depth {}\n\
         # HELP gateway_v3_queue_capacity Configured /v3 ingress queue capacity\n\
         # TYPE gateway_v3_queue_capacity gauge\n\
         gateway_v3_queue_capacity {}\n\
         # HELP gateway_v3_soft_reject_pct /v3 soft reject queue threshold percentage\n\
         # TYPE gateway_v3_soft_reject_pct gauge\n\
         gateway_v3_soft_reject_pct {}\n\
         # HELP gateway_v3_kill_reject_pct /v3 kill reject queue threshold percentage\n\
         # TYPE gateway_v3_kill_reject_pct gauge\n\
         gateway_v3_kill_reject_pct {}\n\
         # HELP gateway_v3_queue_utilization_pct /v3 ingress queue utilization percentage\n\
         # TYPE gateway_v3_queue_utilization_pct gauge\n\
         gateway_v3_queue_utilization_pct {}\n\
         # HELP gateway_v3_kill_switch /v3 kill switch status (1/0)\n\
         # TYPE gateway_v3_kill_switch gauge\n\
         gateway_v3_kill_switch {}\n\
         # HELP gateway_v3_processed_total Total /v3 tasks processed by single-writer\n\
         # TYPE gateway_v3_processed_total counter\n\
         gateway_v3_processed_total {}\n\
        # HELP gateway_backpressure_soft_wal_age_total Total soft rejects due to WAL age\n\
        # TYPE gateway_backpressure_soft_wal_age_total counter\n\
        gateway_backpressure_soft_wal_age_total {}\n\
        # HELP gateway_backpressure_soft_rate_decline_total Total soft rejects due to durable rate declining\n\
        # TYPE gateway_backpressure_soft_rate_decline_total counter\n\
        gateway_backpressure_soft_rate_decline_total {}\n\
        # HELP gateway_backpressure_inflight_total Total rejects due to inflight\n\
        # TYPE gateway_backpressure_inflight_total counter\n\
        gateway_backpressure_inflight_total {}\n\
         # HELP gateway_backpressure_wal_bytes_total Total rejects due to WAL bytes\n\
         # TYPE gateway_backpressure_wal_bytes_total counter\n\
         gateway_backpressure_wal_bytes_total {}\n\
         # HELP gateway_backpressure_wal_age_total Total rejects due to WAL age\n\
         # TYPE gateway_backpressure_wal_age_total counter\n\
         gateway_backpressure_wal_age_total {}\n\
         # HELP gateway_backpressure_disk_free_total Total rejects due to low disk free\n\
         # TYPE gateway_backpressure_disk_free_total counter\n\
         gateway_backpressure_disk_free_total {}\n\
        # HELP gateway_inflight Current inflight order count\n\
        # TYPE gateway_inflight gauge\n\
        gateway_inflight {}\n\
        # HELP gateway_inflight_dynamic_enabled Dynamic inflight controller enabled (1/0)\n\
        # TYPE gateway_inflight_dynamic_enabled gauge\n\
        gateway_inflight_dynamic_enabled {}\n\
        # HELP gateway_inflight_limit_dynamic Current dynamic inflight limit\n\
        # TYPE gateway_inflight_limit_dynamic gauge\n\
        gateway_inflight_limit_dynamic {}\n\
        # HELP gateway_backpressure_soft_reject_rate_ewma Soft reject rate EWMA (events/sec)\n\
        # TYPE gateway_backpressure_soft_reject_rate_ewma gauge\n\
        gateway_backpressure_soft_reject_rate_ewma {}\n\
        # HELP gateway_durable_commit_rate_ewma Durable commit rate EWMA (events/sec)\n\
        # TYPE gateway_durable_commit_rate_ewma gauge\n\
        gateway_durable_commit_rate_ewma {}\n\
        # HELP gateway_durable_inflight Current durable inflight count\n\
        # TYPE gateway_durable_inflight gauge\n\
        gateway_durable_inflight {}\n\
         # HELP gateway_wal_age_ms WAL age in milliseconds\n\
         # TYPE gateway_wal_age_ms gauge\n\
         gateway_wal_age_ms {}\n\
         # HELP gateway_ack_p50_us ACK latency p50 in microseconds\n\
         # TYPE gateway_ack_p50_us gauge\n\
         gateway_ack_p50_us {}\n\
         # HELP gateway_ack_p99_us ACK latency p99 in microseconds\n\
         # TYPE gateway_ack_p99_us gauge\n\
         gateway_ack_p99_us {}\n\
         # HELP gateway_ack_p999_us ACK latency p999 in microseconds\n\
         # TYPE gateway_ack_p999_us gauge\n\
         gateway_ack_p999_us {}\n\
         # HELP gateway_wal_enqueue_p50_us WAL enqueue latency p50 in microseconds\n\
         # TYPE gateway_wal_enqueue_p50_us gauge\n\
         gateway_wal_enqueue_p50_us {}\n\
         # HELP gateway_wal_enqueue_p99_us WAL enqueue latency p99 in microseconds\n\
         # TYPE gateway_wal_enqueue_p99_us gauge\n\
         gateway_wal_enqueue_p99_us {}\n\
         # HELP gateway_wal_enqueue_p999_us WAL enqueue latency p999 in microseconds\n\
         # TYPE gateway_wal_enqueue_p999_us gauge\n\
         gateway_wal_enqueue_p999_us {}\n\
         # HELP gateway_durable_ack_p50_us Durable ACK latency p50 in microseconds\n\
         # TYPE gateway_durable_ack_p50_us gauge\n\
         gateway_durable_ack_p50_us {}\n\
         # HELP gateway_durable_ack_p99_us Durable ACK latency p99 in microseconds\n\
         # TYPE gateway_durable_ack_p99_us gauge\n\
         gateway_durable_ack_p99_us {}\n\
         # HELP gateway_durable_ack_p999_us Durable ACK latency p999 in microseconds\n\
         # TYPE gateway_durable_ack_p999_us gauge\n\
         gateway_durable_ack_p999_us {}\n\
         # HELP gateway_fdatasync_p50_us fdatasync latency p50 in microseconds\n\
         # TYPE gateway_fdatasync_p50_us gauge\n\
         gateway_fdatasync_p50_us {}\n\
         # HELP gateway_fdatasync_p99_us fdatasync latency p99 in microseconds\n\
         # TYPE gateway_fdatasync_p99_us gauge\n\
         gateway_fdatasync_p99_us {}\n\
         # HELP gateway_fdatasync_p999_us fdatasync latency p999 in microseconds\n\
         # TYPE gateway_fdatasync_p999_us gauge\n\
         gateway_fdatasync_p999_us {}\n\
         # HELP gateway_durable_notify_p50_us Durable notify latency p50 in microseconds\n\
         # TYPE gateway_durable_notify_p50_us gauge\n\
         gateway_durable_notify_p50_us {}\n\
         # HELP gateway_durable_notify_p99_us Durable notify latency p99 in microseconds\n\
         # TYPE gateway_durable_notify_p99_us gauge\n\
         gateway_durable_notify_p99_us {}\n\
         # HELP gateway_durable_notify_p999_us Durable notify latency p999 in microseconds\n\
         # TYPE gateway_durable_notify_p999_us gauge\n\
         gateway_durable_notify_p999_us {}\n\
         # HELP gateway_fast_path_processing_p50_us Fast path risk processing latency p50 in microseconds\n\
         # TYPE gateway_fast_path_processing_p50_us gauge\n\
         gateway_fast_path_processing_p50_us {}\n\
         # HELP gateway_fast_path_processing_p99_us Fast path risk processing latency p99 in microseconds\n\
         # TYPE gateway_fast_path_processing_p99_us gauge\n\
         gateway_fast_path_processing_p99_us {}\n\
         # HELP gateway_fast_path_processing_p999_us Fast path risk processing latency p999 in microseconds\n\
         # TYPE gateway_fast_path_processing_p999_us gauge\n\
         gateway_fast_path_processing_p999_us {}\n\
         # HELP gateway_latency_p50_ns Latency p50 in nanoseconds\n\
         # TYPE gateway_latency_p50_ns gauge\n\
         gateway_latency_p50_ns {}\n\
         # HELP gateway_latency_p95_ns Latency p95 in nanoseconds\n\
         # TYPE gateway_latency_p95_ns gauge\n\
         gateway_latency_p95_ns {}\n\
         # HELP gateway_latency_p99_ns Latency p99 in nanoseconds\n\
         # TYPE gateway_latency_p99_ns gauge\n\
         gateway_latency_p99_ns {}\n\
         # HELP gateway_latency_p999_ns Latency p999 in nanoseconds\n\
         # TYPE gateway_latency_p999_ns gauge\n\
         gateway_latency_p999_ns {}\n\
         # HELP gateway_latency_mean_ns Latency mean in nanoseconds\n\
         # TYPE gateway_latency_mean_ns gauge\n\
         gateway_latency_mean_ns {}\n\
         # HELP gateway_latency_count_total Latency sample count\n\
         # TYPE gateway_latency_count_total counter\n\
         gateway_latency_count_total {}\n\
         # HELP gateway_latency_max_ns Latency max in nanoseconds\n\
         # TYPE gateway_latency_max_ns gauge\n\
         gateway_latency_max_ns {}\n",
        state.engine.queue_len(),
        state.sharded_store.shard_count(),
        bus_enabled,
        bus_metrics.publish_queued,
        bus_metrics.publish_delivery_ok,
        bus_metrics.publish_delivery_err,
        bus_metrics.publish_dropped,
        outbox_metrics.lines_read,
        outbox_metrics.events_published,
        outbox_metrics.events_skipped,
        outbox_metrics.read_errors,
        outbox_metrics.publish_errors,
        outbox_metrics.offset_resets,
        outbox_metrics.backoff_base_ms,
        outbox_metrics.backoff_max_ms,
        outbox_metrics.backoff_current_ms,
        idempotency_checked,
        idempotency_hits,
        idempotency_creates,
        idempotency_expired,
        idempotency_handled_ratio,
        idempotency_expired_ratio,
        reject_invalid_qty,
        reject_rate_limit,
        reject_risk,
        reject_invalid_symbol,
        reject_queue_full,
        v3_accepted_total,
        v3_rejected_soft_total,
        v3_rejected_killed_total,
        v3_queue_depth,
        v3_queue_capacity,
        v3_soft_reject_pct,
        v3_kill_reject_pct,
        v3_queue_utilization_pct,
        v3_kill_switch,
        v3_processed_total,
        backpressure_soft_wal_age,
        backpressure_soft_rate_decline,
        backpressure_inflight,
        backpressure_wal_bytes,
        backpressure_wal_age,
        backpressure_disk_free,
        inflight,
        inflight_dynamic_enabled,
        inflight_limit_dynamic,
        (soft_reject_rate_ewma_milli as f64) / 1000.0,
        (durable_commit_rate_ewma_milli as f64) / 1000.0,
        durable_inflight,
        wal_age_ms,
        ack_p50,
        ack_p99,
        ack_p999,
        wal_enqueue_p50,
        wal_enqueue_p99,
        wal_enqueue_p999,
        durable_ack_p50,
        durable_ack_p99,
        durable_ack_p999,
        fdatasync_p50,
        fdatasync_p99,
        fdatasync_p999,
        durable_notify_p50,
        durable_notify_p99,
        durable_notify_p999,
        fast_path_processing_p50,
        fast_path_processing_p99,
        fast_path_processing_p999,
        state.engine.latency_p50(),
        state.engine.latency_p95(),
        state.engine.latency_p99(),
        state.engine.latency_p999(),
        state.engine.latency_mean(),
        state.engine.latency_count(),
        state.engine.latency_max(),
    );
    snapshot.push_str(&format!(
        "# HELP gateway_live_ack_p99_us Contract live ACK latency p99 in microseconds (/v3 hot path)\n\
         # TYPE gateway_live_ack_p99_us gauge\n\
         gateway_live_ack_p99_us {}\n\
         # HELP gateway_live_ack_accepted_p99_us Contract live ACK latency p99 in microseconds (accepted-only /v3 hot path)\n\
         # TYPE gateway_live_ack_accepted_p99_us gauge\n\
         gateway_live_ack_accepted_p99_us {}\n\
         # HELP gateway_v3_live_ack_p99_us /v3 ACK latency p99 in microseconds\n\
         # TYPE gateway_v3_live_ack_p99_us gauge\n\
         gateway_v3_live_ack_p99_us {}\n\
         # HELP gateway_v3_live_ack_accepted_p99_us /v3 ACK latency p99 in microseconds (accepted-only)\n\
         # TYPE gateway_v3_live_ack_accepted_p99_us gauge\n\
         gateway_v3_live_ack_accepted_p99_us {}\n\
         # HELP gateway_v3_durable_confirm_p99_us /v3 durable confirm latency p99 in microseconds\n\
         # TYPE gateway_v3_durable_confirm_p99_us gauge\n\
         gateway_v3_durable_confirm_p99_us {}\n\
         # HELP gateway_v3_accepted_rate /v3 accepted ratio against total responses\n\
         # TYPE gateway_v3_accepted_rate gauge\n\
         gateway_v3_accepted_rate {}\n\
         # HELP gateway_v3_rejected_hard_total Total /v3/orders hard rejects (503)\n\
         # TYPE gateway_v3_rejected_hard_total counter\n\
         gateway_v3_rejected_hard_total {}\n\
         # HELP gateway_v3_hard_reject_pct /v3 hard reject queue threshold percentage\n\
         # TYPE gateway_v3_hard_reject_pct gauge\n\
         gateway_v3_hard_reject_pct {}\n\
         # HELP gateway_v3_shard_kill_switches Number of shards in kill state\n\
         # TYPE gateway_v3_shard_kill_switches gauge\n\
         gateway_v3_shard_kill_switches {}\n\
         # HELP gateway_v3_loss_suspect_total Total /v3 LOSS_SUSPECT detections\n\
         # TYPE gateway_v3_loss_suspect_total counter\n\
         gateway_v3_loss_suspect_total {}\n\
         # HELP gateway_v3_session_loss_suspect_threshold Session-level LOSS_SUSPECT escalation threshold within loss window\n\
         # TYPE gateway_v3_session_loss_suspect_threshold gauge\n\
         gateway_v3_session_loss_suspect_threshold {}\n\
         # HELP gateway_v3_session_killed_total Total session kill escalations\n\
         # TYPE gateway_v3_session_killed_total counter\n\
         gateway_v3_session_killed_total {}\n\
         # HELP gateway_v3_shard_killed_total Total shard kill escalations\n\
         # TYPE gateway_v3_shard_killed_total counter\n\
         gateway_v3_shard_killed_total {}\n\
         # HELP gateway_v3_global_killed_total Total global kill escalations\n\
         # TYPE gateway_v3_global_killed_total counter\n\
         gateway_v3_global_killed_total {}\n\
         # HELP gateway_v3_durable_accepted_total Total /v3 durable accepted outcomes\n\
         # TYPE gateway_v3_durable_accepted_total counter\n\
         gateway_v3_durable_accepted_total {}\n\
         # HELP gateway_v3_durable_rejected_total Total /v3 durable rejected outcomes\n\
         # TYPE gateway_v3_durable_rejected_total counter\n\
         gateway_v3_durable_rejected_total {}\n\
         # HELP gateway_v3_durable_queue_depth Total /v3 durable queue depth across all lanes\n\
         # TYPE gateway_v3_durable_queue_depth gauge\n\
         gateway_v3_durable_queue_depth {}\n\
         # HELP gateway_v3_durable_queue_capacity Total /v3 durable queue capacity across all lanes\n\
         # TYPE gateway_v3_durable_queue_capacity gauge\n\
         gateway_v3_durable_queue_capacity {}\n\
         # HELP gateway_v3_durable_queue_utilization_pct /v3 durable queue utilization percentage (aggregate)\n\
         # TYPE gateway_v3_durable_queue_utilization_pct gauge\n\
         gateway_v3_durable_queue_utilization_pct {}\n\
         # HELP gateway_v3_durable_queue_utilization_pct_max /v3 durable queue utilization percentage (worst lane)\n\
         # TYPE gateway_v3_durable_queue_utilization_pct_max gauge\n\
         gateway_v3_durable_queue_utilization_pct_max {}\n\
         # HELP gateway_v3_durable_lane_skew_pct /v3 durable queue lane skew percentage against lane average\n\
         # TYPE gateway_v3_durable_lane_skew_pct gauge\n\
         gateway_v3_durable_lane_skew_pct {}\n\
         # HELP gateway_v3_durable_lanes Configured /v3 durable lane count\n\
         # TYPE gateway_v3_durable_lanes gauge\n\
         gateway_v3_durable_lanes {}\n\
         # HELP gateway_v3_durable_backlog_growth_per_sec /v3 durable queue depth growth per second\n\
         # TYPE gateway_v3_durable_backlog_growth_per_sec gauge\n\
         gateway_v3_durable_backlog_growth_per_sec {}\n\
         # HELP gateway_v3_durable_queue_full_total Total /v3 durable queue full events\n\
         # TYPE gateway_v3_durable_queue_full_total counter\n\
         gateway_v3_durable_queue_full_total {}\n\
         # HELP gateway_v3_durable_queue_closed_total Total /v3 durable queue closed events\n\
         # TYPE gateway_v3_durable_queue_closed_total counter\n\
         gateway_v3_durable_queue_closed_total {}\n\
         # HELP gateway_v3_durable_worker_processed_total Total /v3 durable worker processed tasks\n\
         # TYPE gateway_v3_durable_worker_processed_total counter\n\
         gateway_v3_durable_worker_processed_total {}\n\
         # HELP gateway_v3_durable_worker_batch_max Configured /v3 durable worker max batch size\n\
         # TYPE gateway_v3_durable_worker_batch_max gauge\n\
         gateway_v3_durable_worker_batch_max {}\n\
         # HELP gateway_v3_durable_worker_batch_min Configured /v3 durable worker min batch size (adaptive floor)\n\
         # TYPE gateway_v3_durable_worker_batch_min gauge\n\
         gateway_v3_durable_worker_batch_min {}\n\
         # HELP gateway_v3_durable_worker_batch_wait_us Configured /v3 durable worker micro-batch wait in microseconds\n\
         # TYPE gateway_v3_durable_worker_batch_wait_us gauge\n\
         gateway_v3_durable_worker_batch_wait_us {}\n\
         # HELP gateway_v3_durable_worker_batch_wait_min_us Configured /v3 durable worker minimum micro-batch wait in microseconds\n\
         # TYPE gateway_v3_durable_worker_batch_wait_min_us gauge\n\
         gateway_v3_durable_worker_batch_wait_min_us {}\n\
         # HELP gateway_v3_durable_worker_batch_adaptive Whether /v3 durable worker adaptive batching is enabled (1/0)\n\
         # TYPE gateway_v3_durable_worker_batch_adaptive gauge\n\
         gateway_v3_durable_worker_batch_adaptive {}\n\
         # HELP gateway_v3_durable_worker_batch_adaptive_low_util_pct Adaptive batching low queue-utilization threshold percentage\n\
         # TYPE gateway_v3_durable_worker_batch_adaptive_low_util_pct gauge\n\
         gateway_v3_durable_worker_batch_adaptive_low_util_pct {}\n\
         # HELP gateway_v3_durable_worker_batch_adaptive_high_util_pct Adaptive batching high queue-utilization threshold percentage\n\
         # TYPE gateway_v3_durable_worker_batch_adaptive_high_util_pct gauge\n\
         gateway_v3_durable_worker_batch_adaptive_high_util_pct {}\n\
         # HELP gateway_v3_durable_worker_loop_p50_us /v3 durable worker loop latency p50 in microseconds\n\
         # TYPE gateway_v3_durable_worker_loop_p50_us gauge\n\
         gateway_v3_durable_worker_loop_p50_us {}\n\
         # HELP gateway_v3_durable_worker_loop_p99_us /v3 durable worker loop latency p99 in microseconds\n\
         # TYPE gateway_v3_durable_worker_loop_p99_us gauge\n\
         gateway_v3_durable_worker_loop_p99_us {}\n\
         # HELP gateway_v3_durable_wal_append_p50_us /v3 durable WAL append latency p50 in microseconds\n\
         # TYPE gateway_v3_durable_wal_append_p50_us gauge\n\
         gateway_v3_durable_wal_append_p50_us {}\n\
         # HELP gateway_v3_durable_wal_append_p99_us /v3 durable WAL append latency p99 in microseconds\n\
         # TYPE gateway_v3_durable_wal_append_p99_us gauge\n\
         gateway_v3_durable_wal_append_p99_us {}\n\
         # HELP gateway_v3_durable_fdatasync_p50_us /v3 durable fdatasync latency p50 in microseconds\n\
         # TYPE gateway_v3_durable_fdatasync_p50_us gauge\n\
         gateway_v3_durable_fdatasync_p50_us {}\n\
         # HELP gateway_v3_durable_fdatasync_p99_us /v3 durable fdatasync latency p99 in microseconds\n\
         # TYPE gateway_v3_durable_fdatasync_p99_us gauge\n\
         gateway_v3_durable_fdatasync_p99_us {}\n",
        v3_live_ack_p99,
        v3_live_ack_accepted_p99,
        v3_live_ack_p99,
        v3_live_ack_accepted_p99,
        v3_durable_confirm_p99,
        v3_accepted_rate,
        v3_rejected_hard_total,
        v3_hard_reject_pct,
        v3_shard_kill_switches,
        v3_loss_suspect_total,
        v3_session_loss_suspect_threshold,
        v3_session_killed_total,
        v3_shard_killed_total,
        v3_global_killed_total,
        v3_durable_accepted_total,
        v3_durable_rejected_total,
        v3_durable_queue_depth,
        v3_durable_queue_capacity,
        v3_durable_queue_utilization_pct,
        v3_durable_queue_utilization_pct_max,
        v3_durable_lane_skew_pct,
        v3_durable_lanes,
        v3_durable_backlog_growth_per_sec,
        v3_durable_queue_full_total,
        v3_durable_queue_closed_total,
        v3_durable_worker_processed_total,
        state.v3_durable_worker_batch_max,
        state.v3_durable_worker_batch_min,
        state.v3_durable_worker_batch_wait_us,
        state.v3_durable_worker_batch_wait_min_us,
        v3_durable_worker_batch_adaptive,
        state.v3_durable_worker_batch_adaptive_low_util_pct,
        state.v3_durable_worker_batch_adaptive_high_util_pct,
        v3_durable_worker_loop_p50,
        v3_durable_worker_loop_p99,
        v3_durable_wal_append_p50,
        v3_durable_wal_append_p99,
        v3_durable_fsync_p50,
        v3_durable_fsync_p99,
    ));
    snapshot.push_str(&format!(
        "# HELP gateway_v3_durable_write_error_total Total /v3 durable write/receipt errors\n\
         # TYPE gateway_v3_durable_write_error_total counter\n\
         gateway_v3_durable_write_error_total {}\n\
         # HELP gateway_v3_durable_receipt_timeout_total Total /v3 durable receipt timeout events inside durable worker\n\
         # TYPE gateway_v3_durable_receipt_timeout_total counter\n\
         gateway_v3_durable_receipt_timeout_total {}\n\
         # HELP gateway_v3_durable_worker_receipt_timeout_us Configured /v3 durable receipt timeout in durable worker (microseconds)\n\
         # TYPE gateway_v3_durable_worker_receipt_timeout_us gauge\n\
         gateway_v3_durable_worker_receipt_timeout_us {}\n\
         # HELP gateway_v3_durable_worker_max_inflight_receipts Configured max in-flight durable receipts per worker\n\
         # TYPE gateway_v3_durable_worker_max_inflight_receipts gauge\n\
         gateway_v3_durable_worker_max_inflight_receipts {}\n\
         # HELP gateway_v3_durable_worker_inflight_soft_cap_pct In-flight receipt cap percentage applied when durable admission level is soft\n\
         # TYPE gateway_v3_durable_worker_inflight_soft_cap_pct gauge\n\
         gateway_v3_durable_worker_inflight_soft_cap_pct {}\n\
         # HELP gateway_v3_durable_worker_inflight_hard_cap_pct In-flight receipt cap percentage applied when durable admission level is hard\n\
         # TYPE gateway_v3_durable_worker_inflight_hard_cap_pct gauge\n\
         gateway_v3_durable_worker_inflight_hard_cap_pct {}\n\
         # HELP gateway_v3_durable_receipt_inflight Current in-flight durable receipts inside worker\n\
         # TYPE gateway_v3_durable_receipt_inflight gauge\n\
         gateway_v3_durable_receipt_inflight {}\n\
         # HELP gateway_v3_durable_receipt_inflight_max Max in-flight durable receipts observed since process start\n\
         # TYPE gateway_v3_durable_receipt_inflight_max gauge\n\
         gateway_v3_durable_receipt_inflight_max {}\n\
         # HELP gateway_v3_durable_backpressure_soft_total Total /v3 soft rejects triggered by durable backpressure\n\
         # TYPE gateway_v3_durable_backpressure_soft_total counter\n\
         gateway_v3_durable_backpressure_soft_total {}\n\
         # HELP gateway_v3_durable_backpressure_hard_total Total /v3 hard rejects triggered by durable backpressure\n\
         # TYPE gateway_v3_durable_backpressure_hard_total counter\n\
         gateway_v3_durable_backpressure_hard_total {}\n\
         # HELP gateway_v3_durable_admission_controller_enabled /v3 durable admission controller enabled (1/0)\n\
         # TYPE gateway_v3_durable_admission_controller_enabled gauge\n\
         gateway_v3_durable_admission_controller_enabled {}\n\
         # HELP gateway_v3_durable_admission_level /v3 durable admission controller level (0=normal,1=soft,2=hard)\n\
         # TYPE gateway_v3_durable_admission_level gauge\n\
         gateway_v3_durable_admission_level {}\n\
         # HELP gateway_v3_durable_admission_soft_trip_total Total transitions into durable admission soft level\n\
         # TYPE gateway_v3_durable_admission_soft_trip_total counter\n\
         gateway_v3_durable_admission_soft_trip_total {}\n\
         # HELP gateway_v3_durable_admission_hard_trip_total Total transitions into durable admission hard level\n\
         # TYPE gateway_v3_durable_admission_hard_trip_total counter\n\
         gateway_v3_durable_admission_hard_trip_total {}\n\
         # HELP gateway_v3_durable_admission_sustain_ticks Durable admission controller sustain ticks\n\
         # TYPE gateway_v3_durable_admission_sustain_ticks gauge\n\
         gateway_v3_durable_admission_sustain_ticks {}\n\
         # HELP gateway_v3_durable_admission_recover_ticks Durable admission controller recover ticks\n\
         # TYPE gateway_v3_durable_admission_recover_ticks gauge\n\
         gateway_v3_durable_admission_recover_ticks {}\n\
         # HELP gateway_v3_durable_admission_soft_fsync_p99_us Durable admission soft signal threshold for fdatasync p99 (us)\n\
         # TYPE gateway_v3_durable_admission_soft_fsync_p99_us gauge\n\
         gateway_v3_durable_admission_soft_fsync_p99_us {}\n\
         # HELP gateway_v3_durable_admission_hard_fsync_p99_us Durable admission hard signal threshold for fdatasync p99 (us)\n\
         # TYPE gateway_v3_durable_admission_hard_fsync_p99_us gauge\n\
         gateway_v3_durable_admission_hard_fsync_p99_us {}\n\
         # HELP gateway_v3_durable_soft_reject_pct /v3 durable-path soft reject threshold percentage\n\
         # TYPE gateway_v3_durable_soft_reject_pct gauge\n\
         gateway_v3_durable_soft_reject_pct {}\n\
         # HELP gateway_v3_durable_hard_reject_pct /v3 durable-path hard reject threshold percentage\n\
         # TYPE gateway_v3_durable_hard_reject_pct gauge\n\
         gateway_v3_durable_hard_reject_pct {}\n\
         # HELP gateway_v3_durable_backlog_soft_reject_per_sec /v3 durable backlog growth soft reject threshold per second\n\
         # TYPE gateway_v3_durable_backlog_soft_reject_per_sec gauge\n\
         gateway_v3_durable_backlog_soft_reject_per_sec {}\n\
         # HELP gateway_v3_durable_backlog_hard_reject_per_sec /v3 durable backlog growth hard reject threshold per second\n\
         # TYPE gateway_v3_durable_backlog_hard_reject_per_sec gauge\n\
         gateway_v3_durable_backlog_hard_reject_per_sec {}\n\
         # HELP gateway_v3_durable_backlog_signal_min_queue_pct /v3 minimum queue utilization pct required before backlog-growth signals are considered\n\
         # TYPE gateway_v3_durable_backlog_signal_min_queue_pct gauge\n\
         gateway_v3_durable_backlog_signal_min_queue_pct {}\n\
         # HELP gateway_v3_durable_admission_fsync_presignal_pct /v3 ratio used for fsync-coupled presignal thresholds against soft limits\n\
         # TYPE gateway_v3_durable_admission_fsync_presignal_pct gauge\n\
         gateway_v3_durable_admission_fsync_presignal_pct {}\n\
         # HELP gateway_v3_durable_confirm_soft_reject_age_us /v3 durable confirm oldest-age soft reject threshold (us, 0=disabled)\n\
         # TYPE gateway_v3_durable_confirm_soft_reject_age_us gauge\n\
         gateway_v3_durable_confirm_soft_reject_age_us {}\n\
         # HELP gateway_v3_durable_confirm_hard_reject_age_us /v3 durable confirm oldest-age hard reject threshold (us, 0=disabled)\n\
         # TYPE gateway_v3_durable_confirm_hard_reject_age_us gauge\n\
         gateway_v3_durable_confirm_hard_reject_age_us {}\n\
         # HELP gateway_v3_durable_confirm_age_soft_reject_total Total /v3 soft rejects due to durable confirm oldest-age guard\n\
         # TYPE gateway_v3_durable_confirm_age_soft_reject_total counter\n\
         gateway_v3_durable_confirm_age_soft_reject_total {}\n\
         # HELP gateway_v3_durable_confirm_age_hard_reject_total Total /v3 hard rejects due to durable confirm oldest-age guard\n\
         # TYPE gateway_v3_durable_confirm_age_hard_reject_total counter\n\
         gateway_v3_durable_confirm_age_hard_reject_total {}\n",
        v3_durable_write_error_total,
        v3_durable_receipt_timeout_total,
        v3_durable_worker_receipt_timeout_us,
        v3_durable_worker_max_inflight_receipts,
        v3_durable_worker_inflight_soft_cap_pct,
        v3_durable_worker_inflight_hard_cap_pct,
        v3_durable_receipt_inflight,
        v3_durable_receipt_inflight_max,
        v3_durable_backpressure_soft_total,
        v3_durable_backpressure_hard_total,
        v3_durable_admission_controller_enabled,
        v3_durable_admission_level,
        v3_durable_admission_soft_trip_total,
        v3_durable_admission_hard_trip_total,
        v3_durable_admission_sustain_ticks,
        v3_durable_admission_recover_ticks,
        v3_durable_admission_soft_fsync_p99_us,
        v3_durable_admission_hard_fsync_p99_us,
        v3_durable_soft_reject_pct,
        v3_durable_hard_reject_pct,
        v3_durable_backlog_soft_reject_per_sec,
        v3_durable_backlog_hard_reject_per_sec,
        v3_durable_backlog_signal_min_queue_pct,
        v3_durable_admission_fsync_presignal_pct,
        v3_durable_confirm_soft_reject_age_us,
        v3_durable_confirm_hard_reject_age_us,
        v3_durable_confirm_age_soft_reject_total,
        v3_durable_confirm_age_hard_reject_total,
    ));
    snapshot.push_str(
        "# HELP gateway_v3_durable_queue_depth_per_lane /v3 durable queue depth per lane\n\
         # TYPE gateway_v3_durable_queue_depth_per_lane gauge\n",
    );
    for (lane, depth) in v3_durable_lane_depths.iter().enumerate() {
        snapshot.push_str(&format!(
            "gateway_v3_durable_queue_depth_per_lane{{lane=\"{}\"}} {}\n",
            lane, depth
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_queue_utilization_pct_per_lane /v3 durable queue utilization percentage per lane\n\
         # TYPE gateway_v3_durable_queue_utilization_pct_per_lane gauge\n",
    );
    for (lane, util_pct) in v3_durable_lane_utils.iter().enumerate() {
        snapshot.push_str(&format!(
            "gateway_v3_durable_queue_utilization_pct_per_lane{{lane=\"{}\"}} {}\n",
            lane, util_pct
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_backlog_growth_per_sec_per_lane /v3 durable queue depth growth per second per lane\n\
         # TYPE gateway_v3_durable_backlog_growth_per_sec_per_lane gauge\n",
    );
    for (lane, growth) in v3_durable_backlog_growth_per_sec_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_backlog_growth_per_sec_per_lane{{lane=\"{}\"}} {}\n",
            lane, growth
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_backpressure_soft_total_per_lane Total /v3 durable soft rejects per lane\n\
         # TYPE gateway_v3_durable_backpressure_soft_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_backpressure_soft_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_backpressure_soft_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_backpressure_hard_total_per_lane Total /v3 durable hard rejects per lane\n\
         # TYPE gateway_v3_durable_backpressure_hard_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_backpressure_hard_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_backpressure_hard_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_level_per_lane /v3 durable admission level per lane (0=normal,1=soft,2=hard)\n\
         # TYPE gateway_v3_durable_admission_level_per_lane gauge\n",
    );
    for (lane, level) in v3_durable_admission_level_per_lane.iter().enumerate() {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_level_per_lane{{lane=\"{}\"}} {}\n",
            lane, level
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_soft_trip_total_per_lane Total transitions into durable admission soft level per lane\n\
         # TYPE gateway_v3_durable_admission_soft_trip_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_admission_soft_trip_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_soft_trip_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_hard_trip_total_per_lane Total transitions into durable admission hard level per lane\n\
         # TYPE gateway_v3_durable_admission_hard_trip_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_admission_hard_trip_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_hard_trip_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_signal_queue_soft_total_per_lane Durable admission queue-soft signal hits per lane\n\
         # TYPE gateway_v3_durable_admission_signal_queue_soft_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_admission_signal_queue_soft_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_signal_queue_soft_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_signal_queue_hard_total_per_lane Durable admission queue-hard signal hits per lane\n\
         # TYPE gateway_v3_durable_admission_signal_queue_hard_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_admission_signal_queue_hard_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_signal_queue_hard_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_signal_backlog_soft_total_per_lane Durable admission backlog-soft signal hits per lane\n\
         # TYPE gateway_v3_durable_admission_signal_backlog_soft_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_admission_signal_backlog_soft_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_signal_backlog_soft_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_signal_backlog_hard_total_per_lane Durable admission backlog-hard signal hits per lane\n\
         # TYPE gateway_v3_durable_admission_signal_backlog_hard_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_admission_signal_backlog_hard_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_signal_backlog_hard_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_signal_fsync_soft_total_per_lane Durable admission fsync-soft signal hits per lane\n\
         # TYPE gateway_v3_durable_admission_signal_fsync_soft_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_admission_signal_fsync_soft_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_signal_fsync_soft_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_admission_signal_fsync_hard_total_per_lane Durable admission fsync-hard signal hits per lane\n\
         # TYPE gateway_v3_durable_admission_signal_fsync_hard_total_per_lane counter\n",
    );
    for (lane, total) in v3_durable_admission_signal_fsync_hard_total_per_lane
        .iter()
        .enumerate()
    {
        snapshot.push_str(&format!(
            "gateway_v3_durable_admission_signal_fsync_hard_total_per_lane{{lane=\"{}\"}} {}\n",
            lane, total
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_fdatasync_p99_us_per_lane /v3 durable fdatasync latency p99 per lane (us)\n\
         # TYPE gateway_v3_durable_fdatasync_p99_us_per_lane gauge\n",
    );
    for (lane, p99) in v3_durable_fsync_p99_per_lane.iter().enumerate() {
        snapshot.push_str(&format!(
            "gateway_v3_durable_fdatasync_p99_us_per_lane{{lane=\"{}\"}} {}\n",
            lane, p99
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_durable_worker_loop_p99_us_per_lane /v3 durable worker loop latency p99 per lane (us)\n\
         # TYPE gateway_v3_durable_worker_loop_p99_us_per_lane gauge\n",
    );
    for (lane, p99) in v3_durable_worker_loop_p99_per_lane.iter().enumerate() {
        snapshot.push_str(&format!(
            "gateway_v3_durable_worker_loop_p99_us_per_lane{{lane=\"{}\"}} {}\n",
            lane, p99
        ));
    }
    snapshot.push_str(&format!(
        "# HELP gateway_v3_confirm_store_size Number of records in /v3 confirm store\n\
         # TYPE gateway_v3_confirm_store_size gauge\n\
         gateway_v3_confirm_store_size {}\n\
         # HELP gateway_v3_confirm_store_lanes Configured lane count for /v3 confirm store\n\
         # TYPE gateway_v3_confirm_store_lanes gauge\n\
         gateway_v3_confirm_store_lanes {}\n\
         # HELP gateway_v3_confirm_lane_skew_pct /v3 confirm store lane skew percentage against lane average\n\
         # TYPE gateway_v3_confirm_lane_skew_pct gauge\n\
         gateway_v3_confirm_lane_skew_pct {}\n\
         # HELP gateway_v3_confirm_oldest_inflight_us Oldest /v3 inflight confirm age in microseconds\n\
         # TYPE gateway_v3_confirm_oldest_inflight_us gauge\n\
         gateway_v3_confirm_oldest_inflight_us {}\n\
         # HELP gateway_v3_confirm_timeout_scan_cost_last Number of timeout-wheel entries scanned in the last monitor tick\n\
         # TYPE gateway_v3_confirm_timeout_scan_cost_last gauge\n\
         gateway_v3_confirm_timeout_scan_cost_last {}\n\
         # HELP gateway_v3_confirm_timeout_scan_cost_total Total timeout-wheel entries scanned by monitor\n\
         # TYPE gateway_v3_confirm_timeout_scan_cost_total counter\n\
         gateway_v3_confirm_timeout_scan_cost_total {}\n\
         # HELP gateway_v3_confirm_gc_removed_total Total /v3 confirm records removed by TTL GC\n\
         # TYPE gateway_v3_confirm_gc_removed_total counter\n\
         gateway_v3_confirm_gc_removed_total {}\n\
         # HELP gateway_v3_confirm_rebuild_restored_total Total /v3 confirm records restored from WAL at startup\n\
         # TYPE gateway_v3_confirm_rebuild_restored_total counter\n\
         gateway_v3_confirm_rebuild_restored_total {}\n\
         # HELP gateway_v3_confirm_rebuild_elapsed_ms Elapsed milliseconds for /v3 confirm WAL rebuild at startup\n\
         # TYPE gateway_v3_confirm_rebuild_elapsed_ms gauge\n\
         gateway_v3_confirm_rebuild_elapsed_ms {}\n",
        v3_confirm_store_size,
        v3_confirm_store_lanes,
        v3_confirm_lane_skew_pct,
        v3_confirm_oldest_inflight_us,
        v3_confirm_timeout_scan_cost_last,
        v3_confirm_timeout_scan_cost_total,
        v3_confirm_gc_removed_total,
        v3_confirm_rebuild_restored_total,
        v3_confirm_rebuild_elapsed_ms,
    ));
    snapshot.push_str(
        "# HELP gateway_v3_confirm_oldest_inflight_us_per_lane Oldest /v3 inflight confirm age per lane in microseconds\n\
         # TYPE gateway_v3_confirm_oldest_inflight_us_per_lane gauge\n",
    );
    for (lane, age) in v3_confirm_oldest_inflight_us_per_lane.iter().enumerate() {
        snapshot.push_str(&format!(
            "gateway_v3_confirm_oldest_inflight_us_per_lane{{lane=\"{}\"}} {}\n",
            lane, age
        ));
    }
    snapshot.push_str(
        "# HELP gateway_v3_confirm_age_p99_us_per_lane /v3 inflight confirm oldest-age p99 per lane (us)\n\
         # TYPE gateway_v3_confirm_age_p99_us_per_lane gauge\n",
    );
    for (lane, p99) in v3_confirm_age_p99_per_lane.iter().enumerate() {
        snapshot.push_str(&format!(
            "gateway_v3_confirm_age_p99_us_per_lane{{lane=\"{}\"}} {}\n",
            lane, p99
        ));
    }
    snapshot.push_str(&format!(
        "# HELP gateway_v2_requests_total Total POST /v2/orders requests\n\
         # TYPE gateway_v2_requests_total counter\n\
         gateway_v2_requests_total {}\n\
         # HELP gateway_v2_durable_wait_timeout_total Total /v2/orders rejects due to durable wait timeout\n\
         # TYPE gateway_v2_durable_wait_timeout_total counter\n\
         gateway_v2_durable_wait_timeout_total {}\n\
         # HELP gateway_v2_durable_wait_timeout_ratio /v2 durable wait timeout ratio against /v2 requests\n\
         # TYPE gateway_v2_durable_wait_timeout_ratio gauge\n\
         gateway_v2_durable_wait_timeout_ratio {}\n\
         # HELP gateway_v2_durable_wait_timeout_ms Configured /v2 durable wait timeout in milliseconds\n\
         # TYPE gateway_v2_durable_wait_timeout_ms gauge\n\
         gateway_v2_durable_wait_timeout_ms {}\n",
        v2_requests_total,
        v2_durable_wait_timeout_total,
        v2_durable_wait_timeout_ratio,
        state.v2_durable_wait_timeout_ms,
    ));
    snapshot.push_str(&format!(
        "# HELP gateway_v3_risk_profile_level /v3 risk profile level (light=1, medium=2, heavy=3)\n\
         # TYPE gateway_v3_risk_profile_level gauge\n\
         gateway_v3_risk_profile_level {}\n\
         # HELP gateway_v3_risk_profile_loops /v3 risk profile loop count per request\n\
         # TYPE gateway_v3_risk_profile_loops gauge\n\
         gateway_v3_risk_profile_loops {}\n\
         # HELP gateway_v3_risk_strict_symbols /v3 strict symbol master enabled (1/0)\n\
         # TYPE gateway_v3_risk_strict_symbols gauge\n\
         gateway_v3_risk_strict_symbols {}\n\
         # HELP gateway_v3_risk_max_order_qty /v3 real risk max order quantity\n\
         # TYPE gateway_v3_risk_max_order_qty gauge\n\
         gateway_v3_risk_max_order_qty {}\n\
         # HELP gateway_v3_risk_max_notional /v3 real risk max notional\n\
         # TYPE gateway_v3_risk_max_notional gauge\n\
         gateway_v3_risk_max_notional {}\n\
         # HELP gateway_v3_risk_daily_notional_limit /v3 per-account daily notional limit\n\
         # TYPE gateway_v3_risk_daily_notional_limit gauge\n\
         gateway_v3_risk_daily_notional_limit {}\n\
         # HELP gateway_v3_risk_max_abs_position_qty /v3 per-account per-symbol max absolute position qty\n\
         # TYPE gateway_v3_risk_max_abs_position_qty gauge\n\
         gateway_v3_risk_max_abs_position_qty {}\n\
         # HELP gateway_v3_symbol_limits_count Number of symbols in /v3 symbol master\n\
         # TYPE gateway_v3_symbol_limits_count gauge\n\
         gateway_v3_symbol_limits_count {}\n\
         # HELP gateway_v3_account_daily_notional_count Number of /v3 account daily notional trackers\n\
         # TYPE gateway_v3_account_daily_notional_count gauge\n\
         gateway_v3_account_daily_notional_count {}\n\
         # HELP gateway_v3_account_symbol_position_count Number of /v3 account-symbol position trackers\n\
         # TYPE gateway_v3_account_symbol_position_count gauge\n\
         gateway_v3_account_symbol_position_count {}\n\
         # HELP gateway_v3_risk_margin_mode /v3 risk margin mode (legacy=1, incremental=2)\n\
         # TYPE gateway_v3_risk_margin_mode gauge\n\
         gateway_v3_risk_margin_mode {}\n\
         # HELP gateway_v3_kill_auto_recover_enabled /v3 kill auto recover enabled (1/0)\n\
         # TYPE gateway_v3_kill_auto_recover_enabled gauge\n\
         gateway_v3_kill_auto_recover_enabled {}\n\
         # HELP gateway_v3_kill_recover_pct /v3 kill recovery threshold percentage\n\
         # TYPE gateway_v3_kill_recover_pct gauge\n\
         gateway_v3_kill_recover_pct {}\n\
         # HELP gateway_v3_kill_recover_after_ms /v3 kill recovery hold duration in milliseconds\n\
         # TYPE gateway_v3_kill_recover_after_ms gauge\n\
         gateway_v3_kill_recover_after_ms {}\n\
         # HELP gateway_v3_kill_recovered_total Total /v3 auto recover transitions from kill to open\n\
         # TYPE gateway_v3_kill_recovered_total counter\n\
         gateway_v3_kill_recovered_total {}\n\
         # HELP gateway_v3_stage_parse_p50_us /v3 parse stage latency p50 in microseconds\n\
         # TYPE gateway_v3_stage_parse_p50_us gauge\n\
         gateway_v3_stage_parse_p50_us {}\n\
         # HELP gateway_v3_stage_parse_p99_us /v3 parse stage latency p99 in microseconds\n\
         # TYPE gateway_v3_stage_parse_p99_us gauge\n\
         gateway_v3_stage_parse_p99_us {}\n\
         # HELP gateway_v3_stage_risk_p50_us /v3 risk stage latency p50 in microseconds\n\
         # TYPE gateway_v3_stage_risk_p50_us gauge\n\
         gateway_v3_stage_risk_p50_us {}\n\
         # HELP gateway_v3_stage_risk_p99_us /v3 risk stage latency p99 in microseconds\n\
         # TYPE gateway_v3_stage_risk_p99_us gauge\n\
         gateway_v3_stage_risk_p99_us {}\n\
         # HELP gateway_v3_stage_risk_position_p50_us /v3 risk position stage latency p50 in microseconds\n\
         # TYPE gateway_v3_stage_risk_position_p50_us gauge\n\
         gateway_v3_stage_risk_position_p50_us {}\n\
         # HELP gateway_v3_stage_risk_position_p99_us /v3 risk position stage latency p99 in microseconds\n\
         # TYPE gateway_v3_stage_risk_position_p99_us gauge\n\
         gateway_v3_stage_risk_position_p99_us {}\n\
         # HELP gateway_v3_stage_risk_margin_p50_us /v3 risk margin stage latency p50 in microseconds\n\
         # TYPE gateway_v3_stage_risk_margin_p50_us gauge\n\
         gateway_v3_stage_risk_margin_p50_us {}\n\
         # HELP gateway_v3_stage_risk_margin_p99_us /v3 risk margin stage latency p99 in microseconds\n\
         # TYPE gateway_v3_stage_risk_margin_p99_us gauge\n\
         gateway_v3_stage_risk_margin_p99_us {}\n\
         # HELP gateway_v3_stage_risk_limits_p50_us /v3 risk limits stage latency p50 in microseconds\n\
         # TYPE gateway_v3_stage_risk_limits_p50_us gauge\n\
         gateway_v3_stage_risk_limits_p50_us {}\n\
         # HELP gateway_v3_stage_risk_limits_p99_us /v3 risk limits stage latency p99 in microseconds\n\
         # TYPE gateway_v3_stage_risk_limits_p99_us gauge\n\
         gateway_v3_stage_risk_limits_p99_us {}\n\
         # HELP gateway_v3_stage_enqueue_p50_us /v3 enqueue stage latency p50 in microseconds\n\
         # TYPE gateway_v3_stage_enqueue_p50_us gauge\n\
         gateway_v3_stage_enqueue_p50_us {}\n\
         # HELP gateway_v3_stage_enqueue_p99_us /v3 enqueue stage latency p99 in microseconds\n\
         # TYPE gateway_v3_stage_enqueue_p99_us gauge\n\
         gateway_v3_stage_enqueue_p99_us {}\n\
         # HELP gateway_v3_stage_serialize_p50_us /v3 serialize stage latency p50 in microseconds\n\
         # TYPE gateway_v3_stage_serialize_p50_us gauge\n\
         gateway_v3_stage_serialize_p50_us {}\n\
         # HELP gateway_v3_stage_serialize_p99_us /v3 serialize stage latency p99 in microseconds\n\
         # TYPE gateway_v3_stage_serialize_p99_us gauge\n\
         gateway_v3_stage_serialize_p99_us {}\n",
        v3_risk_profile_level,
        v3_risk_profile_loops,
        v3_risk_strict_symbols,
        v3_risk_max_order_qty,
        v3_risk_max_notional,
        v3_risk_daily_notional_limit,
        v3_risk_max_abs_position_qty,
        v3_symbol_limits_count,
        v3_account_daily_notional_count,
        v3_account_symbol_position_count,
        v3_risk_margin_mode_level,
        v3_kill_auto_recover,
        v3_kill_recover_pct,
        v3_kill_recover_after_ms,
        v3_kill_recovered_total,
        v3_stage_parse_p50,
        v3_stage_parse_p99,
        v3_stage_risk_p50,
        v3_stage_risk_p99,
        v3_stage_risk_position_p50,
        v3_stage_risk_position_p99,
        v3_stage_risk_margin_p50,
        v3_stage_risk_margin_p99,
        v3_stage_risk_limits_p50,
        v3_stage_risk_limits_p99,
        v3_stage_enqueue_p50,
        v3_stage_enqueue_p99,
        v3_stage_serialize_p50,
        v3_stage_serialize_p99,
    ));
    snapshot
}
