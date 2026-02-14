//! 運用API（観測の入口）:
//! - 役割: FastPathの稼働状態とSLO指標を取得する。
//! - 位置: 運用監視のための読み取り専用パス。
//! - 内包: health と Prometheus metrics の出力。

use axum::{extract::State, Json};
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
    let backpressure_soft_wal_age = state.backpressure_soft_wal_age.load(Ordering::Relaxed);
    let backpressure_soft_rate_decline =
        state.backpressure_soft_rate_decline.load(Ordering::Relaxed);
    let backpressure_inflight = state.backpressure_inflight.load(Ordering::Relaxed);
    let backpressure_wal_bytes = state.backpressure_wal_bytes.load(Ordering::Relaxed);
    let backpressure_wal_age = state.backpressure_wal_age.load(Ordering::Relaxed);
    let backpressure_disk_free = state.backpressure_disk_free.load(Ordering::Relaxed);
    let inflight = state.inflight_controller.inflight();
    let durable_inflight = state.inflight_controller.inflight();
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

    let snapshot = format!(
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
    snapshot
}
