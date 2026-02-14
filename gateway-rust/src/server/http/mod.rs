//! HTTP サーバー（Kotlin版 HttpGateway 互換）
//!
//! 位置づけ:
//! - このモジュールは「HTTP入口層」。注文受付→FastPath投入→監査/Bus/SSEへの橋渡しを担う。
//! - 入口のルーティングをここに集約し、実処理はサブモジュールに分離する。
//!
//! ハンドラの分類（同期境界 / ユーザ用 / 運用用）:
//! - 同期境界（ACKまで）:
//!   - /orders: 低遅延で受理する入口。ここまでが同期境界。
//! - ユーザ向け（注文/通知）:
//!   - /orders/{id}: 受理後の現在状態を取得（UI確認）。
//!   - /orders/{id}/cancel: キャンセル要求を非同期で流す。
//!   - /orders/{id}/stream: 注文単位のリアルタイム通知。
//!   - /stream: アカウント全体のリアルタイム通知。
//! - 運用/監査向け:
//!   - /orders/{id}/events: 監査ログ由来の履歴取得（調査）。
//!   - /accounts/{id}/events: アカウント全体の監査履歴。
//!   - /audit/verify: 監査ログの整合性検証（改ざん検知）。
//!   - /audit/anchor: 監査ハッシュのスナップショット取得。
//!   - /health: 稼働確認・簡易SLO確認。
//!   - /metrics: Prometheus用の観測出力。

// ハンドラはドメイン別に分割（注文 / 監査 / SSE / メトリクス）
mod audit;
mod metrics;
mod orders;
mod sse;

use axum::{
    routing::{get, post},
    Router,
};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedReceiver;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::audit::AuditDurableNotification;
use crate::audit::AuditLog;
use crate::auth::JwtAuth;
use crate::backpressure::BackpressureConfig;
use crate::bus::BusPublisher;
use crate::engine::FastPathEngine;
use crate::inflight::InflightControllerHandle;
use crate::rate_limit::AccountRateLimiter;
use crate::sse::SseHub;
use crate::store::{OrderIdMap, OrderStore, ShardedOrderStore};
use gateway_core::LatencyHistogram;
use std::path::PathBuf;

use audit::{handle_account_events, handle_audit_anchor, handle_audit_verify, handle_order_events};
use metrics::{handle_health, handle_metrics};
use orders::{handle_cancel_order, handle_get_order, handle_get_order_by_client_id, handle_order};
use sse::{handle_account_stream, handle_order_stream};

/// アプリケーション状態
#[derive(Clone)]
pub(super) struct AppState {
    pub(super) engine: FastPathEngine,
    pub(super) jwt_auth: Arc<JwtAuth>,
    /// レガシー OrderStore（後方互換用）
    #[allow(dead_code)]
    pub(super) order_store: Arc<OrderStore>,
    /// HFT最適化版 ShardedOrderStore
    pub(super) sharded_store: Arc<ShardedOrderStore>,
    /// 注文IDマッピング（内部ID ↔ 外部ID）
    pub(super) order_id_map: Arc<OrderIdMap>,
    pub(super) sse_hub: Arc<SseHub>,
    pub(super) order_id_seq: Arc<AtomicU64>,
    pub(super) audit_log: Arc<AuditLog>,
    pub(super) audit_read_path: Arc<PathBuf>,
    pub(super) bus_publisher: Arc<BusPublisher>,
    pub(super) bus_mode_outbox: bool,
    pub(super) backpressure: BackpressureConfig,
    pub(super) inflight_controller: InflightControllerHandle,
    pub(super) rate_limiter: Option<Arc<AccountRateLimiter>>,
    pub(super) ack_hist: Arc<LatencyHistogram>,
    pub(super) wal_enqueue_hist: Arc<LatencyHistogram>,
    pub(super) durable_ack_hist: Arc<LatencyHistogram>,
    pub(super) fdatasync_hist: Arc<LatencyHistogram>,
    pub(super) durable_notify_hist: Arc<LatencyHistogram>,
    pub(super) idempotency_checked: Arc<AtomicU64>,
    pub(super) idempotency_hits: Arc<AtomicU64>,
    pub(super) idempotency_creates: Arc<AtomicU64>,
    pub(super) reject_invalid_qty: Arc<AtomicU64>,
    pub(super) reject_rate_limit: Arc<AtomicU64>,
    pub(super) reject_risk: Arc<AtomicU64>,
    pub(super) reject_invalid_symbol: Arc<AtomicU64>,
    pub(super) reject_queue_full: Arc<AtomicU64>,
    pub(super) backpressure_soft_wal_age: Arc<AtomicU64>,
    pub(super) backpressure_soft_rate_decline: Arc<AtomicU64>,
    pub(super) backpressure_inflight: Arc<AtomicU64>,
    pub(super) backpressure_wal_bytes: Arc<AtomicU64>,
    pub(super) backpressure_wal_age: Arc<AtomicU64>,
    pub(super) backpressure_disk_free: Arc<AtomicU64>,
}

/// 認証エラーレスポンス
#[derive(serde::Serialize)]
pub(super) struct AuthErrorResponse {
    pub(super) error: String,
}

/// HTTPサーバーを起動
pub async fn run(
    port: u16,
    engine: FastPathEngine,
    order_store: Arc<OrderStore>,
    sse_hub: Arc<SseHub>,
    audit_log: Arc<AuditLog>,
    bus_publisher: Arc<BusPublisher>,
    bus_mode_outbox: bool,
    idempotency_ttl_sec: u64,
    durable_rx: Option<UnboundedReceiver<AuditDurableNotification>>,
) -> anyhow::Result<()> {
    let shard_count = std::env::var("ORDER_STORE_SHARDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(64);
    info!("ShardedOrderStore configured (shards={})", shard_count);

    let inflight_controller = crate::inflight::InflightController::spawn_from_env();
    let mut backpressure = BackpressureConfig::from_env();
    if inflight_controller.enabled() {
        backpressure.inflight_max = None;
    }
    let rate_limiter = crate::rate_limit::AccountRateLimiter::from_env().map(Arc::new);

    let audit_read_path = {
        let configured = std::env::var("AUDIT_LOG_PATH").ok().map(PathBuf::from);
        if let Some(path) = configured {
            path
        } else if std::env::var("AUDIT_MIRROR_ENABLE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            PathBuf::from(
                std::env::var("AUDIT_MIRROR_PATH")
                    .unwrap_or_else(|_| "var/gateway/audit.mirror.log".into()),
            )
        } else {
            audit_log.path().to_path_buf()
        }
    };

    let state = AppState {
        engine,
        jwt_auth: Arc::new(JwtAuth::from_env()),
        order_store,
        sharded_store: Arc::new(ShardedOrderStore::new_with_ttl_and_shards(
            idempotency_ttl_sec * 1000,
            shard_count,
        )),
        order_id_map: Arc::new(OrderIdMap::new()),
        sse_hub,
        order_id_seq: Arc::new(AtomicU64::new(1)),
        audit_log,
        audit_read_path: Arc::new(audit_read_path),
        bus_publisher,
        bus_mode_outbox,
        backpressure,
        inflight_controller,
        rate_limiter,
        ack_hist: Arc::new(LatencyHistogram::new()),
        wal_enqueue_hist: Arc::new(LatencyHistogram::new()),
        durable_ack_hist: Arc::new(LatencyHistogram::new()),
        fdatasync_hist: Arc::new(LatencyHistogram::new()),
        durable_notify_hist: Arc::new(LatencyHistogram::new()),
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
    };

    if let Some(rx) = durable_rx {
        let durable_state = state.clone();
        tokio::spawn(async move {
            run_durable_notifier(rx, durable_state).await;
        });
    }

    let app = Router::new()
        .route("/orders", post(handle_order))
        .route("/orders/{order_id}", get(handle_get_order))
        .route(
            "/orders/client/{client_order_id}",
            get(handle_get_order_by_client_id),
        )
        .route("/orders/{order_id}/cancel", post(handle_cancel_order))
        .route("/orders/{order_id}/events", get(handle_order_events))
        .route("/accounts/{account_id}/events", get(handle_account_events))
        .route("/orders/{order_id}/stream", get(handle_order_stream))
        .route("/stream", get(handle_account_stream))
        .route("/audit/verify", get(handle_audit_verify))
        .route("/audit/anchor", get(handle_audit_anchor))
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("HTTP server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn run_durable_notifier(
    mut rx: UnboundedReceiver<AuditDurableNotification>,
    state: AppState,
) {
    while let Some(note) = rx.recv().await {
        let notify_start_ns = gateway_core::now_nanos();
        if note.durable_done_ns >= note.request_start_ns && note.request_start_ns > 0 {
            let elapsed_us = (note.durable_done_ns - note.request_start_ns) / 1_000;
            state.durable_ack_hist.record(elapsed_us);
        }
        if note.fdatasync_ns > 0 {
            state.fdatasync_hist.record(note.fdatasync_ns / 1_000);
        }

        let durable_latency_us = if note.durable_done_ns >= note.request_start_ns {
            (note.durable_done_ns - note.request_start_ns) / 1_000
        } else {
            0
        };
        let data = serde_json::json!({
            "eventType": note.event_type,
            "durableLatencyUs": durable_latency_us,
        })
        .to_string();

        if note.event_type == "OrderAccepted" {
            if let Some(order_id) = note.order_id.as_deref() {
                state
                    .sharded_store
                    .mark_durable(order_id, &note.account_id, note.event_at);
                state.inflight_controller.on_commit(1);
                state
                    .sse_hub
                    .publish_order(order_id, "order_durable", &data);
            }
            state
                .sse_hub
                .publish_account(&note.account_id, "order_durable", &data);
        }

        // durable完了後の通知経路（channel受信〜SSE/状態更新）を観測。
        if note.durable_done_ns > 0 {
            let notify_us = gateway_core::now_nanos().saturating_sub(note.durable_done_ns) / 1_000;
            state.durable_notify_hist.record(notify_us);
        } else {
            let notify_us = gateway_core::now_nanos().saturating_sub(notify_start_ns) / 1_000;
            state.durable_notify_hist.record(notify_us);
        }
    }
}
