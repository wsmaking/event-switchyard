//! HTTP サーバー
//!
//! Kotlin版 HttpGateway と同じAPIを提供。
//!
//! ## エンドポイント
//! - POST /api/orders: 注文受付
//! - GET /health: ヘルスチェック
//! - GET /metrics: メトリクス

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::engine::{FastPathEngine, ProcessResult};
use crate::order::{HealthResponse, OrderRequest, OrderResponse};

/// アプリケーション状態
#[derive(Clone)]
struct AppState {
    engine: FastPathEngine,
    order_id_seq: Arc<AtomicU64>,
}

/// HTTPサーバーを起動
pub async fn run(port: u16, engine: FastPathEngine) -> anyhow::Result<()> {
    let state = AppState {
        engine,
        order_id_seq: Arc::new(AtomicU64::new(1)),
    };

    let app = Router::new()
        .route("/api/orders", post(handle_order))
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

/// 注文受付ハンドラ
///
/// POST /api/orders
async fn handle_order(
    State(state): State<AppState>,
    Json(req): Json<OrderRequest>,
) -> (StatusCode, Json<OrderResponse>) {
    let start = gateway_core::now_nanos();

    // 注文IDを発番
    let order_id = state.order_id_seq.fetch_add(1, Ordering::Relaxed);

    // シンボルをバイト配列に変換
    let symbol = FastPathEngine::symbol_to_bytes(&req.symbol);

    // 注文処理
    let result = state.engine.process_order(
        order_id,
        req.account_id,
        symbol,
        req.side_byte(),
        req.qty,
        req.price,
    );

    let elapsed = gateway_core::now_nanos() - start;

    // レスポンス生成
    let (status, response) = match result {
        ProcessResult::Accepted => (
            StatusCode::OK,
            OrderResponse::accepted(order_id).with_latency(elapsed),
        ),
        ProcessResult::RejectedMaxQty => (
            StatusCode::BAD_REQUEST,
            OrderResponse::rejected(order_id, "Quantity exceeds limit").with_latency(elapsed),
        ),
        ProcessResult::RejectedMaxNotional => (
            StatusCode::BAD_REQUEST,
            OrderResponse::rejected(order_id, "Notional exceeds limit").with_latency(elapsed),
        ),
        ProcessResult::RejectedDailyLimit => (
            StatusCode::BAD_REQUEST,
            OrderResponse::rejected(order_id, "Daily limit exceeded").with_latency(elapsed),
        ),
        ProcessResult::RejectedUnknownSymbol => (
            StatusCode::BAD_REQUEST,
            OrderResponse::rejected(order_id, "Unknown symbol").with_latency(elapsed),
        ),
        ProcessResult::ErrorQueueFull => (
            StatusCode::SERVICE_UNAVAILABLE,
            OrderResponse::error(order_id, "Queue full").with_latency(elapsed),
        ),
    };

    (status, Json(response))
}

/// ヘルスチェックハンドラ
///
/// GET /health
async fn handle_health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "OK".into(),
        queue_len: state.engine.queue_len(),
        latency_p50_ns: state.engine.latency_p50(),
        latency_p99_ns: state.engine.latency_p99(),
    })
}

/// メトリクスハンドラ
///
/// GET /metrics
/// Prometheus形式で出力
async fn handle_metrics(State(state): State<AppState>) -> String {
    let snapshot = format!(
        "# HELP gateway_queue_len Current queue length\n\
         # TYPE gateway_queue_len gauge\n\
         gateway_queue_len {}\n\
         # HELP gateway_latency_p50_ns Latency p50 in nanoseconds\n\
         # TYPE gateway_latency_p50_ns gauge\n\
         gateway_latency_p50_ns {}\n\
         # HELP gateway_latency_p99_ns Latency p99 in nanoseconds\n\
         # TYPE gateway_latency_p99_ns gauge\n\
         gateway_latency_p99_ns {}\n\
         # HELP gateway_latency_max_ns Latency max in nanoseconds\n\
         # TYPE gateway_latency_max_ns gauge\n\
         gateway_latency_max_ns {}\n",
        state.engine.queue_len(),
        state.engine.latency_p50(),
        state.engine.latency_p99(),
        state.engine.latency_max(),
    );
    snapshot
}
