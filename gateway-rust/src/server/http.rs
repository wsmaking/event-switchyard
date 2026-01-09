//! HTTP サーバー
//!
//! Kotlin版 HttpGateway と同じAPIを提供。
//!
//! ## エンドポイント
//! - POST /orders: 注文受付 (JWT認証必須)
//! - GET /orders/:id: 注文詳細取得 (JWT認証必須)
//! - GET /orders/:id/stream: 注文SSEストリーム (JWT認証必須)
//! - GET /stream: アカウントSSEストリーム (JWT認証必須)
//! - GET /health: ヘルスチェック
//! - GET /metrics: メトリクス

use axum::{
    extract::{Path, State},
    http::{header::AUTHORIZATION, StatusCode},
    response::sse::{Event, Sse},
    routing::{get, post},
    Json, Router,
};
use axum::http::HeaderMap;
use futures::stream::Stream;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::auth::{AuthError, AuthResult, JwtAuth};
use crate::engine::{FastPathEngine, ProcessResult};
use crate::order::{HealthResponse, OrderRequest, OrderResponse};
use crate::sse::SseHub;
use crate::store::{OrderIdMap, OrderSnapshot, OrderStore, ShardedOrderStore};

/// アプリケーション状態
#[derive(Clone)]
struct AppState {
    engine: FastPathEngine,
    jwt_auth: Arc<JwtAuth>,
    /// レガシー OrderStore（後方互換用）
    #[allow(dead_code)]
    order_store: Arc<OrderStore>,
    /// HFT最適化版 ShardedOrderStore
    sharded_store: Arc<ShardedOrderStore>,
    /// 注文IDマッピング（内部ID ↔ 外部ID）
    order_id_map: Arc<OrderIdMap>,
    sse_hub: Arc<SseHub>,
    order_id_seq: Arc<AtomicU64>,
}

/// HTTPサーバーを起動
pub async fn run(
    port: u16,
    engine: FastPathEngine,
    order_store: Arc<OrderStore>,
    sse_hub: Arc<SseHub>,
) -> anyhow::Result<()> {
    let state = AppState {
        engine,
        jwt_auth: Arc::new(JwtAuth::from_env()),
        order_store,
        sharded_store: Arc::new(ShardedOrderStore::new()),
        order_id_map: Arc::new(OrderIdMap::new()),
        sse_hub,
        order_id_seq: Arc::new(AtomicU64::new(1)),
    };

    let app = Router::new()
        .route("/orders", post(handle_order))
        .route("/orders/{order_id}", get(handle_get_order))
        .route("/orders/{order_id}/stream", get(handle_order_stream))
        .route("/stream", get(handle_account_stream))
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

/// 認証エラーレスポンス
#[derive(serde::Serialize)]
struct AuthErrorResponse {
    error: String,
}

/// 注文受付ハンドラ
///
/// POST /orders
async fn handle_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> Result<(StatusCode, Json<OrderResponse>), (StatusCode, Json<AuthErrorResponse>)> {
    // JWT 認証
    let auth_header = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            let status = match e {
                AuthError::SecretNotConfigured => StatusCode::INTERNAL_SERVER_ERROR,
                AuthError::TokenExpired | AuthError::TokenNotYetValid => StatusCode::UNAUTHORIZED,
                _ => StatusCode::UNAUTHORIZED,
            };
            return Err((
                status,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    // 注文IDを発番（ord_UUID形式）
    let order_id = format!("ord_{}", uuid::Uuid::new_v4());

    // シンボルをバイト配列に変換
    let symbol = FastPathEngine::symbol_to_bytes(&req.symbol);

    // account_id は JWT から取得
    let account_id = principal.account_id.clone();
    let account_id_num: u64 = account_id.parse().unwrap_or(0);

    // 価格（LIMIT注文の場合は必須）
    let price = req.price.unwrap_or(0);

    // 内部order_idはシーケンス番号を使用
    let internal_order_id = state.order_id_seq.fetch_add(1, Ordering::Relaxed);

    // 注文処理
    let result = state.engine.process_order(
        internal_order_id,
        account_id_num,
        symbol,
        req.side_byte(),
        req.qty as u32,
        price,
    );

    // レスポンス生成（Kotlin Gateway互換）
    let (status, response) = match result {
        ProcessResult::Accepted => {
            // OrderSnapshot を作成
            let snapshot = OrderSnapshot::new(
                order_id.clone(),
                account_id.clone(),
                req.symbol.clone(),
                req.side.clone(),
                req.order_type,
                req.qty,
                req.price,
                req.time_in_force,
                req.expire_at,
                req.client_order_id.clone(),
            );

            // ShardedOrderStore に保存（HFT最適化）
            state.sharded_store.put(snapshot, None);

            // ID マッピングを登録
            state.order_id_map.register_with_internal(
                internal_order_id,
                order_id.clone(),
                account_id,
            );

            (StatusCode::ACCEPTED, OrderResponse::accepted(&order_id))
        }
        ProcessResult::RejectedMaxQty => (
            StatusCode::UNPROCESSABLE_ENTITY,
            OrderResponse::rejected("INVALID_QTY"),
        ),
        ProcessResult::RejectedMaxNotional => (
            StatusCode::UNPROCESSABLE_ENTITY,
            OrderResponse::rejected("RISK_REJECT"),
        ),
        ProcessResult::RejectedDailyLimit => (
            StatusCode::UNPROCESSABLE_ENTITY,
            OrderResponse::rejected("RISK_REJECT"),
        ),
        ProcessResult::RejectedUnknownSymbol => (
            StatusCode::UNPROCESSABLE_ENTITY,
            OrderResponse::rejected("INVALID_SYMBOL"),
        ),
        ProcessResult::ErrorQueueFull => (
            StatusCode::SERVICE_UNAVAILABLE,
            OrderResponse::rejected("QUEUE_REJECT"),
        ),
    };

    Ok((status, Json(response)))
}

/// 注文詳細取得ハンドラ
///
/// GET /orders/:order_id
async fn handle_get_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Json<OrderSnapshotResponse>, (StatusCode, Json<AuthErrorResponse>)> {
    // JWT 認証
    let auth_header = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    // ID マッピングから account_id を取得してシャード検索を高速化
    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);

    // 注文を検索（ShardedOrderStore）
    let order = if let Some(ref acc_id) = account_id_from_map {
        // 高速パス: account_id が分かっている場合は直接シャードを検索
        state.sharded_store.find_by_id_with_account(&order_id, acc_id)
    } else {
        // フォールバック: 全シャード検索
        state.sharded_store.find_by_id(&order_id)
    };

    let order = match order {
        Some(o) => o,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(AuthErrorResponse {
                    error: "NOT_FOUND".into(),
                }),
            ));
        }
    };

    // アカウント所有権チェック
    if order.account_id != principal.account_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    Ok(Json(OrderSnapshotResponse::from(order)))
}

/// 注文スナップショットレスポンス
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OrderSnapshotResponse {
    order_id: String,
    account_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_order_id: Option<String>,
    symbol: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    qty: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<u64>,
    time_in_force: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expire_at: Option<u64>,
    status: String,
    accepted_at: u64,
    last_update_at: u64,
    filled_qty: u64,
}

impl From<OrderSnapshot> for OrderSnapshotResponse {
    fn from(o: OrderSnapshot) -> Self {
        Self {
            order_id: o.order_id,
            account_id: o.account_id,
            client_order_id: o.client_order_id,
            symbol: o.symbol,
            side: o.side,
            order_type: match o.order_type {
                crate::order::OrderType::Limit => "LIMIT".into(),
                crate::order::OrderType::Market => "MARKET".into(),
            },
            qty: o.qty,
            price: o.price,
            time_in_force: match o.time_in_force {
                crate::order::TimeInForce::Gtc => "GTC".into(),
                crate::order::TimeInForce::Gtd => "GTD".into(),
            },
            expire_at: o.expire_at,
            status: o.status.as_str().into(),
            accepted_at: o.accepted_at,
            last_update_at: o.last_update_at,
            filled_qty: o.filled_qty,
        }
    }
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

/// 注文SSEストリームハンドラ
///
/// GET /orders/:order_id/stream
async fn handle_order_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<AuthErrorResponse>)>
{
    // JWT 認証
    let auth_header = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    // 注文の所有権チェック（ShardedOrderStore使用）
    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);
    let order = if let Some(ref acc_id) = account_id_from_map {
        state.sharded_store.find_by_id_with_account(&order_id, acc_id)
    } else {
        state.sharded_store.find_by_id(&order_id)
    };

    if let Some(order) = order {
        if order.account_id != principal.account_id {
            return Err((
                StatusCode::NOT_FOUND,
                Json(AuthErrorResponse {
                    error: "NOT_FOUND".into(),
                }),
            ));
        }
    }

    // SSEストリームを作成
    let rx = state.sse_hub.subscribe_order(&order_id);
    let stream = BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|event| {
            Ok(Event::default()
                .id(event.id.to_string())
                .event(event.event_type)
                .data(event.data))
        })
    });

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

/// アカウントSSEストリームハンドラ
///
/// GET /stream
async fn handle_account_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<AuthErrorResponse>)>
{
    // JWT 認証
    let auth_header = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    // SSEストリームを作成
    let rx = state.sse_hub.subscribe_account(&principal.account_id);
    let stream = BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|event| {
            Ok(Event::default()
                .id(event.id.to_string())
                .event(event.event_type)
                .data(event.data))
        })
    });

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}
