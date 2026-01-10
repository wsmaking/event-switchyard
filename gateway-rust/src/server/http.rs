//! HTTP サーバー
//!
//! Kotlin版 HttpGateway と同じAPIを提供。
//!
//! ## エンドポイント
//! - POST /orders: 注文受付 (JWT認証必須)
//! - GET /orders/:id: 注文詳細取得 (JWT認証必須)
//! - GET /orders/:id/stream: 注文SSEストリーム (JWT認証必須)
//! - GET /stream: アカウントSSEストリーム (JWT認証必須)
//! - GET /audit/verify: 監査ログ検証 (JWT認証必須)
//! - GET /health: ヘルスチェック
//! - GET /metrics: メトリクス

use axum::{
    extract::{Path, State, Query},
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

use crate::audit::{self, AuditAnchor, AuditEvent, AuditLog, AuditVerifyResult};
use crate::bus::{BusEvent, BusPublisher};
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
    audit_log: Arc<AuditLog>,
    bus_publisher: Arc<BusPublisher>,
    bus_mode_outbox: bool,
    idempotency_checked: Arc<AtomicU64>,
    idempotency_hits: Arc<AtomicU64>,
    idempotency_creates: Arc<AtomicU64>,
    reject_invalid_qty: Arc<AtomicU64>,
    reject_risk: Arc<AtomicU64>,
    reject_invalid_symbol: Arc<AtomicU64>,
    reject_queue_full: Arc<AtomicU64>,
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
) -> anyhow::Result<()> {
    let shard_count = std::env::var("ORDER_STORE_SHARDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(64);
    info!("ShardedOrderStore configured (shards={})", shard_count);

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
        bus_publisher,
        bus_mode_outbox,
        idempotency_checked: Arc::new(AtomicU64::new(0)),
        idempotency_hits: Arc::new(AtomicU64::new(0)),
        idempotency_creates: Arc::new(AtomicU64::new(0)),
        reject_invalid_qty: Arc::new(AtomicU64::new(0)),
        reject_risk: Arc::new(AtomicU64::new(0)),
        reject_invalid_symbol: Arc::new(AtomicU64::new(0)),
        reject_queue_full: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/orders", post(handle_order))
        .route("/orders/{order_id}", get(handle_get_order))
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

/// 認証エラーレスポンス
#[derive(serde::Serialize)]
struct AuthErrorResponse {
    error: String,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelResponse {
    order_id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuditVerifyQuery {
    #[serde(default)]
    from_seq: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
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

    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    // シンボルをバイト配列に変換
    let symbol = FastPathEngine::symbol_to_bytes(&req.symbol);

    // account_id は JWT から取得
    let account_id = principal.account_id.clone();
    let account_id_num: u64 = account_id.parse().unwrap_or(0);

    // 価格（LIMIT注文の場合は必須）
    let price = req.price.unwrap_or(0);

    if let Some(ref key) = idempotency_key {
        state.idempotency_checked.fetch_add(1, Ordering::Relaxed);
        let order_id = format!("ord_{}", uuid::Uuid::new_v4());
        let internal_order_id = state.order_id_seq.fetch_add(1, Ordering::Relaxed);
        let mut process_result = ProcessResult::ErrorQueueFull;
        let outcome = state.sharded_store.get_or_create_idempotency(&account_id, key, || {
            let result = state.engine.process_order(
                internal_order_id,
                account_id_num,
                symbol,
                req.side_byte(),
                req.qty as u32,
                price,
            );
            process_result = result.clone();
            if result == ProcessResult::Accepted {
                Some(OrderSnapshot::new(
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
                ))
            } else {
                None
            }
        });

        return match outcome {
            crate::store::IdempotencyOutcome::Existing(existing) => {
                state.idempotency_hits.fetch_add(1, Ordering::Relaxed);
                Ok((
                    StatusCode::ACCEPTED,
                    Json(OrderResponse::accepted(&existing.order_id)),
                ))
            }
            crate::store::IdempotencyOutcome::Created(snapshot) => {
                state.idempotency_creates.fetch_add(1, Ordering::Relaxed);
                state.order_id_map.register_with_internal(
                    internal_order_id,
                    snapshot.order_id.clone(),
                    account_id,
                );

                let bus_account_id = snapshot.account_id.clone();
                let bus_order_id = snapshot.order_id.clone();
                let bus_data = serde_json::json!({
                    "symbol": snapshot.symbol,
                    "side": snapshot.side,
                    "type": match snapshot.order_type {
                        crate::order::OrderType::Limit => "LIMIT",
                        crate::order::OrderType::Market => "MARKET",
                    },
                    "qty": snapshot.qty,
                    "price": snapshot.price,
                    "timeInForce": match snapshot.time_in_force {
                        crate::order::TimeInForce::Gtc => "GTC",
                        crate::order::TimeInForce::Gtd => "GTD",
                    },
                    "expireAt": snapshot.expire_at,
                    "clientOrderId": snapshot.client_order_id,
                });

                state.audit_log.append(AuditEvent {
                    event_type: "OrderAccepted".into(),
                    at: audit::now_millis(),
                    account_id: snapshot.account_id.clone(),
                    order_id: Some(snapshot.order_id.clone()),
                    data: serde_json::json!({
                        "symbol": snapshot.symbol,
                        "side": snapshot.side,
                        "type": match snapshot.order_type {
                            crate::order::OrderType::Limit => "LIMIT",
                            crate::order::OrderType::Market => "MARKET",
                        },
                        "qty": snapshot.qty,
                        "price": snapshot.price,
                        "timeInForce": match snapshot.time_in_force {
                            crate::order::TimeInForce::Gtc => "GTC",
                            crate::order::TimeInForce::Gtd => "GTD",
                        },
                        "expireAt": snapshot.expire_at,
                        "clientOrderId": snapshot.client_order_id,
                    }),
                });
                if !state.bus_mode_outbox {
                    state.bus_publisher.publish(BusEvent {
                        event_type: "OrderAccepted".into(),
                        at: audit::now_millis(),
                        account_id: bus_account_id,
                        order_id: Some(bus_order_id),
                        data: bus_data,
                    });
                }
                Ok((StatusCode::ACCEPTED, Json(OrderResponse::accepted(&snapshot.order_id))))
            }
            crate::store::IdempotencyOutcome::NotCreated => {
                let (status, response) = match process_result {
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
                    ProcessResult::Accepted => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        OrderResponse::rejected("ERROR"),
                    ),
                };
                match process_result {
                    ProcessResult::RejectedMaxQty => {
                        state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
                    }
                    ProcessResult::RejectedMaxNotional | ProcessResult::RejectedDailyLimit => {
                        state.reject_risk.fetch_add(1, Ordering::Relaxed);
                    }
                    ProcessResult::RejectedUnknownSymbol => {
                        state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
                    }
                    ProcessResult::ErrorQueueFull => {
                        state.reject_queue_full.fetch_add(1, Ordering::Relaxed);
                    }
                    ProcessResult::Accepted => {}
                }
                Ok((status, Json(response)))
            }
        };
    }

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
            // 注文IDを発番（ord_UUID形式）
            let order_id = format!("ord_{}", uuid::Uuid::new_v4());
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

            let audit_account_id = snapshot.account_id.clone();
            let audit_order_id = snapshot.order_id.clone();
            let audit_data = serde_json::json!({
                "symbol": snapshot.symbol,
                "side": snapshot.side,
                "type": match snapshot.order_type {
                    crate::order::OrderType::Limit => "LIMIT",
                    crate::order::OrderType::Market => "MARKET",
                },
                "qty": snapshot.qty,
                "price": snapshot.price,
                "timeInForce": match snapshot.time_in_force {
                    crate::order::TimeInForce::Gtc => "GTC",
                    crate::order::TimeInForce::Gtd => "GTD",
                },
                "expireAt": snapshot.expire_at,
                "clientOrderId": snapshot.client_order_id,
            });

            // ShardedOrderStore に保存（HFT最適化）
            state
                .sharded_store
                .put(snapshot, idempotency_key.as_deref());

            // ID マッピングを登録
            state.order_id_map.register_with_internal(
                internal_order_id,
                order_id.clone(),
                account_id,
            );

            state.audit_log.append(AuditEvent {
                event_type: "OrderAccepted".into(),
                at: audit::now_millis(),
                account_id: audit_account_id.clone(),
                order_id: Some(audit_order_id.clone()),
                data: audit_data.clone(),
            });
            if !state.bus_mode_outbox {
                state.bus_publisher.publish(BusEvent {
                    event_type: "OrderAccepted".into(),
                    at: audit::now_millis(),
                    account_id: audit_account_id,
                    order_id: Some(audit_order_id),
                    data: audit_data,
                });
            }

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

    match result {
        ProcessResult::RejectedMaxQty => {
            state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
        }
        ProcessResult::RejectedMaxNotional | ProcessResult::RejectedDailyLimit => {
            state.reject_risk.fetch_add(1, Ordering::Relaxed);
        }
        ProcessResult::RejectedUnknownSymbol => {
            state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
        }
        ProcessResult::ErrorQueueFull => {
            state.reject_queue_full.fetch_add(1, Ordering::Relaxed);
        }
        ProcessResult::Accepted => {}
    }

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

/// 注文キャンセルハンドラ
///
/// POST /orders/:order_id/cancel
async fn handle_cancel_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<(StatusCode, Json<CancelResponse>), (StatusCode, Json<AuthErrorResponse>)> {
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

    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);
    let order = if let Some(ref acc_id) = account_id_from_map {
        state.sharded_store.find_by_id_with_account(&order_id, acc_id)
    } else {
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

    if order.account_id != principal.account_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    if order.status.is_terminal() {
        return Ok((
            StatusCode::CONFLICT,
            Json(CancelResponse {
                order_id,
                status: "REJECTED".into(),
                reason: Some("ORDER_FINAL".into()),
            }),
        ));
    }

    if order.status == crate::store::OrderStatus::CancelRequested {
        return Ok((
            StatusCode::ACCEPTED,
            Json(CancelResponse {
                order_id,
                status: "CANCEL_REQUESTED".into(),
                reason: None,
            }),
        ));
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let _ = state.sharded_store.update(&order.order_id, &order.account_id, |prev| {
        let mut next = prev.clone();
        next.status = crate::store::OrderStatus::CancelRequested;
        next.last_update_at = now_ms;
        next
    });

    state.audit_log.append(AuditEvent {
        event_type: "CancelRequested".into(),
        at: audit::now_millis(),
        account_id: order.account_id.clone(),
        order_id: Some(order.order_id.clone()),
        data: serde_json::json!({}),
    });
    if !state.bus_mode_outbox {
        state.bus_publisher.publish(BusEvent {
            event_type: "CancelRequested".into(),
            at: audit::now_millis(),
            account_id: order.account_id.clone(),
            order_id: Some(order.order_id.clone()),
            data: serde_json::json!({}),
        });
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(CancelResponse {
            order_id,
            status: "CANCEL_REQUESTED".into(),
            reason: None,
        }),
    ))
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditEventsResponse {
    order_id: Option<String>,
    account_id: Option<String>,
    events: Vec<AuditEvent>,
}

async fn handle_order_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<AuditEventsResponse>, (StatusCode, Json<AuthErrorResponse>)> {
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

    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);
    let order = if let Some(ref acc_id) = account_id_from_map {
        state.sharded_store.find_by_id_with_account(&order_id, acc_id)
    } else {
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

    if order.account_id != principal.account_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);
    let since_ms = params.get("since").and_then(|v| audit::parse_time_param(v));
    let after_ms = params.get("after").and_then(|v| audit::parse_time_param(v));

    let events = state.audit_log.read_events(
        &order.account_id,
        Some(&order_id),
        limit,
        since_ms,
        after_ms,
    );
    Ok(Json(AuditEventsResponse {
        order_id: Some(order_id),
        account_id: None,
        events,
    }))
}

async fn handle_account_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<AuditEventsResponse>, (StatusCode, Json<AuthErrorResponse>)> {
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

    if account_id != principal.account_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);
    let since_ms = params.get("since").and_then(|v| audit::parse_time_param(v));
    let after_ms = params.get("after").and_then(|v| audit::parse_time_param(v));

    let events = state.audit_log.read_events(
        &account_id,
        None,
        limit,
        since_ms,
        after_ms,
    );
    Ok(Json(AuditEventsResponse {
        order_id: None,
        account_id: Some(account_id),
        events,
    }))
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

/// 監査ログ検証ハンドラ
///
/// GET /audit/verify
async fn handle_audit_verify(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AuditVerifyQuery>,
) -> Result<Json<AuditVerifyResult>, (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(_) => {
            let from_seq = query.from_seq.unwrap_or(0);
            let limit = query.limit.unwrap_or(1000);
            let result = state.audit_log.verify(from_seq, limit);
            Ok(Json(result))
        }
        AuthResult::Err(e) => {
            let status = match e {
                AuthError::SecretNotConfigured => StatusCode::INTERNAL_SERVER_ERROR,
                AuthError::TokenExpired | AuthError::TokenNotYetValid => StatusCode::UNAUTHORIZED,
                _ => StatusCode::UNAUTHORIZED,
            };
            Err((
                status,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// 監査ログの最新アンカー取得
///
/// GET /audit/anchor
async fn handle_audit_anchor(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<AuditAnchor>, (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(_) => match state.audit_log.anchor_snapshot() {
            Some(anchor) => Ok(Json(anchor)),
            None => Err((
                StatusCode::NOT_FOUND,
                Json(AuthErrorResponse {
                    error: "anchor not available".into(),
                }),
            )),
        },
        AuthResult::Err(e) => {
            let status = match e {
                AuthError::SecretNotConfigured => StatusCode::INTERNAL_SERVER_ERROR,
                AuthError::TokenExpired | AuthError::TokenNotYetValid => StatusCode::UNAUTHORIZED,
                _ => StatusCode::UNAUTHORIZED,
            };
            Err((
                status,
                Json(AuthErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// メトリクスハンドラ
///
/// GET /metrics
/// Prometheus形式で出力
async fn handle_metrics(State(state): State<AppState>) -> String {
    let bus_metrics = state.bus_publisher.metrics();
    let outbox_metrics = crate::outbox::metrics();
    let idempotency_hits = state.idempotency_hits.load(Ordering::Relaxed);
    let idempotency_creates = state.idempotency_creates.load(Ordering::Relaxed);
    let idempotency_expired = state.sharded_store.idempotency_expired_total();
    let idempotency_checked = state.idempotency_checked.load(Ordering::Relaxed);
    let idempotency_handled = idempotency_hits + idempotency_creates + idempotency_expired;
    let idempotency_handled_ratio =
        if idempotency_checked == 0 { 0.0 } else { idempotency_handled as f64 / idempotency_checked as f64 };
    let idempotency_expired_ratio =
        if idempotency_checked == 0 { 0.0 } else { idempotency_expired as f64 / idempotency_checked as f64 };
    let bus_enabled = if bus_metrics.enabled { 1 } else { 0 };
    let reject_invalid_qty = state.reject_invalid_qty.load(Ordering::Relaxed);
    let reject_risk = state.reject_risk.load(Ordering::Relaxed);
    let reject_invalid_symbol = state.reject_invalid_symbol.load(Ordering::Relaxed);
    let reject_queue_full = state.reject_queue_full.load(Ordering::Relaxed);

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
         # HELP gateway_reject_risk_total Total rejects due to risk limits\n\
         # TYPE gateway_reject_risk_total counter\n\
         gateway_reject_risk_total {}\n\
         # HELP gateway_reject_invalid_symbol_total Total rejects due to invalid symbol\n\
         # TYPE gateway_reject_invalid_symbol_total counter\n\
         gateway_reject_invalid_symbol_total {}\n\
         # HELP gateway_reject_queue_full_total Total rejects due to queue full\n\
         # TYPE gateway_reject_queue_full_total counter\n\
         gateway_reject_queue_full_total {}\n\
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
        reject_risk,
        reject_invalid_symbol,
        reject_queue_full,
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

    let last_event_id = parse_last_event_id(&headers);
    let replay = state.sse_hub.replay_order(&order_id, last_event_id);
    let mut initial = Vec::new();
    if let Some(resync) = replay.resync_required {
        let data = serde_json::json!({
            "scope": "order",
            "id": order_id,
            "lastEventId": resync.last_event_id,
            "oldestAvailableId": resync.oldest_available_id,
            "eventsEndpoint": format!("/orders/{}/events", order_id),
        });
        initial.push(Event::default().event("resync_required").data(data.to_string()));
    }
    for ev in replay.events {
        initial.push(Event::default()
            .id(ev.id.to_string())
            .event(ev.event_type)
            .data(ev.data));
    }

    let rx = state.sse_hub.subscribe_order(&order_id);
    let live = BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|event| {
            Ok(Event::default()
                .id(event.id.to_string())
                .event(event.event_type)
                .data(event.data))
        })
    });
    let stream = futures::stream::iter(initial.into_iter().map(Ok)).chain(live);

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

    let last_event_id = parse_last_event_id(&headers);
    let replay = state.sse_hub.replay_account(&principal.account_id, last_event_id);
    let mut initial = Vec::new();
    if let Some(resync) = replay.resync_required {
        let data = serde_json::json!({
            "scope": "account",
            "id": principal.account_id,
            "lastEventId": resync.last_event_id,
            "oldestAvailableId": resync.oldest_available_id,
            "eventsEndpoint": format!("/accounts/{}/events", principal.account_id),
        });
        initial.push(Event::default().event("resync_required").data(data.to_string()));
    }
    for ev in replay.events {
        initial.push(Event::default()
            .id(ev.id.to_string())
            .event(ev.event_type)
            .data(ev.data));
    }

    let rx = state.sse_hub.subscribe_account(&principal.account_id);
    let live = BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|event| {
            Ok(Event::default()
                .id(event.id.to_string())
                .event(event.event_type)
                .data(event.data))
        })
    });
    let stream = futures::stream::iter(initial.into_iter().map(Ok)).chain(live);

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

fn parse_last_event_id(headers: &HeaderMap) -> Option<u64> {
    let raw = headers.get("Last-Event-ID")?.to_str().ok()?.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<u64>().ok()
}
