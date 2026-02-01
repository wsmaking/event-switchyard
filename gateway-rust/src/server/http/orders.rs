//! 注文API（HTTP入口層の中心）:
//! - 役割: 入口で受理した注文を FastPathEngine に投入し、監査ログ/Bus へ記録する。
//! - 位置: `server/http/mod.rs` から呼ばれる入口ハンドラ群（コアフローの同期境界）。
//! - 内包: 受理/取得/キャンセルとレスポンス型をこのファイルに集約。

use axum::{
    extract::{Path, State},
    http::{header::AUTHORIZATION, StatusCode},
    Json,
};
use axum::http::HeaderMap;
use std::sync::atomic::Ordering;
use crate::audit::{self, AuditEvent};
use crate::auth::{AuthError, AuthResult};
use crate::backpressure::{BackpressureLevel, BackpressureMetrics, BackpressureReason};
use crate::bus::BusEvent;
use crate::engine::{FastPathEngine, ProcessResult};
use crate::order::{OrderRequest, OrderResponse};
use crate::store::OrderSnapshot;
use gateway_core::now_nanos;

use super::{AppState, AuthErrorResponse};

// 注文系ハンドラ: 受付/取得/キャンセルとレスポンス変換を集約。

struct ControllerInflightGuard {
    handle: crate::inflight::InflightControllerHandle,
    active: bool,
}

impl ControllerInflightGuard {
    fn new(handle: crate::inflight::InflightControllerHandle) -> Self {
        Self { handle, active: true }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for ControllerInflightGuard {
    fn drop(&mut self) {
        if self.active {
            self.handle.release(1);
        }
    }
}

fn record_ack(state: &AppState, start_ns: u64) {
    let elapsed_us = now_nanos().saturating_sub(start_ns) / 1_000;
    state.ack_hist.record(elapsed_us);
}

fn record_wal_enqueue(state: &AppState, start_ns: u64, timings: audit::AuditAppendTimings) {
    if timings.enqueue_done_ns >= start_ns {
        let elapsed_us = (timings.enqueue_done_ns - start_ns) / 1_000;
        state.wal_enqueue_hist.record(elapsed_us);
    }
}

fn build_request_id(accept_seq: Option<u64>) -> Option<String> {
    accept_seq.map(|seq| format!("req_{}", seq))
}

/// 注文受付（POST /orders）
/// - JWT検証 → Idempotency-Key → FastPath → 監査/Bus/Snapshot 保存
pub(super) async fn handle_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> Result<(StatusCode, Json<OrderResponse>), (StatusCode, Json<AuthErrorResponse>)> {
    let t0 = now_nanos();
    let auth_header = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let principal = match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => p,
        AuthResult::Err(e) => {
            record_ack(&state, t0);
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

    let client_order_id = req.client_order_id.clone();
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| client_order_id.clone());

    if idempotency_key.is_none() {
        record_ack(&state, t0);
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(OrderResponse::rejected("IDEMPOTENCY_REQUIRED")),
        ));
    }

    let (mut inflight_guard, inflight) = match state.inflight_controller.reserve().await {
        crate::inflight::ReserveDecision::Allow { inflight, .. } => (
            Some(ControllerInflightGuard::new(state.inflight_controller.clone())),
            inflight,
        ),
        crate::inflight::ReserveDecision::RejectInflight { .. } => {
            state.backpressure_inflight.fetch_add(1, Ordering::Relaxed);
            record_ack(&state, t0);
            return Ok((
                StatusCode::TOO_MANY_REQUESTS,
                Json(OrderResponse::rejected("BACKPRESSURE_INFLIGHT_SOFT")),
            ));
        }
        crate::inflight::ReserveDecision::RejectRateDecline { .. } => {
            state.backpressure_soft_rate_decline.fetch_add(1, Ordering::Relaxed);
            record_ack(&state, t0);
            return Ok((
                StatusCode::TOO_MANY_REQUESTS,
                Json(OrderResponse::rejected("BACKPRESSURE_DURABLE_RATE_DECLINE")),
            ));
        }
    };
    let metrics = BackpressureMetrics {
        inflight,
        wal_bytes: state.audit_log.wal_bytes(),
        wal_age_ms: state.audit_log.wal_age_ms(),
        disk_free_pct: state.audit_log.disk_free_pct(),
    };
    if let Some(decision) = crate::backpressure::evaluate(&state.backpressure, &metrics) {
        let (status, reason) = match (decision.level, decision.reason) {
            (BackpressureLevel::Soft, BackpressureReason::SoftWalAge) => {
                state.backpressure_soft_wal_age.fetch_add(1, Ordering::Relaxed);
                (StatusCode::TOO_MANY_REQUESTS, "BACKPRESSURE_SOFT_WAL_AGE")
            }
            (_, BackpressureReason::Inflight) => {
                state.backpressure_inflight.fetch_add(1, Ordering::Relaxed);
                (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE_INFLIGHT")
            }
            (_, BackpressureReason::WalBytes) => {
                state.backpressure_wal_bytes.fetch_add(1, Ordering::Relaxed);
                (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE_WAL_BYTES")
            }
            (_, BackpressureReason::WalAge) => {
                state.backpressure_wal_age.fetch_add(1, Ordering::Relaxed);
                (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE_WAL_AGE")
            }
            (_, BackpressureReason::DiskFree) => {
                state.backpressure_disk_free.fetch_add(1, Ordering::Relaxed);
                (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE_DISK_FREE")
            }
            (_, BackpressureReason::SoftWalAge) => {
                state.backpressure_soft_wal_age.fetch_add(1, Ordering::Relaxed);
                (StatusCode::TOO_MANY_REQUESTS, "BACKPRESSURE_SOFT_WAL_AGE")
            }
        };
        record_ack(&state, t0);
        return Ok((status, Json(OrderResponse::rejected(reason))));
    }

    let symbol = FastPathEngine::symbol_to_bytes(&req.symbol);

    let account_id = principal.account_id.clone();
    let account_id_num: u64 = account_id.parse().unwrap_or(0);
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
                let accept_seq = state.order_id_map.to_internal(&existing.order_id);
                let request_id = build_request_id(accept_seq);
                record_ack(&state, t0);
                Ok((
                    StatusCode::ACCEPTED,
                    Json(OrderResponse::accepted(
                        &existing.order_id,
                        accept_seq,
                        request_id,
                        existing.client_order_id.clone(),
                    )),
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

                let timings = state.audit_log.append_with_timings(AuditEvent {
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
                }, t0);
                record_wal_enqueue(&state, t0, timings);
                if state.audit_log.async_enabled() {
                    if let Some(guard) = inflight_guard.as_mut() {
                        guard.disarm();
                    }
                }
                if !state.bus_mode_outbox {
                    state.bus_publisher.publish(BusEvent {
                        event_type: "OrderAccepted".into(),
                        at: crate::bus::format_event_time(audit::now_millis()),
                        account_id: bus_account_id,
                        order_id: Some(bus_order_id),
                        data: bus_data,
                    });
                }
                record_ack(&state, t0);
                let accept_seq = Some(internal_order_id);
                let request_id = build_request_id(accept_seq);
                Ok((
                    StatusCode::ACCEPTED,
                    Json(OrderResponse::accepted(
                        &snapshot.order_id,
                        accept_seq,
                        request_id,
                        snapshot.client_order_id.clone(),
                    )),
                ))
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
                if let Some(ref client_order_id) = client_order_id {
                    if process_result != ProcessResult::Accepted {
                        state
                            .sharded_store
                            .mark_rejected_client_order(&principal.account_id, client_order_id);
                    }
                }
                record_ack(&state, t0);
                Ok((status, Json(response)))
            }
        };
    }

    let internal_order_id = state.order_id_seq.fetch_add(1, Ordering::Relaxed);

    let result = state.engine.process_order(
        internal_order_id,
        account_id_num,
        symbol,
        req.side_byte(),
        req.qty as u32,
        price,
    );

    let (status, response) = match result {
        ProcessResult::Accepted => {
            let order_id = format!("ord_{}", uuid::Uuid::new_v4());
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

            state
                .sharded_store
                .put(snapshot, idempotency_key.as_deref());

            state.order_id_map.register_with_internal(
                internal_order_id,
                order_id.clone(),
                account_id,
            );

            let timings = state.audit_log.append_with_timings(AuditEvent {
                event_type: "OrderAccepted".into(),
                at: audit::now_millis(),
                account_id: audit_account_id.clone(),
                order_id: Some(audit_order_id.clone()),
                data: audit_data.clone(),
            }, t0);
            record_wal_enqueue(&state, t0, timings);
            if state.audit_log.async_enabled() {
                if let Some(guard) = inflight_guard.as_mut() {
                    guard.disarm();
                }
            }
            if !state.bus_mode_outbox {
                state.bus_publisher.publish(BusEvent {
                    event_type: "OrderAccepted".into(),
                    at: crate::bus::format_event_time(audit::now_millis()),
                    account_id: audit_account_id,
                    order_id: Some(audit_order_id),
                    data: audit_data,
                });
            }

            (
                StatusCode::ACCEPTED,
                OrderResponse::accepted(
                    &order_id,
                    Some(internal_order_id),
                    build_request_id(Some(internal_order_id)),
                    req.client_order_id.clone(),
                ),
            )
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
    if let Some(ref client_order_id) = client_order_id {
        if result != ProcessResult::Accepted {
            state
                .sharded_store
                .mark_rejected_client_order(&principal.account_id, client_order_id);
        }
    }

    record_ack(&state, t0);
    Ok((status, Json(response)))
}

/// 注文詳細取得（GET /orders/{order_id}）
/// - 外部ID→account_id でシャード検索、所有権を検証
pub(super) async fn handle_get_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Json<OrderSnapshotResponse>, (StatusCode, Json<AuthErrorResponse>)> {
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
        state
            .sharded_store
            .find_by_client_order_id(&principal.account_id, &order_id)
            .or_else(|| state.sharded_store.find_by_id(&order_id))
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

    Ok(Json(OrderSnapshotResponse::from(order)))
}

/// クライアント注文ID照会（GET /orders/client/{client_order_id}）
/// - PENDING/DURABLE/REJECTED/UNKNOWN を返す
pub(super) async fn handle_get_order_by_client_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(client_order_id): Path<String>,
) -> Result<Json<ClientOrderStatusResponse>, (StatusCode, Json<AuthErrorResponse>)> {
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

    if state
        .sharded_store
        .is_rejected_client_order(&principal.account_id, &client_order_id)
    {
        let response = ClientOrderStatusResponse {
            client_order_id,
            order_id: None,
            status: "REJECTED".into(),
        };
        return Ok(Json(response));
    }

    let order = state
        .sharded_store
        .find_by_client_order_id(&principal.account_id, &client_order_id);

    let response = if let Some(order) = order {
        let status = if order.status == crate::store::OrderStatus::Rejected {
            "REJECTED"
        } else if state
            .sharded_store
            .is_durable(&order.order_id, &order.account_id)
        {
            "DURABLE"
        } else {
            "PENDING"
        };
        ClientOrderStatusResponse {
            client_order_id,
            order_id: Some(order.order_id),
            status: status.into(),
        }
    } else {
        ClientOrderStatusResponse {
            client_order_id,
            order_id: None,
            status: "UNKNOWN".into(),
        }
    };

    Ok(Json(response))
}

/// 注文キャンセル（POST /orders/{order_id}/cancel）
/// - 状態遷移 → 監査/Bus に CancelRequested を記録
pub(super) async fn handle_cancel_order(
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
            at: crate::bus::format_event_time(audit::now_millis()),
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
pub(super) struct CancelResponse {
    order_id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

/// 注文スナップショットレスポンス
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrderSnapshotResponse {
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

/// クライアント注文ID照会レスポンス
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ClientOrderStatusResponse {
    client_order_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<String>,
    status: String,
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
