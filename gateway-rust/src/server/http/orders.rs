//! 注文API（HTTP入口層の中心）:
//! - 役割: 入口で受理した注文を FastPathEngine に投入し、監査ログ/Bus へ記録する。
//! - 位置: `server/http/mod.rs` から呼ばれる入口ハンドラ群（コアフローの同期境界）。
//! - 内包: 受理/取得/キャンセルとレスポンス型をこのファイルに集約。

use crate::audit::{self, AuditEvent};
use crate::auth::{AuthError, AuthResult};
use crate::backpressure::{BackpressureLevel, BackpressureMetrics, BackpressureReason};
use crate::bus::BusEvent;
use crate::engine::{FastPathEngine, ProcessResult};
use crate::order::{OrderRequest, OrderResponse};
use crate::store::OrderSnapshot;
use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, header::AUTHORIZATION},
};
use gateway_core::now_nanos;
use std::{sync::atomic::Ordering, time::Duration};

use super::{AppState, AuthErrorResponse, V3OrderTask};

type AuthResponse<T> = Result<T, (StatusCode, Json<AuthErrorResponse>)>;
type OrderResponseResult =
    Result<(StatusCode, Json<OrderResponse>), (StatusCode, Json<AuthErrorResponse>)>;
type VolatileOrderResponseResult =
    Result<(StatusCode, Json<VolatileOrderResponse>), (StatusCode, Json<AuthErrorResponse>)>;
type DurableOrderStatusResult =
    Result<Json<DurableOrderStatusResponse>, (StatusCode, Json<AuthErrorResponse>)>;

// 注文系ハンドラ: 受付/取得/キャンセルとレスポンス変換を集約。

// Inflight予約のリリース漏れを防ぐRAIIガード。
// 早期returnやエラーでもDropでreleaseされる。
struct ControllerInflightGuard {
    handle: crate::inflight::InflightControllerHandle,
    active: bool,
}

impl ControllerInflightGuard {
    fn new(handle: crate::inflight::InflightControllerHandle) -> Self {
        Self {
            handle,
            active: true,
        }
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

// 入口からのack遅延を観測するための計測。
fn record_ack(state: &AppState, start_ns: u64) {
    let elapsed_us = now_nanos().saturating_sub(start_ns) / 1_000;
    state.ack_hist.record(elapsed_us);
}

fn record_v3_ack(state: &AppState, start_ns: u64) {
    let elapsed_us = now_nanos().saturating_sub(start_ns) / 1_000;
    state.ack_hist.record(elapsed_us);
    state.v3_live_ack_hist.record(elapsed_us);
}

fn record_v3_ack_accepted(state: &AppState, start_ns: u64) {
    let elapsed_us = now_nanos().saturating_sub(start_ns) / 1_000;
    state.v3_live_ack_accepted_hist.record(elapsed_us);
}

// WAL enqueue完了までの遅延を観測。
fn record_wal_enqueue(state: &AppState, start_ns: u64, timings: audit::AuditAppendTimings) {
    if timings.enqueue_done_ns >= start_ns {
        let elapsed_us = (timings.enqueue_done_ns - start_ns) / 1_000;
        state.wal_enqueue_hist.record(elapsed_us);
    }
}

// 内部シーケンスを外部向けrequest_idに変換。
fn build_request_id(accept_seq: Option<u64>) -> Option<String> {
    accept_seq.map(|seq| format!("req_{}", seq))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OrderIngressContract {
    Legacy,
    V2,
}

fn map_existing_response(
    contract: OrderIngressContract,
    state: &AppState,
    existing: &OrderSnapshot,
    accept_seq: Option<u64>,
    request_id: Option<String>,
) -> (StatusCode, OrderResponse) {
    match contract {
        OrderIngressContract::Legacy => (
            StatusCode::ACCEPTED,
            OrderResponse::accepted(
                &existing.order_id,
                accept_seq,
                request_id,
                existing.client_order_id.clone(),
            ),
        ),
        OrderIngressContract::V2 => {
            if state
                .sharded_store
                .is_durable(&existing.order_id, &existing.account_id)
            {
                (
                    StatusCode::OK,
                    OrderResponse::durable(
                        &existing.order_id,
                        accept_seq,
                        request_id,
                        existing.client_order_id.clone(),
                    ),
                )
            } else {
                (
                    StatusCode::ACCEPTED,
                    OrderResponse::pending(
                        &existing.order_id,
                        accept_seq,
                        request_id,
                        existing.client_order_id.clone(),
                    ),
                )
            }
        }
    }
}

fn map_created_response(
    contract: OrderIngressContract,
    snapshot: &OrderSnapshot,
    accept_seq: Option<u64>,
    request_id: Option<String>,
) -> (StatusCode, OrderResponse) {
    match contract {
        OrderIngressContract::Legacy => (
            StatusCode::ACCEPTED,
            OrderResponse::accepted(
                &snapshot.order_id,
                accept_seq,
                request_id,
                snapshot.client_order_id.clone(),
            ),
        ),
        OrderIngressContract::V2 => (
            StatusCode::ACCEPTED,
            OrderResponse::pending(
                &snapshot.order_id,
                accept_seq,
                request_id,
                snapshot.client_order_id.clone(),
            ),
        ),
    }
}

fn finalize_sync_durable_v2(
    state: &AppState,
    snapshot: &OrderSnapshot,
    event_at_ms: u64,
    start_ns: u64,
    timings: audit::AuditAppendTimings,
    inflight_guard: &mut Option<ControllerInflightGuard>,
) {
    if let Some(guard) = inflight_guard.as_mut() {
        guard.disarm();
    }

    if timings.durable_done_ns >= start_ns && start_ns > 0 {
        let elapsed_us = (timings.durable_done_ns - start_ns) / 1_000;
        state.durable_ack_hist.record(elapsed_us);
    }
    if timings.fdatasync_ns > 0 {
        state.fdatasync_hist.record(timings.fdatasync_ns / 1_000);
    }

    if !state
        .sharded_store
        .mark_durable(&snapshot.order_id, &snapshot.account_id, event_at_ms)
    {
        return;
    }
    state.inflight_controller.on_commit(1);

    let durable_latency_us = if timings.durable_done_ns >= start_ns {
        (timings.durable_done_ns - start_ns) / 1_000
    } else {
        0
    };
    let data = serde_json::json!({
        "eventType": "OrderAccepted",
        "durableLatencyUs": durable_latency_us,
    })
    .to_string();
    state
        .sse_hub
        .publish_order(&snapshot.order_id, "order_durable", &data);
    state
        .sse_hub
        .publish_account(&snapshot.account_id, "order_durable", &data);

    if timings.durable_done_ns > 0 {
        let notify_us = now_nanos().saturating_sub(timings.durable_done_ns) / 1_000;
        state.durable_notify_hist.record(notify_us);
    }
}

fn map_snapshot_status_to_v2(state: &AppState, snapshot: &mut OrderSnapshotResponse) {
    if snapshot.status == "REJECTED" {
        return;
    }
    if state
        .sharded_store
        .is_durable(&snapshot.order_id, &snapshot.account_id)
    {
        snapshot.status = "DURABLE".into();
    } else {
        snapshot.status = "PENDING".into();
    }
}

fn authenticate_request(
    state: &AppState,
    headers: &HeaderMap,
    start_ns: u64,
) -> AuthResponse<crate::auth::Principal> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());

    match state.jwt_auth.authenticate(auth_header) {
        AuthResult::Ok(p) => Ok(p),
        AuthResult::Err(e) => {
            record_ack(state, start_ns);
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

fn build_idempotency_key(headers: &HeaderMap, req: &OrderRequest) -> Option<String> {
    let client_order_id = req.client_order_id.clone();
    headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| client_order_id.clone())
}

async fn reserve_inflight(
    state: &AppState,
    start_ns: u64,
) -> Result<(Option<ControllerInflightGuard>, u64), (StatusCode, Json<OrderResponse>)> {
    match state.inflight_controller.reserve().await {
        // inflight枠を1件分確保。guardのDropでreleaseされる。
        crate::inflight::ReserveDecision::Allow { inflight, .. } => Ok((
            Some(ControllerInflightGuard::new(
                state.inflight_controller.clone(),
            )),
            inflight,
        )),
        // inflight上限超過でソフト拒否。429で即時返却する。
        crate::inflight::ReserveDecision::RejectInflight { .. } => {
            state.backpressure_inflight.fetch_add(1, Ordering::Relaxed);
            record_ack(state, start_ns);
            Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(OrderResponse::rejected("BACKPRESSURE_INFLIGHT_SOFT")),
            ))
        }
        // durable処理レート低下を検知したためソフト拒否。429で即時返却する。
        crate::inflight::ReserveDecision::RejectRateDecline { .. } => {
            state
                .backpressure_soft_rate_decline
                .fetch_add(1, Ordering::Relaxed);
            record_ack(state, start_ns);
            Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(OrderResponse::rejected("BACKPRESSURE_DURABLE_RATE_DECLINE")),
            ))
        }
    }
}

fn apply_backpressure(
    state: &AppState,
    start_ns: u64,
    inflight: u64,
) -> Result<(), (StatusCode, Json<OrderResponse>)> {
    // 現在値をまとめて評価器へ渡し、閾値超過なら入口で早期拒否する。
    let metrics = BackpressureMetrics {
        inflight,
        wal_bytes: state.audit_log.wal_bytes(),
        wal_age_ms: state.audit_log.wal_age_ms(),
        disk_free_pct: state.audit_log.disk_free_pct(),
    };
    if let Some(decision) = crate::backpressure::evaluate(&state.backpressure, &metrics) {
        // 判定レベル/理由をHTTPステータスと公開用理由コードに正規化する。
        let (status, reason) = match (decision.level, decision.reason) {
            // 軽度のWAL遅延は429でソフト拒否し、再試行余地を残す。
            (BackpressureLevel::Soft, BackpressureReason::SoftWalAge) => {
                state
                    .backpressure_soft_wal_age
                    .fetch_add(1, Ordering::Relaxed);
                (StatusCode::TOO_MANY_REQUESTS, "BACKPRESSURE_SOFT_WAL_AGE")
            }
            // inflight枠の飽和はサービス都合の過負荷として503。
            (_, BackpressureReason::Inflight) => {
                state.backpressure_inflight.fetch_add(1, Ordering::Relaxed);
                (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE_INFLIGHT")
            }
            // WALサイズ上限超過は保全優先で503。
            (_, BackpressureReason::WalBytes) => {
                state.backpressure_wal_bytes.fetch_add(1, Ordering::Relaxed);
                (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE_WAL_BYTES")
            }
            // WAL遅延のハード閾値超過は503。
            (_, BackpressureReason::WalAge) => {
                state.backpressure_wal_age.fetch_add(1, Ordering::Relaxed);
                (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE_WAL_AGE")
            }
            // ディスク空き容量不足は可用性保護のため503。
            (_, BackpressureReason::DiskFree) => {
                state.backpressure_disk_free.fetch_add(1, Ordering::Relaxed);
                (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE_DISK_FREE")
            }
            // SoftWalAgeの別経路（level非依存）も同じ理由コードに統一。
            (_, BackpressureReason::SoftWalAge) => {
                state
                    .backpressure_soft_wal_age
                    .fetch_add(1, Ordering::Relaxed);
                (StatusCode::TOO_MANY_REQUESTS, "BACKPRESSURE_SOFT_WAL_AGE")
            }
        };
        // 拒否経路でも入口から応答までのACK遅延を計測する。
        record_ack(state, start_ns);
        return Err((status, Json(OrderResponse::rejected(reason))));
    }
    Ok(())
}

fn parse_v3_symbol_key(raw: &str) -> Option<[u8; 8]> {
    let symbol = raw.trim();
    if symbol.is_empty() || symbol.len() > 8 {
        return None;
    }
    let mut key = [0u8; 8];
    for (i, b) in symbol.bytes().enumerate() {
        let up = b.to_ascii_uppercase();
        if !(up.is_ascii_uppercase() || up.is_ascii_digit() || matches!(up, b'_' | b'-' | b'.')) {
            return None;
        }
        key[i] = up;
    }
    Some(key)
}

fn evaluate_v3_hot_risk(state: &AppState, req: &OrderRequest) -> Result<(), &'static str> {
    let position_t0 = now_nanos();
    let side = req.side_byte();
    if side != 1 && side != 2 {
        return Err("INVALID_SIDE");
    }
    if req.qty == 0 || req.qty > state.v3_risk_max_order_qty {
        return Err("INVALID_QTY");
    }
    let symbol_key = match parse_v3_symbol_key(&req.symbol) {
        Some(v) => v,
        None => return Err("INVALID_SYMBOL"),
    };
    let symbol_limits = state.v3_symbol_limits.get(&symbol_key).copied();
    if state.v3_risk_strict_symbols && symbol_limits.is_none() {
        return Err("INVALID_SYMBOL");
    }
    let symbol_limits = symbol_limits.unwrap_or(gateway_core::SymbolLimits {
        max_order_qty: state.v3_risk_max_order_qty.min(u32::MAX as u64) as u32,
        max_notional: state.v3_risk_max_notional,
        tick_size: 1,
    });
    let max_qty = state
        .v3_risk_max_order_qty
        .min(symbol_limits.max_order_qty as u64);
    if req.qty > max_qty {
        return Err("INVALID_QTY");
    }
    let position_elapsed = now_nanos().saturating_sub(position_t0) / 1_000;
    state.v3_stage_risk_position_hist.record(position_elapsed);

    let price = req.price.unwrap_or(0);
    if req.order_type == crate::order::OrderType::Limit && price == 0 {
        return Err("INVALID_PRICE");
    }

    let margin_t0 = now_nanos();
    let notional = (req.qty as u128).saturating_mul(price as u128);
    let max_notional = state.v3_risk_max_notional.min(symbol_limits.max_notional);
    let margin_elapsed = now_nanos().saturating_sub(margin_t0) / 1_000;
    state.v3_stage_risk_margin_hist.record(margin_elapsed);

    let limits_t0 = now_nanos();
    if notional > max_notional as u128 {
        let limits_elapsed = now_nanos().saturating_sub(limits_t0) / 1_000;
        state.v3_stage_risk_limits_hist.record(limits_elapsed);
        return Err("RISK_REJECT");
    }
    let limits_elapsed = now_nanos().saturating_sub(limits_t0) / 1_000;
    state.v3_stage_risk_limits_hist.record(limits_elapsed);

    Ok(())
}

/// v3 注文受付（POST /v3/orders）
/// - hot path 最小化: parse -> risk -> shard enqueue -> VOLATILE_ACCEPT 応答
pub(super) async fn handle_order_v3(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> VolatileOrderResponseResult {
    let t0 = now_nanos();
    let principal = authenticate_request(&state, &headers, t0)?;
    let session_id = principal.session_id;
    let ingress = &state.v3_ingress;
    let shard_id = ingress.shard_for_session(&session_id);
    if ingress.maybe_recover_shard(shard_id, t0) {
        state
            .v3_kill_recovered_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // parse段階: JSON展開済みリクエストから必要項目を取り出す。
    let parse_t0 = now_nanos();
    let symbol = req.symbol.as_str();
    let qty = req.qty;
    let price = req.price.unwrap_or(0);
    let _ = (symbol, qty, price);
    let parse_elapsed = now_nanos().saturating_sub(parse_t0) / 1_000;
    state.v3_stage_parse_hist.record(parse_elapsed);

    if ingress.is_global_killed() {
        state
            .v3_rejected_killed_total
            .fetch_add(1, Ordering::Relaxed);
        record_v3_ack(&state, t0);
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(VolatileOrderResponse::rejected(
                &session_id,
                "KILLED",
                "V3_GLOBAL_KILLED",
            )),
        ));
    }
    if ingress.is_session_killed(&session_id) {
        state
            .v3_rejected_killed_total
            .fetch_add(1, Ordering::Relaxed);
        record_v3_ack(&state, t0);
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(VolatileOrderResponse::rejected(
                &session_id,
                "KILLED",
                "V3_SESSION_KILLED",
            )),
        ));
    }
    if ingress.is_shard_killed(shard_id) {
        state
            .v3_rejected_killed_total
            .fetch_add(1, Ordering::Relaxed);
        record_v3_ack(&state, t0);
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(VolatileOrderResponse::rejected(
                &session_id,
                "KILLED",
                "V3_SHARD_KILLED",
            )),
        ));
    }

    // risk段階: 最小の stateless チェックのみ実行し、共有ロックを避ける。
    let risk_t0 = now_nanos();
    let risk_result = evaluate_v3_hot_risk(&state, &req);
    let risk_elapsed = now_nanos().saturating_sub(risk_t0) / 1_000;
    state.v3_stage_risk_hist.record(risk_elapsed);

    match risk_result {
        Ok(()) => {}
        Err(reason) => {
            match reason {
                "INVALID_QTY" | "INVALID_SIDE" | "INVALID_PRICE" => {
                    state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
                }
                "RISK_REJECT" => {
                    state.reject_risk.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
                }
            }
            record_v3_ack(&state, t0);
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(VolatileOrderResponse::rejected(
                    &session_id,
                    "REJECTED",
                    reason,
                )),
            ));
        }
    }

    // SOFT/HARD/KILL の3段水位。
    let queue_pct = ingress.queue_utilization_pct(shard_id);
    if queue_pct >= state.v3_kill_reject_pct {
        if ingress.kill_shard_due_to_watermark(shard_id, t0) {
            state.v3_shard_killed_total.fetch_add(1, Ordering::Relaxed);
        }
        state
            .v3_rejected_killed_total
            .fetch_add(1, Ordering::Relaxed);
        record_v3_ack(&state, t0);
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(VolatileOrderResponse::rejected(
                &session_id,
                "KILLED",
                "V3_QUEUE_KILLED",
            )),
        ));
    }
    if queue_pct >= state.v3_hard_reject_pct {
        state.v3_rejected_hard_total.fetch_add(1, Ordering::Relaxed);
        record_v3_ack(&state, t0);
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(VolatileOrderResponse::rejected(
                &session_id,
                "REJECTED",
                "V3_BACKPRESSURE_HARD",
            )),
        ));
    }

    if queue_pct >= state.v3_soft_reject_pct {
        state.v3_rejected_soft_total.fetch_add(1, Ordering::Relaxed);
        record_v3_ack(&state, t0);
        return Ok((
            StatusCode::TOO_MANY_REQUESTS,
            Json(VolatileOrderResponse::rejected(
                &session_id,
                "REJECTED",
                "V3_BACKPRESSURE_SOFT",
            )),
        ));
    }

    let session_seq = ingress.next_seq(&session_id);
    let received_at_ns = now_nanos();
    let task = V3OrderTask {
        session_id: session_id.clone(),
        session_seq,
        attempt_seq: session_seq,
        received_at_ns,
        shard_id,
    };

    let enqueue_t0 = now_nanos();
    match ingress.try_enqueue(shard_id, task) {
        Ok(()) => {
            let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
            state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            state.v3_accepted_total.fetch_add(1, Ordering::Relaxed);
            let serialize_t0 = now_nanos();
            let body = VolatileOrderResponse::accepted(session_id, session_seq, received_at_ns);
            let serialize_elapsed = now_nanos().saturating_sub(serialize_t0) / 1_000;
            state.v3_stage_serialize_hist.record(serialize_elapsed);
            record_v3_ack(&state, t0);
            record_v3_ack_accepted(&state, t0);
            Ok((StatusCode::ACCEPTED, Json(body)))
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(task)) => {
            let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
            state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            if ingress.kill_shard_due_to_watermark(shard_id, now_nanos()) {
                state.v3_shard_killed_total.fetch_add(1, Ordering::Relaxed);
            }
            state.register_v3_loss_suspect(
                &task.session_id,
                task.session_seq,
                task.shard_id,
                "V3_INGRESS_QUEUE_FULL",
                now_nanos(),
            );
            state
                .v3_rejected_killed_total
                .fetch_add(1, Ordering::Relaxed);
            record_v3_ack(&state, t0);
            Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(VolatileOrderResponse::rejected(
                    &session_id,
                    "KILLED",
                    "V3_QUEUE_FULL",
                )),
            ))
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(task)) => {
            let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
            state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            if ingress.kill_shard_due_to_watermark(shard_id, now_nanos()) {
                state.v3_shard_killed_total.fetch_add(1, Ordering::Relaxed);
            }
            state.register_v3_loss_suspect(
                &task.session_id,
                task.session_seq,
                task.shard_id,
                "V3_INGRESS_CLOSED",
                now_nanos(),
            );
            state
                .v3_rejected_killed_total
                .fetch_add(1, Ordering::Relaxed);
            record_v3_ack(&state, t0);
            Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(VolatileOrderResponse::rejected(
                    &session_id,
                    "KILLED",
                    "V3_INGRESS_CLOSED",
                )),
            ))
        }
    }
}

/// v3 durable confirm 照会（GET /v3/orders/{sessionId}/{sessionSeq}）。
pub(super) async fn handle_get_order_v3(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session_id, session_seq)): Path<(String, u64)>,
) -> DurableOrderStatusResult {
    let t0 = now_nanos();
    let principal = authenticate_request(&state, &headers, t0)?;
    if principal.session_id != session_id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(AuthErrorResponse {
                error: "forbidden".to_string(),
            }),
        ));
    }

    let body = if let Some(snapshot) = state.v3_confirm_store.snapshot(&session_id, session_seq) {
        DurableOrderStatusResponse {
            session_id: session_id.clone(),
            session_seq,
            status: snapshot.status.as_str().to_string(),
            attempt_id: Some(format!("att_{}", snapshot.attempt_seq)),
            reason: snapshot.reason,
            received_at_ns: Some(snapshot.received_at_ns),
            updated_at_ns: snapshot.updated_at_ns,
            shard_id: snapshot.shard_id as u64,
        }
    } else {
        DurableOrderStatusResponse {
            session_id: session_id.clone(),
            session_seq,
            status: "UNKNOWN".to_string(),
            attempt_id: None,
            reason: None,
            received_at_ns: None,
            updated_at_ns: now_nanos(),
            shard_id: state.v3_ingress.shard_for_session(&session_id) as u64,
        }
    };

    Ok(Json(body))
}

/// 注文受付（POST /orders）
/// - JWT検証 → Idempotency-Key → FastPath → 監査/Bus/Snapshot 保存
pub(super) async fn handle_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> OrderResponseResult {
    handle_order_with_contract(state, headers, req, OrderIngressContract::Legacy).await
}

async fn handle_order_with_contract(
    state: AppState,
    headers: HeaderMap,
    req: OrderRequest,
    contract: OrderIngressContract,
) -> OrderResponseResult {
    if contract == OrderIngressContract::V2 {
        state.v2_requests_total.fetch_add(1, Ordering::Relaxed);
    }
    let t0 = now_nanos();
    let principal = authenticate_request(&state, &headers, t0)?;
    let client_order_id = req.client_order_id.clone();
    // 契約上は Idempotency-Key 必須。未指定時は client_order_id を補助キーとして使う。
    let idempotency_key = build_idempotency_key(&headers, &req);

    // 現在の契約では Idempotency-Key は必須。未指定は入口で即時拒否する。
    if idempotency_key.is_none() {
        record_ack(&state, t0);
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(OrderResponse::rejected("IDEMPOTENCY_REQUIRED")),
        ));
    }

    // account単位レート制限。超過時は業務処理に入る前に429で返す。
    if let Some(ref rate_limiter) = state.rate_limiter {
        if !rate_limiter.try_acquire(&principal.account_id) {
            state.reject_rate_limit.fetch_add(1, Ordering::Relaxed);
            record_ack(&state, t0);
            return Ok((
                StatusCode::TOO_MANY_REQUESTS,
                Json(OrderResponse::rejected("RATE_LIMITED")),
            ));
        }
    }

    // inflight枠を予約できた場合のみ本処理へ進む。
    let (mut inflight_guard, inflight) = match reserve_inflight(&state, t0).await {
        Ok(v) => v,
        Err((status, body)) => return Ok((status, body)),
    };
    // WAL状態/ディスク状態を含む追加の入口制御。
    if let Err((status, body)) = apply_backpressure(&state, t0, inflight) {
        return Ok((status, body));
    }

    // FastPath向けに固定長バイト配列へ変換
    let symbol = FastPathEngine::symbol_to_bytes(&req.symbol);

    let account_id = principal.account_id.clone();
    let account_id_num: u64 = account_id.parse().unwrap_or(0);
    let price = req.price.unwrap_or(0);

    // Idempotency-Key必須の契約に基づき、同一キーの二重受付を防ぐ。
    let key = idempotency_key
        .as_ref()
        .expect("idempotency key is validated above");
    state.idempotency_checked.fetch_add(1, Ordering::Relaxed);
    let order_id = format!("ord_{}", uuid::Uuid::new_v4());
    let internal_order_id = state.order_id_seq.fetch_add(1, Ordering::Relaxed);
    let mut process_result = ProcessResult::ErrorQueueFull;

    // 初回のみFastPathに流し、結果をsnapshot化して保存
    let outcome = state
        .sharded_store
        .get_or_create_idempotency(&account_id, key, || {
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

    // Idempotency判定の結果で、再送(Existing)か初回受理(Created)かを分岐する。
    match outcome {
        crate::store::IdempotencyOutcome::Existing(existing) => {
            // 既存注文をそのまま返す。FastPath/WAL/Bus は再実行しない。
            state.idempotency_hits.fetch_add(1, Ordering::Relaxed);
            let accept_seq = state.order_id_map.to_internal(&existing.order_id);
            let request_id = build_request_id(accept_seq);
            let (status, response) =
                map_existing_response(contract, &state, &existing, accept_seq, request_id);
            record_ack(&state, t0);
            Ok((status, Json(response)))
        }
        crate::store::IdempotencyOutcome::Created(snapshot) => {
            // 初回受理。以降は「ID登録 -> 監査記録 -> (必要なら)Bus送信 -> ACK」の順で進める。
            state.idempotency_creates.fetch_add(1, Ordering::Relaxed);
            state.order_id_map.register_with_internal(
                internal_order_id,
                snapshot.order_id.clone(),
                account_id,
            );

            let order_payload = serde_json::json!({
                "symbol": snapshot.symbol.clone(),
                "side": snapshot.side.clone(),
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
                "clientOrderId": snapshot.client_order_id.clone(),
            });
            let bus_account_id = snapshot.account_id.clone();
            let bus_order_id = snapshot.order_id.clone();
            let bus_data = order_payload.clone();

            // 受理イベントを監査ログ(WAL)へ追記する。
            // t0 は入口時刻で、enqueue完了までの遅延計測に使う。
            let audit_event_at = audit::now_millis();
            let audit_event = AuditEvent {
                event_type: "OrderAccepted".into(),
                at: audit_event_at,
                account_id: snapshot.account_id.clone(),
                order_id: Some(snapshot.order_id.clone()),
                data: order_payload,
            };
            let (timings, durable_receipt_rx) = if contract == OrderIngressContract::V2 {
                let append = state.audit_log.append_with_durable_receipt(audit_event, t0);
                (append.timings, append.durable_rx)
            } else {
                (state.audit_log.append_with_timings(audit_event, t0), None)
            };
            // WAL enqueue までの遅延を別ヒストグラムで観測する。
            record_wal_enqueue(&state, t0, timings);
            if contract == OrderIngressContract::V2 {
                if timings.durable_done_ns > 0 {
                    finalize_sync_durable_v2(
                        &state,
                        &snapshot,
                        audit_event_at,
                        t0,
                        timings,
                        &mut inflight_guard,
                    );
                } else if timings.enqueue_done_ns == 0 {
                    // enqueueもdurableも0の場合は、WAL書き込み自体に失敗している。
                    state.sharded_store.remove(
                        &snapshot.order_id,
                        &snapshot.account_id,
                        Some(key.as_str()),
                    );
                    state.order_id_map.remove(internal_order_id);
                    record_ack(&state, t0);
                    return Ok((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                    ));
                } else if state.audit_log.async_enabled() {
                    if let Some(guard) = inflight_guard.as_mut() {
                        // 非同期 durable 経路は notifier の on_commit で減算する。
                        guard.disarm();
                    }
                    if let Some(rx) = durable_receipt_rx {
                        let timeout =
                            Duration::from_millis(state.v2_durable_wait_timeout_ms.max(1));
                        match tokio::time::timeout(timeout, rx).await {
                            Ok(Ok(receipt)) => {
                                if receipt.durable_done_ns == 0 {
                                    state.sharded_store.remove(
                                        &snapshot.order_id,
                                        &snapshot.account_id,
                                        Some(key.as_str()),
                                    );
                                    state.order_id_map.remove(internal_order_id);
                                    record_ack(&state, t0);
                                    return Ok((
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                                    ));
                                }
                                finalize_sync_durable_v2(
                                    &state,
                                    &snapshot,
                                    audit_event_at,
                                    t0,
                                    audit::AuditAppendTimings {
                                        enqueue_done_ns: timings.enqueue_done_ns,
                                        durable_done_ns: receipt.durable_done_ns,
                                        fdatasync_ns: receipt.fdatasync_ns,
                                    },
                                    &mut inflight_guard,
                                );
                            }
                            Ok(Err(_)) => {
                                record_ack(&state, t0);
                                return Ok((
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                                ));
                            }
                            Err(_) => {
                                state
                                    .v2_durable_wait_timeout_total
                                    .fetch_add(1, Ordering::Relaxed);
                                record_ack(&state, t0);
                                return Ok((
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    Json(OrderResponse::rejected("DURABILITY_WAIT_TIMEOUT")),
                                ));
                            }
                        }
                    } else {
                        // 非同期writerのreceiptを受け取れない場合は契約を満たせないため失敗扱い。
                        state.sharded_store.remove(
                            &snapshot.order_id,
                            &snapshot.account_id,
                            Some(key.as_str()),
                        );
                        state.order_id_map.remove(internal_order_id);
                        record_ack(&state, t0);
                        return Ok((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                        ));
                    }
                } else {
                    // durable 化できなかった場合は作成済みエントリを巻き戻し、再試行可能にする。
                    state.sharded_store.remove(
                        &snapshot.order_id,
                        &snapshot.account_id,
                        Some(key.as_str()),
                    );
                    state.order_id_map.remove(internal_order_id);
                    record_ack(&state, t0);
                    return Ok((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(OrderResponse::rejected("WAL_DURABILITY_FAILED")),
                    ));
                }
            } else if state.audit_log.async_enabled() {
                if let Some(guard) = inflight_guard.as_mut() {
                    // 非同期WAL時はDrop時releaseを止め、durable通知側のon_commitで減算する。
                    guard.disarm();
                }
            }
            if !state.bus_mode_outbox {
                // Outboxモード以外は、この場でBusへ直接publishする。
                state.bus_publisher.publish(BusEvent {
                    event_type: "OrderAccepted".into(),
                    at: crate::bus::format_event_time(audit::now_millis()),
                    account_id: bus_account_id,
                    order_id: Some(bus_order_id),
                    data: bus_data,
                });
            }
            // 受理経路の終端としてACK遅延を計測する。
            record_ack(&state, t0);
            let accept_seq = Some(internal_order_id);
            let request_id = build_request_id(accept_seq);
            let (status, response) =
                map_created_response(contract, &snapshot, accept_seq, request_id);
            Ok((status, Json(response)))
        }
        crate::store::IdempotencyOutcome::NotCreated => {
            // 初回処理は走ったが受理されず、snapshotが作られなかったケース。
            // FastPathの結果をHTTPステータス/エラー理由へ正規化して返す。
            let (status, response) = match process_result {
                // 数量が業務上限を超過。入力値エラーとして422を返す。
                ProcessResult::RejectedMaxQty => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    OrderResponse::rejected("INVALID_QTY"),
                ),
                // 想定元本(数量×価格)が上限超過。リスク拒否として422を返す。
                ProcessResult::RejectedMaxNotional => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    OrderResponse::rejected("RISK_REJECT"),
                ),
                // 日次制限に抵触。リスク拒否として422を返す。
                ProcessResult::RejectedDailyLimit => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    OrderResponse::rejected("RISK_REJECT"),
                ),
                // 銘柄マスタに存在しない。入力不正として422を返す。
                ProcessResult::RejectedUnknownSymbol => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    OrderResponse::rejected("INVALID_SYMBOL"),
                ),
                // FastPathキューが飽和。サーバー都合の一時失敗として503を返す。
                ProcessResult::ErrorQueueFull => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    OrderResponse::rejected("QUEUE_REJECT"),
                ),
                // この分岐では本来到達しない想定。整合性破綻として500を返す。
                ProcessResult::Accepted => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    OrderResponse::rejected("ERROR"),
                ),
            };
            // 拒否理由ごとのカウンタを更新し、後で運用メトリクスとして観測できるようにする。
            match process_result {
                // 数量上限違反の件数。
                ProcessResult::RejectedMaxQty => {
                    state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
                }
                // リスク上限違反（想定元本/日次制限）の件数。
                ProcessResult::RejectedMaxNotional | ProcessResult::RejectedDailyLimit => {
                    state.reject_risk.fetch_add(1, Ordering::Relaxed);
                }
                // 未知銘柄で弾いた件数。
                ProcessResult::RejectedUnknownSymbol => {
                    state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
                }
                // キュー飽和で受理できなかった件数。
                ProcessResult::ErrorQueueFull => {
                    state.reject_queue_full.fetch_add(1, Ordering::Relaxed);
                }
                // 受理時は拒否カウンタを増やさない。
                ProcessResult::Accepted => {}
            }
            if let Some(ref client_order_id) = client_order_id {
                if process_result != ProcessResult::Accepted {
                    // 同じclient_order_idの再送で「過去に拒否済み」と判定できるよう記録する。
                    state
                        .sharded_store
                        .mark_rejected_client_order(&principal.account_id, client_order_id);
                }
            }
            // 拒否経路でも入口から応答までのACK遅延を計測する。
            record_ack(&state, t0);
            Ok((status, Json(response)))
        }
    }
}

/// v2 注文受付（POST /v2/orders）
/// - `PendingAccepted` の durable 完了まで待って `PENDING` で返す。
pub(super) async fn handle_order_v2(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> OrderResponseResult {
    handle_order_with_contract(state, headers, req, OrderIngressContract::V2).await
}

/// 注文詳細取得（GET /orders/{order_id}）
/// - 外部ID→account_id でシャード検索、所有権を検証
pub(super) async fn handle_get_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Json<OrderSnapshotResponse>, (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());

    // 呼び出し主体を認証し、account境界の判定に使う。
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

    // 可能ならIDマップ経由でaccountを先に特定し、シャード探索コストを下げる。
    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);
    let order = if let Some(ref acc_id) = account_id_from_map {
        state
            .sharded_store
            .find_by_id_with_account(&order_id, acc_id)
    } else {
        // マップ不在時は client_order_id 互換を含むフォールバック検索を行う。
        state
            .sharded_store
            .find_by_client_order_id(&principal.account_id, &order_id)
            .or_else(|| state.sharded_store.find_by_id(&order_id))
    };

    // 注文が見つからなければ404。
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

    // 他人の注文を推測できないよう、未存在と同じ404で隠蔽する。
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

/// v2 注文詳細取得（GET /v2/orders/{order_id}）
/// - 状態を `PENDING/DURABLE/REJECTED` に正規化して返す。
pub(super) async fn handle_get_order_v2(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Json<OrderSnapshotResponse>, (StatusCode, Json<AuthErrorResponse>)> {
    let inner_state = state.clone();
    let Json(mut snapshot) = handle_get_order(State(inner_state), headers, Path(order_id)).await?;
    map_snapshot_status_to_v2(&state, &mut snapshot);
    Ok(Json(snapshot))
}

/// クライアント注文ID照会（GET /orders/client/{client_order_id}）
/// - PENDING/DURABLE/REJECTED/UNKNOWN を返す
pub(super) async fn handle_get_order_by_client_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(client_order_id): Path<String>,
) -> Result<Json<ClientOrderStatusResponse>, (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());

    // 呼び出し主体を認証し、account境界の判定に使う。
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

    // 過去に拒否済みのclient_order_idは即時にREJECTEDを返す。
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

    // 同一account配下のclient_order_idで注文を引く。
    let order = state
        .sharded_store
        .find_by_client_order_id(&principal.account_id, &client_order_id);

    let response = if let Some(order) = order {
        // 状態は REJECTED / DURABLE / PENDING の優先順で返す。
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

    // 照会結果を1件返す。
    Ok(Json(response))
}

/// 注文キャンセル（POST /orders/{order_id}/cancel）
/// - 状態遷移 → 監査/Bus に CancelRequested を記録
pub(super) async fn handle_cancel_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<(StatusCode, Json<CancelResponse>), (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());

    // 呼び出し主体を認証し、account境界の判定に使う。
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

    // 可能ならIDマップ経由でaccountを先に特定し、シャード探索コストを下げる。
    let account_id_from_map = state.order_id_map.get_account_id_by_external(&order_id);
    let order = if let Some(ref acc_id) = account_id_from_map {
        state
            .sharded_store
            .find_by_id_with_account(&order_id, acc_id)
    } else {
        // マップ不在時は全体探索のフォールバックで注文を引く。
        state.sharded_store.find_by_id(&order_id)
    };

    // 注文が見つからなければ404。
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

    // 他人の注文を推測できないよう、未存在と同じ404で隠蔽する。
    if order.account_id != principal.account_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    // すでに終端状態ならキャンセル不可。409で業務上の競合を返す。
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

    // すでにキャンセル要求済みなら冪等に同じ結果を返す。
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

    // 以降は CancelRequested へ状態遷移し、監査/配信イベントを残す。
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let _ = state
        .sharded_store
        .update(&order.order_id, &order.account_id, |prev| {
            let mut next = prev.clone();
            next.status = crate::store::OrderStatus::CancelRequested;
            next.last_update_at = now_ms;
            next
        });

    // 監査ログには必ず記録して、後続の追跡/復元に使う。
    state.audit_log.append(AuditEvent {
        event_type: "CancelRequested".into(),
        at: audit::now_millis(),
        account_id: order.account_id.clone(),
        order_id: Some(order.order_id.clone()),
        data: serde_json::json!({}),
    });
    if !state.bus_mode_outbox {
        // Outboxモード以外はこの場でBusへ直接通知する。
        state.bus_publisher.publish(BusEvent {
            event_type: "CancelRequested".into(),
            at: crate::bus::format_event_time(audit::now_millis()),
            account_id: order.account_id.clone(),
            order_id: Some(order.order_id.clone()),
            data: serde_json::json!({}),
        });
    }

    // キャンセル要求を受理したことを返す。
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

/// v3 入口の即時応答（VOLATILE_ACCEPT）
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VolatileOrderResponse {
    session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<String>,
    received_at_ns: u64,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl VolatileOrderResponse {
    fn accepted(
        session_id: String,
        session_seq: u64,
        received_at_ns: u64,
    ) -> Self {
        Self {
            session_id,
            session_seq: Some(session_seq),
            attempt_id: Some(format!("att_{}", session_seq)),
            received_at_ns,
            status: "VOLATILE_ACCEPT".into(),
            reason: None,
        }
    }

    fn rejected(session_id: &str, status: &str, reason: &str) -> Self {
        Self {
            session_id: session_id.to_string(),
            session_seq: None,
            attempt_id: None,
            received_at_ns: now_nanos(),
            status: status.into(),
            reason: Some(reason.into()),
        }
    }
}

/// v3 durable confirm 照会レスポンス。
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DurableOrderStatusResponse {
    session_id: String,
    session_seq: u64,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    received_at_ns: Option<u64>,
    updated_at_ns: u64,
    shard_id: u64,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::AuditLog;
    use crate::backpressure::BackpressureConfig;
    use crate::bus::BusPublisher;
    use crate::engine::FastPathEngine;
    use crate::order::{OrderType, TimeInForce};
    use crate::sse::SseHub;
    use crate::store::{OrderIdMap, OrderStatus, OrderStore, ShardedOrderStore};
    use axum::http::HeaderValue;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use gateway_core::LatencyHistogram;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::{SystemTime, UNIX_EPOCH};

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

        AppState {
            engine: FastPathEngine::new(16_384),
            jwt_auth: Arc::new(crate::auth::JwtAuth::for_test(TEST_JWT_SECRET)),
            order_store: Arc::new(OrderStore::new()),
            sharded_store: Arc::new(ShardedOrderStore::new_with_ttl_and_shards(86_400_000, 64)),
            order_id_map: Arc::new(OrderIdMap::new()),
            sse_hub: Arc::new(SseHub::new()),
            order_id_seq: Arc::new(AtomicU64::new(1)),
            audit_log,
            audit_read_path,
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
            v3_rejected_soft_total: Arc::new(AtomicU64::new(0)),
            v3_rejected_hard_total: Arc::new(AtomicU64::new(0)),
            v3_rejected_killed_total: Arc::new(AtomicU64::new(0)),
            v3_kill_recovered_total: Arc::new(AtomicU64::new(0)),
            v3_loss_suspect_total: Arc::new(AtomicU64::new(0)),
            v3_session_killed_total: Arc::new(AtomicU64::new(0)),
            v3_shard_killed_total: Arc::new(AtomicU64::new(0)),
            v3_global_killed_total: Arc::new(AtomicU64::new(0)),
            v3_durable_accepted_total: Arc::new(AtomicU64::new(0)),
            v3_durable_rejected_total: Arc::new(AtomicU64::new(0)),
            v3_live_ack_hist: Arc::new(LatencyHistogram::new()),
            v3_live_ack_accepted_hist: Arc::new(LatencyHistogram::new()),
            v3_durable_confirm_hist: Arc::new(LatencyHistogram::new()),
            v3_soft_reject_pct: 85,
            v3_hard_reject_pct: 90,
            v3_kill_reject_pct: 95,
            v3_confirm_store: Arc::new(super::super::V3ConfirmStore::new()),
            v3_loss_gap_timeout_ms: 500,
            v3_loss_scan_interval_ms: 50,
            v3_loss_scan_batch: 128,
            v3_risk_profile: super::super::V3RiskProfile::Light,
            v3_risk_margin_mode: super::super::V3RiskMarginMode::Legacy,
            v3_risk_loops: 16,
            v3_risk_strict_symbols: false,
            v3_risk_max_order_qty: 10_000,
            v3_risk_max_notional: 1_000_000_000,
            v3_risk_daily_notional_limit: 1_000_000_000_000,
            v3_risk_max_abs_position_qty: 100_000_000,
            v3_symbol_limits: Arc::new(HashMap::new()),
            v3_account_daily_notional: Arc::new(dashmap::DashMap::new()),
            v3_account_symbol_position: Arc::new(dashmap::DashMap::new()),
            v3_stage_parse_hist: Arc::new(LatencyHistogram::new()),
            v3_stage_risk_hist: Arc::new(LatencyHistogram::new()),
            v3_stage_risk_position_hist: Arc::new(LatencyHistogram::new()),
            v3_stage_risk_margin_hist: Arc::new(LatencyHistogram::new()),
            v3_stage_risk_limits_hist: Arc::new(LatencyHistogram::new()),
            v3_stage_enqueue_hist: Arc::new(LatencyHistogram::new()),
            v3_stage_serialize_hist: Arc::new(LatencyHistogram::new()),
        }
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
        }
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

    #[tokio::test]
    async fn v2_new_and_resend_returns_pending_then_durable() {
        let state = build_test_state();
        let req = request_with_client_id("cid_v2_resend");
        let idem_key = "idem_v2_resend";
        let account_id = "1001";

        let (status1, Json(resp1)) = handle_order_v2(
            State(state.clone()),
            headers(account_id, Some(idem_key)),
            Json(req.clone()),
        )
        .await
        .unwrap_or_else(|_| panic!("first request failed"));
        assert_eq!(status1, StatusCode::ACCEPTED);
        assert_eq!(resp1.status, "PENDING");
        assert!(!resp1.order_id.is_empty());
        let order_id = resp1.order_id.clone();

        let (status2, Json(resp2)) =
            handle_order_v2(State(state), headers(account_id, Some(idem_key)), Json(req))
                .await
                .unwrap_or_else(|_| panic!("second request failed"));
        assert_eq!(status2, StatusCode::OK);
        assert_eq!(resp2.status, "DURABLE");
        assert_eq!(resp2.order_id, order_id);
    }

    #[tokio::test]
    async fn v2_get_order_normalizes_pending_durable_rejected() {
        let state = build_test_state();
        let account_id = "2001";
        let order_id = "ord_v2_norm_1";

        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_v2_norm_1"),
            Some("idem_v2_norm_1"),
        );

        let Json(pending) = handle_get_order_v2(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("pending lookup failed"));
        assert_eq!(pending.status, "PENDING");

        assert!(
            state
                .sharded_store
                .mark_durable(order_id, account_id, audit::now_millis())
        );
        let Json(durable) = handle_get_order_v2(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("durable lookup failed"));
        assert_eq!(durable.status, "DURABLE");

        let _ = state.sharded_store.update(order_id, account_id, |prev| {
            let mut next = prev.clone();
            next.status = OrderStatus::Rejected;
            next
        });
        let Json(rejected) = handle_get_order_v2(
            State(state),
            headers(account_id, None),
            Path(order_id.to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("rejected lookup failed"));
        assert_eq!(rejected.status, "REJECTED");
    }

    #[tokio::test]
    async fn v2_get_order_by_client_id_normalizes_states() {
        let state = build_test_state();
        let account_id = "3001";
        let client_order_id = "cid_v2_client_1";
        let order_id = "ord_v2_client_1";

        let Json(unknown) = handle_get_order_by_client_id(
            State(state.clone()),
            headers(account_id, None),
            Path("cid_v2_unknown".to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("unknown lookup failed"));
        assert_eq!(unknown.status, "UNKNOWN");

        put_order(
            &state,
            order_id,
            account_id,
            Some(client_order_id),
            Some("idem_v2_client_1"),
        );
        let Json(pending) = handle_get_order_by_client_id(
            State(state.clone()),
            headers(account_id, None),
            Path(client_order_id.to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("pending client lookup failed"));
        assert_eq!(pending.status, "PENDING");

        assert!(
            state
                .sharded_store
                .mark_durable(order_id, account_id, audit::now_millis())
        );
        let Json(durable) = handle_get_order_by_client_id(
            State(state.clone()),
            headers(account_id, None),
            Path(client_order_id.to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("durable client lookup failed"));
        assert_eq!(durable.status, "DURABLE");

        state
            .sharded_store
            .mark_rejected_client_order(account_id, "cid_v2_rejected");
        let Json(rejected) = handle_get_order_by_client_id(
            State(state),
            headers(account_id, None),
            Path("cid_v2_rejected".to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("rejected client lookup failed"));
        assert_eq!(rejected.status, "REJECTED");
    }

    #[tokio::test]
    async fn v3_get_order_returns_unknown_and_durable_status() {
        let state = build_test_state();
        let account_id = "5001";

        let Json(unknown) = handle_get_order_v3(
            State(state.clone()),
            headers(account_id, None),
            Path((account_id.to_string(), 1_u64)),
        )
        .await
        .unwrap_or_else(|_| panic!("unknown v3 status lookup failed"));
        assert_eq!(unknown.status, "UNKNOWN");

        state
            .v3_confirm_store
            .mark_durable_accepted(account_id, 2, now_nanos());
        let Json(durable) = handle_get_order_v3(
            State(state),
            headers(account_id, None),
            Path((account_id.to_string(), 2_u64)),
        )
        .await
        .unwrap_or_else(|_| panic!("durable v3 status lookup failed"));
        assert_eq!(durable.status, "DURABLE_ACCEPTED");
    }

    #[tokio::test]
    async fn v2_rolls_back_when_wal_durability_fails() {
        let wal_path =
            std::env::temp_dir().join(format!("gateway-rust-orders-poison-{}.log", now_nanos()));
        let audit_log = Arc::new(AuditLog::new(wal_path).expect("create audit log"));
        audit_log.poison_writer_for_test();
        let state = build_test_state_with_audit_log(audit_log);

        let req = request_with_client_id("cid_v2_poison");
        let (status, Json(resp)) = handle_order_v2(
            State(state.clone()),
            headers("4001", Some("idem_v2_poison")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("poison response failed"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.status, "REJECTED");
        assert_eq!(resp.reason.as_deref(), Some("WAL_DURABILITY_FAILED"));
        assert_eq!(state.sharded_store.count(), 0);
        assert_eq!(state.order_id_map.count(), 0);
    }
}
