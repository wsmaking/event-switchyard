use crate::audit::{self};
use crate::auth::{AuthError, AuthResult};
use crate::backpressure::{BackpressureLevel, BackpressureMetrics, BackpressureReason};
use crate::order::{OrderRequest, OrderResponse};
use crate::store::OrderSnapshot;
use crate::strategy::shadow::ShadowPolicyView;
use axum::http::HeaderMap;
use axum::{
    Json,
    http::{StatusCode, header::AUTHORIZATION},
};
use gateway_core::now_nanos;
use std::sync::{Arc, atomic::Ordering};

use super::{AppState, AuthErrorResponse, AuthResponse, V3TcpDecodedRequest};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum OrderIngressContract {
    Legacy,
    V2,
}

// Inflight予約のリリース漏れを防ぐRAIIガード。
// 早期returnやエラーでもDropでreleaseされる。
pub(super) struct ControllerInflightGuard {
    handle: crate::inflight::InflightControllerHandle,
    active: bool,
}

impl ControllerInflightGuard {
    pub(super) fn new(handle: crate::inflight::InflightControllerHandle) -> Self {
        Self {
            handle,
            active: true,
        }
    }

    pub(super) fn disarm(&mut self) {
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
pub(super) fn record_ack(state: &AppState, start_ns: u64) {
    let elapsed_us = now_nanos().saturating_sub(start_ns) / 1_000;
    state.ack_hist.record(elapsed_us);
}

pub(super) fn record_v3_ack(state: &AppState, start_ns: u64, sampled: bool) {
    if !sampled {
        return;
    }
    let elapsed_ns = now_nanos().saturating_sub(start_ns);
    let elapsed_us = elapsed_ns / 1_000;
    state.ack_hist.record(elapsed_us);
    state.v3_live_ack_hist.record(elapsed_us);
    state.v3_live_ack_hist_ns.record(elapsed_ns);
}

pub(super) fn record_v3_ack_accepted(
    state: &AppState,
    start_ns: u64,
    start_tsc: Option<gateway_core::RdtscpStamp>,
    sampled: bool,
) {
    if !sampled {
        return;
    }
    let elapsed_ns = now_nanos().saturating_sub(start_ns);
    let elapsed_us = elapsed_ns / 1_000;
    state.v3_live_ack_accepted_hist.record(elapsed_us);
    state.v3_live_ack_accepted_hist_ns.record(elapsed_ns);
    if let Some(start_tsc) = start_tsc {
        if let Some(end_tsc) = state.capture_v3_tsc_stamp() {
            state.record_v3_tsc_accepted(start_tsc, end_tsc, elapsed_ns);
        } else {
            state.v3_tsc_fallback_total.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[inline]
pub(super) fn apply_confirm_guard_slack(age_us: u64, slack_pct: u64) -> u64 {
    if age_us == 0 || slack_pct == 0 {
        return age_us;
    }
    age_us.saturating_add(
        (((age_us as u128).saturating_mul(slack_pct.min(100) as u128) + 50) / 100) as u64,
    )
}

// WAL enqueue完了までの遅延を観測。
pub(super) fn record_wal_enqueue(
    state: &AppState,
    start_ns: u64,
    timings: audit::AuditAppendTimings,
) {
    if timings.enqueue_done_ns >= start_ns {
        let elapsed_us = (timings.enqueue_done_ns - start_ns) / 1_000;
        state.wal_enqueue_hist.record(elapsed_us);
    }
}

// 内部シーケンスを外部向けrequest_idに変換。
pub(super) fn build_request_id(accept_seq: Option<u64>) -> Option<String> {
    accept_seq.map(|seq| format!("req_{}", seq))
}

pub(super) fn map_existing_response(
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

pub(super) fn map_created_response(
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

pub(super) fn finalize_sync_durable_v2(
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

pub(super) fn map_snapshot_status_to_v2(state: &AppState, snapshot: &mut OrderSnapshotResponse) {
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

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::server::http) struct CancelResponse {
    pub(super) order_id: String,
    pub(super) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reason: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::server::http) struct AmendRequest {
    pub(super) new_qty: u64,
    pub(super) new_price: u64,
    #[serde(default)]
    pub(super) comment: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::server::http) struct AmendResponse {
    pub(super) order_id: String,
    pub(super) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reason: Option<String>,
}

/// 注文スナップショットレスポンス
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::server::http) struct OrderSnapshotResponse {
    pub(super) order_id: String,
    pub(super) account_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) client_order_id: Option<String>,
    pub(super) symbol: String,
    pub(super) side: String,
    #[serde(rename = "type")]
    pub(super) order_type: String,
    pub(super) qty: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) price: Option<u64>,
    pub(super) time_in_force: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) expire_at: Option<u64>,
    pub(super) status: String,
    pub(super) accepted_at: u64,
    pub(super) last_update_at: u64,
    pub(super) filled_qty: u64,
}

/// クライアント注文ID照会レスポンス
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::server::http) struct ClientOrderStatusResponse {
    pub(super) client_order_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) order_id: Option<String>,
    pub(super) status: String,
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
                crate::order::TimeInForce::Ioc => "IOC".into(),
                crate::order::TimeInForce::Fok => "FOK".into(),
            },
            expire_at: o.expire_at,
            status: o.status.as_str().into(),
            accepted_at: o.accepted_at,
            last_update_at: o.last_update_at,
            filled_qty: o.filled_qty,
        }
    }
}

pub(super) fn authenticate_request(
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

pub(super) fn build_idempotency_key(headers: &HeaderMap, req: &OrderRequest) -> Option<String> {
    let client_order_id = req.client_order_id.clone();
    let decision_key = req
        .decision_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let execution_run_id = req
        .execution_run_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let decision_attempt_seq = req.decision_attempt_seq.unwrap_or(1).max(1);
    headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| client_order_id.clone())
        .or_else(|| {
            execution_run_id
                .zip(decision_key)
                .map(|(execution_run_id, decision_key)| {
                    format!("decision:{execution_run_id}:{decision_key}:{decision_attempt_seq}")
                })
        })
}

pub(super) async fn reserve_inflight(
    state: &AppState,
    start_ns: u64,
) -> Result<(Option<ControllerInflightGuard>, u64), (StatusCode, Json<OrderResponse>)> {
    match state.inflight_controller.reserve().await {
        crate::inflight::ReserveDecision::Allow { inflight, .. } => Ok((
            Some(ControllerInflightGuard::new(
                state.inflight_controller.clone(),
            )),
            inflight,
        )),
        crate::inflight::ReserveDecision::RejectInflight { .. } => {
            state.backpressure_inflight.fetch_add(1, Ordering::Relaxed);
            record_ack(state, start_ns);
            Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(OrderResponse::rejected("BACKPRESSURE_INFLIGHT_SOFT")),
            ))
        }
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

pub(super) fn apply_backpressure(
    state: &AppState,
    start_ns: u64,
    inflight: u64,
) -> Result<(), (StatusCode, Json<OrderResponse>)> {
    let metrics = BackpressureMetrics {
        inflight,
        wal_bytes: state.audit_log.wal_bytes(),
        wal_age_ms: state.audit_log.wal_age_ms(),
        disk_free_pct: state.audit_log.disk_free_pct(),
    };
    if let Some(decision) = crate::backpressure::evaluate(&state.backpressure, &metrics) {
        let (status, reason) = match (decision.level, decision.reason) {
            (BackpressureLevel::Soft, BackpressureReason::SoftWalAge) => {
                state
                    .backpressure_soft_wal_age
                    .fetch_add(1, Ordering::Relaxed);
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
                state
                    .backpressure_soft_wal_age
                    .fetch_add(1, Ordering::Relaxed);
                (StatusCode::TOO_MANY_REQUESTS, "BACKPRESSURE_SOFT_WAL_AGE")
            }
        };
        record_ack(state, start_ns);
        return Err((status, Json(OrderResponse::rejected(reason))));
    }
    Ok(())
}

pub(super) fn parse_v3_symbol_key(raw: &str) -> Option<[u8; 8]> {
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

pub(in crate::server::http) fn render_v3_symbol_key(symbol_key: [u8; 8]) -> String {
    let len = symbol_key
        .iter()
        .position(|b| *b == 0)
        .unwrap_or(symbol_key.len());
    std::str::from_utf8(&symbol_key[..len])
        .unwrap_or_default()
        .to_string()
}

fn exceeds_strategy_notional_limit(qty: u64, price: u64, max_notional: u64) -> bool {
    price > 0 && max_notional > 0 && qty.saturating_mul(price) > max_notional
}

pub(super) fn feedback_metadata<'a>(
    execution_run_id: Option<&'a str>,
    decision_key: Option<&'a str>,
    decision_attempt_seq: Option<u64>,
    intent_id: Option<&'a str>,
    model_id: Option<&'a str>,
) -> (
    Option<&'a str>,
    Option<&'a str>,
    Option<u64>,
    Option<&'a str>,
    Option<&'a str>,
) {
    let execution_run_id = execution_run_id
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let decision_key = decision_key
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let decision_attempt_seq = decision_attempt_seq.filter(|value| *value > 0);
    let intent_id = intent_id.map(str::trim).filter(|value| !value.is_empty());
    let model_id = model_id.map(str::trim).filter(|value| !value.is_empty());
    (
        execution_run_id,
        decision_key,
        decision_attempt_seq,
        intent_id,
        model_id,
    )
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum StrategyAdmissionUrgency {
    Low,
    Normal,
    High,
    Critical,
}

pub(super) fn strategy_admission_urgency(
    actual_policy: Option<&ShadowPolicyView>,
) -> StrategyAdmissionUrgency {
    let Some(raw) = actual_policy.and_then(|policy| policy.urgency.as_deref()) else {
        return StrategyAdmissionUrgency::Normal;
    };
    if raw.eq_ignore_ascii_case("CRITICAL") {
        return StrategyAdmissionUrgency::Critical;
    }
    if raw.eq_ignore_ascii_case("HIGH") {
        return StrategyAdmissionUrgency::High;
    }
    if raw.eq_ignore_ascii_case("LOW") {
        return StrategyAdmissionUrgency::Low;
    }
    StrategyAdmissionUrgency::Normal
}

pub(super) fn strategy_allows_soft_admission_bypass(
    actual_policy: Option<&ShadowPolicyView>,
) -> bool {
    strategy_admission_urgency(actual_policy) >= StrategyAdmissionUrgency::High
}

pub(super) fn validate_order_request(req: &OrderRequest) -> Option<&'static str> {
    if req.symbol.trim().is_empty() {
        return Some("INVALID_SYMBOL");
    }
    if req.qty == 0 {
        return Some("INVALID_QTY");
    }
    if req.order_type == crate::order::OrderType::Limit && req.price.unwrap_or(0) == 0 {
        return Some("INVALID_PRICE");
    }
    let execution_run_id = req
        .execution_run_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if req.execution_run_id.is_some() && execution_run_id.is_none() {
        return Some("INVALID_EXECUTION_RUN_ID");
    }
    let decision_key = req
        .decision_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if req.decision_key.is_some() && decision_key.is_none() {
        return Some("INVALID_DECISION_KEY");
    }
    if decision_key.is_some() && execution_run_id.is_none() {
        return Some("EXECUTION_RUN_ID_REQUIRED");
    }
    if req.decision_attempt_seq == Some(0) {
        return Some("INVALID_DECISION_ATTEMPT");
    }
    if req.decision_attempt_seq.is_some() && decision_key.is_none() {
        return Some("DECISION_KEY_REQUIRED");
    }
    None
}

#[derive(Clone, Copy)]
pub(super) struct PositionCapProjection {
    pub(super) symbol_key: [u8; 8],
    pub(super) delta_qty: i64,
}

#[derive(Clone, Copy)]
pub(super) struct V3HotRiskInput {
    pub(super) symbol_key: Option<[u8; 8]>,
    pub(super) side: u8,
    pub(super) order_type: crate::order::OrderType,
    pub(super) qty: u64,
    pub(super) price: u64,
}

impl V3HotRiskInput {
    #[inline]
    pub(super) fn from_order_request(req: &OrderRequest) -> Self {
        Self {
            symbol_key: parse_v3_symbol_key(&req.symbol),
            side: req.side_byte(),
            order_type: req.order_type,
            qty: req.qty,
            price: req.price.unwrap_or(0),
        }
    }

    #[inline]
    pub(super) fn from_tcp(decoded: &V3TcpDecodedRequest<'_>) -> Self {
        Self {
            symbol_key: Some(decoded.symbol_key),
            side: decoded.side,
            order_type: decoded.order_type,
            qty: decoded.qty,
            price: decoded.price,
        }
    }
}

pub(super) fn evaluate_strategy_snapshot_policy(
    state: &AppState,
    account_id: &str,
    input: &V3HotRiskInput,
    now_ns: u64,
) -> Result<(), (StatusCode, &'static str)> {
    let snapshot = state.strategy_snapshot_store.snapshot();
    if snapshot.is_stale_at(now_ns) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "V3_STRATEGY_SNAPSHOT_STALE",
        ));
    }

    if snapshot.symbol_limits.is_empty() && snapshot.risk_budget_by_account.is_empty() {
        return Ok(());
    }

    if let Some(symbol_key) = input.symbol_key {
        let symbol = render_v3_symbol_key(symbol_key);
        if let Some(override_cfg) = snapshot.symbol_override(&symbol) {
            if let Some(max_order_qty) = override_cfg.max_order_qty {
                if input.qty > max_order_qty {
                    return Err((
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "V3_STRATEGY_MAX_ORDER_QTY",
                    ));
                }
            }
            if let Some(max_notional) = override_cfg.max_notional {
                if exceeds_strategy_notional_limit(input.qty, input.price, max_notional) {
                    return Err((StatusCode::UNPROCESSABLE_ENTITY, "V3_STRATEGY_MAX_NOTIONAL"));
                }
            }
        }
    }

    if let Some(account_budget) = snapshot.account_risk_budget(account_id) {
        if let Some(max_notional) = account_budget.max_notional {
            if exceeds_strategy_notional_limit(input.qty, input.price, max_notional) {
                return Err((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "V3_STRATEGY_ACCOUNT_MAX_NOTIONAL",
                ));
            }
        }
    }

    Ok(())
}

fn projected_position_qty(
    current_position_qty: i64,
    delta_qty: i64,
    max_abs_position_qty: u64,
) -> Result<i64, &'static str> {
    let projected = (current_position_qty as i128) + (delta_qty as i128);
    let max_abs = max_abs_position_qty as i128;
    if projected.abs() > max_abs {
        return Err("POSITION_LIMIT_EXCEEDED");
    }
    i64::try_from(projected).map_err(|_| "POSITION_LIMIT_EXCEEDED")
}

pub(super) fn evaluate_v3_hot_risk(
    state: &AppState,
    account_id: &Arc<str>,
    req: &OrderRequest,
    sampled: bool,
) -> Result<PositionCapProjection, &'static str> {
    evaluate_v3_hot_risk_input(
        state,
        account_id,
        V3HotRiskInput::from_order_request(req),
        sampled,
    )
}

pub(super) fn evaluate_v3_hot_risk_input(
    state: &AppState,
    account_id: &Arc<str>,
    input: V3HotRiskInput,
    sampled: bool,
) -> Result<PositionCapProjection, &'static str> {
    let position_t0 = now_nanos();
    let side = input.side;
    if side != 1 && side != 2 {
        return Err("INVALID_SIDE");
    }
    if input.qty == 0 || input.qty > state.v3_risk_max_order_qty {
        return Err("INVALID_QTY");
    }
    let symbol_key = match input.symbol_key {
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
    if input.qty > max_qty {
        return Err("INVALID_QTY");
    }
    let delta_qty = if side == 1 {
        input.qty as i64
    } else {
        -(input.qty as i64)
    };
    let current_position_qty = state.v3_position_qty(account_id, symbol_key);
    projected_position_qty(
        current_position_qty,
        delta_qty,
        state.v3_risk_max_abs_position_qty,
    )?;
    if sampled {
        let position_elapsed = now_nanos().saturating_sub(position_t0) / 1_000;
        state.v3_stage_risk_position_hist.record(position_elapsed);
    }

    let price = input.price;
    if input.order_type == crate::order::OrderType::Limit && price == 0 {
        return Err("INVALID_PRICE");
    }

    let margin_t0 = now_nanos();
    let notional = (input.qty as u128).saturating_mul(price as u128);
    let max_notional = state.v3_risk_max_notional.min(symbol_limits.max_notional);
    if sampled {
        let margin_elapsed = now_nanos().saturating_sub(margin_t0) / 1_000;
        state.v3_stage_risk_margin_hist.record(margin_elapsed);
    }

    let limits_t0 = now_nanos();
    if notional > max_notional as u128 {
        if sampled {
            let limits_elapsed = now_nanos().saturating_sub(limits_t0) / 1_000;
            state.v3_stage_risk_limits_hist.record(limits_elapsed);
        }
        return Err("RISK_REJECT");
    }
    if sampled {
        let limits_elapsed = now_nanos().saturating_sub(limits_t0) / 1_000;
        state.v3_stage_risk_limits_hist.record(limits_elapsed);
    }

    Ok(PositionCapProjection {
        symbol_key,
        delta_qty,
    })
}
