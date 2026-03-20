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
use crate::strategy::feedback::FeedbackEvent;
use crate::strategy::shadow::ShadowPolicyView;
use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, header::AUTHORIZATION},
};
use gateway_core::{RdtscpStamp, now_nanos};
use std::{
    cell::RefCell,
    ops::Deref,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use super::{
    AppState, AuthErrorResponse, V3OrderTask, maybe_append_strategy_execution_fact,
    publish_quant_feedback,
};

type AuthResponse<T> = Result<T, (StatusCode, Json<AuthErrorResponse>)>;
type OrderResponseResult =
    Result<(StatusCode, Json<OrderResponse>), (StatusCode, Json<AuthErrorResponse>)>;
type VolatileOrderResponseResult =
    Result<(StatusCode, Json<VolatileOrderResponse>), (StatusCode, Json<AuthErrorResponse>)>;
type DurableOrderStatusResult =
    Result<Json<DurableOrderStatusResponse>, (StatusCode, Json<AuthErrorResponse>)>;

pub(super) const V3_TCP_REQUEST_SIZE: usize = 304;
pub(super) const V3_TCP_RESPONSE_SIZE: usize = 32;

const V3_TCP_KIND_ACCEPT: u8 = 0;
const V3_TCP_KIND_REJECTED: u8 = 1;
const V3_TCP_KIND_KILLED: u8 = 2;
const V3_TCP_KIND_DECODE_ERROR: u8 = 4;

const V3_TCP_REASON_NONE: u32 = 0;
pub(super) const V3_TCP_REASON_BAD_TOKEN_LEN: u32 = 101;
pub(super) const V3_TCP_REASON_BAD_SYMBOL: u32 = 102;
pub(super) const V3_TCP_REASON_BAD_SIDE: u32 = 103;
pub(super) const V3_TCP_REASON_BAD_TYPE: u32 = 104;
pub(super) const V3_TCP_REASON_BAD_TOKEN_UTF8: u32 = 105;
pub(super) const V3_TCP_REASON_BAD_INTENT_LEN: u32 = 106;
pub(super) const V3_TCP_REASON_BAD_MODEL_LEN: u32 = 107;
pub(super) const V3_TCP_REASON_BAD_METADATA_UTF8: u32 = 108;
pub(super) const V3_TCP_REASON_METADATA_WITH_INLINE_TOKEN: u32 = 109;
pub(super) const V3_TCP_REASON_AUTH_INVALID: u32 = 201;
pub(super) const V3_TCP_REASON_AUTH_EXPIRED: u32 = 202;
pub(super) const V3_TCP_REASON_AUTH_NOT_YET_VALID: u32 = 203;
pub(super) const V3_TCP_REASON_AUTH_INTERNAL: u32 = 204;
pub(super) const V3_TCP_REASON_AUTH_REQUIRED: u32 = 205;
pub(super) const V3_TCP_REASON_AUTH_UNEXPECTED_TOKEN: u32 = 206;

const V3_TCP_TOKEN_OFFSET: usize = 2;
const V3_TCP_TOKEN_MAX_LEN: usize = 256;
const V3_TCP_SYMBOL_OFFSET: usize = 258;
const V3_TCP_SYMBOL_LEN: usize = 16;
const V3_TCP_SIDE_OFFSET: usize = 274;
const V3_TCP_TYPE_OFFSET: usize = 275;
const V3_TCP_INTENT_LEN_OFFSET: usize = 276;
const V3_TCP_MODEL_LEN_OFFSET: usize = 278;
const V3_TCP_QTY_OFFSET: usize = 280;
const V3_TCP_PRICE_OFFSET: usize = 288;

#[derive(Debug)]
pub(super) struct V3TcpDecodedRequest<'a> {
    pub(super) jwt_token: Option<&'a str>,
    pub(super) intent_id: Option<&'a str>,
    pub(super) model_id: Option<&'a str>,
    pub(super) symbol_key: [u8; 8],
    pub(super) side: u8,
    pub(super) order_type: crate::order::OrderType,
    pub(super) qty: u64,
    pub(super) price: u64,
}

pub(super) enum V3TcpDecodedFrame<'a> {
    AuthInit { jwt_token: &'a str },
    Order(V3TcpDecodedRequest<'a>),
}

#[derive(Clone, Copy)]
struct V3HotPathOutcome {
    status: StatusCode,
    kind: u8,
    reason_code: u32,
    status_text: &'static str,
    reason_text: Option<&'static str>,
    session_seq: Option<u64>,
    received_at_ns: u64,
}

impl V3HotPathOutcome {
    #[inline]
    fn accepted(session_seq: u64, received_at_ns: u64) -> Self {
        Self {
            status: StatusCode::ACCEPTED,
            kind: V3_TCP_KIND_ACCEPT,
            reason_code: V3_TCP_REASON_NONE,
            status_text: "VOLATILE_ACCEPT",
            reason_text: None,
            session_seq: Some(session_seq),
            received_at_ns,
        }
    }

    #[inline]
    fn rejected(
        status: StatusCode,
        kind: u8,
        status_text: &'static str,
        reason_text: &'static str,
        received_at_ns: u64,
    ) -> Self {
        Self {
            status,
            kind,
            reason_code: v3_tcp_reason_code_from_reason(Some(reason_text)),
            status_text,
            reason_text: Some(reason_text),
            session_seq: None,
            received_at_ns,
        }
    }

    #[inline]
    fn to_http(self, session_id: &str) -> VolatileOrderResponse {
        VolatileOrderResponse::from_hotpath(session_id, self)
    }

    #[inline]
    fn to_tcp(self) -> [u8; V3_TCP_RESPONSE_SIZE] {
        let session_seq = self.session_seq.unwrap_or(0);
        encode_v3_tcp_response_raw(
            self.kind,
            self.status,
            self.reason_code,
            session_seq,
            session_seq,
            self.received_at_ns,
        )
    }
}

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

fn record_v3_ack(state: &AppState, start_ns: u64, sampled: bool) {
    if !sampled {
        return;
    }
    let elapsed_ns = now_nanos().saturating_sub(start_ns);
    let elapsed_us = elapsed_ns / 1_000;
    state.ack_hist.record(elapsed_us);
    state.v3_live_ack_hist.record(elapsed_us);
    state.v3_live_ack_hist_ns.record(elapsed_ns);
}

fn record_v3_ack_accepted(
    state: &AppState,
    start_ns: u64,
    start_tsc: Option<RdtscpStamp>,
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
fn apply_confirm_guard_slack(age_us: u64, slack_pct: u64) -> u64 {
    if age_us == 0 || slack_pct == 0 {
        return age_us;
    }
    age_us.saturating_add(
        (((age_us as u128).saturating_mul(slack_pct.min(100) as u128) + 50) / 100) as u64,
    )
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

pub(super) fn render_v3_symbol_key(symbol_key: [u8; 8]) -> String {
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

fn feedback_metadata<'a>(
    execution_run_id: Option<&'a str>,
    intent_id: Option<&'a str>,
    model_id: Option<&'a str>,
) -> (Option<&'a str>, Option<&'a str>, Option<&'a str>) {
    let execution_run_id = execution_run_id
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let intent_id = intent_id.map(str::trim).filter(|value| !value.is_empty());
    let model_id = model_id.map(str::trim).filter(|value| !value.is_empty());
    (execution_run_id, intent_id, model_id)
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum StrategyAdmissionUrgency {
    Low,
    Normal,
    High,
    Critical,
}

fn strategy_admission_urgency(
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

fn strategy_allows_soft_admission_bypass(actual_policy: Option<&ShadowPolicyView>) -> bool {
    strategy_admission_urgency(actual_policy) >= StrategyAdmissionUrgency::High
}

fn validate_order_request(req: &OrderRequest) -> Option<&'static str> {
    if req.symbol.trim().is_empty() {
        return Some("INVALID_SYMBOL");
    }
    if req.qty == 0 {
        return Some("INVALID_QTY");
    }
    if req.order_type == crate::order::OrderType::Limit && req.price.unwrap_or(0) == 0 {
        return Some("INVALID_PRICE");
    }
    None
}

#[derive(Clone, Copy)]
struct PositionCapProjection {
    symbol_key: [u8; 8],
    delta_qty: i64,
}

#[derive(Clone, Copy)]
struct V3HotRiskInput {
    symbol_key: Option<[u8; 8]>,
    side: u8,
    order_type: crate::order::OrderType,
    qty: u64,
    price: u64,
}

impl V3HotRiskInput {
    #[inline]
    fn from_order_request(req: &OrderRequest) -> Self {
        Self {
            symbol_key: parse_v3_symbol_key(&req.symbol),
            side: req.side_byte(),
            order_type: req.order_type,
            qty: req.qty,
            price: req.price.unwrap_or(0),
        }
    }

    #[inline]
    fn from_tcp(decoded: &V3TcpDecodedRequest<'_>) -> Self {
        Self {
            symbol_key: Some(decoded.symbol_key),
            side: decoded.side,
            order_type: decoded.order_type,
            qty: decoded.qty,
            price: decoded.price,
        }
    }
}

fn evaluate_strategy_snapshot_policy(
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

fn evaluate_v3_hot_risk(
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

fn evaluate_v3_hot_risk_input(
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

/// v3 注文受付（POST /v3/orders）
/// - hot path 最小化: parse -> risk -> shard enqueue -> VOLATILE_ACCEPT 応答
pub(super) async fn handle_order_v3(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> VolatileOrderResponseResult {
    let t0 = now_nanos();
    let principal = authenticate_request(&state, &headers, t0)?;
    let (status, body) = process_order_v3_hot_path(
        &state,
        &principal.account_id,
        &principal.session_id,
        req,
        t0,
    );
    Ok((status, Json(body)))
}
pub(super) fn process_order_v3_hot_path(
    state: &AppState,
    account_id: &str,
    session_id: &str,
    req: OrderRequest,
    t0: u64,
) -> (StatusCode, VolatileOrderResponse) {
    process_order_v3_hot_path_with_strategy_context(
        state, account_id, session_id, req, None, None, t0,
    )
}

pub(super) fn process_order_v3_hot_path_with_strategy_context(
    state: &AppState,
    account_id: &str,
    session_id: &str,
    req: OrderRequest,
    actual_policy: Option<Arc<ShadowPolicyView>>,
    effective_risk_budget_ref: Option<Arc<str>>,
    t0: u64,
) -> (StatusCode, VolatileOrderResponse) {
    let outcome = process_order_v3_hot_path_with_input(
        state,
        account_id,
        session_id,
        V3HotRiskInput::from_order_request(&req),
        req.execution_run_id.as_deref(),
        req.intent_id.as_deref(),
        req.model_id.as_deref(),
        actual_policy,
        effective_risk_budget_ref,
        "http_v3",
        t0,
    );
    (outcome.status, outcome.to_http(session_id))
}

pub(super) fn process_order_v3_hot_path_tcp(
    state: &AppState,
    account_id: &str,
    session_id: &str,
    decoded: &V3TcpDecodedRequest<'_>,
    t0: u64,
) -> [u8; V3_TCP_RESPONSE_SIZE] {
    process_order_v3_hot_path_with_input(
        state,
        account_id,
        session_id,
        V3HotRiskInput::from_tcp(decoded),
        None,
        decoded.intent_id,
        decoded.model_id,
        None,
        None,
        "tcp_v3",
        t0,
    )
    .to_tcp()
}

fn process_order_v3_hot_path_with_input(
    state: &AppState,
    account_id: &str,
    session_id: &str,
    risk_input: V3HotRiskInput,
    execution_run_id: Option<&str>,
    intent_id: Option<&str>,
    model_id: Option<&str>,
    actual_policy: Option<Arc<ShadowPolicyView>>,
    effective_risk_budget_ref: Option<Arc<str>>,
    path_tag: &'static str,
    t0: u64,
) -> V3HotPathOutcome {
    let t0_tsc = state.capture_v3_tsc_stamp();
    let hotpath_sampled = state.v3_hotpath_sampled();
    let ingress = &state.v3_ingress;
    let shard_id = ingress.shard_for_session(session_id);
    if ingress.maybe_recover_shard(shard_id, t0) {
        state
            .v3_kill_recovered_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // parse段階: JSON展開済みリクエストから必要項目を取り出す。
    let parse_t0 = now_nanos();
    let _ = (
        risk_input.side,
        risk_input.order_type,
        risk_input.qty,
        risk_input.price,
    );
    if hotpath_sampled {
        let parse_elapsed = now_nanos().saturating_sub(parse_t0) / 1_000;
        state.v3_stage_parse_hist.record(parse_elapsed);
    }
    let symbol = risk_input
        .symbol_key
        .map(render_v3_symbol_key)
        .unwrap_or_default();
    let (execution_run_id, intent_id, model_id) =
        feedback_metadata(execution_run_id, intent_id, model_id);
    // Strategy submit can mark high-urgency flow, but only soft admission is relaxable.
    let strategy_soft_bypass = strategy_allows_soft_admission_bypass(actual_policy.as_deref());
    let rejected_feedback_context = state.quant_feedback_exporter.is_enabled().then(|| {
        (
            session_id.to_string(),
            account_id.to_string(),
            symbol.clone(),
            execution_run_id.map(str::to_string),
            intent_id.map(str::to_string),
            model_id.map(str::to_string),
            effective_risk_budget_ref.as_deref().map(str::to_string),
            actual_policy.as_deref().cloned(),
        )
    });
    let emit_rejected_feedback = |session_seq: u64, reason: &'static str, received_at_ns: u64| {
        if let Some((
            session_id,
            account_id,
            symbol,
            execution_run_id,
            intent_id,
            model_id,
            effective_risk_budget_ref,
            actual_policy,
        )) = &rejected_feedback_context
        {
            let event = FeedbackEvent::rejected(
                session_id.as_str(),
                session_seq,
                account_id.as_str(),
                symbol.as_str(),
                received_at_ns,
                reason,
            );
            let event = if let Some(execution_run_id) = execution_run_id.as_deref() {
                event.with_execution_run_id(execution_run_id)
            } else {
                event
            };
            let event = if let Some(intent_id) = intent_id.as_deref() {
                event.with_intent_id(intent_id)
            } else {
                event
            };
            let event = if let Some(model_id) = model_id.as_deref() {
                event.with_model_id(model_id)
            } else {
                event
            };
            let event =
                if let Some(effective_risk_budget_ref) = effective_risk_budget_ref.as_deref() {
                    event.with_effective_risk_budget_ref(effective_risk_budget_ref)
                } else {
                    event
                };
            let event = if let Some(actual_policy) = actual_policy.as_ref() {
                event.with_actual_policy(actual_policy.clone())
            } else {
                event
            }
            .push_path_tag("v3")
            .push_path_tag(path_tag)
            .push_path_tag("feedback");
            publish_quant_feedback(state, event);
        }
        maybe_append_strategy_execution_fact(
            state,
            execution_run_id,
            intent_id,
            model_id,
            account_id,
            session_id,
            (session_seq != 0).then_some(session_seq),
            symbol.as_str(),
            risk_input.symbol_key.map(|_| {
                if risk_input.side == 1 {
                    risk_input.qty as i64
                } else if risk_input.side == 2 {
                    -(risk_input.qty as i64)
                } else {
                    0
                }
            }),
            received_at_ns,
            super::StrategyExecutionFactStatus::Rejected,
            Some(reason),
        );
    };

    if ingress.is_global_killed() {
        state.increment_v3_rejected_killed_total(shard_id);
        record_v3_ack(&state, t0, hotpath_sampled);
        let rejected_at_ns = now_nanos();
        emit_rejected_feedback(0, "V3_GLOBAL_KILLED", rejected_at_ns);
        return V3HotPathOutcome::rejected(
            StatusCode::SERVICE_UNAVAILABLE,
            V3_TCP_KIND_KILLED,
            "KILLED",
            "V3_GLOBAL_KILLED",
            rejected_at_ns,
        );
    }
    if ingress.is_session_killed(session_id) {
        state.increment_v3_rejected_killed_total(shard_id);
        record_v3_ack(&state, t0, hotpath_sampled);
        let rejected_at_ns = now_nanos();
        emit_rejected_feedback(0, "V3_SESSION_KILLED", rejected_at_ns);
        return V3HotPathOutcome::rejected(
            StatusCode::SERVICE_UNAVAILABLE,
            V3_TCP_KIND_KILLED,
            "KILLED",
            "V3_SESSION_KILLED",
            rejected_at_ns,
        );
    }
    if ingress.is_shard_killed(shard_id) {
        state.increment_v3_rejected_killed_total(shard_id);
        record_v3_ack(&state, t0, hotpath_sampled);
        let rejected_at_ns = now_nanos();
        emit_rejected_feedback(0, "V3_SHARD_KILLED", rejected_at_ns);
        return V3HotPathOutcome::rejected(
            StatusCode::SERVICE_UNAVAILABLE,
            V3_TCP_KIND_KILLED,
            "KILLED",
            "V3_SHARD_KILLED",
            rejected_at_ns,
        );
    }

    let strategy_policy_now_ns = now_nanos();
    if let Err((status, reason)) =
        evaluate_strategy_snapshot_policy(state, account_id, &risk_input, strategy_policy_now_ns)
    {
        if reason == "V3_STRATEGY_SNAPSHOT_STALE" {
            state.increment_v3_rejected_hard_total(shard_id);
        } else {
            state.reject_risk.fetch_add(1, Ordering::Relaxed);
        }
        record_v3_ack(&state, t0, hotpath_sampled);
        emit_rejected_feedback(0, reason, strategy_policy_now_ns);
        return V3HotPathOutcome::rejected(
            status,
            V3_TCP_KIND_REJECTED,
            "REJECTED",
            reason,
            strategy_policy_now_ns,
        );
    }

    let session_id_ref = state.intern_v3_session_id(session_id);
    let account_id_ref = state.intern_v3_account_id(account_id);

    // risk段階: 最小の stateless チェックのみ実行し、共有ロックを避ける。
    let risk_t0 = now_nanos();
    let risk_result =
        evaluate_v3_hot_risk_input(&state, &account_id_ref, risk_input, hotpath_sampled);
    if hotpath_sampled {
        let risk_elapsed = now_nanos().saturating_sub(risk_t0) / 1_000;
        state.v3_stage_risk_hist.record(risk_elapsed);
    }

    let position_projection = match risk_result {
        Ok(projection) => projection,
        Err(reason) => {
            match reason {
                "INVALID_QTY" | "INVALID_SIDE" | "INVALID_PRICE" => {
                    state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
                }
                "RISK_REJECT" | "POSITION_LIMIT_EXCEEDED" => {
                    state.reject_risk.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
                }
            }
            record_v3_ack(&state, t0, hotpath_sampled);
            let rejected_at_ns = now_nanos();
            emit_rejected_feedback(0, reason, rejected_at_ns);
            return V3HotPathOutcome::rejected(
                StatusCode::UNPROCESSABLE_ENTITY,
                V3_TCP_KIND_REJECTED,
                "REJECTED",
                reason,
                rejected_at_ns,
            );
        }
    };

    // durable経路の詰まりも入口判定へ反映する。
    if state.v3_durable_ack_path_guard_enabled {
        let durable_lane_id = state.v3_durable_ingress.lane_for_shard(shard_id);
        let durable_queue_pct = state
            .v3_durable_ingress
            .lane_utilization_pct(durable_lane_id);
        let durable_backlog_growth_per_sec = state
            .v3_durable_backlog_growth_per_sec_per_lane
            .get(durable_lane_id)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or_else(|| {
                state
                    .v3_durable_backlog_growth_per_sec
                    .load(Ordering::Relaxed)
            });
        let durable_backlog_signal_enabled =
            durable_queue_pct >= state.v3_durable_backlog_signal_min_queue_pct;
        let durable_backlog_hard_failsafe = state
            .v3_durable_backlog_hard_reject_per_sec
            .saturating_mul(4);
        let confirm_oldest_age_us_global =
            state.v3_confirm_oldest_inflight_us.load(Ordering::Relaxed);
        let confirm_oldest_age_us_lane = state
            .v3_confirm_oldest_inflight_us_per_lane
            .get(durable_lane_id)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(confirm_oldest_age_us_global);
        // Admission is lane-scoped: one degraded lane should not globally throttle healthy lanes.
        let confirm_oldest_age_us = confirm_oldest_age_us_lane;
        let (confirm_soft_age_us, confirm_hard_age_us) =
            state.v3_durable_confirm_reject_ages_for_lane(durable_lane_id);
        let lane_level = state
            .v3_durable_admission_level_per_lane
            .get(durable_lane_id)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or_else(|| state.v3_durable_admission_level.load(Ordering::Relaxed));
        let lane_inflight_now = state
            .v3_durable_receipt_inflight_per_lane
            .get(durable_lane_id)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or_else(|| state.v3_durable_receipt_inflight.load(Ordering::Relaxed));
        let lane_inflight_pct = if state.v3_durable_worker_max_inflight_receipts > 0 {
            (lane_inflight_now as f64 * 100.0)
                / (state.v3_durable_worker_max_inflight_receipts as f64)
        } else {
            0.0
        };
        let guard_queue_signal = state.v3_durable_confirm_guard_min_queue_pct > 0.0
            && durable_queue_pct >= state.v3_durable_confirm_guard_min_queue_pct;
        let guard_inflight_signal = state.v3_durable_confirm_guard_min_inflight_pct > 0
            && lane_inflight_pct >= state.v3_durable_confirm_guard_min_inflight_pct as f64;
        let guard_backlog_signal = state.v3_durable_confirm_guard_min_backlog_per_sec > 0
            && durable_backlog_growth_per_sec >= state.v3_durable_confirm_guard_min_backlog_per_sec;
        let guard_secondary_signal =
            lane_level > 0 || guard_queue_signal || guard_inflight_signal || guard_backlog_signal;
        let guard_secondary_enabled =
            !state.v3_durable_confirm_guard_secondary_required || guard_secondary_signal;
        let hard_guard_armed = state
            .v3_durable_confirm_guard_hard_armed_per_lane
            .get(durable_lane_id)
            .map(|v| v.load(Ordering::Relaxed) > 0)
            .unwrap_or(true);
        let soft_guard_armed = state
            .v3_durable_confirm_guard_soft_armed_per_lane
            .get(durable_lane_id)
            .map(|v| v.load(Ordering::Relaxed) > 0)
            .unwrap_or(true);
        let mut confirm_soft_guard_age_us = confirm_soft_age_us;
        let mut confirm_hard_guard_age_us = confirm_hard_age_us;
        if lane_level == 0 {
            confirm_soft_guard_age_us = apply_confirm_guard_slack(
                confirm_soft_guard_age_us,
                state.v3_durable_confirm_guard_soft_slack_pct,
            );
            confirm_hard_guard_age_us = apply_confirm_guard_slack(
                confirm_hard_guard_age_us,
                state.v3_durable_confirm_guard_hard_slack_pct,
            );
        }
        if confirm_soft_guard_age_us > 0
            && confirm_hard_guard_age_us > 0
            && confirm_hard_guard_age_us <= confirm_soft_guard_age_us
        {
            confirm_hard_guard_age_us = confirm_soft_guard_age_us.saturating_add(1);
        }
        let hard_guard_admission_enabled =
            !(state.v3_durable_confirm_guard_hard_requires_admission && lane_level == 0);
        let soft_guard_admission_enabled =
            !(state.v3_durable_confirm_guard_soft_requires_admission && lane_level == 0);
        let hard_guard_enabled =
            hard_guard_admission_enabled && guard_secondary_enabled && hard_guard_armed;
        let soft_guard_enabled =
            soft_guard_admission_enabled && guard_secondary_enabled && soft_guard_armed;
        if confirm_hard_guard_age_us > 0 && confirm_oldest_age_us >= confirm_hard_guard_age_us {
            if hard_guard_enabled {
                state.increment_v3_rejected_hard_total(shard_id);
                state
                    .v3_durable_confirm_age_hard_reject_total
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .v3_durable_backpressure_hard_total
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(counter) = state
                    .v3_durable_backpressure_hard_total_per_lane
                    .get(durable_lane_id)
                {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                record_v3_ack(&state, t0, hotpath_sampled);
                let rejected_at_ns = now_nanos();
                emit_rejected_feedback(0, "V3_DURABLE_CONFIRM_AGE_HARD", rejected_at_ns);
                return V3HotPathOutcome::rejected(
                    StatusCode::SERVICE_UNAVAILABLE,
                    V3_TCP_KIND_REJECTED,
                    "REJECTED",
                    "V3_DURABLE_CONFIRM_AGE_HARD",
                    rejected_at_ns,
                );
            } else if !hard_guard_admission_enabled {
                state
                    .v3_durable_confirm_age_hard_reject_skipped_total
                    .fetch_add(1, Ordering::Relaxed);
            } else if !hard_guard_armed {
                state
                    .v3_durable_confirm_age_hard_reject_skipped_unarmed_total
                    .fetch_add(1, Ordering::Relaxed);
            } else if !guard_secondary_enabled {
                state
                    .v3_durable_confirm_age_hard_reject_skipped_low_load_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        if confirm_soft_guard_age_us > 0 && confirm_oldest_age_us >= confirm_soft_guard_age_us {
            if soft_guard_enabled {
                if !strategy_soft_bypass {
                    state.increment_v3_rejected_soft_total(shard_id);
                    state
                        .v3_durable_confirm_age_soft_reject_total
                        .fetch_add(1, Ordering::Relaxed);
                    state
                        .v3_durable_backpressure_soft_total
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(counter) = state
                        .v3_durable_backpressure_soft_total_per_lane
                        .get(durable_lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    record_v3_ack(&state, t0, hotpath_sampled);
                    let rejected_at_ns = now_nanos();
                    emit_rejected_feedback(0, "V3_DURABLE_CONFIRM_AGE_SOFT", rejected_at_ns);
                    return V3HotPathOutcome::rejected(
                        StatusCode::TOO_MANY_REQUESTS,
                        V3_TCP_KIND_REJECTED,
                        "REJECTED",
                        "V3_DURABLE_CONFIRM_AGE_SOFT",
                        rejected_at_ns,
                    );
                }
            } else if !soft_guard_admission_enabled {
                state
                    .v3_durable_confirm_age_soft_reject_skipped_total
                    .fetch_add(1, Ordering::Relaxed);
            } else if !soft_guard_armed {
                state
                    .v3_durable_confirm_age_soft_reject_skipped_unarmed_total
                    .fetch_add(1, Ordering::Relaxed);
            } else if !guard_secondary_enabled {
                state
                    .v3_durable_confirm_age_soft_reject_skipped_low_load_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        // 監視ループより先に深刻な飽和を検知した場合のみ即時hard拒否する。
        let durable_failsafe_hard = durable_queue_pct >= 99.0
            || (durable_backlog_signal_enabled
                && durable_backlog_growth_per_sec >= durable_backlog_hard_failsafe);
        if durable_failsafe_hard {
            state.increment_v3_rejected_hard_total(shard_id);
            state
                .v3_durable_backpressure_hard_total
                .fetch_add(1, Ordering::Relaxed);
            record_v3_ack(&state, t0, hotpath_sampled);
            let rejected_at_ns = now_nanos();
            emit_rejected_feedback(0, "V3_DURABLE_BACKPRESSURE_FAILSAFE", rejected_at_ns);
            return V3HotPathOutcome::rejected(
                StatusCode::SERVICE_UNAVAILABLE,
                V3_TCP_KIND_REJECTED,
                "REJECTED",
                "V3_DURABLE_BACKPRESSURE_FAILSAFE",
                rejected_at_ns,
            );
        }
        if state.v3_durable_admission_controller_enabled {
            let durable_level = lane_level;
            match durable_level {
                2 => {
                    state.increment_v3_rejected_hard_total(shard_id);
                    state
                        .v3_durable_backpressure_hard_total
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(counter) = state
                        .v3_durable_backpressure_hard_total_per_lane
                        .get(durable_lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    record_v3_ack(&state, t0, hotpath_sampled);
                    let rejected_at_ns = now_nanos();
                    emit_rejected_feedback(0, "V3_DURABLE_CONTROLLER_HARD", rejected_at_ns);
                    return V3HotPathOutcome::rejected(
                        StatusCode::SERVICE_UNAVAILABLE,
                        V3_TCP_KIND_REJECTED,
                        "REJECTED",
                        "V3_DURABLE_CONTROLLER_HARD",
                        rejected_at_ns,
                    );
                }
                1 => {
                    if !strategy_soft_bypass {
                        state.increment_v3_rejected_soft_total(shard_id);
                        state
                            .v3_durable_backpressure_soft_total
                            .fetch_add(1, Ordering::Relaxed);
                        if let Some(counter) = state
                            .v3_durable_backpressure_soft_total_per_lane
                            .get(durable_lane_id)
                        {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                        record_v3_ack(&state, t0, hotpath_sampled);
                        let rejected_at_ns = now_nanos();
                        emit_rejected_feedback(0, "V3_DURABLE_CONTROLLER_SOFT", rejected_at_ns);
                        return V3HotPathOutcome::rejected(
                            StatusCode::TOO_MANY_REQUESTS,
                            V3_TCP_KIND_REJECTED,
                            "REJECTED",
                            "V3_DURABLE_CONTROLLER_SOFT",
                            rejected_at_ns,
                        );
                    }
                }
                _ => {}
            }
        } else {
            if durable_queue_pct >= state.v3_durable_hard_reject_pct as f64
                || (durable_backlog_signal_enabled
                    && durable_backlog_growth_per_sec
                        >= state.v3_durable_backlog_hard_reject_per_sec)
            {
                state.increment_v3_rejected_hard_total(shard_id);
                state
                    .v3_durable_backpressure_hard_total
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(counter) = state
                    .v3_durable_backpressure_hard_total_per_lane
                    .get(durable_lane_id)
                {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                record_v3_ack(&state, t0, hotpath_sampled);
                let rejected_at_ns = now_nanos();
                emit_rejected_feedback(0, "V3_DURABLE_BACKPRESSURE_HARD", rejected_at_ns);
                return V3HotPathOutcome::rejected(
                    StatusCode::SERVICE_UNAVAILABLE,
                    V3_TCP_KIND_REJECTED,
                    "REJECTED",
                    "V3_DURABLE_BACKPRESSURE_HARD",
                    rejected_at_ns,
                );
            }
            if durable_queue_pct >= state.v3_durable_soft_reject_pct as f64
                || (durable_backlog_signal_enabled
                    && durable_backlog_growth_per_sec
                        >= state.v3_durable_backlog_soft_reject_per_sec)
            {
                if !strategy_soft_bypass {
                    state.increment_v3_rejected_soft_total(shard_id);
                    state
                        .v3_durable_backpressure_soft_total
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(counter) = state
                        .v3_durable_backpressure_soft_total_per_lane
                        .get(durable_lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    record_v3_ack(&state, t0, hotpath_sampled);
                    let rejected_at_ns = now_nanos();
                    emit_rejected_feedback(0, "V3_DURABLE_BACKPRESSURE_SOFT", rejected_at_ns);
                    return V3HotPathOutcome::rejected(
                        StatusCode::TOO_MANY_REQUESTS,
                        V3_TCP_KIND_REJECTED,
                        "REJECTED",
                        "V3_DURABLE_BACKPRESSURE_SOFT",
                        rejected_at_ns,
                    );
                }
            }
        }
    }

    // SOFT/HARD/KILL の3段水位。
    let queue_pct = ingress.queue_utilization_pct(shard_id);
    if queue_pct >= state.v3_kill_reject_pct {
        if ingress.kill_shard_due_to_watermark(shard_id, t0) {
            state.v3_shard_killed_total.fetch_add(1, Ordering::Relaxed);
        }
        state.increment_v3_rejected_killed_total(shard_id);
        record_v3_ack(&state, t0, hotpath_sampled);
        let rejected_at_ns = now_nanos();
        emit_rejected_feedback(0, "V3_QUEUE_KILLED", rejected_at_ns);
        return V3HotPathOutcome::rejected(
            StatusCode::SERVICE_UNAVAILABLE,
            V3_TCP_KIND_KILLED,
            "KILLED",
            "V3_QUEUE_KILLED",
            rejected_at_ns,
        );
    }
    if queue_pct >= state.v3_hard_reject_pct {
        state.increment_v3_rejected_hard_total(shard_id);
        record_v3_ack(&state, t0, hotpath_sampled);
        let rejected_at_ns = now_nanos();
        emit_rejected_feedback(0, "V3_BACKPRESSURE_HARD", rejected_at_ns);
        return V3HotPathOutcome::rejected(
            StatusCode::SERVICE_UNAVAILABLE,
            V3_TCP_KIND_REJECTED,
            "REJECTED",
            "V3_BACKPRESSURE_HARD",
            rejected_at_ns,
        );
    }

    if queue_pct >= state.v3_soft_reject_pct {
        if !strategy_soft_bypass {
            state.increment_v3_rejected_soft_total(shard_id);
            record_v3_ack(&state, t0, hotpath_sampled);
            let rejected_at_ns = now_nanos();
            emit_rejected_feedback(0, "V3_BACKPRESSURE_SOFT", rejected_at_ns);
            return V3HotPathOutcome::rejected(
                StatusCode::TOO_MANY_REQUESTS,
                V3_TCP_KIND_REJECTED,
                "REJECTED",
                "V3_BACKPRESSURE_SOFT",
                rejected_at_ns,
            );
        }
    }

    let session_seq = ingress.next_seq(session_id);
    let received_at_ns = now_nanos();
    let task = V3OrderTask {
        session_id: Arc::clone(&session_id_ref),
        account_id: Arc::clone(&account_id_ref),
        execution_run_id: execution_run_id.map(Arc::<str>::from),
        intent_id: intent_id.map(Arc::<str>::from),
        model_id: model_id.map(Arc::<str>::from),
        effective_risk_budget_ref: effective_risk_budget_ref.clone(),
        actual_policy: actual_policy.clone(),
        position_symbol_key: position_projection.symbol_key,
        position_delta_qty: position_projection.delta_qty,
        session_seq,
        attempt_seq: session_seq,
        received_at_ns,
        shard_id,
    };
    let feedback_context = state.quant_feedback_exporter.is_enabled().then(|| {
        (
            Arc::clone(&task.session_id),
            Arc::clone(&task.account_id),
            task.execution_run_id.clone(),
            task.intent_id.clone(),
            task.model_id.clone(),
            task.effective_risk_budget_ref.clone(),
            task.actual_policy.clone(),
            task.position_symbol_key,
            task.session_seq,
            task.received_at_ns,
        )
    });

    let enqueue_t0 = now_nanos();
    match ingress.try_enqueue(shard_id, task) {
        Ok(()) => {
            if hotpath_sampled {
                let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
                state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            }
            state.increment_v3_accepted_total(shard_id);
            if let Some((
                session_id,
                account_id,
                execution_run_id,
                intent_id,
                model_id,
                effective_risk_budget_ref,
                actual_policy,
                symbol_key,
                session_seq,
                received_at_ns,
            )) = feedback_context
            {
                let event = FeedbackEvent::accepted(
                    session_id.as_ref(),
                    session_seq,
                    account_id.as_ref(),
                    render_v3_symbol_key(symbol_key),
                    received_at_ns,
                )
                .push_path_tag("v3")
                .push_path_tag(path_tag)
                .push_path_tag("feedback");
                let event = if let Some(execution_run_id) = execution_run_id.as_deref() {
                    event.with_execution_run_id(execution_run_id)
                } else {
                    event
                };
                let event = if let Some(intent_id) = intent_id.as_deref() {
                    event.with_intent_id(intent_id)
                } else {
                    event
                };
                let event = if let Some(model_id) = model_id.as_deref() {
                    event.with_model_id(model_id)
                } else {
                    event
                };
                let event =
                    if let Some(effective_risk_budget_ref) = effective_risk_budget_ref.as_deref() {
                        event.with_effective_risk_budget_ref(effective_risk_budget_ref)
                    } else {
                        event
                    };
                let event = if let Some(actual_policy) = actual_policy.as_deref() {
                    event.with_actual_policy(actual_policy.clone())
                } else {
                    event
                };
                publish_quant_feedback(state, event);
            }
            let serialize_t0 = now_nanos();
            let body = V3HotPathOutcome::accepted(session_seq, received_at_ns);
            if hotpath_sampled {
                let serialize_elapsed = now_nanos().saturating_sub(serialize_t0) / 1_000;
                state.v3_stage_serialize_hist.record(serialize_elapsed);
            }
            record_v3_ack(&state, t0, hotpath_sampled);
            record_v3_ack_accepted(&state, t0, t0_tsc, hotpath_sampled);
            body
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(task)) => {
            if hotpath_sampled {
                let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
                state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            }
            if ingress.kill_shard_due_to_watermark(shard_id, now_nanos()) {
                state.v3_shard_killed_total.fetch_add(1, Ordering::Relaxed);
            }
            state.register_v3_loss_suspect(
                task.session_id.as_ref(),
                task.session_seq,
                task.shard_id,
                "V3_INGRESS_QUEUE_FULL",
                now_nanos(),
            );
            state.increment_v3_rejected_killed_total(shard_id);
            record_v3_ack(&state, t0, hotpath_sampled);
            let rejected_at_ns = now_nanos();
            emit_rejected_feedback(task.session_seq, "V3_QUEUE_FULL", rejected_at_ns);
            V3HotPathOutcome::rejected(
                StatusCode::SERVICE_UNAVAILABLE,
                V3_TCP_KIND_KILLED,
                "KILLED",
                "V3_QUEUE_FULL",
                rejected_at_ns,
            )
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(task)) => {
            if hotpath_sampled {
                let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
                state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            }
            if ingress.kill_shard_due_to_watermark(shard_id, now_nanos()) {
                state.v3_shard_killed_total.fetch_add(1, Ordering::Relaxed);
            }
            state.register_v3_loss_suspect(
                task.session_id.as_ref(),
                task.session_seq,
                task.shard_id,
                "V3_INGRESS_CLOSED",
                now_nanos(),
            );
            state.increment_v3_rejected_killed_total(shard_id);
            record_v3_ack(&state, t0, hotpath_sampled);
            let rejected_at_ns = now_nanos();
            emit_rejected_feedback(task.session_seq, "V3_INGRESS_CLOSED", rejected_at_ns);
            V3HotPathOutcome::rejected(
                StatusCode::SERVICE_UNAVAILABLE,
                V3_TCP_KIND_KILLED,
                "KILLED",
                "V3_INGRESS_CLOSED",
                rejected_at_ns,
            )
        }
    }
}

pub(super) fn decode_v3_tcp_frame<'a>(
    frame: &'a [u8; V3_TCP_REQUEST_SIZE],
) -> Result<V3TcpDecodedFrame<'a>, u32> {
    let token_len =
        u16::from_le_bytes(frame[0..2].try_into().expect("token length bytes")) as usize;
    if token_len > V3_TCP_TOKEN_MAX_LEN {
        return Err(V3_TCP_REASON_BAD_TOKEN_LEN);
    }
    let jwt_token = if token_len == 0 {
        None
    } else {
        let token_end = V3_TCP_TOKEN_OFFSET + token_len;
        let token = std::str::from_utf8(&frame[V3_TCP_TOKEN_OFFSET..token_end])
            .map_err(|_| V3_TCP_REASON_BAD_TOKEN_UTF8)?;
        Some(token)
    };
    let intent_len = u16::from_le_bytes(
        frame[V3_TCP_INTENT_LEN_OFFSET..(V3_TCP_INTENT_LEN_OFFSET + 2)]
            .try_into()
            .expect("intent length bytes"),
    ) as usize;
    let model_len = u16::from_le_bytes(
        frame[V3_TCP_MODEL_LEN_OFFSET..(V3_TCP_MODEL_LEN_OFFSET + 2)]
            .try_into()
            .expect("model length bytes"),
    ) as usize;

    let side_raw = frame[V3_TCP_SIDE_OFFSET];
    let order_type_raw = frame[V3_TCP_TYPE_OFFSET];
    let qty = u64::from_le_bytes(
        frame[V3_TCP_QTY_OFFSET..(V3_TCP_QTY_OFFSET + 8)]
            .try_into()
            .expect("qty bytes"),
    );
    let price_or_reserved = u64::from_le_bytes(
        frame[V3_TCP_PRICE_OFFSET..(V3_TCP_PRICE_OFFSET + 8)]
            .try_into()
            .expect("price bytes"),
    );

    // AuthInit frame marker:
    // side/type/qty/price are all 0, and JWT is present.
    let is_auth_init = side_raw == 0 && order_type_raw == 0 && qty == 0 && price_or_reserved == 0;
    if is_auth_init {
        let jwt_token = jwt_token.ok_or(V3_TCP_REASON_AUTH_REQUIRED)?;
        return Ok(V3TcpDecodedFrame::AuthInit { jwt_token });
    }

    let (intent_id, model_id) = if token_len > 0 {
        if intent_len > 0 || model_len > 0 {
            return Err(V3_TCP_REASON_METADATA_WITH_INLINE_TOKEN);
        }
        (None, None)
    } else {
        if intent_len > V3_TCP_TOKEN_MAX_LEN {
            return Err(V3_TCP_REASON_BAD_INTENT_LEN);
        }
        let model_offset = V3_TCP_TOKEN_OFFSET + intent_len;
        if model_len > V3_TCP_TOKEN_MAX_LEN.saturating_sub(intent_len) {
            return Err(V3_TCP_REASON_BAD_MODEL_LEN);
        }
        let intent_id = if intent_len == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(
                    &frame[V3_TCP_TOKEN_OFFSET..(V3_TCP_TOKEN_OFFSET + intent_len)],
                )
                .map_err(|_| V3_TCP_REASON_BAD_METADATA_UTF8)?,
            )
        };
        let model_id = if model_len == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(&frame[model_offset..(model_offset + model_len)])
                    .map_err(|_| V3_TCP_REASON_BAD_METADATA_UTF8)?,
            )
        };
        (intent_id, model_id)
    };

    let symbol_raw = &frame[V3_TCP_SYMBOL_OFFSET..(V3_TCP_SYMBOL_OFFSET + V3_TCP_SYMBOL_LEN)];
    let symbol_len = symbol_raw
        .iter()
        .position(|b| *b == 0)
        .unwrap_or(symbol_raw.len());
    if symbol_len == 0 {
        return Err(V3_TCP_REASON_BAD_SYMBOL);
    }
    let symbol =
        std::str::from_utf8(&symbol_raw[..symbol_len]).map_err(|_| V3_TCP_REASON_BAD_SYMBOL)?;
    let side = match side_raw {
        1 | 2 => side_raw,
        _ => return Err(V3_TCP_REASON_BAD_SIDE),
    };
    let order_type = match order_type_raw {
        1 => crate::order::OrderType::Limit,
        2 => crate::order::OrderType::Market,
        _ => return Err(V3_TCP_REASON_BAD_TYPE),
    };
    let price = if order_type == crate::order::OrderType::Market {
        0
    } else {
        price_or_reserved
    };
    let symbol_key = parse_v3_symbol_key(symbol).ok_or(V3_TCP_REASON_BAD_SYMBOL)?;

    Ok(V3TcpDecodedFrame::Order(V3TcpDecodedRequest {
        jwt_token,
        intent_id,
        model_id,
        symbol_key,
        side,
        order_type,
        qty,
        price,
    }))
}

pub(super) fn decode_v3_tcp_request<'a>(
    frame: &'a [u8; V3_TCP_REQUEST_SIZE],
) -> Result<V3TcpDecodedRequest<'a>, u32> {
    match decode_v3_tcp_frame(frame)? {
        V3TcpDecodedFrame::Order(decoded) => Ok(decoded),
        V3TcpDecodedFrame::AuthInit { .. } => Err(V3_TCP_REASON_BAD_TYPE),
    }
}

pub(super) fn authenticate_v3_tcp_token(
    state: &AppState,
    jwt_token: &str,
) -> Result<crate::auth::Principal, (StatusCode, u32)> {
    match state.jwt_auth.authenticate_token(jwt_token) {
        AuthResult::Ok(p) => Ok(p),
        AuthResult::Err(e) => {
            let (status, reason) = match e {
                AuthError::SecretNotConfigured => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    V3_TCP_REASON_AUTH_INTERNAL,
                ),
                AuthError::TokenExpired => (StatusCode::UNAUTHORIZED, V3_TCP_REASON_AUTH_EXPIRED),
                AuthError::TokenNotYetValid => {
                    (StatusCode::UNAUTHORIZED, V3_TCP_REASON_AUTH_NOT_YET_VALID)
                }
                _ => (StatusCode::UNAUTHORIZED, V3_TCP_REASON_AUTH_INVALID),
            };
            Err((status, reason))
        }
    }
}

pub(super) fn encode_v3_tcp_decode_error(
    status: StatusCode,
    reason_code: u32,
    received_at_ns: u64,
) -> [u8; V3_TCP_RESPONSE_SIZE] {
    encode_v3_tcp_response_raw(
        V3_TCP_KIND_DECODE_ERROR,
        status,
        reason_code,
        0,
        0,
        received_at_ns,
    )
}

pub(super) fn encode_v3_tcp_auth_ok(received_at_ns: u64) -> [u8; V3_TCP_RESPONSE_SIZE] {
    encode_v3_tcp_response_raw(
        V3_TCP_KIND_ACCEPT,
        StatusCode::ACCEPTED,
        V3_TCP_REASON_NONE,
        0,
        0,
        received_at_ns,
    )
}

pub(super) fn encode_v3_tcp_response(
    status: StatusCode,
    resp: &VolatileOrderResponse,
) -> [u8; V3_TCP_RESPONSE_SIZE] {
    encode_v3_tcp_response_raw(
        v3_tcp_kind(resp),
        status,
        v3_tcp_reason_code_from_reason(resp.reason.as_deref()),
        resp.session_seq.unwrap_or(0),
        resp.session_seq.unwrap_or(0),
        resp.received_at_ns,
    )
}

fn encode_v3_tcp_response_raw(
    kind: u8,
    status: StatusCode,
    reason_code: u32,
    session_seq: u64,
    attempt_seq: u64,
    received_at_ns: u64,
) -> [u8; V3_TCP_RESPONSE_SIZE] {
    let mut out = [0u8; V3_TCP_RESPONSE_SIZE];
    out[0] = kind;
    out[1] = 0;
    out[2..4].copy_from_slice(&(status.as_u16()).to_le_bytes());
    out[4..8].copy_from_slice(&reason_code.to_le_bytes());
    out[8..16].copy_from_slice(&session_seq.to_le_bytes());
    out[16..24].copy_from_slice(&attempt_seq.to_le_bytes());
    out[24..32].copy_from_slice(&received_at_ns.to_le_bytes());
    out
}

fn v3_tcp_kind(resp: &VolatileOrderResponse) -> u8 {
    match resp.status {
        "VOLATILE_ACCEPT" => V3_TCP_KIND_ACCEPT,
        "KILLED" => V3_TCP_KIND_KILLED,
        _ => V3_TCP_KIND_REJECTED,
    }
}

fn v3_tcp_reason_code_from_reason(reason: Option<&str>) -> u32 {
    match reason {
        None => V3_TCP_REASON_NONE,
        Some("INVALID_QTY") => 1_001,
        Some("INVALID_SIDE") => 1_002,
        Some("INVALID_PRICE") => 1_003,
        Some("INVALID_SYMBOL") => 1_004,
        Some("RISK_REJECT") => 1_100,
        Some("BAD_TOKEN_LEN") => V3_TCP_REASON_BAD_TOKEN_LEN,
        Some("BAD_SYMBOL") => V3_TCP_REASON_BAD_SYMBOL,
        Some("BAD_SIDE") => V3_TCP_REASON_BAD_SIDE,
        Some("BAD_TYPE") => V3_TCP_REASON_BAD_TYPE,
        Some("BAD_TOKEN_UTF8") => V3_TCP_REASON_BAD_TOKEN_UTF8,
        Some("AUTH_INVALID") => V3_TCP_REASON_AUTH_INVALID,
        Some("AUTH_EXPIRED") => V3_TCP_REASON_AUTH_EXPIRED,
        Some("AUTH_NOT_YET_VALID") => V3_TCP_REASON_AUTH_NOT_YET_VALID,
        Some("AUTH_INTERNAL") => V3_TCP_REASON_AUTH_INTERNAL,
        Some("AUTH_REQUIRED") => V3_TCP_REASON_AUTH_REQUIRED,
        Some("AUTH_UNEXPECTED_TOKEN") => V3_TCP_REASON_AUTH_UNEXPECTED_TOKEN,
        Some("V3_DURABLE_BACKPRESSURE_SOFT") => 2_001,
        Some("V3_DURABLE_BACKPRESSURE_HARD") => 2_002,
        Some("V3_BACKPRESSURE_SOFT") => 2_101,
        Some("V3_BACKPRESSURE_HARD") => 2_102,
        Some("V3_QUEUE_KILLED") => 2_201,
        Some("V3_QUEUE_FULL") => 2_202,
        Some("V3_INGRESS_CLOSED") => 2_203,
        Some("V3_GLOBAL_KILLED") => 2_301,
        Some("V3_SESSION_KILLED") => 2_302,
        Some("V3_SHARD_KILLED") => 2_303,
        Some("V3_STRATEGY_SNAPSHOT_STALE") => 2_401,
        Some("V3_STRATEGY_MAX_ORDER_QTY") => 2_402,
        Some("V3_STRATEGY_MAX_NOTIONAL") => 2_403,
        Some("V3_STRATEGY_ACCOUNT_MAX_NOTIONAL") => 2_404,
        Some(_) => 9_999,
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

    if let Some(reason) = validate_order_request(&req) {
        match reason {
            "INVALID_QTY" => {
                state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
            }
            "INVALID_SYMBOL" => {
                state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
            }
            "INVALID_PRICE" => {
                state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        record_ack(&state, t0);
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(OrderResponse::rejected(reason)),
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
                    crate::order::TimeInForce::Ioc => "IOC",
                    crate::order::TimeInForce::Fok => "FOK",
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

/// 注文訂正（POST /orders/{order_id}/amend）
/// - 状態遷移: ACTIVE -> AMEND_REQUESTED
/// - 監査/Bus に AmendRequested を記録
pub(super) async fn handle_amend_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
    Json(req): Json<AmendRequest>,
) -> Result<(StatusCode, Json<AmendResponse>), (StatusCode, Json<AuthErrorResponse>)> {
    let auth_header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());

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
        state
            .sharded_store
            .find_by_id_with_account(&order_id, acc_id)
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

    if order.status.is_terminal() || order.status == crate::store::OrderStatus::CancelRequested {
        return Ok((
            StatusCode::CONFLICT,
            Json(AmendResponse {
                order_id,
                status: "REJECTED".into(),
                reason: Some("ORDER_FINAL".into()),
            }),
        ));
    }

    if req.new_qty == 0 {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(AmendResponse {
                order_id,
                status: "REJECTED".into(),
                reason: Some("INVALID_QTY".into()),
            }),
        ));
    }

    if req.new_price == 0 {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(AmendResponse {
                order_id,
                status: "REJECTED".into(),
                reason: Some("INVALID_PRICE".into()),
            }),
        ));
    }

    if order.status == crate::store::OrderStatus::AmendRequested {
        return Ok((
            StatusCode::ACCEPTED,
            Json(AmendResponse {
                order_id,
                status: "AMEND_REQUESTED".into(),
                reason: None,
            }),
        ));
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let updated = state
        .sharded_store
        .update(&order.order_id, &order.account_id, |prev| {
            let mut next = prev.clone();
            next.qty = req.new_qty;
            next.price = Some(req.new_price);
            next.status = crate::store::OrderStatus::AmendRequested;
            next.last_update_at = now_ms;
            next
        });

    if updated.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(AuthErrorResponse {
                error: "NOT_FOUND".into(),
            }),
        ));
    }

    let amend_data = serde_json::json!({
        "newQty": req.new_qty,
        "newPrice": req.new_price,
        "comment": req.comment,
    });
    state.audit_log.append(AuditEvent {
        event_type: "AmendRequested".into(),
        at: audit::now_millis(),
        account_id: order.account_id.clone(),
        order_id: Some(order.order_id.clone()),
        data: amend_data.clone(),
    });
    if !state.bus_mode_outbox {
        state.bus_publisher.publish(BusEvent {
            event_type: "AmendRequested".into(),
            at: crate::bus::format_event_time(audit::now_millis()),
            account_id: order.account_id.clone(),
            order_id: Some(order.order_id.clone()),
            data: amend_data,
        });
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(AmendResponse {
            order_id,
            status: "AMEND_REQUESTED".into(),
            reason: None,
        }),
    ))
}

/// 注文置換（POST /orders/{order_id}/replace）
/// - 現時点では amend と同じ契約で扱う
pub(super) async fn handle_replace_order(
    state: State<AppState>,
    headers: HeaderMap,
    order_id: Path<String>,
    req: Json<AmendRequest>,
) -> Result<(StatusCode, Json<AmendResponse>), (StatusCode, Json<AuthErrorResponse>)> {
    handle_amend_order(state, headers, order_id, req).await
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

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AmendRequest {
    new_qty: u64,
    new_price: u64,
    #[serde(default)]
    comment: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AmendResponse {
    order_id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

/// v3 入口の即時応答（VOLATILE_ACCEPT）
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VolatileOrderResponse {
    session_id: PooledJsonString,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<PooledJsonString>,
    received_at_ns: u64,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<PooledJsonString>,
}

const V3_JSON_STRING_POOL_MAX_ITEMS: usize = 4096;
const V3_JSON_STRING_POOL_MAX_CAPACITY: usize = 256;

thread_local! {
    static V3_JSON_STRING_POOL: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn v3_pool_take_string(min_capacity: usize) -> String {
    V3_JSON_STRING_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if let Some(mut cached) = pool.pop() {
            if cached.capacity() < min_capacity {
                cached.reserve(min_capacity - cached.capacity());
            }
            cached
        } else {
            String::with_capacity(min_capacity.max(32))
        }
    })
}

fn v3_pool_put_string(mut value: String) {
    if value.capacity() > V3_JSON_STRING_POOL_MAX_CAPACITY {
        return;
    }
    value.clear();
    V3_JSON_STRING_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < V3_JSON_STRING_POOL_MAX_ITEMS {
            pool.push(value);
        }
    });
}

#[derive(Debug)]
struct PooledJsonString {
    inner: Option<String>,
}

impl PooledJsonString {
    fn from_str(value: &str) -> Self {
        let mut inner = v3_pool_take_string(value.len());
        inner.push_str(value);
        Self { inner: Some(inner) }
    }

    fn with_prefix_u64(prefix: &str, value: u64) -> Self {
        let mut inner = v3_pool_take_string(prefix.len() + 20);
        inner.push_str(prefix);
        append_u64_decimal(&mut inner, value);
        Self { inner: Some(inner) }
    }

    fn as_str(&self) -> &str {
        self.inner.as_deref().unwrap_or("")
    }
}

impl Clone for PooledJsonString {
    fn clone(&self) -> Self {
        Self::from_str(self.as_str())
    }
}

impl Deref for PooledJsonString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl serde::Serialize for PooledJsonString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Drop for PooledJsonString {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            v3_pool_put_string(inner);
        }
    }
}

fn build_attempt_id(session_seq: u64) -> PooledJsonString {
    PooledJsonString::with_prefix_u64("att_", session_seq)
}

fn append_u64_decimal(out: &mut String, mut value: u64) {
    if value == 0 {
        out.push('0');
        return;
    }
    let mut buf = [0u8; 20];
    let mut pos = buf.len();
    while value > 0 {
        pos -= 1;
        buf[pos] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    for b in &buf[pos..] {
        out.push(*b as char);
    }
}

impl VolatileOrderResponse {
    fn from_hotpath(session_id: &str, outcome: V3HotPathOutcome) -> Self {
        match outcome.session_seq {
            Some(session_seq) => Self {
                session_id: PooledJsonString::from_str(session_id),
                session_seq: Some(session_seq),
                attempt_id: Some(build_attempt_id(session_seq)),
                received_at_ns: outcome.received_at_ns,
                status: outcome.status_text,
                reason: None,
            },
            None => Self {
                session_id: PooledJsonString::from_str(session_id),
                session_seq: None,
                attempt_id: None,
                received_at_ns: outcome.received_at_ns,
                status: outcome.status_text,
                reason: outcome.reason_text.map(PooledJsonString::from_str),
            },
        }
    }

    pub(super) fn algo_runtime_scheduled(session_id: &str, received_at_ns: u64) -> Self {
        Self {
            session_id: PooledJsonString::from_str(session_id),
            session_seq: None,
            attempt_id: None,
            received_at_ns,
            status: "ALGO_RUNTIME_SCHEDULED",
            reason: None,
        }
    }

    fn accepted(session_id: String, session_seq: u64, received_at_ns: u64) -> Self {
        Self {
            session_id: PooledJsonString::from_str(&session_id),
            session_seq: Some(session_seq),
            attempt_id: Some(build_attempt_id(session_seq)),
            received_at_ns,
            status: "VOLATILE_ACCEPT",
            reason: None,
        }
    }

    fn rejected(session_id: &str, status: &'static str, reason: &str) -> Self {
        Self {
            session_id: PooledJsonString::from_str(session_id),
            session_seq: None,
            attempt_id: None,
            received_at_ns: now_nanos(),
            status,
            reason: Some(PooledJsonString::from_str(reason)),
        }
    }

    pub(super) fn session_seq(&self) -> Option<u64> {
        self.session_seq
    }

    pub(super) fn status_text(&self) -> &'static str {
        self.status
    }

    pub(super) fn reason_text(&self) -> Option<&str> {
        self.reason.as_deref()
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

#[cfg(test)]
mod tests {
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
        StrategyIntent, StrategyRecoveryPolicy,
    };
    use crate::strategy::runtime::{
        AlgoChildStatus, AlgoParentExecution, AlgoParentStatus,
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
        let v3_durable_worker_loop_hist_per_lane =
            Arc::new(vec![Arc::new(LatencyHistogram::new())]);

        AppState {
            engine: FastPathEngine::new(16_384),
            jwt_auth: Arc::new(crate::auth::JwtAuth::for_test(TEST_JWT_SECRET)),
            order_store: Arc::new(OrderStore::new()),
            sharded_store: Arc::new(ShardedOrderStore::new_with_ttl_and_shards(86_400_000, 64)),
            order_id_map: Arc::new(OrderIdMap::new()),
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
        }
    }

    fn strategy_intent_fixture() -> StrategyIntent {
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
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
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
            StrategyRecoveryPolicy::GatewayManagedResume,
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
            },
            Some("budget-42".to_string()),
            None,
            None,
            start_at_ns.saturating_sub(10),
        )
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
            frame[model_offset..(model_offset + model_len)]
                .copy_from_slice(&model_bytes[..model_len]);
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

    #[tokio::test]
    async fn v3_hot_risk_fixture_matches_rust_implementation() {
        let fixture: RiskDecisionFixture = load_fixture("contracts/fixtures/risk_decision_v1.json");
        let mut mismatches = Vec::new();

        for case in fixture.cases {
            let mut state = build_test_state();
            state.v3_risk_strict_symbols = case.input.strict_symbols;
            state.v3_risk_max_order_qty = 100_000_000;
            state.v3_risk_max_notional = 1_000_000_000;

            let mut symbol_limits = HashMap::new();
            if case.input.symbol_known {
                if let Some(symbol_key) = parse_v3_symbol_key(&case.input.symbol) {
                    symbol_limits.insert(
                        symbol_key,
                        gateway_core::SymbolLimits {
                            max_order_qty: state.v3_risk_max_order_qty.min(u32::MAX as u64) as u32,
                            max_notional: state.v3_risk_max_notional,
                            tick_size: 1,
                        },
                    );
                }
            }
            state.v3_symbol_limits = Arc::new(symbol_limits);

            let req = OrderRequest {
                symbol: case.input.symbol.clone(),
                side: case.input.side.clone(),
                order_type: fixture_order_type(&case.input.order_type),
                qty: case.input.qty,
                price: Some(case.input.price),
                time_in_force: TimeInForce::Gtc,
                expire_at: None,
                client_order_id: None,
                intent_id: None,
                model_id: None,
                execution_run_id: None,
            };
            let account_ref = state.intern_v3_account_id("fixture-risk");

            let (allowed, reason, http_status) =
                match evaluate_v3_hot_risk(&state, &account_ref, &req, true) {
                    Ok(_) => (true, None, StatusCode::ACCEPTED.as_u16()),
                    Err(reason) => (
                        false,
                        Some(reason.to_string()),
                        StatusCode::UNPROCESSABLE_ENTITY.as_u16(),
                    ),
                };

            if allowed != case.expected.allowed
                || reason.as_ref() != case.expected.reason.as_ref()
                || http_status != case.expected.http_status
            {
                mismatches.push(format!(
                    "{} expected=({}, {:?}, {}) actual=({}, {:?}, {})",
                    case.id,
                    case.expected.allowed,
                    case.expected.reason,
                    case.expected.http_status,
                    allowed,
                    reason,
                    http_status
                ));
            }
        }

        assert!(
            mismatches.is_empty(),
            "risk fixture mismatches: {:?}",
            mismatches
        );
    }

    #[test]
    fn v3_reject_reason_fixture_matches_rust_tcp_reason_mapping() {
        let fixture: RejectReasonFixture = load_fixture("contracts/fixtures/reject_reason_v1.json");
        let mut mismatches = Vec::new();

        for case in fixture.reasons {
            let resp = VolatileOrderResponse::rejected("fixture", "REJECTED", &case.reason);
            let actual = v3_tcp_reason_code_from_reason(resp.reason.as_deref());
            if actual != case.tcp_reason_code {
                mismatches.push(format!(
                    "{} expected={} actual={}",
                    case.reason, case.tcp_reason_code, actual
                ));
            }
        }

        let unknown = VolatileOrderResponse::rejected("fixture", "REJECTED", "UNKNOWN_REASON");
        assert_eq!(
            v3_tcp_reason_code_from_reason(unknown.reason.as_deref()),
            9_999
        );
        assert!(
            mismatches.is_empty(),
            "reject reason fixture mismatches: {:?}",
            mismatches
        );
    }

    #[test]
    fn amend_fixture_matches_phase05_contract_baseline() {
        let fixture: AmendDecisionFixture =
            load_fixture("contracts/fixtures/amend_decision_v1.json");
        assert!(
            !fixture.cases.is_empty(),
            "amend fixture must contain at least one case"
        );

        let mut reasons = HashSet::new();
        let mut has_amend_requested = false;

        for case in fixture.cases {
            assert_eq!(case.scope, "amend_v1");
            assert!(
                [202, 409, 422].contains(&case.expected.http_status),
                "unexpected http status: {}",
                case.expected.http_status
            );

            if case.expected.allowed {
                assert!(
                    case.expected.reason.is_none(),
                    "allowed case should not have reason"
                );
            } else {
                assert!(
                    case.expected.reason.is_some(),
                    "rejected case should have reason"
                );
            }

            if let Some(reason) = case.expected.reason {
                reasons.insert(reason);
            }
            if case.expected.next_status == "AMEND_REQUESTED" {
                has_amend_requested = true;
            }
        }

        assert!(
            has_amend_requested,
            "fixture must include AMEND_REQUESTED transition"
        );
        assert!(
            reasons.contains("ORDER_FINAL"),
            "fixture must include ORDER_FINAL"
        );
        assert!(
            reasons.contains("INVALID_QTY"),
            "fixture must include INVALID_QTY"
        );
        assert!(
            reasons.contains("INVALID_PRICE"),
            "fixture must include INVALID_PRICE"
        );
    }

    #[test]
    fn tif_fixture_matches_phase05_contract_baseline() {
        let fixture: TifPolicyFixture = load_fixture("contracts/fixtures/tif_policy_v1.json");
        assert!(
            !fixture.cases.is_empty(),
            "tif fixture must contain at least one case"
        );

        let mut tifs = HashSet::new();
        let mut reasons = HashSet::new();
        for case in fixture.cases {
            assert_eq!(case.scope, "time_in_force_v1");
            tifs.insert(case.input.time_in_force);

            if case.expected.allowed {
                assert!(
                    case.expected.reason.is_none(),
                    "allowed case should not have reason"
                );
                assert!(
                    case.expected.effective_time_in_force.is_some(),
                    "allowed case should have effectiveTimeInForce"
                );
            } else {
                assert!(
                    case.expected.reason.is_some(),
                    "rejected case should have reason"
                );
            }

            if let Some(reason) = case.expected.reason {
                reasons.insert(reason);
            }
        }

        assert!(tifs.contains("IOC"), "fixture must include IOC");
        assert!(tifs.contains("FOK"), "fixture must include FOK");
        assert!(
            reasons.contains("INVALID_PRICE"),
            "fixture must include INVALID_PRICE scenario"
        );
    }

    #[tokio::test]
    async fn tif_fixture_matches_order_ingress_behavior() {
        let fixture: TifPolicyFixture = load_fixture("contracts/fixtures/tif_policy_v1.json");
        for case in fixture.cases {
            let state = build_test_state();
            let account_id = "4301";
            let idempotency_key = format!("idem_tif_{}", case.id);
            let req = OrderRequest {
                symbol: "AAPL".into(),
                side: "BUY".into(),
                order_type: fixture_order_type(&case.input.order_type),
                qty: 100,
                price: Some(case.input.price),
                time_in_force: fixture_time_in_force(&case.input.time_in_force),
                expire_at: None,
                client_order_id: Some(format!("cid_tif_{}", case.id)),
                intent_id: None,
                model_id: None,
                execution_run_id: None,
            };
            let (status, Json(resp)) = handle_order(
                State(state.clone()),
                headers(account_id, Some(idempotency_key.as_str())),
                Json(req),
            )
            .await
            .unwrap_or_else(|_| panic!("tif fixture case failed: {}", case.id));

            assert_eq!(
                status.as_u16(),
                case.expected.http_status,
                "case {} http status mismatch",
                case.id
            );

            if case.expected.allowed {
                assert_eq!(resp.status, "ACCEPTED", "case {}", case.id);
                assert!(resp.reason.is_none(), "case {}", case.id);
                let stored = state
                    .sharded_store
                    .find_by_id_with_account(&resp.order_id, account_id)
                    .expect("stored order for allowed tif");
                let expected_tif = case
                    .expected
                    .effective_time_in_force
                    .as_deref()
                    .expect("allowed tif must have effective value");
                assert_eq!(
                    stored.time_in_force,
                    fixture_time_in_force(expected_tif),
                    "case {} effective tif mismatch",
                    case.id
                );
            } else {
                assert_eq!(resp.status, "REJECTED", "case {}", case.id);
                assert_eq!(
                    resp.reason.as_deref(),
                    case.expected.reason.as_deref(),
                    "case {} reason mismatch",
                    case.id
                );
            }
        }
    }

    #[test]
    fn position_cap_fixture_matches_phase05_contract_baseline() {
        let fixture: PositionCapFixture = load_fixture("contracts/fixtures/position_cap_v1.json");
        assert!(
            !fixture.cases.is_empty(),
            "position cap fixture must contain at least one case"
        );

        let mut reasons = HashSet::new();
        for case in fixture.cases {
            assert_eq!(case.scope, "position_cap_v1");
            assert!(
                [202, 422].contains(&case.expected.http_status),
                "unexpected http status: {}",
                case.expected.http_status
            );
            if case.expected.allowed {
                assert!(
                    case.expected.reason.is_none(),
                    "allowed case should not have reason"
                );
            } else {
                assert!(
                    case.expected.reason.is_some(),
                    "rejected case should have reason"
                );
            }
            if let Some(reason) = case.expected.reason {
                reasons.insert(reason);
            }
        }

        assert!(
            reasons.contains("POSITION_LIMIT_EXCEEDED"),
            "fixture must include POSITION_LIMIT_EXCEEDED"
        );
        assert!(
            reasons.contains("INVALID_QTY"),
            "fixture must include INVALID_QTY"
        );
        assert!(
            reasons.contains("INVALID_SIDE"),
            "fixture must include INVALID_SIDE"
        );
    }

    #[tokio::test]
    async fn position_cap_fixture_matches_v3_hot_risk_oracle() {
        let fixture: PositionCapFixture = load_fixture("contracts/fixtures/position_cap_v1.json");
        let mut mismatches = Vec::new();

        for case in fixture.cases {
            let mut state = build_test_state();
            state.v3_risk_max_abs_position_qty = case.input.max_abs_position_qty;

            let symbol_key = parse_v3_symbol_key(&case.input.symbol).expect("valid symbol key");
            let account_ref = state.intern_v3_account_id("fixture-position");
            state.v3_account_symbol_position.insert(
                super::super::V3AccountSymbolKey::new(Arc::clone(&account_ref), symbol_key),
                case.input.current_position_qty,
            );

            let req = OrderRequest {
                symbol: case.input.symbol.clone(),
                side: case.input.side.clone(),
                order_type: OrderType::Limit,
                qty: case.input.qty,
                price: Some(15_000),
                time_in_force: TimeInForce::Gtc,
                expire_at: None,
                client_order_id: None,
                intent_id: None,
                model_id: None,
                execution_run_id: None,
            };

            let (allowed, reason, http_status, projected_position_qty) =
                match evaluate_v3_hot_risk(&state, &account_ref, &req, true) {
                    Ok(projection) => (
                        true,
                        None,
                        StatusCode::ACCEPTED.as_u16(),
                        case.input
                            .current_position_qty
                            .saturating_add(projection.delta_qty),
                    ),
                    Err(reason) => {
                        let projected = if reason == "POSITION_LIMIT_EXCEEDED" {
                            fixture_projected_position_qty(
                                &case.input.side,
                                case.input.current_position_qty,
                                case.input.qty,
                            )
                        } else {
                            case.input.current_position_qty
                        };
                        (
                            false,
                            Some(reason.to_string()),
                            StatusCode::UNPROCESSABLE_ENTITY.as_u16(),
                            projected,
                        )
                    }
                };

            if allowed != case.expected.allowed
                || reason.as_ref() != case.expected.reason.as_ref()
                || http_status != case.expected.http_status
                || projected_position_qty != case.expected.projected_position_qty
            {
                mismatches.push(format!(
                    "{} expected=({}, {:?}, {}, {}) actual=({}, {:?}, {}, {})",
                    case.id,
                    case.expected.allowed,
                    case.expected.reason,
                    case.expected.http_status,
                    case.expected.projected_position_qty,
                    allowed,
                    reason,
                    http_status,
                    projected_position_qty
                ));
            }
        }

        assert!(
            mismatches.is_empty(),
            "position fixture mismatches: {:?}",
            mismatches
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
    async fn amend_updates_active_order_to_amend_requested() {
        let state = build_test_state();
        let account_id = "4001";
        let order_id = "ord_amend_1";
        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_amend_1"),
            Some("idem_amend_1"),
        );

        let request = AmendRequest {
            new_qty: 123,
            new_price: 15100,
            comment: Some("replace qty/price".into()),
        };
        let (status, Json(resp)) = handle_amend_order(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.to_string()),
            Json(request),
        )
        .await
        .unwrap_or_else(|_| panic!("amend request failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "AMEND_REQUESTED");
        assert!(resp.reason.is_none());

        let updated = state
            .sharded_store
            .find_by_id_with_account(order_id, account_id)
            .expect("updated order");
        assert_eq!(updated.status, OrderStatus::AmendRequested);
        assert_eq!(updated.qty, 123);
        assert_eq!(updated.price, Some(15100));
    }

    #[tokio::test]
    async fn amend_handler_matches_amend_fixture_cases() {
        let fixture: AmendDecisionFixture =
            load_fixture("contracts/fixtures/amend_decision_v1.json");

        for case in fixture.cases {
            let state = build_test_state();
            let account_id = "4101";
            let order_id = format!("ord_amend_fixture_{}", case.id);
            put_order(
                &state,
                &order_id,
                account_id,
                Some(&format!("cid_amend_fixture_{}", case.id)),
                Some(&format!("idem_amend_fixture_{}", case.id)),
            );
            let from_status = fixture_order_status(&case.from_status);
            if from_status != OrderStatus::Accepted {
                let _ = state.sharded_store.update(&order_id, account_id, |prev| {
                    let mut next = prev.clone();
                    next.status = from_status;
                    next
                });
            }

            let req = AmendRequest {
                new_qty: case.input.new_qty,
                new_price: case.input.new_price,
                comment: case.input.comment.clone(),
            };
            let (status, Json(resp)) = handle_amend_order(
                State(state.clone()),
                headers(account_id, None),
                Path(order_id.clone()),
                Json(req),
            )
            .await
            .unwrap_or_else(|_| panic!("amend fixture case failed: {}", case.id));

            assert_eq!(
                status.as_u16(),
                case.expected.http_status,
                "case {} http status mismatch",
                case.id
            );
            if case.expected.allowed {
                assert_eq!(resp.status, "AMEND_REQUESTED", "case {}", case.id);
                assert!(resp.reason.is_none(), "case {}", case.id);
            } else {
                assert_eq!(resp.status, "REJECTED", "case {}", case.id);
                assert_eq!(
                    resp.reason.as_deref(),
                    case.expected.reason.as_deref(),
                    "case {} reason mismatch",
                    case.id
                );
            }

            let updated = state
                .sharded_store
                .find_by_id_with_account(&order_id, account_id)
                .expect("updated order");
            assert_eq!(
                updated.status.as_str(),
                case.expected.next_status,
                "case {} next status mismatch",
                case.id
            );
        }
    }

    #[tokio::test]
    async fn replace_handler_matches_amend_fixture_cases() {
        let fixture: AmendDecisionFixture =
            load_fixture("contracts/fixtures/amend_decision_v1.json");

        for case in fixture.cases {
            let state = build_test_state();
            let account_id = "4102";
            let order_id = format!("ord_replace_fixture_{}", case.id);
            put_order(
                &state,
                &order_id,
                account_id,
                Some(&format!("cid_replace_fixture_{}", case.id)),
                Some(&format!("idem_replace_fixture_{}", case.id)),
            );
            let from_status = fixture_order_status(&case.from_status);
            if from_status != OrderStatus::Accepted {
                let _ = state.sharded_store.update(&order_id, account_id, |prev| {
                    let mut next = prev.clone();
                    next.status = from_status;
                    next
                });
            }

            let req = AmendRequest {
                new_qty: case.input.new_qty,
                new_price: case.input.new_price,
                comment: case.input.comment.clone(),
            };
            let (status, Json(resp)) = handle_replace_order(
                State(state.clone()),
                headers(account_id, None),
                Path(order_id.clone()),
                Json(req),
            )
            .await
            .unwrap_or_else(|_| panic!("replace fixture case failed: {}", case.id));

            assert_eq!(
                status.as_u16(),
                case.expected.http_status,
                "case {} http status mismatch",
                case.id
            );
            if case.expected.allowed {
                assert_eq!(resp.status, "AMEND_REQUESTED", "case {}", case.id);
                assert!(resp.reason.is_none(), "case {}", case.id);
            } else {
                assert_eq!(resp.status, "REJECTED", "case {}", case.id);
                assert_eq!(
                    resp.reason.as_deref(),
                    case.expected.reason.as_deref(),
                    "case {} reason mismatch",
                    case.id
                );
            }

            let updated = state
                .sharded_store
                .find_by_id_with_account(&order_id, account_id)
                .expect("updated order");
            assert_eq!(
                updated.status.as_str(),
                case.expected.next_status,
                "case {} next status mismatch",
                case.id
            );
        }
    }

    #[tokio::test]
    async fn replace_alias_updates_active_order_to_amend_requested() {
        let state = build_test_state();
        let account_id = "4001";
        let order_id = "ord_replace_1";
        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_replace_1"),
            Some("idem_replace_1"),
        );

        let request = AmendRequest {
            new_qty: 77,
            new_price: 14900,
            comment: Some("replace alias".into()),
        };
        let (status, Json(resp)) = handle_replace_order(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.to_string()),
            Json(request),
        )
        .await
        .unwrap_or_else(|_| panic!("replace request failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "AMEND_REQUESTED");

        let updated = state
            .sharded_store
            .find_by_id_with_account(order_id, account_id)
            .expect("updated order");
        assert_eq!(updated.status, OrderStatus::AmendRequested);
        assert_eq!(updated.qty, 77);
        assert_eq!(updated.price, Some(14900));
    }

    #[tokio::test]
    async fn cancel_updates_active_order_to_cancel_requested() {
        let state = build_test_state();
        let account_id = "4201";
        let order_id = "ord_cancel_1";
        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_cancel_1"),
            Some("idem_cancel_1"),
        );

        let (status, Json(resp)) = handle_cancel_order(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("cancel request failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "CANCEL_REQUESTED");
        assert!(resp.reason.is_none());

        let updated = state
            .sharded_store
            .find_by_id_with_account(order_id, account_id)
            .expect("updated order");
        assert_eq!(updated.status, OrderStatus::CancelRequested);
    }

    #[tokio::test]
    async fn cancel_is_idempotent_for_cancel_requested_order() {
        let state = build_test_state();
        let account_id = "4202";
        let order_id = "ord_cancel_2";
        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_cancel_2"),
            Some("idem_cancel_2"),
        );
        let _ = state.sharded_store.update(order_id, account_id, |prev| {
            let mut next = prev.clone();
            next.status = OrderStatus::CancelRequested;
            next
        });

        let (status, Json(resp)) = handle_cancel_order(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("cancel request failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "CANCEL_REQUESTED");
        assert!(resp.reason.is_none());

        let updated = state
            .sharded_store
            .find_by_id_with_account(order_id, account_id)
            .expect("updated order");
        assert_eq!(updated.status, OrderStatus::CancelRequested);
    }

    #[tokio::test]
    async fn cancel_rejects_terminal_order_with_conflict() {
        let state = build_test_state();
        let account_id = "4203";
        let order_id = "ord_cancel_3";
        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_cancel_3"),
            Some("idem_cancel_3"),
        );
        let _ = state.sharded_store.update(order_id, account_id, |prev| {
            let mut next = prev.clone();
            next.status = OrderStatus::Filled;
            next
        });

        let (status, Json(resp)) = handle_cancel_order(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.to_string()),
        )
        .await
        .unwrap_or_else(|_| panic!("cancel request failed"));

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(resp.status, "REJECTED");
        assert_eq!(resp.reason.as_deref(), Some("ORDER_FINAL"));

        let updated = state
            .sharded_store
            .find_by_id_with_account(order_id, account_id)
            .expect("updated order");
        assert_eq!(updated.status, OrderStatus::Filled);
    }

    #[tokio::test]
    async fn amend_rejects_terminal_order_with_conflict() {
        let state = build_test_state();
        let account_id = "4002";
        let order_id = "ord_amend_2";
        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_amend_2"),
            Some("idem_amend_2"),
        );
        let _ = state.sharded_store.update(order_id, account_id, |prev| {
            let mut next = prev.clone();
            next.status = OrderStatus::Filled;
            next
        });

        let request = AmendRequest {
            new_qty: 200,
            new_price: 15200,
            comment: None,
        };
        let (status, Json(resp)) = handle_amend_order(
            State(state),
            headers(account_id, None),
            Path(order_id.to_string()),
            Json(request),
        )
        .await
        .unwrap_or_else(|_| panic!("amend request failed"));

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(resp.status, "REJECTED");
        assert_eq!(resp.reason.as_deref(), Some("ORDER_FINAL"));
    }

    #[tokio::test]
    async fn amend_rejects_cancel_requested_order_with_conflict() {
        let state = build_test_state();
        let account_id = "4004";
        let order_id = "ord_amend_4";
        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_amend_4"),
            Some("idem_amend_4"),
        );
        let _ = state.sharded_store.update(order_id, account_id, |prev| {
            let mut next = prev.clone();
            next.status = OrderStatus::CancelRequested;
            next
        });

        let request = AmendRequest {
            new_qty: 200,
            new_price: 15200,
            comment: None,
        };
        let (status, Json(resp)) = handle_amend_order(
            State(state),
            headers(account_id, None),
            Path(order_id.to_string()),
            Json(request),
        )
        .await
        .unwrap_or_else(|_| panic!("amend request failed"));

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(resp.status, "REJECTED");
        assert_eq!(resp.reason.as_deref(), Some("ORDER_FINAL"));
    }

    #[tokio::test]
    async fn amend_rejects_invalid_qty_and_price() {
        let state = build_test_state();
        let account_id = "4003";
        let order_id = "ord_amend_3";
        put_order(
            &state,
            order_id,
            account_id,
            Some("cid_amend_3"),
            Some("idem_amend_3"),
        );

        let bad_qty = AmendRequest {
            new_qty: 0,
            new_price: 15200,
            comment: None,
        };
        let (status1, Json(resp1)) = handle_amend_order(
            State(state.clone()),
            headers(account_id, None),
            Path(order_id.to_string()),
            Json(bad_qty),
        )
        .await
        .unwrap_or_else(|_| panic!("amend bad qty request failed"));
        assert_eq!(status1, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(resp1.reason.as_deref(), Some("INVALID_QTY"));

        let bad_price = AmendRequest {
            new_qty: 100,
            new_price: 0,
            comment: None,
        };
        let (status2, Json(resp2)) = handle_amend_order(
            State(state),
            headers(account_id, None),
            Path(order_id.to_string()),
            Json(bad_price),
        )
        .await
        .unwrap_or_else(|_| panic!("amend bad price request failed"));
        assert_eq!(status2, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(resp2.reason.as_deref(), Some("INVALID_PRICE"));
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

    #[test]
    fn v3_tcp_request_parser_accepts_fixed_frame() {
        let token = make_token("42");
        let frame = v3_tcp_request(&token, "AAPL", 1, 1, 100, 15_000);
        let decoded = decode_v3_tcp_request(&frame).expect("tcp parse");
        assert_eq!(decoded.jwt_token, Some(token.as_str()));
        assert_eq!(decoded.intent_id, None);
        assert_eq!(decoded.model_id, None);
        assert_eq!(
            decoded.symbol_key,
            parse_v3_symbol_key("AAPL").expect("symbol key")
        );
        assert_eq!(decoded.side, 1);
        assert_eq!(decoded.order_type, OrderType::Limit);
        assert_eq!(decoded.qty, 100);
        assert_eq!(decoded.price, 15_000);
    }

    #[test]
    fn v3_tcp_frame_parser_accepts_auth_init_frame() {
        let token = make_token("auth_init_1");
        let frame = v3_tcp_auth_init_frame(&token);
        match decode_v3_tcp_frame(&frame).expect("auth init parse") {
            V3TcpDecodedFrame::AuthInit { jwt_token } => assert_eq!(jwt_token, token),
            V3TcpDecodedFrame::Order(_) => panic!("expected auth init frame"),
        }
    }

    #[test]
    fn v3_tcp_request_parser_accepts_tokenless_frame() {
        let frame = v3_tcp_request("", "AAPL", 1, 1, 100, 15_000);
        let decoded = decode_v3_tcp_request(&frame).expect("tcp parse");
        assert_eq!(decoded.jwt_token, None);
        assert_eq!(decoded.intent_id, None);
        assert_eq!(decoded.model_id, None);
        assert_eq!(
            decoded.symbol_key,
            parse_v3_symbol_key("AAPL").expect("symbol key")
        );
        assert_eq!(decoded.side, 1);
        assert_eq!(decoded.order_type, OrderType::Limit);
        assert_eq!(decoded.qty, 100);
        assert_eq!(decoded.price, 15_000);
    }

    #[test]
    fn v3_tcp_request_parser_accepts_tokenless_metadata_frame() {
        let frame = v3_tcp_request_with_metadata(
            "",
            "AAPL",
            1,
            1,
            100,
            15_000,
            Some("intent-tcp-1"),
            Some("model-tcp-1"),
        );
        let decoded = decode_v3_tcp_request(&frame).expect("tcp parse");
        assert_eq!(decoded.jwt_token, None);
        assert_eq!(decoded.intent_id, Some("intent-tcp-1"));
        assert_eq!(decoded.model_id, Some("model-tcp-1"));
        assert_eq!(
            decoded.symbol_key,
            parse_v3_symbol_key("AAPL").expect("symbol key")
        );
    }

    #[test]
    fn v3_tcp_request_parser_rejects_inline_token_with_metadata() {
        let mut frame = v3_tcp_request(&make_token("tcp-metadata-bad"), "AAPL", 1, 1, 100, 15_000);
        frame[V3_TCP_INTENT_LEN_OFFSET..(V3_TCP_INTENT_LEN_OFFSET + 2)]
            .copy_from_slice(&(5u16).to_le_bytes());

        let reason = decode_v3_tcp_request(&frame).expect_err("must reject mixed token/metadata");
        assert_eq!(reason, V3_TCP_REASON_METADATA_WITH_INLINE_TOKEN);
    }

    #[tokio::test]
    async fn v3_tcp_authenticator_accepts_valid_token() {
        let state = build_test_state();
        let token = make_token("tcp-auth-1");
        let principal = authenticate_v3_tcp_token(&state, &token).expect("valid token");
        assert_eq!(principal.account_id, "tcp-auth-1");
        assert_eq!(principal.session_id, "tcp-auth-1");
    }

    #[test]
    fn v3_tcp_response_encoder_sets_accepted_fields() {
        let accepted = VolatileOrderResponse::accepted("42".into(), 7, 1234);
        let bytes = encode_v3_tcp_response(StatusCode::ACCEPTED, &accepted);
        assert_eq!(bytes.len(), V3_TCP_RESPONSE_SIZE);
        assert_eq!(bytes[0], V3_TCP_KIND_ACCEPT);
        assert_eq!(u16::from_le_bytes([bytes[2], bytes[3]]), 202);
        assert!(
            u64::from_le_bytes(bytes[8..16].try_into().expect("seq bytes")) > 0,
            "session seq must be set"
        );
    }

    #[tokio::test]
    async fn v3_tcp_metadata_flows_into_feedback_export() {
        let (mut state, ingress_rx, mut durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        let feedback_path =
            std::env::temp_dir().join(format!("gateway-rust-tcp-feedback-{}.jsonl", now_nanos()));
        state.quant_feedback_exporter = Arc::new(crate::strategy::sink::FeedbackExporter::new(
            crate::strategy::sink::FeedbackExportConfig {
                enabled: true,
                path: feedback_path.clone(),
                queue_capacity: 32,
                drop_policy: crate::strategy::sink::FeedbackDropPolicy::DropNewest,
            },
        ));

        let writer_handle = tokio::spawn(super::super::run_v3_single_writer(
            0,
            ingress_rx,
            state.clone(),
        ));
        let durable_handle = tokio::spawn(super::super::run_v3_durable_worker(
            0,
            durable_rxs.remove(0),
            state.clone(),
            state.v3_durable_worker_batch_max,
            state.v3_durable_worker_batch_wait_us,
            super::super::V3DurableWorkerBatchAdaptiveConfig {
                enabled: state.v3_durable_worker_batch_adaptive,
                batch_min: state.v3_durable_worker_batch_min,
                batch_max: state.v3_durable_worker_batch_max,
                wait_min: Duration::from_micros(state.v3_durable_worker_batch_wait_min_us.max(1)),
                wait_max: Duration::from_micros(state.v3_durable_worker_batch_wait_us.max(1)),
                low_util_pct: state.v3_durable_worker_batch_adaptive_low_util_pct,
                high_util_pct: state.v3_durable_worker_batch_adaptive_high_util_pct,
            },
            super::super::V3DurableWorkerPressureConfig::from_env(
                state.v3_durable_worker_inflight_hard_cap_pct,
            ),
        ));

        let frame = v3_tcp_request_with_metadata(
            "",
            "AAPL",
            1,
            1,
            100,
            15_000,
            Some("intent-tcp-1"),
            Some("model-tcp-1"),
        );
        let decoded = decode_v3_tcp_request(&frame).expect("decode tcp metadata frame");
        let resp =
            process_order_v3_hot_path_tcp(&state, "acc-tcp-1", "sess-tcp-1", &decoded, now_nanos());

        assert_eq!(resp[0], V3_TCP_KIND_ACCEPT);
        assert_eq!(u16::from_le_bytes([resp[2], resp[3]]), 202);

        for _ in 0..100 {
            if state.quant_feedback_exporter.metrics().written_total >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(state.quant_feedback_exporter.metrics().written_total >= 2);

        let raw = wait_for_feedback_lines(&feedback_path, 2);
        assert!(raw.contains("\"intentId\":\"intent-tcp-1\""));
        assert!(raw.contains("\"modelId\":\"model-tcp-1\""));
        assert!(raw.contains("\"pathTags\":[\"v3\",\"tcp_v3\",\"feedback\"]"));

        writer_handle.abort();
        durable_handle.abort();
    }

    #[tokio::test]
    async fn v3_rejects_soft_when_durable_backlog_crosses_soft_threshold() {
        let mut state = build_test_state();
        state.v3_durable_backlog_soft_reject_per_sec = 1_000;
        state.v3_durable_backlog_hard_reject_per_sec = 2_000;
        state
            .v3_durable_backlog_growth_per_sec
            .store(1_200, Ordering::Relaxed);
        if let Some(gauge) = state.v3_durable_backlog_growth_per_sec_per_lane.get(0) {
            gauge.store(1_200, Ordering::Relaxed);
        }

        let account_id = "v3-durable-soft-1";
        let req = request_with_client_id("cid_v3_durable_soft");
        let (status, Json(resp)) = handle_order_v3(
            State(state),
            headers(account_id, Some("idem_v3_durable_soft")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.reason.as_deref(), Some("V3_DURABLE_BACKPRESSURE_SOFT"));
    }

    #[tokio::test]
    async fn v3_rejects_hard_when_durable_backlog_crosses_hard_threshold() {
        let mut state = build_test_state();
        state.v3_durable_backlog_soft_reject_per_sec = 1_000;
        state.v3_durable_backlog_hard_reject_per_sec = 1_500;
        state
            .v3_durable_backlog_growth_per_sec
            .store(1_700, Ordering::Relaxed);
        if let Some(gauge) = state.v3_durable_backlog_growth_per_sec_per_lane.get(0) {
            gauge.store(1_700, Ordering::Relaxed);
        }

        let account_id = "v3-durable-hard-1";
        let req = request_with_client_id("cid_v3_durable_hard");
        let (status, Json(resp)) = handle_order_v3(
            State(state),
            headers(account_id, Some("idem_v3_durable_hard")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.reason.as_deref(), Some("V3_DURABLE_BACKPRESSURE_HARD"));
    }

    #[tokio::test]
    async fn v3_rejects_soft_when_durable_controller_level_is_soft() {
        let mut state = build_test_state();
        state.v3_durable_admission_controller_enabled = true;
        state.v3_durable_admission_level.store(1, Ordering::Relaxed);
        if let Some(level) = state.v3_durable_admission_level_per_lane.get(0) {
            level.store(1, Ordering::Relaxed);
        }

        let account_id = "v3-durable-controller-soft-1";
        let req = request_with_client_id("cid_v3_durable_controller_soft");
        let (status, Json(resp)) = handle_order_v3(
            State(state),
            headers(account_id, Some("idem_v3_durable_controller_soft")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.reason.as_deref(), Some("V3_DURABLE_CONTROLLER_SOFT"));
    }

    #[tokio::test]
    async fn v3_rejects_hard_when_durable_controller_level_is_hard() {
        let mut state = build_test_state();
        state.v3_durable_admission_controller_enabled = true;
        state.v3_durable_admission_level.store(2, Ordering::Relaxed);
        if let Some(level) = state.v3_durable_admission_level_per_lane.get(0) {
            level.store(2, Ordering::Relaxed);
        }

        let account_id = "v3-durable-controller-hard-1";
        let req = request_with_client_id("cid_v3_durable_controller_hard");
        let (status, Json(resp)) = handle_order_v3(
            State(state),
            headers(account_id, Some("idem_v3_durable_controller_hard")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.reason.as_deref(), Some("V3_DURABLE_CONTROLLER_HARD"));
    }

    #[tokio::test]
    async fn v3_rejects_soft_when_confirm_oldest_age_crosses_soft_threshold() {
        let mut state = build_test_state();
        state.v3_durable_confirm_soft_reject_age_us = 5_000;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state
            .v3_confirm_oldest_inflight_us
            .store(6_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(6_000, Ordering::Relaxed);
        }

        let account_id = "v3-confirm-age-soft-1";
        let req = request_with_client_id("cid_v3_confirm_age_soft");
        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_confirm_age_soft")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.reason.as_deref(), Some("V3_DURABLE_CONFIRM_AGE_SOFT"));
        assert_eq!(
            state
                .v3_durable_confirm_age_soft_reject_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn v3_rejects_hard_when_confirm_oldest_age_crosses_hard_threshold() {
        let mut state = build_test_state();
        state.v3_durable_confirm_soft_reject_age_us = 5_000;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state
            .v3_confirm_oldest_inflight_us
            .store(12_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(12_000, Ordering::Relaxed);
        }

        let account_id = "v3-confirm-age-hard-1";
        let req = request_with_client_id("cid_v3_confirm_age_hard");
        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_confirm_age_hard")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.reason.as_deref(), Some("V3_DURABLE_CONFIRM_AGE_HARD"));
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn v3_skips_hard_confirm_guard_when_requires_admission_and_lane_is_normal() {
        let (mut state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state.v3_durable_confirm_soft_reject_age_us = 5_000;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state.v3_durable_confirm_guard_soft_requires_admission = true;
        state.v3_durable_confirm_guard_hard_requires_admission = true;
        state
            .v3_confirm_oldest_inflight_us
            .store(12_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(12_000, Ordering::Relaxed);
        }
        if let Some(level) = state.v3_durable_admission_level_per_lane.get(0) {
            level.store(0, Ordering::Relaxed);
        }

        let account_id = "v3-confirm-age-hard-skip-normal";
        let req = request_with_client_id("cid_v3_confirm_age_hard_skip_normal");
        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_confirm_age_hard_skip_normal")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "VOLATILE_ACCEPT");
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_total
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_skipped_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn v3_skips_hard_confirm_guard_when_low_load_gate_is_not_satisfied() {
        let (mut state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state.v3_durable_confirm_soft_reject_age_us = 0;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state.v3_durable_confirm_guard_secondary_required = true;
        state.v3_durable_confirm_guard_min_queue_pct = 1.0;
        state.v3_durable_confirm_guard_min_inflight_pct = 80;
        state
            .v3_confirm_oldest_inflight_us
            .store(12_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(12_000, Ordering::Relaxed);
        }
        if let Some(gauge) = state.v3_durable_receipt_inflight_per_lane.get(0) {
            gauge.store(32, Ordering::Relaxed);
        }

        let account_id = "v3-confirm-age-hard-skip-low-load";
        let req = request_with_client_id("cid_v3_confirm_age_hard_skip_low_load");
        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_confirm_age_hard_skip_low_load")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "VOLATILE_ACCEPT");
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_total
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_skipped_low_load_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn v3_skips_hard_confirm_guard_when_not_armed() {
        let (mut state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state.v3_durable_confirm_soft_reject_age_us = 0;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state.v3_durable_confirm_guard_secondary_required = false;
        state
            .v3_confirm_oldest_inflight_us
            .store(12_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(12_000, Ordering::Relaxed);
        }
        if let Some(gauge) = state.v3_durable_confirm_guard_hard_armed_per_lane.get(0) {
            gauge.store(0, Ordering::Relaxed);
        }

        let account_id = "v3-confirm-age-hard-skip-unarmed";
        let req = request_with_client_id("cid_v3_confirm_age_hard_skip_unarmed");
        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_confirm_age_hard_skip_unarmed")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "VOLATILE_ACCEPT");
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_total
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_skipped_unarmed_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn v3_applies_hard_confirm_guard_when_requires_admission_and_lane_escalated() {
        let mut state = build_test_state();
        state.v3_durable_confirm_soft_reject_age_us = 5_000;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state.v3_durable_confirm_guard_hard_requires_admission = true;
        state.v3_durable_confirm_guard_min_inflight_pct = 100;
        state
            .v3_confirm_oldest_inflight_us
            .store(12_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(12_000, Ordering::Relaxed);
        }
        if let Some(level) = state.v3_durable_admission_level_per_lane.get(0) {
            level.store(1, Ordering::Relaxed);
        }

        let account_id = "v3-confirm-age-hard-escalated";
        let req = request_with_client_id("cid_v3_confirm_age_hard_escalated");
        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_confirm_age_hard_escalated")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.reason.as_deref(), Some("V3_DURABLE_CONFIRM_AGE_HARD"));
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_skipped_total
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn v3_does_not_soft_reject_when_only_global_confirm_age_is_high() {
        let (mut state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state.v3_durable_confirm_soft_reject_age_us = 5_000;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state
            .v3_confirm_oldest_inflight_us
            .store(6_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(1_000, Ordering::Relaxed);
        }

        let account_id = "v3-confirm-age-global-only-soft";
        let req = request_with_client_id("cid_v3_confirm_global_only_soft");
        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_confirm_global_only_soft")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "VOLATILE_ACCEPT");
        assert_eq!(
            state
                .v3_durable_confirm_age_soft_reject_total
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            state
                .v3_durable_confirm_age_hard_reject_total
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn v3_does_not_reject_when_only_global_durable_controller_level_is_set() {
        let (mut state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state.v3_durable_admission_controller_enabled = true;
        state.v3_durable_admission_level.store(2, Ordering::Relaxed);
        if let Some(level) = state.v3_durable_admission_level_per_lane.get(0) {
            level.store(0, Ordering::Relaxed);
        }

        let account_id = "v3-durable-controller-global-only";
        let req = request_with_client_id("cid_v3_durable_controller_global_only");
        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_durable_controller_global_only")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.status, "VOLATILE_ACCEPT");
        assert_eq!(
            state
                .v3_durable_backpressure_soft_total
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            state
                .v3_durable_backpressure_hard_total
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn v3_integration_pipeline_promotes_to_durable_accepted() {
        let (state, ingress_rx, mut durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        let durable_rx = durable_rxs.pop().expect("durable lane rx");
        let writer_handle = tokio::spawn(super::super::run_v3_single_writer(
            0,
            ingress_rx,
            state.clone(),
        ));
        let durable_handle = tokio::spawn(super::super::run_v3_durable_worker(
            0,
            durable_rx,
            state.clone(),
            state.v3_durable_worker_batch_max,
            state.v3_durable_worker_batch_wait_us,
            super::super::V3DurableWorkerBatchAdaptiveConfig {
                enabled: state.v3_durable_worker_batch_adaptive,
                batch_min: state.v3_durable_worker_batch_min,
                batch_max: state.v3_durable_worker_batch_max,
                wait_min: Duration::from_micros(state.v3_durable_worker_batch_wait_min_us.max(1)),
                wait_max: Duration::from_micros(state.v3_durable_worker_batch_wait_us.max(1)),
                low_util_pct: state.v3_durable_worker_batch_adaptive_low_util_pct,
                high_util_pct: state.v3_durable_worker_batch_adaptive_high_util_pct,
            },
            super::super::V3DurableWorkerPressureConfig::from_env(
                state.v3_durable_worker_inflight_hard_cap_pct,
            ),
        ));

        let account_id = "v3-int-acc-1";
        let req = request_with_client_id("cid_v3_integration_ok");
        let (status, Json(accepted)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_integration_ok")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(accepted.status, "VOLATILE_ACCEPT");
        let session_seq = accepted.session_seq.expect("session seq");

        let mut durable_seen = false;
        for _ in 0..100 {
            let Json(status_resp) = handle_get_order_v3(
                State(state.clone()),
                headers(account_id, None),
                Path((account_id.to_string(), session_seq)),
            )
            .await
            .unwrap_or_else(|_| panic!("v3 durable lookup failed"));
            if status_resp.status == "DURABLE_ACCEPTED" {
                durable_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(durable_seen, "expected eventual DURABLE_ACCEPTED");
        assert_eq!(state.v3_accepted_total_current(), 1);
        assert_eq!(state.v3_durable_accepted_total_current(), 1);

        let metrics = super::super::metrics::handle_metrics(State(state.clone())).await;
        assert!(metrics.contains("gateway_v3_confirm_store_size "));
        assert!(metrics.contains("gateway_v3_confirm_lane_skew_pct "));

        writer_handle.abort();
        durable_handle.abort();
    }

    #[tokio::test]
    async fn v3_integration_loss_monitor_updates_scan_cost_and_gc() {
        let (state, ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(20, 40, 10, 1, 1_024);
        let writer_handle = tokio::spawn(super::super::run_v3_single_writer(
            0,
            ingress_rx,
            state.clone(),
        ));
        let monitor_handle = tokio::spawn(super::super::run_v3_loss_monitor(state.clone()));

        let account_id = "v3-int-acc-2";
        let req = request_with_client_id("cid_v3_integration_timeout");
        let (status, Json(accepted)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_integration_timeout")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));
        assert_eq!(status, StatusCode::ACCEPTED);
        let session_seq = accepted.session_seq.expect("session seq");

        let mut loss_seen = false;
        for _ in 0..120 {
            let Json(status_resp) = handle_get_order_v3(
                State(state.clone()),
                headers(account_id, None),
                Path((account_id.to_string(), session_seq)),
            )
            .await
            .unwrap_or_else(|_| panic!("v3 status lookup failed"));
            if status_resp.status == "LOSS_SUSPECT" {
                loss_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(loss_seen, "expected LOSS_SUSPECT after durable timeout");
        assert!(
            state
                .v3_confirm_timeout_scan_cost_total
                .load(Ordering::Relaxed)
                > 0
        );

        let mut gc_seen = false;
        for _ in 0..120 {
            if state.v3_confirm_gc_removed_total.load(Ordering::Relaxed) > 0 {
                gc_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(gc_seen, "expected confirm GC removal");

        let metrics = super::super::metrics::handle_metrics(State(state.clone())).await;
        assert!(metrics.contains("gateway_v3_confirm_timeout_scan_cost_total "));
        assert!(metrics.contains("gateway_v3_confirm_gc_removed_total "));

        writer_handle.abort();
        monitor_handle.abort();
    }

    #[tokio::test]
    async fn v3_integration_durable_queue_full_marks_loss_suspect() {
        let (state, ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(5_000, 60_000, 20, 1, 1);
        let writer_handle = tokio::spawn(super::super::run_v3_single_writer(
            0,
            ingress_rx,
            state.clone(),
        ));

        let account_id = "v3-int-acc-full";
        let (s1, Json(r1)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("cid_v3_qfull_1")),
            Json(request_with_client_id("cid_v3_qfull_1")),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));
        let (s2, Json(r2)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("cid_v3_qfull_2")),
            Json(request_with_client_id("cid_v3_qfull_2")),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));
        assert_eq!(s1, StatusCode::ACCEPTED);
        assert_eq!(s2, StatusCode::ACCEPTED);
        assert_eq!(r1.status, "VOLATILE_ACCEPT");
        assert_eq!(r2.status, "VOLATILE_ACCEPT");
        let seq2 = r2.session_seq.expect("second seq");

        let mut loss_seen = false;
        for _ in 0..120 {
            let Json(status_resp) = handle_get_order_v3(
                State(state.clone()),
                headers(account_id, None),
                Path((account_id.to_string(), seq2)),
            )
            .await
            .unwrap_or_else(|_| panic!("v3 status lookup failed"));
            if status_resp.status == "LOSS_SUSPECT" {
                loss_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            loss_seen,
            "expected LOSS_SUSPECT when durable queue is full"
        );
        assert!(state.v3_durable_ingress.queue_full_total() > 0);

        writer_handle.abort();
    }

    #[tokio::test]
    async fn v3_integration_durable_queue_closed_marks_loss_suspect() {
        let (state, ingress_rx, durable_rxs) =
            build_test_state_with_v3_pipeline(5_000, 60_000, 20, 1, 8);
        drop(durable_rxs);
        let writer_handle = tokio::spawn(super::super::run_v3_single_writer(
            0,
            ingress_rx,
            state.clone(),
        ));

        let account_id = "v3-int-acc-closed";
        let (status, Json(accepted)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_qclosed_1")),
            Json(request_with_client_id("cid_v3_qclosed_1")),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));
        assert_eq!(status, StatusCode::ACCEPTED);
        let seq = accepted.session_seq.expect("session seq");

        let mut loss_seen = false;
        for _ in 0..120 {
            let Json(status_resp) = handle_get_order_v3(
                State(state.clone()),
                headers(account_id, None),
                Path((account_id.to_string(), seq)),
            )
            .await
            .unwrap_or_else(|_| panic!("v3 status lookup failed"));
            if status_resp.status == "LOSS_SUSPECT" {
                loss_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            loss_seen,
            "expected LOSS_SUSPECT when durable queue is closed"
        );
        assert!(state.v3_durable_ingress.queue_closed_total() > 0);

        writer_handle.abort();
    }

    #[tokio::test]
    async fn strategy_config_endpoint_updates_and_rejects_stale_versions() {
        let state = build_test_state();

        let Json(initial) =
            super::super::strategy::handle_get_strategy_config(State(state.clone())).await;
        assert_eq!(initial.snapshot_id, "default");
        assert_eq!(initial.version, 0);

        let snapshot = ExecutionConfigSnapshot {
            snapshot_id: "snapshot-1".to_string(),
            version: 3,
            applied_at_ns: 123,
            shadow_enabled: true,
            ..ExecutionConfigSnapshot::default()
        };
        let Json(updated) = super::super::strategy::handle_put_strategy_config(
            State(state.clone()),
            Json(snapshot.clone()),
        )
        .await
        .expect("update snapshot");
        assert_eq!(updated.snapshot_id, "snapshot-1");
        assert_eq!(updated.previous_version, 0);

        let Json(current) =
            super::super::strategy::handle_get_strategy_config(State(state.clone())).await;
        assert_eq!(current.snapshot_id, "snapshot-1");
        assert_eq!(current.version, 3);
        assert!(current.shadow_enabled);

        let stale = ExecutionConfigSnapshot {
            snapshot_id: "snapshot-0".to_string(),
            version: 2,
            applied_at_ns: 222,
            ..ExecutionConfigSnapshot::default()
        };
        let err =
            super::super::strategy::handle_put_strategy_config(State(state.clone()), Json(stale))
                .await
                .expect_err("stale snapshot must fail");
        assert_eq!(err.0, StatusCode::CONFLICT);
        assert_eq!(err.1.0.reason, "SNAPSHOT_VERSION_STALE");
        assert_eq!(err.1.0.current_version, 3);
    }

    #[tokio::test]
    async fn strategy_shadow_endpoint_round_trips_record() {
        let state = build_test_state();
        let record = ShadowRecord {
            schema_version: SHADOW_RECORD_SCHEMA_VERSION,
            shadow_run_id: "shadow-1".to_string(),
            model_id: "model-1".to_string(),
            intent_id: "intent-1".to_string(),
            session_id: "sess-1".to_string(),
            session_seq: 7,
            predicted_policy: None,
            actual_policy: None,
            predicted_outcome: None,
            actual_outcome: None,
            score_components: Vec::new(),
            evaluated_at_ns: 100,
            comparison_status: ShadowComparisonStatus::Pending,
        };

        let Json(upserted) = super::super::strategy::handle_post_shadow_record(
            State(state.clone()),
            Json(record.clone()),
        )
        .await
        .expect("upsert shadow");
        assert_eq!(upserted.shadow_run_id, "shadow-1");
        assert_eq!(upserted.intent_id, "intent-1");
        assert!(!upserted.replaced);

        let Json(fetched) = super::super::strategy::handle_get_shadow_record(
            State(state.clone()),
            Path(("shadow-1".to_string(), "intent-1".to_string())),
        )
        .await
        .expect("fetch shadow");
        assert_eq!(fetched.model_id, "model-1");
        assert_eq!(fetched.session_seq, 7);
    }

    #[tokio::test]
    async fn strategy_shadow_summary_endpoint_reports_run_rollup() {
        let state = build_test_state();
        state
            .strategy_shadow_store
            .upsert(ShadowRecord {
                schema_version: SHADOW_RECORD_SCHEMA_VERSION,
                shadow_run_id: "shadow-2".to_string(),
                model_id: "model-1".to_string(),
                intent_id: "intent-1".to_string(),
                session_id: "sess-1".to_string(),
                session_seq: 1,
                predicted_policy: None,
                actual_policy: None,
                predicted_outcome: None,
                actual_outcome: None,
                score_components: vec![ShadowScoreComponent {
                    name: "score-a".to_string(),
                    score_bps: 50,
                    detail: None,
                }],
                evaluated_at_ns: 100,
                comparison_status: ShadowComparisonStatus::Matched,
            })
            .expect("upsert matched shadow");
        state
            .strategy_shadow_store
            .upsert(ShadowRecord {
                schema_version: SHADOW_RECORD_SCHEMA_VERSION,
                shadow_run_id: "shadow-2".to_string(),
                model_id: "model-2".to_string(),
                intent_id: "intent-2".to_string(),
                session_id: "sess-2".to_string(),
                session_seq: 2,
                predicted_policy: None,
                actual_policy: None,
                predicted_outcome: None,
                actual_outcome: None,
                score_components: vec![ShadowScoreComponent {
                    name: "score-b".to_string(),
                    score_bps: -10,
                    detail: None,
                }],
                evaluated_at_ns: 200,
                comparison_status: ShadowComparisonStatus::Pending,
            })
            .expect("upsert pending shadow");

        let Json(summary) = super::super::strategy::handle_get_shadow_run_summary(
            State(state.clone()),
            Path("shadow-2".to_string()),
        )
        .await
        .expect("fetch shadow summary");

        assert_eq!(summary.shadow_run_id, "shadow-2");
        assert_eq!(summary.record_count, 2);
        assert_eq!(summary.pending_count, 1);
        assert_eq!(summary.matched_count, 1);
        assert_eq!(summary.negative_score_count, 1);
        assert_eq!(summary.positive_score_count, 1);
        assert_eq!(summary.total_score_bps, 40);
        assert_eq!(summary.average_score_bps, 20.0);
        assert_eq!(summary.min_score_bps, -10);
        assert_eq!(summary.max_score_bps, 50);
        assert_eq!(summary.last_evaluated_at_ns, 200);
        assert_eq!(summary.top_positive.len(), 1);
        assert_eq!(summary.top_positive[0].intent_id, "intent-1");
        assert_eq!(summary.top_negative.len(), 1);
        assert_eq!(summary.top_negative[0].intent_id, "intent-2");
    }

    #[tokio::test]
    async fn strategy_intent_adapter_returns_adapted_order_and_effective_policy() {
        let state = build_test_state();
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-adapt".to_string(),
                version: 7,
                applied_at_ns: now_nanos(),
                default_execution_policy: ExecutionPolicyConfig {
                    policy: ExecutionPolicyKind::Passive,
                    prefer_passive: true,
                    post_only: false,
                    max_slippage_bps: Some(9),
                    participation_rate_bps: Some(1200),
                },
                symbol_limits: vec![SymbolExecutionOverride {
                    symbol: "AAPL".to_string(),
                    execution_policy: Some(ExecutionPolicyConfig {
                        policy: ExecutionPolicyKind::Passive,
                        prefer_passive: false,
                        post_only: true,
                        max_slippage_bps: Some(4),
                        participation_rate_bps: Some(800),
                    }),
                    urgency_override: Some(IntentUrgency::High),
                    max_order_qty: Some(200),
                    max_notional: Some(2_000_000),
                }],
                risk_budget_by_account: vec![AccountRiskBudget {
                    account_id: "acc-1".to_string(),
                    budget_ref: Some("budget-1".to_string()),
                    max_notional: Some(3_000_000),
                    max_abs_position_qty: None,
                    order_rate_limit_per_sec: None,
                }],
                urgency_overrides: vec![UrgencyOverride {
                    account_id: "acc-1".to_string(),
                    urgency: IntentUrgency::Critical,
                }],
                venue_preference: vec![VenuePreference {
                    symbol: "AAPL".to_string(),
                    venue: "NASDAQ".to_string(),
                    rank: 1,
                }],
                shadow_enabled: true,
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let Json(resp) = super::super::strategy::handle_post_strategy_intent_adapt(
            State(state.clone()),
            Json(strategy_intent_fixture()),
        )
        .await
        .expect("adapt intent");

        assert_eq!(resp.snapshot_id, "snapshot-adapt");
        assert_eq!(resp.version, 7);
        assert_eq!(resp.order_request.symbol, "AAPL");
        assert_eq!(resp.order_request.side, "BUY");
        assert_eq!(resp.order_request.intent_id.as_deref(), Some("intent-1"));
        assert_eq!(resp.order_request.model_id.as_deref(), Some("model-1"));
        assert_eq!(resp.order_request.expire_at, Some(123));
        assert!(resp.policy_adjustments.is_empty());
        assert_eq!(resp.effective_risk_budget_ref.as_deref(), Some("budget-42"));
        assert!(resp.shadow_enabled);
        let policy = resp
            .effective_policy
            .execution_policy
            .expect("effective policy");
        assert_eq!(policy.policy, ExecutionPolicyKind::Aggressive);
        assert!(!policy.prefer_passive);
        assert!(policy.post_only);
        assert_eq!(resp.effective_policy.urgency.as_deref(), Some("CRITICAL"));
        assert_eq!(resp.effective_policy.venue.as_deref(), Some("NASDAQ"));
    }

    #[tokio::test]
    async fn strategy_intent_adapter_rejects_stale_snapshot() {
        let state = build_test_state();
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-stale".to_string(),
                version: 5,
                applied_at_ns: 1,
                kill_switch_policy: KillSwitchPolicy {
                    reject_when_snapshot_stale: true,
                    reject_when_shadow_stale: false,
                    snapshot_stale_after_ns: 1,
                },
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let err = super::super::strategy::handle_post_strategy_intent_adapt(
            State(state.clone()),
            Json(strategy_intent_fixture()),
        )
        .await
        .expect_err("stale snapshot must fail");

        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(err.1.0.reason, "STRATEGY_SNAPSHOT_STALE");
        assert_eq!(err.1.0.current_snapshot_id, "snapshot-stale");
    }

    #[tokio::test]
    async fn strategy_intent_adapter_normalizes_passive_ioc_to_gtc() {
        let state = build_test_state();
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-passive".to_string(),
                version: 6,
                applied_at_ns: now_nanos(),
                default_execution_policy: ExecutionPolicyConfig {
                    policy: ExecutionPolicyKind::Passive,
                    prefer_passive: true,
                    post_only: false,
                    max_slippage_bps: Some(6),
                    participation_rate_bps: Some(900),
                },
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let mut intent = strategy_intent_fixture();
        intent.execution_policy = ExecutionPolicyKind::Passive;
        intent.time_in_force = TimeInForce::Ioc;

        let Json(resp) = super::super::strategy::handle_post_strategy_intent_adapt(
            State(state.clone()),
            Json(intent),
        )
        .await
        .expect("adapt passive intent");

        assert_eq!(resp.order_request.time_in_force, TimeInForce::Gtc);
        assert_eq!(resp.order_request.expire_at, None);
        assert_eq!(resp.policy_adjustments, vec!["PASSIVE_NORMALIZED_TO_GTC"]);
        assert_eq!(
            resp.effective_policy
                .execution_policy
                .as_ref()
                .map(|policy| policy.policy),
            Some(ExecutionPolicyKind::Passive)
        );
    }

    #[tokio::test]
    async fn strategy_intent_adapter_returns_twap_algo_plan() {
        let state = build_test_state();
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-algo".to_string(),
                version: 10,
                applied_at_ns: now_nanos(),
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let Json(resp) = super::super::strategy::handle_post_strategy_intent_adapt(
            State(state.clone()),
            Json(twap_strategy_intent_fixture(1_000)),
        )
        .await
        .expect("algo adapt should succeed");

        let algo_plan = resp.algo_plan.expect("algo plan");
        assert_eq!(algo_plan.policy, ExecutionPolicyKind::Twap);
        assert_eq!(algo_plan.child_count, 4);
        assert_eq!(
            algo_plan
                .slices
                .iter()
                .map(|slice| slice.qty)
                .collect::<Vec<_>>(),
            vec![25, 25, 25, 25]
        );
    }

    #[tokio::test]
    async fn strategy_intent_submit_schedules_twap_algo_runtime_and_updates_shadow() {
        let (state, ingress_rx, mut durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-algo-runtime".to_string(),
                version: 11,
                applied_at_ns: now_nanos(),
                shadow_enabled: true,
                venue_preference: vec![VenuePreference {
                    symbol: "AAPL".to_string(),
                    venue: "NASDAQ".to_string(),
                    rank: 1,
                }],
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let writer_handle = tokio::spawn(super::super::run_v3_single_writer(
            0,
            ingress_rx,
            state.clone(),
        ));
        let durable_handle = tokio::spawn(super::super::run_v3_durable_worker(
            0,
            durable_rxs.remove(0),
            state.clone(),
            state.v3_durable_worker_batch_max,
            state.v3_durable_worker_batch_wait_us,
            super::super::V3DurableWorkerBatchAdaptiveConfig {
                enabled: state.v3_durable_worker_batch_adaptive,
                batch_min: state.v3_durable_worker_batch_min,
                batch_max: state.v3_durable_worker_batch_max,
                wait_min: Duration::from_micros(state.v3_durable_worker_batch_wait_min_us.max(1)),
                wait_max: Duration::from_micros(state.v3_durable_worker_batch_wait_us.max(1)),
                low_util_pct: state.v3_durable_worker_batch_adaptive_low_util_pct,
                high_util_pct: state.v3_durable_worker_batch_adaptive_high_util_pct,
            },
            super::super::V3DurableWorkerPressureConfig::from_env(
                state.v3_durable_worker_inflight_hard_cap_pct,
            ),
        ));

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent: twap_strategy_intent_fixture(now_nanos()),
                shadow_run_id: Some("shadow-algo-1".to_string()),
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("algo runtime submit should succeed");

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.volatile_order.status, "ALGO_RUNTIME_SCHEDULED");
        assert!(resp.algo_plan.is_some());
        assert!(resp.algo_runtime.is_some());

        let mut completed = false;
        for _ in 0..100 {
            let runtime = state
                .strategy_runtime_store
                .get("intent-1")
                .expect("algo runtime state");
            if runtime.status == AlgoParentStatus::Completed
                && runtime.durable_accepted_child_count() == 4
            {
                completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(completed, "expected algo runtime to complete");

        let Json(runtime_resp) = super::super::strategy::handle_get_strategy_runtime(
            State(state.clone()),
            Path("intent-1".to_string()),
        )
        .await
        .expect("runtime endpoint");
        assert_eq!(runtime_resp.status, AlgoParentStatus::Completed);
        assert!(
            runtime_resp
                .slices
                .iter()
                .all(|slice| slice.session_seq.is_some())
        );

        let shadow = state
            .strategy_shadow_store
            .get("shadow-algo-1", "intent-1")
            .expect("shadow record");
        assert_eq!(
            shadow
                .actual_outcome
                .as_ref()
                .and_then(|outcome| outcome.final_status.as_deref()),
            Some("DURABLE_ACCEPTED")
        );
        assert_eq!(shadow.comparison_status, ShadowComparisonStatus::Matched);

        writer_handle.abort();
        durable_handle.abort();
    }

    #[tokio::test]
    async fn strategy_runtime_rebuild_restores_snapshot_and_replays_durable_child() {
        let wal_path =
            std::env::temp_dir().join(format!("gateway-rust-strategy-replay-{}.log", now_nanos()));
        let mut runtime = replay_runtime_fixture(1, 1_000);
        runtime.status = AlgoParentStatus::Running;
        runtime.accepted_at_ns = Some(900);
        runtime.last_updated_at_ns = 900;
        runtime.slices[0].status = AlgoChildStatus::VolatileAccepted;
        runtime.slices[0].session_seq = Some(7);
        runtime.slices[0].received_at_ns = Some(900);

        let events = vec![
            strategy_runtime_snapshot_line(&runtime, 1),
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableAccepted".to_string(),
                at: 2,
                account_id: runtime.account_id.clone(),
                order_id: Some("v3/sess-1/7".to_string()),
                data: json!({
                    "intentId": runtime.slices[0].child_intent_id,
                    "positionSymbolKey": 0,
                    "positionDeltaQty": 0,
                    "shardId": 0,
                }),
            })
            .expect("serialize durable event"),
        ]
        .join("\n");
        fs::write(&wal_path, format!("{events}\n")).expect("write wal");

        let audit_log = Arc::new(AuditLog::new(&wal_path).expect("create audit log"));
        let state = build_test_state_with_audit_log(audit_log);

        let stats = super::super::rebuild_strategy_runtime_from_wal(&state, 1024);
        assert_eq!(stats.parent_restored, 1);
        assert_eq!(stats.durable_child_replayed, 1);
        assert_eq!(stats.resumed_children, 0);

        let restored = state
            .strategy_runtime_store
            .get("intent-1")
            .expect("restored runtime");
        assert_eq!(restored.status, AlgoParentStatus::Completed);
        assert_eq!(restored.slices[0].status, AlgoChildStatus::DurableAccepted);
        assert_eq!(restored.slices[0].durable_at_ns, Some(2_000_000));
    }

    #[tokio::test]
    async fn strategy_runtime_rebuild_resumes_scheduled_children() {
        let wal_path =
            std::env::temp_dir().join(format!("gateway-rust-strategy-resume-{}.log", now_nanos()));
        let runtime = replay_runtime_fixture(1, 0);
        fs::write(
            &wal_path,
            format!("{}\n", strategy_runtime_snapshot_line(&runtime, 1)),
        )
        .expect("write wal");

        let audit_log = Arc::new(AuditLog::new(&wal_path).expect("create audit log"));
        let (state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline_and_audit_log(audit_log, 500, 60_000, 20, 1, 1_024);
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-replay".to_string(),
                version: 1,
                applied_at_ns: now_nanos(),
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let stats = super::super::rebuild_strategy_runtime_from_wal(&state, 1024);
        assert_eq!(stats.parent_restored, 1);
        assert_eq!(stats.resumed_children, 1);

        let mut resumed = false;
        for _ in 0..100 {
            let runtime = state
                .strategy_runtime_store
                .get("intent-1")
                .expect("runtime after resume");
            if runtime.slices[0].status != AlgoChildStatus::Scheduled {
                resumed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(resumed, "expected scheduled child to resume");

        let runtime = state
            .strategy_runtime_store
            .get("intent-1")
            .expect("runtime after resume");
        assert!(
            matches!(
                runtime.slices[0].status,
                AlgoChildStatus::Dispatching | AlgoChildStatus::VolatileAccepted
            ),
            "unexpected child status: {:?}",
            runtime.slices[0].status
        );
    }

    #[tokio::test]
    async fn strategy_runtime_rebuild_pauses_no_auto_resume_parent() {
        let wal_path =
            std::env::temp_dir().join(format!("gateway-rust-strategy-pause-{}.log", now_nanos()));
        let mut runtime = replay_runtime_fixture(1, 0);
        runtime.recovery_policy = StrategyRecoveryPolicy::NoAutoResume;
        fs::write(
            &wal_path,
            format!("{}\n", strategy_runtime_snapshot_line(&runtime, 1)),
        )
        .expect("write wal");

        let audit_log = Arc::new(AuditLog::new(&wal_path).expect("create audit log"));
        let (state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline_and_audit_log(audit_log, 500, 60_000, 20, 1, 1_024);

        let stats = super::super::rebuild_strategy_runtime_from_wal(&state, 1024);
        assert_eq!(stats.parent_restored, 1);
        assert_eq!(stats.resumed_children, 0);

        let paused = state
            .strategy_runtime_store
            .get("intent-1")
            .expect("paused runtime");
        assert_eq!(paused.status, AlgoParentStatus::Paused);
        assert_eq!(
            paused.final_reason.as_deref(),
            Some("STRATEGY_NO_AUTO_RESUME_ON_RESTART")
        );
        assert_eq!(paused.slices[0].status, AlgoChildStatus::Scheduled);
    }

    #[tokio::test]
    async fn strategy_intent_submit_normal_urgency_soft_rejects_under_queue_pressure() {
        let (state, _ingress_rx) = build_test_state_with_soft_queue_pressure(10, 6, 60, 90, 95);
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::Normal;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("strategy submit returns volatile response");

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.volatile_order.status, "REJECTED");
        assert_eq!(
            resp.volatile_order
                .reason
                .as_ref()
                .map(|value| value.as_str()),
            Some("V3_BACKPRESSURE_SOFT")
        );
    }

    #[tokio::test]
    async fn strategy_intent_submit_high_urgency_bypasses_queue_soft_reject() {
        let (state, _ingress_rx) = build_test_state_with_soft_queue_pressure(10, 6, 60, 90, 95);
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::High;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("high urgency should bypass soft reject");

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.volatile_order.status, "VOLATILE_ACCEPT");
        assert!(resp.volatile_order.session_seq.is_some());
        assert_eq!(resp.effective_policy.urgency.as_deref(), Some("HIGH"));
    }

    #[tokio::test]
    async fn strategy_intent_submit_high_urgency_does_not_bypass_hard_reject() {
        let (state, _ingress_rx) = build_test_state_with_soft_queue_pressure(10, 9, 60, 90, 95);
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::High;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("hard reject still returns volatile response");

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.volatile_order.status, "REJECTED");
        assert_eq!(
            resp.volatile_order
                .reason
                .as_ref()
                .map(|value| value.as_str()),
            Some("V3_BACKPRESSURE_HARD")
        );
        assert_eq!(resp.effective_policy.urgency.as_deref(), Some("HIGH"));
    }

    #[tokio::test]
    async fn strategy_intent_submit_normal_urgency_rejects_durable_controller_soft() {
        let mut state = build_test_state();
        state.v3_durable_admission_controller_enabled = true;
        state.v3_durable_admission_level.store(1, Ordering::Relaxed);
        if let Some(level) = state.v3_durable_admission_level_per_lane.get(0) {
            level.store(1, Ordering::Relaxed);
        }
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::Normal;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("strategy submit returns durable controller soft response");

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.volatile_order.status, "REJECTED");
        assert_eq!(
            resp.volatile_order
                .reason
                .as_ref()
                .map(|value| value.as_str()),
            Some("V3_DURABLE_CONTROLLER_SOFT")
        );
    }

    #[tokio::test]
    async fn strategy_intent_submit_high_urgency_bypasses_durable_controller_soft() {
        let (mut state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state.v3_durable_admission_controller_enabled = true;
        state.v3_durable_admission_level.store(1, Ordering::Relaxed);
        if let Some(level) = state.v3_durable_admission_level_per_lane.get(0) {
            level.store(1, Ordering::Relaxed);
        }
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::High;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("high urgency should bypass durable controller soft");

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.volatile_order.status, "VOLATILE_ACCEPT");
        assert!(resp.volatile_order.session_seq.is_some());
        assert_eq!(resp.effective_policy.urgency.as_deref(), Some("HIGH"));
    }

    #[tokio::test]
    async fn strategy_intent_submit_high_urgency_does_not_bypass_durable_controller_hard() {
        let mut state = build_test_state();
        state.v3_durable_admission_controller_enabled = true;
        state.v3_durable_admission_level.store(2, Ordering::Relaxed);
        if let Some(level) = state.v3_durable_admission_level_per_lane.get(0) {
            level.store(2, Ordering::Relaxed);
        }
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::High;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("hard reject still returns durable controller response");

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.volatile_order.status, "REJECTED");
        assert_eq!(
            resp.volatile_order
                .reason
                .as_ref()
                .map(|value| value.as_str()),
            Some("V3_DURABLE_CONTROLLER_HARD")
        );
        assert_eq!(resp.effective_policy.urgency.as_deref(), Some("HIGH"));
    }

    #[tokio::test]
    async fn strategy_intent_submit_normal_urgency_rejects_durable_backpressure_soft() {
        let mut state = build_test_state();
        state.v3_durable_backlog_soft_reject_per_sec = 1_000;
        state.v3_durable_backlog_hard_reject_per_sec = 2_000;
        state
            .v3_durable_backlog_growth_per_sec
            .store(1_200, Ordering::Relaxed);
        if let Some(gauge) = state.v3_durable_backlog_growth_per_sec_per_lane.get(0) {
            gauge.store(1_200, Ordering::Relaxed);
        }
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::Normal;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("strategy submit returns durable backlog soft response");

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.volatile_order.status, "REJECTED");
        assert_eq!(
            resp.volatile_order
                .reason
                .as_ref()
                .map(|value| value.as_str()),
            Some("V3_DURABLE_BACKPRESSURE_SOFT")
        );
    }

    #[tokio::test]
    async fn strategy_intent_submit_high_urgency_bypasses_durable_backpressure_soft() {
        let (mut state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state.v3_durable_backlog_soft_reject_per_sec = 1_000;
        state.v3_durable_backlog_hard_reject_per_sec = 2_000;
        state
            .v3_durable_backlog_growth_per_sec
            .store(1_200, Ordering::Relaxed);
        if let Some(gauge) = state.v3_durable_backlog_growth_per_sec_per_lane.get(0) {
            gauge.store(1_200, Ordering::Relaxed);
        }
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::High;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("high urgency should bypass durable backlog soft");

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.volatile_order.status, "VOLATILE_ACCEPT");
        assert!(resp.volatile_order.session_seq.is_some());
        assert_eq!(resp.effective_policy.urgency.as_deref(), Some("HIGH"));
    }

    #[tokio::test]
    async fn strategy_intent_submit_normal_urgency_rejects_durable_confirm_age_soft() {
        let mut state = build_test_state();
        state.v3_durable_confirm_soft_reject_age_us = 5_000;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state
            .v3_confirm_oldest_inflight_us
            .store(6_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(6_000, Ordering::Relaxed);
        }
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::Normal;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("strategy submit returns durable confirm age soft response");

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.volatile_order.status, "REJECTED");
        assert_eq!(
            resp.volatile_order
                .reason
                .as_ref()
                .map(|value| value.as_str()),
            Some("V3_DURABLE_CONFIRM_AGE_SOFT")
        );
    }

    #[tokio::test]
    async fn strategy_intent_submit_high_urgency_bypasses_durable_confirm_age_soft() {
        let (mut state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state.v3_durable_confirm_soft_reject_age_us = 5_000;
        state.v3_durable_confirm_hard_reject_age_us = 10_000;
        state
            .v3_confirm_oldest_inflight_us
            .store(6_000, Ordering::Relaxed);
        if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(0) {
            gauge.store(6_000, Ordering::Relaxed);
        }
        let mut intent = strategy_intent_fixture();
        intent.urgency = IntentUrgency::High;

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent,
                shadow_run_id: None,
                predicted_policy: None,
                predicted_outcome: None,
            }),
        )
        .await
        .expect("high urgency should bypass durable confirm age soft");

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.volatile_order.status, "VOLATILE_ACCEPT");
        assert!(resp.volatile_order.session_seq.is_some());
        assert_eq!(resp.effective_policy.urgency.as_deref(), Some("HIGH"));
    }

    #[tokio::test]
    async fn strategy_intent_shadow_seed_creates_shadow_record_from_intent() {
        let state = build_test_state();
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-shadow".to_string(),
                version: 4,
                applied_at_ns: now_nanos(),
                default_execution_policy: ExecutionPolicyConfig {
                    policy: ExecutionPolicyKind::Passive,
                    prefer_passive: true,
                    post_only: true,
                    max_slippage_bps: Some(6),
                    participation_rate_bps: Some(1500),
                },
                venue_preference: vec![VenuePreference {
                    symbol: "AAPL".to_string(),
                    venue: "NASDAQ".to_string(),
                    rank: 1,
                }],
                shadow_enabled: true,
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let Json(resp) = super::super::strategy::handle_post_strategy_intent_shadow(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentShadowSeedRequest {
                shadow_run_id: "shadow-seed-1".to_string(),
                intent: strategy_intent_fixture(),
                predicted_policy: Some(ShadowPolicyView {
                    execution_policy: Some(ExecutionPolicyConfig {
                        policy: ExecutionPolicyKind::Passive,
                        prefer_passive: true,
                        post_only: false,
                        max_slippage_bps: Some(9),
                        participation_rate_bps: Some(1000),
                    }),
                    urgency: Some("HIGH".to_string()),
                    venue: Some("BATS".to_string()),
                }),
                predicted_outcome: Some(ShadowOutcomeView {
                    final_status: Some("VOLATILE_ACCEPT".to_string()),
                    reject_reason: None,
                    accepted_at_ns: Some(100),
                    durable_at_ns: None,
                }),
            }),
        )
        .await
        .expect("seed shadow from intent");

        assert_eq!(resp.shadow_run_id, "shadow-seed-1");
        assert_eq!(resp.intent_id, "intent-1");
        assert!(!resp.replaced);

        let record = state
            .strategy_shadow_store
            .get("shadow-seed-1", "intent-1")
            .expect("shadow record");
        assert_eq!(record.model_id, "model-1");
        assert_eq!(record.session_id, "sess-1");
        assert_eq!(record.session_seq, 0);
        assert_eq!(
            record
                .predicted_policy
                .as_ref()
                .and_then(|policy| policy.venue.as_deref()),
            Some("BATS")
        );
        assert_eq!(
            record
                .actual_policy
                .as_ref()
                .and_then(|policy| policy.venue.as_deref()),
            Some("NASDAQ")
        );
        assert_eq!(
            record
                .predicted_outcome
                .as_ref()
                .and_then(|outcome| outcome.final_status.as_deref()),
            Some("VOLATILE_ACCEPT")
        );
        assert_eq!(record.total_score_bps(), -20);
    }

    #[tokio::test]
    async fn strategy_intent_submit_returns_volatile_accept_and_seeds_shadow() {
        let (state, _ingress_rx, _durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-submit".to_string(),
                version: 8,
                applied_at_ns: now_nanos(),
                default_execution_policy: ExecutionPolicyConfig {
                    policy: ExecutionPolicyKind::Passive,
                    prefer_passive: true,
                    post_only: true,
                    max_slippage_bps: Some(4),
                    participation_rate_bps: Some(900),
                },
                venue_preference: vec![VenuePreference {
                    symbol: "AAPL".to_string(),
                    venue: "NASDAQ".to_string(),
                    rank: 1,
                }],
                shadow_enabled: true,
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent: strategy_intent_fixture(),
                shadow_run_id: Some("shadow-submit-1".to_string()),
                predicted_policy: Some(ShadowPolicyView {
                    execution_policy: Some(ExecutionPolicyConfig {
                        policy: ExecutionPolicyKind::Passive,
                        prefer_passive: false,
                        post_only: false,
                        max_slippage_bps: Some(7),
                        participation_rate_bps: None,
                    }),
                    urgency: Some("LOW".to_string()),
                    venue: Some("BATS".to_string()),
                }),
                predicted_outcome: Some(ShadowOutcomeView {
                    final_status: Some("VOLATILE_ACCEPT".to_string()),
                    reject_reason: None,
                    accepted_at_ns: Some(100),
                    durable_at_ns: None,
                }),
            }),
        )
        .await
        .expect("submit intent");

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(resp.snapshot_id, "snapshot-submit");
        assert_eq!(resp.version, 8);
        assert!(resp.shadow_enabled);
        assert!(resp.shadow_seeded);
        assert!(resp.policy_adjustments.is_empty());
        assert_eq!(resp.shadow_run_id.as_deref(), Some("shadow-submit-1"));
        assert_eq!(resp.order_request.intent_id.as_deref(), Some("intent-1"));
        assert_eq!(resp.volatile_order.status, "VOLATILE_ACCEPT");
        assert!(resp.volatile_order.session_seq.is_some());
        assert_eq!(state.v3_accepted_total_current(), 1);

        let record = state
            .strategy_shadow_store
            .get("shadow-submit-1", "intent-1")
            .expect("shadow record");
        assert_eq!(
            record
                .actual_policy
                .as_ref()
                .and_then(|policy| policy.venue.as_deref()),
            Some("NASDAQ")
        );
        assert_eq!(
            record
                .predicted_outcome
                .as_ref()
                .and_then(|outcome| outcome.final_status.as_deref()),
            Some("VOLATILE_ACCEPT")
        );
    }

    #[tokio::test]
    async fn strategy_intent_submit_exports_actual_policy_into_feedback_and_metrics() {
        let (mut state, ingress_rx, mut durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        let feedback_path =
            std::env::temp_dir().join(format!("gateway-rust-quant-submit-{}.jsonl", now_nanos()));
        state.quant_feedback_exporter = Arc::new(crate::strategy::sink::FeedbackExporter::new(
            crate::strategy::sink::FeedbackExportConfig {
                enabled: true,
                path: feedback_path.clone(),
                queue_capacity: 32,
                drop_policy: crate::strategy::sink::FeedbackDropPolicy::DropNewest,
            },
        ));
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-submit-feedback".to_string(),
                version: 9,
                applied_at_ns: now_nanos(),
                default_execution_policy: ExecutionPolicyConfig {
                    policy: ExecutionPolicyKind::Passive,
                    prefer_passive: true,
                    post_only: true,
                    max_slippage_bps: Some(4),
                    participation_rate_bps: Some(900),
                },
                venue_preference: vec![VenuePreference {
                    symbol: "AAPL".to_string(),
                    venue: "NASDAQ".to_string(),
                    rank: 1,
                }],
                shadow_enabled: true,
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let writer_handle = tokio::spawn(super::super::run_v3_single_writer(
            0,
            ingress_rx,
            state.clone(),
        ));
        let durable_handle = tokio::spawn(super::super::run_v3_durable_worker(
            0,
            durable_rxs.remove(0),
            state.clone(),
            state.v3_durable_worker_batch_max,
            state.v3_durable_worker_batch_wait_us,
            super::super::V3DurableWorkerBatchAdaptiveConfig {
                enabled: state.v3_durable_worker_batch_adaptive,
                batch_min: state.v3_durable_worker_batch_min,
                batch_max: state.v3_durable_worker_batch_max,
                wait_min: Duration::from_micros(state.v3_durable_worker_batch_wait_min_us.max(1)),
                wait_max: Duration::from_micros(state.v3_durable_worker_batch_wait_us.max(1)),
                low_util_pct: state.v3_durable_worker_batch_adaptive_low_util_pct,
                high_util_pct: state.v3_durable_worker_batch_adaptive_high_util_pct,
            },
            super::super::V3DurableWorkerPressureConfig::from_env(
                state.v3_durable_worker_inflight_hard_cap_pct,
            ),
        ));

        let (status, Json(resp)) = super::super::strategy::handle_post_strategy_intent_submit(
            State(state.clone()),
            Json(super::super::strategy::StrategyIntentSubmitRequest {
                intent: strategy_intent_fixture(),
                shadow_run_id: Some("shadow-submit-feedback".to_string()),
                predicted_policy: Some(ShadowPolicyView {
                    execution_policy: Some(ExecutionPolicyConfig {
                        policy: ExecutionPolicyKind::Aggressive,
                        prefer_passive: false,
                        post_only: false,
                        max_slippage_bps: Some(10),
                        participation_rate_bps: None,
                    }),
                    urgency: Some("LOW".to_string()),
                    venue: Some("BATS".to_string()),
                }),
                predicted_outcome: Some(ShadowOutcomeView {
                    final_status: Some("VOLATILE_ACCEPT".to_string()),
                    reject_reason: None,
                    accepted_at_ns: Some(100),
                    durable_at_ns: None,
                }),
            }),
        )
        .await
        .expect("submit intent");

        assert_eq!(status, StatusCode::ACCEPTED);
        let _session_seq = resp.volatile_order.session_seq.expect("session seq");

        let mut durable_seen = false;
        for _ in 0..100 {
            let shadow = state
                .strategy_shadow_store
                .get("shadow-submit-feedback", "intent-1");
            let shadow_durable = shadow
                .as_ref()
                .and_then(|record| record.actual_outcome.as_ref())
                .and_then(|outcome| outcome.final_status.as_deref())
                == Some("DURABLE_ACCEPTED");
            if state.v3_durable_accepted_total_current() >= 1 && shadow_durable {
                durable_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            durable_seen,
            "expected durable accept reflected in counters and shadow"
        );

        for _ in 0..100 {
            if state.quant_feedback_exporter.metrics().written_total >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(state.quant_feedback_exporter.metrics().written_total >= 2);

        let raw = wait_for_feedback_lines(&feedback_path, 2);
        assert!(raw.contains("\"intentId\":\"intent-1\""));
        assert!(raw.contains("\"shadowRunIds\":[\"shadow-submit-feedback\"]"));
        assert!(raw.contains("\"actualPolicy\""));
        assert!(raw.contains("\"effectiveRiskBudgetRef\":\"budget-42\""));
        assert!(raw.contains("\"venue\":\"NASDAQ\""));
        assert!(raw.contains("\"finalStatus\":\"DURABLE_ACCEPTED\""));

        let shadow = state
            .strategy_shadow_store
            .get("shadow-submit-feedback", "intent-1")
            .expect("shadow record");
        assert_eq!(
            shadow
                .actual_policy
                .as_ref()
                .and_then(|policy| policy.venue.as_deref()),
            Some("NASDAQ")
        );
        assert_eq!(shadow.comparison_status, ShadowComparisonStatus::Matched);

        let metrics = super::super::metrics::handle_metrics(State(state.clone())).await;
        assert!(metrics.contains("gateway_strategy_shadow_negative_score_count "));
        assert!(metrics.contains("gateway_strategy_shadow_last_evaluated_at_ns "));

        writer_handle.abort();
        durable_handle.abort();
    }

    #[tokio::test]
    async fn v3_hot_path_rejects_stale_strategy_snapshot() {
        let state = build_test_state();
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "stale".to_string(),
                version: 1,
                applied_at_ns: 1,
                kill_switch_policy: KillSwitchPolicy {
                    reject_when_snapshot_stale: true,
                    reject_when_shadow_stale: false,
                    snapshot_stale_after_ns: 1,
                },
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store stale snapshot");

        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers("acc-stale", Some("idem_v3_strategy_stale")),
            Json(request_with_client_id("cid_v3_strategy_stale")),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 response"));

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.status, "REJECTED");
        assert_eq!(resp.reason.as_deref(), Some("V3_STRATEGY_SNAPSHOT_STALE"));
    }

    #[tokio::test]
    async fn v3_hot_path_rejects_strategy_symbol_limit() {
        let state = build_test_state();
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "limits".to_string(),
                version: 1,
                applied_at_ns: now_nanos(),
                symbol_limits: vec![SymbolExecutionOverride {
                    symbol: "AAPL".to_string(),
                    execution_policy: None,
                    urgency_override: None,
                    max_order_qty: Some(10),
                    max_notional: None,
                }],
                ..ExecutionConfigSnapshot::default()
            })
            .expect("store snapshot");

        let (status, Json(resp)) = handle_order_v3(
            State(state.clone()),
            headers("acc-limit", Some("idem_v3_strategy_symbol_limit")),
            Json(request_with_client_id("cid_v3_strategy_symbol_limit")),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 response"));

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(resp.status, "REJECTED");
        assert_eq!(resp.reason.as_deref(), Some("V3_STRATEGY_MAX_ORDER_QTY"));
    }

    #[tokio::test]
    async fn v3_integration_feedback_export_and_shadow_match_durable_accept() {
        let (mut state, ingress_rx, mut durable_rxs) =
            build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
        let feedback_path =
            std::env::temp_dir().join(format!("gateway-rust-quant-feedback-{}.jsonl", now_nanos()));
        state.quant_feedback_exporter = Arc::new(crate::strategy::sink::FeedbackExporter::new(
            crate::strategy::sink::FeedbackExportConfig {
                enabled: true,
                path: feedback_path.clone(),
                queue_capacity: 32,
                drop_policy: crate::strategy::sink::FeedbackDropPolicy::DropNewest,
            },
        ));
        state
            .strategy_snapshot_store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "shadow-enabled".to_string(),
                version: 1,
                applied_at_ns: now_nanos(),
                shadow_enabled: true,
                ..ExecutionConfigSnapshot::default()
            })
            .expect("enable shadow snapshot");
        state
            .strategy_shadow_store
            .upsert(ShadowRecord {
                schema_version: SHADOW_RECORD_SCHEMA_VERSION,
                shadow_run_id: "shadow-run-1".to_string(),
                model_id: "model-q".to_string(),
                intent_id: "intent-q".to_string(),
                session_id: "v3-int-acc-q".to_string(),
                session_seq: 1,
                predicted_policy: None,
                actual_policy: None,
                predicted_outcome: None,
                actual_outcome: None,
                score_components: Vec::new(),
                evaluated_at_ns: 0,
                comparison_status: ShadowComparisonStatus::Pending,
            })
            .expect("seed shadow");

        let writer_handle = tokio::spawn(super::super::run_v3_single_writer(
            0,
            ingress_rx,
            state.clone(),
        ));
        let durable_handle = tokio::spawn(super::super::run_v3_durable_worker(
            0,
            durable_rxs.remove(0),
            state.clone(),
            state.v3_durable_worker_batch_max,
            state.v3_durable_worker_batch_wait_us,
            super::super::V3DurableWorkerBatchAdaptiveConfig {
                enabled: state.v3_durable_worker_batch_adaptive,
                batch_min: state.v3_durable_worker_batch_min,
                batch_max: state.v3_durable_worker_batch_max,
                wait_min: Duration::from_micros(state.v3_durable_worker_batch_wait_min_us.max(1)),
                wait_max: Duration::from_micros(state.v3_durable_worker_batch_wait_us.max(1)),
                low_util_pct: state.v3_durable_worker_batch_adaptive_low_util_pct,
                high_util_pct: state.v3_durable_worker_batch_adaptive_high_util_pct,
            },
            super::super::V3DurableWorkerPressureConfig::from_env(
                state.v3_durable_worker_inflight_hard_cap_pct,
            ),
        ));

        let account_id = "v3-int-acc-q";
        let mut req = request_with_client_id("cid_v3_quant_feedback");
        req.intent_id = Some("intent-q".to_string());
        req.model_id = Some("model-q".to_string());
        let (status, Json(accepted)) = handle_order_v3(
            State(state.clone()),
            headers(account_id, Some("idem_v3_quant_feedback")),
            Json(req),
        )
        .await
        .unwrap_or_else(|_| panic!("v3 order submit failed"));
        assert_eq!(status, StatusCode::ACCEPTED);
        let session_seq = accepted.session_seq.expect("session seq");

        let mut durable_seen = false;
        for _ in 0..100 {
            let Json(status_resp) = handle_get_order_v3(
                State(state.clone()),
                headers(account_id, None),
                Path((account_id.to_string(), session_seq)),
            )
            .await
            .unwrap_or_else(|_| panic!("v3 durable lookup failed"));
            if status_resp.status == "DURABLE_ACCEPTED" {
                durable_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(durable_seen, "expected eventual DURABLE_ACCEPTED");

        for _ in 0..100 {
            if state.quant_feedback_exporter.metrics().written_total >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(state.quant_feedback_exporter.metrics().written_total >= 2);

        let raw = wait_for_feedback_lines(&feedback_path, 2);
        assert!(raw.contains("\"intentId\":\"intent-q\""));
        assert!(raw.contains("\"modelId\":\"model-q\""));
        assert!(raw.contains("\"shadowRunIds\":[\"shadow-run-1\"]"));
        assert!(raw.contains("\"finalStatus\":\"VOLATILE_ACCEPT\""));
        assert!(raw.contains("\"finalStatus\":\"DURABLE_ACCEPTED\""));

        let shadow = state
            .strategy_shadow_store
            .get("shadow-run-1", "intent-q")
            .expect("shadow record");
        assert_eq!(
            shadow
                .actual_outcome
                .as_ref()
                .and_then(|outcome| outcome.final_status.as_deref()),
            Some("DURABLE_ACCEPTED")
        );
        assert_eq!(shadow.comparison_status, ShadowComparisonStatus::Matched);

        writer_handle.abort();
        durable_handle.abort();
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
