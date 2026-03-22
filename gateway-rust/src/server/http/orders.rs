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
        req.decision_key.as_deref(),
        req.decision_attempt_seq,
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
        None,
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
    decision_key: Option<&str>,
    decision_attempt_seq: Option<u64>,
    intent_id: Option<&str>,
    model_id: Option<&str>,
    actual_policy: Option<Arc<ShadowPolicyView>>,
    effective_risk_budget_ref: Option<Arc<str>>,
    path_tag: &'static str,
    t0: u64,
) -> V3HotPathOutcome {
    let t0_tsc = state.capture_v3_tsc_stamp();
    let hotpath_sampled = state.v3_hotpath_sampled();
    if state.v3_startup_rebuild_in_progress() {
        return V3HotPathOutcome::rejected(
            StatusCode::SERVICE_UNAVAILABLE,
            V3_TCP_KIND_REJECTED,
            "REJECTED",
            "STARTUP_REBUILD_IN_PROGRESS",
            t0,
        );
    }
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
    let (execution_run_id, decision_key, decision_attempt_seq, intent_id, model_id) =
        feedback_metadata(
            execution_run_id,
            decision_key,
            decision_attempt_seq,
            intent_id,
            model_id,
        );
    // Strategy submit can mark high-urgency flow, but only soft admission is relaxable.
    let strategy_soft_bypass = strategy_allows_soft_admission_bypass(actual_policy.as_deref());
    let rejected_feedback_context = state.quant_feedback_exporter.is_enabled().then(|| {
        (
            session_id.to_string(),
            account_id.to_string(),
            symbol.clone(),
            execution_run_id.map(str::to_string),
            decision_key.map(str::to_string),
            decision_attempt_seq,
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
            decision_key,
            decision_attempt_seq,
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
            let event = if let Some(decision_key) = decision_key.as_deref() {
                event.with_decision_key(decision_key)
            } else {
                event
            };
            let event = if let Some(decision_attempt_seq) = decision_attempt_seq {
                event.with_decision_attempt_seq(*decision_attempt_seq)
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
            decision_key,
            decision_attempt_seq,
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
        decision_key: decision_key.map(Arc::<str>::from),
        decision_attempt_seq,
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
            task.decision_key.clone(),
            task.decision_attempt_seq,
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
                decision_key,
                decision_attempt_seq,
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
                let event = if let Some(decision_key) = decision_key.as_deref() {
                    event.with_decision_key(decision_key)
                } else {
                    event
                };
                let event = if let Some(decision_attempt_seq) = decision_attempt_seq {
                    event.with_decision_attempt_seq(decision_attempt_seq)
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
mod tests;
