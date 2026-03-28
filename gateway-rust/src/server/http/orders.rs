//! 注文API（HTTP入口層の中心）:
//! - 役割: 入口で受理した注文を FastPathEngine に投入し、監査ログ/Bus へ記録する。
//! - 位置: `server/http/mod.rs` から呼ばれる入口ハンドラ群（コアフローの同期境界）。
//! - 内包: 受理/取得/キャンセルとレスポンス型をこのファイルに集約。

mod classic;
mod response;
mod support;
mod tcp_codec;

use crate::audit::{self, AuditEvent};
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
use gateway_core::now_nanos;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use super::{
    AppState, AuthErrorResponse, V3OrderTask, maybe_append_strategy_execution_fact,
    publish_quant_feedback,
};
pub(super) use classic::{
    handle_amend_order, handle_cancel_order, handle_get_order, handle_get_order_by_client_id,
    handle_get_order_v2, handle_order, handle_order_v2, handle_replace_order,
};
pub(super) use response::{DurableOrderStatusResponse, VolatileOrderResponse};
pub(super) use support::render_v3_symbol_key;
use support::*;
pub(super) use tcp_codec::{
    V3TcpDecodedFrame, V3TcpDecodedRequest, authenticate_v3_tcp_token, decode_v3_tcp_frame,
    decode_v3_tcp_request, encode_v3_tcp_auth_ok, encode_v3_tcp_decode_error,
    encode_v3_tcp_response, encode_v3_tcp_response_raw,
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

#[cfg(test)]
mod tests;
