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
    extract::{Path, State},
    http::{header::AUTHORIZATION, StatusCode},
    Json,
};
use gateway_core::now_nanos;
use std::{
    hint::black_box,
    sync::{Arc, atomic::Ordering},
};

use super::{AppState, AuthErrorResponse, V3OrderTask, V3RiskMarginMode};

type AuthResponse<T> = Result<T, (StatusCode, Json<AuthErrorResponse>)>;
type OrderResponseResult =
    Result<(StatusCode, Json<OrderResponse>), (StatusCode, Json<AuthErrorResponse>)>;
type VolatileOrderResponseResult = Result<
    (StatusCode, Json<VolatileOrderResponse>),
    (StatusCode, Json<AuthErrorResponse>),
>;

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

fn simulate_v3_risk_position_seed(req: &OrderRequest, account_id: &str) -> u64 {
    let mut acc = req
        .qty
        .wrapping_mul(31)
        .wrapping_add(req.price.unwrap_or(0).wrapping_mul(17))
        .wrapping_add(req.symbol.len() as u64);

    for (i, b) in account_id.as_bytes().iter().enumerate() {
        let shift = ((i & 7) * 8) as u32;
        acc ^= (*b as u64) << shift;
    }
    black_box(acc)
}

fn simulate_v3_risk_margin(mut acc: u64, req: &OrderRequest, loops: u32) -> u64 {
    let qty_mix = req.qty.rotate_left(7) ^ req.qty.rotate_right(11);
    let price_mix = req.price.unwrap_or(0) ^ (req.price.unwrap_or(0) >> 3);

    for i in 0..loops {
        let i64 = i as u64;
        acc = acc
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223)
            .wrapping_add(i64 ^ qty_mix);
        // 仮説: ループ内のrotate/mulを減らし、固定mixでtailを下げる。
        acc ^= price_mix.wrapping_add(i64 & 0x3f);
        black_box(acc);
    }
    black_box(acc)
}

fn simulate_v3_risk_margin_incremental(mut acc: u64, req: &OrderRequest, loops: u32) -> u64 {
    let qty = req.qty;
    let price = req.price.unwrap_or(0);
    let mut rot = qty;
    let mut rot_step: u32 = 0;
    let mut price_factor: u64 = 97;
    let mut price_term = price.wrapping_mul(price_factor);

    for i in 0..loops {
        let i64 = i as u64;
        acc = acc
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223)
            .wrapping_add(i64 ^ rot);
        acc ^= price_term;
        black_box(acc);

        rot_step += 1;
        if rot_step == 32 {
            rot_step = 0;
            rot = qty;
        } else {
            rot = rot.rotate_left(1);
        }

        price_factor += 1;
        if price_factor > 160 {
            price_factor = 97;
            price_term = price.wrapping_mul(price_factor);
        } else {
            price_term = price_term.wrapping_add(price);
        }
    }
    black_box(acc)
}

fn simulate_v3_risk_limits(acc: u64, req: &OrderRequest) -> bool {
    let notional = req.qty.saturating_mul(req.price.unwrap_or(0));
    let cap = (acc & 0xffff).saturating_mul(1_000).saturating_add(500_000);
    let pass = notional <= cap;
    black_box(pass)
}

#[derive(Clone)]
struct V3RiskReservation {
    position_key: (String, [u8; 8]),
    prev_position: i64,
    notional: u64,
    account_daily: Arc<gateway_core::AccountPosition>,
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

fn evaluate_v3_real_risk(
    state: &AppState,
    account_id: &str,
    req: &OrderRequest,
) -> Result<V3RiskReservation, &'static str> {
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

    let price = req.price.unwrap_or(0);
    let notional = (req.qty as u128).saturating_mul(price as u128);
    let max_notional = state.v3_risk_max_notional.min(symbol_limits.max_notional);
    if notional > max_notional as u128 {
        return Err("RISK_REJECT");
    }

    if req.qty > i64::MAX as u64 {
        return Err("INVALID_QTY");
    }
    let delta = if side == 1 {
        req.qty as i64
    } else {
        -(req.qty as i64)
    };
    let account_key = account_id.to_owned();
    let position_key = (account_key.clone(), symbol_key);
    let mut position = state
        .v3_account_symbol_position
        .entry(position_key.clone())
        .or_insert(0);
    let prev_position = *position;
    let next_position = prev_position.saturating_add(delta);
    if (next_position as i128).abs() as u128 > state.v3_risk_max_abs_position_qty as u128 {
        return Err("RISK_POSITION_LIMIT");
    }
    *position = next_position;
    drop(position);

    let account_daily = state
        .v3_account_daily_notional
        .entry(account_key)
        .or_insert_with(|| {
            Arc::new(gateway_core::AccountPosition::new(
                state.v3_risk_daily_notional_limit,
            ))
        })
        .clone();
    if !account_daily.try_add_notional(notional as u64) {
        if let Some(mut rollback_pos) = state.v3_account_symbol_position.get_mut(&position_key) {
            *rollback_pos = prev_position;
        }
        return Err("RISK_DAILY_LIMIT");
    }

    Ok(V3RiskReservation {
        position_key,
        prev_position,
        notional: notional as u64,
        account_daily,
    })
}

fn rollback_v3_real_risk(state: &AppState, reservation: &V3RiskReservation) {
    reservation.account_daily.sub_notional(reservation.notional);
    if let Some(mut position) = state
        .v3_account_symbol_position
        .get_mut(&reservation.position_key)
    {
        *position = reservation.prev_position;
    }
}

/// v3 注文受付（POST /v3/orders）
/// - Phase1最小核: 認証 -> single-writer enqueue -> VOLATILE_ACCEPT 応答
pub(super) async fn handle_order_v3(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> VolatileOrderResponseResult {
    let t0 = now_nanos();
    let principal = authenticate_request(&state, &headers, t0)?;
    let ingress = &state.v3_ingress;
    if ingress.maybe_recover(t0) {
        state.v3_kill_recovered_total.fetch_add(1, Ordering::Relaxed);
    }

    // parse段階: JSON展開済みリクエストから必要項目を取り出す。
    let parse_t0 = now_nanos();
    let symbol = req.symbol.as_str();
    let qty = req.qty;
    let price = req.price.unwrap_or(0);
    let _ = (symbol, qty, price);
    let parse_elapsed = now_nanos().saturating_sub(parse_t0) / 1_000;
    state.v3_stage_parse_hist.record(parse_elapsed);

    // risk段階: プロファイルに応じて計算量を変えて支配区間を観測する。
    let risk_t0 = now_nanos();
    let risk_position_t0 = now_nanos();
    let risk_seed = simulate_v3_risk_position_seed(&req, &principal.account_id);
    let risk_position_elapsed = now_nanos().saturating_sub(risk_position_t0) / 1_000;
    state
        .v3_stage_risk_position_hist
        .record(risk_position_elapsed);

    let risk_margin_t0 = now_nanos();
    let risk_acc = match state.v3_risk_margin_mode {
        V3RiskMarginMode::Legacy => simulate_v3_risk_margin(risk_seed, &req, state.v3_risk_loops),
        V3RiskMarginMode::Incremental => {
            simulate_v3_risk_margin_incremental(risk_seed, &req, state.v3_risk_loops)
        }
    };
    let risk_margin_elapsed = now_nanos().saturating_sub(risk_margin_t0) / 1_000;
    state.v3_stage_risk_margin_hist.record(risk_margin_elapsed);

    let risk_limits_t0 = now_nanos();
    let _risk_pass = simulate_v3_risk_limits(risk_acc, &req);
    let risk_limits_elapsed = now_nanos().saturating_sub(risk_limits_t0) / 1_000;
    state.v3_stage_risk_limits_hist.record(risk_limits_elapsed);

    let risk_elapsed = now_nanos().saturating_sub(risk_t0) / 1_000;
    state.v3_stage_risk_hist.record(risk_elapsed);

    // /v3 でも実Risk（qty/notional/symbol/daily-limit/position-limit）を判定して業務拒否を返す。
    let risk_reservation = match evaluate_v3_real_risk(&state, &principal.account_id, &req) {
        Ok(res) => res,
        Err(reason) => {
            match reason {
                "INVALID_QTY" | "INVALID_SIDE" => {
                    state.reject_invalid_qty.fetch_add(1, Ordering::Relaxed);
                }
                "RISK_REJECT" | "RISK_DAILY_LIMIT" | "RISK_POSITION_LIMIT" => {
                    state.reject_risk.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    state.reject_invalid_symbol.fetch_add(1, Ordering::Relaxed);
                }
            }
            record_ack(&state, t0);
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(VolatileOrderResponse::rejected(
                    &principal.account_id,
                    "REJECTED",
                    reason,
                )),
            ));
        }
    };

    // kill判定（95%超 or 既にkill済み）は503。
    let queue_pct = ingress.queue_utilization_pct();
    if ingress.is_killed() || queue_pct >= state.v3_kill_reject_pct {
        rollback_v3_real_risk(&state, &risk_reservation);
        ingress.kill(t0);
        state.v3_rejected_killed_total.fetch_add(1, Ordering::Relaxed);
        record_ack(&state, t0);
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(VolatileOrderResponse::rejected(
                &principal.account_id,
                "KILLED",
                "V3_QUEUE_KILLED",
            )),
        ));
    }

    // ソフト水位（85%超）は429。
    if queue_pct >= state.v3_soft_reject_pct {
        rollback_v3_real_risk(&state, &risk_reservation);
        state.v3_rejected_soft_total.fetch_add(1, Ordering::Relaxed);
        record_ack(&state, t0);
        return Ok((
            StatusCode::TOO_MANY_REQUESTS,
            Json(VolatileOrderResponse::rejected(
                &principal.account_id,
                "REJECTED",
                "V3_BACKPRESSURE_SOFT",
            )),
        ));
    }

    let session_seq = ingress.next_seq();
    let attempt_id = format!("att_{}", session_seq);
    let received_at_ns = now_nanos();
    let task = V3OrderTask {
        _session_seq: session_seq,
        _received_at_ns: received_at_ns,
    };

    let enqueue_t0 = now_nanos();
    match ingress.try_enqueue(task) {
        Ok(()) => {
            let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
            state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            state.v3_accepted_total.fetch_add(1, Ordering::Relaxed);
            let serialize_t0 = now_nanos();
            let body = VolatileOrderResponse::accepted(
                principal.account_id,
                session_seq,
                attempt_id,
                received_at_ns,
            );
            let serialize_elapsed = now_nanos().saturating_sub(serialize_t0) / 1_000;
            state.v3_stage_serialize_hist.record(serialize_elapsed);
            record_ack(&state, t0);
            Ok((StatusCode::ACCEPTED, Json(body)))
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            rollback_v3_real_risk(&state, &risk_reservation);
            let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
            state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            // 実際にfullならkillへ昇格。
            ingress.kill(now_nanos());
            state.v3_rejected_killed_total.fetch_add(1, Ordering::Relaxed);
            record_ack(&state, t0);
            Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(VolatileOrderResponse::rejected(
                    &principal.account_id,
                    "KILLED",
                    "V3_QUEUE_FULL",
                )),
            ))
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            rollback_v3_real_risk(&state, &risk_reservation);
            let enqueue_elapsed = now_nanos().saturating_sub(enqueue_t0) / 1_000;
            state.v3_stage_enqueue_hist.record(enqueue_elapsed);
            ingress.kill(now_nanos());
            state.v3_rejected_killed_total.fetch_add(1, Ordering::Relaxed);
            record_ack(&state, t0);
            Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(VolatileOrderResponse::rejected(
                    &principal.account_id,
                    "KILLED",
                    "V3_INGRESS_CLOSED",
                )),
            ))
        }
    }
}

/// 注文受付（POST /orders）
/// - JWT検証 → Idempotency-Key → FastPath → 監査/Bus/Snapshot 保存
pub(super) async fn handle_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> OrderResponseResult {
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
            // 初回受理。以降は「ID登録 -> 監査記録 -> (必要なら)Bus送信 -> ACK」の順で進める。
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

            // 受理イベントを監査ログ(WAL)へ追記する。
            // t0 は入口時刻で、enqueue完了までの遅延計測に使う。
            let timings = state.audit_log.append_with_timings(
                AuditEvent {
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
                },
                t0,
            );
            // WAL enqueue までの遅延を別ヒストグラムで観測する。
            record_wal_enqueue(&state, t0, timings);
            if state.audit_log.async_enabled() {
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
    fn accepted(session_id: String, session_seq: u64, attempt_id: String, received_at_ns: u64) -> Self {
        Self {
            session_id,
            session_seq: Some(session_seq),
            attempt_id: Some(attempt_id),
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
