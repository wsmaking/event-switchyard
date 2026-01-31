//! 監査API（監査ログの読み出し口）:
//! - 役割: 監査ログを「閲覧/検証/アンカー取得」するための読み取り専用の入口。
//! - 位置: 注文処理の結果を後から追跡・検証するための運用/調査パス。
//! - 内包: 注文/アカウント別イベント取得 + 監査ログ検証/アンカー取得。

use axum::{
    extract::{Path, Query, State},
    http::{header::AUTHORIZATION, StatusCode},
    Json,
};
use axum::http::HeaderMap;

use crate::audit::{self, AuditAnchor, AuditEvent, AuditVerifyResult};
use crate::auth::{AuthError, AuthResult};

use super::{AppState, AuthErrorResponse};

// 監査系ハンドラ: イベント取得と監査ログ検証を担当。

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AuditEventsResponse {
    order_id: Option<String>,
    account_id: Option<String>,
    events: Vec<AuditEvent>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AuditVerifyQuery {
    #[serde(default)]
    from_seq: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

/// 注文イベント取得（GET /orders/{order_id}/events）
/// - 所有権検証 → 監査ログから該当イベントを抽出
pub(super) async fn handle_order_events(
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

    let events = audit::read_events_from_path(
        state.audit_read_path.as_ref(),
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

/// アカウントイベント取得（GET /accounts/{account_id}/events）
/// - 自アカウントのみ許可、監査ログから抽出
pub(super) async fn handle_account_events(
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

    let events = audit::read_events_from_path(
        state.audit_read_path.as_ref(),
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

/// 監査ログ検証（GET /audit/verify）
/// - from_seq/limit で部分検証、ハッシュチェーン不整合を検出
pub(super) async fn handle_audit_verify(
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

/// 監査アンカー取得（GET /audit/anchor）
/// - チェーンルートと最新ハッシュのスナップショット
pub(super) async fn handle_audit_anchor(
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
