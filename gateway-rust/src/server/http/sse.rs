//! SSE API（非同期通知の配信口）:
//! - 役割: ACK後の状態変化をリアルタイム配信し、UI更新の即時性を担保する。
//! - 入口: `/orders/{id}/stream`（注文単位）と `/stream`（アカウント単位）。
//! - 挙動: `Last-Event-ID` があればリプレイし、古すぎる場合は `resync_required` を通知。
//! - 実体: `SseHub` のリングバッファ + broadcast で「リプレイ→live」を連結。

use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, header::AUTHORIZATION},
    response::sse::{Event, Sse},
};
use futures::stream::Stream;
use std::convert::Infallible;
use std::time::Duration;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::auth::AuthResult;

use super::{AppState, AuthErrorResponse};

// SSE配信は「リプレイ→ライブ配信」の順で組み立てる。

/// 注文SSEストリーム（GET /orders/{order_id}/stream）
/// - 用途: 単一注文の状態変化追跡（注文詳細のライブ更新）
/// - 手順: 所有権検証 → リプレイ判定 → live配信
pub(super) async fn handle_order_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<AuthErrorResponse>)>
{
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

    // 直近のイベントIDがあれば、その続きをリプレイする
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
        initial.push(
            Event::default()
                .event("resync_required")
                .data(data.to_string()),
        );
    }
    for ev in replay.events {
        initial.push(
            Event::default()
                .id(ev.id.to_string())
                .event(ev.event_type)
                .data(ev.data),
        );
    }

    // リプレイ後に live 配信へ接続
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

/// アカウントSSEストリーム（GET /stream）
/// - 用途: アカウント全体の更新をまとめて受信（口座UIのライブ更新）
/// - 手順: リプレイ判定 → live配信、必要なら resync_required を通知
pub(super) async fn handle_account_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<AuthErrorResponse>)>
{
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

    // 直近のイベントIDがあれば、その続きをリプレイする
    let last_event_id = parse_last_event_id(&headers);
    let replay = state
        .sse_hub
        .replay_account(&principal.account_id, last_event_id);
    let mut initial = Vec::new();
    if let Some(resync) = replay.resync_required {
        let data = serde_json::json!({
            "scope": "account",
            "id": principal.account_id,
            "lastEventId": resync.last_event_id,
            "oldestAvailableId": resync.oldest_available_id,
            "eventsEndpoint": format!("/accounts/{}/events", principal.account_id),
        });
        initial.push(
            Event::default()
                .event("resync_required")
                .data(data.to_string()),
        );
    }
    for ev in replay.events {
        initial.push(
            Event::default()
                .id(ev.id.to_string())
                .event(ev.event_type)
                .data(ev.data),
        );
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

/// `Last-Event-ID` の解析（リプレイ開始位置）
fn parse_last_event_id(headers: &HeaderMap) -> Option<u64> {
    let raw = headers.get("Last-Event-ID")?.to_str().ok()?.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<u64>().ok()
}
