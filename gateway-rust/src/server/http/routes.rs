use super::AppState;
use super::audit::{
    handle_account_events, handle_audit_anchor, handle_audit_verify, handle_order_events,
};
use super::metrics::{handle_health, handle_metrics};
use super::orders::{
    handle_amend_order, handle_cancel_order, handle_get_order, handle_get_order_by_client_id,
    handle_get_order_v2, handle_get_order_v3, handle_order, handle_order_v2, handle_order_v3,
    handle_replace_order,
};
use super::sse::{handle_account_stream, handle_order_stream};
use super::strategy::{
    handle_get_shadow_record, handle_get_shadow_run_summary, handle_get_strategy_config,
    handle_post_shadow_record, handle_post_strategy_intent_adapt,
    handle_post_strategy_intent_shadow, handle_post_strategy_intent_submit,
    handle_put_strategy_config,
};
use super::strategy_read::{
    handle_get_strategy_catchup_by_execution_run_id, handle_get_strategy_catchup_by_intent_id,
    handle_get_strategy_replay_by_execution_run_id, handle_get_strategy_replay_by_intent_id,
    handle_get_strategy_runtime,
};
use axum::{
    Router,
    routing::{get, post},
};
use tower_http::cors::CorsLayer;

pub(super) fn build_http_router(
    state: AppState,
    v3_http_ingress_enable: bool,
    v3_http_confirm_enable: bool,
) -> Router {
    let app_base = Router::new()
        .route("/orders", post(handle_order))
        .route("/v2/orders", post(handle_order_v2))
        .route("/orders/{order_id}", get(handle_get_order))
        .route("/v2/orders/{order_id}", get(handle_get_order_v2))
        .route(
            "/orders/client/{client_order_id}",
            get(handle_get_order_by_client_id),
        )
        .route(
            "/v2/orders/client/{client_order_id}",
            get(handle_get_order_by_client_id),
        )
        .route("/orders/{order_id}/cancel", post(handle_cancel_order))
        .route("/v2/orders/{order_id}/cancel", post(handle_cancel_order))
        .route("/orders/{order_id}/replace", post(handle_replace_order))
        .route("/v2/orders/{order_id}/replace", post(handle_replace_order))
        .route("/orders/{order_id}/amend", post(handle_amend_order))
        .route("/v2/orders/{order_id}/amend", post(handle_amend_order))
        .route("/orders/{order_id}/events", get(handle_order_events))
        .route("/accounts/{account_id}/events", get(handle_account_events))
        .route("/orders/{order_id}/stream", get(handle_order_stream))
        .route("/stream", get(handle_account_stream))
        .route("/audit/verify", get(handle_audit_verify))
        .route("/audit/anchor", get(handle_audit_anchor))
        .route(
            "/strategy/config",
            get(handle_get_strategy_config).put(handle_put_strategy_config),
        )
        .route(
            "/strategy/intent/adapt",
            post(handle_post_strategy_intent_adapt),
        )
        .route(
            "/strategy/intent/submit",
            post(handle_post_strategy_intent_submit),
        )
        .route(
            "/strategy/intent/shadow",
            post(handle_post_strategy_intent_shadow),
        )
        .route("/strategy/shadow", post(handle_post_shadow_record))
        .route(
            "/strategy/shadow/{shadow_run_id}/summary",
            get(handle_get_shadow_run_summary),
        )
        .route(
            "/strategy/shadow/{shadow_run_id}/{intent_id}",
            get(handle_get_shadow_record),
        )
        .route(
            "/strategy/runtime/{parent_intent_id}",
            get(handle_get_strategy_runtime),
        )
        .route(
            "/strategy/replay/execution/{execution_run_id}",
            get(handle_get_strategy_replay_by_execution_run_id),
        )
        .route(
            "/strategy/catchup/execution/{execution_run_id}",
            get(handle_get_strategy_catchup_by_execution_run_id),
        )
        .route(
            "/strategy/replay/intent/{intent_id}",
            get(handle_get_strategy_replay_by_intent_id),
        )
        .route(
            "/strategy/catchup/intent/{intent_id}",
            get(handle_get_strategy_catchup_by_intent_id),
        )
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics));
    let mut app = app_base;
    if v3_http_ingress_enable {
        app = app.route("/v3/orders", post(handle_order_v3));
    }
    if v3_http_confirm_enable {
        app = app.route(
            "/v3/orders/{session_id}/{session_seq}",
            get(handle_get_order_v3),
        );
    }
    app.layer(CorsLayer::permissive()).with_state(state)
}
