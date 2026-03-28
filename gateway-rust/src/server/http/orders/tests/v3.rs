use super::*;
use crate::server::http::{
    V3DurableWorkerBatchAdaptiveConfig, V3DurableWorkerPressureConfig, metrics,
    run_v3_durable_worker, run_v3_loss_monitor, run_v3_single_writer,
};

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

    let writer_handle = tokio::spawn(run_v3_single_writer(0, ingress_rx, state.clone()));
    let durable_handle = tokio::spawn(run_v3_durable_worker(
        0,
        durable_rxs.remove(0),
        state.clone(),
        state.v3_durable_worker_batch_max,
        state.v3_durable_worker_batch_wait_us,
        V3DurableWorkerBatchAdaptiveConfig {
            enabled: state.v3_durable_worker_batch_adaptive,
            batch_min: state.v3_durable_worker_batch_min,
            batch_max: state.v3_durable_worker_batch_max,
            wait_min: Duration::from_micros(state.v3_durable_worker_batch_wait_min_us.max(1)),
            wait_max: Duration::from_micros(state.v3_durable_worker_batch_wait_us.max(1)),
            low_util_pct: state.v3_durable_worker_batch_adaptive_low_util_pct,
            high_util_pct: state.v3_durable_worker_batch_adaptive_high_util_pct,
        },
        V3DurableWorkerPressureConfig::from_env(state.v3_durable_worker_inflight_hard_cap_pct),
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
async fn v3_rejects_when_startup_rebuild_is_in_progress() {
    let state = build_test_state();
    state
        .v3_startup_rebuild_in_progress
        .store(true, Ordering::Relaxed);

    let (status, Json(resp)) = handle_order_v3(
        State(state),
        headers("v3-startup-rebuild", Some("idem_v3_startup_rebuild")),
        Json(request_with_client_id("cid_v3_startup_rebuild")),
    )
    .await
    .unwrap_or_else(|_| panic!("v3 order submit failed"));

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.reason.as_deref(), Some("STARTUP_REBUILD_IN_PROGRESS"));
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
    let writer_handle = tokio::spawn(run_v3_single_writer(0, ingress_rx, state.clone()));
    let durable_handle = tokio::spawn(run_v3_durable_worker(
        0,
        durable_rx,
        state.clone(),
        state.v3_durable_worker_batch_max,
        state.v3_durable_worker_batch_wait_us,
        V3DurableWorkerBatchAdaptiveConfig {
            enabled: state.v3_durable_worker_batch_adaptive,
            batch_min: state.v3_durable_worker_batch_min,
            batch_max: state.v3_durable_worker_batch_max,
            wait_min: Duration::from_micros(state.v3_durable_worker_batch_wait_min_us.max(1)),
            wait_max: Duration::from_micros(state.v3_durable_worker_batch_wait_us.max(1)),
            low_util_pct: state.v3_durable_worker_batch_adaptive_low_util_pct,
            high_util_pct: state.v3_durable_worker_batch_adaptive_high_util_pct,
        },
        V3DurableWorkerPressureConfig::from_env(state.v3_durable_worker_inflight_hard_cap_pct),
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

    let metrics = metrics::handle_metrics(State(state.clone())).await;
    assert!(metrics.contains("gateway_v3_confirm_store_size "));
    assert!(metrics.contains("gateway_v3_confirm_lane_skew_pct "));

    writer_handle.abort();
    durable_handle.abort();
}

#[tokio::test]
async fn v3_integration_loss_monitor_updates_scan_cost_and_gc() {
    let (state, ingress_rx, _durable_rxs) = build_test_state_with_v3_pipeline(20, 40, 10, 1, 1_024);
    let writer_handle = tokio::spawn(run_v3_single_writer(0, ingress_rx, state.clone()));
    let monitor_handle = tokio::spawn(run_v3_loss_monitor(state.clone()));

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

    let metrics = metrics::handle_metrics(State(state.clone())).await;
    assert!(metrics.contains("gateway_v3_confirm_timeout_scan_cost_total "));
    assert!(metrics.contains("gateway_v3_confirm_gc_removed_total "));

    writer_handle.abort();
    monitor_handle.abort();
}

#[tokio::test]
async fn v3_integration_durable_queue_full_marks_loss_suspect() {
    let (state, ingress_rx, _durable_rxs) =
        build_test_state_with_v3_pipeline(5_000, 60_000, 20, 1, 1);
    let writer_handle = tokio::spawn(run_v3_single_writer(0, ingress_rx, state.clone()));

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
    let writer_handle = tokio::spawn(run_v3_single_writer(0, ingress_rx, state.clone()));

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
