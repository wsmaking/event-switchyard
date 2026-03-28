use super::*;
use crate::server::http::{
    V3DurableWorkerBatchAdaptiveConfig, V3DurableWorkerPressureConfig, metrics,
    run_v3_durable_worker, run_v3_single_writer, startup_rebuild, strategy, strategy_read,
};

#[tokio::test]
async fn strategy_config_endpoint_updates_and_rejects_stale_versions() {
    let state = build_test_state();

    let Json(initial) = strategy::handle_get_strategy_config(State(state.clone())).await;
    assert_eq!(initial.snapshot_id, "default");
    assert_eq!(initial.version, 0);

    let snapshot = ExecutionConfigSnapshot {
        snapshot_id: "snapshot-1".to_string(),
        version: 3,
        applied_at_ns: 123,
        shadow_enabled: true,
        ..ExecutionConfigSnapshot::default()
    };
    let Json(updated) =
        strategy::handle_put_strategy_config(State(state.clone()), Json(snapshot.clone()))
            .await
            .expect("update snapshot");
    assert_eq!(updated.snapshot_id, "snapshot-1");
    assert_eq!(updated.previous_version, 0);

    let Json(current) = strategy::handle_get_strategy_config(State(state.clone())).await;
    assert_eq!(current.snapshot_id, "snapshot-1");
    assert_eq!(current.version, 3);
    assert!(current.shadow_enabled);

    let stale = ExecutionConfigSnapshot {
        snapshot_id: "snapshot-0".to_string(),
        version: 2,
        applied_at_ns: 222,
        ..ExecutionConfigSnapshot::default()
    };
    let err = strategy::handle_put_strategy_config(State(state.clone()), Json(stale))
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

    let Json(upserted) =
        strategy::handle_post_shadow_record(State(state.clone()), Json(record.clone()))
            .await
            .expect("upsert shadow");
    assert_eq!(upserted.shadow_run_id, "shadow-1");
    assert_eq!(upserted.intent_id, "intent-1");
    assert!(!upserted.replaced);

    let Json(fetched) = strategy::handle_get_shadow_record(
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

    let Json(summary) =
        strategy::handle_get_shadow_run_summary(State(state.clone()), Path("shadow-2".to_string()))
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

    let Json(resp) = strategy::handle_post_strategy_intent_adapt(
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

    let err = strategy::handle_post_strategy_intent_adapt(
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
async fn strategy_intent_adapter_rejects_stale_alpha() {
    let state = build_test_state();
    let mut intent = strategy_intent_fixture();
    intent.decision_basis_at_ns = Some(1);
    intent.max_decision_age_ns = Some(1);

    let err = strategy::handle_post_strategy_intent_adapt(State(state.clone()), Json(intent))
        .await
        .expect_err("stale alpha must fail");

    assert_eq!(err.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(err.1.0.reason, "STRATEGY_INTENT_ALPHA_STALE");
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

    let Json(resp) =
        strategy::handle_post_strategy_intent_adapt(State(state.clone()), Json(intent))
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

    let Json(resp) = strategy::handle_post_strategy_intent_adapt(
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let Json(runtime_resp) = strategy_read::handle_get_strategy_runtime(
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
async fn strategy_intent_submit_rejects_stale_alpha() {
    let (state, _ingress_rx, _durable_rxs) =
        build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
    let mut intent = strategy_intent_fixture();
    intent.decision_basis_at_ns = Some(1);
    intent.max_decision_age_ns = Some(1);

    let err = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
            intent,
            shadow_run_id: None,
            predicted_policy: None,
            predicted_outcome: None,
        }),
    )
    .await
    .err()
    .expect("stale alpha submit must fail");

    assert_eq!(err.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(err.1.0.reason, "STRATEGY_INTENT_ALPHA_STALE");
}

#[tokio::test]
async fn strategy_intent_submit_rejects_while_startup_rebuild_is_in_progress() {
    let (state, _ingress_rx, _durable_rxs) =
        build_test_state_with_v3_pipeline(500, 60_000, 20, 1, 1_024);
    state
        .v3_startup_rebuild_in_progress
        .store(true, Ordering::Relaxed);

    let err = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
            intent: strategy_intent_fixture(),
            shadow_run_id: None,
            predicted_policy: None,
            predicted_outcome: None,
        }),
    )
    .await
    .err()
    .expect("startup rebuild must block strategy submit");

    assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.1.0.reason, "STRATEGY_STARTUP_REBUILD_IN_PROGRESS");
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

    let stats = startup_rebuild::rebuild_strategy_runtime_from_wal(&state, 1024);
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

    let stats = startup_rebuild::rebuild_strategy_runtime_from_wal(&state, 1024);
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
    runtime.runtime_mode = AlgoRuntimeMode::PauseOnRestart;
    fs::write(
        &wal_path,
        format!("{}\n", strategy_runtime_snapshot_line(&runtime, 1)),
    )
    .expect("write wal");

    let audit_log = Arc::new(AuditLog::new(&wal_path).expect("create audit log"));
    let (state, _ingress_rx, _durable_rxs) =
        build_test_state_with_v3_pipeline_and_audit_log(audit_log, 500, 60_000, 20, 1, 1_024);

    let stats = startup_rebuild::rebuild_strategy_runtime_from_wal(&state, 1024);
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let Json(resp) = strategy::handle_post_strategy_intent_shadow(
        State(state.clone()),
        Json(strategy::StrategyIntentShadowSeedRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let (status, Json(resp)) = strategy::handle_post_strategy_intent_submit(
        State(state.clone()),
        Json(strategy::StrategyIntentSubmitRequest {
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

    let metrics = metrics::handle_metrics(State(state.clone())).await;
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
