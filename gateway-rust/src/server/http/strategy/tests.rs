use crate::store::strategy_shadow_store::StrategyShadowScoreSample;

use super::{
    ShadowRecordUpsertResponse, ShadowRunSummaryResponse, StrategyConfigErrorResponse,
    StrategyConfigUpdateResponse, StrategyIntentAdaptResponse, StrategyIntentShadowSeedRequest,
    StrategyIntentSubmitRequest,
    support::{
        apply_execution_policy_to_order_request, effective_algo_runtime_mode,
        validate_legacy_recovery_policy,
    },
};

#[test]
fn update_response_serializes_as_camel_case() {
    let raw = serde_json::to_string(&StrategyConfigUpdateResponse {
        snapshot_id: "snapshot-1".to_string(),
        version: 2,
        previous_version: 1,
        applied_total: 3,
        applied_at_ns: 55,
    })
    .expect("serialize response");

    assert!(raw.contains("\"snapshotId\":\"snapshot-1\""));
    assert!(raw.contains("\"previousVersion\":1"));
}

#[test]
fn error_response_serializes_as_camel_case() {
    let raw = serde_json::to_string(&StrategyConfigErrorResponse {
        reason: "SNAPSHOT_VERSION_STALE".to_string(),
        current_snapshot_id: "snapshot-2".to_string(),
        current_version: 9,
        applied_total: 4,
        current_applied_at_ns: 88,
    })
    .expect("serialize error response");

    assert!(raw.contains("\"currentSnapshotId\":\"snapshot-2\""));
    assert!(raw.contains("\"currentVersion\":9"));
}

#[test]
fn shadow_upsert_response_serializes_as_camel_case() {
    let raw = serde_json::to_string(&ShadowRecordUpsertResponse {
        shadow_run_id: "shadow-1".to_string(),
        intent_id: "intent-7".to_string(),
        replaced: true,
        record_count: 3,
        upsert_total: 5,
    })
    .expect("serialize shadow upsert response");

    assert!(raw.contains("\"shadowRunId\":\"shadow-1\""));
    assert!(raw.contains("\"recordCount\":3"));
}

#[test]
fn shadow_run_summary_response_serializes_as_camel_case() {
    let raw = serde_json::to_string(&ShadowRunSummaryResponse {
        shadow_run_id: "shadow-1".to_string(),
        record_count: 3,
        pending_count: 1,
        matched_count: 1,
        skipped_count: 1,
        timed_out_count: 0,
        negative_score_count: 1,
        zero_score_count: 1,
        positive_score_count: 1,
        total_score_bps: 40,
        average_score_bps: 13.33,
        min_score_bps: -10,
        max_score_bps: 50,
        last_evaluated_at_ns: 999,
        top_positive: vec![StrategyShadowScoreSample {
            intent_id: "intent-top".to_string(),
            model_id: "model-top".to_string(),
            score_bps: 50,
            comparison_status: crate::strategy::shadow::ShadowComparisonStatus::Matched,
            evaluated_at_ns: 999,
        }],
        top_negative: vec![StrategyShadowScoreSample {
            intent_id: "intent-low".to_string(),
            model_id: "model-low".to_string(),
            score_bps: -10,
            comparison_status: crate::strategy::shadow::ShadowComparisonStatus::Pending,
            evaluated_at_ns: 777,
        }],
    })
    .expect("serialize shadow summary response");

    assert!(raw.contains("\"shadowRunId\":\"shadow-1\""));
    assert!(raw.contains("\"averageScoreBps\":13.33"));
    assert!(raw.contains("\"lastEvaluatedAtNs\":999"));
    assert!(raw.contains("\"topPositive\""));
    assert!(raw.contains("\"topNegative\""));
}

#[test]
fn intent_adapt_response_serializes_as_camel_case() {
    let raw = serde_json::to_string(&StrategyIntentAdaptResponse {
        snapshot_id: "snapshot-1".to_string(),
        version: 2,
        adapted_at_ns: 55,
        order_request: crate::order::OrderRequest {
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: crate::order::OrderType::Limit,
            qty: 100,
            price: Some(15_000),
            time_in_force: crate::order::TimeInForce::Gtc,
            expire_at: None,
            client_order_id: None,
            intent_id: Some("intent-1".to_string()),
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-1".to_string()),
            decision_attempt_seq: Some(1),
        },
        algo_plan: None,
        effective_policy: crate::strategy::shadow::ShadowPolicyView {
            execution_policy: None,
            urgency: Some("HIGH".to_string()),
            venue: Some("NASDAQ".to_string()),
        },
        effective_risk_budget_ref: Some("budget-1".to_string()),
        policy_adjustments: vec!["PASSIVE_NORMALIZED_TO_GTC".to_string()],
        shadow_enabled: true,
    })
    .expect("serialize adapt response");

    assert!(raw.contains("\"adaptedAtNs\":55"));
    assert!(raw.contains("\"effectiveRiskBudgetRef\":\"budget-1\""));
    assert!(raw.contains("\"policyAdjustments\":[\"PASSIVE_NORMALIZED_TO_GTC\"]"));
    assert!(raw.contains("\"shadowEnabled\":true"));
}

#[test]
fn intent_shadow_seed_request_round_trips_json() {
    let raw = serde_json::to_string(&StrategyIntentShadowSeedRequest {
        shadow_run_id: "shadow-1".to_string(),
        intent: crate::strategy::intent::StrategyIntent {
            schema_version: crate::strategy::intent::STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "intent-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: crate::order::OrderType::Limit,
            qty: 100,
            limit_price: Some(15_000),
            time_in_force: crate::order::TimeInForce::Gtc,
            urgency: crate::strategy::intent::IntentUrgency::Normal,
            execution_policy: crate::strategy::intent::ExecutionPolicyKind::Passive,
            risk_budget_ref: None,
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-1".to_string()),
            decision_attempt_seq: Some(1),
            decision_basis_at_ns: Some(10),
            max_decision_age_ns: Some(100),
            market_snapshot_id: Some("market-1".to_string()),
            signal_id: Some("signal-1".to_string()),
            recovery_policy: None,
            algo: None,
            created_at_ns: 10,
            expires_at_ns: 20,
        },
        predicted_policy: Some(crate::strategy::shadow::ShadowPolicyView {
            execution_policy: None,
            urgency: Some("HIGH".to_string()),
            venue: Some("NASDAQ".to_string()),
        }),
        predicted_outcome: Some(crate::strategy::shadow::ShadowOutcomeView {
            final_status: Some("VOLATILE_ACCEPT".to_string()),
            reject_reason: None,
            accepted_at_ns: Some(10),
            durable_at_ns: None,
        }),
    })
    .expect("serialize shadow seed request");

    assert!(raw.contains("\"shadowRunId\":\"shadow-1\""));
    assert!(raw.contains("\"predictedOutcome\""));
}

#[test]
fn intent_submit_request_round_trips_json() {
    let raw = serde_json::to_string(&StrategyIntentSubmitRequest {
        intent: crate::strategy::intent::StrategyIntent {
            schema_version: crate::strategy::intent::STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "intent-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: crate::order::OrderType::Limit,
            qty: 100,
            limit_price: Some(15_000),
            time_in_force: crate::order::TimeInForce::Gtc,
            urgency: crate::strategy::intent::IntentUrgency::Normal,
            execution_policy: crate::strategy::intent::ExecutionPolicyKind::Passive,
            risk_budget_ref: None,
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-1".to_string()),
            decision_attempt_seq: Some(1),
            decision_basis_at_ns: Some(10),
            max_decision_age_ns: Some(100),
            market_snapshot_id: Some("market-1".to_string()),
            signal_id: Some("signal-1".to_string()),
            recovery_policy: None,
            algo: None,
            created_at_ns: 10,
            expires_at_ns: 20,
        },
        shadow_run_id: Some("shadow-1".to_string()),
        predicted_policy: None,
        predicted_outcome: None,
    })
    .expect("serialize submit request");

    assert!(raw.contains("\"shadowRunId\":\"shadow-1\""));
    assert!(!raw.contains("recoveryPolicy"));
}

fn strategy_intent_fixture() -> crate::strategy::intent::StrategyIntent {
    let decision_basis_at_ns = gateway_core::now_nanos();
    crate::strategy::intent::StrategyIntent {
        schema_version: crate::strategy::intent::STRATEGY_INTENT_SCHEMA_VERSION,
        intent_id: "intent-1".to_string(),
        account_id: "acc-1".to_string(),
        session_id: "sess-1".to_string(),
        symbol: "AAPL".to_string(),
        side: "BUY".to_string(),
        order_type: crate::order::OrderType::Limit,
        qty: 100,
        limit_price: Some(15_000),
        time_in_force: crate::order::TimeInForce::Gtd,
        urgency: crate::strategy::intent::IntentUrgency::Normal,
        execution_policy: crate::strategy::intent::ExecutionPolicyKind::Aggressive,
        risk_budget_ref: None,
        model_id: Some("model-1".to_string()),
        execution_run_id: Some("run-1".to_string()),
        decision_key: Some("decision-1".to_string()),
        decision_attempt_seq: Some(1),
        decision_basis_at_ns: Some(decision_basis_at_ns),
        max_decision_age_ns: Some(60_000_000_000),
        market_snapshot_id: Some("market-1".to_string()),
        signal_id: Some("signal-1".to_string()),
        recovery_policy: None,
        algo: None,
        created_at_ns: 10,
        expires_at_ns: 123_000_000,
    }
}

fn algo_plan_fixture() -> crate::strategy::algo::AlgoExecutionPlan {
    crate::strategy::algo::AlgoExecutionPlan {
        schema_version: crate::strategy::algo::STRATEGY_ALGO_PLAN_SCHEMA_VERSION,
        parent_intent_id: "intent-1".to_string(),
        policy: crate::strategy::intent::ExecutionPolicyKind::Twap,
        total_qty: 100,
        child_count: 4,
        start_at_ns: 10,
        slice_interval_ns: 1_000,
        slices: vec![],
    }
}

#[test]
fn execution_policy_hook_normalizes_passive_immediate_tif_to_gtc() {
    let mut intent = strategy_intent_fixture();
    intent.execution_policy = crate::strategy::intent::ExecutionPolicyKind::Passive;
    intent.time_in_force = crate::order::TimeInForce::Ioc;
    let effective_policy = crate::strategy::shadow::ShadowPolicyView {
        execution_policy: Some(crate::strategy::config::ExecutionPolicyConfig {
            policy: crate::strategy::intent::ExecutionPolicyKind::Passive,
            prefer_passive: true,
            post_only: false,
            max_slippage_bps: None,
            participation_rate_bps: None,
        }),
        urgency: None,
        venue: None,
    };

    let applied = apply_execution_policy_to_order_request(&intent, &effective_policy)
        .expect("passive policy should adapt");

    assert_eq!(
        applied.order_request.order_type,
        crate::order::OrderType::Limit
    );
    assert_eq!(
        applied.order_request.time_in_force,
        crate::order::TimeInForce::Gtc
    );
    assert_eq!(applied.order_request.expire_at, None);
    assert_eq!(applied.adjustments, vec!["PASSIVE_NORMALIZED_TO_GTC"]);
}

#[test]
fn execution_policy_hook_rejects_passive_market_order() {
    let mut intent = strategy_intent_fixture();
    intent.order_type = crate::order::OrderType::Market;
    intent.limit_price = None;
    intent.execution_policy = crate::strategy::intent::ExecutionPolicyKind::Passive;
    let effective_policy = crate::strategy::shadow::ShadowPolicyView {
        execution_policy: Some(crate::strategy::config::ExecutionPolicyConfig {
            policy: crate::strategy::intent::ExecutionPolicyKind::Passive,
            prefer_passive: true,
            post_only: false,
            max_slippage_bps: None,
            participation_rate_bps: None,
        }),
        urgency: None,
        venue: None,
    };

    let err = apply_execution_policy_to_order_request(&intent, &effective_policy)
        .err()
        .expect("passive market must reject");

    assert_eq!(err, "STRATEGY_POLICY_PASSIVE_REQUIRES_LIMIT");
}

#[test]
fn execution_policy_hook_normalizes_aggressive_market_time_in_force() {
    let mut intent = strategy_intent_fixture();
    intent.order_type = crate::order::OrderType::Market;
    intent.limit_price = None;
    intent.time_in_force = crate::order::TimeInForce::Gtd;
    let effective_policy = crate::strategy::shadow::ShadowPolicyView {
        execution_policy: Some(crate::strategy::config::ExecutionPolicyConfig {
            policy: crate::strategy::intent::ExecutionPolicyKind::Aggressive,
            prefer_passive: false,
            post_only: false,
            max_slippage_bps: Some(5),
            participation_rate_bps: None,
        }),
        urgency: None,
        venue: None,
    };

    let applied = apply_execution_policy_to_order_request(&intent, &effective_policy)
        .expect("aggressive market should adapt");

    assert_eq!(
        applied.order_request.time_in_force,
        crate::order::TimeInForce::Ioc
    );
    assert_eq!(applied.order_request.expire_at, None);
    assert_eq!(
        applied.adjustments,
        vec!["AGGRESSIVE_MARKET_NORMALIZED_TO_IOC"]
    );
}

#[test]
fn execution_policy_hook_allows_algo_policy_to_pass_through() {
    let mut intent = strategy_intent_fixture();
    intent.execution_policy = crate::strategy::intent::ExecutionPolicyKind::Twap;
    let effective_policy = crate::strategy::shadow::ShadowPolicyView {
        execution_policy: Some(crate::strategy::config::ExecutionPolicyConfig {
            policy: crate::strategy::intent::ExecutionPolicyKind::Twap,
            prefer_passive: false,
            post_only: false,
            max_slippage_bps: None,
            participation_rate_bps: Some(250),
        }),
        urgency: None,
        venue: None,
    };

    let applied = apply_execution_policy_to_order_request(&intent, &effective_policy)
        .expect("algo policy should pass through");

    assert_eq!(applied.order_request.qty, 100);
    assert!(applied.adjustments.is_empty());
}

#[test]
fn legacy_recovery_policy_is_rejected_for_alpha_intents() {
    let mut intent = strategy_intent_fixture();
    intent.recovery_policy =
        Some(crate::strategy::intent::StrategyRecoveryPolicy::GatewayManagedResume);

    assert_eq!(
        validate_legacy_recovery_policy(&intent),
        Err("STRATEGY_RECOVERY_POLICY_DEPRECATED")
    );
}

#[test]
fn legacy_recovery_policy_is_rejected_for_algo_intents_too() {
    let mut intent = strategy_intent_fixture();
    intent.execution_policy = crate::strategy::intent::ExecutionPolicyKind::Twap;
    intent.recovery_policy =
        Some(crate::strategy::intent::StrategyRecoveryPolicy::GatewayManagedResume);

    assert_eq!(
        validate_legacy_recovery_policy(&intent),
        Err("STRATEGY_RECOVERY_POLICY_DEPRECATED")
    );
}

#[test]
fn algo_defaults_to_gateway_managed_runtime_mode() {
    let plan = algo_plan_fixture();

    assert_eq!(
        effective_algo_runtime_mode(&plan),
        crate::strategy::runtime::AlgoRuntimeMode::GatewayManagedResume
    );
}
