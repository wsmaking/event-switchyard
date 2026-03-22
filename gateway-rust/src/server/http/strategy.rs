mod write;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

use crate::audit::{self, AuditEvent};
use crate::order::{OrderRequest, OrderType, TimeInForce};
use crate::store::strategy_shadow_store::StrategyShadowScoreSample;
use crate::strategy::algo::{AlgoExecutionPlan, build_algo_execution_plan};
use crate::strategy::config::ExecutionConfigSnapshot;
use crate::strategy::intent::{IntentUrgency, StrategyIntent};
use crate::strategy::runtime::{
    AlgoChildExecution, AlgoParentExecution, AlgoRuntimeMode,
    STRATEGY_ALGO_RUNTIME_SNAPSHOT_EVENT_TYPE,
};
use crate::strategy::shadow::{
    SHADOW_RECORD_SCHEMA_VERSION, ShadowOutcomeView, ShadowPolicyView, ShadowRecord,
};

use super::{AppState, orders::VolatileOrderResponse};
pub(super) use write::{
    handle_get_shadow_record, handle_get_shadow_run_summary, handle_get_strategy_config,
    handle_post_shadow_record, handle_post_strategy_intent_adapt,
    handle_post_strategy_intent_shadow, handle_post_strategy_intent_submit,
    handle_put_strategy_config,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StrategyConfigUpdateResponse {
    pub snapshot_id: String,
    pub version: u64,
    pub previous_version: u64,
    pub applied_total: u64,
    pub applied_at_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StrategyConfigErrorResponse {
    pub reason: String,
    pub current_snapshot_id: String,
    pub current_version: u64,
    pub applied_total: u64,
    pub current_applied_at_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ShadowRecordUpsertResponse {
    pub shadow_run_id: String,
    pub intent_id: String,
    pub replaced: bool,
    pub record_count: u64,
    pub upsert_total: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ShadowRunSummaryResponse {
    pub shadow_run_id: String,
    pub record_count: u64,
    pub pending_count: u64,
    pub matched_count: u64,
    pub skipped_count: u64,
    pub timed_out_count: u64,
    pub negative_score_count: u64,
    pub zero_score_count: u64,
    pub positive_score_count: u64,
    pub total_score_bps: i64,
    pub average_score_bps: f64,
    pub min_score_bps: i64,
    pub max_score_bps: i64,
    pub last_evaluated_at_ns: u64,
    pub top_positive: Vec<StrategyShadowScoreSample>,
    pub top_negative: Vec<StrategyShadowScoreSample>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StrategyIntentAdaptResponse {
    pub snapshot_id: String,
    pub version: u64,
    pub adapted_at_ns: u64,
    pub order_request: OrderRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algo_plan: Option<AlgoExecutionPlan>,
    pub effective_policy: ShadowPolicyView,
    pub effective_risk_budget_ref: Option<String>,
    pub policy_adjustments: Vec<String>,
    pub shadow_enabled: bool,
}

#[derive(Debug, Clone, serde::Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StrategyIntentSubmitRequest {
    pub intent: StrategyIntent,
    #[serde(default)]
    pub shadow_run_id: Option<String>,
    #[serde(default)]
    pub predicted_policy: Option<ShadowPolicyView>,
    #[serde(default)]
    pub predicted_outcome: Option<ShadowOutcomeView>,
}

#[derive(Debug, Clone, serde::Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StrategyIntentShadowSeedRequest {
    pub shadow_run_id: String,
    pub intent: StrategyIntent,
    #[serde(default)]
    pub predicted_policy: Option<ShadowPolicyView>,
    #[serde(default)]
    pub predicted_outcome: Option<ShadowOutcomeView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StrategyIntentSubmitResponse {
    pub snapshot_id: String,
    pub version: u64,
    pub submitted_at_ns: u64,
    pub order_request: OrderRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algo_plan: Option<AlgoExecutionPlan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algo_runtime: Option<AlgoParentExecution>,
    pub effective_policy: ShadowPolicyView,
    pub effective_risk_budget_ref: Option<String>,
    pub policy_adjustments: Vec<String>,
    pub shadow_enabled: bool,
    pub shadow_run_id: Option<String>,
    pub shadow_seeded: bool,
    pub volatile_order: VolatileOrderResponse,
}

#[derive(Debug)]
struct AppliedExecutionPolicy {
    order_request: OrderRequest,
    adjustments: Vec<&'static str>,
}

impl AppliedExecutionPolicy {
    fn adjustment_strings(&self) -> Vec<String> {
        self.adjustments
            .iter()
            .map(|value| (*value).to_string())
            .collect()
    }
}

fn snapshot_error(
    state: &AppState,
    snapshot: &ExecutionConfigSnapshot,
    status: StatusCode,
    reason: &'static str,
) -> (StatusCode, Json<StrategyConfigErrorResponse>) {
    (
        status,
        Json(StrategyConfigErrorResponse {
            reason: reason.to_string(),
            current_snapshot_id: snapshot.snapshot_id.clone(),
            current_version: snapshot.version,
            applied_total: state.strategy_snapshot_store.applied_total(),
            current_applied_at_ns: snapshot.applied_at_ns,
        }),
    )
}

fn intent_urgency_name(urgency: IntentUrgency) -> String {
    match urgency {
        IntentUrgency::Low => "LOW",
        IntentUrgency::Normal => "NORMAL",
        IntentUrgency::High => "HIGH",
        IntentUrgency::Critical => "CRITICAL",
    }
    .to_string()
}

fn resolve_effective_policy(
    snapshot: &ExecutionConfigSnapshot,
    intent: &StrategyIntent,
) -> ShadowPolicyView {
    let mut execution_policy = snapshot.default_execution_policy.clone();
    let symbol_override = snapshot.symbol_override(&intent.symbol);
    if let Some(override_cfg) = symbol_override.and_then(|cfg| cfg.execution_policy.clone()) {
        execution_policy = override_cfg;
    }
    if !matches!(
        intent.execution_policy,
        crate::strategy::intent::ExecutionPolicyKind::Default
    ) {
        execution_policy.policy = intent.execution_policy;
    }

    let urgency = snapshot
        .urgency_overrides
        .iter()
        .find(|override_cfg| override_cfg.account_id == intent.account_id)
        .map(|override_cfg| override_cfg.urgency)
        .or_else(|| symbol_override.and_then(|cfg| cfg.urgency_override))
        .unwrap_or(intent.urgency);

    let venue = snapshot
        .venue_preference
        .iter()
        .filter(|preference| preference.symbol == intent.symbol)
        .min_by_key(|preference| preference.rank)
        .map(|preference| preference.venue.clone());

    ShadowPolicyView {
        execution_policy: Some(execution_policy),
        urgency: Some(intent_urgency_name(urgency)),
        venue,
    }
}

fn resolve_effective_risk_budget_ref(
    snapshot: &ExecutionConfigSnapshot,
    intent: &StrategyIntent,
) -> Option<String> {
    intent
        .risk_budget_ref
        .as_ref()
        .map(|reference| reference.budget_id.clone())
        .or_else(|| {
            snapshot
                .account_risk_budget(&intent.account_id)
                .and_then(|budget| budget.budget_ref.clone())
        })
}

fn validate_intent_against_snapshot(
    snapshot: &ExecutionConfigSnapshot,
    intent: &StrategyIntent,
    now_ns: u64,
) -> Result<(), &'static str> {
    if snapshot.is_stale_at(now_ns) {
        return Err("STRATEGY_SNAPSHOT_STALE");
    }

    if let Some(override_cfg) = snapshot.symbol_override(&intent.symbol) {
        if let Some(max_order_qty) = override_cfg.max_order_qty {
            if intent.qty > max_order_qty {
                return Err("STRATEGY_SYMBOL_MAX_ORDER_QTY");
            }
        }
        if let Some(max_notional) = override_cfg.max_notional {
            if intent.limit_price.unwrap_or(0).saturating_mul(intent.qty) > max_notional
                && intent.limit_price.unwrap_or(0) > 0
            {
                return Err("STRATEGY_SYMBOL_MAX_NOTIONAL");
            }
        }
    }

    if let Some(account_budget) = snapshot.account_risk_budget(&intent.account_id) {
        if let Some(max_notional) = account_budget.max_notional {
            if intent.limit_price.unwrap_or(0).saturating_mul(intent.qty) > max_notional
                && intent.limit_price.unwrap_or(0) > 0
            {
                return Err("STRATEGY_ACCOUNT_MAX_NOTIONAL");
            }
        }
    }

    Ok(())
}

fn validate_intent_freshness(intent: &StrategyIntent, now_ns: u64) -> Result<(), &'static str> {
    intent.validate_alpha_freshness(now_ns)
}

fn adapt_order_request(intent: &StrategyIntent) -> OrderRequest {
    let expire_at = if matches!(intent.time_in_force, TimeInForce::Gtd) {
        Some(intent.expires_at_ns / 1_000_000)
    } else {
        None
    };
    OrderRequest {
        symbol: intent.symbol.clone(),
        side: intent.side.to_ascii_uppercase(),
        order_type: intent.order_type,
        qty: intent.qty,
        price: intent.limit_price,
        time_in_force: intent.time_in_force,
        expire_at,
        client_order_id: None,
        intent_id: Some(intent.intent_id.clone()),
        model_id: intent.model_id.clone(),
        execution_run_id: intent.execution_run_id.clone(),
        decision_key: intent.decision_key.clone(),
        decision_attempt_seq: intent.decision_attempt_seq,
    }
}

fn effective_algo_runtime_mode(algo_plan: &AlgoExecutionPlan) -> AlgoRuntimeMode {
    match algo_plan.policy {
        crate::strategy::intent::ExecutionPolicyKind::Twap
        | crate::strategy::intent::ExecutionPolicyKind::Vwap
        | crate::strategy::intent::ExecutionPolicyKind::Pov => {
            AlgoRuntimeMode::GatewayManagedResume
        }
        _ => AlgoRuntimeMode::PauseOnRestart,
    }
}

fn validate_legacy_recovery_policy(intent: &StrategyIntent) -> Result<(), &'static str> {
    if intent.recovery_policy.is_some() {
        return Err("STRATEGY_RECOVERY_POLICY_DEPRECATED");
    }
    Ok(())
}

fn normalize_resting_time_in_force(order_request: &mut OrderRequest) -> bool {
    if matches!(
        order_request.time_in_force,
        TimeInForce::Ioc | TimeInForce::Fok
    ) {
        order_request.time_in_force = TimeInForce::Gtc;
        order_request.expire_at = None;
        return true;
    }
    false
}

fn normalize_aggressive_market_time_in_force(order_request: &mut OrderRequest) -> bool {
    if order_request.order_type == OrderType::Market
        && !matches!(
            order_request.time_in_force,
            TimeInForce::Ioc | TimeInForce::Fok
        )
    {
        order_request.time_in_force = TimeInForce::Ioc;
        order_request.expire_at = None;
        return true;
    }
    false
}

fn apply_execution_policy_to_order_request(
    intent: &StrategyIntent,
    effective_policy: &ShadowPolicyView,
) -> Result<AppliedExecutionPolicy, &'static str> {
    let mut order_request = adapt_order_request(intent);
    let mut adjustments = Vec::new();
    let Some(policy_cfg) = effective_policy.execution_policy.as_ref() else {
        return Ok(AppliedExecutionPolicy {
            order_request,
            adjustments,
        });
    };

    if policy_cfg.post_only {
        if order_request.order_type != OrderType::Limit {
            return Err("STRATEGY_POLICY_POST_ONLY_REQUIRES_LIMIT");
        }
        if normalize_resting_time_in_force(&mut order_request) {
            adjustments.push("POST_ONLY_NORMALIZED_TO_GTC");
        }
    }

    match policy_cfg.policy {
        crate::strategy::intent::ExecutionPolicyKind::Default => {}
        crate::strategy::intent::ExecutionPolicyKind::Passive => {
            if order_request.order_type != OrderType::Limit {
                return Err("STRATEGY_POLICY_PASSIVE_REQUIRES_LIMIT");
            }
            if normalize_resting_time_in_force(&mut order_request) {
                adjustments.push("PASSIVE_NORMALIZED_TO_GTC");
            }
        }
        crate::strategy::intent::ExecutionPolicyKind::Aggressive => {
            if normalize_aggressive_market_time_in_force(&mut order_request) {
                adjustments.push("AGGRESSIVE_MARKET_NORMALIZED_TO_IOC");
            }
        }
        crate::strategy::intent::ExecutionPolicyKind::Twap
        | crate::strategy::intent::ExecutionPolicyKind::Vwap
        | crate::strategy::intent::ExecutionPolicyKind::Pov => {}
    }

    Ok(AppliedExecutionPolicy {
        order_request,
        adjustments,
    })
}

fn upsert_shadow_record_for_intent(
    state: &AppState,
    snapshot: &ExecutionConfigSnapshot,
    shadow_run_id: &str,
    intent: &StrategyIntent,
    predicted_policy: Option<ShadowPolicyView>,
    predicted_outcome: Option<ShadowOutcomeView>,
    now_ns: u64,
) -> Result<bool, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    if shadow_run_id.trim().is_empty() {
        return Err(snapshot_error(
            state,
            snapshot,
            StatusCode::UNPROCESSABLE_ENTITY,
            "SHADOW_RUN_ID_REQUIRED",
        ));
    }

    let model_id = intent
        .model_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            snapshot_error(
                state,
                snapshot,
                StatusCode::UNPROCESSABLE_ENTITY,
                "MODEL_ID_REQUIRED",
            )
        })?;

    let actual_policy = resolve_effective_policy(snapshot, intent);
    let mut record = ShadowRecord {
        schema_version: SHADOW_RECORD_SCHEMA_VERSION,
        shadow_run_id: shadow_run_id.to_string(),
        model_id,
        intent_id: intent.intent_id.clone(),
        session_id: intent.session_id.clone(),
        session_seq: 0,
        predicted_policy,
        actual_policy: Some(actual_policy),
        predicted_outcome,
        actual_outcome: None,
        score_components: Vec::new(),
        evaluated_at_ns: now_ns,
        comparison_status: crate::strategy::shadow::ShadowComparisonStatus::Pending,
    };
    record.recompute_score_components();

    state
        .strategy_shadow_store
        .upsert(record)
        .map(|previous| previous.is_some())
        .map_err(|reason| snapshot_error(state, snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason))
}

fn build_algo_parent_execution(
    request: &StrategyIntentSubmitRequest,
    base_order_request: OrderRequest,
    effective_policy: &ShadowPolicyView,
    effective_risk_budget_ref: Option<String>,
    algo_plan: &AlgoExecutionPlan,
    now_ns: u64,
) -> AlgoParentExecution {
    AlgoParentExecution::from_plan(
        algo_plan,
        request.intent.account_id.clone(),
        request.intent.session_id.clone(),
        request.intent.symbol.clone(),
        request.intent.model_id.clone(),
        request.intent.execution_run_id.clone(),
        effective_algo_runtime_mode(algo_plan),
        base_order_request,
        effective_risk_budget_ref,
        Some(effective_policy.clone()),
        request.shadow_run_id.clone(),
        now_ns,
    )
}

fn build_algo_runtime_snapshot_event(execution: &AlgoParentExecution) -> Option<AuditEvent> {
    let data = serde_json::to_value(execution).ok()?;
    Some(AuditEvent {
        event_type: STRATEGY_ALGO_RUNTIME_SNAPSHOT_EVENT_TYPE.to_string(),
        at: audit::now_millis(),
        account_id: execution.account_id.clone(),
        order_id: None,
        data,
    })
}

pub(super) fn append_algo_runtime_snapshot(state: &AppState, execution: &AlgoParentExecution) {
    if let Some(event) = build_algo_runtime_snapshot_event(execution) {
        state.audit_log.append(event);
    }
}

async fn append_algo_runtime_snapshot_durable(
    state: &AppState,
    execution: &AlgoParentExecution,
    request_start_ns: u64,
) -> Result<(), &'static str> {
    let Some(event) = build_algo_runtime_snapshot_event(execution) else {
        return Err("STRATEGY_ALGO_RUNTIME_WAL_SERIALIZE_FAILED");
    };
    let append = state
        .audit_log
        .append_with_durable_receipt(event, request_start_ns);
    if append.timings.durable_done_ns > 0 {
        return Ok(());
    }
    let Some(durable_rx) = append.durable_rx else {
        return Err("STRATEGY_ALGO_RUNTIME_WAL_FAILED");
    };
    let timeout = Duration::from_millis(state.v2_durable_wait_timeout_ms.max(1));
    match tokio::time::timeout(timeout, durable_rx).await {
        Ok(Ok(receipt)) if receipt.durable_done_ns > 0 => Ok(()),
        Ok(Ok(_)) => Err("STRATEGY_ALGO_RUNTIME_WAL_FAILED"),
        Ok(Err(_)) => Err("STRATEGY_ALGO_RUNTIME_WAL_RECEIPT_CLOSED"),
        Err(_) => Err("STRATEGY_ALGO_RUNTIME_WAL_TIMEOUT"),
    }
}

fn publish_parent_runtime_feedback(
    state: &AppState,
    event: crate::strategy::feedback::FeedbackEvent,
) {
    super::publish_quant_feedback(
        state,
        event
            .push_path_tag("strategy")
            .push_path_tag("algo_parent")
            .push_path_tag("feedback"),
    );
}

async fn dispatch_algo_slice(
    state: AppState,
    account_id: String,
    session_id: String,
    base_order_request: OrderRequest,
    actual_policy: Option<ShadowPolicyView>,
    effective_risk_budget_ref: Option<String>,
    slice: AlgoChildExecution,
) {
    let now_ns = gateway_core::now_nanos();
    if slice.send_at_ns > now_ns {
        tokio::time::sleep(Duration::from_nanos(slice.send_at_ns - now_ns)).await;
    }

    if !state
        .strategy_runtime_store
        .can_dispatch_child(&slice.child_intent_id)
    {
        let _ = state.strategy_runtime_store.mark_child_skipped(
            &slice.child_intent_id,
            gateway_core::now_nanos(),
            "STRATEGY_PARENT_RUNTIME_TERMINAL",
        );
        if let Some(parent) = state
            .strategy_runtime_store
            .get_by_child(&slice.child_intent_id)
        {
            append_algo_runtime_snapshot(&state, &parent);
        }
        return;
    }

    let dispatch_started_at_ns = gateway_core::now_nanos();
    if !state
        .strategy_runtime_store
        .record_child_dispatch_started(&slice.child_intent_id, dispatch_started_at_ns)
    {
        return;
    }
    let Some(parent_after_dispatch) = state
        .strategy_runtime_store
        .get_by_child(&slice.child_intent_id)
    else {
        return;
    };
    if let Err(reason) =
        append_algo_runtime_snapshot_durable(&state, &parent_after_dispatch, dispatch_started_at_ns)
            .await
    {
        let parent_event = state.strategy_runtime_store.record_child_rejected(
            &slice.child_intent_id,
            gateway_core::now_nanos(),
            reason,
        );
        if let Some(parent) = state
            .strategy_runtime_store
            .get_by_child(&slice.child_intent_id)
        {
            append_algo_runtime_snapshot(&state, &parent);
        }
        if let Some(event) = parent_event {
            publish_parent_runtime_feedback(&state, event);
        }
        return;
    }

    let mut child_request = base_order_request;
    child_request.qty = slice.qty;
    child_request.client_order_id = None;
    child_request.intent_id = Some(slice.child_intent_id.clone());
    child_request.decision_key = Some(slice.child_intent_id.clone());
    child_request.decision_attempt_seq = Some(1);
    let submitted_at_ns = gateway_core::now_nanos();
    let (status, volatile_order) = super::orders::process_order_v3_hot_path_with_strategy_context(
        &state,
        &account_id,
        &session_id,
        child_request,
        actual_policy.clone().map(Arc::new),
        effective_risk_budget_ref.clone().map(Arc::<str>::from),
        submitted_at_ns,
    );

    let parent_event =
        if status == StatusCode::ACCEPTED && volatile_order.status_text() == "VOLATILE_ACCEPT" {
            volatile_order.session_seq().and_then(|session_seq| {
                state.strategy_runtime_store.record_child_volatile_accepted(
                    &slice.child_intent_id,
                    session_seq,
                    submitted_at_ns,
                )
            })
        } else {
            state.strategy_runtime_store.record_child_rejected(
                &slice.child_intent_id,
                submitted_at_ns,
                volatile_order
                    .reason_text()
                    .unwrap_or("STRATEGY_ALGO_CHILD_REJECTED"),
            )
        };

    if let Some(parent) = state
        .strategy_runtime_store
        .get_by_child(&slice.child_intent_id)
    {
        append_algo_runtime_snapshot(&state, &parent);
    }
    if let Some(event) = parent_event {
        publish_parent_runtime_feedback(&state, event);
    }
}

pub(super) fn spawn_algo_runtime(state: AppState, runtime: &AlgoParentExecution) {
    for slice in runtime
        .slices
        .iter()
        .filter(|slice| {
            matches!(
                slice.status,
                crate::strategy::runtime::AlgoChildStatus::Scheduled
            )
        })
        .cloned()
    {
        tokio::spawn(dispatch_algo_slice(
            state.clone(),
            runtime.account_id.clone(),
            runtime.session_id.clone(),
            runtime.base_order_request.clone(),
            runtime.actual_policy.clone(),
            runtime.effective_risk_budget_ref.clone(),
            slice,
        ));
    }
}

#[cfg(test)]
mod tests {
    use crate::store::strategy_shadow_store::StrategyShadowScoreSample;

    use super::{
        ShadowRecordUpsertResponse, ShadowRunSummaryResponse, StrategyConfigErrorResponse,
        StrategyConfigUpdateResponse, StrategyIntentAdaptResponse, StrategyIntentShadowSeedRequest,
        StrategyIntentSubmitRequest,
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

        let applied = super::apply_execution_policy_to_order_request(&intent, &effective_policy)
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

        let err = super::apply_execution_policy_to_order_request(&intent, &effective_policy)
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

        let applied = super::apply_execution_policy_to_order_request(&intent, &effective_policy)
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

        let applied = super::apply_execution_policy_to_order_request(&intent, &effective_policy)
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
            super::validate_legacy_recovery_policy(&intent),
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
            super::validate_legacy_recovery_policy(&intent),
            Err("STRATEGY_RECOVERY_POLICY_DEPRECATED")
        );
    }

    #[test]
    fn algo_defaults_to_gateway_managed_runtime_mode() {
        let plan = algo_plan_fixture();

        assert_eq!(
            super::effective_algo_runtime_mode(&plan),
            crate::strategy::runtime::AlgoRuntimeMode::GatewayManagedResume
        );
    }
}
