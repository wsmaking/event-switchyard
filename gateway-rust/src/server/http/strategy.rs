use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Serialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Duration;

use crate::audit::{self, AuditEvent};
use crate::order::{OrderRequest, OrderType, TimeInForce};
use crate::store::strategy_shadow_store::StrategyShadowScoreSample;
use crate::strategy::algo::{AlgoExecutionPlan, build_algo_execution_plan};
use crate::strategy::config::ExecutionConfigSnapshot;
use crate::strategy::intent::{IntentUrgency, StrategyIntent, StrategyRecoveryPolicy};
use crate::strategy::replay::{
    STRATEGY_EXECUTION_FACT_EVENT_TYPE, StrategyExecutionCatchupInput, StrategyExecutionFact,
    StrategyExecutionReplayItem,
};
use crate::strategy::runtime::{
    AlgoChildExecution, AlgoParentExecution, STRATEGY_ALGO_RUNTIME_SNAPSHOT_EVENT_TYPE,
};
use crate::strategy::shadow::{
    SHADOW_RECORD_SCHEMA_VERSION, ShadowOutcomeView, ShadowPolicyView, ShadowRecord,
};

use super::{AppState, orders::VolatileOrderResponse};

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

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StrategyReplayQuery {
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub after_cursor: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StrategyExecutionReplayResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    pub requested_after_cursor: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<u64>,
    pub has_more: bool,
    pub fact_count: usize,
    pub facts: Vec<StrategyExecutionReplayItem>,
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

fn effective_recovery_policy(
    intent: &StrategyIntent,
    algo_plan: &AlgoExecutionPlan,
) -> StrategyRecoveryPolicy {
    intent.recovery_policy.unwrap_or({
        if matches!(
            algo_plan.policy,
            crate::strategy::intent::ExecutionPolicyKind::Twap
                | crate::strategy::intent::ExecutionPolicyKind::Vwap
                | crate::strategy::intent::ExecutionPolicyKind::Pov
        ) {
            StrategyRecoveryPolicy::GatewayManagedResume
        } else {
            StrategyRecoveryPolicy::NoAutoResume
        }
    })
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
        effective_recovery_policy(&request.intent, algo_plan),
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

fn collect_strategy_execution_facts_from_reader<R, F>(
    reader: R,
    after_cursor: u64,
    limit: usize,
    mut predicate: F,
) -> (Vec<StrategyExecutionReplayItem>, bool)
where
    R: BufRead,
    F: FnMut(&StrategyExecutionFact) -> bool,
{
    let limit = limit.clamp(1, 10_000);
    let mut facts = Vec::with_capacity(limit.min(4_096));
    let mut has_more = false;

    for (line_no, line) in reader.lines().enumerate() {
        let cursor = line_no as u64 + 1;
        if cursor <= after_cursor {
            continue;
        }
        let Ok(line) = line else {
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }
        let event: AuditEvent = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if event.event_type != STRATEGY_EXECUTION_FACT_EVENT_TYPE {
            continue;
        }
        let fact: StrategyExecutionFact = match serde_json::from_value(event.data) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !predicate(&fact) {
            continue;
        }
        if facts.len() >= limit {
            has_more = true;
            break;
        }
        facts.push(StrategyExecutionReplayItem { cursor, fact });
    }

    (facts, has_more)
}

fn read_strategy_execution_facts<F>(
    state: &AppState,
    after_cursor: u64,
    limit: usize,
    predicate: F,
) -> (Vec<StrategyExecutionReplayItem>, bool)
where
    F: FnMut(&StrategyExecutionFact) -> bool,
{
    let file = match File::open(state.audit_read_path.as_ref().as_path()) {
        Ok(file) => file,
        Err(_) => return (Vec::new(), false),
    };
    let reader = BufReader::new(file);
    collect_strategy_execution_facts_from_reader(reader, after_cursor, limit, predicate)
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

pub(super) async fn handle_post_strategy_intent_adapt(
    State(state): State<AppState>,
    Json(intent): Json<StrategyIntent>,
) -> Result<Json<StrategyIntentAdaptResponse>, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    intent.validate().map_err(|reason| {
        let snapshot = state.strategy_snapshot_store.snapshot();
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;

    let snapshot = state.strategy_snapshot_store.snapshot();
    let now_ns = gateway_core::now_nanos();
    validate_intent_against_snapshot(&snapshot, &intent, now_ns).map_err(|reason| {
        let status = if reason == "STRATEGY_SNAPSHOT_STALE" {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        snapshot_error(&state, &snapshot, status, reason)
    })?;

    let effective_policy = resolve_effective_policy(&snapshot, &intent);
    let effective_risk_budget_ref = resolve_effective_risk_budget_ref(&snapshot, &intent);
    let applied_policy = apply_execution_policy_to_order_request(&intent, &effective_policy)
        .map_err(|reason| {
            snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
        })?;
    let algo_plan =
        build_algo_execution_plan(&intent, effective_policy.execution_policy.as_ref(), now_ns)
            .map_err(|reason| {
                snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
            })?;

    let policy_adjustments = applied_policy.adjustment_strings();
    let order_request = applied_policy.order_request;

    Ok(Json(StrategyIntentAdaptResponse {
        snapshot_id: snapshot.snapshot_id.clone(),
        version: snapshot.version,
        adapted_at_ns: now_ns,
        order_request,
        algo_plan,
        effective_policy,
        effective_risk_budget_ref,
        policy_adjustments,
        shadow_enabled: snapshot.shadow_enabled,
    }))
}

pub(super) async fn handle_post_strategy_intent_submit(
    State(state): State<AppState>,
    Json(request): Json<StrategyIntentSubmitRequest>,
) -> Result<
    (StatusCode, Json<StrategyIntentSubmitResponse>),
    (StatusCode, Json<StrategyConfigErrorResponse>),
> {
    request.intent.validate().map_err(|reason| {
        let snapshot = state.strategy_snapshot_store.snapshot();
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;

    let snapshot = state.strategy_snapshot_store.snapshot();
    let now_ns = gateway_core::now_nanos();
    validate_intent_against_snapshot(&snapshot, &request.intent, now_ns).map_err(|reason| {
        let status = if reason == "STRATEGY_SNAPSHOT_STALE" {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        snapshot_error(&state, &snapshot, status, reason)
    })?;

    let effective_policy = resolve_effective_policy(&snapshot, &request.intent);
    let effective_risk_budget_ref = resolve_effective_risk_budget_ref(&snapshot, &request.intent);
    let applied_policy =
        apply_execution_policy_to_order_request(&request.intent, &effective_policy).map_err(
            |reason| snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason),
        )?;
    let algo_plan = build_algo_execution_plan(
        &request.intent,
        effective_policy.execution_policy.as_ref(),
        now_ns,
    )
    .map_err(|reason| {
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;
    let order_request = applied_policy.order_request.clone();
    let policy_adjustments = applied_policy.adjustment_strings();
    let shadow_seeded = if let Some(shadow_run_id) = request.shadow_run_id.as_deref() {
        let replaced = upsert_shadow_record_for_intent(
            &state,
            &snapshot,
            shadow_run_id,
            &request.intent,
            request.predicted_policy.clone(),
            request.predicted_outcome.clone(),
            now_ns,
        )?;
        let _ = replaced;
        true
    } else {
        false
    };
    if let Some(algo_plan) = algo_plan.clone() {
        let algo_runtime = build_algo_parent_execution(
            &request,
            order_request.clone(),
            &effective_policy,
            effective_risk_budget_ref.clone(),
            &algo_plan,
            now_ns,
        );
        append_algo_runtime_snapshot_durable(&state, &algo_runtime, now_ns)
            .await
            .map_err(|reason| {
                snapshot_error(&state, &snapshot, StatusCode::SERVICE_UNAVAILABLE, reason)
            })?;
        state
            .strategy_runtime_store
            .insert(algo_runtime.clone())
            .map_err(|reason| {
                snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
            })?;
        spawn_algo_runtime(state.clone(), &algo_runtime);

        return Ok((
            StatusCode::ACCEPTED,
            Json(StrategyIntentSubmitResponse {
                snapshot_id: snapshot.snapshot_id.clone(),
                version: snapshot.version,
                submitted_at_ns: now_ns,
                order_request,
                algo_plan: Some(algo_plan),
                algo_runtime: Some(algo_runtime),
                effective_policy,
                effective_risk_budget_ref,
                policy_adjustments,
                shadow_enabled: snapshot.shadow_enabled,
                shadow_run_id: request.shadow_run_id,
                shadow_seeded,
                volatile_order: VolatileOrderResponse::algo_runtime_scheduled(
                    &request.intent.session_id,
                    now_ns,
                ),
            }),
        ));
    }
    let (status, volatile_order) = super::orders::process_order_v3_hot_path_with_strategy_context(
        &state,
        &request.intent.account_id,
        &request.intent.session_id,
        order_request.clone(),
        Some(Arc::new(effective_policy.clone())),
        effective_risk_budget_ref.clone().map(Arc::<str>::from),
        now_ns,
    );

    Ok((
        status,
        Json(StrategyIntentSubmitResponse {
            snapshot_id: snapshot.snapshot_id.clone(),
            version: snapshot.version,
            submitted_at_ns: now_ns,
            order_request,
            algo_plan: None,
            algo_runtime: None,
            effective_policy,
            effective_risk_budget_ref,
            policy_adjustments,
            shadow_enabled: snapshot.shadow_enabled,
            shadow_run_id: request.shadow_run_id,
            shadow_seeded,
            volatile_order,
        }),
    ))
}

pub(super) async fn handle_post_strategy_intent_shadow(
    State(state): State<AppState>,
    Json(request): Json<StrategyIntentShadowSeedRequest>,
) -> Result<Json<ShadowRecordUpsertResponse>, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    request.intent.validate().map_err(|reason| {
        let snapshot = state.strategy_snapshot_store.snapshot();
        snapshot_error(&state, &snapshot, StatusCode::UNPROCESSABLE_ENTITY, reason)
    })?;

    let snapshot = state.strategy_snapshot_store.snapshot();
    let now_ns = gateway_core::now_nanos();
    validate_intent_against_snapshot(&snapshot, &request.intent, now_ns).map_err(|reason| {
        let status = if reason == "STRATEGY_SNAPSHOT_STALE" {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        snapshot_error(&state, &snapshot, status, reason)
    })?;

    let replaced = upsert_shadow_record_for_intent(
        &state,
        &snapshot,
        &request.shadow_run_id,
        &request.intent,
        request.predicted_policy,
        request.predicted_outcome,
        now_ns,
    )?;

    Ok(Json(ShadowRecordUpsertResponse {
        shadow_run_id: request.shadow_run_id,
        intent_id: request.intent.intent_id,
        replaced,
        record_count: state.strategy_shadow_store.size(),
        upsert_total: state.strategy_shadow_store.upsert_total(),
    }))
}

pub(super) async fn handle_get_strategy_runtime(
    State(state): State<AppState>,
    Path(parent_intent_id): Path<String>,
) -> Result<Json<AlgoParentExecution>, StatusCode> {
    state
        .strategy_runtime_store
        .get(&parent_intent_id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub(super) async fn handle_get_strategy_replay_by_execution_run_id(
    State(state): State<AppState>,
    Path(execution_run_id): Path<String>,
    Query(query): Query<StrategyReplayQuery>,
) -> Json<StrategyExecutionReplayResponse> {
    let requested_after_cursor = query.after_cursor.unwrap_or(0);
    let (facts, has_more) = read_strategy_execution_facts(
        &state,
        requested_after_cursor,
        query.limit.unwrap_or(500),
        |fact| fact.execution_run_id.as_deref() == Some(execution_run_id.as_str()),
    );
    let next_cursor = facts.last().map(|item| item.cursor);
    Json(StrategyExecutionReplayResponse {
        execution_run_id: Some(execution_run_id),
        intent_id: None,
        requested_after_cursor,
        next_cursor,
        has_more,
        fact_count: facts.len(),
        facts,
    })
}

pub(super) async fn handle_get_strategy_catchup_by_execution_run_id(
    State(state): State<AppState>,
    Path(execution_run_id): Path<String>,
    Query(query): Query<StrategyReplayQuery>,
) -> Json<StrategyExecutionCatchupInput> {
    let requested_after_cursor = query.after_cursor.unwrap_or(0);
    let (facts, has_more) = read_strategy_execution_facts(
        &state,
        requested_after_cursor,
        query.limit.unwrap_or(500),
        |fact| fact.execution_run_id.as_deref() == Some(execution_run_id.as_str()),
    );
    let next_cursor = facts.last().map(|item| item.cursor);
    Json(StrategyExecutionCatchupInput::from_replay_items(
        Some(execution_run_id),
        None,
        requested_after_cursor,
        next_cursor,
        has_more,
        facts,
    ))
}

pub(super) async fn handle_get_strategy_replay_by_intent_id(
    State(state): State<AppState>,
    Path(intent_id): Path<String>,
    Query(query): Query<StrategyReplayQuery>,
) -> Json<StrategyExecutionReplayResponse> {
    let requested_after_cursor = query.after_cursor.unwrap_or(0);
    let (facts, has_more) = read_strategy_execution_facts(
        &state,
        requested_after_cursor,
        query.limit.unwrap_or(500),
        |fact| fact.intent_id.as_deref() == Some(intent_id.as_str()),
    );
    let next_cursor = facts.last().map(|item| item.cursor);
    Json(StrategyExecutionReplayResponse {
        execution_run_id: None,
        intent_id: Some(intent_id),
        requested_after_cursor,
        next_cursor,
        has_more,
        fact_count: facts.len(),
        facts,
    })
}

pub(super) async fn handle_get_strategy_catchup_by_intent_id(
    State(state): State<AppState>,
    Path(intent_id): Path<String>,
    Query(query): Query<StrategyReplayQuery>,
) -> Json<StrategyExecutionCatchupInput> {
    let requested_after_cursor = query.after_cursor.unwrap_or(0);
    let (facts, has_more) = read_strategy_execution_facts(
        &state,
        requested_after_cursor,
        query.limit.unwrap_or(500),
        |fact| fact.intent_id.as_deref() == Some(intent_id.as_str()),
    );
    let next_cursor = facts.last().map(|item| item.cursor);
    Json(StrategyExecutionCatchupInput::from_replay_items(
        None,
        Some(intent_id),
        requested_after_cursor,
        next_cursor,
        has_more,
        facts,
    ))
}

pub(super) async fn handle_get_strategy_config(
    State(state): State<AppState>,
) -> Json<ExecutionConfigSnapshot> {
    Json(state.strategy_snapshot_store.snapshot().as_ref().clone())
}

pub(super) async fn handle_put_strategy_config(
    State(state): State<AppState>,
    Json(mut snapshot): Json<ExecutionConfigSnapshot>,
) -> Result<Json<StrategyConfigUpdateResponse>, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    let current = state.strategy_snapshot_store.snapshot();
    if snapshot.version < current.version {
        return Err((
            StatusCode::CONFLICT,
            Json(StrategyConfigErrorResponse {
                reason: "SNAPSHOT_VERSION_STALE".to_string(),
                current_snapshot_id: current.snapshot_id.clone(),
                current_version: current.version,
                applied_total: state.strategy_snapshot_store.applied_total(),
                current_applied_at_ns: current.applied_at_ns,
            }),
        ));
    }

    if snapshot.applied_at_ns == 0 {
        snapshot.applied_at_ns = gateway_core::now_nanos();
    }

    let previous = state
        .strategy_snapshot_store
        .replace(snapshot.clone())
        .map_err(|reason| {
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(StrategyConfigErrorResponse {
                    reason: reason.to_string(),
                    current_snapshot_id: current.snapshot_id.clone(),
                    current_version: current.version,
                    applied_total: state.strategy_snapshot_store.applied_total(),
                    current_applied_at_ns: current.applied_at_ns,
                }),
            )
        })?;

    Ok(Json(StrategyConfigUpdateResponse {
        snapshot_id: snapshot.snapshot_id,
        version: snapshot.version,
        previous_version: previous.version,
        applied_total: state.strategy_snapshot_store.applied_total(),
        applied_at_ns: snapshot.applied_at_ns,
    }))
}

pub(super) async fn handle_post_shadow_record(
    State(state): State<AppState>,
    Json(record): Json<ShadowRecord>,
) -> Result<Json<ShadowRecordUpsertResponse>, (StatusCode, Json<StrategyConfigErrorResponse>)> {
    let current_snapshot = state.strategy_snapshot_store.snapshot();
    let replaced = state
        .strategy_shadow_store
        .upsert(record.clone())
        .map_err(|reason| {
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(StrategyConfigErrorResponse {
                    reason: reason.to_string(),
                    current_snapshot_id: current_snapshot.snapshot_id.clone(),
                    current_version: current_snapshot.version,
                    applied_total: state.strategy_snapshot_store.applied_total(),
                    current_applied_at_ns: current_snapshot.applied_at_ns,
                }),
            )
        })?
        .is_some();

    Ok(Json(ShadowRecordUpsertResponse {
        shadow_run_id: record.shadow_run_id,
        intent_id: record.intent_id,
        replaced,
        record_count: state.strategy_shadow_store.size(),
        upsert_total: state.strategy_shadow_store.upsert_total(),
    }))
}

pub(super) async fn handle_get_shadow_record(
    State(state): State<AppState>,
    Path((shadow_run_id, intent_id)): Path<(String, String)>,
) -> Result<Json<ShadowRecord>, StatusCode> {
    state
        .strategy_shadow_store
        .get(&shadow_run_id, &intent_id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub(super) async fn handle_get_shadow_run_summary(
    State(state): State<AppState>,
    Path(shadow_run_id): Path<String>,
) -> Result<Json<ShadowRunSummaryResponse>, StatusCode> {
    state
        .strategy_shadow_store
        .summary_for_run(&shadow_run_id)
        .map(|summary| {
            Json(ShadowRunSummaryResponse {
                shadow_run_id: summary.shadow_run_id,
                record_count: summary.record_count,
                pending_count: summary.pending_count,
                matched_count: summary.matched_count,
                skipped_count: summary.skipped_count,
                timed_out_count: summary.timed_out_count,
                negative_score_count: summary.negative_score_count,
                zero_score_count: summary.zero_score_count,
                positive_score_count: summary.positive_score_count,
                total_score_bps: summary.total_score_bps,
                average_score_bps: summary.average_score_bps,
                min_score_bps: summary.min_score_bps,
                max_score_bps: summary.max_score_bps,
                last_evaluated_at_ns: summary.last_evaluated_at_ns,
                top_positive: summary.top_positive,
                top_negative: summary.top_negative,
            })
        })
        .ok_or(StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use crate::audit::AuditEvent;
    use crate::store::strategy_shadow_store::StrategyShadowScoreSample;
    use crate::strategy::replay::{
        STRATEGY_EXECUTION_FACT_EVENT_TYPE, StrategyExecutionCatchupInput, StrategyExecutionFact,
        StrategyExecutionFactStatus,
    };

    use super::{
        ShadowRecordUpsertResponse, ShadowRunSummaryResponse, StrategyConfigErrorResponse,
        StrategyConfigUpdateResponse, StrategyIntentAdaptResponse, StrategyIntentShadowSeedRequest,
        StrategyIntentSubmitRequest, collect_strategy_execution_facts_from_reader,
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
                recovery_policy: Some(
                    crate::strategy::intent::StrategyRecoveryPolicy::NoAutoResume,
                ),
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
                recovery_policy: Some(
                    crate::strategy::intent::StrategyRecoveryPolicy::NoAutoResume,
                ),
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
    }

    #[test]
    fn strategy_execution_fact_reader_filters_and_keeps_latest_matches() {
        let events = vec![
            serde_json::to_string(&AuditEvent {
                event_type: STRATEGY_EXECUTION_FACT_EVENT_TYPE.to_string(),
                at: 1,
                account_id: "acc-1".to_string(),
                order_id: Some("v3/sess-1/1".to_string()),
                data: serde_json::to_value(
                    StrategyExecutionFact::new(
                        "acc-1",
                        "sess-1",
                        "AAPL",
                        1_000_000,
                        StrategyExecutionFactStatus::Rejected,
                    )
                    .with_execution_run_id("run-1")
                    .with_intent_id("intent-1")
                    .with_session_seq(1),
                )
                .expect("serialize fact 1"),
            })
            .expect("serialize event 1"),
            serde_json::to_string(&AuditEvent {
                event_type: STRATEGY_EXECUTION_FACT_EVENT_TYPE.to_string(),
                at: 2,
                account_id: "acc-1".to_string(),
                order_id: Some("v3/sess-1/2".to_string()),
                data: serde_json::to_value(
                    StrategyExecutionFact::new(
                        "acc-1",
                        "sess-1",
                        "AAPL",
                        2_000_000,
                        StrategyExecutionFactStatus::Unconfirmed,
                    )
                    .with_execution_run_id("run-2")
                    .with_intent_id("intent-2")
                    .with_session_seq(2),
                )
                .expect("serialize fact 2"),
            })
            .expect("serialize event 2"),
            serde_json::to_string(&AuditEvent {
                event_type: STRATEGY_EXECUTION_FACT_EVENT_TYPE.to_string(),
                at: 3,
                account_id: "acc-1".to_string(),
                order_id: Some("v3/sess-1/3".to_string()),
                data: serde_json::to_value(
                    StrategyExecutionFact::new(
                        "acc-1",
                        "sess-1",
                        "AAPL",
                        3_000_000,
                        StrategyExecutionFactStatus::DurableAccepted,
                    )
                    .with_execution_run_id("run-1")
                    .with_intent_id("intent-1")
                    .with_session_seq(3),
                )
                .expect("serialize fact 3"),
            })
            .expect("serialize event 3"),
        ]
        .join("\n");

        let (facts, has_more) = collect_strategy_execution_facts_from_reader(
            BufReader::new(Cursor::new(events.into_bytes())),
            0,
            1,
            |fact| fact.execution_run_id.as_deref() == Some("run-1"),
        );

        assert!(has_more);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].cursor, 1);
        assert_eq!(facts[0].fact.session_seq, Some(1));
        assert_eq!(facts[0].fact.status, StrategyExecutionFactStatus::Rejected);
    }

    #[test]
    fn catchup_input_batches_latest_states_from_replay_page() {
        let events = vec![
            serde_json::to_string(&AuditEvent {
                event_type: STRATEGY_EXECUTION_FACT_EVENT_TYPE.to_string(),
                at: 1,
                account_id: "acc-1".to_string(),
                order_id: Some("v3/sess-1/7".to_string()),
                data: serde_json::to_value(
                    StrategyExecutionFact::new(
                        "acc-1",
                        "sess-1",
                        "AAPL",
                        1_000_000,
                        StrategyExecutionFactStatus::Unconfirmed,
                    )
                    .with_execution_run_id("run-1")
                    .with_intent_id("intent-1")
                    .with_session_seq(7)
                    .with_position_delta_qty(10),
                )
                .expect("serialize fact 1"),
            })
            .expect("serialize event 1"),
            serde_json::to_string(&AuditEvent {
                event_type: STRATEGY_EXECUTION_FACT_EVENT_TYPE.to_string(),
                at: 2,
                account_id: "acc-1".to_string(),
                order_id: Some("v3/sess-1/7".to_string()),
                data: serde_json::to_value(
                    StrategyExecutionFact::new(
                        "acc-1",
                        "sess-1",
                        "AAPL",
                        2_000_000,
                        StrategyExecutionFactStatus::DurableAccepted,
                    )
                    .with_execution_run_id("run-1")
                    .with_intent_id("intent-1")
                    .with_session_seq(7)
                    .with_position_delta_qty(10),
                )
                .expect("serialize fact 2"),
            })
            .expect("serialize event 2"),
            serde_json::to_string(&AuditEvent {
                event_type: STRATEGY_EXECUTION_FACT_EVENT_TYPE.to_string(),
                at: 3,
                account_id: "acc-1".to_string(),
                order_id: Some("v3/sess-1/8".to_string()),
                data: serde_json::to_value(
                    StrategyExecutionFact::new(
                        "acc-1",
                        "sess-1",
                        "AAPL",
                        3_000_000,
                        StrategyExecutionFactStatus::LossSuspect,
                    )
                    .with_execution_run_id("run-1")
                    .with_intent_id("intent-2")
                    .with_session_seq(8)
                    .with_position_delta_qty(12),
                )
                .expect("serialize fact 3"),
            })
            .expect("serialize event 3"),
        ]
        .join("\n");

        let (facts, has_more) = collect_strategy_execution_facts_from_reader(
            BufReader::new(Cursor::new(events.into_bytes())),
            0,
            32,
            |fact| fact.execution_run_id.as_deref() == Some("run-1"),
        );
        let catchup = StrategyExecutionCatchupInput::from_replay_items(
            Some("run-1".to_string()),
            None,
            0,
            facts.last().map(|item| item.cursor),
            has_more,
            facts,
        );

        assert_eq!(catchup.fact_count, 3);
        assert_eq!(catchup.latest_order_states.len(), 2);
        assert_eq!(catchup.latest_status_totals.durable_accepted, 1);
        assert_eq!(catchup.latest_status_totals.loss_suspect, 1);
        assert_eq!(catchup.next_cursor, Some(3));
    }

    fn strategy_intent_fixture() -> crate::strategy::intent::StrategyIntent {
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
            recovery_policy: Some(crate::strategy::intent::StrategyRecoveryPolicy::NoAutoResume),
            algo: None,
            created_at_ns: 10,
            expires_at_ns: 123_000_000,
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
}
