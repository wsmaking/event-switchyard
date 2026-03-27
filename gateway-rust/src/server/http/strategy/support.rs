use super::*;
use crate::server::http::{orders, publish_quant_feedback};

#[derive(Debug)]
pub(super) struct AppliedExecutionPolicy {
    pub(super) order_request: OrderRequest,
    pub(super) adjustments: Vec<&'static str>,
}

impl AppliedExecutionPolicy {
    pub(super) fn adjustment_strings(&self) -> Vec<String> {
        self.adjustments
            .iter()
            .map(|value| (*value).to_string())
            .collect()
    }
}

pub(super) fn snapshot_error(
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

pub(super) fn resolve_effective_policy(
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

pub(super) fn resolve_effective_risk_budget_ref(
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

pub(super) fn validate_intent_against_snapshot(
    snapshot: &ExecutionConfigSnapshot,
    intent: &StrategyIntent,
    now_ns: u64,
) -> Result<(), &'static str> {
    if snapshot.is_stale_at(now_ns) {
        return Err("STRATEGY_SNAPSHOT_STALE");
    }

    if let Some(override_cfg) = snapshot.symbol_override(&intent.symbol) {
        if let Some(max_order_qty) = override_cfg.max_order_qty
            && intent.qty > max_order_qty
        {
            return Err("STRATEGY_SYMBOL_MAX_ORDER_QTY");
        }
        if let Some(max_notional) = override_cfg.max_notional
            && intent.limit_price.unwrap_or(0).saturating_mul(intent.qty) > max_notional
            && intent.limit_price.unwrap_or(0) > 0
        {
            return Err("STRATEGY_SYMBOL_MAX_NOTIONAL");
        }
    }

    if let Some(account_budget) = snapshot.account_risk_budget(&intent.account_id)
        && let Some(max_notional) = account_budget.max_notional
        && intent.limit_price.unwrap_or(0).saturating_mul(intent.qty) > max_notional
        && intent.limit_price.unwrap_or(0) > 0
    {
        return Err("STRATEGY_ACCOUNT_MAX_NOTIONAL");
    }

    Ok(())
}

pub(super) fn validate_intent_freshness(
    intent: &StrategyIntent,
    now_ns: u64,
) -> Result<(), &'static str> {
    intent.validate_alpha_freshness(now_ns)
}

pub(super) fn adapt_order_request(intent: &StrategyIntent) -> OrderRequest {
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

pub(super) fn effective_algo_runtime_mode(algo_plan: &AlgoExecutionPlan) -> AlgoRuntimeMode {
    match algo_plan.policy {
        crate::strategy::intent::ExecutionPolicyKind::Twap
        | crate::strategy::intent::ExecutionPolicyKind::Vwap
        | crate::strategy::intent::ExecutionPolicyKind::Pov => {
            AlgoRuntimeMode::GatewayManagedResume
        }
        _ => AlgoRuntimeMode::PauseOnRestart,
    }
}

pub(super) fn validate_legacy_recovery_policy(intent: &StrategyIntent) -> Result<(), &'static str> {
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

pub(super) fn apply_execution_policy_to_order_request(
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

pub(super) fn upsert_shadow_record_for_intent(
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

pub(super) fn build_algo_parent_execution(
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

pub(crate) fn append_algo_runtime_snapshot(state: &AppState, execution: &AlgoParentExecution) {
    if let Some(event) = build_algo_runtime_snapshot_event(execution) {
        state.audit_log.append(event);
    }
}

pub(super) async fn append_algo_runtime_snapshot_durable(
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
    publish_quant_feedback(
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
    let (status, volatile_order) = orders::process_order_v3_hot_path_with_strategy_context(
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

pub(crate) fn spawn_algo_runtime(state: AppState, runtime: &AlgoParentExecution) {
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
