use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Serialize;
use std::sync::Arc;

use crate::order::OrderRequest;
use crate::store::strategy_shadow_store::StrategyShadowScoreSample;
use crate::strategy::config::ExecutionConfigSnapshot;
use crate::strategy::intent::{IntentUrgency, StrategyIntent};
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
    pub effective_policy: ShadowPolicyView,
    pub effective_risk_budget_ref: Option<String>,
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
    pub effective_policy: ShadowPolicyView,
    pub effective_risk_budget_ref: Option<String>,
    pub shadow_enabled: bool,
    pub shadow_run_id: Option<String>,
    pub shadow_seeded: bool,
    pub volatile_order: VolatileOrderResponse,
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
    let expire_at = if matches!(intent.time_in_force, crate::order::TimeInForce::Gtd) {
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
    }
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

    Ok(Json(StrategyIntentAdaptResponse {
        snapshot_id: snapshot.snapshot_id.clone(),
        version: snapshot.version,
        adapted_at_ns: now_ns,
        order_request: adapt_order_request(&intent),
        effective_policy,
        effective_risk_budget_ref,
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
    let order_request = adapt_order_request(&request.intent);
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
            effective_policy,
            effective_risk_budget_ref,
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
    use crate::store::strategy_shadow_store::StrategyShadowScoreSample;

    use super::{
        ShadowRecordUpsertResponse, ShadowRunSummaryResponse, StrategyConfigErrorResponse,
        StrategyConfigUpdateResponse, StrategyIntentAdaptResponse,
        StrategyIntentShadowSeedRequest, StrategyIntentSubmitRequest,
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
            },
            effective_policy: crate::strategy::shadow::ShadowPolicyView {
                execution_policy: None,
                urgency: Some("HIGH".to_string()),
                venue: Some("NASDAQ".to_string()),
            },
            effective_risk_budget_ref: Some("budget-1".to_string()),
            shadow_enabled: true,
        })
        .expect("serialize adapt response");

        assert!(raw.contains("\"adaptedAtNs\":55"));
        assert!(raw.contains("\"effectiveRiskBudgetRef\":\"budget-1\""));
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
}
