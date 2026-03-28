mod support;
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
pub(super) use support::{append_algo_runtime_snapshot, spawn_algo_runtime};
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

#[cfg(test)]
mod tests;
