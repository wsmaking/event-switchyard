mod support;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::audit::AuditEvent;
use crate::strategy::replay::{
    STRATEGY_EXECUTION_FACT_EVENT_TYPE, StrategyExecutionCatchupInput,
    StrategyExecutionCatchupOrderState, StrategyExecutionFact, StrategyExecutionLiveOrderState,
    StrategyExecutionReplayItem,
};
use crate::strategy::runtime::AlgoParentExecution;

use super::{AppState, V3ConfirmSnapshot, V3ConfirmStatus, v3_order_id};
use support::{
    build_strategy_execution_catchup, read_strategy_execution_catchup,
    read_strategy_execution_facts,
};

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
    let (facts, latest_order_states, has_more) = read_strategy_execution_catchup(
        &state,
        requested_after_cursor,
        query.limit.unwrap_or(500),
        |fact| fact.execution_run_id.as_deref() == Some(execution_run_id.as_str()),
    );
    let next_cursor = facts.last().map(|item| item.cursor);
    Json(build_strategy_execution_catchup(
        &state,
        Some(execution_run_id),
        None,
        requested_after_cursor,
        next_cursor,
        has_more,
        facts,
        latest_order_states,
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
    let (facts, latest_order_states, has_more) = read_strategy_execution_catchup(
        &state,
        requested_after_cursor,
        query.limit.unwrap_or(500),
        |fact| fact.intent_id.as_deref() == Some(intent_id.as_str()),
    );
    let next_cursor = facts.last().map(|item| item.cursor);
    Json(build_strategy_execution_catchup(
        &state,
        None,
        Some(intent_id),
        requested_after_cursor,
        next_cursor,
        has_more,
        facts,
        latest_order_states,
    ))
}

#[cfg(test)]
mod tests;
