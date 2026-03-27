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
mod tests {
    use super::support::{
        collect_strategy_execution_catchup_from_reader,
        collect_strategy_execution_facts_from_reader, synthetic_live_order_from_confirm_snapshot,
    };
    use crate::audit::AuditEvent;
    use crate::strategy::replay::{
        STRATEGY_EXECUTION_FACT_EVENT_TYPE, StrategyExecutionCatchupInput,
        StrategyExecutionCatchupOrderState, StrategyExecutionFact, StrategyExecutionFactStatus,
    };
    use std::io::{BufReader, Cursor};

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

    #[test]
    fn catchup_reader_keeps_latest_states_outside_requested_cursor_window() {
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
                        StrategyExecutionFactStatus::DurableAccepted,
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
                order_id: Some("v3/sess-1/8".to_string()),
                data: serde_json::to_value(
                    StrategyExecutionFact::new(
                        "acc-1",
                        "sess-1",
                        "AAPL",
                        2_000_000,
                        StrategyExecutionFactStatus::Rejected,
                    )
                    .with_execution_run_id("run-1")
                    .with_intent_id("intent-2")
                    .with_session_seq(8)
                    .with_position_delta_qty(12),
                )
                .expect("serialize fact 2"),
            })
            .expect("serialize event 2"),
        ]
        .join("\n");

        let (facts, latest_order_states, has_more) = collect_strategy_execution_catchup_from_reader(
            BufReader::new(Cursor::new(events.into_bytes())),
            1,
            32,
            |fact| fact.execution_run_id.as_deref() == Some("run-1"),
        );

        assert_eq!(facts.len(), 1);
        assert!(!has_more);
        assert_eq!(latest_order_states.len(), 2);
        assert_eq!(latest_order_states[0].cursor, 1);
        assert_eq!(latest_order_states[1].cursor, 2);
    }

    #[test]
    fn synthetic_live_order_uses_durable_accept_snapshot_when_runtime_order_missing() {
        let order_state = StrategyExecutionCatchupOrderState {
            cursor: 7,
            session_id: "sess-1".to_string(),
            session_seq: Some(7),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-1".to_string()),
            decision_attempt_seq: Some(1),
            intent_id: Some("intent-1".to_string()),
            model_id: Some("model-1".to_string()),
            symbol: "AAPL".to_string(),
            position_delta_qty: Some(15),
            latest_event_at_ns: 7_000,
            latest_status: StrategyExecutionFactStatus::DurableAccepted,
            reason: None,
            live_order: None,
        };
        let confirm_snapshot = super::super::V3ConfirmSnapshot {
            status: super::super::V3ConfirmStatus::DurableAccepted,
            reason: None,
            attempt_seq: 7,
            received_at_ns: 6_000,
            updated_at_ns: 8_000,
            shard_id: 0,
            account_id: Some("acc-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-1".to_string()),
            decision_attempt_seq: Some(1),
            intent_id: Some("intent-1".to_string()),
            model_id: Some("model-1".to_string()),
            position_symbol_key: None,
            position_delta_qty: Some(15),
        };

        let live_order =
            synthetic_live_order_from_confirm_snapshot(&order_state, &confirm_snapshot)
                .expect("durable accepted snapshot should synthesize live order");

        assert_eq!(live_order.order_id, "v3/sess-1/7");
        assert_eq!(live_order.side, "BUY");
        assert_eq!(live_order.qty, 15);
        assert_eq!(live_order.filled_qty, 0);
        assert_eq!(live_order.remaining_qty, 15);
        assert_eq!(live_order.status, "DURABLE_ACCEPTED");
        assert!(!live_order.is_terminal);
        assert_eq!(live_order.accepted_at_ns, 7_000);
        assert_eq!(live_order.last_update_at_ns, 8_000);
    }

    #[test]
    fn synthetic_live_order_skips_unknown_confirm_statuses() {
        let order_state = StrategyExecutionCatchupOrderState {
            cursor: 9,
            session_id: "sess-1".to_string(),
            session_seq: Some(9),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-9".to_string()),
            decision_attempt_seq: Some(1),
            intent_id: Some("intent-9".to_string()),
            model_id: Some("model-1".to_string()),
            symbol: "AAPL".to_string(),
            position_delta_qty: Some(-12),
            latest_event_at_ns: 9_000,
            latest_status: StrategyExecutionFactStatus::LossSuspect,
            reason: Some("DURABILITY_QUEUE_FULL".to_string()),
            live_order: None,
        };
        let confirm_snapshot = super::super::V3ConfirmSnapshot {
            status: super::super::V3ConfirmStatus::LossSuspect,
            reason: Some("DURABILITY_QUEUE_FULL".to_string()),
            attempt_seq: 9,
            received_at_ns: 9_000,
            updated_at_ns: 9_500,
            shard_id: 0,
            account_id: Some("acc-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-9".to_string()),
            decision_attempt_seq: Some(1),
            intent_id: Some("intent-9".to_string()),
            model_id: Some("model-1".to_string()),
            position_symbol_key: None,
            position_delta_qty: Some(-12),
        };

        assert!(
            synthetic_live_order_from_confirm_snapshot(&order_state, &confirm_snapshot).is_none()
        );
    }
}
