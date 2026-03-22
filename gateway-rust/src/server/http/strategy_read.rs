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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum StrategyExecutionLatestOrderKey {
    Decision {
        execution_run_id: Option<String>,
        intent_id: Option<String>,
        decision_key: String,
    },
    SessionOrder {
        session_id: String,
        session_seq: u64,
    },
    IntentOrder {
        execution_run_id: Option<String>,
        intent_id: String,
    },
    Cursor(u64),
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

fn collect_strategy_execution_catchup_from_reader<R, F>(
    reader: R,
    after_cursor: u64,
    limit: usize,
    mut predicate: F,
) -> (
    Vec<StrategyExecutionReplayItem>,
    Vec<StrategyExecutionCatchupOrderState>,
    bool,
)
where
    R: BufRead,
    F: FnMut(&StrategyExecutionFact) -> bool,
{
    let mut facts = Vec::new();
    let mut has_more = false;
    let mut latest_by_order: HashMap<
        StrategyExecutionLatestOrderKey,
        StrategyExecutionCatchupOrderState,
    > = HashMap::new();

    for (line_no, line) in reader.lines().enumerate() {
        let cursor = line_no as u64 + 1;
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

        let item = StrategyExecutionReplayItem { cursor, fact };
        let key = if let Some(decision_key) = item.fact.decision_key.clone() {
            StrategyExecutionLatestOrderKey::Decision {
                execution_run_id: item.fact.execution_run_id.clone(),
                intent_id: item.fact.intent_id.clone(),
                decision_key,
            }
        } else if let Some(session_seq) = item.fact.session_seq {
            StrategyExecutionLatestOrderKey::SessionOrder {
                session_id: item.fact.session_id.clone(),
                session_seq,
            }
        } else if let Some(intent_id) = item.fact.intent_id.clone() {
            StrategyExecutionLatestOrderKey::IntentOrder {
                execution_run_id: item.fact.execution_run_id.clone(),
                intent_id,
            }
        } else {
            StrategyExecutionLatestOrderKey::Cursor(item.cursor)
        };
        latest_by_order.insert(
            key,
            StrategyExecutionCatchupOrderState::from_replay_item(&item),
        );

        if cursor <= after_cursor {
            continue;
        }
        if facts.len() >= limit {
            has_more = true;
            continue;
        }
        facts.push(item);
    }

    let mut latest_order_states = latest_by_order.into_values().collect::<Vec<_>>();
    latest_order_states.sort_by_key(|state| state.cursor);

    (facts, latest_order_states, has_more)
}

fn read_strategy_execution_catchup<F>(
    state: &AppState,
    after_cursor: u64,
    limit: usize,
    predicate: F,
) -> (
    Vec<StrategyExecutionReplayItem>,
    Vec<StrategyExecutionCatchupOrderState>,
    bool,
)
where
    F: FnMut(&StrategyExecutionFact) -> bool,
{
    let file = match File::open(state.audit_read_path.as_ref().as_path()) {
        Ok(file) => file,
        Err(_) => return (Vec::new(), Vec::new(), false),
    };
    let reader = BufReader::new(file);
    collect_strategy_execution_catchup_from_reader(reader, after_cursor, limit, predicate)
}

fn build_strategy_execution_catchup(
    state: &AppState,
    execution_run_id: Option<String>,
    intent_id: Option<String>,
    requested_after_cursor: u64,
    next_cursor: Option<u64>,
    has_more: bool,
    facts: Vec<StrategyExecutionReplayItem>,
    latest_order_states: Vec<StrategyExecutionCatchupOrderState>,
) -> StrategyExecutionCatchupInput {
    let mut catchup = StrategyExecutionCatchupInput::from_replay_items_with_latest_order_states(
        execution_run_id,
        intent_id,
        requested_after_cursor,
        next_cursor,
        has_more,
        facts,
        latest_order_states,
    );
    enrich_strategy_execution_catchup_live_orders(state, &mut catchup);
    catchup
}

fn enrich_strategy_execution_catchup_live_orders(
    state: &AppState,
    catchup: &mut StrategyExecutionCatchupInput,
) {
    for order_state in &mut catchup.latest_order_states {
        let Some(session_seq) = order_state.session_seq else {
            continue;
        };
        let order_id = v3_order_id(&order_state.session_id, session_seq);
        if let Some(order) = state.sharded_store.find_by_id(&order_id) {
            attach_live_order_from_store_snapshot(order_state, &order);
        } else if let Some(confirm_snapshot) = state
            .v3_confirm_store
            .snapshot(&order_state.session_id, session_seq)
        {
            attach_synthetic_live_order_from_confirm_snapshot(order_state, &confirm_snapshot);
        }
        if let Some(live_order) = order_state.live_order.as_ref() {
            order_state.latest_event_at_ns = order_state
                .latest_event_at_ns
                .max(live_order.last_update_at_ns);
        }
    }
}

fn attach_live_order_from_store_snapshot(
    order_state: &mut StrategyExecutionCatchupOrderState,
    order: &crate::store::OrderSnapshot,
) {
    order_state.attach_live_order(StrategyExecutionLiveOrderState::new(
        order.order_id.clone(),
        order.side.clone(),
        order.status.as_str(),
        order.qty,
        order.filled_qty,
        order.accepted_at.saturating_mul(1_000_000),
        order.last_update_at.saturating_mul(1_000_000),
        order.status.is_terminal(),
    ));
}

fn attach_synthetic_live_order_from_confirm_snapshot(
    order_state: &mut StrategyExecutionCatchupOrderState,
    confirm_snapshot: &V3ConfirmSnapshot,
) {
    let live_order = synthetic_live_order_from_confirm_snapshot(order_state, confirm_snapshot);
    if let Some(live_order) = live_order {
        order_state.attach_live_order(live_order);
    }
}

fn synthetic_live_order_from_confirm_snapshot(
    order_state: &StrategyExecutionCatchupOrderState,
    confirm_snapshot: &V3ConfirmSnapshot,
) -> Option<StrategyExecutionLiveOrderState> {
    let signed_qty = order_state
        .position_delta_qty
        .or(confirm_snapshot.position_delta_qty)?;
    if signed_qty == 0 {
        return None;
    }
    let side = if signed_qty > 0 { "BUY" } else { "SELL" };
    let qty = signed_qty.unsigned_abs();
    let status = match confirm_snapshot.status {
        V3ConfirmStatus::DurableAccepted => "DURABLE_ACCEPTED",
        V3ConfirmStatus::DurableRejected => "REJECTED",
        V3ConfirmStatus::VolatileAccept | V3ConfirmStatus::LossSuspect => {
            return None;
        }
    };
    Some(StrategyExecutionLiveOrderState::new(
        v3_order_id(
            &order_state.session_id,
            order_state.session_seq.unwrap_or_default(),
        ),
        side,
        status,
        qty,
        0,
        confirm_snapshot
            .received_at_ns
            .max(order_state.latest_event_at_ns),
        confirm_snapshot
            .updated_at_ns
            .max(confirm_snapshot.received_at_ns)
            .max(order_state.latest_event_at_ns),
        matches!(confirm_snapshot.status, V3ConfirmStatus::DurableRejected),
    ))
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
    use super::{
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
