use super::*;

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

pub(super) fn collect_strategy_execution_facts_from_reader<R, F>(
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

pub(super) fn read_strategy_execution_facts<F>(
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

pub(super) fn collect_strategy_execution_catchup_from_reader<R, F>(
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

pub(super) fn read_strategy_execution_catchup<F>(
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

pub(super) fn build_strategy_execution_catchup(
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

pub(super) fn synthetic_live_order_from_confirm_snapshot(
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
