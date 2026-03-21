use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const STRATEGY_EXECUTION_FACT_SCHEMA_VERSION: u16 = 1;
pub const STRATEGY_EXECUTION_FACT_EVENT_TYPE: &str = "StrategyExecutionFact";
pub const STRATEGY_EXECUTION_CATCHUP_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyExecutionFactStatus {
    Rejected,
    Unconfirmed,
    DurableAccepted,
    DurableRejected,
    LossSuspect,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionFact {
    pub schema_version: u16,
    #[serde(default)]
    pub execution_run_id: Option<String>,
    #[serde(default)]
    pub decision_key: Option<String>,
    #[serde(default)]
    pub decision_attempt_seq: Option<u64>,
    #[serde(default)]
    pub intent_id: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    pub account_id: String,
    pub session_id: String,
    #[serde(default)]
    pub session_seq: Option<u64>,
    pub symbol: String,
    #[serde(default)]
    pub position_delta_qty: Option<i64>,
    pub event_at_ns: u64,
    pub status: StrategyExecutionFactStatus,
    #[serde(default)]
    pub reason: Option<String>,
}

impl StrategyExecutionFact {
    pub fn new(
        account_id: impl Into<String>,
        session_id: impl Into<String>,
        symbol: impl Into<String>,
        event_at_ns: u64,
        status: StrategyExecutionFactStatus,
    ) -> Self {
        Self {
            schema_version: STRATEGY_EXECUTION_FACT_SCHEMA_VERSION,
            execution_run_id: None,
            decision_key: None,
            decision_attempt_seq: None,
            intent_id: None,
            model_id: None,
            account_id: account_id.into(),
            session_id: session_id.into(),
            session_seq: None,
            symbol: symbol.into(),
            position_delta_qty: None,
            event_at_ns,
            status,
            reason: None,
        }
    }

    pub fn with_execution_run_id(mut self, execution_run_id: impl Into<String>) -> Self {
        self.execution_run_id = Some(execution_run_id.into());
        self
    }

    pub fn with_intent_id(mut self, intent_id: impl Into<String>) -> Self {
        self.intent_id = Some(intent_id.into());
        self
    }

    pub fn with_decision_key(mut self, decision_key: impl Into<String>) -> Self {
        self.decision_key = Some(decision_key.into());
        self
    }

    pub fn with_decision_attempt_seq(mut self, decision_attempt_seq: u64) -> Self {
        self.decision_attempt_seq = Some(decision_attempt_seq);
        self
    }

    pub fn with_model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = Some(model_id.into());
        self
    }

    pub fn with_session_seq(mut self, session_seq: u64) -> Self {
        self.session_seq = Some(session_seq);
        self
    }

    pub fn with_position_delta_qty(mut self, position_delta_qty: i64) -> Self {
        self.position_delta_qty = Some(position_delta_qty);
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionReplayItem {
    pub cursor: u64,
    pub fact: StrategyExecutionFact,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionStatusTotals {
    pub rejected: u64,
    pub unconfirmed: u64,
    pub durable_accepted: u64,
    pub durable_rejected: u64,
    pub loss_suspect: u64,
}

impl StrategyExecutionStatusTotals {
    pub fn add_status(&mut self, status: StrategyExecutionFactStatus) {
        match status {
            StrategyExecutionFactStatus::Rejected => self.rejected += 1,
            StrategyExecutionFactStatus::Unconfirmed => self.unconfirmed += 1,
            StrategyExecutionFactStatus::DurableAccepted => self.durable_accepted += 1,
            StrategyExecutionFactStatus::DurableRejected => self.durable_rejected += 1,
            StrategyExecutionFactStatus::LossSuspect => self.loss_suspect += 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionCatchupOrderState {
    pub cursor: u64,
    pub session_id: String,
    #[serde(default)]
    pub session_seq: Option<u64>,
    #[serde(default)]
    pub execution_run_id: Option<String>,
    #[serde(default)]
    pub decision_key: Option<String>,
    #[serde(default)]
    pub decision_attempt_seq: Option<u64>,
    #[serde(default)]
    pub intent_id: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    pub symbol: String,
    #[serde(default)]
    pub position_delta_qty: Option<i64>,
    pub latest_event_at_ns: u64,
    pub latest_status: StrategyExecutionFactStatus,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_order: Option<StrategyExecutionLiveOrderState>,
}

impl StrategyExecutionCatchupOrderState {
    pub fn from_replay_item(item: &StrategyExecutionReplayItem) -> Self {
        Self {
            cursor: item.cursor,
            session_id: item.fact.session_id.clone(),
            session_seq: item.fact.session_seq,
            execution_run_id: item.fact.execution_run_id.clone(),
            decision_key: item.fact.decision_key.clone(),
            decision_attempt_seq: item.fact.decision_attempt_seq,
            intent_id: item.fact.intent_id.clone(),
            model_id: item.fact.model_id.clone(),
            symbol: item.fact.symbol.clone(),
            position_delta_qty: item.fact.position_delta_qty,
            latest_event_at_ns: item.fact.event_at_ns,
            latest_status: item.fact.status,
            reason: item.fact.reason.clone(),
            live_order: None,
        }
    }

    pub fn attach_live_order(&mut self, live_order: StrategyExecutionLiveOrderState) {
        self.live_order = Some(live_order);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionLiveOrderState {
    pub order_id: String,
    pub side: String,
    pub status: String,
    pub qty: u64,
    pub filled_qty: u64,
    pub remaining_qty: u64,
    pub accepted_at_ns: u64,
    pub last_update_at_ns: u64,
    pub is_terminal: bool,
}

impl StrategyExecutionLiveOrderState {
    pub fn new(
        order_id: impl Into<String>,
        side: impl Into<String>,
        status: impl Into<String>,
        qty: u64,
        filled_qty: u64,
        accepted_at_ns: u64,
        last_update_at_ns: u64,
        is_terminal: bool,
    ) -> Self {
        Self {
            order_id: order_id.into(),
            side: side.into(),
            status: status.into(),
            qty,
            filled_qty: filled_qty.min(qty),
            remaining_qty: qty.saturating_sub(filled_qty.min(qty)),
            accepted_at_ns,
            last_update_at_ns,
            is_terminal,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionCatchupInput {
    pub schema_version: u16,
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
    pub latest_order_states: Vec<StrategyExecutionCatchupOrderState>,
    pub latest_status_totals: StrategyExecutionStatusTotals,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum StrategyExecutionOrderKey {
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

impl StrategyExecutionCatchupInput {
    pub fn from_replay_items(
        execution_run_id: Option<String>,
        intent_id: Option<String>,
        requested_after_cursor: u64,
        next_cursor: Option<u64>,
        has_more: bool,
        facts: Vec<StrategyExecutionReplayItem>,
    ) -> Self {
        let latest_order_states = latest_order_states_from_replay_items(&facts);
        Self::from_replay_items_with_latest_order_states(
            execution_run_id,
            intent_id,
            requested_after_cursor,
            next_cursor,
            has_more,
            facts,
            latest_order_states,
        )
    }

    pub fn from_replay_items_with_latest_order_states(
        execution_run_id: Option<String>,
        intent_id: Option<String>,
        requested_after_cursor: u64,
        next_cursor: Option<u64>,
        has_more: bool,
        facts: Vec<StrategyExecutionReplayItem>,
        mut latest_order_states: Vec<StrategyExecutionCatchupOrderState>,
    ) -> Self {
        latest_order_states.sort_by_key(|state| state.cursor);

        let mut latest_status_totals = StrategyExecutionStatusTotals::default();
        for state in &latest_order_states {
            latest_status_totals.add_status(state.latest_status);
        }

        Self {
            schema_version: STRATEGY_EXECUTION_CATCHUP_SCHEMA_VERSION,
            execution_run_id,
            intent_id,
            requested_after_cursor,
            next_cursor,
            has_more,
            fact_count: facts.len(),
            facts,
            latest_order_states,
            latest_status_totals,
        }
    }
}

pub fn latest_order_states_from_replay_items(
    facts: &[StrategyExecutionReplayItem],
) -> Vec<StrategyExecutionCatchupOrderState> {
    let mut latest_by_order: HashMap<
        StrategyExecutionOrderKey,
        StrategyExecutionCatchupOrderState,
    > = HashMap::new();
    for item in facts {
        let key = if let Some(decision_key) = item.fact.decision_key.clone() {
            StrategyExecutionOrderKey::Decision {
                execution_run_id: item.fact.execution_run_id.clone(),
                intent_id: item.fact.intent_id.clone(),
                decision_key,
            }
        } else if let Some(session_seq) = item.fact.session_seq {
            StrategyExecutionOrderKey::SessionOrder {
                session_id: item.fact.session_id.clone(),
                session_seq,
            }
        } else if let Some(intent_id) = item.fact.intent_id.clone() {
            StrategyExecutionOrderKey::IntentOrder {
                execution_run_id: item.fact.execution_run_id.clone(),
                intent_id,
            }
        } else {
            StrategyExecutionOrderKey::Cursor(item.cursor)
        };
        latest_by_order.insert(
            key,
            StrategyExecutionCatchupOrderState::from_replay_item(item),
        );
    }

    latest_by_order.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::{
        StrategyExecutionCatchupInput, StrategyExecutionCatchupOrderState, StrategyExecutionFact,
        StrategyExecutionFactStatus, StrategyExecutionLiveOrderState, StrategyExecutionReplayItem,
    };

    #[test]
    fn fact_round_trips_json() {
        let fact = StrategyExecutionFact::new(
            "acc-1",
            "sess-1",
            "AAPL",
            123,
            StrategyExecutionFactStatus::Unconfirmed,
        )
        .with_execution_run_id("run-1")
        .with_decision_key("dec-1")
        .with_decision_attempt_seq(2)
        .with_intent_id("intent-1")
        .with_model_id("model-1")
        .with_session_seq(7)
        .with_position_delta_qty(100);

        let raw = serde_json::to_string(&fact).expect("serialize fact");
        let parsed: StrategyExecutionFact = serde_json::from_str(&raw).expect("deserialize fact");
        assert_eq!(parsed.execution_run_id.as_deref(), Some("run-1"));
        assert_eq!(parsed.decision_key.as_deref(), Some("dec-1"));
        assert_eq!(parsed.decision_attempt_seq, Some(2));
        assert_eq!(parsed.intent_id.as_deref(), Some("intent-1"));
        assert_eq!(parsed.model_id.as_deref(), Some("model-1"));
        assert_eq!(parsed.session_seq, Some(7));
        assert_eq!(parsed.position_delta_qty, Some(100));
        assert_eq!(parsed.status, StrategyExecutionFactStatus::Unconfirmed);
    }

    #[test]
    fn catchup_input_rolls_latest_states_by_order() {
        let facts = vec![
            StrategyExecutionReplayItem {
                cursor: 11,
                fact: StrategyExecutionFact::new(
                    "acc-1",
                    "sess-1",
                    "AAPL",
                    100,
                    StrategyExecutionFactStatus::Unconfirmed,
                )
                .with_execution_run_id("run-1")
                .with_decision_key("dec-1")
                .with_decision_attempt_seq(1)
                .with_intent_id("intent-1")
                .with_session_seq(7)
                .with_position_delta_qty(10),
            },
            StrategyExecutionReplayItem {
                cursor: 12,
                fact: StrategyExecutionFact::new(
                    "acc-1",
                    "sess-1",
                    "AAPL",
                    200,
                    StrategyExecutionFactStatus::DurableAccepted,
                )
                .with_execution_run_id("run-1")
                .with_decision_key("dec-1")
                .with_decision_attempt_seq(2)
                .with_intent_id("intent-1")
                .with_session_seq(8)
                .with_position_delta_qty(10),
            },
            StrategyExecutionReplayItem {
                cursor: 13,
                fact: StrategyExecutionFact::new(
                    "acc-1",
                    "sess-1",
                    "MSFT",
                    300,
                    StrategyExecutionFactStatus::Rejected,
                )
                .with_execution_run_id("run-1")
                .with_decision_key("dec-2")
                .with_decision_attempt_seq(1)
                .with_intent_id("intent-2"),
            },
        ];

        let catchup = StrategyExecutionCatchupInput::from_replay_items(
            Some("run-1".to_string()),
            None,
            10,
            Some(13),
            false,
            facts,
        );

        assert_eq!(catchup.fact_count, 3);
        assert_eq!(catchup.latest_order_states.len(), 2);
        assert_eq!(catchup.latest_status_totals.durable_accepted, 1);
        assert_eq!(catchup.latest_status_totals.rejected, 1);
        assert_eq!(catchup.latest_order_states[0].cursor, 12);
        assert_eq!(
            catchup.latest_order_states[0].decision_key.as_deref(),
            Some("dec-1")
        );
        assert_eq!(catchup.latest_order_states[0].decision_attempt_seq, Some(2));
        assert_eq!(
            catchup.latest_order_states[0].latest_status,
            StrategyExecutionFactStatus::DurableAccepted
        );
    }

    #[test]
    fn catchup_order_state_attaches_live_order_snapshot() {
        let mut state = StrategyExecutionCatchupOrderState {
            cursor: 11,
            session_id: "sess-1".to_string(),
            session_seq: Some(7),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("dec-1".to_string()),
            decision_attempt_seq: Some(1),
            intent_id: Some("intent-1".to_string()),
            model_id: Some("model-1".to_string()),
            symbol: "AAPL".to_string(),
            position_delta_qty: Some(10),
            latest_event_at_ns: 100,
            latest_status: StrategyExecutionFactStatus::Unconfirmed,
            reason: None,
            live_order: None,
        };
        state.attach_live_order(StrategyExecutionLiveOrderState::new(
            "v3/sess-1/7",
            "BUY",
            "PARTIALLY_FILLED",
            10,
            4,
            12_000_000,
            34_000_000,
            false,
        ));

        let live = state.live_order.expect("live order");
        assert_eq!(live.order_id, "v3/sess-1/7");
        assert_eq!(live.side, "BUY");
        assert_eq!(live.status, "PARTIALLY_FILLED");
        assert_eq!(live.filled_qty, 4);
        assert_eq!(live.remaining_qty, 6);
        assert_eq!(live.accepted_at_ns, 12_000_000);
        assert_eq!(live.last_update_at_ns, 34_000_000);
        assert!(!live.is_terminal);
    }
}
