use super::intent::StrategyIntent;
use super::replay::{
    StrategyExecutionCatchupInput, StrategyExecutionCatchupOrderState, StrategyExecutionFactStatus,
    StrategyExecutionLiveOrderState, StrategyExecutionStatusTotals,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum StrategyExecutionDecisionId {
    Decision {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_run_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        intent_id: Option<String>,
        decision_key: String,
    },
    SessionOrder {
        session_id: String,
        session_seq: u64,
    },
    IntentFallback {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_run_id: Option<String>,
        intent_id: String,
    },
    CursorFallback {
        cursor: u64,
    },
}

impl StrategyExecutionDecisionId {
    pub fn from_order_state(state: &StrategyExecutionCatchupOrderState) -> Self {
        if let Some(decision_key) = state.decision_key.clone() {
            return Self::Decision {
                execution_run_id: state.execution_run_id.clone(),
                intent_id: state.intent_id.clone(),
                decision_key,
            };
        }
        if let Some(session_seq) = state.session_seq {
            return Self::SessionOrder {
                session_id: state.session_id.clone(),
                session_seq,
            };
        }
        if let Some(intent_id) = state.intent_id.clone() {
            return Self::IntentFallback {
                execution_run_id: state.execution_run_id.clone(),
                intent_id,
            };
        }
        Self::CursorFallback {
            cursor: state.cursor,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionDecisionState {
    pub decision_id: StrategyExecutionDecisionId,
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

impl StrategyExecutionDecisionState {
    pub fn from_order_state(state: &StrategyExecutionCatchupOrderState) -> Self {
        Self {
            decision_id: StrategyExecutionDecisionId::from_order_state(state),
            cursor: state.cursor,
            session_id: state.session_id.clone(),
            session_seq: state.session_seq,
            execution_run_id: state.execution_run_id.clone(),
            decision_key: state.decision_key.clone(),
            decision_attempt_seq: state.decision_attempt_seq,
            intent_id: state.intent_id.clone(),
            model_id: state.model_id.clone(),
            symbol: state.symbol.clone(),
            position_delta_qty: state.position_delta_qty,
            latest_event_at_ns: state.latest_event_at_ns,
            latest_status: state.latest_status,
            reason: state.reason.clone(),
            live_order: state.live_order.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionCatchupLoopSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    pub next_cursor: u64,
    pub has_more: bool,
    pub total_fact_count: usize,
    pub latest_status_totals: StrategyExecutionStatusTotals,
    pub decisions: Vec<StrategyExecutionDecisionState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyExecutionCatchupApplyError {
    ExecutionRunIdMismatch { current: String, incoming: String },
    IntentIdMismatch { current: String, incoming: String },
}

#[derive(Debug, Clone, Default)]
pub struct StrategyExecutionCatchupLoop {
    execution_run_id: Option<String>,
    intent_id: Option<String>,
    next_cursor: u64,
    has_more: bool,
    total_fact_count: usize,
    decisions: BTreeMap<StrategyExecutionDecisionId, StrategyExecutionDecisionState>,
}

impl StrategyExecutionCatchupLoop {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn next_cursor(&self) -> u64 {
        self.next_cursor
    }

    #[allow(dead_code)]
    pub fn has_more(&self) -> bool {
        self.has_more
    }

    pub fn apply_page(
        &mut self,
        page: &StrategyExecutionCatchupInput,
    ) -> Result<(), StrategyExecutionCatchupApplyError> {
        if let (Some(current), Some(incoming)) = (
            self.execution_run_id.as_deref(),
            page.execution_run_id.as_deref(),
        ) {
            if current != incoming {
                return Err(StrategyExecutionCatchupApplyError::ExecutionRunIdMismatch {
                    current: current.to_string(),
                    incoming: incoming.to_string(),
                });
            }
        }
        if let (Some(current), Some(incoming)) =
            (self.intent_id.as_deref(), page.intent_id.as_deref())
        {
            if current != incoming {
                return Err(StrategyExecutionCatchupApplyError::IntentIdMismatch {
                    current: current.to_string(),
                    incoming: incoming.to_string(),
                });
            }
        }
        if self.execution_run_id.is_none() {
            self.execution_run_id = page.execution_run_id.clone();
        }
        if self.intent_id.is_none() {
            self.intent_id = page.intent_id.clone();
        }

        self.has_more = page.has_more;
        self.total_fact_count = self.total_fact_count.saturating_add(page.fact_count);
        let derived_next_cursor = page
            .next_cursor
            .or_else(|| page.facts.last().map(|item| item.cursor))
            .unwrap_or(page.requested_after_cursor);
        self.next_cursor = self.next_cursor.max(derived_next_cursor);

        for order_state in &page.latest_order_states {
            let decision_state = StrategyExecutionDecisionState::from_order_state(order_state);
            let key = decision_state.decision_id.clone();
            let should_replace = self
                .decisions
                .get(&key)
                .map(|existing| {
                    decision_state.cursor > existing.cursor
                        || (decision_state.cursor == existing.cursor
                            && decision_state.latest_event_at_ns >= existing.latest_event_at_ns)
                })
                .unwrap_or(true);
            if should_replace {
                self.decisions.insert(key, decision_state);
            }
        }

        Ok(())
    }

    pub fn snapshot(&self) -> StrategyExecutionCatchupLoopSnapshot {
        let decisions = self.decisions.values().cloned().collect::<Vec<_>>();
        let mut latest_status_totals = StrategyExecutionStatusTotals::default();
        for decision in &decisions {
            latest_status_totals.add_status(decision.latest_status);
        }
        StrategyExecutionCatchupLoopSnapshot {
            execution_run_id: self.execution_run_id.clone(),
            intent_id: self.intent_id.clone(),
            next_cursor: self.next_cursor,
            has_more: self.has_more,
            total_fact_count: self.total_fact_count,
            latest_status_totals,
            decisions,
        }
    }
}

#[allow(dead_code)]
pub fn target_signed_qty_for_intent(intent: &StrategyIntent) -> Result<i64, String> {
    target_signed_qty_for_side(&intent.side, intent.qty)
}

fn target_signed_qty_for_side(side: &str, qty: u64) -> Result<i64, String> {
    let signed_qty = i64::try_from(qty).map_err(|_| "QTY_TOO_LARGE".to_string())?;
    match side.trim().to_ascii_uppercase().as_str() {
        "BUY" => Ok(signed_qty),
        "SELL" => Ok(-signed_qty),
        _ => Err("INVALID_SIDE".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{StrategyExecutionCatchupLoop, StrategyExecutionDecisionId};
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::intent::{
        ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION, StrategyIntent,
    };
    use crate::strategy::replay::{
        StrategyExecutionCatchupInput, StrategyExecutionCatchupOrderState,
        StrategyExecutionFactStatus, StrategyExecutionReplayItem,
    };

    fn order_state(
        cursor: u64,
        decision_key: &str,
        session_seq: Option<u64>,
        qty: i64,
        status: StrategyExecutionFactStatus,
    ) -> StrategyExecutionCatchupOrderState {
        StrategyExecutionCatchupOrderState {
            cursor,
            session_id: "sess-1".to_string(),
            session_seq,
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some(decision_key.to_string()),
            decision_attempt_seq: Some(1),
            intent_id: Some(format!("intent-{decision_key}")),
            model_id: Some("model-1".to_string()),
            symbol: "AAPL".to_string(),
            position_delta_qty: Some(qty),
            latest_event_at_ns: cursor * 100,
            latest_status: status,
            reason: None,
            live_order: None,
        }
    }

    fn catchup_page(
        requested_after_cursor: u64,
        next_cursor: Option<u64>,
        has_more: bool,
        latest_order_states: Vec<StrategyExecutionCatchupOrderState>,
    ) -> StrategyExecutionCatchupInput {
        StrategyExecutionCatchupInput {
            schema_version: crate::strategy::replay::STRATEGY_EXECUTION_CATCHUP_SCHEMA_VERSION,
            execution_run_id: Some("run-1".to_string()),
            intent_id: None,
            requested_after_cursor,
            next_cursor,
            has_more,
            fact_count: latest_order_states.len(),
            facts: latest_order_states
                .iter()
                .map(|state| StrategyExecutionReplayItem {
                    cursor: state.cursor,
                    fact: crate::strategy::replay::StrategyExecutionFact::new(
                        "acc-1",
                        &state.session_id,
                        &state.symbol,
                        state.latest_event_at_ns,
                        state.latest_status,
                    )
                    .with_execution_run_id("run-1")
                    .with_decision_key(
                        state
                            .decision_key
                            .clone()
                            .unwrap_or_else(|| "fallback".to_string()),
                    )
                    .with_decision_attempt_seq(state.decision_attempt_seq.unwrap_or(1))
                    .with_intent_id(
                        state
                            .intent_id
                            .clone()
                            .unwrap_or_else(|| "fallback-intent".to_string()),
                    )
                    .with_session_seq(state.session_seq.unwrap_or_default())
                    .with_position_delta_qty(state.position_delta_qty.unwrap_or_default()),
                })
                .collect(),
            latest_order_states,
            latest_status_totals: Default::default(),
        }
    }

    fn intent_template(side: &str, qty: u64) -> StrategyIntent {
        StrategyIntent {
            schema_version: STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "template-intent-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: side.to_string(),
            order_type: OrderType::Limit,
            qty,
            limit_price: Some(100),
            time_in_force: TimeInForce::Ioc,
            urgency: IntentUrgency::High,
            execution_policy: ExecutionPolicyKind::Aggressive,
            risk_budget_ref: None,
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-template-1".to_string()),
            decision_attempt_seq: Some(1),
            decision_basis_at_ns: Some(100),
            max_decision_age_ns: Some(1_000),
            market_snapshot_id: Some("market-template-1".to_string()),
            signal_id: Some("signal-template-1".to_string()),
            recovery_policy: None,
            algo: None,
            created_at_ns: 100,
            expires_at_ns: 1_100,
        }
    }

    #[test]
    fn catchup_loop_applies_pages_and_replaces_latest_decision_state() {
        let mut loop_state = StrategyExecutionCatchupLoop::new();
        let first_page = catchup_page(
            0,
            Some(2),
            true,
            vec![
                order_state(
                    1,
                    "dec-1",
                    Some(11),
                    40,
                    StrategyExecutionFactStatus::Unconfirmed,
                ),
                order_state(2, "dec-2", None, 60, StrategyExecutionFactStatus::Rejected),
            ],
        );
        loop_state.apply_page(&first_page).expect("apply page 1");

        let second_page = catchup_page(
            2,
            Some(4),
            false,
            vec![
                order_state(
                    3,
                    "dec-1",
                    Some(12),
                    40,
                    StrategyExecutionFactStatus::DurableAccepted,
                ),
                order_state(
                    4,
                    "dec-3",
                    Some(13),
                    20,
                    StrategyExecutionFactStatus::LossSuspect,
                ),
            ],
        );
        loop_state.apply_page(&second_page).expect("apply page 2");

        let snapshot = loop_state.snapshot();
        assert_eq!(snapshot.next_cursor, 4);
        assert!(!snapshot.has_more);
        assert_eq!(snapshot.total_fact_count, 4);
        assert_eq!(snapshot.decisions.len(), 3);

        let dec_1 = snapshot
            .decisions
            .iter()
            .find(|decision| {
                decision.decision_id
                    == StrategyExecutionDecisionId::Decision {
                        execution_run_id: Some("run-1".to_string()),
                        intent_id: Some("intent-dec-1".to_string()),
                        decision_key: "dec-1".to_string(),
                    }
            })
            .expect("decision 1 exists");
        assert_eq!(
            dec_1.latest_status,
            StrategyExecutionFactStatus::DurableAccepted
        );
        assert_eq!(dec_1.session_seq, Some(12));
    }

    #[test]
    fn target_signed_qty_for_intent_preserves_side() {
        let buy = intent_template("BUY", 100);
        let sell = intent_template("SELL", 75);

        assert_eq!(
            super::target_signed_qty_for_intent(&buy).expect("buy qty"),
            100
        );
        assert_eq!(
            super::target_signed_qty_for_intent(&sell).expect("sell qty"),
            -75
        );
    }
}
