use super::intent::StrategyIntent;
use super::replay::{
    StrategyExecutionCatchupInput, StrategyExecutionCatchupOrderState, StrategyExecutionFactStatus,
    StrategyExecutionStatusTotals,
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

    pub fn build_recovery_plan(
        &self,
        target_signed_qty: i64,
        policy: StrategyExecutionRecoveryPolicy,
    ) -> StrategyExecutionRecoveryPlan {
        let snapshot = self.snapshot();
        StrategyExecutionRecoveryPlan::from_snapshot(snapshot, target_signed_qty, policy)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyExecutionUnknownQtyPolicy {
    Hold,
    Retry,
    Ignore,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionRecoveryPolicy {
    pub unconfirmed: StrategyExecutionUnknownQtyPolicy,
    pub loss_suspect: StrategyExecutionUnknownQtyPolicy,
}

impl Default for StrategyExecutionRecoveryPolicy {
    fn default() -> Self {
        Self {
            unconfirmed: StrategyExecutionUnknownQtyPolicy::Hold,
            loss_suspect: StrategyExecutionUnknownQtyPolicy::Hold,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyExecutionRecoveryAction {
    Confirmed,
    Retryable,
    Hold,
    Ignore,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionRecoveryDecision {
    pub decision: StrategyExecutionDecisionState,
    pub action: StrategyExecutionRecoveryAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionRecoveryPlan {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    pub target_signed_qty: i64,
    pub confirmed_signed_qty: i64,
    pub retryable_signed_qty: i64,
    pub blocked_signed_qty: i64,
    pub ignored_signed_qty: i64,
    pub fresh_signed_qty_capacity: i64,
    pub requires_manual_intervention: bool,
    pub next_cursor: u64,
    pub has_more: bool,
    pub latest_status_totals: StrategyExecutionStatusTotals,
    pub decisions: Vec<StrategyExecutionRecoveryDecision>,
}

impl StrategyExecutionRecoveryPlan {
    pub fn from_snapshot(
        snapshot: StrategyExecutionCatchupLoopSnapshot,
        target_signed_qty: i64,
        policy: StrategyExecutionRecoveryPolicy,
    ) -> Self {
        let mut confirmed_signed_qty = 0i64;
        let mut retryable_signed_qty = 0i64;
        let mut blocked_signed_qty = 0i64;
        let mut ignored_signed_qty = 0i64;
        let mut decisions = Vec::with_capacity(snapshot.decisions.len());

        for decision in snapshot.decisions {
            let signed_qty = decision.position_delta_qty.unwrap_or_default();
            let action = match decision.latest_status {
                StrategyExecutionFactStatus::DurableAccepted => {
                    confirmed_signed_qty = confirmed_signed_qty.saturating_add(signed_qty);
                    StrategyExecutionRecoveryAction::Confirmed
                }
                StrategyExecutionFactStatus::Rejected
                | StrategyExecutionFactStatus::DurableRejected => {
                    retryable_signed_qty = retryable_signed_qty.saturating_add(signed_qty);
                    StrategyExecutionRecoveryAction::Retryable
                }
                StrategyExecutionFactStatus::Unconfirmed => match policy.unconfirmed {
                    StrategyExecutionUnknownQtyPolicy::Hold => {
                        blocked_signed_qty = blocked_signed_qty.saturating_add(signed_qty);
                        StrategyExecutionRecoveryAction::Hold
                    }
                    StrategyExecutionUnknownQtyPolicy::Retry => {
                        retryable_signed_qty = retryable_signed_qty.saturating_add(signed_qty);
                        StrategyExecutionRecoveryAction::Retryable
                    }
                    StrategyExecutionUnknownQtyPolicy::Ignore => {
                        ignored_signed_qty = ignored_signed_qty.saturating_add(signed_qty);
                        StrategyExecutionRecoveryAction::Ignore
                    }
                },
                StrategyExecutionFactStatus::LossSuspect => match policy.loss_suspect {
                    StrategyExecutionUnknownQtyPolicy::Hold => {
                        blocked_signed_qty = blocked_signed_qty.saturating_add(signed_qty);
                        StrategyExecutionRecoveryAction::Hold
                    }
                    StrategyExecutionUnknownQtyPolicy::Retry => {
                        retryable_signed_qty = retryable_signed_qty.saturating_add(signed_qty);
                        StrategyExecutionRecoveryAction::Retryable
                    }
                    StrategyExecutionUnknownQtyPolicy::Ignore => {
                        ignored_signed_qty = ignored_signed_qty.saturating_add(signed_qty);
                        StrategyExecutionRecoveryAction::Ignore
                    }
                },
            };
            decisions.push(StrategyExecutionRecoveryDecision { decision, action });
        }

        let raw_remaining = target_signed_qty
            .saturating_sub(confirmed_signed_qty)
            .saturating_sub(blocked_signed_qty);
        let fresh_signed_qty_capacity = if target_signed_qty > 0 {
            raw_remaining.max(0)
        } else if target_signed_qty < 0 {
            raw_remaining.min(0)
        } else {
            0
        };

        Self {
            execution_run_id: snapshot.execution_run_id,
            intent_id: snapshot.intent_id,
            target_signed_qty,
            confirmed_signed_qty,
            retryable_signed_qty,
            blocked_signed_qty,
            ignored_signed_qty,
            fresh_signed_qty_capacity,
            requires_manual_intervention: blocked_signed_qty != 0,
            next_cursor: snapshot.next_cursor,
            has_more: snapshot.has_more,
            latest_status_totals: snapshot.latest_status_totals,
            decisions,
        }
    }

    pub fn propose_fresh_intent(
        &self,
        template: &StrategyIntent,
        params: StrategyExecutionFreshIntentParams,
    ) -> StrategyExecutionFreshIntentProposal {
        if let Err(reason) = params.validate() {
            return StrategyExecutionFreshIntentProposal::skipped(
                self.target_signed_qty,
                self.fresh_signed_qty_capacity,
                reason,
            );
        }
        if self.requires_manual_intervention {
            return StrategyExecutionFreshIntentProposal::skipped(
                self.target_signed_qty,
                self.fresh_signed_qty_capacity,
                "MANUAL_INTERVENTION_REQUIRED",
            );
        }
        if self.fresh_signed_qty_capacity == 0 {
            return StrategyExecutionFreshIntentProposal::skipped(
                self.target_signed_qty,
                self.fresh_signed_qty_capacity,
                "NO_FRESH_CAPACITY",
            );
        }

        let template_signed_qty = match target_signed_qty_for_side(&template.side, template.qty) {
            Ok(value) => value,
            Err(reason) => {
                return StrategyExecutionFreshIntentProposal::skipped(
                    self.target_signed_qty,
                    self.fresh_signed_qty_capacity,
                    &reason,
                );
            }
        };

        if template_signed_qty.signum() != self.fresh_signed_qty_capacity.signum() {
            return StrategyExecutionFreshIntentProposal::skipped(
                self.target_signed_qty,
                self.fresh_signed_qty_capacity,
                "SIDE_MISMATCH",
            );
        }

        let mut intent = template.clone();
        intent.intent_id = params.intent_id;
        intent.qty = self.fresh_signed_qty_capacity.unsigned_abs();
        intent.execution_run_id = self
            .execution_run_id
            .clone()
            .or_else(|| template.execution_run_id.clone());
        intent.decision_key = Some(params.decision_key);
        intent.decision_attempt_seq = Some(params.decision_attempt_seq.max(1));
        intent.created_at_ns = params.created_at_ns;
        intent.expires_at_ns = params.expires_at_ns;

        if let Err(reason) = intent.validate() {
            return StrategyExecutionFreshIntentProposal::skipped(
                self.target_signed_qty,
                self.fresh_signed_qty_capacity,
                reason,
            );
        }

        StrategyExecutionFreshIntentProposal {
            proposed: true,
            reason: None,
            target_signed_qty: self.target_signed_qty,
            fresh_signed_qty_capacity: self.fresh_signed_qty_capacity,
            intent: Some(intent),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionFreshIntentParams {
    pub intent_id: String,
    pub decision_key: String,
    pub decision_attempt_seq: u64,
    pub created_at_ns: u64,
    pub expires_at_ns: u64,
}

impl StrategyExecutionFreshIntentParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.intent_id.trim().is_empty() {
            return Err("INTENT_ID_REQUIRED");
        }
        if self.decision_key.trim().is_empty() {
            return Err("DECISION_KEY_REQUIRED");
        }
        if self.decision_attempt_seq == 0 {
            return Err("DECISION_ATTEMPT_SEQ_REQUIRED");
        }
        if self.expires_at_ns <= self.created_at_ns {
            return Err("INVALID_EXPIRY");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyExecutionFreshIntentProposal {
    pub proposed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub target_signed_qty: i64,
    pub fresh_signed_qty_capacity: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent: Option<StrategyIntent>,
}

impl StrategyExecutionFreshIntentProposal {
    fn skipped(
        target_signed_qty: i64,
        fresh_signed_qty_capacity: i64,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            proposed: false,
            reason: Some(reason.into()),
            target_signed_qty,
            fresh_signed_qty_capacity,
            intent: None,
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
    use super::{
        StrategyExecutionCatchupLoop, StrategyExecutionDecisionId,
        StrategyExecutionFreshIntentParams, StrategyExecutionRecoveryAction,
        StrategyExecutionRecoveryPolicy, StrategyExecutionUnknownQtyPolicy,
    };
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::intent::{
        ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION, StrategyIntent,
        StrategyRecoveryPolicy,
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
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
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
    fn recovery_plan_holds_unconfirmed_and_loss_suspect_by_default() {
        let mut loop_state = StrategyExecutionCatchupLoop::new();
        let page = catchup_page(
            0,
            Some(4),
            false,
            vec![
                order_state(
                    1,
                    "dec-confirmed",
                    Some(1),
                    40,
                    StrategyExecutionFactStatus::DurableAccepted,
                ),
                order_state(
                    2,
                    "dec-rejected",
                    None,
                    20,
                    StrategyExecutionFactStatus::Rejected,
                ),
                order_state(
                    3,
                    "dec-unconfirmed",
                    Some(3),
                    30,
                    StrategyExecutionFactStatus::Unconfirmed,
                ),
                order_state(
                    4,
                    "dec-loss",
                    Some(4),
                    10,
                    StrategyExecutionFactStatus::LossSuspect,
                ),
            ],
        );
        loop_state.apply_page(&page).expect("apply page");

        let plan = loop_state.build_recovery_plan(100, StrategyExecutionRecoveryPolicy::default());
        assert_eq!(plan.confirmed_signed_qty, 40);
        assert_eq!(plan.retryable_signed_qty, 20);
        assert_eq!(plan.blocked_signed_qty, 40);
        assert_eq!(plan.fresh_signed_qty_capacity, 20);
        assert!(plan.requires_manual_intervention);
        assert_eq!(plan.latest_status_totals.durable_accepted, 1);
        assert_eq!(plan.latest_status_totals.loss_suspect, 1);

        let blocked = plan
            .decisions
            .iter()
            .filter(|decision| decision.action == StrategyExecutionRecoveryAction::Hold)
            .count();
        assert_eq!(blocked, 2);
    }

    #[test]
    fn recovery_plan_can_retry_unknown_qty_when_policy_allows_it() {
        let mut loop_state = StrategyExecutionCatchupLoop::new();
        let page = catchup_page(
            0,
            Some(2),
            false,
            vec![
                order_state(
                    1,
                    "dec-unconfirmed",
                    Some(1),
                    -30,
                    StrategyExecutionFactStatus::Unconfirmed,
                ),
                order_state(
                    2,
                    "dec-loss",
                    Some(2),
                    -20,
                    StrategyExecutionFactStatus::LossSuspect,
                ),
            ],
        );
        loop_state.apply_page(&page).expect("apply page");

        let plan = loop_state.build_recovery_plan(
            -100,
            StrategyExecutionRecoveryPolicy {
                unconfirmed: StrategyExecutionUnknownQtyPolicy::Retry,
                loss_suspect: StrategyExecutionUnknownQtyPolicy::Ignore,
            },
        );
        assert_eq!(plan.confirmed_signed_qty, 0);
        assert_eq!(plan.retryable_signed_qty, -30);
        assert_eq!(plan.blocked_signed_qty, 0);
        assert_eq!(plan.ignored_signed_qty, -20);
        assert_eq!(plan.fresh_signed_qty_capacity, -100);
        assert!(!plan.requires_manual_intervention);
    }

    #[test]
    fn recovery_plan_proposes_fresh_intent_from_template() {
        let mut loop_state = StrategyExecutionCatchupLoop::new();
        let page = catchup_page(
            0,
            Some(2),
            false,
            vec![
                order_state(
                    1,
                    "dec-confirmed",
                    Some(1),
                    40,
                    StrategyExecutionFactStatus::DurableAccepted,
                ),
                order_state(
                    2,
                    "dec-rejected",
                    None,
                    20,
                    StrategyExecutionFactStatus::Rejected,
                ),
            ],
        );
        loop_state.apply_page(&page).expect("apply page");

        let plan = loop_state.build_recovery_plan(100, StrategyExecutionRecoveryPolicy::default());
        let proposal = plan.propose_fresh_intent(
            &intent_template("BUY", 100),
            StrategyExecutionFreshIntentParams {
                intent_id: "fresh-intent-1".to_string(),
                decision_key: "fresh-decision-1".to_string(),
                decision_attempt_seq: 1,
                created_at_ns: 1_000,
                expires_at_ns: 2_000,
            },
        );

        assert!(proposal.proposed);
        assert_eq!(proposal.reason, None);
        assert_eq!(proposal.fresh_signed_qty_capacity, 60);
        let intent = proposal.intent.expect("proposal intent");
        assert_eq!(intent.intent_id, "fresh-intent-1");
        assert_eq!(intent.qty, 60);
        assert_eq!(intent.execution_run_id.as_deref(), Some("run-1"));
        assert_eq!(intent.decision_key.as_deref(), Some("fresh-decision-1"));
        assert_eq!(intent.decision_attempt_seq, Some(1));
        assert_eq!(intent.created_at_ns, 1_000);
        assert_eq!(intent.expires_at_ns, 2_000);
    }

    #[test]
    fn recovery_plan_skips_fresh_intent_when_manual_intervention_is_required() {
        let mut loop_state = StrategyExecutionCatchupLoop::new();
        let page = catchup_page(
            0,
            Some(1),
            false,
            vec![order_state(
                1,
                "dec-unconfirmed",
                Some(1),
                30,
                StrategyExecutionFactStatus::Unconfirmed,
            )],
        );
        loop_state.apply_page(&page).expect("apply page");

        let plan = loop_state.build_recovery_plan(100, StrategyExecutionRecoveryPolicy::default());
        let proposal = plan.propose_fresh_intent(
            &intent_template("BUY", 100),
            StrategyExecutionFreshIntentParams {
                intent_id: "fresh-intent-1".to_string(),
                decision_key: "fresh-decision-1".to_string(),
                decision_attempt_seq: 1,
                created_at_ns: 1_000,
                expires_at_ns: 2_000,
            },
        );

        assert!(!proposal.proposed);
        assert_eq!(
            proposal.reason.as_deref(),
            Some("MANUAL_INTERVENTION_REQUIRED")
        );
        assert!(proposal.intent.is_none());
    }

    #[test]
    fn recovery_plan_skips_fresh_intent_when_side_does_not_match_capacity() {
        let mut loop_state = StrategyExecutionCatchupLoop::new();
        let page = catchup_page(
            0,
            Some(1),
            false,
            vec![order_state(
                1,
                "dec-confirmed",
                Some(1),
                -20,
                StrategyExecutionFactStatus::DurableAccepted,
            )],
        );
        loop_state.apply_page(&page).expect("apply page");

        let plan = loop_state.build_recovery_plan(-100, StrategyExecutionRecoveryPolicy::default());
        let proposal = plan.propose_fresh_intent(
            &intent_template("BUY", 100),
            StrategyExecutionFreshIntentParams {
                intent_id: "fresh-intent-1".to_string(),
                decision_key: "fresh-decision-1".to_string(),
                decision_attempt_seq: 1,
                created_at_ns: 1_000,
                expires_at_ns: 2_000,
            },
        );

        assert!(!proposal.proposed);
        assert_eq!(proposal.reason.as_deref(), Some("SIDE_MISMATCH"));
        assert!(proposal.intent.is_none());
    }
}
