use super::catchup::{
    StrategyExecutionCatchupLoopSnapshot, StrategyExecutionDecisionState,
    target_signed_qty_for_intent,
};
use super::intent::{StrategyIntent, StrategyRecoveryPolicy};
use super::replay::{StrategyExecutionFactStatus, StrategyExecutionStatusTotals};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlphaDecisionClass {
    Filled,
    Open,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlphaUnknownExposureReason {
    Unconfirmed,
    LossSuspect,
    ResidualUnknown,
    #[default]
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlphaRecoveryDecision {
    pub decision: StrategyExecutionDecisionState,
    pub class: AlphaDecisionClass,
    pub filled_signed_qty: i64,
    pub open_signed_qty: i64,
    pub failed_signed_qty: i64,
    pub unknown_signed_qty: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unknown_reason: Option<AlphaUnknownExposureReason>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct AlphaUnknownExposureBreakdown {
    pub unconfirmed_signed_qty: i64,
    pub loss_suspect_signed_qty: i64,
    pub residual_unknown_signed_qty: i64,
}

impl AlphaUnknownExposureBreakdown {
    fn total_unknown_signed_qty(&self) -> i64 {
        self.unconfirmed_signed_qty
            .saturating_add(self.loss_suspect_signed_qty)
            .saturating_add(self.residual_unknown_signed_qty)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlphaRecoveryOperatorStatus {
    ReadyForReDecision,
    HoldUnconfirmedExposure,
    HoldLossSuspectExposure,
    HoldMixedUnknownExposure,
    HoldResidualUnknownExposure,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlphaRecoveryContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    pub target_signed_qty: i64,
    pub filled_signed_qty: i64,
    pub open_signed_qty: i64,
    pub failed_signed_qty: i64,
    pub unknown_signed_qty: i64,
    pub unknown_exposure_breakdown: AlphaUnknownExposureBreakdown,
    pub unsent_signed_qty: i64,
    pub requires_manual_intervention: bool,
    pub operator_status: AlphaRecoveryOperatorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator_reason: Option<String>,
    pub next_cursor: u64,
    pub has_more: bool,
    pub latest_status_totals: StrategyExecutionStatusTotals,
    pub decisions: Vec<AlphaRecoveryDecision>,
}

impl AlphaRecoveryContext {
    pub fn committed_signed_qty(&self) -> i64 {
        self.filled_signed_qty.saturating_add(self.open_signed_qty)
    }

    pub fn from_snapshot(
        snapshot: StrategyExecutionCatchupLoopSnapshot,
        target_signed_qty: i64,
    ) -> Self {
        let mut filled_signed_qty = 0i64;
        let mut open_signed_qty = 0i64;
        let mut failed_signed_qty = 0i64;
        let mut unknown_signed_qty = 0i64;
        let mut unknown_exposure_breakdown = AlphaUnknownExposureBreakdown::default();
        let mut decisions = Vec::with_capacity(snapshot.decisions.len());

        for decision in snapshot.decisions {
            let classified = classify_recovery_decision(decision);
            filled_signed_qty = filled_signed_qty.saturating_add(classified.filled_signed_qty);
            open_signed_qty = open_signed_qty.saturating_add(classified.open_signed_qty);
            failed_signed_qty = failed_signed_qty.saturating_add(classified.failed_signed_qty);
            unknown_signed_qty = unknown_signed_qty.saturating_add(classified.unknown_signed_qty);
            match classified.unknown_reason {
                Some(AlphaUnknownExposureReason::Unconfirmed) => {
                    unknown_exposure_breakdown.unconfirmed_signed_qty = unknown_exposure_breakdown
                        .unconfirmed_signed_qty
                        .saturating_add(classified.unknown_signed_qty);
                }
                Some(AlphaUnknownExposureReason::LossSuspect) => {
                    unknown_exposure_breakdown.loss_suspect_signed_qty = unknown_exposure_breakdown
                        .loss_suspect_signed_qty
                        .saturating_add(classified.unknown_signed_qty);
                }
                Some(AlphaUnknownExposureReason::ResidualUnknown) => {
                    unknown_exposure_breakdown.residual_unknown_signed_qty =
                        unknown_exposure_breakdown
                            .residual_unknown_signed_qty
                            .saturating_add(classified.unknown_signed_qty);
                }
                Some(AlphaUnknownExposureReason::None) | None => {}
            }
            decisions.push(classified);
        }

        let raw_unsent_signed_qty = target_signed_qty
            .saturating_sub(filled_signed_qty)
            .saturating_sub(open_signed_qty)
            .saturating_sub(failed_signed_qty)
            .saturating_sub(unknown_signed_qty);
        let unsent_signed_qty = if target_signed_qty > 0 {
            raw_unsent_signed_qty.max(0)
        } else if target_signed_qty < 0 {
            raw_unsent_signed_qty.min(0)
        } else {
            0
        };
        let (operator_status, operator_reason) =
            classify_operator_status(&unknown_exposure_breakdown);

        Self {
            execution_run_id: snapshot.execution_run_id,
            intent_id: snapshot.intent_id,
            target_signed_qty,
            filled_signed_qty,
            open_signed_qty,
            failed_signed_qty,
            unknown_signed_qty,
            unknown_exposure_breakdown,
            unsent_signed_qty,
            requires_manual_intervention: operator_status
                != AlphaRecoveryOperatorStatus::ReadyForReDecision,
            operator_status,
            operator_reason,
            next_cursor: snapshot.next_cursor,
            has_more: snapshot.has_more,
            latest_status_totals: snapshot.latest_status_totals,
            decisions,
        }
    }
}

fn classify_recovery_decision(decision: StrategyExecutionDecisionState) -> AlphaRecoveryDecision {
    let sign = decision_sign(&decision);
    let basis_abs_qty = decision_abs_qty(&decision);
    let mut filled_signed_qty = 0i64;
    let mut open_signed_qty = 0i64;
    let mut failed_signed_qty = 0i64;
    let mut unknown_signed_qty = 0i64;
    let mut unknown_reason = None;

    let class = if let Some(live_order) = decision.live_order.as_ref() {
        let filled_abs_qty = live_order.filled_qty.min(basis_abs_qty);
        filled_signed_qty = signed_qty_from_abs(sign, filled_abs_qty);

        let remaining_budget = basis_abs_qty.saturating_sub(filled_abs_qty);
        let open_abs_qty = if live_order.is_terminal {
            0
        } else {
            live_order.remaining_qty.min(remaining_budget)
        };
        open_signed_qty = signed_qty_from_abs(sign, open_abs_qty);

        let residual_abs_qty = basis_abs_qty
            .saturating_sub(filled_abs_qty)
            .saturating_sub(open_abs_qty);
        if live_order.is_terminal && residual_abs_qty > 0 {
            failed_signed_qty = signed_qty_from_abs(sign, residual_abs_qty);
        }

        if open_abs_qty > 0 {
            AlphaDecisionClass::Open
        } else if filled_abs_qty > 0 {
            AlphaDecisionClass::Filled
        } else if live_order.is_terminal {
            failed_signed_qty = signed_qty_from_abs(sign, residual_abs_qty.max(basis_abs_qty));
            AlphaDecisionClass::Failed
        } else {
            unknown_signed_qty = signed_qty_from_abs(sign, basis_abs_qty);
            unknown_reason = Some(AlphaUnknownExposureReason::ResidualUnknown);
            AlphaDecisionClass::Unknown
        }
    } else {
        let decision_signed_qty = signed_qty_from_abs(sign, basis_abs_qty);
        match decision.latest_status {
            StrategyExecutionFactStatus::Rejected
            | StrategyExecutionFactStatus::DurableRejected => {
                failed_signed_qty = decision_signed_qty;
                AlphaDecisionClass::Failed
            }
            StrategyExecutionFactStatus::DurableAccepted => {
                open_signed_qty = decision_signed_qty;
                AlphaDecisionClass::Open
            }
            StrategyExecutionFactStatus::Unconfirmed => {
                unknown_signed_qty = decision_signed_qty;
                unknown_reason = Some(AlphaUnknownExposureReason::Unconfirmed);
                AlphaDecisionClass::Unknown
            }
            StrategyExecutionFactStatus::LossSuspect => {
                unknown_signed_qty = decision_signed_qty;
                unknown_reason = Some(AlphaUnknownExposureReason::LossSuspect);
                AlphaDecisionClass::Unknown
            }
        }
    };

    AlphaRecoveryDecision {
        decision,
        class,
        filled_signed_qty,
        open_signed_qty,
        failed_signed_qty,
        unknown_signed_qty,
        unknown_reason,
    }
}

fn classify_operator_status(
    breakdown: &AlphaUnknownExposureBreakdown,
) -> (AlphaRecoveryOperatorStatus, Option<String>) {
    let has_unconfirmed = breakdown.unconfirmed_signed_qty != 0;
    let has_loss_suspect = breakdown.loss_suspect_signed_qty != 0;
    let has_residual = breakdown.residual_unknown_signed_qty != 0;
    let total_unknown = breakdown.total_unknown_signed_qty();
    if total_unknown == 0 {
        return (AlphaRecoveryOperatorStatus::ReadyForReDecision, None);
    }
    if has_residual {
        return (
            AlphaRecoveryOperatorStatus::HoldResidualUnknownExposure,
            Some("RESIDUAL_UNKNOWN_EXPOSURE_PRESENT".to_string()),
        );
    }
    if has_unconfirmed && has_loss_suspect {
        return (
            AlphaRecoveryOperatorStatus::HoldMixedUnknownExposure,
            Some("MIXED_UNKNOWN_EXPOSURE_PRESENT".to_string()),
        );
    }
    if has_unconfirmed {
        return (
            AlphaRecoveryOperatorStatus::HoldUnconfirmedExposure,
            Some("UNCONFIRMED_EXPOSURE_PRESENT".to_string()),
        );
    }
    (
        AlphaRecoveryOperatorStatus::HoldLossSuspectExposure,
        Some("LOSS_SUSPECT_EXPOSURE_PRESENT".to_string()),
    )
}

fn decision_sign(decision: &StrategyExecutionDecisionState) -> i64 {
    let signed_qty = decision.position_delta_qty.unwrap_or_default();
    if signed_qty != 0 {
        return signed_qty.signum();
    }
    let Some(live_order) = decision.live_order.as_ref() else {
        return 0;
    };
    match live_order.side.trim().to_ascii_uppercase().as_str() {
        "BUY" => 1,
        "SELL" => -1,
        _ => 0,
    }
}

fn decision_abs_qty(decision: &StrategyExecutionDecisionState) -> u64 {
    let decision_abs_qty = decision
        .position_delta_qty
        .map(|value| value.unsigned_abs())
        .unwrap_or_default();
    let live_order_qty = decision
        .live_order
        .as_ref()
        .map(|live_order| live_order.qty)
        .unwrap_or_default();
    decision_abs_qty.max(live_order_qty)
}

fn signed_qty_from_abs(sign: i64, abs_qty: u64) -> i64 {
    let bounded_abs_qty = i64::try_from(abs_qty).unwrap_or(i64::MAX);
    if sign < 0 {
        -bounded_abs_qty
    } else if sign > 0 {
        bounded_abs_qty
    } else {
        0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlphaMarketContext {
    pub observed_at_ns: u64,
    pub desired_signed_qty: i64,
    pub max_decision_age_ns: u64,
    #[serde(default)]
    pub market_snapshot_id: Option<String>,
    #[serde(default)]
    pub signal_id: Option<String>,
}

impl AlphaMarketContext {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.observed_at_ns == 0 {
            return Err("MARKET_OBSERVED_AT_NS_REQUIRED");
        }
        if self.max_decision_age_ns == 0 {
            return Err("MAX_DECISION_AGE_NS_REQUIRED");
        }
        if self
            .market_snapshot_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err("MARKET_SNAPSHOT_ID_REQUIRED");
        }
        if self
            .signal_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err("SIGNAL_ID_REQUIRED");
        }
        Ok(())
    }

    pub fn is_stale_at(&self, now_ns: u64) -> bool {
        now_ns >= self.observed_at_ns
            && now_ns.saturating_sub(self.observed_at_ns) > self.max_decision_age_ns
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlphaNextIntentParams {
    pub intent_id: String,
    pub decision_key: String,
    pub decision_attempt_seq: u64,
    pub created_at_ns: u64,
    #[serde(default)]
    pub expires_at_ns: Option<u64>,
}

impl AlphaNextIntentParams {
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
        if self.created_at_ns == 0 {
            return Err("CREATED_AT_NS_REQUIRED");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AlphaReDecisionInput {
    pub template_intent: StrategyIntent,
    pub recovery: AlphaRecoveryContext,
    pub market: AlphaMarketContext,
    pub next_intent: AlphaNextIntentParams,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlphaReDecisionAction {
    InvalidInput,
    AbortAlphaStale,
    HoldUnknownExposure,
    AbortSideFlip,
    NoopNoSignal,
    NoopAlreadySatisfied,
    SubmitFreshIntent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AlphaReDecision {
    pub action: AlphaReDecisionAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub desired_signed_qty: i64,
    pub effective_desired_signed_qty: i64,
    pub residual_signed_qty: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposed_intent: Option<StrategyIntent>,
}

impl AlphaReDecision {
    pub fn evaluate(input: AlphaReDecisionInput, now_ns: u64) -> Self {
        if let Err(reason) = input.template_intent.validate() {
            return Self::invalid(reason);
        }
        if let Err(reason) = input.market.validate() {
            return Self::invalid(reason);
        }
        if let Err(reason) = input.next_intent.validate() {
            return Self::invalid(reason);
        }
        if input.market.is_stale_at(now_ns) {
            return Self {
                action: AlphaReDecisionAction::AbortAlphaStale,
                reason: Some("STRATEGY_INTENT_ALPHA_STALE".to_string()),
                desired_signed_qty: input.market.desired_signed_qty,
                effective_desired_signed_qty: 0,
                residual_signed_qty: 0,
                proposed_intent: None,
            };
        }
        if input.recovery.requires_manual_intervention {
            return Self {
                action: AlphaReDecisionAction::HoldUnknownExposure,
                reason: input
                    .recovery
                    .operator_reason
                    .clone()
                    .or_else(|| Some("UNKNOWN_EXPOSURE_PRESENT".to_string())),
                desired_signed_qty: input.market.desired_signed_qty,
                effective_desired_signed_qty: 0,
                residual_signed_qty: 0,
                proposed_intent: None,
            };
        }

        let template_signed_qty = match target_signed_qty_for_intent(&input.template_intent) {
            Ok(value) => value,
            Err(reason) => return Self::invalid(&reason),
        };
        let target_sign = template_signed_qty.signum();
        if input.market.desired_signed_qty == 0 {
            return Self {
                action: AlphaReDecisionAction::NoopNoSignal,
                reason: Some("NO_DESIRED_QTY".to_string()),
                desired_signed_qty: 0,
                effective_desired_signed_qty: 0,
                residual_signed_qty: 0,
                proposed_intent: None,
            };
        }
        if input.market.desired_signed_qty.signum() != target_sign {
            return Self {
                action: AlphaReDecisionAction::AbortSideFlip,
                reason: Some("SIDE_FLIP_REQUIRES_NEW_RUN".to_string()),
                desired_signed_qty: input.market.desired_signed_qty,
                effective_desired_signed_qty: 0,
                residual_signed_qty: 0,
                proposed_intent: None,
            };
        }

        let effective_desired_signed_qty = clamp_signed_qty_to_target(
            input.market.desired_signed_qty,
            input.recovery.target_signed_qty,
        );
        let raw_residual_signed_qty =
            effective_desired_signed_qty.saturating_sub(input.recovery.committed_signed_qty());
        let residual_signed_qty = if target_sign > 0 {
            raw_residual_signed_qty.max(0)
        } else if target_sign < 0 {
            raw_residual_signed_qty.min(0)
        } else {
            0
        };
        if residual_signed_qty == 0 {
            return Self {
                action: AlphaReDecisionAction::NoopAlreadySatisfied,
                reason: Some("LIVE_COMMITTED_QTY_ALREADY_SATISFIES_DESIRED_QTY".to_string()),
                desired_signed_qty: input.market.desired_signed_qty,
                effective_desired_signed_qty,
                residual_signed_qty: 0,
                proposed_intent: None,
            };
        }

        let mut intent = input.template_intent.clone();
        intent.intent_id = input.next_intent.intent_id;
        intent.execution_run_id = input
            .recovery
            .execution_run_id
            .clone()
            .or_else(|| intent.execution_run_id.clone());
        intent.decision_key = Some(input.next_intent.decision_key);
        intent.decision_attempt_seq = Some(input.next_intent.decision_attempt_seq);
        intent.decision_basis_at_ns = Some(input.market.observed_at_ns);
        intent.max_decision_age_ns = Some(input.market.max_decision_age_ns);
        intent.market_snapshot_id = input.market.market_snapshot_id.clone();
        intent.signal_id = input.market.signal_id.clone();
        intent.recovery_policy = Some(StrategyRecoveryPolicy::NoAutoResume);
        intent.qty = residual_signed_qty.unsigned_abs();
        intent.algo = None;
        intent.created_at_ns = input.next_intent.created_at_ns;
        intent.expires_at_ns = input.next_intent.expires_at_ns.unwrap_or_else(|| {
            input
                .next_intent
                .created_at_ns
                .saturating_add(input.market.max_decision_age_ns.max(1))
        });

        if let Err(reason) = intent.validate() {
            return Self::invalid(reason);
        }
        if let Err(reason) = intent.validate_alpha_freshness(now_ns) {
            return Self::invalid(reason);
        }

        Self {
            action: AlphaReDecisionAction::SubmitFreshIntent,
            reason: None,
            desired_signed_qty: input.market.desired_signed_qty,
            effective_desired_signed_qty,
            residual_signed_qty,
            proposed_intent: Some(intent),
        }
    }

    fn invalid(reason: impl Into<String>) -> Self {
        Self {
            action: AlphaReDecisionAction::InvalidInput,
            reason: Some(reason.into()),
            desired_signed_qty: 0,
            effective_desired_signed_qty: 0,
            residual_signed_qty: 0,
            proposed_intent: None,
        }
    }
}

fn clamp_signed_qty_to_target(desired_signed_qty: i64, target_signed_qty: i64) -> i64 {
    if target_signed_qty > 0 {
        desired_signed_qty.min(target_signed_qty).max(0)
    } else if target_signed_qty < 0 {
        desired_signed_qty.max(target_signed_qty).min(0)
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AlphaDecisionClass, AlphaMarketContext, AlphaNextIntentParams, AlphaReDecision,
        AlphaReDecisionAction, AlphaReDecisionInput, AlphaRecoveryContext,
        AlphaRecoveryOperatorStatus, AlphaUnknownExposureBreakdown, AlphaUnknownExposureReason,
    };
    use crate::order::{OrderType, TimeInForce};
    use crate::strategy::catchup::{
        StrategyExecutionCatchupLoopSnapshot, StrategyExecutionDecisionState,
    };
    use crate::strategy::intent::{
        ExecutionPolicyKind, IntentUrgency, STRATEGY_INTENT_SCHEMA_VERSION, StrategyIntent,
        StrategyRecoveryPolicy,
    };
    use crate::strategy::replay::{
        StrategyExecutionFactStatus, StrategyExecutionLiveOrderState, StrategyExecutionStatusTotals,
    };

    fn decision(
        cursor: u64,
        status: StrategyExecutionFactStatus,
        qty: i64,
    ) -> StrategyExecutionDecisionState {
        StrategyExecutionDecisionState {
            decision_id: crate::strategy::catchup::StrategyExecutionDecisionId::Decision {
                execution_run_id: Some("run-1".to_string()),
                intent_id: Some(format!("intent-{cursor}")),
                decision_key: format!("decision-{cursor}"),
            },
            cursor,
            session_id: "sess-1".to_string(),
            session_seq: Some(cursor),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some(format!("decision-{cursor}")),
            decision_attempt_seq: Some(1),
            intent_id: Some(format!("intent-{cursor}")),
            model_id: Some("model-1".to_string()),
            symbol: "AAPL".to_string(),
            position_delta_qty: Some(qty),
            latest_event_at_ns: cursor * 10,
            latest_status: status,
            reason: None,
            live_order: None,
        }
    }

    fn live_order(
        side: &str,
        status: &str,
        qty: u64,
        filled_qty: u64,
    ) -> StrategyExecutionLiveOrderState {
        let remaining_qty = qty.saturating_sub(filled_qty.min(qty));
        StrategyExecutionLiveOrderState {
            order_id: "v3/sess-1/1".to_string(),
            side: side.to_string(),
            status: status.to_string(),
            qty,
            filled_qty: filled_qty.min(qty),
            remaining_qty,
            accepted_at_ns: 1_000,
            last_update_at_ns: 2_000,
            is_terminal: matches!(status, "FILLED" | "CANCELED" | "REJECTED"),
        }
    }

    fn template_intent() -> StrategyIntent {
        StrategyIntent {
            schema_version: STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "template-intent".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 100,
            limit_price: Some(100),
            time_in_force: TimeInForce::Ioc,
            urgency: IntentUrgency::High,
            execution_policy: ExecutionPolicyKind::Aggressive,
            risk_budget_ref: None,
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            decision_key: Some("decision-template".to_string()),
            decision_attempt_seq: Some(1),
            decision_basis_at_ns: Some(100),
            max_decision_age_ns: Some(1_000),
            market_snapshot_id: Some("market-template".to_string()),
            signal_id: Some("signal-template".to_string()),
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
            algo: None,
            created_at_ns: 100,
            expires_at_ns: 1_100,
        }
    }

    #[test]
    fn recovery_context_separates_filled_open_failed_unknown_and_unsent() {
        let snapshot = StrategyExecutionCatchupLoopSnapshot {
            execution_run_id: Some("run-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            next_cursor: 4,
            has_more: false,
            total_fact_count: 4,
            latest_status_totals: StrategyExecutionStatusTotals {
                durable_accepted: 1,
                rejected: 1,
                unconfirmed: 1,
                durable_rejected: 0,
                loss_suspect: 1,
            },
            decisions: vec![
                decision(1, StrategyExecutionFactStatus::DurableAccepted, 40),
                decision(2, StrategyExecutionFactStatus::Rejected, 20),
                decision(3, StrategyExecutionFactStatus::Unconfirmed, 10),
                decision(4, StrategyExecutionFactStatus::LossSuspect, 5),
            ],
        };

        let context = AlphaRecoveryContext::from_snapshot(snapshot, 100);

        assert_eq!(context.filled_signed_qty, 0);
        assert_eq!(context.open_signed_qty, 40);
        assert_eq!(context.failed_signed_qty, 20);
        assert_eq!(context.unknown_signed_qty, 15);
        assert_eq!(context.unsent_signed_qty, 25);
        assert!(context.requires_manual_intervention);
        assert_eq!(
            context.unknown_exposure_breakdown,
            AlphaUnknownExposureBreakdown {
                unconfirmed_signed_qty: 10,
                loss_suspect_signed_qty: 5,
                residual_unknown_signed_qty: 0,
            }
        );
        assert_eq!(
            context.operator_status,
            AlphaRecoveryOperatorStatus::HoldMixedUnknownExposure
        );
        assert_eq!(context.decisions[0].class, AlphaDecisionClass::Open);
        assert_eq!(context.decisions[1].class, AlphaDecisionClass::Failed);
        assert_eq!(context.decisions[2].class, AlphaDecisionClass::Unknown);
        assert_eq!(
            context.decisions[2].unknown_reason,
            Some(AlphaUnknownExposureReason::Unconfirmed)
        );
        assert_eq!(
            context.decisions[3].unknown_reason,
            Some(AlphaUnknownExposureReason::LossSuspect)
        );
    }

    #[test]
    fn recovery_context_reconciles_live_fill_and_open_qty() {
        let mut reconciled = decision(1, StrategyExecutionFactStatus::LossSuspect, 20);
        reconciled.live_order = Some(live_order("BUY", "PARTIALLY_FILLED", 20, 5));

        let mut terminal = decision(2, StrategyExecutionFactStatus::DurableAccepted, 30);
        terminal.live_order = Some(live_order("BUY", "FILLED", 30, 30));

        let snapshot = StrategyExecutionCatchupLoopSnapshot {
            execution_run_id: Some("run-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            next_cursor: 2,
            has_more: false,
            total_fact_count: 2,
            latest_status_totals: Default::default(),
            decisions: vec![reconciled, terminal],
        };

        let context = AlphaRecoveryContext::from_snapshot(snapshot, 100);

        assert_eq!(context.filled_signed_qty, 35);
        assert_eq!(context.open_signed_qty, 15);
        assert_eq!(context.failed_signed_qty, 0);
        assert_eq!(context.unknown_signed_qty, 0);
        assert_eq!(context.unsent_signed_qty, 50);
        assert!(!context.requires_manual_intervention);
        assert_eq!(context.decisions[0].class, AlphaDecisionClass::Open);
        assert_eq!(context.decisions[0].filled_signed_qty, 5);
        assert_eq!(context.decisions[0].open_signed_qty, 15);
        assert_eq!(context.decisions[1].class, AlphaDecisionClass::Filled);
    }

    #[test]
    fn redecision_holds_unknown_exposure() {
        let recovery = AlphaRecoveryContext {
            execution_run_id: Some("run-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            target_signed_qty: 100,
            filled_signed_qty: 40,
            open_signed_qty: 0,
            failed_signed_qty: 20,
            unknown_signed_qty: 10,
            unknown_exposure_breakdown: AlphaUnknownExposureBreakdown {
                unconfirmed_signed_qty: 10,
                loss_suspect_signed_qty: 0,
                residual_unknown_signed_qty: 0,
            },
            unsent_signed_qty: 30,
            requires_manual_intervention: true,
            operator_status: AlphaRecoveryOperatorStatus::HoldUnconfirmedExposure,
            operator_reason: Some("UNCONFIRMED_EXPOSURE_PRESENT".to_string()),
            next_cursor: 1,
            has_more: false,
            latest_status_totals: Default::default(),
            decisions: vec![],
        };

        let outcome = AlphaReDecision::evaluate(
            AlphaReDecisionInput {
                template_intent: template_intent(),
                recovery,
                market: AlphaMarketContext {
                    observed_at_ns: 1_000,
                    desired_signed_qty: 80,
                    max_decision_age_ns: 100,
                    market_snapshot_id: Some("market-1".to_string()),
                    signal_id: Some("signal-1".to_string()),
                },
                next_intent: AlphaNextIntentParams {
                    intent_id: "fresh-intent-1".to_string(),
                    decision_key: "fresh-decision-1".to_string(),
                    decision_attempt_seq: 1,
                    created_at_ns: 1_000,
                    expires_at_ns: None,
                },
            },
            1_010,
        );

        assert_eq!(outcome.action, AlphaReDecisionAction::HoldUnknownExposure);
        assert_eq!(
            outcome.reason.as_deref(),
            Some("UNCONFIRMED_EXPOSURE_PRESENT")
        );
        assert!(outcome.proposed_intent.is_none());
    }

    #[test]
    fn redecision_aborts_stale_market_context() {
        let recovery = AlphaRecoveryContext {
            execution_run_id: Some("run-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            target_signed_qty: 100,
            filled_signed_qty: 40,
            open_signed_qty: 0,
            failed_signed_qty: 20,
            unknown_signed_qty: 0,
            unknown_exposure_breakdown: AlphaUnknownExposureBreakdown::default(),
            unsent_signed_qty: 40,
            requires_manual_intervention: false,
            operator_status: AlphaRecoveryOperatorStatus::ReadyForReDecision,
            operator_reason: None,
            next_cursor: 1,
            has_more: false,
            latest_status_totals: Default::default(),
            decisions: vec![],
        };

        let outcome = AlphaReDecision::evaluate(
            AlphaReDecisionInput {
                template_intent: template_intent(),
                recovery,
                market: AlphaMarketContext {
                    observed_at_ns: 1_000,
                    desired_signed_qty: 80,
                    max_decision_age_ns: 5,
                    market_snapshot_id: Some("market-1".to_string()),
                    signal_id: Some("signal-1".to_string()),
                },
                next_intent: AlphaNextIntentParams {
                    intent_id: "fresh-intent-1".to_string(),
                    decision_key: "fresh-decision-1".to_string(),
                    decision_attempt_seq: 1,
                    created_at_ns: 1_000,
                    expires_at_ns: None,
                },
            },
            1_006,
        );

        assert_eq!(outcome.action, AlphaReDecisionAction::AbortAlphaStale);
        assert!(outcome.proposed_intent.is_none());
    }

    #[test]
    fn redecision_builds_fresh_intent_from_current_market_desired_qty() {
        let recovery = AlphaRecoveryContext {
            execution_run_id: Some("run-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            target_signed_qty: 100,
            filled_signed_qty: 10,
            open_signed_qty: 30,
            failed_signed_qty: 20,
            unknown_signed_qty: 0,
            unknown_exposure_breakdown: AlphaUnknownExposureBreakdown::default(),
            unsent_signed_qty: 40,
            requires_manual_intervention: false,
            operator_status: AlphaRecoveryOperatorStatus::ReadyForReDecision,
            operator_reason: None,
            next_cursor: 7,
            has_more: false,
            latest_status_totals: Default::default(),
            decisions: vec![],
        };

        let outcome = AlphaReDecision::evaluate(
            AlphaReDecisionInput {
                template_intent: template_intent(),
                recovery,
                market: AlphaMarketContext {
                    observed_at_ns: 1_000,
                    desired_signed_qty: 70,
                    max_decision_age_ns: 100,
                    market_snapshot_id: Some("market-7".to_string()),
                    signal_id: Some("signal-7".to_string()),
                },
                next_intent: AlphaNextIntentParams {
                    intent_id: "fresh-intent-7".to_string(),
                    decision_key: "fresh-decision-7".to_string(),
                    decision_attempt_seq: 1,
                    created_at_ns: 1_005,
                    expires_at_ns: None,
                },
            },
            1_010,
        );

        assert_eq!(outcome.action, AlphaReDecisionAction::SubmitFreshIntent);
        assert_eq!(outcome.effective_desired_signed_qty, 70);
        assert_eq!(outcome.residual_signed_qty, 30);
        let intent = outcome.proposed_intent.expect("fresh intent");
        assert_eq!(intent.qty, 30);
        assert_eq!(intent.intent_id, "fresh-intent-7");
        assert_eq!(intent.decision_key.as_deref(), Some("fresh-decision-7"));
        assert_eq!(intent.decision_basis_at_ns, Some(1_000));
        assert_eq!(intent.max_decision_age_ns, Some(100));
        assert_eq!(intent.market_snapshot_id.as_deref(), Some("market-7"));
        assert_eq!(intent.signal_id.as_deref(), Some("signal-7"));
        assert_eq!(
            intent.recovery_policy,
            Some(StrategyRecoveryPolicy::NoAutoResume)
        );
    }
}
