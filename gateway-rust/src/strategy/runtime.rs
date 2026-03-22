use serde::{Deserialize, Serialize};

use crate::order::OrderRequest;

use super::algo::{AlgoExecutionPlan, AlgoExecutionSlice};
use super::intent::ExecutionPolicyKind;
use super::shadow::ShadowPolicyView;

pub const STRATEGY_ALGO_RUNTIME_SCHEMA_VERSION: u16 = 1;
pub const STRATEGY_ALGO_RUNTIME_SNAPSHOT_EVENT_TYPE: &str = "StrategyAlgoRuntimeSnapshot";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlgoParentStatus {
    Scheduled,
    Running,
    Paused,
    Completed,
    Rejected,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlgoChildStatus {
    Scheduled,
    Dispatching,
    VolatileAccepted,
    DurableAccepted,
    Rejected,
    Skipped,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlgoRuntimeMode {
    GatewayManagedResume,
    #[serde(alias = "NO_AUTO_RESUME")]
    PauseOnRestart,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlgoChildExecution {
    pub child_intent_id: String,
    pub sequence: u32,
    pub qty: u64,
    pub send_at_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight_bps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_market_volume: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub participation_target_bps: Option<u32>,
    pub status: AlgoChildStatus,
    #[serde(default)]
    pub session_seq: Option<u64>,
    #[serde(default)]
    pub received_at_ns: Option<u64>,
    #[serde(default)]
    pub durable_at_ns: Option<u64>,
    #[serde(default)]
    pub reject_reason: Option<String>,
}

impl From<&AlgoExecutionSlice> for AlgoChildExecution {
    fn from(value: &AlgoExecutionSlice) -> Self {
        Self {
            child_intent_id: value.child_intent_id.clone(),
            sequence: value.sequence,
            qty: value.qty,
            send_at_ns: value.send_at_ns,
            weight_bps: value.weight_bps,
            expected_market_volume: value.expected_market_volume,
            participation_target_bps: value.participation_target_bps,
            status: AlgoChildStatus::Scheduled,
            session_seq: None,
            received_at_ns: None,
            durable_at_ns: None,
            reject_reason: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlgoParentExecution {
    pub schema_version: u16,
    pub parent_intent_id: String,
    pub account_id: String,
    pub session_id: String,
    pub symbol: String,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub execution_run_id: Option<String>,
    pub policy: ExecutionPolicyKind,
    #[serde(rename = "runtimeMode", alias = "recoveryPolicy")]
    pub runtime_mode: AlgoRuntimeMode,
    pub total_qty: u64,
    pub child_count: u32,
    pub status: AlgoParentStatus,
    pub base_order_request: OrderRequest,
    #[serde(default)]
    pub effective_risk_budget_ref: Option<String>,
    #[serde(default)]
    pub actual_policy: Option<ShadowPolicyView>,
    #[serde(default)]
    pub shadow_run_id: Option<String>,
    #[serde(default)]
    pub accepted_at_ns: Option<u64>,
    #[serde(default)]
    pub completed_at_ns: Option<u64>,
    #[serde(default)]
    pub final_reason: Option<String>,
    pub created_at_ns: u64,
    pub last_updated_at_ns: u64,
    pub slices: Vec<AlgoChildExecution>,
}

impl AlgoParentExecution {
    pub fn from_plan(
        plan: &AlgoExecutionPlan,
        account_id: String,
        session_id: String,
        symbol: String,
        model_id: Option<String>,
        execution_run_id: Option<String>,
        runtime_mode: AlgoRuntimeMode,
        base_order_request: OrderRequest,
        effective_risk_budget_ref: Option<String>,
        actual_policy: Option<ShadowPolicyView>,
        shadow_run_id: Option<String>,
        created_at_ns: u64,
    ) -> Self {
        Self {
            schema_version: STRATEGY_ALGO_RUNTIME_SCHEMA_VERSION,
            parent_intent_id: plan.parent_intent_id.clone(),
            account_id,
            session_id,
            symbol,
            model_id,
            execution_run_id,
            policy: plan.policy,
            runtime_mode,
            total_qty: plan.total_qty,
            child_count: plan.child_count,
            status: AlgoParentStatus::Scheduled,
            base_order_request,
            effective_risk_budget_ref,
            actual_policy,
            shadow_run_id,
            accepted_at_ns: None,
            completed_at_ns: None,
            final_reason: None,
            created_at_ns,
            last_updated_at_ns: created_at_ns,
            slices: plan.slices.iter().map(AlgoChildExecution::from).collect(),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            AlgoParentStatus::Completed | AlgoParentStatus::Rejected
        )
    }

    pub fn accepted_child_count(&self) -> u64 {
        self.slices
            .iter()
            .filter(|slice| {
                matches!(
                    slice.status,
                    AlgoChildStatus::VolatileAccepted | AlgoChildStatus::DurableAccepted
                )
            })
            .count() as u64
    }

    pub fn durable_accepted_child_count(&self) -> u64 {
        self.slices
            .iter()
            .filter(|slice| matches!(slice.status, AlgoChildStatus::DurableAccepted))
            .count() as u64
    }

    pub fn rejected_child_count(&self) -> u64 {
        self.slices
            .iter()
            .filter(|slice| matches!(slice.status, AlgoChildStatus::Rejected))
            .count() as u64
    }

    pub fn skipped_child_count(&self) -> u64 {
        self.slices
            .iter()
            .filter(|slice| matches!(slice.status, AlgoChildStatus::Skipped))
            .count() as u64
    }

    pub fn scheduled_child_count(&self) -> u64 {
        self.slices
            .iter()
            .filter(|slice| matches!(slice.status, AlgoChildStatus::Scheduled))
            .count() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::{AlgoParentExecution, AlgoParentStatus, AlgoRuntimeMode};
    use crate::order::{OrderRequest, OrderType, TimeInForce};
    use crate::strategy::algo::{
        AlgoExecutionPlan, AlgoExecutionSlice, STRATEGY_ALGO_PLAN_SCHEMA_VERSION,
    };
    use crate::strategy::intent::ExecutionPolicyKind;

    #[test]
    fn parent_execution_builds_from_plan() {
        let plan = AlgoExecutionPlan {
            schema_version: STRATEGY_ALGO_PLAN_SCHEMA_VERSION,
            parent_intent_id: "intent-1".to_string(),
            policy: ExecutionPolicyKind::Twap,
            total_qty: 100,
            child_count: 2,
            start_at_ns: 10,
            slice_interval_ns: 5,
            slices: vec![
                AlgoExecutionSlice {
                    child_intent_id: "intent-1::child-01".to_string(),
                    sequence: 1,
                    qty: 50,
                    send_at_ns: 10,
                    weight_bps: None,
                    expected_market_volume: None,
                    participation_target_bps: None,
                },
                AlgoExecutionSlice {
                    child_intent_id: "intent-1::child-02".to_string(),
                    sequence: 2,
                    qty: 50,
                    send_at_ns: 15,
                    weight_bps: None,
                    expected_market_volume: None,
                    participation_target_bps: None,
                },
            ],
        };

        let execution = AlgoParentExecution::from_plan(
            &plan,
            "acc-1".to_string(),
            "sess-1".to_string(),
            "AAPL".to_string(),
            Some("model-1".to_string()),
            Some("run-1".to_string()),
            AlgoRuntimeMode::GatewayManagedResume,
            OrderRequest {
                symbol: "AAPL".to_string(),
                side: "BUY".to_string(),
                order_type: OrderType::Limit,
                qty: 100,
                price: Some(101),
                time_in_force: TimeInForce::Gtc,
                expire_at: None,
                client_order_id: None,
                intent_id: Some("intent-1".to_string()),
                model_id: Some("model-1".to_string()),
                execution_run_id: Some("run-1".to_string()),
                decision_key: Some("decision-1".to_string()),
                decision_attempt_seq: Some(1),
            },
            Some("budget-1".to_string()),
            None,
            Some("shadow-1".to_string()),
            99,
        );

        assert_eq!(execution.parent_intent_id, "intent-1");
        assert_eq!(execution.status, AlgoParentStatus::Scheduled);
        assert_eq!(
            execution.runtime_mode,
            AlgoRuntimeMode::GatewayManagedResume
        );
        assert_eq!(execution.child_count, 2);
        assert_eq!(execution.slices.len(), 2);
        assert_eq!(execution.accepted_child_count(), 0);
    }
}
