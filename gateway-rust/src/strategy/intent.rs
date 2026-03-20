use crate::order::{OrderType, TimeInForce};
use serde::{Deserialize, Serialize};

pub const STRATEGY_INTENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyRecoveryPolicy {
    GatewayManagedResume,
    NoAutoResume,
}

impl Default for StrategyRecoveryPolicy {
    fn default() -> Self {
        Self::NoAutoResume
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IntentUrgency {
    Low,
    Normal,
    High,
    Critical,
}

impl Default for IntentUrgency {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionPolicyKind {
    Default,
    Passive,
    Aggressive,
    Twap,
    Vwap,
    Pov,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct AlgoExecutionSpec {
    #[serde(default)]
    pub slice_count: Option<u32>,
    #[serde(default)]
    pub slice_interval_ns: Option<u64>,
    #[serde(default)]
    pub volume_curve_bps: Vec<u32>,
    #[serde(default)]
    pub expected_market_volume: Vec<u64>,
    #[serde(default)]
    pub participation_target_bps: Option<u32>,
    #[serde(default)]
    pub start_at_ns: Option<u64>,
}

impl Default for ExecutionPolicyKind {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RiskBudgetRef {
    pub budget_id: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyIntent {
    pub schema_version: u16,
    pub intent_id: String,
    pub account_id: String,
    pub session_id: String,
    pub symbol: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub qty: u64,
    pub limit_price: Option<u64>,
    #[serde(default)]
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub urgency: IntentUrgency,
    #[serde(default)]
    pub execution_policy: ExecutionPolicyKind,
    #[serde(default)]
    pub risk_budget_ref: Option<RiskBudgetRef>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub execution_run_id: Option<String>,
    #[serde(default)]
    pub recovery_policy: Option<StrategyRecoveryPolicy>,
    #[serde(default)]
    pub algo: Option<AlgoExecutionSpec>,
    pub created_at_ns: u64,
    pub expires_at_ns: u64,
}

impl StrategyIntent {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.intent_id.trim().is_empty() {
            return Err("INTENT_ID_REQUIRED");
        }
        if self.account_id.trim().is_empty() {
            return Err("ACCOUNT_ID_REQUIRED");
        }
        if self.session_id.trim().is_empty() {
            return Err("SESSION_ID_REQUIRED");
        }
        if self.symbol.trim().is_empty() {
            return Err("SYMBOL_REQUIRED");
        }
        if self.qty == 0 {
            return Err("QTY_REQUIRED");
        }
        if !matches!(
            self.side.trim().to_ascii_uppercase().as_str(),
            "BUY" | "SELL"
        ) {
            return Err("INVALID_SIDE");
        }
        if self.expires_at_ns <= self.created_at_ns {
            return Err("INVALID_EXPIRY");
        }
        if self.order_type == OrderType::Limit && self.limit_price.unwrap_or(0) == 0 {
            return Err("LIMIT_PRICE_REQUIRED");
        }
        if self
            .execution_run_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err("EXECUTION_RUN_ID_REQUIRED");
        }
        Ok(())
    }

    pub fn is_expired(&self, now_ns: u64) -> bool {
        now_ns >= self.expires_at_ns
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AlgoExecutionSpec, ExecutionPolicyKind, IntentUrgency, RiskBudgetRef, StrategyIntent,
        StrategyRecoveryPolicy,
    };
    use crate::order::{OrderType, TimeInForce};

    fn intent_fixture() -> StrategyIntent {
        StrategyIntent {
            schema_version: super::STRATEGY_INTENT_SCHEMA_VERSION,
            intent_id: "intent-1".to_string(),
            account_id: "acc-1".to_string(),
            session_id: "sess-1".to_string(),
            symbol: "AAPL".to_string(),
            side: "BUY".to_string(),
            order_type: OrderType::Limit,
            qty: 100,
            limit_price: Some(15_000),
            time_in_force: TimeInForce::Ioc,
            urgency: IntentUrgency::High,
            execution_policy: ExecutionPolicyKind::Passive,
            risk_budget_ref: Some(RiskBudgetRef {
                budget_id: "budget-1".to_string(),
                version: 7,
            }),
            model_id: Some("model-1".to_string()),
            execution_run_id: Some("run-1".to_string()),
            recovery_policy: Some(StrategyRecoveryPolicy::NoAutoResume),
            algo: Some(AlgoExecutionSpec {
                slice_count: Some(4),
                slice_interval_ns: Some(100),
                volume_curve_bps: vec![],
                expected_market_volume: vec![],
                participation_target_bps: None,
                start_at_ns: Some(1_000),
            }),
            created_at_ns: 10,
            expires_at_ns: 20,
        }
    }

    #[test]
    fn strategy_intent_validates_happy_path() {
        let intent = intent_fixture();
        assert_eq!(intent.validate(), Ok(()));
        assert!(!intent.is_expired(19));
        assert!(intent.is_expired(20));
    }

    #[test]
    fn strategy_intent_rejects_invalid_limit_price() {
        let mut intent = intent_fixture();
        intent.limit_price = None;

        assert_eq!(intent.validate(), Err("LIMIT_PRICE_REQUIRED"));
    }

    #[test]
    fn strategy_intent_round_trips_json() {
        let intent = intent_fixture();
        let raw = serde_json::to_string(&intent).expect("serialize intent");
        let parsed: StrategyIntent = serde_json::from_str(&raw).expect("deserialize intent");

        assert_eq!(parsed.intent_id, "intent-1");
        assert_eq!(parsed.execution_policy, ExecutionPolicyKind::Passive);
        assert_eq!(parsed.urgency, IntentUrgency::High);
        assert_eq!(parsed.time_in_force, TimeInForce::Ioc);
        assert_eq!(parsed.execution_run_id.as_deref(), Some("run-1"));
        assert_eq!(
            parsed.recovery_policy,
            Some(StrategyRecoveryPolicy::NoAutoResume)
        );
        assert_eq!(parsed.risk_budget_ref.as_ref().map(|v| v.version), Some(7));
        assert_eq!(
            parsed.algo.as_ref().and_then(|algo| algo.slice_count),
            Some(4)
        );
    }
}
