use super::intent::{ExecutionPolicyKind, IntentUrgency};
use serde::{Deserialize, Serialize};

pub const STRATEGY_CONFIG_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionPolicyConfig {
    pub policy: ExecutionPolicyKind,
    pub prefer_passive: bool,
    pub post_only: bool,
    pub max_slippage_bps: Option<u32>,
    pub participation_rate_bps: Option<u32>,
}

impl Default for ExecutionPolicyConfig {
    fn default() -> Self {
        Self {
            policy: ExecutionPolicyKind::Default,
            prefer_passive: true,
            post_only: false,
            max_slippage_bps: None,
            participation_rate_bps: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SymbolExecutionOverride {
    pub symbol: String,
    #[serde(default)]
    pub execution_policy: Option<ExecutionPolicyConfig>,
    #[serde(default)]
    pub urgency_override: Option<IntentUrgency>,
    #[serde(default)]
    pub max_order_qty: Option<u64>,
    #[serde(default)]
    pub max_notional: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccountRiskBudget {
    pub account_id: String,
    #[serde(default)]
    pub budget_ref: Option<String>,
    #[serde(default)]
    pub max_notional: Option<u64>,
    #[serde(default)]
    pub max_abs_position_qty: Option<u64>,
    #[serde(default)]
    pub order_rate_limit_per_sec: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UrgencyOverride {
    pub account_id: String,
    pub urgency: IntentUrgency,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VenuePreference {
    pub symbol: String,
    pub venue: String,
    pub rank: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KillSwitchPolicy {
    pub reject_when_snapshot_stale: bool,
    pub reject_when_shadow_stale: bool,
    pub snapshot_stale_after_ns: u64,
}

impl Default for KillSwitchPolicy {
    fn default() -> Self {
        Self {
            reject_when_snapshot_stale: false,
            reject_when_shadow_stale: false,
            snapshot_stale_after_ns: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionConfigSnapshot {
    pub schema_version: u16,
    pub snapshot_id: String,
    pub version: u64,
    pub applied_at_ns: u64,
    #[serde(default)]
    pub default_execution_policy: ExecutionPolicyConfig,
    #[serde(default)]
    pub symbol_limits: Vec<SymbolExecutionOverride>,
    #[serde(default)]
    pub risk_budget_by_account: Vec<AccountRiskBudget>,
    #[serde(default)]
    pub urgency_overrides: Vec<UrgencyOverride>,
    #[serde(default)]
    pub venue_preference: Vec<VenuePreference>,
    #[serde(default)]
    pub kill_switch_policy: KillSwitchPolicy,
    #[serde(default)]
    pub shadow_enabled: bool,
}

impl Default for ExecutionConfigSnapshot {
    fn default() -> Self {
        Self {
            schema_version: STRATEGY_CONFIG_SCHEMA_VERSION,
            snapshot_id: "default".to_string(),
            version: 0,
            applied_at_ns: 0,
            default_execution_policy: ExecutionPolicyConfig::default(),
            symbol_limits: Vec::new(),
            risk_budget_by_account: Vec::new(),
            urgency_overrides: Vec::new(),
            venue_preference: Vec::new(),
            kill_switch_policy: KillSwitchPolicy::default(),
            shadow_enabled: false,
        }
    }
}

impl ExecutionConfigSnapshot {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.snapshot_id.trim().is_empty() {
            return Err("SNAPSHOT_ID_REQUIRED");
        }
        if self
            .symbol_limits
            .iter()
            .any(|override_cfg| override_cfg.symbol.trim().is_empty())
        {
            return Err("SYMBOL_OVERRIDE_REQUIRES_SYMBOL");
        }
        if self
            .risk_budget_by_account
            .iter()
            .any(|budget| budget.account_id.trim().is_empty())
        {
            return Err("ACCOUNT_RISK_BUDGET_REQUIRES_ACCOUNT");
        }
        Ok(())
    }

    pub fn is_stale_at(&self, now_ns: u64) -> bool {
        self.kill_switch_policy.reject_when_snapshot_stale
            && self.kill_switch_policy.snapshot_stale_after_ns > 0
            && self.applied_at_ns > 0
            && now_ns.saturating_sub(self.applied_at_ns)
                > self.kill_switch_policy.snapshot_stale_after_ns
    }

    pub fn symbol_override(&self, symbol: &str) -> Option<&SymbolExecutionOverride> {
        self.symbol_limits
            .iter()
            .find(|override_cfg| override_cfg.symbol == symbol)
    }

    pub fn account_risk_budget(&self, account_id: &str) -> Option<&AccountRiskBudget> {
        self.risk_budget_by_account
            .iter()
            .find(|budget| budget.account_id == account_id)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AccountRiskBudget, ExecutionConfigSnapshot, ExecutionPolicyConfig, KillSwitchPolicy,
        SymbolExecutionOverride, VenuePreference,
    };
    use crate::strategy::intent::{ExecutionPolicyKind, IntentUrgency};

    #[test]
    fn config_snapshot_defaults_are_stable() {
        let snapshot = ExecutionConfigSnapshot::default();

        assert_eq!(
            snapshot.schema_version,
            super::STRATEGY_CONFIG_SCHEMA_VERSION
        );
        assert_eq!(snapshot.version, 0);
        assert!(!snapshot.shadow_enabled);
        assert_eq!(
            snapshot.default_execution_policy.policy,
            ExecutionPolicyKind::Default
        );
    }

    #[test]
    fn config_snapshot_validates_populated_payload() {
        let snapshot = ExecutionConfigSnapshot {
            snapshot_id: "snapshot-1".to_string(),
            version: 3,
            applied_at_ns: 123,
            default_execution_policy: ExecutionPolicyConfig {
                policy: ExecutionPolicyKind::Passive,
                prefer_passive: true,
                post_only: true,
                max_slippage_bps: Some(5),
                participation_rate_bps: Some(2500),
            },
            symbol_limits: vec![SymbolExecutionOverride {
                symbol: "AAPL".to_string(),
                execution_policy: None,
                urgency_override: Some(IntentUrgency::High),
                max_order_qty: Some(500),
                max_notional: Some(10_000_000),
            }],
            risk_budget_by_account: vec![AccountRiskBudget {
                account_id: "acc-1".to_string(),
                budget_ref: Some("budget-7".to_string()),
                max_notional: Some(20_000_000),
                max_abs_position_qty: Some(5_000),
                order_rate_limit_per_sec: Some(500),
            }],
            urgency_overrides: Vec::new(),
            venue_preference: vec![VenuePreference {
                symbol: "AAPL".to_string(),
                venue: "NASDAQ".to_string(),
                rank: 1,
            }],
            kill_switch_policy: KillSwitchPolicy {
                reject_when_snapshot_stale: true,
                reject_when_shadow_stale: false,
                snapshot_stale_after_ns: 5_000_000_000,
            },
            shadow_enabled: true,
            ..ExecutionConfigSnapshot::default()
        };

        assert_eq!(snapshot.validate(), Ok(()));
    }

    #[test]
    fn config_snapshot_round_trips_json() {
        let snapshot = ExecutionConfigSnapshot {
            snapshot_id: "snapshot-2".to_string(),
            version: 9,
            applied_at_ns: 777,
            shadow_enabled: true,
            ..ExecutionConfigSnapshot::default()
        };

        let raw = serde_json::to_string(&snapshot).expect("serialize snapshot");
        let parsed: ExecutionConfigSnapshot =
            serde_json::from_str(&raw).expect("deserialize snapshot");

        assert_eq!(parsed.snapshot_id, "snapshot-2");
        assert_eq!(parsed.version, 9);
        assert!(parsed.shadow_enabled);
    }

    #[test]
    fn config_snapshot_staleness_and_lookup_helpers_work() {
        let snapshot = ExecutionConfigSnapshot {
            snapshot_id: "snapshot-3".to_string(),
            version: 4,
            applied_at_ns: 100,
            symbol_limits: vec![SymbolExecutionOverride {
                symbol: "AAPL".to_string(),
                execution_policy: None,
                urgency_override: None,
                max_order_qty: Some(50),
                max_notional: Some(1_000_000),
            }],
            risk_budget_by_account: vec![AccountRiskBudget {
                account_id: "acc-7".to_string(),
                budget_ref: None,
                max_notional: Some(2_000_000),
                max_abs_position_qty: None,
                order_rate_limit_per_sec: None,
            }],
            kill_switch_policy: KillSwitchPolicy {
                reject_when_snapshot_stale: true,
                reject_when_shadow_stale: false,
                snapshot_stale_after_ns: 25,
            },
            ..ExecutionConfigSnapshot::default()
        };

        assert!(!snapshot.is_stale_at(125));
        assert!(snapshot.is_stale_at(126));
        assert_eq!(
            snapshot
                .symbol_override("AAPL")
                .and_then(|override_cfg| override_cfg.max_order_qty),
            Some(50)
        );
        assert_eq!(
            snapshot
                .account_risk_budget("acc-7")
                .and_then(|budget| budget.max_notional),
            Some(2_000_000)
        );
    }
}
