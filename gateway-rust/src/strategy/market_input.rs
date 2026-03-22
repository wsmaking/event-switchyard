use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyMarketInput {
    pub desired_signed_qty: i64,
    #[serde(default)]
    pub observed_at_ns: Option<u64>,
    #[serde(default)]
    pub max_decision_age_ns: Option<u64>,
    #[serde(default)]
    pub market_snapshot_id: Option<String>,
    #[serde(default)]
    pub signal_id: Option<String>,
}

impl StrategyMarketInput {
    pub fn validate(&self) -> Result<(), &'static str> {
        if let Some(max_decision_age_ns) = self.max_decision_age_ns {
            if max_decision_age_ns == 0 {
                return Err("MARKET_INPUT_MAX_DECISION_AGE_NS_MUST_BE_POSITIVE");
            }
        }
        if let Some(market_snapshot_id) = self.market_snapshot_id.as_deref() {
            if market_snapshot_id.trim().is_empty() {
                return Err("MARKET_INPUT_MARKET_SNAPSHOT_ID_REQUIRED");
            }
        }
        if let Some(signal_id) = self.signal_id.as_deref() {
            if signal_id.trim().is_empty() {
                return Err("MARKET_INPUT_SIGNAL_ID_REQUIRED");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::StrategyMarketInput;

    #[test]
    fn market_input_rejects_zero_max_age() {
        let input = StrategyMarketInput {
            desired_signed_qty: 10,
            observed_at_ns: Some(1),
            max_decision_age_ns: Some(0),
            market_snapshot_id: Some("snap-1".to_string()),
            signal_id: Some("sig-1".to_string()),
        };

        assert_eq!(
            input.validate(),
            Err("MARKET_INPUT_MAX_DECISION_AGE_NS_MUST_BE_POSITIVE")
        );
    }

    #[test]
    fn market_input_accepts_minimal_payload() {
        let input = StrategyMarketInput {
            desired_signed_qty: -5,
            observed_at_ns: None,
            max_decision_age_ns: None,
            market_snapshot_id: None,
            signal_id: None,
        };

        assert_eq!(input.validate(), Ok(()));
    }
}
