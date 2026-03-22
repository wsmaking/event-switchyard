#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyTargetScope {
    ExecutionRunId(String),
    IntentId(String),
}

impl StrategyTargetScope {
    pub fn endpoint_path(&self) -> &'static str {
        match self {
            Self::ExecutionRunId(_) => "/strategy/catchup/execution",
            Self::IntentId(_) => "/strategy/catchup/intent",
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::ExecutionRunId(value) | Self::IntentId(value) => value,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::ExecutionRunId(_) => "executionRunId",
            Self::IntentId(_) => "intentId",
        }
    }
}
