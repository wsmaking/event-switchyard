use super::shadow::ShadowPolicyView;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeedbackEventStatus {
    Accepted,
    Rejected,
    DurableAccepted,
    DurableRejected,
    ExchangeSendFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FeedbackEvent {
    pub event_type: FeedbackEventStatus,
    pub execution_run_id: Option<String>,
    pub decision_key: Option<String>,
    pub decision_attempt_seq: Option<u64>,
    pub intent_id: Option<String>,
    pub model_id: Option<String>,
    #[serde(default)]
    pub shadow_run_ids: Vec<String>,
    #[serde(default)]
    pub effective_risk_budget_ref: Option<String>,
    #[serde(default)]
    pub actual_policy: Option<ShadowPolicyView>,
    pub session_id: String,
    pub session_seq: u64,
    pub account_id: String,
    pub symbol: String,
    pub event_at_ns: u64,
    pub accepted_at_ns: Option<u64>,
    pub durable_at_ns: Option<u64>,
    pub final_status: Option<String>,
    pub reject_reason: Option<String>,
    pub queue_delay_ns: Option<u64>,
    pub durable_delay_ns: Option<u64>,
    pub exchange_send_status: Option<String>,
    pub path_tags: Vec<String>,
}

impl FeedbackEvent {
    pub fn accepted(
        session_id: impl Into<String>,
        session_seq: u64,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
        event_at_ns: u64,
    ) -> Self {
        Self {
            event_type: FeedbackEventStatus::Accepted,
            execution_run_id: None,
            decision_key: None,
            decision_attempt_seq: None,
            intent_id: None,
            model_id: None,
            shadow_run_ids: Vec::new(),
            effective_risk_budget_ref: None,
            actual_policy: None,
            session_id: session_id.into(),
            session_seq,
            account_id: account_id.into(),
            symbol: symbol.into(),
            event_at_ns,
            accepted_at_ns: Some(event_at_ns),
            durable_at_ns: None,
            final_status: Some("VOLATILE_ACCEPT".to_string()),
            reject_reason: None,
            queue_delay_ns: None,
            durable_delay_ns: None,
            exchange_send_status: None,
            path_tags: Vec::new(),
        }
    }

    pub fn rejected(
        session_id: impl Into<String>,
        session_seq: u64,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
        event_at_ns: u64,
        reject_reason: impl Into<String>,
    ) -> Self {
        Self {
            event_type: FeedbackEventStatus::Rejected,
            execution_run_id: None,
            decision_key: None,
            decision_attempt_seq: None,
            intent_id: None,
            model_id: None,
            shadow_run_ids: Vec::new(),
            effective_risk_budget_ref: None,
            actual_policy: None,
            session_id: session_id.into(),
            session_seq,
            account_id: account_id.into(),
            symbol: symbol.into(),
            event_at_ns,
            accepted_at_ns: None,
            durable_at_ns: None,
            final_status: Some("REJECTED".to_string()),
            reject_reason: Some(reject_reason.into()),
            queue_delay_ns: None,
            durable_delay_ns: None,
            exchange_send_status: None,
            path_tags: Vec::new(),
        }
    }

    pub fn durable_accepted(mut self, durable_at_ns: u64) -> Self {
        self.event_type = FeedbackEventStatus::DurableAccepted;
        self.durable_at_ns = Some(durable_at_ns);
        self.final_status = Some("DURABLE_ACCEPTED".to_string());
        if let Some(accepted_at_ns) = self.accepted_at_ns {
            self.durable_delay_ns = Some(durable_at_ns.saturating_sub(accepted_at_ns));
        }
        self
    }

    pub fn durable_rejected(
        mut self,
        durable_at_ns: u64,
        reject_reason: impl Into<String>,
    ) -> Self {
        self.event_type = FeedbackEventStatus::DurableRejected;
        self.durable_at_ns = Some(durable_at_ns);
        self.final_status = Some("DURABLE_REJECTED".to_string());
        self.reject_reason = Some(reject_reason.into());
        if let Some(accepted_at_ns) = self.accepted_at_ns {
            self.durable_delay_ns = Some(durable_at_ns.saturating_sub(accepted_at_ns));
        }
        self
    }

    pub fn with_intent_id(mut self, intent_id: impl Into<String>) -> Self {
        self.intent_id = Some(intent_id.into());
        self
    }

    pub fn with_execution_run_id(mut self, execution_run_id: impl Into<String>) -> Self {
        self.execution_run_id = Some(execution_run_id.into());
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

    pub fn with_effective_risk_budget_ref(
        mut self,
        effective_risk_budget_ref: impl Into<String>,
    ) -> Self {
        self.effective_risk_budget_ref = Some(effective_risk_budget_ref.into());
        self
    }

    pub fn with_shadow_run_ids(mut self, shadow_run_ids: Vec<String>) -> Self {
        self.shadow_run_ids = shadow_run_ids;
        self
    }

    pub fn with_actual_policy(mut self, actual_policy: ShadowPolicyView) -> Self {
        self.actual_policy = Some(actual_policy);
        self
    }

    pub fn with_queue_delay_ns(mut self, queue_delay_ns: u64) -> Self {
        self.queue_delay_ns = Some(queue_delay_ns);
        self
    }

    pub fn with_exchange_send_status(mut self, exchange_send_status: impl Into<String>) -> Self {
        self.exchange_send_status = Some(exchange_send_status.into());
        self
    }

    pub fn push_path_tag(mut self, path_tag: impl Into<String>) -> Self {
        self.path_tags.push(path_tag.into());
        self
    }

    pub fn to_json_line(&self) -> serde_json::Result<Vec<u8>> {
        let mut out = serde_json::to_vec(self)?;
        out.push(b'\n');
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use crate::strategy::config::ExecutionPolicyConfig;
    use crate::strategy::intent::ExecutionPolicyKind;
    use crate::strategy::shadow::ShadowPolicyView;

    use super::{FeedbackEvent, FeedbackEventStatus};

    #[test]
    fn accepted_feedback_serializes_as_json_line() {
        let event = FeedbackEvent::accepted("sess-1", 7, "acc-1", "AAPL", 123)
            .with_execution_run_id("run-1")
            .with_decision_key("dec-1")
            .with_decision_attempt_seq(2)
            .with_intent_id("intent-1")
            .with_model_id("model-a")
            .with_shadow_run_ids(vec!["shadow-1".to_string(), "shadow-2".to_string()])
            .with_effective_risk_budget_ref("budget-7")
            .with_actual_policy(ShadowPolicyView {
                execution_policy: Some(ExecutionPolicyConfig {
                    policy: ExecutionPolicyKind::Passive,
                    prefer_passive: true,
                    post_only: true,
                    max_slippage_bps: Some(5),
                    participation_rate_bps: None,
                }),
                urgency: Some("HIGH".to_string()),
                venue: Some("NASDAQ".to_string()),
            })
            .with_queue_delay_ns(88)
            .with_exchange_send_status("PENDING")
            .push_path_tag("tcp_v3");

        let line = event.to_json_line().expect("serialize feedback");
        assert_eq!(line.last().copied(), Some(b'\n'));

        let value: serde_json::Value =
            serde_json::from_slice(&line[..line.len() - 1]).expect("parse serialized feedback");
        assert_eq!(value["eventType"], "ACCEPTED");
        assert_eq!(value["executionRunId"], "run-1");
        assert_eq!(value["decisionKey"], "dec-1");
        assert_eq!(value["decisionAttemptSeq"], 2);
        assert_eq!(value["intentId"], "intent-1");
        assert_eq!(value["modelId"], "model-a");
        assert_eq!(value["shadowRunIds"][0], "shadow-1");
        assert_eq!(value["shadowRunIds"][1], "shadow-2");
        assert_eq!(value["effectiveRiskBudgetRef"], "budget-7");
        assert_eq!(value["sessionId"], "sess-1");
        assert_eq!(value["sessionSeq"], 7);
        assert_eq!(value["accountId"], "acc-1");
        assert_eq!(value["symbol"], "AAPL");
        assert_eq!(value["acceptedAtNs"], 123);
        assert_eq!(value["actualPolicy"]["urgency"], "HIGH");
        assert_eq!(value["actualPolicy"]["venue"], "NASDAQ");
        assert_eq!(value["queueDelayNs"], 88);
        assert_eq!(value["exchangeSendStatus"], "PENDING");
        assert_eq!(value["pathTags"][0], "tcp_v3");
    }

    #[test]
    fn durable_rejected_feedback_sets_delay_and_reason() {
        let event = FeedbackEvent::accepted("sess-9", 11, "acc-2", "BTC", 100)
            .durable_rejected(145, "RISK_REJECT");

        assert_eq!(event.event_type, FeedbackEventStatus::DurableRejected);
        assert_eq!(event.durable_at_ns, Some(145));
        assert_eq!(event.durable_delay_ns, Some(45));
        assert_eq!(event.reject_reason.as_deref(), Some("RISK_REJECT"));
        assert_eq!(event.final_status.as_deref(), Some("DURABLE_REJECTED"));
    }

    #[test]
    fn durable_accepted_feedback_sets_delay() {
        let event = FeedbackEvent::accepted("sess-3", 5, "acc-7", "ETH", 500).durable_accepted(620);

        assert_eq!(event.event_type, FeedbackEventStatus::DurableAccepted);
        assert_eq!(event.durable_at_ns, Some(620));
        assert_eq!(event.durable_delay_ns, Some(120));
        assert_eq!(event.final_status.as_deref(), Some("DURABLE_ACCEPTED"));
        assert_eq!(event.reject_reason, None);
    }

    #[test]
    fn rejected_feedback_does_not_set_accepted_timestamp() {
        let event = FeedbackEvent::rejected("sess-2", 3, "acc-9", "ETH", 999, "INVALID_QTY");

        assert_eq!(event.event_type, FeedbackEventStatus::Rejected);
        assert_eq!(event.accepted_at_ns, None);
        assert_eq!(event.durable_at_ns, None);
        assert_eq!(event.final_status.as_deref(), Some("REJECTED"));
        assert_eq!(event.reject_reason.as_deref(), Some("INVALID_QTY"));
    }
}
