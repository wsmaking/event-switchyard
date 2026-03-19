use super::config::ExecutionPolicyConfig;
use super::feedback::{FeedbackEvent, FeedbackEventStatus};
use serde::{Deserialize, Serialize};

pub const SHADOW_RECORD_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ShadowComparisonStatus {
    Pending,
    Matched,
    Skipped,
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShadowPolicyView {
    #[serde(default)]
    pub execution_policy: Option<ExecutionPolicyConfig>,
    #[serde(default)]
    pub urgency: Option<String>,
    #[serde(default)]
    pub venue: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShadowOutcomeView {
    #[serde(default)]
    pub final_status: Option<String>,
    #[serde(default)]
    pub reject_reason: Option<String>,
    #[serde(default)]
    pub accepted_at_ns: Option<u64>,
    #[serde(default)]
    pub durable_at_ns: Option<u64>,
}

impl ShadowOutcomeView {
    pub fn from_feedback_event(event: &FeedbackEvent) -> Self {
        Self {
            final_status: event.final_status.clone(),
            reject_reason: event.reject_reason.clone(),
            accepted_at_ns: event.accepted_at_ns,
            durable_at_ns: event.durable_at_ns,
        }
    }

    pub fn is_final_event(event: &FeedbackEvent) -> bool {
        matches!(
            event.event_type,
            FeedbackEventStatus::Rejected
                | FeedbackEventStatus::DurableAccepted
                | FeedbackEventStatus::DurableRejected
                | FeedbackEventStatus::ExchangeSendFailed
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShadowScoreComponent {
    pub name: String,
    pub score_bps: i64,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShadowRecord {
    pub schema_version: u16,
    pub shadow_run_id: String,
    pub model_id: String,
    pub intent_id: String,
    pub session_id: String,
    pub session_seq: u64,
    #[serde(default)]
    pub predicted_policy: Option<ShadowPolicyView>,
    #[serde(default)]
    pub actual_policy: Option<ShadowPolicyView>,
    #[serde(default)]
    pub predicted_outcome: Option<ShadowOutcomeView>,
    #[serde(default)]
    pub actual_outcome: Option<ShadowOutcomeView>,
    #[serde(default)]
    pub score_components: Vec<ShadowScoreComponent>,
    pub evaluated_at_ns: u64,
    pub comparison_status: ShadowComparisonStatus,
}

impl ShadowRecord {
    pub fn total_score_bps(&self) -> i64 {
        self.score_components
            .iter()
            .map(|component| component.score_bps)
            .sum()
    }

    pub fn recompute_score_components(&mut self) {
        let mut next = Vec::new();
        if let (Some(predicted), Some(actual)) = (&self.predicted_policy, &self.actual_policy) {
            if predicted.execution_policy == actual.execution_policy {
                next.push(ShadowScoreComponent {
                    name: "executionPolicyMatch".to_string(),
                    score_bps: 10,
                    detail: actual
                        .execution_policy
                        .as_ref()
                        .map(|policy| format!("{:?}", policy.policy)),
                });
            } else if predicted.execution_policy.is_some() || actual.execution_policy.is_some() {
                next.push(ShadowScoreComponent {
                    name: "executionPolicyMatch".to_string(),
                    score_bps: -10,
                    detail: Some(format!(
                        "predicted={} actual={}",
                        predicted
                            .execution_policy
                            .as_ref()
                            .map(|policy| format!("{:?}", policy.policy))
                            .unwrap_or_else(|| "NONE".to_string()),
                        actual
                            .execution_policy
                            .as_ref()
                            .map(|policy| format!("{:?}", policy.policy))
                            .unwrap_or_else(|| "NONE".to_string())
                    )),
                });
            }

            if predicted.urgency == actual.urgency {
                next.push(ShadowScoreComponent {
                    name: "urgencyMatch".to_string(),
                    score_bps: 5,
                    detail: actual.urgency.clone(),
                });
            } else if predicted.urgency.is_some() || actual.urgency.is_some() {
                next.push(ShadowScoreComponent {
                    name: "urgencyMatch".to_string(),
                    score_bps: -5,
                    detail: Some(format!(
                        "predicted={} actual={}",
                        predicted.urgency.as_deref().unwrap_or("NONE"),
                        actual.urgency.as_deref().unwrap_or("NONE")
                    )),
                });
            }

            if predicted.venue == actual.venue {
                next.push(ShadowScoreComponent {
                    name: "venueMatch".to_string(),
                    score_bps: 5,
                    detail: actual.venue.clone(),
                });
            } else if predicted.venue.is_some() || actual.venue.is_some() {
                next.push(ShadowScoreComponent {
                    name: "venueMatch".to_string(),
                    score_bps: -5,
                    detail: Some(format!(
                        "predicted={} actual={}",
                        predicted.venue.as_deref().unwrap_or("NONE"),
                        actual.venue.as_deref().unwrap_or("NONE")
                    )),
                });
            }
        }

        let (Some(predicted), Some(actual)) = (&self.predicted_outcome, &self.actual_outcome)
        else {
            self.score_components = next;
            return;
        };

        let predicted_class = normalized_outcome_class(predicted.final_status.as_deref());
        let actual_class = normalized_outcome_class(actual.final_status.as_deref());
        if predicted_class == actual_class {
            next.push(ShadowScoreComponent {
                name: "outcomeClassMatch".to_string(),
                score_bps: 50,
                detail: predicted_class.map(|class| format!("matched {class}")),
            });
        } else {
            next.push(ShadowScoreComponent {
                name: "outcomeClassMatch".to_string(),
                score_bps: -50,
                detail: Some(format!(
                    "predicted={} actual={}",
                    predicted_class.unwrap_or("UNKNOWN"),
                    actual_class.unwrap_or("UNKNOWN")
                )),
            });
        }

        if predicted.final_status == actual.final_status {
            next.push(ShadowScoreComponent {
                name: "finalStatusExactMatch".to_string(),
                score_bps: 10,
                detail: actual.final_status.clone(),
            });
        } else if predicted.final_status.is_some() || actual.final_status.is_some() {
            next.push(ShadowScoreComponent {
                name: "finalStatusExactMatch".to_string(),
                score_bps: -10,
                detail: Some(format!(
                    "predicted={} actual={}",
                    predicted.final_status.as_deref().unwrap_or("NONE"),
                    actual.final_status.as_deref().unwrap_or("NONE")
                )),
            });
        }

        if predicted.reject_reason.is_some() || actual.reject_reason.is_some() {
            let score_bps = if predicted.reject_reason == actual.reject_reason {
                10
            } else {
                -10
            };
            next.push(ShadowScoreComponent {
                name: "rejectReasonMatch".to_string(),
                score_bps,
                detail: Some(format!(
                    "predicted={} actual={}",
                    predicted.reject_reason.as_deref().unwrap_or("NONE"),
                    actual.reject_reason.as_deref().unwrap_or("NONE")
                )),
            });
        }

        self.score_components = next;
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.shadow_run_id.trim().is_empty() {
            return Err("SHADOW_RUN_ID_REQUIRED");
        }
        if self.model_id.trim().is_empty() {
            return Err("MODEL_ID_REQUIRED");
        }
        if self.intent_id.trim().is_empty() {
            return Err("INTENT_ID_REQUIRED");
        }
        if self.session_id.trim().is_empty() {
            return Err("SESSION_ID_REQUIRED");
        }
        Ok(())
    }
}

fn normalized_outcome_class(final_status: Option<&str>) -> Option<&'static str> {
    match final_status {
        Some("VOLATILE_ACCEPT") | Some("DURABLE_ACCEPTED") => Some("ACCEPTED"),
        Some("REJECTED") | Some("DURABLE_REJECTED") => Some("REJECTED"),
        Some("EXCHANGE_SEND_FAILED") => Some("FAILED"),
        Some(_) => Some("OTHER"),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SHADOW_RECORD_SCHEMA_VERSION, ShadowComparisonStatus, ShadowOutcomeView, ShadowPolicyView,
        ShadowRecord, ShadowScoreComponent,
    };
    use crate::strategy::config::ExecutionPolicyConfig;
    use crate::strategy::feedback::FeedbackEvent;
    use crate::strategy::intent::ExecutionPolicyKind;

    fn shadow_record_fixture() -> ShadowRecord {
        ShadowRecord {
            schema_version: SHADOW_RECORD_SCHEMA_VERSION,
            shadow_run_id: "shadow-1".to_string(),
            model_id: "model-a".to_string(),
            intent_id: "intent-1".to_string(),
            session_id: "sess-1".to_string(),
            session_seq: 17,
            predicted_policy: Some(ShadowPolicyView {
                execution_policy: Some(ExecutionPolicyConfig {
                    policy: ExecutionPolicyKind::Passive,
                    prefer_passive: true,
                    post_only: true,
                    max_slippage_bps: Some(5),
                    participation_rate_bps: None,
                }),
                urgency: Some("HIGH".to_string()),
                venue: Some("NASDAQ".to_string()),
            }),
            actual_policy: None,
            predicted_outcome: Some(ShadowOutcomeView {
                final_status: Some("VOLATILE_ACCEPT".to_string()),
                reject_reason: None,
                accepted_at_ns: Some(100),
                durable_at_ns: None,
            }),
            actual_outcome: Some(ShadowOutcomeView {
                final_status: Some("DURABLE_ACCEPTED".to_string()),
                reject_reason: None,
                accepted_at_ns: Some(100),
                durable_at_ns: Some(160),
            }),
            score_components: vec![
                ShadowScoreComponent {
                    name: "fillQuality".to_string(),
                    score_bps: 40,
                    detail: None,
                },
                ShadowScoreComponent {
                    name: "latencyPenalty".to_string(),
                    score_bps: -10,
                    detail: Some("durable lag".to_string()),
                },
            ],
            evaluated_at_ns: 200,
            comparison_status: ShadowComparisonStatus::Matched,
        }
    }

    #[test]
    fn shadow_record_sums_component_scores() {
        let record = shadow_record_fixture();

        assert_eq!(record.validate(), Ok(()));
        assert_eq!(record.total_score_bps(), 30);
    }

    #[test]
    fn shadow_record_round_trips_json() {
        let record = shadow_record_fixture();
        let raw = serde_json::to_string(&record).expect("serialize shadow record");
        let parsed: ShadowRecord = serde_json::from_str(&raw).expect("deserialize shadow record");

        assert_eq!(parsed.shadow_run_id, "shadow-1");
        assert_eq!(parsed.session_seq, 17);
        assert_eq!(parsed.comparison_status, ShadowComparisonStatus::Matched);
        assert_eq!(parsed.total_score_bps(), 30);
    }

    #[test]
    fn shadow_outcome_view_maps_feedback_fields() {
        let event = FeedbackEvent::accepted("sess-1", 3, "acc-1", "AAPL", 100)
            .with_intent_id("intent-1")
            .durable_accepted(140);
        let outcome = ShadowOutcomeView::from_feedback_event(&event);

        assert_eq!(outcome.final_status.as_deref(), Some("DURABLE_ACCEPTED"));
        assert_eq!(outcome.reject_reason, None);
        assert_eq!(outcome.accepted_at_ns, Some(100));
        assert_eq!(outcome.durable_at_ns, Some(140));
        assert!(ShadowOutcomeView::is_final_event(&event));
    }

    #[test]
    fn shadow_record_recomputes_scores_from_predicted_and_actual_outcomes() {
        let mut record = shadow_record_fixture();
        record.score_components.clear();

        record.recompute_score_components();

        assert!(!record.score_components.is_empty());
        assert!(
            record
                .score_components
                .iter()
                .any(|component| component.name == "outcomeClassMatch")
        );
        assert_eq!(record.total_score_bps(), 40);
    }

    #[test]
    fn shadow_record_recomputes_policy_scores_when_policy_views_exist() {
        let mut record = shadow_record_fixture();
        record.actual_policy = Some(ShadowPolicyView {
            execution_policy: Some(ExecutionPolicyConfig {
                policy: ExecutionPolicyKind::Passive,
                prefer_passive: true,
                post_only: true,
                max_slippage_bps: Some(5),
                participation_rate_bps: None,
            }),
            urgency: Some("HIGH".to_string()),
            venue: Some("NASDAQ".to_string()),
        });
        record.actual_outcome = None;
        record.score_components.clear();

        record.recompute_score_components();

        assert_eq!(record.total_score_bps(), 20);
        assert!(
            record
                .score_components
                .iter()
                .any(|component| component.name == "executionPolicyMatch")
        );
    }
}
