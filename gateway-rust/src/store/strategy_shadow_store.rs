use crate::strategy::feedback::FeedbackEvent;
use crate::strategy::shadow::{ShadowComparisonStatus, ShadowOutcomeView, ShadowRecord};
use serde::Serialize;
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StrategyShadowMetrics {
    pub record_count: u64,
    pub upsert_total: u64,
    pub feedback_apply_total: u64,
    pub pending_count: u64,
    pub matched_count: u64,
    pub skipped_count: u64,
    pub timed_out_count: u64,
    pub negative_score_count: u64,
    pub zero_score_count: u64,
    pub positive_score_count: u64,
    pub last_evaluated_at_ns: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StrategyShadowRunSummary {
    pub shadow_run_id: String,
    pub record_count: u64,
    pub pending_count: u64,
    pub matched_count: u64,
    pub skipped_count: u64,
    pub timed_out_count: u64,
    pub negative_score_count: u64,
    pub zero_score_count: u64,
    pub positive_score_count: u64,
    pub total_score_bps: i64,
    pub average_score_bps: f64,
    pub min_score_bps: i64,
    pub max_score_bps: i64,
    pub last_evaluated_at_ns: u64,
    pub top_positive: Vec<StrategyShadowScoreSample>,
    pub top_negative: Vec<StrategyShadowScoreSample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyShadowScoreSample {
    pub intent_id: String,
    pub model_id: String,
    pub score_bps: i64,
    pub comparison_status: ShadowComparisonStatus,
    pub evaluated_at_ns: u64,
}

pub struct StrategyShadowStore {
    records: RwLock<HashMap<(String, String), ShadowRecord>>,
    intent_index: RwLock<HashMap<String, Vec<String>>>,
    record_count: AtomicU64,
    upsert_total: AtomicU64,
    feedback_apply_total: AtomicU64,
}

impl Default for StrategyShadowStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StrategyShadowStore {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            intent_index: RwLock::new(HashMap::new()),
            record_count: AtomicU64::new(0),
            upsert_total: AtomicU64::new(0),
            feedback_apply_total: AtomicU64::new(0),
        }
    }

    pub fn upsert(&self, record: ShadowRecord) -> Result<Option<ShadowRecord>, &'static str> {
        record.validate()?;
        let key = (record.shadow_run_id.clone(), record.intent_id.clone());
        let run_id = record.shadow_run_id.clone();
        let intent_id = record.intent_id.clone();
        let mut guard = self
            .records
            .write()
            .expect("strategy shadow store write lock poisoned");
        let previous = guard.insert(key, record);
        if previous.is_none() {
            self.record_count.fetch_add(1, Ordering::Relaxed);
            let mut index = self
                .intent_index
                .write()
                .expect("strategy shadow intent index write lock poisoned");
            let entry = index.entry(intent_id).or_default();
            if !entry.iter().any(|value| value == &run_id) {
                entry.push(run_id);
            }
        }
        self.upsert_total.fetch_add(1, Ordering::Relaxed);
        Ok(previous)
    }

    pub fn get(&self, shadow_run_id: &str, intent_id: &str) -> Option<ShadowRecord> {
        self.records
            .read()
            .expect("strategy shadow store read lock poisoned")
            .get(&(shadow_run_id.to_string(), intent_id.to_string()))
            .cloned()
    }

    pub fn size(&self) -> u64 {
        self.record_count.load(Ordering::Relaxed)
    }

    pub fn upsert_total(&self) -> u64 {
        self.upsert_total.load(Ordering::Relaxed)
    }

    pub fn feedback_apply_total(&self) -> u64 {
        self.feedback_apply_total.load(Ordering::Relaxed)
    }

    pub fn metrics(&self) -> StrategyShadowMetrics {
        let guard = self
            .records
            .read()
            .expect("strategy shadow store read lock poisoned");
        let mut metrics = StrategyShadowMetrics {
            record_count: self.record_count.load(Ordering::Relaxed),
            upsert_total: self.upsert_total.load(Ordering::Relaxed),
            feedback_apply_total: self.feedback_apply_total.load(Ordering::Relaxed),
            ..StrategyShadowMetrics::default()
        };
        for record in guard.values() {
            metrics.last_evaluated_at_ns = metrics.last_evaluated_at_ns.max(record.evaluated_at_ns);
            match record.comparison_status {
                ShadowComparisonStatus::Pending => metrics.pending_count += 1,
                ShadowComparisonStatus::Matched => metrics.matched_count += 1,
                ShadowComparisonStatus::Skipped => metrics.skipped_count += 1,
                ShadowComparisonStatus::TimedOut => metrics.timed_out_count += 1,
            }
            match record.total_score_bps().cmp(&0) {
                CmpOrdering::Less => metrics.negative_score_count += 1,
                CmpOrdering::Equal => metrics.zero_score_count += 1,
                CmpOrdering::Greater => metrics.positive_score_count += 1,
            }
        }
        metrics
    }

    pub fn run_ids_for_intent(&self, intent_id: &str) -> Vec<String> {
        self.intent_index
            .read()
            .expect("strategy shadow intent index read lock poisoned")
            .get(intent_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn summary_for_run(&self, shadow_run_id: &str) -> Option<StrategyShadowRunSummary> {
        let guard = self
            .records
            .read()
            .expect("strategy shadow store read lock poisoned");
        let mut records = guard
            .values()
            .filter(|record| record.shadow_run_id == shadow_run_id);
        let first = records.next()?.clone();
        let first_score = first.total_score_bps();
        let mut summary = StrategyShadowRunSummary {
            shadow_run_id: shadow_run_id.to_string(),
            record_count: 0,
            pending_count: 0,
            matched_count: 0,
            skipped_count: 0,
            timed_out_count: 0,
            negative_score_count: 0,
            zero_score_count: 0,
            positive_score_count: 0,
            total_score_bps: first_score,
            average_score_bps: 0.0,
            min_score_bps: first_score,
            max_score_bps: first_score,
            last_evaluated_at_ns: first.evaluated_at_ns,
            top_positive: Vec::new(),
            top_negative: Vec::new(),
        };
        summary.record_count += 1;
        summary.count_record(&first, first_score);
        summary.push_sample(&first, first_score);
        for record in records {
            let score = record.total_score_bps();
            summary.record_count += 1;
            summary.total_score_bps += score;
            summary.min_score_bps = summary.min_score_bps.min(score);
            summary.max_score_bps = summary.max_score_bps.max(score);
            summary.last_evaluated_at_ns = summary.last_evaluated_at_ns.max(record.evaluated_at_ns);
            summary.count_record(record, score);
            summary.push_sample(record, score);
        }
        summary.average_score_bps = summary.total_score_bps as f64 / summary.record_count as f64;
        summary
            .top_positive
            .sort_by(|a, b| b.score_bps.cmp(&a.score_bps).then_with(|| b.evaluated_at_ns.cmp(&a.evaluated_at_ns)));
        summary
            .top_negative
            .sort_by(|a, b| a.score_bps.cmp(&b.score_bps).then_with(|| b.evaluated_at_ns.cmp(&a.evaluated_at_ns)));
        summary.top_positive.truncate(3);
        summary.top_negative.truncate(3);
        Some(summary)
    }

    pub fn apply_feedback(&self, event: &FeedbackEvent) -> u64 {
        let Some(intent_id) = event.intent_id.as_deref() else {
            return 0;
        };
        let run_ids = {
            let index = self
                .intent_index
                .read()
                .expect("strategy shadow intent index read lock poisoned");
            index.get(intent_id).cloned().unwrap_or_default()
        };
        if run_ids.is_empty() {
            return 0;
        }

        let outcome = ShadowOutcomeView::from_feedback_event(event);
        let is_final = ShadowOutcomeView::is_final_event(event);
        let mut updated = 0u64;
        let mut guard = self
            .records
            .write()
            .expect("strategy shadow store write lock poisoned");
        for run_id in run_ids {
            if let Some(record) = guard.get_mut(&(run_id, intent_id.to_string())) {
                if let Some(actual_policy) = event.actual_policy.as_ref() {
                    record.actual_policy = Some(actual_policy.clone());
                }
                record.actual_outcome = Some(outcome.clone());
                record.evaluated_at_ns = event.event_at_ns;
                record.recompute_score_components();
                if is_final {
                    record.comparison_status = ShadowComparisonStatus::Matched;
                }
                updated = updated.saturating_add(1);
            }
        }
        self.feedback_apply_total
            .fetch_add(updated, Ordering::Relaxed);
        updated
    }
}

impl StrategyShadowRunSummary {
    fn count_record(&mut self, record: &ShadowRecord, score: i64) {
        match record.comparison_status {
            ShadowComparisonStatus::Pending => self.pending_count += 1,
            ShadowComparisonStatus::Matched => self.matched_count += 1,
            ShadowComparisonStatus::Skipped => self.skipped_count += 1,
            ShadowComparisonStatus::TimedOut => self.timed_out_count += 1,
        }
        match score.cmp(&0) {
            CmpOrdering::Less => self.negative_score_count += 1,
            CmpOrdering::Equal => self.zero_score_count += 1,
            CmpOrdering::Greater => self.positive_score_count += 1,
        }
    }

    fn push_sample(&mut self, record: &ShadowRecord, score: i64) {
        let sample = StrategyShadowScoreSample {
            intent_id: record.intent_id.clone(),
            model_id: record.model_id.clone(),
            score_bps: score,
            comparison_status: record.comparison_status,
            evaluated_at_ns: record.evaluated_at_ns,
        };
        match score.cmp(&0) {
            CmpOrdering::Less => self.top_negative.push(sample),
            CmpOrdering::Greater => self.top_positive.push(sample),
            CmpOrdering::Equal => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{StrategyShadowMetrics, StrategyShadowStore};
    use crate::strategy::config::ExecutionPolicyConfig;
    use crate::strategy::feedback::FeedbackEvent;
    use crate::strategy::shadow::{
        SHADOW_RECORD_SCHEMA_VERSION, ShadowComparisonStatus, ShadowOutcomeView, ShadowPolicyView,
        ShadowRecord,
    };
    use crate::strategy::intent::ExecutionPolicyKind;

    fn shadow_fixture() -> ShadowRecord {
        ShadowRecord {
            schema_version: SHADOW_RECORD_SCHEMA_VERSION,
            shadow_run_id: "shadow-1".to_string(),
            model_id: "model-1".to_string(),
            intent_id: "intent-1".to_string(),
            session_id: "sess-1".to_string(),
            session_seq: 1,
            predicted_policy: None,
            actual_policy: None,
            predicted_outcome: Some(ShadowOutcomeView {
                final_status: Some("VOLATILE_ACCEPT".to_string()),
                reject_reason: None,
                accepted_at_ns: Some(100),
                durable_at_ns: None,
            }),
            actual_outcome: None,
            score_components: Vec::new(),
            evaluated_at_ns: 100,
            comparison_status: ShadowComparisonStatus::Pending,
        }
    }

    #[test]
    fn shadow_store_upserts_and_reads_back() {
        let store = StrategyShadowStore::new();
        assert_eq!(store.size(), 0);

        let previous = store.upsert(shadow_fixture()).expect("upsert shadow");
        assert!(previous.is_none());
        assert_eq!(store.size(), 1);
        assert_eq!(store.upsert_total(), 1);

        let fetched = store
            .get("shadow-1", "intent-1")
            .expect("shadow record exists");
        assert_eq!(fetched.session_id, "sess-1");
        assert_eq!(fetched.session_seq, 1);
        assert_eq!(store.run_ids_for_intent("intent-1"), vec!["shadow-1".to_string()]);
    }

    #[test]
    fn shadow_store_replaces_existing_record() {
        let store = StrategyShadowStore::new();
        store.upsert(shadow_fixture()).expect("initial upsert");

        let mut next = shadow_fixture();
        next.session_seq = 9;
        let previous = store.upsert(next).expect("replace shadow");

        assert_eq!(previous.expect("previous").session_seq, 1);
        assert_eq!(store.upsert_total(), 2);
        assert_eq!(
            store
                .get("shadow-1", "intent-1")
                .expect("current")
                .session_seq,
            9
        );
    }

    #[test]
    fn shadow_store_applies_feedback_by_intent_id() {
        let store = StrategyShadowStore::new();
        store.upsert(shadow_fixture()).expect("initial upsert");

        let updated = store.apply_feedback(
            &FeedbackEvent::accepted("sess-1", 1, "acc-1", "AAPL", 100)
                .with_intent_id("intent-1")
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
                .durable_accepted(180),
        );

        assert_eq!(updated, 1);
        assert_eq!(store.feedback_apply_total(), 1);
        let record = store.get("shadow-1", "intent-1").expect("updated shadow");
        assert_eq!(
            record
                .actual_outcome
                .as_ref()
                .and_then(|outcome| outcome.final_status.as_deref()),
            Some("DURABLE_ACCEPTED")
        );
        assert_eq!(
            record
                .actual_policy
                .as_ref()
                .and_then(|policy| policy.venue.as_deref()),
            Some("NASDAQ")
        );
        assert!(!record.score_components.is_empty());
        assert_eq!(record.total_score_bps(), 40);
        assert_eq!(record.comparison_status, ShadowComparisonStatus::Matched);
    }

    #[test]
    fn shadow_store_metrics_reports_status_and_score_buckets() {
        let store = StrategyShadowStore::new();

        let mut negative = shadow_fixture();
        negative.shadow_run_id = "shadow-2".to_string();
        negative.comparison_status = ShadowComparisonStatus::Matched;
        negative.score_components = vec![crate::strategy::shadow::ShadowScoreComponent {
            name: "policyMismatch".to_string(),
            score_bps: -10,
            detail: None,
        }];
        negative.evaluated_at_ns = 222;

        let mut zero = shadow_fixture();
        zero.shadow_run_id = "shadow-3".to_string();
        zero.comparison_status = ShadowComparisonStatus::Skipped;
        zero.score_components.clear();
        zero.evaluated_at_ns = 333;

        store.upsert(shadow_fixture()).expect("upsert pending");
        store.upsert(negative).expect("upsert negative");
        store.upsert(zero).expect("upsert zero");

        let metrics = store.metrics();

        assert_eq!(
            metrics,
            StrategyShadowMetrics {
                record_count: 3,
                upsert_total: 3,
                feedback_apply_total: 0,
                pending_count: 1,
                matched_count: 1,
                skipped_count: 1,
                timed_out_count: 0,
                negative_score_count: 1,
                zero_score_count: 2,
                positive_score_count: 0,
                last_evaluated_at_ns: 333,
            }
        );
    }

    #[test]
    fn shadow_store_summarizes_single_run() {
        let store = StrategyShadowStore::new();

        let mut matched_negative = shadow_fixture();
        matched_negative.shadow_run_id = "run-1".to_string();
        matched_negative.comparison_status = ShadowComparisonStatus::Matched;
        matched_negative.score_components = vec![crate::strategy::shadow::ShadowScoreComponent {
            name: "policyMismatch".to_string(),
            score_bps: -10,
            detail: None,
        }];
        matched_negative.evaluated_at_ns = 200;

        let mut pending_zero = shadow_fixture();
        pending_zero.shadow_run_id = "run-1".to_string();
        pending_zero.intent_id = "intent-2".to_string();
        pending_zero.comparison_status = ShadowComparisonStatus::Pending;
        pending_zero.score_components.clear();
        pending_zero.evaluated_at_ns = 300;

        let mut skipped_positive = shadow_fixture();
        skipped_positive.shadow_run_id = "run-1".to_string();
        skipped_positive.intent_id = "intent-3".to_string();
        skipped_positive.comparison_status = ShadowComparisonStatus::Skipped;
        skipped_positive.score_components = vec![crate::strategy::shadow::ShadowScoreComponent {
            name: "outcomeClassMatch".to_string(),
            score_bps: 50,
            detail: None,
        }];
        skipped_positive.evaluated_at_ns = 400;

        let mut other_run = shadow_fixture();
        other_run.shadow_run_id = "run-2".to_string();

        store.upsert(matched_negative).expect("upsert run-1 matched");
        store.upsert(pending_zero).expect("upsert run-1 pending");
        store.upsert(skipped_positive).expect("upsert run-1 skipped");
        store.upsert(other_run).expect("upsert run-2");

        let summary = store.summary_for_run("run-1").expect("summary");

        assert_eq!(summary.shadow_run_id, "run-1");
        assert_eq!(summary.record_count, 3);
        assert_eq!(summary.pending_count, 1);
        assert_eq!(summary.matched_count, 1);
        assert_eq!(summary.skipped_count, 1);
        assert_eq!(summary.timed_out_count, 0);
        assert_eq!(summary.negative_score_count, 1);
        assert_eq!(summary.zero_score_count, 1);
        assert_eq!(summary.positive_score_count, 1);
        assert_eq!(summary.total_score_bps, 40);
        assert_eq!(summary.average_score_bps, 40.0 / 3.0);
        assert_eq!(summary.min_score_bps, -10);
        assert_eq!(summary.max_score_bps, 50);
        assert_eq!(summary.last_evaluated_at_ns, 400);
        assert_eq!(summary.top_positive.len(), 1);
        assert_eq!(summary.top_positive[0].intent_id, "intent-3");
        assert_eq!(summary.top_negative.len(), 1);
        assert_eq!(summary.top_negative[0].intent_id, "intent-1");
    }
}
