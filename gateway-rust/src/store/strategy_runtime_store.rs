use crate::strategy::feedback::FeedbackEvent;
use crate::strategy::runtime::{AlgoChildStatus, AlgoParentExecution, AlgoParentStatus};
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StrategyRuntimeMetrics {
    pub parent_count: u64,
    pub active_parent_count: u64,
    pub completed_parent_count: u64,
    pub rejected_parent_count: u64,
    pub paused_parent_count: u64,
    pub created_total: u64,
    pub child_scheduled_count: u64,
    pub child_dispatching_count: u64,
    pub child_accepted_count: u64,
    pub child_durable_accepted_count: u64,
    pub child_rejected_count: u64,
    pub child_skipped_count: u64,
    pub last_updated_at_ns: u64,
}

#[derive(Debug, Clone)]
struct ChildIndexEntry {
    parent_intent_id: String,
    child_index: usize,
}

pub struct StrategyRuntimeStore {
    parents: RwLock<HashMap<String, AlgoParentExecution>>,
    child_index: RwLock<HashMap<String, ChildIndexEntry>>,
    created_total: AtomicU64,
}

impl Default for StrategyRuntimeStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StrategyRuntimeStore {
    pub fn new() -> Self {
        Self {
            parents: RwLock::new(HashMap::new()),
            child_index: RwLock::new(HashMap::new()),
            created_total: AtomicU64::new(0),
        }
    }

    pub fn insert(&self, execution: AlgoParentExecution) -> Result<(), &'static str> {
        if execution.parent_intent_id.trim().is_empty() {
            return Err("STRATEGY_RUNTIME_PARENT_INTENT_ID_REQUIRED");
        }
        let parent_intent_id = execution.parent_intent_id.clone();
        let child_links = execution
            .slices
            .iter()
            .enumerate()
            .map(|(child_index, child)| {
                (
                    child.child_intent_id.clone(),
                    ChildIndexEntry {
                        parent_intent_id: parent_intent_id.clone(),
                        child_index,
                    },
                )
            })
            .collect::<Vec<_>>();

        let mut parents = self
            .parents
            .write()
            .expect("strategy runtime store write lock poisoned");
        if parents.contains_key(&parent_intent_id) {
            return Err("STRATEGY_RUNTIME_PARENT_ALREADY_EXISTS");
        }
        parents.insert(parent_intent_id, execution);
        drop(parents);

        let mut child_index = self
            .child_index
            .write()
            .expect("strategy runtime child index write lock poisoned");
        for (child_intent_id, entry) in child_links {
            child_index.insert(child_intent_id, entry);
        }
        self.created_total.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn get(&self, parent_intent_id: &str) -> Option<AlgoParentExecution> {
        self.parents
            .read()
            .expect("strategy runtime store read lock poisoned")
            .get(parent_intent_id)
            .cloned()
    }

    pub fn get_by_child(&self, child_intent_id: &str) -> Option<AlgoParentExecution> {
        let entry = self.child_entry(child_intent_id)?;
        self.get(&entry.parent_intent_id)
    }

    pub fn list(&self) -> Vec<AlgoParentExecution> {
        self.parents
            .read()
            .expect("strategy runtime store read lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn metrics(&self) -> StrategyRuntimeMetrics {
        let parents = self
            .parents
            .read()
            .expect("strategy runtime store read lock poisoned");
        let mut metrics = StrategyRuntimeMetrics {
            created_total: self.created_total.load(Ordering::Relaxed),
            ..StrategyRuntimeMetrics::default()
        };
        for execution in parents.values() {
            metrics.parent_count += 1;
            metrics.last_updated_at_ns =
                metrics.last_updated_at_ns.max(execution.last_updated_at_ns);
            match execution.status {
                AlgoParentStatus::Scheduled | AlgoParentStatus::Running => {
                    metrics.active_parent_count += 1;
                }
                AlgoParentStatus::Paused => metrics.paused_parent_count += 1,
                AlgoParentStatus::Completed => metrics.completed_parent_count += 1,
                AlgoParentStatus::Rejected => metrics.rejected_parent_count += 1,
            }
            for child in &execution.slices {
                match child.status {
                    AlgoChildStatus::Scheduled => metrics.child_scheduled_count += 1,
                    AlgoChildStatus::Dispatching => metrics.child_dispatching_count += 1,
                    AlgoChildStatus::VolatileAccepted => metrics.child_accepted_count += 1,
                    AlgoChildStatus::DurableAccepted => {
                        metrics.child_accepted_count += 1;
                        metrics.child_durable_accepted_count += 1;
                    }
                    AlgoChildStatus::Rejected => metrics.child_rejected_count += 1,
                    AlgoChildStatus::Skipped => metrics.child_skipped_count += 1,
                }
            }
        }
        metrics
    }

    pub fn can_dispatch_child(&self, child_intent_id: &str) -> bool {
        let Some(entry) = self.child_entry(child_intent_id) else {
            return false;
        };
        let parents = self
            .parents
            .read()
            .expect("strategy runtime store read lock poisoned");
        let Some(parent) = parents.get(&entry.parent_intent_id) else {
            return false;
        };
        let Some(child) = parent.slices.get(entry.child_index) else {
            return false;
        };
        !parent.is_terminal() && matches!(child.status, AlgoChildStatus::Scheduled)
    }

    pub fn record_child_dispatch_started(&self, child_intent_id: &str, started_at_ns: u64) -> bool {
        let Some(entry) = self.child_entry(child_intent_id) else {
            return false;
        };
        let mut parents = self
            .parents
            .write()
            .expect("strategy runtime store write lock poisoned");
        let Some(parent) = parents.get_mut(&entry.parent_intent_id) else {
            return false;
        };
        if parent.is_terminal() {
            return false;
        }
        let Some(child) = parent.slices.get_mut(entry.child_index) else {
            return false;
        };
        if !matches!(child.status, AlgoChildStatus::Scheduled) {
            return false;
        }
        child.status = AlgoChildStatus::Dispatching;
        child.received_at_ns = Some(started_at_ns);
        parent.last_updated_at_ns = started_at_ns;
        if matches!(parent.status, AlgoParentStatus::Scheduled) {
            parent.status = AlgoParentStatus::Running;
        }
        true
    }

    pub fn pause_parent_on_restart(
        &self,
        parent_intent_id: &str,
        paused_at_ns: u64,
        reason: &str,
    ) -> bool {
        let mut parents = self
            .parents
            .write()
            .expect("strategy runtime store write lock poisoned");
        let Some(parent) = parents.get_mut(parent_intent_id) else {
            return false;
        };
        if parent.is_terminal() {
            return false;
        }
        parent.status = AlgoParentStatus::Paused;
        parent.last_updated_at_ns = paused_at_ns;
        parent.final_reason = Some(reason.to_string());
        true
    }

    pub fn mark_child_skipped(
        &self,
        child_intent_id: &str,
        now_ns: u64,
        reason: &'static str,
    ) -> bool {
        let Some(entry) = self.child_entry(child_intent_id) else {
            return false;
        };
        let mut parents = self
            .parents
            .write()
            .expect("strategy runtime store write lock poisoned");
        let Some(parent) = parents.get_mut(&entry.parent_intent_id) else {
            return false;
        };
        let Some(child) = parent.slices.get_mut(entry.child_index) else {
            return false;
        };
        if !matches!(child.status, AlgoChildStatus::Scheduled) {
            return false;
        }
        child.status = AlgoChildStatus::Skipped;
        child.received_at_ns = Some(now_ns);
        child.reject_reason = Some(reason.to_string());
        parent.last_updated_at_ns = now_ns;
        true
    }

    pub fn record_child_volatile_accepted(
        &self,
        child_intent_id: &str,
        session_seq: u64,
        accepted_at_ns: u64,
    ) -> Option<FeedbackEvent> {
        let entry = self.child_entry(child_intent_id)?;
        let mut parents = self
            .parents
            .write()
            .expect("strategy runtime store write lock poisoned");
        let parent = parents.get_mut(&entry.parent_intent_id)?;
        if parent.is_terminal() {
            return None;
        }
        let child = parent.slices.get_mut(entry.child_index)?;
        if !matches!(
            child.status,
            AlgoChildStatus::Scheduled | AlgoChildStatus::Dispatching
        ) {
            return None;
        }

        child.status = AlgoChildStatus::VolatileAccepted;
        child.session_seq = Some(session_seq);
        child.received_at_ns = Some(accepted_at_ns);
        child.reject_reason = None;
        parent.last_updated_at_ns = accepted_at_ns;
        if parent.accepted_at_ns.is_none() {
            parent.accepted_at_ns = Some(accepted_at_ns);
            parent.status = AlgoParentStatus::Running;
            return Some(self.parent_accepted_event(parent, accepted_at_ns));
        }
        if matches!(parent.status, AlgoParentStatus::Paused) {
            parent.status = AlgoParentStatus::Running;
        }
        None
    }

    pub fn record_child_rejected(
        &self,
        child_intent_id: &str,
        rejected_at_ns: u64,
        reason: &str,
    ) -> Option<FeedbackEvent> {
        let entry = self.child_entry(child_intent_id)?;
        let mut parents = self
            .parents
            .write()
            .expect("strategy runtime store write lock poisoned");
        let parent = parents.get_mut(&entry.parent_intent_id)?;
        let child = parent.slices.get_mut(entry.child_index)?;
        if matches!(
            child.status,
            AlgoChildStatus::Rejected | AlgoChildStatus::Skipped | AlgoChildStatus::DurableAccepted
        ) {
            return None;
        }

        child.status = AlgoChildStatus::Rejected;
        child.reject_reason = Some(reason.to_string());
        child.received_at_ns.get_or_insert(rejected_at_ns);
        parent.last_updated_at_ns = rejected_at_ns;

        if parent.completed_at_ns.is_none() {
            parent.status = AlgoParentStatus::Rejected;
            parent.completed_at_ns = Some(rejected_at_ns);
            parent.final_reason = Some(reason.to_string());
            return Some(self.parent_rejected_event(parent, rejected_at_ns, reason));
        }
        None
    }

    pub fn record_child_durable_accepted(
        &self,
        child_intent_id: &str,
        durable_at_ns: u64,
    ) -> Option<FeedbackEvent> {
        let entry = self.child_entry(child_intent_id)?;
        let mut parents = self
            .parents
            .write()
            .expect("strategy runtime store write lock poisoned");
        let parent = parents.get_mut(&entry.parent_intent_id)?;
        let child = parent.slices.get_mut(entry.child_index)?;
        if matches!(
            child.status,
            AlgoChildStatus::Rejected | AlgoChildStatus::Skipped | AlgoChildStatus::DurableAccepted
        ) {
            return None;
        }

        child.status = AlgoChildStatus::DurableAccepted;
        child.durable_at_ns = Some(durable_at_ns);
        child.received_at_ns.get_or_insert(durable_at_ns);
        parent.last_updated_at_ns = durable_at_ns;

        let all_children_complete = parent
            .slices
            .iter()
            .all(|slice| matches!(slice.status, AlgoChildStatus::DurableAccepted));
        if all_children_complete && parent.completed_at_ns.is_none() {
            parent.status = AlgoParentStatus::Completed;
            parent.completed_at_ns = Some(durable_at_ns);
            return Some(self.parent_durable_accepted_event(parent, durable_at_ns));
        }
        None
    }

    pub fn record_child_durable_rejected(
        &self,
        child_intent_id: &str,
        rejected_at_ns: u64,
        reason: &str,
    ) -> Option<FeedbackEvent> {
        let entry = self.child_entry(child_intent_id)?;
        let mut parents = self
            .parents
            .write()
            .expect("strategy runtime store write lock poisoned");
        let parent = parents.get_mut(&entry.parent_intent_id)?;
        let child = parent.slices.get_mut(entry.child_index)?;
        if matches!(
            child.status,
            AlgoChildStatus::Rejected | AlgoChildStatus::Skipped | AlgoChildStatus::DurableAccepted
        ) {
            return None;
        }

        child.status = AlgoChildStatus::Rejected;
        child.reject_reason = Some(reason.to_string());
        child.durable_at_ns = Some(rejected_at_ns);
        child.received_at_ns.get_or_insert(rejected_at_ns);
        parent.last_updated_at_ns = rejected_at_ns;

        if parent.completed_at_ns.is_none() {
            parent.status = AlgoParentStatus::Rejected;
            parent.completed_at_ns = Some(rejected_at_ns);
            parent.final_reason = Some(reason.to_string());
            return Some(self.parent_durable_rejected_event(parent, rejected_at_ns, reason));
        }
        None
    }

    fn child_entry(&self, child_intent_id: &str) -> Option<ChildIndexEntry> {
        self.child_index
            .read()
            .expect("strategy runtime child index read lock poisoned")
            .get(child_intent_id)
            .cloned()
    }

    fn parent_accepted_event(
        &self,
        parent: &AlgoParentExecution,
        accepted_at_ns: u64,
    ) -> FeedbackEvent {
        self.decorate_parent_event(
            FeedbackEvent::accepted(
                parent.session_id.as_str(),
                0,
                parent.account_id.as_str(),
                parent.symbol.as_str(),
                accepted_at_ns,
            ),
            parent,
        )
    }

    fn parent_rejected_event(
        &self,
        parent: &AlgoParentExecution,
        rejected_at_ns: u64,
        reason: &str,
    ) -> FeedbackEvent {
        self.decorate_parent_event(
            FeedbackEvent::rejected(
                parent.session_id.as_str(),
                0,
                parent.account_id.as_str(),
                parent.symbol.as_str(),
                rejected_at_ns,
                reason,
            ),
            parent,
        )
    }

    fn parent_durable_accepted_event(
        &self,
        parent: &AlgoParentExecution,
        durable_at_ns: u64,
    ) -> FeedbackEvent {
        let accepted_at_ns = parent.accepted_at_ns.unwrap_or(durable_at_ns);
        self.decorate_parent_event(
            FeedbackEvent::accepted(
                parent.session_id.as_str(),
                0,
                parent.account_id.as_str(),
                parent.symbol.as_str(),
                accepted_at_ns,
            )
            .durable_accepted(durable_at_ns),
            parent,
        )
    }

    fn parent_durable_rejected_event(
        &self,
        parent: &AlgoParentExecution,
        rejected_at_ns: u64,
        reason: &str,
    ) -> FeedbackEvent {
        if let Some(accepted_at_ns) = parent.accepted_at_ns {
            self.decorate_parent_event(
                FeedbackEvent::accepted(
                    parent.session_id.as_str(),
                    0,
                    parent.account_id.as_str(),
                    parent.symbol.as_str(),
                    accepted_at_ns,
                )
                .durable_rejected(rejected_at_ns, reason),
                parent,
            )
        } else {
            self.parent_rejected_event(parent, rejected_at_ns, reason)
        }
    }

    fn decorate_parent_event(
        &self,
        event: FeedbackEvent,
        parent: &AlgoParentExecution,
    ) -> FeedbackEvent {
        let event = event.with_intent_id(parent.parent_intent_id.as_str());
        let event = if let Some(execution_run_id) = parent.execution_run_id.as_deref() {
            event.with_execution_run_id(execution_run_id)
        } else {
            event
        };
        let event = if let Some(model_id) = parent.model_id.as_deref() {
            event.with_model_id(model_id)
        } else {
            event
        };
        let event =
            if let Some(effective_risk_budget_ref) = parent.effective_risk_budget_ref.as_deref() {
                event.with_effective_risk_budget_ref(effective_risk_budget_ref)
            } else {
                event
            };
        if let Some(actual_policy) = parent.actual_policy.as_ref() {
            event.with_actual_policy(actual_policy.clone())
        } else {
            event
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{StrategyRuntimeMetrics, StrategyRuntimeStore};
    use crate::strategy::algo::{
        AlgoExecutionPlan, AlgoExecutionSlice, STRATEGY_ALGO_PLAN_SCHEMA_VERSION,
    };
    use crate::strategy::intent::ExecutionPolicyKind;
    use crate::strategy::runtime::{AlgoParentExecution, AlgoParentStatus};
    use crate::{
        order::{OrderRequest, OrderType, TimeInForce},
        strategy::{intent::StrategyRecoveryPolicy, runtime::AlgoChildStatus},
    };

    fn runtime_fixture() -> AlgoParentExecution {
        AlgoParentExecution::from_plan(
            &AlgoExecutionPlan {
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
            },
            "acc-1".to_string(),
            "sess-1".to_string(),
            "AAPL".to_string(),
            Some("model-1".to_string()),
            Some("run-1".to_string()),
            StrategyRecoveryPolicy::GatewayManagedResume,
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
            5,
        )
    }

    #[test]
    fn runtime_store_emits_parent_events_and_completes() {
        let store = StrategyRuntimeStore::new();
        store.insert(runtime_fixture()).expect("insert runtime");

        assert!(store.record_child_dispatch_started("intent-1::child-01", 90));
        let accepted = store
            .record_child_volatile_accepted("intent-1::child-01", 7, 100)
            .expect("accepted event");
        assert_eq!(accepted.intent_id.as_deref(), Some("intent-1"));

        assert!(
            store
                .record_child_durable_accepted("intent-1::child-01", 150)
                .is_none()
        );
        assert!(
            store
                .record_child_volatile_accepted("intent-1::child-02", 8, 200)
                .is_none()
        );
        let durable = store
            .record_child_durable_accepted("intent-1::child-02", 250)
            .expect("durable event");
        assert_eq!(durable.intent_id.as_deref(), Some("intent-1"));

        let execution = store.get("intent-1").expect("parent execution");
        assert_eq!(execution.status, AlgoParentStatus::Completed);
        assert_eq!(execution.durable_accepted_child_count(), 2);
    }

    #[test]
    fn runtime_store_metrics_track_parent_and_child_counts() {
        let store = StrategyRuntimeStore::new();
        store.insert(runtime_fixture()).expect("insert runtime");
        assert!(store.record_child_dispatch_started("intent-1::child-01", 11));

        let metrics = store.metrics();
        assert_eq!(
            metrics,
            StrategyRuntimeMetrics {
                parent_count: 1,
                active_parent_count: 1,
                completed_parent_count: 0,
                rejected_parent_count: 0,
                paused_parent_count: 0,
                created_total: 1,
                child_scheduled_count: 1,
                child_dispatching_count: 1,
                child_accepted_count: 0,
                child_durable_accepted_count: 0,
                child_rejected_count: 0,
                child_skipped_count: 0,
                last_updated_at_ns: 11,
            }
        );
    }

    #[test]
    fn runtime_store_gets_parent_by_child() {
        let store = StrategyRuntimeStore::new();
        store.insert(runtime_fixture()).expect("insert runtime");

        let runtime = store
            .get_by_child("intent-1::child-02")
            .expect("parent by child");
        assert_eq!(runtime.parent_intent_id, "intent-1");
        assert_eq!(runtime.slices[1].status, AlgoChildStatus::Scheduled);
    }
}
