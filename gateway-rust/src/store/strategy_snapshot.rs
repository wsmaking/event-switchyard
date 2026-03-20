use crate::strategy::config::ExecutionConfigSnapshot;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

pub struct StrategySnapshotStore {
    current: RwLock<Arc<ExecutionConfigSnapshot>>,
    applied_total: AtomicU64,
    last_applied_at_ns: AtomicU64,
}

impl Default for StrategySnapshotStore {
    fn default() -> Self {
        Self::new(ExecutionConfigSnapshot::default())
    }
}

impl StrategySnapshotStore {
    pub fn new(initial: ExecutionConfigSnapshot) -> Self {
        let applied_at_ns = initial.applied_at_ns;
        Self {
            current: RwLock::new(Arc::new(initial)),
            applied_total: AtomicU64::new(0),
            last_applied_at_ns: AtomicU64::new(applied_at_ns),
        }
    }

    pub fn snapshot(&self) -> Arc<ExecutionConfigSnapshot> {
        self.current
            .read()
            .expect("strategy snapshot read lock poisoned")
            .clone()
    }

    pub fn replace(
        &self,
        next: ExecutionConfigSnapshot,
    ) -> Result<Arc<ExecutionConfigSnapshot>, &'static str> {
        next.validate()?;
        let next_applied_at_ns = next.applied_at_ns;
        let next = Arc::new(next);
        let mut guard = self
            .current
            .write()
            .expect("strategy snapshot write lock poisoned");
        let prev = std::mem::replace(&mut *guard, Arc::clone(&next));
        self.applied_total.fetch_add(1, Ordering::Relaxed);
        self.last_applied_at_ns
            .store(next_applied_at_ns, Ordering::Relaxed);
        Ok(prev)
    }

    pub fn applied_total(&self) -> u64 {
        self.applied_total.load(Ordering::Relaxed)
    }

    pub fn last_applied_at_ns(&self) -> u64 {
        self.last_applied_at_ns.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::StrategySnapshotStore;
    use crate::strategy::config::ExecutionConfigSnapshot;

    #[test]
    fn snapshot_store_replaces_snapshot_and_tracks_version() {
        let store = StrategySnapshotStore::default();
        let initial = store.snapshot();
        assert_eq!(initial.version, 0);

        let prev = store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: "snapshot-1".to_string(),
                version: 1,
                applied_at_ns: 123,
                shadow_enabled: true,
                ..ExecutionConfigSnapshot::default()
            })
            .expect("replace snapshot");

        let current = store.snapshot();
        assert_eq!(prev.version, 0);
        assert_eq!(current.version, 1);
        assert_eq!(current.snapshot_id, "snapshot-1");
        assert!(current.shadow_enabled);
        assert_eq!(store.applied_total(), 1);
        assert_eq!(store.last_applied_at_ns(), 123);
    }

    #[test]
    fn snapshot_store_rejects_invalid_snapshot() {
        let store = StrategySnapshotStore::default();
        let err = store
            .replace(ExecutionConfigSnapshot {
                snapshot_id: String::new(),
                ..ExecutionConfigSnapshot::default()
            })
            .expect_err("invalid snapshot should fail");

        assert_eq!(err, "SNAPSHOT_ID_REQUIRED");
        assert_eq!(store.applied_total(), 0);
    }
}
