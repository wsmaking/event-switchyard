//! ShardedOrderStore - HFT最適化版注文ストア
//!
//! account_id % SHARD_COUNT でシャーディングし、ロック競合を軽減。
//! 64シャードで並列処理時のスループットを最大化。

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{ExecReport, OrderSnapshot, OrderStatus};

/// シャード数（2のべき乗で効率的なmodulo演算）
const SHARD_COUNT: usize = 64;

/// シャード単位のストア
struct Shard {
    /// order_id -> OrderSnapshot
    by_id: HashMap<String, OrderSnapshot>,
    /// idempotency_key -> order_id
    idempotency_index: HashMap<String, IdempotencyEntry>,
}

impl Shard {
    fn new() -> Self {
        Self {
            by_id: HashMap::new(),
            idempotency_index: HashMap::new(),
        }
    }
}

/// シャード化された注文ストア
///
/// account_id に基づいてシャードを選択し、ロック競合を軽減。
/// 同一アカウントの注文は同一シャードに配置されるため、
/// アカウント単位の操作は一貫性を保つ。
pub struct ShardedOrderStore {
    shards: Box<[RwLock<Shard>; SHARD_COUNT]>,
    idempotency_ttl_ms: u64,
    idempotency_expired_total: AtomicU64,
}

pub enum IdempotencyOutcome {
    Existing(OrderSnapshot),
    Created(OrderSnapshot),
    NotCreated,
}

#[derive(Clone)]
struct IdempotencyEntry {
    order_id: String,
    created_at_ms: u64,
}

impl ShardedOrderStore {
    pub fn new() -> Self {
        Self::new_with_ttl_ms(default_idempotency_ttl_ms())
    }

    pub fn new_with_ttl_ms(idempotency_ttl_ms: u64) -> Self {
        // 配列を直接初期化
        let shards: [RwLock<Shard>; SHARD_COUNT] = std::array::from_fn(|_| RwLock::new(Shard::new()));
        Self {
            shards: Box::new(shards),
            idempotency_ttl_ms,
            idempotency_expired_total: AtomicU64::new(0),
        }
    }

    /// account_id からシャードインデックスを計算
    #[inline]
    fn shard_index(account_id: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        account_id.hash(&mut hasher);
        (hasher.finish() as usize) & (SHARD_COUNT - 1)
    }

    /// 注文IDで検索
    ///
    /// 注意: order_id から直接シャードを特定できないため、
    /// 別途 OrderIdMap と組み合わせて account_id を取得する必要がある。
    /// この実装では全シャードを検索（非推奨パス）。
    pub fn find_by_id(&self, order_id: &str) -> Option<OrderSnapshot> {
        // 全シャードを検索（フォールバック用）
        for shard in self.shards.iter() {
            let guard = shard.read().unwrap();
            if let Some(order) = guard.by_id.get(order_id) {
                return Some(order.clone());
            }
        }
        None
    }

    /// account_id を指定して注文IDで検索（推奨）
    pub fn find_by_id_with_account(&self, order_id: &str, account_id: &str) -> Option<OrderSnapshot> {
        let idx = Self::shard_index(account_id);
        let guard = self.shards[idx].read().unwrap();
        guard.by_id.get(order_id).cloned()
    }

    /// idempotency_key で検索
    #[allow(dead_code)]
    /// 個別参照API向けのヘルパー
    pub fn find_by_idempotency_key(&self, account_id: &str, key: &str) -> Option<OrderSnapshot> {
        let idx = Self::shard_index(account_id);
        let idx_key = Self::idempotency_key(account_id, key);

        let now_ms = now_millis();
        let mut guard = self.shards[idx].write().unwrap();
        let entry = guard.idempotency_index.get(&idx_key)?.clone();
        if self.is_expired(entry.created_at_ms, now_ms) {
            guard.idempotency_index.remove(&idx_key);
            self.idempotency_expired_total.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        guard.by_id.get(&entry.order_id).cloned()
    }

    pub fn get_or_create_idempotency<F>(
        &self,
        account_id: &str,
        key: &str,
        create: F
    ) -> IdempotencyOutcome
    where
        F: FnOnce() -> Option<OrderSnapshot>,
    {
        let idx = Self::shard_index(account_id);
        let idx_key = Self::idempotency_key(account_id, key);
        let mut guard = self.shards[idx].write().unwrap();
        let now_ms = now_millis();

        if let Some(entry) = guard.idempotency_index.get(&idx_key).cloned() {
            if !self.is_expired(entry.created_at_ms, now_ms) {
                if let Some(order) = guard.by_id.get(&entry.order_id) {
                    return IdempotencyOutcome::Existing(order.clone());
                }
            } else {
                guard.idempotency_index.remove(&idx_key);
                self.idempotency_expired_total.fetch_add(1, Ordering::Relaxed);
            }
        }

        let order = match create() {
            Some(order) => order,
            None => return IdempotencyOutcome::NotCreated,
        };
        let order_id = order.order_id.clone();
        guard.by_id.insert(order_id.clone(), order.clone());
        guard.idempotency_index.insert(
            idx_key,
            IdempotencyEntry {
                order_id,
                created_at_ms: now_ms,
            },
        );
        IdempotencyOutcome::Created(order)
    }

    /// 注文を保存
    pub fn put(&self, order: OrderSnapshot, idempotency_key: Option<&str>) {
        let idx = Self::shard_index(&order.account_id);
        let order_id = order.order_id.clone();
        let account_id = order.account_id.clone();
        let accepted_at = order.accepted_at;

        let mut guard = self.shards[idx].write().unwrap();
        guard.by_id.insert(order_id.clone(), order);

        if let Some(key) = idempotency_key {
            let idx_key = Self::idempotency_key(&account_id, key);
            guard
                .idempotency_index
                .entry(idx_key)
                .or_insert(IdempotencyEntry {
                    order_id,
                    created_at_ms: accepted_at,
                });
        }
    }

    /// 注文を削除
    #[allow(dead_code)]
    /// 回収処理を実装するまでの保持
    pub fn remove(&self, order_id: &str, account_id: &str, idempotency_key: Option<&str>) {
        let idx = Self::shard_index(account_id);
        let mut guard = self.shards[idx].write().unwrap();

        guard.by_id.remove(order_id);

        if let Some(key) = idempotency_key {
            let idx_key = Self::idempotency_key(account_id, key);
            guard.idempotency_index.remove(&idx_key);
        }
    }

    /// 注文を更新
    pub fn update<F>(&self, order_id: &str, account_id: &str, f: F) -> Option<OrderSnapshot>
    where
        F: FnOnce(&OrderSnapshot) -> OrderSnapshot,
    {
        let idx = Self::shard_index(account_id);
        let mut guard = self.shards[idx].write().unwrap();

        if let Some(order) = guard.by_id.get(order_id) {
            let updated = f(order);
            guard.by_id.insert(order_id.to_string(), updated.clone());
            Some(updated)
        } else {
            None
        }
    }

    /// Execution Report を適用
    pub fn apply_execution_report(&self, report: &ExecReport, account_id: &str) -> Option<OrderSnapshot> {
        self.update(&report.order_id, account_id, |prev| {
            let next_filled = prev.filled_qty.max(report.filled_qty_total);

            if prev.status.is_terminal() {
                let mut updated = prev.clone();
                updated.last_update_at = report.at;
                return updated;
            }

            let next_status = match report.status {
                OrderStatus::PartiallyFilled => {
                    if next_filled >= prev.qty {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    }
                }
                OrderStatus::Filled => OrderStatus::Filled,
                OrderStatus::Canceled => OrderStatus::Canceled,
                OrderStatus::Rejected => {
                    if next_filled > 0 {
                        prev.status
                    } else {
                        OrderStatus::Rejected
                    }
                }
                other => other,
            };

            let mut updated = prev.clone();
            updated.status = next_status;
            updated.filled_qty = next_filled;
            updated.last_update_at = report.at;
            updated
        })
    }

    fn idempotency_key(account_id: &str, key: &str) -> String {
        format!("{}::{}", account_id, key)
    }

    fn is_expired(&self, created_at_ms: u64, now_ms: u64) -> bool {
        if self.idempotency_ttl_ms == 0 {
            return false;
        }
        now_ms.saturating_sub(created_at_ms) > self.idempotency_ttl_ms
    }

    pub fn idempotency_expired_total(&self) -> u64 {
        self.idempotency_expired_total.load(Ordering::Relaxed)
    }

    /// 全注文数を取得
    pub fn count(&self) -> usize {
        self.shards.iter().map(|s| s.read().unwrap().by_id.len()).sum()
    }

    /// シャードごとの注文数を取得（デバッグ用）
    pub fn shard_counts(&self) -> Vec<usize> {
        self.shards.iter().map(|s| s.read().unwrap().by_id.len()).collect()
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn default_idempotency_ttl_ms() -> u64 {
    24 * 60 * 60 * 1000
}

impl Default for ShardedOrderStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::{OrderType, TimeInForce};
    use std::sync::{Arc, Barrier, Mutex};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_sharding_distribution() {
        let store = ShardedOrderStore::new();

        // 100件の注文を異なるアカウントから追加
        for i in 0..100 {
            let order = OrderSnapshot::new(
                format!("ord_{}", i),
                format!("acc_{}", i),
                "AAPL".into(),
                "BUY".into(),
                OrderType::Limit,
                100,
                Some(15000),
                TimeInForce::Gtc,
                None,
                None,
            );
            store.put(order, None);
        }

        // 全件数確認
        assert_eq!(store.count(), 100);

        // シャード分散を確認（完全に偏っていないこと）
        let counts = store.shard_counts();
        let non_empty = counts.iter().filter(|&&c| c > 0).count();
        assert!(non_empty > 10, "シャードが偏りすぎ: {} non-empty shards", non_empty);
    }

    #[test]
    fn test_same_account_same_shard() {
        let store = ShardedOrderStore::new();

        // 同一アカウントの注文は同一シャードに配置される
        for i in 0..10 {
            let order = OrderSnapshot::new(
                format!("ord_{}", i),
                "acc_1".into(),  // 同一アカウント
                "AAPL".into(),
                "BUY".into(),
                OrderType::Limit,
                100,
                Some(15000),
                TimeInForce::Gtc,
                None,
                None,
            );
            store.put(order, None);
        }

        // シャード分散を確認（1シャードに集中）
        let counts = store.shard_counts();
        let non_empty = counts.iter().filter(|&&c| c > 0).count();
        assert_eq!(non_empty, 1, "同一アカウントは1シャードに集中すべき");
    }

    #[test]
    fn test_find_with_account() {
        let store = ShardedOrderStore::new();

        let order = OrderSnapshot::new(
            "ord_1".into(),
            "acc_1".into(),
            "AAPL".into(),
            "BUY".into(),
            OrderType::Limit,
            100,
            Some(15000),
            TimeInForce::Gtc,
            None,
            None,
        );
        store.put(order.clone(), None);

        // account_id を指定した検索（高速）
        let found = store.find_by_id_with_account("ord_1", "acc_1").unwrap();
        assert_eq!(found.order_id, "ord_1");

        // 誤ったaccount_idでは見つからない
        assert!(store.find_by_id_with_account("ord_1", "acc_2").is_none());

        // order_id のみでも検索可能（低速フォールバック）
        let found2 = store.find_by_id("ord_1").unwrap();
        assert_eq!(found2.order_id, "ord_1");
    }

    #[test]
    fn test_apply_execution_report() {
        let store = ShardedOrderStore::new();

        let order = OrderSnapshot::new(
            "ord_1".into(),
            "acc_1".into(),
            "AAPL".into(),
            "BUY".into(),
            OrderType::Limit,
            100,
            Some(15000),
            TimeInForce::Gtc,
            None,
            None,
        );
        store.put(order, None);

        let report = ExecReport {
            order_id: "ord_1".into(),
            status: OrderStatus::PartiallyFilled,
            filled_qty_delta: 50,
            filled_qty_total: 50,
            at: 1234567890,
        };

        let updated = store.apply_execution_report(&report, "acc_1").unwrap();
        assert_eq!(updated.status, OrderStatus::PartiallyFilled);
        assert_eq!(updated.filled_qty, 50);
    }

    #[test]
    fn test_idempotency_concurrent_single_create() {
        let store = Arc::new(ShardedOrderStore::new());
        let created = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let results = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            let created = Arc::clone(&created);
            let barrier = Arc::clone(&barrier);
            let results = Arc::clone(&results);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let outcome = store.get_or_create_idempotency("acc_1", "idem-key", || {
                    created.fetch_add(1, Ordering::SeqCst);
                    Some(OrderSnapshot::new(
                        format!("ord_{}", created.load(Ordering::SeqCst)),
                        "acc_1".into(),
                        "AAPL".into(),
                        "BUY".into(),
                        OrderType::Limit,
                        100,
                        Some(15000),
                        TimeInForce::Gtc,
                        None,
                        None,
                    ))
                });
                let order_id = match outcome {
                    IdempotencyOutcome::Existing(order) => order.order_id,
                    IdempotencyOutcome::Created(order) => order.order_id,
                    IdempotencyOutcome::NotCreated => "none".to_string(),
                };
                results.lock().unwrap().push(order_id);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(created.load(Ordering::SeqCst), 1);
        let results = results.lock().unwrap();
        let first = results.first().cloned().unwrap();
        assert!(results.iter().all(|id| id == &first));
        assert_ne!(first, "none");
    }

    #[test]
    fn test_idempotency_ttl_expiry() {
        let store = ShardedOrderStore::new_with_ttl_ms(1);
        let created = AtomicUsize::new(0);

        let first = store.get_or_create_idempotency("acc_1", "idem-ttl", || {
            created.fetch_add(1, Ordering::SeqCst);
            Some(OrderSnapshot::new(
                format!("ord_{}", created.load(Ordering::SeqCst)),
                "acc_1".into(),
                "AAPL".into(),
                "BUY".into(),
                OrderType::Limit,
                100,
                Some(15000),
                TimeInForce::Gtc,
                None,
                None,
            ))
        });
        match first {
            IdempotencyOutcome::Existing(_) | IdempotencyOutcome::NotCreated => panic!("expected create"),
            IdempotencyOutcome::Created(_) => {}
        }

        thread::sleep(Duration::from_millis(2));

        let second = store.get_or_create_idempotency("acc_1", "idem-ttl", || {
            created.fetch_add(1, Ordering::SeqCst);
            Some(OrderSnapshot::new(
                format!("ord_{}", created.load(Ordering::SeqCst)),
                "acc_1".into(),
                "AAPL".into(),
                "BUY".into(),
                OrderType::Limit,
                100,
                Some(15000),
                TimeInForce::Gtc,
                None,
                None,
            ))
        });
        match second {
            IdempotencyOutcome::Created(_) => {}
            _ => panic!("expected create after ttl expiry"),
        }

        assert_eq!(created.load(Ordering::SeqCst), 2);
        assert_eq!(store.idempotency_expired_total(), 1);
    }
}
