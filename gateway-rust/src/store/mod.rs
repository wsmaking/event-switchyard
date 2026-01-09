//! 注文ストア
//!
//! 注文状態をメモリ内で管理し、Execution Reportを適用する。

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::order::{OrderType, TimeInForce};

/// 注文ステータス
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Accepted,
    Sent,
    CancelRequested,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

impl OrderStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "ACCEPTED",
            Self::Sent => "SENT",
            Self::CancelRequested => "CANCEL_REQUESTED",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
            Self::Rejected => "REJECTED",
        }
    }
}

/// 注文スナップショット
#[derive(Debug, Clone)]
pub struct OrderSnapshot {
    pub order_id: String,
    pub account_id: String,
    pub client_order_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub order_type: OrderType,
    pub qty: u64,
    pub price: Option<u64>,
    pub time_in_force: TimeInForce,
    pub expire_at: Option<u64>,
    pub status: OrderStatus,
    pub accepted_at: u64,
    pub last_update_at: u64,
    pub filled_qty: u64,
}

impl OrderSnapshot {
    pub fn new(
        order_id: String,
        account_id: String,
        symbol: String,
        side: String,
        order_type: OrderType,
        qty: u64,
        price: Option<u64>,
        time_in_force: TimeInForce,
        expire_at: Option<u64>,
        client_order_id: Option<String>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            order_id,
            account_id,
            client_order_id,
            symbol,
            side,
            order_type,
            qty,
            price,
            time_in_force,
            expire_at,
            status: OrderStatus::Accepted,
            accepted_at: now,
            last_update_at: now,
            filled_qty: 0,
        }
    }
}

/// Execution Report（取引所からの約定通知）
#[derive(Debug, Clone)]
pub struct ExecReport {
    pub order_id: String,
    pub status: OrderStatus,
    pub filled_qty_delta: u64,
    pub filled_qty_total: u64,
    pub at: u64,
}

/// インメモリ注文ストア
pub struct OrderStore {
    by_id: RwLock<HashMap<String, OrderSnapshot>>,
    idempotency_index: RwLock<HashMap<String, String>>,
}

impl OrderStore {
    pub fn new() -> Self {
        Self {
            by_id: RwLock::new(HashMap::new()),
            idempotency_index: RwLock::new(HashMap::new()),
        }
    }

    pub fn find_by_id(&self, order_id: &str) -> Option<OrderSnapshot> {
        self.by_id.read().unwrap().get(order_id).cloned()
    }

    pub fn find_by_idempotency_key(&self, account_id: &str, key: &str) -> Option<OrderSnapshot> {
        let idx_key = Self::idempotency_key(account_id, key);
        let order_id = self.idempotency_index.read().unwrap().get(&idx_key).cloned()?;
        self.find_by_id(&order_id)
    }

    pub fn put(&self, order: OrderSnapshot, idempotency_key: Option<&str>) {
        let order_id = order.order_id.clone();
        let account_id = order.account_id.clone();

        self.by_id.write().unwrap().insert(order_id.clone(), order);

        if let Some(key) = idempotency_key {
            let idx_key = Self::idempotency_key(&account_id, key);
            self.idempotency_index
                .write()
                .unwrap()
                .entry(idx_key)
                .or_insert(order_id);
        }
    }

    pub fn remove(&self, order_id: &str, idempotency_key: Option<&str>) {
        let order = self.by_id.write().unwrap().remove(order_id);

        if let (Some(key), Some(order)) = (idempotency_key, order) {
            let idx_key = Self::idempotency_key(&order.account_id, key);
            self.idempotency_index.write().unwrap().remove(&idx_key);
        }
    }

    pub fn update<F>(&self, order_id: &str, f: F) -> Option<OrderSnapshot>
    where
        F: FnOnce(&OrderSnapshot) -> OrderSnapshot,
    {
        let mut map = self.by_id.write().unwrap();
        if let Some(order) = map.get(order_id) {
            let updated = f(order);
            map.insert(order_id.to_string(), updated.clone());
            Some(updated)
        } else {
            None
        }
    }

    pub fn apply_execution_report(&self, report: &ExecReport) -> Option<OrderSnapshot> {
        self.update(&report.order_id, |prev| {
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

    pub fn count(&self) -> usize {
        self.by_id.read().unwrap().len()
    }
}

impl Default for OrderStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_find() {
        let store = OrderStore::new();
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
        let found = store.find_by_id("ord_1").unwrap();
        assert_eq!(found.order_id, "ord_1");
        assert_eq!(found.status, OrderStatus::Accepted);
    }

    #[test]
    fn test_idempotency() {
        let store = OrderStore::new();
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

        store.put(order, Some("idem_key_1"));

        let found = store.find_by_idempotency_key("acc_1", "idem_key_1").unwrap();
        assert_eq!(found.order_id, "ord_1");

        assert!(store.find_by_idempotency_key("acc_1", "other_key").is_none());
    }

    #[test]
    fn test_apply_execution_report() {
        let store = OrderStore::new();
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

        let updated = store.apply_execution_report(&report).unwrap();
        assert_eq!(updated.status, OrderStatus::PartiallyFilled);
        assert_eq!(updated.filled_qty, 50);

        let report2 = ExecReport {
            order_id: "ord_1".into(),
            status: OrderStatus::Filled,
            filled_qty_delta: 50,
            filled_qty_total: 100,
            at: 1234567900,
        };

        let updated2 = store.apply_execution_report(&report2).unwrap();
        assert_eq!(updated2.status, OrderStatus::Filled);
        assert_eq!(updated2.filled_qty, 100);
    }
}
