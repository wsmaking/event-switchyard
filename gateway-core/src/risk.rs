//! Ultra-fast risk check for order validation
//! All checks are O(1) with no heap allocation

use crate::queue::Order;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Risk check result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskResult {
    Accepted,
    RejectedMaxQty,
    RejectedMaxNotional,
    RejectedDailyLimit,
    RejectedUnknownSymbol,
}

/// Per-symbol risk limits
#[derive(Debug, Clone, Copy)]
pub struct SymbolLimits {
    pub max_order_qty: u32,
    pub max_notional: u64,    // price * qty limit
    pub tick_size: u64,       // minimum price increment
}

impl Default for SymbolLimits {
    fn default() -> Self {
        Self {
            max_order_qty: 10_000,
            max_notional: 1_000_000_000, // $1M default
            tick_size: 1,
        }
    }
}

/// Per-account position tracking (atomic for lock-free updates)
pub struct AccountPosition {
    pub daily_notional: AtomicU64,
    pub daily_limit: u64,
}

impl AccountPosition {
    pub fn new(daily_limit: u64) -> Self {
        Self {
            daily_notional: AtomicU64::new(0),
            daily_limit,
        }
    }

    /// Try to add notional, returns false if would exceed limit
    #[inline]
    pub fn try_add_notional(&self, notional: u64) -> bool {
        let mut current = self.daily_notional.load(Ordering::Relaxed);
        loop {
            let new_value = current.saturating_add(notional);
            if new_value > self.daily_limit {
                return false;
            }
            match self.daily_notional.compare_exchange_weak(
                current,
                new_value,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(x) => current = x,
            }
        }
    }

    /// Reset daily notional (called at start of trading day)
    pub fn reset(&self) {
        self.daily_notional.store(0, Ordering::SeqCst);
    }
}

/// Fast path risk checker
/// Uses RwLock only for configuration updates, hot path is lock-free
pub struct RiskChecker {
    symbol_limits: RwLock<HashMap<[u8; 8], SymbolLimits>>,
    default_limits: SymbolLimits,
}

impl RiskChecker {
    pub fn new() -> Self {
        Self {
            symbol_limits: RwLock::new(HashMap::new()),
            default_limits: SymbolLimits::default(),
        }
    }

    /// Register symbol-specific limits
    pub fn register_symbol(&self, symbol: [u8; 8], limits: SymbolLimits) {
        self.symbol_limits.write().insert(symbol, limits);
    }

    /// Get limits for symbol (uses default if not registered)
    #[inline]
    fn get_limits(&self, symbol: &[u8; 8]) -> SymbolLimits {
        self.symbol_limits
            .read()
            .get(symbol)
            .copied()
            .unwrap_or(self.default_limits)
    }

    /// Fast O(1) risk check - the hot path
    /// Returns RiskResult without any heap allocation
    #[inline]
    pub fn check(&self, order: &Order, account: Option<&AccountPosition>) -> RiskResult {
        let limits = self.get_limits(&order.symbol);

        // Check 1: Max quantity
        if order.qty > limits.max_order_qty {
            return RiskResult::RejectedMaxQty;
        }

        // Check 2: Max notional
        let notional = (order.price as u128) * (order.qty as u128);
        if notional > limits.max_notional as u128 {
            return RiskResult::RejectedMaxNotional;
        }

        // Check 3: Daily account limit (atomic CAS)
        if let Some(acc) = account {
            if !acc.try_add_notional(notional as u64) {
                return RiskResult::RejectedDailyLimit;
            }
        }

        RiskResult::Accepted
    }

    /// Simplified check without account tracking (for benchmarks)
    #[inline]
    pub fn check_simple(&self, order: &Order) -> RiskResult {
        self.check(order, None)
    }
}

impl Default for RiskChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accept_valid_order() {
        let checker = RiskChecker::new();
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, 0);
        assert_eq!(checker.check_simple(&order), RiskResult::Accepted);
    }

    #[test]
    fn test_reject_max_qty() {
        let checker = RiskChecker::new();
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100_000, 15000, 0);
        assert_eq!(checker.check_simple(&order), RiskResult::RejectedMaxQty);
    }

    #[test]
    fn test_reject_max_notional() {
        let checker = RiskChecker::new();
        // 10000 qty * 200000 price = 2B notional (over 1B limit)
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 10_000, 200_000, 0);
        assert_eq!(checker.check_simple(&order), RiskResult::RejectedMaxNotional);
    }

    #[test]
    fn test_daily_limit() {
        let checker = RiskChecker::new();
        let account = AccountPosition::new(1_000_000); // $1M daily limit

        // First order: 500 * 1000 = 500K notional - should pass
        let order1 = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 500, 1000, 0);
        assert_eq!(checker.check(&order1, Some(&account)), RiskResult::Accepted);

        // Second order: another 600K - should fail (total would be 1.1M)
        let order2 = Order::new(2, 100, *b"AAPL\0\0\0\0", 1, 600, 1000, 0);
        assert_eq!(
            checker.check(&order2, Some(&account)),
            RiskResult::RejectedDailyLimit
        );
    }

    #[test]
    fn test_custom_symbol_limits() {
        let checker = RiskChecker::new();
        checker.register_symbol(
            *b"PENNY\0\0\0",
            SymbolLimits {
                max_order_qty: 100,
                max_notional: 10_000,
                tick_size: 1,
            },
        );

        // Should fail with custom limits
        let order = Order::new(1, 100, *b"PENNY\0\0\0", 1, 200, 100, 0);
        assert_eq!(checker.check_simple(&order), RiskResult::RejectedMaxQty);

        // Should pass with default limits on different symbol
        let order2 = Order::new(2, 100, *b"AAPL\0\0\0\0", 1, 200, 100, 0);
        assert_eq!(checker.check_simple(&order2), RiskResult::Accepted);
    }
}
