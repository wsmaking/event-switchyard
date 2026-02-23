//! リスクチェック（注文バリデーション）
//!
//! ## 目的
//! 不正・過大な注文を取引所に送る前にブロックする
//! - Fat finger 防止（誤発注）
//! - 口座枠超過防止
//! - コンプライアンス要件
//!
//! ## 性能目標
//! - O(1) 計算量（注文数に依存しない）
//! - ヒープ確保なし（GC 負荷ゼロ）
//! - 6ns 以下

use crate::queue::Order;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// リスクチェック結果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskResult {
    Accepted,               // 通過
    RejectedMaxQty,         // 最大数量超過
    RejectedMaxNotional,    // 最大想定元本超過（価格×数量）
    RejectedDailyLimit,     // 日次取引枠超過
    RejectedUnknownSymbol,  // 未登録銘柄
}

/// 銘柄ごとのリスク上限
#[derive(Debug, Clone, Copy)]
pub struct SymbolLimits {
    pub max_order_qty: u32,   // 1注文あたり最大数量
    pub max_notional: u64,    // 1注文あたり最大想定元本
    pub tick_size: u64,       // 最小価格刻み（将来用）
}

impl Default for SymbolLimits {
    fn default() -> Self {
        Self {
            max_order_qty: 10_000,
            max_notional: 1_000_000_000, // 10億（$10M相当）
            tick_size: 1,
        }
    }
}

/// 口座ポジション（日次累積）
///
/// ## なぜ AtomicU64 か？
/// - 複数スレッドから同時にチェック・更新される
/// - mutex を使うとレイテンシが悪化
/// - CAS (Compare-And-Swap) でロックフリー更新
pub struct AccountPosition {
    pub daily_notional: AtomicU64,  // 日次累積想定元本
    pub daily_limit: u64,           // 日次上限
}

impl AccountPosition {
    pub fn new(daily_limit: u64) -> Self {
        Self {
            daily_notional: AtomicU64::new(0),
            daily_limit,
        }
    }

    /// 想定元本を加算（上限チェック付き）
    ///
    /// ## CAS ループの仕組み
    /// 1. 現在値を読む
    /// 2. 新しい値を計算
    /// 3. 「現在値が変わっていなければ」新値に更新
    /// 4. 他スレッドに先を越されたら 1 に戻る
    ///
    /// これにより mutex なしで安全に更新できる
    #[inline]
    pub fn try_add_notional(&self, notional: u64) -> bool {
        let mut current = self.daily_notional.load(Ordering::Relaxed);
        loop {
            let new_value = current.saturating_add(notional);
            if new_value > self.daily_limit {
                return false;  // 上限超過
            }
            // CAS: current のままなら new_value に更新
            match self.daily_notional.compare_exchange_weak(
                current,
                new_value,
                Ordering::SeqCst,  // 成功時は強い順序保証
                Ordering::Relaxed, // 失敗時は緩い順序でOK
            ) {
                Ok(_) => return true,   // 更新成功
                Err(x) => current = x,  // 他スレッドに先を越された → 再試行
            }
        }
    }

    /// 日次リセット（営業日開始時に呼ぶ）
    pub fn reset(&self) {
        self.daily_notional.store(0, Ordering::SeqCst);
    }

    /// 加算済み想定元本の補正（ロールバック用途）。
    #[inline]
    pub fn sub_notional(&self, notional: u64) {
        let _ = self.daily_notional.fetch_update(
            Ordering::SeqCst,
            Ordering::Relaxed,
            |cur| Some(cur.saturating_sub(notional)),
        );
    }
}

/// リスクチェッカー
///
/// ## 設計思想
/// - 銘柄設定の参照: RwLock（読み取り頻度 >> 更新頻度）
/// - チェック処理自体: ロックフリー（計算のみ）
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

    /// 銘柄固有のリスク上限を登録
    pub fn register_symbol(&self, symbol: [u8; 8], limits: SymbolLimits) {
        self.symbol_limits.write().insert(symbol, limits);
    }

    /// 銘柄の上限を取得（未登録ならデフォルト）
    #[inline]
    fn get_limits(&self, symbol: &[u8; 8]) -> SymbolLimits {
        self.symbol_limits
            .read()
            .get(symbol)
            .copied()
            .unwrap_or(self.default_limits)
    }

    /// リスクチェック本体（ホットパス）
    ///
    /// ## チェック順序（早期リターン）
    /// 1. 数量チェック（最も軽量）
    /// 2. 想定元本チェック（乗算1回）
    /// 3. 口座枠チェック（CAS、最も重い）
    ///
    /// 軽いチェックを先にすることで、
    /// 大半の不正注文を早期に弾ける
    #[inline]
    pub fn check(&self, order: &Order, account: Option<&AccountPosition>) -> RiskResult {
        let limits = self.get_limits(&order.symbol);

        // チェック1: 最大数量
        if order.qty > limits.max_order_qty {
            return RiskResult::RejectedMaxQty;
        }

        // チェック2: 最大想定元本
        // u128 でオーバーフロー防止（price * qty が u64 を超える可能性）
        let notional = (order.price as u128) * (order.qty as u128);
        if notional > limits.max_notional as u128 {
            return RiskResult::RejectedMaxNotional;
        }

        // チェック3: 口座日次枠（CAS で atomic 更新）
        if let Some(acc) = account {
            if !acc.try_add_notional(notional as u64) {
                return RiskResult::RejectedDailyLimit;
            }
        }

        RiskResult::Accepted
    }

    /// 口座チェックなし版（ベンチマーク用）
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
        // 100,000株 → デフォルト上限10,000を超過
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100_000, 15000, 0);
        assert_eq!(checker.check_simple(&order), RiskResult::RejectedMaxQty);
    }

    #[test]
    fn test_reject_max_notional() {
        let checker = RiskChecker::new();
        // 10,000株 × $2,000 = $20M → デフォルト上限$10Mを超過
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 10_000, 200_000, 0);
        assert_eq!(checker.check_simple(&order), RiskResult::RejectedMaxNotional);
    }

    #[test]
    fn test_daily_limit() {
        let checker = RiskChecker::new();
        let account = AccountPosition::new(1_000_000); // 日次100万ドル枠

        // 1回目: 500株 × $1,000 = $500K → 通過
        let order1 = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 500, 1000, 0);
        assert_eq!(checker.check(&order1, Some(&account)), RiskResult::Accepted);

        // 2回目: 600株 × $1,000 = $600K → 累計$1.1M で超過
        let order2 = Order::new(2, 100, *b"AAPL\0\0\0\0", 1, 600, 1000, 0);
        assert_eq!(
            checker.check(&order2, Some(&account)),
            RiskResult::RejectedDailyLimit
        );
    }

    #[test]
    fn test_custom_symbol_limits() {
        let checker = RiskChecker::new();
        // ペニー株は上限を厳しく
        checker.register_symbol(
            *b"PENNY\0\0\0",
            SymbolLimits {
                max_order_qty: 100,
                max_notional: 10_000,
                tick_size: 1,
            },
        );

        // PENNY: 200株 → 上限100を超過
        let order = Order::new(1, 100, *b"PENNY\0\0\0", 1, 200, 100, 0);
        assert_eq!(checker.check_simple(&order), RiskResult::RejectedMaxQty);

        // AAPL: デフォルト上限適用 → 通過
        let order2 = Order::new(2, 100, *b"AAPL\0\0\0\0", 1, 200, 100, 0);
        assert_eq!(checker.check_simple(&order2), RiskResult::Accepted);
    }
}
