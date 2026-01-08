//! FastPath エンジン
//!
//! 注文の受付 → リスクチェック → キュー投入 を担当。
//! gateway-core の機能をそのまま使用し、FFIオーバーヘッドなしで動作。

use gateway_core::{
    now_nanos, FastPathQueue, LatencyHistogram, Order, RiskChecker, RiskResult,
};
use std::sync::Arc;

/// 注文処理結果
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessResult {
    /// 受理（キュー投入成功）
    Accepted,
    /// リスク拒否: 数量超過
    RejectedMaxQty,
    /// リスク拒否: 想定元本超過
    RejectedMaxNotional,
    /// リスク拒否: 日次上限超過
    RejectedDailyLimit,
    /// リスク拒否: 未登録銘柄
    RejectedUnknownSymbol,
    /// エラー: キュー満杯
    ErrorQueueFull,
}

impl From<RiskResult> for ProcessResult {
    fn from(r: RiskResult) -> Self {
        match r {
            RiskResult::Accepted => ProcessResult::Accepted,
            RiskResult::RejectedMaxQty => ProcessResult::RejectedMaxQty,
            RiskResult::RejectedMaxNotional => ProcessResult::RejectedMaxNotional,
            RiskResult::RejectedDailyLimit => ProcessResult::RejectedDailyLimit,
            RiskResult::RejectedUnknownSymbol => ProcessResult::RejectedUnknownSymbol,
        }
    }
}

/// FastPath エンジン
///
/// スレッドセーフ。複数スレッドから同時にprocess_orderを呼び出し可能。
pub struct FastPathEngine {
    queue: Arc<FastPathQueue>,
    risk_checker: Arc<RiskChecker>,
    histogram: Arc<LatencyHistogram>,
}

impl FastPathEngine {
    /// 新しいエンジンを作成
    ///
    /// # 引数
    /// - `queue_capacity`: キュー容量（2のべき乗に丸められる）
    pub fn new(queue_capacity: usize) -> Self {
        Self {
            queue: Arc::new(FastPathQueue::new(queue_capacity)),
            risk_checker: Arc::new(RiskChecker::new()),
            histogram: Arc::new(LatencyHistogram::new()),
        }
    }

    /// 注文を処理（リスクチェック → キュー投入）
    ///
    /// # 引数
    /// - `order_id`: 注文ID
    /// - `account_id`: 口座ID
    /// - `symbol`: 銘柄コード（8バイト）
    /// - `side`: 売買方向（1=買い, 2=売り）
    /// - `qty`: 数量
    /// - `price`: 価格
    ///
    /// # 戻り値
    /// 処理結果
    pub fn process_order(
        &self,
        order_id: u64,
        account_id: u64,
        symbol: [u8; 8],
        side: u8,
        qty: u32,
        price: u64,
    ) -> ProcessResult {
        let start = now_nanos();

        // Order構造体を作成
        let order = Order::new(order_id, account_id, symbol, side, qty, price, start);

        // リスクチェック
        let risk_result = self.risk_checker.check_simple(&order);

        let result = match risk_result {
            RiskResult::Accepted => {
                // キュー投入
                match self.queue.push(order) {
                    Ok(()) => ProcessResult::Accepted,
                    Err(_) => ProcessResult::ErrorQueueFull,
                }
            }
            other => ProcessResult::from(other),
        };

        // レイテンシ記録
        let elapsed = now_nanos() - start;
        self.histogram.record(elapsed);

        result
    }

    /// シンボル文字列から [u8; 8] に変換
    ///
    /// 8文字未満の場合は0埋め
    pub fn symbol_to_bytes(symbol: &str) -> [u8; 8] {
        let mut buf = [0u8; 8];
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(8);
        buf[..len].copy_from_slice(&bytes[..len]);
        buf
    }

    /// キューから注文を取り出す
    pub fn pop_order(&self) -> Option<Order> {
        self.queue.pop()
    }

    /// キュー長を取得
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// レイテンシ統計: p50 (ナノ秒)
    pub fn latency_p50(&self) -> u64 {
        self.histogram.snapshot().percentile(50.0)
    }

    /// レイテンシ統計: p99 (ナノ秒)
    pub fn latency_p99(&self) -> u64 {
        self.histogram.snapshot().percentile(99.0)
    }

    /// レイテンシ統計: 最大値 (ナノ秒)
    pub fn latency_max(&self) -> u64 {
        self.histogram.snapshot().max_nanos
    }

    /// レイテンシ統計をリセット
    pub fn latency_reset(&self) {
        self.histogram.reset();
    }
}

// Arc で包んでいるのでClone可能
impl Clone for FastPathEngine {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            risk_checker: Arc::clone(&self.risk_checker),
            histogram: Arc::clone(&self.histogram),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_order_accepted() {
        let engine = FastPathEngine::new(1024);
        let symbol = FastPathEngine::symbol_to_bytes("AAPL");

        let result = engine.process_order(1, 100, symbol, 1, 100, 15000);

        assert_eq!(result, ProcessResult::Accepted);
        assert_eq!(engine.queue_len(), 1);
    }

    #[test]
    fn test_symbol_conversion() {
        let symbol = FastPathEngine::symbol_to_bytes("AAPL");
        assert_eq!(&symbol[..4], b"AAPL");
        assert_eq!(&symbol[4..], &[0, 0, 0, 0]);

        let long_symbol = FastPathEngine::symbol_to_bytes("VERYLONGSYMBOL");
        assert_eq!(&long_symbol, b"VERYLONG");
    }
}
