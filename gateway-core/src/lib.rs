//! Gateway Core - 超低レイテンシ注文処理ライブラリ（Rust）
//!
//! ## 概要
//! 注文ゲートウェイの「ホットパス」をRustで実装。
//! GCを持たない言語でナノ秒レベルの安定したレイテンシを実現する。
//!
//! ## 構成
//! - `queue.rs`: ロックフリー・リングバッファ（crossbeam ArrayQueue）
//! - `risk.rs`: O(1) リスクチェック（数量・想定元本・口座枠）
//! - `metrics.rs`: ナノ秒精度レイテンシ計測（ヒストグラム）
//!
//! ## 性能目標
//! - キュー + リスクチェック合計: p99 < 1μs
//! - スループット: 9M+ ops/sec
//!
//! ## ベンチマーク実行
//! ```bash
//! cargo bench
//! ```

pub mod metrics;
pub mod queue;
pub mod risk;

// 主要な型を再エクスポート（使いやすさのため）
pub use metrics::{now_nanos, LatencyGuard, LatencyHistogram, LatencyStats};
pub use queue::{FastPathQueue, Order};
pub use risk::{AccountPosition, RiskChecker, RiskResult, SymbolLimits};

/// ホットパス: 注文を処理してレイテンシを記録
///
/// ## 処理フロー
/// 1. 開始時刻を記録
/// 2. リスクチェック実行
/// 3. 終了時刻を記録、ヒストグラムに追加
///
/// ## 使用例
/// ```ignore
/// let result = process_order(&order, &checker, &histogram);
/// match result {
///     RiskResult::Accepted => send_to_exchange(&order),
///     _ => reject_order(&order, result),
/// }
/// ```
#[inline]
pub fn process_order(
    order: &Order,
    checker: &RiskChecker,
    histogram: &LatencyHistogram,
) -> RiskResult {
    let start = now_nanos();
    let result = checker.check_simple(order);
    let elapsed = now_nanos() - start;
    histogram.record(elapsed);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 統合テスト: キュー → リスクチェック → 計測
    #[test]
    fn test_fast_path_integration() {
        let queue = FastPathQueue::new(1024);
        let checker = RiskChecker::new();
        let histogram = LatencyHistogram::new();

        // 注文作成 → キュー投入
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, now_nanos());
        queue.push(order).unwrap();

        // キューから取り出し → 処理
        let dequeued = queue.pop().unwrap();
        let result = process_order(&dequeued, &checker, &histogram);

        assert_eq!(result, RiskResult::Accepted);

        let stats = histogram.snapshot();
        assert_eq!(stats.count, 1);
        println!("ホットパス・レイテンシ: {}", stats.summary());
    }

    /// 高スループットテスト: 10万注文処理
    #[test]
    fn test_high_throughput() {
        let queue = FastPathQueue::new(65536);
        let checker = RiskChecker::new();
        let histogram = LatencyHistogram::new();

        for i in 0..100_000u64 {
            let order = Order::new(i, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, now_nanos());
            queue.push(order).unwrap();

            let dequeued = queue.pop().unwrap();
            process_order(&dequeued, &checker, &histogram);
        }

        let stats = histogram.snapshot();
        assert_eq!(stats.count, 100_000);
        println!(
            "10万注文処理完了 - p50: {}ns, p99: {}ns, max: {}ns",
            stats.percentile(50.0),
            stats.percentile(99.0),
            stats.max_nanos
        );
    }
}
