//! レイテンシ計測モジュール
//!
//! ## 目的
//! - ナノ秒精度でホットパスの処理時間を測定
//! - p50/p95/p99 などのパーセンタイルをリアルタイム算出
//! - オーバーヘッド最小化（計測が処理を遅くしては本末転倒）
//!
//! ## 設計思想
//! - ヒストグラム方式: 個々の値を保存せず、バケットにカウント
//! - Atomic 操作: マルチスレッドでも mutex 不要
//! - 固定バケット: メモリ確保なし

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// 現在時刻をナノ秒で取得
///
/// ## 実装方式
/// - プロセス起動時の Instant を基準に経過時間を計算
/// - OnceLock で初回のみ初期化（以降はゼロコスト参照）
///
/// ## プラットフォーム別の内部実装
/// - macOS (Apple Silicon): mach_absolute_time()
/// - Linux: clock_gettime(CLOCK_MONOTONIC)
/// - Windows: QueryPerformanceCounter()
#[inline]
pub fn now_nanos() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// レイテンシ分布ヒストグラム
///
/// ## なぜヒストグラム？
/// - 全データを保存すると O(n) メモリ
/// - ヒストグラムなら O(1) メモリ（固定8バケット）
/// - パーセンタイルは近似だが、実用上十分な精度
///
/// ## バケット境界
/// ```text
/// [0]   0 - 100ns    : 超高速（キャッシュヒット）
/// [1] 100 - 500ns    : 高速
/// [2] 500ns - 1μs    : 良好
/// [3]  1μs - 5μs     : 許容範囲
/// [4]  5μs - 10μs    : 要注意
/// [5] 10μs - 50μs    : 問題あり
/// [6] 50μs - 100μs   : 深刻
/// [7]     100μs+     : 異常（GC? コンテキストスイッチ?）
/// ```
#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; 8],   // 各バケットのカウント
    sum_nanos: AtomicU64,      // 合計（平均計算用）
    count: AtomicU64,          // 総サンプル数
    min_nanos: AtomicU64,      // 最小値
    max_nanos: AtomicU64,      // 最大値
}

/// バケット境界値（ナノ秒）
impl LatencyHistogram {
    pub const BUCKET_BOUNDS: [u64; 8] = [
        100,        // ~ 100ns
        500,        // ~ 500ns
        1_000,      // ~ 1μs
        5_000,      // ~ 5μs
        10_000,     // ~ 10μs
        50_000,     // ~ 50μs
        100_000,    // ~ 100μs
        u64::MAX,   // それ以上
    ];

    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
            min_nanos: AtomicU64::new(u64::MAX),
            max_nanos: AtomicU64::new(0),
        }
    }

    /// レイテンシを記録
    ///
    /// ## 処理内容
    /// 1. 該当バケットを特定してカウントアップ
    /// 2. 合計・カウントを更新（平均計算用）
    /// 3. min/max を CAS で更新
    #[inline]
    pub fn record(&self, latency_nanos: u64) {
        // バケット特定: 最初に境界を超えた位置
        let bucket_idx = Self::BUCKET_BOUNDS
            .iter()
            .position(|&bound| latency_nanos <= bound)
            .unwrap_or(7);

        // Relaxed: 順序保証不要、速度優先
        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(latency_nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        // min 更新（CAS ループ）
        let mut current_min = self.min_nanos.load(Ordering::Relaxed);
        while latency_nanos < current_min {
            match self.min_nanos.compare_exchange_weak(
                current_min,
                latency_nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // max 更新（CAS ループ）
        let mut current_max = self.max_nanos.load(Ordering::Relaxed);
        while latency_nanos > current_max {
            match self.max_nanos.compare_exchange_weak(
                current_max,
                latency_nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    /// 統計スナップショットを取得
    pub fn snapshot(&self) -> LatencyStats {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum_nanos.load(Ordering::Relaxed);
        let min = self.min_nanos.load(Ordering::Relaxed);
        let max = self.max_nanos.load(Ordering::Relaxed);

        let buckets: [u64; 8] = std::array::from_fn(|i| {
            self.buckets[i].load(Ordering::Relaxed)
        });

        LatencyStats {
            count,
            sum_nanos: sum,
            min_nanos: if min == u64::MAX { 0 } else { min },
            max_nanos: max,
            mean_nanos: if count > 0 { sum / count } else { 0 },
            buckets,
        }
    }

    /// 全カウンタをリセット
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.sum_nanos.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        self.min_nanos.store(u64::MAX, Ordering::Relaxed);
        self.max_nanos.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// 統計スナップショット
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub count: u64,
    pub sum_nanos: u64,
    pub min_nanos: u64,
    pub max_nanos: u64,
    pub mean_nanos: u64,
    pub buckets: [u64; 8],
}

impl LatencyStats {
    /// パーセンタイル値を推定
    ///
    /// ## 計算方法
    /// 1. 目標順位を計算（例: p99 なら上位1%の位置）
    /// 2. バケットを累積して目標順位を含むバケットを特定
    /// 3. そのバケットの上限値を返す（近似）
    ///
    /// ## 精度
    /// - バケット境界でしか値を返せないため、粗い近似
    /// - より精度が必要なら HDR Histogram 等を検討
    pub fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }

        let target = (self.count as f64 * p / 100.0).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, &bucket_count) in self.buckets.iter().enumerate() {
            cumulative += bucket_count;
            if cumulative >= target {
                return LatencyHistogram::BUCKET_BOUNDS[i];
            }
        }

        LatencyHistogram::BUCKET_BOUNDS[7]
    }

    /// 人間可読な要約文字列
    pub fn summary(&self) -> String {
        format!(
            "count={}, min={}ns, mean={}ns, max={}ns, p50={}ns, p99={}ns",
            self.count,
            self.min_nanos,
            self.mean_nanos,
            self.max_nanos,
            self.percentile(50.0),
            self.percentile(99.0)
        )
    }
}

/// RAII ガード: スコープ終了時に自動で経過時間を記録
///
/// ## 使用例
/// ```ignore
/// let hist = LatencyHistogram::new();
/// {
///     let _guard = LatencyGuard::new(&hist);
///     // この中の処理時間が自動計測される
///     do_something();
/// } // ここで drop され、record() が呼ばれる
/// ```
pub struct LatencyGuard<'a> {
    histogram: &'a LatencyHistogram,
    start: u64,
}

impl<'a> LatencyGuard<'a> {
    #[inline]
    pub fn new(histogram: &'a LatencyHistogram) -> Self {
        Self {
            histogram,
            start: now_nanos(),
        }
    }
}

impl Drop for LatencyGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let elapsed = now_nanos() - self.start;
        self.histogram.record(elapsed);
    }
}

/// コードブロックの計測マクロ
#[macro_export]
macro_rules! time_block {
    ($histogram:expr, $block:expr) => {{
        let _guard = $crate::metrics::LatencyGuard::new($histogram);
        $block
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_basic() {
        let hist = LatencyHistogram::new();

        hist.record(50);    // バケット[0]: 0-100ns
        hist.record(200);   // バケット[1]: 100-500ns
        hist.record(800);   // バケット[2]: 500ns-1μs
        hist.record(3000);  // バケット[3]: 1-5μs

        let stats = hist.snapshot();
        assert_eq!(stats.count, 4);
        assert_eq!(stats.min_nanos, 50);
        assert_eq!(stats.max_nanos, 3000);
    }

    #[test]
    fn test_percentile() {
        let hist = LatencyHistogram::new();

        // 99サンプルを最初のバケットに
        for _ in 0..99 {
            hist.record(50);
        }
        // 1サンプルを最後のバケットに
        hist.record(200_000);

        let stats = hist.snapshot();
        assert_eq!(stats.percentile(50.0), 100);      // p50: バケット[0]
        assert_eq!(stats.percentile(99.0), 100);      // p99: バケット[0]
        assert_eq!(stats.percentile(100.0), u64::MAX); // p100: バケット[7]
    }

    #[test]
    fn test_now_nanos_monotonic() {
        let t1 = now_nanos();
        let t2 = now_nanos();
        assert!(t2 >= t1);  // 単調増加を確認
    }

    #[test]
    fn test_latency_guard() {
        let hist = LatencyHistogram::new();
        {
            let _guard = LatencyGuard::new(&hist);
            std::hint::black_box(42);  // 最適化防止
        }
        let stats = hist.snapshot();
        assert_eq!(stats.count, 1);
        assert!(stats.min_nanos > 0);
    }
}
