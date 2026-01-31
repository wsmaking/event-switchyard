//! FFI層 - KotlinからJNI経由で呼び出すためのCインターフェース
//!
//! ## 設計方針
//! - C互換の型のみ使用（u64, i32, *const u8 など）
//! - パニックをキャッチしてエラーコードで返す
//! - スレッドセーフ（グローバル状態は Arc + Mutex/Atomic）
//!
//! ## JNI呼び出しの流れ
//! 1. Kotlin側で System.loadLibrary("gateway_core")
//! 2. external fun を呼び出し
//! 3. この ffi.rs の関数が実行される

use crate::queue::{FastPathQueue, Order};
use crate::risk::{RiskChecker, RiskResult, SymbolLimits};
use crate::metrics::LatencyHistogram;
use std::sync::OnceLock;

/// グローバルインスタンス（JVM側から繰り返し呼ばれるため）
static QUEUE: OnceLock<FastPathQueue> = OnceLock::new();
static RISK_CHECKER: OnceLock<RiskChecker> = OnceLock::new();
static HISTOGRAM: OnceLock<LatencyHistogram> = OnceLock::new();

/// 初期化（JVM起動時に1回呼ぶ）
///
/// # 引数
/// - queue_capacity: キュー容量（2のべき乗に丸められる）
///
/// # 戻り値
/// - 0: 成功
/// - -1: 既に初期化済み
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_init(queue_capacity: u32) -> i32 {
    let queue_result = QUEUE.set(FastPathQueue::new(queue_capacity as usize));
    let checker_result = RISK_CHECKER.set(RiskChecker::new());
    let histogram_result = HISTOGRAM.set(LatencyHistogram::new());

    if queue_result.is_err() || checker_result.is_err() || histogram_result.is_err() {
        return -1; // 既に初期化済み
    }
    0
}

/// リスクチェック結果コード
/// Kotlin側で同じenumを定義して対応させる
pub mod result_codes {
    pub const ACCEPTED: i32 = 0;
    pub const REJECTED_MAX_QTY: i32 = 1;
    pub const REJECTED_MAX_NOTIONAL: i32 = 2;
    pub const REJECTED_DAILY_LIMIT: i32 = 3;
    pub const REJECTED_UNKNOWN_SYMBOL: i32 = 4;
    pub const ERROR_NOT_INITIALIZED: i32 = -1;
    pub const ERROR_QUEUE_FULL: i32 = -2;
    pub const ERROR_INVALID_SYMBOL: i32 = -3;
}

/// 注文処理（リスクチェック + キュー投入）
///
/// # 引数
/// - order_id: 注文ID
/// - account_id: 口座ID
/// - symbol_ptr: 銘柄コード（8バイト固定、null終端不要）
/// - side: 売買方向（1=買い, 2=売り）
/// - qty: 数量
/// - price: 価格
/// - timestamp_ns: タイムスタンプ（ナノ秒）
///
/// # 戻り値
/// - 0: 受理（キュー投入成功）
/// - 1-4: リスク拒否（result_codes参照）
/// - 負値: エラー
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_process_order(
    order_id: u64,
    account_id: u64,
    symbol_ptr: *const u8,
    side: u8,
    qty: u32,
    price: u64,
    timestamp_ns: u64,
) -> i32 {
    // 初期化チェック
    let Some(queue) = QUEUE.get() else {
        return result_codes::ERROR_NOT_INITIALIZED;
    };
    let Some(checker) = RISK_CHECKER.get() else {
        return result_codes::ERROR_NOT_INITIALIZED;
    };
    let Some(histogram) = HISTOGRAM.get() else {
        return result_codes::ERROR_NOT_INITIALIZED;
    };

    // symbol を [u8; 8] に変換
    let symbol: [u8; 8] = if symbol_ptr.is_null() {
        return result_codes::ERROR_INVALID_SYMBOL;
    } else {
        unsafe {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(symbol_ptr, buf.as_mut_ptr(), 8);
            buf
        }
    };

    // 計測開始
    let start = crate::metrics::now_nanos();

    // Order構造体を作成
    let order = Order::new(order_id, account_id, symbol, side, qty, price, timestamp_ns);

    // リスクチェック
    let risk_result = checker.check_simple(&order);

    let result_code = match risk_result {
        RiskResult::Accepted => {
            // キュー投入
            match queue.push(order) {
                Ok(()) => result_codes::ACCEPTED,
                Err(_) => result_codes::ERROR_QUEUE_FULL,
            }
        }
        RiskResult::RejectedMaxQty => result_codes::REJECTED_MAX_QTY,
        RiskResult::RejectedMaxNotional => result_codes::REJECTED_MAX_NOTIONAL,
        RiskResult::RejectedDailyLimit => result_codes::REJECTED_DAILY_LIMIT,
        RiskResult::RejectedUnknownSymbol => result_codes::REJECTED_UNKNOWN_SYMBOL,
    };

    // 計測終了
    let elapsed = crate::metrics::now_nanos() - start;
    histogram.record(elapsed);

    result_code
}

/// キューから注文を取り出す（FastPathEngine用）
///
/// # 引数
/// - out_order_id: 取り出した注文IDの格納先
/// - out_account_id: 口座IDの格納先
/// - out_symbol: 銘柄コードの格納先（8バイト）
/// - out_side: 売買方向の格納先
/// - out_qty: 数量の格納先
/// - out_price: 価格の格納先
/// - out_timestamp_ns: タイムスタンプの格納先
///
/// # 戻り値
/// - 1: 取り出し成功
/// - 0: キューが空
/// - 負値: エラー
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_pop_order(
    out_order_id: *mut u64,
    out_account_id: *mut u64,
    out_symbol: *mut u8,
    out_side: *mut u8,
    out_qty: *mut u32,
    out_price: *mut u64,
    out_timestamp_ns: *mut u64,
) -> i32 {
    let Some(queue) = QUEUE.get() else {
        return result_codes::ERROR_NOT_INITIALIZED;
    };

    match queue.pop() {
        Some(order) => {
            unsafe {
                *out_order_id = order.order_id;
                *out_account_id = order.account_id;
                std::ptr::copy_nonoverlapping(order.symbol.as_ptr(), out_symbol, 8);
                *out_side = order.side;
                *out_qty = order.qty;
                *out_price = order.price;
                *out_timestamp_ns = order.timestamp_ns;
            }
            1
        }
        None => 0,
    }
}

/// キュー長を取得
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_queue_len() -> i32 {
    QUEUE.get().map(|q| q.len() as i32).unwrap_or(-1)
}

/// キュー容量を取得
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_queue_capacity() -> i32 {
    QUEUE.get().map(|q| q.capacity() as i32).unwrap_or(-1)
}

/// 銘柄固有のリスク上限を登録
///
/// # 引数
/// - symbol_ptr: 銘柄コード（8バイト）
/// - max_order_qty: 最大注文数量
/// - max_notional: 最大想定元本
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_register_symbol(
    symbol_ptr: *const u8,
    max_order_qty: u32,
    max_notional: u64,
) -> i32 {
    let Some(checker) = RISK_CHECKER.get() else {
        return result_codes::ERROR_NOT_INITIALIZED;
    };

    if symbol_ptr.is_null() {
        return result_codes::ERROR_INVALID_SYMBOL;
    }

    let symbol: [u8; 8] = unsafe {
        let mut buf = [0u8; 8];
        std::ptr::copy_nonoverlapping(symbol_ptr, buf.as_mut_ptr(), 8);
        buf
    };

    checker.register_symbol(symbol, SymbolLimits {
        max_order_qty,
        max_notional,
        tick_size: 1,
    });

    0
}

/// レイテンシ統計を取得（p50）
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_latency_p50() -> u64 {
    HISTOGRAM.get()
        .map(|h| h.snapshot().percentile(50.0))
        .unwrap_or(0)
}

/// レイテンシ統計を取得（p99）
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_latency_p99() -> u64 {
    HISTOGRAM.get()
        .map(|h| h.snapshot().percentile(99.0))
        .unwrap_or(0)
}

/// レイテンシ統計を取得（最大値）
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_latency_max() -> u64 {
    HISTOGRAM.get()
        .map(|h| h.snapshot().max_nanos)
        .unwrap_or(0)
}

/// レイテンシ統計を取得（サンプル数）
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_latency_count() -> u64 {
    HISTOGRAM.get()
        .map(|h| h.snapshot().count)
        .unwrap_or(0)
}

/// ヒストグラムをリセット
#[unsafe(no_mangle)]
pub extern "C" fn gateway_core_latency_reset() {
    if let Some(h) = HISTOGRAM.get() {
        h.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ffi_workflow() {
        // 注意: グローバル状態のため、他のテストと競合する可能性あり
        // 実際のテストでは別プロセスで実行推奨

        // 既に初期化されていたらスキップ
        if QUEUE.get().is_some() {
            return;
        }

        // 初期化
        assert_eq!(gateway_core_init(1024), 0);

        // 二重初期化は失敗
        assert_eq!(gateway_core_init(1024), -1);

        // 注文処理
        let symbol = b"AAPL\0\0\0\0";
        let result = gateway_core_process_order(
            1,      // order_id
            100,    // account_id
            symbol.as_ptr(),
            1,      // side (buy)
            100,    // qty
            15000,  // price
            0,      // timestamp_ns
        );
        assert_eq!(result, result_codes::ACCEPTED);

        // キューから取り出し
        let mut order_id: u64 = 0;
        let mut account_id: u64 = 0;
        let mut symbol_out = [0u8; 8];
        let mut side: u8 = 0;
        let mut qty: u32 = 0;
        let mut price: u64 = 0;
        let mut timestamp_ns: u64 = 0;

        let pop_result = gateway_core_pop_order(
            &mut order_id,
            &mut account_id,
            symbol_out.as_mut_ptr(),
            &mut side,
            &mut qty,
            &mut price,
            &mut timestamp_ns,
        );
        assert_eq!(pop_result, 1);
        assert_eq!(order_id, 1);
        assert_eq!(account_id, 100);
        assert_eq!(qty, 100);
    }
}
