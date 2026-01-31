//! ロックフリー・リングバッファによる注文キュー
//!
//! ## なぜリングバッファか？
//! - 固定サイズ配列なのでヒープ確保が発生しない
//! - Producer/Consumer が別スレッドでも mutex 不要
//! - キャッシュ局所性が高い（連続メモリアクセス）
//!
//! ## crossbeam ArrayQueue の内部動作
//! - head/tail ポインタを atomic 操作で更新
//! - CAS (Compare-And-Swap) でスレッド競合を解決
//! - 失敗したら再試行するスピンロック方式

use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

/// 注文データ構造体（64バイト固定）
///
/// ## なぜ64バイト？
/// - CPUキャッシュラインが64バイト単位
/// - 1注文 = 1キャッシュライン → false sharing 防止
/// - メモリアライメントが効率的
///
/// ## フィールド配置の意図
/// - 8バイト境界でアラインされるようu64を先頭に配置
/// - パディングで64バイトぴったりに調整
#[repr(C)]  // Cのメモリレイアウトを強制（予測可能な配置）
#[derive(Clone, Copy, Debug, Default)]
pub struct Order {
    pub order_id: u64,      // 注文ID
    pub account_id: u64,    // 口座ID
    pub price: u64,         // 価格（整数表現、例: 15000 = $150.00）
    pub timestamp_ns: u64,  // 受信時刻（ナノ秒）
    pub symbol: [u8; 8],    // 銘柄コード（例: "AAPL\0\0\0\0"）
    pub qty: u32,           // 数量
    pub side: u8,           // 売買方向（1=買い, 2=売り）
    _padding: [u8; 19],     // 64バイトに合わせるパディング
}

impl Order {
    /// 新規注文を作成
    #[inline]  // 関数呼び出しオーバーヘッドを削減
    pub fn new(
        order_id: u64,
        account_id: u64,
        symbol: [u8; 8],
        side: u8,
        qty: u32,
        price: u64,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            order_id,
            account_id,
            price,
            timestamp_ns,
            symbol,
            qty,
            side,
            _padding: [0u8; 19],
        }
    }

    /// 買い注文かどうか
    #[inline]
    pub fn is_buy(&self) -> bool {
        self.side == 1
    }
}

/// 高速注文キュー
///
/// ## 設計思想
/// - ロックフリー: mutex を使わず atomic 操作のみ
/// - バックプレッシャー: キューが満杯なら即座にエラーを返す（ブロックしない）
/// - スレッド間共有: Arc で参照カウント管理
pub struct FastPathQueue {
    inner: Arc<ArrayQueue<Order>>,  // crossbeam のロックフリーキュー
    capacity: usize,
}

impl FastPathQueue {
    /// 指定容量でキューを作成
    ///
    /// ## なぜ2のべき乗に丸める？
    /// - ビット演算でインデックス計算が高速化（mod → AND）
    /// - 例: index % 1024 → index & 1023
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        Self {
            inner: Arc::new(ArrayQueue::new(capacity)),
            capacity,
        }
    }

    /// 注文をキューに追加
    ///
    /// ## 戻り値
    /// - Ok(()): 成功
    /// - Err(order): キュー満杯（注文を返却）
    ///
    /// ## バックプレッシャーの意図
    /// 満杯時にブロックせず即座に失敗を返すことで、
    /// 上流に「処理が追いついていない」ことを伝える
    #[inline]
    pub fn push(&self, order: Order) -> Result<(), Order> {
        self.inner.push(order)
    }

    /// キューから注文を取り出す
    ///
    /// ## 戻り値
    /// - Some(order): 注文を取得
    /// - None: キューが空
    #[inline]
    pub fn pop(&self) -> Option<Order> {
        self.inner.pop()
    }

    /// キューが空かどうか
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// 現在のキュー長
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// キュー容量
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// スレッド間で共有するためのハンドルを複製
    ///
    /// Arc::clone により参照カウントが増えるだけで、
    /// キュー本体はコピーされない
    pub fn clone_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            capacity: self.capacity,
        }
    }
}

// ArrayQueue 内部で atomic 操作を使っているため、
// 手動で Send/Sync を実装しても安全
unsafe impl Send for FastPathQueue {}
unsafe impl Sync for FastPathQueue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_size() {
        // Order が正確に64バイトであることを保証
        assert_eq!(std::mem::size_of::<Order>(), 64);
    }

    #[test]
    fn test_queue_push_pop() {
        let queue = FastPathQueue::new(1024);
        let order = Order::new(1, 100, *b"AAPL\0\0\0\0", 1, 100, 15000, 0);

        assert!(queue.push(order).is_ok());
        assert_eq!(queue.len(), 1);

        let popped = queue.pop().unwrap();
        assert_eq!(popped.order_id, 1);
        assert_eq!(popped.account_id, 100);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_queue_full() {
        let queue = FastPathQueue::new(2);
        let order = Order::default();

        assert!(queue.push(order).is_ok());
        assert!(queue.push(order).is_ok());
        assert!(queue.push(order).is_err()); // 満杯 → 失敗
    }

    #[test]
    fn test_power_of_two_capacity() {
        let queue = FastPathQueue::new(100);
        assert_eq!(queue.capacity(), 128); // 128 に丸められる
    }
}
