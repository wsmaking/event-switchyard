//! Lock-free 注文IDマッピング
//!
//! 内部ID (u64) と外部ID ("ord_xxx") の相互変換を提供。
//! DashMap を使用しロックフリーで高速な参照を実現。

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// 注文IDマッピング
///
/// - 内部ID: u64（シーケンシャル、高速比較）
/// - 外部ID: String（"ord_{uuid}" 形式、API互換）
///
/// HFT環境では内部的にu64を使用し、APIレスポンス生成時のみ
/// 外部ID文字列に変換することでオーバーヘッドを最小化。
pub struct OrderIdMap {
    /// 内部ID -> 外部ID
    internal_to_external: DashMap<u64, String>,
    /// 外部ID -> 内部ID
    external_to_internal: DashMap<String, u64>,
    /// 内部ID -> account_id（シャードルックアップ用）
    internal_to_account: DashMap<u64, String>,
    /// 次の内部ID
    next_id: AtomicU64,
}

impl OrderIdMap {
    pub fn new() -> Self {
        Self {
            internal_to_external: DashMap::new(),
            external_to_internal: DashMap::new(),
            internal_to_account: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// 新しい内部IDを発番し、外部IDとマッピングを登録
    pub fn register(&self, external_id: String, account_id: String) -> u64 {
        let internal_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.internal_to_external.insert(internal_id, external_id.clone());
        self.external_to_internal.insert(external_id, internal_id);
        self.internal_to_account.insert(internal_id, account_id);
        internal_id
    }

    /// 外部IDを指定して内部IDを登録（内部ID事前発番済みの場合）
    pub fn register_with_internal(&self, internal_id: u64, external_id: String, account_id: String) {
        self.internal_to_external.insert(internal_id, external_id.clone());
        self.external_to_internal.insert(external_id, internal_id);
        self.internal_to_account.insert(internal_id, account_id);
    }

    /// 内部ID -> 外部ID
    #[inline]
    pub fn to_external(&self, internal_id: u64) -> Option<String> {
        self.internal_to_external.get(&internal_id).map(|v| v.clone())
    }

    /// 外部ID -> 内部ID
    #[inline]
    pub fn to_internal(&self, external_id: &str) -> Option<u64> {
        self.external_to_internal.get(external_id).map(|v| *v)
    }

    /// 内部ID -> account_id
    #[inline]
    pub fn get_account_id(&self, internal_id: u64) -> Option<String> {
        self.internal_to_account.get(&internal_id).map(|v| v.clone())
    }

    /// 外部ID -> account_id（ショートカット）
    #[inline]
    pub fn get_account_id_by_external(&self, external_id: &str) -> Option<String> {
        let internal_id = self.to_internal(external_id)?;
        self.get_account_id(internal_id)
    }

    /// マッピングを削除
    pub fn remove(&self, internal_id: u64) {
        if let Some((_, external_id)) = self.internal_to_external.remove(&internal_id) {
            self.external_to_internal.remove(&external_id);
        }
        self.internal_to_account.remove(&internal_id);
    }

    /// 外部IDでマッピングを削除
    pub fn remove_by_external(&self, external_id: &str) {
        if let Some((_, internal_id)) = self.external_to_internal.remove(external_id) {
            self.internal_to_external.remove(&internal_id);
            self.internal_to_account.remove(&internal_id);
        }
    }

    /// 登録数を取得
    pub fn count(&self) -> usize {
        self.internal_to_external.len()
    }

    /// 次のIDをピーク（テスト用）
    pub fn peek_next_id(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed)
    }
}

impl Default for OrderIdMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_lookup() {
        let map = OrderIdMap::new();

        let internal_id = map.register("ord_abc".into(), "acc_1".into());
        assert_eq!(internal_id, 1);

        assert_eq!(map.to_external(1), Some("ord_abc".into()));
        assert_eq!(map.to_internal("ord_abc"), Some(1));
        assert_eq!(map.get_account_id(1), Some("acc_1".into()));
    }

    #[test]
    fn test_sequential_ids() {
        let map = OrderIdMap::new();

        let id1 = map.register("ord_1".into(), "acc_1".into());
        let id2 = map.register("ord_2".into(), "acc_1".into());
        let id3 = map.register("ord_3".into(), "acc_2".into());

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_remove() {
        let map = OrderIdMap::new();

        let internal_id = map.register("ord_abc".into(), "acc_1".into());
        assert_eq!(map.count(), 1);

        map.remove(internal_id);
        assert_eq!(map.count(), 0);
        assert!(map.to_external(internal_id).is_none());
        assert!(map.to_internal("ord_abc").is_none());
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let map = Arc::new(OrderIdMap::new());
        let mut handles = vec![];

        // 10スレッドで並列登録
        for i in 0..10 {
            let map = Arc::clone(&map);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let external_id = format!("ord_{}_{}", i, j);
                    let account_id = format!("acc_{}", i);
                    map.register(external_id, account_id);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // 1000件登録されている
        assert_eq!(map.count(), 1000);
    }

    #[test]
    fn test_get_account_id_by_external() {
        let map = OrderIdMap::new();

        map.register("ord_abc".into(), "acc_42".into());

        assert_eq!(
            map.get_account_id_by_external("ord_abc"),
            Some("acc_42".into())
        );
        assert!(map.get_account_id_by_external("ord_nonexistent").is_none());
    }
}
