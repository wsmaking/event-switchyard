//! 注文関連の型定義
//!
//! HTTPリクエスト/レスポンスで使用する構造体。

use serde::{Deserialize, Serialize};

/// 注文リクエスト（HTTPボディ）
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    /// 口座ID
    pub account_id: u64,
    /// 銘柄コード
    pub symbol: String,
    /// 売買方向: "BUY" or "SELL"
    pub side: String,
    /// 数量
    pub qty: u32,
    /// 価格
    pub price: u64,
}

impl OrderRequest {
    /// side を数値に変換（1=BUY, 2=SELL）
    pub fn side_byte(&self) -> u8 {
        match self.side.to_uppercase().as_str() {
            "BUY" => 1,
            "SELL" => 2,
            _ => 0,
        }
    }
}

/// 注文レスポンス
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    /// 注文ID
    pub order_id: u64,
    /// ステータス: "ACCEPTED", "REJECTED", "ERROR"
    pub status: String,
    /// メッセージ（拒否理由など）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// 処理時間（ナノ秒）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ns: Option<u64>,
}

impl OrderResponse {
    pub fn accepted(order_id: u64) -> Self {
        Self {
            order_id,
            status: "ACCEPTED".into(),
            message: None,
            latency_ns: None,
        }
    }

    pub fn rejected(order_id: u64, reason: &str) -> Self {
        Self {
            order_id,
            status: "REJECTED".into(),
            message: Some(reason.into()),
            latency_ns: None,
        }
    }

    pub fn error(order_id: u64, reason: &str) -> Self {
        Self {
            order_id,
            status: "ERROR".into(),
            message: Some(reason.into()),
            latency_ns: None,
        }
    }

    pub fn with_latency(mut self, latency_ns: u64) -> Self {
        self.latency_ns = Some(latency_ns);
        self
    }
}

/// ヘルスチェックレスポンス
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    pub status: String,
    pub queue_len: usize,
    pub latency_p50_ns: u64,
    pub latency_p99_ns: u64,
}
