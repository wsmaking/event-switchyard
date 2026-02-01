//! 注文関連の型定義
//!
//! HTTPリクエスト/レスポンスで使用する構造体。
//! Kotlin Gateway (HttpGateway.kt) と互換性のある形式。

use serde::{Deserialize, Serialize};

/// 注文タイプ
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    Limit,
    Market,
}

/// TimeInForce
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeInForce {
    Gtc,
    Gtd,
}

impl Default for TimeInForce {
    fn default() -> Self {
        Self::Gtc
    }
}

/// 注文リクエスト（HTTPボディ）
///
/// Kotlin Gateway の CreateOrderRequest と互換。
/// account_id は JWT トークンから取得するため、リクエストボディには含めない。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    /// 銘柄コード
    pub symbol: String,
    /// 売買方向: "BUY" or "SELL"
    pub side: String,
    /// 注文タイプ: LIMIT or MARKET
    #[serde(rename = "type")]
    pub order_type: OrderType,
    /// 数量
    pub qty: u64,
    /// 価格（LIMIT注文の場合必須）
    #[serde(default)]
    pub price: Option<u64>,
    /// TimeInForce（デフォルト: GTC）
    #[serde(default)]
    pub time_in_force: TimeInForce,
    /// 期限（GTDの場合必須、ミリ秒エポック）
    #[serde(default)]
    pub expire_at: Option<u64>,
    /// クライアント注文ID
    #[serde(default)]
    pub client_order_id: Option<String>,
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

/// 注文レスポンス（Kotlin Gateway互換）
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    /// 注文ID (文字列形式)
    pub order_id: String,
    /// 受理シーケンス（内部IDベース）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accept_seq: Option<u64>,
    /// サーバー発行のリクエストID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// クライアント注文ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    /// ステータス: "ACCEPTED", "REJECTED"
    pub status: String,
    /// 拒否理由（REJECTEDの場合のみ）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl OrderResponse {
    pub fn accepted(
        order_id: &str,
        accept_seq: Option<u64>,
        request_id: Option<String>,
        client_order_id: Option<String>,
    ) -> Self {
        Self {
            order_id: order_id.to_string(),
            accept_seq,
            request_id,
            client_order_id,
            status: "ACCEPTED".into(),
            reason: None,
        }
    }

    pub fn rejected(reason: &str) -> Self {
        Self {
            order_id: String::new(),
            accept_seq: None,
            request_id: None,
            client_order_id: None,
            status: "REJECTED".into(),
            reason: Some(reason.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OrderRequest;
    use std::fs;
    use std::path::PathBuf;

    fn find_fixture(rel: &str) -> PathBuf {
        let mut dir = std::env::current_dir().expect("cwd");
        for _ in 0..6 {
            let candidate = dir.join(rel);
            if candidate.exists() {
                return candidate;
            }
            if !dir.pop() {
                break;
            }
        }
        panic!("fixture not found: {rel}");
    }

    #[test]
    fn order_request_fixture_deserializes() {
        let path = find_fixture("contracts/fixtures/order_request_v1.json");
        let raw = fs::read_to_string(path).expect("read fixture");
        let parsed: OrderRequest = serde_json::from_str(&raw).expect("deserialize");
        assert_eq!(parsed.symbol, "BTC");
        assert_eq!(parsed.qty, 1);
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
