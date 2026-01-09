//! Exchange プロトコル定義
//!
//! Kotlin Gateway と同じ JSONL 形式。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TcpExchangeRequestType {
    New,
    Cancel,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Pending,
    Accepted,
    Sent,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

impl OrderStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected)
    }
}

/// 取引所へのリクエスト
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpExchangeRequest {
    #[serde(rename = "type")]
    pub request_type: TcpExchangeRequestType,
    pub order_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<OrderSide>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<i64>,
}

/// 取引所からの Execution Report
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionReport {
    pub order_id: String,
    pub status: OrderStatus,
    pub filled_qty_delta: i64,
    pub filled_qty_total: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<i64>,
    pub at: String, // ISO8601
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let req = TcpExchangeRequest {
            request_type: TcpExchangeRequestType::New,
            order_id: "order-1".to_string(),
            symbol: Some("AAPL".to_string()),
            side: Some(OrderSide::Buy),
            qty: Some(100),
            price: Some(15000),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"type\":\"NEW\""));
        assert!(json.contains("\"orderId\":\"order-1\""));
        assert!(json.contains("\"side\":\"BUY\""));
    }

    #[test]
    fn test_execution_report_deserialization() {
        let json = r#"{"orderId":"order-1","status":"ACCEPTED","filledQtyDelta":0,"filledQtyTotal":0,"price":null,"at":"2025-01-09T12:00:00Z"}"#;
        let report: ExecutionReport = serde_json::from_str(json).unwrap();
        assert_eq!(report.order_id, "order-1");
        assert_eq!(report.status, OrderStatus::Accepted);
    }
}
