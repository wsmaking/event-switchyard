//! バイナリプロトコル定義
//!
//! シンプルな固定長バイナリフォーマットで最小オーバーヘッドを実現。
//!
//! ## フォーマット
//! ```text
//! Request (48 bytes):
//!   order_id:   u64 (8)
//!   account_id: u64 (8)
//!   symbol:     [u8; 8]
//!   side:       u8 (1)
//!   _pad:       [u8; 3]
//!   qty:        u32 (4)
//!   price:      u64 (8)
//!   timestamp:  u64 (8)
//!
//! Response (24 bytes):
//!   order_id:   u64 (8)
//!   status:     u8 (1)
//!   _pad:       [u8; 7]
//!   latency_ns: u64 (8)
//! ```

/// リクエストサイズ (48 bytes)
pub const REQUEST_SIZE: usize = 48;

/// レスポンスサイズ (24 bytes)
pub const RESPONSE_SIZE: usize = 24;

/// ステータスコード
pub mod status {
    pub const ACCEPTED: u8 = 0;
    pub const REJECTED_MAX_QTY: u8 = 1;
    pub const REJECTED_MAX_NOTIONAL: u8 = 2;
    pub const REJECTED_DAILY_LIMIT: u8 = 3;
    pub const REJECTED_UNKNOWN_SYMBOL: u8 = 4;
    pub const ERROR_QUEUE_FULL: u8 = 5;
}

/// リクエストをパース
#[inline]
pub fn parse_request(buf: &[u8; REQUEST_SIZE]) -> (u64, u64, [u8; 8], u8, u32, u64, u64) {
    let order_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let account_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let mut symbol = [0u8; 8];
    symbol.copy_from_slice(&buf[16..24]);
    let side = buf[24];
    // buf[25..28] is padding
    let qty = u32::from_le_bytes(buf[28..32].try_into().unwrap());
    let price = u64::from_le_bytes(buf[32..40].try_into().unwrap());
    let timestamp = u64::from_le_bytes(buf[40..48].try_into().unwrap());
    (order_id, account_id, symbol, side, qty, price, timestamp)
}

/// レスポンスを構築
#[inline]
pub fn build_response(buf: &mut [u8; RESPONSE_SIZE], order_id: u64, status: u8, latency_ns: u64) {
    buf[0..8].copy_from_slice(&order_id.to_le_bytes());
    buf[8] = status;
    buf[9..16].fill(0); // padding
    buf[16..24].copy_from_slice(&latency_ns.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_response_roundtrip() {
        // リクエスト構築
        let mut req_buf = [0u8; REQUEST_SIZE];
        req_buf[0..8].copy_from_slice(&1u64.to_le_bytes()); // order_id
        req_buf[8..16].copy_from_slice(&100u64.to_le_bytes()); // account_id
        req_buf[16..24].copy_from_slice(b"AAPL\0\0\0\0"); // symbol
        req_buf[24] = 1; // side (BUY)
        req_buf[28..32].copy_from_slice(&100u32.to_le_bytes()); // qty
        req_buf[32..40].copy_from_slice(&15000u64.to_le_bytes()); // price
        req_buf[40..48].copy_from_slice(&0u64.to_le_bytes()); // timestamp

        // パース
        let (order_id, account_id, symbol, side, qty, price, _ts) = parse_request(&req_buf);
        assert_eq!(order_id, 1);
        assert_eq!(account_id, 100);
        assert_eq!(&symbol[..4], b"AAPL");
        assert_eq!(side, 1);
        assert_eq!(qty, 100);
        assert_eq!(price, 15000);

        // レスポンス構築
        let mut resp_buf = [0u8; RESPONSE_SIZE];
        build_response(&mut resp_buf, order_id, status::ACCEPTED, 500);

        // 検証
        assert_eq!(u64::from_le_bytes(resp_buf[0..8].try_into().unwrap()), 1);
        assert_eq!(resp_buf[8], status::ACCEPTED);
        assert_eq!(u64::from_le_bytes(resp_buf[16..24].try_into().unwrap()), 500);
    }
}
