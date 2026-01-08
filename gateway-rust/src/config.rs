//! 設定管理
//!
//! 環境変数から設定を読み込む。Kotlin版と同じ環境変数名を使用。

use std::env;

/// Gateway設定
#[derive(Debug, Clone)]
pub struct Config {
    /// HTTPサーバーポート
    pub port: u16,
    /// キュー容量
    pub queue_capacity: usize,
    /// 最大注文数量
    pub max_order_qty: u32,
    /// 最大想定元本
    pub max_notional: u64,
}

impl Config {
    /// 環境変数から設定を読み込む
    ///
    /// Kotlin版と同じ環境変数名を使用:
    /// - GATEWAY_PORT (デフォルト: 8081)
    /// - QUEUE_CAPACITY (デフォルト: 65536)
    /// - MAX_ORDER_QTY (デフォルト: 10000)
    /// - MAX_NOTIONAL (デフォルト: 1000000000)
    pub fn from_env() -> Self {
        // .envファイルがあれば読み込む（無くてもエラーにしない）
        let _ = dotenvy::dotenv();

        Self {
            port: env::var("GATEWAY_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8081),
            queue_capacity: env::var("QUEUE_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(65536),
            max_order_qty: env::var("MAX_ORDER_QTY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
            max_notional: env::var("MAX_NOTIONAL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1_000_000_000),
        }
    }
}
