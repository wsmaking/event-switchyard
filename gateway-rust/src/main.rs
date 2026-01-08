//! Gateway Rust - フル Rust 実装の注文ゲートウェイ
//!
//! Kotlin版と同じ機能を提供しつつ、GC無しで安定したレイテンシを実現する。
//!
//! ## 起動方法
//! ```bash
//! GATEWAY_PORT=8081 cargo run --release -p gateway-rust
//! ```
//!
//! ## 環境変数
//! - `GATEWAY_PORT`: HTTPサーバーのポート（デフォルト: 8081）
//! - `RUST_LOG`: ログレベル（デフォルト: info）

mod config;
mod engine;
mod order;
mod server;

use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ロギング初期化
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,gateway_rust=debug".into()),
        )
        .init();

    // 設定読み込み
    let config = config::Config::from_env();
    info!("Gateway Rust starting with config: {:?}", config);

    // エンジン初期化
    let engine = engine::FastPathEngine::new(config.queue_capacity);
    info!(
        "FastPathEngine initialized (queue_capacity: {})",
        config.queue_capacity
    );

    // HTTPサーバー起動
    server::http::run(config.port, engine).await?;

    Ok(())
}
