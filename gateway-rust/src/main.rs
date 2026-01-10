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

mod auth;
mod audit;
mod bus;
mod outbox;
mod config;
mod engine;
mod exchange;
mod order;
mod protocol;
mod server;
mod sse;
mod store;

use std::sync::Arc;
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

    // OrderStore 初期化
    let order_store = Arc::new(store::OrderStore::new());
    info!("OrderStore initialized");

    let audit_path = std::env::var("GATEWAY_AUDIT_PATH").unwrap_or_else(|_| "var/gateway/audit.log".into());
    let audit_log = Arc::new(audit::AuditLog::new(audit_path)?);
    info!("AuditLog initialized");

    let bus_publisher = Arc::new(bus::BusPublisher::from_env()?);
    info!("BusPublisher initialized (enabled={})", bus_publisher.is_enabled());
    let bus_mode = std::env::var("BUS_MODE").unwrap_or_else(|_| "outbox".into());
    let outbox_enabled = bus_mode == "outbox" && bus_publisher.is_enabled();
    if outbox_enabled {
        let offset_path = std::env::var("OUTBOX_OFFSET_PATH").unwrap_or_else(|_| "var/gateway/outbox.offset".into());
        outbox::OutboxWorker::new(audit_log.path(), Arc::clone(&bus_publisher), offset_path).start();
        info!("Outbox worker started");
    }

    // SSE Hub 初期化
    let sse_hub = Arc::new(sse::SseHub::from_env());
    info!("SseHub initialized");

    // エンジン初期化
    let engine = engine::FastPathEngine::new(config.queue_capacity);
    info!(
        "FastPathEngine initialized (queue_capacity: {})",
        config.queue_capacity
    );

    // Exchange Worker 起動（Exchange 接続が設定されている場合）
    if let Some(ref exchange_host) = config.exchange_host {
        let queue = engine.queue();
        let host = exchange_host.clone();
        let port = config.exchange_port;
        engine::exchange_worker::start_worker(queue, host, port);
        info!("Exchange worker started ({}:{})", exchange_host, port);
    }

    // HTTP と TCP を並行起動
    let http_engine = engine.clone();
    let tcp_engine = engine;
    let http_order_store = Arc::clone(&order_store);
    let http_sse_hub = Arc::clone(&sse_hub);
    let http_audit_log = Arc::clone(&audit_log);
    let http_bus = Arc::clone(&bus_publisher);

    let http_port = config.port;
    let tcp_port = config.tcp_port;
    let idempotency_ttl_sec = config.idempotency_ttl_sec;

    tokio::select! {
        result = server::http::run(http_port, http_engine, http_order_store, http_sse_hub, http_audit_log, http_bus, outbox_enabled, idempotency_ttl_sec) => {
            result?;
        }
        result = server::tcp::run(tcp_port, tcp_engine) => {
            result?;
        }
    }

    Ok(())
}
