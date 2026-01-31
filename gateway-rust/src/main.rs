//! Gateway Rust - フル Rust 実装の注文ゲートウェイ
//!
//! Kotlin版と同じ機能を提供しつつ、GC無しで安定したレイテンシを実現する。
//!
//! ## 起動方法
//! ```bash
//! GATEWAY_PORT=8081 cargo run --release -p gateway-rust
//! ```
//!
//! ## 全体フロー（超要約）
//! 1) HTTP/TCP で注文を受理（Risk→Queue投入までが同期境界）
//! 2) FastPathQueue を Exchange worker が消費して外部送信
//! 3) 監査ログへ append（正本）
//! 4) Outbox が監査ログを読み、Kafkaへ配信（best-effort）
//! 5) SSE で注文/アカウントの非同期イベントを配信
//!
//! ## 環境変数
//! - `GATEWAY_PORT`: HTTPサーバーのポート（デフォルト: 8081）
//! - `RUST_LOG`: ログレベル（デフォルト: info）

mod auth;
mod audit;
mod audit_mirror;
mod backpressure;
mod inflight;
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

    let wal_path = std::env::var("GATEWAY_WAL_PATH")
        .or_else(|_| std::env::var("GATEWAY_AUDIT_PATH"))
        .unwrap_or_else(|_| "var/gateway/audit.log".into());
    let audit_log = Arc::new(audit::AuditLog::new(wal_path)?);
    info!("WAL initialized");
    let (durable_tx, durable_rx) = tokio::sync::mpsc::unbounded_channel();
    audit_log
        .clone()
        .start_async_writer(Some(durable_tx));

    let mirror_enabled = std::env::var("AUDIT_MIRROR_ENABLE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if mirror_enabled {
        let mirror_path = std::env::var("AUDIT_LOG_PATH")
            .or_else(|_| std::env::var("AUDIT_MIRROR_PATH"))
            .unwrap_or_else(|_| "var/gateway/audit.mirror.log".into());
        let mirror_offset = std::env::var("AUDIT_MIRROR_OFFSET_PATH")
            .unwrap_or_else(|_| "var/gateway/audit.mirror.offset".into());
        audit_mirror::AuditMirrorWorker::new(audit_log.path(), mirror_path, mirror_offset).start();
        info!("Audit mirror worker started");
    }

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
        // 同一キューへの共有ハンドル（Arc）を取り出して worker に渡す
        let queue = engine.queue();
        let host = exchange_host.clone();
        let port = config.exchange_port;
        let workers = std::env::var("EXCHANGE_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1);
        for _ in 0..workers {
            // worker ごとに Arc を複製して所有権を渡す（キュー本体は共有）
            engine::exchange_worker::start_worker(Arc::clone(&queue), host.clone(), port);
        }
        info!(
            "Exchange worker started ({}:{}) workers={}",
            exchange_host, port, workers
        );
    } else {
        let drain_enabled = std::env::var("FASTPATH_DRAIN_ENABLE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if drain_enabled {
            // Exchange が無い環境のキュー詰まり防止用ドレイン
            let queue = engine.queue();
            let workers = std::env::var("FASTPATH_DRAIN_WORKERS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1)
                .max(1);
            for i in 0..workers {
                engine::drain_worker::start_worker(Arc::clone(&queue), i);
            }
            info!("FastPath drain workers started (workers={})", workers);
        }
    }

    // HTTP と TCP を並行起動（先に終了/エラーした方で終了）
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
        result = server::http::run(http_port, http_engine, http_order_store, http_sse_hub, http_audit_log, http_bus, outbox_enabled, idempotency_ttl_sec, Some(durable_rx)) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "HTTP server exited with error");
                return Err(e.into());
            }
        }
        result = server::tcp::run(tcp_port, tcp_engine) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "TCP server exited with error");
                return Err(e.into());
            }
        }
    }

    Ok(())
}
