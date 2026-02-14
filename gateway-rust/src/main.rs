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

mod audit;
mod audit_mirror;
mod auth;
mod backpressure;
mod bus;
mod config;
mod engine;
mod exchange;
mod inflight;
mod order;
mod outbox;
mod protocol;
mod rate_limit;
mod server;
mod sse;
mod store;

use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1) プロセス初期化（ログ + 設定）
    init_tracing();
    let config = config::Config::from_env();
    info!("Gateway Rust starting with config: {:?}", config);

    // 2) コア依存の初期化（Store / Audit / Bus / SSE / Engine）
    let order_store = Arc::new(store::OrderStore::new());
    info!("OrderStore initialized");

    let (audit_log, durable_rx) = init_audit()?;
    start_audit_mirror_if_enabled(&audit_log);

    let bus_publisher = Arc::new(bus::BusPublisher::from_env()?);
    info!(
        "BusPublisher initialized (enabled={})",
        bus_publisher.is_enabled()
    );
    let outbox_enabled = start_outbox_if_enabled(&audit_log, &bus_publisher);

    let sse_hub = Arc::new(sse::SseHub::from_env());
    info!("SseHub initialized");

    let engine = engine::FastPathEngine::new(config.queue_capacity);
    info!(
        "FastPathEngine initialized (queue_capacity: {})",
        config.queue_capacity
    );

    // 3) バックグラウンド worker 起動（Exchange or Drain）
    start_exchange_or_drain_workers(&engine, &config);

    // 4) サーバー起動（HTTP/TCPを並列で待機）
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

/// ログ出力基盤を初期化する。
/// `RUST_LOG` が無い場合は `info,gateway_rust=debug` を既定値に使う。
fn init_tracing() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,gateway_rust=debug".into()),
        )
        .init();
}

/// 監査ログ（WAL）を初期化し、非同期writerと durable 通知チャネルを起動する。
/// 戻り値は `AuditLog` 本体と、durable 通知を受け取る receiver。
fn init_audit() -> anyhow::Result<(
    Arc<audit::AuditLog>,
    tokio::sync::mpsc::UnboundedReceiver<audit::AuditDurableNotification>,
)> {
    let wal_path = std::env::var("GATEWAY_WAL_PATH")
        .or_else(|_| std::env::var("GATEWAY_AUDIT_PATH"))
        .unwrap_or_else(|_| "var/gateway/audit.log".into());
    let audit_log = Arc::new(audit::AuditLog::new(wal_path)?);
    info!("WAL initialized");

    let (durable_tx, durable_rx) = tokio::sync::mpsc::unbounded_channel();
    audit_log.clone().start_async_writer(Some(durable_tx));
    Ok((audit_log, durable_rx))
}

/// 監査ミラー機能を有効化している場合のみ、ミラー worker を起動する。
/// 有効判定は `AUDIT_MIRROR_ENABLE` を参照する。
fn start_audit_mirror_if_enabled(audit_log: &Arc<audit::AuditLog>) {
    if !env_bool("AUDIT_MIRROR_ENABLE") {
        return;
    }

    let mirror_path = std::env::var("AUDIT_LOG_PATH")
        .or_else(|_| std::env::var("AUDIT_MIRROR_PATH"))
        .unwrap_or_else(|_| "var/gateway/audit.mirror.log".into());
    let mirror_offset = std::env::var("AUDIT_MIRROR_OFFSET_PATH")
        .unwrap_or_else(|_| "var/gateway/audit.mirror.offset".into());

    audit_mirror::AuditMirrorWorker::new(audit_log.path(), mirror_path, mirror_offset).start();
    info!("Audit mirror worker started");
}

/// Outbox モードが有効なときに Outbox worker を起動する。
/// 戻り値は「Outboxモードで動かしているか」を表す。
fn start_outbox_if_enabled(
    audit_log: &Arc<audit::AuditLog>,
    bus_publisher: &Arc<bus::BusPublisher>,
) -> bool {
    let bus_mode = std::env::var("BUS_MODE").unwrap_or_else(|_| "outbox".into());
    let outbox_enabled = bus_mode == "outbox" && bus_publisher.is_enabled();
    if !outbox_enabled {
        return false;
    }

    let offset_path =
        std::env::var("OUTBOX_OFFSET_PATH").unwrap_or_else(|_| "var/gateway/outbox.offset".into());
    outbox::OutboxWorker::new(audit_log.path(), Arc::clone(bus_publisher), offset_path).start();
    info!("Outbox worker started");
    true
}

/// Exchange 接続がある場合は exchange worker を起動し、
/// 無い場合は設定に応じて drain worker を起動する。
fn start_exchange_or_drain_workers(engine: &engine::FastPathEngine, config: &config::Config) {
    if let Some(exchange_host) = config.exchange_host.clone() {
        let queue = engine.queue();
        let workers = std::env::var("EXCHANGE_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1);
        for _ in 0..workers {
            engine::exchange_worker::start_worker(
                Arc::clone(&queue),
                exchange_host.clone(),
                config.exchange_port,
            );
        }
        info!(
            "Exchange worker started ({}:{}) workers={}",
            exchange_host, config.exchange_port, workers
        );
        return;
    }

    if !env_bool("FASTPATH_DRAIN_ENABLE") {
        return;
    }

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

/// 環境変数を bool として解釈するヘルパー。
/// `"1"` または `"true"`（大文字小文字無視）を `true` とみなす。
fn env_bool(key: &str) -> bool {
    std::env::var(key)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}
